"""
Cold email sequence generator with topic-aware knowledge injection
and self-review for quality assurance.

Two-pass generation:
  Pass 1: Gemini Flash (default) or Sonnet generates 3-email sequence
  Pass 2: Gemini Flash Lite or Haiku reviews for quality, triggers revision if needed

Set GEMINI_EMAIL_GENERATION=true in .env to use Gemini Flash (~10x cheaper than Sonnet).
"""

import json
import logging
import os
from anthropic import Anthropic
from config import (ANTHROPIC_API_KEY, SONNET_MODEL, HAIKU_MODEL,
                    GEMINI_API_KEY, GEMINI_FLASH_MODEL, GEMINI_FLASH_LITE_MODEL,
                    GEMINI_EMAIL_GENERATION, GOOGLE_GENAI_USE_VERTEXAI, GOOGLE_CLOUD_PROJECT)
from generation.knowledge_base import get_topic_documents, get_research_document
from db import save_email_sequence, get_leads_for_email_gen, get_campaign_brief
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

client = Anthropic(api_key=ANTHROPIC_API_KEY)


def _gemini_json_call(prompt: str, system: str, operation: str,
                      campaign_id: str, lead_id: str,
                      temperature: float = 0.4,
                      max_output_tokens: int = 4096,
                      model: str = None) -> dict:
    """Call Gemini Flash and return parsed JSON. Mirrors Arjun's implementation.
    Uses Vertex AI when GOOGLE_GENAI_USE_VERTEXAI=true, otherwise direct API key."""
    from google import genai as _genai
    from google.genai import types as _genai_types

    if GOOGLE_GENAI_USE_VERTEXAI and GOOGLE_CLOUD_PROJECT:
        gemini_client = _genai.Client(vertexai=True, project=GOOGLE_CLOUD_PROJECT,
                                      location="us-central1")
    else:
        gemini_client = _genai.Client(api_key=GEMINI_API_KEY)
    used_model = model or GEMINI_FLASH_MODEL

    # Gemini 2.5 thinking mode eats output tokens — pin a small budget
    response = gemini_client.models.generate_content(
        model=used_model,
        contents=prompt,
        config=_genai_types.GenerateContentConfig(
            system_instruction=system,
            response_mime_type="application/json",
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            thinking_config=_genai_types.ThinkingConfig(thinking_budget=512),
        ),
    )
    provider = "gemini_flash_lite" if used_model == GEMINI_FLASH_LITE_MODEL else "gemini_flash"
    cost = 0.00005 if used_model == GEMINI_FLASH_LITE_MODEL else 0.0005
    track_cost(campaign_id, lead_id, provider, operation, cost_usd=cost)
    return _parse_json_response(response.text)

# Load prompts
_prompt_dir = os.path.join(os.path.dirname(__file__), "..", "prompts")


def _load_prompt(filename: str) -> str:
    path = os.path.join(_prompt_dir, filename)
    if os.path.exists(path):
        with open(path, "r") as f:
            return f.read()
    return ""


EMAIL_SYSTEM_PROMPT = _load_prompt("email_system.txt")
EMAIL_REVIEWER_PROMPT = _load_prompt("email_reviewer.txt")


def generate_batch(campaign_id: str, batch_size: int = 100) -> dict:
    """Generate email sequences for SEND leads without sequences.
    Returns {generated, revised, errors}."""
    leads = get_leads_for_email_gen(campaign_id, batch_size)
    stats = {"generated": 0, "revised": 0, "errors": 0}

    # Load context once for the batch
    research_context = _get_smart_research_context()
    brief = get_campaign_brief(campaign_id)
    brief_context = _build_brief_context(brief) if brief else ""

    for lead in leads:
        try:
            sequences, was_revised = generate_sequence(
                lead, research_context, brief_context, campaign_id
            )
            if sequences:
                save_email_sequence(str(lead["lead_id"]), campaign_id, sequences)
                stats["generated"] += 1
                if was_revised:
                    stats["revised"] += 1
        except Exception as e:
            logger.error(f"Email gen error for lead {lead['lead_id']}: {e}")
            stats["errors"] += 1

    logger.info(f"Email generation batch: {stats}")
    return stats


def generate_sequence(lead: dict, research_context: str,
                      brief_context: str = "",
                      campaign_id: str = None) -> tuple:
    """Generate a 3-email sequence for a single lead.
    Returns (sequences_dict, was_revised)."""
    lead_context = _build_lead_context(lead)

    # Build the prompt with brief context if available
    brief_section = ""
    if brief_context:
        brief_section = f"""
## What You Are Selling
{brief_context}

IMPORTANT: Every email MUST clearly communicate the service above.
Use the case study as social proof. Sign with the sender name. Use the specified CTA.
"""

    prompt = f"""Generate a 3-email cold outreach sequence for this lead.
{brief_section}
## Lead Details
{lead_context}

Rules:
- Email 1: Initial outreach — value-first, personalized to their business, clearly communicate what you offer
- Email 2: Follow-up (3-5 days later) — different angle, use the case study as social proof
- Email 3: Break-up (7-10 days later) — permission close, create urgency
- Keep each email under 150 words
- Use the owner's first name if available, otherwise use a natural greeting
- Reference specific details about their business (rating, reviews, location)
- Never use generic phrases like "I hope this email finds you well"
- Subject lines: short (3-7 words), curiosity-driven or benefit-driven
- Do NOT start any email with "I" — lead with them, not you

Respond in this exact JSON format:
{{
    "email_1_subject": "...",
    "email_1_body": "...",
    "email_2_subject": "...",
    "email_2_body": "...",
    "email_3_subject": "...",
    "email_3_body": "..."
}}"""

    system = EMAIL_SYSTEM_PROMPT
    if research_context:
        system += f"\n\n## Cold Email Research & Frameworks\n{research_context[:15000]}"

    try:
        # Pass 1: Generate
        if GEMINI_EMAIL_GENERATION:
            sequences = _gemini_json_call(prompt, system, "email_generation",
                                          campaign_id, str(lead.get("lead_id")),
                                          max_output_tokens=2000)
        else:
            response = client.messages.create(
                model=SONNET_MODEL, max_tokens=2000, system=system,
                messages=[{"role": "user", "content": prompt}]
            )
            sequences = _parse_json_response(response.content[0].text)
            track_cost(campaign_id, str(lead.get("lead_id")),
                       "claude_sonnet", "email_generation")

        # Pass 2: Self-review with Haiku
        if EMAIL_REVIEWER_PROMPT:
            review = _review_sequence(sequences, lead_context, campaign_id,
                                      str(lead.get("lead_id")))
            if review and review.get("needs_revision"):
                # One revision pass with Sonnet
                revised = _revise_sequence(sequences, review["feedback"],
                                           lead_context, system, campaign_id,
                                           str(lead.get("lead_id")))
                if revised:
                    return revised, True

        return sequences, False

    except Exception as e:
        logger.error(f"Email generation error: {e}")
        return None, False


def _review_sequence(sequences: dict, lead_context: str,
                     campaign_id: str, lead_id: str) -> dict:
    """Haiku reviews the generated sequence for quality."""
    review_prompt = f"""Review this cold email sequence for quality.

Lead context:
{lead_context}

Generated sequence:
Email 1 Subject: {sequences.get('email_1_subject', '')}
Email 1 Body:
{sequences.get('email_1_body', '')}

Email 2 Subject: {sequences.get('email_2_subject', '')}
Email 2 Body:
{sequences.get('email_2_body', '')}

Email 3 Subject: {sequences.get('email_3_subject', '')}
Email 3 Body:
{sequences.get('email_3_body', '')}

Score 1-10 on each dimension:
1. Personalization: Uses specific business details (name, rating, location)?
2. Spam risk: Any trigger words, ALL CAPS, excessive punctuation?
3. Subject lines: Short (3-7 words), curiosity/benefit-driven, lowercase?
4. Brevity: Each email under 150 words?
5. Value-first: Leads with value, not features or self-introduction?
6. Natural tone: Sounds like a real person, not a template?

Respond in JSON:
{{"scores": {{"personalization": N, "spam_risk": N, "subject_lines": N, "brevity": N, "value_first": N, "natural_tone": N}}, "needs_revision": true/false, "feedback": "specific revision instructions if needed"}}

Set needs_revision=true only if any score is below 6."""

    try:
        if GEMINI_EMAIL_GENERATION:
            result = _gemini_json_call(
                review_prompt,
                EMAIL_REVIEWER_PROMPT or "You are a cold email quality reviewer.",
                "email_review", campaign_id, lead_id,
                max_output_tokens=800, model=GEMINI_FLASH_LITE_MODEL,
            )
        else:
            response = client.messages.create(
                model=HAIKU_MODEL, max_tokens=800,
                system=EMAIL_REVIEWER_PROMPT or "You are a cold email quality reviewer.",
                messages=[{"role": "user", "content": review_prompt}]
            )
            result = _parse_json_response(response.content[0].text)
            track_cost(campaign_id, lead_id, "claude_haiku", "email_review", cost_usd=0.003)

        if result.get("needs_revision"):
            logger.info(f"Review flagged for revision: {result.get('feedback', '')[:100]}")

        return result
    except Exception as e:
        logger.error(f"Review error: {e}")
        return None


def _revise_sequence(original: dict, feedback: str, lead_context: str,
                     system: str, campaign_id: str, lead_id: str) -> dict:
    """Revise a sequence based on reviewer feedback."""
    prompt = f"""Revise this cold email sequence based on the reviewer's feedback.

Lead context:
{lead_context}

Original sequence (JSON):
{json.dumps(original, indent=2)}

Reviewer feedback:
{feedback}

Revise the sequence to address the feedback. Keep the same JSON format.
Respond with ONLY the revised JSON — no explanation."""

    try:
        if GEMINI_EMAIL_GENERATION:
            revised = _gemini_json_call(prompt, system, "email_revision",
                                        campaign_id, lead_id, max_output_tokens=2000)
        else:
            response = client.messages.create(
                model=SONNET_MODEL, max_tokens=2000, system=system,
                messages=[{"role": "user", "content": prompt}]
            )
            revised = _parse_json_response(response.content[0].text)
            track_cost(campaign_id, lead_id, "claude_sonnet", "email_revision")
        return revised
    except Exception as e:
        logger.error(f"Revision error: {e}")
        return None


def _get_smart_research_context() -> str:
    """Get research context, preferring topic-specific docs over monolithic."""
    # Try topic-specific docs first (covers all key areas)
    topics = get_topic_documents([
        "frameworks", "subject_lines", "personalization",
        "sequence_structure", "mistakes_and_deliverability"
    ])
    if topics:
        return topics

    # Fallback to old-style research document
    return get_research_document()


def _build_lead_context(lead: dict) -> str:
    """Build a context string from lead data for the prompt."""
    parts = []
    if lead.get("owner_name"):
        parts.append(f"Owner/Contact: {lead['owner_name']}")
    else:
        parts.append("Owner/Contact: Unknown (use 'Hi there' greeting)")
    if lead.get("business_name"):
        parts.append(f"Business: {lead['business_name']}")
    if lead.get("website"):
        parts.append(f"Website: {lead['website']}")
    if lead.get("industry"):
        parts.append(f"Industry: {lead['industry']}")
    if lead.get("city") and lead.get("state"):
        parts.append(f"Location: {lead['city']}, {lead['state']}")
    elif lead.get("city"):
        parts.append(f"Location: {lead['city']}")
    if lead.get("rating"):
        parts.append(f"Google Rating: {lead['rating']}/5")
    if lead.get("review_count"):
        parts.append(f"Reviews: {lead['review_count']}")
    if lead.get("company_size"):
        parts.append(f"Company Size: {lead['company_size']}")
    return "\n".join(parts)


def _build_brief_context(brief: dict) -> str:
    """Build prompt context from a campaign brief."""
    import json as _json
    parts = []
    parts.append(f"Service: {brief['service_name']}")
    if brief.get("service_detail"):
        parts.append(f"What you deliver: {brief['service_detail']}")
    if brief.get("value_prop"):
        parts.append(f"Core value proposition: {brief['value_prop']}")
    if brief.get("case_studies"):
        studies = brief["case_studies"]
        if isinstance(studies, str):
            studies = _json.loads(studies)
        if studies:
            parts.append("Case studies / social proof:")
            for cs in studies:
                parts.append(f"  - {cs.get('summary', _json.dumps(cs))}")
    sender = brief.get("sender_name", "{sender_name}")
    title = brief.get("sender_title", "")
    if title:
        parts.append(f"Sign as: {sender}, {title}")
    else:
        parts.append(f"Sign as: {sender}")
    cta_type = brief.get("cta_type", "call")
    cta_detail = brief.get("cta_detail", "")
    if cta_detail:
        parts.append(f"CTA: {cta_detail}")
    else:
        parts.append(f"CTA type: {cta_type}")
    if brief.get("custom_notes"):
        parts.append(f"Custom instructions: {brief['custom_notes']}")
    return "\n".join(parts)


def _resolve_greeting(lead: dict) -> str:
    """Determine the best greeting name using SOP fallback chain:
    1. owner_name first name
    2. Email local part (skip generics)
    3. Extract name from business_name (Dr. X → X)
    4. Practice name fallback
    """
    GENERIC_PREFIXES = {"info", "contact", "admin", "office", "team", "hello",
                        "support", "noreply", "no-reply", "reception", "booking",
                        "appointments", "mail", "help", "care", "inquiries"}

    # 1. Owner name
    owner = lead.get("owner_name", "")
    if owner:
        first = owner.strip().split()[0]
        if first.lower() not in GENERIC_PREFIXES:
            return first

    # 2. Email local part
    email = lead.get("email", "")
    if email and "@" in email:
        local = email.split("@")[0].lower()
        # Skip generic inboxes
        if local not in GENERIC_PREFIXES and not any(g in local for g in GENERIC_PREFIXES):
            # Try to extract a name: john.smith → John
            parts = local.replace(".", " ").replace("_", " ").replace("-", " ").split()
            candidate = parts[0].capitalize()
            if len(candidate) >= 2 and candidate.isalpha():
                return candidate

    # 3. Parse "Dr. First Last, Title" from business name
    import re
    biz = lead.get("business_name", "")
    dr_match = re.search(r"\bDr\.?\s+([A-Z][a-z]+)", biz)
    if dr_match:
        return dr_match.group(1)

    # 4. Practice name fallback
    return biz or "there"


def _scrape_website_insights(website: str) -> str:
    """Scrape a practice website and return a brief insights summary (for email personalization).
    Returns a short string with 3-5 key observations, or empty string if scraping fails."""
    if not website:
        return ""

    try:
        from enrichment.website_scraper import scrape_website
        data = scrape_website(website)
        if not data or not data.get("pages"):
            return ""

        # Combine homepage + about/team pages (max 8000 chars to keep tokens low)
        priority_pages = []
        other_pages = []
        for page in data["pages"]:
            url = page.get("url", "").lower()
            text = page.get("text", "")
            if not text:
                continue
            if any(p in url for p in ["/about", "/team", "/doctor", "/provider",
                                       "/staff", "/meet", "/welcome", "/our"]):
                priority_pages.append(text)
            else:
                other_pages.append(text)

        combined = "\n\n".join(priority_pages + other_pages)
        return combined[:8000]
    except Exception as e:
        logger.debug(f"Website scrape failed for {website}: {e}")
        return ""


def generate_personalized_sequence(lead: dict, brief: dict,
                                    campaign_id: str = None) -> tuple:
    """Generate a website-personalized 3-email sequence for a single lead.
    Scrapes the practice website, extracts insights, personalizes the email.
    Returns (sequences_dict, website_insights_str, was_revised)."""

    # Determine website URL
    website = lead.get("website", "")
    if not website and lead.get("business_domain"):
        website = f"https://{lead['business_domain']}"
    elif not website and lead.get("email") and "@" in lead["email"]:
        domain = lead["email"].split("@")[1]
        website = f"https://{domain}"

    # Scrape website for context
    website_raw = _scrape_website_insights(website) if website else ""

    # Use Claude Haiku to summarize website into 3-5 key insights
    website_insights = ""
    if website_raw:
        try:
            insight_prompt = f"""You are reviewing a medical practice website to find 3-5 specific, useful details that could personalize a cold email.

Practice: {lead.get('business_name', '')}
Website content (excerpt):
{website_raw[:6000]}

Extract 3-5 specific, concrete observations about this practice that make it unique. Focus on:
- Doctor names, credentials, years of experience
- Specific services or specialties they emphasize
- Technology or equipment mentioned
- Patient philosophy or unique approach
- Any awards, recognition, or notable features

Write as a brief bullet list. Be specific — avoid generic observations like "they offer OB/GYN services."
If the website has no useful unique content, write "No distinctive details found."

Respond with ONLY the bullet list, nothing else."""

            if GEMINI_EMAIL_GENERATION:
                result = _gemini_json_call(
                    insight_prompt, "", "email_generation",
                    campaign_id or "", str(lead.get("lead_id", "")),
                    max_output_tokens=300, model=GEMINI_FLASH_LITE_MODEL,
                )
                website_insights = result if isinstance(result, str) else json.dumps(result)
            else:
                ins_response = client.messages.create(
                    model=HAIKU_MODEL, max_tokens=300,
                    messages=[{"role": "user", "content": insight_prompt}]
                )
                website_insights = ins_response.content[0].text.strip()
        except Exception as e:
            logger.debug(f"Insight extraction failed: {e}")
            website_insights = ""

    # Build greeting
    greeting_name = _resolve_greeting(lead)

    # Build brief context
    brief_context = _build_brief_context(brief) if brief else ""

    # Build personalization block
    personalization_block = ""
    if website_insights and "No distinctive details" not in website_insights:
        personalization_block = f"""
## Practice-Specific Research
Website: {website}
Key insights:
{website_insights}

Use 1-2 of these details to make the first email feel like you've actually looked at their practice.
"""

    lead_context = _build_lead_context(lead)

    prompt = f"""Generate a 3-email cold outreach sequence for this OBGYN physician practice.

## What You Are Selling
{brief_context}

## Lead Details
{lead_context}
Greeting name: {greeting_name}
{personalization_block}
Rules:
- Email 1: Initial outreach — open with a practice-specific observation (if available), then value-first pitch. Under 120 words.
- Email 2: Follow-up (3-5 days later) — lead with the Allegiance case study numbers ($373K, 1,768 visits in 6 weeks). Under 100 words.
- Email 3: Break-up (7-10 days later) — short permission close, create urgency. Under 80 words.
- Start every email with "Hi {greeting_name}," (use the provided greeting name)
- Never use generic openers like "I hope this email finds you well"
- Subject lines: 3-7 words, lowercase, curiosity or benefit-driven
- Do NOT start any email body with "I" — lead with them or with an observation

Respond in this exact JSON format:
{{
    "email_1_subject": "...",
    "email_1_body": "...",
    "email_2_subject": "...",
    "email_2_body": "...",
    "email_3_subject": "...",
    "email_3_body": "..."
}}"""

    research_context = _get_smart_research_context()
    system = EMAIL_SYSTEM_PROMPT
    if research_context:
        system += f"\n\n## Cold Email Research & Frameworks\n{research_context[:12000]}"

    try:
        if GEMINI_EMAIL_GENERATION:
            sequences = _gemini_json_call(prompt, system, "email_generation",
                                          campaign_id, str(lead.get("lead_id")),
                                          max_output_tokens=2000)
        else:
            response = client.messages.create(
                model=SONNET_MODEL, max_tokens=2000, system=system,
                messages=[{"role": "user", "content": prompt}]
            )
            sequences = _parse_json_response(response.content[0].text)
            track_cost(campaign_id, str(lead.get("lead_id")),
                       "claude_sonnet", "email_generation")

        # Review pass
        was_revised = False
        if EMAIL_REVIEWER_PROMPT:
            lead_ctx_for_review = lead_context
            if personalization_block:
                lead_ctx_for_review += "\n" + personalization_block
            review = _review_sequence(sequences, lead_ctx_for_review,
                                      campaign_id, str(lead.get("lead_id")))
            if review and review.get("needs_revision"):
                revised = _revise_sequence(sequences, review["feedback"],
                                           lead_ctx_for_review, system,
                                           campaign_id, str(lead.get("lead_id")))
                if revised:
                    sequences = revised
                    was_revised = True

        return sequences, website_insights, was_revised

    except Exception as e:
        logger.error(f"Personalized email generation error: {e}")
        return None, website_insights, False


def _parse_json_response(text: str) -> dict:
    """Parse JSON from Claude response, stripping markdown fences."""
    text = text.strip()
    if text.startswith("```"):
        first_newline = text.index("\n")
        text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[:-3].strip()
    return json.loads(text)
