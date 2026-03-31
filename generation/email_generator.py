"""
Cold email sequence generator with topic-aware knowledge injection
and Haiku self-review for quality assurance.

Two-pass generation:
  Pass 1: Sonnet generates 3-email sequence with topic-specific research context
  Pass 2: Haiku reviews for quality, triggers revision if needed
"""

import json
import logging
import os
from anthropic import Anthropic
from config import ANTHROPIC_API_KEY, SONNET_MODEL, HAIKU_MODEL
from generation.knowledge_base import get_topic_documents, get_research_document
from db import save_email_sequence, get_leads_for_email_gen, get_campaign_brief
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

client = Anthropic(api_key=ANTHROPIC_API_KEY)

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
        # Pass 1: Generate with Sonnet
        response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=2000,
            system=system,
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
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=400,
            system=EMAIL_REVIEWER_PROMPT or "You are a cold email quality reviewer.",
            messages=[{"role": "user", "content": review_prompt}]
        )
        result = _parse_json_response(response.content[0].text)
        track_cost(campaign_id, lead_id, "claude_haiku", "email_review",
                   cost_usd=0.003)

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
        response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=2000,
            system=system,
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


def _parse_json_response(text: str) -> dict:
    """Parse JSON from Claude response, stripping markdown fences."""
    text = text.strip()
    if text.startswith("```"):
        first_newline = text.index("\n")
        text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[:-3].strip()
    return json.loads(text)
