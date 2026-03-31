import json
import logging
import os
from anthropic import Anthropic
from config import ANTHROPIC_API_KEY, SONNET_MODEL
from generation.knowledge_base import get_research_document
from db import save_email_sequence, get_leads_for_email_gen
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

client = Anthropic(api_key=ANTHROPIC_API_KEY)

# Load system prompt and examples
_prompt_dir = os.path.join(os.path.dirname(__file__), "..", "prompts")

def _load_prompt(filename: str) -> str:
    with open(os.path.join(_prompt_dir, filename), "r") as f:
        return f.read()

EMAIL_SYSTEM_PROMPT = _load_prompt("email_system.txt")


def generate_batch(campaign_id: str, batch_size: int = 100) -> dict:
    """Generate email sequences for SEND leads without sequences.
    Returns {generated, errors}."""
    leads = get_leads_for_email_gen(campaign_id, batch_size)
    stats = {"generated": 0, "errors": 0}

    research_context = get_research_document()

    for lead in leads:
        try:
            sequences = generate_sequence(lead, research_context, campaign_id)
            if sequences:
                save_email_sequence(str(lead["lead_id"]), campaign_id, sequences)
                stats["generated"] += 1
        except Exception as e:
            logger.error(f"Email gen error for lead {lead['lead_id']}: {e}")
            stats["errors"] += 1

    logger.info(f"Email generation batch: {stats}")
    return stats


def generate_sequence(lead: dict, research_context: str,
                      campaign_id: str = None) -> dict:
    """Generate a 3-email sequence for a single lead."""
    lead_context = _build_lead_context(lead)

    prompt = f"""Generate a 3-email cold outreach sequence for this lead.

{lead_context}

Rules:
- Email 1: Initial outreach — value-first, personalized to their business
- Email 2: Follow-up (3-5 days later) — different angle, add social proof
- Email 3: Break-up (7-10 days later) — permission close, create urgency
- Keep each email under 150 words
- Use the owner's first name if available
- Reference specific details about their business (rating, reviews, location)
- Never use generic phrases like "I hope this email finds you well"
- Subject lines: short (3-7 words), curiosity-driven or benefit-driven

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
        system += f"\n\n## Cold Email Research & Frameworks\n{research_context[:10000]}"

    try:
        response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=2000,
            system=system,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            first_newline = text.index("\n")
            text = text[first_newline + 1:]
            if text.endswith("```"):
                text = text[:-3].strip()
        sequences = json.loads(text)

        track_cost(campaign_id, str(lead.get("lead_id")),
                   "claude_sonnet", "email_generation")
        return sequences
    except Exception as e:
        logger.error(f"Email generation error: {e}")
        return None


def _build_lead_context(lead: dict) -> str:
    """Build a context string from lead data for the prompt."""
    parts = []
    if lead.get("owner_name"):
        parts.append(f"Owner/Contact: {lead['owner_name']}")
    if lead.get("business_name"):
        parts.append(f"Business: {lead['business_name']}")
    if lead.get("website"):
        parts.append(f"Website: {lead['website']}")
    if lead.get("industry"):
        parts.append(f"Industry: {lead['industry']}")
    if lead.get("city") and lead.get("state"):
        parts.append(f"Location: {lead['city']}, {lead['state']}")
    if lead.get("rating"):
        parts.append(f"Google Rating: {lead['rating']}/5")
    if lead.get("review_count"):
        parts.append(f"Reviews: {lead['review_count']}")
    if lead.get("company_size"):
        parts.append(f"Company Size: {lead['company_size']}")
    return "\n".join(parts)
