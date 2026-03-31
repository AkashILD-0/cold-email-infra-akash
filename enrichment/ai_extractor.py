import json
import logging
from anthropic import Anthropic
from config import ANTHROPIC_API_KEY, HAIKU_MODEL
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

client = Anthropic(api_key=ANTHROPIC_API_KEY)

# Load prompts
import os
_prompt_dir = os.path.join(os.path.dirname(__file__), "..", "prompts")

def _load_prompt(filename: str) -> str:
    with open(os.path.join(_prompt_dir, filename), "r") as f:
        return f.read()

OWNER_DISCOVERY_PROMPT = _load_prompt("owner_discovery.txt")
OWNER_VERIFICATION_PROMPT = _load_prompt("owner_verification.txt")


def extract_owner_from_website(business_name: str, website: str,
                                website_content: str,
                                campaign_id: str = None,
                                lead_id: str = None) -> dict:
    """Use Claude Haiku to extract owner from website content."""
    prompt = OWNER_DISCOVERY_PROMPT.format(
        business_name=business_name,
        website=website,
        website_content=website_content[:50000]  # Limit token usage
    )

    try:
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text
        result = json.loads(text)
        track_cost(campaign_id, lead_id, "claude_haiku", "owner_discovery")
        return result
    except Exception as e:
        logger.error(f"AI owner extraction error: {e}")
        return {"owner_name": None, "evidence": str(e), "confidence": None}


def extract_owner_from_search(business_name: str, website: str,
                               search_results: str,
                               campaign_id: str = None,
                               lead_id: str = None) -> dict:
    """Use Claude Haiku to extract owner from Google search results."""
    prompt = OWNER_DISCOVERY_PROMPT.format(
        business_name=business_name,
        website=website,
        website_content=search_results[:20000]
    )

    try:
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text
        result = json.loads(text)
        track_cost(campaign_id, lead_id, "claude_haiku", "owner_discovery",
                   cost_usd=0.005)
        return result
    except Exception as e:
        logger.error(f"AI search extraction error: {e}")
        return {"owner_name": None, "evidence": str(e), "confidence": None}


def verify_owner(business_name: str, website: str,
                 evidence_list: list,
                 campaign_id: str = None,
                 lead_id: str = None) -> dict:
    """Verify owner identity using multiple evidence sources."""
    evidence_str = "\n".join([
        f"- Source: {e['source']}, Name: {e.get('owner_name', 'unknown')}, "
        f"Evidence: {e.get('evidence', 'none')}"
        for e in evidence_list
    ])

    prompt = OWNER_VERIFICATION_PROMPT.format(
        business_name=business_name,
        website=website,
        evidence_list=evidence_str
    )

    try:
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text
        result = json.loads(text)
        track_cost(campaign_id, lead_id, "claude_haiku", "owner_verification",
                   cost_usd=0.002)
        return result
    except Exception as e:
        logger.error(f"AI verification error: {e}")
        return {"owner_name": None, "confidence": "low", "reasoning": str(e)}
