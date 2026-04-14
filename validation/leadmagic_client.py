import logging
import requests
from config import LEADMAGIC_API_KEY, LEADMAGIC_BASE_URL
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


def _headers():
    return {"X-API-Key": LEADMAGIC_API_KEY, "Content-Type": "application/json"}


def find_email(owner_name: str, domain: str,
               campaign_id: str = None, lead_id: str = None) -> dict:
    """Find email for a person at a domain. Returns {email, confidence}."""
    parts = owner_name.strip().split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""

    try:
        resp = requests.post(
            f"{LEADMAGIC_BASE_URL}/email-finder",
            headers=_headers(),
            json={"first_name": first_name, "last_name": last_name, "domain": domain},
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        track_cost(campaign_id, lead_id, "leadmagic", "email_finder")
        return {"email": data.get("email"), "confidence": data.get("confidence")}
    except Exception as e:
        logger.error(f"LeadMagic email finder error: {e}")
        return {"email": None, "confidence": None}


def validate_email(email: str,
                   campaign_id: str = None, lead_id: str = None) -> dict:
    """Validate an email. Returns {status, is_catchall}."""
    try:
        resp = requests.post(
            f"{LEADMAGIC_BASE_URL}/email-validate",
            headers=_headers(),
            json={"email": email},
            timeout=8
        )
        resp.raise_for_status()
        data = resp.json()
        track_cost(campaign_id, lead_id, "leadmagic", "email_validate")
        return {
            "status": data.get("status"),
            "is_catchall": data.get("is_catchall", False),
        }
    except Exception as e:
        logger.error(f"LeadMagic validate error: {e}")
        return {"status": "unknown", "is_catchall": False}


def search_company(domain: str,
                   campaign_id: str = None, lead_id: str = None) -> dict:
    """Enrich company data from domain. Returns company info dict."""
    try:
        resp = requests.post(
            f"{LEADMAGIC_BASE_URL}/company-search",
            headers=_headers(),
            json={"domain": domain},
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
        track_cost(campaign_id, lead_id, "leadmagic", "company_search")
        return {
            "company_name": data.get("name"),
            "industry": data.get("industry"),
            "company_size": data.get("size"),
            "emails": data.get("emails", []),
        }
    except Exception as e:
        logger.error(f"LeadMagic company search error: {e}")
        return {}
