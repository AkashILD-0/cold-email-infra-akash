import logging
import requests
from config import APOLLO_API_KEY, APOLLO_BASE_URL
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


def search_people(domain: str = None, title_keywords: list = None,
                  location: str = None, limit: int = 25,
                  campaign_id: str = None) -> list:
    """Search Apollo for people matching criteria. Returns lead dicts."""
    params = {
        "api_key": APOLLO_API_KEY,
        "per_page": min(limit, 100),
    }
    if domain:
        params["q_organization_domains"] = domain
    if title_keywords:
        params["person_titles"] = title_keywords
    if location:
        params["person_locations"] = [location]

    try:
        resp = requests.post(
            f"{APOLLO_BASE_URL}/mixed_people/search",
            json=params, timeout=30
        )
        resp.raise_for_status()
        people = resp.json().get("people", [])

        leads = []
        for person in people:
            org = person.get("organization", {})
            domain = org.get("primary_domain")
            lead = {
                "business_name": org.get("name"),
                "business_domain": domain,
                "website": f"https://{domain}" if domain else None,
                "phone": person.get("phone_numbers", [{}])[0].get("sanitized_number") if person.get("phone_numbers") else None,
                "city": person.get("city"),
                "state": person.get("state"),
                "country": person.get("country", "US"),
                "industry": org.get("industry"),
                "company_size": org.get("estimated_num_employees"),
                "owner_name": person.get("name"),
                "owner_source": "apollo",
                "owner_confidence": "high",
                "owner_status": "found",
                "email": person.get("email"),
                "email_source": "apollo" if person.get("email") else None,
                "email_type": "personal" if person.get("email") else None,
                "sources": ["apollo"],
                "raw_data": person,
            }
            leads.append(lead)

        track_cost(campaign_id, None, "apollo", "people_search", credits_used=len(leads))
        logger.info(f"Apollo: {len(leads)} people found")
        return leads
    except Exception as e:
        logger.error(f"Apollo search error: {e}")
        return []
