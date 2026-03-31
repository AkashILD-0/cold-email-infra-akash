import logging
from enrichment.website_scraper import scrape_website
from enrichment.ai_extractor import (
    extract_owner_from_website, extract_owner_from_search, verify_owner
)
from ingestion.apollo_client import search_people
from db import update_lead_fields

logger = logging.getLogger(__name__)


def discover_owner(lead: dict, campaign_id: str) -> dict:
    """Run the 5-step owner discovery cascade for a single lead.
    Returns {owner_name, owner_source, owner_confidence, owner_status}."""

    lead_id = str(lead["lead_id"])
    business_name = lead.get("business_name", "")
    website = lead.get("website", "")
    domain = lead.get("business_domain", "")

    evidence = []

    # Step 1: LinkedIn / Apollo check
    if domain:
        apollo_results = search_people(
            domain=domain,
            title_keywords=["Owner", "Founder", "CEO", "President", "Principal"],
            limit=5, campaign_id=campaign_id
        )
        for person in apollo_results:
            if person.get("owner_name"):
                evidence.append({
                    "source": "apollo",
                    "owner_name": person["owner_name"],
                    "evidence": f"Found on LinkedIn/Apollo with title at {domain}"
                })
                break

    # Step 2: Website scrape + AI extraction
    website_data = None
    if website and not _has_high_confidence(evidence):
        website_data = scrape_website(website)
        if website_data["pages"]:
            all_text = "\n\n".join([p["text"] for p in website_data["pages"]])
            ai_result = extract_owner_from_website(
                business_name, website, all_text,
                campaign_id=campaign_id, lead_id=lead_id
            )
            if ai_result.get("owner_name"):
                evidence.append({
                    "source": "website_scrape_ai",
                    "owner_name": ai_result["owner_name"],
                    "evidence": ai_result.get("evidence", ""),
                })

    # Step 3: Google Search + AI extraction
    if not _has_high_confidence(evidence):
        search_text = _google_search(business_name, lead.get("city", ""))
        if search_text:
            ai_result = extract_owner_from_search(
                business_name, website, search_text,
                campaign_id=campaign_id, lead_id=lead_id
            )
            if ai_result.get("owner_name"):
                evidence.append({
                    "source": "google_search_ai",
                    "owner_name": ai_result["owner_name"],
                    "evidence": ai_result.get("evidence", ""),
                })

    # Step 4: Verification (if any evidence found)
    if evidence:
        if len(evidence) == 1 and evidence[0].get("source") == "apollo":
            # Apollo alone is high confidence — skip verification
            result = {
                "owner_name": evidence[0]["owner_name"],
                "owner_source": "apollo",
                "owner_confidence": "high",
                "owner_status": "found",
            }
        else:
            verified = verify_owner(
                business_name, website, evidence,
                campaign_id=campaign_id, lead_id=lead_id
            )
            result = {
                "owner_name": verified.get("owner_name"),
                "owner_source": _best_source(evidence),
                "owner_confidence": verified.get("confidence", "low"),
                "owner_status": "found" if verified.get("owner_name") else "not_found",
            }
    else:
        # Step 5: No owner found
        result = {
            "owner_name": None,
            "owner_source": None,
            "owner_confidence": None,
            "owner_status": "not_found",
        }

    # Also attach website scrape data for email waterfall reuse
    result["_website_data"] = website_data

    # Update lead in DB
    update_lead_fields(lead_id, {
        "owner_name": result["owner_name"],
        "owner_source": result["owner_source"],
        "owner_confidence": result["owner_confidence"],
        "owner_status": result["owner_status"],
    })

    logger.info(
        f"Owner discovery for {business_name}: "
        f"{result['owner_name']} ({result['owner_confidence']})"
    )
    return result


def _has_high_confidence(evidence: list) -> bool:
    return len(evidence) >= 2 or (
        len(evidence) == 1 and evidence[0].get("source") == "apollo"
    )


def _best_source(evidence: list) -> str:
    priority = {"apollo": 3, "website_scrape_ai": 2, "google_search_ai": 1}
    return max(evidence, key=lambda e: priority.get(e["source"], 0))["source"]


def _google_search(business_name: str, city: str) -> str:
    """Search Google for business owner. Returns text results."""
    from config import GOOGLE_SEARCH_API_KEY, GOOGLE_SEARCH_CX
    if not GOOGLE_SEARCH_API_KEY:
        return ""

    import requests
    query = f"{business_name} {city} owner"
    try:
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": GOOGLE_SEARCH_API_KEY, "cx": GOOGLE_SEARCH_CX,
                    "q": query, "num": 5},
            timeout=10
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
        return "\n\n".join([
            f"Title: {item.get('title')}\nSnippet: {item.get('snippet')}"
            for item in items
        ])
    except Exception as e:
        logger.error(f"Google search error: {e}")
        return ""
