import logging
from ingestion.apify_client import scrape_google_maps
from ingestion.apollo_client import search_people
from ingestion.deduplicator import deduplicate_leads
from db import insert_lead

logger = logging.getLogger(__name__)

# Pre-built niche configs (from LeadForge v1)
NICHE_CONFIGS = {
    "dentists": {
        "queries": ["dentist", "dental office", "dental practice", "family dentist"],
        "apollo_titles": ["Owner", "Dentist", "DDS", "DMD", "Practice Owner"],
    },
    "plastic_surgeons": {
        "queries": ["plastic surgeon", "cosmetic surgeon", "plastic surgery clinic"],
        "apollo_titles": ["Owner", "Surgeon", "MD", "Plastic Surgeon"],
    },
    "med_spas": {
        "queries": ["med spa", "medical spa", "medspa", "aesthetic clinic"],
        "apollo_titles": ["Owner", "Founder", "Medical Director"],
    },
    "supplement_stores": {
        "queries": ["supplement store", "vitamin shop", "nutrition store", "health food store"],
        "apollo_titles": ["Owner", "Founder", "Manager"],
    },
    "chiropractors": {
        "queries": ["chiropractor", "chiropractic office", "chiropractic clinic"],
        "apollo_titles": ["Owner", "Chiropractor", "DC"],
    },
    # Add more niches as needed — this is extensible
}


def route_and_ingest(niche: str, location: str, campaign_id: str,
                     sources: list = None) -> dict:
    """Route a lead request to best sources, merge, dedup, and insert.
    Returns {total_found, unique, inserted}."""

    if sources is None:
        sources = ["apify_gmaps", "apollo"]

    config = NICHE_CONFIGS.get(niche, {"queries": [niche], "apollo_titles": ["Owner", "Founder"]})
    all_leads = []

    # Apify Google Maps
    if "apify_gmaps" in sources:
        for query in config["queries"]:
            leads = scrape_google_maps(query, location, campaign_id=campaign_id)
            all_leads.extend(leads)

    # Apollo
    if "apollo" in sources:
        leads = search_people(
            title_keywords=config["apollo_titles"],
            location=location,
            campaign_id=campaign_id
        )
        all_leads.extend(leads)

    total_found = len(all_leads)

    # Dedup
    unique_leads = deduplicate_leads(all_leads)

    # Insert into DB
    inserted = 0
    for lead in unique_leads:
        lead_id = insert_lead(lead, campaign_id)
        if lead_id:
            inserted += 1

    stats = {"total_found": total_found, "unique": len(unique_leads), "inserted": inserted}
    logger.info(f"Source router: {stats}")
    return stats
