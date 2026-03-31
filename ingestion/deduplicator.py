import logging

logger = logging.getLogger(__name__)


def deduplicate_leads(leads: list) -> list:
    """Deduplicate a list of lead dicts. Merge on domain, fallback on name+city+state.
    Keeps the record with more filled columns, merges sources."""
    seen_domains = {}
    seen_name_loc = {}
    deduped = []

    for lead in leads:
        domain = lead.get("business_domain")
        name_key = _name_location_key(lead)

        if domain and domain in seen_domains:
            # Merge into existing record
            existing = seen_domains[domain]
            _merge_lead(existing, lead)
            continue

        if name_key and name_key in seen_name_loc:
            existing = seen_name_loc[name_key]
            _merge_lead(existing, lead)
            continue

        # New lead
        if domain:
            seen_domains[domain] = lead
        if name_key:
            seen_name_loc[name_key] = lead
        deduped.append(lead)

    logger.info(f"Dedup: {len(leads)} input → {len(deduped)} unique")
    return deduped


def _name_location_key(lead: dict) -> str:
    """Create a dedup key from business_name + city + state."""
    name = (lead.get("business_name") or "").strip().lower()
    city = (lead.get("city") or "").strip().lower()
    state = (lead.get("state") or "").strip().lower()
    if name and city and state:
        return f"{name}|{city}|{state}"
    return None


def _merge_lead(existing: dict, incoming: dict):
    """Merge incoming lead data into existing. Fill empty fields, merge sources."""
    # Merge sources
    existing_sources = set(existing.get("sources", []))
    incoming_sources = set(incoming.get("sources", []))
    existing["sources"] = list(existing_sources | incoming_sources)

    # Fill empty fields from incoming
    for key, value in incoming.items():
        if key in ("sources", "raw_data"):
            continue
        if value and not existing.get(key):
            existing[key] = value
