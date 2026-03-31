"""
Clay-style column-batch enrichment engine.

Instead of processing each lead through all steps sequentially,
we process ALL leads through one step at a time (column-parallel):

  Column 1: Owner discovery   → batch all leads concurrently
  Column 2: Email finding     → batch leads with null email concurrently
  Column 3: Email validation  → batch leads with email but no verdict concurrently

Each column runs its batch with ThreadPoolExecutor. Within each column,
the waterfall/cascade logic still applies (try source A, if null try B, etc.).
After each column completes, only leads that still need the next column proceed.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from enrichment.owner_discovery import discover_owner
from enrichment.email_waterfall import find_email
from validation.cascade_validator import validate_lead_email
from db import get_leads_needing_enrichment, update_lead_fields
from config import PROCESS_BATCH_SIZE, OWNER_DISCOVERY_WORKERS, EMAIL_WATERFALL_WORKERS, VALIDATION_WORKERS

logger = logging.getLogger(__name__)


def _run_column(leads: list, fn, campaign_id: str, workers: int, column_name: str) -> list:
    """Run a single enrichment column across all leads concurrently.
    Returns list of (lead, result) tuples."""
    if not leads:
        return []

    actual_workers = min(workers, len(leads))
    logger.info(f"  Column '{column_name}': {len(leads)} leads, {actual_workers} workers")

    results = []
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        future_to_lead = {
            executor.submit(fn, lead, campaign_id): lead
            for lead in leads
        }
        for future in as_completed(future_to_lead):
            lead = future_to_lead[future]
            try:
                result = future.result()
                results.append((lead, result))
            except Exception as e:
                lead_id = str(lead.get("lead_id", "?"))
                logger.error(f"  Column '{column_name}' error for lead {lead_id}: {e}")
                results.append((lead, None))

    return results


def _owner_step(lead: dict, campaign_id: str) -> dict:
    """Owner discovery for one lead. Used as column function."""
    lead_id = str(lead["lead_id"])
    update_lead_fields(lead_id, {"enrichment_status": "enriching"})
    result = discover_owner(lead, campaign_id)
    # Update lead dict in-place for downstream columns
    if result.get("owner_name"):
        lead["owner_name"] = result["owner_name"]
    lead["owner_status"] = result.get("owner_status", "not_found")
    lead["_website_data"] = result.get("_website_data")
    # Update status
    new_status = _determine_status(lead)
    update_lead_fields(lead_id, {"enrichment_status": new_status})
    return result


def _email_step(lead: dict, campaign_id: str) -> dict:
    """Email finding for one lead. Used as column function."""
    lead_id = str(lead["lead_id"])
    website_data = lead.get("_website_data")
    result = find_email(lead, campaign_id, website_data=website_data)
    if result.get("email"):
        lead["email"] = result["email"]
    new_status = _determine_status(lead)
    update_lead_fields(lead_id, {"enrichment_status": new_status})
    return result


def _validation_step(lead: dict, campaign_id: str) -> dict:
    """Email validation for one lead. Used as column function."""
    lead_id = str(lead["lead_id"])
    result = validate_lead_email(lead, campaign_id)
    if result.get("email_verdict"):
        lead["email_verdict"] = result["email_verdict"]
    new_status = _determine_status(lead)
    update_lead_fields(lead_id, {"enrichment_status": new_status})
    return result


def process_batch(campaign_id: str, batch_size: int = None) -> dict:
    """Process a batch of leads through column-batch enrichment.

    Clay-style: all leads go through Column 1 (owner discovery) concurrently,
    then leads needing email go through Column 2, then validation in Column 3.

    Returns {processed, owners_found, emails_found, validated}.
    """
    if batch_size is None:
        batch_size = PROCESS_BATCH_SIZE

    all_leads = get_leads_needing_enrichment(campaign_id, batch_size)
    stats = {"processed": 0, "owners_found": 0, "emails_found": 0, "validated": 0}

    if not all_leads:
        logger.info("No leads needing enrichment")
        return stats

    logger.info(f"Column-batch enrichment: {len(all_leads)} leads")

    # ── Column 1: Owner Discovery ──
    needs_owner = [l for l in all_leads if l.get("owner_status") == "pending"]
    if needs_owner:
        results = _run_column(needs_owner, _owner_step, campaign_id,
                              OWNER_DISCOVERY_WORKERS, "owner_discovery")
        for lead, result in results:
            if result and result.get("owner_name"):
                stats["owners_found"] += 1

    logger.info(f"  Column 1 done: {stats['owners_found']} owners found")

    # ── Column 2: Email Finding ──
    # Re-filter: leads that still have no email after column 1
    needs_email = [l for l in all_leads if not l.get("email")]
    if needs_email:
        results = _run_column(needs_email, _email_step, campaign_id,
                              EMAIL_WATERFALL_WORKERS, "email_finding")
        for lead, result in results:
            if result and result.get("email"):
                stats["emails_found"] += 1

    logger.info(f"  Column 2 done: {stats['emails_found']} emails found")

    # ── Column 3: Email Validation ──
    # Re-filter: leads with email but no verdict
    needs_validation = [l for l in all_leads
                        if l.get("email") and not l.get("email_verdict")]
    if needs_validation:
        results = _run_column(needs_validation, _validation_step, campaign_id,
                              VALIDATION_WORKERS, "email_validation")
        for lead, result in results:
            if result and result.get("email_verdict"):
                stats["validated"] += 1

    logger.info(f"  Column 3 done: {stats['validated']} validated")

    stats["processed"] = len(all_leads)
    logger.info(f"Enrichment batch complete: {stats}")
    return stats


def _determine_status(lead: dict) -> str:
    """Determine enrichment status based on which columns are filled."""
    has_owner = lead.get("owner_status") != "pending"
    has_email = lead.get("email") is not None
    has_verdict = lead.get("email_verdict") is not None

    if has_owner and has_email and has_verdict:
        return "validated"
    elif has_owner and has_email:
        return "enriched"
    elif has_owner:
        return "partial"
    else:
        return "enriching"
