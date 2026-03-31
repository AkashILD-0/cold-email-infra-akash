import logging
from enrichment.owner_discovery import discover_owner
from enrichment.email_waterfall import find_email
from validation.cascade_validator import validate_lead_email
from db import get_leads_needing_enrichment, update_lead_fields
from config import PROCESS_BATCH_SIZE

logger = logging.getLogger(__name__)


def process_batch(campaign_id: str, batch_size: int = None) -> dict:
    """Process a batch of leads through the enrichment pipeline.
    Returns {processed, owners_found, emails_found, validated}."""

    if batch_size is None:
        batch_size = PROCESS_BATCH_SIZE

    leads = get_leads_needing_enrichment(campaign_id, batch_size)
    stats = {"processed": 0, "owners_found": 0, "emails_found": 0, "validated": 0}

    for lead in leads:
        lead_id = str(lead["lead_id"])

        try:
            # Mark as enriching
            update_lead_fields(lead_id, {"enrichment_status": "enriching"})

            website_data = None

            # Step A: Owner discovery (if needed)
            if lead.get("owner_status") == "pending":
                owner_result = discover_owner(lead, campaign_id)
                if owner_result.get("owner_name"):
                    stats["owners_found"] += 1
                    lead["owner_name"] = owner_result["owner_name"]
                lead["owner_status"] = owner_result.get("owner_status", "not_found")
                # Carry website_data forward to avoid re-scraping
                website_data = owner_result.get("_website_data")

            # Step B: Email finding (if needed)
            if not lead.get("email"):
                email_result = find_email(lead, campaign_id, website_data=website_data)
                if email_result.get("email"):
                    stats["emails_found"] += 1
                    # Update lead dict for validation step
                    lead["email"] = email_result["email"]

            # Step C: Email validation (if email exists but no verdict)
            if lead.get("email") and not lead.get("email_verdict"):
                val_result = validate_lead_email(lead, campaign_id)
                if val_result.get("email_verdict"):
                    stats["validated"] += 1
                    lead["email_verdict"] = val_result["email_verdict"]

            # Update enrichment status — re-read lead to get current state
            new_status = _determine_status(lead)
            update_lead_fields(lead_id, {"enrichment_status": new_status})

            stats["processed"] += 1

        except Exception as e:
            logger.error(f"Enrichment error for lead {lead_id}: {e}")
            update_lead_fields(lead_id, {"enrichment_status": "error"})

    logger.info(f"Enrichment batch: {stats}")
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
