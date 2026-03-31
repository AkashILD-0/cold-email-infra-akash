import logging
from campaigns.instantly_client import InstantlyClient
from campaigns.client_manager import get_client_api_key
from db import get_connection, update_lead_fields
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


def launch_campaign(campaign_id: str) -> dict:
    """Sync validated leads with email sequences to Instantly.
    Returns {synced, errors}."""

    # Get campaign + client info
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, cl.instantly_api_key, cl.sending_domains
                FROM campaigns c
                JOIN clients cl ON c.client_id = cl.client_id
                WHERE c.campaign_id = %s
            """, (campaign_id,))
            campaign = cur.fetchone()
    finally:
        conn.close()

    if not campaign:
        logger.error(f"Campaign {campaign_id} not found")
        return {"synced": 0, "errors": 0}

    # Initialize Instantly client with this client's API key
    instantly = InstantlyClient(campaign["instantly_api_key"])

    # Create Instantly campaign if not exists
    if not campaign.get("instantly_campaign_id"):
        result = instantly.create_campaign(campaign["campaign_name"])
        instantly_campaign_id = result.get("id")
        if not instantly_campaign_id:
            logger.error("Failed to create Instantly campaign")
            return {"synced": 0, "errors": 1}

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE campaigns SET instantly_campaign_id = %s WHERE campaign_id = %s",
                    (instantly_campaign_id, campaign_id)
                )
                conn.commit()
        finally:
            conn.close()
    else:
        instantly_campaign_id = campaign["instantly_campaign_id"]

    # Get ready leads (SEND verdict + email sequence ready)
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.*, es.email_1_subject, es.email_1_body,
                       es.email_2_subject, es.email_2_body,
                       es.email_3_subject, es.email_3_body
                FROM leads l
                JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s
                  AND l.email_verdict = 'SEND'
                  AND es.status = 'draft'
                  AND l.enrichment_status = 'validated'
            """, (campaign_id,))
            leads = cur.fetchall()
    finally:
        conn.close()

    if not leads:
        logger.info(f"No leads ready to sync for campaign {campaign_id}")
        return {"synced": 0, "errors": 0}

    # Format leads for Instantly
    instantly_leads = []
    for lead in leads:
        first_name = (lead.get("owner_name") or "").split(" ")[0]
        instantly_leads.append({
            "email": lead["email"],
            "first_name": first_name,
            "last_name": " ".join((lead.get("owner_name") or "").split(" ")[1:]),
            "company_name": lead.get("business_name", ""),
            "custom_variables": {
                "business_name": lead.get("business_name", ""),
                "city": lead.get("city", ""),
                "state": lead.get("state", ""),
                "website": lead.get("website", ""),
            }
        })

    # Sync to Instantly
    stats = {"synced": 0, "errors": 0}

    # Add leads in batches of 100
    for i in range(0, len(instantly_leads), 100):
        batch = instantly_leads[i:i+100]
        result = instantly.add_leads_to_campaign(instantly_campaign_id, batch)
        if result:
            stats["synced"] += len(batch)
        else:
            stats["errors"] += len(batch)

    # Mark sequences as synced
    if stats["synced"] > 0:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                lead_ids = [str(l["lead_id"]) for l in leads[:stats["synced"]]]
                cur.execute("""
                    UPDATE email_sequences SET status = 'synced'
                    WHERE lead_id = ANY(%s)
                """, (lead_ids,))
                conn.commit()
        finally:
            conn.close()

    logger.info(f"Campaign launch: {stats}")
    return stats
