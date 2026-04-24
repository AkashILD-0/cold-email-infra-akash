"""
Push personalized OBGYN email sequences to the Instantly HealthAI campaign.

What this does:
1. Sets up the Instantly campaign (735ef703) with a 3-step email template
   using {{email_1_subject}}, {{email_1_body}}, etc. as per-lead custom variables
2. Re-pushes all SEND leads with their personalized email content as custom
   variables so each lead gets their own email in Instantly
3. Marks sequences as 'synced' when done

Usage:
  python -m tests.push_obgyn_sequences_to_instantly
  python -m tests.push_obgyn_sequences_to_instantly --dry-run   (show stats only)
"""

import argparse
import logging
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

from psycopg2.extras import RealDictCursor
from db import get_connection
from campaigns.instantly_client import InstantlyClient
import requests

OBGYN_CAMPAIGN_ID = "b3fafa6f-623d-4c55-a475-0dc6ddfc5e6e"
INSTANTLY_CAMPAIGN_ID = "735ef703-d8ea-44d1-aa0a-d9356ebfd8eb"


def get_instantly_api_key(campaign_id: str) -> str:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT cl.instantly_api_key
                FROM campaigns c
                JOIN clients cl ON c.client_id = cl.client_id
                WHERE c.campaign_id = %s
            """, (campaign_id,))
            row = cur.fetchone()
            return row["instantly_api_key"] if row else None
    finally:
        conn.close()


def get_leads_with_sequences(campaign_id: str) -> list:
    """Get all SEND leads that have personalized sequences saved in DB."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.lead_id, l.owner_name, l.business_name, l.email,
                       l.website, l.city, l.state, l.email_verdict,
                       es.sequence_id, es.status,
                       es.email_1_subject, es.email_1_body,
                       es.email_2_subject, es.email_2_body,
                       es.email_3_subject, es.email_3_body
                FROM leads l
                JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s
                  AND l.email_verdict = 'SEND'
                  AND l.enrichment_status = 'validated'
                  AND es.status IN ('draft', 'ready')
                ORDER BY l.ingested_at ASC
            """, (campaign_id,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def setup_campaign_sequences(instantly: InstantlyClient, instantly_campaign_id: str) -> bool:
    """
    Set up the Instantly campaign with a 3-step template that uses
    per-lead custom variables for email subject and body.
    This allows each lead to get their own personalized email.
    """
    sequences = [{
        "steps": [
            {
                "type": "email",
                "delay": 0,
                "variants": [{
                    "subject": "{{email_1_subject}}",
                    "body": "{{email_1_body}}"
                }]
            },
            {
                "type": "email",
                "delay": 3,
                "variants": [{
                    "subject": "{{email_2_subject}}",
                    "body": "{{email_2_body}}"
                }]
            },
            {
                "type": "email",
                "delay": 7,
                "variants": [{
                    "subject": "{{email_3_subject}}",
                    "body": "{{email_3_body}}"
                }]
            }
        ]
    }]

    result = instantly.set_campaign_sequences(instantly_campaign_id, sequences)
    if result:
        logger.info("Campaign sequences template set successfully.")
        return True
    else:
        logger.error("Failed to set campaign sequences.")
        return False


def build_email_to_id_map(instantly: InstantlyClient, campaign_id: str) -> dict:
    """Paginate through leads in the campaign and return {email: instantly_lead_id}.
    Filters by campaign_id to avoid matching leads from other campaigns."""
    import time
    mapping = {}
    starting_after = None
    page = 0
    while True:
        body = {"limit": 100, "campaign_id": campaign_id}
        if starting_after:
            body["starting_after"] = starting_after
        retries = 3
        data = None
        for attempt in range(retries):
            try:
                resp = requests.post(
                    f"{InstantlyClient.BASE_URL}/leads/list",
                    headers=instantly._headers(),
                    json=body,
                    timeout=30
                )
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                logger.warning(f"Lead list page {page} attempt {attempt+1} error: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
        if data is None:
            logger.error(f"Failed page {page} after {retries} attempts, stopping.")
            break

        items = data.get("items", [])
        for item in items:
            # Only map leads that belong to our target campaign
            if item.get("campaign") == campaign_id:
                mapping[item["email"]] = item["id"]

        starting_after = data.get("next_starting_after")
        page += 1
        if not starting_after or not items:
            break

    return mapping


def add_lead_to_instantly(instantly: InstantlyClient, instantly_campaign_id: str, lead: dict) -> bool:
    """POST a new lead to Instantly with email content as custom variables."""
    try:
        resp = requests.post(
            f"{InstantlyClient.BASE_URL}/leads",
            headers=instantly._headers(),
            json={
                "campaign_id": instantly_campaign_id,
                "email": lead.get("email", ""),
                "custom_variables": {
                    "email_1_subject": lead.get("email_1_subject", ""),
                    "email_1_body":    lead.get("email_1_body", ""),
                    "email_2_subject": lead.get("email_2_subject", ""),
                    "email_2_body":    lead.get("email_2_body", ""),
                    "email_3_subject": lead.get("email_3_subject", ""),
                    "email_3_body":    lead.get("email_3_body", ""),
                    "city":            lead.get("city", ""),
                    "state":           lead.get("state", ""),
                    "business_name":   lead.get("business_name", ""),
                },
            },
            timeout=30
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"POST error ({lead.get('email')}): {e}")
        return False


def patch_lead_sequences(instantly: InstantlyClient, lead_id: str, lead: dict) -> bool:
    """PATCH an existing Instantly lead with personalized email content.
    Must use custom_variables nested key — flat keys are silently ignored by Instantly."""
    try:
        resp = requests.patch(
            f"{InstantlyClient.BASE_URL}/leads/{lead_id}",
            headers=instantly._headers(),
            json={
                "custom_variables": {
                    "email_1_subject": lead.get("email_1_subject", ""),
                    "email_1_body": lead.get("email_1_body", ""),
                    "email_2_subject": lead.get("email_2_subject", ""),
                    "email_2_body": lead.get("email_2_body", ""),
                    "email_3_subject": lead.get("email_3_subject", ""),
                    "email_3_body": lead.get("email_3_body", ""),
                    "city": lead.get("city", ""),
                    "state": lead.get("state", ""),
                    "business_name": lead.get("business_name", ""),
                }
            },
            timeout=30
        )
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"PATCH error ({lead.get('email')}): {e}")
        return False


def mark_synced(lead_ids: list, campaign_id: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE email_sequences SET status = 'synced'
                WHERE lead_id = ANY(%s::uuid[])
                  AND campaign_id = %s
            """, (lead_ids, campaign_id))
        conn.commit()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show counts only, don't push to Instantly")
    parser.add_argument("--campaign", default=OBGYN_CAMPAIGN_ID)
    parser.add_argument("--instantly-campaign", default=INSTANTLY_CAMPAIGN_ID)
    args = parser.parse_args()

    api_key = get_instantly_api_key(args.campaign)
    if not api_key:
        print("ERROR: Could not get Instantly API key from DB.")
        sys.exit(1)

    instantly = InstantlyClient(api_key)

    print("Fetching leads with sequences from DB...")
    leads = get_leads_with_sequences(args.campaign)
    print(f"Found {len(leads)} SEND leads with sequences.")

    draft = [l for l in leads if l.get("status") == "draft"]
    synced = [l for l in leads if l.get("status") == "synced"]
    print(f"  Draft (not yet pushed):  {len(draft)}")
    print(f"  Synced (pushed before):  {len(synced)}")
    print(f"  Total to push:           {len(leads)}")

    if args.dry_run:
        print("\nDry run complete. No changes made.")
        return

    # Step 1: Set up campaign sequences template
    print("\nSetting up Instantly campaign sequences template...")
    ok = setup_campaign_sequences(instantly, args.instantly_campaign)
    if not ok:
        print("ERROR: Could not set campaign sequences. Aborting.")
        sys.exit(1)

    # Step 2: Build email → Instantly UUID mapping
    print(f"\nFetching Instantly lead IDs (paginating campaign {args.instantly_campaign})...")
    email_to_id = build_email_to_id_map(instantly, args.instantly_campaign)
    print(f"  Found {len(email_to_id)} leads in campaign.")

    # Step 3: PATCH existing leads or POST new ones
    print(f"\nPushing {len(leads)} leads to Instantly...")
    patched = 0
    added = 0
    errors = 0
    synced_ids = []

    for i, lead in enumerate(leads, 1):
        lead_email = lead.get("email", "")
        instantly_id = email_to_id.get(lead_email)
        if instantly_id:
            success = patch_lead_sequences(instantly, instantly_id, lead)
            if success:
                patched += 1
                synced_ids.append(str(lead["lead_id"]))
            else:
                errors += 1
        else:
            success = add_lead_to_instantly(instantly, args.instantly_campaign, lead)
            if success:
                added += 1
                synced_ids.append(str(lead["lead_id"]))
            else:
                errors += 1

        if i % 50 == 0 or i == len(leads):
            print(f"  [{i}/{len(leads)}] Patched: {patched}, Added: {added}, Errors: {errors}")

    # Step 3: Mark all successfully pushed sequences as synced
    if synced_ids:
        mark_synced(synced_ids, args.campaign)
        print(f"\nMarked {len(synced_ids)} sequences as synced in DB.")

    print(f"\nDone. {patched} patched, {added} added new, {errors} errors.")
    print(f"Review at: https://app.instantly.ai/campaign/{args.instantly_campaign}")


if __name__ == "__main__":
    main()