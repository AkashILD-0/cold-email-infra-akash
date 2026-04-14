#!/usr/bin/env python3
"""
Re-sync OBGYN campaign (b3fafa6f) to HealthAI Instantly workspace.

The campaign was previously synced to the wrong account (My Organization).
This script:
  1. Updates the client's instantly_api_key in DB to the HealthAI key
  2. Clears the old instantly_campaign_id so launcher creates a new one
  3. Resets email_sequences back to 'draft' so launcher can pick them up
  4. Calls launch_campaign() which will now create the campaign in HealthAI
"""

import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("resync_obgyn")

OBGYN_CAMPAIGN_ID = "b3fafa6f-623d-4c55-a475-0dc6ddfc5e6e"
HEALTHAI_API_KEY = "NTgwYTM0MmMtMzBjNC00YTliLWJhZGEtMzcwOGIzZTI0ZmRiOnRmSGt6TkptVHB2TQ=="


def main():
    from db import get_connection
    from campaigns.campaign_launcher import launch_campaign

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # 1. Update client API key to HealthAI
            cur.execute("""
                UPDATE clients
                SET instantly_api_key = %s
                WHERE client_id = (
                    SELECT client_id FROM campaigns WHERE campaign_id = %s
                )
            """, (HEALTHAI_API_KEY, OBGYN_CAMPAIGN_ID))
            logger.info(f"  Updated client API key — {cur.rowcount} row(s)")

            # 2. Clear old Instantly campaign ID
            cur.execute("""
                UPDATE campaigns SET instantly_campaign_id = NULL
                WHERE campaign_id = %s
            """, (OBGYN_CAMPAIGN_ID,))
            logger.info(f"  Cleared instantly_campaign_id — {cur.rowcount} row(s)")

            # 3. Reset sequences to draft
            cur.execute("""
                UPDATE email_sequences SET status = 'draft'
                WHERE campaign_id = %s
            """, (OBGYN_CAMPAIGN_ID,))
            logger.info(f"  Reset {cur.rowcount} sequences to 'draft'")

            conn.commit()
    finally:
        conn.close()

    # 4. Sync to HealthAI
    logger.info("  Launching campaign → HealthAI workspace...")
    result = launch_campaign(OBGYN_CAMPAIGN_ID)
    logger.info(f"  Sync result: {result}")

    # 5. Check new Instantly campaign ID
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT instantly_campaign_id FROM campaigns WHERE campaign_id = %s",
                (OBGYN_CAMPAIGN_ID,)
            )
            row = cur.fetchone()
            if row and row[0]:
                logger.info(f"  New Instantly campaign ID: {row[0]}")
            else:
                logger.warning("  No Instantly campaign ID — sync may have failed.")
    finally:
        conn.close()

    return result


if __name__ == "__main__":
    main()
