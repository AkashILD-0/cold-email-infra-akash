"""
Personalized OBGYN email batch generator — 10 leads at a time.

Usage:
  # Preview batch (no DB changes):
  python -m tests.personalize_obgyn_batch

  # Paginate:
  python -m tests.personalize_obgyn_batch --offset 10
  python -m tests.personalize_obgyn_batch --offset 20

  # Save to DB after reviewing:
  python -m tests.personalize_obgyn_batch --offset 0 --save

Fetches 10 SEND leads from the OBGYN campaign (b3fafa6f), scrapes each practice
website, generates personalized 3-email sequences, and displays them for review.
Does NOT save to DB unless --save is passed.
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.WARNING)

from db import get_connection, get_campaign_brief
from psycopg2.extras import RealDictCursor
from generation.email_generator import generate_personalized_sequence

OBGYN_CAMPAIGN_ID = "b3fafa6f-623d-4c55-a475-0dc6ddfc5e6e"
BATCH_SIZE = 10
DIVIDER = "-" * 72


def fetch_batch(campaign_id: str, offset: int, limit: int) -> list:
    """Fetch SEND leads from campaign, ordered by ingested_at.
    Includes leads with or without existing sequences (for replacement)."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.lead_id, l.owner_name, l.business_name, l.email,
                       l.website, l.business_domain, l.city, l.state,
                       l.email_verdict, es.sequence_id
                FROM leads l
                LEFT JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s
                  AND l.email_verdict = 'SEND'
                ORDER BY l.ingested_at ASC
                LIMIT %s OFFSET %s
            """, (campaign_id, limit, offset))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def overwrite_sequence(lead_id: str, campaign_id: str, sequences: dict):
    """Delete existing sequence (if any) and insert the new one."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM email_sequences WHERE lead_id = %s AND campaign_id = %s",
                (lead_id, campaign_id)
            )
            cur.execute("""
                INSERT INTO email_sequences (
                    lead_id, campaign_id,
                    email_1_subject, email_1_body,
                    email_2_subject, email_2_body,
                    email_3_subject, email_3_body
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                lead_id, campaign_id,
                sequences.get("email_1_subject"), sequences.get("email_1_body"),
                sequences.get("email_2_subject"), sequences.get("email_2_body"),
                sequences.get("email_3_subject"), sequences.get("email_3_body"),
            ))
        conn.commit()
    finally:
        conn.close()


def display_result(index: int, lead: dict, sequences: dict,
                   website_insights: str, was_revised: bool):
    """Print a single lead's personalized sequence in a readable format."""
    name = lead.get("owner_name") or lead.get("business_name") or lead.get("email", "Unknown")
    practice = lead.get("business_name", "")
    location_parts = [p for p in [lead.get("city"), lead.get("state")] if p]
    location = ", ".join(location_parts) if location_parts else "Unknown location"
    website = lead.get("website") or (
        f"https://{lead['business_domain']}" if lead.get("business_domain")
        else (f"https://{lead['email'].split('@')[1]}" if lead.get("email") and "@" in lead["email"] else "N/A")
    )

    has_existing = bool(lead.get("sequence_id"))
    status_tag = " [REPLACING existing]" if has_existing else " [NEW]"
    revised_tag = " (revised by Haiku)" if was_revised else ""

    print(f"\n{DIVIDER}")
    print(f"  Lead {index}: {name}  |  {practice}  |  {location}{status_tag}{revised_tag}")
    print(f"  Website: {website}")
    print(DIVIDER)

    if website_insights and "No distinctive details" not in website_insights:
        print("  WEBSITE INSIGHTS:")
        for line in website_insights.strip().splitlines():
            print(f"    {line}")
        print()
    else:
        print("  WEBSITE INSIGHTS: None found (generic email will be used)\n")

    if sequences:
        for i in range(1, 4):
            subject = sequences.get(f"email_{i}_subject", "(missing)")
            body = sequences.get(f"email_{i}_body", "(missing)")
            labels = {1: "Initial outreach", 2: "Allegiance follow-up", 3: "Break-up"}
            print(f"  EMAIL {i} ({labels[i]})  --  Subject: \"{subject}\"")
            print()
            for line in body.strip().splitlines():
                print(f"    {line}")
            print()
    else:
        print("  ERROR: Sequence generation failed for this lead.\n")


def main():
    parser = argparse.ArgumentParser(
        description="Generate personalized OBGYN email batches (10 at a time)"
    )
    parser.add_argument("--offset", type=int, default=0,
                        help="Which lead to start from (default: 0)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE,
                        help=f"How many leads to process (default: {BATCH_SIZE})")
    parser.add_argument("--save", action="store_true",
                        help="Save generated sequences to DB (overwrites existing)")
    parser.add_argument("--campaign", default=OBGYN_CAMPAIGN_ID,
                        help="Campaign ID (default: OBGYN b3fafa6f)")
    args = parser.parse_args()

    print(f"\nFetching {args.batch_size} OBGYN SEND leads (offset {args.offset})...")
    leads = fetch_batch(args.campaign, args.offset, args.batch_size)

    if not leads:
        print("No more leads found at this offset.")
        return

    print(f"Fetched {len(leads)} leads. Loading campaign brief...")
    brief = get_campaign_brief(args.campaign)

    if not brief:
        print("ERROR: No campaign brief found. Run the campaign setup first.")
        sys.exit(1)

    results = []
    errors = 0

    for i, lead in enumerate(leads, start=1):
        lead_name = lead.get("owner_name") or lead.get("business_name") or lead.get("email")
        print(f"\n[{i}/{len(leads)}] Processing: {lead_name}...")

        sequences, website_insights, was_revised = generate_personalized_sequence(
            lead=dict(lead),
            brief=dict(brief),
            campaign_id=args.campaign
        )

        results.append((lead, sequences, website_insights, was_revised))

        if sequences:
            print(f"  Generated (revised={was_revised}). Insights: {'Yes' if website_insights and 'No distinctive' not in website_insights else 'None'}")
        else:
            errors += 1
            print(f"  FAILED to generate sequence.")

    # Display all results
    print(f"\n\n{'=' * 72}")
    print(f"  BATCH PREVIEW  |  Offset {args.offset}-{args.offset + len(leads) - 1}  |  Errors: {errors}")
    print(f"{'=' * 72}")

    for i, (lead, sequences, website_insights, was_revised) in enumerate(results, start=1):
        display_result(i, lead, sequences, website_insights, was_revised)

    print(f"\n{DIVIDER}")
    print(f"  END OF BATCH — {len(leads)} leads processed, {errors} errors")
    print(f"{DIVIDER}\n")

    if args.save:
        print("Saving sequences to DB...")
        saved = 0
        for lead, sequences, _, _ in results:
            if sequences:
                overwrite_sequence(str(lead["lead_id"]), args.campaign, sequences)
                saved += 1
        print(f"Saved {saved}/{len(results)} sequences. Next batch: --offset {args.offset + len(leads)}")
    else:
        print("Preview only — no DB changes made.")
        print(f"To save: python -m tests.personalize_obgyn_batch --offset {args.offset} --save")
        print(f"Next batch: python -m tests.personalize_obgyn_batch --offset {args.offset + len(leads)}")


if __name__ == "__main__":
    main()
