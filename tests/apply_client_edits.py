"""
Apply 24 client-provided before→after text edits to 5 OBGYN lead sequences,
update the DB, and re-push those 5 leads to Instantly.

Usage:
  python -m tests.apply_client_edits           # apply + push
  python -m tests.apply_client_edits --dry-run  # show what would change, no writes
"""

import os
import sys
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.WARNING)

import argparse
import requests
from psycopg2.extras import RealDictCursor
from db import get_connection
from campaigns.instantly_client import InstantlyClient

OBGYN_CAMPAIGN_ID = "b3fafa6f-623d-4c55-a475-0dc6ddfc5e6e"
INSTANTLY_CAMPAIGN_ID = "735ef703-d8ea-44d1-aa0a-d9356ebfd8eb"

# Each entry: (field, before_text, after_text)
EDITS = {
    "Altura": [
        ("email_1_body",
         "Most OBGYN practices lose 15-20% of their revenue to no-shows and missed care windows. The manual outreach to prevent this burns out staff who should be focusing on patient care instead.",
         "Most OB practices that size struggle with managing patient demand \u2014 from inbound calls and scheduling requests to follow-ups \u2014 while staff spend hours on manual outreach and phone work instead of patient care."),
        ("email_2_body",
         "The outcome? Fewer no-shows, better patient outcomes, and your staff can focus on clinical work instead of chasing down patients.",
         "The outcome? More captured demand, better patient outcomes, and your staff can focus on clinical work instead of chasing down patients."),
        ("email_3_body",
         "if no-shows and staff burnout from manual outreach are still issues, this could change how your clinic operates.",
         "if managing patient demand and reducing staff phone workload are still challenges, this could change how your clinic operates."),
    ],
    "Aguirre": [
        ("email_1_body",
         "Most OBGYN practices I work with struggle with the same challenge: patients who need follow-up care or preventive visits simply don't schedule them, leaving revenue on the table.",
         "Most specialty practices like Aguirre struggle with the same challenge: patient hesitation and missed opportunities, especially when inbound inquiries or consult requests aren't captured or followed up quickly."),
        ("email_1_body",
         "Practices typically see 15-25% more patient encounters without adding staff.",
         "Practices typically see more captured demand, more scheduled procedures, and meaningful revenue growth without adding staff."),
        ("email_2_body",
         "For a specialized practice like Aguirre, the impact on follow-up scheduling and preventive care could be significant.",
         "For a specialty practice like yours, the impact on capturing and converting high value cosmetic and elective procedures could be even greater."),
        ("email_3_body",
         "But if you're curious how other specialty practices are booking 15-25% more visits automatically, I'm happy to show you.",
         "But if you're curious how other specialty practices are capturing more inbound interest and converting more patients into scheduled procedures, I'm happy to show you."),
    ],
    "St Clair": [
        ("email_1_subject",
         "patient no-shows at st clair",
         "capturing more patient demand"),
        ("email_1_body",
         "OBGYN practices typically see 15-20% no-show rates, which hits harder when you're managing prenatal appointments and time-sensitive care.",
         "OBGYN practices in Michigan are dealing with rising patient demand, inbound scheduling requests, and gaps in converting that demand into visits, especially for prenatal appointments and time-sensitive care."),
        ("email_1_body",
         "Practices using our platform see 40% fewer no-shows and 25% more patient encounters without adding staff time.",
         "Most practices book 2 to 3 additional appointments per day while reducing front desk workload and phone volume."),
        ("email_3_body",
         "Should I stop reaching out, or does a quick 15-minute demo make sense?",
         "Should I stop reaching out, or would you like to see that 15-minute demo of how we're helping OBGYN practices capture more patient demand, book more appointments, and reduce front-end workload?"),
    ],
    "Nezhat": [
        ("email_1_body",
         "Most high-volume specialty practices like yours lose 15-20% of potential revenue to no-shows and missed follow-ups \u2014 especially challenging with oncology patients who need consistent care coordination.",
         "Most specialty practices I work with struggle to manage inbound demand, surgical scheduling coordination, and last-minute changes efficiently \u2014 especially challenging with oncology patients who need consistent care coordination."),
        ("email_1_body",
         "HealthTalk A.I. automates patient outreach and appointment scheduling for OBGYN practices, reducing staff workload while ensuring critical follow-ups never fall through the cracks.",
         "HealthTalk A.I. manages the full patient access lifecycle, capturing inbound calls and requests, automating scheduling, and coordinating pre and post-op engagement to keep your schedule full. All HIPAA-compliant and integrates with your existing EHR."),
    ],
    "Eastern CT": [
        ("email_1_subject",
         "patient no-shows hurting revenue?",
         "missed patient demand costing you"),
        ("email_1_body",
         "Most OBGYN practices I talk to are losing 15-20% of potential revenue to patient no-shows and missed follow-ups.",
         "OBGYN practices lose significant revenue from missed demand, whether it is no-shows, missed calls, or unconverted scheduling requests."),
        ("email_1_body",
         "Practices using our platform see fewer no-shows, more consistent appointment schedules, and measurable revenue growth without adding staff.",
         "Practices using our platform see more annual visits, stronger patient retention, and hours of front desk workload eliminated each day."),
        ("email_2_body",
         "Following up on my last email about reducing patient no-shows.",
         "Following up on my last email about capturing more patient demand."),
        ("email_3_body",
         "Most OBGYN practices are losing $30K+ annually to preventable no-shows and missed care opportunities.",
         "Most OBGYN practices are missing significant revenue from unconverted demand, missed inbound calls, and scheduling gaps."),
    ],
}

# Map label → DB ILIKE pattern
LEAD_PATTERNS = {
    "Altura":     "%Altura%",
    "Aguirre":    "%Aguirre%",
    "St Clair":   "%St Clair%",
    "Nezhat":     "%Nezhat%",
    "Eastern CT": "%Eastern Connecticut%",
}


def fetch_lead_sequence(campaign_id, pattern):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.lead_id, l.business_name, l.email,
                       l.city, l.state,
                       es.sequence_id,
                       es.email_1_subject, es.email_1_body,
                       es.email_2_subject, es.email_2_body,
                       es.email_3_subject, es.email_3_body
                FROM leads l
                JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s
                  AND l.business_name ILIKE %s
                  AND l.email_verdict = 'SEND'
                LIMIT 1
            """, (campaign_id, pattern))
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def apply_edits(row, edits):
    updated = dict(row)
    applied = []
    for field, before, after in edits:
        current = updated.get(field) or ""
        if before in current:
            updated[field] = current.replace(before, after, 1)
            applied.append(f"  [OK] {field}: replaced snippet")
        else:
            applied.append(f"  [MISS] {field}: text not found — '{before[:60]}...'")
    return updated, applied


def save_sequence(row):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE email_sequences
                SET email_1_subject = %s,
                    email_1_body    = %s,
                    email_2_subject = %s,
                    email_2_body    = %s,
                    email_3_subject = %s,
                    email_3_body    = %s
                WHERE sequence_id = %s
            """, (
                row["email_1_subject"], row["email_1_body"],
                row["email_2_subject"], row["email_2_body"],
                row["email_3_subject"], row["email_3_body"],
                row["sequence_id"],
            ))
        conn.commit()
    finally:
        conn.close()


def get_instantly_api_key(campaign_id):
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


def build_email_to_id_map(instantly, campaign_id):
    mapping = {}
    starting_after = None
    while True:
        body = {"limit": 100, "campaign_id": campaign_id}
        if starting_after:
            body["starting_after"] = starting_after
        resp = requests.post(
            f"{InstantlyClient.BASE_URL}/leads/list",
            headers=instantly._headers(),
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("items", []):
            if item.get("campaign") == campaign_id:
                mapping[item["email"]] = item["id"]
        starting_after = data.get("next_starting_after")
        if not starting_after or not data.get("items"):
            break
    return mapping


def patch_lead(instantly, instantly_id, row):
    resp = requests.patch(
        f"{InstantlyClient.BASE_URL}/leads/{instantly_id}",
        headers=instantly._headers(),
        json={"custom_variables": {
            "email_1_subject": row.get("email_1_subject", ""),
            "email_1_body":    row.get("email_1_body", ""),
            "email_2_subject": row.get("email_2_subject", ""),
            "email_2_body":    row.get("email_2_body", ""),
            "email_3_subject": row.get("email_3_subject", ""),
            "email_3_body":    row.get("email_3_body", ""),
        }},
        timeout=30,
    )
    resp.raise_for_status()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("HTAI OBGYN — Applying 24 client edits to 5 leads")
    print("=" * 60)

    updated_rows = []

    for label, edits in EDITS.items():
        pattern = LEAD_PATTERNS[label]
        row = fetch_lead_sequence(OBGYN_CAMPAIGN_ID, pattern)
        if not row:
            print(f"\n[{label}] NOT FOUND in DB — skipping")
            continue

        print(f"\n[{label}] Found: {row['business_name']} ({row['email']})")
        updated, log = apply_edits(row, edits)
        for line in log:
            print(line)

        if not args.dry_run:
            save_sequence(updated)
            print(f"  DB updated.")
        updated_rows.append(updated)

    if args.dry_run:
        print("\nDry run complete — no changes written.")
        return

    print("\n" + "=" * 60)
    print("Re-pushing 5 leads to Instantly...")
    api_key = get_instantly_api_key(OBGYN_CAMPAIGN_ID)
    if not api_key:
        print("ERROR: no Instantly API key found in DB.")
        sys.exit(1)

    instantly = InstantlyClient(api_key)
    print(f"Building email->ID map for campaign {INSTANTLY_CAMPAIGN_ID}...")
    email_to_id = build_email_to_id_map(instantly, INSTANTLY_CAMPAIGN_ID)
    print(f"  {len(email_to_id)} leads mapped.")

    pushed = 0
    for row in updated_rows:
        email = row.get("email", "")
        instantly_id = email_to_id.get(email)
        if not instantly_id:
            print(f"  [NOT FOUND in Instantly] {email}")
            continue
        try:
            patch_lead(instantly, instantly_id, row)
            print(f"  [PATCHED] {row['business_name']} ({email})")
            pushed += 1
        except Exception as e:
            print(f"  [ERROR] {email}: {e}")

    print(f"\nDone. {pushed}/{len(updated_rows)} leads re-pushed to Instantly.")
    print(f"Review: https://app.instantly.ai/campaign/{INSTANTLY_CAMPAIGN_ID}")


if __name__ == "__main__":
    main()
