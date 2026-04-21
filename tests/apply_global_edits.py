"""
Apply campaign-wide messaging fixes to all OBGYN leads:
- Remove inflated no-show % claims (15-20%, 40%, 25%, 15-25%, 30-40%)
- Shift no-show framing to patient demand / access language
- Fix subject lines that lead with "patient no-shows"
- Re-push all affected leads to Instantly

Usage:
  python -m tests.apply_global_edits --dry-run   # preview counts only
  python -m tests.apply_global_edits              # apply + push to Instantly
"""

import os
import sys
import re
import time
import argparse
import requests
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from psycopg2.extras import RealDictCursor
from db import get_connection
from campaigns.instantly_client import InstantlyClient

OBGYN_CAMPAIGN_ID   = "b3fafa6f-623d-4c55-a475-0dc6ddfc5e6e"
INSTANTLY_CAMPAIGN_ID = "735ef703-d8ea-44d1-aa0a-d9356ebfd8eb"

# ---------------------------------------------------------------------------
# Ordered list of (pattern, replacement) applied to every email body/subject.
# More specific patterns come first to avoid partial double-replacements.
# ---------------------------------------------------------------------------
BODY_REPLACEMENTS = [
    # Full-sentence: "Practices using our platform see X% fewer no-shows and Y% more encounters..."
    (r"Practices using our platform see \d+[\-\u2013]?\d*% fewer no-shows and \d+% more (?:annual )?(?:patient )?(?:visits?|encounters?) without adding (?:staff|headcount)(?: workload)?[.]?",
     "Practices using our platform see more annual visits, stronger patient retention, and hours of front desk workload eliminated each day."),

    # Full-sentence: "Practices using it see 30-40% fewer no-shows without adding staff workload."
    (r"Practices using (?:our platform|it) see \d+[\-\u2013]?\d*% fewer no-shows without adding (?:staff|headcount)(?: workload)?[.]?",
     "Practices using our platform capture more patient demand and reduce front desk workload without adding staff."),

    # Full-sentence: "Most practices see X% fewer no-shows within the first month."
    (r"Most practices see \d+[\-\u2013]?\d*% fewer no-shows within the first month[.]?",
     "Most practices book 2 to 3 additional appointments per day while reducing front desk workload and phone volume."),

    # Full-sentence: "Practices typically see 30-40% fewer no-shows within the first month."
    (r"Practices typically see \d+[\-\u2013]?\d*% fewer no-shows within the first month[.]?",
     "Practices typically book 2 to 3 additional appointments per day while reducing front desk workload and phone volume."),

    # Full-sentence: "We help OBGYN practices reduce no-shows by X% while..."
    (r"We help (?:OBGYN|OB/GYN|reproductive medicine|medical) practices reduce (?:no-shows?|missed appointments?) by (?:up to )?\d+[\-\u2013]?\d*%[^.]*\.",
     "We help practices capture more patient demand, reduce front-end staff burden, and convert inbound requests into scheduled visits."),

    # Full-sentence: "Most OBGYN groups lose 30-40% of postpartum appointments..."
    (r"Most OBGYN groups lose \d+[\-\u2013]?\d*% of postpartum appointments[^.]*\.",
     "Most OBGYN groups struggle to capture and re-engage postpartum patients before they disengage from care."),

    # "$200 per no-show" stat
    (r"(?:lose an average of )?\$200 per no-show appointment[^.]*\.",
     "OBGYN practices lose significant revenue from missed demand, whether it is no-shows, missed calls, or unconverted scheduling requests."),

    # "OBGYN practices lose 15-20% of potential visits to scheduling friction"
    (r"(?:Most )?OBGYN practices lose \d+[\-\u2013]?\d*% of potential visits to [^.]+\.",
     "OBGYN practices lose significant revenue from missed demand, whether it is no-shows, missed calls, or unconverted scheduling requests."),

    # "OBGYN practices lose 15-20% revenue to no-shows..."
    (r"(?:Most )?OBGYN practices lose \d+[\-\u2013]?\d*% revenue to no-shows[^.]*\.",
     "OBGYN practices lose significant revenue from missed demand, whether it is no-shows, missed calls, or unconverted scheduling requests."),

    # "practices are dealing with 15-20% no-show rates" (various lead-specific openers)
    (r"(?:OBGYN practices(?: in [^-]+)?|(?:Running|Managing) a [\w\s]+ practice)[^.]*\b\d+[\-\u2013]?\d*%\s+no-show(?: rate)?s?[^.]*\.",
     "OBGYN practices are dealing with rising patient demand, inbound scheduling requests, and gaps in converting that demand into scheduled visits."),

    # "are seeing 15-20% no-show rates" inline
    (r"(?:are )?seeing \d+[\-\u2013]?\d*% no-show(?: rate)?s?",
     "are dealing with significant missed patient demand"),

    # "reduce no-shows by X%" inline
    (r"reduce(?:d)?\s+no-shows?\s+by\s+(?:up\s+to\s+)?\d+[\-\u2013]?\d*\s*%",
     "capture more patient demand"),

    # "cut no-shows / cut their no-show rate by X%"
    (r"cut(?:ting)?\s+(?:their\s+)?no-show(?:s|\s+rate)?\s+by\s+(?:about\s+)?\d+[\-\u2013]?\d*\s*%",
     "capture more inbound demand"),

    # "reduce missed appointments by up to X%"
    (r"reduce\s+missed\s+appointments?\s+by\s+(?:up\s+to\s+)?\d+[\-\u2013]?\d*\s*%",
     "reduce missed scheduling opportunities"),

    # "X% fewer no-shows" — residual inline replacements
    (r"\d+[\-\u2013]?\d*%\s+fewer\s+no-shows?",
     "more captured patient demand"),

    # "X% no-show rates" inline
    (r"\d+[\-\u2013]?\d*%\s+no-show(?: rate)?s?",
     "significant missed patient demand"),

    # "and X% more annual visits / patient encounters" inline
    (r"and\s+\d+[\-\u2013]?\d*%\s+more\s+(?:annual\s+)?(?:patient\s+)?(?:visits?|encounters?)",
     "and more patient encounters"),

    # standalone "X% more annual visits / patient encounters"
    (r"\d+[\-\u2013]?\d*%\s+more\s+(?:annual\s+)?(?:patient\s+)?(?:visits?|encounters?)",
     "more patient encounters"),

    # "15-25% more encounters" inline
    (r"\d+[\-\u2013]?\d*%\s+more\s+encounters?",
     "more patient encounters"),

    # "no-show rate drop by X%"
    (r"(?:no-show|missed appointment)\s+rate\s+drop\s+by\s+\d+[\-\u2013]?\d*%",
     "inbound demand captured more consistently"),

    # "missed appointment rate drop by X%"
    (r"missed\s+appointment\s+rate\s+drop\s+by\s+\d+[\-\u2013]?\d*%",
     "scheduling gaps close faster"),
]

SUBJECT_REPLACEMENTS = [
    (r"patient no-shows(?: at | costing | hurting |\?)",
     "capturing more patient demand"),
    (r"no-show appointments?",
     "patient demand"),
    (r"no-shows costing",
     "missed demand costing"),
    (r"patient no-shows$",
     "patient demand management"),
]


def apply_replacements(text, patterns):
    if not text:
        return text, 0
    count = 0
    for pat, repl in patterns:
        new_text, n = re.subn(pat, repl, text, flags=re.IGNORECASE)
        count += n
        text = new_text
    return text, count


def fetch_all_sequences(campaign_id):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.lead_id, l.email, l.business_name, l.city, l.state,
                       es.sequence_id,
                       es.email_1_subject, es.email_1_body,
                       es.email_2_subject, es.email_2_body,
                       es.email_3_subject, es.email_3_body
                FROM leads l
                JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s AND l.email_verdict = 'SEND'
                ORDER BY l.ingested_at ASC
            """, (campaign_id,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def save_sequence(row):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE email_sequences
                SET email_1_subject = %s, email_1_body    = %s,
                    email_2_subject = %s, email_2_body    = %s,
                    email_3_subject = %s, email_3_body    = %s
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
                SELECT cl.instantly_api_key FROM campaigns c
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
            json=body, timeout=30,
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

    print("Fetching all sequences...")
    leads = fetch_all_sequences(OBGYN_CAMPAIGN_ID)
    print(f"  {len(leads)} leads loaded.\n")

    changed = []
    total_replacements = 0

    for lead in leads:
        updated = dict(lead)
        lead_changes = 0

        for field, patterns in [
            ("email_1_subject", SUBJECT_REPLACEMENTS),
            ("email_2_subject", SUBJECT_REPLACEMENTS),
            ("email_3_subject", SUBJECT_REPLACEMENTS),
            ("email_1_body",    BODY_REPLACEMENTS),
            ("email_2_body",    BODY_REPLACEMENTS),
            ("email_3_body",    BODY_REPLACEMENTS),
        ]:
            new_val, n = apply_replacements(updated.get(field), patterns)
            if n > 0:
                updated[field] = new_val
                lead_changes += n

        if lead_changes > 0:
            total_replacements += lead_changes
            changed.append(updated)

    print(f"Leads needing changes: {len(changed)} / {len(leads)}")
    print(f"Total replacements:    {total_replacements}\n")

    if args.dry_run:
        # Show a sample
        for r in changed[:5]:
            print(f"  Sample: {r['business_name']} ({r['email']})")
        print("\nDry run — no changes written.")
        return

    # Save to DB
    print("Saving to DB...")
    for i, row in enumerate(changed, 1):
        save_sequence(row)
        if i % 50 == 0 or i == len(changed):
            print(f"  [{i}/{len(changed)}] saved")

    # Push to Instantly
    print(f"\nFetching Instantly API key...")
    api_key = get_instantly_api_key(OBGYN_CAMPAIGN_ID)
    if not api_key:
        print("ERROR: no API key found.")
        sys.exit(1)

    instantly = InstantlyClient(api_key)
    print(f"Building email->ID map for {INSTANTLY_CAMPAIGN_ID}...")
    email_to_id = build_email_to_id_map(instantly, INSTANTLY_CAMPAIGN_ID)
    print(f"  {len(email_to_id)} leads mapped in Instantly.\n")

    pushed = errors = not_found = 0
    for i, row in enumerate(changed, 1):
        lead_email = row.get("email", "")
        instantly_id = email_to_id.get(lead_email)
        if not instantly_id:
            not_found += 1
            continue
        try:
            patch_lead(instantly, instantly_id, row)
            pushed += 1
        except Exception as e:
            errors += 1
            print(f"  ERROR {lead_email}: {e}")

        if i % 50 == 0 or i == len(changed):
            print(f"  [{i}/{len(changed)}] Patched: {pushed}, Not found: {not_found}, Errors: {errors}")

    print(f"\nDone.")
    print(f"  DB updated:        {len(changed)} leads")
    print(f"  Instantly patched: {pushed}")
    print(f"  Not in Instantly:  {not_found}")
    print(f"  Errors:            {errors}")
    print(f"\nReview: https://app.instantly.ai/campaign/{INSTANTLY_CAMPAIGN_ID}")


if __name__ == "__main__":
    main()