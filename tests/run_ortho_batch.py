"""
Import, validate, generate, and push a batch of Orthopedics leads to Instantly.

Usage:
  python -m tests.run_ortho_batch                        # next 5 (offset auto-detected)
  python -m tests.run_ortho_batch --offset 5 --count 5  # explicit offset
  python -m tests.run_ortho_batch --dry-run             # preview only, no writes
"""

import os
import sys
import argparse
import logging
import time
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ortho_batch")

import openpyxl
from psycopg2.extras import RealDictCursor
from db import get_connection
from validation.cascade_validator import validate_lead_email
from generation.email_generator import generate_batch
from campaigns.instantly_client import InstantlyClient

CAMPAIGN_ID          = "831c7077-b3a6-42d4-9262-f70aa9ecd194"
INSTANTLY_CAMPAIGN_ID = "6c3db052-4e7a-49e1-8139-c218b44c88b3"
EXCEL_FILE  = r"C:\Users\akash\Downloads\HTAI_Campaign_Segments_Doctor_v3.xlsx"
SHEET_NAME  = "C-Orthopedics"


def count_existing_leads():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM leads WHERE campaign_id = %s", (CAMPAIGN_ID,))
            return cur.fetchone()[0]
    finally:
        conn.close()


def read_excel_rows(offset: int, count: int) -> list:
    """Read `count` data rows starting at `offset` (0-based from first data row)."""
    wb = openpyxl.load_workbook(EXCEL_FILE, read_only=True, data_only=True)
    ws = wb[SHEET_NAME]
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    # Row 0 = sheet title, Row 1 = headers, data starts Row 2
    headers = rows[1]
    data_rows = rows[2:]
    return headers, data_rows[offset:offset + count]


def import_rows(headers, data_rows) -> dict:
    import json
    stats = {"imported": 0, "skipped": 0, "errors": 0}
    raw_headers = [str(h).strip() if h is not None else "" for h in headers]

    def col(name):
        for i, h in enumerate(raw_headers):
            if h.lower() == name.lower():
                return i
        return None

    idx = {
        "first_name": col("First Name"),
        "last_name":  col("Last Name"),
        "email":      col("Business Email"),
        "business":   col("Physician Group Name"),
        "specialty":  col("Main Specialty"),
        "address":    col("Address"),
        "city":       col("City"),
        "state":      col("State"),
        "num_phys":   col("# of Physicians"),
    }

    def get(row, key):
        i = idx.get(key)
        if i is None or i >= len(row):
            return None
        val = row[i]
        return str(val).strip() if val is not None else None

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for row in data_rows:
                first = get(row, "first_name") or ""
                last  = get(row, "last_name") or ""
                owner = (first + " " + last).strip() or None
                email = get(row, "email")
                biz   = get(row, "business")
                if not biz and not email:
                    stats["skipped"] += 1
                    continue
                try:
                    cur.execute("""
                        INSERT INTO leads (
                            campaign_id, business_name, industry,
                            address, city, state, company_size,
                            sources, raw_data,
                            owner_name, owner_status, owner_source,
                            email, email_source, enrichment_status, country
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s, 'US'
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING lead_id
                    """, (
                        CAMPAIGN_ID,
                        biz,
                        get(row, "specialty"),
                        get(row, "address"),
                        get(row, "city"),
                        get(row, "state"),
                        get(row, "num_phys"),
                        ["htai_client_list"],
                        json.dumps({raw_headers[i]: str(row[i]) if row[i] is not None else None
                                    for i in range(len(raw_headers)) if i < len(row)}),
                        owner,
                        "found" if owner else "pending",
                        "client_list" if owner else None,
                        email,
                        "client_list" if email else None,
                        "partial" if email else "raw",
                    ))
                    result = cur.fetchone()
                    if result:
                        stats["imported"] += 1
                        logger.info(f"  Imported: {biz} ({email})")
                    else:
                        stats["skipped"] += 1
                        logger.info(f"  Skipped (duplicate): {biz} ({email})")
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"  Error inserting {biz}: {e}")
                    conn.rollback()
        conn.commit()
    finally:
        conn.close()
    return stats


def get_unvalidated_leads():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT lead_id, email FROM leads
                WHERE campaign_id = %s AND email IS NOT NULL AND email_verdict IS NULL
                ORDER BY ingested_at ASC
            """, (CAMPAIGN_ID,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_instantly_api_key():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT cl.instantly_api_key FROM campaigns c
                JOIN clients cl ON c.client_id = cl.client_id
                WHERE c.campaign_id = %s
            """, (CAMPAIGN_ID,))
            row = cur.fetchone()
            return row["instantly_api_key"] if row else None
    finally:
        conn.close()


def build_email_to_id_map(instantly):
    mapping = {}
    starting_after = None
    while True:
        body = {"limit": 100, "campaign_id": INSTANTLY_CAMPAIGN_ID}
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
            if item.get("campaign") == INSTANTLY_CAMPAIGN_ID:
                mapping[item["email"]] = item["id"]
        starting_after = data.get("next_starting_after")
        if not starting_after or not data.get("items"):
            break
    return mapping


def add_lead_to_instantly(instantly, row):
    resp = requests.post(
        f"{InstantlyClient.BASE_URL}/leads",
        headers=instantly._headers(),
        json={
            "campaign_id": INSTANTLY_CAMPAIGN_ID,
            "email": row["email"],
            "custom_variables": {
                "email_1_subject": row.get("email_1_subject", ""),
                "email_1_body":    row.get("email_1_body", ""),
                "email_2_subject": row.get("email_2_subject", ""),
                "email_2_body":    row.get("email_2_body", ""),
                "email_3_subject": row.get("email_3_subject", ""),
                "email_3_body":    row.get("email_3_body", ""),
                "business_name":   row.get("business_name", ""),
                "city":            row.get("city", ""),
                "state":           row.get("state", ""),
            },
        },
        timeout=30,
    )
    resp.raise_for_status()


def patch_lead_instantly(instantly, instantly_id, row):
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


def get_send_leads_with_sequences():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.lead_id, l.email, l.business_name, l.city, l.state,
                       es.sequence_id,
                       es.email_1_subject, es.email_1_body,
                       es.email_2_subject, es.email_2_body,
                       es.email_3_subject, es.email_3_body,
                       es.status as seq_status
                FROM leads l
                JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s AND l.email_verdict = 'SEND'
                ORDER BY l.ingested_at ASC
            """, (CAMPAIGN_ID,))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=None,
                        help="Row offset into Excel sheet (0-based). Default: auto (current lead count).")
    parser.add_argument("--count",  type=int, default=5, help="Number of leads to import (default: 5)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    existing = count_existing_leads()
    offset = args.offset if args.offset is not None else existing
    logger.info(f"Orthopedics batch — offset={offset}, count={args.count}, existing={existing}")

    # Step 1: Preview rows
    headers, data_rows = read_excel_rows(offset, args.count)
    logger.info(f"Reading rows {offset+1}–{offset+len(data_rows)} from {SHEET_NAME}:")
    for r in data_rows:
        fname = str(r[0] or "").strip()
        lname = str(r[1] or "").strip()
        email = str(r[4] or "").strip() if len(r) > 4 else ""
        biz   = str(r[5] or "").strip() if len(r) > 5 else ""
        city  = str(r[11] or "").strip() if len(r) > 11 else ""
        state = str(r[12] or "").strip() if len(r) > 12 else ""
        logger.info(f"  {fname} {lname} | {email} | {biz} | {city}, {state}")

    if args.dry_run:
        logger.info("\nDry run — no changes written.")
        return

    # Step 2: Import
    logger.info("\nImporting leads...")
    import_stats = import_rows(headers, data_rows)
    logger.info(f"Import: {import_stats}")

    # Step 3: Validate
    to_validate = get_unvalidated_leads()
    logger.info(f"\nValidating {len(to_validate)} leads...")
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _validate(lead):
        try:
            validate_lead_email(dict(lead), CAMPAIGN_ID)
            return True
        except Exception as e:
            logger.error(f"  Validation error {lead['email']}: {e}")
            return False

    with ThreadPoolExecutor(max_workers=5) as pool:
        list(as_completed({pool.submit(_validate, l): l for l in to_validate}))

    # Step 4: Generate sequences
    logger.info("\nGenerating email sequences...")
    gen_stats = generate_batch(CAMPAIGN_ID, batch_size=args.count + 5)
    logger.info(f"Generated: {gen_stats}")

    # Step 5: Push to Instantly
    logger.info("\nPushing to Instantly...")
    api_key = get_instantly_api_key()
    if not api_key:
        logger.error("No Instantly API key found.")
        sys.exit(1)

    instantly = InstantlyClient(api_key)
    email_to_id = build_email_to_id_map(instantly)
    logger.info(f"  {len(email_to_id)} leads already in Instantly campaign.")

    leads_to_push = get_send_leads_with_sequences()
    pushed = added = errors = 0
    for row in leads_to_push:
        email = row.get("email", "")
        instantly_id = email_to_id.get(email)
        try:
            if instantly_id:
                patch_lead_instantly(instantly, instantly_id, row)
                pushed += 1
            else:
                add_lead_to_instantly(instantly, row)
                added += 1
            logger.info(f"  {'PATCHED' if instantly_id else 'ADDED'}: {row['business_name']} ({email})")
        except Exception as e:
            errors += 1
            logger.error(f"  ERROR {email}: {e}")

    logger.info(f"\nDone.")
    logger.info(f"  Imported:        {import_stats['imported']}")
    logger.info(f"  Patched:         {pushed}")
    logger.info(f"  Added (new):     {added}")
    logger.info(f"  Errors:          {errors}")
    logger.info(f"  Review: https://app.instantly.ai/campaign/{INSTANTLY_CAMPAIGN_ID}")


if __name__ == "__main__":
    main()