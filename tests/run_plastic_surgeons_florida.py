#!/usr/bin/env python3
"""
LeadForge v2 — Plastic Surgeons in Florida
Full pipeline: scrape → enrich → generate emails → sync to Instantly (DRAFT)

SAFETY: This script NEVER activates an Instantly campaign. No emails are sent.

Usage:
  cd cold-email-infra
  python -m tests.run_plastic_surgeons_florida
"""

import os
import sys
import time
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import INSTANTLY_API_KEY, ANTHROPIC_API_KEY, HAIKU_MODEL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("run_plastic_surgeons")

# ── Campaign config ──────────────────────────────────────────────
CAMPAIGN_NAME   = "Plastic Surgeons Florida"
NICHE           = "plastic_surgeons"
LOCATION        = "Florida"
SCRAPE_QUERIES  = ["plastic surgeon", "cosmetic surgeon", "plastic surgery clinic"]
APOLLO_TITLES   = ["Owner", "Surgeon", "MD", "Plastic Surgeon", "Medical Director"]
MAX_PER_QUERY   = 10   # Apify results per query
APOLLO_LIMIT    = 15   # Apollo results

CAMPAIGN_ID = None
CLIENT_ID   = None

# ── SAFETY GUARD ─────────────────────────────────────────────────
import campaigns.instantly_client as _instantly_module

def _blocked_activate(self, campaign_id):
    raise RuntimeError("SAFETY GUARD: activate_campaign() is BLOCKED. No emails will be sent.")

def _blocked_set_sequences(self, campaign_id, sequences):
    raise RuntimeError("SAFETY GUARD: set_campaign_sequences() is BLOCKED.")

_instantly_module.InstantlyClient.activate_campaign    = _blocked_activate
_instantly_module.InstantlyClient.set_campaign_sequences = _blocked_set_sequences


# ── Helpers ──────────────────────────────────────────────────────

def header(title):
    logger.info("")
    logger.info("=" * 60)
    logger.info(f"  {title}")
    logger.info("=" * 60)


def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    msg = f"  [{status}] {name}"
    if detail:
        msg += f" — {detail}"
    if passed:
        logger.info(msg)
    else:
        logger.error(msg)
    return passed


def query_one(sql, params=None):
    from db import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    finally:
        conn.close()


def query_all(sql, params=None):
    from db import get_connection
    from psycopg2.extras import RealDictCursor
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


# ── Phase 0: Pre-flight ──────────────────────────────────────────

def phase_0_preflight():
    header("PHASE 0: PRE-FLIGHT CHECKS")

    import requests
    from config import (
        APOLLO_API_KEY, APOLLO_BASE_URL,
        APIFY_TOKEN, APIFY_BASE_URL,
        LEADMAGIC_API_KEY, LEADMAGIC_BASE_URL,
        MILLION_VERIFIER_API_KEY, MILLION_VERIFIER_BASE_URL,
    )

    all_pass = True

    # DB
    try:
        from db import get_connection
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
        conn.close()
        all_pass &= check("DB connection", db_name == "leadgen_db", f"connected to {db_name}")
    except Exception as e:
        logger.error(f"  FATAL: Cannot connect to DB: {e}")
        sys.exit(1)

    # APIs
    checks = [
        ("Apollo",          lambda: requests.post(f"{APOLLO_BASE_URL}/mixed_people/api_search",
                                headers={"X-Api-Key": APOLLO_API_KEY, "Content-Type": "application/json"},
                                json={"per_page": 1, "person_titles": ["CEO"]}, timeout=15)),
        ("Apify",           lambda: requests.get(f"{APIFY_BASE_URL}/acts",
                                headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
                                params={"limit": 1}, timeout=15)),
        ("LeadMagic",       lambda: requests.post(f"{LEADMAGIC_BASE_URL}/email-validate",
                                headers={"X-API-Key": LEADMAGIC_API_KEY, "Content-Type": "application/json"},
                                json={"email": "test@google.com"}, timeout=15)),
        ("Million Verifier", lambda: requests.get(MILLION_VERIFIER_BASE_URL,
                                params={"api": MILLION_VERIFIER_API_KEY, "email": "test@google.com"}, timeout=15)),
        ("Instantly",       lambda: requests.get("https://api.instantly.ai/api/v2/campaigns",
                                headers={"Authorization": f"Bearer {INSTANTLY_API_KEY}"},
                                params={"limit": 1}, timeout=15)),
    ]
    for name, fn in checks:
        try:
            resp = fn()
            all_pass &= check(name, resp.status_code == 200, f"status={resp.status_code}")
        except Exception as e:
            all_pass &= check(name, False, str(e))

    # Anthropic
    try:
        from anthropic import Anthropic
        c = Anthropic(api_key=ANTHROPIC_API_KEY)
        r = c.messages.create(model=HAIKU_MODEL, max_tokens=5,
                               messages=[{"role": "user", "content": "Hi"}])
        all_pass &= check("Anthropic", r.content[0].text is not None)
    except Exception as e:
        all_pass &= check("Anthropic", False, str(e))

    logger.info("\n  Pre-flight complete.")
    return all_pass


# ── Phase 1: Seed DB ─────────────────────────────────────────────

def phase_1_seed_db():
    global CLIENT_ID, CAMPAIGN_ID
    header("PHASE 1: SEED DATABASE")

    from db import create_client, create_campaign

    CLIENT_ID = create_client(
        "Infinite Labs Digital",
        INSTANTLY_API_KEY,
        ["infinitelabsdigital.com"]
    )
    logger.info(f"  Client ID: {CLIENT_ID}")

    CAMPAIGN_ID = create_campaign(
        CAMPAIGN_NAME,
        CLIENT_ID,
        NICHE,
        "state",
        LOCATION
    )
    logger.info(f"  Campaign ID: {CAMPAIGN_ID}")

    check("Client created", CLIENT_ID is not None)
    check("Campaign created", CAMPAIGN_ID is not None, CAMPAIGN_NAME)


# ── Phase 2: Training corpus ─────────────────────────────────────

def phase_2_training_corpus():
    header("PHASE 2: TRAINING CORPUS")

    existing = query_one("SELECT COUNT(*) FROM training_corpus WHERE source = 'synthesis'")
    if existing and existing[0] > 0:
        logger.info(f"  Research doc already exists ({existing[0]} rows). Skipping.")
        return

    logger.info("  Inserting cold email training content...")
    from db import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO training_corpus (source, source_url, title, content)
                VALUES (%s, %s, %s, %s)
            """, (
                "manual", "plastic_surgeons_run",
                "Cold Email Frameworks for Medical/Aesthetic Practices",
                """
Cold Email Frameworks for Medical & Aesthetic Practices

## Framework 1: AIDA
- Subject line creates curiosity (Attention)
- First sentence hooks with relevance (Interest)
- Body provides value/social proof (Desire)
- Clear, low-friction CTA (Action)

## Framework 2: PAS
- Identify a specific problem (new patient acquisition, online visibility)
- Agitate: cost of not solving it
- Present solution naturally

## Subject Line Patterns (High Open Rate)
- "Quick question about [practice name]"
- "Idea for [doctor name]'s practice"
- "How [similar practice] gets 20+ new patients/month"
- "your Google reviews"
- Short lowercase: "hey [first name]"

## Personalization Techniques
- Reference their Google rating and review count
- Mention specific location
- Reference the types of procedures they offer
- Mention their website or online presence

## 3-Email Sequence
Email 1 (Day 0): Value-first, observation about their practice, soft CTA
Email 2 (Day 4): Different angle, case study or result, slightly more direct CTA
Email 3 (Day 8): Break-up email, permission close "Should I close your file?"

## What NOT to Do
- Never use "I hope this email finds you well"
- Don't start with "My name is..."
- Avoid long paragraphs
- Don't use ALL CAPS
- Never be vague about what you offer

## Medical/Aesthetic Practice Tips
- Focus on new patient acquisition
- Reference Google Maps presence and reviews
- Mention ROI of digital marketing for aesthetic practices
- Highlight competitor comparison tactfully
                """
            ))
            conn.commit()
    finally:
        conn.close()

    logger.info("  Building research document via Claude...")
    from generation.knowledge_base import build_research_document
    doc = build_research_document(CAMPAIGN_ID)
    check("Research document built", len(doc) > 100, f"{len(doc)} chars")


# ── Phase 3: Scrape ──────────────────────────────────────────────

def phase_3_scrape():
    header("PHASE 3: SCRAPE LOOP (Apify + Apollo)")

    from ingestion.apify_client import scrape_google_maps
    from ingestion.apollo_client import search_people
    from ingestion.deduplicator import deduplicate_leads
    from db import insert_lead

    all_leads = []

    for query in SCRAPE_QUERIES:
        logger.info(f"  Apify: '{query} in {LOCATION}' (max {MAX_PER_QUERY})...")
        try:
            leads = scrape_google_maps(query, LOCATION,
                                        max_results=MAX_PER_QUERY,
                                        campaign_id=CAMPAIGN_ID)
            logger.info(f"    -> {len(leads)} results")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"    Apify error: {e}")

    logger.info(f"  Apollo: searching for plastic surgeons in {LOCATION}...")
    try:
        apollo_leads = search_people(
            title_keywords=APOLLO_TITLES,
            location=LOCATION,
            limit=APOLLO_LIMIT,
            campaign_id=CAMPAIGN_ID
        )
        logger.info(f"    → {len(apollo_leads)} results")
        all_leads.extend(apollo_leads)
    except Exception as e:
        logger.error(f"    Apollo error: {e}")

    logger.info(f"  Total raw leads: {len(all_leads)}")

    unique_leads = deduplicate_leads(all_leads)
    logger.info(f"  After dedup: {len(unique_leads)}")

    inserted = 0
    for lead in unique_leads:
        if insert_lead(lead, CAMPAIGN_ID):
            inserted += 1

    logger.info(f"  Inserted into DB: {inserted}")
    count = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s", (CAMPAIGN_ID,))
    check("Leads in DB", count and count[0] > 0, f"{count[0]} leads")
    return inserted


# ── Phase 4: Enrichment ──────────────────────────────────────────

def phase_4_process():
    header("PHASE 4: PROCESS LOOP (Enrichment + Validation)")

    from enrichment.enrichment_engine import process_batch
    from tracking.budget_guard import get_budget_status

    budget = get_budget_status()
    logger.info(f"  Budget: ${budget['spent']:.2f} / ${budget['budget']:.2f} ({budget['percent']:.0%})")

    logger.info("  Running enrichment pipeline...")
    stats = process_batch(CAMPAIGN_ID, batch_size=30)
    logger.info(f"  Enrichment results: {stats}")
    check("Leads processed", stats["processed"] > 0, f"{stats['processed']} leads")

    for label, sql in [
        ("Owner discovery", "SELECT owner_status, COUNT(*) as cnt FROM leads WHERE campaign_id = %s GROUP BY owner_status"),
        ("Email verdicts",  "SELECT email_verdict, COUNT(*) as cnt FROM leads WHERE campaign_id = %s AND email_verdict IS NOT NULL GROUP BY email_verdict"),
    ]:
        rows = query_all(sql, (CAMPAIGN_ID,))
        logger.info(f"  {label}:")
        for r in rows:
            logger.info(f"    {list(r.values())[0]}: {list(r.values())[1]}")

    budget = get_budget_status()
    logger.info(f"  Budget after enrichment: ${budget['spent']:.2f} / ${budget['budget']:.2f}")
    return stats


# ── Phase 5: Launch ──────────────────────────────────────────────

def phase_5_launch():
    header("PHASE 5: LAUNCH LOOP (Email Gen + Instantly Sync)")

    send_count = query_one(
        "SELECT COUNT(*) FROM leads WHERE campaign_id = %s AND email_verdict = 'SEND'",
        (CAMPAIGN_ID,)
    )
    logger.info(f"  SEND leads available: {send_count[0] if send_count else 0}")

    if not send_count or send_count[0] == 0:
        logger.warning("  No SEND leads — skipping generation and sync.")
        return

    from generation.email_generator import generate_batch
    gen_stats = generate_batch(CAMPAIGN_ID, batch_size=30)
    logger.info(f"  Email generation: {gen_stats}")
    check("Sequences generated", gen_stats["generated"] > 0, f"{gen_stats['generated']} sequences")

    # Sample emails
    samples = query_all("""
        SELECT es.email_1_subject, es.email_1_body, l.business_name, l.owner_name
        FROM email_sequences es
        JOIN leads l ON es.lead_id = l.lead_id
        WHERE es.campaign_id = %s
        LIMIT 3
    """, (CAMPAIGN_ID,))

    for i, s in enumerate(samples, 1):
        logger.info(f"\n  --- Sample Email {i} ---")
        logger.info(f"  Business: {s['business_name']}")
        logger.info(f"  Owner: {s['owner_name']}")
        logger.info(f"  Subject: {s['email_1_subject']}")
        logger.info(f"  Body preview: {s['email_1_body'][:200]}...")

    logger.info("\n  Syncing to Instantly (DRAFT — will NOT be activated)...")
    from campaigns.campaign_launcher import launch_campaign
    launch_stats = launch_campaign(CAMPAIGN_ID)
    logger.info(f"  Launch result: {launch_stats}")
    check("Leads synced to Instantly",
          launch_stats["synced"] > 0 or launch_stats["errors"] == 0,
          f"synced={launch_stats['synced']}, errors={launch_stats['errors']}")

    instantly_id = query_one(
        "SELECT instantly_campaign_id FROM campaigns WHERE campaign_id = %s", (CAMPAIGN_ID,)
    )
    if instantly_id and instantly_id[0]:
        logger.info(f"  Instantly campaign ID: {instantly_id[0]}")
        check("Instantly campaign created", True)

    logger.info("\n  SAFETY CONFIRMED: activate_campaign() was NOT called.")
    logger.info("  The Instantly campaign is in DRAFT state. No emails will be sent.")


# ── Phase 6: Summary ─────────────────────────────────────────────

def phase_6_summary():
    header("PHASE 6: SUMMARY REPORT")

    total   = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s", (CAMPAIGN_ID,))
    owners  = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s AND owner_status = 'found'", (CAMPAIGN_ID,))
    emails  = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s AND email IS NOT NULL", (CAMPAIGN_ID,))
    seqs    = query_one("SELECT COUNT(*) FROM email_sequences WHERE campaign_id = %s", (CAMPAIGN_ID,))
    synced  = query_one("SELECT COUNT(*) FROM email_sequences WHERE campaign_id = %s AND status = 'synced'", (CAMPAIGN_ID,))

    verdict_rows = query_all(
        "SELECT email_verdict, COUNT(*) as cnt FROM leads WHERE campaign_id = %s GROUP BY email_verdict",
        (CAMPAIGN_ID,)
    )
    verdicts = {r["email_verdict"]: r["cnt"] for r in verdict_rows}

    from tracking.budget_guard import get_budget_status
    budget = get_budget_status()

    t = total[0] if total else 0

    print("\n")
    print("=" * 60)
    print(f"          {CAMPAIGN_NAME}")
    print("=" * 60)
    print(f"  Leads ingested:        {t}")
    print(f"  Owners found:          {owners[0] if owners else 0} ({owners[0]/t*100:.0f}%)" if t else "  Owners found:          0")
    print(f"  Emails found:          {emails[0] if emails else 0} ({emails[0]/t*100:.0f}%)" if t else "  Emails found:          0")
    print(f"    - SEND:              {verdicts.get('SEND', 0)}")
    print(f"    - RISKY:             {verdicts.get('RISKY', 0)}")
    print(f"    - DO NOT SEND:       {verdicts.get('DO NOT SEND', 0)}")
    print(f"    - UNVERIFIED:        {verdicts.get('UNVERIFIED', 0)}")
    print(f"    - No verdict:        {verdicts.get(None, 0)}")
    print(f"  Sequences generated:   {seqs[0] if seqs else 0}")
    print(f"  Synced to Instantly:   {synced[0] if synced else 0}")
    print(f"  Total API spend:       ${budget['spent']:.2f}")
    if t:
        print(f"  Cost per lead:         ${budget['spent']/t:.2f}")
    print()

    costs = query_all(
        "SELECT service, operation, COUNT(*) as calls, SUM(cost_usd) as cost "
        "FROM cost_events WHERE campaign_id = %s "
        "GROUP BY service, operation ORDER BY cost DESC",
        (CAMPAIGN_ID,)
    )
    print("  Per-service breakdown:")
    for c in costs:
        print(f"    {c['service']}/{c['operation']:25s} ${float(c['cost']):8.4f}  ({c['calls']} calls)")

    print()
    print(f"  Campaign ID:   {CAMPAIGN_ID}")
    print(f"  Client ID:     {CLIENT_ID}")
    instantly_id = query_one(
        "SELECT instantly_campaign_id FROM campaigns WHERE campaign_id = %s", (CAMPAIGN_ID,)
    )
    print(f"  Instantly ID:  {instantly_id[0] if instantly_id and instantly_id[0] else 'N/A'}")
    print()
    print("  SAFETY: No emails were sent. Instantly campaign is in DRAFT state.")
    print("=" * 60)
    print()


# ── Main ─────────────────────────────────────────────────────────

def main():
    start = time.time()

    print("\n")
    print(f"  LeadForge v2 — {CAMPAIGN_NAME}")
    print(f"  Niche: {NICHE} | Location: {LOCATION}")
    print("  SAFETY: No emails will be sent.\n")

    phase_0_preflight()
    phase_1_seed_db()
    phase_2_training_corpus()

    inserted = phase_3_scrape()
    if not inserted:
        logger.error("No leads inserted. Check Apify/Apollo credentials.")
        sys.exit(1)

    phase_4_process()
    phase_5_launch()
    phase_6_summary()

    print(f"  Total time: {(time.time() - start)/60:.1f} minutes\n")


if __name__ == "__main__":
    main()