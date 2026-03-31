#!/usr/bin/env python3
"""
LeadForge v2 — End-to-End Pipeline Test

Runs the full cold email pipeline locally against Cloud SQL:
  Phase 0: Pre-flight checks (DB, APIs, prompt files)
  Phase 1: Seed DB (client + campaign)
  Phase 2: Ingest training corpus (YouTube transcripts)
  Phase 3: Scrape loop (Apify + Apollo → ~20 leads)
  Phase 4: Process loop (owner discovery + email waterfall + validation)
  Phase 5: Launch loop (email gen + Instantly sync — NO ACTIVATION)
  Phase 6: Summary report

SAFETY: This script NEVER activates an Instantly campaign. No emails are sent.

Usage:
  cd cold-email-infra
  python3 tests/e2e_test.py
"""

import os
import sys
import time
import json
import logging
import requests

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    APOLLO_API_KEY, APOLLO_BASE_URL,
    APIFY_TOKEN, APIFY_BASE_URL,
    GOOGLE_SEARCH_API_KEY, GOOGLE_SEARCH_CX,
    LEADMAGIC_API_KEY, LEADMAGIC_BASE_URL,
    MILLION_VERIFIER_API_KEY, MILLION_VERIFIER_BASE_URL,
    ANTHROPIC_API_KEY, HAIKU_MODEL,
    INSTANTLY_API_KEY,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("e2e_test")

# Track test campaign for queries
TEST_CAMPAIGN_ID = None
TEST_CLIENT_ID = None


# ============================================================
# SAFETY GUARD: Monkey-patch InstantlyClient to block activation
# ============================================================
import campaigns.instantly_client as _instantly_module

_original_activate = _instantly_module.InstantlyClient.activate_campaign

def _blocked_activate(self, campaign_id):
    raise RuntimeError(
        "SAFETY GUARD: activate_campaign() is BLOCKED during E2E test. "
        "No emails will be sent."
    )

_instantly_module.InstantlyClient.activate_campaign = _blocked_activate

_original_set_sequences = _instantly_module.InstantlyClient.set_campaign_sequences

def _blocked_set_sequences(self, campaign_id, sequences):
    raise RuntimeError(
        "SAFETY GUARD: set_campaign_sequences() is BLOCKED during E2E test. "
        "No email sequences will be pushed to Instantly."
    )

_instantly_module.InstantlyClient.set_campaign_sequences = _blocked_set_sequences


# ============================================================
# Helpers
# ============================================================

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


# ============================================================
# Phase 0: Pre-Flight Checks
# ============================================================

def phase_0_preflight():
    header("PHASE 0: PRE-FLIGHT CHECKS")
    all_pass = True

    # --- DB Connectivity ---
    logger.info("  Checking database...")
    try:
        from db import get_connection
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            db_name = cur.fetchone()[0]
        conn.close()
        all_pass &= check("DB connection", db_name == "leadgen_db", f"connected to {db_name}")
    except Exception as e:
        all_pass &= check("DB connection", False, str(e))
        logger.error("  FATAL: Cannot connect to database. Aborting.")
        sys.exit(1)

    # Check all tables exist
    expected_tables = ["leads", "campaigns", "clients", "cost_events",
                       "email_sequences", "campaign_metrics", "training_corpus", "scrape_jobs"]
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cur.fetchall()]
        conn.close()
        for t in expected_tables:
            all_pass &= check(f"Table '{t}'", t in tables)
    except Exception as e:
        all_pass &= check("Table check", False, str(e))

    # --- API Smoke Tests ---
    logger.info("  Checking APIs...")

    # Apollo
    try:
        resp = requests.post(
            f"{APOLLO_BASE_URL}/mixed_people/api_search",
            headers={"X-Api-Key": APOLLO_API_KEY, "Content-Type": "application/json"},
            json={"per_page": 1, "person_titles": ["CEO"]},
            timeout=15
        )
        all_pass &= check("Apollo API", resp.status_code == 200, f"status={resp.status_code}")
    except Exception as e:
        all_pass &= check("Apollo API", False, str(e))

    # Apify
    try:
        resp = requests.get(
            f"{APIFY_BASE_URL}/acts",
            headers={"Authorization": f"Bearer {APIFY_TOKEN}"},
            params={"limit": 1}, timeout=15
        )
        all_pass &= check("Apify API", resp.status_code == 200, f"status={resp.status_code}")
    except Exception as e:
        all_pass &= check("Apify API", False, str(e))

    # Google Custom Search
    try:
        resp = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": GOOGLE_SEARCH_API_KEY, "cx": GOOGLE_SEARCH_CX,
                    "q": "test", "num": 1},
            timeout=10
        )
        all_pass &= check("Google Search API", resp.status_code == 200, f"status={resp.status_code}")
    except Exception as e:
        all_pass &= check("Google Search API", False, str(e))

    # LeadMagic
    try:
        resp = requests.post(
            f"{LEADMAGIC_BASE_URL}/email-validate",
            headers={"X-API-Key": LEADMAGIC_API_KEY, "Content-Type": "application/json"},
            json={"email": "test@google.com"},
            timeout=15
        )
        all_pass &= check("LeadMagic API", resp.status_code == 200, f"status={resp.status_code}")
    except Exception as e:
        all_pass &= check("LeadMagic API", False, str(e))

    # Million Verifier
    try:
        resp = requests.get(
            MILLION_VERIFIER_BASE_URL,
            params={"api": MILLION_VERIFIER_API_KEY, "email": "test@google.com"},
            timeout=15
        )
        all_pass &= check("Million Verifier API", resp.status_code == 200, f"status={resp.status_code}")
    except Exception as e:
        all_pass &= check("Million Verifier API", False, str(e))

    # Anthropic
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = client.messages.create(
            model=HAIKU_MODEL, max_tokens=5,
            messages=[{"role": "user", "content": "Say hi"}]
        )
        all_pass &= check("Anthropic API", resp.content[0].text is not None,
                          f"response: {resp.content[0].text[:30]}")
    except Exception as e:
        all_pass &= check("Anthropic API", False, str(e))

    # Instantly
    try:
        resp = requests.get(
            "https://api.instantly.ai/api/v2/campaigns",
            headers={"Authorization": f"Bearer {INSTANTLY_API_KEY}",
                     "Content-Type": "application/json"},
            params={"limit": 1},
            timeout=15
        )
        all_pass &= check("Instantly API", resp.status_code == 200, f"status={resp.status_code}")
    except Exception as e:
        all_pass &= check("Instantly API", False, str(e))

    # --- Prompt Files ---
    logger.info("  Checking prompt files...")
    prompt_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "prompts")
    for fname in ["owner_discovery.txt", "owner_verification.txt", "email_system.txt", "email_examples.json"]:
        fpath = os.path.join(prompt_dir, fname)
        exists = os.path.isfile(fpath) and os.path.getsize(fpath) > 0
        all_pass &= check(f"Prompt: {fname}", exists)

    if not all_pass:
        logger.warning("\n  Some pre-flight checks failed. Review above.")
        logger.warning("  Continuing with non-critical failures (Google Search is optional)...")

    logger.info("\n  Pre-flight checks complete.")
    return all_pass


# ============================================================
# Phase 1: Seed DB
# ============================================================

def phase_1_seed_db():
    global TEST_CLIENT_ID, TEST_CAMPAIGN_ID
    header("PHASE 1: SEED DATABASE")

    from db import create_client, create_campaign

    # Create client
    TEST_CLIENT_ID = create_client(
        "Infinite Labs Digital",
        INSTANTLY_API_KEY,
        ["infinitelabsdigital.com"]
    )
    logger.info(f"  Created client: {TEST_CLIENT_ID}")

    # Create campaign
    TEST_CAMPAIGN_ID = create_campaign(
        "E2E Test - Dentists Orlando",
        TEST_CLIENT_ID,
        "dentists",
        "city",
        "Orlando, FL"
    )
    logger.info(f"  Created campaign: {TEST_CAMPAIGN_ID}")

    # Verify
    client_row = query_one("SELECT client_name FROM clients WHERE client_id = %s", (TEST_CLIENT_ID,))
    check("Client created", client_row is not None, client_row[0] if client_row else "NOT FOUND")

    campaign_row = query_one("SELECT campaign_name, status FROM campaigns WHERE campaign_id = %s", (TEST_CAMPAIGN_ID,))
    check("Campaign created", campaign_row is not None,
          f"{campaign_row[0]}, status={campaign_row[1]}" if campaign_row else "NOT FOUND")


# ============================================================
# Phase 2: Ingest Training Corpus
# ============================================================

def phase_2_training_corpus():
    header("PHASE 2: INGEST TRAINING CORPUS")

    # Check if there's already a research document
    existing = query_one("SELECT COUNT(*) FROM training_corpus WHERE source = 'synthesis'")
    if existing and existing[0] > 0:
        logger.info(f"  Research document already exists ({existing[0]} rows). Skipping ingest.")
        return

    # Insert a sample cold email knowledge base manually
    # (faster and cheaper than running Apify YouTube actor for a test)
    logger.info("  Inserting sample cold email training content...")

    sample_content = """
    Cold Email Frameworks and Best Practices (Compiled Research)

    ## Framework 1: AIDA (Attention, Interest, Desire, Action)
    - Subject line creates curiosity (Attention)
    - First sentence hooks with relevance (Interest)
    - Body provides value/social proof (Desire)
    - Clear, low-friction CTA (Action)

    ## Framework 2: PAS (Problem, Agitate, Solve)
    - Identify a specific problem the prospect faces
    - Agitate: show the cost of not solving it
    - Present your solution as the natural fix

    ## Framework 3: Before-After-Bridge
    - Before: Their current state (pain)
    - After: What life looks like with the solution
    - Bridge: How you get them there

    ## Subject Line Patterns (High Open Rate)
    - Question format: "Quick question about [their business]"
    - Name + specificity: "[First name], noticed something about [business name]"
    - Curiosity gap: "Idea for [business name]"
    - Short and lowercase: "hey [first name]" (casual, stands out)
    - Number-driven: "[X] patients/month from Google"

    ## Personalization Techniques
    - Reference their Google rating and review count
    - Mention their specific location/neighborhood
    - Note something specific from their website
    - Reference a recent Google review
    - Mention a competitor who is doing well

    ## 3-Email Sequence Structure
    Email 1 (Day 0): Value-first outreach
    - Personal observation about their business
    - One specific way you can help
    - Soft CTA: "Would it be worth a quick chat?"

    Email 2 (Day 3-5): Different angle + social proof
    - Reference Email 1 briefly
    - Share a case study or result
    - Slightly more direct CTA

    Email 3 (Day 7-10): Break-up email
    - Acknowledge they're busy
    - One final value point
    - Permission close: "Should I close your file?"

    ## What NOT to Do
    - Never use "I hope this email finds you well"
    - Don't start with "My name is..."
    - Avoid long paragraphs (max 2-3 sentences each)
    - Don't attach files in cold emails
    - Never use ALL CAPS or excessive exclamation marks
    - Don't be vague — always be specific about what you offer
    - Avoid generic subject lines like "Partnership opportunity"

    ## Dental Practice Specific Tips
    - Focus on patient acquisition (new patients/month)
    - Reference their Google Maps presence
    - Mention specific services (implants, cosmetic, Invisalign)
    - Compare their online presence to nearby competitors
    - Highlight the ROI of digital marketing for dental practices
    """

    from db import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO training_corpus (source, source_url, title, content)
                VALUES (%s, %s, %s, %s)
            """, (
                "manual",
                "e2e_test",
                "Cold Email Frameworks and Best Practices",
                sample_content,
            ))
            conn.commit()
    finally:
        conn.close()
    logger.info("  Sample training content inserted.")

    # Build research document
    logger.info("  Building research document via Claude Sonnet...")
    from generation.knowledge_base import build_research_document
    research_doc = build_research_document(TEST_CAMPAIGN_ID)

    has_doc = len(research_doc) > 100
    check("Research document built", has_doc, f"{len(research_doc)} chars")

    synthesis_count = query_one("SELECT COUNT(*) FROM training_corpus WHERE source = 'synthesis'")
    check("Research doc in DB", synthesis_count and synthesis_count[0] > 0)


# ============================================================
# Phase 3: Scrape Loop
# ============================================================

def phase_3_scrape():
    header("PHASE 3: SCRAPE LOOP (Apify + Apollo)")

    from ingestion.apify_client import scrape_google_maps
    from ingestion.apollo_client import search_people
    from ingestion.deduplicator import deduplicate_leads
    from db import insert_lead

    all_leads = []

    # Apify Google Maps — limit to 5 results per query to stay around 20 total
    queries = ["dentist", "dental office"]  # Use 2 of 4 queries to limit volume
    for query in queries:
        logger.info(f"  Apify: scraping '{query} in Orlando, FL' (max 10)...")
        try:
            leads = scrape_google_maps(query, "Orlando, FL",
                                        max_results=10,
                                        campaign_id=TEST_CAMPAIGN_ID)
            logger.info(f"  Apify: {len(leads)} results for '{query}'")
            all_leads.extend(leads)
        except Exception as e:
            logger.error(f"  Apify error for '{query}': {e}")

    # Apollo
    logger.info("  Apollo: searching for dental practice owners in Orlando...")
    try:
        apollo_leads = search_people(
            title_keywords=["Owner", "Dentist", "DDS", "DMD"],
            location="Orlando, FL",
            limit=10,
            campaign_id=TEST_CAMPAIGN_ID
        )
        logger.info(f"  Apollo: {len(apollo_leads)} results")
        all_leads.extend(apollo_leads)
    except Exception as e:
        logger.error(f"  Apollo error: {e}")

    total_found = len(all_leads)
    logger.info(f"  Total raw leads: {total_found}")

    # Dedup
    unique_leads = deduplicate_leads(all_leads)
    logger.info(f"  After dedup: {len(unique_leads)}")

    # Insert
    inserted = 0
    for lead in unique_leads:
        lead_id = insert_lead(lead, TEST_CAMPAIGN_ID)
        if lead_id:
            inserted += 1

    logger.info(f"  Inserted into DB: {inserted}")

    # Verify
    count = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s", (TEST_CAMPAIGN_ID,))
    check("Leads in DB", count and count[0] > 0, f"{count[0]} leads")

    statuses = query_all(
        "SELECT enrichment_status, COUNT(*) as cnt FROM leads WHERE campaign_id = %s GROUP BY enrichment_status",
        (TEST_CAMPAIGN_ID,)
    )
    for s in statuses:
        logger.info(f"    enrichment_status={s['enrichment_status']}: {s['cnt']}")

    costs = query_all(
        "SELECT service, operation, COUNT(*) as calls, SUM(cost_usd) as cost FROM cost_events WHERE campaign_id = %s GROUP BY service, operation",
        (TEST_CAMPAIGN_ID,)
    )
    for c in costs:
        logger.info(f"    cost: {c['service']}/{c['operation']}: {c['calls']} calls, ${float(c['cost']):.4f}")

    return inserted


# ============================================================
# Phase 4: Process Loop (Enrichment)
# ============================================================

def phase_4_process():
    header("PHASE 4: PROCESS LOOP (Enrichment + Validation)")

    from enrichment.enrichment_engine import process_batch
    from tracking.budget_guard import get_budget_status

    budget = get_budget_status()
    logger.info(f"  Budget before processing: ${budget['spent']:.2f} / ${budget['budget']:.2f} ({budget['percent']:.0%})")

    logger.info("  Running enrichment pipeline...")
    stats = process_batch(TEST_CAMPAIGN_ID, batch_size=25)

    logger.info(f"  Enrichment results: {stats}")
    check("Leads processed", stats["processed"] > 0, f"{stats['processed']} leads")

    # Verify owner discovery
    owner_stats = query_all(
        "SELECT owner_status, COUNT(*) as cnt FROM leads WHERE campaign_id = %s GROUP BY owner_status",
        (TEST_CAMPAIGN_ID,)
    )
    logger.info("  Owner discovery results:")
    for s in owner_stats:
        logger.info(f"    {s['owner_status']}: {s['cnt']}")

    # Verify email finding
    email_stats = query_all(
        "SELECT email_source, COUNT(*) as cnt FROM leads WHERE campaign_id = %s AND email IS NOT NULL GROUP BY email_source",
        (TEST_CAMPAIGN_ID,)
    )
    logger.info("  Email sources:")
    for s in email_stats:
        logger.info(f"    {s['email_source']}: {s['cnt']}")

    # Verify validation
    verdict_stats = query_all(
        "SELECT email_verdict, COUNT(*) as cnt FROM leads WHERE campaign_id = %s AND email_verdict IS NOT NULL GROUP BY email_verdict",
        (TEST_CAMPAIGN_ID,)
    )
    logger.info("  Email verdicts:")
    for s in verdict_stats:
        logger.info(f"    {s['email_verdict']}: {s['cnt']}")

    # Enrichment status breakdown
    enrichment_stats = query_all(
        "SELECT enrichment_status, COUNT(*) as cnt FROM leads WHERE campaign_id = %s GROUP BY enrichment_status",
        (TEST_CAMPAIGN_ID,)
    )
    logger.info("  Enrichment status:")
    for s in enrichment_stats:
        logger.info(f"    {s['enrichment_status']}: {s['cnt']}")

    # Cost breakdown so far
    costs = query_all(
        "SELECT service, operation, COUNT(*) as calls, SUM(cost_usd) as cost FROM cost_events WHERE campaign_id = %s GROUP BY service, operation ORDER BY cost DESC",
        (TEST_CAMPAIGN_ID,)
    )
    logger.info("  Cost breakdown:")
    for c in costs:
        logger.info(f"    {c['service']}/{c['operation']}: {c['calls']} calls, ${float(c['cost']):.4f}")

    budget = get_budget_status()
    logger.info(f"  Budget after processing: ${budget['spent']:.2f} / ${budget['budget']:.2f}")

    return stats


# ============================================================
# Phase 5: Launch Loop (Email Gen + Instantly Sync)
# ============================================================

def phase_5_launch():
    header("PHASE 5: LAUNCH LOOP (Email Gen + Instantly Sync)")

    # 5a: Email generation
    logger.info("  5a: Generating email sequences for SEND leads...")

    send_count = query_one(
        "SELECT COUNT(*) FROM leads WHERE campaign_id = %s AND email_verdict = 'SEND'",
        (TEST_CAMPAIGN_ID,)
    )
    logger.info(f"  SEND leads available: {send_count[0] if send_count else 0}")

    if not send_count or send_count[0] == 0:
        logger.warning("  No SEND leads — skipping email generation and Instantly sync.")
        logger.warning("  This means either no emails were found or none passed validation.")
        return

    from generation.email_generator import generate_batch
    gen_stats = generate_batch(TEST_CAMPAIGN_ID, batch_size=25)
    logger.info(f"  Email generation: {gen_stats}")
    check("Sequences generated", gen_stats["generated"] > 0, f"{gen_stats['generated']} sequences")

    # Sample a few sequences
    samples = query_all("""
        SELECT es.email_1_subject, es.email_1_body, l.business_name, l.owner_name
        FROM email_sequences es
        JOIN leads l ON es.lead_id = l.lead_id
        WHERE es.campaign_id = %s
        LIMIT 3
    """, (TEST_CAMPAIGN_ID,))

    for i, s in enumerate(samples, 1):
        logger.info(f"\n  --- Sample Email {i} ---")
        logger.info(f"  Business: {s['business_name']}")
        logger.info(f"  Owner: {s['owner_name']}")
        logger.info(f"  Subject: {s['email_1_subject']}")
        logger.info(f"  Body preview: {s['email_1_body'][:200]}...")

    # 5b: Campaign launch (sync to Instantly — NO ACTIVATION)
    logger.info("\n  5b: Syncing to Instantly (campaign will NOT be activated)...")

    from campaigns.campaign_launcher import launch_campaign
    launch_stats = launch_campaign(TEST_CAMPAIGN_ID)
    logger.info(f"  Launch result: {launch_stats}")
    check("Leads synced to Instantly", launch_stats["synced"] > 0 or launch_stats["errors"] == 0,
          f"synced={launch_stats['synced']}, errors={launch_stats['errors']}")

    # Verify campaign ID saved
    instantly_id = query_one(
        "SELECT instantly_campaign_id FROM campaigns WHERE campaign_id = %s",
        (TEST_CAMPAIGN_ID,)
    )
    if instantly_id and instantly_id[0]:
        logger.info(f"  Instantly campaign ID: {instantly_id[0]}")
        check("Instantly campaign created", True)
    else:
        check("Instantly campaign created", False, "No campaign ID saved")

    # Verify sequence status updated
    synced = query_one(
        "SELECT COUNT(*) FROM email_sequences WHERE campaign_id = %s AND status = 'synced'",
        (TEST_CAMPAIGN_ID,)
    )
    if synced:
        logger.info(f"  Sequences marked as synced: {synced[0]}")

    logger.info("\n  SAFETY CONFIRMED: activate_campaign() was NOT called.")
    logger.info("  The Instantly campaign is in DRAFT state. No emails will be sent.")


# ============================================================
# Phase 6: Summary Report
# ============================================================

def phase_6_summary():
    header("PHASE 6: SUMMARY REPORT")

    total_leads = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s", (TEST_CAMPAIGN_ID,))
    owners_found = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s AND owner_status = 'found'", (TEST_CAMPAIGN_ID,))
    emails_found = query_one("SELECT COUNT(*) FROM leads WHERE campaign_id = %s AND email IS NOT NULL", (TEST_CAMPAIGN_ID,))

    verdict_stats = query_all(
        "SELECT email_verdict, COUNT(*) as cnt FROM leads WHERE campaign_id = %s GROUP BY email_verdict",
        (TEST_CAMPAIGN_ID,)
    )
    verdicts = {r["email_verdict"]: r["cnt"] for r in verdict_stats}

    seq_count = query_one("SELECT COUNT(*) FROM email_sequences WHERE campaign_id = %s", (TEST_CAMPAIGN_ID,))
    synced_count = query_one("SELECT COUNT(*) FROM email_sequences WHERE campaign_id = %s AND status = 'synced'", (TEST_CAMPAIGN_ID,))

    from tracking.budget_guard import get_budget_status
    budget = get_budget_status()

    total = total_leads[0] if total_leads else 0
    owners = owners_found[0] if owners_found else 0
    emails = emails_found[0] if emails_found else 0
    seqs = seq_count[0] if seq_count else 0
    synced = synced_count[0] if synced_count else 0

    print("\n")
    print("=" * 60)
    print("          E2E TEST RESULTS — LeadForge v2")
    print("=" * 60)
    print(f"  Leads ingested:        {total}")
    print(f"  Owners found:          {owners} ({owners/total*100:.0f}%)" if total > 0 else "  Owners found:          0")
    print(f"  Emails found:          {emails} ({emails/total*100:.0f}%)" if total > 0 else "  Emails found:          0")
    print(f"    - SEND:              {verdicts.get('SEND', 0)}")
    print(f"    - RISKY:             {verdicts.get('RISKY', 0)}")
    print(f"    - DO NOT SEND:       {verdicts.get('DO NOT SEND', 0)}")
    print(f"    - UNVERIFIED:        {verdicts.get('UNVERIFIED', 0)}")
    print(f"    - No verdict:        {verdicts.get(None, 0)}")
    print(f"  Sequences generated:   {seqs}")
    print(f"  Synced to Instantly:   {synced}")
    print(f"  Total API spend:       ${budget['spent']:.2f}")
    if total > 0:
        print(f"  Cost per lead:         ${budget['spent']/total:.2f}")
    print()

    # Per-service breakdown
    costs = query_all(
        "SELECT service, operation, COUNT(*) as calls, SUM(cost_usd) as cost FROM cost_events WHERE campaign_id = %s GROUP BY service, operation ORDER BY cost DESC",
        (TEST_CAMPAIGN_ID,)
    )
    print("  Per-service breakdown:")
    for c in costs:
        print(f"    {c['service']}/{c['operation']:20s} ${float(c['cost']):8.4f}  ({c['calls']} calls)")

    print()
    print("  Campaign ID:  ", TEST_CAMPAIGN_ID)
    print("  Client ID:    ", TEST_CLIENT_ID)

    instantly_id = query_one("SELECT instantly_campaign_id FROM campaigns WHERE campaign_id = %s", (TEST_CAMPAIGN_ID,))
    print("  Instantly ID: ", instantly_id[0] if instantly_id and instantly_id[0] else "N/A")

    print()
    print("  SAFETY: No emails were sent. Instantly campaign is in DRAFT state.")
    print("=" * 60)
    print()


# ============================================================
# Main
# ============================================================

def main():
    start_time = time.time()

    print("\n")
    print("  LeadForge v2 — End-to-End Pipeline Test")
    print("  Target: 20 dentist leads in Orlando, FL")
    print("  SAFETY: No emails will be sent.\n")

    # Phase 0: Pre-flight
    phase_0_preflight()

    # Phase 1: Seed DB
    phase_1_seed_db()

    # Phase 2: Training corpus
    phase_2_training_corpus()

    # Phase 3: Scrape
    inserted = phase_3_scrape()
    if not inserted or inserted == 0:
        logger.error("No leads were inserted. Cannot continue. Check Apify/Apollo.")
        sys.exit(1)

    # Phase 4: Process (enrichment)
    phase_4_process()

    # Phase 5: Launch (email gen + Instantly sync)
    phase_5_launch()

    # Phase 6: Summary
    phase_6_summary()

    elapsed = time.time() - start_time
    print(f"  Total test time: {elapsed/60:.1f} minutes")
    print()


if __name__ == "__main__":
    main()
