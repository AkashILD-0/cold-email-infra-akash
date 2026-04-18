# Akash Cold Email Infra

A Python-based cold email automation pipeline for **client-CSV-based outreach campaigns** — built for HealthTalk AI and similar clients where leads are provided pre-qualified in Excel/CSV files.

> **Maintained by:** Akash (Infinite Labs Digital)
> **Repo:** `Infinite-Labs-Digital/akash-cold-email-infra`
> **Separate from:** Arjun's scraping-based LeadForge (`infra-cold-email`) — this pipeline skips Apollo/GMaps scraping and owner discovery entirely.

---

## What It Does

```
Client Excel/CSV
      │
      ▼
 Import Leads  ──────────────────────────────► PostgreSQL (Cloud SQL)
      │
      ▼
 Email Validation  (LeadMagic + Million Verifier cascade)
      │
      ▼
 Website Scraping  (per-lead practice website → key insights)
      │
      ▼
 AI Email Generation  (Claude Sonnet 3-email sequence, Haiku review)
      │
      ▼
 Push to Instantly  (per-lead custom variables via PATCH API)
      │
      ▼
 Campaign Live in Instantly  ──► Replies flow into GoHighLevel
```

---

## Key Difference from LeadForge

| | This repo (Akash) | LeadForge (Arjun) |
|---|---|---|
| Lead source | Client-provided Excel/CSV | GMaps / Apollo / Directory scraping |
| Owner discovery | Skipped (client provides) | Automated via Apollo + AI |
| Email validation | Same cascade | Same cascade |
| Email generation | Website-personalized | Template-based |
| Target use case | HealthTalk AI, similar clients | ILD internal campaigns |

---

## Tech Stack

- **Language:** Python 3.11
- **Database:** PostgreSQL (Cloud SQL @ `34.46.61.90`, db `leadgen_db`)
- **AI:** Claude Haiku (website insight extraction, email review) + Claude Sonnet (email generation)
- **Email Validation:** LeadMagic → Million Verifier cascade
- **Cold Email Platform:** Instantly.ai v2 API
- **Web Scraping:** Scrapling (`enrichment/website_scraper.py`)
- **PDF Export:** fpdf2 with Calibri TrueType font

---

## Project Structure

```
cold-email-infra/
│
├── db.py                          # Postgres connection + all DB queries
├── config.py                      # Env vars, constants
├── requirements.txt
│
├── ingestion/
│   ├── csv_importer.py            # Import leads from .csv or .xlsx
│   ├── apollo_client.py           # Apollo.io people search
│   ├── apify_client.py            # Apify actor runner (GMaps etc.)
│   ├── source_router.py           # Route niche+geo to best lead source
│   └── deduplicator.py            # Dedup on domain + name + location
│
├── enrichment/
│   ├── website_scraper.py         # Scrape practice website → text
│   ├── ai_extractor.py            # Haiku: extract insights from scraped text
│   ├── email_waterfall.py         # Find emails via LeadMagic / Apollo
│   ├── owner_discovery.py         # Discover practice owner name
│   └── enrichment_engine.py      # Orchestrate enrichment steps
│
├── validation/
│   ├── cascade_validator.py       # LeadMagic → MV fallback, sets email_verdict
│   ├── leadmagic_client.py        # LeadMagic API wrapper
│   └── million_verifier_client.py # Million Verifier API wrapper
│
├── generation/
│   ├── email_generator.py         # generate_personalized_sequence() — main entry point
│   └── knowledge_base.py          # Campaign brief / case study store
│
├── campaigns/
│   ├── instantly_client.py        # Instantly v2 API wrapper
│   ├── campaign_launcher.py       # Sync validated leads + sequences → Instantly
│   ├── campaign_monitor.py        # Monitor campaign health
│   └── client_manager.py          # Multi-client API key management
│
└── tests/
    ├── run_healthtalk_campaigns.py      # Main runner: import → validate → generate → launch
    ├── personalize_obgyn_batch.py       # 10-at-a-time personalized email batch generator
    ├── push_obgyn_sequences_to_instantly.py  # Push email content to Instantly as custom vars
    ├── export_batch_pdf.py              # Export batch previews to PDF
    └── run_plastic_surgeons_florida.py  # Florida plastic surgeon campaign runner
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in your values:

```
DB_HOST=34.46.61.90
DB_NAME=leadgen_db
DB_USER=...
DB_PASSWORD=...

ANTHROPIC_API_KEY=...
LEADMAGIC_API_KEY=...
MILLION_VERIFIER_API_KEY=...
```

The Instantly API key is stored **per-client in the DB** (`clients.instantly_api_key`), not in `.env`.

---

## Running the HealthTalk AI Pipeline

### Full campaign run (import → validate → generate → push)

```bash
python -m tests.run_healthtalk_campaigns --sheets "OBGYN" --sender "Alex"
```

### Generate personalized emails (10 at a time, review before saving)

```bash
# Preview batch — no DB changes
python -m tests.personalize_obgyn_batch --offset 0 --batch-size 10

# Save after reviewing
python -m tests.personalize_obgyn_batch --offset 0 --save

# Next batch
python -m tests.personalize_obgyn_batch --offset 10 --save
```

### Export batch to PDF for review

```bash
python -m tests.export_batch_pdf --offset 0 --batch-size 10
```

### Push email sequences to Instantly

```bash
# Dry run — show counts only
python -m tests.push_obgyn_sequences_to_instantly --dry-run

# Full push
python -m tests.push_obgyn_sequences_to_instantly
```

---

## How Personalized Emails Work

Each lead gets a unique 3-email sequence generated by AI:

1. **Website scraping** — `enrichment/website_scraper.py` scrapes the practice website
2. **Insight extraction** — Claude Haiku pulls 3–5 key facts (doctor name, specialties, practice philosophy, tech, milestones)
3. **Sequence generation** — Claude Sonnet writes 3 emails using those insights + the campaign brief (case studies, value props)
4. **Review pass** — Claude Haiku checks tone, accuracy, and personalization quality
5. **Stored in DB** — `email_sequences` table, status `draft`
6. **Pushed to Instantly** — each lead's emails stored as Instantly custom variables

### Instantly Integration (Important API Note)

Campaign template uses `{{email_1_body}}`, `{{email_2_body}}`, etc. — Instantly substitutes per-lead values at send time.

Per-lead values are stored via `PATCH /api/v2/leads/{id}` with this exact format:

```json
{
  "custom_variables": {
    "email_1_subject": "...",
    "email_1_body": "...",
    "email_2_subject": "...",
    "email_2_body": "...",
    "email_3_subject": "...",
    "email_3_body": "..."
  }
}
```

> **Warning:** Sending these as flat keys (not nested under `custom_variables`) returns HTTP 200 but silently ignores the values. Always use the nested format.

---

## Active Campaigns

| Specialty | DB Campaign ID | Instantly Campaign ID | Status |
|-----------|---------------|----------------------|--------|
| OBGYN | `b3fafa6f-623d-4c55-a475-0dc6ddfc5e6e` | `735ef703-d8ea-44d1-aa0a-d9356ebfd8eb` | 575 leads pushed ✅ |

---

## Cost Estimate (per lead)

| Step | Cost |
|------|------|
| LeadMagic email validation | $0.010 |
| Million Verifier fallback (~50%) | $0.001 |
| Website scraping | free |
| Claude Sonnet email generation | $0.040 |
| Claude Haiku review | $0.003 |
| **Total** | **~$0.054** |

592 OBGYN leads ≈ **$32 total**