# Akash Cold Email Infra

> **Client-CSV-based cold email pipeline** for HealthTalk AI and similar clients where leads arrive pre-qualified in Excel/CSV files — no scraping, no owner discovery, straight to validated emails and AI-generated sequences.

**Maintained by:** Akash @ Infinite Labs Digital  
**Repos:** [`AkashILD-0/cold-email-infra-akash`](https://github.com/AkashILD-0/cold-email-infra-akash) · [`Infinite-Labs-Digital/akash-cold-email-infra`](https://github.com/Infinite-Labs-Digital/akash-cold-email-infra)  
**Separate from:** Arjun's scraping-based LeadForge (`infra-cold-email`)

---

## Pipeline Overview

```
Client Excel/CSV
      │
      ▼
  Import Leads ──────────────────────────► PostgreSQL (Cloud SQL)
      │
      ▼
  Email Validation  (LeadMagic + Million Verifier cascade)
      │
      ▼
  Website Scraping  (practice website → key insights)
      │
      ▼
  AI Email Generation  (Gemini 2.5 Flash — 3-email sequence)
      │
      ▼
  Push to Instantly  (per-lead custom variables via PATCH/POST API)
      │
      ▼
  Campaign Live ──────────────────────────► Replies flow into GoHighLevel
```

---

## vs. LeadForge (Arjun's pipeline)

| | **This repo** | **LeadForge** |
|---|---|---|
| Lead source | Client-provided Excel/CSV | GMaps / Apollo / Directory scraping |
| Owner discovery | Skipped — client provides | Automated via Apollo + AI |
| Email generation | Gemini 2.5 Flash (Vertex AI) | Gemini 2.5 Flash |
| Target use case | HealthTalk AI, client campaigns | ILD internal outbound |

---

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.11 |
| Database | PostgreSQL — Google Cloud SQL |
| AI — Email Generation | Gemini 2.5 Flash via Vertex AI |
| AI — Insight Extraction / Review | Claude Haiku 4.5 |
| Email Validation | LeadMagic → Million Verifier cascade |
| Cold Email Platform | Instantly.ai v2 API |
| Web Scraping | Scrapling (`enrichment/website_scraper.py`) |
| PDF Preview Export | fpdf2 + Calibri TrueType |

---

## Project Structure

```
cold-email-infra/
│
├── db.py                              # Postgres connection + all DB helpers
├── config.py                          # Env vars, model names, constants
├── requirements.txt
│
├── ingestion/
│   ├── csv_importer.py                # Import leads from .csv or .xlsx
│   ├── apollo_client.py               # Apollo.io people search
│   ├── apify_client.py                # Apify actor runner (GMaps etc.)
│   ├── source_router.py               # Route niche + geo to best lead source
│   └── deduplicator.py                # Dedup on domain + name + location
│
├── enrichment/
│   ├── website_scraper.py             # Scrape practice website → raw text
│   ├── ai_extractor.py                # Haiku: extract key insights from scraped text
│   ├── email_waterfall.py             # Find emails via LeadMagic / Apollo
│   ├── owner_discovery.py             # Discover practice owner name
│   └── enrichment_engine.py          # Orchestrate enrichment steps
│
├── validation/
│   ├── cascade_validator.py           # LeadMagic → MV fallback, sets email_verdict
│   ├── leadmagic_client.py            # LeadMagic API wrapper
│   └── million_verifier_client.py     # Million Verifier API wrapper
│
├── generation/
│   ├── email_generator.py             # generate_batch() — main generation entry point
│   └── knowledge_base.py              # Campaign brief / case study store
│
├── campaigns/
│   ├── instantly_client.py            # Instantly v2 API wrapper
│   ├── campaign_launcher.py           # Sync leads + sequences → Instantly
│   ├── campaign_monitor.py            # Monitor campaign health metrics
│   └── client_manager.py             # Multi-client Instantly API key management
│
└── tests/
    ├── run_healthtalk_campaigns.py         # Main runner: import → validate → generate → push
    ├── run_ortho_batch.py                  # Orthopedics batch runner (5 leads at a time)
    ├── personalize_obgyn_batch.py          # 10-at-a-time personalized OBGYN email batch
    ├── push_obgyn_sequences_to_instantly.py  # Push email content → Instantly custom variables
    ├── apply_client_edits.py               # Apply exact client-provided text edits to sequences
    ├── apply_global_edits.py               # Campaign-wide regex edits across all sequences
    └── export_batch_pdf.py                 # Export batch previews to PDF for review
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env`:

```env
# Database
DB_HOST=your-cloud-sql-ip
DB_NAME=your-db-name
DB_USER=your-db-user
DB_PASSWORD=your-db-password

# AI — Haiku (insight extraction + review)
ANTHROPIC_API_KEY=your-anthropic-key

# Gemini Flash — email generation (~80x cheaper than Sonnet)
GEMINI_EMAIL_GENERATION=true
GOOGLE_GENAI_USE_VERTEXAI=True
GOOGLE_CLOUD_PROJECT=your-gcp-project
GOOGLE_CLOUD_LOCATION=global

# Email validation
LEADMAGIC_API_KEY=your-leadmagic-key
MILLION_VERIFIER_API_KEY=your-mv-key
```

> **Note:** The Instantly API key is stored **per-client in the DB** (`clients.instantly_api_key`), not in `.env`. This keeps HTAI and ILD accounts cleanly separated.

---

## Running Campaigns

### Full campaign run (import → validate → generate → push)

```bash
python -m tests.run_healthtalk_campaigns --sheets "OBGYN" --sender "Alex"
```

### Add Orthopedics leads in batches

```bash
# Next 5 leads (auto-detects offset)
python -m tests.run_ortho_batch

# Explicit offset
python -m tests.run_ortho_batch --offset 13 --count 5

# Dry run preview only
python -m tests.run_ortho_batch --dry-run
```

### Generate personalized OBGYN emails (10 at a time)

```bash
# Preview — no DB changes
python -m tests.personalize_obgyn_batch --offset 0 --batch-size 10

# Save after reviewing
python -m tests.personalize_obgyn_batch --offset 0 --save

# Next batch
python -m tests.personalize_obgyn_batch --offset 10 --save
```

### Push email sequences to Instantly

```bash
# Dry run — show counts only
python -m tests.push_obgyn_sequences_to_instantly --dry-run \
  --campaign <db-campaign-id> \
  --instantly-campaign <instantly-campaign-id>

# Full push
python -m tests.push_obgyn_sequences_to_instantly \
  --campaign <db-campaign-id> \
  --instantly-campaign <instantly-campaign-id>
```

### Export batch to PDF for review

```bash
python -m tests.export_batch_pdf --offset 0 --batch-size 10
```

---

## Instantly API — Critical Notes

### 1. Custom variables must be nested

`PATCH /api/v2/leads/{id}` with flat keys returns HTTP 200 but **silently ignores** the values:

```json
// ❌ WRONG — silently ignored
{ "email_1_subject": "...", "email_1_body": "..." }

// ✅ CORRECT — must nest under custom_variables
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

### 2. Campaign filter doesn't work server-side

`POST /api/v2/leads/list` with `campaign_id` in the body does **not** filter results — it returns all workspace leads. Always filter client-side:

```python
if item.get("campaign") == campaign_id:
    mapping[item["email"]] = item["id"]
```

### 3. Campaign sequence template

Set once per campaign via `PATCH /api/v2/campaigns/{id}`:

```json
{
  "sequences": [{
    "steps": [
      {"type": "email", "delay": 0, "variants": [{"subject": "{{email_1_subject}}", "body": "{{email_1_body}}"}]},
      {"type": "email", "delay": 3, "variants": [{"subject": "{{email_2_subject}}", "body": "{{email_2_body}}"}]},
      {"type": "email", "delay": 7, "variants": [{"subject": "{{email_3_subject}}", "body": "{{email_3_body}}"}]}
    ]
  }]
}
```

---

## Active Campaigns

| Specialty | Client | Leads Synced |
|-----------|--------|-------------|
| Dentist Outreach | ILD | 3,440 ✅ |
| OBGYN | HTAI | 592 ✅ |
| Orthopedics | HTAI | 4 (in progress) |

> Campaign IDs are stored in the DB and `.env` — not committed to the repo.

---

## Cost Estimate (per lead)

| Step | Old Cost (Sonnet) | New Cost (Gemini Flash) |
|------|-------------------|------------------------|
| LeadMagic email validation | $0.010 | $0.010 |
| Million Verifier fallback (~50%) | $0.001 | $0.001 |
| Website scraping | free | free |
| Email generation (3-sequence) | $0.040 | $0.0005 |
| Review pass | $0.003 | $0.0001 |
| **Total per lead** | **~$0.054** | **~$0.012** |

> Switched to Gemini 2.5 Flash via Vertex AI on 2026-04-23 — ~80x cheaper on generation.  
> Enable with `GEMINI_EMAIL_GENERATION=true` in `.env`.

---

## How Personalized Emails Are Generated

1. **Website scraping** — `enrichment/website_scraper.py` scrapes the practice website
2. **Insight extraction** — Claude Haiku pulls 3–5 key facts (doctor name, specialties, practice philosophy, tech, milestones)
3. **Sequence generation** — Gemini 2.5 Flash writes a 3-email sequence using those insights + campaign brief (case studies, value props)
4. **Review pass** — Claude Haiku checks tone, accuracy, and personalization quality
5. **Stored in DB** — `email_sequences` table with status `draft`
6. **Pushed to Instantly** — per-lead content stored as custom variables, substituted at send time