-- Cold Email Infrastructure Database Schema
-- Database: leadgen_db on Cloud SQL instance 34.46.61.90

-- Clients (must exist before campaigns)
CREATE TABLE IF NOT EXISTS clients (
    client_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_name     TEXT NOT NULL,
    instantly_api_key TEXT NOT NULL,
    sending_domains TEXT[],
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Campaigns
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_name   TEXT NOT NULL,
    client_id       UUID REFERENCES clients(client_id),
    niche           TEXT,
    location_scope  TEXT,
    location_detail TEXT,
    status          TEXT DEFAULT 'active',
    instantly_campaign_id TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Leads (column-based enrichment)
CREATE TABLE IF NOT EXISTS leads (
    lead_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id         UUID REFERENCES campaigns(campaign_id),

    -- Business info (filled by ingestion)
    business_name       TEXT,
    business_domain     TEXT,
    website             TEXT,
    phone               TEXT,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    zip                 TEXT,
    country             TEXT DEFAULT 'US',
    rating              NUMERIC(2,1),
    review_count        INTEGER,
    industry            TEXT,
    company_size        TEXT,

    -- Source tracking
    sources             TEXT[],
    ingested_at         TIMESTAMPTZ DEFAULT now(),

    -- Owner discovery
    owner_name          TEXT,
    owner_source        TEXT,
    owner_confidence    TEXT,
    owner_status        TEXT DEFAULT 'pending',

    -- Email finding
    email               TEXT,
    email_source        TEXT,
    email_type          TEXT,
    email_generic       TEXT,

    -- Email validation
    email_verdict       TEXT,
    lm_status           TEXT,
    lm_catchall         BOOLEAN,
    mv_result           TEXT,
    mv_quality_score    INTEGER,

    -- Processing status
    enrichment_status   TEXT DEFAULT 'raw',

    -- Metadata
    raw_data            JSONB,
    updated_at          TIMESTAMPTZ DEFAULT now()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_leads_enrichment_status ON leads(enrichment_status);
CREATE INDEX IF NOT EXISTS idx_leads_campaign_id ON leads(campaign_id);
CREATE INDEX IF NOT EXISTS idx_leads_business_domain ON leads(business_domain);
CREATE INDEX IF NOT EXISTS idx_leads_email_verdict ON leads(email_verdict);
CREATE INDEX IF NOT EXISTS idx_leads_owner_status ON leads(owner_status);

-- Unique constraint for dedup
CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_domain_campaign
    ON leads(business_domain, campaign_id) WHERE business_domain IS NOT NULL;

-- Email sequences
CREATE TABLE IF NOT EXISTS email_sequences (
    sequence_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id         UUID REFERENCES leads(lead_id),
    campaign_id     UUID REFERENCES campaigns(campaign_id),
    email_1_subject TEXT,
    email_1_body    TEXT,
    email_2_subject TEXT,
    email_2_body    TEXT,
    email_3_subject TEXT,
    email_3_body    TEXT,
    status          TEXT DEFAULT 'draft',
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Campaign metrics (daily snapshots)
CREATE TABLE IF NOT EXISTS campaign_metrics (
    metric_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id     UUID REFERENCES campaigns(campaign_id),
    snapshot_date   DATE NOT NULL,
    emails_sent     INTEGER DEFAULT 0,
    opens           INTEGER DEFAULT 0,
    replies         INTEGER DEFAULT 0,
    bounces         INTEGER DEFAULT 0,
    unsubscribes    INTEGER DEFAULT 0,
    meetings_booked INTEGER DEFAULT 0,
    open_rate       NUMERIC(5,4),
    reply_rate      NUMERIC(5,4),
    bounce_rate     NUMERIC(5,4),
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Cost events (every API call logged)
CREATE TABLE IF NOT EXISTS cost_events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id     UUID REFERENCES campaigns(campaign_id),
    lead_id         UUID REFERENCES leads(lead_id),
    service         TEXT NOT NULL,
    operation       TEXT NOT NULL,
    credits_used    NUMERIC,
    cost_usd        NUMERIC(10,6),
    timestamp       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cost_events_campaign ON cost_events(campaign_id);
CREATE INDEX IF NOT EXISTS idx_cost_events_service ON cost_events(service);
CREATE INDEX IF NOT EXISTS idx_cost_events_timestamp ON cost_events(timestamp);

-- Training corpus (YouTube transcripts + other content)
CREATE TABLE IF NOT EXISTS training_corpus (
    corpus_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT NOT NULL,
    source_url      TEXT,
    title           TEXT,
    content         TEXT,
    summary         TEXT,
    channel_handle  TEXT,
    topic_tags      TEXT[],
    relevance_score NUMERIC(3,2),
    filtered_out    BOOLEAN DEFAULT FALSE,
    published_at    TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_training_corpus_source_url
    ON training_corpus(source_url) WHERE source_url IS NOT NULL;

-- Topic-specific research documents (synthesized from training corpus)
CREATE TABLE IF NOT EXISTS research_topics (
    topic_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_slug  TEXT UNIQUE NOT NULL,
    title       TEXT NOT NULL,
    content     TEXT,
    version     INTEGER DEFAULT 1,
    built_at    TIMESTAMPTZ DEFAULT now()
);

-- Ingestion cursors for RSS polling
CREATE TABLE IF NOT EXISTS ingestion_cursors (
    channel_handle  TEXT PRIMARY KEY,
    channel_url     TEXT NOT NULL,
    channel_id      TEXT,
    last_video_date TIMESTAMPTZ,
    last_checked    TIMESTAMPTZ DEFAULT now()
);

-- Campaign briefs (what the sender is selling + how to pitch it)
CREATE TABLE IF NOT EXISTS campaign_briefs (
    brief_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id      UUID REFERENCES campaigns(campaign_id) UNIQUE,
    service_name     TEXT NOT NULL,
    service_detail   TEXT,
    value_prop       TEXT,
    case_studies     JSONB DEFAULT '[]',
    sender_name      TEXT DEFAULT '{sender_name}',
    sender_title     TEXT,
    cta_type         TEXT DEFAULT 'call',
    cta_detail       TEXT,
    custom_notes     TEXT,
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

-- Scrape jobs (job log)
CREATE TABLE IF NOT EXISTS scrape_jobs (
    job_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          TEXT NOT NULL,
    target          TEXT,
    campaign_id     UUID REFERENCES campaigns(campaign_id),
    status          TEXT DEFAULT 'running',
    leads_found     INTEGER DEFAULT 0,
    errors          TEXT,
    started_at      TIMESTAMPTZ DEFAULT now(),
    completed_at    TIMESTAMPTZ
);
