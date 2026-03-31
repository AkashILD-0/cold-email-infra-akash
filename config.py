import os
from dotenv import load_dotenv

load_dotenv()

# --- LeadMagic ---
LEADMAGIC_API_KEY = os.environ.get("LEADMAGIC_API_KEY", "")
LEADMAGIC_BASE_URL = "https://api.leadmagic.io"

# --- Million Verifier ---
MILLION_VERIFIER_API_KEY = os.environ.get("MILLION_VERIFIER_API_KEY", "")
MILLION_VERIFIER_BASE_URL = "https://api.millionverifier.com/api/v3/"

# --- Anthropic ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6-20250326"

# --- Apollo.io ---
APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")
APOLLO_BASE_URL = "https://api.apollo.io/v1"

# --- Apify ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN", "")
APIFY_BASE_URL = "https://api.apify.com/v2"

# --- Instantly.ai ---
INSTANTLY_API_KEY = os.environ.get("INSTANTLY_API_KEY", "")
INSTANTLY_BASE_URL = "https://api.instantly.ai/api/v1"

# --- Google Search ---
GOOGLE_SEARCH_API_KEY = os.environ.get("GOOGLE_SEARCH_API_KEY", "")
GOOGLE_SEARCH_CX = os.environ.get("GOOGLE_SEARCH_CX", "")

# --- Database ---
DB_HOST = os.environ.get("DB_HOST", "34.46.61.90")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "leadgen_db")
DB_USER = os.environ.get("DB_USER", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")

# --- Budget Controls ---
DAILY_API_BUDGET_USD = float(os.environ.get("DAILY_API_BUDGET_USD", "50"))
BUDGET_ALERT_THRESHOLD = 0.80  # Alert at 80% of daily budget

# --- Orchestrator Timing ---
SCRAPE_LOOP_INTERVAL = 300        # 5 minutes between scrape cycles
PROCESS_LOOP_INTERVAL = 21600     # 6 hours between enrichment cycles
LAUNCH_LOOP_INTERVAL = 86400      # 24 hours between launch cycles
PROCESS_BATCH_SIZE = 500          # Leads per enrichment batch

# --- Owner Discovery ---
WEBSITE_SCRAPE_MAX_SUBPAGES = 20
WEBSITE_SCRAPE_TIMEOUT = 10       # seconds per page
OWNER_DISCOVERY_WORKERS = 4       # concurrent workers

# --- Email Waterfall ---
EMAIL_WATERFALL_WORKERS = 6

# --- Validation ---
VALIDATION_WORKERS = 6
MILLION_VERIFIER_DELAY = 0.2      # seconds between MV calls

# --- Campaign Thresholds ---
BOUNCE_RATE_PAUSE_THRESHOLD = 0.05
UNSUBSCRIBE_RATE_PAUSE_THRESHOLD = 0.02

# --- Junk Email Patterns ---
JUNK_EMAIL_PATTERNS = {
    "wixpress", "googleapis", "sentry", "example.com",
    "noreply", "no-reply", "mailer-daemon", "postmaster",
    "wordpress", "squarespace", "godaddy", "hostgator"
}

# --- Contact Page Paths ---
CONTACT_PATHS = [
    "/", "/about", "/about-us", "/contact", "/contact-us",
    "/team", "/our-team", "/staff", "/leadership", "/bio",
    "/our-story", "/meet-the-team", "/people", "/founders",
    "/owner", "/company", "/who-we-are", "/management",
    "/about-the-owner", "/about-me", "/meet-us"
]
