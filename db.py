import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

logger = logging.getLogger(__name__)


def get_connection():
    """Connection factory. Each caller gets a fresh connection."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )


# --- Lead Operations ---

def insert_lead(lead_data: dict, campaign_id: str = None) -> str:
    """Insert a lead. Returns lead_id. Uses ON CONFLICT to skip duplicates."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO leads (
                    campaign_id, business_name, business_domain, website,
                    phone, address, city, state, zip, country,
                    rating, review_count, industry, company_size,
                    sources, raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (business_domain, campaign_id)
                    WHERE business_domain IS NOT NULL
                DO UPDATE SET
                    sources = array_cat(leads.sources, EXCLUDED.sources),
                    updated_at = now()
                RETURNING lead_id
            """, (
                campaign_id,
                lead_data.get("business_name"),
                lead_data.get("business_domain"),
                lead_data.get("website"),
                lead_data.get("phone"),
                lead_data.get("address"),
                lead_data.get("city"),
                lead_data.get("state"),
                lead_data.get("zip"),
                lead_data.get("country", "US"),
                lead_data.get("rating"),
                lead_data.get("review_count"),
                lead_data.get("industry"),
                lead_data.get("company_size"),
                lead_data.get("sources", []),
                json.dumps(lead_data.get("raw_data", {})),
            ))
            result = cur.fetchone()
            conn.commit()
            return str(result[0]) if result else None
    finally:
        conn.close()


def get_leads_needing_enrichment(campaign_id: str, batch_size: int = 500) -> list:
    """Get leads where owner or email columns are empty."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM leads
                WHERE campaign_id = %s
                  AND enrichment_status IN ('raw', 'enriching')
                  AND (owner_status = 'pending' OR email IS NULL OR email_verdict IS NULL)
                ORDER BY ingested_at ASC
                LIMIT %s
            """, (campaign_id, batch_size))
            return cur.fetchall()
    finally:
        conn.close()


def update_lead_fields(lead_id: str, fields: dict):
    """Update specific columns on a lead. Dynamic field update."""
    if not fields:
        return
    conn = get_connection()
    try:
        set_clauses = []
        values = []
        for key, value in fields.items():
            set_clauses.append(f"{key} = %s")
            values.append(value)
        set_clauses.append("updated_at = now()")
        values.append(lead_id)

        sql = f"UPDATE leads SET {', '.join(set_clauses)} WHERE lead_id = %s"
        with conn.cursor() as cur:
            cur.execute(sql, values)
            conn.commit()
    finally:
        conn.close()


def get_leads_for_validation(campaign_id: str, batch_size: int = 500) -> list:
    """Get leads with email but no verdict."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM leads
                WHERE campaign_id = %s
                  AND email IS NOT NULL
                  AND email_verdict IS NULL
                ORDER BY ingested_at ASC
                LIMIT %s
            """, (campaign_id, batch_size))
            return cur.fetchall()
    finally:
        conn.close()


def get_leads_for_email_gen(campaign_id: str, batch_size: int = 100) -> list:
    """Get SEND leads that don't have email sequences yet."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT l.* FROM leads l
                LEFT JOIN email_sequences es ON l.lead_id = es.lead_id
                WHERE l.campaign_id = %s
                  AND l.email_verdict = 'SEND'
                  AND es.sequence_id IS NULL
                ORDER BY l.ingested_at ASC
                LIMIT %s
            """, (campaign_id, batch_size))
            return cur.fetchall()
    finally:
        conn.close()


# --- Campaign Operations ---

def create_campaign(name: str, client_id: str, niche: str,
                    location_scope: str, location_detail: str) -> str:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO campaigns (campaign_name, client_id, niche,
                                       location_scope, location_detail)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING campaign_id
            """, (name, client_id, niche, location_scope, location_detail))
            result = cur.fetchone()
            conn.commit()
            return str(result[0])
    finally:
        conn.close()


# --- Client Operations ---

def create_client(name: str, api_key: str, domains: list) -> str:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO clients (client_name, instantly_api_key, sending_domains)
                VALUES (%s, %s, %s)
                RETURNING client_id
            """, (name, api_key, domains))
            result = cur.fetchone()
            conn.commit()
            return str(result[0])
    finally:
        conn.close()


def create_campaign_brief(campaign_id: str, service_name: str, **kwargs) -> str:
    """Create or update a campaign brief. Returns brief_id."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO campaign_briefs (
                    campaign_id, service_name, service_detail, value_prop,
                    case_studies, sender_name, sender_title, cta_type,
                    cta_detail, custom_notes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (campaign_id) DO UPDATE SET
                    service_name = EXCLUDED.service_name,
                    service_detail = EXCLUDED.service_detail,
                    value_prop = EXCLUDED.value_prop,
                    case_studies = EXCLUDED.case_studies,
                    sender_name = EXCLUDED.sender_name,
                    sender_title = EXCLUDED.sender_title,
                    cta_type = EXCLUDED.cta_type,
                    cta_detail = EXCLUDED.cta_detail,
                    custom_notes = EXCLUDED.custom_notes,
                    updated_at = now()
                RETURNING brief_id
            """, (
                campaign_id, service_name,
                kwargs.get("service_detail"),
                kwargs.get("value_prop"),
                json.dumps(kwargs.get("case_studies", [])),
                kwargs.get("sender_name", "{sender_name}"),
                kwargs.get("sender_title"),
                kwargs.get("cta_type", "call"),
                kwargs.get("cta_detail"),
                kwargs.get("custom_notes"),
            ))
            result = cur.fetchone()
            conn.commit()
            return str(result[0])
    finally:
        conn.close()


def get_campaign_brief(campaign_id: str) -> dict:
    """Fetch the brief for a campaign. Returns dict or None."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM campaign_briefs WHERE campaign_id = %s",
                (campaign_id,)
            )
            return cur.fetchone()
    finally:
        conn.close()


def get_client(client_id: str) -> dict:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM clients WHERE client_id = %s", (client_id,))
            return cur.fetchone()
    finally:
        conn.close()


# --- Cost Operations ---

def log_cost_event(campaign_id: str, lead_id: str, service: str,
                   operation: str, credits_used: float, cost_usd: float):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cost_events
                    (campaign_id, lead_id, service, operation, credits_used, cost_usd)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (campaign_id, lead_id, service, operation, credits_used, cost_usd))
            conn.commit()
    finally:
        conn.close()


def get_today_spend() -> float:
    """Total USD spent today across all campaigns."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(SUM(cost_usd), 0) as total
                FROM cost_events
                WHERE timestamp >= CURRENT_DATE
            """)
            return float(cur.fetchone()[0])
    finally:
        conn.close()


def get_campaign_cost_summary(campaign_id: str) -> list:
    """Cost breakdown per service for a campaign."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT service, operation,
                       COUNT(*) as call_count,
                       SUM(cost_usd) as total_cost,
                       SUM(credits_used) as total_credits
                FROM cost_events
                WHERE campaign_id = %s
                GROUP BY service, operation
                ORDER BY total_cost DESC
            """, (campaign_id,))
            return cur.fetchall()
    finally:
        conn.close()


# --- Email Sequence Operations ---

def save_email_sequence(lead_id: str, campaign_id: str, sequences: dict) -> str:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO email_sequences (
                    lead_id, campaign_id,
                    email_1_subject, email_1_body,
                    email_2_subject, email_2_body,
                    email_3_subject, email_3_body
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING sequence_id
            """, (
                lead_id, campaign_id,
                sequences.get("email_1_subject"), sequences.get("email_1_body"),
                sequences.get("email_2_subject"), sequences.get("email_2_body"),
                sequences.get("email_3_subject"), sequences.get("email_3_body"),
            ))
            result = cur.fetchone()
            conn.commit()
            return str(result[0])
    finally:
        conn.close()


# --- Campaign Metrics ---

def save_campaign_metrics(campaign_id: str, metrics: dict):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO campaign_metrics (
                    campaign_id, snapshot_date,
                    emails_sent, opens, replies, bounces, unsubscribes,
                    meetings_booked, open_rate, reply_rate, bounce_rate
                ) VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                campaign_id,
                metrics.get("emails_sent", 0),
                metrics.get("opens", 0),
                metrics.get("replies", 0),
                metrics.get("bounces", 0),
                metrics.get("unsubscribes", 0),
                metrics.get("meetings_booked", 0),
                metrics.get("open_rate"),
                metrics.get("reply_rate"),
                metrics.get("bounce_rate"),
            ))
            conn.commit()
    finally:
        conn.close()
