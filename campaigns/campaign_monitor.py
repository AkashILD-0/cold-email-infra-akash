import logging
from campaigns.instantly_client import InstantlyClient
from campaigns.client_manager import get_client_api_key
from db import get_connection, save_campaign_metrics
from psycopg2.extras import RealDictCursor
from config import BOUNCE_RATE_PAUSE_THRESHOLD, UNSUBSCRIBE_RATE_PAUSE_THRESHOLD

logger = logging.getLogger(__name__)


def monitor_campaigns() -> dict:
    """Monitor all active campaigns. Returns {checked, paused, alerts}."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT c.*, cl.instantly_api_key
                FROM campaigns c
                JOIN clients cl ON c.client_id = cl.client_id
                WHERE c.status = 'active'
                  AND c.instantly_campaign_id IS NOT NULL
            """)
            campaigns = cur.fetchall()
    finally:
        conn.close()

    stats = {"checked": 0, "paused": 0, "alerts": []}

    for campaign in campaigns:
        try:
            instantly = InstantlyClient(campaign["instantly_api_key"])
            analytics = instantly.get_campaign_analytics(campaign["instantly_campaign_id"])

            if not analytics:
                continue

            # Calculate rates
            sent = analytics.get("sent", 0) or 1  # avoid division by zero
            metrics = {
                "emails_sent": analytics.get("sent", 0),
                "opens": analytics.get("opened", 0),
                "replies": analytics.get("replied", 0),
                "bounces": analytics.get("bounced", 0),
                "unsubscribes": analytics.get("unsubscribed", 0),
                "meetings_booked": analytics.get("meetings_booked", 0),
                "open_rate": analytics.get("opened", 0) / sent,
                "reply_rate": analytics.get("replied", 0) / sent,
                "bounce_rate": analytics.get("bounced", 0) / sent,
            }

            # Save daily snapshot
            save_campaign_metrics(str(campaign["campaign_id"]), metrics)

            # Check thresholds
            if metrics["bounce_rate"] > BOUNCE_RATE_PAUSE_THRESHOLD:
                instantly.pause_campaign(campaign["instantly_campaign_id"])
                _update_campaign_status(str(campaign["campaign_id"]), "paused")
                alert = f"PAUSED {campaign['campaign_name']}: bounce rate {metrics['bounce_rate']:.1%} > {BOUNCE_RATE_PAUSE_THRESHOLD:.0%}"
                stats["alerts"].append(alert)
                stats["paused"] += 1
                logger.warning(alert)

            unsub_rate = analytics.get("unsubscribed", 0) / sent
            if unsub_rate > UNSUBSCRIBE_RATE_PAUSE_THRESHOLD:
                instantly.pause_campaign(campaign["instantly_campaign_id"])
                _update_campaign_status(str(campaign["campaign_id"]), "paused")
                alert = f"PAUSED {campaign['campaign_name']}: unsubscribe rate {unsub_rate:.1%} > {UNSUBSCRIBE_RATE_PAUSE_THRESHOLD:.0%}"
                stats["alerts"].append(alert)
                stats["paused"] += 1
                logger.warning(alert)

            stats["checked"] += 1

        except Exception as e:
            logger.error(f"Monitor error for campaign {campaign['campaign_id']}: {e}")

    logger.info(f"Campaign monitor: {stats}")
    return stats


def _update_campaign_status(campaign_id: str, status: str):
    """Update campaign status in DB."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE campaigns SET status = %s, updated_at = now() WHERE campaign_id = %s",
                (status, campaign_id)
            )
            conn.commit()
    finally:
        conn.close()
