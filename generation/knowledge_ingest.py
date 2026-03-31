"""
YouTube channel ingestion for cold email copywriter knowledge base.

Uses Apify actors to:
1. List videos from a channel (last 12 months)
2. Extract transcripts for each video
3. Store in training_corpus with dedup and channel tagging
"""

import logging
from ingestion.apify_client import run_actor
from db import get_connection
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

# Apify actors — use tilde format for the API URL
TRANSCRIPT_ACTOR = "starvibe~youtube-video-transcript"


def ingest_youtube_channel(channel_url: str, channel_handle: str = "",
                           max_videos: int = 100,
                           campaign_id: str = None) -> dict:
    """Ingest YouTube transcripts from a channel.

    Uses the transcript actor which accepts a channelUrl and returns
    video titles + transcripts directly.

    Returns {ingested, skipped, errors}.
    """
    stats = {"ingested": 0, "skipped": 0, "errors": 0}

    logger.info(f"Ingesting channel: {channel_handle} ({channel_url}), max={max_videos}")

    from datetime import datetime, timedelta
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    items = run_actor(TRANSCRIPT_ACTOR, {
        "channel_url": channel_url,
        "max_videos": max_videos,
        "start_date": start_date,
        "include_transcript_text": True,
    }, timeout=600)

    logger.info(f"Apify returned {len(items)} items for {channel_handle}")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for item in items:
                source_url = item.get("url", "") or item.get("video_url", "")
                title = item.get("title", "") or item.get("video_title", "")
                transcript = (item.get("transcript_text", "")
                              or item.get("transcript", "")
                              or item.get("text", ""))

                if not transcript or len(transcript) < 100:
                    stats["errors"] += 1
                    continue

                try:
                    cur.execute("""
                        INSERT INTO training_corpus
                            (source, source_url, title, content, channel_handle)
                        VALUES ('youtube', %s, %s, %s, %s)
                        ON CONFLICT (source_url) WHERE source_url IS NOT NULL
                        DO NOTHING
                    """, (source_url, title, transcript, channel_handle))

                    if cur.rowcount > 0:
                        stats["ingested"] += 1
                    else:
                        stats["skipped"] += 1
                except Exception as e:
                    logger.error(f"Ingest error for '{title[:40]}': {e}")
                    conn.rollback()
                    stats["errors"] += 1
            conn.commit()
    finally:
        conn.close()

    # Update ingestion cursor
    _update_cursor(channel_handle, channel_url)

    # Track cost
    cost = len(items) * 0.005
    track_cost(campaign_id, None, "apify", "youtube_transcripts", cost_usd=cost)

    logger.info(f"Channel {channel_handle}: {stats}")
    return stats


def bulk_ingest_channels(channels: dict, max_per_channel: int = 100,
                         campaign_id: str = None) -> dict:
    """Ingest transcripts from multiple YouTube channels.

    Args:
        channels: {handle: url, ...}
        max_per_channel: max videos per channel

    Returns aggregate stats.
    """
    total = {"channels": 0, "ingested": 0, "skipped": 0, "errors": 0}

    for handle, url in channels.items():
        logger.info(f"\n{'='*50}\n  Channel: {handle}\n{'='*50}")
        stats = ingest_youtube_channel(url, handle,
                                       max_videos=max_per_channel,
                                       campaign_id=campaign_id)
        total["channels"] += 1
        total["ingested"] += stats["ingested"]
        total["skipped"] += stats["skipped"]
        total["errors"] += stats["errors"]

    logger.info(f"\nBulk ingestion complete: {total}")
    return total


def _update_cursor(channel_handle: str, channel_url: str):
    """Update the ingestion cursor for a channel."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingestion_cursors (channel_handle, channel_url, last_checked)
                VALUES (%s, %s, now())
                ON CONFLICT (channel_handle)
                DO UPDATE SET last_checked = now()
            """, (channel_handle, channel_url))
            conn.commit()
    finally:
        conn.close()
