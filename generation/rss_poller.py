"""
RSS-based YouTube channel poller for ongoing knowledge ingestion.

Checks YouTube RSS feeds for new videos, then uses Apify to extract
transcripts for any new content. Designed to run on a weekly timer.
"""

import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import requests
from db import get_connection
from generation.knowledge_ingest import ingest_youtube_channel
from generation.transcript_filter import filter_corpus_batch
from generation.knowledge_base import build_topic_documents

logger = logging.getLogger(__name__)

# YouTube channel IDs (resolved from handles)
# These are stable and don't change. Resolved once, hardcoded.
CHANNEL_IDS = {
    "leadgenjay": "UCjQ3CHEmk-v4lMbJG1z_bQQ",
    "ericnowoslawski": "UCqaBlPrqoFib0xLCdfVTsmg",
    "InstantlyAI": "UCwIsHfKPQETD0bCkDu92Wzg",
    "GrowWithClay": "UCu53J2kB6xLiLqjun0XnZjw",
}


def get_rss_feed_url(channel_id: str) -> str:
    """Build YouTube RSS feed URL from channel ID."""
    return f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"


def check_new_videos(channel_handle: str) -> list:
    """Check RSS feed for videos newer than our last cursor.
    Returns list of {url, title, published} dicts."""
    channel_id = CHANNEL_IDS.get(channel_handle)
    if not channel_id:
        logger.warning(f"Unknown channel handle: {channel_handle}")
        return []

    # Get our last checked timestamp
    last_date = _get_last_video_date(channel_handle)

    # Fetch and parse RSS
    feed_url = get_rss_feed_url(channel_id)
    try:
        resp = requests.get(feed_url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"RSS fetch error for {channel_handle}: {e}")
        return []

    # Parse Atom XML
    root = ET.fromstring(resp.content)
    ns = {"atom": "http://www.w3.org/2005/Atom", "yt": "http://www.youtube.com/xml/schemas/2015"}

    new_videos = []
    for entry in root.findall("atom:entry", ns):
        title_el = entry.find("atom:title", ns)
        link_el = entry.find("atom:link", ns)
        published_el = entry.find("atom:published", ns)
        video_id_el = entry.find("yt:videoId", ns)

        title = title_el.text if title_el is not None else ""
        published_str = published_el.text if published_el is not None else ""
        video_id = video_id_el.text if video_id_el is not None else ""

        if not video_id:
            continue

        video_url = f"https://www.youtube.com/watch?v={video_id}"

        # Parse published date
        try:
            published = datetime.fromisoformat(published_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            published = datetime.now(timezone.utc)

        # Only include videos newer than our cursor
        if last_date and published <= last_date:
            continue

        new_videos.append({
            "url": video_url,
            "title": title,
            "published": published,
        })

    logger.info(f"RSS {channel_handle}: {len(new_videos)} new videos")
    return new_videos


def poll_and_ingest_new() -> dict:
    """Poll all channels for new videos, ingest transcripts, filter, rebuild.
    This is the main function called by the weekly timer."""
    from config import YOUTUBE_CHANNELS

    stats = {"channels_checked": 0, "new_videos": 0,
             "transcripts_ingested": 0, "topics_rebuilt": False}

    any_new = False

    for handle, url in YOUTUBE_CHANNELS.items():
        new_videos = check_new_videos(handle)
        stats["channels_checked"] += 1
        stats["new_videos"] += len(new_videos)

        if new_videos:
            any_new = True
            # Ingest new videos via Apify (only the new ones)
            result = ingest_youtube_channel(
                url, handle,
                max_videos=len(new_videos) + 5  # small buffer
            )
            stats["transcripts_ingested"] += result["ingested"]

            # Update cursor
            latest = max(v["published"] for v in new_videos)
            _update_last_video_date(handle, latest)

    # Filter and rebuild if we got new content
    if any_new:
        logger.info("New content found — filtering and rebuilding topics...")
        filter_corpus_batch()
        build_topic_documents()
        stats["topics_rebuilt"] = True
        logger.info("Knowledge base updated with new content")
    else:
        logger.info("No new videos found across all channels")

    logger.info(f"RSS poll complete: {stats}")
    return stats


def _get_last_video_date(channel_handle: str):
    """Get the last video date from our cursor."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_video_date FROM ingestion_cursors WHERE channel_handle = %s",
                (channel_handle,)
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _update_last_video_date(channel_handle: str, date):
    """Update the last video date cursor."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ingestion_cursors
                SET last_video_date = %s, last_checked = now()
                WHERE channel_handle = %s
            """, (date, channel_handle))
            conn.commit()
    finally:
        conn.close()
