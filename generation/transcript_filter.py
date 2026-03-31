"""
Two-stage transcript filtering for cold email copywriter knowledge base.

Stage 1: Title-based pre-filter (free, instant) — reject obvious non-content
Stage 2: Haiku-based classification (cheap, ~$0.002/call) — classify and tag topics
"""

import re
import json
import logging
from anthropic import Anthropic
from config import ANTHROPIC_API_KEY, HAIKU_MODEL
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

client = Anthropic(api_key=ANTHROPIC_API_KEY)

# Stage 1: Title patterns that indicate non-copywriting content
REJECT_PATTERNS = [
    r"(?i)\btutorial\b",
    r"(?i)\bwalkthrough\b",
    r"(?i)\bsetup\s+guide\b",
    r"(?i)\bgetting\s+started\b",
    r"(?i)\bhow\s+to\s+set\s+up\b",
    r"(?i)\bhow\s+to\s+install\b",
    r"(?i)\bhow\s+to\s+connect\b",
    r"(?i)\bpricing\s+(update|change|plan)\b",
    r"(?i)\bdemo\s+(walkthrough|overview|video)\b",
    r"(?i)\bintegration\s+guide\b",
    r"(?i)\bAPI\s+(tutorial|guide|overview)\b",
    r"(?i)\bproduct\s+update\b",
    r"(?i)\bchangelog\b",
    r"(?i)\brelease\s+notes\b",
    r"(?i)\bvs\.\s",   # comparison videos (e.g., "Instantly vs SmartLead")
    r"(?i)\baffiliate\b",
]

# Topics the copywriter cares about
VALID_TOPICS = [
    "subject_lines",
    "personalization",
    "sequence_structure",
    "frameworks",
    "mistakes_and_deliverability",
    "industry_tips",
]


def title_prefilter(title: str) -> bool:
    """Stage 1: Returns True if the title likely contains copywriting content.
    Returns False to reject obvious non-content (tutorials, demos, etc.)."""
    if not title:
        return False
    for pattern in REJECT_PATTERNS:
        if re.search(pattern, title):
            logger.debug(f"Title rejected by pattern '{pattern}': {title}")
            return False
    return True


def classify_transcript(title: str, transcript: str,
                        campaign_id: str = None) -> dict:
    """Stage 2: Use Haiku to classify transcript content.
    Returns {relevant: bool, topics: [...], relevance_score: 0-1, reason: str}."""

    preview = transcript[:3000] if transcript else ""

    prompt = f"""Classify this YouTube video transcript for a cold email copywriter's knowledge base.

Title: {title}
Transcript (first 3000 chars):
---
{preview}
---

Classify whether this content is RELEVANT for training a cold email copywriter.

KEEP if it contains:
- Cold email copywriting frameworks, templates, or structures
- Subject line patterns, open rate data, or examples
- Personalization techniques or data points
- Email sequence design (openers, follow-ups, break-ups, timing)
- What's working vs not working in cold email RIGHT NOW
- Deliverability tips that affect email copy (spam words, formatting)
- Industry-specific cold outreach advice
- Reply rate data or A/B test results on email copy

REJECT if it's primarily about:
- Software setup tutorials, dashboard walkthroughs, or tool configuration
- Pricing or plan comparisons
- Data enrichment mechanics (not the outreach strategy)
- General business/marketing advice unrelated to email copy
- Product announcements or feature updates
- Hiring, team building, or company culture

Respond in JSON:
{{"relevant": true/false, "topics": ["subject_lines", "personalization", "sequence_structure", "frameworks", "mistakes_and_deliverability", "industry_tips"], "relevance_score": 0.0-1.0, "reason": "one sentence"}}

Only include topics that are actually covered. relevance_score: 0.8+ = highly relevant, 0.5-0.8 = partially relevant, <0.5 = mostly irrelevant."""

    try:
        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text[text.index("\n") + 1:]
            if text.endswith("```"):
                text = text[:-3].strip()
        result = json.loads(text)
        track_cost(campaign_id, None, "claude_haiku", "transcript_filter", cost_usd=0.002)
        return result
    except Exception as e:
        logger.error(f"Transcript classification error: {e}")
        return {"relevant": True, "topics": [], "relevance_score": 0.5,
                "reason": f"Classification failed: {e}"}


def filter_corpus_batch(campaign_id: str = None) -> dict:
    """Run both filter stages on all unfiltered transcripts in training_corpus.
    Updates filtered_out, topic_tags, and relevance_score columns.
    Returns {total, kept, filtered_out}."""
    from db import get_connection

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT corpus_id, title, content
                FROM training_corpus
                WHERE source = 'youtube'
                  AND filtered_out = FALSE
                  AND topic_tags IS NULL
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    stats = {"total": len(rows), "kept": 0, "filtered_out": 0}
    logger.info(f"Filtering {len(rows)} unprocessed transcripts")

    for corpus_id, title, content in rows:
        # Stage 1: Title pre-filter
        if not title_prefilter(title or ""):
            _update_filter_result(corpus_id, filtered_out=True,
                                  topics=[], score=0.0)
            stats["filtered_out"] += 1
            continue

        # Stage 2: Haiku classification
        result = classify_transcript(title or "", content or "",
                                     campaign_id=campaign_id)

        if not result.get("relevant", False) or result.get("relevance_score", 0) < 0.4:
            _update_filter_result(corpus_id, filtered_out=True,
                                  topics=result.get("topics", []),
                                  score=result.get("relevance_score", 0))
            stats["filtered_out"] += 1
        else:
            _update_filter_result(corpus_id, filtered_out=False,
                                  topics=result.get("topics", []),
                                  score=result.get("relevance_score", 0))
            stats["kept"] += 1

    logger.info(f"Filter results: {stats}")
    return stats


def _update_filter_result(corpus_id: str, filtered_out: bool,
                          topics: list, score: float):
    from db import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE training_corpus
                SET filtered_out = %s, topic_tags = %s, relevance_score = %s
                WHERE corpus_id = %s
            """, (filtered_out, topics, score, corpus_id))
            conn.commit()
    finally:
        conn.close()
