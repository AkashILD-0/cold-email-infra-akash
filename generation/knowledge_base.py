"""
Topic-specific knowledge synthesis for cold email copywriter.

Instead of one monolithic research document, builds 6 focused topic documents
from filtered YouTube transcripts. Each topic doc is stored in research_topics
and injected into the email generator at generation time.
"""

import logging
from anthropic import Anthropic
from config import ANTHROPIC_API_KEY, SONNET_MODEL
from db import get_connection
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

client = Anthropic(api_key=ANTHROPIC_API_KEY)

# Topic definitions — each becomes a focused research document
TOPICS = {
    "subject_lines": {
        "title": "Subject Line Patterns & Open Rate Data",
        "prompt": """Extract everything about cold email subject lines:
- Proven patterns with examples (exact phrases that get opens)
- Open rate data and A/B test results
- What lengths, formats, and styles work best
- What to avoid (spam triggers, overused phrases)
- Industry-specific subject line strategies
Include SPECIFIC examples and templates, not generic advice.""",
    },
    "personalization": {
        "title": "Personalization Techniques & Data Points",
        "prompt": """Extract everything about personalizing cold emails:
- What data points to reference (reviews, ratings, website details, recent news)
- Personalization at scale techniques
- First-line personalization patterns that get replies
- How to research prospects quickly
- Compliment-based vs. observation-based personalization
- What level of personalization actually moves the needle
Include SPECIFIC examples of personalized openers.""",
    },
    "sequence_structure": {
        "title": "Email Sequence Design & Timing",
        "prompt": """Extract everything about cold email sequence structure:
- How many emails in a sequence (what data says)
- Optimal timing between emails
- Email 1 structure (hook, value, CTA patterns)
- Follow-up email strategies (different angle, social proof, case study)
- Break-up email patterns (permission close, scarcity)
- When to stop vs. keep going
- Multi-channel sequence strategies (email + LinkedIn)
Include SPECIFIC sequence templates with timing.""",
    },
    "frameworks": {
        "title": "Cold Email Copywriting Frameworks",
        "prompt": """Extract all cold email frameworks and copy structures:
- Named frameworks (AIDA, PAS, Before-After-Bridge, etc.)
- Value-first vs. pain-first approaches
- Email body structure patterns (short paragraphs, one idea per email)
- CTA patterns that get replies (questions vs. statements)
- Tone and voice guidelines (conversational, direct, casual)
- Word count and formatting best practices
Include SPECIFIC templates for each framework.""",
    },
    "mistakes_and_deliverability": {
        "title": "Common Mistakes & Deliverability Tips",
        "prompt": """Extract everything about what kills cold email performance:
- Copy mistakes that tank reply rates
- Spam trigger words and phrases to avoid
- Formatting issues (links, images, HTML vs. plain text)
- Deliverability tips that relate to email copy
- What NOT to say in cold emails
- Common beginner mistakes
- Things that changed recently (what used to work but doesn't anymore)
Include SPECIFIC examples of bad vs. good copy.""",
    },
    "industry_tips": {
        "title": "Industry-Specific Cold Outreach Advice",
        "prompt": """Extract industry/niche-specific cold email advice:
- Tips for reaching small business owners (dentists, lawyers, etc.)
- Tips for B2B SaaS outreach
- Tips for agency outreach
- Tips for e-commerce outreach
- What angles work for different industries
- How to position value propositions by industry
- Local business vs. national company approaches
Include SPECIFIC examples per industry where available.""",
    },
}


def build_topic_documents(campaign_id: str = None) -> dict:
    """Build all topic-specific research documents from filtered transcripts.
    Returns {topics_built, errors}."""
    stats = {"topics_built": 0, "errors": 0}

    for slug, topic_def in TOPICS.items():
        try:
            _build_one_topic(slug, topic_def, campaign_id)
            stats["topics_built"] += 1
            logger.info(f"Built topic: {slug}")
        except Exception as e:
            logger.error(f"Topic build error for {slug}: {e}")
            stats["errors"] += 1

    logger.info(f"Topic synthesis complete: {stats}")
    return stats


def _build_one_topic(slug: str, topic_def: dict, campaign_id: str = None):
    """Build a single topic document from relevant transcripts."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Get transcripts tagged with this topic
            cur.execute("""
                SELECT title, content FROM training_corpus
                WHERE source = 'youtube'
                  AND filtered_out = FALSE
                  AND %s = ANY(topic_tags)
                ORDER BY relevance_score DESC NULLS LAST
            """, (slug,))
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        # Fall back to all non-filtered transcripts if no topic tags yet
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT title, content FROM training_corpus
                    WHERE source IN ('youtube', 'manual')
                      AND filtered_out = FALSE
                    ORDER BY ingested_at DESC
                """)
                rows = cur.fetchall()
        finally:
            conn.close()

    if not rows:
        logger.warning(f"No transcripts available for topic: {slug}")
        return

    # Combine transcripts (cap at 80K chars)
    corpus = "\n\n".join([
        f"=== {row[0]} ===\n{row[1]}"
        for row in rows
    ])[:80000]

    prompt = f"""{topic_def['prompt']}

Analyze these expert cold email transcripts and extract everything relevant to this topic.
Be SPECIFIC — include actual templates, exact phrases, numbers, and examples.
Do not give generic advice. Ground everything in what these experts actually teach.

Transcripts:
---
{corpus}
---

Output a structured, actionable research document that a cold email copywriter can reference when writing emails."""

    response = client.messages.create(
        model=SONNET_MODEL,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}]
    )
    content = response.content[0].text

    # Upsert into research_topics
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO research_topics (topic_slug, title, content, version, built_at)
                VALUES (%s, %s, %s, 1, now())
                ON CONFLICT (topic_slug)
                DO UPDATE SET
                    content = EXCLUDED.content,
                    version = research_topics.version + 1,
                    built_at = now()
            """, (slug, topic_def["title"], content))
            conn.commit()
    finally:
        conn.close()

    track_cost(campaign_id, None, "claude_sonnet", "topic_synthesis", cost_usd=0.10)


def get_topic_documents(topics: list = None) -> str:
    """Retrieve topic-specific research documents.
    If topics is None, returns all topics concatenated."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if topics:
                cur.execute("""
                    SELECT topic_slug, title, content FROM research_topics
                    WHERE topic_slug = ANY(%s)
                    ORDER BY topic_slug
                """, (topics,))
            else:
                cur.execute("""
                    SELECT topic_slug, title, content FROM research_topics
                    ORDER BY topic_slug
                """)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return ""

    return "\n\n".join([
        f"## {row[1]}\n{row[2]}"
        for row in rows
    ])


def get_research_document() -> str:
    """Backwards-compatible: return all topic docs as one research document.
    Falls back to old synthesis table if no topic docs exist."""
    result = get_topic_documents()
    if result:
        return result

    # Fallback: old-style synthesis document
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT content FROM training_corpus
                WHERE source = 'synthesis'
                ORDER BY ingested_at DESC LIMIT 1
            """)
            row = cur.fetchone()
            return row[0] if row else ""
    finally:
        conn.close()


def build_research_document(campaign_id: str = None) -> str:
    """Backwards-compatible wrapper. Builds topic docs and returns combined."""
    build_topic_documents(campaign_id)
    return get_research_document()
