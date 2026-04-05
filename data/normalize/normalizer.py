from __future__ import annotations

import json
import logging
import time

try:
    import anthropic
except ImportError:
    anthropic = None

import config
from normalize.schema import Activity, ExperienceType, ParentParticipation, TimeSlot

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a data normalizer for a kids activities database (ages 0-12) in NYC.
You will receive a batch of event listings. For each one, determine if it's suitable for kids and extract structured fields.
Return a JSON array with one object per event, in the same order. No markdown, just the JSON array."""

BATCH_PROMPT = """For each event below, determine:
1. Could a parent reasonably bring kids (ages 0-12) to this? Be INCLUSIVE — only filter out things that are clearly inappropriate for children (bars, nightclubs, adult comedy, explicit concerts, etc.). Sports events, dog shows, cultural exhibitions, parades, outdoor festivals, and similar "all-ages" events ARE suitable even if not specifically marketed to kids.
2. If suitable, extract structured fields.

Return a JSON array where each element has:
- index: the event number (starting at 0)
- is_family_friendly: true or false
- age_min: integer 0-12 (default 0 if unclear)
- age_max: integer 0-12 (default 12 if unclear)
- experience_type: one of "active", "creative", "educational", "nature", "performance", "events"
- parent_participation: "not_required" or "required"
- indoor: true or false
- time_slots: array of "morning", "afternoon", "evening"

Events:
{events_text}

Return only the JSON array, nothing else."""

BATCH_SIZE = 25

# Sources that are already pre-vetted and don't need LLM review
AUTO_APPROVE_SOURCES = {"curated", "nypl", "nyc_parks"}

# Google Places categories that are obviously kid-friendly
OBVIOUS_KID_CATEGORIES = [
    "playground", "zoo", "aquarium", "children", "museum", "library",
    "park", "bowling", "kids", "toy", "baby", "toddler",
]


def _needs_llm_review(a: Activity) -> bool:
    """Decide whether an activity needs LLM review or can be auto-approved."""
    if a.source in AUTO_APPROVE_SOURCES:
        return False
    if a.source == "google_places":
        text = f"{a.name} {a.category}".lower()
        if any(kw in text for kw in OBVIOUS_KID_CATEGORIES):
            return False
    return True


def _format_event(i: int, a: Activity) -> str:
    desc = a.description[:150] if a.description else ""
    return f"[{i}] Name: {a.name} | Category: {a.category} | Description: {desc} | Hours: {a.hours}"


def _apply_llm_fields(activity: Activity, data: dict) -> None:
    """Apply LLM-normalized fields to an activity in place."""
    if "age_min" in data:
        activity.age_min = max(0, min(12, int(data["age_min"])))
    if "age_max" in data:
        activity.age_max = max(activity.age_min, min(12, int(data["age_max"])))
    if "experience_type" in data:
        try:
            activity.experience_type = ExperienceType(data["experience_type"])
        except ValueError:
            pass
    if "parent_participation" in data:
        try:
            activity.parent_participation = ParentParticipation(data["parent_participation"])
        except ValueError:
            pass
    if "indoor" in data:
        activity.indoor = bool(data["indoor"])
    if "time_slots" in data:
        slots = []
        for s in data["time_slots"]:
            try:
                slots.append(TimeSlot(s))
            except ValueError:
                pass
        if slots:
            activity.time_slots = slots


def normalize_with_llm(activities: list[Activity]) -> list[Activity]:
    """Use Claude to filter non-family-friendly events and normalize fields, in batches.

    Auto-approves activities from trusted sources (curated, NYPL, NYC Parks)
    and obviously kid-friendly Google Places venues to minimize API costs.
    """
    if anthropic is None:
        logger.warning("anthropic package not installed — skipping LLM normalization")
        return activities
    if not config.ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — skipping LLM normalization")
        return activities

    # Split into auto-approve vs needs-review
    auto_approved = []
    needs_review = []
    for a in activities:
        if _needs_llm_review(a):
            needs_review.append(a)
        else:
            auto_approved.append(a)

    logger.info(
        "LLM filter: %d auto-approved, %d need review (batches of %d = ~%d API calls, ~$%.2f)",
        len(auto_approved),
        len(needs_review),
        BATCH_SIZE,
        len(needs_review) // BATCH_SIZE + 1,
        ((len(needs_review) // BATCH_SIZE + 1) * 4000 / 1_000_000) * 0.80
        + ((len(needs_review) // BATCH_SIZE + 1) * 3000 / 1_000_000) * 4.00,
    )

    if not needs_review:
        return activities

    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    kept = list(auto_approved)
    removed = 0

    for batch_start in range(0, len(needs_review), BATCH_SIZE):
        batch = needs_review[batch_start : batch_start + BATCH_SIZE]

        events_text = "\n".join(_format_event(i, a) for i, a in enumerate(batch))
        prompt = BATCH_PROMPT.format(events_text=events_text)

        try:
            response = client.messages.create(
                model=config.LLM_MODEL,
                max_tokens=4000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            results = json.loads(text)

            # Build lookup by index
            result_map = {}
            for r in results:
                result_map[r.get("index", -1)] = r

            for i, activity in enumerate(batch):
                data = result_map.get(i)
                if data is None:
                    kept.append(activity)
                    continue

                if not data.get("is_family_friendly", True):
                    removed += 1
                    logger.debug("Filtered out: %s", activity.name)
                    continue

                _apply_llm_fields(activity, data)
                kept.append(activity)

        except json.JSONDecodeError:
            logger.warning("LLM returned invalid JSON for batch starting at %d — keeping all", batch_start)
            kept.extend(batch)
        except Exception:
            logger.warning("LLM batch failed at %d — keeping all", batch_start, exc_info=True)
            kept.extend(batch)

        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = len(needs_review) // BATCH_SIZE + 1
        logger.info("LLM progress: batch %d/%d", batch_num, total_batches)

        # Small delay to avoid rate limits
        if batch_start + BATCH_SIZE < len(needs_review):
            time.sleep(1.5)

    logger.info("LLM filtering complete: kept %d, removed %d non-family-friendly", len(kept), removed)
    return kept
