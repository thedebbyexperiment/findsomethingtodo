from __future__ import annotations

import logging

from Levenshtein import ratio as levenshtein_ratio

from normalize.schema import Activity

logger = logging.getLogger(__name__)

# Source priority — higher number = prefer this source's data when merging
SOURCE_PRIORITY = {
    "seatgeek": 4,      # best for prices
    "eventbrite": 3,     # good structured data
    "nyc_parks": 2,      # free events
    "google_places": 1,  # enrichment only
}


def _are_duplicates(a: Activity, b: Activity) -> bool:
    """Check if two activities are likely the same event."""
    # Same source + source_id = definite duplicate
    if a.source == b.source and a.source_id == b.source_id:
        return True

    # Different date = not a duplicate (unless both are venues)
    if a.event_date and b.event_date and a.event_date != b.event_date:
        return False

    # Name similarity
    name_sim = levenshtein_ratio(a.name.lower(), b.name.lower())
    if name_sim < 0.7:
        return False

    # If names are very similar (>0.9), likely same thing
    if name_sim > 0.9:
        return True

    # Names are somewhat similar (0.7-0.9) — check location
    if a.lat and b.lat and a.lng and b.lng:
        # Within ~200m
        lat_diff = abs(a.lat - b.lat)
        lng_diff = abs(a.lng - b.lng)
        if lat_diff < 0.002 and lng_diff < 0.002:
            return True

    # Address similarity as fallback
    if a.address and b.address:
        addr_sim = levenshtein_ratio(a.address.lower(), b.address.lower())
        if addr_sim > 0.8:
            return True

    return False


def _merge(a: Activity, b: Activity) -> Activity:
    """Merge two duplicate activities, preferring the higher-priority source."""
    pri_a = SOURCE_PRIORITY.get(a.source, 0)
    pri_b = SOURCE_PRIORITY.get(b.source, 0)

    primary, secondary = (a, b) if pri_a >= pri_b else (b, a)

    # Fill in blanks from the secondary source
    if not primary.description and secondary.description:
        primary.description = secondary.description
    if primary.price_min is None and secondary.price_min is not None:
        primary.price_min = secondary.price_min
        primary.price_max = secondary.price_max
        primary.price_display = secondary.price_display
    if not primary.hours and secondary.hours:
        primary.hours = secondary.hours
    if primary.lat is None and secondary.lat is not None:
        primary.lat = secondary.lat
        primary.lng = secondary.lng
    if not primary.url and secondary.url:
        primary.url = secondary.url

    return primary


def deduplicate(activities: list[Activity]) -> list[Activity]:
    """Remove duplicate activities, merging data from multiple sources."""
    if not activities:
        return []

    result: list[Activity] = []

    for activity in activities:
        merged = False
        for i, existing in enumerate(result):
            if _are_duplicates(activity, existing):
                result[i] = _merge(existing, activity)
                merged = True
                logger.debug("Deduped: '%s' (%s) ≈ '%s' (%s)",
                             activity.name, activity.source, existing.name, existing.source)
                break
        if not merged:
            result.append(activity)

    removed = len(activities) - len(result)
    if removed:
        logger.info("Deduplication removed %d duplicates (%d → %d)", removed, len(activities), len(result))

    return result
