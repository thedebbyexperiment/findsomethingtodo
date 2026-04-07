from __future__ import annotations

import logging

from Levenshtein import ratio as levenshtein_ratio

from normalize.schema import Activity

logger = logging.getLogger(__name__)

# Source priority — higher number = prefer this source's data when merging
SOURCE_PRIORITY = {
    "curated": 5,        # hand-picked, highest trust
    "seatgeek": 4,       # best for prices
    "eventbrite": 3,     # good structured data
    "nyc_parks": 2,      # free events
    "nypl": 2,           # free events
    "ticketmaster": 2,   # ticketed events
    "patch": 2,          # community events
    "google_places": 1,  # enrichment only — lowest priority
}


def _nearby(a: Activity, b: Activity, threshold: float = 0.002) -> bool:
    """Check if two activities are within ~200m of each other."""
    if a.lat and b.lat and a.lng and b.lng:
        return abs(a.lat - b.lat) < threshold and abs(a.lng - b.lng) < threshold
    return False


def _are_duplicates(a: Activity, b: Activity) -> bool:
    """Check if two activities are likely the same event."""
    # Same source + source_id = definite duplicate
    if a.source == b.source and a.source_id == b.source_id:
        return True

    # Different date = not a duplicate (unless both are venues)
    if a.event_date and b.event_date and a.event_date != b.event_date:
        return False

    name_a = a.name.lower()
    name_b = b.name.lower()

    # Name similarity
    name_sim = levenshtein_ratio(name_a, name_b)

    # If names are very similar (>0.9), likely same thing
    if name_sim > 0.9:
        return True

    # Cross-source dedup: a Google Places venue is a duplicate of a curated
    # program when the GP name appears inside the curated name (which has
    # format "Venue: Program") and they are at the same location.
    # e.g. "Intrepid Museum" (gplaces) vs "Intrepid Museum: Kids Week Programs" (curated)
    if _nearby(a, b):
        # One name is a prefix/substring of the other
        if name_a in name_b or name_b in name_a:
            return True
        # Check the venue portion of "Venue: Program" format
        venue_a = name_a.split(":")[0].strip()
        venue_b = name_b.split(":")[0].strip()
        venue_sim = levenshtein_ratio(venue_a, venue_b)
        if venue_sim > 0.7:
            return True

    if name_sim < 0.7:
        return False

    # Names are somewhat similar (0.7-0.9) — check location
    if _nearby(a, b):
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
