from __future__ import annotations

import logging
from datetime import datetime, date

import config
from normalize.schema import Activity, DataType, ExperienceType, ParentParticipation, TimeSlot
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# SeatGeek taxonomy → experience_type mapping
TAXONOMY_MAP = {
    "concert": ExperienceType.performance,
    "theater": ExperienceType.performance,
    "broadway_tickets_national": ExperienceType.performance,
    "family": ExperienceType.events,
    "comedy": ExperienceType.performance,
    "dance_performance_tour": ExperienceType.performance,
    "circus": ExperienceType.performance,
    "sports": ExperienceType.active,
    "minor_league_baseball": ExperienceType.active,
    "nba": ExperienceType.active,
    "nhl": ExperienceType.active,
    "mls": ExperienceType.active,
}

FAMILY_KEYWORDS = [
    "kids", "children", "family", "young people", "toddler", "sesame street",
    "disney", "paw patrol", "bluey", "peppa pig", "baby shark", "cocomelon",
    "circus", "ice show", "storytime", "puppet",
]


def _is_family_event(event: dict) -> bool:
    """Check if an event is family/kids oriented."""
    title = event.get("title", "").lower()
    taxonomies = [t.get("name", "").lower() for t in event.get("taxonomies", [])]

    if "family" in taxonomies:
        return True
    return any(kw in title for kw in FAMILY_KEYWORDS)


def _time_slot_from_datetime(dt_str: str) -> list[TimeSlot]:
    """Determine time slot from event datetime."""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        hour = dt.hour
        if hour < 12:
            return [TimeSlot.morning]
        elif hour < 17:
            return [TimeSlot.afternoon]
        else:
            return [TimeSlot.evening]
    except (ValueError, AttributeError):
        return []


def _category_from_taxonomies(taxonomies: list[dict]) -> str:
    """Extract a human-readable category from SeatGeek taxonomies."""
    for t in taxonomies:
        name = t.get("name", "")
        if name:
            return name.replace("_", " ").title()
    return "Event"


class SeatGeekScraper(BaseScraper):
    source_name = "seatgeek"
    rate_limit = 1.0

    def __init__(self):
        super().__init__()
        if not config.SEATGEEK_CLIENT_ID:
            logger.warning("SEATGEEK_CLIENT_ID not set — scraper will fail")

    def fetch_raw(self) -> list[dict]:
        all_events = []
        page = 1
        per_page = 100

        while True:
            resp = self._rate_limited_get(
                "https://api.seatgeek.com/2/events",
                params={
                    "client_id": config.SEATGEEK_CLIENT_ID,
                    "venue.city": "New York",
                    "per_page": per_page,
                    "page": page,
                    "datetime_utc.gte": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                    "sort": "datetime_utc.asc",
                },
            )
            data = resp.json()
            events = data.get("events", [])
            if not events:
                break

            all_events.extend(events)
            page += 1

            # Cap at 500 events to be reasonable
            if len(all_events) >= 500 or page > 5:
                break

        return all_events

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        activities = []
        for event in raw_items:
            if not _is_family_event(event):
                continue

            venue = event.get("venue", {})
            stats = event.get("stats", {})
            lowest = stats.get("lowest_price")
            highest = stats.get("highest_price")

            # Build price display
            if lowest and highest and lowest != highest:
                price_display = f"${lowest}–${highest}/ticket"
            elif lowest:
                price_display = f"${lowest}/ticket"
            else:
                price_display = ""

            # Determine experience type from taxonomies
            experience_type = ExperienceType.events
            for t in event.get("taxonomies", []):
                mapped = TAXONOMY_MAP.get(t.get("name", "").lower())
                if mapped:
                    experience_type = mapped
                    break

            event_date_str = event.get("datetime_utc", "")
            try:
                event_date = datetime.fromisoformat(event_date_str.replace("Z", "+00:00")).date()
            except (ValueError, AttributeError):
                event_date = None

            try:
                activity = Activity(
                    id=f"seatgeek-evt-{event['id']}",
                    name=event.get("title", "Unknown Event"),
                    category=_category_from_taxonomies(event.get("taxonomies", [])),
                    experience_type=experience_type,
                    parent_participation=ParentParticipation.not_required,
                    description=event.get("description", "") or "",
                    address=venue.get("address", "") or "",
                    lat=venue.get("location", {}).get("lat"),
                    lng=venue.get("location", {}).get("lon"),
                    age_min=0,
                    age_max=12,
                    price_min=float(lowest) if lowest else None,
                    price_max=float(highest) if highest else None,
                    price_display=price_display,
                    indoor=True,  # most SeatGeek events are indoor venues
                    hours="",
                    url=event.get("url", ""),
                    reservation_required=True,
                    time_slots=_time_slot_from_datetime(event_date_str),
                    source="seatgeek",
                    source_id=str(event["id"]),
                    event_date=event_date,
                    last_updated=datetime.utcnow(),
                    data_type=DataType.event,
                )
                activities.append(activity)
            except Exception:
                logger.warning("Failed to normalize SeatGeek event %s", event.get("id"), exc_info=True)

        return activities
