from __future__ import annotations

import logging
from datetime import datetime

import config
from normalize.schema import Activity, DataType, ExperienceType, ParentParticipation, TimeSlot
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Ticketmaster segment/genre → experience_type mapping
GENRE_MAP = {
    "children's theatre": ExperienceType.performance,
    "circus & specialty acts": ExperienceType.performance,
    "dance": ExperienceType.performance,
    "magic & illusion": ExperienceType.performance,
    "puppetry": ExperienceType.performance,
    "ice shows": ExperienceType.performance,
    "music": ExperienceType.performance,
    "film": ExperienceType.performance,
    "comedy": ExperienceType.performance,
    "athletic": ExperienceType.active,
    "sports": ExperienceType.active,
    "baseball": ExperienceType.active,
    "basketball": ExperienceType.active,
    "ice hockey": ExperienceType.active,
    "soccer": ExperienceType.active,
    "football": ExperienceType.active,
    "tennis": ExperienceType.active,
    "wrestling": ExperienceType.active,
    "educational": ExperienceType.educational,
    "hobby/special interest expos": ExperienceType.educational,
    "family": ExperienceType.events,
    "festival": ExperienceType.events,
    "fairs & festivals": ExperienceType.events,
}


def _time_slot_from_local(local_time: str) -> list[TimeSlot]:
    """Determine time slot from HH:MM:SS local time string."""
    try:
        hour = int(local_time.split(":")[0])
        if hour < 12:
            return [TimeSlot.morning]
        elif hour < 17:
            return [TimeSlot.afternoon]
        else:
            return [TimeSlot.evening]
    except (ValueError, IndexError, AttributeError):
        return []


def _extract_price(price_ranges: list[dict]) -> tuple[float | None, float | None, str]:
    """Extract price info from Ticketmaster priceRanges."""
    if not price_ranges:
        return None, None, ""

    mins = []
    maxs = []
    for pr in price_ranges:
        if pr.get("min") is not None:
            mins.append(pr["min"])
        if pr.get("max") is not None:
            maxs.append(pr["max"])

    price_min = min(mins) if mins else None
    price_max = max(maxs) if maxs else None

    if price_min is not None and price_max is not None:
        if price_min == price_max:
            return price_min, price_max, f"${price_min:.0f}"
        return price_min, price_max, f"${price_min:.0f}–${price_max:.0f}"
    elif price_min is not None:
        return price_min, price_min, f"${price_min:.0f}"
    return None, None, ""


def _map_genre(classifications: list[dict]) -> tuple[ExperienceType, str]:
    """Map Ticketmaster classifications to experience type and category."""
    for c in classifications:
        segment = (c.get("segment", {}).get("name") or "").lower()
        genre = (c.get("genre", {}).get("name") or "").lower()
        subgenre = (c.get("subGenre", {}).get("name") or "").lower()

        # Check subgenre, genre, segment in priority order
        for label in (subgenre, genre, segment):
            mapped = GENRE_MAP.get(label)
            if mapped:
                display = (c.get("subGenre", {}).get("name")
                           or c.get("genre", {}).get("name")
                           or c.get("segment", {}).get("name")
                           or "Event")
                return mapped, display

    return ExperienceType.events, "Family Event"


class TicketmasterScraper(BaseScraper):
    source_name = "ticketmaster"
    rate_limit = 0.25  # 5 req/sec allowed

    def __init__(self):
        super().__init__()
        if not config.TICKETMASTER_API_KEY:
            logger.warning("TICKETMASTER_API_KEY not set — scraper will fail")

    # Keywords to search for family/kids events + sports
    SEARCH_KEYWORDS = [
        "disney", "family", "kids", "sesame", "children", "puppet", "circus", "ice show",
        "yankees", "mets", "knicks", "nets", "rangers", "islanders", "nycfc", "liberty",
        "baseball", "basketball", "hockey", "soccer",
        "broadway kids", "lion king", "aladdin", "wicked", "frozen",
        "auto show", "monster truck", "car show", "monster jam",
    ]

    # Major event centers — fetch ALL upcoming events at these venues
    # so the LLM filter can decide what's family-friendly
    VENUE_IDS = [
        "KovZpZAFnIEA",  # Javits Center
        "KovZpZAEnIkA",  # Barclays Center
        "KovZpZAFaE1A",  # Madison Square Garden
        "KovZpa2lUe",    # Prudential Center
        "KovZpZAFaadA",  # UBS Arena
        "KovZpZAFtJAA",  # Citi Field
        "KovZpZAFaEnA",  # Yankee Stadium
        "KovZpZAFae7A",  # MetLife Stadium
    ]

    def fetch_raw(self) -> list[dict]:
        all_events = {}  # keyed by event ID to dedupe across keyword searches
        start_dt = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Fetch all events at major event centers
        for venue_id in self.VENUE_IDS:
            try:
                resp = self._rate_limited_get(
                    "https://app.ticketmaster.com/discovery/v2/events.json",
                    params={
                        "apikey": config.TICKETMASTER_API_KEY,
                        "venueId": venue_id,
                        "size": 100,
                        "page": 0,
                        "sort": "date,asc",
                        "startDateTime": start_dt,
                    },
                )
                data = resp.json()
                events = data.get("_embedded", {}).get("events", [])
                for event in events:
                    all_events[event["id"]] = event
                logger.debug("Fetched %d events from venue %s", len(events), venue_id)
            except Exception:
                logger.warning("Failed to fetch events for venue %s", venue_id, exc_info=True)

        # Keyword-based search
        for keyword in self.SEARCH_KEYWORDS:
            page = 0
            while True:
                resp = self._rate_limited_get(
                    "https://app.ticketmaster.com/discovery/v2/events.json",
                    params={
                        "apikey": config.TICKETMASTER_API_KEY,
                        "latlong": f"{config.NYC_LAT},{config.NYC_LNG}",
                        "radius": str(config.NYC_RADIUS_KM),
                        "unit": "km",
                        "keyword": keyword,
                        "size": 100,
                        "page": page,
                        "sort": "date,asc",
                        "startDateTime": start_dt,
                    },
                )
                data = resp.json()
                embedded = data.get("_embedded", {})
                events = embedded.get("events", [])
                if not events:
                    break

                for event in events:
                    all_events[event["id"]] = event

                page_info = data.get("page", {})
                total_pages = page_info.get("totalPages", 1)
                page += 1

                if page >= total_pages or page >= 3:
                    break

        return list(all_events.values())

    def _collapse_recurring(self, raw_items: list[dict]) -> list[dict]:
        """Collapse recurring shows (same name + venue) into a single entry with the earliest date."""
        grouped: dict[str, list[dict]] = {}
        for event in raw_items:
            name = (event.get("name") or "").strip().lower()
            venues = event.get("_embedded", {}).get("venues", [{}])
            venue_id = venues[0].get("id", "") if venues else ""
            key = f"{name}||{venue_id}"
            grouped.setdefault(key, []).append(event)

        collapsed = []
        for key, events in grouped.items():
            # Sort by date, keep the earliest upcoming
            events.sort(key=lambda e: e.get("dates", {}).get("start", {}).get("localDate", "9999"))
            collapsed.append(events[0])

        removed = len(raw_items) - len(collapsed)
        if removed:
            logger.info("Collapsed %d recurring show dates into %d unique events", len(raw_items), len(collapsed))
        return collapsed

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        raw_items = self._collapse_recurring(raw_items)
        activities = []
        for event in raw_items:
            classifications = event.get("classifications", [])
            experience_type, category = _map_genre(classifications)

            # Date and time
            dates = event.get("dates", {})
            start = dates.get("start", {})
            local_date = start.get("localDate", "")
            local_time = start.get("localTime", "")

            try:
                event_date = datetime.strptime(local_date, "%Y-%m-%d").date() if local_date else None
            except ValueError:
                event_date = None

            # Venue
            venues = event.get("_embedded", {}).get("venues", [{}])
            venue = venues[0] if venues else {}
            address_parts = []
            addr = venue.get("address", {})
            if addr.get("line1"):
                address_parts.append(addr["line1"])
            city = venue.get("city", {}).get("name", "")
            state = venue.get("state", {}).get("stateCode", "")
            if city:
                address_parts.append(f"{city}, {state}" if state else city)
            address = ", ".join(address_parts)

            lat = None
            lng = None
            location = venue.get("location", {})
            if location.get("latitude"):
                try:
                    lat = float(location["latitude"])
                    lng = float(location["longitude"])
                except (ValueError, TypeError):
                    pass

            # Price
            price_ranges = event.get("priceRanges", [])
            price_min, price_max, price_display = _extract_price(price_ranges)

            # URL
            url = event.get("url", "")

            # Hours display
            hours = local_time[:5] if local_time else ""  # HH:MM

            try:
                activity = Activity(
                    id=f"ticketmaster-{event['id']}",
                    name=event.get("name", "Unknown Event"),
                    category=category,
                    experience_type=experience_type,
                    parent_participation=ParentParticipation.not_required,
                    description=event.get("info", "") or event.get("pleaseNote", "") or "",
                    address=address,
                    lat=lat,
                    lng=lng,
                    age_min=0,
                    age_max=12,
                    price_min=price_min,
                    price_max=price_max,
                    price_display=price_display,
                    indoor=True,
                    hours=hours,
                    url=url,
                    reservation_required=True,
                    time_slots=_time_slot_from_local(local_time),
                    source="ticketmaster",
                    source_id=event["id"],
                    event_date=event_date,
                    last_updated=datetime.utcnow(),
                    data_type=DataType.event,
                )
                activities.append(activity)
            except Exception:
                logger.warning("Failed to normalize Ticketmaster event %s", event.get("id"), exc_info=True)

        return activities
