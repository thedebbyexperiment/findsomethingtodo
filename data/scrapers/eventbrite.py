from __future__ import annotations

import json
import logging
import re
from datetime import datetime

from bs4 import BeautifulSoup

from normalize.schema import Activity, DataType, ExperienceType, ParentParticipation, TimeSlot
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Search pages to scrape (Eventbrite paginates with ?page=N)
SEARCH_URLS = [
    "https://www.eventbrite.com/d/ny--new-york/family--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/kids--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/children--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/museum-family--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/kids-art-workshop--events/?page={}",
    # Performing arts
    "https://www.eventbrite.com/d/ny--new-york/kids-theater--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/kids-music--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/kids-dance--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/family-concert--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/children-ballet--events/?page={}",
    "https://www.eventbrite.com/d/ny--new-york/community-theater--events/?page={}",
]

MAX_PAGES_PER_SEARCH = 3


def _time_slot_from_hour(hour: int) -> list[TimeSlot]:
    if hour < 12:
        return [TimeSlot.morning]
    elif hour < 17:
        return [TimeSlot.afternoon]
    else:
        return [TimeSlot.evening]


def _parse_price(event: dict) -> tuple[float | None, float | None, str]:
    """Extract price from Eventbrite event data."""
    is_free = event.get("is_free", False)
    if is_free:
        return 0.0, 0.0, "Free"

    # Try ticket_availability or summary price
    ticket_info = event.get("ticket_availability", {})
    min_price = ticket_info.get("minimum_ticket_price", {})
    max_price = ticket_info.get("maximum_ticket_price", {})

    price_min = None
    price_max = None

    if min_price and min_price.get("major_value"):
        try:
            price_min = float(min_price["major_value"])
        except (ValueError, TypeError):
            pass
    if max_price and max_price.get("major_value"):
        try:
            price_max = float(max_price["major_value"])
        except (ValueError, TypeError):
            pass

    # Fallback: try summary string
    if price_min is None:
        summary = event.get("summary", "") or ""
        price_match = re.search(r"\$(\d+(?:\.\d{2})?)", summary)
        if price_match:
            price_min = float(price_match.group(1))
            price_max = price_min

    if price_min is not None:
        if price_max and price_max != price_min:
            return price_min, price_max, f"${price_min:.0f}–${price_max:.0f}"
        return price_min, price_min, f"${price_min:.0f}"

    return None, None, ""


class EventbriteScraper(BaseScraper):
    source_name = "eventbrite"
    rate_limit = 2.0  # respectful scraping

    def __init__(self):
        super().__init__()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

    def fetch_raw(self) -> list[dict]:
        all_events = {}  # keyed by event ID to dedupe

        for url_template in SEARCH_URLS:
            for page in range(1, MAX_PAGES_PER_SEARCH + 1):
                url = url_template.format(page)
                try:
                    resp = self._rate_limited_get(url)
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # Look for __SERVER_DATA__ JSON blob
                    events = []
                    for script in soup.find_all("script"):
                        text = script.string or ""
                        if "window.__SERVER_DATA__" in text:
                            # Extract JSON — greedy match to capture full nested object
                            match = re.search(r"window\.__SERVER_DATA__\s*=\s*(\{.*\})\s*;", text, re.DOTALL)
                            if match:
                                try:
                                    data = json.loads(match.group(1))
                                    search_data = data.get("search_data", data)
                                    results = search_data.get("events", {}).get("results", [])
                                    events = results
                                except json.JSONDecodeError:
                                    logger.warning("Failed to parse __SERVER_DATA__ JSON on page %d", page)

                    if not events:
                        # Try JSON-LD as fallback
                        for script in soup.find_all("script", type="application/ld+json"):
                            try:
                                ld = json.loads(script.string)
                                if isinstance(ld, list):
                                    events.extend(ld)
                                elif isinstance(ld, dict) and ld.get("@type") == "Event":
                                    events.append(ld)
                            except (json.JSONDecodeError, TypeError):
                                pass

                    if not events:
                        logger.debug("No events found on %s", url)
                        break

                    for event in events:
                        eid = str(event.get("id", event.get("@id", "")))
                        if eid:
                            all_events[eid] = event

                    logger.debug("Fetched %d events from %s", len(events), url)

                except Exception:
                    logger.warning("Failed to fetch %s", url, exc_info=True)
                    break

        return list(all_events.values())

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        activities = []
        for event in raw_items:
            # Handle both __SERVER_DATA__ format and JSON-LD format
            eid = str(event.get("id", event.get("@id", "")))
            name = event.get("name", event.get("title", "Untitled Event"))
            if isinstance(name, dict):
                name = name.get("text", "Untitled Event")

            description = event.get("summary", event.get("description", "")) or ""
            if isinstance(description, dict):
                description = description.get("text", "")

            # Date parsing
            start_date_str = event.get("start_date", "")
            start_time_str = event.get("start_time", "")
            event_date = None
            time_slots = []

            if start_date_str:
                try:
                    event_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                except ValueError:
                    pass
            elif event.get("start_date_time"):
                try:
                    dt = datetime.fromisoformat(event["start_date_time"].replace("Z", "+00:00"))
                    event_date = dt.date()
                    time_slots = _time_slot_from_hour(dt.hour)
                except (ValueError, AttributeError):
                    pass

            if start_time_str and not time_slots:
                try:
                    hour = int(start_time_str.split(":")[0])
                    time_slots = _time_slot_from_hour(hour)
                except (ValueError, IndexError):
                    pass

            # Location
            venue = event.get("primary_venue", event.get("venue", {})) or {}
            address = venue.get("address", {}) if isinstance(venue.get("address"), dict) else {}
            address_str = address.get("localized_address_display", "") or venue.get("name", "")

            lat = None
            lng = None
            if address.get("latitude"):
                try:
                    lat = float(address["latitude"])
                    lng = float(address["longitude"])
                except (ValueError, TypeError):
                    pass

            # Price
            price_min, price_max, price_display = _parse_price(event)

            # URL
            url = event.get("url", event.get("tickets_url", ""))

            try:
                activity = Activity(
                    id=f"eventbrite-{eid}",
                    name=name[:200],
                    category="Family & Education",
                    experience_type=ExperienceType.events,
                    parent_participation=ParentParticipation.not_required,
                    description=description[:500],
                    address=address_str,
                    lat=lat,
                    lng=lng,
                    age_min=0,
                    age_max=12,
                    price_min=price_min,
                    price_max=price_max,
                    price_display=price_display,
                    indoor=None,
                    hours=start_time_str or "",
                    url=url,
                    reservation_required=True,
                    time_slots=time_slots,
                    source="eventbrite",
                    source_id=eid,
                    event_date=event_date,
                    last_updated=datetime.utcnow(),
                    data_type=DataType.event,
                )
                activities.append(activity)
            except Exception:
                logger.warning("Failed to normalize Eventbrite event %s", eid, exc_info=True)

        return activities
