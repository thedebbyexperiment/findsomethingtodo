from __future__ import annotations

import json
import logging
import re
from datetime import datetime

from bs4 import BeautifulSoup

from normalize.schema import Activity, DataType, ExperienceType, ParentParticipation, TimeSlot
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# NYC neighborhood calendar pages on Patch
NEIGHBORHOOD_CALENDARS = [
    "https://patch.com/new-york/upper-west-side-nyc/calendar",
    "https://patch.com/new-york/upper-east-side-nyc/calendar",
    "https://patch.com/new-york/chelsea-ny/calendar",
    "https://patch.com/new-york/east-village/calendar",
    "https://patch.com/new-york/west-village/calendar",
    "https://patch.com/new-york/harlem/calendar",
    "https://patch.com/new-york/midtown-nyc/calendar",
    "https://patch.com/new-york/gramercy-murray-hill/calendar",
    "https://patch.com/new-york/new-york-city/calendar",
    "https://patch.com/new-york/parkslope/calendar",
    "https://patch.com/new-york/williamsburg/calendar",
    "https://patch.com/new-york/heights-dumbo/calendar",
    "https://patch.com/new-york/astoria-long-island-city/calendar",
]

# Keywords to identify family/kids events (case-insensitive)
FAMILY_KEYWORDS = {
    "kids", "children", "family", "families", "toddler", "baby", "babies",
    "youth", "junior", "school", "kindergarten", "preschool", "storytime",
    "story time", "puppet", "ages 0", "ages 1", "ages 2", "ages 3",
    "ages 4", "ages 5", "ages 6", "ages 7", "ages 8", "ages 9",
    "ages 10", "ages 11", "ages 12", "all ages", "kid-friendly",
    "kid friendly", "child", "young people", "little ones",
    "theater for young", "concert for kids", "community orchestra",
    "school production", "recital", "spring show", "holiday show",
    "dance recital", "art workshop", "craft", "stem", "science fair",
    "easter egg", "halloween", "trick or treat", "playground",
    "festival", "fair", "carnival", "parade", "free!",
}


def _time_slot_from_hour(hour: int) -> list[TimeSlot]:
    if hour < 12:
        return [TimeSlot.morning]
    elif hour < 17:
        return [TimeSlot.afternoon]
    return [TimeSlot.evening]


def _is_family_relevant(name: str, description: str) -> bool:
    """Check if the event seems family/kid relevant."""
    text = (name + " " + description).lower()
    return any(kw in text for kw in FAMILY_KEYWORDS)


class PatchScraper(BaseScraper):
    source_name = "patch"
    rate_limit = 2.0  # respectful scraping

    def __init__(self):
        super().__init__()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def fetch_raw(self) -> list[dict]:
        all_events = {}  # keyed by event UUID to dedupe across neighborhoods

        for cal_url in NEIGHBORHOOD_CALENDARS:
            try:
                resp = self._rate_limited_get(cal_url)
                soup = BeautifulSoup(resp.text, "html.parser")

                # Find event links on the calendar page
                event_links = set()
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if "/calendar/event/" in href:
                        if href.startswith("/"):
                            href = "https://patch.com" + href
                        event_links.add(href)

                logger.debug("Found %d event links on %s", len(event_links), cal_url)

                # Fetch each event detail page for JSON-LD
                for event_url in event_links:
                    # Extract UUID from URL to dedupe
                    uuid_match = re.search(r"/calendar/event/\d+/([a-f0-9-]+)/", event_url)
                    if not uuid_match:
                        continue
                    uuid = uuid_match.group(1)
                    if uuid in all_events:
                        continue

                    try:
                        detail_resp = self._rate_limited_get(event_url)
                        detail_soup = BeautifulSoup(detail_resp.text, "html.parser")

                        # Look for JSON-LD structured data
                        event_data = None
                        for script in detail_soup.find_all("script", type="application/ld+json"):
                            try:
                                ld = json.loads(script.string)
                                if isinstance(ld, dict) and ld.get("@type") == "Event":
                                    event_data = ld
                                    break
                                elif isinstance(ld, list):
                                    for item in ld:
                                        if isinstance(item, dict) and item.get("@type") == "Event":
                                            event_data = item
                                            break
                            except (json.JSONDecodeError, TypeError):
                                pass

                        if event_data:
                            event_data["_patch_url"] = event_url
                            event_data["_patch_uuid"] = uuid
                            all_events[uuid] = event_data
                        else:
                            # Fallback: parse from HTML
                            title_el = detail_soup.find("h1")
                            if title_el:
                                all_events[uuid] = {
                                    "name": title_el.get_text(strip=True),
                                    "_patch_url": event_url,
                                    "_patch_uuid": uuid,
                                    "_html_fallback": True,
                                }

                    except Exception:
                        logger.debug("Failed to fetch event detail: %s", event_url)
                        continue

            except Exception:
                logger.warning("Failed to fetch calendar: %s", cal_url, exc_info=True)
                continue

        return list(all_events.values())

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        activities = []
        for event in raw_items:
            name = event.get("name", "")
            if isinstance(name, dict):
                name = name.get("text", "")
            name = name.strip()
            if not name:
                continue

            description = event.get("description", "") or ""
            if isinstance(description, dict):
                description = description.get("text", "")

            # Only keep family-relevant events
            if not _is_family_relevant(name, description):
                continue

            # Date parsing
            start_date_str = event.get("startDate", "")
            event_date = None
            time_slots = []
            hours = ""

            if start_date_str:
                try:
                    dt = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
                    event_date = dt.date()
                    hours = dt.strftime("%H:%M")
                    time_slots = _time_slot_from_hour(dt.hour)
                except (ValueError, AttributeError):
                    pass

            # Location from JSON-LD
            location = event.get("location", {})
            venue_name = ""
            address_str = ""
            lat = None
            lng = None

            if isinstance(location, dict):
                venue_name = location.get("name", "")
                addr = location.get("address", {})
                if isinstance(addr, dict):
                    parts = []
                    if addr.get("streetAddress"):
                        parts.append(addr["streetAddress"])
                    if addr.get("addressLocality"):
                        city_state = addr["addressLocality"]
                        if addr.get("addressRegion"):
                            city_state += ", " + addr["addressRegion"]
                        if addr.get("postalCode"):
                            city_state += " " + addr["postalCode"]
                        parts.append(city_state)
                    address_str = ", ".join(parts)
                elif isinstance(addr, str):
                    address_str = addr

                geo = location.get("geo", {})
                if isinstance(geo, dict):
                    try:
                        lat = float(geo.get("latitude", 0))
                        lng = float(geo.get("longitude", 0))
                        if lat == 0 and lng == 0:
                            lat = lng = None
                    except (ValueError, TypeError):
                        pass

            if venue_name and not address_str:
                address_str = venue_name

            # Price — check for "Free" in offers
            price_min = None
            price_max = None
            price_display = ""
            offers = event.get("offers", {})
            if isinstance(offers, dict):
                price_val = offers.get("price")
                if price_val is not None:
                    try:
                        price_min = float(price_val)
                        price_max = price_min
                        price_display = f"${price_min:.0f}" if price_min > 0 else "Free"
                        if price_min == 0:
                            price_display = "Free"
                    except (ValueError, TypeError):
                        if str(price_val).lower() == "free":
                            price_min = 0.0
                            price_max = 0.0
                            price_display = "Free"
            elif isinstance(offers, list):
                for offer in offers:
                    if isinstance(offer, dict) and offer.get("price") is not None:
                        try:
                            p = float(offer["price"])
                            if price_min is None or p < price_min:
                                price_min = p
                            if price_max is None or p > price_max:
                                price_max = p
                        except (ValueError, TypeError):
                            pass

            # Check name for FREE
            if "free" in name.lower() and price_min is None:
                price_min = 0.0
                price_max = 0.0
                price_display = "Free"

            # URL — prefer external registration link, fall back to patch URL
            url = ""
            if isinstance(offers, dict) and offers.get("url"):
                url = offers["url"]
            if not url:
                url = event.get("_patch_url", "")

            uuid = event.get("_patch_uuid", "")

            try:
                activity = Activity(
                    id=f"patch-{uuid}",
                    name=name[:200],
                    category="Community Event",
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
                    hours=hours,
                    url=url,
                    reservation_required=False,
                    time_slots=time_slots,
                    source="patch",
                    source_id=uuid,
                    event_date=event_date,
                    last_updated=datetime.utcnow(),
                    data_type=DataType.event,
                )
                activities.append(activity)
            except Exception:
                logger.warning("Failed to normalize Patch event %s", uuid, exc_info=True)

        return activities
