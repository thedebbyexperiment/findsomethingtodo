from __future__ import annotations

import logging
import re
from datetime import datetime, date

from bs4 import BeautifulSoup

from normalize.schema import Activity, DataType, ExperienceType, ParentParticipation, TimeSlot
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# NYC Parks category slug → experience_type mapping
CATEGORY_MAP = {
    "running": ExperienceType.active,
    "fitness": ExperienceType.active,
    "sports": ExperienceType.active,
    "basketball": ExperienceType.active,
    "soccer": ExperienceType.active,
    "tennis": ExperienceType.active,
    "swimming": ExperienceType.active,
    "skating": ExperienceType.active,
    "yoga": ExperienceType.active,
    "dance": ExperienceType.active,
    "nature": ExperienceType.nature,
    "garden": ExperienceType.nature,
    "bird": ExperienceType.nature,
    "arts": ExperienceType.creative,
    "craft": ExperienceType.creative,
    "art": ExperienceType.creative,
    "music": ExperienceType.performance,
    "concert": ExperienceType.performance,
    "theater": ExperienceType.performance,
    "film": ExperienceType.performance,
    "movie": ExperienceType.performance,
    "education": ExperienceType.educational,
    "history": ExperienceType.educational,
    "tour": ExperienceType.educational,
    "festival": ExperienceType.events,
}


def _map_category(category_text: str) -> tuple[ExperienceType, str]:
    """Map NYC Parks category to experience type."""
    cat_lower = category_text.lower()
    for keyword, exp_type in CATEGORY_MAP.items():
        if keyword in cat_lower:
            return exp_type, category_text
    return ExperienceType.events, category_text or "Park Event"


def _parse_time_slot(time_str: str) -> list[TimeSlot]:
    """Parse time string like '9:00 a.m.' into time slots."""
    if not time_str:
        return []
    match = re.search(r"(\d{1,2}):?\d*\s*(a\.?m\.?|p\.?m\.?)", time_str.lower())
    if match:
        hour = int(match.group(1))
        ampm = match.group(2).replace(".", "")
        if "pm" in ampm and hour != 12:
            hour += 12
        elif "am" in ampm and hour == 12:
            hour = 0
        if hour < 12:
            return [TimeSlot.morning]
        elif hour < 17:
            return [TimeSlot.afternoon]
        else:
            return [TimeSlot.evening]
    return []


class NYCParksScraper(BaseScraper):
    source_name = "nyc_parks"
    rate_limit = 2.0  # respectful scraping

    BASE_URL = "https://www.nycgovparks.org/events"
    MAX_PAGES = 5

    def __init__(self):
        super().__init__()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })

    def fetch_raw(self) -> list[dict]:
        all_events = []
        today = date.today().strftime("%Y-%m-%d")

        for page in range(1, self.MAX_PAGES + 1):
            try:
                url = f"{self.BASE_URL}?date_start={today}&page={page}"
                resp = self._rate_limited_get(url)
                soup = BeautifulSoup(resp.text, "html.parser")
                event_divs = soup.find_all("div", class_="event")

                if not event_divs:
                    break

                for div in event_divs:
                    event = self._parse_event_div(div)
                    if event:
                        all_events.append(event)

                logger.debug("Fetched %d events from page %d", len(event_divs), page)

            except Exception:
                logger.warning("Failed to fetch NYC Parks page %d", page, exc_info=True)
                break

        return all_events

    def _parse_event_div(self, div) -> dict | None:
        """Parse a single event div into a raw dict."""
        try:
            # Title and URL
            title_tag = div.find("h3", class_="event-title")
            if not title_tag:
                return None
            link = title_tag.find("a")
            name = link.get_text(strip=True) if link else title_tag.get_text(strip=True)
            url = f"https://www.nycgovparks.org{link['href']}" if link and link.get("href") else ""

            # Date from month/day spans
            month = div.find("span", class_="cal_month")
            day = div.find("span", class_="cal_day")
            date_str = ""
            if month and day:
                date_str = f"{month.get_text(strip=True)} {day.get_text(strip=True)}"

            # Start/end from meta tags (Schema.org)
            start_meta = div.find("meta", itemprop="startDate")
            end_meta = div.find("meta", itemprop="endDate")
            start_dt = start_meta["content"] if start_meta else ""
            end_dt = end_meta["content"] if end_meta else ""

            # Location
            location_tag = div.find("h4", class_="location")
            venue_name = ""
            address = ""
            borough = ""
            if location_tag:
                venue_span = location_tag.find("span", itemprop="name")
                venue_name = venue_span.get_text(strip=True) if venue_span else ""
                addr_meta = location_tag.find("meta", itemprop="streetAddress")
                address = addr_meta["content"] if addr_meta else ""
                borough_span = location_tag.find("span", itemprop="addressLocality")
                borough = borough_span.get_text(strip=True) if borough_span else ""

            # Description
            desc_span = div.find("span", class_="description")
            description = desc_span.get_text(strip=True) if desc_span else ""

            # Category
            category = ""
            category_links = div.find_all("a", href=lambda h: h and "/events/c" in str(h))
            if category_links:
                category = category_links[0].get_text(strip=True)

            # Free?
            text = div.get_text()
            is_free = "Free" in text

            return {
                "name": name,
                "url": url,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "date_str": date_str,
                "venue_name": venue_name,
                "address": address,
                "borough": borough,
                "description": description,
                "category": category,
                "is_free": is_free,
            }
        except Exception:
            logger.debug("Failed to parse event div", exc_info=True)
            return None

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        activities = []
        for event in raw_items:
            experience_type, category = _map_category(event.get("category", ""))

            # Parse date from ISO start_dt
            event_date = None
            time_slots = []
            start_dt = event.get("start_dt", "")
            if start_dt:
                try:
                    dt = datetime.fromisoformat(start_dt)
                    event_date = dt.date()
                    time_slots = _parse_time_slot(dt.strftime("%I:%M %p"))
                except (ValueError, AttributeError):
                    pass

            # Build address
            address_parts = [event.get("address", ""), event.get("venue_name", "")]
            if event.get("borough"):
                address_parts.append(event["borough"])
            address = ", ".join(p for p in address_parts if p)

            # Build hours display
            hours = ""
            if start_dt and event.get("end_dt"):
                try:
                    s = datetime.fromisoformat(start_dt)
                    e = datetime.fromisoformat(event["end_dt"])
                    hours = f"{s.strftime('%I:%M %p').lstrip('0')}–{e.strftime('%I:%M %p').lstrip('0')}"
                except (ValueError, AttributeError):
                    pass

            # Generate stable ID from URL slug
            url = event.get("url", "")
            slug = url.rstrip("/").split("/")[-1] if url else event["name"][:30].replace(" ", "-").lower()
            event_id = f"nycparks-{slug}"

            try:
                activity = Activity(
                    id=event_id,
                    name=event["name"],
                    category=category,
                    experience_type=experience_type,
                    parent_participation=ParentParticipation.not_required,
                    description=event.get("description", "")[:500],
                    address=address,
                    lat=None,
                    lng=None,
                    age_min=0,
                    age_max=12,
                    price_min=0 if event.get("is_free") else None,
                    price_max=0 if event.get("is_free") else None,
                    price_display="Free" if event.get("is_free") else "",
                    indoor=False,
                    hours=hours,
                    url=url,
                    reservation_required=False,
                    time_slots=time_slots,
                    source="nyc_parks",
                    source_id=slug,
                    event_date=event_date,
                    last_updated=datetime.utcnow(),
                    data_type=DataType.event,
                )
                activities.append(activity)
            except Exception:
                logger.warning("Failed to normalize NYC Parks event: %s", event.get("name"), exc_info=True)

        return activities
