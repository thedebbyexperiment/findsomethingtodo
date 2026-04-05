from __future__ import annotations

import logging
import re
from datetime import datetime, date

from normalize.schema import Activity, DataType, ExperienceType, ParentParticipation, TimeSlot
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

KIDS_KEYWORDS = [
    "baby", "toddler", "storytime", "story time", "kids", "children",
    "family", "craft", "lapsit", "playdate", "play date", "open play",
    "puppet", "preschool", "little movers", "bilingual storytime",
    "stem", "lego", "coding", "art class", "music class",
]

EXPERIENCE_MAP = {
    "storytime": ExperienceType.educational,
    "story time": ExperienceType.educational,
    "craft": ExperienceType.creative,
    "art": ExperienceType.creative,
    "lego": ExperienceType.creative,
    "stem": ExperienceType.educational,
    "coding": ExperienceType.educational,
    "music": ExperienceType.performance,
    "puppet": ExperienceType.performance,
    "play": ExperienceType.active,
    "dance": ExperienceType.active,
}


def _map_experience(name: str) -> ExperienceType:
    name_lower = name.lower()
    for keyword, exp_type in EXPERIENCE_MAP.items():
        if keyword in name_lower:
            return exp_type
    return ExperienceType.educational


class NYPLScraper(BaseScraper):
    source_name = "nypl"
    rate_limit = 1.0

    API_URL = "https://refinery.nypl.org/api/nypl/ndo/v0.1/site-data/events"
    MAX_PAGES = 20

    def fetch_raw(self) -> list[dict]:
        all_events = []

        for page in range(1, self.MAX_PAGES + 1):
            try:
                resp = self._rate_limited_get(
                    self.API_URL,
                    params={"limit": 50, "page": page},
                    headers={"Accept": "application/json"},
                )
                data = resp.json()
                events = data.get("data", [])
                if not events:
                    break

                all_events.extend(events)

                # Check if we've got all pages
                meta = data.get("meta", {})
                total_pages = meta.get("page", {}).get("count", 1)
                if page >= total_pages:
                    break

            except Exception:
                logger.warning("Failed to fetch NYPL API page %d", page, exc_info=True)
                break

        return all_events

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        activities = []
        today = date.today()

        for event in raw_items:
            attrs = event.get("attributes", {})
            name = attrs.get("name", "")

            # Filter for kids events by keyword
            name_lower = name.lower()
            if not any(kw in name_lower for kw in KIDS_KEYWORDS):
                continue

            # Parse dates
            start_str = attrs.get("start-date", "")
            event_date = None
            time_slots = []
            if start_str:
                try:
                    dt = datetime.fromisoformat(start_str)
                    event_date = dt.date()
                    hour = dt.hour
                    if hour < 12:
                        time_slots = [TimeSlot.morning]
                    elif hour < 17:
                        time_slots = [TimeSlot.afternoon]
                    else:
                        time_slots = [TimeSlot.evening]
                except (ValueError, AttributeError):
                    pass

            # Skip past events
            if event_date and event_date < today:
                continue

            # Build hours
            hours = ""
            end_str = attrs.get("end-date", "")
            if start_str and end_str:
                try:
                    s = datetime.fromisoformat(start_str)
                    e = datetime.fromisoformat(end_str)
                    hours = f"{s.strftime('%I:%M %p').lstrip('0')}–{e.strftime('%I:%M %p').lstrip('0')}"
                except (ValueError, AttributeError):
                    pass

            # Description
            desc = attrs.get("description-short", "") or ""
            if not desc:
                desc = attrs.get("description-full", "") or ""
                # Strip HTML
                desc = re.sub(r"<[^>]+>", " ", desc)
                desc = re.sub(r"\s+", " ", desc).strip()

            # URL
            uri = attrs.get("uri", "")
            if isinstance(uri, dict):
                uri = uri.get("path", uri.get("url", ""))
            uri = str(uri) if uri else ""
            url = f"https://www.nypl.org{uri}" if uri and uri.startswith("/") else uri

            event_id = str(attrs.get("event-id", ""))
            slug = uri.rstrip("/").split("/")[-1] if uri else event_id

            requires_registration = attrs.get("registration-type") is not None

            try:
                activity = Activity(
                    id=f"nypl-{event_id}",
                    name=name,
                    category="Library Program",
                    experience_type=_map_experience(name),
                    parent_participation=ParentParticipation.required,
                    description=desc[:500],
                    address="",
                    lat=None,
                    lng=None,
                    age_min=0,
                    age_max=5,  # Most NYPL kids programs are for younger children
                    price_min=0,
                    price_max=0,
                    price_display="Free",
                    indoor=True,
                    hours=hours,
                    url=url,
                    reservation_required=requires_registration,
                    time_slots=time_slots,
                    source="nypl",
                    source_id=event_id,
                    event_date=event_date,
                    last_updated=datetime.utcnow(),
                    data_type=DataType.event,
                )
                activities.append(activity)
            except Exception:
                logger.warning("Failed to normalize NYPL event: %s", name, exc_info=True)

        return activities
