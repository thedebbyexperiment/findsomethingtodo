from __future__ import annotations

import logging
from datetime import datetime

import config
from normalize.schema import Activity, DataType, ExperienceType, ParentParticipation
from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Google Places types → experience_type mapping
TYPE_MAP = {
    "museum": ExperienceType.educational,
    "amusement_park": ExperienceType.active,
    "zoo": ExperienceType.nature,
    "aquarium": ExperienceType.nature,
    "park": ExperienceType.nature,
    "playground": ExperienceType.active,
    "bowling_alley": ExperienceType.active,
    "art_gallery": ExperienceType.creative,
    "library": ExperienceType.educational,
    "movie_theater": ExperienceType.performance,
}

INCLUDED_TYPES = list(TYPE_MAP.keys())

# Google Places types to skip at the scraper level — these produce generic
# venues (e.g. AMC, Regal) that crowd out kid-specific results.  The curated
# scraper already carries kid-specific cinema programs (Sensory Friendly Films,
# Lil' Hawk, Film Forum Jr, etc.).
SKIP_TYPES = {"movie_theater"}

# Names starting with these prefixes are generic chains, not kid-specific
GENERIC_VENUE_PREFIXES = [
    "amc ", "regal ", "angelika", "village east", "metrograph",
    "ipic ", "showcase cinema", "cinepolis",
]

# Text search queries for kid-specific venues that don't fit standard types
TEXT_SEARCHES = [
    ("kids play space NYC", ExperienceType.active),
    ("children's museum NYC", ExperienceType.educational),
    ("kids indoor playground NYC", ExperienceType.active),
    ("family experience store NYC", ExperienceType.events),
    ("kids science center NYC", ExperienceType.educational),
    ("children's theater NYC", ExperienceType.performance),
    ("trampoline park NYC", ExperienceType.active),
    ("kids cooking class NYC", ExperienceType.creative),
    ("kids art studio NYC", ExperienceType.creative),
    ("fire museum NYC", ExperienceType.educational),
    ("kids STEM NYC", ExperienceType.educational),
    ("family fun center NYC", ExperienceType.active),
    ("kids climbing gym NYC", ExperienceType.active),
    ("immersive experience NYC kids", ExperienceType.events),
    ("farmers market NYC", ExperienceType.nature),
    ("flea market NYC", ExperienceType.events),
    ("outdoor market NYC family", ExperienceType.events),
    ("kids bookstore NYC", ExperienceType.educational),
    ("children's bookstore reading NYC", ExperienceType.educational),
    ("bookstore storytime NYC", ExperienceType.educational),
    ("Central Park kids NYC", ExperienceType.nature),
    ("Central Park playground NYC", ExperienceType.active),
    ("Central Park carousel NYC", ExperienceType.active),
    ("Prospect Park kids Brooklyn", ExperienceType.nature),
    ("Hudson River Park playground NYC", ExperienceType.active),
    ("Riverside Park playground NYC", ExperienceType.active),
    ("botanical garden NYC family", ExperienceType.nature),
    ("community garden NYC kids", ExperienceType.nature),
    ("high line NYC", ExperienceType.nature),
    ("public garden NYC family", ExperienceType.nature),
    ("convention center NYC", ExperienceType.events),
    ("event center NYC family", ExperienceType.events),
    ("museum family program NYC", ExperienceType.educational),
    ("kids martial arts NYC drop in", ExperienceType.active),
    ("kids gymnastics NYC open gym", ExperienceType.active),
    ("kids swim class NYC", ExperienceType.active),
    ("kids dance class NYC", ExperienceType.active),
    ("kids music class NYC", ExperienceType.creative),
    ("kids pottery class NYC", ExperienceType.creative),
    ("kids cooking class drop in NYC", ExperienceType.creative),
    ("kids rock climbing NYC", ExperienceType.active),
    ("ice skating rink NYC public", ExperienceType.active),
    ("kids yoga NYC", ExperienceType.active),
    ("coursehorse kids NYC", ExperienceType.educational),
    ("mini golf NYC", ExperienceType.active),
    ("bowling alley NYC family", ExperienceType.active),
    ("arcade family NYC", ExperienceType.active),
    ("amusement park NYC kids", ExperienceType.active),
    ("laser tag NYC kids", ExperienceType.active),
    ("go karts NYC family", ExperienceType.active),
    ("escape room kids NYC", ExperienceType.active),
]

# Venues with these statuses should be excluded
EXCLUDED_STATUSES = {"CLOSED_PERMANENTLY", "CLOSED_TEMPORARILY"}


def _format_hours(periods: list[dict]) -> str:
    """Format Google Places opening hours into a readable string."""
    if not periods:
        return ""
    day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    parts = []
    for period in periods[:7]:  # cap at a week
        open_info = period.get("open", {})
        close_info = period.get("close", {})
        day = open_info.get("day", 0)
        open_time = open_info.get("time", "")
        close_time = close_info.get("time", "")
        if open_time and close_time:
            parts.append(f"{day_names[day]} {open_time[:2]}:{open_time[2:]}-{close_time[:2]}:{close_time[2:]}")
    return "; ".join(parts)


class GooglePlacesScraper(BaseScraper):
    source_name = "google_places"
    rate_limit = 0.5

    def __init__(self):
        super().__init__()
        if not config.GOOGLE_PLACES_API_KEY:
            logger.warning("GOOGLE_PLACES_API_KEY not set — scraper will fail")

    def fetch_raw(self) -> list[dict]:
        """Fetch kid-friendly venues near NYC using Places API (New)."""
        all_places = []

        for place_type in INCLUDED_TYPES:
            try:
                resp = self.session.post(
                    "https://places.googleapis.com/v1/places:searchNearby",
                    headers={
                        "X-Goog-Api-Key": config.GOOGLE_PLACES_API_KEY,
                        "X-Goog-FieldMask": (
                            "places.id,places.displayName,places.formattedAddress,"
                            "places.location,places.types,places.regularOpeningHours,"
                            "places.websiteUri,places.rating,places.priceLevel,"
                            "places.businessStatus"
                        ),
                    },
                    json={
                        "includedTypes": [place_type],
                        "locationRestriction": {
                            "circle": {
                                "center": {"latitude": config.NYC_LAT, "longitude": config.NYC_LNG},
                                "radius": config.NYC_RADIUS_KM * 1000,
                            }
                        },
                        "maxResultCount": 20,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                places = resp.json().get("places", [])
                for p in places:
                    p["_searched_type"] = place_type
                all_places.extend(places)
            except Exception:
                logger.warning("Failed to fetch Google Places for type %s", place_type, exc_info=True)

        # Text search for niche kid-specific venues
        for query, exp_type in TEXT_SEARCHES:
            try:
                resp = self.session.post(
                    "https://places.googleapis.com/v1/places:searchText",
                    headers={
                        "X-Goog-Api-Key": config.GOOGLE_PLACES_API_KEY,
                        "X-Goog-FieldMask": (
                            "places.id,places.displayName,places.formattedAddress,"
                            "places.location,places.types,places.regularOpeningHours,"
                            "places.websiteUri,places.rating,places.priceLevel,"
                            "places.businessStatus"
                        ),
                    },
                    json={
                        "textQuery": query,
                        "locationBias": {
                            "circle": {
                                "center": {"latitude": config.NYC_LAT, "longitude": config.NYC_LNG},
                                "radius": config.NYC_RADIUS_KM * 1000,
                            }
                        },
                        "maxResultCount": 20,
                    },
                    timeout=30,
                )
                resp.raise_for_status()
                places = resp.json().get("places", [])
                for p in places:
                    p["_searched_type"] = "text_search"
                    p["_experience_type"] = exp_type.value
                all_places.extend(places)
            except Exception:
                logger.warning("Failed text search for '%s'", query, exc_info=True)

        return all_places

    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        activities = []
        seen_ids = set()

        for place in raw_items:
            place_id = place.get("id", "")
            if place_id in seen_ids:
                continue
            seen_ids.add(place_id)

            # Skip closed venues
            status = place.get("businessStatus", "")
            if status in EXCLUDED_STATUSES:
                logger.debug("Skipping closed venue: %s (%s)", place.get("displayName", {}).get("text", ""), status)
                continue

            searched_type = place.get("_searched_type", "")
            display_name = place.get("displayName", {}).get("text", "Unknown Venue")
            name_lower = display_name.lower()

            # Skip generic venue types (e.g. movie_theater → AMC, Regal)
            if searched_type in SKIP_TYPES:
                # Allow through if name suggests kid-specific
                if not any(kw in name_lower for kw in ("kids", "children", "family", "junior", "sensory")):
                    logger.debug("Skipping generic %s: %s", searched_type, display_name)
                    continue

            # Skip generic chain venues regardless of search type
            if any(name_lower.startswith(prefix) for prefix in GENERIC_VENUE_PREFIXES):
                logger.debug("Skipping generic chain: %s", display_name)
                continue
            if searched_type == "text_search":
                try:
                    experience_type = ExperienceType(place.get("_experience_type", "events"))
                except ValueError:
                    experience_type = ExperienceType.events
                # Use first Google type as category, or fall back
                gtypes = place.get("types", [])
                category = gtypes[0].replace("_", " ").title() if gtypes else "Attraction"
            else:
                experience_type = TYPE_MAP.get(searched_type, ExperienceType.events)
                category = searched_type.replace("_", " ").title()

            location = place.get("location", {})

            hours_data = place.get("regularOpeningHours", {})
            hours = _format_hours(hours_data.get("periods", []))

            try:
                activity = Activity(
                    id=f"gplaces-{place_id}",
                    name=display_name,
                    category=category,
                    experience_type=experience_type,
                    parent_participation=ParentParticipation.not_required,
                    description="",
                    address=place.get("formattedAddress", ""),
                    lat=location.get("latitude"),
                    lng=location.get("longitude"),
                    age_min=0,
                    age_max=12,
                    price_min=None,
                    price_max=None,
                    price_display="",
                    indoor=searched_type not in ("park", "playground"),
                    hours=hours,
                    url=place.get("websiteUri", ""),
                    reservation_required=False,
                    time_slots=[],
                    source="google_places",
                    source_id=place_id,
                    event_date=None,
                    last_updated=datetime.utcnow(),
                    data_type=DataType.venue,
                )
                activities.append(activity)
            except Exception:
                logger.warning("Failed to normalize Google Place %s", place_id, exc_info=True)

        return activities
