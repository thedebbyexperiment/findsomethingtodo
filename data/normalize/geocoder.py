from __future__ import annotations

import logging

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

from normalize.schema import Activity

logger = logging.getLogger(__name__)

_geocoder = None


def _get_geocoder() -> Nominatim:
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent="FindSomethingToDo/1.0", timeout=10)
    return _geocoder


def geocode_activities(activities: list[Activity]) -> list[Activity]:
    """Fill in missing lat/lng for activities that have an address."""
    geocoder = _get_geocoder()
    missing = [a for a in activities if a.lat is None and a.address]

    if not missing:
        return activities

    logger.info("Geocoding %d activities with missing coordinates", len(missing))

    for activity in missing:
        try:
            # Append NYC to improve geocoding accuracy
            query = activity.address
            if "new york" not in query.lower() and "ny" not in query.lower():
                query += ", New York, NY"

            location = geocoder.geocode(query)
            if location:
                activity.lat = location.latitude
                activity.lng = location.longitude
                logger.debug("Geocoded %s → (%f, %f)", activity.address, activity.lat, activity.lng)
        except GeocoderTimedOut:
            logger.warning("Geocoding timed out for: %s", activity.address)
        except Exception:
            logger.warning("Geocoding failed for: %s", activity.address, exc_info=True)

    return activities
