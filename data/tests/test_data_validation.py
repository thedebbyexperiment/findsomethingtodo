"""Validate activities.json before deploy.

Run: cd data && python -m pytest tests/test_data_validation.py -v
"""
from __future__ import annotations

import json
import os
import re

import pytest

DATA_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "public", "data", "activities.json"
)

VALID_EXPERIENCE_TYPES = {"active", "creative", "educational", "nature", "performance", "events"}
VALID_PARENT_PARTICIPATION = {"not_required", "required"}
VALID_DATA_TYPES = {"venue", "event"}
VALID_TIME_SLOTS = {"morning", "afternoon", "evening"}

# 5 boroughs bounding box
NYC_LAT_MIN, NYC_LAT_MAX = 40.49, 40.92
NYC_LNG_MIN, NYC_LNG_MAX = -74.26, -73.68


@pytest.fixture(scope="module")
def data():
    with open(DATA_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def activities(data):
    return data["activities"]


# --- Top-level structure ---


def test_has_disclaimer(data):
    assert "disclaimer" in data
    assert "short" in data["disclaimer"]
    assert "full" in data["disclaimer"]


def test_has_count(data):
    assert data["count"] == len(data["activities"])


def test_has_generated_at(data):
    assert "generated_at" in data


def test_not_empty(activities):
    assert len(activities) > 0, "No activities in export"


# --- Per-activity field validation ---


def test_all_have_id(activities):
    ids = [a["id"] for a in activities]
    assert all(ids), "Some activities have empty IDs"


def test_no_duplicate_ids(activities):
    ids = [a["id"] for a in activities]
    dupes = [i for i in ids if ids.count(i) > 1]
    assert not dupes, f"Duplicate IDs: {set(dupes)}"


def test_all_have_name(activities):
    empty = [a["id"] for a in activities if not a.get("name", "").strip()]
    assert not empty, f"Activities with empty names: {empty}"


def test_no_cancelled_events(activities):
    cancelled = [a["name"] for a in activities if re.search(r"\bcancell?ed\b", a.get("name", ""), re.I)]
    assert not cancelled, f"Cancelled events in data: {cancelled}"


def test_valid_experience_types(activities):
    bad = [(a["id"], a.get("experience_type")) for a in activities if a.get("experience_type") not in VALID_EXPERIENCE_TYPES]
    assert not bad, f"Invalid experience types: {bad}"


def test_valid_parent_participation(activities):
    bad = [(a["id"], a.get("parent_participation")) for a in activities if a.get("parent_participation") not in VALID_PARENT_PARTICIPATION]
    assert not bad, f"Invalid parent_participation: {bad}"


def test_valid_data_types(activities):
    bad = [(a["id"], a.get("data_type")) for a in activities if a.get("data_type") not in VALID_DATA_TYPES]
    assert not bad, f"Invalid data types: {bad}"


def test_valid_time_slots(activities):
    bad = []
    for a in activities:
        for ts in a.get("time_slots", []):
            if ts not in VALID_TIME_SLOTS:
                bad.append((a["id"], ts))
    assert not bad, f"Invalid time slots: {bad}"


def test_ages_make_sense(activities):
    bad = [(a["id"], a["age_min"], a["age_max"]) for a in activities if a.get("age_min", 0) > a.get("age_max", 12)]
    assert not bad, f"Activities where age_min > age_max: {bad}"


def test_ages_in_range(activities):
    bad = [(a["id"], a.get("age_min"), a.get("age_max")) for a in activities
           if a.get("age_min", 0) < 0 or a.get("age_max", 12) > 18]
    assert not bad, f"Activities with ages out of 0-18 range: {bad}"


def test_prices_not_negative(activities):
    bad = [(a["id"], a.get("price_min"), a.get("price_max")) for a in activities
           if (a.get("price_min") is not None and a["price_min"] < 0)
           or (a.get("price_max") is not None and a["price_max"] < 0)]
    assert not bad, f"Activities with negative prices: {bad}"


def test_price_min_lte_max(activities):
    bad = [(a["id"], a["price_min"], a["price_max"]) for a in activities
           if a.get("price_min") is not None and a.get("price_max") is not None and a["price_min"] > a["price_max"]]
    assert not bad, f"Activities where price_min > price_max: {bad}"


def test_coords_in_nyc(activities):
    bad = []
    for a in activities:
        lat, lng = a.get("lat"), a.get("lng")
        if lat is not None and lng is not None:
            if not (NYC_LAT_MIN <= lat <= NYC_LAT_MAX and NYC_LNG_MIN <= lng <= NYC_LNG_MAX):
                bad.append((a["id"], a["name"], lat, lng))
    assert not bad, f"Activities outside NYC bounding box: {bad}"


def test_urls_are_valid(activities):
    bad = []
    for a in activities:
        url = a.get("url", "")
        if url and not url.startswith(("http://", "https://")):
            bad.append((a["id"], url))
    assert not bad, f"Activities with invalid URLs: {bad}"


def test_non_curated_events_have_dates(activities):
    """Curated recurring programs are data_type=event without dates (ongoing). That's OK.
    But scraped events from APIs should always have dates."""
    bad = [a["id"] for a in activities
           if a.get("data_type") == "event" and not a.get("event_date")
           and a.get("source") != "curated"]
    assert not bad, f"Non-curated events without dates: {bad}"


def test_no_grocery_stores(activities):
    grocery_terms = ["grocery", "supermarket", "bodega", "deli"]
    bad = [a["name"] for a in activities
           if any(t in a.get("name", "").lower() for t in grocery_terms)
           or any(t in a.get("category", "").lower() for t in grocery_terms)]
    assert not bad, f"Grocery stores in data: {bad}"


def test_no_obviously_adult_venues(activities):
    adult_terms = ["bar ", "nightclub", "cocktail", "brewery", "winery", "strip club"]
    bad = [a["name"] for a in activities
           if any(t in a.get("name", "").lower() for t in adult_terms)]
    assert not bad, f"Adult venues in data: {bad}"


def test_no_empty_categories(activities):
    bad = [a["id"] for a in activities if not a.get("category", "").strip()]
    assert not bad, f"Activities with empty categories: {bad}"


def test_no_corporate_services(activities):
    corporate_terms = ["corporate", "team building", "team-building", "corporate events",
                       "private events only", "company outing", "offsite"]
    bad = [a["name"] for a in activities
           if any(t in (a.get("name", "") + " " + a.get("description", "")).lower() for t in corporate_terms)]
    assert not bad, f"Corporate/B2B services in data: {bad}"


def test_no_summer_camps(activities):
    """Summer camps are enrollment-based seasonal programs, not drop-in activities."""
    bad = [a["name"] for a in activities
           if "summer camp" in (a.get("category", "") + " " + a.get("name", "")).lower()]
    assert not bad, f"Summer camps in data (should be filtered — not drop-in): {bad}"


def test_indoor_is_boolean_or_none(activities):
    bad = [(a["id"], a["indoor"]) for a in activities
           if a.get("indoor") is not None and not isinstance(a["indoor"], bool)]
    assert not bad, f"Activities with non-boolean indoor: {bad}"
