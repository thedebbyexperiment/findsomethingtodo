"""Unit tests for pipeline components (schema, dedup, store, curated scraper).

Run: cd data && python -m pytest tests/test_pipeline.py -v
"""
from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime

import pytest

from normalize.schema import Activity, ExperienceType, ParentParticipation, DataType, TimeSlot
from normalize.deduplicator import deduplicate, _are_duplicates
from db.store import ActivityStore


# --- Schema tests ---


def _make_activity(**overrides) -> Activity:
    defaults = dict(
        id="test-1",
        name="Test Activity",
        category="Museum",
        experience_type=ExperienceType.educational,
        parent_participation=ParentParticipation.not_required,
        description="A test activity",
        address="123 Test St, New York, NY 10001",
        lat=40.75,
        lng=-73.99,
        age_min=0,
        age_max=12,
        source="test",
        source_id="test-1",
        data_type=DataType.venue,
    )
    defaults.update(overrides)
    return Activity(**defaults)


class TestSchema:
    def test_basic_creation(self):
        a = _make_activity()
        assert a.name == "Test Activity"
        assert a.experience_type == ExperienceType.educational

    def test_age_max_corrected_if_below_min(self):
        a = _make_activity(age_min=5, age_max=3)
        assert a.age_max == 5, "age_max should be raised to age_min"

    def test_age_max_valid(self):
        a = _make_activity(age_min=2, age_max=8)
        assert a.age_max == 8

    def test_export_dict_serializable(self):
        a = _make_activity(
            event_date=date(2026, 4, 10),
            time_slots=[TimeSlot.morning, TimeSlot.afternoon],
        )
        d = a.to_export_dict()
        json.dumps(d)  # should not raise
        assert d["experience_type"] == "educational"
        assert d["time_slots"] == ["morning", "afternoon"]
        assert d["event_date"] == "2026-04-10"

    def test_invalid_experience_type_rejected(self):
        with pytest.raises(ValueError):
            _make_activity(experience_type="invalid")

    def test_age_out_of_range_rejected(self):
        with pytest.raises(ValueError):
            _make_activity(age_min=-1)
        with pytest.raises(ValueError):
            _make_activity(age_max=20)


# --- Deduplication tests ---


class TestDeduplication:
    def test_exact_same_source_id(self):
        a = _make_activity(id="sg-1", source="seatgeek", source_id="evt-100")
        b = _make_activity(id="sg-2", source="seatgeek", source_id="evt-100", name="Different Name")
        assert _are_duplicates(a, b)

    def test_very_similar_names(self):
        a = _make_activity(name="Brooklyn Children's Museum")
        b = _make_activity(name="Brooklyn Childrens Museum", source="google_places", source_id="gp-1")
        assert _are_duplicates(a, b)

    def test_different_events_not_duped(self):
        a = _make_activity(name="Story Time", event_date=date(2026, 4, 10), source_id="a")
        b = _make_activity(name="Story Time", event_date=date(2026, 4, 17), source_id="b")
        assert not _are_duplicates(a, b)

    def test_different_names_not_duped(self):
        a = _make_activity(name="Central Park Zoo", source_id="a")
        b = _make_activity(name="Brooklyn Botanic Garden", source_id="b")
        assert not _are_duplicates(a, b)

    def test_deduplicate_merges(self):
        a = _make_activity(id="sg-1", source="seatgeek", source_id="evt-1",
                           name="Kids Show", price_min=10.0, price_max=25.0, description="")
        b = _make_activity(id="gp-1", source="google_places", source_id="gp-1",
                           name="Kids Show", description="A great show")
        result = deduplicate([a, b])
        assert len(result) == 1
        # SeatGeek has higher priority, so it should be the primary
        assert result[0].source == "seatgeek"
        # Description should be filled from google_places
        assert result[0].description == "A great show"

    def test_deduplicate_empty_list(self):
        assert deduplicate([]) == []

    def test_nearby_similar_names_are_dupes(self):
        a = _make_activity(name="Chelsea Piers Bowling", lat=40.7465, lng=-74.0014, source_id="a")
        b = _make_activity(name="Chelsea Pier Bowling", lat=40.7466, lng=-74.0013, source_id="b")
        assert _are_duplicates(a, b)


# --- Store tests ---


class TestStore:
    @pytest.fixture
    def store(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        return ActivityStore(db_path)

    def test_upsert_and_get_all(self, store):
        activities = [_make_activity(id=f"test-{i}", source_id=f"s-{i}") for i in range(3)]
        store.upsert(activities)
        result = store.get_all()
        assert len(result) == 3

    def test_upsert_replaces_on_same_id(self, store):
        a1 = _make_activity(id="test-1", name="Original")
        a2 = _make_activity(id="test-1", name="Updated")
        store.upsert([a1])
        store.upsert([a2])
        result = store.get_all()
        assert len(result) == 1
        assert result[0]["name"] == "Updated"

    def test_get_count(self, store):
        activities = [_make_activity(id=f"test-{i}", source_id=f"s-{i}") for i in range(5)]
        store.upsert(activities)
        assert store.get_count() == 5

    def test_cleanup_past_events(self, store):
        past = _make_activity(id="past-1", data_type=DataType.event, event_date=date(2020, 1, 1))
        future = _make_activity(id="future-1", data_type=DataType.event, event_date=date(2030, 1, 1))
        venue = _make_activity(id="venue-1", data_type=DataType.venue)
        store.upsert([past, future, venue])
        store.cleanup_past_events()
        assert store.get_count() == 2  # past event removed

    def test_row_to_dict_types(self, store):
        a = _make_activity(indoor=True, reservation_required=False, time_slots=[TimeSlot.morning])
        store.upsert([a])
        result = store.get_all()[0]
        assert result["indoor"] is True
        assert result["reservation_required"] is False
        assert result["time_slots"] == ["morning"]


# --- Curated scraper tests ---


class TestCuratedScraper:
    def test_scraper_produces_activities(self):
        from scrapers.curated import CuratedScraper
        scraper = CuratedScraper()
        activities = scraper.run()
        assert len(activities) > 0

    def test_all_have_required_fields(self):
        from scrapers.curated import CuratedScraper
        scraper = CuratedScraper()
        activities = scraper.run()
        for a in activities:
            assert a.name, f"Empty name for {a.id}"
            assert a.address, f"Empty address for {a.id}"
            assert a.source == "curated"
            assert a.lat is not None, f"Missing lat for {a.name}"
            assert a.lng is not None, f"Missing lng for {a.name}"

    def test_recurring_programs_include_venue_name(self):
        from scrapers.curated import CuratedScraper
        scraper = CuratedScraper()
        activities = scraper.run()
        programs = [a for a in activities if ": " in a.name]
        for p in programs:
            # The venue name should be the prefix before ":"
            venue_name = p.name.split(":")[0].strip()
            assert len(venue_name) > 0, f"Program missing venue prefix: {p.name}"
