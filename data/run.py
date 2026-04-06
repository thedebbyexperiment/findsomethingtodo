#!/usr/bin/env python3
"""FindSomethingToDo data pipeline orchestrator.

Usage:
    python run.py --all              # run all scrapers
    python run.py --source seatgeek  # run one scraper
    python run.py --tier 1           # run all Tier 1
    python run.py --export           # just re-export JSON from existing DB
    python run.py --weekend          # export only this weekend's activities
    python run.py --cleanup          # remove past events
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime

import config
from db.store import ActivityStore
from normalize.deduplicator import deduplicate
from normalize.geocoder import geocode_activities
from normalize.normalizer import normalize_with_llm
from normalize.schema import Activity
from normalize.url_checker import validate_urls
from scrapers.seatgeek import SeatGeekScraper
from scrapers.ticketmaster import TicketmasterScraper
from scrapers.eventbrite import EventbriteScraper
from scrapers.nyc_parks import NYCParksScraper
from scrapers.google_places import GooglePlacesScraper
from scrapers.nypl import NYPLScraper
from scrapers.curated import CuratedScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

SCRAPERS = {
    "seatgeek": SeatGeekScraper,
    "ticketmaster": TicketmasterScraper,
    "eventbrite": EventbriteScraper,
    "nyc_parks": NYCParksScraper,
    "google_places": GooglePlacesScraper,
    "nypl": NYPLScraper,
    "curated": CuratedScraper,
}

TIERS = {
    1: ["seatgeek", "ticketmaster", "eventbrite", "nyc_parks", "google_places"],
    2: ["nypl", "curated"],
}


def run_scrapers(sources: list[str]) -> list[Activity]:
    """Run specified scrapers and return all activities."""
    all_activities: list[Activity] = []

    for source_name in sources:
        scraper_cls = SCRAPERS.get(source_name)
        if not scraper_cls:
            logger.warning("Unknown source: %s", source_name)
            continue

        scraper = scraper_cls()
        activities = scraper.run()
        all_activities.extend(activities)

    return all_activities


DISCLAIMER = {
    "short": "Activities are suggestions only. Parents/guardians are solely responsible for determining suitability for their children.",
    "full": (
        "FindSomethingToDo provides activity listings for informational purposes only. "
        "We do not operate, endorse, or guarantee any listed venue, event, or program. "
        "Parents and guardians are solely responsible for (1) verifying that an activity "
        "is age-appropriate and suitable for their child, (2) confirming event details "
        "including dates, times, prices, and availability directly with the venue, "
        "(3) assessing safety, accessibility, and comfort needs such as noise levels, "
        "changing facilities, stroller access, and allergy accommodations, and "
        "(4) supervising their children at all times. Listings may contain errors, "
        "outdated information, or AI-generated summaries. Always confirm details with "
        "the venue before attending. Use of this site constitutes acceptance of these terms."
    ),
    "version": "1.0",
}


def export_json(store: ActivityStore, output_path: str, weekend_only: bool = False):
    """Export activities to JSON file with disclaimer metadata."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if weekend_only:
        activities = store.get_weekend()
    else:
        activities = store.get_all()

    export = {
        "disclaimer": DISCLAIMER,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "count": len(activities),
        "activities": activities,
    }

    with open(output_path, "w") as f:
        json.dump(export, f, indent=2, default=str)

    logger.info("Exported %d activities to %s", len(activities), output_path)


def main():
    parser = argparse.ArgumentParser(description="FindSomethingToDo data pipeline")
    parser.add_argument("--all", action="store_true", help="Run all scrapers")
    parser.add_argument("--source", type=str, help="Run a specific scraper")
    parser.add_argument("--tier", type=int, choices=[1, 2], help="Run all scrapers in a tier")
    parser.add_argument("--export", action="store_true", help="Export JSON from existing DB")
    parser.add_argument("--weekend", action="store_true", help="Export only weekend activities")
    parser.add_argument("--cleanup", action="store_true", help="Remove past events")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM normalization")
    parser.add_argument("--no-geocode", action="store_true", help="Skip geocoding")
    parser.add_argument("--no-url-check", action="store_true", help="Skip URL validation")
    args = parser.parse_args()

    store = ActivityStore(config.DB_PATH)

    # Cleanup past events
    if args.cleanup:
        store.cleanup_past_events()
        logger.info("Cleanup complete. %d activities in DB.", store.get_count())
        return

    # Export only
    if args.export or args.weekend:
        filename = "events_this_weekend.json" if args.weekend else "activities.json"
        output_path = os.path.join(config.OUTPUT_DIR, filename)
        export_json(store, output_path, weekend_only=args.weekend)
        return

    # Determine which scrapers to run
    if args.all:
        sources = list(SCRAPERS.keys())
    elif args.source:
        sources = [args.source]
    elif args.tier:
        sources = TIERS.get(args.tier, [])
    else:
        parser.print_help()
        sys.exit(1)

    # Run pipeline: scrape → normalize → geocode → dedup → store → export
    logger.info("Running scrapers: %s", ", ".join(sources))
    activities = run_scrapers(sources)

    if not activities:
        logger.warning("No activities fetched from any source")
        return

    # LLM normalization
    if not args.no_llm:
        activities = normalize_with_llm(activities)

    # Geocode missing coordinates
    if not args.no_geocode:
        activities = geocode_activities(activities)

    # Deduplicate
    activities = deduplicate(activities)

    # Validate URLs (remove dead links)
    if not args.no_url_check:
        activities = validate_urls(activities)

    # Store in DB
    store.upsert(activities)
    logger.info("Pipeline complete. %d activities in DB.", store.get_count())

    # Export
    export_json(store, os.path.join(config.OUTPUT_DIR, "activities.json"))

    if args.weekend:
        export_json(store, os.path.join(config.OUTPUT_DIR, "events_this_weekend.json"), weekend_only=True)


if __name__ == "__main__":
    main()
