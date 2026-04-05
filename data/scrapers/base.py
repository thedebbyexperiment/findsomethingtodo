from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod

import requests

from normalize.schema import Activity

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Base class for all data source scrapers."""

    source_name: str = ""
    rate_limit: float = 1.0  # seconds between requests

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "FindSomethingToDo/1.0"})
        self._last_request_time = 0.0

    def _rate_limited_get(self, url: str, params: dict | None = None, **kwargs) -> requests.Response:
        """Make a GET request with rate limiting and retries."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30, **kwargs)
                self._last_request_time = time.time()
                resp.raise_for_status()
                return resp
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning("Rate limited by %s, waiting %ds", self.source_name, wait)
                    time.sleep(wait)
                    continue
                raise
            except requests.exceptions.RequestException as e:
                if attempt < 2:
                    logger.warning("Request failed for %s (attempt %d): %s", self.source_name, attempt + 1, e)
                    time.sleep(2 ** attempt)
                    continue
                raise

        raise requests.exceptions.RetryError(f"Failed after 3 attempts for {self.source_name}")

    @abstractmethod
    def fetch_raw(self) -> list[dict]:
        """Fetch raw data from the source. Returns a list of raw dicts."""
        ...

    @abstractmethod
    def normalize(self, raw_items: list[dict]) -> list[Activity]:
        """Convert raw data into normalized Activity objects."""
        ...

    def run(self) -> list[Activity]:
        """Full pipeline: fetch → normalize → return."""
        logger.info("Running %s scraper...", self.source_name)
        try:
            raw = self.fetch_raw()
            logger.info("Fetched %d raw items from %s", len(raw), self.source_name)
            activities = self.normalize(raw)
            logger.info("Normalized %d activities from %s", len(activities), self.source_name)
            return activities
        except Exception:
            logger.exception("Error running %s scraper", self.source_name)
            return []
