"""Validate activity URLs and remove entries with dead links.

Runs HEAD requests against every URL in the dataset. Flags and removes
activities whose websites are unreachable, parked, or return errors.

Typical runtime: ~2 minutes for 1,100 URLs with concurrency of 20.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from normalize.schema import Activity

logger = logging.getLogger(__name__)

# Domains that are known domain-parking / registrar pages
PARKED_DOMAINS = {
    "godaddy.com",
    "sedoparking.com",
    "hugedomains.com",
    "dan.com",
    "afternic.com",
    "bodis.com",
    "parkingcrew.net",
    "above.com",
}

# Strings in page titles or bodies that indicate a parked/dead domain
PARKED_INDICATORS = [
    "domain is for sale",
    "this domain is parked",
    "buy this domain",
    "domain parking",
    "godaddy",
    "this site can't be reached",
    "registered at namecheap",
    "coming soon",
]

# Timeout per request in seconds
REQUEST_TIMEOUT = 10

# Max concurrent requests
MAX_WORKERS = 20


def _check_url(url: str) -> tuple[str, bool, str]:
    """Check a single URL. Returns (url, is_valid, reason)."""
    if not url:
        return url, True, "no_url"

    try:
        # Try HEAD first (faster)
        resp = requests.head(
            url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": "FindSomethingToDo-Bot/1.0 (link-checker)"},
        )

        # Some servers don't support HEAD — fall back to GET
        if resp.status_code == 405:
            resp = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
                headers={"User-Agent": "FindSomethingToDo-Bot/1.0 (link-checker)"},
                stream=True,  # don't download full body
            )

        # Check for redirect to parked domain
        final_url = resp.url.lower()
        for parked in PARKED_DOMAINS:
            if parked in final_url:
                return url, False, f"redirects to parked domain ({parked})"

        # Check status code
        if resp.status_code >= 400:
            return url, False, f"HTTP {resp.status_code}"

        # For GET responses, check for parked page indicators in body
        if resp.request.method == "GET" and hasattr(resp, "_content"):
            # Only read first 2KB to check for parked indicators
            try:
                content = resp.content[:2048].decode("utf-8", errors="ignore").lower()
                for indicator in PARKED_INDICATORS:
                    if indicator in content:
                        return url, False, f"parked page ({indicator})"
            except Exception:
                pass

        return url, True, "ok"

    except requests.exceptions.SSLError:
        return url, False, "SSL error"
    except requests.exceptions.ConnectionError:
        return url, False, "connection failed"
    except requests.exceptions.Timeout:
        return url, False, "timeout"
    except requests.exceptions.TooManyRedirects:
        return url, False, "too many redirects"
    except Exception as e:
        return url, False, f"error: {type(e).__name__}"


def validate_urls(activities: list[Activity]) -> list[Activity]:
    """Check all activity URLs concurrently and remove dead links.

    Returns the filtered list with dead-link activities removed.
    """
    # Build URL → activity mapping (multiple activities can share a URL)
    url_to_activities: dict[str, list[int]] = {}
    for i, a in enumerate(activities):
        if a.url:
            url_to_activities.setdefault(a.url, []).append(i)

    unique_urls = list(url_to_activities.keys())
    logger.info("URL check: validating %d unique URLs across %d activities", len(unique_urls), len(activities))

    dead_urls: dict[str, str] = {}  # url → reason

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_check_url, url): url for url in unique_urls}
        done = 0
        for future in as_completed(futures):
            done += 1
            url, is_valid, reason = future.result()
            if not is_valid:
                dead_urls[url] = reason
            if done % 100 == 0:
                logger.info("URL check progress: %d/%d", done, len(unique_urls))

    if not dead_urls:
        logger.info("URL check complete: all URLs valid")
        return activities

    # Remove activities with dead URLs
    remove_indices = set()
    for url, reason in dead_urls.items():
        for idx in url_to_activities.get(url, []):
            remove_indices.add(idx)
            logger.warning("Dead URL: %s — %s (%s)", activities[idx].name, url, reason)

    kept = [a for i, a in enumerate(activities) if i not in remove_indices]
    logger.info(
        "URL check complete: %d dead URLs found, removed %d activities (%d → %d)",
        len(dead_urls), len(remove_indices), len(activities), len(kept),
    )
    return kept
