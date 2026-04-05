from __future__ import annotations

import re

# Common age-related phrases → (age_min, age_max)
AGE_PHRASES = {
    "infant": (0, 1),
    "infants": (0, 1),
    "baby": (0, 1),
    "babies": (0, 1),
    "newborn": (0, 0),
    "toddler": (1, 3),
    "toddlers": (1, 3),
    "preschool": (3, 5),
    "preschooler": (3, 5),
    "preschoolers": (3, 5),
    "pre-k": (3, 5),
    "kindergarten": (5, 6),
    "elementary": (5, 11),
    "tween": (9, 12),
    "tweens": (9, 12),
    "teen": (13, 18),
    "teens": (13, 18),
    "all ages": (0, 12),
    "family-friendly": (0, 12),
    "family friendly": (0, 12),
    "families": (0, 12),
}

# Regex patterns for explicit age ranges
AGE_RANGE_PATTERNS = [
    re.compile(r"ages?\s+(\d{1,2})\s*[-–to]+\s*(\d{1,2})", re.IGNORECASE),
    re.compile(r"(\d{1,2})\s*[-–]\s*(\d{1,2})\s*(?:years?\s*old|yrs?|yo)", re.IGNORECASE),
    re.compile(r"(?:for|ages?)\s+(\d{1,2})\+", re.IGNORECASE),
    re.compile(r"(\d{1,2})\+?\s*(?:years?\s*old|yrs?)\s*(?:and\s*(?:up|older|above))?", re.IGNORECASE),
    re.compile(r"under\s+(\d{1,2})", re.IGNORECASE),
]


def parse_ages(text: str) -> tuple[int, int] | None:
    """Parse age range from text. Returns (age_min, age_max) or None if no age info found."""
    if not text:
        return None

    text_lower = text.lower()

    # Try explicit range patterns first
    for pattern in AGE_RANGE_PATTERNS:
        match = pattern.search(text)
        if match:
            groups = match.groups()
            if len(groups) == 2:
                age_min = int(groups[0])
                age_max = int(groups[1])
                return (min(age_min, 18), min(age_max, 18))
            elif len(groups) == 1:
                age = int(groups[0])
                if "under" in match.group().lower():
                    return (0, min(age, 18))
                else:
                    return (min(age, 18), 12)  # "5+" → 5-12

    # Try phrase matching
    for phrase, ages in AGE_PHRASES.items():
        if phrase in text_lower:
            return ages

    return None
