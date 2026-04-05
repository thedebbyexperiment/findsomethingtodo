from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, date, timedelta

from normalize.schema import Activity

logger = logging.getLogger(__name__)


class ActivityStore:
    """SQLite-backed store for activities."""

    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS activities (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT,
                    experience_type TEXT,
                    parent_participation TEXT,
                    description TEXT,
                    address TEXT,
                    lat REAL,
                    lng REAL,
                    age_min INTEGER DEFAULT 0,
                    age_max INTEGER DEFAULT 12,
                    price_min REAL,
                    price_max REAL,
                    price_display TEXT,
                    indoor INTEGER,
                    hours TEXT,
                    url TEXT,
                    reservation_required INTEGER,
                    time_slots TEXT,
                    seasonal TEXT,
                    source TEXT,
                    source_id TEXT,
                    event_date TEXT,
                    last_updated TEXT,
                    data_type TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_source ON activities(source, source_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_event_date ON activities(event_date)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_type ON activities(data_type)
            """)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def upsert(self, activities: list[Activity]):
        """Insert or update activities."""
        with self._conn() as conn:
            for a in activities:
                conn.execute(
                    """INSERT OR REPLACE INTO activities
                       (id, name, category, experience_type, parent_participation,
                        description, address, lat, lng, age_min, age_max,
                        price_min, price_max, price_display, indoor, hours,
                        url, reservation_required, time_slots, seasonal,
                        source, source_id, event_date, last_updated, data_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        a.id, a.name, a.category, a.experience_type.value,
                        a.parent_participation.value, a.description, a.address,
                        a.lat, a.lng, a.age_min, a.age_max,
                        a.price_min, a.price_max, a.price_display,
                        1 if a.indoor else (0 if a.indoor is False else None),
                        a.hours, a.url,
                        1 if a.reservation_required else (0 if a.reservation_required is False else None),
                        json.dumps([ts.value for ts in a.time_slots]),
                        a.seasonal, a.source, a.source_id,
                        a.event_date.isoformat() if a.event_date else None,
                        a.last_updated.isoformat(),
                        a.data_type.value,
                    ),
                )
        logger.info("Upserted %d activities", len(activities))

    def get_all(self) -> list[dict]:
        """Get all activities as export-ready dicts."""
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM activities ORDER BY event_date ASC, name ASC").fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_weekend(self) -> list[dict]:
        """Get activities for the coming weekend (Sat + Sun)."""
        today = date.today()
        days_until_saturday = (5 - today.weekday()) % 7
        if days_until_saturday == 0 and today.weekday() != 5:
            days_until_saturday = 7
        saturday = today + timedelta(days=days_until_saturday)
        sunday = saturday + timedelta(days=1)

        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM activities
                   WHERE event_date IN (?, ?)
                      OR data_type = 'venue'
                   ORDER BY event_date ASC, name ASC""",
                (saturday.isoformat(), sunday.isoformat()),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_count(self) -> int:
        with self._conn() as conn:
            return conn.execute("SELECT COUNT(*) FROM activities").fetchone()[0]

    def cleanup_past_events(self):
        """Remove events with dates in the past."""
        today = date.today().isoformat()
        with self._conn() as conn:
            cursor = conn.execute(
                "DELETE FROM activities WHERE event_date IS NOT NULL AND event_date < ? AND data_type = 'event'",
                (today,),
            )
            if cursor.rowcount:
                logger.info("Cleaned up %d past events", cursor.rowcount)

    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        d = dict(row)
        d["indoor"] = bool(d["indoor"]) if d["indoor"] is not None else None
        d["reservation_required"] = bool(d["reservation_required"]) if d["reservation_required"] is not None else None
        d["time_slots"] = json.loads(d["time_slots"]) if d["time_slots"] else []
        return d
