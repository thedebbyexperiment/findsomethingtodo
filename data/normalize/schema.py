from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class ExperienceType(str, Enum):
    active = "active"
    creative = "creative"
    educational = "educational"
    nature = "nature"
    performance = "performance"
    events = "events"


class ParentParticipation(str, Enum):
    not_required = "not_required"
    required = "required"


class TimeSlot(str, Enum):
    morning = "morning"
    afternoon = "afternoon"
    evening = "evening"


class DataType(str, Enum):
    venue = "venue"
    event = "event"


class Activity(BaseModel):
    id: str = Field(description="Source-prefixed unique ID, e.g. seatgeek-evt-12345")
    name: str
    category: str = Field(description="Human-readable category")
    experience_type: ExperienceType
    parent_participation: ParentParticipation = ParentParticipation.not_required
    description: str = ""
    address: str = ""
    lat: Optional[float] = None
    lng: Optional[float] = None
    age_min: int = Field(ge=0, le=18, default=0)
    age_max: int = Field(ge=0, le=18, default=12)
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    price_display: str = ""
    indoor: Optional[bool] = None
    hours: str = ""
    url: str = ""
    reservation_required: Optional[bool] = None
    time_slots: list[TimeSlot] = Field(default_factory=list)
    seasonal: Optional[str] = None
    source: str
    source_id: str
    event_date: Optional[date] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    data_type: DataType = DataType.event

    @field_validator("age_max")
    @classmethod
    def age_max_gte_min(cls, v: int, info) -> int:
        age_min = info.data.get("age_min", 0)
        if v < age_min:
            return age_min
        return v

    def to_export_dict(self) -> dict:
        """Return a JSON-serializable dict for frontend consumption."""
        d = self.model_dump()
        d["experience_type"] = self.experience_type.value
        d["parent_participation"] = self.parent_participation.value
        d["data_type"] = self.data_type.value
        d["time_slots"] = [ts.value for ts in self.time_slots]
        d["event_date"] = self.event_date.isoformat() if self.event_date else None
        d["last_updated"] = self.last_updated.isoformat()
        return d
