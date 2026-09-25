"""Domain models and enums."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class WindowState(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    PURGED = "PURGED"


class EventStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class Event:
    event_id: str
    key: str
    event_time: int
    value: int = 1


@dataclass
class Window:
    window_start: int
    window_end: int
    key: str
    state: WindowState
    count: int = 0
    sum_value: int = 0


@dataclass
class EventResult:
    status: EventStatus
    event_id: str
    window_start: int | None = None
    window_end: int | None = None
    watermark: int = 0
    reject_code: str | None = None
    reject_message: str | None = None


@dataclass
class BatchResult:
    accepted: int = 0
    duplicates: int = 0
    rejected: int = 0
    watermark: int = 0
    results: list[EventResult] = field(default_factory=list)
