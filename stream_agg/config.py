"""Runtime configuration.

Values can be overridden through environment variables, which makes the
service easy to run and verify locally::

    STREAM_DB_PATH              sqlite database file (":memory:" allowed)
    STREAM_WINDOW_SIZE_MS       tumbling window size in ms
    STREAM_ALLOWED_LATENESS_MS  extra ms a closed window stays restorable
    STREAM_MAX_OOO_MS           watermark lag behind max observed event time
    STREAM_QUEUE_CAPACITY       bounded ingestion queue size
    STREAM_PROCESSING_TIMEOUT_MS max wait for queue admission / processing
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Config:
    db_path: str = "stream_agg.db"
    window_size_ms: int = 10_000
    allowed_lateness_ms: int = 5_000
    max_out_of_orderness_ms: int = 2_000
    queue_capacity: int = 1024
    processing_timeout_ms: float = 5_000.0

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            db_path=os.environ.get("STREAM_DB_PATH", "stream_agg.db"),
            window_size_ms=_env_int("STREAM_WINDOW_SIZE_MS", 10_000),
            allowed_lateness_ms=_env_int("STREAM_ALLOWED_LATENESS_MS", 5_000),
            max_out_of_orderness_ms=_env_int("STREAM_MAX_OOO_MS", 2_000),
            queue_capacity=_env_int("STREAM_QUEUE_CAPACITY", 1024),
            processing_timeout_ms=_env_float(
                "STREAM_PROCESSING_TIMEOUT_MS", 5_000.0
            ),
        )
