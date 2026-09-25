"""Error types with distinguishable reject reasons."""
from __future__ import annotations

from enum import Enum


class RejectCode(str, Enum):
    """Reject reasons exposed to callers."""

    LATE_EVENT_TOO_OLD = "LATE_EVENT_TOO_OLD"
    WATERMARK_REGRESSION = "WATERMARK_REGRESSION"
    QUEUE_FULL = "QUEUE_FULL"
    PROCESSING_TIMEOUT = "PROCESSING_TIMEOUT"
    INVALID_EVENT = "INVALID_EVENT"


class IngestError(Exception):
    """Raised when an event or watermark request must be rejected."""

    def __init__(self, code: RejectCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code.value, "message": self.message}
