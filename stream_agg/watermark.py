"""Monotonic event-time watermark.

The watermark is a promise: events with event time < watermark are no
longer expected.  It MUST be monotonic; a request to move it backwards is
rejected with ``WATERMARK_REGRESSION`` instead of being silently ignored.

Two ways to advance:

* :meth:`WatermarkGenerator.observe_event_time` -- heuristic watermark
  ``max_observed_event_time - max_out_of_orderness_ms``;
* :meth:`WatermarkGenerator.advance_to` -- explicit advance, e.g. driven by
  an upstream source or a timer / test client.
"""
from __future__ import annotations

import threading

from stream_agg.errors import IngestError, RejectCode


class WatermarkGenerator:
    """Holds the current watermark; it may only advance, never regress."""

    def __init__(
        self,
        initial: int = 0,
        max_out_of_orderness_ms: int = 2_000,
        initial_max_observed: int | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._max_ooo = max_out_of_orderness_ms
        self._watermark = initial
        # On recovery the true max observed time is unknown; assume events up
        # to watermark + out-of-orderness have already been seen.
        self._max_observed = (
            initial_max_observed
            if initial_max_observed is not None
            else initial + max_out_of_orderness_ms
        )

    @property
    def current(self) -> int:
        with self._lock:
            return self._watermark

    @property
    def max_observed(self) -> int:
        with self._lock:
            return self._max_observed

    def observe_event_time(self, event_time: int) -> int:
        """Update the heuristic watermark from an observed event time.

        Returns the resulting watermark (it never decreases).
        """
        with self._lock:
            if event_time > self._max_observed:
                self._max_observed = event_time
            candidate = max(0, self._max_observed - self._max_ooo)
            if candidate > self._watermark:
                self._watermark = candidate
            return self._watermark

    def advance_to(self, proposed: int) -> int:
        """Explicitly advance the watermark.

        Rejects (WATERMARK_REGRESSION) any value below the current watermark.
        Equal values are idempotent no-ops.
        """
        with self._lock:
            if proposed < self._watermark:
                raise IngestError(
                    RejectCode.WATERMARK_REGRESSION,
                    f"watermark cannot move backwards: "
                    f"current={self._watermark} proposed={proposed}",
                )
            if proposed > self._max_observed:
                self._max_observed = proposed
            self._watermark = proposed
            return self._watermark
