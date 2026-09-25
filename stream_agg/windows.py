"""Tumbling window assignment and lifecycle.

Window lifecycle relative to the watermark ``w`` for a window with
exclusive end ``we``:

* ``w <  we``                         -> OPEN   (normal events land here)
* ``we <= w <= we + allowed_lateness`` -> CLOSED  (late events still
  accepted; the finalized output is corrected)
* ``w >  we + allowed_lateness``      -> PURGED  (late events rejected with
  ``LATE_EVENT_TOO_OLD``; in-memory/durable working state removed)

Windows *close* as soon as the watermark crosses their end boundary; their
finalized result is written to the ``outputs`` table.  During the allowed
lateness gap a late event updates the same window and its stored output
(idempotently, by event id).  Once the gap expires the window is purged.
"""
from __future__ import annotations

from typing import Callable

from stream_agg.models import Window, WindowState
from stream_agg.store import SQLiteStore


def assign_window(event_time: int, window_size_ms: int) -> tuple[int, int]:
    """Return [start, end) of the tumbling window containing event_time."""
    start = (event_time // window_size_ms) * window_size_ms
    return start, start + window_size_ms


class WindowManager:
    def __init__(
        self,
        store: SQLiteStore,
        window_size_ms: int,
        allowed_lateness_ms: int,
    ) -> None:
        self._store = store
        self._size = window_size_ms
        self._lateness = allowed_lateness_ms

    def add_or_update(self, key: str, event_time: int, value: int) -> Window:
        """Fold one event into its window; late corrections are allowed."""
        start, end = assign_window(event_time, self._size)
        window = self._store.get_window(key, start, end)
        if window is None:
            # A late event arriving after purge re-creates a CLOSED window
            # only if the caller has already checked it is not past the
            # lateness horizon.  Default state is recomputed by the caller
            # through get_window_status if needed; OPEN is safe here because
            # watermark <= end + lateness and status is derived, not stored.
            window = Window(
                window_start=start,
                window_end=end,
                key=key,
                state=WindowState.OPEN,
                count=0,
                sum_value=0,
            )
        window.count += 1
        window.sum_value += value
        self._store.upsert_window(window)
        return window

    def get_window_status(self, window_end: int, watermark: int) -> WindowState:
        if watermark < window_end:
            return WindowState.OPEN
        if watermark <= window_end + self._lateness:
            return WindowState.CLOSED
        return WindowState.PURGED

    def is_purged(self, window_end: int, watermark: int) -> bool:
        return self.get_window_status(window_end, watermark) == WindowState.PURGED

    def on_watermark_advance(
        self,
        old_wm: int,
        new_wm: int,
        log_cb: Callable[[str], None] | None = None,
    ) -> list[Window]:
        """React to a watermark jump: close due windows, purge expired ones.

        Must run inside the engine's processing transaction.  Returns windows
        that were newly finalized by this jump.
        """
        finalized: list[Window] = []
        windows = self._store.list_windows()
        for window in windows:
            before = window.state
            new_state = self.get_window_status(window.window_end, new_wm)
            window.state = new_state

            # Close: persist finalized output exactly when crossing boundary.
            if before == WindowState.OPEN and new_state != WindowState.OPEN:
                self._store.upsert_output(window, new_wm)
                finalized.append(window)
                if log_cb:
                    log_cb(
                        f"WINDOW CLOSED key={window.key} "
                        f"interval=[{window.window_start},{window.window_end}) "
                        f"count={window.count} sum={window.sum_value} "
                        f"watermark={new_wm}"
                    )
            elif new_state == WindowState.CLOSED:
                # Late correction: refresh the already-finalized output.
                self._store.upsert_output(window, new_wm)

            if new_state == WindowState.PURGED:
                self._store.purge_window(window.key, window.window_start)
                if log_cb:
                    log_cb(
                        f"WINDOW PURGED key={window.key} "
                        f"interval=[{window.window_start},{window.window_end}) "
                        f"watermark={new_wm}"
                    )
            else:
                self._store.upsert_window(window)

        return finalized
