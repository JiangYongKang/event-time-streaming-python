"""StreamEngine: bounded buffer, concurrent ingestion, durable processing.

Design
------
* A single bounded :class:`queue.Queue` feeds one worker thread.  All
  durable state changes happen on that worker while holding the store
  transaction lock, so concurrent updates to the same key can never race and
  lose counts.
* The queue is the back-pressure boundary: when it is full the producer gets
  an immediate, distinguishable ``QUEUE_FULL`` rejection (after at most
  ``processing_timeout_ms``).  Memory use is therefore bounded by
  ``queue_capacity``.
* Each accepted event id, window update and watermark move commit together in
  one SQLite transaction: batch all-or-nothing, duplicate protection and
  restart consistency all follow from that.
* The watermark advances automatically (heuristic: max event time - max
  out-of-orderness) and may also be advanced explicitly via
  :meth:`advance_watermark`; it never regresses.

The ``STREAM_SIMULATED_PROCESS_DELAY_MS`` env var injects an artificial
worker delay so the QUEUE_FULL / back-pressure behaviour is deterministically
testable.
"""
from __future__ import annotations

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass

from stream_agg.config import Config
from stream_agg.errors import IngestError, RejectCode
from stream_agg.models import (
    BatchResult,
    Event,
    EventResult,
    EventStatus,
    Window,
    WindowState,
)
from stream_agg.store import SQLiteStore
from stream_agg.watermark import WatermarkGenerator
from stream_agg.windows import WindowManager, assign_window

logger = logging.getLogger("stream_agg")

_SENTINEL = object()


@dataclass
class _Job:
    events: list[Event]
    done: threading.Event
    result: BatchResult | None = None
    error: BaseException | None = None


def _simulated_delay() -> float:
    raw = os.environ.get("STREAM_SIMULATED_PROCESS_DELAY_MS")
    return float(raw) / 1000.0 if raw else 0.0


class StreamEngine:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._store = SQLiteStore(config.db_path)
        recovered_wm = self._store.get_watermark()
        recovered_max = self._store.get_max_observed()
        self._watermarks = WatermarkGenerator(
            initial=recovered_wm,
            max_out_of_orderness_ms=config.max_out_of_orderness_ms,
            initial_max_observed=recovered_max,
        )
        self._windows = WindowManager(
            self._store, config.window_size_ms, config.allowed_lateness_ms
        )
        self._queue: queue.Queue[_Job | object] = queue.Queue(
            maxsize=config.queue_capacity
        )
        self._worker: threading.Thread | None = None
        self._running = False
        self._total_enqueued = 0
        self._total_rejected_full = 0
        self._stats_lock = threading.Lock()

    # -- lifecycle --------------------------------------------------------
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._worker = threading.Thread(
            target=self._run_worker, name="stream-worker", daemon=True
        )
        self._worker.start()
        logger.info(
            "engine started db=%s window_size_ms=%d allowed_lateness_ms=%d "
            "queue_capacity=%d recovered_watermark=%d",
            self._config.db_path,
            self._config.window_size_ms,
            self._config.allowed_lateness_ms,
            self._config.queue_capacity,
            self._store.get_watermark(),
        )

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._queue.put(_SENTINEL)
        if self._worker is not None:
            self._worker.join(timeout=5)
        self._store.close()

    # -- worker -----------------------------------------------------------
    def _run_worker(self) -> None:
        while True:
            job = self._queue.get()
            try:
                if job is _SENTINEL:
                    return
                if isinstance(job, _CallableJob):
                    job.fn()  # type: ignore[misc]
                else:
                    assert isinstance(job, _Job)
                    try:
                        job.result = self._process_batch(job.events)
                    except BaseException as exc:  # propagated to caller
                        job.error = exc
                    finally:
                        job.done.set()
            finally:
                self._queue.task_done()

    # -- core processing --------------------------------------------------
    def _process_batch(self, events: list[Event]) -> BatchResult:
        """Atomically process a batch; all-or-nothing commit."""
        results: list[EventResult] = []
        accepted = duplicates = rejected = 0
        # event ids seen *within this batch* count as duplicates too
        batch_seen: set[str] = set()
        delay = _simulated_delay()

        with self._store.tx_lock:
            self._store.begin()
            try:
                for event in events:
                    self._validate(event)
                    # 1) duplicate check (persistent + intra-batch)
                    if event.event_id in batch_seen or self._store.is_duplicate(
                        event.event_id
                    ):
                        duplicates += 1
                        batch_seen.add(event.event_id)
                        results.append(
                            EventResult(
                                status=EventStatus.DUPLICATE,
                                event_id=event.event_id,
                                watermark=self._watermarks.current,
                            )
                        )
                        logger.info(
                            "DUPLICATE event_id=%s key=%s watermark=%d",
                            event.event_id,
                            event.key,
                            self._watermarks.current,
                        )
                        continue

                    # 2) observe event time, then evaluate lateness against
                    #    the watermark *before* this event affects it
                    old_wm = self._watermarks.current
                    start, end = assign_window(
                        event.event_time, self._config.window_size_ms
                    )
                    if self._windows.is_purged(end, old_wm):
                        rejected += 1
                        msg = (
                            f"event_id={event.event_id} event_time="
                            f"{event.event_time} is later than the lateness "
                            f"horizon window_end={end} +"
                            f" allowed_lateness_ms="
                            f"{self._config.allowed_lateness_ms} watermark="
                            f"{old_wm}"
                        )
                        results.append(
                            EventResult(
                                status=EventStatus.REJECTED,
                                event_id=event.event_id,
                                window_start=start,
                                window_end=end,
                                watermark=old_wm,
                                reject_code=RejectCode.LATE_EVENT_TOO_OLD.value,
                                reject_message=msg,
                            )
                        )
                        logger.info("REJECTED LATE_EVENT_TOO_OLD %s", msg)
                        continue

                    # 3) apply update + dedupe record
                    window = self._windows.add_or_update(
                        event.key, event.event_time, event.value
                    )
                    self._store.insert_event(
                        event.event_id,
                        event.key,
                        event.event_time,
                        event.value,
                        int(time.time() * 1000),
                    )
                    batch_seen.add(event.event_id)

                    # 4) advance heuristic watermark (monotonic)
                    new_wm = self._watermarks.observe_event_time(
                        event.event_time
                    )
                    accepted += 1
                    status = self._windows.get_window_status(end, new_wm)
                    if status == WindowState.CLOSED:
                        # late correction at a constant watermark: refresh
                        # the already-finalized output in the same tx
                        self._store.upsert_output(window, new_wm)
                    results.append(
                        EventResult(
                            status=EventStatus.ACCEPTED,
                            event_id=event.event_id,
                            window_start=start,
                            window_end=end,
                            watermark=new_wm,
                        )
                    )
                    logger.info(
                        "ACCEPTED event_id=%s key=%s event_time=%d "
                        "window=[%d,%d) window_state=%s count=%d "
                        "watermark=%d",
                        event.event_id,
                        event.key,
                        event.event_time,
                        start,
                        end,
                        status.value,
                        window.count,
                        new_wm,
                    )

                # 5) persist watermark + close/purge windows
                wm = self._watermarks.current
                if wm != self._store.get_watermark():
                    self._store.set_watermark(wm)
                    self._store.set_max_observed(
                        self._watermarks.max_observed
                    )
                    self._windows.on_watermark_advance(
                        old_wm if events else self._store.get_watermark(),
                        wm,
                        log_cb=logger.info,
                    )

                if delay:
                    time.sleep(delay)
                self._store.commit()
            except BaseException:
                self._store.rollback()
                raise

        return BatchResult(
            accepted=accepted,
            duplicates=duplicates,
            rejected=rejected,
            watermark=wm,
            results=results,
        )

    @staticmethod
    def _validate(event: Event) -> None:
        if not event.event_id:
            raise IngestError(
                RejectCode.INVALID_EVENT, "event_id must be non-empty"
            )
        if not event.key:
            raise IngestError(RejectCode.INVALID_EVENT, "key must be non-empty")
        if event.event_time < 0:
            raise IngestError(
                RejectCode.INVALID_EVENT, "event_time must be >= 0"
            )

    # -- public API -------------------------------------------------------
    def ingest(self, event: Event, timeout_ms: float | None = None) -> EventResult:
        batch = self.ingest_batch([event], timeout_ms=timeout_ms)
        return batch.results[0]

    def ingest_batch(
        self, events: list[Event], timeout_ms: float | None = None
    ) -> BatchResult:
        if not events:
            return BatchResult(watermark=self._watermarks.current)
        timeout = (
            self._config.processing_timeout_ms / 1000.0
            if timeout_ms is None
            else timeout_ms / 1000.0
        )
        job = _Job(events=events, done=threading.Event())
        # Phase 1: bounded-buffer admission. Non-blocking so a saturated
        # pipeline produces an immediate QUEUE_FULL (back-pressure), which
        # is distinguishable from a slow accepted job (phase 2 timeout).
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            with self._stats_lock:
                self._total_rejected_full += 1
            raise IngestError(
                RejectCode.QUEUE_FULL,
                f"ingestion queue is full (capacity="
                f"{self._config.queue_capacity}); back-pressure: retry later",
            )
        with self._stats_lock:
            self._total_enqueued += 1
        # Phase 2: bounded wait for the single worker to finish processing.
        if not job.done.wait(timeout=timeout):
            raise IngestError(
                RejectCode.PROCESSING_TIMEOUT,
                f"processing did not complete within {timeout * 1000:.0f}ms",
            )
        if job.error is not None:
            raise job.error
        assert job.result is not None
        return job.result

    def advance_watermark(self, proposed: int) -> int:
        """Explicitly advance (never regress) and finalize due windows."""

        def _do() -> BatchResult:
            # WatermarkGenerator rejects regression itself; run inside a job
            # so all state changes stay serialized with ingestion.
            old = self._watermarks.current
            new = self._watermarks.advance_to(proposed)
            with self._store.tx_lock:
                self._store.begin()
                try:
                    self._store.set_watermark(new)
                    self._store.set_max_observed(self._watermarks.max_observed)
                    self._windows.on_watermark_advance(
                        old, new, log_cb=logger.info
                    )
                    self._store.commit()
                except BaseException:
                    self._store.rollback()
                    raise
            logger.info("WATERMARK advanced %d -> %d", old, new)
            return BatchResult(watermark=new)

        self._submit_callable(_do)
        return self._watermarks.current

    def _submit_callable(self, fn) -> None:  # type: ignore[no-untyped-def]
        timeout = self._config.processing_timeout_ms / 1000.0
        done = threading.Event()
        box: dict[str, object] = {}

        def task() -> None:
            try:
                box["result"] = fn()
            except BaseException as exc:
                box["error"] = exc
            finally:
                done.set()

        # callable jobs piggy-back on the same single worker
        callable_job = _CallableJob(task)
        try:
            self._queue.put_nowait(callable_job)
        except queue.Full:
            with self._stats_lock:
                self._total_rejected_full += 1
            raise IngestError(
                RejectCode.QUEUE_FULL,
                "ingestion queue is full; back-pressure: retry later",
            )
        if not done.wait(timeout=timeout):
            raise IngestError(
                RejectCode.PROCESSING_TIMEOUT,
                f"processing did not complete within {timeout * 1000:.0f}ms",
            )
        if "error" in box:
            raise box["error"]  # type: ignore[misc]

    @property
    def watermark(self) -> int:
        return self._watermarks.current

    def windows(self) -> list[Window]:
        with self._store.tx_lock:
            return self._store.list_windows()

    def outputs(self) -> list[Window]:
        with self._store.tx_lock:
            return self._store.list_outputs()

    def queue_stats(self) -> dict[str, int]:
        with self._stats_lock:
            return {
                "queue_size": self._queue.qsize(),
                "capacity": self._config.queue_capacity,
                "total_enqueued": self._total_enqueued,
                "total_queue_full_rejections": self._total_rejected_full,
            }


@dataclass
class _CallableJob:
    fn: object
