"""Concurrent ingestion: no lost updates; bounded queue; distinct rejections."""
from __future__ import annotations

import threading

import pytest

from conftest import LOG, dump_state

from stream_agg.errors import IngestError, RejectCode
from stream_agg.models import Event, EventStatus


def test_concurrent_writers_same_key_no_lost_counts(make_engine):
    engine = make_engine(
        max_out_of_orderness_ms=0,
        queue_capacity=10_000,
        processing_timeout_ms=10_000,
    )
    n_threads = 8
    per_thread = 125
    errors: list[BaseException] = []

    def worker(tid: int) -> None:
        try:
            for i in range(per_thread):
                engine.ingest(Event(f"t{tid}-{i}", "hot-key", 1_000))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors
    engine.advance_watermark(10_000)
    out = {o.key: o for o in engine.outputs()}["hot-key"]
    LOG.info("concurrent final count=%d expected=%d", out.count, n_threads * per_thread)
    dump_state(engine, "after concurrent ingestion")
    assert out.count == n_threads * per_thread
    assert out.sum_value == n_threads * per_thread


def test_queue_full_returns_distinct_backpressure_reason(make_engine, monkeypatch):
    engine = make_engine(
        queue_capacity=1,
        processing_timeout_ms=10_000.0,
        max_out_of_orderness_ms=0,
    )
    # block the single worker on a latch so the bounded queue fills
    latch = threading.Event()
    entered = threading.Event()

    from stream_agg import engine as engine_mod

    def delayed_store(self, event_id, key, event_time, value, accepted_at):
        entered.set()
        latch.wait(timeout=5)
        return self.__class__.insert_event_orig(
            self, event_id, key, event_time, value, accepted_at
        )

    store_cls = engine_mod.SQLiteStore
    store_cls.insert_event_orig = store_cls.insert_event
    monkeypatch.setattr(store_cls, "insert_event", delayed_store)

    outcome: dict[str, object] = {}

    def slow_ingest() -> None:
        try:
            engine.ingest(Event("slow", "k", 1_000))
        except BaseException as exc:  # noqa: BLE001
            outcome["slow_error"] = exc

    th = threading.Thread(target=slow_ingest)
    th.start()
    entered.wait(timeout=2)

    # The worker is blocked inside the "slow" transaction.
    # fill-0 occupies the single buffer slot and then waits for the stalled
    # worker -> PROCESSING_TIMEOUT; every further admission is refused at the
    # full buffer immediately -> QUEUE_FULL. Both reasons are distinguishable.
    rejected_full = None
    rejected_timeout = None
    for i in range(5):
        try:
            engine.ingest(Event(f"fill-{i}", "k", 1_000), timeout_ms=100)
        except IngestError as exc:
            LOG.info("fill-%d rejected: %s", i, exc.code.value)
            if exc.code == RejectCode.PROCESSING_TIMEOUT:
                rejected_timeout = exc
            elif exc.code == RejectCode.QUEUE_FULL:
                rejected_full = exc
                break
    latch.set()
    th.join()

    assert rejected_timeout is not None
    assert rejected_timeout.code == RejectCode.PROCESSING_TIMEOUT
    assert rejected_full is not None
    assert rejected_full.code == RejectCode.QUEUE_FULL
    LOG.info("QUEUE_FULL reason: %s", rejected_full.message)
    stats = engine.queue_stats()
    assert stats["total_queue_full_rejections"] >= 1
    LOG.info("queue stats=%s", stats)
    monkeypatch.undo()


def test_processing_timeout_reason_is_distinguishable(make_engine, monkeypatch):
    engine = make_engine(
        queue_capacity=4, processing_timeout_ms=50, max_out_of_orderness_ms=0
    )
    latch = threading.Event()
    entered = threading.Event()

    from stream_agg import engine as engine_mod

    def hang(self, *_a, **_kw):
        entered.set()
        latch.wait(timeout=5)

    monkeypatch.setattr(engine_mod.StreamEngine, "_process_batch", hang)

    with pytest.raises(IngestError) as exc:
        engine.ingest(Event("hang", "k", 1_000))
    assert exc.value.code == RejectCode.PROCESSING_TIMEOUT
    LOG.info("slow worker rejected: %s -- %s", exc.value.code.value,
             exc.value.message)
    latch.set()
