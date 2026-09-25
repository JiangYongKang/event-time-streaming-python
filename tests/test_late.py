"""Late events: explicit accept-vs-reject semantics after window close.

Rules (window_end=10000, allowed_lateness_ms=5000):

* watermark < 10000            window OPEN
* 10000 <= watermark <= 15000  window CLOSED, late events ACCEPTED and the
                               finalized output is corrected
* watermark > 15000            window PURGED, late events REJECTED with
                               LATE_EVENT_TOO_OLD
"""
from __future__ import annotations

from conftest import LOG, dump_state

from stream_agg.models import Event, EventStatus


def _by_start(windows):
    return {w.window_start: w for w in windows}


def test_late_within_allowed_lateness_corrects_output(make_engine):
    engine = make_engine(max_out_of_orderness_ms=0)
    for eid, et in [("e1", 1_000), ("e2", 9_000)]:
        engine.ingest(Event(eid, "k", et))
    engine.advance_watermark(10_000)
    out = _by_start(engine.outputs())[0]
    assert out.count == 2
    LOG.info("finalized window=[0,10000) count=%d", out.count)

    # late but within the 5s lateness gap
    r = engine.ingest(Event("late-ok", "k", 500))
    LOG.info(
        "late event -> %s watermark=%d reason=%s",
        r.status.value,
        r.watermark,
        r.reject_code,
    )
    assert r.status == EventStatus.ACCEPTED
    out = _by_start(engine.outputs())[0]
    assert out.count == 3  # corrected, consistent result
    dump_state(engine, "late event accepted within lateness")


def test_late_beyond_horizon_rejected_with_distinct_reason(make_engine):
    engine = make_engine(max_out_of_orderness_ms=0)
    engine.ingest(Event("e1", "k", 1_000))
    engine.advance_watermark(10_000)   # close [0,10000)
    engine.advance_watermark(15_001)   # purge it
    dump_state(engine, "after purge watermark=15001")

    r = engine.ingest(Event("late-too-old", "k", 500))
    LOG.info(
        "over-horizon late event -> %s reason=%s message=%s",
        r.status.value,
        r.reject_code,
        r.reject_message,
    )
    assert r.status == EventStatus.REJECTED
    assert r.reject_code == "LATE_EVENT_TOO_OLD"

    # rejected event must not be counted anywhere
    outputs = engine.outputs()
    windows = engine.windows()
    assert all(w.count == 1 for w in outputs if w.window_start == 0)
    assert not any(w.window_start == 0 for w in windows)
    # and it is not remembered as an accepted duplicate
    assert engine.ingest(Event("late-too-old", "k", 500)).status == (
        EventStatus.REJECTED
    )
    dump_state(engine, "over-horizon late event rejected")


def test_boundary_lateness_values(make_engine):
    # watermark == window_end           -> closed but accepts late
    # watermark == window_end+lateness  -> still accepts late (<=)
    # watermark == end+lateness+1       -> rejected
    engine = make_engine(
        max_out_of_orderness_ms=0,
        allowed_lateness_ms=5_000,
    )
    engine.ingest(Event("e1", "k", 0))

    engine.advance_watermark(10_000)
    assert engine.ingest(Event("L1", "k", 0)).status == EventStatus.ACCEPTED
    engine.advance_watermark(15_000)
    assert engine.ingest(Event("L2", "k", 0)).status == EventStatus.ACCEPTED
    engine.advance_watermark(15_001)
    r = engine.ingest(Event("L3", "k", 0))
    assert r.status == EventStatus.REJECTED
    assert r.reject_code == "LATE_EVENT_TOO_OLD"
    LOG.info("boundary rejection at watermark=%d: %s", r.watermark, r.reject_message)
