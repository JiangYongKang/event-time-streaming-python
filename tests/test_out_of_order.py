"""Out-of-order events must be aggregated by EVENT time, deterministically.

Two flavours:

* bounded reordering against the *heuristic* watermark (arrival jumps of at
  most max_out_of_orderness + allowed_lateness);
* arbitrary reordering when the watermark is controlled explicitly.
"""
from __future__ import annotations

from conftest import LOG, dump_state

from stream_agg.models import Event


def test_out_of_order_within_lateness_horizon(make_engine):
    engine = make_engine(
        max_out_of_orderness_ms=1_000, allowed_lateness_ms=20_000
    )
    # arrival is shuffled but never jumps more than ~6s forward
    arrivals = [
        ("a1", 5_000),
        ("a2", 1_000),
        ("a3", 6_000),
        ("a4", 2_000),
        ("a5", 7_000),
    ]
    for eid, et in arrivals:
        r = engine.ingest(Event(eid, "k", et))
        LOG.info(
            "ingest id=%s event_time=%d -> %s window=[%s,%s) watermark=%d",
            eid,
            et,
            r.status.value,
            r.window_start,
            r.window_end,
            r.watermark,
        )
        assert r.status.value == "ACCEPTED"

    dump_state(engine, "after out-of-order ingestion")
    windows = {w.window_start: w for w in engine.windows()}
    assert windows[0].count == 5

    engine.advance_watermark(10_000)
    dump_state(engine, "watermark crossed window end")
    outputs = {o.window_start: o for o in engine.outputs()}
    assert outputs[0].count == 5
    assert outputs[0].sum_value == 5


def test_arbitrary_reorder_with_explicit_watermark(make_engine):
    engine = make_engine(max_out_of_orderness_ms=0, allowed_lateness_ms=100_000)
    # Keep the heuristic watermark from getting involved: ingest all events
    # for the same window BEFORE explicitly advancing the watermark.
    events = [
        Event("b1", "k", 25_000),
        Event("b2", "k", 1_000),
        Event("b3", "k", 21_000),
        Event("b4", "k", 9_000),
        Event("b5", "k", 2_000),
    ]
    for e in events:
        assert engine.ingest(e).status.value == "ACCEPTED"

    # deterministic counts grouped by event-time window
    engine.advance_watermark(30_000)
    dump_state(engine, "after watermark=30000")
    outputs = {(o.window_start, o.key): o for o in engine.outputs()}
    assert outputs[(0, "k")].count == 3
    assert outputs[(20_000, "k")].count == 2


def test_multiple_keys_are_independent(make_engine):
    engine = make_engine(max_out_of_orderness_ms=0, allowed_lateness_ms=100_000)
    for eid, key, et in [
        ("x1", "x", 1_000),
        ("y1", "y", 1_000),
        ("x2", "x", 2_000),
        ("y2", "y", 11_000),
    ]:
        assert engine.ingest(Event(eid, key, et)).status.value == "ACCEPTED"

    engine.advance_watermark(20_000)
    outputs = {(o.key, o.window_start): o for o in engine.outputs()}
    assert outputs[("x", 0)].count == 2
    assert outputs[("y", 0)].count == 1
    assert outputs[("y", 10_000)].count == 1
    dump_state(engine, "multi-key finalized")
