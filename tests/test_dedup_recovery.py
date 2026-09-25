"""Idempotency: duplicate event ids, atomic batches, restart consistency."""
from __future__ import annotations

from conftest import LOG, dump_state

from stream_agg.models import Event, EventStatus


def test_duplicate_event_id_counted_once(make_engine):
    engine = make_engine(max_out_of_orderness_ms=0)
    first = engine.ingest(Event("dup-1", "k", 1_000))
    assert first.status == EventStatus.ACCEPTED

    second = engine.ingest(Event("dup-1", "k", 1_000))
    LOG.info(
        "redelivered id=dup-1 -> %s watermark=%d", second.status.value, second.watermark
    )
    assert second.status == EventStatus.DUPLICATE

    # even a duplicate with a *different* payload is never counted twice
    third = engine.ingest(Event("dup-1", "k", 9_999, value=99))
    assert third.status == EventStatus.DUPLICATE

    engine.advance_watermark(10_000)
    outputs = {o.window_start: o for o in engine.outputs()}
    assert outputs[0].count == 1
    assert outputs[0].sum_value == 1
    dump_state(engine, "after redelivery attempts")


def test_intra_batch_duplicates_collapsed(make_engine):
    engine = make_engine(max_out_of_orderness_ms=0)
    result = engine.ingest_batch(
        [
            Event("b1", "k", 1_000),
            Event("b2", "k", 2_000),
            Event("b1", "k", 1_000),  # same id twice in one batch
            Event("b3", "k", 3_000),
        ]
    )
    LOG.info(
        "batch accepted=%d duplicates=%d rejected=%d watermark=%d",
        result.accepted,
        result.duplicates,
        result.rejected,
        result.watermark,
    )
    assert (result.accepted, result.duplicates, result.rejected) == (3, 1, 0)
    statuses = [r.status for r in result.results]
    assert statuses.count(EventStatus.DUPLICATE) == 1
    engine.advance_watermark(10_000)
    assert engine.outputs()[0].count == 3


def test_batch_failure_leaves_no_partial_result(make_engine, monkeypatch):
    engine = make_engine(max_out_of_orderness_ms=0)
    # seed one accepted event
    engine.ingest(Event("seed", "k", 1_000))

    # Force the transaction to fail midway through the batch by making the
    # watermark step raise AFTER some events would have been applied.
    from stream_agg import engine as engine_mod

    class Boom(RuntimeError):
        pass

    def boom(_self, _event_time):
        raise Boom("simulated worker failure")

    monkeypatch.setattr(
        engine_mod.WatermarkGenerator, "observe_event_time", boom
    )

    try:
        engine.ingest_batch(
            [Event("x1", "k", 2_000), Event("x2", "k", 3_000)]
        )
        raised = False
    except Boom:
        raised = True
    assert raised
    monkeypatch.undo()

    # all-or-nothing: neither x1 nor x2 survived the rollback
    for eid in ("x1", "x2"):
        r = engine.ingest(Event(eid, "k", 2_000))
        assert r.status == EventStatus.ACCEPTED  # not a remembered duplicate
    engine.advance_watermark(10_000)
    out = {o.window_start: o for o in engine.outputs()}
    # seed + x1 + x2 == 3; if partial commit had happened it would be 4+
    LOG.info("post-rollback finalized count=%d", out[0].count)
    assert out[0].count == 3
    dump_state(engine, "after failed batch rollback")


def test_confirmed_results_consistent_after_restart(db_path, make_engine):
    engine = make_engine(max_out_of_orderness_ms=0)
    engine.ingest_batch(
        [
            Event("r1", "k", 1_000),
            Event("r2", "k", 9_000),
            Event("r3", "other", 1_000),
        ]
    )
    engine.advance_watermark(10_000)
    before = sorted(
        (o.key, o.window_start, o.count, o.sum_value) for o in engine.outputs()
    )
    LOG.info("before restart outputs=%s", before)

    restarted = make_engine(max_out_of_orderness_ms=0)
    after = sorted(
        (o.key, o.window_start, o.count, o.sum_value)
        for o in restarted.outputs()
    )
    assert before == after
    assert restarted.watermark == 10_000

    # replay the whole confirmed batch after restart: all duplicates
    replay = restarted.ingest_batch(
        [
            Event("r1", "k", 1_000),
            Event("r2", "k", 9_000),
            Event("r3", "other", 1_000),
        ]
    )
    assert replay.accepted == 0
    assert replay.duplicates == 3
    after_replay = sorted(
        (o.key, o.window_start, o.count, o.sum_value)
        for o in restarted.outputs()
    )
    assert after_replay == before  # unchanged: exactly-once effective result
    dump_state(restarted, "after restart + replay")
