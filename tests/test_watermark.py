"""Watermark: strictly monotonic; regressions rejected with a distinct code."""
from __future__ import annotations

import pytest

from conftest import LOG, dump_state

from stream_agg.errors import IngestError, RejectCode
from stream_agg.models import Event


def test_explicit_regression_is_rejected(make_engine):
    engine = make_engine(max_out_of_orderness_ms=0)
    engine.ingest(Event("e1", "k", 5_000))
    assert engine.advance_watermark(12_345) == 12_345
    LOG.info("watermark advanced to %d", engine.watermark)

    for proposed in (0, 12_344, 1):
        with pytest.raises(IngestError) as exc:
            engine.advance_watermark(proposed)
        assert exc.value.code == RejectCode.WATERMARK_REGRESSION
        LOG.info(
            "regression proposed=%d -> REJECTED reason=%s (%s)",
            proposed,
            exc.value.code.value,
            exc.value.message,
        )
        # rejection leaves state untouched
        assert engine.watermark == 12_345


def test_equal_watermark_is_idempotent(make_engine):
    engine = make_engine()
    engine.advance_watermark(7_777)
    assert engine.advance_watermark(7_777) == 7_777
    assert engine.watermark == 7_777
    dump_state(engine, "idempotent equal watermark")


def test_heuristic_watermark_is_monotonic(make_engine):
    engine = make_engine(max_out_of_orderness_ms=1_000)
    prev = -1
    for eid, et in [("a", 5_000), ("b", 9_000), ("c", 6_000), ("d", 20_000)]:
        r = engine.ingest(Event(eid, "k", et))
        assert r.watermark >= prev
        prev = r.watermark
        LOG.info("event_time=%d heuristic watermark=%d", et, r.watermark)
    # the out-of-order event at 6000 must not pull it backwards
    assert engine.watermark == 20_000 - 1_000


def test_watermark_survives_restart(db_path, make_engine):
    engine = make_engine(max_out_of_orderness_ms=0)
    engine.ingest(Event("e1", "k", 1_000))
    engine.advance_watermark(42_000)

    restarted = make_engine(max_out_of_orderness_ms=0)
    assert restarted.watermark == 42_000
    with pytest.raises(IngestError) as exc:
        restarted.advance_watermark(41_999)
    assert exc.value.code == RejectCode.WATERMARK_REGRESSION
    LOG.info(
        "after restart watermark=%d regression still rejected: %s",
        restarted.watermark,
        exc.value.code.value,
    )
    dump_state(restarted, "restarted engine")
