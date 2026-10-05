"""同一 event_id 并发提交：只计一次，且所有调用方拿到语义一致的结果。"""

from concurrent.futures import ThreadPoolExecutor

from conftest import log_line

from app.aggregator import EventTimeAggregator
from app.ingest import IngestService
from app.models import DecisionStatus, Event, RejectReason


def test_concurrent_same_event_id_consistent_results(settings):
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    svc.start()
    n = 32
    try:
        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(
                pool.map(
                    lambda _: svc.submit(Event("same-1", "k", 100, value=5)),
                    range(n),
                )
            )
    finally:
        svc.stop()

    kinds = {(r.accepted, r.reason) for r in results}
    log_line(f"concurrent same-id result kinds={kinds}")
    # 语义一致：不允许出现含义不清的结果；要么都算接受，要么都算重复
    assert kinds in (
        {(True, None)},
        {(False, RejectReason.DUPLICATE_EVENT)},
        {(True, None), (False, RejectReason.DUPLICATE_EVENT)},
    )
    # 且至少一个被接受，最终只计一次
    assert any(r.accepted for r in results)
    win = agg.snapshot().windows[0]
    log_line(f"window {win.window.interval()} count={win.count} total={win.total}")
    assert win.count == 1 and win.total == 5

    d = agg.query_event("same-1")
    assert d.status == DecisionStatus.ACCEPTED


def test_concurrent_same_id_aggregator_level(settings):
    """绕过队列直接并发打聚合器：去重集合保证只计一次。"""
    agg = EventTimeAggregator(settings)
    n = 64
    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(
            pool.map(lambda _: agg.add_event(Event("same-2", "k", 100)), range(n))
        )
    accepted = sum(r.accepted for r in results)
    dup = sum(r.reason == RejectReason.DUPLICATE_EVENT for r in results)
    log_line(f"aggregator-level: accepted={accepted} duplicate={dup}")
    assert accepted == 1
    assert accepted + dup == n
    assert agg.snapshot().windows[0].count == 1
