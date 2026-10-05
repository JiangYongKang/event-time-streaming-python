"""同一 event_id 并发提交：只计一次，所有并发结果语义一致。"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from conftest import log_line

from app.aggregator import EventTimeAggregator
from app.ingest import IngestService
from app.models import Event, OutcomeStatus, RejectReason


class SlowAggregator(EventTimeAggregator):
    """拉长处理耗时，制造真实的并发竞争窗口。"""

    def __init__(self, settings, delay=0.01):
        super().__init__(settings)
        self.delay = delay

    def add_event(self, event):
        result = super().add_event(event)
        time.sleep(self.delay)
        return result


def test_concurrent_same_id_counted_once_and_consistent(settings):
    agg = SlowAggregator(settings, delay=0.01)
    svc = IngestService(agg, settings)
    svc.start()
    n = 32
    barrier = threading.Barrier(n)
    try:
        def submit_same(_):
            barrier.wait()  # 尽量同时发出
            return svc.submit(Event("same-id", "k", 100, value=5), timeout=10)

        with ThreadPoolExecutor(max_workers=n) as pool:
            results = list(pool.map(submit_same, range(n)))
    finally:
        svc.stop()

    accepted = sum(1 for r in results if r.accepted)
    duplicates = sum(1 for r in results if r.reason == RejectReason.DUPLICATE_EVENT)
    others = [
        r for r in results
        if not r.accepted and r.reason != RejectReason.DUPLICATE_EVENT
    ]
    log_line(
        f"concurrent same-id: accepted={accepted} duplicate={duplicates} "
        f"ambiguous={len(others)} watermark={results[0].watermark_ms}"
    )
    # 不允许出现含义不清的结果（BUSY/BUFFER_FULL 等）
    assert not others, f"ambiguous results: {others}"
    assert accepted >= 1
    assert accepted + duplicates == n

    # 只计一次
    win = agg.snapshot().windows[0]
    log_line(f"window {win.window.interval()} count={win.count} total={win.total}")
    assert win.count == 1 and win.total == 5

    # 台账结果明确：该标识已被接受
    outcome = svc.query_event("same-id")
    assert outcome.status == OutcomeStatus.ACCEPTED


def test_concurrent_same_id_distinct_events_unaffected(settings):
    """并发同 id 的合并不影响不同 id 的正常并发摄取。"""
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(
                pool.map(
                    lambda i: svc.submit(Event(f"distinct-{i % 10}", "k", 100)),
                    range(50),
                )
            )
    finally:
        svc.stop()
    accepted = sum(1 for r in results if r.accepted)
    duplicates = sum(1 for r in results if r.reason == RejectReason.DUPLICATE_EVENT)
    log_line(f"distinct ids: accepted={accepted} duplicate={duplicates}")
    assert accepted + duplicates == 50
    win = agg.snapshot().windows[0]
    assert win.count == 10  # 10 个不同 id 各计一次
