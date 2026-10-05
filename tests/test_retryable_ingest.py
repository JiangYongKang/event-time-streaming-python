"""可重试摄取：暂时性失败进入有界暂存区，恢复后自动、有节奏地重试。"""

import threading
import time

from conftest import log_line

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.ingest import IngestService
from app.models import DecisionStatus, Event, RejectReason
from app.storage import StateStore


def _retry_settings(tmp_path, **kw):
    base = {
        "window_size_ms": 1000,
        "allowed_lateness_ms": 500,
        "max_pending_events": 2,
        "ingest_workers": 1,
        "state_path": str(tmp_path / "state.json"),
        "max_retry_events": 10,
        "retry_interval_ms": 50,
    }
    base.update(kw)
    return Settings(**base)


class SlowAggregator(EventTimeAggregator):
    """处理慢，制造队列占满 -> BUFFER_FULL 的场景。"""

    def __init__(self, settings, delay=0.05, **kw):
        super().__init__(settings, **kw)
        self.delay = delay

    def add_event(self, event):
        result = super().add_event(event)
        time.sleep(self.delay)
        return result


def _wait_until(pred, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def test_transient_failure_buffered_then_auto_retried(tmp_path):
    settings = _retry_settings(tmp_path)
    agg = SlowAggregator(settings, delay=0.05)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        results = []

        def fire(i):
            results.append(svc.submit(Event(f"r-{i}", "k", 100), timeout=3))

        threads = [threading.Thread(target=fire, args=(i,)) for i in range(12)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        scheduled = [r for r in results if r.retry_scheduled]
        log_line(
            f"burst: accepted={sum(r.accepted for r in results)} "
            f"retry_scheduled={len(scheduled)} "
            f"reasons={[r.reason for r in results]}"
        )
        # 队列容量只有 2，必然有部分事件被暂时拒绝并进入暂存区
        assert scheduled, "expected some BUFFER_FULL events to be buffered"
        assert all(r.reason == RejectReason.BUFFER_FULL for r in scheduled)

        # 容量恢复后，维护线程自动重试，最终全部接受且只计一次
        assert _wait_until(lambda: svc.retry_buffer_size() == 0)
        win = agg.snapshot().windows[0]
        log_line(
            f"after retries: window {win.window.interval()} count={win.count} "
            f"retry_buffer={svc.retry_buffer_size()}"
        )
        assert win.count == 12
        for i in range(12):
            d = agg.query_event(f"r-{i}")
            assert d.status == DecisionStatus.ACCEPTED, d
    finally:
        svc.stop()


def test_terminal_rejection_not_buffered(tmp_path):
    settings = _retry_settings(tmp_path)
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        agg.advance_watermark(2000)  # 宽限到 1500
        r = svc.submit(Event("late-1", "k", 1000))
        log_line(
            f"terminal reject: reason={r.reason} "
            f"retry_scheduled={r.retry_scheduled}"
        )
        assert r.reason == RejectReason.LATE_EVENT
        assert r.retry_scheduled is False
        assert svc.retry_buffer_size() == 0
        d = agg.query_event("late-1")
        assert d.status == DecisionStatus.REJECTED
        assert d.reason == RejectReason.LATE_EVENT
    finally:
        svc.stop()


def test_retry_buffer_is_bounded(tmp_path):
    settings = _retry_settings(tmp_path, max_retry_events=2)
    agg = SlowAggregator(settings, delay=0.2)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        results = []

        def fire(i):
            results.append(svc.submit(Event(f"b-{i}", "k", 100), timeout=5))

        threads = [threading.Thread(target=fire, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        scheduled = [r for r in results if r.retry_scheduled]
        dropped = [
            r for r in results
            if r.reason == RejectReason.BUFFER_FULL and not r.retry_scheduled
        ]
        log_line(
            f"bounded buffer: scheduled={len(scheduled)} "
            f"dropped={len(dropped)} buffer_cap=2"
        )
        # 暂存区容量为 2：超出部分的暂时性失败直接返回给客户端自行重试
        assert len(scheduled) <= 2
        assert svc.retry_buffer_size() <= 2
        assert dropped, "overflow beyond retry buffer must be reported"
    finally:
        svc.stop()


def test_pending_retries_survive_restart(tmp_path):
    settings = _retry_settings(tmp_path)
    store = StateStore(settings.state_path)
    agg1 = EventTimeAggregator(settings, store=store)
    svc1 = IngestService(agg1, settings)
    # 不启动工作线程，直接登记一条待重试记录并落盘
    agg1.mark_pending(Event("p-1", "k", 100, value=7), RejectReason.BUFFER_FULL)
    d = agg1.query_event("p-1")
    log_line(f"before restart: p-1 status={d.status} reason={d.reason}")
    assert d.status == DecisionStatus.PENDING

    # 重启：待重试事件从快照恢复，服务启动后自动重试并最终接受
    agg2 = EventTimeAggregator(settings, store=store)
    svc2 = IngestService(agg2, settings)
    svc2.start()
    try:
        assert _wait_until(
            lambda: agg2.query_event("p-1").status == DecisionStatus.ACCEPTED
        )
        win = agg2.snapshot().windows[0]
        log_line(
            f"after restart+retry: window {win.window.interval()} "
            f"count={win.count} total={win.total}"
        )
        assert win.count == 1 and win.total == 7
        assert svc2.retry_buffer_size() == 0
    finally:
        svc2.stop()
    svc1.stop()
