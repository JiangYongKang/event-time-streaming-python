"""可重试摄取：暂时性失败进入有界暂存区并自动重试；终态拒绝不进暂存区。"""

import threading
import time

from conftest import log_line

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.ingest import IngestService
from app.models import Event, OutcomeStatus, RejectReason
from app.storage import StateStore


def _settings(tmp_path, **overrides):
    base = {
        "window_size_ms": 1000,
        "allowed_lateness_ms": 500,
        "max_pending_events": 100,
        "ingest_workers": 1,
        "state_path": str(tmp_path / "state.json"),
        "max_retry_events": 10,
        "retry_interval_ms": 50,
    }
    base.update(overrides)
    return Settings(**base)


def _wait_until(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class SlowAggregator(EventTimeAggregator):
    def __init__(self, settings, delay=0.1):
        super().__init__(settings)
        self.delay = delay

    def add_event(self, event):
        result = super().add_event(event)
        time.sleep(self.delay)
        return result


def test_buffer_full_is_staged_and_retried_automatically(tmp_path):
    settings = _settings(tmp_path, max_pending_events=1)
    agg = SlowAggregator(settings, delay=0.1)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        results: dict[str, object] = {}

        def fire(eid, t):
            results[eid] = svc.submit(Event(eid, "k", t), timeout=5)

        # 并发提交：s1 占住工作线程，s2 占住队列，s3 触发 BUFFER_FULL
        t1 = threading.Thread(target=fire, args=("s1", 100))
        t2 = threading.Thread(target=fire, args=("s2", 200))
        t1.start()
        time.sleep(0.03)  # 等 s1 被工作线程取走
        t2.start()
        time.sleep(0.03)  # 等 s2 占满队列
        r3 = svc.submit(Event("s3", "k", 300), timeout=5)
        t1.join()
        t2.join()
        r1, r2 = results["s1"], results["s2"]
        log_line(
            f"r1 accepted={r1.accepted} r2 accepted={r2.accepted} "
            f"r3 reason={r3.reason} retryable={r3.retryable}"
        )
        assert r1.accepted and r2.accepted
        assert r3.reason == RejectReason.BUFFER_FULL and r3.retryable

        # 容量恢复后暂存事件被自动重试并接受
        assert _wait_until(lambda: svc.retry_buffer_size() == 0)
        outcome = svc.query_event("s3")
        log_line(f"s3 final outcome: {outcome.status}")
        assert outcome.status == OutcomeStatus.ACCEPTED
    finally:
        svc.stop()
    win = agg.snapshot().windows[0]
    log_line(f"window {win.window.interval()} count={win.count}")
    assert win.count == 3  # 每个事件恰好计一次


def test_terminal_rejections_are_not_staged(tmp_path):
    settings = _settings(tmp_path)
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        agg.advance_watermark(2000)  # 宽限到 1500
        late = svc.submit(Event("t-late", "k", 1000))
        log_line(f"late reason={late.reason} retryable={late.retryable}")
        assert late.reason == RejectReason.LATE_EVENT and not late.retryable

        svc.submit(Event("t-dup", "k", 1700))
        dup = svc.submit(Event("t-dup", "k", 1700))
        log_line(f"dup reason={dup.reason} retryable={dup.retryable}")
        assert dup.reason == RejectReason.DUPLICATE_EVENT and not dup.retryable

        invalid = svc.submit(Event("", "k", 100))
        log_line(f"invalid reason={invalid.reason} retryable={invalid.retryable}")
        assert invalid.reason == RejectReason.INVALID and not invalid.retryable

        # 终态拒绝不进入暂存区
        assert svc.retry_buffer_size() == 0
    finally:
        svc.stop()


def test_retry_buffer_is_bounded_and_recovers_after_restart_of_service(tmp_path):
    settings = _settings(tmp_path, max_retry_events=2)
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    # 服务未启动：BUSY 为暂时性失败，进入暂存区；容量 2，超出部分直接返回
    results = [svc.submit(Event(f"b-{i}", "k", 100 + i)) for i in range(4)]
    assert all(r.reason == RejectReason.BUSY and r.retryable for r in results)
    log_line(f"staged={svc.retry_buffer_size()}/2 (capacity bound)")
    assert svc.retry_buffer_size() == 2

    # 服务恢复后自动、有节奏地重试暂存事件
    svc.start()
    try:
        assert _wait_until(lambda: svc.retry_buffer_size() == 0)
        win = agg.snapshot().windows[0]
        log_line(f"after recovery window count={win.count}")
        assert win.count == 2  # 只有入区的两个被补上，无重复计数
        assert svc.query_event("b-0").status == OutcomeStatus.ACCEPTED
    finally:
        svc.stop()


def test_pending_retries_survive_restart(tmp_path):
    settings = _settings(tmp_path)
    store = StateStore(settings.state_path)
    agg1 = EventTimeAggregator(settings, store=store)
    svc1 = IngestService(agg1, settings)
    # 未启动时提交 -> BUSY -> 暂存并随快照落盘
    svc1.submit(Event("r-1", "k", 100))
    svc1.submit(Event("r-2", "k", 200))
    log_line(f"staged before restart={svc1.retry_buffer_size()}")
    assert svc1.retry_buffer_size() == 2

    # 模拟重启：同一状态文件重建聚合器与摄取服务
    agg2 = EventTimeAggregator(settings, store=store)
    svc2 = IngestService(agg2, settings)
    svc2.start()
    try:
        assert _wait_until(lambda: svc2.retry_buffer_size() == 0)
        win = agg2.snapshot().windows[0]
        log_line(f"after restart window count={win.count}")
        assert win.count == 2
        assert svc2.query_event("r-1").status == OutcomeStatus.ACCEPTED
        assert svc2.query_event("r-2").status == OutcomeStatus.ACCEPTED
    finally:
        svc2.stop()


def test_manual_resubmit_during_staging_counts_once(tmp_path):
    settings = _settings(tmp_path, max_retry_events=4)
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    # 未启动 -> 暂存
    svc.submit(Event("m-1", "k", 100))
    assert svc.retry_buffer_size() == 1

    svc.start()
    try:
        # 客户端同时也手动重发同一标识：一个接受、一个重复，只计一次
        r = svc.submit(Event("m-1", "k", 100))
        log_line(f"manual resubmit accepted={r.accepted} reason={r.reason}")
        assert _wait_until(lambda: svc.retry_buffer_size() == 0)
        win = agg.snapshot().windows[0]
        log_line(f"window count={win.count} (expect 1)")
        assert win.count == 1
        assert svc.query_event("m-1").status == OutcomeStatus.ACCEPTED
    finally:
        svc.stop()
