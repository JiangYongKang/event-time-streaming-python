"""并发摄取与有界缓冲背压测试。"""

import threading
from concurrent.futures import ThreadPoolExecutor

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.ingest import IngestService
from app.models import Event, RejectReason

from conftest import log_line


class SlowAggregator(EventTimeAggregator):
    """在 add_event 上人为加延迟，制造真实的并发竞争窗口。"""

    def __init__(self, settings, delay=0.002):
        super().__init__(settings)
        self.delay = delay

    def add_event(self, event):
        result = super().add_event(event)
        # 延迟放在聚合后仍可使队列积压（消费速率下降）
        import time
        time.sleep(self.delay)
        return result


def test_concurrent_same_key_no_lost_count(settings):
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    svc.start()
    n = 200
    try:
        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(
                pool.map(
                    lambda i: svc.submit(Event(f"ev-{i}", "k", 100, value=1)),
                    range(n),
                )
            )
    finally:
        svc.stop()

    accepted = [r for r in results if r.accepted]
    log_line(f"concurrent accepted={len(accepted)}/{n} watermark={results[0].watermark_ms}")
    assert len(accepted) == n
    win = agg.snapshot().windows[0]
    log_line(f"window {win.window.interval()} count={win.count} total={win.total}")
    assert win.count == n and win.total == n


def test_buffer_full_gives_distinguishable_reason(tmp_path):
    small = Settings(
        window_size_ms=1000,
        allowed_lateness_ms=500,
        max_pending_events=2,
        ingest_workers=1,
        state_path=str(tmp_path / "state.json"),
    )
    agg = SlowAggregator(small, delay=0.05)
    svc = IngestService(agg, small)
    svc.start()
    reasons = []
    try:
        # 以远超消费速度的方式突发提交，占满有界队列
        threads = []

        def fire(i):
            r = svc.submit(Event(f"burst-{i}", "k", 100), timeout=3)
            reasons.append(r.reason.value if r.reason else "ACCEPTED")

        for i in range(20):
            t = threading.Thread(target=fire, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
    finally:
        svc.stop()

    accepted_n = reasons.count("ACCEPTED")
    full_n = reasons.count("BUFFER_FULL")
    log_line(
        f"burst results: accepted={accepted_n} "
        f"buffer_full={full_n} peak_buffer_capacity=2"
    )
    assert accepted_n >= 1
    assert full_n >= 1
    # 拒绝原因必须可区分：不能与迟到/重复混淆
    assert set(reasons) <= {"ACCEPTED", "BUFFER_FULL"}
    # 被接受的计数必须准确（无丢失）
    assert agg.snapshot().windows[0].count == accepted_n


def test_busy_after_stop(settings):
    agg = EventTimeAggregator(settings)
    svc = IngestService(agg, settings)
    svc.start()
    svc.stop()
    r = svc.submit(Event("late-start", "k", 100))
    log_line(f"submit after stop reason={r.reason}")
    assert r.reason == RejectReason.BUSY
