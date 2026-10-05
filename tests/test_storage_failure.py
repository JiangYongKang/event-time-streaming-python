"""落盘暂时失败：服务不停摆、状态不生效一半、恢复后自愈且结果一致。"""

import time

from conftest import log_line

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.ingest import IngestService
from app.models import Event, OutcomeStatus, RejectReason
from app.storage import StateStore


class FlakyStore(StateStore):
    """可开关的故障注入存储：fail=True 时写入抛 IOError（模拟磁盘故障）。"""

    def __init__(self, path):
        super().__init__(path)
        self.fail = False

    def save(self, *args, **kwargs):
        if self.fail:
            raise OSError("simulated disk failure")
        return super().save(*args, **kwargs)


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


def test_accept_rolls_back_when_persist_fails(tmp_path):
    settings = _settings(tmp_path)
    store = FlakyStore(settings.state_path)
    agg = EventTimeAggregator(settings, store=store)
    assert agg.add_event(Event("ok-1", "k", 100)).accepted

    store.fail = True
    r = agg.add_event(Event("f-1", "k", 200))
    log_line(
        f"persist-failed accept: accepted={r.accepted} reason={r.reason} "
        f"retryable={r.retryable} store_healthy={agg.store_healthy}"
    )
    assert r.reason == RejectReason.STORE_UNAVAILABLE and r.retryable
    assert not agg.store_healthy

    # 内存变更已回滚：不出现“只生效一半”
    win = agg.snapshot().windows[0]
    log_line(f"window after rollback count={win.count}")
    assert win.count == 1
    assert agg.query_event("f-1") is None  # 未确认，查询如实反映

    # 磁盘恢复后重发同一事件：成功且只计一次
    store.fail = False
    r2 = agg.add_event(Event("f-1", "k", 200))
    log_line(f"after recovery re-accept: accepted={r2.accepted}")
    assert r2.accepted and agg.store_healthy
    assert agg.snapshot().windows[0].count == 2
    assert agg.query_event("f-1").status == OutcomeStatus.ACCEPTED


def test_disk_state_not_half_applied(tmp_path):
    settings = _settings(tmp_path)
    store = FlakyStore(settings.state_path)
    agg = EventTimeAggregator(settings, store=store)
    agg.add_event(Event("d-1", "k", 100))
    agg.advance_watermark(500)

    store.fail = True
    agg.add_event(Event("d-2", "k", 200))       # 回滚
    agg.advance_watermark(1500)                  # 内存推进，落盘失败

    # 磁盘上仍是上一份完整快照（无部分写入）
    disk = StateStore(settings.state_path).load()
    log_line(
        f"disk snapshot during outage: watermark={disk.watermark_ms} "
        f"seen={sorted(disk.seen_ids)}"
    )
    assert disk.watermark_ms == 500
    assert disk.seen_ids == {"d-1"}

    # 恢复后 flush 自愈，完整状态落盘
    store.fail = False
    assert agg.flush() and agg.store_healthy
    disk2 = StateStore(settings.state_path).load()
    log_line(
        f"disk snapshot after heal: watermark={disk2.watermark_ms} "
        f"seen={sorted(disk2.seen_ids)}"
    )
    assert disk2.watermark_ms == 1500
    assert disk2.seen_ids == {"d-1"}


def test_service_self_heals_and_stays_consistent(tmp_path):
    settings = _settings(tmp_path)
    store = FlakyStore(settings.state_path)
    agg = EventTimeAggregator(settings, store=store)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        assert svc.submit(Event("h-0", "k", 100)).accepted

        # 落盘故障期间：服务不停摆，返回可重试拒绝并自动暂存
        store.fail = True
        r = svc.submit(Event("h-1", "k", 200), timeout=5)
        log_line(
            f"during outage: reason={r.reason} retryable={r.retryable} "
            f"store_healthy={agg.store_healthy}"
        )
        assert r.reason == RejectReason.STORE_UNAVAILABLE and r.retryable
        assert agg.add_event(Event("h-2", "k", 300)).reason == (
            RejectReason.STORE_UNAVAILABLE
        )  # 服务仍在处理

        # 落盘恢复：暂存事件被自动重试接受，且只计一次
        store.fail = False
        assert _wait_until(lambda: svc.retry_buffer_size() == 0)
        assert _wait_until(lambda: agg.store_healthy)
        outcome = svc.query_event("h-1")
        log_line(f"h-1 outcome after heal: {outcome.status}")
        assert outcome.status == OutcomeStatus.ACCEPTED
        win = agg.snapshot().windows[0]
        log_line(f"window after heal count={win.count}")
        assert win.count == 2  # h-0 + h-1，h-2 被回滚未计入
    finally:
        svc.stop()

    # 重启后：已确认结果仍然一致
    agg2 = EventTimeAggregator(settings, store=FlakyStore(settings.state_path))
    win2 = agg2.snapshot().windows[0]
    log_line(
        f"after restart: watermark={agg2.watermark_ms} count={win2.count} "
        f"h-1={agg2.query_event('h-1').status}"
    )
    assert win2.count == 2
    assert agg2.query_event("h-1").status == OutcomeStatus.ACCEPTED
    # 重试不会造成重复计数
    dup = agg2.add_event(Event("h-1", "k", 200))
    assert dup.reason == RejectReason.DUPLICATE_EVENT
    assert agg2.snapshot().windows[0].count == 2
