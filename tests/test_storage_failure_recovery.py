"""落盘暂时失败：服务不停摆、不出现半生效状态，恢复后自愈且结果一致。"""

import json
import os

from conftest import log_line

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.ingest import IngestService
from app.models import DecisionStatus, Event
from app.storage import StateStore


class FlakyStore(StateStore):
    """可开关故障的存储：fail=True 时 save 抛 OSError 模拟落盘故障。"""

    def __init__(self, path):
        super().__init__(path)
        self.fail = False
        self.saved = 0

    def save(self, *args, **kwargs):
        if self.fail:
            raise OSError("simulated disk failure")
        self.saved += 1
        return super().save(*args, **kwargs)


def _settings(tmp_path, **kw):
    base = {
        "window_size_ms": 1000,
        "allowed_lateness_ms": 500,
        "max_pending_events": 100,
        "ingest_workers": 2,
        "state_path": str(tmp_path / "state.json"),
        "max_retry_events": 0,
        "retry_interval_ms": 50,
    }
    base.update(kw)
    return Settings(**base)


def test_persist_failure_does_not_stop_ingest_and_self_heals(tmp_path):
    settings = _settings(tmp_path)
    store = FlakyStore(settings.state_path)
    agg = EventTimeAggregator(settings, store=store)
    svc = IngestService(agg, settings)
    svc.start()
    try:
        assert svc.submit(Event("ok-1", "k", 100)).accepted

        # 落盘开始失败：摄取链路不停摆，结果仍给出，台账可查
        store.fail = True
        r = svc.submit(Event("ok-2", "k", 200, value=2))
        log_line(
            f"during outage: accepted={r.accepted} "
            f"degraded={agg.storage_degraded}"
        )
        assert r.accepted
        assert agg.storage_degraded
        assert agg.query_event("ok-2").status == DecisionStatus.ACCEPTED

        # 落盘恢复：维护线程自动重试落盘（自愈），无需重启
        store.fail = False
        deadline = 100
        for _ in range(deadline):
            if not agg.storage_degraded:
                break
            import time
            time.sleep(0.02)
        log_line(f"after heal: degraded={agg.storage_degraded}")
        assert not agg.storage_degraded

        # 恢复后状态完整：重启（用同一状态文件）结果一致、不重复计数
        agg2 = EventTimeAggregator(settings, store=StateStore(settings.state_path))
        win = agg2.snapshot().windows[0]
        log_line(
            f"after restart: window {win.window.interval()} "
            f"count={win.count} total={win.total}"
        )
        assert win.count == 2 and win.total == 3
        assert agg2.query_event("ok-2").status == DecisionStatus.ACCEPTED
    finally:
        svc.stop()


def test_failed_save_leaves_no_partial_state(tmp_path):
    settings = _settings(tmp_path)
    store = FlakyStore(settings.state_path)
    agg = EventTimeAggregator(settings, store=store)
    agg.add_event(Event("s-1", "k", 100))
    # 磁盘上有一份完整快照
    with open(settings.state_path, encoding="utf-8") as fh:
        good = json.load(fh)
    assert good["watermark_ms"] == -1

    # 故障期间的写入要么完整生效、要么保持旧快照，不留半个文件
    store.fail = True
    agg.add_event(Event("s-2", "k", 200))
    with open(settings.state_path, encoding="utf-8") as fh:
        on_disk = json.load(fh)  # 若文件损坏这里会直接抛错
    log_line(f"during outage on-disk seen_ids={on_disk['seen_ids']}")
    assert on_disk == good  # 仍是上一份完整快照
    # 目录下不残留临时文件
    leftovers = [f for f in os.listdir(tmp_path) if f.startswith(".state-")]
    assert leftovers == []

    # 恢复后一次成功保存即包含全部状态（全量快照，无半生效）
    store.fail = False
    assert agg.flush()
    agg2 = EventTimeAggregator(settings, store=StateStore(settings.state_path))
    assert agg2.snapshot().windows[0].count == 2


def test_restart_during_outage_keeps_confirmed_results_consistent(tmp_path):
    """故障期间重启：已确认（已落盘）的结果不丢、不变、不重复。"""
    settings = _settings(tmp_path)
    store = FlakyStore(settings.state_path)
    agg = EventTimeAggregator(settings, store=store)
    agg.add_event(Event("c-1", "k", 100))  # 已确认落盘
    store.fail = True
    agg.add_event(Event("c-2", "k", 200))  # 仅内存确认，未落盘

    # 故障期间重启：只能恢复到最后一份完整快照，且该快照是自洽的
    agg2 = EventTimeAggregator(settings, store=StateStore(settings.state_path))
    snap = agg2.snapshot()
    log_line(
        f"restart during outage: watermark={snap.watermark_ms} "
        f"count={snap.windows[0].count}"
    )
    assert snap.windows[0].count == 1  # c-1 在，c-2 未持久化不算丢失确认
    assert agg2.query_event("c-1").status == DecisionStatus.ACCEPTED
    # c-1 重放仍是重复，不会重复计数
    from app.models import RejectReason
    r = agg2.add_event(Event("c-1", "k", 100))
    assert r.reason == RejectReason.DUPLICATE_EVENT
    assert agg2.snapshot().windows[0].count == 1
