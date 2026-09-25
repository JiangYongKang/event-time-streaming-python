"""重复投递幂等性与重启恢复一致性测试。"""

from app.aggregator import EventTimeAggregator
from app.models import Event, RejectReason
from app.storage import StateStore

from conftest import log_line


def test_duplicate_event_counted_once(settings):
    agg = EventTimeAggregator(settings)
    first = agg.add_event(Event("dup-1", "k", 100, value=3))
    assert first.accepted
    for i in range(4):
        r = agg.add_event(Event("dup-1", "k", 100, value=3))
        log_line(f"duplicate #{i + 1} accepted={r.accepted} reason={r.reason}")
        assert not r.accepted
        assert r.reason == RejectReason.DUPLICATE_EVENT

    win = agg.snapshot().windows[0]
    log_line(f"window {win.window.interval()} count={win.count} total={win.total}")
    assert win.count == 1 and win.total == 3


def test_duplicate_after_restart_still_rejected(settings):
    store = StateStore(settings.state_path)
    agg1 = EventTimeAggregator(settings, store=store)
    agg1.add_event(Event("persist-1", "k", 100))
    agg1.advance_watermark(500)
    before = agg1.snapshot()
    log_line(
        f"before restart: watermark={before.watermark_ms} "
        f"windows={[(w.window.interval(), w.count) for w in before.windows]}"
    )

    # 模拟重启：用同一个状态文件新建聚合器
    agg2 = EventTimeAggregator(settings, store=store)
    after = agg2.snapshot()
    log_line(
        f"after restart:  watermark={after.watermark_ms} "
        f"windows={[(w.window.interval(), w.count, w.closed) for w in after.windows]}"
    )
    assert after.watermark_ms == before.watermark_ms
    assert after.windows[0].count == 1

    r = agg2.add_event(Event("persist-1", "k", 100))
    log_line(f"replayed event after restart accepted={r.accepted} reason={r.reason}")
    assert r.reason == RejectReason.DUPLICATE_EVENT
    assert agg2.snapshot().windows[0].count == 1


def test_closed_window_state_survives_restart(settings):
    store = StateStore(settings.state_path)
    agg1 = EventTimeAggregator(settings, store=store)
    agg1.add_event(Event("c1", "k", 100))
    closed = agg1.advance_watermark(1500)
    assert len(closed) == 1
    log_line(f"closed before restart: {[w.interval() for w in closed]}")

    agg2 = EventTimeAggregator(settings, store=store)
    win = agg2.snapshot().windows[0]
    log_line(f"after restart window {win.window.interval()} closed={win.closed}")
    assert win.closed

    r = agg2.add_event(Event("c2", "k", 200))
    log_line(f"late into reopened? accepted={r.accepted} reason={r.reason}")
    assert r.reason == RejectReason.LATE_EVENT
