"""乱序、迟到、水位线回退、窗口关闭语义测试。"""

from app.aggregator import EventTimeAggregator
from app.models import Event, RejectReason

from conftest import log_line


def test_out_of_order_within_watermark(settings):
    agg = EventTimeAggregator(settings)
    # 乱序到达：1500 -> 100 -> 1200
    for eid, t in [("e1", 1500), ("e2", 100), ("e3", 1200)]:
        r = agg.add_event(Event(eid, "k", t))
        assert r.accepted, r.reason
        log_line(f"ingest t={t:>5} accepted watermark={r.watermark_ms}")

    snap = agg.snapshot()
    win0 = [w for w in snap.windows if w.window.start_ms == 0][0]
    win1 = [w for w in snap.windows if w.window.start_ms == 1000][0]
    log_line(f"window {win0.window.interval()} count={win0.count}")
    log_line(f"window {win1.window.interval()} count={win1.count}")
    assert win0.count == 1
    assert win1.count == 2


def test_watermark_never_moves_back(settings):
    agg = EventTimeAggregator(settings)
    assert agg.advance_watermark(2000) == []
    log_line(f"watermark advanced -> {agg.watermark_ms}")
    # 回退尝试必须被忽略
    assert agg.advance_watermark(500) == []
    log_line(f"watermark after rollback attempt -> {agg.watermark_ms} (unchanged)")
    assert agg.watermark_ms == 2000
    # 相等值不产生变化
    assert agg.advance_watermark(2000) == []
    assert agg.watermark_ms == 2000


def test_late_event_accepted_within_grace_then_rejected(settings):
    agg = EventTimeAggregator(settings)
    agg.add_event(Event("e1", "k", 1200))
    agg.advance_watermark(2000)  # W=2000, 宽限到 1500
    log_line("watermark=2000 grace-floor=1500")

    # t=1700 迟到但在宽限内 [1500, 2000)
    r_late_ok = agg.add_event(Event("e2", "k", 1700))
    log_line(
        f"late t=1700 accepted={r_late_ok.accepted} late_flag={r_late_ok.late}"
    )
    assert r_late_ok.accepted and r_late_ok.late

    # t=1400 迟到且超出宽限
    r_late_bad = agg.add_event(Event("e3", "k", 1400))
    log_line(
        f"late t=1400 accepted={r_late_bad.accepted} reason={r_late_bad.reason}"
    )
    assert not r_late_bad.accepted
    assert r_late_bad.reason == RejectReason.LATE_EVENT


def test_window_closes_after_watermark_crosses_end_plus_grace(settings):
    agg = EventTimeAggregator(settings)
    agg.add_event(Event("e1", "k", 100))
    agg.add_event(Event("e2", "k", 900))

    # W=1400: cutoff=900, 窗口 [0,1000) end=1000 > 900 不关闭
    assert agg.advance_watermark(1400) == []
    log_line(f"watermark=1400 window [0, 1000) still open")

    # W=1500: cutoff=1000, end<=cutoff -> 关闭
    closed = agg.advance_watermark(1500)
    log_line(
        "watermark=1500 closed windows: "
        + ", ".join(w.interval() for w in closed)
    )
    assert len(closed) == 1 and closed[0].start_ms == 0

    # 关闭后同窗口事件必须以 LATE_EVENT 拒绝
    r = agg.add_event(Event("e3", "k", 200))
    log_line(f"post-close t=200 rejected reason={r.reason}")
    assert r.reason == RejectReason.LATE_EVENT

    win = [w for w in agg.snapshot().windows if w.window.start_ms == 0][0]
    assert win.closed and win.count == 2


def test_per_key_windows_close_independently(settings):
    agg = EventTimeAggregator(settings)
    agg.add_event(Event("a1", "a", 100))
    agg.add_event(Event("b1", "b", 5100))
    closed = agg.advance_watermark(1500)
    log_line(
        "closed at W=1500: " + ", ".join(f"{w.key}:{w.interval()}" for w in closed)
    )
    assert {w.key for w in closed} == {"a"}
