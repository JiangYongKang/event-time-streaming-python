"""事件时间窗口聚合核心。

语义约定
--------
* 事件时间按 ``window_size_ms`` 切分为左闭右开窗口 ``[start, end)``。
* 水位线 ``W`` 表示“认为不会再收到 event_time < W 的事件”。水位线单调
  递增，任何回退调用都会被忽略（保持当前值）。
* 迟到事件：``event_time < W``。
  - 若 ``event_time >= W - allowed_lateness_ms``，在宽限期内，仍计入对应
    窗口，结果中 ``late=True``；
  - 否则以 :data:`RejectReason.LATE_EVENT` 拒绝（目标窗口可能已关闭）。
* 窗口关闭：当 ``W >= end + allowed_lateness_ms`` 时窗口永久关闭；关闭后
  该窗口不再接受任何事件（迟到事件被拒绝）。
* 同一 ``event_id`` 只计一次，重复投递返回 DUPLICATE_EVENT。
"""

import threading
from collections import defaultdict

from app.config import Settings
from app.models import (
    AcceptResult,
    Event,
    RejectReason,
    Snapshot,
    WindowAggregate,
    WindowId,
)
from app.storage import StateStore

_INITIAL_WATERMARK = -1


def validate_event(event: Event) -> RejectReason | None:
    if not event.event_id or not isinstance(event.event_id, str):
        return RejectReason.INVALID
    if not event.key or not isinstance(event.key, str):
        return RejectReason.INVALID
    if event.event_time_ms < 0:
        return RejectReason.INVALID
    return None


class EventTimeAggregator:
    def __init__(self, settings: Settings, store: StateStore | None = None) -> None:
        self.settings = settings
        self._store = store
        self._lock = threading.RLock()
        # key -> window_start_ms -> aggregate
        self._windows: dict[str, dict[int, WindowAggregate]] = defaultdict(dict)
        self._seen_ids: set[str] = set()
        self._watermark_ms: int = _INITIAL_WATERMARK
        self._closed_ever: list[WindowId] = []
        if store is not None:
            windows, watermark_ms, seen_ids = store.load()
            self._windows = defaultdict(dict, windows)
            self._seen_ids = seen_ids
            self._watermark_ms = max(_INITIAL_WATERMARK, watermark_ms)

    # ---- 内部工具 -------------------------------------------------------

    def _window_of(self, key: str, event_time_ms: int) -> WindowId:
        size = self.settings.window_size_ms
        start = (event_time_ms // size) * size
        return WindowId(key=key, start_ms=start, end_ms=start + size)

    def _persist_locked(self) -> None:
        if self._store is not None:
            self._store.save(
                self._windows, self._watermark_ms, self._seen_ids
            )

    # ---- 水位线 ---------------------------------------------------------

    def advance_watermark(self, timestamp_ms: int) -> list[WindowId]:
        """推进水位线；小于等于当前值的调用不会使其回退。

        返回本次推进新关闭的窗口列表。
        """
        with self._lock:
            if timestamp_ms <= self._watermark_ms:
                # 水位线不得回退：忽略，状态不变
                return []
            self._watermark_ms = timestamp_ms
            closed = self.close_expired_windows()
            self._persist_locked()
            return closed

    def close_expired_windows(self) -> list[WindowId]:
        cutoff = self._watermark_ms - self.settings.allowed_lateness_ms
        newly_closed: list[WindowId] = []
        for buckets in self._windows.values():
            for agg in buckets.values():
                if not agg.closed and agg.window.end_ms <= cutoff:
                    agg.closed = True
                    newly_closed.append(agg.window)
        self._closed_ever.extend(newly_closed)
        return newly_closed

    # ---- 事件摄取 -------------------------------------------------------

    def add_event(self, event: Event) -> AcceptResult:
        invalid = validate_event(event)
        if invalid is not None:
            return AcceptResult(
                accepted=False,
                watermark_ms=self._watermark_ms,
                reason=invalid,
            )
        with self._lock:
            wm = self._watermark_ms
            window = self._window_of(event.key, event.event_time_ms)

            # 1) 幂等去重
            if event.event_id in self._seen_ids:
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    reason=RejectReason.DUPLICATE_EVENT,
                )

            # 2) 迟到判定（在宽限期之外 -> 拒绝）
            late = event.event_time_ms < wm
            if late and event.event_time_ms < wm - self.settings.allowed_lateness_ms:
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    late=True, reason=RejectReason.LATE_EVENT,
                )

            # 3) 窗口已关闭 -> 拒绝（与迟到拒绝同因，结果可解释）
            buckets = self._windows[event.key]
            agg = buckets.get(window.start_ms)
            if agg is not None and agg.closed:
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    late=late, reason=RejectReason.LATE_EVENT,
                )

            # 4) 计入（聚合 + 去重集合在同一把锁内更新，避免竞态丢计数）
            if agg is None:
                agg = WindowAggregate(window=window)
                buckets[window.start_ms] = agg
            agg.count += 1
            agg.total += event.value
            agg.event_ids.add(event.event_id)
            self._seen_ids.add(event.event_id)
            self._persist_locked()

            return AcceptResult(
                accepted=True,
                watermark_ms=wm,
                late=late,
            )

    # ---- 观测 -----------------------------------------------------------

    def snapshot(self) -> Snapshot:
        with self._lock:
            all_windows = [
                agg
                for buckets in self._windows.values()
                for agg in buckets.values()
            ]
            return Snapshot(
                watermark_ms=self._watermark_ms,
                windows=sorted(
                    all_windows,
                    key=lambda a: (a.window.key, a.window.start_ms),
                ),
            )

    @property
    def watermark_ms(self) -> int:
        return self._watermark_ms
