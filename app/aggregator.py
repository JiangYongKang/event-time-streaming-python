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

import logging
import threading
from collections import defaultdict

from app.config import Settings
from app.models import (
    AcceptResult,
    DecisionStatus,
    Event,
    EventDecision,
    PendingRetry,
    RejectReason,
    Snapshot,
    WindowAggregate,
    WindowId,
)
from app.storage import StateStore

_INITIAL_WATERMARK = -1

logger = logging.getLogger(__name__)


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
        # 事件处理结果台账：event_id -> 首个确定性结果（接受/终态拒绝/待重试）
        self._decisions: dict[str, EventDecision] = {}
        # 可重试摄取暂存记录（由摄取层维护，随快照一起持久化）
        self._pending_retries: dict[str, PendingRetry] = {}
        # 落盘是否处于降级状态（最近一次持久化失败）
        self._persist_degraded = False
        if store is not None:
            windows, watermark_ms, seen_ids, decisions, pending = store.load()
            self._windows = defaultdict(dict, windows)
            self._seen_ids = seen_ids
            self._watermark_ms = max(_INITIAL_WATERMARK, watermark_ms)
            self._decisions = decisions
            self._pending_retries = pending

    # ---- 内部工具 -------------------------------------------------------

    def _window_of(self, key: str, event_time_ms: int) -> WindowId:
        size = self.settings.window_size_ms
        start = (event_time_ms // size) * size
        return WindowId(key=key, start_ms=start, end_ms=start + size)

    def _persist_locked(self) -> None:
        """尽力持久化；失败不抛出，只标记降级，等待后续自愈。

        快照是“全量 + 原子替换”，因此落盘暂时失败时内存状态仍然是完整的
        唯一事实来源；恢复后任意一次成功保存都会把完整状态写盘，不会出现
        只生效一半的状态。
        """
        if self._store is None:
            return
        try:
            self._store.save(
                self._windows,
                self._watermark_ms,
                self._seen_ids,
                self._decisions,
                self._pending_retries,
            )
        except Exception as exc:  # noqa: BLE001 - 落盘故障不应拖垮摄取链路
            if not self._persist_degraded:
                logger.warning(
                    "state persist failed, entering degraded mode: %s", exc
                )
            self._persist_degraded = True
            return
        if self._persist_degraded:
            logger.info("state persist recovered, degraded mode cleared")
        self._persist_degraded = False

    def flush(self) -> bool:
        """主动重试持久化（落盘恢复后的自愈入口）。返回当前是否健康。"""
        with self._lock:
            self._persist_locked()
            return not self._persist_degraded

    @property
    def storage_degraded(self) -> bool:
        return self._persist_degraded

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

    def _record_decision_locked(self, decision: EventDecision) -> None:
        """记录首个确定性结果；终态结果不会被后续投递覆盖。

        PENDING（待重试）是中间态，可以被随后的终态结果覆盖。
        """
        existing = self._decisions.get(decision.event_id)
        if existing is not None and existing.status is not DecisionStatus.PENDING:
            return
        self._decisions[decision.event_id] = decision

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

            # 1) 幂等去重（保留台账中的首个结果，不被重复投递覆盖）
            if event.event_id in self._seen_ids:
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    reason=RejectReason.DUPLICATE_EVENT,
                )

            # 2) 迟到判定（在宽限期之外 -> 终态拒绝，记入台账）
            late = event.event_time_ms < wm
            if late and event.event_time_ms < wm - self.settings.allowed_lateness_ms:
                self._record_decision_locked(
                    EventDecision(
                        event_id=event.event_id,
                        status=DecisionStatus.REJECTED,
                        reason=RejectReason.LATE_EVENT,
                        late=True,
                        watermark_ms=wm,
                    )
                )
                self._persist_locked()
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    late=True, reason=RejectReason.LATE_EVENT,
                )

            # 3) 窗口已关闭 -> 终态拒绝（与迟到拒绝同因，结果可解释）
            buckets = self._windows[event.key]
            agg = buckets.get(window.start_ms)
            if agg is not None and agg.closed:
                self._record_decision_locked(
                    EventDecision(
                        event_id=event.event_id,
                        status=DecisionStatus.REJECTED,
                        reason=RejectReason.LATE_EVENT,
                        late=late,
                        watermark_ms=wm,
                    )
                )
                self._persist_locked()
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
            self._record_decision_locked(
                EventDecision(
                    event_id=event.event_id,
                    status=DecisionStatus.ACCEPTED,
                    late=late,
                    watermark_ms=wm,
                )
            )
            # 事件已有终态结果，不再属于待重试集合
            self._pending_retries.pop(event.event_id, None)
            self._persist_locked()

            return AcceptResult(
                accepted=True,
                watermark_ms=wm,
                late=late,
            )

    # ---- 按标识查询 -----------------------------------------------------

    def query_event(self, event_id: str) -> EventDecision | None:
        """按 event_id 查询处理结果；未处理过返回 None。"""
        with self._lock:
            return self._decisions.get(event_id)

    # ---- 可重试摄取暂存记录（由摄取层调用） ------------------------------

    def mark_pending(self, event: Event, reason: RejectReason) -> None:
        """把暂时性失败的事件登记为待重试，并在台账中记为 PENDING。"""
        with self._lock:
            existing = self._pending_retries.get(event.event_id)
            attempts = existing.attempts if existing is not None else 0
            self._pending_retries[event.event_id] = PendingRetry(
                event=event, reason=reason, attempts=attempts
            )
            self._record_decision_locked(
                EventDecision(
                    event_id=event.event_id,
                    status=DecisionStatus.PENDING,
                    reason=reason,
                    watermark_ms=self._watermark_ms,
                )
            )
            self._persist_locked()

    def clear_pending(self, event_id: str) -> None:
        """重试得出终态结果后，移除暂存记录（终态决策由 add_event 记录）。"""
        with self._lock:
            if event_id in self._pending_retries:
                self._pending_retries.pop(event_id, None)
                self._persist_locked()

    def pending_retries(self) -> list[PendingRetry]:
        with self._lock:
            return list(self._pending_retries.values())

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
