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

处理结果台账（outcome ledger）
------------------------------
每个被处理过的 ``event_id``（接受或终态拒绝）都会记录一条
:class:`EventOutcome`，随状态快照一起原子持久化。客户端超时/失败后可按
``event_id`` 查询当时的结果，放心决定重试或放弃；重启后台账依旧可查。

落盘失败处理
------------
* 接受事件的持久化失败时，**回滚内存中的聚合与去重变更**（不出现只生效
  一半的状态），返回 ``STORE_UNAVAILABLE``（可重试），由上层放入暂存区
  稍后自动重试；服务本身不停摆。
* 水位线推进的持久化失败时，内存水位线保持（单调性优先），标记为
  “待刷盘”，后续任何一次成功写入或 :meth:`flush` 都会把完整快照落盘，
  实现自愈。
"""

import logging
import threading
from collections import defaultdict
from collections.abc import Callable

from app.config import Settings
from app.models import (
    AcceptResult,
    Event,
    EventOutcome,
    OutcomeStatus,
    RejectReason,
    Snapshot,
    WindowAggregate,
    WindowId,
)
from app.storage import StateStore

logger = logging.getLogger("ets.aggregator")

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
        # event_id -> 已确认处理结果（接受或终态拒绝）
        self._outcomes: dict[str, EventOutcome] = {}
        # 暂存区内容提供者（由摄取层注册，持久化时一并快照）
        self._retry_provider: Callable[[], list[Event]] | None = None
        # 上次落盘是否失败（待自愈）
        self._persist_degraded = False
        self._loaded_retry_events: list[Event] = []
        if store is not None:
            loaded = store.load()
            self._windows = defaultdict(dict, loaded.windows)
            self._seen_ids = loaded.seen_ids
            self._watermark_ms = max(_INITIAL_WATERMARK, loaded.watermark_ms)
            self._outcomes = dict(loaded.outcomes)
            self._loaded_retry_events = list(loaded.retry_events)

    # ---- 内部工具 -------------------------------------------------------

    def _window_of(self, key: str, event_time_ms: int) -> WindowId:
        size = self.settings.window_size_ms
        start = (event_time_ms // size) * size
        return WindowId(key=key, start_ms=start, end_ms=start + size)

    def _persist_locked(self) -> None:
        """落盘完整快照；失败时标记 degraded 并抛出，由调用方决定回滚/自愈。"""
        if self._store is None:
            return
        try:
            self._store.save(
                self._windows,
                self._watermark_ms,
                self._seen_ids,
                outcomes=self._outcomes,
                retry_events=(
                    self._retry_provider() if self._retry_provider else []
                ),
            )
        except Exception:
            if not self._persist_degraded:
                logger.warning(
                    "state persist failed; service keeps running on in-memory "
                    "state and will retry on next write/flush"
                )
            self._persist_degraded = True
            raise
        if self._persist_degraded:
            logger.info("state persist recovered; snapshot is durable again")
        self._persist_degraded = False

    # ---- 与摄取层的衔接 --------------------------------------------------

    def set_retry_provider(self, provider: Callable[[], list[Event]]) -> None:
        """注册暂存区内容回调，持久化时把待重试事件一并写入快照。"""
        self._retry_provider = provider

    def take_loaded_retry_events(self) -> list[Event]:
        """取出重启前暂存区中未重试完的事件（只取一次）。"""
        events, self._loaded_retry_events = self._loaded_retry_events, []
        return events

    def persist(self) -> bool:
        """主动落盘一次完整快照（暂存区内容变化后由摄取层调用）。"""
        with self._lock:
            try:
                self._persist_locked()
            except Exception:  # noqa: BLE001 - 落盘故障必须兜底，不能拖垮摄取
                return False
            return True

    def flush(self) -> bool:
        """落盘自愈：若之前写入失败，重试一次完整快照。返回当前是否健康。"""
        with self._lock:
            if not self._persist_degraded:
                return True
            try:
                self._persist_locked()
            except Exception:  # noqa: BLE001 - 落盘故障必须兜底，不能拖垮摄取
                return False
            return True

    @property
    def store_healthy(self) -> bool:
        return not self._persist_degraded

    # ---- 水位线 ---------------------------------------------------------

    def advance_watermark(self, timestamp_ms: int) -> list[WindowId]:
        """推进水位线；小于等于当前值的调用不会使其回退。

        返回本次推进新关闭的窗口列表。落盘失败时水位线在内存中保持
        （单调性优先），标记为待刷盘，后续成功写入会自愈。
        """
        with self._lock:
            if timestamp_ms <= self._watermark_ms:
                # 水位线不得回退：忽略，状态不变
                return []
            self._watermark_ms = timestamp_ms
            closed = self.close_expired_windows()
            try:
                self._persist_locked()
            except Exception:  # noqa: BLE001 - 落盘故障必须兜底，不能拖垮摄取
                # 水位线不回滚（单调性），等待后续写入自愈；
                # _persist_locked 已记录告警
                logger.debug("watermark persist deferred until next write")
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
            with self._lock:
                self._record_outcome_locked(event, False, invalid, False)
            return AcceptResult(
                accepted=False,
                watermark_ms=self._watermark_ms,
                reason=invalid,
            )
        with self._lock:
            wm = self._watermark_ms
            window = self._window_of(event.key, event.event_time_ms)

            # 1) 幂等去重（保留原始 ACCEPTED 台账，不覆盖）
            if event.event_id in self._seen_ids:
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    reason=RejectReason.DUPLICATE_EVENT,
                )

            # 2) 迟到判定（在宽限期之外 -> 终态拒绝，记录台账）
            late = event.event_time_ms < wm
            if late and event.event_time_ms < wm - self.settings.allowed_lateness_ms:
                self._record_outcome_locked(
                    event, False, RejectReason.LATE_EVENT, True
                )
                self._persist_safely_locked()
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    late=True, reason=RejectReason.LATE_EVENT,
                )

            # 3) 窗口已关闭 -> 终态拒绝（与迟到拒绝同因，结果可解释）
            buckets = self._windows[event.key]
            agg = buckets.get(window.start_ms)
            if agg is not None and agg.closed:
                self._record_outcome_locked(
                    event, False, RejectReason.LATE_EVENT, late
                )
                self._persist_safely_locked()
                return AcceptResult(
                    accepted=False, watermark_ms=wm,
                    late=late, reason=RejectReason.LATE_EVENT,
                )

            # 4) 计入（聚合 + 去重集合在同一把锁内更新，避免竞态丢计数）
            existed = agg is not None
            if agg is None:
                agg = WindowAggregate(window=window)
                buckets[window.start_ms] = agg
            agg.count += 1
            agg.total += event.value
            agg.event_ids.add(event.event_id)
            self._seen_ids.add(event.event_id)
            self._record_outcome_locked(event, True, None, late)
            try:
                self._persist_locked()
            except Exception:  # noqa: BLE001 - 落盘故障必须兜底，不能拖垮摄取
                # 落盘失败：回滚内存变更，保证不出现“只生效一半”的状态；
                # 返回可重试拒绝，由上层暂存稍后自动重试
                agg.count -= 1
                agg.total -= event.value
                agg.event_ids.discard(event.event_id)
                self._seen_ids.discard(event.event_id)
                self._outcomes.pop(event.event_id, None)
                if not existed:
                    del buckets[window.start_ms]
                return AcceptResult(
                    accepted=False,
                    watermark_ms=wm,
                    reason=RejectReason.STORE_UNAVAILABLE,
                    retryable=True,
                )

            return AcceptResult(
                accepted=True,
                watermark_ms=wm,
                late=late,
            )

    def _record_outcome_locked(
        self,
        event: Event,
        accepted: bool,
        reason: RejectReason | None,
        late: bool,
    ) -> None:
        """记录已确认结果；同一 event_id 只记首次决定，不被后续重复覆盖。"""
        if not event.event_id or event.event_id in self._outcomes:
            return
        self._outcomes[event.event_id] = EventOutcome(
            event_id=event.event_id,
            status=OutcomeStatus.ACCEPTED if accepted else OutcomeStatus.REJECTED,
            watermark_ms=self._watermark_ms,
            late=late,
            reason=reason,
        )

    def _persist_safely_locked(self) -> None:
        """终态拒绝台账的落盘：失败不影响拒绝语义，等待后续写入自愈。"""
        try:
            self._persist_locked()
        except Exception:  # noqa: BLE001 - 落盘故障必须兜底，不能拖垮摄取
            logger.debug("rejection-outcome persist deferred until next write")

    # ---- 观测 -----------------------------------------------------------

    def query_event(self, event_id: str) -> EventOutcome | None:
        """按 event_id 查询已确认的处理结果；未处理过返回 None。"""
        with self._lock:
            return self._outcomes.get(event_id)

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
