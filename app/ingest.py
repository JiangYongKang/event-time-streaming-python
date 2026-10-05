"""有界缓冲、并发摄取与可重试摄取。

* :class:`BoundedIngestQueue` 基于有界队列做入口背压：容量超限时
  ``offer`` 立即返回 False（非阻塞，不无限占用内存），由上层翻译为
  BUFFER_FULL。
* :class:`RetryBuffer` 是有界的重试暂存区：暂时性失败（BUFFER_FULL 等
  “过一会儿重发很可能成功”的失败）不直接丢掉，而是登记在此，由维护线程
  按固定节奏自动重试；容量有上限，满了就不再接收新的暂存。
* :class:`IngestService` 启动固定数量工作线程消费队列；聚合器内部用锁
  保护，同一键的并发更新不会因竞态丢失计数。
* 同一 event_id 的并发提交共享同一个在途 future：只入队一次，所有并发
  调用方拿到同一个结果，语义一致（同接受或同拒绝）。
* 维护线程还负责落盘自愈：聚合器处于降级状态（持久化暂时失败）时，
  每个周期尝试重新落盘，恢复后自动回到正常状态。
"""

import logging
import queue
import threading
import time
from concurrent.futures import Future

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.models import (
    AcceptResult,
    DecisionStatus,
    Event,
    PendingRetry,
    RejectReason,
    is_transient,
)

_SENTINEL = None

logger = logging.getLogger(__name__)


class BoundedIngestQueue:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._q: queue.Queue[Event | None] = queue.Queue(
            maxsize=max(1, settings.max_pending_events)
        )

    def offer(self, event: Event) -> bool:
        """非阻塞入队。满则立即返回 False（触发背压拒绝）。"""
        try:
            self._q.put_nowait(event)
            return True
        except queue.Full:
            return False

    def get(self, timeout: float | None = None) -> Event | None:
        return self._q.get(timeout=timeout)

    def task_done(self) -> None:
        self._q.task_done()

    def size(self) -> int:
        return self._q.qsize()


class RetryBuffer:
    """有界重试暂存区：按 event_id 去重，容量超限即拒绝新的暂存。"""

    def __init__(self, capacity: int) -> None:
        self._capacity = max(0, capacity)
        self._items: dict[str, PendingRetry] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._capacity > 0

    def add(self, event: Event, reason: RejectReason, delay_s: float) -> bool:
        """登记一个待重试事件；已满（且不是已有标识）时返回 False。"""
        with self._lock:
            existing = self._items.get(event.event_id)
            if existing is None and len(self._items) >= self._capacity:
                return False
            attempts = existing.attempts if existing is not None else 0
            self._items[event.event_id] = PendingRetry(
                event=event,
                reason=reason,
                attempts=attempts,
                not_before=time.monotonic() + delay_s,
            )
            return True

    def due(self, limit: int) -> list[PendingRetry]:
        """取出已到重试时刻的事件（按入档顺序，最多 limit 条）。"""
        now = time.monotonic()
        with self._lock:
            ready = [p for p in self._items.values() if p.not_before <= now]
        return ready[:limit]

    def reschedule(self, pending: PendingRetry, delay_s: float) -> None:
        """重试仍是暂时性失败：增加尝试次数并推迟下次重试时刻。"""
        with self._lock:
            if pending.event.event_id not in self._items:
                return
            self._items[pending.event.event_id] = PendingRetry(
                event=pending.event,
                reason=pending.reason,
                attempts=pending.attempts + 1,
                not_before=time.monotonic() + delay_s,
            )

    def remove(self, event_id: str) -> None:
        with self._lock:
            self._items.pop(event_id, None)

    def size(self) -> int:
        with self._lock:
            return len(self._items)


class IngestService:
    def __init__(
        self,
        aggregator: EventTimeAggregator,
        settings: Settings,
    ) -> None:
        self.aggregator = aggregator
        self.settings = settings
        self._buffer = BoundedIngestQueue(settings)
        self._retry_buffer = RetryBuffer(settings.max_retry_events)
        self._workers: list[threading.Thread] = []
        self._maintenance: threading.Thread | None = None
        self._running = threading.Event()
        self._stopped = threading.Event()
        # 每个在途事件关联一个 future，等待聚合器处理结果；
        # 同一 event_id 的并发提交共享同一个 future（只在途一次）
        self._pending: dict[str, Future[AcceptResult]] = {}
        self._pending_lock = threading.Lock()

    # ---- 生命周期 -------------------------------------------------------

    def start(self) -> None:
        if self._running.is_set():
            return
        self._running.set()
        self._stopped.clear()
        # 重启恢复：把上次持久化的待重试事件重新装入暂存区
        for pending in self.aggregator.pending_retries():
            self._retry_buffer.add(
                pending.event, pending.reason, delay_s=0.0
            )
        for i in range(max(1, self.settings.ingest_workers)):
            t = threading.Thread(
                target=self._worker_loop, name=f"ingest-{i}", daemon=True
            )
            t.start()
            self._workers.append(t)
        self._maintenance = threading.Thread(
            target=self._maintenance_loop, name="ingest-maintenance", daemon=True
        )
        self._maintenance.start()

    def stop(self, drain: bool = True, timeout: float = 5.0) -> None:
        """优雅停止：先停维护线程（不再向队列补充重试事件），再排空队列。"""
        if not self._running.is_set():
            return
        # 先让维护线程退出，否则排空期间它会持续把暂存事件重新入队，
        # 导致 queue.join() 永远等不到 unfinished_tasks == 0
        self._running.clear()
        self._stopped.set()
        if self._maintenance is not None:
            self._maintenance.join(timeout=timeout)
            self._maintenance = None
        if drain:
            self._buffer._q.join()
        for _ in self._workers:
            self._buffer._q.put_nowait(_SENTINEL)
        for t in self._workers:
            t.join(timeout=timeout)
        self._workers.clear()
        # 仍在等待的请求以 BUSY 结束
        with self._pending_lock:
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_result(
                        AcceptResult(
                            accepted=False,
                            watermark_ms=self.aggregator.watermark_ms,
                            reason=RejectReason.BUSY,
                        )
                    )
            self._pending.clear()

    # ---- 工作线程 -------------------------------------------------------

    def _worker_loop(self) -> None:
        while True:
            event = self._buffer.get()
            try:
                if event is _SENTINEL:
                    return
                result = self.aggregator.add_event(event)
                with self._pending_lock:
                    fut = self._pending.pop(event.event_id, None)
                if fut is not None and not fut.done():
                    fut.set_result(result)
            finally:
                if event is not _SENTINEL:
                    self._buffer.task_done()

    # ---- 维护线程：有节奏地自动重试 + 落盘自愈 ---------------------------

    def _retry_interval_s(self) -> float:
        return max(0.01, self.settings.retry_interval_ms / 1000.0)

    def _maintenance_loop(self) -> None:
        interval = self._retry_interval_s()
        # _running 清除后 wait 立即返回 True，循环退出
        while not self._stopped.wait(interval):
            self._maintenance_tick()

    def _maintenance_tick(self) -> None:
        # 1) 落盘自愈：持久化曾失败时，每个周期尝试恢复
        if self.aggregator.storage_degraded:
            healed = self.aggregator.flush()
            logger.info(
                "storage self-heal tick: degraded=%s", not healed
            )
        # 2) 可重试摄取：按节奏处理到期的暂存事件
        if not self._retry_buffer.enabled:
            return
        for pending in self._retry_buffer.due(limit=8):
            if not self._running.is_set():
                return
            self._retry_one(pending)

    def _finalize_if_terminal(self, event_id: str, attempts: int) -> bool:
        """若台账中已有终态结果，则收尾暂存记录并返回 True。

        上一次重试可能超时（BUSY）但事件其实已被工作线程处理，
        台账是判断“是否已有终态”的唯一依据，避免盲目重复入队。
        """
        decision = self.aggregator.query_event(event_id)
        if decision is None or decision.status is DecisionStatus.PENDING:
            return False
        self._retry_buffer.remove(event_id)
        self.aggregator.clear_pending(event_id)
        logger.info(
            "retry finalized event=%s attempts=%d status=%s reason=%s",
            event_id, attempts, decision.status, decision.reason,
        )
        return True

    def _retry_one(self, pending: PendingRetry) -> None:
        event = pending.event
        # 先查台账：之前的尝试可能已得出终态，无需再次入队
        if self._finalize_if_terminal(event.event_id, pending.attempts):
            return
        result = self._offer_and_wait(
            event, timeout=max(2 * self._retry_interval_s(), 0.1)
        )
        if result.accepted or not is_transient(result.reason):
            # 终态（接受 / 重复 / 超宽限迟到等）：移出暂存区。
            # 终态决策已由聚合器写入台账，重试不会重复计数。
            self._retry_buffer.remove(event.event_id)
            self.aggregator.clear_pending(event.event_id)
            logger.info(
                "retry finalized event=%s attempts=%d accepted=%s reason=%s",
                event.event_id, pending.attempts + 1,
                result.accepted, result.reason,
            )
        else:
            # 仍是暂时性失败：事件可能已在队列中被处理，先查台账再决定
            if self._finalize_if_terminal(event.event_id, pending.attempts + 1):
                return
            self._retry_buffer.reschedule(pending, self._retry_interval_s())
            logger.info(
                "retry still transient event=%s attempts=%d reason=%s",
                event.event_id, pending.attempts + 1, result.reason,
            )

    # ---- 入口 -----------------------------------------------------------

    def _offer_and_wait(self, event: Event, timeout: float) -> AcceptResult:
        """入队并等待处理结果；同一 event_id 在途时共享同一个 future。"""
        fut: Future[AcceptResult] = Future()
        owner = False
        with self._pending_lock:
            existing = self._pending.get(event.event_id)
            if existing is not None:
                # 同一标识已在处理中：不重复入队，共享同一个结果
                fut = existing
            else:
                self._pending[event.event_id] = fut
                owner = True
        if owner and not self._buffer.offer(event):
            with self._pending_lock:
                self._pending.pop(event.event_id, None)
            return AcceptResult(
                accepted=False,
                watermark_ms=self.aggregator.watermark_ms,
                reason=RejectReason.BUFFER_FULL,
            )
        try:
            return fut.result(timeout=timeout)
        except TimeoutError:
            if owner:
                # 事件可能仍在队列中，稍后会由工作线程处理并写入台账；
                # 调用方可按 event_id 查询最终结果
                with self._pending_lock:
                    self._pending.pop(event.event_id, None)
            return AcceptResult(
                accepted=False,
                watermark_ms=self.aggregator.watermark_ms,
                reason=RejectReason.BUSY,
            )

    def submit(self, event: Event, timeout: float = 2.0) -> AcceptResult:
        """提交事件并等待处理结果。

        * 服务未运行 -> BUSY；
        * 有界缓冲已满 -> BUFFER_FULL；若开启了可重试摄取，则登记到
          重试暂存区（``retry_scheduled=True``），稍后自动重试；
        * 等待超时 -> BUSY（事件可能已被接受，可按 event_id 查询）。
        """
        if not self._running.is_set():
            return AcceptResult(
                accepted=False,
                watermark_ms=self.aggregator.watermark_ms,
                reason=RejectReason.BUSY,
            )
        result = self._offer_and_wait(event, timeout)
        if (
            result.reason == RejectReason.BUFFER_FULL
            and self._retry_buffer.enabled
        ):
            if self._retry_buffer.add(
                event, result.reason, delay_s=self._retry_interval_s()
            ):
                self.aggregator.mark_pending(event, result.reason)
                logger.info(
                    "event=%s buffered for retry reason=%s retry_buffer=%d",
                    event.event_id, result.reason, self._retry_buffer.size(),
                )
                return AcceptResult(
                    accepted=False,
                    watermark_ms=result.watermark_ms,
                    reason=result.reason,
                    retry_scheduled=True,
                )
            logger.warning(
                "retry buffer full (%d), event=%s rejected reason=%s",
                self._retry_buffer.size(), event.event_id, result.reason,
            )
        return result

    def buffer_size(self) -> int:
        return self._buffer.size()

    def retry_buffer_size(self) -> int:
        return self._retry_buffer.size()
