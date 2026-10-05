"""有界缓冲、并发摄取与可重试暂存。

* :class:`BoundedIngestQueue` 基于有界队列做入口背压：容量超限时
  ``offer`` 立即返回 False（非阻塞，不无限占用内存），由上层翻译为
  BUFFER_FULL。
* :class:`IngestService` 启动固定数量工作线程消费队列；聚合器内部用锁
  保护，同一键的并发更新不会因竞态丢失计数。
* ``submit`` 返回 :class:`AcceptResult`：为了让调用方拿到确定性的接受/
  拒绝结果，事件入队后等待该事件处理完成（按 event_id 关联 future）。
* **同一 event_id 的并发提交会被合并**：只入队一次，所有并发调用共享
  同一个处理结果（要么都算接受、要么都算重复），语义一致且只计一次。
* **可重试暂存**：暂时性失败（BUFFER_FULL / BUSY / STORE_UNAVAILABLE）
  的事件进入有界暂存区，由后台线程按固定节奏自动重试；暂存区内容随
  状态快照持久化，重启后继续重试，且因 event_id 幂等不会重复计数。
"""

import logging
import queue
import threading
from concurrent.futures import Future

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.models import (
    AcceptResult,
    Event,
    EventOutcome,
    OutcomeStatus,
    RejectReason,
)
from app.retry import RetryBuffer

logger = logging.getLogger("ets.ingest")

_SENTINEL = None


class BoundedIngestQueue:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._q: "queue.Queue[Event | None]" = queue.Queue(
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


class IngestService:
    def __init__(
        self,
        aggregator: EventTimeAggregator,
        settings: Settings,
    ) -> None:
        self.aggregator = aggregator
        self.settings = settings
        self._buffer = BoundedIngestQueue(settings)
        self._workers: list[threading.Thread] = []
        self._running = threading.Event()
        # 每个在途 event_id 关联一个共享 future：并发同 id 提交合并等待
        self._pending: dict[str, Future[AcceptResult]] = {}
        self._pending_lock = threading.Lock()
        # 暂时性失败的有界暂存区 + 后台重试线程
        self._retry_buffer = RetryBuffer(settings.max_retry_events)
        self._retry_wake = threading.Event()
        self._retry_thread: threading.Thread | None = None
        # 暂存区内容随聚合状态一起原子落盘
        self.aggregator.set_retry_provider(self._retry_buffer.events)

    # ---- 生命周期 -------------------------------------------------------

    def start(self) -> None:
        if self._running.is_set():
            return
        self._running.set()
        # 恢复重启前未重试完的事件
        restored = self.aggregator.take_loaded_retry_events()
        if restored:
            self._retry_buffer.load(restored)
            logger.info("restored %d pending retry events", len(restored))
        for i in range(max(1, self.settings.ingest_workers)):
            t = threading.Thread(
                target=self._worker_loop, name=f"ingest-{i}", daemon=True
            )
            t.start()
            self._workers.append(t)
        self._retry_thread = threading.Thread(
            target=self._retry_loop, name="ingest-retry", daemon=True
        )
        self._retry_thread.start()

    def stop(self, drain: bool = True, timeout: float = 5.0) -> None:
        """优雅停止：默认先排空队列再退出。"""
        if not self._running.is_set():
            return
        if drain:
            self._buffer._q.join()
        self._running.clear()
        self._retry_wake.set()
        for _ in self._workers:
            self._buffer._q.put_nowait(_SENTINEL)
        for t in self._workers:
            t.join(timeout=timeout)
        self._workers.clear()
        if self._retry_thread is not None:
            self._retry_thread.join(timeout=timeout)
            self._retry_thread = None
        # 停止前把暂存区内容落盘，重启后继续重试
        self.aggregator.persist()
        # 仍在等待的请求以 BUSY 结束
        with self._pending_lock:
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_result(
                        AcceptResult(
                            accepted=False,
                            watermark_ms=self.aggregator.watermark_ms,
                            reason=RejectReason.BUSY,
                            retryable=True,
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
                if not result.accepted and result.retryable:
                    # 工作线程内发生的暂时性失败（如落盘暂不可用）同样入区重试
                    self._retry_buffer.offer(event, result.reason)
                    self.aggregator.persist()
                with self._pending_lock:
                    fut = self._pending.pop(event.event_id, None)
                if fut is not None and not fut.done():
                    fut.set_result(result)
            finally:
                if event is not _SENTINEL:
                    self._buffer.task_done()

    # ---- 自动重试 -------------------------------------------------------

    def _retry_loop(self) -> None:
        interval = max(1, self.settings.retry_interval_ms) / 1000.0
        while self._running.is_set():
            self._retry_wake.wait(timeout=interval)
            self._retry_wake.clear()
            if not self._running.is_set():
                return
            self._retry_cycle()

    def _retry_cycle(self) -> None:
        # 落盘自愈：之前写盘失败时，恢复后先补一次完整快照
        if not self.aggregator.store_healthy:
            if self.aggregator.flush():
                logger.info("store recovered during retry cycle")
            else:
                logger.warning("store still unavailable; will retry next cycle")
        batch = self._retry_buffer.drain()
        if not batch:
            return
        for entry in batch:
            event = entry.event
            result = self.aggregator.add_event(event)
            if result.accepted:
                logger.info(
                    "retry accepted event_id=%s attempts=%d watermark=%d late=%s",
                    event.event_id, entry.attempts + 1,
                    result.watermark_ms, result.late,
                )
            elif result.retryable:
                logger.info(
                    "retry deferred event_id=%s reason=%s attempts=%d",
                    event.event_id, result.reason, entry.attempts + 1,
                )
                self._retry_buffer.requeue(entry)
            else:
                logger.info(
                    "retry terminal event_id=%s reason=%s attempts=%d",
                    event.event_id, result.reason, entry.attempts + 1,
                )
        # 暂存区内容在本周期发生变化，落盘一次保持一致
        self.aggregator.persist()

    # ---- 入口 -----------------------------------------------------------

    def submit(self, event: Event, timeout: float = 2.0) -> AcceptResult:
        """提交事件并等待处理结果。

        * 服务未运行 -> BUSY（可重试，进入暂存区）；
        * 有界缓冲已满 -> BUFFER_FULL（可重试，进入暂存区）；
        * 等待超时 -> BUSY；
        * 同一 event_id 并发提交只入队一次，共享同一处理结果。
        """
        if not self._running.is_set():
            return self._stage_if_retryable(
                event,
                AcceptResult(
                    accepted=False,
                    watermark_ms=self.aggregator.watermark_ms,
                    reason=RejectReason.BUSY,
                    retryable=True,
                ),
            )
        owner = False
        with self._pending_lock:
            fut = self._pending.get(event.event_id)
            if fut is None:
                fut = Future()
                self._pending[event.event_id] = fut
                owner = True
        if not owner:
            # 并发同 id 提交：合并等待同一个处理结果，不重复入队
            try:
                return fut.result(timeout=timeout)
            except TimeoutError:
                return AcceptResult(
                    accepted=False,
                    watermark_ms=self.aggregator.watermark_ms,
                    reason=RejectReason.BUSY,
                    retryable=True,
                )
        if not self._buffer.offer(event):
            result = AcceptResult(
                accepted=False,
                watermark_ms=self.aggregator.watermark_ms,
                reason=RejectReason.BUFFER_FULL,
                retryable=True,
            )
            # 唤醒共享等待者，结果一致
            if not fut.done():
                fut.set_result(result)
            with self._pending_lock:
                self._pending.pop(event.event_id, None)
            return self._stage_if_retryable(event, result)
        try:
            return fut.result(timeout=timeout)
        except TimeoutError:
            with self._pending_lock:
                self._pending.pop(event.event_id, None)
            return AcceptResult(
                accepted=False,
                watermark_ms=self.aggregator.watermark_ms,
                reason=RejectReason.BUSY,
                retryable=True,
            )

    def _stage_if_retryable(
        self, event: Event, result: AcceptResult
    ) -> AcceptResult:
        """暂时性失败放入有界暂存区，稍后自动重试；暂存区满则原样返回。"""
        if result.accepted or not result.retryable:
            return result
        if self._retry_buffer.offer(event, result.reason):
            logger.info(
                "staged for retry event_id=%s reason=%s buffer=%d/%d",
                event.event_id, result.reason,
                self._retry_buffer.size(), self.settings.max_retry_events,
            )
            self.aggregator.persist()
        else:
            logger.warning(
                "retry buffer full (%d); event_id=%s reason=%s returned "
                "to client for manual retry",
                self.settings.max_retry_events, event.event_id, result.reason,
            )
        return result

    # ---- 观测 -----------------------------------------------------------

    def query_event(self, event_id: str) -> EventOutcome | None:
        """按 event_id 查询处理结果：已确认结果优先，暂存区中为 PENDING。"""
        outcome = self.aggregator.query_event(event_id)
        if outcome is not None:
            return outcome
        if event_id in self._retry_buffer:
            return EventOutcome(
                event_id=event_id,
                status=OutcomeStatus.PENDING,
                watermark_ms=self.aggregator.watermark_ms,
            )
        return None

    def buffer_size(self) -> int:
        return self._buffer.size()

    def retry_buffer_size(self) -> int:
        return self._retry_buffer.size()
