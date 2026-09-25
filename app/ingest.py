"""有界缓冲与并发摄取。

* :class:`BoundedIngestQueue` 基于有界队列做入口背压：容量超限时
  ``offer`` 立即返回 False（非阻塞，不无限占用内存），由上层翻译为
  BUFFER_FULL。
* :class:`IngestService` 启动固定数量工作线程消费队列；聚合器内部用锁
  保护，同一键的并发更新不会因竞态丢失计数。
* ``submit`` 返回 :class:`AcceptResult`：为了让调用方拿到确定性的接受/
  拒绝结果，事件入队后等待该事件处理完成（按 event_id 关联 future）。
"""

import queue
import threading
from concurrent.futures import Future

from app.aggregator import EventTimeAggregator
from app.config import Settings
from app.models import AcceptResult, Event, RejectReason

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
        # 每个入队事件关联一个 future，等待聚合器处理结果
        self._pending: dict[str, Future[AcceptResult]] = {}
        self._pending_lock = threading.Lock()

    # ---- 生命周期 -------------------------------------------------------

    def start(self) -> None:
        if self._running.is_set():
            return
        self._running.set()
        for i in range(max(1, self.settings.ingest_workers)):
            t = threading.Thread(
                target=self._worker_loop, name=f"ingest-{i}", daemon=True
            )
            t.start()
            self._workers.append(t)

    def stop(self, drain: bool = True, timeout: float = 5.0) -> None:
        """优雅停止：默认先排空队列再退出。"""
        if not self._running.is_set():
            return
        if drain:
            self._buffer._q.join()
        self._running.clear()
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

    # ---- 入口 -----------------------------------------------------------

    def submit(self, event: Event, timeout: float = 2.0) -> AcceptResult:
        """提交事件并等待处理结果。

        * 服务未运行 -> BUSY；
        * 有界缓冲已满 -> BUFFER_FULL（立即拒绝，背压）；
        * 等待超时 -> BUSY。
        """
        if not self._running.is_set():
            return AcceptResult(
                accepted=False,
                watermark_ms=self.aggregator.watermark_ms,
                reason=RejectReason.BUSY,
            )
        fut: Future[AcceptResult] = Future()
        with self._pending_lock:
            self._pending[event.event_id] = fut
        if not self._buffer.offer(event):
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
            with self._pending_lock:
                self._pending.pop(event.event_id, None)
            return AcceptResult(
                accepted=False,
                watermark_ms=self.aggregator.watermark_ms,
                reason=RejectReason.BUSY,
            )

    def buffer_size(self) -> int:
        return self._buffer.size()
