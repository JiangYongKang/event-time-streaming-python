"""可重试暂存区（有界）。

对暂时性失败（BUFFER_FULL / BUSY / STORE_UNAVAILABLE 等“过一会儿重发
很可能成功”的情况）的事件，先放入本暂存区兜住，由摄取层按固定节奏
自动重试；终态拒绝（LATE_EVENT / DUPLICATE_EVENT / INVALID）不会进入。

保证：
* 有界：容量 ``max_retry_events``，满时 ``offer`` 返回 False，由调用方
  把暂时性拒绝原样返回客户端（不无限占内存）；
* 幂等：同一 ``event_id`` 在暂存区中至多一份，重复入区被忽略；
* 可恢复：内容随聚合状态快照一起原子持久化，重启后继续自动重试。
"""

import threading
from collections import deque
from dataclasses import dataclass

from app.models import Event, RejectReason


@dataclass
class RetryEntry:
    event: Event
    last_reason: RejectReason
    attempts: int = 0


class RetryBuffer:
    def __init__(self, capacity: int) -> None:
        self._capacity = max(1, capacity)
        self._items: deque[RetryEntry] = deque()
        self._ids: set[str] = set()
        self._lock = threading.Lock()

    def offer(self, event: Event, reason: RejectReason) -> bool:
        """入区。满或同 id 已在区中时返回 False（不抛异常、不阻塞）。"""
        with self._lock:
            if event.event_id in self._ids:
                return True  # 已在区中，幂等视为成功
            if len(self._items) >= self._capacity:
                return False
            self._items.append(RetryEntry(event=event, last_reason=reason))
            self._ids.add(event.event_id)
            return True

    def drain(self) -> list[RetryEntry]:
        """取出当前全部待重试项（本轮重试失败后需用 ``requeue`` 放回）。"""
        with self._lock:
            batch = list(self._items)
            self._items.clear()
            self._ids.clear()
            return batch

    def requeue(self, entry: RetryEntry) -> bool:
        """重试仍未成功，把原条目放回区尾（保持有界与幂等）。"""
        entry.attempts += 1
        with self._lock:
            if entry.event.event_id in self._ids:
                return True
            if len(self._items) >= self._capacity:
                return False
            self._items.append(entry)
            self._ids.add(entry.event.event_id)
            return True

    def remove(self, event_id: str) -> None:
        with self._lock:
            if event_id not in self._ids:
                return
            self._ids.discard(event_id)
            self._items = deque(
                item for item in self._items if item.event.event_id != event_id
            )

    def load(self, events: list[Event]) -> None:
        """重启恢复：把上次快照中的待重试事件重新入区。"""
        for event in events:
            self.offer(event, RejectReason.BUSY)

    def events(self) -> list[Event]:
        with self._lock:
            return [item.event for item in self._items]

    def __contains__(self, event_id: str) -> bool:
        with self._lock:
            return event_id in self._ids

    def size(self) -> int:
        with self._lock:
            return len(self._items)
