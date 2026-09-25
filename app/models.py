"""领域模型与拒绝原因。

事件以毫秒级事件时间（event_time_ms）表示；窗口为左闭右开区间
``[start_ms, end_ms)``。
"""

from dataclasses import dataclass, field
from enum import Enum


class RejectReason(str, Enum):
    """事件被拒绝的可区分原因。"""

    #: 迟到且超出允许迟到范围，目标窗口已关闭
    LATE_EVENT = "LATE_EVENT"
    #: 同一 event_id 重复投递，幂等忽略
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    #: 有界缓冲已满，触发背压拒绝
    BUFFER_FULL = "BUFFER_FULL"
    #: 服务正在停止/工作线程不可用
    BUSY = "BUSY"
    #: 事件字段非法（缺 id、时间为负等）
    INVALID = "INVALID"


@dataclass(frozen=True)
class Event:
    event_id: str
    key: str
    event_time_ms: int
    value: int = 1


@dataclass(frozen=True)
class WindowId:
    key: str
    start_ms: int
    end_ms: int

    def interval(self) -> str:
        return f"[{self.start_ms}, {self.end_ms})"


@dataclass
class WindowAggregate:
    window: WindowId
    count: int = 0
    total: int = 0
    closed: bool = False
    event_ids: set[str] = field(default_factory=set)


@dataclass(frozen=True)
class AcceptResult:
    accepted: bool
    watermark_ms: int
    #: 被接受但落在当前水位线之后（迟到但仍在宽限内）
    late: bool = False
    reason: RejectReason | None = None
    closed_windows: list[WindowId] = field(default_factory=list)


@dataclass(frozen=True)
class Snapshot:
    watermark_ms: int
    windows: list[WindowAggregate]
