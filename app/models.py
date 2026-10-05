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
    #: 状态落盘暂时失败（已回滚内存变更，稍后重试可能成功）
    STORE_UNAVAILABLE = "STORE_UNAVAILABLE"


#: 暂时性（可重试）拒绝原因：过一会儿重发很可能成功
RETRYABLE_REASONS: frozenset[RejectReason] = frozenset(
    {
        RejectReason.BUFFER_FULL,
        RejectReason.BUSY,
        RejectReason.STORE_UNAVAILABLE,
    }
)


def is_retryable(reason: RejectReason | None) -> bool:
    """判断拒绝原因是否为暂时性（重发可能成功）。"""
    return reason in RETRYABLE_REASONS


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
    #: 拒绝是否为暂时性（可安全重试）；接受时恒为 False
    retryable: bool = False
    closed_windows: list[WindowId] = field(default_factory=list)


class OutcomeStatus(str, Enum):
    """按 event_id 查询到的处理结果状态。"""

    #: 已被接受并计入窗口
    ACCEPTED = "ACCEPTED"
    #: 已被终态拒绝（重发也不会成功）
    REJECTED = "REJECTED"
    #: 在可重试暂存区中，等待自动重试
    PENDING = "PENDING"


@dataclass(frozen=True)
class EventOutcome:
    """某个 event_id 的已确认处理结果（用于按标识查询）。"""

    event_id: str
    status: OutcomeStatus
    watermark_ms: int
    late: bool = False
    reason: RejectReason | None = None

    @property
    def retryable(self) -> bool:
        return self.status == OutcomeStatus.PENDING or is_retryable(self.reason)


@dataclass(frozen=True)
class Snapshot:
    watermark_ms: int
    windows: list[WindowAggregate]
