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


#: 暂时性失败（过一会儿重发很可能成功），可进入重试暂存区
TRANSIENT_REASONS: frozenset[RejectReason] = frozenset(
    {RejectReason.BUFFER_FULL, RejectReason.BUSY}
)


def is_transient(reason: RejectReason | None) -> bool:
    """判断拒绝原因是否为暂时性失败（可重试）。"""
    return reason in TRANSIENT_REASONS


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
    #: 本次结果为暂时性失败且已进入重试暂存区，稍后将自动重试
    retry_scheduled: bool = False


@dataclass(frozen=True)
class Snapshot:
    watermark_ms: int
    windows: list[WindowAggregate]


class DecisionStatus(str, Enum):
    """按事件标识查询到的处理结果状态。"""

    #: 已接受并计入窗口
    ACCEPTED = "ACCEPTED"
    #: 终态拒绝（重发也不会成功，如超宽限迟到、字段非法）
    REJECTED = "REJECTED"
    #: 暂时性失败，已放入重试暂存区，等待自动重试
    PENDING = "PENDING"


@dataclass(frozen=True)
class EventDecision:
    """某个 event_id 的（首个）确定性处理结果。

    客户端超时/失败后可按 event_id 查询该记录，决定是否可以安全重发。
    重复投递不会覆盖已记录的终态结果。
    """

    event_id: str
    status: DecisionStatus
    reason: RejectReason | None = None
    late: bool = False
    watermark_ms: int = -1


@dataclass(frozen=True)
class PendingRetry:
    """重试暂存区中的一条记录（暂时性失败、等待自动重试的事件）。"""

    event: Event
    reason: RejectReason
    attempts: int = 0
    #: 单调时钟（time.monotonic）上的下次可重试时刻；持久化时不保存
    not_before: float = 0.0
