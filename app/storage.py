"""持久化存储：全量快照 + 临时文件原子替换。

每次保存都把完整状态写入同目录临时文件，再通过 ``os.replace`` 原子改名。
这样即使写入中途进程崩溃，也只会留下“旧快照”或“完整新快照”，
不会出现部分写入的损坏状态。

快照内容（单文件、单次原子替换，保证要么全生效要么全不生效）：

* 水位线、窗口聚合、已见事件 ID（原有）；
* 按 event_id 的已确认处理结果（``outcomes``，用于查询与安全重试）；
* 可重试暂存区中的待重试事件（``retry_events``，重启后继续自动重试）。
"""

import json
import os
import tempfile
from dataclasses import dataclass, field

from app.models import (
    Event,
    EventOutcome,
    OutcomeStatus,
    RejectReason,
    WindowAggregate,
    WindowId,
)


@dataclass
class LoadedState:
    """``StateStore.load`` 的返回载体。"""

    windows: dict[str, dict[int, WindowAggregate]] = field(default_factory=dict)
    watermark_ms: int = -1
    seen_ids: set[str] = field(default_factory=set)
    outcomes: dict[str, EventOutcome] = field(default_factory=dict)
    retry_events: list[Event] = field(default_factory=list)


def _outcome_to_json(o: EventOutcome) -> dict:
    return {
        "event_id": o.event_id,
        "status": o.status.value,
        "watermark_ms": o.watermark_ms,
        "late": o.late,
        "reason": o.reason.value if o.reason else None,
    }


def _outcome_from_json(raw: dict) -> EventOutcome:
    reason = raw.get("reason")
    return EventOutcome(
        event_id=raw["event_id"],
        status=OutcomeStatus(raw["status"]),
        watermark_ms=int(raw.get("watermark_ms", -1)),
        late=bool(raw.get("late", False)),
        reason=RejectReason(reason) if reason else None,
    )


def _event_to_json(e: Event) -> dict:
    return {
        "event_id": e.event_id,
        "key": e.key,
        "event_time_ms": e.event_time_ms,
        "value": e.value,
    }


def _event_from_json(raw: dict) -> Event:
    return Event(
        event_id=raw["event_id"],
        key=raw["key"],
        event_time_ms=int(raw["event_time_ms"]),
        value=int(raw.get("value", 1)),
    )


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def save(
        self,
        windows: dict[str, dict[int, WindowAggregate]],
        watermark_ms: int,
        seen_ids: set[str],
        outcomes: dict[str, EventOutcome] | None = None,
        retry_events: list[Event] | None = None,
    ) -> None:
        payload = {
            "watermark_ms": watermark_ms,
            "seen_ids": sorted(seen_ids),
            "windows": {
                key: [
                    {
                        "key": agg.window.key,
                        "start_ms": agg.window.start_ms,
                        "end_ms": agg.window.end_ms,
                        "count": agg.count,
                        "total": agg.total,
                        "closed": agg.closed,
                        "event_ids": sorted(agg.event_ids),
                    }
                    for agg in sorted(buckets.values(), key=lambda a: a.window.start_ms)
                ]
                for key, buckets in sorted(windows.items())
            },
            "outcomes": [
                _outcome_to_json(o)
                for _, o in sorted((outcomes or {}).items())
            ],
            "retry_events": [_event_to_json(e) for e in (retry_events or [])],
        }
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".state-", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.path)
        except BaseException:
            # 写入失败时清理临时文件，磁盘上仍是上一份完整快照
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    def load(self) -> LoadedState:
        state = LoadedState()
        if not os.path.exists(self.path):
            return state
        with open(self.path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        for key, raw_list in payload.get("windows", {}).items():
            buckets: dict[int, WindowAggregate] = {}
            for item in raw_list:
                wid = WindowId(
                    key=item["key"],
                    start_ms=item["start_ms"],
                    end_ms=item["end_ms"],
                )
                buckets[wid.start_ms] = WindowAggregate(
                    window=wid,
                    count=item["count"],
                    total=item["total"],
                    closed=item["closed"],
                    event_ids=set(item.get("event_ids", [])),
                )
            state.windows[key] = buckets
        state.seen_ids = set(payload.get("seen_ids", []))
        state.watermark_ms = int(payload.get("watermark_ms", -1))
        # 旧版本快照没有 outcomes / retry_events 字段，缺省为空（向后兼容）
        state.outcomes = {
            o.event_id: o
            for o in (
                _outcome_from_json(raw) for raw in payload.get("outcomes", [])
            )
        }
        state.retry_events = [
            _event_from_json(raw) for raw in payload.get("retry_events", [])
        ]
        return state
