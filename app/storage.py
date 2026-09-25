"""持久化存储：全量快照 + 临时文件原子替换。

每次保存都把完整状态写入同目录临时文件，再通过 ``os.replace`` 原子改名。
这样即使写入中途进程崩溃，也只会留下“旧快照”或“完整新快照”，
不会出现部分写入的损坏状态。
"""

import json
import os
import tempfile

from app.models import WindowAggregate, WindowId


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def save(
        self,
        windows: dict[str, dict[int, WindowAggregate]],
        watermark_ms: int,
        seen_ids: set[str],
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

    def load(
        self,
    ) -> tuple[dict[str, dict[int, WindowAggregate]], int, set[str]]:
        if not os.path.exists(self.path):
            return {}, -1, set()
        with open(self.path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        windows: dict[str, dict[int, WindowAggregate]] = {}
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
            windows[key] = buckets
        seen_ids = set(payload.get("seen_ids", []))
        watermark_ms = int(payload.get("watermark_ms", -1))
        return windows, watermark_ms, seen_ids
