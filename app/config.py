"""服务配置。

所有配置项均可通过环境变量覆盖，命名规则为 ``ETS_`` 前缀 + 大写字段名，
例如 ``ETS_WINDOW_SIZE_MS``。
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    #: 窗口长度（毫秒），事件时间按该长度切分滚动窗口
    window_size_ms: int = 1000
    #: 允许迟到时间（毫秒）：水位线越过窗口结束时间多久之内仍接受迟到事件
    allowed_lateness_ms: int = 500
    #: 摄取队列最大容量，超过后新事件以 BUFFER_FULL 被拒绝（背压）
    max_pending_events: int = 1000
    #: 后台摄取工作线程数
    ingest_workers: int = 2
    #: 聚合状态持久化文件路径（重启恢复用）
    state_path: str = "data/state.json"
    #: 可重试暂存区容量上限（有界，超过后暂时性失败直接返回给客户端）
    max_retry_events: int = 500
    #: 暂存区自动重试的间隔（毫秒），控制重试节奏
    retry_interval_ms: int = 200


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    value = int(raw)
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")
    return value


def get_settings() -> Settings:
    """从环境变量读取配置；缺省时使用默认值。"""
    return Settings(
        window_size_ms=_env_int("ETS_WINDOW_SIZE_MS", 1000),
        allowed_lateness_ms=_env_int("ETS_ALLOWED_LATENESS_MS", 500),
        max_pending_events=_env_int("ETS_MAX_PENDING_EVENTS", 1000),
        ingest_workers=_env_int("ETS_INGEST_WORKERS", 2),
        state_path=os.environ.get("ETS_STATE_PATH", "data/state.json"),
        max_retry_events=_env_int("ETS_MAX_RETRY_EVENTS", 500),
        retry_interval_ms=_env_int("ETS_RETRY_INTERVAL_MS", 200),
    )
