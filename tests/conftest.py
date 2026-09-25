"""pytest 公共夹具与日志辅助。"""

import logging

import pytest

from app.config import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def log_line(message: str) -> None:
    """统一打印测试过程中的水位线 / 窗口区间 / 拒绝原因。"""
    logging.info(message)


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        window_size_ms=1000,
        allowed_lateness_ms=500,
        max_pending_events=100,
        ingest_workers=2,
        state_path=str(tmp_path / "state.json"),
    )
