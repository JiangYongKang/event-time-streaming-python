"""Shared pytest fixtures and logging helpers.

Each test gets an isolated temp-file database; the engine worker is started
and stopped around the test so recovery scenarios can reopen the same file.
"""
from __future__ import annotations

import logging
import os
import tempfile

import pytest

from stream_agg.config import Config
from stream_agg.engine import StreamEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOG = logging.getLogger("test")


@pytest.fixture
def db_path():
    fd, path = tempfile.mkstemp(prefix="stream-agg-", suffix=".db")
    os.close(fd)
    os.remove(path)  # engine creates it; tests need the path only
    yield path
    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass


@pytest.fixture
def config(db_path):
    return Config(
        db_path=db_path,
        window_size_ms=10_000,
        allowed_lateness_ms=5_000,
        max_out_of_orderness_ms=2_000,
        queue_capacity=8,
        processing_timeout_ms=3_000.0,
    )


@pytest.fixture
def engine(config):
    eng = StreamEngine(config)
    eng.start()
    try:
        yield eng
    finally:
        eng.stop()


@pytest.fixture
def make_engine(db_path):
    """Factory for engines with per-test config overrides (same db file)."""
    started: list[StreamEngine] = []

    def _make(**overrides):
        defaults = dict(
            db_path=db_path,
            window_size_ms=10_000,
            allowed_lateness_ms=5_000,
            max_out_of_orderness_ms=2_000,
            queue_capacity=8,
            processing_timeout_ms=3_000.0,
        )
        defaults.update(overrides)
        eng = StreamEngine(Config(**defaults))
        eng.start()
        started.append(eng)
        return eng

    yield _make
    for eng in started:
        eng.stop()


def dump_state(eng: StreamEngine, title: str) -> None:
    """Print watermark, open windows and finalized outputs for the log."""
    LOG.info("----- %s -----", title)
    LOG.info("watermark=%d stats=%s", eng.watermark, eng.queue_stats())
    for w in eng.windows():
        LOG.info(
            "WINDOW %s key=%s interval=[%d,%d) count=%d sum=%d",
            w.state.value,
            w.key,
            w.window_start,
            w.window_end,
            w.count,
            w.sum_value,
        )
    for w in eng.outputs():
        LOG.info(
            "OUTPUT CLOSED key=%s interval=[%d,%d) count=%d sum=%d",
            w.key,
            w.window_start,
            w.window_end,
            w.count,
            w.sum_value,
        )
