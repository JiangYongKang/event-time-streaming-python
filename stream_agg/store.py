"""SQLite-backed durable storage.

Durability goals:

* idempotency  -- accepted event ids are stored in the same transaction as
  the aggregate update, so a redelivered or restart-replayed event is never
  counted twice;
* atomic batches -- an ingestion batch runs in a single transaction, a
  failure rolls the whole batch back (no partial results);
* recovery -- on restart the watermark, window aggregates and finalized
  outputs are read back from disk.

The store owns a reentrant lock (:attr:`tx_lock`) which the engine holds for
the duration of an atomic processing transaction.  Checkpoint connection
setting guarantees commits survive process crashes.
"""
from __future__ import annotations

import sqlite3
import threading

from stream_agg.models import Window, WindowState


class SQLiteStore:
    """Owns the SQLite connection and serializes access."""

    def __init__(self, db_path: str) -> None:
        self._path = db_path
        self.tx_lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._init_schema()

    def _init_schema(self) -> None:
        with self.tx_lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    key TEXT NOT NULL,
                    event_time INTEGER NOT NULL,
                    value INTEGER NOT NULL,
                    accepted_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS windows (
                    key TEXT NOT NULL,
                    window_start INTEGER NOT NULL,
                    window_end INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    sum_value INTEGER NOT NULL,
                    PRIMARY KEY (key, window_start)
                );
                CREATE TABLE IF NOT EXISTS outputs (
                    key TEXT NOT NULL,
                    window_start INTEGER NOT NULL,
                    window_end INTEGER NOT NULL,
                    count INTEGER NOT NULL,
                    sum_value INTEGER NOT NULL,
                    finalized_watermark INTEGER NOT NULL,
                    PRIMARY KEY (key, window_start)
                );
                """
            )

    def close(self) -> None:
        with self.tx_lock:
            self._conn.close()

    # -- transaction helpers ---------------------------------------------
    def begin(self) -> None:
        self._conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self._conn.execute("COMMIT")

    def rollback(self) -> None:
        self._conn.execute("ROLLBACK")

    # -- watermark --------------------------------------------------------
    def get_watermark(self) -> int:
        cur = self._conn.execute(
            "SELECT value FROM kv WHERE key='watermark'"
        )
        row = cur.fetchone()
        return int(row["value"]) if row is not None else 0

    def get_max_observed(self) -> int | None:
        cur = self._conn.execute(
            "SELECT value FROM kv WHERE key='max_observed'"
        )
        row = cur.fetchone()
        return int(row["value"]) if row is not None else None

    def set_watermark(self, watermark: int) -> None:
        self._conn.execute(
            "INSERT INTO kv(key, value) VALUES('watermark', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (watermark,),
        )

    def set_max_observed(self, value: int) -> None:
        self._conn.execute(
            "INSERT INTO kv(key, value) VALUES('max_observed', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (value,),
        )

    # -- idempotency ------------------------------------------------------
    def is_duplicate(self, event_id: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM events WHERE event_id=?", (event_id,)
        )
        return cur.fetchone() is not None

    def insert_event(
        self,
        event_id: str,
        key: str,
        event_time: int,
        value: int,
        accepted_at: int,
    ) -> None:
        self._conn.execute(
            "INSERT INTO events(event_id, key, event_time, value, accepted_at)"
            " VALUES(?, ?, ?, ?, ?)",
            (event_id, key, event_time, value, accepted_at),
        )

    # -- windows ----------------------------------------------------------
    @staticmethod
    def _row_to_window(row: sqlite3.Row) -> Window:
        return Window(
            window_start=int(row["window_start"]),
            window_end=int(row["window_end"]),
            key=row["key"],
            state=WindowState(row["state"]),
            count=int(row["count"]),
            sum_value=int(row["sum_value"]),
        )

    def get_window(
        self, key: str, window_start: int, window_end: int
    ) -> Window | None:
        cur = self._conn.execute(
            "SELECT * FROM windows WHERE key=? AND window_start=?",
            (key, window_start),
        )
        row = cur.fetchone()
        return self._row_to_window(row) if row is not None else None

    def upsert_window(self, window: Window) -> None:
        self._conn.execute(
            "INSERT INTO windows(key, window_start, window_end, state, count,"
            " sum_value) VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(key, window_start) DO UPDATE SET "
            "window_end=excluded.window_end, state=excluded.state, "
            "count=excluded.count, sum_value=excluded.sum_value",
            (
                window.key,
                window.window_start,
                window.window_end,
                window.state.value,
                window.count,
                window.sum_value,
            ),
        )

    def list_windows(self) -> list[Window]:
        cur = self._conn.execute(
            "SELECT * FROM windows ORDER BY window_start, key"
        )
        return [self._row_to_window(r) for r in cur.fetchall()]

    def list_outputs(self) -> list[Window]:
        cur = self._conn.execute(
            "SELECT key, window_start, window_end, count, sum_value, "
            "'CLOSED' AS state FROM outputs ORDER BY window_start, key"
        )
        return [self._row_to_window(r) for r in cur.fetchall()]

    def upsert_output(self, window: Window, finalized_watermark: int) -> None:
        self._conn.execute(
            "INSERT INTO outputs(key, window_start, window_end, count, "
            "sum_value, finalized_watermark) VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(key, window_start) DO UPDATE SET "
            "window_end=excluded.window_end, count=excluded.count, "
            "sum_value=excluded.sum_value, "
            "finalized_watermark=excluded.finalized_watermark",
            (
                window.key,
                window.window_start,
                window.window_end,
                window.count,
                window.sum_value,
                finalized_watermark,
            ),
        )

    def purge_window(self, key: str, window_start: int) -> None:
        self._conn.execute(
            "DELETE FROM windows WHERE key=? AND window_start=?",
            (key, window_start),
        )
