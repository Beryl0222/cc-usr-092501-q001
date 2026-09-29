"""只增事件存储（SQLite，标准库）。

所有状态变化只能以事件形式追加；重启服务时从事件流完整回放，
未完成点交、待复核封签、冻结与保管状态均随之恢复。
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from .events import Event

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    event_type     TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id   TEXT NOT NULL,
    occurred_at    TEXT NOT NULL,
    version        INTEGER NOT NULL,
    actor          TEXT NOT NULL,
    summary        TEXT NOT NULL,
    payload        TEXT NOT NULL,
    causation_id   TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_aggregate
    ON events (aggregate_type, aggregate_id, seq);
"""


class DuplicateEvent(Exception):
    """event_id 已存在（确定性重放，例如完全相同的封签回执重传）。"""


class EventStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def append_all(self, events: Iterable[Event]) -> None:
        """在单个事务内原子追加一批事件；任一 event_id 重复则整批回滚。"""
        rows = [
            (
                e.event_id,
                e.event_type,
                e.aggregate_type,
                e.aggregate_id,
                e.occurred_at,
                e.version,
                e.actor,
                e.summary,
                json.dumps(e.payload, ensure_ascii=False, sort_keys=True),
                e.causation_id,
            )
            for e in events
        ]
        try:
            with self._conn:  # 自动 BEGIN/COMMIT，异常回滚
                self._conn.executemany(
                    "INSERT INTO events (event_id, event_type, aggregate_type,"
                    " aggregate_id, occurred_at, version, actor, summary, payload,"
                    " causation_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )
        except sqlite3.IntegrityError as error:
            raise DuplicateEvent(str(error)) from error

    def append(self, event: Event) -> None:
        self.append_all([event])

    def all_events(self) -> list[Event]:
        rows = self._conn.execute(
            "SELECT * FROM events ORDER BY seq"
        ).fetchall()
        return [self._from_row(row) for row in rows]

    def events_for(self, aggregate_type: str, aggregate_id: str) -> list[Event]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE aggregate_type=? AND aggregate_id=?"
            " ORDER BY seq",
            (aggregate_type, aggregate_id),
        ).fetchall()
        return [self._from_row(row) for row in rows]

    def find_event(self, event_id: str) -> Event | None:
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        return self._from_row(row) if row else None

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _from_row(row: sqlite3.Row) -> Event:
        return Event(
            event_id=row["event_id"],
            event_type=row["event_type"],
            aggregate_type=row["aggregate_type"],
            aggregate_id=row["aggregate_id"],
            occurred_at=row["occurred_at"],
            version=row["version"],
            actor=row["actor"],
            summary=row["summary"],
            payload=json.loads(row["payload"]),
            causation_id=row["causation_id"],
        )
