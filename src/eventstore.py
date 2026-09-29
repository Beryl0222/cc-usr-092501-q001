"""仅追加事件存储与服务重启后的重放。

事件按全局序号写入 JSONL 文件，每条事件同时携带其聚合的单调版本号。
进程重启时直接重放整个日志即可还原责任链状态；未完成点交与待复核
封签不依赖额外的内存结构。
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class DomainError(Exception):
    """领域规则被违反（并发冲突、状态不允许、权限不足等）。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def now_iso() -> str:
    """当前 UTC 时间，带时区的 ISO 8601 字符串。"""

    return datetime.now(timezone.utc).isoformat()


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise DomainError("INVALID_TIME", "时间必须包含时区")
    return parsed


class EventStore:
    """线程安全的 JSONL 事件存储。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._seq = 0
        self._versions: dict[tuple[str, str], int] = {}

    # ---- 读取与重放 -------------------------------------------------

    def load(self) -> list[dict[str, Any]]:
        """读取并返回日志中全部事件，同时校准序号与聚合版本。"""

        events: list[dict[str, Any]] = []
        with self._lock:
            self._seq = 0
            self._versions = {}
            if not self.path.exists():
                return events
            with self.path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    event = json.loads(line)
                    events.append(event)
                    self._seq = max(self._seq, int(event["seq"]))
                    key = (event["aggregate_type"], event["aggregate_id"])
                    self._versions[key] = int(event["version"])
            return events

    def replay(self, fold: Callable[[dict[str, Any]], None]) -> None:
        for event in self.load():
            fold(event)

    # ---- 写入 -------------------------------------------------------

    def next_seq(self) -> int:
        with self._lock:
            return self._seq + 1

    def append(
        self,
        events: Iterable[tuple[str, str, str, dict[str, Any]]],
        *,
        expected_versions: dict[tuple[str, str], int] | None = None,
    ) -> list[dict[str, Any]]:
        """原子追加一组事件。

        每个元组为 ``(event_type, aggregate_type, aggregate_id, payload)``。
        ``expected_versions`` 声明写入前各聚合的当前版本，构成乐观并发
        控制：实体转库与责任人变更借此保证同一时刻只有一个有效保管方。
        组内事件整体成功或整体失败（失败时不落盘任何一条）。
        """

        with self._lock:
            expected = expected_versions or {}
            for key, expected_version in expected.items():
                current = self._versions.get(key, 0)
                if current != expected_version:
                    raise DomainError(
                        "CONCURRENT_MODIFICATION",
                        f"聚合 {key[1]} 已被其他操作修改（当前版本 {current}，期望 {expected_version}）",
                    )

            pending: list[dict[str, Any]] = []
            local_versions = dict(self._versions)
            stamp = now_iso()
            seq = self._seq
            for event_type, aggregate_type, aggregate_id, payload in events:
                key = (aggregate_type, aggregate_id)
                version = local_versions.get(key, 0) + 1
                seq += 1
                pending.append(
                    {
                        "seq": seq,
                        "event_id": f"{aggregate_id}:{event_type}:{version}",
                        "event_type": event_type,
                        "aggregate_type": aggregate_type,
                        "aggregate_id": aggregate_id,
                        "version": version,
                        "occurred_at": stamp,
                        "summary": payload.pop("__summary__", event_type),
                        "payload": payload,
                    }
                )
                local_versions[key] = version

            with self.path.open("a", encoding="utf-8") as handle:
                for event in pending:
                    handle.write(json.dumps(event, ensure_ascii=False) + "\n")
                handle.flush()
                import os

                os.fsync(handle.fileno())

            self._seq = seq
            self._versions = local_versions
            return pending

    def version_of(self, aggregate_type: str, aggregate_id: str) -> int:
        with self._lock:
            return self._versions.get((aggregate_type, aggregate_id), 0)
