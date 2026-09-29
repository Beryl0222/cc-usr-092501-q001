"""命令入口：校验本地 JSON 事件文件，或启动入藏 HTTP 服务。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from .envelope import validate_event


def check_file(path: str) -> int:
    try:
        record = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"无法读取事件：{error}", file=sys.stderr)
        return 2
    errors = validate_event(record)
    if errors:
        print("；".join(errors), file=sys.stderr)
        return 1
    print(f"事件有效：{record['event_id']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)

    if args and args[0] == "serve":
        import argparse

        parser = argparse.ArgumentParser(prog="src.cli serve", description="启动入藏 HTTP 服务")
        parser.add_argument("--host", default="127.0.0.1")
        parser.add_argument("--port", type=int, default=8080)
        parser.add_argument("--store", default="data/events.jsonl", help="事件日志 JSONL 路径")
        parsed = parser.parse_args(args[1:])
        from .httpapi import serve

        serve(host=parsed.host, port=parsed.port, store_path=parsed.store)
        return 0

    if len(args) == 1:
        return check_file(args[0])

    print("用法：python3 -m src.cli <事件文件> | python3 -m src.cli serve [--host H] [--port P] [--store PATH]",
          file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
