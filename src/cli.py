"""检查本地 JSON 事件文件。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from .envelope import validate_event

def main() -> int:
    if len(sys.argv) != 2:
        print("用法：python3 -m src.cli <事件文件>", file=sys.stderr)
        return 2
    try:
        record = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"无法读取事件：{error}", file=sys.stderr)
        return 2
    errors = validate_event(record)
    if errors:
        print("；".join(errors), file=sys.stderr)
        return 1
    print(f"事件有效：{record['event_id']}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
