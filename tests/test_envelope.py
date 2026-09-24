from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

from src.envelope import validate_event

ROOT = Path(__file__).parents[1]

class EnvelopeTest(unittest.TestCase):
    def test_sample_is_valid(self) -> None:
        record = json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8"))
        self.assertEqual(validate_event(record), [])

    def test_timezone_is_required(self) -> None:
        record = json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8"))
        record["occurred_at"] = "2026-09-24T10:00:00"
        self.assertIn("occurred_at 必须包含时区", validate_event(record))

    def test_cli_accepts_sample(self) -> None:
        result = subprocess.run([sys.executable, "-m", "src.cli", "data/sample.json"], cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("事件有效", result.stdout)

if __name__ == "__main__":
    unittest.main()
