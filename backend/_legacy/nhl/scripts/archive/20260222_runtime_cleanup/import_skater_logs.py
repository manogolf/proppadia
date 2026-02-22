#!/usr/bin/env python3
"""Compatibility wrapper for legacy workflow path.

Delegates to seed_skater_logs_for_date.py.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    target = Path(__file__).with_name("seed_skater_logs_for_date.py")
    cmd = [sys.executable, str(target)]
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
