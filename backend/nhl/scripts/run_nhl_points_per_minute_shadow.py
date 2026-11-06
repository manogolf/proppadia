#!/usr/bin/env python3
"""Score the Points per-minute shadow for every Points capture of a slate (P2.46).

Each capture is scored once; captures already scored are skipped. Runs as a
non-blocking step of each scheduled daily run; a date range rescoring past
captures is retrospective (inputs are the captures' own point-in-time files).

    ./bin/python backend/nhl/scripts/run_nhl_points_per_minute_shadow.py --slate-date 2026-11-06
    ./bin/python backend/nhl/scripts/run_nhl_points_per_minute_shadow.py --start 2026-11-01 --end 2026-11-05
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_per_minute_shadow as shadow  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--slate-date")
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()
    if args.start and args.end:
        days = pd.date_range(args.start, args.end).strftime("%Y-%m-%d").tolist()
    else:
        days = [args.slate_date or datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")]
    results = []
    for day in days:
        for cap in shadow.captures_for(day):
            try:
                results.append({"slate_date": day, **shadow.score_capture(cap)})
            except Exception as error:  # one bad capture never stops the others
                results.append({"slate_date": day, "status": "ERROR", "path": str(cap),
                                "error": f"{type(error).__name__}:{error}"})
    counts = pd.Series([r["status"] for r in results]).value_counts().to_dict() if results else {}
    print("NHL_POINTS_PER_MINUTE_SHADOW=" + json.dumps({"days": len(days), "captures": len(results), **counts}))
    for r in results:
        if r["status"] == "ERROR":
            print(json.dumps(r))
    return 1 if any(r["status"] == "ERROR" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
