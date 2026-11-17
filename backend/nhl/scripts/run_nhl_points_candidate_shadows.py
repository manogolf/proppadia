#!/usr/bin/env python3
"""Score the Points candidate shadow arms for every Points capture of a slate (P2.58).

Each capture is scored once per arm; captures already scored are skipped. Runs as
a non-blocking step of each scheduled daily run. Captures taken before the PP TOI
correction (2026-11-15) carry pre-correction PP TOI, so the first scored slate is
2026-11-16; rescoring a past date is retrospective (the captures' own files and
that day's morning odds observation).

    ./bin/python backend/nhl/scripts/run_nhl_points_candidate_shadows.py --slate-date 2026-11-17
    ./bin/python backend/nhl/scripts/run_nhl_points_candidate_shadows.py --start 2026-11-16 --end 2026-11-16
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
from backend.nhl import points_candidate_shadows as shadow  # noqa: E402


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
    print("NHL_POINTS_CANDIDATE_SHADOWS=" + json.dumps({"days": len(days), "captures": len(results), **counts}))
    for r in results:
        if r["status"] == "ERROR":
            print(json.dumps(r))
    return 1 if any(r["status"] == "ERROR" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
