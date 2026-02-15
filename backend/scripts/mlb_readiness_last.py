#!/usr/bin/env python3
"""Show last N MLB readiness snapshots and simple regression signals."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


def _load_history(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    return rows


def _regressions(prev: dict[str, Any], cur: dict[str, Any]) -> list[str]:
    out: list[str] = []

    prev_stat = int((((prev.get("checks") or {}).get("stat_derived") or {}).get("count") or 0))
    cur_stat = int((((cur.get("checks") or {}).get("stat_derived") or {}).get("count") or 0))
    if cur_stat < prev_stat:
        out.append(f"stat_derived_count_drop:{prev_stat}->{cur_stat}")

    prev_roster = int((((prev.get("checks") or {}).get("roster") or {}).get("total_players") or 0))
    cur_roster = int((((cur.get("checks") or {}).get("roster") or {}).get("total_players") or 0))
    if cur_roster < prev_roster:
        out.append(f"roster_total_drop:{prev_roster}->{cur_roster}")

    prev_stale = (((prev.get("checks") or {}).get("roster") or {}).get("stale"))
    cur_stale = (((cur.get("checks") or {}).get("roster") or {}).get("stale"))
    if prev_stale is False and cur_stale is True:
        out.append("roster_became_stale")

    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Display last MLB readiness snapshots.")
    ap.add_argument("--input", default="artifacts/mlb_readiness_history.jsonl")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else [])

    path = Path(args.input)
    history = _load_history(path)
    tail = history[-max(1, int(args.limit)) :]

    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "stat_derived_count": (((item.get("checks") or {}).get("stat_derived") or {}).get("count")),
                "roster_total_players": (((item.get("checks") or {}).get("roster") or {}).get("total_players")),
                "roster_stale": (((item.get("checks") or {}).get("roster") or {}).get("stale")),
                "regressions": _regressions(prev, item) if prev else [],
            }
        )

    payload = {
        "input": str(path),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"Readiness history: {path} ({len(history)} rows total)")
        for row in rows:
            reg = ", ".join(row["regressions"]) if row["regressions"] else "none"
            print(
                f"- {row['captured_at']} | {row['status']} | stat={row['stat_derived_count']} "
                f"| roster={row['roster_total_players']} stale={row['roster_stale']} | regressions={reg}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
