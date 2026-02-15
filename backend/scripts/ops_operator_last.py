#!/usr/bin/env python3
"""Show last N compact ops operator snapshots with simple regressions."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from backend.scripts.mlb_readiness_last import _load_history


def _regressions(prev: dict[str, Any], cur: dict[str, Any]) -> list[str]:
    out: list[str] = []
    prev_ok = bool(prev.get("ok"))
    cur_ok = bool(cur.get("ok"))
    if prev_ok and not cur_ok:
        out.append("overall_became_fail")

    prev_stat = int((((prev.get("mlb_readiness") or {}).get("stat_count")) or 0))
    cur_stat = int((((cur.get("mlb_readiness") or {}).get("stat_count")) or 0))
    if cur_stat < prev_stat:
        out.append(f"stat_count_drop:{prev_stat}->{cur_stat}")

    prev_roster_stale = (((prev.get("mlb_readiness") or {}).get("roster_stale")))
    cur_roster_stale = (((cur.get("mlb_readiness") or {}).get("roster_stale")))
    if prev_roster_stale is False and cur_roster_stale is True:
        out.append("roster_became_stale")

    prev_blockers = int((((prev.get("season_activation") or {}).get("blocker_count")) or 0))
    cur_blockers = int((((cur.get("season_activation") or {}).get("blocker_count")) or 0))
    if cur_blockers > prev_blockers:
        out.append(f"blockers_increase:{prev_blockers}->{cur_blockers}")
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Display last compact ops summary rows.")
    ap.add_argument("--input", default="artifacts/ops_operator_history.jsonl")
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
                "status": item.get("status"),
                "ok": item.get("ok"),
                "governance_ok": (((item.get("governance") or {}).get("ok"))),
                "readiness_ok": (((item.get("mlb_readiness") or {}).get("ok"))),
                "season_ok": (((item.get("season_activation") or {}).get("ok"))),
                "stat_count": (((item.get("mlb_readiness") or {}).get("stat_count"))),
                "roster_stale": (((item.get("mlb_readiness") or {}).get("roster_stale"))),
                "blocker_count": (((item.get("season_activation") or {}).get("blocker_count"))),
                "top_blocker": (((item.get("season_activation") or {}).get("top_blocker"))),
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
        print(f"Ops summary history: {path} ({len(history)} rows total)")
        for row in rows:
            reg = ",".join(row["regressions"]) if row["regressions"] else "none"
            print(
                f"- {row['status']} ok={row['ok']} gov={row['governance_ok']} "
                f"readiness={row['readiness_ok']} season={row['season_ok']} "
                f"stat={row['stat_count']} stale={row['roster_stale']} blockers={row['blocker_count']} "
                f"top={row['top_blocker'] or '-'} regressions={reg}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
