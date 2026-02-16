#!/usr/bin/env python3
"""Show recent season cutover cadence snapshots and regression signals."""

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
        raw = line.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _lane_map(item: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for lane in item.get("lanes") or []:
        if not isinstance(lane, dict):
            continue
        name = str(lane.get("name") or "").strip()
        cron = str(lane.get("cron") or "").strip()
        if name:
            out[name] = cron
    return out


def _regressions(prev: dict[str, Any] | None, cur: dict[str, Any]) -> list[str]:
    if not prev:
        return []
    out: list[str] = []
    prev_tz = str(prev.get("timezone") or "")
    cur_tz = str(cur.get("timezone") or "")
    if prev_tz != cur_tz:
        out.append(f"timezone_changed:{prev_tz}->{cur_tz}")

    prev_lanes = _lane_map(prev)
    cur_lanes = _lane_map(cur)
    for lane_name, cur_cron in sorted(cur_lanes.items()):
        prev_cron = prev_lanes.get(lane_name)
        if prev_cron is not None and prev_cron != cur_cron:
            out.append(f"cron_changed:{lane_name}:{prev_cron}->{cur_cron}")
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Display recent season cutover cadence snapshots.")
    ap.add_argument("--input", default="artifacts/season_cutover_history.jsonl")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else [])

    history = _load_history(Path(args.input))
    tail = history[-max(1, int(args.limit)) :]
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(tail):
        prev = tail[idx - 1] if idx > 0 else None
        rows.append(
            {
                "captured_at": item.get("captured_at"),
                "status": item.get("status"),
                "ok": item.get("ok"),
                "timezone": item.get("timezone"),
                "lane_count": len(item.get("lanes") or []),
                "regressions": _regressions(prev, item),
            }
        )

    payload = {
        "input": str(args.input),
        "history_count": len(history),
        "returned": len(rows),
        "rows": rows,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"Season cutover history: {args.input} ({len(history)} rows total)")
        for row in rows:
            reg = ",".join(row["regressions"]) if row["regressions"] else "none"
            print(
                f"- {row['captured_at']} | {row['status']} ok={row['ok']} "
                f"tz={row['timezone']} lanes={row['lane_count']} regressions={reg}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
