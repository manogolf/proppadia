#!/usr/bin/env python3
"""
Replay MLB wide+slate artifacts from archived odds_history folders.

Purpose:
- regenerate `mlb_predictions_wide_calibrated.csv` + `mlb_slate_output.csv` under
  `backend/mlb/exports/odds_history/YYYY-MM-DD/`
- enable row-level reconcile runs on historical dates without fresh OddsAPI pulls
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

from backend.mlb.scripts import build_mlb_predictions_wide
from backend.mlb.scripts import build_mlb_slate_output


DEFAULT_ODDS_ROOT = "backend/mlb/exports/odds_history"


def _parse_date(s: str) -> date:
    return datetime.strptime(str(s), "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> List[date]:
    out: List[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


@dataclass
class DayResult:
    day: str
    status: str
    reason: str


def _classify_empty_or_non_playable_snapshot(*, odds_file: Path, prop_types_csv: str) -> Optional[str]:
    """
    Return a skip reason when a snapshot has no playable MLB offers for the requested prop filter.

    This covers off-days / all-star style snapshots where replay should be skipped, not failed.
    Returns None when classification is inconclusive and caller should treat as a true failure.
    """
    try:
        events = build_mlb_predictions_wide._load_events_from_snapshot_file(odds_file)
    except Exception as exc:
        return f"snapshot_parse_error:{type(exc).__name__}"

    if not events:
        return "no_events_in_snapshot"

    try:
        market_to_prop = build_mlb_predictions_wide._invert_market_map()
        team_name_rev = build_mlb_predictions_wide._build_team_name_reverse()
        prop_filter = build_mlb_predictions_wide._parse_prop_types_csv(str(prop_types_csv or ""))
        offers, flatten_counts = build_mlb_predictions_wide._flatten_market_snapshot(
            events=events,
            market_to_prop=market_to_prop,
            team_name_rev=team_name_rev,
            prop_filter=prop_filter,
        )
    except Exception as exc:
        return f"offer_scan_error:{type(exc).__name__}"

    if len(offers) > 0:
        return None

    unknown_team = int(flatten_counts.get("skip_unknown_team_name", 0))
    if unknown_team >= len(events):
        return f"non_mlb_or_unmapped_events(events={len(events)})"
    return f"no_supported_offers(events={len(events)})"


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay MLB slate outputs from archived odds history snapshots.")
    ap.add_argument("--odds-root", default=DEFAULT_ODDS_ROOT)
    ap.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--odds-filename",
        default="odds_latest_compatible.json",
        help="Archived odds file to load (e.g., odds_latest_compatible.json or odds_mlb_playerprops.json)",
    )
    ap.add_argument("--wide-filename", default="mlb_predictions_wide_calibrated.csv")
    ap.add_argument("--slate-filename", default="mlb_slate_output.csv")
    ap.add_argument("--prop-types", default="", help="Optional CSV prop_type filter passed to wide replay.")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--strict", action="store_true", help="Exit non-zero on first failed day.")
    ap.add_argument("--max-days", type=int, default=0, help="Optional cap on dates processed.")
    ap.add_argument("--out-summary-json", default="tmp/mlb_replay_slate_outputs_summary.json")
    args = ap.parse_args()

    start = _parse_date(args.from_date)
    end = _parse_date(args.to_date)
    if end < start:
        raise SystemExit("--to-date must be >= --from-date")

    days = _date_range(start, end)
    if int(args.max_days or 0) > 0:
        days = days[: int(args.max_days)]

    odds_root = Path(str(args.odds_root)).expanduser()
    summary_path = Path(str(args.out_summary_json)).expanduser()
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    results: List[DayResult] = []
    processed = 0
    skipped = 0
    failed = 0

    for d in days:
        day = d.isoformat()
        day_dir = odds_root / day
        odds_file = day_dir / str(args.odds_filename)
        wide_file = day_dir / str(args.wide_filename)
        slate_file = day_dir / str(args.slate_filename)

        if not odds_file.exists():
            skipped += 1
            results.append(DayResult(day=day, status="skipped", reason=f"missing {odds_file.name}"))
            continue

        if not args.overwrite and wide_file.exists() and slate_file.exists():
            skipped += 1
            results.append(DayResult(day=day, status="skipped", reason="already_has_wide_and_slate"))
            continue

        pre_skip_reason = _classify_empty_or_non_playable_snapshot(
            odds_file=odds_file,
            prop_types_csv=str(args.prop_types or ""),
        )
        if pre_skip_reason and (
            pre_skip_reason.startswith("no_events_in_snapshot")
            or pre_skip_reason.startswith("no_supported_offers")
            or pre_skip_reason.startswith("non_mlb_or_unmapped_events")
        ):
            skipped += 1
            results.append(DayResult(day=day, status="skipped", reason=pre_skip_reason))
            continue

        wide_args = [
            "--slate-date",
            day,
            "--output",
            str(wide_file),
            "--odds-snapshot-in",
            str(odds_file),
            "--require-min-rows",
            "1",
        ]
        if str(args.prop_types or "").strip():
            wide_args.extend(["--prop-types", str(args.prop_types).strip()])

        rc_wide = int(build_mlb_predictions_wide.main(wide_args))
        if rc_wide != 0:
            skip_reason = _classify_empty_or_non_playable_snapshot(
                odds_file=odds_file,
                prop_types_csv=str(args.prop_types or ""),
            )
            if skip_reason and (
                skip_reason.startswith("no_events_in_snapshot")
                or skip_reason.startswith("no_supported_offers")
                or skip_reason.startswith("non_mlb_or_unmapped_events")
            ):
                skipped += 1
                results.append(DayResult(day=day, status="skipped", reason=skip_reason))
                continue

            failed += 1
            reason = f"wide_rc={rc_wide}"
            if skip_reason:
                reason = f"{reason}; {skip_reason}"
            results.append(DayResult(day=day, status="failed", reason=reason))
            if args.strict:
                break
            continue

        slate_args = [
            "--slate-date",
            day,
            "--pred-csv",
            str(wide_file),
            "--out-csv",
            str(slate_file),
        ]
        rc_slate = int(build_mlb_slate_output.main(slate_args))
        if rc_slate != 0:
            failed += 1
            reason = f"slate_rc={rc_slate}"
            results.append(DayResult(day=day, status="failed", reason=reason))
            if args.strict:
                break
            continue

        processed += 1
        results.append(DayResult(day=day, status="processed", reason="ok"))
        print(f"[mlb-replay] processed {day}")

    payload = {
        "from_date": args.from_date,
        "to_date": args.to_date,
        "requested_days": len(days),
        "processed": processed,
        "skipped": skipped,
        "failed": failed,
        "odds_root": str(odds_root),
        "odds_filename": str(args.odds_filename),
        "wide_filename": str(args.wide_filename),
        "slate_filename": str(args.slate_filename),
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "results": [r.__dict__ for r in results],
    }
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(
        f"[mlb-replay] done requested={len(days)} processed={processed} "
        f"skipped={skipped} failed={failed} summary={summary_path}"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
