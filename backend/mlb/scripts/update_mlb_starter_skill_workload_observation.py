#!/usr/bin/env python3
"""Update no-write starter skill/workload prospective observation status.

This script is intentionally narrow. It reads existing starter skill/workload
research artifacts, optionally checks completed starter outcomes from
mlb.player_stats with read-only SQL, and writes only research observation
artifacts. It does not write to the database or alter production behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from zoneinfo import ZoneInfo

from backend.shared.db.pg import pg_fetchall


DEFAULT_DAILY_ROOT = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_daily")
DEFAULT_OBS_ROOT = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_prospective_observation"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _load_env(path: Path = Path("backend/.env")) -> None:
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _latest_run_dir(daily_root: Path, date_value: str) -> Path | None:
    runs = daily_root / date_value / "runs"
    if not runs.exists():
        return None
    candidates = [p for p in runs.iterdir() if p.is_dir() and (p / "starter_game_rows.csv").exists()]
    if not candidates:
        return None
    return sorted(candidates, key=lambda p: p.stat().st_mtime)[-1]


def _fetch_actual_starters(date_value: str, no_db: bool) -> pd.DataFrame:
    if no_db:
        return pd.DataFrame()
    rows = pg_fetchall(
        """
        SELECT game_date, game_id, player_id, team, opponent, position,
               is_starter, hits_allowed, outs_recorded, earned_runs,
               walks_allowed, strikeouts_pitching
        FROM mlb.player_stats
        WHERE game_date = %s
          AND is_starter = 1
        """,
        (date_value,),
    )
    return pd.DataFrame(rows)


def _stage(count: int) -> int:
    if count >= 15:
        return 15
    if count >= 10:
        return 10
    if count >= 5:
        return 5
    return 0


def _stage_decision_label(stage: int) -> str:
    return {
        0: "OBSERVATION_IN_PROGRESS_BELOW_FIRST_REVIEW",
        5: "OPERATIONAL_SHAKEOUT_REVIEW_DUE",
        10: "INITIAL_PROSPECTIVE_EVIDENCE_REVIEW_DUE",
        15: "MODELING_READINESS_DECISION_DUE",
    }[stage]


def _is_completed_slate_date(date_value: str) -> bool:
    try:
        slate_date = datetime.strptime(date_value, "%Y-%m-%d").date()
    except ValueError:
        return False
    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    return slate_date < today_et


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def update_observation(args: argparse.Namespace) -> dict[str, Any]:
    _load_env()
    obs_root = Path(args.observation_root)
    daily_root = Path(args.daily_root)
    date_value = args.date
    run_dir = Path(args.run_dir) if args.run_dir else _latest_run_dir(daily_root, date_value)
    generated_at = _utc_now()

    if run_dir is None:
        row = {
            "slate_date": date_value,
            "pregame_run_tag": "",
            "pregame_run_timestamp": "",
            "pregame_run_dir": "",
            "starter_game_row_count": 0,
            "strict_prior_row_count": 0,
            "strict_prior_fail_count": 0,
            "completeness_status": "missing_pregame_artifact",
            "outcome_binding_status": "not_attempted",
            "actual_starter_outcome_row_count": 0,
            "completed_slate_eligibility": "not_eligible",
            "exclusion_reason": "missing_pregame_artifact",
            "cumulative_eligible_completed_slate_count": 0,
            "stage_reached": 0,
            "stage_decision_label": _stage_decision_label(0),
            "updated_at_utc": generated_at,
        }
        starter_rows = pd.DataFrame()
        actual = pd.DataFrame()
    else:
        starter_path = run_dir / "starter_game_rows.csv"
        starter_rows = pd.read_csv(starter_path, low_memory=False)
        readiness = _read_json(run_dir / "readiness.json")
        actual = _fetch_actual_starters(date_value, args.no_db)
        strict_prior_count = int(starter_rows["strict_prior_status"].eq("PASS_STRICT_PRIOR").sum())
        strict_fail_count = int(len(starter_rows) - strict_prior_count)
        slate_date_completed = _is_completed_slate_date(date_value)
        if not slate_date_completed:
            outcome_status = "pending_current_or_future_slate"
            actual_count = int(len(actual)) if not actual.empty else 0
            eligible = "not_eligible"
            exclusion = "slate_date_not_completed_relative_to_america_new_york"
        elif actual.empty:
            outcome_status = "pending_or_unavailable"
            actual_count = 0
            eligible = "not_eligible"
            exclusion = "actual_starter_outcomes_not_bound"
        else:
            actual_count = int(len(actual))
            expected_ids = set(starter_rows["expected_starter_id"].dropna().astype(int).astype(str))
            actual_ids = set(pd.to_numeric(actual["player_id"], errors="coerce").dropna().astype(int).astype(str))
            matched = len(expected_ids & actual_ids)
            match_pct = matched / len(expected_ids) if expected_ids else 0.0
            if match_pct >= args.min_starter_match_pct:
                outcome_status = "bound_sufficient"
                eligible = "eligible"
                exclusion = ""
            else:
                outcome_status = "bound_insufficient"
                eligible = "not_eligible"
                exclusion = f"starter_match_pct_{match_pct:.3f}_below_{args.min_starter_match_pct:.3f}"
        row = {
            "slate_date": date_value,
            "pregame_run_tag": readiness.get("run_tag", run_dir.name),
            "pregame_run_timestamp": readiness.get("generated_at_utc", ""),
            "pregame_run_dir": str(run_dir),
            "starter_game_row_count": int(len(starter_rows)),
            "strict_prior_row_count": strict_prior_count,
            "strict_prior_fail_count": strict_fail_count,
            "completeness_status": "complete_with_warnings" if strict_fail_count else "complete",
            "outcome_binding_status": outcome_status,
            "actual_starter_outcome_row_count": actual_count,
            "completed_slate_eligibility": eligible,
            "exclusion_reason": exclusion,
            "cumulative_eligible_completed_slate_count": 0,
            "stage_reached": 0,
            "stage_decision_label": "",
            "updated_at_utc": generated_at,
        }

    index_path = obs_root / "starter_skill_workload_observation_index.csv"
    if index_path.exists():
        existing = pd.read_csv(index_path, low_memory=False)
        rows = existing[existing["slate_date"].astype(str).ne(date_value)].to_dict("records")
    else:
        rows = []
    rows.append(row)
    rows = sorted(rows, key=lambda r: str(r.get("slate_date", "")))
    eligible_count = 0
    for item in rows:
        if item.get("completed_slate_eligibility") == "eligible":
            eligible_count += 1
        item["cumulative_eligible_completed_slate_count"] = eligible_count
        item["stage_reached"] = _stage(eligible_count)
        item["stage_decision_label"] = _stage_decision_label(int(item["stage_reached"]))

    fields = [
        "slate_date",
        "pregame_run_tag",
        "pregame_run_timestamp",
        "pregame_run_dir",
        "starter_game_row_count",
        "strict_prior_row_count",
        "strict_prior_fail_count",
        "completeness_status",
        "outcome_binding_status",
        "actual_starter_outcome_row_count",
        "completed_slate_eligibility",
        "exclusion_reason",
        "cumulative_eligible_completed_slate_count",
        "stage_reached",
        "stage_decision_label",
        "updated_at_utc",
    ]
    _write_csv(index_path, rows, fields)
    latest_summary = {
        "generated_at_utc": generated_at,
        "date": date_value,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
        "index_path": str(index_path),
        "latest_row": rows[-1] if rows else {},
        "cumulative_eligible_completed_slate_count": eligible_count,
        "stage_reached": _stage(eligible_count),
        "stage_decision_label": _stage_decision_label(_stage(eligible_count)),
        "eligibility_rule": (
            "eligible only when pregame artifact exists, strict-prior support is retained, "
            "and actual starter outcomes bind for at least the configured starter-match threshold"
        ),
    }
    (obs_root / "starter_skill_workload_observation_status_latest.json").write_text(
        json.dumps(latest_summary, indent=2, sort_keys=True) + "\n"
    )
    return latest_summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--daily-root", default=str(DEFAULT_DAILY_ROOT))
    parser.add_argument("--observation-root", default=str(DEFAULT_OBS_ROOT))
    parser.add_argument("--min-starter-match-pct", type=float, default=0.80)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    result = update_observation(parse_args(argv))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
