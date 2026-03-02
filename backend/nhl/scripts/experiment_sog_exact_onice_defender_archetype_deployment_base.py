#!/usr/bin/env python3
"""Evaluate exact on-ice opposing-defender deployment using shooter expected-SOG archetype buckets."""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from typing import Any, Deque, Dict, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_exact_onice_defender_deployment_base import (
    DEFENDER_TOI_SQL,
    TEST_SHIFTS_SQL,
    THRESHOLDS,
    _combined_metric,
    _fetch_df,
    _metric_rows,
    _poisson_tail,
    _round,
    _split_df,
    _build_test_overlap_map,
)
from backend.shared.db.pg import pg_fetchall


DEFENDER_ONICE_ARCHETYPE_SHOTS_SQL = """
SELECT
  g.game_date::date AS game_date,
  e.game_id::bigint AS game_id,
  e.defending_team_id::bigint AS defending_team_id,
  d.player_id::bigint AS defender_id,
  e.shooting_player_id::bigint AS shooting_player_id
FROM nhl.shot_on_goal_events e
JOIN nhl.games g
  ON g.game_id = e.game_id
JOIN nhl.shiftcharts_shifts d
  ON d.game_id = e.game_id
 AND d.team_id = e.defending_team_id
 AND e.event_abs_sec >= d.start_sec
 AND e.event_abs_sec < d.end_sec
JOIN nhl.players p
  ON p.player_id = d.player_id
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND COALESCE(NULLIF(BTRIM(p.position), ''), 'F') = 'D'
  AND e.shooting_player_id IS NOT NULL
ORDER BY g.game_date, e.game_id, d.player_id
"""


def _build_defender_prior_rate_map(
    toi_df: pd.DataFrame,
    shot_rows_df: pd.DataFrame,
    bucket_map: Dict[Tuple[int, int], str],
) -> Dict[Tuple[int, int, str], float]:
    if toi_df.empty or shot_rows_df.empty:
        return {}

    shots = shot_rows_df.copy()
    shots["shooter_bucket"] = shots.apply(
        lambda row: bucket_map.get((int(row["game_id"]), int(row["shooting_player_id"])), "missing"),
        axis=1,
    )
    shots = shots[shots["shooter_bucket"] != "missing"].copy()
    if shots.empty:
        return {}

    shots = (
        shots.groupby(["game_date", "game_id", "defending_team_id", "defender_id", "shooter_bucket"], as_index=False)
        .size()
        .rename(columns={"size": "sog_allowed_onice"})
    )

    buckets = sorted(set(shots["shooter_bucket"].astype(str).unique().tolist()) | {"<1.5", "1.5-2.5", "2.5-3.5", "3.5+"})
    frames: List[pd.DataFrame] = []
    base = toi_df.copy()
    for bucket in buckets:
        tmp = base.copy()
        tmp["shooter_bucket"] = bucket
        frames.append(tmp)
    full = pd.concat(frames, ignore_index=True)
    full = full.merge(
        shots,
        on=["game_date", "game_id", "defending_team_id", "defender_id", "shooter_bucket"],
        how="left",
    )
    full["sog_allowed_onice"] = pd.to_numeric(full["sog_allowed_onice"], errors="coerce").fillna(0.0)
    full["toi_minutes"] = pd.to_numeric(full["toi_minutes"], errors="coerce").fillna(0.0)
    full = full.sort_values(["defender_id", "shooter_bucket", "game_date", "game_id"])

    out: Dict[Tuple[int, int, str], float] = {}
    for (defender_id, shooter_bucket), grp in full.groupby(["defender_id", "shooter_bucket"], sort=False):
        last10: Deque[Tuple[float, float]] = deque(maxlen=10)
        for row in grp.itertuples(index=False):
            key = (int(row.game_id), int(defender_id), str(shooter_bucket))
            if last10:
                sog_sum = sum(v[0] for v in last10)
                toi_sum = sum(v[1] for v in last10)
                out[key] = (sog_sum * 60.0 / toi_sum) if toi_sum > 0 else math.nan
            else:
                out[key] = math.nan
            last10.append((float(row.sog_allowed_onice or 0.0), float(row.toi_minutes or 0.0)))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Evaluate exact on-ice opposing-defender deployment using shooter expected-SOG archetype buckets."
    )
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--dataset-csv", default=None)
    ap.add_argument("--test-game-days", type=int, default=21)
    ap.add_argument("--write-scored-csv", default=None)
    args = ap.parse_args()

    if args.dataset_csv:
        df = pd.read_csv(args.dataset_csv)
    else:
        df = build_dataset_df(args.season, args.from_date, args.to_date)
    if df.empty:
        raise SystemExit("No rows available for the requested season/date range.")

    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)
    bucket_map = {
        (int(row.game_id), int(row.player_id)): str(row.expected_sog_bucket)
        for row in train.itertuples(index=False)
    }

    toi_df = _fetch_df(DEFENDER_TOI_SQL, args.season, args.from_date, args.to_date)
    shot_rows = _fetch_df(DEFENDER_ONICE_ARCHETYPE_SHOTS_SQL, args.season, args.from_date, args.to_date)
    if toi_df.empty or shot_rows.empty:
        raise SystemExit("Missing shiftcharts or exact on-ice shot data for the requested window.")

    defender_prior_map = _build_defender_prior_rate_map(toi_df, shot_rows, bucket_map)
    test_start = min(test_dates)
    test_end = max(test_dates)
    test_shift_rows = pg_fetchall(TEST_SHIFTS_SQL, (args.season, test_start, test_end))
    test_shifts = pd.DataFrame(test_shift_rows or [])
    overlap_map = _build_test_overlap_map(test_shifts, test)

    scored = test.copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)

    matchup_rates: List[float] = []
    matched_defender_counts: List[int] = []
    overlap_minutes: List[float] = []

    for row in scored.itertuples(index=False):
        key = (int(row.game_id), int(row.player_id))
        defender_secs = overlap_map.get(key, {})
        shooter_bucket = str(row.expected_sog_bucket)
        weighted_num = 0.0
        weighted_den = 0.0
        matched = 0
        total_sec = 0
        for defender_id, sec in defender_secs.items():
            rate = defender_prior_map.get((int(row.game_id), int(defender_id), shooter_bucket), math.nan)
            total_sec += int(sec)
            if rate is None or not math.isfinite(rate):
                continue
            weighted_num += float(rate) * float(sec)
            weighted_den += float(sec)
            matched += 1
        matchup_rates.append((weighted_num / weighted_den) if weighted_den > 0 else math.nan)
        matched_defender_counts.append(matched)
        overlap_minutes.append(total_sec / 60.0)

    scored["matchup_defender_archetype_allowed_d10_per60"] = matchup_rates
    scored["matched_defender_count"] = matched_defender_counts
    scored["matchup_overlap_minutes"] = overlap_minutes
    scored["lambda_defense_archetype"] = (
        pd.to_numeric(scored["matchup_defender_archetype_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
        * scored["d10_toi_min_avg"]
        / 60.0
    ).clip(lower=0.0)

    both = (scored["lambda_offense"] > 0) & (scored["lambda_defense_archetype"] > 0)
    scored["lambda_combined_archetype"] = scored["lambda_offense"]
    scored.loc[both, "lambda_combined_archetype"] = (
        (scored.loc[both, "rate_offense"] * scored.loc[both, "matchup_defender_archetype_allowed_d10_per60"]) ** 0.5
        * scored.loc[both, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both) & (scored["lambda_defense_archetype"] > 0), "lambda_combined_archetype"] = scored.loc[
        (~both) & (scored["lambda_defense_archetype"] > 0), "lambda_defense_archetype"
    ]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("defense_archetype", "lambda_defense_archetype"),
        ("combined_archetype", "lambda_combined_archetype"),
    ]:
        for line, threshold in THRESHOLDS.items():
            col = f"p_{kind}_over_{str(line).replace('.', '_')}"
            scored[col] = pd.to_numeric(scored[lam_col], errors="coerce").apply(
                lambda lam: _poisson_tail(float(lam), threshold) if pd.notna(lam) else math.nan
            )

    summary = {
        "ok": True,
        "season": args.season,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "test_window": {"from": min(test_dates), "to": max(test_dates), "days": len(test_dates)},
        "coverage": {
            "toi_rows": int(len(toi_df)),
            "shot_rows": int(len(shot_rows)),
            "rows_with_matchup_rate": int(pd.to_numeric(scored["matchup_defender_archetype_allowed_d10_per60"], errors="coerce").notna().sum()),
            "avg_matched_defenders": _round(pd.to_numeric(scored["matched_defender_count"], errors="coerce").mean()),
            "avg_overlap_minutes": _round(pd.to_numeric(scored["matchup_overlap_minutes"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_archetype": _combined_metric(scored, "defense_archetype"),
            "combined_archetype": _combined_metric(scored, "combined_archetype"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "defense_archetype": _metric_rows(scored, f"p_defense_archetype_over_{str(line).replace('.', '_')}", threshold),
            "combined_archetype": _metric_rows(scored, f"p_combined_archetype_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
