#!/usr/bin/env python3
"""Evaluate exact on-ice opposing-defender deployment scaled by player position-group SOG share."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_exact_onice_defender_playernorm_deployment_base import (
    SHIFTS_SQL,
    SHOTS_SQL,
    THRESHOLDS,
    _combined_metric,
    _fetch_df,
    _metric_rows,
    _norm_pos,
    _poisson_tail,
    _round,
    _build_defender_prior_rate_map,
    _build_test_overlap_map,
    _split_df,
)
from backend.shared.db.pg import pg_fetchall


RAW_SHARE_SQL = """
SELECT
  g.game_date::date AS game_date,
  l.game_id::bigint AS game_id,
  l.player_id::bigint AS player_id,
  l.team_id::bigint AS team_id,
  CASE
    WHEN COALESCE(NULLIF(BTRIM(p.position), ''), 'F') = 'D' THEN 'D'
    ELSE 'F'
  END AS position_bucket,
  l.shots_on_goal::int AS shots_on_goal
FROM nhl.skater_game_logs_raw l
JOIN nhl.games g USING (game_id)
LEFT JOIN nhl.players p
  ON p.player_id = l.player_id
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND l.shots_on_goal IS NOT NULL
ORDER BY g.game_date, g.game_id, l.team_id, l.player_id
"""


def _build_player_pos_share_map(raw_df: pd.DataFrame) -> Dict[Tuple[int, int], float]:
    if raw_df.empty:
        return {}

    grouped = (
        raw_df.groupby(["team_id", "position_bucket", "game_date", "game_id"], sort=False)
        .agg(player_rows=("player_id", list), shots_rows=("shots_on_goal", list))
        .reset_index()
        .sort_values(["team_id", "position_bucket", "game_date", "game_id"])
    )

    out: Dict[Tuple[int, int], float] = {}
    for (team_id, pos_bucket), grp in grouped.groupby(["team_id", "position_bucket"], sort=False):
        last10: Deque[Tuple[float, Dict[int, float]]] = deque(maxlen=10)
        window_total = 0.0
        player_window_sum: Dict[int, float] = defaultdict(float)

        for row in grp.itertuples(index=False):
            players = [int(v) for v in row.player_rows]
            shots = [float(v or 0.0) for v in row.shots_rows]
            current_game_player_shots = dict(zip(players, shots))

            for player_id in players:
                if window_total > 0:
                    out[(int(row.game_id), int(player_id))] = min(
                        1.0, max(0.0, float(player_window_sum.get(int(player_id), 0.0)) / float(window_total))
                    )
                else:
                    out[(int(row.game_id), int(player_id))] = math.nan

            game_total = float(sum(shots))
            last10.append((game_total, current_game_player_shots))
            window_total += game_total
            for player_id, sog in current_game_player_shots.items():
                player_window_sum[int(player_id)] += float(sog)

            if len(last10) > 10:
                old_total, old_map = last10.popleft()
                window_total -= float(old_total)
                for player_id, sog in old_map.items():
                    player_window_sum[int(player_id)] -= float(sog)
                    if abs(player_window_sum[int(player_id)]) < 1e-12:
                        player_window_sum.pop(int(player_id), None)

    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Evaluate exact on-ice opposing-defender deployment scaled by player position-group SOG share."
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

    raw_rows = pg_fetchall(RAW_SHARE_SQL, (args.season, args.from_date, args.from_date, args.to_date, args.to_date))
    raw_df = pd.DataFrame(raw_rows or [])
    if raw_df.empty:
        raise SystemExit("No raw skater log rows available for player position-share build.")

    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)
    shifts_df = _fetch_df(SHIFTS_SQL, args.season, args.from_date, args.to_date)
    shots_df = _fetch_df(SHOTS_SQL, args.season, args.from_date, args.to_date)
    if shifts_df.empty or shots_df.empty:
        raise SystemExit("Missing shiftcharts or shot-on-goal event data for the requested window.")

    defender_prior_map = _build_defender_prior_rate_map(shifts_df, shots_df)
    test_shifts = shifts_df[shifts_df["game_date"].astype(str).isin(test_dates)].copy()
    overlap_map = _build_test_overlap_map(test_shifts, test)
    player_pos_share_map = _build_player_pos_share_map(raw_df)

    scored = test.copy()
    scored["position_bucket"] = scored["position_raw"].fillna("F").astype(str).apply(_norm_pos)
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)
    scored["player_pos_share_10"] = scored.apply(
        lambda row: player_pos_share_map.get((int(row["game_id"]), int(row["player_id"])), math.nan),
        axis=1,
    )

    matchup_rates_total: List[float] = []
    matchup_rates_share: List[float] = []
    matched_defender_counts: List[int] = []
    overlap_minutes: List[float] = []

    for row in scored.itertuples(index=False):
        key = (int(row.game_id), int(row.player_id))
        defender_secs = overlap_map.get(key, {})
        pos_bucket = str(row.position_bucket)
        weighted_num = 0.0
        weighted_den = 0.0
        matched = 0
        total_sec = 0
        for defender_id, sec in defender_secs.items():
            rate = defender_prior_map.get((int(row.game_id), int(defender_id), pos_bucket), math.nan)
            total_sec += int(sec)
            if rate is None or not math.isfinite(rate):
                continue
            weighted_num += float(rate) * float(sec)
            weighted_den += float(sec)
            matched += 1
        pos_total_rate = (weighted_num / weighted_den) if weighted_den > 0 else math.nan
        matchup_rates_total.append(pos_total_rate)
        share = float(row.player_pos_share_10) if pd.notna(row.player_pos_share_10) else math.nan
        matchup_rates_share.append(pos_total_rate * share if math.isfinite(pos_total_rate) and math.isfinite(share) else math.nan)
        matched_defender_counts.append(matched)
        overlap_minutes.append(total_sec / 60.0)

    scored["matchup_defender_pos_total_d10_per60"] = matchup_rates_total
    scored["matchup_defender_share_allowed_d10_per60"] = matchup_rates_share
    scored["matched_defender_count"] = matched_defender_counts
    scored["matchup_overlap_minutes"] = overlap_minutes
    scored["lambda_defense_share"] = (
        pd.to_numeric(scored["matchup_defender_share_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
        * scored["d10_toi_min_avg"]
        / 60.0
    ).clip(lower=0.0)

    both = (scored["lambda_offense"] > 0) & (scored["lambda_defense_share"] > 0)
    scored["lambda_combined_share"] = scored["lambda_offense"]
    scored.loc[both, "lambda_combined_share"] = (
        (scored.loc[both, "rate_offense"] * scored.loc[both, "matchup_defender_share_allowed_d10_per60"]) ** 0.5
        * scored.loc[both, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both) & (scored["lambda_defense_share"] > 0), "lambda_combined_share"] = scored.loc[
        (~both) & (scored["lambda_defense_share"] > 0), "lambda_defense_share"
    ]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("defense_share", "lambda_defense_share"),
        ("combined_share", "lambda_combined_share"),
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
            "shift_rows": int(len(shifts_df)),
            "shot_rows": int(len(shots_df)),
            "raw_share_rows": int(len(raw_df)),
            "rows_with_share": int(pd.to_numeric(scored["player_pos_share_10"], errors="coerce").notna().sum()),
            "rows_with_matchup_rate": int(pd.to_numeric(scored["matchup_defender_share_allowed_d10_per60"], errors="coerce").notna().sum()),
            "avg_player_pos_share_10": _round(pd.to_numeric(scored["player_pos_share_10"], errors="coerce").mean()),
            "avg_matched_defenders": _round(pd.to_numeric(scored["matched_defender_count"], errors="coerce").mean()),
            "avg_overlap_minutes": _round(pd.to_numeric(scored["matchup_overlap_minutes"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_share": _combined_metric(scored, "defense_share"),
            "combined_share": _combined_metric(scored, "combined_share"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "defense_share": _metric_rows(scored, f"p_defense_share_over_{str(line).replace('.', '_')}", threshold),
            "combined_share": _metric_rows(scored, f"p_combined_share_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
