#!/usr/bin/env python3
"""Evaluate exact on-ice opposing-defender deployment scaled by same-unit position share."""

from __future__ import annotations

import argparse
import json
import math
from typing import Dict, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_exact_onice_defender_deployment_base import (
    DEFENDER_ONICE_SHOTS_SQL,
    DEFENDER_TOI_SQL,
    TEST_SHIFTS_SQL,
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


def _overlap_seconds(a: List[Tuple[int, int]], b: List[Tuple[int, int]]) -> int:
    i = 0
    j = 0
    total = 0
    while i < len(a) and j < len(b):
        a_start, a_end = a[i]
        b_start, b_end = b[j]
        start = max(a_start, b_start)
        end = min(a_end, b_end)
        if end > start:
            total += end - start
        if a_end <= b_end:
            i += 1
        else:
            j += 1
    return total


def _build_unit_share_map(
    shifts_df: pd.DataFrame,
    test_rows: pd.DataFrame,
) -> Dict[Tuple[int, int], float]:
    out: Dict[Tuple[int, int], float] = {}
    if shifts_df.empty or test_rows.empty:
        return out

    test_key = test_rows[["game_id", "player_id", "team_id", "position_bucket", "d10_sog_per60"]].copy()
    test_key["position_bucket"] = test_key["position_bucket"].astype(str).apply(_norm_pos)
    rate_map = {
        (int(r.game_id), int(r.player_id)): float(r.d10_sog_per60 or 0.0)
        for r in test_key.itertuples(index=False)
    }

    rows_needed = (
        test_key[["game_id", "player_id", "team_id", "position_bucket"]]
        .drop_duplicates()
        .sort_values(["game_id", "player_id"])
    )
    game_to_requests: Dict[int, List[Tuple[int, int, str]]] = {}
    for row in rows_needed.itertuples(index=False):
        game_to_requests.setdefault(int(row.game_id), []).append((int(row.player_id), int(row.team_id), str(row.position_bucket)))

    for game_id, game_shifts in shifts_df.groupby("game_id", sort=False):
        reqs = game_to_requests.get(int(game_id))
        if not reqs:
            continue

        player_intervals: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
        player_pos: Dict[Tuple[int, int], str] = {}
        player_toi_sec: Dict[Tuple[int, int], int] = {}
        for row in game_shifts.itertuples(index=False):
            team_id = int(row.team_id)
            player_id = int(row.player_id)
            pos = _norm_pos(row.position_raw)
            if pos == "G":
                continue
            key = (team_id, player_id)
            player_intervals.setdefault(key, []).append((int(row.start_sec), int(row.end_sec)))
            player_pos[key] = pos
            player_toi_sec[key] = player_toi_sec.get(key, 0) + max(0, int(row.end_sec) - int(row.start_sec))

        for player_id, team_id, pos in reqs:
            p_key = (team_id, player_id)
            p_intervals = player_intervals.get(p_key)
            if not p_intervals:
                continue
            own_rate = float(rate_map.get((int(game_id), int(player_id)), 0.0))
            own_toi = float(player_toi_sec.get(p_key, 0))
            own_weight = own_rate * own_toi
            unit_total = own_weight
            for (mate_team, mate_id), mate_intervals in player_intervals.items():
                if mate_team != team_id or mate_id == player_id:
                    continue
                if player_pos.get((mate_team, mate_id)) != pos:
                    continue
                mate_rate = float(rate_map.get((int(game_id), int(mate_id)), 0.0))
                if mate_rate <= 0:
                    continue
                overlap_sec = _overlap_seconds(p_intervals, mate_intervals)
                if overlap_sec <= 0:
                    continue
                unit_total += mate_rate * float(overlap_sec)
            out[(int(game_id), int(player_id))] = (own_weight / unit_total) if unit_total > 0 else math.nan
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Evaluate exact on-ice opposing-defender deployment scaled by same-unit position share."
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
    shifts_df = _fetch_df(DEFENDER_TOI_SQL, args.season, args.from_date, args.to_date)
    shots_df = _fetch_df(DEFENDER_ONICE_SHOTS_SQL, args.season, args.from_date, args.to_date)
    if shifts_df.empty or shots_df.empty:
        raise SystemExit("Missing shiftcharts or shot-on-goal event data for the requested window.")

    defender_prior_map = _build_defender_prior_rate_map(shifts_df, shots_df)
    test_start = min(test_dates)
    test_end = max(test_dates)
    test_shift_rows = pg_fetchall(TEST_SHIFTS_SQL, (args.season, test_start, test_end))
    test_shifts = pd.DataFrame(test_shift_rows or [])
    overlap_map = _build_test_overlap_map(test_shifts, test)

    test_work = test.copy()
    test_work["position_bucket"] = test_work["position_raw"].fillna("F").astype(str).apply(_norm_pos)
    unit_share_map = _build_unit_share_map(test_shifts, test_work)

    scored = test_work.copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)
    scored["unit_pos_share"] = scored.apply(
        lambda row: unit_share_map.get((int(row["game_id"]), int(row["player_id"])), math.nan),
        axis=1,
    )

    matchup_rates_total: List[float] = []
    matchup_rates_unitshare: List[float] = []
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
        share = float(row.unit_pos_share) if pd.notna(row.unit_pos_share) else math.nan
        matchup_rates_unitshare.append(pos_total_rate * share if math.isfinite(pos_total_rate) and math.isfinite(share) else math.nan)
        matched_defender_counts.append(matched)
        overlap_minutes.append(total_sec / 60.0)

    scored["matchup_defender_pos_total_d10_per60"] = matchup_rates_total
    scored["matchup_defender_unitshare_allowed_d10_per60"] = matchup_rates_unitshare
    scored["matched_defender_count"] = matched_defender_counts
    scored["matchup_overlap_minutes"] = overlap_minutes
    scored["lambda_defense_unitshare"] = (
        pd.to_numeric(scored["matchup_defender_unitshare_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
        * scored["d10_toi_min_avg"]
        / 60.0
    ).clip(lower=0.0)

    both = (scored["lambda_offense"] > 0) & (scored["lambda_defense_unitshare"] > 0)
    scored["lambda_combined_unitshare"] = scored["lambda_offense"]
    scored.loc[both, "lambda_combined_unitshare"] = (
        (scored.loc[both, "rate_offense"] * scored.loc[both, "matchup_defender_unitshare_allowed_d10_per60"]) ** 0.5
        * scored.loc[both, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both) & (scored["lambda_defense_unitshare"] > 0), "lambda_combined_unitshare"] = scored.loc[
        (~both) & (scored["lambda_defense_unitshare"] > 0), "lambda_defense_unitshare"
    ]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("defense_unitshare", "lambda_defense_unitshare"),
        ("combined_unitshare", "lambda_combined_unitshare"),
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
            "rows_with_unit_share": int(pd.to_numeric(scored["unit_pos_share"], errors="coerce").notna().sum()),
            "rows_with_matchup_rate": int(pd.to_numeric(scored["matchup_defender_unitshare_allowed_d10_per60"], errors="coerce").notna().sum()),
            "avg_unit_pos_share": _round(pd.to_numeric(scored["unit_pos_share"], errors="coerce").mean()),
            "avg_matched_defenders": _round(pd.to_numeric(scored["matched_defender_count"], errors="coerce").mean()),
            "avg_overlap_minutes": _round(pd.to_numeric(scored["matchup_overlap_minutes"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_unitshare": _combined_metric(scored, "defense_unitshare"),
            "combined_unitshare": _combined_metric(scored, "combined_unitshare"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "defense_unitshare": _metric_rows(scored, f"p_defense_unitshare_over_{str(line).replace('.', '_')}", threshold),
            "combined_unitshare": _metric_rows(scored, f"p_combined_unitshare_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
