#!/usr/bin/env python3
"""Evaluate exact on-ice opposing defense-pair deployment using shooter expected-SOG archetype buckets."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict, deque
from itertools import combinations
from typing import Deque, Dict, Iterable, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_exact_onice_defender_deployment_base import (
    TEST_SHIFTS_SQL,
    THRESHOLDS,
    _combined_metric,
    _metric_rows,
    _norm_pos,
    _poisson_tail,
    _round,
    _split_df,
)
from backend.shared.db.pg import pg_fetchall


SHIFT_ROWS_SQL = """
SELECT
  g.game_date::date AS game_date,
  s.game_id::bigint AS game_id,
  s.team_id::bigint AS team_id,
  s.player_id::bigint AS player_id,
  COALESCE(NULLIF(BTRIM(p.position), ''), 'F') AS position_raw,
  s.start_sec::int AS start_sec,
  s.end_sec::int AS end_sec
FROM nhl.shiftcharts_shifts s
JOIN nhl.games g USING (game_id)
LEFT JOIN nhl.players p
  ON p.player_id = s.player_id
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND s.start_sec IS NOT NULL
  AND s.end_sec IS NOT NULL
ORDER BY g.game_date, s.game_id, s.team_id, s.player_id, s.start_sec
"""

SHOT_EVENT_SQL = """
SELECT
  g.game_date::date AS game_date,
  e.game_id::bigint AS game_id,
  e.defending_team_id::bigint AS defending_team_id,
  e.shooting_player_id::bigint AS shooting_player_id,
  e.event_abs_sec::int AS event_abs_sec
FROM nhl.shot_on_goal_events e
JOIN nhl.games g
  ON g.game_id = e.game_id
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND e.shooting_player_id IS NOT NULL
  AND e.event_abs_sec IS NOT NULL
ORDER BY g.game_date, e.game_id, e.event_abs_sec
"""


def _fetch_df(sql: str, season: int, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    rows = pg_fetchall(sql, (season, from_date, from_date, to_date, to_date))
    return pd.DataFrame(rows or [])


def _interval_overlap(a: Tuple[int, int], b: Tuple[int, int]) -> int:
    start = max(int(a[0]), int(b[0]))
    end = min(int(a[1]), int(b[1]))
    return max(0, end - start)


def _build_game_structures(
    shifts_df: pd.DataFrame,
) -> tuple[
    Dict[int, Dict[Tuple[int, int], List[Tuple[int, int]]]],
    Dict[int, Dict[int, Dict[Tuple[int, int], List[Tuple[int, int]]]]],
]:
    player_intervals_by_game: Dict[int, Dict[Tuple[int, int], List[Tuple[int, int]]]] = {}
    pair_intervals_by_game: Dict[int, Dict[int, Dict[Tuple[int, int], List[Tuple[int, int]]]]] = {}

    for game_id, game_shifts in shifts_df.groupby("game_id", sort=False):
        game_player_map: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
        def_by_team: Dict[int, Dict[int, List[Tuple[int, int]]]] = defaultdict(lambda: defaultdict(list))
        for row in game_shifts.itertuples(index=False):
            team_id = int(row.team_id)
            player_id = int(row.player_id)
            interval = (int(row.start_sec), int(row.end_sec))
            pos = _norm_pos(row.position_raw)
            if pos == "G":
                continue
            game_player_map.setdefault((team_id, player_id), []).append(interval)
            if pos == "D":
                def_by_team[team_id][player_id].append(interval)

        team_pairs: Dict[int, Dict[Tuple[int, int], List[Tuple[int, int]]]] = {}
        for team_id, defenders in def_by_team.items():
            pair_map: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
            defender_ids = sorted(defenders.keys())
            for d1, d2 in combinations(defender_ids, 2):
                intervals_1 = defenders[d1]
                intervals_2 = defenders[d2]
                i = j = 0
                overlaps: List[Tuple[int, int]] = []
                while i < len(intervals_1) and j < len(intervals_2):
                    a = intervals_1[i]
                    b = intervals_2[j]
                    start = max(a[0], b[0])
                    end = min(a[1], b[1])
                    if end > start:
                        overlaps.append((start, end))
                    if a[1] <= b[1]:
                        i += 1
                    else:
                        j += 1
                if overlaps:
                    pair_map[(d1, d2)] = overlaps
            team_pairs[team_id] = pair_map

        player_intervals_by_game[int(game_id)] = game_player_map
        pair_intervals_by_game[int(game_id)] = team_pairs

    return player_intervals_by_game, pair_intervals_by_game


def _build_pair_prior_rate_map(
    shifts_df: pd.DataFrame,
    shot_df: pd.DataFrame,
    bucket_map: Dict[Tuple[int, int], str],
) -> Dict[Tuple[int, int, int, str], float]:
    player_intervals_by_game, pair_intervals_by_game = _build_game_structures(shifts_df)

    pair_game_toi_rows: List[Dict[str, object]] = []
    for game_id, team_pairs in pair_intervals_by_game.items():
        game_date = None
        if not shifts_df.empty:
            match = shifts_df.loc[shifts_df["game_id"] == game_id, "game_date"]
            if not match.empty:
                game_date = str(match.iloc[0])
        for team_id, pair_map in team_pairs.items():
            for (d1, d2), intervals in pair_map.items():
                toi_minutes = sum(max(0, end - start) for start, end in intervals) / 60.0
                pair_game_toi_rows.append(
                    {
                        "game_date": game_date,
                        "game_id": int(game_id),
                        "defending_team_id": int(team_id),
                        "pair_a": int(d1),
                        "pair_b": int(d2),
                        "toi_minutes": float(toi_minutes),
                    }
                )

    pair_toi_df = pd.DataFrame(pair_game_toi_rows)
    if pair_toi_df.empty:
        return {}

    shot_rows: List[Dict[str, object]] = []
    for row in shot_df.itertuples(index=False):
        game_id = int(row.game_id)
        team_id = int(row.defending_team_id)
        shooter_id = int(row.shooting_player_id)
        bucket = bucket_map.get((game_id, shooter_id), "missing")
        if bucket == "missing":
            continue
        event_sec = int(row.event_abs_sec)
        pair_map = pair_intervals_by_game.get(game_id, {}).get(team_id, {})
        matched_pairs = []
        for pair_ids, intervals in pair_map.items():
            for interval in intervals:
                if interval[0] <= event_sec < interval[1]:
                    matched_pairs.append(pair_ids)
                    break
        if len(matched_pairs) != 1:
            continue
        pair_a, pair_b = matched_pairs[0]
        shot_rows.append(
            {
                "game_date": str(row.game_date),
                "game_id": game_id,
                "defending_team_id": team_id,
                "pair_a": int(pair_a),
                "pair_b": int(pair_b),
                "shooter_bucket": str(bucket),
            }
        )

    shot_events_df = pd.DataFrame(shot_rows)
    if shot_events_df.empty:
        return {}
    shot_events_df = (
        shot_events_df.groupby(["game_date", "game_id", "defending_team_id", "pair_a", "pair_b", "shooter_bucket"], as_index=False)
        .size()
        .rename(columns={"size": "sog_allowed"})
    )

    buckets = sorted(set(shot_events_df["shooter_bucket"].astype(str).unique().tolist()) | {"<1.5", "1.5-2.5", "2.5-3.5", "3.5+"})
    frames: List[pd.DataFrame] = []
    for bucket in buckets:
        tmp = pair_toi_df.copy()
        tmp["shooter_bucket"] = bucket
        frames.append(tmp)
    full = pd.concat(frames, ignore_index=True)
    full = full.merge(
        shot_events_df,
        on=["game_date", "game_id", "defending_team_id", "pair_a", "pair_b", "shooter_bucket"],
        how="left",
    )
    full["sog_allowed"] = pd.to_numeric(full["sog_allowed"], errors="coerce").fillna(0.0)
    full["toi_minutes"] = pd.to_numeric(full["toi_minutes"], errors="coerce").fillna(0.0)
    full = full.sort_values(["pair_a", "pair_b", "shooter_bucket", "game_date", "game_id"])

    out: Dict[Tuple[int, int, int, str], float] = {}
    for (pair_a, pair_b, bucket), grp in full.groupby(["pair_a", "pair_b", "shooter_bucket"], sort=False):
        last10: Deque[Tuple[float, float]] = deque(maxlen=10)
        for row in grp.itertuples(index=False):
            key = (int(row.game_id), int(pair_a), int(pair_b), str(bucket))
            if last10:
                sog_sum = sum(v[0] for v in last10)
                toi_sum = sum(v[1] for v in last10)
                out[key] = (sog_sum * 60.0 / toi_sum) if toi_sum > 0 else math.nan
            else:
                out[key] = math.nan
            last10.append((float(row.sog_allowed or 0.0), float(row.toi_minutes or 0.0)))
    return out


def _build_test_pair_overlap_map(
    test_shifts: pd.DataFrame,
    test_rows: pd.DataFrame,
) -> Dict[Tuple[int, int], Dict[Tuple[int, int], int]]:
    player_intervals_by_game, pair_intervals_by_game = _build_game_structures(test_shifts)
    rows_needed = (
        test_rows[["game_id", "player_id", "opponent_id"]]
        .drop_duplicates()
        .sort_values(["game_id", "player_id"])
    )
    out: Dict[Tuple[int, int], Dict[Tuple[int, int], int]] = {}
    for row in rows_needed.itertuples(index=False):
        game_id = int(row.game_id)
        player_id = int(row.player_id)
        opponent_id = int(row.opponent_id)
        p_intervals = player_intervals_by_game.get(game_id, {}).get((int(test_rows.loc[(test_rows["game_id"] == game_id) & (test_rows["player_id"] == player_id), "team_id"].iloc[0]), player_id))
        if not p_intervals:
            continue
        pair_map = pair_intervals_by_game.get(game_id, {}).get(opponent_id, {})
        overlaps: Dict[Tuple[int, int], int] = {}
        for pair_ids, intervals in pair_map.items():
            sec = 0
            for p_int in p_intervals:
                for pair_int in intervals:
                    sec += _interval_overlap(p_int, pair_int)
            if sec > 0:
                overlaps[pair_ids] = sec
        out[(game_id, player_id)] = overlaps
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Evaluate exact on-ice opposing defense-pair deployment using shooter expected-SOG archetype buckets."
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

    shifts_df = _fetch_df(SHIFT_ROWS_SQL, args.season, args.from_date, args.to_date)
    shot_df = _fetch_df(SHOT_EVENT_SQL, args.season, args.from_date, args.to_date)
    if shifts_df.empty or shot_df.empty:
        raise SystemExit("Missing shiftcharts or shot event data for the requested window.")

    pair_prior_map = _build_pair_prior_rate_map(shifts_df, shot_df, bucket_map)
    test_start = min(test_dates)
    test_end = max(test_dates)
    test_shift_rows = pg_fetchall(TEST_SHIFTS_SQL, (args.season, test_start, test_end))
    test_shifts = pd.DataFrame(test_shift_rows or [])
    pair_overlap_map = _build_test_pair_overlap_map(test_shifts, test)

    scored = test.copy()
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)

    matchup_rates: List[float] = []
    matched_pair_counts: List[int] = []
    overlap_minutes: List[float] = []

    for row in scored.itertuples(index=False):
        key = (int(row.game_id), int(row.player_id))
        pair_secs = pair_overlap_map.get(key, {})
        shooter_bucket = str(row.expected_sog_bucket)
        weighted_num = 0.0
        weighted_den = 0.0
        matched = 0
        total_sec = 0
        for (pair_a, pair_b), sec in pair_secs.items():
            rate = pair_prior_map.get((int(row.game_id), int(pair_a), int(pair_b), shooter_bucket), math.nan)
            total_sec += int(sec)
            if rate is None or not math.isfinite(rate):
                continue
            weighted_num += float(rate) * float(sec)
            weighted_den += float(sec)
            matched += 1
        matchup_rates.append((weighted_num / weighted_den) if weighted_den > 0 else math.nan)
        matched_pair_counts.append(matched)
        overlap_minutes.append(total_sec / 60.0)

    scored["matchup_pair_archetype_allowed_d10_per60"] = matchup_rates
    scored["matched_pair_count"] = matched_pair_counts
    scored["matchup_overlap_minutes"] = overlap_minutes
    scored["lambda_defense_pair_archetype"] = (
        pd.to_numeric(scored["matchup_pair_archetype_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
        * scored["d10_toi_min_avg"]
        / 60.0
    ).clip(lower=0.0)

    both = (scored["lambda_offense"] > 0) & (scored["lambda_defense_pair_archetype"] > 0)
    scored["lambda_combined_pair_archetype"] = scored["lambda_offense"]
    scored.loc[both, "lambda_combined_pair_archetype"] = (
        (scored.loc[both, "rate_offense"] * scored.loc[both, "matchup_pair_archetype_allowed_d10_per60"]) ** 0.5
        * scored.loc[both, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both) & (scored["lambda_defense_pair_archetype"] > 0), "lambda_combined_pair_archetype"] = scored.loc[
        (~both) & (scored["lambda_defense_pair_archetype"] > 0), "lambda_defense_pair_archetype"
    ]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("defense_pair_archetype", "lambda_defense_pair_archetype"),
        ("combined_pair_archetype", "lambda_combined_pair_archetype"),
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
            "shot_rows": int(len(shot_df)),
            "rows_with_matchup_rate": int(pd.to_numeric(scored["matchup_pair_archetype_allowed_d10_per60"], errors="coerce").notna().sum()),
            "avg_matched_pairs": _round(pd.to_numeric(scored["matched_pair_count"], errors="coerce").mean()),
            "avg_overlap_minutes": _round(pd.to_numeric(scored["matchup_overlap_minutes"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_pair_archetype": _combined_metric(scored, "defense_pair_archetype"),
            "combined_pair_archetype": _combined_metric(scored, "combined_pair_archetype"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "defense_pair_archetype": _metric_rows(scored, f"p_defense_pair_archetype_over_{str(line).replace('.', '_')}", threshold),
            "combined_pair_archetype": _metric_rows(scored, f"p_combined_pair_archetype_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
