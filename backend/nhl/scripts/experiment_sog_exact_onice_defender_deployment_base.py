#!/usr/bin/env python3
"""Evaluate exact on-ice opposing-defender deployment as a defense-side NHL SOG base."""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.shared.db.pg import pg_fetchall


THRESHOLDS = {1.5: 2, 2.5: 3, 3.5: 4}
POSITION_BUCKETS = ("F", "D")

DEFENDER_TOI_SQL = """
SELECT
  g.game_date::date AS game_date,
  s.game_id::bigint AS game_id,
  s.team_id::bigint AS defending_team_id,
  s.player_id::bigint AS defender_id,
  SUM(s.dur_sec)::float8 / 60.0 AS toi_minutes
FROM nhl.shiftcharts_shifts s
JOIN nhl.games g USING (game_id)
JOIN nhl.players p
  ON p.player_id = s.player_id
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND COALESCE(NULLIF(BTRIM(p.position), ''), 'F') = 'D'
  AND s.start_sec IS NOT NULL
  AND s.end_sec IS NOT NULL
GROUP BY g.game_date, s.game_id, s.team_id, s.player_id
ORDER BY g.game_date, s.game_id, s.player_id
"""

DEFENDER_ONICE_SHOTS_SQL = """
SELECT
  g.game_date::date AS game_date,
  e.game_id::bigint AS game_id,
  e.defending_team_id::bigint AS defending_team_id,
  d.player_id::bigint AS defender_id,
  COALESCE(NULLIF(BTRIM(e.shooter_position_bucket), ''), 'F') AS shooter_position_bucket,
  COUNT(*)::int AS sog_allowed_onice
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
GROUP BY g.game_date, e.game_id, e.defending_team_id, d.player_id, COALESCE(NULLIF(BTRIM(e.shooter_position_bucket), ''), 'F')
ORDER BY g.game_date, e.game_id, d.player_id
"""

TEST_SHIFTS_SQL = """
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
  AND g.game_date >= %s::date
  AND g.game_date <= %s::date
  AND s.start_sec IS NOT NULL
  AND s.end_sec IS NOT NULL
ORDER BY g.game_id, s.team_id, s.player_id, s.start_sec
"""


def _round(v: float | None, digits: int = 4) -> float | None:
    if v is None:
        return None
    return round(float(v), digits)


def _poisson_tail(lam: float, threshold: int) -> float:
    if not math.isfinite(lam) or lam < 0:
        return float("nan")
    cutoff = max(0, threshold - 1)
    cdf = 0.0
    for k in range(cutoff + 1):
        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _metric_rows(df: pd.DataFrame, prob_col: str, threshold: int) -> Dict[str, Any]:
    if df.empty:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
    probs = pd.to_numeric(df[prob_col], errors="coerce")
    ys = (pd.to_numeric(df["shots_on_goal"], errors="coerce") >= threshold).astype(int)
    mask = probs.notna() & ys.notna()
    probs = probs[mask].astype(float)
    ys = ys[mask].astype(int)
    n = int(len(probs))
    if n == 0:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
    avg_p = float(probs.mean())
    hit_rate = float(ys.mean())
    brier = float(((probs - ys) ** 2).mean())
    return {
        "n": n,
        "avg_p": _round(avg_p),
        "hit_rate": _round(hit_rate),
        "gap": _round(avg_p - hit_rate),
        "brier": _round(brier),
    }


def _combined_metric(scored: pd.DataFrame, kind: str) -> Dict[str, Any]:
    probs = pd.concat(
        [scored[f"p_{kind}_over_{str(line).replace('.', '_')}"] for line in THRESHOLDS],
        ignore_index=True,
    )
    ys = pd.concat(
        [
            (pd.to_numeric(scored["shots_on_goal"], errors="coerce") >= threshold).astype(int)
            for threshold in THRESHOLDS.values()
        ],
        ignore_index=True,
    )
    return {
        "n": int(len(probs)),
        "avg_p": _round(float(probs.mean())),
        "hit_rate": _round(float(ys.mean())),
        "gap": _round(float(probs.mean() - ys.mean())),
        "brier": _round(float(((probs - ys) ** 2).mean())),
    }


def _fetch_df(sql: str, season: int, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    rows = pg_fetchall(sql, (season, from_date, from_date, to_date, to_date))
    return pd.DataFrame(rows or [])


def _split_df(df: pd.DataFrame, test_game_days: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    dates = sorted(str(d) for d in pd.Series(df["game_date"]).dropna().astype(str).unique().tolist())
    if len(dates) <= test_game_days:
        raise ValueError(f"Need more than {test_game_days} distinct game dates; found {len(dates)}.")
    test_dates = dates[-test_game_days:]
    train_dates = dates[:-test_game_days]
    train = df[df["game_date"].astype(str).isin(train_dates)].copy()
    test = df[df["game_date"].astype(str).isin(test_dates)].copy()
    return train, test, train_dates, test_dates


def _build_defender_prior_rate_map(
    toi_df: pd.DataFrame,
    shots_df: pd.DataFrame,
) -> Dict[Tuple[int, int, str], float]:
    if toi_df.empty:
        return {}
    base = toi_df.copy()
    frames: List[pd.DataFrame] = []
    for bucket in POSITION_BUCKETS:
        tmp = base.copy()
        tmp["shooter_position_bucket"] = bucket
        frames.append(tmp)
    full = pd.concat(frames, ignore_index=True)
    full = full.merge(
        shots_df,
        on=["game_date", "game_id", "defending_team_id", "defender_id", "shooter_position_bucket"],
        how="left",
    )
    full["sog_allowed_onice"] = pd.to_numeric(full["sog_allowed_onice"], errors="coerce").fillna(0.0)
    full["toi_minutes"] = pd.to_numeric(full["toi_minutes"], errors="coerce").fillna(0.0)
    full = full.sort_values(["defender_id", "shooter_position_bucket", "game_date", "game_id"])

    out: Dict[Tuple[int, int, str], float] = {}
    for (defender_id, shooter_bucket), grp in full.groupby(["defender_id", "shooter_position_bucket"], sort=False):
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


def _norm_pos(raw: str | None) -> str:
    return "D" if str(raw or "").strip() == "D" else "F"


def _overlap_seconds(a: Iterable[Tuple[int, int]], b: Iterable[Tuple[int, int]]) -> int:
    la = list(a)
    lb = list(b)
    i = 0
    j = 0
    total = 0
    while i < len(la) and j < len(lb):
        a_start, a_end = la[i]
        b_start, b_end = lb[j]
        start = max(a_start, b_start)
        end = min(a_end, b_end)
        if end > start:
            total += end - start
        if a_end <= b_end:
            i += 1
        else:
            j += 1
    return total


def _build_test_overlap_map(
    shifts_df: pd.DataFrame,
    test_rows: pd.DataFrame,
) -> Dict[Tuple[int, int], Dict[int, int]]:
    out: Dict[Tuple[int, int], Dict[int, int]] = {}
    if shifts_df.empty or test_rows.empty:
        return out

    rows_needed = (
        test_rows[["game_id", "player_id", "opponent_id"]]
        .drop_duplicates()
        .sort_values(["game_id", "player_id"])
    )
    game_to_requests: Dict[int, List[Tuple[int, int]]] = {}
    for row in rows_needed.itertuples(index=False):
        game_to_requests.setdefault(int(row.game_id), []).append((int(row.player_id), int(row.opponent_id)))

    for game_id, game_shifts in shifts_df.groupby("game_id", sort=False):
        reqs = game_to_requests.get(int(game_id))
        if not reqs:
            continue
        player_intervals: Dict[Tuple[int, int], List[Tuple[int, int]]] = {}
        defender_intervals_by_team: Dict[int, Dict[int, List[Tuple[int, int]]]] = {}
        for row in game_shifts.itertuples(index=False):
            team_id = int(row.team_id)
            player_id = int(row.player_id)
            interval = (int(row.start_sec), int(row.end_sec))
            pos = _norm_pos(row.position_raw)
            if pos != "G":
                player_intervals.setdefault((team_id, player_id), []).append(interval)
            if pos == "D":
                defender_intervals_by_team.setdefault(team_id, {}).setdefault(player_id, []).append(interval)

        for player_id, opponent_id in reqs:
            p_key = None
            for (team_id, pid) in player_intervals.keys():
                if pid == player_id:
                    p_key = (team_id, pid)
                    break
            if p_key is None:
                continue
            p_intervals = player_intervals[p_key]
            defenders = defender_intervals_by_team.get(int(opponent_id), {})
            if not defenders:
                continue
            defender_secs: Dict[int, int] = {}
            for defender_id, d_intervals in defenders.items():
                sec = _overlap_seconds(p_intervals, d_intervals)
                if sec > 0:
                    defender_secs[int(defender_id)] = int(sec)
            if defender_secs:
                out[(int(game_id), int(player_id))] = defender_secs
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Evaluate exact on-ice opposing-defender deployment as a defense-side NHL SOG base."
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

    toi_df = _fetch_df(DEFENDER_TOI_SQL, args.season, args.from_date, args.to_date)
    shots_df = _fetch_df(DEFENDER_ONICE_SHOTS_SQL, args.season, args.from_date, args.to_date)
    if toi_df.empty or shots_df.empty:
        raise SystemExit("Missing defender TOI or exact on-ice shot-event data for the requested window.")

    defender_prior_map = _build_defender_prior_rate_map(toi_df, shots_df)
    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)
    test_start = min(test_dates)
    test_end = max(test_dates)
    shifts_rows = pg_fetchall(TEST_SHIFTS_SQL, (args.season, test_start, test_end))
    shifts_df = pd.DataFrame(shifts_rows or [])
    overlap_map = _build_test_overlap_map(shifts_df, test)

    scored = test.copy()
    scored["position_bucket"] = scored["position_raw"].fillna("F").astype(str).apply(_norm_pos)
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)

    matchup_rates: List[float] = []
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
        matchup_rates.append((weighted_num / weighted_den) if weighted_den > 0 else math.nan)
        matched_defender_counts.append(matched)
        overlap_minutes.append(total_sec / 60.0)

    scored["matchup_defender_allowed_d10_per60"] = matchup_rates
    scored["matched_defender_count"] = matched_defender_counts
    scored["matchup_overlap_minutes"] = overlap_minutes
    scored["lambda_defense_matchup"] = (
        pd.to_numeric(scored["matchup_defender_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
        * scored["d10_toi_min_avg"]
        / 60.0
    ).clip(lower=0.0)

    both = (scored["lambda_offense"] > 0) & (scored["lambda_defense_matchup"] > 0)
    scored["lambda_combined_matchup"] = scored["lambda_offense"]
    scored.loc[both, "lambda_combined_matchup"] = (
        (scored.loc[both, "rate_offense"] * scored.loc[both, "matchup_defender_allowed_d10_per60"]) ** 0.5
        * scored.loc[both, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both) & (scored["lambda_defense_matchup"] > 0), "lambda_combined_matchup"] = scored.loc[
        (~both) & (scored["lambda_defense_matchup"] > 0), "lambda_defense_matchup"
    ]

    for kind, lam_col in [
        ("offense", "lambda_offense"),
        ("defense_matchup", "lambda_defense_matchup"),
        ("combined_matchup", "lambda_combined_matchup"),
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
            "defender_prior_rows": int(len(toi_df)),
            "onice_shot_rows": int(len(shots_df)),
            "test_shift_rows": int(len(shifts_df)),
            "rows_with_matchup_rate": int(pd.to_numeric(scored["matchup_defender_allowed_d10_per60"], errors="coerce").notna().sum()),
            "avg_matched_defenders": _round(pd.to_numeric(scored["matched_defender_count"], errors="coerce").mean()),
            "avg_overlap_minutes": _round(pd.to_numeric(scored["matchup_overlap_minutes"], errors="coerce").mean()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_matchup": _combined_metric(scored, "defense_matchup"),
            "combined_matchup": _combined_metric(scored, "combined_matchup"),
        },
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        key = str(line)
        summary["by_line"][key] = {
            "offense": _metric_rows(scored, f"p_offense_over_{str(line).replace('.', '_')}", threshold),
            "defense_matchup": _metric_rows(scored, f"p_defense_matchup_over_{str(line).replace('.', '_')}", threshold),
            "combined_matchup": _metric_rows(scored, f"p_combined_matchup_over_{str(line).replace('.', '_')}", threshold),
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        summary["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
