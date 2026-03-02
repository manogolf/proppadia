#!/usr/bin/env python3
"""Evaluate an attempts -> blocks -> on-net -> goalie position defense base against the NHL SOG Poisson offense base."""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from typing import Any, Deque, Dict, Tuple

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.shared.db.pg import pg_fetchall


THRESHOLDS = {1.5: 2, 2.5: 3, 3.5: 4}

RAW_SQL = """
SELECT
  g.game_date::date AS game_date,
  g.season::int AS season,
  l.game_id::bigint AS game_id,
  l.player_id::bigint AS player_id,
  l.team_id::bigint AS team_id,
  l.opponent_id::bigint AS opponent_id,
  CASE
    WHEN COALESCE(NULLIF(BTRIM(p.position), ''), 'F') = 'D' THEN 'D'
    ELSE 'F'
  END AS position_bucket,
  l.shots_on_goal::int AS shots_on_goal,
  l.shot_attempts::int AS shot_attempts,
  COALESCE(l.toi_minutes, 0)::float8 AS toi_minutes
FROM nhl.skater_game_logs_raw l
JOIN nhl.games g USING (game_id)
LEFT JOIN nhl.players p
  ON p.player_id = l.player_id
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND l.shots_on_goal IS NOT NULL
  AND l.shot_attempts IS NOT NULL
  AND COALESCE(l.toi_minutes, 0) > 0
ORDER BY g.game_date, l.game_id, l.player_id
"""

BLOCK_SQL = """
SELECT
  game_date::date AS game_date,
  season::int AS season,
  game_id::bigint AS game_id,
  blocking_team_id::int AS defending_team_id,
  COALESCE(NULLIF(BTRIM(shooter_position_bucket), ''), 'F') AS shooter_position_bucket,
  COALESCE(NULLIF(BTRIM(blocker_position_bucket), ''), 'F') AS blocker_position_bucket
FROM nhl.blocked_shot_events
WHERE season = %s
  AND (%s::date IS NULL OR game_date >= %s::date)
  AND (%s::date IS NULL OR game_date <= %s::date)
ORDER BY game_date, game_id, defending_team_id
"""

GOALIE_START_SQL = """
SELECT
  g.game_date::date AS game_date,
  g.season::int AS season,
  gl.game_id::bigint AS game_id,
  gl.player_id::bigint AS goalie_id,
  gl.team_id::bigint AS defending_team_id
FROM nhl.goalie_game_logs_raw gl
JOIN nhl.games g USING (game_id)
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND gl.start_flag IS TRUE
  AND COALESCE(gl.toi_minutes, 0) > 0
ORDER BY g.game_date, gl.game_id, gl.player_id
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


def _split_df(df: pd.DataFrame, test_game_days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = sorted(str(d) for d in pd.Series(df["game_date"]).dropna().astype(str).unique().tolist())
    if len(dates) <= test_game_days:
        raise ValueError(f"Need more than {test_game_days} distinct game dates; found {len(dates)}.")
    test_dates = dates[-test_game_days:]
    train_dates = dates[:-test_game_days]
    train = df[df["game_date"].astype(str).isin(train_dates)].copy()
    test = df[df["game_date"].astype(str).isin(test_dates)].copy()
    return train, test


def _fetch_raw_logs(season: int, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    rows = pg_fetchall(RAW_SQL, (season, from_date, from_date, to_date, to_date))
    return pd.DataFrame(rows or [])


def _fetch_block_events(season: int, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    rows = pg_fetchall(BLOCK_SQL, (season, from_date, from_date, to_date, to_date))
    return pd.DataFrame(rows or [])


def _fetch_goalie_starts(season: int, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    rows = pg_fetchall(GOALIE_START_SQL, (season, from_date, from_date, to_date, to_date))
    return pd.DataFrame(rows or [])


def _build_maps(
    raw: pd.DataFrame,
    blocked: pd.DataFrame,
    starts: pd.DataFrame,
) -> tuple[
    Dict[Tuple[int, int, str], float],
    Dict[Tuple[int, int, str], float],
    Dict[Tuple[int, int, str], float],
    Dict[Tuple[int, int, str], float],
    Dict[Tuple[int, int, str], float],
    Dict[Tuple[int, int], int],
]:
    allowed = (
        raw.groupby(["game_date", "game_id", "opponent_id", "position_bucket"], as_index=False)
        .agg(
            pos_sog_allowed=("shots_on_goal", "sum"),
            pos_attempts_allowed=("shot_attempts", "sum"),
            pos_toi_faced=("toi_minutes", "sum"),
        )
        .rename(columns={"opponent_id": "defending_team_id", "position_bucket": "shooter_position_bucket"})
    )

    blocked_all = (
        blocked.groupby(["game_date", "game_id", "defending_team_id", "shooter_position_bucket"], as_index=False)
        .agg(blocked_all=("game_id", "size"))
    )
    blocked_d = (
        blocked[blocked["blocker_position_bucket"] == "D"]
        .groupby(["game_date", "game_id", "defending_team_id", "shooter_position_bucket"], as_index=False)
        .agg(blocked_d=("game_id", "size"))
    )

    frame = (
        allowed.merge(
            blocked_all,
            on=["game_date", "game_id", "defending_team_id", "shooter_position_bucket"],
            how="left",
        )
        .merge(
            blocked_d,
            on=["game_date", "game_id", "defending_team_id", "shooter_position_bucket"],
            how="left",
        )
        .fillna({"blocked_all": 0.0, "blocked_d": 0.0})
        .sort_values(["defending_team_id", "shooter_position_bucket", "game_date", "game_id"])
    )

    attempts_per60_map: Dict[Tuple[int, int, str], float] = {}
    blockthrough_all_map: Dict[Tuple[int, int, str], float] = {}
    blockthrough_d_map: Dict[Tuple[int, int, str], float] = {}
    team_onnet_map: Dict[Tuple[int, int, str], float] = {}
    goalie_onnet_map: Dict[Tuple[int, int, str], float] = {}
    actual_starter_map: Dict[Tuple[int, int], int] = {}

    # Team baseline maps by position.
    for (team_id, shooter_pos), grp in frame.groupby(["defending_team_id", "shooter_position_bucket"], sort=False):
        last10: Deque[Tuple[float, float, float, float, float]] = deque(maxlen=10)
        for row in grp.itertuples(index=False):
            key = (int(row.game_id), int(team_id), str(shooter_pos))
            if last10:
                sog_sum = sum(v[0] for v in last10)
                attempts_sum = sum(v[1] for v in last10)
                blk_all_sum = sum(v[2] for v in last10)
                blk_d_sum = sum(v[3] for v in last10)
                toi_sum = sum(v[4] for v in last10)
                attempts_per60_map[key] = (attempts_sum * 60.0 / toi_sum) if toi_sum > 0 else math.nan
                blockthrough_all_map[key] = ((attempts_sum - blk_all_sum) / attempts_sum) if attempts_sum > 0 else math.nan
                blockthrough_d_map[key] = ((attempts_sum - blk_d_sum) / attempts_sum) if attempts_sum > 0 else math.nan
                unblocked_all = max(0.0, attempts_sum - blk_all_sum)
                team_onnet_map[key] = (sog_sum / unblocked_all) if unblocked_all > 0 else math.nan
            else:
                attempts_per60_map[key] = math.nan
                blockthrough_all_map[key] = math.nan
                blockthrough_d_map[key] = math.nan
                team_onnet_map[key] = math.nan
            last10.append(
                (
                    float(row.pos_sog_allowed or 0.0),
                    float(row.pos_attempts_allowed or 0.0),
                    float(row.blocked_all or 0.0),
                    float(row.blocked_d or 0.0),
                    float(row.pos_toi_faced or 0.0),
                )
            )

    for row in starts.itertuples(index=False):
        actual_starter_map[(int(row.game_id), int(row.defending_team_id))] = int(row.goalie_id)

    # Goalie on-net conversion after blocks by position.
    goalie_frame = (
        starts.merge(
            frame[[
                "game_id",
                "defending_team_id",
                "shooter_position_bucket",
                "pos_sog_allowed",
                "pos_attempts_allowed",
                "blocked_all",
                "pos_toi_faced",
            ]],
            on=["game_id", "defending_team_id"],
            how="left",
        )
        .sort_values(["goalie_id", "shooter_position_bucket", "game_date", "game_id"])
    )
    for (goalie_id, shooter_pos), grp in goalie_frame.groupby(["goalie_id", "shooter_position_bucket"], sort=False):
        last10: Deque[Tuple[float, float, float]] = deque(maxlen=10)
        for row in grp.itertuples(index=False):
            key = (int(row.game_id), int(goalie_id), str(shooter_pos))
            if last10:
                sog_sum = sum(v[0] for v in last10)
                att_sum = sum(v[1] for v in last10)
                blk_sum = sum(v[2] for v in last10)
                unblocked = max(0.0, att_sum - blk_sum)
                goalie_onnet_map[key] = (sog_sum / unblocked) if unblocked > 0 else math.nan
            else:
                goalie_onnet_map[key] = math.nan
            last10.append(
                (
                    float(row.pos_sog_allowed or 0.0),
                    float(row.pos_attempts_allowed or 0.0),
                    float(row.blocked_all or 0.0),
                )
            )

    return (
        attempts_per60_map,
        blockthrough_all_map,
        blockthrough_d_map,
        team_onnet_map,
        goalie_onnet_map,
        actual_starter_map,
    )


def _goalie_onnet_delta(goalie_onnet: pd.Series, team_onnet: pd.Series) -> pd.Series:
    g = pd.to_numeric(goalie_onnet, errors="coerce")
    t = pd.to_numeric(team_onnet, errors="coerce")
    ratio = (g / t).where(g.notna() & t.notna() & (t > 0), other=1.0)
    return ratio.clip(lower=0.75, upper=1.25)


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate an attempts->blocks->on-net->goalie position defense base against the NHL SOG Poisson offense base.")
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

    raw = _fetch_raw_logs(args.season, args.from_date, args.to_date)
    blocked = _fetch_block_events(args.season, args.from_date, args.to_date)
    starts = _fetch_goalie_starts(args.season, args.from_date, args.to_date)
    if raw.empty or blocked.empty or starts.empty:
        raise SystemExit("Required raw/block/goalie inputs are missing for the requested range.")

    (
        attempts_per60_map,
        blockthrough_all_map,
        blockthrough_d_map,
        team_onnet_map,
        goalie_onnet_map,
        actual_starter_map,
    ) = _build_maps(raw, blocked, starts)

    train, test = _split_df(df, args.test_game_days)
    scored = test.copy()
    scored["position_bucket"] = scored["position_raw"].fillna("F").astype(str).apply(lambda v: "D" if v == "D" else "F")
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)

    scored["opp_pos_attempts_d10_per60"] = scored.apply(
        lambda row: attempts_per60_map.get((int(row["game_id"]), int(row["opponent_id"]), str(row["position_bucket"])), math.nan),
        axis=1,
    )
    scored["opp_pos_blockthrough_all_d10"] = scored.apply(
        lambda row: blockthrough_all_map.get((int(row["game_id"]), int(row["opponent_id"]), str(row["position_bucket"])), math.nan),
        axis=1,
    )
    scored["opp_pos_blockthrough_d_d10"] = scored.apply(
        lambda row: blockthrough_d_map.get((int(row["game_id"]), int(row["opponent_id"]), str(row["position_bucket"])), math.nan),
        axis=1,
    )
    scored["opp_team_pos_onnet_d10"] = scored.apply(
        lambda row: team_onnet_map.get((int(row["game_id"]), int(row["opponent_id"]), str(row["position_bucket"])), math.nan),
        axis=1,
    )
    scored["actual_goalie_id"] = scored.apply(
        lambda row: actual_starter_map.get((int(row["game_id"]), int(row["opponent_id"])), math.nan),
        axis=1,
    )
    scored["goalie_pos_onnet_d10"] = scored.apply(
        lambda row: goalie_onnet_map.get((int(row["game_id"]), int(row["actual_goalie_id"]), str(row["position_bucket"])), math.nan)
        if pd.notna(row["actual_goalie_id"]) else math.nan,
        axis=1,
    )
    scored["goalie_onnet_delta"] = _goalie_onnet_delta(scored["goalie_pos_onnet_d10"], scored["opp_team_pos_onnet_d10"])

    scored["rate_defense_all"] = (
        pd.to_numeric(scored["opp_pos_attempts_d10_per60"], errors="coerce")
        * pd.to_numeric(scored["opp_pos_blockthrough_all_d10"], errors="coerce")
        * pd.to_numeric(scored["opp_team_pos_onnet_d10"], errors="coerce")
    ).clip(lower=0.0)
    scored["rate_defense_d"] = (
        pd.to_numeric(scored["opp_pos_attempts_d10_per60"], errors="coerce")
        * pd.to_numeric(scored["opp_pos_blockthrough_d_d10"], errors="coerce")
        * pd.to_numeric(scored["opp_team_pos_onnet_d10"], errors="coerce")
    ).clip(lower=0.0)
    scored["rate_defense_all_goalie"] = (scored["rate_defense_all"] * scored["goalie_onnet_delta"]).clip(lower=0.0)
    scored["rate_defense_d_goalie"] = (scored["rate_defense_d"] * scored["goalie_onnet_delta"]).clip(lower=0.0)

    for suffix, rate_col in [
        ("all", "rate_defense_all"),
        ("d", "rate_defense_d"),
        ("all_goalie", "rate_defense_all_goalie"),
        ("d_goalie", "rate_defense_d_goalie"),
    ]:
        lam_col = f"lambda_defense_{suffix}"
        scored[lam_col] = (scored[rate_col] * scored["d10_toi_min_avg"] / 60.0).clip(lower=0.0)

    for suffix, rate_col, def_lam in [
        ("all", "rate_defense_all", "lambda_defense_all"),
        ("d", "rate_defense_d", "lambda_defense_d"),
        ("all_goalie", "rate_defense_all_goalie", "lambda_defense_all_goalie"),
        ("d_goalie", "rate_defense_d_goalie", "lambda_defense_d_goalie"),
    ]:
        lam_col = f"lambda_combined_{suffix}"
        both = (scored["lambda_offense"] > 0) & (scored[def_lam] > 0)
        scored[lam_col] = scored["lambda_offense"]
        scored.loc[both, lam_col] = (
            (scored.loc[both, "rate_offense"] * scored.loc[both, rate_col]) ** 0.5
            * scored.loc[both, "d10_toi_min_avg"]
            / 60.0
        )
        scored.loc[(~both) & (scored[def_lam] > 0), lam_col] = scored.loc[(~both) & (scored[def_lam] > 0), def_lam]

    model_map = {
        "offense": "lambda_offense",
        "defense_all": "lambda_defense_all",
        "defense_d": "lambda_defense_d",
        "defense_all_goalie": "lambda_defense_all_goalie",
        "defense_d_goalie": "lambda_defense_d_goalie",
        "combined_all": "lambda_combined_all",
        "combined_d": "lambda_combined_d",
        "combined_all_goalie": "lambda_combined_all_goalie",
        "combined_d_goalie": "lambda_combined_d_goalie",
    }
    for prefix, lam_col in model_map.items():
        for line, threshold in THRESHOLDS.items():
            col = f"p_{prefix}_over_{str(line).replace('.', '_')}"
            scored[col] = scored[lam_col].apply(lambda lam: _poisson_tail(float(lam), threshold) if pd.notna(lam) else math.nan)

    out: Dict[str, Any] = {
        "ok": True,
        "season": args.season,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "coverage": {
            "rows_with_attempts_per60": int(scored["opp_pos_attempts_d10_per60"].notna().sum()),
            "rows_with_team_onnet": int(scored["opp_team_pos_onnet_d10"].notna().sum()),
            "rows_with_goalie_onnet": int(scored["goalie_pos_onnet_d10"].notna().sum()),
            "rows_with_actual_goalie": int(pd.to_numeric(scored["actual_goalie_id"], errors="coerce").notna().sum()),
        },
        "overall": {prefix: _combined_metric(scored, prefix) for prefix in model_map},
        "by_line": {},
    }
    for line, threshold in THRESHOLDS.items():
        line_key = str(line)
        out["by_line"][line_key] = {
            prefix: _metric_rows(scored, f"p_{prefix}_over_{line_key.replace('.', '_')}", threshold)
            for prefix in model_map
        }

    if args.write_scored_csv:
        scored.to_csv(args.write_scored_csv, index=False)
        out["write_scored_csv"] = args.write_scored_csv

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
