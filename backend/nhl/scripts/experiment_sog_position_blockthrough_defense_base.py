#!/usr/bin/env python3
"""Evaluate a blocks-through defensive base against the NHL SOG Poisson offense base."""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from typing import Any, Deque, Dict, List, Tuple

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
  COALESCE(l.toi_minutes, 0)::float8 AS toi_minutes,
  COALESCE(l.blocks, 0)::float8 AS blocks
FROM nhl.skater_game_logs_raw l
JOIN nhl.games g USING (game_id)
LEFT JOIN nhl.players p
  ON p.player_id = l.player_id
WHERE g.season = %s
  AND (%s::date IS NULL OR g.game_date >= %s::date)
  AND (%s::date IS NULL OR g.game_date <= %s::date)
  AND l.shots_on_goal IS NOT NULL
  AND COALESCE(l.toi_minutes, 0) > 0
ORDER BY g.game_date, l.game_id, l.player_id
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


def _split_df(df: pd.DataFrame, test_game_days: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str], list[str]]:
    dates = sorted(str(d) for d in pd.Series(df["game_date"]).dropna().astype(str).unique().tolist())
    if len(dates) <= test_game_days:
        raise ValueError(f"Need more than {test_game_days} distinct game dates; found {len(dates)}.")
    test_dates = dates[-test_game_days:]
    train_dates = dates[:-test_game_days]
    train = df[df["game_date"].astype(str).isin(train_dates)].copy()
    test = df[df["game_date"].astype(str).isin(test_dates)].copy()
    return train, test, train_dates, test_dates


def _fetch_raw_logs(season: int, from_date: str | None, to_date: str | None) -> pd.DataFrame:
    rows = pg_fetchall(RAW_SQL, (season, from_date, from_date, to_date, to_date))
    return pd.DataFrame(rows or [])


def _build_position_allowed_per60_map(raw: pd.DataFrame) -> Dict[Tuple[int, int, str], float]:
    allowed = (
        raw.groupby(["game_date", "game_id", "opponent_id", "position_bucket"], as_index=False)
        .agg(
            pos_sog_allowed=("shots_on_goal", "sum"),
            pos_toi_faced=("toi_minutes", "sum"),
        )
        .rename(columns={"opponent_id": "defending_team_id"})
    )
    allowed = allowed.sort_values(["defending_team_id", "position_bucket", "game_date", "game_id"])

    out: Dict[Tuple[int, int, str], float] = {}
    for (team_id, pos_bucket), grp in allowed.groupby(["defending_team_id", "position_bucket"], sort=False):
        last10: Deque[Tuple[float, float]] = deque(maxlen=10)
        for row in grp.itertuples(index=False):
            if last10:
                sog_sum = sum(v[0] for v in last10)
                toi_sum = sum(v[1] for v in last10)
                out[(int(row.game_id), int(team_id), str(pos_bucket))] = (sog_sum * 60.0 / toi_sum) if toi_sum > 0 else math.nan
            else:
                out[(int(row.game_id), int(team_id), str(pos_bucket))] = math.nan
            last10.append((float(row.pos_sog_allowed or 0.0), float(row.pos_toi_faced or 0.0)))
    return out


def _build_block_rate_maps(raw: pd.DataFrame) -> tuple[Dict[Tuple[int, int], float], Dict[Tuple[int, int], float]]:
    blocks = (
        raw.groupby(["game_date", "game_id", "team_id", "position_bucket"], as_index=False)
        .agg(
            pos_blocks=("blocks", "sum"),
            pos_toi=("toi_minutes", "sum"),
        )
        .rename(columns={"team_id": "defending_team_id"})
    )
    team_toi = (
        raw.groupby(["game_date", "game_id", "team_id"], as_index=False)
        .agg(team_toi=("toi_minutes", "sum"))
        .rename(columns={"team_id": "defending_team_id"})
    )
    blocks = blocks.merge(team_toi, on=["game_date", "game_id", "defending_team_id"], how="left")
    blocks = blocks.sort_values(["defending_team_id", "position_bucket", "game_date", "game_id"])

    d_blocks: Dict[Tuple[int, int], float] = {}
    all_blocks: Dict[Tuple[int, int], float] = {}

    # Defensemen-only rolling block rate measured against total team TOI exposure.
    d_rows = blocks[blocks["position_bucket"] == "D"].copy()
    for team_id, grp in d_rows.groupby("defending_team_id", sort=False):
        last10: Deque[Tuple[float, float]] = deque(maxlen=10)
        for row in grp.itertuples(index=False):
            if last10:
                blk_sum = sum(v[0] for v in last10)
                toi_sum = sum(v[1] for v in last10)
                d_blocks[(int(row.game_id), int(team_id))] = (blk_sum * 60.0 / toi_sum) if toi_sum > 0 else math.nan
            else:
                d_blocks[(int(row.game_id), int(team_id))] = math.nan
            last10.append((float(row.pos_blocks or 0.0), float(row.team_toi or 0.0)))

    team_rows = (
        raw.groupby(["game_date", "game_id", "team_id"], as_index=False)
        .agg(
            team_blocks=("blocks", "sum"),
            team_toi=("toi_minutes", "sum"),
        )
        .rename(columns={"team_id": "defending_team_id"})
        .sort_values(["defending_team_id", "game_date", "game_id"])
    )
    for team_id, grp in team_rows.groupby("defending_team_id", sort=False):
        last10: Deque[Tuple[float, float]] = deque(maxlen=10)
        for row in grp.itertuples(index=False):
            if last10:
                blk_sum = sum(v[0] for v in last10)
                toi_sum = sum(v[1] for v in last10)
                all_blocks[(int(row.game_id), int(team_id))] = (blk_sum * 60.0 / toi_sum) if toi_sum > 0 else math.nan
            else:
                all_blocks[(int(row.game_id), int(team_id))] = math.nan
            last10.append((float(row.team_blocks or 0.0), float(row.team_toi or 0.0)))

    return d_blocks, all_blocks


def _through_factor(sog_allowed_per60: pd.Series, blocks_per60: pd.Series, league_rate: float | None) -> pd.Series:
    sog = pd.to_numeric(sog_allowed_per60, errors="coerce")
    blk = pd.to_numeric(blocks_per60, errors="coerce")
    if league_rate is None or not math.isfinite(league_rate) or league_rate <= 0:
        return pd.Series(1.0, index=sog.index, dtype=float)
    denom = sog + blk
    netthrough = (sog / denom).where((sog > 0) & (denom > 0), other=math.nan)
    factor = (netthrough / float(league_rate)).where(netthrough.notna(), other=1.0)
    return factor.clip(lower=0.75, upper=1.25)


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate a blocks-through position defense base against the NHL SOG Poisson offense base.")
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
    if raw.empty:
        raise SystemExit("No raw log rows available for the requested season/date range.")

    pos_allowed_per60_map = _build_position_allowed_per60_map(raw)
    d_blocks_per60_map, all_blocks_per60_map = _build_block_rate_maps(raw)

    train, test, train_dates, test_dates = _split_df(df, args.test_game_days)

    scored = test.copy()
    scored["position_bucket"] = scored["position_raw"].fillna("F").astype(str).apply(lambda v: "D" if v == "D" else "F")
    scored["lambda_offense"] = pd.to_numeric(scored["lambda_base"], errors="coerce").clip(lower=0.0)
    scored["rate_offense"] = pd.to_numeric(scored["d10_sog_per60"], errors="coerce").clip(lower=0.0)
    scored["d10_toi_min_avg"] = pd.to_numeric(scored["d10_toi_min_avg"], errors="coerce").clip(lower=0.0)
    scored["opp_pos_allowed_d10_per60"] = scored.apply(
        lambda row: pos_allowed_per60_map.get((int(row["game_id"]), int(row["opponent_id"]), str(row["position_bucket"])), math.nan),
        axis=1,
    )
    scored["opp_d_blocks_d10_per60"] = scored.apply(
        lambda row: d_blocks_per60_map.get((int(row["game_id"]), int(row["opponent_id"])), math.nan),
        axis=1,
    )
    scored["opp_all_blocks_d10_per60"] = scored.apply(
        lambda row: all_blocks_per60_map.get((int(row["game_id"]), int(row["opponent_id"])), math.nan),
        axis=1,
    )

    # League baseline uses the same through-rate construction as the per-row factors.
    train_raw = raw[raw["game_date"].astype(str).isin(train_dates)].copy()
    train_allowed = (
        train_raw.groupby(["game_date", "game_id", "opponent_id", "position_bucket"], as_index=False)
        .agg(pos_sog_allowed=("shots_on_goal", "sum"))
        .rename(columns={"opponent_id": "defending_team_id"})
    )
    train_d = (
        train_raw[train_raw["position_bucket"] == "D"]
        .groupby(["game_date", "game_id", "team_id"], as_index=False)
        .agg(d_blocks=("blocks", "sum"))
        .rename(columns={"team_id": "defending_team_id"})
    )
    train_all = (
        train_raw.groupby(["game_date", "game_id", "team_id"], as_index=False)
        .agg(team_blocks=("blocks", "sum"))
        .rename(columns={"team_id": "defending_team_id"})
    )
    train_join = train_allowed.merge(train_d, on=["game_date", "game_id", "defending_team_id"], how="left").merge(
        train_all, on=["game_date", "game_id", "defending_team_id"], how="left"
    )
    league_d_through = float(
        train_join["pos_sog_allowed"].fillna(0.0).sum()
        / max(1.0, (train_join["pos_sog_allowed"].fillna(0.0) + train_join["d_blocks"].fillna(0.0)).sum())
    )
    league_all_through = float(
        train_join["pos_sog_allowed"].fillna(0.0).sum()
        / max(1.0, (train_join["pos_sog_allowed"].fillna(0.0) + train_join["team_blocks"].fillna(0.0)).sum())
    )

    scored["d_blockthrough_factor"] = _through_factor(
        scored["opp_pos_allowed_d10_per60"],
        scored["opp_d_blocks_d10_per60"],
        league_d_through,
    )
    scored["all_blockthrough_factor"] = _through_factor(
        scored["opp_pos_allowed_d10_per60"],
        scored["opp_all_blocks_d10_per60"],
        league_all_through,
    )

    scored["rate_defense_pos"] = pd.to_numeric(scored["opp_pos_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
    scored["rate_defense_pos_dblocks"] = (scored["rate_defense_pos"] * scored["d_blockthrough_factor"]).clip(lower=0.0)
    scored["rate_defense_pos_allblocks"] = (scored["rate_defense_pos"] * scored["all_blockthrough_factor"]).clip(lower=0.0)

    for suffix, rate_col in [
        ("pos", "rate_defense_pos"),
        ("pos_dblocks", "rate_defense_pos_dblocks"),
        ("pos_allblocks", "rate_defense_pos_allblocks"),
    ]:
        lam_col = f"lambda_defense_{suffix}"
        scored[lam_col] = (scored[rate_col] * scored["d10_toi_min_avg"] / 60.0).clip(lower=0.0)

    for suffix, rate_col in [
        ("plain", "rate_defense_pos"),
        ("dblocks", "rate_defense_pos_dblocks"),
        ("allblocks", "rate_defense_pos_allblocks"),
    ]:
        lam_col = f"lambda_combined_{suffix}"
        def_lam = f"lambda_defense_pos" if suffix == "plain" else f"lambda_defense_{'pos_' + suffix}"
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
        "defense_pos": "lambda_defense_pos",
        "defense_pos_dblocks": "lambda_defense_pos_dblocks",
        "defense_pos_allblocks": "lambda_defense_pos_allblocks",
        "combined_plain": "lambda_combined_plain",
        "combined_dblocks": "lambda_combined_dblocks",
        "combined_allblocks": "lambda_combined_allblocks",
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
        "train_date_min": None if train.empty else str(train["game_date"].min()),
        "train_date_max": None if train.empty else str(train["game_date"].max()),
        "test_date_min": None if test.empty else str(test["game_date"].min()),
        "test_date_max": None if test.empty else str(test["game_date"].max()),
        "coverage": {
            "rows_with_opp_pos_allowed_per60": int(scored["opp_pos_allowed_d10_per60"].notna().sum()),
            "rows_with_opp_d_blocks_per60": int(scored["opp_d_blocks_d10_per60"].notna().sum()),
            "rows_with_opp_all_blocks_per60": int(scored["opp_all_blocks_d10_per60"].notna().sum()),
            "league_d_blockthrough": _round(league_d_through),
            "league_all_blockthrough": _round(league_all_through),
        },
        "overall": {
            prefix: _combined_metric(scored, prefix)
            for prefix in model_map
        },
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
