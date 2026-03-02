#!/usr/bin/env python3
"""Evaluate a position-group defense-side NHL SOG base against the Poisson offense base."""

from __future__ import annotations

import argparse
import json
import math
from collections import deque
from pathlib import Path
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
  COALESCE(l.toi_minutes, 0)::float8 AS toi_minutes
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


def _bucket_stats(df: pd.DataFrame, prob_col: str, threshold: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for bucket, group in sorted(df.groupby("expected_sog_bucket"), key=lambda item: item[0]):
        out.append(
            {
                "segment_value": bucket,
                "n": int(len(group)),
                prob_col: _metric_rows(group, prob_col, threshold),
            }
        )
    return out


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


def _safe_goalie_factor(series: pd.Series, league_avg: float | None) -> pd.Series:
    vals = pd.to_numeric(series, errors="coerce")
    if league_avg is None or not math.isfinite(league_avg) or league_avg <= 0:
        return pd.Series(1.0, index=vals.index, dtype=float)
    ratio = vals / float(league_avg)
    ratio = ratio.where(ratio.notna(), other=1.0)
    return ratio.clip(lower=0.5, upper=1.5)


def _safe_goalie_delta_factor(
    goalie_sf60: pd.Series,
    goalie_team_sa60: pd.Series,
    start_prob: pd.Series,
) -> pd.Series:
    g = pd.to_numeric(goalie_sf60, errors="coerce")
    t = pd.to_numeric(goalie_team_sa60, errors="coerce")
    sp = pd.to_numeric(start_prob, errors="coerce").fillna(1.0).clip(lower=0.0, upper=1.0)
    delta = (g - t).where(g.notna() & t.notna(), other=0.0)
    # Positive delta means this goalie environment has seen more shots than team baseline.
    raw = (1.0 + (delta / 30.0)).clip(lower=0.75, upper=1.25)
    return (1.0 - sp) + (sp * raw)


def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate a position-group defense-side base against the NHL SOG Poisson offense base.")
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

    league_goalie_sf60 = float(
        pd.to_numeric(train["projected_goalie_d10_shots_faced_per60"], errors="coerce").dropna().mean()
    )
    scored["goalie_env_factor"] = _safe_goalie_factor(scored["projected_goalie_d10_shots_faced_per60"], league_goalie_sf60)
    scored["goalie_delta_factor"] = _safe_goalie_delta_factor(
        scored["projected_goalie_d10_shots_faced_per60"],
        scored["projected_goalie_team_d10_sa_per60"],
        scored["projected_goalie_start_prob"],
    )

    scored["rate_defense_pos"] = pd.to_numeric(scored["opp_pos_allowed_d10_per60"], errors="coerce").clip(lower=0.0)
    scored["lambda_defense_pos"] = (scored["rate_defense_pos"] * scored["d10_toi_min_avg"] / 60.0).clip(lower=0.0)
    scored["lambda_defense_pos_goalie"] = (scored["lambda_defense_pos"] * scored["goalie_env_factor"]).clip(lower=0.0)
    scored["lambda_defense_pos_goalie_delta"] = (scored["lambda_defense_pos"] * scored["goalie_delta_factor"]).clip(lower=0.0)
    scored["rate_defense_pos_goalie"] = (scored["rate_defense_pos"] * scored["goalie_env_factor"]).clip(lower=0.0)
    scored["rate_defense_pos_goalie_delta"] = (scored["rate_defense_pos"] * scored["goalie_delta_factor"]).clip(lower=0.0)

    both_plain = (scored["lambda_offense"] > 0) & (scored["lambda_defense_pos"] > 0)
    both_goalie = (scored["lambda_offense"] > 0) & (scored["lambda_defense_pos_goalie"] > 0)
    both_goalie_delta = (scored["lambda_offense"] > 0) & (scored["lambda_defense_pos_goalie_delta"] > 0)

    scored["lambda_combined_plain"] = scored["lambda_offense"]
    scored.loc[both_plain, "lambda_combined_plain"] = (
        (scored.loc[both_plain, "rate_offense"] * scored.loc[both_plain, "rate_defense_pos"]) ** 0.5
        * scored.loc[both_plain, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both_plain) & (scored["lambda_defense_pos"] > 0), "lambda_combined_plain"] = scored.loc[
        (~both_plain) & (scored["lambda_defense_pos"] > 0), "lambda_defense_pos"
    ]

    scored["lambda_combined_goalie"] = scored["lambda_offense"]
    scored.loc[both_goalie, "lambda_combined_goalie"] = (
        (scored.loc[both_goalie, "rate_offense"] * scored.loc[both_goalie, "rate_defense_pos_goalie"]) ** 0.5
        * scored.loc[both_goalie, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both_goalie) & (scored["lambda_defense_pos_goalie"] > 0), "lambda_combined_goalie"] = scored.loc[
        (~both_goalie) & (scored["lambda_defense_pos_goalie"] > 0), "lambda_defense_pos_goalie"
    ]

    scored["lambda_combined_goalie_delta"] = scored["lambda_offense"]
    scored.loc[both_goalie_delta, "lambda_combined_goalie_delta"] = (
        (scored.loc[both_goalie_delta, "rate_offense"] * scored.loc[both_goalie_delta, "rate_defense_pos_goalie_delta"]) ** 0.5
        * scored.loc[both_goalie_delta, "d10_toi_min_avg"]
        / 60.0
    )
    scored.loc[(~both_goalie_delta) & (scored["lambda_defense_pos_goalie_delta"] > 0), "lambda_combined_goalie_delta"] = scored.loc[
        (~both_goalie_delta) & (scored["lambda_defense_pos_goalie_delta"] > 0), "lambda_defense_pos_goalie_delta"
    ]

    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        scored[f"p_offense_over_{key}"] = scored["lambda_offense"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_defpos_over_{key}"] = scored["lambda_defense_pos"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_defposg_over_{key}"] = scored["lambda_defense_pos_goalie"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_defposgd_over_{key}"] = scored["lambda_defense_pos_goalie_delta"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_comb_plain_over_{key}"] = scored["lambda_combined_plain"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_comb_goalie_over_{key}"] = scored["lambda_combined_goalie"].apply(lambda v: _poisson_tail(float(v), threshold))
        scored[f"p_comb_goalie_delta_over_{key}"] = scored["lambda_combined_goalie_delta"].apply(lambda v: _poisson_tail(float(v), threshold))

    result: Dict[str, Any] = {
        "ok": True,
        "rows": {
            "train": int(len(train)),
            "test": int(len(test)),
            "raw": int(len(raw)),
        },
        "dates": {
            "train_min": train_dates[0],
            "train_max": train_dates[-1],
            "test_min": test_dates[0],
            "test_max": test_dates[-1],
            "test_game_days": int(args.test_game_days),
        },
        "coverage": {
            "league_goalie_sf60": _round(league_goalie_sf60, 6),
            "rows_with_opp_pos_allowed_per60": int(pd.to_numeric(scored["opp_pos_allowed_d10_per60"], errors="coerce").notna().sum()),
            "rows_with_goalie_env": int(pd.to_numeric(scored["projected_goalie_d10_shots_faced_per60"], errors="coerce").notna().sum()),
        },
        "overall": {
            "offense": _combined_metric(scored, "offense"),
            "defense_position": _combined_metric(scored, "defpos"),
            "defense_position_goalie": _combined_metric(scored, "defposg"),
            "defense_position_goalie_delta": _combined_metric(scored, "defposgd"),
            "combined_plain": _combined_metric(scored, "comb_plain"),
            "combined_goalie": _combined_metric(scored, "comb_goalie"),
            "combined_goalie_delta": _combined_metric(scored, "comb_goalie_delta"),
        },
        "by_line": {},
    }

    for line, threshold in THRESHOLDS.items():
        key = str(line).replace(".", "_")
        result["by_line"][str(line)] = {
            "offense": _metric_rows(scored, f"p_offense_over_{key}", threshold),
            "defense_position": _metric_rows(scored, f"p_defpos_over_{key}", threshold),
            "defense_position_goalie": _metric_rows(scored, f"p_defposg_over_{key}", threshold),
            "defense_position_goalie_delta": _metric_rows(scored, f"p_defposgd_over_{key}", threshold),
            "combined_plain": _metric_rows(scored, f"p_comb_plain_over_{key}", threshold),
            "combined_goalie": _metric_rows(scored, f"p_comb_goalie_over_{key}", threshold),
            "combined_goalie_delta": _metric_rows(scored, f"p_comb_goalie_delta_over_{key}", threshold),
            "offense_by_expected_bucket": _bucket_stats(scored, f"p_offense_over_{key}", threshold),
            "combined_goalie_by_expected_bucket": _bucket_stats(scored, f"p_comb_goalie_over_{key}", threshold),
            "combined_goalie_delta_by_expected_bucket": _bucket_stats(scored, f"p_comb_goalie_delta_over_{key}", threshold),
        }

    if args.write_scored_csv:
        out_path = Path(args.write_scored_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        scored.to_csv(out_path, index=False)
        result["scored_csv"] = str(out_path)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
