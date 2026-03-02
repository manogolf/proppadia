#!/usr/bin/env python3
"""Build an NHL SOG second-tier dataset on top of the Poisson base."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from backend.shared.db.pg import pg_fetchall


SQL = """
WITH base AS (
  SELECT
    f.game_date::date AS game_date,
    f.season::int AS season,
    f.player_id::bigint AS player_id,
    COALESCE(pl.full_name, concat_ws(' ', pl.first_name, pl.last_name), f.player_id::text) AS player_name,
    COALESCE(NULLIF(BTRIM(pl.position), ''), 'UNK') AS position_raw,
    f.game_id::bigint AS game_id,
    f.team_id::bigint AS team_id,
    f.opponent_id::bigint AS opponent_id,
    f.is_home,
    s.shots_on_goal::int AS shots_on_goal,
    s.blocks::int AS blocks,
    f.d5_sog_per60::float8 AS d5_sog_per60,
    f.d10_sog_per60::float8 AS d10_sog_per60,
    f.d20_sog_per60::float8 AS d20_sog_per60,
    f.attempts_d10_per60::float8 AS attempts_d10_per60,
    f.role_pp_share::float8 AS role_pp_share,
    f.toi_trend_3v10::float8 AS toi_trend_3v10,
    f.d10_toi_cv::float8 AS d10_toi_cv,
    f.d10_toi_min_avg::float8 AS d10_toi_min_avg,
    f.last10_team_sog_share::float8 AS last10_team_sog_share,
    f.opp_d10_sf_allowed_per_game::float8 AS opp_d10_sf_allowed_per_game,
    f.pace_matchup_index::float8 AS pace_matchup_index,
    tc.d10_sa_per60::float8 AS team_d10_sa_per60,
    tc.opp_d10_sf_per60::float8 AS opp_d10_sf_per60,
    tc.opp_d10_sa_per60::float8 AS opp_d10_sa_per60,
    pg.player_id::bigint AS projected_goalie_id,
    pg.start_prob::float8 AS projected_goalie_start_prob,
    pg.d10_shots_faced_per60::float8 AS projected_goalie_d10_shots_faced_per60,
    pg.d10_save_pct::float8 AS projected_goalie_d10_save_pct,
    pg.team_d10_sa_per60::float8 AS projected_goalie_team_d10_sa_per60,
    pg.opp_d10_sf_per60::float8 AS projected_goalie_opp_d10_sf_per60
  FROM nhl.training_features_nhl_sog_enriched_pregame_v2 f
  JOIN nhl.skater_game_logs_raw s
    ON s.game_id = f.game_id
   AND s.player_id = f.player_id
  LEFT JOIN nhl.players pl
    ON pl.player_id = f.player_id
  LEFT JOIN nhl.team_context_rolling tc
    ON tc.game_id = f.game_id
   AND tc.team_id = f.team_id
  LEFT JOIN LATERAL (
    SELECT
      g.player_id,
      g.start_prob,
      g.d10_shots_faced_per60,
      g.d10_save_pct,
      g.team_d10_sa_per60,
      g.opp_d10_sf_per60
    FROM nhl.training_features_goalie_saves_v2 g
    WHERE g.game_id = f.game_id
      AND g.team_id = f.opponent_id
    ORDER BY COALESCE(g.start_prob, 0) DESC, g.player_id
    LIMIT 1
  ) pg ON TRUE
  WHERE f.season = %s
    AND (%s::date IS NULL OR f.game_date >= %s::date)
    AND (%s::date IS NULL OR f.game_date <= %s::date)
    AND f.d10_sog_per60 IS NOT NULL
    AND f.d10_toi_min_avg IS NOT NULL
    AND s.shots_on_goal IS NOT NULL
)
SELECT
  game_date,
  season,
  player_id,
  player_name,
  position_raw,
  game_id,
  team_id,
  opponent_id,
  is_home,
  shots_on_goal,
  blocks,
  d5_sog_per60,
  d10_sog_per60,
  d20_sog_per60,
  attempts_d10_per60,
  role_pp_share,
  toi_trend_3v10,
  d10_toi_cv,
  d10_toi_min_avg,
  last10_team_sog_share,
  opp_d10_sf_allowed_per_game,
  pace_matchup_index,
  team_d10_sa_per60,
  opp_d10_sf_per60,
  opp_d10_sa_per60,
  projected_goalie_id,
  projected_goalie_start_prob,
  projected_goalie_d10_shots_faced_per60,
  projected_goalie_d10_save_pct,
  projected_goalie_team_d10_sa_per60,
  projected_goalie_opp_d10_sf_per60,
  ((d10_sog_per60 * d10_toi_min_avg) / 60.0)::float8 AS lambda_base
FROM base
ORDER BY game_date, player_id
"""


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _expected_bucket(v: float | None) -> str:
    if v is None or not math.isfinite(v):
        return "missing"
    if v < 1.5:
        return "<1.5"
    if v < 2.5:
        return "1.5-2.5"
    if v < 3.5:
        return "2.5-3.5"
    return "3.5+"


def fetch_dataset_rows(season: int, from_date: Optional[str], to_date: Optional[str]) -> List[Dict[str, Any]]:
    rows = pg_fetchall(SQL, (season, from_date, from_date, to_date, to_date))
    return list(rows or [])


def build_dataset_df(season: int, from_date: Optional[str], to_date: Optional[str]) -> pd.DataFrame:
    rows = fetch_dataset_rows(season, from_date, to_date)
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["lambda_base"] = pd.to_numeric(df["lambda_base"], errors="coerce").clip(lower=0.0)
    df["shots_on_goal"] = pd.to_numeric(df["shots_on_goal"], errors="coerce")
    df = df.dropna(subset=["lambda_base", "shots_on_goal"]).copy()

    df["expected_sog_bucket"] = df["lambda_base"].apply(lambda v: _expected_bucket(_to_float(v)))
    df["d5_minus_d10"] = pd.to_numeric(df["d5_sog_per60"], errors="coerce") - pd.to_numeric(df["d10_sog_per60"], errors="coerce")
    df["d20_minus_d10"] = pd.to_numeric(df["d20_sog_per60"], errors="coerce") - pd.to_numeric(df["d10_sog_per60"], errors="coerce")

    # Stable residual target for learning an additive correction to log(1 + lambda_base).
    df["target_log1p_residual"] = (
        (df["shots_on_goal"].astype(float) + 1.0).apply(math.log)
        - (df["lambda_base"].astype(float) + 1.0).apply(math.log)
    )
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a season-scoped NHL SOG Poisson residual dataset.")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument(
        "--out-csv",
        default="backend/nhl/data/analysis/sog_poisson_residual_dataset_season_2025.csv",
    )
    args = ap.parse_args()

    df = build_dataset_df(args.season, args.from_date, args.to_date)
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    summary = {
        "ok": True,
        "season": args.season,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "rows": int(len(df)),
        "dates": {
            "min": None if df.empty else str(df["game_date"].min()),
            "max": None if df.empty else str(df["game_date"].max()),
            "distinct": 0 if df.empty else int(df["game_date"].nunique()),
        },
        "players": 0 if df.empty else int(df["player_id"].nunique()),
        "expected_sog_bucket_counts": {} if df.empty else df["expected_sog_bucket"].value_counts().sort_index().to_dict(),
        "out_csv": str(out_path),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
