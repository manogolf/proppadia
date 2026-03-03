#!/usr/bin/env python3
"""Build NHL SOG shadow probabilities using a projected-context defense-surprise adjustment."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

from backend.nhl.scripts.build_sog_poisson_residual_dataset import build_dataset_df
from backend.nhl.scripts.experiment_sog_projected_context_surprise_defense_base import (
    _attach_projected_signature,
    _apply_surprise,
    _build_projected_rows,
    _build_recent_faced_baseline,
)
from backend.nhl.scripts.score_sog_poisson_baseline import (
    _bucket_series,
    _coalesce,
    _poisson_tail,
    _to_numeric,
)
from backend.shared.db.pg import pg_fetchall


DEFAULT_OUT = "backend/nhl/data/processed/sog_predictions_wide_defense_surprise_shadow.csv"


def _infer_season(slate_date: str) -> int:
    y, m, _ = (int(x) for x in slate_date.split("-"))
    return y if m >= 9 else (y - 1)


def _history_cutoff(slate_date: str) -> str:
    dt = datetime.strptime(slate_date, "%Y-%m-%d").date()
    return dt.isoformat()


def _parse_alphas(raw: str) -> List[float]:
    vals: List[float] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if token:
            vals.append(float(token))
    if not vals:
        raise ValueError("Need at least one alpha value")
    return vals


def _load_player_info(player_ids) -> pd.DataFrame:
    ids = sorted({int(x) for x in player_ids if pd.notna(x)})
    if not ids:
        return pd.DataFrame(columns=["player_id", "player_name", "position_raw"])
    rows = pg_fetchall(
        """
        SELECT
          p.player_id::bigint AS player_id,
          COALESCE(p.full_name, concat_ws(' ', p.first_name, p.last_name), p.player_id::text) AS player_name,
          COALESCE(NULLIF(BTRIM(p.position), ''), 'UNK') AS position_raw
        FROM nhl.players p
        WHERE p.player_id = ANY(%s::bigint[])
        """,
        (ids,),
    )
    return pd.DataFrame(rows)


def _load_projected_goalies(game_rows: pd.DataFrame) -> pd.DataFrame:
    pairs = (
        game_rows[["game_id", "opponent_id"]]
        .dropna()
        .drop_duplicates()
        .rename(columns={"opponent_id": "team_id"})
    )
    if pairs.empty:
        return pd.DataFrame(
            columns=[
                "game_id",
                "team_id",
                "projected_goalie_id",
                "projected_goalie_start_prob",
                "projected_goalie_d10_shots_faced_per60",
                "projected_goalie_d10_save_pct",
            ]
        )
    game_ids = sorted({int(x) for x in pairs["game_id"].tolist()})
    team_ids = sorted({int(x) for x in pairs["team_id"].tolist()})
    rows = pg_fetchall(
        """
        WITH ranked AS (
          SELECT
            g.game_id::bigint AS game_id,
            g.team_id::bigint AS team_id,
            g.player_id::bigint AS projected_goalie_id,
            g.start_prob::float8 AS projected_goalie_start_prob,
            g.d10_shots_faced_per60::float8 AS projected_goalie_d10_shots_faced_per60,
            g.d10_save_pct::float8 AS projected_goalie_d10_save_pct,
            ROW_NUMBER() OVER (
              PARTITION BY g.game_id, g.team_id
              ORDER BY COALESCE(g.start_prob, 0) DESC, g.player_id
            ) AS rn
          FROM nhl.training_features_goalie_saves_v2 g
          WHERE g.game_id = ANY(%s::bigint[])
            AND g.team_id = ANY(%s::bigint[])
        )
        SELECT
          game_id,
          team_id,
          projected_goalie_id,
          projected_goalie_start_prob,
          projected_goalie_d10_shots_faced_per60,
          projected_goalie_d10_save_pct
        FROM ranked
        WHERE rn = 1
        """,
        (game_ids, team_ids),
    )
    return pd.DataFrame(rows)


def _latest_faced_baseline_by_player(full_hist: pd.DataFrame) -> dict[int, float]:
    work = full_hist.sort_values(["player_id", "game_date", "game_id"]).copy()
    out: dict[int, float] = {}
    for pid, grp in work.groupby("player_id", sort=False):
        vals = pd.to_numeric(grp["projected_signature_rate_per60"], errors="coerce").dropna()
        if not vals.empty:
            out[int(pid)] = float(vals.tail(10).mean())
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a shadow NHL SOG projected-context defense-surprise CSV from current slate features.")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", default=DEFAULT_OUT)
    ap.add_argument("--slate-date", default=None)
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--alphas", default="0.5,0.6")
    ap.add_argument("--bandwidth", type=float, default=0.6)
    ap.add_argument("--goalie-weight", type=float, default=0.7)
    ap.add_argument("--clip-low", type=float, default=0.75)
    ap.add_argument("--clip-high", type=float, default=1.25)
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    alphas = _parse_alphas(args.alphas)

    df = pd.read_csv(in_path)
    if df.empty:
        raise SystemExit(f"[defense shadow] empty input CSV: {in_path}")

    slate_dates = sorted({str(x) for x in df.get("game_date", pd.Series(dtype=str)).dropna().astype(str).tolist()})
    slate_date = args.slate_date or (slate_dates[0] if len(slate_dates) == 1 else None)
    if not slate_date:
        raise SystemExit("[defense shadow] slate date is required when input contains multiple or no game_date values")
    season = int(
        args.season
        or (
            pd.to_numeric(df.get("season", pd.Series(dtype=float)), errors="coerce").dropna().iloc[0]
            if "season" in df.columns and pd.to_numeric(df.get("season"), errors="coerce").dropna().size
            else _infer_season(slate_date)
        )
    )

    rate = _coalesce(
        _to_numeric(df, "d10_sog_per60"),
        _to_numeric(df, "d20_sog_per60"),
        _to_numeric(df, "d5_sog_per60"),
    )
    toi = _coalesce(
        _to_numeric(df, "d10_toi_min_avg"),
        _to_numeric(df, "d20_toi_min_avg"),
        _to_numeric(df, "d5_toi_min_avg"),
        (_to_numeric(df, "szn_toi_per_game_5on5") + _to_numeric(df, "szn_toi_per_game_pp")),
        (_to_numeric(df, "season_5on5_icetime_per_game") / 60.0)
        + (_to_numeric(df, "season_5on4_icetime_per_game") / 60.0),
    )
    lambda_base = ((rate * toi) / 60.0).where(rate.notna() | toi.notna(), 0.0).clip(lower=0.0)

    curr = df.copy()
    curr["lambda_base"] = lambda_base.astype(float)
    curr["expected_sog_bucket"] = _bucket_series(curr["lambda_base"])
    curr["poisson_source"] = (
        (_to_numeric(df, "d10_sog_per60").notna() & _to_numeric(df, "d10_toi_min_avg").notna())
        .map({True: "d10", False: "fallback"})
    )

    player_info = _load_player_info(curr["player_id"].tolist())
    if not player_info.empty:
        curr = curr.merge(player_info, how="left", on="player_id")
    else:
        curr["player_name"] = curr["player_id"].astype(str)
        curr["position_raw"] = "UNK"

    goalie_df = _load_projected_goalies(curr[["game_id", "opponent_id"]])
    if not goalie_df.empty:
        curr = curr.merge(goalie_df, how="left", left_on=["game_id", "opponent_id"], right_on=["game_id", "team_id"])
        if "team_id_y" in curr.columns:
            curr = curr.drop(columns=["team_id_y"])
        if "team_id_x" in curr.columns:
            curr = curr.rename(columns={"team_id_x": "team_id"})
    else:
        curr["projected_goalie_id"] = pd.NA
        curr["projected_goalie_start_prob"] = pd.NA
        curr["projected_goalie_d10_shots_faced_per60"] = pd.NA
        curr["projected_goalie_d10_save_pct"] = pd.NA

    hist_to = _history_cutoff(slate_date)
    hist = build_dataset_df(season, None, hist_to)
    if hist.empty:
        raise SystemExit(f"[defense shadow] no historical rows available for season={season} through {hist_to}")

    goalie_rows, opp_rows = _build_projected_rows(hist)
    hist = _attach_projected_signature(hist, goalie_rows, opp_rows, float(args.bandwidth), float(args.goalie_weight))
    faced_map = _latest_faced_baseline_by_player(hist)
    curr = _attach_projected_signature(curr, goalie_rows, opp_rows, float(args.bandwidth), float(args.goalie_weight))
    curr["faced_projected_rate_last10"] = curr["player_id"].map(faced_map)
    curr["defense_surprise_ratio"] = (
        pd.to_numeric(curr["projected_signature_rate_per60"], errors="coerce")
        / pd.to_numeric(curr["faced_projected_rate_last10"], errors="coerce")
    ).replace([math.inf, -math.inf], math.nan)

    out = pd.DataFrame()
    out["shadow_model"] = "projected_context_surprise"
    for c in [
        "player_id", "player_name", "position_raw", "game_id", "team_id", "opponent_id",
        "is_home", "game_date", "season", "projected_goalie_id", "projected_goalie_start_prob",
        "expected_sog_bucket", "poisson_source",
    ]:
        if c in curr.columns:
            out[c] = curr[c]
    out["lambda_offense"] = pd.to_numeric(curr["lambda_base"], errors="coerce").clip(lower=0.0)
    out["projected_signature_rate_per60"] = pd.to_numeric(curr["projected_signature_rate_per60"], errors="coerce")
    out["projected_signature_source"] = curr["projected_signature_source"]
    out["faced_projected_rate_last10"] = pd.to_numeric(curr["faced_projected_rate_last10"], errors="coerce")
    out["defense_surprise_ratio"] = pd.to_numeric(curr["defense_surprise_ratio"], errors="coerce")
    out["defense_surprise_applied"] = out["defense_surprise_ratio"].notna()
    out["p_offense_over_1_5"] = out["lambda_offense"].apply(lambda v: _poisson_tail(float(v), 2))
    out["p_offense_over_2_5"] = out["lambda_offense"].apply(lambda v: _poisson_tail(float(v), 3))
    out["p_offense_over_3_5"] = out["lambda_offense"].apply(lambda v: _poisson_tail(float(v), 4))

    for alpha in alphas:
        key = str(alpha).replace('.', '_')
        lam_col = f"lambda_projected_a{key}"
        raw_ratio_col = f"defense_surprise_raw_ratio_a{key}"
        clipped_ratio_col = f"defense_surprise_clipped_ratio_a{key}"
        reason_col = f"defense_surprise_reason_a{key}"
        lam = _apply_surprise(curr, float(alpha), float(args.clip_low), float(args.clip_high))
        raw_ratio = pd.to_numeric(curr["projected_signature_rate_per60"], errors="coerce") / pd.to_numeric(curr["faced_projected_rate_last10"], errors="coerce")
        raw_ratio = raw_ratio.replace([math.inf, -math.inf], math.nan)
        clipped_ratio = raw_ratio.clip(lower=float(args.clip_low), upper=float(args.clip_high))
        reason = pd.Series("applied", index=curr.index, dtype="object")
        reason = reason.where(raw_ratio.notna(), "missing_recent_projected_baseline")
        out[lam_col] = lam
        out[raw_ratio_col] = raw_ratio
        out[clipped_ratio_col] = clipped_ratio
        out[reason_col] = reason
        out[f"p_projected_a{key}_over_1_5"] = out[lam_col].apply(lambda v: _poisson_tail(float(v), 2))
        out[f"p_projected_a{key}_over_2_5"] = out[lam_col].apply(lambda v: _poisson_tail(float(v), 3))
        out[f"p_projected_a{key}_over_3_5"] = out[lam_col].apply(lambda v: _poisson_tail(float(v), 4))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    summary = {
        "ok": True,
        "shadow_model": "projected_context_surprise",
        "slate_date": slate_date,
        "season": season,
        "rows": int(len(out)),
        "history_rows": int(len(hist)),
        "coverage": {
            "rows_with_projected_signature": int(pd.to_numeric(out["projected_signature_rate_per60"], errors="coerce").notna().sum()),
            "rows_with_faced_baseline": int(pd.to_numeric(out["faced_projected_rate_last10"], errors="coerce").notna().sum()),
            "projected_signature_source_counts": out["projected_signature_source"].value_counts(dropna=False).to_dict(),
        },
        "alphas": alphas,
        "out_csv": str(out_path),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
