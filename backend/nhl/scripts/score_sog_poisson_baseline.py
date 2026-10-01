#!/usr/bin/env python3
"""Score NHL SOG wide probabilities from a simple Poisson baseline."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_OUT = "backend/nhl/data/processed/sog_predictions_wide_calibrated.csv"


def _to_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(np.nan, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


def _coalesce(*series: pd.Series) -> pd.Series:
    if not series:
        raise ValueError("Need at least one series to coalesce")
    out = series[0].copy()
    for s in series[1:]:
        out = out.where(out.notna(), s)
    return out


def score_predictions(
    df: pd.DataFrame, *, source_run_id: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Score rows with a known rate and exposure; return skipped rows separately."""
    if df.empty:
        raise ValueError("SOG_SCORER_INPUT_EMPTY")

    rate_fields = ("d10_sog_per60", "d20_sog_per60", "d5_sog_per60")
    rate_inputs = [_to_numeric(df, name).where(np.isfinite(_to_numeric(df, name))) for name in rate_fields]
    # Preserve the established TOI fallback calculations and their order.
    toi_inputs = [
        _to_numeric(df, "d10_toi_min_avg"),
        _to_numeric(df, "d20_toi_min_avg"),
        _to_numeric(df, "d5_toi_min_avg"),
        _to_numeric(df, "szn_toi_per_game_5on5") + _to_numeric(df, "szn_toi_per_game_pp"),
        (_to_numeric(df, "season_5on5_icetime_per_game") / 60.0)
        + (_to_numeric(df, "season_5on4_icetime_per_game") / 60.0),
    ]
    toi_inputs = [value.where(np.isfinite(value)) for value in toi_inputs]
    rate = _coalesce(*rate_inputs)
    toi = _coalesce(*toi_inputs)

    has_rate = rate.notna()
    has_exposure = toi.notna()
    valid_rate = has_rate & rate.ge(0)
    valid_exposure = has_exposure & toi.ge(0)
    expected_sog = ((rate * toi) / 60.0).where(valid_rate & valid_exposure)
    valid_expected_sog = expected_sog.notna() & np.isfinite(expected_sog)
    scorable = valid_rate & valid_exposure & valid_expected_sog
    expected_sog = expected_sog.where(scorable)

    scored_input = df.loc[scorable].copy()
    out = pd.DataFrame(index=scored_input.index)
    for column in ("player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "season"):
        if column in scored_input:
            out[column] = scored_input[column]
    out["expected_sog"] = expected_sog.loc[scorable].astype(float)
    out["expected_sog_bucket"] = _bucket_series(out["expected_sog"])
    out["poisson_source"] = np.where(
        _to_numeric(scored_input, "d10_sog_per60").notna()
        & _to_numeric(scored_input, "d10_toi_min_avg").notna(),
        "d10", "fallback")
    for column, threshold in (("p_over_1_5", 2), ("p_over_2_5", 3), ("p_over_3_5", 4)):
        out[column] = out["expected_sog"].apply(lambda value: _poisson_tail(float(value), threshold)).astype(float)
    out["p_0_1"] = (1.0 - out["p_over_1_5"]).clip(0, 1)
    out["p_2"] = (out["p_over_1_5"] - out["p_over_2_5"]).clip(0, 1)
    out["p_3"] = (out["p_over_2_5"] - out["p_over_3_5"]).clip(0, 1)
    out["p_4p"] = out["p_over_3_5"].clip(0, 1)

    skipped_mask = ~scorable
    skipped_indices = df.index[skipped_mask]
    status = np.where(
        ~has_exposure, "UNSCORED_MISSING_EXPOSURE",
        np.where(~has_rate, "UNSCORED_MISSING_RATE",
                 np.where(~valid_exposure, "UNSCORED_INVALID_EXPOSURE",
                          np.where(~valid_rate, "UNSCORED_INVALID_RATE", "UNSCORED_INVALID_EXPECTED_SOG"))),
    )
    skipped_rows: list[dict[str, object]] = []
    for idx in skipped_indices:
        row = df.loc[idx]
        row_status = str(status[df.index.get_loc(idx)])
        selected_rate = rate.loc[idx]
        selected_toi = toi.loc[idx]
        for line in (1.5, 2.5, 3.5):
            skipped_rows.append({
                **{key: row[key] for key in ("game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date", "season", "player_name", "player_team_code") if key in row.index},
                **{key: row[key] for key in (*rate_fields, "d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game") if key in row.index},
                "line": line,
                "selected_sog_rate_per60": selected_rate,
                "selected_sog_rate_source": next((field for field, values in zip(rate_fields, rate_inputs) if pd.notna(values.loc[idx])), ""),
                "selected_toi_minutes": selected_toi,
                "selected_toi_source": next((field for field, values in zip(("d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg", "SEASON_TOI_5V5_PLUS_PP", "SEASON_TOI_5V5_PLUS_5V4"), toi_inputs) if pd.notna(values.loc[idx])), ""),
                "expected_sog": np.nan,
                "scoring_status": row_status,
                "reason": row_status.removeprefix("UNSCORED_"),
                "model_family": "poisson_baseline",
                "model_version": "baseline_v1",
                "source_run_id": source_run_id,
            })
    skipped_columns = [
        "game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date", "season",
        "player_name", "player_team_code", *rate_fields, "d10_toi_min_avg", "d20_toi_min_avg",
        "d5_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp",
        "season_5on5_icetime_per_game", "season_5on4_icetime_per_game", "line",
        "selected_sog_rate_per60", "selected_sog_rate_source", "selected_toi_minutes",
        "selected_toi_source", "expected_sog", "scoring_status", "reason", "model_family",
        "model_version", "source_run_id",
    ]
    skipped = pd.DataFrame(skipped_rows, columns=skipped_columns)
    return out.reset_index(drop=True), skipped


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


def _poisson_tail(lam: float, threshold: int) -> float:
    if not math.isfinite(lam) or lam < 0:
        return float("nan")
    cutoff = max(0, threshold - 1)
    cdf = 0.0
    for k in range(cutoff + 1):
        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _bucket_series(expected_sog: pd.Series) -> pd.Series:
    return expected_sog.apply(lambda v: _expected_bucket(float(v)) if pd.notna(v) else "missing")


def main() -> None:
    ap = argparse.ArgumentParser(description="Score NHL SOG probabilities using a Poisson baseline.")
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", default=DEFAULT_OUT)
    ap.add_argument("--unscored-out", dest="unscored_out_path", default=None,
                    help="CSV ledger for rows without valid scoring inputs (defaults beside --out).")
    ap.add_argument("--names", dest="names_path", default=None,
                    help="Optional player/game names CSV used to enrich the unscored ledger.")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    df = pd.read_csv(in_path)
    if args.names_path:
        names = pd.read_csv(args.names_path)
        required_name_columns = {"player_id", "game_id", "full_name"}
        if required_name_columns - set(names.columns):
            raise SystemExit("SOG_UNSCORED_NAMES_SCHEMA_INVALID")
        names = names[[column for column in ("player_id", "game_id", "full_name", "team_code") if column in names]].copy()
        names["player_id"] = pd.to_numeric(names.player_id, errors="coerce")
        names["game_id"] = pd.to_numeric(names.game_id, errors="coerce")
        if names.duplicated(["player_id", "game_id"], keep=False).any():
            raise SystemExit("SOG_UNSCORED_NAMES_IDENTITY_CONFLICT")
        names = names.rename(columns={"full_name": "player_name", "team_code": "player_team_code"})
        df["player_id"] = pd.to_numeric(df.player_id, errors="coerce")
        df["game_id"] = pd.to_numeric(df.game_id, errors="coerce")
        df = df.merge(names, on=["player_id", "game_id"], how="left", validate="many_to_one")
    out, unscored = score_predictions(df, source_run_id=out_path.parent.name)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    unscored_path = Path(args.unscored_out_path) if args.unscored_out_path else out_path.with_name(
        f"{out_path.stem}_unscored.csv")
    unscored_path.parent.mkdir(parents=True, exist_ok=True)
    unscored.to_csv(unscored_path, index=False)
    print(f"[poisson scorer] rows={len(out)} wrote={out_path}")
    print(f"[poisson scorer] unscored_rows={len(unscored)} wrote={unscored_path}")
    print(
        "[poisson scorer] source_counts="
        + str(out["poisson_source"].value_counts(dropna=False).to_dict())
    )


if __name__ == "__main__":
    main()
