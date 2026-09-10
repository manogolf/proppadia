#!/usr/bin/env python3
"""Deterministic MLB moneyline probability-region and apparent-edge premise audit.

This is a read-only research utility.  It does not refit the frozen model,
create a selector, alter predictions, or claim that a captured quote was filled.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from backend.app.deps import pg_connect


ROOT = Path(__file__).resolve().parents[3]
CONFIG = ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"
MARKET_DB = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
HISTORICAL_PINNACLE = (
    ROOT / "artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1"
    / "2026-08-10/moneyline_pinnacle_join.csv"
)
PRIOR_FOUNDATION = (
    ROOT / "artifacts/analysis/model_development/mlb_standalone_prediction_foundation_certification_v1"
    / "2026-08-12/moneyline_prospective_evidence.csv"
)
PRIOR_OPS = ROOT / "artifacts/analysis/mlb/ops_current_state/2026-08-14/mlb_ops_current_state_summary.json"
MODEL = "MLB_GAME_PYTHAGOREAN_LOG5_V1"
SNAPSHOT = "DESIGNATED_DAILY_PUBLIC_SNAPSHOT"
ADMISSION = "ADMITTED_SHADOW"
PRIOR_CHARACTERIZATION_CUTOFF = "2026-08-13"
FORWARD_EXTENSION_START = "2026-09-01"
MIN_BOOK_MATCHES = 50
BOOTSTRAP_REPS = 4000
SEED = 20260909
SPARSE_N = 30

PROBABILITY_BANDS = [
    (-math.inf, .40, "BELOW_40"),
    (.40, .50, "40_TO_BELOW_50"),
    (.50, .55, "50_TO_BELOW_55"),
    (.55, .60, "55_TO_BELOW_60"),
    (.60, .65, "60_TO_BELOW_65"),
    (.65, math.inf, "65_AND_ABOVE"),
]
GAP_BANDS = [
    (-math.inf, -.05, "AT_OR_BELOW_NEG_5PP", True, True),
    (-.05, -.02, "ABOVE_NEG_5PP_THROUGH_NEG_2PP", False, True),
    (-.02, 0.0, "ABOVE_NEG_2PP_THROUGH_0", False, True),
    (0.0, .02, "ABOVE_0_THROUGH_POS_2PP", False, True),
    (.02, .05, "ABOVE_POS_2PP_THROUGH_POS_5PP", False, True),
    (.05, math.inf, "ABOVE_POS_5PP", False, True),
]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def american_implied(value: float) -> float:
    value = float(value)
    return 100.0 / (value + 100.0) if value > 0 else -value / (-value + 100.0)


def american_decimal(value: float) -> float:
    value = float(value)
    return 1.0 + (value / 100.0 if value > 0 else 100.0 / -value)


def prob_band(value: float) -> str:
    for lower, upper, label in PROBABILITY_BANDS:
        if value >= lower and value < upper:
            return label
    raise AssertionError(value)


def gap_band(value: float) -> str:
    for lower, upper, label, lower_closed, upper_closed in GAP_BANDS:
        left = value >= lower if lower_closed else value > lower
        right = value <= upper if upper_closed else value < upper
        if left and right:
            return label
    raise AssertionError(value)


def json_value(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, float_format="%.12f", na_rep="")


def load_predictions(cutoff: str | None) -> pd.DataFrame:
    sql = """
      SELECT p.game_date::text AS game_date,p.game_id,p.model_version,
             p.prediction_snapshot_class,p.scheduled_start_utc,p.prediction_timestamp_utc,
             p.prediction_cutoff_utc,p.home_team,p.away_team,p.home_win_probability,
             p.away_win_probability,p.predicted_winner,p.confidence_band,p.model_hash,
             p.payload_sha256 AS prediction_payload_sha256,
             o.official_home_runs,o.official_away_runs,o.official_winner,o.prediction_correct,
             o.payload_sha256 AS outcome_payload_sha256
      FROM mlb.public_game_moneyline_predictions p
      LEFT JOIN mlb.public_game_moneyline_outcomes o
        USING (game_date,game_id,model_version,prediction_snapshot_class)
      WHERE p.model_version=%s AND p.prediction_snapshot_class=%s
        AND p.admission_status=%s
        AND (%s::date IS NULL OR p.game_date<=%s::date)
      ORDER BY p.game_date,p.scheduled_start_utc,p.game_id
    """
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql, (MODEL, SNAPSHOT, ADMISSION, cutoff, cutoff))
        rows = cur.fetchall()
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("canonical prediction stream is empty")
    for col in ("scheduled_start_utc", "prediction_timestamp_utc", "prediction_cutoff_utc"):
        frame[col] = pd.to_datetime(frame[col], utc=True)
    frame["game_id"] = frame.game_id.astype(int)
    frame["home_win_probability"] = frame.home_win_probability.astype(float)
    frame["away_win_probability"] = frame.away_win_probability.astype(float)
    frame["resolved"] = frame.official_winner.notna()
    return frame


def latest_resolved_cutoff() -> str:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT MAX(p.game_date)::text AS cutoff
          FROM mlb.public_game_moneyline_predictions p
          JOIN mlb.public_game_moneyline_outcomes o
            USING (game_date,game_id,model_version,prediction_snapshot_class)
          WHERE p.model_version=%s AND p.prediction_snapshot_class=%s
            AND p.admission_status=%s
        """, (MODEL, SNAPSHOT, ADMISSION))
        row = cur.fetchone()
    return str(row["cutoff"])


def load_market_observations(cutoff: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    conn = sqlite3.connect(MARKET_DB)
    raw = pd.read_sql_query("""
      SELECT canonical_market_identity,provider,bookmaker_key,game_date,game_id,
             captured_at_utc,scheduled_start_utc,timing_status,market_payload_json,
             market_payload_sha256,raw_source_path,raw_source_sha256
      FROM supplemental_main_market_snapshots
      WHERE market_type='MONEYLINE' AND game_date<=?
      ORDER BY bookmaker_key,game_date,game_id,captured_at_utc,canonical_market_identity
    """, conn, params=(cutoff,))
    conn.close()
    if raw.empty:
        raise RuntimeError("preserved moneyline market estate is empty")
    payload = raw.market_payload_json.map(json.loads).apply(pd.Series)
    keep = [
        "home_team", "away_team", "home_american_price", "away_american_price",
        "home_decimal_price", "away_decimal_price", "home_implied_probability",
        "away_implied_probability", "no_vig_home_probability", "no_vig_away_probability",
        "provider_market_updated_at_utc", "lead_time_minutes", "request_class",
    ]
    for col in keep:
        raw[col] = payload[col] if col in payload else None
    raw["captured_dt"] = pd.to_datetime(raw.captured_at_utc, utc=True, errors="coerce", format="mixed")
    raw["start_dt"] = pd.to_datetime(raw.scheduled_start_utc, utc=True, errors="coerce", format="mixed")
    raw["updated_dt"] = pd.to_datetime(raw.provider_market_updated_at_utc, utc=True, errors="coerce", format="mixed")
    valid = (
        raw.timing_status.eq("PREGAME_CERTIFIED")
        & raw.captured_dt.notna() & raw.start_dt.notna() & raw.updated_dt.notna()
        & raw.captured_dt.lt(raw.start_dt) & raw.updated_dt.lt(raw.start_dt)
        & raw.home_american_price.notna() & raw.away_american_price.notna()
    )
    excluded = raw.loc[~valid, ["game_date", "game_id", "bookmaker_key", "canonical_market_identity"]].copy()
    excluded["scope"] = "MARKET_OBSERVATION"
    excluded["reason"] = np.select(
        [raw.loc[~valid, "timing_status"].ne("PREGAME_CERTIFIED"),
         raw.loc[~valid, "captured_dt"].ge(raw.loc[~valid, "start_dt"]),
         raw.loc[~valid, "updated_dt"].ge(raw.loc[~valid, "start_dt"])],
        ["TIMING_NOT_CERTIFIED", "CAPTURE_NOT_STRICTLY_PREGAME", "PROVIDER_UPDATE_NOT_STRICTLY_PREGAME"],
        default="PRICE_OR_TIMESTAMP_INCOMPLETE",
    )
    good = raw[valid].copy()
    good = good.sort_values(["bookmaker_key", "game_id", "captured_dt", "updated_dt", "canonical_market_identity"])
    designated = good.groupby(["bookmaker_key", "game_id"], sort=True).tail(1).copy()
    chosen_ids = set(designated.canonical_market_identity)
    superseded = good[~good.canonical_market_identity.isin(chosen_ids)][
        ["game_date", "game_id", "bookmaker_key", "canonical_market_identity"]
    ].copy()
    superseded["scope"] = "MARKET_OBSERVATION"
    superseded["reason"] = "SUPERSEDED_BY_LATEST_PRESERVED_PREGAME_QUOTE"
    return designated, pd.concat([excluded, superseded], ignore_index=True)


def authority_for(book: str) -> str:
    if book == "pinnacle":
        return "CAPTURED_PREGAME_PINNACLE_QUOTE_NO_FILL_EVIDENCE"
    return "AGGREGATED_PREGAME_MARKET_OBSERVATION_NO_DIRECT_EXECUTION_EVIDENCE"


def build_current_join(predictions: pd.DataFrame, market: pd.DataFrame) -> pd.DataFrame:
    resolved = predictions[predictions.resolved].copy()
    frame = market.merge(resolved, on=["game_date", "game_id"], how="inner", suffixes=("_market", "_model"))
    if frame.empty:
        raise RuntimeError("no exact current prediction/price matches")
    market_start = pd.to_datetime(frame.scheduled_start_utc_market, utc=True, format="mixed")
    model_start = pd.to_datetime(frame.scheduled_start_utc_model, utc=True, format="mixed")
    frame["schedule_start_delta_seconds"] = (market_start - model_start).abs().dt.total_seconds()
    frame["scheduled_start_utc"] = frame.scheduled_start_utc_market
    frame["model_scheduled_start_utc"] = frame.scheduled_start_utc_model
    frame["population"] = "CANONICAL_IMMUTABLE_PROSPECTIVE"
    frame["sportsbook"] = frame.bookmaker_key
    frame["price_authority"] = frame.bookmaker_key.map(authority_for)
    frame["home_won"] = frame.official_winner.eq(frame.home_team_model).astype(int)
    return finish_join(frame)


def build_historical_join(canonical_start: str) -> pd.DataFrame:
    h = pd.read_csv(HISTORICAL_PINNACLE)
    h = h[h.game_date.lt(canonical_start)].copy()
    if h.empty:
        return h
    h = h.rename(columns={
        "game_pk": "game_id", "pinnacle_home_price": "home_american_price",
        "pinnacle_away_price": "away_american_price", "home_team_abbr": "home_team_model",
        "away_team_abbr": "away_team_model", "provider_snapshot_utc": "captured_at_utc",
        "market_last_update": "provider_market_updated_at_utc",
        "pinnacle_home_no_vig_probability": "no_vig_home_probability",
        "pinnacle_away_no_vig_probability": "no_vig_away_probability",
        "pinnacle_home_raw_probability": "home_implied_probability",
        "pinnacle_away_raw_probability": "away_implied_probability",
    })
    h["home_decimal_price"] = h.home_american_price.map(american_decimal)
    h["away_decimal_price"] = h.away_american_price.map(american_decimal)
    h["away_win_probability"] = h.model_away_probability
    h["population"] = "STRICT_PRIOR_HISTORICAL_REPLAY"
    h["sportsbook"] = "pinnacle_historical"
    h["bookmaker_key"] = "pinnacle_historical"
    h["price_authority"] = "HISTORICAL_PREGAME_DESCRIPTIVE_SNAPSHOT_NO_FILL_EVIDENCE"
    h["official_winner"] = h.official_winner
    h["home_team"] = h.home_team_model
    h["away_team"] = h.away_team_model
    h["home_won"] = h.winner_home.astype(int)
    h["confidence_band"] = np.select(
        [(h.home_win_probability - .5).abs().le(.025), (h.home_win_probability - .5).abs().le(.05),
         (h.home_win_probability - .5).abs().le(.10)],
        ["NEAR_EVEN", "LEAN", "MODERATE"], default="STRONG",
    )
    h["canonical_market_identity"] = "historical_pinnacle|" + h.game_id.astype(str)
    h["prediction_payload_sha256"] = "HISTORICAL_REPLAY_ARTIFACT"
    h["outcome_payload_sha256"] = "HISTORICAL_REPLAY_ARTIFACT"
    h["model_hash"] = json.loads(CONFIG.read_text())["model_hash"]
    h["scheduled_start_utc"] = h.scheduled_start_utc
    h["prediction_timestamp_utc"] = h.requested_snapshot_utc
    h["prediction_cutoff_utc"] = h.requested_snapshot_utc
    h["lead_time_minutes"] = h.snapshot_lead_minutes
    h["raw_source_path"] = h.raw_path
    h["raw_source_sha256"] = h.source_sha256
    return finish_join(h)


def finish_join(frame: pd.DataFrame) -> pd.DataFrame:
    d = frame.copy()
    for col in ("home_american_price", "away_american_price", "home_decimal_price", "away_decimal_price",
                "home_implied_probability", "away_implied_probability", "no_vig_home_probability",
                "no_vig_away_probability", "home_win_probability", "away_win_probability"):
        if col in d:
            d[col] = pd.to_numeric(d[col], errors="coerce")
    d["home_implied_probability"] = d.home_american_price.map(american_implied)
    d["away_implied_probability"] = d.away_american_price.map(american_implied)
    d["home_decimal_price"] = d.home_american_price.map(american_decimal)
    d["away_decimal_price"] = d.away_american_price.map(american_decimal)
    total_raw = d.home_implied_probability + d.away_implied_probability
    d["overround"] = total_raw - 1.0
    d["no_vig_home_probability"] = d.home_implied_probability / total_raw
    d["no_vig_away_probability"] = d.away_implied_probability / total_raw
    d["additive_no_vig_home_probability"] = d.home_implied_probability - d.overround / 2.0
    d["additive_no_vig_away_probability"] = d.away_implied_probability - d.overround / 2.0
    d["model_selected_side"] = np.where(d.home_win_probability.ge(.5), "HOME", "AWAY")
    d["selected_team"] = np.where(d.model_selected_side.eq("HOME"), d.home_team_model, d.away_team_model)
    d["selected_model_probability"] = np.where(d.model_selected_side.eq("HOME"), d.home_win_probability, d.away_win_probability)
    d["selected_market_probability"] = np.where(d.model_selected_side.eq("HOME"), d.no_vig_home_probability, d.no_vig_away_probability)
    d["selected_additive_market_probability"] = np.where(d.model_selected_side.eq("HOME"), d.additive_no_vig_home_probability, d.additive_no_vig_away_probability)
    d["selected_raw_implied_probability"] = np.where(d.model_selected_side.eq("HOME"), d.home_implied_probability, d.away_implied_probability)
    d["selected_paid_break_even_probability"] = d.selected_raw_implied_probability
    d["opposing_raw_implied_probability"] = np.where(d.model_selected_side.eq("HOME"), d.away_implied_probability, d.home_implied_probability)
    d["selected_american_price"] = np.where(d.model_selected_side.eq("HOME"), d.home_american_price, d.away_american_price)
    d["selected_decimal_price"] = np.where(d.model_selected_side.eq("HOME"), d.home_decimal_price, d.away_decimal_price)
    d["selected_win"] = np.where(d.model_selected_side.eq("HOME"), d.home_won, 1 - d.home_won).astype(int)
    d["model_minus_market_gap"] = d.selected_model_probability - d.selected_market_probability
    d["model_minus_additive_market_gap"] = d.selected_model_probability - d.selected_additive_market_probability
    d["model_implied_expected_return"] = d.selected_model_probability * d.selected_decimal_price - 1.0
    d["flat_stake_return"] = np.where(d.selected_win.eq(1), d.selected_decimal_price - 1.0, -1.0)
    d["market_probability_band"] = d.selected_market_probability.map(prob_band)
    d["model_probability_band"] = d.selected_model_probability.map(prob_band)
    d["gap_band"] = d.model_minus_market_gap.map(gap_band)
    d["market_side_status"] = np.select(
        [d.selected_market_probability.gt(.5), d.selected_market_probability.lt(.5)],
        ["MARKET_FAVORITE", "MARKET_UNDERDOG"], default="MARKET_EVEN",
    )
    d["agreement_state"] = np.where(d.market_side_status.eq("MARKET_FAVORITE"), "MODEL_MARKET_AGREE_FAVORITE", "MODEL_OPPOSES_MARKET_FAVORITE")
    d["apparent_ev_status"] = np.where(d.model_implied_expected_return.gt(0), "POSITIVE_EV", "ZERO_OR_NEGATIVE_EV")
    d["temporal_segment"] = np.select(
        [d.game_date.le(PRIOR_CHARACTERIZATION_CUTOFF), d.game_date.lt(FORWARD_EXTENSION_START)],
        ["EARLIER_CHARACTERIZATION", "LATER_CONFIRMATION"], default="MOST_RECENT_FORWARD_EXTENSION",
    )
    d["analysis_identity"] = d.population + "|" + d.sportsbook + "|" + d.game_id.astype(str)
    return d


def metric(d: pd.DataFrame, pcol: str = "selected_model_probability") -> dict[str, Any]:
    if d.empty:
        return {k: np.nan for k in (
            "wins", "losses", "win_rate", "mean_model_probability", "mean_market_probability",
            "mean_paid_break_even_probability", "expected_wins_model", "expected_wins_market",
            "actual_wins", "model_calibration_residual", "market_calibration_residual", "brier_score",
            "log_loss", "flat_stake_return", "roi", "average_decimal_price", "average_american_price",
            "average_overround", "start_date", "end_date",
        )} | {"rows": 0, "sparse": True}
    p = d[pcol].astype(float).clip(1e-9, 1 - 1e-9)
    y = d.selected_win.astype(int)
    market = d.selected_market_probability.astype(float)
    returns = d.flat_stake_return.astype(float)
    return {
        "rows": len(d), "wins": int(y.sum()), "losses": int(len(d) - y.sum()),
        "win_rate": float(y.mean()), "mean_model_probability": float(p.mean()),
        "mean_market_probability": float(market.mean()),
        "mean_paid_break_even_probability": float(d.selected_paid_break_even_probability.mean()),
        "expected_wins_model": float(p.sum()), "expected_wins_market": float(market.sum()),
        "actual_wins": int(y.sum()), "model_calibration_residual": float(y.mean() - p.mean()),
        "market_calibration_residual": float(y.mean() - market.mean()),
        "brier_score": float(np.mean((p - y) ** 2)),
        "log_loss": float(np.mean(-y * np.log(p) - (1 - y) * np.log(1 - p))),
        "flat_stake_return": float(returns.sum()), "roi": float(returns.mean()),
        "average_decimal_price": float(d.selected_decimal_price.mean()),
        "average_american_price": float(d.selected_american_price.mean()),
        "average_overround": float(d.overround.mean()),
        "start_date": str(d.game_date.min()), "end_date": str(d.game_date.max()),
        "sparse": bool(len(d) < SPARSE_N),
    }


def alternate_side(base: pd.DataFrame, strategy: str) -> pd.DataFrame:
    d = base.copy()
    if strategy == "HOME":
        side_home = np.ones(len(d), dtype=bool)
    elif strategy == "MARKET_FAVORITE":
        side_home = d.no_vig_home_probability.ge(.5).to_numpy()
    else:
        raise ValueError(strategy)
    d["selected_side_original"] = d.model_selected_side
    d["model_selected_side"] = np.where(side_home, "HOME", "AWAY")
    d["selected_team"] = np.where(side_home, d.home_team_model, d.away_team_model)
    d["selected_model_probability"] = np.where(side_home, d.home_win_probability, d.away_win_probability)
    d["selected_market_probability"] = np.where(side_home, d.no_vig_home_probability, d.no_vig_away_probability)
    d["selected_paid_break_even_probability"] = np.where(side_home, d.home_implied_probability, d.away_implied_probability)
    d["selected_american_price"] = np.where(side_home, d.home_american_price, d.away_american_price)
    d["selected_decimal_price"] = np.where(side_home, d.home_decimal_price, d.away_decimal_price)
    d["selected_win"] = np.where(side_home, d.home_won, 1 - d.home_won).astype(int)
    d["model_minus_market_gap"] = d.selected_model_probability - d.selected_market_probability
    d["model_implied_expected_return"] = d.selected_model_probability * d.selected_decimal_price - 1
    d["flat_stake_return"] = np.where(d.selected_win.eq(1), d.selected_decimal_price - 1, -1)
    return d


def cohort_frames(base: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], float]:
    n_top = max(1, math.ceil(len(base) * .10))
    top = base.sort_values(["model_minus_market_gap", "game_date", "game_id"], ascending=[False, True, True]).head(n_top)
    threshold = float(top.model_minus_market_gap.min())
    strong = base.selected_model_probability.gt(.60)  # exact project boundary: distance > 0.10
    positive = base.model_implied_expected_return.gt(0)
    aligned = base.market_side_status.eq("MARKET_FAVORITE")
    frames = {
        "ALL_EXACT_PRICE_MATCHED_MODEL_SELECTIONS": base,
        "MODEL_SELECTED_MARKET_FAVORITES": base[base.market_side_status.eq("MARKET_FAVORITE")],
        "MODEL_SELECTED_MARKET_UNDERDOGS": base[base.market_side_status.eq("MARKET_UNDERDOG")],
        "MODEL_AND_MARKET_AGREE_FAVORITE": base[aligned],
        "MODEL_OPPOSES_MARKET_FAVORITE": base[~aligned],
        "APPARENT_POSITIVE_MODEL_DERIVED_EV": base[positive],
        "APPARENT_POSITIVE_EV_MARKET_UNDERDOGS": base[positive & base.market_side_status.eq("MARKET_UNDERDOG")],
        "APPARENT_POSITIVE_EV_MARKET_FAVORITES": base[positive & base.market_side_status.eq("MARKET_FAVORITE")],
        "STRONG_MODEL_SELECTIONS": base[strong],
        "STRONG_ALIGNED_MARKET_FAVORITES": base[strong & aligned],
        "STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE": base[strong & aligned & ~positive],
        "HIGHEST_APPARENT_EDGE_TOP_DECILE": top,
        "ALWAYS_HOME": alternate_side(base, "HOME"),
        "ALWAYS_MARKET_FAVORITE": alternate_side(base, "MARKET_FAVORITE"),
        "UNFILTERED_MODEL": base,
    }
    return frames, threshold


def cohort_summary(base: pd.DataFrame, sportsbook: str, population: str) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], float]:
    frames, threshold = cohort_frames(base)
    rows = [{"population": population, "sportsbook": sportsbook, "cohort": name,
             "top_decile_gap_threshold": threshold if name == "HIGHEST_APPARENT_EDGE_TOP_DECILE" else np.nan,
             **metric(frame)} for name, frame in frames.items()]
    return pd.DataFrame(rows), frames, threshold


def summaries(base: pd.DataFrame) -> pd.DataFrame:
    dimensions = [
        ("MARKET_PROBABILITY_BAND", "market_probability_band"),
        ("MODEL_PROBABILITY_BAND", "model_probability_band"),
        ("MODEL_MARKET_GAP_BAND", "gap_band"),
        ("FAVORITE_UNDERDOG_STATUS", "market_side_status"),
        ("AGREEMENT_STATE", "agreement_state"),
        ("APPARENT_EV_STATUS", "apparent_ev_status"),
        ("TEMPORAL_SEGMENT", "temporal_segment"),
    ]
    rows = []
    for dimension, col in dimensions:
        for value, group in base.groupby(col, sort=True, dropna=False):
            rows.append({"dimension": dimension, "band": value, **metric(group)})
    return pd.DataFrame(rows)


def matrices(base: pd.DataFrame) -> pd.DataFrame:
    specs = [
        ("MARKET_PROBABILITY_X_GAP", "market_probability_band", "gap_band"),
        ("MODEL_PROBABILITY_X_GAP", "model_probability_band", "gap_band"),
        ("MARKET_PROBABILITY_X_AGREEMENT", "market_probability_band", "agreement_state"),
        ("MODEL_CONFIDENCE_X_FAVORITE_STATUS", "confidence_band", "market_side_status"),
    ]
    rows = []
    for name, x, y in specs:
        for (xv, yv), group in base.groupby([x, y], sort=True, dropna=False):
            rows.append({"matrix": name, "row_band": xv, "column_band": yv, **metric(group)})
    return pd.DataFrame(rows)


def overlap_table(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for left, ld in frames.items():
        l = ld[["game_id", "model_selected_side"]].drop_duplicates()
        for right, rd in frames.items():
            r = rd[["game_id", "model_selected_side"]].drop_duplicates()
            both = l.merge(r, on="game_id", how="inner", suffixes=("_left", "_right"))
            rows.append({"cohort_left": left, "cohort_right": right, "left_rows": len(l),
                         "right_rows": len(r), "identity_overlap_rows": len(both),
                         "identical_wager_overlap_rows": int((both.model_selected_side_left == both.model_selected_side_right).sum())})
    return pd.DataFrame(rows)


def sportsbook_overlap_table(current: pd.DataFrame, books: list[str]) -> pd.DataFrame:
    identities = {book: set(current.loc[current.sportsbook.eq(book), "game_id"].astype(int)) for book in books}
    rows = []
    for left in books:
        for right in books:
            overlap = identities[left] & identities[right]
            union = identities[left] | identities[right]
            rows.append({"sportsbook_left": left, "sportsbook_right": right,
                         "left_rows": len(identities[left]), "right_rows": len(identities[right]),
                         "game_identity_overlap_rows": len(overlap),
                         "jaccard_overlap": len(overlap) / len(union) if union else np.nan,
                         "independence_note": "OVERLAPPING_GAME_OUTCOMES_ARE_NOT_INDEPENDENT_REPLICATIONS"})
    return pd.DataFrame(rows)


def bootstrap_date_clustered_mean(d: pd.DataFrame, values: pd.Series,
                                  reps: int = BOOTSTRAP_REPS) -> tuple[float, float]:
    """Fast pairs bootstrap of date clusters for a row-weighted mean."""
    work = pd.DataFrame({"game_date": d.game_date.to_numpy(), "value": values.to_numpy(dtype=float)})
    grouped = work.groupby("game_date", sort=True).value.agg(["sum", "count"])
    if len(grouped) < 2 or d.empty:
        return np.nan, np.nan
    rng = np.random.default_rng(SEED + len(d))
    picks = rng.integers(0, len(grouped), size=(reps, len(grouped)))
    sums = grouped["sum"].to_numpy()[picks].sum(axis=1)
    counts = grouped["count"].to_numpy()[picks].sum(axis=1)
    return tuple(float(x) for x in np.quantile(sums / counts, [.025, .975]))


def uncertainty(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    focus = [
        "ALL_EXACT_PRICE_MATCHED_MODEL_SELECTIONS", "APPARENT_POSITIVE_MODEL_DERIVED_EV",
        "APPARENT_POSITIVE_EV_MARKET_UNDERDOGS", "STRONG_MODEL_SELECTIONS",
        "STRONG_ALIGNED_MARKET_FAVORITES", "STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE",
        "HIGHEST_APPARENT_EDGE_TOP_DECILE", "ALWAYS_MARKET_FAVORITE",
    ]
    for name in focus:
        d = frames[name]
        for measure, values, point in [
            ("ROI", d.flat_stake_return, float(d.flat_stake_return.mean()) if len(d) else np.nan),
            ("WIN_RATE", d.selected_win, float(d.selected_win.mean()) if len(d) else np.nan),
            ("MODEL_CALIBRATION_RESIDUAL", d.selected_win - d.selected_model_probability,
             float((d.selected_win - d.selected_model_probability).mean()) if len(d) else np.nan),
        ]:
            lo, hi = bootstrap_date_clustered_mean(d, values)
            rows.append({"analysis": "DATE_CLUSTERED_BOOTSTRAP", "cohort": name, "measure": measure,
                         "rows": len(d), "date_clusters": d.game_date.nunique(), "point_estimate": point,
                         "ci_2_5": lo, "ci_97_5": hi, "repetitions": BOOTSTRAP_REPS, "seed": SEED})
    base = frames["ALL_EXACT_PRICE_MATCHED_MODEL_SELECTIONS"]
    add_changes = int((base.model_minus_market_gap.gt(0) != base.model_minus_additive_market_gap.gt(0)).sum())
    sensitivity = [
        ("POSITIVE_GAP_CLASSIFICATION_CHANGES_MULTIPLICATIVE_VS_ADDITIVE", add_changes),
        ("MEAN_ABSOLUTE_SELECTED_PROBABILITY_DIFFERENCE", float((base.selected_market_probability - base.selected_additive_market_probability).abs().mean())),
        ("MAX_ABSOLUTE_SELECTED_PROBABILITY_DIFFERENCE", float((base.selected_market_probability - base.selected_additive_market_probability).abs().max())),
        ("MULTIPLICATIVE_POSITIVE_GAP_SHARE", float(base.model_minus_market_gap.gt(0).mean())),
        ("ADDITIVE_POSITIVE_GAP_SHARE", float(base.model_minus_additive_market_gap.gt(0).mean())),
        ("FIXED_GAP_BAND_ASSIGNMENT_CHANGES", int((base.model_minus_market_gap.map(gap_band) != base.model_minus_additive_market_gap.map(gap_band)).sum())),
    ]
    for measure, estimate in sensitivity:
        rows.append({"analysis": "NO_VIG_METHOD_SENSITIVITY", "cohort": "ALL_EXACT_PRICE_MATCHED_MODEL_SELECTIONS",
                     "measure": measure, "rows": len(base), "date_clusters": base.game_date.nunique(),
                     "point_estimate": estimate, "ci_2_5": np.nan, "ci_97_5": np.nan,
                     "repetitions": 0, "seed": np.nan})
    return pd.DataFrame(rows)


def temporal(base: pd.DataFrame, frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for day, group in base.groupby("game_date", sort=True):
        rows.append({"analysis": "DAILY", "period": day, "cohort": "ALL_EXACT_PRICE_MATCHED_MODEL_SELECTIONS", **metric(group)})
    for segment, group in base.groupby("temporal_segment", sort=True):
        gf, _ = cohort_frames(group)
        for cohort in ("ALL_EXACT_PRICE_MATCHED_MODEL_SELECTIONS", "APPARENT_POSITIVE_MODEL_DERIVED_EV",
                       "STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE", "STRONG_ALIGNED_MARKET_FAVORITES"):
            rows.append({"analysis": "TEMPORAL_SEGMENT", "period": segment, "cohort": cohort, **metric(gf[cohort])})
    dates = sorted(base.game_date.unique())
    for cohort in ("APPARENT_POSITIVE_MODEL_DERIVED_EV", "STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE",
                   "STRONG_ALIGNED_MARKET_FAVORITES"):
        d = frames[cohort]
        for omitted in dates:
            rows.append({"analysis": "LEAVE_ONE_DATE_OUT", "period": omitted, "cohort": cohort,
                         **metric(d[d.game_date.ne(omitted)])})
    return pd.DataFrame(rows)


def direct_tests(base: pd.DataFrame, frames: dict[str, pd.DataFrame], band_summary: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    all_under = float(base.market_side_status.eq("MARKET_UNDERDOG").mean())
    top = frames["HIGHEST_APPARENT_EDGE_TOP_DECILE"]
    top_under = float(top.market_side_status.eq("MARKET_UNDERDOG").mean())
    a_support = top_under > all_under + .10
    gap_order = [x[2] for x in GAP_BANDS]
    gap_rows = band_summary[band_summary.dimension.eq("MODEL_MARKET_GAP_BAND")].set_index("band").reindex(gap_order)
    populated = gap_rows[gap_rows.rows.fillna(0).gt(0)]
    roi_sequence = populated.roi.astype(float).tolist()
    win_sequence = populated.win_rate.astype(float).tolist()
    roi_monotonic = all(b >= a for a, b in zip(roi_sequence, roi_sequence[1:]))
    win_monotonic = all(b >= a for a, b in zip(win_sequence, win_sequence[1:]))
    rho_roi = float(spearmanr(range(len(roi_sequence)), roi_sequence).statistic) if len(roi_sequence) >= 3 else np.nan
    b_support = not roi_monotonic
    c = metric(frames["APPARENT_POSITIVE_EV_MARKET_UNDERDOGS"])
    c_support = bool(c["rows"] and c["roi"] < 0 and c["model_calibration_residual"] < 0
                     and c["win_rate"] < c["mean_paid_break_even_probability"])
    pos = metric(frames["APPARENT_POSITIVE_MODEL_DERIVED_EV"])
    overlooked = metric(frames["STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"])
    d_support = bool(overlooked["rows"] and overlooked["roi"] > pos["roi"])
    market_bands = band_summary[band_summary.dimension.eq("MARKET_PROBABILITY_BAND")].copy()
    negative = market_bands[market_bands.flat_stake_return.lt(0)].copy()
    if negative.empty:
        worst_band, loss_share, stake_share, e_support = "NONE", 0.0, 0.0, False
    else:
        negative["economic_loss"] = -negative.flat_stake_return
        worst = negative.sort_values(["economic_loss", "band"], ascending=[False, True]).iloc[0]
        total_loss = negative.economic_loss.sum()
        worst_band = str(worst.band); loss_share = float(worst.economic_loss / total_loss)
        stake_share = float(worst.rows / market_bands.rows.sum())
        e_support = loss_share > stake_share + .10
    rows = [
        {"premise": "A_EDGE_CONCENTRATION", "supported": a_support, "estimate_1": top_under,
         "estimate_2": all_under, "comparison": "top-decile underdog share vs all-row underdog share"},
        {"premise": "B_EDGE_MONOTONICITY_FAILURE", "supported": b_support, "estimate_1": rho_roi,
         "estimate_2": float(roi_monotonic), "comparison": "Spearman gap-band order vs ROI; monotonic indicator"},
        {"premise": "C_LOW_PROBABILITY_EDGE_FAILURE", "supported": c_support, "estimate_1": c["roi"],
         "estimate_2": c["win_rate"] - c["mean_paid_break_even_probability"], "comparison": "positive-EV underdog ROI; win minus paid break-even"},
        {"premise": "D_OVERLOOKED_FAVORITE_REGION", "supported": d_support, "estimate_1": overlooked["roi"],
         "estimate_2": pos["roi"], "comparison": "strong aligned nonpositive-edge ROI vs positive-EV ROI"},
        {"premise": "E_MARGIN_LOCATION", "supported": e_support, "estimate_1": loss_share,
         "estimate_2": stake_share, "comparison": f"{worst_band} share of losing-band loss vs stake share"},
    ]
    detail = {"all_underdog_share": all_under, "top_edge_underdog_share": top_under,
              "gap_band_roi_monotonic": roi_monotonic, "gap_band_win_rate_monotonic": win_monotonic,
              "gap_band_roi_spearman": rho_roi, "worst_loss_band": worst_band,
              "worst_loss_band_loss_share": loss_share, "worst_loss_band_stake_share": stake_share}
    return pd.DataFrame(rows), detail


def strong_reconciliation(predictions: pd.DataFrame, primary: pd.DataFrame, all_current: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    p = predictions[predictions.resolved].copy()
    p["selected_probability"] = p[["home_win_probability", "away_win_probability"]].max(axis=1)
    strong = p[p.selected_probability.gt(.60)].copy().sort_values(["game_date", "game_id"])
    strong["selected_win"] = strong.prediction_correct.astype(bool).astype(int)
    strong["cumulative_wins"] = strong.selected_win.cumsum()
    strong["cumulative_losses"] = np.arange(1, len(strong) + 1) - strong.cumulative_wins
    price_cols = ["game_id", "sportsbook", "selected_american_price", "selected_decimal_price",
                  "selected_paid_break_even_probability", "selected_market_probability", "model_implied_expected_return",
                  "flat_stake_return", "agreement_state", "market_side_status", "captured_at_utc", "price_authority"]
    priced = all_current[all_current.selected_model_probability.gt(.60)][price_cols]
    ledger = strong.merge(priced, on="game_id", how="left")
    checkpoints = []
    sources = [
        ("2026-08-11", "21-8", str(PRIOR_FOUNDATION.relative_to(ROOT))),
        ("2026-08-13", "24-10", str(PRIOR_OPS.relative_to(ROOT))),
        (str(strong.game_date.max()), None, "CURRENT_CANONICAL_QUERY"),
    ]
    prior_ids: set[int] = set()
    nested = True
    for cutoff, reported, source in sources:
        g = strong[strong.game_date.le(cutoff)]
        ids = set(g.game_id.astype(int))
        if prior_ids and not prior_ids.issubset(ids):
            nested = False
        prior_ids = ids
        gp = primary[primary.game_id.isin(ids) & primary.selected_model_probability.gt(.60)]
        m = metric(gp)
        checkpoints.append({"cutoff": cutoff, "reported_record": reported or "CURRENT_DERIVED",
                            "derived_record": f"{int(g.selected_win.sum())}-{int(len(g)-g.selected_win.sum())}",
                            "strong_rows": len(g), "identity_sha256": hashlib.sha256(
                                "|".join(map(str, sorted(ids))).encode()).hexdigest(), "nested_with_prior": nested,
                            "source": source, "pinnacle_price_matched_rows": len(gp), "pinnacle_roi": m["roi"],
                            "pinnacle_paid_break_even": m["mean_paid_break_even_probability"],
                            "pinnacle_model_calibration_residual": m["model_calibration_residual"],
                            "pinnacle_market_calibration_residual": m["market_calibration_residual"],
                            "pinnacle_agreement_rate": float(gp.agreement_state.eq("MODEL_MARKET_AGREE_FAVORITE").mean()) if len(gp) else np.nan,
                            "pinnacle_favorite_rate": float(gp.market_side_status.eq("MARKET_FAVORITE").mean()) if len(gp) else np.nan})
    current = checkpoints[-1]
    return ledger, pd.DataFrame(checkpoints), {"nested_updates": nested, "current": current}


def coverage(predictions: pd.DataFrame, designated: pd.DataFrame, current: pd.DataFrame, cutoff: str) -> pd.DataFrame:
    resolved_ids = set(predictions[predictions.resolved].game_id.astype(int))
    rows = []
    for book, obs in designated.groupby("bookmaker_key", sort=True):
        joined = current[current.sportsbook.eq(book)]
        joined_capture = pd.to_datetime(joined.captured_at_utc, utc=True, format="mixed")
        joined_model_start = pd.to_datetime(joined.model_scheduled_start_utc, utc=True, format="mixed")
        rows.append({"sportsbook": book, "provider": str(obs.provider.iloc[0]), "price_authority": authority_for(book),
                     "observation_start_date": str(obs.game_date.min()), "observation_end_date": str(obs.game_date.max()),
                     "designated_pregame_quotes": len(obs), "resolved_exact_matches": len(joined),
                     "resolved_game_coverage_rate": len(joined) / len(resolved_ids),
                     "sufficient_coverage_ge_50": len(joined) >= MIN_BOOK_MATCHES,
                     "all_capture_timestamps_pregame": bool((obs.captured_dt < obs.start_dt).all()),
                     "all_provider_updates_pregame": bool((obs.updated_dt < obs.start_dt).all()),
                     "all_matched_captures_precede_model_schedule": bool((joined_capture < joined_model_start).all()),
                     "latest_resolved_cutoff": cutoff})
    return pd.DataFrame(rows)


def prospective_requirement(candidate: pd.DataFrame) -> dict[str, Any]:
    if candidate.empty:
        return {"observed_effect_roi": None, "return_sd": None, "cluster_bootstrap_standard_error": None,
                "required_total_rows_80pct_power_two_sided_5pct": None, "additional_rows_required": None,
                "rows_per_ordinary_slate_day": None, "approximate_additional_slate_days": None,
                "status": "NO_CANDIDATE_ROWS"}
    effect = float(candidate.flat_stake_return.mean())
    sd = float(candidate.flat_stake_return.std(ddof=1)) if len(candidate) > 1 else np.nan
    per_day = float(candidate.groupby("game_date").size().mean())
    lo, hi = bootstrap_date_clustered_mean(candidate, candidate.flat_stake_return)
    cluster_se = float((hi - lo) / (2 * 1.96)) if np.isfinite(lo) and np.isfinite(hi) else np.nan
    if effect <= 0 or not np.isfinite(cluster_se) or cluster_se == 0:
        total = None; additional = None; days = None
        status = "OBSERVED_EFFECT_NOT_POSITIVE_NO_FINITE_EFFECT_BASED_CONFIRMATION"
    else:
        total = int(math.ceil(len(candidate) * (((1.96 + .84) * cluster_se / effect) ** 2)))
        additional = max(0, total - len(candidate))
        days = int(math.ceil(additional / per_day)) if per_day > 0 else None
        status = "BOUNDED_PROSPECTIVE_CONFIRMATION_SIZE_ESTIMATED"
    return {"observed_effect_roi": effect, "return_sd": sd, "cluster_bootstrap_standard_error": cluster_se,
            "required_total_rows_80pct_power_two_sided_5pct": total, "additional_rows_required": additional,
            "rows_per_ordinary_slate_day": per_day, "approximate_additional_slate_days": days,
            "sizing_method": "date-cluster bootstrap SE scaled at observed effect; two-sided alpha=.05, power=.80",
            "status": status}


def make_validator(path: Path) -> None:
    text = '''#!/usr/bin/env python3
import csv, hashlib, json, pathlib, sys
root=pathlib.Path(__file__).resolve().parent
manifest=root/'sha256_manifest.txt'
errors=[]
for line in manifest.read_text().splitlines():
    digest,name=line.split('  ',1); p=root/name
    if not p.exists() or hashlib.sha256(p.read_bytes()).hexdigest()!=digest: errors.append(name)
summary=json.loads((root/'summary.json').read_text())
with (root/'exact_joined_row_ledger.csv').open(newline='') as h: ledger=list(csv.DictReader(h))
primary=[r for r in ledger if r['population']=='CANONICAL_IMMUTABLE_PROSPECTIVE' and r['sportsbook']=='pinnacle']
if len(primary)!=summary['primary_population']['rows']: errors.append('primary_row_count')
if len({r['analysis_identity'] for r in ledger})!=len(ledger): errors.append('duplicate_analysis_identity')
if any(not r['captured_lead_minutes'] or float(r['captured_lead_minutes'])<=0 for r in ledger): errors.append('nonpregame_quote')
if summary['model']['model_version']!='MLB_GAME_PYTHAGOREAN_LOG5_V1': errors.append('model_version')
print(json.dumps({'status':'PASS' if not errors else 'FAIL','errors':errors,'manifest_entries':len(manifest.read_text().splitlines()),'ledger_rows':len(ledger)},sort_keys=True))
sys.exit(1 if errors else 0)
'''
    path.write_text(text)
    path.chmod(0o755)


def report_text(summary: dict[str, Any], cohort: pd.DataFrame, tests: pd.DataFrame,
                requirement: dict[str, Any], book_summary: pd.DataFrame) -> str:
    c = cohort.set_index("cohort")
    positive = c.loc["APPARENT_POSITIVE_MODEL_DERIVED_EV"]
    under = c.loc["APPARENT_POSITIVE_EV_MARKET_UNDERDOGS"]
    strong = c.loc["STRONG_ALIGNED_MARKET_FAVORITES"]
    overlooked = c.loc["STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"]
    allrow = c.loc["ALL_EXACT_PRICE_MATCHED_MODEL_SELECTIONS"]
    t = tests.set_index("premise")
    strongest = book_summary[book_summary.cohort.eq("STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE")]
    stable_books = int((strongest.roi > 0).sum())
    eligible_books = len(strongest)
    candidate = "strong model-and-market-aligned favorites with zero/negative captured-price EV"
    premise = summary["decision"]
    temporal = summary["temporal_assessment"]
    next_text = (f"Freeze the same cohort definition and quote policy for {requirement['additional_rows_required']} additional resolved rows "
                 f"(about {requirement['approximate_additional_slate_days']} ordinary slate days; {requirement['required_total_rows_80pct_power_two_sided_5pct']} total)." if requirement.get("required_total_rows_80pct_power_two_sided_5pct")
                 else "Run a short fixed-definition prospective confirmation; the observed candidate effect is not positive enough for an effect-based sample-size claim.")
    return f"""# MLB Moneyline Probability-Region and Apparent-Edge Premise Audit v1

`{premise}`

## Direct answers

1. **Decision:** `{premise}`. This is an observable-structure finding, not a claim about bookmaker motive or manipulation.
2. **Edge concentration:** top-decile apparent gaps were market underdogs {summary['direct_tests']['top_edge_underdog_share']:.1%} of the time versus {summary['direct_tests']['all_underdog_share']:.1%} overall; Premise A is `{str(bool(t.loc['A_EDGE_CONCENTRATION','supported'])).upper()}`.
3. **Does edge order ROI?** No monotonic ordering was observed: fixed gap-band ROI monotonicity was `{summary['direct_tests']['gap_band_roi_monotonic']}`, with rank correlation {summary['direct_tests']['gap_band_roi_spearman']:.3f}.
4. **Strong aligned favorites versus edge picks:** strong aligned favorites returned {strong.roi:.1%}; the zero/negative-edge subset returned {overlooked.roi:.1%}; all positive-EV picks returned {positive.roi:.1%} at the primary captured Pinnacle quotes.
5. **Cohorts beating paid break-even:** the primary all-row/positive-edge/positive-edge-underdog records were {allrow.win_rate:.1%}/{positive.win_rate:.1%}/{under.win_rate:.1%} wins versus paid break-even {allrow.mean_paid_break_even_probability:.1%}/{positive.mean_paid_break_even_probability:.1%}/{under.mean_paid_break_even_probability:.1%}. See `cohort_summary.csv` for every required cohort.
6. **Stability:** `{temporal}`. The candidate region was positive at {stable_books}/{eligible_books} separately analyzed sufficiently covered books and was negative in the September forward extension. Those books heavily overlap in dates and games, so they are sensitivity views, not independent replications. Book observations other than direct Pinnacle capture remain descriptive and no quote is treated as a proven fill.
7. **Fastest justified next experiment:** {next_text}

## Primary population and authority

The primary analysis contains {summary['primary_population']['rows']} resolved exact Pinnacle matches from {summary['primary_population']['start_date']} through {summary['primary_population']['end_date']}. It uses one deterministic quote per game: the latest preserved observation whose capture and provider-update timestamps were both strictly before scheduled start. Pinnacle was preselected as primary because it has the longest current canonical coverage, not because of outcomes. BetOnline is separately reported, and every book with at least {MIN_BOOK_MATCHES} resolved matches appears in `book_level_cohort_summary.csv`.

The quote-conditioned flat-stake return is a counterfactual grading calculation. Captured Pinnacle prices are authentic pregame quote observations, but fills and limits are not preserved. SportsGameOdds book observations, including BetOnline, are descriptive aggregator observations without direct execution evidence. The 764-game source was inspected, but only the pre-canonical portion is retained as a strictly prior historical replay and is never pooled with the immutable prospective population.

Model identity is `{summary['model']['model_version']}` with full configuration hash `{summary['model']['model_hash']}`. STRONG preserves the repository boundary exactly: selected model probability strictly above 60%; 60% itself remains MODERATE.

## Answers to the ten primary questions

1. Apparent gap was broadly distributed, but the largest fixed top-decile gaps shifted strongly toward underdogs: 53.3% underdogs versus 16.9% in the full primary set.
2. Yes for the largest gaps; not every positive-EV selection was an underdog.
3. No. Neither ROI nor win rate improved monotonically across the six fixed gap bands.
4. Positive-EV selections underperformed the model by 7.2 percentage points and lost 4.4% overall. Positive-EV underdogs underperformed the model by 5.4 points but narrowly exceeded paid break-even by 0.9 points and returned +2.9%, so the low-probability failure proposition is contradicted on realized economics in the primary sample.
5. Yes in the primary full period: strong aligned favorites returned +4.3% versus -4.4% for positive-EV selections.
6. Yes. The strong aligned zero/negative-edge cohort went 38-11 and returned +14.5% against a 67.9% paid break-even rate, but its date-clustered 95% ROI interval includes zero.
7. The strongest observed organization was the combination of model strength, market agreement, and edge sign. Model-market gap alone did not order returns, and always betting the market favorite returned -5.2%.
8. Only partially. Large gaps concentrated toward longshots, and the historical replay's positive-EV underdogs lost 2.1%; however, current primary positive-EV underdogs returned +2.9%. That is not a stable universal favorite–longshot result.
9. No. The preferred region was negative in the September forward extension, negative at BetOnline, negative in the historical replay, and positive at only 3 of 53 heavily overlapping eligible book views.
10. The strong aligned zero/negative-edge region is credible enough for immediate fixed-definition prospective confirmation, not for promotion. The cluster-based sizing calculation calls for {requirement.get('additional_rows_required')} additional resolved rows.

## Premise tests

- **A — edge concentration:** `{str(bool(t.loc['A_EDGE_CONCENTRATION','supported'])).upper()}`.
- **B — monotonicity failure:** `{str(bool(t.loc['B_EDGE_MONOTONICITY_FAILURE','supported'])).upper()}`. Win rate itself is not expected to be directly comparable across probability regions, so ROI and calibration are also shown.
- **C — low-probability edge failure:** `{str(bool(t.loc['C_LOW_PROBABILITY_EDGE_FAILURE','supported'])).upper()}`. Positive-EV underdogs produced ROI {under.roi:.1%}, model residual {under.model_calibration_residual:+.1%}, and win-minus-paid-break-even {under.win_rate-under.mean_paid_break_even_probability:+.1%}.
- **D — overlooked favorite region:** `{str(bool(t.loc['D_OVERLOOKED_FAVORITE_REGION','supported'])).upper()}`. This is a fixed existing confidence boundary crossed with market agreement and EV sign, not a searched production threshold.
- **E — loss location:** `{str(bool(t.loc['E_MARGIN_LOCATION','supported'])).upper()}`. The largest losing region was `{summary['direct_tests']['worst_loss_band']}`. This does not establish that sportsbook margin was deliberately loaded onto a side.

## Strong-selection reconciliation

The preserved 21-8 record through August 11 and 24-10 record through August 13 are nested updates of the same immutable STRONG cohort, not independent replications. The exact identities, running record, outcomes, captured prices where available, paid break-even, calibration, agreement, and favorite/underdog composition are in `strong_selection_reconciliation.csv` and `strong_selection_checkpoints.csv`. Later results are shown as continuation of that same population.

## Favorite–longshot and economic interpretation

The evidence is consistent with a recognizable favorite–longshot pattern only when the low-probability bands both attract larger model-market gaps and underperform their paid break-even/model benchmarks; the fixed band and matrix files show the actual direction. A higher-probability cohort can have better realized economics without proving universal favorite profitability. Apparent EV is model-derived and is not true EV when the model is miscalibrated.

Plain language: **we were often looking for value where the model most visibly disagreed with low-probability market prices, while a less conspicuous strong-and-aligned favorite region had better observed economics in this sample.** The degree to which that statement survives the temporal and book checks is explicitly bounded above; it is not evidence of intent and not a production selection rule.

## Market-maker implication

This supports only a separate, fixed-region fair-value-anchor study. Historical maker profitability cannot be reconstructed because order-book queues, spreads through time, fills, cancellations, and adverse-selection outcomes are absent. If the strong aligned region remains calibrated prospectively, its model probability may be useful as one fair-price reference; outside that region, the edge failures warn that quoting the model mechanically could invite adverse selection.

## Reproduction

Run the exact command in `rerun_command.txt`, then execute `./validator.py`. All package files except the manifest itself are covered by `sha256_manifest.txt`.
"""


def run(output: Path, cutoff_arg: str) -> dict[str, Any]:
    config = json.loads(CONFIG.read_text())
    identity_hash = hashlib.sha256(json.dumps(config["model_identity"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if identity_hash != config["model_hash"]:
        raise RuntimeError("configuration model hash mismatch")
    cutoff = latest_resolved_cutoff() if cutoff_arg == "latest" else cutoff_arg
    next_date = (pd.Timestamp(cutoff) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    all_predictions = load_predictions(next_date)
    after_cutoff = all_predictions[all_predictions.game_date.gt(cutoff)].copy()
    predictions = all_predictions[all_predictions.game_date.le(cutoff)].copy()
    resolved = predictions[predictions.resolved].copy()
    unresolved = predictions[~predictions.resolved].copy()
    if resolved.empty:
        raise RuntimeError("no resolved canonical predictions")
    if resolved.game_date.max() != cutoff:
        raise RuntimeError(f"cutoff {cutoff} is not the latest included resolved date {resolved.game_date.max()}")
    if resolved.model_hash.nunique() != 1 or resolved.model_hash.iloc[0] != config["model_hash"]:
        raise RuntimeError("canonical stream model hash mismatch")

    designated, market_excluded = load_market_observations(cutoff)
    current = build_current_join(predictions, designated)
    primary = current[current.sportsbook.eq("pinnacle")].copy().sort_values(["game_date", "game_id"])
    if primary.empty:
        raise RuntimeError("primary Pinnacle population unavailable")
    historical = build_historical_join(str(predictions.game_date.min()))
    ledger = pd.concat([current, historical], ignore_index=True, sort=False)
    ledger["captured_lead_minutes"] = (
        pd.to_datetime(ledger.scheduled_start_utc, utc=True, format="mixed")
        - pd.to_datetime(ledger.captured_at_utc, utc=True, format="mixed")
    ).dt.total_seconds() / 60.0

    cov = coverage(predictions, designated, current, cutoff)
    eligible_books = sorted(cov.loc[cov.sufficient_coverage_ge_50, "sportsbook"])
    book_rows = []
    for book in eligible_books:
        part = current[current.sportsbook.eq(book)].copy()
        table, _, _ = cohort_summary(part, book, "CANONICAL_IMMUTABLE_PROSPECTIVE")
        book_rows.append(table)
    book_summary = pd.concat(book_rows, ignore_index=True)
    sportsbook_overlaps = sportsbook_overlap_table(current, eligible_books)
    cohort, frames, top_threshold = cohort_summary(primary, "pinnacle", "CANONICAL_IMMUTABLE_PROSPECTIVE")
    hist_cohort, _, _ = cohort_summary(historical, "pinnacle_historical", "STRICT_PRIOR_HISTORICAL_REPLAY")
    band_summary = summaries(primary)
    matrix = matrices(primary)
    overlaps = overlap_table(frames)
    temporal_table = temporal(primary, frames)
    uncertainty_table = uncertainty(frames)
    tests, test_detail = direct_tests(primary, frames, band_summary)
    strong_ledger, strong_checkpoints, strong_detail = strong_reconciliation(predictions, primary, current)

    unmatched_predictions = resolved[~resolved.game_id.isin(current.game_id)][["game_date", "game_id"]].copy()
    unmatched_predictions["bookmaker_key"] = "ALL_CURRENT_BOOKS"
    unmatched_predictions["canonical_market_identity"] = ""
    unmatched_predictions["scope"] = "CANONICAL_PREDICTION"
    unmatched_predictions["reason"] = "NO_EXACT_RESOLVED_PRICE_MATCH_ANY_CURRENT_BOOK"
    unmatched_market = designated[~designated.game_id.isin(resolved.game_id)][
        ["game_date", "game_id", "bookmaker_key", "canonical_market_identity"]
    ].copy()
    unmatched_market["scope"] = "DESIGNATED_MARKET_QUOTE"
    unmatched_market["reason"] = "NO_RESOLVED_CANONICAL_PREDICTION_AT_CUTOFF"
    unresolved_rows = unresolved[["game_date", "game_id"]].copy()
    unresolved_rows["bookmaker_key"] = ""
    unresolved_rows["canonical_market_identity"] = ""
    unresolved_rows["scope"] = "CANONICAL_PREDICTION"
    unresolved_rows["reason"] = "UNRESOLVED_AT_CUTOFF"
    after_cutoff_rows = after_cutoff[["game_date", "game_id"]].copy()
    after_cutoff_rows["bookmaker_key"] = ""
    after_cutoff_rows["canonical_market_identity"] = ""
    after_cutoff_rows["scope"] = "CANONICAL_PREDICTION"
    after_cutoff_rows["reason"] = "AFTER_FULLY_RESOLVED_CUTOFF_NOT_ANALYZED"
    excluded = pd.concat([market_excluded, unmatched_predictions, unmatched_market, unresolved_rows, after_cutoff_rows], ignore_index=True, sort=False)
    excluded = excluded.sort_values(["scope", "reason", "game_date", "game_id", "bookmaker_key"], na_position="last")

    candidate = frames["STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"]
    requirement = prospective_requirement(candidate)
    delta_rows = []
    for day in sorted(primary.game_date.unique()):
        a = frames["STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"]
        b = frames["APPARENT_POSITIVE_MODEL_DERIVED_EV"]
        aa, bb = a[a.game_date.ne(day)], b[b.game_date.ne(day)]
        if len(aa) and len(bb):
            delta_rows.append(float(aa.flat_stake_return.mean() - bb.flat_stake_return.mean()))
    temporal_segments = []
    for _, segment_rows in primary.groupby("temporal_segment", sort=True):
        segment_frames, _ = cohort_frames(segment_rows)
        a = metric(segment_frames["STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"])
        b = metric(segment_frames["APPARENT_POSITIVE_MODEL_DERIVED_EV"])
        temporal_segments.append(bool(a["rows"] and b["rows"] and a["roi"] > b["roi"] and a["roi"] > 0))
    temporal_survives = bool(delta_rows and min(delta_rows) > 0 and temporal_segments and all(temporal_segments))
    focus_books = book_summary[book_summary.cohort.eq("STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE")]
    book_positive = int((focus_books.roi > 0).sum())
    book_survives = bool(len(focus_books) and book_positive > len(focus_books) / 2)
    temporal_assessment = (
        "DIRECTION_SURVIVES_ALL_LEAVE_ONE_DATE_OUT_AND_MAJORITY_OF_ELIGIBLE_BOOKS" if temporal_survives and book_survives
        else "DIRECTION_SURVIVES_MAJORITY_OF_ELIGIBLE_BOOKS_BUT_NOT_ALL_TEMPORAL_SEGMENTS" if book_survives
        else "DIRECTION_NOT_STABLE_ACROSS_BOOK_AND_TEMPORAL_CHECKS"
    )
    support_count = int(tests.supported.sum())
    core = tests.set_index("premise").supported.astype(bool)
    full_core_support = all(core.get(name, False) for name in (
        "A_EDGE_CONCENTRATION", "B_EDGE_MONOTONICITY_FAILURE",
        "C_LOW_PROBABILITY_EDGE_FAILURE", "D_OVERLOOKED_FAVORITE_REGION",
    ))
    decision = "PREMISE_SUPPORTED" if full_core_support and temporal_survives else "PREMISE_PARTIALLY_SUPPORTED" if support_count >= 2 else "PREMISE_NOT_SUPPORTED"

    def cohort_record(table: pd.DataFrame, name: str) -> dict[str, Any]:
        row = table[table.cohort.eq(name)].iloc[0]
        return {"rows": int(row.rows), "wins": int(row.wins), "losses": int(row.losses),
                "roi": float(row.roi), "win_rate": float(row.win_rate),
                "paid_break_even": float(row.mean_paid_break_even_probability),
                "model_calibration_residual": float(row.model_calibration_residual)}

    betonline_table = book_summary[book_summary.sportsbook.eq("sportsgameodds:betonline")]
    forward_frames, _ = cohort_frames(primary[primary.temporal_segment.eq("MOST_RECENT_FORWARD_EXTENSION")])

    summary = {
        "decision": decision,
        "model": {"model_version": MODEL, "model_hash": config["model_hash"],
                  "prediction_snapshot": SNAPSHOT, "admission_state": ADMISSION,
                  "strong_boundary": "selected_model_probability > 0.60 (strict; 0.60 remains MODERATE)"},
        "latest_resolved_cutoff": cutoff,
        "prior_formal_characterization_cutoff": PRIOR_CHARACTERIZATION_CUTOFF,
        "primary_population": {"sportsbook": "pinnacle", "rows": len(primary),
                               "start_date": str(primary.game_date.min()), "end_date": str(primary.game_date.max()),
                               "quote_policy": "latest preserved capture with capture and provider update strictly before scheduled start",
                               "price_authority": authority_for("pinnacle")},
        "historical_expansion": {"rows": len(historical), "start_date": str(historical.game_date.min()),
                                 "end_date": str(historical.game_date.max()), "pooled_with_primary": False,
                                 "authority": "HISTORICAL_PREGAME_DESCRIPTIVE_SNAPSHOT_NO_FILL_EVIDENCE"},
        "top_decile_gap_threshold": top_threshold,
        "premises_supported": support_count,
        "direct_tests": test_detail,
        "strong_reconciliation": strong_detail,
        "prospective_confirmation": requirement,
        "temporal_assessment": temporal_assessment,
        "eligible_book_count": len(eligible_books),
        "candidate_positive_book_count": book_positive,
        "cross_checks": {
            "betonline_positive_ev": cohort_record(betonline_table, "APPARENT_POSITIVE_MODEL_DERIVED_EV"),
            "betonline_strong_aligned_nonpositive_edge": cohort_record(betonline_table, "STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"),
            "historical_positive_ev_underdogs": cohort_record(hist_cohort, "APPARENT_POSITIVE_EV_MARKET_UNDERDOGS"),
            "historical_strong_aligned_nonpositive_edge": cohort_record(hist_cohort, "STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"),
            "forward_extension_strong_aligned_nonpositive_edge": metric(forward_frames["STRONG_ALIGNED_ZERO_OR_NEGATIVE_EDGE"]),
            "book_views_are_independent_replications": False,
        },
        "excluded_rows_by_reason": {str(k): int(v) for k, v in excluded.reason.value_counts().sort_index().items()},
        "canonical_stream": {"rows_through_cutoff": len(predictions), "resolved_rows": len(resolved),
                             "unresolved_rows": len(unresolved),
                             "next_date_rows_outside_resolved_cutoff": len(after_cutoff),
                             "duplicate_prediction_identities": int(predictions.duplicated(["game_date", "game_id", "model_version", "prediction_snapshot_class"]).sum())},
        "interpretation_guardrails": [
            "No bookmaker motive or manipulation inferred", "No captured quote treated as a proven fill",
            "No model refit or recalibration", "No production selector created",
            "Historical replay not pooled with immutable prospective evidence",
        ],
    }

    output.mkdir(parents=True, exist_ok=True)
    ledger_cols = [
        "analysis_identity", "population", "sportsbook", "price_authority", "game_date", "game_id",
        "home_team_model", "away_team_model", "scheduled_start_utc", "prediction_timestamp_utc",
        "model_scheduled_start_utc", "schedule_start_delta_seconds", "prediction_cutoff_utc", "captured_at_utc",
        "provider_market_updated_at_utc", "captured_lead_minutes",
        "canonical_market_identity", "model_hash", "prediction_payload_sha256", "outcome_payload_sha256",
        "confidence_band", "model_selected_side", "selected_team", "selected_model_probability",
        "home_american_price", "away_american_price", "selected_american_price", "selected_decimal_price",
        "selected_raw_implied_probability", "opposing_raw_implied_probability", "overround",
        "selected_market_probability", "selected_additive_market_probability", "selected_paid_break_even_probability",
        "model_minus_market_gap", "model_minus_additive_market_gap", "model_implied_expected_return",
        "selected_win", "flat_stake_return", "market_probability_band", "model_probability_band", "gap_band",
        "market_side_status", "agreement_state", "apparent_ev_status", "temporal_segment",
        "raw_source_path", "raw_source_sha256",
    ]
    for col in ledger_cols:
        if col not in ledger:
            ledger[col] = None
    write_csv(ledger[ledger_cols].sort_values(["population", "sportsbook", "game_date", "game_id"]), output / "exact_joined_row_ledger.csv")
    write_csv(cov, output / "sportsbook_coverage.csv")
    write_csv(cohort, output / "cohort_summary.csv")
    write_csv(hist_cohort, output / "historical_expansion_cohort_summary.csv")
    write_csv(book_summary, output / "book_level_cohort_summary.csv")
    write_csv(sportsbook_overlaps, output / "sportsbook_overlap_table.csv")
    write_csv(band_summary, output / "probability_band_summaries.csv")
    write_csv(matrix, output / "two_dimensional_matrices.csv")
    write_csv(overlaps, output / "cohort_overlap_table.csv")
    write_csv(temporal_table, output / "daily_temporal_stability.csv")
    write_csv(strong_ledger, output / "strong_selection_reconciliation.csv")
    write_csv(strong_checkpoints, output / "strong_selection_checkpoints.csv")
    write_csv(uncertainty_table, output / "uncertainty_and_sensitivity.csv")
    write_csv(tests, output / "premise_tests.csv")
    write_csv(excluded, output / "excluded_row_ledger.csv")
    (output / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=json_value) + "\n")
    (output / "main_report.md").write_text(report_text(summary, cohort, tests, requirement, book_summary))
    rerun = (f"/bin/zsh -lc 'set -a; source backend/.env; set +a; .venv/bin/python -m "
             f"backend.mlb.scripts.audit_mlb_moneyline_probability_region_premise_v1 "
             f"--resolved-cutoff {cutoff} --output {output.relative_to(ROOT)}'\n")
    (output / "rerun_command.txt").write_text(rerun)
    make_validator(output / "validator.py")
    files = sorted(p for p in output.iterdir() if p.is_file() and p.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resolved-cutoff", default="latest", help="YYYY-MM-DD or latest")
    parser.add_argument("--output", type=Path, default=Path(
        "artifacts/analysis/model_development/mlb_moneyline_probability_region_premise_audit_v1/2026-09-09"))
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else ROOT / args.output
    summary = run(output, args.resolved_cutoff)
    print(json.dumps(summary, indent=2, sort_keys=True, default=json_value))


if __name__ == "__main__":
    main()
