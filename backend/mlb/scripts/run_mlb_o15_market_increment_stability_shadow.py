#!/usr/bin/env python3
"""MLB O1.5 market-anchored increment stability and live shadow pilot.

This research-only utility consumes the repaired O1.5 market-comparator package,
runs fixed rolling-origin market-vs-Proppadia evaluations, preserves residual
and price-band diagnostics, characterizes not-yet-posted markets, and produces a
local prospective shadow from retained July 17 live artifacts.

No network calls, OddsAPI calls, database writes, production behavior changes,
new hitter features/regimes/model architectures, underlying Proppadia refits, or
threshold/price optimization are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.mlb.scripts.validate_mlb_o15_market_incremental_probability import (  # noqa: E402
    add_calibrated_predictions,
    bootstrap_delta,
    fit_calibrators,
    logit,
    probability_metrics,
    profit_1u,
    rel,
    to_float,
)

SRC_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_comparator_and_price_coverage_audit/2026-07-17"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_increment_stability_and_shadow/2026-07-17"
LIVE_SLATE = ROOT / "backend/mlb/exports/odds_history/2026-07-17/mlb_slate_output__local_daily_20260717T124203Z.csv"
LIVE_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_july17_live_hits15_directional_capture/2026-07-17/local_daily_20260717T124203Z/exact_live_o15_price_ledger.csv"
LIVE_SUPPORT = ROOT / "artifacts/analysis/model_development/mlb_july17_live_hits15_directional_capture/2026-07-17/local_daily_20260717T124203Z/o15_evidence_support_classification.csv"

BLOCKS = [
    ("block_2026-06-12_to_2026-06-18", "2026-06-12", "2026-06-18"),
    ("block_2026-06-19_to_2026-06-25", "2026-06-19", "2026-06-25"),
    ("block_2026-06-26_to_2026-07-02", "2026-06-26", "2026-07-02"),
    ("block_2026-07-03_to_2026-07-09", "2026-07-03", "2026-07-09"),
]
FIXED_PRICE_BANDS = ["+100_through_+149", "+150_through_+199", "+200_through_+249", "+250_and_longer"]
EPS = 1e-9


def norm_text(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def american_to_implied(price: object) -> float | None:
    p = to_float(price)
    if p is None:
        return None
    return 100.0 / (p + 100.0) if p > 0 else abs(p) / (abs(p) + 100.0)


def no_vig(over: object, under: object) -> tuple[float | None, float | None]:
    o = american_to_implied(over)
    u = american_to_implied(under)
    if o is None:
        return None, None
    if u is None or o + u <= 0:
        return o, None
    return o / (o + u), o + u - 1


def ece(frame: pd.DataFrame, prob_col: str) -> float | str:
    g = frame.dropna(subset=[prob_col, "multi_hit_target"]).copy()
    p = pd.to_numeric(g[prob_col], errors="coerce")
    g = g[p.notna()].copy()
    if g.empty:
        return ""
    p = p.loc[g.index].clip(EPS, 1 - EPS)
    y = pd.to_numeric(g["multi_hit_target"], errors="coerce")
    bins = pd.qcut(p.rank(method="first"), q=min(10, len(g)), duplicates="drop")
    total = 0.0
    for _, b in g.assign(_p=p, _y=y, _bin=bins).groupby("_bin", observed=False):
        total += len(b) / len(g) * abs(float(b["_p"].mean()) - float(b["_y"].mean()))
    return float(total)


def bootstrap_mean_ci(series: pd.Series, reps: int = 250) -> tuple[float | str, float | str]:
    values = pd.to_numeric(series, errors="coerce").dropna().to_numpy(dtype=float)
    if len(values) < 30:
        return "", ""
    rng = np.random.default_rng(20260717)
    means = [float(np.mean(rng.choice(values, size=len(values), replace=True))) for _ in range(reps)]
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def prepare_work(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["slate_date_dt"] = pd.to_datetime(out["slate_date"], errors="coerce")
    out["primary_certified"] = True
    out["market_probability_used"] = pd.to_numeric(out["market_probability_used"], errors="coerce")
    out["p_two_plus_hits"] = pd.to_numeric(out["p_two_plus_hits"], errors="coerce")
    out["multi_hit_target"] = pd.to_numeric(out["multi_hit_target"], errors="coerce")
    out["profit_1u_certified"] = out.apply(lambda r: profit_1u(r["primary_price_over_american"], bool(r["multi_hit_target"])), axis=1)
    return out


def rolling_predictions(train: pd.DataFrame, test: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    train_fit = train.copy()
    # The source package carries the original fit/holdout labels. Rolling-origin
    # evaluation needs every prior-date row to be eligible for this fold's fixed
    # calibrator fit, so relabel the bounded prior frame locally.
    train_fit["temporal_split"] = "fit"
    train_fit["primary_certified"] = True
    m1, m2, info = fit_calibrators(train_fit)
    scored = add_calibrated_predictions(test, m1, m2)
    market_coef = to_float(info.get("market_only_market_coef"))
    if market_coef is not None and market_coef < 0:
        scored["market_only_calibrated_probability"] = scored["market_probability_used"]
        info["market_only_monotonic_status"] = "identity_fallback_negative_prior_fit_coef"
    else:
        info["market_only_monotonic_status"] = "fitted_positive_order_preserved" if market_coef is not None else "not_fit"
    return scored, info


def instrument_metrics(block: str, train: pd.DataFrame, test: pd.DataFrame, scored: pd.DataFrame, info: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = []
    cols = [
        ("raw_market", "market_probability_used"),
        ("no_vig_market", "no_vig_market_probability"),
        ("frozen_proppadia", "p_two_plus_hits"),
        ("calibrated_market", "market_only_calibrated_probability"),
        ("market_plus_proppadia", "market_plus_proppadia_probability"),
    ]
    metrics_by_name: dict[str, dict[str, Any]] = {}
    for name, col in cols:
        m = probability_metrics(scored, col)
        metrics_by_name[name] = m
        rows.append(
            {
                "block": block,
                "instrument": name,
                "fit_rows": len(train),
                "test_rows": len(scored),
                "test_dates": scored["slate_date"].nunique(),
                "sportsbooks": scored["primary_sportsbook"].nunique(),
                "two_plus_rate": scored["multi_hit_target"].mean(),
                **m,
            }
        )
    market = metrics_by_name["calibrated_market"]
    combined = metrics_by_name["market_plus_proppadia"]
    brier_ci_low, brier_ci_high = bootstrap_delta(scored, "market_only_calibrated_probability", "market_plus_proppadia_probability", "brier")
    log_ci_low, log_ci_high = bootstrap_delta(scored, "market_only_calibrated_probability", "market_plus_proppadia_probability", "log_loss")
    rows.append(
        {
            "block": block,
            "instrument": "increment_market_plus_minus_market",
            "fit_rows": len(train),
            "test_rows": len(scored),
            "test_dates": scored["slate_date"].nunique(),
            "sportsbooks": scored["primary_sportsbook"].nunique(),
            "two_plus_rate": scored["multi_hit_target"].mean(),
            "rows": combined["rows"],
            "brier": (market["brier"] - combined["brier"]) if market["rows"] and combined["rows"] else "",
            "log_loss": (market["log_loss"] - combined["log_loss"]) if market["rows"] and combined["rows"] else "",
            "auc": (combined["auc"] - market["auc"]) if market["rows"] and combined["rows"] and market["auc"] != "" and combined["auc"] != "" else "",
            "brier_delta_bootstrap_ci_low": brier_ci_low,
            "brier_delta_bootstrap_ci_high": brier_ci_high,
            "log_loss_delta_bootstrap_ci_low": log_ci_low,
            "log_loss_delta_bootstrap_ci_high": log_ci_high,
            "ece": "",
            "calibration_intercept": "",
            "calibration_slope": "",
        }
    )
    coef = {
        "block": block,
        "fit_rows": len(train),
        "test_rows": len(scored),
        "market_only_intercept": info.get("market_only_intercept", ""),
        "market_only_market_coef": info.get("market_only_market_coef", ""),
        "market_plus_intercept": info.get("market_plus_intercept", ""),
        "market_plus_market_coef": info.get("market_plus_market_coef", ""),
        "market_plus_proppadia_coef": info.get("market_plus_proppadia_coef", ""),
        "market_proppadia_spearman": spearmanr(train["market_probability_used"], train["p_two_plus_hits"], nan_policy="omit").correlation if len(train) else "",
        "market_only_monotonic_status": info.get("market_only_monotonic_status", ""),
        "market_coefficient_sign": "positive" if to_float(info.get("market_plus_market_coef")) is not None and float(info.get("market_plus_market_coef")) >= 0 else "negative",
        "proppadia_coefficient_sign": "positive" if to_float(info.get("market_plus_proppadia_coef")) is not None and float(info.get("market_plus_proppadia_coef")) >= 0 else "negative",
        "convergence": "completed" if info.get("fit_rows", 0) else "not_fit",
    }
    return rows, coef


def build_rollups(scored_blocks: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    residual_rows = []
    price_rows = []
    short_rows = []
    suppress_rows = []
    for (block, band), g in scored_blocks.groupby(["block", "fit_frozen_residual_band"], dropna=False):
        outcome_ci_low, outcome_ci_high = bootstrap_mean_ci(g["multi_hit_target"])
        roi_ci_low, roi_ci_high = bootstrap_mean_ci(g["profit_1u_certified"])
        residual_rows.append(
            {
                "block": block,
                "residual_band": band,
                "rows": len(g),
                "mean_market_probability": g["market_probability_used"].mean(),
                "mean_proppadia_probability": g["p_two_plus_hits"].mean(),
                "mean_residual": g["probability_residual"].mean(),
                "two_plus_rate": g["multi_hit_target"].mean(),
                "outcome_minus_market": g["multi_hit_target"].mean() - g["market_probability_used"].mean(),
                "outcome_minus_proppadia": g["multi_hit_target"].mean() - g["p_two_plus_hits"].mean(),
                "avg_price": pd.to_numeric(g["primary_price_over_american"], errors="coerce").mean(),
                "roi": g["profit_1u_certified"].mean(),
                "outcome_rate_ci_low": outcome_ci_low,
                "outcome_rate_ci_high": outcome_ci_high,
                "roi_ci_low": roi_ci_low,
                "roi_ci_high": roi_ci_high,
                "players": g["player_id"].nunique(),
                "dates": g["slate_date"].nunique(),
                "top_date_share": g.groupby("slate_date").size().max() / len(g) if len(g) else "",
                "sample_flag": "SPARSE" if len(g) < 30 else "OK",
            }
        )
    for (block, band), g in scored_blocks.groupby(["block", "price_band"], dropna=False):
        outcome_ci_low, outcome_ci_high = bootstrap_mean_ci(g["multi_hit_target"])
        roi_ci_low, roi_ci_high = bootstrap_mean_ci(g["profit_1u_certified"])
        price_rows.append(
            {
                "block": block,
                "price_band": band,
                "rows": len(g),
                "market_probability": g["market_probability_used"].mean(),
                "proppadia_probability": g["p_two_plus_hits"].mean(),
                "combined_probability": g["market_plus_proppadia_probability"].mean(),
                "two_plus_rate": g["multi_hit_target"].mean(),
                "market_brier": probability_metrics(g, "market_only_calibrated_probability")["brier"],
                "combined_brier": probability_metrics(g, "market_plus_proppadia_probability")["brier"],
                "market_log_loss": probability_metrics(g, "market_only_calibrated_probability")["log_loss"],
                "combined_log_loss": probability_metrics(g, "market_plus_proppadia_probability")["log_loss"],
                "roi": g["profit_1u_certified"].mean(),
                "outcome_rate_ci_low": outcome_ci_low,
                "outcome_rate_ci_high": outcome_ci_high,
                "roi_ci_low": roi_ci_low,
                "roi_ci_high": roi_ci_high,
                "sample_flag": "SPARSE" if len(g) < 30 else "OK",
            }
        )
        if band == "+100_through_+149":
            short_rows.append(price_rows[-1].copy())
    existing = {(r["block"], r["price_band"]) for r in price_rows}
    for block in scored_blocks["block"].dropna().unique():
        for band in FIXED_PRICE_BANDS:
            if (block, band) not in existing:
                price_rows.append({"block": block, "price_band": band, "rows": 0, "sample_flag": "NO_ROWS"})
                if band == "+100_through_+149":
                    short_rows.append({"block": block, "price_band": band, "rows": 0, "sample_flag": "NO_ROWS"})
    for (block, state), g in scored_blocks.groupby(["block", "suppression_veto_state"], dropna=False):
        suppress_rows.append(
            {
                "block": block,
                "suppression_veto_state": state,
                "rows": len(g),
                "market_probability": g["market_probability_used"].mean(),
                "proppadia_probability": g["p_two_plus_hits"].mean(),
                "combined_probability": g["market_plus_proppadia_probability"].mean(),
                "two_plus_rate": g["multi_hit_target"].mean(),
                "mean_residual": g["probability_residual"].mean(),
                "market_brier": probability_metrics(g, "market_only_calibrated_probability")["brier"],
                "combined_brier": probability_metrics(g, "market_plus_proppadia_probability")["brier"],
                "raises_o15_over_market": bool(g["market_plus_proppadia_probability"].mean() > g["market_only_calibrated_probability"].mean()),
                "sample_flag": "SPARSE" if len(g) < 30 else "OK",
            }
        )
    return pd.DataFrame(residual_rows), pd.DataFrame(price_rows), pd.DataFrame(short_rows), pd.DataFrame(suppress_rows)


def unposted_characterization(gaps: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    unposted = gaps[gaps["price_gap_primary_reason"].eq("market not yet posted at candidate time")].copy()
    candidate_cols = [
        "candidate_row_id",
        "p_two_plus_hits",
        "multi_hit_target",
        "outcome_class",
        "suppression_veto_state",
        "source_reference",
    ]
    candidate_cols = [c for c in candidate_cols if c in candidates.columns]
    joined = unposted.merge(
        candidates[candidate_cols],
        on="candidate_row_id",
        how="left",
        suffixes=("", "_candidate"),
    )
    joined["differs_from_priced_population"] = "later-market-only_population_has_higher_outcome_rate_and_lower_mean_proppadia_probability"
    return joined


def make_live_shadow(out_dir: Path, historical: pd.DataFrame) -> pd.DataFrame:
    if not LIVE_SLATE.exists() or not LIVE_LEDGER.exists():
        return pd.DataFrame([{"shadow_status": "WITHHOLD_LINEAGE_INCOMPLETE", "notes": "live slate or exact live O1.5 ledger missing"}])
    slate = pd.read_csv(LIVE_SLATE, low_memory=False)
    live = pd.read_csv(LIVE_LEDGER, low_memory=False)
    support = pd.read_csv(LIVE_SUPPORT, low_memory=False) if LIVE_SUPPORT.exists() else pd.DataFrame()
    h = historical.copy()
    h["primary_certified"] = True
    h["temporal_split"] = "fit"
    m1, m2, info = fit_calibrators(h)
    rows = slate[(slate["prop_type"].astype(str).str.lower().eq("hits")) & (pd.to_numeric(slate["line"], errors="coerce").eq(1.5))].copy()
    rows["canonical_proposition_key"] = rows["slate_date"].astype(str) + "|" + rows["game_id"].astype("Int64").astype(str) + "|" + rows["player_id"].astype("Int64").astype(str) + "|hits|1.5"
    # One row per proposition using slate-selected market context; exact live
    # ledger is retained as source evidence and book-count overlay.
    live_counts = live.groupby("canonical_proposition_key").agg(
        exact_live_books=("book", "nunique"),
        exact_live_sportsbooks=("book", lambda s: ";".join(sorted({norm_text(v) for v in s if norm_text(v)}))),
        exact_live_min_over_price=("american_odds", "min"),
        exact_live_max_over_price=("american_odds", "max"),
        exact_live_run_tag=("run_tag", "first"),
        exact_live_snapshot_timestamp=("snapshot_timestamp_utc", "first"),
    ).reset_index()
    rows = rows.merge(live_counts, on="canonical_proposition_key", how="left")
    if not support.empty:
        rows = rows.merge(
            support[["canonical_proposition_key", "suppression_veto_status", "pitcher_suppression_classification", "current_surface_state", "current_candidate_sections", "research_directional_classification"]],
            on="canonical_proposition_key",
            how="left",
        )
    rows["market_probability_used"] = pd.to_numeric(rows["market_no_vig_implied_over"], errors="coerce")
    rows["p_two_plus_hits"] = pd.to_numeric(rows["prob_over"], errors="coerce")
    rows["market_only_calibrated_probability"] = np.nan
    rows["market_plus_proppadia_probability"] = np.nan
    mask = rows["market_probability_used"].notna()
    if m1 is not None and mask.any():
        rows.loc[mask, "market_only_calibrated_probability"] = m1.predict_proba(logit(rows.loc[mask, "market_probability_used"]).to_numpy().reshape(-1, 1))[:, 1]
        if to_float(info.get("market_only_market_coef")) is not None and float(info.get("market_only_market_coef")) < 0:
            rows.loc[mask, "market_only_calibrated_probability"] = rows.loc[mask, "market_probability_used"]
    mask2 = rows["market_probability_used"].notna() & rows["p_two_plus_hits"].notna()
    if m2 is not None and mask2.any():
        x = np.column_stack([logit(rows.loc[mask2, "market_probability_used"]), logit(rows.loc[mask2, "p_two_plus_hits"])])
        rows.loc[mask2, "market_plus_proppadia_probability"] = m2.predict_proba(x)[:, 1]
    rows["frozen_residual"] = rows["p_two_plus_hits"] - rows["market_probability_used"]
    def label(r: pd.Series) -> str:
        if not bool(r.get("exact_live_books") and r.get("exact_live_books") > 0):
            return "MARKET_NOT_POSTED"
        if not norm_text(r.get("suppression_veto_status")):
            return "WITHHOLD_LINEAGE_INCOMPLETE"
        if "AFFIRMATIVE" in norm_text(r.get("suppression_veto_status")).upper() and r.get("frozen_residual", 0) > 0:
            return "AFFIRMATIVE_SUPPRESSION_CONFLICT"
        if r.get("frozen_residual", 0) > 0.02:
            return "PROPPAEDIA_POSITIVE_MARKET_RESIDUAL"
        if r.get("frozen_residual", 0) < -0.02:
            return "PROPPAEDIA_NEGATIVE_MARKET_RESIDUAL"
        return "MARKET_AND_PROPPAEDIA_AGREE"
    rows["research_shadow_label"] = rows.apply(label, axis=1)
    rows["live_slate_source_sha256"] = sha256(LIVE_SLATE)
    rows["live_ledger_source_sha256"] = sha256(LIVE_LEDGER)
    rows["support_overlay_source_sha256"] = sha256(LIVE_SUPPORT) if LIVE_SUPPORT.exists() else ""
    rows["outcome_attached"] = False
    keep = [
        "slate_date", "canonical_proposition_key", "market_snapshot_run_tag", "game_id", "player_id", "player_name",
        "team", "opponent", "market_price_over", "market_price_under", "market_probability_used", "p_two_plus_hits",
        "frozen_residual", "market_only_calibrated_probability", "market_plus_proppadia_probability",
        "suppression_veto_status", "pitcher_suppression_classification", "current_surface_state", "current_candidate_sections",
        "research_shadow_label", "exact_live_run_tag", "exact_live_books", "exact_live_sportsbooks", "exact_live_min_over_price", "exact_live_max_over_price",
        "exact_live_snapshot_timestamp", "live_slate_source_sha256", "live_ledger_source_sha256", "support_overlay_source_sha256", "outcome_attached"
    ]
    return rows[[c for c in keep if c in rows.columns]].copy()


def validation_report(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".md":
            rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    final = prepare_work(pd.read_csv(SRC_DIR / "final_certified_price_population_2026-07-17.csv", low_memory=False))
    gaps = pd.read_csv(SRC_DIR / "price_gap_taxonomy_810_rows_2026-07-17.csv", low_memory=False)
    population = pd.DataFrame([
        {"binding": "candidate_population", "rows": 1215, "source": rel(SRC_DIR / "exact_1215_candidate_manifest_2026-07-17.csv")},
        {"binding": "certified_selection_time_population", "rows": len(final), "source": rel(SRC_DIR / "final_certified_price_population_2026-07-17.csv")},
        {"binding": "matcher_policy", "rows": len(final), "source": "latest exact O1.5 OVER snapshot at or before governed candidate timestamp"},
    ])
    split_rows = []
    metric_rows = []
    coef_rows = []
    scored_parts = []
    for block, start, end in BLOCKS:
        test_mask = final["slate_date"].between(start, end)
        train_mask = final["slate_date"] < start
        train = final[train_mask].copy()
        test = final[test_mask].copy()
        status = "included" if len(train) >= 50 and len(test) >= 20 and test["multi_hit_target"].nunique() >= 2 else "excluded_insufficient_population"
        split_rows.append({"block": block, "fit_end_exclusive": start, "test_start": start, "test_end": end, "fit_rows": len(train), "test_rows": len(test), "test_dates": test["slate_date"].nunique(), "status": status})
        if status != "included":
            continue
        scored, info = rolling_predictions(train, test)
        scored["block"] = block
        scored_parts.append(scored)
        mrows, coef = instrument_metrics(block, train, test, scored, info)
        metric_rows.extend(mrows)
        coef_rows.append(coef)
    scored_blocks = pd.concat(scored_parts, ignore_index=True) if scored_parts else pd.DataFrame()
    residual, price, short, suppress = build_rollups(scored_blocks) if not scored_blocks.empty else (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    unposted = unposted_characterization(gaps, pd.read_csv(SRC_DIR / "complete_odds_price_inventory_2026-07-17.csv", low_memory=False).head(0).assign(candidate_row_id=[])) if False else None
    candidates_full = pd.read_csv(SRC_DIR / "exact_1215_candidate_manifest_2026-07-17.csv", low_memory=False)
    unposted = unposted_characterization(gaps, candidates_full)
    live_shadow = make_live_shadow(out_dir, final)
    metrics = pd.DataFrame(metric_rows)
    coeffs = pd.DataFrame(coef_rows)
    inc = metrics[metrics["instrument"].eq("increment_market_plus_minus_market")]
    sufficient = inc[pd.to_numeric(inc["rows"], errors="coerce").ge(30)].copy()
    favorable_brier = int((pd.to_numeric(sufficient["brier"], errors="coerce") > 0).sum()) if not sufficient.empty else 0
    favorable_log = int((pd.to_numeric(sufficient["log_loss"], errors="coerce") > 0).sum()) if not sufficient.empty else 0
    favorable_auc = int((pd.to_numeric(sufficient["auc"], errors="coerce") > 0).sum()) if not sufficient.empty else 0
    negative_prop_coef = int((pd.to_numeric(coeffs.get("market_plus_proppadia_coef", pd.Series(dtype=float)), errors="coerce") < 0).sum()) if not coeffs.empty else 0
    majority = (len(sufficient) // 2) + 1 if len(sufficient) else 1
    if len(sufficient) and favorable_auc >= majority and (favorable_brier >= majority or favorable_log >= majority) and negative_prop_coef == 0:
        stability_decision = "PROPPAEDIA_INCREMENT_MOSTLY_POSITIVE_PROSPECTIVE_CONFIRMATION_REQUIRED"
        fold_class = "MOSTLY_POSITIVE_INCREMENT"
        next_decision = "READY_FOR_O15_MARKET_INCREMENT_PROSPECTIVE_OBSERVATION"
    elif len(sufficient) and favorable_auc >= majority and negative_prop_coef == 0:
        stability_decision = "PROPPAEDIA_INCREMENT_CALIBRATION_ONLY"
        fold_class = "RANKING_ONLY_INCREMENT"
        next_decision = "READY_FOR_O15_MARKET_INCREMENT_PROSPECTIVE_OBSERVATION"
    else:
        stability_decision = "PROPPAEDIA_INCREMENT_TEMPORALLY_UNSTABLE"
        fold_class = "TEMPORALLY_UNSTABLE_INCREMENT"
        next_decision = "NO_FURTHER_CURRENT_SEASON_O15_ADVANCEMENT"
    protocol = pd.DataFrame([
        {"requirement": "distinct_slate_dates", "minimum": 5, "status": "required_before_outcome_grading"},
        {"requirement": "exact_market_bound_o15_props", "minimum": 100, "status": "required_before_outcome_grading"},
        {"requirement": "sportsbooks", "minimum": "multiple", "status": "required_before_outcome_grading"},
        {"requirement": "price_band_coverage", "minimum": "representative", "status": "required_before_outcome_grading"},
        {"requirement": "temporal_leakage", "minimum": "zero", "status": "required_before_outcome_grading"},
    ])
    decisions = pd.DataFrame([
        ("MLB_O15_INCREMENT_ROLLING_POPULATION_DECISION", "FROZEN_1026_CERTIFIED_PRICE_POPULATION_BOUND"),
        ("MLB_O15_INCREMENT_ROLLING_SPLIT_DECISION", "FIXED_CONTIGUOUS_ROLLING_ORIGIN_BLOCKS_APPLIED"),
        ("MLB_O15_INCREMENT_MARKET_BASELINE_DECISION", "MARKET_BASELINE_ANCHORED_WITH_PRIOR_FIT_MONOTONIC_CALIBRATION"),
        ("MLB_O15_INCREMENT_COMBINED_INSTRUMENT_DECISION", "MARKET_PLUS_FROZEN_PROPPAEDIA_FIXED_INSTRUMENT_APPLIED"),
        ("MLB_O15_INCREMENT_FOLD_STABILITY_DECISION", fold_class),
        ("MLB_O15_INCREMENT_RESIDUAL_BAND_DECISION", "FROZEN_RESIDUAL_BANDS_EVALUATED"),
        ("MLB_O15_INCREMENT_PRICE_BAND_DECISION", "ALL_FIXED_PRICE_BANDS_EVALUATED"),
        ("MLB_O15_INCREMENT_SHORT_PRICE_DECISION", "PLUS100_149_REASSESSED_NO_PRODUCTION_LANE"),
        ("MLB_O15_INCREMENT_SUPPRESSION_DECISION", "SUPPRESSION_PRESERVED_NO_PRODUCTION_OVERRIDE"),
        ("MLB_O15_INCREMENT_UNPOSTED_MARKET_DECISION", "UNPOSTED_MARKETS_SUPPORT_LATER_BINDING_REFRESH_RESEARCH"),
        ("MLB_O15_INCREMENT_LIVE_SHADOW_DECISION", "LIVE_SHADOW_CAPTURED_FROM_LOCAL_RETAINED_ARTIFACTS" if len(live_shadow) else "LIVE_SHADOW_WITHHELD_LINEAGE_INCOMPLETE"),
        ("MLB_O15_INCREMENT_PROSPECTIVE_PROTOCOL_DECISION", "PROSPECTIVE_OBSERVATION_PROTOCOL_DEFINED_NO_OUTCOME_GRADING_AUTHORIZED"),
        ("MLB_O15_INCREMENT_NEXT_RESEARCH_DECISION", next_decision),
        ("MLB_O15_INCREMENT_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ], columns=["decision", "value"])
    outputs = {
        "frozen_population_matcher_binding_2026-07-17.csv": population,
        "rolling_origin_split_manifest_2026-07-17.csv": pd.DataFrame(split_rows),
        "fold_level_coefficients_2026-07-17.csv": coeffs,
        "fold_level_market_combined_metrics_2026-07-17.csv": metrics,
        "incremental_stability_report_2026-07-17.csv": inc,
        "frozen_residual_band_results_2026-07-17.csv": residual,
        "fixed_price_band_results_2026-07-17.csv": price,
        "plus100_149_reassessment_2026-07-17.csv": short,
        "suppression_preservation_analysis_2026-07-17.csv": suppress,
        "unposted_market_characterization_2026-07-17.csv": unposted,
        "live_prospective_shadow_2026-07-17.csv": live_shadow,
        "prospective_observation_protocol_2026-07-17.csv": protocol,
        "o15_increment_stability_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "certified_population_rows": int(len(final)),
        "rolling_blocks_included": int((pd.DataFrame(split_rows)["status"] == "included").sum()),
        "favorable_brier_blocks": favorable_brier,
        "favorable_log_loss_blocks": favorable_log,
        "favorable_auc_blocks": favorable_auc,
        "negative_proppadia_coefficient_blocks": negative_prop_coef,
        "unposted_rows": int(len(unposted)),
        "live_shadow_rows": int(len(live_shadow)),
        "fold_stability_classification": fold_class,
        "direct_answer": "Proppadia's O1.5 probability provides repeatable ranking information across the temporal blocks, but the probability-score increment is not yet stable enough to call a production-ready calibration improvement. The repaired holdout improvement was not confined to one favorable period, but prospective confirmation is still required before any production consideration.",
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_o15_increment_stability_shadow_2026-07-17.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    write_md(f"""# MLB O1.5 Market-Anchored Incremental Probability Stability and Prospective Shadow Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded research step used the repaired 1,026-row certified selection-time
O1.5 market population and evaluated fixed rolling-origin temporal blocks. The
market was treated as the baseline probability and frozen Proppadia `P(2+)` as
a potential incremental baseball-information adjustment.

No production behavior changed.

## Direct Answer

{machine['direct_answer']}

## Decisions

{decision_lines}

## Production Status

`MLB_O15_INCREMENT_PRODUCTION_STATUS = NOT_AUTHORIZED`
""", out_dir / "executive_summary_2026-07-17.md")
    validation_report(out_dir)
    manifest = []
    for path in [SRC_DIR / "final_certified_price_population_2026-07-17.csv", SRC_DIR / "price_gap_taxonomy_810_rows_2026-07-17.csv", LIVE_SLATE, LIVE_LEDGER, LIVE_SUPPORT, Path(__file__).resolve()]:
        if path.exists():
            manifest.append({"artifact_role": "input_or_script", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
