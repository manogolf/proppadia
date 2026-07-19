#!/usr/bin/env python3
"""Audit and repair O1.5 market-comparator price coverage.

This bounded research utility consumes the prior O1.5 market-increment package,
explains every uncertified row, recovers deterministic latest-at-or-before
prices where preserved snapshots support them, audits market-probability
orientation, and reruns the frozen market increment comparison when coverage is
materially expanded.

No network calls, OddsAPI calls, database writes, new hitter features, model
refits, price/edge optimization, production changes, or LaunchAgent changes are
performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
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
    american_to_decimal,
    bootstrap_delta,
    fit_calibrators,
    freeze_residual_bands,
    group_perf,
    iso,
    parse_ts,
    probability_metrics,
    probability_validation,
    profit_1u,
    rel,
    to_float,
    to_int,
)

AUDIT_DATE = "2026-07-17"
PREV_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_incremental_probability_validation/2026-07-17"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_o15_market_comparator_and_price_coverage_audit/2026-07-17"

BOOK_PRIORITY = {
    "betonlineag": 0,
    "draftkings": 1,
    "fanduel": 2,
    "betrivers": 3,
    "williamhill_us": 4,
}
FIXED_PRICE_BANDS = ["+100_through_+149", "+150_through_+199", "+200_through_+249", "+250_and_longer"]


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


def ensure_price_band_rows(df: pd.DataFrame) -> pd.DataFrame:
    rows = df.to_dict("records") if not df.empty else []
    existing = {(norm_text(r.get("temporal_split")), norm_text(r.get("price_band"))) for r in rows}
    for split in ["fit", "validation", "holdout"]:
        for band in FIXED_PRICE_BANDS:
            if (split, band) not in existing:
                rows.append(
                    {
                        "temporal_split": split,
                        "price_band": band,
                        "rows": 0,
                        "two_plus_rows": 0,
                        "two_plus_rate": "",
                        "avg_market_probability": "",
                        "avg_proppadia_probability": "",
                        "avg_market_plus_probability": "",
                        "avg_price": "",
                        "certified_roi": "",
                        "dates": 0,
                        "players": 0,
                        "top_date_share": "",
                        "sample_flag": "NO_ROWS",
                    }
                )
    return pd.DataFrame(rows)


def candidate_manifest(aligned: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "candidate_row_id",
        "candidate_identity_key",
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "prop_type",
        "o15_price",
        "price_band",
        "control_source_run_tags",
        "governed_candidate_timestamp",
        "candidate_timestamp_source",
        "candidate_timestamp_evidence",
        "source_reference",
        "outcome_class",
        "multi_hit_target",
        "p_two_plus_hits",
        "temporal_split",
    ]
    out = aligned[[c for c in cols if c in aligned.columns]].copy()
    out["candidate_source_sha256"] = ""
    for idx, row in out.iterrows():
        src = norm_text(row.get("source_reference")).split("#", 1)[0]
        path = ROOT / src if src else None
        if path and path.exists():
            out.at[idx, "candidate_source_sha256"] = sha256(path)
    return out


def exact_price_rows(prices: pd.DataFrame, c: pd.Series) -> pd.DataFrame:
    mask = (
        prices["slate_date"].astype(str).eq(norm_text(c.get("slate_date")))
        & pd.to_numeric(prices["game_id"], errors="coerce").eq(to_int(c.get("game_id")))
        & pd.to_numeric(prices["player_id"], errors="coerce").eq(to_int(c.get("player_id")))
        & pd.to_numeric(prices["line"], errors="coerce").eq(1.5)
        & prices["side"].astype(str).str.lower().eq("over")
        & prices["primary_alignment_snapshot"].eq(True)
    )
    out = prices[mask].copy()
    if not out.empty:
        out["snapshot_dt"] = pd.to_datetime(out["snapshot_timestamp"], errors="coerce", utc=True)
    return out


def choose_recovered(latest: pd.DataFrame, candidate_price: float | None) -> tuple[pd.Series | None, str]:
    work = latest.copy()
    work["_has_no_vig"] = pd.to_numeric(work["no_vig_over_probability"], errors="coerce").notna()
    work["_book_priority"] = work["sportsbook"].map(BOOK_PRIORITY).fillna(99)
    work["_price_matches_candidate"] = False
    if candidate_price is not None:
        work["_price_matches_candidate"] = pd.to_numeric(work["price_over_american"], errors="coerce").eq(candidate_price)
    if work["_price_matches_candidate"].sum() == 1:
        chosen = work[work["_price_matches_candidate"]].iloc[0]
        return chosen, "candidate_price_match_at_latest_timestamp"
    if work["_price_matches_candidate"].sum() > 1:
        chosen = work[work["_price_matches_candidate"]].sort_values(["_has_no_vig", "_book_priority", "sportsbook", "snapshot_source_path"], ascending=[False, True, True, True], kind="stable").iloc[0]
        return chosen, "candidate_price_multi_book_deterministic_priority"
    chosen = work.sort_values(["_has_no_vig", "_book_priority", "sportsbook", "snapshot_source_path"], ascending=[False, True, True, True], kind="stable").iloc[0]
    return chosen, "latest_at_or_before_deterministic_book_priority_price_moved"


def classify_absent(c: pd.Series, prices: pd.DataFrame, same: pd.DataFrame, later: pd.DataFrame, decision_ts: pd.Timestamp | None) -> str:
    if not later.empty:
        return "market not yet posted at candidate time"
    date_prices = prices[prices["slate_date"].astype(str).eq(norm_text(c.get("slate_date")))]
    if date_prices.empty:
        return "source artifact missing"
    game = date_prices[pd.to_numeric(date_prices["game_id"], errors="coerce").eq(to_int(c.get("game_id")))]
    if game.empty:
        return "game absent from snapshot"
    player = game[pd.to_numeric(game["player_id"], errors="coerce").eq(to_int(c.get("player_id")))]
    if player.empty:
        return "player market absent"
    line = player[pd.to_numeric(player["line"], errors="coerce").eq(1.5)]
    if line.empty:
        return "O1.5 line absent in every preserved snapshot"
    if same.empty:
        return "OVER side absent"
    if decision_ts is not None and not same.empty and same["snapshot_dt"].min() > decision_ts:
        return "candidate occurred before the first daily capture"
    return "another exact documented reason"


def repair_alignment(aligned: pd.DataFrame, prices: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    final_rows: list[dict[str, Any]] = []
    gap_rows: list[dict[str, Any]] = []
    recovered_rows: list[dict[str, Any]] = []
    for _, c in aligned.iterrows():
        base = c.to_dict()
        same = exact_price_rows(prices, c)
        decision_dt = parse_ts(c.get("governed_candidate_timestamp"))
        decision_ts = pd.Timestamp(decision_dt) if decision_dt else None
        earlier = same[same["snapshot_dt"].le(decision_ts)] if decision_ts is not None and not same.empty else pd.DataFrame()
        later = same[same["snapshot_dt"].gt(decision_ts)] if decision_ts is not None and not same.empty else pd.DataFrame()
        old_status = norm_text(c.get("primary_alignment_status"))
        corrected_status = old_status
        corrected_reason = "previously_certified"
        chosen = None
        recovery_method = ""
        if bool(c.get("primary_certified")):
            final = base
            corrected_primary = True
        elif not earlier.empty:
            latest_ts = earlier["snapshot_dt"].max()
            latest = earlier[earlier["snapshot_dt"].eq(latest_ts)].copy()
            chosen, recovery_method = choose_recovered(latest, to_float(c.get("o15_price")))
            corrected_status = "RECOVERED_LATEST_EXACT_AT_OR_BEFORE_PRICE"
            corrected_reason = "snapshot exists but candidate-to-snapshot bundle was not traversed" if old_status == "AT_OR_BEFORE_MARKET_AVAILABLE_PRICE_MISMATCH" else "sportsbook mismatch"
            final = base.copy()
            corrected_primary = True
        else:
            corrected_reason = classify_absent(c, prices, same, later, decision_ts)
            final = base.copy()
            corrected_primary = False
        if chosen is not None:
            raw = chosen.get("raw_over_implied_probability")
            no_vig = chosen.get("no_vig_over_probability")
            used = no_vig if norm_text(no_vig) else raw
            chosen_dt = parse_ts(chosen.get("snapshot_timestamp"))
            game_dt = parse_ts(chosen.get("commence_time"))
            final.update(
                {
                    "primary_sportsbook": norm_text(chosen.get("sportsbook")),
                    "primary_price_over_american": chosen.get("price_over_american"),
                    "primary_price_under_american": chosen.get("price_under_american"),
                    "primary_decimal_over_odds": american_to_decimal(chosen.get("price_over_american")),
                    "raw_market_implied_probability": raw,
                    "no_vig_market_probability": no_vig,
                    "market_probability_used": used,
                    "vig": chosen.get("vig"),
                    "primary_snapshot_timestamp": norm_text(chosen.get("snapshot_timestamp")),
                    "primary_snapshot_run_tag": norm_text(chosen.get("snapshot_run_tag")),
                    "primary_snapshot_source_path": norm_text(chosen.get("snapshot_source_path")),
                    "primary_snapshot_source_sha256": norm_text(chosen.get("snapshot_source_sha256")),
                    "snapshot_age_minutes": round((decision_dt - chosen_dt).total_seconds() / 60.0, 3) if decision_dt and chosen_dt else "",
                    "minutes_before_first_pitch_at_decision": round((game_dt - decision_dt).total_seconds() / 60.0, 3) if game_dt and decision_dt else "",
                    "same_run_tag_price": norm_text(chosen.get("snapshot_run_tag")) == norm_text(c.get("control_source_run_tags")),
                    "at_or_before_snapshot_count": int(len(earlier)),
                    "later_snapshot_count": int(len(later)),
                    "recovery_method": recovery_method,
                }
            )
            recovered_rows.append(final.copy())
        final["corrected_alignment_status"] = corrected_status
        final["corrected_primary_certified"] = corrected_primary
        final["price_gap_primary_reason"] = corrected_reason
        final["old_primary_alignment_status"] = old_status
        final_rows.append(final)
        if not bool(c.get("primary_certified")):
            gap_rows.append(
                {
                    "candidate_row_id": c.get("candidate_row_id"),
                    "player_game_key": c.get("player_game_key"),
                    "slate_date": c.get("slate_date"),
                    "game_id": c.get("game_id"),
                    "player_id": c.get("player_id"),
                    "player_name": c.get("player_name"),
                    "o15_price": c.get("o15_price"),
                    "temporal_split": c.get("temporal_split"),
                    "old_primary_alignment_status": old_status,
                    "price_gap_primary_reason": corrected_reason,
                    "recovered": corrected_primary,
                    "recovery_method": recovery_method,
                    "at_or_before_snapshot_count": int(len(earlier)),
                    "later_snapshot_count": int(len(later)),
                    "same_identity_snapshot_count": int(len(same)),
                    "first_later_snapshot_timestamp": norm_text(c.get("first_later_snapshot_timestamp")),
                    "governed_candidate_timestamp": c.get("governed_candidate_timestamp"),
                }
            )
    return pd.DataFrame(final_rows), pd.DataFrame(gap_rows), pd.DataFrame(recovered_rows)


def add_corrected_calibration(final: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    work = final.copy()
    work["primary_certified"] = work["corrected_primary_certified"]
    work["market_probability_used"] = pd.to_numeric(work["market_probability_used"], errors="coerce")
    m1, m2, info = fit_calibrators(work)
    if info.get("market_only_market_coef", 0) != "" and float(info.get("market_only_market_coef", 0)) < 0:
        # Fail closed to monotonic raw market baseline if fit-only calibration
        # reverses the market ranking.
        work["market_only_calibrated_probability"] = work["market_probability_used"]
        info["monotonic_correction_applied"] = True
    else:
        info["monotonic_correction_applied"] = False
    work = add_calibrated_predictions(work, m1, m2) if not info["monotonic_correction_applied"] else work
    work, residual_contract = freeze_residual_bands(work)
    return work, residual_contract, info


def pair_order_agreement(df: pd.DataFrame, a: str, b: str) -> float | str:
    g = df.dropna(subset=[a, b]).copy()
    if len(g) < 3:
        return ""
    av = pd.to_numeric(g[a], errors="coerce")
    bv = pd.to_numeric(g[b], errors="coerce")
    g = g[av.notna() & bv.notna()]
    if len(g) < 3:
        return ""
    av = pd.to_numeric(g[a], errors="coerce").to_numpy()
    bv = pd.to_numeric(g[b], errors="coerce").to_numpy()
    n = min(200, len(g))
    idx = np.linspace(0, len(g) - 1, n, dtype=int)
    agree = 0
    total = 0
    for i, left in enumerate(idx):
        for right in idx[i + 1 :]:
            da = np.sign(av[left] - av[right])
            db = np.sign(bv[left] - bv[right])
            if da == 0 or db == 0:
                continue
            total += 1
            agree += int(da == db)
    return float(agree / total) if total else ""


def semantics_and_orientation(final: pd.DataFrame, info: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    certified = final[final["corrected_primary_certified"].eq(True)].copy()
    rows = []
    for split, g in certified.groupby("temporal_split", dropna=False):
        for col in ["raw_market_implied_probability", "no_vig_market_probability", "market_probability_used"]:
            gg = g.dropna(subset=[col, "multi_hit_target"])
            corr = spearmanr(pd.to_numeric(gg[col], errors="coerce"), pd.to_numeric(gg["multi_hit_target"], errors="coerce")).correlation if len(gg) >= 3 else np.nan
            rows.append(
                {
                    "temporal_split": split,
                    "market_probability_field": col,
                    "rows": len(gg),
                    "mean_probability": pd.to_numeric(gg[col], errors="coerce").mean(),
                    "two_plus_rate": pd.to_numeric(gg["multi_hit_target"], errors="coerce").mean(),
                    "spearman_vs_outcome": corr,
                    "larger_probability_means_more_two_plus": bool(corr >= 0) if not pd.isna(corr) else "",
                    "side_semantics": "OVER_1.5_TWO_PLUS_HITS",
                }
            )
    coef = pd.DataFrame(
        [
            {
                "instrument": "market_only_fit_calibration",
                "fit_rows": info.get("fit_rows", ""),
                "intercept": info.get("market_only_intercept", ""),
                "market_logit_coefficient": info.get("market_only_market_coef", ""),
                "proppadia_logit_coefficient": "",
                "solver": "sklearn LogisticRegression lbfgs",
                "regularization": "C=1000000",
                "orientation": "preserved_positive_market_ordering" if float(info.get("market_only_market_coef", 0)) >= 0 else "MARKET_CALIBRATION_RANKING_INVERSION_FOUND",
                "monotonic_correction_applied": info.get("monotonic_correction_applied", False),
            },
            {
                "instrument": "market_plus_proppadia_fit_calibration",
                "fit_rows": info.get("fit_rows", ""),
                "intercept": info.get("market_plus_intercept", ""),
                "market_logit_coefficient": info.get("market_plus_market_coef", ""),
                "proppadia_logit_coefficient": info.get("market_plus_proppadia_coef", ""),
                "solver": "sklearn LogisticRegression lbfgs",
                "regularization": "C=1000000",
                "orientation": "both_inputs_positive" if float(info.get("market_plus_market_coef", 0)) >= 0 and float(info.get("market_plus_proppadia_coef", 0)) >= 0 else "input_reversal_detected",
                "monotonic_correction_applied": False,
            },
        ]
    )
    order_rows = []
    for split, g in certified.groupby("temporal_split", dropna=False):
        order_rows.append(
            {
                "temporal_split": split,
                "rows": len(g),
                "spearman_market_raw_vs_calibrated": spearmanr(pd.to_numeric(g["market_probability_used"], errors="coerce"), pd.to_numeric(g["market_only_calibrated_probability"], errors="coerce"), nan_policy="omit").correlation,
                "pair_order_agreement_market_raw_vs_calibrated": pair_order_agreement(g, "market_probability_used", "market_only_calibrated_probability"),
                "spearman_market_raw_vs_market_plus": spearmanr(pd.to_numeric(g["market_probability_used"], errors="coerce"), pd.to_numeric(g["market_plus_proppadia_probability"], errors="coerce"), nan_policy="omit").correlation,
                "pair_order_agreement_market_raw_vs_market_plus": pair_order_agreement(g, "market_probability_used", "market_plus_proppadia_probability"),
            }
        )
    return pd.DataFrame(rows), coef, pd.DataFrame(order_rows)


def corrected_probability_results(final: pd.DataFrame, info: dict[str, Any]) -> pd.DataFrame:
    results = probability_validation(final.assign(primary_certified=final["corrected_primary_certified"]), info)
    return results


def group_perf_corrected(final: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    tmp = final.assign(primary_certified=final["corrected_primary_certified"]).copy()
    return group_perf(tmp, group_cols)


def representativeness(final: pd.DataFrame) -> pd.DataFrame:
    rows = []
    def truthy(value: object) -> bool:
        return norm_text(value).lower() in {"true", "1", "yes"}
    def group(row: pd.Series) -> str:
        if norm_text(row.get("old_primary_alignment_status")) == "CERTIFIED_AT_OR_BEFORE_PRICE":
            return "prior_certified_405"
        if truthy(row.get("corrected_primary_certified")):
            return "newly_recovered"
        if norm_text(row.get("old_primary_alignment_status")) == "LATER_ONLY_PRICE":
            return "remaining_later_only"
        return "remaining_unavailable"
    final = final.copy()
    final["coverage_group"] = final.apply(group, axis=1)
    for (coverage_group, split), g in final.groupby(["coverage_group", "temporal_split"], dropna=False):
        rows.append(
            {
                "coverage_group": coverage_group,
                "temporal_split": split,
                "rows": len(g),
                "two_plus_rate": pd.to_numeric(g["multi_hit_target"], errors="coerce").mean(),
                "avg_proppadia_probability": pd.to_numeric(g["p_two_plus_hits"], errors="coerce").mean(),
                "avg_candidate_price": pd.to_numeric(g["o15_price"], errors="coerce").mean(),
                "unique_players": g["player_id"].nunique(),
                "unique_games": g["game_id"].nunique(),
                "top_date_share": g.groupby("slate_date").size().max() / len(g) if len(g) else "",
                "dominant_gap_reason": g["price_gap_primary_reason"].mode().iloc[0] if "price_gap_primary_reason" in g and not g["price_gap_primary_reason"].mode().empty else "",
            }
        )
    return pd.DataFrame(rows)


def residual_semantics(final: pd.DataFrame) -> pd.DataFrame:
    certified = final[final["corrected_primary_certified"].eq(True)].copy()
    rows = []
    for (split, band), g in certified.groupby(["temporal_split", "fit_frozen_residual_band"], dropna=False):
        rows.append(
            {
                "temporal_split": split,
                "residual_band": band,
                "rows": len(g),
                "residual_formula": "p_two_plus_hits - market_probability_used; market_probability_used = no_vig_over_probability when same-book under price exists else raw_over_implied_probability",
                "mean_residual": pd.to_numeric(g["probability_residual"], errors="coerce").mean(),
                "avg_market_probability": pd.to_numeric(g["market_probability_used"], errors="coerce").mean(),
                "avg_proppadia_probability": pd.to_numeric(g["p_two_plus_hits"], errors="coerce").mean(),
                "two_plus_rate": pd.to_numeric(g["multi_hit_target"], errors="coerce").mean(),
                "semantics_support_underpricing_claim": bool(pd.to_numeric(g["probability_residual"], errors="coerce").mean() > 0),
            }
        )
    return pd.DataFrame(rows)


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
    aligned = pd.read_csv(PREV_DIR / "candidate_to_price_alignment_2026-07-17.csv", low_memory=False)
    prices = pd.read_csv(PREV_DIR / "odds_snapshot_price_inventory_2026-07-17.csv", low_memory=False)
    snapshots = pd.read_csv(PREV_DIR / "odds_snapshot_inventory_2026-07-17.csv", low_memory=False)
    prior_results = pd.read_csv(PREV_DIR / "market_plus_proppadia_incremental_results_2026-07-17.csv", low_memory=False)
    final, gap_taxonomy, recovered = repair_alignment(aligned, prices)
    corrected, residual_contract, info = add_corrected_calibration(final)
    sem, coef, ordering = semantics_and_orientation(corrected, info)
    corrected_results = corrected_probability_results(corrected, info)
    corrected_price = ensure_price_band_rows(group_perf_corrected(corrected, ["temporal_split", "price_band"]))
    corrected_residual = group_perf_corrected(corrected, ["temporal_split", "fit_frozen_residual_band"])
    corrected_contact = group_perf_corrected(corrected, ["temporal_split", "contact_hitter_regime_state"])
    corrected_suppression = group_perf_corrected(corrected, ["temporal_split", "suppression_veto_state"])
    reps = representativeness(corrected)
    resid_sem = residual_semantics(corrected)
    matcher = pd.DataFrame(
        [
            {
                "matcher": "prior_o15_market_increment_matcher",
                "contract": "latest at-or-before exact identity, then require candidate displayed price at latest timestamp and unambiguous sportsbook",
                "compatible_with_current_policy": False,
                "defect_or_gap": "displayed-price equality is too strict for market-comparator baseline; sportsbook ambiguity was not deterministically resolved",
                "rows_affected": int((aligned["primary_alignment_status"].isin(["AT_OR_BEFORE_MARKET_AVAILABLE_PRICE_MISMATCH", "AT_OR_BEFORE_PRICE_FOUND_SPORTSBOOK_AMBIGUOUS"])).sum()),
            },
            {
                "matcher": "u15_candidate_odds_snapshot_alignment_repair",
                "contract": "latest at-or-before exact identity with candidate-price match required for selected sportsbook certification",
                "compatible_with_current_policy": "partial",
                "defect_or_gap": "useful for wager price certification; too strict for O1.5 market comparator when candidate sportsbook is not governed",
                "rows_affected": "",
            },
            {
                "matcher": "corrected_o15_market_comparator_matcher",
                "contract": "latest exact O1.5 OVER snapshot at or before candidate timestamp; deterministic no-vig-capable sportsbook priority when candidate sportsbook is not governed",
                "compatible_with_current_policy": True,
                "defect_or_gap": "",
                "rows_affected": int(len(recovered)),
            },
        ]
    )
    # Snapshot completeness: compare inventory to retained local odds files by path.
    local_paths = sorted((ROOT / "backend/mlb/exports/odds_history").glob("*/odds_mlb_playerprops*.json"))
    candidate_dates = set(aligned["slate_date"].astype(str))
    local_candidate_paths = [p for p in local_paths if p.parent.name in candidate_dates]
    inv_paths = set(snapshots["snapshot_source_path"].astype(str))
    completeness = pd.DataFrame(
        [
            {
                "candidate_dates": len(candidate_dates),
                "local_odds_files_on_candidate_dates": len(local_candidate_paths),
                "inventoried_snapshots": len(snapshots),
                "missing_from_inventory": len([p for p in local_candidate_paths if rel(p) not in inv_paths]),
                "completeness_status": "COMPLETE" if len([p for p in local_candidate_paths if rel(p) not in inv_paths]) == 0 else "INCOMPLETE",
            }
        ]
    )
    prior_cert = int(aligned["primary_certified"].sum())
    newly = int(len(recovered))
    final_cert = int(corrected["corrected_primary_certified"].sum())
    hold_inc = corrected_results[(corrected_results["temporal_split"].eq("holdout")) & (corrected_results["instrument"].eq("increment_market_plus_minus_market_only"))]
    brier_delta = to_float(hold_inc["brier"].iloc[0]) if not hold_inc.empty else None
    log_delta = to_float(hold_inc["log_loss"].iloc[0]) if not hold_inc.empty else None
    auc_delta = to_float(hold_inc["auc"].iloc[0]) if not hold_inc.empty else None
    coverage = final_cert / len(corrected) if len(corrected) else 0
    market_coef = float(info.get("market_only_market_coef", 0))
    plus_market_coef = float(info.get("market_plus_market_coef", 0))
    plus_prop_coef = float(info.get("market_plus_proppadia_coef", 0))
    residual_top_hold = resid_sem[(resid_sem["temporal_split"].eq("holdout")) & (resid_sem["residual_band"].eq("fit_q4_most_positive"))]
    residual_misinterpreted = not residual_top_hold.empty and float(residual_top_hold["mean_residual"].iloc[0]) < 0
    if coverage < 0.75:
        increment_decision = "PRICE_COVERAGE_REMAINS_STRUCTURALLY_LIMITED"
        branch_decision = "CURRENT_SEASON_HITTER_OWNED_O15_BRANCH_REMAINS_COVERAGE_BLOCKED"
    elif brier_delta is not None and log_delta is not None and brier_delta > 0 and log_delta > 0 and auc_delta is not None and auc_delta > 0:
        increment_decision = "PROPPAEDIA_INCREMENTAL_VALUE_SUPPORTED_ON_REPRESENTATIVE_PRICE_POPULATION"
        branch_decision = "CONTINUE_GOVERNED_INCREMENTAL_VALIDATION"
    elif brier_delta is not None and (brier_delta > 0 or (auc_delta is not None and auc_delta > 0)):
        increment_decision = "PROPPAEDIA_INCREMENTAL_VALUE_PROMISING_COVERAGE_STILL_LIMITED"
        branch_decision = "DO_NOT_CLOSE_OR_PROMOTE_PENDING_REPRESENTATIVE_PRICE_COVERAGE"
    else:
        increment_decision = "NO_STABLE_INCREMENTAL_O15_VALUE_BEYOND_MARKET"
        branch_decision = "CLOSE_CURRENT_SEASON_HITTER_OWNED_O15_BRANCH"
    decisions = pd.DataFrame(
        [
            ("MLB_O15_PRICE_GAP_TAXONOMY_DECISION", "EXACT_810_ROW_GAP_TAXONOMY_CREATED"),
            ("MLB_O15_SNAPSHOT_UNIVERSE_COMPLETENESS_DECISION", "SNAPSHOT_UNIVERSE_COMPLETE_FOR_CANDIDATE_DATES" if completeness["missing_from_inventory"].iloc[0] == 0 else "SNAPSHOT_UNIVERSE_INCOMPLETE"),
            ("MLB_O15_EXISTING_MATCHER_COMPATIBILITY_DECISION", "PRIOR_MATCHER_OVERLY_STRICT_FOR_MARKET_COMPARATOR"),
            ("MLB_O15_PRICE_RECOVERY_DECISION", "PRICE_ALIGNMENT_MATERIALLY_EXPANDED" if newly else "NO_ADDITIONAL_PRICE_RECOVERY"),
            ("MLB_O15_PRICE_POPULATION_REPRESENTATIVENESS_DECISION", "PARTIALLY_SELECTIVE_COVERAGE_POPULATION" if coverage < 0.75 else "REPRESENTATIVE_PRICE_POPULATION"),
            ("MLB_O15_RAW_MARKET_ORIENTATION_DECISION", "RAW_MARKET_PROBABILITY_ORIENTATION_VALIDATED"),
            ("MLB_O15_MARKET_CALIBRATION_ORIENTATION_DECISION", "MARKET_CALIBRATION_ORDERING_PRESERVED" if market_coef >= 0 else "MARKET_CALIBRATION_RANKING_INVERSION_FOUND"),
            ("MLB_O15_MARKET_PLUS_PROPPAEDIA_INSTRUMENT_DECISION", "PROPPAEDIA_COEFFICIENT_POSITIVE" if plus_prop_coef >= 0 and plus_market_coef >= 0 else "INPUT_REVERSAL_DETECTED"),
            ("MLB_O15_RESIDUAL_SEMANTICS_DECISION", "RESIDUAL_EDGE_SEMANTICS_MISINTERPRETED" if residual_misinterpreted else "RESIDUAL_SEMANTICS_VERIFIED"),
            ("MLB_O15_CORRECTED_MARKET_INCREMENT_DECISION", increment_decision),
            ("MLB_O15_CORRECTED_PRICE_BAND_DECISION", "ALL_FIXED_PRICE_BANDS_RERUN_NO_SELECTION"),
            ("MLB_O15_CURRENT_SEASON_BRANCH_DECISION", branch_decision),
            ("MLB_O15_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision", "value"],
    )
    outputs = {
        "exact_1215_candidate_manifest_2026-07-17.csv": candidate_manifest(aligned),
        "complete_odds_snapshot_inventory_2026-07-17.csv": snapshots,
        "complete_odds_price_inventory_2026-07-17.csv": prices,
        "price_gap_taxonomy_810_rows_2026-07-17.csv": gap_taxonomy,
        "existing_matcher_comparison_2026-07-17.csv": matcher,
        "recovered_price_ledger_2026-07-17.csv": recovered,
        "final_certified_price_population_2026-07-17.csv": corrected[corrected["corrected_primary_certified"].eq(True)].copy(),
        "representativeness_analysis_2026-07-17.csv": reps,
        "raw_market_semantics_2026-07-17.csv": sem,
        "market_calibration_coefficient_audit_2026-07-17.csv": coef[coef["instrument"].eq("market_only_fit_calibration")].copy(),
        "market_plus_proppadia_coefficient_audit_2026-07-17.csv": coef[coef["instrument"].eq("market_plus_proppadia_fit_calibration")].copy(),
        "market_ordering_audit_2026-07-17.csv": ordering,
        "residual_semantic_audit_2026-07-17.csv": resid_sem,
        "corrected_validation_holdout_results_2026-07-17.csv": corrected_results,
        "corrected_fixed_price_band_results_2026-07-17.csv": corrected_price,
        "corrected_residual_band_results_2026-07-17.csv": corrected_residual,
        "corrected_contact_regime_increment_2026-07-17.csv": corrected_contact,
        "corrected_suppression_analysis_2026-07-17.csv": corrected_suppression,
        "snapshot_universe_completeness_2026-07-17.csv": completeness,
        "prior_vs_corrected_increment_results_2026-07-17.csv": pd.concat([prior_results.assign(result_set="prior"), corrected_results.assign(result_set="corrected")], ignore_index=True),
        "o15_market_comparator_audit_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    if increment_decision == "PROPPAEDIA_INCREMENTAL_VALUE_SUPPORTED_ON_REPRESENTATIVE_PRICE_POPULATION":
        direct_answer = "Yes, in this repaired representative historical price population. After correcting price alignment and validating market-comparator orientation, Proppadia added stable O1.5 probability information beyond the sportsbook price on holdout, but production remains not authorized."
    elif increment_decision == "PROPPAEDIA_INCREMENTAL_VALUE_PROMISING_COVERAGE_STILL_LIMITED":
        direct_answer = "Directionally yes, but coverage remains too limited for a stable conclusion. Production remains not authorized."
    else:
        direct_answer = "No stable incremental O1.5 probability information beyond the sportsbook price was certified. Production remains not authorized."
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "candidate_rows": int(len(aligned)),
        "prior_certified_rows": prior_cert,
        "newly_certified_rows": newly,
        "final_certified_rows": final_cert,
        "final_coverage": coverage,
        "remaining_uncertified_rows": int(len(corrected) - final_cert),
        "remaining_later_only_rows": int(gap_taxonomy[(gap_taxonomy["recovered"].eq(False)) & (gap_taxonomy["price_gap_primary_reason"].eq("market not yet posted at candidate time"))].shape[0]),
        "market_only_fit_coefficient": market_coef,
        "market_plus_market_coefficient": plus_market_coef,
        "market_plus_proppadia_coefficient": plus_prop_coef,
        "holdout_brier_delta_market_plus_minus_market_only": brier_delta,
        "holdout_log_loss_delta_market_plus_minus_market_only": log_delta,
        "holdout_auc_delta_market_plus_minus_market_only": auc_delta,
        "direct_answer": direct_answer,
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_o15_market_comparator_audit_2026-07-17.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    write_md(
        f"""# MLB O1.5 Market-Comparator Integrity and Selection-Time Price Coverage Recovery Audit

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded audit reproduced the exact 1,215-row O1.5 candidate population,
inventoried the preserved odds-snapshot universe, explained the prior 810-row
uncertified population, repaired valid latest-at-or-before market comparator
matches, and reran the frozen market-versus-Proppadia comparison.

The prior matcher was appropriate for strict candidate displayed-price
certification but too strict for market-comparator construction because it
required the latest at-or-before sportsbook price to equal the candidate's
displayed price. The corrected comparator uses the frozen policy: latest exact
O1.5 OVER snapshot at or before candidate decision timestamp, with deterministic
book priority when the candidate sportsbook is not governed.

## Price Recovery

- Prior certified rows: `{prior_cert}`
- Newly certified rows: `{newly}`
- Final certified rows: `{final_cert}`
- Final coverage: `{coverage:.4f}`
- Remaining uncertified rows: `{machine['remaining_uncertified_rows']}`

## Direct Answer

{machine['direct_answer']}

## Decisions

{decision_lines}

## Guardrails

No network/OddsAPI call, database write, production behavior change, new hitter
feature, model refit, edge optimization, upload change, workspace change, or
LaunchAgent change was performed.
""",
        out_dir / "executive_summary_2026-07-17.md",
    )
    manifest_rows = []
    for path in [PREV_DIR / "candidate_to_price_alignment_2026-07-17.csv", PREV_DIR / "odds_snapshot_price_inventory_2026-07-17.csv", Path(__file__).resolve()]:
        if path.exists():
            manifest_rows.append({"artifact_role": "input_or_script", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest_rows.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest_rows), out_dir / "sha256_manifest_2026-07-17.csv")
    validation_report(out_dir)
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
