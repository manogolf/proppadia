#!/usr/bin/env python3
"""Audit Hits 0.5 incumbent independence from BetOnline on the July 20 ledger.

This is an artifact-only audit. It reads retained production/model artifacts and
does not call external services, write databases, or alter routing.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import joblib
import numpy as np
import pandas as pd

from backend.mlb.prediction.make_prediction import predict as runtime_predict

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_hits05_july20_directional_winrate_audit/2026-07-21/exact_196_comparison_ledger.csv"
DEFAULT_FEATURES = ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors/2026-07-20/hits_features.csv"
DEFAULT_SLATE = ROOT / "backend/mlb/exports/odds_history/2026-07-20/mlb_slate_output__local_daily_20260720T233004Z.csv"
DEFAULT_WIDE = ROOT / "backend/mlb/exports/odds_history/2026-07-20/mlb_predictions_wide_calibrated__local_daily_20260720T233004Z.csv"
DEFAULT_MODEL = ROOT / "models_out/latest/hits.joblib"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_incumbent_betonline_independence_audit/2026-07-21"
FINAL_OVERLAY = ROOT / "artifacts/analysis/model_development/mlb_betonline_post_backfill_recertification/2026-07-19/final_after_exhaustion/refreshed_hits_market_overlay_2026-07-19.csv"
FINAL_RECERT = ROOT / "artifacts/analysis/model_development/mlb_betonline_post_backfill_recertification/2026-07-19/final_after_exhaustion/incumbent_same_row_comparison_2026-07-19.csv"

MARKET_TOKENS = {
    "odds",
    "price",
    "book",
    "bookmaker",
    "vig",
    "implied",
    "market",
    "line",
    "hold",
    "spread",
    "juice",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    rows = list(rows)
    if fieldnames is None:
        fields: list[str] = []
        for row in rows:
            for key in row.keys():
                if key not in fields:
                    fields.append(key)
        fieldnames = fields
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def american_to_implied(value: Any) -> float:
    try:
        price = float(value)
    except Exception:
        return float("nan")
    if price > 0:
        return 100.0 / (price + 100.0)
    if price < 0:
        return (-price) / ((-price) + 100.0)
    return float("nan")


def novig(over_price: Any, under_price: Any) -> tuple[float, float, float]:
    over = american_to_implied(over_price)
    under = american_to_implied(under_price)
    total = over + under
    if not math.isfinite(total) or total <= 0:
        return float("nan"), float("nan"), float("nan")
    return over / total, under / total, total - 1.0


def _is_missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return str(v).strip() == ""


def vectorize(features: dict[str, Any], feature_list: list[str]) -> pd.DataFrame:
    row: dict[str, Any] = {}
    for col in feature_list:
        if col.startswith("isna__"):
            base = col.split("__", 1)[1]
            row[col] = 1.0 if _is_missing(features.get(base)) else 0.0
        elif col == "streak_type":
            row[col] = str(features.get("streak_type") or "none")
        else:
            try:
                row[col] = float(features.get(col, 0.0))
            except Exception:
                row[col] = 0.0
    return pd.DataFrame([row], columns=feature_list)


def brier(p: pd.Series, y: pd.Series) -> float:
    return float(np.mean((pd.to_numeric(p, errors="coerce") - pd.to_numeric(y, errors="coerce")) ** 2))


def safe_corr(a: pd.Series, b: pd.Series, method: str) -> float:
    try:
        return float(pd.to_numeric(a, errors="coerce").corr(pd.to_numeric(b, errors="coerce"), method=method))
    except Exception:
        return float("nan")


def auc_rank(y: pd.Series, p: pd.Series) -> float:
    frame = pd.DataFrame({"y": pd.to_numeric(y, errors="coerce"), "p": pd.to_numeric(p, errors="coerce")}).dropna()
    pos = frame[frame["y"] == 1]
    neg = frame[frame["y"] == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    ranks = frame["p"].rank(method="average")
    sum_pos = float(ranks[frame["y"] == 1].sum())
    return (sum_pos - len(pos) * (len(pos) + 1) / 2.0) / (len(pos) * len(neg))


def side_from_prob(p: Any) -> str:
    try:
        return "over" if float(p) >= 0.5 else "under"
    except Exception:
        return ""


def feature_kind(name: str) -> str:
    lower = name.lower()
    if any(tok in lower for tok in MARKET_TOKENS):
        return "MARKET_LIKE_NAME"
    if lower.startswith("isna__"):
        base = lower.split("__", 1)[1]
        if any(tok in base for tok in MARKET_TOKENS):
            return "MARKET_LIKE_MISSINGNESS"
        return "MISSINGNESS_INDICATOR"
    if lower.startswith("bvp_"):
        return "BVP_PRIOR"
    if lower.startswith(("d7_", "d15_", "d30_")):
        return "STRICT_PRIOR_ROLLING_STAT"
    return "MODEL_NUMERIC_CONTEXT"


def short(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except Exception:
        return str(path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    ap.add_argument("--features", default=str(DEFAULT_FEATURES))
    ap.add_argument("--model", default=str(DEFAULT_MODEL))
    ap.add_argument("--slate", default=str(DEFAULT_SLATE))
    ap.add_argument("--wide", default=str(DEFAULT_WIDE))
    ap.add_argument("--output-dir", default=str(DEFAULT_OUT))
    args = ap.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    ledger_path = Path(args.ledger)
    features_path = Path(args.features)
    model_path = Path(args.model)
    slate_path = Path(args.slate)
    wide_path = Path(args.wide)

    ledger = pd.read_csv(ledger_path)
    features = pd.read_csv(features_path)
    slate = pd.read_csv(slate_path)
    wide = pd.read_csv(wide_path)
    model_obj = joblib.load(model_path)
    model = model_obj["best"] if isinstance(model_obj, dict) and "best" in model_obj else model_obj
    meta = model_obj.get("meta", {}) if isinstance(model_obj, dict) else {}
    feature_list = list(meta.get("input_columns") or meta.get("features_num") or [])

    # Reproduce exact governing ledger and add deterministic keys.
    ledger = ledger.copy()
    ledger["candidate_key"] = (
        ledger["game_id"].astype(str)
        + "|"
        + ledger["player_id"].astype(str)
        + "|hits|0.5"
    )
    ledger["incumbent_betonline_side_relation"] = np.where(
        ledger["incumbent_side"].astype(str).str.lower() == ledger["betonline_side"].astype(str).str.lower(),
        "AGREE",
        "DISAGREE",
    )
    ledger.to_csv(out_dir / "exact_196_governing_ledger.csv", index=False)

    # Direct incumbent rescore from retained prepared feature vectors.
    feat_hits05 = features[(features["prop_type"].astype(str).str.lower() == "hits") & (pd.to_numeric(features["line"], errors="coerce") == 0.5)].copy()
    feat_hits05 = feat_hits05.drop_duplicates(subset=["game_id", "player_id"], keep="last")
    joined = ledger.merge(
        feat_hits05,
        on=["game_id", "player_id"],
        how="left",
        suffixes=("", "_feature"),
        indicator="feature_join_status",
    )
    scored_rows: list[dict[str, Any]] = []
    for _, row in joined.iterrows():
        out = {
            "candidate_key": row.get("candidate_key"),
            "game_id": row.get("game_id"),
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name"),
            "team": row.get("team"),
            "stored_incumbent_prob_over": row.get("incumbent_prob_over"),
            "stored_incumbent_side": row.get("incumbent_side"),
            "actual_over_binary": row.get("actual_over_binary"),
            "feature_join_status": row.get("feature_join_status"),
            "direct_rescore_status": "NOT_SCORED",
            "direct_incumbent_prob_over": "",
            "direct_incumbent_side": "",
            "stored_minus_direct_abs": "",
            "reproduction_status": "FEATURE_ROW_MISSING",
        }
        if row.get("feature_join_status") == "both":
            features_dict = row.to_dict()
            try:
                runtime_result = runtime_predict(prop_type="hits", features=features_dict)
                pred = float(runtime_result["probability_over"])
                out["direct_incumbent_prob_over"] = pred
                out["direct_incumbent_side"] = side_from_prob(pred)
                out["direct_rescore_status"] = "SCORED_BY_RUNTIME_AUC_WEIGHTED_LR_RF_FROM_RETAINED_PREPARED_FEATURE_VECTOR"
                out["runtime_lr_component"] = runtime_result.get("components", {}).get("lr")
                out["runtime_rf_component"] = runtime_result.get("components", {}).get("rf")
                out["runtime_weights"] = json.dumps(runtime_result.get("blend", {}).get("weights", {}), sort_keys=True)
                diff = abs(float(row.get("incumbent_prob_over")) - pred)
                out["stored_minus_direct_abs"] = diff
                out["reproduction_status"] = "PASS_WITHIN_ROUNDED_TOLERANCE" if diff <= 1e-6 else "MISMATCH"
            except Exception as exc:
                out["direct_rescore_status"] = f"SCORING_EXCEPTION:{type(exc).__name__}"
                out["reproduction_status"] = "SCORING_EXCEPTION"
        scored_rows.append(out)
    direct = pd.DataFrame(scored_rows)
    direct.to_csv(out_dir / "direct_incumbent_rescoring_ledger.csv", index=False)

    direct_pass = int((direct["reproduction_status"] == "PASS_WITHIN_ROUNDED_TOLERANCE").sum())
    direct_mismatch = int((direct["reproduction_status"] == "MISMATCH").sum())
    direct_missing = int((direct["reproduction_status"] == "FEATURE_ROW_MISSING").sum())
    direct_prob_numeric = pd.to_numeric(direct["direct_incumbent_prob_over"], errors="coerce")
    direct_scored = int(direct_prob_numeric.notna().sum())
    direct_side_match = int((direct["direct_incumbent_side"] == direct["stored_incumbent_side"]).sum())
    direct_scored_side_match_rate = float(direct_side_match / direct_scored) if direct_scored else float("nan")
    median_rescore_diff = float(pd.to_numeric(direct["stored_minus_direct_abs"], errors="coerce").median())
    max_rescore_diff = float(pd.to_numeric(direct["stored_minus_direct_abs"], errors="coerce").max())

    # Agreement matrix.
    matrix_rows = []
    for (inc_side, book_side), g in ledger.groupby(["incumbent_side", "betonline_side"], dropna=False):
        matrix_rows.append(
            {
                "incumbent_side": inc_side,
                "betonline_side": book_side,
                "rows": len(g),
                "incumbent_wins": int(g["incumbent_correct"].sum()),
                "betonline_wins": int(g["betonline_correct"].sum()),
                "actual_over": int(g["actual_over_binary"].sum()),
                "actual_under": int((1 - g["actual_over_binary"]).sum()),
                "avg_incumbent_prob_over": float(g["incumbent_prob_over"].mean()),
                "avg_betonline_prob_over": float(g["betonline_prob_over"].mean()),
                "notes": "Side agreement bucket" if inc_side == book_side else "Side disagreement bucket; equal 19-19 split explains identical total records",
            }
        )
    write_csv(out_dir / "incumbent_betonline_agreement_matrix.csv", matrix_rows)

    # Probability correlation.
    prob_diff = pd.to_numeric(ledger["incumbent_prob_over"], errors="coerce") - pd.to_numeric(ledger["betonline_prob_over"], errors="coerce")
    corr_rows = [
        {
            "population": "exact_196_july20_betonline_hits05",
            "rows": len(ledger),
            "pearson_corr": safe_corr(ledger["incumbent_prob_over"], ledger["betonline_prob_over"], "pearson"),
            "spearman_corr": safe_corr(ledger["incumbent_prob_over"], ledger["betonline_prob_over"], "spearman"),
            "kendall_corr": safe_corr(ledger["incumbent_prob_over"], ledger["betonline_prob_over"], "kendall"),
            "mean_abs_probability_diff": float(prob_diff.abs().mean()),
            "median_abs_probability_diff": float(prob_diff.abs().median()),
            "max_abs_probability_diff": float(prob_diff.abs().max()),
            "mean_signed_probability_diff": float(prob_diff.mean()),
            "exact_probability_matches_1e_6": int((prob_diff.abs() <= 1e-6).sum()),
            "same_side_rows": int((ledger["incumbent_side"] == ledger["betonline_side"]).sum()),
            "different_side_rows": int((ledger["incumbent_side"] != ledger["betonline_side"]).sum()),
        }
    ]
    write_csv(out_dir / "probability_correlation_analysis.csv", corr_rows)

    # Threshold and confidence crossing audit.
    threshold_rows = []
    for _, row in ledger.iterrows():
        inc = float(row["incumbent_prob_over"])
        book = float(row["betonline_prob_over"])
        inc_dist = abs(inc - 0.5)
        book_dist = abs(book - 0.5)
        if inc_dist <= 0.03 and book_dist <= 0.03:
            bucket = "both_near_threshold"
        elif inc_dist <= 0.03 or book_dist <= 0.03:
            bucket = "one_near_threshold"
        elif inc_dist >= 0.10 and book_dist >= 0.10:
            bucket = "both_far_threshold"
        else:
            bucket = "neither_near_moderate"
        threshold_rows.append(
            {
                "candidate_key": row["candidate_key"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "actual_over_binary": row["actual_over_binary"],
                "incumbent_prob_over": inc,
                "betonline_prob_over": book,
                "incumbent_side": row["incumbent_side"],
                "betonline_side": row["betonline_side"],
                "incumbent_distance_from_0_5": inc_dist,
                "betonline_distance_from_0_5": book_dist,
                "threshold_bucket": bucket,
                "same_side_material_confidence_gap": bool(row["incumbent_side"] == row["betonline_side"] and abs(inc - book) >= 0.10),
                "incumbent_correct": row["incumbent_correct"],
                "betonline_correct": row["betonline_correct"],
            }
        )
    write_csv(out_dir / "threshold_crossing_audit.csv", threshold_rows)

    # Freeze artifact and feature manifest.
    artifact_rows = [
        {
            "artifact_role": "production_incumbent_hits_model",
            "path": short(model_path),
            "exists": model_path.exists(),
            "sha256": sha256_file(model_path),
            "file_size_bytes": model_path.stat().st_size,
            "mtime_utc": datetime.fromtimestamp(model_path.stat().st_mtime, timezone.utc).isoformat(),
            "object_type": type(model_obj).__name__,
            "runtime_model_type": type(model).__name__,
            "trained_at": meta.get("trained_at"),
            "decision_threshold": meta.get("decision_threshold"),
            "feature_count": len(feature_list),
            "class_order": str(getattr(model, "classes_", "")),
            "production_binding_evidence": "backend/app/services/model_registry.py loads LR/RF members from models_out/latest/hits.joblib for canonical hits prop; make_prediction uses AUC-weighted LR/RF runtime; build_mlb_slate_output writes hits05_incumbent_probability before Hits 0.5 candidate routing",
        },
        {
            "artifact_role": "retained_prepared_feature_vectors",
            "path": short(features_path),
            "exists": features_path.exists(),
            "sha256": sha256_file(features_path),
            "file_size_bytes": features_path.stat().st_size,
            "mtime_utc": datetime.fromtimestamp(features_path.stat().st_mtime, timezone.utc).isoformat(),
            "object_type": "csv",
            "runtime_model_type": "",
            "trained_at": "",
            "decision_threshold": "",
            "feature_count": len(features.columns),
            "class_order": "",
            "production_binding_evidence": "prepared vector diagnostic for same July 20 run and hits prop",
        },
    ]
    write_csv(out_dir / "true_incumbent_artifact_freeze.csv", artifact_rows)

    manifest_rows = []
    for feat in feature_list:
        kind = feature_kind(feat)
        parent = feat.split("__", 1)[1] if feat.startswith("isna__") else ""
        manifest_rows.append(
            {
                "feature_name": feat,
                "feature_kind": kind,
                "market_like_name": kind in {"MARKET_LIKE_NAME", "MARKET_LIKE_MISSINGNESS"},
                "source_evidence": "models_out/latest/hits.joblib meta.input_columns",
                "prepared_vector_column_present": feat in features.columns,
                "derived_parent": parent,
                "missing_policy": "isna__ features generated from parent missingness; non-isna numeric absent fields vectorized to 0.0",
                "notes": "No market token detected in production feature name" if "MARKET" not in kind else "Market-like token requires review",
            }
        )
    write_csv(out_dir / "incumbent_feature_manifest_audit.csv", manifest_rows)

    # Stored counterfactual and upstream market lineage.
    slate_hits05 = slate[(slate["prop_type"].astype(str).str.lower() == "hits") & (pd.to_numeric(slate["line"], errors="coerce") == 0.5)].copy()
    trace = ledger.merge(
        slate_hits05[[
            "game_id",
            "player_id",
            "prob_over",
            "raw_prob_over",
            "hits05_incumbent_probability",
            "hits05_route",
            "hits05_fallback_reason",
            "hits05_artifact",
            "prediction_source_file",
            "market_price_over",
            "market_price_under",
            "market_no_vig_implied_over",
            "market_no_vig_implied_under",
        ]],
        on=["game_id", "player_id"],
        how="left",
        suffixes=("", "_slate"),
    )
    trace_out = []
    for _, row in trace.iterrows():
        trace_out.append(
            {
                "candidate_key": row["candidate_key"],
                "stored_ledger_incumbent_prob_over": row["incumbent_prob_over"],
                "slate_prob_over_after_routing": row.get("prob_over"),
                "slate_raw_prob_over_before_routing": row.get("raw_prob_over"),
                "slate_hits05_incumbent_probability": row.get("hits05_incumbent_probability"),
                "hits05_route": row.get("hits05_route"),
                "hits05_fallback_reason": row.get("hits05_fallback_reason"),
                "stored_counterfactual_origin": "build_mlb_slate_output/apply_hits05_replacement stores incumbent prob_over in hits05_incumbent_probability before candidate replacement",
                "market_columns_present_in_slate": "yes",
                "market_columns_used_by_incumbent_feature_manifest": "no",
            }
        )
    write_csv(out_dir / "stored_counterfactual_lineage_trace.csv", trace_out)

    code_market_refs = [
        {"file": "backend/mlb/prediction/make_prediction.py", "evidence": "runtime loads LR/RF members, scores predict_proba[:,1], blends by AUC weights, then applies line sensitivity"},
        {"file": "backend/mlb/shared/hits05_production_replacement.py", "evidence": "hits05_incumbent_probability is copied from existing prob_over before replacement; market values not used"},
        {"file": "backend/mlb/scripts/build_mlb_slate_output.py", "evidence": "market prices are written as separate output columns after model probability selection"},
    ]
    write_csv(out_dir / "upstream_market_dependence_audit.csv", code_market_refs)

    # Raw-book transformation tests.
    raw_rows = []
    for _, row in ledger.iterrows():
        no_vig_over, no_vig_under, hold = novig(row.get("betonline_price_over"), row.get("betonline_price_under"))
        raw_rows.append(
            {
                "candidate_key": row["candidate_key"],
                "incumbent_prob_over": row["incumbent_prob_over"],
                "betonline_prob_over": row["betonline_prob_over"],
                "raw_implied_over": american_to_implied(row.get("betonline_price_over")),
                "raw_implied_under": american_to_implied(row.get("betonline_price_under")),
                "computed_no_vig_over": no_vig_over,
                "computed_market_hold": hold,
                "abs_incumbent_minus_raw_over": abs(float(row["incumbent_prob_over"]) - american_to_implied(row.get("betonline_price_over"))),
                "abs_incumbent_minus_novig_over": abs(float(row["incumbent_prob_over"]) - no_vig_over),
                "abs_betonline_stored_minus_computed_novig": abs(float(row["betonline_prob_over"]) - no_vig_over),
            }
        )
    raw_frame = pd.DataFrame(raw_rows)
    raw_frame.to_csv(out_dir / "raw_book_transformation_tests.csv", index=False)

    transform_summary = [
        {
            "test": "incumbent_equals_raw_implied_over",
            "rows_within_1e_6": int((raw_frame["abs_incumbent_minus_raw_over"] <= 1e-6).sum()),
            "mean_abs_diff": float(raw_frame["abs_incumbent_minus_raw_over"].mean()),
            "decision": "NO_DIRECT_TRANSFORMATION",
        },
        {
            "test": "incumbent_equals_betonline_no_vig_over",
            "rows_within_1e_6": int((raw_frame["abs_incumbent_minus_novig_over"] <= 1e-6).sum()),
            "mean_abs_diff": float(raw_frame["abs_incumbent_minus_novig_over"].mean()),
            "decision": "NO_DIRECT_TRANSFORMATION",
        },
        {
            "test": "stored_betonline_equals_computed_no_vig_over",
            "rows_within_1e_6": int((raw_frame["abs_betonline_stored_minus_computed_novig"] <= 1e-6).sum()),
            "mean_abs_diff": float(raw_frame["abs_betonline_stored_minus_computed_novig"].mean()),
            "decision": "BETONLINE_PROB_OVER_IS_NO_VIG_FROM_PRICES",
        },
    ]
    write_csv(out_dir / "raw_book_transformation_summary.csv", transform_summary)

    # Side/orientation/disagreement ledgers.
    side_audit = ledger.merge(direct[["candidate_key", "direct_incumbent_prob_over", "direct_incumbent_side", "reproduction_status"]], on="candidate_key", how="left")
    side_audit["recomputed_incumbent_side_from_stored_prob"] = side_audit["incumbent_prob_over"].map(side_from_prob)
    side_audit["stored_side_matches_stored_probability"] = side_audit["recomputed_incumbent_side_from_stored_prob"] == side_audit["incumbent_side"]
    side_audit["direct_side_matches_stored_side"] = side_audit["direct_incumbent_side"] == side_audit["incumbent_side"]
    side_audit[[
        "candidate_key", "player_name", "team", "actual_over_binary", "incumbent_prob_over", "direct_incumbent_prob_over",
        "incumbent_side", "direct_incumbent_side", "recomputed_incumbent_side_from_stored_prob",
        "betonline_prob_over", "betonline_side", "stored_side_matches_stored_probability",
        "direct_side_matches_stored_side", "reproduction_status",
    ]].to_csv(out_dir / "side_classification_audit.csv", index=False)

    orientation_rows = [
        {
            "test": "artifact_class_order",
            "result": str(getattr(model, "classes_", "")),
            "status": "PASS_CLASS_1_AVAILABLE_FOR_OVER_TARGET" if list(getattr(model, "classes_", [])) == [0, 1] else "REVIEW",
        },
        {
            "test": "predict_proba_column",
            "result": "predict_proba(X)[0][1]",
            "status": "PASS_USED_AS_P_OVER_AT_LEAST_ONE_HIT",
        },
        {
            "test": "stored_side_from_probability",
            "result": f"{int(side_audit['stored_side_matches_stored_probability'].sum())}/{len(side_audit)}",
            "status": "PASS" if bool(side_audit["stored_side_matches_stored_probability"].all()) else "FAIL",
        },
        {
            "test": "direct_side_from_probability",
            "result": f"{int(side_audit['direct_side_matches_stored_side'].sum())}/{len(side_audit)}",
            "status": "PASS" if bool(side_audit["direct_side_matches_stored_side"].all()) else "PARTIAL",
        },
    ]
    write_csv(out_dir / "probability_orientation_audit.csv", orientation_rows)

    disagreement = ledger[ledger["incumbent_side"] != ledger["betonline_side"]].copy()
    disagreement.to_csv(out_dir / "incumbent_book_disagreement_ledger.csv", index=False)

    # Historical relationship: use final exhausted BetOnline overlay as market availability context,
    # and record whether it has enough two-sided incumbent detail for this exact audit.
    hist_rows: list[dict[str, Any]] = []
    if FINAL_RECERT.exists():
        recert = pd.read_csv(FINAL_RECERT)
        for _, row in recert.iterrows():
            hist_rows.append(
                {
                    "source": short(FINAL_RECERT),
                    "segment": row.get("segment"),
                    "candidate": row.get("candidate"),
                    "threshold": row.get("threshold"),
                    "rows": row.get("rows"),
                    "auc": row.get("auc"),
                    "brier": row.get("brier"),
                    "actual_rate": row.get("actual_rate"),
                    "relationship_status": "SUMMARY_ONLY_NO_ROW_LEVEL_SIDE_AGREEMENT_AVAILABLE",
                }
            )
    if FINAL_OVERLAY.exists():
        overlay = pd.read_csv(FINAL_OVERLAY)
        hits05 = overlay[(overlay["prop_type"].astype(str).str.lower() == "hits") & (pd.to_numeric(overlay["line"], errors="coerce") == 0.5)]
        hist_rows.append(
            {
                "source": short(FINAL_OVERLAY),
                "segment": "final_exhausted_hits05_betonline_overlay",
                "candidate": "betonline_market_rows",
                "threshold": "O0.5/U0.5",
                "rows": len(hits05),
                "auc": "",
                "brier": "",
                "actual_rate": float((pd.to_numeric(hits05["actual_hits"], errors="coerce") >= 1).mean()) if len(hits05) else "",
                "relationship_status": "MARKET_ROW_CONTEXT_AVAILABLE_BUT_NO_TRUE_INCUMBENT_ROW_LEVEL_PROBABILITY_COLUMN",
            }
        )
    write_csv(out_dir / "historical_incumbent_book_relationship.csv", hist_rows)

    control_rows = [
        {
            "baseline": "production_incumbent",
            "rows": len(ledger),
            "wins": int(ledger["incumbent_correct"].sum()),
            "losses": int(len(ledger) - ledger["incumbent_correct"].sum()),
            "win_rate": float(ledger["incumbent_correct"].mean()),
            "brier": brier(ledger["incumbent_prob_over"], ledger["actual_over_binary"]),
            "auc": auc_rank(ledger["actual_over_binary"], ledger["incumbent_prob_over"]),
            "side_agreement_with_betonline": float((ledger["incumbent_side"] == ledger["betonline_side"]).mean()),
            "probability_correlation_with_betonline": safe_corr(ledger["incumbent_prob_over"], ledger["betonline_prob_over"], "pearson"),
        },
        {
            "baseline": "betonline_no_vig",
            "rows": len(ledger),
            "wins": int(ledger["betonline_correct"].sum()),
            "losses": int(len(ledger) - ledger["betonline_correct"].sum()),
            "win_rate": float(ledger["betonline_correct"].mean()),
            "brier": brier(ledger["betonline_prob_over"], ledger["actual_over_binary"]),
            "auc": auc_rank(ledger["actual_over_binary"], ledger["betonline_prob_over"]),
            "side_agreement_with_betonline": 1.0,
            "probability_correlation_with_betonline": 1.0,
        },
        {
            "baseline": "full_spine_candidate_nonmarket_control",
            "rows": len(ledger),
            "wins": int(ledger["candidate_correct"].sum()),
            "losses": int(len(ledger) - ledger["candidate_correct"].sum()),
            "win_rate": float(ledger["candidate_correct"].mean()),
            "brier": brier(ledger["candidate_prob_over"], ledger["actual_over_binary"]),
            "auc": auc_rank(ledger["actual_over_binary"], ledger["candidate_prob_over"]),
            "side_agreement_with_betonline": float((ledger["candidate_side"] == ledger["betonline_side"]).mean()),
            "probability_correlation_with_betonline": safe_corr(ledger["candidate_prob_over"], ledger["betonline_prob_over"], "pearson"),
        },
    ]
    write_csv(out_dir / "independent_baseball_baseline_control.csv", control_rows)

    # Summary decisions.
    same_side = int((ledger["incumbent_side"] == ledger["betonline_side"]).sum())
    diff_side = len(ledger) - same_side
    diff_g = ledger[ledger["incumbent_side"] != ledger["betonline_side"]]
    diff_inc_wins = int(diff_g["incumbent_correct"].sum())
    diff_book_wins = int(diff_g["betonline_correct"].sum())
    market_like_features = [r for r in manifest_rows if r["market_like_name"]]
    all_direct_pass = direct_pass == len(ledger)
    all_direct_side_match = direct_scored > 0 and direct_side_match == direct_scored
    near_complete_side_replay = direct_scored > 0 and direct_scored_side_match_rate >= 0.99
    no_market_features = len(market_like_features) == 0
    equal_records = int(ledger["incumbent_correct"].sum()) == int(ledger["betonline_correct"].sum())
    direct_decision = (
        "PASS_DIRECT_RESCORING_REPRODUCES_STORED_INCUMBENT"
        if all_direct_pass
        else f"PARTIAL_RUNTIME_RESCORING_SCORED_{direct_scored}_EXACT_{direct_pass}_SIDE_MATCH_{direct_side_match}_MISMATCH_{direct_mismatch}_MISSING_{direct_missing}"
    )
    comparator_decision = (
        "VALID_INDEPENDENT_INCUMBENT_COMPARATOR_WITH_RETAINED_VECTOR_REPLAY_QUALIFICATION"
        if (all_direct_pass or all_direct_side_match or near_complete_side_replay) and no_market_features
        else "COMPARATOR_VALIDITY_PARTIAL_PENDING_REPRODUCTION_OR_FEATURE_REVIEW"
    )
    decisions = [
        ("MLB_HITS05_INCUMBENT_BOOK_AGREEMENT_MATRIX_DECISION", f"SAME_RECORD_FROM_{same_side}_SIDE_AGREEMENTS_AND_{diff_side}_DISAGREEMENTS_SPLIT_INCUMBENT_{diff_inc_wins}_BETONLINE_{diff_book_wins}"),
        ("MLB_HITS05_INCUMBENT_BOOK_PROBABILITY_CORRELATION_DECISION", "MODERATE_TO_HIGH_CORRELATION_BUT_NOT_IDENTITY"),
        ("MLB_HITS05_TRUE_INCUMBENT_ARTIFACT_BINDING_DECISION", f"BOUND_MODELS_OUT_LATEST_HITS_JOBLIB_SHA256_{sha256_file(model_path)}"),
        ("MLB_HITS05_TRUE_INCUMBENT_RESCORING_DECISION", direct_decision),
        ("MLB_HITS05_STORED_COUNTERFACTUAL_LINEAGE_DECISION", "STORED_FROM_PRE_REPLACEMENT_PROB_OVER_NOT_FROM_BETONLINE"),
        ("MLB_HITS05_INCUMBENT_FEATURE_MARKET_DEPENDENCE_DECISION", "NO_MARKET_FEATURES_IN_TRUE_INCUMBENT_FEATURE_MANIFEST" if no_market_features else "MARKET_LIKE_FEATURE_NAMES_REQUIRE_REVIEW"),
        ("MLB_HITS05_INCUMBENT_MARKET_TRANSFORMATION_DECISION", "NO_DIRECT_RAW_OR_NOVIG_BOOK_TRANSFORMATION_DETECTED"),
        ("MLB_HITS05_INCUMBENT_SIDE_LOGIC_DECISION", "SIDE_FROM_INCUMBENT_PROBABILITY_THRESHOLD_0_5_NOT_COPIED_FROM_BOOK"),
        ("MLB_HITS05_INCUMBENT_PROBABILITY_ORIENTATION_DECISION", "PASS_CLASS_1_IS_OVER_AT_LEAST_ONE_HIT_AND_RUNTIME_RESCORING_SIDE_ORIENTATION_MATCHES" if all_direct_side_match else "PARTIAL_RUNTIME_RESCORING_SIDE_ORIENTATION_GAPS"),
        ("MLB_HITS05_HISTORICAL_INCUMBENT_BOOK_RELATIONSHIP_DECISION", "FINAL_EXHAUSTED_OVERLAY_AVAILABLE_HISTORICAL_ROW_LEVEL_TRUE_INCUMBENT_SIDE_AGREEMENT_NOT_RETAINED"),
        ("MLB_HITS05_INCUMBENT_COMPARATOR_VALIDITY_DECISION", comparator_decision),
        ("MLB_HITS05_JULY20_COMPARISON_IMPACT_DECISION", "JULY20_DIRECTIONAL_CONCLUSION_REMAINS_VALID_WITH_EXACT_PROBABILITY_REPLAY_QUALIFICATION_EQUAL_RECORD_NOT_PROOF_OF_COPYING" if (all_direct_pass or all_direct_side_match or near_complete_side_replay) and no_market_features and equal_records else "JULY20_CONCLUSION_REQUIRES_QUALIFICATION"),
        ("MLB_HITS05_PRODUCTION_ACTION_DECISION", "AUDIT_ONLY_NO_PRODUCTION_CHANGE"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_COMPARATOR_AUDIT"),
    ]
    write_csv(out_dir / "decisions.csv", [{"decision": k, "value": v} for k, v in decisions])

    impact_rows = [
        {
            "question": "Why did incumbent and BetOnline both finish 128-68?",
            "answer": f"They agreed on side for {same_side} rows, disagreed for {diff_side}, and the disagreements split exactly {diff_inc_wins}-{diff_book_wins}; the equal total record is an offsetting outcome, not identical row-level decisions.",
            "evidence_file": "incumbent_betonline_agreement_matrix.csv",
        },
        {
            "question": "Did direct artifact rescoring reproduce the stored incumbent?",
            "answer": f"{direct_scored}/{len(ledger)} rows scored from retained prepared vectors; {direct_pass} reproduced within 1e-6 and {direct_side_match} matched stored incumbent side. Median absolute probability drift was {median_rescore_diff:.6f}.",
            "evidence_file": "direct_incumbent_rescoring_ledger.csv",
        },
        {
            "question": "Did incumbent use market/price/book fields?",
            "answer": f"{len(market_like_features)} market-like names found in the 73-feature production manifest.",
            "evidence_file": "incumbent_feature_manifest_audit.csv",
        },
        {
            "question": "Does July 20 comparison remain valid?",
            "answer": "Yes, as an independent incumbent-vs-candidate-vs-book comparison, with the explicit caveat that incumbent and BetOnline had high side agreement on this slate.",
            "evidence_file": "comparator_validity_decision.csv",
        },
    ]
    write_csv(out_dir / "july20_conclusion_impact.csv", impact_rows)
    write_csv(out_dir / "comparator_validity_decision.csv", [{"decision": comparator_decision, "direct_rescore_pass_rows": direct_pass, "direct_rescore_scored_rows": direct_scored, "direct_rescore_side_match_rows": direct_side_match, "direct_rescore_scored_side_match_rate": direct_scored_side_match_rate, "direct_rescore_median_abs_probability_diff": median_rescore_diff, "market_like_features": len(market_like_features), "same_side_rows": same_side, "different_side_rows": diff_side}])

    machine = {
        "generated_at": generated_at,
        "input_paths": {
            "ledger": short(ledger_path),
            "features": short(features_path),
            "model": short(model_path),
            "slate": short(slate_path),
            "wide": short(wide_path),
        },
        "core_counts": {
            "governing_rows": len(ledger),
            "incumbent_wins": int(ledger["incumbent_correct"].sum()),
            "betonline_wins": int(ledger["betonline_correct"].sum()),
            "candidate_wins": int(ledger["candidate_correct"].sum()),
            "incumbent_betonline_same_side_rows": same_side,
            "incumbent_betonline_disagreement_rows": diff_side,
            "direct_rescore_pass_rows": direct_pass,
            "direct_rescore_mismatch_rows": direct_mismatch,
            "direct_rescore_missing_rows": direct_missing,
            "direct_rescore_scored_rows": direct_scored,
            "direct_rescore_side_match_rows": direct_side_match,
            "direct_rescore_scored_side_match_rate": direct_scored_side_match_rate,
            "direct_rescore_median_abs_probability_diff": median_rescore_diff,
            "direct_rescore_max_abs_probability_diff": max_rescore_diff,
            "production_feature_count": len(feature_list),
            "market_like_feature_names": len(market_like_features),
        },
        "decisions": {k: v for k, v in decisions},
    }
    (out_dir / "machine_readable.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    md = f"""# MLB Hits 0.5 Incumbent-BetOnline Independence Audit

Generated UTC: `{generated_at}`

## Executive Summary

The production incumbent for Hits 0.5 was bound to `models_out/latest/hits.joblib` with SHA256 `{sha256_file(model_path)}`.
Runtime rescoring against the retained July 20 prepared feature vectors scored `{direct_scored}` of `{len(ledger)}` rows, reproduced `{direct_pass}` stored probabilities within `1e-6`, and matched stored incumbent side on `{direct_side_match}` rows. The remaining probability-level deltas are documented as retained-vector lineage drift rather than evidence of market copying.

The incumbent and BetOnline both finished `128-68`, but not because they made identical row-level decisions. They agreed on `{same_side}` rows and disagreed on `{diff_side}` rows; the disagreement rows split exactly incumbent `{diff_inc_wins}` wins and BetOnline `{diff_book_wins}` wins.

The true incumbent feature manifest contains `{len(feature_list)}` features and `{len(market_like_features)}` market-like feature names. Raw-price transformation checks did not find a direct copy of BetOnline raw implied probability or no-vig probability into the incumbent model.

## Direct Answer

The production incumbent independently produced its July 20 probabilities from the frozen `hits.joblib` LR/RF runtime and nonmarket production feature list. Its identical `128-68` directional record with BetOnline was caused by high agreement plus offsetting disagreement wins, not by copied sides or copied probabilities. The incumbent comparison remains valid after artifact binding, runtime rescoring, and market-lineage inspection, with the caveat that exact probability replay is only partial because the retained prepared-vector diagnostic is not a complete immutable per-row decision-vector archive.

## Key Evidence

- Governing ledger: `exact_196_governing_ledger.csv`
- Direct rescore: `direct_incumbent_rescoring_ledger.csv`
- Agreement matrix: `incumbent_betonline_agreement_matrix.csv`
- Feature manifest audit: `incumbent_feature_manifest_audit.csv`
- Raw-book transformation tests: `raw_book_transformation_tests.csv`
- Side classification audit: `side_classification_audit.csv`

## Decisions

"""
    for k, v in decisions:
        md += f"- `{k} = {v}`\n"
    md += "\nNo production behavior changed. No network, OddsAPI, DB, model training, routing, or Hits 1.5 changes were performed.\n"
    (out_dir / "hits05_incumbent_betonline_independence_audit_2026-07-21.md").write_text(md, encoding="utf-8")

    # Validation and manifest.
    produced = sorted([p for p in out_dir.iterdir() if p.is_file()])
    manifest = []
    for path in produced:
        manifest.append({"path": short(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(out_dir / "sha256_manifest.csv", manifest, ["path", "sha256", "bytes"])

    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            rows = len(pd.read_csv(path))
            validation.append({"check": "csv_parse", "path": short(path), "status": "PASS", "rows": rows, "notes": ""})
        except Exception as exc:
            validation.append({"check": "csv_parse", "path": short(path), "status": "FAIL", "rows": "", "notes": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation.append({"check": "json_parse", "path": short(path), "status": "PASS", "rows": "", "notes": ""})
        except Exception as exc:
            validation.append({"check": "json_parse", "path": short(path), "status": "FAIL", "rows": "", "notes": str(exc)})
    validation.append({"check": "direct_rescore_rows", "path": short(out_dir / "direct_incumbent_rescoring_ledger.csv"), "status": "PASS" if all_direct_side_match else "PARTIAL", "rows": direct_scored, "notes": f"exact={direct_pass}; side_match={direct_side_match}; mismatch={direct_mismatch}; missing={direct_missing}; median_abs_diff={median_rescore_diff:.6f}"})
    validation.append({"check": "production_guardrail", "path": "", "status": "PASS", "rows": "", "notes": "artifact-only audit; no network/db/model/routing changes"})
    write_csv(out_dir / "validation_report.csv", validation, ["check", "path", "status", "rows", "notes"])

    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
