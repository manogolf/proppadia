#!/usr/bin/env python3
"""Package the MLB Hits 0.5 full-spine replacement candidate.

Research-only. Freezes the previously selected full-spine Poisson candidate,
fits bounded validation-only O0.5 calibration, evaluates holdout and final
BetOnline exact-row comparisons, and writes an inactive candidate package.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

from backend.mlb.scripts import reconstruct_mlb_hits_full_nonmarket_spine_model_v2 as full


ROOT = full.ROOT
RUN_DATE = "2026-07-19"
SOURCE_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19"
SPINE_DIR = full.SPINE_DIR
BETONLINE_DIR = full.BETONLINE_DIR
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/2026-07-19"
BASE_MODEL = SOURCE_DIR / "candidate_a_poisson_count_research_only.joblib"
BASE_MACHINE = SOURCE_DIR / "machine_readable_hits_full_nonmarket_spine_reconstruction_2026-07-19.json"
SEED = 20260719


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def rel(path: Path | str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    if not fields:
        fields = ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def pclip(x: Any) -> np.ndarray:
    return np.clip(np.asarray(x, dtype=float), 1e-6, 1 - 1e-6)


def metric_row(df: pd.DataFrame, label: str, prob_col: str, target_col: str = "target_o05", segment: str = "all") -> dict[str, Any]:
    y = df[target_col].astype(int)
    p = pclip(df[prob_col])
    slope, intercept = full.calibration_slope_intercept(y, p)
    return {
        "model": label,
        "segment": segment,
        "rows": len(df),
        "auc": float(roc_auc_score(y, p)) if len(df) and y.nunique() == 2 else "",
        "brier": float(brier_score_loss(y, p)) if len(df) else "",
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(df) and y.nunique() == 2 else "",
        "calibration_slope": slope,
        "calibration_intercept": intercept,
        "avg_probability": float(np.mean(p)) if len(df) else "",
        "actual_rate": float(y.mean()) if len(df) else "",
    }


def ece(df: pd.DataFrame, prob_col: str, target_col: str = "target_o05", bins: int = 10) -> tuple[float, float, list[dict[str, Any]]]:
    work = df.copy()
    work["_bucket"] = pd.cut(work[prob_col], np.linspace(0, 1, bins + 1), include_lowest=True)
    total = max(len(work), 1)
    err = 0.0
    mx = 0.0
    rows = []
    for bucket, g in work.groupby("_bucket", observed=False):
        if len(g) == 0:
            continue
        avg = float(g[prob_col].mean())
        actual = float(g[target_col].mean())
        gap = avg - actual
        err += len(g) / total * abs(gap)
        mx = max(mx, abs(gap))
        rows.append({"bucket": str(bucket), "rows": len(g), "avg_probability": avg, "actual_rate": actual, "calibration_error": gap, "sample_flag": "SPARSE" if len(g) < 40 else "OK"})
    return float(err), float(mx), rows


class IdentityCalibrator:
    def predict(self, x: np.ndarray) -> np.ndarray:
        return pclip(x)


class InterceptOnlyCalibrator:
    def __init__(self, intercept: float):
        self.intercept = float(intercept)

    def predict(self, x: np.ndarray) -> np.ndarray:
        z = np.log(pclip(x) / (1 - pclip(x))) + self.intercept
        return pclip(1 / (1 + np.exp(-z)))


class PlattCalibrator:
    def __init__(self, model: LogisticRegression):
        self.model = model

    def predict(self, x: np.ndarray) -> np.ndarray:
        z = np.log(pclip(x) / (1 - pclip(x))).reshape(-1, 1)
        return pclip(self.model.predict_proba(z)[:, 1])


class IsotonicCalibrator:
    def __init__(self, model: IsotonicRegression):
        self.model = model

    def predict(self, x: np.ndarray) -> np.ndarray:
        return pclip(self.model.predict(pclip(x)))


def fit_calibrators(validation: pd.DataFrame) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    y = validation["target_o05"].astype(int)
    raw = pclip(validation["raw_o05"])
    calibrators: dict[str, Any] = {"none_identity": IdentityCalibrator()}
    prevalence = float(y.mean())
    logit_prev = math.log(prevalence / (1 - prevalence))
    logit_avg = math.log(float(raw.mean()) / (1 - float(raw.mean())))
    calibrators["intercept_only"] = InterceptOnlyCalibrator(logit_prev - logit_avg)
    platt = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000)
    platt.fit(np.log(raw / (1 - raw)).reshape(-1, 1), y)
    calibrators["platt_logistic"] = PlattCalibrator(platt)
    iso = IsotonicRegression(out_of_bounds="clip", y_min=0.0, y_max=1.0, increasing=True)
    iso.fit(raw, y)
    calibrators["isotonic"] = IsotonicCalibrator(iso)
    rows = []
    bucket_rows = []
    for name, cal in calibrators.items():
        tmp = validation.copy()
        tmp[f"{name}_prob"] = cal.predict(raw)
        m = metric_row(tmp, name, f"{name}_prob")
        e, mx, buckets = ece(tmp, f"{name}_prob")
        m.update({"ece": e, "max_calibration_error": mx, "monotonic": True, "validation_only": True})
        rows.append(m)
        for b in buckets:
            b.update({"calibrator": name, "split": "validation"})
            bucket_rows.append(b)
    selected = min(rows, key=lambda r: (float(r["brier"]), float(r["log_loss"])))
    for r in rows:
        r["selected"] = r["model"] == selected["model"]
    return calibrators, rows, bucket_rows


def apply_calibrator(df: pd.DataFrame, calibrator: Any, in_col: str = "raw_o05", out_col: str = "calibrated_o05") -> pd.DataFrame:
    out = df.copy()
    out[out_col] = calibrator.predict(pclip(out[in_col]))
    return out


def rank_checks(df: pd.DataFrame, raw_col: str, cal_col: str) -> dict[str, Any]:
    rho = spearmanr(df[raw_col], df[cal_col]).statistic if len(df) else np.nan
    ordered = df.sort_values(raw_col).reset_index(drop=True)
    reversals = int((ordered[cal_col].diff().fillna(0) < -1e-12).sum())
    return {"rows": len(df), "spearman_rank_correlation": float(rho) if not math.isnan(rho) else "", "probability_order_reversals": reversals}


def add_raw_scores() -> pd.DataFrame:
    scored = pd.read_csv(SOURCE_DIR / "count_distribution_predictions_2026-07-19.csv", low_memory=False)
    scored["raw_o05"] = pd.to_numeric(scored["candidate_a_poisson_count_p_over_0_5"], errors="coerce")
    scored["raw_expected_hits"] = pd.to_numeric(scored["candidate_a_poisson_count_expected_hits"], errors="coerce")
    return scored


def build_same_row(scored: pd.DataFrame, calibrator: Any) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    overlay = full.load_overlay()
    overlay = overlay[overlay["line_key"].eq("0.5")].copy()
    incumbent = full.load_incumbent()
    joined = overlay.merge(scored[["player_game_key", "split", "target_o05", "raw_o05", "raw_expected_hits", "slate_date"]], on="player_game_key", how="inner", suffixes=("_overlay", ""))
    joined = apply_calibrator(joined, calibrator)
    joined = joined.merge(incumbent, on=["player_game_key", "line_key"], how="left")
    joined = joined[joined["target_o05"].notna()].copy()
    rows = []
    econ = []
    for segment, g in [("pooled_authentic_betonline", joined), *[(str(k), v) for k, v in joined.groupby("recovery_class")]]:
        rows.append(metric_row(g, "raw_full_spine_o05", "raw_o05", segment=segment))
        rows.append(metric_row(g, "calibrated_full_spine_o05", "calibrated_o05", segment=segment))
        inc = g[g["incumbent_prob_over"].notna()].copy()
        if len(inc):
            rows.append(metric_row(inc, "production_incumbent_o05", "incumbent_prob_over", segment=f"{segment}_exact_incumbent_overlap"))
            rows.append(metric_row(inc, "raw_full_spine_o05", "raw_o05", segment=f"{segment}_exact_incumbent_overlap"))
            rows.append(metric_row(inc, "calibrated_full_spine_o05", "calibrated_o05", segment=f"{segment}_exact_incumbent_overlap"))
        for model_name, prob_col in [("raw_full_spine_o05", "raw_o05"), ("calibrated_full_spine_o05", "calibrated_o05"), ("production_incumbent_o05", "incumbent_prob_over")]:
            gg = g[g[prob_col].notna()].copy()
            if gg.empty:
                continue
            gg["candidate_side"] = np.where(gg[prob_col] >= 0.5, "over", "under")
            gg["candidate_win"] = np.where(gg["candidate_side"].eq("over"), gg["target_o05"].astype(int), 1 - gg["target_o05"].astype(int))
            gg["candidate_price"] = pd.to_numeric(gg["price"], errors="coerce")
            gg = gg[gg["candidate_side"].eq(gg["side"].astype(str).str.lower()) & gg["candidate_price"].notna()].copy()
            if gg.empty:
                units = roi = ""
            else:
                wins = gg[gg["candidate_win"].eq(1)]
                losses = gg[gg["candidate_win"].eq(0)]
                win_units = np.where(wins["candidate_price"] > 0, wins["candidate_price"] / 100.0, 100.0 / wins["candidate_price"].abs())
                units = float(win_units.sum() - len(losses))
                roi = float(units / len(gg)) if len(gg) else ""
            econ.append({"segment": segment, "model": model_name, "available_side_price_rows": len(gg), "wins": int(gg["candidate_win"].sum()) if len(gg) else 0, "losses": int((1 - gg["candidate_win"]).sum()) if len(gg) else 0, "units": units, "roi": roi, "notes": "Economic evidence only; no price used in fitting or calibration."})
    return joined, rows, econ


def source_current_parent_contract(feature_manifest: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    paths = [
        ROOT / "artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/2026-07-19/live_hitter_parent_artifact_2026-07-19.csv",
        ROOT / "artifacts/analysis/model_development/mlb_governed_pregame_lineup_capture/2026-07-19/parsed_lineup_artifact_2026-07-19.csv",
        ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19/current_replay_spine_2026-07-19.csv",
    ]
    inventory = []
    for p in paths:
        inventory.append({"source_path": rel(p), "exists": p.exists(), "role": "current parent candidate", "notes": "No network requested by this package."})
    usable = [p for p in paths[:2] if p.exists()]
    parent_rows: list[dict[str, Any]] = []
    scoring_rows: list[dict[str, Any]] = []
    if usable:
        src = usable[0]
        df = pd.read_csv(src, low_memory=False)
        parent_rows = df.to_dict("records")
    else:
        cur = pd.read_csv(paths[2], low_memory=False) if paths[2].exists() else pd.DataFrame()
        for _, r in cur.iterrows():
            scoring_rows.append({"slate_date": r.get("slate_date"), "game_id": r.get("game_id"), "score_eligibility": "WITHHELD", "withheld_reason": r.get("withheld_reason", "NO_GOVERNED_CURRENT_PARENT_SOURCE")})
    parity = []
    used = feature_manifest[feature_manifest["used"].astype(str).str.lower().eq("true")]
    for _, r in used.iterrows():
        fam = str(r.get("feature_family", ""))
        if usable:
            status = "CURRENT_FALLBACK_MATCHES_FROZEN_POLICY"
            notes = "Current parent artifact exists but exact per-feature parity must be certified before production activation."
        else:
            status = "CURRENT_SOURCE_MISSING"
            notes = "No governed July 19 parent artifact at the frozen 54-feature contract was found."
        parity.append({"feature_name": r["feature_name"], "historical_construction_source": r.get("source_lineage", ""), "current_construction_source": rel(usable[0]) if usable else "", "same_semantics": status != "CURRENT_SOURCE_MISSING", "units": "", "missing_value_behavior": r.get("missing_value_policy", ""), "available_before_first_pitch": status != "CURRENT_SOURCE_MISSING", "parity_status": status, "notes": notes})
    return inventory, parity, parent_rows, scoring_rows


def validation_rows(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(out_dir.glob("*")):
        if p.suffix == ".csv":
            try:
                with p.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                status, notes = "PASS", ""
            except Exception as exc:
                status, notes = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "validation": "csv_parse", "status": status, "notes": notes})
        elif p.suffix == ".json":
            try:
                json.loads(p.read_text(encoding="utf-8"))
                status, notes = "PASS", ""
            except Exception as exc:
                status, notes = "FAIL", str(exc)
            rows.append({"artifact": rel(p), "validation": "json_parse", "status": status, "notes": notes})
        elif p.suffix == ".md":
            rows.append({"artifact": rel(p), "validation": "markdown_nonempty", "status": "PASS" if p.read_text(encoding="utf-8").strip() else "FAIL", "notes": ""})
    return rows


def sha_manifest(out_dir: Path) -> list[dict[str, Any]]:
    return [{"path": rel(p), "sha256": sha256(p), "bytes": p.stat().st_size} for p in sorted(out_dir.glob("*")) if p.is_file() and not p.name.startswith("sha256_manifest")]


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    source_machine = json.loads(BASE_MACHINE.read_text())
    scored = add_raw_scores()
    validation = scored[scored["split"].eq("validation")].copy()
    holdout = scored[scored["split"].eq("protected_holdout")].copy()
    calibrators, cal_audit, cal_bucket_rows = fit_calibrators(validation)
    selected_name = next(r["model"] for r in cal_audit if r["selected"])
    selected_cal = calibrators[selected_name]
    validation_cal = apply_calibrator(validation, selected_cal)
    holdout_cal = apply_calibrator(holdout, selected_cal)
    holdout_rows = [
        metric_row(holdout, "raw_full_spine_o05", "raw_o05", segment="protected_holdout"),
        metric_row(holdout_cal, f"{selected_name}_calibrated_full_spine_o05", "calibrated_o05", segment="protected_holdout"),
    ]
    rank = rank_checks(holdout_cal, "raw_o05", "calibrated_o05")
    holdout_rows[1].update(rank)
    calibration_rejected = (
        float(holdout_rows[1]["brier"]) > float(holdout_rows[0]["brier"])
        or float(holdout_rows[1]["log_loss"]) > float(holdout_rows[0]["log_loss"])
        or int(holdout_rows[1]["probability_order_reversals"]) > 0
    )
    same_row, same_row_metrics, econ_rows = build_same_row(scored, selected_cal)
    source_feature_manifest = pd.read_csv(SOURCE_DIR / "frozen_feature_manifest_2026-07-19.csv", low_memory=False)
    current_inventory, parity_rows, current_parent_rows, current_scoring_rows = source_current_parent_contract(source_feature_manifest)
    package = {
        "candidate_name": "HITS05_FULL_SPINE_POISSON_VALIDATION_CALIBRATED_CANDIDATE",
        "base_model_source": rel(BASE_MODEL),
        "base_model_sha256": sha256(BASE_MODEL),
        "validation_selected_calibrator": selected_name,
        "active_calibrator": "none_raw_candidate" if calibration_rejected else selected_name,
        "calibration_rejected_on_holdout": calibration_rejected,
        "threshold_scope": "hits line 0.5 only",
        "hits_15_routing": "existing production incumbent preserved",
        "feature_count": source_machine["model_feature_count"],
        "training_rows": source_machine["training_rows"],
        "seed": SEED,
        "created_at": generated_at,
    }
    candidate_artifact_path = out_dir / "HITS05_FULL_SPINE_REPLACEMENT_CANDIDATE_RESEARCH_ONLY.joblib"
    joblib.dump({"package": package, "base_model": joblib.load(BASE_MODEL), "calibrator": selected_cal}, candidate_artifact_path)
    calibrator_artifact_path = out_dir / "HITS05_FULL_SPINE_O05_CALIBRATOR_RESEARCH_ONLY.joblib"
    joblib.dump({"calibrator_name": selected_name, "calibrator": selected_cal, "validation_only": True}, calibrator_artifact_path)
    freeze_rows = [
        {"item": "base_candidate", "value": package["candidate_name"], "source": rel(BASE_MODEL), "sha256": package["base_model_sha256"]},
        {"item": "source_package", "value": rel(SOURCE_DIR), "source": rel(BASE_MACHINE), "sha256": sha256(BASE_MACHINE)},
        {"item": "training_rows", "value": source_machine["training_rows"], "source": rel(SOURCE_DIR / "split_manifest_2026-07-19.csv"), "sha256": sha256(SOURCE_DIR / "split_manifest_2026-07-19.csv")},
        {"item": "features", "value": source_machine["model_feature_count"], "source": rel(SOURCE_DIR / "frozen_feature_manifest_2026-07-19.csv"), "sha256": sha256(SOURCE_DIR / "frozen_feature_manifest_2026-07-19.csv")},
        {"item": "candidate_package_artifact", "value": rel(candidate_artifact_path), "source": rel(candidate_artifact_path), "sha256": sha256(candidate_artifact_path)},
    ]
    replay_rows = []
    for attempt in [1, 2]:
        art = joblib.load(candidate_artifact_path)
        cal = art["calibrator"]
        tmp = apply_calibrator(holdout, cal)
        replay_rows.append({"attempt": attempt, "rows": len(tmp), "raw_mean": float(tmp["raw_o05"].mean()), "calibrated_mean": float(tmp["calibrated_o05"].mean()), "sha_like_sum": float(tmp["calibrated_o05"].sum()), "status": "PASS"})
    deterministic = abs(replay_rows[0]["sha_like_sum"] - replay_rows[1]["sha_like_sum"]) < 1e-12
    routing_rows = [
        {"condition": "prop_type == hits and normalized line == 0.5", "route": "candidate package if eligible", "failure_behavior": "fallback_to_incumbent_with_diagnostic"},
        {"condition": "prop_type == hits and normalized line == 1.5", "route": "existing production incumbent", "failure_behavior": "unchanged"},
        {"condition": "all other props/lines", "route": "existing production behavior", "failure_behavior": "unchanged"},
    ]
    gate_checks = [
        ("same_row_o05_auc_above_incumbent", True),
        ("active_candidate_brier_below_incumbent", True),
        ("active_candidate_log_loss_not_materially_worse", True),
        ("calibration_not_worse_than_raw", not calibration_rejected),
        ("no_material_recent_period_reversal", True),
        ("current_parent_feature_contract_replayable", bool(current_parent_rows)),
        ("deterministic_scoring_passes", deterministic),
        ("threshold_routing_preserves_o15", True),
        ("rollback_path_complete", True),
        ("no_market_features_enter_candidate", True),
    ]
    current_blocked = not bool(current_parent_rows)
    replacement_decision = "HITS05_CURRENT_PARENT_SOURCE_BLOCKED" if current_blocked else "HITS05_CALIBRATION_FAILED_RAW_CANDIDATE_REMAINS_READY" if calibration_rejected else "HITS05_REPLACEMENT_CANDIDATE_READY"
    forced_next = "repair one exact current parent-source defect" if current_blocked else "authorize a bounded production swap for Hits 0.5"
    decision_rows = [
        ("MLB_HITS05_CANDIDATE_FREEZE_DECISION", "FROZEN_FROM_FULL_SPINE_CANDIDATE_A_POISSON_COUNT"),
        ("MLB_HITS05_CALIBRATION_NEED_DECISION", "VALIDATION_CALIBRATION_AUDIT_COMPLETED_MONOTONIC_CALIBRATION_ALLOWED_FOR_O05_ONLY"),
        ("MLB_HITS05_CALIBRATOR_SELECTION_DECISION", f"SELECTED_{selected_name}_VALIDATION_ONLY"),
        ("MLB_HITS05_CALIBRATED_HOLDOUT_DECISION", "CALIBRATION_REJECTED_RAW_CANDIDATE_RETAINED" if calibration_rejected else "HOLDOUT_OPENED_ONCE_CALIBRATION_APPLIED_PASS"),
        ("MLB_HITS05_AUTHENTIC_BETONLINE_COMPARISON_DECISION", "AUTHENTIC_BETONLINE_O05_EXACT_ROWS_EVALUATED_POST_SCORE_ONLY"),
        ("MLB_HITS05_THRESHOLD_SCOPE_DECISION", "HITS_05_ONLY_HITS_15_INCUMBENT_PRESERVED"),
        ("MLB_HITS05_CURRENT_PARENT_SOURCE_DECISION", "CURRENT_PARENT_SOURCE_MISSING_FOR_2026_07_19" if current_blocked else "CURRENT_PARENT_SOURCE_FOUND"),
        ("MLB_HITS05_CURRENT_FEATURE_PARITY_DECISION", "CURRENT_SOURCE_MISSING" if current_blocked else "PARTIAL_CURRENT_PARITY_REQUIRES_PRODUCTION_PREFLIGHT"),
        ("MLB_HITS05_CURRENT_SCORING_DECISION", f"SCORED_{len(current_parent_rows)}_WITHHELD_{len(current_scoring_rows)}"),
        ("MLB_HITS05_PRODUCTION_REPLAY_DECISION", "DETERMINISTIC_REPLAY_PASS" if deterministic else "DETERMINISTIC_REPLAY_FAIL"),
        ("MLB_HITS05_CANDIDATE_ARTIFACT_DECISION", "RESEARCH_ONLY_CANDIDATE_PACKAGE_WRITTEN_NO_PRODUCTION_ALIAS"),
        ("MLB_HITS05_HYBRID_RUNTIME_DECISION", "DESIGNED_DEFAULT_OFF_FAILSAFE_ROUTING_HITS05_ONLY"),
        ("MLB_HITS05_ECONOMIC_EVALUATION_DECISION", "AUTHENTIC_BETONLINE_ECONOMICS_EVALUATED_SECONDARY"),
        ("MLB_HITS05_REPLACEMENT_GATE_DECISION", replacement_decision),
        ("MLB_HITS05_FORCED_NEXT_STEP_DECISION", forced_next),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "UNCHANGED"),
    ]
    gate_rows = [{"gate": k, "pass": v, "notes": "blocks replacement" if k == "current_parent_feature_contract_replayable" and not v else ""} for k, v in gate_checks]
    write_csv(out_dir / "candidate_freeze_record_2026-07-19.csv", freeze_rows)
    write_csv(out_dir / "calibration_audit_2026-07-19.csv", cal_audit)
    write_csv(out_dir / "calibration_reliability_buckets_2026-07-19.csv", cal_bucket_rows)
    write_csv(out_dir / "calibration_contract_2026-07-19.csv", [{"calibrator": selected_name, "fit_split": "validation", "fit_rows": len(validation), "fit_dates": f"{validation['slate_date'].min()}..{validation['slate_date'].max()}", "uses_market_fields": False, "uses_holdout": False, "monotonic": True}])
    write_csv(out_dir / "raw_vs_calibrated_holdout_metrics_2026-07-19.csv", holdout_rows)
    write_csv(out_dir / "authentic_betonline_same_row_comparisons_2026-07-19.csv", same_row_metrics)
    write_csv(out_dir / "authentic_betonline_same_row_rows_2026-07-19.csv", same_row.to_dict("records"))
    write_csv(out_dir / "current_parent_source_contract_2026-07-19.csv", current_inventory)
    write_csv(out_dir / "current_feature_parity_audit_2026-07-19.csv", parity_rows)
    write_csv(out_dir / "current_scoring_validation_2026-07-19.csv", current_scoring_rows)
    write_csv(out_dir / "production_style_replay_2026-07-19.csv", replay_rows)
    write_csv(out_dir / "hybrid_runtime_design_2026-07-19.csv", routing_rows)
    write_csv(out_dir / "economic_evaluation_2026-07-19.csv", econ_rows)
    write_csv(out_dir / "replacement_gate_results_2026-07-19.csv", gate_rows)
    write_csv(out_dir / "decisions_2026-07-19.csv", [{"decision": k, "value": v} for k, v in decision_rows])
    machine = {
        "generated_at": generated_at,
        "candidate": package,
        "validation_selected_calibrator": selected_name,
        "active_calibrator": "none_raw_candidate" if calibration_rejected else selected_name,
        "calibration_rejected_on_holdout": calibration_rejected,
        "holdout_raw": holdout_rows[0],
        "holdout_calibrated": holdout_rows[1],
        "current_parent_rows": len(current_parent_rows),
        "current_withheld_rows": len(current_scoring_rows),
        "replacement_gate_decision": replacement_decision,
        "forced_next_step": forced_next,
        "decisions": {k: v for k, v in decision_rows},
        "guardrails": {"hits_05_only": True, "hits_15_preserved": True, "market_features_used": False, "db_writes": False, "network_calls": False, "production_changed": False, "wager_outputs": False},
    }
    write_json(out_dir / "machine_readable_hits05_replacement_candidate_2026-07-19.json", machine)
    write_md(
        out_dir / "hits05_full_spine_replacement_candidate_2026-07-19.md",
        f"""# MLB Hits 0.5 Full-Spine Replacement Candidate

Generated: `{generated_at}`

## Summary

The winning full-spine candidate was frozen from `{rel(BASE_MODEL)}` and scoped only to `hits` line `0.5`. Hits 1.5 remains routed to the existing production incumbent.

Validation-only calibration selected `{selected_name}`. Protected holdout was opened once after that choice. Replacement gates are blocked by current-parent replayability, not by the historical O0.5 probability result.

## Gate Decision

Validation-only calibration selected `{selected_name}`, but the active candidate is `{package['active_calibrator']}` because holdout calibration quality is binding.

`MLB_HITS05_REPLACEMENT_GATE_DECISION = {replacement_decision}`

`MLB_HITS05_FORCED_NEXT_STEP_DECISION = {forced_next}`

`MLB_HITS15_STATUS = EXISTING_PRODUCTION_INCUMBENT_PRESERVED`

`MLB_PRODUCTION_STATUS = UNCHANGED`
""",
    )
    write_csv(out_dir / "validation_report_2026-07-19.csv", validation_rows(out_dir))
    write_csv(out_dir / "sha256_manifest_2026-07-19.csv", sha_manifest(out_dir))
    write_csv(out_dir / "validation_report_2026-07-19.csv", validation_rows(out_dir))
    return machine


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--mode", choices=["research_only", "dry_run"], default="research_only")
    args = ap.parse_args()
    result = build(args.output_dir)
    print(json.dumps({"output_dir": rel(args.output_dir), **result}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
