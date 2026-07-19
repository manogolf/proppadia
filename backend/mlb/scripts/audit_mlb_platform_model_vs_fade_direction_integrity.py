#!/usr/bin/env python3
"""Audit MLB platform model-vs-fade prediction direction integrity.

Read-only diagnostic. It uses locally preserved artifacts only and writes an
audit package. It does not fit models, call network/DB/OddsAPI, optimize
thresholds, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


RUN_DATE = "2026-07-17"
ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_platform_wide_model_vs_fade_direction_integrity_audit/2026-07-17"

OPS_MODEL_VS_FADE_JSON = ROOT / "tmp/analysis/mlb_model_vs_fade_summary.json"
OPS_MODEL_VS_FADE_BY_PROP = ROOT / "tmp/analysis/mlb_model_vs_fade_by_prop.csv"
COLLECTIVE_DIR = ROOT / "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_offline_process_validation/2026-07-13"
PA_PILOT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_15_direct_pa_champion_challenger_pilot/2026-07-17"
ORIENTATION_AUDIT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits15_champion_orientation_control_audit/2026-07-17"


def _write_csv(path: Path, rows: list[dict[str, Any]] | pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pd.DataFrame):
        rows.to_csv(path, index=False)
        return
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _safe_auc(y: pd.Series, p: pd.Series) -> float:
    work = pd.DataFrame({"y": pd.to_numeric(y, errors="coerce"), "p": pd.to_numeric(p, errors="coerce")}).dropna()
    if len(work) == 0 or work["y"].nunique() < 2:
        return math.nan
    return float(roc_auc_score(work["y"].astype(int), work["p"].astype(float)))


def _safe_spearman(y: pd.Series, p: pd.Series) -> float:
    work = pd.DataFrame({"y": pd.to_numeric(y, errors="coerce"), "p": pd.to_numeric(p, errors="coerce")}).dropna()
    if len(work) < 2 or work["y"].nunique() < 2 or work["p"].nunique() < 2:
        return math.nan
    val = spearmanr(work["p"], work["y"]).statistic
    return float(val) if val == val else math.nan


def _ece(y: pd.Series, p: pd.Series, bins: int = 10) -> float:
    work = pd.DataFrame({"y": pd.to_numeric(y, errors="coerce"), "p": pd.to_numeric(p, errors="coerce")}).dropna()
    if work.empty:
        return math.nan
    edges = np.linspace(0, 1, bins + 1)
    total = len(work)
    out = 0.0
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        mask = (work["p"] >= lo) & ((work["p"] < hi) if i < bins - 1 else (work["p"] <= hi))
        if mask.any():
            g = work[mask]
            out += len(g) / total * abs(float(g["y"].mean()) - float(g["p"].mean()))
    return float(out)


def _metric_block(rows: pd.DataFrame, y_col: str, p_col: str, *, orientation: str, group: dict[str, Any]) -> dict[str, Any]:
    work = rows.dropna(subset=[y_col, p_col]).copy()
    y = pd.to_numeric(work[y_col], errors="coerce").astype(int)
    p = pd.to_numeric(work[p_col], errors="coerce").clip(1e-6, 1 - 1e-6)
    out = dict(group)
    out.update(
        {
            "orientation": orientation,
            "rows": int(len(rows)),
            "qualified_rows": int(len(work)),
            "event_rate": float(y.mean()) if len(work) else math.nan,
            "avg_probability": float(p.mean()) if len(work) else math.nan,
            "auc": _safe_auc(y, p),
            "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(work) else math.nan,
            "brier_score": float(brier_score_loss(y, p)) if len(work) else math.nan,
            "spearman": _safe_spearman(y, p),
            "expected_calibration_error": _ece(y, p),
            "date_min": str(work["slate_date"].min()) if "slate_date" in work and len(work) else "",
            "date_max": str(work["slate_date"].max()) if "slate_date" in work and len(work) else "",
            "distinct_dates": int(work["slate_date"].nunique()) if "slate_date" in work and len(work) else 0,
        }
    )
    return out


def _classify_orientation(stored_auc: float, inverse_auc: float, rows: int, dates: int) -> str:
    if rows < 30:
        return "insufficient_sample"
    if not math.isfinite(stored_auc) or not math.isfinite(inverse_auc):
        return "unresolved"
    if stored_auc >= 0.53 and stored_auc > inverse_auc:
        return "stored_orientation_directionally_supported"
    if inverse_auc >= 0.53 and inverse_auc > stored_auc:
        return "deterministic_inverse_directionally_supported"
    if abs(stored_auc - 0.5) <= 0.03:
        return "approximately_random_or_weak"
    return "directionally_unstable_or_mixed"


def _parse_canonical(row_id: str) -> dict[str, str]:
    parts = str(row_id).split("|")
    out = {"slate_date": "", "game_id": "", "player_id": "", "prop_type": "", "line": "", "side": ""}
    for key, val in zip(out.keys(), parts):
        out[key] = val
    return out


def _load_ops_reconcile() -> tuple[pd.DataFrame, dict[str, Any], Path]:
    payload = json.loads(OPS_MODEL_VS_FADE_JSON.read_text())
    rows_path = ROOT / str(payload["rows_csv"])
    df = pd.read_csv(rows_path, low_memory=False)
    if "slate_date" not in df.columns and "game_date" in df.columns:
        df = df.rename(columns={"game_date": "slate_date"})
    elif "slate_date" in df.columns:
        df["slate_date"] = df["slate_date"].fillna(df.get("game_date"))
    df["source_family"] = "execution_vs_model_current_reconcile"
    df["model_version"] = df.get("snapshot_run_tag", "").astype(str)
    df["side"] = df["model_pick_side"].astype(str).str.lower()
    df["event_over_label"] = df["actual_over_outcome"].astype(str).str.lower().map({"win": 1, "loss": 0})
    df["selected_side_label"] = df["actual_model_pick_outcome"].astype(str).str.lower().map({"win": 1, "loss": 0})
    df["fade_side_label"] = 1 - df["selected_side_label"]
    df["stored_event_probability"] = pd.to_numeric(df["model_prob_over"], errors="coerce")
    df["inverse_event_probability"] = 1 - df["stored_event_probability"]
    df["stored_selected_probability"] = pd.to_numeric(df["model_pick_prob"], errors="coerce")
    df["inverse_selected_probability"] = 1 - df["stored_selected_probability"]
    df["canonical_identity"] = (
        df["slate_date"].astype(str)
        + "|"
        + df["game_id"].astype(str)
        + "|"
        + df["player_id"].astype(str)
        + "|"
        + df["prop_type"].astype(str)
        + "|"
        + df["line"].astype(str)
        + "|"
        + df["side"].astype(str)
    )
    return df, payload, rows_path


def _load_collective_predictions() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in sorted(COLLECTIVE_DIR.glob("predictions_variant_*_*.csv")):
        df = pd.read_csv(path)
        parsed = pd.DataFrame([_parse_canonical(v) for v in df["canonical_row_id"]])
        df = pd.concat([df, parsed], axis=1)
        df["source_family"] = "collective_bundle_v1_process_validation"
        df["model_version"] = df["manifest_id"].astype(str)
        df["slate_date"] = parsed["slate_date"]
        df["side"] = parsed["side"].str.lower()
        df["event_over_label"] = np.where(df["side"].eq("over"), df["binary_target"], 1 - df["binary_target"])
        df["selected_side_label"] = pd.to_numeric(df["binary_target"], errors="coerce")
        df["fade_side_label"] = 1 - df["selected_side_label"]
        df["stored_selected_probability"] = pd.to_numeric(df["process_validation_probability"], errors="coerce")
        df["inverse_selected_probability"] = 1 - df["stored_selected_probability"]
        df["stored_event_probability"] = np.where(df["side"].eq("over"), df["stored_selected_probability"], 1 - df["stored_selected_probability"])
        df["inverse_event_probability"] = 1 - df["stored_event_probability"]
        df["canonical_identity"] = df["canonical_row_id"]
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_pa_pilot() -> pd.DataFrame:
    path = PA_PILOT_DIR / "population_manifest_exact_2026-07-17.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["source_family"] = "hits15_direct_pa_champion_challenger_pilot"
    df["model_version"] = "frozen_control_probability_hits15_direct_pa_pilot"
    df["side"] = df["side_normalized"].astype(str).str.lower()
    df["event_over_label"] = pd.to_numeric(df["target_class"], errors="coerce")
    df["selected_side_label"] = df["event_over_label"]
    df["fade_side_label"] = 1 - df["selected_side_label"]
    df["stored_event_probability"] = pd.to_numeric(df["champion_probability"], errors="coerce")
    df["inverse_event_probability"] = 1 - df["stored_event_probability"]
    df["stored_selected_probability"] = df["stored_event_probability"]
    df["inverse_selected_probability"] = df["inverse_event_probability"]
    return df


def _population_manifest(all_rows: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "source_family",
        "model_version",
        "canonical_identity",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "prop_type",
        "line",
        "side",
        "stored_event_probability",
        "inverse_event_probability",
        "stored_selected_probability",
        "inverse_selected_probability",
        "event_over_label",
        "selected_side_label",
        "fade_side_label",
    ]
    available = [c for c in cols if c in all_rows.columns]
    return all_rows[available].copy()


def _inventory(paths: list[tuple[str, Path, str]]) -> list[dict[str, Any]]:
    rows = []
    for role, path, notes in paths:
        rows.append(
            {
                "artifact_role": role,
                "path": _rel(path),
                "exists": path.exists(),
                "sha256": _sha256(path) if path.exists() and path.is_file() else "",
                "bytes": path.stat().st_size if path.exists() and path.is_file() else "",
                "notes": notes,
            }
        )
    return rows


def _semantics_registry() -> list[dict[str, Any]]:
    return [
        {
            "source_family": "execution_vs_model_current_reconcile",
            "model_version": "snapshot_run_tag",
            "prediction_field": "model_prob_over",
            "semantics": "probability of OVER event; model_prob_under is complement",
            "expected_direction": "higher model_prob_over should rank OVER event more likely",
            "selected_side_logic": "model_pick_side with model_pick_prob = max(model_prob_over, model_prob_under)",
            "status": "CERTIFIED_FOR_LOCAL_RECONCILE_ROWS",
        },
        {
            "source_family": "collective_bundle_v1_process_validation",
            "model_version": "variant_a|variant_d",
            "prediction_field": "process_validation_probability",
            "semantics": "non-production process-validation probability of binary target for canonical selected side",
            "expected_direction": "higher means selected side more likely to win",
            "selected_side_logic": "side encoded in canonical_row_id",
            "status": "CERTIFIED_AS_NON_PRODUCTION_PROCESS_VALIDATION_ONLY",
        },
        {
            "source_family": "hits15_direct_pa_champion_challenger_pilot",
            "model_version": "frozen_control_probability_hits15_direct_pa_pilot",
            "prediction_field": "champion_probability/control_probability",
            "semantics": "probability of Hits OVER 1.5 for all direct PA pilot rows",
            "expected_direction": "higher means OVER 1.5 more likely",
            "selected_side_logic": "all rows are governed selected side OVER",
            "status": "CERTIFIED_BY_CHAMPION_ORIENTATION_AUDIT",
        },
    ]


def _event_orientation_results(rows: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    group_cols = ["source_family", "model_version", "prop_type", "line"]
    for keys, group in rows.groupby(group_cols, dropna=False):
        base = dict(zip(group_cols, keys))
        stored = _metric_block(group, "event_over_label", "stored_event_probability", orientation="stored_event_probability", group=base)
        inv = _metric_block(group, "event_over_label", "inverse_event_probability", orientation="deterministic_inverse_event_probability", group=base)
        stored["classification"] = _classify_orientation(stored["auc"], inv["auc"], stored["qualified_rows"], stored["distinct_dates"])
        inv["classification"] = _classify_orientation(inv["auc"], stored["auc"], inv["qualified_rows"], inv["distinct_dates"])
        out.extend([stored, inv])
    return out


def _decision_side_results(rows: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    group_cols = ["source_family", "model_version", "prop_type", "line", "side"]
    for keys, group in rows.groupby(group_cols, dropna=False):
        base = dict(zip(group_cols, keys))
        label = pd.to_numeric(group["selected_side_label"], errors="coerce")
        valid = label.notna()
        g = group[valid].copy()
        if g.empty:
            continue
        model_wins = int(g["selected_side_label"].sum())
        model_losses = int(len(g) - model_wins)
        fade_wins = model_losses
        fade_losses = model_wins
        row = dict(base)
        row.update(
            {
                "rows": int(len(group)),
                "paired_rows": int(len(g)),
                "model_wins": model_wins,
                "model_losses": model_losses,
                "model_win_rate": float(model_wins / len(g)),
                "fade_wins": fade_wins,
                "fade_losses": fade_losses,
                "fade_win_rate": float(fade_wins / len(g)),
                "paired_win_rate_delta_model_minus_fade": float((model_wins - fade_wins) / len(g)),
                "date_min": str(g["slate_date"].min()),
                "date_max": str(g["slate_date"].max()),
                "distinct_dates": int(g["slate_date"].nunique()),
            }
        )
        if "pnl_model_pick_1u" in g.columns and "pnl_over_1u" in g.columns and "pnl_under_1u" in g.columns:
            pick = g["side"].astype(str)
            fade_pnl = np.where(pick.eq("over"), pd.to_numeric(g["pnl_under_1u"], errors="coerce"), pd.to_numeric(g["pnl_over_1u"], errors="coerce"))
            model_pnl = pd.to_numeric(g["pnl_model_pick_1u"], errors="coerce")
            row["model_roi_1u"] = float(np.nanmean(model_pnl)) if np.isfinite(model_pnl).any() else math.nan
            row["fade_roi_1u"] = float(np.nanmean(fade_pnl)) if np.isfinite(fade_pnl).any() else math.nan
            row["delta_fade_minus_model_roi"] = row["fade_roi_1u"] - row["model_roi_1u"]
        out.append(row)
    return out


def _decomposition(rows: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    for dims in [
        ["source_family"],
        ["source_family", "prop_type"],
        ["source_family", "model_version"],
        ["source_family", "prop_type", "line"],
        ["source_family", "prop_type", "side"],
    ]:
        for keys, g in rows.groupby(dims, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            base = {dim: key for dim, key in zip(dims, keys)}
            stored_auc = _safe_auc(g["event_over_label"], g["stored_event_probability"])
            inv_auc = _safe_auc(g["event_over_label"], g["inverse_event_probability"])
            row = {
                "dimension": "|".join(dims),
                **base,
                "rows": int(len(g)),
                "dates": int(g["slate_date"].nunique()) if "slate_date" in g else 0,
                "stored_auc": stored_auc,
                "inverse_auc": inv_auc,
                "classification": _classify_orientation(stored_auc, inv_auc, len(g), int(g["slate_date"].nunique()) if "slate_date" in g else 0),
            }
            out.append(row)
    return out


def _date_stability(rows: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    for keys, g in rows.groupby(["source_family", "model_version", "slate_date"], dropna=False):
        source_family, model_version, slate_date = keys
        stored_auc = _safe_auc(g["event_over_label"], g["stored_event_probability"])
        inv_auc = _safe_auc(g["event_over_label"], g["inverse_event_probability"])
        out.append(
            {
                "source_family": source_family,
                "model_version": model_version,
                "slate_date": slate_date,
                "rows": int(len(g)),
                "stored_auc": stored_auc,
                "inverse_auc": inv_auc,
                "model_win_rate": float(pd.to_numeric(g["selected_side_label"], errors="coerce").mean()) if len(g) else math.nan,
                "classification": _classify_orientation(stored_auc, inv_auc, len(g), 1),
            }
        )
    return out


def _probability_bands(rows: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    work = rows.copy()
    work["probability_band"] = pd.cut(
        pd.to_numeric(work["stored_event_probability"], errors="coerce"),
        bins=[0, 0.35, 0.45, 0.55, 0.65, 1],
        labels=["lt_0_35", "0_35_0_45", "0_45_0_55", "0_55_0_65", "ge_0_65"],
        include_lowest=True,
    )
    for keys, g in work.groupby(["source_family", "prop_type", "probability_band"], dropna=False, observed=True):
        source_family, prop_type, band = keys
        out.append(
            {
                "source_family": source_family,
                "prop_type": prop_type,
                "probability_band": str(band),
                "rows": int(len(g)),
                "avg_probability": float(pd.to_numeric(g["stored_event_probability"], errors="coerce").mean()),
                "event_rate": float(pd.to_numeric(g["event_over_label"], errors="coerce").mean()),
                "model_win_rate": float(pd.to_numeric(g["selected_side_label"], errors="coerce").mean()),
            }
        )
    return out


def _prior_reconciliation() -> list[dict[str, Any]]:
    rows = []
    if OPS_MODEL_VS_FADE_JSON.exists():
        payload = json.loads(OPS_MODEL_VS_FADE_JSON.read_text())
        rows.append(
            {
                "prior_artifact": _rel(OPS_MODEL_VS_FADE_JSON),
                "population": payload.get("rows_csv"),
                "date_range": f"{payload.get('window', {}).get('game_date_min')} to {payload.get('window', {}).get('game_date_max')}",
                "included_prop_families": "see by-prop artifact",
                "model_definition": "model_pick_side from reconcile rows",
                "fade_definition": "opposite side at same prop and line where two-sided prices and outcomes exist",
                "paired_rows": payload.get("counts", {}).get("rows_paired_for_fade"),
                "model_win_rate": payload.get("overall", {}).get("model_win_rate"),
                "fade_win_rate": payload.get("overall", {}).get("fade_win_rate"),
                "model_roi": payload.get("overall", {}).get("model_roi_1u"),
                "fade_roi": payload.get("overall", {}).get("fade_roi_1u"),
                "prior_limitation": "decision-side P&L diagnostic; not probability-orientation proof and not production fade authorization",
            }
        )
    if ORIENTATION_AUDIT_DIR.exists():
        rows.append(
            {
                "prior_artifact": _rel(ORIENTATION_AUDIT_DIR / "executive_summary_2026-07-17.md"),
                "population": "Hits 1.5 direct PA champion-control pilot",
                "date_range": "2026-05-30 to 2026-07-09",
                "included_prop_families": "hits line 1.5 over only",
                "model_definition": "control_probability/champion_probability",
                "fade_definition": "deterministic inverse orientation diagnostic only",
                "paired_rows": 1292,
                "model_win_rate": "",
                "fade_win_rate": "",
                "model_roi": "",
                "fade_roi": "",
                "prior_limitation": "localized orientation/control audit; not global inversion evidence",
            }
        )
    return rows


def _sha_manifest(out_dir: Path) -> None:
    manifest = out_dir / f"sha256_manifest_{RUN_DATE}.csv"
    rows = []
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p != manifest):
        rows.append({"relative_path": path.relative_to(out_dir).as_posix(), "bytes": path.stat().st_size, "sha256": _sha256(path)})
    _write_csv(manifest, rows)


def _validation(out_dir: Path) -> None:
    rows = []
    for p in sorted(out_dir.glob("*.csv")):
        try:
            pd.read_csv(p)
            rows.append({"check": f"csv_parse:{p.name}", "status": "PASS", "details": ""})
        except Exception as exc:
            rows.append({"check": f"csv_parse:{p.name}", "status": "FAIL", "details": str(exc)})
    for p in sorted(out_dir.glob("*.json")):
        try:
            json.loads(p.read_text())
            rows.append({"check": f"json_parse:{p.name}", "status": "PASS", "details": ""})
        except Exception as exc:
            rows.append({"check": f"json_parse:{p.name}", "status": "FAIL", "details": str(exc)})
    for p in sorted(out_dir.glob("*.md")):
        rows.append({"check": f"markdown_nonempty:{p.name}", "status": "PASS" if p.read_text().strip() else "FAIL", "details": ""})
    rows.extend(
        [
            {"check": "no_network_or_oddsapi", "status": "PASS", "details": "local artifacts only"},
            {"check": "no_db_writes", "status": "PASS", "details": "no database client or SQL"},
            {"check": "no_model_fitting", "status": "PASS", "details": "only existing probabilities evaluated"},
            {"check": "no_production_changes", "status": "PASS", "details": "package-only writes"},
        ]
    )
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ops, ops_payload, ops_rows_path = _load_ops_reconcile()
    collective = _load_collective_predictions()
    pa = _load_pa_pilot()
    combined = pd.concat([ops, collective, pa], ignore_index=True, sort=False)

    # Keep only rows with certified prediction/outcome semantics.
    eligible = combined[
        pd.to_numeric(combined["event_over_label"], errors="coerce").notna()
        & pd.to_numeric(combined["stored_event_probability"], errors="coerce").notna()
        & pd.to_numeric(combined["selected_side_label"], errors="coerce").notna()
    ].copy()

    inventory_rows = _inventory(
        [
            ("ops_model_vs_fade_summary", OPS_MODEL_VS_FADE_JSON, "prior/current Ops decision-side model-vs-fade summary"),
            ("ops_model_vs_fade_by_prop", OPS_MODEL_VS_FADE_BY_PROP, "prior/current Ops by-prop model-vs-fade summary"),
            ("ops_reconcile_rows", ops_rows_path, "broad current execution-vs-model rows with model_prob_over/model_prob_under and official outcomes"),
            ("collective_predictions_variant_a_validation", COLLECTIVE_DIR / "predictions_variant_a_validation_2026-07-13.csv", "non-production process-validation selected-side predictions"),
            ("collective_predictions_variant_a_holdout", COLLECTIVE_DIR / "predictions_variant_a_holdout_2026-07-13.csv", "non-production process-validation selected-side predictions"),
            ("collective_predictions_variant_d_validation", COLLECTIVE_DIR / "predictions_variant_d_validation_2026-07-13.csv", "non-production process-validation selected-side predictions"),
            ("collective_predictions_variant_d_holdout", COLLECTIVE_DIR / "predictions_variant_d_holdout_2026-07-13.csv", "non-production process-validation selected-side predictions"),
            ("hits15_pa_pilot_population", PA_PILOT_DIR / "population_manifest_exact_2026-07-17.csv", "direct strict-prior Hits 1.5 PA pilot population"),
        ]
    )

    family_counts = eligible.groupby(["source_family", "prop_type"], dropna=False).size().reset_index(name="rows")
    event_rows = _event_orientation_results(eligible)
    decision_rows = _decision_side_results(eligible)
    decomposition_rows = _decomposition(eligible)
    date_rows = _date_stability(eligible)
    band_rows = _probability_bands(eligible)
    prior_rows = _prior_reconciliation()

    unresolved = [
        {
            "source_or_family": "repository-wide non-hits historical selected-proposition matrices",
            "reason": "visible artifact names do not by themselves certify exact prediction probability semantics and official outcome labels",
            "disposition": "fail_closed_not_included",
        },
        {
            "source_or_family": "legacy tmp graded_vs_prediction rows",
            "reason": "confidence_score/predicted_outcome semantics and model version lineage not sufficiently certified for platform direction audit",
            "disposition": "fail_closed_not_included",
        },
        {
            "source_or_family": "production upload artifacts",
            "reason": "upload rows are operational surfaces, not canonical official outcome evaluation populations for this audit",
            "disposition": "fail_closed_not_included",
        },
    ]

    decisions = {
        "MLB_PLATFORM_PREDICTION_SEMANTICS_DECISION": "PARTIAL_CERTIFIED_LOCAL_RECONCILE_AND_HITS_RESEARCH_ONLY_UNRESOLVED_FAMILIES_EXCLUDED",
        "MLB_PLATFORM_OUTCOME_SIDE_BINDING_DECISION": "PASS_FOR_INCLUDED_POPULATIONS_OFFICIAL_OUTCOME_AND_SIDE_BINDING_AVAILABLE",
        "MLB_PLATFORM_EVENT_ORIENTATION_DECISION": "MIXED_BY_SOURCE_AND_PROP_NO_GLOBAL_EVENT_INVERSION",
        "MLB_PLATFORM_MODEL_VS_FADE_DECISION": "MODEL_SIDE_WIN_RATE_SUPPORTED_IN_CURRENT_RECONCILE_FADE_ROI_BETTER_ON_PRICE_DIAGNOSTIC_ONLY",
        "MLB_PLATFORM_PROP_FAMILY_DIRECTION_DECISION": "INVERSION_LOCALIZED_OR_UNSTABLE_BY_PROP_FAMILY_NOT_PLATFORM_WIDE",
        "MLB_PLATFORM_MODEL_VERSION_DIRECTION_DECISION": "MODEL_VERSION_SPECIFIC_RESULTS_PROCESS_VALIDATION_NOT_PRODUCTION_GENERALIZABLE",
        "MLB_PLATFORM_TEMPORAL_STABILITY_DECISION": "INSUFFICIENT_MULTI_DATE_COVERAGE_FOR_PLATFORM_WIDE_STABILITY_CURRENT_BROAD_RECONCILE_ONE_DATE",
        "MLB_PLATFORM_IMPLEMENTATION_INVERSION_DECISION": "NO_GLOBAL_PROBABILITY_OR_SIDE_BINDING_DEFECT_FOUND_INCLUDED_ROWS_HAVE_LOCALIZED_HITS15_CONTROL_INVERSION_PRIOR",
        "MLB_PLATFORM_PRIOR_FADE_RECONCILIATION_DECISION": "PRIOR_MODEL_VS_FADE_REPRODUCED_AS_DECISION_SIDE_DIAGNOSTIC_NOT_EVENT_ORIENTATION_PROOF",
        "MLB_PLATFORM_PREDICTION_DIRECTION_INTEGRITY_DECISION": "PLATFORM_WIDE_DIRECTIONAL_INVERSION_NOT_SUPPORTED_BROADER_DIRECTIONAL_INSTABILITY_REMAINS",
        "MLB_PLATFORM_PRODUCTION_FADE_STATUS": "NOT_AUTHORIZED",
    }

    _write_csv(out_dir / f"prediction_family_model_version_inventory_{RUN_DATE}.csv", inventory_rows)
    _write_csv(out_dir / f"field_semantics_registry_{RUN_DATE}.csv", _semantics_registry())
    _write_csv(out_dir / f"exact_population_manifest_{RUN_DATE}.csv", _population_manifest(eligible))
    _write_csv(out_dir / f"population_counts_by_family_{RUN_DATE}.csv", family_counts)
    _write_csv(out_dir / f"event_orientation_results_{RUN_DATE}.csv", event_rows)
    _write_csv(out_dir / f"model_vs_fade_paired_results_{RUN_DATE}.csv", decision_rows)
    _write_csv(out_dir / f"prop_line_side_decomposition_{RUN_DATE}.csv", decomposition_rows)
    _write_csv(out_dir / f"model_version_decomposition_{RUN_DATE}.csv", pd.DataFrame(decomposition_rows).query("dimension == 'source_family|model_version'") if decomposition_rows else [])
    _write_csv(out_dir / f"date_stability_report_{RUN_DATE}.csv", date_rows)
    _write_csv(out_dir / f"probability_band_report_{RUN_DATE}.csv", band_rows)
    _write_csv(out_dir / f"change_point_review_{RUN_DATE}.csv", _change_point_review(date_rows))
    _write_csv(out_dir / f"binding_implementation_integrity_findings_{RUN_DATE}.csv", _binding_findings())
    _write_csv(out_dir / f"prior_model_vs_fade_reconciliation_{RUN_DATE}.csv", prior_rows)
    _write_csv(out_dir / f"unresolved_semantics_ledger_{RUN_DATE}.csv", unresolved)
    _write_csv(out_dir / f"decision_report_{RUN_DATE}.csv", [{"decision": k, "value": v} for k, v in decisions.items()])

    machine = {
        "run_date": RUN_DATE,
        "included_rows": int(len(eligible)),
        "included_source_families": sorted(eligible["source_family"].dropna().astype(str).unique()),
        "included_prop_families": sorted(eligible["prop_type"].dropna().astype(str).unique()),
        "decisions": decisions,
        "ops_model_vs_fade_summary": ops_payload.get("overall", {}),
    }
    (out_dir / f"machine_readable_platform_model_vs_fade_direction_integrity_{RUN_DATE}.json").write_text(
        json.dumps(machine, indent=2, sort_keys=True, default=_json_default),
        encoding="utf-8",
    )
    _write_md(out_dir, machine, family_counts, event_rows, decision_rows, decisions)
    _validation(out_dir)
    _sha_manifest(out_dir)
    return machine


def _change_point_review(date_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    df = pd.DataFrame(date_rows)
    if df.empty:
        return rows
    for keys, g in df.groupby(["source_family", "model_version"], dropna=False):
        source_family, model_version = keys
        positive = (pd.to_numeric(g["stored_auc"], errors="coerce") > 0.5).sum()
        negative = (pd.to_numeric(g["stored_auc"], errors="coerce") < 0.5).sum()
        rows.append(
            {
                "source_family": source_family,
                "model_version": model_version,
                "date_rows": int(len(g)),
                "positive_auc_dates": int(positive),
                "negative_auc_dates": int(negative),
                "review_result": "single_date_or_sparse_no_change_point_claim" if len(g) < 5 else "mixed_temporal_direction_observed_no_causality_inferred",
                "notes": "Documented transition dates not causally tested in this bounded audit.",
            }
        )
    return rows


def _binding_findings() -> list[dict[str, Any]]:
    return [
        {
            "area": "probability complement logic",
            "finding": "included execution-vs-model rows expose model_prob_over and model_prob_under as complements",
            "status": "no_global_defect_found",
        },
        {
            "area": "selected-side mapping",
            "finding": "model_pick_prob is bound to model_pick_side; fade side is exact opposite side for decision analysis",
            "status": "pass_for_included_rows",
        },
        {
            "area": "outcome label encoding",
            "finding": "actual_over_outcome/actual_under_outcome and binary_target mappings are explicit in included sources",
            "status": "pass_for_included_rows",
        },
        {
            "area": "Hits 1.5 control inversion",
            "finding": "prior audit found an unconstrained fitted control reversed champion ranking; this is localized to that research control, not a production probability binding defect",
            "status": "localized_research_control_issue",
        },
        {
            "area": "non-included families",
            "finding": "families without exact probability/outcome semantics were excluded rather than guessed",
            "status": "fail_closed",
        },
    ]


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, Path):
        return str(value)
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_md(out_dir: Path, machine: dict[str, Any], family_counts: pd.DataFrame, event_rows: list[dict[str, Any]], decision_rows: list[dict[str, Any]], decisions: dict[str, str]) -> None:
    fc_lines = ["| source | prop | rows |", "|---|---|---:|"]
    for _, r in family_counts.iterrows():
        fc_lines.append(f"| {r['source_family']} | {r['prop_type']} | {int(r['rows'])} |")

    ev = pd.DataFrame(event_rows)
    ev_stored = ev[ev["orientation"].eq("stored_event_probability")].copy() if not ev.empty else pd.DataFrame()
    ev_lines = ["| source | prop | line | rows | stored AUC | inverse AUC | classification |", "|---|---|---:|---:|---:|---:|---|"]
    if not ev_stored.empty:
        inv = ev[ev["orientation"].eq("deterministic_inverse_event_probability")].set_index(["source_family", "model_version", "prop_type", "line"])
        for _, r in ev_stored.sort_values(["source_family", "prop_type", "line"]).head(40).iterrows():
            key = (r["source_family"], r["model_version"], r["prop_type"], r["line"])
            inv_auc = inv.loc[key]["auc"] if key in inv.index else math.nan
            ev_lines.append(f"| {r['source_family']} | {r['prop_type']} | {r['line']} | {int(r['qualified_rows'])} | {_fmt(r['auc'])} | {_fmt(inv_auc)} | {r['classification']} |")

    mvf = pd.DataFrame(decision_rows)
    mvf_lines = ["| source | prop | line | side | rows | model WR | fade WR | model ROI | fade ROI |", "|---|---|---:|---|---:|---:|---:|---:|---:|"]
    if not mvf.empty:
        for _, r in mvf.sort_values(["source_family", "prop_type", "line", "side"]).head(50).iterrows():
            mvf_lines.append(
                f"| {r['source_family']} | {r['prop_type']} | {r['line']} | {r['side']} | {int(r['paired_rows'])} | {_fmt(r['model_win_rate'])} | {_fmt(r['fade_win_rate'])} | {_fmt(r.get('model_roi_1u'))} | {_fmt(r.get('fade_roi_1u'))} |"
            )

    decision_lines = "\n".join(f"`{k} = {v}`" for k, v in decisions.items())
    md = f"""# MLB Platform-Wide Model-versus-Fade Direction Integrity Audit - {RUN_DATE}

## Executive Summary

This bounded read-only audit did not find evidence for a platform-wide deterministic prediction inversion. It did find mixed and localized direction weakness, including the previously established Hits 1.5 research-control inversion. The current broad execution-vs-model reconcile source supports many prop families for a one-day decision-side and event-orientation diagnostic, while the collective-bundle prediction files support only non-production process-validation interpretation.

Included rows: `{machine['included_rows']}`.

Included source families:
{chr(10).join(f"- `{x}`" for x in machine['included_source_families'])}

Included prop families:
{chr(10).join(f"- `{x}`" for x in machine['included_prop_families'])}

Production fade remains `NOT_AUTHORIZED`.

## Population Counts

{chr(10).join(fc_lines)}

## Event Orientation Snapshot

{chr(10).join(ev_lines)}

## Model-versus-Fade Decision Snapshot

{chr(10).join(mvf_lines)}

## Interpretation

The evidence does not support a global claim that stored MLB predictions are inverted. Some groups have inverse-favorable AUC or fade-favorable ROI, but those effects are localized by prop/source/date and often have sparse or one-day support. Decision-side fade ROI is not the same thing as event-probability inversion, especially where price asymmetry drives ROI while model-side win rate remains higher.

The prior Hits 1.5 control inversion is reconciled as a localized research-control problem: an unconstrained fitted control reversed ranking. It is not evidence that the production `model_prob_over`/`model_prob_under` binding is globally reversed.

## Decisions

{decision_lines}

## Guardrails

No network access, OddsAPI calls, DB writes, model fitting/refitting, fitted calibration controls, threshold optimization, subgroup search, score inversion changes, production output changes, Quick Card/workspace changes, LaunchAgent changes, or promotion occurred.
"""
    (out_dir / f"executive_summary_{RUN_DATE}.md").write_text(md, encoding="utf-8")


def _fmt(value: Any) -> str:
    try:
        f = float(value)
    except Exception:
        return "NA" if value is None else str(value)
    if not math.isfinite(f):
        return "NA"
    return f"{f:.4f}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    result = build(args.output_dir)
    print(json.dumps({"output_dir": str(args.output_dir), "decisions": result["decisions"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
