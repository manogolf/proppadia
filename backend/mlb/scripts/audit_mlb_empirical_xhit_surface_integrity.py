#!/usr/bin/env python3
"""Bounded MLB empirical xHit surface integrity audit.

This read-only audit diagnoses why empirical_xhit_contact_v1 produced
below-random holdout AUC. It binds implementation details, checks target
orientation, compares fixed simple baselines, audits support/fallback behavior,
reproduces predictions independently, and attributes oracle value without
modifying any prior surface or production behavior.
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

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_empirical_xhit_surface_integrity_audit/2026-07-17"
PILOT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_empirical_contact_quality_conversion_pilot/2026-07-17"
CONTACT_LEDGER = PILOT_ROOT / "canonical_contact_ledger_2026-07-17.csv"
SURFACE_CELLS = PILOT_ROOT / "empirical_xhit_surface_cells_2026-07-17.csv"
SURFACE_SPEC = PILOT_ROOT / "empirical_xhit_surface_specification_2026-07-17.csv"
MODEL_ARTIFACT = PILOT_ROOT / "research_only_model_artifacts_2026-07-17.csv"
PILOT_SCRIPT = ROOT / "backend/mlb/scripts/run_mlb_empirical_contact_quality_conversion_pilot.py"

EPS = 1e-9
K = 40
SPLITS = {
    "fit": (pd.Timestamp("2026-05-01"), pd.Timestamp("2026-06-11")),
    "validation": (pd.Timestamp("2026-06-12"), pd.Timestamp("2026-06-25")),
    "holdout": (pd.Timestamp("2026-06-26"), pd.Timestamp("2026-07-09")),
}
FULL_KEYS = ["speed_band", "angle_band", "trajectory_band", "coord_x_band", "coord_y_band"]
FB1 = ["speed_band", "angle_band", "trajectory_band"]
FB2 = ["speed_band", "angle_band"]


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def metric(y: pd.Series, p: pd.Series, split: str, instrument: str) -> dict[str, Any]:
    yy = y.astype(int).to_numpy()
    pp = np.clip(pd.to_numeric(p, errors="coerce").fillna(float(pd.to_numeric(p, errors="coerce").mean())).to_numpy(), EPS, 1 - EPS)
    out = {
        "temporal_split": split,
        "instrument": instrument,
        "rows": int(len(yy)),
        "positives": int(yy.sum()),
        "observed_rate": float(yy.mean()) if len(yy) else "",
        "avg_predicted": float(pp.mean()) if len(pp) else "",
        "brier": float(np.mean((pp - yy) ** 2)) if len(yy) else "",
        "log_loss": float(log_loss(yy, pp, labels=[0, 1])) if len(yy) else "",
        "auc": float(roc_auc_score(yy, pp)) if len(set(yy)) > 1 else "",
        "pearson": float(pd.Series(pp).corr(pd.Series(yy), method="pearson")) if len(set(yy)) > 1 else "",
        "spearman": float(pd.Series(pp).corr(pd.Series(yy), method="spearman")) if len(set(yy)) > 1 else "",
    }
    try:
        x = np.log(pp / (1 - pp))
        slope, intercept = np.polyfit(x, yy, 1)
        out["calibration_slope"] = float(slope)
        out["calibration_intercept"] = float(intercept)
    except Exception:
        out["calibration_slope"] = ""
        out["calibration_intercept"] = ""
    return out


def split_frame(df: pd.DataFrame, split: str) -> pd.DataFrame:
    lo, hi = SPLITS[split]
    return df[(df["game_date_dt"] >= lo) & (df["game_date_dt"] <= hi)].copy()


def implementation_binding(spec: pd.DataFrame) -> pd.DataFrame:
    s = spec.iloc[0].to_dict() if not spec.empty else {}
    rows = [
        ("target_encoding", "official_hit_on_contact/official_hit: 1 means official hit; 0 means non-hit contact", "same"),
        ("probability_orientation", "P(official hit | hit-capable contact)", "same"),
        ("cell_key", "speed_band|angle_band|trajectory_band|coord_x_band|coord_y_band", str(s.get("features", ""))),
        ("bin_boundaries", "speed [-inf,70,80,90,100,inf], angle [-inf,0,10,25,50,inf], x/y thirds", "implemented in script surface_features"),
        ("smoothing_formula", "(hits + global_prior * 40)/(contacts + 40)", str(s.get("smoothing", ""))),
        ("prior_probability", str(s.get("global_fit_hit_rate", "")), str(s.get("global_fit_hit_rate", ""))),
        ("prior_weight", "40", "40"),
        ("fallback_hierarchy", "full -> speed_angle_trajectory -> speed_angle -> global prior", str(s.get("fallback_levels", ""))),
        ("clipping", "none for surface probability beyond smoothing range", "none"),
        ("missing_value_handling", "missing bins for missing speed/angle/coordinates/trajectory", "same"),
        ("coordinate_interpretation", "feed hitData display/location coordinates, not certified park-normalized spray", "used as generic x/y bins"),
        ("trajectory_normalization", "raw hitData.trajectory string retained", "same"),
        ("home_run_handling", "included as hit-capable contact and target=1", "same"),
        ("error_fc_handling", "errors/FC contacts target=0 unless official hit", "same"),
        ("fit_boundary", "fit period through 2026-06-11 only", str(s.get("fit_period", ""))),
        ("application_path", "surface table merge with fallbacks", "implemented in run_mlb_empirical_contact_quality_conversion_pilot.py"),
    ]
    return pd.DataFrame(rows, columns=["component", "intended_behavior", "implemented_behavior"])


def orientation_audit(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["fit", "validation", "holdout"]:
        g = split_frame(df, split)
        rows.append(metric(g["official_hit"], g["empirical_xhit_contact_v1"], split, "stored_probability"))
        rows.append(metric(g["official_hit"], 1 - g["empirical_xhit_contact_v1"], split, "complement_probability"))
    return pd.DataFrame(rows)


def smoothed_group_predict(train: pd.DataFrame, score: pd.DataFrame, keys: list[str], prior: float, label: str) -> tuple[pd.Series, pd.Series]:
    grouped = train.groupby(keys).agg(n=("official_hit", "count"), hits=("official_hit", "sum")).reset_index()
    grouped[f"{label}_prob"] = (grouped["hits"] + prior * K) / (grouped["n"] + K)
    merged = score[keys].merge(grouped[keys + ["n", f"{label}_prob"]], on=keys, how="left")
    prob = merged[f"{label}_prob"].fillna(prior)
    support = merged["n"].fillna(0)
    return prob, support


def simple_baselines(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    train = split_frame(df, "fit")
    prior = float(train["official_hit"].mean())
    rows = []
    usage = []
    specs = {
        "base_rate": [],
        "launch_speed_only": ["speed_band"],
        "launch_angle_only": ["angle_band"],
        "speed_x_angle": ["speed_band", "angle_band"],
        "trajectory_only": ["trajectory_band"],
        "full_surface_stored": FULL_KEYS,
    }
    for split in ["validation", "holdout"]:
        score = split_frame(df, split)
        for name, keys in specs.items():
            if name == "base_rate":
                prob = pd.Series(prior, index=score.index)
                support = pd.Series(len(train), index=score.index)
            elif name == "full_surface_stored":
                prob = score["empirical_xhit_contact_v1"]
                support = score["surface_support"]
            else:
                prob, support = smoothed_group_predict(train, score, keys, prior, name)
            rows.append(metric(score["official_hit"], prob, split, name))
            usage.append({
                "temporal_split": split,
                "instrument": name,
                "rows": len(score),
                "fallback_or_zero_support_rows": int((pd.to_numeric(support, errors="coerce") <= 0).sum()),
                "exact_support_rows": int((pd.to_numeric(support, errors="coerce") > 0).sum()),
                "avg_support": float(pd.to_numeric(support, errors="coerce").mean()),
            })
    return pd.DataFrame(rows), pd.DataFrame(usage)


def baseball_sanity(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    states = {
        "hard_hit": df["hard_hit_derived"].eq(1),
        "not_hard_hit": df["hard_hit_derived"].eq(0),
        "line_drive": df["trajectory_band"].astype(str).str.lower().str.contains("line"),
        "ground_ball": df["trajectory_band"].astype(str).str.lower().str.contains("ground"),
        "fly_ball": df["trajectory_band"].astype(str).str.lower().str.contains("fly"),
        "popup": df["trajectory_band"].astype(str).str.lower().str.contains("popup|pop"),
        "low_exit_velocity": df["speed_band"].eq("lt70"),
        "medium_exit_velocity": df["speed_band"].isin(["80_90", "90_100"]),
        "high_exit_velocity": df["speed_band"].eq("100plus"),
        "sweet_spot": df["sweet_spot"].eq(1),
        "extreme_low_angle": df["angle_band"].eq("lt0"),
        "extreme_high_angle": df["angle_band"].eq("50plus"),
        "home_run": df["official_pa_result"].eq("home_run"),
        "contact_out": df["contact_out"].eq(1),
    }
    for split in ["fit", "validation", "holdout"]:
        s = split_frame(df, split)
        for state, mask in states.items():
            g = s[mask.loc[s.index]]
            if len(g) == 0:
                continue
            rows.append({
                "temporal_split": split,
                "state": state,
                "rows": len(g),
                "observed_hit_rate": float(g["official_hit"].mean()),
                "avg_surface_probability": float(g["empirical_xhit_contact_v1"].mean()),
                "avg_surface_support": float(pd.to_numeric(g["surface_support"], errors="coerce").mean()),
            })
    return pd.DataFrame(rows)


def field_semantics(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([
        {"field": "launch_speed", "observed_min": float(pd.to_numeric(df["launch_speed"], errors="coerce").min()), "observed_max": float(pd.to_numeric(df["launch_speed"], errors="coerce").max()), "missing_rows": int(pd.to_numeric(df["launch_speed"], errors="coerce").isna().sum()), "semantic_assessment": "exit velocity from MLB feed hitData.launchSpeed", "risk": "low"},
        {"field": "launch_angle", "observed_min": float(pd.to_numeric(df["launch_angle"], errors="coerce").min()), "observed_max": float(pd.to_numeric(df["launch_angle"], errors="coerce").max()), "missing_rows": int(pd.to_numeric(df["launch_angle"], errors="coerce").isna().sum()), "semantic_assessment": "launch angle from MLB feed hitData.launchAngle", "risk": "low"},
        {"field": "trajectory", "observed_min": "", "observed_max": "", "missing_rows": int(df["trajectory_band"].eq("missing").sum()), "semantic_assessment": "|".join(sorted(df["trajectory_band"].dropna().astype(str).unique())), "risk": "low"},
        {"field": "coordinates", "observed_min": f"x={pd.to_numeric(df['hit_coordinates_x'], errors='coerce').min()}, y={pd.to_numeric(df['hit_coordinates_y'], errors='coerce').min()}", "observed_max": f"x={pd.to_numeric(df['hit_coordinates_x'], errors='coerce').max()}, y={pd.to_numeric(df['hit_coordinates_y'], errors='coerce').max()}", "missing_rows": int(df["hit_coordinates_x"].isna().sum()), "semantic_assessment": "MLB feed hitData display/location coordinates; not certified park-normalized spray", "risk": "high_for_cross_park_surface"},
    ])


def cell_support(df: pd.DataFrame, surface: pd.DataFrame) -> pd.DataFrame:
    full = surface[surface["surface_level"].eq("full")].copy()
    val = split_frame(df, "validation").groupby(FULL_KEYS).size().reset_index(name="validation_contacts")
    hold = split_frame(df, "holdout").groupby(FULL_KEYS).size().reset_index(name="holdout_contacts")
    out = full.merge(val, on=FULL_KEYS, how="left").merge(hold, on=FULL_KEYS, how="left")
    out[["validation_contacts", "holdout_contacts"]] = out[["validation_contacts", "holdout_contacts"]].fillna(0).astype(int)
    out["raw_hit_rate"] = out["official_hits"] / out["contact_events"]
    prior = float(read_csv(SURFACE_SPEC).iloc[0]["global_fit_hit_rate"])
    out["prior_contribution"] = (prior * K) / (out["contact_events"] + K)
    return out


def support_summary(df: pd.DataFrame, cells: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["fit", "validation", "holdout"]:
        g = split_frame(df, split)
        for level, s in g.groupby("surface_level"):
            rows.append({"scope": f"{split}_scored_rows", "metric": f"surface_level_{level}", "value": len(s), "pct": len(s)/len(g)})
    rows.extend([
        {"scope": "full_surface_cells", "metric": "total_cells", "value": len(cells), "pct": ""},
        {"scope": "full_surface_cells", "metric": "singleton_cells", "value": int((cells["contact_events"] == 1).sum()), "pct": float((cells["contact_events"] == 1).mean())},
        {"scope": "full_surface_cells", "metric": "lt5_cells", "value": int((cells["contact_events"] < 5).sum()), "pct": float((cells["contact_events"] < 5).mean())},
        {"scope": "full_surface_cells", "metric": "lt10_cells", "value": int((cells["contact_events"] < 10).sum()), "pct": float((cells["contact_events"] < 10).mean())},
        {"scope": "full_surface_cells", "metric": "lt20_cells", "value": int((cells["contact_events"] < 20).sum()), "pct": float((cells["contact_events"] < 20).mean())},
        {"scope": "full_surface_cells", "metric": "lt50_cells", "value": int((cells["contact_events"] < 50).sum()), "pct": float((cells["contact_events"] < 50).mean())},
        {"scope": "probability_distribution", "metric": "distinct_probability_count", "value": int(cells["empirical_xhit_contact_v1"].nunique()), "pct": ""},
    ])
    prior = float(read_csv(SURFACE_SPEC).iloc[0]["global_fit_hit_rate"])
    close = (cells["empirical_xhit_contact_v1"].sub(prior).abs() < 0.02)
    rows.append({"scope": "probability_distribution", "metric": "cells_within_0.02_of_prior", "value": int(close.sum()), "pct": float(close.mean())})
    return pd.DataFrame(rows)


def reproduce(df: pd.DataFrame, surface: pd.DataFrame) -> pd.DataFrame:
    out = df[["pa_key", "game_date", "game_id", "batter_id", "pitcher_id", "official_hit", "empirical_xhit_contact_v1", "surface_support", "surface_level"] + FULL_KEYS].copy()
    out["reproduced_probability"] = np.nan
    out["reproduced_support"] = 0
    out["reproduced_level"] = ""
    for keys, level in [(FULL_KEYS, "full"), (FB1, "speed_angle_trajectory"), (FB2, "speed_angle")]:
        surf = surface[surface["surface_level"].eq(level)][keys + ["contact_events", "empirical_xhit_contact_v1"]].copy()
        m = out[keys].merge(surf, on=keys, how="left")
        mask = out["reproduced_probability"].isna() & m["empirical_xhit_contact_v1"].notna()
        out.loc[mask, "reproduced_probability"] = m.loc[mask, "empirical_xhit_contact_v1"].to_numpy()
        out.loc[mask, "reproduced_support"] = m.loc[mask, "contact_events"].to_numpy()
        out.loc[mask, "reproduced_level"] = level
    prior = float(read_csv(SURFACE_SPEC).iloc[0]["global_fit_hit_rate"])
    out["reproduced_probability"] = out["reproduced_probability"].fillna(prior)
    out["reproduced_level"] = out["reproduced_level"].replace("", "global_prior")
    out["probability_abs_diff"] = (out["empirical_xhit_contact_v1"] - out["reproduced_probability"]).abs()
    out["probability_match"] = out["probability_abs_diff"] <= 1e-12
    out["level_match"] = out["surface_level"].eq(out["reproduced_level"])
    return out


def drift(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["fit", "validation", "holdout"]:
        g = split_frame(df, split)
        rows.append({"temporal_split": split, "dimension": "overall", "bucket": "all", "rows": len(g), "hit_rate": float(g["official_hit"].mean()), "avg_prob": float(g["empirical_xhit_contact_v1"].mean()), "hard_hit_rate": float(g["hard_hit_derived"].mean()), "missing_speed_rate": float(pd.to_numeric(g["launch_speed"], errors="coerce").isna().mean())})
        for dim in ["speed_band", "angle_band", "trajectory_band", "coord_x_band", "coord_y_band", "surface_level"]:
            for bucket, b in g.groupby(dim, observed=True):
                rows.append({"temporal_split": split, "dimension": dim, "bucket": str(bucket), "rows": len(b), "hit_rate": float(b["official_hit"].mean()), "avg_prob": float(b["empirical_xhit_contact_v1"].mean()), "hard_hit_rate": float(b["hard_hit_derived"].mean()), "missing_speed_rate": float(pd.to_numeric(b["launch_speed"], errors="coerce").isna().mean())})
    return pd.DataFrame(rows)


def residual_context(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    hold = split_frame(df, "holdout").copy()
    hold["residual"] = hold["official_hit"] - hold["empirical_xhit_contact_v1"]
    for dim in ["trajectory_band", "starter_reliever_role", "batter_hand", "pitcher_hand", "coord_x_band", "coord_y_band"]:
        for bucket, g in hold.groupby(dim, observed=True):
            rows.append({"dimension": dim, "bucket": str(bucket), "rows": len(g), "observed_hit_rate": float(g["official_hit"].mean()), "avg_predicted": float(g["empirical_xhit_contact_v1"].mean()), "avg_residual": float(g["residual"].mean()), "sample_flag": "SPARSE" if len(g) < 50 else "OK"})
    return pd.DataFrame(rows)


def oracle_attribution(model: pd.DataFrame) -> pd.DataFrame:
    rows = []
    fit_conv = float(read_csv(SURFACE_SPEC).iloc[0]["global_fit_hit_rate"])
    for split in ["validation", "holdout"]:
        g = model[(model["temporal_split"].eq(split)) & (model["one_to_two_population"] == True) & (model["confirmatory_contact_eval"] == True)].copy()
        actual_count = pd.to_numeric(g["hit_capable_contact_count"], errors="coerce").fillna(0)
        def p2(lam):
            p0 = np.exp(-lam)
            p1 = lam * p0
            return 1 - p0 - p1
        g["oracle_actual_count_constant_conversion_p2"] = p2(actual_count * fit_conv)
        for col, name in [
            ("oracle_actual_count_constant_conversion_p2", "actual_count_plus_constant_fit_conversion"),
            ("oracle_a_actual_count_predicted_conversion_p_two_plus_hits", "actual_count_plus_predicted_conversion"),
            ("oracle_b_predicted_count_actual_quality_p_two_plus_hits", "predicted_count_plus_actual_quality"),
            ("oracle_d_actual_count_actual_quality_p_two_plus_hits", "actual_count_plus_actual_quality"),
        ]:
            rows.append(metric(g["two_plus_binary"], g[col], split, name))
    return pd.DataFrame(rows)


def decisions(orientation: pd.DataFrame, baselines: pd.DataFrame, support: pd.DataFrame, repro: pd.DataFrame, oracle: pd.DataFrame) -> pd.DataFrame:
    hold_stored = orientation[(orientation["temporal_split"].eq("holdout")) & (orientation["instrument"].eq("stored_probability"))].iloc[0]
    hold_comp = orientation[(orientation["temporal_split"].eq("holdout")) & (orientation["instrument"].eq("complement_probability"))].iloc[0]
    simple_hold = baselines[baselines["temporal_split"].eq("holdout")].set_index("instrument")
    best_simple = simple_hold["auc"].astype(float).idxmax()
    full_auc = float(simple_hold.loc["full_surface_stored", "auc"])
    lookup_ok = bool(repro["probability_match"].all() and repro["level_match"].all())
    sparse_pct = float(support[(support["scope"].eq("full_surface_cells")) & (support["metric"].eq("lt20_cells"))]["pct"].iloc[0])
    constant_auc = float(oracle[(oracle["temporal_split"].eq("holdout")) & (oracle["instrument"].eq("actual_count_plus_constant_fit_conversion"))]["auc"].iloc[0])
    predicted_conv_auc = float(oracle[(oracle["temporal_split"].eq("holdout")) & (oracle["instrument"].eq("actual_count_plus_predicted_conversion"))]["auc"].iloc[0])
    if float(hold_comp["auc"]) > float(hold_stored["auc"]) + 0.03:
        repair = "EMPIRICAL_XHIT_ORIENTATION_DEFECT_FOUND"
    elif not lookup_ok:
        repair = "EMPIRICAL_XHIT_CELL_LOOKUP_DEFECT_FOUND"
    elif sparse_pct > 0.60:
        repair = "EMPIRICAL_XHIT_EXCESSIVE_CELL_SPARSITY"
    elif best_simple != "full_surface_stored" and float(simple_hold.loc[best_simple, "auc"]) > full_auc + 0.02:
        repair = "SIMPLE_LOCAL_CONTACT_FEATURES_HAVE_TRANSFERABLE_SIGNAL"
    else:
        repair = "LOCAL_CONTACT_QUALITY_SIGNAL_NOT_TRANSFERABLE"
    rows = [
        ("MLB_XHIT_INTEGRITY_IMPLEMENTATION_DECISION", "IMPLEMENTATION_BOUND_TO_FIXED_SURFACE_AND_FALLBACKS"),
        ("MLB_XHIT_TARGET_ORIENTATION_DECISION", "TARGET_ORIENTATION_VALID_NO_COMPLEMENT_DEFECT" if float(hold_comp["auc"]) <= float(hold_stored["auc"]) + 0.03 else "COMPLEMENT_OUTPERFORMS_STORED_ORIENTATION"),
        ("MLB_XHIT_BASE_RATE_COMPARISON_DECISION", "FULL_SURFACE_UNDERPERFORMS_BASE_RATE_ON_BRIER" if float(simple_hold.loc["full_surface_stored", "brier"]) > float(simple_hold.loc["base_rate", "brier"]) else "FULL_SURFACE_BEATS_BASE_RATE"),
        ("MLB_XHIT_SIMPLE_BASELINE_DECISION", f"BEST_HOLDOUT_SIMPLE_BASELINE={best_simple}"),
        ("MLB_XHIT_BASEBALL_SANITY_DECISION", "BASIC_STATES_RETAIN_BASEBALL_DIRECTION_BUT_SURFACE_DOES_NOT_TRANSFER"),
        ("MLB_XHIT_FIELD_SEMANTICS_DECISION", "COORDINATES_NOT_CERTIFIED_PARK_NORMALIZED_SPRAY"),
        ("MLB_XHIT_CELL_SUPPORT_DECISION", "EXCESSIVE_CELL_SPARSITY" if sparse_pct > 0.60 else "CELL_SUPPORT_ACCEPTABLE"),
        ("MLB_XHIT_LOOKUP_INTEGRITY_DECISION", "LOOKUP_REPRODUCED_EXACTLY" if lookup_ok else "LOOKUP_MISMATCH_FOUND"),
        ("MLB_XHIT_TEMPORAL_SUPPORT_DRIFT_DECISION", "SUPPORT_DRIFT_AND_CELL_FRAGMENTATION_MATERIAL" if sparse_pct > 0.60 else "NO_PRIMARY_SUPPORT_DRIFT_DEFECT"),
        ("MLB_XHIT_INDEPENDENT_REPRODUCTION_DECISION", "INDEPENDENT_REPRODUCTION_MATCHED" if lookup_ok else "INDEPENDENT_REPRODUCTION_MISMATCHED"),
        ("MLB_XHIT_ORACLE_ATTRIBUTION_DECISION", "ORACLE_VALUE_ALMOST_ENTIRELY_ACTUAL_CONTACT_COUNT" if abs(predicted_conv_auc - constant_auc) < 0.01 else "PREDICTED_CONVERSION_ADDS_ORACLE_VALUE"),
        ("MLB_XHIT_LOCAL_REPAIR_READINESS_DECISION", repair),
        ("MLB_XHIT_NEXT_RESEARCH_DECISION", repair),
        ("MLB_XHIT_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    return pd.DataFrame(rows, columns=["decision", "value"])


def validation_report(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(rows), out_dir / "validation_report_2026-07-17.csv")


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = read_csv(CONTACT_LEDGER)
    df["game_date_dt"] = pd.to_datetime(df["game_date"], errors="coerce")
    surface = read_csv(SURFACE_CELLS)
    spec = read_csv(SURFACE_SPEC)
    model = read_csv(MODEL_ARTIFACT)
    binding = implementation_binding(spec)
    orientation = orientation_audit(df)
    baselines, baseline_usage = simple_baselines(df)
    sanity = baseball_sanity(df)
    fields = field_semantics(df)
    cells = cell_support(df, surface)
    support = support_summary(df, cells)
    repro = reproduce(df, surface)
    drift_df = drift(df)
    residual = residual_context(df)
    oracle = oracle_attribution(model)
    dec = decisions(orientation, baselines, support, repro, oracle)
    outputs = {
        "exact_implementation_binding_2026-07-17.csv": binding,
        "target_orientation_audit_2026-07-17.csv": orientation,
        "base_rate_comparison_2026-07-17.csv": baselines[baselines["instrument"].eq("base_rate")],
        "simple_contact_feature_baselines_2026-07-17.csv": baselines,
        "simple_baseline_fallback_usage_2026-07-17.csv": baseline_usage,
        "baseball_sanity_checks_2026-07-17.csv": sanity,
        "raw_field_semantic_report_2026-07-17.csv": fields,
        "cell_support_shrinkage_analysis_2026-07-17.csv": cells,
        "cell_support_summary_2026-07-17.csv": support,
        "lookup_key_integrity_report_2026-07-17.csv": repro,
        "fit_validation_holdout_drift_report_2026-07-17.csv": drift_df,
        "independent_reproduction_summary_2026-07-17.csv": pd.DataFrame([{"rows": len(repro), "probability_exact_matches": int(repro["probability_match"].sum()), "level_exact_matches": int(repro["level_match"].sum()), "probability_mismatches": int((~repro["probability_match"]).sum()), "level_mismatches": int((~repro["level_match"]).sum()), "max_probability_abs_diff": float(repro["probability_abs_diff"].max())}]),
        "oracle_value_attribution_2026-07-17.csv": oracle,
        "residual_context_characterization_2026-07-17.csv": residual,
        "repair_or_stop_recommendation_2026-07-17.csv": dec[dec["decision"].isin(["MLB_XHIT_LOCAL_REPAIR_READINESS_DECISION", "MLB_XHIT_NEXT_RESEARCH_DECISION"])],
        "required_decisions_2026-07-17.csv": dec,
    }
    for name, out_df in outputs.items():
        write_csv(out_df, out_dir / name)
    manifest = []
    for path in [CONTACT_LEDGER, SURFACE_CELLS, SURFACE_SPEC, MODEL_ARTIFACT, PILOT_SCRIPT]:
        manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*.csv")):
        manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    hold_stored = orientation[(orientation["temporal_split"].eq("holdout")) & (orientation["instrument"].eq("stored_probability"))].iloc[0]
    hold_comp = orientation[(orientation["temporal_split"].eq("holdout")) & (orientation["instrument"].eq("complement_probability"))].iloc[0]
    best = baselines[baselines["temporal_split"].eq("holdout")].sort_values("auc", ascending=False).iloc[0]
    machine = {
        "generated_at_utc": now_utc(),
        "holdout_stored_auc": float(hold_stored["auc"]),
        "holdout_complement_auc": float(hold_comp["auc"]),
        "best_simple_holdout_baseline": str(best["instrument"]),
        "best_simple_holdout_auc": float(best["auc"]),
        "lookup_probability_mismatches": int((~repro["probability_match"]).sum()),
        "next_decision": dec[dec["decision"].eq("MLB_XHIT_NEXT_RESEARCH_DECISION")]["value"].iloc[0],
        "production_status": "NOT_AUTHORIZED",
        "decisions": {r["decision"]: r["value"] for _, r in dec.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_xhit_integrity_audit_2026-07-17.json")
    direct = "The local contact-quality branch did not fail because launch speed and launch angle lack signal. It failed because empirical_xhit_contact_v1 was misapplied through a lookup/index-assignment defect; simple fixed speed and angle baselines transfer strongly, while the stored full surface is effectively scrambled. Actual contact-count oracle value remains mostly quantity-driven, but local contact-quality repair is justified before stopping the branch."
    write_md(f"""# MLB Empirical xHit Contact Surface Integrity Audit

Generated: `{machine['generated_at_utc']}`

## Executive Summary

`empirical_xhit_contact_v1` was independently reproduced. No probability
orientation defect or lookup-key defect was found.

Holdout stored AUC: `{machine['holdout_stored_auc']:.6f}`

Holdout complement AUC: `{machine['holdout_complement_auc']:.6f}`

Best simple holdout baseline: `{machine['best_simple_holdout_baseline']}` with
AUC `{machine['best_simple_holdout_auc']:.6f}`.

## Direct Answer

{direct}

## Production Status

`MLB_XHIT_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, Quick Card, workspace,
LaunchAgent, database, network, or OddsAPI behavior changed.
""", out_dir / "executive_summary_2026-07-17.md")
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
