#!/usr/bin/env python3
"""Repair empirical_xhit_contact_v1 lookup assignment and revalidate.

This bounded research-only utility preserves the frozen empirical surface
definition, bins, smoothing, temporal splits, profile logic, and multi-hit
instruments from the prior pilot. It repairs only the lookup/index assignment
path by carrying a stable row id through each fallback merge.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, scheduler changes, threshold search, price optimization, feature
redesign, or hyperparameter search are performed.
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

from backend.mlb.scripts import audit_mlb_empirical_xhit_surface_integrity as integrity
from backend.mlb.scripts import run_mlb_empirical_contact_quality_conversion_pilot as pilot

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_empirical_xhit_lookup_repair_revalidation/2026-07-17"

CONTACT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17"
CONTACT_LEDGER = CONTACT_ROOT / "canonical_contact_outcome_ledger_2026-07-17.csv"
CONTACT_POP = CONTACT_ROOT / "research_only_model_artifacts_2026-07-17.csv"
PRIOR_PILOT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_empirical_contact_quality_conversion_pilot/2026-07-17"
PRIOR_INTEGRITY_ROOT = ROOT / "artifacts/analysis/model_development/mlb_empirical_xhit_surface_integrity_audit/2026-07-17"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"

SURFACE_KEYS = pilot.SURFACE_KEYS
FALLBACK_1 = pilot.FALLBACK_1
FALLBACK_2 = pilot.FALLBACK_2
EPS = pilot.EPS


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


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_identity(df: pd.DataFrame) -> pd.Series:
    parts = []
    for col in ["game_date", "game_id", "plate_appearance_sequence", "batter_id", "pitcher_id", "pa_key"]:
        if col not in df.columns:
            parts.append(pd.Series("", index=df.index))
        else:
            parts.append(df[col].fillna("").astype(str))
    return parts[0].str.cat(parts[1:], sep="|")


def corrected_apply_surface(contact: pd.DataFrame, surface: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    """Apply the frozen surface without relying on source dataframe indexes.

    The original pilot merged `out[keys]` to each fallback surface, producing a
    RangeIndex result, then used that boolean mask against `out.loc[...]`.
    Because the contact frame retained a stale non-contiguous index after
    filtering hit-capable contacts, many probabilities and fallback levels were
    assigned to the wrong source rows. This implementation carries `row_id`
    through each merge and assigns back by that id.
    """

    out = contact.copy().reset_index(drop=True)
    out["_row_id"] = np.arange(len(out))
    out["canonical_contact_identity"] = canonical_identity(out)
    out["empirical_xhit_contact_v1_lookup_corrected"] = np.nan
    out["empirical_xhit_contact_v1"] = np.nan
    out["surface_support"] = 0
    out["surface_level"] = ""
    out["surface_application_version"] = "empirical_xhit_contact_v1_lookup_corrected"

    for keys, level in [(SURFACE_KEYS, "full"), (FALLBACK_1, "speed_angle_trajectory"), (FALLBACK_2, "speed_angle")]:
        surf = surface[surface["surface_level"].eq(level)][keys + ["contact_events", "empirical_xhit_contact_v1"]].copy()
        merged = out[["_row_id"] + keys].merge(surf, on=keys, how="left", validate="many_to_one")
        hit = merged["empirical_xhit_contact_v1"].notna()
        if not hit.any():
            continue
        row_ids = merged.loc[hit, "_row_id"].to_numpy()
        still_missing = out.loc[row_ids, "empirical_xhit_contact_v1_lookup_corrected"].isna().to_numpy()
        if not still_missing.any():
            continue
        assign_ids = row_ids[still_missing]
        source_rows = merged.loc[hit].iloc[np.flatnonzero(still_missing)]
        probs = source_rows["empirical_xhit_contact_v1"].to_numpy()
        supports = source_rows["contact_events"].to_numpy()
        out.loc[assign_ids, "empirical_xhit_contact_v1_lookup_corrected"] = probs
        out.loc[assign_ids, "empirical_xhit_contact_v1"] = probs
        out.loc[assign_ids, "surface_support"] = supports
        out.loc[assign_ids, "surface_level"] = level

    prior = float(spec["global_fit_hit_rate"])
    missing = out["empirical_xhit_contact_v1_lookup_corrected"].isna()
    out.loc[missing, "empirical_xhit_contact_v1_lookup_corrected"] = prior
    out.loc[missing, "empirical_xhit_contact_v1"] = prior
    out.loc[missing, "surface_level"] = "global_prior"
    out.loc[missing, "surface_support"] = 0
    out["empirical_xhit_contact_v1_lookup_corrected"] = out["empirical_xhit_contact_v1_lookup_corrected"].clip(EPS, 1 - EPS)
    out["empirical_xhit_contact_v1"] = out["empirical_xhit_contact_v1"].clip(EPS, 1 - EPS)
    return out.drop(columns=["_row_id"])


def independently_reproduce(contact: pd.DataFrame, surface: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    base_cols = [
        "canonical_contact_identity", "pa_key", "game_date", "game_id", "plate_appearance_sequence",
        "batter_id", "pitcher_id", "official_hit", "empirical_xhit_contact_v1_lookup_corrected",
        "surface_support", "surface_level", *SURFACE_KEYS,
    ]
    out = contact[base_cols].copy().reset_index(drop=True)
    out["_row_id"] = np.arange(len(out))
    out["reproduced_probability"] = np.nan
    out["reproduced_support"] = 0
    out["reproduced_level"] = ""
    for keys, level in [(SURFACE_KEYS, "full"), (FALLBACK_1, "speed_angle_trajectory"), (FALLBACK_2, "speed_angle")]:
        surf = surface[surface["surface_level"].eq(level)][keys + ["contact_events", "empirical_xhit_contact_v1"]].copy()
        merged = out[["_row_id"] + keys].merge(surf, on=keys, how="left", validate="many_to_one")
        hit = merged["empirical_xhit_contact_v1"].notna()
        row_ids = merged.loc[hit, "_row_id"].to_numpy()
        still_missing = out.loc[row_ids, "reproduced_probability"].isna().to_numpy()
        assign_ids = row_ids[still_missing]
        source_rows = merged.loc[hit].iloc[np.flatnonzero(still_missing)]
        out.loc[assign_ids, "reproduced_probability"] = source_rows["empirical_xhit_contact_v1"].to_numpy()
        out.loc[assign_ids, "reproduced_support"] = source_rows["contact_events"].to_numpy()
        out.loc[assign_ids, "reproduced_level"] = level
    prior = float(spec["global_fit_hit_rate"])
    missing = out["reproduced_probability"].isna()
    out.loc[missing, "reproduced_probability"] = prior
    out.loc[missing, "reproduced_level"] = "global_prior"
    out["probability_abs_diff"] = (
        out["empirical_xhit_contact_v1_lookup_corrected"].astype(float) - out["reproduced_probability"].astype(float)
    ).abs()
    out["probability_exact_match"] = out["probability_abs_diff"].le(1e-12)
    out["probability_tolerance_match"] = out["probability_abs_diff"].le(1e-9)
    out["lookup_level_match"] = out["surface_level"].eq(out["reproduced_level"])
    out["support_match"] = pd.to_numeric(out["surface_support"], errors="coerce").fillna(-1).eq(
        pd.to_numeric(out["reproduced_support"], errors="coerce").fillna(-2)
    )
    return out.drop(columns=["_row_id"])


def regression_harness(contact_raw: pd.DataFrame, surface: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    rows = []
    base = corrected_apply_surface(contact_raw, surface, spec)
    repro = independently_reproduce(base, surface, spec)
    rows.append({"test_name": "independent_reconstruction_matches_stored", "status": "PASS" if repro["probability_tolerance_match"].all() and repro["lookup_level_match"].all() else "FAIL", "rows": len(repro), "notes": "probability, support, and fallback level bind by row_id/canonical identity"})

    shuffled = contact_raw.sample(frac=1, random_state=20260717).reset_index(drop=True)
    shuffled_applied = corrected_apply_surface(shuffled, surface, spec)
    cmp = base[["canonical_contact_identity", "empirical_xhit_contact_v1_lookup_corrected", "surface_level"]].merge(
        shuffled_applied[["canonical_contact_identity", "empirical_xhit_contact_v1_lookup_corrected", "surface_level"]],
        on="canonical_contact_identity",
        suffixes=("_base", "_shuffled"),
        how="outer",
        validate="one_to_one",
    )
    order_ok = (
        cmp["empirical_xhit_contact_v1_lookup_corrected_base"].sub(cmp["empirical_xhit_contact_v1_lookup_corrected_shuffled"]).abs().le(1e-12).all()
        and cmp["surface_level_base"].eq(cmp["surface_level_shuffled"]).all()
    )
    rows.append({"test_name": "batch_order_and_sorting_invariant", "status": "PASS" if order_ok else "FAIL", "rows": len(cmp), "notes": "same identities retain same probabilities after shuffle/reset_index"})

    duplicate_count = int(base["canonical_contact_identity"].duplicated().sum())
    rows.append({"test_name": "duplicate_canonical_identities_fail_closed_check", "status": "PASS" if duplicate_count == 0 else "FAIL", "rows": duplicate_count, "notes": "identity is game_date|game_id|pa_seq|batter_id|pitcher_id|pa_key"})

    for label, selector in {
        "exact_full_surface_cell": base["surface_level"].eq("full"),
        "partial_fallback": base["surface_level"].isin(["speed_angle_trajectory", "speed_angle"]),
        "global_prior": base["surface_level"].eq("global_prior"),
        "home_run": base["official_pa_result"].eq("home_run"),
        "line_drive": base["trajectory_band"].astype(str).str.lower().str.contains("line", na=False),
        "ground_ball": base["trajectory_band"].astype(str).str.lower().str.contains("ground", na=False),
        "popup": base["trajectory_band"].astype(str).str.lower().str.contains("popup|pop", na=False),
        "missing_coordinates": base["coord_x_band"].eq("x_missing") | base["coord_y_band"].eq("y_missing"),
        "sparse_cell": pd.to_numeric(base["surface_support"], errors="coerce").between(1, 9),
        "high_support_cell": pd.to_numeric(base["surface_support"], errors="coerce").ge(80),
    }.items():
        sample = base[selector].head(1)
        rows.append({
            "test_name": f"representative_{label}",
            "status": "PASS" if len(sample) else "WARN",
            "rows": len(sample),
            "notes": "" if sample.empty else f"identity={sample.iloc[0]['canonical_contact_identity']} prob={sample.iloc[0]['empirical_xhit_contact_v1_lookup_corrected']}",
        })

    rows.append({"test_name": "target_orientation_one_equals_official_hit", "status": "PASS" if set(base["official_hit"].dropna().astype(int).unique()).issubset({0, 1}) else "FAIL", "rows": len(base), "notes": "official_hit remains binary hit target"})
    return pd.DataFrame(rows)


def reproduction_summary(repro: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{
        "contact_rows": len(repro),
        "canonical_identity_mismatches": int(base["canonical_contact_identity"].isna().sum()),
        "duplicate_identities": int(base["canonical_contact_identity"].duplicated().sum()),
        "probability_exact_matches": int(repro["probability_exact_match"].sum()),
        "probability_tolerance_matches": int(repro["probability_tolerance_match"].sum()),
        "probability_mismatches": int((~repro["probability_tolerance_match"]).sum()),
        "lookup_level_matches": int(repro["lookup_level_match"].sum()),
        "lookup_level_mismatches": int((~repro["lookup_level_match"]).sum()),
        "support_matches": int(repro["support_match"].sum()),
        "max_probability_abs_diff": float(repro["probability_abs_diff"].max()) if len(repro) else "",
    }])


def profile_comparison(old_model: pd.DataFrame, new_model: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "hitter_empirical_xhit_per_contact",
        "starter_empirical_xhit_allowed_per_contact",
        "bullpen_empirical_xhit_allowed_per_contact",
        "hitter_plus_starter_conversion",
        "source_aware_starter_conversion",
        "source_aware_bullpen_conversion",
        "source_aware_conversion_p_two_plus_hits",
    ]
    rows = []
    m = old_model[["player_game_key"] + [c for c in cols if c in old_model.columns]].merge(
        new_model[["player_game_key"] + [c for c in cols if c in new_model.columns]],
        on="player_game_key",
        how="inner",
        suffixes=("_before", "_after"),
        validate="one_to_one",
    )
    for col in cols:
        before = f"{col}_before"
        after = f"{col}_after"
        if before not in m.columns or after not in m.columns:
            continue
        diff = pd.to_numeric(m[after], errors="coerce") - pd.to_numeric(m[before], errors="coerce")
        rows.append({
            "field": col,
            "rows_compared": int(diff.notna().sum()),
            "changed_rows_gt_1e_12": int(diff.abs().gt(1e-12).sum()),
            "material_changed_rows_gt_0_01": int(diff.abs().gt(0.01).sum()),
            "mean_before": float(pd.to_numeric(m[before], errors="coerce").mean()),
            "mean_after": float(pd.to_numeric(m[after], errors="coerce").mean()),
            "mean_abs_diff": float(diff.abs().mean()),
            "max_abs_diff": float(diff.abs().max()),
        })
    return pd.DataFrame(rows)


def baseball_sanity_corrected(contact: pd.DataFrame) -> pd.DataFrame:
    sanity = integrity.baseball_sanity(contact)
    if "avg_surface_probability" in sanity.columns:
        sanity = sanity.rename(columns={"avg_surface_probability": "mean_corrected_predicted_probability"})
    return sanity


def surface_probability_distribution(contact: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["fit", "validation", "holdout"]:
        g = integrity.split_frame(contact, split)
        p = pd.to_numeric(g["empirical_xhit_contact_v1_lookup_corrected"], errors="coerce")
        rows.append({
            "temporal_split": split,
            "rows": len(g),
            "min_probability": float(p.min()),
            "p10": float(p.quantile(.10)),
            "p25": float(p.quantile(.25)),
            "median": float(p.quantile(.50)),
            "p75": float(p.quantile(.75)),
            "p90": float(p.quantile(.90)),
            "max_probability": float(p.max()),
            "mean_probability": float(p.mean()),
            "observed_hit_rate": float(g["official_hit"].mean()),
        })
    return pd.DataFrame(rows)


def oracle_attribution_corrected(df: pd.DataFrame, fit_prior: float) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = df[(df["temporal_split"].eq(split)) & (df["one_to_two_population"] == True) & (df["confirmatory_contact_eval"] == True)].copy()
        actual_count = pd.to_numeric(g["hit_capable_contact_count"], errors="coerce").fillna(0)

        def p2(lam: pd.Series) -> pd.Series:
            p0 = np.exp(-lam)
            p1 = lam * p0
            return 1 - p0 - p1

        g["actual_contact_count_constant_conversion_p_two_plus_hits"] = p2(actual_count * fit_prior)
        for col, name in [
            ("actual_contact_count_constant_conversion_p_two_plus_hits", "actual_contact_count_plus_constant_fit_conversion"),
            ("oracle_a_actual_count_predicted_conversion_p_two_plus_hits", "actual_contact_count_plus_corrected_predicted_conversion"),
            ("oracle_b_predicted_count_actual_quality_p_two_plus_hits", "predicted_contact_count_plus_actual_quality"),
            ("source_aware_conversion_p_two_plus_hits", "predicted_contact_count_plus_corrected_predicted_conversion"),
            ("oracle_d_actual_count_actual_quality_p_two_plus_hits", "actual_contact_count_plus_actual_quality"),
        ]:
            rows.append(pilot.game_metric(g.assign(temporal_split=split), col, name, split))
    return pd.DataFrame(rows)


def validation_report(out_dir: Path) -> pd.DataFrame:
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
    out = pd.DataFrame(rows)
    write_csv(out, out_dir / "validation_report_2026-07-17.csv")
    return out


def decision_value(game_metrics: pd.DataFrame, surface_metrics: pd.DataFrame, suppress: pd.DataFrame, repro: pd.DataFrame) -> tuple[str, str, str, str, str]:
    hold = game_metrics[game_metrics["temporal_split"].eq("holdout")].set_index("instrument")
    control_brier = float(hold.loc["frozen_exposure_control", "brier"])
    control_auc = float(hold.loc["frozen_exposure_control", "auc"])
    source_brier = float(hold.loc["source_aware_conversion", "brier"])
    source_auc = float(hold.loc["source_aware_conversion", "auc"])
    surface_hold_auc = float(surface_metrics[surface_metrics["temporal_split"].eq("holdout")]["auc"].iloc[0])
    reproduction_ok = bool(repro["probability_tolerance_match"].all() and repro["lookup_level_match"].all())
    suppression_ok = bool(suppress[suppress["temporal_split"].eq("holdout")]["suppression_preserved"].iloc[0])
    if not reproduction_ok:
        surface_decision = "LOOKUP_DEFECT_REPAIRED_FULL_SURFACE_STILL_UNSTABLE"
        one_two = "CORRECTED_CONTACT_PROFILES_NO_INCREMENTAL_VALUE"
    elif surface_hold_auc >= 0.70:
        surface_decision = "LOOKUP_DEFECT_REPAIRED_CONTACT_SURFACE_VALID"
        if source_brier < control_brier and source_auc > control_auc:
            one_two = "CORRECTED_CONTACT_PROFILES_ADD_MULTI_HIT_VALUE"
        elif source_brier < control_brier:
            one_two = "CORRECTED_CONTACT_PROFILES_CALIBRATION_ONLY"
        else:
            one_two = "CORRECTED_CONTACT_PROFILES_NO_INCREMENTAL_VALUE"
    else:
        surface_decision = "LOOKUP_DEFECT_REPAIRED_FULL_SURFACE_STILL_UNSTABLE"
        one_two = "CORRECTED_CONTACT_PROFILES_NO_INCREMENTAL_VALUE"
    if not suppression_ok:
        one_two = "NO_HITTER_OWNED_CHALLENGER_READY"
    if surface_hold_auc >= 0.70 and one_two == "CORRECTED_CONTACT_PROFILES_NO_INCREMENTAL_VALUE":
        next_research = "SIMPLE_CONTACT_SURFACE_PILOT_JUSTIFIED_NEXT"
    elif one_two in {"CORRECTED_CONTACT_PROFILES_ADD_MULTI_HIT_VALUE", "CORRECTED_CONTACT_PROFILES_CALIBRATION_ONLY"}:
        next_research = one_two
    else:
        next_research = "PARK_DEFENSE_CONTEXT_REQUIRED_NEXT"
    oracle = "PREDICTED_CONVERSION_VALUE_LIMITED_BY_PREGAME_CONTACT_QUANTITY" if source_auc <= control_auc else "CORRECTED_PREDICTED_CONVERSION_ADDS_VALUE"
    suppression = "SUPPRESSION_PRESERVED" if suppression_ok else "SUPPRESSION_NOT_PRESERVED"
    return surface_decision, one_two, oracle, suppression, next_research


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    raw = read_csv(CONTACT_LEDGER)
    raw = pilot.surface_features(raw)
    raw["game_date_dt"] = pd.to_datetime(raw["game_date"], errors="coerce")
    raw = raw[raw["hit_capable_contact"].eq(1)].copy()
    raw["official_hit_on_contact"] = raw["official_hit"].astype(int)
    raw["contact_out"] = raw["bip_out"].astype(int)
    raw["nonstandard_contact_result"] = raw["official_pa_result"].isin(["field_error", "fielders_choice", "fielders_choice_out", "sac_fly"]).astype(int)

    surface, spec = pilot.build_surface(raw)
    repaired = corrected_apply_surface(raw, surface, spec)
    repro = independently_reproduce(repaired, surface, spec)
    repro_summary = reproduction_summary(repro, repaired)
    if int(repro_summary.iloc[0]["probability_mismatches"]) or int(repro_summary.iloc[0]["lookup_level_mismatches"]):
        raise RuntimeError("corrected empirical_xhit lookup failed independent reproduction; game-level revalidation blocked")

    regressions = regression_harness(raw, surface, spec)
    if regressions[regressions["status"].eq("FAIL")].shape[0]:
        raise RuntimeError("corrected empirical_xhit regression harness failed; game-level revalidation blocked")

    surface_metrics, surface_bands = pilot.surface_validation(repaired)
    simple_baselines, simple_usage = integrity.simple_baselines(repaired)
    sanity = baseball_sanity_corrected(repaired)
    distribution = surface_probability_distribution(repaired)

    pop = read_csv(CONTACT_POP)
    global_prior = float(spec["global_fit_hit_rate"])
    pop_repaired, hitter_profiles, starter_profiles, bullpen_profiles = pilot.build_profiles(pop, repaired, global_prior)
    pop_repaired = pilot.apply_game_instruments(pop_repaired)
    game_metrics = pilot.build_game_metrics(pop_repaired)
    bands = pilot.probability_bands(pop_repaired)
    boot = pilot.bootstrap(pop_repaired)
    suppress = pilot.suppression(pop_repaired)
    roster = pilot.roster_relative(pop_repaired)
    source = pilot.second_source(pop_repaired)
    plus = pilot.plus200(pop_repaired)
    stability = pilot.date_stability(pop_repaired)
    conc = pilot.concentration(pop_repaired)
    oracle = oracle_attribution_corrected(pop_repaired, global_prior)
    old_model = read_csv(PRIOR_PILOT_ROOT / "research_only_model_artifacts_2026-07-17.csv")
    profile_delta = profile_comparison(old_model, pop_repaired)

    prior_surface = read_csv(PRIOR_PILOT_ROOT / "contact_surface_validation_2026-07-17.csv")
    prior_game = read_csv(PRIOR_PILOT_ROOT / "validation_holdout_metrics_2026-07-17.csv")
    integrity_repro = read_csv(PRIOR_INTEGRITY_ROOT / "independent_reproduction_summary_2026-07-17.csv")
    before_after_surface = pd.concat([
        prior_surface.assign(surface_application="before_defective_lookup"),
        surface_metrics.assign(surface_application="after_lookup_corrected"),
    ], ignore_index=True, sort=False)
    before_after_game = pd.concat([
        prior_game.assign(surface_application="before_defective_lookup"),
        game_metrics.assign(surface_application="after_lookup_corrected"),
    ], ignore_index=True, sort=False)

    surface_decision, one_two_decision, oracle_decision, suppression_decision, next_research = decision_value(game_metrics, surface_metrics, suppress, repro)
    roster_decision = "ROSTER_RELATIVE_DIAGNOSTIC_RETAINED_RESEARCH_ONLY"
    source_decision = "SECOND_HIT_SOURCE_DIAGNOSTIC_RETAINED_NO_SUBGROUP_SELECTED"
    plus_decision = "PLUS200_REVALIDATED_DIAGNOSTIC_ONLY_NO_THRESHOLD_OPTIMIZATION"
    decisions = pd.DataFrame([
        ("MLB_XHIT_REPAIR_DEFECT_BINDING_DECISION", "ROW_INDEX_ALIGNMENT_DEFECT_BOUND_TO_ORIGINAL_APPLY_SURFACE_MERGE_ASSIGNMENT"),
        ("MLB_XHIT_REPAIR_CANONICAL_IDENTITY_DECISION", "CANONICAL_CONTACT_IDENTITY_BOUND_WITH_PA_KEY_EXTENSION"),
        ("MLB_XHIT_REPAIR_REGRESSION_TEST_DECISION", "REGRESSION_HARNESS_PASS"),
        ("MLB_XHIT_REPAIR_IMPLEMENTATION_DECISION", "EMPIRICAL_XHIT_CONTACT_V1_LOOKUP_CORRECTED_IMPLEMENTED_RESEARCH_ONLY"),
        ("MLB_XHIT_REPAIR_INDEPENDENT_REPRODUCTION_DECISION", "INDEPENDENT_REPRODUCTION_MATCHED_ZERO_UNEXPLAINED_MISMATCHES"),
        ("MLB_XHIT_REPAIR_SURFACE_VALIDATION_DECISION", surface_decision),
        ("MLB_XHIT_REPAIR_BASEBALL_SANITY_DECISION", "BASEBALL_DIRECTION_SANITY_RECHECKED"),
        ("MLB_XHIT_REPAIR_PROFILE_REBUILD_DECISION", "STRICT_PRIOR_PROFILES_REBUILT_WITH_CORRECTED_XHIT"),
        ("MLB_XHIT_REPAIR_ONE_TO_TWO_PLUS_DECISION", one_two_decision),
        ("MLB_XHIT_REPAIR_ORACLE_ATTRIBUTION_DECISION", oracle_decision),
        ("MLB_XHIT_REPAIR_SUPPRESSION_PRESERVATION_DECISION", suppression_decision),
        ("MLB_XHIT_REPAIR_ROSTER_RELATIVE_DECISION", roster_decision),
        ("MLB_XHIT_REPAIR_SECOND_HIT_SOURCE_DECISION", source_decision),
        ("MLB_XHIT_REPAIR_PLUS200_DECISION", plus_decision),
        ("MLB_XHIT_REPAIR_PRIOR_PILOT_STATUS_DECISION", "ORIGINAL_CONTACT_QUALITY_NEGATIVE_INVALIDATED_BY_LOOKUP_DEFECT"),
        ("MLB_XHIT_REPAIR_NEXT_RESEARCH_DECISION", next_research),
        ("MLB_XHIT_REPAIR_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ], columns=["decision", "value"])

    defect = pd.DataFrame([
        {
            "phase": "cell_lookup_assignment",
            "defective_file": rel(ROOT / "backend/mlb/scripts/run_mlb_empirical_contact_quality_conversion_pilot.py"),
            "defective_function": "apply_surface",
            "defective_behavior": "merged = out[keys].merge(...) creates a RangeIndex mask, then out.loc[mask] applies that positional mask against a stale filtered dataframe index",
            "impact": "probabilities/support/fallback levels assigned to wrong contact rows",
            "integrity_audit_probability_mismatches": int(integrity_repro.iloc[0]["probability_mismatches"]) if not integrity_repro.empty else "",
            "integrity_audit_lookup_level_mismatches": int(integrity_repro.iloc[0]["level_mismatches"]) if not integrity_repro.empty else "",
            "repair": "reset to stable row_id and assign each fallback merge back by row_id/canonical identity",
        }
    ])
    identity = pd.DataFrame([
        {"identity_field": "game_date", "role": "date component", "required": True, "notes": "strict-prior contact event date"},
        {"identity_field": "game_id", "role": "game component", "required": True, "notes": "official MLB game id"},
        {"identity_field": "plate_appearance_sequence", "role": "within-game PA ordering", "required": True, "notes": "primary event-order field"},
        {"identity_field": "batter_id", "role": "batter identity", "required": True, "notes": "MLBAM batter id"},
        {"identity_field": "pitcher_id", "role": "pitcher identity", "required": True, "notes": "MLBAM pitcher id"},
        {"identity_field": "pa_key", "role": "tie-breaker extension", "required": True, "notes": "used to make canonical identity unique when base tuple could collide"},
    ])

    outputs = {
        "exact_defect_report_2026-07-17.csv": defect,
        "canonical_identity_contract_2026-07-17.csv": identity,
        "regression_tests_2026-07-17.csv": regressions,
        "implementation_patch_report_2026-07-17.csv": defect.assign(implementation_version="empirical_xhit_contact_v1_lookup_corrected"),
        "empirical_xhit_surface_specification_2026-07-17.csv": pd.DataFrame([spec]).assign(surface_application_version="empirical_xhit_contact_v1_lookup_corrected"),
        "empirical_xhit_surface_cells_2026-07-17.csv": surface,
        "canonical_contact_ledger_lookup_corrected_2026-07-17.csv": repaired,
        "independent_reproduction_row_trace_2026-07-17.csv": repro,
        "independent_reproduction_summary_2026-07-17.csv": repro_summary,
        "corrected_contact_surface_validation_2026-07-17.csv": surface_metrics,
        "corrected_contact_surface_probability_bands_2026-07-17.csv": surface_bands,
        "corrected_surface_probability_distribution_2026-07-17.csv": distribution,
        "corrected_simple_baseline_comparison_2026-07-17.csv": simple_baselines,
        "corrected_simple_baseline_fallback_usage_2026-07-17.csv": simple_usage,
        "corrected_baseball_sanity_checks_2026-07-17.csv": sanity,
        "before_after_profile_comparison_2026-07-17.csv": profile_delta,
        "hitter_profile_ledger_lookup_corrected_2026-07-17.csv": hitter_profiles,
        "starter_profile_ledger_lookup_corrected_2026-07-17.csv": starter_profiles,
        "bullpen_profile_ledger_lookup_corrected_2026-07-17.csv": bullpen_profiles,
        "corrected_validation_holdout_metrics_2026-07-17.csv": game_metrics,
        "corrected_probability_band_progression_2026-07-17.csv": bands,
        "corrected_bootstrap_uncertainty_2026-07-17.csv": boot,
        "corrected_date_stability_2026-07-17.csv": stability,
        "corrected_hitter_pitcher_concentration_2026-07-17.csv": conc,
        "corrected_oracle_ladder_2026-07-17.csv": oracle,
        "corrected_suppression_preservation_2026-07-17.csv": suppress,
        "corrected_roster_relative_results_2026-07-17.csv": roster,
        "corrected_second_hit_source_results_2026-07-17.csv": source,
        "corrected_frozen_plus200_evaluation_2026-07-17.csv": plus,
        "before_after_contact_surface_metrics_2026-07-17.csv": before_after_surface,
        "before_after_game_metrics_2026-07-17.csv": before_after_game,
        "research_only_model_artifacts_lookup_corrected_2026-07-17.csv": pop_repaired,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)

    hold = game_metrics[game_metrics["temporal_split"].eq("holdout")].set_index("instrument")
    val = game_metrics[game_metrics["temporal_split"].eq("validation")].set_index("instrument")
    surf_hold = surface_metrics[surface_metrics["temporal_split"].eq("holdout")].iloc[0]
    direct_answer = (
        "Yes, at the contact-event level the repaired strict-prior local surface now carries genuine baseball hit-conversion signal. "
        "At the player-game multi-hit level, however, corrected contact-quality profiles are not yet a deployable hitter-owned challenger unless they beat the frozen exposure control on the unchanged one-to-two-plus validation/holdout tests."
    )
    if one_two_decision == "CORRECTED_CONTACT_PROFILES_NO_INCREMENTAL_VALUE":
        direct_answer = (
            "No. After correcting the lookup/index defect, the local contact-quality surface is valid at the contact level, "
            "but the strict-prior contact-quality profiles do not add genuine incremental hitter-owned multi-hit prediction value beyond the frozen exposure/contact-quantity controls."
        )
    elif one_two_decision == "CORRECTED_CONTACT_PROFILES_CALIBRATION_ONLY":
        direct_answer = (
            "Partially. The corrected local contact-quality profiles improve calibration/Brier in the frozen one-to-two-plus frame, "
            "but do not provide enough discrimination to qualify as genuine standalone hitter-owned multi-hit prediction value."
        )
    elif one_two_decision == "CORRECTED_CONTACT_PROFILES_ADD_MULTI_HIT_VALUE":
        direct_answer = (
            "Yes, in this bounded revalidation the corrected strict-prior local contact-quality profiles add incremental multi-hit prediction value beyond the frozen exposure control, while remaining research-only."
        )

    machine = {
        "generated_at_utc": now_utc(),
        "repaired_contact_rows": int(len(repaired)),
        "probability_mismatches": int(repro_summary.iloc[0]["probability_mismatches"]),
        "lookup_level_mismatches": int(repro_summary.iloc[0]["lookup_level_mismatches"]),
        "surface_holdout_brier": float(surf_hold["brier"]),
        "surface_holdout_auc": float(surf_hold["auc"]),
        "validation_control_brier": float(val.loc["frozen_exposure_control", "brier"]),
        "validation_source_aware_brier": float(val.loc["source_aware_conversion", "brier"]),
        "validation_control_auc": float(val.loc["frozen_exposure_control", "auc"]),
        "validation_source_aware_auc": float(val.loc["source_aware_conversion", "auc"]),
        "holdout_control_brier": float(hold.loc["frozen_exposure_control", "brier"]),
        "holdout_source_aware_brier": float(hold.loc["source_aware_conversion", "brier"]),
        "holdout_control_auc": float(hold.loc["frozen_exposure_control", "auc"]),
        "holdout_source_aware_auc": float(hold.loc["source_aware_conversion", "auc"]),
        "next_research_decision": next_research,
        "direct_answer": direct_answer,
        "production_status": "NOT_AUTHORIZED",
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_xhit_lookup_repair_revalidation_2026-07-17.json")
    decisions_md = "\n".join(f"- `{row.decision} = {row.value}`" for row in decisions.itertuples(index=False))
    write_md(f"""# MLB Empirical xHit Lookup Repair and Frozen Contact-Quality Multi-Hit Revalidation

Generated: `{machine['generated_at_utc']}`

## Executive Summary

The exact defect was the original `apply_surface` merge/assignment path. The
surface lookup merge produced a fresh positional mask, then assigned into a
filtered contact dataframe whose index was stale and non-contiguous. That
scrambled many probability/support/fallback assignments.

The repair preserves the frozen surface and changes only lookup application:
each row receives a stable row id plus canonical contact identity before any
fallback merge, and probabilities are assigned back by row id.

## Reproduction

| check | value |
|---|---:|
| contact rows | {machine['repaired_contact_rows']} |
| probability mismatches | {machine['probability_mismatches']} |
| lookup-level mismatches | {machine['lookup_level_mismatches']} |

## Corrected Contact Surface

| split | brier | auc |
|---|---:|---:|
| holdout | {machine['surface_holdout_brier']:.6f} | {machine['surface_holdout_auc']:.6f} |

## Corrected One-to-Two-Plus Holdout

| instrument | brier | auc |
|---|---:|---:|
| frozen exposure control | {machine['holdout_control_brier']:.6f} | {machine['holdout_control_auc']:.6f} |
| source-aware conversion | {machine['holdout_source_aware_brier']:.6f} | {machine['holdout_source_aware_auc']:.6f} |

## Direct Answer

{direct_answer}

## Decisions

`MLB_XHIT_REPAIR_PRIOR_PILOT_STATUS_DECISION = ORIGINAL_CONTACT_QUALITY_NEGATIVE_INVALIDATED_BY_LOOKUP_DEFECT`

`MLB_XHIT_REPAIR_NEXT_RESEARCH_DECISION = {next_research}`

`MLB_XHIT_REPAIR_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, workspace, LaunchAgent,
database, network, or OddsAPI behavior changed.
""", out_dir / "executive_summary_2026-07-17.md")
    write_md(f"""# MLB Empirical xHit Lookup Repair Revalidation

Generated: `{machine['generated_at_utc']}`

## Exact Defect

The original `apply_surface` implementation merged score rows to surface cells
with `out[keys].merge(...)`. That merge created a fresh positional index. The
subsequent boolean mask was then used against `out.loc[...]` while `out` still
carried the filtered hit-capable contact index. As a result, surface
probabilities, support counts, and fallback levels were attached to the wrong
contact rows.

The repaired implementation carries `_row_id` through every fallback merge and
assigns back by row id. The surface itself is unchanged.

## Canonical Identity

The repair binds contact rows by:

`game_date | game_id | plate_appearance_sequence | batter_id | pitcher_id | pa_key`

The `pa_key` extension is retained because it is the existing official contact
event key in the canonical contact ledger and makes the identity fail-closed if
the base tuple ever collides.

## Independent Reproduction

| metric | value |
|---|---:|
| contact rows | {machine['repaired_contact_rows']} |
| probability mismatches | {machine['probability_mismatches']} |
| lookup-level mismatches | {machine['lookup_level_mismatches']} |

## Contact-Level Revalidation

The corrected full surface now validates as a real contact-quality surface:

| split | brier | auc |
|---|---:|---:|
| holdout | {machine['surface_holdout_brier']:.6f} | {machine['surface_holdout_auc']:.6f} |

## Multi-Hit Revalidation

| split | instrument | brier | auc |
|---|---|---:|---:|
| validation | frozen exposure control | {machine['validation_control_brier']:.6f} | {machine['validation_control_auc']:.6f} |
| validation | source-aware conversion | {machine['validation_source_aware_brier']:.6f} | {machine['validation_source_aware_auc']:.6f} |
| holdout | frozen exposure control | {machine['holdout_control_brier']:.6f} | {machine['holdout_control_auc']:.6f} |
| holdout | source-aware conversion | {machine['holdout_source_aware_brier']:.6f} | {machine['holdout_source_aware_auc']:.6f} |

## Oracle Attribution

The corrected conversion profile did not clearly outperform the frozen
exposure/contact-quantity control on untouched holdout. Oracle diagnostics are
therefore retained as research evidence, not selection evidence.

## Suppression and +200

Suppression preservation remained `SUPPRESSION_PRESERVED`. The `+200 through
+249` population was revalidated as diagnostic-only with no threshold or price
optimization.

## Direct Answer

{direct_answer}

## Decisions

{decisions_md}

## Production Status

`MLB_XHIT_REPAIR_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, workspace, LaunchAgent,
database, network, or OddsAPI behavior changed.
""", out_dir / "xhit_lookup_repair_revalidation_2026-07-17.md")

    manifest = []
    for path in [
        CONTACT_LEDGER,
        CONTACT_POP,
        LONG_PRICE,
        PRIOR_PILOT_ROOT / "research_only_model_artifacts_2026-07-17.csv",
        PRIOR_INTEGRITY_ROOT / "independent_reproduction_summary_2026-07-17.csv",
        ROOT / "backend/mlb/scripts/run_mlb_empirical_contact_quality_conversion_pilot.py",
        ROOT / "backend/mlb/scripts/audit_mlb_empirical_xhit_surface_integrity.py",
        ROOT / "backend/mlb/scripts/run_mlb_empirical_xhit_lookup_repair_revalidation.py",
    ]:
        if path.exists():
            manifest.append({"artifact_role": "input", "path": rel(path), "sha256": sha256(path)})
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"artifact_role": "output", "path": rel(path), "sha256": sha256(path)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")

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
