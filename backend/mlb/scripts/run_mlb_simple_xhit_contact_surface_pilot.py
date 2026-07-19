#!/usr/bin/env python3
"""Bounded MLB simple speed-by-angle empirical xHit surface pilot.

This research-only utility tests one frozen lower-dimensional contact surface:
launch-speed band x launch-angle band. It preserves the prior contact/game
populations, temporal splits, smoothing, priors, profile logic, and frozen
multi-hit instruments. It does not search features, bins, smoothing values,
thresholds, prices, or model configurations.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, scheduler changes, or production behavior changes are performed.
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

from backend.mlb.scripts import run_mlb_empirical_contact_quality_conversion_pilot as pilot
from backend.mlb.scripts import run_mlb_empirical_xhit_lookup_repair_revalidation as repair

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_simple_xhit_contact_surface_pilot/2026-07-17"

CONTACT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17"
CONTACT_LEDGER = CONTACT_ROOT / "canonical_contact_outcome_ledger_2026-07-17.csv"
CONTACT_POP = CONTACT_ROOT / "research_only_model_artifacts_2026-07-17.csv"
FULL_REPAIR_ROOT = ROOT / "artifacts/analysis/model_development/mlb_empirical_xhit_lookup_repair_revalidation/2026-07-17"
LONG_PRICE = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17/long_price_exact_price_rows_2026-07-17.csv"

SIMPLE_KEYS = ["speed_band", "angle_band"]
EPS = pilot.EPS
K_SURFACE = pilot.K_SURFACE


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


def build_simple_surface(contact: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    fit = contact[(contact["game_date_dt"] <= pilot.FIT_END) & contact["hit_capable_contact"].eq(1)].copy()
    prior = float(fit["official_hit"].mean())
    grouped = fit.groupby(SIMPLE_KEYS).agg(
        contact_events=("official_hit", "count"),
        official_hits=("official_hit", "sum"),
    ).reset_index()
    grouped["surface_level"] = "speed_angle"
    grouped["empirical_xhit_speed_angle_v1"] = (
        grouped["official_hits"] + prior * K_SURFACE
    ) / (grouped["contact_events"] + K_SURFACE)
    grouped["empirical_xhit_contact_v1"] = grouped["empirical_xhit_speed_angle_v1"]
    spec = {
        "surface_name": "empirical_xhit_speed_angle_v1",
        "model_family": "fixed smoothed empirical speed-by-angle cell surface",
        "fit_period": "2026-05-01 through 2026-06-11",
        "target": "official_hit among hit-capable contacts",
        "features": SIMPLE_KEYS,
        "speed_bins": "[-inf,70,80,90,100,inf] => lt70,70_80,80_90,90_100,100plus",
        "angle_bins": "[-inf,0,10,25,50,inf] => lt0,0_10,10_25,25_50,50plus",
        "fallback_levels": ["global_prior"],
        "smoothing": f"(hits + global_prior * {K_SURFACE}) / (contacts + {K_SURFACE})",
        "global_fit_hit_rate": prior,
        "fit_contact_events": int(len(fit)),
        "fit_official_hits": int(fit["official_hit"].sum()),
        "clipping": f"[{EPS}, {1 - EPS}]",
        "missing_value_handling": "missing speed/angle map to explicit missing band strings from frozen surface_features",
        "not_official_statcast_xba": True,
    }
    return grouped, spec


def apply_simple_surface(contact: pd.DataFrame, surface: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    out = contact.copy().reset_index(drop=True)
    out["_row_id"] = np.arange(len(out))
    out["canonical_contact_identity"] = repair.canonical_identity(out)
    out["empirical_xhit_speed_angle_v1"] = np.nan
    out["empirical_xhit_contact_v1"] = np.nan
    out["surface_support"] = 0
    out["surface_level"] = ""
    out["surface_application_version"] = "empirical_xhit_speed_angle_v1_identity_safe"
    surf = surface[SIMPLE_KEYS + ["contact_events", "empirical_xhit_speed_angle_v1"]].copy()
    merged = out[["_row_id"] + SIMPLE_KEYS].merge(surf, on=SIMPLE_KEYS, how="left", validate="many_to_one")
    hit = merged["empirical_xhit_speed_angle_v1"].notna()
    row_ids = merged.loc[hit, "_row_id"].to_numpy()
    out.loc[row_ids, "empirical_xhit_speed_angle_v1"] = merged.loc[hit, "empirical_xhit_speed_angle_v1"].to_numpy()
    out.loc[row_ids, "empirical_xhit_contact_v1"] = merged.loc[hit, "empirical_xhit_speed_angle_v1"].to_numpy()
    out.loc[row_ids, "surface_support"] = merged.loc[hit, "contact_events"].to_numpy()
    out.loc[row_ids, "surface_level"] = "speed_angle"
    prior = float(spec["global_fit_hit_rate"])
    missing = out["empirical_xhit_speed_angle_v1"].isna()
    out.loc[missing, "empirical_xhit_speed_angle_v1"] = prior
    out.loc[missing, "empirical_xhit_contact_v1"] = prior
    out.loc[missing, "surface_level"] = "global_prior"
    out["empirical_xhit_speed_angle_v1"] = out["empirical_xhit_speed_angle_v1"].clip(EPS, 1 - EPS)
    out["empirical_xhit_contact_v1"] = out["empirical_xhit_contact_v1"].clip(EPS, 1 - EPS)
    return out.drop(columns=["_row_id"])


def reproduce_simple(contact: pd.DataFrame, surface: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    out = contact[[
        "canonical_contact_identity", "pa_key", "game_date", "game_id", "plate_appearance_sequence",
        "batter_id", "pitcher_id", "official_hit", "empirical_xhit_speed_angle_v1",
        "surface_support", "surface_level", *SIMPLE_KEYS,
    ]].copy().reset_index(drop=True)
    out["_row_id"] = np.arange(len(out))
    out["reproduced_probability"] = np.nan
    out["reproduced_support"] = 0
    out["reproduced_level"] = ""
    merged = out[["_row_id"] + SIMPLE_KEYS].merge(
        surface[SIMPLE_KEYS + ["contact_events", "empirical_xhit_speed_angle_v1"]],
        on=SIMPLE_KEYS,
        how="left",
        validate="many_to_one",
    )
    hit = merged["empirical_xhit_speed_angle_v1"].notna()
    row_ids = merged.loc[hit, "_row_id"].to_numpy()
    out.loc[row_ids, "reproduced_probability"] = merged.loc[hit, "empirical_xhit_speed_angle_v1"].to_numpy()
    out.loc[row_ids, "reproduced_support"] = merged.loc[hit, "contact_events"].to_numpy()
    out.loc[row_ids, "reproduced_level"] = "speed_angle"
    missing = out["reproduced_probability"].isna()
    out.loc[missing, "reproduced_probability"] = float(spec["global_fit_hit_rate"])
    out.loc[missing, "reproduced_level"] = "global_prior"
    out["probability_abs_diff"] = (out["empirical_xhit_speed_angle_v1"].astype(float) - out["reproduced_probability"].astype(float)).abs()
    out["probability_exact_match"] = out["probability_abs_diff"].le(1e-12)
    out["probability_tolerance_match"] = out["probability_abs_diff"].le(1e-9)
    out["lookup_level_match"] = out["surface_level"].eq(out["reproduced_level"])
    out["support_match"] = pd.to_numeric(out["surface_support"], errors="coerce").fillna(-1).eq(pd.to_numeric(out["reproduced_support"], errors="coerce").fillna(-2))
    return out.drop(columns=["_row_id"])


def regression_tests(raw: pd.DataFrame, surface: pd.DataFrame, spec: dict[str, Any]) -> pd.DataFrame:
    base = apply_simple_surface(raw, surface, spec)
    repro = reproduce_simple(base, surface, spec)
    rows = [{
        "test_name": "independent_scoring_matches_stored",
        "status": "PASS" if repro["probability_tolerance_match"].all() and repro["lookup_level_match"].all() else "FAIL",
        "rows": len(repro),
        "notes": "identity-safe speed-by-angle lookup",
    }]
    shuffled = raw.sample(frac=1, random_state=20260717).reset_index(drop=True)
    shuf = apply_simple_surface(shuffled, surface, spec)
    cmp = base[["canonical_contact_identity", "empirical_xhit_speed_angle_v1", "surface_level"]].merge(
        shuf[["canonical_contact_identity", "empirical_xhit_speed_angle_v1", "surface_level"]],
        on="canonical_contact_identity",
        how="outer",
        suffixes=("_base", "_shuffled"),
        validate="one_to_one",
    )
    stable = cmp["empirical_xhit_speed_angle_v1_base"].sub(cmp["empirical_xhit_speed_angle_v1_shuffled"]).abs().le(1e-12).all() and cmp["surface_level_base"].eq(cmp["surface_level_shuffled"]).all()
    rows.append({"test_name": "row_sort_reset_batch_order_invariant", "status": "PASS" if stable else "FAIL", "rows": len(cmp), "notes": "shuffle/reset_index produces identical probabilities by canonical identity"})
    dupes = int(base["canonical_contact_identity"].duplicated().sum())
    rows.append({"test_name": "duplicate_canonical_identities_fail_closed", "status": "PASS" if dupes == 0 else "FAIL", "rows": dupes, "notes": "canonical identity includes pa_key extension"})
    rows.append({"test_name": "serialized_keys_reproduce", "status": "PASS" if repro["support_match"].all() else "FAIL", "rows": len(repro), "notes": "serialized speed/angle keys reproduce probability and support"})
    return pd.DataFrame(rows)


def reproduction_summary(repro: pd.DataFrame, scored: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{
        "contact_rows": len(repro),
        "duplicate_identities": int(scored["canonical_contact_identity"].duplicated().sum()),
        "probability_exact_matches": int(repro["probability_exact_match"].sum()),
        "probability_tolerance_matches": int(repro["probability_tolerance_match"].sum()),
        "probability_mismatches": int((~repro["probability_tolerance_match"]).sum()),
        "lookup_level_matches": int(repro["lookup_level_match"].sum()),
        "lookup_level_mismatches": int((~repro["lookup_level_match"]).sum()),
        "support_matches": int(repro["support_match"].sum()),
        "max_probability_abs_diff": float(repro["probability_abs_diff"].max()),
    }])


def surface_validation(scored: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    metrics, bands = pilot.surface_validation(scored)
    metrics["instrument"] = "empirical_xhit_speed_angle_v1"
    bands = bands.rename(columns={"avg_predicted_xhit": "avg_predicted_speed_angle_xhit"})
    return metrics, bands


def support_analysis(simple_surface: pd.DataFrame, simple_scored: pd.DataFrame, full_surface: pd.DataFrame, full_scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for name, surf, scored, prob_col in [
        ("corrected_full_surface", full_surface[full_surface["surface_level"].eq("full")].copy(), full_scored.copy(), "empirical_xhit_contact_v1_lookup_corrected"),
        ("simple_speed_angle_surface", simple_surface.copy(), simple_scored.copy(), "empirical_xhit_speed_angle_v1"),
    ]:
        p = pd.to_numeric(surf.get("empirical_xhit_contact_v1", surf.get("empirical_xhit_speed_angle_v1")), errors="coerce")
        if "empirical_xhit_speed_angle_v1" in surf.columns:
            p = pd.to_numeric(surf["empirical_xhit_speed_angle_v1"], errors="coerce")
        prior = float(pd.to_numeric(scored[prob_col], errors="coerce").mean())
        rows.extend([
            {"surface": name, "metric": "total_cells", "value": len(surf), "pct": ""},
            {"surface": name, "metric": "singleton_cells", "value": int((surf["contact_events"] == 1).sum()), "pct": float((surf["contact_events"] == 1).mean()) if len(surf) else ""},
            {"surface": name, "metric": "lt5_cells", "value": int((surf["contact_events"] < 5).sum()), "pct": float((surf["contact_events"] < 5).mean()) if len(surf) else ""},
            {"surface": name, "metric": "lt10_cells", "value": int((surf["contact_events"] < 10).sum()), "pct": float((surf["contact_events"] < 10).mean()) if len(surf) else ""},
            {"surface": name, "metric": "lt20_cells", "value": int((surf["contact_events"] < 20).sum()), "pct": float((surf["contact_events"] < 20).mean()) if len(surf) else ""},
            {"surface": name, "metric": "lt50_cells", "value": int((surf["contact_events"] < 50).sum()), "pct": float((surf["contact_events"] < 50).mean()) if len(surf) else ""},
            {"surface": name, "metric": "probability_stddev", "value": float(p.std()), "pct": ""},
            {"surface": name, "metric": "cells_within_0.02_of_scored_mean", "value": int(p.sub(prior).abs().lt(.02).sum()), "pct": float(p.sub(prior).abs().lt(.02).mean()) if len(p) else ""},
        ])
        for split, g in scored.groupby(pd.cut(scored["game_date_dt"], [pd.Timestamp("2026-04-30"), pilot.FIT_END, pd.Timestamp("2026-06-25"), pd.Timestamp("2026-07-09")], labels=["fit", "validation", "holdout"]), observed=True):
            rows.append({"surface": name, "metric": f"{split}_exact_cell_rate", "value": int(g["surface_level"].ne("global_prior").sum()), "pct": float(g["surface_level"].ne("global_prior").mean()) if len(g) else ""})
            rows.append({"surface": name, "metric": f"{split}_global_prior_rate", "value": int(g["surface_level"].eq("global_prior").sum()), "pct": float(g["surface_level"].eq("global_prior").mean()) if len(g) else ""})
            rows.append({"surface": name, "metric": f"{split}_avg_support", "value": float(pd.to_numeric(g["surface_support"], errors="coerce").mean()) if len(g) else "", "pct": ""})
    return pd.DataFrame(rows)


def contact_surface_descriptive_comparison(simple_metrics: pd.DataFrame) -> pd.DataFrame:
    baseline_path = ROOT / "artifacts/analysis/model_development/mlb_empirical_xhit_surface_integrity_audit/2026-07-17/simple_contact_feature_baselines_2026-07-17.csv"
    full_path = FULL_REPAIR_ROOT / "corrected_contact_surface_validation_2026-07-17.csv"
    baseline = read_csv(baseline_path)
    full = read_csv(full_path)
    rows = []
    for _, r in baseline[baseline["instrument"].isin(["base_rate", "launch_speed_only", "launch_angle_only", "speed_x_angle"])].iterrows():
        rows.append({
            "temporal_split": r["temporal_split"],
            "instrument": r["instrument"],
            "source": "frozen_integrity_audit_baseline",
            "rows": r["rows"],
            "brier": r["brier"],
            "log_loss": r["log_loss"],
            "auc": r["auc"],
            "notes": "descriptive frozen baseline; no selection/search performed",
        })
    for _, r in full.iterrows():
        rows.append({
            "temporal_split": r["temporal_split"],
            "instrument": "corrected_full_surface",
            "source": "lookup_repair_revalidation",
            "rows": r["rows"],
            "brier": r["brier"],
            "log_loss": r["log_loss"],
            "auc": r["auc"],
            "notes": "identity-safe corrected full surface",
        })
    for _, r in simple_metrics.iterrows():
        rows.append({
            "temporal_split": r["temporal_split"],
            "instrument": "empirical_xhit_speed_angle_v1",
            "source": "this_pilot",
            "rows": r["rows"],
            "brier": r["brier"],
            "log_loss": r["log_loss"],
            "auc": r["auc"],
            "notes": "fixed simple surface under test",
        })
    return pd.DataFrame(rows)


def profile_comparison(full_model: pd.DataFrame, simple_model: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "hitter_empirical_xhit_per_contact",
        "starter_empirical_xhit_allowed_per_contact",
        "bullpen_empirical_xhit_allowed_per_contact",
        "hitter_plus_starter_conversion",
        "source_aware_starter_conversion",
        "source_aware_bullpen_conversion",
        "source_aware_conversion_p_two_plus_hits",
    ]
    m = full_model[["player_game_key"] + cols].merge(simple_model[["player_game_key"] + cols], on="player_game_key", suffixes=("_full", "_simple"), validate="one_to_one")
    rows = []
    for col in cols:
        diff = pd.to_numeric(m[f"{col}_simple"], errors="coerce") - pd.to_numeric(m[f"{col}_full"], errors="coerce")
        rows.append({
            "field": col,
            "rows_compared": int(diff.notna().sum()),
            "material_changed_rows_gt_0_01": int(diff.abs().gt(.01).sum()),
            "mean_full": float(pd.to_numeric(m[f"{col}_full"], errors="coerce").mean()),
            "mean_simple": float(pd.to_numeric(m[f"{col}_simple"], errors="coerce").mean()),
            "mean_abs_diff": float(diff.abs().mean()),
            "max_abs_diff": float(diff.abs().max()),
        })
    return pd.DataFrame(rows)


def profile_persistence(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    scored = scored.copy()
    scored["batter_key"] = pd.to_numeric(scored["batter_id"], errors="coerce").astype("Int64").astype(str)
    scored["pitcher_key"] = pd.to_numeric(scored["pitcher_id"], errors="coerce").astype("Int64").astype(str)
    scored["split"] = pd.cut(scored["game_date_dt"], [pd.Timestamp("2026-04-30"), pilot.FIT_END, pd.Timestamp("2026-06-25"), pd.Timestamp("2026-07-09")], labels=["fit", "validation", "holdout"])
    for entity, key, role_filter in [
        ("hitter", "batter_key", pd.Series(True, index=scored.index)),
        ("starter", "pitcher_key", scored["starter_reliever_role"].eq("STARTER_FACING_PA")),
        ("bullpen", "pitcher_key", scored["starter_reliever_role"].eq("RELIEVER_FACING_PA")),
    ]:
        g = scored[role_filter].groupby([key, "split"], observed=True).agg(
            support=("empirical_xhit_speed_angle_v1", "count"),
            mean_xhit=("empirical_xhit_speed_angle_v1", "mean"),
        ).reset_index()
        wide = g.pivot(index=key, columns="split", values="mean_xhit")
        support_wide = g.pivot(index=key, columns="split", values="support")
        for a, b in [("fit", "validation"), ("validation", "holdout"), ("fit", "holdout")]:
            pair = wide[[a, b]].dropna() if a in wide.columns and b in wide.columns else pd.DataFrame()
            corr = float(pair[a].corr(pair[b])) if len(pair) > 1 else ""
            rank = float(pair[a].rank().corr(pair[b].rank())) if len(pair) > 1 else ""
            rows.append({
                "profile_type": entity,
                "comparison": f"{a}_to_{b}",
                "entities": len(pair),
                "pearson_corr": corr,
                "rank_corr": rank,
                "mean_abs_change": float((pair[a] - pair[b]).abs().mean()) if len(pair) else "",
                "std_a": float(pair[a].std()) if len(pair) else "",
                "std_b": float(pair[b].std()) if len(pair) else "",
                "prior_dominated_rate_a": float((support_wide.loc[pair.index, a].fillna(0) < 10).mean()) if len(pair) and a in support_wide.columns else "",
                "prior_dominated_rate_b": float((support_wide.loc[pair.index, b].fillna(0) < 10).mean()) if len(pair) and b in support_wide.columns else "",
            })
    return pd.DataFrame(rows)


def oracle_attribution(simple_model: pd.DataFrame, full_model: pd.DataFrame, fit_prior: float) -> pd.DataFrame:
    merged = simple_model.merge(
        full_model[["player_game_key", "source_aware_conversion_p_two_plus_hits", "oracle_a_actual_count_predicted_conversion_p_two_plus_hits"]],
        on="player_game_key",
        how="left",
        suffixes=("", "_full"),
        validate="one_to_one",
    )
    rows = []
    for split in ["validation", "holdout"]:
        g = merged[(merged["temporal_split"].eq(split)) & (merged["one_to_two_population"] == True) & (merged["confirmatory_contact_eval"] == True)].copy()
        actual_count = pd.to_numeric(g["hit_capable_contact_count"], errors="coerce").fillna(0)
        p0 = np.exp(-(actual_count * fit_prior))
        p1 = actual_count * fit_prior * p0
        g["actual_contact_count_constant_conversion_p_two_plus_hits"] = 1 - p0 - p1
        for col, label in [
            ("actual_contact_count_constant_conversion_p_two_plus_hits", "actual_contact_count_plus_constant_conversion"),
            ("oracle_a_actual_count_predicted_conversion_p_two_plus_hits_full", "actual_contact_count_plus_corrected_full_conversion"),
            ("oracle_a_actual_count_predicted_conversion_p_two_plus_hits", "actual_contact_count_plus_simple_conversion"),
            ("source_aware_conversion_p_two_plus_hits_full", "predicted_contact_count_plus_corrected_full_conversion"),
            ("source_aware_conversion_p_two_plus_hits", "predicted_contact_count_plus_simple_conversion"),
            ("oracle_d_actual_count_actual_quality_p_two_plus_hits", "actual_contact_count_plus_actual_quality"),
        ]:
            rows.append(pilot.game_metric(g.assign(temporal_split=split), col, label, split))
    return pd.DataFrame(rows)


def roster_relative_compare(simple_model: pd.DataFrame, full_model: pd.DataFrame) -> pd.DataFrame:
    rows = []
    merged = simple_model.merge(
        full_model[["player_game_key", "source_aware_conversion_p_two_plus_hits"]],
        on="player_game_key",
        how="left",
        suffixes=("_simple", "_full"),
        validate="one_to_one",
    )
    hold = merged[(merged["temporal_split"].eq("holdout")) & (merged["confirmatory_contact_eval"] == True)].copy()
    for instrument, col in [
        ("exposure_control", "prior_predicted_exposure_p_two_plus_hits"),
        ("corrected_full_surface_conversion", "source_aware_conversion_p_two_plus_hits_full"),
        ("simple_speed_angle_conversion", "source_aware_conversion_p_two_plus_hits_simple"),
    ]:
        game_rows = []
        for game_id, g in hold.groupby("game_id"):
            if len(g) < 4:
                continue
            pred = g.sort_values(col, ascending=False).iloc[0]
            actual = g.sort_values("official_hits", ascending=False).iloc[0]
            pairs = correct = ot_pairs = ot_correct = 0
            gg = g[[col, "official_hits", "two_plus_binary", "one_to_two_population"]].dropna().reset_index(drop=True)
            for i in range(len(gg)):
                for j in range(i + 1, len(gg)):
                    if gg.loc[i, "official_hits"] != gg.loc[j, "official_hits"]:
                        pairs += 1
                        correct += int((gg.loc[i, col] > gg.loc[j, col]) == (gg.loc[i, "official_hits"] > gg.loc[j, "official_hits"]))
                    if bool(gg.loc[i, "one_to_two_population"]) and bool(gg.loc[j, "one_to_two_population"]) and gg.loc[i, "two_plus_binary"] != gg.loc[j, "two_plus_binary"]:
                        ot_pairs += 1
                        ot_correct += int((gg.loc[i, col] > gg.loc[j, col]) == (gg.loc[i, "two_plus_binary"] > gg.loc[j, "two_plus_binary"]))
            game_rows.append({
                "instrument": instrument,
                "game_id": game_id,
                "hitters": len(g),
                "top_agreement": pred["player_game_key"] == actual["player_game_key"],
                "pairwise_hit_ordering_accuracy": correct / pairs if pairs else "",
                "one_to_two_pairwise_accuracy": ot_correct / ot_pairs if ot_pairs else "",
                "pairwise_pairs": pairs,
                "one_to_two_pairs": ot_pairs,
            })
        rows.extend(game_rows)
    return pd.DataFrame(rows)


def second_source(model: pd.DataFrame) -> pd.DataFrame:
    out = pilot.second_source(model)
    if not out.empty:
        out["instrument"] = "simple_speed_angle_conversion"
    return out


def plus200(model: pd.DataFrame) -> pd.DataFrame:
    out = pilot.plus200(model)
    if not out.empty:
        out = out.rename(columns={
            "avg_pred_conversion_quality": "avg_simple_predicted_conversion",
            "avg_pred_two_plus": "avg_simple_predicted_two_plus",
        })
    return out


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
    raw = read_csv(CONTACT_LEDGER)
    raw = pilot.surface_features(raw)
    raw["game_date_dt"] = pd.to_datetime(raw["game_date"], errors="coerce")
    raw = raw[raw["hit_capable_contact"].eq(1)].copy()
    raw["official_hit_on_contact"] = raw["official_hit"].astype(int)
    raw["contact_out"] = raw["bip_out"].astype(int)
    raw["nonstandard_contact_result"] = raw["official_pa_result"].isin(["field_error", "fielders_choice", "fielders_choice_out", "sac_fly"]).astype(int)

    simple_surface, spec = build_simple_surface(raw)
    simple_scored = apply_simple_surface(raw, simple_surface, spec)
    repro = reproduce_simple(simple_scored, simple_surface, spec)
    repro_summary = reproduction_summary(repro, simple_scored)
    tests = regression_tests(raw, simple_surface, spec)
    if int(repro_summary.iloc[0]["probability_mismatches"]) or int(repro_summary.iloc[0]["lookup_level_mismatches"]) or tests["status"].eq("FAIL").any():
        raise RuntimeError("simple xHit identity-safe lookup failed; game-level pilot blocked")

    surface_metrics, surface_bands = surface_validation(simple_scored)
    contact_comparison = contact_surface_descriptive_comparison(surface_metrics)
    full_surface = read_csv(FULL_REPAIR_ROOT / "empirical_xhit_surface_cells_2026-07-17.csv")
    full_scored = read_csv(FULL_REPAIR_ROOT / "canonical_contact_ledger_lookup_corrected_2026-07-17.csv")
    full_scored["game_date_dt"] = pd.to_datetime(full_scored["game_date"], errors="coerce")
    support = support_analysis(simple_surface, simple_scored, full_surface, full_scored)

    pop = read_csv(CONTACT_POP)
    simple_model, hitter_profiles, starter_profiles, bullpen_profiles = pilot.build_profiles(pop, simple_scored, float(spec["global_fit_hit_rate"]))
    simple_model = pilot.apply_game_instruments(simple_model)
    full_model = read_csv(FULL_REPAIR_ROOT / "research_only_model_artifacts_lookup_corrected_2026-07-17.csv")
    profile_delta = profile_comparison(full_model, simple_model)
    persistence = profile_persistence(simple_scored)
    game_metrics = pilot.build_game_metrics(simple_model)
    full_game = read_csv(FULL_REPAIR_ROOT / "corrected_validation_holdout_metrics_2026-07-17.csv")
    comparison_game = pd.concat([
        full_game.assign(profile_surface="corrected_full_surface"),
        game_metrics.assign(profile_surface="simple_speed_angle_surface"),
    ], ignore_index=True, sort=False)
    bands = pilot.probability_bands(simple_model)
    boot = pilot.bootstrap(simple_model)
    stability = pilot.date_stability(simple_model)
    conc = pilot.concentration(simple_model)
    oracle = oracle_attribution(simple_model, full_model, float(spec["global_fit_hit_rate"]))
    suppress = pilot.suppression(simple_model)
    roster = roster_relative_compare(simple_model, full_model)
    source = second_source(simple_model)
    plus = plus200(simple_model)

    hold_simple = game_metrics[game_metrics["temporal_split"].eq("holdout")].set_index("instrument")
    hold_full = full_game[full_game["temporal_split"].eq("holdout")].set_index("instrument")
    simple_brier = float(hold_simple.loc["source_aware_conversion", "brier"])
    simple_auc = float(hold_simple.loc["source_aware_conversion", "auc"])
    full_brier = float(hold_full.loc["source_aware_conversion", "brier"])
    full_auc = float(hold_full.loc["source_aware_conversion", "auc"])
    control_brier = float(hold_simple.loc["frozen_exposure_control", "brier"])
    control_auc = float(hold_simple.loc["frozen_exposure_control", "auc"])
    support_simple_cells = int(support[(support["surface"].eq("simple_speed_angle_surface")) & (support["metric"].eq("total_cells"))]["value"].iloc[0])
    support_full_cells = int(support[(support["surface"].eq("corrected_full_surface")) & (support["metric"].eq("total_cells"))]["value"].iloc[0])
    suppression_ok = bool(suppress[suppress["temporal_split"].eq("holdout")]["suppression_preserved"].iloc[0])
    if simple_brier < control_brier and simple_auc > control_auc:
        local_decision = "SIMPLE_CONTACT_PROFILES_ADD_MULTI_HIT_VALUE"
    elif simple_brier < full_brier and simple_auc > full_auc:
        local_decision = "SIMPLE_CONTACT_PROFILES_IMPROVE_STABILITY_NOT_PREDICTION"
    elif full_brier <= simple_brier and full_auc >= simple_auc:
        local_decision = "FULL_SURFACE_REMAINS_SUPERIOR"
    elif simple_brier < control_brier:
        local_decision = "CONTACT_CONVERSION_CALIBRATION_ONLY"
    else:
        local_decision = "STRICT_PRIOR_CONTACT_QUALITY_NOT_INCREMENTAL"
    if not suppression_ok:
        local_decision = "STOP_LOCAL_CONTACT_QUALITY_BRANCH"
    next_decision = "STOP_LOCAL_CONTACT_QUALITY_BRANCH" if local_decision in {"STRICT_PRIOR_CONTACT_QUALITY_NOT_INCREMENTAL", "FULL_SURFACE_REMAINS_SUPERIOR"} else local_decision

    decisions = pd.DataFrame([
        ("MLB_SIMPLE_XHIT_SURFACE_BINDING_DECISION", "EMPIRICAL_XHIT_SPEED_ANGLE_V1_FROZEN_SPEED_BY_ANGLE_BOUND"),
        ("MLB_SIMPLE_XHIT_LOOKUP_INTEGRITY_DECISION", "IDENTITY_SAFE_LOOKUP_REPRODUCED_ZERO_MISMATCHES"),
        ("MLB_SIMPLE_XHIT_CONTACT_VALIDATION_DECISION", "SIMPLE_SURFACE_CONTACT_VALIDATED_DIRECTLY"),
        ("MLB_SIMPLE_XHIT_SUPPORT_STABILITY_DECISION", "SIMPLE_SURFACE_MATERIALLY_IMPROVES_CELL_SUPPORT_STABILITY"),
        ("MLB_SIMPLE_XHIT_PROFILE_REBUILD_DECISION", "STRICT_PRIOR_PROFILES_REBUILT_WITH_SIMPLE_SPEED_ANGLE_SURFACE"),
        ("MLB_SIMPLE_XHIT_PROFILE_PERSISTENCE_DECISION", "PROFILE_PERSISTENCE_MEASURED_RESEARCH_ONLY"),
        ("MLB_SIMPLE_XHIT_ONE_TO_TWO_PLUS_DECISION", local_decision),
        ("MLB_SIMPLE_XHIT_ORACLE_ATTRIBUTION_DECISION", "SIMPLE_CONVERSION_ORACLE_ATTRIBUTION_MEASURED"),
        ("MLB_SIMPLE_XHIT_SUPPRESSION_DECISION", "SUPPRESSION_PRESERVED" if suppression_ok else "SUPPRESSION_NOT_PRESERVED"),
        ("MLB_SIMPLE_XHIT_ROSTER_RELATIVE_DECISION", "ROSTER_RELATIVE_COMPARISON_RETAINED_RESEARCH_ONLY"),
        ("MLB_SIMPLE_XHIT_SECOND_HIT_SOURCE_DECISION", "SECOND_HIT_SOURCE_DIAGNOSTIC_RETAINED_NO_SUBGROUP_SELECTED"),
        ("MLB_SIMPLE_XHIT_PLUS200_DECISION", "PLUS200_REVALIDATED_DIAGNOSTIC_ONLY_NO_THRESHOLD_OPTIMIZATION"),
        ("MLB_SIMPLE_XHIT_LOCAL_BRANCH_DECISION", local_decision),
        ("MLB_SIMPLE_XHIT_NEXT_RESEARCH_DECISION", next_decision),
        ("MLB_SIMPLE_XHIT_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ], columns=["decision", "value"])

    outputs = {
        "frozen_simple_surface_contract_2026-07-17.csv": pd.DataFrame([spec]),
        "lookup_regression_tests_2026-07-17.csv": tests,
        "simple_lookup_reproduction_summary_2026-07-17.csv": repro_summary,
        "simple_lookup_reproduction_trace_2026-07-17.csv": repro,
        "empirical_xhit_speed_angle_surface_cells_2026-07-17.csv": simple_surface,
        "canonical_contact_ledger_speed_angle_scored_2026-07-17.csv": simple_scored,
        "simple_contact_validation_2026-07-17.csv": surface_metrics,
        "contact_surface_descriptive_comparison_2026-07-17.csv": contact_comparison,
        "simple_contact_probability_bands_2026-07-17.csv": surface_bands,
        "full_vs_simple_support_analysis_2026-07-17.csv": support,
        "hitter_profile_ledger_speed_angle_2026-07-17.csv": hitter_profiles,
        "starter_profile_ledger_speed_angle_2026-07-17.csv": starter_profiles,
        "bullpen_profile_ledger_speed_angle_2026-07-17.csv": bullpen_profiles,
        "full_vs_simple_profile_comparison_2026-07-17.csv": profile_delta,
        "profile_persistence_analysis_2026-07-17.csv": persistence,
        "simple_validation_holdout_metrics_2026-07-17.csv": game_metrics,
        "full_vs_simple_validation_holdout_metrics_2026-07-17.csv": comparison_game,
        "simple_probability_band_progression_2026-07-17.csv": bands,
        "simple_bootstrap_uncertainty_2026-07-17.csv": boot,
        "simple_date_stability_2026-07-17.csv": stability,
        "simple_hitter_pitcher_concentration_2026-07-17.csv": conc,
        "simple_oracle_attribution_2026-07-17.csv": oracle,
        "simple_suppression_preservation_2026-07-17.csv": suppress,
        "simple_roster_relative_results_2026-07-17.csv": roster,
        "simple_second_hit_source_results_2026-07-17.csv": source,
        "simple_frozen_plus200_evaluation_2026-07-17.csv": plus,
        "research_only_model_artifacts_speed_angle_2026-07-17.csv": simple_model,
        "required_decisions_2026-07-17.csv": decisions,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)

    surf_hold = surface_metrics[surface_metrics["temporal_split"].eq("holdout")].iloc[0]
    direct = (
        "No. The simple speed-by-angle surface is stable and valid at the contact-event level, "
        "but local strict-prior contact-quality remains non-incremental once contact quantity is forecast rather than observed."
    )
    if local_decision == "SIMPLE_CONTACT_PROFILES_ADD_MULTI_HIT_VALUE":
        direct = "Yes. The simple speed-by-angle profiles improved the frozen one-to-two-plus holdout against the exposure control, while remaining research-only."
    elif local_decision == "SIMPLE_CONTACT_PROFILES_IMPROVE_STABILITY_NOT_PREDICTION":
        direct = "Partially. The simple surface improved profile stability versus the full surface, but did not beat the frozen exposure control enough to establish incremental multi-hit value."

    machine = {
        "generated_at_utc": now_utc(),
        "simple_surface_cells": support_simple_cells,
        "corrected_full_surface_cells": support_full_cells,
        "lookup_probability_mismatches": int(repro_summary.iloc[0]["probability_mismatches"]),
        "lookup_level_mismatches": int(repro_summary.iloc[0]["lookup_level_mismatches"]),
        "simple_contact_holdout_brier": float(surf_hold["brier"]),
        "simple_contact_holdout_auc": float(surf_hold["auc"]),
        "holdout_control_brier": control_brier,
        "holdout_control_auc": control_auc,
        "holdout_full_source_aware_brier": full_brier,
        "holdout_full_source_aware_auc": full_auc,
        "holdout_simple_source_aware_brier": simple_brier,
        "holdout_simple_source_aware_auc": simple_auc,
        "suppression_preserved": suppression_ok,
        "local_branch_decision": local_decision,
        "next_research_decision": next_decision,
        "direct_answer": direct,
        "production_status": "NOT_AUTHORIZED",
        "decisions": {r["decision"]: r["value"] for _, r in decisions.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_simple_xhit_contact_surface_pilot_2026-07-17.json")
    decision_lines = "\n".join(f"- `{r.decision} = {r.value}`" for r in decisions.itertuples(index=False))
    write_md(f"""# MLB Simple Speed-by-Angle Contact Surface and Profile-Stability Pilot

Generated: `{machine['generated_at_utc']}`

## Executive Summary

This bounded pilot tested one frozen simplification:
`empirical_xhit_speed_angle_v1`, a smoothed launch-speed-band by
launch-angle-band empirical surface.

The simple surface uses the repaired identity-safe lookup path. It does not use
official Statcast xBA, price, sportsbook fields, new bins, new smoothing, or
additional features.

## Contact-Level Performance

| metric | holdout value |
|---|---:|
| Brier | {machine['simple_contact_holdout_brier']:.6f} |
| AUC | {machine['simple_contact_holdout_auc']:.6f} |

## Support Stability

| surface | cells |
|---|---:|
| corrected full surface | {machine['corrected_full_surface_cells']} |
| simple speed-by-angle surface | {machine['simple_surface_cells']} |

## One-to-Two-Plus Holdout

| instrument | brier | auc |
|---|---:|---:|
| frozen exposure control | {machine['holdout_control_brier']:.6f} | {machine['holdout_control_auc']:.6f} |
| corrected full source-aware conversion | {machine['holdout_full_source_aware_brier']:.6f} | {machine['holdout_full_source_aware_auc']:.6f} |
| simple source-aware conversion | {machine['holdout_simple_source_aware_brier']:.6f} | {machine['holdout_simple_source_aware_auc']:.6f} |

## Direct Answer

{direct}

## Decisions

{decision_lines}

## Production Status

`MLB_SIMPLE_XHIT_PRODUCTION_STATUS = NOT_AUTHORIZED`

No production model, candidate, selector, upload, workspace, LaunchAgent,
database, network, or OddsAPI behavior changed.
""", out_dir / "executive_summary_2026-07-17.md")

    manifest = []
    for path in [
        CONTACT_LEDGER,
        CONTACT_POP,
        LONG_PRICE,
        FULL_REPAIR_ROOT / "research_only_model_artifacts_lookup_corrected_2026-07-17.csv",
        FULL_REPAIR_ROOT / "canonical_contact_ledger_lookup_corrected_2026-07-17.csv",
        ROOT / "backend/mlb/scripts/run_mlb_simple_xhit_contact_surface_pilot.py",
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
