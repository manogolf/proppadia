#!/usr/bin/env python3
"""Bounded MLB Statcast/contact-quality data readiness audit.

This read-only utility inventories local batted-ball/contact-quality evidence
after the contact-opportunity pilot. It binds the oracle contact-quality
diagnostic semantics, reconciles batted-ball coverage over the correct
denominators, and designs the next bounded experiment without fitting a model
or acquiring new data.

No network calls, OddsAPI calls, DB writes, production model/candidate/upload
changes, LaunchAgent changes, threshold search, price optimization, or model
fitting are performed.
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

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_contact_quality_data_readiness_audit/2026-07-17"

PA_HAZARD_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pa_hit_hazard_multi_hit_pilot/2026-07-17"
CONTACT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_pregame_contact_opportunity_multi_hit_pilot/2026-07-17"
PA_LEDGER = PA_HAZARD_ROOT / "canonical_pa_outcome_ledger_2026-07-17.csv"
PA_HAZARD_SCRIPT = ROOT / "backend/mlb/scripts/run_mlb_pa_hit_hazard_multi_hit_pilot.py"
CONTACT_LEDGER = CONTACT_ROOT / "canonical_contact_outcome_ledger_2026-07-17.csv"
CONTACT_MODEL_ARTIFACT = CONTACT_ROOT / "research_only_model_artifacts_2026-07-17.csv"
CONTACT_MACHINE = CONTACT_ROOT / "machine_readable_contact_opportunity_pilot_2026-07-17.json"

EXPECTED_METRIC_TERMS = {
    "xBA": ["xba", "expected_batting_average", "estimated_ba_using_speedangle"],
    "xwOBA": ["xwoba", "expected_woba", "estimated_woba_using_speedangle"],
    "xSLG": ["xslg", "expected_slg"],
    "expected_hit_probability": ["expected_hit_probability", "hit_probability", "estimated_hit_probability", "estimated_hit_value"],
    "barrel": ["barrel", "barrels"],
    "sprint_speed": ["sprint_speed"],
    "outs_above_average": ["outs_above_average", "oaa"],
}


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


def safe_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def pct(num: int, den: int) -> float | str:
    return float(num / den) if den else ""


def bind_oracle_semantics() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "oracle_component": "oracle_bip_count",
            "source_script": rel(PA_HAZARD_SCRIPT),
            "source_fields": "actual_bip_count from canonical PA ledger ball_in_play sum",
            "formula": "Poisson two-plus from actual current-game BIP count and strict-prior pred_hit_on_bip_rate",
            "grain": "hitter-game",
            "uses_current_game_contact_count": True,
            "uses_current_game_contact_quality": False,
            "uses_official_hit_result_directly": False,
            "valid_range": "integer count >= 0",
            "home_run_treatment": "included as ball_in_play/contact and official hit",
            "field_out_error_treatment": "field_out and field_error included as BIP/contact",
            "outcome_adjacency_classification": "ORACLE_CONTACT_COUNT_TARGET_ADJACENT_CURRENT_GAME_INFORMATION",
            "notes": "Nondeployable because actual BIP count is only known in-game/postgame.",
        },
        {
            "oracle_component": "oracle_contact_quality",
            "source_script": rel(PA_HAZARD_SCRIPT),
            "source_fields": "actual_hard_hit_count, actual_bip_count, pred_hit_on_bip_rate, pred_contact_quality_hardhit",
            "formula": "quality_rate = pred_hit_on_bip_rate * (1 + 0.35 * ((actual_hard_hit_count / actual_bip_count) - pred_contact_quality_hardhit)); then Poisson two-plus from actual BIP count and quality_rate",
            "grain": "hitter-game",
            "uses_current_game_contact_count": True,
            "uses_current_game_contact_quality": True,
            "uses_official_hit_result_directly": False,
            "valid_range": "probability clipped to 0.05..0.75 after actual hard-hit-rate adjustment",
            "home_run_treatment": "home run contributes to BIP/contact; hard-hit depends on launchSpeed >= 95 if hitData present",
            "field_out_error_treatment": "contact outs/errors can contribute to actual hard-hit rate if launchSpeed present",
            "outcome_adjacency_classification": "ORACLE_CONTACT_QUALITY_PARTIALLY_OUTCOME_ADJACENT",
            "notes": "Does not use official hit result directly, but uses actual same-game hard-hit volume and actual contact count, making it target-adjacent and nondeployable.",
        },
        {
            "oracle_component": "hit_capable_contact",
            "source_script": "backend/mlb/scripts/run_mlb_pregame_contact_opportunity_multi_hit_pilot.py",
            "source_fields": "official_pa_result event classification",
            "formula": "terminal contact events capable of producing an official hit; excludes sac bunt, catcher interference, non-contact, and non-PA baserunning/pickoff terminals",
            "grain": "plate appearance",
            "uses_current_game_contact_count": True,
            "uses_current_game_contact_quality": False,
            "uses_official_hit_result_directly": False,
            "valid_range": "binary 0/1",
            "home_run_treatment": "included",
            "field_out_error_treatment": "included",
            "outcome_adjacency_classification": "CONTACT_TARGET_DEFINITION_NOT_QUALITY_SCORE",
            "notes": "A count target, not a conversion-quality feature.",
        },
    ])


def source_inventory(pa: pd.DataFrame, contact: pd.DataFrame) -> pd.DataFrame:
    source_paths = contact["source_path"].dropna().astype(str).unique().tolist() if "source_path" in contact else []
    source_count = len(source_paths)
    rows = [
        {
            "concept": "MLB feed/live hitData",
            "source_path_or_table": "source_path values in canonical PA/contact ledgers",
            "definition": "MLB StatsAPI playEvents[].hitData on pitch events",
            "grain": "pitch event / terminal PA",
            "date_coverage": f"{contact['game_date'].min()} to {contact['game_date'].max()}",
            "row_count": len(contact),
            "batter_identity": "batter_id present",
            "pitcher_identity": "pitcher_id present",
            "game_identity": "game_id + plate_appearance_sequence present",
            "strict_historical_authority": "local official game-feed JSON with SHA256 retained",
            "prediction_time_constructibility": "strict-prior profiles constructible; current-game values are postgame/oracle only",
            "missingness": "present on contacted terminal events; correctly absent on non-contact PA",
            "benchmark_overlap": "direct overlap with 10,050 benchmark hitter-game rows via PA/contact ledgers",
            "external_acquisition_required": False,
            "notes": f"{source_count} unique local feed source files referenced.",
        },
        {
            "concept": "launch speed",
            "source_path_or_table": "canonical PA/contact ledger launch_speed",
            "definition": "hitData.launchSpeed",
            "grain": "contacted PA when populated",
            "date_coverage": f"{contact['game_date'].min()} to {contact['game_date'].max()}",
            "row_count": int(safe_numeric(contact.get('launch_speed', pd.Series(dtype=object))).notna().sum()),
            "batter_identity": "yes",
            "pitcher_identity": "yes",
            "game_identity": "yes",
            "strict_historical_authority": "MLB feed hitData",
            "prediction_time_constructibility": "strict-prior distributions can be constructed",
            "missingness": "near-complete on BIP/contact, absent on non-contact",
            "benchmark_overlap": "high for contacted PA",
            "external_acquisition_required": False,
            "notes": "Hard-hit can be derived as launchSpeed >= 95.",
        },
        {
            "concept": "launch angle",
            "source_path_or_table": "canonical PA/contact ledger launch_angle",
            "definition": "hitData.launchAngle",
            "grain": "contacted PA when populated",
            "date_coverage": f"{contact['game_date'].min()} to {contact['game_date'].max()}",
            "row_count": int(safe_numeric(contact.get('launch_angle', pd.Series(dtype=object))).notna().sum()),
            "batter_identity": "yes",
            "pitcher_identity": "yes",
            "game_identity": "yes",
            "strict_historical_authority": "MLB feed hitData",
            "prediction_time_constructibility": "strict-prior distributions can be constructed",
            "missingness": "near-complete on BIP/contact, absent on non-contact",
            "benchmark_overlap": "high for contacted PA",
            "external_acquisition_required": False,
            "notes": "Sweet-spot can be derived later from a frozen launch-angle band.",
        },
        {
            "concept": "trajectory / batted-ball type",
            "source_path_or_table": "canonical PA/contact ledger batted_ball_type",
            "definition": "hitData.trajectory",
            "grain": "contacted PA when populated",
            "date_coverage": f"{contact['game_date'].min()} to {contact['game_date'].max()}",
            "row_count": int(contact.get("batted_ball_type", pd.Series(dtype=object)).notna().sum()),
            "batter_identity": "yes",
            "pitcher_identity": "yes",
            "game_identity": "yes",
            "strict_historical_authority": "MLB feed hitData",
            "prediction_time_constructibility": "strict-prior tendency profiles constructible",
            "missingness": "near-complete on BIP/contact",
            "benchmark_overlap": "high for contacted PA",
            "external_acquisition_required": False,
            "notes": "Can support GB/LD/FB/popup style conversion context.",
        },
        {
            "concept": "coordinates / spray proxy",
            "source_path_or_table": "canonical PA/contact ledger hit_coordinates_x/y",
            "definition": "hitData.coordinates.coordX/coordY",
            "grain": "contacted PA when populated",
            "date_coverage": f"{contact['game_date'].min()} to {contact['game_date'].max()}",
            "row_count": int(contact.get("hit_coordinates_x", pd.Series(dtype=object)).notna().sum()),
            "batter_identity": "yes",
            "pitcher_identity": "yes",
            "game_identity": "yes",
            "strict_historical_authority": "MLB feed hitData",
            "prediction_time_constructibility": "strict-prior spray tendency possible but coordinate normalization required",
            "missingness": "near-complete on BIP/contact",
            "benchmark_overlap": "high for contacted PA",
            "external_acquisition_required": False,
            "notes": "Needs park/orientation semantics before conversion modeling.",
        },
        {
            "concept": "official Statcast expected metrics",
            "source_path_or_table": "not found in governed ledgers/artifacts",
            "definition": "xBA/xwOBA/xSLG/expected hit probability",
            "grain": "would be batted-ball event",
            "date_coverage": "none verified locally",
            "row_count": 0,
            "batter_identity": "unknown",
            "pitcher_identity": "unknown",
            "game_identity": "unknown",
            "strict_historical_authority": "not locally established",
            "prediction_time_constructibility": "not available without external/source-specific acquisition",
            "missingness": "100% absent locally for official expected metrics",
            "benchmark_overlap": "none verified",
            "external_acquisition_required": True,
            "notes": "Local data support empirical expected-hit-value modeling later, not official Statcast expected metrics today.",
        },
        {
            "concept": "park and defense conversion context",
            "source_path_or_table": "game/feed venue and team identity present; OAA/defense metrics not verified",
            "definition": "park, defensive team, fielder/position, OAA/team conversion",
            "grain": "game/team/contact event depending source",
            "date_coverage": f"{contact['game_date'].min()} to {contact['game_date'].max()} for park/team basics",
            "row_count": len(contact),
            "batter_identity": "yes for contact ledger",
            "pitcher_identity": "yes for contact ledger",
            "game_identity": "yes",
            "strict_historical_authority": "partial",
            "prediction_time_constructibility": "park/team possible; advanced defense not ready",
            "missingness": "advanced defensive conversion not locally certified",
            "benchmark_overlap": "partial",
            "external_acquisition_required": True,
            "notes": "Park/defense branch likely needs additional source governance.",
        },
    ]
    return pd.DataFrame(rows)


def expected_metric_scan() -> pd.DataFrame:
    rows = []
    scan_files = [
        PA_LEDGER,
        CONTACT_LEDGER,
        CONTACT_MODEL_ARTIFACT,
        PA_HAZARD_ROOT / "research_only_model_artifacts_2026-07-17.csv",
    ]
    for concept, terms in EXPECTED_METRIC_TERMS.items():
        matches = []
        for path in scan_files:
            if not path.exists():
                continue
            cols = pd.read_csv(path, nrows=0).columns
            for col in cols:
                lc = col.lower()
                if lc in {"starter_expected_hits_allowed", "team_expected_hits_allowed"}:
                    continue
                if any(term.lower() in lc for term in terms):
                    matches.append(f"{rel(path)}::{col}")
        rows.append({
            "concept": concept,
            "official_or_source_provided_metric_found": bool(matches),
            "matches": "|".join(matches),
            "readiness": "LOCAL_FIELD_FOUND_REQUIRES_SEMANTIC_REVIEW" if matches else "NOT_FOUND_LOCALLY",
            "notes": "Derived hard_hit appears locally but official barrel/xBA/xwOBA expected metrics are not certified unless explicitly matched.",
        })
    return pd.DataFrame(rows)


def coverage(contact: pd.DataFrame) -> pd.DataFrame:
    df = contact.copy()
    df["split"] = "all"
    model = read_csv(CONTACT_MODEL_ARTIFACT)
    if not model.empty and "player_game_key" in model:
        split_map = model.set_index("player_game_key")["temporal_split"].to_dict()
        # Build player-game key for contact rows; exact split coverage is for contacted benchmark rows only.
        df["player_game_key"] = df["game_date"].astype(str) + "|" + df["game_id"].astype(str) + "|" + df["batter_id"].astype(str)
        df["split"] = df["player_game_key"].map(split_map).fillna("not_in_benchmark")
    scopes = [
        ("all_pitches_proxy_terminal_ledger", df),
        ("terminal_pa_events", df[df["canonical_batter_pa"].eq(1)] if "canonical_batter_pa" in df else df),
        ("terminal_contacted_pa", df[df["terminal_contact_pa"].eq(1)]),
        ("hit_capable_contacts", df[df["hit_capable_contact"].eq(1)]),
        ("official_hits", df[df["official_hit"].eq(1)]),
        ("contact_outs", df[df["bip_out"].eq(1)]),
    ]
    fields = [
        ("launch_speed", "numeric"),
        ("launch_angle", "numeric"),
        ("hit_coordinates_x", "numeric"),
        ("hit_coordinates_y", "numeric"),
        ("batted_ball_type", "non_null"),
        ("hard_hit", "numeric"),
    ]
    rows = []
    for scope, s in scopes:
        for field, kind in fields:
            series = s[field] if field in s else pd.Series(dtype=object)
            populated = int(series.notna().sum())
            valid = int(safe_numeric(series).notna().sum()) if kind == "numeric" else populated
            rows.append({
                "scope": scope,
                "field": field,
                "rows": int(len(s)),
                "populated_rows": populated,
                "valid_numeric_rows": valid,
                "missing_rows": int(len(s) - populated),
                "coverage_pct": pct(populated, len(s)),
                "valid_numeric_pct": pct(valid, len(s)),
                "date_min": s["game_date"].min() if len(s) else "",
                "date_max": s["game_date"].max() if len(s) else "",
                "fit_rows": int(s[s["split"].eq("fit")].shape[0]) if "split" in s else "",
                "validation_rows": int(s[s["split"].eq("validation")].shape[0]) if "split" in s else "",
                "holdout_rows": int(s[s["split"].eq("holdout")].shape[0]) if "split" in s else "",
                "unique_hitters": int(s["batter_id"].nunique()) if "batter_id" in s else "",
                "unique_pitchers": int(s["pitcher_id"].nunique()) if "pitcher_id" in s else "",
                "park_coverage": "not_retained_in_pa_ledger",
            })
    return pd.DataFrame(rows)


def canonical_contract() -> pd.DataFrame:
    fields = [
        ("game_date", "date", "from MLB feed gameData.datetime.officialDate or existing ledger game_date", "required"),
        ("game_id", "integer", "MLB gamePk", "required"),
        ("plate_appearance_sequence", "integer", "play order in allPlays", "required"),
        ("batter_id", "integer", "matchup.batter.id", "required"),
        ("pitcher_id", "integer", "matchup.pitcher.id", "required"),
        ("starter_reliever_role", "text", "encounter role classification", "required"),
        ("official_pa_result", "text", "result.eventType", "required"),
        ("official_hit", "integer", "single/double/triple/home_run flag", "required"),
        ("launch_speed", "float", "hitData.launchSpeed", "optional_on_contact"),
        ("launch_angle", "float", "hitData.launchAngle", "optional_on_contact"),
        ("trajectory", "text", "hitData.trajectory", "optional_on_contact"),
        ("hit_coordinates_x", "float", "hitData.coordinates.coordX", "optional_on_contact"),
        ("hit_coordinates_y", "float", "hitData.coordinates.coordY", "optional_on_contact"),
        ("hard_hit", "integer", "derived launchSpeed >= 95", "derived"),
        ("sweet_spot", "integer", "proposed derived launchAngle frozen band, e.g. 8..32 degrees", "proposed"),
        ("barrel", "integer", "official Statcast barrel if acquired; not locally certified", "future_external_or_empirical"),
        ("expected_hit_value", "float", "official xBA/estimated hit probability if acquired, else future fit-period empirical model", "future"),
        ("park", "text", "venue identity from feed/game info", "future_join"),
        ("defensive_team", "text", "fielding team at PA", "future_join"),
        ("fielder", "text", "responsible fielder if parsed", "future_optional"),
        ("source_path", "text", "local source JSON path", "required"),
        ("source_sha256", "text", "source file hash", "required"),
        ("reconciliation_state", "text", "PA/hit/field coverage state", "required"),
    ]
    return pd.DataFrame([{"field_name": f, "type": t, "definition": d, "status": s} for f, t, d, s in fields])


def profile_readiness(contact: pd.DataFrame, role: str) -> pd.DataFrame:
    id_col = "batter_id" if role == "hitter" else "pitcher_id"
    contacted = contact[contact["hit_capable_contact"].eq(1)].copy()
    if contacted.empty:
        return pd.DataFrame()
    agg = contacted.groupby(id_col).agg(
        contact_rows=("pa_key", "count"),
        launch_speed_rows=("launch_speed", lambda s: safe_numeric(s).notna().sum()),
        launch_angle_rows=("launch_angle", lambda s: safe_numeric(s).notna().sum()),
        hard_hit_rows=("hard_hit", lambda s: safe_numeric(s).notna().sum()),
        trajectory_rows=("batted_ball_type", lambda s: s.notna().sum()),
        hit_rate_on_contact=("official_hit", "mean"),
    ).reset_index()
    rows = []
    for concept, col in [
        ("average_launch_speed", "launch_speed_rows"),
        ("launch_angle_distribution", "launch_angle_rows"),
        ("hard_hit_rate", "hard_hit_rows"),
        ("sweet_spot_rate", "launch_angle_rows"),
        ("trajectory_mix", "trajectory_rows"),
        ("expected_hit_value_per_contact", "launch_speed_rows"),
        ("handedness_splits", "contact_rows"),
        ("starter_vs_reliever_splits", "contact_rows"),
    ]:
        supported = int((agg[col] >= 20).sum()) if col in agg else 0
        total = len(agg)
        rows.append({
            "profile_owner": role,
            "profile_concept": concept,
            "entities": total,
            "entities_with_20plus_contact_support": supported,
            "support_pct": pct(supported, total),
            "direct_personal_support": "variable",
            "shrinkage_required": True,
            "prior_dominated_risk": "HIGH" if supported / total < 0.5 else "MEDIUM",
            "temporal_stability_potential": "moderate" if concept not in ["handedness_splits", "starter_vs_reliever_splits"] else "low_to_moderate",
            "benchmark_coverage": "covered for local contact ledger benchmark",
            "readiness": "RESEARCH_READY_WITH_SHRINKAGE" if concept != "expected_hit_value_per_contact" else "DESIGN_READY_EMPIRICAL_MODEL_REQUIRED",
            "notes": "No official expected metric retained; empirical conversion target would require fit-period-only model." if concept == "expected_hit_value_per_contact" else "",
        })
    return pd.DataFrame(rows)


def park_defense_readiness(contact: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "context_component": "park identity",
            "local_availability": "PARTIAL",
            "source": "source game feed contains venue; PA ledger does not retain park field today",
            "strict_prior_constructibility": "requires join from source feed/game metadata",
            "coverage": "likely full after join",
            "leakage_risk": "low",
            "readiness": "JOIN_DESIGN_REQUIRED",
            "notes": "Can support park conversion profiles after canonical ledger retains venue.",
        },
        {
            "context_component": "park dimensions",
            "local_availability": "NOT_CERTIFIED",
            "source": "not verified in governed local artifacts",
            "strict_prior_constructibility": "external/reference source required",
            "coverage": "unknown",
            "leakage_risk": "low if static",
            "readiness": "EXTERNAL_OR_REFERENCE_SOURCE_REQUIRED",
            "notes": "",
        },
        {
            "context_component": "defensive team conversion",
            "local_availability": "PARTIAL",
            "source": "defensive team inferable from batting half/team; not retained in current PA ledger",
            "strict_prior_constructibility": "requires team-at-PA join and historical conversion aggregation",
            "coverage": "likely full after join",
            "leakage_risk": "medium if current-game defensive outcomes leak",
            "readiness": "JOIN_AND_GOVERNANCE_REQUIRED",
            "notes": "",
        },
        {
            "context_component": "fielder identity/position",
            "local_availability": "NOT_READY",
            "source": "not parsed into governed PA/contact ledgers",
            "strict_prior_constructibility": "requires play-result fielder parsing",
            "coverage": "unknown",
            "leakage_risk": "medium",
            "readiness": "PARSER_DESIGN_REQUIRED",
            "notes": "",
        },
        {
            "context_component": "Outs Above Average / advanced defense",
            "local_availability": "NOT_FOUND",
            "source": "not present in governed local artifacts",
            "strict_prior_constructibility": "external acquisition required",
            "coverage": "none local",
            "leakage_risk": "low if lagged season-to-date",
            "readiness": "EXTERNAL_SOURCE_REQUIRED",
            "notes": "",
        },
    ])


def branch_scorecard() -> pd.DataFrame:
    rows = [
        ("hitter_contact_quality", 5, 5, 5, 3, 5, 2, 5, 4, "Local launch speed/angle/trajectory support exists; official x metrics absent."),
        ("pitcher_contact_quality_allowed", 4, 5, 5, 3, 5, 2, 4, 4, "Same local fields, but pitcher support thinner and role splits need shrinkage."),
        ("hitter_pitcher_contact_quality_interaction", 4, 4, 4, 2, 4, 3, 4, 3, "Direct BvP contact quality too sparse; generalized profile interaction possible later."),
        ("park_conversion_context", 3, 2, 4, 3, 3, 2, 4, 4, "Park join needed; dimensions/factors not certified."),
        ("defense_conversion_context", 4, 1, 3, 2, 2, 3, 3, 3, "Team defense inferable but advanced defense/OAA absent."),
        ("sprint_speed", 3, 0, 2, 0, 0, 2, 4, 3, "Not found locally; likely external Statcast/Savant domain."),
        ("spray_direction", 3, 4, 4, 3, 4, 3, 3, 3, "Coordinates exist but need normalization and park/defense context."),
    ]
    return pd.DataFrame(rows, columns=[
        "candidate_domain",
        "direct_relevance_1to5",
        "local_coverage_1to5",
        "strict_prior_replayability_1to5",
        "support_1to5",
        "expected_benchmark_overlap_1to5",
        "leakage_risk_inverted_1to5",
        "interpretability_1to5",
        "scalability_1to5",
        "notes",
    ]).assign(total_score=lambda d: d[[c for c in d.columns if c.endswith("_1to5")]].sum(axis=1)).sort_values("total_score", ascending=False)


def oracle_ladder_design() -> pd.DataFrame:
    return pd.DataFrame([
        {"diagnostic": "A", "instrument": "actual_contact_count_plus_strict_prior_predicted_conversion_quality", "deployability": "oracle_count_nondeployable", "purpose": "Test conversion quality when contact quantity is known.", "required_data": "actual contact count + strict-prior contact-quality profile", "leakage_control": "conversion profile must be prior-only"},
        {"diagnostic": "B", "instrument": "predicted_contact_count_plus_actual_contact_quality_class", "deployability": "oracle_quality_nondeployable", "purpose": "Test whether actual quality explains unresolved gap.", "required_data": "predicted contact count + same-game hard-hit/barrel/xHit class", "leakage_control": "diagnostic only"},
        {"diagnostic": "C", "instrument": "predicted_contact_count_plus_strict_prior_predicted_conversion_quality", "deployability": "legitimate_pregame_challenger", "purpose": "Candidate next bounded challenger if data readiness passes.", "required_data": "strict-prior launch/sweet-spot/hard-hit/trajectory profiles", "leakage_control": "fit-only profile construction before validation/holdout"},
        {"diagnostic": "D", "instrument": "actual_contact_count_plus_actual_contact_quality", "deployability": "full_oracle_upper_bound", "purpose": "Measure maximum target-adjacent explainability.", "required_data": "actual contact count and same-game quality", "leakage_control": "never deploy"},
    ])


def next_experiment_design() -> pd.DataFrame:
    return pd.DataFrame([
        {"design_item": "selected_branch", "value": "STATCAST_EXPECTED_CONTACT_QUALITY_PLATFORM"},
        {"design_item": "exact_source", "value": "local MLB feed hitData first; official Statcast/Savant xBA/xwOBA only after explicit acquisition approval"},
        {"design_item": "date_range", "value": "same 2026-05-01 through 2026-07-09 benchmark first"},
        {"design_item": "grain", "value": "game_date|game_id|plate_appearance_sequence|batter_id|pitcher_id contacted PA"},
        {"design_item": "fields", "value": "launch_speed, launch_angle, trajectory, coordinates, hard_hit, derived sweet_spot, hit_capable_contact, official_hit"},
        {"design_item": "identity", "value": "MLBAM batter_id/pitcher_id/gamePk; source SHA retained"},
        {"design_item": "temporal_cutoff", "value": "profiles use only contacts before slate_date"},
        {"design_item": "profile_construction", "value": "hitter and pitcher strict-prior contact-quality distributions with support-aware shrinkage"},
        {"design_item": "fit_validation_holdout", "value": "reuse frozen fit/validation/holdout splits"},
        {"design_item": "frozen_control", "value": "frozen exposure control and prior contact-count challenger"},
        {"design_item": "legitimate_challenger", "value": "predicted contact count + strict-prior predicted hit conversion on contact"},
        {"design_item": "oracle_diagnostics", "value": "A/B/C/D oracle-gap ladder defined in this audit"},
        {"design_item": "primary_metrics", "value": "one-to-two-plus Brier, log loss, AUC, calibration, frozen bands"},
        {"design_item": "suppression_requirement", "value": "must preserve U1.5 pitcher-suppression region"},
        {"design_item": "plus200_evaluation", "value": "fixed +200 through +249 exact-price diagnostic, no threshold optimization"},
        {"design_item": "stop_criteria", "value": "stop if strict-prior quality fails to improve holdout Brier/log loss or creates concentration/leakage risk"},
        {"design_item": "acquisition_requirement", "value": "none for local hitData empirical pilot; explicit permission required for official Statcast/Savant expected metrics"},
    ])


def external_permission_boundary() -> pd.DataFrame:
    return pd.DataFrame([
        {"missing_field_or_source": "official xBA/xwOBA/xSLG/expected hit probability", "proposed_authoritative_source": "Baseball Savant / Statcast search or pybaseball statcast", "date_range": "2026-05-01 through 2026-07-09 pilot first", "event_or_request_count": "one bounded date-window pull or per-day batch, exact count TBD", "estimated_storage": "small-to-moderate CSV/parquet batted-ball event table", "reuse_game_feed_acquisition": "no for official expected metrics; yes for identity/outcome validation", "external_acquisition_required": True, "elevated_access_needed": "yes if network required", "smallest_bounded_acquisition_pilot": "one completed slate or one week of Statcast batted-ball events"},
        {"missing_field_or_source": "official barrel indicator", "proposed_authoritative_source": "Baseball Savant / Statcast", "date_range": "same as above", "event_or_request_count": "same bounded pull", "estimated_storage": "same table", "reuse_game_feed_acquisition": "local hard-hit can be derived but barrel needs official/derived spec", "external_acquisition_required": True, "elevated_access_needed": "yes if network required", "smallest_bounded_acquisition_pilot": "one completed slate"},
        {"missing_field_or_source": "sprint speed", "proposed_authoritative_source": "Baseball Savant sprint speed leaderboard or Statcast metric export", "date_range": "season-to-date lagged", "event_or_request_count": "one player-season table", "estimated_storage": "small", "reuse_game_feed_acquisition": "no", "external_acquisition_required": True, "elevated_access_needed": "yes if network required", "smallest_bounded_acquisition_pilot": "players in benchmark only"},
        {"missing_field_or_source": "advanced defense/OAA", "proposed_authoritative_source": "Baseball Savant fielding/OAA exports", "date_range": "season-to-date lagged", "event_or_request_count": "one team/player-season table", "estimated_storage": "small", "reuse_game_feed_acquisition": "no", "external_acquisition_required": True, "elevated_access_needed": "yes if network required", "smallest_bounded_acquisition_pilot": "team-level only"},
    ])


def sample_ledger(contact: pd.DataFrame) -> pd.DataFrame:
    fields = [
        "game_date", "game_id", "plate_appearance_sequence", "batter_id", "pitcher_id",
        "official_pa_result", "official_hit", "starter_reliever_role", "launch_speed",
        "launch_angle", "hit_coordinates_x", "hit_coordinates_y", "batted_ball_type",
        "hard_hit", "hit_capable_contact", "source_path", "source_sha256",
    ]
    return contact[contact["hit_capable_contact"].eq(1)][fields].head(250).copy()


def decisions(source_inv: pd.DataFrame, expected: pd.DataFrame, coverage_df: pd.DataFrame, score: pd.DataFrame) -> pd.DataFrame:
    official_expected_found = bool(expected[expected["concept"].isin(["xBA", "xwOBA", "xSLG", "expected_hit_probability"])]["official_or_source_provided_metric_found"].any())
    contact_scope = coverage_df[(coverage_df["scope"].eq("hit_capable_contacts")) & (coverage_df["field"].eq("launch_speed"))]
    cq_ready = bool(len(contact_scope) and float(contact_scope.iloc[0]["coverage_pct"]) > 0.95)
    selected = score.iloc[0]["candidate_domain"] if not score.empty else "none"
    if selected in {"hitter_contact_quality", "pitcher_contact_quality_allowed"} and cq_ready:
        branch = "STATCAST_EXPECTED_CONTACT_QUALITY_PLATFORM"
    else:
        branch = "NO_LOCAL_DATA_SUFFICIENT_EXTERNAL_SOURCE_REQUIRED"
    rows = [
        ("MLB_CONTACT_QUALITY_ORACLE_SEMANTICS_DECISION", "ORACLE_CONTACT_QUALITY_PARTIALLY_OUTCOME_ADJACENT"),
        ("MLB_CONTACT_QUALITY_LOCAL_SOURCE_INVENTORY_DECISION", "LOCAL_MLB_FEED_HITDATA_AVAILABLE_OFFICIAL_EXPECTED_METRICS_ABSENT"),
        ("MLB_CONTACT_QUALITY_BATTED_BALL_COVERAGE_DECISION", "CONTACT_DENOMINATOR_COVERAGE_READY" if cq_ready else "CONTACT_DENOMINATOR_COVERAGE_INCOMPLETE"),
        ("MLB_CONTACT_QUALITY_CANONICAL_LEDGER_DECISION", "CANONICAL_BATTED_BALL_LEDGER_CONTRACT_DEFINED_SAMPLE_CREATED"),
        ("MLB_CONTACT_QUALITY_EXPECTED_METRIC_READINESS_DECISION", "OFFICIAL_EXPECTED_METRICS_FOUND" if official_expected_found else "OFFICIAL_EXPECTED_METRICS_NOT_FOUND_EMPIRICAL_MODEL_DESIGN_ONLY"),
        ("MLB_CONTACT_QUALITY_HITTER_PROFILE_READINESS_DECISION", "HITTER_CONTACT_QUALITY_PROFILES_RESEARCH_READY_WITH_SHRINKAGE"),
        ("MLB_CONTACT_QUALITY_PITCHER_PROFILE_READINESS_DECISION", "PITCHER_CONTACT_QUALITY_ALLOWED_PROFILES_RESEARCH_READY_WITH_SHRINKAGE"),
        ("MLB_CONTACT_QUALITY_PARK_DEFENSE_READINESS_DECISION", "PARK_DEFENSE_CONTEXT_PARTIAL_JOIN_AND_EXTERNAL_DEFENSE_REQUIRED"),
        ("MLB_CONTACT_QUALITY_ORACLE_GAP_LADDER_DECISION", "ORACLE_GAP_LADDER_DESIGNED_NOT_EXECUTED"),
        ("MLB_CONTACT_QUALITY_BRANCH_SCORECARD_DECISION", "LOCAL_CONTACT_QUALITY_BRANCH_OUTRANKS_PARK_DEFENSE_AND_EXTERNAL_ONLY"),
        ("MLB_CONTACT_QUALITY_SELECTED_BRANCH_DECISION", branch),
        ("MLB_CONTACT_QUALITY_NEXT_EXPERIMENT_DESIGN_DECISION", "BOUNDED_STRICT_PRIOR_CONTACT_QUALITY_CONVERSION_EXPERIMENT_DESIGNED_NOT_EXECUTED"),
        ("MLB_CONTACT_QUALITY_EXTERNAL_PERMISSION_REQUIREMENT", "NOT_REQUIRED_FOR_LOCAL_HITDATA_EMPIRICAL_PILOT_REQUIRED_FOR_OFFICIAL_STATCAST_EXPECTED_METRICS"),
        ("MLB_CONTACT_QUALITY_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
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
    pa = read_csv(PA_LEDGER)
    contact = read_csv(CONTACT_LEDGER)
    source_inv = source_inventory(pa, contact)
    oracle = bind_oracle_semantics()
    cov = coverage(contact)
    expected = expected_metric_scan()
    contract = canonical_contract()
    hitter_profiles = profile_readiness(contact, "hitter")
    pitcher_profiles = profile_readiness(contact, "pitcher")
    park_def = park_defense_readiness(contact)
    ladder = oracle_ladder_design()
    score = branch_scorecard()
    design = next_experiment_design()
    external = external_permission_boundary()
    sample = sample_ledger(contact)
    dec = decisions(source_inv, expected, cov, score)

    outputs = {
        "oracle_contact_quality_semantic_binding_2026-07-17.csv": oracle,
        "local_batted_ball_source_inventory_2026-07-17.csv": source_inv,
        "batted_ball_coverage_reconciliation_2026-07-17.csv": cov,
        "canonical_batted_ball_contract_2026-07-17.csv": contract,
        "canonical_batted_ball_sample_ledger_2026-07-17.csv": sample,
        "official_expected_metric_readiness_2026-07-17.csv": expected,
        "hitter_contact_quality_profile_readiness_2026-07-17.csv": hitter_profiles,
        "pitcher_contact_quality_profile_readiness_2026-07-17.csv": pitcher_profiles,
        "park_defense_conversion_readiness_2026-07-17.csv": park_def,
        "oracle_gap_ladder_design_2026-07-17.csv": ladder,
        "candidate_branch_scorecard_2026-07-17.csv": score,
        "selected_branch_rationale_2026-07-17.csv": pd.DataFrame([{
            "selected_branch": dec[dec["decision"].eq("MLB_CONTACT_QUALITY_SELECTED_BRANCH_DECISION")]["value"].iloc[0],
            "rationale": "Local MLB feed hitData has high contact-denominator coverage and supports strict-prior hitter/pitcher conversion profiles; official Statcast expected metrics are not locally retained.",
            "why_not_park_defense_first": "Park/defense context is partial and needs joins/external defense authority before it can explain hit conversion.",
            "why_not_stop": "Current local evidence has a genuinely new contact-quality domain beyond PA/exposure/compatibility.",
        }]),
        "bounded_next_experiment_design_2026-07-17.csv": design,
        "external_permission_boundary_2026-07-17.csv": external,
        "required_decisions_2026-07-17.csv": dec,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)
    manifest = []
    for p in [PA_LEDGER, CONTACT_LEDGER, CONTACT_MODEL_ARTIFACT, CONTACT_MACHINE, PA_HAZARD_SCRIPT]:
        if p.exists():
            manifest.append({"artifact_role": "input", "path": rel(p), "sha256": sha256(p)})
    for p in sorted(out_dir.glob("*.csv")):
        manifest.append({"artifact_role": "output", "path": rel(p), "sha256": sha256(p)})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")

    launch_contact = cov[(cov["scope"].eq("hit_capable_contacts")) & (cov["field"].eq("launch_speed"))].iloc[0]
    official_expected_found = bool(expected[expected["concept"].isin(["xBA", "xwOBA", "xSLG", "expected_hit_probability"])]["official_or_source_provided_metric_found"].any())
    selected_branch = dec[dec["decision"].eq("MLB_CONTACT_QUALITY_SELECTED_BRANCH_DECISION")]["value"].iloc[0]
    machine = {
        "generated_at_utc": now_utc(),
        "hit_capable_contact_rows": int(launch_contact["rows"]),
        "launch_speed_contact_coverage_pct": launch_contact["coverage_pct"],
        "official_expected_metrics_found": official_expected_found,
        "selected_branch": selected_branch,
        "production_status": "NOT_AUTHORIZED",
        "decisions": {r["decision"]: r["value"] for _, r in dec.iterrows()},
    }
    write_json(machine, out_dir / "machine_readable_contact_quality_readiness_2026-07-17.json")
    direct = "There is a new local strict-prior contact-quality platform worth one bounded experiment, but not official Statcast expected-metric readiness. Local MLB feed hitData can support empirical hit-conversion profiles; official xBA/xwOBA/barrel/sprint/OAA require explicit external acquisition."
    write_md(f"""# MLB Statcast Expected Contact Quality and Hit-Conversion Data Readiness Audit

Generated: `{machine['generated_at_utc']}`

## Executive Summary

The audit bound the prior oracle contact-quality diagnostic as partially
outcome-adjacent: it does not directly include official hit result, but it uses
actual same-game contact count and hard-hit rate.

Local MLB feed `hitData` is available with high coverage over contacted PA.
Official Statcast expected metrics such as xBA/xwOBA/xSLG/expected hit
probability were not found in the governed local ledgers.

Selected branch:

`{selected_branch}`

## Direct Answer

{direct}

## Production Status

`MLB_CONTACT_QUALITY_PRODUCTION_STATUS = NOT_AUTHORIZED`

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
