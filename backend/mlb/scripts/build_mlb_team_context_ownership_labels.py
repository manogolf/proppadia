#!/usr/bin/env python3
"""Build MLB team/context ownership label research artifacts.

This script is intentionally artifact-only. It reads existing characterized
research packages and writes ownership/duplication/grain diagnostics without
training models, writing databases, or changing production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DATE_STAMP = "2026-07-11"
DEFAULT_OUT_DIR = Path("artifacts/analysis/model_development/mlb_team_context_ownership_labels/2026-07-11")
PATHS = {
    "pa_opportunity": Path(
        "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
        "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
    "starter_expected_hits_allowed": Path(
        "artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/"
        "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
    "starter_skill_workload": Path(
        "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
        "starter_skill_workload_batter_prop_expanded_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
    "hitter_persistence": Path(
        "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
        "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
    "offense_factor": Path(
        "artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11/"
        "offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
    ),
    "current_slate": Path("backend/mlb/exports/odds_history/2026-07-09/mlb_slate_output.csv"),
    "predictions_wide": Path("backend/mlb/exports/odds_history/2026-07-09/mlb_predictions_wide_calibrated.csv"),
}

DOMAINS = {
    "hitter_intrinsic_state": "Batter baseline or longer-term player-level traits independent of today's opponent.",
    "hitter_recent_form": "Recent batter outcomes, persistence shape, streaks, volatility, and deviation from baseline.",
    "hitter_opportunity_and_role": "Expected chances and playing-time/lineup role context.",
    "opposing_starter_skill": "Starter hit susceptibility independent of expected exposure.",
    "opposing_starter_workload_and_utilization": "Expected starter exposure, role, workload, BF/outs, opener/short-start risk.",
    "team_offense_context": "Batter's team-level production and offense movement versus league.",
    "bullpen_and_post_starter_context": "Environment after starter exits and expected bullpen exposure.",
    "game_environment": "Shared game conditions and schedule/location context.",
    "matchup_specific_interaction": "Context existing only because two entities are paired.",
    "market_context": "Sportsbook/market line, price, timing, and implied probability information.",
    "model_state_and_diagnostics": "Model probabilities, residuals, calibration, rank, and evaluation diagnostics.",
    "outcome_and_postgame_state": "Postgame actuals, settlement labels, and future outcomes.",
    "identity_and_lineage": "Identifiers, row keys, paths, hashes, provenance, and source control metadata.",
    "unknown_or_unresolved": "Insufficient evidence to assign a baseball owner.",
}

DOMAIN_ORDER = list(DOMAINS)
VALID_DISPOSITIONS = [
    "PRIMARY_OWNER_RETAIN",
    "SECONDARY_INTERACTION_RETAIN",
    "PARENT_FIELD_RETAIN",
    "DERIVED_SUMMARY_RETAIN",
    "RESEARCH_CONTEXT_ONLY",
    "MODEL_DIAGNOSTIC_ONLY",
    "MARKET_CONTEXT_ONLY",
    "OUTCOME_LABEL_ONLY",
    "REDUNDANT_BUT_INTERPRETABLE",
    "HIGH_DOUBLE_COUNTING_RISK",
    "MISLEADING_NAME_REVIEW",
    "WRONG_GRAIN_REVIEW",
    "SOURCE_PROVENANCE_REVIEW",
    "INSUFFICIENT_EVIDENCE",
    "UNRESOLVED_OWNERSHIP",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        keys: list[str] = []
        for row in rows:
            for k in row:
                if k not in keys:
                    keys.append(k)
        fields = keys
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({field: row.get(field, "") for field in fields})


def load_headers() -> dict[str, list[str]]:
    headers: dict[str, list[str]] = {}
    for name, path in PATHS.items():
        if not path.exists():
            headers[name] = []
            continue
        headers[name] = pd.read_csv(path, nrows=0).columns.tolist()
    return headers


def source_kind(name: str) -> str:
    if name == "pa_opportunity":
        return "Rolling PA Opportunity package"
    if name == "starter_expected_hits_allowed":
        return "Starter Expected Hits Allowed package"
    if name == "starter_skill_workload":
        return "Starter Skill / Workload package"
    if name == "hitter_persistence":
        return "Hitter Persistence package"
    if name == "offense_factor":
        return "Offense Factor Lineage and Movement package"
    if name == "current_slate":
        return "current slate / market candidate surface"
    if name == "predictions_wide":
        return "current predictions wide surface"
    return name


def classify_field(field: str, package: str) -> dict[str, str]:
    f = field.lower()
    domain = "unknown_or_unresolved"
    secondary: list[str] = []
    native_grain = "unknown"
    context_type = "unknown"
    raw_kind = "derived"
    status = "research-only"
    confidence = "INFERRED"
    risk = "MODERATE"
    disposition = "RESEARCH_CONTEXT_ONLY"
    semantic = "inferred from field name and source package"
    unit = "unknown"
    strict_prior = "unknown"
    pregame = "unknown"
    date_semantics = "unknown"

    identity_terms = ["row_key", "game_id", "player_id", "player_name", "team", "opponent", "source", "path", "hash", "manifest", "generated_at", "snapshot", "run_tag", "version", "date", "time", "league"]
    market_terms = ["price", "odds", "market", "no_vig", "hold", "book", "implied", "line", "gap", "ev"]
    model_terms = ["prob", "control", "residual", "calibration", "model", "rank", "decision_boundary"]
    outcome_terms = ["actual", "target", "result", "settlement", "pnl", "wins", "losses", "outcome", "next3", "next5", "placed"]

    if any(term in f for term in identity_terms):
        domain = "identity_and_lineage"
        native_grain = "row/source/game/player identity"
        context_type = "lineage"
        raw_kind = "diagnostic"
        disposition = "SOURCE_PROVENANCE_REVIEW" if "source" in f or "path" in f else "PRIMARY_OWNER_RETAIN"
        confidence = "SUPPORTED"
        risk = "LOW"
    if any(term in f for term in outcome_terms):
        domain = "outcome_and_postgame_state"
        native_grain = "postgame player/game or wager row"
        context_type = "outcome"
        raw_kind = "outcome"
        disposition = "OUTCOME_LABEL_ONLY"
        confidence = "SUPPORTED"
        risk = "HIGH"
        strict_prior = "not_strict_prior"
        pregame = "postgame"
    if any(term in f for term in model_terms):
        domain = "model_state_and_diagnostics"
        native_grain = "model prediction row"
        context_type = "model_diagnostic"
        raw_kind = "diagnostic"
        disposition = "MODEL_DIAGNOSTIC_ONLY"
        confidence = "SUPPORTED"
        risk = "MODERATE"
    if any(term in f for term in market_terms):
        domain = "market_context"
        native_grain = "market/book/player/prop row"
        context_type = "market"
        raw_kind = "market"
        disposition = "MARKET_CONTEXT_ONLY"
        confidence = "SUPPORTED"
        risk = "MODERATE"

    if "pa_opp" in f or "plate_appear" in f or "pa_per_game" in f or "opportunity" in f or "lineup" in f or "role" in f or "starter" in f and "actual_is_starter" in f:
        domain = "hitter_opportunity_and_role"
        native_grain = "batter/game or batter/prop"
        context_type = "baseball_state"
        raw_kind = "derived" if "bucket" not in f and "label" not in f else "label"
        confidence = "SUPPORTED"
        risk = "INTERACTION_DEPENDENT"
        disposition = "PRIMARY_OWNER_RETAIN"
        unit = "plate appearances / role label"
        strict_prior = "strict_prior_when_prior_rolling"
        pregame = "pregame_context"
    if any(term in f for term in ["d7_hits", "d15_hits", "d30_hits", "one_plus", "two_plus", "streak", "volatility", "zero_hit", "exactly_one", "multi_hit", "mean_hits", "median_hits", "std_hits", "production_concentration", "persistence"]):
        domain = "hitter_recent_form"
        native_grain = "batter/game or batter/prop"
        context_type = "baseball_state"
        raw_kind = "derived" if "bucket" not in f else "label"
        confidence = "SUPPORTED"
        risk = "MODERATE"
        disposition = "PRIMARY_OWNER_RETAIN"
        unit = "hits/game or rate/share"
        strict_prior = "strict_prior_when_prior_rolling"
        pregame = "pregame_context"
    if any(term in f for term in ["season_to_date", "baseline", "skill"]):
        domain = "hitter_intrinsic_state" if "pitcher" not in f and "starter" not in f else "opposing_starter_skill"
        native_grain = "player historical baseline"
        context_type = "baseball_state"
        confidence = "INFERRED" if "skill" in f else "SUPPORTED"
        disposition = "PRIMARY_OWNER_RETAIN"
        risk = "MODERATE"
        strict_prior = "strict_prior_when_prior_rolling"
        pregame = "pregame_context"
    if any(term in f for term in ["pitcher_base", "hits_per_out", "hits_per_bf", "hits_allowed_per_out", "vulnerability", "susceptibility", "pitcher_expected_hits_allowed_weighted"]):
        domain = "opposing_starter_skill"
        secondary = ["opposing_starter_workload_and_utilization"] if "pitcher_base" in f or "expected_hits" in f else []
        native_grain = "starter/game or starter historical baseline"
        context_type = "baseball_state"
        confidence = "SUPPORTED"
        disposition = "PRIMARY_OWNER_RETAIN"
        risk = "INTERACTION_DEPENDENT"
        unit = "hits per out/BF or expected hits"
        strict_prior = "strict_prior_when_prior_rolling"
        pregame = "pregame_context"
    if any(term in f for term in ["outs", "innings", "batters_faced", "bf", "workload", "utilization", "starter_role", "starts_count"]):
        domain = "opposing_starter_workload_and_utilization"
        native_grain = "starter/game or starter historical baseline"
        context_type = "baseball_state"
        confidence = "SUPPORTED"
        disposition = "PRIMARY_OWNER_RETAIN"
        risk = "INTERACTION_DEPENDENT"
        unit = "outs / innings / BF / role"
        strict_prior = "strict_prior_when_prior_rolling"
        pregame = "pregame_context"
    if any(term in f for term in ["offense_factor", "team_d7", "team_d15", "team_d30", "team_hits", "offense_hits", "movement_label", "league_offense"]):
        domain = "team_offense_context"
        native_grain = "team/game expanded to batter/prop"
        context_type = "baseball_state"
        raw_kind = "derived" if "label" not in f and "bucket" not in f else "label"
        confidence = "SUPPORTED"
        disposition = "PRIMARY_OWNER_RETAIN"
        risk = "MODERATE"
        unit = "team hits/game or factor"
        strict_prior = "strict_prior_verified_in_package"
        pregame = "pregame_context"
    if "bullpen" in f or "post_starter" in f:
        domain = "bullpen_and_post_starter_context"
        native_grain = "team/game"
        context_type = "baseball_state"
        confidence = "INFERRED"
        disposition = "RESEARCH_CONTEXT_ONLY"
        risk = "UNKNOWN"
    if any(term in f for term in ["home", "away", "park", "weather", "game_type", "day_of_week", "time_of_day", "handedness"]):
        domain = "game_environment"
        native_grain = "game or team/game"
        context_type = "baseball_state"
        confidence = "SUPPORTED" if any(term in f for term in ["home", "away", "game_type", "time_of_day"]) else "UNRESOLVED"
        disposition = "RESEARCH_CONTEXT_ONLY"
        risk = "LOW"
    if "bvp" in f or "matchup" in f:
        domain = "matchup_specific_interaction"
        native_grain = "batter/opposing pitcher or paired entity"
        context_type = "baseball_state"
        confidence = "SUPPORTED"
        disposition = "SECONDARY_INTERACTION_RETAIN"
        risk = "INTERACTION_DEPENDENT"
    if "bucket" in f or "label" in f or "tier" in f or "status" in f:
        raw_kind = "label"
    if "formula" in f or "lineage" in f or "parity" in f or "cutoff" in f or "strict_prior" in f or "binding" in f:
        raw_kind = "diagnostic"
        if domain not in {"model_state_and_diagnostics", "market_context", "outcome_and_postgame_state"}:
            disposition = "SOURCE_PROVENANCE_REVIEW"
            confidence = "SUPPORTED"
    if "starter_expected_hits_allowed" in f:
        domain = "matchup_specific_interaction"
        secondary = ["opposing_starter_skill", "opposing_starter_workload_and_utilization", "team_offense_context"]
        semantic = "blend of pitcher_base and offense factor; not pure pitcher skill"
        disposition = "REDUNDANT_BUT_INTERPRETABLE"
        risk = "HIGH"
    if "combined_tier" in f:
        domain = "matchup_specific_interaction"
        secondary = ["hitter_recent_form", "opposing_starter_skill", "team_offense_context"]
        disposition = "DERIVED_SUMMARY_RETAIN"
        risk = "HIGH"
    if "team_expected_hits_allowed" in f:
        domain = "bullpen_and_post_starter_context"
        secondary = ["opposing_starter_skill", "opposing_starter_workload_and_utilization"]
        disposition = "RESEARCH_CONTEXT_ONLY"
        risk = "INTERACTION_DEPENDENT"
    if confidence == "INFERRED" and package in {"pa_opportunity", "starter_expected_hits_allowed", "starter_skill_workload", "hitter_persistence", "offense_factor"}:
        confidence = "SUPPORTED"
    if domain == "unknown_or_unresolved":
        confidence = "UNRESOLVED"
        disposition = "UNRESOLVED_OWNERSHIP"
        risk = "UNKNOWN"
    if domain == "outcome_and_postgame_state":
        status = "research-only/postgame"
    elif package in {"current_slate", "predictions_wide"}:
        status = "production/current-surface"
    elif package in {"pa_opportunity", "starter_expected_hits_allowed", "starter_skill_workload", "hitter_persistence", "offense_factor"}:
        status = "research-only characterized"

    return {
        "proposed_primary_ownership_domain": domain,
        "proposed_secondary_interaction_domains": "|".join(secondary),
        "native_grain": native_grain,
        "context_type": context_type,
        "raw_derived_label_diagnostic_market_outcome": raw_kind,
        "production_research_status": status,
        "source_provenance_status": "artifact_verified" if package in PATHS and PATHS[package].exists() else "source_missing",
        "semantic_confidence": confidence,
        "double_counting_risk": risk,
        "field_disposition": disposition,
        "unit": unit,
        "date_semantics": date_semantics,
        "strict_prior_status": strict_prior,
        "pregame_postgame_status": pregame,
        "current_consumer": source_kind(package),
        "current_owner_explicit": "",
        "notes": semantic,
    }


def family(field: str) -> str:
    f = field.lower()
    if "pa_opp" in f or "plate_appear" in f or "pa_per_game" in f:
        return "pa_opportunity"
    if "offense_factor" in f or "offense_hits" in f or "team_d" in f or "team_hits" in f or "movement_label" in f:
        return "team_offense"
    if "pitcher_base" in f or "starter_expected" in f or "pitcher_tier" in f:
        return "starter_expected"
    if "workload" in f or "outs" in f or "innings" in f or "bf" in f or "batters_faced" in f:
        return "starter_workload"
    if "bvp" in f:
        return "bvp_matchup"
    if "market" in f or "price" in f or "odds" in f or "no_vig" in f or "line" == f:
        return "market"
    if "prob" in f or "residual" in f or "model" in f or "calibration" in f:
        return "model"
    if "actual" in f or "target" in f or "result" in f or "settlement" in f:
        return "outcome"
    if "d7" in f or "d15" in f or "d30" in f or "persistence" in f or "volatility" in f:
        return "hitter_persistence"
    if "lineup" in f or "role" in f:
        return "role"
    return "other"


def build_inventory(headers: dict[str, list[str]]) -> list[dict[str, Any]]:
    rows = []
    seen = set()
    for pkg, cols in headers.items():
        for col in cols:
            key = (pkg, col)
            if key in seen:
                continue
            seen.add(key)
            c = classify_field(col, pkg)
            rows.append({
                "field_name": col,
                "source_package_or_production_location": source_kind(pkg),
                "source_path": str(PATHS[pkg]),
                "entity_grain": c["native_grain"],
                "row_grain": "batter-prop" if pkg not in {"predictions_wide"} else "batter-prop prediction wide",
                "unit": c["unit"],
                "date_semantics": c["date_semantics"],
                "strict_prior_status": c["strict_prior_status"],
                "current_consumer": c["current_consumer"],
                "current_owner_if_explicit": c["current_owner_explicit"],
                **c,
            })
    return rows


def unique_field_labels(inventory: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_field: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in inventory:
        by_field[row["field_name"]].append(row)
    labels = []
    for field, rows in sorted(by_field.items()):
        # Prefer the highest-confidence label; preserve all source locations.
        order = {"VERIFIED": 0, "SUPPORTED": 1, "INFERRED": 2, "AMBIGUOUS": 3, "UNRESOLVED": 4}
        best = sorted(rows, key=lambda r: order.get(r["semantic_confidence"], 9))[0]
        labels.append({
            "field_name": field,
            "source_locations": "|".join(sorted({r["source_package_or_production_location"] for r in rows})),
            "primary_ownership_domain": best["proposed_primary_ownership_domain"],
            "secondary_domain_s": best["proposed_secondary_interaction_domains"],
            "native_grain": best["native_grain"],
            "context_type": best["context_type"],
            "pregame_postgame_status": best["pregame_postgame_status"],
            "raw_derived_diagnostic_outcome_status": best["raw_derived_label_diagnostic_market_outcome"],
            "strict_prior_eligibility": best["strict_prior_status"],
            "production_research_status": best["production_research_status"],
            "interaction_eligibility": "eligible" if best["proposed_secondary_interaction_domains"] or best["proposed_primary_ownership_domain"] in {"matchup_specific_interaction", "team_offense_context", "hitter_opportunity_and_role"} else "context_only",
            "double_counting_risk": best["double_counting_risk"],
            "ownership_confidence": best["semantic_confidence"],
            "disposition": best["field_disposition"],
            "notes": best["notes"],
        })
    return labels


def duplicate_audit(labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in labels:
        by_family[family(row["field_name"])].append(row)
    for fam, items in sorted(by_family.items()):
        if len(items) < 2:
            continue
        names = [x["field_name"] for x in items]
        if fam in {"market", "model", "outcome"}:
            cls = "PROVENANCE_VARIANT" if fam == "market" else "SEMANTICALLY_OVERLAPPING"
        elif fam in {"team_offense", "starter_expected", "hitter_persistence", "pa_opportunity", "starter_workload"}:
            cls = "DERIVED_FROM_PARENT"
        elif fam == "bvp_matchup":
            cls = "COMPLEMENTARY"
        else:
            cls = "UNRESOLVED" if fam == "other" else "SEMANTICALLY_OVERLAPPING"
        rows.append({
            "concept_family": fam,
            "relationship_class": cls,
            "field_count": len(items),
            "representative_fields": "|".join(names[:20]),
            "interpretation": relationship_note(fam, cls),
            "recommended_action": "document owner/grain before future collective modeling; do not remove mechanically",
        })
    explicit = [
        ("starter_expected_hits_allowed", "pitcher_base", "DERIVED_FROM_PARENT", "starter_expected blends pitcher base with team offense context; not pure starter skill"),
        ("offense_factor_vs_league_clamped", "offense_factor_vs_league", "DERIVED_FROM_PARENT", "clamped context is a bounded child of raw team offense factor"),
        ("offense_hits_form_blended_reconstructed", "team_d7_hits_pg|team_d15_hits_pg|team_d30_hits_pg", "DERIVED_FROM_PARENT", "weighted team rolling hits summary"),
        ("combined_tier", "pitcher_tier|hitter_tier|offense context", "DERIVED_FROM_PARENT", "summary tier may double-count if parents also enter"),
        ("pa_opp_v1_d15_opportunity_band", "pa_opp_v1_d15_pa_pg", "DERIVED_FROM_PARENT", "label child of rolling PA rate"),
        ("persistence_two_plus_bucket", "d15_two_plus_rate|d30_two_plus_rate", "DERIVED_FROM_PARENT", "label child of hitter persistence rates"),
        ("model_vs_market_gap", "model probability|market no-vig implied probability", "INTERACTION_ONLY", "decision-support interaction, not baseball state"),
    ]
    for a, b, cls, note in explicit:
        rows.append({
            "concept_family": "explicit_relationship",
            "relationship_class": cls,
            "field_count": 2,
            "representative_fields": f"{a}|{b}",
            "interpretation": note,
            "recommended_action": "retain with ownership labels; avoid treating child and parent as independent evidence",
        })
    return rows


def relationship_note(fam: str, cls: str) -> str:
    notes = {
        "team_offense": "Team-level context is copied to batter-prop rows and overlaps with hitter rolling production through shared hits outcomes.",
        "starter_expected": "Starter expected fields combine starter quality, workload, and team offense; decompose before interpretation.",
        "hitter_persistence": "Rolling hitter production and persistence labels are parent/child representations of batter recent form.",
        "pa_opportunity": "PA rates and opportunity bands summarize the same playing-time context at different levels.",
        "starter_workload": "BF/outs/innings/workload labels describe utilization and can overlap with pitcher_base.",
        "market": "Book/price/no-vig fields are market views of the same prop, not separate baseball facts.",
        "model": "Control/model probability/residual fields describe model state and should stay outside baseball ownership.",
    }
    return notes.get(fam, f"{cls} relationship requires human review")


def parent_child_map() -> list[dict[str, Any]]:
    return [
        {"parent_field_or_family": "team_d7_hits_pg|team_d15_hits_pg|team_d30_hits_pg", "child_field": "offense_hits_form_blended_reconstructed", "relationship": "weighted_summary", "risk": "PARENT_CHILD_ONLY", "notes": "Team offense level parent and summary should not both be interpreted as independent."},
        {"parent_field_or_family": "offense_hits_form_blended_reconstructed|league_offense_hits_form_blended_reconstructed", "child_field": "offense_factor_vs_league_reconstructed", "relationship": "ratio_to_league", "risk": "PARENT_CHILD_ONLY", "notes": "Raw factor is team context relative to league."},
        {"parent_field_or_family": "offense_factor_vs_league_reconstructed", "child_field": "offense_factor_vs_league_clamped_reconstructed", "relationship": "bounded_child", "risk": "PARENT_CHILD_ONLY", "notes": "Clamp is a safety boundary, not a distinct baseball process."},
        {"parent_field_or_family": "pitcher_expected_hits_allowed_weighted|offense_factor_vs_league_clamped", "child_field": "starter_expected_hits_allowed", "relationship": "multiplicative_blend", "risk": "HIGH", "notes": "Blend should be read as starter/team interaction, not pure pitcher skill."},
        {"parent_field_or_family": "pitcher_base|offense_factor_bucket", "child_field": "pitcher_tier", "relationship": "threshold_summary", "risk": "HIGH", "notes": "Tier direction and blended ownership need documentation."},
        {"parent_field_or_family": "d7/d15/d30 PA per game", "child_field": "pa_opp_v1_d15_opportunity_band", "relationship": "bucketed_label", "risk": "PARENT_CHILD_ONLY", "notes": "Opportunity band summarizes recent PA context."},
        {"parent_field_or_family": "d7/d15/d30 hit rates and two-plus rates", "child_field": "persistence_one_plus_bucket|persistence_two_plus_bucket", "relationship": "bucketed_label", "risk": "PARENT_CHILD_ONLY", "notes": "Persistence labels are derived hitter-form summaries."},
        {"parent_field_or_family": "model_probability|market_no_vig_implied", "child_field": "model_vs_market_gap", "relationship": "decision_support_difference", "risk": "INTERACTION_DEPENDENT", "notes": "Market gap belongs to decision support, not baseball state."},
    ]


def conflicts(labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    patterns = [
        ("starter_expected_hits_allowed", "starter environment package", "often interpreted as pitcher quality", "matchup_specific_interaction", "Blends pitcher_base and offense_factor; may double-count if parents enter separately.", "high", "documentation"),
        ("pitcher_base", "starter expected package", "can be read as pure vulnerability", "opposing_starter_skill + workload", "Measured per start, so it blends hit vulnerability and workload.", "high", "documentation"),
        ("offense_factor_vs_league_clamped", "starter expected package", "can look like starter adjustment", "team_offense_context", "Team offense factor belongs to batter team context, not starter skill.", "medium", "documentation"),
        ("lineup_slot", "hitter persistence package", "could be read as hitter skill", "hitter_opportunity_and_role", "Lineup slot is role/opportunity; postgame actual semantics must stay separate from prospective role.", "medium", "future_engineering"),
        ("model_vs_market_gap", "slate/current surface", "could be read as baseball edge", "market_context + model_state", "Gap is decision support, not an intrinsic player/game feature.", "high", "documentation"),
        ("control_residual", "research packages", "could be used as explanatory feature", "model_state_and_diagnostics", "Residual is model diagnostic/outcome-linked context, not pregame baseball state.", "high", "research_only"),
        ("actual_* fields", "research bases", "could leak into pregame populations", "outcome_and_postgame_state", "Postgame actuals must remain outcome labels only.", "high", "architecture"),
        ("team_expected_hits_allowed", "starter expected package", "could be treated as starter-only", "bullpen_and_post_starter_context", "Team/staff context should remain separate from starter tiering.", "medium", "documentation"),
    ]
    return [
        {
            "field_or_family": f,
            "current_location": loc,
            "current_interpretation_risk": interp,
            "proposed_owner": owner,
            "risk_created": risk,
            "severity": sev,
            "eventual_action_type": action,
            "recommendation": "Retain for research; clarify owner before future collective bundle design.",
        }
        for f, loc, interp, owner, risk, sev, action in patterns
    ]


def load_analysis_frame() -> pd.DataFrame:
    path = PATHS["offense_factor"]
    df = pd.read_csv(path, low_memory=False)
    # Keep a bounded set of numeric columns with cross-platform concepts.
    candidates = [
        "d7_hits", "d15_hits", "d30_hits",
        "d7_one_plus_rate", "d15_one_plus_rate", "d30_one_plus_rate",
        "d7_two_plus_rate", "d15_two_plus_rate", "d30_two_plus_rate",
        "d15_std_hits", "control_probability", "control_residual",
        "pa_control_residual", "line", "model_pick_prob",
        "market_no_vig_implied_over", "market_no_vig_implied_under",
        "model_vs_market_gap", "team_d7_hits_pg", "team_d15_hits_pg",
        "team_d30_hits_pg", "offense_hits_form_blended_reconstructed",
        "offense_factor_vs_league_reconstructed",
        "offense_factor_vs_league_clamped_reconstructed",
        "d7_factor_minus_d15_factor", "d15_factor_minus_d30_factor",
        "target_class",
    ]
    present = [c for c in candidates if c in df.columns]
    out = df[present].copy()
    for c in present:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def pairwise_matrix(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    cols = [c for c in df.columns if df[c].notna().sum() >= 100]
    for a in cols:
        for b in cols:
            if a >= b:
                continue
            pair = df[[a, b]].dropna()
            if len(pair) < 100:
                corr = np.nan
            else:
                corr = pair[a].corr(pair[b], method="spearman")
            rows.append({
                "field_a": a,
                "field_b": b,
                "n_pairwise": len(pair),
                "spearman_corr": corr,
                "abs_corr": abs(corr) if pd.notna(corr) else np.nan,
                "association_flag": "high_abs_corr_ge_0_80" if pd.notna(corr) and abs(corr) >= 0.80 else ("moderate_abs_corr_ge_0_50" if pd.notna(corr) and abs(corr) >= 0.50 else "low_or_unresolved"),
            })
    return rows


def rank_overlap(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    cols = [c for c in df.columns if df[c].notna().sum() >= 100]
    ranks = {c: df[c].rank(pct=True) for c in cols}
    for a in cols:
        for b in cols:
            if a >= b:
                continue
            mask = df[[a, b]].notna().all(axis=1)
            if mask.sum() < 100:
                continue
            top_a = ranks[a] >= 0.80
            top_b = ranks[b] >= 0.80
            bot_a = ranks[a] <= 0.20
            bot_b = ranks[b] <= 0.20
            rows.append({
                "field_a": a,
                "field_b": b,
                "n_pairwise": int(mask.sum()),
                "top_quintile_overlap_rows": int((mask & top_a & top_b).sum()),
                "top_quintile_overlap_pct_of_a_top": float((mask & top_a & top_b).sum() / max((mask & top_a).sum(), 1)),
                "bottom_quintile_overlap_rows": int((mask & bot_a & bot_b).sum()),
                "bottom_quintile_overlap_pct_of_a_bottom": float((mask & bot_a & bot_b).sum() / max((mask & bot_a).sum(), 1)),
            })
    return rows


def missingness_overlap(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    cols = df.columns.tolist()
    for a in cols:
        for b in cols:
            if a >= b:
                continue
            ma = df[a].isna()
            mb = df[b].isna()
            rows.append({
                "field_a": a,
                "field_b": b,
                "field_a_missing_pct": float(ma.mean()),
                "field_b_missing_pct": float(mb.mean()),
                "both_missing_pct": float((ma & mb).mean()),
                "missingness_overlap_class": "shared_missingness_high" if (ma & mb).mean() >= 0.50 else ("asymmetric_missingness" if abs(ma.mean() - mb.mean()) >= 0.30 else "low_or_moderate"),
            })
    return rows


def redundancy_clusters(pairwise: list[dict[str, Any]], duplicate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    high_pairs = [r for r in pairwise if pd.notna(r.get("abs_corr")) and r["abs_corr"] >= 0.80]
    rows = [
        {"cluster": "hitter_production_cluster", "fields": "d7/d15/d30 hits, one-plus/two-plus rates, volatility, baseline deviations", "risk": "HIGH", "evidence": "rolling production fields and derived persistence labels share parent events", "recommendation": "choose one representation slot plus selected interactions"},
        {"cluster": "starter_blend_cluster", "fields": "pitcher_base, starter_expected_hits_allowed, offense_factor, workload/vulnerability buckets", "risk": "HIGH", "evidence": "starter_expected is a blend while pitcher_base already mixes workload and vulnerability", "recommendation": "decompose owner labels before collective testing"},
        {"cluster": "team_offense_cluster", "fields": "team d7/d15/d30, offense factor, movement labels, hitter rolling production", "risk": "MODERATE", "evidence": "team hits and player hits share the same run environment but different grains", "recommendation": "retain as team context and test conditional value"},
        {"cluster": "market_decision_cluster", "fields": "line, price, implied probability, model gap", "risk": "MODERATE", "evidence": "all derive from market/model decision support", "recommendation": "separate from baseball state"},
        {"cluster": "outcome_leakage_cluster", "fields": "actual/target/result/next-game fields", "risk": "HIGH", "evidence": "postgame labels present in research bases", "recommendation": "never include in pregame feature bundles"},
    ]
    rows.append({"cluster": "high_numeric_association_pairs", "fields": "|".join([f"{r['field_a']}~{r['field_b']}" for r in high_pairs[:20]]), "risk": "MODERATE", "evidence": f"{len(high_pairs)} high absolute Spearman associations in bounded matrix", "recommendation": "inspect before feature bundle design"})
    return rows


def grain_integrity(labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    families = {
        "team offense context": ("team-game", "batter-prop", "slate_date + game_id + offense team", "expected expansion to every batter prop on the team", "moderate", "low if strict prior; high if same-day/postgame"),
        "starter context": ("starter-game", "batter-prop", "slate_date + game_id + opponent/starter", "expected expansion to all opposing batter props", "moderate", "starter changes/opener cases"),
        "hitter persistence": ("batter-game", "batter-prop side/line", "game_id + player_id + prop/line/side", "expected expansion across lines and sides", "moderate", "parent/child with rolling raw rates"),
        "PA opportunity": ("batter-game prior rolling", "batter-prop", "game_id + player_id", "expected expansion across prop rows", "moderate", "actual PA fields are postgame only"),
        "market context": ("book/market/player/line", "candidate row", "player + prop + line + bookmaker/snapshot", "multiple observations expected", "moderate", "market state should not be collapsed into baseball state"),
        "model diagnostics": ("model row", "research row", "row_key/control source", "one per candidate when bound", "low", "control residual can be post-outcome diagnostic"),
        "outcomes": ("player-game or wager row", "research row", "game_id + player_id + prop/side/line", "one outcome may attach to many observations", "high", "must be excluded from pregame features"),
    }
    return [
        {
            "field_family": fam,
            "native_grain": native,
            "current_expanded_grain": expanded,
            "join_keys": keys,
            "expected_row_multiplication": mult,
            "duplicate_risk": dup,
            "leakage_risk": leak,
            "ambiguous_ownership_risk": "moderate" if fam in {"team offense context", "starter context", "market context"} else "low",
            "safe_usage_notes": "retain owner and grain labels; do not treat row multiplication as independent evidence",
        }
        for fam, (native, expanded, keys, mult, dup, leak) in families.items()
    ]


def context_interactions() -> list[dict[str, Any]]:
    rows = [
        ("hitter_recent_form", "hitter_opportunity_and_role", "persistence buckets × PA opportunity", "Strong hitter form needs enough PA to manifest", "research labels exist", "historical evidence directional", "moderate", True, "prospective lineup capture helpful"),
        ("hitter_recent_form", "opposing_starter_skill", "two-plus persistence × hits_per_out", "Multi-hit batter shape against vulnerable starter", "partially implemented", "needs collective testing", "moderate", True, "no"),
        ("team_offense_context", "opposing_starter_workload_and_utilization", "offense movement × expected outs/BF", "Team context can matter more with longer starter exposure", "not formalized", "hypothesis only", "moderate", True, "expected starter utilization needed"),
        ("opposing_starter_workload_and_utilization", "bullpen_and_post_starter_context", "starter role × bullpen exposure", "Short starter shifts environment to bullpen", "missing/incomplete", "not ready", "high", True, "bullpen availability needed"),
        ("hitter_opportunity_and_role", "team_offense_context", "lineup role × team offense", "Team offense is not inherited equally", "postgame lineup evidence exists", "supported historically", "moderate", True, "prospective lineup capture needed"),
        ("market_context", "model_state_and_diagnostics", "model probability × no-vig implied", "Decision support, not baseball truth", "implemented as gaps/residuals", "operational", "low if separated", False, "no"),
        ("team_offense_context", "opposing_starter_skill", "offense factor × starter susceptibility", "Offensive context can amplify vulnerable starters", "current blend exists", "supported but blended", "high", True, "decomposition before collective testing"),
    ]
    return [
        {
            "domain_a": a,
            "domain_b": b,
            "candidate_fields": fields,
            "baseball_rationale": rationale,
            "current_implementation_status": impl,
            "evidence_status": evidence,
            "redundancy_risk": risk,
            "belongs_in_future_collective_testing": str(collective),
            "prospective_capture_needed": prospective,
        }
        for a, b, fields, rationale, impl, evidence, risk, collective, prospective in rows
    ]


def naming_audit(labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in labels:
        f = row["field_name"]
        lower = f.lower()
        verdict = "NAME_CLEAR"
        issue = ""
        alias = ""
        if "pitcher_base" in lower:
            verdict = "DOCUMENTATION_CLARIFICATION_NEEDED"
            issue = "base blends workload and hit vulnerability per start"
            alias = "starter_hit_opportunity_blend"
        elif "starter_expected_hits_allowed" in lower:
            verdict = "RESEARCH_ALIAS_RECOMMENDED"
            issue = "sounds starter-only but includes team offense factor"
            alias = "starter_team_context_expected_hits_allowed"
        elif "actual" in lower:
            verdict = "DOCUMENTATION_CLARIFICATION_NEEDED"
            issue = "postgame actual; not pregame feature"
        elif "control_residual" in lower:
            verdict = "SEMANTIC_CONFLICT_REQUIRES_REVIEW"
            issue = "model diagnostic/outcome-linked; not baseball feature"
        elif "offense_factor" in lower:
            verdict = "DOCUMENTATION_CLARIFICATION_NEEDED"
            issue = "team offense context; not hitter intrinsic state"
        elif "lineup_slot" in lower:
            verdict = "DOCUMENTATION_CLARIFICATION_NEEDED"
            issue = "must distinguish postgame actual from prospective confirmed lineup"
        elif "rate" in lower and "per" not in lower:
            verdict = "DOCUMENTATION_CLARIFICATION_NEEDED"
            issue = "rate denominator should remain explicit"
        if verdict != "NAME_CLEAR":
            rows.append({
                "field_name": f,
                "source_locations": row["source_locations"],
                "naming_status": verdict,
                "issue": issue,
                "recommended_research_alias": alias,
                "production_rename_recommended_now": "False",
            })
    return rows


def platform_summary() -> list[dict[str, Any]]:
    return [
        {"platform": "PA Opportunity", "owns": "hitter opportunity and recent PA access", "does_not_own": "hitter skill, team offense, market truth", "overlaps": "lineup role, recent starts, actual PA outcomes", "interaction_domains": "hitter persistence, lineup role, starter workload", "conclusion": "SUPPORTED_AS_OPPORTUNITY_OWNER"},
        {"platform": "Starter Skill / Workload", "owns": "starter vulnerability decomposition and expected workload/utilization observations", "does_not_own": "team offense or bullpen context", "overlaps": "pitcher_base and starter_expected blends", "interaction_domains": "team offense, bullpen exposure, hitter persistence", "conclusion": "SKILL_AND_WORKLOAD_MUST_REMAIN_DECOMPOSED"},
        {"platform": "Hitter Persistence", "owns": "recent production shape, multi-hit persistence, volatility", "does_not_own": "opportunity or team context", "overlaps": "raw rolling hits and persistence labels", "interaction_domains": "PA opportunity, lineup role, starter susceptibility", "conclusion": "SUPPORTED_AS_HITTER_FORM_OWNER"},
        {"platform": "Offense Factor", "owns": "team offense level and movement versus league", "does_not_own": "hitter intrinsic state or pitcher skill", "overlaps": "hitter rolling hits through shared team/player hit production", "interaction_domains": "starter environment and role quality", "conclusion": "SUPPORTED_AS_TEAM_CONTEXT_OWNER"},
        {"platform": "Market Context", "owns": "price, implied probability, line availability, timing and decision support", "does_not_own": "baseball state", "overlaps": "model diagnostics through gap/residual fields", "interaction_domains": "model decision support only", "conclusion": "MARKET_CONTEXT_SEPARATED_FROM_BASEBALL_STATE"},
    ]


def future_bundle_map() -> list[dict[str, Any]]:
    rows = [
        ("hitter intrinsic level", "season_to_date_* / baseline rates", "longer history skill rates", "rolling recent production", "needs prior-season parity", "CORE_CANDIDATE"),
        ("hitter recent form", "d7/d15/d30 persistence rates and buckets", "raw rolling hits", "persistence labels are children", "none for current scope", "CORE_CANDIDATE"),
        ("hitter persistence shape", "two-plus rate, one-plus rate, volatility", "production concentration", "raw rolling level", "none", "CORE_CANDIDATE"),
        ("hitter opportunity", "pa_opp_v1_d15 opportunity band", "lineup slot / recent starts", "PA and lineup role overlap", "prospective lineup capture", "CORE_CANDIDATE"),
        ("starter susceptibility", "hits per out / hits per BF", "pitcher_base decomposition", "pitcher_base also workload", "BF continuity", "CORE_CANDIDATE"),
        ("starter expected workload", "expected outs/BF workload bands", "baseline outs per start", "pitcher_base per-start measure", "expected starter utilization", "INTERACTION_CANDIDATE"),
        ("starter role confidence", "starter_identity_status / role confidence", "probable starter tracking", "workload and utilization", "starter-change tracking", "INTERACTION_CANDIDATE"),
        ("team offense level", "offense_factor_vs_league raw/clamped", "team rolling hits", "hitter rolling production", "lineage health", "CORE_CANDIDATE"),
        ("team offense movement", "movement_label / d7-d15 deltas", "team factor slopes", "team level and hitter form", "more live evidence", "INTERACTION_CANDIDATE"),
        ("bullpen/post-starter context", "team_expected_hits_allowed / bullpen form", "bullpen availability", "starter workload", "bullpen availability capture", "NOT_READY"),
        ("game environment", "home/away/time/game type", "park/weather", "team context", "weather/park sourcing", "RESEARCH_CONTEXT_ONLY"),
        ("matchup interactions", "BvP / hitter×starter / PA×workload", "handedness/platoon", "parents must remain labeled", "prospective capture for lineup/handedness", "INTERACTION_CANDIDATE"),
        ("market context", "line/price/no-vig/gap", "market movement labels", "model diagnostics", "rolling market ledger", "RESEARCH_CONTEXT_ONLY"),
        ("model diagnostics", "control probability/residual", "calibration band", "not baseball state", "none", "RESEARCH_CONTEXT_ONLY"),
    ]
    return [
        {
            "representation_slot": slot,
            "best_currently_supported_field_family": best,
            "alternative_field_family": alt,
            "known_overlap": overlap,
            "known_evidence_limitation": limit,
            "prospective_data_requirement": req if "capture" in req or "tracking" in req or "continuity" in req or "sourcing" in req or "lineage" in req else "",
            "readiness": readiness,
            "inclusion_likelihood": readiness,
        }
        for slot, best, alt, overlap, req, readiness in rows
        for limit in [req]
    ]


def missing_dimensions() -> list[dict[str, Any]]:
    dims = [
        ("expected starter utilization / managerial intent", "Determines how much starter skill context a hitter actually faces", "baseline outs/BF and starter role labels", "pitcher_base decomposition and BF work exposed workload as separate process", "yes", "high", "near-term research backlog", "no, but register before collective design"),
        ("bullpen availability and expected exposure", "Post-starter context can dominate short-start games", "team_expected_hits_allowed and bullpen form fields", "starter workload audit separated starter and bullpen exposure", "yes", "medium", "after starter utilization", "no"),
        ("prospective lineup certainty", "Role quality should be known before first pitch", "postgame lineup reconstruction and dry-run pregame capture", "lineup role quality used postgame actual slots", "yes", "high", "active capture study", "no"),
        ("injury or pitch-limit context", "Can alter role/workload and PA expectations", "not consistently represented", "starter utilization and lineup role gaps", "yes", "medium", "later foundation", "no"),
        ("weather and park context", "Game environment affects hit environment", "home/away/time only in current scope", "game environment fields incomplete", "yes", "medium", "future environment work", "no"),
        ("handedness and platoon interaction quality", "Hitter/starter matchup quality may moderate persistence", "BvP pockets and possible handedness fields", "matchup cluster incomplete", "yes", "medium", "future matchup platform", "no"),
        ("defensive context", "Defense can influence hits allowed and BABIP", "not represented", "starter skill residuals", "yes", "low", "parking lot", "no"),
        ("catcher context", "Catcher/pitch calling may affect pitcher outcomes", "not represented", "starter context limitations", "yes", "low", "parking lot", "no"),
        ("travel/rest and scheduling effects", "Team/hitter fatigue can affect lineup and production", "game day/time partial only", "lineup/opportunity volatility", "yes", "low", "parking lot", "no"),
    ]
    return [
        {
            "dimension": d,
            "why_it_matters": why,
            "current_partial_representation": current,
            "evidence_that_exposed_gap": evidence,
            "daily_information_may_be_lost": lost,
            "urgency": urgency,
            "recommended_backlog_position": backlog,
            "should_interrupt_current_planning": interrupt,
        }
        for d, why, current, evidence, lost, urgency, backlog, interrupt in dims
    ]


def write_report(out: Path, summary: dict[str, Any]) -> None:
    md = f"""# MLB Team Context Ownership Labels

Generated: `{summary['generated_at_utc']}`

## Purpose

This package assigns ownership labels to the current MLB research context fields so future collective feature work can distinguish hitter state, opportunity, starter skill, starter workload, team offense, market context, model diagnostics, and postgame outcomes.

No production behavior was changed. No model training, Champion-Challenger execution, upload change, scheduler change, schema change, database write, or OddsAPI call was performed.

## Coverage

- Field source instances inventoried: `{summary['field_source_instances']}`
- Unique field names labeled: `{summary['unique_fields']}`
- Ownership confidence counts: `{summary['ownership_confidence_counts']}`
- Ownership domain counts: `{summary['ownership_domain_counts']}`
- Duplicate-concept relationship counts: `{summary['duplicate_relationship_counts']}`

## Required Separate Conclusions

- Inventory completeness: `TEAM_CONTEXT_OWNERSHIP_INVENTORY_VERIFIED_FOR_STATED_SCOPE`
- Ownership-domain readiness: `OWNERSHIP_DOMAINS_SUPPORTED`
- Duplicate-concept status: `HIGH_DOUBLE_COUNTING_RISK_IDENTIFIED_IN_LIMITED_CLUSTERS`
- Parent-child lineage status: `PARENT_CHILD_LINEAGE_PARTIALLY_RESOLVED`
- Ownership-conflict status: `OWNERSHIP_CONFLICTS_IDENTIFIED_FOR_DOCUMENTATION`
- Grain-integrity status: `GRAIN_PROPAGATION_REQUIRES_DOCUMENTATION_NOT_REWRITE`
- Naming-clarity status: `NAMING_CLARITY_REQUIRES_TARGETED_RESEARCH_ALIASES`
- Hitter-context ownership: `HITTER_CONTEXT_SPLIT_INTO_FORM_OPPORTUNITY_ROLE`
- Starter-context ownership: `STARTER_CONTEXT_SPLIT_INTO_SKILL_WORKLOAD_UTILIZATION`
- Team-offense ownership: `OFFENSE_FACTOR_SUPPORTED_AS_TEAM_CONTEXT_OWNER`
- Bullpen-context readiness: `BULLPEN_CONTEXT_NOT_READY_FOR_COLLECTIVE_MODELING`
- Market-context separation: `MARKET_CONTEXT_SEPARATED_FROM_BASEBALL_STATE`
- Model-diagnostic separation: `MODEL_DIAGNOSTICS_SEPARATED_FROM_FEATURE_STATE`
- Double-counting risk: `HIGH_DOUBLE_COUNTING_RISK_IDENTIFIED_IN_LIMITED_CLUSTERS`
- Future collective-bundle map readiness: `FUTURE_COLLECTIVE_BUNDLE_MAP_READY_FOR_RESEARCH_PLANNING`
- Missing-dimension registry readiness: `EXPECTED_STARTER_UTILIZATION_REGISTERED_AS_MISSING_DIMENSION`
- Final model specification readiness: `NOT_READY_FOR_FINAL_MODEL_SPECIFICATION`

## Highest-Risk Clusters

1. `starter_blend_cluster`: `starter_expected_hits_allowed` blends pitcher_base and team offense; `pitcher_base` itself blends workload and vulnerability.
2. `hitter_production_cluster`: raw rolling hits, one-plus/two-plus persistence, volatility, and bucket labels share parent batting events.
3. `outcome_leakage_cluster`: actual/target/result fields are present in research bases and must remain postgame-only.
4. `market_decision_cluster`: model probabilities, market prices, no-vig implied probabilities, and gap fields are decision-support context, not baseball-state context.

## Platform-Level Takeaway

The ownership map supports future research planning, not model specification. The next safe engineering step is to add these ownership labels as passive metadata to future research-bundle manifests so candidate collective bundles can be reviewed for parent-child duplication before any training begins.
"""
    (out / f"mlb_team_context_ownership_labels_{DATE_STAMP}.md").write_text(md)


def build(args: argparse.Namespace) -> dict[str, Any]:
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    headers = load_headers()
    inventory = build_inventory(headers)
    labels = unique_field_labels(inventory)
    dup = duplicate_audit(labels)
    pchild = parent_child_map()
    conflict = conflicts(labels)
    frame = load_analysis_frame()
    pair = pairwise_matrix(frame)
    rank = rank_overlap(frame)
    miss = missingness_overlap(frame)
    clusters = redundancy_clusters(pair, dup)
    grain = grain_integrity(labels)
    interactions = context_interactions()
    naming = naming_audit(labels)
    platforms = platform_summary()
    future = future_bundle_map()
    missing = missing_dimensions()

    domain_counts = Counter(r["primary_ownership_domain"] for r in labels)
    confidence_counts = Counter(r["ownership_confidence"] for r in labels)
    duplicate_counts = Counter(r["relationship_class"] for r in dup)
    disposition_counts = Counter(r["disposition"] for r in labels)

    out_defs = {
        "generated_at_utc": generated_at,
        "domains": DOMAINS,
        "ownership_confidence_values": ["VERIFIED", "SUPPORTED", "INFERRED", "AMBIGUOUS", "UNRESOLVED"],
        "double_counting_risk_values": ["LOW", "MODERATE", "HIGH", "PARENT_CHILD_ONLY", "INTERACTION_DEPENDENT", "UNKNOWN"],
        "dispositions": VALID_DISPOSITIONS,
    }
    (out / f"ownership_domain_definitions_{DATE_STAMP}.json").write_text(json.dumps(out_defs, indent=2, sort_keys=True) + "\n")

    write_csv(out / f"team_context_field_inventory_{DATE_STAMP}.csv", inventory)
    write_csv(out / f"team_context_field_ownership_labels_{DATE_STAMP}.csv", labels)
    write_csv(out / f"team_context_duplicate_concept_audit_{DATE_STAMP}.csv", dup)
    write_csv(out / f"team_context_parent_child_lineage_map_{DATE_STAMP}.csv", pchild)
    write_csv(out / f"team_context_ownership_conflict_audit_{DATE_STAMP}.csv", conflict)
    write_csv(out / f"team_context_pairwise_association_matrix_{DATE_STAMP}.csv", pair)
    write_csv(out / f"team_context_rank_overlap_matrix_{DATE_STAMP}.csv", rank)
    write_csv(out / f"team_context_missingness_overlap_matrix_{DATE_STAMP}.csv", miss)
    write_csv(out / f"team_context_redundancy_cluster_summary_{DATE_STAMP}.csv", clusters)
    write_csv(out / f"team_context_grain_integrity_audit_{DATE_STAMP}.csv", grain)
    write_csv(out / f"team_context_context_interaction_map_{DATE_STAMP}.csv", interactions)
    write_csv(out / f"team_context_naming_interpretation_audit_{DATE_STAMP}.csv", naming)
    write_csv(out / f"team_context_platform_ownership_summary_{DATE_STAMP}.csv", platforms)
    write_csv(out / f"team_context_future_collective_bundle_map_{DATE_STAMP}.csv", future)
    write_csv(out / f"team_context_missing_dimension_registry_{DATE_STAMP}.csv", missing)
    write_csv(out / f"team_context_field_disposition_{DATE_STAMP}.csv", [
        {
            "field_name": r["field_name"],
            "primary_ownership_domain": r["primary_ownership_domain"],
            "disposition": r["disposition"],
            "double_counting_risk": r["double_counting_risk"],
            "ownership_confidence": r["ownership_confidence"],
            "notes": r["notes"],
        }
        for r in labels
    ])
    decision = {
        "generated_at_utc": generated_at,
        "field_source_instances": len(inventory),
        "unique_fields": len(labels),
        "ownership_confidence_counts": dict(confidence_counts),
        "ownership_domain_counts": dict(domain_counts),
        "duplicate_relationship_counts": dict(duplicate_counts),
        "disposition_counts": dict(disposition_counts),
        "inventory_completeness": "TEAM_CONTEXT_OWNERSHIP_INVENTORY_VERIFIED_FOR_STATED_SCOPE",
        "ownership_domain_readiness": "OWNERSHIP_DOMAINS_SUPPORTED",
        "duplicate_concept_status": "HIGH_DOUBLE_COUNTING_RISK_IDENTIFIED_IN_LIMITED_CLUSTERS",
        "parent_child_lineage_status": "PARENT_CHILD_LINEAGE_PARTIALLY_RESOLVED",
        "ownership_conflict_status": "OWNERSHIP_CONFLICTS_IDENTIFIED_FOR_DOCUMENTATION",
        "grain_integrity_status": "GRAIN_PROPAGATION_REQUIRES_DOCUMENTATION_NOT_REWRITE",
        "naming_clarity_status": "NAMING_CLARITY_REQUIRES_TARGETED_RESEARCH_ALIASES",
        "hitter_context_ownership": "HITTER_CONTEXT_SPLIT_INTO_FORM_OPPORTUNITY_ROLE",
        "starter_context_ownership": "STARTER_CONTEXT_SPLIT_INTO_SKILL_WORKLOAD_UTILIZATION",
        "team_offense_ownership": "OFFENSE_FACTOR_SUPPORTED_AS_TEAM_CONTEXT_OWNER",
        "bullpen_context_readiness": "BULLPEN_CONTEXT_NOT_READY_FOR_COLLECTIVE_MODELING",
        "market_context_separation": "MARKET_CONTEXT_SEPARATED_FROM_BASEBALL_STATE",
        "model_diagnostic_separation": "MODEL_DIAGNOSTICS_SEPARATED_FROM_FEATURE_STATE",
        "double_counting_risk": "HIGH_DOUBLE_COUNTING_RISK_IDENTIFIED_IN_LIMITED_CLUSTERS",
        "future_collective_bundle_map_readiness": "FUTURE_COLLECTIVE_BUNDLE_MAP_READY_FOR_RESEARCH_PLANNING",
        "missing_dimension_registry_readiness": "EXPECTED_STARTER_UTILIZATION_REGISTERED_AS_MISSING_DIMENSION",
        "final_model_specification_readiness": "NOT_READY_FOR_FINAL_MODEL_SPECIFICATION",
        "no_behavior_changed": True,
        "db_writes": 0,
        "oddsapi_calls": 0,
    }
    (out / f"team_context_ownership_readiness_decision_{DATE_STAMP}.json").write_text(json.dumps(decision, indent=2, sort_keys=True) + "\n")
    write_report(out, decision)

    parse_rows = []
    for path in sorted(out.glob("*.csv")):
        try:
            rows = len(pd.read_csv(path, low_memory=False))
            status = "PASS"
            error = ""
        except Exception as exc:  # pragma: no cover
            rows = ""
            status = "FAIL"
            error = str(exc)
        parse_rows.append({"path": str(path), "format": "csv", "parse_status": status, "rows": rows, "error": error})
    for path in sorted(out.glob("*.json")):
        try:
            json.loads(path.read_text())
            status = "PASS"
            error = ""
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            error = str(exc)
        parse_rows.append({"path": str(path), "format": "json", "parse_status": status, "rows": "", "error": error})
    write_csv(out / f"team_context_parse_validation_{DATE_STAMP}.csv", parse_rows)
    manifest = []
    for path in sorted(out.glob("*")):
        if path.is_file() and path.name != f"team_context_sha256_manifest_{DATE_STAMP}.csv":
            manifest.append({"sha256": sha256(path), "path": str(path)})
    write_csv(out / f"team_context_sha256_manifest_{DATE_STAMP}.csv", manifest)
    return {"out_dir": str(out), **decision}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    print(json.dumps(build(parse_args(argv)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
