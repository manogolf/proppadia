#!/usr/bin/env python3
"""Audit MLB research bundle fields against the ownership metadata registry.

The utility is read-only with respect to inspected research artifacts. It builds
or consumes a normalized ownership registry and writes descriptive audit outputs
only; it does not select features, train models, or alter production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DATE_STAMP = "2026-07-12"
REGISTRY_VERSION = "mlb-team-context-ownership-v1.0-2026-07-12"
SOURCE_PACKAGE = Path("artifacts/analysis/model_development/mlb_team_context_ownership_labels/2026-07-11")
DEFAULT_REGISTRY = Path(
    "artifacts/analysis/model_development/mlb_research_bundle_ownership_metadata/2026-07-12/"
    "mlb_team_context_ownership_registry_2026-07-12.csv"
)

SOURCE_FILES = {
    "labels": SOURCE_PACKAGE / "team_context_field_ownership_labels_2026-07-11.csv",
    "parent_child": SOURCE_PACKAGE / "team_context_parent_child_lineage_map_2026-07-11.csv",
    "duplicates": SOURCE_PACKAGE / "team_context_duplicate_concept_audit_2026-07-11.csv",
    "grain": SOURCE_PACKAGE / "team_context_grain_integrity_audit_2026-07-11.csv",
    "naming": SOURCE_PACKAGE / "team_context_naming_interpretation_audit_2026-07-11.csv",
    "disposition": SOURCE_PACKAGE / "team_context_field_disposition_2026-07-11.csv",
    "domains": SOURCE_PACKAGE / "ownership_domain_definitions_2026-07-11.json",
    "future_slots": SOURCE_PACKAGE / "team_context_future_collective_bundle_map_2026-07-11.csv",
    "missing_dimensions": SOURCE_PACKAGE / "team_context_missing_dimension_registry_2026-07-11.csv",
}

ALIAS_RULES = [
    {
        "alias_field": "offense_factor_vs_league_clamped",
        "registry_field": "offense_factor_vs_league_clamped_reconstructed",
        "alias_rule": "production_short_name_to_reconstructed_research_name",
        "match_confidence": "SUPPORTED",
    },
    {
        "alias_field": "offense_factor_vs_league",
        "registry_field": "offense_factor_vs_league_reconstructed",
        "alias_rule": "production_short_name_to_reconstructed_research_name",
        "match_confidence": "SUPPORTED",
    },
    {
        "alias_field": "offense_hits_form_blended",
        "registry_field": "offense_hits_form_blended_reconstructed",
        "alias_rule": "production_short_name_to_reconstructed_research_name",
        "match_confidence": "SUPPORTED",
    },
    {
        "alias_field": "model_prob",
        "registry_field": "model_pick_prob",
        "alias_rule": "model_probability_common_alias",
        "match_confidence": "SUPPORTED",
    },
    {
        "alias_field": "model_probability",
        "registry_field": "model_pick_prob",
        "alias_rule": "model_probability_common_alias",
        "match_confidence": "SUPPORTED",
    },
    {
        "alias_field": "of_factor_clamped",
        "registry_field": "offense_factor_vs_league_clamped_reconstructed",
        "alias_rule": "offense_factor_short_alias",
        "match_confidence": "SUPPORTED",
    },
    {
        "alias_field": "selected_price",
        "registry_field": "selected_side_price",
        "alias_rule": "selected_side_price_alias",
        "match_confidence": "SUPPORTED",
    },
]

FIELD_AUDIT_SCHEMA = [
    "input_field",
    "registry_match",
    "match_method",
    "match_confidence",
    "alias_rule",
    "primary_owner",
    "secondary_owners",
    "native_grain",
    "dataset_grain",
    "field_type",
    "strict_prior_eligibility",
    "parent_fields",
    "child_fields",
    "double_counting_risk",
    "grain_risk",
    "naming_status",
    "ownership_confidence",
    "disposition",
    "audit_status",
    "warning_text",
]

PARENT_CONFLICT_CLASSES = [
    "EXPECTED_PARENT_CHILD_COEXISTENCE",
    "REVIEW_FOR_DOUBLE_COUNTING",
    "HIGH_DOUBLE_COUNTING_RISK",
    "DIAGNOSTIC_NOT_MODEL_FEATURE",
    "MARKET_CONTEXT_REVIEW",
    "NO_CONFLICT",
]

GRAIN_CLASSES = [
    "GRAIN_COMPATIBLE",
    "EXPECTED_PROPAGATION",
    "DOCUMENTATION_REQUIRED",
    "WRONG_GRAIN_REVIEW",
    "POTENTIAL_LEAKAGE",
    "UNRESOLVED_GRAIN",
]

SLOT_MAP = {
    "hitter_intrinsic_state": "hitter intrinsic level",
    "hitter_recent_form": "hitter recent form",
    "hitter_opportunity_and_role": "hitter opportunity",
    "opposing_starter_skill": "starter susceptibility",
    "opposing_starter_workload_and_utilization": "starter workload",
    "team_offense_context": "team offense level",
    "bullpen_and_post_starter_context": "bullpen/post-starter context",
    "game_environment": "game environment",
    "matchup_specific_interaction": "matchup interaction",
    "market_context": "market context",
    "model_state_and_diagnostics": "model diagnostics",
    "outcome_and_postgame_state": "outcome labels",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
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
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def split_fields(value: Any) -> list[str]:
    if value is None or pd.isna(value):
        return []
    parts: list[str] = []
    for piece in str(value).split("|"):
        p = piece.strip()
        if p:
            parts.append(p)
    return parts


def load_source_registry() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    labels = pd.read_csv(SOURCE_FILES["labels"], low_memory=False)
    parent = pd.read_csv(SOURCE_FILES["parent_child"], low_memory=False)
    duplicate = pd.read_csv(SOURCE_FILES["duplicates"], low_memory=False)
    naming = pd.read_csv(SOURCE_FILES["naming"], low_memory=False)
    disposition = pd.read_csv(SOURCE_FILES["disposition"], low_memory=False)
    return labels, parent, duplicate, naming, disposition


def infer_duplicate_class(field_name: str, duplicate: pd.DataFrame) -> str:
    for _, row in duplicate.iterrows():
        reps = split_fields(row.get("representative_fields"))
        if field_name in reps:
            return str(row.get("relationship_class", ""))
    return ""


def build_registry(out_dir: Path) -> dict[str, Any]:
    labels, parent, duplicate, naming, disposition = load_source_registry()
    naming_map = {row["field_name"]: row for _, row in naming.iterrows()}
    disp_map = {row["field_name"]: row for _, row in disposition.iterrows()}
    parent_map: dict[str, list[str]] = defaultdict(list)
    child_map: dict[str, list[str]] = defaultdict(list)
    for _, row in parent.iterrows():
        child = str(row["child_field"]).strip()
        parents = split_fields(row["parent_field_or_family"])
        parent_map[child].extend(parents)
        for p in parents:
            child_map[p].append(child)

    rows: list[dict[str, Any]] = []
    for _, row in labels.iterrows():
        field = str(row["field_name"])
        naming_row = naming_map.get(field)
        disp_row = disp_map.get(field)
        naming_status = str(naming_row["naming_status"]) if naming_row is not None else "NAME_CLEAR"
        duplicate_class = infer_duplicate_class(field, duplicate)
        grain_risk = "UNRESOLVED_GRAIN" if row.get("native_grain", "") == "unknown" else (
            "POTENTIAL_LEAKAGE" if row.get("pregame_postgame_status") == "postgame" else (
                "DOCUMENTATION_REQUIRED" if "expanded" in str(row.get("native_grain", "")).lower() or row.get("double_counting_risk") in {"HIGH", "INTERACTION_DEPENDENT"} else "GRAIN_COMPATIBLE"
            )
        )
        rows.append(
            {
                "field_name": field,
                "normalized_field_name": norm(field),
                "source_package_or_family": row.get("source_locations", ""),
                "primary_ownership_domain": row.get("primary_ownership_domain", ""),
                "secondary_domain_list": row.get("secondary_domain_s", ""),
                "native_grain": row.get("native_grain", ""),
                "context_type": row.get("context_type", ""),
                "pregame_postgame_status": row.get("pregame_postgame_status", ""),
                "field_type": row.get("raw_derived_diagnostic_outcome_status", ""),
                "strict_prior_eligibility": row.get("strict_prior_eligibility", ""),
                "production_research_dormant_status": row.get("production_research_status", ""),
                "parent_fields": "|".join(dict.fromkeys(parent_map.get(field, []))),
                "child_fields": "|".join(dict.fromkeys(child_map.get(field, []))),
                "duplicate_concept_class": duplicate_class,
                "double_counting_risk": row.get("double_counting_risk", ""),
                "naming_status": naming_status,
                "grain_risk": grain_risk,
                "ownership_confidence": row.get("ownership_confidence", ""),
                "disposition": disp_row["disposition"] if disp_row is not None else row.get("disposition", ""),
                "notes": row.get("notes", ""),
                "registry_version": REGISTRY_VERSION,
                "source_package_date": "2026-07-11",
            }
        )

    reg_csv = out_dir / f"mlb_team_context_ownership_registry_{DATE_STAMP}.csv"
    write_csv(reg_csv, rows)
    reg_json = out_dir / f"mlb_team_context_ownership_registry_{DATE_STAMP}.json"
    reg_json.write_text(json.dumps({"registry_version": REGISTRY_VERSION, "fields": rows}, indent=2, sort_keys=True) + "\n")
    registry_sha = sha256(reg_csv)

    contract = {
        "registry_version": REGISTRY_VERSION,
        "registry_csv": str(reg_csv),
        "registry_sha256": registry_sha,
        "source_package": str(SOURCE_PACKAGE),
        "source_package_date": "2026-07-11",
        "matching_methods": ["exact", "normalized", "alias", "unresolved"],
        "broad_fuzzy_matching_allowed": False,
        "unmatched_status": "UNRESOLVED_NEW_FIELD",
        "generated_at_utc": utc_now(),
    }
    (out_dir / f"ownership_registry_contract_{DATE_STAMP}.json").write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n")
    write_csv(out_dir / f"ownership_alias_matching_rules_{DATE_STAMP}.csv", ALIAS_RULES)
    write_csv(out_dir / f"field_ownership_audit_schema_{DATE_STAMP}.csv", [{"column": c, "description": c.replace("_", " ")} for c in FIELD_AUDIT_SCHEMA])
    write_csv(out_dir / f"parent_child_conflict_rule_definitions_{DATE_STAMP}.csv", [{"conflict_class": c, "description": conflict_description(c)} for c in PARENT_CONFLICT_CLASSES])
    write_csv(out_dir / f"grain_compatibility_rule_definitions_{DATE_STAMP}.csv", [{"grain_class": c, "description": grain_description(c)} for c in GRAIN_CLASSES])
    write_csv(
        out_dir / f"outcome_market_diagnostic_separation_rules_{DATE_STAMP}.csv",
        [
            {"field_group": "outcome", "detection": "field_type=outcome or postgame status", "required_handling": "separate from candidate baseball features"},
            {"field_group": "market", "detection": "primary_owner=market_context", "required_handling": "decision support only"},
            {"field_group": "model_diagnostic", "detection": "primary_owner=model_state_and_diagnostics", "required_handling": "model state, not baseball state"},
        ],
    )
    future_slots = pd.read_csv(SOURCE_FILES["future_slots"], low_memory=False)
    future_slots.to_csv(out_dir / f"future_bundle_slot_definitions_{DATE_STAMP}.csv", index=False)
    missing = pd.read_csv(SOURCE_FILES["missing_dimensions"], low_memory=False)
    missing.to_csv(out_dir / f"missing_dimension_registry_snapshot_{DATE_STAMP}.csv", index=False)

    return {
        "registry_csv": str(reg_csv),
        "registry_json": str(reg_json),
        "registry_sha256": registry_sha,
        "registry_version": REGISTRY_VERSION,
        "field_count": len(rows),
    }


def conflict_description(cls: str) -> str:
    return {
        "EXPECTED_PARENT_CHILD_COEXISTENCE": "Parent and child are present; document relationship.",
        "REVIEW_FOR_DOUBLE_COUNTING": "Multiple related baseball-state fields may represent the same information.",
        "HIGH_DOUBLE_COUNTING_RISK": "Combined field and decomposed parents coexist or high-risk cluster is present.",
        "DIAGNOSTIC_NOT_MODEL_FEATURE": "Model diagnostic fields are present and must remain separated.",
        "MARKET_CONTEXT_REVIEW": "Market fields are present beside baseball-state fields.",
        "NO_CONFLICT": "No parent-child or separation conflict detected.",
    }[cls]


def grain_description(cls: str) -> str:
    return {
        "GRAIN_COMPATIBLE": "Native grain is compatible with declared dataset grain.",
        "EXPECTED_PROPAGATION": "Higher-level context intentionally expanded to row grain.",
        "DOCUMENTATION_REQUIRED": "Grain propagation is plausible but requires explicit documentation.",
        "WRONG_GRAIN_REVIEW": "Field appears at a grain that may imply the wrong owner.",
        "POTENTIAL_LEAKAGE": "Postgame or outcome context appears in a pregame-oriented dataset.",
        "UNRESOLVED_GRAIN": "Grain cannot be determined from registry.",
    }[cls]


def read_input_fields(args: argparse.Namespace) -> tuple[list[str], str]:
    if args.input_csv:
        df = pd.read_csv(args.input_csv, nrows=0)
        return df.columns.tolist(), str(args.input_csv)
    if args.field_manifest:
        path = Path(args.field_manifest)
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text())
            if isinstance(data, dict):
                fields = data.get("fields") or data.get("columns") or []
            else:
                fields = data
            return [str(x) for x in fields], str(path)
        mf = pd.read_csv(path, low_memory=False)
        col = "field_name" if "field_name" in mf.columns else ("column" if "column" in mf.columns else mf.columns[0])
        return mf[col].astype(str).tolist(), str(path)
    raise SystemExit("Either --input-csv or --field-manifest is required unless --build-registry is used.")


def load_registry(path: Path) -> tuple[pd.DataFrame, str]:
    df = pd.read_csv(path, low_memory=False)
    return df, sha256(path)


def match_field(field: str, registry: pd.DataFrame) -> dict[str, Any]:
    exact = registry[registry["field_name"].eq(field)]
    if not exact.empty:
        row = exact.iloc[0].to_dict()
        row.update({"input_field": field, "match_method": "exact", "match_confidence": row.get("ownership_confidence", ""), "alias_rule": ""})
        return row
    normalized = norm(field)
    normed = registry[registry["normalized_field_name"].eq(normalized)]
    if not normed.empty:
        row = normed.iloc[0].to_dict()
        row.update({"input_field": field, "match_method": "normalized", "match_confidence": row.get("ownership_confidence", ""), "alias_rule": ""})
        return row
    for rule in ALIAS_RULES:
        if norm(rule["alias_field"]) == normalized:
            aliased = registry[registry["field_name"].eq(rule["registry_field"])]
            if not aliased.empty:
                row = aliased.iloc[0].to_dict()
                row.update({"input_field": field, "match_method": "alias", "match_confidence": rule["match_confidence"], "alias_rule": rule["alias_rule"]})
                return row
    return {
        "input_field": field,
        "field_name": "",
        "primary_ownership_domain": "unknown_or_unresolved",
        "secondary_domain_list": "",
        "native_grain": "",
        "field_type": "",
        "strict_prior_eligibility": "",
        "parent_fields": "",
        "child_fields": "",
        "double_counting_risk": "UNKNOWN",
        "grain_risk": "UNRESOLVED_GRAIN",
        "naming_status": "UNRESOLVED",
        "ownership_confidence": "UNRESOLVED",
        "disposition": "UNRESOLVED_OWNERSHIP",
        "pregame_postgame_status": "",
        "match_method": "unresolved",
        "match_confidence": "UNRESOLVED",
        "alias_rule": "",
        "notes": "",
    }


def audit_fields(fields: list[str], registry: pd.DataFrame, dataset_grain: str) -> list[dict[str, Any]]:
    rows = []
    for field in fields:
        m = match_field(field, registry)
        warnings = []
        status = "OWNERSHIP_METADATA_COMPLETE_FOR_SUPPORTED_FIELDS"
        if m["match_method"] == "unresolved":
            status = "UNRESOLVED_NEW_FIELD"
            warnings.append("field missing from ownership registry")
        if m.get("pregame_postgame_status") == "postgame" or m.get("field_type") == "outcome":
            warnings.append("postgame/outcome field must be separated")
        if m.get("primary_ownership_domain") == "market_context":
            warnings.append("market context, not baseball state")
        if m.get("primary_ownership_domain") == "model_state_and_diagnostics":
            warnings.append("model diagnostic, not baseball state")
        if m.get("double_counting_risk") == "HIGH":
            warnings.append("high double-counting risk")
        if grain_class(m, dataset_grain) in {"WRONG_GRAIN_REVIEW", "POTENTIAL_LEAKAGE", "UNRESOLVED_GRAIN"}:
            warnings.append(f"grain review: {grain_class(m, dataset_grain)}")
        rows.append(
            {
                "input_field": field,
                "registry_match": m.get("field_name", ""),
                "match_method": m.get("match_method", ""),
                "match_confidence": m.get("match_confidence", ""),
                "alias_rule": m.get("alias_rule", ""),
                "primary_owner": m.get("primary_ownership_domain", ""),
                "secondary_owners": m.get("secondary_domain_list", ""),
                "native_grain": m.get("native_grain", ""),
                "dataset_grain": dataset_grain,
                "field_type": m.get("field_type", ""),
                "strict_prior_eligibility": m.get("strict_prior_eligibility", ""),
                "parent_fields": m.get("parent_fields", ""),
                "child_fields": m.get("child_fields", ""),
                "double_counting_risk": m.get("double_counting_risk", ""),
                "grain_risk": grain_class(m, dataset_grain),
                "naming_status": m.get("naming_status", ""),
                "ownership_confidence": m.get("ownership_confidence", ""),
                "disposition": m.get("disposition", ""),
                "audit_status": status,
                "warning_text": "; ".join(warnings),
            }
        )
    return rows


def grain_class(match: dict[str, Any], dataset_grain: str) -> str:
    domain = match.get("primary_ownership_domain", "")
    native = str(match.get("native_grain", "")).lower()
    dtype = match.get("field_type", "")
    if match.get("match_method") == "unresolved":
        return "UNRESOLVED_GRAIN"
    if dtype == "outcome" or match.get("pregame_postgame_status") == "postgame":
        return "POTENTIAL_LEAKAGE"
    if "batter-prop" in dataset_grain and domain in {"team_offense_context", "opposing_starter_skill", "opposing_starter_workload_and_utilization", "bullpen_and_post_starter_context"}:
        return "EXPECTED_PROPAGATION"
    if "unknown" in native or not native:
        return "UNRESOLVED_GRAIN"
    if match.get("double_counting_risk") in {"HIGH", "INTERACTION_DEPENDENT"}:
        return "DOCUMENTATION_REQUIRED"
    return "GRAIN_COMPATIBLE"


def parent_conflicts(audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    present = {r["input_field"] for r in audit}
    present_registry = {r["registry_match"] for r in audit if r["registry_match"]}
    rows = []
    for r in audit:
        parents = split_fields(r["parent_fields"])
        children = split_fields(r["child_fields"])
        parent_hits = [p for p in parents if p in present or p in present_registry]
        child_hits = [c for c in children if c in present or c in present_registry]
        cls = "NO_CONFLICT"
        if r["primary_owner"] == "model_state_and_diagnostics":
            cls = "DIAGNOSTIC_NOT_MODEL_FEATURE"
        elif r["primary_owner"] == "market_context":
            cls = "MARKET_CONTEXT_REVIEW"
        elif r["double_counting_risk"] == "HIGH" and (parent_hits or child_hits):
            cls = "HIGH_DOUBLE_COUNTING_RISK"
        elif parent_hits or child_hits:
            cls = "REVIEW_FOR_DOUBLE_COUNTING" if r["double_counting_risk"] in {"MODERATE", "INTERACTION_DEPENDENT"} else "EXPECTED_PARENT_CHILD_COEXISTENCE"
        if cls != "NO_CONFLICT":
            rows.append(
                {
                    "field": r["input_field"],
                    "registry_match": r["registry_match"],
                    "conflict_class": cls,
                    "present_parent_fields": "|".join(parent_hits),
                    "present_child_fields": "|".join(child_hits),
                    "double_counting_risk": r["double_counting_risk"],
                    "notes": conflict_description(cls),
                }
            )
    if not rows:
        rows.append({"field": "", "registry_match": "", "conflict_class": "NO_CONFLICT", "present_parent_fields": "", "present_child_fields": "", "double_counting_risk": "", "notes": "No conflicts detected."})
    return rows


def summarize(audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for group_field in ["primary_owner", "field_type", "ownership_confidence", "disposition", "audit_status"]:
        counts = Counter(r[group_field] for r in audit)
        for value, count in sorted(counts.items()):
            rows.append({"summary_group": group_field, "value": value, "field_count": count})
    return rows


def separation(audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for r in audit:
        cls = ""
        if r["field_type"] == "outcome" or r["primary_owner"] == "outcome_and_postgame_state":
            cls = "OUTCOME_FIELD_PRESENT_AND_SEPARATED"
        elif r["primary_owner"] == "market_context":
            cls = "MARKET_FIELD_PRESENT_AND_SEPARATED"
        elif r["primary_owner"] == "model_state_and_diagnostics":
            cls = "DIAGNOSTIC_FIELD_PRESENT_AND_SEPARATED"
        if cls:
            rows.append({"field": r["input_field"], "registry_match": r["registry_match"], "separation_class": cls, "required_handling": "do not treat as candidate baseball feature"})
    return rows or [{"field": "", "registry_match": "", "separation_class": "NO_OUTCOME_MARKET_DIAGNOSTIC_FIELDS_DETECTED", "required_handling": ""}]


def unresolved_report(audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for r in audit:
        if r["match_method"] == "unresolved" or r["ownership_confidence"] == "UNRESOLVED":
            rows.append(
                {
                    "input_field": r["input_field"],
                    "issue": "UNRESOLVED_NEW_FIELD" if r["match_method"] == "unresolved" else "UNRESOLVED_REGISTRY_FIELD",
                    "recommended_ownership_review_action": "add explicit ownership metadata before collective bundle design",
                    "dataset_grain": r["dataset_grain"],
                }
            )
    return rows or [{"input_field": "", "issue": "NO_UNRESOLVED_FIELDS", "recommended_ownership_review_action": "", "dataset_grain": ""}]


def slot_summary(audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(SLOT_MAP.get(r["primary_owner"], "unresolved slots") for r in audit)
    all_slots = [
        "hitter intrinsic level",
        "hitter recent form",
        "hitter persistence shape",
        "hitter opportunity",
        "starter susceptibility",
        "starter workload",
        "starter role/utilization",
        "team offense level",
        "team offense movement",
        "bullpen/post-starter context",
        "game environment",
        "matchup interaction",
        "market context",
        "model diagnostics",
        "outcome labels",
    ]
    rows = []
    for slot in all_slots:
        count = counts.get(slot, 0)
        rows.append(
            {
                "future_bundle_slot": slot,
                "field_count": count,
                "slot_status": "represented" if count else "missing",
                "overrepresentation_flag": "overrepresented" if count >= 20 else "",
            }
        )
    unresolved = counts.get("unresolved slots", 0)
    if unresolved:
        rows.append({"future_bundle_slot": "unresolved slots", "field_count": unresolved, "slot_status": "unresolved", "overrepresentation_flag": ""})
    return rows


def audit_bundle(args: argparse.Namespace) -> dict[str, Any]:
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    fields, input_path = read_input_fields(args)
    registry, registry_sha = load_registry(Path(args.registry_csv))
    dataset_name = args.dataset_name or Path(input_path).stem
    dataset_grain = args.dataset_grain or "unknown"
    audit = audit_fields(fields, registry, dataset_grain)
    conflicts = parent_conflicts(audit)
    grain_rows = [
        {
            "field": r["input_field"],
            "registry_match": r["registry_match"],
            "native_grain": r["native_grain"],
            "dataset_grain": r["dataset_grain"],
            "grain_class": r["grain_risk"],
            "notes": grain_description(r["grain_risk"]),
        }
        for r in audit
    ]
    sep = separation(audit)
    unresolved = unresolved_report(audit)
    slots = slot_summary(audit)
    prefix = dataset_name.replace(" ", "_").lower()
    write_csv(out / f"{prefix}_field_ownership_audit_{DATE_STAMP}.csv", audit, FIELD_AUDIT_SCHEMA)
    write_csv(out / f"{prefix}_ownership_domain_summary_{DATE_STAMP}.csv", summarize(audit))
    write_csv(out / f"{prefix}_parent_child_conflict_audit_{DATE_STAMP}.csv", conflicts)
    write_csv(out / f"{prefix}_grain_compatibility_audit_{DATE_STAMP}.csv", grain_rows)
    write_csv(out / f"{prefix}_outcome_market_diagnostic_separation_audit_{DATE_STAMP}.csv", sep)
    write_csv(out / f"{prefix}_unresolved_field_report_{DATE_STAMP}.csv", unresolved)
    write_csv(out / f"{prefix}_future_bundle_slot_summary_{DATE_STAMP}.csv", slots)
    status = {
        "dataset_name": dataset_name,
        "input_path": input_path,
        "dataset_grain": dataset_grain,
        "field_count": len(fields),
        "exact_matches": sum(1 for r in audit if r["match_method"] == "exact"),
        "normalized_matches": sum(1 for r in audit if r["match_method"] == "normalized"),
        "alias_matches": sum(1 for r in audit if r["match_method"] == "alias"),
        "unresolved_fields": sum(1 for r in audit if r["match_method"] == "unresolved"),
        "parent_child_conflicts": sum(1 for r in conflicts if r["conflict_class"] != "NO_CONFLICT"),
        "high_double_counting_risks": sum(1 for r in conflicts if r["conflict_class"] == "HIGH_DOUBLE_COUNTING_RISK"),
        "wrong_grain_risks": sum(1 for r in grain_rows if r["grain_class"] in {"WRONG_GRAIN_REVIEW", "POTENTIAL_LEAKAGE", "UNRESOLVED_GRAIN"}),
        "outcome_market_diagnostic_fields": sum(1 for r in sep if r["field"]),
        "represented_slots": "|".join([r["future_bundle_slot"] for r in slots if r["slot_status"] == "represented"]),
        "registry_version": REGISTRY_VERSION,
        "registry_sha256": registry_sha,
        "statuses": bundle_statuses(audit, conflicts, grain_rows, sep),
        "db_writes": 0,
        "oddsapi_calls": 0,
        "production_behavior_changed": False,
        "generated_at_utc": utc_now(),
    }
    (out / f"{prefix}_ownership_audit_readiness_{DATE_STAMP}.json").write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
    return status


def bundle_statuses(audit: list[dict[str, Any]], conflicts: list[dict[str, Any]], grain_rows: list[dict[str, Any]], sep: list[dict[str, Any]]) -> list[str]:
    statuses = ["OWNERSHIP_METADATA_COMPLETE_FOR_SUPPORTED_FIELDS"]
    if any(r["match_method"] == "unresolved" for r in audit):
        statuses.append("UNRESOLVED_FIELDS_REQUIRE_REVIEW")
    if any(r["conflict_class"] == "HIGH_DOUBLE_COUNTING_RISK" for r in conflicts):
        statuses.append("HIGH_DOUBLE_COUNTING_RISK_PRESENT")
    if any(r["field"] and r["separation_class"].startswith("OUTCOME") for r in sep):
        statuses.append("OUTCOME_FIELDS_PRESENT_AND_SEPARATED")
    if any(r["field"] and r["separation_class"].startswith("MARKET") for r in sep):
        statuses.append("MARKET_FIELDS_PRESENT_AND_SEPARATED")
    if any(r["field"] and r["separation_class"].startswith("DIAGNOSTIC") for r in sep):
        statuses.append("DIAGNOSTIC_FIELDS_PRESENT_AND_SEPARATED")
    if any(r["grain_class"] in {"EXPECTED_PROPAGATION", "DOCUMENTATION_REQUIRED"} for r in grain_rows):
        statuses.append("GRAIN_PROPAGATION_DOCUMENTED")
    statuses.append("BUNDLE_READY_FOR_RESEARCH_DESIGN_REVIEW")
    statuses.append("BUNDLE_NOT_READY_FOR_MODEL_INPUT_SELECTION")
    return statuses


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-registry", action="store_true")
    parser.add_argument("--input-csv")
    parser.add_argument("--field-manifest")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--registry-csv", default=str(DEFAULT_REGISTRY))
    parser.add_argument("--dataset-name", default="")
    parser.add_argument("--dataset-grain", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.build_registry:
        print(json.dumps(build_registry(Path(args.output_dir)), indent=2, sort_keys=True))
        return 0
    print(json.dumps(audit_bundle(args), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
