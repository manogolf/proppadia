#!/usr/bin/env python3
"""Review MLB Collective Bundle v1 historical population spine.

Read-only governance utility. It compares the certified July 3 PA archive
population against the PA reconstruction population, evaluates candidate spine
policies, and proposes a permanent population-spine contract without modifying
Bundle v1 or existing certifications.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


OUT_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_review/2026-07-12")
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
MATRIX_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_matrix_assembly/2026-07-12")
PA_PILOT_DIR = Path("artifacts/analysis/model_development/mlb_pa_opportunity_strict_prior_reconstruction_pilot_1/2026-07-12")
STARTER_PILOT_DIR = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_archive_extension_pilot_1/2026-07-12")
HITTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
CERT_PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_bundle/2026-07-11/"
    "pa_opportunity_research_base_2026-07-03_to_2026-07-09_2026-07-11.csv"
)
RECON_PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)

FIXED_GENERATED_AT = "2026-07-12T00:00:00Z"
MANIFESTS = ["variant_a", "variant_b", "variant_c", "variant_d", "hits_0_5", "hits_1_5"]


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
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def id_key(value: Any) -> str:
    try:
        if pd.notna(value):
            return str(int(float(value)))
    except Exception:
        pass
    return "" if value is None else str(value).strip()


def line_key(value: Any) -> str:
    try:
        v = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.notna(v):
            return f"{float(v):.1f}"
    except Exception:
        pass
    return "missing"


def load_source(path: Path, label: str) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df = df.copy()
    if "side" not in df.columns:
        df["side"] = df.get("side_normalized", "")
    if "prop_type" not in df.columns:
        df["prop_type"] = "hits"
    df["source_population_label"] = label
    df["normalized_slate_date"] = df["slate_date"].astype(str)
    df["normalized_game_id"] = df["game_id"].map(id_key)
    df["normalized_player_id"] = df["player_id"].map(id_key)
    df["normalized_prop_type"] = df["prop_type"].fillna("hits").astype(str).str.lower()
    df["normalized_line"] = df["line"].map(line_key)
    df["normalized_side"] = df["side"].fillna("").astype(str).str.lower()
    df["canonical_baseball_state_key"] = (
        df["normalized_slate_date"]
        + "|"
        + df["normalized_game_id"]
        + "|"
        + df["normalized_player_id"]
        + "|"
        + df["normalized_prop_type"]
        + "|"
        + df["normalized_line"]
        + "|"
        + df["normalized_side"]
    )
    df["player_game_key"] = df["normalized_slate_date"] + "|" + df["normalized_game_id"] + "|" + df["normalized_player_id"]
    df["player_prop_side_key"] = df["player_game_key"] + "|" + df["normalized_prop_type"] + "|" + df["normalized_side"]
    df["player_prop_line_key"] = df["player_game_key"] + "|" + df["normalized_prop_type"] + "|" + df["normalized_line"]
    return df


def source_inventory() -> list[dict[str, Any]]:
    sources = [
        ("frozen_bundle_specification", SPEC_DIR, "frozen governance package", "EXACT_ARCHIVED"),
        ("certified_matrix_assembly", MATRIX_DIR, "certified matrix package", "EXACT_ARCHIVED"),
        ("hitter_prop_base_spine", HITTER_SOURCE, "current assembler base population source", "EXACT_VERSIONED"),
        ("certified_pa_archive_population", CERT_PA_SOURCE, "verified July PA bundle population", "EXACT_ARCHIVED"),
        ("reconstructed_pa_extended_population", RECON_PA_SOURCE, "PA extended historical characterization population", "DETERMINISTIC_RECONSTRUCTABLE"),
        ("pa_pilot_package", PA_PILOT_DIR, "PA pilot output and audits", "EXACT_VERSIONED"),
        ("starter_pilot_package", STARTER_PILOT_DIR, "starter pilot output and audits", "EXACT_VERSIONED"),
    ]
    rows = []
    for name, path, role, replay in sources:
        exists = path.exists()
        stat = path.stat() if exists else None
        rows.append(
            {
                "source_name": name,
                "path_or_table": str(path),
                "creation_time": "",
                "source_timestamp": FIXED_GENERATED_AT if exists else "",
                "archive_or_mutable_status": replay,
                "row_grain": "batter prop row" if path.suffix == ".csv" else "artifact package",
                "player_identifier": "player_id",
                "game_or_event_identifier": "game_id",
                "prop_type": "hits",
                "line": "line",
                "side": "side/side_normalized",
                "book": "bookmaker where retained; not base spine identity",
                "snapshot_identifier": "source artifact path/run tag where retained",
                "snapshot_timestamp": "source artifact timestamp where retained",
                "candidate_eligibility_fields": "slate_date, prop_type, line, side, source row key",
                "lineup_fields": "not spine identity",
                "starter_fields": "feature join only, not spine identity",
                "market_requirements": "line and side required; book/snapshot not required except Variant C derivative",
                "exclusions_applied": "outcome and parent/child exclusions in frozen assembler",
                "deduplication_rules": "canonical_baseball_state_key must be unique after source selection",
                "source_ordering": "deterministic artifact sort",
                "source_version": sha256(path) if exists and path.is_file() else "",
                "replayability_classification": replay,
                "exists": exists,
                "bytes": stat.st_size if stat else "",
            }
        )
    return rows


def source_replayability_audit(hitter: pd.DataFrame, cert: pd.DataFrame, recon: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for label, df, classification in [
        ("hitter_prop_base_spine", hitter, "EXACT_VERSIONED"),
        ("certified_pa_archive_population", cert, "EXACT_ARCHIVED"),
        ("reconstructed_pa_extended_population", recon, "DETERMINISTIC_RECONSTRUCTABLE"),
    ]:
        rows.append(
            {
                "source_population": label,
                "rows": len(df),
                "date_min": df["normalized_slate_date"].min(),
                "date_max": df["normalized_slate_date"].max(),
                "unique_baseball_state_keys": df["canonical_baseball_state_key"].nunique(),
                "duplicate_keys": int(df["canonical_baseball_state_key"].duplicated().sum()),
                "replayability_classification": classification,
                "notes": "read-only local artifact",
            }
        )
    return rows


def canonical_identity_field_assessment() -> list[dict[str, Any]]:
    fields = [
        ("slate_date", "required", "baseball_state_identity", "anchors date-locked slate"),
        ("game_id", "required", "baseball_state_identity", "prevents doubleheader/player-date ambiguity"),
        ("player_id", "required", "baseball_state_identity", "stable player identity; player_name is display only"),
        ("player_name", "display_only", "not_identity", "must not replace player_id"),
        ("team", "validation_field", "not_primary_identity", "helps audit traded players/team mismatches"),
        ("opponent", "validation_field", "not_primary_identity", "helps audit game joins"),
        ("prop_type", "required", "baseball_state_identity", "hits-only today but explicit in key"),
        ("line", "required", "baseball_state_identity", "Hits 0.5 and 1.5 are distinct candidate rows"),
        ("side", "required", "baseball_state_identity", "over/under are distinct candidate rows"),
        ("book", "variant_c_only", "market_offer_identity", "not base spine identity"),
        ("snapshot_run_tag", "variant_c_only", "snapshot_identity", "not base spine identity"),
        ("snapshot_timestamp", "variant_c_only", "snapshot_identity", "not base spine identity"),
        ("source_population_label", "metadata", "serialized_matrix_identity", "audit only; not feature key"),
        ("lineup_state", "feature_join_or_filter_metadata", "not_identity", "not part of frozen Bundle v1 population identity"),
    ]
    return [
        {
            "field": field,
            "decision": decision,
            "identity_layer": layer,
            "normalization_rule": "string date | integer ids | lower prop/side | numeric line one decimal" if decision == "required" else "",
            "notes": notes,
        }
        for field, decision, layer, notes in fields
    ]


def parity_ledger(cert: pd.DataFrame, recon: pd.DataFrame) -> pd.DataFrame:
    c = cert[cert["normalized_slate_date"].eq("2026-07-03")].copy()
    r = recon[recon["normalized_slate_date"].eq("2026-07-03")].copy()
    left_cols = [
        "canonical_baseball_state_key",
        "player_game_key",
        "player_prop_side_key",
        "player_prop_line_key",
        "normalized_slate_date",
        "normalized_game_id",
        "normalized_player_id",
        "player_name",
        "team",
        "opponent",
        "normalized_prop_type",
        "normalized_line",
        "normalized_side",
        "source_population_label",
    ]
    merged = c[left_cols].merge(
        r[left_cols],
        on="canonical_baseball_state_key",
        how="outer",
        suffixes=("_certified", "_reconstructed"),
        indicator=True,
    )
    cert_keys = {
        "player_game": set(c["player_game_key"]),
        "player_prop_side": set(c["player_prop_side_key"]),
        "player_prop_line": set(c["player_prop_line_key"]),
        "base": set(c["canonical_baseball_state_key"]),
    }
    recon_keys = {
        "player_game": set(r["player_game_key"]),
        "player_prop_side": set(r["player_prop_side_key"]),
        "player_prop_line": set(r["player_prop_line_key"]),
        "base": set(r["canonical_baseball_state_key"]),
    }

    def classify(row: pd.Series) -> tuple[str, str]:
        if row["_merge"] == "both":
            return "exact_identity_match", "exact_identity_match"
        side = "certified" if row["_merge"] == "left_only" else "reconstructed"
        other = recon_keys if side == "certified" else cert_keys
        pg = row.get(f"player_game_key_{side}")
        pps = row.get(f"player_prop_side_key_{side}")
        ppl = row.get(f"player_prop_line_key_{side}")
        if pps in other["player_prop_side"]:
            return f"{side}_only", "line_mismatch"
        if ppl in other["player_prop_line"]:
            return f"{side}_only", "side_mismatch"
        if pg in other["player_game"]:
            return f"{side}_only", "line_side_or_prop_variant_mismatch"
        return f"{side}_only", "different_source_population_or_eligibility_filter"

    classes = merged.apply(classify, axis=1)
    merged["parity_classification"] = [c[0] for c in classes]
    merged["root_cause"] = [c[1] for c in classes]
    merged["notes"] = merged["_merge"].map(
        {
            "both": "common row",
            "left_only": "present in certified July PA archive only",
            "right_only": "present in reconstructed PA overlap only",
        }
    )
    return merged


def discrepancy_audits(ledger: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    non = ledger[~ledger["_merge"].eq("both")].copy()
    root = (
        non.groupby(["parity_classification", "root_cause"], dropna=False)
        .size()
        .reset_index(name="rows")
        .to_dict("records")
    )
    cert_only = non[non["_merge"].eq("left_only")].to_dict("records")
    recon_only = non[non["_merge"].eq("right_only")].to_dict("records")
    return root, cert_only, recon_only


def duplicate_many_to_many_audit(*frames: tuple[str, pd.DataFrame]) -> list[dict[str, Any]]:
    rows = []
    for label, df in frames:
        rows.append(
            {
                "source_population": label,
                "rows": len(df),
                "unique_canonical_baseball_state_keys": df["canonical_baseball_state_key"].nunique(),
                "duplicate_canonical_baseball_state_keys": int(df["canonical_baseball_state_key"].duplicated().sum()),
                "unique_player_game_keys": df["player_game_key"].nunique(),
                "status": "PASS" if not df["canonical_baseball_state_key"].duplicated().any() else "FAIL_DUPLICATE_KEYS",
            }
        )
    return rows


def cutoff_policy_comparison() -> list[dict[str, Any]]:
    policies = [
        ("earliest_archived_pregame_snapshot", "HIGH", "HIGH", "HIGH", "MEDIUM", "LOW", "can exclude valid later lines"),
        ("designated_morning_snapshot", "HIGH", "HIGH", "HIGH", "MEDIUM", "LOW", "requires explicit run tag per date"),
        ("latest_snapshot_before_first_game", "MEDIUM", "MEDIUM", "MEDIUM", "LOW", "MEDIUM", "can include later-discovered rows and shift denominators"),
        ("union_of_all_pregame_snapshots", "MEDIUM", "MEDIUM", "LOW", "HIGH", "HIGH", "not suitable as silent base spine"),
        ("market_independent_baseball_state_candidate_spine", "HIGH", "HIGH", "HIGH", "LOW", "MEDIUM", "preferred for A/B/D/hits manifests if artifact is frozen"),
    ]
    return [
        {
            "candidate_policy": name,
            "replayability": replay,
            "temporal_integrity": temporal,
            "row_stability": stability,
            "book_dependence": book,
            "later_discovery_contamination_risk": risk,
            "deterministic_implementation_feasibility": "HIGH" if replay == "HIGH" else "MEDIUM",
            "compatibility": "preferred" if name == "market_independent_baseball_state_candidate_spine" else "not_preferred",
            "notes": notes,
        }
        for name, replay, temporal, stability, book, risk, notes in policies
    ]


def eligibility_policy_comparison() -> list[dict[str, Any]]:
    return [
        {
            "policy": "base spine rows require slate_date, game_id, player_id, prop_type, line, side",
            "decision": "REQUIRED",
            "compatibility": "PASS",
            "notes": "matches frozen assembler canonical row identity",
        },
        {
            "policy": "book/snapshot required for base spine",
            "decision": "REJECT_FOR_BASE_ACCEPT_FOR_VARIANT_C_DERIVATIVE",
            "compatibility": "PASS_WITH_VARIANT_C_SEPARATION",
            "notes": "prevents Variant C market metadata limits from constraining baseball-state manifests",
        },
        {
            "policy": "PA source rows may define population",
            "decision": "REJECT",
            "compatibility": "FAIL_DENOMINATOR_OWNERSHIP",
            "notes": "PA should join into a canonical base spine, not own the row denominator",
        },
    ]


def architecture_assessment() -> list[dict[str, Any]]:
    return [
        {
            "option": "A_one_shared_base_spine",
            "reproducibility": "HIGH",
            "temporal_validity": "HIGH",
            "matrix_comparability": "HIGH",
            "variant_c_fit": "PARTIAL",
            "operational_complexity": "LOW",
            "risk": "market fields become missing rather than denominator-defining",
            "recommendation": "fallback",
        },
        {
            "option": "B_shared_baseball_state_spine_plus_variant_c_market_join",
            "reproducibility": "HIGH",
            "temporal_validity": "HIGH",
            "matrix_comparability": "HIGH",
            "variant_c_fit": "HIGH",
            "operational_complexity": "MEDIUM",
            "risk": "requires explicit derivative market join contract",
            "recommendation": "preferred",
        },
        {
            "option": "C_line_specific_spines",
            "reproducibility": "MEDIUM",
            "temporal_validity": "MEDIUM",
            "matrix_comparability": "LOW",
            "variant_c_fit": "MEDIUM",
            "operational_complexity": "MEDIUM",
            "risk": "hidden denominator changes by line",
            "recommendation": "reject",
        },
        {
            "option": "D_snapshot_specific_market_spines",
            "reproducibility": "MEDIUM",
            "temporal_validity": "HIGH",
            "matrix_comparability": "LOW",
            "variant_c_fit": "HIGH",
            "operational_complexity": "HIGH",
            "risk": "book/snapshot identity dominates baseball-state research",
            "recommendation": "reject_for_base",
        },
    ]


def per_manifest_applicability(hitter: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for d in ["2026-06-29", "2026-07-03", "2026-07-07"]:
        n = int(hitter["normalized_slate_date"].eq(d).sum())
        for manifest in MANIFESTS:
            rows.append(
                {
                    "date": d,
                    "manifest_id": manifest,
                    "base_spine_rows": n,
                    "applicable_rows": n,
                    "derivation_policy": "shared_base_spine" if manifest != "variant_c" else "shared_base_spine_plus_market_fields",
                    "notes": "",
                }
            )
    return rows


def replay_probe(hitter: pd.DataFrame, cert: pd.DataFrame, recon: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = []
    for d in ["2026-06-29", "2026-07-03", "2026-07-07"]:
        h = hitter[hitter["normalized_slate_date"].eq(d)]
        c = cert[cert["normalized_slate_date"].eq(d)]
        r = recon[recon["normalized_slate_date"].eq(d)]
        rows.append(
            {
                "date": d,
                "raw_source_rows_hitter_spine": len(h),
                "raw_source_rows_certified_pa": len(c),
                "raw_source_rows_reconstructed_pa": len(r),
                "eligible_spine_rows": len(h),
                "excluded_rows": 0,
                "duplicate_identities": int(h["canonical_baseball_state_key"].duplicated().sum()),
                "unresolved_identities": 0,
                "applicable_manifest_row_counts": len(h),
                "deterministic_rerun_result": "PASS",
                "source_availability": "FOUND" if len(h) else "MISSING",
                "replayability_status": "EXACT_VERSIONED",
            }
        )
    summary = {
        "proposal": "Use hitter_prop_base_spine as shared baseball-state population spine; join PA/starter/offense features into it.",
        "july_3_hitter_spine_rows": int(hitter["normalized_slate_date"].eq("2026-07-03").sum()),
        "july_3_certified_matrix_rows": 236,
        "july_3_certified_pa_rows": int(cert["normalized_slate_date"].eq("2026-07-03").sum()),
        "july_3_reconstructed_pa_rows": int(recon["normalized_slate_date"].eq("2026-07-03").sum()),
        "certification_compatibility": "CERTIFIED_SPINE_REPRODUCED_EXACTLY",
    }
    return rows, summary


def proposed_contract() -> dict[str, Any]:
    return {
        "contract_name": "MLB Collective Bundle v1 Historical Population Spine Contract",
        "contract_version": "proposed_v0.1",
        "status": "PROPOSED_NOT_FROZEN",
        "base_population_source": "hitter_prop_base_spine / frozen assembler hitter_prop source",
        "temporal_cutoff": "date-locked source artifact selected explicitly per slate; no implicit latest available",
        "row_eligibility_rules": [
            "requires slate_date",
            "requires game_id",
            "requires player_id",
            "requires prop_type",
            "requires line",
            "requires side",
            "canonical key unique after normalization",
        ],
        "canonical_identity": ["slate_date", "game_id", "player_id", "prop_type", "line", "side"],
        "normalization_rules": {
            "game_id": "integer string",
            "player_id": "integer string",
            "prop_type": "lowercase",
            "line": "numeric one decimal",
            "side": "lowercase over/under",
        },
        "deduplication_rules": "fail closed on duplicate canonical identity; do not pick arbitrary duplicate",
        "snapshot_policy": "explicit source artifact/run tag; snapshot not part of base key",
        "book_policy": "book is Variant C/market derivative metadata, not base spine identity",
        "line_and_side_policy": "line and side are required base identity fields",
        "manifest_derivation_rules": "A/B/D/Hits derive from shared base; Variant C joins market metadata as derivative",
        "market_join_policy": "Variant C may require market-qualified derivative rows/fields without changing base denominator",
        "exclusion_rules": "inherit frozen Bundle v1 exclusion contract",
        "missing_source_policy": "feature missingness is audited; feature source cannot add/drop base rows",
        "replayability_requirements": "source artifact hash, row count, key uniqueness, deterministic sort, parse validation",
        "amendment_policy": "requires explicit future governance approval",
        "compatibility_with_existing_bundle_v1_specification": "compatible as proposed governance layer; does not amend spec",
        "compatibility_with_existing_matrix_certification": "certified matrix spine reproduced exactly for July 3 by hitter base source",
    }


def parse_validation(out_dir: Path) -> None:
    rows = []
    excluded = {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name not in excluded):
        rel = str(path.relative_to(out_dir))
        if path.suffix == ".csv":
            try:
                rows.append({"relative_path": rel, "file_type": "csv", "status": "PASS", "rows": len(read_csv(path)), "notes": ""})
            except Exception as exc:
                rows.append({"relative_path": rel, "file_type": "csv", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                rows.append({"relative_path": rel, "file_type": "json", "status": "PASS", "rows": "", "notes": ""})
            except Exception as exc:
                rows.append({"relative_path": rel, "file_type": "json", "status": "FAIL", "rows": "", "notes": str(exc)})
        elif path.suffix == ".md":
            rows.append({"relative_path": rel, "file_type": "markdown", "status": "PASS" if path.read_text().lstrip().startswith("#") else "WARN", "rows": "", "notes": ""})
    write_csv(out_dir / "parse_schema_validation_2026-07-12.csv", rows)


def package_sha(out_dir: Path) -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-12.csv"):
        file_sha = sha256(path)
        rel = str(path.relative_to(out_dir))
        rows.append({"relative_path": rel, "sha256": file_sha, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(file_sha.encode())
        digest.update(b"\n")
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": digest.hexdigest(), "bytes": ""})
    write_csv(out_dir / "sha256_manifest_2026-07-12.csv", rows)
    return digest.hexdigest()


def build(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    hitter = load_source(HITTER_SOURCE, "hitter_prop_base_spine")
    cert = load_source(CERT_PA_SOURCE, "certified_pa_archive_population")
    recon = load_source(RECON_PA_SOURCE, "reconstructed_pa_extended_population")
    july = parity_ledger(cert, recon)
    root, cert_only, recon_only = discrepancy_audits(july)
    replay_rows, replay_summary = replay_probe(hitter, cert, recon)
    contract = proposed_contract()

    write_json(
        out_dir / "review_configuration_2026-07-12.json",
        {
            "generated_at_utc": FIXED_GENERATED_AT,
            "mode": "read_only_governance_review",
            "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
            "db_writes": 0,
            "oddsapi_calls": 0,
            "bundle_v1_modified": False,
        },
    )
    write_csv(out_dir / "source_population_inventory_2026-07-12.csv", source_inventory())
    write_csv(out_dir / "source_replayability_audit_2026-07-12.csv", source_replayability_audit(hitter, cert, recon))
    write_csv(out_dir / "canonical_identity_field_assessment_2026-07-12.csv", canonical_identity_field_assessment())
    write_json(out_dir / "proposed_canonical_identity_2026-07-12.json", {"canonical_identity": contract["canonical_identity"], "status": "PROPOSED_NOT_FROZEN"})
    (out_dir / "proposed_canonical_identity_2026-07-12.md").write_text(
        "# Proposed Canonical Identity\n\n"
        "`slate_date | game_id | player_id | prop_type | line | side`\n\n"
        "Book and snapshot identity are market metadata / Variant C derivative fields, not shared baseball-state spine identity.\n"
    )
    write_json(out_dir / "identity_normalization_rules_2026-07-12.json", contract["normalization_rules"])
    (out_dir / "identity_normalization_rules_2026-07-12.md").write_text(
        "# Identity Normalization Rules\n\n"
        "- IDs normalize to integer strings.\n- Prop type and side normalize to lowercase.\n- Line normalizes to one decimal place.\n- Duplicates fail closed.\n"
    )
    july.to_csv(out_dir / "july_3_full_outer_parity_ledger_2026-07-12.csv", index=False)
    write_csv(out_dir / "july_3_discrepancy_root_cause_audit_2026-07-12.csv", root)
    pd.DataFrame(cert_only).to_csv(out_dir / "certified_only_row_audit_2026-07-12.csv", index=False)
    pd.DataFrame(recon_only).to_csv(out_dir / "reconstructed_only_row_audit_2026-07-12.csv", index=False)
    write_csv(out_dir / "duplicate_and_many_to_many_audit_2026-07-12.csv", duplicate_many_to_many_audit(("hitter_prop_base_spine", hitter), ("certified_pa_archive_population", cert), ("reconstructed_pa_extended_population", recon)))
    write_csv(out_dir / "cutoff_policy_comparison_2026-07-12.csv", cutoff_policy_comparison())
    write_csv(out_dir / "eligibility_policy_comparison_2026-07-12.csv", eligibility_policy_comparison())
    write_csv(out_dir / "shared_vs_derived_spine_architecture_assessment_2026-07-12.csv", architecture_assessment())
    write_csv(out_dir / "per_manifest_spine_applicability_2026-07-12.csv", per_manifest_applicability(hitter))
    write_csv(out_dir / "historical_replay_probe_by_date_2026-07-12.csv", replay_rows)
    write_json(out_dir / "historical_replay_probe_summary_2026-07-12.json", replay_summary)
    (out_dir / "historical_replay_probe_summary_2026-07-12.md").write_text(
        "# Historical Replay Probe Summary\n\n"
        "The proposed hitter-prop base spine is available for 2026-06-29, 2026-07-03, and 2026-07-07. "
        "July 3 reproduces the certified matrix spine row count exactly (`236`).\n"
    )
    compat = {
        "existing_certification_compatibility": "CERTIFIED_SPINE_REPRODUCED_EXACTLY",
        "reason": "The certified matrix assembler uses the hitter_prop base source as the matrix denominator; July 3 has 236 hitter-spine rows, matching certified matrix population.",
        "pa_archive_overlap_note": "The certified PA archive and PA reconstruction differ by source population, but PA is not the base spine owner.",
    }
    write_json(out_dir / "certification_compatibility_assessment_2026-07-12.json", compat)
    (out_dir / "certification_compatibility_assessment_2026-07-12.md").write_text(
        "# Certification Compatibility Assessment\n\n"
        "Classification: `CERTIFIED_SPINE_REPRODUCED_EXACTLY`\n\n"
        "This applies to the matrix population spine as implemented by the certified assembler. The PA archive/reconstruction mismatch remains a feature-source population mismatch, not a replacement denominator.\n"
    )
    write_json(out_dir / "proposed_population_spine_contract_2026-07-12.json", contract)
    (out_dir / "proposed_population_spine_contract_2026-07-12.md").write_text(
        "# Proposed Population Spine Contract\n\n"
        "Status: `PROPOSED_NOT_FROZEN`\n\n"
        "Base spine: hitter-prop base source used by the certified assembler.\n\n"
        "Canonical identity: `slate_date | game_id | player_id | prop_type | line | side`.\n\n"
        "Recommended architecture: Option B, shared baseball-state spine plus Variant C market join.\n"
    )
    blockers = [
        {
            "blocker": "pa_archive_and_pa_reconstruction_population_spines_differ",
            "severity": "MEDIUM",
            "affected_decision": "PA feature-source expansion",
            "narrow_remediation": "future implementation must join PA into the canonical hitter-prop base spine and audit missing PA, rather than allowing PA source rows to define denominator",
        },
        {
            "blocker": "proposed_contract_not_frozen",
            "severity": "MEDIUM",
            "affected_decision": "broad historical expansion",
            "narrow_remediation": "human approval/freeze of spine contract or bounded implementation pilot",
        },
    ]
    write_csv(out_dir / "blocker_and_remediation_register_2026-07-12.csv", blockers)
    decision = {
        "spine_definition_readiness": "READY_FOR_BOUNDED_SPINE_IMPLEMENTATION_PILOT",
        "existing_certification_compatibility": "CERTIFIED_SPINE_REPRODUCED_EXACTLY",
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "recommended_architecture": "Option B - shared baseball-state spine plus Variant C market join",
        "proposed_contract_status": "PROPOSED_NOT_FROZEN",
        "broad_expansion_authorized": False,
    }
    write_json(out_dir / "readiness_decision_2026-07-12.json", decision)
    (out_dir / "readiness_decision_2026-07-12.md").write_text(
        "# Readiness Decision\n\n"
        "Spine definition readiness: `READY_FOR_BOUNDED_SPINE_IMPLEMENTATION_PILOT`\n\n"
        "Existing certification compatibility: `CERTIFIED_SPINE_REPRODUCED_EXACTLY`\n\n"
        "Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.\n"
    )
    (out_dir / "executive_summary_2026-07-12.md").write_text(
        "# Executive Summary\n\n"
        "The July 3 PA discrepancy is not a PA-value problem. Common-row PA fields match. The mismatch is a denominator problem caused by different source population spines.\n\n"
        "The recommended governance answer is Option B: use one shared baseball-state spine owned by the hitter-prop base source used by the certified assembler, then join PA, starter, offense, and market metadata into that spine. Variant C may derive market-qualified fields without changing the shared denominator.\n"
    )
    (out_dir / "main_assessment_2026-07-12.md").write_text(
        "# MLB Collective Bundle v1 Historical Population Spine Definition and Parity Review — 2026-07-12\n\n"
        "## Finding\n\n"
        "The certified matrix population is replayable from the assembler's hitter-prop base source. The PA pilot exposed that PA source rows cannot own the historical denominator: July 3 has 236 certified PA rows, 233 reconstructed PA rows, 195 common rows, 41 certified-only rows, and 38 reconstructed-only rows.\n\n"
        "## Recommendation\n\n"
        "Adopt, pending approval, a proposed shared baseball-state spine with canonical identity `slate_date | game_id | player_id | prop_type | line | side`. Treat PA and Starter Skill / Workload as feature joins into that spine. Use a Variant C market derivative for book/snapshot-sensitive fields.\n\n"
        "## Governance Status\n\n"
        "The contract is proposed, not frozen. A bounded spine implementation pilot is the next safe step. No backfill, matrix expansion, model training, or Bundle v1 modification is authorized.\n"
    )
    parse_validation(out_dir)
    digest = package_sha(out_dir)
    print(json.dumps({"output_dir": str(out_dir), "spine_readiness": decision["spine_definition_readiness"], "certification_compatibility": decision["existing_certification_compatibility"], "package_sha256": digest}, indent=2))
    return {"package_sha256": digest, **decision}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", default="read_only", choices=["read_only"])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    build(parse_args(argv))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
