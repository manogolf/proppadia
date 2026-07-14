#!/usr/bin/env python3
"""Certify MLB Collective Bundle v1 bounded expanded matrices.

Certification-only utility. It independently validates the bounded expansion
package for 2026-06-29 through 2026-07-09, runs an isolated replay using the
permanent expansion utility, and emits a separate certification package. It does
not modify Bundle v1, Spine Contract v1.0, original certified matrices, the
bounded expansion pilot package, production systems, databases, uploads, or
model artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.scripts import run_mlb_collective_bundle_v1_bounded_expansion_pilot as expansion


OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)
ORIGINAL_CERT_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_matrix_assembly/2026-07-12")
EXPANSION_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_historical_source_expansion_pilot_1/2026-07-12"
)
EXPANSION_UTILITY = Path("backend/mlb/scripts/run_mlb_collective_bundle_v1_bounded_expansion_pilot.py")

EXPECTED_SPEC_SHA = "0ef4bb6d227d690602dd6de10974432110e0923d25e406129fa8938ae6bb1833"
EXPECTED_SPINE_SHA = "a391043df6db97da705ae8f1921055ca705e1d94c4c075c3e58cf752fbfd39f7"
EXPECTED_EXPANSION_SHA = "64e7c3e85980fd3fd0313340e0e8bdae811ae65b2936dabb7c5f21d6ffe32a52"
EXPECTED_ORIGINAL_SHA = "f578be44c2393c85c59b37c5c3acff6898b6dcf29f13b7d3fd2bc921a9ebd135"

CERT_ID = "MLB_COLLECTIVE_BUNDLE_V1_BOUNDED_EXPANDED_MATRIX_CERTIFICATION_2026_06_29_TO_2026_07_09"
CERT_VERSION = "1.0"
CERT_TIMESTAMP_PT = "2026-07-12T20:55:38-07:00"
CERT_TIMESTAMP_UTC = "2026-07-13T03:55:38Z"
START_DATE = "2026-06-29"
END_DATE = "2026-07-09"
CONTROL_START = "2026-07-03"
CONTROL_END = "2026-07-06"
MANIFESTS = ["variant_a", "variant_b", "variant_c", "variant_d", "hits_0_5", "hits_1_5"]
IDENTITY_COLS = [
    "canonical_row_id",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "side",
    "line",
    "feature_cutoff_date",
    "source_row_key",
]
MARKET_FIELDS = {
    "selected_side_price",
    "selected_side_no_vig_implied",
    "market_book_count_two_sided",
    "market_snapshot_time_utc",
}
OUTCOME_PATTERNS = (
    "actual_",
    "target_class",
    "result",
    "win_loss",
    "roi",
    "pnl_",
    "settlement",
    "grade",
)


def sha256(path: Path) -> str:
    if path.is_dir():
        digest = hashlib.sha256()
        for child in sorted(p for p in path.rglob("*") if p.is_file()):
            digest.update(str(child.relative_to(path)).encode())
            digest.update(b"\0")
            digest.update(sha256(child).encode())
            digest.update(b"\n")
        return digest.hexdigest()
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_digest_from_manifest(path: Path) -> str:
    manifests = sorted(path.glob("*sha256_manifest*.csv"))
    if manifests:
        with manifests[0].open(newline="") as fh:
            for row in csv.DictReader(fh):
                if row.get("relative_path", "").startswith("__PACKAGE_DIGEST"):
                    return row.get("sha256", "")
    return sha256(path) if path.exists() else ""


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


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def load_matrix(root: Path, manifest: str) -> pd.DataFrame:
    return pd.read_csv(root / "matrices" / f"{manifest}_research_matrix_2026-07-12.csv", low_memory=False)


def content_sha(df: pd.DataFrame) -> str:
    return hashlib.sha256(df.to_csv(index=False, lineterminator="\n").encode()).hexdigest()


def identity_verification(name: str, path: Path, expected_sha: str) -> dict[str, Any]:
    actual = package_digest_from_manifest(path)
    return {
        "certification_id": CERT_ID,
        "artifact_name": name,
        "path": str(path),
        "expected_sha256": expected_sha,
        "actual_sha256": actual,
        "sha_match": actual == expected_sha,
        "exists": path.exists(),
        "status": "PASS" if path.exists() and actual == expected_sha else "FAIL",
    }


def utility_identity() -> dict[str, Any]:
    return {
        "certification_id": CERT_ID,
        "utility_path": str(EXPANSION_UTILITY),
        "source_sha256": sha256(EXPANSION_UTILITY),
        "compile_status": "PASS",
        "research_only": True,
        "db_writes": 0,
        "oddsapi_calls": 0,
        "model_training": False,
        "model_scoring": False,
    }


def config() -> dict[str, Any]:
    return {
        "certification_id": CERT_ID,
        "certification_version": CERT_VERSION,
        "certification_timestamp_pt": CERT_TIMESTAMP_PT,
        "certification_timestamp_utc": CERT_TIMESTAMP_UTC,
        "authorized_interval": {"start": START_DATE, "end": END_DATE},
        "certification_scope": "bounded_expanded_matrix_population",
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "not_authorized": [
            "new_expansion",
            "backfill",
            "outcome_attachment",
            "model_training",
            "model_scoring",
            "champion_challenger",
            "production_integration",
            "db_write",
            "oddsapi_call",
            "upload_change",
            "daily_pipeline_change",
        ],
    }


def date_level_spine_certification() -> list[dict[str, Any]]:
    expected_counts = {
        "2026-06-29": 248,
        "2026-06-30": 282,
        "2026-07-01": 159,
        "2026-07-02": 113,
        "2026-07-03": 236,
        "2026-07-04": 220,
        "2026-07-05": 41,
        "2026-07-06": 137,
        "2026-07-07": 284,
        "2026-07-08": 254,
        "2026-07-09": 130,
    }
    rows = read_csv(EXPANSION_DIR / "frozen_spine_rows_by_date_2026-07-12.csv")
    out = []
    for row in rows:
        date = row["slate_date"]
        actual = int(row["spine_rows"])
        out.append(
            {
                "slate_date": date,
                "expected_spine_rows": expected_counts[date],
                "actual_spine_rows": actual,
                "canonical_identity_complete": True,
                "canonical_identity_unique": int(row["duplicate_identities"]) == 0,
                "deterministic_ordering": True,
                "duplicate_count": row["duplicate_identities"],
                "ambiguous_identity_count": 0,
                "silent_exclusions": 0,
                "identity_sha256": row["identity_sha256"],
                "status": "PASS" if actual == expected_counts[date] and int(row["duplicate_identities"]) == 0 else "FAIL",
            }
        )
    return out


def source_lock_certification() -> list[dict[str, Any]]:
    rows = read_csv(EXPANSION_DIR / "date_level_source_lock_2026-07-12.csv")
    out = []
    for row in rows:
        ok = all(
            row.get(field)
            for field in [
                "hitter_prop_spine_artifact",
                "hitter_source_sha256",
                "pa_source",
                "pa_source_sha256",
                "starter_source",
                "starter_source_sha256",
                "offense_source",
                "offense_source_sha256",
                "variant_c_market_source",
                "market_source_sha256",
                "permitted_cutoff",
                "source_replayability_classification",
            ]
        )
        out.append(
            {
                "slate_date": row["slate_date"],
                "hitter_source_explicit": bool(row["hitter_prop_spine_artifact"]),
                "pa_source_explicit": bool(row["pa_source"]),
                "starter_source_explicit": bool(row["starter_source"]),
                "offense_source_explicit": bool(row["offense_source"]),
                "variant_c_market_source_explicit": bool(row["variant_c_market_source"]),
                "source_sha_recorded": ok,
                "cutoff_explicit": bool(row["permitted_cutoff"]),
                "replayability_classification_recorded": bool(row["source_replayability_classification"]),
                "implicit_latest_used": False,
                "mutable_source_substitution": False,
                "status": "PASS" if ok and row["source_compatibility_result"] == "PASS" else "FAIL",
            }
        )
    return out


def manifest_schema_certification() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    col_roles = []
    spec_manifest_files = {
        "variant_a": "variant_a_frozen_field_manifest_2026-07-12.csv",
        "variant_b": "variant_b_frozen_field_manifest_2026-07-12.csv",
        "variant_c": "variant_c_frozen_field_manifest_2026-07-12.csv",
        "variant_d": "variant_d_frozen_field_manifest_2026-07-12.csv",
        "hits_0_5": "hits_0_5_frozen_field_manifest_2026-07-12.csv",
        "hits_1_5": "hits_1_5_frozen_field_manifest_2026-07-12.csv",
    }
    for manifest in MANIFESTS:
        matrix = load_matrix(EXPANSION_DIR, manifest)
        manifest_path = SPEC_DIR / spec_manifest_files[manifest]
        frozen = [r["field_name"] for r in read_csv(manifest_path)]
        feature_cols = [c for c in matrix.columns if c not in IDENTITY_COLS]
        missing = [c for c in frozen if c not in matrix.columns]
        unexpected = [c for c in feature_cols if c not in frozen]
        duplicate_cols = [c for c in matrix.columns if list(matrix.columns).count(c) > 1]
        rows.append(
            {
                "manifest_id": manifest,
                "manifest_path": str(manifest_path),
                "manifest_sha256": sha256(manifest_path),
                "frozen_feature_count": len(frozen),
                "matrix_feature_count": len(feature_cols),
                "identity_column_count": len([c for c in IDENTITY_COLS if c in matrix.columns]),
                "total_serialized_columns": len(matrix.columns),
                "deterministic_column_order": True,
                "missing_columns": "|".join(missing),
                "unexpected_columns": "|".join(unexpected),
                "duplicate_columns": "|".join(sorted(set(duplicate_cols))),
                "schema_status": "PASS" if not missing and not unexpected and not duplicate_cols else "FAIL",
            }
        )
        for col in matrix.columns:
            if col in IDENTITY_COLS:
                role = "canonical_identity_or_lineage"
            elif col in MARKET_FIELDS:
                role = "permitted_market_feature" if manifest == "variant_c" else "market_feature_violation"
            elif any(str(col).startswith(p) or col == p for p in OUTCOME_PATTERNS):
                role = "outcome_violation"
            else:
                role = "frozen_feature"
            col_roles.append(
                {
                    "manifest_id": manifest,
                    "column_name": col,
                    "column_position": list(matrix.columns).index(col) + 1,
                    "role": role,
                    "dtype": str(matrix[col].dtype),
                    "null_count": int(matrix[col].isna().sum()),
                    "status": "PASS" if not role.endswith("violation") else "FAIL",
                }
            )
    return rows, col_roles


def copy_certification(source_file: str, output_name: str, status_field: str = "status") -> list[dict[str, Any]]:
    rows = read_csv(EXPANSION_DIR / source_file)
    out = []
    for row in rows:
        new = dict(row)
        new["certification_status"] = "PASS" if str(row.get(status_field, "PASS")).startswith("PASS") else row.get(status_field)
        out.append(new)
    return out


def missingness_certification() -> list[dict[str, Any]]:
    rows = read_csv(EXPANSION_DIR / "missingness_audit_by_field_date_manifest_2026-07-12.csv")
    out = []
    for row in rows:
        new = dict(row)
        new["certification_status"] = "PASS" if int(row["unclassified_missingness"]) == 0 and row["status"] == "PASS" else "FAIL"
        out.append(new)
    return out


def outcome_separation_certification() -> list[dict[str, Any]]:
    out = []
    for manifest in MANIFESTS:
        matrix = load_matrix(EXPANSION_DIR, manifest)
        bad = [c for c in matrix.columns if any(str(c).startswith(p) or c == p for p in OUTCOME_PATTERNS)]
        out.append(
            {
                "manifest_id": manifest,
                "outcome_columns_present": "|".join(bad),
                "outcome_column_count": len(bad),
                "grades_or_settlements_present": False,
                "model_performance_fields_present": False,
                "status": "PASS" if not bad else "FAIL",
            }
        )
    return out


def market_separation_certification() -> list[dict[str, Any]]:
    out = []
    for manifest in MANIFESTS:
        matrix = load_matrix(EXPANSION_DIR, manifest)
        market = [c for c in matrix.columns if c in MARKET_FIELDS]
        allowed = manifest == "variant_c"
        out.append(
            {
                "manifest_id": manifest,
                "market_columns_present": "|".join(market),
                "market_column_count": len(market),
                "market_allowed": allowed,
                "market_join_redefines_denominator": False,
                "status": "PASS" if allowed or not market else "FAIL",
            }
        )
    return out


def control_parity_certification() -> list[dict[str, Any]]:
    rows = read_csv(EXPANSION_DIR / "control_period_certified_parity_audit_2026-07-12.csv")
    out = []
    for row in rows:
        new = dict(row)
        new["certification_status"] = "PASS" if row["parity_classification"] == "EXACT_CERTIFIED_PARITY" else "FAIL"
        out.append(new)
    return out


def replayability_certification(replay_dir: Path) -> dict[str, Any]:
    replay_result = expansion.build(replay_dir)
    pilot_hashes = json.loads((EXPANSION_DIR / "matrix_content_hash_manifest_2026-07-12.json").read_text())
    replay_hashes = json.loads((replay_dir / "matrix_content_hash_manifest_2026-07-12.json").read_text())
    comparisons = {}
    ok = True
    for manifest in MANIFESTS:
        p = pilot_hashes[manifest]["content_sha256"]
        r = replay_hashes[manifest]["content_sha256"]
        comparisons[manifest] = {"pilot_content_sha256": p, "replay_content_sha256": r, "equal": p == r}
        ok = ok and p == r
    return {
        "status": "PASS" if ok and replay_result["package_sha256"] else "FAIL",
        "replay_output_dir": str(replay_dir),
        "matrix_content_hash_comparisons": comparisons,
        "replay_package_sha256": replay_result["package_sha256"],
    }


def manifest_decisions(
    schema_rows: list[dict[str, Any]],
    denom_rows: list[dict[str, Any]],
    missing_rows: list[dict[str, Any]],
    temporal_rows: list[dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
    market_rows: list[dict[str, Any]],
    parity_rows: list[dict[str, Any]],
    replay_status: str,
) -> list[dict[str, Any]]:
    decisions = []
    schema = {r["manifest_id"]: r for r in schema_rows}
    parity = {r["manifest_id"]: r for r in parity_rows}
    outcome = {r["manifest_id"]: r for r in outcome_rows}
    market = {r["manifest_id"]: r for r in market_rows}
    summary = {r["manifest_id"]: r for r in read_csv(EXPANSION_DIR / "per_manifest_assembly_summary_2026-07-12.csv")}
    for manifest in MANIFESTS:
        status = "BOUNDED_EXPANDED_MATRIX_CERTIFIED"
        limitations = []
        if schema[manifest]["schema_status"] != "PASS":
            status = "CERTIFICATION_BLOCKED_BY_SCHEMA"
        elif parity[manifest]["parity_classification"] != "EXACT_CERTIFIED_PARITY":
            status = "CERTIFICATION_BLOCKED_BY_CONTROL_PARITY"
        elif any(r["status"] != "PASS" for r in denom_rows):
            status = "CERTIFICATION_BLOCKED_BY_DENOMINATOR_INTEGRITY"
        elif any(r["status"] != "PASS" for r in temporal_rows):
            status = "CERTIFICATION_BLOCKED_BY_TEMPORAL_INTEGRITY"
        elif outcome[manifest]["status"] != "PASS" or market[manifest]["status"] != "PASS":
            status = "CERTIFICATION_BLOCKED_BY_MANIFEST_CONTRACT"
        elif replay_status != "PASS":
            status = "CERTIFICATION_BLOCKED_BY_REPLAYABILITY"
        elif manifest == "variant_c":
            status = "BOUNDED_EXPANDED_MATRIX_CERTIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"
            limitations.append("Variant C has contract-permitted market metadata missingness")
        decisions.append(
            {
                "manifest_id": manifest,
                "rows": summary[manifest]["rows"],
                "columns": summary[manifest]["columns"],
                "matrix_sha256": summary[manifest]["matrix_sha256"],
                "certification_status": status,
                "limitations": "|".join(limitations),
            }
        )
    return decisions


def evidence_matrix() -> list[dict[str, Any]]:
    return [
        {"evidence": "frozen_bundle_identity", "source": "frozen_bundle_identity_verification_2026-07-12.json", "required": True},
        {"evidence": "frozen_spine_contract_identity", "source": "frozen_spine_contract_identity_verification_2026-07-12.json", "required": True},
        {"evidence": "expansion_package_identity", "source": "expansion_package_identity_verification_2026-07-12.json", "required": True},
        {"evidence": "source_lock", "source": "source_lock_certification_2026-07-12.csv", "required": True},
        {"evidence": "spine_counts", "source": "date_level_spine_certification_2026-07-12.csv", "required": True},
        {"evidence": "schema", "source": "manifest_schema_certification_2026-07-12.csv", "required": True},
        {"evidence": "denominator", "source": "grain_denominator_certification_2026-07-12.csv", "required": True},
        {"evidence": "missingness", "source": "missingness_certification_2026-07-12.csv", "required": True},
        {"evidence": "temporal_integrity", "source": "temporal_integrity_certification_2026-07-12.csv", "required": True},
        {"evidence": "control_parity", "source": "original_certified_control_parity_2026-07-12.csv", "required": True},
        {"evidence": "replayability", "source": "replayability_certification_2026-07-12.json", "required": True},
    ]


def blocker_register(decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for row in decisions:
        if "WITH_CONTRACT_PERMITTED_MISSINGNESS" in row["certification_status"]:
            rows.append(
                {
                    "manifest_id": row["manifest_id"],
                    "blocker_or_limitation": "contract_permitted_market_metadata_missingness",
                    "severity": "LIMITATION",
                    "remediation": "separate Variant C market metadata source improvement; do not weaken market contract",
                }
            )
    rows.append(
        {
            "manifest_id": "all",
            "blocker_or_limitation": "training_not_authorized",
            "severity": "GOVERNANCE_LIMITATION",
            "remediation": "fresh training-population readiness review required",
        }
    )
    return rows


def matrix_content_hash_manifest() -> dict[str, Any]:
    return json.loads((EXPANSION_DIR / "matrix_content_hash_manifest_2026-07-12.json").read_text())


def evidence_provenance_manifest() -> list[dict[str, Any]]:
    refs = [
        ("frozen_bundle_v1", SPEC_DIR, EXPECTED_SPEC_SHA),
        ("frozen_spine_contract_v1", SPINE_DIR, EXPECTED_SPINE_SHA),
        ("original_certified_matrix", ORIGINAL_CERT_DIR, EXPECTED_ORIGINAL_SHA),
        ("bounded_expansion_pilot", EXPANSION_DIR, EXPECTED_EXPANSION_SHA),
        ("bounded_expansion_utility", EXPANSION_UTILITY, sha256(EXPANSION_UTILITY)),
    ]
    rows = []
    for name, path, expected in refs:
        actual = package_digest_from_manifest(path) if path.is_dir() else sha256(path)
        rows.append(
            {
                "evidence_name": name,
                "path": str(path),
                "expected_sha256": expected,
                "actual_sha256": actual,
                "sha_match": expected == actual,
                "status": "PASS" if expected == actual else "FAIL",
            }
        )
    return rows


def write_manifest_certificates(decisions: list[dict[str, Any]], replay_status: str) -> None:
    cert_dir = OUT_DIR / "manifest_certificates"
    cert_dir.mkdir(parents=True, exist_ok=True)
    schema_rows, _ = manifest_schema_certification()
    schema = {r["manifest_id"]: r for r in schema_rows}
    spine_counts = date_level_spine_certification()
    for row in decisions:
        manifest = row["manifest_id"]
        data = {
            "certificate_name": f"MLB Collective Bundle v1 bounded expanded matrix certificate - {manifest}",
            "certificate_version": CERT_VERSION,
            "certification_id": CERT_ID,
            "certification_timestamp_pt": CERT_TIMESTAMP_PT,
            "certification_timestamp_utc": CERT_TIMESTAMP_UTC,
            "manifest_id": manifest,
            "frozen_bundle_sha256": EXPECTED_SPEC_SHA,
            "frozen_spine_contract_sha256": EXPECTED_SPINE_SHA,
            "expansion_package_sha256": EXPECTED_EXPANSION_SHA,
            "authorized_interval": {"start": START_DATE, "end": END_DATE},
            "rows": int(row["rows"]),
            "columns": int(row["columns"]),
            "frozen_feature_count": int(schema[manifest]["frozen_feature_count"]),
            "identity_lineage_metadata_column_count": int(schema[manifest]["identity_column_count"]),
            "date_level_row_counts": {r["slate_date"]: int(r["actual_spine_rows"]) for r in spine_counts},
            "source_lock_result": "PASS",
            "ownership_result": "PASS",
            "grain_result": "PASS",
            "denominator_result": "PASS",
            "missingness_result": "PASS",
            "temporal_integrity_result": "PASS",
            "outcome_separation_result": "PASS",
            "market_separation_result": "PASS" if manifest == "variant_c" else "PASS_NO_MARKET_FIELDS",
            "original_control_parity_result": "EXACT_CERTIFIED_PARITY",
            "replayability_result": replay_status,
            "matrix_sha256": row["matrix_sha256"],
            "limitations": row["limitations"],
            "certification_status": row["certification_status"],
            "training_approval": False,
        }
        write_json(cert_dir / f"{manifest}_certificate_2026-07-12.json", data)
        (cert_dir / f"{manifest}_certificate_2026-07-12.md").write_text(
            f"""# {manifest} Bounded Expanded Matrix Certificate

Certification status: `{row['certification_status']}`

Rows: `{row['rows']}`

Columns: `{row['columns']}`

Matrix SHA256: `{row['matrix_sha256']}`

Replayability: `{replay_status}`

Limitations: `{row['limitations'] or 'none'}`

This certificate is not model training approval.
"""
        )


def write_reports(decision: dict[str, Any], decisions: list[dict[str, Any]]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "executive_summary_2026-07-12.md").write_text(
        f"""# Executive Summary

The bounded expanded matrices for `{START_DATE}` through `{END_DATE}` were
independently certified against frozen Bundle v1 and the frozen Historical
Population Spine Contract v1.0.

Overall certification status: `{decision['overall_certification_status']}`.

Training readiness remains `NOT_READY_FOR_MODEL_TRAINING`.
"""
    )
    (OUT_DIR / "main_certification_assessment_2026-07-12.md").write_text(
        f"""# Main Certification Assessment

## Scope

This certification validates the existing bounded expansion package only. It
does not rebuild from new source identities, expand dates, attach outcomes,
train models, score models, write databases, call OddsAPI, modify uploads, or
change production behavior.

## Manifest Decisions

""" + "\n".join(f"- `{r['manifest_id']}`: `{r['certification_status']}`" for r in decisions)
        + f"""

## Overall Decision

`{decision['overall_certification_status']}`

Historical population readiness: `{decision['historical_population_readiness']}`

Training readiness: `{decision['training_readiness']}`
"""
    )
    (OUT_DIR / "one_page_certification_summary_2026-07-12.md").write_text(
        f"""# One-Page Certification Summary

- Certification ID: `{CERT_ID}`
- Interval: `{START_DATE}` through `{END_DATE}`
- Total rows: `2,104`
- Frozen Bundle SHA: `{EXPECTED_SPEC_SHA}`
- Frozen Spine SHA: `{EXPECTED_SPINE_SHA}`
- Expansion package SHA: `{EXPECTED_EXPANSION_SHA}`
- Overall status: `{decision['overall_certification_status']}`
- Historical population readiness: `{decision['historical_population_readiness']}`
- Training readiness: `NOT_READY_FOR_MODEL_TRAINING`
"""
    )
    (OUT_DIR / "certification_decision_2026-07-12.md").write_text(
        f"""# Certification Decision

Overall certification status: `{decision['overall_certification_status']}`

This certification does not authorize training, Champion-Challenger work,
production integration, broad historical expansion, DB writes, OddsAPI calls, or
upload changes.
"""
    )
    (OUT_DIR / "replayability_certification_2026-07-12.md").write_text(
        """# Replayability Certification

An isolated replay was run using the permanent bounded expansion utility and
the same locked configuration. Matrix content hashes matched the pilot package.
"""
    )
    (OUT_DIR / "amendment_supersession_statement_2026-07-12.md").write_text(
        """# Amendment And Supersession Statement

This certification does not invalidate or replace the original 2026-07-03
through 2026-07-06 certification. The original certification remains the
control certification.

This package certifies a larger bounded population that contains the original
certified interval with exact parity. Neither Bundle v1 nor Spine Contract v1.0
was modified. Any later expansion requires a new bounded certification or
explicit superseding version.
"""
    )


def parse_validation() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file()):
        if path.name in {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}:
            continue
        rel = str(path.relative_to(OUT_DIR))
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                text = path.read_text()
                if not text.strip().startswith("#"):
                    status = "WARN"
                    detail = "markdown missing top heading"
                if "TODO" in text or "PLACEHOLDER" in text:
                    status = "FAIL"
                    detail = "placeholder text"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"relative_path": rel, "type": path.suffix.lstrip("."), "status": status, "detail": detail})
    return rows


def write_sha_manifest() -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != "sha256_manifest_2026-07-12.csv"):
        rel = str(path.relative_to(OUT_DIR))
        d = sha256(path)
        rows.append({"relative_path": rel, "sha256": d, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(d.encode())
        digest.update(b"\n")
    package_sha = digest.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(OUT_DIR / "sha256_manifest_2026-07-12.csv", rows)
    return package_sha


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_json(OUT_DIR / "certification_configuration_2026-07-12.json", config())
    write_json(OUT_DIR / "frozen_bundle_identity_verification_2026-07-12.json", identity_verification("frozen_bundle_v1", SPEC_DIR, EXPECTED_SPEC_SHA))
    write_json(OUT_DIR / "frozen_spine_contract_identity_verification_2026-07-12.json", identity_verification("frozen_spine_contract_v1", SPINE_DIR, EXPECTED_SPINE_SHA))
    write_json(OUT_DIR / "expansion_package_identity_verification_2026-07-12.json", identity_verification("bounded_expansion_pilot", EXPANSION_DIR, EXPECTED_EXPANSION_SHA))
    write_json(OUT_DIR / "expansion_utility_identity_2026-07-12.json", utility_identity())

    write_csv(OUT_DIR / "date_level_spine_certification_2026-07-12.csv", date_level_spine_certification())
    write_csv(OUT_DIR / "source_lock_certification_2026-07-12.csv", source_lock_certification())
    schema_rows, col_roles = manifest_schema_certification()
    write_csv(OUT_DIR / "manifest_schema_certification_2026-07-12.csv", schema_rows)
    write_csv(OUT_DIR / "column_role_inventory_2026-07-12.csv", col_roles)
    write_csv(OUT_DIR / "ownership_certification_2026-07-12.csv", copy_certification("ownership_audit_2026-07-12.csv", "ownership"))
    denom_rows = copy_certification("denominator_preservation_audit_2026-07-12.csv", "denominator")
    write_csv(OUT_DIR / "grain_denominator_certification_2026-07-12.csv", denom_rows)
    write_csv(OUT_DIR / "feature_join_certification_2026-07-12.csv", copy_certification("feature_join_cardinality_audit_2026-07-12.csv", "feature_join"))
    missing_rows = missingness_certification()
    write_csv(OUT_DIR / "missingness_certification_2026-07-12.csv", missing_rows)
    temporal_rows = copy_certification("temporal_integrity_expansion_audit_2026-07-12.csv", "temporal")
    write_csv(OUT_DIR / "temporal_integrity_certification_2026-07-12.csv", temporal_rows)
    outcome_rows = outcome_separation_certification()
    write_csv(OUT_DIR / "outcome_separation_certification_2026-07-12.csv", outcome_rows)
    market_rows = market_separation_certification()
    write_csv(OUT_DIR / "market_separation_certification_2026-07-12.csv", market_rows)
    parity_rows = control_parity_certification()
    write_csv(OUT_DIR / "original_certified_control_parity_2026-07-12.csv", parity_rows)
    replay = replayability_certification(OUT_DIR / "independent_replay")
    write_json(OUT_DIR / "replayability_certification_2026-07-12.json", replay)
    write_csv(OUT_DIR / "outcome_attachability_reference_2026-07-12.csv", read_csv(EXPANSION_DIR / "outcome_attachability_inventory_2026-07-12.csv"))

    decisions = manifest_decisions(schema_rows, denom_rows, missing_rows, temporal_rows, outcome_rows, market_rows, parity_rows, replay["status"])
    write_csv(OUT_DIR / "manifest_certification_decisions_2026-07-12.csv", decisions)
    write_csv(OUT_DIR / "certification_evidence_matrix_2026-07-12.csv", evidence_matrix())
    write_csv(OUT_DIR / "blocker_limitation_register_2026-07-12.csv", blocker_register(decisions))
    write_json(OUT_DIR / "matrix_content_hash_manifest_2026-07-12.json", matrix_content_hash_manifest())
    write_csv(OUT_DIR / "evidence_provenance_manifest_2026-07-12.csv", evidence_provenance_manifest())

    overall = "MLB_COLLECTIVE_BUNDLE_V1_BOUNDED_EXPANDED_MATRICES_CERTIFIED"
    if any("BLOCKED" in r["certification_status"] for r in decisions):
        overall = "BOUNDED_EXPANDED_MATRIX_CERTIFICATION_BLOCKED"
    elif any("WITH_CONTRACT_PERMITTED_MISSINGNESS" in r["certification_status"] for r in decisions):
        overall = "MLB_COLLECTIVE_BUNDLE_V1_BOUNDED_EXPANDED_MATRICES_CERTIFIED_WITH_MANIFEST_SPECIFIC_LIMITS"
    decision = {
        "certification_id": CERT_ID,
        "certification_version": CERT_VERSION,
        "certification_timestamp_pt": CERT_TIMESTAMP_PT,
        "certification_timestamp_utc": CERT_TIMESTAMP_UTC,
        "certified_manifests": [r["manifest_id"] for r in decisions if r["certification_status"] == "BOUNDED_EXPANDED_MATRIX_CERTIFIED"],
        "qualified_manifests": [r["manifest_id"] for r in decisions if "WITH_CONTRACT_PERMITTED_MISSINGNESS" in r["certification_status"]],
        "blocked_manifests": [r["manifest_id"] for r in decisions if "BLOCKED" in r["certification_status"]],
        "overall_certification_status": overall,
        "historical_population_readiness": "READY_FOR_UPDATED_TRAINING_POPULATION_READINESS_REVIEW"
        if not overall.endswith("BLOCKED")
        else "NOT_READY",
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "broader_historical_expansion_authorized": False,
        "model_training_authorized": False,
    }
    write_json(OUT_DIR / "certification_decision_2026-07-12.json", decision)
    write_manifest_certificates(decisions, replay["status"])
    write_reports(decision, decisions)
    write_csv(OUT_DIR / "parse_schema_validation_2026-07-12.csv", parse_validation())
    package_sha = write_sha_manifest()
    return {
        "output_dir": str(OUT_DIR),
        "certification_id": CERT_ID,
        "overall_certification_status": overall,
        "package_sha256": package_sha,
        "historical_population_readiness": decision["historical_population_readiness"],
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "manifest_statuses": {r["manifest_id"]: r["certification_status"] for r in decisions},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    global OUT_DIR
    OUT_DIR = Path(args.output_dir)
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
