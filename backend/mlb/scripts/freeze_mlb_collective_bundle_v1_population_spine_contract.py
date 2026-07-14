#!/usr/bin/env python3
"""Freeze MLB Collective Bundle v1 Historical Population Spine Contract v1.0.

Governance artifact writer only. This script creates a frozen, machine-readable
and human-readable contract package from already approved evidence. It does not
modify Bundle v1, certified matrices, production behavior, databases, uploads,
or model artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
MATRIX_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_matrix_assembly/2026-07-12")
TRAINING_READINESS_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_training_population_readiness/2026-07-12"
)
EXPANSION_DESIGN_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_source_expansion_design/2026-07-12"
)
SPINE_REVIEW_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_review/2026-07-12"
)
PILOT_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_population_spine_implementation_pilot_1/2026-07-12"
)
PILOT_SCRIPT = Path("backend/mlb/scripts/implement_mlb_collective_bundle_v1_population_spine_pilot.py")

CONTRACT_NAME = "MLB Collective Bundle v1 Historical Population Spine Contract"
CONTRACT_VERSION = "1.0"
CONTRACT_IDENTIFIER = "MLB_COLLECTIVE_BUNDLE_V1_HISTORICAL_POPULATION_SPINE_V1"
FREEZE_DECISION = "MLB_COLLECTIVE_BUNDLE_V1_HISTORICAL_POPULATION_SPINE_V1_FROZEN"
STATUS = "FROZEN"
FREEZE_TIMESTAMP_PT = "2026-07-12T19:31:08-07:00"
FREEZE_TIMESTAMP_UTC = "2026-07-13T02:31:08Z"
CANONICAL_IDENTITY = ["slate_date", "game_id", "player_id", "prop_type", "line", "side"]

EXPECTED_EVIDENCE_SHAS = {
    "frozen_bundle_specification": "0ef4bb6d227d690602dd6de10974432110e0923d25e406129fa8938ae6bb1833",
    "certified_matrix_assembly": "f578be44c2393c85c59b37c5c3acff6898b6dcf29f13b7d3fd2bc921a9ebd135",
    "population_spine_review": "33156cf8ed617b33c0668990a22974a095b9c53220a6dabdfc0a28d2fa727706",
    "population_spine_implementation_pilot_1": "a299def56dcc1f117a27a1d51a2d3412b4dd2bc0dda2416b50802ee9f1659e26",
}


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
    if not manifests:
        return sha256(path) if path.exists() else ""
    with manifests[0].open(newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("relative_path", "").startswith("__PACKAGE_DIGEST"):
                return row.get("sha256", "")
    return sha256(path) if path.exists() else ""


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


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


def identity_block() -> dict[str, Any]:
    return {
        "contract_name": CONTRACT_NAME,
        "contract_version": CONTRACT_VERSION,
        "contract_identifier": CONTRACT_IDENTIFIER,
        "status": STATUS,
        "freeze_decision": FREEZE_DECISION,
        "freeze_timestamp_pt": FREEZE_TIMESTAMP_PT,
        "freeze_timestamp_utc": FREEZE_TIMESTAMP_UTC,
        "canonical_identity": CANONICAL_IDENTITY,
        "population_owner": "date_locked_hitter_prop_source_artifact",
        "architecture": "shared_baseball_state_spine_plus_separate_variant_c_market_join",
        "implementation_binding": {
            "binding_status": "retained_as_reference_implementation",
            "source_path": str(PILOT_SCRIPT),
            "source_sha256": sha256(PILOT_SCRIPT) if PILOT_SCRIPT.exists() else "",
            "research_only": True,
            "default_certified_assembler_behavior_changed": False,
        },
    }


def contract_json() -> dict[str, Any]:
    return {
        **identity_block(),
        "purpose": {
            "defines": "canonical historical candidate denominator for Bundle v1 research matrix assembly and future bounded historical expansion",
            "does_not_define": [
                "feature_values",
                "outcomes",
                "model_labels",
                "market_pricing",
                "training_authorization",
                "production_behavior",
            ],
        },
        "population_ownership": {
            "owner": "explicit date-locked hitter-prop source artifact",
            "non_owners": ["PA Opportunity", "Starter Skill / Workload", "Offense Context", "Market joins"],
            "feature_gap_policy": "missingness or compatibility issue; never silent denominator removal",
        },
        "source_identity_and_date_lock": {
            "requires": [
                "explicit source artifact path or immutable source identifier",
                "explicit run tag where applicable",
                "explicit slate date",
                "explicit cutoff policy",
                "source SHA or equivalent source identity where available",
            ],
            "forbids": [
                "implicit latest available",
                "unversioned mutable source substitution",
                "silent fallback to a different source artifact",
                "cross-date source reuse without explicit contract rule",
            ],
        },
        "feature_join_contract": {
            "join_type": "left join from frozen spine unless another frozen Bundle v1 contract explicitly requires otherwise",
            "required_audits": [
                "input spine row count",
                "output row count",
                "row delta",
                "duplicate delta",
                "join cardinality",
                "unmatched-row classification",
                "no denominator changes",
                "no row multiplication",
                "no silent row loss",
            ],
        },
        "variant_c": {
            "derivation": "separate explicitly governed market join from shared spine",
            "must_record": ["market source", "snapshot identity", "cutoff", "permitted market fields", "unmatched market rows"],
            "must_not": "redefine shared baseball-state denominator",
        },
        "amendment_policy": "v1 is immutable; identity, ownership, eligibility, cutoff, source, dedupe, ordering, joins, or Variant C changes require new amendment and version",
    }


def markdown_contract() -> str:
    return f"""# {CONTRACT_NAME} v{CONTRACT_VERSION}

Status: `{STATUS}`

Contract identifier: `{CONTRACT_IDENTIFIER}`

Freeze decision: `{FREEZE_DECISION}`

Freeze timestamp PT: `{FREEZE_TIMESTAMP_PT}`

Freeze timestamp UTC: `{FREEZE_TIMESTAMP_UTC}`

## Purpose

This contract defines the canonical historical candidate denominator used for
Bundle v1 research matrix assembly and future bounded historical expansion. It
does not define feature values, outcomes, model labels, market pricing,
training authorization, or production behavior.

## Population Ownership

The denominator is owned by an explicit date-locked hitter-prop source artifact.
PA Opportunity, Starter Skill / Workload, Offense Context, and market joins may
only join into the spine. They cannot own eligibility, redefine the denominator,
or silently remove rows. Feature-source gaps remain missingness or compatibility
issues unless a future frozen contract explicitly states otherwise.

## Canonical Identity

Frozen identity:

`slate_date | game_id | player_id | prop_type | line | side`

Player name, team, opponent, book, snapshot tag, snapshot timestamp, and source
artifact identity are lineage, validation, display, or Variant C metadata. They
are not part of the base baseball-state identity.

## Source Identity And Date Lock

Every build must use explicit archived source artifacts, immutable source
identities, or explicit run tags. Implicit `latest available`, unversioned
mutable source substitution, silent fallback, and cross-date reuse without an
explicit rule are forbidden.

## Eligibility

Rows require slate date, game ID, player ID, prop type, line, and side after
normalization. Unsupported, malformed, missing-source, duplicate, and
ambiguous rows must be classified with explicit exclusion reasons. No silent
exclusions are permitted.

## Deduplication And Ordering

Duplicate detection uses the frozen canonical identity. Exact duplicates may be
handled only by a deterministic winner-selection rule; ambiguous conflicts and
many-to-many identities fail closed. Canonical serialization is sorted by the
canonical identity fields in order.

## Feature Joins

Feature platforms join left from the frozen spine. Each join must report input
row count, output row count, row delta, duplicate delta, cardinality,
unmatched-row classification, and denominator preservation.

## Shared And Derived Populations

Variants A, B, D, Hits 0.5, and Hits 1.5 use the shared baseball-state spine.
Variant C derives market fields through a separate governed market join and may
not constrain the shared denominator.

## Temporal Integrity

Sources must be valid at the permitted historical cutoff. Postgame
contamination, future snapshots, later lineup knowledge, mutable current-state
substitution, and future-derived outcomes or diagnostics are prohibited.

## Replayability

Deterministic reconstruction from the same locked inputs must reproduce row
count, identities, ordering, eligibility decisions, exclusion reasons, lineage,
output SHA256, and validation SHA256 exactly.

## Compatibility

The certified July 3 spine was reproduced exactly, including 236 rows and the
control identity SHA `defa5d1cf612fef4a873910cedb0a7519c7e7e28cc164cf71a23d66a1ce83919`.
The existing July 3-6 Bundle v1 matrix certification remains valid. This freeze
does not rewrite or supersede the certified assembly package.

## Amendment Policy

Version 1.0 is immutable. Any change to canonical identity, denominator owner,
eligibility, cutoff, source selection, deduplication, ordering, feature-join
behavior, or Variant C derivation requires a proposed amendment, impact
analysis, compatibility review, new contract version, new SHA identity, and new
certification where population identity changes.
"""


def registry_rows() -> list[dict[str, Any]]:
    rows = []
    for rule_id, category, rule, status in [
        ("purpose_001", "purpose", "defines historical candidate denominator only", "FROZEN"),
        ("owner_001", "population_owner", "date-locked hitter-prop source artifact owns denominator", "FROZEN"),
        ("identity_001", "canonical_identity", "slate_date|game_id|player_id|prop_type|line|side", "FROZEN"),
        ("source_001", "source_selection", "explicit archived source artifact or run tag required", "FROZEN"),
        ("source_002", "source_selection", "implicit latest available prohibited", "FROZEN"),
        ("eligibility_001", "eligibility", "required identifiers and supported prop/line/side after normalization", "FROZEN"),
        ("dedupe_001", "deduplication", "canonical identity detects duplicate rows", "FROZEN"),
        ("dedupe_002", "deduplication", "ambiguous conflicts fail closed", "FROZEN"),
        ("ordering_001", "ordering", "canonical sort by identity fields", "FROZEN"),
        ("join_001", "feature_join", "feature platforms left join into spine", "FROZEN"),
        ("join_002", "feature_join", "feature joins cannot change denominator", "FROZEN"),
        ("variant_c_001", "variant_c", "Variant C derives from governed market join", "FROZEN"),
        ("replay_001", "replayability", "same locked inputs must reproduce exact identities/order/SHA", "FROZEN"),
        ("amend_001", "amendment", "v1 immutable; population-impacting changes require new version", "FROZEN"),
    ]:
        rows.append(
            {
                "contract_identifier": CONTRACT_IDENTIFIER,
                "contract_version": CONTRACT_VERSION,
                "rule_id": rule_id,
                "category": category,
                "rule": rule,
                "status": status,
            }
        )
    return rows


def canonical_identity_rows() -> list[dict[str, Any]]:
    specs = [
        ("slate_date", "string", "YYYY-MM-DD", "required", 1),
        ("game_id", "integer_string", "integer string; no decimals", "required", 2),
        ("player_id", "integer_string", "integer string; no decimals", "required", 3),
        ("prop_type", "string", "lowercase canonical prop key", "required", 4),
        ("line", "decimal_string", "one decimal numeric string", "required", 5),
        ("side", "string", "lowercase over/under", "required", 6),
    ]
    return [
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "field_name": name,
            "data_type": dtype,
            "normalization_rule": norm,
            "null_policy": null_policy,
            "serialization_order": order,
            "hashing_behavior": "joined with pipe delimiter in serialization order",
            "status": STATUS,
        }
        for name, dtype, norm, null_policy, order in specs
    ]


def source_cutoff_rows() -> list[dict[str, Any]]:
    return [
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "source_rule": "explicit_source_artifact",
            "requirement": "source artifact path or immutable source identifier is required",
            "forbidden": "implicit latest available",
            "status": STATUS,
        },
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "source_rule": "date_lock",
            "requirement": "explicit slate date and cutoff policy required",
            "forbidden": "cross-date reuse without explicit contract rule",
            "status": STATUS,
        },
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "source_rule": "source_identity",
            "requirement": "source SHA or equivalent source identity where available",
            "forbidden": "silent fallback to alternate source",
            "status": STATUS,
        },
    ]


def eligibility_rows() -> list[dict[str, Any]]:
    reasons = [
        ("supported_prop_type", "prop_type must be supported by Bundle v1 manifest scope"),
        ("supported_line", "line must be normalized and supported by manifest scope"),
        ("supported_side", "side must normalize to over or under"),
        ("required_game", "game_id required"),
        ("required_player", "player_id required"),
        ("duplicate_identity", "duplicates use frozen dedupe policy"),
        ("missing_source", "missing source is explicit exclusion reason"),
        ("malformed_row", "malformed row is explicit exclusion reason"),
        ("contract_exclusion", "frozen exclusion contract applies where relevant"),
    ]
    return [
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule_id": rule_id,
            "rule": rule,
            "exclusion_reason_code": rule_id,
            "silent_exclusion_allowed": False,
            "status": STATUS,
        }
        for rule_id, rule in reasons
    ]


def dedupe_rows() -> list[dict[str, Any]]:
    return [
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule": "duplicate_detection_identity",
            "definition": "|".join(CANONICAL_IDENTITY),
            "behavior": "detect duplicates before feature joins",
            "status": STATUS,
        },
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule": "exact_duplicate_resolution",
            "definition": "deterministic source-order winner only when rows are exact duplicates",
            "behavior": "record duplicate audit",
            "status": STATUS,
        },
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule": "ambiguous_conflict",
            "definition": "same identity with conflicting non-identical source rows",
            "behavior": "fail closed",
            "status": STATUS,
        },
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule": "canonical_ordering",
            "definition": "sort by canonical identity serialization",
            "behavior": "stable serialization order",
            "status": STATUS,
        },
    ]


def feature_join_rows() -> list[dict[str, Any]]:
    platforms = ["PA Opportunity", "Starter Skill / Workload", "Offense Context"]
    return [
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "feature_platform": platform,
            "join_type": "left_join_from_frozen_spine",
            "denominator_owner": False,
            "required_audits": "input_rows|output_rows|row_delta|duplicate_delta|join_cardinality|unmatched_classification",
            "silent_row_loss_allowed": False,
            "row_multiplication_allowed": False,
            "status": STATUS,
        }
        for platform in platforms
    ]


def applicability_rows() -> list[dict[str, Any]]:
    rows = []
    for manifest in ["variant_a", "variant_b", "variant_d", "hits_0_5", "hits_1_5"]:
        rows.append(
            {
                "manifest_id": manifest,
                "population_type": "shared_baseball_state_spine",
                "uses_canonical_identity": "|".join(CANONICAL_IDENTITY),
                "variant_c_market_join_required": False,
                "status": STATUS,
            }
        )
    rows.append(
        {
            "manifest_id": "variant_c",
            "population_type": "shared_baseball_state_spine_with_derived_market_join",
            "uses_canonical_identity": "|".join(CANONICAL_IDENTITY),
            "variant_c_market_join_required": True,
            "status": STATUS,
        }
    )
    return rows


def variant_c_rows() -> list[dict[str, Any]]:
    return [
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule": "market_join_source",
            "definition": "explicit market source and snapshot identity required",
            "base_denominator_effect": "none",
            "status": STATUS,
        },
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule": "market_unmatched_rows",
            "definition": "record unmatched rows and null/exclusion decision under frozen Variant C contract",
            "base_denominator_effect": "none",
            "status": STATUS,
        },
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "rule": "market_limitations",
            "definition": "Variant C limitations cannot constrain shared baseball-state spine",
            "base_denominator_effect": "none",
            "status": STATUS,
        },
    ]


def replayability_rows() -> list[dict[str, Any]]:
    checks = [
        "row_count",
        "canonical_identities",
        "row_ordering",
        "eligibility_decisions",
        "exclusion_reasons",
        "lineage",
        "output_sha256",
        "validation_sha256",
    ]
    return [
        {
            "contract_identifier": CONTRACT_IDENTIFIER,
            "replay_check": check,
            "requirement": "exact deterministic equality from same locked inputs",
            "failure_action": "fail closed; do not certify expansion",
            "status": STATUS,
        }
        for check in checks
    ]


def evidence_rows() -> list[dict[str, Any]]:
    evidence = [
        ("frozen_bundle_specification", SPEC_DIR, EXPECTED_EVIDENCE_SHAS["frozen_bundle_specification"], "authoritative frozen Bundle v1 specification"),
        ("certified_matrix_assembly", MATRIX_DIR, EXPECTED_EVIDENCE_SHAS["certified_matrix_assembly"], "certified July 3-6 assembly compatibility reference"),
        ("training_population_readiness_review", TRAINING_READINESS_DIR, package_digest_from_manifest(TRAINING_READINESS_DIR), "training readiness remains not ready"),
        ("historical_source_expansion_design", EXPANSION_DESIGN_DIR, package_digest_from_manifest(EXPANSION_DESIGN_DIR), "expansion design context"),
        ("population_spine_review", SPINE_REVIEW_DIR, EXPECTED_EVIDENCE_SHAS["population_spine_review"], "definition and parity review"),
        ("population_spine_implementation_pilot_1", PILOT_DIR, EXPECTED_EVIDENCE_SHAS["population_spine_implementation_pilot_1"], "successful implementation pilot"),
        ("implementation_reference", PILOT_SCRIPT, sha256(PILOT_SCRIPT), "contract-bound research implementation reference"),
    ]
    rows = []
    for name, path, expected, role in evidence:
        actual = package_digest_from_manifest(path) if path.is_dir() else (sha256(path) if path.exists() else "")
        rows.append(
            {
                "evidence_name": name,
                "path": str(path),
                "role": role,
                "expected_sha256": expected,
                "actual_sha256": actual,
                "sha_match": expected == actual if expected else bool(actual),
                "exists": path.exists(),
                "status": "PASS" if path.exists() and (not expected or expected == actual) else "FAIL",
            }
        )
    return rows


def compatibility_binding() -> dict[str, Any]:
    control = read_csv(PILOT_DIR / "control_reproduction_audit_2026-07-12.csv")[0]
    return {
        **identity_block(),
        "certified_july_3_control": {
            "spine_rows": int(control["spine_rows"]),
            "certified_rows": int(control["certified_matrix_rows"]),
            "identity_equality": control["identity_equality"] == "True",
            "row_equality": control["row_equality"] == "True",
            "ordering_equality": control["ordering_equality"] == "True",
            "control_identity_sha256": control["spine_identity_sha256"],
            "status": control["status"],
        },
        "certified_july_3_to_6_assembly_compatibility": "PRESERVED",
        "certified_matrix_package_modified": False,
        "bundle_v1_specification_modified": False,
    }


def freeze_decision_json(package_sha: str = "") -> dict[str, Any]:
    return {
        **identity_block(),
        "final_status": FREEZE_DECISION,
        "historical_expansion_readiness": "READY_FOR_BOUNDED_INCREMENTAL_HISTORICAL_SOURCE_EXPANSION",
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "package_sha256": package_sha,
        "authorized": {
            "contract_freeze": True,
            "bounded_incremental_historical_source_expansion_readiness": True,
            "historical_backfill": False,
            "broad_matrix_expansion": False,
            "model_training": False,
            "model_scoring": False,
            "champion_challenger": False,
            "production_integration": False,
            "db_write": False,
            "oddsapi_call": False,
            "upload_change": False,
            "daily_pipeline_change": False,
        },
    }


def write_markdown_files() -> None:
    (OUT_DIR / "historical_population_spine_contract_v1_2026-07-12.md").write_text(markdown_contract())
    (OUT_DIR / "one_page_summary_2026-07-12.md").write_text(
        f"""# Population Spine Contract v1.0 Summary

`{CONTRACT_IDENTIFIER}` is frozen.

- Architecture: shared baseball-state spine plus separate Variant C market join
- Population owner: date-locked hitter-prop source artifact
- Canonical identity: `slate_date | game_id | player_id | prop_type | line | side`
- Feature platforms: PA Opportunity, Starter Skill / Workload, and Offense Context join into the spine
- July 3 control: 236 spine rows, 236 certified rows, identity/order PASS
- Historical expansion readiness: `READY_FOR_BOUNDED_INCREMENTAL_HISTORICAL_SOURCE_EXPANSION`
- Training readiness: `NOT_READY_FOR_MODEL_TRAINING`

This freeze does not authorize broad expansion, backfill, training, scoring, production integration, uploads, DB writes, or OddsAPI calls.
"""
    )
    (OUT_DIR / "historical_population_spine_freeze_rationale_2026-07-12.md").write_text(
        """# Historical Population Spine Freeze Rationale

The hitter-prop spine owns the denominator because it is the date-locked source
that reproduced the certified Bundle v1 July 3 population exactly and aligns
with the frozen matrix assembly behavior. Feature platforms are intentionally
joins because PA Opportunity, Starter Skill / Workload, and Offense Context have
their own coverage limits. Letting any of them define eligibility would create
hidden denominator drift and make feature availability indistinguishable from
candidate eligibility.

Explicit archived artifacts and run tags are required because historical matrix
assembly must be replayable. Implicit `latest available` is prohibited because it
can silently change row populations as data refreshes, corrections arrive, or
market snapshots evolve.

Option B was selected because it preserves one comparable baseball-state spine
for Variants A, B, D, Hits 0.5, and Hits 1.5 while allowing Variant C to attach
market-specific metadata through a governed derivative join. A fully
market-dependent spine would allow book/snapshot availability to dominate
baseball research. Separate uncontrolled spines would break cross-manifest
comparability.

Exact July 3 reproduction supports the freeze: the implementation pilot
reproduced 236 certified rows with identical identity and ordering, including
the control identity SHA
`defa5d1cf612fef4a873910cedb0a7519c7e7e28cc164cf71a23d66a1ce83919`.

Incomplete PA and starter join rates do not invalidate the spine because they
are feature coverage findings, not denominator findings. Their nulls must remain
auditable missingness rather than silent row removal.

The freeze improves future historical comparability by binding ownership,
identity, source selection, cutoff policy, feature joins, and replay checks
before any bounded incremental historical expansion proceeds.
"""
    )
    (OUT_DIR / "amendment_policy_2026-07-12.md").write_text(
        """# Amendment Policy

Version 1.0 is immutable.

Any change to canonical identity, denominator owner, eligibility rules, cutoff
policy, source-selection policy, deduplication, ordering, feature-join behavior,
or Variant C derivation requires:

- proposed amendment
- impact analysis
- compatibility review
- new contract version
- new SHA identity
- new certification where population identity changes

No silent mutation of v1 is permitted.
"""
    )
    (OUT_DIR / "compatibility_binding_2026-07-12.md").write_text(
        """# Compatibility Binding

The frozen spine contract is compatible with the existing Bundle v1 certified
matrix assembly. The July 3 control reproduced exactly:

- spine rows: 236
- certified rows: 236
- identity equality: PASS
- row equality: PASS
- ordering equality: PASS
- control identity SHA: `defa5d1cf612fef4a873910cedb0a7519c7e7e28cc164cf71a23d66a1ce83919`

The freeze does not rewrite, supersede, or modify the certified matrix package.
Future historical expansions must cite both the frozen Bundle v1 specification
and this frozen population spine contract.
"""
    )


def parse_validation() -> list[dict[str, Any]]:
    rows = []
    identity_values = set()
    status_values = set()
    decision_values = set()
    canonical_strings = set()
    for path in sorted(OUT_DIR.iterdir()):
        if not path.is_file() or path.name in {"sha256_manifest_2026-07-12.csv", "parse_schema_validation_2026-07-12.csv"}:
            continue
        status = "PASS"
        detail = ""
        try:
            text = path.read_text() if path.suffix in {".md", ".json"} else ""
            if path.suffix == ".json":
                data = json.loads(text)
                if isinstance(data, dict):
                    if "contract_identifier" in data:
                        identity_values.add(data["contract_identifier"])
                    if "status" in data:
                        status_values.add(data["status"])
                    if "freeze_decision" in data:
                        decision_values.add(data["freeze_decision"])
                    if "canonical_identity" in data:
                        canonical_strings.add("|".join(data["canonical_identity"]))
            elif path.suffix == ".csv":
                csv_rows = read_csv(path)
                if len(csv_rows) != len({tuple(sorted(r.items())) for r in csv_rows}) and "registry" in path.name:
                    status = "FAIL"
                    detail = "duplicate registry rows"
                for row in csv_rows:
                    if "contract_identifier" in row:
                        identity_values.add(row["contract_identifier"])
                    if "status" in row:
                        status_values.add(row["status"])
            elif path.suffix == ".md":
                if not text.strip():
                    status = "FAIL"
                    detail = "empty markdown"
                if "TODO" in text or "PLACEHOLDER" in text:
                    status = "FAIL"
                    detail = "unresolved placeholder text"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"path": path.name, "type": path.suffix.lstrip("."), "status": status, "detail": detail})
    checks = [
        ("contract_identifier_consistency", identity_values == {CONTRACT_IDENTIFIER}, sorted(identity_values)),
        ("status_consistency", STATUS in status_values and not (status_values - {STATUS, "PASS", ""}), sorted(status_values)),
        ("freeze_decision_consistency", decision_values == {FREEZE_DECISION}, sorted(decision_values)),
        ("canonical_identity_consistency", not canonical_strings or canonical_strings == {"|".join(CANONICAL_IDENTITY)}, sorted(canonical_strings)),
        ("authoritative_evidence_sha_match", all(r["status"] == "PASS" for r in evidence_rows()), ""),
    ]
    for name, ok, detail in checks:
        rows.append({"path": name, "type": "contract_check", "status": "PASS" if ok else "FAIL", "detail": detail})
    return rows


def write_sha_manifest() -> str:
    rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-12.csv":
            rows.append({"relative_path": path.name, "sha256": sha256(path), "bytes": path.stat().st_size})
    digest = hashlib.sha256()
    for row in rows:
        digest.update(row["relative_path"].encode())
        digest.update(b"\0")
        digest.update(row["sha256"].encode())
        digest.update(b"\n")
    package_sha = digest.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(OUT_DIR / "sha256_manifest_2026-07-12.csv", rows)
    return package_sha


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_json(OUT_DIR / "contract_identity_2026-07-12.json", identity_block())
    write_json(OUT_DIR / "historical_population_spine_contract_v1_2026-07-12.json", contract_json())
    write_json(OUT_DIR / "compatibility_binding_2026-07-12.json", compatibility_binding())
    write_json(OUT_DIR / "amendment_policy_2026-07-12.json", {**identity_block(), "policy": contract_json()["amendment_policy"]})
    write_json(OUT_DIR / "freeze_decision_2026-07-12.json", freeze_decision_json())
    write_csv(OUT_DIR / "contract_field_rule_registry_2026-07-12.csv", registry_rows())
    write_csv(OUT_DIR / "canonical_identity_specification_2026-07-12.csv", canonical_identity_rows())
    write_csv(OUT_DIR / "source_selection_cutoff_contract_2026-07-12.csv", source_cutoff_rows())
    write_csv(OUT_DIR / "eligibility_exclusion_contract_2026-07-12.csv", eligibility_rows())
    write_csv(OUT_DIR / "deduplication_ordering_contract_2026-07-12.csv", dedupe_rows())
    write_csv(OUT_DIR / "feature_join_contract_2026-07-12.csv", feature_join_rows())
    write_csv(OUT_DIR / "manifest_population_applicability_2026-07-12.csv", applicability_rows())
    write_csv(OUT_DIR / "variant_c_derivation_contract_2026-07-12.csv", variant_c_rows())
    write_csv(OUT_DIR / "replayability_contract_2026-07-12.csv", replayability_rows())
    write_csv(OUT_DIR / "evidence_provenance_manifest_2026-07-12.csv", evidence_rows())
    write_markdown_files()
    write_csv(OUT_DIR / "parse_schema_validation_2026-07-12.csv", parse_validation())
    package_sha = write_sha_manifest()
    decision = freeze_decision_json(package_sha)
    write_json(OUT_DIR / "freeze_decision_2026-07-12.json", decision)
    write_csv(OUT_DIR / "parse_schema_validation_2026-07-12.csv", parse_validation())
    package_sha = write_sha_manifest()
    return {
        "output_dir": str(OUT_DIR),
        "contract_identifier": CONTRACT_IDENTIFIER,
        "contract_version": CONTRACT_VERSION,
        "freeze_decision": FREEZE_DECISION,
        "historical_expansion_readiness": "READY_FOR_BOUNDED_INCREMENTAL_HISTORICAL_SOURCE_EXPANSION",
        "training_readiness": "NOT_READY_FOR_MODEL_TRAINING",
        "package_sha256": package_sha,
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
