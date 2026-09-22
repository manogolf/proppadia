#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Build bounded offline evidence for the training phase eligibility dry run."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from backend.mlb.season_transition.game_phase_authority_v1 import (
    EXPECTED_PHASE_CONTRACT_SHA256,
    EXPECTED_PROPOSAL_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.training_phase_eligibility_v1 import (
    BLOCKED_ABSENT_AUTHORITY,
    BLOCKED_AUTHORITY_HASH_MISMATCH,
    BLOCKED_CONFLICTING_TYPE,
    BLOCKED_DUPLICATE_AUTHORITY,
    BLOCKED_MISSING_GAME_PK,
    BLOCKED_SPECIAL_TYPE,
    BLOCKED_STALE_AUTHORITY,
    BLOCKED_UNKNOWN_TYPE,
    EXCLUDED_POSTSEASON,
    EXCLUDED_PRESEASON,
    filter_regular_season_membership,
)


CONTRACT_NAME = "MLB_2026_TRAINING_PHASE_ELIGIBILITY_DRY_RUN_V1"
ROOT = Path(__file__).resolve().parents[3]
FREEZE = ROOT / (
    "docs/contracts/"
    "mlb_2026_stat_derived_model_training_phase_population_freeze_v1"
)
ROW_MANIFEST = FREEZE / (
    "exact_game_pk_manifests/operational_model_training_props_2026_rows.csv"
)
EVIDENCE_DIR = ROOT / (
    "docs/contracts/mlb_2026_training_phase_eligibility_dry_run_v1"
)
EXPECTED_FREEZE_BYTES = 244_578_450
EXPECTED_FREEZE_FILES = 19
EXPECTED_FREEZE_MANIFEST_SHA256 = (
    "28b6a840314e4a6b4dafcfea4ad0f6a3620dedce2cbad7035ace8ad98e9ce679"
)
EXPECTED_ROW_MANIFEST_SHA256 = (
    "4578599d5f41e8b183fa174a00374690b8a490a24768357b712081ba28f583e4"
)
EXPECTED_COUNTS = {
    "input_rows": 600_766,
    "input_game_pks": 2_812,
    "admitted_rows": 459_604,
    "admitted_game_pks": 2_341,
    "excluded_preseason_rows": 141_162,
    "excluded_preseason_game_pks": 471,
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _freeze_manifest() -> dict[str, str]:
    manifest_path = FREEZE / "sha256_manifest.txt"
    if sha256(manifest_path) != EXPECTED_FREEZE_MANIFEST_SHA256:
        raise RuntimeError("FREEZE_MANIFEST_IDENTITY_MISMATCH")
    entries: dict[str, str] = {}
    for line in manifest_path.read_text(encoding="utf-8").splitlines():
        if not line:
            continue
        digest, relative = line.split("  ", 1)
        if relative in entries:
            raise RuntimeError(f"FREEZE_MANIFEST_DUPLICATE:{relative}")
        entries[relative] = digest
    actual_files = sorted(
        path.relative_to(FREEZE).as_posix()
        for path in FREEZE.rglob("*")
        if path.is_file()
    )
    expected_files = sorted([*entries, "sha256_manifest.txt"])
    if actual_files != expected_files:
        raise RuntimeError("FREEZE_MANIFEST_COVERAGE_MISMATCH")
    for relative, expected in sorted(entries.items()):
        actual = sha256(FREEZE / relative)
        if actual != expected:
            raise RuntimeError(f"FREEZE_FILE_HASH_MISMATCH:{relative}")
    total_bytes = sum((FREEZE / relative).stat().st_size for relative in actual_files)
    if len(actual_files) != EXPECTED_FREEZE_FILES:
        raise RuntimeError("FREEZE_FILE_COUNT_MISMATCH")
    if total_bytes != EXPECTED_FREEZE_BYTES:
        raise RuntimeError("FREEZE_PACKAGE_SIZE_MISMATCH")
    if entries.get(ROW_MANIFEST.relative_to(FREEZE).as_posix()) != EXPECTED_ROW_MANIFEST_SHA256:
        raise RuntimeError("FREEZE_ROW_MANIFEST_DECLARATION_MISMATCH")
    return {
        "manifest_sha256": EXPECTED_FREEZE_MANIFEST_SHA256,
        "row_manifest_sha256": EXPECTED_ROW_MANIFEST_SHA256,
        "file_count": len(actual_files),
        "total_bytes": total_bytes,
        "all_declared_file_hashes_verified": True,
    }


def _rows():
    with ROW_MANIFEST.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "source_row_fingerprint",
            "game_pk",
            "game_date",
            "source_season",
            "prop_type",
            "prop_source",
            "retained_game_type",
            "authoritative_game_type",
            "season_phase",
            "model_training_base_eligible",
            "predicted_outcome",
            "confidence_score",
            "was_correct",
            "source_type_conflict",
        }
        if set(reader.fieldnames or ()) != required:
            raise RuntimeError("FREEZE_ROW_MANIFEST_SCHEMA_MISMATCH")
        yield from reader


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _write_evidence(report: dict[str, Any], output_dir: Path) -> list[str]:
    if output_dir.resolve() != EVIDENCE_DIR.resolve():
        raise RuntimeError("DRY_RUN_EVIDENCE_DESTINATION_NOT_GOVERNED")
    exclusions = report["gate_report"]["decision_game_pk_row_counts"]
    csv_lines = ["game_pk,authoritative_phase,authoritative_game_type,row_count"]
    authority = HashedProposalAuthority()
    for decision, phase in (
        (EXCLUDED_PRESEASON, "PRESEASON"),
        (EXCLUDED_POSTSEASON, "POSTSEASON"),
    ):
        for item in exclusions[decision]:
            game_pk = int(item["game_pk"])
            record = authority.lookup_exact(game_pk)
            csv_lines.append(
                f"{game_pk},{phase},{record.source_game_type},{int(item['row_count'])}"
            )
    _atomic_write(
        output_dir / "excluded_game_pks.csv",
        ("\n".join(csv_lines) + "\n").encode("utf-8"),
    )
    _atomic_write(
        output_dir / "dry_run_report.json",
        (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    return ["dry_run_report.json", "excluded_game_pks.csv"]


def execute_dry_run(*, output_dir: Path | None = None) -> dict[str, Any]:
    """Verify the freeze, execute only the membership gate, and optionally emit evidence."""

    freeze = _freeze_manifest()
    authority = HashedProposalAuthority()
    result = filter_regular_season_membership(
        _rows(),
        game_pk_field="game_pk",
        source_type_field="retained_game_type",
        authority=authority,
        consumer_identity="backend/mlb/model_trainer.py::DRY_RUN_ONLY",
        input_identity=(
            "OPERATIONAL_MODEL_TRAINING_PROPS_ALL_2026@"
            + EXPECTED_ROW_MANIFEST_SHA256
        ),
        row_identity_fields=("source_row_fingerprint", "game_pk"),
        invariant_field_groups={
            # The fingerprint is the freeze's one-way commitment to the full
            # operational source row.  No feature material is rehydrated.
            "retained_feature_source_row_commitment": (
                "source_row_fingerprint",
                "game_pk",
                "game_date",
                "prop_type",
                "prop_source",
            ),
            "retained_target_projection": (
                "source_row_fingerprint",
                "predicted_outcome",
                "confidence_score",
                "was_correct",
            ),
            "retained_row_order": ("source_row_fingerprint", "game_pk"),
        },
        collect_admitted_rows=False,
    )
    gate = dict(result.report)
    blocked_codes = (
        BLOCKED_MISSING_GAME_PK,
        BLOCKED_ABSENT_AUTHORITY,
        BLOCKED_SPECIAL_TYPE,
        BLOCKED_UNKNOWN_TYPE,
        BLOCKED_CONFLICTING_TYPE,
        BLOCKED_DUPLICATE_AUTHORITY,
        BLOCKED_AUTHORITY_HASH_MISMATCH,
        BLOCKED_STALE_AUTHORITY,
    )
    counts_match = {
        "input_rows": gate["input_row_count"] == EXPECTED_COUNTS["input_rows"],
        "input_game_pks": gate["input_distinct_game_pk_count"]
        == EXPECTED_COUNTS["input_game_pks"],
        "admitted_rows": gate["admitted_row_count"]
        == EXPECTED_COUNTS["admitted_rows"],
        "admitted_game_pks": gate["admitted_distinct_game_pk_count"]
        == EXPECTED_COUNTS["admitted_game_pks"],
        "excluded_preseason_rows": gate["decision_row_counts"][EXCLUDED_PRESEASON]
        == EXPECTED_COUNTS["excluded_preseason_rows"],
        "excluded_preseason_game_pks": len(gate["decision_game_pks"][EXCLUDED_PRESEASON])
        == EXPECTED_COUNTS["excluded_preseason_game_pks"],
        "excluded_postseason_zero": gate["decision_row_counts"][EXCLUDED_POSTSEASON]
        == 0,
        "all_blocked_zero": all(
            gate["decision_row_counts"][code] == 0 for code in blocked_codes
        ),
    }
    invariant_hashes = gate["invariant_hashes"]
    invariants_match = all(
        item["identical"] for item in invariant_hashes.values()
    ) and gate["admitted_identity_identical"]
    authority_metadata = authority.metadata.to_dict()
    authority_match = (
        authority_metadata["proposal_sha256"] == EXPECTED_PROPOSAL_SHA256
        and authority_metadata["source_manifest_sha256"]
        == EXPECTED_SOURCE_MANIFEST_SHA256
        and authority_metadata["phase_contract_sha256"]
        == EXPECTED_PHASE_CONTRACT_SHA256
        and authority_metadata["missing_count"] == 0
        and authority_metadata["unknown_count"] == 0
        and authority_metadata["conflicting_count"] == 0
        and authority_metadata["duplicate_identity_count"] == 0
    )
    passed = all(counts_match.values()) and invariants_match and authority_match
    report: dict[str, Any] = {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if passed else "FAIL",
        "execution_mode": "OFFLINE_FROZEN_POPULATION_DRY_RUN_ONLY",
        "freeze_verification": freeze,
        "authority_verified": authority_match,
        "expected_counts": EXPECTED_COUNTS,
        "count_checks": counts_match,
        "gate_report": gate,
        "invariance": {
            "all_retained_hashes_identical": invariants_match,
            "hashes": invariant_hashes,
            "evidence_scope": (
                "The frozen source-row fingerprint commits the original operational row; "
                "the compact freeze does not disclose raw feature vectors. The helper "
                "passes admitted mapping objects through unchanged and hashes before/after "
                "projections in stable input order."
            ),
        },
        "safety": {
            "database_connections": 0,
            "network_requests": 0,
            "paid_requests": 0,
            "model_fit_calls": 0,
            "model_train_calls": 0,
            "model_score_calls": 0,
            "model_artifact_writes": 0,
            "prediction_writes": 0,
            "dataset_writes": 0,
            "operational_artifact_writes": 0,
            "review_evidence_writes": 2 if output_dir is not None else 0,
        },
        "ordinary_trainer_selector_activated": False,
        "active_selector_cutover_justified": bool(passed),
        "active_selector_cutover_authorized": False,
    }
    if output_dir is not None:
        report["evidence_files"] = ["dry_run_report.json", "excluded_game_pks.csv"]
        _write_evidence(report, output_dir)
    return report


if __name__ == "__main__":
    rendered = execute_dry_run(output_dir=EVIDENCE_DIR)
    print(json.dumps(rendered, indent=2, sort_keys=True))
    raise SystemExit(0 if rendered["status"] == "PASS" else 1)
