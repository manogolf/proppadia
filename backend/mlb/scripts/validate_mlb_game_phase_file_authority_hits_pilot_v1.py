#!/usr/bin/env python3
"""Validate the 2026 hashed-file phase authority Hits pilot offline."""

from __future__ import annotations

import argparse
import ast
import csv
import hashlib
import io
import json
import math
import unittest
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.season_transition.game_phase_authority_v1 import (
    EXPECTED_PHASE_CONTRACT_SHA256,
    EXPECTED_PROPOSAL_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    GamePhaseAuthorityError,
    HashedProposalAuthority,
)


CONTRACT_NAME = "MLB_2026_GAME_PHASE_FILE_AUTHORITY_HITS_PILOT_V1"
ROOT = Path(__file__).resolve().parents[3]
FROZEN_INPUT = ROOT / (
    "artifacts/analysis/model_development/"
    "mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14/"
    "frozen_hits_review_population.csv"
)
EXPECTED_FROZEN_INPUT_SHA256 = (
    "7c94ead53af9669c1164e41fcd9714edd0656dc02d6550f0c773aa1a3c1147fd"
)
TARGET_CONSUMER = ROOT / "backend/mlb/scripts/evaluate_hits_model_candidates.py"
TEST_MODULE = "backend.mlb.tests.test_mlb_game_phase_file_authority_hits_pilot_v1"
EXPECTED_FUNCTION_AST_SHA256 = {
    "_score_all_rows_for_model": "5c915281f092dc9b43c323e5d6d2bab6819c2cdd734354f3f91508e460b3feda",
    "_metrics_for_slice": "f5d5b117b1245c34f0ff0bffff30864bf5e834be2539aa2dd81f148acf0f30c4",
    "_deciles_for_slice": "4c5decc271a556b3bc24e4418cdb2c71545a0b8107911d8cdda0d88f64515d64",
    "_threshold_table_for_slice": "0e4bdfb5a9ba031dcd9685075ce9e5dd74998764c5a7d6082f813601cc7009de",
    "_cohort_slices": "3a81e08b2b0e8e56aeee71d299eae342cdba9a21de917aa7c5a5b7e6d2afb1f1",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def function_ast_hashes(path: Path, names: Iterable[str]) -> dict[str, str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    return {
        name: hashlib.sha256(
            ast.dump(functions[name], include_attributes=False).encode("utf-8")
        ).hexdigest()
        for name in names
    }


def binary_auc(probabilities: list[float], outcomes: list[int]) -> float | None:
    positives = sum(outcomes)
    negatives = len(outcomes) - positives
    if positives == 0 or negatives == 0:
        return None
    ordered = sorted(zip(probabilities, outcomes), key=lambda pair: pair[0])
    positive_rank_sum = 0.0
    position = 1
    cursor = 0
    while cursor < len(ordered):
        end = cursor + 1
        while end < len(ordered) and ordered[end][0] == ordered[cursor][0]:
            end += 1
        average_rank = (position + (position + end - cursor - 1)) / 2.0
        positive_rank_sum += average_rank * sum(outcome for _, outcome in ordered[cursor:end])
        position += end - cursor
        cursor = end
    return (
        positive_rank_sum - positives * (positives + 1) / 2.0
    ) / (positives * negatives)


def metrics(rows: list[dict[str, str]]) -> dict[str, Any]:
    probabilities = [float(row["model_probability"]) for row in rows]
    outcomes = [int(row["target"]) for row in rows]
    count = len(rows)
    correct = sum(
        int(probability >= 0.5) == outcome
        for probability, outcome in zip(probabilities, outcomes)
    )
    epsilon = 1e-15
    brier = sum(
        (probability - outcome) ** 2
        for probability, outcome in zip(probabilities, outcomes)
    ) / count
    log_loss = -sum(
        outcome * math.log(max(epsilon, min(1.0 - epsilon, probability)))
        + (1 - outcome)
        * math.log(max(epsilon, min(1.0 - epsilon, 1.0 - probability)))
        for probability, outcome in zip(probabilities, outcomes)
    ) / count
    auc = binary_auc(probabilities, outcomes)
    return {
        "rows": count,
        "correct_at_0_5": correct,
        "accuracy_pct_at_0_5": round(100.0 * correct / count, 6),
        "auc": round(auc, 12) if auc is not None else None,
        "brier_score": round(brier, 12),
        "log_loss": round(log_loss, 12),
    }


def execute_tests() -> dict[str, Any]:
    suite = unittest.defaultTestLoader.loadTestsFromName(TEST_MODULE)
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=1).run(suite)
    return {
        "executed": int(result.testsRun),
        "passed": int(result.testsRun - len(result.failures) - len(result.errors)),
        "failed": int(len(result.failures) + len(result.errors)),
        "skipped": int(len(result.skipped)),
        "successful": bool(result.wasSuccessful()),
    }


def validate() -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append(
            {
                "check": name,
                "status": "PASS" if passed else "FAIL",
                "detail": detail,
            }
        )

    input_sha256 = sha256(FROZEN_INPUT)
    check(
        "frozen_retained_input_identity",
        input_sha256 == EXPECTED_FROZEN_INPUT_SHA256,
        {
            "path": str(FROZEN_INPUT.relative_to(ROOT)),
            "sha256": input_sha256,
        },
    )
    with FROZEN_INPUT.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    required_fields = {
        "canonical_identity",
        "game_date",
        "game_id",
        "side",
        "model_probability",
        "actual_value",
        "target",
    }
    check(
        "frozen_input_required_fields",
        bool(rows) and required_fields.issubset(rows[0]),
        sorted(rows[0]) if rows else [],
    )

    authority = HashedProposalAuthority()
    authority.require_supported_window(
        min(row["game_date"] for row in rows),
        max(row["game_date"] for row in rows),
    )
    metadata = authority.metadata.to_dict()
    check(
        "authority_identity_and_population",
        metadata["proposal_sha256"] == EXPECTED_PROPOSAL_SHA256
        and metadata["source_manifest_sha256"] == EXPECTED_SOURCE_MANIFEST_SHA256
        and metadata["phase_contract_sha256"] == EXPECTED_PHASE_CONTRACT_SHA256
        and metadata["proposal_count"] == 2919
        and metadata["source_file_count"] == 464
        and metadata["missing_count"] == 0
        and metadata["unknown_count"] == 0
        and metadata["conflicting_count"] == 0
        and metadata["duplicate_identity_count"] == 0,
        metadata,
    )

    admitted_rows: list[dict[str, str]] = []
    phase_by_game_pk: dict[int, str] = {}
    excluded_game_pks: dict[str, set[int]] = {
        "PRESEASON": set(),
        "POSTSEASON": set(),
    }
    blocked: dict[int, str] = {}
    for row in rows:
        game_pk = int(row["game_id"])
        try:
            record = authority.lookup_exact(game_pk)
        except GamePhaseAuthorityError as exc:
            blocked[game_pk] = exc.code
            continue
        phase_by_game_pk[game_pk] = str(record.season_phase)
        if record.season_phase == "REGULAR_SEASON":
            admitted_rows.append(row)
        elif record.season_phase in excluded_game_pks:
            excluded_game_pks[str(record.season_phase)].add(game_pk)
        else:
            blocked[game_pk] = "GAME_PHASE_AUTHORITY_STATUS_UNKNOWN"

    game_pks = sorted({int(row["game_id"]) for row in rows})
    admitted_game_pks = sorted({int(row["game_id"]) for row in admitted_rows})
    check(
        "exact_game_pk_authority_coverage",
        len(rows) == 7564
        and len(game_pks) == 651
        and game_pks == admitted_game_pks
        and not blocked
        and not excluded_game_pks["PRESEASON"]
        and not excluded_game_pks["POSTSEASON"]
        and Counter(phase_by_game_pk.values()) == {"REGULAR_SEASON": 651},
        {
            "input_rows": len(rows),
            "distinct_game_pks": len(game_pks),
            "admitted_rows": len(admitted_rows),
            "admitted_distinct_game_pks": len(admitted_game_pks),
            "phase_counts_by_distinct_game_pk": dict(
                sorted(Counter(phase_by_game_pk.values()).items())
            ),
            "excluded_preseason_game_pks": sorted(excluded_game_pks["PRESEASON"]),
            "excluded_postseason_game_pks": sorted(excluded_game_pks["POSTSEASON"]),
            "blocked_game_pks": [
                {"game_pk": game_pk, "reason": blocked[game_pk]}
                for game_pk in sorted(blocked)
            ],
        },
    )

    def identities(values: list[dict[str, str]]) -> list[str]:
        return [row["canonical_identity"] for row in values]

    def predictions(values: list[dict[str, str]]) -> list[dict[str, str]]:
        return [
            {
                "canonical_identity": row["canonical_identity"],
                "model_probability": row["model_probability"],
                "side": row["side"],
            }
            for row in values
        ]

    def outcomes(values: list[dict[str, str]]) -> list[dict[str, str]]:
        return [
            {
                "canonical_identity": row["canonical_identity"],
                "actual_value": row["actual_value"],
                "target": row["target"],
            }
            for row in values
        ]

    before_metrics = metrics(rows)
    after_metrics = metrics(admitted_rows)
    before_hashes = {
        "cohort_identity_sha256": canonical_sha256(identities(rows)),
        "prediction_values_sha256": canonical_sha256(predictions(rows)),
        "outcome_values_sha256": canonical_sha256(outcomes(rows)),
    }
    after_hashes = {
        "cohort_identity_sha256": canonical_sha256(identities(admitted_rows)),
        "prediction_values_sha256": canonical_sha256(predictions(admitted_rows)),
        "outcome_values_sha256": canonical_sha256(outcomes(admitted_rows)),
    }
    check(
        "shared_prediction_pick_outcome_invariance",
        before_hashes == after_hashes,
        {"before": before_hashes, "after": after_hashes},
    )
    check(
        "evaluation_metric_invariance",
        before_metrics == after_metrics,
        {"before": before_metrics, "after": after_metrics},
    )

    actual_ast_hashes = function_ast_hashes(
        TARGET_CONSUMER,
        EXPECTED_FUNCTION_AST_SHA256,
    )
    consumer_source = TARGET_CONSUMER.read_text(encoding="utf-8")
    check(
        "probability_and_metric_functions_unchanged",
        actual_ast_hashes == EXPECTED_FUNCTION_AST_SHA256,
        actual_ast_hashes,
    )
    check(
        "legacy_default_removed_and_authority_gate_present",
        "COALESCE(NULLIF(upper(trim(to_jsonb(m)->>'game_type')), ''), 'R')"
        not in consumer_source
        and "HashedProposalAuthority" in consumer_source
        and "_apply_regular_season_authority" in consumer_source,
        str(TARGET_CONSUMER.relative_to(ROOT)),
    )

    test_report = execute_tests()
    check(
        "dependency_free_tests",
        test_report
        == {
            "executed": 13,
            "passed": 13,
            "failed": 0,
            "skipped": 0,
            "successful": True,
        },
        test_report,
    )

    failed = [row["check"] for row in checks if row["status"] != "PASS"]
    report: dict[str, Any] = {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if not failed else "FAIL",
        "supported_season": 2026,
        "supported_authority_window": {
            "from": metadata["supported_from_date"],
            "through": metadata["supported_through_date"],
        },
        "frozen_input": {
            "path": str(FROZEN_INPUT.relative_to(ROOT)),
            "sha256": input_sha256,
            "rows": len(rows),
            "distinct_game_pks": len(game_pks),
            "earliest_game_date": min(row["game_date"] for row in rows),
            "latest_game_date": max(row["game_date"] for row in rows),
        },
        "authority": metadata,
        "before": {
            "cohort_rows": len(rows),
            "distinct_game_pks": len(game_pks),
            "metrics": before_metrics,
            "hashes": before_hashes,
        },
        "after": {
            "cohort_rows": len(admitted_rows),
            "distinct_game_pks": len(admitted_game_pks),
            "metrics": after_metrics,
            "hashes": after_hashes,
        },
        "excluded_preseason_game_pks": sorted(excluded_game_pks["PRESEASON"]),
        "excluded_postseason_game_pks": sorted(excluded_game_pks["POSTSEASON"]),
        "blocked_game_pks": [
            {"game_pk": game_pk, "reason": blocked[game_pk]}
            for game_pk in sorted(blocked)
        ],
        "newly_excluded_or_blocked_game_pks": sorted(
            excluded_game_pks["PRESEASON"]
            | excluded_game_pks["POSTSEASON"]
            | set(blocked)
        ),
        "probability_invariance": "PASS" if before_hashes == after_hashes else "FAIL",
        "test_report": test_report,
        "check_count": len(checks),
        "passed_check_count": len(checks) - len(failed),
        "failed_checks": failed,
        "network_requests": 0,
        "database_connections": 0,
        "database_writes": 0,
        "checks": checks,
    }
    report["validation_sha256"] = canonical_sha256(report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = validate()
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
