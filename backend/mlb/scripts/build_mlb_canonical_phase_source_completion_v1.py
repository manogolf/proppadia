#!/usr/bin/env python3
"""Build source-completion evidence entirely from retained local inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.scripts.build_mlb_canonical_game_phase_backfill_v1 import (
    build_proposal,
    write_outputs as write_backfill_outputs,
)
from backend.mlb.scripts.build_mlb_canonical_phase_coverage_gate_v1 import (
    build as build_coverage,
    scan_authoritative_schedules,
)
from backend.mlb.scripts.run_mlb_canonical_phase_stdlib_tests_v1 import execute_target
from backend.mlb.season_transition.canonical_phase_v1 import (
    CanonicalGamePhaseIndex,
    canonical_json,
    canonical_phase_record,
)
from backend.mlb.season_transition.contract_v1 import PhaseContractError


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_SOURCE_COMPLETION_V1"
ROOT = Path(__file__).resolve().parents[3]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(canonical_json(row) + "\n" for row in rows), encoding="utf-8")


def schedule_games(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("dates"), list):
        raise RuntimeError("STATSAPI_SCHEDULE_DATES_REQUIRED")
    games: list[dict[str, Any]] = []
    for block in payload["dates"]:
        if not isinstance(block, dict) or not isinstance(block.get("games", []), list):
            raise RuntimeError("STATSAPI_SCHEDULE_GAMES_INVALID")
        for game in block.get("games", []):
            if not isinstance(game, dict):
                raise RuntimeError("STATSAPI_SCHEDULE_GAME_INVALID")
            games.append(game)
    return games


def _write_coverage(output_dir: Path, report: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=False)
    omitted = {"missing_ledger", "conflict_ledger", "authoritative_schedule_source_manifest"}
    write_json(
        output_dir / "canonical_population_reconciliation.json",
        {key: value for key, value in report.items() if key not in omitted},
    )
    write_jsonl(output_dir / "missing_game_pk_ledger.jsonl", report["missing_ledger"])
    write_jsonl(output_dir / "conflict_ledger.jsonl", report["conflict_ledger"])
    write_jsonl(
        output_dir / "authoritative_schedule_source_manifest.jsonl",
        report["authoritative_schedule_source_manifest"],
    )
    write_json(
        output_dir / "source_completion_acquisition_proposal.json",
        report["source_completion_proposal"],
    )
    write_json(output_dir / "activation_recommendation.json", report["activation_recommendation"])
    write_json(
        output_dir / "coverage_gate_summary.json",
        {
            "contract_name": report["contract_name"],
            "canonical_universe": report["canonical_universe"],
            "authoritative_schedule_summary": report["authoritative_schedule_summary"],
            "offline_proposal_is_retained_file_subset": report[
                "offline_proposal_is_retained_file_subset"
            ],
            "gate_status": (
                "READY" if report["canonical_universe"]["fully_classified"] else "BLOCKED"
            ),
        },
    )


def build(
    *,
    receipt_path: Path,
    prior_missing_path: Path,
    database_snapshot: Path,
    output_dir: Path,
) -> dict[str, Any]:
    receipt = json.loads(receipt_path.read_text())
    raw_path = ROOT / receipt["raw_response_path"]
    raw = raw_path.read_bytes()
    raw_sha = hashlib.sha256(raw).hexdigest()
    if raw_sha != receipt["raw_response_sha256"] or len(raw) != receipt["raw_response_byte_count"]:
        raise RuntimeError("RETAINED_RESPONSE_RECEIPT_MISMATCH")
    if receipt.get("request_count") != 1 or receipt.get("retry_count") != 0:
        raise RuntimeError("REQUEST_ACCOUNTING_MISMATCH")
    if receipt.get("paid_provider_request_count") != 0 or receipt.get("paid_credit_count") != 0:
        raise RuntimeError("PAID_ACCOUNTING_NONZERO")

    prior_missing_rows = read_jsonl(prior_missing_path)
    prior_missing = {int(row["game_pk"]) for row in prior_missing_rows}
    games = schedule_games(json.loads(raw))
    index = CanonicalGamePhaseIndex()
    classification_errors: list[dict[str, Any]] = []
    response_ids: list[int] = []
    response_records: dict[int, dict[str, Any]] = {}
    for ordinal, game in enumerate(games):
        try:
            record = canonical_phase_record(
                game,
                source_sha256=raw_sha,
                source_path=receipt["raw_response_path"],
            )
            response_ids.append(int(record["game_pk"]))
            index.add(record)
            response_records[int(record["game_pk"])] = record
        except (PhaseContractError, KeyError, TypeError, ValueError) as exc:
            classification_errors.append(
                {
                    "observation_ordinal": ordinal,
                    "game_pk": game.get("gamePk"),
                    "reason": str(exc),
                }
            )
    if classification_errors:
        raise RuntimeError(f"AUTHORITATIVE_RESPONSE_CLASSIFICATION_ERRORS:{len(classification_errors)}")

    response_set = set(response_ids)
    resolved = sorted(prior_missing & response_set)
    unresolved = sorted(prior_missing - response_set)
    outside = sorted(response_set - prior_missing)
    response_duplicate_count = len(response_ids) - len(response_set)
    returned_type_counts = Counter(
        str(response_records[game_pk]["source_game_type"]) for game_pk in response_set
    )
    returned_phase_counts = Counter(
        str(response_records[game_pk]["season_phase"]) for game_pk in response_set
    )
    resolved_type_counts = Counter(
        str(response_records[game_pk]["source_game_type"]) for game_pk in resolved
    )
    resolved_phase_counts = Counter(
        str(response_records[game_pk]["season_phase"]) for game_pk in resolved
    )
    outside_rows = [
        {
            "game_pk": game_pk,
            "source_game_type": response_records[game_pk]["source_game_type"],
            "season_phase": response_records[game_pk]["season_phase"],
            "source_season": response_records[game_pk]["source_season"],
        }
        for game_pk in outside
    ]

    authoritative, source_manifest, scan_errors = scan_authoritative_schedules()
    if scan_errors:
        raise RuntimeError(f"RETAINED_AUTHORITATIVE_SCAN_ERRORS:{len(scan_errors)}")
    source_paths = [(ROOT / row["source_path"]).resolve() for row in source_manifest]
    proposal, proposal_sources, proposal_summary = build_proposal(source_paths, repo_root=ROOT)
    proposal_dir = output_dir / "canonical_backfill_proposal"
    write_backfill_outputs(proposal_dir, proposal, proposal_sources, proposal_summary)

    coverage_dir = output_dir / "population_reconciliation"
    proposal_path = proposal_dir / "canonical_game_phase_backfill_proposal.jsonl"
    coverage = build_coverage(database_snapshot, proposal_path)
    _write_coverage(coverage_dir, coverage)

    tests = execute_target()
    write_json(output_dir / "executed_dependency_free_test_report.json", tests)
    write_jsonl(output_dir / "returned_outside_prior_missing_ledger.jsonl", outside_rows)
    write_jsonl(
        output_dir / "unresolved_prior_missing_ledger.jsonl",
        [{"game_pk": game_pk, "reason": "NOT_RETURNED_BY_AUTHORIZED_RESPONSE"} for game_pk in unresolved],
    )

    violations = {
        "unresolved_prior_missing_count": len(unresolved),
        "current_missing_classification_count": coverage["canonical_universe"][
            "missing_authoritative_type_count"
        ],
        "unknown_or_invalid_response_observation_count": len(classification_errors),
        "source_conflict_count": len(coverage["conflict_ledger"]),
        "duplicate_identity_conflict_count": coverage["authoritative_schedule_summary"][
            "duplicate_identity_conflict_count"
        ],
    }
    ready = (
        all(value == 0 for value in violations.values())
        and len(resolved) == len(prior_missing) == 471
        and tests["gate_status"] == "PASS"
        and coverage["canonical_universe"]["fully_classified"]
    )
    report = {
        "contract_name": CONTRACT_NAME,
        "status": "READY" if ready else "BLOCKED",
        "network_request_made": True,
        "request_count": receipt["request_count"],
        "retry_count": receipt["retry_count"],
        "paid_provider_request_count": receipt["paid_provider_request_count"],
        "paid_credit_count": receipt["paid_credit_count"],
        "raw_response_path": receipt["raw_response_path"],
        "raw_response_byte_count": len(raw),
        "raw_response_sha256": raw_sha,
        "prior_missing_game_pk_count": len(prior_missing),
        "returned_observation_count": len(games),
        "returned_distinct_game_pk_count": len(response_set),
        "resolved_prior_missing_game_pk_count": len(resolved),
        "unresolved_prior_missing_game_pk_count": len(unresolved),
        "returned_outside_prior_missing_count": len(outside),
        "returned_outside_prior_missing_game_pks": outside,
        "returned_source_game_type_counts": dict(sorted(returned_type_counts.items())),
        "returned_normalized_phase_counts": dict(sorted(returned_phase_counts.items())),
        "resolved_source_game_type_counts": dict(sorted(resolved_type_counts.items())),
        "resolved_normalized_phase_counts": dict(sorted(resolved_phase_counts.items())),
        "response_consistent_duplicate_observation_count": index.consistent_duplicate_count,
        "response_duplicate_game_pk_observation_count": response_duplicate_count,
        "retained_source_file_count": len(source_manifest),
        "canonical_backfill_proposal_count": proposal_summary["canonical_game_count"],
        "retained_canonical_universe": coverage["canonical_universe"],
        "retained_authoritative_schedule_summary": coverage["authoritative_schedule_summary"],
        "violations": violations,
        "dependency_free_tests": {
            key: tests[key]
            for key in (
                "intended_scenarios",
                "executed_scenarios",
                "passed",
                "failed",
                "skipped",
                "unexecuted",
                "existing_validator_check_count",
                "existing_validator_status",
                "gate_status",
            )
        },
        "database_writes": 0,
        "phase_inference_from_dates": False,
        "activation_performed": False,
        "recommendation": (
            "CANONICAL_PHASE_SOURCE_COMPLETION_READY"
            if ready
            else "CANONICAL_PHASE_SOURCE_COMPLETION_BLOCKED"
        ),
        "exact_next_step": (
            "Proceed only to the separately governed migration activation preflight; "
            "do not apply the migration or canonical backfill without separate authorization."
            if ready
            else "Resolve the listed source-completion violations before any activation preflight."
        ),
    }
    write_json(output_dir / "source_completion_reconciliation.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--prior-missing-ledger", required=True, type=Path)
    parser.add_argument("--database-snapshot", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    if not output_dir.is_dir():
        raise SystemExit(f"OUTPUT_DIRECTORY_REQUIRED:{output_dir}")
    if any((output_dir / name).exists() for name in ("canonical_backfill_proposal", "population_reconciliation")):
        raise SystemExit(f"OUTPUT_ALREADY_BUILT:{output_dir}")
    report = build(
        receipt_path=args.receipt.resolve(),
        prior_missing_path=args.prior_missing_ledger.resolve(),
        database_snapshot=args.database_snapshot.resolve(),
        output_dir=output_dir,
    )
    print(canonical_json(report))
    return 0 if report["status"] == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
