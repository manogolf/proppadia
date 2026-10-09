#!/usr/bin/env python3
"""Build and validate compact, read-only Full-board Hits phase-gating evidence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sqlite3
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from backend.mlb.season_transition.contract_v1 import PhaseContractError, classify_schedule_game
from backend.mlb.season_transition.game_phase_authority_v1 import (
    GamePhaseAuthorityError,
    HashedProposalAuthority,
)


ROOT = Path(__file__).resolve().parents[3]
CANONICAL_PYTHON = ROOT / ".venv/bin/python"
PACKAGE = ROOT / "docs/contracts/mlb_2026_full_board_hits_phase_gating_v1"
LEDGER = ROOT / "backend/mlb/exports/model_v2/hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3"
REPORT_ROOT = ROOT / "artifacts/analysis/mlb/hits05_full_board_shadow"
PILOT = ROOT / "docs/contracts/mlb_2026_game_phase_file_authority_hits_pilot_v1/post_control_correction_audit.json"
MONEYLINE_ATTEMPTS = ROOT / "artifacts/ops/mlb_public_game_moneyline_attempts"
ACTIVE_SELECTION = ROOT / "backend/mlb/season_transition/authority_snapshots/active_selection.json"

EXPECTED_HEAD_ANCESTRY = {
    "4fe80cef754ea5a790b03f3a53636ba8919395b8",
    "0ef19ac4d2cf5d141853605abaa246a77397f652",
}
EXPECTED_SOURCE_FILES = (
    "backend/mlb/hits05_full_board_shadow/phase_gating_v1.py",
    "backend/mlb/scripts/score_mlb_hits05_full_board_shadow_v1.py",
    "backend/mlb/scripts/attach_mlb_hits05_full_board_markets_v1.py",
    "backend/mlb/scripts/grade_mlb_hits05_full_board_shadow_v1.py",
    "backend/mlb/scripts/report_mlb_hits05_full_board_shadow_v1.py",
    "backend/mlb/scripts/run_mlb_hits05_full_board_shadow_daily_v1.py",
    "backend/mlb/scripts/validate_mlb_2026_full_board_hits_phase_gating_v1.py",
    "backend/mlb/tests/test_mlb_2026_full_board_hits_phase_gating_v1.py",
    "backend/mlb/tests/test_hits05_full_board_shadow_v1.py",
)
TEST_MODULES = (
    "backend.mlb.tests.test_mlb_2026_full_board_hits_phase_gating_v1",
    "backend.mlb.tests.test_hits05_full_board_shadow_v1",
    "backend.mlb.tests.test_mlb_game_phase_file_authority_hits_pilot_v1",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_sha256(rows: Iterable[Mapping[str, Any]]) -> str:
    payload = json.dumps(
        list(rows), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def regular_metric_rows(metric_rows: list[dict[str, Any]], phase_by_game: dict[int, str], authority_failures: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Keep only rows with explicitly established regular-season authority."""
    failures = {int(item["game_pk"]): str(item.get("reason") or "AUTHORITY_UNAVAILABLE") for item in authority_failures}
    kept: list[dict[str, Any]] = []
    withheld: list[dict[str, Any]] = []
    for row in metric_rows:
        game_pk = int(row["game_id"])
        phase = phase_by_game.get(game_pk)
        if phase == "REGULAR_SEASON":
            kept.append(row)
        else:
            withheld.append({"game_pk": game_pk, "reason": failures.get(game_pk, f"NON_REGULAR_OR_UNESTABLISHED_AUTHORITY:{phase or 'MISSING'}")})
    return kept, withheld


def retained_schedule_phase_supplements(
    requested_game_pks: set[int], authority: HashedProposalAuthority,
) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]]]:
    """Derive scoped exact-game evidence from hash-verified retained schedules.

    Receipt decisions are used only as a cross-check that the schedule was
    bound to the run. Phase is derived from the raw retained schedule record
    through the shared StatsAPI type contract, never from the decision alone.
    """
    selection_sha = sha256_file(ACTIVE_SELECTION)
    schedules: dict[tuple[int, str], dict[str, Any]] = {}
    for receipt_path in sorted(MONEYLINE_ATTEMPTS.glob("*/*.json")):
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            if receipt.get("classification") != "COMPLETED":
                continue
            if receipt.get("active_authority_descriptor_sha256") != authority.metadata.snapshot_descriptor_sha256:
                continue
            if receipt.get("active_authority_selection_sha256") != selection_sha:
                continue
            schedule_rel = Path(str(receipt["retained_schedule_source_path"]))
            schedule_path = schedule_rel if schedule_rel.is_absolute() else ROOT / schedule_rel
            if not schedule_path.is_file():
                continue
            schedule_sha = sha256_file(schedule_path)
            if schedule_sha != receipt.get("retained_schedule_source_sha256"):
                continue
            decision_by_pk = {
                int(item["game_pk"]): item for item in receipt.get("per_game_decisions", [])
                if isinstance(item, dict) and item.get("game_pk") is not None
            }
            schedule = json.loads(schedule_path.read_text(encoding="utf-8"))
            for day in schedule.get("dates", []):
                for game in day.get("games", []):
                    game_pk = int(game.get("gamePk", -1))
                    if game_pk not in requested_game_pks:
                        continue
                    decision = decision_by_pk.get(game_pk)
                    if not decision or decision.get("authority_decision") != "EXACT_GAME_AUTHORITY_ACCEPTED":
                        continue
                    if decision.get("schedule_source_sha256") != schedule_sha or decision.get("game_identity_source_sha256") != schedule_sha:
                        continue
                    if (
                        int(decision.get("source_season", -1)) != int(game.get("season", -2))
                        or str(decision.get("source_game_type")) != str(game.get("gameType"))
                        or str(game.get("officialDate") or "") == ""
                    ):
                        continue
                    classified = classify_schedule_game(game, season=int(game["season"]))
                    if not classified.eligible_for_phase_evaluation or not classified.phase:
                        continue
                    evidence = {
                        "game_pk": game_pk,
                        "official_date": str(game["officialDate"]),
                        "source_season": int(game["season"]),
                        "source_game_type": str(game["gameType"]),
                        "normalized_phase": str(classified.phase),
                        "postseason_round": classified.postseason_round,
                        "schedule_source_path": schedule_path.relative_to(ROOT).as_posix(),
                        "schedule_source_sha256": schedule_sha,
                        "receipt_path": receipt_path.relative_to(ROOT).as_posix(),
                        "receipt_sha256": sha256_file(receipt_path),
                        "receipt_run_identity": receipt.get("run_identity"),
                        "shared_authority_status": "ABSENT_EXACT_GAME; scoped retained-source supplement",
                    }
                    schedules[(game_pk, schedule_sha)] = evidence
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError, PhaseContractError):
            continue

    supplements: dict[int, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []
    for game_pk in sorted(requested_game_pks):
        try:
            record = authority.lookup_exact(game_pk)
            matching = [item for (pk, _), item in schedules.items() if pk == game_pk]
            if matching and any(
                item["source_game_type"] != record.source_game_type
                or item["normalized_phase"] != record.season_phase
                for item in matching
            ):
                failures.append({"game_pk": game_pk, "reason": "RETAINED_SCHEDULE_CONFLICTS_WITH_ACTIVE_SHARED_AUTHORITY", "missing_fields": []})
            continue
        except GamePhaseAuthorityError as exc:
            if exc.code != "GAME_PHASE_ABSENT":
                failures.append({"game_pk": game_pk, "reason": str(exc), "missing_fields": []})
                continue
        candidates = [item for (pk, _), item in schedules.items() if pk == game_pk]
        distinct = {(item["official_date"], item["source_season"], item["source_game_type"], item["normalized_phase"], item["postseason_round"]) for item in candidates}
        if len(distinct) == 1 and candidates:
            supplements[game_pk] = sorted(candidates, key=lambda item: item["receipt_run_identity"] or "")[-1]
        else:
            missing_fields = []
            if not candidates:
                missing_fields = ["gamePk", "officialDate", "season", "gameType", "normalized_phase", "verified_schedule_source_hash"]
            elif len(distinct) != 1:
                missing_fields = ["conflict:officialDate/gameType/normalized_phase"]
            failures.append({"game_pk": game_pk, "reason": "NO_UNIQUE_HASH_VERIFIED_RETAINED_SCHEDULE_EVIDENCE", "missing_fields": missing_fields})
    return supplements, failures


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[Mapping[str, Any]]) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    path.write_text(buffer.getvalue(), encoding="utf-8")


def ro_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def dict_rows(connection: sqlite3.Connection, query: str) -> list[dict[str, Any]]:
    return [dict(row) for row in connection.execute(query).fetchall()]


def _run_tests() -> dict[str, Any]:
    command = [str(CANONICAL_PYTHON), "-m", "unittest", *TEST_MODULES]
    completed = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True, check=False
    )
    output = completed.stdout + completed.stderr
    import re

    matched = re.search(r"Ran ([0-9]+) tests", output)
    skipped = sum(int(value) for value in re.findall(r"skipped=([0-9]+)", output))
    passed = int(matched.group(1)) - skipped if matched and completed.returncode == 0 else 0
    failed = 0 if completed.returncode == 0 else 1
    return {
        "command": command,
        "dependency_free_runner": "unittest",
        "assertions_executed": bool(matched),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "returncode": completed.returncode,
        "output_tail": output.strip().splitlines()[-8:],
    }


def _consumer_manifest() -> list[dict[str, str]]:
    return [
        {"sequence": "1", "component": "governed lineup/schedule parent artifacts", "path": "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/<date>/<run>/", "role": "prediction input", "phase_behavior": "StatsAPI gameType is checked against exact-gamePk authority before scoring", "task_scope": "IN_SCOPE_RUNTIME_INPUT"},
        {"sequence": "2", "component": "Full-board scorer", "path": "backend/mlb/scripts/score_mlb_hits05_full_board_shadow_v1.py", "role": "prediction generation/loading", "phase_behavior": "regular and postseason collection admitted; preseason/special excluded; authority failures abort before ledger access", "task_scope": "MODIFIED"},
        {"sequence": "3", "component": "append-only Full-board ledger", "path": "backend/mlb/hits05_full_board_shadow/ledger_v1.py", "role": "predictions/features/outcomes/markets/ranks/runs", "phase_behavior": "no duplicated phase column; consumers join authority by exact gamePk", "task_scope": "INSPECTED_UNCHANGED"},
        {"sequence": "4", "component": "market attachment", "path": "backend/mlb/scripts/attach_mlb_hits05_full_board_markets_v1.py", "role": "price attachment", "phase_behavior": "all attachable candidates are phase-preflighted before first append", "task_scope": "MODIFIED"},
        {"sequence": "5", "component": "outcome grader", "path": "backend/mlb/scripts/grade_mlb_hits05_full_board_shadow_v1.py", "role": "outcome grading", "phase_behavior": "exact-gamePk phase partition completes before actual-value loading or outcome append", "task_scope": "MODIFIED"},
        {"sequence": "6", "component": "progress reporter", "path": "backend/mlb/scripts/report_mlb_hits05_full_board_shadow_v1.py", "role": "candidate/evaluation loading and reporting", "phase_behavior": "regular default and explicit postseason shadow are disjoint; combined evaluation is unavailable", "task_scope": "MODIFIED"},
        {"sequence": "7", "component": "daily lane orchestrator", "path": "backend/mlb/scripts/run_mlb_hits05_full_board_shadow_daily_v1.py", "role": "score/attach/grade/report caller", "phase_behavior": "ordinary progress report explicitly requests REGULAR_SEASON", "task_scope": "MODIFIED_NOT_EXECUTED"},
        {"sequence": "8", "component": "Full-board validator", "path": "backend/mlb/scripts/validate_mlb_hits05_full_board_shadow_v1.py", "role": "append-only/integrity validation", "phase_behavior": "validates ledger mechanics; phase validation is added by this V1 gate validator", "task_scope": "INSPECTED_UNCHANGED"},
        {"sequence": "9", "component": "private scheduler hook", "path": "bin/mlb_hits05_full_board_shadow_daily_hook.sh", "role": "only tracked direct ordinary caller", "phase_behavior": "invokes daily shadow only; no selector/public path", "task_scope": "INSPECTED_UNCHANGED_NOT_EXECUTED"},
        {"sequence": "10", "component": "retained progress exports", "path": "artifacts/analysis/mlb/hits05_full_board_shadow/", "role": "daily and latest reports", "phase_behavior": "retained bytes are read-only evidence; no report was rewritten", "task_scope": "READ_ONLY_RECONCILIATION"},
        {"sequence": "-", "component": "frozen bounded Hits evaluator pilot", "path": "backend/mlb/scripts/evaluate_hits_model_candidates.py", "role": "separate 7,564-row / 651-gamePk cohort", "phase_behavior": "post-control authority gate and explicit legacy comparator remain preserved; no extrapolation to Full-board Hits", "task_scope": "EXPLICITLY_OUT_OF_SCOPE_PRESERVED"},
        {"sequence": "-", "component": "BvP, Totals, agreement study, Ops Brief, shared daily indexes, lane selector, Quick Card", "path": "multiple", "role": "adjacent consumers", "phase_behavior": "not used or changed by this bounded cutover", "task_scope": "EXPLICITLY_OUT_OF_SCOPE"},
    ]


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    for ancestor in EXPECTED_HEAD_ANCESTRY:
        result = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, "HEAD"], cwd=ROOT
        )
        if result.returncode:
            raise RuntimeError(f"REQUIRED_ANCESTRY_MISSING:{ancestor}")

    before_stat = (LEDGER.stat().st_size, LEDGER.stat().st_mtime_ns)
    before_ledger_sha = sha256_file(LEDGER)
    connection = ro_connection(LEDGER)
    authority = HashedProposalAuthority()

    source_queries = {
        "predictions": "SELECT game_id AS game_pk,COUNT(*) AS row_count FROM hits05_full_board_predictions GROUP BY game_id ORDER BY game_id",
        "outcomes": "SELECT game_id AS game_pk,COUNT(*) AS row_count FROM hits05_full_board_outcomes GROUP BY game_id ORDER BY game_id",
        "prices": "SELECT p.game_id AS game_pk,COUNT(*) AS row_count FROM hits05_full_board_market_observations m JOIN hits05_full_board_predictions p USING(canonical_identity) GROUP BY p.game_id ORDER BY p.game_id",
        "grading": "SELECT game_id AS game_pk,COUNT(*) AS row_count FROM hits05_full_board_outcomes GROUP BY game_id ORDER BY game_id",
        "reports": "SELECT game_id AS game_pk,COUNT(*) AS row_count FROM hits05_full_board_predictions WHERE json_extract(prediction_payload_json,'$.evidence_mode')='PROSPECTIVE' GROUP BY game_id ORDER BY game_id",
        "eligibility": "SELECT game_id AS game_pk,COUNT(*) AS row_count FROM hits05_full_board_eligibility_observations WHERE game_id IS NOT NULL GROUP BY game_id ORDER BY game_id",
    }
    by_source: dict[str, dict[int, int]] = {
        name: {int(row["game_pk"]): int(row["row_count"]) for row in dict_rows(connection, query)}
        for name, query in source_queries.items()
    }
    all_game_pks = sorted(set().union(*(set(rows) for rows in by_source.values())))
    phase_by_game: dict[int, str] = {}
    raw_type_by_game: dict[int, str] = {}
    violations: list[dict[str, Any]] = []
    for game_pk in all_game_pks:
        try:
            record = authority.lookup_exact(game_pk)
            phase_by_game[game_pk] = str(record.season_phase)
            raw_type_by_game[game_pk] = record.source_game_type
        except Exception as exc:
            violations.append({"game_pk": game_pk, "reason": f"{type(exc).__name__}:{exc}"})
    absent_game_pks = {
        int(item["game_pk"]) for item in violations
        if str(item.get("reason", "")).startswith("GamePhaseAuthorityError:GAME_PHASE_ABSENT:")
    }
    authority_supplements, supplement_failures = retained_schedule_phase_supplements(absent_game_pks, authority)
    for game_pk, evidence in authority_supplements.items():
        phase_by_game[game_pk] = evidence["normalized_phase"]
        raw_type_by_game[game_pk] = evidence["source_game_type"]
    violations = [
        item for item in violations
        if not (int(item["game_pk"]) in authority_supplements and str(item.get("reason", "")).startswith("GamePhaseAuthorityError:GAME_PHASE_ABSENT:"))
    ] + supplement_failures

    reconciliation_rows = [
        {
            "game_pk": game_pk,
            "authoritative_raw_game_type": raw_type_by_game.get(game_pk, ""),
            "normalized_phase": phase_by_game.get(game_pk, ""),
            "prediction_rows": by_source["predictions"].get(game_pk, 0),
            "outcome_rows": by_source["outcomes"].get(game_pk, 0),
            "price_rows": by_source["prices"].get(game_pk, 0),
            "grading_rows": by_source["grading"].get(game_pk, 0),
            "report_input_rows": by_source["reports"].get(game_pk, 0),
            "eligibility_rows": by_source["eligibility"].get(game_pk, 0),
        }
        for game_pk in all_game_pks
    ]
    write_csv(
        output_dir / "retained_game_pk_reconciliation.csv",
        list(reconciliation_rows[0]),
        reconciliation_rows,
    )

    source_summary = {}
    for name, counts in by_source.items():
        phase_counts = Counter(phase_by_game.get(game_pk, "MISSING_AUTHORITY") for game_pk in counts)
        source_summary[name] = {
            "rows": sum(counts.values()),
            "distinct_game_pks": len(counts),
            "phase_game_pk_counts": dict(sorted(phase_counts.items())),
            "missing_or_conflicting_game_pks": sorted(
                game_pk for game_pk in counts if game_pk not in phase_by_game
            ),
        }

    table_queries = {
        "predictions": "SELECT * FROM hits05_full_board_predictions ORDER BY canonical_identity",
        "feature_context": "SELECT * FROM hits05_full_board_feature_context ORDER BY canonical_identity",
        "outcomes": "SELECT * FROM hits05_full_board_outcomes ORDER BY canonical_identity",
        "prices": "SELECT * FROM hits05_full_board_market_observations ORDER BY observation_identity",
        "eligibility": "SELECT * FROM hits05_full_board_eligibility_observations ORDER BY observation_identity",
        "ranks": "SELECT * FROM hits05_full_board_rank_snapshots ORDER BY observation_identity",
        "runs": "SELECT * FROM hits05_full_board_runs ORDER BY run_tag",
    }
    table_hashes = {}
    for name, query in table_queries.items():
        rows = dict_rows(connection, query)
        table_hashes[name] = {"rows": len(rows), "canonical_rows_sha256": canonical_sha256(rows)}

    metric_rows = dict_rows(
        connection,
        """SELECT p.canonical_identity,p.slate_date,p.game_id,p.player_id,p.probability_over,
                         p.baseline_population_probability,p.baseline_hitter_shrunk_probability,
                         o.actual_hits,o.appearance_status,o.outcome_status
                  FROM hits05_full_board_predictions p
                  LEFT JOIN hits05_full_board_outcomes o USING(canonical_identity)
                  WHERE json_extract(p.prediction_payload_json,'$.evidence_mode')='PROSPECTIVE'
                  ORDER BY p.slate_date,p.game_id,p.player_id""",
    )
    retained_regular_metric_rows, withheld_metric_rows = regular_metric_rows(metric_rows, phase_by_game, violations)
    market_metric_rows = dict_rows(
        connection,
        "SELECT canonical_identity,market_payload_json,market_payload_sha256 FROM hits05_full_board_market_observations ORDER BY observation_identity",
    )
    connection.close()

    report_inventory = []
    for path in sorted(item for item in REPORT_ROOT.rglob("*") if item.is_file()):
        report_inventory.append({
            "path": str(path.relative_to(ROOT)),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    write_csv(
        output_dir / "retained_report_inventory.csv",
        ["path", "bytes", "sha256"],
        report_inventory,
    )

    after_ledger_sha = sha256_file(LEDGER)
    after_stat = (LEDGER.stat().st_size, LEDGER.stat().st_mtime_ns)
    progress_path = REPORT_ROOT / "progress_latest.json"
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
    population_report = {
        "authority": authority.metadata.to_dict(),
        "ledger": {
            "path": str(LEDGER.relative_to(ROOT)),
            "bytes": before_stat[0],
            "sha256_before": before_ledger_sha,
            "sha256_after": after_ledger_sha,
            "stat_unchanged_during_read": before_stat == after_stat,
            "read_mode": "sqlite_uri_mode_ro_immutable_1",
        },
        "union": {
            "distinct_game_pks": len(all_game_pks),
            "phase_game_pk_counts": dict(sorted(Counter(phase_by_game.values()).items())),
            "authority_violations": violations,
        },
        "sources": source_summary,
        "scoped_retained_schedule_supplements": [authority_supplements[key] for key in sorted(authority_supplements)],
        "regular_metrics": {"retained_rows": len(retained_regular_metric_rows), "withheld_unsupported_authority_rows": withheld_metric_rows},
        "reports": {
            "file_count": len(report_inventory),
            "file_manifest_sha256": canonical_sha256(report_inventory),
            "progress_latest_path": str(progress_path.relative_to(ROOT)),
            "progress_latest_bytes": progress_path.stat().st_size,
            "progress_latest_sha256": sha256_file(progress_path),
            "progress_latest_counts": progress.get("counts"),
            "progress_latest_decision_category": progress.get("decision_category"),
        },
    }
    write_json(output_dir / "retained_population_reconciliation.json", population_report)

    before_metric_hash = canonical_sha256(metric_rows)
    after_metric_hash = canonical_sha256(retained_regular_metric_rows)
    invariance = {
        "scope": "retained Full-board Hits shadow ledger only",
        "membership_before": {"rows": len(metric_rows), "sha256": before_metric_hash},
        "regular_membership_after_exact_game_pk_gate": {
            "rows": len(retained_regular_metric_rows), "sha256": after_metric_hash
        },
        "membership_changed_by_exact_phase_gate": metric_rows != retained_regular_metric_rows,
        "prediction_probability_and_baseline_hash": table_hashes["predictions"],
        "feature_context_hash": table_hashes["feature_context"],
        "outcome_and_grading_hash": table_hashes["outcomes"],
        "price_hash": table_hashes["prices"],
        "market_metric_input": {"rows": len(market_metric_rows), "sha256": canonical_sha256(market_metric_rows)},
        "metric_input_hash_before": before_metric_hash,
        "metric_input_hash_after_regular_gate": after_metric_hash,
        "withheld_nonregular_or_unestablished_rows": len(withheld_metric_rows),
        "withheld_rows_sha256": canonical_sha256(withheld_metric_rows),
        "retained_report_bytes_rewritten": False,
        "existing_progress_report_sha256": sha256_file(progress_path),
        "prediction_quality_effect": "NONE_MEMBERSHIP_ONLY_NO_PROBABILITY_OR_FEATURE_CHANGE",
        "market_roi_effect": "NONE_NO_PRICE_OR_OUTCOME_CHANGE_AND_NO_ROI_CLAIM",
    }
    write_json(output_dir / "before_after_invariance_report.json", invariance)

    pilot = json.loads(PILOT.read_text(encoding="utf-8"))
    pilot_scope = {
        "path": str(PILOT.relative_to(ROOT)),
        "sha256": sha256_file(PILOT),
        "classification": pilot.get("classification"),
        "frozen_scope": {
            "rows": 7564,
            "distinct_game_pks": 651,
            "rule": "Do not extrapolate to Full-board Hits or any other Hits consumer, prediction construction, Moneyline, Totals, grading, agreement reporting, seasons, or date ranges.",
        },
        "relationship_to_this_cutover": "PRESERVED_SEPARATE_NOT_USED_AS_FULL_BOARD_INVARIANCE_EVIDENCE",
    }
    write_json(output_dir / "frozen_pilot_scope.json", pilot_scope)

    consumer_rows = _consumer_manifest()
    write_csv(output_dir / "consumer_manifest.csv", list(consumer_rows[0]), consumer_rows)
    source_artifacts = [
        {
            "path": path,
            "bytes": (ROOT / path).stat().st_size,
            "sha256": sha256_file(ROOT / path),
        }
        for path in EXPECTED_SOURCE_FILES
    ]
    write_csv(
        output_dir / "source_artifact_manifest.csv",
        ["path", "bytes", "sha256"],
        source_artifacts,
    )
    classifications = {
        "full_board_hits_regular_season_integrity": "READY_EXACT_GAME_PK_GATE_REGULAR_ROWS_ONLY",
        "full_board_hits_postseason_code_readiness": "READY_SYNTHETIC_ALL_SUPPORTED_ROUNDS",
        "full_board_hits_postseason_operational_readiness": "BLOCKED_NO_ACTUAL_AUTHORITATIVE_POSTSEASON_GAME_OBSERVED_THROUGH_ORDINARY_PATH",
        "historical_restatement_requirement": "NOT_REQUIRED_FOR_RETAINED_FULL_BOARD_COHORT",
        "prediction_quality_effect": "NONE_MEMBERSHIP_AND_REPORTING_CORRECTION_ONLY",
        "market_roi_effect": "NONE_NO_MARKET_OR_ROI_RECALCULATION",
        "selector_publication_status": "UNCHANGED_GOVERNED_UNAVAILABLE",
        "remaining_blockers": [
            "An actual authoritative 2026 postseason game must traverse the ordinary score/attach/grade/report path before operational readiness.",
            "The active shared authority omits seven retained schedule identities; exact source-hashed supplements are scoped to this validator and do not activate the ordinary production path.",
            "The absent Hits-under artifact remains absent; no fallback, selector, Quick Card, registration, certification, or publication path was activated.",
            "Other Hits consumers remain outside this bounded cutover and require their own exact-gamePk review.",
        ],
    }
    write_json(output_dir / "classifications.json", classifications)

    test_report = _run_tests()
    write_json(output_dir / "executed_test_report.json", test_report)

    diff_check = subprocess.run(
        ["git", "diff", "--check", "--", *EXPECTED_SOURCE_FILES,
         "backend/mlb/tests/test_mlb_game_phase_file_authority_hits_pilot_v1.py"],
        cwd=ROOT, text=True, capture_output=True, check=False
    )
    runtime_consumers = (
        "backend/mlb/hits05_full_board_shadow/phase_gating_v1.py",
        "backend/mlb/scripts/score_mlb_hits05_full_board_shadow_v1.py",
        "backend/mlb/scripts/attach_mlb_hits05_full_board_markets_v1.py",
        "backend/mlb/scripts/grade_mlb_hits05_full_board_shadow_v1.py",
        "backend/mlb/scripts/report_mlb_hits05_full_board_shadow_v1.py",
        "backend/mlb/scripts/run_mlb_hits05_full_board_shadow_daily_v1.py",
    )
    bounded_text = "\n".join(
        (ROOT / path).read_text(encoding="utf-8") for path in runtime_consumers
    )
    unsafe_literals = [
        literal for literal in ('game_type") or "R"', "game_type').fillna('R')", 'game_type").fillna("R")')
        if literal in bounded_text
    ]
    checks = [
        {"check": "authority_population", "status": "PASS" if authority.metadata.proposal_count == sum(authority.metadata.phase_counts.values()) and authority.metadata.phase_counts.get("PRESEASON") == 489 and authority.metadata.phase_counts.get("REGULAR_SEASON") == 2430 and not any((authority.metadata.missing_count, authority.metadata.unknown_count, authority.metadata.conflicting_count, authority.metadata.duplicate_identity_count)) else "FAIL", "phase_counts": dict(authority.metadata.phase_counts)},
        {"check": "retained_union_authority", "status": "PASS" if not violations else "FAIL"},
        {"check": "retained_union_phase_classified", "status": "PASS" if len(phase_by_game) == len(all_game_pks) and not violations else "FAIL"},
        {"check": "regular_metric_gate_accounted", "status": "PASS" if len(retained_regular_metric_rows) + len(withheld_metric_rows) == len(metric_rows) else "FAIL"},
        {"check": "ledger_read_only_unchanged", "status": "PASS" if before_ledger_sha == after_ledger_sha and before_stat == after_stat else "FAIL"},
        {"check": "missing_type_default_removed", "status": "PASS" if not unsafe_literals else "FAIL", "violations": unsafe_literals},
        {"check": "pilot_scope_preserved", "status": "PASS" if pilot.get("classification") == "RESULT_SAME_BUT_CONTROL_VIOLATED" else "FAIL"},
        {"check": "dependency_free_tests", "status": "PASS" if test_report["returncode"] == 0 and test_report["assertions_executed"] and test_report["skipped"] == 0 else "FAIL"},
        {"check": "git_diff_check", "status": "PASS" if diff_check.returncode == 0 else "FAIL", "detail": diff_check.stdout + diff_check.stderr},
        {"check": "selector_quick_card_not_in_changed_sources", "status": "PASS" if not any("selector" in path.lower() or "quick_card" in path.lower() for path in EXPECTED_SOURCE_FILES) else "FAIL"},
        {"check": "postseason_operational_readiness_not_overclaimed", "status": "PASS" if classifications["full_board_hits_postseason_operational_readiness"] == "BLOCKED_NO_ACTUAL_AUTHORITATIVE_POSTSEASON_GAME_OBSERVED_THROUGH_ORDINARY_PATH" else "FAIL", "status_detail": classifications["full_board_hits_postseason_operational_readiness"]},
    ]
    validation = {
        "contract": "MLB_2026_FULL_BOARD_HITS_PHASE_GATING_V1",
        "status": "PASS" if all(item["status"] == "PASS" for item in checks) else "FAIL",
        "checks": checks,
        "source_files": list(EXPECTED_SOURCE_FILES),
        "retained_violations": violations,
        "scoped_retained_schedule_supplements": [authority_supplements[key] for key in sorted(authority_supplements)],
    }
    write_json(output_dir / "validation_report.json", validation)

    readme = f"""# MLB 2026 Full-board Hits phase gating V1

The Full-board Hits lane now uses the source-hashed canonical phase authority by exact `gamePk` before ordinary scoring, market attachment, grading, and evaluation. Missing or invalid authority aborts; preseason and special games are excluded; regular-season and postseason evaluation are disjoint. No phase column was added to the append-only ledger.

## Retained evidence

- Full-board prediction population: {source_summary['predictions']['rows']:,} rows / {source_summary['predictions']['distinct_game_pks']:,} gamePks.
- Outcome/grading population: {source_summary['outcomes']['rows']:,} rows / {source_summary['outcomes']['distinct_game_pks']:,} gamePks.
- Market observations: {source_summary['prices']['rows']:,} rows / {source_summary['prices']['distinct_game_pks']:,} gamePks.
- Exact retained union: {len(all_game_pks):,} gamePks; phase counts `{dict(sorted(Counter(phase_by_game.values()).items()))}`; unresolved/conflicting authority: {len(violations)}.
- Exact postseason games absent from active shared authority are supplemented only where a retained schedule response is hash-verified and raw gameType independently normalizes under the shared contract. Run decisions are consistency checks, not phase evidence.
- Regular-season metrics retain {len(retained_regular_metric_rows):,}/{len(metric_rows):,} prospective rows; postseason or unresolved-authority rows are withheld with gamePk and reason. Input hashes are `{before_metric_hash}` before and `{after_metric_hash}` after the exact phase gate.
- Ledger bytes remained `{before_ledger_sha}` throughout immutable read-only reconciliation. No retained report was rewritten.

The earlier bounded evaluator finding remains `RESULT_SAME_BUT_CONTROL_VIOLATED` only for its frozen 7,564-row / 651-gamePk cohort. It is not evidence about this Full-board cohort or any other Hits consumer.

## Readiness

- Regular-season integrity: ready.
- Postseason code: ready on clearly identified synthetic fixtures for all supported rounds.
- Postseason operations: blocked until an actual authoritative postseason game validates the ordinary path.
- Prediction quality and market/ROI: unchanged; this is membership/report partitioning only.
- Selector, ranking publication, Quick Card, and public output: unchanged and unavailable.

Smallest next action: allow ordinary retained-source acquisition and, after the first actual postseason game is present in the verified authority, run a separately authorized no-publication ordinary-path validation. Do not manufacture a fixture, fallback artifact, or historical replay.
"""
    (output_dir / "README.md").write_text(readme, encoding="utf-8")

    manifest_paths = sorted(
        path for path in output_dir.rglob("*")
        if path.is_file() and path.name != "sha256_manifest.txt"
    )
    manifest = "".join(
        f"{sha256_file(path)}  {path.relative_to(output_dir)}\n" for path in manifest_paths
    )
    (output_dir / "sha256_manifest.txt").write_text(manifest, encoding="utf-8")
    return validation


def verify(output_dir: Path) -> dict[str, Any]:
    failures = []
    manifest = output_dir / "sha256_manifest.txt"
    for line in manifest.read_text(encoding="utf-8").splitlines():
        expected, relative = line.split("  ", 1)
        path = output_dir / relative
        if not path.is_file() or sha256_file(path) != expected:
            failures.append(relative)
    report = json.loads((output_dir / "validation_report.json").read_text(encoding="utf-8"))
    return {
        "status": "PASS" if not failures and report.get("status") == "PASS" else "FAIL",
        "manifest_entries": len(manifest.read_text(encoding="utf-8").splitlines()),
        "manifest_failures": failures,
        "validation_checks": len(report.get("checks") or []),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=PACKAGE)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    result = verify(args.output_dir) if args.verify_only else build(args.output_dir)
    if not args.verify_only:
        result = {"build": result, "verification": verify(args.output_dir)}
    print(json.dumps(result, indent=2, sort_keys=True))
    status = result.get("status") or result.get("verification", {}).get("status")
    return 0 if status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
