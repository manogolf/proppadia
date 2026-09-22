#!/usr/bin/env python3
"""Build the offline MLB canonical phase population-coverage gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as parquet

from backend.mlb.season_transition.contract_v1 import (
    PhaseContractError,
    classify_schedule_game,
    normalize_source_game_type,
)


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_COVERAGE_AND_TEST_GATE_V1"
REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEDULE_ROOTS = (
    Path("backend/mlb/data"),
    Path("backend/mlb/exports"),
    Path("artifacts/analysis/mlb"),
    Path("artifacts/analysis/model_development"),
)
NORMALIZED_GAMES = Path(
    "backend/mlb/data/external/normalized/v1/games/season=2026/part-000.parquet"
)
SQLITE_ROOT = Path("backend/mlb/exports")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path.resolve())


def schedule_files() -> list[Path]:
    files: set[Path] = set()
    for relative_root in SCHEDULE_ROOTS:
        root = REPO_ROOT / relative_root
        if root.exists():
            files.update(root.rglob("*schedule*.json"))
    return sorted(files, key=lambda path: display_path(path))


def _games(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("dates"), list):
        return [
            game
            for block in payload["dates"]
            if isinstance(block, dict)
            for game in block.get("games", [])
            if isinstance(game, dict)
        ]
    if payload.get("gamePk") is not None:
        return [payload]
    return []


def scan_authoritative_schedules() -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    records: dict[int, dict[str, Any]] = {}
    source_manifest: list[dict[str, Any]] = []
    observation_errors: list[dict[str, Any]] = []
    for path in schedule_files():
        raw = path.read_bytes()
        payload = json.loads(raw)
        source_path = display_path(path)
        retained_games = [
            game for game in _games(payload) if str(game.get("season", "")) == "2026"
        ]
        if not retained_games:
            continue
        source_manifest.append(
            {
                "source_path": source_path,
                "source_sha256": hashlib.sha256(raw).hexdigest(),
                "source_bytes": len(raw),
                "retained_2026_game_observations": len(retained_games),
            }
        )
        for game in retained_games:
            try:
                game_pk = int(game["gamePk"])
            except (KeyError, TypeError, ValueError):
                observation_errors.append(
                    {
                        "source_path": source_path,
                        "reason": "MISSING_OR_INVALID_GAME_PK",
                    }
                )
                continue
            try:
                classification = classify_schedule_game(game)
            except PhaseContractError as exc:
                observation_errors.append(
                    {
                        "game_pk": game_pk,
                        "source_path": source_path,
                        "raw_game_type": game.get("gameType"),
                        "reason": str(exc),
                    }
                )
                continue
            date_value = str(game.get("officialDate") or game.get("gameDate") or "")[:10]
            record = records.setdefault(
                game_pk,
                {
                    "game_pk": game_pk,
                    "source_seasons": set(),
                    "source_game_types": set(),
                    "season_phases": set(),
                    "postseason_rounds": set(),
                    "scheduled_dates": set(),
                    "source_paths": set(),
                    "observation_count": 0,
                },
            )
            record["source_seasons"].add(classification.season)
            record["source_game_types"].add(classification.raw_game_type)
            record["season_phases"].add(classification.phase)
            record["postseason_rounds"].add(classification.postseason_round)
            if date_value:
                record["scheduled_dates"].add(date_value)
            record["source_paths"].add(source_path)
            record["observation_count"] += 1
    return records, source_manifest, observation_errors


def load_proposal(path: Path) -> dict[int, dict[str, Any]]:
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return {int(row["game_pk"]): row for row in rows}


def load_database_populations(path: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text())
    return dict(payload["populations"])


def load_normalized_games() -> tuple[set[int], dict[int, str], dict[str, Any]]:
    path = REPO_ROOT / NORMALIZED_GAMES
    # ParquetFile avoids Hive partition inference adding a second, dictionary-
    # typed `season` column from the parent directory name.
    table = parquet.ParquetFile(path).read(
        columns=["game_pk", "game_date", "season", "game_type"]
    )
    rows = table.to_pylist()
    filtered = [row for row in rows if int(row["season"]) == 2026]
    ids = {int(row["game_pk"]) for row in filtered}
    types = {int(row["game_pk"]): str(row["game_type"]) for row in filtered}
    dates = [str(row["game_date"])[:10] for row in filtered]
    return ids, types, {
        "source_path": str(NORMALIZED_GAMES),
        "source_sha256": sha256(path),
        "raw_row_count": len(filtered),
        "distinct_game_pk_count": len(ids),
        "earliest_date": min(dates, default=None),
        "latest_date": max(dates, default=None),
    }


def sqlite_populations() -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for path in sorted((REPO_ROOT / SQLITE_ROOT).rglob("*.sqlite3"), key=str):
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            tables = [
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
            ]
            for table in tables:
                columns = [
                    row[1]
                    for row in connection.execute(
                        f'PRAGMA table_info("{table}")'
                    )
                ]
                id_column = next(
                    (column for column in ("game_pk", "game_id") if column in columns),
                    None,
                )
                date_column = next(
                    (column for column in ("game_date", "slate_date") if column in columns),
                    None,
                )
                if id_column is None or date_column is None:
                    continue
                rows = list(
                    connection.execute(
                        f"""
                        SELECT {id_column}, {date_column}
                        FROM "{table}"
                        WHERE {date_column} >= '2026-01-01'
                          AND {date_column} < '2027-01-01'
                        ORDER BY {id_column}, {date_column}
                        """
                    )
                )
                ids = {int(row[0]) for row in rows if row[0] is not None}
                dates = [str(row[1])[:10] for row in rows if row[1] is not None]
                name = f"sqlite:{display_path(path)}:{table}"
                output[name] = {
                    "ids": ids,
                    "raw_row_count": len(rows),
                    "earliest_date": min(dates, default=None),
                    "latest_date": max(dates, default=None),
                    "source_sha256": sha256(path),
                }
        finally:
            connection.close()
    return output


def _population_report(
    name: str,
    ids: set[int],
    dates: Iterable[str],
    proposal_ids: set[int],
    authoritative: dict[int, dict[str, Any]],
    *,
    raw_row_count: int | None = None,
    phase_columns_present: list[str] | None = None,
) -> dict[str, Any]:
    covered = ids.intersection(authoritative)
    phase_counts = Counter()
    type_counts = Counter()
    special_count = 0
    for game_pk in covered:
        record = authoritative[game_pk]
        if len(record["season_phases"]) == 1:
            phase = next(iter(record["season_phases"]))
            if phase is None:
                special_count += 1
            else:
                phase_counts[str(phase)] += 1
        if len(record["source_game_types"]) == 1:
            type_counts[str(next(iter(record["source_game_types"])))] += 1
    values = sorted(value for value in dates if value)
    return {
        "source": name,
        "raw_row_count": raw_row_count,
        "distinct_game_pk_count": len(ids),
        "game_pks": sorted(ids),
        "intersection_with_proposal_count": len(ids & proposal_ids),
        "intersection_with_proposal_game_pks": sorted(ids & proposal_ids),
        "source_only_vs_proposal_count": len(ids - proposal_ids),
        "source_only_vs_proposal_game_pks": sorted(ids - proposal_ids),
        "proposal_only_vs_source_count": len(proposal_ids - ids),
        "proposal_only_vs_source_game_pks": sorted(proposal_ids - ids),
        "authoritative_type_covered_count": len(covered),
        "authoritative_type_missing_count": len(ids - covered),
        "authoritative_type_missing_game_pks": sorted(ids - covered),
        "authoritative_type_counts": dict(sorted(type_counts.items())),
        "authoritative_phase_counts": dict(sorted(phase_counts.items())),
        "authoritative_special_event_count": special_count,
        "earliest_scheduled_date": min(values, default=None),
        "latest_scheduled_date": max(values, default=None),
        "phase_columns_present": phase_columns_present,
    }


def build(database_snapshot: Path, proposal_path: Path) -> dict[str, Any]:
    authoritative, source_manifest, observation_errors = scan_authoritative_schedules()
    proposal = load_proposal(proposal_path)
    proposal_ids = set(proposal)
    populations: list[dict[str, Any]] = []
    authoritative_ids = set(authoritative)
    authoritative_dates = [
        date for row in authoritative.values() for date in row["scheduled_dates"]
    ]
    populations.append(
        _population_report(
            "retained_authoritative_statsapi_schedules",
            authoritative_ids,
            authoritative_dates,
            proposal_ids,
            authoritative,
            raw_row_count=sum(row["observation_count"] for row in authoritative.values()),
        )
    )
    proposal_dates = [
        date
        for game_pk in proposal_ids.intersection(authoritative)
        for date in authoritative[game_pk]["scheduled_dates"]
    ]
    populations.append(
        _population_report(
            "offline_canonical_phase_proposal",
            proposal_ids,
            proposal_dates,
            proposal_ids,
            authoritative,
            raw_row_count=len(proposal),
        )
    )

    database = load_database_populations(database_snapshot)
    for name, population in database.items():
        ids = {int(row["game_pk"]) for row in population["rows"]}
        dates = [date for row in population["rows"] for date in row["game_dates"]]
        populations.append(
            _population_report(
                name,
                ids,
                dates,
                proposal_ids,
                authoritative,
                raw_row_count=int(population["raw_row_count"]),
                phase_columns_present=list(population["phase_columns_present"]),
            )
        )

    normalized_ids, normalized_types, normalized_summary = load_normalized_games()
    normalized_conflicts = []
    for game_pk, raw_type in sorted(normalized_types.items()):
        try:
            normalized = normalize_source_game_type(raw_type, season=2026)
        except PhaseContractError as exc:
            normalized_conflicts.append(
                {"game_pk": game_pk, "source": "normalized_games_2026", "reason": str(exc)}
            )
            continue
        schedule = authoritative.get(game_pk)
        if schedule and normalized.raw_game_type not in schedule["source_game_types"]:
            normalized_conflicts.append(
                {
                    "game_pk": game_pk,
                    "source": "normalized_games_2026",
                    "reason": "NORMALIZED_VS_SCHEDULE_TYPE_CONFLICT",
                    "normalized_game_type": normalized.raw_game_type,
                    "schedule_game_types": sorted(schedule["source_game_types"]),
                }
            )
    populations.append(
        _population_report(
            "normalized_games_2026",
            normalized_ids,
            [normalized_summary["earliest_date"], normalized_summary["latest_date"]],
            proposal_ids,
            authoritative,
            raw_row_count=normalized_summary["raw_row_count"],
        )
    )

    sqlite_sources = sqlite_populations()
    for name, source in sqlite_sources.items():
        populations.append(
            _population_report(
                name,
                source["ids"],
                [source["earliest_date"], source["latest_date"]],
                proposal_ids,
                authoritative,
                raw_row_count=source["raw_row_count"],
            )
        )

    duplicate_conflicts: list[dict[str, Any]] = []
    for game_pk, record in sorted(authoritative.items()):
        conflict_fields = {
            key: sorted(value, key=lambda item: str(item))
            for key, value in (
                ("source_seasons", record["source_seasons"]),
                ("source_game_types", record["source_game_types"]),
                ("season_phases", record["season_phases"]),
                ("postseason_rounds", record["postseason_rounds"]),
            )
            if len(value) > 1
        }
        if conflict_fields:
            duplicate_conflicts.append(
                {"game_pk": game_pk, "reason": "DUPLICATE_IDENTITY_CONFLICT", **conflict_fields}
            )
    duplicate_conflicts.extend(normalized_conflicts)
    for source_name, population in database.items():
        for row in population.get("duplicate_identity_conflicts", []):
            duplicate_conflicts.append(
                {
                    "game_pk": int(row["game_pk"]),
                    "source": source_name,
                    "reason": "DATABASE_DUPLICATE_IDENTITY_CONFLICT",
                    "identity_variants": row["identity_variants"],
                }
            )

    source_sets = {
        row["source"]: set(row["game_pks"])
        for row in populations
    }
    invalid_schedule_ids = {
        int(row["game_pk"])
        for row in observation_errors
        if row.get("game_pk") is not None
    }
    canonical_universe = set().union(*source_sets.values(), invalid_schedule_ids)
    missing_ids = canonical_universe - authoritative_ids
    source_membership = {
        game_pk: sorted(
            name for name, ids in source_sets.items() if game_pk in ids
        )
        for game_pk in canonical_universe
    }
    observed_dates_by_game: dict[int, set[str]] = {}
    for game_pk, record in authoritative.items():
        observed_dates_by_game.setdefault(game_pk, set()).update(record["scheduled_dates"])
    for population in database.values():
        for row in population["rows"]:
            observed_dates_by_game.setdefault(int(row["game_pk"]), set()).update(
                str(value) for value in row["game_dates"]
            )
    missing_ledger = [
        {
            "game_pk": game_pk,
            "reason": "NO_RETAINED_AUTHORITATIVE_STATSAPI_TYPE",
            "present_in_sources": source_membership[game_pk],
            "observed_identity_dates_not_used_for_phase": sorted(
                observed_dates_by_game.get(game_pk, set())
            ),
        }
        for game_pk in sorted(missing_ids)
    ]
    phase_counts = Counter()
    type_counts = Counter()
    special_count = 0
    for record in authoritative.values():
        phase = next(iter(record["season_phases"]))
        raw_type = next(iter(record["source_game_types"]))
        type_counts[str(raw_type)] += 1
        if phase is None:
            special_count += 1
        else:
            phase_counts[str(phase)] += 1

    missing_dates = sorted(
        date
        for game_pk in missing_ids
        for date in observed_dates_by_game.get(game_pk, set())
    )
    missing_game_pks = sorted(missing_ids)
    source_completion_proposal = {
        "status": "PROPOSED_NOT_EXECUTED" if missing_ids else "NOT_REQUIRED",
        "execution_authorized": False,
        "purpose": "complete authoritative source type only; never infer phase from date",
        "provider": "MLB StatsAPI",
        "endpoint": "https://statsapi.mlb.com/api/v1/schedule",
        "parameters": (
            {
                "sportId": 1,
                "startDate": min(missing_dates),
                "endDate": max(missing_dates),
            }
            if missing_ids
            else None
        ),
        "expected_request_count": 1 if missing_ids else 0,
        "expected_paid_credit_count": 0,
        "expected_target_game_pk_count": len(missing_game_pks),
        "expected_target_game_pks_sha256": hashlib.sha256(
            canonical_json(missing_game_pks).encode()
        ).hexdigest(),
        "storage_path": (
            "backend/mlb/data/external/statsapi/raw/2026/"
            f"schedule_{min(missing_dates)}_{max(missing_dates)}.json"
            if missing_ids
            else None
        ),
        "hashing_method": "SHA-256 over exact HTTP response bytes before JSON parsing",
        "idempotence_guard": (
            "refuse overwrite when target path exists; verify and reuse identical bytes; "
            "after parsing require every target gamePk exactly once or consistently duplicated, "
            "season=2026, an exact contract_v1 gameType, zero conflicting types, and a source hash"
        ),
        "smallest_request_rationale": (
            (
                "one free inclusive StatsAPI schedule range spans every observed identity date for "
                "the missing set; dates scope acquisition only and do not classify phase"
            )
            if missing_ids
            else "no source-completion acquisition is required because the missing set is empty"
        ),
    }
    return {
        "contract_name": CONTRACT_NAME,
        "evidence_mode": "READ_ONLY_LOCAL_FILES_PLUS_READ_ONLY_DATABASE_SNAPSHOT",
        "phase_inference_from_dates": False,
        "population_selection_dates_are_identity_filters_only": True,
        "offline_proposal_is_retained_file_subset": proposal_ids < authoritative_ids,
        "population_count": len(populations),
        "populations": populations,
        "pairwise_intersections": [
            {
                "left": left,
                "right": right,
                "intersection_count": len(source_sets[left] & source_sets[right]),
                "left_only_count": len(source_sets[left] - source_sets[right]),
                "right_only_count": len(source_sets[right] - source_sets[left]),
            }
            for index, left in enumerate(sorted(source_sets))
            for right in sorted(source_sets)[index + 1 :]
        ],
        "authoritative_schedule_summary": {
            "source_file_count": len(source_manifest),
            "observation_count": sum(
                row["retained_2026_game_observations"] for row in source_manifest
            ),
            "distinct_game_pk_count": len(authoritative_ids),
            "consistent_duplicate_observation_count": sum(
                row["observation_count"] for row in authoritative.values()
            )
            - len(authoritative_ids),
            "missing_or_unknown_observation_count": len(observation_errors),
            "duplicate_identity_conflict_count": len(duplicate_conflicts),
            "source_game_type_counts": dict(sorted(type_counts.items())),
            "season_phase_counts": dict(sorted(phase_counts.items())),
            "special_event_count": special_count,
            "earliest_scheduled_date": min(authoritative_dates, default=None),
            "latest_scheduled_date": max(authoritative_dates, default=None),
        },
        "canonical_universe": {
            "distinct_game_pk_count": len(canonical_universe),
            "authoritatively_classified_count": len(canonical_universe & authoritative_ids),
            "missing_authoritative_type_count": len(missing_ids),
            "missing_authoritative_type_game_pks": sorted(missing_ids),
            "fully_classified": not missing_ids and not duplicate_conflicts and not observation_errors,
        },
        "missing_ledger": missing_ledger,
        "conflict_ledger": duplicate_conflicts + observation_errors,
        "source_completion_proposal": source_completion_proposal,
        "activation_recommendation": {
            "recommendation": (
                "CANONICAL_PHASE_ACTIVATION_GATE_READY"
                if not missing_ids and not duplicate_conflicts and not observation_errors
                else "CANONICAL_PHASE_ACTIVATION_GATE_BLOCKED"
            ),
            "blocking_reasons": [
                reason
                for condition, reason in (
                    (bool(missing_ids), f"{len(missing_ids)} canonical gamePks lack retained authoritative type"),
                    (bool(duplicate_conflicts), f"{len(duplicate_conflicts)} authoritative identity conflicts"),
                    (bool(observation_errors), f"{len(observation_errors)} invalid authoritative observations"),
                )
                if condition
            ],
            "exact_next_step": (
                "Review and separately authorize the unexecuted one-request free StatsAPI "
                "source-completion proposal; retain and hash its bytes, rebuild this gate, "
                "and require missing/conflict counts of zero before migration activation."
                if missing_ids
                else "Proceed to the separately governed migration activation preflight."
            ),
        },
        "authoritative_schedule_source_manifest": source_manifest,
        "normalized_games_summary": normalized_summary,
        "database_snapshot_sha256": sha256(database_snapshot),
        "offline_proposal_sha256": sha256(proposal_path),
    }


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.write_text("".join(canonical_json(row) + "\n" for row in rows), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-snapshot", required=True, type=Path)
    parser.add_argument("--proposal", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--replace-output",
        action="store_true",
        help="Replace only this builder's known files in an existing output directory",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_names = {
        "canonical_population_reconciliation.json",
        "missing_game_pk_ledger.jsonl",
        "conflict_ledger.jsonl",
        "authoritative_schedule_source_manifest.jsonl",
        "source_completion_acquisition_proposal.json",
        "activation_recommendation.json",
        "coverage_gate_summary.json",
    }
    if args.output_dir.exists() and not args.replace_output:
        raise SystemExit(f"OUTPUT_DIRECTORY_ALREADY_EXISTS:{args.output_dir}")
    if args.output_dir.exists():
        unexpected = sorted(
            path.name
            for path in args.output_dir.iterdir()
            if not path.is_file() or path.name not in output_names
        )
        if unexpected:
            raise SystemExit(f"OUTPUT_DIRECTORY_HAS_UNKNOWN_CONTENT:{','.join(unexpected)}")
    report = build(args.database_snapshot, args.proposal)
    args.output_dir.mkdir(parents=True, exist_ok=args.replace_output)
    report_without_ledgers = {
        key: value
        for key, value in report.items()
        if key not in {"missing_ledger", "conflict_ledger", "authoritative_schedule_source_manifest"}
    }
    write_json(args.output_dir / "canonical_population_reconciliation.json", report_without_ledgers)
    write_jsonl(args.output_dir / "missing_game_pk_ledger.jsonl", report["missing_ledger"])
    write_jsonl(args.output_dir / "conflict_ledger.jsonl", report["conflict_ledger"])
    write_jsonl(
        args.output_dir / "authoritative_schedule_source_manifest.jsonl",
        report["authoritative_schedule_source_manifest"],
    )
    write_json(
        args.output_dir / "source_completion_acquisition_proposal.json",
        report["source_completion_proposal"],
    )
    write_json(
        args.output_dir / "activation_recommendation.json",
        report["activation_recommendation"],
    )
    summary = {
        "contract_name": CONTRACT_NAME,
        "canonical_universe": report["canonical_universe"],
        "authoritative_schedule_summary": report["authoritative_schedule_summary"],
        "offline_proposal_is_retained_file_subset": report[
            "offline_proposal_is_retained_file_subset"
        ],
        "gate_status": (
            "READY" if report["canonical_universe"]["fully_classified"] else "BLOCKED"
        ),
    }
    write_json(args.output_dir / "coverage_gate_summary.json", summary)
    print(canonical_json(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
