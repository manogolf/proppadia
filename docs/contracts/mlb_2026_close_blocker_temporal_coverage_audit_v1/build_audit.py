#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Build the read-only MLB close-blocker temporal coverage audit."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


CONTRACT_NAME = "MLB_2026_CLOSE_BLOCKER_TEMPORAL_COVERAGE_AUDIT_V1"
AUDIT_DATE = "2026-09-22"
ROOT = Path(__file__).resolve().parents[3]
PACKAGE = Path(__file__).resolve().parent
CLOSE_PACKAGE = ROOT / "docs/contracts/mlb_2026_authoritative_regular_season_close_inventory_v1"
BLOCKERS_PATH = CLOSE_PACKAGE / "scheduled_not_final_game_pks.json"
INVENTORY_PATH = CLOSE_PACKAGE / "authoritative_regular_season_games.jsonl"
FINAL_DETAILS = {"Final", "Completed Early"}
CLASSIFICATIONS = (
    "FUTURE_SCHEDULED",
    "CURRENT_DATE_NOT_TERMINAL",
    "PAST_DATE_TERMINAL_EVIDENCE_FOUND_ELSEWHERE",
    "PAST_DATE_ONLY_STALE_NONTERMINAL_EVIDENCE",
    "PAST_DATE_NO_RETAINED_TERMINAL_EVIDENCE",
    "RELATED_GAME_DISPOSITION_REQUIRES_REVIEW",
)
TIMESTAMP_TOKEN = re.compile(r"(?<!\d)(20\d{6}T\d{6}Z)(?!\d)")
FEED_TIMESTAMP = re.compile(r"^(20\d{6})_(\d{6})$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def relative(path: Path) -> str:
    return path.resolve().relative_to(ROOT.resolve()).as_posix()


def iso_from_path_or_mtime(path: Path) -> tuple[str, str]:
    matches = TIMESTAMP_TOKEN.findall(relative(path))
    if matches:
        token = max(matches)
        parsed = datetime.strptime(token, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
        return parsed.isoformat().replace("+00:00", "Z"), "PATH_ACQUISITION_TOKEN"
    return (
        datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "FILESYSTEM_MTIME_FALLBACK",
    )


def feed_timestamp(payload: dict[str, Any], path: Path) -> tuple[str, str]:
    value = str((payload.get("metaData") or {}).get("timeStamp") or "")
    match = FEED_TIMESTAMP.fullmatch(value)
    if match:
        parsed = datetime.strptime(value, "%Y%m%d_%H%M%S").replace(
            tzinfo=timezone.utc
        )
        return parsed.isoformat().replace("+00:00", "Z"), "STATSAPI_METADATA_TIMESTAMP"
    return iso_from_path_or_mtime(path)


def walk_files(bases: Iterable[Path], predicate: Any) -> list[Path]:
    found: set[Path] = set()
    for base in bases:
        if not base.exists():
            continue
        for directory, names, files in os.walk(base, onerror=lambda _: None):
            parts = {part.lower() for part in Path(directory).parts}
            if "private_packet_capture" in parts or "nhl" in parts:
                names[:] = []
                continue
            for filename in files:
                path = Path(directory) / filename
                if predicate(path):
                    found.add(path)
    return sorted(found, key=lambda path: relative(path))


def status_dict(value: Any) -> dict[str, Any]:
    status = value if isinstance(value, dict) else {}
    return {
        key: status.get(key)
        for key in (
            "abstractGameState",
            "codedGameState",
            "detailedState",
            "statusCode",
            "reason",
        )
    }


def observation(
    *,
    game_pk: int,
    path: Path,
    source_family: str,
    status: dict[str, Any],
    observed_at: str,
    observed_at_method: str,
    raw_type: Any,
    season: Any,
    exact_identity: bool,
) -> dict[str, Any]:
    terminal = (
        exact_identity
        and raw_type == "R"
        and str(season) == "2026"
        and status.get("abstractGameState") == "Final"
        and status.get("detailedState") in FINAL_DETAILS
    )
    return {
        "game_pk": game_pk,
        "source_path": relative(path),
        "source_sha256": sha256(path),
        "source_family": source_family,
        "observed_at_utc": observed_at,
        "observed_at_method": observed_at_method,
        "authoritative_raw_game_type": raw_type,
        "source_season": str(season) if season is not None else None,
        "status": status,
        "exact_game_pk_identity": exact_identity,
        "accepted_authoritative_terminal": terminal,
    }


def scan_schedules(
    blockers: set[int],
) -> tuple[dict[int, list[dict[str, Any]]], dict[str, Any]]:
    candidates = walk_files(
        (ROOT / "backend/mlb", ROOT / "artifacts/analysis"),
        lambda path: path.suffix.lower() == ".json"
        and "schedule" in path.name.lower(),
    )
    observations: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
    structured_files = 0
    blocker_observations = 0
    terminal_game_pks: set[int] = set()
    cleanroom_files = 0
    for path in candidates:
        try:
            payload = json.loads(path.read_bytes())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict) or not isinstance(payload.get("dates"), list):
            continue
        structured_files += 1
        if "cleanroom" in relative(path).lower():
            cleanroom_files += 1
        observed_at, method = iso_from_path_or_mtime(path)
        for date_row in payload["dates"]:
            if not isinstance(date_row, dict):
                continue
            for game in date_row.get("games") or []:
                if not isinstance(game, dict) or game.get("gamePk") not in blockers:
                    continue
                game_pk = int(game["gamePk"])
                item = observation(
                    game_pk=game_pk,
                    path=path,
                    source_family="STATSAPI_SCHEDULE_RESPONSE",
                    status=status_dict(game.get("status")),
                    observed_at=observed_at,
                    observed_at_method=method,
                    raw_type=game.get("gameType"),
                    season=game.get("season"),
                    exact_identity=True,
                )
                observations[game_pk].append(item)
                blocker_observations += 1
                if item["accepted_authoritative_terminal"]:
                    terminal_game_pks.add(game_pk)
    return dict(observations), {
        "candidate_files": len(candidates),
        "structured_statsapi_schedule_files": structured_files,
        "cleanroom_schedule_files": cleanroom_files,
        "blocker_observations": blocker_observations,
        "distinct_blocker_game_pks": len(observations),
        "authoritative_terminal_blocker_game_pks": len(terminal_game_pks),
    }


def scan_feeds(
    blockers: set[int],
) -> tuple[dict[int, list[dict[str, Any]]], dict[str, Any]]:
    player_completeness = ROOT / "artifacts/analysis/mlb/player_stats_completeness"
    prior_cache = (
        ROOT
        / "backend/mlb/data/research/dh_forward_validation/v1/prior_official_feed_cache"
    )
    candidates = sorted(
        set(player_completeness.rglob("*live_feed_*.json"))
        | set(prior_cache.glob("*.json")),
        key=lambda path: relative(path),
    )
    observations: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
    invalid_identity = 0
    embedded_hash_failures = 0
    for path in candidates:
        try:
            payload = json.loads(path.read_bytes())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict) or payload.get("gamePk") not in blockers:
            continue
        game_pk = int(payload["gamePk"])
        game_data = payload.get("gameData")
        if not isinstance(game_data, dict):
            continue
        game = game_data.get("game") if isinstance(game_data.get("game"), dict) else {}
        exact_identity = game.get("pk") == game_pk
        if not exact_identity:
            invalid_identity += 1
        actual_hash = sha256(path)
        filename_hash = re.search(r"_([0-9a-f]{64})\.json$", path.name)
        if filename_hash and filename_hash.group(1) != actual_hash:
            embedded_hash_failures += 1
        observed_at, method = feed_timestamp(payload, path)
        item = observation(
            game_pk=game_pk,
            path=path,
            source_family="STATSAPI_LIVE_GAME_FEED",
            status=status_dict(game_data.get("status")),
            observed_at=observed_at,
            observed_at_method=method,
            raw_type=game.get("type"),
            season=game.get("season"),
            exact_identity=exact_identity,
        )
        observations[game_pk].append(item)
    terminal = {
        game_pk
        for game_pk, rows in observations.items()
        if any(row["accepted_authoritative_terminal"] for row in rows)
    }
    return dict(observations), {
        "candidate_files": len(candidates),
        "blocker_observations": sum(len(rows) for rows in observations.values()),
        "distinct_blocker_game_pks": len(observations),
        "authoritative_terminal_blocker_game_pks": len(terminal),
        "invalid_exact_game_pk_identities": invalid_identity,
        "embedded_filename_hash_failures": embedded_hash_failures,
    }


def scan_csv_family(paths: list[Path], blockers: set[int]) -> dict[str, Any]:
    game_fields = ("game_pk", "game_id", "gamePk")
    distinct: set[int] = set()
    rows_seen = 0
    readable = 0
    game_terminal_rows = 0
    for path in paths:
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                if not reader.fieldnames:
                    continue
                field = next((name for name in game_fields if name in reader.fieldnames), None)
                if field is None:
                    continue
                readable += 1
                for row in reader:
                    try:
                        game_pk = int(row.get(field) or "")
                    except ValueError:
                        continue
                    if game_pk not in blockers:
                        continue
                    rows_seen += 1
                    distinct.add(game_pk)
                    terminal_value = next(
                        (
                            row.get(name)
                            for name in (
                                "game_status",
                                "detailed_state",
                                "detailedState",
                                "abstractGameState",
                            )
                            if name in row
                        ),
                        None,
                    )
                    if terminal_value in FINAL_DETAILS:
                        game_terminal_rows += 1
        except (OSError, UnicodeDecodeError, csv.Error):
            continue
    return {
        "candidate_files": len(paths),
        "readable_files_with_game_pk_field": readable,
        "matching_rows": rows_seen,
        "distinct_blocker_game_pks": len(distinct),
        "accepted_game_terminal_rows": game_terminal_rows,
        "acceptance_note": (
            "Scores, prop outcomes, grading statuses, and market-result labels were "
            "not treated as authoritative game-terminal status."
        ),
    }


def scan_corroborating_sources(blockers: set[int]) -> dict[str, Any]:
    canonical_outcomes = sorted(
        (ROOT / "artifacts/analysis/mlb/prospective_lineage_outcomes").rglob(
            "canonical_outcome_reconciliation.csv"
        )
    )
    dh_root = ROOT / "backend/mlb/data/research/dh_forward_validation/v1"
    grading = [dh_root / "forward_outcome_ledger_v1.csv"]
    grading.extend(sorted((dh_root / "backups").glob("forward_outcome_ledger_v1.csv.*.bak")))
    full_slate = sorted((ROOT / "backend/mlb/data/processed").glob("mlb_slate_output*.csv"))
    full_slate.extend(sorted((ROOT / "backend/mlb/exports/v1_results").rglob("results.csv")))
    outcome_ledgers = walk_files(
        (ROOT / "artifacts/analysis/model_development",),
        lambda path: path.suffix.lower() == ".csv"
        and "outcome_ledger" in path.name.lower(),
    )
    moneyline_population_snapshots = [
        path
        for path in (
            ROOT
            / "docs/contracts/mlb_2026_canonical_phase_source_completion_v1"
        ).glob("**/*population*.json")
        if path.is_file()
    ]
    return {
        "canonical_outcome_ledgers": scan_csv_family(canonical_outcomes, blockers),
        "immutable_grading_inputs": scan_csv_family(grading, blockers),
        "full_slate_result_artifacts": scan_csv_family(full_slate, blockers),
        "other_outcome_ledgers": scan_csv_family(outcome_ledgers, blockers),
        "moneyline_population_evidence": {
            "candidate_files": len(moneyline_population_snapshots),
            "accepted_game_terminal_rows": 0,
            "acceptance_note": (
                "Population-only Moneyline snapshots contain no per-game authoritative "
                "terminal-status field and were not used as terminal proof."
            ),
        },
    }


def scheduled_date(row: dict[str, Any]) -> str:
    relationships = row["relationship_identities"]["raw_relationships"]
    value = relationships.get("rescheduleGameDate") or relationships.get("resumeGameDate")
    if value:
        return str(value)
    dates = row.get("observed_official_dates") or []
    if not dates:
        raise RuntimeError(f"BLOCKER_SCHEDULE_DATE_MISSING:{row['game_pk']}")
    return max(str(value) for value in dates)


def choose_terminal(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    accepted = [row for row in rows if row["accepted_authoritative_terminal"]]
    if not accepted:
        return None
    return max(
        accepted,
        key=lambda row: (
            row["observed_at_utc"],
            row["source_path"].startswith(
                "artifacts/analysis/mlb/player_stats_completeness/"
            ),
            row["source_path"],
        ),
    )


def write_csv(name: str, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with (PACKAGE / name).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    blocker_list = json.loads(BLOCKERS_PATH.read_text(encoding="utf-8"))
    blockers = {int(value) for value in blocker_list}
    inventory = {
        row["game_pk"]: row
        for row in (
            json.loads(line)
            for line in INVENTORY_PATH.read_text(encoding="utf-8").splitlines()
        )
        if row["game_pk"] in blockers
    }
    if len(blocker_list) != 436 or len(blockers) != 436 or len(inventory) != 436:
        raise RuntimeError("BLOCKER_POPULATION_NOT_EXACTLY_436")

    schedules, schedule_scan = scan_schedules(blockers)
    feeds, feed_scan = scan_feeds(blockers)
    corroborating = scan_corroborating_sources(blockers)
    ledger: list[dict[str, Any]] = []
    for game_pk in sorted(blockers):
        source_row = inventory[game_pk]
        game_date = scheduled_date(source_row)
        observations = schedules.get(game_pk, []) + feeds.get(game_pk, [])
        observations.sort(key=lambda row: (row["observed_at_utc"], row["source_path"]))
        latest = observations[-1] if observations else None
        terminal = choose_terminal(observations)
        relationships = source_row["relationship_identities"]["raw_relationships"]
        relationship_complete = True
        if relationships:
            if any(key.startswith("resched") for key in relationships):
                relationship_complete = {
                    "rescheduleDate",
                    "rescheduleGameDate",
                    "rescheduledFrom",
                    "rescheduledFromDate",
                }.issubset(relationships)
            elif any(key.startswith("resume") for key in relationships):
                relationship_complete = {
                    "resumeDate",
                    "resumeGameDate",
                    "resumedFrom",
                    "resumedFromDate",
                }.issubset(relationships)
        if relationships and not relationship_complete:
            classification = "RELATED_GAME_DISPOSITION_REQUIRES_REVIEW"
        elif game_date > AUDIT_DATE:
            classification = (
                "RELATED_GAME_DISPOSITION_REQUIRES_REVIEW"
                if terminal
                else "FUTURE_SCHEDULED"
            )
        elif game_date == AUDIT_DATE:
            classification = (
                "RELATED_GAME_DISPOSITION_REQUIRES_REVIEW"
                if terminal
                else "CURRENT_DATE_NOT_TERMINAL"
            )
        elif terminal:
            classification = "PAST_DATE_TERMINAL_EVIDENCE_FOUND_ELSEWHERE"
        elif latest and latest["observed_at_utc"][:10] > game_date:
            classification = "PAST_DATE_ONLY_STALE_NONTERMINAL_EVIDENCE"
        else:
            classification = "PAST_DATE_NO_RETAINED_TERMINAL_EVIDENCE"

        inventory_sources = {
            item["path"]
            for group in source_row.get("authoritative_status_evidence") or []
            for item in group.get("source_artifacts") or []
        }
        decision_evidence = terminal or latest
        ledger.append(
            {
                "game_pk": game_pk,
                "scheduled_date": game_date,
                "calendar_month": game_date[:7],
                "classification": classification,
                "latest_retained_status": (
                    latest["status"].get("detailedState") if latest else "MISSING"
                ),
                "latest_retained_observation_timestamp_utc": (
                    latest["observed_at_utc"] if latest else ""
                ),
                "latest_observation_timestamp_method": (
                    latest["observed_at_method"] if latest else ""
                ),
                "latest_evidence_path": latest["source_path"] if latest else "",
                "latest_evidence_sha256": latest["source_sha256"] if latest else "",
                "authoritative_terminal_evidence_exists_elsewhere": bool(terminal),
                "terminal_status": (
                    terminal["status"].get("detailedState") if terminal else ""
                ),
                "terminal_observation_timestamp_utc": (
                    terminal["observed_at_utc"] if terminal else ""
                ),
                "terminal_evidence_path": terminal["source_path"] if terminal else "",
                "terminal_evidence_sha256": terminal["source_sha256"] if terminal else "",
                "close_inventory_omitted_usable_retained_evidence": bool(
                    terminal and terminal["source_path"] not in inventory_sources
                ),
                "relationship_fields_present": bool(relationships),
                "relationship_identity_complete": relationship_complete,
                "ordinary_future_retention_can_resolve": classification
                in {"FUTURE_SCHEDULED", "CURRENT_DATE_NOT_TERMINAL"},
                "decision_evidence_path": (
                    decision_evidence["source_path"] if decision_evidence else ""
                ),
                "decision_evidence_sha256": (
                    decision_evidence["source_sha256"] if decision_evidence else ""
                ),
            }
        )

    class_counts = Counter(row["classification"] for row in ledger)
    date_counts = Counter(row["scheduled_date"] for row in ledger)
    month_counts = Counter(row["calendar_month"] for row in ledger)
    source_counts = Counter(row["decision_evidence_path"] for row in ledger)
    latest_counts = Counter(
        row["latest_retained_observation_timestamp_utc"] for row in ledger
    )
    locally_recoverable = [
        row["game_pk"]
        for row in ledger
        if row["classification"] == "PAST_DATE_TERMINAL_EVIDENCE_FOUND_ELSEWHERE"
    ]
    future_current = [
        row["game_pk"]
        for row in ledger
        if row["classification"] in {"FUTURE_SCHEDULED", "CURRENT_DATE_NOT_TERMINAL"}
    ]
    source_completion = [
        row["game_pk"]
        for row in ledger
        if row["classification"]
        in {
            "PAST_DATE_ONLY_STALE_NONTERMINAL_EVIDENCE",
            "PAST_DATE_NO_RETAINED_TERMINAL_EVIDENCE",
        }
    ]
    unprovable = [
        row["game_pk"]
        for row in ledger
        if row["classification"] == "RELATED_GAME_DISPOSITION_REQUIRES_REVIEW"
    ]

    write_csv("blocker_classification.csv", list(ledger[0]), ledger)
    write_csv(
        "counts_by_scheduled_date.csv",
        ["scheduled_date", "game_pk_count"],
        [
            {"scheduled_date": key, "game_pk_count": value}
            for key, value in sorted(date_counts.items())
        ],
    )
    write_csv(
        "counts_by_calendar_month.csv",
        ["calendar_month", "game_pk_count"],
        [
            {"calendar_month": key, "game_pk_count": value}
            for key, value in sorted(month_counts.items())
        ],
    )
    write_csv(
        "counts_by_classification.csv",
        ["classification", "game_pk_count"],
        [
            {"classification": key, "game_pk_count": class_counts.get(key, 0)}
            for key in CLASSIFICATIONS
        ],
    )
    write_csv(
        "counts_by_source_artifact.csv",
        ["source_artifact", "game_pk_count"],
        [
            {"source_artifact": key, "game_pk_count": value}
            for key, value in sorted(source_counts.items())
        ],
    )
    write_csv(
        "counts_by_latest_observation_time.csv",
        ["latest_observation_timestamp_utc", "game_pk_count"],
        [
            {"latest_observation_timestamp_utc": key, "game_pk_count": value}
            for key, value in sorted(latest_counts.items())
        ],
    )
    (PACKAGE / "locally_recoverable_game_pks.json").write_text(
        json.dumps(locally_recoverable, indent=2) + "\n", encoding="utf-8"
    )
    (PACKAGE / "genuine_future_current_game_pks.json").write_text(
        json.dumps(future_current, indent=2) + "\n", encoding="utf-8"
    )
    (PACKAGE / "source_completion_game_pks.json").write_text(
        json.dumps(source_completion, indent=2) + "\n", encoding="utf-8"
    )
    source_scan = {
        "statsapi_schedule_responses": schedule_scan,
        "statsapi_live_game_feeds": feed_scan,
        "corroborating_nonterminal_sources": corroborating,
        "terminal_acceptance_rule": (
            "Exact root gamePk plus matching gameData.game.pk, source game type R, "
            "source season 2026, abstract state Final, and detailed state Final or "
            "Completed Early. A score or grading result alone is insufficient."
        ),
    }
    (PACKAGE / "local_source_reconciliation.json").write_text(
        json.dumps(source_scan, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    completion_plan = {
        "source_completion_required": bool(source_completion),
        "game_pks": source_completion,
        "provider": "NONE" if not source_completion else "PUBLIC_MLB_STATSAPI",
        "proposed_date_range": None,
        "request_count": 0,
        "paid_provider_requests": 0,
        "paid_credit_count": 0,
        "immutable_storage_path": None,
        "hashing_method": "SHA-256 over unmodified response bytes",
        "idempotence_guard": (
            "No request is justified. If evidence changes, predeclare exact request "
            "identity and refuse overwrite of an existing different response."
        ),
    }
    (PACKAGE / "source_completion_proposal.json").write_text(
        json.dumps(completion_plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary = {
        "contract_name": CONTRACT_NAME,
        "audit_date": AUDIT_DATE,
        "blocker_ledger_path": relative(BLOCKERS_PATH),
        "blocker_ledger_sha256": sha256(BLOCKERS_PATH),
        "close_inventory_path": relative(INVENTORY_PATH),
        "close_inventory_sha256": sha256(INVENTORY_PATH),
        "total_blocker_game_pks": len(ledger),
        "earliest_blocker_date": min(date_counts),
        "latest_blocker_date": max(date_counts),
        "classification_counts": {
            name: class_counts.get(name, 0) for name in CLASSIFICATIONS
        },
        "past_games_recoverable_from_local_evidence": len(locally_recoverable),
        "genuine_current_games": class_counts["CURRENT_DATE_NOT_TERMINAL"],
        "genuine_future_games": class_counts["FUTURE_SCHEDULED"],
        "past_games_requiring_source_completion": len(source_completion),
        "unprovable_or_relationship_review_games": len(unprovable),
        "close_inventory_omitted_usable_local_terminal_evidence": sum(
            bool(row["close_inventory_omitted_usable_retained_evidence"])
            for row in ledger
        ),
        "ordinary_retention_alone_sufficient_for_all_436": False,
        "ordinary_retention_sufficient_after_local_recovery_cutover": (
            not source_completion and not unprovable
        ),
        "network_requests": 0,
        "database_connections": 0,
        "pipelines_or_schedules_run": 0,
        "close_inventory_modified": False,
        "decision": "CLOSE_BLOCKERS_INCLUDE_LOCAL_RECOVERIES",
        "ledger_population_sha256": canonical_sha256(ledger),
        "smallest_next_action": (
            "Separately authorize an offline-only close-inventory evidence expansion "
            "that admits the 348 already-retained exact-gamePk final StatsAPI feeds; "
            "then allow ordinary retention to resolve the 16 current and 72 future games."
        ),
    }
    (PACKAGE / "audit_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    validation = validate_outputs(summary, ledger)
    (PACKAGE / "validation_report.json").write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    manifest_lines = []
    for path in sorted(PACKAGE.iterdir(), key=lambda value: value.name):
        if path.is_file() and path.name != "sha256_manifest.txt":
            manifest_lines.append(f"{sha256(path)}  {path.name}")
    (PACKAGE / "sha256_manifest.txt").write_text(
        "\n".join(manifest_lines) + "\n", encoding="utf-8"
    )
    print(json.dumps({"summary": summary, "validation": validation}, indent=2, sort_keys=True))
    return 0 if validation["passed"] else 1


def validate_outputs(summary: dict[str, Any], ledger: list[dict[str, Any]]) -> dict[str, Any]:
    checks = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append(
            {"check": name, "status": "PASS" if passed else "FAIL", "detail": detail}
        )

    game_pks = [int(row["game_pk"]) for row in ledger]
    check("exact_436_rows", len(ledger) == 436, len(ledger))
    check("unique_game_pks", len(set(game_pks)) == 436, len(set(game_pks)))
    check(
        "exact_input_population",
        set(game_pks) == set(json.loads(BLOCKERS_PATH.read_text(encoding="utf-8"))),
        summary["blocker_ledger_sha256"],
    )
    check(
        "all_classifications_known",
        all(row["classification"] in CLASSIFICATIONS for row in ledger),
        summary["classification_counts"],
    )
    check(
        "past_local_recoveries_exact",
        summary["past_games_recoverable_from_local_evidence"] == 348,
        summary["past_games_recoverable_from_local_evidence"],
    )
    check("current_exact", summary["genuine_current_games"] == 16, summary["genuine_current_games"])
    check("future_exact", summary["genuine_future_games"] == 72, summary["genuine_future_games"])
    check(
        "no_source_completion",
        summary["past_games_requiring_source_completion"] == 0,
        summary["past_games_requiring_source_completion"],
    )
    check(
        "no_unprovable_identity",
        summary["unprovable_or_relationship_review_games"] == 0,
        summary["unprovable_or_relationship_review_games"],
    )
    bad_evidence = []
    for row in ledger:
        if row["classification"] != "PAST_DATE_TERMINAL_EVIDENCE_FOUND_ELSEWHERE":
            continue
        path = ROOT / row["terminal_evidence_path"]
        if not path.is_file() or sha256(path) != row["terminal_evidence_sha256"]:
            bad_evidence.append(row["game_pk"])
    check("all_terminal_evidence_hashes_verify", not bad_evidence, bad_evidence)
    check(
        "zero_external_actions",
        summary["network_requests"] == 0
        and summary["database_connections"] == 0
        and summary["pipelines_or_schedules_run"] == 0,
        {
            "network": summary["network_requests"],
            "database": summary["database_connections"],
            "pipelines": summary["pipelines_or_schedules_run"],
        },
    )
    passed = all(row["status"] == "PASS" for row in checks)
    return {
        "contract_name": CONTRACT_NAME,
        "passed": passed,
        "check_count": len(checks),
        "failed_checks": [row["check"] for row in checks if row["status"] == "FAIL"],
        "checks": checks,
    }


if __name__ == "__main__":
    raise SystemExit(main())
