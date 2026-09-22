#!/usr/bin/env python3
"""Build the local-only evidence tables for the postseason identity audit.

This script reads retained files only.  It has no network or database client
imports and writes only inside its own review package.
"""
from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
AUDIT_TIMESTAMP = "2026-09-22T22:37:47Z"
HEAD_AT_EXTRACTION = "0b99d2b34162a3f597a18021a5882fad8e713fc5"
HEAD_AT_FINAL_VALIDATION = "005099d0b8a33847e7e3a1bd025f33d1507d31bf"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(name: str, rows: list[dict[str, object]], fields: list[str]) -> None:
    path = OUT / name
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_events(path: Path) -> list[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("events", payload.get("data", []))
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def player_prop_events() -> tuple[list[dict[str, object]], dict[str, int]]:
    population: dict[str, dict[str, object]] = {}
    files = sorted((ROOT / "backend/mlb/exports/odds_history").glob("2026-*/odds_mlb_playerprops*.json"))
    unique_bytes: set[str] = set()
    for path in files:
        source_hash = sha256(path)
        unique_bytes.add(source_hash)
        for event in load_events(path):
            event_id = str(event.get("id") or "").strip()
            if not event_id:
                continue
            row = population.setdefault(event_id, {
                "event_id": event_id,
                "commence_times": set(),
                "away_teams": set(),
                "home_teams": set(),
                "source_paths": set(),
                "source_hashes": set(),
                "observations": 0,
                "betonline": False,
            })
            row["observations"] = int(row["observations"]) + 1
            row["commence_times"].add(str(event.get("commence_time") or ""))
            row["away_teams"].add(str(event.get("away_team") or ""))
            row["home_teams"].add(str(event.get("home_team") or ""))
            row["source_paths"].add(str(path.relative_to(ROOT)))
            row["source_hashes"].add(source_hash)
            books = event.get("bookmakers") or []
            if any(str(book.get("key") or "").lower() == "betonlineag" for book in books if isinstance(book, dict)):
                row["betonline"] = True
    output: list[dict[str, object]] = []
    for event_id, row in sorted(population.items()):
        output.append({
            "lane": "BETONLINE_PLAYER_PROPS" if row["betonline"] else "GENERAL_PLAYER_PROP_SNAPSHOT",
            "provider": "THE_ODDS_API",
            "provider_event_id": event_id,
            "exact_game_pk": "",
            "mapping_classification": "CONDITIONAL_IDENTITY_NOT_PROVEN",
            "mapping_status": "DURABLE_PROVIDER_EVENT_TO_GAME_PK_BINDING_ABSENT",
            "observation_count": row["observations"],
            "provider_commence_times": "|".join(sorted(row["commence_times"])),
            "provider_away_teams": "|".join(sorted(row["away_teams"])),
            "provider_home_teams": "|".join(sorted(row["home_teams"])),
            "candidate_game_pks": "",
            "source_file_count": len(row["source_paths"]),
            "source_sha256_count": len(row["source_hashes"]),
            "notes": "Raw event is retained; build_mlb_predictions_wide drops provider event_id after a team/start choice and does not bind the schedule-response hash.",
        })
    stats = {
        "files": len(files),
        "unique_bytes": len(unique_bytes),
        "events": len(output),
        "betonline_events": sum(row["lane"] == "BETONLINE_PLAYER_PROPS" for row in output),
    }
    return output, stats


def pinnacle_events() -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, int]]:
    state: dict[str, dict[str, object]] = {}
    files = sorted(ROOT.glob("backend/mlb/exports/market_history/full_game_totals/2026-*/**/pinnacle_identity_audit.json"))
    observations = 0
    for path in files:
        rows = json.loads(path.read_text(encoding="utf-8"))
        for item in rows:
            observations += 1
            event_id = str(item.get("provider_event_id") or "").strip()
            if not event_id:
                continue
            row = state.setdefault(event_id, {
                "game_pks": set(), "statuses": Counter(), "candidates": set(),
                "away": set(), "home": set(), "starts": set(), "paths": set(),
            })
            if item.get("game_pk") is not None:
                row["game_pks"].add(int(item["game_pk"]))
            row["statuses"][str(item.get("certification_status") or "UNKNOWN")] += 1
            row["candidates"].update(int(value) for value in (item.get("candidate_game_pks") or []))
            row["away"].add(str(item.get("away_team") or ""))
            row["home"].add(str(item.get("home_team") or ""))
            row["starts"].add(str(item.get("scheduled_start_utc") or ""))
            row["paths"].add(str(path.relative_to(ROOT)))
    output: list[dict[str, object]] = []
    violations: list[dict[str, object]] = []
    for event_id, row in sorted(state.items()):
        game_pks = sorted(row["game_pks"])
        statuses = row["statuses"]
        conflict = len(game_pks) > 1
        has_mapping = len(game_pks) == 1
        output.append({
            "lane": "PINNACLE_MAIN_MARKETS",
            "provider": "THE_ODDS_API",
            "provider_event_id": event_id,
            "exact_game_pk": game_pks[0] if has_mapping else "",
            "mapping_classification": "CONDITIONAL_IDENTITY_NOT_PROVEN" if has_mapping else "BLOCKED_MISSING_OR_AMBIGUOUS_IDENTITY",
            "mapping_status": "EXACT_BINDING_RETAINED_SCHEDULE_SOURCE_HASH_UNBOUND" if has_mapping else "CONFLICTING_OR_MISSING_EXACT_BINDING",
            "observation_count": sum(statuses.values()),
            "provider_commence_times": "|".join(sorted(row["starts"])),
            "provider_away_teams": "|".join(sorted(row["away"])),
            "provider_home_teams": "|".join(sorted(row["home"])),
            "candidate_game_pks": "|".join(map(str, sorted(row["candidates"]))),
            "source_file_count": len(row["paths"]),
            "source_sha256_count": len({sha256(ROOT / path) for path in row["paths"]}),
            "notes": "statuses=" + "|".join(f"{key}:{statuses[key]}" for key in sorted(statuses)),
        })
        if conflict or not has_mapping:
            violations.append({
                "lane": "PINNACLE_MAIN_MARKETS",
                "provider_event_id": event_id,
                "exact_game_pk": "|".join(map(str, game_pks)),
                "reason_code": "CONFLICTING_EXACT_GAME_PK" if conflict else "MISSING_EXACT_GAME_PK",
                "classification": "BLOCKED_MISSING_OR_AMBIGUOUS_IDENTITY",
                "evidence": "|".join(f"{key}:{statuses[key]}" for key in sorted(statuses)),
            })
    stats = {
        "audit_files": len(files),
        "observations": observations,
        "events": len(output),
        "mapped_events": sum(bool(row["exact_game_pk"]) for row in output),
        "conflict_or_missing_events": len(violations),
        "events_with_noncertified_observation": sum(
            any(key in row["notes"] for key in ("GAME_NOT_FOUND", "AMBIGUOUS", "TIMING_UNRESOLVED"))
            for row in output
        ),
    }
    return output, violations, stats


def main() -> None:
    provider_fields = [
        "lane", "provider", "provider_event_id", "exact_game_pk", "mapping_classification",
        "mapping_status", "observation_count", "provider_commence_times", "provider_away_teams",
        "provider_home_teams", "candidate_game_pks", "source_file_count", "source_sha256_count", "notes",
    ]
    player_rows, player_stats = player_prop_events()
    pinnacle_rows, pinnacle_violations, pinnacle_stats = pinnacle_events()
    write_csv("provider_event_to_game_pk_audit.csv", pinnacle_rows + player_rows, provider_fields)
    missing = list(pinnacle_violations)
    for row in player_rows:
        missing.append({
            "lane": row["lane"],
            "provider_event_id": row["provider_event_id"],
            "exact_game_pk": "",
            "reason_code": "DURABLE_PROVIDER_EVENT_TO_GAME_PK_BINDING_ABSENT",
            "classification": "CONDITIONAL_IDENTITY_NOT_PROVEN",
            "evidence": f"observations={row['observation_count']};source_files={row['source_file_count']}",
        })
    write_csv(
        "ambiguous_missing_identity_ledger.csv", missing,
        ["lane", "provider_event_id", "exact_game_pk", "reason_code", "classification", "evidence"],
    )

    source_paths = [
        "Makefile",
        "bin/mlb_prod12_cron_cycle.sh",
        "bin/mlb_full_game_totals_daily_hook.sh",
        "bin/mlb_bvp_inline_daily_hook.sh",
        "backend/app/services/mlb/market_odds_service.py",
        "backend/mlb/scripts/capture_mlb_governed_pregame_lineups.py",
        "backend/mlb/scripts/player_stats_game_completeness.py",
        "backend/mlb/scripts/refresh_mlb_players_rosters.py",
        "backend/mlb/scripts/run_mlb_bvp_inline_daily.py",
        "backend/mlb/scripts/refresh_mlb_bvp_pvb.py",
        "backend/mlb/shared/bvp_inline.py",
        "backend/mlb/shared/bvp_identity.py",
        "backend/mlb/shared/mlb_api_v2.py",
        "backend/mlb/scripts/build_mlb_predictions_wide.py",
        "backend/mlb/shared/prospective_lineage.py",
        "backend/mlb/scripts/attach_mlb_hits05_full_board_markets_v1.py",
        "backend/mlb/scripts/capture_mlb_pinnacle_main_markets_v1.py",
        "backend/mlb/markets/pinnacle_main_market_capture_v1.py",
        "backend/mlb/markets/full_game_total_capture_v1.py",
        "backend/mlb/totals_predictions/live_context_bridge_v1.py",
        "backend/mlb/identity/canonical_game_identity.py",
        "backend/mlb/season_transition/game_phase_authority_v1.py",
        "docs/contracts/mlb_2026_full_board_hits_phase_gating_v1/consumer_manifest.csv",
        "docs/contracts/mlb_2026_totals_phase_gating_v1/consumer_manifest.csv",
    ]
    source_rows = []
    for relative in source_paths:
        path = ROOT / relative
        source_rows.append({
            "path": relative,
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
            "role": "reviewed_source_or_governed_dependency",
        })
    write_csv("source_sha256_manifest.csv", source_rows, ["path", "bytes", "sha256", "role"])

    summary = {
        "audit": "MLB_2026_POSTSEASON_COLLECTION_IDENTITY_READINESS_AUDIT_V1",
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "head_at_extraction": HEAD_AT_EXTRACTION,
        "head_at_final_validation": HEAD_AT_FINAL_VALIDATION,
        "network_requests": 0,
        "paid_requests": 0,
        "paid_credits": 0,
        "operational_database_connections": 0,
        "player_prop_population": player_stats,
        "pinnacle_population": pinnacle_stats,
        "feature_lineage": {
            "files": 51,
            "rows": 126962,
            "unique_canonical_row_identities": 126962,
            "distinct_game_pks": 674,
            "rows_missing_feature_or_odds_snapshot_hash": 0,
        },
        "bvp_inline": {
            "attempts": 4,
            "results": 4,
            "successes": 4,
            "success_dates": 4,
            "duplicate_success_dates": 0,
            "latest_date": "2026-09-22",
            "later_same_date_status": "BVP_INLINE_SUCCESS_ALREADY_EXISTS",
            "identity_journal_files": 4,
            "identity_journal_rows": 1426,
            "distinct_game_pks": 49,
        },
        "phase_authority": {
            "game_pks": 2919,
            "regular_season": 2430,
            "preseason": 489,
            "postseason": 0,
            "supported_through_date": "2026-09-27",
            "authority_records_sha256": "5a7cdc460cc42ca2b4ed328c74e978b9da6967f95d7a7c4ba888b3b8d3401a84",
        },
        "synthetic_fixture_scope": "identity logic only; not operational postseason evidence",
    }
    (OUT / "audit_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    package_rows = []
    for path in sorted(OUT.iterdir(), key=lambda item: item.name):
        if not path.is_file() or path.name in {"sha256_manifest.csv", "validation_report.json"}:
            continue
        package_rows.append({"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    write_csv("sha256_manifest.csv", package_rows, ["path", "bytes", "sha256"])


if __name__ == "__main__":
    main()
