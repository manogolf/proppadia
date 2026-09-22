#!/usr/bin/env python3
"""Operator CLI for governed NHL postgame reconciliation.

Preflight is database-read-only. Execute uses official NHL public sources plus the
existing postgame collectors; it never imports or calls a bookmaker client.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
import requests

from backend.nhl.official_request_journal import (
    ENV_CACHE,
    ENV_GAME_HASH,
    ENV_GAME_IDS,
    ENV_JOURNAL,
    ENV_REQUIRED,
    ENV_RESPONSE_SOURCE_LEDGER,
    ENV_RUN_ID,
    ENV_SLATE,
    ENV_SOURCE_CACHE,
    ENV_SOURCE_JOURNAL_SHA256,
    ENV_SOURCE_RUN_ID,
    RequestContext,
    canonical_game_set_hash,
    build_typed_response_source_ledger,
    official_get,
    read_journal,
    request_run_tree_fingerprint,
    sha256_bytes,
    summarize_journal,
    verify_payload_identity,
    verify_failed_execution_ancestor,
    verify_failed_request_ancestor,
    verify_preserved_response_run,
    verify_player_identity_response_run,
    verify_roster_response_run,
)
from backend.nhl.postgame_reconcile.core import (
    build_outcomes,
    reconciliation_lock,
    publish_reconciliation,
    resolve_operational_sources,
    validate_staging_identity_sets,
    validate_final_slate,
)
from backend.nhl.player_external_identity import (
    authoritative_player_name,
    create_or_verify_player,
    is_authoritative_local_name,
    localized_text,
    resolve_player_external_identity,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
DEFAULT_PREDICTION_ROOT = ROOT / "artifacts/operational/nhl/preseason_catchup"
DEFAULT_OPERATIONAL_ROOT = ROOT / "artifacts/operational/nhl"
SCRIPTS = ROOT / "backend/nhl/scripts"
SQL = ROOT / "backend/nhl/sql"
FINAL_STATES = {"FINAL", "OFF"}

REQUEST_RUN_RECEIPTS = {
    "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6": {
        "roles": ["AUTHORITY_RESPONSE_SOURCE", "FAILED_EXECUTION_ANCESTOR"],
        "slate_date": "2026-09-20",
        "journal_sha256": "2994ef2bd162c483eacc20b02cf83cd0938fb5f79fd76d208ce57981ed473cb7",
        "tree_fingerprint": "c6e6402b2eba12a8efb30a60df607324cf8684361d357fb5340cfc9051fa7110",
    },
    "nhlpostgame_20260920_20260922T161724179739Z_cef0bc8b": {
        "roles": ["FAILED_EXECUTION_ANCESTOR"],
        "slate_date": "2026-09-20",
        "journal_sha256": "30625a8088ea6837e2293b9339a8b38787a044ba7d0b7033ab54965ec695459c",
        "tree_fingerprint": "fe6a4cb817f3e291447b187af68749eee6a733a854886cb6420a90303a019b84",
    },
    "nhlpostgame_20260920_20260922T171356619916Z_cd2ac1d9": {
        "roles": ["ROSTER_RESPONSE_SOURCE", "FAILED_EXECUTION_ANCESTOR"],
        "slate_date": "2026-09-20",
        "journal_sha256": "39534187c6f712a248a48e2c16873d23a93216bee1b12ae00f217bed6af67c72",
        "tree_fingerprint": "814a658ecf719d217cdfb568d75c060a28c0697e5d4c6ead2f7ce66b56215444",
        "response_set_sha256": "905a42a0e09ab83b3f196d55b94056b8813454545cf0589b9305bc91bad7428f",
        "teams": ["ANA", "BOS", "CAR", "CGY", "COL", "FLA", "NJD",
                  "NSH", "NYI", "SEA", "SJS", "TBL", "UTA", "WSH"],
    },
    "nhlpostgame_20260920_20260922T181726727181Z_70f0280d": {
        "roles": ["PLAYER_IDENTITY_RESPONSE_SOURCE", "FAILED_EXECUTION_ANCESTOR"],
        "slate_date": "2026-09-20",
        "journal_sha256": "cf32772ebd2c5116e05f9ae12ac8da821c1277e16f8432aa27e8c37bd89d1c0e",
        "tree_fingerprint": "31c676ce2e3cc606d7e469be42d899fc6961735889b99bdc90145058bacdaca0",
        "player_id": 8485386,
        "object_sha256": "69733de66231bda93e5deb6c5d013aa3ce6a146835c1ae301ffdc2be477fb7b4",
        "index_sha256": "797f6100afd0c418f366c914e464ecb311768923f3b80fab48f18d55d33aa99f",
    },
}


def _receipt(run_id: str, *, role: str, slate_date: str) -> dict[str, object]:
    receipt = REQUEST_RUN_RECEIPTS.get(run_id)
    if receipt is None:
        raise RuntimeError(f"REQUEST_RUN_RECEIPT_NOT_ALLOWLISTED:{run_id}")
    if role not in receipt["roles"] or receipt["slate_date"] != slate_date:
        raise RuntimeError(f"REQUEST_RUN_RECEIPT_ROLE_OR_DATE_MISMATCH:{run_id}")
    return receipt


def _public_response_source(binding: dict[str, object]) -> dict[str, object]:
    public = {key: binding[key] for key in (
        "contract_version", "role", "source_run_id", "source_journal_sha256",
        "tree_fingerprint", "canonical_game_set_hash",
        "response_set_sha256",
    )}
    public["response_count"] = len(binding["responses"])
    public["responses"] = binding["responses"]
    return public


def _parse_response_source_specs(values: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        role, separator, run_id = value.partition("=")
        if (not separator or role not in {"AUTHORITY_RESPONSE_SOURCE", "ROSTER_RESPONSE_SOURCE",
                                          "PLAYER_IDENTITY_RESPONSE_SOURCE"}
                or not run_id):
            raise RuntimeError(f"TYPED_RESPONSE_SOURCE_ARGUMENT_INVALID:{value}")
        if role in parsed:
            raise RuntimeError(f"TYPED_RESPONSE_SOURCE_ROLE_DUPLICATE:{role}")
        parsed[role] = run_id
    return parsed


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def canonical_slate(dsn: str, slate_date: str) -> pd.DataFrame:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("BEGIN READ ONLY")
        cursor.execute("""
            SELECT season AS canonical_season, game_date::text AS slate_date, game_id,
                   start_time_utc AS scheduled_start_time_utc, home_team_id, away_team_id,
                   home_team_code, away_team_code, game_type AS game_type_code,
                   upper(coalesce(status, 'SCHEDULED')) AS retained_game_state
            FROM nhl.games
            WHERE season=2026 AND game_date=%s::date
            ORDER BY start_time_utc, game_id
        """, (slate_date,))
        rows = cursor.fetchall()
        columns = [item.name for item in cursor.description]
        connection.rollback()
    return pd.DataFrame(rows, columns=columns)


def read_only_preflight(dsn: str, slate_date: str) -> tuple[dict[str, object], int]:
    slate = canonical_slate(dsn, slate_date)
    states = {str(key): int(value) for key, value in slate.retained_game_state.value_counts().items()} if len(slate) else {}
    complete = bool(len(slate)) and bool(slate.retained_game_state.isin(FINAL_STATES).all())
    payload = {
        "contract_version": "NHL_POSTGAME_RECONCILIATION_V1",
        "mode": "PREFLIGHT_READ_ONLY",
        "slate_date": slate_date,
        "canonical_games": len(slate),
        "game_ids": slate.game_id.astype(int).tolist() if len(slate) else [],
        "retained_state_counts": states,
        "all_retained_games_final": complete,
        "official_final_authority_refresh_performed": False,
        "readiness": "READY_FOR_EXECUTE" if complete else "NOT_READY_UNFINISHED_OR_UNAVAILABLE",
        "database_writes": 0,
        "external_requests": 0,
        "bookmaker_requests": 0,
        "paid_credits": 0,
    }
    return payload, 0 if complete else 2


def official_games_for_slate(payload: dict, slate_date: str) -> pd.DataFrame:
    """Flatten only the requested schedule day.

    NHL's schedule response assigns the authoritative date to each ``gameWeek``
    parent.  Child games commonly omit ``gameDate``; a child value must not move
    a game into or out of its authoritative parent day.  The legacy top-level
    ``games`` shape has no parent, so it requires an exact child date.
    """
    rows: list[dict[str, object]] = []
    for day in payload.get("gameWeek", []) or []:
        if str(day.get("date") or "") != slate_date:
            continue
        for item in day.get("games") or []:
            rows.append(item)
    for item in payload.get("games", []) or []:
        if str(item.get("gameDate") or "") == slate_date:
            rows.append(item)

    games = []
    for item in rows:
        raw_id = item.get("id") or item.get("gamePk") or item.get("gameId")
        if raw_id is None:
            raise RuntimeError("OFFICIAL_GAME_IDENTITY_MISSING")
        games.append({
            "game_id": int(raw_id),
            "game_state": str(item.get("gameState") or item.get("gameScheduleState") or "").upper(),
            "home_team_id": int((item.get("homeTeam") or {}).get("id")),
            "away_team_id": int((item.get("awayTeam") or {}).get("id")),
            "home_score": (item.get("homeTeam") or {}).get("score"),
            "away_score": (item.get("awayTeam") or {}).get("score"),
        })
    official = pd.DataFrame(games, columns=[
        "game_id", "game_state", "home_team_id", "away_team_id", "home_score", "away_score",
    ])
    if official.game_id.duplicated().any():
        duplicate_ids = sorted(official.loc[official.game_id.duplicated(False), "game_id"].astype(int).unique())
        raise RuntimeError(f"OFFICIAL_DUPLICATE_GAME_IDENTITY:{duplicate_ids}")
    return official


def local_conditional_lookup_inventory(authority: dict[str, object],
                                       roster: dict[str, object]) -> dict[str, object]:
    participants: dict[int, dict[str, object]] = {}
    authority_cache = Path(str(authority["source_cache"]))
    authority_root = authority_cache.parent
    journal_path = authority_root / "official_request_journal.jsonl"
    boxscore_claims = [claim for claim in authority["responses"]
                       if claim["endpoint_family"] == "BOXSCORE"]
    if (canonical_game_set_hash(
            claim["resource_identity"]["game_id"] for claim in boxscore_claims)
            != authority["canonical_game_set_hash"]
            or any(str(claim["resource_identity"].get("slate_date")) != "2026-09-20"
                   for claim in boxscore_claims)):
        raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_GAME_SET_MISMATCH")
    if sha256_bytes(journal_path.read_bytes()) != authority["source_journal_sha256"]:
        raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_JOURNAL_CHANGED")
    if request_run_tree_fingerprint(authority_root, repository_root=ROOT) != authority["tree_fingerprint"]:
        raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_TREE_CHANGED")
    journal_records = read_journal(journal_path)
    for claim in authority["responses"]:
        if claim["endpoint_family"] != "BOXSCORE":
            continue
        identity = claim["resource_identity"]
        index_token = sha256_bytes(json.dumps({
            "endpoint_family": "BOXSCORE", "identity": identity,
        }, sort_keys=True, separators=(",", ":")).encode())
        index_path = authority_cache / "index" / f"{index_token}.json"
        if not index_path.is_file() or sha256_bytes(index_path.read_bytes()) != claim["index_sha256"]:
            raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_INDEX_CHANGED")
        index = json.loads(index_path.read_text())
        if (index.get("endpoint_family") != "BOXSCORE" or index.get("identity") != identity
                or index.get("response_sha256") != claim["object_sha256"]
                or index.get("response_bytes") != claim["response_bytes"]
                or index.get("object_name") != f"{claim['object_sha256']}.json"):
            raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_INDEX_IDENTITY_MISMATCH")
        object_path = authority_cache / "objects" / f"{claim['object_sha256']}.json"
        if not object_path.is_file():
            raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_OBJECT_MISSING")
        body = object_path.read_bytes()
        if (sha256_bytes(body) != claim["object_sha256"]
                or len(body) != claim["response_bytes"]):
            raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_OBJECT_CHANGED")
        verify_payload_identity("BOXSCORE", identity, body)
        journal_row = next((row for row in journal_records
                            if row.get("endpoint_family") == "BOXSCORE"
                            and row.get("resource_identity") == identity), None)
        if (journal_row is None or journal_row.get("response_sha256") != claim["object_sha256"]
                or journal_row.get("response_bytes") != claim["response_bytes"]
                or journal_row.get("canonical_game_set_hash") != authority["canonical_game_set_hash"]
                or journal_row.get("run_id") != authority["source_run_id"]):
            raise RuntimeError("NUMERIC_IDENTITY_AUTHORITY_JOURNAL_IDENTITY_MISMATCH")
        payload = json.loads(body)
        for side in ("homeTeam", "awayTeam"):
            team = (payload.get("playerByGameStats") or {}).get(side) or {}
            for section in ("forwards", "defense", "defensemen", "goalies"):
                for row_index, row in enumerate(team.get(section) or []):
                    raw = row.get("playerId") or row.get("id")
                    if raw is not None:
                        player_id = int(raw)
                        role = "goalie" if section == "goalies" else "skater"
                        entry = participants.setdefault(
                            player_id, {"roles": set(), "team_ids": set(), "evidence": []})
                        entry["roles"].add(role)
                        team_id = (payload.get(side) or {}).get("id")
                        if team_id is not None:
                            entry["team_ids"].add(int(team_id))
                        entry["evidence"].append({
                            "player_id": player_id,
                            "game_id": int(identity["game_id"]),
                            "authority_source_run": authority["source_run_id"],
                            "response_object_sha256": claim["object_sha256"],
                            "response_index_sha256": claim["index_sha256"],
                            "source_journal_sha256": authority["source_journal_sha256"],
                            "request_run_tree_fingerprint": authority["tree_fingerprint"],
                            "canonical_game_set_hash": authority["canonical_game_set_hash"],
                            "json_identity_locator":
                                f"$.playerByGameStats.{side}.{section}[{row_index}].playerId",
                        })
    roster_ids: set[int] = set()
    named = 0
    for claim in roster["responses"]:
        body = (Path(str(roster["source_cache"])) / "objects" /
                f"{claim['object_sha256']}.json").read_bytes()
        payload = json.loads(body)
        for section in ("forwards", "defense", "defensemen", "goalies"):
            for row in payload.get(section) or []:
                raw = row.get("id") or row.get("playerId") or (row.get("player") or {}).get("id")
                if raw is None:
                    continue
                roster_ids.add(int(raw))
                if localized_text(row.get("firstName")) and localized_text(row.get("lastName")):
                    named += 1
    if len(roster_ids) != 548 or named != 548:
        raise RuntimeError(f"SEPTEMBER_20_ROSTER_NAME_INVENTORY_MISMATCH:{len(roster_ids)}:{named}")
    participant_ids = set(participants)
    skaters = sorted(player_id for player_id, value in participants.items()
                     if value["roles"] == {"skater"})
    goalies = sorted(player_id for player_id, value in participants.items()
                     if value["roles"] == {"goalie"})
    ambiguous = sorted(player_id for player_id, value in participants.items()
                       if len(value["roles"]) != 1)
    absent_from_rosters = participant_ids - roster_ids
    if ambiguous:
        raise RuntimeError(f"PARTICIPANT_ROLE_CONFLICT:{ambiguous}")
    if len(participant_ids) != 280 or len(skaters) != 252 or len(goalies) != 28:
        raise RuntimeError(
            f"SEPTEMBER_20_PARTICIPANT_INVENTORY_MISMATCH:"
            f"{len(participant_ids)}:{len(skaters)}:{len(goalies)}")
    if len(absent_from_rosters) != 54:
        raise RuntimeError(
            f"SEPTEMBER_20_ROSTER_ABSENCE_INVENTORY_MISMATCH:{len(absent_from_rosters)}")
    return {
        "contract_version": "NHL_LOCAL_PARTICIPANT_INVENTORY_V2",
        "participating_nhl_ids": len(participant_ids), "verified_roster_ids": len(roster_ids),
        "localized_roster_names_retained": named,
        "absent_from_verified_rosters": len(absent_from_rosters),
        "participant_ids": sorted(participant_ids), "skater_ids": skaters,
        "goalie_ids": goalies, "roster_absent_ids": sorted(absent_from_rosters),
        "participant_team_ids": {
            str(player_id): sorted(value["team_ids"])
            for player_id, value in sorted(participants.items())
        },
        "official_numeric_identity_evidence": {
            str(player_id): sorted(value["evidence"], key=lambda row: (
                row["game_id"], row["json_identity_locator"]))
            for player_id, value in sorted(participants.items())
        },
        "database_mapping_coverage": "UNVERIFIED",
        "status": "IMMUTABLE_LOCAL_EVIDENCE_VALID_DATABASE_COVERAGE_UNVERIFIED",
    }


def _identity_partition_digest(payload: dict[str, object]) -> str:
    normalized = {key: value for key, value in payload.items()
                  if key not in {"partition_sha256", "transaction"}}
    return hashlib.sha256(json.dumps(
        normalized, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def classify_database_identity_rows(
    *, inventory: dict[str, object], player_rows: list[dict[str, object]],
    external_rows: list[dict[str, object]], preserved_player_ids: set[int],
    authorized_lookup_ids: set[int], slate_date: str,
) -> dict[str, object]:
    participant_ids = {int(value) for value in inventory["participant_ids"]}
    goalie_ids = {int(value) for value in inventory["goalie_ids"]}
    player_by_id = {int(row["player_id"]): row for row in player_rows}
    by_internal: dict[int, list[dict[str, object]]] = {}
    by_external: dict[int, list[dict[str, object]]] = {}
    for row in external_rows:
        if str(row.get("provider")) != "nhl":
            continue
        internal = int(row["player_id"])
        try:
            external = int(str(row["provider_player_id"]))
        except ValueError:
            continue
        by_internal.setdefault(internal, []).append(row)
        by_external.setdefault(external, []).append(row)

    categories = {name: [] for name in (
        "exact_mapping", "deterministic_same_number_bind",
        "numeric_identity_proven_bind",
        "preserved_response_resolution", "new_official_lookup", "conflict")}
    official_numeric_evidence = {
        int(player_id): list(evidence)
        for player_id, evidence in
        dict(inventory.get("official_numeric_identity_evidence") or {}).items()
    }
    numeric_provenance: list[dict[str, object]] = []
    records: list[dict[str, object]] = []
    for player_id in sorted(participant_ids):
        internal_rows = by_internal.get(player_id, [])
        external_rows_for_id = by_external.get(player_id, [])
        exact = [row for row in external_rows_for_id
                 if int(row["player_id"]) == player_id]
        disagreements = [row for row in internal_rows
                         if str(row["provider_player_id"]) != str(player_id)]
        disagreements.extend(row for row in external_rows_for_id
                             if int(row["player_id"]) != player_id)
        player = player_by_id.get(player_id)
        first_name = str((player or {}).get("first_name") or "").strip()
        last_name = str((player or {}).get("last_name") or "").strip()
        component_name = f"{first_name} {last_name}".strip()
        full_name = str((player or {}).get("full_name") or "").strip()
        authoritative_name_value = (
            component_name if first_name and last_name
            and is_authoritative_local_name(component_name)
            else full_name if is_authoritative_local_name(full_name) else "")
        authoritative_name = bool(authoritative_name_value)
        if disagreements or len(exact) > 1:
            category = "conflict"
        elif len(exact) == 1:
            category = "exact_mapping"
        elif player is not None and authoritative_name:
            category = "deterministic_same_number_bind"
        elif player is not None and official_numeric_evidence.get(player_id):
            evidence = official_numeric_evidence[player_id]
            required_evidence_fields = {
                "player_id", "game_id", "authority_source_run",
                "response_object_sha256", "response_index_sha256",
                "source_journal_sha256", "request_run_tree_fingerprint",
                "canonical_game_set_hash", "json_identity_locator",
            }
            if any(not required_evidence_fields.issubset(set(row))
                   or int(row["player_id"]) != player_id for row in evidence):
                category = "conflict"
            else:
                category = "numeric_identity_proven_bind"
                numeric_provenance.append({
                    "player_id": player_id,
                    "evidence": evidence,
                    "mapping": {
                        "internal_player_id": player_id,
                        "provider": "nhl",
                        "provider_external_id": str(player_id),
                    },
                })
        elif player_id in preserved_player_ids:
            category = "preserved_response_resolution"
        else:
            category = "new_official_lookup"
        categories[category].append(player_id)
        roles = (["goalie"] if player_id in goalie_ids
                 else ["skater"])
        records.append({
            "nhl_id": player_id, "roles": roles, "classification": category,
            "player_row_exists": player is not None,
            "authoritative_local_name": authoritative_name,
            "authoritative_name_source": (
                "FIRST_LAST_COLUMNS" if authoritative_name_value == component_name
                and bool(component_name) else "FULL_NAME_COLUMN"
                if authoritative_name_value else None),
            "internal_player_id": player_id if player is not None else None,
            "exact_external_mapping": len(exact) == 1,
        })
    actual_lookups = set(categories["new_official_lookup"])

    roster_absent = {int(value) for value in inventory["roster_absent_ids"]}
    absent_partition = {
        name: sorted(roster_absent.intersection(values))
        for name, values in categories.items()
    }
    absent_counts = {name: len(values) for name, values in absent_partition.items()}
    goalie_partition = {
        name: sorted(goalie_ids.intersection(values))
        for name, values in categories.items()
    }
    goalie_preparation = {
        name: values for name, values in goalie_partition.items()
        if name != "exact_mapping"
    }
    flattened = [player_id for values in categories.values() for player_id in values]
    partition_complete = (len(flattened) == len(participant_ids)
                          and len(set(flattened)) == len(participant_ids)
                          and set(flattened) == participant_ids)
    roster_absent_complete = (
        sum(absent_counts.values()) == len(roster_absent)
        and set(player_id for values in absent_partition.values()
                for player_id in values) == roster_absent)
    failure = None
    if categories["conflict"]:
        failure = f"DATABASE_IDENTITY_CONFLICT:{categories['conflict']}"
    elif not partition_complete or not roster_absent_complete:
        failure = "DATABASE_IDENTITY_PARTITION_NOT_MUTUALLY_EXCLUSIVE_COMPLETE"
    elif categories["preserved_response_resolution"] != [8485386]:
        failure = (
            "SEPTEMBER_20_PRESERVED_PLAYER_PARTITION_MISMATCH:"
            f"{categories['preserved_response_resolution']}")
    elif actual_lookups != authorized_lookup_ids:
        failure = (
            "AUTHORIZED_PLAYER_LOOKUP_SET_MISMATCH:"
            f"actual={sorted(actual_lookups)}:authorized={sorted(authorized_lookup_ids)}")
    if slate_date == "2026-09-20":
        allowed_goalie_resolution = (
            set(goalie_partition["exact_mapping"])
            | set(goalie_partition["deterministic_same_number_bind"])
            | set(goalie_partition["numeric_identity_proven_bind"])
        )
        if failure is None and (
                sum(len(values) for values in goalie_partition.values()) != 28
                or goalie_partition["new_official_lookup"] != [8485525]
                or goalie_partition["preserved_response_resolution"]
                or goalie_partition["conflict"]
                or allowed_goalie_resolution != goalie_ids - {8485525}):
            failure = f"SEPTEMBER_20_GOALIE_IDENTITY_PARTITION_MISMATCH:{goalie_preparation}"
    if (failure is None and 8482103 in participant_ids
            and 8482103 not in categories["exact_mapping"]):
        failure = "PROTECTED_PLAYER_8482103_MAPPING_NOT_EXACT"

    payload: dict[str, object] = {
        "contract_version": "NHL_DATABASE_IDENTITY_PREFLIGHT_V1",
        "status": ("DATABASE_IDENTITY_PREFLIGHT_VALID" if failure is None
                   else "FAILED_CLOSED_DATABASE_PREFLIGHT"),
        "slate_date": slate_date, "participant_count": len(participant_ids),
        "classification": categories, "classification_records": records,
        "partition_complete_and_mutually_exclusive": partition_complete,
        "roster_absent_partition_complete": roster_absent_complete,
        "roster_absent_partition": absent_partition,
        "roster_absent_classification_counts": absent_counts,
        "goalie_partition": goalie_partition,
        "goalie_partition_coverage": sum(len(values) for values in goalie_partition.values()),
        "goalie_identity_preparation": goalie_preparation,
        "numeric_identity_provenance": numeric_provenance,
        "numeric_identity_provenance_sha256": sha256_bytes(json.dumps(
            numeric_provenance, sort_keys=True, separators=(",", ":")).encode()),
        "authorized_new_official_lookup_ids": sorted(authorized_lookup_ids),
        "lookup_authorization_matches": actual_lookups == authorized_lookup_ids,
        "observed_identity_evidence": {
            str(player_id): next(row for row in records if row["nhl_id"] == player_id)
            for player_id in (8477467, 8478477, 8483010)
        },
        "database_writes": 0, "official_network_requests": 0,
    }
    if failure is not None:
        payload["failure"] = failure
    payload["partition_sha256"] = _identity_partition_digest(payload)
    return payload


def database_identity_preflight(
    dsn: str, *, inventory: dict[str, object], preserved_player_ids: set[int],
    authorized_lookup_ids: set[int], slate_date: str,
) -> dict[str, object]:
    """Inspect live mappings once, read-only, and always explicitly roll back."""
    participant_ids = sorted({int(value) for value in inventory["participant_ids"]})
    connection = psycopg.connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN TRANSACTION READ ONLY")
            cursor.execute("SHOW transaction_read_only")
            if str(cursor.fetchone()[0]).lower() != "on":
                raise RuntimeError("DATABASE_IDENTITY_PREFLIGHT_NOT_READ_ONLY")
            cursor.execute(
                """
                SELECT player_id, full_name, first_name, last_name, team_id, position
                FROM nhl.players
                WHERE player_id = ANY(%s)
                ORDER BY player_id
                """, (participant_ids,))
            columns = [item.name for item in cursor.description]
            player_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.execute(
                """
                SELECT player_id, provider, provider_player_id
                FROM nhl.player_external_ids
                WHERE provider = 'nhl'
                  AND (player_id = ANY(%s)
                       OR (provider_player_id ~ '^[0-9]+$'
                           AND provider_player_id::bigint = ANY(%s)))
                ORDER BY player_id, provider_player_id
                """, (participant_ids, participant_ids))
            columns = [item.name for item in cursor.description]
            external_rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            result = classify_database_identity_rows(
                inventory=inventory, player_rows=player_rows,
                external_rows=external_rows, preserved_player_ids=preserved_player_ids,
                authorized_lookup_ids=authorized_lookup_ids, slate_date=slate_date)
        connection.rollback()
        result["transaction"] = {
            "read_only": True, "transactions": 1, "read_queries": 2,
            "writes": 0, "explicitly_rolled_back": True, "closed": True,
        }
        return result
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def fetch_official(slate_date: str, canonical_game_ids: set[int], *, reuse_authority: bool = False) -> tuple[pd.DataFrame, dict[int, dict], dict[str, int]]:
    expected_ids = {int(value) for value in canonical_game_ids}
    response = official_get(
        f"https://api-web.nhle.com/v1/schedule/{slate_date}", timeout=30,
        stage="POSTGAME_AUTHORITY", endpoint_family="SCHEDULE",
        identity={"slate_date": slate_date}, authority_boundary=True,
        preserve_response=not reuse_authority, reuse_preserved=reuse_authority,
    )
    response.raise_for_status()
    official = official_games_for_slate(response.json(), slate_date)
    actual_ids = set(official.game_id.astype(int))
    if actual_ids != expected_ids:
        missing = sorted(expected_ids - actual_ids)
        unexpected = sorted(actual_ids - expected_ids)
        raise RuntimeError(
            "OFFICIAL_GAME_SET_MISMATCH_BEFORE_BOXSCORE_REQUESTS:"
            f"expected={len(expected_ids)}:actual={len(actual_ids)}:"
            f"missing={missing}:unexpected={unexpected}"
        )
    boxes: dict[int, dict] = {}
    for gid in sorted(expected_ids):
        box = official_get(
            f"https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore", timeout=30,
            stage="POSTGAME_AUTHORITY", endpoint_family="BOXSCORE",
            identity={"slate_date": slate_date, "game_id": gid},
            authority_boundary=True, preserve_response=not reuse_authority,
            reuse_preserved=reuse_authority,
        )
        box.raise_for_status()
        boxes[gid] = box.json()
    requests_made = {
        "schedule_requests_expected": 1,
        "schedule_requests_actual": 1,
        "game_requests_expected": len(expected_ids),
        "game_requests_actual": len(boxes),
        "official_requests_expected": 1 + len(expected_ids),
        "official_requests_actual": 1 + len(boxes),
    }
    if reuse_authority:
        requests_made.update({"network_attempts": 0,
                              "preserved_response_reuses": 1 + len(boxes)})
    if requests_made["official_requests_actual"] != requests_made["official_requests_expected"]:
        raise RuntimeError(f"OFFICIAL_REQUEST_COUNT_MISMATCH:{requests_made}")
    return official, boxes, requests_made


def _run(command: list[str], slate_date: str, dsn: str) -> None:
    env = os.environ.copy()
    # Some retained collectors prefer DATABASE_URL while others prefer
    # SUPABASE_DB_URL.  Resolve both to the already validated DSN so a literal
    # shell-style alias from an env file cannot leak into a child process.
    env.update({"SLATE_DATE": slate_date, "SUPABASE_DB_URL": dsn, "DATABASE_URL": dsn})
    if env.get(ENV_REQUIRED) == "1":
        # Fail before spawning a network-capable child if the shared governed
        # context is absent, inconsistent, or has been partially overwritten.
        RequestContext.from_env(required=True)
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def _promote_stage(dsn: str, slate_date: str) -> None:
    """Idempotently promote existing governed skater/goalie stage tables."""
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("""
            WITH latest_roster AS (
              SELECT DISTINCT ON (game_id,player_id) game_id,player_id,team_id
              FROM nhl.roster_status
              WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date=%s::date)
              ORDER BY game_id,player_id,asof_ts DESC
            ), source AS (
              SELECT s.player_id,s.game_id,s.game_date,coalesce(s.team_id,r.team_id) AS team_id,
                     s.shots_on_goal,s.shot_attempts,s.toi_minutes,s.pp_toi_minutes,
                     s.blocks,s.hits,s.fenwick_for,s.missed_shots,s.blocked_shots_taken,
                     s.rebounds_for,s.takeaways,s.giveaways,s.penalties_drawn,s.penalties_taken,
                     s.ev_shot_attempts,s.pp_shot_attempts,s.sh_shot_attempts,
                     s.ev_sog,s.pp_sog,s.sh_sog,s.goals,s.assists
              FROM nhl.import_skater_logs_stage s
              LEFT JOIN latest_roster r USING(game_id,player_id)
              WHERE s.game_date=%s::date
            )
            INSERT INTO nhl.skater_game_logs_raw
              (player_id,game_id,team_id,opponent_id,is_home,game_date,shots_on_goal,shot_attempts,
               toi_minutes,pp_toi_minutes,blocks,hits,fenwick_for,missed_shots,blocked_shots_taken,
               rebounds_for,takeaways,giveaways,penalties_drawn,penalties_taken,
               ev_shot_attempts,pp_shot_attempts,sh_shot_attempts,ev_sog,pp_sog,sh_sog,goals,assists,points)
            SELECT s.player_id,s.game_id,s.team_id,
                   CASE WHEN s.team_id=g.home_team_id THEN g.away_team_id ELSE g.home_team_id END,
                   s.team_id=g.home_team_id,s.game_date,s.shots_on_goal,s.shot_attempts,s.toi_minutes,
                   nullif(s.pp_toi_minutes,0),s.blocks,s.hits,s.fenwick_for,s.missed_shots,
                   s.blocked_shots_taken,s.rebounds_for,s.takeaways,s.giveaways,s.penalties_drawn,
                   s.penalties_taken,s.ev_shot_attempts,s.pp_shot_attempts,s.sh_shot_attempts,
                   s.ev_sog,s.pp_sog,s.sh_sog,s.goals,s.assists,
                   CASE WHEN s.goals IS NOT NULL OR s.assists IS NOT NULL THEN coalesce(s.goals,0)+coalesce(s.assists,0) END
            FROM source s JOIN nhl.games g USING(game_id)
            WHERE s.team_id IN (g.home_team_id,g.away_team_id)
            ON CONFLICT (player_id,game_id) DO UPDATE SET
              team_id=excluded.team_id,opponent_id=excluded.opponent_id,is_home=excluded.is_home,
              game_date=excluded.game_date,shots_on_goal=excluded.shots_on_goal,
              shot_attempts=coalesce(excluded.shot_attempts,nhl.skater_game_logs_raw.shot_attempts),
              toi_minutes=excluded.toi_minutes,
              pp_toi_minutes=coalesce(excluded.pp_toi_minutes,nhl.skater_game_logs_raw.pp_toi_minutes),
              blocks=excluded.blocks,hits=excluded.hits,fenwick_for=excluded.fenwick_for,
              missed_shots=excluded.missed_shots,blocked_shots_taken=excluded.blocked_shots_taken,
              rebounds_for=excluded.rebounds_for,takeaways=excluded.takeaways,giveaways=excluded.giveaways,
              penalties_drawn=excluded.penalties_drawn,penalties_taken=excluded.penalties_taken,
              ev_shot_attempts=excluded.ev_shot_attempts,pp_shot_attempts=excluded.pp_shot_attempts,
              sh_shot_attempts=excluded.sh_shot_attempts,ev_sog=excluded.ev_sog,pp_sog=excluded.pp_sog,
              sh_sog=excluded.sh_sog,goals=excluded.goals,assists=excluded.assists,points=excluded.points
        """, (slate_date, slate_date))
        cursor.execute("""
            WITH latest_roster AS (
              SELECT DISTINCT ON (game_id,player_id) game_id,player_id,team_id
              FROM nhl.roster_status
              WHERE game_id IN (SELECT game_id FROM nhl.games WHERE game_date=%s::date)
              ORDER BY game_id,player_id,asof_ts DESC
            ), ranked AS (
              SELECT s.*,coalesce(s.team_id,r.team_id) AS resolved_team_id,
                     row_number() OVER (PARTITION BY s.game_id,coalesce(s.team_id,r.team_id) ORDER BY s.toi_minutes DESC NULLS LAST,s.player_id) AS rn
              FROM nhl.import_goalie_logs_stage s LEFT JOIN latest_roster r USING(game_id,player_id)
              WHERE s.game_date=%s::date
            )
            INSERT INTO nhl.goalie_game_logs_raw
              (player_id,game_id,team_id,opponent_id,is_home,game_date,saves,shots_faced,goals_allowed,
               toi_minutes,start_flag,start_prob,ev_shots_faced,pp_shots_faced,sh_shots_faced,
               high_danger_shots_faced,rebounds_allowed)
            SELECT s.player_id,s.game_id,s.resolved_team_id,
                   CASE WHEN s.resolved_team_id=g.home_team_id THEN g.away_team_id ELSE g.home_team_id END,
                   s.resolved_team_id=g.home_team_id,s.game_date,s.saves,s.shots_faced,
                   CASE WHEN s.shots_faced IS NOT NULL AND s.saves IS NOT NULL THEN s.shots_faced-s.saves END,
                   s.toi_minutes,s.rn=1,s.start_prob,s.ev_shots_faced,s.pp_shots_faced,
                   s.sh_shots_faced,s.high_danger_shots_faced,s.rebounds_allowed
            FROM ranked s JOIN nhl.games g USING(game_id)
            WHERE s.resolved_team_id IN (g.home_team_id,g.away_team_id)
            ON CONFLICT (player_id,game_id) DO UPDATE SET
              team_id=excluded.team_id,opponent_id=excluded.opponent_id,is_home=excluded.is_home,
              game_date=excluded.game_date,saves=excluded.saves,shots_faced=excluded.shots_faced,
              goals_allowed=excluded.goals_allowed,toi_minutes=excluded.toi_minutes,start_flag=excluded.start_flag,
              start_prob=excluded.start_prob,ev_shots_faced=excluded.ev_shots_faced,
              pp_shots_faced=excluded.pp_shots_faced,sh_shots_faced=excluded.sh_shots_faced,
              high_danger_shots_faced=excluded.high_danger_shots_faced,
              rebounds_allowed=excluded.rebounds_allowed
        """, (slate_date, slate_date))
        connection.commit()


def prepare_participant_identities(
    dsn: str, *, slate_date: str, inventory: dict[str, object],
    partition: dict[str, object],
) -> dict[str, object]:
    """Bind verified identities before either appearance collector runs."""
    categories = partition["classification"]
    deterministic = [int(value) for value in categories["deterministic_same_number_bind"]]
    numeric_proven = [int(value) for value in categories["numeric_identity_proven_bind"]]
    preserved = [int(value) for value in categories["preserved_response_resolution"]]
    new_lookups = [int(value) for value in categories["new_official_lookup"]]
    responses: dict[int, str] = {}
    for player_id in preserved + new_lookups:
        reuse = player_id in set(preserved)
        response = official_get(
            f"https://api-web.nhle.com/v1/player/{player_id}/landing",
            timeout=10, stage="PLAYER_IDENTITY_PREPARATION",
            endpoint_family="PLAYER_LANDING",
            identity={"slate_date": slate_date, "player_id": player_id},
            max_attempts=1, retry_statuses=(), preserve_response=not reuse,
            reuse_preserved=reuse,
        )
        response.raise_for_status()
        payload = response.json()
        returned_id = payload.get("playerId") or payload.get("id")
        if returned_id is None or int(returned_id) != player_id:
            raise RuntimeError(f"PLAYER_LANDING_IDENTITY_MISMATCH:{player_id}:{returned_id}")
        responses[player_id] = authoritative_player_name(
            payload.get("firstName"), payload.get("lastName"))

    goalies = {int(value) for value in inventory["goalie_ids"]}
    teams = {int(key): [int(value) for value in values]
             for key, values in inventory["participant_team_ids"].items()}
    with psycopg.connect(dsn) as connection:
        with connection.transaction():
            for player_id in deterministic + numeric_proven:
                resolve_player_external_identity(
                    connection, player_id=player_id, provider="nhl",
                    provider_player_id=player_id)
            for player_id, full_name in responses.items():
                team_ids = teams.get(player_id, [])
                create_or_verify_player(
                    connection, player_id=player_id, full_name=full_name,
                    team_id=team_ids[0] if len(team_ids) == 1 else None,
                    position="G" if player_id in goalies else "F")
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT player_id, provider_player_id
                    FROM nhl.player_external_ids
                    WHERE player_id = 8482103 AND provider = 'nhl'
                    FOR KEY SHARE
                    """)
                protected = cursor.fetchall()
                if protected != [(8482103, "8482103")]:
                    raise RuntimeError("PROTECTED_PLAYER_8482103_MAPPING_CHANGED")
    return {
        "contract_version": "NHL_PARTICIPANT_IDENTITY_PREPARATION_V1",
        "deterministic_binds": deterministic,
        "numeric_identity_proven_binds": numeric_proven,
        "preserved_response_resolutions": preserved,
        "new_official_lookup_resolutions": new_lookups,
        "partition_sha256": partition["partition_sha256"],
        "status": "IDENTITIES_PREPARED",
    }


def verify_staging_completeness(
    dsn: str, *, slate_date: str, games: pd.DataFrame,
    skaters: pd.DataFrame, goalies: pd.DataFrame,
) -> dict[str, object]:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT game_id, player_id
            FROM nhl.import_skater_logs_stage
            WHERE game_date = %s::date
            ORDER BY game_id, player_id
            """, (slate_date,))
        staged_skaters = [(int(game_id), int(player_id))
                          for game_id, player_id in cursor.fetchall()]
        cursor.execute(
            """
            SELECT game_id, player_id, toi_minutes
            FROM nhl.import_goalie_logs_stage
            WHERE game_date = %s::date
            ORDER BY game_id, player_id
            """, (slate_date,))
        staged_goalie_rows = [(int(game_id), int(player_id), float(toi or 0.0))
                              for game_id, player_id, toi in cursor.fetchall()]
        connection.rollback()
    staged_goalies = [(game_id, player_id) for game_id, player_id, unused in staged_goalie_rows]
    expected_team = {(int(row.game_id), int(row.goalie_id)): int(row.team_id)
                     for row in goalies.itertuples(index=False)}
    ranked: dict[tuple[int, int], list[tuple[float, int]]] = {}
    for game_id, player_id, toi in staged_goalie_rows:
        team_id = expected_team.get((game_id, player_id))
        if team_id is not None:
            ranked.setdefault((game_id, team_id), []).append((toi, player_id))
    actual_starters = [(game_id, max(rows)[1])
                       for (game_id, unused_team), rows in sorted(ranked.items()) if rows]
    return validate_staging_identity_sets(
        expected_games=games.game_id.astype(int).tolist(),
        expected_skaters=list(skaters[["game_id", "player_id"]]
                             .itertuples(index=False, name=None)),
        expected_goalies=list(goalies[["game_id", "goalie_id"]]
                             .itertuples(index=False, name=None)),
        expected_starters=list(goalies.loc[goalies.actual_start_flag,
                                           ["game_id", "goalie_id"]]
                               .itertuples(index=False, name=None)),
        actual_games=sorted({game_id for game_id, unused in staged_skaters}
                            | {game_id for game_id, unused in staged_goalies}),
        actual_skaters=staged_skaters, actual_goalies=staged_goalies,
        actual_starters=actual_starters,
    )


def governed_collectors(
    dsn: str, slate_date: str, *, inventory: dict[str, object] | None = None,
    identity_partition: dict[str, object] | None = None,
    games: pd.DataFrame | None = None, skaters: pd.DataFrame | None = None,
    goalies: pd.DataFrame | None = None,
) -> None:
    python = str(ROOT / ".venv/bin/python")
    _run([python, str(SCRIPTS / "import_schedule_today.py")], slate_date, dsn)
    _run([python, str(SCRIPTS / "refresh_players_and_roster_today.py")], slate_date, dsn)
    if inventory is not None and identity_partition is not None:
        prepare_participant_identities(
            dsn, slate_date=slate_date, inventory=inventory, partition=identity_partition)
    _run([python, str(SCRIPTS / "seed_goalie_logs_for_date.py")], slate_date, dsn)
    _run([python, str(SCRIPTS / "seed_skater_logs_for_date.py")], slate_date, dsn)
    if all(value is not None for value in (games, skaters, goalies)):
        verify_staging_completeness(
            dsn, slate_date=slate_date, games=games, skaters=skaters, goalies=goalies)
    _run([python, str(SCRIPTS / "ingest_shiftcharts_for_date.py"), "--date", slate_date], slate_date, dsn)
    _promote_stage(dsn, slate_date)
    _run([python, str(SCRIPTS / "backfill_game_manpower_segments.py"), "--start-date", slate_date,
          "--end-date", slate_date, "--season", "2026"], slate_date, dsn)
    _run([python, str(SCRIPTS / "fill_pp_toi_minutes_for_date.py"), "--date", slate_date, "--commit"], slate_date, dsn)
    _run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-v", f"game_date={slate_date}",
          "-f", str(SQL / "shiftcharts_pairings_for_date.sql")], slate_date, dsn)
    refresh = SCRIPTS / "refresh.sql"
    if refresh.is_file():
        _run(["psql", dsn, "-v", "ON_ERROR_STOP=1", "-f", str(refresh)], slate_date, dsn)


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--preflight", action="store_true")
    mode.add_argument("--local-input-preflight", action="store_true")
    mode.add_argument("--database-identity-preflight", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--date", dest="slate_date")
    parser.add_argument("date_arg", nargs="?")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--prediction-root", type=Path, default=DEFAULT_PREDICTION_ROOT)
    parser.add_argument("--operational-root", type=Path, default=DEFAULT_OPERATIONAL_ROOT)
    parser.add_argument("--response-source", action="append", default=[])
    parser.add_argument("--reuse-request-run-id")
    parser.add_argument("--lineage-request-run-id", action="append", default=[])
    parser.add_argument("--authorized-player-lookup-id", action="append", type=int, default=[])
    args = parser.parse_args()
    if len(args.authorized_player_lookup_id) != len(set(args.authorized_player_lookup_id)):
        parser.error("duplicate --authorized-player-lookup-id")
    slate_date = args.slate_date or args.date_arg
    if not slate_date:
        parser.error("provide --date YYYY-MM-DD or positional YYYY-MM-DD")
    try:
        date.fromisoformat(slate_date)
    except ValueError:
        parser.error("date must be YYYY-MM-DD")
    source_binding = None
    authority_binding = None
    roster_binding = None
    player_binding = None
    response_source_ledger = None
    conditional_inventory = None
    topology = None
    request_lineage = None
    if slate_date != "2026-09-19":
        try:
            source_binding = resolve_operational_sources(
                slate_date=slate_date, operational_root=args.operational_root)
            if (((args.execute or args.database_identity_preflight)
                 and slate_date == "2026-09-20")
                    or args.response_source or args.reuse_request_run_id
                    or args.lineage_request_run_id):
                typed_sources = _parse_response_source_specs(args.response_source)
                if args.reuse_request_run_id:
                    raise RuntimeError("LEGACY_SINGLE_RESPONSE_SOURCE_NOT_PERMITTED")
                required_sources = {
                    "AUTHORITY_RESPONSE_SOURCE":
                        "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6",
                    "ROSTER_RESPONSE_SOURCE":
                        "nhlpostgame_20260920_20260922T171356619916Z_cd2ac1d9",
                    "PLAYER_IDENTITY_RESPONSE_SOURCE":
                        "nhlpostgame_20260920_20260922T181726727181Z_70f0280d",
                }
                if slate_date == "2026-09-20" and typed_sources != required_sources:
                    raise RuntimeError("SEPTEMBER_20_TYPED_RESPONSE_SOURCES_REQUIRED")
                required_ancestors = [
                    "nhlpostgame_20260920_20260922T151929437487Z_f4cd9da6",
                    "nhlpostgame_20260920_20260922T161724179739Z_cef0bc8b",
                    "nhlpostgame_20260920_20260922T171356619916Z_cd2ac1d9",
                    "nhlpostgame_20260920_20260922T181726727181Z_70f0280d",
                ]
                if (slate_date == "2026-09-20"
                        and args.lineage_request_run_id != required_ancestors):
                    raise RuntimeError("SEPTEMBER_20_ALL_FAILED_ANCESTORS_REQUIRED")
                if len(args.lineage_request_run_id) != len(set(args.lineage_request_run_id)):
                    raise RuntimeError("DUPLICATE_LINEAGE_REQUEST_RUN_ID")
                source_receipt = _receipt(
                    typed_sources["AUTHORITY_RESPONSE_SOURCE"], role="AUTHORITY_RESPONSE_SOURCE",
                    slate_date=slate_date)
                prior_root = (args.output_root / "request_runs" / slate_date /
                              typed_sources["AUTHORITY_RESPONSE_SOURCE"])
                authority_binding = verify_preserved_response_run(
                    prior_root, expected_run_id=typed_sources["AUTHORITY_RESPONSE_SOURCE"],
                    slate_date=slate_date, game_ids=source_binding["game_ids"],
                    repository_root=ROOT,
                    expected_journal_sha256=source_receipt["journal_sha256"],
                    expected_tree_fingerprint=source_receipt["tree_fingerprint"],
                )
                roster_receipt = _receipt(
                    typed_sources["ROSTER_RESPONSE_SOURCE"], role="ROSTER_RESPONSE_SOURCE",
                    slate_date=slate_date)
                roster_binding = verify_roster_response_run(
                    args.output_root / "request_runs" / slate_date /
                    typed_sources["ROSTER_RESPONSE_SOURCE"],
                    expected_run_id=typed_sources["ROSTER_RESPONSE_SOURCE"],
                    slate_date=slate_date, game_ids=source_binding["game_ids"],
                    expected_teams=roster_receipt["teams"], repository_root=ROOT,
                    expected_journal_sha256=roster_receipt["journal_sha256"],
                    expected_tree_fingerprint=roster_receipt["tree_fingerprint"],
                    expected_response_set_sha256=roster_receipt["response_set_sha256"],
                )
                player_receipt = _receipt(
                    typed_sources["PLAYER_IDENTITY_RESPONSE_SOURCE"],
                    role="PLAYER_IDENTITY_RESPONSE_SOURCE", slate_date=slate_date)
                player_binding = verify_player_identity_response_run(
                    args.output_root / "request_runs" / slate_date /
                    typed_sources["PLAYER_IDENTITY_RESPONSE_SOURCE"],
                    expected_run_id=typed_sources["PLAYER_IDENTITY_RESPONSE_SOURCE"],
                    slate_date=slate_date, game_ids=source_binding["game_ids"],
                    expected_player_id=int(player_receipt["player_id"]),
                    repository_root=ROOT,
                    expected_journal_sha256=player_receipt["journal_sha256"],
                    expected_tree_fingerprint=player_receipt["tree_fingerprint"],
                    expected_object_sha256=player_receipt["object_sha256"],
                    expected_index_sha256=player_receipt["index_sha256"],
                )
                response_source_ledger = build_typed_response_source_ledger(
                    [authority_binding, roster_binding, player_binding])
                conditional_inventory = local_conditional_lookup_inventory(
                    authority_binding, roster_binding)
                topology = {
                    "contract_version": "NHL_POSTGAME_PREFLIGHT_TOPOLOGY_V1",
                    "base_logical_operations": 59,
                    "authority_response_identities": len(authority_binding["responses"]),
                    "roster_response_identities": len(roster_binding["responses"]),
                    "player_identity_response_identities": len(player_binding["responses"]),
                    "authority_source_reuse_events": 31,
                    "roster_source_reuse_events": 14,
                    "player_identity_source_reuse_events": 1,
                    "preserved_response_reuses": "REQUIRES_DATABASE_IDENTITY_PREFLIGHT",
                    "required_shift_pbp_network_operations": 14,
                    "conditional_player_lookup_network_operations":
                        "REQUIRES_DATABASE_IDENTITY_PREFLIGHT",
                    "logical_operations": "REQUIRES_DATABASE_IDENTITY_PREFLIGHT",
                    "new_network_operations": "REQUIRES_DATABASE_IDENTITY_PREFLIGHT",
                    "database_mapping_coverage": "UNVERIFIED",
                }
                ancestors = []
                for ancestor_run_id in args.lineage_request_run_id:
                    receipt = _receipt(
                        ancestor_run_id, role="FAILED_EXECUTION_ANCESTOR",
                        slate_date=slate_date)
                    ancestor_root = args.output_root / "request_runs" / slate_date / ancestor_run_id
                    if ancestor_run_id == required_ancestors[1]:
                        ancestor = verify_failed_execution_ancestor(
                            ancestor_root, expected_run_id=ancestor_run_id,
                            slate_date=slate_date, game_ids=source_binding["game_ids"],
                            response_source=authority_binding, repository_root=ROOT,
                            expected_journal_sha256=receipt["journal_sha256"],
                            expected_tree_fingerprint=receipt["tree_fingerprint"],
                        )
                    else:
                        failure_class = {
                            required_ancestors[0]: "IMMUTABLE_MAINLINE_RUN_CARDINALITY:0",
                            required_ancestors[2]: "PLAYER_EXTERNAL_IDENTITY_TRANSACTION_ABORT",
                            required_ancestors[3]: "PLAYER_AUTHORITATIVE_NAME_INVALID",
                        }[ancestor_run_id]
                        ancestor = verify_failed_request_ancestor(
                            ancestor_root, expected_run_id=ancestor_run_id,
                            game_ids=source_binding["game_ids"], repository_root=ROOT,
                            expected_journal_sha256=receipt["journal_sha256"],
                            expected_tree_fingerprint=receipt["tree_fingerprint"],
                            failure_class=failure_class,
                            response_source=(authority_binding
                                             if ancestor_run_id == required_ancestors[2] else None),
                            response_sources=([authority_binding, roster_binding]
                                              if ancestor_run_id == required_ancestors[3]
                                              else None),
                        )
                    ancestors.append(ancestor)
                request_lineage = {
                    "contract_version": "NHL_POSTGAME_REQUEST_LINEAGE_V4",
                    "slate_date": slate_date,
                    "canonical_game_set_hash": authority_binding["canonical_game_set_hash"],
                    "response_sources": [
                        _public_response_source(authority_binding),
                        _public_response_source(roster_binding),
                        _public_response_source(player_binding),
                    ],
                    "failed_ancestors": ancestors,
                    "out_of_band_diagnostic_requests": [{
                        "description": "authorized roster redirect diagnostic GET",
                        "requested_url": "https://api-web.nhle.com/v1/roster/NJD/current",
                        "http_status": 307,
                        "redirect_location":
                            "https://api-web.nhle.com/v1/roster/NJD/20262027",
                        "official_nhl_network_attempts": 1,
                        "redirects_followed": 0,
                        "included_in_request_journal": False,
                        "included_in_lineage_totals": False,
                    }],
                }
        except RuntimeError as error:
            print(json.dumps({"status": "FAILED_CLOSED_LOCAL_INPUT", "failure": str(error),
                              "database_requests": 0, "external_requests": 0,
                              "bookmaker_requests": 0, "paid_credits": 0},
                             indent=2, sort_keys=True))
            return 5
    if args.local_input_preflight:
        if source_binding is None:
            print(json.dumps({"status": "LEGACY_SEPTEMBER_19_BINDING_UNCHANGED",
                              "slate_date": slate_date, "database_requests": 0,
                              "external_requests": 0}, indent=2, sort_keys=True))
        else:
            print(json.dumps({"status": "LOCAL_INPUTS_VALID", "database_requests": 0,
                              "external_requests": 0, "source_binding": source_binding,
                              "response_source_ledger": ({
                                  "contract_version": response_source_ledger["contract_version"],
                                  "declared_response_identities":
                                      response_source_ledger["declared_response_identities"],
                                  "sources": [_public_response_source(source)
                                              for source in response_source_ledger["sources"]],
                              } if response_source_ledger else None),
                              "conditional_player_lookups": conditional_inventory,
                              "topology": topology, "request_lineage": request_lineage},
                             indent=2, sort_keys=True))
        return 0
    load_env(args.env_file)
    dsn = (os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not dsn:
        print(json.dumps({"status": "FAILED", "failure": "DATABASE_URL_MISSING"}))
        return 3
    identity_partition = None
    if args.database_identity_preflight or (args.execute and slate_date != "2026-09-19"):
        if conditional_inventory is None or player_binding is None:
            print(json.dumps({"status": "FAILED_CLOSED_DATABASE_PREFLIGHT",
                              "failure": "DATABASE_IDENTITY_PREFLIGHT_SOURCES_REQUIRED"},
                             indent=2, sort_keys=True))
            return 5
        preserved_ids = {
            int(response["resource_identity"]["player_id"])
            for response in player_binding["responses"]
        }
        try:
            identity_partition = database_identity_preflight(
                dsn, inventory=conditional_inventory,
                preserved_player_ids=preserved_ids,
                authorized_lookup_ids=set(args.authorized_player_lookup_id),
                slate_date=slate_date)
        except Exception as error:
            print(json.dumps({
                "status": "FAILED_CLOSED_DATABASE_PREFLIGHT",
                "failure": f"{type(error).__name__}:{error}",
                "request_run_created": False, "database_writes": 0,
                "external_requests": 0, "bookmaker_requests": 0, "paid_credits": 0,
            }, indent=2, sort_keys=True))
            return 5
        if identity_partition["status"] != "DATABASE_IDENTITY_PREFLIGHT_VALID":
            print(json.dumps({
                "status": "FAILED_CLOSED_DATABASE_PREFLIGHT",
                "failure": identity_partition.get(
                    "failure", "DATABASE_IDENTITY_PREFLIGHT_INVALID_WITHOUT_REASON"),
                "request_run_created": False,
                "database_preflight": identity_partition,
                "database_transactions": 1, "database_read_queries": 2,
                "database_writes": 0,
                "external_requests": 0, "bookmaker_requests": 0, "paid_credits": 0,
            }, indent=2, sort_keys=True))
            return 5
        topology = {
            **(topology or {}),
            "database_mapping_coverage": "VERIFIED_READ_ONLY",
            "preserved_response_reuses": 46,
            "conditional_player_lookup_network_operations":
                len(identity_partition["classification"]["new_official_lookup"]),
            "logical_operations": 59
                + len(identity_partition["classification"]["preserved_response_resolution"])
                + len(identity_partition["classification"]["new_official_lookup"]),
            "new_network_operations": 14
                + len(identity_partition["classification"]["new_official_lookup"]),
            "identity_partition_sha256": identity_partition["partition_sha256"],
        }
        if request_lineage is not None:
            request_lineage["database_identity_preflight"] = {
                "contract_version": identity_partition["contract_version"],
                "partition_sha256": identity_partition["partition_sha256"],
                "status": identity_partition["status"],
                "authorized_new_official_lookup_ids":
                    identity_partition["authorized_new_official_lookup_ids"],
                "numeric_identity_provenance":
                    identity_partition["numeric_identity_provenance"],
                "numeric_identity_provenance_sha256":
                    identity_partition["numeric_identity_provenance_sha256"],
            }
    if args.database_identity_preflight:
        print(json.dumps({
            "status": "DATABASE_IDENTITY_PREFLIGHT_VALID",
            "request_run_created": False, "database_preflight": identity_partition,
            "topology": topology, "database_transactions": 1,
            "database_read_queries": 2, "database_writes": 0,
            "external_requests": 0,
            "bookmaker_requests": 0, "paid_credits": 0,
        }, indent=2, sort_keys=True))
        return 0
    if not args.execute:
        payload, code = read_only_preflight(dsn, slate_date)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return code
    try:
        with reconciliation_lock(args.output_root, slate_date):
            canonical = canonical_slate(dsn, slate_date)
            canonical_ids = set(canonical.game_id.astype(int))
            if source_binding is not None and canonical_ids != set(source_binding["game_ids"]):
                raise RuntimeError("DATABASE_CANONICAL_GAME_SET_DIFFERS_FROM_IMMUTABLE_LOCAL_SPINE")
            game_hash = canonical_game_set_hash(canonical_ids)
            started = datetime.now(timezone.utc)
            run_id = f"nhlpostgame_{slate_date.replace('-', '')}_{started.strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:8]}"
            request_root = args.output_root / "request_runs" / slate_date / run_id
            context_values = {
                ENV_REQUIRED: "1", ENV_RUN_ID: run_id,
                ENV_JOURNAL: str(request_root / "official_request_journal.jsonl"),
                ENV_CACHE: str(request_root / "preserved_responses"),
                ENV_SLATE: slate_date, ENV_GAME_HASH: game_hash,
                ENV_GAME_IDS: ",".join(str(value) for value in sorted(canonical_ids)),
            }
            if response_source_ledger is not None:
                context_values[ENV_RESPONSE_SOURCE_LEDGER] = json.dumps(
                    response_source_ledger, sort_keys=True, separators=(",", ":"))
                for key in (ENV_SOURCE_CACHE, ENV_SOURCE_RUN_ID, ENV_SOURCE_JOURNAL_SHA256):
                    os.environ.pop(key, None)
            else:
                for key in (ENV_SOURCE_CACHE, ENV_SOURCE_RUN_ID, ENV_SOURCE_JOURNAL_SHA256,
                            ENV_RESPONSE_SOURCE_LEDGER):
                    os.environ.pop(key, None)
            os.environ.update(context_values)
            RequestContext.from_env(required=True)
            official, boxes, request_counts = fetch_official(
                slate_date, canonical_ids, reuse_authority=authority_binding is not None)
            validated = validate_final_slate(canonical, official, slate_date)
            expected_games, expected_skaters, expected_goalies = build_outcomes(
                validated, boxes, datetime.now(timezone.utc).isoformat())
            destination, disposition = publish_reconciliation(
                canonical=canonical, official=official, boxscores=boxes, slate_date=slate_date,
                prediction_root=args.prediction_root / slate_date, output_root=args.output_root,
                collector=lambda: governed_collectors(
                    dsn, slate_date, inventory=conditional_inventory,
                    identity_partition=identity_partition, games=expected_games,
                    skaters=expected_skaters, goalies=expected_goalies),
                observed_at=datetime.now(timezone.utc).isoformat(),
                request_journal=Path(context_values[ENV_JOURNAL]),
                request_accounting_factory=lambda: summarize_journal(
                    Path(context_values[ENV_JOURNAL]), run_id=run_id,
                    expected_game_hash=game_hash,
                ),
                source_binding=source_binding,
                request_lineage=request_lineage,
            )
            accounting = json.loads((destination / "official_request_accounting.json").read_text())
        print(json.dumps({
            "status": disposition, "output": str(destination),
            "authority_boundary_request_counts": request_counts,
            "official_request_accounting": accounting,
            "bookmaker_requests": 0, "paid_credits": 0,
        }, indent=2, sort_keys=True))
        return 0
    except RuntimeError as error:
        message = str(error)
        print(json.dumps({"status": "FAILED_CLOSED", "failure": message,
                          "bookmaker_requests": 0, "paid_credits": 0}, indent=2, sort_keys=True))
        if "NOT_OFFICIAL_FINAL" in message:
            return 2
        if "IDENTITY" in message or "UNSUPPORTED" in message:
            return 3
        if "ALREADY_RUNNING" in message:
            return 4
        if "CONFLICTING_RETAINED" in message:
            return 6
        return 5
    except Exception as error:
        print(json.dumps({"status": "FAILED_CLOSED",
                          "failure": f"{type(error).__name__}:{error}",
                          "bookmaker_requests": 0, "paid_credits": 0},
                         indent=2, sort_keys=True))
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
