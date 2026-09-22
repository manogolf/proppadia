"""Authoritative, transaction-scoped NHL postgame skater-stage synchronization.

This module has no network entry point and does not create request runs.  Its
read-only preflight and write correction both consume an already retained,
receipt-verified official authority response run.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import psycopg

from backend.nhl.official_request_journal import (
    canonical_game_set_hash,
    sha256_bytes,
    verify_payload_identity,
    verify_preserved_response_run,
)
from backend.nhl.postgame_reconcile.core import validate_staging_identity_sets


CONTRACT = "NHL_AUTHORITATIVE_SKATER_STAGING_SYNC_V1"
PREFLIGHT_CONTRACT = "NHL_AUTHORITATIVE_STAGING_PREFLIGHT_V2"
AUTHORIZATION_CONTRACT = "NHL_AUTHORITATIVE_STAGING_CORRECTION_AUTHORIZATION_V2"
HEX64 = re.compile(r"^[0-9a-f]{64}$")


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def identity_set_sha256(identities: Iterable[tuple[int, int]]) -> str:
    normalized = [[int(game_id), int(player_id)]
                  for game_id, player_id in sorted(set(identities))]
    return hashlib.sha256(_canonical_json(normalized)).hexdigest()


def authorized_extra_set_digest(
    *, slate_date: str, canonical_game_set_sha256: str,
    expected_identity_set_sha256: str, extra_identities: Iterable[tuple[int, int]],
) -> str:
    payload = {
        "contract_version": CONTRACT,
        "slate_date": str(slate_date),
        "canonical_game_set_sha256": str(canonical_game_set_sha256),
        "expected_identity_set_sha256": str(expected_identity_set_sha256),
        "extra_identities": [[int(game_id), int(player_id)]
                             for game_id, player_id in sorted(set(extra_identities))],
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _identity_inventory(
    expected_identities: Iterable[tuple[int, int]],
    existing_identities: Iterable[tuple[int, int]],
) -> dict[str, object]:
    expected_rows = sorted((int(game_id), int(player_id))
                           for game_id, player_id in expected_identities)
    existing_rows = sorted((int(game_id), int(player_id))
                           for game_id, player_id in existing_identities)
    if len(expected_rows) != len(set(expected_rows)):
        raise RuntimeError("AUTHORITATIVE_EXPECTED_DUPLICATE_IDENTITY")
    expected_set, existing_set = set(expected_rows), set(existing_rows)
    missing, extra = sorted(expected_set - existing_set), sorted(existing_set - expected_set)
    duplicate_rows = [
        {"game_id": game_id, "player_id": player_id, "multiplicity": multiplicity}
        for (game_id, player_id), multiplicity in sorted(Counter(existing_rows).items())
        if multiplicity > 1
    ]
    return {
        "expected_count": len(expected_rows),
        "existing_count": len(existing_rows),
        "missing_count": len(missing),
        "extra_count": len(extra),
        "duplicate_natural_key_count": len(duplicate_rows),
        "duplicate_excess_row_count": sum(int(row["multiplicity"]) - 1
                                          for row in duplicate_rows),
        "expected_identities": expected_rows,
        "existing_identities": existing_rows,
        "missing_identities": missing,
        "extra_identities": extra,
        "duplicate_natural_keys": duplicate_rows,
        "expected_identity_set_sha256": identity_set_sha256(expected_set),
        "existing_identity_set_sha256": identity_set_sha256(existing_set),
        "missing_identity_set_sha256": identity_set_sha256(missing),
        "extra_identity_set_sha256": identity_set_sha256(extra),
        "duplicate_inventory_sha256": hashlib.sha256(
            _canonical_json(duplicate_rows)).hexdigest(),
    }


def _required_cardinalities(evidence: dict[str, object]) -> dict[str, int]:
    game_ids = [int(value) for value in evidence["game_ids"]]
    skaters = [tuple(value) for value in evidence["skater_identities"]]
    official_goalies, official_starters, official_teams = _authoritative_goalie_contract(
        evidence["goalie_rows"], game_ids=game_ids)
    if (len(game_ids) != 7 or len(set(game_ids)) != 7
            or len(skaters) != 252 or len(set(skaters)) != 252
            or any(evidence["per_game"].get(str(game_id)) !=
                   {"skaters": 36, "goalies": 4} for game_id in game_ids)
            or official_goalies != [tuple(value) for value in evidence["goalie_identities"]]
            or official_starters != [tuple(value) for value in evidence["starter_identities"]]
            or official_teams != evidence["goalie_team_membership"]):
        raise RuntimeError("AUTHORITATIVE_STAGING_CARDINALITY_CONTRACT_MISMATCH")
    return {
        "canonical_games": 7,
        "skater_appearances": 252,
        "skaters_per_game": 36,
        "goalie_appearances": 28,
        "goalies_per_game": 4,
        "official_teams_per_game": 2,
        "confirmed_starters": 14,
    }


def correction_authorization_digest(
    evidence: dict[str, object], *, skaters: dict[str, object],
    goalies: dict[str, object],
) -> str:
    inventory_keys = (
        "expected_identity_set_sha256", "existing_identity_set_sha256",
        "missing_identity_set_sha256", "extra_identity_set_sha256",
        "duplicate_inventory_sha256",
    )
    payload = {
        "contract_version": AUTHORIZATION_CONTRACT,
        "slate_date": str(evidence["slate_date"]),
        "canonical_game_set_sha256": str(evidence["canonical_game_set_sha256"]),
        "authority_source_run_id": str(evidence["authority_source_run_id"]),
        "authority_response_set_sha256": str(evidence["authority_response_set_sha256"]),
        "required_cardinalities": _required_cardinalities(evidence),
        "skaters": {
            "hashes": {key: str(skaters[key]) for key in inventory_keys},
            "duplicate_natural_keys": skaters["duplicate_natural_keys"],
        },
        "goalies": {
            "hashes": {key: str(goalies[key]) for key in inventory_keys},
            "duplicate_natural_keys": goalies["duplicate_natural_keys"],
        },
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _toi_minutes(value: object) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value) / 60.0
    pieces = str(value).split(":")
    try:
        if len(pieces) == 2:
            return float(int(pieces[0])) + float(int(pieces[1])) / 60.0
    except (TypeError, ValueError):
        pass
    raise RuntimeError(f"AUTHORITATIVE_TOI_MALFORMED:{value}")


def _authoritative_goalie_contract(
    goalies: Iterable[dict[str, object]], *, game_ids: Iterable[int],
    canonical_teams: dict[int, set[int]] | None = None,
) -> tuple[list[tuple[int, int]], list[tuple[int, int]], dict[str, list[int]]]:
    """Validate official goalie membership and select one starter per team.

    Team membership and TOI in this function are official boxscore evidence.
    Staging columns are deliberately not accepted as authority.
    """
    expected_games = sorted({int(value) for value in game_ids})
    by_game: dict[int, list[tuple[int, int, float]]] = {
        game_id: [] for game_id in expected_games
    }
    identity_teams: dict[tuple[int, int], int] = {}
    identities: list[tuple[int, int]] = []
    for row in goalies:
        try:
            game_id = int(row["game_id"])
            player_id = int(row["player_id"])
            team_id = int(row["team_id"])
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("AUTHORITATIVE_GOALIE_TEAM_MEMBERSHIP_INVALID") from error
        if game_id not in by_game:
            raise RuntimeError(f"AUTHORITATIVE_GOALIE_GAME_OUT_OF_SCOPE:{game_id}")
        identity = (game_id, player_id)
        prior_team = identity_teams.get(identity)
        if prior_team is not None:
            reason = ("CONFLICTING_TEAM" if prior_team != team_id
                      else "DUPLICATE_IDENTITY")
            raise RuntimeError(f"AUTHORITATIVE_GOALIE_{reason}:{game_id}:{player_id}")
        raw_toi = row.get("toi_minutes")
        try:
            toi = float(raw_toi)
        except (TypeError, ValueError) as error:
            raise RuntimeError(
                f"AUTHORITATIVE_GOALIE_TOI_UNUSABLE:{game_id}:{player_id}") from error
        if not math.isfinite(toi) or toi < 0:
            raise RuntimeError(f"AUTHORITATIVE_GOALIE_TOI_UNUSABLE:{game_id}:{player_id}")
        identity_teams[identity] = team_id
        identities.append(identity)
        by_game[game_id].append((player_id, team_id, toi))

    starters: list[tuple[int, int]] = []
    team_membership: dict[str, list[int]] = {}
    for game_id in expected_games:
        rows = by_game[game_id]
        if len(rows) != 4:
            raise RuntimeError(f"AUTHORITATIVE_STAGING_GOALIES_PER_GAME:{game_id}:{len(rows)}")
        teams = {team_id for unused_player, team_id, unused_toi in rows}
        if len(teams) != 2:
            raise RuntimeError(f"AUTHORITATIVE_GOALIE_TEAM_CARDINALITY:{game_id}:{len(teams)}")
        if canonical_teams is not None and teams != canonical_teams.get(game_id, set()):
            raise RuntimeError(f"AUTHORITATIVE_GOALIE_CANONICAL_TEAM_MISMATCH:{game_id}")
        team_membership[str(game_id)] = sorted(teams)
        for team_id in sorted(teams):
            team_rows = [(player_id, toi) for player_id, row_team, toi in rows
                         if row_team == team_id]
            maximum = max(toi for unused_player, toi in team_rows)
            winners = [player_id for player_id, toi in team_rows if toi == maximum]
            if len(winners) != 1:
                raise RuntimeError(
                    f"AUTHORITATIVE_GOALIE_STARTER_NOT_UNIQUE:{game_id}:{team_id}")
            starters.append((game_id, winners[0]))

    identities.sort(); starters.sort()
    if len(identities) != 28 or len(starters) != 14:
        raise RuntimeError(
            f"AUTHORITATIVE_GOALIE_TOTALS_INVALID:{len(identities)}:{len(starters)}")
    return identities, starters, team_membership


def build_authoritative_staging_set(
    binding: dict[str, object], *, slate_date: str, expected_game_ids: Iterable[int],
) -> dict[str, object]:
    """Rebuild exact skater/goalie identities from verified response objects."""
    game_ids = sorted({int(value) for value in expected_game_ids})
    if len(game_ids) != 7:
        raise RuntimeError(f"AUTHORITATIVE_STAGING_GAME_CARDINALITY:{len(game_ids)}")
    game_hash = canonical_game_set_hash(game_ids)
    if (binding.get("role") != "AUTHORITY_RESPONSE_SOURCE"
            or binding.get("canonical_game_set_hash") != game_hash):
        raise RuntimeError("AUTHORITATIVE_STAGING_SOURCE_BINDING_MISMATCH")

    claims = [row for row in binding.get("responses", [])
              if row.get("endpoint_family") == "BOXSCORE"]
    claim_ids = [int(row["resource_identity"]["game_id"]) for row in claims]
    if (sorted(claim_ids) != game_ids or len(claim_ids) != len(set(claim_ids))
            or any(str(row["resource_identity"].get("slate_date")) != slate_date
                   for row in claims)):
        raise RuntimeError("AUTHORITATIVE_STAGING_BOXSCORE_IDENTITY_SET_MISMATCH")

    cache = Path(str(binding["source_cache"]))
    skaters: list[dict[str, object]] = []
    goalies: list[dict[str, object]] = []
    canonical_teams: dict[int, set[int]] = {}
    per_game: dict[str, dict[str, int]] = {}
    for claim in sorted(claims, key=lambda row: int(row["resource_identity"]["game_id"])):
        identity = claim["resource_identity"]
        game_id = int(identity["game_id"])
        object_path = cache / "objects" / f"{claim['object_sha256']}.json"
        if not object_path.is_file():
            raise RuntimeError("AUTHORITATIVE_STAGING_OBJECT_MISSING")
        body = object_path.read_bytes()
        if (sha256_bytes(body) != claim["object_sha256"]
                or len(body) != int(claim["response_bytes"])):
            raise RuntimeError("AUTHORITATIVE_STAGING_OBJECT_CHANGED")
        verify_payload_identity("BOXSCORE", identity, body)
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as error:
            raise RuntimeError("AUTHORITATIVE_STAGING_OBJECT_MALFORMED") from error
        if str(payload.get("gameDate") or "") != slate_date:
            raise RuntimeError("AUTHORITATIVE_STAGING_OBJECT_DATE_MISMATCH")

        game_skaters: list[dict[str, object]] = []
        game_goalies: list[dict[str, object]] = []
        game_team_ids: set[int] = set()
        for side, is_home in (("homeTeam", True), ("awayTeam", False)):
            team_id = (payload.get(side) or {}).get("id")
            other = "awayTeam" if side == "homeTeam" else "homeTeam"
            opponent_id = (payload.get(other) or {}).get("id")
            if team_id is None or opponent_id is None or int(team_id) == int(opponent_id):
                raise RuntimeError(f"AUTHORITATIVE_STAGING_TEAM_IDENTITY_INVALID:{game_id}")
            game_team_ids.add(int(team_id))
            team = (payload.get("playerByGameStats") or {}).get(side)
            if not isinstance(team, dict):
                raise RuntimeError(f"AUTHORITATIVE_STAGING_TEAM_STATS_MISSING:{game_id}:{side}")
            defense = team.get("defense") or []
            defensemen = team.get("defensemen") or []
            if defense and defensemen:
                raise RuntimeError(f"AUTHORITATIVE_STAGING_DEFENSE_SCHEMA_CONFLICT:{game_id}:{side}")
            for section, rows in (("forwards", team.get("forwards") or []),
                                  ("defense", defense or defensemen)):
                if not isinstance(rows, list):
                    raise RuntimeError(f"AUTHORITATIVE_STAGING_SECTION_MALFORMED:{game_id}:{side}:{section}")
                for row_index, row in enumerate(rows):
                    raw_id = row.get("playerId")
                    if raw_id is None:
                        raise RuntimeError(f"AUTHORITATIVE_STAGING_PLAYER_ID_MISSING:{game_id}")
                    try:
                        player_id = int(raw_id)
                    except (TypeError, ValueError) as error:
                        raise RuntimeError(f"AUTHORITATIVE_STAGING_PLAYER_ID_MALFORMED:{game_id}") from error
                    attempts = row.get("shotAttempts")
                    if attempts is None and row.get("missedShots") is not None:
                        attempts = int(row.get("sog") or 0) + int(row["missedShots"])
                    game_skaters.append({
                        "game_id": game_id,
                        "player_id": player_id,
                        "game_date": slate_date,
                        "team_id": int(team_id),
                        "opponent_id": int(opponent_id),
                        "is_home": is_home,
                        "shots_on_goal": int(row.get("sog") or 0),
                        "shot_attempts": None if attempts is None else int(attempts),
                        "toi_minutes": _toi_minutes(row.get("toi")),
                        "pp_toi_minutes": None,
                        "goals": int(row.get("goals") or 0),
                        "assists": int(row.get("assists") or 0),
                        "blocks": int(row.get("blockedShots") or 0),
                        "official_identity_locator":
                            f"$.playerByGameStats.{side}.{section}[{row_index}].playerId",
                    })
            goalie_rows = team.get("goalies") or []
            if not isinstance(goalie_rows, list):
                raise RuntimeError(f"AUTHORITATIVE_STAGING_GOALIES_MALFORMED:{game_id}:{side}")
            for row_index, row in enumerate(goalie_rows):
                raw_id = row.get("playerId")
                if raw_id is None:
                    raise RuntimeError(f"AUTHORITATIVE_STAGING_GOALIE_ID_MISSING:{game_id}")
                game_goalies.append({
                    "game_id": game_id, "player_id": int(raw_id),
                    "team_id": int(team_id), "toi_minutes": _toi_minutes(row.get("toi")),
                    "official_identity_locator":
                        f"$.playerByGameStats.{side}.goalies[{row_index}].playerId",
                })

        skater_keys = [(int(row["game_id"]), int(row["player_id"])) for row in game_skaters]
        goalie_keys = [(int(row["game_id"]), int(row["player_id"])) for row in game_goalies]
        if len(skater_keys) != 36:
            raise RuntimeError(f"AUTHORITATIVE_STAGING_SKATERS_PER_GAME:{game_id}:{len(skater_keys)}")
        if len(goalie_keys) != 4:
            raise RuntimeError(f"AUTHORITATIVE_STAGING_GOALIES_PER_GAME:{game_id}:{len(goalie_keys)}")
        if len(game_team_ids) != 2:
            raise RuntimeError(f"AUTHORITATIVE_STAGING_TEAM_CARDINALITY:{game_id}")
        if len(skater_keys) != len(set(skater_keys)) or len(goalie_keys) != len(set(goalie_keys)):
            raise RuntimeError(f"AUTHORITATIVE_STAGING_DUPLICATE_IDENTITY:{game_id}")
        if set(skater_keys) & set(goalie_keys):
            raise RuntimeError(f"AUTHORITATIVE_STAGING_ROLE_CONFLICT:{game_id}")
        skaters.extend(game_skaters); goalies.extend(game_goalies)
        canonical_teams[game_id] = game_team_ids
        per_game[str(game_id)] = {"skaters": len(skater_keys), "goalies": len(goalie_keys)}

    skater_identities = sorted((int(row["game_id"]), int(row["player_id"]))
                                for row in skaters)
    goalie_identities, starters, goalie_team_membership = _authoritative_goalie_contract(
        goalies, game_ids=game_ids, canonical_teams=canonical_teams)
    if (len(skater_identities) != 252 or len(set(skater_identities)) != 252
            or len(goalie_identities) != 28 or len(set(goalie_identities)) != 28
            or len(starters) != 14):
        raise RuntimeError(
            f"AUTHORITATIVE_STAGING_TOTALS_INVALID:{len(set(skater_identities))}:"
            f"{len(set(goalie_identities))}:{len(starters)}")
    return {
        "contract_version": CONTRACT,
        "slate_date": slate_date,
        "game_ids": game_ids,
        "canonical_game_set_sha256": game_hash,
        "authority_source_run_id": binding["source_run_id"],
        "authority_source_journal_sha256": binding["source_journal_sha256"],
        "authority_source_tree_fingerprint": binding["tree_fingerprint"],
        "authority_response_set_sha256": binding["response_set_sha256"],
        "skater_rows": skaters,
        "skater_identities": skater_identities,
        "goalie_rows": goalies,
        "goalie_identities": goalie_identities,
        "starter_identities": starters,
        "goalie_team_membership": goalie_team_membership,
        "expected_identity_set_sha256": identity_set_sha256(skater_identities),
        "per_game": per_game,
        "exact_provider_id_resolution": True,
    }


def load_verified_authoritative_staging_set(
    request_root: Path, *, source_run_id: str, slate_date: str,
    game_ids: Iterable[int], repository_root: Path,
    expected_journal_sha256: str, expected_tree_fingerprint: str,
) -> dict[str, object]:
    binding = verify_preserved_response_run(
        request_root, expected_run_id=source_run_id, slate_date=slate_date,
        game_ids=game_ids, repository_root=repository_root,
        expected_journal_sha256=expected_journal_sha256,
        expected_tree_fingerprint=expected_tree_fingerprint,
    )
    return build_authoritative_staging_set(
        binding, slate_date=slate_date, expected_game_ids=game_ids)


def diff_staging_identities(
    evidence: dict[str, object], existing_identities: Iterable[tuple[int, int]],
) -> dict[str, object]:
    return _identity_inventory(evidence["skater_identities"], existing_identities)


def _begin_read_only(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")


def _begin_correction(connection: Any) -> None:
    with connection.cursor() as cursor:
        cursor.execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE")
        cursor.execute("SET LOCAL lock_timeout = '10s'")
        # A table lock protects the target set from collectors that do not know
        # about the synchronizer's advisory-lock namespace.
        cursor.execute(
            "LOCK TABLE nhl.import_skater_logs_stage IN SHARE ROW EXCLUSIVE MODE")


def _fetch_canonical_game_ids(connection: Any, slate_date: str) -> list[int]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT game_id FROM nhl.games WHERE game_date=%s::date ORDER BY game_id",
            (slate_date,))
        return [int(row[0]) for row in cursor.fetchall()]


def _fetch_target_skater_identities(
    connection: Any, *, slate_date: str, game_ids: list[int], for_update: bool,
) -> list[tuple[int, int]]:
    suffix = " FOR UPDATE" if for_update else ""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT game_id, player_id FROM nhl.import_skater_logs_stage "
            "WHERE game_date=%s::date AND game_id=ANY(%s) "
            f"ORDER BY game_id, player_id{suffix}", (slate_date, game_ids))
        return [(int(game_id), int(player_id)) for game_id, player_id in cursor.fetchall()]


def _natural_goalie_identities(rows: Iterable[tuple[object, ...]]) -> list[tuple[int, int]]:
    """Project staged rows to their only authoritative natural identity."""
    identities: list[tuple[int, int]] = []
    for row in rows:
        if len(row) < 2:
            raise RuntimeError("STAGED_GOALIE_IDENTITY_MALFORMED")
        try:
            identities.append((int(row[0]), int(row[1])))
        except (TypeError, ValueError) as error:
            raise RuntimeError("STAGED_GOALIE_IDENTITY_MALFORMED") from error
    return identities


def _fetch_goalie_stage_identities(
    connection: Any, *, slate_date: str, game_ids: list[int],
) -> list[tuple[int, int]]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT game_id, player_id "
            "FROM nhl.import_goalie_logs_stage "
            "WHERE game_date=%s::date AND game_id=ANY(%s) "
            "ORDER BY game_id, player_id", (slate_date, game_ids))
        return _natural_goalie_identities(cursor.fetchall())


def staging_set_preflight(
    dsn: str, evidence: dict[str, object], *,
    connection_factory: Callable[..., Any] = psycopg.connect,
) -> dict[str, object]:
    connection = connection_factory(dsn, autocommit=False)
    try:
        _begin_read_only(connection)
        game_ids = [int(value) for value in evidence["game_ids"]]
        database_games = _fetch_canonical_game_ids(connection, str(evidence["slate_date"]))
        if database_games != game_ids:
            raise RuntimeError("STAGING_SYNC_DATABASE_CANONICAL_GAME_SET_MISMATCH")
        existing_skaters = _fetch_target_skater_identities(
            connection, slate_date=str(evidence["slate_date"]), game_ids=game_ids,
            for_update=False)
        existing_goalies = _fetch_goalie_stage_identities(
            connection, slate_date=str(evidence["slate_date"]), game_ids=game_ids)
        skaters = _identity_inventory(evidence["skater_identities"], existing_skaters)
        goalies = _identity_inventory(evidence["goalie_identities"], existing_goalies)
        authorization = correction_authorization_digest(
            evidence, skaters=skaters, goalies=goalies)
        connection.rollback()
        return {
            "contract_version": PREFLIGHT_CONTRACT,
            "status": "STAGING_SET_PREFLIGHT_VALID",
            "slate_date": evidence["slate_date"],
            "canonical_games": len(game_ids),
            "required_cardinalities": _required_cardinalities(evidence),
            "skaters": skaters,
            "goalies": goalies,
            "confirmed_starter_identities": evidence["starter_identities"],
            "official_goalie_team_membership": evidence["goalie_team_membership"],
            "authorized_correction_digest": authorization,
            "source_evidence": {
                key: evidence[key] for key in (
                    "authority_source_run_id", "authority_source_journal_sha256",
                    "authority_source_tree_fingerprint", "authority_response_set_sha256",
                    "canonical_game_set_sha256", "expected_identity_set_sha256",
                )
            },
            "request_run_created": False,
            "database_transactions": 1,
            "database_writes": 0,
            "external_requests": 0,
            "bookmaker_requests": 0,
            "paid_credits": 0,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _materialize_expected_rows(connection: Any, rows: list[dict[str, object]]) -> None:
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TEMP TABLE nhl_expected_skater_stage (
                player_id bigint NOT NULL, game_id bigint NOT NULL,
                game_date date NOT NULL, team_id bigint NOT NULL,
                opponent_id bigint NOT NULL, is_home boolean NOT NULL,
                shots_on_goal integer, shot_attempts integer,
                toi_minutes numeric, pp_toi_minutes numeric,
                goals integer, assists integer, blocks integer,
                PRIMARY KEY (game_id, player_id)
            ) ON COMMIT DROP
        """)
        cursor.executemany("""
            INSERT INTO nhl_expected_skater_stage (
                player_id,game_id,game_date,team_id,opponent_id,is_home,
                shots_on_goal,shot_attempts,toi_minutes,pp_toi_minutes,
                goals,assists,blocks)
            VALUES (%s,%s,%s::date,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, [(
            row["player_id"], row["game_id"], row["game_date"], row["team_id"],
            row["opponent_id"], row["is_home"], row["shots_on_goal"],
            row["shot_attempts"], row["toi_minutes"], row["pp_toi_minutes"],
            row["goals"], row["assists"], row["blocks"],
        ) for row in rows])


def _upsert_expected_rows(connection: Any) -> int:
    with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO nhl.import_skater_logs_stage (
                player_id,game_id,game_date,team_id,opponent_id,is_home,
                shots_on_goal,shot_attempts,toi_minutes,pp_toi_minutes,
                goals,assists,blocks)
            SELECT player_id,game_id,game_date,team_id,opponent_id,is_home,
                   shots_on_goal,shot_attempts,toi_minutes,pp_toi_minutes,
                   goals,assists,blocks
            FROM nhl_expected_skater_stage
            ON CONFLICT (player_id,game_id) DO UPDATE SET
                game_date=EXCLUDED.game_date,
                team_id=EXCLUDED.team_id,
                opponent_id=EXCLUDED.opponent_id,
                is_home=EXCLUDED.is_home,
                shots_on_goal=EXCLUDED.shots_on_goal,
                shot_attempts=COALESCE(EXCLUDED.shot_attempts,
                    nhl.import_skater_logs_stage.shot_attempts),
                toi_minutes=EXCLUDED.toi_minutes,
                pp_toi_minutes=COALESCE(EXCLUDED.pp_toi_minutes,
                    nhl.import_skater_logs_stage.pp_toi_minutes),
                goals=EXCLUDED.goals,
                assists=EXCLUDED.assists,
                blocks=EXCLUDED.blocks
        """)
        return int(cursor.rowcount)


def _delete_extra_rows(
    connection: Any, *, slate_date: str, game_ids: list[int],
) -> list[tuple[int, int]]:
    with connection.cursor() as cursor:
        cursor.execute("""
            DELETE FROM nhl.import_skater_logs_stage AS target
            WHERE target.game_date=%s::date
              AND target.game_id=ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM nhl_expected_skater_stage expected
                  WHERE expected.game_id=target.game_id
                    AND expected.player_id=target.player_id
              )
            RETURNING target.game_id, target.player_id
        """, (slate_date, game_ids))
        return sorted((int(game_id), int(player_id))
                      for game_id, player_id in cursor.fetchall())


def _validate_postwrite_staging(
    evidence: dict[str, object], *, actual_skaters: list[tuple[int, int]],
    staged_goalie_rows: Iterable[tuple[object, ...]],
) -> dict[str, object]:
    official_goalies, official_starters, official_teams = _authoritative_goalie_contract(
        evidence["goalie_rows"], game_ids=evidence["game_ids"])
    if (official_goalies != [tuple(value) for value in evidence["goalie_identities"]]
            or official_starters != [tuple(value) for value in evidence["starter_identities"]]
            or official_teams != evidence["goalie_team_membership"]):
        raise RuntimeError("AUTHORITATIVE_GOALIE_EVIDENCE_MISMATCH")
    actual_goalies = _natural_goalie_identities(staged_goalie_rows)
    return validate_staging_identity_sets(
        expected_games=[int(value) for value in evidence["game_ids"]],
        expected_skaters=[tuple(value) for value in evidence["skater_identities"]],
        expected_goalies=official_goalies,
        expected_starters=official_starters,
        actual_games=sorted({game_id for game_id, unused in actual_skaters}
                            | {game_id for game_id, unused in actual_goalies}),
        actual_skaters=actual_skaters,
        actual_goalies=actual_goalies,
        # Starter identity is an official postgame determination. Staging has
        # no authoritative team or starter column and cannot override it.
        actual_starters=official_starters,
    )


def synchronize_staging_set(
    dsn: str, evidence: dict[str, object], *, authorized_extra_digest: str,
    connection_factory: Callable[..., Any] = psycopg.connect,
    failure_injector: Callable[[], None] | None = None,
) -> dict[str, object]:
    if not HEX64.fullmatch(str(authorized_extra_digest)):
        raise RuntimeError("STAGING_SYNC_AUTHORIZED_V2_DIGEST_INVALID")
    connection = connection_factory(dsn, autocommit=False)
    committed = False
    try:
        _begin_correction(connection)
        slate_date = str(evidence["slate_date"])
        game_ids = [int(value) for value in evidence["game_ids"]]
        if _fetch_canonical_game_ids(connection, slate_date) != game_ids:
            raise RuntimeError("STAGING_SYNC_DATABASE_CANONICAL_GAME_SET_MISMATCH")
        before = _fetch_target_skater_identities(
            connection, slate_date=slate_date, game_ids=game_ids, for_update=True)
        staged_goalies_before = _fetch_goalie_stage_identities(
            connection, slate_date=slate_date, game_ids=game_ids)
        skater_inventory = _identity_inventory(evidence["skater_identities"], before)
        goalie_inventory = _identity_inventory(
            evidence["goalie_identities"], staged_goalies_before)
        authorization = correction_authorization_digest(
            evidence, skaters=skater_inventory, goalies=goalie_inventory)
        if authorization != authorized_extra_digest:
            raise RuntimeError("STAGING_SYNC_AUTHORIZED_V2_DIGEST_MISMATCH")
        if (skater_inventory["duplicate_natural_keys"]
                or goalie_inventory["duplicate_natural_keys"]):
            raise RuntimeError("STAGING_SYNC_DUPLICATE_NATURAL_KEYS")

        _materialize_expected_rows(connection, list(evidence["skater_rows"]))
        upserted = _upsert_expected_rows(connection)
        deleted = _delete_extra_rows(
            connection, slate_date=slate_date, game_ids=game_ids)
        if deleted != skater_inventory["extra_identities"]:
            raise RuntimeError("STAGING_SYNC_DELETED_SET_MISMATCH")
        if failure_injector is not None:
            failure_injector()

        after = _fetch_target_skater_identities(
            connection, slate_date=slate_date, game_ids=game_ids, for_update=False)
        staged_goalies = _fetch_goalie_stage_identities(
            connection, slate_date=slate_date, game_ids=game_ids)
        equality = _validate_postwrite_staging(
            evidence, actual_skaters=after, staged_goalie_rows=staged_goalies)
        connection.commit(); committed = True
        return {
            "contract_version": CONTRACT,
            "status": "STAGING_SET_SYNCHRONIZED",
            "slate_date": slate_date,
            "authorization_contract_version": AUTHORIZATION_CONTRACT,
            "authorized_correction_digest": authorized_extra_digest,
            "before": {"skaters": skater_inventory, "goalies": goalie_inventory},
            "upserted_rows": upserted,
            "deleted_identities": deleted,
            "deleted_rows": len(deleted),
            "after_identity_set_sha256": identity_set_sha256(after),
            "staging_equality": equality,
            "transaction_committed": True,
            "request_run_created": False,
            "external_requests": 0,
            "bookmaker_requests": 0,
            "paid_credits": 0,
        }
    except Exception:
        connection.rollback()
        raise
    finally:
        if not committed:
            # Harmless after an explicit rollback and defensive for adapters.
            try:
                connection.rollback()
            except Exception:
                pass
        connection.close()
