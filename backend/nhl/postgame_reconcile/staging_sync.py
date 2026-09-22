"""Authoritative, transaction-scoped NHL postgame skater-stage synchronization.

This module has no network entry point and does not create request runs.  Its
read-only preflight and write correction both consume an already retained,
receipt-verified official authority response run.
"""
from __future__ import annotations

import hashlib
import json
import re
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
PREFLIGHT_CONTRACT = "NHL_AUTHORITATIVE_SKATER_STAGING_PREFLIGHT_V1"
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


def _goalie_starters(goalies: list[dict[str, object]]) -> list[tuple[int, int]]:
    by_team: dict[tuple[int, int], list[tuple[float, int]]] = {}
    for row in goalies:
        by_team.setdefault((int(row["game_id"]), int(row["team_id"])), []).append(
            (float(row["toi_minutes"] or 0.0), int(row["player_id"])))
    return sorted((game_id, max(values)[1])
                  for (game_id, unused_team), values in by_team.items())


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
        for side, is_home in (("homeTeam", True), ("awayTeam", False)):
            team_id = (payload.get(side) or {}).get("id")
            other = "awayTeam" if side == "homeTeam" else "homeTeam"
            opponent_id = (payload.get(other) or {}).get("id")
            if team_id is None or opponent_id is None or int(team_id) == int(opponent_id):
                raise RuntimeError(f"AUTHORITATIVE_STAGING_TEAM_IDENTITY_INVALID:{game_id}")
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
        if len(skater_keys) != len(set(skater_keys)) or len(goalie_keys) != len(set(goalie_keys)):
            raise RuntimeError(f"AUTHORITATIVE_STAGING_DUPLICATE_IDENTITY:{game_id}")
        if set(skater_keys) & set(goalie_keys):
            raise RuntimeError(f"AUTHORITATIVE_STAGING_ROLE_CONFLICT:{game_id}")
        skaters.extend(game_skaters); goalies.extend(game_goalies)
        per_game[str(game_id)] = {"skaters": len(skater_keys), "goalies": len(goalie_keys)}

    skater_identities = sorted((int(row["game_id"]), int(row["player_id"]))
                                for row in skaters)
    goalie_identities = sorted((int(row["game_id"]), int(row["player_id"]))
                               for row in goalies)
    starters = _goalie_starters(goalies)
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
        "goalie_identities": goalie_identities,
        "starter_identities": starters,
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
    expected_list = [(int(game_id), int(player_id))
                     for game_id, player_id in evidence["skater_identities"]]
    existing_list = [(int(game_id), int(player_id))
                     for game_id, player_id in existing_identities]
    if len(existing_list) != len(set(existing_list)):
        raise RuntimeError("STAGING_SYNC_EXISTING_DUPLICATE_IDENTITY")
    expected, existing = set(expected_list), set(existing_list)
    missing, extra = sorted(expected - existing), sorted(existing - expected)
    authorization = authorized_extra_set_digest(
        slate_date=str(evidence["slate_date"]),
        canonical_game_set_sha256=str(evidence["canonical_game_set_sha256"]),
        expected_identity_set_sha256=str(evidence["expected_identity_set_sha256"]),
        extra_identities=extra,
    )
    return {
        "existing_identities": sorted(existing),
        "missing_identities": missing,
        "extra_identities": extra,
        "existing_identity_set_sha256": identity_set_sha256(existing),
        "missing_identity_set_sha256": identity_set_sha256(missing),
        "extra_identity_set_sha256": identity_set_sha256(extra),
        "authorized_extra_set_digest": authorization,
    }


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


def _fetch_goalie_stage_rows(
    connection: Any, *, slate_date: str, game_ids: list[int],
) -> list[tuple[int, int, int, float]]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT game_id, player_id, team_id, COALESCE(toi_minutes,0) "
            "FROM nhl.import_goalie_logs_stage "
            "WHERE game_date=%s::date AND game_id=ANY(%s) "
            "ORDER BY game_id, player_id", (slate_date, game_ids))
        return [(int(game_id), int(player_id), int(team_id), float(toi))
                for game_id, player_id, team_id, toi in cursor.fetchall()]


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
        existing = _fetch_target_skater_identities(
            connection, slate_date=str(evidence["slate_date"]), game_ids=game_ids,
            for_update=False)
        difference = diff_staging_identities(evidence, existing)
        connection.rollback()
        return {
            "contract_version": PREFLIGHT_CONTRACT,
            "status": "STAGING_SET_PREFLIGHT_VALID",
            "slate_date": evidence["slate_date"],
            "canonical_games": len(game_ids),
            "expected_skater_appearances": len(evidence["skater_identities"]),
            "existing_skater_appearances": len(difference["existing_identities"]),
            "missing_skater_appearances": len(difference["missing_identities"]),
            "extra_skater_appearances": len(difference["extra_identities"]),
            **difference,
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


def _actual_starters(
    goalie_rows: list[tuple[int, int, int, float]],
    expected_goalies: set[tuple[int, int]],
) -> list[tuple[int, int]]:
    by_team: dict[tuple[int, int], list[tuple[float, int]]] = {}
    for game_id, player_id, team_id, toi in goalie_rows:
        if (game_id, player_id) in expected_goalies:
            by_team.setdefault((game_id, team_id), []).append((toi, player_id))
    return sorted((game_id, max(values)[1])
                  for (game_id, unused_team), values in by_team.items())


def synchronize_staging_set(
    dsn: str, evidence: dict[str, object], *, authorized_extra_digest: str,
    connection_factory: Callable[..., Any] = psycopg.connect,
    failure_injector: Callable[[], None] | None = None,
) -> dict[str, object]:
    if not HEX64.fullmatch(str(authorized_extra_digest)):
        raise RuntimeError("STAGING_SYNC_AUTHORIZED_EXTRA_DIGEST_INVALID")
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
        difference = diff_staging_identities(evidence, before)
        if difference["authorized_extra_set_digest"] != authorized_extra_digest:
            raise RuntimeError("STAGING_SYNC_AUTHORIZED_EXTRA_SET_DIGEST_MISMATCH")

        _materialize_expected_rows(connection, list(evidence["skater_rows"]))
        upserted = _upsert_expected_rows(connection)
        deleted = _delete_extra_rows(
            connection, slate_date=slate_date, game_ids=game_ids)
        if deleted != difference["extra_identities"]:
            raise RuntimeError("STAGING_SYNC_DELETED_SET_MISMATCH")
        if failure_injector is not None:
            failure_injector()

        after = _fetch_target_skater_identities(
            connection, slate_date=slate_date, game_ids=game_ids, for_update=False)
        goalie_rows = _fetch_goalie_stage_rows(
            connection, slate_date=slate_date, game_ids=game_ids)
        actual_goalies = [(game_id, player_id)
                          for game_id, player_id, unused_team, unused_toi in goalie_rows]
        expected_goalies = {tuple(value) for value in evidence["goalie_identities"]}
        equality = validate_staging_identity_sets(
            expected_games=game_ids,
            expected_skaters=[tuple(value) for value in evidence["skater_identities"]],
            expected_goalies=sorted(expected_goalies),
            expected_starters=[tuple(value) for value in evidence["starter_identities"]],
            actual_games=sorted({game_id for game_id, unused in after}
                                | {game_id for game_id, unused in actual_goalies}),
            actual_skaters=after,
            actual_goalies=actual_goalies,
            actual_starters=_actual_starters(goalie_rows, expected_goalies),
        )
        connection.commit(); committed = True
        return {
            "contract_version": CONTRACT,
            "status": "STAGING_SET_SYNCHRONIZED",
            "slate_date": slate_date,
            "authorized_extra_set_digest": authorized_extra_digest,
            "before": difference,
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
