"""Fail-closed NHL player-name parsing and external-identity resolution."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class PlayerIdentityConflict(RuntimeError):
    """A provider identity disagrees with an existing canonical binding."""


@dataclass(frozen=True)
class IdentityResolution:
    player_id: int
    provider: str
    provider_player_id: str
    disposition: str


def localized_text(value: Any) -> str | None:
    """Return a normalized plain string from NHL string/localized-object fields."""
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        default = value.get("default")
        return default.strip() if isinstance(default, str) and default.strip() else None
    return None


def classify_identity_rows(*, player_id: int, provider: str,
                           provider_player_id: str,
                           rows: list[dict[str, Any]]) -> str:
    exact = [row for row in rows
             if int(row["player_id"]) == player_id
             and str(row["provider"]) == provider
             and str(row["provider_player_id"]) == provider_player_id]
    internal_conflicts = [row for row in rows
                          if int(row["player_id"]) == player_id
                          and str(row["provider"]) == provider
                          and str(row["provider_player_id"]) != provider_player_id]
    external_conflicts = [row for row in rows
                          if str(row["provider"]) == provider
                          and str(row["provider_player_id"]) == provider_player_id
                          and int(row["player_id"]) != player_id]
    if internal_conflicts:
        raise PlayerIdentityConflict(
            f"INTERNAL_PLAYER_EXTERNAL_ID_CONFLICT:{player_id}:{provider}:"
            f"existing={internal_conflicts[0]['provider_player_id']}:attempted={provider_player_id}")
    if external_conflicts:
        raise PlayerIdentityConflict(
            f"EXTERNAL_ID_PLAYER_CONFLICT:{provider}:{provider_player_id}:"
            f"existing={external_conflicts[0]['player_id']}:attempted={player_id}")
    if len(exact) != 1:
        raise RuntimeError("PLAYER_EXTERNAL_ID_POSTCONDITION_FAILED")
    return "EXACT_IDEMPOTENT_MAPPING"


def resolve_player_external_identity(connection: Any, *, player_id: int,
                                     provider_player_id: int | str,
                                     provider: str = "nhl") -> IdentityResolution:
    """Insert-or-verify both unique identity dimensions inside a savepoint.

    ``ON CONFLICT DO NOTHING`` handles races on either unique key.  The
    post-insert read then distinguishes exact idempotency from either conflict
    direction without ever overwriting an existing binding.
    """
    internal = int(player_id)
    external = str(provider_player_id)
    if not provider or not external:
        raise ValueError("PLAYER_EXTERNAL_ID_INVALID")
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO nhl.player_external_ids
                    (player_id, provider, provider_player_id)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (internal, provider, external),
            )
            cursor.execute(
                """
                SELECT player_id, provider, provider_player_id
                FROM nhl.player_external_ids
                WHERE provider = %s
                  AND (player_id = %s OR provider_player_id = %s)
                ORDER BY player_id, provider_player_id
                FOR KEY SHARE
                """,
                (provider, internal, external),
            )
            columns = [item.name if hasattr(item, "name") else item[0]
                       for item in cursor.description]
            rows = [dict(zip(columns, row)) if not isinstance(row, dict) else row
                    for row in (cursor.fetchall() or [])]
            disposition = classify_identity_rows(
                player_id=internal, provider=provider,
                provider_player_id=external, rows=rows)
    return IdentityResolution(internal, provider, external, disposition)
