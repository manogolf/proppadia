"""PP-TOI promotion safeguards and run-bound conflict evidence."""

from __future__ import annotations

import hashlib
import json
import os
import psycopg
from pathlib import Path
from typing import Any, Iterable
from decimal import Decimal, ROUND_HALF_UP

ROOT = Path(__file__).resolve().parents[2]


def pp_toi_conflict(
    *, game_id: int, player_id: int, team_id: int | None,
    existing_value: float | None, incoming_value: float | None,
    source_kind: str, source_reference: str,
    official_team_id: int | None = None,
    official_team_mapping_count: int = 0,
) -> dict[str, Any] | None:
    """Return a conflict only when both observations are known and differ."""
    if existing_value is None or incoming_value is None:
        return None
    # skater_game_logs_raw.pp_toi_minutes is NUMERIC(5,2). A shift overlap is
    # naturally measured in seconds/60, so compare at the persisted precision
    # instead of reporting harmless float/rounding differences as conflicts.
    storage_quantum = Decimal("0.01")
    existing_stored = Decimal(str(existing_value)).quantize(
        storage_quantum, rounding=ROUND_HALF_UP
    )
    incoming_stored = Decimal(str(incoming_value)).quantize(
        storage_quantum, rounding=ROUND_HALF_UP
    )
    if existing_stored == incoming_stored:
        return None
    row = {
        "game_id": int(game_id),
        "player_id": int(player_id),
        "team_id": None if team_id is None else int(team_id),
        "team_id_namespace": "NHL_DATABASE_TEAM_ID",
        "official_team_id": None if official_team_id is None else int(official_team_id),
        "official_team_mapping_status": (
            "VERIFIED_UNIQUE_CROSSWALK" if int(official_team_mapping_count) == 1
            else "UNRESOLVED_CROSSWALK"
        ),
        "existing_pp_toi_minutes": float(existing_value),
        "incoming_pp_toi_minutes": float(incoming_value),
        "status": "PRESERVED_EXISTING_VALUE_CONFLICT_REQUIRES_SOURCE_REVIEW",
        "source_kind": str(source_kind),
        "source_reference": str(source_reference),
    }
    evidence = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
    row["comparison_evidence_sha256"] = hashlib.sha256(evidence).hexdigest()
    return row


def write_pp_toi_conflict_diagnostic(
    *, run_id: str, slate_date: str, stage: str, conflicts: Iterable[dict[str, Any]],
    source_reference: str, source_evidence_sha256: str | None = None,
    source_observation_set_sha256: str | None = None,
) -> dict[str, Any]:
    """Write a create-only JSON diagnostic bound to the owning run and source."""
    safe_run_id = "".join(c for c in str(run_id) if c.isalnum() or c in "_.:-")
    if not safe_run_id or safe_run_id != str(run_id):
        raise ValueError("INVALID_PP_TOI_DIAGNOSTIC_RUN_ID")
    rows = list(conflicts)
    payload = {
        "schema_version": "NHL_PP_TOI_SOURCE_CONFLICTS_V1",
        "run_id": safe_run_id,
        "slate_date": str(slate_date),
        "stage": str(stage),
        "source_reference": str(source_reference),
        "source_evidence_sha256": source_evidence_sha256,
        "source_observation_set_sha256": source_observation_set_sha256,
        "conflict_count": len(rows),
        "conflicts": rows,
    }
    body = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode()
    digest = hashlib.sha256(body).hexdigest()
    root = Path(os.environ.get("NHL_PP_TOI_CONFLICT_ROOT", ROOT / "artifacts/operational/nhl/pp_toi_source_conflicts"))
    directory = root / f"run_id={safe_run_id}"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stage}_{slate_date}.json"
    if path.exists():
        if path.read_bytes() != body:
            raise RuntimeError("PP_TOI_CONFLICT_DIAGNOSTIC_ALREADY_EXISTS_WITH_DIFFERENT_CONTENT")
    else:
        path.write_bytes(body)
    return {"path": str(path), "sha256": digest, "conflict_count": len(rows)}


def capture_stage_pp_toi_conflicts(
    dsn: str, *, slate_date: str, run_id: str, stage: str,
) -> dict[str, Any]:
    """Compare staged PP-TOI to populated raw values before routine promotion."""
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            WITH latest_roster AS (
              SELECT DISTINCT ON (game_id, player_id) game_id, player_id, team_id
              FROM nhl.roster_status
              WHERE game_id IN (
                SELECT game_id FROM nhl.games WHERE game_date = %s::date
              )
              ORDER BY game_id, player_id, asof_ts DESC
            )
            SELECT s.game_id::bigint, s.player_id::bigint,
                   COALESCE(rs.team_id, s.team_id)::bigint AS team_id,
                   team_map.official_team_id, team_map.mapping_count,
                   r.pp_toi_minutes, s.pp_toi_minutes
            FROM nhl.import_skater_logs_stage s
            JOIN nhl.skater_game_logs_raw r
              ON r.game_id = s.game_id AND r.player_id = s.player_id
            LEFT JOIN latest_roster rs
              ON rs.game_id = s.game_id AND rs.player_id = s.player_id
            LEFT JOIN LATERAL (
              SELECT
                CASE WHEN COUNT(DISTINCT e.provider_team_id::bigint) = 1
                     THEN MIN(e.provider_team_id::bigint) END AS official_team_id,
                COUNT(DISTINCT e.provider_team_id::bigint) AS mapping_count
              FROM nhl.team_external_ids e
              WHERE e.team_id = COALESCE(rs.team_id, s.team_id)
                AND e.provider = 'nhl'
            ) team_map ON TRUE
            WHERE s.game_date = %s::date
              AND r.pp_toi_minutes IS NOT NULL
              AND s.pp_toi_minutes IS NOT NULL
              AND r.pp_toi_minutes IS DISTINCT FROM s.pp_toi_minutes
            ORDER BY s.game_id, s.player_id;
            """,
            (slate_date, slate_date),
        )
        rows = cursor.fetchall()
    journal = os.environ.get("NHL_OFFICIAL_REQUEST_JOURNAL")
    journal_path = Path(journal) if journal else None
    journal_hash = None
    if journal_path and journal_path.is_file():
        journal_hash = hashlib.sha256(journal_path.read_bytes()).hexdigest()
    source_ref = "nhl.import_skater_logs_stage via official boxscore acquisition"
    if journal_path:
        source_ref += f"; request_journal={journal_path}"
    conflicts = [
        item for game_id, player_id, team_id, official_team_id, mapping_count, existing, incoming in rows
        if (item := pp_toi_conflict(
            game_id=game_id, player_id=player_id, team_id=team_id,
            official_team_id=official_team_id,
            official_team_mapping_count=mapping_count,
            existing_value=existing, incoming_value=incoming,
            source_kind="OFFICIAL_BOX_SCORE_STAGE",
            source_reference=source_ref,
        )) is not None
    ]
    observation_digest = hashlib.sha256(json.dumps(
        [[int(g), int(p), None if t is None else int(t),
          None if ot is None else int(ot), int(mc), float(old), float(new)]
         for g, p, t, ot, mc, old, new in rows], separators=(",", ":")
    ).encode()).hexdigest()
    return write_pp_toi_conflict_diagnostic(
        run_id=run_id, slate_date=slate_date, stage=stage,
        conflicts=conflicts, source_reference=source_ref,
        source_evidence_sha256=journal_hash,
        source_observation_set_sha256=observation_digest,
    )
