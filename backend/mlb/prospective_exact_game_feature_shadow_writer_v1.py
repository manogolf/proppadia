"""Prospective, file-backed exact-player/game feature shadow writer V1.

This module is deliberately isolated from production feature consumers.  Its
database adapter opens one REPEATABLE READ, READ ONLY transaction and emits an
immutable file package.  The pure builder and file backend are dependency
injectable so their chronology and idempotency rules can be tested offline.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import tempfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.mlb.exact_game_features.contract_v1 import (
    CONTRACT_VERSION,
    EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE,
    ExactGameAuthorityV1,
    ExactGameContractError,
    ExactGameFeatureStateV1,
    SourceObservationV1,
    canonical_json_bytes,
    coalesce_exact_states,
    content_sha256,
)
from backend.mlb.exact_game_features.offline_builder_v1 import (
    ExactGameTargetV1,
    HistoricalExactGameFactV1,
    OfflineExactGameCandidateBuilderV1,
)
from backend.mlb.identity.playable_terminal_v1 import reconcile_schedule_by_game_pk
from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
)


WRITER_CONTRACT = "MLB_2026_PROSPECTIVE_EXACT_GAME_FEATURE_SHADOW_WRITER_V1"
FILE_BACKEND = "IMMUTABLE_CREATE_ONLY_FILE_BACKEND_V1"
SUPPORTED_PHASES = frozenset({"REGULAR_SEASON", "POSTSEASON"})
OUTPUT_ROOT = Path("artifacts/ops/mlb_exact_game_feature_shadow_v1")
METRIC_FIELDS = (
    "plate_appearances",
    "at_bats",
    "hits",
    "total_bases",
    "rbis",
    "runs_scored",
    "strikeouts_batting",
    "walks",
    "home_runs",
    "stolen_bases",
    "strikeouts_pitching",
    "walks_allowed",
    "hits_allowed",
    "outs_recorded",
    "earned_runs",
)
ADMITTED_FIELDS = (
    "player_id",
    "game_pk",
    "official_date",
    "scheduled_start_utc",
    "season_phase",
    "authority",
    "feature_input_cutoff_utc",
    "source_observations",
    "eligible_prior_game_pks",
    "feature_payload",
    "feature_payload_sha256",
    "row_sha256",
    "contract_version",
)
REJECTED_FIELDS = (
    "game_pk",
    "player_id",
    "official_date",
    "scheduled_start_utc",
    "reason",
    "detail",
)


class ShadowWriterError(RuntimeError):
    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(code if not detail else f"{code}:{detail}")


def utc(value: str | datetime) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        raise ShadowWriterError("TIMESTAMP_INVALID", str(value)) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowWriterError("TIMESTAMP_NOT_UTC_AWARE", str(value))
    return parsed.astimezone(timezone.utc)


def iso(value: str | datetime) -> str:
    return utc(value).isoformat().replace("+00:00", "Z")


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json_line(value: Any) -> bytes:
    return canonical_json_bytes(_json_value(value)) + b"\n"


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def rows_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(b"".join(canonical_json_line(dict(row)) for row in rows)).hexdigest()


def query_identity(sql: str) -> str:
    normalized = " ".join(sql.split())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SnapshotRowsV1:
    cutoff_utc: str
    isolation_level: str
    read_only: bool
    roster_rows: tuple[Mapping[str, Any], ...]
    player_stat_rows: tuple[Mapping[str, Any], ...]
    roster_query_sha256: str
    player_stats_query_sha256: str


@dataclass(frozen=True)
class BuildResultV1:
    admitted: tuple[Mapping[str, Any], ...]
    rejected: tuple[Mapping[str, Any], ...]
    source_manifest: tuple[Mapping[str, Any], ...]
    receipt: Mapping[str, Any]
    summary: Mapping[str, Any]


ROSTER_SQL = """
WITH source AS (
  SELECT player_id, to_jsonb(p) AS payload
  FROM mlb.player_ids p
)
SELECT player_id,
       payload ->> 'player_name' AS player_name,
       payload ->> 'team' AS team,
       payload ->> 'team_id' AS team_id,
       payload ->> 'active' AS active,
       payload ->> 'status' AS status,
       payload ->> 'updated_at' AS updated_at
FROM source
WHERE CASE
        WHEN payload ? 'active' THEN COALESCE((payload ->> 'active')::boolean, false)
        WHEN payload ? 'status' THEN lower(COALESCE(payload ->> 'status', '')) = 'active'
        ELSE true
      END
ORDER BY player_id
"""

PLAYER_STATS_SQL = """
WITH source AS (
  SELECT player_id, game_id, to_jsonb(ps) AS payload
  FROM mlb.player_stats ps
  WHERE game_id IS NOT NULL
    AND player_id IS NOT NULL
    AND game_date >= DATE '2026-01-01'
    AND game_date < DATE '2027-01-01'
)
SELECT player_id, game_id,
       payload ->> 'game_date' AS game_date,
       payload ->> 'team' AS team,
       payload ->> 'opponent' AS opponent,
       payload ->> 'is_home' AS is_home,
       payload ->> 'position' AS position,
       payload ->> 'plate_appearances' AS plate_appearances,
       payload ->> 'at_bats' AS at_bats,
       payload ->> 'hits' AS hits,
       payload ->> 'total_bases' AS total_bases,
       payload ->> 'rbis' AS rbis,
       payload ->> 'runs_scored' AS runs_scored,
       payload ->> 'strikeouts_batting' AS strikeouts_batting,
       payload ->> 'walks' AS walks,
       payload ->> 'home_runs' AS home_runs,
       payload ->> 'stolen_bases' AS stolen_bases,
       payload ->> 'strikeouts_pitching' AS strikeouts_pitching,
       payload ->> 'walks_allowed' AS walks_allowed,
       payload ->> 'hits_allowed' AS hits_allowed,
       payload ->> 'outs_recorded' AS outs_recorded,
       payload ->> 'earned_runs' AS earned_runs,
       payload ->> 'updated_at' AS updated_at
FROM source
WHERE game_id IS NOT NULL AND player_id IS NOT NULL
ORDER BY player_id, game_id
"""


def load_read_only_snapshot(dsn: str) -> SnapshotRowsV1:
    """Read both sources in one explicit snapshot; never request a txid."""
    import psycopg
    from psycopg.rows import dict_row

    with psycopg.connect(dsn, row_factory=dict_row, autocommit=False, prepare_threshold=0) as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cur.execute("SHOW transaction_isolation")
            isolation = str(cur.fetchone()["transaction_isolation"])
            cur.execute("SHOW transaction_read_only")
            read_only = str(cur.fetchone()["transaction_read_only"]).lower() == "on"
            if isolation.lower() != "repeatable read" or not read_only:
                raise ShadowWriterError("DATABASE_SNAPSHOT_NOT_REPEATABLE_READ_ONLY")
            # statement_timestamp is not a transaction-id request.  This first
            # snapshot-visible statement establishes the immutable cutoff.
            cur.execute("SELECT statement_timestamp() AS cutoff_utc")
            cutoff = iso(cur.fetchone()["cutoff_utc"])
            cur.execute(ROSTER_SQL)
            rosters = tuple(dict(row) for row in cur.fetchall())
            cur.execute(PLAYER_STATS_SQL)
            stats = tuple(dict(row) for row in cur.fetchall())
            conn.rollback()
    return SnapshotRowsV1(
        cutoff_utc=cutoff,
        isolation_level=isolation,
        read_only=read_only,
        roster_rows=rosters,
        player_stat_rows=stats,
        roster_query_sha256=query_identity(ROSTER_SQL),
        player_stats_query_sha256=query_identity(PLAYER_STATS_SQL),
    )


def _schedule_games(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    games: list[Mapping[str, Any]] = []
    for date_row in payload.get("dates", []) or []:
        for game in date_row.get("games", []) or []:
            if isinstance(game, Mapping):
                games.append(game)
    return games


def _team(game: Mapping[str, Any], side: str) -> tuple[int | None, str]:
    item = ((game.get("teams") or {}).get(side) or {}).get("team") or {}
    try:
        team_id = int(item.get("id"))
    except (TypeError, ValueError):
        team_id = None
    return team_id, str(item.get("abbreviation") or "").strip().upper()


def _relationships(game: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "doubleHeader",
        "gameNumber",
        "rescheduleDate",
        "rescheduledFrom",
        "resumeDate",
        "resumeGameDate",
        "seriesGameNumber",
    )
    return {key: game[key] for key in keys if game.get(key) not in (None, "")}


def _game_status_rejection(game: Mapping[str, Any]) -> str | None:
    status = game.get("status") or {}
    abstract = str(status.get("abstractGameState") or "").strip().lower()
    detailed = str(status.get("detailedState") or "").strip().lower()
    coded = str(status.get("codedGameState") or "").strip().upper()
    if any(token in detailed for token in ("postpon", "cancel", "suspend")):
        return "NON_PLAYABLE_SCHEDULE_STATUS"
    if abstract not in {"preview", ""} or coded in {"D", "C"}:
        return "TARGET_NOT_PREGAME"
    return None


def _game_sort_key(game: Mapping[str, Any]) -> tuple[str, int, str]:
    raw = game.get("gamePk")
    try:
        number = int(raw)
    except (TypeError, ValueError):
        number = 0
    return str(game.get("gameDate") or ""), number, str(raw or "")


def _observation(row: Mapping[str, Any], cutoff: datetime, kind: str, source_hash: str) -> SourceObservationV1:
    observed = row.get("updated_at")
    if observed in (None, ""):
        raise ShadowWriterError("SOURCE_OBSERVATION_TIME_MISSING", kind)
    observed_utc = utc(observed)
    if observed_utc > cutoff:
        raise ShadowWriterError("SOURCE_OBSERVED_POST_CUTOFF", kind)
    return SourceObservationV1.create(
        source_path=f"database/{kind}/snapshot/{source_hash}",
        source_sha256=source_hash,
        observed_at_utc=observed_utc,
        source_kind=kind,
    )


def _numeric(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)


def _feature_payload(facts: Sequence[HistoricalExactGameFactV1]) -> dict[str, Any]:
    ordered = sorted(facts, key=lambda item: (item.authority.scheduled_start_utc, item.authority.game_pk))
    payload: dict[str, Any] = {
        "feature_family": "STRICT_PRIOR_EXACT_GAME_SUMMARY_V1",
        "prior_game_count": len(ordered),
        "prior_game_pks": [item.authority.game_pk for item in ordered],
    }
    for window in (7, 15, 30):
        window_rows = ordered[-window:]
        for metric in METRIC_FIELDS:
            values = [_numeric(item.fact_payload.get(metric)) for item in window_rows]
            payload[f"prior_{window}_{metric}_mean"] = None if not values else round(sum(values) / len(values), 12)
    return payload


def build_shadow(
    *,
    slate_date: str,
    run_identity: str,
    wrapper_started_at_utc: str,
    snapshot: SnapshotRowsV1,
    schedule_payload: Mapping[str, Any],
    schedule_source: Mapping[str, Any],
    phase_authority: CanonicalGamePhaseAuthority,
    code_commit: str,
    contract_sha256: str,
    interpreter: str,
) -> BuildResultV1:
    cutoff = utc(snapshot.cutoff_utc)
    started = utc(wrapper_started_at_utc)
    retrieved = utc(str(schedule_source.get("retrieved_at_utc") or ""))
    if retrieved < started or retrieved > cutoff:
        raise ShadowWriterError("SCHEDULE_NOT_CURRENT_RUN_PRE_CUTOFF")
    if not snapshot.read_only or snapshot.isolation_level.lower() != "repeatable read":
        raise ShadowWriterError("DATABASE_SNAPSHOT_NOT_REPEATABLE_READ_ONLY")

    roster_rows = tuple(_json_value(row) for row in snapshot.roster_rows)
    player_stat_rows = tuple(_json_value(row) for row in snapshot.player_stat_rows)
    roster_hash = rows_sha256(roster_rows)
    stats_hash = rows_sha256(player_stat_rows)
    schedule_games = _schedule_games(schedule_payload)
    schedule_observation = SourceObservationV1.create(
        source_path=str(schedule_source["source_path"]),
        source_sha256=str(schedule_source["source_sha256"]),
        observed_at_utc=retrieved,
        source_kind="IMMUTABLE_HISTORY_SCHEDULE",
    )
    final_by_game = {
        decision.game_pk: decision.selected
        for decision in reconcile_schedule_by_game_pk(schedule_payload)
        if decision.decision == "FETCH_PLAYABLE_FINAL" and decision.selected is not None
    }
    roster_by_team_id: dict[int, list[Mapping[str, Any]]] = {}
    roster_by_team: dict[str, list[Mapping[str, Any]]] = {}
    for row in roster_rows:
        try:
            player_id = int(row.get("player_id"))
        except (TypeError, ValueError):
            continue
        if player_id <= 0:
            continue
        try:
            team_id = int(row.get("team_id"))
            roster_by_team_id.setdefault(team_id, []).append(row)
        except (TypeError, ValueError):
            pass
        team = str(row.get("team") or "").strip().upper()
        if team:
            roster_by_team.setdefault(team, []).append(row)

    facts: list[HistoricalExactGameFactV1] = []
    facts_by_player: dict[int, list[HistoricalExactGameFactV1]] = {}
    fact_rejections: Counter[str] = Counter()
    for row in player_stat_rows:
        try:
            player_id = int(row.get("player_id"))
            game_pk = int(row.get("game_id"))
            final_appearance = final_by_game.get(game_pk)
            if final_appearance is None:
                fact_rejections["HISTORICAL_GAME_NOT_PLAYABLE_TERMINAL_AT_CUTOFF"] += 1
                continue
            authority = ExactGameAuthorityV1.from_canonical_interface(
                phase_authority,
                game_pk=game_pk,
                official_date=final_appearance.official_date,
                scheduled_start_utc=final_appearance.game_date_utc,
                schedule_relationships=final_appearance.relations,
            )
            if authority.season_phase not in SUPPORTED_PHASES:
                fact_rejections["UNSUPPORTED_HISTORICAL_PHASE"] += 1
                continue
            observation = _observation(row, cutoff, "mlb.player_stats", stats_hash)
            fact = HistoricalExactGameFactV1.create(
                player_id=player_id,
                authority=authority,
                terminal_observed_at_utc=schedule_observation.observed_at_utc,
                source_observations=(schedule_observation, observation),
                fact_payload={field: row.get(field) for field in METRIC_FIELDS},
            )
        except (ExactGameContractError, GamePhaseAuthorityError, ShadowWriterError, TypeError, ValueError) as exc:
            fact_rejections[getattr(exc, "code", "HISTORICAL_FACT_INVALID")] += 1
            continue
        facts.append(fact)
        facts_by_player.setdefault(player_id, []).append(fact)

    targets: list[ExactGameTargetV1] = []
    target_rows: dict[tuple[int, int], tuple[ExactGameAuthorityV1, SourceObservationV1]] = {}
    rejected: list[dict[str, Any]] = []
    games_seen = 0
    for game in sorted(schedule_games, key=_game_sort_key):
        official = str(game.get("officialDate") or game.get("gameDate") or "")[:10]
        if official != slate_date:
            continue
        games_seen += 1
        try:
            game_pk = int(game.get("gamePk"))
            start = utc(str(game.get("gameDate") or ""))
        except (TypeError, ValueError, ShadowWriterError):
            rejected.append({"game_pk": game.get("gamePk"), "player_id": None, "official_date": official, "scheduled_start_utc": game.get("gameDate"), "reason": "TARGET_GAME_IDENTITY_INVALID", "detail": ""})
            continue
        reason = _game_status_rejection(game)
        if reason:
            rejected.append({"game_pk": game_pk, "player_id": None, "official_date": official, "scheduled_start_utc": iso(start), "reason": reason, "detail": ""})
            continue
        if start <= cutoff:
            rejected.append({"game_pk": game_pk, "player_id": None, "official_date": official, "scheduled_start_utc": iso(start), "reason": "TARGET_GAME_STARTED_AT_OR_BEFORE_CUTOFF", "detail": ""})
            continue
        try:
            authority = ExactGameAuthorityV1.from_canonical_interface(
                phase_authority,
                game_pk=game_pk,
                official_date=official,
                scheduled_start_utc=start,
                schedule_relationships=_relationships(game),
            )
        except ExactGameContractError as exc:
            rejected.append({"game_pk": game_pk, "player_id": None, "official_date": official, "scheduled_start_utc": iso(start), "reason": exc.code, "detail": exc.detail})
            continue
        if authority.season_phase not in SUPPORTED_PHASES:
            rejected.append({"game_pk": game_pk, "player_id": None, "official_date": official, "scheduled_start_utc": iso(start), "reason": "UNSUPPORTED_TARGET_PHASE", "detail": authority.season_phase})
            continue
        players: dict[int, Mapping[str, Any]] = {}
        for side in ("away", "home"):
            team_id, abbr = _team(game, side)
            candidates = roster_by_team_id.get(team_id or -1, []) or roster_by_team.get(abbr, [])
            for row in candidates:
                players[int(row["player_id"])] = row
        if not players:
            rejected.append({"game_pk": game_pk, "player_id": None, "official_date": official, "scheduled_start_utc": iso(start), "reason": "EXACT_ACTIVE_ROSTER_MISSING", "detail": ""})
            continue
        for player_id, roster_row in sorted(players.items()):
            try:
                roster_observation = _observation(roster_row, cutoff, "mlb.player_ids", roster_hash)
            except ShadowWriterError as exc:
                rejected.append({"game_pk": game_pk, "player_id": player_id, "official_date": official, "scheduled_start_utc": iso(start), "reason": exc.code, "detail": exc.detail})
                continue
            target = ExactGameTargetV1.create(player_id=player_id, authority=authority, feature_input_cutoff_utc=cutoff)
            targets.append(target)
            target_rows[target.identity] = (authority, roster_observation)

    proposals = OfflineExactGameCandidateBuilderV1().build(facts=facts, targets=targets)
    admitted_states: list[ExactGameFeatureStateV1] = []
    admitted: list[dict[str, Any]] = []
    for proposal in proposals:
        identity = (proposal.player_id, proposal.game_pk)
        authority, roster_observation = target_rows[identity]
        if proposal.classification != EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE:
            rejected.append({"game_pk": proposal.game_pk, "player_id": proposal.player_id, "official_date": proposal.official_date, "scheduled_start_utc": proposal.scheduled_start_utc, "reason": proposal.reason, "detail": proposal.classification})
            continue
        eligible_ids = set(proposal.eligible_prior_game_pks)
        eligible_facts = [fact for fact in facts_by_player.get(proposal.player_id, []) if fact.authority.game_pk in eligible_ids]
        observations = [schedule_observation, roster_observation]
        observations.extend(source for fact in eligible_facts for source in fact.source_observations)
        unique_observations = {content_sha256(item.to_dict()): item for item in observations}
        state = ExactGameFeatureStateV1.create(
            player_id=proposal.player_id,
            authority=authority,
            feature_input_cutoff_utc=cutoff,
            source_observations=tuple(unique_observations[key] for key in sorted(unique_observations)),
            feature_payload=_feature_payload(eligible_facts),
        )
        admitted_states.append(state)
        admitted.append({
            "player_id": state.player_id,
            "game_pk": state.authority.game_pk,
            "official_date": state.authority.official_date.isoformat(),
            "scheduled_start_utc": iso(state.authority.scheduled_start_utc),
            "season_phase": state.authority.season_phase,
            "authority": state.authority.to_dict(),
            "feature_input_cutoff_utc": iso(state.feature_input_cutoff_utc),
            "source_observations": [item.to_dict() for item in state.source_observations],
            "eligible_prior_game_pks": list(proposal.eligible_prior_game_pks),
            "feature_payload": dict(state.feature_payload),
            "feature_payload_sha256": state.feature_payload_sha256,
            "row_sha256": state.row_sha256,
            "contract_version": state.contract_version,
        })
    coalesce_exact_states(admitted_states)
    admitted.sort(key=lambda row: (int(row["game_pk"]), int(row["player_id"])))
    rejected.sort(key=lambda row: (int(row["game_pk"] or 0), int(row["player_id"] or 0), str(row["reason"])))

    metadata = phase_authority.metadata
    source_manifest = (
        {"source_kind": "IMMUTABLE_HISTORY_SCHEDULE", "path": str(schedule_source["source_path"]), "sha256": str(schedule_source["source_sha256"]), "observed_at_utc": iso(str(schedule_source["retrieved_at_utc"])), "row_count": len(schedule_games)},
        {"source_kind": "IMMUTABLE_HISTORY_SELECTION_RECEIPT", "path": str(schedule_source["selection_receipt_path"]), "sha256": str(schedule_source["selection_receipt_sha256"]), "observed_at_utc": iso(str(schedule_source["retrieved_at_utc"])), "row_count": len(final_by_game)},
        {"source_kind": "READ_ONLY_ROSTER_SNAPSHOT", "path": f"database/mlb.player_ids/query/{snapshot.roster_query_sha256}", "sha256": roster_hash, "observed_at_utc": iso(cutoff), "row_count": len(snapshot.roster_rows)},
        {"source_kind": "READ_ONLY_EXACT_GAME_FACT_SNAPSHOT", "path": f"database/mlb.player_stats/query/{snapshot.player_stats_query_sha256}", "sha256": stats_hash, "observed_at_utc": iso(cutoff), "row_count": len(snapshot.player_stat_rows)},
        {"source_kind": "CANONICAL_PHASE_AUTHORITY_DESCRIPTOR", "path": metadata.snapshot_descriptor_path or metadata.proposal_path, "sha256": metadata.snapshot_descriptor_sha256 or metadata.proposal_sha256, "observed_at_utc": iso(cutoff), "row_count": metadata.proposal_count},
    )
    reasons = Counter(str(row["reason"]) for row in rejected)
    receipt = {
        "contract": WRITER_CONTRACT,
        "feature_contract": CONTRACT_VERSION,
        "file_backend": FILE_BACKEND,
        "slate_date": slate_date,
        "run_identity": run_identity,
        "wrapper_started_at_utc": iso(started),
        "feature_input_cutoff_utc": iso(cutoff),
        "transaction": {"isolation_level": snapshot.isolation_level.upper().replace(" ", "_"), "read_only": snapshot.read_only, "transaction_id_requested": False, "database_writes": 0},
        "source_queries": {
            "roster_query_sha256": snapshot.roster_query_sha256,
            "roster_row_count": len(snapshot.roster_rows),
            "roster_rows_sha256": roster_hash,
            "player_stats_query_sha256": snapshot.player_stats_query_sha256,
            "player_stats_row_count": len(snapshot.player_stat_rows),
            "player_stats_rows_sha256": stats_hash,
        },
        "population": {
            "schedule_games_examined": games_seen,
            "target_pairs_considered": len(targets),
            "admitted_rows": len(admitted),
            "rejected_rows": len(rejected),
            "admitted_games": len({row["game_pk"] for row in admitted}),
            "admitted_players": len({row["player_id"] for row in admitted}),
            "target_game_pks": sorted({target.authority.game_pk for target in targets}),
            "target_player_ids": sorted({target.player_id for target in targets}),
            "exact_game_pks": sorted({int(row["game_pk"]) for row in admitted}),
            "exact_player_ids": sorted({int(row["player_id"]) for row in admitted}),
        },
        "population_hashes": {
            "admitted_rows_sha256": rows_sha256(admitted),
            "rejected_rows_sha256": rows_sha256(rejected),
        },
        "authority": {"interface": metadata.authority_interface, "backend": metadata.backend, "snapshot_id": metadata.snapshot_id, "descriptor_path": metadata.snapshot_descriptor_path, "descriptor_sha256": metadata.snapshot_descriptor_sha256, "proposal_sha256": metadata.proposal_sha256},
        "schedule_source": dict(schedule_source),
        "code_commit": code_commit,
        "contract_sha256": contract_sha256,
        "interpreter": interpreter,
        "production_consumers": 0,
        "production_writes": 0,
        "network_requests": 0,
        "paid_credits": 0,
    }
    summary = {
        "contract": WRITER_CONTRACT,
        "status": "SHADOW_SNAPSHOT_READY",
        "slate_date": slate_date,
        "run_identity": run_identity,
        "feature_input_cutoff_utc": iso(cutoff),
        "admitted_rows": len(admitted),
        "rejected_rows": len(rejected),
        "rejection_reasons": dict(sorted(reasons.items())),
        "historical_fact_rejections": dict(sorted(fact_rejections.items())),
        "shadow_only": True,
        "production_barrier": False,
    }
    return BuildResultV1(tuple(admitted), tuple(rejected), source_manifest, receipt, summary)


def _serialize_csv(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    import io
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(fields), lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        item = dict(row)
        for key, value in list(item.items()):
            if isinstance(value, (dict, list, tuple)):
                item[key] = json.dumps(value, sort_keys=True, separators=(",", ":"))
        writer.writerow(item)
    return output.getvalue().encode("utf-8")


def package_bytes(result: BuildResultV1) -> dict[str, bytes]:
    files = {
        "snapshot_receipt.json": canonical_json_line(dict(result.receipt)),
        "admitted_candidates.csv": _serialize_csv(result.admitted, ADMITTED_FIELDS),
        "rejected_candidates.csv": _serialize_csv(result.rejected, REJECTED_FIELDS),
        "source_manifest.jsonl": b"".join(canonical_json_line(row) for row in result.source_manifest),
        "summary.json": canonical_json_line(dict(result.summary)),
    }
    manifest = {
        "contract": WRITER_CONTRACT,
        "manifest_version": 1,
        "files": {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())},
    }
    files["SHA256SUMS.json"] = canonical_json_line(manifest)
    return files


def _existing_matches(directory: Path, files: Mapping[str, bytes]) -> bool:
    expected = set(files)
    actual = {path.name for path in directory.iterdir() if path.is_file()}
    return actual == expected and all((directory / name).read_bytes() == data for name, data in files.items())


def publish_create_only(*, root: Path, slate_date: str, run_identity: str, result: BuildResultV1) -> tuple[str, Path]:
    for value, code in ((slate_date, "SLATE_DATE_PATH_INVALID"), (run_identity, "RUN_IDENTITY_PATH_INVALID")):
        if not value or Path(value).name != value or value in {".", ".."}:
            raise ShadowWriterError(code, value)
    final = root / slate_date / run_identity
    files = package_bytes(result)
    if final.exists():
        if final.is_dir() and _existing_matches(final, files):
            return "SHADOW_SNAPSHOT_ALREADY_EXISTS_IDENTICAL", final
        raise ShadowWriterError("IMMUTABLE_RUN_IDENTITY_CONFLICT", str(final))
    parent = final.parent
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    os.chmod(parent, 0o700)
    temp = Path(tempfile.mkdtemp(prefix=f".{run_identity}.", dir=parent))
    os.chmod(temp, 0o700)
    try:
        for name, data in files.items():
            path = temp / name
            with path.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(path, 0o600)
        try:
            os.rename(temp, final)
        except FileExistsError:
            if final.is_dir() and _existing_matches(final, files):
                shutil.rmtree(temp)
                return "SHADOW_SNAPSHOT_ALREADY_EXISTS_IDENTICAL", final
            raise ShadowWriterError("IMMUTABLE_RUN_IDENTITY_CONFLICT", str(final))
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return "SHADOW_SNAPSHOT_CREATED", final
    except Exception:
        if temp.exists():
            shutil.rmtree(temp)
        raise


def verify_package(directory: Path) -> dict[str, Any]:
    manifest = json.loads((directory / "SHA256SUMS.json").read_text())
    failures = []
    for name, expected in manifest.get("files", {}).items():
        path = directory / name
        if not path.is_file() or sha256_path(path) != expected:
            failures.append(name)
    return {"status": "PASS" if not failures else "FAIL", "failures": failures, "file_count": len(manifest.get("files", {}))}
