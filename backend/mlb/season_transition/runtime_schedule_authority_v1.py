"""Exact-game phase overlay from a retained, already-fetched schedule response.

The selected file authority remains immutable. This adapter only supplies exact
postseason identities absent from that snapshot, binding each such decision to
the retained response bytes and active descriptor used for the run.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from backend.mlb.season_transition.canonical_phase_v1 import canonical_phase_record
from backend.mlb.season_transition.contract_v1 import (
    GAME_TYPE_CONTRACT,
    PhaseContractError,
    classify_schedule_game,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT


class RuntimeScheduleAuthority(CanonicalGamePhaseAuthority):
    """Read-only exact-PK overlay whose novel entries are schedule-hash-bound."""

    def __init__(
        self,
        *,
        source_path: Path,
        expected_source_sha256: str,
        base: CanonicalGamePhaseAuthority | None = None,
    ) -> None:
        self.base = base or HashedProposalAuthority()
        path = Path(source_path).resolve()
        try:
            self.source_path = path.relative_to(REPO_ROOT.resolve()).as_posix()
        except ValueError:
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_SOURCE_OUTSIDE_REPOSITORY") from None
        raw = path.read_bytes()
        actual = hashlib.sha256(raw).hexdigest()
        if actual != expected_source_sha256:
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_SOURCE_HASH_MISMATCH")
        self.source_sha256 = actual
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_SOURCE_MALFORMED") from None
        if not isinstance(payload, dict) or not isinstance(payload.get("dates"), list):
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_DATES_MISSING")
        self._overlay: dict[int, GamePhaseAuthorityRecord] = {}
        self._all: dict[int, Mapping[str, Any]] = {}
        grouped: dict[int, list[Mapping[str, Any]]] = {}
        for block in payload["dates"]:
            if not isinstance(block, dict) or not isinstance(block.get("games", []), list):
                raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_GAMES_INVALID")
            for game in block.get("games", []):
                if not isinstance(game, dict):
                    raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_GAME_INVALID")
                try:
                    pk = int(game.get("gamePk"))
                except (TypeError, ValueError):
                    raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_GAME_PK_MISSING") from None
                if pk <= 0:
                    raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_GAME_PK_INVALID", game_pk=pk)
                nested_pk = (((game.get("gameData") or {}).get("game") or {}).get("pk"))
                if nested_pk not in (None, ""):
                    try:
                        if int(nested_pk) != pk:
                            raise GamePhaseAuthorityError(
                                "RUNTIME_SCHEDULE_GAME_PK_CONFLICT", game_pk=pk
                            )
                    except (TypeError, ValueError):
                        raise GamePhaseAuthorityError(
                            "RUNTIME_SCHEDULE_GAME_PK_CONFLICT", game_pk=pk
                        ) from None
                grouped.setdefault(pk, []).append(game)
        for pk, game_rows in grouped.items():
            game = self._select_schedule_identity(pk, game_rows)
            self._all[pk] = game
            try:
                classification = classify_schedule_game(game)
            except PhaseContractError as exc:
                raise GamePhaseAuthorityError(
                    "RUNTIME_SCHEDULE_IDENTITY_INVALID", game_pk=pk, detail=str(exc)
                ) from None
            if classification.season != self.metadata.supported_season:
                continue
            try:
                existing = self.base.lookup_exact(pk)
            except GamePhaseAuthorityError as exc:
                if exc.code not in {"GAME_PHASE_ABSENT", "GAME_PHASE_SPECIAL_EXCLUDED"}:
                    raise
                existing = None
            if existing is not None:
                if (
                    existing.source_season != classification.season
                    or existing.source_game_type != classification.raw_game_type
                    or existing.season_phase != classification.phase
                    or existing.postseason_round != classification.postseason_round
                    or existing.source_round != classification.source_round
                ):
                    raise GamePhaseAuthorityError(
                        "RUNTIME_SCHEDULE_AUTHORITY_IDENTITY_CONFLICT", game_pk=pk
                    )
                continue
            if classification.phase != "POSTSEASON":
                continue
            self._validate_new_postseason_game(game, pk)
            try:
                row = canonical_phase_record(
                    game, source_sha256=actual, source_path=self.source_path
                )
                start = str(game.get("gameDate") or "")
                parsed = datetime.fromisoformat(start.replace("Z", "+00:00"))
                if parsed.tzinfo is None or parsed.year != classification.season:
                    raise ValueError("scheduled start season invalid")
            except (PhaseContractError, TypeError, ValueError) as exc:
                raise GamePhaseAuthorityError(
                    "RUNTIME_SCHEDULE_POSTSEASON_IDENTITY_INVALID", game_pk=pk,
                    detail=str(exc),
                ) from None
            self._overlay[pk] = GamePhaseAuthorityRecord(
                game_pk=pk,
                source_season=int(row["source_season"]),
                source_game_type=str(row["source_game_type"]),
                season_phase=str(row["season_phase"]),
                postseason_round=str(row["postseason_round"]),
                season_name=str(row["season_name"]),
                source_round=str(row["source_round"]),
                schedule_relationships=dict(row["schedule_relationships"]),
                primary_source_path=self.source_path,
                primary_source_sha256=actual,
                source_paths=(self.source_path,),
                source_hashes=(actual,),
                phase_decision=str(row["phase_decision"]),
                authority_status="AUTHORITATIVE_UNAMBIGUOUS",
                scheduled_start_utc=start,
            )

    @staticmethod
    def _select_schedule_identity(pk: int, rows: list[Mapping[str, Any]]) -> Mapping[str, Any]:
        if len(rows) == 1:
            return rows[0]
        identities = set()
        starts: list[str] = []
        for row in rows:
            try:
                classification = classify_schedule_game(row)
            except PhaseContractError as exc:
                raise GamePhaseAuthorityError(
                    "RUNTIME_SCHEDULE_IDENTITY_INVALID", game_pk=pk, detail=str(exc)
                ) from None
            identities.add((classification.season, classification.raw_game_type,
                            classification.phase, classification.postseason_round,
                            classification.source_round))
            starts.append(str(row.get("gameDate") or ""))
        if len(identities) != 1 or any(not value for value in starts):
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_DUPLICATE_GAME_PK", game_pk=pk)
        ordered = sorted(zip(starts, rows), key=lambda item: item[0])
        for (prior_start, _), (later_start, later_row) in zip(ordered, ordered[1:]):
            relation = str(later_row.get("rescheduledFrom") or "")
            if relation != prior_start:
                raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_DUPLICATE_GAME_PK", game_pk=pk)
        return ordered[-1][1]

    @staticmethod
    def _validate_new_postseason_game(game: Mapping[str, Any], pk: int) -> None:
        if not str(game.get("gameType") or "").strip() or game.get("season") in (None, ""):
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_IDENTITY_INCOMPLETE", game_pk=pk)
        spec = GAME_TYPE_CONTRACT.get(str(game.get("gameType")))
        if not spec or spec.get("phase") != "POSTSEASON":
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_GAME_TYPE_UNSUPPORTED", game_pk=pk)
        if not str(game.get("seriesDescription") or "").strip():
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_ROUND_MISSING", game_pk=pk)
        status = game.get("status")
        if not isinstance(status, dict):
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_STATUS_MISSING", game_pk=pk)
        abstract = str(status.get("abstractGameState") or "")
        coded = str(status.get("codedGameState") or "")
        detailed = str(status.get("detailedState") or "")
        status_code = str(status.get("statusCode") or "")
        accepted = {
            ("Preview", "S", "S"), ("Preview", "P", "P"),
            ("Live", "I", "I"), ("Live", "P", "PW"),
            ("Final", "F", "F"), ("Final", "O", "O"),
        }
        if not detailed or (abstract, coded, status_code) not in accepted:
            raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_STATUS_CONFLICT", game_pk=pk)
        if game.get("gameData"):
            nested = (game.get("gameData") or {}).get("game") or {}
            if nested.get("type") not in (None, "", game.get("gameType")):
                raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_GAME_TYPE_CONFLICT", game_pk=pk)
            if nested.get("season") not in (None, ""):
                try:
                    if int(nested["season"]) != int(game.get("season")):
                        raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_SEASON_CONFLICT", game_pk=pk)
                except (TypeError, ValueError):
                    raise GamePhaseAuthorityError("RUNTIME_SCHEDULE_SEASON_CONFLICT", game_pk=pk) from None

    @property
    def metadata(self):
        return self.base.metadata

    def lookup_exact(self, game_pk: Any) -> GamePhaseAuthorityRecord:
        try:
            return self.base.lookup_exact(game_pk)
        except GamePhaseAuthorityError as exc:
            if exc.code not in {"GAME_PHASE_ABSENT", "GAME_PHASE_SPECIAL_EXCLUDED"}:
                raise
        try:
            exact = int(game_pk)
        except (TypeError, ValueError):
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING") from None
        record = self._overlay.get(exact)
        if record is None:
            return self.base.lookup_exact(exact)
        return record

    def is_runtime_exact(self, game_pk: int) -> bool:
        return int(game_pk) in self._overlay

    def require_exact_game_date(self, game_pk: Any, game_date: Any) -> None:
        """Permit dates beyond the pinned snapshot only for an exact sourced PK."""
        try:
            exact = int(game_pk)
        except (TypeError, ValueError):
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING") from None
        source_game = self._all.get(exact)
        if source_game is not None and str(source_game.get("officialDate") or "") == str(game_date):
            return
        inherited = getattr(self.base, "require_exact_game_date", None)
        if inherited is not None:
            inherited(exact, game_date)
            return
        if source_game is None or str(source_game.get("officialDate") or "") != str(game_date):
            raise GamePhaseAuthorityError(
                "GAME_PHASE_AUTHORITY_STALE", game_pk=exact,
                detail=f"schedule_date_mismatch={game_date}",
            )


def source_bound_schedule_decisions(
    authority: RuntimeScheduleAuthority,
) -> list[dict[str, Any]]:
    """Return a deterministic per-game source-bound authority inventory."""
    decisions: list[dict[str, Any]] = []
    for pk in sorted(authority._all):
        try:
            record = authority.lookup_exact(pk)
        except GamePhaseAuthorityError as exc:
            game = authority._all[pk]
            spec = GAME_TYPE_CONTRACT.get(str(game.get("gameType") or ""))
            special = (
                exc.code == "GAME_PHASE_SPECIAL_EXCLUDED"
                or bool(spec is not None and spec.get("phase") is None)
            )
            decisions.append({
                "game_pk": pk,
                "source_season": game.get("season"),
                "source_game_type": game.get("gameType"),
                "normalized_phase": None,
                "postseason_round": None,
                "source_round": game.get("seriesDescription"),
                "authority_decision": (
                    "EXCLUDED_SPECIAL_AUTHORITATIVE_TYPE"
                    if special else "REJECTED_NO_EXACT_GAME_AUTHORITY"
                ),
                "failure_code": None if special else exc.code,
                "authority_proposal_sha256": authority.metadata.proposal_sha256,
                "authority_records_sha256": authority.metadata.authority_records_sha256,
                "authority_descriptor_sha256": authority.metadata.snapshot_descriptor_sha256,
                "schedule_source_path": authority.source_path,
                "schedule_source_sha256": authority.source_sha256,
            })
            continue
        decisions.append({
            "game_pk": pk,
            "source_season": record.source_season,
            "source_game_type": record.source_game_type,
            "normalized_phase": record.season_phase,
            "postseason_round": record.postseason_round,
            "source_round": record.source_round,
            "authority_decision": "EXACT_GAME_AUTHORITY_ACCEPTED",
            "authority_proposal_sha256": authority.metadata.proposal_sha256,
            "authority_records_sha256": authority.metadata.authority_records_sha256,
            "authority_descriptor_sha256": authority.metadata.snapshot_descriptor_sha256,
            "schedule_source_path": authority.source_path,
            "schedule_source_sha256": authority.source_sha256,
            "game_identity_source_sha256": record.primary_source_sha256,
        })
    return decisions


def phase_authority_binding(authority: RuntimeScheduleAuthority) -> dict[str, Any]:
    metadata = authority.metadata
    return {
        "contract_name": "MLB_2026_RUN_SCHEDULE_PHASE_AUTHORITY_BINDING_V1",
        "active_authority_descriptor_path": metadata.snapshot_descriptor_path,
        "active_authority_descriptor_sha256": metadata.snapshot_descriptor_sha256,
        "authority_proposal_sha256": metadata.proposal_sha256,
        "authority_records_sha256": metadata.authority_records_sha256,
        "schedule_source_path": authority.source_path,
        "schedule_source_sha256": authority.source_sha256,
        "decisions": source_bound_schedule_decisions(authority),
    }
