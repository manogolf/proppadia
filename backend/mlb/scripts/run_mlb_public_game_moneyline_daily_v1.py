#!/usr/bin/env python3
"""Dry-run by default: advance, score, persist, and grade public moneylines."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import urlopen

from backend.mlb.public_game_predictions.durable_store_v1 import (
    append_official_finals, append_outcome_grade, append_prediction_rows,
    append_state_snapshot, designated_snapshot_exists,
    fetch_ungraded_final_predictions, load_official_finals_before,
)
from backend.mlb.public_game_predictions.pythagorean_log5_v1 import (
    SNAPSHOT_CLASS, PublicGamePredictionError, build_official_final_grade, score_schedule_payload,
)
from backend.mlb.public_game_predictions.finality_v1 import (
    CONTRACT_VERSION as FINALITY_CONTRACT_VERSION,
    PLAYABLE_TERMINAL,
    canonical_json_bytes,
    classify_playable_terminal,
    dependency_blocked_current_games,
    receipt_sha256,
    reconcile_schedule_by_game_pk,
)
from backend.mlb.public_game_predictions.state_v1 import OfficialFinalGame, reconstruct_state
from backend.mlb.shared.current_slate_source_provenance_v1 import retain_bytes, write_receipt
from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_PATH,
)
from backend.mlb.season_transition.runtime_schedule_authority_v1 import (
    RuntimeScheduleAuthority,
    phase_authority_binding,
)

STATSAPI = "https://statsapi.mlb.com/api/v1/schedule"
GAME_FEED = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"
DEFAULT_RETAINED_SOURCE_DIR = Path("artifacts/ops/mlb_public_game_moneyline_sources")
DEFAULT_RETAINED_HISTORY_DIR = Path("artifacts/ops/mlb_public_game_moneyline_history_schedules")
DEFAULT_ATTEMPT_RECEIPT_DIR = Path("artifacts/ops/mlb_public_game_moneyline_attempts")
_ATTEMPT_CONTEXT: dict[str, Any] = {}
_CURRENT_SLATE_EVIDENCE: dict[str, Any] = {}


def _schedule_query(start_date: str, end_date: str) -> dict[str, Any]:
    return {
        "sportId": 1,
        "startDate": start_date,
        "endDate": end_date,
        "hydrate": "status,linescore,team",
    }


def _fetch_schedule(start_date: str, end_date: str) -> tuple[dict, bytes]:
    query=urlencode(_schedule_query(start_date,end_date))
    _CURRENT_SLATE_EVIDENCE["current_schedule_source_identity"] = f"{STATSAPI}?{query}"
    try:
        with urlopen(f'{STATSAPI}?{query}',timeout=30) as response:  # nosec B310: fixed official MLB host
            raw=response.read()
    except HTTPError as error:
        _CURRENT_SLATE_EVIDENCE["unparsed_current_schedule_raw"] = error.read()
        _CURRENT_SLATE_EVIDENCE["current_schedule_response_received_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        raise
    _CURRENT_SLATE_EVIDENCE["unparsed_current_schedule_raw"] = raw
    _CURRENT_SLATE_EVIDENCE["current_schedule_response_received_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return json.loads(raw),raw


def _fetch_game_feed(game_pk: int) -> tuple[dict, bytes]:
    with urlopen(GAME_FEED.format(game_pk=int(game_pk)),timeout=30) as response:  # nosec B310
        raw=response.read()
    return json.loads(raw),raw


def _utc_text(value: str) -> str:
    return datetime.fromisoformat(str(value).replace('Z','+00:00')).astimezone(timezone.utc).isoformat().replace('+00:00','Z')


def _final_effective_utc(feed: dict) -> tuple[str,str]:
    plays=((feed.get('liveData') or {}).get('plays') or {}).get('allPlays') or []
    for play in reversed(plays):
        about=play.get('about') or {}
        for field in ('endTime','startTime'):
            if about.get(field):
                return _utc_text(about[field]),f'OFFICIAL_LAST_PLAY_{field.upper()}'
    start=(((feed.get('gameData') or {}).get('datetime') or {}).get('dateTime'))
    if not start:
        raise ValueError('FINAL_EFFECTIVE_TIME_UNAVAILABLE')
    # Conservative deterministic fallback: no same-day game is admitted until
    # twelve hours after its official scheduled start when play chronology is absent.
    fallback=datetime.fromisoformat(str(start).replace('Z','+00:00')).astimezone(timezone.utc)+timedelta(hours=12)
    return fallback.isoformat().replace('+00:00','Z'),'SCHEDULED_START_PLUS_12H_CONSERVATIVE_FALLBACK'


def _retain_raw(raw: bytes, *, game_pk: int, root: Path) -> Path:
    digest=hashlib.sha256(raw).hexdigest(); target=root/str(int(game_pk))/f'{digest}.json'
    target.parent.mkdir(parents=True,exist_ok=True)
    if not target.exists():
        tmp=target.with_suffix('.tmp');tmp.write_bytes(raw);os.replace(tmp,target)
    if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:
        raise RuntimeError('RETAINED_FINAL_SOURCE_HASH_MISMATCH')
    return target


def _atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(raw)
    os.replace(tmp, path)


def _write_attempt_receipt(*, classification: str, error: BaseException | None = None) -> dict[str, str]:
    """Publish one immutable run-specific attempt receipt; never replace an existing run."""
    context = dict(_ATTEMPT_CONTEXT)
    if not context:
        return {}
    identity = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(context["run_identity"])).strip("_.-")
    if not identity:
        identity = f"moneyline_{uuid.uuid4().hex}"
    payload = {
        "schema_version": "MLB_MONEYLINE_ATTEMPT_RECEIPT_V1",
        **context,
        "classification": classification,
        "failure_type": type(error).__name__ if error is not None else None,
        "failure": str(error)[:2000] if error is not None else None,
        "finished_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
    path = DEFAULT_ATTEMPT_RECEIPT_DIR / str(context.get("mlb_date") or "unknown") / f"{identity}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.link(temp, path)
        return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}
    finally:
        temp.unlink(missing_ok=True)


def _retain_history_schedule(
    raw: bytes, *, retrieved_at_utc: str, query: dict[str, Any], root: Path,
) -> dict[str, Any]:
    digest = hashlib.sha256(raw).hexdigest()
    stamp = retrieved_at_utc.replace("-", "").replace(":", "").replace(".", "").replace("Z", "Z")
    horizon = f"{query.get('startDate','unknown')}_{query.get('endDate','unknown')}"
    target = root / str(query.get("endDate") or "provided") / f"{stamp}_{horizon}_{digest}.json"
    if not target.exists():
        _atomic_write(target, raw)
    if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
        raise RuntimeError("RETAINED_HISTORY_SCHEDULE_HASH_MISMATCH")
    return {
        "endpoint": STATSAPI,
        "query_parameters": query,
        "date_horizon": {
            "start_date": query.get("startDate"),
            "end_date": query.get("endDate"),
        },
        "retrieved_at_utc": retrieved_at_utc,
        "source_path": str(target),
        "source_sha256": digest,
    }


def _retain_current_schedule(raw: bytes, *, run_identity: str, slate_date: str,
                             source_identity: str, acquired_at_utc: str) -> dict[str, Any]:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_identity)
    path = Path("artifacts/ops/mlb_current_slate_sources") / slate_date / token / "moneyline_schedule.json"
    source = retain_bytes(path, raw)
    return {**source, "source_identity": source_identity, "requested_slate_date": slate_date,
            "acquired_at_utc": acquired_at_utc}


def _write_current_slate_receipt(*, status: str, counts: dict[str, Any]) -> dict[str, Any]:
    if not _CURRENT_SLATE_EVIDENCE:
        return {}
    evidence = _CURRENT_SLATE_EVIDENCE
    safe_identity = re.sub(r"[^A-Za-z0-9_.-]+", "_", evidence["run_identity"]).strip("_.-")
    payload = {
        "schema_version": "MLB_CURRENT_SLATE_SOURCE_PROVENANCE_V1",
        "consumer": "MONEYLINE",
        "run_identity": evidence["run_identity"],
        "requested_slate_date": evidence["requested_slate_date"],
        "acquired_at_utc": evidence["acquired_at_utc"],
        "current_schedule": evidence.get("current_schedule", {
            "source_identity": "StatsAPI current-slate schedule request",
            "path": None, "sha256": None, "status": "MISSING_OR_INVALID",
        }),
        "historical_authority_schedule": evidence.get("historical_authority_schedule"),
        "historical_authority_decision_counts": evidence.get("historical_authority_decision_counts", {}),
        "schedule_status": evidence["schedule_status"],
        "counts": counts,
        "prediction_status": status,
        "moneyline_attempt_receipt_path": str(DEFAULT_ATTEMPT_RECEIPT_DIR / evidence["requested_slate_date"] / f"{safe_identity}.json"),
    }
    result = write_receipt(Path("artifacts/ops/mlb_current_slate_provenance") /
                           evidence["requested_slate_date"] / f"moneyline__{safe_identity}.json", payload)
    evidence["receipt"] = result
    _ATTEMPT_CONTEXT["current_slate_source_provenance_path"] = result["path"]
    _ATTEMPT_CONTEXT["current_slate_source_provenance_sha256"] = result["sha256"]
    return result


@dataclass(frozen=True)
class FinalCollection:
    finals: tuple[OfficialFinalGame, ...]
    decisions: tuple[dict[str, Any], ...]
    dependency_team_ids: tuple[int, ...]
    dependency_isolation_proven: bool

    @property
    def quarantined_count(self) -> int:
        return sum(1 for item in self.decisions if item["final_decision"] == "QUARANTINED")

    @property
    def rejected_count(self) -> int:
        return sum(1 for item in self.decisions if item["final_decision"] == "REJECTED_NONPLAYABLE")


def _games(payload: dict):
    for block in payload.get('dates') or []:
        yield from block.get('games') or []


def official_final_from_feed(feed: dict, *, observed_at_utc: str,
                             source_identity: str, source_sha256: str) -> OfficialFinalGame:
    game_data=feed.get('gameData') or {}; status=game_data.get('status') or {}
    finality = classify_playable_terminal(status)
    if finality.classification != PLAYABLE_TERMINAL:
        raise PublicGamePredictionError(
            f"OFFICIAL_GAME_NOT_PLAYABLE_TERMINAL:{finality.classification}:{finality.reason}"
        )
    lines=((feed.get('liveData') or {}).get('linescore') or {}).get('teams') or {}
    home,away=(game_data.get('teams') or {}).get('home') or {},(game_data.get('teams') or {}).get('away') or {}
    effective,_=_final_effective_utc(feed)
    return OfficialFinalGame(
        game_pk=int(feed['gamePk']),game_date=str((game_data.get('datetime') or {}).get('officialDate')),
        scheduled_start_utc=str((game_data.get('datetime') or {}).get('dateTime')),
        game_number=int((game_data.get('game') or {}).get('gameNumber') or 1),
        home_team_id=int(home['id']),away_team_id=int(away['id']),
        home_runs=int((lines.get('home') or {})['runs']),away_runs=int((lines.get('away') or {})['runs']),
        official_status='Final',official_final_effective_utc=effective,
        observed_final_at_utc=observed_at_utc,source_identity=source_identity,source_sha256=source_sha256,
    )


def _feed_team_ids(feed: dict[str, Any]) -> tuple[int, ...]:
    teams = (feed.get("gameData") or {}).get("teams") or {}
    values: list[int] = []
    for side in ("away", "home"):
        try:
            values.append(int((teams.get(side) or {})["id"]))
        except (KeyError, TypeError, ValueError):
            pass
    return tuple(values)


def _feed_identity_consistent(selected: Any, feed: dict[str, Any]) -> tuple[bool, str]:
    try:
        if int(feed.get("gamePk")) != selected.game_pk:
            return False, "FEED_GAME_PK_MISMATCH"
    except (TypeError, ValueError):
        return False, "FEED_GAME_PK_MISSING"
    feed_teams = _feed_team_ids(feed)
    if len(selected.team_ids) != 2 or len(feed_teams) != 2:
        return False, "TEAM_IDENTITY_INCOMPLETE"
    if selected.team_ids != feed_teams:
        return False, "FEED_TEAM_IDENTITY_MISMATCH"
    feed_data = feed.get("gameData") or {}
    feed_date = str((feed_data.get("datetime") or {}).get("officialDate") or "")
    if feed_date == selected.official_date:
        return True, "EXACT_OFFICIAL_DATE"
    feed_start = str((feed_data.get("datetime") or {}).get("dateTime") or "")
    relation_values = set(selected.relations.values())
    if feed_start in relation_values or feed_date in relation_values:
        return True, "AUTHORITATIVE_RESCHEDULE_RELATION"
    return False, "FEED_OFFICIAL_DATE_MISMATCH_WITHOUT_RELATION"


def collect_official_finals(payload: dict, *, retained_source_dir: Path) -> FinalCollection:
    """Collect independent playable finals; status/identity failures are game-local."""
    rows: list[OfficialFinalGame] = []
    receipts: list[dict[str, Any]] = []
    dependency_teams: set[int] = set()
    isolation_proven = True
    for schedule_decision in reconcile_schedule_by_game_pk(payload):
        record = schedule_decision.as_dict()
        record.update({
            "fresh_feed_path": None,
            "fresh_feed_sha256": None,
            "fresh_feed_status": None,
            "final_decision": schedule_decision.decision,
            "final_reason": schedule_decision.reason,
        })
        if schedule_decision.decision == "QUARANTINED":
            if schedule_decision.game_pk <= 0:
                isolation_proven = False
            elif schedule_decision.dependency_team_ids:
                dependency_teams.update(schedule_decision.dependency_team_ids)
            else:
                isolation_proven = False
            receipts.append(record)
            continue
        if schedule_decision.decision != "FETCH_PLAYABLE_FINAL":
            receipts.append(record)
            continue

        selected = schedule_decision.selected
        assert selected is not None
        observed=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
        feed,raw=_fetch_game_feed(selected.game_pk)
        retained=_retain_raw(raw,game_pk=selected.game_pk,root=retained_source_dir)
        digest=hashlib.sha256(raw).hexdigest()
        feed_status=classify_playable_terminal((feed.get('gameData') or {}).get('status') or {})
        record.update({
            "fresh_feed_path": str(retained),
            "fresh_feed_sha256": digest,
            "fresh_feed_status": feed_status.as_dict(),
        })
        identity_ok, identity_reason = _feed_identity_consistent(selected, feed)
        if not feed_status.accepted or not identity_ok:
            record.update({
                "final_decision": "QUARANTINED",
                "final_reason": (
                    f"FEED_STATUS_{feed_status.classification}:{feed_status.reason}"
                    if not feed_status.accepted else identity_reason
                ),
            })
            if selected.team_ids:
                dependency_teams.update(selected.team_ids)
            else:
                isolation_proven = False
            receipts.append(record)
            continue
        rows.append(official_final_from_feed(
            feed,observed_at_utc=observed,source_identity=str(retained),source_sha256=digest,
        ))
        record.update({"final_decision": "ADMITTED_PLAYABLE_FINAL", "final_reason": identity_reason})
        receipts.append(record)
    return FinalCollection(tuple(rows),tuple(receipts),tuple(sorted(dependency_teams)),isolation_proven)


def _write_selection_receipt(
    history_source: dict[str, Any], collection: FinalCollection, *, root: Path,
    phase_binding: dict[str, Any] | None = None,
) -> dict[str, Any]:
    receipt = {
        "schema_version": "MLB_MONEYLINE_HISTORY_SELECTION_RECEIPT_V1",
        "finality_contract_version": FINALITY_CONTRACT_VERSION,
        "history_schedule": history_source,
        "dependency_isolation_proven": collection.dependency_isolation_proven,
        "dependency_team_ids": list(collection.dependency_team_ids),
        "decisions": list(collection.decisions),
        "phase_authority_binding": phase_binding,
    }
    digest = receipt_sha256(receipt)
    schedule_path = Path(history_source["source_path"])
    target = root / schedule_path.parent.name / f"{schedule_path.stem}_{digest}.selection.json"
    raw = canonical_json_bytes(receipt) + b"\n"
    if target.exists() and target.read_bytes() != raw:
        raise RuntimeError("IMMUTABLE_SELECTION_RECEIPT_CONFLICT")
    if not target.exists():
        _atomic_write(target, raw)
    return {"path": str(target), "sha256": hashlib.sha256(raw).hexdigest(), **receipt}


def _apply_dependency_blocks(
    rows: list[dict[str, Any]], blocked_game_pks: set[int], *, block_all: bool,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        if row.get("admission_status") == "ADMITTED_SHADOW" and (
            block_all or int(row.get("game_id")) in blocked_game_pks
        ):
            row = {
                **row,
                "admission_status": "REJECTED_FAIL_CLOSED",
                "failure_reason": "STRICT_PRIOR_QUARANTINED_GAME_DEPENDENCY",
            }
        result.append(row)
    return result


def _execute() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument('--mlb-date',default=date.today().isoformat())
    parser.add_argument('--prediction-cutoff-utc',default='auto')
    parser.add_argument('--schedule-json',type=Path)
    parser.add_argument('--finals-json',type=Path)
    parser.add_argument('--write-durable',action='store_true')
    parser.add_argument('--skip-if-designated-snapshot-exists',action='store_true')
    parser.add_argument('--retained-source-dir',type=Path,default=DEFAULT_RETAINED_SOURCE_DIR)
    parser.add_argument('--retained-history-dir',type=Path,default=DEFAULT_RETAINED_HISTORY_DIR)
    parser.add_argument('--output-json',type=Path)
    parser.add_argument('--run-identity', default=os.getenv('MLB_RUN_IDENTITY'))
    args=parser.parse_args()
    run_identity = args.run_identity or (
        "moneyline_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + f"_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    )
    _ATTEMPT_CONTEXT.clear()
    _CURRENT_SLATE_EVIDENCE.clear()
    _CURRENT_SLATE_EVIDENCE.update({
        "run_identity": run_identity, "requested_slate_date": args.mlb_date,
        "acquired_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "schedule_status": "MISSING_OR_INVALID",
        "current_schedule": {"source_identity": "StatsAPI current-slate schedule request",
                             "path": None, "sha256": None, "status": "MISSING_OR_INVALID"},
        "counts": {},
    })
    _ATTEMPT_CONTEXT.update({
        "run_identity": run_identity,
        "mlb_date": args.mlb_date,
        "started_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "daily_schedule_sha256": None,
        "retained_schedule_source_path": None,
        "retained_schedule_source_sha256": None,
        "active_authority_descriptor_path": None,
        "active_authority_descriptor_sha256": None,
        "active_authority_selection_sha256": None,
        "per_game_decisions": [],
        "stage": "SCHEDULE_ACQUISITION",
    })
    if args.skip_if_designated_snapshot_exists and not args.write_durable:
        parser.error('--skip-if-designated-snapshot-exists requires --write-durable')
    if args.schedule_json:
        schedule_source_identity = f"provided_file:{args.schedule_json}"
        _CURRENT_SLATE_EVIDENCE["current_schedule_source_identity"] = schedule_source_identity
        schedule_raw=args.schedule_json.read_bytes()
        _CURRENT_SLATE_EVIDENCE["unparsed_current_schedule_raw"] = schedule_raw
        _CURRENT_SLATE_EVIDENCE["current_schedule_response_received_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        schedule=json.loads(schedule_raw)
    else:
        schedule,schedule_raw=_fetch_schedule(args.mlb_date,args.mlb_date)
        schedule_source_identity = f"{STATSAPI}?{urlencode(_schedule_query(args.mlb_date,args.mlb_date))}"
    schedule_acquired_at = str(_CURRENT_SLATE_EVIDENCE.pop(
        "current_schedule_response_received_at_utc",
        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    ))
    _CURRENT_SLATE_EVIDENCE.pop("unparsed_current_schedule_raw", None)
    schedule_hash=hashlib.sha256(schedule_raw).hexdigest()
    _ATTEMPT_CONTEXT["daily_schedule_sha256"] = schedule_hash
    games_discovered=sum(1 for _ in _games(schedule))
    current_schedule = _retain_current_schedule(
        schedule_raw, run_identity=run_identity, slate_date=args.mlb_date,
        source_identity=schedule_source_identity, acquired_at_utc=schedule_acquired_at,
    )
    _CURRENT_SLATE_EVIDENCE.update({
        "run_identity": run_identity, "requested_slate_date": args.mlb_date,
        "acquired_at_utc": schedule_acquired_at, "current_schedule": current_schedule,
        "schedule_status": (
            "INVALID_SHAPE" if not isinstance(schedule, dict) or not isinstance(schedule.get("dates"), list)
            else "VALID_EMPTY" if games_discovered == 0 else "VALID_NONEMPTY"
        ),
        "counts": {"schedule_games": games_discovered},
    })
    _ATTEMPT_CONTEXT["current_schedule_source_path"] = current_schedule["path"]
    _ATTEMPT_CONTEXT["current_schedule_source_sha256"] = current_schedule["sha256"]
    inserted_finals=canonical_duplicates=0
    append_finals_after_phase_binding = False
    if args.finals_json:
        history_raw=args.finals_json.read_bytes();finals_payload=json.loads(history_raw)
        retrieved=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
        history_query={"source":"provided_file","path":str(args.finals_json),
                       "startDate":None,"endDate":args.mlb_date,"hydrate":None}
        history_source=_retain_history_schedule(
            history_raw,retrieved_at_utc=retrieved,query=history_query,root=args.retained_history_dir,
        )
        _CURRENT_SLATE_EVIDENCE["historical_authority_schedule"] = history_source
        collection=collect_official_finals(finals_payload,retained_source_dir=args.retained_source_dir)
        finals=list(collection.finals)
    elif args.write_durable:
        history,history_raw=_fetch_schedule('2026-08-05',args.mlb_date)
        retrieved=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
        history_query=_schedule_query('2026-08-05',args.mlb_date)
        history_source=_retain_history_schedule(
            history_raw,retrieved_at_utc=retrieved,query=history_query,root=args.retained_history_dir,
        )
        _CURRENT_SLATE_EVIDENCE["historical_authority_schedule"] = history_source
        collection=collect_official_finals(history,retained_source_dir=args.retained_source_dir)
        finals=list(collection.finals)
        append_finals_after_phase_binding = True
    else:
        finals=[];collection=FinalCollection((),(),(),True);history_source=None
    runtime_authority = None
    phase_binding = None
    if history_source is not None:
        _ATTEMPT_CONTEXT.update({
            "retained_schedule_source_path": history_source["source_path"],
            "retained_schedule_source_sha256": history_source["source_sha256"],
        })
        selection_raw = ACTIVE_SELECTION_PATH.read_bytes()
        active_selection = json.loads(selection_raw)
        selected_descriptor_path = str(active_selection.get("descriptor_path") or "")
        _ATTEMPT_CONTEXT.update({
            "active_authority_descriptor_path": selected_descriptor_path,
            "active_authority_descriptor_sha256": active_selection.get("descriptor_sha256"),
            "active_authority_selection_sha256": hashlib.sha256(selection_raw).hexdigest(),
            "stage": "PHASE_AUTHORITY_LOAD",
        })
        base_authority = HashedProposalAuthority()
        base_metadata = base_authority.metadata
        _ATTEMPT_CONTEXT.update({
            "retained_schedule_source_path": history_source["source_path"],
            "retained_schedule_source_sha256": history_source["source_sha256"],
            "active_authority_descriptor_path": base_metadata.snapshot_descriptor_path,
            "active_authority_descriptor_sha256": base_metadata.snapshot_descriptor_sha256,
            "active_authority_selection_sha256": hashlib.sha256(selection_raw).hexdigest(),
            "stage": "PHASE_AUTHORITY_INGESTION",
        })
        runtime_authority = RuntimeScheduleAuthority(
            source_path=Path(history_source["source_path"]),
            expected_source_sha256=str(history_source["source_sha256"]),
            base=base_authority,
        )
        phase_binding = phase_authority_binding(runtime_authority)
        _CURRENT_SLATE_EVIDENCE["historical_authority_decision_counts"] = {
            phase: sum(1 for decision in phase_binding["decisions"]
                       if decision.get("normalized_phase") == phase)
            for phase in ("REGULAR_SEASON", "POSTSEASON")
        }
        metadata = runtime_authority.metadata
        _ATTEMPT_CONTEXT.update({
            "retained_schedule_source_path": history_source["source_path"],
            "retained_schedule_source_sha256": history_source["source_sha256"],
            "active_authority_descriptor_path": metadata.snapshot_descriptor_path,
            "active_authority_descriptor_sha256": metadata.snapshot_descriptor_sha256,
            "per_game_decisions": phase_binding["decisions"],
            "stage": "PHASE_AUTHORITY_READY",
        })
    if append_finals_after_phase_binding:
        _ATTEMPT_CONTEXT["stage"] = "OFFICIAL_FINALS_PERSISTENCE"
        inserted_finals=append_official_finals(finals)
        canonical_duplicates=len(finals)-inserted_finals
    selection_receipt=(None if history_source is None else _write_selection_receipt(
        history_source,collection,root=args.retained_history_dir,
        phase_binding=phase_binding,
    ))
    cutoff=(datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
            if str(args.prediction_cutoff_utc).lower()=='auto' else _utc_text(args.prediction_cutoff_utc))
    generated=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    if args.write_durable:
        finals=load_official_finals_before(cutoff)
    snapshot=reconstruct_state(finals,prediction_cutoff_utc=cutoff,state_generated_at_utc=generated)
    grading_rows_written=0
    grading_rows_eligible=0
    if args.write_durable:
        _ATTEMPT_CONTEXT["stage"] = "MONEYLINE_GRADING_READ_AND_GATE"
        grade_inputs=fetch_ungraded_final_predictions(cutoff, authority=runtime_authority)
        grading_rows_eligible=len(grade_inputs)
        for item in grade_inputs:
            grade=build_official_final_grade(
                item['prediction'],official_home_runs=item['official_home_runs'],
                official_away_runs=item['official_away_runs'],
                official_source_path=item['official_source_identity'],
                official_source_sha256=item['official_source_sha256'],
                grading_timestamp_utc=generated,
            )
            _ATTEMPT_CONTEXT["stage"] = "MONEYLINE_GRADING_PERSISTENCE"
            grading_rows_written+=int(append_outcome_grade(grade, authority=runtime_authority))
    skip_scoring=(args.skip_if_designated_snapshot_exists and
                  designated_snapshot_exists(args.mlb_date,SNAPSHOT_CLASS))
    if skip_scoring:
        result={'mode':'DURABLE_WRITE','mlb_date':args.mlb_date,
                'model_version':'MLB_GAME_PYTHAGOREAN_LOG5_V1',
                'prediction_snapshot_class':SNAPSHOT_CLASS,'status':'SKIPPED',
                'skip_reason':'SUCCESSFUL_DESIGNATED_SNAPSHOT_EXISTS',
                'prediction_cutoff_utc':cutoff,'source_schedule_hash':schedule_hash,
                'state_hash':snapshot['state_hash'],
                'state_through_game_date':snapshot['state_through_game_date'],
                'official_finals_considered':len(finals),
                'official_finals_inserted':inserted_finals,
                'canonical_final_duplicates':canonical_duplicates,
                'games_discovered':games_discovered,
                'grading_rows_eligible':grading_rows_eligible,
                'grading_rows_written':grading_rows_written,
                'predictions_written':0,'outcomes_accessed':grading_rows_eligible,
                'history_schedule_source':history_source,
                'current_slate_source_provenance':_write_current_slate_receipt(
                    status="SKIPPED_EXISTING_DESIGNATED_SNAPSHOT",
                    counts={"schedule_games": games_discovered,
                            "prediction_scoring": "NOT_RUN_SKIPPED_EXISTING_DESIGNATED_SNAPSHOT"},
                ),
                'history_selection_receipt':None if selection_receipt is None else {
                    'path':selection_receipt['path'],'sha256':selection_receipt['sha256']},
                'playable_terminal_contract':FINALITY_CONTRACT_VERSION,
                'history_games_quarantined':collection.quarantined_count,
                'history_games_rejected_nonplayable':collection.rejected_count,
                'dependency_blocked_predictions':0,'post_start_predictions':0,
                'agreement_barrier_status':'VALID_EXISTING_IMMUTABLE_SNAPSHOT'}
        text=json.dumps(result,indent=2)
        receipt = _write_attempt_receipt(classification="COMPLETED")
        result["moneyline_attempt_receipt"] = receipt
        text=json.dumps(result,indent=2)
        if args.output_json:
            args.output_json.parent.mkdir(parents=True,exist_ok=True)
            _atomic_write(args.output_json,(text+'\n').encode())
        print(text)
        return 0
    rows=score_schedule_payload(schedule,prediction_timestamp_utc=cutoff,
                                source_schedule_hash=schedule_hash,team_state_snapshot=snapshot)
    block_all=not collection.dependency_isolation_proven
    blocked_game_pks=dependency_blocked_current_games(
        schedule,collection.dependency_team_ids,block_all=block_all,
    )
    rows=_apply_dependency_blocks(rows,blocked_game_pks,block_all=block_all)
    admitted=[row for row in rows if row['admission_status']=='ADMITTED_SHADOW']
    decision_counts: dict[str, int] = {}
    for row in rows:
        reason = str(row.get('failure_reason') or 'ADMITTED')
        decision_counts[reason] = decision_counts.get(reason, 0) + 1
    current_counts = {
        "schedule_games": games_discovered, "scored_rows": len(rows),
        "admitted_rows": len(admitted), "rejected_rows": len(rows) - len(admitted),
        "decision_reasons": decision_counts,
    }
    provenance = _write_current_slate_receipt(
        status=("VALID_EMPTY" if games_discovered == 0 else
                "NO_ADMITTED_PREDICTIONS" if not admitted else "PREDICTIONS_ADMITTED"),
        counts=current_counts,
    )
    _CURRENT_SLATE_EVIDENCE["counts"] = current_counts
    state_written=predictions_written=0
    if args.write_durable:
        _ATTEMPT_CONTEXT["stage"] = "MONEYLINE_PREDICTION_PERSISTENCE"
        state_written=int(append_state_snapshot(snapshot))
        predictions_written=append_prediction_rows(admitted, authority=runtime_authority)
    result={'mode':'DURABLE_WRITE' if args.write_durable else 'DRY_RUN','mlb_date':args.mlb_date,
            'prediction_generated_at_utc':generated,
            'prediction_cutoff_utc':cutoff,'source_schedule_hash':schedule_hash,
            'state_hash':snapshot['state_hash'],'state_through_game_date':snapshot['state_through_game_date'],
            'official_finals_considered':len(finals),'games_newly_applied':len(snapshot['applied_game_ids']),
            'official_finals_inserted':inserted_finals,'canonical_final_duplicates':canonical_duplicates,
            'genuine_corrections':0,'unresolved_games':snapshot['unresolved_games'],'rows':rows,'admitted':len(admitted),
            'games_discovered':games_discovered,
            'state_snapshot_written':state_written,'predictions_written':predictions_written,
            'grading_rows_eligible':grading_rows_eligible,'grading_rows_written':grading_rows_written,
            'outcomes_accessed':grading_rows_eligible,
            'history_schedule_source':history_source,
            'current_slate_source_provenance':provenance,
            'history_selection_receipt':None if selection_receipt is None else {
                'path':selection_receipt['path'],'sha256':selection_receipt['sha256']},
            'playable_terminal_contract':FINALITY_CONTRACT_VERSION,
            'history_games_quarantined':collection.quarantined_count,
            'history_games_rejected_nonplayable':collection.rejected_count,
            'dependency_isolation_proven':collection.dependency_isolation_proven,
            'dependency_blocked_predictions':sum(
                1 for row in rows if row.get('failure_reason')=='STRICT_PRIOR_QUARANTINED_GAME_DEPENDENCY'),
            'post_start_predictions':sum(
                1 for row in rows if row.get('failure_reason')=='PREGAME_CUTOFF_FAILED'),
            'agreement_barrier_status':(
                'VALID_IMMUTABLE_MONEYLINE_ARTIFACT' if admitted else 'INVALID_NO_ADMITTED_PREDICTIONS'),
            'publication_counts':{
                'admitted':len(admitted),'quarantined':collection.quarantined_count,
                'dependency_blocked':sum(
                    1 for row in rows if row.get('failure_reason')=='STRICT_PRIOR_QUARANTINED_GAME_DEPENDENCY'),
                'post_start':sum(1 for row in rows if row.get('failure_reason')=='PREGAME_CUTOFF_FAILED'),
            }}
    text=json.dumps(result,indent=2)
    receipt = _write_attempt_receipt(classification="COMPLETED")
    result["moneyline_attempt_receipt"] = receipt
    text=json.dumps(result,indent=2)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True,exist_ok=True)
        _atomic_write(args.output_json,(text+'\n').encode())
    print(text)
    return 0


def main() -> int:
    try:
        return _execute()
    except BaseException as error:
        classification = str(_ATTEMPT_CONTEXT.get("stage") or "MONEYLINE_STAGE_EXCEPTION")
        if "PhaseGate" in type(error).__name__ or "AUTHORITY" in str(error):
            classification = "MONEYLINE_PHASE_GATE_FAILURE"
        elif "PERSISTENCE" in classification:
            classification = "MONEYLINE_PERSISTENCE_FAILURE"
        try:
            unparsed_raw = _CURRENT_SLATE_EVIDENCE.pop("unparsed_current_schedule_raw", None)
            if unparsed_raw is not None:
                retained = _retain_current_schedule(
                    unparsed_raw, run_identity=str(_CURRENT_SLATE_EVIDENCE["run_identity"]),
                    slate_date=str(_CURRENT_SLATE_EVIDENCE["requested_slate_date"]),
                    source_identity=str(_CURRENT_SLATE_EVIDENCE.get(
                        "current_schedule_source_identity",
                        "current-slate schedule source retained before JSON validation",
                    )),
                    acquired_at_utc=str(_CURRENT_SLATE_EVIDENCE.get("current_schedule_response_received_at_utc") or ""),
                )
                retained["status"] = "RETAINED_BUT_INVALID"
                _CURRENT_SLATE_EVIDENCE["current_schedule"] = retained
                _CURRENT_SLATE_EVIDENCE["schedule_status"] = "INVALID_SHAPE_OR_HTTP_RESPONSE"
            if _CURRENT_SLATE_EVIDENCE and "receipt" not in _CURRENT_SLATE_EVIDENCE:
                _write_current_slate_receipt(
                    status="FAILED_OR_INCOMPLETE",
                    counts=_CURRENT_SLATE_EVIDENCE.get("counts", {}),
                )
            receipt = _write_attempt_receipt(classification=classification, error=error)
            if receipt:
                print(f"MLB_MONEYLINE_ATTEMPT_RECEIPT={receipt['path']} sha256={receipt['sha256']}", file=sys.stderr)
        except Exception as receipt_error:
            print(f"MLB_MONEYLINE_FAILURE_RECEIPT_WRITE_FAILED={type(receipt_error).__name__}:{receipt_error}", file=sys.stderr)
        raise


if __name__=='__main__': raise SystemExit(main())
