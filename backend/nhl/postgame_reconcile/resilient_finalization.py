"""Outcome-independent selection of immutable pregame prediction evidence.

This module deliberately has no access to game results. Lane adapters must
validate each source artifact and its receipt/certificate lineage before
passing it to the selector.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Iterable


CONTRACT = "NHL_RESILIENT_PREGAME_FINALIZATION_V1"

LANE_PHASES = {
    "MONEYLINE": frozenset({"MIDDAY", "FINAL_PREGAME", "REFRESH"}),
    "PUCK_LINE": frozenset({"MIDDAY", "FINAL_PREGAME", "REFRESH"}),
    "SOG": frozenset({"MIDDAY", "FINAL_PREGAME"}),
    "POINTS": frozenset({"MIDDAY", "FINAL_PREGAME"}),
    "SAVES": frozenset({"MIDDAY", "FINAL_PREGAME"}),
}


def canonical_atomic_identity(lane: str, row: dict[str, Any]) -> str:
    """Return a role/model-specific prediction group key, excluding phase/run id."""
    lane = lane.upper()
    game_id = int(row["game_id"])
    if lane in {"MONEYLINE", "PUCK_LINE"}:
        identity = [lane, game_id, str(row["model_role"]),
                    str(row["model_identity"]), str(row["feature_contract_identity"])]
    elif lane == "SOG":
        identity = [lane, game_id, int(row["player_id"]), str(row["contract_arm"]),
                    str(row["contract_sha256"])]
    elif lane == "POINTS":
        identity = [lane, game_id, int(row["player_id"]), str(row["model_version"]),
                    str(row["probability_construction"])]
    elif lane == "SAVES":
        identity = [lane, game_id, int(row["goalie_id"]), str(row["model_version"]),
                    str(row["prediction_semantics"])]
    else:
        raise RuntimeError(f"RESILIENT_FINALIZATION_LANE_UNSUPPORTED:{lane}")
    return hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()


def validate_atomic_group(lane: str, rows: list[dict[str, Any]]) -> None:
    """Require a complete same-observation ladder/distribution for one group."""
    lane = lane.upper()
    if not rows:
        raise RuntimeError("RESILIENT_FINALIZATION_ATOMIC_GROUP_EMPTY")
    phases = {str(row.get("phase")) for row in rows}
    timestamps = {str(row.get("prediction_timestamp_utc") or
                       row.get("prediction_creation_time_utc")) for row in rows}
    run_ids = {str(row.get("run_id") or row.get("source_run_id")) for row in rows}
    if len(phases) != 1 or len(timestamps) != 1 or len(run_ids) != 1:
        raise RuntimeError("RESILIENT_FINALIZATION_ATOMIC_GROUP_MIXED_SOURCE")
    phase = next(iter(phases))
    if phase not in LANE_PHASES.get(lane, frozenset()):
        raise RuntimeError(f"RESILIENT_FINALIZATION_PHASE_INELIGIBLE:{lane}:{phase}")
    lines = [float(row["line"]) for row in rows]
    expected = ({0.5, 1.5, 2.5} if lane == "POINTS"
                else {18.5 + value for value in range(13)} if lane == "SAVES"
                else None)
    if expected is not None and (len(lines) != len(set(lines)) or set(lines) != expected):
        raise RuntimeError(f"RESILIENT_FINALIZATION_ATOMIC_LINE_SET_INVALID:{lane}")


def _utc(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise RuntimeError(f"RESILIENT_FINALIZATION_TIMESTAMP_INVALID:{field}") from error
    if parsed.tzinfo is None:
        raise RuntimeError(f"RESILIENT_FINALIZATION_TIMESTAMP_NOT_UTC:{field}")
    return parsed.astimezone(timezone.utc)


def select_latest_atomic_sources(
    *, lane: str, slate_date: str, canonical_game_ids: Iterable[int],
    scheduled_starts_utc: dict[int, str], candidates: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Select one latest qualifying source per atomic prediction identity.

    Candidate adapters are responsible for validating manifests, completion
    markers, model/feature contracts, observer receipts, reconciliation
    certificates, and lane-specific roster/source evidence. This function
    validates chronology and selection ambiguity without seeing outcomes.
    """
    supplied_games = [int(game_id) for game_id in canonical_game_ids]
    games = sorted(set(supplied_games))
    if len(games) != len(supplied_games):
        raise RuntimeError("RESILIENT_FINALIZATION_CANONICAL_GAME_DUPLICATE")
    if set(games) != {int(game_id) for game_id in scheduled_starts_utc}:
        raise RuntimeError("RESILIENT_FINALIZATION_SCHEDULE_GAME_SET_MISMATCH")
    starts = {int(game_id): _utc(value, "scheduled_start")
              for game_id, value in scheduled_starts_utc.items()}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    excluded: list[dict[str, Any]] = []
    for candidate in candidates:
        game_id = int(candidate.get("game_id", -1))
        key = str(candidate.get("atomic_prediction_identity") or "")
        reason = None
        if game_id not in starts:
            reason = "GAME_NOT_IN_CANONICAL_SLATE"
        elif not key:
            reason = "ATOMIC_PREDICTION_IDENTITY_MISSING"
        elif candidate.get("eligible") is not True:
            reason = str(candidate.get("exclusion_reason") or "SOURCE_NOT_QUALIFIED")
        else:
            required_proof = (
                "source_path", "source_manifest_sha256", "observer_receipt_path",
                "observer_receipt_sha256", "certificate_sha256", "model_identity",
                "feature_contract_identity", "roster_source_sha256",
                "prediction_payload_sha256", "source_phase",
            )
            absent = [name for name in required_proof if not candidate.get(name)]
            if absent:
                reason = "SOURCE_PROOF_MISSING:" + ",".join(absent)
        if reason is None:
            feature_at = _utc(candidate.get("feature_cutoff_timestamp_utc"), "feature_cutoff")
            published_at = _utc(candidate.get("durable_publication_timestamp_utc"), "durable_publication")
            if feature_at >= starts[game_id]:
                reason = "FEATURE_CUTOFF_NOT_PREGAME"
            elif published_at >= starts[game_id]:
                reason = "PUBLICATION_NOT_PREGAME"
            elif feature_at > published_at:
                reason = "FEATURE_CUTOFF_AFTER_PUBLICATION"
        if reason:
            excluded.append({"game_id": game_id, "atomic_prediction_identity": key or None,
                             "reason": reason,
                             "source_path": candidate.get("source_path")})
            continue
        grouped[key].append(candidate)

    selected: list[dict[str, Any]] = []
    for key, options in sorted(grouped.items()):
        latest = max(_utc(item["durable_publication_timestamp_utc"], "durable_publication")
                     for item in options)
        tied = [item for item in options
                if _utc(item["durable_publication_timestamp_utc"], "durable_publication") == latest]
        # Identical content at a shared timestamp is deterministically resolved
        # by source path. Different content is ambiguous and fails closed.
        content_hashes = {str(item.get("prediction_payload_sha256") or "") for item in tied}
        if len(tied) > 1 and ("" in content_hashes or len(content_hashes) != 1):
            raise RuntimeError(f"RESILIENT_FINALIZATION_CONFLICTING_TIE:{key}")
        winner = min(tied, key=lambda item: str(item.get("source_path") or ""))
        selected.append({
            "game_id": int(winner["game_id"]),
            "atomic_prediction_identity": key,
            "source_phase": str(winner["source_phase"]),
            "model_identity": winner.get("model_identity"),
            "feature_contract_identity": winner.get("feature_contract_identity"),
            "source_path": str(winner["source_path"]),
            "source_manifest_sha256": str(winner["source_manifest_sha256"]),
            "observer_receipt_path": str(winner["observer_receipt_path"]),
            "observer_receipt_sha256": str(winner["observer_receipt_sha256"]),
            "certificate_sha256": str(winner["certificate_sha256"]),
            "roster_source_sha256": winner.get("roster_source_sha256"),
            "feature_cutoff_timestamp_utc": str(winner["feature_cutoff_timestamp_utc"]),
            "durable_publication_timestamp_utc": str(winner["durable_publication_timestamp_utc"]),
            "prediction_payload_sha256": str(winner["prediction_payload_sha256"]),
            "tie_break": "LEXICOGRAPHIC_SOURCE_PATH_IDENTICAL_PAYLOAD" if len(tied) > 1 else None,
        })
    selected_keys = {item["atomic_prediction_identity"] for item in selected}
    for game_id in games:
        if not any(item["game_id"] == game_id for item in selected):
            excluded.append({"game_id": game_id, "atomic_prediction_identity": None,
                             "reason": "NO_QUALIFIED_PREGAME_PREDICTION_SOURCE",
                             "source_path": None})
    return {
        "contract_version": CONTRACT,
        "evaluation_population": "RESILIENT_PREGAME",
        "lane": lane,
        "slate_date": slate_date,
        "canonical_game_ids": games,
        "selected_atomic_prediction_count": len(selected),
        "selected": selected,
        "excluded": sorted(excluded, key=lambda row: (
            int(row["game_id"]), str(row.get("atomic_prediction_identity") or ""),
            str(row["reason"]), str(row.get("source_path") or ""))),
        "coverage_status": "EMPTY" if not selected else (
            "FULL" if len({item["game_id"] for item in selected}) == len(games) else "PARTIAL"),
    }
