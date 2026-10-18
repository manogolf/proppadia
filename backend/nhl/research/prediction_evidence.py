"""Research-only classification for retained NHL prediction evidence.

This module is intentionally separate from governed reconciliation. It never
certifies a source for ``RESILIENT_PREGAME`` and never reads outcomes when
classifying prediction provenance.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


VERIFIED_PROSPECTIVE = "VERIFIED_PROSPECTIVE"
CORROBORATED_PREGAME = "CORROBORATED_PREGAME"
CHRONOLOGY_UNVERIFIED = "CHRONOLOGY_UNVERIFIED"
RESEARCH_POPULATION = "RESEARCH_ONLY_CORROBORATED_PREGAME"


@dataclass(frozen=True)
class Evidence:
    classification: str
    chronology: str
    identity: str
    input_availability: str
    model_lineage: str
    file_integrity: str
    operational_compliance: str
    reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["reasons"] = list(self.reasons)
        return result


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def verify_manifest(directory: Path) -> dict[str, Any]:
    """Verify every file named in SHA256SUMS; report, never repair, defects."""
    manifest = directory / "SHA256SUMS"
    result: dict[str, Any] = {"valid": False, "manifest_path": str(manifest),
                              "manifest_sha256": None, "errors": [], "files": {}}
    if not manifest.is_file():
        result["errors"].append("MANIFEST_MISSING")
        return result
    result["manifest_sha256"] = sha256_file(manifest)
    for raw in manifest.read_text().splitlines():
        parts = raw.strip().split(maxsplit=1)
        if len(parts) != 2:
            result["errors"].append("MANIFEST_LINE_INVALID")
            continue
        expected, name = parts
        name = name.lstrip("* ")
        if Path(name).name != name:
            result["errors"].append(f"MANIFEST_PATH_UNSAFE:{name}")
            continue
        target = directory / name
        if not target.is_file():
            result["errors"].append(f"MANIFEST_FILE_MISSING:{name}")
            continue
        actual = sha256_file(target)
        result["files"][name] = actual
        if actual != expected:
            result["errors"].append(f"MANIFEST_HASH_MISMATCH:{name}")
    result["valid"] = not result["errors"] and bool(result["files"])
    return result


def _parse_utc(value: Any) -> datetime | None:
    if value in (None, "", "None"):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def classify_evidence(
    *,
    scheduled_start_utc: Any,
    feature_cutoff_utc: Any,
    prediction_timestamp_utc: Any,
    durable_publication_timestamp_utc: Any,
    contemporaneous_execution_witness: bool,
    source_content_hash_bound_at_publication: bool,
    canonical_identity_valid: bool,
    prediction_values_valid: bool,
    input_snapshot_complete: bool,
    model_identity_valid: bool,
    manifest_valid: bool,
    reconciliation_certificate_bound: bool = False,
    roster_evidence_bound: bool = False,
    conflicting_evidence: bool = False,
) -> Evidence:
    """Classify one atomic prediction group and retain component judgments."""
    reasons: list[str] = []
    start = _parse_utc(scheduled_start_utc)
    cutoff = _parse_utc(feature_cutoff_utc)
    predicted = _parse_utc(prediction_timestamp_utc)
    published = _parse_utc(durable_publication_timestamp_utc)

    chronology_ok = all((start, cutoff, predicted, published))
    if not start:
        reasons.append("SCHEDULED_START_MISSING_OR_INVALID")
    if not cutoff:
        reasons.append("FEATURE_CUTOFF_MISSING_OR_INVALID")
    if not predicted:
        reasons.append("PREDICTION_TIMESTAMP_MISSING_OR_INVALID")
    if not published:
        reasons.append("DURABLE_PUBLICATION_TIME_MISSING_OR_INVALID")
    if chronology_ok:
        if cutoff >= start:
            reasons.append("FEATURE_CUTOFF_NOT_BEFORE_GAME_START")
            chronology_ok = False
        if predicted >= start:
            reasons.append("PREDICTION_NOT_BEFORE_GAME_START")
            chronology_ok = False
        if published >= start:
            reasons.append("PUBLICATION_NOT_BEFORE_GAME_START")
            chronology_ok = False
        if cutoff > predicted or predicted > published:
            reasons.append("TIMESTAMP_ORDER_CONTRADICTION")
            chronology_ok = False
    if not contemporaneous_execution_witness:
        reasons.append("CONTEMPORANEOUS_EXECUTION_WITNESS_MISSING")
    if not canonical_identity_valid:
        reasons.append("CANONICAL_PREDICTION_IDENTITY_INVALID")
    if not prediction_values_valid:
        reasons.append("PREDICTION_VALUES_INVALID_OR_CONFLICTING")
    if not input_snapshot_complete:
        reasons.append("INPUT_SNAPSHOT_INCOMPLETE")
    if not model_identity_valid:
        reasons.append("MODEL_OR_FEATURE_IDENTITY_MISSING")
    if not manifest_valid:
        reasons.append("SOURCE_MANIFEST_INVALID")
    if conflicting_evidence:
        reasons.append("CONFLICTING_RETAINED_EVIDENCE")

    bases_ok = (chronology_ok and contemporaneous_execution_witness
                and canonical_identity_valid and prediction_values_valid
                and input_snapshot_complete and model_identity_valid
                and manifest_valid and not conflicting_evidence)
    governed = reconciliation_certificate_bound and roster_evidence_bound
    if not chronology_ok or not contemporaneous_execution_witness:
        classification = CHRONOLOGY_UNVERIFIED
    elif bases_ok and source_content_hash_bound_at_publication and governed:
        classification = VERIFIED_PROSPECTIVE
    elif bases_ok:
        classification = CORROBORATED_PREGAME
    else:
        classification = CHRONOLOGY_UNVERIFIED

    return Evidence(
        classification=classification,
        chronology="SUPPORTED_PRESTART" if chronology_ok else "UNVERIFIED_OR_CONTRADICTORY",
        identity="VALID" if canonical_identity_valid and prediction_values_valid else "INVALID_OR_INCOMPLETE",
        input_availability="COMPLETE_SNAPSHOT" if input_snapshot_complete else "INCOMPLETE_OR_UNVERIFIED",
        model_lineage="VERIFIED" if model_identity_valid else "MISSING_OR_UNVERIFIED",
        file_integrity="MANIFEST_VALID" if manifest_valid else "MANIFEST_INVALID",
        operational_compliance=("CERTIFICATE_AND_ROSTER_BOUND" if governed
                                else "GOVERNED_PROOF_NOT_ESTABLISHED"),
        reasons=tuple(reasons),
    )


def select_latest_groups(candidates: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Choose latest corroborated source per group, independent of outcomes."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    excluded: list[dict[str, Any]] = []
    for candidate in candidates:
        group = str(candidate.get("atomic_group_identity") or "")
        if not group:
            excluded.append({**candidate, "selection_reason": "ATOMIC_GROUP_IDENTITY_MISSING"})
        elif candidate.get("classification") not in {VERIFIED_PROSPECTIVE, CORROBORATED_PREGAME}:
            excluded.append({**candidate, "selection_reason": "NOT_ELIGIBLE_FOR_RESEARCH_GRADING"})
        else:
            grouped.setdefault(group, []).append(candidate)
    selected: list[dict[str, Any]] = []
    for group, rows in sorted(grouped.items()):
        stamped = [(row, _parse_utc(row.get("durable_publication_timestamp_utc"))) for row in rows]
        if any(stamp is None for _, stamp in stamped):
            excluded.extend({**row, "selection_reason": "DURABLE_TIME_UNAVAILABLE"}
                            for row, stamp in stamped if stamp is None)
            stamped = [(row, stamp) for row, stamp in stamped if stamp is not None]
        if not stamped:
            continue
        newest = max(stamp for _, stamp in stamped if stamp is not None)
        tied = [row for row, stamp in stamped if stamp == newest]
        hashes = {row.get("prediction_payload_sha256") for row in tied}
        if len(tied) > 1 and (None in hashes or len(hashes) != 1):
            excluded.extend({**row, "selection_reason": "CONFLICTING_LATEST_SOURCE_TIE"} for row in tied)
            continue
        selected.append(min(tied, key=lambda row: str(row.get("source_path", ""))))
    return selected, excluded


def canonical_group_id(lane: str, key_parts: tuple[Any, ...]) -> str:
    payload = json.dumps([lane.upper(), *key_parts], separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def compare_replay(source_rows: list[dict[str, Any]], replay_rows: list[dict[str, Any]],
                   *, identity_fields: tuple[str, ...], value_fields: tuple[str, ...],
                   tolerance: float = 1e-12) -> dict[str, Any]:
    """Compare a deterministic local replay to retained rows without mutation."""
    if len(source_rows) != len(replay_rows):
        return {"status": "FAILED_ROW_COUNT_MISMATCH", "source_rows": len(source_rows),
                "replay_rows": len(replay_rows)}
    key = lambda row: tuple(str(row.get(field)) for field in identity_fields)
    source = sorted(source_rows, key=key)
    replay = sorted(replay_rows, key=key)
    if [key(row) for row in source] != [key(row) for row in replay]:
        return {"status": "FAILED_IDENTITY_MISMATCH", "source_rows": len(source),
                "replay_rows": len(replay)}
    differences: dict[str, float] = {}
    try:
        for field in value_fields:
            differences[field] = max(
                (abs(float(left[field]) - float(right[field]))
                 for left, right in zip(source, replay)), default=0.0)
    except (KeyError, TypeError, ValueError) as error:
        return {"status": f"FAILED_VALUE_FIELD:{type(error).__name__}",
                "detail": str(error)}
    return {"status": "REPRODUCIBLE" if all(value <= tolerance for value in differences.values())
            else "FAILED_VALUE_MISMATCH", "source_rows": len(source),
            "replay_rows": len(replay), "identity_match": True,
            "max_absolute_differences": differences, "tolerance": tolerance}
