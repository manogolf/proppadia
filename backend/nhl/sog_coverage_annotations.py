"""Small, explicit cause vocabulary for NHL SOG retained-coverage analysis."""
from __future__ import annotations

from typing import Any, Mapping


SCHEMA_VERSION = "NHL_SOG_COVERAGE_CAUSE_ANNOTATION_V1"
NORMAL_MARKET_OBSERVED = "NORMAL_MARKET_OBSERVED"
NORMAL_MARKET_UNMATCHED = "NORMAL_MARKET_UNMATCHED"
POSTSTART_INELIGIBLE = "POSTSTART_INELIGIBLE"
PRESTART_EVIDENCE_UNAVAILABLE = "PRESTART_EVIDENCE_UNAVAILABLE"
PIPELINE_REPAIR_DELAY = "PRESTART_EVIDENCE_UNAVAILABLE_DUE_TO_PIPELINE_REPAIR_DELAY"
ANALYTICAL_STATES = frozenset({
    NORMAL_MARKET_OBSERVED, NORMAL_MARKET_UNMATCHED, POSTSTART_INELIGIBLE,
    PRESTART_EVIDENCE_UNAVAILABLE, PIPELINE_REPAIR_DELAY,
})


def validate_annotation(payload: Mapping[str, Any], *, slate_date: str) -> dict[str, Any]:
    """Validate the narrowly scoped game annotation before analytical use."""
    if payload.get("schema_version") != SCHEMA_VERSION or payload.get("slate_date") != slate_date:
        raise ValueError("SOG_COVERAGE_ANNOTATION_SCHEMA_OR_SLATE_MISMATCH")
    rows = payload.get("game_annotations")
    if not isinstance(rows, list):
        raise ValueError("SOG_COVERAGE_ANNOTATIONS_MISSING")
    for row in rows:
        if row.get("cause_classification") not in {PIPELINE_REPAIR_DELAY, PRESTART_EVIDENCE_UNAVAILABLE}:
            raise ValueError("SOG_COVERAGE_ANNOTATION_CAUSE_INVALID")
        if row.get("coverage_status") != "NO_VALID_PRESTART_EVIDENCE":
            raise ValueError("SOG_COVERAGE_ANNOTATION_STATUS_INVALID")
        if any(row.get(key) is not False for key in (
                "market_absence_inferred", "bookmaker_behavior_inferred",
                "prediction_absence_inferred", "recoverable_after_start")):
            raise ValueError("SOG_COVERAGE_ANNOTATION_INFERENCE_FORBIDDEN")
        if (row.get("cause_classification") == PIPELINE_REPAIR_DELAY
                and (row.get("cause_scope") != "OPERATIONAL_PIPELINE"
                     or int(row.get("affected_proposition_keys", 0)) <= 0)):
            raise ValueError("SOG_PIPELINE_REPAIR_ANNOTATION_SCOPE_OR_COUNT_INVALID")
    return dict(payload)


def classify_sog_coverage(*, game_id: int, market_status: str | None = None,
                          timing_status: str | None = None,
                          annotations: Mapping[int | str, Mapping[str, Any]] | None = None) -> str:
    """Classify an exact proposition's coverage without inferring market absence.

    A specific validated operational annotation overrides the generic no-snapshot
    status only for its game. Normal later-game matches/unmatches are unaffected.
    """
    annotations = annotations or {}
    annotation = annotations.get(game_id, annotations.get(str(game_id)))
    if annotation and annotation.get("cause_classification") == PIPELINE_REPAIR_DELAY:
        return PIPELINE_REPAIR_DELAY
    if timing_status == POSTSTART_INELIGIBLE:
        return POSTSTART_INELIGIBLE
    if timing_status == PRESTART_EVIDENCE_UNAVAILABLE:
        return PRESTART_EVIDENCE_UNAVAILABLE
    if market_status == "MATCHED":
        return NORMAL_MARKET_OBSERVED
    if market_status == "UNMATCHED":
        return NORMAL_MARKET_UNMATCHED
    if timing_status == "NO_VALID_PRESTART_EVIDENCE":
        return PRESTART_EVIDENCE_UNAVAILABLE
    raise ValueError("SOG_COVERAGE_STATE_UNCLASSIFIABLE")
