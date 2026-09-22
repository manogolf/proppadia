#!/usr/bin/env python3
"""Attach official Hits outcomes to the independent full-board shadow ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger
from backend.mlb.hits05_full_board_shadow.phase_gating_v1 import partition_full_board_hits_rows
from backend.mlb.season_transition.game_phase_authority_v1 import CanonicalGamePhaseAuthority
from backend.mlb.scripts.build_mlb_reconcile_rows import _load_actual_values
from backend.mlb.scripts.reconcile_mlb_prospective_lineage_outcomes import require_complete


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3"
COMPLETENESS_ROOT = ROOT / "artifacts/analysis/mlb/player_stats_completeness"
OUTCOME_CONTRACT = "MLB_API_CANONICAL_ACTUAL_WITH_PLAYER_STATS_FALLBACK_V1"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def grade_date(
    slate_date: str,
    ledger_path: Path,
    grading_timestamp: str | None = None,
    *,
    phase_authority: CanonicalGamePhaseAuthority | None = None,
) -> dict[str, Any]:
    completeness = COMPLETENESS_ROOT / slate_date / f"player_stats_date_completeness_{slate_date}.csv"
    require_complete(slate_date, completeness)
    connection = ledger.connect_ledger(ledger_path)
    predictions = ledger.predictions_for_date(connection, slate_date)
    partitions = partition_full_board_hits_rows(
        predictions,
        authority=phase_authority,
        unique_identity_fields=("canonical_identity",),
    )
    predictions = list(partitions.admitted)
    actuals = _load_actual_values(from_date=slate_date, to_date=slate_date)
    source_state = {
        "contract": OUTCOME_CONTRACT,
        "completeness_path": str(completeness.relative_to(ROOT)),
        "completeness_sha256": sha256_file(completeness),
        "actual_identity_count": len(actuals),
        "actual_identity_state_sha256": ledger.payload_hash([
            {"game_id": key[0], "player_id": key[1], "prop_type": key[2], **value}
            for key, value in sorted(actuals.items())
        ]),
    }
    source_hash = ledger.payload_hash(source_state)
    completeness_file_mtime = datetime.fromtimestamp(
        completeness.stat().st_mtime, tz=timezone.utc
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    verification_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    timestamp = grading_timestamp or verification_timestamp
    added = existing = conflicts = resolved = no_appearance = 0
    comparison_counts: Counter[str] = Counter()
    for prediction in predictions:
        key = (int(prediction["game_id"]), int(prediction["player_id"]), "hits")
        actual = actuals.get(key) or {}
        value = actual.get("actual_value")
        distinct = int(actual.get("distinct_actual_values") or 0)
        if value is not None and distinct <= 1:
            appearance = "APPEARANCE_RESOLVED"
            status = "CANONICAL_RESOLVED_OFFICIAL_PLAYER_STAT"
            actual_hits: float | None = float(value)
            resolved += 1
        elif distinct > 1:
            appearance = "OUTCOME_CONFLICT_UNRESOLVED"
            status = "UNRESOLVED_CANONICAL_OUTCOME_CONFLICT"
            actual_hits = None
        else:
            appearance = "NO_APPEARANCE_UNRESOLVED"
            status = "UNRESOLVED_NO_OFFICIAL_APPEARANCE"
            actual_hits = None
            no_appearance += 1
        payload = {
            "slate_date": slate_date,
            "game_id": int(prediction["game_id"]),
            "player_id": int(prediction["player_id"]),
            "actual_hits": actual_hits,
            "appearance_status": appearance,
            "outcome_status": status,
            "grading_timestamp_utc": timestamp,
            "grading_source": OUTCOME_CONTRACT,
            "grading_source_sha256": source_hash,
            "grading_source_state": source_state,
            "actual_sample_rows": int(actual.get("sample_rows") or 0),
            "actual_distinct_values": distinct,
            # Operational filesystem provenance is retained but excluded from
            # substantive outcome equality.  It is not an authoritative
            # source-observation timestamp.
            "completeness_file_modification_time_utc": completeness_file_mtime,
        }
        action = ledger.append_outcome(connection, prediction["canonical_identity"], payload)
        comparison_counts[action] += 1
        if action == "APPENDED_NEW":
            added += 1
        elif action in {"IDEMPOTENT_SUBSTANTIVE_MATCH", "METADATA_ONLY_DIFFERENCE"}:
            existing += 1
        else:
            conflicts += 1
    return {
        "status": "PASS" if conflicts == 0 else "FAIL_CLOSED_OUTCOME_CONFLICT_PRESERVED",
        "slate_date": slate_date,
        "prediction_rows": len(predictions),
        "outcomes_added": added,
        "outcomes_existing": existing,
        "outcome_conflicts": conflicts,
        "substantive_idempotent_matches": existing,
        "metadata_only_differences": comparison_counts["METADATA_ONLY_DIFFERENCE"],
        "comparison_classification_counts": {
            classification: comparison_counts[classification]
            for classification in ledger.OUTCOME_COMPARISON_CLASSIFICATIONS
        },
        "outcome_idempotency_contract_version": ledger.OUTCOME_IDEMPOTENCY_CONTRACT_V2,
        "appearance_resolved": resolved,
        "no_appearance_unresolved": no_appearance,
        "grading_source_sha256": source_hash,
        "verification_timestamp_utc": verification_timestamp,
        "authoritative_source_observation_time_utc": None,
        "completeness_file_modification_time_utc": completeness_file_mtime,
        "ledger_counts": ledger.counts(connection),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--grading-timestamp", default="")
    args = parser.parse_args()
    print(json.dumps(grade_date(args.date, args.ledger, args.grading_timestamp or None), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
