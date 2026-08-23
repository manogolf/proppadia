#!/usr/bin/env python3
"""Deterministically validate the full-board shadow contract and append-only ledgers."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger
from backend.mlb.scripts.score_mlb_hits05_full_board_shadow_v1 import (
    MODEL_PATH,
    SEMANTIC_MANIFEST,
    sha256_file,
    verified_model_bundle,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3"
PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_hits05_sportsbook_independent_full_board_shadow_stream_v1/2026-08-23"
OUTCOME_TOKENS = ('"actual_hits"', '"outcome_status"', '"result"')
MARKET_TOKENS = ('"bookmaker_key"', '"price_over_american"', '"price_under_american"')


def validate(ledger_path: Path, manifest_path: Path | None = None) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any = "") -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    verified_model_bundle()
    check("frozen_model_sha256", sha256_file(MODEL_PATH) == ledger.MODEL_HASH, sha256_file(MODEL_PATH))
    semantic = json.loads(SEMANTIC_MANIFEST.read_text())
    registration = semantic.get("registration_payload") or {}
    check("semantic_identity", registration.get("semantic_model_id") == ledger.MODEL_ID, registration.get("semantic_model_id"))
    check("semantic_model_binding", registration.get("loaded_artifact_sha256") == ledger.MODEL_HASH, registration.get("loaded_artifact_sha256"))
    if manifest_path and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        mismatches = []
        for item in manifest.get("files", []):
            path = ROOT / item["path"]
            if not path.exists() or sha256_file(path) != item["sha256"]:
                mismatches.append(item["path"])
        check("package_sha256_manifest", not mismatches, mismatches)
    if not ledger_path.exists():
        check("ledger_exists", False, str(ledger_path))
        return {"status": "FAIL", "checks": checks}
    connection = ledger.connect_ledger(ledger_path)
    check("sqlite_integrity", connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok")
    check("sqlite_foreign_keys", not connection.execute("PRAGMA foreign_key_check").fetchall())
    counts = ledger.counts(connection)
    check("duplicate_prediction_identities", counts["duplicate_prediction_identities"] == 0, counts["duplicate_prediction_identities"])
    check("duplicate_outcome_identities", counts["duplicate_outcome_identities"] == 0, counts["duplicate_outcome_identities"])
    trigger_count = connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name LIKE 'hits05_fb_%'").fetchone()[0]
    check("append_only_triggers", trigger_count == 14, trigger_count)
    timing_failures = connection.execute(
        "SELECT canonical_identity FROM hits05_full_board_predictions WHERE prediction_timestamp_utc >= scheduled_start_utc"
    ).fetchall()
    check("strict_prestart_predictions", not timing_failures, [row[0] for row in timing_failures])
    wrong_models = connection.execute(
        "SELECT canonical_identity FROM hits05_full_board_predictions WHERE model_semantic_id<>? OR model_artifact_sha256<>?",
        (ledger.MODEL_ID, ledger.MODEL_HASH),
    ).fetchall()
    check("exact_model_on_every_prediction", not wrong_models, [row[0] for row in wrong_models])
    contaminated = []
    hash_failures = []
    for row in connection.execute(
        "SELECT p.canonical_identity,p.prediction_payload_json,p.prediction_payload_sha256,c.feature_payload_json,c.feature_payload_sha256 FROM hits05_full_board_predictions p JOIN hits05_full_board_feature_context c USING(canonical_identity)"
    ):
        prediction, context = json.loads(row[1]), json.loads(row[3])
        if ledger.payload_hash(prediction) != row[2] or ledger.payload_hash(context) != row[4]:
            hash_failures.append(row[0])
        serialized = (row[1] + row[3]).lower()
        if any(token in serialized for token in OUTCOME_TOKENS + MARKET_TOKENS):
            contaminated.append(row[0])
    check("prediction_and_context_payload_hashes", not hash_failures, hash_failures)
    check("no_market_or_outcome_prediction_contamination", not contaminated, contaminated)
    market_timing = connection.execute(
        "SELECT observation_identity FROM hits05_full_board_market_observations WHERE observation_timestamp_utc >= scheduled_start_utc"
    ).fetchall()
    check("market_observations_pregame", not market_timing, [row[0] for row in market_timing])
    scoring_outcomes = connection.execute("SELECT run_tag FROM hits05_full_board_runs WHERE outcomes_accessed<>0").fetchall()
    check("scoring_outcomes_accessed_zero", not scoring_outcomes, [row[0] for row in scoring_outcomes])
    check("experiment_start_frozen", ledger.EXPERIMENT_START_DATE == "2026-08-24", ledger.EXPERIMENT_START_DATE)
    return {"status": "PASS" if all(row["status"] == "PASS" for row in checks) else "FAIL", "checks": checks, "counts": counts}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--manifest", type=Path, default=PACKAGE / "sha256_manifest.json")
    args = parser.parse_args()
    result = validate(args.ledger, args.manifest)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
