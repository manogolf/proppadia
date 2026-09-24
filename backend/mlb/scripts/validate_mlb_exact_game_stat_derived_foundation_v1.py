"""Deterministic source/package validator; performs no DB or network I/O."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "docs/contracts/mlb_2026_exact_game_stat_derived_foundation_v1"
MIGRATION = ROOT / "backend/mlb/sql/migrations/20260924_prepare_player_game_feature_state_v1.sql"
ROLLBACK = ROOT / "backend/mlb/sql/migrations/20260924_rollback_player_game_feature_state_v1.sql"
FIXTURE = ROOT / "backend/mlb/tests/fixtures/exact_game_stat_derived_v1/retained_and_synthetic_cases.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate() -> dict[str, object]:
    contract = json.loads((PACKAGE / "foundation_contract.json").read_text(encoding="utf-8"))
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert contract["database_writes"] == contract["api_requests"] == contract["pipeline_runs"] == 0
    assert contract["legacy_rows_modified"] == 0
    for retained in fixture["retained_sources"].values():
        assert sha256(ROOT / retained["path"]) == retained["sha256"]

    with (PACKAGE / "consumer_cutover_classification.csv").open(newline="", encoding="utf-8") as handle:
        consumers = list(csv.DictReader(handle))
    assert len(consumers) == 55
    counts: dict[str, int] = {}
    for row in consumers:
        key = row["cutover_classification"]
        counts[key] = counts.get(key, 0) + 1
    assert counts == contract["consumer_classification_counts"]

    migration = MIGRATION.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")
    forbidden = (
        "ALTER TABLE mlb.player_derived_stats",
        "UPDATE mlb.player_derived_stats",
        "DELETE FROM mlb.player_derived_stats",
        "ALTER TABLE mlb.player_stats",
        "UPDATE mlb.player_stats",
        "ALTER TABLE mlb.model_training_props",
        "UPDATE mlb.model_training_props",
    )
    assert not any(token in migration for token in forbidden)
    assert "player_derived_stats" not in rollback
    assert "model_training_props" not in rollback
    assert "player_stats" not in rollback

    manifest = PACKAGE / "sha256_manifest.csv"
    if manifest.exists():
        with manifest.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                assert sha256(ROOT / row["path"]) == row["sha256"]
    return {
        "status": "PASS",
        "consumer_count": len(consumers),
        "retained_source_hash_count": len(fixture["retained_sources"]),
        "database_writes": 0,
        "api_requests": 0,
        "migration_applied": False,
    }


def main() -> int:
    print(json.dumps(validate(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
