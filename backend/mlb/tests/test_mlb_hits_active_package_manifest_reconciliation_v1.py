from __future__ import annotations

import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = (
    ROOT
    / "artifacts/analysis/model_development/"
    "mlb_hits05_sportsbook_independent_full_board_shadow_stream_v1/2026-08-23/sha256_manifest.json"
)


def test_active_hits_package_manifest_matches_all_current_bindings():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    assert manifest["manifest_contract"] == "SHA256_OF_EACH_LISTED_FILE_BYTES; MANIFEST_EXCLUDES_ITSELF"
    assert manifest["frozen_at_utc"] == "2026-08-23T18:08:27Z"
    assert len(manifest["files"]) == len({entry["path"] for entry in manifest["files"]})

    mismatches = []
    for entry in manifest["files"]:
        path = Path(entry["path"])
        if not path.is_absolute():
            path = ROOT / path
        if not path.is_file():
            mismatches.append((entry["path"], "MISSING"))
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != entry["sha256"]:
            mismatches.append((entry["path"], actual))

    assert mismatches == []


def test_active_manifest_reconciliation_preserves_superseded_hashes_and_scope():
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    reconciliation = next(
        item
        for item in manifest["integrity_reconciliations"]
        if item.get("contract_version") == "HITS05_ACTIVE_PACKAGE_HASH_RECONCILIATION_V1"
    )

    assert reconciliation["frozen_experiment_contract_changed"] is False
    assert reconciliation["runtime_behavior_changed"] is False
    assert reconciliation["previous_bindings_preserved"] is True
    assert len(reconciliation["bindings"]) == 7
    for binding in reconciliation["bindings"]:
        active = next(item for item in manifest["files"] if item["path"] == binding["path"])
        assert active["sha256"] == binding["sha256"]
        assert active["active_package_previous_sha256"] == binding["previous_sha256"]
