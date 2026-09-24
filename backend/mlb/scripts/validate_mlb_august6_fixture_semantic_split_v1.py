"""Offline validator for the August 6 Moneyline/Totals fixture split."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "docs/contracts/mlb_2026_august6_fixture_semantic_split_v1"
BINDINGS = PACKAGE / "fixture_bindings.json"
CONSUMERS = PACKAGE / "consumer_mapping.csv"
MONEYLINE_CONFIG = ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _git_blob(data: bytes) -> str:
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


def _games(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [game for block in payload.get("dates") or [] for game in block.get("games") or []]


def validate() -> dict[str, Any]:
    contract = json.loads(BINDINGS.read_text(encoding="utf-8"))
    bindings = contract["bindings"]
    assert set(bindings) == {
        "IMMUTABLE_MONEYLINE_FROZEN_BEHAVIOR",
        "TOTALS_ENRICHED_LIVE_CONTEXT",
    }
    original_binding = bindings["IMMUTABLE_MONEYLINE_FROZEN_BEHAVIOR"]
    enriched_binding = bindings["TOTALS_ENRICHED_LIVE_CONTEXT"]
    assert original_binding["path"] != enriched_binding["path"]

    original_bytes = (ROOT / original_binding["path"]).read_bytes()
    enriched_bytes = (ROOT / enriched_binding["path"]).read_bytes()
    assert _sha256(original_bytes) == original_binding["sha256"]
    assert _git_blob(original_bytes) == original_binding["git_blob"]
    assert _sha256(enriched_bytes) == enriched_binding["sha256"]
    assert original_bytes != enriched_bytes

    original_games = _games(json.loads(original_bytes))
    enriched_games = _games(json.loads(enriched_bytes))
    assert len(original_games) == len(enriched_games) == 11
    assert [game["gamePk"] for game in original_games] == [game["gamePk"] for game in enriched_games]
    original_keys = {"gamePk", "gameDate", "officialDate", "gameNumber", "doubleHeader", "teams"}
    assert all(set(game) == original_keys for game in original_games)
    assert all("probablePitcher" not in game["teams"][side] for game in original_games for side in ("away", "home"))
    assert all("status" in game and "venue" in game for game in enriched_games)
    assert all(game["teams"][side].get("probablePitcher", {}).get("id") for game in enriched_games for side in ("away", "home"))

    moneyline_config = json.loads(MONEYLINE_CONFIG.read_text(encoding="utf-8"))
    fixture_hashes = moneyline_config["self_contained_reproduction_fixtures"]
    assert fixture_hashes["august6_schedule.json"] == original_binding["sha256"]
    assert enriched_binding["path"] not in json.dumps(moneyline_config, sort_keys=True)

    with CONSUMERS.open(newline="", encoding="utf-8") as handle:
        consumers = list(csv.DictReader(handle))
    assert len(consumers) == 8
    moneyline_consumers = [row for row in consumers if row["classification"] == "MONEYLINE_ORIGINAL"]
    assert len(moneyline_consumers) == 4
    assert all(row["assigned_fixture"] == original_binding["path"] for row in moneyline_consumers)
    totals_consumers = [row for row in consumers if row["classification"] == "TOTALS_ENRICHED"]
    assert len(totals_consumers) == 1
    assert totals_consumers[0]["assigned_fixture"] == enriched_binding["path"]

    for row in moneyline_consumers:
        source = (ROOT / row["consumer_path"]).read_text(encoding="utf-8")
        assert "august6_schedule.json" in source
        assert "august6_enriched_live_context_schedule.json" not in source
    totals_source = (ROOT / totals_consumers[0]["consumer_path"]).read_text(encoding="utf-8")
    assert "august6_enriched_live_context_schedule.json" in totals_source

    manifest = PACKAGE / "sha256_manifest.csv"
    if manifest.exists():
        with manifest.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                assert _sha256((ROOT / row["path"]).read_bytes()) == row["sha256"]

    return {
        "status": "PASS",
        "original_git_blob": original_binding["git_blob"],
        "original_sha256": original_binding["sha256"],
        "enriched_sha256": enriched_binding["sha256"],
        "game_count": len(original_games),
        "consumer_count": len(consumers),
        "database_effect": "NONE",
        "pipeline_effect": "NONE",
    }


def main() -> int:
    print(json.dumps(validate(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
