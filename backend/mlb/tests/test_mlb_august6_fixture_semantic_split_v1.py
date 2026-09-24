from __future__ import annotations

import json
from pathlib import Path

from backend.mlb.scripts.validate_mlb_august6_fixture_semantic_split_v1 import validate


ROOT = Path(__file__).resolve().parents[3]
ENRICHED = ROOT / (
    "backend/mlb/tests/fixtures/totals_live_context_v1/"
    "august6_enriched_live_context_schedule.json"
)


def test_august6_fixture_semantic_split_contract():
    result = validate()
    assert result["status"] == "PASS"
    assert result["original_sha256"] == "fc3a5e482c90825fdf1b8dbb9e3485d5db49beea1c338fd5620de34f0122bdad"
    assert result["enriched_sha256"] == "66593c8fc050743ab52730ea689c08bf75c5b0afd06bf31981649a324b818fb2"
    assert result["game_count"] == 11


def test_enriched_fixture_has_totals_live_context_semantics():
    payload = json.loads(ENRICHED.read_text(encoding="utf-8"))
    games = [game for day in payload["dates"] for game in day["games"]]
    assert len(games) == 11
    assert all(game.get("status") and game.get("venue", {}).get("id") for game in games)
    assert all(
        game["teams"][side].get("probablePitcher", {}).get("id")
        for game in games
        for side in ("away", "home")
    )
