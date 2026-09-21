from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.mlb.season_transition.contract_v1 import (
    CONTRACT_NAME,
    REQUIRED_CLOSE_LANES,
    PhaseContractError,
    classify_schedule_game,
    normalize_source_game_type,
    validate_close_inventory,
    validate_fixture_suite,
)


FIXTURES = Path("backend/mlb/season_transition/fixtures/phase_and_close_cases_v1.json")


def _passing_inventory() -> dict:
    zeroes = {
        "manifest_status": "PASS",
        "ledger_status": "PASS",
        "ungraded_eligible_predictions": 0,
        "duplicate_prediction_identities": 0,
        "duplicate_outcome_identities": 0,
        "post_start_violations": 0,
        "outcome_leakage_violations": 0,
        "postseason_rows_in_regular_outputs": 0,
    }
    return {
        "contract_name": CONTRACT_NAME,
        "season": 2026,
        "canonical_regular_season_games": [
            {
                "game_pk": 1,
                "source_game_type": "R",
                "season_phase": "REGULAR_SEASON",
                "close_disposition": "FINAL",
            },
            {
                "game_pk": 2,
                "source_game_type": "R",
                "season_phase": "REGULAR_SEASON",
                "close_disposition": "POSTPONED_AUTHORITATIVELY_DISPOSED",
                "authoritative_disposition_ref": "replacement-game:3",
            },
        ],
        "lanes": {name: dict(zeroes) for name in REQUIRED_CLOSE_LANES},
        "proper_scores": {},
        "bvp_acquisition_identity_status": {},
        "feature_lineage_health": {},
        "market_coverage": {},
        "agreement_study_progress": {},
        "api_credit_accounting": {},
        "outstanding_unresolved_rows": [],
        "model_qualification_publication_status": {
            "model_promoted": False,
            "published": False,
            "wagering_authorized": False,
        },
        "source_config_identities": [{"identity": "fixture", "sha256": "a" * 64}],
    }


def test_fixture_suite_is_deterministic_and_complete() -> None:
    payload = json.loads(FIXTURES.read_text())
    first = validate_fixture_suite(payload)
    second = validate_fixture_suite(payload)
    assert first == second
    assert first["passed"] is True


def test_date_never_overrides_authoritative_type() -> None:
    regular = normalize_source_game_type("R", season=2026)
    postseason = normalize_source_game_type("W", season=2026)
    assert regular.phase == "REGULAR_SEASON"
    assert postseason.phase == "POSTSEASON"


def test_schedule_and_feed_type_conflict_fails_closed() -> None:
    with pytest.raises(PhaseContractError, match="CONFLICTING_AUTHORITATIVE_GAME_TYPES"):
        classify_schedule_game(
            {"season": 2026, "gameType": "R", "gameData": {"game": {"type": "W"}}}
        )


def test_all_star_is_known_but_excluded() -> None:
    result = normalize_source_game_type("A", season=2026)
    assert result.phase is None
    assert result.eligible_for_phase_evaluation is False


def test_passing_close_inventory_authorizes() -> None:
    report = validate_close_inventory(_passing_inventory())
    assert report["passed"] is True
    assert report["decision"] == "REGULAR_SEASON_CLOSE_AUTHORIZED"


def test_postseason_row_blocks_regular_close() -> None:
    payload = _passing_inventory()
    payload["canonical_regular_season_games"].append(
        {
            "game_pk": 9,
            "source_game_type": "F",
            "season_phase": "REGULAR_SEASON",
            "close_disposition": "FINAL",
        }
    )
    report = validate_close_inventory(payload)
    assert report["passed"] is False
    assert "regular_game_phase_and_disposition" in report["failed_checks"]


def test_late_scheduled_regular_game_blocks_close() -> None:
    payload = _passing_inventory()
    payload["canonical_regular_season_games"][0]["close_disposition"] = "SCHEDULED"
    assert validate_close_inventory(payload)["passed"] is False


def test_lane_leakage_or_ungraded_rows_block_close() -> None:
    payload = _passing_inventory()
    payload["lanes"]["MONEYLINE"]["postseason_rows_in_regular_outputs"] = 1
    payload["lanes"]["RAW_TOTALS"]["ungraded_eligible_predictions"] = 1
    report = validate_close_inventory(payload)
    assert report["passed"] is False
    assert "MONEYLINE:postseason_rows_in_regular_outputs" in report["failed_checks"]
    assert "RAW_TOTALS:ungraded_eligible_predictions" in report["failed_checks"]
