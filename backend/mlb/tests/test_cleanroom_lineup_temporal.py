from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.mlb.scripts.cleanroom_v1.lineup_temporal import (
    VALID,
    classify_lineup,
    top_order_action,
)


PITCH = datetime(2026, 7, 31, 20, 0, tzinfo=timezone.utc)
MARKET = PITCH - timedelta(minutes=10)
RUN_STARTED = MARKET + timedelta(minutes=1)


def classify(**overrides):
    values = {
        "observed_at": MARKET - timedelta(minutes=5),
        "scheduled_first_pitch": PITCH,
        "governing_market_at": MARKET,
        "ingestion_completed_at": MARKET - timedelta(minutes=2),
        "governing_run_started_at": RUN_STARTED,
        "exact_player_identity": True,
        "confirmed_official_order": True,
    }
    values.update(overrides)
    return classify_lineup(**values)


def test_pregame_lineup_before_capture_is_eligible():
    assert classify() == VALID


def test_pregame_lineup_after_governing_capture_is_ineligible():
    assert classify(observed_at=MARKET + timedelta(seconds=1)) == "LINEUP_AFTER_GOVERNING_CAPTURE"


@pytest.mark.parametrize("offset", [timedelta(0), timedelta(minutes=90)])
def test_first_pitch_or_postgame_lineup_is_ineligible(offset):
    assert classify(observed_at=PITCH + offset) == "LINEUP_POST_FIRST_PITCH"


def test_latest_overall_can_differ_from_latest_valid_pregame():
    observations = [MARKET - timedelta(minutes=5), PITCH + timedelta(minutes=30)]
    eligible = [value for value in observations if classify(observed_at=value) == VALID]
    assert max(observations) != max(eligible)


def test_game_pk_only_join_cannot_admit_late_observation():
    assert classify(observed_at=PITCH + timedelta(minutes=1)) != VALID


def test_slate_date_only_join_cannot_admit_late_observation():
    assert classify(observed_at=PITCH + timedelta(minutes=1)) != VALID


def test_run_invisible_lineup_is_ineligible():
    assert classify(ingestion_completed_at=RUN_STARTED + timedelta(seconds=1)) == "LINEUP_NOT_RUN_VISIBLE"


def test_missing_observation_time_fails_closed():
    assert classify(observed_at=None) == "LINEUP_TIME_MISSING"


def test_missing_schedule_time_fails_closed():
    assert classify(scheduled_first_pitch=None) == "LINEUP_SCHEDULE_TIME_MISSING"


def test_unresolved_identity_and_unconfirmed_order_fail_closed():
    assert classify(exact_player_identity=False) == "LINEUP_IDENTITY_UNRESOLVED"
    assert classify(confirmed_official_order=False) == "LINEUP_IDENTITY_UNRESOLVED"


def test_h1_cannot_use_inadmissible_lineup():
    assert top_order_action("LINEUP_POST_FIRST_PITCH", 1) == "ORDER_NOT_CONFIRMED"
    assert top_order_action("LINEUP_AFTER_GOVERNING_CAPTURE", 2) == "ORDER_NOT_CONFIRMED"


def test_h1_uses_only_valid_confirmed_order():
    assert top_order_action(VALID, 1) == "REJECT_TOP_ORDER"
    assert top_order_action(VALID, 4) == "RETAIN_CONFIRMED_NON_TOP_ORDER"


def test_no_valid_lineup_emits_order_not_confirmed():
    assert top_order_action("LINEUP_NOT_RUN_VISIBLE", None) == "ORDER_NOT_CONFIRMED"


def test_failed_ingestion_id_is_frozen_regression_constant():
    script = Path("backend/mlb/scripts/cleanroom_v1/audit_lineup_temporal_admissibility.py").read_text()
    assert "14951a25-57cb-49f1-88c1-15424cac4f94" in script


def test_temporal_view_contains_strict_first_pitch_predicate():
    sql = Path("backend/mlb/sql/migrations/20260801_create_valid_pregame_lineup_observations.sql").read_text()
    assert "snapshot_timestamp_utc >= g.scheduled_start_utc" in sql
    assert "temporal_classification = 'LINEUP_VALID_PREGAME'" in sql
