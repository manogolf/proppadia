from datetime import datetime, timedelta, timezone
from pathlib import Path
from backend.mlb.scripts.cleanroom_v1.prospective_lineage import exact_identity_certified, observation_admissible, payload_hash_certified, total_bases
NOW = datetime(2026, 8, 1, tzinfo=timezone.utc)

def test_strict_prior_success_and_time_failures():
    assert observation_admissible(NOW, NOW, NOW+timedelta(hours=1), NOW, NOW)
    assert not observation_admissible(NOW+timedelta(seconds=1), NOW, NOW+timedelta(hours=1), NOW, NOW)
    assert not observation_admissible(NOW, NOW, NOW, NOW, NOW)
    assert not observation_admissible(NOW, NOW, NOW+timedelta(hours=1), NOW+timedelta(seconds=1), NOW)

def test_missing_times_fail():
    assert not observation_admissible(None, NOW, NOW, NOW, NOW)
    assert not observation_admissible(NOW, None, NOW, NOW, NOW)
    assert not observation_admissible(NOW, NOW, None, NOW, NOW)

def test_hash_and_identity_fail_closed(tmp_path):
    payload = tmp_path / "p"; payload.write_text("{}")
    assert not payload_hash_certified(payload, "0"*64)
    assert not exact_identity_certified(game_pk=1, player_mlb_id=None, event_candidate_count=1, normalized_candidate_count=1)
    assert not exact_identity_certified(game_pk=1, player_mlb_id=2, event_candidate_count=2, normalized_candidate_count=1)
    assert exact_identity_certified(game_pk=1, player_mlb_id=2, event_candidate_count=1, normalized_candidate_count=1)

def test_total_bases_arithmetic(): assert total_bases(1,2,1,1) == 12

def test_signal_commands_hard_paused():
    for path in ("backend/mlb/scripts/cleanroom_v1/manage_cleanroom_bol_tb15_under_hypotheses.py", "backend/mlb/scripts/cleanroom_v1/manage_cleanroom_bol_tb15_under_toporder.py"):
        assert "SIGNAL_RESEARCH_PAUSED = True" in Path(path).read_text()

def test_market_materialization_requires_strictly_pregame_observation():
    text = Path("backend/mlb/scripts/cleanroom_v1/materialize_capture_snapshot.py").read_text()
    assert "o.snapshot_timestamp_utc < g.scheduled_start_utc" in text
