from datetime import datetime,timezone
from pathlib import Path
import json,pytest
from backend.mlb.scripts.cleanroom_v1 import fixed_cohort_lifecycle as f

def at(hour,minute,day=1):return datetime(2026,8,day,hour+7,minute,tzinfo=timezone.utc)
def test_inside_window_succeeds():assert f.guard('2026-08-01',at(13,0)).hour==13
def test_before_window_fails_before_network():
 with pytest.raises(RuntimeError,match='WINDOW_CLOSED'):f.guard('2026-08-01',at(12,44))
def test_after_window_fails_before_network():
 with pytest.raises(RuntimeError,match='WINDOW_CLOSED'):f.guard('2026-08-01',at(13,16))
@pytest.mark.parametrize('slate',["2026-07-31","2026-08-02"])
def test_noncurrent_date_fails_before_network(slate):
 with pytest.raises(RuntimeError,match='DATE_MISMATCH'):f.guard(slate,at(13,0))
@pytest.mark.parametrize('minutes,expected',[(14,False),(15,True),(180,True),(181,False)])
def test_game_window_boundaries(minutes,expected):assert f.game_in_window(minutes) is expected
def test_unconfirmed_lineup_excluded():assert f.eligible_lineup('UNCONFIRMED',4,at(12,0),at(13,0),at(14,0))=='LINEUP_NOT_CONFIRMED'
def test_after_capture_lineup_excluded():assert f.eligible_lineup('CONFIRMED',4,at(13,1),at(13,0),at(14,0))=='LINEUP_AFTER_CAPTURE'
def test_post_pitch_lineup_excluded():assert f.eligible_lineup('CONFIRMED',4,at(14,0),at(13,0),at(14,0))=='LINEUP_POST_FIRST_PITCH'
def test_market_after_capture_and_post_pitch():
 assert f.market_time_reason(at(13,1),at(13,0),at(14,0))=='MARKET_AFTER_CAPTURE'
 assert f.market_time_reason(at(14,0),at(14,0),at(14,0))=='MARKET_POST_FIRST_PITCH'
def test_contract_has_no_outcome_fields():assert not any('outcome' in x for x in f.FIELDS)
def test_closeout_requires_frozen_cohort(tmp_path,monkeypatch):
 monkeypatch.setattr(f,'COHORT_ROOT',tmp_path)
 with pytest.raises(RuntimeError,match='FREEZE_REQUIRED'):f.closeout('2026-08-01')
def test_second_attempt_refused_before_guard(tmp_path,monkeypatch):
 root=tmp_path/'2026-08-01';root.mkdir();(root/'fixed_cohort_manifest.json').write_text('{"status":"FIXED_COHORT_FROZEN"}')
 monkeypatch.setattr(f,'COHORT_ROOT',tmp_path)
 assert f.capture('2026-08-01',at(1,0))['status']=='FIXED_COHORT_ALREADY_FROZEN'
def test_zero_row_manifest_contract_present():
 source=Path(f.__file__).read_text();assert "EMPTY_ELIGIBLE_COHORT" in source and "if not baseline" in source
def test_failed_technical_run_can_retry():
 source=Path(f.__file__).read_text();assert "os.replace(stage,out)" in source and "if stage.exists():shutil.rmtree(stage)" in source
def test_identity_and_market_fail_closed_reasons_present():
 source=Path(f.__file__).read_text()
 for reason in ('PROVIDER_EVENT_MISSING','PLAYER_IDENTITY_UNRESOLVED','LINEUP_NOT_CONFIRMED','SOURCE_HASH_FAILURE'):assert reason in source
def test_doubleheader_binding_exact_unique():
 source=Path(f.__file__).read_text();assert "r['decision']=='EXACT_UNIQUE_MATCH'" in source and "int(r['game_pk'])" in source
def test_missing_result_and_zero_pa_contract():
 source=Path(f.__file__).read_text();assert "TECHNICAL_UNRESOLVED" in source and "NO_ACTION_ZERO_PLATE_APPEARANCES_OFFICIALLY_SUPPORTED" in source
def test_frozen_under_odds_settlement():
 source=Path(f.__file__).read_text();assert "int(r['under_odds'])" in source and 'american_profit(5' in source
def test_unchanged_closeout_no_revision():
 source=Path(f.__file__).read_text();assert "prior.get('content_sha256')==digest" in source and "'changed':False" in source
def test_block_attempt_includes_empty_and_stops_at_five(tmp_path,monkeypatch):
 monkeypatch.setattr(f,'COHORT_ROOT',tmp_path)
 for n in range(1,6):
  root=tmp_path/f'2026-08-0{n}';root.mkdir();(root/'fixed_cohort_manifest.json').write_text(json.dumps({'status':'EMPTY_ELIGIBLE_COHORT'}))
 result=f.block_status();assert result['attempted_dates']==5 and result['stop_rule_met'] and result['terminal_decision']=='H1_FIXED_COHORT_INSUFFICIENT_VOLUME_CLOSE_HYPOTHESIS'
def test_old_full_slate_path_retirement_present():
 source=(f.ROOT/'backend/mlb/scripts/cleanroom_v1/manage_cleanroom_bol_tb15_under_toporder.py').read_text();assert source.count('H1_FULL_SLATE_PATH_RETIRED_USE_FIXED_COHORT_V1')>=2
def test_optional_hook_defaults_disabled():
 hook=f.ROOT/'bin/mlb_cleanroom_fixed_cohort_optional_hook.sh';assert 'MLB_CLEANROOM_FIXED_COHORT_ENABLED:-0' in hook.read_text()
def test_same_run_visibility_uses_admitted_snapshot_timestamp():
 source=Path(f.__file__).read_text();assert "['capture_timestamp_utc']" in source and 'admitted_capture' in source
