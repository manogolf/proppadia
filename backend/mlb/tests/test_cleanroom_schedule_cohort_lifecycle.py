from datetime import datetime,timezone
from pathlib import Path
import json,pytest
from backend.mlb.scripts.cleanroom_v1 import schedule_cohort_lifecycle as s

def pt(h,m):return datetime(2026,8,2,h,m,tzinfo=s.PT)
def test_weekend_early_selects_0930():assert s.select_wrapper(pt(10,30)).strftime('%H:%M')=='09:30'
def test_midday_selects_1100():assert s.select_wrapper(pt(12,40)).strftime('%H:%M')=='11:00'
def test_late_selects_1300():assert s.select_wrapper(pt(16,10)).strftime('%H:%M')=='13:00'
def test_no_valid_wrapper():assert s.select_wrapper(pt(5,0)) is None
def test_selected_wrapper_match_window():assert s.matches(pt(9,35),pt(9,30)) and not s.matches(pt(11,0),pt(9,30))
@pytest.mark.parametrize('minutes,expected',[(29,False),(30,True),(240,True),(241,False)])
def test_v2_game_window(minutes,expected):
 old=(s.v1.MIN_GAME,s.v1.MAX_GAME);s.v1.MIN_GAME,s.v1.MAX_GAME=s.MIN_GAME,s.MAX_GAME
 try:assert s.v1.game_in_window(minutes) is expected
 finally:s.v1.MIN_GAME,s.v1.MAX_GAME=old
def test_historical_emergency_refused_before_network(monkeypatch):
 monkeypatch.setattr(s,'fetch_schedule',lambda _:pytest.fail('network reached'))
 with pytest.raises(RuntimeError,match='DATE_MISMATCH'):s.capture('2026-08-01',True,pt(8,40))
def test_existing_cohort_refused_before_network(tmp_path,monkeypatch):
 root=tmp_path/'2026-08-02';root.mkdir();(root/'schedule_cohort_manifest.json').write_text('{"status":"SCHEDULE_COHORT_FROZEN"}')
 monkeypatch.setattr(s,'COHORT_ROOT',tmp_path);monkeypatch.setattr(s,'fetch_schedule',lambda _:pytest.fail('network reached'))
 assert s.capture('2026-08-02',False,pt(9,30))['status']=='SCHEDULE_COHORT_ALREADY_FROZEN'
def test_nonselected_wrapper_no_research_writes(tmp_path,monkeypatch):
 schedule={'dates':[{'games':[{'officialDate':'2026-08-02','gameDate':'2026-08-02T17:30:00Z'}]}]}
 monkeypatch.setattr(s,'COHORT_ROOT',tmp_path);monkeypatch.setattr(s,'fetch_schedule',lambda _:schedule)
 result=s.capture('2026-08-02',False,pt(5,30));assert result['status']=='NOT_SELECTED_SCHEDULE_RELATIVE_WRAPPER' and not list(tmp_path.iterdir())
def test_closeout_requires_schedule_cohort(tmp_path,monkeypatch):
 monkeypatch.setattr(s,'COHORT_ROOT',tmp_path)
 with pytest.raises(RuntimeError,match='FREEZE_REQUIRED'):s.closeout('2026-08-02')
def test_retired_cohort_hooks_disabled():
 env=(s.ROOT/'backend/.env').read_text();assert 'MLB_CLEANROOM_FIXED_COHORT_ENABLED=0' in env and 'MLB_CLEANROOM_SCHEDULE_COHORT_ENABLED=0' in env
def test_atomic_and_strict_prior_contract_present():
 src=Path(s.__file__).read_text();assert 'os.replace(root,final)' in src and 'LINEUP_AFTER_CAPTURE' in Path(s.v1.__file__).read_text() and 'MARKET_AFTER_CAPTURE' in Path(s.v1.__file__).read_text()
def test_exact_identity_reused():assert "EXACT_UNIQUE_MATCH" in Path(s.v1.__file__).read_text()
def test_make_target_has_no_emergency_flag():
 make=(s.ROOT/'Makefile').read_text();line=make.split('mlb-cleanroom-bol-tb15-schedule-cohort:',1)[1].split('\n\n',1)[0];assert '--execute-current-valid-window' not in line
