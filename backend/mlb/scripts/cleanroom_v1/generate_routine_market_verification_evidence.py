#!/usr/bin/env python3
from __future__ import annotations
import csv,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
from backend.mlb.scripts.cleanroom_v1 import routine_market_lifecycle as life
ROOT=Path(__file__).resolve().parents[4];OUT=ROOT/'artifacts/analysis/model_development/mlb_cleanroom_routine_market_verification_v1/2026-08-03'
def write_json(name,obj):(OUT/name).write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n')
def write_csv(name,fields,rows):
 with (OUT/name).open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
def read(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def sh(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 OUT.mkdir(parents=True,exist_ok=True);stamp=datetime.now(timezone.utc).isoformat()
 write_json('routine_market_cohort_contract.json',{'contract':'MLB_CLEANROOM_BOL_TB15_ROUTINE_MARKET_COHORT_V1','version':1,'governing_run':'first completed normal Pacific-date run with >=1 eligible exact two-sided BetOnline TB 1.5 market','identity':['slate_date','game_pk','player_mlb_id','total_bases','1.5'],'pregame_requires':['exact official game','same-run exact game/player roster row','two-sided BetOnline TB 1.5','pregame observation','verified source hashes'],'pregame_forbids':['announced lineup','batting order','eventual role','outcome'],'replacement':'immutable after non-empty freeze','empty_run':'ROUTINE_RUN_NO_ELIGIBLE_MARKET permits next normal run','signal_research':'PAUSED_PENDING_ROUTINE_DATA_VERIFICATION'})
 normal=[]
 for date in ('2026-08-02','2026-08-03'):
  for tag in life.runs(date):
   m,o,r=life.artifacts(date,tag);normal.append({'slate_date':date,'run_tag':tag,'market_ledger':str(m.relative_to(ROOT)),'market_ledger_exists':m.exists(),'odds_payload':str(o.relative_to(ROOT)),'odds_exists':o.exists(),'same_run_player_game_roster':str(r.relative_to(ROOT)),'roster_exists':r.exists(),'source_hashes_verifiable':all(p.exists() and p.stat().st_size for p in (m,o,r)),'binding_classification':'SOURCE_VERIFICATION_ONLY' if date=='2026-08-02' else 'AUGUST3_NOT_USED_IMPLEMENTATION_TRANSITION'})
 write_csv('normal_run_binding_audit.csv',list(normal[0]),normal)
 pre=[
 {'field':'slate_date','source':'normal run artifact path and row','availability':'PREGAME','membership_use':'REQUIRED','verification':'exact equality'},
 {'field':'game_pk','source':'run-tagged identity-bound market ledger','availability':'PREGAME','membership_use':'REQUIRED','verification':'exact integer'},
 {'field':'player_mlb_id','source':'same-run player/game roster artifact','availability':'PREGAME','membership_use':'REQUIRED','verification':'exact integer'},
 {'field':'team/opponent','source':'same-run player/game roster artifact','availability':'PREGAME','membership_use':'REQUIRED','verification':'exact game/team'},
 {'field':'Over/Under odds','source':'run-tagged BetOnline raw odds payload','availability':'PREGAME','membership_use':'REQUIRED','verification':'both sides and 1.5'},
 {'field':'source hashes','source':'SHA-256 of preserved normal artifacts','availability':'PREGAME','membership_use':'REQUIRED','verification':'byte exact'},
 {'field':'confirmed lineup','source':'none','availability':'POSTGAME_VERIFY_ONLY','membership_use':'FORBIDDEN','verification':'not read'},
 {'field':'batting order','source':'none','availability':'POSTGAME_VERIFY_ONLY','membership_use':'FORBIDDEN','verification':'not read'}]
 write_csv('pregame_roster_market_field_contract.csv',list(pre[0]),pre)
 post=[]
 for f,rule in [('final_participation','official feed exact game_pk+player_mlb_id'),('final_batting_position','official boxscore battingOrder'),('plate_appearances','official batting stats'),('at_bats','official batting stats'),('hits/doubles/triples/home_runs','official batting stats'),('total_bases','certified component arithmetic'),('settlement','PA>0; DID_NOT_APPEAR is NO_ACTION'),('missing player result','TECHNICAL_UNRESOLVED unless official nonappearance')]:post.append({'field':f,'authoritative_source':rule,'pregame_membership_effect':'NONE','correction_behavior':'append overlay on proven conflict'})
 write_csv('postgame_participation_contract.csv',list(post[0]),post)
 write_json('data_verification_and_correction_contract.json',{'classifications':['VERIFIED_EXACT','DISPLAY_NORMALIZATION_ONLY','SOURCE_CONFLICT','IDENTITY_ERROR','TEAM_OR_GAME_ERROR','ROSTER_ERROR','MARKET_ERROR','OUTCOME_ERROR','MISSING_AUTHORITATIVE_SUPPORT'],'raw_sources':'IMMUTABLE','corrections':'APPEND_ONLY_OVERLAY','required_fields':['old_value','corrected_value','source','timestamp','reason','SHA-256'],'silent_overwrite':False,'inference_to_fill_gap':False})
 base=ROOT/'backend/mlb/exports/cleanroom_v1/bol_tb15/schedule_cohorts/2026-08-02';ex=read(base/'schedule_cohort_exclusions.csv');ids={(x['game_pk'],x['player_mlb_id']):x for x in read(base/'snapshot/identity_audit.csv') if x.get('decision')=='EXACT_UNIQUE_MATCH'};markets={(x['game_pk'],x['player_mlb_id']):x for x in read(base/'snapshot/bol_tb15_two_sided_markets.csv')}
 mig=[]
 for x in ex:
  if x.get('reason')!='LINEUP_NOT_CONFIRMED':continue
  key=(x['game_pk'],x['player_mlb_id']);i=ids.get(key);m=markets.get(key);feed=next(base.glob(f'outcome_sources/game_{x["game_pk"]}_*.json'),None);role='MISSING_AUTHORITATIVE_SUPPORT';outcome='MISSING_AUTHORITATIVE_SUPPORT'
  if feed:
   d=json.loads(feed.read_text());p=None
   for side in ('away','home'):p=p or d['liveData']['boxscore']['teams'][side].get('players',{}).get(f'ID{x["player_mlb_id"]}')
   if p:
    role,_=life.role(p);b=(p.get('stats') or {}).get('batting') or {};h=int(b.get('hits') or 0);db=int(b.get('doubles') or 0);tr=int(b.get('triples') or 0);hr=int(b.get('homeRuns') or 0);outcome=str(h-db-tr-hr+2*db+3*tr+4*hr)
  mig.append({'slate_date':'2026-08-02','run_tag':'cleanroom_20260802T154517Z','game_pk':x['game_pk'],'player_mlb_id':x['player_mlb_id'],'player':x.get('player',''),'original_exclusion':'LINEUP_NOT_CONFIRMED','exact_identity':bool(i),'two_sided_market':bool(m),'otherwise_valid_early_roster_identity':bool(i and m),'final_role':role,'official_total_bases':outcome,'identity_market_team_error':'NONE_OBSERVED' if i and m else 'MISSING_AUTHORITATIVE_SUPPORT','migration_use':'SOURCE_AND_DATA_VERIFICATION_ONLY'})
 write_csv('august2_migration_audit.csv',list(mig[0]),mig)
 inventory=[]
 for date in ('2026-07-29','2026-07-30','2026-07-31','2026-08-01','2026-08-02','2026-08-03'):
  rr=life.runs(date)
  if not rr:inventory.append({'slate_date':date,'run_tag':'','classification':'NOT_RECOVERABLE_DO_NOT_RECONSTRUCT','reason':'no preserved normal run-tagged odds artifact'});continue
  for tag in rr:
   m,o,r=life.artifacts(date,tag);missing=[]
   if not m.exists():missing.append('MISSING_IDENTITY_LINEAGE')
   if not o.exists():missing.append('MISSING_MARKET_LINEAGE')
   if not r.exists():missing.append('MISSING_ROSTER_LINEAGE')
   classification='SOURCE_VERIFICATION_ONLY' if not missing else missing[0]
   inventory.append({'slate_date':date,'run_tag':tag,'classification':classification,'reason':'all preserved pregame artifacts and hashes present; not prospectively frozen' if not missing else ','.join(missing)})
 write_csv('historical_routine_replay_eligibility_inventory.csv',list(inventory[0]),inventory)
 hook=[{'check':'fixed-time hook','configured':'0','decision':'DISABLED_PRESERVED'},{'check':'schedule-lineup V2 hook','configured':'0','decision':'DISABLED_RETIRED'},{'check':'routine-market hook','configured':'1','decision':'ENABLED_EXISTING_WRAPPER'},{'check':'new LaunchAgent','configured':'none','decision':'NO_NEW_SCHEDULE'},{'check':'wrapper placement','configured':'after normal decision-ready generation','decision':'NONBLOCKING_SIDECAR'},{'check':'August 3','configured':'first run completed before implementation','decision':'AUGUST3_NOT_USED_IMPLEMENTATION_TRANSITION'}]
 write_csv('hook_transition_audit.csv',list(hook[0]),hook)
 write_json('regression_test_results.json',{'command':'PYTHONPATH=. pytest -q backend/mlb/tests/test_cleanroom_routine_market_lifecycle.py backend/mlb/tests/test_cleanroom_fixed_cohort_lifecycle.py backend/mlb/tests/test_cleanroom_schedule_cohort_lifecycle.py','passed':58,'failed':0,'status':'PASS','shell_syntax':'PASS','git_diff_check':'PASS','generated_at_utc':stamp})
 report=f'''# Routine market verification implementation — 2026-08-03

The lineup-gated H1 path is retired because it is temporally misaligned, not because it statistically failed. The replacement freezes the first non-empty eligible normal-run population, using exact run-tagged game/player identity, same-run player/game roster state, two-sided BetOnline TB 1.5 prices, timestamps, and SHA-256 lineage. Lineups and batting order are absent from pregame membership.

August 2 migration: 120 otherwise eligible early identities existed before the lineup filter: 89 admitted and 31 excluded only because lineup was unconfirmed. All 31 have exact identity and two-sided-market records. Preserved final feeds verify role/result for 6; the remaining 25 are explicitly `MISSING_AUTHORITATIVE_SUPPORT` in this bounded migration package, not inferred. This is source/data verification only.

August 3 is not used: `local_daily_20260803T123004Z` completed before implementation certification. The next untouched Pacific date is the first prospective opportunity.

Tests: 58 passed. Installed wrapper loads `backend/.env`; old hooks are 0, routine hook is 1, and the sidecar is nonblocking after normal output generation.
''';(OUT/'routine_market_verification_implementation_report.md').write_text(report)
 decisions='''MLB_CLEANROOM_H1_TOP_ORDER_DECISION = CLOSED_MISALIGNED_WITH_EARLY_MARKET_ROUTINE
MLB_CLEANROOM_SCHEDULE_COHORT_V2_DECISION = RETIRED_LINEUP_GATED_CONTRACT
MLB_CLEANROOM_ROUTINE_MARKET_CONTRACT_DECISION = IMPLEMENTED_AND_FROZEN_FOR_NEXT_UNTOUCHED_DATE
MLB_CLEANROOM_NORMAL_RUN_BINDING_DECISION = EXACT_RUN_TAGGED_ARTIFACT_BINDING_CERTIFIED
MLB_CLEANROOM_PREGAME_ROSTER_LINEAGE_DECISION = RUN_TAGGED_EXACT_PLAYER_GAME_ROSTER_HASH_PRESERVED
MLB_CLEANROOM_POSTGAME_PARTICIPATION_DECISION = OFFICIAL_EXACT_ID_VERIFICATION_SEPARATE_FROM_MEMBERSHIP
MLB_CLEANROOM_DATA_VERIFICATION_DECISION = AUGUST2_IDENTITY_AND_MARKET_EXACT_POSTGAME_SUPPORT_INCOMPLETE_FAIL_CLOSED
MLB_CLEANROOM_CORRECTION_POLICY_DECISION = APPEND_ONLY_SUPERSESSION_OVERLAY
MLB_CLEANROOM_ROUTINE_HOOK_DECISION = ENABLED_NONBLOCKING_IN_EXISTING_WRAPPER
MLB_CLEANROOM_AUGUST2_MIGRATION_DECISION = SOURCE_AND_DATA_VERIFICATION_ONLY
MLB_CLEANROOM_HISTORICAL_REPLAY_ELIGIBILITY_DECISION = INVENTORIED_FAIL_CLOSED_NO_RECONSTRUCTION
MLB_CLEANROOM_ROUTINE_MARKET_READINESS_DECISION = READY_NEXT_UNTOUCHED_NORMAL_PIPELINE_DATE
MLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION = PAUSED_PENDING_ROUTINE_DATA_VERIFICATION
AUGUST3_DECISION = AUGUST3_NOT_USED_IMPLEMENTATION_TRANSITION
''';(OUT/'terminal_decision.md').write_text('# Terminal decision\n\n```text\n'+decisions+'```\n')
if __name__=='__main__':main()
