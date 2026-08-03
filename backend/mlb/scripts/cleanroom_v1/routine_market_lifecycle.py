#!/usr/bin/env python3
"""Immutable early routine-market cohort and authoritative postgame verification."""
from __future__ import annotations
import argparse,csv,hashlib,json,os,re,shutil,tempfile
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import requests

ROOT=Path(__file__).resolve().parents[4]; PT=ZoneInfo('America/Los_Angeles')
OUT=ROOT/'backend/mlb/exports/cleanroom_v1/bol_tb15/routine_cohorts'
NORMAL=ROOT/'backend/mlb/exports/model_v2/bol_tb15'; ODDS=ROOT/'backend/mlb/exports/odds_history'
CONTRACT='MLB_CLEANROOM_BOL_TB15_ROUTINE_MARKET_COHORT_V1'
TEAM_NAMES={'ARI':'Arizona Diamondbacks','ATH':'Athletics','OAK':'Athletics','ATL':'Atlanta Braves','BAL':'Baltimore Orioles','BOS':'Boston Red Sox','CHC':'Chicago Cubs','CWS':'Chicago White Sox','CIN':'Cincinnati Reds','CLE':'Cleveland Guardians','COL':'Colorado Rockies','DET':'Detroit Tigers','HOU':'Houston Astros','KC':'Kansas City Royals','LAA':'Los Angeles Angels','LAD':'Los Angeles Dodgers','MIA':'Miami Marlins','MIL':'Milwaukee Brewers','MIN':'Minnesota Twins','NYM':'New York Mets','NYY':'New York Yankees','PHI':'Philadelphia Phillies','PIT':'Pittsburgh Pirates','SD':'San Diego Padres','SEA':'Seattle Mariners','SF':'San Francisco Giants','STL':'St. Louis Cardinals','TB':'Tampa Bay Rays','TEX':'Texas Rangers','TOR':'Toronto Blue Jays','WSH':'Washington Nationals'}
FIELDS=['slate_date','game_pk','player_mlb_id','player','team','opponent','scheduled_first_pitch_utc','normal_pipeline_run_tag','cohort_freeze_timestamp','provider_event_id','roster_observation_timestamp','roster_ingestion_run','roster_source_payload','roster_source_sha256','over_odds','under_odds','market_observation_timestamp','market_ingestion_run','market_source_payload','market_source_sha256','membership_decision','membership_reason']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def source_path(p):
 try:return str(p.relative_to(ROOT))
 except ValueError:return str(p)
def read_csv(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def csv_bytes(fields,rows):
 import io;s=io.StringIO();w=csv.DictWriter(s,fieldnames=fields,lineterminator='\n');w.writeheader();w.writerows(rows);return s.getvalue().encode()
def current_date():return datetime.now(timezone.utc).astimezone(PT).date().isoformat()
def guard_current(slate):
 if slate!=current_date():raise RuntimeError(f'CAPTURE_DATE_MISMATCH requested_date={slate} current_local_date={current_date()}')
def runs(slate):
 d=ODDS/slate
 return sorted(re.match(r'odds_mlb_playerprops__(local_daily_\d{8}T\d{6}Z)\.json$',p.name).group(1) for p in d.glob('odds_mlb_playerprops__local_daily_*.json'))
def artifacts(slate,tag):
 market=NORMAL/slate/'snapshots'/tag/'bol_tb15_market_rows.csv'; odds=ODDS/slate/f'odds_mlb_playerprops__{tag}.json'; roster=ODDS/slate/f'mlb_predictions_wide_calibrated__{tag}.csv'
 return market,odds,roster
def verify_source(p):
 if not p.exists() or not p.is_file() or not p.stat().st_size:raise RuntimeError(f'MISSING_AUTHORITATIVE_SUPPORT {p}')
 return sha(p)
def pitch_map(roster):
 out={}
 for r in read_csv(roster):
  if r.get('game_id') and r.get('game_time'):out[int(float(r['game_id']))]=r['game_time']
 return out
def build(slate,tag,freeze):
 market,odds,roster=artifacts(slate,tag); mh,oh,rh=map(verify_source,(market,odds,roster)); prices=json.loads(odds.read_text()); pitch=pitch_map(roster)
 capture=prices.get('captured_at_utc') or freeze.isoformat(); rows=[];excluded=[]
 roster_keys={(int(float(r['game_id'])),int(float(r['player_id']))):r for r in read_csv(roster) if r.get('game_id') and r.get('player_id')}
 for r in read_csv(market):
  game=int(r['game_pk']);pid=int(r['batter_mlb_id']);key=(game,pid); rr=roster_keys.get(key); reason='ELIGIBLE'
  if not rr:reason='PLAYER_NOT_ON_SAME_RUN_ROSTER'
  elif not r.get('over_odds') or not r.get('under_odds'):reason='MARKET_NOT_TWO_SIDED'
  elif game not in pitch:reason='PROVIDER_EVENT_MISSING'
  else:
   try:
    first=datetime.fromisoformat(pitch[game]).astimezone(timezone.utc); observed=datetime.fromisoformat(str(r['source_timestamp']).replace('Z','+00:00'))
    if observed>=first:reason='MARKET_POST_FIRST_PITCH'
   except Exception:reason='MISSING_AUTHORITATIVE_SUPPORT'
  if reason!='ELIGIBLE':excluded.append({'slate_date':slate,'game_pk':game,'player_mlb_id':pid,'player':r.get('player',''),'reason':reason});continue
  away,home=r.get('game','').split(' @ '); matches=[e for e in prices.get('events',[]) if e.get('away_team')==TEAM_NAMES.get(away) and e.get('home_team')==TEAM_NAMES.get(home)]
  if len(matches)>1 and all(e.get('commence_time') for e in matches):
   matches=[e for e in matches if e.get('commence_time') and datetime.fromisoformat(str(e['commence_time']).replace('Z','+00:00'))==first]
  if len(matches)!=1:
   excluded.append({'slate_date':slate,'game_pk':game,'player_mlb_id':pid,'player':r.get('player',''),'reason':'EVENT_BINDING_AMBIGUOUS' if len(matches)>1 else 'PROVIDER_EVENT_MISSING'});continue
  provider=str(matches[0].get('id',''))
  rows.append({'slate_date':slate,'game_pk':game,'player_mlb_id':pid,'player':r['player'],'team':r['team'],'opponent':r['opponent'],'scheduled_first_pitch_utc':first.isoformat(),'normal_pipeline_run_tag':tag,'cohort_freeze_timestamp':freeze.isoformat(),'provider_event_id':provider,'roster_observation_timestamp':capture,'roster_ingestion_run':tag,'roster_source_payload':source_path(roster),'roster_source_sha256':rh,'over_odds':r['over_odds'],'under_odds':r['under_odds'],'market_observation_timestamp':r['source_timestamp'],'market_ingestion_run':tag,'market_source_payload':source_path(odds),'market_source_sha256':oh,'membership_decision':'ROUTINE_MARKET_BASELINE','membership_reason':'SAME_NORMAL_RUN_EXACT_GAME_PLAYER_ROSTER_TWO_SIDED_PREGAME'})
 rows.sort(key=lambda x:(x['game_pk'],x['player_mlb_id']));return rows,excluded,{'market_ledger_sha256':mh,'odds_sha256':oh,'roster_sha256':rh,'capture_timestamp':capture}
def cohort(slate,tag=None,now=None):
 guard_current(slate); out=OUT/slate; manifest=out/'routine_market_manifest.json'
 if manifest.exists():return {**json.loads(manifest.read_text()),'decision':'ROUTINE_COHORT_ALREADY_FROZEN'}
 examined=[]
 state=out/'normal_runs_examined.json'
 if state.exists():examined=json.loads(state.read_text()).get('runs',[])
 available=runs(slate); candidates=[tag] if tag else [x for x in available if x not in examined]
 if not candidates:raise RuntimeError('NO_COMPLETED_NORMAL_RUN_AVAILABLE')
 selected=candidates[0]
 if selected not in available:raise RuntimeError('NORMAL_RUN_NOT_DECISION_READY')
 freeze=now or datetime.now(timezone.utc); baseline,exclusions,source=build(slate,selected,freeze)
 if not baseline:
  out.mkdir(parents=True,exist_ok=True); examined.append(selected); state.write_text(json.dumps({'date':slate,'runs':examined,'last_decision':'ROUTINE_RUN_NO_ELIGIBLE_MARKET'},indent=2)+'\n');return {'status':'ROUTINE_RUN_NO_ELIGIBLE_MARKET','governing_normal_run':selected,'normal_runs_examined':len(examined)}
 out.parent.mkdir(parents=True,exist_ok=True);stage=Path(tempfile.mkdtemp(dir=out.parent,prefix=f'.{slate}_routine_'))
 try:
  (stage/'routine_market_baseline.csv').write_bytes(csv_bytes(FIELDS,baseline));(stage/'routine_market_exclusions.csv').write_bytes(csv_bytes(['slate_date','game_pk','player_mlb_id','player','reason'],exclusions))
  correction=['slate_date','game_pk','player_mlb_id','field','old_value','corrected_value','classification','source_path','source_sha256','correction_timestamp','reason'];(stage/'correction_overlay.csv').write_bytes(csv_bytes(correction,[]))
  m={'contract':CONTRACT,'slate_date':slate,'governing_normal_run':selected,'freeze_timestamp_utc':freeze.isoformat(),'status':'ROUTINE_MARKET_BASELINE_FROZEN','frozen_market_identities':len(baseline),'games_represented':len({r['game_pk'] for r in baseline}),'identity_exclusions':len(exclusions),'normal_runs_examined':len(examined)+1,'source_lineage':source,'lineup_required':False,'batting_order_used':False,'outcome_fields_present':False,'baseline_sha256':sha(stage/'routine_market_baseline.csv')}
  (stage/'routine_market_manifest.json').write_text(json.dumps(m,indent=2)+'\n');os.replace(stage,out);return m
 finally:
  if stage.exists():shutil.rmtree(stage)
def role(player):
 b=(player.get('stats') or {}).get('batting') or {};pa=int(b.get('plateAppearances') or 0);status=player.get('gameStatus') or {};order=str(player.get('battingOrder') or '')
 if order and not status.get('isSubstitute'):return 'STARTED',int(order[0])
 if status.get('isSubstitute') and pa>0:return 'PINCH_HITTER',int(order[0]) if order else ''
 if status.get('isSubstitute') and int(b.get('runs') or 0)>0:return 'PINCH_RUNNER',''
 if status.get('isSubstitute'):return 'OTHER_SUBSTITUTE',''
 if status.get('isOnBench') and pa==0:return 'DID_NOT_APPEAR',''
 return 'ROLE_AMBIGUOUS_TECHNICAL_UNRESOLVED',''
def closeout(slate):
 out=OUT/slate;mp=out/'routine_market_manifest.json'
 if not mp.exists():raise RuntimeError('ROUTINE_COHORT_FREEZE_REQUIRED')
 from backend.mlb.scripts.cleanroom_v1 import routine_outcome_reconciliation as certified
 frozen,audit,corrections,feeds,_=certified.reconcile(slate,out);certified.write(out/'certified_outcome_reconciliation.csv',certified.fields_for(audit),audit);certified.write(out/'correction_overlay_manifest.csv',certified.fields_for(corrections) if corrections else ['slate_date','game_pk','player_mlb_id','field','old_value','corrected_value','source_payload','source_sha256','reason','discovery_timestamp','database_write'],corrections)
 from backend.mlb.scripts.cleanroom_v1.settlement_eligibility import classify_game,classify_book_settlement
 game_classes={int(g):classify_game(feed[0]['gameData']['status'],sum(1 for x in feed[0].get('liveData',{}).get('linescore',{}).get('innings',[]) if x.get('home',{}).get('runs') is not None and x.get('away',{}).get('runs') is not None)) for g,feed in feeds.items()}
 results=[]
 for r in audit:
  participation=r['final_participation_role'];pa=r['official_plate_appearances'];tb=r['official_total_bases']
  if not r['official_game_final']:settle,outcome='PENDING','PENDING'
  elif r['official_source_status']=='OFFICIAL_NONAPPEARANCE_SUPPORTED' or (str(pa)!='' and int(pa)==0):settle,outcome='NO_ACTION','NO_ACTION'
  elif 'UNRESOLVED' in r['final_support_decision'] or r['official_source_status']=='OFFICIAL_ROLE_ONLY_RESULT_MISSING':settle,outcome='TECHNICAL_UNRESOLVED','TECHNICAL_UNRESOLVED'
  else:settle,outcome='SETTLED','OVER_WIN' if int(tb)>1 else 'OVER_LOSS'
  game_class=game_classes.get(int(r['game_pk']),'NONSTANDARD_FINAL_REQUIRES_BOOK_RULE')
  book_settlement=classify_book_settlement(game_class,int(pa) if str(pa)!='' else None,slate_date=slate) if r['official_game_final'] else 'BOOK_SETTLEMENT_PENDING'
  results.append({**r,'final_participation':participation,'final_batting_position':r['official_final_batting_position'],'plate_appearances':pa,'at_bats':r['official_at_bats'],'hits':r['official_hits'],'doubles':r['official_doubles'],'triples':r['official_triples'],'home_runs':r['official_home_runs'],'total_bases':tb,'official_game_classification':game_class,'official_outcome':outcome,'book_settlement':book_settlement,'settlement_status':settle,'outcome':outcome,'outcome_source':r['official_source_payload'],'outcome_sha256':r['official_source_sha256']})
 fields=list(results[0]) if results else FIELDS;data=csv_bytes(fields,results);digest=hashlib.sha256(data).hexdigest();cm=out/'routine_market_closeout_manifest.json';prior=json.loads(cm.read_text()) if cm.exists() else {}
 if prior.get('content_sha256')==digest:return {**prior,'changed':False}
 revision=int(prior.get('revision',0))+1;c=Counter(r['final_participation'] for r in results);s=Counter(r['settlement_status'] for r in results);o=Counter(r['outcome'] for r in results);status='FINAL' if not s['PENDING'] and not s['TECHNICAL_UNRESOLVED'] else 'OUTCOME_CLOSEOUT_PENDING'
 (out/'routine_market_closeout_rows.csv').write_bytes(data);manifest={'slate_date':slate,'revision':revision,'status':status,'content_sha256':digest,'final_games':sum(d['gameData']['status']['abstractGameState']=='Final' for d,_,_,_ in feeds.values()),'roles':dict(c),'settlements':dict(s),'outcomes':dict(o),'data_discrepancies':sum(r['local_status']=='LOCAL_ROW_STAT_MISMATCH' for r in audit),'corrections_created':len(corrections),'certified_outcome_reconciliation_sha256':sha(out/'certified_outcome_reconciliation.csv')};cm.write_text(json.dumps(manifest,indent=2)+'\n');return manifest
def status(slate):
 out=OUT/slate;m=json.loads((out/'routine_market_manifest.json').read_text()) if (out/'routine_market_manifest.json').exists() else {};c=json.loads((out/'routine_market_closeout_manifest.json').read_text()) if (out/'routine_market_closeout_manifest.json').exists() else {};attempt=json.loads((out/'normal_runs_examined.json').read_text()) if (out/'normal_runs_examined.json').exists() else {}
 return {'normal_runs_examined':m.get('normal_runs_examined',len(attempt.get('runs',[]))),'governing_normal_run':m.get('governing_normal_run'),'cohort_freeze_status':m.get('status','NOT_FROZEN'),'frozen_market_identities':m.get('frozen_market_identities',0),'identity_exclusions':m.get('identity_exclusions',0),'games_represented':m.get('games_represented',0),'final_games':c.get('final_games',0),'starters':c.get('roles',{}).get('STARTED',0),'pinch_hitters':c.get('roles',{}).get('PINCH_HITTER',0),'pinch_runners':c.get('roles',{}).get('PINCH_RUNNER',0),'other_substitutes':c.get('roles',{}).get('OTHER_SUBSTITUTE',0),'did_not_appear':c.get('roles',{}).get('DID_NOT_APPEAR',0),'NO_ACTION':c.get('settlements',{}).get('NO_ACTION',0),'wins':c.get('outcomes',{}).get('OVER_WIN',0),'losses':c.get('outcomes',{}).get('OVER_LOSS',0),'pending':c.get('settlements',{}).get('PENDING',0),'technical_unresolved':c.get('settlements',{}).get('TECHNICAL_UNRESOLVED',0),'data_discrepancies':c.get('data_discrepancies',0),'corrections_created':c.get('corrections_created',0),'closeout_revision':c.get('revision',0)}
def verification_status():return {'contract':CONTRACT,'dates':[status(p.name) | {'slate_date':p.name} for p in sorted(OUT.glob('????-??-??'))]}
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--date');ap.add_argument('--run-tag');ap.add_argument('--mode',required=True,choices=['cohort','closeout','status','verification-status']);a=ap.parse_args()
 if a.mode!='verification-status' and not a.date:ap.error('--date required')
 result=cohort(a.date,a.run_tag) if a.mode=='cohort' else closeout(a.date) if a.mode=='closeout' else status(a.date) if a.mode=='status' else verification_status();print(json.dumps(result,indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
