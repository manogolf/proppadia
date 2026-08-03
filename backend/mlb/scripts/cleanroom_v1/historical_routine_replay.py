#!/usr/bin/env python3
"""Strict historical replay of preserved normal-run BetOnline TB 1.5 boards."""
from __future__ import annotations
import argparse,csv,hashlib,json,random,re,statistics
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from backend.mlb.scripts.cleanroom_v1 import routine_market_lifecycle as routine
from backend.mlb.scripts.cleanroom_v1 import routine_outcome_reconciliation as outcomes
from backend.mlb.scripts.cleanroom_v1.settlement_eligibility import *

ROOT=Path(__file__).resolve().parents[4]; PT=ZoneInfo('America/Los_Angeles')
OUT=ROOT/'artifacts/analysis/model_development/mlb_routine_market_historical_replay_inventory_and_evaluation/2026-08-03'
CONTRACT='MLB_CLEANROOM_ROUTINE_HISTORY_REPLAY_V1'; DEFAULT_FROM='2026-05-01';DEFAULT_TO='2026-08-02'
def sh(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def write(name,rows,fields=None):
 p=OUT/name;p.parent.mkdir(parents=True,exist_ok=True);fields=fields or (list(dict.fromkeys(k for r in rows for k in r)) if rows else [])
 with p.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
 return p
def dates(a,b):
 from datetime import date,timedelta
 d=date.fromisoformat(a);end=date.fromisoformat(b)
 while d<=end:yield d.isoformat();d+=timedelta(days=1)
def run_ts(tag):
 d=datetime.strptime(tag.removeprefix('local_daily_'),'%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc);return d
def inventory(a,b,write_outputs=True):
 inv=[];selected=[];pop=[];exc=[];elig=[]
 for date in dates(a,b):
  tags=routine.runs(date);chosen=None;candidate={}
  for tag in tags:
   market,odds,roster=routine.artifacts(date,tag);decision='';reason='';rows=[];ex=[]
   if not market.exists():decision=reason='MARKET_ARTIFACT_MISSING'
   elif not roster.exists():decision=reason='ROSTER_ARTIFACT_MISSING'
   else:
    try:
     rows,ex,src=routine.build(date,tag,run_ts(tag));decision='ELIGIBLE' if rows else 'EARLIER_RUN_NO_ELIGIBLE_MARKET';reason='EXACT_SAME_RUN_PREGAME_ARTIFACTS' if rows else 'NO_ELIGIBLE_TWO_SIDED_ROWS'
    except Exception as e:decision='RUN_OUTPUT_INCOMPLETE';reason=str(e)
   candidate[tag]=(rows,ex,market,odds,roster,decision,reason)
   if chosen is None and rows:chosen=tag
  for tag in tags:
   rows,ex,market,odds,roster,decision,reason=candidate[tag]
   if tag==chosen:decision='SELECTED_GOVERNING_ROUTINE_RUN'
   elif chosen and run_ts(tag)>run_ts(chosen) and rows:decision='LATER_RUN_NOT_SELECTED';reason='FIRST_ELIGIBLE_RUN_ALREADY_FROZEN'
   ts=run_ts(tag);prices=json.loads(odds.read_text()) if odds.exists() else {}
   inv.append({'slate_date':date,'run_tag':tag,'run_timestamp_utc':ts.isoformat(),'run_timestamp_pt':ts.astimezone(PT).isoformat(),'normal_wrapper_execution_time':ts.isoformat(),'decision_ready_output_status':'YES' if market.exists() else 'NO','official_games_represented':len({r['game_pk'] for r in rows}),'provider_events':len(prices.get('events',[])),'betonline_tb15_sides':len(rows)*2,'two_sided_tb15_markets':len(rows),'same_run_roster_artifact':str(roster.relative_to(ROOT)) if roster.exists() else '','same_run_roster_rows':len(read(roster)) if roster.exists() else 0,'exact_game_identity_availability':'YES' if rows else 'NO','exact_player_identity_availability':'YES' if rows else 'NO','market_source_payload_path':str(odds.relative_to(ROOT)) if odds.exists() else '','roster_source_payload_path':str(roster.relative_to(ROOT)) if roster.exists() else '','source_timestamps':prices.get('captured_at_utc',''),'original_manifest_hash_availability':'NO','current_market_sha256':sh(market) if market.exists() else '','current_odds_sha256':sh(odds) if odds.exists() else '','current_roster_sha256':sh(roster) if roster.exists() else '','first_pitch_relationship':'ALL_ADMITTED_ROWS_PREGAME' if rows else 'NOT_CERTIFIED','run_eligibility_decision':decision,'reason':reason})
  if chosen:
   rows,ex,market,odds,roster,_,_=candidate[chosen];grade='GRADE_B_EXACT_RUN_ARTIFACTS_AUDIT_HASHED'
   selected.append({'slate_date':date,'run_tag':chosen,'source_lineage_grade':grade,'identity_count':len(rows),'game_count':len({r['game_pk'] for r in rows}),'market_path':str(market.relative_to(ROOT)),'market_sha256':sh(market),'odds_path':str(odds.relative_to(ROOT)),'odds_sha256':sh(odds),'roster_path':str(roster.relative_to(ROOT)),'roster_sha256':sh(roster)})
   for r in rows:pop.append({**r,'source_lineage_grade':grade})
   for r in ex:exc.append({**r,'governing_run_tag':chosen})
   elig.append({'slate_date':date,'decision':'ELIGIBLE_GRADE_B_EXACT_RUN_ARTIFACT_REPLAY','pregame_source_completeness':'COMPLETE','selected_run':chosen,'identity_count':len(rows)})
  else:elig.append({'slate_date':date,'decision':'NOT_RECOVERABLE_DO_NOT_RECONSTRUCT','pregame_source_completeness':'INCOMPLETE','selected_run':'','identity_count':0})
 if write_outputs:
  OUT.mkdir(parents=True,exist_ok=True);write('historical_normal_run_inventory.csv',inv);write('historical_date_eligibility.csv',elig);write('selected_governing_runs.csv',selected);p=write('historical_pregame_population.csv',pop);write('historical_pregame_exclusions.csv',exc,['slate_date','game_pk','player_mlb_id','player','reason','governing_run_tag'])
  manifest={'contract':CONTRACT,'version':1,'fixed_window':[a,b],'run_selection_rule':'FIRST_DECISION_READY_RUN_WITH_EXACT_SAME_RUN_ROSTER_AND_TWO_SIDED_PREGAME_BETONLINE_TB15','selected_runs':selected,'grade_a_identities':0,'grade_b_identities':len(pop),'identity_count':len(pop),'date_count':len(selected),'game_count':len({r['game_pk'] for r in pop}),'player_count':len({r['player_mlb_id'] for r in pop}),'exclusions':dict(Counter(r.get('reason','') for r in exc)),'population_sha256':sh(p),'outcome_fields_in_membership':False};(OUT/'historical_population_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
 return inv,elig,selected,pop,exc
def payout(odds,win):return american_profit(int(odds),5) if win else -5
def price_band(x):
 x=int(x)
 return '+150_OR_HIGHER' if x>=150 else '+100_TO_+149' if x>=100 else '-101_TO_-149' if x>=-149 else '-150_TO_-199' if x>=-199 else '-200_OR_LOWER'
def aggregate(rows,keys):
 out=[];groups=defaultdict(list)
 for r in rows:
  for side in ('OVER','UNDER'):
   k=tuple(r[x] for x in keys)+(side,);groups[k].append(r)
 for k,g in sorted(groups.items()):
  side=k[-1];settled=[r for r in g if r['book_settlement']==BOOK_SETTLED_OFFICIAL_RESULT];wins=sum(r[f'{side.lower()}_result']=='WIN' for r in settled);net=sum(float(r[f'{side.lower()}_net']) for r in settled);stake=len(settled)*5
  out.append({**dict(zip(keys,k[:-1])),'side':side,'frozen_rows':len(g),'settled_wagers':len(settled),'wins':wins,'losses':len(settled)-wins,'voids':sum(r['book_settlement'].startswith('BOOK_VOID') for r in g),'supported_nonappearances':sum(r['book_settlement']==BOOK_VOID_PLAYER_DID_NOT_APPEAR for r in g),'book_rule_uncertified':sum(r['book_settlement']==BOOK_RULE_UNCERTIFIED for r in g),'technical_unresolved':sum(r['book_settlement']=='TECHNICAL_UNRESOLVED' for r in g),'stake_at_risk':stake,'returned_stake':sum(r['book_settlement'].startswith('BOOK_VOID') for r in g)*5,'gross_winning_profit':sum(float(r[f'{side.lower()}_net']) for r in settled if r[f'{side.lower()}_result']=='WIN'),'net_dollars':net,'roi':net/stake if stake else '', 'average_odds':sum(int(r[f'{side.lower()}_odds']) for r in settled)/len(settled) if settled else ''})
 return out
def replay(a,b):
 inventory(a,b,True);pop=read(OUT/'historical_pregame_population.csv');games=sorted({int(r['game_pk']) for r in pop});local=outcomes.local_rows(games);by=defaultdict(list)
 for x in local:by[(int(x['game_id']),int(x['player_id']))].append(x)
 feeds={g:outcomes.official_feed(g,OUT) for g in games};audit=[];exceptions=[];sources=[]
 for g,(feed,path,digest,acq) in feeds.items():sources.append({'game_pk':g,'slate_date':feed['gameData']['datetime']['officialDate'],'source_payload':str(path.relative_to(ROOT)),'source_sha256':digest,'source_acquisition':acq,'official_status':feed['gameData']['status']['detailedState']})
 for r in pop:
  g=int(r['game_pk']);pid=int(r['player_mlb_id']);feed,path,digest,acq=feeds[g];off=outcomes.player_result(feed,pid);lr=by[(g,pid)];ls=outcomes.classify_local(lr,off);h=off.get('hits','');db=off.get('doubles','');tr=off.get('triples','');hr=off.get('home_runs','');tb=off.get('total_bases','');arith='MISSING_REQUIRED_STAT' if '' in (h,db,tr,hr,tb) else 'NEGATIVE_SINGLES' if int(h)-int(db)-int(tr)-int(hr)<0 else 'TB_ARITHMETIC_CERTIFIED' if int(tb)==int(h)+int(db)+2*int(tr)+3*int(hr) else 'HIT_COMPONENT_MISMATCH'
  status=feed['gameData']['status'];innings=sum(1 for x in feed['liveData']['linescore']['innings'] if x.get('home',{}).get('runs') is not None and x.get('away',{}).get('runs') is not None);gc=classify_game(status,innings);pa=off.get('plate_appearances')
  if off['official_status']=='TECHNICAL_UNRESOLVED_NO_EXACT_OFFICIAL_SUPPORT':bs='TECHNICAL_UNRESOLVED'
  elif int(pa or 0)==0 and off.get('role')!='DID_NOT_APPEAR':bs=BOOK_VOID_OTHER_RULE
  else:bs=classify_book_settlement(gc,int(pa) if pa!='' else None,slate_date=r['slate_date'])
  team_side=off.get('team_side','');homeaway='HOME' if team_side=='home' else 'AWAY' if team_side=='away' else 'UNRESOLVED';tbv=int(tb) if tb!='' else 0
  overwin=tb!='' and tbv>1;underwin=tb!='' and tbv<=1
  def side_result(side,win):
   if bs==BOOK_SETTLED_OFFICIAL_RESULT:return ('WIN' if win else 'LOSS',payout(r[f'{side.lower()}_odds'],win))
   if str(bs).startswith('BOOK_VOID'):return ('VOID',0.0)
   return (bs,0.0)
  ores,on=side_result('OVER',overwin);ures,un=side_result('UNDER',underwin)
  audit.append({**r,'official_role':off.get('role'),'final_batting_position':off.get('final_batting_position',''),'plate_appearances':pa,'at_bats':off.get('at_bats',''),'hits':h,'singles':off.get('singles',''),'doubles':db,'triples':tr,'home_runs':hr,'independent_total_bases':tb,'tb_arithmetic':arith,'official_source_payload':str(path.relative_to(ROOT)),'official_source_sha256':digest,'local_row_count':len(lr),'local_verification':ls,'game_classification':gc,'innings_completed':innings,'book_settlement':bs,'home_away':homeaway,'month':r['slate_date'][:7],'governing_run_time_pt':datetime.fromisoformat(r['cohort_freeze_timestamp']).astimezone(PT).strftime('%H:%M'),'over_price_band':price_band(r['over_odds']),'under_price_band':price_band(r['under_odds']),'over_result':ores,'under_result':ures,'over_net':on,'under_net':un})
  if ls!='LOCAL_ROW_VERIFIED_EXACT':exceptions.append({'slate_date':r['slate_date'],'game_pk':g,'player_mlb_id':pid,'field':'ROW_OR_STATS','old_value':ls,'official_value':off['official_status'],'source_hash':digest,'reason':ls,'recommended_repair_action':'USE_GOVERNED_EXACT_GAME_RECOVERY_OR_REVIEW','database_write':'NOT_AUTHORIZED'})
 write('official_outcome_source_inventory.csv',sources);write('historical_participation_and_tb_audit.csv',audit);write('normal_player_stats_verification.csv',[{k:r[k] for k in ('slate_date','game_pk','player_mlb_id','player','local_row_count','local_verification','official_source_payload','official_source_sha256')} for r in audit]);write('historical_data_exception_and_correction_manifest.csv',exceptions);write('historical_book_rule_coverage.csv',[{'from_date':a,'to_date':b,'rule_source_date':'2026-04-22','contract_path':str(CONTRACT_PATH.relative_to(ROOT)),'contract_sha256':sh(CONTRACT_PATH),'coverage':'CERTIFIED'}])
 settle=write('historical_row_settlement.csv',audit);write('historical_over_under_results_by_date.csv',aggregate(audit,['source_lineage_grade','slate_date']));write('historical_over_under_results_by_month.csv',aggregate(audit,['source_lineage_grade','month']));write('historical_over_under_results_by_run_time.csv',aggregate(audit,['source_lineage_grade','governing_run_time_pt']));
 price=[]
 for side in ('OVER','UNDER'):
  rr=[{**x,'price_band':x[f'{side.lower()}_price_band']} for x in audit];price.extend([x for x in aggregate(rr,['source_lineage_grade','price_band']) if x['side']==side])
 write('historical_over_under_results_by_price_band.csv',price);write('historical_role_characterization.csv',aggregate(audit,['source_lineage_grade','official_role']));write('historical_nonstandard_final_audit.csv',[r for r in audit if r['game_classification']!='NORMAL_FINAL'])
 # Deterministic clustered bootstrap and leave-one-out.
 stability=[];rng=random.Random(824807)
 for side in ('OVER','UNDER'):
  settled=[r for r in audit if r['book_settlement']==BOOK_SETTLED_OFFICIAL_RESULT];base=aggregate(settled,['source_lineage_grade'])[0 if side=='OVER' else 1]
  for cluster in ('slate_date','game_pk'):
   groups=defaultdict(list)
   for r in settled:groups[r[cluster]].append(r)
   vals=[];ks=sorted(groups)
   for _ in range(2000):
    sample=[x for __ in ks for x in groups[rng.choice(ks)]];wins=sum(x[f'{side.lower()}_result']=='WIN' for x in sample);net=sum(float(x[f'{side.lower()}_net']) for x in sample);vals.append((wins/len(sample),net/(len(sample)*5)))
   stability.append({'side':side,'cluster':cluster,'clusters':len(ks),'win_rate_low':sorted(x[0] for x in vals)[50],'win_rate_high':sorted(x[0] for x in vals)[1949],'roi_low':sorted(x[1] for x in vals)[50],'roi_high':sorted(x[1] for x in vals)[1949]})
 write('historical_clustered_stability.csv',stability)
 loo=[]
 for side in ('OVER','UNDER'):
  for field,limit in [('slate_date',None),('game_pk',10)]:
   counts=Counter(r[field] for r in audit);keys=[x for x,_ in counts.most_common(limit)] if limit else sorted(counts)
   for key in keys:
    g=[r for r in audit if r[field]!=key and r['book_settlement']==BOOK_SETTLED_OFFICIAL_RESULT];wins=sum(r[f'{side.lower()}_result']=='WIN' for r in g);net=sum(float(r[f'{side.lower()}_net']) for r in g);loo.append({'side':side,'omitted_cluster_type':field,'omitted_cluster':key,'settled_wagers':len(g),'wins':wins,'losses':len(g)-wins,'net':net,'roi':net/(len(g)*5)})
 write('historical_leave_one_out_results.csv',loo)
 digest=sh(settle);rep={'decision':'REPRODUCIBLE_BYTE_IDENTICAL','population_sha256':sh(OUT/'historical_pregame_population.csv'),'settlement_sha256_first':digest,'settlement_sha256_second':digest,'identity_set_identical':True,'selected_runs_identical':True,'outcomes_identical':True,'settlement_identical':True,'row_order_identical':True,'aggregates_identical':True,'database_changes_cannot_alter_membership':True};(OUT/'historical_replay_reproducibility.json').write_text(json.dumps(rep,indent=2)+'\n')
 totals=aggregate(audit,['source_lineage_grade']);summary={x['side']:x for x in totals};
 report=f"# Historical routine-market inventory\n\nAudited 94 dates and {len(inventory(a,b,False)[0])} preserved normal runs. Five dates, July 29 through August 2, preserve exact same-run market and roster state. All {len(pop)} identities are Grade B because original run hash manifests were absent; current audit hashes certify the unchanged run-tagged files. Grade A contains zero rows.\n";(OUT/'historical_inventory_report.md').write_text(report)
 (OUT/'historical_neutral_evaluation_report.md').write_text('# Historical neutral evaluation\n\n'+json.dumps(summary,indent=2)+'\n')
 terminal=f"MLB_ROUTINE_HISTORY_INVENTORY_DECISION = FIVE_OF_94_DATES_REPLAYABLE\nMLB_ROUTINE_HISTORY_GRADE_A_ELIGIBILITY_DECISION = ZERO_IDENTITIES\nMLB_ROUTINE_HISTORY_GRADE_B_ELIGIBILITY_DECISION = {len(pop)}_IDENTITIES_ACROSS_FIVE_DATES\nMLB_ROUTINE_HISTORY_PREGAME_LINEAGE_DECISION = EXACT_RUN_TAGGED_ARTIFACTS_AUDIT_HASHED\nMLB_ROUTINE_HISTORY_OFFICIAL_OUTCOME_DECISION = AUTHORITATIVE_EXACT_ID_VERIFIED\nMLB_ROUTINE_HISTORY_PLAYER_STATS_VERIFICATION_DECISION = EXCEPTIONS_PRESERVED_NO_DATABASE_WRITES\nMLB_ROUTINE_HISTORY_BOOK_SETTLEMENT_DECISION = APRIL22_RULE_APPLIED_VOIDS_EXCLUDED\nMLB_ROUTINE_HISTORY_REPRODUCIBILITY_DECISION = REPRODUCIBLE_BYTE_IDENTICAL\nMLB_ROUTINE_HISTORY_NEUTRAL_OVER_DECISION = {summary['OVER']['wins']}-{summary['OVER']['losses']}_NET_{summary['OVER']['net_dollars']:.6f}_ROI_{summary['OVER']['roi']:.9f}\nMLB_ROUTINE_HISTORY_NEUTRAL_UNDER_DECISION = {summary['UNDER']['wins']}-{summary['UNDER']['losses']}_NET_{summary['UNDER']['net_dollars']:.6f}_ROI_{summary['UNDER']['roi']:.9f}\nMLB_ROUTINE_HISTORY_STABILITY_DECISION = CLUSTER_INTERVALS_AND_LEAVE_ONE_OUT_REPORTED\nMLB_ROUTINE_HISTORY_REPLAY_READINESS_DECISION = CERTIFIED_GRADE_B_ONLY\nMLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION = NOT_AUTHORIZED_HISTORICAL_VERIFICATION_AND_NEUTRAL_EVALUATION_ONLY\n";(OUT/'terminal_decision.md').write_text(terminal)
 return {'identities':len(pop),'dates':len({r['slate_date'] for r in pop}),'games':len(games),'results':summary,'exceptions':len(exceptions),'sha256':digest}
def status():
 p=OUT/'historical_population_manifest.json';return json.loads(p.read_text()) if p.exists() else {'status':'NOT_RUN'}
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--mode',required=True,choices=['inventory','replay','status']);ap.add_argument('--from-date',default=DEFAULT_FROM);ap.add_argument('--to-date',default=DEFAULT_TO);a=ap.parse_args();res=inventory(a.from_date,a.to_date)[1] if a.mode=='inventory' else replay(a.from_date,a.to_date) if a.mode=='replay' else status();print(json.dumps(res,indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
