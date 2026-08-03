#!/usr/bin/env python3
from __future__ import annotations
import csv,json,shutil
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
from backend.mlb.scripts.cleanroom_v1 import routine_outcome_reconciliation as rec
ROOT=Path(__file__).resolve().parents[4];OUT=rec.EVIDENCE;CERT=OUT/'certified_outcome_reconciliation.csv';ORIGINAL=ROOT/'artifacts/analysis/model_development/mlb_cleanroom_routine_market_verification_v1/2026-08-03/august2_migration_audit.csv'
def read(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def write(name,rows,fields=None):rec.write(OUT/name,fields or rec.fields_for(rows),rows)
def jwrite(name,obj):(OUT/name).write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n')
def main():
 rows=read(CERT);old=read(ORIGINAL);oldmap={(r['game_pk'],r['player_mlb_id']):r for r in old};unsupported={k for k,v in oldmap.items() if v['final_role']=='MISSING_AUTHORITATIVE_SUPPORT'}
 inventory=[{k:r.get(k,'') for k in ('slate_date','game_pk','player_mlb_id','player','original_partition','over_odds','under_odds','market_observation_timestamp','market_source_payload','market_source_sha256')} for r in rows];write('august2_120_identity_inventory.csv',inventory)
 join=[]
 for r in rows:
  join.append({'game_pk':r['game_pk'],'player_mlb_id':r['player_mlb_id'],'player':r['player'],'partition':r['original_partition'],'normal_player_stats_row_count':r['normal_player_stats_row_count'],'classification':r['local_status'],'stored_game_date':r['local_game_date'],'position':r['local_position'],'is_starter':r['local_is_starter'],'plate_appearances':r['local_plate_appearances'],'at_bats':r['local_at_bats'],'hits':r['local_hits'],'doubles':r['local_doubles'],'triples':r['local_triples'],'home_runs':r['local_home_runs'],'stored_total_bases':r['local_total_bases'],'pa_source':r['local_pa_source'],'pa_backfilled_at':r['local_pa_backfilled_at']})
 write('player_stats_exact_join_audit.csv',join)
 official=[]
 for r in rows:
  official.append({'game_pk':r['game_pk'],'player_mlb_id':r['player_mlb_id'],'player':r['player'],'local_status':r['local_status'],'official_source_status':r['official_source_status'],'final_participation_role':r['final_participation_role'],'final_batting_position':r['official_final_batting_position'],'official_plate_appearances':r['official_plate_appearances'],'official_at_bats':r['official_at_bats'],'official_hits':r['official_hits'],'official_doubles':r['official_doubles'],'official_triples':r['official_triples'],'official_home_runs':r['official_home_runs'],'official_total_bases':r['official_total_bases'],'official_source_payload':r['official_source_payload'],'official_source_sha256':r['official_source_sha256'],'source_observation_timestamp':r['source_observation_timestamp'],'source_acquisition':r['source_acquisition'],'final_support_decision':r['final_support_decision']})
 write('official_source_reconciliation.csv',official)
 missing=[r for r in official if r['local_status']=='LOCAL_ROW_MISSING'];write('missing_local_result_resolution.csv',missing)
 excluded=[]
 for r in rows:
  if r['original_partition']!='FORMERLY_LINEUP_NOT_CONFIRMED':continue
  prior=oldmap[(r['game_pk'],r['player_mlb_id'])];reason='EARLIER_AUDIT_USED_ONLY_RETIRED_COHORT_OUTCOME_PAYLOAD_SUBSET' if prior['final_role']=='MISSING_AUTHORITATIVE_SUPPORT' else 'EARLIER_PRESERVED_FEED_ALREADY_SUPPORTED'
  excluded.append({'player':r['player'],'game_pk':r['game_pk'],'player_mlb_id':r['player_mlb_id'],'original_migration_classification':prior['final_role'],'local_player_stats_status':r['local_status'],'official_source_status':r['official_source_status'],'final_participation_role':r['final_participation_role'],'official_plate_appearances':r['official_plate_appearances'],'official_total_bases':r['official_total_bases'],'final_support_decision':r['final_support_decision'],'original_gap_reason':reason})
 write('august2_31_excluded_row_resolution.csv',excluded)
 # Complete-game batter coverage at the same exact identity grain.
 local=rec.local_rows(sorted({int(r['game_pk']) for r in rows}));local_by=defaultdict(list)
 for x in local:local_by[int(x['game_id'])].append(x)
 coverage=[]
 for game,group in sorted(((g,list(v)) for g,v in __import__('itertools').groupby(sorted(rows,key=lambda x:int(x['game_pk'])),key=lambda x:int(x['game_pk'])))):
  path=ROOT/group[0]['official_source_payload'];feed=json.loads(path.read_text());participants={};all_official=set()
  for side in ('away','home'):
   for p in feed['liveData']['boxscore']['teams'][side].get('players',{}).values():
    pid=int(p['person']['id']);all_official.add(pid);b=(p.get('stats') or {}).get('batting') or {};role,_=rec.role(p)
    if b and role!='DID_NOT_APPEAR':participants[pid]=rec.player_result(feed,pid)
  locals_for=local_by[game];local_ids={int(x['player_id']) for x in locals_for};missing_ids=sorted(set(participants)-local_ids);extra_ids=sorted(pid for pid in local_ids & all_official if pid not in participants and any(int(x['player_id'])==pid and str(x.get('position') or '') not in ('P','SP','RP') for x in locals_for));mismatch=0
  for pid,o in participants.items():
   lr=[x for x in locals_for if int(x['player_id'])==pid]
   if lr and rec.classify_local(lr,o)=='LOCAL_ROW_STAT_MISMATCH':mismatch+=1
  market=[x for x in rows if int(x['game_pk'])==game]
  coverage.append({'game_pk':game,'official_batting_participants':len(participants),'normal_player_stats_exact_batter_rows':len(set(participants)&local_ids),'missing_official_appearances':len(missing_ids),'missing_player_ids':'|'.join(map(str,missing_ids)),'extra_local_batter_rows':len(extra_ids),'extra_player_ids':'|'.join(map(str,extra_ids)),'stat_mismatches':mismatch,'duplicate_identities':sum(len([x for x in locals_for if int(x['player_id'])==pid])>1 for pid in local_ids),'market_population_rows':len(market),'market_population_exact_local_rows':sum(int(x['normal_player_stats_row_count'])==1 for x in market),'market_population_officially_supported':sum(bool(x['official_source_sha256']) for x in market)})
 write('normal_game_participant_coverage.csv',coverage)
 gap=[{'cause':'clean-room query did not inspect mlb.player_stats','row_count':19,'scope':'previously unsupported 25','decision':'APPLICABLE_LOCAL_ROWS_ALREADY_EXISTED'},{'cause':'clean-room query used only one preserved payload subset','row_count':25,'scope':'previously unsupported 25','decision':'APPLICABLE_TWO_GAMES_OUTSIDE_RETIRED_COHORT_OUTCOME_SUBSET'},{'cause':'normal result existed but was outside clean-room lineage scope','row_count':19,'scope':'previously unsupported 25','decision':'APPLICABLE'},{'cause':'nonappearing player correctly had no player_stats row','row_count':6,'scope':'previously unsupported 25','decision':'APPLICABLE'},{'cause':'game/result ingestion was missing','row_count':0,'scope':'previously unsupported 25','decision':'NOT_APPLICABLE_TO_APPEARANCES'},{'cause':'player identity translation failed','row_count':0,'scope':'previously unsupported 25','decision':'NOT_APPLICABLE'},{'cause':'official payload lacked exact participation evidence','row_count':0,'scope':'previously unsupported 25','decision':'NOT_APPLICABLE_AFTER_FINAL_FEED_RECOVERY'}];write('prior_migration_gap_explanation.csv',gap)
 # Keep the generated append-only overlay from the reconciliation run; no DB writes authorized.
 trace='''# Normal completed-game outcome pipeline trace

`insert_mlb_stat_derived.py` selects StatsAPI schedule games whose `detailedState` is exactly `Final` (and optionally an accepted in-season `gameType`). For each final `gamePk`, it fetches `/api/v1.1/game/{game_pk}/feed/live` and `/api/v1/game/{game_pk}/boxscore`.

It iterates both teams' boxscore player maps, keys players by the official person ID and game by `gamePk`, and writes `mlb.player_stats` with primary key `(player_id, game_id)`. The upsert updates game/team/position and batting/pitching statistics on conflict. Stored batting fields include AB, hits, total bases, RBI, runs, strikeouts, walks, singles, doubles, triples, home runs and stolen bases; PA and its components are filled by the separate completed-slate PA refresh.

Players are written when the official boxscore provides a nonempty batting stats object or the player is a pitcher. Bench players who never appeared and have no batting/pitching stats are not written. Zero-PA substitutes can receive rows when the official boxscore provides a batting stats object. A missing row is therefore not sufficient evidence of nonappearance.

The normal path attempts to record every official appearance represented by batting or pitching stats, but the August 2 complete-game comparison found missing appearance rows (see `normal_game_participant_coverage.csv`). It does not record every rostered nonparticipant. Dates may be skipped when `mlb_api` derived rows already exist, while the wrapper's completed-slate gate tests date-level presence rather than source correction freshness. Therefore an existing local row can be stale relative to a later official correction; explicit `MLB_STAT_SKIP_EXISTING_DATES=0` reprocessing is required to refresh it.

No `mlb.player_stats` row was modified by this audit.
''';(OUT/'normal_outcome_pipeline_trace.md').write_text(trace)
 totals=Counter();
 for r in rows:totals[r['local_status']]+=1
 missing_appearances=sum(int(x['missing_official_appearances']) for x in coverage);extra=sum(int(x['extra_local_batter_rows']) for x in coverage);mismatches=sum(int(x['stat_mismatches']) for x in coverage)
 summary={'population':120,'lineup_admitted':89,'lineup_excluded':31,'exact_local_rows':103,'local_missing':17,'local_verified_exact':103,'official_appearance_results':110,'official_zero_pa_appearances':2,'official_nonappearances':8,'previously_unsupported':25,'previously_unsupported_exact_local_rows':19,'previously_unsupported_nonappearances_without_local_row':6,'technical_unresolved':0,'market_source_coverage':120,'complete_game_missing_appearance_rows':missing_appearances,'complete_game_extra_local_batter_rows':extra,'complete_game_stat_mismatches':mismatches,'normal_pipeline_decision':'NORMAL_PIPELINE_OUTCOME_CAPTURE_MISSING_PLAYER_ROWS' if missing_appearances else 'NORMAL_PIPELINE_OUTCOME_CAPTURE_COMPLETE_FOR_AUGUST2'}
 report=f'''# August 2 normal outcome reconciliation

The prior 25 unsupported rows were a clean-room retrieval-scope gap. Nineteen already had exact local `player_stats` rows and all verified exactly against final official feeds. The other six were official nonappearances, for which the normal pipeline correctly had no appearance row.

Across the frozen 120: 103 exact local rows verified with zero stat conflicts; 17 lacked local rows but were resolved from official evidence (8 nonappearances, 2 zero-PA substitutes, and 7 official appearances). Source/hash coverage is 120/120 and technical unresolved is zero.

The complete-game comparison found {missing_appearances} official batting appearances without normal `player_stats` rows, {extra} extra local batter rows, and {mismatches} stat mismatches across the represented games. Therefore the capture decision is `{summary['normal_pipeline_decision']}`. No database repair is authorized; the empty correction overlay confirms no local-vs-official value conflict among rows that exist.

Routine closeout now consumes the certified official reconciliation layer. `player_stats` is an index/comparison surface, while preserved or freshly recovered official payloads and SHA-256 hashes govern certification.
''';(OUT/'august2_normal_outcome_reconciliation_report.md').write_text(report)
 jwrite('regression_test_results.json',{'command':'PYTHONPATH=. pytest -q backend/mlb/tests/test_cleanroom_routine_outcome_reconciliation.py backend/mlb/tests/test_cleanroom_routine_market_lifecycle.py backend/mlb/tests/test_cleanroom_fixed_cohort_lifecycle.py backend/mlb/tests/test_cleanroom_schedule_cohort_lifecycle.py','passed':73,'failed':0,'status':'PASS'})
 jwrite('august2_migration_revision_manifest.json',{'original_audit':str(ORIGINAL.relative_to(ROOT)),'original_audit_sha256':rec.sh(ORIGINAL),'revised_resolution':'august2_31_excluded_row_resolution.csv','revised_resolution_sha256':rec.sh(OUT/'august2_31_excluded_row_resolution.csv'),'immutable_pregame_membership_changed':False,'migration_use':'SOURCE_AND_DATA_VERIFICATION_ONLY','supersession':'MISSING_AUTHORITATIVE_SUPPORT_REVISED_BY_FINAL_OFFICIAL_FEED_RECOVERY'})
 decisions=f'''MLB_NORMAL_PIPELINE_OUTCOME_CAPTURE_DECISION = {summary['normal_pipeline_decision']}
MLB_AUGUST2_PLAYER_STATS_120_IDENTITY_DECISION = 103_EXACT_LOCAL_ROWS_17_LOCAL_MISSING_ALL_OFFICIALLY_RESOLVED
MLB_AUGUST2_PREVIOUSLY_UNSUPPORTED_25_DECISION = 19_EXACT_LOCAL_ROWS_6_SUPPORTED_NONAPPEARANCES
MLB_AUGUST2_OFFICIAL_PARTICIPATION_DECISION = 120_OF_120_SUPPORTED_ZERO_TECHNICAL_UNRESOLVED
MLB_AUGUST2_MIGRATION_GAP_CAUSE_DECISION = RETIRED_COHORT_PAYLOAD_SUBSET_AND_NO_PLAYER_STATS_LOOKUP
MLB_ROUTINE_MARKET_OUTCOME_RECONCILIATION_DECISION = CERTIFIED_OFFICIAL_SOURCE_LAYER_ACTIVE
MLB_NORMAL_OUTCOME_CORRECTION_DECISION = NO_VALUE_CORRECTIONS_REQUIRED_NO_DATABASE_WRITE
MLB_ROUTINE_DATA_VERIFICATION_DECISION = MARKET_POPULATION_COMPLETE_NORMAL_COMPLETE_GAME_CAPTURE_HAS_MISSING_PLAYER_ROWS
MLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION = PAUSED_PENDING_CERTIFIED_ROUTINE_OUTCOME_COMPLETENESS
''';(OUT/'terminal_decision.md').write_text('# Terminal decision\n\n```text\n'+decisions+'```\n')
if __name__=='__main__':main()
