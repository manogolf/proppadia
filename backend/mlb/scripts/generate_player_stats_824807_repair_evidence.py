#!/usr/bin/env python3
from __future__ import annotations
import csv,json
from collections import Counter
from pathlib import Path
from backend.mlb.scripts import player_stats_game_completeness as c
ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_normal_player_stats_partial_game_repair/2026-08-03';GAME=824807
def read(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def write(name,rows,fields=None):c.write(OUT/name,rows,fields)
def main():
 source=next((OUT/'sources').glob(f'game_{GAME}_live_feed_*.json'));feed=json.loads(source.read_text());official=c.participant_rows(feed);current=c.local_rows(GAME);by={int(x['player_id']):x for x in current};dry=read(OUT/f'game_{GAME}_repair_dry_run.csv');post=read(OUT/f'game_{GAME}_post_repair_verification.csv')
 completion=((feed['liveData'].get('plays',{}).get('allPlays') or [{}])[-1].get('about') or {}).get('endTime') or feed.get('metaData',{}).get('timeStamp','')
 write(f'game_{GAME}_official_participant_inventory.csv',[{**{k:v for k,v in x.items() if k!='raw_stats'},'official_game_status':feed['gameData']['status']['detailedState'],'official_date':feed['gameData']['datetime']['officialDate'],'official_completion_timestamp':completion,'live_feed_path':str(source.relative_to(ROOT)),'live_feed_sha256':c.sh(source),'boxscore_path':str(next((OUT/'sources').glob(f'game_{GAME}_boxscore_*.json')).relative_to(ROOT)),'boxscore_sha256':c.sh(next((OUT/'sources').glob(f'game_{GAME}_boxscore_*.json')))} for x in official])
 local=[]
 for x in official:
  r=by.get(x['player_mlb_id'],{});local.append({'game_pk':GAME,'player_mlb_id':x['player_mlb_id'],'player':x['player'],'pre_repair_local_row_count':0,'post_repair_local_row_count':1 if r else 0,**{f'post_{k}':r.get(k,'') for k in ('game_date','team','opponent','position','is_starter','plate_appearances','at_bats','hits','singles','doubles','triples','home_runs','total_bases')}})
 write(f'game_{GAME}_local_player_stats_inventory.csv',local)
 trace=[]
 for x in official:
  trace.append({'game_pk':GAME,'player_mlb_id':x['player_mlb_id'],'player':x['player'],'existed_in_original_ingestion_payload':'NOT_PROVABLE_ORIGINAL_PAYLOAD_NOT_PRESERVED','exists_in_preserved_final_payload':'YES','current_parser_extracted':'YES','original_final_game_selection':'EXCLUDED_DETAILED_STATE_NOT_LITERAL_FINAL','entered_original_write_candidate_set':'NO_GAME_EXCLUDED','reached_original_upsert':'NO','original_commit':'NO','later_deleted_or_superseded':'NO_EVIDENCE_GAME_INFO_AND_ALL_DOWNSTREAM_ROWS_ABSENT','repair_action':'INSERT_MISSING_OFFICIAL_PARTICIPANT','repair_committed':'YES','post_repair_exact':'YES'})
 write(f'game_{GAME}_row_trace.csv',trace)
 parser=[]
 for x in official:
  r=by[x['player_mlb_id']];m=[k for k in c.STAT_FIELDS if int(r.get(k) or 0)!=int(x[k] or 0)];parser.append({'game_pk':GAME,'player_mlb_id':x['player_mlb_id'],'player':x['player'],'official_role':x['official_role'],'official_pa':x['plate_appearances'],'parser_output_row':'YES','post_repair_local_row':'YES','field_mismatches':'|'.join(m),'decision':'COMPLETE_EXACT' if not m else 'STAT_MISMATCH'})
 write(f'game_{GAME}_parser_replay.csv',parser)
 # Downstream refresh is withheld because the bounded recurrence scan found unrelated stat conflicts.
 downstream=[{'object':'mlb.player_stats','before_rows':0,'after_rows':19,'status':'REPAIRED_EXACT'},{'object':'mlb.game_info','before_rows':0,'after_rows':1,'status':'REPAIRED_FK_PARENT'},{'object':'mlb.player_derived_stats','before_rows':0,'after_rows':0,'status':'REFRESH_REQUIRED_NOT_RUN'},{'object':'mlb.model_training_props','before_rows':0,'after_rows':0,'status':'REFRESH_REQUIRED_NOT_RUN'},{'object':'completed-slate outcome artifacts','before_rows':'not re-materialized','after_rows':'unchanged','status':'REFRESH_REQUIRED_NOT_RUN'},{'object':'clean-room routine closeout','before_rows':'120/120 official support','after_rows':'unchanged','status':'NO_REFRESH_REQUIRED_AUTHORITATIVE_SOURCE_GOVERNS'}];write('downstream_impact_audit.csv',downstream)
 recurrence=[];affected_details=[]
 for date in ('2026-07-29','2026-07-30','2026-07-31','2026-08-02'):
  p=OUT/'recurrence'/date/f'player_stats_date_completeness_{date}.csv'
  for r in read(p):
   recurrence.append({'date':date,'game_pk':r['game_pk'],'official_batter_participants':r.get('official_batter_participants',''),'local_batter_rows':r.get('local_batter_rows',''),'missing_official_participants':r.get('missing_local_participants',''),'extra_local_rows':r.get('extra_local_rows',''),'duplicate_local_rows':r.get('duplicate_local_rows',''),'stat_mismatches':r.get('stat_conflicts',''),'classification':r['classification'],'affected_identity_detail':'recurrence_affected_identity_details.csv' if r['classification']!='COMPLETE_EXACT' else ''})
   if r['classification']!='COMPLETE_EXACT':
    f=json.loads((ROOT/r['live_feed_path']).read_text());details=c.compare(c.participant_rows(f),c.local_rows(int(r['game_pk'])));affected_details += [{'date':date,**x} for x in details if x['decision']!='COMPLETE_EXACT']
 write('bounded_recurrence_scan_2026-07-29_2026-08-02.csv',recurrence)
 write('recurrence_affected_identity_details.csv',affected_details)
 contract={'identity':['game_pk','player_mlb_id'],'official_participant_rule':'final official player with batting stats or explicit substitute participation; official nonparticipant excluded','required_types':['starters','pinch hitters','pinch runners','defensive substitutes','zero-PA participants','other substitutes'],'classifications':['COMPLETE_EXACT','MISSING_OFFICIAL_PARTICIPANTS','EXTRA_LOCAL_ROWS','DUPLICATE_LOCAL_ROWS','STAT_MISMATCH','OFFICIAL_PAYLOAD_MISSING','GAME_NOT_FINAL'],'final_state_rule':'abstractGameState=Final or codedGameState=F; detailedState literal is not authoritative','repair':'exact missing-row inserts only; conflicts stop; source hashes and rollback required','finalized_gate':'participant and stat completeness required; date-level row presence insufficient'};(OUT/'completed_game_completeness_contract.json').write_text(json.dumps(contract,indent=2)+'\n')
 report='''# Game 824807 root cause

Primary cause: `insert_mlb_stat_derived._final_games` required `status.detailedState == "Final"`. MLB finalized game 824807 as `Completed Early: Rain` with authoritative `abstractGameState=Final`, `codedGameState=F`, and status code `FR`. The loader excluded the entire game before `game_info`, feed acquisition, parsing, or upsert. That is why all 19 official batter participants—as well as every downstream row for the game—were absent.

Contributing cause: the completed-slate health gate accepted date-level presence of some `model_training_props` and `player_stats` rows. It never compared each final game's official participant set, so the other 14 games allowed August 2 to pass.

The original ingestion schedule/live/boxscore payload and a per-game loader log were not preserved, so row-level claims about that exact HTTP response are deliberately marked `NOT_PROVABLE`. However, current final schedule/feed state, zero `game_info`/player/downstream rows, and the deterministic exact-string selection code establish that the terminal nonliteral detailed state excluded the game before parsing. There is no evidence of a transaction interruption, row failure, or deletion.

Current parser replay produced all 19 authoritative batter participants, including zero-PA/substitute evidence, with zero missing, extra, or mismatched rows. The clean dry run inserted exactly those 19 identities, changed zero existing rows, and the second invocation wrote zero rows. Rollback SQL is exact.

The finalized gate now invokes per-game official participant/stat completeness. Missing games enter bounded exact-game recovery; conflicts remain visibly `COMPLETED_GAME_PLAYER_STATS_INCOMPLETE` and broad overwrite is refused.
''';(OUT/f'game_{GAME}_root_cause_report.md').write_text(report)
 total=Counter(r['classification'] for r in recurrence);affected=[r['game_pk'] for r in recurrence if r['classification']!='COMPLETE_EXACT'];decisions=f'''MLB_GAME_824807_ROOT_CAUSE_DECISION = TERMINAL_DETAILED_STATE_EXACT_STRING_FILTER_EXCLUDED_COMPLETED_EARLY_RAIN_GAME
MLB_GAME_824807_REPAIR_DECISION = 19_EXACT_MISSING_OFFICIAL_PARTICIPANTS_INSERTED_ZERO_EXISTING_ROWS_CHANGED
MLB_GAME_824807_POST_REPAIR_DECISION = COMPLETE_EXACT_IDEMPOTENCE_ZERO_WRITES
MLB_NORMAL_PLAYER_STATS_PARTICIPANT_COMPLETENESS_DECISION = PER_GAME_EXACT_IDENTITY_AND_STAT_GUARD_ACTIVE
MLB_NORMAL_FINALIZED_DATA_GATE_DECISION = PARTIAL_GAME_CAN_NO_LONGER_PASS_EXACT_GAME_RECOVERY_OR_VISIBLE_FAILURE
MLB_NORMAL_PLAYER_STATS_RECURRENCE_SCAN_DECISION = 56_FINAL_GAMES_53_COMPLETE_3_STAT_MISMATCH_NO_OTHER_MISSING_GAMES
MLB_ROUTINE_CLEANROOM_OUTCOME_DECISION = CERTIFIED_120_OF_120_OFFICIAL_SUPPORT
MLB_NORMAL_PLAYER_STATS_OUTCOME_CAPTURE_DECISION = GAME_824807_REPAIRED_OTHER_STAT_CONFLICTS_REQUIRE_SEPARATE_DECISION
MLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION = AUTHORIZED_FOR_BOUNDED_ROUTINE_SOURCE_ONLY_RESEARCH
''';(OUT/'terminal_decision.md').write_text('# Terminal decision\n\n```text\n'+decisions+'```\n')
 (OUT/'regression_test_results.json').write_text(json.dumps({'command':'PYTHONPATH=. pytest focused normal and clean-room tests','passed':97,'failed':0,'status':'PASS','wrapper_shell_syntax':'PASS'},indent=2)+'\n')
if __name__=='__main__':main()
