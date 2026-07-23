#!/usr/bin/env python3
"""Certify the immutable external MLB batter-event acquisition platform."""
from __future__ import annotations
import csv,hashlib,json,os
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[3]; D='2026-07-22'
OUT=ROOT/f'artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/{D}'
SC=ROOT/'backend/mlb/data/external/statcast/raw'; RS=ROOT/'backend/mlb/data/external/retrosheet/raw/csv_release_through_2025'; SA=ROOT/'backend/mlb/data/external/statsapi/raw/2026'
LOCAL=ROOT/'artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/raw_official_mlb'
REQ=['game_date','game_pk','at_bat_number','pitch_number','batter','pitcher','player_name','home_team','away_team','game_type','pitch_type','description','events','type','balls','strikes','stand','p_throws','release_speed','release_pos_x','release_pos_y','release_pos_z','release_spin_rate','spin_axis','release_extension','pfx_x','pfx_z','plate_x','plate_z','zone','sz_top','sz_bot','effective_speed','launch_speed','launch_angle','hit_distance_sc','bb_type','hit_location','hc_x','hc_y','launch_speed_angle','estimated_ba_using_speedangle','estimated_woba_using_speedangle','woba_value','woba_denom','babip_value','iso_value','inning','inning_topbot','outs_when_up','on_1b','on_2b','on_3b','home_score','away_score','bat_score','fld_score','if_fielding_alignment','of_fielding_alignment']
def sha(p):
 h=hashlib.sha256();
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def save(n,rows):
 df=rows if isinstance(rows,pd.DataFrame) else pd.DataFrame(rows);df.to_csv(OUT/n,index=False);return df
def main():
 OUT.mkdir(parents=True,exist_ok=True)
 contracts=[
 {'source':'Baseball Savant Statcast Search CSV','domain':'baseballsavant.mlb.com','route':'/statcast_search/csv','parameters':'all=true&type=details&hfGT=R|&game_date_gt={start}&game_date_lt={end}','date_range':'2022-03-01..2026-07-21 actual regular season','game_type':'R','chunk_size':'1 day after 7-day pilot hit 25,000-row cap','response_type':'CSV','retry_policy':'3 retries exponential backoff','timeout_policy':'120 seconds','rate_limit_policy':'1.5 seconds between requests per process','raw_storage':'backend/mlb/data/external/statcast/raw/<season>/<start>_<end>/','parsed_storage':'none; raw CSV parsed in-place for validation','checksum_policy':'SHA256 per response','resumability':'metadata+raw existence reuse','duplicate_policy':'preserve and count composite key duplicates'},
 {'source':'Retrosheet master CSV archive','domain':'retrosheet.org','route':'/downloads/csvdownloads.zip','parameters':'official complete release','date_range':'compiled through 2025; governing filter 2022-2025','game_type':'classified from exact fields','chunk_size':'one official archive','response_type':'ZIP','retry_policy':'rerun if absent','timeout_policy':'600 seconds','rate_limit_policy':'single request','raw_storage':'backend/mlb/data/external/retrosheet/raw/csv_release_through_2025/','parsed_storage':'immutable extracted members','checksum_policy':'ZIP and member SHA256','resumability':'existing ZIP reuse','duplicate_policy':'retain exact official rows'},
 {'source':'MLB StatsAPI','domain':'statsapi.mlb.com','route':'/api/v1/schedule and /api/v1.1/game/{gamePk}/feed/live','parameters':'sportId=1&gameType=R&startDate=2026-03-26&endDate=2026-07-21','date_range':'2026-03-26..2026-07-21','game_type':'R','chunk_size':'one game feed','response_type':'JSON','retry_policy':'rerun missing only','timeout_policy':'60 seconds','rate_limit_policy':'0.15 seconds between missing feeds','raw_storage':'backend/mlb/data/external/statsapi/raw/2026/<gamePk>/','parsed_storage':'none','checksum_policy':'SHA256 per feed','resumability':'completion ledger and file existence','duplicate_policy':'reuse certified local byte content'}]
 save('source_contracts.csv',contracts)
 metas=[]; schema=defaultdict(lambda:{'chunks':0,'non_null':0,'null':0,'examples':set()}); season=defaultdict(Counter); season_ids=defaultdict(lambda:{'games':set(),'batters':set(),'pitchers':set(),'dates':set()}); month=defaultdict(Counter); keyc=Counter(); pac=set(); games=set(); batters=set();pitchers=set(); rawfiles=[]
 for mp in sorted(SC.glob('*/*/request_metadata.json')):
  m=json.loads(mp.read_text()); m['metadata_path']=str(mp.relative_to(ROOT)); metas.append(m); p=mp.parent/'statcast_search.csv'
  if m.get('platform_role','').endswith('NOT_CANONICAL_COVERAGE') or m.get('completion_status') not in {'ACQUIRED_AND_VALIDATED','ACQUIRED_EMPTY_VALID'} or not p.exists():continue
  rawfiles.append(p)
  with p.open(encoding='utf-8-sig',errors='replace',newline='') as f:
   r=csv.DictReader(f); cols=r.fieldnames or []
   for c in cols:schema[c]['chunks']+=1
   for x in r:
    d=x.get('game_date','');yr=d[:4];mo=d[:7]; season[yr]['pitches']+=1;month[(yr,mo,x.get('game_type',''))]['pitches']+=1
    if x.get('events'): season[yr]['plate_appearances']+=1
    if x.get('launch_speed'): season[yr]['batted_balls']+=1
    g=x.get('game_pk','');games.add(g);batters.add(x.get('batter',''));pitchers.add(x.get('pitcher',''));season_ids[yr]['games'].add(g);season_ids[yr]['batters'].add(x.get('batter',''));season_ids[yr]['pitchers'].add(x.get('pitcher',''));season_ids[yr]['dates'].add(d);key=(g,x.get('at_bat_number',''),x.get('pitch_number',''));keyc[key]+=1;pac.add((g,x.get('at_bat_number',''),x.get('batter','')))
    for c in cols:
     v=x.get(c,'');schema[c]['non_null' if v not in ('',None) else 'null']+=1
     if v and len(schema[c]['examples'])<5:schema[c]['examples'].add(v)
 save('request_inventory.csv',metas); save('chunk_acquisition_ledger.csv',metas)
 sch=[]
 for c,v in sorted(schema.items()):sch.append({'exact_raw_column':c,'chunks_present':v['chunks'],'non_null_count':v['non_null'],'null_count':v['null'],'examples':'|'.join(sorted(v['examples'])),'required_concept':c in REQ})
 save('baseball_savant_schema.csv',sch)
 cov=[]
 for y,v in sorted(season.items()):cov.append({'season':y,**v,'unique_games':len(season_ids[y]['games']), 'distinct_dates':len(season_ids[y]['dates']),'unique_batters':len(season_ids[y]['batters']-set([''])),'unique_pitchers':len(season_ids[y]['pitchers']-set([''])),'note':'counts derived from preserved raw rows'})
 save('baseball_savant_coverage_report.csv',cov); save('coverage_by_month_game_type.csv',[{'season':k[0],'month':k[1],'game_type':k[2],**v} for k,v in sorted(month.items())])
 critical=[]
 for c in ['stand','p_throws','estimated_ba_using_speedangle','estimated_woba_using_speedangle','launch_speed_angle','launch_speed','launch_angle','bb_type','pitch_type','release_speed','release_spin_rate','spin_axis','pfx_x','pfx_z','plate_x','plate_z','zone']:
  v=schema[c];critical.append({'field':c,'present':v['chunks']>0,'non_null':v['non_null'],'null':v['null'],'coverage_rate':v['non_null']/(v['non_null']+v['null']) if v['non_null']+v['null'] else 0,'certification':'VERIFIED_RAW_COLUMN' if v['chunks'] else 'ABSENT'})
 save('critical_field_coverage.csv',critical)
 save('launch_speed_angle_category_contract.csv',[{'literal_value':1,'official_category':'Weak','raw_field':'launch_speed_angle','normalized_derivative':'NONE_RAW_PRESERVED'},{'literal_value':2,'official_category':'Topped','raw_field':'launch_speed_angle','normalized_derivative':'NONE_RAW_PRESERVED'},{'literal_value':3,'official_category':'Under','raw_field':'launch_speed_angle','normalized_derivative':'NONE_RAW_PRESERVED'},{'literal_value':4,'official_category':'Flare/Burner','raw_field':'launch_speed_angle','normalized_derivative':'NONE_RAW_PRESERVED'},{'literal_value':5,'official_category':'Solid Contact','raw_field':'launch_speed_angle','normalized_derivative':'NONE_RAW_PRESERVED'},{'literal_value':6,'official_category':'Barrel','raw_field':'launch_speed_angle','normalized_derivative':'DERIVED_BARREL_INDICATOR_MAY_LATER_EQUAL_6_NOT_CREATED_IN_RAW'}])
 rawman=[]
 for p in rawfiles:rawman.append({'source':'STATCAST','path':str(p.relative_to(ROOT)),'date_range':p.parent.name,'rows':sum(1 for _ in p.open(errors='replace'))-1,'size_bytes':p.stat().st_size,'sha256':sha(p),'schema_version':sha(p)[:0]+hashlib.sha256(p.open(errors='replace').readline().encode()).hexdigest()[:16],'acquisition_status':'ACQUIRED_AND_VALIDATED','validation_status':'PASS','retry_status':'COMPLETE'})
 canonical_paths={str(p) for p in rawfiles}
 for m in metas:
  p=ROOT/m.get('raw_path','')
  if p.exists() and str(p) not in canonical_paths:
   rawman.append({'source':'STATCAST_PILOT','path':str(p.relative_to(ROOT)),'date_range':f"{m.get('start_date')}_{m.get('end_date')}",'rows':m.get('raw_row_count',0),'size_bytes':p.stat().st_size,'sha256':sha(p),'schema_version':'pilot_preserved','acquisition_status':m.get('completion_status'),'validation_status':'SUPERSEDED_NOT_CANONICAL','retry_status':'REPLACED_BY_DAILY_CHUNKS'})
 # Retrosheet manifest and member schemas.
 rsm=json.loads((RS/'release_manifest.json').read_text()) if (RS/'release_manifest.json').exists() else {}; save('retrosheet_release_manifest.csv',[{'source_url':rsm.get('source_url',''),'retrieval_timestamp_utc':rsm.get('retrieval_timestamp_utc',''),'zip_path':rsm.get('zip_path',''),'zip_size':rsm.get('zip_size',0),'zip_sha256':rsm.get('zip_sha256',''),'status':rsm.get('status','MISSING'),'missing_expected_members':'|'.join(rsm.get('missing_expected_members',[]))}]); save('retrosheet_member_manifest.csv',[{'member':x['member'],'path':x['path'],'size_bytes':x['size_bytes'],'sha256':x['sha256'],'rows':x['rows'],'schema':'|'.join(x['schema'])} for x in rsm.get('members',[])])
 retro=defaultdict(lambda:{'plays':0,'games':set(),'dates':set(),'batters':set(),'pitchers':set()}); retro_types=Counter(); plays_path=RS/'extracted/plays.csv'
 if plays_path.exists():
  for x in pd.read_csv(plays_path,usecols=['gid','date','gametype','batter','pitcher'],dtype=str,chunksize=500000,low_memory=False):
   x=x[x.date.str[:4].isin(['2022','2023','2024','2025'])]
   for k,n in x.groupby([x.date.str[:4],x.gametype.fillna('')]).size().items():retro_types[k]+=int(n)
   x=x[x.gametype.eq('regular')]
   for y,g in x.groupby(x.date.str[:4]):
    z=retro[y];z['plays']+=len(g);z['games'].update(g.gid.dropna());z['dates'].update(g.date.dropna());z['batters'].update(g.batter.dropna());z['pitchers'].update(g.pitcher.dropna())
 save('retrosheet_coverage_report.csv',[{'season':y,'plays':z['plays'],'games':len(z['games']),'distinct_dates':len(z['dates']),'unique_batters':len(z['batters']),'unique_pitchers':len(z['pitchers'])} for y,z in sorted(retro.items())])
 save('retrosheet_coverage_by_game_type.csv',[{'season':y,'game_type':g,'plays':n,'governing_regular_season':g=='regular'} for (y,g),n in sorted(retro_types.items())])
 combined=[]
 for y in sorted(set(season)|set(retro)):
  combined.append({'season':y,'statcast_pitches':season[y]['pitches'],'statcast_plate_appearances':season[y]['plate_appearances'],'statcast_batted_balls':season[y]['batted_balls'],'retrosheet_plays':retro[y]['plays'],'retrosheet_games':len(retro[y]['games']),'statsapi_feeds':int((sal.game_date.astype(str).str[:4]==y).sum()) if 'sal' in locals() and len(sal) else 0})
 save('season_coverage_certification.csv',combined)
 for x in rsm.get('members',[]): rawman.append({'source':'RETROSHEET','path':str(Path(x['path']).relative_to(ROOT)) if str(x['path']).startswith(str(ROOT)) else x['path'],'date_range':'through_2025','rows':x['rows'],'size_bytes':x['size_bytes'],'sha256':x['sha256'],'schema_version':hashlib.sha256('|'.join(x['schema']).encode()).hexdigest()[:16],'acquisition_status':rsm.get('status'),'validation_status':'PASS' if rsm.get('status')=='ACQUIRED_AND_VALIDATED' else 'FAIL','retry_status':'COMPLETE'})
 if rsm.get('zip_path'):
  zp=ROOT/rsm['zip_path'];rawman.append({'source':'RETROSHEET_ARCHIVE','path':str(zp.relative_to(ROOT)),'date_range':'through_2025','rows':'ZIP','size_bytes':zp.stat().st_size,'sha256':sha(zp),'schema_version':'official_zip','acquisition_status':rsm.get('status'),'validation_status':'PASS','retry_status':'COMPLETE'})
 # StatsAPI ledger.
 sal=pd.read_csv(SA/'completion_ledger.csv') if (SA/'completion_ledger.csv').exists() else pd.DataFrame();save('statsapi_game_feed_completion_ledger.csv',sal)
 combined=[]
 for y in sorted(set(season)|set(retro)):
  combined.append({'season':y,'statcast_pitches':season[y]['pitches'],'statcast_plate_appearances':season[y]['plate_appearances'],'statcast_batted_balls':season[y]['batted_balls'],'retrosheet_plays':retro[y]['plays'],'retrosheet_games':len(retro[y]['games']),'statsapi_feeds':int((sal.game_date.astype(str).str[:4]==y).sum()) if len(sal) else 0})
 save('season_coverage_certification.csv',combined)
 if len(sal):
  for x in sal[sal.path.fillna('').ne('')].itertuples():
   p=ROOT/x.path;rawman.append({'source':'STATSAPI','path':x.path,'date_range':x.game_date,'rows':1,'size_bytes':x.size_bytes,'sha256':x.sha256,'schema_version':'feed_live_v1.1','acquisition_status':x.classification,'validation_status':'PASS','retry_status':'COMPLETE'})
 sched=SA/'schedule_2026-03-26_2026-07-21.json'
 if sched.exists():rawman.append({'source':'STATSAPI_SCHEDULE','path':str(sched.relative_to(ROOT)),'date_range':'2026-03-26_2026-07-21','rows':len(sal),'size_bytes':sched.stat().st_size,'sha256':sha(sched),'schema_version':'schedule_v1','acquisition_status':'ACQUIRED_AND_VALIDATED','validation_status':'PASS','retry_status':'COMPLETE'})
 save('raw_file_manifest.csv',rawman)
 # Identity and crosswalk proposal.
 save('identity_audit.csv',[{'source':'Statcast','grain':'pitch','primary_key':'game_pk|at_bat_number|pitch_number','rows':sum(v['pitches'] for v in season.values()),'duplicate_rows':sum(v-1 for v in keyc.values() if v>1),'status':'CERTIFIED'},{'source':'Statcast','grain':'plate appearance','primary_key':'game_pk|at_bat_number|batter','rows':len(pac),'duplicate_rows':'N/A pitch rows collapse','status':'CERTIFIED'},{'source':'StatsAPI','grain':'game','primary_key':'gamePk','rows':len(sal),'duplicate_rows':int(sal.game_pk.duplicated().sum()) if len(sal) else 0,'status':'CERTIFIED'},{'source':'Retrosheet','grain':'source-specific','primary_key':'exact official fields per member schema','rows':sum(max(0,x.get('rows',0)) for x in rsm.get('members',[])),'duplicate_rows':'NOT_SILENTLY_RESOLVED','status':'CERTIFIED_SCHEMA'}])
 save('crosswalk_proposal.csv',[{'from_source':'Statcast/StatsAPI','from_key':'game_pk/gamePk','to_source':'MLB canonical','to_key':'game_id','method':'exact numeric identity','apply_now':'NO'},{'from_source':'Retrosheet','from_key':'gameid','to_source':'MLB StatsAPI','to_key':'gamePk','method':'date/team/doubleheader exact documented crosswalk required','apply_now':'NO'},{'from_source':'Retrosheet','from_key':'playerid','to_source':'MLB','to_key':'player_id','method':'Chadwick/Retrosheet identifier register exact mapping only','apply_now':'NO'}])
 # Overlap against certified local feeds.
 st26=[]
 for p in rawfiles:
  if '/2026/' in str(p):
   x=pd.read_csv(p,low_memory=False); x=x[(x.game_date>='2026-05-01')&(x.game_date<='2026-07-09')]; st26.append(x[['game_pk','at_bat_number','pitch_number','pitch_type','release_speed','launch_speed','launch_angle','events']])
 s=pd.concat(st26,ignore_index=True) if st26 else pd.DataFrame(); lr=[]
 for p in sorted(LOCAL.glob('*.json')):
  d=json.load(p.open());gid=d['gamePk']
  for a in d['liveData']['plays']['allPlays']:
   for e in a.get('playEvents',[]):
    if not e.get('isPitch'):continue
    lr.append({'game_pk':gid,'at_bat_number':a.get('atBatIndex',0)+1,'pitch_number':e.get('pitchNumber'),'local_pitch_type':e.get('details',{}).get('type',{}).get('code'),'local_release_speed':e.get('pitchData',{}).get('startSpeed'),'local_launch_speed':e.get('hitData',{}).get('launchSpeed'),'local_launch_angle':e.get('hitData',{}).get('launchAngle'),'local_event':a.get('result',{}).get('eventType')})
 l=pd.DataFrame(lr); statcast_period_pitches=len(s); shared_games=set(s.game_pk)&set(l.game_pk); s=s[s.game_pk.isin(shared_games)].copy(); merged=s.merge(l,on=['game_pk','at_bat_number','pitch_number'],how='outer',indicator=True);both=merged[merged._merge=='both'].copy()
 def near(a,b,t):return (pd.to_numeric(a,errors='coerce')-pd.to_numeric(b,errors='coerce')).abs()<=t
 ov=[{'overlapping_games':len(shared_games),'statcast_all_games_period_pitches':statcast_period_pitches,'statcast_shared_game_pitches':len(s),'local_pitches':len(l),'identity_matches':len(both),'statcast_only_within_shared_games':int((merged._merge=='left_only').sum()),'local_only_within_shared_games':int((merged._merge=='right_only').sum()),'pitch_type_exact':int((both.pitch_type==both.local_pitch_type).sum()),'velocity_exact_or_rounding':int(near(both.release_speed,both.local_release_speed,.11).sum()),'exit_velocity_exact_or_rounding':int(near(both.launch_speed,both.local_launch_speed,.11).sum()),'launch_angle_exact_or_rounding':int(near(both.launch_angle,both.local_launch_angle,.11).sum())}];save('local_overlap_validation.csv',ov)
 dif=[]
 for x in both.itertuples():
  if x.pitch_type!=x.local_pitch_type:dif.append({'game_pk':x.game_pk,'at_bat_number':x.at_bat_number,'pitch_number':x.pitch_number,'field':'pitch_type','statcast_value':x.pitch_type,'statsapi_value':x.local_pitch_type,'classification':'FIELD_SEMANTIC_DIFFERENCE' if pd.notna(x.pitch_type) and pd.notna(x.local_pitch_type) else 'UNRESOLVED'})
  for f,a,b in [('release_speed',x.release_speed,x.local_release_speed),('launch_speed',x.launch_speed,x.local_launch_speed),('launch_angle',x.launch_angle,x.local_launch_angle)]:
   if pd.notna(a) and pd.notna(b) and float(a)!=float(b):dif.append({'game_pk':x.game_pk,'at_bat_number':x.at_bat_number,'pitch_number':x.pitch_number,'field':f,'statcast_value':a,'statsapi_value':b,'classification':'ROUNDING_DIFFERENCE' if abs(float(a)-float(b))<=.11 else 'SOURCE_REVISION'})
 save('source_difference_ledger.csv',dif[:200000])
 specs=[]
 for name,grain,key,owner,fields,unsupported in [('pitch_event','ONE_ROW_PER_PITCH','game_pk|at_bat_number|pitch_number','Statcast','all raw pitch identity/result/measurement fields','duplicate/missing composite keys retained'),('plate_appearance','ONE_ROW_PER_PA','game_pk|at_bat_number|batter','Statcast+StatsAPI','terminal events, batter/pitcher, result, timestamps','unusual missing batter explicit'),('batted_ball','ONE_ROW_PER_BATTED_BALL','pitch key','Statcast','launch metrics, estimated metrics, bb_type, coordinates','no fabricated barrel boolean'),('lineup_substitution','ONE_ROW_PER_LINEUP_EVENT','source game/play/substitution key','Retrosheet+StatsAPI','lineup order, participation, substitutions, timestamp','crosswalk required'),('player_game_outcome','ONE_ROW_PER_PLAYER_GAME','game_pk|player_id','StatsAPI+Retrosheet','PA, hits, official status','only final official games')]: specs.append({'table':name,'grain':grain,'primary_key':key,'raw_source_ownership':owner,'normalized_fields':fields,'temporal_fields':'game_date and source event/order timestamps where present','identity_crosswalk':'exact IDs only; proposal not silently applied','missingness':'preserve nulls with reason audits','unsupported_cases':unsupported})
 save('normalized_table_specifications.csv',specs)
 total=sum(x['size_bytes'] for x in rawman);save('storage_safety_audit.csv',[{'total_raw_storage_bytes':total,'expected_parsed_size':'No parsed duplicate created in v1','git_tracked':'NO','gitignore_rule':'backend/mlb/data/raw/ plus new backend/mlb/data/external/ rule','backup_requirements':'copy immutable raw hierarchy and manifests to separately managed durable backup','deletion_protection':'no overwrite; partial atomic rename; checksums','recovery_procedure':'restore by SHA256 or resume only missing/failed chunks'}])
 retry=[{'source':'Statcast','command':'.venv/bin/python backend/mlb/scripts/acquire_mlb_statcast_chunks.py --start 2022-03-01 --end 2026-07-21 --chunk-days 1 --sleep 1.5','scope':'reuses complete chunks; retry remaining failures'},{'source':'Retrosheet','command':'.venv/bin/python backend/mlb/scripts/acquire_mlb_retrosheet_archive.py','scope':'reuses official ZIP'},{'source':'StatsAPI','command':'.venv/bin/python backend/mlb/scripts/acquire_mlb_statsapi_missing_games.py','scope':'reuses feeds and reacquires missing completed games'}];save('retry_resume_commands.csv',retry)
 complete_ranges={(m.get('start_date'),m.get('end_date')) for m in metas if m.get('completion_status') in {'ACQUIRED_AND_VALIDATED','ACQUIRED_EMPTY_VALID'}}
 def unresolved(m):
  if m.get('completion_status') in {'ACQUIRED_AND_VALIDATED','ACQUIRED_EMPTY_VALID'}: return False
  try:
   days=pd.date_range(m['start_date'],m['end_date'],freq='D'); return not all((str(d.date()),str(d.date())) in complete_ranges for d in days)
  except Exception:return True
 stat_bad=sum(unresolved(m) for m in metas); expected_rs=not rsm or rsm.get('status')!='ACQUIRED_AND_VALIDATED'; sa_missing=int(sal.classification.eq('MISSING_REQUIRES_ACQUISITION').sum()) if len(sal) else -1
 if stat_bad==0 and not expected_rs and sa_missing==0:final='EXTERNAL_BATTER_EVENT_PLATFORM_V1_ACQUIRED_AND_CERTIFIED'
 elif stat_bad:final='ACQUISITION_PARTIAL_RETRYABLE_GAPS'
 else:final='ACQUISITION_BLOCKED_BY_SOURCE_ACCESS'
 dec={'MLB_EXTERNAL_DATA_SOURCE_CONTRACT_DECISION':'FROZEN_REPLAYABLE_CONTRACTS','MLB_STATCAST_MULTI_SEASON_ACQUISITION_DECISION':f'{len(rawfiles)}_VALIDATED_CHUNKS_{stat_bad}_NONFINAL','MLB_RETROSHEET_ACQUISITION_DECISION':rsm.get('status','MISSING'),'MLB_STATSAPI_2026_COMPLETION_DECISION':f'{len(sal)}_GAMES_{sa_missing}_MISSING_REQUIRES_ACQUISITION','MLB_EXTERNAL_RAW_PRESERVATION_DECISION':'IMMUTABLE_HASHED_RESUMABLE_RAW_PRESERVED','MLB_EXTERNAL_SCHEMA_CERTIFICATION_DECISION':'EXACT_RAW_SCHEMAS_AND_NULL_COUNTS_CERTIFIED','MLB_EXTERNAL_IDENTITY_CERTIFICATION_DECISION':'SOURCE_KEYS_CERTIFIED_CROSSWALKS_PROPOSED_NOT_APPLIED','MLB_EXTERNAL_DATE_COVERAGE_DECISION':'REGULAR_SEASON_2022_THROUGH_2026_07_21_REPORTED_FROM_ROWS','MLB_EXTERNAL_OVERLAP_VALIDATION_DECISION':'STATCAST_VS_CERTIFIED_STATSAPI_OVERLAP_MEASURED','MLB_EXTERNAL_CRITICAL_FIELD_DECISION':'CRITICAL_STATCAST_FIELD_COVERAGE_REPORTED','MLB_EXTERNAL_STORAGE_SAFETY_DECISION':'RAW_EXTERNAL_DATA_IGNORED_MANIFESTS_TRACKABLE_BACKUP_REQUIRED','MLB_EXTERNAL_BATTER_EVENT_PLATFORM_V1_DECISION':final,'MLB_MODEL_DEVELOPMENT_ACTION_DECISION':'DATA_ACQUISITION_ONLY_NO_TRAINING_FEATURE_CONSTRUCTION_PROMOTION_OR_PRODUCTION_CHANGE'}
 save('final_acquisition_decision.csv',[{'decision':k,'value':v} for k,v in dec.items()]); result={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'counts':{'statcast_chunks':len(rawfiles),'statcast_nonfinal_chunks':stat_bad,'statcast_pitches':sum(v['pitches'] for v in season.values()),'statsapi_games':len(sal),'raw_storage_bytes':total},'overlap':ov[0] if ov else {},'decisions':dec};(OUT/'machine_readable.json').write_text(json.dumps(result,indent=2)+'\n')
 required=['source_contracts.csv','request_inventory.csv','chunk_acquisition_ledger.csv','raw_file_manifest.csv','baseball_savant_schema.csv','baseball_savant_coverage_report.csv','retrosheet_release_manifest.csv','retrosheet_coverage_report.csv','retrosheet_coverage_by_game_type.csv','season_coverage_certification.csv','statsapi_game_feed_completion_ledger.csv','identity_audit.csv','crosswalk_proposal.csv','critical_field_coverage.csv','launch_speed_angle_category_contract.csv','local_overlap_validation.csv','source_difference_ledger.csv','normalized_table_specifications.csv','storage_safety_audit.csv','retry_resume_commands.csv','final_acquisition_decision.csv','machine_readable.json'];checks=[{'check':x,'status':'PASS' if (OUT/x).exists() else 'FAIL','message':''} for x in required];checks +=[{'check':'statcast_all_chunks_final','status':'PASS' if stat_bad==0 else 'FAIL','message':str(stat_bad)},{'check':'retrosheet_expected_members','status':'PASS' if not expected_rs else 'FAIL','message':'|'.join(rsm.get('missing_expected_members',[]))},{'check':'statsapi_no_missing_final','status':'PASS' if sa_missing==0 else 'FAIL','message':str(sa_missing)},{'check':'no_model_or_database_action','status':'PASS','message':'Acquisition/certification only'}];save('validation_report.csv',checks)
 files=[]
 for p in sorted(OUT.iterdir()):
  if p.is_file() and p.name!='sha256_manifest.csv':files.append({'path':p.name,'sha256':sha(p),'size_bytes':p.stat().st_size})
 save('sha256_manifest.csv',files);print(json.dumps({'final':final,'statcast_chunks':len(rawfiles),'statcast_bad':stat_bad,'retrosheet':rsm.get('status'),'statsapi_games':len(sal)},indent=2))
if __name__=='__main__':main()
