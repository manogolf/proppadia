#!/usr/bin/env python3
"""Daily retained-artifact denominator audit for historical MLB Hits 0.5."""
from __future__ import annotations
import hashlib,json,re
from collections import defaultdict
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'artifacts/analysis/model_development/mlb_hits05_historical_daily_denominator_audit_v1/2026-08-14'
RECOVERED=ROOT/'artifacts/analysis/model_development/mlb_hits_historical_identity_recovery_v1/2026-08-14/hits_recovered_synchronized_population.csv'
ORIGINAL=ROOT/'artifacts/analysis/model_development/mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14/frozen_hits_review_population.csv'
START,END='2026-03-25','2026-08-14'

def write(name,rows): pd.DataFrame(rows).to_csv(OUT/name,index=False,lineterminator='\n')
def ratio(a,b): return a/b if b else None
def norm(v): return ' '.join(re.sub(r'[^a-z0-9 ]',' ',str(v or '').lower()).split())

def load_model():
 rows=[]; artifact=[]
 for daydir in sorted((ROOT/'backend/mlb/exports/odds_history').glob('2026-??-??')):
  day=daydir.name
  if not START<=day<=END:continue
  files=sorted(daydir.glob('mlb_slate_output__*.csv'))
  if not files:
   p=daydir/'mlb_slate_output.csv'; files=[p] if p.exists() else []
  for f in files:
   try:d=pd.read_csv(f,low_memory=False)
   except Exception:continue
   if 'prop_type' not in d:continue
   d=d[d.prop_type.astype(str).eq('hits')].copy()
   if d.empty:continue
   d['date']=day; d['artifact']=str(f.relative_to(ROOT)); d['line_num']=pd.to_numeric(d.line,errors='coerce'); d['side']=d.model_pick_side.astype(str).str.lower()
   d['game_id']=d.game_id.astype(str).str.replace(r'\.0$','',regex=True); d['player_id']=d.player_id.astype(str).str.replace(r'\.0$','',regex=True)
   rows.append(d)
   artifact.append({'date':day,'artifact':str(f.relative_to(ROOT)),'rows':len(d),'hits05_rows':int(d.line_num.eq(.5).sum()),
    'has_game_time':'game_time' in d,'has_model_name':any(c in d for c in ['model_name','model_id']),'has_version':any(c in d for c in ['model_version','semantic_model_id']),
    'has_hash':any('hash' in c.lower() for c in d.columns),'has_prediction_source':'prediction_source_file' in d,'has_run_tag':'__' in f.name,
    'prediction_source_values':'|'.join(sorted(d.prediction_source_file.dropna().astype(str).unique())) if 'prediction_source_file' in d else ''})
 return pd.concat(rows,ignore_index=True,sort=False),pd.DataFrame(artifact)

def load_raw():
 obs=[]; event_inventory=[]
 for daydir in sorted((ROOT/'backend/mlb/exports/odds_history').glob('2026-??-??')):
  day=daydir.name
  if not START<=day<=END:continue
  files=sorted(daydir.glob('odds_mlb_playerprops__*.json'))
  if not files:
   p=daydir/'odds_mlb_playerprops.json'; files=[p] if p.exists() else []
  for f in files:
   try:p=json.loads(f.read_text())
   except Exception:continue
   events=p.get('events',[]) if isinstance(p,dict) else p if isinstance(p,list) else []
   for e in events:
    if not isinstance(e,dict):continue
    eid=str(e.get('id') or ''); has_bol=False; has_any_hits=False; has_05=False
    start=pd.to_datetime(e.get('commence_time'),utc=True,errors='coerce')
    event_day=start.tz_convert('America/New_York').date().isoformat() if pd.notna(start) else day
    for b in e.get('bookmakers',[]):
     if not isinstance(b,dict) or b.get('key')!='betonlineag':continue
     has_bol=True
     for m in b.get('markets',[]):
      if not isinstance(m,dict) or m.get('key')!='batter_hits':continue
      has_any_hits=True
      for o in m.get('outcomes',[]):
       if not isinstance(o,dict):continue
       line=pd.to_numeric(o.get('point'),errors='coerce'); side=str(o.get('name') or '').lower(); player=str(o.get('description') or '')
       if line==.5:has_05=True
       obs.append({'date':event_day,'capture_date':day,'event_id':eid,'commence_time':e.get('commence_time'),'player_name':player,'player_key':norm(player),'line':line,'side':side,'capture_file':str(f.relative_to(ROOT))})
    event_inventory.append({'date':event_day,'capture_date':day,'event_id':eid,'has_betonline':has_bol,'has_hits':has_any_hits,'has_hits05':has_05})
 raw=pd.DataFrame(obs).drop_duplicates(['date','event_id','player_key','line','side'])
 events=pd.DataFrame(event_inventory).groupby(['date','event_id'],as_index=False).max(numeric_only=False)
 return raw,events

def load_reconcile():
 rows=[]; schema=[]
 for p in sorted((ROOT/'artifacts/analysis/mlb/execution_vs_model').glob('2026-??-??/reconcile_rows.csv')):
  day=p.parent.name
  if not START<=day<=END:continue
  d=pd.read_csv(p,low_memory=False); d['date']=day; rows.append(d)
  schema.append({'date':day,'has_reconcile':True,'has_game_time':'game_time' in d,'has_snapshot_time':'snapshot_time_utc' in d,'has_outcome':'actual_value' in d,'columns':len(d.columns)})
 return pd.concat(rows,ignore_index=True,sort=False),pd.DataFrame(schema)

def main():
 OUT.mkdir(parents=True,exist_ok=True); model,artifacts=load_model(); raw,events=load_raw(); rec,schemas=load_reconcile()
 dates=pd.DataFrame({'date':pd.date_range(START,END).strftime('%Y-%m-%d')})
 h05=model[model.line_num.eq(.5)&model.side.isin(['over','under'])].copy(); raw05=raw[raw.line.eq(.5)&raw.side.isin(['over','under'])].copy()
 # Distinct rows across snapshots; a side flip remains a distinct historical selected-side prediction.
 h05u=h05.sort_values('artifact').drop_duplicates(['date','game_id','player_id','line_num','side'])
 opportunities=model.drop_duplicates(['date','game_id','player_id'])
 gamebase=model.drop_duplicates(['date','game_id'])
 rawpg=raw05.drop_duplicates(['date','event_id','player_key'])
 sync=pd.read_csv(RECOVERED,dtype={'game_id':str,'player_id':str}); sync=sync[sync.line.eq(.5)].copy()
 orig=pd.read_csv(ORIGINAL,dtype={'game_id':str,'player_id':str}); orig=orig[orig.line.eq(.5)].copy()
 syncpg=sync.drop_duplicates(['date','game_id','player_id']); origpg=orig.drop_duplicates(['game_date','game_id','player_id'])
 official={}
 for p in (ROOT/'backend/mlb/data/external/statsapi/raw/2026').glob('schedule_*.json'):
  try:j=json.loads(p.read_text())
  except Exception:continue
  for dd in j.get('dates',[]):official[str(dd.get('date'))]=len(dd.get('games',[]))
 game_rows=[]; player_rows=[]; side_rows=[]; pred_rows=[]; syn_rows=[]
 for day in dates.date:
  mg=gamebase[gamebase.date.eq(day)]; ev=events[events.date.eq(day)]; bo=ev[ev.has_betonline]; rh=ev[ev.has_hits]; r05e=ev[ev.has_hits05]; mp=h05u[h05u.date.eq(day)]
  sg=sync[sync.date.eq(day)]; opp=opportunities[opportunities.date.eq(day)]; rp=rawpg[rawpg.date.eq(day)]; s_p=syncpg[syncpg.date.eq(day)]
  confirmed=0
  if 'hits05_lineup_status' in mp: confirmed=len(mp[mp.hits05_lineup_status.eq('CONFIRMED_PREGAME_STARTER')].drop_duplicates(['game_id','player_id']))
  game_rows.append({'date':day,'official_local_schedule_games':official.get(day,''),'official_schedule_status':'LOCAL_OFFICIAL_SNAPSHOT_AVAILABLE' if day in official else 'LOCAL_OFFICIAL_SCHEDULE_UNAVAILABLE',
   'repository_denominator_games':mg.game_id.nunique(),'repository_denominator_status':'RETAINED_SLATE_ARTIFACT_ONLY_INCOMPLETE_OFFICIAL_DENOMINATOR','games_with_betonline_player_prop_capture':bo.event_id.nunique(),
   'games_with_any_hits_market':rh.event_id.nunique(),'games_with_hits05_market':r05e.event_id.nunique(),'games_with_proppadia_hits05':mp.game_id.nunique(),'games_in_recovered_sync_hits05':sg.game_id.nunique()})
  player_rows.append({'date':day,'hitter_game_opportunities':len(opp),'starting_hitter_game_opportunities':confirmed if confirmed else '',
   'starting_denominator_status':'EXACT_CONFIRMED_PREGAME_STARTER_METADATA' if confirmed else 'UNAVAILABLE_NOT_APPROXIMATED','betonline_any_hits_player_games':len(raw[raw.date.eq(day)].drop_duplicates(['event_id','player_key'])),
   'betonline_hits05_player_games':len(rp),'proppadia_hits05_player_games':len(mp.drop_duplicates(['game_id','player_id'])),'synchronized_hits05_player_games':len(s_p),
   'betonline_hits05_proposition_sides':len(raw05[raw05.date.eq(day)]),'proppadia_hits05_prediction_sides':len(mp),'synchronized_hits05_rows':len(sg)})
  rb=raw05[raw05.date.eq(day)]; ppg=mp.groupby(['game_id','player_id']).side.nunique() if len(mp) else pd.Series(dtype=int); spg=sg.groupby(['game_id','player_id']).side.nunique() if len(sg) else pd.Series(dtype=int)
  side_rows.append({'date':day,'betonline_o05':int(rb.side.eq('over').sum()),'betonline_u05':int(rb.side.eq('under').sum()),'betonline_paired_player_games':int((rb.groupby(['event_id','player_key']).side.nunique()>=2).sum()) if len(rb) else 0,
   'proppadia_o05':int(mp.side.eq('over').sum()),'proppadia_u05':int(mp.side.eq('under').sum()),'proppadia_both_side_player_games':int((ppg>=2).sum()),'proppadia_one_side_player_games':int((ppg==1).sum()),
   'synchronized_o05':int(sg.side.eq('over').sum()),'synchronized_u05':int(sg.side.eq('under').sum()),'synchronized_paired_player_games':int((spg>=2).sum())})
  bpg=len(rp); mpg=len(mp.drop_duplicates(['game_id','player_id'])); both=int((ppg>=2).sum()); either=mpg
  pred_rows.append({'date':day,'games':mg.game_id.nunique(),'betonline_hits05_player_games':bpg,'hitter_game_opportunities':len(opp),'raw_betonline_o05':int(rb.side.eq('over').sum()),'raw_betonline_u05':int(rb.side.eq('under').sum()),'proppadia_hits05_player_games':mpg,'proppadia_o05_predictions':int(mp.side.eq('over').sum()),'proppadia_u05_predictions':int(mp.side.eq('under').sum()),
   'proppadia_either_side_player_games':either,'proppadia_both_side_player_games':both,'model_pg_per_betonline_pg':ratio(mpg,bpg),'model_pg_per_hitter_opportunity':ratio(mpg,len(opp)),'both_side_pg_per_betonline_pg':ratio(both,bpg),
   'synchronized_o05':int(sg.side.eq('over').sum()),'synchronized_u05':int(sg.side.eq('under').sum()),'total_synchronized_05':len(sg),'synchronization_rate':ratio(len(sg),len(mp)),
   'daily_class':'ZERO_PREDICTION' if not len(mp) else 'HIGH_COVERAGE' if bpg and mpg/bpg>=.8 else 'PARTIAL','regime_boundary':day in {'2026-04-09','2026-05-08','2026-07-21','2026-08-03','2026-08-04'}})
  op=orig[orig.game_date.eq(day)]; opg=origpg[origpg.game_date.eq(day)]
  syn_rows.append({'date':day,'model_hits05_player_games':mpg,'model_hits05_prediction_rows':len(mp),'original_synchronized_player_games':len(opg),'original_synchronized_rows':len(op),
   'recovered_synchronized_player_games':len(s_p),'recovered_synchronized_rows':len(sg),'original_pg_per_model_pg':ratio(len(opg),mpg),'recovered_pg_per_model_pg':ratio(len(s_p),mpg),
   'original_rows_per_model_rows':ratio(len(op),len(mp)),'recovered_rows_per_model_rows':ratio(len(sg),len(mp))})
 write('hits05_daily_game_denominator.csv',game_rows); write('hits05_daily_player_game_denominator.csv',player_rows); write('hits05_daily_side_counts.csv',side_rows); write('hits05_daily_prediction_coverage.csv',pred_rows); write('hits05_daily_synchronization_coverage.csv',syn_rows)

 # Producer and regime inventory is based only on retained schemas and source code contracts.
 contracts=[
  {'producer':'build_mlb_slate_output.py','active_dates':'2026-03-25 through 2026-08-03 retained','row_generation_contract':'one model_pick_side per source player/prop/line row; probabilities for both directions retained','board_class':'SELECTED_MODEL_DIRECTION_NOT_GOVERNED_BOTH_SIDE_BOARD','semantic_change':'schema enrichment over time; duplicates/side flips across snapshots can yield both historical sides'},
  {'producer':'daily local capture archive','active_dates':'2026-04-30 through 2026-08-03 tagged runs','row_generation_contract':'multiple immutable intraday slate snapshots plus aliases','board_class':'REPEATED_SELECTED_SIDE_SNAPSHOTS','semantic_change':'capture cadence expanded; not a new all-board contract'},
  {'producer':'build_mlb_reconcile_rows.py','active_dates':'2026-04-09 through 2026-08-02 retained','row_generation_contract':'exact selected model row joined to provider line/prices/outcome','board_class':'SELECTED_SUBSET_RECONCILIATION','semantic_change':'game_time added 2026-05-08; BetOnline availability intermittent'},
  {'producer':'current governed Hits05 parent/lifecycle','active_dates':'after legacy slate archive','row_generation_contract':'separate governed prediction lifecycle','board_class':'NOT_MERGED_INTO_HISTORICAL_BENCHMARK','semantic_change':'prospective authority/provenance contract differs'},]
 write('hits05_historical_producer_contracts.csv',contracts)
 regimes=[
  {'start_date':'2026-03-25','end_date':'2026-04-08','producer_identity':'build_mlb_slate_output.py; semantic model unresolved','output_contract':'selected model direction','betonline_coverage':'raw retained; no reconcile','synchronized_coverage':'none strictly auditable','regime':'LEGACY_PREDICTION_AND_RAW_ONLY'},
  {'start_date':'2026-04-09','end_date':'2026-05-07','producer_identity':'build_mlb_slate_output.py + reconcile; semantic model unresolved','output_contract':'selected direction; paired prices/outcomes possible','betonline_coverage':'reconcile available','synchronized_coverage':'blocked by absent game_time strict timing field','regime':'RECONCILE_WITHOUT_STRICT_START_TIME'},
  {'start_date':'2026-05-08','end_date':'2026-07-20','producer_identity':'build_mlb_slate_output.py + strict reconcile; semantic model unresolved','output_contract':'selected direction across intraday snapshots','betonline_coverage':'variable, including provider gaps','synchronized_coverage':'recovered benchmark eligible','regime':'STRICT_TIMED_HISTORICAL_BENCHMARK'},
  {'start_date':'2026-07-21','end_date':'2026-08-02','producer_identity':'enriched slate schema + strict reconcile; semantic model unresolved','output_contract':'selected direction with lineup/starter metadata where populated','betonline_coverage':'retained','synchronized_coverage':'recovered benchmark eligible','regime':'ENRICHED_STRICT_TIMED_BENCHMARK'},
  {'start_date':'2026-08-03','end_date':'2026-08-03','producer_identity':'legacy slate artifact retained; semantic model unresolved','output_contract':'selected-direction slate artifacts','betonline_coverage':'raw retained','synchronized_coverage':'no retained reconcile/outcome package','regime':'POST_BENCHMARK_LEGACY_ARTIFACT'},
  {'start_date':'2026-08-04','end_date':'2026-08-14','producer_identity':'regular raw collection; legacy producer absent','output_contract':'market capture only for this denominator','betonline_coverage':'raw retained','synchronized_coverage':'not historical-benchmark eligible','regime':'RAW_COLLECTION_PROSPECTIVE_ONLY'}]
 # Add descriptive typical counts.
 daily=pd.DataFrame(side_rows).merge(pd.DataFrame(player_rows),on='date')
 for x in regimes:
  g=daily[daily.date.between(x['start_date'],x['end_date'])]; x['median_daily_model_hits05_rows']=g.proppadia_hits05_prediction_sides.median() if len(g) else 0; x['median_daily_betonline_hits05_player_games']=g.betonline_hits05_player_games.median() if len(g) else 0; x['median_daily_synchronized_rows']=g.synchronized_hits05_rows.median() if len(g) else 0
 write('hits05_historical_regimes.csv',regimes)

 provenance=[]
 for day in dates.date:
  a=artifacts[artifacts.date.eq(day)]
  if a.empty:status='UNRECOVERABLE'; reason='no retained legacy prediction artifact'
  elif a.has_model_name.any() and a.has_version.any() and a.has_hash.any():status='RECOVERABLE'; reason='exact identity metadata present'
  else:status='PARTIALLY_RECOVERABLE'; reason='producer path/source/run tag retained; semantic model ID/version/hash absent'
  provenance.append({'date':day,'prediction_artifacts':len(a),'model_name_present':bool(a.has_model_name.any()) if len(a) else False,'version_present':bool(a.has_version.any()) if len(a) else False,'hash_present':bool(a.has_hash.any()) if len(a) else False,
   'producer_path_recoverable':bool(a.has_prediction_source.any()) if len(a) else False,'run_tag_recoverable':bool(a.has_run_tag.any()) if len(a) else False,'identity_status':status,'reason':reason})
 write('hits05_model_provenance_by_date.csv',provenance)

 market=[]
 for r in player_rows:
  market.append({'date':r['date'],'month':r['date'][:7],'hitter_game_opportunities':r['hitter_game_opportunities'],'starting_hitter_game_opportunities':r['starting_hitter_game_opportunities'],'betonline_hits05_player_games':r['betonline_hits05_player_games'],
   'coverage_vs_hitter_games':ratio(r['betonline_hits05_player_games'],r['hitter_game_opportunities']),'coverage_vs_starting_hitters':ratio(r['betonline_hits05_player_games'],r['starting_hitter_game_opportunities']) if r['starting_hitter_game_opportunities']!='' else None})
 mdf=pd.DataFrame(market); monthly=[]
 for month,g in mdf.groupby('month'):
  x=g.coverage_vs_hitter_games.dropna(); monthly.append({'date':f'{month}-MONTHLY_SUMMARY','month':month,'hitter_game_opportunities':g.hitter_game_opportunities.sum(),'betonline_hits05_player_games':g.betonline_hits05_player_games.sum(),'coverage_vs_hitter_games':x.mean() if len(x) else None,'monthly_daily_median':x.median() if len(x) else None,'monthly_daily_min':x.min() if len(x) else None,'monthly_daily_max':x.max() if len(x) else None})
 write('hits05_betonline_market_coverage.csv',market+monthly)
 strong=pd.DataFrame(pred_rows).merge(pd.DataFrame(game_rows)[['date','repository_denominator_games']],on='date'); strong=strong[(strong.repository_denominator_games>=10)&strong.model_pg_per_betonline_pg.fillna(0).ge(.8)]
 scale=[{'population':'STRONG_RETAINED_COVERAGE_DAYS','days':len(strong),'selection_rule':'repository games >=10 and model player-game coverage >=80% of retained BetOnline player-games',
  'median_games_per_day':strong.repository_denominator_games.median(),'median_hitter_game_opportunities':pd.DataFrame(player_rows).set_index('date').loc[strong.date].hitter_game_opportunities.median() if len(strong) else None,
  'median_betonline_hits05_player_games':strong.betonline_hits05_player_games.median(),'median_proppadia_hits05_prediction_rows':(strong.proppadia_o05_predictions+strong.proppadia_u05_predictions).median(),'median_synchronized_rows':pd.DataFrame(syn_rows).set_index('date').loc[strong.date].recovered_synchronized_rows.median() if len(strong) else None}]
 write('hits05_expected_daily_scale.csv',scale)

 outcomes=[]
 if 'actual_value' in rec:
  rr=rec[(rec.prop_type.astype(str).eq('hits'))&pd.to_numeric(rec.actual_value,errors='coerce').notna()].copy(); rr['actual']=pd.to_numeric(rr.actual_value); rr['game_id']=rr.game_id.astype(str); rr['player_id']=rr.player_id.astype(str); rr=rr.drop_duplicates(['date','game_id','player_id'])
  for day,g in rr.groupby('date'):outcomes.append({'date':day,'outcome_denominator':'retained reconciled hitter-games only; not the full official batting universe','hitter_games':len(g),'total_hits_recorded':g.actual.sum(),'unique_hitters_with_ge1_hit':g[g.actual>=1].player_id.nunique(),'hitter_games_0_hits':int(g.actual.eq(0).sum()),'hitter_games_1_hit':int(g.actual.eq(1).sum()),'hitter_games_2plus_hits':int(g.actual.ge(2).sum())})
 write('hits05_daily_outcome_context.csv',outcomes)

 may8="""# May 8 boundary analysis\n\nMay 8 is not the first retained Hits 0.5 market or prediction date. Raw/provider and model-selected Hits 0.5 artifacts exist from March 25; provider-specific reconcile rows with outcomes exist from April 9. The April 9–May 7 reconcile schema has 72 columns and lacks `game_time`. May 8 is the first 80-column reconcile schema containing both `snapshot_time_utc` and `game_time`, which makes the benchmark's strict `snapshot < scheduled start` pregame rule auditable. The boundary is therefore the first strict temporal-contract-compatible reconcile artifact, not model or market launch.\n"""
 (OUT/'hits05_may8_boundary_analysis.md').write_text(may8)
 (OUT/'hits05_pre_may8_recoverability.md').write_text("# Pre-May 8 recoverability\n\n`PRE_MAY8_RECOVERY = PARTIALLY_RECOVERABLE`\n\nRetained raw BetOnline Hits 0.5 markets and Proppadia selected-side predictions begin March 25. Provider-specific reconciles and outcomes begin April 9, with exact line/side identity, but scheduled start is absent through May 7, so the unchanged strict pregame timing rule cannot be proven from those reconcile rows. Semantic model version/hash is also absent. No reconstruction was performed.\n")
 (OUT/'hits05_post_aug2_gap_analysis.md').write_text("# Post-August 2 gap analysis\n\n`POST_AUG2_RECOVERY = PROSPECTIVE_ONLY`\n\nAugust 2 is the last retained `execution_vs_model` reconcile package. August 3 retains legacy slate predictions and raw BetOnline prices but no reconcile/outcome package. August 4–14 retains regular raw market capture but no legacy slate output in this archive. These dates are not merged into the benchmark; their prediction authority/provenance and outcome synchronization are governed by later prospective lifecycles.\n")
 side_daily=pd.DataFrame(side_rows); model_days=side_daily[(side_daily.proppadia_o05+side_daily.proppadia_u05)>0]; benchmark_days=side_daily[side_daily.date.between('2026-05-08','2026-08-02')]
 totals={'calendar_dates':len(dates),'dates_with_model':h05u.date.nunique(),'dates_with_raw_betonline_hits05':raw05.date.nunique(),'dates_with_synchronized_hits05':sync.date.nunique(),'official_local_schedule_dates':len(official),
  'games':h05u.game_id.nunique(),'synchronized_games':sync.game_id.nunique(),'hitter_games':len(opportunities),'betonline_player_games':len(rawpg),'model_player_games':len(h05u.drop_duplicates(['date','game_id','player_id'])),'model_o':int(h05u.side.eq('over').sum()),'model_u':int(h05u.side.eq('under').sum()),
  'median_daily_model_prediction_rows':float((model_days.proppadia_o05+model_days.proppadia_u05).median()),'median_daily_synchronized_rows_benchmark_calendar':float((benchmark_days.synchronized_o05+benchmark_days.synchronized_u05).median()),
  'sync_rows':len(sync),'sync_h05_rows':len(sync),'original_sync_h05':len(orig)}
 decision='HITS05_HISTORY_IS_SELECTED_SUBSET_NOT_FULL_BOARD'; pre='PARTIALLY_RECOVERABLE'; post='PROSPECTIVE_ONLY'
 md=f"""# MLB Hits 0.5 historical daily denominator audit v1\n\n- Legitimate retained Hits 0.5 range: {START} through {END}; legacy model artifacts end 2026-08-03 and recovered benchmark ends 2026-08-02.\n- May 8 is the first reconcile schema with scheduled start, enabling strict pregame validation.\n- Retained model dates: {totals['dates_with_model']}; games: {totals['games']}; hitter-game opportunities: {totals['hitter_games']:,}.\n- BetOnline Hits 0.5 player-games: {totals['betonline_player_games']:,}; model-selected player-games: {totals['model_player_games']:,}.\n- Model selected-side rows: Over {totals['model_o']:,}; Under {totals['model_u']:,}. The producer emitted a preferred direction per source row, not a governed two-sided all-board population; side flips and repeated snapshots can expose both historical sides.\n- Original synchronized Hits 0.5 rows: {len(orig):,}; recovered: {len(sync):,}.\n- `PRE_MAY8_RECOVERY = {pre}`; `POST_AUG2_RECOVERY = {post}`; `HISTORICAL_MODEL_PROVENANCE = UNRESOLVED`.\n- Decision: `{decision}`.\n"""
 (OUT/'concise_mlb_hits05_historical_daily_denominator_audit_v1.md').write_text(md)
 summary={'task_id':'MLB_HITS05_HISTORICAL_DAILY_DENOMINATOR_AUDIT_V1','earliest_legitimate_date':START,'latest_raw_date':END,'latest_legacy_prediction_date':h05u.date.max(),'latest_benchmark_date':sync.date.max(),**totals,'pre_may8':pre,'post_aug2':post,'historical_model_provenance':'UNRESOLVED','decision':decision}
 (OUT/'audit_summary.json').write_text(json.dumps(summary,indent=2)+'\n')
 products=sorted(p for p in OUT.iterdir() if p.name!='reproducibility_hashes.sha256'); (OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n' for p in products)); print(json.dumps(summary,indent=2)); return 0
if __name__=='__main__':raise SystemExit(main())
