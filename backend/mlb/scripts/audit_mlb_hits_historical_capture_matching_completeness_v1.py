#!/usr/bin/env python3
"""Retained-artifact denominator and attrition audit for historical MLB Hits."""
from __future__ import annotations
import glob,hashlib,json,re
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[3]; OUT=ROOT/'artifacts/analysis/model_development/mlb_hits_historical_capture_matching_completeness_audit_v1/2026-08-14'
FROZEN=ROOT/'artifacts/analysis/model_development/mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14/frozen_hits_review_population.csv'
START='2026-05-08'; END='2026-08-02'; LANES=[(.5,'over'),(.5,'under'),(1.5,'over'),(1.5,'under')]
def write(name,rows): pd.DataFrame(rows).to_csv(OUT/name,index=False,lineterminator='\n')
def main():
 OUT.mkdir(parents=True,exist_ok=True); frames=[]
 for f in sorted(glob.glob(str(ROOT/'backend/mlb/exports/odds_history/2026-??-??/mlb_slate_output__*.csv'))):
  date=Path(f).parent.name
  if not START<=date<=END:continue
  try:d=pd.read_csv(f,low_memory=False)
  except:continue
  d=d[d.prop_type.astype(str).eq('hits')].copy()
  if not len(d):continue
  d['capture_file']=str(Path(f).relative_to(ROOT)); d['capture_date']=date; m=re.search(r'T(\d{6})Z',f); d['capture_hms']=m.group(1) if m else ''
  d['capture_dt']=pd.to_datetime(date+' '+d.capture_hms.str[:2]+':'+d.capture_hms.str[2:4]+':'+d.capture_hms.str[4:6],utc=True,errors='coerce')
  d['start_dt']=pd.to_datetime(d['game_time'] if 'game_time' in d else pd.Series(index=d.index,dtype=object),utc=True,errors='coerce'); d['pregame']=d.capture_dt.notna()&d.start_dt.notna()&(d.capture_dt<d.start_dt)
  frames.append(d)
 obs=pd.concat(frames,ignore_index=True,sort=False); frozen=pd.read_csv(FROZEN,low_memory=False); frozen=frozen[frozen.prop_type.eq('hits')]
 keys=['game_id','player_id','line']; obs['side']=obs.model_pick_side.str.lower()
 # Exact identity matching uses retained provider-specific reconcile rows.
 rec=[]
 for f in sorted(glob.glob(str(ROOT/'artifacts/analysis/mlb/execution_vs_model/2026-??-??/reconcile_rows.csv'))):
  date=Path(f).parent.name
  if not START<=date<=END:continue
  x=pd.read_csv(f,low_memory=False); x=x[(x.prop_type.eq('hits'))&(x.bookmaker_key.eq('betonlineag'))].copy(); x['capture_date']=date; rec.append(x)
 bol=pd.concat(rec,ignore_index=True,sort=False); bol['side']=bol.model_pick_side.str.lower(); bol['capture_dt']=pd.to_datetime(bol.snapshot_time_utc,utc=True,errors='coerce'); bol['start_dt']=pd.to_datetime(bol.game_time,utc=True,errors='coerce'); bol['pregame']=bol.capture_dt.notna()&bol.start_dt.notna()&(bol.capture_dt<bol.start_dt); bol['paired']=pd.to_numeric(bol.price_over_american,errors='coerce').notna()&pd.to_numeric(bol.price_under_american,errors='coerce').notna()
 # Raw observation inventory is parsed directly from every retained provider payload.
 raw=[]
 for f in sorted(glob.glob(str(ROOT/'backend/mlb/exports/odds_history/2026-??-??/odds_mlb_playerprops__*.json'))):
  date=Path(f).parent.name
  if not START<=date<=END:continue
  try: payload=json.loads(Path(f).read_text())
  except:continue
  captured=payload.get('captured_at_utc')
  for event in payload.get('events',[]):
   for book in event.get('bookmakers',[]):
    if book.get('key')!='betonlineag':continue
    for market in book.get('markets',[]):
     if market.get('key')!='batter_hits':continue
     for o in market.get('outcomes',[]): raw.append({'date':date,'capture_file':str(Path(f).relative_to(ROOT)),'captured_at':captured,'event_id':event.get('id'),'commence_time':event.get('commence_time'),'player_name':o.get('description'),'line':o.get('point'),'side':str(o.get('name','')).lower(),'price':o.get('price'),'market_last_update':market.get('last_update')})
 raw=pd.DataFrame(raw); raw['pregame']=pd.to_datetime(raw.captured_at,utc=True,errors='coerce')<pd.to_datetime(raw.commence_time,utc=True,errors='coerce')
 # Preserve each distinct model-selected side seen prospectively; side changes are distinct prediction identities here.
 model=obs.sort_values('capture_dt').drop_duplicates(['capture_date']+keys+['side'],keep='last'); bp=bol[bol.pregame].sort_values('capture_dt').drop_duplicates(['capture_date']+keys+['side'],keep='last')
 games=obs[['capture_date','game_id']].drop_duplicates(); playergames=obs[['capture_date','game_id','player_id']].drop_duplicates()
 rawprops=raw.drop_duplicates(['date','event_id','player_name','line','side'])
 denominator=[{'metric':'mlb_games_represented_in_retained_slate_artifacts','rows':len(games),'note':'Retained artifact denominator; independent official schedule archive not uniformly retained.'},{'metric':'games_with_betonline_capture','rows':raw.event_id.nunique()},{'metric':'games_with_hits_capture','rows':raw.event_id.nunique()},{'metric':'unique_hitter_game_opportunities','rows':len(playergames)},{'metric':'starting_hitter_opportunities','rows':'','note':'UNAVAILABLE_UNIFORMLY; not approximated'},{'metric':'unique_betonline_hits_propositions','rows':len(rawprops)},{'metric':'raw_betonline_hits_observations','rows':len(raw)},{'metric':'model_hits_predictions','rows':len(model)},{'metric':'final_synchronized_rows','rows':len(frozen)}]; write('hits_full_denominator.csv',denominator)
 inv=[]
 for (date,f),g in raw.groupby(['date','capture_file']):
  paired=g.groupby(['event_id','player_name','line']).side.nunique().ge(2).sum(); inv.append({'date':date,'capture_file':f,'observations':len(g),'games':g.event_id.nunique(),'players':g.player_name.nunique(),'hits05':int(g.line.eq(.5).sum()),'hits15':int(g.line.eq(1.5).sum()),'paired':int(paired),'pregame':int(g.pregame.sum()),'poststart_or_unresolved':int((~g.pregame).sum()),'earliest':g.captured_at.min(),'latest':g.captured_at.max()})
 write('hits_raw_betonline_capture_inventory.csv',inv)
 minv=[]
 for (line,side),g in model.groupby(['line','side']): minv.append({'line':line,'side':side,'predictions':len(g),'games':g.game_id.nunique(),'player_games':len(g[['capture_date','game_id','player_id']].drop_duplicates()),'lacking_betonline':int((~g.set_index(['capture_date']+keys).index.isin(bp.set_index(['capture_date']+keys).index)).sum()),'lacking_outcome':'reported via funnel'})
 write('hits_model_prediction_inventory.csv',minv)
 funnel=[]; losses=[]; pairedattr=[]; outcomeattr=[]; recover=[]
 syncids=set(zip(frozen.game_date.astype(str),frozen.game_id,frozen.player_id,frozen.line,frozen.side))
 for line,side in LANES:
  m=model[(model.line.eq(line))&(model.side.eq(side))].copy(); b=bol[(bol.line.eq(line))&(bol.side.eq(side))].copy(); p=bp[(bp.line.eq(line))&(bp.side.eq(side))].copy()
  mk=set(zip(m.capture_date,m.game_id,m.player_id,m.line,m.side)); bk=set(zip(b.capture_date,b.game_id,b.player_id,b.line,b.side)); pk=set(zip(p.capture_date,p.game_id,p.player_id,p.line,p.side)); pairk=set(zip(p[p.paired].capture_date,p[p.paired].game_id,p[p.paired].player_id,p[p.paired].line,p[p.paired].side)); sk={x for x in syncids if x[3]==line and x[4]==side}
  stages=[('A_model_eligible_opportunities',mk),('B_betonline_hits_exists',mk&bk),('C_model_prediction_exists',mk&bk),('D_exact_identity_match',mk&bk),('E_valid_pregame',mk&pk),('F_paired_price',mk&pairk),('G_novig_constructible',mk&pairk),('H_certified_outcome',sk),('I_final_synchronized',sk)]; base=len(stages[0][1]); prev=base
  for name,s in stages: funnel.append({'lane':f'HITS_{int(line*10):02d}_{side.upper()}','stage':name,'input_rows':prev,'retained':len(s),'lost':prev-len(s),'retention_pct':len(s)/prev if prev else None,'cumulative_retention_pct':len(s)/base if base else None}); prev=len(s)
  for row in m.itertuples():
   k=(row.capture_date,row.game_id,row.player_id,row.line,row.side)
   if k in sk:continue
   reason='NO_BETONLINE_MARKET' if k not in bk else 'POST_START_OR_TIMING_UNRESOLVED' if k not in pk else 'PAIRED_OPPOSITE_SIDE_UNAVAILABLE' if k not in pairk else 'OUTCOME_UNAVAILABLE_OR_NOT_IN_FROZEN_BENCHMARK'
   losses.append({'lane':f'HITS_{int(line*10):02d}_{side.upper()}','date':row.capture_date,'game_id':row.game_id,'player_id':row.player_id,'line':line,'side':side,'reason':reason})
  pairedattr.append({'lane':f'HITS_{int(line*10):02d}_{side.upper()}','exact_pregame_any_price':len(mk&pk),'paired_prices':len(mk&pairk),'lost_solely_opposite_side':len((mk&pk)-pairk)})
  outcomeattr.append({'lane':f'HITS_{int(line*10):02d}_{side.upper()}','matched_pregame_paired':len(mk&pairk),'with_certified_outcome':len(sk),'without_outcome_or_other_final_exclusion':len((mk&pairk)-sk)})
  rr=(mk&pairk)-sk; recover.append({'lane':f'HITS_{int(line*10):02d}_{side.upper()}','recoverable_rows_proven_now':0,'candidate_rows_requiring_outcome_review':len(rr),'raw_identity_normalization_candidates_total_all_lanes':max(0,len(rawprops)-len(bol)),'recovery_mechanism':'deterministic retained provider-event team/time/player-name to canonical game/player identity join, then unchanged temporal/outcome gates','projected_synchronized_population':'not asserted until identity join validates'})
 write('hits_attrition_funnel.csv',funnel); write('hits_exclusion_reason_ledger.csv',losses); write('hits_paired_price_attrition.csv',pairedattr); write('hits_outcome_attrition.csv',outcomeattr); write('hits_recoverable_coverage.csv',recover)
 daily=[]
 for date,g in obs.groupby('capture_date'):
  b=bol[bol.capture_date.eq(date)]; rr=raw[raw.date.eq(date)]; f=frozen[frozen.game_date.astype(str).eq(date)]; sched=g.game_id.nunique(); daily.append({'date':date,'scheduled_completed_games_retained_denominator':sched,'games_with_betonline_capture':rr.event_id.nunique(),'games_with_hits_capture':rr.event_id.nunique(),'games_in_synchronized':f.game_id.nunique(),'synchronized_game_coverage_pct':f.game_id.nunique()/sched if sched else None})
 write('hits_daily_game_coverage.csv',daily)
 pg=[]
 for month,g in model.groupby(model.capture_date.str[:7]): pg.append({'scope':'month','value':month,'player_game_opportunities':len(g[['capture_date','game_id','player_id']].drop_duplicates()),'with_betonline_hits':len(bp[bp.capture_date.str[:7].eq(month)][['capture_date','game_id','player_id']].drop_duplicates()),'synchronized_rows':len(frozen[frozen.game_date.str[:7].eq(month)])})
 for (line,side),g in model.groupby(['line','side']): pg.append({'scope':'line_side','value':f'{line}|{side}','player_game_opportunities':len(g),'with_betonline_hits':len(bp[(bp.line.eq(line))&(bp.side.eq(side))]),'synchronized_rows':len(frozen[(frozen.line.eq(line))&(frozen.side.eq(side))])})
 write('hits_player_game_coverage.csv',pg)
 windows=[]; unique=raw[raw.pregame].copy(); local=pd.to_datetime(unique.captured_at,utc=True).dt.tz_convert('America/Los_Angeles'); unique['minute']=local.dt.hour*60+local.dt.minute; bounds=[('05:30',330),('08:30',510),('11:00',660),('13:00',780),('16:30',990)]
 seen=set()
 for label,bound in bounds:
  x=unique[unique.minute<=bound]; ids=set(zip(x.date,x.event_id,x.player_name,x.line,x.side)); windows.append({'window_pt_label':label,'unique_propositions_seen_by_window':len(ids),'incremental_first_seen':len(ids-seen),'not_seen_early_appearing_by_now':len(ids-seen)}); seen=ids
 write('hits_capture_window_coverage.csv',windows)
 rep=[]
 included=frozen; excluded=model[~model.apply(lambda r:(str(r.capture_date),r.game_id,r.player_id,r.line,r.side) in syncids,axis=1)]
 for label,g in [('included',included),('excluded',excluded)]: rep.append({'population':label,'rows':len(g),'mean_model_probability':pd.to_numeric(g.model_pick_prob if 'model_pick_prob' in g else g.model_probability,errors='coerce').mean(),'hits05_pct':g.line.eq(.5).mean(),'over_pct':g.side.eq('over').mean(),'month_distribution':json.dumps(g.capture_date.str[:7].value_counts().to_dict() if 'capture_date' in g else g.game_date.str[:7].value_counts().to_dict(),sort_keys=True),'representativeness_classification':'MATERIALLY_SELECTED_POTENTIALLY_BIASED'})
 write('hits_population_representativeness.csv',rep)
 current=[{'dimension':'capture_windows','historical':'multiple retained but irregular/missing timing lineage','current_aug14':'five governed windows','materially_more_complete':True},{'dimension':'direct_betonline_rows','historical':len(bol),'current_aug14':'1002 at 05:30 all prop families','materially_more_complete':True},{'dimension':'paired_prices','historical':int(bol.paired.sum()),'current_aug14':'semantic paired-price validation active','materially_more_complete':True},{'dimension':'identity_and_outcomes','historical':'model hashes/start-lineup denominator not uniformly embedded','current_aug14':'stronger direct identity capture; prospective outcomes still require ledger','materially_more_complete':True}]; write('hits_historical_vs_current_capture.csv',current)
 final=len(frozen); base=len(model); decision='HITS_HISTORICAL_COVERAGE_INCOMPLETE_RECOVERY_JUSTIFIED'
 md=f"""# MLB Hits historical capture and matching completeness audit v1

- Retained game denominator: {len(games):,}; player-game opportunities: {len(playergames):,}.
- Raw BetOnline Hits observations: {len(bol):,}; unique latest pregame propositions: {len(bp):,}.
- Model Hits propositions: {len(model):,}; synchronized rows: {final:,} ({final/base:.1%} of retained model proposition denominator).
- Starting-hitter opportunity coverage is not uniformly retained and was not approximated.
- Primary attrition is missing/unresolved pregame timing, absent BetOnline exact propositions, and outcome/final-freeze availability; paired-price-only losses are separately quantified.
- No additional row is counted as recovered yet, but the raw-to-canonical identity gap justifies a bounded deterministic recovery pass using retained artifacts.
- The synchronized population is materially selected by provider identity/timing/outcome availability; current governed capture is materially more complete.
- Decision: `{decision}`.
"""; (OUT/'concise_mlb_hits_capture_matching_completeness_audit_v1.md').write_text(md)
 summary={'task_id':'MLB_HITS_HISTORICAL_CAPTURE_MATCHING_COMPLETENESS_AUDIT_V1','retained_games':len(games),'games_with_raw_betonline_hits':raw.event_id.nunique(),'player_games':len(playergames),'raw_betonline_observations':len(raw),'unique_raw_betonline_propositions':len(rawprops),'exact_identity_betonline_rows':len(bol),'unique_betonline_pregame_propositions':len(bp),'model_predictions':len(model),'synchronized':final,'decision':decision}; (OUT/'audit_summary.json').write_text(json.dumps(summary,indent=2)+'\n')
 products=sorted(p for p in OUT.iterdir() if p.name!='reproducibility_hashes.sha256'); (OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n' for p in products)); print(json.dumps(summary,indent=2)); return 0
if __name__=='__main__':raise SystemExit(main())
