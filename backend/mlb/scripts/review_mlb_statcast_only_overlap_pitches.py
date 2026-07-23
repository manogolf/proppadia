#!/usr/bin/env python3
"""Classify the frozen Statcast-only pitch differences without mutating either source."""
import csv,json,hashlib
from pathlib import Path
from collections import Counter,defaultdict
import pandas as pd
ROOT=Path(__file__).resolve().parents[3];SC=ROOT/'backend/mlb/data/external/statcast/raw';LOCAL=ROOT/'artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/raw_official_mlb';OUT=ROOT/'artifacts/analysis/model_development/mlb_external_batter_event_platform_v1_normalization/2026-07-22'
def main():
 s=[]
 for mp in sorted(SC.glob('2026/*/request_metadata.json')):
  m=json.load(mp.open());
  if m.get('platform_role','').endswith('NOT_CANONICAL_COVERAGE') or m.get('completion_status') not in {'ACQUIRED_AND_VALIDATED','ACQUIRED_EMPTY_VALID'}:continue
  p=mp.parent/'statcast_search.csv';x=pd.read_csv(p,dtype=str,encoding='utf-8-sig',low_memory=False);x=x[(x.game_date>='2026-05-01')&(x.game_date<='2026-07-09')]
  if len(x):x['statcast_source_chunk']=str(p.relative_to(ROOT));s.append(x[['game_pk','at_bat_number','pitch_number','batter','pitcher','description','events','pitch_type','statcast_source_chunk']])
 s=pd.concat(s,ignore_index=True);local=[];contexts={}
 for p in sorted(LOCAL.glob('*.json')):
  d=json.load(p.open());gid=str(d['gamePk'])
  for a in d['liveData']['plays']['allPlays']:
   ab=str(a.get('atBatIndex',0)+1);ev=[]
   for e in a.get('playEvents',[]):
    if e.get('isPitch'):
     row={'game_pk':gid,'at_bat_number':ab,'pitch_number':str(e.get('pitchNumber')),'local_batter':str(a.get('matchup',{}).get('batter',{}).get('id','')),'local_pitcher':str(a.get('matchup',{}).get('pitcher',{}).get('id','')),'local_description':e.get('details',{}).get('description'),'local_index':e.get('index')};local.append(row);ev.append(row)
   contexts[(gid,ab)]=json.dumps(ev,separators=(',',':'))
 l=pd.DataFrame(local);keys=['game_pk','at_bat_number','pitch_number'];s=s[s.game_pk.isin(set(l.game_pk))].copy();s['duplicate_composite_key']=s.duplicated(keys,keep=False);m=s.merge(l[keys],on=keys,how='left',indicator=True);e=m[m._merge.eq('left_only')].copy();localset=set(map(tuple,l[keys].astype(str).values));local_by_game=defaultdict(list)
 for r in local:local_by_game[r['game_pk']].append(r)
 out=[]
 for r in e.itertuples():
  gid=str(r.game_pk);ab=str(r.at_bat_number);pn=str(r.pitch_number);near=[x for x in local_by_game[gid] if x['pitch_number']==pn and x['local_batter']==str(r.batter) and x['local_pitcher']==str(r.pitcher) and abs(int(x['at_bat_number'])-int(ab))<=1]
  duplicate=int(r.duplicate_composite_key)
  if duplicate:cls='DUPLICATE_STATCAST_ROW';reason='duplicate Statcast composite key'
  elif near:cls='PITCH_NUMBERING_DIFFERENCE';reason='same batter/pitcher/pitch number found at adjacent local at-bat identity'
  elif str(r.description) in {'pitchout','intent_ball'}:cls='NON_PITCH_EVENT_INCLUDED_BY_STATCAST';reason='Statcast literal description has special pitch semantics'
  else:cls='LOCAL_FEED_OMISSION';reason='Statcast pitch has no local composite or adjacent-PA identity match'
  out.append({'game_pk':gid,'at_bat_number':ab,'pitch_number':pn,'batter':r.batter,'pitcher':r.pitcher,'description':r.description,'terminal_status':'TERMINAL' if pd.notna(r.events) else 'NONTERMINAL','pa_result':r.events,'statcast_source_chunk':r.statcast_source_chunk,'local_surrounding_event_context':contexts.get((gid,ab),''),'local_identity_offset_exists':bool(near),'classification':cls,'reason':reason,'prior_local_work_materiality':'NO_OUTCOME_LOSS_ALL_LOCAL_PITCHES_MATCHED;PITCH_PROFILE_COUNTS_MAY_DIFFER'})
 OUT.mkdir(parents=True,exist_ok=True);pd.DataFrame(out).to_csv(OUT/'statcast_only_466_pitch_investigation.csv',index=False);pd.DataFrame([{'classification':k,'rows':v} for k,v in Counter(x['classification'] for x in out).items()]).to_csv(OUT/'statcast_only_466_pitch_summary.csv',index=False);print(json.dumps({'rows':len(out),'classes':Counter(x['classification'] for x in out)},default=dict,indent=2))
if __name__=='__main__':main()
