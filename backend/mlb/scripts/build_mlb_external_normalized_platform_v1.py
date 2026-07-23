#!/usr/bin/env python3
"""Deterministically materialize normalized MLB external platform v1 Parquet tables."""
from __future__ import annotations
import argparse,csv,hashlib,json,time,tracemalloc
from collections import defaultdict
from pathlib import Path
import numpy as np,pandas as pd,pyarrow as pa,pyarrow.parquet as pq
ROOT=Path(__file__).resolve().parents[3]; RAW=ROOT/'backend/mlb/data/external'; NORM=RAW/'normalized/v1'; ACQ=ROOT/'artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22'
SC=RAW/'statcast/raw'; SA=RAW/'statsapi/raw/2026'; RS=RAW/'retrosheet/raw/csv_release_through_2025/extracted'; CH=ROOT/'backend/mlb/data/raw/retrosheet/chadwick_register/people.csv'
TEAM={'AZ':'ARI','ARI':'ARI','CHA':'CWS','CHW':'CWS','CWS':'CWS','KCA':'KC','KC':'KC','TBA':'TB','TB':'TB','SDN':'SD','SD':'SD','SFN':'SF','SF':'SF','LAN':'LAD','LAD':'LAD','NYA':'NYY','NYY':'NYY','NYN':'NYM','NYM':'NYM','CHN':'CHC','CHC':'CHC','SLN':'STL','STL':'STL','WAS':'WSH','WSH':'WSH','ANA':'LAA','LAA':'LAA','FLO':'MIA','MIA':'MIA'}
def sha(p):
 h=hashlib.sha256();
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1<<20),b''):h.update(b)
 return h.hexdigest()
def normalized_display_path(p):
 try:return str(p.relative_to(ROOT))
 except ValueError:return str(p.relative_to(NORM))
def write(df,table,season,part):
 d=NORM/table/f'season={season}';d.mkdir(parents=True,exist_ok=True);p=d/f'part-{part}.parquet';tmp=p.with_suffix('.parquet.partial')
 tab=pa.Table.from_pandas(df,preserve_index=False);pq.write_table(tab,tmp,compression='zstd',compression_level=3,use_dictionary=True);tmp.replace(p);return p
def main():
 global NORM
 ap=argparse.ArgumentParser();ap.add_argument('--rebuild',action='store_true');ap.add_argument('--output-root',type=Path,default=NORM);ap.add_argument('--measure-memory',action='store_true');a=ap.parse_args();NORM=a.output_root.resolve();NORM.mkdir(parents=True,exist_ok=True);start=time.time()
 if a.measure_memory:tracemalloc.start()
 files=[];reports=[];games=[];players={};pa_total=bb_total=pitch_total=0
 metas=[]
 for mp in sorted(SC.glob('*/*/request_metadata.json')):
  m=json.load(mp.open());
  if m.get('platform_role','').endswith('NOT_CANONICAL_COVERAGE') or m.get('completion_status') not in {'ACQUIRED_AND_VALIDATED','ACQUIRED_EMPTY_VALID'}:continue
  p=mp.parent/'statcast_search.csv'
  if not p.exists() or not p.stat().st_size:continue
  metas.append((mp,p,m))
 # Statcast tables retain every raw field plus exact lineage.
 for idx,(mp,p,m) in enumerate(metas):
  x=pd.read_csv(p,dtype=str,encoding='utf-8-sig',low_memory=False); season=int(m['start_date'][:4]); part=f"{m['start_date']}_{m['end_date']}"
  if not len(x):continue
  x['source_raw_path']=str(p.relative_to(ROOT));x['source_raw_sha256']=m['sha256'];x['source_raw_row_ordinal']=np.arange(1,len(x)+1,dtype=np.int64);x['normalized_season']=season
  keys=['game_pk','at_bat_number','pitch_number','batter','pitcher']
  for c in keys:x[c]=pd.to_numeric(x[c],errors='coerce').astype('Int64')
  x['canonical_pitch_key']=x.game_pk.astype(str)+'|'+x.at_bat_number.astype(str)+'|'+x.pitch_number.astype(str)
  dup=x.duplicated('canonical_pitch_key',keep=False);null=x[keys[:3]].isna().any(axis=1)
  x['canonical_status']=np.where(null,'NULL_KEY_BLOCKED',np.where(dup,'DUPLICATE_KEY_BLOCKED','CANONICAL'))
  canonical=x[x.canonical_status.eq('CANONICAL')].copy();pitch_total+=len(canonical);out=write(canonical,'pitches',season,part);files.append(('pitches',out,len(canonical),str(p.relative_to(ROOT)),m['sha256']))
  if dup.any():
   q=write(x[dup],'pitch_duplicate_ledger',season,part);files.append(('pitch_duplicate_ledger',q,int(dup.sum()),str(p.relative_to(ROOT)),m['sha256']))
  terminal=canonical.events.notna()&canonical.events.ne('');pa_df=canonical[terminal].copy();pa_df['canonical_pa_key']=pa_df.game_pk.astype(str)+'|'+pa_df.at_bat_number.astype(str);pa_df['terminal_pitch_key']=pa_df.canonical_pitch_key
  ev=pa_df.events.fillna('');pa_df['hit']=ev.isin(['single','double','triple','home_run']).astype('int8');
  for n,val in [('single','single'),('double','double'),('triple','triple'),('home_run','home_run'),('strikeout','strikeout'),('walk','walk'),('hit_by_pitch','hit_by_pitch')]:pa_df[n]=ev.eq(val).astype('int8')
  pa_df['sacrifice']=ev.str.contains('sac_',regex=False).astype('int8');pa_df['reach_on_error']=ev.eq('field_error').astype('int8');pa_df['fielders_choice']=ev.str.contains('fielders_choice',regex=False).astype('int8');pa_df['ball_in_play']=ev.isin(['single','double','triple','home_run','field_out','force_out','grounded_into_double_play','field_error','fielders_choice','fielders_choice_out','sac_fly','sac_bunt']).astype('int8');pa_df['pitches_seen']=pa_df.pitch_number
  pa_total+=len(pa_df);q=write(pa_df,'plate_appearances',season,part);files.append(('plate_appearances',q,len(pa_df),str(p.relative_to(ROOT)),m['sha256']))
  bmask=terminal&(canonical[['launch_speed','launch_angle','bb_type','launch_speed_angle']].notna().any(axis=1));bb=canonical[bmask].copy();bb['canonical_pa_key']=bb.game_pk.astype(str)+'|'+bb.at_bat_number.astype(str);bb['terminal_pitch_key']=bb.canonical_pitch_key;bb_total+=len(bb);q=write(bb,'batted_balls',season,part);files.append(('batted_balls',q,len(bb),str(p.relative_to(ROOT)),m['sha256']))
  g=x.groupby('game_pk',dropna=True).first().reset_index()
  for r in g.itertuples():games.append({'game_pk':int(r.game_pk),'game_date':r.game_date,'season':season,'game_type':r.game_type,'home_team':r.home_team,'away_team':r.away_team,'savant_coverage':True,'statsapi_coverage':False,'retrosheet_coverage':False,'source':'STATCAST'})
  for role in ('batter','pitcher'):
   for z in x[[role]].dropna()[role].unique():players.setdefault(int(z),{'player_id':int(z),'source_roles':set(),'player_name':''})['source_roles'].add(role.upper())
 # StatsAPI authoritative game/player/lineup/outcome rows for 2026.
 lineups=[];batout=[];pitout=[]
 for fp in sorted(SA.glob('*/feed_live.json')):
  d=json.load(fp.open());gd=d.get('gameData',{});live=d.get('liveData',{});gid=int(d['gamePk']);dt=gd.get('datetime',{}).get('officialDate',''); teams=gd.get('teams',{})
  games.append({'game_pk':gid,'game_date':dt,'season':2026,'game_type':gd.get('game',{}).get('type','R'),'home_team':teams.get('home',{}).get('abbreviation'),'away_team':teams.get('away',{}).get('abbreviation'),'venue':gd.get('venue',{}).get('name'),'official_start_time':gd.get('datetime',{}).get('dateTime'),'official_status':gd.get('status',{}).get('detailedState'),'statsapi_coverage':True,'source':'STATSAPI','source_raw_path':str(fp.relative_to(ROOT)),'source_raw_sha256':sha(fp)})
  for k,v in gd.get('players',{}).items():
   pid=int(v['id']);z=players.setdefault(pid,{'player_id':pid,'source_roles':set(),'player_name':''});z['player_name']=v.get('fullName','');z['bat_side']=v.get('batSide',{}).get('code');z['pitch_hand']=v.get('pitchHand',{}).get('code')
  box=live.get('boxscore',{})
  for side in ('home','away'):
   team=box.get('teams',{}).get(side,{});tid=team.get('team',{}).get('id');tc=team.get('team',{}).get('abbreviation');opp=box.get('teams',{}).get('away' if side=='home' else 'home',{}).get('team',{}).get('abbreviation')
   for pid in team.get('batters',[]):
    z=team.get('players',{}).get(f'ID{pid}',{});s=z.get('stats',{}).get('batting',{});bo=str(z.get('battingOrder') or '')
    if bo and bo.isdigit() and int(bo)%100==0:lineups.append({'game_pk':gid,'team_id':tid,'team':tc,'batting_order_position':int(bo)//100,'player_id':pid,'defensive_position':z.get('position',{}).get('abbreviation'),'home_away':side,'source':'STATSAPI_FINAL_BOXSCORE','lineup_certification_status':'FINAL_LINEUP_ONLY','source_raw_path':str(fp.relative_to(ROOT))})
    batout.append({'game_pk':gid,'player_id':pid,'team':tc,'opponent':opp,'home_away':side,'starting_status':bool(z.get('gameStatus',{}).get('isCurrentBatter') or (bo and bo.isdigit() and int(bo)%100==0)),'lineup_position':int(bo)//100 if bo and bo.isdigit() else pd.NA,'actual_pa':s.get('plateAppearances'),'ab':s.get('atBats'),'hits':s.get('hits'),'doubles':s.get('doubles'),'triples':s.get('triples'),'home_runs':s.get('homeRuns'),'walks':s.get('baseOnBalls'),'hbp':s.get('hitByPitch'),'strikeouts':s.get('strikeOuts'),'runs':s.get('runs'),'rbi':s.get('rbi'),'total_bases':s.get('totalBases'),'official_completion_status':gd.get('status',{}).get('abstractGameState'),'source':'STATSAPI'})
   for pid in team.get('pitchers',[]):
    z=team.get('players',{}).get(f'ID{pid}',{});s=z.get('stats',{}).get('pitching',{});pitout.append({'game_pk':gid,'player_id':pid,'team':tc,'opponent':opp,'home_away':side,'games_started':s.get('gamesStarted'),'innings_pitched':s.get('inningsPitched'),'batters_faced':s.get('battersFaced'),'hits_allowed':s.get('hits'),'walks':s.get('baseOnBalls'),'strikeouts':s.get('strikeOuts'),'source':'STATSAPI'})
 # Exact documented player crosswalk.
 ch=pd.read_csv(CH,dtype=str,low_memory=False);cw=ch[ch.key_mlbam.notna()&ch.key_retro.notna()][['key_mlbam','key_retro','key_person','name_first','name_last']].copy();cw['mlb_player_id']=pd.to_numeric(cw.key_mlbam,errors='coerce').astype('Int64');cw=cw[cw.mlb_player_id.notna()];cw['crosswalk_status']='EXACT_DOCUMENTED';cw['evidence_path']=str(CH.relative_to(ROOT));cw['evidence_sha256']=sha(CH);q=write(cw,'player_identity_crosswalk','all','000');files.append(('player_identity_crosswalk',q,len(cw),str(CH.relative_to(ROOT)),sha(CH)));retro_to_mlb=dict(zip(cw.key_retro,cw.mlb_player_id))
 # Retrosheet exact unique game matching by date/team; doubleheaders remain candidates.
 gi=pd.read_csv(RS/'gameinfo.csv',dtype=str,low_memory=False);gi=gi[(gi.season.astype(int).between(2022,2025))&gi.gametype.eq('regular')].copy();gi['game_date']=pd.to_datetime(gi.date).dt.strftime('%Y-%m-%d');gi['home_mlb']=gi.hometeam.map(lambda x:TEAM.get(x,x));gi['away_mlb']=gi.visteam.map(lambda x:TEAM.get(x,x))
 gdf=pd.DataFrame(games).sort_values(['game_pk','source']).drop_duplicates('game_pk',keep='last');gdf['pair']=gdf.game_date.astype(str)+'|'+gdf.home_team.astype(str)+'|'+gdf.away_team.astype(str);gi['pair']=gi.game_date+'|'+gi.home_mlb+'|'+gi.away_mlb
 gp=gdf.groupby('pair').game_pk.agg(list);rp=gi.groupby('pair').gid.agg(list);cross=[]
 for pair,gids in gp.items():
  rids=rp.get(pair,[]);status='EXACT_MULTI_SOURCE_MATCH' if len(gids)==len(rids)==1 else ('DATE_TEAM_CANDIDATE_NOT_CERTIFIED' if rids else 'MLB_ONLY')
  for gid in gids:cross.append({'game_pk':gid,'retrosheet_game_id':rids[0] if status=='EXACT_MULTI_SOURCE_MATCH' else pd.NA,'game_identity_status':status,'candidate_retrosheet_ids':'|'.join(rids)})
 gc=pd.DataFrame(cross);gdf=gdf.merge(gc,on='game_pk',how='left');gdf['retrosheet_coverage']=gdf.game_identity_status.eq('EXACT_MULTI_SOURCE_MATCH');
 for y,z in gdf.groupby('season'):q=write(z,'games',int(y),'000');files.append(('games',q,len(z),'MULTI_SOURCE',''))
 gidmap=dict(zip(gc.dropna(subset=['retrosheet_game_id']).retrosheet_game_id,gc.dropna(subset=['retrosheet_game_id']).game_pk));save_cross=write(gc,'game_identity_reconciliation','all','000');files.append(('game_identity_reconciliation',save_cross,len(gc),'MULTI_SOURCE',''))
 # Player table.
 pdf=pd.DataFrame([{**{k:v for k,v in z.items() if k!='source_roles'},'source_roles':'|'.join(sorted(z['source_roles']))} for z in players.values()]);q=write(pdf,'players','all','000');files.append(('players',q,len(pdf),'MULTI_SOURCE',''))
 # 2026 lineups and outcomes.
 for name,data in [('starting_lineups',lineups),('player_game_batting',batout),('player_game_pitching',pitout)]:
  df=pd.DataFrame(data);q=write(df,name,2026,'000');files.append((name,q,len(df),'STATSAPI_2026',''))
 # Retrosheet 2022-25 lineups and player-game outcomes, exact game and player mappings only.
 ts=pd.read_csv(RS/'teamstats.csv',dtype=str,low_memory=False);ts=ts[ts.gid.isin(gidmap)&ts.gametype.eq('regular')];rl=[]
 for r in ts.itertuples():
  for n in range(1,10):
   rid=getattr(r,f'start_l{n}');rl.append({'game_pk':gidmap[r.gid],'team':TEAM.get(r.team,r.team),'batting_order_position':n,'retrosheet_player_id':rid,'player_id':retro_to_mlb.get(rid,pd.NA),'defensive_position':getattr(r,f'start_f{n}'),'home_away':r.vishome,'source':'RETROSHEET_FINAL_GAME_RECORD','lineup_certification_status':'STARTING_LINEUP_CERTIFIED_TIMESTAMP_UNKNOWN'})
 rdf=pd.DataFrame(rl);rdf['season']=rdf.game_pk.map(dict(zip(gdf.game_pk,gdf.season))); 
 for y,z in rdf.groupby('season'):q=write(z,'starting_lineups',int(y),'retro');files.append(('starting_lineups',q,len(z),'RETROSHEET_TEAMSTATS',''))
 def retro_out(file,kind):
  d=pd.read_csv(RS/file,dtype=str,low_memory=False);d=d[d.gid.isin(gidmap)&d.gametype.eq('regular')].copy();d['game_pk']=d.gid.map(gidmap);d['retrosheet_player_id']=d.id;d['player_id']=d.id.map(retro_to_mlb);d['identity_status']=np.where(d.player_id.notna(),'EXACT_DOCUMENTED','UNMAPPED');d['season']=pd.to_datetime(d.date).dt.year;return d
 rb=retro_out('batting.csv','batting');rpi=retro_out('pitching.csv','pitching')
 for y,z in rb.groupby('season'):q=write(z,'player_game_batting',int(y),'retro');files.append(('player_game_batting',q,len(z),'RETROSHEET_BATTING',''))
 for y,z in rpi.groupby('season'):q=write(z,'player_game_pitching',int(y),'retro');files.append(('player_game_pitching',q,len(z),'RETROSHEET_PITCHING',''))
 # Player-game outcomes is batting union with stable normalized outcome columns.
 outs=[]
 for y,z in rb.groupby('season'):
  o=pd.DataFrame({'game_pk':z.game_pk,'player_id':z.player_id,'retrosheet_player_id':z.id,'team':z.team.map(lambda x:TEAM.get(x,x)),'actual_pa':pd.to_numeric(z.b_pa,errors='coerce'),'ab':pd.to_numeric(z.b_ab,errors='coerce'),'hits':pd.to_numeric(z.b_h,errors='coerce'),'doubles':pd.to_numeric(z.b_d,errors='coerce'),'triples':pd.to_numeric(z.b_t,errors='coerce'),'home_runs':pd.to_numeric(z.b_hr,errors='coerce'),'walks':pd.to_numeric(z.b_w,errors='coerce'),'hbp':pd.to_numeric(z.b_hbp,errors='coerce'),'strikeouts':pd.to_numeric(z.b_k,errors='coerce'),'runs':pd.to_numeric(z.b_r,errors='coerce'),'rbi':pd.to_numeric(z.b_rbi,errors='coerce'),'source':'RETROSHEET'});o['total_bases']=o.hits+o.doubles+2*o.triples+3*o.home_runs;o['hits_runs_rbi']=o.hits+o.runs+o.rbi;q=write(o,'player_game_outcomes',int(y),'retro');files.append(('player_game_outcomes',q,len(o),'RETROSHEET_BATTING',''))
 so=pd.DataFrame(batout);so['hits_runs_rbi']=pd.to_numeric(so.hits)+pd.to_numeric(so.runs)+pd.to_numeric(so.rbi);q=write(so,'player_game_outcomes',2026,'statsapi');files.append(('player_game_outcomes',q,len(so),'STATSAPI_2026',''))
 # Retrosheet observed lineup-state changes yield generic certified substitutions.
 use=['gid','event','inning','top_bot','outs_pre','batteam','date','gametype']+[f'l{i}' for i in range(1,10)]+[f'lf{i}' for i in range(1,10)];chunks=[]
 for x in pd.read_csv(RS/'plays.csv',usecols=use,dtype=str,chunksize=500000,low_memory=False):
  x=x[x.gid.isin(gidmap)&x.gametype.eq('regular')];
  if len(x):chunks.append(x)
 plays=pd.concat(chunks,ignore_index=True);subs=[]
 for gid,z in plays.groupby('gid',sort=False):
  prev={}
  for seq,r in enumerate(z.itertuples(),1):
   team=r.batteam
   for n in range(1,10):
    cur=getattr(r,f'l{n}');old=prev.get((team,n));
    if old and cur and old!=cur:subs.append({'game_pk':gidmap[gid],'retrosheet_game_id':gid,'event_sequence':seq,'inning':r.inning,'half_inning':r.top_bot,'outs':r.outs_pre,'team':TEAM.get(team,team),'player_entering_retro':cur,'player_leaving_retro':old,'player_entering_id':retro_to_mlb.get(cur,pd.NA),'player_leaving_id':retro_to_mlb.get(old,pd.NA),'batting_order_slot':n,'defensive_position':getattr(r,f'lf{n}'),'raw_event_type':r.event,'source_certification_status':'GENERIC_SUBSTITUTION','source':'RETROSHEET_PLAYS'})
    if cur:prev[(team,n)]=cur
 sd=pd.DataFrame(subs);sd['season']=sd.game_pk.map(dict(zip(gdf.game_pk,gdf.season))); 
 for y,z in sd.groupby('season'):q=write(z,'substitutions',int(y),'retro');files.append(('substitutions',q,len(z),'RETROSHEET_PLAYS',''))
 # Source lineage table and manifest.
 lin=pd.DataFrame([{'normalized_table':t,'normalized_path':normalized_display_path(p),'rows':n,'normalized_sha256':sha(p),'raw_source_path':rp,'raw_source_sha256':rh} for t,p,n,rp,rh in files]);q=write(lin,'source_lineage','all','000');files.append(('source_lineage',q,len(lin),'NORMALIZED_MANIFEST',''))
 manifest=pd.DataFrame([{'table':t,'path':normalized_display_path(p),'rows':n,'size_bytes':p.stat().st_size,'sha256':sha(p),'raw_source_path':rp,'raw_source_sha256':rh} for t,p,n,rp,rh in files]);manifest.to_csv(NORM/'normalized_file_manifest.csv',index=False)
 peak=tracemalloc.get_traced_memory()[1] if a.measure_memory else None
 if a.measure_memory:tracemalloc.stop()
 summary={'pitch_rows':pitch_total,'pa_rows':pa_total,'batted_ball_rows':bb_total,'games':len(gdf),'players':len(pdf),'lineups':len(lineups)+len(rdf),'substitutions':len(sd),'player_game_outcomes':len(rb)+len(so),'partitions':len(files),'duration_seconds':time.time()-start,'peak_python_bytes':peak,'compression':'parquet_zstd_level_3','raw_platform_manifest_sha256':sha(ACQ/'raw_file_manifest.csv')};(NORM/'build_summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
if __name__=='__main__':main()
