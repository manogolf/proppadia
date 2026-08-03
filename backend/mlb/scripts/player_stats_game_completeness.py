#!/usr/bin/env python3
"""Exact official-participant completeness and bounded game repair for player_stats."""
from __future__ import annotations
import argparse,csv,hashlib,json,uuid
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
import requests
from backend.shared.db.pg import pg_connect
from backend.mlb.scripts.insert_mlb_stat_derived import _extract_player_stats_row,_upsert_game_info_min,_upsert_player_id_min
from backend.mlb.scripts.cleanroom_v1.routine_market_lifecycle import role
ROOT=Path(__file__).resolve().parents[3]
DEFAULT=ROOT/'artifacts/analysis/mlb/player_stats_completeness'
STAT_FIELDS=['plate_appearances','at_bats','hits','singles','doubles','triples','home_runs','total_bases']
def sh(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def display_path(p):
 try:return str(p.relative_to(ROOT))
 except ValueError:return str(p)
def write(p,rows,fields=None):
 fields=fields or list(dict.fromkeys(k for r in rows for k in r));p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields,lineterminator='\n');w.writeheader();w.writerows(rows)
def read(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def fetch(url):
 q=requests.get(url,timeout=45);q.raise_for_status();return q.content
def sources(game,out):
 out.mkdir(parents=True,exist_ok=True);files={}
 for kind,url in [('live_feed',f'https://statsapi.mlb.com/api/v1.1/game/{game}/feed/live'),('boxscore',f'https://statsapi.mlb.com/api/v1/game/{game}/boxscore')]:
  data=fetch(url);h=hashlib.sha256(data).hexdigest();p=out/f'game_{game}_{kind}_{h}.json'
  if not p.exists():p.write_bytes(data)
  files[kind]=(json.loads(data),p,h)
 return files
def participant_rows(feed):
 game=int(feed['gameData']['game']['pk']);teams=feed['gameData']['teams'];out=[]
 for side in ('away','home'):
  team=teams[side];opp=teams['home' if side=='away' else 'away']
  for p in feed['liveData']['boxscore']['teams'][side].get('players',{}).values():
   pid=int(p['person']['id']);rr,pos=role(p);bat=(p.get('stats') or {}).get('batting') or {}
   if rr=='DID_NOT_APPEAR' or (not bat and rr=='ROLE_AMBIGUOUS_TECHNICAL_UNRESOLVED'):continue
   h=int(bat.get('hits') or 0);db=int(bat.get('doubles') or 0);tr=int(bat.get('triples') or 0);hr=int(bat.get('homeRuns') or 0);sing=h-db-tr-hr
   out.append({'game_pk':game,'player_mlb_id':pid,'player':p['person'].get('fullName',''),'team':team['abbreviation'],'opponent':opp['abbreviation'],'team_id':team['id'],'is_home':side=='home','position':(p.get('position') or {}).get('abbreviation'),'official_role':rr,'batting_order':pos,'plate_appearances':int(bat.get('plateAppearances') or 0),'at_bats':int(bat.get('atBats') or 0),'hits':h,'singles':sing,'doubles':db,'triples':tr,'home_runs':hr,'total_bases':sing+2*db+3*tr+4*hr,'walks':int(bat.get('baseOnBalls') or 0),'strikeouts_batting':int(bat.get('strikeOuts') or 0),'runs_scored':int(bat.get('runs') or 0),'rbis':int(bat.get('rbi') or 0),'stolen_bases':int(bat.get('stolenBases') or 0),'hit_by_pitch':int(bat.get('hitByPitch') or 0),'sacrifice_flies':int(bat.get('sacFlies') or 0),'sacrifice_hits':int(bat.get('sacBunts') or 0),'catcher_interference':int(bat.get('catchersInterference') or 0),'raw_stats':p.get('stats') or {}})
 return sorted(out,key=lambda x:x['player_mlb_id'])
def local_rows(game,conn=None):
 own=conn is None;conn=conn or pg_connect()
 try:
  with conn.cursor() as cur:cur.execute('SELECT player_id,game_id,game_date,team,opponent,is_home,position,is_starter,plate_appearances,at_bats,hits,singles,doubles,triples,home_runs,total_bases FROM mlb.player_stats WHERE game_id=%s',(game,));return [dict(x) for x in cur.fetchall()]
 finally:
  if own:conn.close()
def compare(official,local):
 by=defaultdict(list)
 for x in local:by[(int(x['game_id']),int(x['player_id']))].append(x)
 official_ids={int(x['player_mlb_id']) for x in official};rows=[]
 for x in official:
  found=by[(int(x['game_pk']),int(x['player_mlb_id']))];decision='COMPLETE_EXACT';mismatch=[]
  if not found:decision='MISSING_OFFICIAL_PARTICIPANTS'
  elif len(found)>1:decision='DUPLICATE_LOCAL_ROWS'
  else:
   for k in STAT_FIELDS:
    if int(found[0].get(k) or 0)!=int(x[k] or 0):mismatch.append(k)
   if mismatch:decision='STAT_MISMATCH'
  rows.append({**{k:v for k,v in x.items() if k!='raw_stats'},'local_row_count':len(found),'decision':decision,'mismatch_fields':'|'.join(mismatch)})
 extra=[x for x in local if int(x['player_id']) not in official_ids and str(x.get('position') or '') not in ('P','SP','RP')]
 for x in extra:rows.append({'game_pk':x['game_id'],'player_mlb_id':x['player_id'],'player':'','local_row_count':1,'decision':'EXTRA_LOCAL_ROWS','mismatch_fields':''})
 return rows
def decision(rows):
 priority=['OFFICIAL_PAYLOAD_MISSING','GAME_NOT_FINAL','DUPLICATE_LOCAL_ROWS','MISSING_OFFICIAL_PARTICIPANTS','EXTRA_LOCAL_ROWS','STAT_MISMATCH']
 states={r['decision'] for r in rows}
 return next((x for x in priority if x in states),'COMPLETE_EXACT')
def inspect_game(game,out):
 src=sources(game,out/'sources');feed,path,digest=src['live_feed'];status=feed['gameData']['status'];final=status.get('abstractGameState')=='Final' or status.get('codedGameState')=='F';official=participant_rows(feed) if final else [];local=local_rows(game);rows=compare(official,local) if final else [{'game_pk':game,'decision':'GAME_NOT_FINAL'}]
 summary={'game_pk':game,'official_status':status,'official_completion_timestamp':feed.get('liveData',{}).get('gameData',{}).get('datetime'),'official_batter_participants':len(official),'local_batter_rows':sum(r['local_row_count']!='0' for r in rows if r.get('decision')!='EXTRA_LOCAL_ROWS'),'exact_matches':sum(r['decision']=='COMPLETE_EXACT' for r in rows),'missing_local_participants':sum(r['decision']=='MISSING_OFFICIAL_PARTICIPANTS' for r in rows),'extra_local_rows':sum(r['decision']=='EXTRA_LOCAL_ROWS' for r in rows),'duplicate_local_rows':sum(r['decision']=='DUPLICATE_LOCAL_ROWS' for r in rows),'stat_conflicts':sum(r['decision']=='STAT_MISMATCH' for r in rows),'identity_conflicts':0,'classification':decision(rows),'live_feed_path':display_path(path),'live_feed_sha256':digest,'boxscore_path':display_path(src['boxscore'][1]),'boxscore_sha256':src['boxscore'][2]}
 return summary,official,local,rows,feed
def canonical_row(x,date):
 row=_extract_player_stats_row(player_id=x['player_mlb_id'],game_id=x['game_pk'],game_date=date,team_abbr=x['team'],opponent_abbr=x['opponent'],is_home=x['is_home'],position=x['position'],stats=x['raw_stats'],is_starter=False);row.update({'plate_appearances':x['plate_appearances'],'hit_by_pitch':x['hit_by_pitch'],'sacrifice_flies':x['sacrifice_flies'],'sacrifice_hits':x['sacrifice_hits'],'catcher_interference':x['catcher_interference'],'pa_source':'statsapi','pa_backfilled_at':datetime.now(timezone.utc)});return row
def dry_run(game,out):
 summary,official,local,trace,feed=inspect_game(game,out);by=Counter(int(x['player_id']) for x in local);date=feed['gameData']['datetime']['officialDate'];rows=[]
 for x in official:
  n=by[x['player_mlb_id']];action='INSERT_MISSING_OFFICIAL_PARTICIPANT' if n==0 else 'NO_ACTION_ALREADY_PRESENT' if n==1 else 'HARD_REJECT_IDENTITY_CONFLICT';c=canonical_row(x,date);rows.append({**{k:v for k,v in x.items() if k!='raw_stats'},**{f'canonical_{k}':v for k,v in c.items()},'existing_local_row_count':n,'repair_action':action,'official_payload_path':summary['live_feed_path'],'official_payload_sha256':summary['live_feed_sha256'],'arithmetic_verified':x['total_bases']==x['singles']+2*x['doubles']+3*x['triples']+4*x['home_runs']})
 write(out/f'game_{game}_repair_dry_run.csv',rows);manifest={'game_pk':game,'created_at_utc':datetime.now(timezone.utc).isoformat(),'source_sha256':summary['live_feed_sha256'],'dry_run_sha256':sh(out/f'game_{game}_repair_dry_run.csv'),'clean':all(r['repair_action'] in ('INSERT_MISSING_OFFICIAL_PARTICIPANT','NO_ACTION_ALREADY_PRESENT') and r['arithmetic_verified'] for r in rows),'insert_candidates':sum(r['repair_action']=='INSERT_MISSING_OFFICIAL_PARTICIPANT' for r in rows),'conflicts':sum(r['repair_action'].startswith('HARD_REJECT') for r in rows)};(out/f'game_{game}_repair_dry_run_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');return manifest,rows,feed
def repair(game,out):
 existing_manifest=out/f'game_{game}_repair_write_manifest.csv'
 if existing_manifest.exists():
  prior=read(existing_manifest);summary,official,local,verify,feed=inspect_game(game,out);src=sources(game,out/'sources')['live_feed'];record={'repair_run_id':str(uuid.uuid4()),'repair_timestamp_utc':datetime.now(timezone.utc).isoformat(),'game_pk':game,'source_payload_sha256':src[2],'before_player_stats_rows':len(local),'inserted_rows':0,'inserted_player_ids':'','changed_existing_rows':0,'after_player_stats_rows':len(local),'post_repair_classification':summary['classification'],'rollback_sql':prior[0]['rollback_sql'],'idempotence_replay':True};write(existing_manifest,prior+[record]);write(out/f'game_{game}_post_repair_verification.csv',verify);return record
 manifest,rows,feed=dry_run(game,out)
 if not manifest['clean']:raise RuntimeError('REPAIR_DRY_RUN_NOT_CLEAN')
 before=local_rows(game);run_id=str(uuid.uuid4());inserted=[];date=feed['gameData']['datetime']['officialDate'];game_info_existed=False
 with pg_connect() as conn:
  with conn.cursor() as cur:cur.execute('SELECT 1 FROM mlb.game_info WHERE game_id=%s',(game,));game_info_existed=cur.fetchone() is not None
  schedule_game={'gamePk':game,'gameDate':feed['gameData']['datetime']['dateTime'],'teams':{'home':{'team':feed['gameData']['teams']['home']},'away':{'team':feed['gameData']['teams']['away']}}};_upsert_game_info_min(conn,schedule_game,date)
  has_team=True;has_team_id=True;has_placeholder=True
  for x in rows:
   if x['repair_action']!='INSERT_MISSING_OFFICIAL_PARTICIPANT':continue
   c={k[10:]:v for k,v in x.items() if k.startswith('canonical_')};_upsert_player_id_min(conn,player_id=int(x['player_mlb_id']),player_name=x['player'],team_abbr=x['team'],team_id=int(x['team_id']),has_team_col=has_team,has_team_id_col=has_team_id,has_placeholder_col=has_placeholder)
   with conn.cursor() as cur:
    cur.execute('''INSERT INTO mlb.player_stats (player_id,game_id,game_date,team,opponent,is_home,position,at_bats,hits,total_bases,rbis,runs_scored,strikeouts_batting,walks,singles,doubles,triples,home_runs,stolen_bases,strikeouts_pitching,walks_allowed,hits_allowed,outs_recorded,earned_runs,is_starter,plate_appearances,hit_by_pitch,sacrifice_flies,sacrifice_hits,catcher_interference,pa_source,pa_backfilled_at) VALUES (%(player_id)s,%(game_id)s,%(game_date)s,%(team)s,%(opponent)s,%(is_home)s,%(position)s,%(at_bats)s,%(hits)s,%(total_bases)s,%(rbis)s,%(runs_scored)s,%(strikeouts_batting)s,%(walks)s,%(singles)s,%(doubles)s,%(triples)s,%(home_runs)s,%(stolen_bases)s,%(strikeouts_pitching)s,%(walks_allowed)s,%(hits_allowed)s,%(outs_recorded)s,%(earned_runs)s,%(is_starter)s,%(plate_appearances)s,%(hit_by_pitch)s,%(sacrifice_flies)s,%(sacrifice_hits)s,%(catcher_interference)s,%(pa_source)s,%(pa_backfilled_at)s) ON CONFLICT (player_id,game_id) DO NOTHING''',c)
    if cur.rowcount!=1:raise RuntimeError(f'CONCURRENT_OR_CONFLICTING_ROW player={x["player_mlb_id"]}')
   inserted.append(int(x['player_mlb_id']))
  conn.commit()
 after_summary,official,after,verify,_=inspect_game(game,out);rollback=['BEGIN;']+[f'DELETE FROM mlb.player_stats WHERE game_id = {game} AND player_id = {pid};' for pid in inserted]
 if not game_info_existed:rollback.append(f'DELETE FROM mlb.game_info WHERE game_id = {game};')
 rollback+=['COMMIT;'];(out/f'game_{game}_rollback.sql').write_text('\n'.join(rollback)+'\n');write(out/f'game_{game}_post_repair_verification.csv',verify)
 record={'repair_run_id':run_id,'repair_timestamp_utc':datetime.now(timezone.utc).isoformat(),'game_pk':game,'source_payload_sha256':manifest['source_sha256'],'before_player_stats_rows':len(before),'inserted_rows':len(inserted),'inserted_player_ids':'|'.join(map(str,inserted)),'changed_existing_rows':0,'after_player_stats_rows':len(after),'post_repair_classification':after_summary['classification'],'rollback_sql':display_path(out/f'game_{game}_rollback.sql'),'idempotence_replay':False};write(out/f'game_{game}_repair_write_manifest.csv',[record]);return record
def inspect_date(date,out):
 schedule=json.loads(fetch(f'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}'));games=[]
 for block in schedule.get('dates',[]):
  for g in block.get('games',[]):
   s=g.get('status') or {}
   if s.get('abstractGameState')=='Final' or s.get('codedGameState')=='F':games.append(int(g['gamePk']))
 rows=[]
 for game in games:
  try:summary,*_=inspect_game(game,out/f'game_{game}');rows.append(summary)
  except Exception as e:rows.append({'game_pk':game,'classification':'OFFICIAL_PAYLOAD_MISSING','error':f'{type(e).__name__}: {e}'})
 write(out/f'player_stats_date_completeness_{date}.csv',rows);return rows
def recover_date(date,out):
 before=inspect_date(date,out);blocked=[r for r in before if r['classification'] not in ('COMPLETE_EXACT','MISSING_OFFICIAL_PARTICIPANTS')]
 if blocked:raise RuntimeError('COMPLETED_GAME_PLAYER_STATS_INCOMPLETE conflicts='+','.join(f"{r['game_pk']}:{r['classification']}" for r in blocked))
 repaired=[]
 for r in before:
  if r['classification']=='MISSING_OFFICIAL_PARTICIPANTS':repaired.append(repair(int(r['game_pk']),out/f"game_{r['game_pk']}"))
 after=inspect_date(date,out);remaining=[r for r in after if r['classification']!='COMPLETE_EXACT']
 if remaining:raise RuntimeError('COMPLETED_GAME_PLAYER_STATS_INCOMPLETE recovery_failed='+','.join(f"{r['game_pk']}:{r['classification']}" for r in remaining))
 return {'date':date,'affected_games':len(repaired),'new_writes':sum(int(r['inserted_rows']) for r in repaired),'status':'COMPLETE_EXACT'}
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--mode',choices=('game','date','repair','recover-date'),required=True);ap.add_argument('--game-pk',type=int);ap.add_argument('--date');ap.add_argument('--output-dir',type=Path);a=ap.parse_args();out=a.output_dir or DEFAULT/(a.date or str(a.game_pk));out=out.resolve()
 if a.mode=='game':
  if not a.game_pk:ap.error('--game-pk required')
  summary,official,local,rows,_=inspect_game(a.game_pk,out);write(out/f'game_{a.game_pk}_official_participants.csv',[{k:v for k,v in x.items() if k!='raw_stats'} for x in official]);write(out/f'game_{a.game_pk}_local_player_stats.csv',local);write(out/f'game_{a.game_pk}_completeness.csv',rows);dry_run(a.game_pk,out);print(json.dumps(summary,indent=2));return 0 if summary['classification']=='COMPLETE_EXACT' else 3
 if a.mode=='date':
  if not a.date:ap.error('--date required')
  rows=inspect_date(a.date,out);print(json.dumps({'date':a.date,'final_games_audited':len(rows),'classifications':dict(Counter(r['classification'] for r in rows))},indent=2));return 0 if all(r['classification']=='COMPLETE_EXACT' for r in rows) else 3
 if a.mode=='recover-date':
  if not a.date:ap.error('--date required')
  print(json.dumps(recover_date(a.date,out),indent=2));return 0
 if not a.game_pk:ap.error('--game-pk required')
 print(json.dumps(repair(a.game_pk,out),indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
