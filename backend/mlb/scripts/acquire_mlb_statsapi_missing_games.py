#!/usr/bin/env python3
"""Inventory 2026 regular-season games and acquire missing final StatsAPI feeds."""
import argparse,csv,hashlib,json,time
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request,urlopen
BASE='https://statsapi.mlb.com'; LOCAL=Path('artifacts/analysis/model_development/mlb_full_benchmark_encounter_ledger_expansion/2026-07-17/raw_official_mlb')
def fetch(u,t):
 with urlopen(Request(u,headers={'User-Agent':'proppadia-authoritative-research/1.0'}),timeout=t) as r:return r.status,r.read()
def sha(b):return hashlib.sha256(b).hexdigest()
def feed_is_final(path):
 try:
  data=json.loads(path.read_text())
  return data.get('gameData',{}).get('status',{}).get('abstractGameState')=='Final'
 except Exception:return False
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--start',default='2026-03-26');ap.add_argument('--end',default='2026-07-21');ap.add_argument('--out-root',default='backend/mlb/data/external/statsapi/raw/2026');ap.add_argument('--timeout',type=int,default=60);ap.add_argument('--sleep',type=float,default=.15);a=ap.parse_args();out=Path(a.out_root);out.mkdir(parents=True,exist_ok=True)
 u=BASE+'/api/v1/schedule?'+urlencode({'sportId':1,'startDate':a.start,'endDate':a.end,'gameType':'R','hydrate':'status'}); _,b=fetch(u,a.timeout); sched=json.loads(b); schedule_path=out/f'schedule_{a.start}_{a.end}.json';schedule_path.write_bytes(b); rows=[]
 local={int(p.stem.rsplit('_',1)[-1]):p for p in LOCAL.glob('*.json')}
 for day in sched.get('dates',[]):
  for g in day.get('games',[]):
   gid=int(g['gamePk']); state=g.get('status',{}).get('abstractGameState',''); detail=g.get('status',{}).get('detailedState',''); dest=out/str(gid)/'feed_live.json'; cls='GAME_NOT_FINAL'
   if state=='Final':
    if dest.exists() and feed_is_final(dest):
     cls='LOCAL_CERTIFIED_REUSED'
    elif gid in local and feed_is_final(local[gid]):
     cls='LOCAL_CERTIFIED_REUSED'; dest.parent.mkdir(parents=True,exist_ok=True); lb=local[gid].read_bytes();
     dest.write_bytes(lb)
    else:
     cls='MISSING_REQUIRES_ACQUISITION'; _,fb=fetch(BASE+f'/api/v1.1/game/{gid}/feed/live',a.timeout); dest.parent.mkdir(parents=True,exist_ok=True); tmp=dest.with_suffix('.json.partial');tmp.write_bytes(fb);tmp.replace(dest);cls='ACQUIRED_AND_VALIDATED';time.sleep(a.sleep)
   elif any(x in detail.lower() for x in ('postpon','suspend')): cls='POSTPONED_OR_SUSPENDED'
   rows.append({'game_date':day.get('date'),'game_pk':gid,'game_type':g.get('gameType'),'abstract_state':state,'detailed_state':detail,'classification':cls,'path':str(dest) if dest.exists() else '','size_bytes':dest.stat().st_size if dest.exists() else 0,'sha256':sha(dest.read_bytes()) if dest.exists() else ''})
 ledger_path=out/f'completion_ledger_{a.start}_{a.end}.csv'
 with ledger_path.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
 print(json.dumps({'games':len(rows),'classes':{c:sum(r['classification']==c for r in rows) for c in sorted({r['classification'] for r in rows})}},indent=2))
if __name__=='__main__':main()
