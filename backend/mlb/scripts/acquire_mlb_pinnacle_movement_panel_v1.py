#!/usr/bin/env python3
"""Acquire the authorized immutable A/B/C Pinnacle historical movement panel."""
from __future__ import annotations
import hashlib,json,os,time
from datetime import datetime,timezone
from pathlib import Path
import pandas as pd,requests

ROOT=Path(__file__).resolve().parents[3];PRE=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10';FUS=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_market_state_fusion_v1/2026-08-12/pinnacle_fusion_population.csv';OUT=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_movement_and_later_market_prediction_v1/2026-08-12';RAW=OUT/'raw';TARGETS={'A_EARLY':18,'B_MORNING':8,'C_LATE':4}
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 OUT.mkdir(parents=True,exist_ok=True);RAW.mkdir(exist_ok=True);d=pd.read_csv(FUS);d.start_utc=pd.to_datetime(d.scheduled_start_utc,utc=True);key=os.environ.get('ODDS_API_KEY') or os.environ.get('THE_ODDS_API_KEY');assert key,'Odds API key unavailable'
 rows=[];start_quota=None;end_quota=None
 for day,g in d.groupby('game_date'):
  anchor=g.start_utc.min()
  for slot,hours in TARGETS.items():
   target=anchor-pd.Timedelta(hours=hours);stamp=target.isoformat().replace('+00:00','Z');dest=RAW/f'{day}_{slot}_{target.strftime("%H%M%SZ")}.json';meta=dest.with_suffix('.metadata.json')
   if dest.exists() and meta.exists():
    md=json.loads(meta.read_text());rows.append(md);continue
   url='https://api.the-odds-api.com/v4/historical/sports/baseball_mlb/odds';params={'apiKey':key,'regions':'us','markets':'h2h,totals,spreads','bookmakers':'pinnacle','oddsFormat':'american','dateFormat':'iso','date':stamp};fetch=datetime.now(timezone.utc).isoformat();resp=requests.get(url,params=params,timeout=60);payload=resp.json() if resp.ok else {'error':resp.text};dest.write_text(json.dumps(payload,sort_keys=True,separators=(',',':')))
   cost=int(resp.headers.get('x-requests-last',0) or 0);rem=resp.headers.get('x-requests-remaining');used=resp.headers.get('x-requests-used');start_quota=start_quota or (int(rem)+cost if rem else None);end_quota=int(rem) if rem else end_quota
   md={'game_date':day,'slot':slot,'target_hours':hours,'anchor_start_utc':anchor.isoformat(),'requested_timestamp_utc':stamp,'returned_snapshot_utc':payload.get('timestamp') if isinstance(payload,dict) else None,'fetch_timestamp_utc':fetch,'endpoint':url,'query_parameters':'regions=us;markets=h2h,totals,spreads;bookmakers=pinnacle;oddsFormat=american;dateFormat=iso','http_status':resp.status_code,'request_cost':cost,'quota_remaining':rem,'quota_used':used,'raw_path':str(dest.relative_to(ROOT)),'response_sha256':sha(dest),'events_returned':len(payload.get('data',[])) if isinstance(payload,dict) else 0,'status':'SUCCEEDED' if resp.ok else 'FAILED'};meta.write_text(json.dumps(md,indent=2,sort_keys=True)+'\n');rows.append(md);resp.raise_for_status();time.sleep(.1)
 pd.DataFrame(rows).to_csv(OUT/'movement_historical_acquisition_manifest.csv',index=False);print(json.dumps({'requests':len(rows),'succeeded':sum(x['status']=='SUCCEEDED' for x in rows),'credits':sum(x['request_cost'] for x in rows),'starting_quota':start_quota,'ending_quota':end_quota},indent=2))
if __name__=='__main__':main()
