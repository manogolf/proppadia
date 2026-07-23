#!/usr/bin/env python3
"""Resumable official Baseball Savant Statcast CSV acquisition."""
from __future__ import annotations
import argparse,csv,hashlib,json,time
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request,urlopen

BASE='https://baseballsavant.mlb.com/statcast_search/csv'
REQ={'game_date','game_pk','at_bat_number','pitch_number','batter','pitcher','game_type'}
def sha(b): return hashlib.sha256(b).hexdigest()
def chunks(a,b,n):
 x=a
 while x<=b:
  y=min(b,x+timedelta(days=n-1)); yield x,y; x=y+timedelta(days=1)
def url(a,b):
 params={'all':'true','type':'details','game_date_gt':a.isoformat(),'game_date_lt':b.isoformat(),'hfGT':'R|','player_type':'batter','group_by':'name-date','min_pitches':'0','min_results':'0','min_pas':'0','sort_col':'pitches','sort_order':'desc'}
 return BASE+'?'+urlencode(params)
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--start',required=True); ap.add_argument('--end',required=True); ap.add_argument('--chunk-days',type=int,default=7); ap.add_argument('--out-root',default='backend/mlb/data/external/statcast/raw'); ap.add_argument('--timeout',type=int,default=120); ap.add_argument('--retries',type=int,default=3); ap.add_argument('--sleep',type=float,default=1.0); a=ap.parse_args()
 start=date.fromisoformat(a.start); end=date.fromisoformat(a.end); assert 1<=a.chunk_days<=31
 ledger=[]
 for lo,hi in chunks(start,end,a.chunk_days):
  folder=Path(a.out_root)/str(lo.year)/f'{lo}_{hi}'; folder.mkdir(parents=True,exist_ok=True); raw=folder/'statcast_search.csv'; meta=folder/'request_metadata.json'; u=url(lo,hi)
  if raw.exists() and meta.exists():
   m=json.loads(meta.read_text())
   if int(m.get('raw_row_count',0))>=25000:
    m['completion_status']='RETRYABLE_FAILURE'; m['error']='possible 25000-row source truncation; preserve raw and reacquire with smaller chunks'; meta.write_text(json.dumps(m,indent=2)+'\n')
   ledger.append(m); print('REUSED',lo,hi,m.get('raw_row_count'),m.get('completion_status')); continue
  status='RETRYABLE_FAILURE'; err=''; body=None; http=''; tries=0; ts=datetime.now(timezone.utc).isoformat()
  for tries in range(a.retries+1):
   try:
    with urlopen(Request(u,headers={'User-Agent':'proppadia-authoritative-research/1.0','Accept':'text/csv'}),timeout=a.timeout) as r: http=r.status; body=r.read()
    if body.lstrip().lower().startswith(b'<!doctype html') or b'<html' in body[:500].lower(): raise ValueError('HTML response')
    break
   except Exception as e: err=repr(e); body=None; time.sleep(min(30,2**tries))
  rows=0; parsed=0; cols=[]; dup=0
  if body is not None:
   text=body.decode('utf-8-sig'); rr=list(csv.DictReader(text.splitlines())); rows=parsed=len(rr); cols=list(rr[0]) if rr else (text.splitlines()[0].split(',') if text.strip() else []); cols_normalized=[c.strip().strip('"') for c in cols]
   keys=[(r.get('game_pk'),r.get('at_bat_number'),r.get('pitch_number')) for r in rr]; dup=len(keys)-len(set(keys)); missing=sorted(REQ-set(cols)); out_of_range=sum(not(lo.isoformat()<=str(r.get('game_date',''))<=hi.isoformat()) for r in rr); bad_game=sum(not str(r.get('game_pk','')).replace('.0','').isdigit() for r in rr)
   missing=sorted(REQ-set(cols_normalized))
   status='ACQUIRED_EMPTY_VALID' if not rr and not missing else ('SCHEMA_DRIFT' if missing else ('PARSE_FAILURE' if out_of_range or bad_game else ('RETRYABLE_FAILURE' if rows>=25000 else 'ACQUIRED_AND_VALIDATED')))
   if status in {'ACQUIRED_AND_VALIDATED','ACQUIRED_EMPTY_VALID'}:
    tmp=raw.with_suffix('.csv.partial'); tmp.write_bytes(body); tmp.replace(raw)
   else: err=json.dumps({'missing_columns':missing,'out_of_range':out_of_range,'bad_game_pk':bad_game,'possible_25000_row_truncation':rows>=25000})
  m={'source':'BASEBALL_SAVANT_STATCAST_SEARCH_CSV','domain':'baseballsavant.mlb.com','start_date':str(lo),'end_date':str(hi),'request_url':u,'request_timestamp_utc':ts,'http_status':http,'response_size':len(body or b''),'raw_row_count':rows,'parsed_row_count':parsed,'sha256':sha(body) if body else '','duplicate_pitch_keys':dup,'schema_columns':cols,'retry_count':tries,'completion_status':status,'error':err,'raw_path':str(raw)}; meta.write_text(json.dumps(m,indent=2)+'\n'); ledger.append(m); print(status,lo,hi,rows,len(body or b'')); time.sleep(a.sleep)
  if status not in {'ACQUIRED_AND_VALIDATED','ACQUIRED_EMPTY_VALID'}: break
 print(json.dumps({'chunks':len(ledger),'statuses':{s:sum(x['completion_status']==s for x in ledger) for s in sorted({x['completion_status'] for x in ledger})}},indent=2))
if __name__=='__main__': main()
