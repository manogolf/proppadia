#!/usr/bin/env python3
"""Manual-only CLI for NHL moneyline shadow capture; no scheduler integration."""
from __future__ import annotations
import argparse,json,urllib.parse,urllib.request
from datetime import datetime,timezone
from pathlib import Path
from .core import grade_run,historical_parity,run_shadow

def main():
 ap=argparse.ArgumentParser(); sub=ap.add_subparsers(dest='cmd',required=True)
 p=sub.add_parser('parity'); p.add_argument('--matrix',type=Path,required=True); p.add_argument('--predictions',type=Path,required=True); p.add_argument('--output',type=Path,required=True)
 r=sub.add_parser('run'); r.add_argument('--schedule-csv',type=Path,required=True); r.add_argument('--history-csv',type=Path,required=True); r.add_argument('--odds-json',type=Path); r.add_argument('--output-root',type=Path,default=Path('backend/nhl/exports/mainline_shadow')); r.add_argument('--slate-date',required=True); r.add_argument('--run-timestamp-utc',required=True); r.add_argument('--run-type',choices=['MIDDAY','FINAL_PREGAME'],required=True); r.add_argument('--historical-fixture',action='store_true')
 f=sub.add_parser('fetch-h2h'); f.add_argument('--api-key',required=True); f.add_argument('--output',type=Path,required=True); f.add_argument('--regions',default='us,us2')
 g=sub.add_parser('grade'); g.add_argument('--run-dir',type=Path,required=True); g.add_argument('--outcomes-csv',type=Path,required=True); g.add_argument('--grade-root',type=Path,required=True); g.add_argument('--grading-timestamp-utc',required=True)
 a=ap.parse_args()
 if a.cmd=='parity':
  z=historical_parity(a.matrix,a.predictions); a.output.parent.mkdir(parents=True,exist_ok=True); z.to_csv(a.output,index=False); print(z.to_json(orient='records'))
 elif a.cmd=='run': print(run_shadow(a.schedule_csv,a.history_csv,a.odds_json,a.output_root,a.slate_date,a.run_timestamp_utc,a.run_type,a.historical_fixture))
 elif a.cmd=='fetch-h2h':
  if a.output.exists(): raise SystemExit('OVERWRITE_ATTEMPT_BLOCKED')
  q=urllib.parse.urlencode({'regions':a.regions,'markets':'h2h','oddsFormat':'american','dateFormat':'iso','apiKey':a.api_key}); url='https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds?'+q
  req=urllib.request.Request(url,headers={'User-Agent':'proppadia-nhl-mainline-shadow/1.0'}); data=urllib.request.urlopen(req,timeout=30).read(); provider=json.loads(data); captured=datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z'); envelope={'capture_timestamp_utc':captured,'source_acquisition_timestamp_utc':captured,'provider_response':provider}; a.output.parent.mkdir(parents=True,exist_ok=True); a.output.write_text(json.dumps(envelope,sort_keys=True,separators=(',',':'))+'\n'); print(a.output)
 elif a.cmd=='grade': print(grade_run(a.run_dir,a.outcomes_csv,a.grade_root,a.grading_timestamp_utc))
if __name__=='__main__': main()
