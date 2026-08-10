#!/usr/bin/env python3
"""Explicit/manual CLI; no scheduler integration and no legacy-path writes."""
from __future__ import annotations
import argparse,json,urllib.parse,urllib.request
from datetime import datetime,timezone
from pathlib import Path
from .core import capture_run

def main() -> None:
    ap=argparse.ArgumentParser(); sub=ap.add_subparsers(dest="command",required=True)
    fetch=sub.add_parser("fetch"); fetch.add_argument("--api-key",required=True); fetch.add_argument("--output",type=Path,required=True); fetch.add_argument("--regions",default="us,us2")
    run=sub.add_parser("run"); run.add_argument("--payload-json",type=Path,required=True); run.add_argument("--games-csv",type=Path,required=True); run.add_argument("--players-csv",type=Path,required=True); run.add_argument("--output-root",type=Path,default=Path("backend/nhl/exports/sog_shadow")); run.add_argument("--slate-date",required=True); run.add_argument("--run-timestamp-utc",required=True); run.add_argument("--run-type",choices=["MIDDAY","FINAL_PREGAME"],required=True)
    args=ap.parse_args()
    if args.command=="fetch":
        if args.output.exists(): raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
        params={"regions":args.regions,"markets":"player_shots_on_goal,player_shots_on_goal_alternate","oddsFormat":"american","dateFormat":"iso","apiKey":args.api_key}
        url="https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds?"+urllib.parse.urlencode(params)
        request=urllib.request.Request(url,headers={"User-Agent":"proppadia-nhl-sog-quote-capture/1.0"}); data=urllib.request.urlopen(request,timeout=30).read(); provider=json.loads(data)
        captured=datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00","Z")
        envelope={"capture_timestamp_utc":captured,"source_acquisition_timestamp_utc":captured,"provider":"THE_ODDS_API","request_metadata":{"endpoint":"icehockey_nhl/odds","regions":args.regions,"markets":["player_shots_on_goal","player_shots_on_goal_alternate"],"odds_format":"american","date_format":"iso"},"provider_response":provider}
        args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(envelope,sort_keys=True,separators=(",",":"))+"\n"); print(args.output)
    else:
        print(capture_run(payload_json=args.payload_json,games_csv=args.games_csv,players_csv=args.players_csv,output_root=args.output_root,slate_date=args.slate_date,run_timestamp_utc=args.run_timestamp_utc,run_type=args.run_type))
if __name__=="__main__": main()
