"""Explicit-only CLI for fetching and archiving NHL Saves quotes."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from backend.nhl.odds_regions import NHL_ODDS_REGIONS_CSV

from .core import capture_run


def fetch(*, api_key: str, output: Path, regions: str=NHL_ODDS_REGIONS_CSV) -> Path:
    if output.exists(): raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    import requests
    response=requests.get("https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds",params={"apiKey":api_key,"regions":regions,"markets":"player_total_saves","oddsFormat":"american","dateFormat":"iso"},timeout=45)
    response.raise_for_status();output.parent.mkdir(parents=True,exist_ok=True)
    envelope={"capture_timestamp_utc":datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),"provider_response":response.json(),"request_metadata":{"provider":"THE_ODDS_API","sport":"icehockey_nhl","markets":["player_total_saves"],"regions":regions,"remaining_requests":response.headers.get("x-requests-remaining"),"used_requests":response.headers.get("x-requests-used")}}
    output.write_text(json.dumps(envelope,sort_keys=True,separators=(",",":"))+"\n");return output


def main() -> int:
    parser=argparse.ArgumentParser();sub=parser.add_subparsers(dest="command",required=True)
    get=sub.add_parser("fetch");get.add_argument("--api-key",required=True);get.add_argument("--output",type=Path,required=True);get.add_argument("--regions",default=NHL_ODDS_REGIONS_CSV)
    archive=sub.add_parser("archive")
    archive.add_argument("--payload-json",type=Path,required=True);archive.add_argument("--games-csv",type=Path,required=True);archive.add_argument("--goalies-csv",type=Path,required=True);archive.add_argument("--parent-manifest",type=Path,required=True);archive.add_argument("--output-root",type=Path,required=True)
    archive.add_argument("--slate-date",required=True);archive.add_argument("--run-timestamp-utc",required=True);archive.add_argument("--run-type",choices=["MIDDAY","FINAL_PREGAME"],required=True);archive.add_argument("--source",default="THE_ODDS_API")
    args=parser.parse_args();values=vars(args);command=values.pop("command")
    print(fetch(**values) if command=="fetch" else capture_run(**values));return 0


if __name__ == "__main__":
    raise SystemExit(main())
