#!/usr/bin/env python3
"""File-only CLI for immutable cold-start SOG prediction and grading fixtures."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .core import build_predictions, grade_predictions, sha256_file


def manifest(directory: Path) -> None:
    files=sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (directory/"SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))


def main() -> int:
    ap=argparse.ArgumentParser();sub=ap.add_subparsers(dest="command",required=True)
    p=sub.add_parser("predict");p.add_argument("--features",type=Path,required=True);p.add_argument("--slate-date",required=True);p.add_argument("--phase",choices=["MIDDAY","FINAL_PREGAME"],required=True);p.add_argument("--prediction-timestamp-utc",required=True);p.add_argument("--input-cutoff-utc",required=True);p.add_argument("--output",type=Path,required=True)
    g=sub.add_parser("grade");g.add_argument("--prediction-run",type=Path,required=True);g.add_argument("--outcomes",type=Path,required=True);g.add_argument("--grading-timestamp-utc",required=True);g.add_argument("--output",type=Path,required=True)
    a=ap.parse_args()
    if a.output.exists(): raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    a.output.mkdir(parents=True,exist_ok=False)
    if a.command=="predict":
        pred,features,excluded=build_predictions(pd.read_csv(a.features),slate_date=a.slate_date,phase=a.phase,prediction_timestamp_utc=a.prediction_timestamp_utc,input_cutoff_utc=a.input_cutoff_utc)
        pred.to_csv(a.output/"immutable_predictions.csv",index=False);features.to_csv(a.output/"prediction_features.csv",index=False);excluded.to_csv(a.output/"excluded_players.csv",index=False)
        meta={"status":"COMPLETE","slate_date":a.slate_date,"phase":a.phase,"prediction_timestamp_utc":a.prediction_timestamp_utc,"input_cutoff_utc":a.input_cutoff_utc,"prediction_rows":len(pred),"admitted_players":int(features.player_id.nunique()) if not features.empty else 0,"prediction_arms":sorted(features.contract_arm.unique().tolist()) if not features.empty else [],"excluded_players":len(excluded),"market_requests":0,"market_attachment":"UNAVAILABLE_NOT_REQUIRED"}
    else:
        run=a.prediction_run
        for line in (run/"SHA256SUMS").read_text().splitlines():
            expected,name=line.split("  ",1)
            if sha256_file(run/name)!=expected: raise RuntimeError("PREDICTION_RUN_MANIFEST_FAILURE")
        graded=grade_predictions(pd.read_csv(run/"immutable_predictions.csv"),pd.read_csv(a.outcomes),grading_timestamp_utc=a.grading_timestamp_utc)
        graded.to_csv(a.output/"canonical_outcomes.csv",index=False)
        meta={"status":"COMPLETE","source_prediction_run":str(run),"grading_timestamp_utc":a.grading_timestamp_utc,"rows":len(graded),"settled":int(graded.grading_state.isin(["WIN","LOSS"]).sum()),"market_requests":0}
    (a.output/"run_metadata.json").write_text(json.dumps(meta,indent=2,sort_keys=True)+"\n");manifest(a.output);print(a.output);return 0

if __name__=="__main__": raise SystemExit(main())
