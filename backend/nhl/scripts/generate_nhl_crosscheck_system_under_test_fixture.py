#!/usr/bin/env python3
"""Generate system-under-test candidate outputs; not an independent verifier."""
from __future__ import annotations
import argparse,json
from pathlib import Path
import pandas as pd
from backend.nhl.sog_candidate_lineage.core import effective_config,evaluate

def main():
 ap=argparse.ArgumentParser();ap.add_argument("--output-dir",type=Path,required=True);a=ap.parse_args();o=a.output_dir
 if o.exists():raise RuntimeError("GOVERNED_PACKAGE_EXISTS_ABORT")
 o.mkdir(parents=True)
 seg={f"{s}:{l}":{"min_ev":.05,"min_gap":.03,"train_wilson_lb":.60} for s in ["over","under"] for l in ["1.5","2.5","3.5"]};cfg=effective_config(seg,{"max_per_slate":3})
 rows=[]
 specs=[("pass",101,1.5,.75),("duplicate",101,2.5,.70),("weak",102,2.5,.51),("badline",103,4.5,.80),("missing",104,3.5,.80),("price",105,3.5,.70),("cap1",106,1.5,.72),("cap2",107,2.5,.71),("under",108,1.5,.25),("tiea",109,2.5,.72),("tieb",110,2.5,.72)]
 for ident,pid,line,p in specs:rows.append({"canonical_season":2026,"slate_date":"2026-10-10","run_id":"sut_fixture","prediction_identity":ident,"game_id":1 if pid<107 else 2,"player_id":pid,"prop_type":"shots_on_goal","line":line,"model_version":"baseline_v1","p_over":p})
 quotes=[]
 for ident,pid,line,p in specs:
  if ident=="missing":continue
  op,up=(140,-160) if ident=="price" else (110,-125)
  for book,delta in [("book_a",0),("book_b",5)]:
   for side,price in [("OVER",op+delta),("UNDER",up-delta)]:quotes.append({"run_id":"sut_fixture","game_id":1 if pid<107 else 2,"player_id":pid,"line":line,"side":side,"raw_price":price,"sportsbook":book,"raw_payload_sha256":"payload_fixture","canonical_prop_type":"shots_on_goal","quote_qualification_status":"PREGAME_QUALIFIED_PROVIDER_TIMESTAMP"})
 pred=pd.DataFrame(rows);q=pd.DataFrame(quotes);manual=pd.DataFrame([{"prediction_identity":"pass","side":"OVER","manual_override_status":"MANUAL_REMOVE","operator_id":"fixture","reason":"isolation","timestamp_utc":"2026-10-10T16:02:00Z"}]);final,ledger,man=evaluate(pred,q,cfg,"2026-10-10T16:00:00Z",manual)
 pred.to_csv(o/"predictions.csv",index=False);q.to_csv(o/"quotes.csv",index=False);final.to_csv(o/"production_candidate_summary.csv",index=False);ledger.to_csv(o/"production_rule_ledger.csv",index=False);man.to_csv(o/"manual.csv",index=False);(o/"config.json").write_text(json.dumps(cfg,indent=2,sort_keys=True)+"\n")
if __name__=="__main__":main()
