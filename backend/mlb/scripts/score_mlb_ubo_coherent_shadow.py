#!/usr/bin/env python3
"""Score certified pregame feature rows into the frozen coherent UBO shadow ledger."""
from __future__ import annotations
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
import joblib,numpy as np,pandas as pd

from backend.mlb.scripts import run_mlb_ubo_coherent_revision as rev
from backend.mlb.scripts import run_mlb_unified_batter_outcome_v1 as base

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/"artifacts/analysis/model_development/mlb_unified_batter_outcome_v1_coherent_revision/2026-07-23"
MODEL=OUT/"final_artifacts"

def joint_states(pa_prob,terminal):
    one=[(int(base.HIT_VALUE[i]),int(base.TB_VALUE[i]),int(base.HR_VALUE[i]),float(terminal[i])) for i in range(8)]
    mixed={}
    for n,pn in enumerate(pa_prob):
        states={(0,0,0):1.0}
        for _ in range(n):
            nxt={}
            for (h,t,r),p in states.items():
                for dh,dt,dr,q in one:nxt[(h+dh,t+dt,r+dr)]=nxt.get((h+dh,t+dt,r+dr),0)+p*q
            states=nxt
        for state,p in states.items():mixed[state]=mixed.get(state,0)+float(pn)*p
    return json.dumps({f"{h}|{t}|{r}":p for (h,t,r),p in sorted(mixed.items())},separators=(",",":"))

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--feature-parquet",type=Path,required=True)
    ap.add_argument("--prediction-timestamp-utc",required=True);a=ap.parse_args()
    ts=pd.Timestamp(a.prediction_timestamp_utc)
    if ts.tzinfo is None:raise SystemExit("prediction timestamp must be timezone-aware")
    d=pd.read_parquet(a.feature_parquet)
    required={"game_pk","game_date","batter_mlb_id","official_start_time","starting_hitter_certified","source_lineage"}
    missing=required-set(d)
    if missing:raise SystemExit("missing certified input columns: "+",".join(sorted(missing)))
    if not d.starting_hitter_certified.fillna(False).all():raise SystemExit("uncertified starting hitter row")
    starts=pd.to_datetime(d.official_start_time,utc=True)
    if not (ts<starts).all():raise SystemExit("retroactive or post-start prediction prohibited")
    identity=json.loads((MODEL/"model_identity_manifest.json").read_text())
    features=identity["feature_order"]
    absent=set(features+rev.OPP+[f"h_career_rate_{i}" for i in range(8)])-set(d)
    if absent:raise SystemExit("missing frozen feature columns: "+",".join(sorted(absent)))
    for c in ["actual_pa","actual_hits","actual_tb","actual_hr"]:d[c]=0
    d["split"]="shadow"
    pa_model=joblib.load(MODEL/"pa_opportunity_model.joblib");terminal=joblib.load(MODEL/"coherent_terminal_model.joblib")
    pred,pp,op=rev.routed_predictions(d,pa_model,terminal,features,float(identity["temperature"]),np.zeros(8),"COHERENT_SHADOW")
    model_hash=hashlib.sha256((MODEL/"coherent_terminal_model.joblib").read_bytes()).hexdigest()
    rows=[]
    for i,r in pred.iterrows():
        rows.append({"slate_date":str(pd.Timestamp(r.game_date).date()),"game_pk":r.game_pk,"batter_mlb_id":r.batter_mlb_id,
          "route":r.route,"strict_prior_pa":r.strict_prior_pa,"p_pa_json":r.p_pa_json,
          "terminal_probability_json":r.terminal_probability_json,"joint_distribution_json":joint_states(pp[i],op[i]),
          "p_hits0":r.p_h0,"p_hits2plus":1-r.p_h0-r.p_h1,"p_tb0":r.p_tb0,"p_tb1":r.p_tb1,
          "p_tb2":r.p_tb2,"p_tb3":r.p_tb3,"p_tb4plus":r.p_tb4plus,"p_hr1plus":r.p_hr1plus,
          "model_hash":model_hash,"prediction_timestamp_utc":ts.isoformat(),"feature_completeness":"PASS",
          "source_lineage":d.iloc[i].source_lineage,
          "label":"UBO COHERENT-JOINT REVISION SHADOW — NO PRODUCTION OR WAGER EFFECT"})
    new=pd.DataFrame(rows);ledger=OUT/"immutable_shadow_predictions.csv";old=pd.read_csv(ledger)
    combined=pd.concat([old,new],ignore_index=True)
    if combined.duplicated(["slate_date","game_pk","batter_mlb_id"]).any():raise SystemExit("duplicate immutable shadow identity")
    combined.to_csv(ledger,index=False)
    print(json.dumps({"scored_rows":len(new),"ledger_rows":len(combined),"production_effect":"NONE"},indent=2))

if __name__=="__main__":main()
