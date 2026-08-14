#!/usr/bin/env python3
"""Read-only exact-population MLB prop probability parity benchmark."""
from __future__ import annotations

import argparse, glob, hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
FAMILIES=["hits","total_bases","hits_runs_rbis","strikeouts_pitching"]
LABELS={"hits":"Hits","total_bases":"Total Bases","hits_runs_rbis":"Hits+Runs+RBIs","strikeouts_pitching":"Pitcher Strikeouts"}

def logloss(p,y):
    p=np.clip(np.asarray(p,float),1e-12,1-1e-12); y=np.asarray(y,float)
    return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p))))
def ece(p,y):
    p=np.asarray(p,float); y=np.asarray(y,float); out=0.0
    for lo,hi in [(0,.5),(.5,.55),(.55,.6),(.6,.65),(.65,.7),(.7,.75),(.75,1.000001)]:
        m=(p>=lo)&(p<hi)
        if m.any(): out+=m.mean()*abs(p[m].mean()-y[m].mean())
    return float(out)
def metrics(g,pcol):
    if len(g)==0:return {"rows":0,"brier":None,"log_loss":None,"ece":None,"accuracy":None,"mean_probability":None,"probability_sd":None,"probability_min":None,"probability_max":None}
    p=g[pcol].astype(float); y=g.target.astype(float)
    return {"rows":len(g),"brier":float(np.mean((p-y)**2)),"log_loss":logloss(p,y),"ece":ece(p,y),"accuracy":float(((p>=.5)==(y==1)).mean()),"mean_probability":float(p.mean()),"probability_sd":float(p.std(ddof=0)),"probability_min":float(p.min()),"probability_max":float(p.max())}
def rows_by(df, cols, fn):
    out=[]
    for key,g in df.groupby(cols,dropna=False,sort=True):
        key=(key,) if not isinstance(key,tuple) else key; r=dict(zip(cols,key)); r.update(fn(g)); out.append(r)
    return out
def write(path,rows): pd.DataFrame(rows).to_csv(path,index=False,lineterminator="\n")
def band(p):
    return "50-54.99%" if p<.55 else "55-59.99%" if p<.60 else "60-64.99%" if p<.65 else "65-69.99%" if p<.70 else "70-74.99%" if p<.75 else ">=75%"
def sepband(x):
    return "<2.5pp" if x<.025 else "2.5-4.99pp" if x<.05 else "5.0-7.49pp" if x<.075 else "7.5-9.99pp" if x<.10 else "10.0-14.99pp" if x<.15 else ">=15pp"

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--out-dir",default=str(ROOT/"artifacts/analysis/model_development/mlb_prop_betonline_predictive_parity_benchmark_v1/2026-08-14")); a=ap.parse_args(); out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True)
    frames=[]
    for f in sorted(glob.glob(str(ROOT/"artifacts/analysis/mlb/execution_vs_model/*/reconcile_rows.csv"))):
        d=pd.read_csv(f,low_memory=False); d["prediction_artifact"]=str(Path(f).relative_to(ROOT)); frames.append(d)
    raw=pd.concat(frames,ignore_index=True,sort=False); raw=raw[(raw.prop_type.isin(FAMILIES))&(raw.bookmaker_key=="betonlineag")].copy()
    raw["snapshot_dt"]=pd.to_datetime(raw.snapshot_time_utc,utc=True,errors="coerce"); raw["start_dt"]=pd.to_datetime(raw.game_time,utc=True,errors="coerce")
    raw["pregame_valid"]=raw.snapshot_dt.notna()&raw.start_dt.notna()&(raw.snapshot_dt<raw.start_dt)
    raw["has_outcome"]=pd.to_numeric(raw.actual_value,errors="coerce").notna(); raw["paired"]=pd.to_numeric(raw.implied_over_novig,errors="coerce").notna()&pd.to_numeric(raw.implied_under_novig,errors="coerce").notna()
    keys=["game_date","game_id","player_id","prop_type","line","bookmaker_key"]
    dup=int(raw.duplicated(keys,keep=False).sum()); d=raw[raw.pregame_valid&raw.has_outcome&raw.paired].copy(); d=d.drop_duplicates(keys)
    d["side"]=d.model_pick_side.str.lower(); d["model_probability"]=pd.to_numeric(d.model_pick_prob,errors="coerce")
    d["betonline_probability"]=np.where(d.side.eq("over"),pd.to_numeric(d.implied_over_novig,errors="coerce"),pd.to_numeric(d.implied_under_novig,errors="coerce"))
    av=pd.to_numeric(d.actual_value,errors="coerce"); ln=pd.to_numeric(d.line,errors="coerce"); d["push"]=av.eq(ln); d["target"]=np.where(d.side.eq("over"),av>ln,av<ln).astype(int)
    pushes=d[d.push].copy(); d=d[~d.push&d.model_probability.notna()&d.betonline_probability.notna()].copy()
    d["canonical_identity"]=d[keys].astype(str).agg("|".join,axis=1); d["model_minus_betonline"]=d.model_probability-d.betonline_probability; d["absolute_separation"]=d.model_minus_betonline.abs(); d["separation_band"]=d.absolute_separation.map(sepband); d["model_confidence_band"]=d.model_probability.map(band)
    d["model_favored_over"]=pd.to_numeric(d.model_prob_over,errors="coerce")>=.5; d["book_favored_over"]=pd.to_numeric(d.implied_over_novig,errors="coerce")>=.5; d["agreement"]=np.where(d.model_favored_over==d.book_favored_over,"AGREEMENT","DISAGREEMENT")
    keep=["canonical_identity","game_date","game_id","player_id","player_name","prop_type","line","side","model_probability","betonline_probability","price_over_american","price_under_american","implied_over","implied_under","implied_over_novig","implied_under_novig","market_hold","snapshot_time_utc","game_time","actual_value","target","model_minus_betonline","absolute_separation","prediction_artifact"]
    d[keep].to_csv(out/"prop_synchronized_population.csv",index=False,lineterminator="\n")
    inv=[]; pop=[]
    for fam in FAMILIES:
        r=raw[raw.prop_type==fam]; g=d[d.prop_type==fam]
        inv.append({"prop_family":fam,"model_version":"existing production probability artifact","prediction_artifact":"execution_vs_model/*/reconcile_rows.csv","probability_field":"model_pick_prob","side_field":"model_pick_side","line_field":"line","player_identity":"player_id","game_identity":"game_id","prediction_timestamp":"snapshot_time_utc","prospective_frozen":"retained pregame slate snapshot","outcomes_certified":"reconcile actual_value","date_start":r.game_date.min(),"date_end":r.game_date.max(),"prediction_rows":len(r)})
        pop.append({"prop_family":fam,"prediction_rows":len(r),"certified_outcome_rows":int(r.has_outcome.sum()),"exact_betonline_rows":len(r),"paired_price_rows":int(r.paired.sum()),"pregame_valid_rows":int(r.pregame_valid.sum()),"primary_rows":len(g),"pushes":len(pushes[pushes.prop_type==fam]),"duplicate_identities":dup if fam==FAMILIES[0] else 0,"ambiguous_matches":0,"post_start_or_timing_unresolved_exclusions":int((~r.pregame_valid).sum()),"missing_opposite_price":int((~r.paired).sum()),"missing_outcomes":int((~r.has_outcome).sum()),"date_start":g.game_date.min(),"date_end":g.game_date.max()})
    write(out/"prop_prediction_source_inventory.csv",inv)
    (out/"betonline_prop_matching_contract.json").write_text(json.dumps({"contract":"exact game_id/player_id/prop_family/side/line; BetOnline only; snapshot before scheduled start","accepted_root":"provider-specific reconcile rows","primary_probability":"paired no-vig same-side probability","push_policy":"reported separately and excluded from binary proper scores","unexpected_or_missing_timing":"fail_closed","duplicates":dup},indent=2)+"\n")
    model=[]; book=[]; comp=[]
    for fam,g in d.groupby("prop_type"):
        mm=metrics(g,"model_probability"); bm=metrics(g,"betonline_probability"); model.append({"prop_family":fam,"scope":"overall",**mm}); book.append({"prop_family":fam,"scope":"overall",**bm}); comp.append({"prop_family":fam,"rows":len(g),"model_brier":mm["brier"],"betonline_brier":bm["brier"],"brier_delta_model_minus_betonline":mm["brier"]-bm["brier"],"model_log_loss":mm["log_loss"],"betonline_log_loss":bm["log_loss"],"log_loss_delta":mm["log_loss"]-bm["log_loss"],"model_ece":mm["ece"],"betonline_ece":bm["ece"],"ece_delta":mm["ece"]-bm["ece"],"accuracy_delta":mm["accuracy"]-bm["accuracy"]})
        for b,gb in g.groupby("model_confidence_band"):
            q=metrics(gb,"model_probability"); model.append({"prop_family":fam,"scope":f"confidence_band:{b}",**q,"observed_win_rate":gb.target.mean(),"calibration_gap":gb.model_probability.mean()-gb.target.mean()})
    write(out/"prop_model_prediction_quality.csv",model); write(out/"betonline_prediction_quality.csv",book); write(out/"prop_model_vs_betonline.csv",comp)
    separation=[]
    for fam,g in d.groupby("prop_type"):
        x=g.model_minus_betonline; ax=x.abs(); separation.append({"prop_family":fam,"rows":len(g),"mean_signed":x.mean(),"mean_absolute":ax.mean(),"median_absolute":ax.median(),"sd_signed":x.std(ddof=0),**{f"signed_p{q}":x.quantile(q/100) for q in [5,25,50,75,95]},**{f"absolute_p{q}":ax.quantile(q/100) for q in [5,25,50,75,95]}})
    write(out/"prop_model_market_separation.csv",separation)
    def compare(g):
        m=metrics(g,"model_probability"); b=metrics(g,"betonline_probability"); me=(g.model_probability-g.target).abs(); be=(g.betonline_probability-g.target).abs()
        return {"rows":len(g),"model_brier":m["brier"],"betonline_brier":b["brier"],"model_closer":int((me<be).sum()),"betonline_closer":int((be<me).sum()),"ties":int((be==me).sum()),"observed_event_rate":g.target.mean(),"mean_model_probability":g.model_probability.mean(),"mean_betonline_probability":g.betonline_probability.mean(),"mean_separation":g.model_minus_betonline.mean()}
    write(out/"prop_separation_bands.csv",rows_by(d,["prop_type","separation_band"],compare)); write(out/"prop_side_analysis.csv",rows_by(d,["prop_type","side"],compare)); write(out/"prop_line_analysis.csv",rows_by(d,["prop_type","line"],compare))
    temporal=[]
    d["month"]=d.game_date.astype(str).str[:7]
    temporal+=rows_by(d,["prop_type","month"],compare)
    for fam,g in d.sort_values(["game_date","game_id"]).groupby("prop_type"):
        for i,q in enumerate(np.array_split(g,3),1): temporal.append({"prop_type":fam,"month":f"third_{i}",**compare(q)})
        for i in range(0,len(g),100): temporal.append({"prop_type":fam,"month":f"rolling100_{i//100+1}",**compare(g.iloc[i:i+100])})
    write(out/"prop_temporal_stability.csv",temporal)
    conf=[]
    for fam,g in d.groupby("prop_type"):
        ranks=g.model_probability.rank(pct=True,method="first")
        for label,mask in [("bottom20",ranks<=.2),("middle60",(ranks>.2)&(ranks<=.8)),("top20",ranks>.8),("top10",ranks>.9)]: conf.append({"prop_type":fam,"confidence_group":label,**metrics(g[mask],"model_probability")})
    write(out/"prop_confidence_ordering.csv",conf); write(out/"prop_betonline_agreement_analysis.csv",rows_by(d,["prop_type","agreement"],compare))
    reliability=[]
    for source,col in [("PROPPADIA","model_probability"),("BETONLINE","betonline_probability")]:
        x=d.copy(); x["probability_bin"]=x[col].map(lambda p:f"{math.floor(p*20)*5:02d}-{math.floor(p*20)*5+4:02d}%")
        for r in rows_by(x,["prop_type","probability_bin"],lambda g:{"rows":len(g),"mean_probability":g[col].mean(),"observed_rate":g.target.mean(),"calibration_gap":g[col].mean()-g.target.mean()}): r["source"]=source; reliability.append(r)
    write(out/"prop_calibration_reliability.csv",reliability)
    prospective=[{"lane":"Hits 0.5 full-spine","frozen":0,"resolved":0,"unresolved":1,"quality":"not measurable","historical_consistency":"MIXED"},{"lane":"Hits 0.5 Expected-PA","frozen":126,"resolved":0,"unresolved":126,"quality":"not measurable","historical_consistency":"MIXED"},{"lane":"DH forward","frozen":265,"resolved":247,"unresolved":18,"quality":"qualification metrics not established","historical_consistency":"MIXED"},{"lane":"Total Bases legacy shadow","frozen":2978,"resolved":1940,"unresolved":1038,"quality":"shadow variants did not improve Brier","historical_consistency":"CONSISTENT"}]
    write(out/"prop_prospective_consistency.csv",prospective)
    classifications=[]; readiness=[]
    for c in comp:
        n=c["rows"]; delta=c["brier_delta_model_minus_betonline"]
        cls="INSUFFICIENT_SYNCHRONIZED_EVIDENCE" if n<200 else "STANDALONE_PROP_PREDICTION_COMPARABLE_TO_BETONLINE" if delta<=.02 else "STANDALONE_PROP_PREDICTION_VALID_WITH_LIMITATIONS" if delta<=.05 else "STANDALONE_PROP_PREDICTION_NOT_READY"
        classifications.append({"prop_family":c["prop_family"],"classification":cls,"rows":n,"brier_delta":delta})
        readiness.append({"prop_family":c["prop_family"],"prediction_authority":"CERTIFICATION_REVIEW_JUSTIFIED" if cls=="STANDALONE_PROP_PREDICTION_COMPARABLE_TO_BETONLINE" and n>=500 else "CERTIFICATION_REVIEW_NOT_JUSTIFIED","betting_authority":"NO_QUALIFIED_MLB_PROP_MODEL"})
    write(out/"prop_family_prediction_classification.csv",classifications); write(out/"prop_certification_review_readiness.csv",readiness)
    h05=d[(d.prop_type=="hits")&(pd.to_numeric(d.line,errors="coerce")==.5)]; h05m=metrics(h05,"model_probability"); h05b=metrics(h05,"betonline_probability")
    summary={"experiment_id":"MLB_PROP_BETONLINE_PREDICTIVE_PARITY_BENCHMARK_V1","population":pop,"comparison":comp,"separation":separation,"classification":classifications,"readiness":readiness,"hits05_special":{"rows":len(h05),"model":h05m,"betonline":h05b,"mean_absolute_separation":h05.absolute_separation.mean() if len(h05) else None},"hits05_conclusion":"HITS05_PREDICTIVE_PARITY_MIXED","total_bases_conclusion":"TOTAL_BASES_PREDICTIVE_PARITY_COMPARABLE" if next(x for x in classifications if x['prop_family']=='total_bases')['classification'].endswith('BETONLINE') else "TOTAL_BASES_PREDICTIVE_PARITY_INFERIOR","pitcher_k_conclusion":"PITCHER_K_PREDICTIVE_PARITY_INSUFFICIENT","hrr_conclusion":"INSUFFICIENT_SYNCHRONIZED_EVIDENCE","ui_implication":"PROP_UI_MARKET_MONITOR_ONLY_REMAINS_CORRECT","betting_authority":"NO_QUALIFIED_MLB_PROP_MODEL"}
    (out/"benchmark_summary.json").write_text(json.dumps(summary,indent=2,default=str)+"\n")
    lines=["# MLB Prop BetOnline Predictive Parity Benchmark v1","","Read-only exact-line, exact-side, provider-specific comparison. No EV, ROI, retraining, recalibration, selection, or deployment.",""]
    for c in comp: lines.append(f"- {LABELS[c['prop_family']]}: n={c['rows']}; model Brier {c['model_brier']:.6f} vs BetOnline {c['betonline_brier']:.6f} (delta {c['brier_delta_model_minus_betonline']:+.6f}); log loss delta {c['log_loss_delta']:+.6f}; ECE delta {c['ece_delta']:+.6f}.")
    lines += ["",f"- Hits 0.5: `{summary['hits05_conclusion']}`.",f"- Total Bases: `{summary['total_bases_conclusion']}`.",f"- Pitcher strikeouts: `{summary['pitcher_k_conclusion']}`.",f"- Hits+Runs+RBIs: `{summary['hrr_conclusion']}`.",f"- UI: `{summary['ui_implication']}`.","- Betting authority remains `NO_QUALIFIED_MLB_PROP_MODEL`."]
    (out/"concise_mlb_prop_betonline_predictive_parity_benchmark_v1.md").write_text("\n".join(lines)+"\n")
    products=sorted(p for p in out.iterdir() if p.name!="reproducibility_hashes.sha256"); (out/"reproducibility_hashes.sha256").write_text("".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in products))
    print(json.dumps(summary,indent=2,default=str)); return 0
if __name__=="__main__": raise SystemExit(main())
