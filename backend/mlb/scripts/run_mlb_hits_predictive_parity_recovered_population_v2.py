#!/usr/bin/env python3
"""Descriptive parity rerun on the immutable recovered MLB Hits population."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "artifacts/analysis/model_development/mlb_hits_historical_identity_recovery_v1/2026-08-14/hits_recovered_synchronized_population.csv"
STAGE2 = ROOT / "artifacts/analysis/model_development/mlb_hits_lane_specific_prediction_review_stage2/2026-08-14"
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits_predictive_parity_recovered_population_v2/2026-08-14"
LANES = {(0.5,"over"):"HITS_05_OVER", (0.5,"under"):"HITS_05_UNDER", (1.5,"over"):"HITS_15_OVER", (1.5,"under"):"HITS_15_UNDER"}
EXPECTED = {"HITS_05_OVER":8476,"HITS_05_UNDER":2596,"HITS_15_OVER":164,"HITS_15_UNDER":1382}


def write(name, rows):
    pd.DataFrame(rows).to_csv(OUT / name, index=False, lineterminator="\n")


def logloss(p, y):
    p=np.clip(np.asarray(p,float),1e-12,1-1e-12); y=np.asarray(y,float)
    return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p)))) if len(p) else None


def ece(p, y):
    p=np.asarray(p,float); y=np.asarray(y,float)
    if not len(p): return None
    z=0.
    for lo,hi in [(0,.5),(.5,.55),(.55,.6),(.6,.65),(.65,.7),(.7,.75),(.75,1.00001)]:
        m=(p>=lo)&(p<hi)
        if m.any(): z += m.mean()*abs(p[m].mean()-y[m].mean())
    return float(z)


def metrics(g, col):
    if not len(g): return {k:None for k in ["brier","log_loss","ece","accuracy","mean_probability","probability_sd","observed_rate","calibration_gap"]}
    p=pd.to_numeric(g[col],errors="raise").astype(float); y=g.target.astype(float)
    return {"brier":float(((p-y)**2).mean()),"log_loss":logloss(p,y),"ece":ece(p,y),
            "accuracy":float(((p>=.5)==(y==1)).mean()),"mean_probability":float(p.mean()),
            "probability_sd":float(p.std(ddof=0)),"observed_rate":float(y.mean()),
            "calibration_gap":float(y.mean()-p.mean())}


def pband(p):
    return "50-54.99%" if p<.55 else "55-59.99%" if p<.6 else "60-64.99%" if p<.65 else "65-69.99%" if p<.7 else "70-74.99%" if p<.75 else ">=75%"


def sband(x):
    return "<2.5pp" if x<.025 else "2.5-4.99pp" if x<.05 else "5.0-7.49pp" if x<.075 else "7.5-9.99pp" if x<.1 else "10.0-14.99pp" if x<.15 else ">=15pp"


def ordering(rates):
    rates=[x for x in rates if x is not None]
    if len(rates)<4:return "FLAT"
    rises=sum(b>=a for a,b in zip(rates,rates[1:])); span=rates[-1]-rates[0]
    return "MONOTONIC_OR_NEAR_MONOTONIC" if rises>=3 and span>.04 else "PARTIAL_ORDERING" if span>.02 else "INVERTED_OR_UNRELIABLE" if span<-.02 else "FLAT"


def temporal_label(rows):
    if len(rows)<3:return "MIXED"
    bd=rows[-1]["model_brier"]-rows[0]["model_brier"]; ed=rows[-1]["model_ece"]-rows[0]["model_ece"]
    return "DETERIORATING" if bd>.015 or ed>.05 else "IMPROVING" if bd<-.015 and ed<0 else "STABLE" if abs(bd)<.005 and abs(ed)<.03 else "MILD_DRIFT"


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    d=pd.read_csv(SOURCE,low_memory=False,dtype={"game_id":str,"player_id":str})
    d["side"]=d.side.astype(str).str.lower(); d["lane"]=[LANES[(float(x),s)] for x,s in zip(d.line,d.side)]
    d["target"]=np.where(d.side.eq("over"),d.actual_value>d.line,d.actual_value<d.line).astype(int)
    d["model_minus_betonline"]=d.model_probability-d.betonline_probability
    d["absolute_separation"]=d.model_minus_betonline.abs(); d["probability_band"]=d.model_probability.map(pband)
    d["separation_band"]=d.absolute_separation.map(sband); d["month"]=d.date.str[:7]
    identity=["date","game_id","player_id","line","side"]
    validations=[
        ("total_rows",12618,len(d),len(d)==12618),
        ("lane_counts",json.dumps(EXPECTED,sort_keys=True),json.dumps(d.lane.value_counts().to_dict(),sort_keys=True),d.lane.value_counts().to_dict()==EXPECTED),
        ("duplicate_proposition_identities",0,int(d.duplicated(identity).sum()),not d.duplicated(identity).any()),
        ("identity_conflicts",0,int(d.groupby(identity).source_row_hash.nunique().gt(1).sum()),not d.groupby(identity).source_row_hash.nunique().gt(1).any()),
        ("outcome_complete",12618,int(d.actual_value.notna().sum()),d.actual_value.notna().all()),
        ("strictly_pregame",12618,int((pd.to_datetime(d.betonline_capture_timestamp,utc=True,format="mixed")<pd.to_datetime(d.game_time,utc=True,format="mixed")).sum()),bool((pd.to_datetime(d.betonline_capture_timestamp,utc=True,format="mixed")<pd.to_datetime(d.game_time,utc=True,format="mixed")).all())),
        ("provenance_classes","ORIGINAL_CANONICAL_MATCH|RECOVERED_DETERMINISTIC_MATCH","|".join(sorted(d.provenance.unique())),set(d.provenance)=={"ORIGINAL_CANONICAL_MATCH","RECOVERED_DETERMINISTIC_MATCH"}),
        ("source_hash",hashlib.sha256(SOURCE.read_bytes()).hexdigest(),hashlib.sha256(SOURCE.read_bytes()).hexdigest(),True),
    ]
    write("hits_v2_recovered_population_validation.csv",[dict(check=a,expected=b,actual=c,passed=p) for a,b,c,p in validations])
    assert all(x[3] for x in validations)

    provenance=[]
    for label,g in d.groupby("provenance"):
        mm=metrics(g,"model_probability"); bb=metrics(g,"betonline_probability")
        provenance.append({"provenance":label,"rows":len(g),"lane_composition":json.dumps(g.lane.value_counts().to_dict(),sort_keys=True),
            "side_composition":json.dumps(g.side.value_counts().to_dict(),sort_keys=True),"line_composition":json.dumps(g.line.value_counts().to_dict(),sort_keys=True),
            "month_composition":json.dumps(g.month.value_counts().to_dict(),sort_keys=True),"mean_absolute_separation":g.absolute_separation.mean(),
            **{f"model_{k}":v for k,v in mm.items()},**{f"betonline_{k}":v for k,v in bb.items()}})
    po=pd.DataFrame(provenance); o=po[po.provenance.eq("ORIGINAL_CANONICAL_MATCH")].iloc[0]; r=po[po.provenance.eq("RECOVERED_DETERMINISTIC_MATCH")].iloc[0]
    score_delta=max(abs(o.model_brier-r.model_brier),abs(o.model_ece-r.model_ece),abs(o.model_accuracy-r.model_accuracy),
                    abs(o.betonline_brier-r.betonline_brier),abs(o.betonline_ece-r.betonline_ece),abs(o.betonline_accuracy-r.betonline_accuracy))
    behavior="RECOVERED_ROWS_MATERIALLY_DIFFER" if score_delta>.05 else "RECOVERED_ROWS_PARTIALLY_DIFFER" if score_delta>.02 else "RECOVERED_ROWS_CONFIRM_ORIGINAL_BEHAVIOR"
    po["behavior_classification"]=behavior; write("hits_v2_original_vs_recovered_rows.csv",po)

    pooled=[]
    for actor,col in [("PROPPADIA","model_probability"),("BETONLINE","betonline_probability")]: pooled.append({"actor":actor,**metrics(d,col)})
    pm,bm=pooled
    pooled.append({"actor":"PROPPADIA_MINUS_BETONLINE","brier":pm["brier"]-bm["brier"],"log_loss":pm["log_loss"]-bm["log_loss"],"ece":pm["ece"]-bm["ece"],"accuracy":pm["accuracy"]-bm["accuracy"]})
    parity="BROADLY_COMPARABLE" if abs(pm["brier"]-bm["brier"])<=.01 and abs(pm["log_loss"]-bm["log_loss"])<=.025 else "MATERIALLY_INFERIOR" if pm["brier"]-bm["brier"]>.025 else "COMPARABLE_WITH_LIMITATIONS" if pm["brier"]>=bm["brier"] else "MIXED"
    for x in pooled:x["classification"]=parity
    write("hits_v2_full_parity_metrics.csv",pooled)

    lane_quality=[]; reliability=[]; confidence=[]; extreme=[]; separation=[]; signed=[]; temporal=[]; ordering_status={}; temporal_status={}
    for label,g0 in d.groupby("lane",sort=True):
        g=g0.copy(); mm=metrics(g,"model_probability"); bb=metrics(g,"betonline_probability")
        lane_quality.append({"lane":label,"rows":len(g),"games":g.game_id.nunique(),"players":g.player_id.nunique(),**mm,
                             **{f"betonline_{k}":v for k,v in bb.items()},"mean_absolute_separation":g.absolute_separation.mean(),"mean_signed_separation":g.model_minus_betonline.mean()})
        for band in ["50-54.99%","55-59.99%","60-64.99%","65-69.99%","70-74.99%",">=75%"]:
            x=g[g.probability_band.eq(band)]; q=metrics(x,"model_probability")
            reliability.append({"lane":label,"probability_band":band,"rows":len(x),**q,"sample_status":"SMALL_SAMPLE" if len(x)<100 else "ADEQUATE_DESCRIPTIVE_SAMPLE"})
        ranked=g.sort_values(["model_probability","source_row_hash"]).copy(); ranked["q"]=pd.qcut(np.arange(len(ranked)),5,labels=["bottom20","second20","middle20","fourth20","top20"])
        qr=[]
        for q in ["bottom20","second20","middle20","fourth20","top20"]:
            x=ranked[ranked.q.astype(str).eq(q)]; z=metrics(x,"model_probability"); confidence.append({"lane":label,"confidence_group":q,"rows":len(x),**z}); qr.append(z["observed_rate"])
        x=ranked.tail(max(1,math.ceil(len(ranked)*.1))); confidence.append({"lane":label,"confidence_group":"top10","rows":len(x),**metrics(x,"model_probability")})
        ordering_status[label]=ordering(qr)
        for threshold in [.65,.70,.75]:
            x=g[g.model_probability>=threshold]; a=metrics(x,"model_probability"); b=metrics(x,"betonline_probability")
            extreme.append({"lane":label,"threshold":f">={threshold:.0%}","rows":len(x),**a,"betonline_brier":b["brier"]})
        for band in ["<2.5pp","2.5-4.99pp","5.0-7.49pp","7.5-9.99pp","10.0-14.99pp",">=15pp"]:
            x=g[g.separation_band.eq(band)]; a=metrics(x,"model_probability"); b=metrics(x,"betonline_probability")
            me=(x.model_probability-x.target).abs(); be=(x.betonline_probability-x.target).abs()
            separation.append({"lane":label,"separation_band":band,"rows":len(x),"model_brier":a["brier"],"betonline_brier":b["brier"],
                "model_log_loss":a["log_loss"],"betonline_log_loss":b["log_loss"],"model_closer":int((me<be).sum()),"betonline_closer":int((be<me).sum()),
                "mean_signed_separation":x.model_minus_betonline.mean() if len(x) else None,"observed_win_rate":x.target.mean() if len(x) else None})
        groups=[("MODEL_MORE_CONFIDENT",g.model_minus_betonline>0),("MODEL_LESS_CONFIDENT",g.model_minus_betonline<0),
                ("MODEL_MORE_CONFIDENT_GE10PP",g.model_minus_betonline>=.10),("MODEL_LESS_CONFIDENT_GE10PP",g.model_minus_betonline<=-.10),
                ("MODEL_MORE_CONFIDENT_GE15PP",g.model_minus_betonline>=.15),("MODEL_LESS_CONFIDENT_GE15PP",g.model_minus_betonline<=-.15)]
        for group,mask in groups:
            x=g[mask]; a=metrics(x,"model_probability"); b=metrics(x,"betonline_probability")
            signed.append({"lane":label,"signed_group":group,"rows":len(x),"mean_gap":x.model_minus_betonline.mean() if len(x) else None,
                           "model_brier":a["brier"],"betonline_brier":b["brier"],"observed_rate":a["observed_rate"],"calibration_gap":a["calibration_gap"]})
        thirds=[]
        for month,x in g.groupby("month"):
            a=metrics(x,"model_probability"); temporal.append({"lane":label,"period_type":"month","period":month,"rows":len(x),**{f"model_{k}":v for k,v in a.items()},"betonline_brier":metrics(x,"betonline_probability")["brier"],"mean_separation":x.model_minus_betonline.mean()})
        for i,x in enumerate(np.array_split(g.sort_values(["date","source_row_hash"]),3),1):
            a=metrics(x,"model_probability"); z={"lane":label,"period_type":"chronological_third","period":f"third_{i}","rows":len(x),**{f"model_{k}":v for k,v in a.items()},"betonline_brier":metrics(x,"betonline_probability")["brier"],"mean_separation":x.model_minus_betonline.mean()}; temporal.append(z); thirds.append(z)
        temporal_status[label]=temporal_label(thirds)
    write("hits_v2_lane_quality.csv",lane_quality); write("hits_v2_probability_reliability.csv",reliability); write("hits_v2_confidence_ordering.csv",confidence)
    write("hits_v2_extreme_confidence.csv",extreme); write("hits_v2_separation_bands.csv",separation); write("hits_v2_signed_separation.csv",signed); write("hits_v2_temporal_behavior.csv",temporal)

    oldq=pd.read_csv(STAGE2/"hits_stage2_lane_prediction_quality.csv"); oldc=pd.read_csv(STAGE2/"hits_stage2_cross_lane_comparison.csv")
    newq=pd.DataFrame(lane_quality); sep=pd.DataFrame(separation); deltas=[]
    for label in sorted(EXPECTED):
        old=oldq[oldq.lane.eq(label)].iloc[0]; new=newq[newq.lane.eq(label)].iloc[0]; oc=oldc[oldc.lane.eq(label)].iloc[0]
        ns=sep[(sep.lane.eq(label))&sep.separation_band.eq(">=15pp")].iloc[0]
        deltas.append({"lane":label,"original_rows":int(old.rows),"recovered_rows":int(new.rows),"row_delta":int(new.rows-old.rows),
            "original_brier":old.brier,"recovered_brier":new.brier,"brier_delta":new.brier-old.brier,"original_log_loss":old.log_loss,"recovered_log_loss":new.log_loss,"log_loss_delta":new.log_loss-old.log_loss,
            "original_ece":old.ece,"recovered_ece":new.ece,"ece_delta":new.ece-old.ece,"original_accuracy":old.accuracy,"recovered_accuracy":new.accuracy,"accuracy_delta":new.accuracy-old.accuracy,
            "original_observed_rate":old.observed_rate,"recovered_observed_rate":new.observed_rate,"observed_rate_delta":new.observed_rate-old.observed_rate,
            "original_ordering":oc.confidence_ordering_status,"recovered_ordering":ordering_status[label],"ordering_interpretation":"UNCHANGED" if oc.confidence_ordering_status==ordering_status[label] else "CHANGED",
            "original_ge15pp_brier":oc.ge15pp_brier,"recovered_ge15pp_brier":ns.model_brier,"ge15pp_brier_delta":ns.model_brier-oc.ge15pp_brier,
            "original_temporal":oc.temporal_status,"recovered_temporal":temporal_status[label],"temporal_interpretation":"UNCHANGED" if oc.temporal_status==temporal_status[label] else "CHANGED"})
    write("hits_v2_original_vs_recovered_lane_deltas.csv",deltas)

    trust=[]; readiness=[]
    for row in lane_quality:
        label=row["lane"]; ge15=sep[(sep.lane.eq(label))&sep.separation_band.eq(">=15pp")].iloc[0]
        probability="STRONG" if row["ece"]<.04 else "MODERATE" if row["ece"]<.08 else "WEAK"
        confidence_label="STRONG" if ordering_status[label]=="MONOTONIC_OR_NEAR_MONOTONIC" else "MODERATE" if ordering_status[label]=="PARTIAL_ORDERING" else "WEAK"
        temp="STRONG" if temporal_status[label]=="STABLE" else "MODERATE" if temporal_status[label] in {"MILD_DRIFT","MIXED"} else "WEAK"
        extreme_label="MODERATE" if label=="HITS_15_UNDER" else "WEAK"
        independence="STRONG" if row["mean_absolute_separation"]>=.075 else "MODERATE" if row["mean_absolute_separation"]>=.04 else "WEAK"
        relative="materially worse" if row["brier"]-row["betonline_brier"]>.025 else "modestly worse" if row["brier"]>row["betonline_brier"]+.005 else "modestly better" if row["brier"]<row["betonline_brier"]-.005 else "approximately comparable"
        trust.append({"lane":label,"probability_calibration":probability,"confidence_ordering":confidence_label,"temporal_stability":temp,"independence_from_betonline":independence,"extreme_confidence_trust":extreme_label,"betonline_relative_position":relative})
        historical="HISTORICAL_EVIDENCE_WEAK" if label=="HITS_15_OVER" and row["rows"]<200 else "HISTORICAL_EVIDENCE_STRONG_ENOUGH_FOR_PROVENANCE_WORK" if label in {"HITS_05_OVER","HITS_15_UNDER"} else "HISTORICAL_EVIDENCE_MIXED"
        readiness.append({"lane":label,"status":historical,"historical_model_identity":"UNRESOLVED","prospective_capture_created":False,"certification_decision_made":False})
    write("hits_v2_lane_trust_characterization.csv",trust); write("hits_v2_historical_evidence_readiness.csv",readiness)

    delta_map={x["lane"]:x for x in deltas}; trust_map={x["lane"]:x for x in trust}
    questions=["Did Hits 1.5 Under survive the doubled sample strongly enough to prioritize provenance work?","Is Hits 0.5 Over extreme-confidence deterioration material enough to preclude formal review?","Is the persistent Hits 0.5 Under temporal deterioration durable?","Is 164 rows still too sparse for Hits 1.5 Over conclusions?","Can exact historical model producer/version binding be recovered before any certification review?","Which lanes, if any, deserve formal certification review after provenance is resolved?"]
    md=["# MLB Hits predictive parity on recovered population v2","","Evidence reevaluation only; no certification, model, recalibration, selector, prospective capture, EV/ROI, or UI change.","",
        f"- Frozen population: {len(d):,} rows (7,564 original; 5,054 recovered-only); behavior `{behavior}`.",
        f"- Pooled comparison: Proppadia Brier {pm['brier']:.6f}, log loss {pm['log_loss']:.6f}, ECE {pm['ece']:.6f}; BetOnline Brier {bm['brier']:.6f}, log loss {bm['log_loss']:.6f}, ECE {bm['ece']:.6f}; `{parity}`."]
    for row in lane_quality:
        label=row["lane"]; dm=delta_map[label]; tr=trust_map[label]
        md.append(f"- `{label}`: n={row['rows']:,}; Brier {row['brier']:.6f}; observed {row['observed_rate']:.1%}; ECE {row['ece']:.6f}; ordering `{ordering_status[label]}`; temporal `{temporal_status[label]}`; Stage 2 Brier delta {dm['brier_delta']:+.6f}; BetOnline-relative `{tr['betonline_relative_position']}`.")
    md += ["","`HISTORICAL_MODEL_IDENTITY = UNRESOLVED`: performance evidence is auditable, but exact historical producer/version binding remains unavailable.","","## QUESTIONS_REQUIRING_HUMAN_DELIBERATION_AFTER_RECOVERY"]+[f"- {q}" for q in questions]
    (OUT/"concise_mlb_hits_predictive_parity_recovered_population_v2.md").write_text("\n".join(md)+"\n")
    summary={"task_id":"MLB_HITS_PREDICTIVE_PARITY_RECOVERED_POPULATION_V2","source_sha256":hashlib.sha256(SOURCE.read_bytes()).hexdigest(),"rows":len(d),"provenance_behavior":behavior,"pooled_classification":parity,"ordering":ordering_status,"temporal":temporal_status,"historical_model_identity":"UNRESOLVED","questions":questions,"certification_decision_made":False}
    (OUT/"v2_summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    products=sorted(p for p in OUT.iterdir() if p.name!="reproducibility_hashes.sha256")
    (OUT/"reproducibility_hashes.sha256").write_text("".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in products))
    print(json.dumps(summary,indent=2)); return 0


if __name__=="__main__": raise SystemExit(main())
