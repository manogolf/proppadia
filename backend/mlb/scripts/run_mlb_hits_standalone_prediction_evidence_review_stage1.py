#!/usr/bin/env python3
"""Descriptive Stage 1 review of the frozen Hits parity population."""
from __future__ import annotations
import argparse, hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/"artifacts/analysis/model_development/mlb_prop_betonline_predictive_parity_benchmark_v1/2026-08-14/prop_synchronized_population.csv"
DEFAULT_OUT=ROOT/"artifacts/analysis/model_development/mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14"

def ll(p,y):
 p=np.clip(np.asarray(p,float),1e-12,1-1e-12); y=np.asarray(y,float); return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p))))
def ece(p,y):
 p=np.asarray(p,float); y=np.asarray(y,float); z=0.
 for lo,hi in [(0,.5),(.5,.55),(.55,.6),(.6,.65),(.65,.7),(.7,.75),(.75,1.00001)]:
  m=(p>=lo)&(p<hi)
  if m.any(): z+=m.mean()*abs(p[m].mean()-y[m].mean())
 return float(z)
def met(g,col):
 if not len(g): return {"rows":0}
 p=g[col].astype(float); y=g.target.astype(float)
 return {"rows":len(g),"brier":float(((p-y)**2).mean()),"log_loss":ll(p,y),"ece":ece(p,y),"accuracy":float(((p>=.5)==(y==1)).mean()),"mean_probability":p.mean(),"probability_sd":p.std(ddof=0),"observed_rate":y.mean()}
def both(g):
 m=met(g,"model_probability"); b=met(g,"betonline_probability"); me=(g.model_probability-g.target).abs(); be=(g.betonline_probability-g.target).abs()
 return {"rows":len(g),**{f"model_{k}":v for k,v in m.items() if k!="rows"},**{f"betonline_{k}":v for k,v in b.items() if k!="rows"},"mean_absolute_separation":g.absolute_separation.mean(),"median_absolute_separation":g.absolute_separation.median(),"model_closer":int((me<be).sum()),"betonline_closer":int((be<me).sum())}
def write(p,rows): pd.DataFrame(rows).to_csv(p,index=False,lineterminator="\n")
def groups(d,cols,fn):
 out=[]
 for k,g in d.groupby(cols,dropna=False,sort=True):
  k=(k,) if not isinstance(k,tuple) else k; r=dict(zip(cols,k)); r.update(fn(g)); out.append(r)
 return out
def pband(p): return "50-54.99%" if p<.55 else "55-59.99%" if p<.6 else "60-64.99%" if p<.65 else "65-69.99%" if p<.7 else "70-74.99%" if p<.75 else ">=75%"
def sband(x): return "<2.5pp" if x<.025 else "2.5-4.99pp" if x<.05 else "5.0-7.49pp" if x<.075 else "7.5-9.99pp" if x<.1 else "10.0-14.99pp" if x<.15 else ">=15pp"

def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--out-dir",default=str(DEFAULT_OUT)); a=ap.parse_args(); out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True)
 allrows=pd.read_csv(SOURCE,low_memory=False); d=allrows[allrows.prop_type.eq("hits")].copy()
 assert len(d)==7564 and d.canonical_identity.nunique()==7564
 d.to_csv(out/"frozen_hits_review_population.csv",index=False,lineterminator="\n")
 d["line_label"]=d.line.map(lambda x:f"Hits {float(x):g}"); d["probability_band"]=d.model_probability.map(pband); d["separation_band"]=d.absolute_separation.map(sband)
 composition=[{"scope":"overall","rows":len(d),"unique_games":d.game_id.nunique(),"unique_players":d.player_id.nunique(),"unique_dates":d.game_date.nunique(),"date_start":d.game_date.min(),"date_end":d.game_date.max(),"months":"|".join(sorted(d.game_date.str[:7].unique())),"over_rows":int(d.side.eq('over').sum()),"under_rows":int(d.side.eq('under').sum()),"duplicate_identities":int(d.canonical_identity.duplicated().sum()),"correlated_player_game_groups":int((d.groupby(['game_id','player_id']).size()>1).sum())}]
 composition+=groups(d,["line","side"],lambda g:{"rows":len(g),"unique_games":g.game_id.nunique(),"unique_players":g.player_id.nunique()}); write(out/"hits_population_composition.csv",composition)
 write(out/"hits_line_quality.csv",groups(d,["line"],both)); write(out/"hits_side_quality.csv",groups(d,["line","side"],both))
 reliability=[]
 scopes=[("all",d),("hits_0.5",d[d.line.eq(.5)]),("hits_1.5",d[d.line.eq(1.5)]),("over",d[d.side.eq('over')]),("under",d[d.side.eq('under')])]
 for scope,g in scopes:
  for band,b in g.groupby("probability_band"):
   m=met(b,"model_probability"); reliability.append({"scope":scope,"band":band,**m,"calibration_gap":m.get('mean_probability',np.nan)-m.get('observed_rate',np.nan),"sample_status":"SMALL_SAMPLE" if len(b)<100 else "ADEQUATE_DESCRIPTIVE_SAMPLE"})
 write(out/"hits_probability_reliability.csv",reliability)
 confidence=[]
 for scope,g in scopes[:3]:
  g=g.sort_values(["model_probability","canonical_identity"]).copy(); g["quintile"]=pd.qcut(np.arange(len(g)),5,labels=["bottom20","second20","middle20","fourth20","top20"])
  for q,x in g.groupby("quintile",observed=False): confidence.append({"scope":scope,"confidence_group":str(q),**met(x,"model_probability"),"over_pct":x.side.eq('over').mean(),"hits05_pct":x.line.eq(.5).mean()})
  x=g.tail(max(1,math.ceil(len(g)*.1))); confidence.append({"scope":scope,"confidence_group":"top10",**met(x,"model_probability"),"over_pct":x.side.eq('over').mean(),"hits05_pct":x.line.eq(.5).mean()})
 write(out/"hits_confidence_ordering.csv",confidence)
 sep=[]
 for band,g in d.groupby("separation_band"):
  r={"analysis":"absolute","band":band,**both(g),"over_pct":g.side.eq('over').mean(),"hits05_pct":g.line.eq(.5).mean()}; sep.append(r)
 for label,g in [("model_more_confident_ge15pp",d[d.model_minus_betonline>=.15]),("model_less_confident_ge15pp",d[d.model_minus_betonline<=-.15])]: sep.append({"analysis":"signed","band":label,**both(g),"over_pct":g.side.eq('over').mean() if len(g) else None,"hits05_pct":g.line.eq(.5).mean() if len(g) else None})
 write(out/"hits_separation_analysis.csv",sep)
 temporal=[]; d["month"]=d.game_date.str[:7]
 for month,g in d.groupby("month"): temporal.append({"period_type":"month","period":month,**both(g),"confidence_ordering_delta_top_minus_bottom":g.nlargest(max(1,len(g)//5),'model_probability').target.mean()-g.nsmallest(max(1,len(g)//5),'model_probability').target.mean()})
 for i,g in enumerate(np.array_split(d.sort_values(["game_date","canonical_identity"]),3),1): temporal.append({"period_type":"chronological_third","period":f"third_{i}",**both(g),"confidence_ordering_delta_top_minus_bottom":g.nlargest(max(1,len(g)//5),'model_probability').target.mean()-g.nsmallest(max(1,len(g)//5),'model_probability').target.mean()})
 write(out/"hits_temporal_behavior.csv",temporal)
 unavailable=[{"classification":"NOT_AVAILABLE_IN_FROZEN_SYNCHRONIZED_LINEAGE","rows":0,"reason":"No governed existing player-history-depth classification is embedded in the frozen population; thresholds were not invented."}]; write(out/"hits_history_depth_analysis.csv",unavailable)
 write(out/"hits_starter_context_analysis.csv",[{"classification":"NOT_AVAILABLE_IN_FROZEN_SYNCHRONIZED_LINEAGE","rows":0,"reason":"No governed pregame starter-strength/history/handedness category is embedded in the frozen population; no rematch or fitted proxy was created."}])
 prospective=[{"lane":"Hits 0.5 full-spine","predictions":0,"resolved":0,"unresolved":1,"latest_date":"2026-08-14","maturity":"INSUFFICIENT"},{"lane":"Hits 0.5 Expected-PA","predictions":126,"resolved":0,"unresolved":126,"latest_date":"2026-07-21","maturity":"INSUFFICIENT"},{"lane":"active Hits prediction ledger","predictions":0,"resolved":0,"unresolved":0,"latest_date":"","maturity":"NO_QUALIFIED_ACTIVE_LEDGER_IDENTIFIED"}]; write(out/"hits_prospective_evidence_status.csv",prospective)
 line=groups(d,["line"],both); side=groups(d,["line","side"],both); high=next(x for x in sep if x['band']=='>=15pp'); questions=["Should Hits 0.5 and Hits 1.5 be reviewed as separate prediction authorities?","Is the observed line/side asymmetry operationally material?","Does deterioration at >=15 pp separation undermine probability trust despite aggregate parity?","Is temporal behavior stable enough across the retained May-August window?","Must prospective resolved evidence mature before any certification decision?"]
 md=["# MLB Hits standalone prediction evidence review — Stage 1","",f"Frozen population: {len(d):,} exact synchronized rows, {d.game_date.min()} through {d.game_date.max()}; immutable source hash `{hashlib.sha256(SOURCE.read_bytes()).hexdigest()}`.","","This is descriptive evidence only. No certification decision, model change, recalibration, selector, ROI/EV, or UI recommendation was made.","","## Line evidence"]
 for x in line: md.append(f"- Hits {x['line']:g}: n={x['rows']}; model Brier {x['model_brier']:.6f} vs BetOnline {x['betonline_brier']:.6f}; model log loss {x['model_log_loss']:.6f} vs {x['betonline_log_loss']:.6f}; mean absolute separation {x['mean_absolute_separation']:.2%}.")
 md += ["","## Separation",f"At >=15 pp: n={high['rows']}; model Brier {high['model_brier']:.6f} vs BetOnline {high['betonline_brier']:.6f}; Over share {high['over_pct']:.1%}; Hits 0.5 share {high['hits05_pct']:.1%}.","","## Evidence limits","- Governed history-depth and starter-context classifications are not embedded in the frozen population, so those analyses fail closed.","- Current prospective Hits evidence is not mature enough to compare with the historical population.","","## QUESTIONS_REQUIRING_REVIEW_BEFORE_CERTIFICATION"]+[f"- {q}" for q in questions]
 (out/"concise_mlb_hits_stage1_evidence_review.md").write_text("\n".join(md)+"\n")
 meta={"task_id":"MLB_HITS_STANDALONE_PREDICTION_EVIDENCE_REVIEW_STAGE1","population_rows":len(d),"source_sha256":hashlib.sha256(SOURCE.read_bytes()).hexdigest(),"model_identity":"retained per-date production probability; semantic version not embedded in frozen rows","lines":sorted(d.line.unique().tolist()),"sides":sorted(d.side.unique().tolist()),"questions_requiring_review_before_certification":questions,"certification_decision_made":False}
 (out/"stage1_summary.json").write_text(json.dumps(meta,indent=2)+"\n")
 products=sorted(p for p in out.iterdir() if p.name!="reproducibility_hashes.sha256"); (out/"reproducibility_hashes.sha256").write_text("".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in products)); print(json.dumps(meta,indent=2)); return 0
if __name__=="__main__": raise SystemExit(main())
