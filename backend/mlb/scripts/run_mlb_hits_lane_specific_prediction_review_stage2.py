#!/usr/bin/env python3
"""Lane-specific descriptive review of the immutable Stage 1 Hits population."""
from __future__ import annotations
import argparse, hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/"artifacts/analysis/model_development/mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14/frozen_hits_review_population.csv"
DEFAULT_OUT=ROOT/"artifacts/analysis/model_development/mlb_hits_lane_specific_prediction_review_stage2/2026-08-14"
LANES={(0.5,"over"):"HITS_05_OVER",(0.5,"under"):"HITS_05_UNDER",(1.5,"over"):"HITS_15_OVER",(1.5,"under"):"HITS_15_UNDER"}

def logloss(p,y):
 p=np.clip(np.asarray(p,float),1e-12,1-1e-12); y=np.asarray(y,float); return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p))))
def ece(p,y):
 p=np.asarray(p,float); y=np.asarray(y,float); z=0.
 for lo,hi in [(0,.5),(.5,.55),(.55,.6),(.6,.65),(.65,.7),(.7,.75),(.75,1.00001)]:
  m=(p>=lo)&(p<hi)
  if m.any(): z+=m.mean()*abs(p[m].mean()-y[m].mean())
 return float(z)
def metrics(g,col):
 if not len(g): return {"rows":0,"brier":None,"log_loss":None,"ece":None,"accuracy":None,"mean_probability":None,"observed_rate":None,"probability_sd":None,"probability_min":None,"probability_max":None,"calibration_gap_observed_minus_predicted":None}
 p=g[col].astype(float); y=g.target.astype(float)
 return {"rows":len(g),"brier":float(((p-y)**2).mean()),"log_loss":logloss(p,y),"ece":ece(p,y),"accuracy":float(((p>=.5)==(y==1)).mean()),"mean_probability":p.mean(),"observed_rate":y.mean(),"probability_sd":p.std(ddof=0),"probability_min":p.min(),"probability_max":p.max(),"calibration_gap_observed_minus_predicted":y.mean()-p.mean()}
def write(p,rows): pd.DataFrame(rows).to_csv(p,index=False,lineterminator="\n")
def pband(p): return "50-54.99%" if p<.55 else "55-59.99%" if p<.6 else "60-64.99%" if p<.65 else "65-69.99%" if p<.7 else "70-74.99%" if p<.75 else ">=75%"
def sband(x): return "<2.5pp" if x<.025 else "2.5-4.99pp" if x<.05 else "5.0-7.49pp" if x<.075 else "7.5-9.99pp" if x<.1 else "10.0-14.99pp" if x<.15 else ">=15pp"
def ordering(vals):
 rates=list(vals); rises=sum(b>=a for a,b in zip(rates,rates[1:])); span=rates[-1]-rates[0]
 return "MONOTONIC_OR_NEAR_MONOTONIC" if rises>=3 and span>.04 else "PARTIAL_ORDERING" if span>.02 else "INVERTED_OR_UNRELIABLE" if span<-.02 else "FLAT"
def temporal_label(rows):
 if len(rows)<3:return "MIXED"
 delta=rows[-1]["model_brier"]-rows[0]["model_brier"]
 ece_delta=rows[-1]["model_ece"]-rows[0]["model_ece"]
 return "DETERIORATING" if delta>.015 or ece_delta>.05 else "IMPROVING" if delta<-.015 and ece_delta<0 else "STABLE" if abs(delta)<.005 and abs(ece_delta)<.03 else "MILD_DRIFT"
def closer(g):
 me=(g.model_probability-g.target).abs(); be=(g.betonline_probability-g.target).abs(); return int((me<be).sum()),int((be<me).sum())

def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--out-dir",default=str(DEFAULT_OUT)); a=ap.parse_args(); out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True)
 d=pd.read_csv(SOURCE,low_memory=False); assert len(d)==7564 and d.canonical_identity.nunique()==7564
 d["lane"]=[LANES[(float(x),str(s))] for x,s in zip(d.line,d.side)]; assert set(d.lane)==set(LANES.values()) and d.groupby('lane').size().sum()==7564
 d["probability_band"]=d.model_probability.map(pband); d["separation_band"]=d.absolute_separation.map(sband); d["month"]=d.game_date.str[:7]
 populations=[]; quality=[]; book=[]; reliability=[]; confidence=[]; extreme=[]; separation=[]; signed=[]; temporal=[]; concentration=[]; ordering_status={}; temporal_status={}
 for lane,g0 in d.groupby("lane",sort=True):
  g=g0.copy(); m=metrics(g,"model_probability"); b=metrics(g,"betonline_probability")
  populations.append({"lane":lane,"rows":len(g),"games":g.game_id.nunique(),"players":g.player_id.nunique(),"date_start":g.game_date.min(),"date_end":g.game_date.max(),"mean_model_probability":g.model_probability.mean(),"mean_betonline_probability":g.betonline_probability.mean(),"probability_sd":g.model_probability.std(ddof=0),"mean_absolute_separation":g.absolute_separation.mean(),"median_absolute_separation":g.absolute_separation.median()})
  quality.append({"lane":lane,**m}); book.append({"lane":lane,**{f"betonline_{k}":v for k,v in b.items()},"brier_delta_model_minus_betonline":m['brier']-b['brier'],"log_loss_delta_model_minus_betonline":m['log_loss']-b['log_loss'],"ece_delta_model_minus_betonline":m['ece']-b['ece']})
  for pb,x in g.groupby("probability_band",sort=True):
   q=metrics(x,"model_probability"); reliability.append({"lane":lane,"probability_band":pb,**q,"sample_status":"SMALL_SAMPLE" if len(x)<100 else "ADEQUATE_DESCRIPTIVE_SAMPLE"})
  ranked=g.sort_values(["model_probability","canonical_identity"]); ranked["q"]=pd.qcut(np.arange(len(ranked)),5,labels=["bottom20","second20","middle20","fourth20","top20"])
  qr=[]
  for q,x in ranked.groupby("q",observed=False):
   z=metrics(x,"model_probability"); confidence.append({"lane":lane,"confidence_group":str(q),**z}); qr.append(z['observed_rate'])
  x=ranked.tail(max(1,math.ceil(len(ranked)*.1))); confidence.append({"lane":lane,"confidence_group":"top10",**metrics(x,"model_probability")}); ordering_status[lane]=ordering(qr)
  for threshold in [.65,.70,.75]:
   x=g[g.model_probability>=threshold]; mm=metrics(x,"model_probability"); bb=metrics(x,"betonline_probability"); extreme.append({"lane":lane,"threshold":f">={threshold:.0%}",**mm,"betonline_brier":bb['brier']})
  for sb,x in g.groupby("separation_band",sort=True):
   mm=metrics(x,"model_probability"); bb=metrics(x,"betonline_probability"); mc,bc=closer(x); separation.append({"lane":lane,"separation_band":sb,"rows":len(x),"model_brier":mm['brier'],"betonline_brier":bb['brier'],"model_log_loss":mm['log_loss'],"betonline_log_loss":bb['log_loss'],"model_closer":mc,"betonline_closer":bc,"mean_signed_separation":x.model_minus_betonline.mean(),"observed_win_rate":x.target.mean()})
  signed_groups=[("MODEL_MORE_CONFIDENT",g.model_minus_betonline>0),("MODEL_LESS_CONFIDENT",g.model_minus_betonline<0),("MODEL_MORE_CONFIDENT_GE10PP",g.model_minus_betonline>=.10),("MODEL_LESS_CONFIDENT_GE10PP",g.model_minus_betonline<=-.10),("MODEL_MORE_CONFIDENT_GE15PP",g.model_minus_betonline>=.15),("MODEL_LESS_CONFIDENT_GE15PP",g.model_minus_betonline<=-.15)]
  for label,mask in signed_groups:
   x=g[mask]; mm=metrics(x,"model_probability"); bb=metrics(x,"betonline_probability"); signed.append({"lane":lane,"signed_group":label,"rows":len(x),"mean_signed_gap":x.model_minus_betonline.mean() if len(x) else None,"model_brier":mm['brier'],"betonline_brier":bb['brier'],"observed_win_rate":x.target.mean() if len(x) else None,"calibration_gap_observed_minus_predicted":mm['calibration_gap_observed_minus_predicted']})
  thirds=[]
  for month,x in g.groupby("month"): temporal.append({"lane":lane,"period_type":"month","period":month,**{f"model_{k}":v for k,v in metrics(x,'model_probability').items()},"betonline_brier":metrics(x,'betonline_probability')['brier'],"mean_absolute_separation":x.absolute_separation.mean()})
  for i,x in enumerate(np.array_split(g.sort_values(["game_date","canonical_identity"]),3),1):
   mm=metrics(x,'model_probability'); thirds.append({"lane":lane,"period_type":"chronological_third","period":f"third_{i}",**{f"model_{k}":v for k,v in mm.items()},"betonline_brier":metrics(x,'betonline_probability')['brier'],"mean_absolute_separation":x.absolute_separation.mean()}); temporal.append(thirds[-1])
  temporal_status[lane]=temporal_label(thirds)
  counts=g.groupby('player_id').size().sort_values(ascending=False); err=(g.model_probability-g.target)**2; perr=pd.DataFrame({'player_id':g.player_id,'e':err}).groupby('player_id').e.sum().sort_values(ascending=False)
  concentration.append({"lane":lane,"unique_games":g.game_id.nunique(),"unique_players":g.player_id.nunique(),"max_rows_per_player":int(counts.max()),"top10_players_row_share":counts.head(10).sum()/len(g),"top10_players_error_share":perr.head(10).sum()/err.sum()})
 write(out/"hits_stage2_lane_population.csv",populations); write(out/"hits_stage2_lane_prediction_quality.csv",quality); write(out/"hits_stage2_lane_betonline_comparison.csv",book); write(out/"hits_stage2_probability_reliability.csv",reliability); write(out/"hits_stage2_confidence_ordering.csv",confidence); write(out/"hits_stage2_extreme_confidence.csv",extreme); write(out/"hits_stage2_separation_bands.csv",separation); write(out/"hits_stage2_signed_separation.csv",signed); write(out/"hits_stage2_temporal_stability.csv",temporal); write(out/"hits_stage2_player_concentration.csv",concentration)
 pg=d.groupby(['game_id','player_id']); correlated=[]; only05=only15=bothlines=0
 for _,x in pg:
  lines=set(x.line); only05+=lines=={.5}; only15+=lines=={1.5}; bothlines+=lines=={.5,1.5}
  correlated.append({"player_game_line_structure":"both_0.5_and_1.5" if lines=={.5,1.5} else "only_0.5" if lines=={.5} else "only_1.5","side_combination":"|".join(sorted(f"{z.line:g}:{z.side}" for z in x.itertuples())),"player_games":1})
 corr=pd.DataFrame(correlated).groupby(['player_game_line_structure','side_combination'],as_index=False).player_games.sum(); corr.to_csv(out/"hits_stage2_correlated_structure.csv",index=False,lineterminator="\n")
 cross=[]; trust=[]
 for lane in sorted(LANES.values()):
  g=d[d.lane==lane]; m=metrics(g,'model_probability'); b=metrics(g,'betonline_probability'); ext=g[g.absolute_separation>=.15]
  cross.append({"lane":lane,"rows":len(g),"brier":m['brier'],"log_loss":m['log_loss'],"ece":m['ece'],"accuracy":m['accuracy'],"probability_sd":m['probability_sd'],"confidence_ordering_status":ordering_status[lane],"ge15pp_rows":len(ext),"ge15pp_brier":metrics(ext,'model_probability')['brier'],"betonline_brier":b['brier'],"temporal_status":temporal_status[lane]})
  probability="STRONG" if m['ece']<.04 else "MODERATE" if m['ece']<.08 else "WEAK" if len(g)>=100 else "INSUFFICIENT"; confidence_trust="STRONG" if ordering_status[lane].startswith('MONOTONIC') else "MODERATE" if ordering_status[lane]=='PARTIAL_ORDERING' else "WEAK"; extreme_trust="INSUFFICIENT" if len(ext)<50 else "MODERATE" if metrics(ext,'model_probability')['brier']<=metrics(ext,'betonline_probability')['brier']+.01 else "WEAK"; temp="INSUFFICIENT" if len(g)<150 else "STRONG" if temporal_status[lane]=='STABLE' else "MODERATE" if temporal_status[lane] in {'MILD_DRIFT','MIXED'} else "WEAK"
  trust.append({"lane":lane,"probability_trust":probability,"confidence_trust":confidence_trust,"extreme_trust":extreme_trust,"temporal_trust":temp,"market_independence":"STRONG" if g.absolute_separation.mean()>=.075 else "MODERATE"})
 write(out/"hits_stage2_cross_lane_comparison.csv",cross); write(out/"hits_stage2_trust_characterization.csv",trust)
 readiness=[{"lane":"HITS_05_OVER","status":"PROSPECTIVE_CAPTURE_PARTIAL","detail":"BetOnline and full-spine machinery preserve fields, but no mature resolved frozen prediction ledger."},{"lane":"HITS_05_UNDER","status":"PROSPECTIVE_CAPTURE_PARTIAL","detail":"BetOnline and full-spine machinery preserve fields, but no mature resolved frozen prediction ledger."},{"lane":"HITS_15_OVER","status":"PROSPECTIVE_CAPTURE_NOT_READY","detail":"No active lane-specific immutable prediction/outcome ledger identified."},{"lane":"HITS_15_UNDER","status":"PROSPECTIVE_CAPTURE_NOT_READY","detail":"No active lane-specific immutable prediction/outcome ledger identified."}]; write(out/"hits_stage2_prospective_capture_readiness.csv",readiness)
 q=["Is Hits 0.5 high-confidence overconfidence mild enough to tolerate, or does it undermine probability trust?","Do Hits 0.5 Over and Under differ enough to require separate treatment?","Is Hits 1.5 Under evidence sufficiently large and persistent for lane-specific prospective capture?","Is Hits 1.5 Over too sparse for a reliable conclusion?","Should resolved prospective evidence be required before deliberating about any lane?","Should any later decision be lane-specific rather than Hits-family-wide?"]
 md=["# MLB Hits lane-specific prediction review — Stage 2","","Descriptive review only; no certification, recalibration, threshold, selector, EV/ROI, UI, or model change.",""]
 for x in cross: md.append(f"- `{x['lane']}`: n={x['rows']}; Brier {x['brier']:.6f}; log loss {x['log_loss']:.6f}; ECE {x['ece']:.6f}; ordering `{x['confidence_ordering_status']}`; temporal `{x['temporal_status']}`; >=15pp n={x['ge15pp_rows']}, Brier {x['ge15pp_brier'] if x['ge15pp_brier'] is not None else 'n/a'}.")
 md += ["","## QUESTIONS_REQUIRING_HUMAN_DELIBERATION_STAGE2"]+[f"- {x}" for x in q]; (out/"concise_mlb_hits_lane_specific_prediction_review_stage2.md").write_text("\n".join(md)+"\n")
 summary={"task_id":"MLB_HITS_LANE_SPECIFIC_PREDICTION_REVIEW_STAGE2","source_sha256":hashlib.sha256(SOURCE.read_bytes()).hexdigest(),"rows":{x['lane']:x['rows'] for x in populations},"sum_rows":sum(x['rows'] for x in populations),"ordering":ordering_status,"temporal":temporal_status,"questions":q,"certification_decision_made":False}; (out/"stage2_summary.json").write_text(json.dumps(summary,indent=2)+"\n")
 products=sorted(x for x in out.iterdir() if x.name!='reproducibility_hashes.sha256'); (out/"reproducibility_hashes.sha256").write_text("".join(f"{hashlib.sha256(x.read_bytes()).hexdigest()}  {x.name}\n" for x in products)); print(json.dumps(summary,indent=2)); return 0
if __name__=="__main__": raise SystemExit(main())
