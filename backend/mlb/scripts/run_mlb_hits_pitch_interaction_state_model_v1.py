#!/usr/bin/env python3
"""Pitch/PA-unit MLB Hits interaction-state research model (production forbidden)."""
from __future__ import annotations

import argparse, hashlib, json, math
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

ROOT=next(p for p in Path(__file__).resolve().parents if (p/"Makefile").exists())
FAMILIES=("fastball","breaking","offspeed","other")
EPS=1e-6

def sha(p):
 h=hashlib.sha256();
 with Path(p).open("rb") as f:
  for x in iter(lambda:f.read(1<<20),b""):h.update(x)
 return h.hexdigest()

def family(x):
 x=str(x);return "fastball" if x in {"FF","SI","FC","FA"} else "breaking" if x in {"SL","ST","CU","KC","SV","CS"} else "offspeed" if x in {"CH","FS","FO","SC"} else "other"

def prep_pitches(files):
 cols=["game_date","game_pk","batter","pitcher","stand","p_throws","pitch_type","release_speed","pfx_x","pfx_z","plate_x","plate_z","balls","strikes","description","events","at_bat_number","pitch_number","inning","inning_topbot","home_team","away_team","n_thruorder_pitcher","estimated_ba_using_speedangle","source_raw_sha256"]
 parts=[pd.read_parquet(p,columns=cols) for p in files];d=pd.concat(parts,ignore_index=True)
 for c in ["game_pk","batter","pitcher","balls","strikes","pitch_number","at_bat_number","inning","release_speed","pfx_x","pfx_z","plate_x","plate_z","n_thruorder_pitcher","estimated_ba_using_speedangle"]:d[c]=pd.to_numeric(d[c],errors="coerce")
 d["game_date"]=d.game_date.astype(str);d=d.sort_values(["game_date","game_pk","at_bat_number","pitch_number"])
 d["pitch_family"]=d.pitch_type.map(family);desc=d.description.fillna("").astype(str);ev=d.events.fillna("").astype(str)
 d["swing"]=desc.str.contains("swing|foul|hit_into_play",case=False,regex=True).astype(int);d["whiff"]=desc.str.contains("swinging_strike|missed_bunt",case=False,regex=True).astype(int)
 d["called_strike"]=desc.eq("called_strike").astype(int);d["foul"]=desc.str.contains("foul",case=False).astype(int);d["ball_in_play"]=(desc.eq("hit_into_play")|ev.isin(["single","double","triple","home_run","field_out","force_out","grounded_into_double_play","field_error"])).astype(int)
 d["hit_terminal"]=ev.isin(["single","double","triple","home_run"]).astype(int);d["terminal"]=ev.ne("").astype(int);d["zone_pitch"]=d.zone.between(1,9).astype(int) if "zone" in d else ((d.plate_x.abs()<=.83)&d.plate_z.between(1.5,3.5)).astype(int)
 d["chase"]=(d.swing.eq(1)&d.zone_pitch.eq(0)).astype(int);d["contact"]=(d.swing.eq(1)&d.whiff.eq(0)).astype(int)
 d["pitching_team"]=np.where(d.inning_topbot.astype(str).str.lower().eq("top"),d.home_team,d.away_team)
 first=d.groupby(["game_pk","pitching_team"]).pitcher.transform("first");d["starter_pitch"]=(d.pitcher==first).astype(int)
 return d

def sequential_states(d,key,prefix):
 base=d.groupby(["game_date",key]).agg(pitches=("pitcher","size"),swing=("swing","sum"),chase=("chase","sum"),whiff=("whiff","sum"),called_strike=("called_strike","sum"),foul=("foul","sum"),ball_in_play=("ball_in_play","sum"),hit_terminal=("hit_terminal","sum"),terminal=("terminal","sum"),zone=("zone_pitch","sum"),velocity=("release_speed","mean"),movement_x=("pfx_x","mean"),movement_z=("pfx_z","mean")).reset_index()
 fam=d.groupby(["game_date",key,"pitch_family"]).size().unstack(fill_value=0).reset_index();base=base.merge(fam,on=["game_date",key],how="left")
 base=base.sort_values([key,"game_date"]);out=[]
 for ident,g in base.groupby(key,sort=False):
  g=g.copy();prior_n=g.pitches.cumsum().shift(fill_value=0);g[f"{prefix}_prior_pitch_depth"]=prior_n
  metrics={"swing_rate":("swing","pitches"),"chase_rate":("chase","pitches"),"whiff_rate":("whiff","pitches"),"called_strike_rate":("called_strike","pitches"),"foul_rate":("foul","pitches"),"bip_rate":("ball_in_play","pitches"),"hit_per_terminal":("hit_terminal","terminal"),"hit_per_bip":("hit_terminal","ball_in_play"),"zone_rate":("zone","pitches")}
  for name,(num,den) in metrics.items():
   long=(g[num].cumsum().shift(fill_value=0)+10*.2)/(g[den].cumsum().shift(fill_value=0)+10);current=(g[num]/g[den].replace(0,np.nan)).ewm(alpha=.18,adjust=False).mean().shift()
   g[f"{prefix}_long_{name}"]=long;g[f"{prefix}_current_{name}"]=current.fillna(long);g[f"{prefix}_uncertainty_{name}"]=np.sqrt(long*(1-long)/(prior_n+11))
  for fam in FAMILIES:
   if fam not in g:g[fam]=0
   g[f"{prefix}_pitch_family_{fam}"]=(g[fam].cumsum().shift(fill_value=0)+5)/(prior_n+20)
  for c in ["velocity","movement_x","movement_z"]:g[f"{prefix}_current_{c}"]=g[c].ewm(alpha=.18,adjust=False).mean().shift();g[f"{prefix}_long_{c}"]=g[c].expanding().mean().shift()
  out.append(g)
 return pd.concat(out,ignore_index=True)

def pa_dist(train_pa,slot,home):
 league=np.bincount(train_pa.pa_count.clip(0,6),minlength=7)+1.;league=league/league.sum();g=train_pa[train_pa.batting_order_position.eq(slot)&train_pa.home_away.eq(home)];c=np.bincount(g.pa_count.clip(0,6),minlength=7).astype(float)+20*league;return c/c.sum()

def pb(ps):
 q=np.zeros(5);q[0]=1
 for p in ps:
  z=np.zeros(5);z[0]=q[0]*(1-p)
  for k in range(1,4):z[k]=q[k]*(1-p)+q[k-1]*p
  z[4]=q[4]+q[3]*p;q=z
 return q/q.sum()

def main():
 ap=argparse.ArgumentParser();ap.add_argument("--pitch-root",type=Path,required=True);ap.add_argument("--lineups",type=Path,required=True);ap.add_argument("--out-dir",type=Path,required=True);a=ap.parse_args()
 if a.out_dir.exists() and any(a.out_dir.iterdir()):raise FileExistsError(a.out_dir)
 a.out_dir.mkdir(parents=True);files=sorted(a.pitch_root.glob("*.parquet"));d=prep_pitches(files)
 bat=sequential_states(d,"batter","batter");pit=sequential_states(d,"pitcher","pitcher")
 bat.to_csv(a.out_dir/"sequential_batter_state_dataset.csv",index=False);pit.to_csv(a.out_dir/"sequential_pitcher_state_dataset.csv",index=False)
 line=pd.read_parquet(a.lineups);line=line[line.lineup_certification_status.eq("FINAL_LINEUP_ONLY")].copy();line["game_pk"]=pd.to_numeric(line.game_pk);line["player_id"]=pd.to_numeric(line.player_id);line["batting_order_position"]=pd.to_numeric(line.batting_order_position)
 games=d.groupby("game_pk").agg(game_date=("game_date","first"),home_team=("home_team","first"),away_team=("away_team","first")).reset_index();line=line.merge(games,on="game_pk",how="inner");line["home_away"]=line.home_away.astype(str)
 pa=d[d.terminal.eq(1)].groupby(["game_date","game_pk","batter"]).agg(pa_count=("terminal","sum"),hits=("hit_terminal","sum")).reset_index();line=line.merge(pa,left_on=["game_date","game_pk","player_id"],right_on=["game_date","game_pk","batter"],how="inner")
 # Official starter identity is the research lineup-lock opponent identity; no realized later usage is a feature.
 starters=d[d.starter_pitch.eq(1)].drop_duplicates(["game_pk","pitching_team"])[["game_pk","pitching_team","pitcher","p_throws"]]
 line["opponent_team"]=np.where(line.home_away.eq("home"),line.away_team,line.home_team);line=line.merge(starters,left_on=["game_pk","opponent_team"],right_on=["game_pk","pitching_team"],how="left")
 bat_keep=[c for c in bat if c.startswith("batter_") or c in ["game_date","batter"]];pit_keep=[c for c in pit if c.startswith("pitcher_") or c in ["game_date","pitcher"]]
 x=line.merge(bat[bat_keep],left_on=["game_date","player_id"],right_on=["game_date","batter"],how="left").merge(pit[pit_keep],on=["game_date","pitcher"],how="left",suffixes=("","_pit"))
 x=x[x.batter_prior_pitch_depth.ge(150)&x.pitcher_prior_pitch_depth.ge(150)].copy();rows=[]
 for date,g in x.groupby("game_date"):
  prior=line[line.game_date<date]
  if prior.game_date.nunique()<14:continue
  # Prior-only global transition probabilities are count-conditioned and frozen for D.
  td=d[d.game_date<date];counts=td.groupby(["balls","strikes"]);trans={}
  for state,h in counts:
   n=len(h);trans[state]={"ball":float(h.description.astype(str).str.contains("ball",case=False).mean()),"called_strike":float(h.called_strike.mean()),"swinging_strike":float(h.whiff.mean()),"foul":float(h.foul.mean()),"ball_in_play":float(h.ball_in_play.mean())}
  pa_cache={(slot,ha):pa_dist(prior,slot,ha) for slot in range(1,10) for ha in ["home","away"]}
  for _,r in g.iterrows():
   mix=[]
   for fam in FAMILIES:mix.append(float(.75*r.get(f"pitcher_pitch_family_{fam}",.25)+.25*r.get(f"batter_pitch_family_{fam}",.25)))
   mix=np.array(mix);mix=mix/mix.sum()
   contact=float(np.clip(.55*r.batter_current_bip_rate+.25*(1-r.pitcher_current_whiff_rate)+.20*r.batter_current_swing_rate,.05,.65))
   bip_hit=float(np.clip(.65*r.batter_current_hit_per_bip+.35*r.pitcher_current_hit_per_bip,.05,.55))
   # Compact absorbing count-state recursion. Interaction alters BIP probability; global prior controls count progression.
   memo={}
   def hitprob(b,s,depth=0):
    if depth>12:return contact*bip_hit
    key=(b,s,depth)
    if key in memo:return memo[key]
    t=trans.get((b,s),{"ball":.35,"called_strike":.16,"swinging_strike":.11,"foul":.17,"ball_in_play":.21});p_bip=np.clip(.5*t["ball_in_play"]+.5*contact,.05,.55);p_ball=t["ball"];p_cs=t["called_strike"];p_sw=t["swinging_strike"];p_f=t["foul"];tot=p_bip+p_ball+p_cs+p_sw+p_f;p_bip,p_ball,p_cs,p_sw,p_f=[z/tot for z in [p_bip,p_ball,p_cs,p_sw,p_f]]
    val=p_bip*bip_hit
    val+=p_ball*(0 if b>=3 else hitprob(b+1,s,depth+1));val+=(p_cs+p_sw)*(0 if s>=2 else hitprob(b,s+1,depth+1));val+=p_f*hitprob(b,min(s+1,2),depth+1);memo[key]=val;return val
   ph=float(np.clip(hitprob(0,0),.01,.60));control=float(np.clip(r.batter_long_hit_per_terminal,.01,.60));pdst=pa_cache[(int(r.batting_order_position),r.home_away)];dist=np.zeros(5);ctrl=np.zeros(5)
   for n,pn in enumerate(pdst):dist+=pn*pb([ph]*n);ctrl+=pn*pb([control]*n)
   rows.append({"game_date":date,"game_pk":int(r.game_pk),"batter_id":int(r.player_id),"pitcher_id":int(r.pitcher),"batting_order_position":int(r.batting_order_position),"home_away":r.home_away,"replay_state":"HISTORICAL_LINEUP_LOCK_RESEARCH_REPLAY","fit_cutoff":str(pd.Timestamp(date)-pd.Timedelta(days=1))[:10],"expected_fastball_share":mix[0],"expected_breaking_share":mix[1],"expected_offspeed_share":mix[2],"expected_other_share":mix[3],"interaction_contact_probability":contact,"interaction_bip_hit_probability":bip_hit,"pa_hit_probability":ph,"control_pa_hit_probability":control,**{f"p_hits_{k if k<4 else '4_plus'}":dist[k] for k in range(5)},"hits_over_05_probability":1-dist[0],"hits_under_05_probability":dist[0],"hits_over_15_probability":dist[2:].sum(),"hits_under_15_probability":dist[:2].sum(),"control_o05":1-ctrl[0],"control_o15":ctrl[2:].sum(),"actual_hits":int(r.hits),"actual_pa":int(r.pa_count)})
 pred=pd.DataFrame(rows);frozen=pred.drop(columns=["actual_hits","actual_pa"]);frozen.to_csv(a.out_dir/"rolling_player_game_prediction_ledger.csv",index=False)
 comp=[]
 for linev,ycol,pcol,ccol in [(.5,"y05","hits_over_05_probability","control_o05"),(1.5,"y15","hits_over_15_probability","control_o15")]:
  pred[ycol]=(pred.actual_hits>linev).astype(int);y=pred[ycol];diff=(pred[pcol]-y)**2-(pred[ccol]-y)**2
  comp.append({"line":linev,"rows":len(pred),"dates":pred.game_date.nunique(),"interaction_brier":float(((pred[pcol]-y)**2).mean()),"control_brier":float(((pred[ccol]-y)**2).mean()),"absolute_brier_improvement":float(-diff.mean()),"interaction_log_loss":float(log_loss(y,np.clip(pred[pcol],EPS,1-EPS))),"control_log_loss":float(log_loss(y,np.clip(pred[ccol],EPS,1-EPS))),"percentage_dates_improved":float((pred.assign(loss_diff=diff).groupby("game_date")["loss_diff"].mean()<0).mean()),"changed_2pp_share":float(((pred[pcol]-pred[ccol]).abs()>=.02).mean()),"changed_5pp_share":float(((pred[pcol]-pred[ccol]).abs()>=.05).mean()),"changed_10pp_share":float(((pred[pcol]-pred[ccol]).abs()>=.10).mean())})
 pd.DataFrame(comp).to_csv(a.out_dir/"prop_comparison.csv",index=False)
 # Component scoring on frozen date-level states and observed pitches.
 ev=d.merge(bat[bat_keep],on=["game_date","batter"],how="left").merge(pit[pit_keep],on=["game_date","pitcher"],how="left");ev=ev[ev.batter_prior_pitch_depth.ge(150)&ev.pitcher_prior_pitch_depth.ge(150)]
 fam_obs=pd.get_dummies(ev.pitch_family).reindex(columns=FAMILIES,fill_value=0);fam_pred=np.column_stack([.75*ev[f"pitcher_pitch_family_{f}"]+.25*ev[f"batter_pitch_family_{f}"] for f in FAMILIES]);fam_pred=fam_pred/fam_pred.sum(1,keepdims=True)
 selection_ll=float(-np.mean(np.sum(fam_obs.to_numpy()*np.log(np.clip(fam_pred,EPS,1)),axis=1)))
 paev=ev[ev.terminal.eq(1)];pa_p=np.clip(.55*paev.batter_current_hit_per_terminal+.45*paev.pitcher_current_hit_per_terminal,.01,.60)
 component=pd.DataFrame([{"component":"pitch_selection","rows":len(ev),"metric":"multiclass_log_loss","value":selection_ll},{"component":"count_transition","rows":len(ev),"metric":"source_count_state_coverage","value":float(ev[['balls','strikes']].drop_duplicates().shape[0]/12)},{"component":"PA_hit_terminal","rows":len(paev),"metric":"brier","value":float(((pa_p-paev.hit_terminal)**2).mean())},{"component":"PA_hit_terminal","rows":len(paev),"metric":"log_loss","value":float(log_loss(paev.hit_terminal,pa_p))},{"component":"player_game_coherence","rows":len(pred),"metric":"violations","value":float(((pred.hits_over_15_probability>pred.hits_over_05_probability)|((pred.hits_over_05_probability+pred.hits_under_05_probability-1).abs()>1e-10)).sum())}]);component.to_csv(a.out_dir/"component_evaluation.csv",index=False)
 spec={"architecture":"MLB_HITS_PITCH_INTERACTION_STATE_MODEL_V1","unit":"pitch_and_plate_appearance","prediction_point":"OFFICIAL_STARTING_LINEUP_LOCK_BEFORE_FIRST_PITCH","replay_label":"HISTORICAL_LINEUP_LOCK_RESEARCH_REPLAY","decay_alpha":.18,"state":"prior-day long-term plus exponentially weighted current plus uncertainty","market_inputs":False,"production":"NO_QUALIFIED_MLB_MODEL","closed_predecessor":"STRUCTURAL_V1_V2_CLOSED_NO_FURTHER_REFINEMENT_AUTHORIZED"};(a.out_dir/"pitch_pa_event_state_specification.json").write_text(json.dumps(spec,indent=2)+"\n")
 transition={"architecture":"count-state absorbing Markov recursion","states":"balls 0-3 x strikes 0-2","outcomes":["ball","called_strike","swinging_strike","foul","ball_in_play"],"pitch_mix_families":FAMILIES,"fitting":"strict-prior expanding by evaluation date"};(a.out_dir/"pitch_selection_count_transition_model.json").write_text(json.dumps(transition,indent=2)+"\n")
 outcome={"architecture":"partially pooled batter-pitcher dynamic interaction","mechanisms":["expected pitch mix","contact","ball in play","batted-ball success","count progression","opponent suppression"],"player_game_constructor":"fixed prior batting-slot/home PA distribution plus Poisson-binomial aggregation"};(a.out_dir/"pa_outcome_model.json").write_text(json.dumps(outcome,indent=2)+"\n")
 best=max([r["absolute_brier_improvement"] for r in comp],default=-1);decision="PITCH_LEVEL_COMPONENT_SIGNAL_ONLY" if best>0 else "PITCH_INTERACTION_MODEL_FAILED"
 if best>=.005:decision="PITCH_INTERACTION_BASEBALL_MODEL_PRACTICALLY_USEFUL_BELOW_MARKET"
 (a.out_dir/"concise_interpretation.md").write_text(f"# Pitch Interaction State Model v1\n\nFinal decision: **{decision}**. The 0.005 practical continuation bar {'was' if best>=.005 else 'was not'} met. Market data was excluded and no production action is authorized. If the bar was not met, this architecture is closed.\n")
 hashes=[]
 for p in sorted(a.out_dir.iterdir()):
  if p.is_file() and p.name!="reproducibility_hashes.csv":hashes.append({"path":p.name,"sha256":sha(p),"bytes":p.stat().st_size})
 pd.DataFrame(hashes).to_csv(a.out_dir/"reproducibility_hashes.csv",index=False);print(json.dumps({"decision":decision,"pitch_rows":len(d),"prediction_rows":len(pred),"dates":pred.game_date.nunique(),"best_brier_improvement":best},indent=2))

if __name__=="__main__":main()
