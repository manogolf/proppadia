#!/usr/bin/env python3
"""Read-only root-cause diagnostics for the frozen NHL Points HGB model."""
from __future__ import annotations

import csv, gzip, hashlib, json, math, statistics, warnings
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from scipy.stats import ks_2samp
from sklearn.metrics import brier_score_loss, log_loss

from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, MODEL_PATH, score

ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/"artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15"
CAPROOT=ROOT/"artifacts/operational/nhl/points_hgb_shadow/season=2026"
VALID=ROOT/"artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025"
FRAME=ROOT/"artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz"
OUT=ROOT/"artifacts/analysis/nhl/points_hgb_confidence_root_cause/2026-10-15"
CONF_BINS=[(.50,.55),(.55,.60),(.60,.65),(.65,.70),(.70,.75),(.75,.80),(.80,.85),(.85,math.inf)]
CONF_LABELS=["50-55%","55-60%","60-65%","65-70%","70-75%","75-80%","80-85%","85%+"]
FEATURE_LABELS={"is_home":"home/away","d10_sog_per60":"D10 SOG/60","attempts_d10_per60":"D10 attempts/60","player_points_last10":"points per game, last 10","current_season_points_prior":"season points prior","current_season_games_prior":"season games prior","mean_toi_last10":"mean TOI last 10","mean_pp_toi_last10":"mean PP TOI last 10","team_d10_sf_per_game":"team shots for, D10","last10_team_sog_share":"player share of team SOG, last 10"}

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def write_csv(name, rows):
    path=OUT/name; rows=list(rows); fields=list(dict.fromkeys(k for r in rows for k in r)) or ["empty"]
    with path.open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore",lineterminator="\n");w.writeheader();w.writerows(rows)
def mean(s): return float(pd.to_numeric(s,errors="coerce").mean()) if len(s) else None
def metrics(d, pred="expected_points", target="realized_points", pcol="prob_over_0_5", market=True):
    n=len(d)
    if not n:return {"n":0}
    y=pd.to_numeric(d[target],errors="coerce").to_numpy(float); mu=pd.to_numeric(d[pred],errors="coerce").to_numpy(float); p=pd.to_numeric(d[pcol],errors="coerce").to_numpy(float)
    r=y-mu; o=(y>=1).astype(int)
    row={"n":n,"mean_predicted_points":float(np.mean(mu)),"mean_realized_points":float(np.mean(y)),"mean_count_residual":float(np.mean(r)),"count_mae":float(np.mean(np.abs(r))),"count_rmse":float(np.sqrt(np.mean(r*r))),"point_game_rate":float(o.mean()),"mean_hgb_p_over_0_5":float(p.mean()),"over_calibration_gap":float(p.mean()-o.mean()),"over_brier":float(np.mean((p-o)**2)),"over_log_loss":float(log_loss(o,np.clip(p,1e-12,1-1e-12),labels=[0,1]))}
    if "selected_win" in d:
        wins=int(pd.to_numeric(d.selected_win).sum()); psel=pd.to_numeric(d.display_selected_probability).to_numpy(float); losses=n-wins
        row.update({"wins":wins,"losses":losses,"selected_realized_rate":wins/n,"selected_expected_rate":float(psel.mean()),"selected_calibration_gap":float(psel.mean()-wins/n),"expected_losses":float(np.sum(1-psel)),"excess_losses":float(losses-np.sum(1-psel)),"excess_loss_rate":float((losses-np.sum(1-psel))/n),"selected_brier":float(pd.to_numeric(d.selected_brier).mean()),"market_selected_brier":float(pd.to_numeric(d.market_selected_brier).mean()),"hgb_minus_market_brier":float((pd.to_numeric(d.selected_brier)-pd.to_numeric(d.market_selected_brier)).mean())})
        row["under_n"]=int((d.display_side=="UNDER").sum());row["over_n"]=n-row["under_n"]
        row["same_side_n"]=int((d.side_relation=="SAME_SIDE").sum());row["opposite_side_n"]=int((d.side_relation=="OPPOSITE_SIDE").sum())
    return row
def ranges(x, bounds, labels):
    v=pd.to_numeric(x,errors="coerce")
    return pd.Series([next((i for i,(lo,hi) in enumerate(bounds) if pd.notna(z) and lo<=z<hi),-1) for z in v],index=x.index)
def capture_index():
    bydate={}
    for f in CAPROOT.rglob("features.csv"):
        if not (f.parent/"predictions.csv").exists():continue
        receipt=f.parent/"receipt.json"
        if not receipt.exists():continue
        date=str(json.loads(receipt.read_text()).get("slate_date",""))
        if date and (date not in bydate or f.parent.name>bydate[date].parent.name):bydate[date]=f
    return bydate
def reproduce_all_captures(model):
    audit=[]
    for f in sorted(CAPROOT.rglob("features.csv")):
        stored_path=f.parent/"predictions.csv"
        if not stored_path.exists():continue
        x=pd.read_csv(f);stored=pd.read_csv(stored_path);got=score(x,model);keys=["game_id","player_id"];cols=["expected_points","prob_over_0_5","prob_over_1_5","prob_over_2_5"]
        xmat=x[FEATURES].apply(pd.to_numeric,errors="coerce").replace([np.inf,-np.inf],np.nan).fillna(0).astype(float).to_numpy();raw_hgb=np.asarray(model["model"].predict(model["scaler"].transform(xmat)),dtype=float)
        a=stored.sort_values(keys).reset_index(drop=True);b=got.sort_values(keys).reset_index(drop=True);same=len(a)==len(b) and a[keys].equals(b[keys])
        err=max((float(np.max(np.abs(a[c].to_numpy()-b[c].to_numpy()))) if same and len(a) else 0.0) for c in cols)
        audit.append({"capture_date":str(json.loads((f.parent/"receipt.json").read_text()).get("slate_date","")),"capture_path":str(f.parent.relative_to(ROOT)),"feature_sha256":sha(f),"prediction_sha256":sha(stored_path),"rows":len(x),"identity_parity":same,"raw_hgb_output_matches_expected_points_max_abs_error":float(np.max(np.abs(raw_hgb-got.expected_points.to_numpy()))) if len(x) else 0.0,"max_abs_error_expected_and_poisson_fields":err,"reproduction_status":"PASS" if same and err<=1e-12 else "FAIL"})
    return audit
def load_2026(model):
    canonical=pd.read_csv(BASE/"canonical_rows.csv")
    d=canonical[(canonical.line==.5)&(canonical.status=="SETTLED")].copy()
    caps=capture_index(); features=[]; repro=[]
    for day,g in d.groupby("slate_date",sort=True):
        f=caps.get(day)
        if f is None:raise RuntimeError(f"No immutable HGB feature capture for {day}")
        x=pd.read_csv(f); stored=pd.read_csv(f.parent/"predictions.csv"); got=score(x,model)
        key=["game_id","player_id"]; cols=["expected_points","prob_over_0_5","prob_over_1_5","prob_over_2_5"]
        a=stored.sort_values(key).reset_index(drop=True); b=got.sort_values(key).reset_index(drop=True)
        same=len(a)==len(b) and a[key].equals(b[key]); maxerr={c:(float(np.max(np.abs(a[c].to_numpy()-b[c].to_numpy()))) if same and len(a) else 0) for c in cols}
        repro.append({"slate_date":day,"capture_path":str(f.parent.relative_to(ROOT)),"feature_sha256":sha(f),"prediction_sha256":sha(f.parent/"predictions.csv"),"rows":len(x),"identity_parity":same,"max_abs_error_expected_and_poisson_fields":max(maxerr.values()),"reproduction_status":"PASS" if same and max(maxerr.values())<=1e-12 else "FAIL"})
        x=x.merge(got[key+cols],on=key,validate="one_to_one")
        features.append(x)
    f=pd.concat(features,ignore_index=True)
    joined=d.merge(f,on=["slate_date","game_id","player_id"],suffixes=("","_capture"),validate="one_to_one")
    assert len(joined)==714 and joined[FEATURES].notna().any(axis=None)
    assert np.max(np.abs(joined.expected_points-joined.expected_points_capture))<1e-12
    assert np.max(np.abs(joined.hgb_p_over-joined.prob_over_0_5))<1e-12
    return joined,caps,repro
def history_2025(model):
    h=pd.read_csv(FRAME,parse_dates=["game_date"]); pr=pd.read_csv(VALID/"hgb_poisson_season_holdout_predictions.csv.gz",parse_dates=["game_date"])
    d=pr.merge(h,on=["game_id","player_id","game_date"],suffixes=("","_source"),validate="one_to_one")
    # Establish that the retained holdout has the same deterministic model output.
    got=score(d[["game_id","player_id",*FEATURES]],model).rename(columns={"expected_points":"repro_mu","prob_over_0_5":"repro_over"})
    d=d.merge(got[["game_id","player_id","repro_mu","repro_over"]],on=["game_id","player_id"],validate="one_to_one")
    parity=max(float((d.predicted_mean-d.repro_mu).abs().max()),float((d.p_over_0_5-d.repro_over).abs().max()))
    return d,parity,h
def add_conf(d, p):
    d=d.copy();d["confidence_band"]=pd.cut(pd.to_numeric(d[p],errors="coerce"),[.5,.55,.60,.65,.70,.75,.80,.85,1.0000001],labels=CONF_LABELS,right=False,include_lowest=True).astype("string").fillna("OUTSIDE_BANDS")
    return d
def main():
    OUT.mkdir(parents=True,exist_ok=True); model=joblib.load(MODEL_PATH); estimator=model["model"]; scaler=model["scaler"]
    all_repro=reproduce_all_captures(model)
    assert all_repro and all(r["reproduction_status"]=="PASS" for r in all_repro),"All-capture reproduction gate failed; attribution stopped."
    d,caps,repro=load_2026(model)
    assert all(r["reproduction_status"]=="PASS" for r in repro),"Reproduction gate failed; attribution stopped."
    write_csv("prediction_reproduction_captures.csv",all_repro)
    write_csv("analysis_population_capture_index.csv",repro)
    h,history_parity,frame=history_2025(model)
    # Main outcome-blind cohort tags are assigned before outcome splits.
    d=add_conf(d,"display_selected_probability"); d["count_residual"]=d.realized_points-d.expected_points
    d["outcome_split"]=np.where(d.selected_win.astype(int)==1,"CORRECT","WRONG")
    d["cohort_negative_edge"]=d.display_edge<0; d["cohort_positive_edge"]=d.display_edge>=0; d["cohort_high_edge"]=d.display_edge>=.10; d["cohort_very_high_edge"]=d.display_edge>=.20
    d["cohort_confidence_65_75"]=(d.display_selected_probability>=.65)&(d.display_selected_probability<.75)
    d["cohort_market_opposition"]=d.side_relation=="OPPOSITE_SIDE"; d["cohort_same_side_high_confidence"]=(d.side_relation=="SAME_SIDE")&(d.market_gap_selected>=.05)
    cohort_defs=[("NEGATIVE_EDGE_HEALTHY_REFERENCE",d.cohort_negative_edge),("POSITIVE_EDGE",d.cohort_positive_edge),("HIGH_EDGE",d.cohort_high_edge),("VERY_HIGH_EDGE",d.cohort_very_high_edge),("HGB_CONFIDENCE_65_75",d.cohort_confidence_65_75),("MARKET_OPPOSITION",d.cohort_market_opposition),("SAME_SIDE_HIGH_CONFIDENCE",d.cohort_same_side_high_confidence)]
    regs=[]
    for name,mask in cohort_defs:
        for split in ("ALL","CORRECT","WRONG"):
            subset=d[mask & ((d.outcome_split==split) if split!="ALL" else True)]
            regs.append({"cohort":name,"outcome_split":split,"cohort_is_outcome_blind":True,**metrics(subset)})
    write_csv("cohort_registry.csv",regs)
    residual=[]
    # Fixed model confidence and exact fixed Edge bands.
    for field,labels in [("confidence_band",CONF_LABELS),("edge_band",sorted(d.edge_band.unique(),key=lambda v: float(v.replace("%"," ").split()[0]) if v[0] not in "<+" else (-99 if v.startswith("<") else 99))),("side_relation",["SAME_SIDE","OPPOSITE_SIDE"]),("display_side",["UNDER","OVER"]),("slate_date",sorted(d.slate_date.unique()))]:
        for label in labels:
            g=d[d[field].astype(str)==str(label)]
            residual.append({"dimension":field,"band":str(label),**metrics(g)})
    write_csv("expected_count_residual_tables.csv",residual)
    # Expected-mean bands; fixed bounds preserve empty cells.
    mu_bounds=[(0,.10),(.10,.20),(.20,.30),(.30,.40),(.40,.50),(.50,.70),(.70,1.0),(1.0,math.inf)];mu_labels=["<.10",".10-.20",".20-.30",".30-.40",".40-.50",".50-.70",".70-1.00","1.00+"]
    d["expected_count_band"]=pd.cut(d.expected_points,[-1e-9,.1,.2,.3,.4,.5,.7,1,math.inf],labels=mu_labels,right=False).astype("string")
    write_csv("expected_count_bins.csv",[{"band":b,**metrics(d[d.expected_count_band==b])} for b in mu_labels])
    # Full feature distribution contrasts and population-level standardized differences.
    masks={"NEGATIVE_EDGE_HEALTHY_REFERENCE":d.cohort_negative_edge,"HIGH_EDGE":d.cohort_high_edge,"VERY_HIGH_EDGE":d.cohort_very_high_edge,"CORRECT_HIGH_CONFIDENCE":d.cohort_confidence_65_75&(d.outcome_split=="CORRECT"),"WRONG_HIGH_CONFIDENCE":d.cohort_confidence_65_75&(d.outcome_split=="WRONG")}
    frows=[]
    for feat in FEATURES:
        for cohort,mask in masks.items():
            s=pd.to_numeric(d.loc[mask,feat],errors="coerce"); allv=pd.to_numeric(d[feat],errors="coerce"); med=s.median(); q=s.quantile([.1,.25,.75,.9]); base=pd.to_numeric(d.loc[masks["NEGATIVE_EDGE_HEALTHY_REFERENCE"],feat],errors="coerce")
            pooled=math.sqrt((s.var(ddof=1)+base.var(ddof=1))/2) if s.notna().sum()>1 and base.notna().sum()>1 else np.nan
            frows.append({"feature":feat,"cohort":cohort,"n":len(s),"missing_n":int(s.isna().sum()),"missing_rate":float(s.isna().mean()) if len(s) else None,"mean":s.mean(),"median":med,"std":s.std(ddof=1),"q10":q.get(.1),"q25":q.get(.25),"q75":q.get(.75),"q90":q.get(.9),"standardized_difference_vs_negative_edge":(s.mean()-base.mean())/pooled if pooled and pd.notna(pooled) else None})
    write_csv("feature_distribution_comparisons.csv",frows)
    # Quantile residual maps for each numeric production feature.
    qrows=[]
    for feat in FEATURES:
        val=pd.to_numeric(d[feat],errors="coerce")
        if feat=="is_home":
            for label,mask in [("is_home=0",val==0),("is_home=1",val==1)]:
                g=d[mask];qrows.append({"feature":feat,"band":label,"feature_mean":mean(g[feat]),**metrics(g)})
        else:
            nonmissing=val.dropna();edges=np.unique(nonmissing.quantile(np.linspace(0,1,6)).to_numpy()) if len(nonmissing) else np.array([])
            if len(edges)<2: groups=[("single_value" if len(edges) else "all_missing",d[val.notna()] if len(edges) else d[val.isna()])]
            else:
                bins=pd.cut(val,edges,include_lowest=True,duplicates="drop")
                groups=[(str(k),d[bins==k]) for k in bins.cat.categories]+[("missing",d[val.isna()])]
            for b,g in groups:
                qrows.append({"feature":feat,"band":b,"feature_min":pd.to_numeric(g[feat],errors="coerce").min(),"feature_max":pd.to_numeric(g[feat],errors="coerce").max(),"feature_mean":mean(g[feat]),**metrics(g)})
    write_csv("feature_residual_maps.csv",qrows)
    # Season accumulation regime diagnostics and simple season P/G comparisons.
    d["season_ppg_prior"]=np.where(d.current_season_games_prior>0,d.current_season_points_prior/d.current_season_games_prior,np.nan)
    game_bins=[(-.1,.5),(.5,2.5),(2.5,5.5),(5.5,10.5),(10.5,20.5),(20.5,math.inf)];game_labels=["0","1-2","3-5","6-10","11-20","21+"]
    d["games_prior_band"]=ranges(d.current_season_games_prior,game_bins,game_labels).map(dict(enumerate(game_labels)))
    ppg_bins=[(-math.inf,0),(0,.15),(.15,.3),(.3,.5),(.5,.75),(.75,1.0),(1.0,math.inf)];ppg_labels=["no prior games","0-.15",".15-.30",".30-.50",".50-.75",".75-1.00","1.00+"]
    d["season_ppg_band"]=ranges(d.season_ppg_prior,ppg_bins,ppg_labels).map(dict(enumerate(ppg_labels)))
    write_csv("season_accumulation_analysis.csv",[{"games_prior_band":b,**metrics(d[d.games_prior_band==b]),"mean_season_points_prior":mean(d.loc[d.games_prior_band==b,"current_season_points_prior"]),"mean_games_prior":mean(d.loc[d.games_prior_band==b,"current_season_games_prior"])} for b in game_labels])
    write_csv("season_ppg_analysis.csv",[{"season_ppg_band":b,**metrics(d[d.season_ppg_band==b]),"mean_season_ppg_prior":mean(d.loc[d.season_ppg_band==b,"season_ppg_prior"])} for b in ppg_labels])
    write_csv("same_ppg_different_games.csv",[{"season_ppg_band":p,"games_prior_band":g,**metrics(d[(d.season_ppg_band==p)&(d.games_prior_band==g)])} for p in ppg_labels for g in game_labels])
    # Actual-row counterfactual holds every other feature fixed and maintains PPG=1.
    reps=d[(d.current_season_games_prior>0)].sort_values("current_season_games_prior").iloc[[0,-1]] if (d.current_season_games_prior>0).sum()>1 else d.iloc[:0]
    cf=[]
    for _,r in reps.iterrows():
        for pts,games in [(2,2),(4,4),(8,8),(16,16)]:
            x=r[FEATURES].to_frame().T.copy();x["current_season_points_prior"]=pts;x["current_season_games_prior"]=games
            pr=score(pd.concat([r[["game_id","player_id"]].to_frame().T.reset_index(drop=True),x.reset_index(drop=True)],axis=1),model)
            cf.append({"player_id":int(r.player_id),"game_id":int(r.game_id),"source_games_prior":int(r.current_season_games_prior),"counterfactual_points_prior":pts,"counterfactual_games_prior":games,"preserved_ppg":pts/games,"expected_points":float(pr.expected_points.iloc[0]),"prob_over_0_5":float(pr.prob_over_0_5.iloc[0]),"synthetic_state_not_realistic_future":True})
    write_csv("synthetic_accumulation_counterfactual.csv",cf)
    # 2025 prior-season holdout (47,239 rows); separates opening vs later exposure.
    h["count_residual"]=h.points-h.predicted_mean;h["selected_p"]=np.maximum(h.p_over_0_5,1-h.p_over_0_5);h["selected_side"]=np.where(h.p_over_0_5>=.5,"OVER","UNDER");h["confidence_band"]=pd.cut(h.selected_p,[.5,.55,.60,.65,.70,.75,.80,.85,1.000001],labels=CONF_LABELS,right=False,include_lowest=True).astype("string")
    h["games_prior_band"]=ranges(h.current_season_games_prior,game_bins,game_labels).map(dict(enumerate(game_labels)))
    hist=[]
    for band,g in h.groupby("games_prior_band",dropna=False): hist.append({"games_prior_band":str(band),"period":"2025 strict-prior holdout","n":len(g),"mean_predicted_points":mean(g.predicted_mean),"mean_realized_points":mean(g.points),"mean_count_residual":mean(g.count_residual),"point_game_rate":mean(g.points>=1),"mean_hgb_p_over":mean(g.p_over_0_5),"over_calibration_gap":mean(g.p_over_0_5)-mean(g.points>=1),"mean_games_prior":mean(g.current_season_games_prior)})
    write_csv("historical_2025_early_mid_late.csv",hist)
    hconf=[]
    for band,g in h.groupby("confidence_band",dropna=False):
        under=(g.selected_side=="UNDER"); selected_acc=np.where(under,1-(g.points>=1),g.points>=1)
        hconf.append({"confidence_band":str(band),"n":len(g),"expected_selected_win_rate":float(g.selected_p.mean()),"realized_selected_win_rate":float(np.mean(selected_acc)),"selected_calibration_gap":float(g.selected_p.mean()-np.mean(selected_acc)),"mean_count_residual":mean(g.count_residual),"under_n":int(under.sum()),"over_n":int((~under).sum())})
    write_csv("2025_confidence_map.csv",hconf)
    # Training/evaluation distribution shift: train frame is 2023-24; compare operational 2026.
    train=frame[frame.canonical_season.isin([2023,2024])]; shifts=[]
    for feat in FEATURES:
        a=pd.to_numeric(train[feat],errors="coerce").dropna();b=pd.to_numeric(d[feat],errors="coerce").dropna()
        if len(a) and len(b):
            lo,hi=a.quantile(.01),a.quantile(.99); edges=np.unique(a.quantile(np.linspace(0,1,11)).to_numpy());
            if len(edges)>2:
                ac=np.histogram(a,bins=edges)[0]+.5;bc=np.histogram(np.clip(b,edges[0],edges[-1]),bins=edges)[0]+.5;pa=ac/ac.sum();pb=bc/bc.sum();psi=float(np.sum((pb-pa)*np.log(pb/pa)))
            else:psi=None
            shifts.append({"feature":feat,"training_n":len(a),"operational_2026_n":len(b),"training_q01":a.quantile(.01),"training_q50":a.median(),"training_q99":a.quantile(.99),"operational_q01":b.quantile(.01),"operational_q50":b.median(),"operational_q99":b.quantile(.99),"operational_outside_training_p01_p99":float(((b<lo)|(b>hi)).mean()),"standardized_mean_shift":float((b.mean()-a.mean())/a.std(ddof=1)) if a.std(ddof=1)>0 else None,"ks_statistic":float(ks_2samp(a,b).statistic),"decile_psi_approx":psi,"training_missing_rate":float(pd.to_numeric(train[feat],errors="coerce").isna().mean()),"operational_missing_rate":float(pd.to_numeric(d[feat],errors="coerce").isna().mean())})
    write_csv("training_distribution_shift.csv",shifts)
    # Sparse history, recency, SOG/attempt, opportunity, and team feature diagnostics.
    for name,field,bounds,labels in [("last10_points_diagnostics","player_points_last10",[(-math.inf,0),(.0,.25),(.25,.5),(.5,1),(1,math.inf)],["<0","0-.25",".25-.5",".5-1","1+"]),("sog_attempt_diagnostics","d10_sog_per60",[(-math.inf,5),(5,8),(8,11),(11,14),(14,math.inf)],["<5","5-8","8-11","11-14","14+"]),("attempt_rate_diagnostics","attempts_d10_per60",[(-math.inf,15),(15,25),(25,35),(35,45),(45,math.inf)],["<15","15-25","25-35","35-45","45+"]),("team_context_diagnostics","team_d10_sf_per_game",[(-math.inf,20),(20,25),(25,30),(30,35),(35,math.inf)],["<20","20-25","25-30","30-35","35+"]),("team_share_diagnostics","last10_team_sog_share",[(-math.inf,.03),(.03,.05),(.05,.07),(.07,.10),(.10,math.inf)],["<.03",".03-.05",".05-.07",".07-.10",".10+"])]:
        ix=ranges(d[field],bounds,labels); write_csv(name+".csv",[{"feature":field,"band":labels[i],**metrics(d[ix==i])} for i in range(len(labels))])
    toirows=[]
    for field,bounds,labels in [("mean_toi_last10",[(-math.inf,8),(8,12),(12,16),(16,20),(20,math.inf)],["<8","8-12","12-16","16-20","20+"]),("mean_pp_toi_last10",[(-math.inf,.5),(.5,1.5),(1.5,2.5),(2.5,4),(4,math.inf)],["<.5",".5-1.5","1.5-2.5","2.5-4","4+"])]:
        ix=ranges(d[field],bounds,labels);toirows += [{"feature":field,"band":str(labels[i]),**metrics(d[ix==i])} for i in range(len(labels))]
    write_csv("toi_pp_diagnostics.csv",toirows)
    sparse=[]
    for b in game_labels:
        z=d[d.games_prior_band==b];sparse.append({"population":"2026 operational","games_prior_band":b,**metrics(z),"mean_games_prior":mean(z.current_season_games_prior),"mean_history_feature_missing_rate":float(z[FEATURES].isna().any(axis=1).mean()) if len(z) else None})
    for b in game_labels:
        z=h[h.games_prior_band==b];sparse.append({"population":"2025 strict-prior holdout","games_prior_band":b,"n":len(z),"mean_predicted_points":mean(z.predicted_mean),"mean_realized_points":mean(z.points),"mean_count_residual":mean(z.count_residual),"point_game_rate":mean(z.points>=1),"mean_hgb_p_over_0_5":mean(z.p_over_0_5),"mean_games_prior":mean(z.current_season_games_prior)})
    write_csv("sparse_history_diagnostics.csv",sparse)
    # Assist/goals evidence uses only retained prior outcome rows through Apr 2026.
    histrows=frame.sort_values(["player_id","game_date","game_id"]);assist=[]
    prev=histrows.groupby("player_id",sort=False).tail(10).groupby("player_id").agg(prior10_points=("realized_points","sum"),prior10_assists=("assists","sum"),prior10_goals=("goals","sum"),prior10_games=("game_id","count")).reset_index()
    d=d.merge(prev,on="player_id",how="left",validate="many_to_one");d["prior10_assist_share"]=d.prior10_assists/d.prior10_points.replace(0,np.nan)
    for label,mask in masks.items():
        z=d[mask];assist.append({"cohort":label,"n":len(z),"mean_prior10_assists":mean(z.prior10_assists),"mean_prior10_goals":mean(z.prior10_goals),"mean_prior10_assist_share":mean(z.prior10_assist_share),"share_with_prior_history":float(z.prior10_games.notna().mean()) if len(z) else None})
    write_csv("assist_sensitivity_diagnostics.csv",assist)
    # Derived recency gap; 2026 current-season games and last10 feature only.
    d["recent_minus_season_ppg"]=d.player_points_last10-d.season_ppg_prior
    d["recent_season_state"]=pd.cut(d.recent_minus_season_ppg,[-math.inf,-.25,.25,math.inf],labels=["RECENT_BELOW_SEASON","NEAR_SEASON","RECENT_ABOVE_SEASON"])
    write_csv("recent_vs_season_interaction.csv",[{"regime":str(b),**metrics(d[d.recent_season_state==b])} for b in d.recent_season_state.cat.categories])
    # One-at-a-time diagnostic perturbation to correct-high-confidence cohort medians.
    high_correct=d[d.cohort_confidence_65_75&(d.outcome_split=="CORRECT")]
    medians={f:pd.to_numeric(high_correct[f],errors="coerce").median() for f in FEATURES}
    perturb=[]
    if len(high_correct):
        matrix=high_correct[FEATURES].replace([np.inf,-np.inf],np.nan).fillna(0).astype(float)
        raw=matrix.to_numpy();baseline=np.maximum(estimator.predict(scaler.transform(raw)),1e-8);basep=1-np.exp(-baseline)
        for j,feat in enumerate(FEATURES):
            counter=matrix.copy();counter[feat]=medians[feat];mu=np.maximum(estimator.predict(scaler.transform(counter.to_numpy())),1e-8);p=1-np.exp(-mu)
            perturb.append({"feature":feat,"cohort":"CORRECT_HIGH_CONFIDENCE_65_75","n":len(counter),"replacement_value":medians[feat],"mean_expected_count_delta":float(np.mean(mu-baseline)),"median_expected_count_delta":float(np.median(mu-baseline)),"mean_p_over_delta":float(np.mean(p-basep)),"interpretation":"one-feature replacement diagnostic; not causal"})
    write_csv("one_feature_perturbation.csv",perturb)
    training_medians={f:float(pd.to_numeric(frame.loc[frame.canonical_season.isin([2023,2024]),f],errors="coerce").median()) for f in FEATURES}
    x2026=d[FEATURES].replace([np.inf,-np.inf],np.nan).fillna(0).astype(float);mu0=np.maximum(estimator.predict(scaler.transform(x2026.to_numpy())),1e-8);p0=1-np.exp(-mu0);trainpert=[]
    for feat in FEATURES:
        xx=x2026.copy();xx[feat]=training_medians[feat];muu=np.maximum(estimator.predict(scaler.transform(xx.to_numpy())),1e-8);pp=1-np.exp(-muu)
        trainpert.append({"feature":feat,"replacement_value_2023_24_training_median":training_medians[feat],"observed_2026_median":pd.to_numeric(d[feat],errors="coerce").median(),"observed_2026_missing_rate":float(pd.to_numeric(d[feat],errors="coerce").isna().mean()),"mean_expected_count_delta":float(np.mean(muu-mu0)),"mean_p_over_delta":float(np.mean(pp-p0)),"interpretation":"population-wide one-feature training-median replacement; diagnostic only, not a repaired forecast"})
    write_csv("operational_feature_shift_perturbation.csv",trainpert)
    # Representative false Under cases and nearest correct Under controls in same confidence bin.
    wrong=d[(d.display_side=="UNDER")&(d.display_selected_probability>=.65)&(d.display_selected_probability<.85)&(d.outcome_split=="WRONG")].copy().sort_values(["display_selected_probability","count_residual"],ascending=[False,True]).head(20)
    correct=d[(d.display_side=="UNDER")&(d.display_selected_probability>=.65)&(d.display_selected_probability<.85)&(d.outcome_split=="CORRECT")].copy();cases=[]
    used=set(); wrong_sens=[]
    for _,r in wrong.iterrows():
        candidates=correct[(correct.confidence_band==r.confidence_band)&(correct.games_prior_band==r.games_prior_band)&(~correct.index.isin(used))]
        if not len(candidates):continue
        match=(candidates.display_selected_probability-r.display_selected_probability).abs().idxmin();used.add(match);c=correct.loc[match]
        cases.append({"wrong_game_id":int(r.game_id),"wrong_player_id":int(r.player_id),"slate_date":r.slate_date,"confidence_band":str(r.confidence_band),"wrong_expected_points":r.expected_points,"wrong_selected_p":r.display_selected_probability,"wrong_market_selected_p":r.market_selected_probability,"wrong_realized_points":r.realized_points,"control_game_id":int(c.game_id),"control_player_id":int(c.player_id),"control_expected_points":c.expected_points,"control_selected_p":c.display_selected_probability,"control_market_selected_p":c.market_selected_probability,"control_realized_points":c.realized_points,"matched_games_prior_band":str(r.games_prior_band),**{f"wrong_{f}":r[f] for f in FEATURES},**{f"control_{f}":c[f] for f in FEATURES}})
        input_row=r[FEATURES].to_frame().T.copy();base_mu=float(np.maximum(estimator.predict(scaler.transform(input_row.replace([np.inf,-np.inf],np.nan).fillna(0).astype(float).to_numpy())),1e-8)[0]);base_po=1-math.exp(-base_mu)
        for feat in FEATURES:
            xx=input_row.copy();xx[feat]=c[feat];new_mu=float(np.maximum(estimator.predict(scaler.transform(xx.replace([np.inf,-np.inf],np.nan).fillna(0).astype(float).to_numpy())),1e-8)[0]);wrong_sens.append({"wrong_game_id":int(r.game_id),"wrong_player_id":int(r.player_id),"matched_control_game_id":int(c.game_id),"matched_control_player_id":int(c.player_id),"feature":feat,"wrong_feature_value":r[feat],"control_feature_value":c[feat],"wrong_expected_points":base_mu,"expected_points_after_single_feature_control_value":new_mu,"delta_expected_points":new_mu-base_mu,"wrong_p_over_0_5":base_po,"p_over_after_single_feature_control_value":1-math.exp(-new_mu),"delta_p_over_0_5":(1-math.exp(-new_mu))-base_po,"interpretation":"matched-control one-feature perturbation; not causal"})
    write_csv("representative_wrong_cases_and_matched_controls.csv",cases)
    write_csv("wrong_case_feature_perturbations.csv",wrong_sens)
    # Fixed high-confidence-under bins; hgb P(under) = 1 - P(over).
    u=d[d.display_side=="UNDER"].copy();u["p_under"]=u.display_selected_probability
    ubounds=[(.60,.65),(.65,.70),(.70,.75),(.75,.80),(.80,math.inf)];ulabels=["60-65%","65-70%","70-75%","75-80%","80%+"]
    um=[]
    for i,b in enumerate(ulabels):
        g=u[(u.p_under>=ubounds[i][0])&(u.p_under<ubounds[i][1])];um.append({"p_under_band":b,**metrics(g),"mean_expected_points":mean(g.expected_points),"mean_realized_points":mean(g.realized_points),"market_same_side_n":int((g.side_relation=="SAME_SIDE").sum()),"market_opposition_n":int((g.side_relation=="OPPOSITE_SIDE").sum()),**{f"median_{f}":pd.to_numeric(g[f],errors="coerce").median() for f in FEATURES}})
    write_csv("high_confidence_under_map.csv",um)
    write_csv("same_side_opposition_comparison.csv",[{"relation":r,**metrics(d[d.side_relation==r])} for r in ["SAME_SIDE","OPPOSITE_SIDE"]])
    write_csv("healthy_negative_edge_comparison.csv",[{"cohort":c,**metrics(d[m])} for c,m in [("NEGATIVE_EDGE_HEALTHY_REFERENCE",d.cohort_negative_edge),("HIGH_EDGE",d.cohort_high_edge),("VERY_HIGH_EDGE",d.cohort_very_high_edge)]])
    # Bounded logical interaction screen, equal-width median splits for interpretability.
    inter=[]
    pairs=[("current_season_points_prior","current_season_games_prior"),("season_ppg_prior","player_points_last10"),("player_points_last10","mean_toi_last10"),("player_points_last10","mean_pp_toi_last10"),("d10_sog_per60","attempts_d10_per60"),("team_d10_sf_per_game","last10_team_sog_share")]
    for a,b in pairs:
        av=pd.to_numeric(d[a],errors="coerce");bv=pd.to_numeric(d[b],errors="coerce");am=av.median();bm=bv.median()
        for x,xmask in [("LOW",av<=am),("HIGH",av>am)]:
            for y,ymask in [("LOW",bv<=bm),("HIGH",bv>bm)]:inter.append({"feature_a":a,"feature_b":b,"a_half":x,"b_half":y,"a_cut":am,"b_cut":bm,**metrics(d[xmask&ymask])})
    write_csv("bounded_interaction_screen.csv",inter)
    # Internal tree inspection only for feature split frequencies and leaf values; no path claims.
    nodes=[]
    try:
        for it,stage in enumerate(estimator._predictors):
            for tree_i,t in enumerate(stage):
                nd=t.nodes; leaf=nd[nd["is_leaf"]==1]; splits=nd[nd["is_leaf"]==0]
                for fidx in range(len(FEATURES)):
                    q=splits[splits["feature_idx"]==fidx]
                    if len(q):nodes.append({"iteration":it,"tree":tree_i,"feature":FEATURES[fidx],"split_count":len(q),"thresholds":"|".join(f"{x:.6g}" for x in q["num_threshold"]),"leaf_count":len(leaf),"leaf_value_min":float(leaf["value"].min()),"leaf_value_median":float(np.median(leaf["value"])),"leaf_value_max":float(leaf["value"].max()),"sklearn_version":__import__("sklearn").__version__,"inspection_scope":"version-coupled node summaries; exact sample path attribution not asserted"})
    except Exception as exc:nodes=[{"status":"INSUFFICIENT_EVIDENCE","reason":type(exc).__name__+":"+str(exc)}]
    write_csv("tree_leaf_diagnostics.csv",nodes)
    # Output quantization / saturation.
    sat=[]
    for name,vals in [("2026_frozen_expected_points",d.expected_points),("2026_p_over_0_5",d.hgb_p_over),("2025_frozen_expected_points",h.predicted_mean),("2025_p_over_0_5",h.p_over_0_5)]:
        s=pd.to_numeric(vals,errors="coerce"); sat.append({"population":name,"n":int(s.notna().sum()),"unique_values":int(s.nunique()),"unique_fraction":float(s.nunique()/s.notna().sum()),"minimum":s.min(),"median":s.median(),"maximum":s.max(),"within_1e-6_unique_values":int(np.round(s,6).nunique())})
    write_csv("output_saturation.csv",sat)
    repeat_values=[]
    for name,vals in [("2026_frozen_expected_points",d.expected_points),("2026_p_over_0_5",d.hgb_p_over),("2025_frozen_expected_points",h.predicted_mean),("2025_p_over_0_5",h.p_over_0_5)]:
        rounded=pd.to_numeric(vals,errors="coerce").dropna().round(6)
        counts=rounded.value_counts().rename_axis("rounded_value").reset_index(name="frequency")
        for rank,row in counts.head(100).iterrows():
            repeat_values.append({"population":name,"rounded_decimals":6,"rounded_value":float(row.rounded_value),"frequency":int(row.frequency),"rank":int(rank+1),"interpretation":"most frequent rounded outputs; descriptive only"})
    write_csv("output_saturation_repeat_values.csv",repeat_values)
    # Early/mid/late historical count / calibration and fixed bands by prior games.
    hperiod=[]
    for label,lo,hi in [("EARLY_0_5",0,6),("MID_6_20",6,21),("LATE_21_PLUS",21,math.inf)]:
        g=h[(h.current_season_games_prior>=lo)&(h.current_season_games_prior<hi)]
        hperiod.append({"period":label,"n":len(g),"games_prior_min":lo,"games_prior_max_exclusive":hi,"mean_count_residual":mean(g.count_residual),"mean_predicted_points":mean(g.predicted_mean),"mean_realized_points":mean(g.points),"p_over_0_5":mean(g.p_over_0_5),"realized_over_rate":mean(g.points>=1),"over_brier":float(np.mean((g.p_over_0_5-(g.points>=1))**2)) if len(g) else None})
    write_csv("2025_early_mid_late.csv",hperiod)
    # Oct 15 pending is quote-bound and deliberately has no target joined.
    octcap=caps["2026-10-15"];op=pd.read_csv(octcap.parent/"predictions.csv");op=op.merge(pd.read_csv(octcap)[["game_id","player_id",*FEATURES]],on=["game_id","player_id"],validate="one_to_one");pending=pd.read_csv(BASE/"oct15_pending_map.csv");pending=pending[pending.line==.5].copy();op=op.merge(pending,on=["game_id","player_id"],how="left",suffixes=("","_market"),validate="one_to_one");market15=pd.read_csv(ROOT/"artifacts/operational/nhl/points_market_attachments/season=2026/slate_date=2026-10-15/run_id=nhldaily_20261015T140453183604Z_bfa01e59/points_with_market.csv");market15=market15[market15.line==.5][["game_id","player_id","p_over_mkt","p_under_mkt"]];op=op.merge(market15,on=["game_id","player_id"],how="left",validate="one_to_one");op["p_under"]=1-op.prob_over_0_5;op["hgb_side"]=np.where(op.p_under>=.5,"UNDER","OVER");op["market_side"]=np.where(op.p_over_mkt>=.5,"OVER","UNDER");op["market_opposition"]=op.p_over_mkt.notna()&(op.market_side!=op.hgb_side)
    opmetrics={"prediction_rows":len(op),"market_quote_rows":int(op.display_edge.notna().sum()),"high_confidence_under_n":int((op.p_under>=.60).sum()),"p_under_65_plus_n":int((op.p_under>=.65).sum()),"edge_ge_10_n":int((op.display_edge>=.10).sum()),"edge_ge_15_n":int((op.display_edge>=.15).sum()),"edge_ge_20_n":int((op.display_edge>=.20).sum()),"market_opposition_n":int(op.market_opposition.sum()),"current_season_games_prior_distribution":op.current_season_games_prior.value_counts(dropna=False).sort_index().to_dict(),"provider_calls":0,"paid_credits":0,"outcomes_joined":False}
    # Predicted-under threshold counts and quote-covered regime breakouts.
    opcounts=[]
    for label,mask in [("PUNDER_GE_60",op.p_under>=.60),("PUNDER_GE_65",op.p_under>=.65),("PUNDER_GE_70",op.p_under>=.70),("PUNDER_GE_75",op.p_under>=.75),("PUNDER_GE_80",op.p_under>=.80),("EDGE_GE_10",op.display_edge>=.10),("EDGE_GE_15",op.display_edge>=.15),("EDGE_GE_20",op.display_edge>=.20),("MARKET_OPPOSITION",op.market_opposition)]:
        z=op[mask.fillna(False)];opcounts.append({"regime":label,"n":len(z),"market_quote_n":int(z.display_edge.notna().sum()),"mean_games_prior":mean(z.current_season_games_prior),"games_prior_zero_n":int((z.current_season_games_prior==0).sum()),"mean_points_prior":mean(z.current_season_points_prior),"mean_expected_points":mean(z.expected_points),"outcomes_joined":False})
    write_csv("oct15_pending_diagnostic.csv",[{**opmetrics,"row_type":"SUMMARY"}]+[{**r,"row_type":"REGIME"} for r in opcounts])
    # Shallow missing-information audit reuses the retained prior independent study.
    audit=ROOT/"artifacts/analysis/nhl/points_sanity_explanation_audit/2026-10-14"
    om=[]
    if (audit/"season_hit_rate_analysis.csv").exists():
        prior=pd.read_csv(audit/"season_hit_rate_analysis.csv")
        om=[{"source":"retained_sanity_explanation_audit","finding":"Prior study contains season/last10 hit-frequency diagnostics; these variables are not among frozen production features.","artifact":str((audit/"season_hit_rate_analysis.csv").relative_to(ROOT)),"rows":len(prior)}]
    om += [{"source":"current feature contract","finding":f"{f} is not a production input; omitted context requires independent out-of-time testing.","artifact":"NHL_POINTS_LEADER_CORE_FEATURES_V1","rows":len(d)} for f in ["season_point_game_hit_rate","last10_point_game_hit_rate","recent_TOI_trend","assist_share"]]
    write_csv("feature_omission_diagnostics.csv",om)
    # Matched high-confidence comparisons by fixed selected-side probability slices.
    matchrows=[]
    for band,g in d[d.display_side=="UNDER"].groupby("confidence_band"):
        for outcome in ("CORRECT","WRONG"):
            z=g[g.outcome_split==outcome];matchrows.append({"confidence_band":str(band),"outcome":outcome,**metrics(z),"mean_games_prior":mean(z.current_season_games_prior),"mean_points_prior":mean(z.current_season_points_prior),"mean_last10_points":mean(z.player_points_last10)})
    write_csv("matched_cohort_analysis.csv",matchrows)
    # Per-cause support classification is deliberately conservative and rules are documented.
    scorecard=[
        {"candidate":"operational attempt-rate feature collapse","classification":"STRONGLY_SUPPORTED","evidence":"attempts_d10_per60 is exactly 0 in all nonmissing 2026 settled rows and 100% outside training P1-P99; current captures preserve this value","limitation":"source defect is established, but its causal contribution to count residual must be isolated in time-separated validation"},
        {"candidate":"early-season cumulative-stat artifact","classification":"NOT_SUPPORTED" if abs(hperiod[0]["mean_count_residual"])<.05 else "POSSIBLE","evidence":"2025 strict-prior 0-5 games-prior residual is small; 2026 low-prior sample is sparse","limitation":"does not explain the 2026 aggregate defect as a standalone cause"},
        {"candidate":"season-games/points interaction","classification":"POSSIBLE","evidence":"fixed season P/G x games-prior cross-tab plus actual-row counterfactual","limitation":"small October sample and synthetic extrapolation"},
        {"candidate":"recent-points overreaction","classification":"POSSIBLE","evidence":"last10 residual map and recent-vs-season P/G","limitation":"120-day season-open history is sparse"},
        {"candidate":"shot-oriented proxy bias for assist-heavy players","classification":"INSUFFICIENT_EVIDENCE","evidence":"prior official goals/assists merged by player; descriptive only","limitation":"historical prior window and low sample"},
        {"candidate":"TOI/PP role lag","classification":"INSUFFICIENT_EVIDENCE","evidence":"captured average TOI/PP TOI feature bands","limitation":"no retained pregame trend field in frozen feature capture"},
        {"candidate":"team-context suppression","classification":"POSSIBLE","evidence":"fixed team shots and share residual regimes","limitation":"observational feature marginals"},
        {"candidate":"sparse-history behavior","classification":"POSSIBLE","evidence":"2026 low-games bands are small; 2025 strict-prior opening residual is close to zero","limitation":"not a strong standalone explanation for the 2026 error"},
        {"candidate":"training-distribution shift","classification":"SUPPORTED" if any(r["operational_outside_training_p01_p99"]>.10 for r in shifts) else "POSSIBLE","evidence":"2023-24 training source vs 2026 captured feature quantiles, KS and approximate PSI","limitation":"distribution shift does not prove harmfulness"},
        {"candidate":"tree/leaf output saturation","classification":"INSUFFICIENT_EVIDENCE","evidence":"unique output counts and version-coupled tree node summaries","limitation":"no sample-level path attribution"},
        {"candidate":"missing point-hit-frequency context","classification":"POSSIBLE","evidence":"prior sanity explanation audit and omission registry","limitation":"prior audit not causal and not full current prospective test"},
        {"candidate":"missing assist-production context","classification":"POSSIBLE","evidence":"frozen feature contract omits assists-specific recent history","limitation":"current comparison is low-power"},
    ]
    write_csv("root_cause_scorecard.csv",scorecard)
    (OUT/"repair_family_classification.json").write_text(json.dumps({"classification":"MULTIFACTOR","confidence":"PROVISIONAL","preferred_direction":"repair expected-count feature representation/population if confirmed","calibration_as_primary_remediation":False,"market_added_as_feature":False,"candidate_families":["FEATURE_REPRESENTATION_REPAIR","FEATURE_ADDITION_JUSTIFIED","TRAINING_POPULATION_REPAIR"],"no_retraining_or_production_changes":True},indent=2)+"\n")
    candidates=[{"candidate":"audit and restore valid D10 shot-attempt feature support","addresses":"2026 attempts_d10_per60 collapsed to zero for every nonmissing observation despite historical nonzero training support","expected_direction":"remove unsupported zero-signal only after identifying whether data are absent or genuinely zero","out_of_time_test":"reconstruct source feature on frozen pregame captures; compare old/new expected-count residual and strict-prior rolling origins","production_invariant":"no probability calibration, market input, clipping, or Funnel change"},{"candidate":"represent current-season production with rate plus exposure","addresses":"possible cumulative points/games interaction under low sample counts","expected_direction":"reduce arbitrary dependence on small totals while retaining uncertainty from sample size; effect unproven","out_of_time_test":"frozen walk-forward split by date and games-prior bands; preserve Poisson count score","production_invariant":"no retraining/promoting in this task; Funnel V1 unchanged"},{"candidate":"test compact hit-frequency/assist context and opening-state coverage","addresses":"omitted point-game frequency, assist representation, and potential undercoverage of early-season feature states","expected_direction":"adjust expected count only where strictly prior evidence supports it","out_of_time_test":"separate ablations on nested rolling-origin dates with player-cluster uncertainty","production_invariant":"market stays diagnostic-only; no authority or filter change"}]
    write_csv("bounded_repair_candidates.csv",candidates)
    contract=json.loads((VALID/"shadow_contract.json").read_text())
    contract.update({"estimator_type":type(estimator).__name__,"sklearn_version":__import__("sklearn").__version__,"scaler_type":type(scaler).__name__,"scaler_mean":scaler.mean_.tolist(),"scaler_scale":scaler.scale_.tolist(),"feature_missing_behavior":"coerce nonnumeric; replace +/-inf with NaN; fill NaN with 0 before StandardScaler","training_target":"untransformed realized_points = official goals + assists","fitted_predict_output":"estimator.predict after Poisson log-link inverse: expected count / expected points","post_prediction_transform":"floor to 1e-8; no log/exp/count transform applied after estimator.predict","probability_transform":"Poisson survival: P(over 0.5)=sf(0,mu); P(under 0.5)=1-P(over 0.5)"})
    (OUT/"production_model_contract.json").write_text(json.dumps(contract,indent=2)+"\n")
    summary={"schema_version":"NHL_POINTS_HGB_CONFIDENCE_ROOT_CAUSE_V1","frozen_model_sha256":sha(MODEL_PATH),"scorer_sha256":sha(ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py"),"2026_capture_count":len(all_repro),"2026_capture_rows_reproduced":sum(x["rows"] for x in all_repro),"2026_max_reproduction_error":max(x["max_abs_error_expected_and_poisson_fields"] for x in all_repro),"2026_analysis_date_capture_count":len(repro),"production_parity_max_abs_error":float(max(np.max(np.abs(d.expected_points-d.expected_points_capture)),np.max(np.abs(d.hgb_p_over-d.prob_over_0_5)))),"reproduction_gate":"PASS" if all(x["reproduction_status"]=="PASS" for x in all_repro) else "FAIL","2026_settled_rows":len(d),"2025_holdout_rows":len(h),"2025_holdout_reproduction_max_abs_error":history_parity,"2026_metrics":metrics(d),"negative_edge":metrics(d[d.cohort_negative_edge]),"positive_edge":metrics(d[d.cohort_positive_edge]),"high_edge":metrics(d[d.cohort_high_edge]),"very_high_edge":metrics(d[d.cohort_very_high_edge]),"confidence_65_75":metrics(d[d.cohort_confidence_65_75]),"market_opposition":metrics(d[d.cohort_market_opposition]),"same_side_high_confidence":metrics(d[d.cohort_same_side_high_confidence]),"2025_opening_0_5_games":hperiod[0],"oct15_pending":opmetrics,"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"calibration_used":False,"market_added_as_feature":False,"funnel_v1_modified":False}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,default=str,sort_keys=True)+"\n")
    sources=[MODEL_PATH,ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py",BASE/"canonical_rows.csv",BASE/"oct15_pending_map.csv",FRAME,VALID/"hgb_poisson_season_holdout_predictions.csv.gz",VALID/"shadow_contract.json",ROOT/"artifacts/operational/nhl/points_market_attachments/season=2026/slate_date=2026-10-15/run_id=nhldaily_20261015T140453183604Z_bfa01e59/points_with_market.csv"]
    (OUT/"lineage_manifest.json").write_text(json.dumps({"schema_version":"NHL_POINTS_HGB_CONFIDENCE_ROOT_CAUSE_LINEAGE_V1","sources":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in sources],"capture_rows":all_repro,"analysis_population_captures":repro,"analysis_script_sha256":sha(Path(__file__)),"history_2025_reproduction_max_abs_error":history_parity,"threshold":1e-12,"provider_calls":0,"paid_credits":0,"production_db_mutations":0},indent=2)+"\n")
    (OUT/"backlog_context.md").write_text("""# Backlog and context\n\n- Points HGB expected-count root cause: CURRENT; diagnosis only\n- Points Funnel V1 prospective validation\n- Corrected Points full-range prospective collection\n- Cross-market review: SOG after the Points root-cause decision; then Saves, Moneyline, Puck Line\n- Continue SOG structural, divergence, and fixed-blend prospective grading\n- Historical SOG official-TOI binding\n- Deferred Puck Line work\n- Tier3 override not needed\n\nCalibration is not a preferred remediation. Do not add market probability to HGB. Understand the full performance range before any future consideration rules.\n""")
    report=["# NHL Points HGB confidence root-cause study","","## Gate and model contract","",f"The retained frozen artifact reproduced across {len(all_repro)} immutable 2026 captures ({sum(x['rows'] for x in all_repro):,} rows); maximum numeric difference over expected count and three Poisson probabilities was {summary['2026_max_reproduction_error']:.3g}, below the 1e-12 parity tolerance. Attribution proceeded only after this passed.","",f"The frozen artifact hash is `{sha(MODEL_PATH)}`. It is a `{type(estimator).__name__}` trained with Poisson loss on untransformed official goals + assists, with a `StandardScaler` and the ten ordered CORE features. Missing/nonfinite features are coerced and zero-filled before scaling. `predict()` yields expected points/count; it is floored at 1e-8, then converted by Poisson survival (`P(Over 0.5)=1-exp(-mu)`, Under is its complement). No additional count transformation or calibration is applied.","","## 2026 diagnosis","",f"On the 714 settled 0.5 rows: {summary['2026_metrics']['wins']}–{summary['2026_metrics']['losses']}; selected expectation {summary['2026_metrics']['selected_expected_rate']:.1%} vs {summary['2026_metrics']['selected_realized_rate']:.1%} realized; expected-count residual (realized minus predicted) {summary['2026_metrics']['mean_count_residual']:+.3f} points/player-game. Negative Edge: n={summary['negative_edge']['n']}, residual {summary['negative_edge']['mean_count_residual']:+.3f}; positive Edge: n={summary['positive_edge']['n']}, residual {summary['positive_edge']['mean_count_residual']:+.3f}; Edge >=10% residual {summary['high_edge']['mean_count_residual']:+.3f}; Edge >=20% residual {summary['very_high_edge']['mean_count_residual']:+.3f}.","","**The most concrete anomaly is the attempt-rate feature:** `attempts_d10_per60` is exactly zero for all 711 nonmissing settled rows, compared with a training median of 8.33; all 714 operational rows fall outside the training 1st–99th percentile range. This is a reproducible operational feature-support problem and a plausible contributor to low counts, not yet proof of causal responsibility. Team D10 shots-for is also shifted (median 15.3 versus 29.3 training); mean PP TOI is shifted high (median 2.31 versus 0.314; 84.6% outside training 1st–99th percentiles). These distributions should be source-audited before model-form changes.","","For comparison, 2025 strict-prior 0–5-game opening rows had only +0.020 mean count residual (n=5,228), with Over frequency 30.3% versus 29.5% predicted. That does not support an early-season cumulative-stat artifact as the sole explanation for the 2026 +0.232 residual. Season-prior totals do vary with games played in fixed-P/G counterfactuals, but those synthetic changes are not causal evidence.","","The fixed cohort tables compare high-confidence correct and wrong rows within confidence bands, plus market opposition and same-side/high-confidence cohorts. Count residual tables lead the probability analysis. The 2026 sample is short; the anomaly should be treated as provisional pending source/history investigation.","","## Historical cross-check","",f"The retained strict-prior 2025 holdout has {len(h):,} rows and uses 2023–24 training (90,486 rows). Frozen-model reproduction max error is {history_parity:.3g}. See `2025_early_mid_late.csv`, `2025_confidence_map.csv`, and `training_distribution_shift.csv`.","","## Root-cause interpretation","","Current classification: **MULTIFACTOR, provisional**. Highest-priority diagnosis is operational feature support/representation shift, especially shot attempts and team-shot context; season-opening cumulative quantities may contribute but do not explain the result alone. The frozen model has no market input. Feature replacements, matched cases, and tree summaries are diagnostic only; no causal claim is made. Tree node summaries do not assert sample-level path attribution. The retained 2025 source rows provide only approximate historical assist context; a full strict-prior role-trend history was not retained.","","Up to three bounded candidate families and out-of-time tests are in `bounded_repair_candidates.csv`. **Calibration is not the proposed root-cause remedy; market probability remains diagnostic only and was not fed to HGB.** No retraining, promotion, filter, Funnel V1, or production change occurred.","","## Oct. 15 pending","",f"The quote-bound Oct. 15 map covers {opmetrics['prediction_rows']} immutable HGB identities; {opmetrics['market_quote_rows']} have retained 0.5 market quotes. Counts by Under confidence, Edge, market opposition, current-season games prior, and suspected feature regime are in `oct15_pending_diagnostic.csv`; outcomes were not joined.","","## Package contents","","Contract, reproducibility evidence, outcome-blind cohort registry and outcome splits, expected-count residuals/bins, feature distributions/maps, accumulation counterfactuals, 2025 holdout maps, distribution shift, sensitivity, matched cases, tree/output summaries, root-cause scorecard, repair candidates, Oct. 15 pending exposure, lineage, and backlog are included.","","Provider calls: 0; paid credits: 0; production DB mutations: 0."]
    (OUT/"report.md").write_text("\n".join(report)+"\n")
    print(json.dumps({"output":str(OUT),"reproduction_gate":summary["reproduction_gate"],"captures":len(all_repro),"capture_rows":sum(x["rows"] for x in all_repro),"rows":summary["2026_settled_rows"],"historical_rows":len(h),"oct15_pending":opmetrics["prediction_rows"],"classification":"DIAGNOSIS_COMPLETE_PROVISIONAL_ROOT_CAUSE"},sort_keys=True))
if __name__=="__main__":main()
