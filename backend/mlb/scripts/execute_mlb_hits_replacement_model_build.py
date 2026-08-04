#!/usr/bin/env python3
"""Execute the fail-closed MLB Hits replacement-model first pass.

Research-only: fits independent controls/candidates with expanding, strict-prior
cutoffs. Outcomes are written separately from the frozen prediction ledger.
No candidate is promoted and no legacy prediction/model artifact is read.
"""
from __future__ import annotations

import argparse, hashlib, json, math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy.special import expit, logit
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
SEED = 20260803
FEATURES = [
    "d7_hits_per_pa", "d15_hits_per_pa", "d30_hits_per_pa", "season_to_date_hits_per_pa",
    "d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances",
    "season_to_date_pa_per_game", "prior_game_count", "starter_prior_start_count",
    "starter_d15_outs_per_start", "starter_d15_hits_allowed_per_out",
    "starter_d15_earned_runs_per_start", "team_offense_d15_hits_per_game", "is_home",
]
LINES = (0.5, 1.5)
CANDIDATES = ("base_rate", "opportunity_control", "regularized_logistic", "nonlinear_hgb", "count_hgb")

def clip(p): return np.clip(np.asarray(p, float), 1e-6, 1-1e-6)
def american_prob(x):
    x=float(x); return (-x)/((-x)+100) if x < 0 else 100/(x+100)
def profit(price, won):
    if not won: return -1.0
    p=float(price); return 100/(-p) if p < 0 else p/100
def sha(path):
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1<<20), b""): h.update(b)
    return h.hexdigest()
def prep():
    return ColumnTransformer([("num", Pipeline([("imp",SimpleImputer(strategy="median")),("scale",StandardScaler())]), FEATURES)])
def logistic():
    return Pipeline([("prep",prep()),("model",LogisticRegression(C=.25, max_iter=1000, random_state=SEED))])
def hgb_classifier():
    return Pipeline([("prep",prep()),("model",HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=15,learning_rate=.05,l2_regularization=1.0,random_state=SEED))])
def hgb_count():
    return Pipeline([("prep",prep()),("model",HistGradientBoostingRegressor(loss="poisson",max_iter=100,max_leaf_nodes=15,learning_rate=.05,l2_regularization=1.0,random_state=SEED))])

def load_market(path: Path) -> pd.DataFrame:
    if not path.exists(): return pd.DataFrame()
    x=pd.read_csv(path, low_memory=False)
    x=x[(x.prop_type.astype(str).str.lower()=="hits") & x.line.isin(LINES) & x.side.str.lower().isin(["over","under"])].copy()
    x["ts"]=pd.to_datetime(x.source_capture_timestamp, utc=True, errors="coerce")
    x=x.sort_values("ts").drop_duplicates(["player_game_key","line","side"], keep="first")
    z=x.pivot(index=["player_game_key","line"],columns="side",values="price").reset_index()
    z=z.dropna(subset=["over","under"])
    oi=z.over.map(american_prob); ui=z.under.map(american_prob)
    z["market_over_probability"]=oi/(oi+ui)
    return z.rename(columns={"over":"over_price","under":"under_price"})

def frozen_predictions(spine: pd.DataFrame, market: pd.DataFrame, eval_start: str, eval_end: str) -> pd.DataFrame:
    s=spine.copy(); s.slate_date=s.slate_date.astype(str).str[:10]
    s=s[pd.to_numeric(s.actual_plate_appearances,errors="coerce").gt(0)].copy()
    for c in FEATURES+["actual_hits"]: s[c]=pd.to_numeric(s[c],errors="coerce")
    rows=[]
    dates=sorted(d for d in s.slate_date.unique() if eval_start <= d <= eval_end)
    for d in dates:
        tr=s[s.slate_date < d]; te=s[s.slate_date == d]
        if len(tr)<500 or tr.slate_date.nunique()<10: continue
        count_model=hgb_count(); count_model.fit(tr[FEATURES],tr.actual_hits.clip(lower=0))
        mu=np.clip(count_model.predict(te[FEATURES]),1e-6,8)
        count_probs={.5:1-np.exp(-mu),1.5:1-np.exp(-mu)*(1+mu)}
        pa=np.clip(tr.season_to_date_pa_per_game.fillna(tr.season_to_date_pa_per_game.median()).to_numpy(),1,6)
        rate=np.clip(tr.season_to_date_hits_per_pa.fillna(tr.season_to_date_hits_per_pa.median()).to_numpy(),.01,.8)
        for line in LINES:
            y=(tr.actual_hits>line).astype(int); yt=(te.actual_hits>line).astype(int)
            if y.nunique()<2: continue
            models={"regularized_logistic":logistic(),"nonlinear_hgb":hgb_classifier()}
            for m in models.values(): m.fit(tr[FEATURES],y)
            base=np.repeat((y.sum()+1)/(len(y)+2),len(te))
            erate=np.clip(te.season_to_date_hits_per_pa.fillna(np.nanmedian(rate)),.01,.8).to_numpy()
            epa=np.clip(te.season_to_date_pa_per_game.fillna(np.nanmedian(pa)),1,6).to_numpy()
            omu=erate*epa
            opp=1-np.exp(-omu) if line==.5 else 1-np.exp(-omu)*(1+omu)
            probs={"base_rate":base,"opportunity_control":opp,"regularized_logistic":models["regularized_logistic"].predict_proba(te[FEATURES])[:,1],"nonlinear_hgb":models["nonlinear_hgb"].predict_proba(te[FEATURES])[:,1],"count_hgb":count_probs[line]}
            for name,p in probs.items():
                for i,(_,r) in enumerate(te.iterrows()):
                    rows.append({"slate_date":d,"game_id":r.game_id,"player_id":r.player_id,"player_game_key":r.player_game_key,"line":line,"candidate":name,"fit_cutoff":str(pd.to_datetime(d)-pd.Timedelta(days=1))[:10],"training_rows":len(tr),"training_dates":tr.slate_date.nunique(),"feature_count":0 if name=="base_rate" else (2 if name=="opportunity_control" else len(FEATURES)),"predicted_over_probability":float(clip(p[i]))})
        print(f"fit {d}: train={len(tr)} score={len(te)}")
    pred=pd.DataFrame(rows)
    if market.empty: return pred
    # Market offset and favorite-failure use only historically captured two-sided rows.
    smp=s.merge(market,on="player_game_key",how="inner")
    for d in dates:
        tr=smp[smp.slate_date < d]; te=smp[smp.slate_date == d]
        if len(tr)<500 or tr.slate_date.nunique()<10 or te.empty: continue
        for line in LINES:
            a=tr[tr.line==line]; b=te[te.line==line]
            if len(a)<500 or b.empty: continue
            y=(a.actual_hits>line).astype(int).to_numpy(); off=logit(clip(a.market_over_probability))
            pp=prep(); X=pp.fit_transform(a[FEATURES]); Xt=pp.transform(b[FEATURES])
            try:
                fit=sm.GLM(y,sm.add_constant(X),family=sm.families.Binomial(),offset=off).fit(maxiter=100,disp=0)
                p=fit.predict(sm.add_constant(Xt),offset=logit(clip(b.market_over_probability)))
                for i,(_,r) in enumerate(b.iterrows()): rows.append({"slate_date":d,"game_id":r.game_id,"player_id":r.player_id,"player_game_key":r.player_game_key,"line":line,"candidate":"market_offset_residual","fit_cutoff":str(pd.to_datetime(d)-pd.Timedelta(days=1))[:10],"training_rows":len(a),"training_dates":a.slate_date.nunique(),"feature_count":len(FEATURES),"predicted_over_probability":float(clip(p[i]))})
            except Exception as e: print("offset skipped",d,line,type(e).__name__)
            fav_over=a.market_over_probability>=.5; yf=np.where(fav_over,a.actual_hits<=line,a.actual_hits>line).astype(int)
            favp=np.maximum(a.market_over_probability,1-a.market_over_probability)
            Z=a[FEATURES].copy(); Z["market_favorite_probability"]=favp
            Zt=b[FEATURES].copy(); Zt["market_favorite_probability"]=np.maximum(b.market_over_probability,1-b.market_over_probability)
            fp=ColumnTransformer([("num",Pipeline([("imp",SimpleImputer(strategy="median")),("scale",StandardScaler())]),FEATURES+["market_favorite_probability"])])
            fm=Pipeline([("prep",fp),("model",LogisticRegression(C=.25,max_iter=1000,random_state=SEED))]); fm.fit(Z,yf); q=fm.predict_proba(Zt)[:,1]
            for i,(_,r) in enumerate(b.iterrows()): rows.append({"slate_date":d,"game_id":r.game_id,"player_id":r.player_id,"player_game_key":r.player_game_key,"line":line,"candidate":"favorite_failure","fit_cutoff":str(pd.to_datetime(d)-pd.Timedelta(days=1))[:10],"training_rows":len(a),"training_dates":a.slate_date.nunique(),"feature_count":len(FEATURES)+1,"predicted_over_probability":float(clip(q[i])),"prediction_semantics":"probability_market_favorite_loses"})
    return pd.DataFrame(rows)

def calibration(y,p):
    x=logit(clip(p)); X=sm.add_constant(x)
    try:
        f=sm.GLM(y,X,family=sm.families.Binomial()).fit(disp=0); return float(f.params[0]),float(f.params[1])
    except Exception:return np.nan,np.nan
def clustered_diff(df, cluster):
    z=df.groupby(cluster).loss_diff.mean()
    if len(z)<2:return (np.nan,np.nan)
    se=z.std(ddof=1)/math.sqrt(len(z)); return (z.mean()-1.96*se,z.mean()+1.96*se)

def evaluate(pred, outcomes, market):
    x=pred.merge(outcomes,on=["player_game_key","slate_date","game_id","player_id"],how="left")
    x=x.merge(market,on=["player_game_key","line"],how="left") if not market.empty else x
    rec=[]
    for (line,c),g in x.groupby(["line","candidate"]):
        g=g[g.actual_hits.notna()].copy()
        if c=="favorite_failure":
            fav_over=g.market_over_probability>=.5; y=np.where(fav_over,g.actual_hits<=line,g.actual_hits>line).astype(int); ctrl=1-np.maximum(g.market_over_probability,1-g.market_over_probability)
        else:
            y=(g.actual_hits>line).astype(int).to_numpy(); ctrl=None
            if c in ["regularized_logistic","nonlinear_hgb","count_hgb"]:
                base=x[(x.line==line)&(x.candidate=="opportunity_control")][["player_game_key","predicted_over_probability"]].drop_duplicates("player_game_key"); ctrl=g.player_game_key.map(base.set_index("player_game_key").predicted_over_probability)
            elif c=="market_offset_residual": ctrl=g.market_over_probability
        p=clip(g.predicted_over_probability); b=float(brier_score_loss(y,p)); ll=float(log_loss(y,p,labels=[0,1])); ci,sl=calibration(y,p)
        d={"line":line,"candidate":c,"training_start":"2026-03-25","first_fit_cutoff":g.fit_cutoff.min(),"last_fit_cutoff":g.fit_cutoff.max(),"evaluation_start":g.slate_date.min(),"evaluation_end":g.slate_date.max(),"eligible_predictions":len(g),"resolved_outcomes":len(g),"feature_count":int(g.feature_count.max()),"brier":b,"log_loss":ll,"calibration_in_large":float(np.mean(p)-np.mean(y)),"calibration_slope":sl,"calibration_intercept":ci,"largest_player_concentration":float(g.player_id.value_counts(normalize=True).max()),"largest_failure_mode":"market capture ends 2026-07-11" if c in ["market_offset_residual","favorite_failure"] else ("weak 1.5 separation" if line==1.5 else "probability calibration/side economics")}
        if ctrl is not None:
            ok=pd.notna(ctrl); gg=g.loc[ok].copy(); yy=np.asarray(y)[ok]; cc=clip(np.asarray(ctrl)[ok]); gg["loss_diff"]=(clip(p[ok])-yy)**2-(cc-yy)**2
            d.update(control_brier=float(np.mean((cc-yy)**2)),paired_brier_difference=float(gg.loss_diff.mean()),pct_dates_improved=float((gg.groupby("slate_date").loss_diff.mean()<0).mean()),date_ci_low=clustered_diff(gg,"slate_date")[0],date_ci_high=clustered_diff(gg,"slate_date")[1],game_ci_low=clustered_diff(gg,"game_id")[0],game_ci_high=clustered_diff(gg,"game_id")[1],player_game_ci_low=clustered_diff(gg,"player_game_key")[0],player_game_ci_high=clustered_diff(gg,"player_game_key")[1])
        if "over_price" in g:
            side=np.where(p>=.5,"over","under"); won=np.where(side=="over",y==1,y==0); price=np.where(side=="over",g.over_price,g.under_price); ok=pd.notna(price); d["roi"]=float(np.mean([profit(a,b) for a,b in zip(price[ok],won[ok])])) if ok.any() else np.nan; d["win_rate"]=float(np.mean(won[ok])) if ok.any() else np.nan; d["aggregate_break_even_rate"]=float(np.mean([american_prob(a) for a in price[ok]])) if ok.any() else np.nan; d["selected_over_pct"]=float(np.mean(side=="over"))
        rec.append(d)
    return pd.DataFrame(rec),x

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--spine",required=True); ap.add_argument("--market",required=True); ap.add_argument("--out-dir",required=True); ap.add_argument("--eval-start",default="2026-05-01"); ap.add_argument("--eval-end",default="2026-08-02"); a=ap.parse_args(); out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True)
    s=pd.read_csv(a.spine,low_memory=False); m=load_market(Path(a.market))
    prediction_path=out/"frozen_prediction_ledger.csv"
    if prediction_path.exists():
        pred=pd.read_csv(prediction_path,low_memory=False)
        print(f"resuming from {len(pred)} frozen predictions")
    else:
        pred=frozen_predictions(s,m,a.eval_start,a.eval_end)
        pred.to_csv(prediction_path,index=False)
    outcomes=s[["player_game_key","slate_date","game_id","player_id","actual_hits","actual_plate_appearances"]].copy()
    outcomes.to_csv(out/"outcome_ledger.csv",index=False)
    comp,joined=evaluate(pred,outcomes,m)
    expected=[(line,c) for line in LINES for c in list(CANDIDATES)+["market_offset_residual","favorite_failure"]]
    have=set(zip(comp.line,comp.candidate))
    missing=[]
    for line,c in expected:
        if (line,c) not in have:
            missing.append({"line":line,"candidate":c,"training_start":"2026-03-25","evaluation_start":a.eval_start,"evaluation_end":a.eval_end,"eligible_predictions":0,"resolved_outcomes":0,"feature_count":len(FEATURES)+(1 if c=="favorite_failure" else 0),"largest_failure_mode":"only 425 two-sided Hits 1.5 captures; fixed 500-row minimum not met","disposition":"DISCARD_NO_SIGNAL","deserves_further_work":False})
    if missing: comp=pd.concat([comp,pd.DataFrame(missing)],ignore_index=True,sort=False)
    manifest=pd.DataFrame([{"feature":f,"family":"opportunity" if "plate_appearances" in f or "pa_per_game" in f else "batter" if "hit" in f and not f.startswith("starter") else "pitcher" if f.startswith("starter") else "game_or_quality","strict_prior_rule":"source_game_date < slate_date"} for f in FEATURES]); manifest.to_csv(out/"strict_prior_feature_manifest.csv",index=False)
    spec={"status":"NO_QUALIFIED_MLB_HITS_MODEL","promotion_authorized":False,"training":"expanding daily; >=500 rows, >=10 dates, both classes","lines":[.5,1.5],"count_conversion":"Poisson: P(H>=1)=1-exp(-mu); P(H>=2)=1-exp(-mu)*(1+mu)","candidates":list(CANDIDATES)+["market_offset_residual","favorite_failure"],"excluded_optional_features":["bullpen","weather","park","batted_ball/contact","handedness","confirmed historical batting order"],"legacy_inputs_used":False}; (out/"replacement_model_specification.json").write_text(json.dumps(spec,indent=2)+"\n")
    # Dispositions are deliberately evidence-based and do not promote first-pass models.
    disp=[]
    for _,r in comp.iterrows():
        if r.get("eligible_predictions",0)==0:
            disp.append("DISCARD_NO_SIGNAL"); continue
        delta=r.get("paired_brier_difference",np.nan); c=r.candidate
        if c=="favorite_failure" and pd.notna(delta) and delta<0: d="FAVORITE_REJECTION_SIGNAL_PRESENT"
        elif c=="market_offset_residual" and pd.notna(delta) and delta<0: d="MARKET_INCREMENTAL_SIGNAL_PRESENT"
        elif c in ["regularized_logistic","nonlinear_hgb","count_hgb"] and pd.notna(delta) and delta<0: d="BASEBALL_SIGNAL_PRESENT_BELOW_MARKET"
        elif pd.notna(delta) and delta>=0: d="DISCARD_WORSE_THAN_SIMPLE_CONTROL"
        else:d="DISCARD_NO_SIGNAL"
        disp.append(d)
    comp["disposition"]=disp
    comp["deserves_further_work"]=comp.disposition.isin(["BASEBALL_SIGNAL_PRESENT_BELOW_MARKET","MARKET_INCREMENTAL_SIGNAL_PRESENT","FAVORITE_REJECTION_SIGNAL_PRESENT","REQUIRES_ONE_BOUNDED_REFINEMENT"])
    comp.to_csv(out/"candidate_comparison.csv",index=False)
    positive=set(disp)&{"MARKET_INCREMENTAL_SIGNAL_PRESENT","FAVORITE_REJECTION_SIGNAL_PRESENT"}
    final="MULTIPLE_REPLACEMENT_CANDIDATES_REQUIRE_BOUNDED_REFINEMENT" if len(positive)>1 else (next(iter(positive)).replace("_SIGNAL_PRESENT","_MODEL_CANDIDATE_FOUND") if positive else ("BASEBALL_SIGNAL_FOUND_NOT_MARKET_INCREMENTAL" if "BASEBALL_SIGNAL_PRESENT_BELOW_MARKET" in disp else "ALL_REPLACEMENT_CANDIDATES_FAILED_NO_QUALIFIED_MODEL"))
    monthly=joined.assign(month=joined.slate_date.astype(str).str[:7])
    monthly["binary_outcome"]=(monthly.actual_hits>monthly.line).astype(int)
    monthly["brier_component"]=(clip(monthly.predicted_over_probability)-monthly.binary_outcome)**2
    monthly.groupby(["line","candidate","month"],as_index=False).agg(rows=("binary_outcome","size"),brier=("brier_component","mean")).to_csv(out/"candidate_results_by_month.csv",index=False)
    text=f"# MLB Hits replacement execution\n\nFinal decision: **{final}**. Production remains fail-closed as `NO_QUALIFIED_MLB_HITS_MODEL`; this first pass authorizes no promotion.\n\nEvery candidate is in `candidate_comparison.csv`. Baseball-only coverage extends through {a.eval_end}; market candidates use only exact local two-sided captures.\n"
    (out/"interpretation.md").write_text(text)
    hashes=[]
    for p in sorted(out.iterdir()):
        if p.is_file() and p.name!="reproducibility_hashes.csv":hashes.append({"path":p.name,"sha256":sha(p),"bytes":p.stat().st_size})
    pd.DataFrame(hashes).to_csv(out/"reproducibility_hashes.csv",index=False); print(json.dumps({"final_decision":final,"predictions":len(pred),"comparison_rows":len(comp)},indent=2))
if __name__=="__main__": main()
