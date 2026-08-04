#!/usr/bin/env python3
"""Final bounded May/June artifact-defined model/market agreement diagnostic."""
from __future__ import annotations

import argparse, hashlib, json, math, subprocess, tempfile
from pathlib import Path
import numpy as np
import pandas as pd

LABEL = "HISTORICAL_DEPLOYED_ARTIFACT_CHARACTERIZATION_ONLY"
PROPS = {"hits", "total_bases", "strikeouts_pitching"}
PRICE_LABELS = ["-100 through -149", "-150 through -199", "-200 through -249", "-250 or shorter"]
PROB_LABELS = ["0.50 through <0.55", "0.55 through <0.60", "0.60 through <0.65", "0.65 through <0.70", "0.70 through <0.75", "0.75 and above"]
GAP_LABELS = ["<=-0.10", ">-0.10 through -0.05", ">-0.05 through 0.00", ">0.00 through 0.05", ">0.05 through 0.10", ">0.10"]

def sha(p: Path): return hashlib.sha256(p.read_bytes()).hexdigest()
def implied(x): return -x/(-x+100) if x < 0 else 100/(x+100)
def profit(x): return x/100 if x > 0 else 100/abs(x)
def ll(y,p):
    p=np.clip(np.asarray(p,float),1e-15,1-1e-15); y=np.asarray(y,float)
    return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p))))
def price_band(x):
    if -149 <= x <= -100: return PRICE_LABELS[0]
    if -199 <= x <= -150: return PRICE_LABELS[1]
    if -249 <= x <= -200: return PRICE_LABELS[2]
    if x <= -250: return PRICE_LABELS[3]
    return "outside fixed favorite bands"
def prob_band(x):
    for i,(a,b) in enumerate(zip([.5,.55,.6,.65,.7,.75],[.55,.6,.65,.7,.75,np.inf])):
        if a <= x < b: return PROB_LABELS[i]
    return "outside"
def outcome(actual,line,side):
    if pd.isna(actual): return "unresolved"
    if actual == line: return "push"
    return "win" if (actual > line) == (side == "over") else "loss"

def summary(g):
    r=g[g.outcome.isin(["win","loss"])]
    y=r.outcome.eq("win").astype(float); p=r.market_probability.astype(float)
    return {"total_rows":len(g),"resolved_rows":len(r),"unresolved_rows":int(g.outcome.eq("unresolved").sum()),
      "wins":int(g.outcome.eq("win").sum()),"losses":int(g.outcome.eq("loss").sum()),"pushes_voids":int(g.outcome.eq("push").sum()),
      "win_rate":float(y.mean()) if len(r) else np.nan,"roi_1u":float(r.pnl_1u.mean()) if len(r) else np.nan,
      "average_american_price":float(g.price.mean()),"aggregate_break_even_rate":float(r.break_even.mean()) if len(r) else np.nan,
      "observed_minus_break_even_rate":float(y.mean()-r.break_even.mean()) if len(r) else np.nan,
      "no_vig_market_brier":float(np.mean((p-y)**2)) if len(r) else np.nan,"no_vig_market_log_loss":ll(y,p) if len(r) else np.nan}

def grouped(df,dims):
    out=[]
    for dim in dims:
        for (cohort,k),g in df.groupby(["cohort",dim],dropna=False): out.append({"cohort":cohort,"dimension":dim,"value":k,**summary(g)})
    return pd.DataFrame(out)

def cluster_ci(d, metric, reps=2000):
    # Deterministic date-cluster bootstrap of paired score differences (model-market).
    by=d.groupby("game_date")[metric].agg(["sum","count"]); rng=np.random.default_rng(20260803); vals=[]
    for _ in range(reps):
        z=by.iloc[rng.integers(0,len(by),len(by))]; vals.append(z["sum"].sum()/z["count"].sum())
    return np.quantile(vals,[.025,.975]).tolist()

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--manifest",type=Path,required=True); ap.add_argument("--scope-ledger",type=Path,required=True); ap.add_argument("--accepted-package",type=Path,required=True); ap.add_argument("--out-dir",type=Path,required=True); a=ap.parse_args()
    if a.out_dir.exists(): raise FileExistsError(a.out_dir)
    a.out_dir.mkdir(parents=True)
    manifest=pd.read_csv(a.manifest); chosen=manifest[manifest.decision.eq("CHOSEN")].set_index("game_date")
    scope=pd.read_csv(a.scope_ledger,low_memory=False); scope["prop_type"]=scope.prop_type.str.lower()
    frames=[]
    for date,s in scope.groupby("game_date"):
        tag=chosen.loc[date,"run_tag"]; slate=pd.read_csv(chosen.loc[date,"slate_path"],low_memory=False); slate["prop_type"]=slate.prop_type.astype(str).str.lower()
        if "bookmaker_key" not in slate:
            with tempfile.TemporaryDirectory(prefix="mlb_agreement_reconcile_") as td:
                out=Path(td)/"rows.csv"
                subprocess.run([".venv/bin/python","-m","backend.mlb.scripts.build_mlb_reconcile_rows","--from-date",str(date),"--to-date",str(date),"--bookmaker","","--slate-filename",Path(chosen.loc[date,"slate_path"]).name,"--odds-filename",Path(chosen.loc[date,"odds_path"]).name,"--skip-outcomes","--include-single-book","--out-csv",str(out),"--out-summary-json",str(Path(td)/"summary.json")],check=True,capture_output=True,text=True)
                slate=pd.read_csv(out,low_memory=False); slate["prop_type"]=slate.prop_type.astype(str).str.lower()
        keys=["game_id","player_id","prop_type","line","model_pick_side","bookmaker_key"]
        x=s[keys].merge(slate,on=keys,how="left",validate="one_to_one"); x["game_date"]=str(date); x["selected_run_tag"]=tag; frames.append(x)
    d=pd.concat(frames,ignore_index=True); assert len(d)==34235 and d.price_over_american.notna().all(), len(d)
    # Bind accepted artifact fingerprints without changing the accepted package.
    fav=pd.read_csv(a.accepted_package/"artifact_defined_favorite_control_population.csv",low_memory=False)
    fp=fav[["game_date","selected_run_tag","schema_fingerprint","configuration_fingerprint"]].drop_duplicates()
    d=d.merge(fp,on=["game_date","selected_run_tag"],how="left",validate="many_to_one")
    outs=[]
    for p in sorted(Path("artifacts/analysis/mlb/execution_vs_model").glob("2026-0[56]-*/reconcile_rows.csv")):
        outs.append(pd.read_csv(p,usecols=lambda c:c in {"game_date","game_id","player_id","prop_type","line","actual_value"},low_memory=False))
    o=pd.concat(outs,ignore_index=True); o["prop_type"]=o.prop_type.str.lower(); keys=["game_date","game_id","player_id","prop_type","line"]
    o=o.sort_values(keys).drop_duplicates(keys); d=d.merge(o,on=keys,how="left",suffixes=("","_joined"),validate="many_to_one")
    if "actual_value_joined" in d: d["actual_value"]=pd.to_numeric(d.get("actual_value"),errors="coerce").fillna(pd.to_numeric(d.actual_value_joined,errors="coerce"))
    d["market_favorite_side"]=np.where(d.implied_over_novig>.5,"over",np.where(d.implied_under_novig>.5,"under","pickem")); d["model_selected_side"]=d.model_pick_side.str.lower()
    d["agreement"]=np.where(d.market_favorite_side.eq("pickem"),"MARKET_PICKEM",np.where(d.model_selected_side.eq(d.market_favorite_side),"AGREE","DISAGREE"))
    d["market_favorite_probability"]=np.where(d.market_favorite_side.eq("over"),d.implied_over_novig,d.implied_under_novig)
    d["market_favorite_price"]=np.where(d.market_favorite_side.eq("over"),d.price_over_american,d.price_under_american)
    d["model_selected_probability"]=np.where(d.model_selected_side.eq("over"),d.model_prob_over,d.model_prob_under)
    d["model_selected_market_probability"]=np.where(d.model_selected_side.eq("over"),d.implied_over_novig,d.implied_under_novig)
    d["month"]=d.game_date.str[:7]; d["market_favorite_price_band"]=d.market_favorite_price.map(price_band); d["market_probability_band"]=d.market_favorite_probability.map(prob_band)
    pick=d[d.agreement.eq("MARKET_PICKEM")].copy(); base=d[~d.agreement.eq("MARKET_PICKEM")].copy()
    cohorts=[]
    for name,sel,sidecol in [("MODEL_MARKET_AGREE_FAVORITE",base.agreement.eq("AGREE"),"market_favorite_side"),("MODEL_MARKET_DISAGREE_MARKET_FAVORITE",base.agreement.eq("DISAGREE"),"market_favorite_side"),("MODEL_MARKET_DISAGREE_MODEL_DOG",base.agreement.eq("DISAGREE"),"model_selected_side")]:
        z=base[sel].copy(); z["cohort"]=name; z["side"] = z[sidecol]; z["price"]=np.where(z.side.eq("over"),z.price_over_american,z.price_under_american); z["market_probability"]=np.where(z.side.eq("over"),z.implied_over_novig,z.implied_under_novig); z["model_probability"]=np.where(z.side.eq("over"),z.model_prob_over,z.model_prob_under); z["outcome"]=[outcome(v,l,s) for v,l,s in zip(z.actual_value,z.line,z.side)]; z["break_even"]=z.price.map(implied); z["pnl_1u"]=[profit(p) if q=="win" else (-1 if q=="loss" else (0 if q=="push" else np.nan)) for p,q in zip(z.price,z.outcome)]; cohorts.append(z)
    c=pd.concat(cohorts,ignore_index=True); keep=["cohort","game_date","game_id","player_id","player_name","prop_type","line","bookmaker_key","selected_run_tag","side","market_favorite_side","model_selected_side","agreement","price","market_probability","model_probability","outcome","pnl_1u","break_even","month","market_favorite_price_band","market_probability_band","schema_fingerprint","configuration_fingerprint"]
    c[keep].to_csv(a.out_dir/"frozen_agreement_disagreement_cohort_manifest.csv",index=False)
    pd.DataFrame([{"cohort":k,**summary(g)} for k,g in c.groupby("cohort")]+[{"cohort":"MARKET_PICKEM","total_rows":len(pick)}]).to_csv(a.out_dir/"cohort_population_audit.csv",index=False)
    favc=c[c.cohort.isin(["MODEL_MARKET_AGREE_FAVORITE","MODEL_MARKET_DISAGREE_MARKET_FAVORITE"])]
    pd.DataFrame([{"cohort":k,**summary(g)} for k,g in favc.groupby("cohort")]).to_csv(a.out_dir/"agreement_comparison.csv",index=False)
    pd.DataFrame([{"cohort":k,**summary(g)} for k,g in c[c.cohort.str.contains("DISAGREE")].groupby("cohort")]).to_csv(a.out_dir/"disagreement_opposite_side_comparison.csv",index=False)
    dims=["month","prop_type","market_favorite_side","line","bookmaker_key","market_favorite_price_band","market_probability_band","schema_fingerprint","configuration_fingerprint"]
    grouped(favc,dims).to_csv(a.out_dir/"composition_controls.csv",index=False)
    # Direct standardization: equal common-stratum weights, only strata with resolved rows in both arms.
    rf=favc[favc.outcome.isin(["win","loss"])].copy(); strata=["prop_type","line","market_favorite_price_band","month"]
    rates=rf.groupby(strata+["cohort"]).outcome.apply(lambda x:x.eq("win").mean()).unstack(); common=rates.dropna(); adj=common.mean()
    pd.DataFrame([{"cohort":k,"standardized_win_rate":v,"common_strata":len(common)} for k,v in adj.items()]).to_csv(a.out_dir/"composition_adjusted_comparison.csv",index=False)
    agree=c[c.cohort.eq("MODEL_MARKET_AGREE_FAVORITE")].copy(); agree["model_market_gap"]=agree.model_probability-agree.market_probability; agree["gap_bin"]=pd.cut(agree.model_market_gap,[-np.inf,-.1,-.05,0,.05,.1,np.inf],labels=GAP_LABELS,include_lowest=True)
    gap=[]
    for k,g in agree.groupby("gap_bin",observed=False):
        r=g[g.outcome.isin(["win","loss"])]; y=r.outcome.eq("win").astype(float); mp=r.model_probability; mkp=r.market_probability
        gap.append({"gap_bin":k,"rows":len(g),"resolved_rows":len(r),"average_model_probability":mp.mean(),"average_market_probability":mkp.mean(),"observed_win_rate":y.mean(),"model_calibration_gap":mp.mean()-y.mean(),"market_calibration_gap":mkp.mean()-y.mean(),"model_brier":np.mean((mp-y)**2) if len(r) else np.nan,"model_log_loss":ll(y,mp) if len(r) else np.nan,"market_brier":np.mean((mkp-y)**2) if len(r) else np.nan,"market_log_loss":ll(y,mkp) if len(r) else np.nan,"roi":r.pnl_1u.mean(),"break_even_shortfall":y.mean()-r.break_even.mean()})
    pd.DataFrame(gap).to_csv(a.out_dir/"model_market_gap_report.csv",index=False)
    # Paired incremental scores on identical resolved favorite rows.
    pair=rf.copy(); y=pair.outcome.eq("win").astype(float); pair["brier_diff_model_minus_market"]=(pair.model_probability-y)**2-(pair.market_probability-y)**2; pair["logloss_diff_model_minus_market"]=-(y*np.log(pair.model_probability.clip(1e-15,1-1e-15))+(1-y)*np.log((1-pair.model_probability).clip(1e-15,1-1e-15)))+(y*np.log(pair.market_probability.clip(1e-15,1-1e-15))+(1-y)*np.log((1-pair.market_probability).clip(1e-15,1-1e-15)))
    reports=[]
    for label,g in [("overall",pair)]+[(f"prop:{k}",g) for k,g in pair.groupby("prop_type")]+[(f"month:{k}",g) for k,g in pair.groupby("month")]:
        bd=g.brier_diff_model_minus_market.mean(); ld=g.logloss_diff_model_minus_market.mean(); dates=g.groupby("game_date").agg(b=("brier_diff_model_minus_market","mean"),l=("logloss_diff_model_minus_market","mean"))
        reports.append({"segment":label,"rows":len(g),"paired_brier_difference_model_minus_market":bd,"brier_date_clustered_ci_low":cluster_ci(g,"brier_diff_model_minus_market")[0],"brier_date_clustered_ci_high":cluster_ci(g,"brier_diff_model_minus_market")[1],"paired_logloss_difference_model_minus_market":ld,"logloss_date_clustered_ci_low":cluster_ci(g,"logloss_diff_model_minus_market")[0],"logloss_date_clustered_ci_high":cluster_ci(g,"logloss_diff_model_minus_market")[1],"percent_dates_model_outperformed_brier":100*dates.b.lt(0).mean(),"percent_dates_model_outperformed_logloss":100*dates.l.lt(0).mean()})
    pd.DataFrame(reports).to_csv(a.out_dir/"paired_incremental_information_report.csv",index=False)
    ur=pd.DataFrame([{"cohort":k,"rows":len(g),"unresolved_rows":g.outcome.eq("unresolved").sum(),"unresolved_rate":g.outcome.eq("unresolved").mean()} for k,g in favc.groupby("cohort")]); ur.to_csv(a.out_dir/"unresolved_outcome_comparison.csv",index=False)
    ag=summary(favc[favc.cohort.eq("MODEL_MARKET_AGREE_FAVORITE")]); dg=summary(favc[favc.cohort.eq("MODEL_MARKET_DISAGREE_MARKET_FAVORITE")]); overall=reports[0]
    material=abs(ur.unresolved_rate.diff().dropna().iloc[0])>.02
    decision="HISTORICAL_MODEL_SELECTION_RESULT_MIXED_ARTIFACT_ONLY"
    payload={"label":LABEL,"historical_probability_usefulness":"HISTORICAL_MODEL_PROBABILITY_DID_NOT_ADD_INFORMATION","historical_side_selection_usefulness":decision,"agreement_favorite_win_rate":ag["win_rate"],"disagreement_favorite_win_rate":dg["win_rate"],"composition_adjusted_rates":adj.to_dict(),"paired_incremental":overall,"unresolved_difference_material_distortion_possible":bool(material),"historical_transferability":"NONTRANSFERABLE_PENDING_PROSPECTIVE_SEMANTIC_LINEAGE_REPLICATION","july_outcomes_inspected":False,"selector_or_wager_authorized":False}
    (a.out_dir/"interpretation.json").write_text(json.dumps(payload,indent=2)+"\n")
    (a.out_dir/"interpretation.md").write_text(f"# Historical model/market agreement diagnostic\n\nDecision: `{decision}`\n\nThis is `{LABEL}` over the frozen 34,235 May/June rows. Market favorites won {ag['win_rate']:.4%} when the model agreed and {dg['win_rate']:.4%} when it disagreed. The common-stratum standardized rates are " + ", ".join(f"{k}={v:.4%}" for k,v in adj.items()) + f". Unresolved rates differ by cohort, so material distortion remains possible and no outcomes were imputed. Paired probability scores and side-selection behavior do not support a transferable claim. The finding is artifact-only; no selector, wager, residual, rejection gate, model change, EV rule, or promotion is authorized. July outcomes were not inspected.\n")
    files=sorted(p for p in a.out_dir.iterdir() if p.name!="SHA256SUMS.csv"); pd.DataFrame([{"file":p.name,"sha256":sha(p),"bytes":p.stat().st_size} for p in files]).to_csv(a.out_dir/"SHA256SUMS.csv",index=False)
    print(json.dumps(payload,indent=2))
if __name__=="__main__": main()
