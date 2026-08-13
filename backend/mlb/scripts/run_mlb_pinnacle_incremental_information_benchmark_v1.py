#!/usr/bin/env python3
"""Bounded historical Pinnacle incremental-information benchmark (no deployment)."""
from __future__ import annotations

import argparse, hashlib, json, math, os, time
from datetime import timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10"
ML = ROOT / "artifacts/analysis/model_development/mlb_established_game_prediction_methods_benchmark_v1/2026-08-05"
TOT = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06"
SPINE = ROOT / "artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06"
START, END = "2026-05-01", "2026-08-09"

def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def american_prob(x):
    x=float(x); return 100/(x+100) if x>0 else -x/(-x+100)

def clip(x): return np.clip(np.asarray(x,dtype=float),1e-6,1-1e-6)
def logit(x):
    x=clip(x); return np.log(x/(1-x))

def metrics(y,p):
    y=np.asarray(y,dtype=int); p=clip(p)
    return dict(games=len(y),accuracy=float(np.mean((p>=.5)==y)),brier=float(brier_score_loss(y,p)),
                log_loss=float(log_loss(y,p,labels=[0,1])),calibration_error=float(np.mean(p)-np.mean(y)),
                predicted_mean=float(np.mean(p)),observed_rate=float(np.mean(y)))

def fit_incremental(df, ycol, a_cols, b_cols, lane):
    d=df.dropna(subset=[ycol]+b_cols).sort_values(["game_date","game_pk"]).copy()
    dates=sorted(d.game_date.unique()); cut=dates[max(0,int(len(dates)*.7)-1)]
    tr=d.game_date<=cut; te=~tr
    rows=[]; preds={}
    for label,cols in [("A_MARKET_ONLY",a_cols),("B_MARKET_PLUS_PROPpadia",b_cols)]:
        model=LogisticRegression(C=1.0,max_iter=2000,random_state=17).fit(d.loc[tr,cols],d.loc[tr,ycol])
        p=model.predict_proba(d.loc[te,cols])[:,1]; preds[label]=p
        m=metrics(d.loc[te,ycol],p)
        rows.append({"lane":lane,"model":label,"train_games":int(tr.sum()),"test_games":int(te.sum()),
                     "train_end_date":cut,"test_start_date":d.loc[te,"game_date"].min(),**m,
                     "features":"|".join(cols),"regularization":"L2_C_1.0"})
    delta={k:rows[1][k]-rows[0][k] for k in ["brier","log_loss","calibration_error"]}
    calibration_delta=abs(rows[1]["calibration_error"])-abs(rows[0]["calibration_error"])
    # The requested primary test requires improvement in all three dimensions.
    # Small score gains accompanied by worse calibration are explicitly mixed.
    decision=("INCREMENTAL_INFORMATION_PRESENT" if delta["brier"]<0 and delta["log_loss"]<0 and calibration_delta<=0 else
              "NO_INCREMENTAL_INFORMATION" if delta["brier"]>=0 and delta["log_loss"]>=0 and calibration_delta>=0 else "MIXED")
    for r in rows: r.update({"brier_delta_B_minus_A":delta["brier"],"log_loss_delta_B_minus_A":delta["log_loss"],
                             "abs_calibration_delta_B_minus_A":calibration_delta,
                             "decision":decision})
    test=d.loc[te].copy()
    test["market_only_diagnostic_probability"]=preds["A_MARKET_ONLY"]
    test["market_plus_model_diagnostic_probability"]=preds["B_MARKET_PLUS_PROPpadia"]
    return pd.DataFrame(rows),test,decision

def acquire(api_key: str):
    OUT.mkdir(parents=True,exist_ok=True); raw=OUT/"raw"; raw.mkdir(exist_ok=True)
    spine=pd.read_csv(SPINE/"canonical_totals_game_spine.csv")
    spine=spine[spine.game_date.between(START,END)].copy()
    spine["scheduled_start_utc"]=pd.to_datetime(spine.scheduled_start_utc,utc=True)
    dates=pd.date_range(START,END).strftime("%Y-%m-%d")
    rows=[]
    for day in dates:
        games=spine[spine.game_date.eq(day)]
        if games.empty:
            rows.append({"game_date":day,"status":"NO_CERTIFIED_SCHEDULE","scheduled_games":0,"request_cost":0})
            continue
        requested=(games.scheduled_start_utc.min()-pd.Timedelta(minutes=60)).to_pydatetime().astimezone(timezone.utc)
        stamp=requested.isoformat(timespec="seconds").replace("+00:00","Z")
        dest=raw/f"{day}_{requested.strftime('%H%M%SZ')}.json"
        if dest.exists():
            payload=json.loads(dest.read_text()); hdr={}
        else:
            url="https://api.the-odds-api.com/v4/historical/sports/baseball_mlb/odds"
            resp=requests.get(url,params={"apiKey":api_key,"regions":"us","markets":"h2h,totals,spreads",
                              "bookmakers":"pinnacle","oddsFormat":"american","dateFormat":"iso","date":stamp},timeout=60)
            resp.raise_for_status(); payload=resp.json(); dest.write_text(json.dumps(payload,sort_keys=True,separators=(",",":")))
            hdr=resp.headers
        rows.append({"game_date":day,"status":"ACQUIRED","scheduled_games":len(games),"requested_snapshot_utc":stamp,
                     "provider_snapshot_utc":payload.get("timestamp"),"raw_path":str(dest.relative_to(ROOT)),"source_sha256":sha(dest),
                     "events_returned":len(payload.get("data",[])),"request_cost":int(hdr.get("x-requests-last",30) or 30),
                     "quota_remaining":hdr.get("x-requests-remaining"),"quota_used":hdr.get("x-requests-used")})
        time.sleep(.08)
    pd.DataFrame(rows).to_csv(OUT/"pinnacle_historical_snapshot_manifest.csv",index=False)

def parse_market():
    manifest=pd.read_csv(OUT/"pinnacle_historical_snapshot_manifest.csv")
    rows=[]
    for _,m in manifest[manifest.status.eq("ACQUIRED")].iterrows():
        payload=json.loads((ROOT/m.raw_path).read_text())
        for e in payload.get("data",[]):
            books=[b for b in e.get("bookmakers",[]) if b.get("key")=="pinnacle"]
            if not books: continue
            base={"event_id":e.get("id"),"game_date":m.game_date,"commence_time":e.get("commence_time"),
                  "home_team":e.get("home_team"),"away_team":e.get("away_team"),"requested_snapshot_utc":m.requested_snapshot_utc,
                  "provider_snapshot_utc":m.provider_snapshot_utc,"raw_path":m.raw_path,"source_sha256":m.source_sha256}
            for market in books[0].get("markets",[]):
                key=market.get("key")
                for o in market.get("outcomes",[]):
                    rows.append({**base,"market":key,"market_last_update":market.get("last_update"),"outcome":o.get("name"),
                                 "description":o.get("description"),"price":o.get("price"),"point":o.get("point")})
    return pd.DataFrame(rows)

def bind_events(market):
    spine=pd.read_csv(SPINE/"canonical_totals_game_spine.csv")
    spine=spine[spine.game_date.between(START,END)].copy(); spine.scheduled_start_utc=pd.to_datetime(spine.scheduled_start_utc,utc=True)
    # Exact team names and closest-start matching; ambiguity fails closed.
    events=market[["event_id","home_team","away_team","commence_time"]].drop_duplicates().copy()
    events.commence_time=pd.to_datetime(events.commence_time,utc=True)
    binds=[]
    for _,e in events.iterrows():
        c=spine[(spine.home_team.eq(e.home_team))&(spine.away_team.eq(e.away_team))].copy()
        c["delta_minutes"]=(c.scheduled_start_utc-e.commence_time).abs().dt.total_seconds()/60
        c=c[c.delta_minutes<=180].sort_values("delta_minutes")
        status="UNMAPPED"; game_pk=np.nan; delta=np.nan
        if len(c) and (len(c)==1 or c.iloc[0].delta_minutes<c.iloc[1].delta_minutes):
            status="EXACT_TEAMS_UNIQUE_CLOSEST_START"; game_pk=int(c.iloc[0].game_pk); delta=c.iloc[0].delta_minutes
        binds.append({"event_id":e.event_id,"game_pk":game_pk,"mapping_status":status,"start_delta_minutes":delta})
    return market.merge(pd.DataFrame(binds),on="event_id",how="left")

def moneyline(m):
    x=m[(m.market.eq("h2h"))&m.game_pk.notna()].copy()
    out=[]
    pred=pd.read_csv(ML/"benchmark_game_predictions.csv")
    pred=pred[(pred.method.eq("PYTHAGOREAN_LOG5"))&pred.game_date.between(START,END)]
    pop=pd.read_csv(ML/"certified_chronological_game_population.csv")
    for pk,g in x.groupby("game_pk"):
        home=g.home_team.iloc[0]; away=g.away_team.iloc[0]
        hr=g[g.outcome.eq(home)]; ar=g[g.outcome.eq(away)]
        if len(hr)!=1 or len(ar)!=1: continue
        ph,pa=american_prob(hr.price.iloc[0]),american_prob(ar.price.iloc[0]); s=ph+pa
        out.append({"game_pk":int(pk),"pinnacle_home_price":hr.price.iloc[0],"pinnacle_away_price":ar.price.iloc[0],
                    "pinnacle_home_raw_probability":ph,"pinnacle_away_raw_probability":pa,
                    "pinnacle_home_no_vig_probability":ph/s,"pinnacle_away_no_vig_probability":pa/s,
                    **{k:g.iloc[0][k] for k in ["event_id","requested_snapshot_utc","provider_snapshot_utc","market_last_update","mapping_status","start_delta_minutes","raw_path","source_sha256"]}})
    d=pd.DataFrame(out).merge(pred[["game_pk","game_date","split","home_team_abbr","away_team_abbr","home_win_probability"]],on="game_pk")
    d=d.merge(pop[["game_pk","winner_home","scheduled_start_utc"]],on="game_pk")
    d["model_away_probability"]=1-d.home_win_probability; d["model_minus_pinnacle_home_probability"]=d.home_win_probability-d.pinnacle_home_no_vig_probability
    d["model_predicted_winner"]=np.where(d.home_win_probability>=.5,d.home_team_abbr,d.away_team_abbr)
    d["pinnacle_favorite"]=np.where(d.pinnacle_home_no_vig_probability>=.5,d.home_team_abbr,d.away_team_abbr)
    d["official_winner"]=np.where(d.winner_home.eq(1),d.home_team_abbr,d.away_team_abbr)
    d["snapshot_lead_minutes"]=(pd.to_datetime(d.scheduled_start_utc,utc=True)-pd.to_datetime(d.provider_snapshot_utc,utc=True)).dt.total_seconds()/60
    d.to_csv(OUT/"moneyline_pinnacle_join.csv",index=False)
    rows=[]
    for label,pcol in [("PROPPADIA_PYTHAGOREAN_LOG5","home_win_probability"),("PINNACLE_NO_VIG","pinnacle_home_no_vig_probability")]: rows.append({"model":label,**metrics(d.winner_home,d[pcol])})
    comp=pd.DataFrame(rows); comp["brier_difference_model_minus_pinnacle"]=rows[0]["brier"]-rows[1]["brier"]
    comp["log_loss_difference_model_minus_pinnacle"]=rows[0]["log_loss"]-rows[1]["log_loss"]
    comp["accuracy_difference_model_minus_pinnacle"]=rows[0]["accuracy"]-rows[1]["accuracy"]
    comp.to_csv(OUT/"moneyline_model_vs_pinnacle.csv",index=False)
    a=d.model_minus_pinnacle_home_probability.abs(); labels=["<2.5pp","2.5-4.99pp","5.0-7.49pp","7.5-9.99pp",">=10pp"]
    d["disagreement_band"]=pd.cut(a,[-1,.025,.05,.075,.10,np.inf],labels=labels,right=False)
    bands=[]
    for (band,direction),g in d.groupby(["disagreement_band",np.where(d.model_minus_pinnacle_home_probability>=0,"MODEL_MORE_HOME","MODEL_MORE_AWAY")],observed=True):
        bands.append({"band":band,"direction":direction,"games":len(g),"model_accuracy":metrics(g.winner_home,g.home_win_probability)["accuracy"],
          "pinnacle_favorite_accuracy":metrics(g.winner_home,g.pinnacle_home_no_vig_probability)["accuracy"],"model_brier":metrics(g.winner_home,g.home_win_probability)["brier"],
          "pinnacle_brier":metrics(g.winner_home,g.pinnacle_home_no_vig_probability)["brier"],"observed_home_win_rate":g.winner_home.mean()})
    pd.DataFrame(bands).to_csv(OUT/"moneyline_disagreement_bands.csv",index=False)
    d["pinnacle_logit"]=logit(d.pinnacle_home_no_vig_probability); d["model_logit"]=logit(d.home_win_probability)
    inc,test,decision=fit_incremental(d,"winner_home",["pinnacle_logit"],["pinnacle_logit","model_logit"],"MONEYLINE")
    inc.to_csv(OUT/"moneyline_incremental_information_test.csv",index=False)
    return d,comp,test,decision

def model_over(g,line):
    p=0.0
    for n in range(21):
        if n>line: p+=float(g[f"p_total_{n}"])
    if 20>line: p+=float(g["p_total_20_plus"])
    return p

def totals(m):
    x=m[(m.market.eq("totals"))&m.game_pk.notna()].copy(); out=[]
    for pk,g in x.groupby("game_pk"):
        overs=g[g.outcome.eq("Over")]; unders=g[g.outcome.eq("Under")]
        pairs=overs.merge(unders,on="point",suffixes=("_o","_u"))
        if len(pairs)!=1: continue
        r=pairs.iloc[0]; po,pu=american_prob(r.price_o),american_prob(r.price_u); s=po+pu
        out.append({"game_pk":int(pk),"pinnacle_total_line":r.point,"pinnacle_over_price":r.price_o,"pinnacle_under_price":r.price_u,
                    "pinnacle_over_raw_probability":po,"pinnacle_under_raw_probability":pu,"pinnacle_over_no_vig_probability":po/s,
                    **{k:r[k+"_o"] for k in ["event_id","requested_snapshot_utc","provider_snapshot_utc","market_last_update","mapping_status","start_delta_minutes","raw_path","source_sha256"]}})
    d=pd.DataFrame(out)
    pred=pd.read_csv(TOT/"total_distribution_predictions.csv")
    pred=pred[(pred.model.eq("MODEL_C_INDEPENDENT_HOME_AWAY_POISSON"))&pred.game_date.between(START,END)]
    outcome=pd.read_csv(SPINE/"regulation_and_final_outcome_spine.csv")
    d=d.merge(pred,on="game_pk").merge(outcome[["game_pk","regulation_total","final_total","extra_inning","shortened_game"]],on="game_pk")
    d["model_over_probability"]=[model_over(r,r.pinnacle_total_line) for _,r in d.iterrows()]
    d["model_minus_pinnacle_line"]=d.expected_total-d.pinnacle_total_line
    d["actual_residual_from_pinnacle_line"]=d.final_total-d.pinnacle_total_line
    d["result"]=np.where(d.final_total>d.pinnacle_total_line,"OVER",np.where(d.final_total<d.pinnacle_total_line,"UNDER","PUSH"))
    d["over_binary"]=np.where(d.result.eq("PUSH"),np.nan,d.result.eq("OVER").astype(float))
    d.to_csv(OUT/"totals_pinnacle_join.csv",index=False)
    scored=d.dropna(subset=["over_binary"]); rows=[]
    for label,pcol in [("PROPPADIA_TOTALS_V1_AT_PINNACLE_LINE","model_over_probability"),("PINNACLE_NO_VIG_OVER","pinnacle_over_no_vig_probability")]:
        rows.append({"model":label,**metrics(scored.over_binary,scored[pcol]),"pushes":int(d.result.eq("PUSH").sum()),
                     "expected_total_mae":float(np.mean(abs(d.expected_total-d.final_total))) if label.startswith("PRO") else np.nan,
                     "line_mae":float(np.mean(abs(d.pinnacle_total_line-d.final_total))) if label.startswith("PIN") else np.nan,
                     "distribution_crps":float(d.distribution_crps.mean()) if label.startswith("PRO") else np.nan})
    comp=pd.DataFrame(rows); comp["brier_difference_model_minus_pinnacle"]=rows[0]["brier"]-rows[1]["brier"]
    comp["log_loss_difference_model_minus_pinnacle"]=rows[0]["log_loss"]-rows[1]["log_loss"]
    comp.to_csv(OUT/"totals_model_vs_pinnacle.csv",index=False)
    d["band"]=pd.cut(d.model_minus_pinnacle_line.abs(),[-1,.25,.5,.75,1,1.5,np.inf],labels=["<0.25","0.25-0.49","0.50-0.74","0.75-0.99","1.00-1.49",">=1.50"],right=False)
    bands=[]
    for (band,direction),g in d.groupby(["band",np.where(d.model_minus_pinnacle_line>=0,"MODEL_OVER_LINE","MODEL_UNDER_LINE")],observed=True):
        s=g.dropna(subset=["over_binary"]); bands.append({"band":band,"direction":direction,"games":len(g),"pushes":int(g.result.eq("PUSH").sum()),
          "mean_model_signed_difference":g.model_minus_pinnacle_line.mean(),"mean_actual_residual":g.actual_residual_from_pinnacle_line.mean(),
          "model_brier":metrics(s.over_binary,s.model_over_probability)["brier"] if len(s) else np.nan,
          "pinnacle_brier":metrics(s.over_binary,s.pinnacle_over_no_vig_probability)["brier"] if len(s) else np.nan,
          "model_closer_count":int((abs(g.expected_total-g.final_total)<abs(g.pinnacle_total_line-g.final_total)).sum()),
          "pinnacle_closer_count":int((abs(g.expected_total-g.final_total)>abs(g.pinnacle_total_line-g.final_total)).sum())})
    pd.DataFrame(bands).to_csv(OUT/"totals_disagreement_bands.csv",index=False)
    scored=scored.copy(); scored["pinnacle_logit"]=logit(scored.pinnacle_over_no_vig_probability); scored["model_logit"]=logit(scored.model_over_probability)
    inc,test,decision=fit_incremental(scored,"over_binary",["pinnacle_total_line","pinnacle_logit"],["pinnacle_total_line","pinnacle_logit","expected_total","model_logit"],"TOTALS")
    inc.to_csv(OUT/"totals_incremental_information_test.csv",index=False)
    return d,comp,test,decision

def finish(m,md,mc,mt,td,tc,tt,mdec,tdec):
    # Run-line feasibility.
    events=m[m.market.eq("spreads") & m.game_pk.notna()]
    stats=[]
    for pk,g in events.groupby("game_pk"):
        standard=g.point.abs().eq(1.5).all() and set(np.round(g.point,1))=={-1.5,1.5}
        stats.append({"game_pk":int(pk),"standard_plus_minus_1_5":standard,"paired_prices":standard and g.price.notna().all(),"timestamp_present":g.market_last_update.notna().all(),"mapping_exact":g.mapping_status.str.startswith("EXACT").all()})
    rs=pd.DataFrame(stats); denom=m.game_pk.dropna().nunique()
    summary=pd.DataFrame([{"historical_market_events":denom,"spread_games":len(rs),"spread_game_coverage":len(rs)/denom if denom else 0,
      "standard_plus_minus_1_5_games":int(rs.standard_plus_minus_1_5.sum()) if len(rs) else 0,"paired_price_games":int(rs.paired_prices.sum()) if len(rs) else 0,
      "timestamp_complete_games":int(rs.timestamp_present.sum()) if len(rs) else 0,"exact_mapping_games":int(rs.mapping_exact.sum()) if len(rs) else 0,
      "decision":"RUN_LINE_HISTORY_READY_FOR_MODEL_RESEARCH" if len(rs)>=100 and rs.paired_prices.mean()>=.7 else "RUN_LINE_HISTORY_NOT_READY"}])
    summary.to_csv(OUT/"run_line_historical_feasibility.csv",index=False)
    # Stability: test-set diagnostic deltas by month, rolling 50, and fixed strength/confidence slices.
    rows=[]
    def add_slices(test,lane,y):
        test=test.sort_values(["game_date","game_pk"]).copy(); test["month"]=test.game_date.str[:7]
        groups=[("month",test.groupby("month"))]
        test["rolling_50_block"]=(np.arange(len(test))//50).astype(int); groups.append(("rolling_50",test.groupby("rolling_50_block")))
        if lane=="MONEYLINE":
            test["model_confidence_band"]=pd.cut(abs(test.home_win_probability-.5),[-1,.05,.1,.2,.5],labels=["0-.05",".05-.10",".10-.20",">=.20"])
            test["market_strength_band"]=pd.cut(abs(test.pinnacle_home_no_vig_probability-.5),[-1,.05,.1,.2,.5],labels=["0-.05",".05-.10",".10-.20",">=.20"])
        else:
            test["model_confidence_band"]=pd.cut(abs(test.model_over_probability-.5),[-1,.05,.1,.2,.5],labels=["0-.05",".05-.10",".10-.20",">=.20"])
            test["market_strength_band"]=pd.cut(test.pinnacle_total_line,[0,7.5,8.5,9.5,99],labels=["<7.5","7.5-8.49","8.5-9.49",">=9.5"])
        groups += [("model_confidence",test.groupby("model_confidence_band",observed=True)),("market_strength_or_line",test.groupby("market_strength_band",observed=True))]
        for typ,grp in groups:
            for val,g in grp:
                if len(g)<5: continue
                a=metrics(g[y],g.market_only_diagnostic_probability); b=metrics(g[y],g.market_plus_model_diagnostic_probability)
                rows.append({"lane":lane,"slice_type":typ,"slice_value":val,"games":len(g),"market_only_brier":a["brier"],"market_plus_model_brier":b["brier"],
                             "brier_delta_B_minus_A":b["brier"]-a["brier"],"market_only_log_loss":a["log_loss"],"market_plus_model_log_loss":b["log_loss"],"log_loss_delta_B_minus_A":b["log_loss"]-a["log_loss"]})
    add_slices(mt,"MONEYLINE","winner_home"); add_slices(tt,"TOTALS","over_binary")
    pd.DataFrame(rows).to_csv(OUT/"incremental_signal_temporal_stability.csv",index=False)
    manifest=pd.read_csv(OUT/"pinnacle_historical_snapshot_manifest.csv"); cost=int(manifest.request_cost.fillna(0).sum())
    mdecision={"INCREMENTAL_INFORMATION_PRESENT":"MONEYLINE_INCREMENTAL_INFORMATION_PRESENT","NO_INCREMENTAL_INFORMATION":"MONEYLINE_NO_INCREMENTAL_INFORMATION_VS_PINNACLE","MIXED":"MONEYLINE_PINNACLE_BENCHMARK_RESULT_MIXED"}[mdec]
    tdecision={"INCREMENTAL_INFORMATION_PRESENT":"TOTALS_INCREMENTAL_INFORMATION_PRESENT","NO_INCREMENTAL_INFORMATION":"TOTALS_NO_INCREMENTAL_INFORMATION_VS_PINNACLE","MIXED":"TOTALS_PINNACLE_BENCHMARK_RESULT_MIXED"}[tdec]
    (OUT/"historical_vs_prospective_consistency.md").write_text("# Historical versus prospective consistency\n\nThe frozen historical populations end on 2026-08-04. Existing August 5/6 shadow files are prediction-only and do not yet contain certified outcomes sufficient for a like-for-like scored comparison. They were not merged into training. Directional prospective consistency is therefore **not yet established**; this is an explicit evidence boundary, not a negative result.\n")
    report=f"""# MLB Pinnacle Incremental-Information Benchmark v1

Experiment: `MLB_PINNACLE_INCREMENTAL_INFORMATION_BENCHMARK_V1`

## Result

- Historical request window: {START} through {END}; one frozen slate snapshot at or before 60 minutes before the earliest certified first pitch, on {int((manifest.status=='ACQUIRED').sum())} recoverable game dates.
- API credits consumed: {cost}.
- Moneyline exact joins: {len(md)} games. Totals exact joins: {len(td)} games.
- Moneyline Brier: model {mc.iloc[0].brier:.6f}, Pinnacle {mc.iloc[1].brier:.6f}; log loss: model {mc.iloc[0].log_loss:.6f}, Pinnacle {mc.iloc[1].log_loss:.6f}.
- Moneyline decision: `{mdecision}`.
- Totals Brier at posted line: model {tc.iloc[0].brier:.6f}, Pinnacle {tc.iloc[1].brier:.6f}; log loss: model {tc.iloc[0].log_loss:.6f}, Pinnacle {tc.iloc[1].log_loss:.6f}.
- Totals decision: `{tdecision}`.
- Spread decision: `{summary.iloc[0].decision}`.

## Interpretation

Disagreement bands are fixed ex ante and reported separately; they are descriptive, not wager selectors. Temporal slices test whether diagnostic improvement is distributed. Pinnacle already captures the dominant consensus information embodied in price, favorite strength, and posted total. Any residual contribution is limited to the chronological diagnostic deltas reported in the incremental-test files and does not establish profitability, closing-line value, or a staking rule.

No underlying model was refit, nothing was deployed, and no prospective ledger or public prediction behavior was changed.
"""
    (OUT/"concise_mlb_pinnacle_incremental_information_benchmark_v1.md").write_text(report)
    # Hash all final products except the hash file itself.
    files=sorted(p for p in OUT.iterdir() if p.is_file() and p.name!="reproducibility_hashes.sha256")
    (OUT/"reproducibility_hashes.sha256").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))

def cost_audit():
    manifest=pd.read_csv(OUT/"pinnacle_historical_snapshot_manifest.csv") if (OUT/"pinnacle_historical_snapshot_manifest.csv").exists() else pd.DataFrame()
    actual=int(manifest.request_cost.fillna(0).sum()) if len(manifest) else 0
    text=f"""# Pinnacle historical acquisition cost audit

## Contract and frozen policy

- Endpoint: `GET /v4/historical/sports/baseball_mlb/odds`.
- Bookmaker filter: `bookmakers=pinnacle`; markets: `h2h,totals,spreads`.
- Provider history begins 2020-06-06. Snapshot resolution is 10 minutes historically through September 2022 and 5 minutes thereafter; the endpoint returns the closest snapshot at or before the requested timestamp.
- Historical charge: 10 credits per market per region-equivalent. Three markets and one Pinnacle bookmaker cost 30 credits per requested date.
- Frozen primary policy: one slate observation requested 60 minutes before that date's earliest certified scheduled first pitch. No post-start observations are accepted. Exact historical model-time timestamps were unavailable.

## Pre-acquisition estimate

The zero-credit quota check showed 87,371 credits remaining on the owner's 100k plan. A calendar-day upper bound for 2026-05-01 through 2026-08-09 was 101 x 30 = 3,030 credits (3.47% of remaining); four observations per day would be 12,120 (13.87%). A full-season-through-August-9 upper bound of 138 days was 4,140 credits once daily or 16,560 four times daily. The once-per-game-date design was accepted as modest; no performance-driven extra snapshots were requested.

## Actual

Requests were limited to recoverable certified game dates. Recorded historical credits consumed: {actual}. Raw JSON, requested/provider timestamps, response hashes, and quota headers are in the manifest. Dates without a certified schedule were not charged. The provider response's immutable raw body is retained under `raw/`.
"""
    (OUT/"pinnacle_historical_acquisition_cost_audit.md").write_text(text)

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--acquire",action="store_true"); args=ap.parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    if args.acquire:
        key=os.environ.get("ODDS_API_KEY","").strip()
        if not key: raise SystemExit("ODDS_API_KEY missing")
        acquire(key)
    cost_audit()
    m=bind_events(parse_market()); md,mc,mt,mdec=moneyline(m); td,tc,tt,tdec=totals(m); finish(m,md,mc,mt,td,tc,tt,mdec,tdec)

if __name__=="__main__": main()
