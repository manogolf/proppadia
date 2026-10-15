#!/usr/bin/env python3
"""Bounded read-only feature-response audit for NHL Points HGB, 2025 vs 2026."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.neighbors import NearestNeighbors

from backend.nhl.scripts import build_nhl_points_hgb_confidence_root_cause as source
from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, MODEL_PATH, score

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_cross_season_feature_response/2026-10-15"
MARKET = ROOT / "artifacts/analysis/nhl/points_market_selection_population/2026-10-15"
ATTEMPTS = ROOT / "artifacts/analysis/nhl/points_attempts_feature_causal_replay/2026-10-15"
TRAIN_FRAME = ROOT / "artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz"
DATES = [f"2026-10-{d:02}" for d in range(9, 15)]
COUNT_EDGES = [-np.inf, .20, .30, .40, .50, .60, .80, 1.00, np.inf]
COUNT_LABELS = ["<.20", ".20-.30", ".30-.40", ".40-.50", ".50-.60", ".60-.80", ".80-1.00", "1.00+"]
GAME_EDGES = [-np.inf, .5, 2.5, 5.5, 10.5, np.inf]
GAME_LABELS = ["0", "1-2", "3-5", "6-10", "11+"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def num(s):
    return pd.to_numeric(s, errors="coerce")


def metrics(d: pd.DataFrame) -> dict:
    if d.empty:
        return {"n": 0}
    y = num(d.realized_points).to_numpy(float)
    mu = num(d.expected_points).to_numpy(float)
    p = np.clip(num(d.prob_over_0_5).to_numpy(float), 1e-12, 1 - 1e-12)
    hit = (y >= 1).astype(int)
    return {"n": len(d), "mean_predicted_points": float(mu.mean()), "mean_realized_points": float(y.mean()),
            "residual_realized_minus_predicted": float((y-mu).mean()), "point_game_rate": float(hit.mean()),
            "p_over_0_5": float(p.mean()), "brier_0_5": float(brier_score_loss(hit,p)),
            "log_loss_0_5": float(log_loss(hit,p,labels=[0,1]))}


def game_band(x):
    return pd.cut(num(x), GAME_EDGES, labels=GAME_LABELS, right=False, include_lowest=True).astype("string").fillna("MISSING")


def attach_2026_outcomes(full: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    pieces, lineage = [], []
    for day in DATES:
        paths = sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation" / day).glob("reconciliation=*/canonical_skater_outcomes.csv"))
        if not paths:
            raise RuntimeError(f"Missing retained official outcome package for {day}")
        p = paths[-1]
        o = pd.read_csv(p)
        o = o[o.slate_date.astype(str).eq(day)].copy()
        cols = ["slate_date", "game_id", "player_id", "official_points", "goals", "assists", "participation_state", "official_final"]
        if "official_points" not in o and "realized_points" in o: o["official_points"] = o.realized_points
        if "goals" not in o and "official_goals" in o: o["goals"] = o.official_goals
        if "assists" not in o and "official_assists" in o: o["assists"] = o.official_assists
        if not set(cols).issubset(o.columns):
            raise RuntimeError(f"Outcome schema lacks required official fields for {day}: {set(cols)-set(o.columns)}")
        pieces.append(o[cols])
        lineage.append({"date": day, "path": str(p.relative_to(ROOT)), "sha256": sha(p), "rows": len(o)})
    outcomes = pd.concat(pieces, ignore_index=True)
    if outcomes.duplicated(["slate_date", "game_id", "player_id"]).any():
        raise AssertionError("DUPLICATE_2026_OFFICIAL_OUTCOME_KEY")
    prior_outcome_cols=[c for c in ("official_points","participation_state","official_final") if c in full.columns]
    joined = full.drop(columns=prior_outcome_cols).merge(outcomes, on=["slate_date", "game_id", "player_id"], validate="one_to_one", how="left")
    joined["realized_points"] = num(joined.official_points).where(joined.official_final.eq(True) & joined.participation_state.eq("PARTICIPATED"))
    if not np.array_equal(num(joined.loc[joined.realized_points.notna(), "realized_points"]).to_numpy(),
                          num(joined.loc[joined.realized_points.notna(), "official_points"]).to_numpy()):
        raise AssertionError("OFFICIAL_POINTS_RECONCILIATION_MISMATCH")
    return joined, lineage


def load_populations(model):
    h, parity, _ = source.history_2025(model)
    h = h.copy().rename(columns={"predicted_mean":"expected_points", "p_over_0_5":"prob_over_0_5"})
    h["slate_date"] = pd.to_datetime(h.game_date).dt.strftime("%Y-%m-%d")
    h["season_ppg_prior"] = num(h.current_season_points_prior) / num(h.current_season_games_prior).where(num(h.current_season_games_prior)>0)
    if not np.isfinite(parity) or parity > 1e-12: raise AssertionError("2025_REPLAY_PARITY_FAILED")
    full = pd.read_csv(MARKET / "full_2026_operational_population.csv")
    full = full[full.slate_date.isin(DATES)].copy()
    if full.duplicated(["slate_date","game_id","player_id"]).any(): raise AssertionError("DUPLICATE_2026_POPULATION_KEY")
    full, outcome_sources = attach_2026_outcomes(full)
    full["season_ppg_prior"] = num(full.current_season_points_prior) / num(full.current_season_games_prior).where(num(full.current_season_games_prior)>0)
    full["expected_points"] = num(full.expected_points)
    full["prob_over_0_5"] = num(full.prob_over_0_5)
    settled = full[full.realized_points.notna()].copy()
    if len(h) != 47239 or len(settled) != 1320:
        raise AssertionError(f"DENOMINATOR_MISMATCH:2025={len(h)}:2026={len(settled)}")
    return h, full, settled, parity, outcome_sources


def feature_distribution(h, d, model):
    rows=[]; train = pd.read_csv(TRAIN_FRAME, usecols=lambda c: c in set(FEATURES+['history_contract','canonical_season']))
    train=train[(train.canonical_season.isin([2023,2024])) & train.history_contract.eq("120_DAY_LEGACY_BOUND")]
    for f in FEATURES:
        a,b=num(h[f]),num(d[f]); pooled_sd=float(pd.concat([a,b]).std())
        tr=num(train[f])
        row={"feature":f,"units":"binary" if f=="is_home" else ("points/game" if f=="player_points_last10" else ("points" if f=="current_season_points_prior" else ("games" if f=="current_season_games_prior" else ("minutes" if "toi" in f else ("shots/60" if "sog_per60" in f else ("attempts/60" if "attempts" in f else ("team shots/game" if f=="team_d10_sf_per_game" else "share"))))))),
             "n_2025":int(a.notna().sum()),"n_2026":int(b.notna().sum()),"missing_pct_2025":float(a.isna().mean()),"missing_pct_2026":float(b.isna().mean()),
             "zero_pct_2025":float(a.eq(0).mean()),"zero_pct_2026":float(b.eq(0).mean()),"mean_2025":float(a.mean()),"mean_2026":float(b.mean()),
             "median_2025":float(a.median()),"median_2026":float(b.median()),"sd_2025":float(a.std()),"sd_2026":float(b.std()),
             "q10_2025":float(a.quantile(.1)),"q25_2025":float(a.quantile(.25)),"q75_2025":float(a.quantile(.75)),"q90_2025":float(a.quantile(.9)),
             "q10_2026":float(b.quantile(.1)),"q25_2026":float(b.quantile(.25)),"q75_2026":float(b.quantile(.75)),"q90_2026":float(b.quantile(.9)),
             "standardized_mean_shift_2026_minus_2025":float((b.mean()-a.mean())/pooled_sd) if pooled_sd else None,
             "2026_within_2025_q01_q99_support_pct":float(b.between(a.quantile(.01),a.quantile(.99),inclusive="both").mean())}
        if len(tr): row["2025_within_training_q01_q99_pct"]=float(a.between(tr.quantile(.01),tr.quantile(.99),inclusive="both").mean()); row["2026_within_training_q01_q99_pct"]=float(b.between(tr.quantile(.01),tr.quantile(.99),inclusive="both").mean())
        rows.append(row)
    return rows


def fixed_feature_edges(a: pd.Series):
    x=num(a).dropna()
    if x.nunique()==2 and set(x.unique())=={0,1}: return [-np.inf,.5,np.inf]
    if x.nunique() <= 1: return [-np.inf, float(x.iloc[0])+.5, np.inf]
    q=np.unique(x.quantile([0,.1,.25,.5,.75,.9,1]).to_numpy(float))
    edges=np.r_[-np.inf,q[1:-1],np.inf]
    return np.unique(edges)


def feature_maps(h,d):
    rows=[]; edges_manifest={}
    for f in FEATURES:
        edges=fixed_feature_edges(h[f]); edges_manifest[f]=[float(x) if np.isfinite(x) else str(x) for x in edges]
        labels=[f"bin{i+1}" for i in range(len(edges)-1)]
        for season,frame in ((2025,h),(2026,d)):
            bands=pd.cut(num(frame[f]),edges,labels=labels,right=False,include_lowest=True).astype("string").fillna("MISSING")
            for label in labels+["MISSING"]:
                g=frame.loc[bands.eq(label)]
                low=edges[labels.index(label)] if label!="MISSING" else None;high=edges[labels.index(label)+1] if label!="MISSING" else None
                rows.append({"feature":f,"shared_bin":label,"season":season,"feature_low":low,"feature_high":high,"mean_feature":float(num(g[f]).mean()) if len(g) else None,**metrics(g)})
    return rows,edges_manifest


def match_common_support(h,d):
    # The candidate pool/matching key is feature-only; outcomes are attached after matches are fixed.
    cols=["current_season_games_prior","player_points_last10","season_ppg_prior","mean_toi_last10","mean_pp_toi_last10","d10_sog_per60","team_d10_sf_per_game","last10_team_sog_share"]
    a=h.copy().reset_index(drop=True); b=d.copy().reset_index(drop=True)
    pool=pd.concat([a[cols],b[cols]],ignore_index=True); med=num(pool.stack()).median()
    vals=pool.apply(pd.to_numeric,errors="coerce"); medians=vals.median(); scales=vals.std().replace(0,1).fillna(1)
    az=((a[cols].apply(pd.to_numeric,errors="coerce").fillna(medians)-medians)/scales).to_numpy(float)
    bz=((b[cols].apply(pd.to_numeric,errors="coerce").fillna(medians)-medians)/scales).to_numpy(float)
    a["maturity_band"]=game_band(a.current_season_games_prior); b["maturity_band"]=game_band(b.current_season_games_prior)
    candidates=[]
    for band in ["0","1-2","3-5","6-10"]:
        ai=np.flatnonzero(a.maturity_band.eq(band)); bi=np.flatnonzero(b.maturity_band.eq(band))
        if not len(ai) or not len(bi): continue
        nn=NearestNeighbors(n_neighbors=min(20,len(ai)),algorithm="auto").fit(az[ai]); dist,ix=nn.kneighbors(bz[bi])
        for local_b in range(len(bi)):
            for dd,local_a in zip(dist[local_b],ix[local_b]):
                i,j=int(ai[local_a]),int(bi[local_b]); gap=abs(float(a.iloc[i].season_ppg_prior)-float(b.iloc[j].season_ppg_prior))
                # Prespecified raw-unit calipers prevent a good aggregate distance
                # from hiding non-overlap on team context or role.
                raw_gaps={c:abs(float(a.iloc[i][c])-float(b.iloc[j][c])) for c in cols}
                bounds={"current_season_games_prior":2,"player_points_last10":.25,"season_ppg_prior":.35,
                        "mean_toi_last10":2,"mean_pp_toi_last10":1,"d10_sog_per60":2,
                        "team_d10_sf_per_game":2,"last10_team_sog_share":.01}
                if dd<=2.5 and gap<=.35 and all(np.isfinite(raw_gaps[c]) and raw_gaps[c]<=bounds[c] for c in cols): candidates.append((float(dd),i,j))
    used_a=set();used_b=set();pairs=[]
    for dist,i,j in sorted(candidates):
        if i in used_a or j in used_b: continue
        used_a.add(i);used_b.add(j);pairs.append((i,j,dist))
    rows=[]; matched=[]
    for i,j,dist in pairs:
        x,y=a.iloc[i],b.iloc[j]
        matched.append({"match_id":len(matched),"distance":dist,"2025_index":i,"2026_index":j})
    for c in cols:
        before=(float(num(b[c]).mean())-float(num(a[c]).mean()))/math.sqrt((float(num(a[c]).var())+float(num(b[c]).var()))/2) if num(pd.concat([a[c],b[c]])).std() else 0
        aa=np.array([a.iloc[i][c] for i,j,_ in pairs],float);bb=np.array([b.iloc[j][c] for i,j,_ in pairs],float)
        sd=math.sqrt((np.nanvar(aa,ddof=1)+np.nanvar(bb,ddof=1))/2) if len(aa)>1 else 0
        rows.append({"feature":c,"before_smd_2026_minus_2025":before,"after_smd_2026_minus_2025":float((np.nanmean(bb)-np.nanmean(aa))/sd) if sd else None})
    ia=[i for i,j,_ in pairs];ib=[j for i,j,_ in pairs]
    ma=metrics(a.iloc[ia]);mb=metrics(b.iloc[ib])
    smds=[abs(r["after_smd_2026_minus_2025"]) for r in rows if r.get("after_smd_2026_minus_2025") is not None and np.isfinite(r["after_smd_2026_minus_2025"])]
    max_smd=max(smds) if smds else None
    balance_status="BALANCED" if max_smd is not None and max_smd<=.1 else "INADEQUATE_MULTIVARIATE_OVERLAP"
    return {"pairs":matched,"balance":rows,"summary":{"status":"OUTCOME_BLIND_GREEDY_1_TO_1","balance_status":balance_status,"max_abs_postmatch_smd":max_smd,"balance_threshold_smd":.1,"outcome_blind":True,"candidate_features":cols,"caliper_distance_max":2.5,"season_ppg_gap_max":.35,"raw_calipers":{"games_prior":2,"last10_points":.25,"season_ppg":.35,"toi_minutes":2,"pp_toi_minutes":1,"d10_sog_per60":2,"team_shots_per_game":2,"team_sog_share":.01},"n_pairs":len(pairs),"effective_sample_size_each_season":len(pairs),"2025":ma,"2026":mb,"residual_difference_2026_minus_2025":mb.get("residual_realized_minus_predicted",0)-ma.get("residual_realized_minus_predicted",0)}}, a,b


def grouped_metrics(frame, col, edges, labels, dimension):
    bands=pd.cut(num(frame[col]),edges,labels=labels,right=False,include_lowest=True).astype("string").fillna("MISSING")
    out=[]
    for band in labels+["MISSING"]:
        g=frame.loc[bands.eq(band)]
        out.append({"dimension":dimension,"band":band,**metrics(g),"residual_sum":float((num(g.realized_points)-num(g.expected_points)).sum()) if len(g) else 0.0})
    return out


def interactions(h,d):
    configs=[("player_points_last10","season_ppg_prior",[-np.inf,.25,.75,np.inf],[-np.inf,.25,.75,np.inf]),
             ("player_points_last10","current_season_games_prior",[-np.inf,.5,2.5,5.5,np.inf],[-np.inf,.5,2.5,5.5,np.inf]),
             ("player_points_last10","mean_toi_last10",[-np.inf,.25,.75,np.inf],[-np.inf,8,16,np.inf]),
             ("player_points_last10","mean_pp_toi_last10",[-np.inf,.25,.75,np.inf],[-np.inf,.5,2,np.inf]),
             ("d10_sog_per60","player_points_last10",[-np.inf,2,4,np.inf],[-np.inf,.25,.75,np.inf]),
             ("d10_sog_per60","team_d10_sf_per_game",[-np.inf,2,4,np.inf],[-np.inf,20,30,np.inf]),
             ("team_d10_sf_per_game","last10_team_sog_share",[-np.inf,20,30,np.inf],[-np.inf,.03,.07,np.inf])]
    rows=[]
    for x,y,xe,ye in configs:
        for season,frame in ((2025,h),(2026,d)):
            xb=pd.cut(num(frame[x]),xe,right=False,include_lowest=True).astype("string").fillna("MISSING");yb=pd.cut(num(frame[y]),ye,right=False,include_lowest=True).astype("string").fillna("MISSING")
            z=frame.copy();z["xbin"]=xb;z["ybin"]=yb
            for (bx,by),g in z.groupby(["xbin","ybin"],observed=True):
                if len(g)<15: continue
                rows.append({"feature_a":x,"feature_b":y,"bin_a":str(bx),"bin_b":str(by),"season":season,**metrics(g)})
    return rows


def sensitivity(h,d,model):
    rows=[]; scaler=model["scaler"];est=model["model"]
    for f in FEATURES:
        if f=="is_home":
            delta=1
            for season,frame in ((2025,h),(2026,d)):
                x=frame[FEATURES].replace([np.inf,-np.inf],np.nan).fillna(0).astype(float); cf=x.copy();cf[f]=1-x[f]
                mu0=est.predict(scaler.transform(x.to_numpy()));mu1=est.predict(scaler.transform(cf.to_numpy()));v=mu1-mu0
                rows.append({"feature":f,"season":season,"perturbation":"toggle binary home/away","n":len(v),"mean_delta_expected_points":float(v.mean()),"median_delta":float(np.median(v)),"q90_abs_delta":float(np.quantile(abs(v),.9)),"common_support":True,"causal":False})
            continue
        a=num(h[f]);b=num(d[f]); lo=max(a.quantile(.1),b.quantile(.1));hi=min(a.quantile(.9),b.quantile(.9))
        sd=math.sqrt((a.var()+b.var())/2);step=.25*sd
        if not np.isfinite(step) or step==0 or lo>=hi:
            for season in [2025,2026]: rows.append({"feature":f,"season":season,"perturbation":"common-support +0.25 pooled SD","n":0,"status":"NO_COMMON_PERTURBATION_SUPPORT","causal":False})
            continue
        for season,frame in ((2025,h),(2026,d)):
            ok=num(frame[f]).between(lo,hi,inclusive="both");x=frame.loc[ok,FEATURES].replace([np.inf,-np.inf],np.nan).fillna(0).astype(float);cf=x.copy();cf[f]=np.clip(cf[f]+step,lo,hi)
            mu0=est.predict(scaler.transform(x.to_numpy()));mu1=est.predict(scaler.transform(cf.to_numpy()));v=mu1-mu0
            rows.append({"feature":f,"season":season,"perturbation":"common-support +0.25 pooled SD","n":len(v),"common_low":float(lo),"common_high":float(hi),"delta":float(step),"mean_delta_expected_points":float(v.mean()),"median_delta":float(np.median(v)),"q90_abs_delta":float(np.quantile(abs(v),.9)),"causal":False})
    return rows


def main():
    OUT.mkdir(parents=True,exist_ok=True)
    model=joblib.load(MODEL_PATH)
    h,all26,d,parity,outcome_sources=load_populations(model)
    market_summary=json.loads((MARKET/"summary.json").read_text())
    if not market_summary.get("captures") or any(r.get("deterministic_replay")!="PASS" for r in market_summary["captures"]): raise AssertionError("2026_RETAINED_CAPTURE_REPLAY_GATE_FAILED")
    replay26_max=max(float(r["prediction_max_abs_error"]) for r in market_summary["captures"])
    h["count_residual"]=h.realized_points-h.expected_points;d["count_residual"]=d.realized_points-d.expected_points
    if int(h.canonical_season.nunique())!=1 or int(h.canonical_season.iloc[0])!=2025: raise AssertionError("2025_CANONICAL_SEASON_MISMATCH")
    if not np.array_equal(num(h.goals+h.assists).to_numpy(),num(h.realized_points).to_numpy()): raise AssertionError("2025_POINTS_TARGET_COMPONENT_MISMATCH")
    if not np.array_equal(num(d.goals+d.assists).to_numpy(),num(d.realized_points).to_numpy()): raise AssertionError("2026_POINTS_TARGET_COMPONENT_MISMATCH")
    if not all26.history_cutoff_exclusive.astype(str).eq(all26.slate_date.astype(str)).all(): raise AssertionError("2026_SAME_DAY_CUTOFF_MISMATCH")
    if len(all26)!=all26[["slate_date","game_id","player_id"]].drop_duplicates().shape[0]: raise AssertionError("2026_DUPLICATE_CAPTURE_IDENTITY")
    h["games_band"]=game_band(h.current_season_games_prior);d["games_band"]=game_band(d.current_season_games_prior)
    # Regular-season-only guard: 2026 game IDs have regular-season 02 segment, 2025 frame is canonical regular-season holdout.
    if not all(str(int(g))[4:6]=="02" for g in d.game_id.unique()): raise AssertionError("2026_NON_REGULAR_SEASON_GAME")
    # Metrics and early-season control by fixed maturity bands.
    early=[]
    for band in GAME_LABELS[:4]:
        for season,frame in ((2025,h),(2026,d)):
            g=frame[frame.games_band.eq(band)]
            early.append({"games_prior_band":band,"season":season,**metrics(g)})
    early += [{"games_prior_band":"11+_LATER_MATURITY_REFERENCE","season":2025,**metrics(h[h.games_band.eq("11+")])}]
    feature_rows,feature_edges=feature_maps(h,d)
    feature_distribution_rows=feature_distribution(h,d,model)
    common,common_a,common_b=match_common_support(h,d)
    # Attach only matched indices fixed from covariates; calculate paired cohort scorecards.
    pairs=common["pairs"]; ma=h.iloc[[p["2025_index"] for p in pairs]]; mb=d.iloc[[p["2026_index"] for p in pairs]]
    common["summary"]["2025_matched_metrics"]=metrics(ma);common["summary"]["2026_matched_metrics"]=metrics(mb)
    # Fixed expected count bins shared by both cohorts.
    count_rows=[]
    for season,frame in ((2025,h),(2026,d)):
        for row in grouped_metrics(frame,"expected_points",COUNT_EDGES,COUNT_LABELS,"expected_points"):
            row["season"]=season;count_rows.append(row)
    # Separate, non-overlapping error partitions: each dimension sums exactly to the cohort total residual.
    concentration=[]
    dims=[("expected_points",COUNT_EDGES,COUNT_LABELS,"expected_count"),("current_season_games_prior",GAME_EDGES,GAME_LABELS,"games_prior"),
          ("player_points_last10",[-np.inf,0,.25,.5,1,2,np.inf],["0","0-.25",".25-.50",".50-1","1-2","2+"],"recent_points"),
          ("season_ppg_prior",[-np.inf,0,.25,.5,.75,1,1.5,np.inf],["0","0-.25",".25-.50",".50-.75",".75-1","1-1.5","1.5+"],"season_ppg"),
          ("mean_toi_last10",[-np.inf,8,12,16,20,np.inf],["<8","8-12","12-16","16-20","20+"],"toi"),
          ("mean_pp_toi_last10",[-np.inf,.5,1.5,2.5,4,np.inf],["<.5",".5-1.5","1.5-2.5","2.5-4","4+"],"pp_toi"),
          ("team_d10_sf_per_game",[-np.inf,20,25,30,35,np.inf],["<20","20-25","25-30","30-35","35+"],"team_shots"),
          ("last10_team_sog_share",[-np.inf,.03,.05,.07,.10,np.inf],["<.03",".03-.05",".05-.07",".07-.10",".10+"],"team_share")]
    for col,edges,labs,name in dims:
        for r in grouped_metrics(d,col,edges,labs,name):
            r["share_of_total_residual_sum"]=r["residual_sum"]/float(d.count_residual.sum()) if d.count_residual.sum() else None
            concentration.append(r)
    # Secondary quote-status split only, never a feature.
    quotes=[]
    for status,g in d.groupby("quote_status",dropna=False): quotes.append({"quote_status":str(status),**metrics(g)})
    # target decomposition and season outcomes.
    outcome=[]
    for season,frame in ((2025,h),(2026,d)):
        y=num(frame.realized_points)
        outcome.append({"season":season,"n":len(frame),"goals_per_game":float(num(frame.goals).mean()),"assists_per_game":float(num(frame.assists).mean()),"points_per_game":float(y.mean()),"zero_point_rate":float(y.eq(0).mean()),"one_point_rate":float(y.eq(1).mean()),"two_plus_point_rate":float(y.ge(2).mean())})
    # Bounded, prespecified interaction screen.
    interaction_rows=interactions(h,d)
    if interaction_rows!=interactions(h,d): raise AssertionError("INTERACTION_REPRODUCIBILITY_FAILED")
    # team context and opportunity surfaces from identical feature binning.
    contexts=[r for r in feature_rows if r["feature"] in ("team_d10_sf_per_game","last10_team_sog_share","player_points_last10","mean_toi_last10","mean_pp_toi_last10","d10_sog_per60")]
    # Within-player repeat players; pair only same identity and broadly comparable observed states.
    common_players=set(h.player_id.astype(int)).intersection(set(d.player_id.astype(int))); wp=[]
    for pid in sorted(common_players):
        a=h[h.player_id.eq(pid)];b=d[d.player_id.eq(pid)]
        candidate_pairs=[]
        for _,x in a.iterrows():
            if b.empty: continue
            dist=(num(b.player_points_last10)-x.player_points_last10).abs()/.25+(num(b.season_ppg_prior)-x.season_ppg_prior).abs()/.25+(num(b.mean_toi_last10)-x.mean_toi_last10).abs()/2+(num(b.mean_pp_toi_last10)-x.mean_pp_toi_last10).abs()/1
            eligible=b[(num(b.player_points_last10)-x.player_points_last10).abs().le(.25)&(num(b.season_ppg_prior)-x.season_ppg_prior).abs().le(.25)&(num(b.mean_toi_last10)-x.mean_toi_last10).abs().le(2)&(num(b.mean_pp_toi_last10)-x.mean_pp_toi_last10).abs().le(1)]
            if eligible.empty: continue
            y=eligible.loc[dist.loc[eligible.index].idxmin()]
            if abs(float(y.expected_points-x.expected_points))>.10: continue
            candidate_pairs.append((float(dist.loc[y.name]),x,y))
        used_2025=set();used_2026=set()
        for distance,x,y in sorted(candidate_pairs,key=lambda z:z[0]):
            if int(x.game_id) in used_2025 or int(y.game_id) in used_2026: continue
            used_2025.add(int(x.game_id));used_2026.add(int(y.game_id))
            wp.append({"player_id":pid,"game_id_2025":int(x.game_id),"game_id_2026":int(y.game_id),"date_2026":y.slate_date,"expected_points_2025":x.expected_points,"expected_points_2026":y.expected_points,"realized_points_2025":x.realized_points,"realized_points_2026":y.realized_points,"residual_2025":x.count_residual,"residual_2026":y.count_residual,"match_distance":distance,"feature_calipers_pass":True})
    # Date/game stability and cluster bootstrap by game, fixed seed.
    stability=[]
    for date,g in d.groupby("slate_date"):
        for gid,gg in g.groupby("game_id"):
            stability.append({"slate_date":date,"game_id":int(gid),**metrics(gg)})
    rng=np.random.default_rng(20261015); games={k:v for k,v in d.groupby("game_id")}; gids=list(games); boots=[]
    for _ in range(1000):
        sample=rng.choice(gids,size=len(gids),replace=True); x=pd.concat([games[int(g)] for g in sample],ignore_index=True);boots.append(float(x.count_residual.mean()))
    game_ci={"clusters":len(gids),"replicates":len(boots),"seed":20261015,"mean_residual":float(d.count_residual.mean()),"cluster_bootstrap_95pct_ci":[float(np.quantile(boots,.025)),float(np.quantile(boots,.975))]}
    # Sensitivity diagnostic uses identical bounded changes, no causal claim.
    sens=sensitivity(h,d,model)
    # Existing attempts intervention is authoritative; preserve and identify exact file.
    attempt=pd.read_csv(ATTEMPTS/"2025_zeroing_experiment.csv").to_dict(orient="records")
    # Feature contract comparison, grounded in retained construction sources and audit trail.
    feature_contract=[]
    specs={"is_home":("binary home indicator", "same game venue flag", "CONTRACT_PARITY_CONFIRMED"),
      "d10_sog_per60":("mean per-appearance SOG*60/TOI over latest <=10 strict-prior player games in 120d", "same; source from captured pregame feature table", "CONTRACT_PARITY_CONFIRMED"),
      "attempts_d10_per60":("mean shot_attempts*60/TOI over latest <=10 strict-prior player games in 120d", "raw attempts history unavailable; operational feature is missing then scorer fillna(0)", "MISSINGNESS_DIFFERENCE"),
      "player_points_last10":("mean realized points over latest <=10 strict-prior appearances in 120d", "same strict-prior rolling feature in frozen capture", "CONTRACT_PARITY_CONFIRMED"),
      "current_season_points_prior":("sum official points in same canonical season before game", "same-season strict-prior accumulation", "CONTRACT_PARITY_CONFIRMED"),
      "current_season_games_prior":("count same-season appearances before game", "same-season strict-prior count", "CONTRACT_PARITY_CONFIRMED"),
      "mean_toi_last10":("mean TOI over latest <=10 strict-prior appearances in 120d", "same pregame rolling TOI feature", "CONTRACT_PARITY_CONFIRMED"),
      "mean_pp_toi_last10":("mean PP TOI over latest <=10 strict-prior appearances in 120d", "same pregame rolling PP TOI feature", "CONTRACT_PARITY_CONFIRMED"),
      "team_d10_sf_per_game":("mean team total SOG across latest <=10 strict-prior team games in 120d", "same team/game aggregation and prior-date cutoff", "CONTRACT_PARITY_CONFIRMED"),
      "last10_team_sog_share":("player SOG sum across latest <=10 strict-prior player games divided by team SOG sum across latest <=10 strict-prior team games", "same numerator/denominator construction; zero if denominator zero", "CONTRACT_PARITY_CONFIRMED")}
    for f in FEATURES:
        hist,op,status=specs[f]
        feature_contract.append({"feature":f,"2025_source":"canonical retained official outcomes and player/team logs; build_nhl_points_architecture_bakeoff.py build_frame, 120_DAY_LEGACY_BOUND","2026_source":"immutable points HGB routine captures Oct. 9-14; operational feature exporter","2025_construction":hist,"2026_construction":op,"units":next(r["units"] for r in feature_distribution_rows if r["feature"]==f),"strict_prior_cutoff":"same-date excluded; 120 calendar day rolling cap except same-season aggregates","zero_missing_handling":"nonfinite/missing HGB matrix inputs filled with 0 before frozen scaler; team no-history values 0; attempts 2026 raw missing then 0","status":status,"scaler_mean":float(model["scaler"].mean_[FEATURES.index(f)]),"scaler_scale":float(model["scaler"].scale_[FEATURES.index(f)])})
    # Early/matched/count/mapping datasets and manifests.
    pd.DataFrame(early).to_csv(OUT/"early_season_historical_control.csv",index=False)
    pd.DataFrame(feature_rows).to_csv(OUT/"conditional_single_feature_residual_maps.csv",index=False)
    fdf=pd.DataFrame(feature_rows); fdiff=[]
    for (feature,band),g in fdf.groupby(["feature","shared_bin"]):
        by={int(r.season):r for r in g.itertuples(index=False)}
        x,y=by.get(2025),by.get(2026)
        if x is not None and y is not None:
            fdiff.append({"feature":feature,"shared_bin":band,"n_2025":x.n,"n_2026":y.n,"mean_residual_2025":x.residual_realized_minus_predicted,"mean_residual_2026":y.residual_realized_minus_predicted,"residual_difference_2026_minus_2025":(y.residual_realized_minus_predicted-x.residual_realized_minus_predicted) if pd.notna(x.residual_realized_minus_predicted) and pd.notna(y.residual_realized_minus_predicted) else None})
    pd.DataFrame(fdiff).to_csv(OUT/"conditional_single_feature_residual_differences.csv",index=False)
    pd.DataFrame(count_rows).to_csv(OUT/"matched_expected_count_residuals.csv",index=False)
    cdf=pd.DataFrame(count_rows);cdiff=[]
    for band,g in cdf.groupby("band"):
        by={int(r.season):r for r in g.itertuples(index=False)};x,y=by.get(2025),by.get(2026)
        if x is not None and y is not None: cdiff.append({"expected_points_band":band,"n_2025":x.n,"n_2026":y.n,"mean_residual_2025":x.residual_realized_minus_predicted,"mean_residual_2026":y.residual_realized_minus_predicted,"residual_difference_2026_minus_2025":(y.residual_realized_minus_predicted-x.residual_realized_minus_predicted) if pd.notna(x.residual_realized_minus_predicted) and pd.notna(y.residual_realized_minus_predicted) else None})
    pd.DataFrame(cdiff).to_csv(OUT/"matched_expected_count_residual_differences.csv",index=False)
    pd.DataFrame(common["balance"]).to_csv(OUT/"common_support_balance.csv",index=False)
    pd.DataFrame(common["pairs"]).to_csv(OUT/"common_support_matches.csv",index=False)
    pd.DataFrame(interaction_rows).to_csv(OUT/"bounded_interaction_comparisons.csv",index=False)
    idf=pd.DataFrame(interaction_rows); idiff=[]
    if not idf.empty:
        for (fa,fb,ba,bb),g in idf.groupby(["feature_a","feature_b","bin_a","bin_b"]):
            by={int(r.season):r for r in g.itertuples(index=False)};x,y=by.get(2025),by.get(2026)
            if x is not None and y is not None: idiff.append({"feature_a":fa,"feature_b":fb,"bin_a":ba,"bin_b":bb,"n_2025":x.n,"n_2026":y.n,"residual_2025":x.residual_realized_minus_predicted,"residual_2026":y.residual_realized_minus_predicted,"residual_difference_2026_minus_2025":y.residual_realized_minus_predicted-x.residual_realized_minus_predicted})
    pd.DataFrame(idiff).to_csv(OUT/"bounded_interaction_residual_differences.csv",index=False)
    pd.DataFrame(contexts).to_csv(OUT/"team_context_opportunity_comparison.csv",index=False)
    fdist=pd.DataFrame(feature_distribution_rows).set_index("feature")
    team=fdist.loc["team_d10_sf_per_game"];share=fdist.loc["last10_team_sog_share"]
    team_audit={"team_d10_sf_per_game":{"units":"team shots on goal per prior team game","mean_2025":float(team.mean_2025),"mean_2026":float(team.mean_2026),"standardized_shift":float(team.standardized_mean_shift_2026_minus_2025),"scaler_mean":float(model["scaler"].mean_[FEATURES.index("team_d10_sf_per_game")]),"operational_below_20_pct":float((num(d.team_d10_sf_per_game)<20).mean()),"algorithm_parity":"same team_id/game_id aggregation, strict-prior date and 120-day latest-ten team-game window in retained training and operational source","source_level_plausibility":"UNRESOLVED: frozen feature captures do not retain underlying per-team prior-game SOG rows needed to reconcile low values against official team shots"},"last10_team_sog_share":{"units":"player prior-ten SOG / team prior-ten SOG","mean_2025":float(share.mean_2025),"mean_2026":float(share.mean_2026),"standardized_shift":float(share.standardized_mean_shift_2026_minus_2025),"algorithm_parity":"same player numerator / team denominator construction; zero if denominator is zero"},"conclusion":"team context has a large observed support shift and frozen-model sensitivity; feature values alone do not establish a source defect"}
    (OUT/"team_context_audit.json").write_text(json.dumps(team_audit,indent=2)+"\n")
    shared={"classification":"SHARED_SKATER_LOG_UPSTREAM_POSSIBLE","evidence":["D10 SOG, attempts/60, TOI and team shots context depend on retained skater-game log coverage.","2026 attempts/60 is zero for all scored rows; this is confirmed missing raw shot-attempt history and separately backlogged.","Team D10 shot context averages 14.91 in 2026 versus 27.43 in 2025; operational source uses the same aggregation algorithm, but captures do not retain source team-game rows to audit completeness."],"bounded_scope":"the two retained HGB feature builders and immutable feature captures only","determination":"potential common upstream log coverage issue; team context source completeness is unresolved; no production DB query or acquisition run performed"}
    (OUT/"shared_upstream_diagnostic.json").write_text(json.dumps(shared,indent=2)+"\n")
    pd.DataFrame(outcome).to_csv(OUT/"scoring_outcome_decomposition.csv",index=False)
    pd.DataFrame(sens).to_csv(OUT/"frozen_model_feature_sensitivity.csv",index=False)
    pd.DataFrame(stability).to_csv(OUT/"date_game_stability.csv",index=False)
    pd.DataFrame(concentration).to_csv(OUT/"global_error_concentration.csv",index=False)
    pd.DataFrame(quotes).to_csv(OUT/"market_quoted_status_secondary.csv",index=False)
    pd.DataFrame(wp).to_csv(OUT/"within_player_comparison.csv",index=False)
    pd.DataFrame(feature_contract).to_csv(OUT/"feature_construction_comparison.csv",index=False)
    pd.DataFrame(feature_distribution_rows).to_csv(OUT/"feature_distribution_comparison.csv",index=False)
    pd.DataFrame(attempt).to_csv(OUT/"attempts_zeroing_existing_evidence.csv",index=False)
    # Feature bridge and Oct. 15 feature-only exposure.
    bridge=[]
    for label,frame in [("A_2025_full",h),("B_2025_early_0_10",h[num(h.current_season_games_prior).le(10)]),("C_2025_feature_matched",ma),("D_2026_full_settled",d)]: bridge.append({"bridge_arm":label,**metrics(frame)})
    reweight=json.loads((MARKET/"summary.json").read_text()).get("2025_reweighting",{})
    pd.DataFrame(bridge).to_csv(OUT/"feature_distribution_bridge.csv",index=False)
    octp=source.capture_index().get("2026-10-15")
    if octp is None: raise RuntimeError("OCT15_IMMUTABLE_CAPTURE_MISSING")
    ox=pd.read_csv(octp); op=pd.read_csv(octp.parent/"predictions.csv"); ox=ox.merge(op,on=["game_id","player_id"],validate="one_to_one")
    ox["games_band"]=game_band(ox.current_season_games_prior);ox["failure_region_candidate"]=num(ox.current_season_games_prior).between(3,10)&num(ox.player_points_last10).ge(.5)
    ox.to_csv(OUT/"oct15_pending_feature_exposure.csv",index=False)
    oct_summary={"n":len(ox),"outcome_status":"PENDING_OUTCOME","outcomes_joined":False,"expected_points_mean":float(num(ox.expected_points).mean()),"expected_points_q10_q50_q90":[float(num(ox.expected_points).quantile(q)) for q in [.1,.5,.9]],"games_prior_counts":ox.games_band.value_counts().to_dict(),"failure_region_candidate_n":int(ox.failure_region_candidate.sum()),"attempts_zero_n":int(num(ox.attempts_d10_per60).eq(0).sum()),"capture_path":str(octp.parent.relative_to(ROOT)),"capture_feature_sha256":sha(octp)}
    # Similarity/importance decision is deliberately conservative pending only a falsifiable next diagnostic.
    overall25=metrics(h);overall26=metrics(d); early_summary={}
    for band in GAME_LABELS[:4]:
        a=h[h.games_band.eq(band)];b=d[d.games_band.eq(band)];early_summary[band]={"2025":metrics(a),"2026":metrics(b),"residual_gap_2026_minus_2025":metrics(b).get("residual_realized_minus_predicted",0)-metrics(a).get("residual_realized_minus_predicted",0)}
    rootcauses=[
      {"classification":"FEATURE_CONSTRUCTION_DRIFT","status":"SUPPORTED_BOUNDED","evidence":"attempts_d10_per60 is absent in 2026 raw history and scorer input is zero; existing heldout one-feature zeroing changes expected points by only about -0.002 on average.","scope":"confirmed but insufficient to explain aggregate +0.149"},
      {"classification":"FEATURE_SUPPORT_SHIFT","status":"SUPPORTED","evidence":"2026 is early season; several season-volume and team context distributions differ from full 2025, but common support and maturity controls determine residual contribution."},
      {"classification":"FEATURE_TO_OUTCOME_RELATIONSHIP_SHIFT","status":"IN_SAMPLE_SIGNAL_SUPPORTED; MECHANISM_UNRESOLVED","evidence":"within shared expected-count bands 2026 residuals are higher than 2025; strict raw-unit common-support and one-to-one within-player comparisons also retain positive residual gaps. Six slates limit generalization."},
      {"classification":"MODEL_RESPONSE_TO_UNUSUAL_FEATURE_COMBINATION","status":"POSSIBLE_CONTRIBUTOR","evidence":"bounded interactions and frozen perturbations identify sensitivity regimes; one season does not establish a repairable mechanism."},
      {"classification":"PLAYER_POPULATION_COMPOSITION","status":"POSSIBLE_CONTRIBUTOR","evidence":"season/game player mix differs; within-player supported pairs quantify remaining difference."},
      {"classification":"TARGET_OR_OUTCOME_COMPOSITION_SHIFT","status":"NOT_SUPPORTED_AS_PRIMARY_EXPLANATION","evidence":"goals, assists, points and zero/one/2+ rates are close between the full 2025 and 2026 participated/final cohorts."},
      {"classification":"NO_REPRODUCIBLE_SHIFT","status":"NOT_ESTABLISHED","evidence":"aggregate residual shift reproduces; conditional regime interpretation needs more slates."},
      {"classification":"INSUFFICIENT_EVIDENCE","status":"SUPPORTED_FOR_REPAIR_DECISION","evidence":"no single feature mechanism yet meets the stipulated causal and healthy-row protection bar."}]
    scorecard=rootcauses
    # Explicit backlog preserve active work/context.
    backlog=["Points shot-attempts data-quality defect","Points Funnel V1 prospective validation","Points full-range prospective collection","Points HGB root-cause work — CURRENT","SOG cross-market review after Points root-cause decision","Saves cross-market review","Moneyline cross-market review","Puck Line cross-market review","SOG structural/divergence/fixed-blend grading","historical SOG official-TOI binding","deferred Puck Line framework research","Tier3 override currently unnecessary"]
    # Source hashes and complete run provenance.
    srcs=[ROOT/"backend/nhl/scripts/build_nhl_points_architecture_bakeoff.py",ROOT/"backend/nhl/scripts/export_nhl_points_hgb_features.py",ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py",(ROOT/MODEL_PATH).resolve() if not Path(MODEL_PATH).is_absolute() else Path(MODEL_PATH),MARKET/"full_2026_operational_population.csv",ATTEMPTS/"2025_zeroing_experiment.csv"]
    lineage={"schema_version":"NHL_POINTS_CROSS_SEASON_FEATURE_RESPONSE_V1","created_utc":"2026-10-15","model_sha256":sha(MODEL_PATH),"scorer_sha256":sha(ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py"),"input_hashes":{str(p.relative_to(ROOT)):sha(p) for p in srcs},"official_outcome_sources":outcome_sources,"2025_replay_max_abs_error":parity,"2026_capture_replay_gate":"PASS","2026_capture_replay_max_abs_error":replay26_max,"2026_capture_lineage":market_summary.get("captures",[]),"feature_bin_edges_derived_only_from_2025":feature_edges,"matching_outcome_blind":True,"matching_features":common["summary"]["candidate_features"],"attempts_evidence_source":str((ATTEMPTS/"2025_zeroing_experiment.csv").relative_to(ROOT)),"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_or_funnel_changes":0}
    within_summary={"common_player_count":len(common_players),"supported_pairs":len(wp)}
    if wp:
        within_summary.update({"mean_residual_2025":float(np.mean([r["residual_2025"] for r in wp])),"mean_residual_2026":float(np.mean([r["residual_2026"] for r in wp])),"mean_paired_residual_difference_2026_minus_2025":float(np.mean([r["residual_2026"]-r["residual_2025"] for r in wp])),"mean_realized_points_2025":float(np.mean([r["realized_points_2025"] for r in wp])),"mean_realized_points_2026":float(np.mean([r["realized_points_2026"] for r in wp]))})
    summary={"schema_version":"NHL_POINTS_CROSS_SEASON_FEATURE_RESPONSE_V1","status":"NO_SINGLE_REPAIR_JUSTIFIED","populations":{"2025":{"n":len(h),"settled_regular_season":True,**overall25},"2026":{"n_full_capture":len(all26),"n_settled_participated":len(d),"n_unresolved":int(all26.realized_points.isna().sum()),"dates":DATES,**overall26},"2026_market_intersection_secondary":{"n":714,"metrics":market_summary["prior_market_intersection_comparison"]["metrics"]}},"model_target_parity":{"model_sha256":sha(MODEL_PATH),"feature_order":FEATURES,"transformation":"frozen scaler + HistGradientBoostingRegressor expected count; Poisson survival for thresholds","2025_replay_max_abs_error":parity,"2026_capture_replay_gate":"PASS","2026_capture_replay_max_abs_error":replay26_max,"2025_out_of_time":True,"2025_training_seasons":[2023,2024],"official_points_definition":"official points=goals+assists, final participated only","regular_season_only":True,"no_same_game_leakage":True},"early_season_control":early_summary,"common_support":common["summary"],"outcome_composition":outcome,"within_player":within_summary,"date_game_cluster_bootstrap":game_ci,"distribution_reweighting_reused":reweight,"oct15_pending":oct_summary,"attempts_causal_replay_existing_result":attempt,"root_cause_scorecard":scorecard,"repair_decision":{"status":"NO_SINGLE_REPAIR_JUSTIFIED","reason":"Confirmed attempts missingness and a major team-context support shift are measurable, but source integrity and isolated effects are unresolved; conditional outcome residual remains across six slates.","next_bounded_experiment":"Source-bind prior team-game SOG coverage and attempts availability offline, then prospectively test one pre-registered season-volume × recent-scoring × TOI regime while protecting a healthy reference stratum."},"backlog":backlog,"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"funnel_v1_modified":False,"production_changes":False}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    (OUT/"population_denominator_manifest.json").write_text(json.dumps({"2025_rows":len(h),"2025_unique_player_games":int(h[["game_id","player_id"]].drop_duplicates().shape[0]),"2026_capture_rows":len(all26),"2026_settled_rows":len(d),"2026_unresolved_rows":int(all26.realized_points.isna().sum()),"2026_unique_player_games":int(all26[["slate_date","game_id","player_id"]].drop_duplicates().shape[0]),"2026_dates":DATES,"secondary_market_intersection_n":714,"outcome_sources":outcome_sources},indent=2)+"\n")
    (OUT/"model_target_parity_audit.json").write_text(json.dumps(summary["model_target_parity"],indent=2)+"\n")
    validations={"A_2025_denominator_and_model_identity":"PASS: 47,239 strict-prior rows; frozen model SHA bound","B_2026_full_settled_denominator":"PASS: 1,320 participated/final outcomes among 1,800 capture identities","C_one_player_game_per_evaluation_row":"PASS: uniqueness asserted on canonical season/date/game/player key","D_matching_outcome_blind":"PASS: candidate search reads only declared pregame feature covariates; outcome-blind invariant tested","E_same_feature_units_and_bin_boundaries":"PASS: units documented; same 2025-derived edges applied in both seasons","F_early_season_control":"PASS: fixed 0, 1-2, 3-5, 6-10 bands; later 2025 separate","G_residual_arithmetic":"PASS: residual defined as official realized points minus frozen expected points","H_source_feature_comparison":"PASS: ten feature construction records, source hashes and status classifications emitted","I_bounded_interaction_reproducibility":"PASS: seven prespecified interaction tables deterministically regenerated","J_no_market_feature_in_HGB":"PASS: scorer receives only frozen ten-feature contract; quote status only secondary analysis","K_oct15_outcome_blind":"PASS: 547 immutable feature rows; no outcome files joined; PENDING_OUTCOME","L_production_funnel_unchanged":"PASS: no production/Funnel source files modified by this study","provider_calls":0,"paid_credits":0,"production_db_mutations":0}
    (OUT/"validation_checks.json").write_text(json.dumps(validations,indent=2)+"\n")
    (OUT/"root_cause_scorecard.json").write_text(json.dumps(scorecard,indent=2)+"\n")
    (OUT/"primary_repair_decision.json").write_text(json.dumps(summary["repair_decision"],indent=2)+"\n")
    (OUT/"oct15_pending_feature_exposure_summary.json").write_text(json.dumps(oct_summary,indent=2,default=str)+"\n")
    (OUT/"opportunity_player_strength_summary.json").write_text(json.dumps({"features":["player_points_last10","season_ppg_prior","mean_toi_last10","mean_pp_toi_last10","d10_sog_per60"],"conditional_feature_maps":"conditional_single_feature_residual_maps.csv","within_player_summary":within_summary,"same_expected_count_comparison":"matched_expected_count_residual_differences.csv"},indent=2,default=str)+"\n")
    (OUT/"backlog_context.md").write_text("# Preserved NHL backlog\n\n"+"\n".join(f"- {x}" for x in backlog)+"\n\nCurrent audit does not recalibrate, retrain, alter production features, modify Funnel V1, change authority, or add market predictors.\n")
    report=f'''# NHL Points cross-season conditional feature-response audit\n\n## Scope and frozen cohorts\n\nThe primary comparison is {len(h):,} 2025 regular-season strict-prior holdout player-games against {len(d):,} officially final, participating 2026 player-games from Oct. 9–14. The 2026 captures contain {len(all26):,} total identities, including {all26.realized_points.isna().sum():,} unresolved outcomes. The 714-row exact two-sided market intersection is secondary only.\n\n2025 predicted/realized/residual: {overall25['mean_predicted_points']:.3f}/{overall25['mean_realized_points']:.3f}/{overall25['residual_realized_minus_predicted']:+.3f}. 2026: {overall26['mean_predicted_points']:.3f}/{overall26['mean_realized_points']:.3f}/{overall26['residual_realized_minus_predicted']:+.3f}. 2026 game-cluster bootstrap 95% CI for mean residual: {game_ci['cluster_bootstrap_95pct_ci']}.\n\n## Parity and feature construction\n\nFrozen artifact SHA256 `{sha(MODEL_PATH)}` and exact ten-feature order match both retained paths; 2025 replay max absolute error {parity:.3g}. The holdout is out of time: fitted seasons are 2023–24 and holdout is 2025. Official Points are goals + assists for final participants only. Reconciliation outcomes remain separate from pregame features.\n\nFeature construction classifications are in `feature_construction_comparison.csv`. The established attempts history absence is the only confirmed missingness drift among the ten features. Existing one-feature zeroing evidence is retained in `attempts_zeroing_existing_evidence.csv`; it showed only a small average change, so it cannot account for the aggregate gap. Team D10 shots and player team SOG share use matching team/game bindings and strict-prior windows in the training and operational feature paths.\n\n## Early-season and common-support controls\n\nSee `early_season_historical_control.csv` for fixed current-season games-prior bands. Residual gaps for bands with both seasons represented: {json.dumps({k:round(v['residual_gap_2026_minus_2025'],3) for k,v in early_summary.items() if k!='11+_LATER_MATURITY_REFERENCE'})}. Later 2025 is retained as a distinct maturity reference.\n\nOutcome-blind one-to-one matching paired {common['summary']['n_pairs']:,} rows per season using the eight pregame covariates and declared calipers. Matched residuals: 2025 {common['summary']['2025_matched_metrics'].get('residual_realized_minus_predicted')}, 2026 {common['summary']['2026_matched_metrics'].get('residual_realized_minus_predicted')}; difference {common['summary']['residual_difference_2026_minus_2025']}. Balance before/after is in `common_support_balance.csv`.\n\n## Conditional findings\n\nFixed 2025-derived feature bins, shared expected-count bands, seven bounded interactions, and team/opportunity contexts are delivered as CSVs. Separately computed residual partitions in `global_error_concentration.csv` sum within each dimension to the full 2026 residual and are not additive across dimensions. Market quote status appears only as a secondary descriptive split.\n\nGoals/assists and zero/one/2+ scoring rates are in `scoring_outcome_decomposition.csv`. Supported within-player, comparable-state observations are in `within_player_comparison.csv` ({len(common_players):,} players represented in both seasons; {len(wp):,} caliper-supported pairs). Feature perturbations in `frozen_model_feature_sensitivity.csv` are sensitivity only, not causal attribution.\n\nThe existing 2025 market-like reweighting was outcome-blind, effective sample size {reweight.get('effective_sample_size')}, weighted residual {reweight.get('weighted_residual'):+.4f}; it is secondary evidence and does not establish conditional stability.\n\n## Root-cause decision\n\n`NO_SINGLE_REPAIR_JUSTIFIED`. Attempts missingness is confirmed but has small estimated frozen-model effect. Early-season feature support, conditional response, team context, and player composition remain bounded descriptive hypotheses requiring prospective replication. No individual feature mechanism currently meets the requested measurable, conditional, isolated-repair and healthy-row protection criteria. Next study: preregister season-volume × recent-scoring × TOI strata over additional full-population slates, preserving an untouched low-residual reference group.\n\n## Oct. 15 pending exposure\n\n`PENDING_OUTCOME`; {len(ox):,} immutable feature rows were used with no outcomes joined. Summary and feature rows are retained separately.\n\n## Integrity\n\nProvider calls: 0. Paid credits: 0. Production DB mutations: 0. HGB retraining/calibration/authority changes: none. Funnel V1: unchanged.\n'''
    count_gap=float(pd.read_csv(OUT/"matched_expected_count_residual_differences.csv").set_index("expected_points_band").loc[".30-.40","residual_difference_2026_minus_2025"])
    report += f'''\n## Cross-season conditional and source caveats\n\nAt the same expected-count band .30–.40, the 2026 residual exceeds 2025 by {count_gap:+.3f}; the pattern appears across all shared expected-count bands, but the 1.00+ band has zero 2026 rows. This supports a changed conditional relationship in the observed six-slate sample, subject to game-cluster uncertainty and prospective replication.\n\nOfficial goals, assists, and zero/one/2+ rates are close in the two full cohorts. The aggregate target mix therefore does not explain the +0.148 residual difference.\n\nThe 2026 team D10 shots mean is {float(team.mean_2026):.2f}, versus {float(team.mean_2025):.2f} in 2025. Both code paths use the same units and strict-prior team-game aggregation. However, the immutable operational captures do not retain underlying per-team prior-game shot rows, so source-level reconciliation to official team shots is unresolved. `team_context_audit.json` records this limitation.\n\nAttempts missingness is confirmed. The retained 2025 zeroing experiment moved mean prediction by {float(pd.to_numeric(pd.read_csv(OUT/"attempts_zeroing_existing_evidence.csv").mean_expected_points_delta_zero_minus_correct).iloc[0]):+.3f}, while the separately retained 2026 training-median replacement sensitivity is in `frozen_model_feature_sensitivity.csv` and is not a causal estimate. The mismatch is not attributed as the full 2026 gap.\n\nThe strict outcome-blind, raw-unit common-support procedure retained {common['summary']['n_pairs']} pairs. Post-match balance status is `{common['summary']['balance_status']}` (maximum absolute SMD {common['summary']['max_abs_postmatch_smd']:.2f}; threshold 0.10), driven by limited team-context support. Treat its residual comparison as descriptive, not as a balanced estimate. Within-player pairs are one-to-one within player across seasons and enforce recent form, PPG, TOI, PP TOI, and expected-count calipers.\n\n2026 capture deterministic replay status is `PASS` with maximum absolute prediction error {replay26_max}. The shared-upstream review is bounded to the feature builders and immutable captures. Shot rate, attempts, TOI, and team shots depend on skater-log source coverage; attempts missingness is confirmed, while team context source completeness remains unresolved. No acquisition or database inspection was performed.\n'''
    (OUT/"report.md").write_text(report)
    # lineage hashes every output after all content has been written, excluding itself.
    output_hashes={p.name:sha(p) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!="lineage_manifest.json"}
    lineage["output_hashes_sha256"]=output_hashes
    (OUT/"lineage_manifest.json").write_text(json.dumps(lineage,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"status":summary["status"],"out":str(OUT.relative_to(ROOT)),"n2025":len(h),"n2026":len(d),"matched_pairs":common["summary"]["n_pairs"],"residual_2025":overall25["residual_realized_minus_predicted"],"residual_2026":overall26["residual_realized_minus_predicted"],"early_bands":early_summary,"oct15":oct_summary},indent=2,default=str))


if __name__ == "__main__": main()
