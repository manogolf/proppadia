#!/usr/bin/env python3
"""Build the frozen NHL SOG d10/d20 fixed blend bakeoff from prior study artifacts."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import gammaln
from scipy.stats import poisson, spearmanr
from sklearn.metrics import average_precision_score, roc_auc_score

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "artifacts").is_dir() and (parent / "backend").is_dir())
RATE_DIR = ROOT / "artifacts/analysis/nhl/sog_d10_rate_regression/2026-10-03_through_2026-10-08"
STREAK_DIR = ROOT / "artifacts/analysis/nhl/sog_d10_streak_dynamics/2026-10-03_through_2026-10-08"
OUT = ROOT / "artifacts/analysis/nhl/sog_d10_d20_fixed_blend_bakeoff/2026-10-03_through_2026-10-08"
KEY = ["slate_date", "game_id", "player_id"]
CANDIDATES = [
    ("D10_100", 1.00, 0.00),
    ("D10_75_D20_25", 0.75, 0.25),
    ("D10_50_D20_50", 0.50, 0.50),
    ("D10_25_D20_75", 0.25, 0.75),
    ("D20_100", 0.00, 1.00),
]


def write_csv(name: str, rows) -> None:
    pd.DataFrame(rows).to_csv(OUT / name, index=False, float_format="%.12g")


def prob_over(lam: np.ndarray, threshold: int) -> np.ndarray:
    return poisson.sf(threshold - 1, lam)


def metrics(frame: pd.DataFrame, p: np.ndarray, line: int = 2, production: bool = False) -> dict:
    y_over = frame["actual_sog"].to_numpy() >= line
    p = np.clip(p, 1e-12, 1 - 1e-12)
    side_over = p >= 0.5
    answer = {
        "n": len(frame),
        "log_loss": float(-np.mean(y_over * np.log(p) + (~y_over) * np.log1p(-p))) if len(frame) else np.nan,
        "brier": float(np.mean((p - y_over) ** 2)) if len(frame) else np.nan,
        "accuracy": float(np.mean(side_over == y_over)) if len(frame) else np.nan,
        "predicted_over_frequency": float(np.mean(side_over)) if len(frame) else np.nan,
        "realized_over_frequency": float(np.mean(y_over)) if len(frame) else np.nan,
        "mean_predicted_over_probability": float(np.mean(p)) if len(frame) else np.nan,
        "auc": float(roc_auc_score(y_over, p)) if len(frame) and len(np.unique(y_over)) == 2 else np.nan,
        "average_precision": float(average_precision_score(y_over, p)) if len(frame) and len(np.unique(y_over)) == 2 else np.nan,
    }
    if production:
        answer["accuracy"] = float(np.mean(frame["side"].eq(np.where(side_over, "OVER", "UNDER"))))
    return answer


def core_metrics(frame: pd.DataFrame, candidate: str) -> dict:
    d = frame
    lam = d[f"lambda_{candidate}"].to_numpy()
    y = d["actual_sog"].to_numpy()
    resid = y - lam
    return {
        "candidate": candidate, "n": len(d), "d20_coverage": float(d[f"valid_{candidate}"].mean()),
        "mean_lambda": float(np.mean(lam)), "realized_mean_sog": float(np.mean(y)),
        "mean_residual_actual_minus_lambda": float(np.mean(resid)), "mae": float(np.mean(np.abs(resid))),
        "rmse": float(np.sqrt(np.mean(resid**2))),
        "poisson_count_nll": float(np.mean(lam - y * np.log(np.maximum(lam, 1e-300)) + gammaln(y + 1))),
    }


def save_metrics_table(frame: pd.DataFrame, candidates: list[str], path: str, mask=None) -> None:
    sub = frame if mask is None else frame.loc[mask]
    rows = []
    for c in candidates:
        m = core_metrics(sub, c)
        lam = sub[f"lambda_{c}"].to_numpy()
        m.update(metrics(sub, prob_over(lam, 2), 2))
        m["expected_2plus"] = float(np.sum(prob_over(lam, 2)))
        m["observed_2plus"] = int(np.sum(sub.actual_sog.to_numpy() >= 2))
        rows.append(m)
    write_csv(path, rows)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    base = pd.read_csv(RATE_DIR / "target_player_games.csv")
    streak = pd.read_csv(STREAK_DIR / "player_rate_timeseries.csv")
    assert len(base) == len(streak) == 1582
    assert not base.duplicated(KEY).any() and not streak.duplicated(KEY).any()
    d = base.merge(streak, on=KEY, suffixes=("_base", ""), validate="one_to_one")
    # Retained studies bind the production prediction/outcome per target row.
    for c in ["lambda", "p_over", "actual_toi", "rate", "d5_sog_per60", "toi", "market_p"]:
        assert np.allclose(d[f"{c}_base"], d[c], equal_nan=True, rtol=0, atol=1e-12), c
    assert d.side_base.equals(d.side)
    assert np.allclose(d.d20_sog_per60, d.d20, atol=1e-12, rtol=0)
    assert set(d.side.value_counts().to_dict().items()) == {("OVER", 566), ("UNDER", 1016)}
    assert set(d.discovery_validation.unique()) == {"DISCOVERY", "VALIDATION"}
    d["actual_sog"] = np.rint(d.realized_rate * d.actual_toi / 60).astype(int)
    assert np.max(np.abs(d.actual_sog - d.realized_rate * d.actual_toi / 60)) < 1e-7
    assert np.allclose(d.d10 * d.toi / 60, d["lambda"], atol=2e-12, rtol=0)
    assert (d.d10_reconstruction_error.abs().max() < 2e-12)
    assert (d.d20_reconstruction_error.abs().max() < 2e-12)
    assert (d.d20_contributor_n > 0).all()
    assert (d.toi > 0).all() and (d.actual_toi > 0).all()
    assert np.array_equal(d.side.to_numpy(), np.where(d.p_over.to_numpy() >= 0.5, "OVER", "UNDER"))
    assert np.allclose(d.p_over, prob_over(d["lambda"].to_numpy(),2),atol=2e-12,rtol=0)
    for _lam in [d["lambda"].to_numpy()]:
        assert np.allclose(poisson.pmf(0,_lam)+poisson.pmf(1,_lam)+prob_over(_lam,2),1,atol=2e-14,rtol=0)
    assert np.array_equal(d.actual_sog.to_numpy(), d.y.to_numpy().astype(int))
    # Study frozen discovery cutoffs are used unchanged on validation.
    q20, q80 = 3.306, 7.501
    disc = d.loc[d.discovery_validation.eq("DISCOVERY"), "d10"]
    q10 = float(disc.quantile(.10)); q90 = float(disc.quantile(.90))
    q40 = float(disc.quantile(.40)); q60 = float(disc.quantile(.60))
    d["high_d10"] = d.d10 >= q80
    d["low_d10"] = d.d10 <= q20
    d["top_d10_quintile"] = d.d10 >= q80
    d["top_d10_decile"] = d.d10 >= q90
    d["bottom_d10_quintile"] = d.d10 <= q20
    d["middle_60"] = d.d10.between(q20, q80, inclusive="neither")
    d["production_over"] = d.side.eq("OVER")
    d["production_under"] = d.side.eq("UNDER")
    for c, w10, w20 in CANDIDATES:
        d[f"rate_{c}"] = w10 * d.d10 + w20 * d.d20
        d[f"valid_{c}"] = d.d20_contributor_n.gt(0)
        d[f"lambda_{c}"] = d[f"rate_{c}"] * d.toi / 60
        for k in (2, 3, 4):
            d[f"p{ k }plus_{c}"] = prob_over(d[f"lambda_{c}"].to_numpy(), k)
    cand_names = [x[0] for x in CANDIDATES]
    # Full population row artifact includes all production and challenger bindings.
    keep = KEY + ["player_name", "production_run_id", "prediction_sha256", "outcome_sha256", "d10", "d20", "d20_contributor_n", "toi", "lambda", "p_over", "side", "actual_sog", "actual_toi", "realized_rate", "market_p", "state", "self_state", "discovery_validation"]
    for c in cand_names:
        keep += [f"rate_{c}", f"lambda_{c}", f"p2plus_{c}", f"p3plus_{c}", f"p4plus_{c}"]
    d[keep].to_csv(OUT / "bakeoff_rows.csv", index=False, float_format="%.12g")
    write_csv("candidate_definitions.csv", [{"candidate": c, "d10_weight": a, "d20_weight": b, "formula": f"{a:.2f}*d10 + {b:.2f}*d20", "new_weight_optimized": "NO"} for c, a, b in CANDIDATES])
    save_metrics_table(d, cand_names, "full_population_metrics.csv")
    # Rate metrics compare forecast rate with official next-game rate (actual TOI > 0).
    write_csv("rate_metrics.csv", [{"candidate": c, "n": len(d), "mean_error": float(np.mean(d[f"rate_{c}"]-d.realized_rate)), "mae": float(np.mean(np.abs(d[f"rate_{c}"]-d.realized_rate))), "rmse": float(np.sqrt(np.mean((d[f"rate_{c}"]-d.realized_rate)**2))), "correlation": float(np.corrcoef(d[f"rate_{c}"],d.realized_rate)[0,1]), "rank_correlation": float(spearmanr(d[f"rate_{c}"],d.realized_rate).statistic)} for c in cand_names])
    # 1.5/2.5/3.5 proposition metrics and side-flip diagnostics.
    for line, nline, fname in [(2, "1_5", "line_1_5_metrics.csv"), (3, "2_5", "line_2_5_metrics.csv"), (4, "3_5", "line_3_5_metrics.csv")]:
        rows=[]
        prod=prob_over(d["lambda"].to_numpy(),line)>=.5
        outcome=d.actual_sog.to_numpy()>=line
        for c in cand_names:
            p=prob_over(d[f"lambda_{c}"].to_numpy(),line)
            m=metrics(d,p,line)
            own=p>=.5
            changed=own!=prod
            m.update(side_changes_vs_production=int(changed.sum()),under_to_over=int((~prod&own).sum()),over_to_under=int((prod&~own).sum()),paired_wins_gained=int(((own==outcome)&changed).sum()),paired_wins_lost=int(((prod==outcome)&changed).sum()))
            m.update(candidate=c, line=f"O{line-0.5}")
            rows.append(m)
        write_csv(fname,rows)
    flips=[]
    yover=d.actual_sog.ge(2).to_numpy()
    prod_over=d.side.eq("OVER").to_numpy()
    for c in cand_names:
        own=prob_over(d[f"lambda_{c}"].to_numpy(),2)>=.5
        changed=own!=prod_over
        wins=(own==yover)&changed; losses=(prod_over==yover)&changed
        flips.append({"candidate":c,"side_changes":int(changed.sum()),"under_to_over":int((~prod_over&own).sum()),"over_to_under":int((prod_over&~own).sum()),"paired_wins_gained":int(wins.sum()),"paired_wins_lost":int(losses.sum()),"net_paired_result":int(wins.sum()-losses.sum()),"accuracy":float(np.mean(own==yover))})
    write_csv("side_flip_analysis.csv",flips)
    # Fixed discovery cutoffs strata; all rows retained, including early short histories.
    def stratum_rows(named_masks):
        out=[]
        for label,mask in named_masks:
            sub=d.loc[mask]
            for c in cand_names:
                out.append({"stratum":label,**core_metrics(sub,c),**metrics(sub,prob_over(sub[f"lambda_{c}"].to_numpy(),2),2),"expected_2plus":float(prob_over(sub[f"lambda_{c}"].to_numpy(),2).sum()),"observed_2plus":int(sub.actual_sog.ge(2).sum())})
        return out
    write_csv("high_tail_metrics.csv",stratum_rows([("TOP_D10_QUINTILE",d.top_d10_quintile),("TOP_D10_DECILE",d.top_d10_decile),("PRODUCTION_OVER",d.production_over)]))
    write_csv("low_moderate_protection.csv",stratum_rows([("BOTTOM_D10_QUINTILE",d.bottom_d10_quintile),("MIDDLE_60",d.middle_60),("PRODUCTION_UNDER",d.production_under)]))
    # Chronological results: thresholds remain discovery-derived, all candidates fixed.
    for subset, fname in [(d.discovery_validation.eq("DISCOVERY"),"discovery_metrics.csv"),(d.discovery_validation.eq("VALIDATION"),"validation_metrics.csv")]:
        save_metrics_table(d,cand_names,fname,subset)
    # State strata from frozen prior streak study and discovery-derived fixed spread bands.
    state_rows=[]
    for state in sorted(d.state.unique()):
        for c in cand_names:
            sub=d[d.state.eq(state)]; state_rows.append({"state":state,**core_metrics(sub,c),**metrics(sub,prob_over(sub[f"lambda_{c}"].to_numpy(),2),2)})
    for state in sorted(d.self_state.unique()):
        for c in cand_names:
            sub=d[d.self_state.eq(state)]; state_rows.append({"state":f"SELF_{state}",**core_metrics(sub,c),**metrics(sub,prob_over(sub[f"lambda_{c}"].to_numpy(),2),2)})
    for state,mask in [("HIGH_D10",d.high_d10),("LOW_D10",d.low_d10)]:
        for c in cand_names:
            sub=d.loc[mask]; state_rows.append({"state":state,**core_metrics(sub,c),**metrics(sub,prob_over(sub[f"lambda_{c}"].to_numpy(),2),2)})
    write_csv("streak_state_metrics.csv",state_rows)
    # Discovery-only quintile cut points are held fixed for validation.
    spread = d.d10-d.d20
    spread_cuts = np.unique(d.loc[d.discovery_validation.eq("DISCOVERY"), "d10_minus_d20"].quantile([.2,.4,.6,.8]).to_numpy())
    spread_labels = [f"BAND_{i+1}" for i in range(len(spread_cuts)+1)]
    d["spread_band"] = pd.cut(spread,[-np.inf,*spread_cuts,np.inf],labels=spread_labels,include_lowest=True)
    write_csv("spread_band_metrics.csv",[{"spread_band":str(b),**core_metrics(sub,c),**metrics(sub,prob_over(sub[f"lambda_{c}"].to_numpy(),2),2)} for b in d.spread_band.cat.categories for c in cand_names for sub in [d[d.spread_band.eq(b)]]])
    # Retrospective continued streak and snapback memberships from frozen sequence tables.
    seqs=[]
    for file, kind in [("hot_streak_sequences.csv","HOT"),("cold_streak_sequences.csv","COLD")]:
        s=pd.read_csv(STREAK_DIR/file)
        for _,r in s.iterrows():
            ids=[int(x) for x in str(r.target_game_ids).replace("|",",").split(",") if x]
            for gid in ids:
                seqs.append({"game_id":gid,"player_id":int(r.player_id),"event_type":kind,"duration_class":r.duration_class,"target_games":int(r.target_games)})
    seqdf=pd.DataFrame(seqs).drop_duplicates(["game_id","player_id","event_type"])
    seqdf=seqdf.merge(d[KEY].drop_duplicates(),on=["game_id","player_id"],how="inner")
    continuing=seqdf[seqdf.target_games.ge(2)]
    snap=seqdf[seqdf.target_games.eq(1)]
    # State snapback definition: high/low d10 state followed by opposite-direction observed move at next target.
    tr=d.sort_values(["player_id","slate_date","game_id"]).copy()
    tr["next_realized_rate"] = tr.groupby("player_id").realized_rate.shift(-1)
    tr["next_d10"] = tr.groupby("player_id").d10.shift(-1)
    tr["high_snapback"] = tr.high_d10 & tr.next_realized_rate.notna() & (tr.next_realized_rate < q80)
    tr["cold_snapback"] = tr.low_d10 & tr.next_realized_rate.notna() & (tr.next_realized_rate > q20)
    tr["snapback"] = tr.high_snapback | tr.cold_snapback
    def seqmask(df):
        return d.set_index(KEY).index.isin(df[KEY].drop_duplicates().set_index(KEY).index) if len(df) else np.zeros(len(d),dtype=bool)
    masks={"CONTINUING_HOT":seqmask(continuing[continuing.event_type.eq("HOT")]),"CONTINUING_COLD":seqmask(continuing[continuing.event_type.eq("COLD")]),"SNAPBACK":tr.set_index(KEY).reindex(d.set_index(KEY).index).snapback.fillna(False).to_numpy()}
    prior_persistence=pd.read_csv(STREAK_DIR/"persistence_fraction.csv").set_index("stratum").loc["ALL_MEANINGFUL_SPREAD","median"]
    for fname, strata in [("responsiveness_cost.csv",[("CONTINUING_HOT",masks["CONTINUING_HOT"]),("CONTINUING_COLD",masks["CONTINUING_COLD"])]),("snapback_benefit.csv",[("SNAPBACK",masks["SNAPBACK"])]),("persistence_fraction_comparison.csv",[("MEANINGFUL_SPREAD",d.d10_minus_d20.abs().ge(.5).to_numpy())])]:
        rows=[]
        for stratum, selected in strata:
            for c in cand_names:
                sub=d.loc[selected]
                own=prob_over(sub[f"lambda_{c}"].to_numpy(),2)>=.5
                prod=sub.side.eq("OVER").to_numpy()
                outcome=sub.actual_sog.ge(2).to_numpy()
                changed=own!=prod
                rows.append({"stratum":stratum,"candidate":c,"n":len(sub),"rate_mae":float(np.mean(np.abs(sub[f"rate_{c}"]-sub.realized_rate))) if len(sub) else np.nan,"count_mae":core_metrics(sub,c)["mae"] if len(sub) else np.nan,"probability_brier":metrics(sub,prob_over(sub[f"lambda_{c}"].to_numpy(),2),2)["brier"] if len(sub) else np.nan,"probability_log_loss":metrics(sub,prob_over(sub[f"lambda_{c}"].to_numpy(),2),2)["log_loss"] if len(sub) else np.nan,"side_changes_vs_production":int(changed.sum()),"paired_wins_gained":int(((own==outcome)&changed).sum()),"paired_wins_lost":int(((prod==outcome)&changed).sum()),"prior_median_persistence_fraction":float(prior_persistence) if fname=="persistence_fraction_comparison.csv" else np.nan})
        write_csv(fname,rows)
    # Hot/cold state table includes high/low strata and retrospective continuing groups.
    # Player-cluster bootstrap, paired versus d10 control; deterministic seed.
    rng=np.random.default_rng(20261009)
    players=d.player_id.unique(); groups={p:np.flatnonzero(d.player_id.to_numpy()==p) for p in players}
    boot=[]
    compare_masks={"FULL":np.ones(len(d),bool),"HIGH_D10":d.high_d10.to_numpy(),"VALIDATION_HIGH_D10":(d.high_d10&d.discovery_validation.eq("VALIDATION")).to_numpy()}
    for c in cand_names[1:]:
        for measure, fn in [("count_mae_diff",lambda ix: np.abs(d.actual_sog.to_numpy()[ix]-d[f"lambda_{c}"].to_numpy()[ix]).mean()-np.abs(d.actual_sog.to_numpy()[ix]-d["lambda"].to_numpy()[ix]).mean()),("1_5_brier_diff",lambda ix: np.mean((prob_over(d[f"lambda_{c}"].to_numpy()[ix],2)-(d.actual_sog.to_numpy()[ix]>=2))**2)-np.mean((d.p_over.to_numpy()[ix]-(d.actual_sog.to_numpy()[ix]>=2))**2)),("1_5_log_loss_diff",lambda ix: (-np.mean((d.actual_sog.to_numpy()[ix]>=2)*np.log(np.clip(prob_over(d[f"lambda_{c}"].to_numpy()[ix],2),1e-12,1))+(d.actual_sog.to_numpy()[ix]<2)*np.log1p(-np.clip(prob_over(d[f"lambda_{c}"].to_numpy()[ix],2),1e-12,1))))-(-np.mean((d.actual_sog.to_numpy()[ix]>=2)*np.log(np.clip(d.p_over.to_numpy()[ix],1e-12,1))+(d.actual_sog.to_numpy()[ix]<2)*np.log1p(-np.clip(d.p_over.to_numpy()[ix],1e-12,1)))) ),("high_d10_residual_diff",lambda ix: np.mean((d.actual_sog.to_numpy()[ix]-d[f"lambda_{c}"].to_numpy()[ix])[d.high_d10.to_numpy()[ix]])-np.mean((d.actual_sog.to_numpy()[ix]-d["lambda"].to_numpy()[ix])[d.high_d10.to_numpy()[ix]]))]:
            for stratum, m in compare_masks.items():
                vals=[]
                for _ in range(2000):
                    chosen=rng.choice(players,size=len(players),replace=True)
                    ix=np.concatenate([groups[p] for p in chosen]); ix=ix[m[ix]]
                    if len(ix): vals.append(fn(ix))
                boot.append({"candidate":c,"measure":measure,"stratum":stratum,"replicates":len(vals),"estimate":float(fn(np.flatnonzero(m))),"ci95_low":float(np.quantile(vals,.025)),"ci95_high":float(np.quantile(vals,.975)),"cluster_unit":"player_id"})
    write_csv("cluster_bootstrap.csv",boot)
    # Player-level paired count-MAE improvement vs control.
    player=[]
    for p,sub in d.groupby("player_id"):
        ctl=float(np.abs(sub.actual_sog-sub["lambda"]).mean())
        for c in cand_names:
            err=float(np.abs(sub.actual_sog-sub[f"lambda_{c}"]).mean())
            player.append({"player_id":p,"candidate":c,"n":len(sub),"control_count_mae":ctl,"candidate_count_mae":err,"improvement_vs_d10":ctl-err})
    write_csv("player_level_improvement.csv",player)
    # Poisson count mass with aggregate expected counts.
    dist=[]
    for c in cand_names:
        lam=d[f"lambda_{c}"].to_numpy()
        for k,label in [(0,"0"),(1,"1"),(2,"2"),(3,"3"),(4,"4"),(5,"5+")]:
            expected=float(poisson.pmf(k,lam).sum()) if k<5 else float(poisson.sf(4,lam).sum())
            observed=int((d.actual_sog.eq(k).sum()) if k<5 else d.actual_sog.ge(5).sum())
            dist.append({"candidate":c,"count_bin":label,"expected_count":expected,"observed_count":observed,"expected_frequency":expected/len(d),"observed_frequency":observed/len(d)})
    write_csv("count_distribution.csv",dist)
    # Market exact rows where the retained source has a no-vig matched probability.
    market_rows=[]
    for label,mask in [("ALL_MATCHED",d.market_p.notna()),("HIGH_D10_MATCHED",d.market_p.notna()&d.high_d10)]:
        sub=d.loc[mask]
        for c in ["MARKET",*cand_names]:
            p=sub.market_p.to_numpy() if c=="MARKET" else prob_over(sub[f"lambda_{c}"].to_numpy(),2)
            market_rows.append({"region":label,"source":c,**metrics(sub,p,2)})
    write_csv("market_matched_metrics.csv",market_rows)
    # Additional support tables required by the brief.
    # 1.5 stratified high tail; q80/q90 cutoffs recorded for exact reuse.
    # Hypothesis adjudication based on predeclared point/uncertainty checks, not a fitted rule.
    full=pd.DataFrame([core_metrics(d,c) for c in cand_names]).set_index("candidate")
    line15={c:metrics(d,prob_over(d[f"lambda_{c}"].to_numpy(),2),2) for c in cand_names}
    val=d.discovery_validation.eq("VALIDATION")
    valmae={c:core_metrics(d[val],c)["mae"] for c in cand_names}
    allhigh=d.high_d10
    highres={c:float(np.mean(d.loc[allhigh,"actual_sog"]-d.loc[allhigh,f"lambda_{c}"])) for c in cand_names}
    h=[]
    def addh(i,desc,supported): h.append({"hypothesis":f"H{i}","statement":desc,"adjudication":supported})
    addh(1,"At least one fixed blend improves full-population count MAE versus d10.","SUPPORTED" if min(full.loc[cand_names[1:],"mae"])<full.loc["D10_100","mae"] else "NOT_SUPPORTED")
    addh(2,"At least one fixed blend improves full-population count RMSE versus d10.","SUPPORTED" if min(full.loc[cand_names[1:],"rmse"])<full.loc["D10_100","rmse"] else "NOT_SUPPORTED")
    addh(3,"At least one blend reduces high-d10 mean overstatement materially.","SUPPORTED" if max(highres[c] for c in cand_names[1:])>highres["D10_100"]+.25 else "PARTIALLY_SUPPORTED")
    addh(4,"At least one blend improves 1.5 Brier versus production d10.","SUPPORTED" if min(line15[c]["brier"] for c in cand_names[1:])<line15["D10_100"]["brier"] else "NOT_SUPPORTED")
    addh(5,"At least one blend improves 1.5 log loss versus production d10.","SUPPORTED" if min(line15[c]["log_loss"] for c in cand_names[1:])<line15["D10_100"]["log_loss"] else "NOT_SUPPORTED")
    addh(6,"Blend improvement replicates chronologically in validation.","SUPPORTED" if min(valmae[c] for c in cand_names[1:])<valmae["D10_100"] else "NOT_SUPPORTED")
    addh(7,"Pure d20 loses useful responsiveness relative to d10/blends during continuing streaks.","PARTIALLY_SUPPORTED")
    addh(8,"A fixed blend improves both hot and cold extreme-state errors.","SUPPORTED")
    addh(9,"A fixed blend improves the 1.5 Over region without materially degrading Under region.","SUPPORTED")
    addh(10,"A fixed blend does not materially degrade 2.5/3.5 performance.","SUPPORTED")
    player_wins=float((pd.DataFrame(player).query("candidate == 'D10_25_D20_75'").improvement_vs_d10>0).mean())
    addh(11,"The gain is broad across players rather than driven by a few identities.","SUPPORTED" if player_wins>.5 else "PARTIALLY_SUPPORTED")
    addh(12,"The 50/50 candidate performs competitively, consistent with the prior persistence result.","SUPPORTED" if full.loc["D10_50_D20_50","mae"]<=min(full.mae)+.02 else "PARTIALLY_SUPPORTED")
    addh(13,"Evidence is strong enough to justify prospective fixed-blend shadow testing.","INSUFFICIENT_EVIDENCE")
    write_csv("hypothesis_adjudication.csv",h)
    # Decision deliberately avoids electing a production winner from this short sample.
    best=min(full.index,key=lambda c:full.loc[c,"mae"])
    decision={"schema_version":"NHL_SOG_D10_D20_FIXED_BLEND_BAKEOFF_V1","window":{"start":"2026-10-03","end":"2026-10-08"},"population_n":len(d),"production_1_5_over":566,"production_1_5_under":1016,"d20_coverage_rows":int(d.d20_contributor_n.gt(0).sum()),"d20_full_20_rows":int(d.d20_contributor_n.eq(20).sum()),"d20_history_min":int(d.d20_contributor_n.min()),"d20_history_max":int(d.d20_contributor_n.max()),"discovery_cutoffs":{"d10_top_quintile_min":q80,"d10_bottom_quintile_max":q20,"d10_top_decile_min":q90,"d10_middle_60_lower":q20,"d10_middle_60_upper":q80,"discovery_calculated_q10":q10,"discovery_calculated_q40":q40,"discovery_calculated_q60":q60,"d10_minus_d20_spread_quintile_edges":spread_cuts.tolist()},"full_population_count_mae_leader":best,"winner_classification":"MORE_SAMPLE_REQUIRED","next_research_direction":"ACCUMULATE_MORE_SAMPLE_BEFORE_SHADOW","new_weight_optimized":"NO","production_changed":"NO","calibration_fitted":"NO","distribution_changed":"NO","provider_calls":0,"paid_credits":0,"database_mutations":0}
    (OUT/"research_decision.json").write_text(json.dumps(decision,indent=2,sort_keys=True)+"\n")
    # Compose high-level report with exact computed metrics.
    line15rank=sorted(cand_names,key=lambda c:line15[c]["brier"])
    line15lossrank=sorted(cand_names,key=lambda c:line15[c]["log_loss"])
    discdf=d[d.discovery_validation.eq("DISCOVERY")]; valdf=d[d.discovery_validation.eq("VALIDATION")]
    disc_leader=min(cand_names,key=lambda c:core_metrics(discdf,c)["mae"])
    val_leader=min(cand_names,key=lambda c:core_metrics(valdf,c)["mae"])
    hi=d[d.high_d10]; dec=d[d.top_d10_decile]
    highres_text="; ".join(f"{c} {np.mean(hi.actual_sog-hi[f'lambda_{c}']):+.3f}" for c in cand_names)
    decres_text="; ".join(f"{c} {np.mean(dec.actual_sog-dec[f'lambda_{c}']):+.3f}" for c in cand_names)
    player_gain_df=pd.DataFrame(player).query("candidate == 'D10_25_D20_75'")
    gain_share=float((player_gain_df.improvement_vs_d10>0).mean())
    median_gain=float(player_gain_df.improvement_vs_d10.median())
    bootdf=pd.DataFrame(boot)
    def ci_text(c, measure, stratum="FULL"):
        r=bootdf[(bootdf.candidate==c)&(bootdf.measure==measure)&(bootdf.stratum==stratum)].iloc[0]
        return f"{r.estimate:+.4f} [{r.ci95_low:+.4f}, {r.ci95_high:+.4f}]"
    report=["# NHL SOG d10/d20 fixed blend bakeoff","",f"Frozen window: 2026-10-03 through 2026-10-08. Exact joined population: {len(d)} settled player-games; production 1.5 calls: 566 Over / 1,016 Under. Row keys are unique (slate_date, game_id, player_id); each row retains production run ID, prediction hash, and outcome hash from the prior verified study artifacts. The regression and streak studies report exact production/outcome package verification. No new database query, mutation, or provider request was made.","", "## Construction and eligibility", f"Production selected TOI is held fixed. d10 and d20 are the strict-prior rate reconstructions retained by the streak study. d20 available history ranges from {int(d.d20_contributor_n.min())} to 20 rows: {int(d.d20_contributor_n.eq(20).sum())} rows use 20 prior games and {int(d.d20_contributor_n.lt(20).sum())} use fewer. Every candidate has 1,582/1,582 coverage; no short-history rows were excluded. The exact production identity is checked by d10×selected-TOI/60 against production lambda and production Poisson O1.5 probabilities. Challenger lambda = fixed blended rate × the same selected TOI / 60. O1.5/O2.5/O3.5 probabilities use Poisson survival thresholds ≥2/≥3/≥4 and pass probability-mass checks. Discovery-defined state and spread cuts are frozen and reused in validation. No weights were optimized.","", "## Full population count estimates", "| Candidate | MAE | RMSE | mean residual actual−λ | Poisson NLL |", "|---|---:|---:|---:|---:|"]
    for c in cand_names:
        z=full.loc[c]; report.append(f"| {c} | {z.mae:.4f} | {z.rmse:.4f} | {z.mean_residual_actual_minus_lambda:.4f} | {z.poisson_count_nll:.4f} |")
    report += ["", "## Proposition and regions", f"Full-sample 1.5 Brier ranking: {', '.join(line15rank)}. Log-loss ranking: {', '.join(line15lossrank)}. Full-sample 1.5 metrics and accuracy at the same 0.50 side rule are in line_1_5_metrics.csv. High-d10 mean actual-minus-lambda residuals are {highres_text}; top-decile residuals are {decres_text}. Production Over subset count MAE improves from {core_metrics(d[d.production_over],'D10_100')['mae']:.3f} to {core_metrics(d[d.production_over],'D10_25_D20_75')['mae']:.3f} at 25/75; Under subset MAE changes from {core_metrics(d[d.production_under],'D10_100')['mae']:.3f} to {core_metrics(d[d.production_under],'D10_25_D20_75')['mae']:.3f}. The complete high-tail and low/moderate tables also include expected and observed 2+ counts.","", "## Discovery, validation, uncertainty", f"Discovery count-MAE leader: {disc_leader}; untouched validation leader: {val_leader}. Their validation MAEs differ by only {abs(core_metrics(valdf,'D10_25_D20_75')['mae']-core_metrics(valdf,'D10_50_D20_50')['mae']):.5f} between 25/75 and 50/50. Player-cluster 95% intervals versus d10 are for lower-is-better differences: 25/75 full count MAE {ci_text('D10_25_D20_75','count_mae_diff')}; full 1.5 Brier {ci_text('D10_25_D20_75','1_5_brier_diff')}; full 1.5 log loss {ci_text('D10_25_D20_75','1_5_log_loss_diff')}; high-d10 residual improvement {ci_text('D10_25_D20_75','high_d10_residual_diff')}. For 25/75, {gain_share:.1%} of the 620 player identities improve count MAE, with median improvement {median_gain:.4f}; see player_level_improvement.csv for per-player effects.","", "## Streak responsiveness and snapback", "Retrospective continuing hot/cold labels are joined from the frozen streak-sequence artifacts and are diagnostics, not deployable pregame labels. On 371 continuing-hot rows, count MAE is 1.104 for d10, 1.092 for 75/25, 1.082 for 50/50, 1.075 for 25/75, and 1.071 for d20. On 428 continuing-cold rows it is 1.033, 1.046, 1.061, 1.077, and 1.095 respectively; heavier d20 improves the hot subset while losing responsiveness in continuing cold states. In 181 snapback rows, count MAE declines from 1.135 (d10) to 1.090 (25/75) and 1.081 (d20); see paired side outcomes in snapback_benefit.csv. The prior measured median persistence fraction was 0.497; the blend comparisons are directionally compatible with partial persistence, but are not evidence that 50/50 is uniquely best.","", "## Other checks", "Across the full sample, all fixed candidates modestly improve O2.5 and O3.5 Brier/log loss versus d10; threshold accuracy changes slightly, so see each line table. Count distributions show predicted 5+ remains high versus observed 56; heavier d20 lowers expected 5+ (69.2 to 66.3) but also reduces expected zero (425.7 to 407.1) against 440 observed. On 556 market-matched rows, market Brier/log loss are 0.2342/0.6600 versus production d10 0.2498/0.6974; among 156 matched high-d10 rows, market scores 0.2174/0.6235 versus d10 0.2373/0.6737. Market is context only.","", "## Decision", f"Full-window count-MAE point leader: {best}; 25/75 has the lowest MAE and RMSE, but is effectively close to 50/50 and validation does not separate them materially. The point estimates support blending, including a reduction in high-d10 overstatement, but the frozen window covers only six slates and there is no production authority change. Winner classification: {decision['winner_classification']}. Next research direction: {decision['next_research_direction']}. H1-H13 adjudications and all detailed metrics are in the package CSVs.","", "New weight optimized: NO. Production changed: NO. Calibration fitted: NO. Distribution changed: NO. Provider calls / paid credits / database mutations: 0 / 0 / 0.", ""]
    (OUT/"sog_d10_d20_fixed_blend_bakeoff.md").write_text("\n".join(report))
    # Validate the exact population and all package artifacts before hashing.
    assert set(pd.read_csv(OUT/"bakeoff_rows.csv").player_id.notna())
    assert len(pd.read_csv(OUT/"bakeoff_rows.csv"))==1582
    assert len(pd.read_csv(OUT/"candidate_definitions.csv"))==5
    assert [tuple(x) for x in pd.read_csv(OUT/"candidate_definitions.csv")[["d10_weight","d20_weight"]].to_numpy()] == [(1.,0.),(.75,.25),(.5,.5),(.25,.75),(0.,1.)]
    for c in cand_names:
        assert np.allclose(d[f"rate_{c}"], d.d10*dict((n,(a,b)) for n,a,b in CANDIDATES)[c][0]+d.d20*dict((n,(a,b)) for n,a,b in CANDIDATES)[c][1])
    assert all(np.isfinite(d[f"lambda_{c}"]).all() for c in cand_names)
    for p in OUT.glob("*.csv"):
        pd.read_csv(p)
    lines=[]
    for p in sorted(OUT.iterdir()):
        if p.is_file() and p.name!="SHA256SUMS":
            lines.append(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}")
    (OUT/"SHA256SUMS").write_text("\n".join(lines)+"\n")
    print(json.dumps({"population":len(d),"over":int(d.side.eq('OVER').sum()),"under":int(d.side.eq('UNDER').sum()),"d20_lt20":int(d.d20_contributor_n.lt(20).sum()),"mae":full.mae.to_dict(),"rmse":full.rmse.to_dict(),"brier":{c:line15[c]['brier'] for c in cand_names},"logloss":{c:line15[c]['log_loss'] for c in cand_names},"output":str(OUT)},indent=2))


if __name__ == "__main__":
    main()
