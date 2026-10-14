#!/usr/bin/env python3
"""Offline characterization of NHL SOG production rate inputs."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14"
OUT = ROOT / "artifacts/analysis/nhl/sog_shot_rate_contribution/2026-10-14"
RATE_FIELDS = ("d10_sog_per60", "d20_sog_per60", "d5_sog_per60")
LINES = {"1.5": 2, "2.5": 3, "3.5": 4}
TOL = 1e-9


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def num(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce") if col in df else pd.Series(np.nan, index=df.index, dtype=float)


def select_rate(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    values = [num(df, c).where(np.isfinite(num(df, c))) for c in RATE_FIELDS]
    rate = pd.Series(np.nan, index=df.index, dtype=float)
    source = pd.Series("UNAVAILABLE", index=df.index, dtype=object)
    for name, value in zip(RATE_FIELDS, values):
        use = rate.isna() & value.notna()
        rate.loc[use], source.loc[use] = value.loc[use], name
    return rate, source


def poisson_tail(lam: pd.Series, threshold: int) -> pd.Series:
    return lam.map(lambda x: 1 - sum(math.exp(-x) * x**k / math.factorial(k) for k in range(threshold))
                   if pd.notna(x) and np.isfinite(x) and x >= 0 else np.nan)


def auc_score(p: pd.Series, y: pd.Series) -> float:
    z = pd.DataFrame({"p": p, "y": y}).dropna()
    if z.y.nunique() != 2:
        return np.nan
    ranks = z.p.rank(method="average")
    n1, n0 = int(z.y.sum()), int((1-z.y).sum())
    return float((ranks[z.y.eq(1)].sum() - n1*(n1+1)/2) / (n1*n0))


def metric_row(df: pd.DataFrame, label: str) -> dict:
    a, lam = num(df, "actual_sog"), num(df, "production_lambda")
    valid = a.notna() & lam.notna()
    a, lam = a[valid], lam[valid]
    residual = a - lam
    out = {"group": label, "n": int(len(a)), "mean_lambda": lam.mean(), "realized_sog": a.mean(),
           "mean_residual": residual.mean(), "mae": residual.abs().mean(), "rmse": np.sqrt((residual**2).mean())}
    for line, threshold in LINES.items():
        p = poisson_tail(lam, threshold).clip(1e-15, 1 - 1e-15)
        y = (a >= threshold).astype(float)
        out[f"brier_{line}"] = ((p-y)**2).mean()
        out[f"log_loss_{line}"] = (-(y*np.log(p)+(1-y)*np.log(1-p))).mean()
        out[f"calibration_bias_{line}"] = p.mean()-y.mean()
        out[f"auc_{line}"] = auc_score(p, y)
    return out


def prepare_hist() -> pd.DataFrame:
    d = pd.read_csv(BASE / "historical_characterization_rows.csv").drop_duplicates(["game_id", "player_id"]).copy()
    d["actual_sog"] = num(d, "official_sog").fillna(num(d, "actual_sog"))
    d["production_lambda"] = num(d, "production_lambda").fillna(num(d, "regenerated_expected_sog"))
    rate, source = select_rate(d)
    d["replayed_rate"], d["replayed_rate_source"] = rate, source
    d["selected_toi_minutes"] = num(d, "selected_toi_minutes")
    d["replayed_lambda"] = rate * d.selected_toi_minutes / 60
    d["replay_abs_error"] = (d.replayed_lambda-num(d, "production_lambda")).abs()
    # Retained historical snapshots lack a certified prior player-game count.
    # num_event_last10 is not an appearance count and is not substituted.
    d["prior_games_d10"] = np.nan
    return d


def prepare_prospective() -> tuple[pd.DataFrame, pd.DataFrame]:
    paired = pd.read_csv(BASE / "prospective_paired_rows.csv").drop_duplicates(["game_id", "player_id"]).copy()
    paired["actual_sog"] = num(paired, "actual_sog").fillna(num(paired, "official_sog"))
    paired["production_lambda"] = num(paired, "production_lambda").fillna(num(paired, "lambda_production"))
    features = pd.read_csv(BASE / "prospective_feature_bound_rows.csv").drop_duplicates(["game_id", "player_id"]).copy()
    preferred = set((*RATE_FIELDS, "selected_rate", "selected_toi_minutes", "selected_toi_source", "d10_prior_game_count", "d20_prior_game_count", "attempts_d10_per60", "role_pp_share", "team_d10_sf_per_game", "opp_d10_sf_allowed_per_game"))
    keep = ["game_id", "player_id", *[c for c in features if c not in {"game_id", "player_id"} and (c not in paired.columns or c in preferred)]]
    merged = paired.merge(features[keep], on=["game_id", "player_id"], how="left", suffixes=("", "_feature"), validate="one_to_one")
    for c in (*RATE_FIELDS, "selected_rate", "selected_toi_minutes", "selected_toi_source", "d10_prior_game_count", "d20_prior_game_count", "attempts_d10_per60", "role_pp_share", "team_d10_sf_per_game", "opp_d10_sf_allowed_per_game"):
        if c + "_feature" in merged:
            merged[c] = merged[c+"_feature"].combine_first(merged[c]) if c in merged else merged[c+"_feature"]
    rate, source = select_rate(merged)
    merged["replayed_rate"], merged["replayed_rate_source"] = rate, source
    merged["replayed_lambda"] = rate * num(merged, "selected_toi_minutes") / 60
    merged["replay_abs_error"] = (merged.replayed_lambda-num(merged,"production_lambda")).abs()
    merged["prior_games_d10"] = num(merged, "d10_prior_game_count")
    return merged, features


def depth_table(df: pd.DataFrame, population: str) -> pd.DataFrame:
    g = df[df.prior_games_d10.notna()].copy()
    g["depth_group"] = pd.cut(g.prior_games_d10, [-np.inf, 2, 4, 7, 9, 10, np.inf],
                               labels=["1-2", "3-4", "5-7", "8-9", "10", "11+"])
    rows = []
    if g.empty:
        return pd.DataFrame([{"population": population, "depth_group": "NOT_RETAINED", "n": 0,
                              "note": "Exact prior player-game count unavailable in retained rows."}])
    for name, part in g.groupby("depth_group", observed=False):
        m = metric_row(part, str(name)); m["population"] = population
        rates = num(part, "replayed_rate")
        m.update({"prior_game_min": part.prior_games_d10.min(), "prior_game_max": part.prior_games_d10.max(),
                  "rate_mean": rates.mean(), "rate_sd": rates.std(), "rate_q25": rates.quantile(.25),
                  "rate_q75": rates.quantile(.75), "rate_p05": rates.quantile(.05), "rate_p95": rates.quantile(.95)})
        rows.append(m)
    return pd.DataFrame(rows)


def relation_table(df: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for left,right in (("d5_sog_per60","d10_sog_per60"),("d10_sog_per60","d20_sog_per60"),("d5_sog_per60","d20_sog_per60")):
        x,y=num(df,left),num(df,right); z=pd.DataFrame({"x":x,"y":y}).dropna()
        delta=z.x-z.y
        rows.append({"pair":f"{left}_vs_{right}","n":len(z),"pearson_r":z.x.corr(z.y),"spearman_rho":z.x.corr(z.y,method="spearman"),
                     "mean_signed_difference":delta.mean(),"median_signed_difference":delta.median(),"mean_absolute_difference":delta.abs().mean(),
                     "difference_sd":delta.std(),"difference_p05":delta.quantile(.05),"difference_p95":delta.quantile(.95)})
    return pd.DataFrame(rows)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    hist=prepare_hist(); prosp, features=prepare_prospective()
    fbound=prosp[prosp.prior_games_d10.notna()].copy()
    hist.to_csv(OUT/"historical_replay_rows.csv",index=False); prosp.to_csv(OUT/"prospective_replay_rows.csv",index=False)
    usage=[]
    for label,df in (("historical",hist),("prospective_oct08_13",prosp),("prospective_feature_bound_oct10_13",fbound)):
        counts=df.replayed_rate_source.value_counts(dropna=False)
        for source,count in counts.items(): usage.append({"population":label,"source":source,"count":int(count),"pct":count/len(df) if len(df) else np.nan,"n":len(df)})
    usage=pd.DataFrame(usage); usage.to_csv(OUT/"rate_source_usage.csv",index=False)
    depths=pd.concat([depth_table(hist,"historical"),depth_table(fbound,"prospective_feature_bound")],ignore_index=True)
    depths.to_csv(OUT/"history_depth_metrics.csv",index=False); depths.to_csv(OUT/"sparse_history_metrics.csv",index=False)
    sparse=fbound.copy(); sparse["history_group"]=np.where(sparse.prior_games_d10.lt(10),"fewer_than_10","full_10")
    pd.DataFrame([metric_row(g,name) for name,g in sparse.groupby("history_group")]).to_csv(OUT/"sparse_history_summary.csv",index=False)
    # Exact common-row fixed-window comparisons. Exposure is held at production selected exposure.
    rows=[]
    for label,df in (("historical",hist),("prospective_feature_bound",fbound)):
        common=df[[num(df,c).notna() for c in RATE_FIELDS].all(axis=0)] if False else df.copy()
        valid=np.logical_and.reduce([num(df,c).notna().to_numpy() for c in RATE_FIELDS])
        common=df.loc[valid].copy()
        for source in RATE_FIELDS:
            counter=common.copy(); counter["production_lambda"]=num(counter,source)*num(counter,"selected_toi_minutes")/60
            changes=num(counter,"production_lambda")-num(counter,"replayed_lambda")
            m=metric_row(counter,source); m.update({"population":label,"rate_source":source,"affected_rows":int(changes.abs().gt(1e-12).sum()),
                "common_rows":len(counter),"mean_lambda_change":changes.mean()})
            rows.append(m)
        # D10 to next canonical fallback, only rows where D10 is source.
        eligible=df.replayed_rate_source.eq(RATE_FIELDS[0]) & num(df,RATE_FIELDS[1]).notna()
        counter=df.loc[eligible].copy(); baseline=counter.production_lambda.copy()
        counter["production_lambda"]=num(counter,RATE_FIELDS[1])*num(counter,"selected_toi_minutes")/60
        changes=counter.production_lambda-baseline
        m=metric_row(counter,"replace_D10_with_D20"); m.update({"population":label,"rate_source":"D20_on_D10_rows","affected_rows":int(changes.abs().gt(1e-12).sum()),"common_rows":len(counter),"mean_lambda_change":changes.mean()})
        rows.append(m)
    ablation=pd.DataFrame(rows); ablation.to_csv(OUT/"rate_source_ablations.csv",index=False); ablation.to_csv(OUT/"d10_ablation.csv",index=False); ablation.to_csv(OUT/"rate_window_comparison.csv",index=False)
    relations=pd.concat([relation_table(hist).assign(population="historical"),relation_table(fbound).assign(population="prospective_feature_bound")],ignore_index=True)
    relations.to_csv(OUT/"d5_d10_d20_comparison.csv",index=False)
    # D10 divergence regimes: near means absolute rate gap <= 1 SOG/60 (descriptive cut).
    context=[]
    for label,df in (("historical",hist),("prospective_feature_bound",fbound)):
        for component in ("attempts_d10_per60","role_pp_share","team_d10_sf_per_game","opp_d10_sf_allowed_per_game"):
            vals=num(df,component); ok=vals.notna(); temp=df.loc[ok].copy(); temp["bucket"]=pd.qcut(vals.loc[ok],4,duplicates="drop").astype(str).values if ok.sum() else []
            if len(temp):
                for b,g in temp.groupby("bucket",dropna=False):
                    m=metric_row(g,str(b)); m.update({"population":label,"component":component,"field_present_n":int(ok.sum()),"bucket":b}); context.append(m)
        d10,d20=num(df,"d10_sog_per60"),num(df,"d20_sog_per60")
        common=d10.notna()&d20.notna()
        for regime,mask in (("near_gap_le_1_per60",(d10-d20).abs().le(1)),("divergent_gap_gt_1_per60",(d10-d20).abs().gt(1))):
            g=df.loc[common&mask].copy()
            for rate_name in ("d10_sog_per60","d20_sog_per60"):
                g["production_lambda"]=num(g,rate_name)*num(g,"selected_toi_minutes")/60
                m=metric_row(g,regime+"_"+rate_name); m.update({"population":label,"component":"D10_D20_rate_regime","field_present_n":int(common.sum()),"bucket":regime,"rate_source":rate_name}); context.append(m)
    pd.DataFrame(context).to_csv(OUT/"context_contribution_metrics.csv",index=False)
    # Rate tails by population; fixed quantiles from selected rate.
    tails=[]
    for label,df in (("historical",hist),("prospective_feature_bound",fbound)):
        r=num(df,"replayed_rate");
        for name,q,upper in (("bottom_5pct",.05,False),("bottom_decile",.10,False),("top_decile",.90,True),("top_5pct",.95,True)):
            cut=r.quantile(q if not upper else q)
            g=df[r.ge(cut) if upper else r.le(cut)]
            m=metric_row(g,name); m.update({"population":label,"rate_cut":cut,"rate_mean":r.loc[g.index].mean()}); tails.append(m)
    pd.DataFrame(tails).to_csv(OUT/"rate_tail_metrics.csv",index=False)
    # Sequential prospective persistence is descriptive and strictly chronological.
    seq=fbound.sort_values(["player_id","game_date","game_id"]).copy()
    for c in ("d5_sog_per60","d10_sog_per60","d20_sog_per60"):
        seq["next_"+c]=seq.groupby("player_id")[c].shift(-1)
    pers=[]
    for gap_name,gap,next_gap_name,next_gap in (
        ("prior_D5_minus_D10",num(seq,"d5_sog_per60")-num(seq,"d10_sog_per60"),"next_D5_minus_D10",num(seq,"next_d5_sog_per60")-num(seq,"next_d10_sog_per60")),
        ("prior_D10_minus_D20",num(seq,"d10_sog_per60")-num(seq,"d20_sog_per60"),"next_D10_minus_D20",num(seq,"next_d10_sog_per60")-num(seq,"next_d20_sog_per60"))):
        z=pd.DataFrame({gap_name:gap,next_gap_name:next_gap}).dropna()
        pers.append({"relationship":gap_name+"_to_"+next_gap_name,"n":len(z),"pearson_r":z.iloc[:,0].corr(z.iloc[:,1]),"spearman_rho":z.iloc[:,0].corr(z.iloc[:,1],method="spearman"),"mean_change":z.iloc[:,1].mean(),"mean_abs_change":z.iloc[:,1].abs().mean()})
    pd.DataFrame(pers).to_csv(OUT/"player_rate_persistence.csv",index=False)
    # Explicit use/absence scorecard.
    scorecard=pd.DataFrame([
        ("D10 rate","CORE_CONTRIBUTOR","First valid rate; >99% historical/prospective feature-bound usage."),
        ("D20 rate","CONDITIONAL_CONTRIBUTOR","Second fallback controlled no measured row; fixed-window reference modestly improved count and threshold scores and remains useful for divergence diagnostics."),
        ("D5 rate","NO_DEMONSTRATED_VALUE","Third fallback controlled no measured row; fixed-window D5 scored worse than D10 in both populations."),
        ("Season rate","NO_DEMONSTRATED_VALUE","Not present in production rate coalesce."),
        ("Attempts context","NO_DEMONSTRATED_VALUE","Generated upstream but scorer does not consume attempts."),
        ("Team context","NO_DEMONSTRATED_VALUE","Team shot-generation fields are not read by production scorer."),
        ("Opponent context","NO_DEMONSTRATED_VALUE","Opponent suppression fields are not read by production scorer."),
        ("PP role","NO_DEMONSTRATED_VALUE","Role/PP fields do not modify production rate or lambda."),
    ],columns=["component","classification","evidence"])
    scorecard.to_csv(OUT/"feature_contribution_scorecard.csv",index=False)
    pd.DataFrame([
        ("D10/D20 divergence","Previously identified; handled by existing prospective shadow","NOT_NEW_WEAKNESS"),
        ("Extreme D10 rate estimates","High selected-rate decile and top 5% show negative residuals in historical and prospective samples; the low-rate tail has the reverse bias.","LIKELY_SYSTEMATIC"),
        ("Sparse history","Only 48 prospective feature-bound rows have fewer than 10 prior games; depth groups are too small for a stable history rule.","NO_EVIDENCE_BEYOND_NOISE"),
        ("D5 short horizon","D5 controls no retained row; the fixed-window counterfactual is worse but does not establish a fallback defect.","NO_EVIDENCE_BEYOND_NOISE"),
    ],columns=["candidate","evidence","classification"]).to_csv(OUT/"weakness_scorecard.csv",index=False)
    tree="""# Production SOG rate decision tree\n\nFor each row the scorer converts each input to numeric, treats non-finite values as missing, then selects the first nonmissing value:\n\n1. `d10_sog_per60`\n2. else `d20_sog_per60`\n3. else `d5_sog_per60`\n4. else unscored for missing rate\n\nThe selected rate is accepted when >= 0. Exposure independently coalesces D10 TOI, D20 TOI, D5 TOI, season 5v5+PP minutes, then season 5v5+5v4 seconds converted to minutes. Lambda = selected rate × selected exposure minutes / 60. Negative rate/exposure or non-finite lambda is unscored. There is no rate cap, floor, smoothing, rate transform, season SOG rate, attempts multiplier, team/opponent adjustment, or PP-role multiplier in the scorer. Upstream rolling rates are total prior-window SOG divided by total prior-window TOI × 60; window frames include up to 5/10/20 prior player-game rows; zero/nonpositive denominator yields NULL. No minimum count gate is applied by the scorer.\n"""
    (OUT/"production_rate_decision_tree.md").write_text(tree)
    # Validate replay and summarize all denominators.
    replay=[]
    unreplayable=[]
    for label,df in (("historical",hist),("prospective_oct08_13",prosp),("prospective_feature_bound_oct10_13",fbound)):
        err=num(df,"replay_abs_error"); replay.append({"population":label,"rows":len(df),"replayed":int(err.notna().sum()),"unreplayable":int(err.isna().sum()),"matches_1e-9":int(err.le(TOL).sum()),"mismatches":int(err.gt(TOL).sum()),"max_abs_lambda_difference":err.max()})
        missing=err.isna()
        if missing.any():
            has_rate=num(df,"replayed_rate").notna(); has_toi=num(df,"selected_toi_minutes").notna()
            reason=np.where(~has_rate,"MISSING_RATE_INPUT",np.where(~has_toi,"MISSING_EXPOSURE_INPUT","MISSING_BOUND_FEATURE_ROW"))
            for why,count in pd.Series(reason[missing.to_numpy()]).value_counts().items(): unreplayable.append({"population":label,"reason":why,"rows":int(count)})
    pd.DataFrame(replay).to_csv(OUT/"replay_validation.csv",index=False)
    pd.DataFrame(unreplayable,columns=["population","reason","rows"]).to_csv(OUT/"unreplayable_reasons.csv",index=False)
    allmetrics=pd.concat([pd.DataFrame([metric_row(d,label)]) for label,d in (("historical",hist),("prospective_oct08_13",prosp),("prospective_feature_bound_oct10_13",fbound))],ignore_index=True)
    allmetrics.to_csv(OUT/"population_metrics.csv",index=False)
    inputs=[BASE/"historical_characterization_rows.csv",BASE/"prospective_paired_rows.csv",BASE/"prospective_feature_bound_rows.csv",ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py",ROOT/"backend/nhl/cli.py",ROOT/"backend/nhl/sog_feature_input.py"]
    prepared_sources=[]
    for rel in sorted(hist.prepared_source_path.dropna().unique()):
        p=ROOT/rel
        expected=hist.loc[hist.prepared_source_path.eq(rel),"prepared_source_sha256"].dropna().unique().tolist()
        actual=sha(p) if p.is_file() else None
        prepared_sources.append({"path":rel,"sha256":actual,"retained_expected_sha256":expected,"hash_matches_retained":actual in expected})
    official_sources=[]
    for p in sorted((ROOT/"artifacts/operational/nhl/postgame_reconciliation").glob("2026-10-*/reconciliation=*/canonical_skater_outcomes.csv")):
        if p.parent.parent.name in {f"2026-10-{day:02d}" for day in range(8,14)}:
            official_sources.append({"path":str(p.relative_to(ROOT)),"sha256":sha(p)})
    manifest={"study":"NHL_SOG_SHOT_RATE_CONTRIBUTION_V1","inputs":{str(p.relative_to(ROOT)):{"sha256":sha(p)} for p in inputs},"historical_prepared_sources":prepared_sources,"prospective_official_outcomes":official_sources,"methods":{"key":"distinct game_id/player_id","replay_tolerance":TOL,"historical_depth":"unavailable; retained num_event_last10 is not a player appearance count","prospective_depth_source":"d10_prior_game_count","window_comparison":"exact common rows with production exposure held fixed","provider_calls":0,"paid_credits":0,"production_db_mutations":0}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    (OUT/"research_backlog_context.md").write_text("""# Preserved research backlog\n\n- Bind historical official TOI to certified SOG rows for deeper exposure validation.\n- Grade `NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1` prospectively as final outcomes accumulate.\n- Continue fixed-blend prospective collection.\n- Defer further Puck Line work until at least tomorrow.\n\nNo backlog item was executed by this study.\n""")
    report=["# NHL SOG shot-rate contribution", "", "## Populations and replay", "", "```json", json.dumps(replay,indent=2,default=str), "```", "", "Historical unreplayable rows are primarily missing D10 exposure (42 total); the wider prospective denominator has 501 rows without detailed bound feature inputs. Counts and reasons: `unreplayable_reasons.csv`.", "", "## Actual rate-source use", "", "D10 controls all 13,369 historical rows with a usable rate and all 1,107 feature-bound prospective rows. Neither D20 nor D5 supplied production rate in these measured rows. In the wider 1,608 paired prospective denominator, 501 rows lack bound feature detail, so their source is unknown from retained evidence. See `rate_source_usage.csv`.", "", "## History depth", "", "Prospective feature-bound D10 has 10 prior games for 1,059/1,107 rows; 48 have fewer than 10 (1 row at 2; 8 at 3–4; 21 at 5–7; 18 at 8–9). Their combined MAE is 1.010 versus 1.041 at full 10; the sparse group has only 48 rows and no monotonic pattern across bins, so this does not show material degradation or support a gate. Historical certified rows lack an exact retained prior-game count; `num_event_last10` is not an appearance count. Upstream 5/10/20 rollups use up to the last 5/10/20 prior appearances with a positive aggregate TOI denominator; scorer imposes no minimum-history gate.", "", "## D5/D10/D20 relationships", "", "Across 13,369 historical common rows, D5/D10, D10/D20, and D5/D20 Pearson correlations are 0.862, 0.905, and 0.781; prospective values (n=1,107) are 0.864, 0.918, and 0.776. Mean absolute D5-D10 rate gap is about 1.13 SOG/60; D10-D20 is 0.82 historical and 0.79 prospective. D10-D20 differences persisted across adjacent prospective player captures (n=504, r=0.854); D5-D10 persistence was lower (r=0.704). See `d5_d10_d20_comparison.csv` and `player_rate_persistence.csv`.", "", "## Fixed-window and D10 fallback ablations", "", "On exact common rows, fixed D20 versus D10 reduced historical MAE from 1.035 to 1.016 (n=13,347) and prospective MAE from 1.039 to 1.030 (n=1,107). D5 increased those MAEs to 1.073 and 1.078. D20 also modestly improved threshold proper scores; D5 generally worsened them. These are fixed-window research comparisons, not production changes. See `rate_window_comparison.csv`, `rate_source_ablations.csv`, and `d10_ablation.csv`.", "", "## Context contribution", "", "Production scoring reads only D10/D20/D5 rates plus the independent TOI hierarchy. Attempts, season SOG rate, team/opponent shot context, and PP role are generated upstream but do not enter production lambda. Context-group descriptions are in `context_contribution_metrics.csv`; they are not causal contribution estimates.", "", "## Rate tails and next weakness", "", "Selected-rate tails show consistent regression-to-the-mean error: historical top decile n=1,337 has mean residual -0.54 SOG (top 5% n=669: -0.68); prospective top decile n=111 has -0.56 (top 5% n=56: -0.65). The bottom decile underpredicts by +0.36 historical and +0.25 prospective. Top-decile MAE is 1.42 historical and 1.52 prospective. This is the strongest independent candidate weakness, separate from the already-shadowed D10/D20 divergence. It supports a bounded correction study after more prospective accumulation; no correction is designed or applied here. See `rate_tail_metrics.csv`.", "", "The prior exposure study's actual-TOI oracle reduced prospective MAE by about 0.030 SOG; fixed D20's current-sample rate-side difference is about 0.010 MAE. This descriptive scale comparison suggests tail calibration is the more promising next research direction, without causal attribution.", "", "## Scorecards", "", "See `feature_contribution_scorecard.csv` and `weakness_scorecard.csv`. D10/D20 divergence is the previously identified mechanism handled by the existing shadow, not a new weakness.", "", "## Backlog", "", "See `research_backlog_context.md`.", ""]
    (OUT/"report.md").write_text("\n".join(report))
    summary={"study":manifest["study"],"historical_player_games":len(hist),"prospective_player_games_oct08_13":len(prosp),"prospective_feature_bound_player_games_oct10_13":len(fbound),"replay_validation":replay,"rate_source_usage":usage.to_dict("records"),"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"package_path":str(OUT.relative_to(ROOT))}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=lambda x: None if pd.isna(x) else float(x))+"\n")
    manifest["output_sha256"]={p.name:sha(p) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!="lineage_manifest.json"}
    (OUT/"lineage_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"historical":len(hist),"prospective":len(prosp),"feature_bound":len(fbound),"replay":replay},indent=2,default=str))


if __name__ == "__main__":
    main()
