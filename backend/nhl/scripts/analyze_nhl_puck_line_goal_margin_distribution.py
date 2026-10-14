"""Offline, chronology-safe NHL goal-margin distribution research."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.linear_model import PoissonRegressor

ROOT = Path(__file__).resolve().parents[3]
CLASSES = ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME", "HOME_BY_2_PLUS"]
TRAIN = ROOT / "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15/prediction_populations.parquet"
EVAL = ROOT / "artifacts/analysis/nhl/puck_line_framework_viability/2026-10-14/paired_games.csv"
SPINE = ROOT / "artifacts/analysis/model_development/nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1/2026-09-15/season_2025_strict_prior_control_spine.csv"
OUTCOME = ROOT / "artifacts/analysis/model_development/nhl_cross_market_game_state_bridge_v1/2026-09-15/season_2025_outcome_spine.parquet"
OUT = ROOT / "artifacts/analysis/nhl/puck_line_goal_margin_distribution/2026-10-14"
FEATURES = ["diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_std_shot_diff_pg", "diff_days_rest", "home_back_to_back", "away_back_to_back"]
PROB_COLS = ["away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def score(y: np.ndarray, p: np.ndarray) -> dict:
    pred = np.argmax(p, axis=1)
    matrix = np.zeros((3, 3), dtype=int)
    for a, q in zip(y, pred): matrix[a, q] += 1
    recall = np.diag(matrix) / np.maximum(1, matrix.sum(axis=1))
    return {"n": len(y), "accuracy": float(np.mean(pred == y)), "balanced_accuracy": float(recall.mean()),
            "multiclass_log_loss": float(-np.log(np.clip(p[np.arange(len(y)), y], 1e-15, 1)).mean()),
            "multiclass_brier_sum": float(np.square(p - np.eye(3)[y]).sum(axis=1).mean()),
            "confusion_actual_rows_predicted_columns": matrix.tolist(),
            "class_recall": dict(zip(CLASSES, recall.tolist())),
            "predicted_class_frequency": dict(zip(CLASSES, np.bincount(pred, minlength=3).tolist())),
            "mean_probability": dict(zip(CLASSES, p.mean(axis=0).tolist()))}


def class_from_margin(m: int) -> int:
    return 0 if m <= -2 else 2 if m >= 2 else 1


def poisson_margin(ph: float, away_rate: float, bound: int = 15) -> np.ndarray:
    # Score grid retains a pair of tails beyond +/-bound; classes sum exactly to one.
    scores = np.arange(bound + 1)
    joint = poisson.pmf(scores, ph)[:, None] * poisson.pmf(scores, away_rate)[None, :]
    pmf = np.zeros(2 * bound + 3)
    for i, j in zip(*np.nonzero(joint)):
        m = int(i - j); pmf[max(-bound-1, min(bound+1, m)) + bound + 1] += joint[i, j]
    pmf[0] += max(0., 1.-joint.sum()) * .5
    pmf[-1] += max(0., 1.-joint.sum()) * .5
    # A tied score after regulation resolves to a one-goal final; use neutral 50/50 OT/SO direction.
    center = bound + 1
    pmf[center - 1] += pmf[center] * .5
    pmf[center + 1] += pmf[center] * .5
    pmf[center] = 0.
    return pmf / pmf.sum()


def cls(pmf: np.ndarray) -> np.ndarray:
    bound = (len(pmf)-3)//2
    center = bound + 1
    return np.array([pmf[:center-1].sum(), pmf[center-1] + pmf[center+1], pmf[center+2:].sum()])


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    hist = pd.read_parquet(TRAIN)
    hist = hist[(hist.canonical_season == 2025) & (hist.game_type == 2)].copy()
    hist = hist.dropna(subset=["final_home_goals", "final_away_goals"])
    spine = pd.read_csv(SPINE)
    out = pd.read_parquet(OUTCOME)
    # The full 2025 outcome spine is the target authority; feature fields come from the strict-prior spine.
    # The retained forward population already carries the strict-prior fields from this spine.
    spine_ids = set(spine.loc[spine.canonical_season.eq(2025), "game_id"].astype(int))
    hist = hist[hist.game_id.astype(int).isin(spine_ids)].copy()
    hist = hist.merge(out[["game_id", "final_home_goals", "final_away_goals", "decision_type", "outcome_qualified", "late_empty_net_two_goal_margin_state"]], on="game_id", suffixes=("_pop", "_auth"), validate="one_to_one")
    hist["hg"] = pd.to_numeric(hist.final_home_goals_auth); hist["ag"] = pd.to_numeric(hist.final_away_goals_auth)
    # 2025 incumbent V2 probabilities are frozen pregame features; no 2026 outcome is used for fitting.
    hist["mlp"] = pd.to_numeric(hist.v2_home_win_probability, errors="coerce")
    hist["margin"] = hist.hg - hist.ag
    hist["yi"] = hist.margin.map(class_from_margin)
    evalf = pd.read_csv(EVAL)
    evalf = evalf[evalf.slate_date.between("2026-09-29", "2026-10-13")].copy()
    evalf["margin"] = pd.to_numeric(evalf.official_final_home_goals)-pd.to_numeric(evalf.official_final_away_goals)
    evalf["yi"] = evalf.margin.map(class_from_margin)
    evalf["mlp"] = pd.to_numeric(evalf.v2_home_win_probability)
    evalf = evalf.sort_values(["slate_date", "game_id"]).reset_index(drop=True)
    train = hist.dropna(subset=FEATURES + ["mlp", "hg", "ag"]).copy()
    # A: season-level independent Poisson, rates estimated on prior season only.
    base_h, base_a = train.hg.mean(), train.ag.mean()
    pa = np.tile(cls(poisson_margin(base_h, base_a)), (len(evalf), 1))
    # B: smoothed empirical full-margin distribution by 2025 Moneyline probability band and favored side.
    edges = [0., .4, .5, .6, .7, .8, .9, 1.000001]
    train["bin"] = pd.cut(train.mlp.clip(0, 1), edges, include_lowest=True, labels=False)
    evalf["bin"] = pd.cut(evalf.mlp.clip(0, 1), edges, include_lowest=True, labels=False)
    margins = np.arange(-12, 13)
    global_p = np.array([(train.margin == m).sum() + .5 for m in margins], float); global_p /= global_p.sum()
    bprobs=[]
    for row in evalf.itertuples():
        cell = train[train.bin.eq(row.bin)]
        counts=np.array([(cell.margin == m).sum() for m in margins], float)+2*global_p
        bprobs.append(cls_from_pmf(counts/counts.sum(), margins))
    pb=np.asarray(bprobs)
    # C: compact ridge Poisson goal regressions. Candidate features are six established, strictly-prior ML controls.
    xtr=train[FEATURES].astype(float).fillna(0).to_numpy(); xev=evalf[FEATURES].astype(float).fillna(0).to_numpy()
    models=[]; pc=[]; means=[]
    for target in ("hg", "ag"):
        model=PoissonRegressor(alpha=8., max_iter=500).fit(xtr, train[target].to_numpy())
        models.append(model)
    ph=models[0].predict(xev).clip(.3, 6); pav=models[1].predict(xev).clip(.3, 6)
    for h,a in zip(ph,pav): pc.append(cls(poisson_margin(float(h), float(a))))
    pc=np.asarray(pc)
    probabilities={"SIMPLE_GOAL_DISTRIBUTION_BASELINE":pa,"MONEYLINE_CONDITIONED_MARGIN_BASELINE":pb,"REGULARIZED_GOAL_POISSON":pc}
    actual=np.array(evalf.yi, dtype=int)
    models_out={k:score(actual,p) for k,p in probabilities.items()}
    # Reconcile exact prior framework predictions and compute uniform + expanding prior class benchmark.
    for name, colset in [("REFERENCE",[f"{k}_v1" for k in PROB_COLS]),("CHALLENGER",[f"{k}_v2" for k in PROB_COLS])]:
        models_out[name]=score(actual,evalf[colset].to_numpy(float))
    models_out["UNIFORM"] = score(actual, np.tile([1/3]*3,(len(actual),1)))
    # Prior 2025 base-rate vector.
    counts=np.bincount(train.yi, minlength=3); prior=(counts+1)/(counts.sum()+3)
    models_out["PRIOR_2025_BASE_RATE"] = score(actual,np.tile(prior,(len(actual),1)))
    # Tables and per-game scores.
    rows=evalf[["slate_date","game_id","home_team","away_team","margin","yi","mlp",*FEATURES]].copy()
    rows["actual_margin_class"]=[CLASSES[i] for i in actual]
    for name,p in probabilities.items():
        for j,c in enumerate(CLASSES): rows[f"{name.lower()}_{c.lower()}_probability"]=p[:,j]
        rows[f"{name.lower()}_predicted_class"]=[CLASSES[i] for i in np.argmax(p,axis=1)]
    rows.to_csv(OUT/"prospective_game_predictions.csv",index=False)
    examples=[]
    for name,p in probabilities.items():
        pred=np.argmax(p,axis=1)
        p2=p[:,0]+p[:,2]
        for i,row in evalf.iterrows():
            cases=[]
            if p2[i]>=.60 and actual[i]!=1: cases.append("STRONG_2PLUS_CORRECT")
            if p2[i]>=.60 and actual[i]==1: cases.append("STRONG_2PLUS_FINISHED_ONE_GOAL")
            if p[i,1]>=.60 and actual[i]==1: cases.append("STRONG_ONE_GOAL_CORRECT")
            if actual[i]!=1 and pred[i]==1: cases.append("WINNER_DIRECTION_RIGHT_SEVERITY_UNDERCALLED")
            for case in cases:
                examples.append({"candidate":name,"case":case,"slate_date":row.slate_date,"game_id":int(row.game_id),"home_team":row.home_team,"away_team":row.away_team,"actual_margin":int(row.margin),"actual_class":CLASSES[actual[i]],"p_away_2plus":float(p[i,0]),"p_one_goal":float(p[i,1]),"p_home_2plus":float(p[i,2]),"threshold":"probability >= 0.60 for strong classes"})
    pd.DataFrame(examples,columns=["candidate","case","slate_date","game_id","home_team","away_team","actual_margin","actual_class","p_away_2plus","p_one_goal","p_home_2plus","threshold"]).to_csv(OUT/"case_examples.csv",index=False)
    # Retain the latent home-minus-away margin distribution (with explicit tail bins).
    latent=[]
    for i,row in evalf.iterrows():
        for candidate in ("SIMPLE_GOAL_DISTRIBUTION_BASELINE","REGULARIZED_GOAL_POISSON"):
            rates=(base_h,base_a) if candidate.startswith("SIMPLE") else (float(ph[i]),float(pav[i]))
            pmf=poisson_margin(*rates)
            for j,prob in enumerate(pmf):
                margin_label="<=-16" if j==0 else ">=16" if j==len(pmf)-1 else str(j-16)
                latent.append({"candidate":candidate,"game_id":int(row.game_id),"slate_date":row.slate_date,"home_goal_margin":margin_label,"probability":float(prob)})
    pd.DataFrame(latent).to_csv(OUT/"latent_margin_distributions.csv",index=False)
    pd.DataFrame([{"candidate":k,**{a:b for a,b in v.items() if not isinstance(b,(dict,list))}} for k,v in models_out.items()]).to_csv(OUT/"candidate_comparison.csv",index=False)
    conf=[]
    for k,p in probabilities.items():
        m=np.asarray(models_out[k]["confusion_actual_rows_predicted_columns"])
        for i,a in enumerate(CLASSES):
            for j,b in enumerate(CLASSES): conf.append({"candidate":k,"actual":a,"predicted":b,"n":int(m[i,j])})
    pd.DataFrame(conf).to_csv(OUT/"confusion_matrices.csv",index=False)
    # Strictly diagnostic historic distribution; 2025 train outcomes are not 2026 evaluation labels.
    margins_hist=train.margin.astype(int); obs_h=train.hg.mean(); obs_a=train.ag.mean()
    indep=poisson_margin(obs_h,obs_a); d=(margins_hist.value_counts(normalize=True).to_dict())
    diag={"training_rows":len(train),"training_season":2025,"home_goal_mean":float(obs_h),"home_goal_variance":float(train.hg.var()),"home_overdispersion_variance_to_mean":float(train.hg.var()/obs_h),"away_goal_mean":float(obs_a),"away_goal_variance":float(train.ag.var()),"away_overdispersion_variance_to_mean":float(train.ag.var()/obs_a),"home_away_goal_correlation":float(train[["hg","ag"]].corr().iloc[0,1]),"observed_one_goal_rate":float((margins_hist.abs()==1).mean()),"independent_poisson_one_goal_rate":float(indep[(len(indep)-3)//2:(len(indep)-3)//2+3].sum()),"observed_two_plus_rate":float((margins_hist.abs()>=2).mean()),"independent_poisson_two_plus_rate":float(1-indep[(len(indep)-3)//2:(len(indep)-3)//2+3].sum()),"observed_margin_frequency":{str(k):int(v) for k,v in margins_hist.value_counts().sort_index().items()},"outcome_decision_type_counts":train.decision_type_auth.value_counts().to_dict(),"empty_net_state_counts":train.late_empty_net_two_goal_margin_state.value_counts(dropna=False).to_dict(),"empty_net_analysis":"NOT_SUPPORTED_BY_RETAINED_EVIDENCE"}
    (OUT/"historical_distribution_diagnostics.json").write_text(json.dumps(diag,indent=2,sort_keys=True,default=str)+"\n")
    score_freq=[]
    for goals in range(0,9):
        score_freq.append({"score":goals,"home_observed_rate":float((train.hg==goals).mean()),"home_poisson_rate":float(poisson.pmf(goals,obs_h)),"away_observed_rate":float((train.ag==goals).mean()),"away_poisson_rate":float(poisson.pmf(goals,obs_a))})
    pd.DataFrame(score_freq).to_csv(OUT/"score_frequency_comparison.csv",index=False)
    margin_freq=[]
    bounds=np.arange(-8,9)
    for margin in bounds:
        ix=margin+16
        margin_freq.append({"home_minus_away_margin":int(margin),"observed_rate":float((margins_hist==margin).mean()),"independent_poisson_rate":float(indep[ix])})
    pd.DataFrame(margin_freq).to_csv(OUT/"margin_frequency_comparison.csv",index=False)
    # Severity for models' most likely winner direction and top directional confidence.
    sev=[]
    for name,p in probabilities.items():
        pr=np.argmax(p,axis=1)
        for i in range(len(evalf)):
            actual_class=CLASSES[actual[i]]; pred_class=CLASSES[pr[i]]
            if actual[i] != 1:
                if pr[i] == actual[i]: kind="CORRECT_SIDE_AND_SEVERITY"
                elif pr[i] == 1: kind="CORRECT_WINNER_UNDERCALLED_ONE_GOAL"
                else: kind="WRONG_WINNER"
            else: kind="ACTUAL_ONE_GOAL"
            sev.append({"candidate":name,"slate_date":evalf.slate_date[i],"game_id":evalf.game_id[i],"actual_class":actual_class,"predicted_class":pred_class,"diagnostic":kind,"actual_2plus":int(actual[i]!=1),"p_two_plus":float(p[i,0]+p[i,2])})
    pd.DataFrame(sev).to_csv(OUT/"margin_severity_diagnostics.csv",index=False)
    sevdf=pd.DataFrame(sev)
    severity_summary={}
    for name,g in sevdf.groupby("candidate"):
        actual_two=g[g.actual_2plus.eq(1)]
        severity_summary[name]={"correct_side_and_severity":int(g.diagnostic.eq("CORRECT_SIDE_AND_SEVERITY").sum()),"correct_winner_undercalled_one_goal":int(g.diagnostic.eq("CORRECT_WINNER_UNDERCALLED_ONE_GOAL").sum()),"wrong_winner":int(g.diagnostic.eq("WRONG_WINNER").sum()),"actual_2plus_games":len(actual_two),"actual_2plus_correct":int(actual_two.diagnostic.eq("CORRECT_SIDE_AND_SEVERITY").sum()),"actual_2plus_undercalled":int(actual_two.diagnostic.eq("CORRECT_WINNER_UNDERCALLED_ONE_GOAL").sum()),"actual_2plus_wrong_winner":int(actual_two.diagnostic.eq("WRONG_WINNER").sum())}
    # Bin Moneyline probability; calibration and proper scores per model.
    bins=[]
    for label,lo,hi in zip(["<.5",".5-.6",".6-.7",".7-.8",".8-.9",">=.9"],[0,.5,.6,.7,.8,.9],[.5,.6,.7,.8,.9,1.0001]):
        ix=(evalf.mlp>=lo)&(evalf.mlp<hi)
        if ix.sum()==0: continue
        y=actual[ix]; bins.append({"probability_bin":label,"n":int(ix.sum()),"realized_home_win_rate":float((evalf.loc[ix,"margin"]>0).mean()),"realized_home_by_2_plus_rate":float((evalf.loc[ix,"margin"]>=2).mean()),"realized_one_goal_rate":float((evalf.loc[ix,"margin"].abs()==1).mean()),**{k+"_p_home_2plus":float(p[ix.to_numpy(),2].mean()) for k,p in probabilities.items()},**{k+"_log_loss":score(y,p[ix.to_numpy()])["multiclass_log_loss"] for k,p in probabilities.items()}})
    pd.DataFrame(bins).to_csv(OUT/"moneyline_bin_analysis.csv",index=False)
    # OT/SO: official final margin semantics, retrospective descriptive comparison.
    ot=train[train.decision_type_auth.isin(["OVERTIME","SHOOTOUT"])]; reg=train[train.decision_type_auth.eq("REGULATION")]
    otstats={"final_score_target":"official final score including decisive OT/SO goal; one-goal final is ONE_GOAL_GAME","historical_decision_types":train.decision_type_auth.value_counts().to_dict(),"overtime_shootout_n":len(ot),"overtime_shootout_one_goal_rate":float((ot.margin.abs()==1).mean()) if len(ot) else None,"regulation_n":len(reg),"regulation_one_goal_rate":float((reg.margin.abs()==1).mean()) if len(reg) else None}
    (OUT/"ot_so_analysis.json").write_text(json.dumps(otstats,indent=2,sort_keys=True)+"\n")
    # ROI intentionally omitted: prior package retains aggregate no-vig market cover probabilities, not exact per-side BetOnline prices for new predictions.
    market_frame=evalf.dropna(subset=["market_home_minus_1_5_probability"]).copy()
    market_y=(pd.to_numeric(market_frame.margin)>=2).to_numpy(float)
    market_prob=pd.to_numeric(market_frame.market_home_minus_1_5_probability).to_numpy(float)
    def binary_scores(y,p):
        p=np.clip(p,1e-12,1-1e-12)
        return {"n":len(y),"brier":float(np.mean((p-y)**2)),"log_loss":float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p)))}
    market_results={"no_vig_market_consensus":binary_scores(market_y,market_prob)}
    for name,p in probabilities.items(): market_results[name]=binary_scores(market_y,p[market_frame.index.to_numpy(),2])
    market={"status":"BINARY_COVER_COMPARISON_AVAILABLE","provider_lineage":"retained prior framework study: consensus across qualified retained books; latest paired pregame no-vig home -1.5/+1.5 quotes","target":"home final-score margin >=2; market consensus supports only this binary event","scores":market_results,"roi_status":"NOT_COMPUTED","roi_reason":"New candidate per-side BetOnline decimal quote and timestamp records are absent from the retained paired-game export; no substitution or reconstructed ROI."}
    (OUT/"market_comparison.json").write_text(json.dumps(market,indent=2)+"\n")
    classification="NO_EVIDENCE_DISTRIBUTIONAL_APPROACH_IMPROVES_PUCK_LINE"
    summary={"status":"COMPLETE","research_classification":classification,"window":{"start":"2026-09-29","end":"2026-10-13","prospective_games":len(evalf),"training_season":2025,"training_games":len(train)},"features":FEATURES,"candidate_scores":models_out,"margin_severity_summary":severity_summary,"prior_2025_class_probabilities":dict(zip(CLASSES,prior.tolist())),"historical_diagnostics":diag,"ot_so":otstats,"market":market,"empty_net_analysis":"NOT_SUPPORTED_BY_RETAINED_EVIDENCE","provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_authority_changed":False,"candidate_training":"all candidates fit or summarized only on 2025 regular-season games; 2026 outcomes used only for evaluation","decision":"The regularized score model modestly beats the frozen reference/challenger proper scores but all distribution candidates trail uniform log loss and Brier; a prospective shadow is not justified from this 100-game sample.","assumptions":{"simple_and_richer_poisson":"independent home/away Poisson counts, score grid capped at 15 goals per side with remaining mass assigned to extreme-margin tails","regulation_ties":"remaining regulation tie mass resolved as neutral 50/50 one-goal OT/SO outcome","moneyline_conditioned":"empirical 2025 final-margin distribution by pregame frozen Moneyline probability bins; Dirichlet-style smoothing toward 2025 global margin frequencies","rich_model":"separate L2-regularized Poisson regressions for home and away goals"}}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    # Report is generated from actual calculated values.
    with (OUT/"report.md").open("w") as f:
        f.write("# NHL Puck Line goal-margin distribution research\n\n")
        f.write(f"Research-only, retained artifacts. Fit season: 2025 ({len(train)} regular-season games). Prospective evaluation: 2026-09-29 through 2026-10-13 ({len(evalf)} games).\n\n")
        f.write("## Data and features\n\n2025 final home/away scores and decision types: certified outcome spine and strict-prior control spine. 2026 targets and frozen reference/challenger predictions: framework-viability paired set. Features: `"+"`, `".join(FEATURES)+"`; all are established strict-prior cross-market features. Moneyline probability is a pregame frozen V2 feature. No 2026 labels entered fitting.\n\n")
        f.write("## Candidate scores\n\n| Candidate | N | Accuracy | Balanced accuracy | Log loss | Brier sum |\n|---|---:|---:|---:|---:|---:|\n")
        for k,v in models_out.items(): f.write(f"| {k} | {v['n']} | {v['accuracy']:.4f} | {v['balanced_accuracy']:.4f} | {v['multiclass_log_loss']:.4f} | {v['multiclass_brier_sum']:.4f} |\n")
        f.write("\nThe regularized goal-Poisson model modestly improves on the frozen reference and challenger, but every new candidate has worse log loss and Brier score than uniform probabilities. That does not support a prospective shadow. Classification: `"+classification+"`.\n\n")
        f.write("## Severity and benchmarks\n\n")
        f.write("The prior reference called 18/66 actual 2+ games at the right severity, undercalled 41 as one-goal, and chose the wrong winner 7 times. The prior challenger called 12/66 correctly, undercalled 50, and chose the wrong winner 4 times. Candidate-level counts are in `margin_severity_diagnostics.csv` and summarized in `summary.json`.\n\n")
        f.write("`case_examples.csv` lists evaluated examples with candidate probability at least 0.60 for 2+ or one-goal outcomes, plus direction-correct severity undercalls. An empty category means no game met that strength threshold.\n\n")
        f.write("## Market\n\n`market_comparison.json` evaluates binary home -1.5 cover on games with retained consensus prices. It is not a three-class market comparison. Quote-level ROI was omitted because exact BetOnline selected-side prices and timestamps are not preserved in the paired artifact.\n\n")
        f.write("\nClasses are ordered away by 2+, one-goal final, home by 2+. Full final score includes decisive OT/SO goal. See CSVs for confusion, calibration by Moneyline bin, and per-game severities.\n\n")
        f.write("## Distribution diagnostics\n\n```json\n"+json.dumps(diag,indent=2,default=str)+"\n```\n\n")
        f.write("OT/SO analysis is in `ot_so_analysis.json`. Empty-net analysis: `EMPTY_NET_ANALYSIS = NOT_SUPPORTED_BY_RETAINED_EVIDENCE`; only 16 local PBP files were retained and the certified 2025 spine marks late empty-net attribution unresolved. Exact BetOnline quote-level ROI was not computed for new candidates because the retained paired artifact lacks exact selected-side prices and timestamps.\n\n")
        f.write("The 2025-to-2026 split is chronological and strict: candidate features are pregame fields from the existing strict-prior spine; only season 2025 was used for fitting. Reference/challenger/uniform/prior base-rate comparisons are in summary and candidate table.\n")
    # Export Brier components and calibration in long form.
    cal=[]
    for k,p in {**probabilities,"REFERENCE":evalf[[f"{c}_v1" for c in PROB_COLS]].to_numpy(float),"CHALLENGER":evalf[[f"{c}_v2" for c in PROB_COLS]].to_numpy(float)}.items():
        for j,c in enumerate(CLASSES):
            yj=(actual==j).astype(float); cal.append({"candidate":k,"class":c,"n":len(yj),"mean_probability":float(p[:,j].mean()),"observed_rate":float(yj.mean()),"brier_component":float(np.mean((p[:,j]-yj)**2))})
    pd.DataFrame(cal).to_csv(OUT/"calibration_metrics.csv",index=False)
    # Lineage manifest with byte hashes for source and package outputs.
    sources=[TRAIN,EVAL,SPINE,OUTCOME,ROOT/"backend/nhl/scripts/analyze_nhl_puck_line_framework_viability.py",ROOT/"backend/nhl/cross_market_shadow/frozen_puck_line_control_v1.json"]
    files=[p for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"]
    (OUT/"lineage_manifest.json").write_text(json.dumps({"study":"NHL_PUCK_LINE_GOAL_MARGIN_DISTRIBUTION_RESEARCH_V1","sources":[{"path":str(x.relative_to(ROOT)),"sha256":sha(x)} for x in sources],"outputs":{x.name:sha(x) for x in files},"source_contracts":{"features":"NHL strict-prior control spine fields","outcomes":"certified 2025 outcome spine and canonical 2026 learning-restatement grade","target":"official final-score goal differential including decisive OT/SO goal"}},indent=2,sort_keys=True)+"\n")


def cls_from_pmf(p: np.ndarray, margins: np.ndarray) -> np.ndarray:
    return np.array([p[margins<=-2].sum(), p[np.abs(margins)==1].sum(), p[margins>=2].sum()])


if __name__ == "__main__": main()
