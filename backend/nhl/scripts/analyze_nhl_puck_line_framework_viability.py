"""Characterize retained prospective NHL puck-line evidence without side effects."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
CLASSES = ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME", "HOME_BY_2_PLUS"]
PROBS = ["away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability"]
REF = "NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1"
CHALLENGER = "NHL_PUCK_LINE_SHOT_PRIOR_CHALLENGER_V2"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def score(frame: pd.DataFrame, prefix: str) -> dict:
    actual = frame.actual_margin_class
    pred = frame[f"predicted_{prefix}"]
    n = len(frame)
    confusion = {a: {p: int(((actual == a) & (pred == p)).sum()) for p in CLASSES} for a in CLASSES}
    recalls = {c: confusion[c][c] / max(1, int((actual == c).sum())) for c in CLASSES}
    precisions = {c: confusion[c][c] / max(1, int((pred == c).sum())) for c in CLASSES}
    p = frame[[f"{x}_{prefix}" for x in PROBS]].to_numpy(float)
    y = np.column_stack([(actual == c).to_numpy(float) for c in CLASSES])
    p = np.clip(p, 1e-15, 1)
    return {"n": n, "correct": int((actual == pred).sum()), "accuracy": float((actual == pred).mean()) if n else None,
            "confusion_actual_rows_predicted_columns": confusion, "class_recall": recalls,
            "class_precision": precisions, "balanced_accuracy": float(np.mean(list(recalls.values()))) if n else None,
            "multiclass_log_loss": float(-np.log(p[np.arange(n), y.argmax(axis=1)]).mean()) if n else None,
            "multiclass_brier_sum": float(((p-y)**2).sum(axis=1).mean()) if n else None,
            "mean_probability_by_class": {c: float(p[:, i].mean()) for i,c in enumerate(CLASSES)} if n else {}}


def build(out: Path) -> dict:
    start, end = "2026-09-29", "2026-10-13"
    rows, missing, lineage, market_quote_frames, wager_quote_frames = [], [], [], [], []
    root = ROOT / "artifacts/operational/nhl"
    for date in pd.date_range(start, end, freq="D").strftime("%Y-%m-%d"):
        cbase = root / f"cross_market_shadow/season=2026/slate_date={date}/run_type=FINAL_PREGAME"
        states = sorted(cbase.glob("state=*"))
        rpath = root / f"postgame_reconciliation/learning_restatements/{date}"
        grade_paths = sorted(rpath.glob("*/graded_puck_line.csv")) if rpath.exists() else []
        if not states or not grade_paths:
            missing.append({"date": date, "reason": "MISSING_FINAL_PREGAME_CAPTURE_OR_RECONCILED_GRADE"})
            continue
        cap = states[-1]
        if not (cap / "puck_line_v2_shot_prior_challenger_predictions.csv").is_file():
            reference_rows = pd.read_csv(cap / "puck_line_v1_immutable_predictions.csv")
            for game_id in reference_rows.game_id:
                missing.append({"date": date, "game_id": int(game_id), "reason": "CHALLENGER_NOT_AVAILABLE_IN_PROSPECTIVE_CAPTURE"})
            continue
        # Learning restatement is the canonical latest reconciliation lineage for the date.
        gp = grade_paths[-1]
        ref = pd.read_csv(cap / "puck_line_v1_immutable_predictions.csv").rename(columns={**{p:f"{p}_v1" for p in PROBS},"prediction_creation_time_utc":"prediction_time_v1"})
        chp = pd.read_csv(cap / "puck_line_v2_shot_prior_challenger_predictions.csv").rename(columns={**{p:f"{p}_v2" for p in PROBS},"prediction_creation_time_utc":"prediction_time_v2"})
        grade = pd.read_csv(gp)
        ml = pd.read_csv(cap / "v2_immutable_predictions.csv")
        merged = ref.merge(chp[["game_id", *[f"{p}_v2" for p in PROBS], "prediction_time_v2"]], on="game_id", validate="one_to_one")
        merged = merged.merge(grade[["game_id", "actual_margin_class", "official_final_home_goals", "official_final_away_goals", "canonical_phase", "canonical_game_type_code", "scheduled_start_time_utc"]], on="game_id", validate="one_to_one")
        merged = merged.merge(ml[["game_id", "v2_home_win_probability", "prediction_creation_time_utc"]], on="game_id", suffixes=("", "_ml"), validate="one_to_one")
        merged["slate_date"] = date
        merged["margin_home_minus_away"] = pd.to_numeric(merged.official_final_home_goals)-pd.to_numeric(merged.official_final_away_goals)
        # Only regular-season paired predictions explicitly generated before scheduled start.
        starts = pd.to_datetime(merged.scheduled_start_time_utc_y, utc=True, errors="coerce")
        ref_time = pd.to_datetime(merged.prediction_time_v1, utc=True, errors="coerce")
        ch_time = pd.to_datetime(merged.prediction_time_v2, utc=True, errors="coerce")
        ml_time = pd.to_datetime(merged.prediction_creation_time_utc, utc=True, errors="coerce")
        valid = (merged.actual_margin_class.isin(CLASSES) & merged.canonical_phase.eq("REGULAR_SEASON") &
                 merged.regular_season_evaluation_eligible.astype(str).str.lower().eq("true") &
                 ref_time.lt(starts) & ch_time.lt(starts) & ml_time.lt(starts))
        paired = merged[valid].copy()
        for model, suffix in (("v1", "v1"), ("v2", "v2")):
            cols=[f"{p}_{suffix}" for p in PROBS]
            paired[f"predicted_{model}"] = paired[cols].astype(float).idxmax(axis=1).map(dict(zip(cols,CLASSES)))
        paired["moneyline_favored_side"] = np.where(pd.to_numeric(paired.v2_home_win_probability)>=.5,"HOME","AWAY")
        paired["actual_winner_side"] = np.where(paired.margin_home_minus_away>0,"HOME","AWAY")
        paired["moneyline_direction_correct"] = paired.moneyline_favored_side.eq(paired.actual_winner_side)
        paired["ref_direction_correct"] = paired.predicted_v1.map(lambda x: "HOME" if x=="HOME_BY_2_PLUS" else "AWAY" if x=="AWAY_BY_2_PLUS" else None).eq(paired.actual_winner_side)
        paired["challenger_direction_correct"] = paired.predicted_v2.map(lambda x: "HOME" if x=="HOME_BY_2_PLUS" else "AWAY" if x=="AWAY_BY_2_PLUS" else None).eq(paired.actual_winner_side)
        # Market binary benchmark: average paired, latest pregame no-vig prices by book on home -1.5 / away +1.5.
        market = pd.read_csv(cap / "normalized_market_observations.csv")
        market = market[(market.market_type=="STANDARD_PUCK_LINE") & market.qualification_status.eq("PREGAME_QUALIFIED")].copy()
        market["game_id"] = pd.to_numeric(market.game_id, errors="coerce").astype("Int64")
        market["point"] = pd.to_numeric(market.point, errors="coerce")
        market["observation_timestamp_utc"] = pd.to_datetime(market.observation_timestamp_utc,utc=True,errors="coerce")
        market["start"] = pd.to_datetime(market.scheduled_start_time_utc,utc=True,errors="coerce")
        market=market[market.observation_timestamp_utc < market.start]
        market=market[market.point.isin([-1.5,1.5])]
        wager_quotes=market[market.point.eq(-1.5)].sort_values("observation_timestamp_utc").groupby(["game_id","sportsbook_key","side_orientation"],as_index=False).tail(1)
        wager_quotes=wager_quotes[["game_id","sportsbook_key","side_orientation","decimal_price","observation_timestamp_utc"]].copy()
        wager_quotes["slate_date"] = date
        wager_quote_frames.append(wager_quotes)
        market=market[((market.side_orientation.eq("HOME") & market.point.eq(-1.5)) | (market.side_orientation.eq("AWAY") & market.point.eq(1.5)))].copy()
        market["is_home_cover_quote"] = (market.side_orientation.eq("HOME") & market.point.eq(-1.5)).astype(int)
        market["rawp"] = pd.to_numeric(market.raw_implied_probability,errors="coerce")
        market=market.sort_values("observation_timestamp_utc").groupby(["game_id","sportsbook_key","is_home_cover_quote"],as_index=False).tail(1)
        piv=market.pivot_table(index=["game_id","sportsbook_key"],columns="is_home_cover_quote",values="rawp",aggfunc="first").dropna()
        if 1 not in piv.columns or 0 not in piv.columns:
            piv = piv.iloc[0:0].copy()
            piv["book_home_cover_nv"] = pd.Series(dtype=float)
        else:
            piv["book_home_cover_nv"] = piv[1]/(piv[0]+piv[1])
        book_quotes=piv.reset_index()
        book_quotes["slate_date"] = date
        market_quote_frames.append(book_quotes[["slate_date","game_id","sportsbook_key","book_home_cover_nv"]])
        consensus=(book_quotes.groupby("game_id",as_index=False).book_home_cover_nv.mean()
                   .rename(columns={"book_home_cover_nv":"market_home_minus_1_5_probability"}))
        consensus["game_id"] = pd.to_numeric(consensus.game_id, errors="coerce").astype("Int64")
        paired=paired.merge(consensus,on="game_id",how="left")
        paired["actual_home_minus_1_5_cover"]=(paired.margin_home_minus_away>=2).astype(int)
        paired["reference_home_minus_1_5_probability"] = paired.home_by_2_plus_probability_v1
        paired["challenger_home_minus_1_5_probability"] = paired.home_by_2_plus_probability_v2
        rows.append(paired)
        lineage.append({"slate_date":date,"capture_path":str(cap.relative_to(ROOT)),"capture_state":cap.name,
                        "capture_files_sha256":{n:digest(cap/n) for n in ["puck_line_v1_immutable_predictions.csv","puck_line_v2_shot_prior_challenger_predictions.csv","v2_immutable_predictions.csv","normalized_market_observations.csv"]},
                        "reconciliation_path":str(gp.relative_to(ROOT)),"reconciliation_sha256":digest(gp),"paired_games":len(paired)})
        # Keep exact omitted identities and cause in output.
        for _, r in merged[~valid].iterrows():
            missing.append({"date":date,"game_id":int(r.game_id),"reason":"NOT_FINAL_REGULAR_SEASON_PAIRED_PREGAME"})
    allg=pd.concat(rows,ignore_index=True) if rows else pd.DataFrame()
    if allg.empty: raise RuntimeError("NO_PAIRED_GAMES")
    out.mkdir(parents=True,exist_ok=True)
    allg.to_csv(out/"paired_games.csv",index=False)
    pd.DataFrame(missing,columns=["date","game_id","reason"]).to_csv(out/"unpaired_games.csv",index=False)
    conf=[]
    for mod in ["v1","v2"]:
        s=score(allg,mod)
        for a in CLASSES:
            conf.append({"model":mod,"actual_class":a,"recall":s["class_recall"][a],"precision":s["class_precision"][a],**{f"pred_{p}":s["confusion_actual_rows_predicted_columns"][a][p] for p in CLASSES}})
    pd.DataFrame(conf).to_csv(out/"class_confusion_metrics.csv",index=False)
    calibration=[]
    for mod in ["v1","v2"]:
        for i,c in enumerate(CLASSES):
            observed=float(allg.actual_margin_class.eq(c).mean())
            predicted=float(allg[f"{PROBS[i]}_{mod}"].mean())
            calibration.append({"model":mod,"class":c,"games":len(allg),"mean_predicted_probability":predicted,"observed_frequency":observed,"calibration_gap_predicted_minus_observed":predicted-observed})
    pd.DataFrame(calibration).to_csv(out/"class_probability_calibration.csv",index=False)
    dates=[]
    for date,g in allg.groupby("slate_date"):
        dates.append({"slate_date":date,"games":len(g),"reference_correct":int((g.predicted_v1==g.actual_margin_class).sum()),"challenger_correct":int((g.predicted_v2==g.actual_margin_class).sum()),"away_by_2_plus":int(g.actual_margin_class.eq("AWAY_BY_2_PLUS").sum()),"one_goal_games":int(g.actual_margin_class.eq("ONE_GOAL_GAME").sum()),"home_by_2_plus":int(g.actual_margin_class.eq("HOME_BY_2_PLUS").sum()),"two_plus_games":int((g.margin_home_minus_away.abs()>=2).sum())})
    pd.DataFrame(dates).to_csv(out/"date_level_metrics.csv",index=False)
    cumul=[]
    seen=[]
    for date,g in allg.groupby("slate_date",sort=True):
        seen.append(g)
        z=pd.concat(seen,ignore_index=True)
        cumul.append({"through_date":date,"games":len(z),"reference_correct":int(z.predicted_v1.eq(z.actual_margin_class).sum()),"reference_accuracy":float(z.predicted_v1.eq(z.actual_margin_class).mean()),"challenger_correct":int(z.predicted_v2.eq(z.actual_margin_class).sum()),"challenger_accuracy":float(z.predicted_v2.eq(z.actual_margin_class).mean())})
    pd.DataFrame(cumul).to_csv(out/"cumulative_metrics.csv",index=False)
    disag=allg[allg.predicted_v1.ne(allg.predicted_v2)].copy(); disag.to_csv(out/"disagreement_rows.csv",index=False)
    bins=pd.cut(allg.v2_home_win_probability,[0,.5,.6,.7,.8,.9,1],include_lowest=True)
    b=[]
    for key,g in allg.groupby(bins,observed=False):
        if len(g): b.append({"moneyline_home_probability_bin":str(key),"n":len(g),"one_goal_frequency":float(g.actual_margin_class.eq("ONE_GOAL_GAME").mean()),"two_plus_frequency":float((g.margin_home_minus_away.abs()>=2).mean()),"home_winner_by_2_plus_frequency":float(g.actual_margin_class.eq("HOME_BY_2_PLUS").mean()),"moneyline_direction_accuracy":float(g.moneyline_direction_correct.mean()),"reference_accuracy":float(g.predicted_v1.eq(g.actual_margin_class).mean()),"challenger_accuracy":float(g.predicted_v2.eq(g.actual_margin_class).mean())})
    pd.DataFrame(b).to_csv(out/"moneyline_probability_bins.csv",index=False)
    market=allg.dropna(subset=["market_home_minus_1_5_probability"])
    market_quote_frame=pd.concat(market_quote_frames,ignore_index=True) if market_quote_frames else pd.DataFrame(columns=["slate_date","game_id","sportsbook_key","book_home_cover_nv"])
    market_quote_frame.to_csv(out/"market_provider_coverage.csv",index=False)
    provider_coverage={str(book):int(group.game_id.nunique()) for book,group in market_quote_frame.groupby("sportsbook_key")}
    market_metrics={"games":len(market),"coverage":len(market)/len(allg),"reference_cover_brier":float(((market.reference_home_minus_1_5_probability-market.actual_home_minus_1_5_cover)**2).mean()) if len(market) else None,"challenger_cover_brier":float(((market.challenger_home_minus_1_5_probability-market.actual_home_minus_1_5_cover)**2).mean()) if len(market) else None,"market_cover_brier":float(((market.market_home_minus_1_5_probability-market.actual_home_minus_1_5_cover)**2).mean()) if len(market) else None,"reference_cover_side_accuracy":float(((market.reference_home_minus_1_5_probability>=.5).astype(int)==market.actual_home_minus_1_5_cover).mean()) if len(market) else None,"challenger_cover_side_accuracy":float(((market.challenger_home_minus_1_5_probability>=.5).astype(int)==market.actual_home_minus_1_5_cover).mean()) if len(market) else None,"market_direction_accuracy":float(((market.market_home_minus_1_5_probability>=.5).astype(int)==market.actual_home_minus_1_5_cover).mean()) if len(market) else None,"provider_semantics":"consensus across retained books, latest qualified pregame paired quotes; standard home -1.5 vs away +1.5 binary cover event"}
    market_metrics["provider_game_coverage"] = provider_coverage
    # Expanding prior class mode is strictly pregame; first day has no prior sample.
    expanding=[]
    for date,g in allg.groupby("slate_date",sort=True):
        prior=allg[allg.slate_date<date].actual_margin_class
        if len(prior):
            mode=prior.value_counts().sort_values(ascending=False).index[0]
            expanding.extend((int(a==mode) for a in g.actual_margin_class))
    baseline={"window_modal_class_descriptive":"ONE_GOAL_GAME" if allg.actual_margin_class.value_counts().get("ONE_GOAL_GAME",0)>=max(allg.actual_margin_class.value_counts().get("HOME_BY_2_PLUS",0),allg.actual_margin_class.value_counts().get("AWAY_BY_2_PLUS",0)) else str(allg.actual_margin_class.mode().iloc[0]),"window_modal_accuracy":float(allg.actual_margin_class.value_counts().max()/len(allg)),"expanding_prior_mode_games":len(expanding),"expanding_prior_mode_accuracy":float(np.mean(expanding)) if expanding else None,"moneyline_direction_accuracy":float(allg.moneyline_direction_correct.mean())}
    baseline["uniform_class_probability_log_loss"] = float(math.log(3))
    baseline["uniform_class_probability_brier_sum"] = float(2/3)
    conditional_correct=[]
    conditional_fallbacks=0
    for date,g in allg.groupby("slate_date",sort=True):
        prior=allg[allg.slate_date<date]
        if prior.empty: continue
        for row in g.itertuples(index=False):
            cell=prior[prior.moneyline_favored_side.eq(row.moneyline_favored_side)].actual_margin_class
            if cell.empty:
                cell=prior.actual_margin_class
                conditional_fallbacks+=1
            guess=cell.value_counts().sort_values(ascending=False).index[0]
            conditional_correct.append(guess==row.actual_margin_class)
    baseline["expanding_moneyline_direction_conditioned_class_mode_games"]=len(conditional_correct)
    baseline["expanding_moneyline_direction_conditioned_class_mode_accuracy"]=float(np.mean(conditional_correct)) if conditional_correct else None
    baseline["expanding_moneyline_direction_conditioned_class_mode_overall_fallbacks"]=conditional_fallbacks
    all_wager_quotes=pd.concat(wager_quote_frames,ignore_index=True) if wager_quote_frames else pd.DataFrame(columns=["game_id","sportsbook_key","side_orientation","decimal_price","observation_timestamp_utc","slate_date"])
    all_wager_quotes=all_wager_quotes[all_wager_quotes.sportsbook_key.eq("betonlineag")].rename(columns={"side_orientation":"selected_side"})
    roi_rows=[]
    for mod in ["v1","v2"]:
        picks=allg[["game_id","slate_date","margin_home_minus_away",f"predicted_{mod}"]].copy()
        picks["selected_side"]=picks[f"predicted_{mod}"].map({"HOME_BY_2_PLUS":"HOME","AWAY_BY_2_PLUS":"AWAY"})
        picks=picks.dropna(subset=["selected_side"])
        picks=picks.merge(all_wager_quotes,on=["game_id","slate_date","selected_side"],how="left",validate="one_to_one")
        picks["won"]=np.where(picks.selected_side.eq("HOME"),picks.margin_home_minus_away.ge(2),picks.margin_home_minus_away.le(-2))
        priced=picks.dropna(subset=["decimal_price"])
        returns=np.where(priced.won,pd.to_numeric(priced.decimal_price)-1,-1)
        roi_rows.append({"model":mod,"provider":"BetOnline (betonlineag)","observation_rule":"latest qualified standard -1.5 quote before scheduled start","stake_convention":"one unit staked per predicted two-plus winner; abstain on ONE_GOAL_GAME","candidate_directional_picks":len(picks),"priced_bets":len(priced),"missing_selected_side_price":int(picks.decimal_price.isna().sum()),"one_goal_abstentions":int(allg[f"predicted_{mod}"].eq("ONE_GOAL_GAME").sum()),"wins":int(priced.won.sum()),"losses":int((~priced.won).sum()),"net_units":float(returns.sum()),"roi_per_unit_staked":float(returns.sum()/len(priced)) if len(priced) else None,"slate_coverage":len(priced)/len(allg)})
    summary={"study_window":{"requested_start":start,"latest_reconciled_date":max(allg.slate_date),"dates":sorted(allg.slate_date.unique()),"games":len(allg),"games_per_date":{d:int(n) for d,n in allg.groupby("slate_date").size().items()},"unpaired_or_missing":missing},"models":{"reference":{"identity":REF,"model_sha256":"2b72a59436ace35435b86c2dd1fa36afd3ee65fa73542db5093dd92571121d9a","artifact_sha256":lineage[-1]["capture_files_sha256"]["puck_line_v1_immutable_predictions.csv"],**score(allg,"v1")},"challenger":{"identity":CHALLENGER,"model_sha256":"2b72a59436ace35435b86c2dd1fa36afd3ee65fa73542db5093dd92571121d9a","feature_policy":"NHL_PUCK_LINE_SHOT_PRIOR_POLICY_V1","feature_policy_sha256":digest(ROOT/"backend/nhl/cross_market_shadow/shot_prior.py"),"artifact_sha256":lineage[-1]["capture_files_sha256"]["puck_line_v2_shot_prior_challenger_predictions.csv"],**score(allg,"v2")}},"realized_classes":allg.actual_margin_class.value_counts().reindex(CLASSES,fill_value=0).to_dict(),"home_away_actual_split":{"home_wins":int((allg.margin_home_minus_away>0).sum()),"away_wins":int((allg.margin_home_minus_away<0).sum())},"one_goal_frequency":float(allg.actual_margin_class.eq("ONE_GOAL_GAME").mean()),"two_plus_frequency":float((allg.margin_home_minus_away.abs()>=2).mean()),"goal_margin_bands":{"one_goal":int((allg.margin_home_minus_away.abs()==1).sum()),"two_goal":int((allg.margin_home_minus_away.abs()==2).sum()),"three_plus":int((allg.margin_home_minus_away.abs()>=3).sum())},"margin_goal_counts":{str(int(x)):int(n) for x,n in allg.margin_home_minus_away.abs().value_counts().sort_index().items()},"benchmarks":baseline,"disagreement":{"n":len(disag),"reference_correct_challenger_wrong":int(((disag.predicted_v1==disag.actual_margin_class)&(disag.predicted_v2!=disag.actual_margin_class)).sum()),"challenger_correct_reference_wrong":int(((disag.predicted_v2==disag.actual_margin_class)&(disag.predicted_v1!=disag.actual_margin_class)).sum()),"both_wrong":int(((disag.predicted_v1!=disag.actual_margin_class)&(disag.predicted_v2!=disag.actual_margin_class)).sum()),"realized_classes":disag.actual_margin_class.value_counts().to_dict(),"predicted_classes_reference":disag.predicted_v1.value_counts().to_dict(),"predicted_classes_challenger":disag.predicted_v2.value_counts().to_dict()},"moneyline_bins":b,"market":market_metrics,"roi":{"status":"OMITTED_LOW_MARKET_COVERAGE_AND_NO_UNAMBIGUOUS_THREE_CLASS_TO_BET_SIDE_MAPPING"},"authority_terms":{"repository_uses":"operational/reference and challenger","formal_puck_line_champion_label_found":False},"diagnosis":{"canonical_outcome":"official final score goal differential including the decisive OT/SO goal; absolute margin 1 is ONE_GOAL_GAME, margin >=2 is winner-by-2-plus","market_target":"binary standard -1.5/+1.5 cover, not equivalent to the three-class target","empty_net":"not evaluated; retained inputs do not provide certified empty-net goal attribution in this package","recommendation":"C_DEMOTE_BOTH_TO_BASELINE_REFERENCE_AND_DESIGN_MARGIN_DISTRIBUTION_RESEARCH"},"lineage":lineage,"sources":{"parameter_contract":"backend/nhl/cross_market_shadow/frozen_puck_line_control_v1.json","challenger_policy":"backend/nhl/cross_market_shadow/shot_prior.py","scorer":"backend/nhl/cross_market_shadow/core.py","class_definition":"backend/nhl/postgame_reconcile/core.py"}}
    summary["roi"]={"provider":"BetOnline (betonlineag)","timing":"latest qualified standard -1.5 quote before scheduled start","stake":"one unit per directional two-plus prediction; abstain on ONE_GOAL_GAME","results":roi_rows}
    (out/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    (out/"benchmark_comparison.csv").write_text("benchmark,n,accuracy,log_loss,brier_sum,notes\n"+f"window modal class,{len(allg)},{baseline['window_modal_accuracy']:.8f},,,descriptive full-window class frequency\n"+f"expanding prior mode,{len(expanding)},{baseline['expanding_prior_mode_accuracy'] if expanding else ''},,,prior dates only\n"+f"expanding moneyline-conditioned class mode,{baseline['expanding_moneyline_direction_conditioned_class_mode_games']},{baseline['expanding_moneyline_direction_conditioned_class_mode_accuracy']:.8f},,,prior-date class mode within ML-favored home/away cell\n"+f"uniform class probabilities,{len(allg)},,{baseline['uniform_class_probability_log_loss']:.8f},{baseline['uniform_class_probability_brier_sum']:.8f},no-skill proper-score reference\n"+f"moneyline direction,{len(allg)},{baseline['moneyline_direction_accuracy']:.8f},,,favored winner direction only; not a margin-class score\n")
    (out/"market_comparison.json").write_text(json.dumps(market_metrics,indent=2)+"\n")
    pd.DataFrame(roi_rows).to_csv(out/"roi.csv",index=False)
    source_files = [ROOT / p for p in [
        "backend/nhl/cross_market_shadow/frozen_puck_line_control_v1.json",
        "backend/nhl/cross_market_shadow/shot_prior.py",
        "backend/nhl/cross_market_shadow/core.py",
        "backend/nhl/postgame_reconcile/core.py",
        "backend/nhl/cross_market_shadow/activation_v1.json",
        "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15/model_contract.json",
        "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15/fitted_research_artifact.json",
        "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15/prospective_output_contract.json",
        "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15/report.md",
    ]]
    (out/"lineage_manifest.json").write_text(json.dumps({"study":"NHL_PUCK_LINE_FRAMEWORK_VIABILITY_V1","generated_from_retained_evidence_only":True,"source_artifacts":[{"path":str(p.relative_to(ROOT)),"sha256":digest(p)} for p in source_files],"daily_capture_and_reconciliation_lineage":lineage,"output_files":{p.name:digest(p) for p in out.iterdir() if p.is_file() and p.name!="lineage_manifest.json"}},indent=2,sort_keys=True)+"\n")
    return summary


def main() -> None:
    parser=argparse.ArgumentParser(); parser.add_argument("--out-dir",default="artifacts/analysis/nhl/puck_line_framework_viability/2026-10-14")
    args=parser.parse_args(); summary=build(ROOT/args.out_dir); print(json.dumps({"games":summary["study_window"]["games"],"dates":len(summary["study_window"]["dates"]),"ref":summary["models"]["reference"]["accuracy"],"challenger":summary["models"]["challenger"]["accuracy"],"market":summary["market"]},indent=2))

if __name__=="__main__": main()
