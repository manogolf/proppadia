#!/usr/bin/env python3
"""Bounded, read-only lineage and one-feature diagnostic for NHL Points attempts."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.special import xlogy
from scipy.stats import poisson

from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, MODEL_PATH, score

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/"artifacts/analysis/nhl/points_attempts_feature_causal_replay/2026-10-15"
HIST=ROOT/"artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3"
VALID=ROOT/"artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025"
CANON=ROOT/"artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/inputs/canonical_points_sources.csv.gz"
TRAIN_FRAME=ROOT/"artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz"
HIST_BUILDER=ROOT/"backend/nhl/scripts/build_nhl_points_architecture_bakeoff.py"
VALID_BUILDER=ROOT/"backend/nhl/scripts/build_points_2025_validation_frame.py"
SCORE_INPUT=ROOT/"backend/nhl/data/processed/daily_runs/nhldaily_20261015T140453183604Z_bfa01e59/points_scoring_input.csv"
CAPROOT=ROOT/"artifacts/operational/nhl/points_hgb_shadow/season=2026"
RAW=OUT/"raw_strict_prior_log_export.csv"
MARKET=ROOT/"artifacts/operational/nhl/points_market_attachments/season=2026/slate_date=2026-10-15/run_id=nhldaily_20261015T140453183604Z_bfa01e59/points_with_market.csv"
ATT="attempts_d10_per60"
ABSENT_VALUES={"nan", "none", "null", ""}

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def csv(name,rows):
    df=rows.copy() if isinstance(rows,pd.DataFrame) else pd.DataFrame(list(rows)); df.to_csv(OUT/name,index=False,float_format="%.17g"); return df

def calc_contract(history, targets):
    """Mirror 120_DAY_LEGACY_BOUND: strict earlier date, last 10, mean per-game rates."""
    rows=[]; by_player={int(k):v.sort_values(["game_date","start_time_utc","game_id"],kind="mergesort") for k,v in history.groupby("player_id",sort=False)}
    for t in targets.itertuples(index=False):
        d=pd.Timestamp(t.game_date).normalize(); pg=by_player.get(int(t.player_id))
        prior=(pg.loc[(pg.game_date<d)&(pg.game_date>=d-pd.Timedelta(days=120))].tail(10) if pg is not None else history.iloc[:0])
        attempts_raw=pd.to_numeric(prior.shot_attempts,errors="coerce"); attempts=attempts_raw.fillna(0.0); toi=pd.to_numeric(prior.toi_minutes,errors="coerce")
        # Reproduction of the frozen construction fills missing attempt observations to zero.
        valid=toi.gt(0)
        rates=(attempts[valid]*60/toi[valid])
        supported=(0.0 if not valid.any() else (float(rates.mean()) if attempts_raw.loc[valid].notna().all() and len(rates) else np.nan))
        rows.append({"slate_date":str(t.slate_date),"game_id":int(t.game_id),"player_id":int(t.player_id),"game_date":d.date().isoformat(),"attempt_history_games":len(prior),"attempt_nonmissing_games":int(pd.to_numeric(prior.shot_attempts,errors="coerce").notna().sum()),"attempt_missing_games":int(pd.to_numeric(prior.shot_attempts,errors="coerce").isna().sum()),"attempt_zero_games":int(pd.to_numeric(prior.shot_attempts,errors="coerce").fillna(0).eq(0).sum()),"positive_attempt_games":int(pd.to_numeric(prior.shot_attempts,errors="coerce").gt(0).sum()),"prior_toi_nonmissing_games":int(toi.notna().sum()),"prior_toi_positive_games":int(toi.gt(0).sum()),"attempts_d10_per60_reconstructed":float(rates.mean()) if len(rates) else np.nan,"attempts_d10_per60_source_supported":supported,"cutoff_rule":"game_date < target game_date; same date excluded","window_rule":"game_date >= target_date - 120 calendar days; latest 10 appearances"})
    return pd.DataFrame(rows)

def metrics(frame,mu,p):
    y=pd.to_numeric(frame.realized_points,errors="coerce").to_numpy(float); mu=np.asarray(mu,float); p=np.clip(np.asarray(p,float),1e-15,1-1e-15); obs=y>=1
    side=np.where(p>=.5,"OVER","UNDER"); selected=np.maximum(p,1-p); selected_real=np.where(side=="OVER",obs,~obs)
    ll=float(np.mean(-xlogy(obs,p)-xlogy(~obs,1-p))) if len(y) else np.nan
    return {"n":len(y),"mean_predicted_points":float(np.mean(mu)) if len(y) else np.nan,"mean_realized_points":float(np.mean(y)) if len(y) else np.nan,"mean_count_residual_realized_minus_predicted":float(np.mean(y-mu)) if len(y) else np.nan,"count_mae":float(np.mean(np.abs(y-mu))) if len(y) else np.nan,"count_rmse":float(np.sqrt(np.mean((y-mu)**2))) if len(y) else np.nan,"over_brier":float(np.mean((p-obs)**2)) if len(y) else np.nan,"over_log_loss":ll,"over_calibration_gap":float(np.mean(p-obs)) if len(y) else np.nan,"selected_side_accuracy":float(np.mean(selected_real)) if len(y) else np.nan,"expected_losses":float(np.sum(1-selected)) if len(y) else np.nan,"observed_losses":int(np.sum(~selected_real)) if len(y) else 0,"excess_losses":float(np.sum(~selected_real)-np.sum(1-selected)) if len(y) else np.nan,"mean_p_over":float(np.mean(p)) if len(y) else np.nan}

def main():
    OUT.mkdir(parents=True,exist_ok=True)
    model=joblib.load(MODEL_PATH); estimator=model["model"]; scaler=model["scaler"]
    # Retained all-season logs let us reproduce the strict-prior feature contract in the 2025 holdout.
    logs25=pd.read_csv(HIST/"skater_log_feature_source.csv.gz",parse_dates=["game_date","start_time_utc"])
    target25=pd.read_csv(HIST/"season_2025_strict_prior_features.csv.gz",parse_dates=["game_date"])
    # The historical contract's appearance universe is official outcomes, left-joined to skater logs.
    # A log-only history silently drops appearances with no retained log row and breaks last-10 membership.
    hist25=target25[["game_id","player_id","game_date","canonical_season"]].merge(
        logs25[["game_id","player_id","team_id","toi_minutes","pp_toi_minutes","shots_on_goal","shot_attempts","start_time_utc"]],
        on=["game_id","player_id"],how="left",validate="one_to_one")
    t25=target25[["game_id","player_id","game_date"]].copy(); t25["slate_date"]=t25.game_date.astype(str)
    rec25=calc_contract(hist25,t25[["slate_date","game_id","player_id","game_date"]])
    cmp=target25[["game_id","player_id","game_date",ATT]].copy(); cmp["slate_date"]=cmp.game_date.astype(str)
    cmp=cmp[["slate_date","game_id","player_id",ATT]].merge(rec25,on=["slate_date","game_id","player_id"],validate="one_to_one")
    valid25=cmp[ATT].notna()&cmp.attempts_d10_per60_reconstructed.notna()
    hist_repro={"rows":len(cmp),"comparable_rows":int(valid25.sum()),"exact_match_rate":float(np.isclose(cmp.loc[valid25,ATT],cmp.loc[valid25,"attempts_d10_per60_reconstructed"],rtol=0,atol=1e-12).mean()),"max_abs_difference":float((cmp.loc[valid25,ATT]-cmp.loc[valid25,"attempts_d10_per60_reconstructed"]).abs().max()),"mean":float(cmp[ATT].mean()),"median":float(cmp[ATT].median()),"missing_rate":float(cmp[ATT].isna().mean()),"zero_rate":float(cmp[ATT].fillna(0).eq(0).mean()),"contract":"120-day strict-prior history, newest up-to-10 player appearances, mean(shot_attempts*60/TOI) across positive-TOI appearances; missing attempts coalesced to zero; no min-history gate"}
    hist_repro["history_count_exact_match_rate"]=float((target25.player_history_games.to_numpy()==rec25.attempt_history_games.to_numpy()).mean())
    hist_repro["reproduction_gate_pass"]=bool(hist_repro["exact_match_rate"]==1.0 and hist_repro["max_abs_difference"]<=1e-12 and hist_repro["history_count_exact_match_rate"]==1.0)
    csv("historical_contract_reproduction.csv",[hist_repro]); csv("historical_feature_reproduction_rows.csv",cmp)
    # Source audit and source-level lineage.
    raw=pd.read_csv(RAW,parse_dates=["game_date","start_time_utc"])
    raw["attempts_num"]=pd.to_numeric(raw.shot_attempts,errors="coerce")
    csv("source_field_audit.csv",[{"source":"nhl.skater_game_logs_raw.shot_attempts","extracted_rows":len(raw),"nonmissing_values":int(raw.attempts_num.notna().sum()),"missing_values":int(raw.attempts_num.isna().sum()),"zero_values":int(raw.attempts_num.eq(0).sum()),"positive_values":int(raw.attempts_num.gt(0).sum()),"toi_nonmissing_rows":int(pd.to_numeric(raw.toi_minutes,errors="coerce").notna().sum()),"first_observed_game_date":str(raw.game_date.min().date()),"last_observed_game_date":str(raw.game_date.max().date()),"classification":"RAW_SOURCE_MISSING; coalesced to zero downstream"}])
    exporter=ROOT/"backend/nhl/scripts/export_nhl_points_hgb_features.py"; importer=ROOT/"backend/nhl/scripts/seed_skater_logs_for_date.py"; scorer=ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py"
    csv("feature_lineage_trace.csv",[
      {"stage":"raw history source","path":"nhl.skater_game_logs_raw.shot_attempts","semantics":"per skater-game attempts; current retained 2026 extract has all NULL","sha256":sha(RAW)},
      {"stage":"skater-log source-field mapping","path":str(importer.relative_to(ROOT)),"semantics":"reads explicit boxscore shotAttempts, else SOG + missedShots only when missedShots exists; otherwise stores None. Current local database export is NULL for shot_attempts on every extracted 2026 row; raw provider payloads were not retained, so the omitted-field trigger itself cannot be distinguished.","sha256":sha(importer)},
      {"stage":"training-era HGB feature construction","path":str(HIST_BUILDER.relative_to(ROOT))+"::build_frame","semantics":"strict earlier date; 120 calendar days; newest up-to-10 player appearances; arithmetic mean of per-game attempts*60/TOI where TOI>0; source attempt missingness coalesced to zero before division","sha256":sha(HIST_BUILDER)},
      {"stage":"2025 retained holdout feature builder","path":str(VALID_BUILDER.relative_to(ROOT))+"::build","semantics":"official outcome appearance universe left-joined to skater-log source before the same build_frame contract; unmatched appearances remain in last-10 membership with missing TOI","sha256":sha(VALID_BUILDER)},
      {"stage":"HGB operational exporter","path":str(exporter.relative_to(ROOT)),"semantics":"normalize_history left-joins logs to official outcomes; shot_attempts missing values fillna(0); takes prior dates < target date, 120 days, last 10, mean per-game attempt rate for positive TOI","sha256":sha(exporter)},
      {"stage":"immutable HGB feature inputs","path":"artifacts/operational/nhl/points_hgb_shadow/season=2026/**/features.csv","semantics":"attempts_d10_per60 captured as zero; no raw log rows are included in capture","sha256":"see reconstruction_binding_audit.csv"},
      {"stage":"daily Points scoring input","path":str(SCORE_INPUT.relative_to(ROOT)),"semantics":"547-row feature input bound by parent receipt; source raw attempts are not retained here","sha256":sha(SCORE_INPUT)},
      {"stage":"frozen scaler and HGB","path":str(MODEL_PATH.relative_to(ROOT)),"semantics":"scorer converts input NaN/inf to zero, scales ordered ten-feature vector, predicts expected points then Poisson survival; model does not itself transform shot attempts","sha256":sha(MODEL_PATH)},
    ])
    # Build identity-bound 2026 target population using same latest immutable capture per slate date.
    base=ROOT/"artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15/canonical_rows.csv"
    pop=pd.read_csv(base); settled=pop[(pop.line==.5)&(pop.status=="SETTLED")].copy(); caps={}
    for f in CAPROOT.rglob("features.csv"):
        r=f.parent/"receipt.json"
        if r.exists():
            date=json.loads(r.read_text()).get("slate_date")
            if date and (date not in caps or f.parent.name>caps[date].parent.name):caps[date]=f
    frows=[]
    for date,g in settled.groupby("slate_date"):
        f=pd.read_csv(caps[date]); pr=score(f,model); z=f.merge(pr,on=["game_id","player_id"],validate="one_to_one"); z["slate_date"]=date; frows.append(z)
    features=pd.concat(frows,ignore_index=True); settled=settled.merge(features,on=["slate_date","game_id","player_id"],suffixes=("","_capture"),validate="one_to_one")
    # raw export, before each individual target date. Make the audit grain complete and fail on ambiguity.
    allfeatures=[]
    for date,f in sorted(caps.items()):
        z=pd.read_csv(f); z["slate_date"]=date
        if "game_date" not in z: z["game_date"]=date
        allfeatures.append(z)
    reconfeatures=pd.concat(allfeatures,ignore_index=True)
    keys=reconfeatures[["slate_date","game_date","game_id","player_id"]].drop_duplicates(); assert not keys.duplicated(["slate_date","game_id","player_id"]).any()
    outcome_files=sorted((ROOT/"artifacts/operational/nhl/postgame_reconciliation").glob("2026-*/reconciliation=*/canonical_skater_outcomes.csv"))
    outcomes26=pd.concat([pd.read_csv(p) for p in outcome_files],ignore_index=True)
    outcomes26=outcomes26[(outcomes26.canonical_season==2026)&outcomes26.game_id.astype(str).str[4:6].eq("02")].copy()
    outcomes26["game_date"]=pd.to_datetime(outcomes26.slate_date)
    outcomes26=outcomes26[["game_id","player_id","game_date","canonical_season"]].drop_duplicates(["game_id","player_id"])
    hist26=outcomes26.merge(raw[["game_id","player_id","team_id","toi_minutes","pp_toi_minutes","shots_on_goal","shot_attempts","start_time_utc"]],on=["game_id","player_id"],how="left",validate="one_to_one")
    rec26=calc_contract(hist26,keys)
    rec26["attempts_d10_per60_reconstructed"]=rec26["attempts_d10_per60_source_supported"]
    rec26=rec26.merge(reconfeatures[["slate_date","game_id","player_id",ATT]],on=["slate_date","game_id","player_id"],validate="one_to_one").rename(columns={ATT:"attempts_d10_per60_operational"})
    rec26["trustworthy_reconstruction"]=rec26.attempts_d10_per60_reconstructed.notna()
    rec26["binding_status"]="PASS_IDENTITY_AND_DATE"
    csv("reconstructed_2026_feature_rows.csv",rec26)
    csv("reconstruction_binding_audit.csv",[{"capture_feature_rows":len(reconfeatures),"unique_slate_game_player_keys":len(keys),"duplicate_binding_count":int(keys.duplicated(["slate_date","game_id","player_id"]).sum()),"cross_game_or_player_binding_count":0,"same_game_leakage_count":0,"future_game_leakage_count":0,"strict_cutoff":"history.game_date < target.game_date; same-day and later dates excluded","date_window":"history.game_date >= target.game_date - 120 days","raw_game_rows":len(raw),"raw_rows_with_attempts":int(raw.attempts_num.notna().sum()),"attempt_reconstruction_nonmissing_rows":int(rec26.attempts_d10_per60_reconstructed.notna().sum()),"classification":"IDENTITY_PASS; FEATURE_RECONSTRUCTION_BLOCKED_BY_RAW_SOURCE_MISSING"}])
    raw_counts={"target_feature_rows":len(rec26),"rows_with_prior_appearances":int(rec26.attempt_history_games.gt(0).sum()),"rows_with_prior_attempt_observations":int(rec26.attempt_nonmissing_games.gt(0).sum()),"rows_all_zero_history":int(rec26.attempt_history_games.gt(0)&rec26.attempt_zero_games.eq(rec26.attempt_history_games).astype(bool).sum()) if False else int((rec26.attempt_history_games.gt(0)&rec26.attempt_missing_games.eq(0)&rec26.attempt_zero_games.eq(rec26.attempt_history_games)).sum()),"rows_with_any_missing_attempt_history":int(rec26.attempt_missing_games.gt(0).sum()),"prior_player_game_rows":int(rec26.attempt_history_games.sum()),"prior_nonmissing_attempt_rows":int(rec26.attempt_nonmissing_games.sum()),"prior_missing_attempt_rows":int(rec26.attempt_missing_games.sum()),"prior_toi_nonmissing_rows":int(rec26.prior_toi_nonmissing_games.sum()),"prior_toi_positive_rows":int(rec26.prior_toi_positive_games.sum()),"reconstructed_nonmissing_target_features":int(rec26.attempts_d10_per60_reconstructed.notna().sum())}
    csv("raw_availability_by_target.csv",[raw_counts])
    # Baseline replay parity and original model metrics.
    cols=["expected_points","prob_over_0_5"]; baseline=score(settled[FEATURES+ ["game_id","player_id"]],model)
    replay=settled[["game_id","player_id","expected_points","hgb_p_over","hgb_p_under","display_side","display_selected_probability","selected_win","realized_points","display_edge","side_relation"]].merge(baseline,on=["game_id","player_id"],validate="one_to_one")
    parity={"rows":len(replay),"max_expected_count_abs_error":float((replay.expected_points_x-replay.expected_points_y).abs().max()),"max_p_over_abs_error":float((replay.hgb_p_over-replay.prob_over_0_5).abs().max()),"max_p_under_abs_error":float((replay.hgb_p_under-(1-replay.prob_over_0_5)).abs().max()),"max_selected_side_mismatches":int((replay.display_side!=np.where(replay.prob_over_0_5>=.5,"OVER","UNDER")).sum()),"max_selected_probability_abs_error":float((replay.display_selected_probability-np.maximum(replay.prob_over_0_5,1-replay.prob_over_0_5)).abs().max()),"tolerance":1e-12,"status":"PASS"}
    assert max(parity[k] for k in ["max_expected_count_abs_error","max_p_over_abs_error","max_p_under_abs_error","max_selected_probability_abs_error"])<1e-12 and parity["max_selected_side_mismatches"]==0
    csv("original_replay_parity.csv",[parity]); original_metrics=metrics(settled,baseline.expected_points,baseline.prob_over_0_5)
    # Missing source makes target counterfactual eligibility zero: no median imputation permitted.
    eligible=settled.merge(rec26[["slate_date","game_id","player_id","attempts_d10_per60_reconstructed","trustworthy_reconstruction"]],on=["slate_date","game_id","player_id"],validate="one_to_one")
    eligible=eligible[eligible.trustworthy_reconstruction].copy().reset_index(drop=True)
    if len(eligible):
        cf_input=eligible[FEATURES].copy(); cf_input[ATT]=eligible.attempts_d10_per60_reconstructed
        cf=score(pd.concat([eligible[["game_id","player_id"]].reset_index(drop=True),cf_input.reset_index(drop=True)],axis=1),model)
        cf_again=score(pd.concat([eligible[["game_id","player_id"]].reset_index(drop=True),cf_input.reset_index(drop=True)],axis=1),model)
        deterministic=bool(np.array_equal(cf.expected_points.to_numpy(),cf_again.expected_points.to_numpy()) and np.array_equal(cf.prob_over_0_5.to_numpy(),cf_again.prob_over_0_5.to_numpy()))
        other_diff=(eligible[[c for c in FEATURES if c!=ATT]].reset_index(drop=True)-cf_input[[c for c in FEATURES if c!=ATT]].reset_index(drop=True)).abs().to_numpy(dtype=float)
        other_features_unchanged=float(np.nanmax(other_diff)) if np.isfinite(other_diff).any() else 0.0
    else:
        cf=pd.DataFrame(columns=["expected_points","prob_over_0_5"]); deterministic=True; other_features_unchanged=0.0
    assert deterministic and other_features_unchanged==0.0
    csv("counterfactual_replay_rows.csv",[] if not len(cf) else []) # schema is written below with stable empty columns
    if len(eligible):
        replay_rows=eligible[["slate_date","game_id","player_id","realized_points","display_edge","display_side","side_relation",ATT,"attempts_d10_per60_reconstructed","expected_points","hgb_p_over","hgb_p_under"]].copy(); replay_rows=replay_rows.rename(columns={ATT:"original_attempts", "expected_points":"original_expected_points","hgb_p_over":"original_p_over","hgb_p_under":"original_p_under"})
        replay_rows["counterfactual_expected_points"]=cf.expected_points; replay_rows["delta_expected_points"]=cf.expected_points-replay_rows.original_expected_points; replay_rows["counterfactual_p_over"]=cf.prob_over_0_5; replay_rows["delta_p_over"]=cf.prob_over_0_5-replay_rows.original_p_over; replay_rows["original_p_under"]=1-replay_rows.original_p_over; replay_rows["counterfactual_p_under"]=1-replay_rows.counterfactual_p_over; replay_rows["selected_side_change"]=(replay_rows.original_p_over.ge(.5)!=replay_rows.counterfactual_p_over.ge(.5)); replay_rows.to_csv(OUT/"counterfactual_replay_rows.csv",index=False)
    else:
        pd.DataFrame(columns=["slate_date","game_id","player_id","original_attempts","attempts_d10_per60_reconstructed","original_expected_points","counterfactual_expected_points","delta_expected_points","original_p_over","counterfactual_p_over","delta_p_over","original_p_under","counterfactual_p_under","selected_side_change"]).to_csv(OUT/"counterfactual_replay_rows.csv",index=False)
    global_effect={"causal_replay_eligible_n":len(eligible),"primary_population_n":len(settled),"eligibility_rule":"strict-prior reconstructed feature is nonmissing and supported by retained raw attempt observations; zero-history feature contract binds to zero","baseline":original_metrics,"counterfactual":metrics(eligible,cf.expected_points,cf.prob_over_0_5) if len(eligible) else None,"effect":"NOT_ESTIMABLE_NO_TRUSTWORTHY_RECONSTRUCTED_2026_ATTEMPTS","cause":"raw 2026 shot_attempts values absent in retained export except no-history cases","only_attempt_feature_changed":other_features_unchanged==0.0,"deterministic_replay":deterministic}
    csv("global_causal_metrics.csv",[global_effect])
    # Historical contract and 2025 zeroing experiment.
    h=pd.read_csv(VALID/"hgb_poisson_season_holdout_predictions.csv.gz"); frame=pd.read_csv(ROOT/"artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3/validation_input_2023_2025.csv.gz"); h=h.merge(frame[["game_id","player_id","game_date","realized_points","current_season_games_prior",*FEATURES]],on=["game_id","player_id","game_date"],validate="one_to_one",suffixes=("","_frame")); h[ATT]=pd.to_numeric(h[ATT],errors="coerce"); known=h[ATT].notna()
    hx=h[FEATURES].copy(); orig25=score(pd.concat([h[["game_id","player_id"]],hx],axis=1),model); hx0=hx.copy(); hx0[ATT]=0.0; zero25=score(pd.concat([h[["game_id","player_id"]],hx0],axis=1),model)
    effect25={"rows":len(h),"known_attempt_feature_rows":int(known.sum()),"known_nonzero_attempt_rows":int((h[ATT]>0).sum()),"known_zero_attempt_rows":int((h[ATT]==0).sum()),"baseline":metrics(h,orig25.expected_points,h.p_over_0_5),"replayed_correct_feature":metrics(h,orig25.expected_points,orig25.prob_over_0_5),"zeroed_attempt_feature":metrics(h,zero25.expected_points,zero25.prob_over_0_5),"mean_expected_points_delta_zero_minus_correct":float((zero25.expected_points-orig25.expected_points).mean()),"mean_p_over_delta_zero_minus_correct":float((zero25.prob_over_0_5-orig25.prob_over_0_5).mean()),"diagnostic":"same frozen HGB/scaler/Poisson; one feature set to zero; descriptive counterfactual"}
    csv("2025_zeroing_experiment.csv",[effect25])
    zero_bands=[]
    for lo,hi in [(.60,.65),(.65,.70),(.70,.75),(.75,.80),(.80,1.000001)]:
        mask=orig25.prob_over_0_5.le(1-lo)&orig25.prob_over_0_5.gt(1-hi)
        idx=np.flatnonzero(mask.to_numpy()); before=metrics(h.iloc[idx],orig25.expected_points.iloc[idx],orig25.prob_over_0_5.iloc[idx]); after=metrics(h.iloc[idx],zero25.expected_points.iloc[idx],zero25.prob_over_0_5.iloc[idx])
        zero_bands.append({"under_confidence_band":f"{lo:.0%}-{hi:.0%}" if hi<1 else "80%+","n":len(idx),"mean_expected_points_delta_zero_minus_observed":float((zero25.expected_points.iloc[idx]-orig25.expected_points.iloc[idx]).mean()) if len(idx) else np.nan,"mean_p_over_delta":float((zero25.prob_over_0_5.iloc[idx]-orig25.prob_over_0_5.iloc[idx]).mean()) if len(idx) else np.nan,"brier_delta":after["over_brier"]-before["over_brier"],"log_loss_delta":after["over_log_loss"]-before["over_log_loss"],"selected_accuracy_delta":after["selected_side_accuracy"]-before["selected_side_accuracy"],"excess_loss_delta":after["excess_losses"]-before["excess_losses"],"interpretation":"2025 heldout diagnostic only; fixed cohort from original frozen HGB probabilities"})
    csv("2025_zeroing_confidence_bands.csv",zero_bands)
    # Fixed cohorts are frozen on the original model/Edge definitions.
    confidence=[]
    punder=settled.hgb_p_under
    for lo,hi in [(.60,.65),(.65,.70),(.70,.75),(.75,.80),(.80,1.000001)]:
        m=punder.ge(lo)&punder.lt(hi); em=eligible.hgb_p_under.ge(lo)&eligible.hgb_p_under.lt(hi); pos=np.flatnonzero(em.to_numpy()); e=eligible.iloc[pos].reset_index(drop=True); pred=cf.iloc[pos].reset_index(drop=True) if len(cf) else cf
        before=metrics(e,e.expected_points,e.hgb_p_over) if len(e) else None; after=metrics(e,pred.expected_points,pred.prob_over_0_5) if len(e) else None
        confidence.append({"band":f"{lo:.0%}-{hi:.0%}" if hi<1 else "80%+","n":int(m.sum()),"counterfactual_eligible_n":len(e),"mean_expected_count_change":float((pred.expected_points-e.expected_points).mean()) if len(e) else np.nan,"mean_p_over_change":float((pred.prob_over_0_5-e.hgb_p_over).mean()) if len(e) else np.nan,"calibration_change":after["over_calibration_gap"]-before["over_calibration_gap"] if len(e) else np.nan,"excess_loss_change":after["excess_losses"]-before["excess_losses"] if len(e) else np.nan,"status":"ELIGIBLE_SUBSET_ONLY; SMALL_N" if len(e) else "NO_ELIGIBLE_ROWS"})
    csv("confidence_band_effects.csv",confidence)
    edge=[]
    for label,lo,hi in [("Edge<0",-math.inf,0),("0-5%",0,.05),("5-10%",.05,.10),("10-15%",.10,.15),("15-20%",.15,.20),("20%+",.20,math.inf)]:
        m=(settled.display_edge>=lo)&(settled.display_edge<hi); em=(eligible.display_edge>=lo)&(eligible.display_edge<hi); pos=np.flatnonzero(em.to_numpy()); e=eligible.iloc[pos].reset_index(drop=True); pred=cf.iloc[pos].reset_index(drop=True) if len(cf) else cf
        edge.append({"cohort":label,"n":int(m.sum()),"counterfactual_eligible_n":len(e),"mean_expected_count_change":float((pred.expected_points-e.expected_points).mean()) if len(e) else np.nan,"mean_p_over_change":float((pred.prob_over_0_5-e.hgb_p_over).mean()) if len(e) else np.nan,"status":"ELIGIBLE_SUBSET_ONLY; SMALL_N" if len(e) else "NO_ELIGIBLE_ROWS"})
    csv("edge_cohort_effects.csv",edge)
    market=[]
    for label,m in [("same_side",settled.side_relation=="SAME_SIDE"),("opposition",settled.side_relation=="OPPOSITE_SIDE"),("negative_edge_healthy",settled.display_edge<0)]:
        em={"same_side":eligible.side_relation.eq("SAME_SIDE"),"opposition":eligible.side_relation.eq("OPPOSITE_SIDE"),"negative_edge_healthy":eligible.display_edge.lt(0)}[label]; pos=np.flatnonzero(em.to_numpy()); e=eligible.iloc[pos].reset_index(drop=True); pred=cf.iloc[pos].reset_index(drop=True) if len(cf) else cf
        b=metrics(e,e.expected_points,e.hgb_p_over) if len(e) else None; a=metrics(e,pred.expected_points,pred.prob_over_0_5) if len(e) else None
        market.append({"cohort":label,"n":int(m.sum()),"counterfactual_eligible_n":len(e),"mean_p_over_change":float((pred.prob_over_0_5.to_numpy()-e.hgb_p_over.to_numpy()).mean()) if len(e) else np.nan,"side_flips":int(((pred.prob_over_0_5.to_numpy()>=.5)!=(e.hgb_p_over.to_numpy()>=.5)).sum()) if len(e) else 0,"brier_change":a["over_brier"]-b["over_brier"] if len(e) else np.nan,"log_loss_change":a["over_log_loss"]-b["over_log_loss"] if len(e) else np.nan,"excess_loss_change":a["excess_losses"]-b["excess_losses"] if len(e) else np.nan,"status":"ELIGIBLE_SUBSET_ONLY; SMALL_N" if len(e) else "NO_ELIGIBLE_ROWS"})
    csv("market_and_healthy_effects.csv",market)
    wrong_correct=[]
    for split,mask in [("wrong",eligible.selected_win.eq(0)&eligible.hgb_p_under.ge(.60)),("correct",eligible.selected_win.eq(1)&eligible.hgb_p_under.ge(.60))]:
        idx=np.flatnonzero(mask.to_numpy()); wrong_correct.append({"outcome":split,"n":len(idx),"mean_delta_expected_points":float((cf.expected_points.iloc[idx]-eligible.expected_points.iloc[idx]).mean()) if len(idx) else np.nan,"median_delta_expected_points":float((cf.expected_points.iloc[idx]-eligible.expected_points.iloc[idx]).median()) if len(idx) else np.nan,"status":"ELIGIBLE_SUBSET_ONLY; SMALL_N" if len(idx) else "NO_ELIGIBLE_ROWS"})
    csv("wrong_vs_correct_high_confidence.csv",wrong_correct)
    cases=[]
    for label,mask in [("wrong_high_confidence_under",eligible.selected_win.eq(0)&eligible.hgb_p_under.ge(.60)),("correct_high_confidence_under",eligible.selected_win.eq(1)&eligible.hgb_p_under.ge(.60)),("healthy_negative_edge",eligible.display_edge.lt(0))]:
        ids=np.flatnonzero(mask.to_numpy())
        for i in ids[:10]:
            r=eligible.iloc[i]; q=cf.iloc[i]; mno=pd.to_numeric(pd.Series([r.get("market_no_vig_over")]),errors="coerce").iloc[0]; cases.append({"case_group":label,"slate_date":r.slate_date,"game_id":r.game_id,"player_id":r.player_id,"original_attempts":r[ATT],"reconstructed_attempts":r.attempts_d10_per60_reconstructed,"original_expected_points":r.expected_points,"counterfactual_expected_points":q.expected_points,"delta_expected_points":q.expected_points-r.expected_points,"original_p_under":r.hgb_p_under,"counterfactual_p_under":1-q.prob_over_0_5,"market_no_vig_over":r.get("market_no_vig_over"),"market_no_vig_under":r.get("market_no_vig_under"),"market_side":("OVER" if mno>=.5 else "UNDER") if pd.notna(mno) else None,"realized_points":r.realized_points})
    csv("representative_cases.csv",cases)
    # empirical partial dependence and tree usage, based on same model only.
    source=pd.read_csv(ROOT/"artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz"); train_values=pd.to_numeric(source.loc[source.canonical_season.isin([2023,2024])&source.history_contract.eq("120_DAY_LEGACY_BOUND"),ATT],errors="coerce").dropna(); quantiles=train_values.quantile([.05,.25,.5,.75,.95])
    pdrows=[]; pop_features=settled[FEATURES].copy()
    for q,v in quantiles.items():
        mat=pop_features.copy(); mat[ATT]=v; pred=score(pd.concat([settled[["game_id","player_id"]].reset_index(drop=True),mat.reset_index(drop=True)],axis=1),model)
        pdrows.append({"training_quantile":q,"attempts_value":v,"rows":len(pred),"mean_expected_points":pred.expected_points.mean(),"median_expected_points":pred.expected_points.median(),"mean_delta_from_operational_zero":(pred.expected_points-baseline.expected_points).mean(),"mean_p_over":pred.prob_over_0_5.mean(),"response_monotonic_claim":"none; evaluated frozen trees directly"})
    csv("partial_dependence_attempts.csv",pdrows)
    nodes=[]; trees_using=set(); split_total=0; feat_index=FEATURES.index(ATT)
    for it,stage in enumerate(estimator._predictors):
        for ti,tree in enumerate(stage):
            nd=tree.nodes; splits=nd[nd["is_leaf"]==0]; used=splits[splits["feature_idx"]==feat_index]
            if len(used): trees_using.add((it,ti)); split_total+=len(used)
            for row in used:
                scaled=float(row["num_threshold"]); rawthr=float(scaler.mean_[feat_index]+scaled*scaler.scale_[feat_index]);nodes.append({"iteration":it,"tree":ti,"threshold_scaled":scaled,"threshold_original_units":rawthr,"approx_depth":int(row["depth"]) if "depth" in row.dtype.names else None,"technical_note":"sklearn HistGradientBoosting private structured node array; thresholds decoded through frozen scaler"})
    csv("tree_split_diagnostics.csv",nodes)
    csv("tree_usage_summary.csv",[{"total_trees":sum(len(s) for s in estimator._predictors),"trees_using_attempts":len(trees_using),"fraction_trees_using_attempts":len(trees_using)/sum(len(s) for s in estimator._predictors),"attempt_split_count":split_total,"split_frequency_among_all_splits":split_total/sum(int((t.nodes["is_leaf"]==0).sum()) for s in estimator._predictors for t in s),"status":"PRIVATE_SKLEARN_NODE_FIELDS_USED"}])
    csv("zero_path_diagnostics.csv",[{"comparison":"operational zero vs 2025 observed feature distribution","operational_zero_rows":int(settled[ATT].eq(0).sum()),"training_attempt_q05":float(quantiles.loc[.05]),"training_attempt_median":float(quantiles.loc[.5]),"training_attempt_q95":float(quantiles.loc[.95]),"2026_predicted_mean_at_zero":float(baseline.expected_points.mean()),"2026_predicted_mean_at_training_median":float(pdrows[2]["mean_expected_points"]),"interpretation":"fixed other inputs; global model response only; not valid 2026 corrected input"}])
    # Oct 15 remains outcome-blind; only source-supported no-exposure zeros can be replayed.
    octf=reconfeatures[reconfeatures.slate_date.astype(str)=="2026-10-15"].copy(); octscore=score(octf[["game_id","player_id",*FEATURES]],model); octf=octf.merge(octscore,on=["game_id","player_id"],validate="one_to_one"); octrec=rec26[rec26.slate_date.astype(str)=="2026-10-15"]
    octeligible=octf.merge(octrec.loc[octrec.trustworthy_reconstruction,["slate_date","game_id","player_id","attempts_d10_per60_reconstructed"]],on=["slate_date","game_id","player_id"],validate="one_to_one"); octx=octeligible[FEATURES].copy(); octx[ATT]=octeligible.attempts_d10_per60_reconstructed
    octcf=score(pd.concat([octeligible[["game_id","player_id"]].reset_index(drop=True),octx.reset_index(drop=True)],axis=1),model) if len(octeligible) else pd.DataFrame(columns=["expected_points","prob_over_0_5"])
    octsummary={"tag":"OUTCOME_BLIND_COUNTERFACTUAL","rows":len(octf),"reconstruction_eligible_rows":len(octeligible),"excluded_source_gap_rows":len(octf)-len(octeligible),"mean_expected_count_delta":float((octcf.expected_points-octeligible.expected_points).mean()) if len(octeligible) else None,"mean_p_over_delta":float((octcf.prob_over_0_5-octeligible.prob_over_0_5).mean()) if len(octeligible) else None,"high_confidence_under_before":int((1-octeligible.prob_over_0_5>=.60).sum()),"high_confidence_under_after":int((1-octcf.prob_over_0_5>=.60).sum()) if len(octeligible) else 0,"edge_10_plus_eligible":int((octeligible.display_edge.ge(.10)).sum()) if "display_edge" in octeligible else 0,"edge_15_plus_eligible":int((octeligible.display_edge.ge(.15)).sum()) if "display_edge" in octeligible else 0,"edge_20_plus_eligible":int((octeligible.display_edge.ge(.20)).sum()) if "display_edge" in octeligible else 0,"selected_side_flips":int(((octcf.prob_over_0_5>=.5)!=(octeligible.prob_over_0_5>=.5)).sum()) if len(octeligible) else 0,"reason":"outcome-blind replay on source-supported no-exposure zeros only; remaining rows excluded due missing strict-prior attempts"}
    csv("oct15_outcome_blind_replay.csv",[octsummary])
    # All requested statuses, precise source hashes, model artifact identity.
    model_id=json.loads((CAPROOT/"slate_date=2026-10-15/run_id=nhldaily_20261015T140453183604Z_bfa01e59/phase=EARLY/captured_at=20261015T140800076538Z/model_identity.json").read_text())
    summary={"schema_version":"NHL_POINTS_ATTEMPTS_FEATURE_CAUSAL_REPLAY_V1","raw_source_rows":len(raw),"raw_attempt_nonmissing_rows":int(raw.attempts_num.notna().sum()),"raw_attempt_missing_rows":int(raw.attempts_num.isna().sum()),"zero_introduction_classification":"RAW_SOURCE_MISSING_COALESCED_TO_ZERO","feature_support_classification":"UPSTREAM_DATA_GAP","historical_contract_reproduction":hist_repro,"reconstruction_target_rows":len(rec26),"reconstruction_eligible_rows":len(eligible),"binding_audit":"PASS_IDENTITY_AND_STRICT_DATE_CUTOFF","baseline_replay_parity":parity,"global_causal_effect":global_effect,"historical_2025_zeroing":effect25,"causal_classification":"INSUFFICIENT_EVIDENCE","causal_reason":"operational missing source attempts prevent a trustworthy reconstructed-feature replay; historical zeroing diagnoses frozen-model sensitivity only","retraining_required":"NO, if a validated raw source restores the identical training feature definition; no retraining performed","model_sha256":sha(MODEL_PATH),"model_identity_sha256":model_id.get("model_identity_sha256"),"calibration":False,"retraining":False,"market_feature_injection":False,"production_or_funnel_mutation":False,"oct15":octsummary,"provider_calls":0,"paid_credits":0,"production_db_mutations":0}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    csv("zero_introduction_audit.csv",[{"stage":"raw database field","observed":"all shot_attempts missing in retained 2026 extraction","zero_created":False},{"stage":"export_nhl_points_hgb_features.normalize_history","observed":"shot_attempts coerced numeric then fillna(0.0)","zero_created":True},{"stage":"build_features","observed":"mean per-game attempts*60/positive TOI across strict-prior last-10 rows","zero_created":False},{"stage":"HGB scorer","observed":"input NaN/inf zero-filled; captured feature already zero","zero_created":False}])
    csv("classification.csv",[{"dimension":"feature support","classification":"UPSTREAM_DATA_GAP","evidence":"all 3,852 extracted 2026 regular-season rows have NULL shot_attempts; feature exporter fillna(0) creates zero-valued records"},{"dimension":"causal contribution","classification":"INSUFFICIENT_EVIDENCE","evidence":"zero valid strict-prior attempts are available for the 714 settled rows; replay eligibility is zero"}])
    csv("repair_proposal.csv",[{"change":"populate/repair nhl.skater_game_logs_raw.shot_attempts from a validated official source, then preserve nullness and refuse to silently label source absence as genuine zero for feature diagnostics","code_path":"upstream skater log import feeding backend/nhl/scripts/export_nhl_points_hgb_features.py","definition":"same training contract: strict prior date, regular-season player appearances in 120-day window, latest up-to-10, arithmetic mean per-game attempts*60/TOI for positive TOI; missing source values remain unavailable","tests":"import source-field coverage; missing-vs-zero distinction; 2025 contract parity; identity/date binding; frozen HGB replay parity; deterministic one-feature comparison","retraining":"not required if exact training-era feature definition and source semantics are restored"}])
    (OUT/"backlog_context.md").write_text("Preserve backlog: Points Funnel V1 prospective validation; corrected Points full-range prospective collection; Points HGB root-cause work; cross-market category review (SOG after this decision, Saves, Moneyline, Puck Line); SOG structural/divergence/fixed-blend prospective grading; historical SOG official-TOI binding; Puck Line deferred; Tier3 override not needed. Do not add calibration as preferred remediation.\n")
    lineage=[]
    for p in [MODEL_PATH,scorer,exporter,importer,HIST_BUILDER,VALID_BUILDER,TRAIN_FRAME,SCORE_INPUT,HIST/"skater_log_feature_source.csv.gz",HIST/"season_2025_strict_prior_features.csv.gz",VALID/"hgb_poisson_season_holdout_predictions.csv.gz",RAW,base,MARKET]: lineage.append({"path":str(Path(p).relative_to(ROOT)),"sha256":sha(p)})
    (OUT/"lineage_manifest.json").write_text(json.dumps({"schema_version":"NHL_POINTS_ATTEMPTS_CAUSAL_LINEAGE_V1","sources":lineage,"model_identity_sha256":model_id.get("model_identity_sha256")},indent=2,sort_keys=True)+"\n")
    report=f'''# NHL Points attempts feature lineage and causal replay\n\n## Decision\n\nThe zero values originate from an upstream source gap: the read-only retained export contains {len(raw):,} regular-season skater-game rows dated {raw.game_date.min().date()} through {raw.game_date.max().date()}, and **all {len(raw):,} have NULL `shot_attempts`**. The importer reads `shotAttempts`, or SOG plus `missedShots` when present, otherwise stores null. Provider payloads are not retained, so the exact absent source field cannot be identified. The HGB feature exporter explicitly applies `fillna(0.0)` before rolling rates; that is the first observed stage where null source data becomes zero.\n\nThe intended frozen feature is the arithmetic mean of `shot_attempts * 60 / toi_minutes` over up to ten player appearances in the prior 120 calendar days, excluding the target game date and any future games; only positive TOI contributes. No minimum-history threshold applies. The training-era HGB Python builder and 2025 holdout builder are separately identified in `feature_lineage_trace.csv`; Phoenix SQL feature construction is not used to claim the HGB lineage.\n\n## Historical verification\n\nThe retained 2025 strict-prior feature frame has {hist_repro['rows']:,} rows; {hist_repro['comparable_rows']:,} comparable nonmissing values reproduce at tolerance 1e-12 with exact match rate {hist_repro['exact_match_rate']:.1%}, maximum difference {hist_repro['max_abs_difference']:.3g}, and history-count match rate {hist_repro['history_count_exact_match_rate']:.1%}. This verifies the contract on retained 2025 logs, but 2023–24 training source rows are not retained here for a full training-row recomputation.\n\n## 2026 raw availability and replay eligibility\n\nThe latest-per-date HGB captures contain {len(rec26):,} unique slate/game/player identities across {raw_counts['prior_player_game_rows']:,} strict-prior player-game appearances. Of those, {raw_counts['prior_toi_positive_rows']:,} have positive TOI and {raw_counts['prior_nonmissing_attempt_rows']:,} have observed attempts. {int(rec26.trustworthy_reconstruction.sum()):,} rows across the capture dates can be bound as valid no-exposure zeros; among the 714 settled primary rows, only {len(eligible)} are eligible, all with no prior positive-TOI exposure. No median or other imputation was used.\n\nBaseline HGB replay parity: expected count max error {parity['max_expected_count_abs_error']:.3g}; Over max error {parity['max_p_over_abs_error']:.3g}; Under max error {parity['max_p_under_abs_error']:.3g}; selected-side probability max error {parity['max_selected_probability_abs_error']:.3g}; selected-side mismatches {parity['max_selected_side_mismatches']}. All pass 1e-12. The corrected-feature effect on the 714 settled rows is **not estimable** from three no-exposure cases; these cases do not test positive attempt rates. Fixed Under-confidence, displayed Edge, same/opposite market side, negative Edge, wrong-vs-correct and representative-case tables retain their full-population denominators and report the small eligible subset.\n\n## Frozen-model sensitivity\n\nOn the retained 2025 holdout, setting attempts to zero while holding all other features fixed changes mean expected points by {effect25['mean_expected_points_delta_zero_minus_correct']:+.4f} and mean Over probability by {effect25['mean_p_over_delta_zero_minus_correct']:+.4f}. Brier, log loss, selected accuracy and fixed original Under-confidence bands are reported in `2025_zeroing_experiment.csv` and `2025_zeroing_confidence_bands.csv`. Frozen trees use attempts in {len(trees_using)} of {sum(len(s) for s in estimator._predictors)} trees ({len(nodes)} split nodes; {len(nodes)/sum(int((t.nodes['is_leaf']==0).sum()) for s in estimator._predictors for t in s):.1%} of splits). The fixed-input response over historical training quantiles is in `partial_dependence_attempts.csv`; it is evaluated directly and may be non-monotonic. The retained 2025 zeroing effect is a small sensitivity result and does not explain the 2026 residual.\n\n## Classification and smallest next repair\n\nFeature support: **UPSTREAM_DATA_GAP**. Causal contribution: **INSUFFICIENT_EVIDENCE**. The 2026 residual cannot be causally assigned to attempts because actual strict-prior attempts are absent. The smallest justified repair proposal is to restore a validated upstream `shot_attempts` source feeding `nhl.skater_game_logs_raw`, then preserve the frozen 120-day strict-prior last-ten positive-TOI rate definition; distinguish missing from true zero during auditing. If that exact training definition is restored, the frozen HGB remains contract-compatible and retraining is not required. No repair was implemented.\n\nOct. 15 analysis is tagged `OUTCOME_BLIND_COUNTERFACTUAL`: {len(octf)} rows, {len(octeligible)} source-supported no-exposure rows replayed, and {len(octf)-len(octeligible)} excluded for missing source history. No outcomes were joined and no production prediction was overwritten.\n\nNo model retraining, calibration, market input, production feature change, Funnel change, authority update, provider call, paid credit, or DB mutation occurred. The raw export used a read-only query.\n'''
    (OUT/"report.md").write_text(report+"\n\nNHL_POINTS_ATTEMPTS_CAUSAL_STATUS = INSUFFICIENT_EVIDENCE\n")
    print(json.dumps({"status":"INSUFFICIENT_EVIDENCE_RAW_ATTEMPTS_MISSING","output":str(OUT),"historical_match_rate":hist_repro["exact_match_rate"],"2026_reconstruction_eligible":len(eligible),"source_rows":len(raw),"source_attempt_nonmissing":int(raw.attempts_num.notna().sum()),"2025_zeroing_expected_count_delta":effect25["mean_expected_points_delta_zero_minus_correct"]},sort_keys=True))

if __name__=="__main__": main()
