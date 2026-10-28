"""Immutable prospective combined D10 structural correction SOG shadow."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash, verify_package
from backend.nhl.sog_feature_input import sha256_file
from backend.nhl.scripts.score_sog_poisson_baseline import _poisson_tail

MODEL_IDENTITY = "NHL_SOG_D10_STRUCTURAL_CORRECTION_V1"
ROLE = "PROSPECTIVE_SHADOW"
LINES = (1.5, 2.5, 3.5)
STAGE1_THRESHOLD = 0.5
STAGE2_THRESHOLD = 9.0
COMPRESSION = 0.5


def _num(frame: pd.DataFrame, key: str) -> pd.Series:
    if key not in frame:
        return pd.Series(np.nan, index=frame.index, dtype=float)
    v = pd.to_numeric(frame[key], errors="coerce")
    return v.where(np.isfinite(v))


def build_predictions(features: pd.DataFrame, production: pd.DataFrame, *, slate_date: str,
                      run_id: str, feature_cutoff_utc: str,
                      canonical_game_ids: list[int], started_excluded_game_ids: list[int] | None = None) -> pd.DataFrame:
    keys = ["game_id", "player_id"]
    if features.duplicated(keys).any() or production.duplicated(keys).any():
        raise ValueError("SOG_STRUCTURAL_DUPLICATE_PLAYER_GAME")
    if not {"game_id", "player_id", "expected_sog"}.issubset(production.columns):
        raise ValueError("SOG_STRUCTURAL_PRODUCTION_SCHEMA_INVALID")
    pcols = keys + [c for c in ("expected_sog", "selected_sog_rate_per60", "selected_sog_rate_source",
        "selected_toi_minutes", "selected_toi_source", "p_over_1_5", "p_over_2_5", "p_over_3_5") if c in production]
    merged = production[pcols].merge(features, on=keys, how="left", validate="one_to_one", indicator=True, suffixes=("_production", ""))
    if not merged._merge.eq("both").all():
        raise ValueError("SOG_STRUCTURAL_PRODUCTION_ROW_MISSING_FROM_FEATURES")
    if not set(pd.to_numeric(merged.game_id).astype(int)).issubset(set(map(int, canonical_game_ids))):
        raise ValueError("SOG_STRUCTURAL_NONCANONICAL_GAME")
    d10, d20, d5 = (_num(merged, f"{x}_sog_per60") for x in ("d10", "d20", "d5"))
    selected_rate = _num(merged, "selected_sog_rate_per60") if "selected_sog_rate_per60" in merged else d10.where(d10.notna(), d20).where(d10.notna() | d20.notna(), d5)
    toi = _num(merged, "selected_toi_minutes") if "selected_toi_minutes" in merged else None
    if toi is None:
        toi_parts = [_num(merged, x) for x in ("d10_toi_min_avg", "d20_toi_min_avg", "d5_toi_min_avg")]
        toi_parts += [_num(merged, "szn_toi_per_game_5on5") + _num(merged, "szn_toi_per_game_pp"),
                      _num(merged, "season_5on5_icetime_per_game") / 60 + _num(merged, "season_5on4_icetime_per_game") / 60]
        toi = toi_parts[0]
        for part in toi_parts[1:]: toi = toi.where(toi.notna(), part)
    prod_lam = _num(merged, "expected_sog")
    if selected_rate.isna().any() or toi.isna().any() or not selected_rate.ge(0).all() or not toi.ge(0).all():
        raise ValueError("SOG_STRUCTURAL_PRODUCTION_INPUT_INVALID")
    replay_lam = selected_rate * toi / 60
    if prod_lam.isna().any() or not (prod_lam - replay_lam).abs().le(1e-10).all():
        raise ValueError("SOG_STRUCTURAL_PRODUCTION_LAMBDA_REPLAY_FAILED")
    divergence = d10 - d20
    d20_ok = d20.notna()
    use_midpoint = d10.notna() & d20_ok & divergence.abs().gt(STAGE1_THRESHOLD)
    stage1 = selected_rate.astype(float).copy()
    stage1.loc[use_midpoint] = ((d10.loc[use_midpoint] + d20.loc[use_midpoint]) / 2)
    high = d10.gt(STAGE2_THRESHOLD) & stage1.gt(STAGE2_THRESHOLD)
    final_rate = stage1.astype(float).copy()
    final_rate.loc[high] = STAGE2_THRESHOLD + COMPRESSION * (stage1.loc[high] - STAGE2_THRESHOLD)
    combined_lam = final_rate * toi / 60
    div_lam = stage1 * toi / 60
    state = np.select([use_midpoint & high, use_midpoint, high], ["BOTH", "DIVERGENCE_ONLY", "HIGH_RATE_ONLY"], default="NEITHER")
    reason = np.where(~d20_ok, "D20_UNAVAILABLE_DIVERGENCE_SKIPPED", np.where(use_midpoint, "MIDPOINT_CORRECTION_APPLIED", "DIVERGENCE_WITHIN_THRESHOLD"))
    out = merged[[c for c in ("game_id", "player_id", "team_id", "opponent_id", "is_home", "game_date", "season") if c in merged]].copy()
    out.insert(0, "slate_date", slate_date)
    out["parent_run_id"] = run_id
    out["feature_cutoff_utc"] = feature_cutoff_utc
    out["d10_sog_per60"], out["d20_sog_per60"], out["d5_sog_per60"] = d10, d20, d5
    out["divergence"] = divergence
    out["production_exposure_minutes"] = toi
    out["production_exposure_source"] = merged.get("selected_toi_source", pd.Series("production_selected", index=merged.index))
    out["divergence_correction_applied"] = use_midpoint
    out["divergence_reason"] = reason
    out["high_rate_compression_applied"] = high
    out["activation_state"] = state
    out["d20_unavailable"] = ~d20_ok
    out["production_rate_per60"] = selected_rate
    out["rate_after_divergence_per60"] = stage1
    out["final_shadow_rate_per60"] = final_rate
    out["production_lambda"] = prod_lam
    out["divergence_only_lambda"] = div_lam
    out["combined_shadow_lambda"] = combined_lam
    parts=[]
    for line, threshold, pcol in zip(LINES, (2,3,4), ("p_over_1_5","p_over_2_5","p_over_3_5")):
        pp = _num(merged, pcol) if pcol in merged else prod_lam.map(lambda x: _poisson_tail(float(x), threshold))
        pdv = div_lam.map(lambda x: _poisson_tail(float(x), threshold))
        pc = combined_lam.map(lambda x: _poisson_tail(float(x), threshold))
        part=out.copy(); part["line"]=line; part["production_p_over"]=pp; part["divergence_only_p_over"]=pdv; part["combined_p_over"]=pc
        part["delta_p_vs_production"]=pc-pp; part["delta_p_vs_divergence_only"]=pc-pdv
        part["p_under"]=1-pc; part["selected_side"]=np.where(pc.ge(.5),"OVER","UNDER"); parts.append(part)
    result=pd.concat(parts,ignore_index=True)
    ps=result.pivot(index=keys,columns="line",values="combined_p_over")
    if not ((ps[1.5]>=ps[2.5])&(ps[2.5]>=ps[3.5])).all(): raise ValueError("SOG_STRUCTURAL_THRESHOLD_COHERENCE_FAILED")
    probs=result[["production_p_over","divergence_only_p_over","combined_p_over","p_under"]].to_numpy(float)
    if not np.isfinite(probs).all() or (probs<0).any() or (probs>1).any(): raise ValueError("SOG_STRUCTURAL_INVALID_PROBABILITY")
    return result


def capture(*, feature_path: Path, production_path: Path, output_root: Path, run_id: str,
            slate_date: str, season: int, feature_sha256: str, feature_cutoff_utc: str,
            canonical_game_ids: list[int], started_excluded_game_ids: list[int] | None,
            scorer_path: Path, production_identity: tuple[str, str] = ("poisson_baseline", "baseline_v1")) -> dict[str, Any]:
    if sha256_file(feature_path)!=feature_sha256: raise ValueError("SOG_STRUCTURAL_FEATURE_SHA_MISMATCH")
    f,p=pd.read_csv(feature_path),pd.read_csv(production_path)
    kwargs=dict(slate_date=slate_date,run_id=run_id,feature_cutoff_utc=feature_cutoff_utc,canonical_game_ids=canonical_game_ids,started_excluded_game_ids=started_excluded_game_ids)
    frame=build_predictions(f,p,**kwargs); replay=build_predictions(f,p,**kwargs)
    root=Path(output_root)/f"season={season}"/f"slate_date={slate_date}"/f"run_id={run_id}"
    final=root/f"model={MODEL_IDENTITY}"; stage=root/f".{MODEL_IDENTITY}.incomplete"
    root.mkdir(parents=True,exist_ok=True)
    if final.exists(): raise FileExistsError(f"SOG_STRUCTURAL_CAPTURE_ALREADY_EXISTS:{final}")
    stage.mkdir(exist_ok=False)
    pred=stage/"predictions.csv"; frame.to_csv(pred,index=False,float_format="%.17g")
    replay_path=stage/"replay_predictions.csv"; replay.to_csv(replay_path,index=False,float_format="%.17g")
    replay_sha=sha256_file(replay_path)
    if replay_sha!=sha256_file(pred): raise ValueError("SOG_STRUCTURAL_DETERMINISTIC_REPLAY_FAILED")
    replay_path.unlink()
    pg=frame.drop_duplicates(["game_id","player_id"])
    activation={k:int(v) for k,v in pg.activation_state.value_counts().to_dict().items()}
    manifest={"contract":"NHL_SOG_D10_STRUCTURAL_CORRECTION_V1","model_identity":MODEL_IDENTITY,"research_role":ROLE,
      "production_model_identity":f"{production_identity[0]}/{production_identity[1]}","comparator_model_identity":"poisson_baseline/baseline_v1","divergence_shadow_identity":"NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1",
      "stage1_threshold":STAGE1_THRESHOLD,"stage1_weights":{"d10":.5,"d20":.5},"stage2_d10_threshold":STAGE2_THRESHOLD,"stage2_compression_factor":COMPRESSION,
      "parent_daily_run_id":run_id,"season":season,"slate_date":slate_date,"canonical_game_ids":sorted(set(map(int,canonical_game_ids))),"canonical_game_set_hash":canonical_game_set_hash(canonical_game_ids),
      "eligible_game_ids":sorted(set(pd.to_numeric(f.game_id).astype(int))),"started_excluded_game_ids":sorted(set(map(int,started_excluded_game_ids or []))),
      "production_feature_input_path":str(Path(feature_path).resolve()),"production_feature_input_sha256":feature_sha256,
      "production_scorer_path":str(Path(scorer_path).resolve()),"production_scorer_sha256":sha256_file(scorer_path),
      "production_prediction_path":str(Path(production_path).resolve()),"production_prediction_sha256":sha256_file(production_path),
      "feature_cutoff_utc":feature_cutoff_utc,"player_game_count":len(pg),"line_count":3,"prediction_row_count":len(frame),
      "activation_state_counts":activation,"d20_unavailable_count":int(pg.d20_unavailable.sum()),"threshold_coherence_crossing_count":0,
      "prediction_sha256":sha256_file(pred),"replay_prediction_sha256":replay_sha,"deterministic_replay":"PASS","status":"COMPLETE"}
    (stage/"manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    check=pd.read_csv(pred)
    if len(check)!=len(frame) or not check[["game_id","player_id","line"]].equals(frame[["game_id","player_id","line"]]): raise ValueError("SOG_STRUCTURAL_CAPTURE_READBACK_FAILED")
    (stage/"RUN_COMPLETE.json").write_text(json.dumps({"status":"COMPLETE","parent_daily_run_id":run_id})+"\n")
    (stage/"SHA256SUMS").write_text("".join(f"{sha256_file(x)}  {x.name}\n" for x in sorted(stage.iterdir()) if x.is_file() and x.name!="SHA256SUMS"))
    stage.rename(final); verify_package(final)
    manifest.update(path=str(final.resolve()),manifest_sha256=sha256_file(final/"manifest.json"),provider_requests=0,credits_consumed=0,database_write_status="NONE")
    return manifest


def grade_slate(*, slate_date: str, reconciliation_package: Path, capture_root: Path) -> dict[str,Any]:
    outcome_path=Path(reconciliation_package)/"canonical_skater_outcomes.csv"
    if not outcome_path.is_file(): return {"status":"NO_FINAL_OUTCOMES","role":ROLE,"grades":[]}
    outcomes=pd.read_csv(outcome_path); outcomes=outcomes[outcomes.slate_date.astype(str).eq(slate_date)]
    if "participation_state" in outcomes: outcomes=outcomes[outcomes.participation_state.eq("PARTICIPATED")]
    value="shots_on_goal" if "shots_on_goal" in outcomes else "official_sog"
    grades=[]
    for package in sorted(Path(capture_root).glob(f"season=*/slate_date={slate_date}/run_id=*/model={MODEL_IDENTITY}")):
        verify_package(package); man=json.loads((package/"manifest.json").read_text()); pred=pd.read_csv(package/"predictions.csv")
        joined=pred.merge(outcomes[["game_id","player_id",value]],on=["game_id","player_id"],how="left",validate="many_to_one")
        ycount=pd.to_numeric(joined[value],errors="coerce"); settled=ycount.notna()&ycount.ge(0); rows=joined[settled].copy(); ycount=ycount[settled]
        from sklearn.metrics import accuracy_score, average_precision_score, roc_auc_score
        def count_metrics(part, lamcol):
            yy=pd.to_numeric(part[value],errors="coerce"); ll=pd.to_numeric(part[lamcol],errors="coerce"); e=ll-yy
            return {"n":int(len(part)//3),"mean_residual":float(e.mean()) if len(e) else None,"mae":float(e.abs().mean()) if len(e) else None,"rmse":float(np.sqrt((e*e).mean())) if len(e) else None,"mean_lambda":float(ll.mean()) if len(ll) else None}
        def prob_metrics(part, probcol):
            result={};
            for line in LINES:
                z=part[part.line.eq(line)]; y=(pd.to_numeric(z[value])>line).astype(float); p=pd.to_numeric(z[probcol]).clip(1e-15,1-1e-15)
                b=float(((p-y)**2).mean()) if len(z) else None; ll=float((-(y*np.log(p)+(1-y)*np.log(1-p))).mean()) if len(z) else None
                result[str(line)]={"n":len(z),"brier":b,"log_loss":ll,"auc":float(roc_auc_score(y,p)) if y.nunique()==2 else None,"average_precision":float(average_precision_score(y,p)) if y.nunique()==2 else None,"accuracy":float(accuracy_score(y,p.ge(.5))) if len(z) else None,"predicted_over_rate":float(p.mean()) if len(z) else None,"realized_over_rate":float(y.mean()) if len(z) else None,"calibration_bias":float(p.mean()-y.mean()) if len(z) else None}
            return result
        player=rows.drop_duplicates(["game_id","player_id"]); item={"model_identity":MODEL_IDENTITY,"research_role":ROLE,"run_id":man["parent_daily_run_id"],"capture_path":str(package),"prediction_sha256":man["prediction_sha256"],"settled_player_games":int(len(player)),"unsettled_line_rows":int((~settled).sum()),"count":count_metrics(rows,"combined_shadow_lambda"),"by_line":prob_metrics(rows,"combined_p_over"),"activation_state_diagnostics":{}}
        def paired_control(control_rows: pd.DataFrame, label: str, control_lambda: str, control_p: str) -> dict[str, Any]:
            joined_pair=rows.merge(control_rows[["game_id","player_id","line",control_lambda,control_p]],on=["game_id","player_id","line"],how="inner",validate="one_to_one",suffixes=("","_control"))
            yy=pd.to_numeric(joined_pair[value]); cc=pd.to_numeric(joined_pair[control_lambda]); ss=pd.to_numeric(joined_pair.combined_shadow_lambda)
            result={"paired_n":int(len(joined_pair)//3),"count_mae_difference":float((ss-yy).abs().mean()-(cc-yy).abs().mean()) if len(joined_pair) else None,"by_line":{}}
            for ln in LINES:
                q=joined_pair[joined_pair.line.eq(ln)]; y=(pd.to_numeric(q[value])>ln).astype(float); ps=pd.to_numeric(q.combined_p_over).clip(1e-15,1-1e-15); pc=pd.to_numeric(q[control_p]).clip(1e-15,1-1e-15)
                result["by_line"][str(ln)]={"paired_n":len(q),"brier_difference":float(((ps-y)**2).mean()-((pc-y)**2).mean()) if len(q) else None,"log_loss_difference":float((-(y*np.log(ps)+(1-y)*np.log(1-ps))).mean()-(-(y*np.log(pc)+(1-y)*np.log(1-pc))).mean()) if len(q) else None,"side_disagreements":int(ps.ge(.5).ne(pc.ge(.5)).sum()),"wins_gained":int(((ps.ge(.5)==y.eq(1))&(pc.ge(.5)!=y.eq(1))).sum()),"wins_lost":int(((ps.ge(.5)!=y.eq(1))&(pc.ge(.5)==y.eq(1))).sum())}
            return result
        item["paired_vs_production"]=paired_control(rows,"production","production_lambda","production_p_over")
        div_root=Path(capture_root).parent/"sog_divergence_shadows"/f"season={man['season']}"/f"slate_date={slate_date}"/f"run_id={man['parent_daily_run_id']}"/"model=NHL_SOG_D10_D20_DIVERGENCE_SHRINK_V1"
        if div_root.is_dir():
            verify_package(div_root); divrows=pd.read_csv(div_root/"predictions.csv")
            # The comparator is joined by immutable player-game-line identity.
            divrows=divrows.rename(columns={"shadow_lambda":"divergence_only_lambda","shadow_p_over":"divergence_only_p_over"})
            item["paired_vs_divergence_only"]=paired_control(divrows,"divergence_only","divergence_only_lambda","divergence_only_p_over")
        else: item["paired_vs_divergence_only"]={"status":"NO_MATCHING_DIVERGENCE_CAPTURE"}
        for state,grp in player.groupby("activation_state"):
            item["activation_state_diagnostics"][state]={"player_games":len(grp),"count":count_metrics(rows[rows.activation_state.eq(state)],"combined_shadow_lambda"),"by_line":prob_metrics(rows[rows.activation_state.eq(state)],"combined_p_over")}
        d10high=player.d10_sog_per60.gt(9)
        for label,mask in (("d10_gt_9",d10high),("HIGH_RATE_ONLY",player.activation_state.eq("HIGH_RATE_ONLY")),("BOTH",player.activation_state.eq("BOTH"))):
            sub=player.loc[mask,["game_id","player_id"]]; grp=rows.merge(sub,on=["game_id","player_id"]); item.setdefault("high_rate_groups",{})[label]={"count":count_metrics(grp,"combined_shadow_lambda"),"by_line":prob_metrics(grp,"combined_p_over")}
        toi_limit=float(player.production_exposure_minutes.quantile(.9)) if len(player) else float("inf")
        top=player.production_exposure_minutes.ge(toi_limit); sub=player.loc[top,["game_id","player_id"]]
        high_toi=rows.merge(sub,on=["game_id","player_id"])
        item["high_toi_monitoring"]={"toi_cutoff_minutes_top_decile":toi_limit,"player_games":int(top.sum()),"production":count_metrics(high_toi,"production_lambda"),"combined_shadow":count_metrics(high_toi,"combined_shadow_lambda")}
        grades.append(item)
    return {"status":"COMPLETE" if grades else "NO_IMMUTABLE_SHADOW_CAPTURE","role":ROLE,"grades":grades}
