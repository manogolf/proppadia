"""Immutable season-2026 NHL Saves P/M shadow and conditional grading."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.nhl.saves_quote_capture.core import QUALIFIED, iso, parse_utc, sha256_file, write_manifest
from backend.nhl.scripts.score_nhl_props import interp_apply, prepare_X, prob_over_poisson

ROOT = Path(__file__).resolve().parents[3]
IDENTITY_PATH = Path(__file__).with_name("frozen_saves_v1.json")
POLICY_C_FIXTURE = ROOT/"artifacts/analysis/model_development/nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance_v1_regenerated/2026-09-08/authoritative_replay_v2"
GAME_TYPES = {1: "PRESEASON", 2: "REGULAR_SEASON", 3: "POSTSEASON"}
RUN_TYPES = {"MIDDAY", "FINAL_PREGAME"}
POLICY_STATUS = "P_M_SHADOW_ONLY_C_U_E_UNAUTHORIZED"
STARTER_LABEL = "MARKET_LISTED_STARTER_UNCONFIRMED"
IDENTITY_COLUMNS = [
    "canonical_season", "slate_date", "game_id", "goalie_id", "goalie_name", "team",
    "opponent", "scheduled_start_time_utc", "game_type_code", "feature_cutoff_timestamp_utc",
    "feature_history_max_timestamp_utc", "goalie_eligibility_state", "roster_source_timestamp_utc",
    "population_contract", "scorer_eligible",
    "expected_complete_population_rows",
]
ACTUAL_LEAKAGE_COLUMNS = {
    "start_flag", "actual_start_flag", "actual_starter", "confirmed_starter", "projected_starter",
    "actual_participation", "participation_state", "official_saves", "saves",
}


def digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _array_hash(value: Any) -> str:
    array = np.asarray(value, dtype=float)
    out = hashlib.sha256(); out.update(str(array.dtype).encode()); out.update(str(array.shape).encode()); out.update(array.tobytes())
    return out.hexdigest()


def frozen_identity() -> dict[str, Any]:
    return json.loads(IDENTITY_PATH.read_text())


def verify_frozen_identity() -> dict[str, Any]:
    identity = frozen_identity()
    for key in ("model_artifact", "model_index", "feature_metadata", "scorer"):
        if sha256_file(ROOT/identity[f"{key}_path"]) != identity[f"{key}_sha256"]:
            raise RuntimeError(f"SAVES_{key.upper()}_HASH_DRIFT")
    artifact = json.loads((ROOT/identity["model_artifact_path"]).read_text())
    index = json.loads((ROOT/identity["model_index_path"]).read_text())
    model = artifact.get("sklearn_poisson", {})
    if artifact.get("family") != "poisson" or index.get("family") != "poisson": raise RuntimeError("SAVES_MODEL_FAMILY_DRIFT")
    if model.get("feature_order") != identity["feature_order"]: raise RuntimeError("SAVES_FEATURE_ORDER_DRIFT")
    if digest(model["feature_order"]) != identity["feature_order_sha256"]: raise RuntimeError("SAVES_FEATURE_ORDER_HASH_DRIFT")
    if _array_hash(model["coef"]) != identity["coefficient_sha256"]: raise RuntimeError("SAVES_COEFFICIENT_HASH_DRIFT")
    if _array_hash([model["intercept"]]) != identity["intercept_sha256"]: raise RuntimeError("SAVES_INTERCEPT_HASH_DRIFT")
    position = identity["start_prob_zero_based_position"]
    if model["feature_order"][position] != "start_prob" or not math.isclose(model["coef"][position], identity["start_prob_coefficient"], abs_tol=0):
        raise RuntimeError("SAVES_START_PROB_CONTRACT_DRIFT")
    return identity


def _artifact(identity: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    artifact = json.loads((ROOT/identity["model_artifact_path"]).read_text())
    index = json.loads((ROOT/identity["model_index_path"]).read_text())
    metadata = json.loads((ROOT/identity["feature_metadata_path"]).read_text())["goalie_saves"]
    raw_features = metadata if isinstance(metadata, list) else next(metadata[x] for x in ("features", "columns", "feature_cols", "cols") if isinstance(metadata.get(x), list))
    return artifact, index, list(map(str, raw_features))


def score_frozen(features: pd.DataFrame, start_prob: float | None, identity: dict[str, Any] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Score one complete batch. `None` preserves historical null-to-zero semantics."""
    identity = identity or verify_frozen_identity(); artifact,index,raw_features = _artifact(identity)
    missing = [x for x in set(raw_features)|{"player_id","game_id"} if x not in features]
    if missing: raise ValueError(f"SAVES_FEATURE_INPUT_MISSING:{','.join(sorted(missing))}")
    frame=features.copy(); frame["start_prob"] = np.nan if start_prob is None else float(start_prob)
    model=artifact["sklearn_poisson"]; X=prepare_X(frame,raw_features,model["feature_order"])
    mu=np.exp(X.values@np.asarray(model["coef"],dtype=float)+float(model["intercept"]))
    probabilities: dict[float,np.ndarray]={}
    raw_by_line: dict[float,np.ndarray]={}
    for line in identity["lines"]:
        raw=np.clip(prob_over_poisson(mu,float(line)),1e-6,1-1e-6); raw_by_line[float(line)]=raw
        key=f"{float(line):g}"; raw_metric=index.get("metrics_holdout",{}).get(key); cal_metric=index.get("metrics_holdout_calibrated",{}).get(key); cfg=artifact.get("calibration",{}).get(key)
        use=bool(cfg and raw_metric and cal_metric and (("log_loss" in raw_metric and "log_loss" in cal_metric and cal_metric["log_loss"]<=raw_metric["log_loss"]) or ("log_loss" not in raw_metric and cal_metric.get("brier",2)<=raw_metric.get("brier",1))))
        probabilities[float(line)]=np.clip(interp_apply(raw,np.asarray(cfg["grid_x"]),np.asarray(cfg["grid_y"])),1e-6,1-1e-6) if use else raw
    matrix=np.column_stack([probabilities[float(x)] for x in identity["lines"]])
    for j in range(1,matrix.shape[1]): matrix[:,j]=np.minimum(matrix[:,j],matrix[:,j-1])
    rows=[]
    for i,row in frame.reset_index(drop=True).iterrows():
        for j,line in enumerate(identity["lines"]):
            rows.append({"game_id":int(row.game_id),"goalie_id":int(row.player_id),"line":float(line),"expected_saves":float(mu[i]),
                "raw_prob_over":float(raw_by_line[float(line)][i]),"prob_over":float(matrix[i,j]),"prob_under":float(1-matrix[i,j]),
                "start_prob_operational_input":None if start_prob is None else float(start_prob),"model":identity["model_name"],"model_version":identity["model_version"]})
    return pd.DataFrame(rows), X


def _wide(predictions: pd.DataFrame, order: pd.DataFrame | None = None) -> pd.DataFrame:
    wide=predictions.pivot(index=["goalie_id","game_id"],columns="line",values="prob_over").reset_index()
    wide=wide.rename(columns={"goalie_id":"player_id",**{line:f"p_over_{str(line).replace('.','_')}" for line in predictions.line.unique()}})
    if order is not None:
        keys=order[["player_id","game_id"]].drop_duplicates().reset_index(drop=True); keys["_ordinal"]=keys.index
        wide=keys.merge(wide,on=["player_id","game_id"],how="left",validate="one_to_one").sort_values("_ordinal").drop(columns="_ordinal")
    return wide[["player_id","game_id"]+[f"p_over_{str(x).replace('.','_')}" for x in frozen_identity()["lines"]]]


def verify_historical_parity() -> dict[str, Any]:
    identity=verify_frozen_identity(); ip=ROOT/identity["historical_fixed_input"]["path"]; op=ROOT/identity["historical_fixed_output"]["path"]
    if sha256_file(ip)!=identity["historical_fixed_input"]["sha256"] or sha256_file(op)!=identity["historical_fixed_output"]["sha256"]: raise RuntimeError("SAVES_HISTORICAL_FIXTURE_HASH_DRIFT")
    frame=pd.read_csv(ip); predictions,_=score_frozen(frame,None,identity); data=_wide(predictions,frame).to_csv(index=False).encode()
    if hashlib.sha256(data).hexdigest()!=identity["historical_fixed_output"]["sha256"]: raise RuntimeError("SAVES_HISTORICAL_REPLAY_PARITY_FAILURE")
    return {"status":"EXACT_BYTE_PARITY","input_rows":28,"prediction_rows":28,"output_sha256":hashlib.sha256(data).hexdigest()}


def verify_operational_amendment() -> dict[str, Any]:
    identity=verify_frozen_identity(); frame=pd.read_csv(ROOT/identity["historical_fixed_input"]["path"])
    old,_=score_frozen(frame,None,identity); new,_=score_frozen(frame,1.0,identity)
    p=float((new.prob_over-old.prob_over).abs().max()); mu=float((new.expected_saves-old.expected_saves).abs().max())
    contract=identity["operational_amendment"]
    if p>contract["max_probability_delta"]+1e-15 or mu>contract["max_expected_saves_delta"]+1e-15: raise RuntimeError("SAVES_OPERATIONAL_AMENDMENT_BOUND_EXCEEDED")
    return {"status":"BOUNDED_OPERATIONAL_INPUT_CONTRACT_AMENDMENT","start_prob":1.0,"max_probability_delta":p,"max_probability_delta_percentage_points":100*p,"max_expected_saves_delta":mu}


def verify_policy_c_historical_fixture() -> dict[str, Any]:
    identity=verify_frozen_identity(); manifest=POLICY_C_FIXTURE/"SHA256SUMS"
    if sha256_file(manifest)!=identity["policy_c"]["source_package_manifest_sha256"]: raise RuntimeError("POLICY_C_PARENT_MANIFEST_HASH_DRIFT")
    for raw in manifest.read_text().splitlines():
        expected,name=raw.split("  ",1)
        if sha256_file(POLICY_C_FIXTURE/name)!=expected: raise RuntimeError(f"POLICY_C_PARENT_HASH_MISMATCH:{name}")
    policies=pd.read_csv(POLICY_C_FIXTURE/"prospective_policy_evaluation.csv")
    row=policies[policies.policy.eq("C_MULTIPLE_BOOKS_UNIQUE_GOALIE_AGREEMENT")]
    if len(row)!=1:raise RuntimeError("POLICY_C_HISTORICAL_FIXTURE_IDENTITY_FAILURE")
    row=row.iloc[0];universe=int(row.actual_starter_team_game_universe);selected=int(row.selected_team_games);mismatches=int(row.mismatches)
    contract=identity["policy_c"]
    if (universe,selected,mismatches)!=(contract["historical_team_games_universe"],contract["historical_team_games_selected"],contract["historical_mismatches"]): raise RuntimeError("POLICY_C_HISTORICAL_FIXTURE_PARITY_FAILURE")
    return {"status":"EXACT_CERTIFIED_POLICY_C_POPULATION_PARITY","universe_team_games":universe,"selected_team_games":selected,"mismatches":mismatches,"mismatch_rate":mismatches/selected}


def _verify_manifest(directory: Path) -> None:
    if not (directory/"RUN_COMPLETE.json").exists() or not (directory/"SHA256SUMS").exists(): raise RuntimeError("PARENT_INCOMPLETE_OR_UNMANIFESTED")
    for raw in (directory/"SHA256SUMS").read_text().splitlines():
        expected,name=raw.split("  ",1)
        if sha256_file(directory/name)!=expected: raise RuntimeError(f"PARENT_HASH_MISMATCH:{name}")


def _validate_inputs(games: pd.DataFrame, goalies: pd.DataFrame, slate_date: str, run_timestamp_utc: str) -> None:
    need_games={"canonical_season","slate_date","game_id","home_team","away_team","scheduled_start_time_utc","game_type_code"}
    if need_games-set(games) or set(IDENTITY_COLUMNS)-set(goalies): raise ValueError("SAVES_IDENTITY_SCHEMA_INCOMPLETE")
    leaked=ACTUAL_LEAKAGE_COLUMNS&set(goalies)
    if leaked: raise RuntimeError(f"ACTUAL_OR_STARTER_STATE_LEAKAGE:{','.join(sorted(leaked))}")
    if games.empty or games.game_id.duplicated().any() or not games.canonical_season.eq(2026).all() or not games.slate_date.astype(str).eq(slate_date).all(): raise RuntimeError("GAME_SPINE_IDENTITY_FAILURE")
    if not games.game_type_code.isin(GAME_TYPES).all(): raise RuntimeError("UNKNOWN_GAME_TYPE")
    run=parse_utc(run_timestamp_utc); starts=pd.to_datetime(games.scheduled_start_time_utc,utc=True,errors="coerce")
    if starts.isna().any() or (run>=starts).any(): raise RuntimeError("RUN_NOT_STRICTLY_PREGAME")
    if goalies.empty or goalies.duplicated(["game_id","goalie_id"]).any() or goalies.goalie_id.isna().any(): raise RuntimeError("GOALIE_GAME_IDENTITY_FAILURE")
    if not goalies.population_contract.eq("COMPLETE_SCORER_ELIGIBLE").all() or not goalies.scorer_eligible.map(lambda x:str(x).lower() in {"true","1"}).all(): raise RuntimeError("PRE_SCORING_POPULATION_NOT_COMPLETE")
    expected=pd.to_numeric(goalies.expected_complete_population_rows,errors="coerce")
    if expected.isna().any() or expected.nunique()!=1 or int(expected.iloc[0])!=len(goalies): raise RuntimeError("PRE_SCORING_POPULATION_COLLAPSE")
    if goalies.get("start_prob",pd.Series(np.nan,index=goalies.index)).notna().any(): raise RuntimeError("CALLER_SUPPLIED_START_PROB_FORBIDDEN")
    joined=goalies.merge(games,on="game_id",how="left",suffixes=("_goalie","_game"),validate="many_to_one")
    team=joined.team.astype(str); home=joined.home_team.astype(str); expected=np.where(team.eq(home),joined.away_team,joined.home_team)
    checks=[joined.canonical_season_goalie.eq(joined.canonical_season_game),joined.slate_date_goalie.astype(str).eq(joined.slate_date_game.astype(str)),
            pd.to_numeric(joined.game_type_code_goalie).eq(pd.to_numeric(joined.game_type_code_game)),team.eq(home)|team.eq(joined.away_team.astype(str)),
            joined.opponent.astype(str).eq(pd.Series(expected,index=joined.index).astype(str)),
            pd.to_datetime(joined.scheduled_start_time_utc_goalie,utc=True,errors="coerce").eq(pd.to_datetime(joined.scheduled_start_time_utc_game,utc=True,errors="coerce"))]
    if any(not x.all() for x in checks): raise RuntimeError("GOALIE_GAME_PARENT_OR_ORIENTATION_MISMATCH")
    start=pd.to_datetime(goalies.scheduled_start_time_utc,utc=True,errors="coerce")
    for column in ["feature_cutoff_timestamp_utc","feature_history_max_timestamp_utc","roster_source_timestamp_utc"]:
        stamp=pd.to_datetime(goalies[column],utc=True,errors="coerce")
        if stamp.isna().any() or (stamp>=start).any() or (stamp>run).any(): raise RuntimeError(f"STRICT_PRIOR_TIMING_FAILURE:{column}")


def policy_c_selection(quotes: pd.DataFrame, population: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame]:
    q=_latest_qualified(quotes)
    support=(q.groupby(["game_id","team","goalie_id"],dropna=False).sportsbook.nunique().reset_index(name="distinct_book_support"))
    rows=[]
    for key,group in population.groupby(["game_id","team"],sort=True):
        s=support[(support.game_id==key[0])&(support.team.astype(str)==str(key[1]))]
        if s.empty: decision,selected,reason="BLOCKED",None,"NO_SAVES_MARKET"
        else:
            top=int(s.distinct_book_support.max()); winners=s[s.distinct_book_support.eq(top)]
            if top<2: decision,selected,reason="BLOCKED",None,"INSUFFICIENT_MULTIBOOK_AGREEMENT"
            elif len(winners)!=1: decision,selected,reason="BLOCKED",None,"MULTIPLE_LISTED_GOALIES_AMBIGUOUS"
            else: decision,selected,reason="SELECTED",int(winners.iloc[0].goalie_id),"POLICY_C_UNIQUE_TOP_MULTIBOOK"
        rows.append({"game_id":key[0],"team":key[1],"policy":"C_MULTIPLE_BOOKS_UNIQUE_GOALIE_AGREEMENT","decision":decision,
                     "selected_goalie_id":selected,"starter_state_label":STARTER_LABEL if selected is not None else None,"reason":reason,
                     "listed_goalie_count":int(len(s)),"maximum_distinct_book_support":0 if s.empty else int(s.distinct_book_support.max())})
    return pd.DataFrame(rows),support


def _market_view(quotes: pd.DataFrame, selected: pd.DataFrame, shadow_run_id: str) -> pd.DataFrame:
    chosen=selected[selected.decision.eq("SELECTED")][["game_id","team","selected_goalie_id"]].rename(columns={"selected_goalie_id":"goalie_id"})
    q=_latest_qualified(quotes).merge(chosen,on=["game_id","team","goalie_id"],how="inner")
    rows=[]
    for key,g in q.groupby(["game_id","goalie_id","line"],sort=True):
        row={"run_id":shadow_run_id,"game_id":key[0],"goalie_id":key[1],"line":float(key[2]),"starter_state_label":STARTER_LABEL,
             "sportsbooks":"|".join(sorted(set(g.sportsbook.astype(str)))),"distinct_book_count":int(g.sportsbook.nunique()),
             "market_evidence_sha256":digest(g.sort_values(["sportsbook","side","raw_price"]).to_dict("records"))}
        for side in ["OVER","UNDER"]:
            x=g[g.side.eq(side)]; row[f"median_{side.lower()}_price"]=None if x.empty else float(pd.to_numeric(x.raw_price).median()); row[f"{side.lower()}_quote_count"]=len(x)
        rows.append(row)
    columns=["run_id","game_id","goalie_id","line","starter_state_label","sportsbooks","distinct_book_count","market_evidence_sha256","median_over_price","over_quote_count","median_under_price","under_quote_count"]
    return pd.DataFrame(rows,columns=columns)


def _latest_qualified(quotes: pd.DataFrame) -> pd.DataFrame:
    q=quotes[quotes.quote_qualification_status.isin(QUALIFIED)&quotes.canonical_prop_type.eq("goalie_saves")].copy()
    if q.empty:return q
    stamps=q[["provider_quote_timestamp_utc","provider_market_timestamp_utc","source_timestamp_utc","capture_timestamp_utc"]].apply(lambda x:pd.to_datetime(x,utc=True,errors="coerce"))
    q["_effective_timestamp"]=stamps.max(axis=1)
    keys=["game_id","team","goalie_id","sportsbook","line","side"]
    return q.sort_values(keys+["_effective_timestamp","provider_outcome_id"]).drop_duplicates(keys,keep="last").drop(columns="_effective_timestamp")


def make_run_id(slate_date: str, run_timestamp_utc: str, run_type: str) -> str:
    if run_type not in RUN_TYPES: raise ValueError("INVALID_RUN_TYPE")
    return f"nhlsavesshadow_s2026_d{slate_date.replace('-','')}_t{parse_utc(run_timestamp_utc).strftime('%Y%m%dT%H%M%S%fZ')}_{run_type}_v1"


def run_shadow(*, game_spine_csv: Path, game_spine_manifest: Path, goalie_inputs_csv: Path, goalie_inputs_manifest: Path,
               quote_run_dir: Path, output_root: Path, slate_date: str, run_timestamp_utc: str, run_type: str,
               pre_scoring_population: str="COMPLETE_SCORER_ELIGIBLE", candidate_policy_json: Path|None=None) -> Path:
    if pre_scoring_population!="COMPLETE_SCORER_ELIGIBLE": raise RuntimeError("PRE_SCORING_MARKET_FILTER_FORBIDDEN")
    if candidate_policy_json is not None: raise RuntimeError("SAVES_CANDIDATE_POLICY_UNAUTHORIZED")
    identity=verify_frozen_identity(); parity=verify_historical_parity(); amendment=verify_operational_amendment()
    run_id=make_run_id(slate_date,run_timestamp_utc,run_type); dest=output_root/"2026"/slate_date/run_id; staging=dest.with_name(dest.name+".incomplete")
    lock_dir=output_root/"locks";lock_dir.mkdir(parents=True,exist_ok=True);lock=(lock_dir/f"{slate_date}_{run_type}.lock").open("a+")
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError as exc: raise RuntimeError("SAVES_SHADOW_ALREADY_RUNNING") from exc
    if dest.exists() or staging.exists(): raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    for manifest,path in [(game_spine_manifest,game_spine_csv),(goalie_inputs_manifest,goalie_inputs_csv)]:
        entries={name:h for h,name in (x.split("  ",1) for x in manifest.read_text().splitlines())}
        if entries.get(path.name)!=sha256_file(path): raise RuntimeError(f"PARENT_HASH_MISMATCH_OR_MUTABLE:{path.name}")
    _verify_manifest(quote_run_dir)
    games,goalies=pd.read_csv(game_spine_csv),pd.read_csv(goalie_inputs_csv);_validate_inputs(games,goalies,slate_date,run_timestamp_utc)
    qmeta=json.loads((quote_run_dir/"run_metadata.json").read_text())
    if qmeta.get("slate_date")!=slate_date or qmeta.get("run_type")!=run_type or qmeta.get("canonical_season")!=2026: raise RuntimeError("QUOTE_RUN_IDENTITY_CONFLICT")
    if parse_utc(qmeta["capture_timestamp_utc"])>parse_utc(run_timestamp_utc): raise RuntimeError("QUOTE_RUN_AFTER_SHADOW_RUN")
    quotes=pd.read_csv(quote_run_dir/"saves_quotes.csv")
    scoring_inputs=goalies.copy();scoring_inputs["player_id"]=scoring_inputs.goalie_id
    predictions,_=score_frozen(scoring_inputs,1.0,identity)
    if len(predictions)!=len(goalies)*len(identity["lines"]): raise RuntimeError("PRE_SCORING_POPULATION_COLLAPSE")
    identity_part=goalies[["game_id","goalie_id","goalie_name","team","opponent","game_type_code","scheduled_start_time_utc"]]
    predictions=predictions.merge(identity_part,on=["game_id","goalie_id"],validate="many_to_one")
    predictions.insert(0,"run_id",run_id);predictions["prediction_semantics"]=identity["operational_amendment"]["semantic_contract"]
    predictions["export_status"]="SHADOW_PREDICTION_EXPORT";predictions["market_qualified"]=False
    selected,support=policy_c_selection(quotes,goalies);view=_market_view(quotes,selected,run_id)
    qualified=predictions.merge(view,on=["run_id","game_id","goalie_id","line"],how="inner",suffixes=("","_market"))
    if not qualified.empty:
        check=predictions.merge(qualified[["game_id","goalie_id","line","prob_over"]],on=["game_id","goalie_id","line"],suffixes=("_p","_m"))
        if not np.array_equal(check.prob_over_p.to_numpy(),check.prob_over_m.to_numpy()): raise RuntimeError("POST_GATE_PROBABILITY_MUTATION")
        keys=pd.MultiIndex.from_frame(qualified[["game_id","goalie_id","line"]]);pkeys=pd.MultiIndex.from_frame(predictions[["game_id","goalie_id","line"]]);predictions.loc[pkeys.isin(keys),"market_qualified"]=True
    staging.mkdir(parents=True,exist_ok=False)
    for src,name in [(game_spine_csv,"canonical_game_spine.csv"),(goalie_inputs_csv,"source_goalie_inputs.csv")]: (staging/name).write_bytes(src.read_bytes())
    amended=goalies.copy();amended["start_prob"]=1.0;amended.to_csv(staging/"operational_goalie_inputs.csv",index=False)
    predictions.to_csv(staging/"complete_prediction_population.csv",index=False);qualified.to_csv(staging/"market_qualified_population.csv",index=False)
    quotes.to_csv(staging/"book_level_quote_evidence.csv",index=False);view.to_csv(staging/"derived_market_view.csv",index=False)
    selected.to_csv(staging/"policy_c_team_game_decisions.csv",index=False);support.to_csv(staging/"goalie_book_support.csv",index=False)
    for name in ["candidate_population.csv","upload_population.csv","execution_population.csv"]: pd.DataFrame(columns=["run_id","reason"]).to_csv(staging/name,index=False)
    sentinel={"schema_version":"nhl_saves_shadow_sentinel_v1","status":"PASS","checks":{"parent_hashes":"PASS","population_before_market_gate":"PASS","quote_coverage":"VISIBLE","multiple_goalies":"VISIBLE_IN_POLICY_LEDGER","multibook":"FAIL_CLOSED_BY_POLICY_C","alias_and_timing":"FAIL_CLOSED_BY_CAPTURE","batch_preprocessing_drift":"PASS_FULL_POPULATION","start_prob_constant_one":"PASS","actual_starter_leakage":"PASS","mutable_dependency":"PASS","season_slate_game_type":"PASS"},"population":{"goalies":len(goalies),"predictions_P":len(predictions),"market_rows_M":len(qualified),"team_games_selected":int(selected.decision.eq("SELECTED").sum())}}
    (staging/"saves_live_failure_sentinel.json").write_text(json.dumps(sentinel,indent=2,sort_keys=True)+"\n")
    metadata={"schema_version":"nhl_saves_shadow_run_v1","run_id":run_id,"canonical_season":2026,"slate_date":slate_date,"run_type":run_type,"run_timestamp_utc":iso(parse_utc(run_timestamp_utc)),
              "scoring_order":["complete_scorer_eligible_population","identity_validation","constant_start_prob_1","complete_batch_preprocessing","score_all_predictions","policy_c_market_gate"],
              "prediction_semantics":identity["operational_amendment"]["semantic_contract"],"starter_state_label":STARTER_LABEL,"policy_status":POLICY_STATUS,
              "P":len(predictions),"M":len(qualified),"C":0,"U":0,"E":0,"G":0,"historical_parity":parity,"operational_amendment":amendment,
              "model_artifact_sha256":identity["model_artifact_sha256"],"coefficient_sha256":identity["coefficient_sha256"],"operational_wrapper_sha256":sha256_file(Path(__file__)),"quote_run_id":qmeta["run_id"],"quote_run_manifest_sha256":sha256_file(quote_run_dir/"SHA256SUMS")}
    (staging/"run_metadata.json").write_text(json.dumps(metadata,indent=2,sort_keys=True)+"\n");(staging/"RUN_COMPLETE.json").write_text(json.dumps({"run_id":run_id,"status":"COMPLETE"},sort_keys=True)+"\n")
    write_manifest(staging,complete_only=True);staging.rename(dest);return dest


def grade_shadow(*, shadow_run_dir: Path, outcomes_csv: Path, output_root: Path, correction_reason: str|None=None) -> Path:
    _verify_manifest(shadow_run_dir);meta=json.loads((shadow_run_dir/"run_metadata.json").read_text());pred=pd.read_csv(shadow_run_dir/"market_qualified_population.csv");out=pd.read_csv(outcomes_csv)
    need={"canonical_season","slate_date","game_id","goalie_id","actual_start_flag","goalie_participation_state","official_saves","outcome_source_timestamp_utc"}
    if need-set(out): raise ValueError("SAVES_OUTCOME_SCHEMA_INCOMPLETE")
    allowed={"STARTED","RELIEF_APPEARANCE","DRESSED_DID_NOT_PARTICIPATE","SCRATCHED_INACTIVE","UNRESOLVED","POSTPONED"}
    if not set(out.goalie_participation_state).issubset(allowed): raise RuntimeError("UNKNOWN_GOALIE_PARTICIPATION_STATE")
    if out.duplicated(["game_id","goalie_id"]).any() or not out.canonical_season.eq(2026).all() or not out.slate_date.astype(str).eq(meta["slate_date"]).all(): raise RuntimeError("OUTCOME_IDENTITY_CONFLICT")
    started=out.goalie_participation_state.eq("STARTED");flags=out.actual_start_flag.map(lambda x:str(x).lower() in {"true","1"})
    if not flags.eq(started).all(): raise RuntimeError("ACTUAL_START_FLAG_PARTICIPATION_CONFLICT")
    merged=pred.merge(out,on=["game_id","goalie_id"],how="left",validate="many_to_one")
    known=merged.outcome_source_timestamp_utc.notna()
    if (pd.to_datetime(merged.loc[known,"outcome_source_timestamp_utc"],utc=True,errors="coerce")<=pd.to_datetime(merged.loc[known,"scheduled_start_time_utc"],utc=True,errors="coerce")).any(): raise RuntimeError("OUTCOME_NOT_POSTGAME")
    def classify(r):
        if pd.isna(r.goalie_participation_state): return "UNRESOLVED"
        if int(r.game_type_code)==1:return "PRESEASON_NON_EVALUATION"
        if int(r.game_type_code)!=2:return "NON_REGULAR_SEASON_NON_EVALUATION"
        if r.goalie_participation_state=="STARTED" and pd.notna(r.official_saves):return "REGULAR_SEASON_CONDITIONAL_OBSERVED"
        return "NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION" if r.goalie_participation_state not in {"UNRESOLVED","POSTPONED"} else r.goalie_participation_state
    merged["grading_status"]=merged.apply(classify,axis=1);numeric=merged.grading_status.eq("REGULAR_SEASON_CONDITIONAL_OBSERVED")
    merged["observed_over"]=np.where(numeric,pd.to_numeric(merged.official_saves)>merged.line,np.nan);merged["regular_season_evaluation_target"]=np.where(numeric,merged.observed_over,np.nan)
    revision=digest({"run":meta["run_id"],"outcomes":sha256_file(outcomes_csv),"reason":correction_reason or "INITIAL"})[:20];dest=output_root/meta["slate_date"]/meta["run_id"]/revision
    if dest.exists():raise FileExistsError("GRADE_REVISION_OVERWRITE_BLOCKED")
    dest.mkdir(parents=True);merged.to_csv(dest/"shadow_grades.csv",index=False)
    summary={"schema_version":"nhl_saves_shadow_grading_v1","shadow_run_id":meta["run_id"],"revision_id":revision,"correction_reason":correction_reason,"rows":len(merged),"conditional_evaluation_rows":int(numeric.sum()),"preseason_numeric_targets":int(merged.loc[merged.game_type_code.eq(1),"regular_season_evaluation_target"].notna().sum()),"entered_regular_season_feature_history_rows":0,"nonstarter_rows":int(merged.grading_status.eq("NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION").sum())}
    (dest/"grade_metadata.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n");(dest/"RUN_COMPLETE.json").write_text(json.dumps({"status":"COMPLETE","revision_id":revision},sort_keys=True)+"\n");write_manifest(dest,complete_only=True);return dest
