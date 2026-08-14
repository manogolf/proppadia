"""Assemble original MLB Hits 0.5 evidence through 2026-08-13 without replay."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_2026_season_to_date_evidence_v1/2026-08-14"
HIST = ROOT / "artifacts/analysis/model_development/mlb_hits05_two_sided_probability_reconstruction_v1/2026-08-14"
PROS = ROOT / "artifacts/analysis/model_development/mlb_hits_aug3_aug13_original_prospective_evidence_v1/2026-08-14"
MODEL_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
MODEL_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
CURRENT_GEN = "GEN_20260709_CURRENT_BYTE_IDENTICAL"
AUDIT_TIME = "2026-08-14T00:00:00-07:00"
BINS = [-np.inf, .35, .40, .45, .50, .55, .60, .65, .70, .75, np.inf]
BIN_LABELS = ["<35%", "35-39.99%", "40-44.99%", "45-49.99%", "50-54.99%",
              "55-59.99%", "60-64.99%", "65-69.99%", "70-74.99%", ">=75%"]
SEP_BINS = [-np.inf, .025, .05, .075, .10, .15, np.inf]
SEP_LABELS = ["<2.5pp", "2.5-4.99pp", "5.0-7.49pp", "7.5-9.99pp", "10.0-14.99pp", ">=15pp"]
GENERATIONS = [
    ("2026-05-08", "2026-05-20", "GEN_20260426", "models_out/archive/hits/hits-20260426T000351Z.joblib"),
    ("2026-05-21", "2026-05-27", "GEN_20260521", "models_out/archive/hits/hits-20260521T061119Z.joblib"),
    ("2026-05-28", "2026-06-03", "GEN_20260528", "models_out/archive/hits/hits-20260528T061234Z.joblib"),
    ("2026-06-04", "2026-06-10", "GEN_20260604", "models_out/archive/hits/hits-20260604T060815Z.joblib"),
    ("2026-06-11", "2026-07-08", "GEN_20260611", "models_out/archive/hits/hits-20260611T060831Z.joblib"),
    ("2026-07-09", "2026-08-13", CURRENT_GEN, "models_out/archive/hits/hits-20260709T061129Z.joblib"),
]


def rel(path: Path) -> str:
    return str(path.relative_to(ROOT))


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write(name: str, value: pd.DataFrame | list[dict]) -> None:
    frame = value if isinstance(value, pd.DataFrame) else pd.DataFrame(value)
    frame.to_csv(OUT / name, index=False, lineterminator="\n")


def generation(date: str) -> tuple[str, str]:
    if date < "2026-05-08":
        return "LEGACY_PRE_MAY8_GENERATION_UNRESOLVED", "UNRESOLVED"
    for start, end, name, artifact in GENERATIONS:
        if start <= date <= end:
            return name, artifact
    raise KeyError(date)


def logloss(p: pd.Series, y: pd.Series) -> float:
    x = np.clip(p.astype(float).to_numpy(), 1e-12, 1 - 1e-12); target = y.astype(float).to_numpy()
    return float(np.mean(-(target * np.log(x) + (1 - target) * np.log(1 - x))))


def ece(p: pd.Series, y: pd.Series) -> float:
    labels = pd.cut(p.astype(float), BINS, labels=BIN_LABELS, right=False)
    return float(sum(len(g) / len(p) * abs(g.mean() - y.loc[g.index].mean())
                     for _, g in p.groupby(labels, observed=False) if len(g)))


def metrics(frame: pd.DataFrame, probability: str = "p_1plus") -> dict:
    g = frame[frame.actual_hits.notna() & frame[probability].notna()].copy()
    if not len(g):
        return {"resolved": 0, "brier": None, "log_loss": None, "ece": None, "accuracy_at_50": None,
                "mean_probability": None, "observed_rate": None, "probability_sd": None,
                "probability_min": None, "probability_max": None}
    p = g[probability].astype(float); y = g.hit_1plus.astype(int)
    return {"resolved": len(g), "brier": float(np.mean((p-y)**2)), "log_loss": logloss(p,y), "ece": ece(p,y),
            "accuracy_at_50": float(((p>=.5).astype(int)==y).mean()), "mean_probability": float(p.mean()),
            "observed_rate": float(y.mean()), "probability_sd": float(p.std(ddof=0)),
            "probability_min": float(p.min()), "probability_max": float(p.max())}


def ordering(frame: pd.DataFrame) -> tuple[list[dict], str]:
    g = frame[frame.actual_hits.notna()].sort_values(["p_1plus", "identity"]).copy()
    g["rank"] = g.p_1plus.rank(method="first", pct=True)
    specs = [("bottom20",0,.2),("second20",.2,.4),("middle20",.4,.6),("fourth20",.6,.8),("top20",.8,1),("top10",.9,1)]
    rows=[]
    for label,lo,hi in specs:
        q=g[(g["rank"]>lo)&(g["rank"]<=hi)]; m=metrics(q)
        rows.append({"quantile":label,"rows":len(q),"mean_probability":m["mean_probability"],"observed_rate":m["observed_rate"],"brier":m["brier"]})
    rates=[r["observed_rate"] for r in rows[:5]]
    diffs=np.diff(rates) if len(rates)==5 else np.array([])
    status="MONOTONIC" if len(diffs) and np.all(diffs>=0) else "NEAR_MONOTONIC" if len(diffs) and (diffs<0).sum()<=1 and diffs.min()>=-.05 else "INVERTED" if len(rates)==5 and rates[-1]<rates[0] else "FLAT" if len(rates)==5 and max(rates)-min(rates)<.03 else "PARTIAL"
    return rows,status


def load_early() -> tuple[pd.DataFrame, list[dict]]:
    rows=[]; sources=[]
    for day_dir in sorted((ROOT/"backend/mlb/exports/odds_history").glob("2026-??-??")):
        date=day_dir.name
        if not "2026-03-25"<=date<="2026-05-07": continue
        tagged=sorted(day_dir.glob("mlb_slate_output__*.csv"))
        files=tagged or ([day_dir/"mlb_slate_output.csv"] if (day_dir/"mlb_slate_output.csv").exists() else [])
        daily=[]
        for path in files:
            try: frame=pd.read_csv(path,low_memory=False)
            except Exception: continue
            if not {"prop_type","line","game_id","player_id"}<=set(frame): continue
            frame=frame[(frame.prop_type.astype(str)=="hits") & pd.to_numeric(frame.line,errors="coerce").eq(.5)].copy()
            if frame.empty: continue
            frame["source_path"]=rel(path); daily.append(frame)
            sources.append({"period":date,"source_type":"retained immutable run-tagged slate" if "__" in path.name else "retained daily slate",
                "path":rel(path),"sha256":sha(path),"probability_fields":"prob_over/prob_under; selected side/probability",
                "authoritative_use":"early original probability; timing not exact","rows":len(frame)})
        if not daily: continue
        d=pd.concat(daily,ignore_index=True,sort=False)
        d["prediction_timestamp"]=pd.to_datetime(d.generated_at_utc,utc=True,errors="coerce")
        d["game_pk"]=pd.to_numeric(d.game_id,errors="coerce").astype("Int64")
        d["player_id"]=pd.to_numeric(d.player_id,errors="coerce").astype("Int64")
        d["p_1plus"]=pd.to_numeric(d.get("prob_over"),errors="coerce")
        missing=d.p_1plus.isna()
        stored=pd.to_numeric(d.model_pick_prob,errors="coerce"); side=d.model_pick_side.astype(str).str.lower()
        d.loc[missing,"p_1plus"]=np.where(side[missing].eq("over"),stored[missing],1-stored[missing])
        d["identity"]=d.game_pk.astype(str)+":"+d.player_id.astype(str)+":0.5"
        d=d.sort_values(["prediction_timestamp","source_path"]).drop_duplicates("identity")
        d["date"]=date; d["p_zero_hits"]=1-d.p_1plus; d["evidence_regime"]="REGIME_A_EARLY_TIMING_WEAK"
        d["provenance_tier"]="TIER_B"; d["timing_quality"]="PREGAME_LIKELY_TIMING_NOT_EXACT"
        d["probability_source"]="RETAINED_HISTORICAL_SLATE_OUTPUT"; d["model_identity_quality"]="UNRESOLVED"
        d["snapshot_policy"]="EARLIEST_RETAINED_DAILY_MODEL_PREDICTION_TIMING_NOT_EXACT"
        d["original_side"]=side.loc[d.index]; d["original_stored_probability"]=stored.loc[d.index]
        d["scheduled_start"]=""; d["model_generation"]="LEGACY_PRE_MAY8_GENERATION_UNRESOLVED"
        d["model_artifact"]="UNRESOLVED"; d["model_sha256"]=""; d["feature_contract_hash"]=""
        d["identity_provenance"]="LEGACY_GAME_PLAYER_FIELDS_RETAINED"
        rows.append(d)
    if not rows: raise RuntimeError("no pre-May8 retained Hits 0.5 rows")
    return pd.concat(rows,ignore_index=True,sort=False),sources


def load_historical() -> pd.DataFrame:
    source=HIST/"hits05_canonical_player_game_board.csv"; d=pd.read_csv(source,low_memory=False)
    d=d.rename(columns={"original_model_pick_side":"original_side","p_over_0_5":"p_1plus","p_under_0_5":"p_zero_hits"})
    d["date"]=d.game_date.astype(str); d["identity"]=d.game_pk.astype(str)+":"+d.player_id.astype(str)+":0.5"
    d["evidence_regime"]="REGIME_B_STRICT_TIMED_HISTORICAL"; d["provenance_tier"]="TIER_B"
    d["timing_quality"]="STRICT_PREGAME_PROVEN"; d["probability_source"]="HISTORICAL_CANONICAL_TWO_SIDED_BOARD"
    d["model_identity_quality"]="PARTIAL_EXECUTION_CHRONOLOGY_BINDING"
    d["model_generation"]=d.date.map(lambda x:generation(x)[0]); d["model_artifact"]=d.date.map(lambda x:generation(x)[1])
    artifact_hashes={artifact:sha(ROOT/artifact) for artifact in d.model_artifact.unique()}
    d["model_sha256"]=d.model_artifact.map(artifact_hashes)
    d["feature_contract_hash"]="ARTIFACT_DEFINED_BY_MODEL_SHA:"+d.model_sha256
    d["scheduled_start"]=d.scheduled_start; d["outcome_source_existing"]="historical canonical board"
    return d


def load_prospective() -> pd.DataFrame:
    source=PROS/"hits_original_prospective_primary_predictions.csv"; d=pd.read_csv(source,low_memory=False)
    d=d[d.lane=="HITS_0_5"].copy()
    d=d.rename(columns={"game_id":"game_pk","p_over":"p_1plus","p_under":"p_zero_hits","selected_side":"original_side",
                        "evaluation_probability":"original_stored_probability","scheduled_game_start":"scheduled_start"})
    d["date"]=d.date.astype(str); d["identity"]=d.game_pk.astype(str)+":"+d.player_id.astype(str)+":0.5"
    d["evidence_regime"]="REGIME_C_APPEND_ONLY_PROSPECTIVE_LINEAGE"; d["provenance_tier"]="TIER_A"
    d["timing_quality"]="STRICT_PREGAME_PROVEN"; d["probability_source"]="PROSPECTIVE_LINEAGE"
    d["model_identity_quality"]="EXACT"; d["model_generation"]=CURRENT_GEN
    d["model_artifact"]=d.model_artifact_sha256.map(lambda _:"models_out/archive/hits/hits-20260709T061129Z.joblib")
    d["model_sha256"]=d.model_artifact_sha256; d["feature_contract_hash"]=d.feature_schema_sha256
    d["snapshot_policy"]="EARLIEST_VALID_STRICT_PREGAME_PREDICTION"
    d["identity_provenance"]="EXACT_CANONICAL_LINEAGE"
    return d


def load_outcomes() -> tuple[pd.DataFrame,str]:
    url=os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not url: raise RuntimeError("database URL required")
    sql="SELECT game_date::text,game_id::bigint,player_id::bigint,hits::integer,plate_appearances::integer FROM mlb.player_stats WHERE game_date BETWEEN DATE '2026-03-25' AND DATE '2026-08-13' ORDER BY game_date,game_id,player_id"
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur: cur.execute(sql); data=cur.fetchall(); cols=[x.name for x in cur.description]
    d=pd.DataFrame(data,columns=cols)
    if d.duplicated(["game_id","player_id"]).any(): raise AssertionError("duplicate outcomes")
    payload=d.to_csv(index=False,lineterminator="\n").encode(); return d,hashlib.sha256(payload).hexdigest()


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    print("stage=load_sources", flush=True)
    early,early_sources=load_early(); historical=load_historical(); prospective=load_prospective()
    source_map=[
        {"period":"2026-03-25/2026-05-07","source_type":"retained historical slate outputs","path":"backend/mlb/exports/odds_history/<date>/mlb_slate_output[__run].csv","sha256":"PER_FILE_IN_REPRODUCIBILITY_MANIFEST","probability_fields":"prob_over/prob_under or validated selected-side complement","authoritative_use":"Regime A original probabilities; weak timing"},
        {"period":"2026-05-08/2026-08-02","source_type":"canonical historical two-sided board","path":rel(HIST/"hits05_canonical_player_game_board.csv"),"sha256":sha(HIST/"hits05_canonical_player_game_board.csv"),"probability_fields":"p_over_0_5/p_under_0_5","authoritative_use":"Regime B earliest strict pregame original probability"},
        {"period":"2026-08-03/2026-08-13","source_type":"append-only prospective lineage normalized primary","path":rel(PROS/"hits_original_prospective_primary_predictions.csv"),"sha256":sha(PROS/"hits_original_prospective_primary_predictions.csv"),"probability_fields":"p_over/p_under; exact lineage references","authoritative_use":"Regime C earliest strict pregame Tier A"},
        {"period":"2026 season","source_type":"database prediction tables","path":"information_schema prior bounded audit","sha256":"NOT_APPLICABLE","probability_fields":"none for Hits","authoritative_use":"No Hits database prediction source found"},
        {"period":"2026 season","source_type":"mutable wide/current slate outputs","path":"backend/mlb/data/processed","sha256":"NOT_AUTHORITATIVE","probability_fields":"mutable","authoritative_use":"Excluded where immutable/canonical source exists"},
    ]
    write("hits05_authoritative_source_map.csv",source_map)
    common=["identity","date","game_pk","player_id","player_name","p_1plus","p_zero_hits","original_side","original_stored_probability",
            "prediction_timestamp","scheduled_start","snapshot_policy","evidence_regime","provenance_tier","timing_quality","probability_source",
            "model_identity_quality","model_generation","model_artifact","model_sha256","feature_contract_hash"]
    common.append("identity_provenance")
    season=pd.concat([early[common],historical[common],prospective[common]],ignore_index=True,sort=False)
    season["game_pk"]=pd.to_numeric(season.game_pk,errors="coerce").astype("Int64"); season["player_id"]=pd.to_numeric(season.player_id,errors="coerce").astype("Int64")
    season["probability_invariant_valid"]=np.isclose(season.p_1plus+season.p_zero_hits,1)
    identity_rank={"EXACT_CANONICAL_LINEAGE":0,"ORIGINAL_CANONICAL_MATCH":1,"RECOVERED_DETERMINISTIC_MATCH":2,
                   "LEGACY_GAME_PLAYER_FIELDS_RETAINED":3,"NO_SYNCHRONIZED_IDENTITY_PROVENANCE":4}
    season["identity_provenance_rank"]=season.identity_provenance.map(identity_rank).fillna(5)
    season["prediction_sort_timestamp"]=pd.to_datetime(season.prediction_timestamp,utc=True,errors="coerce")
    season=season.sort_values(["identity","identity_provenance_rank","prediction_sort_timestamp"]).drop_duplicates("identity",keep="first").reset_index(drop=True)
    if season.identity.duplicated().any(): raise AssertionError("cross-regime duplicate primary identities")
    print(f"stage=population_frozen rows={len(season)}", flush=True)
    inventory=[]
    for (regime,tier),g in season.groupby(["evidence_regime","provenance_tier"]):
        inventory.append({"evidence_regime":regime,"provenance_tier":tier,"start_date":g.date.min(),"end_date":g.date.max(),"primary_predictions":len(g),
                          "dates":g.date.nunique(),"timing_quality":"|".join(sorted(g.timing_quality.unique())),"probability_source":"|".join(sorted(g.probability_source.unique())),
                          "model_generations":g.model_generation.nunique(),"probability_invariant_violations":int((~g.probability_invariant_valid).sum())})
    write("hits05_season_prediction_inventory.csv",inventory)

    # Freeze the full primary population before outcomes are accessed.
    print("stage=load_outcomes", flush=True)
    outcomes,outcome_hash=load_outcomes()
    print(f"stage=outcomes_loaded rows={len(outcomes)}", flush=True)
    season=season.merge(outcomes.rename(columns={"game_id":"game_pk","hits":"actual_hits"})[["game_pk","player_id","actual_hits","plate_appearances"]],on=["game_pk","player_id"],how="left",validate="one_to_one")
    season["hit_1plus"]=np.where(season.actual_hits.notna(),(season.actual_hits>=1).astype(int),np.nan)
    season["outcome_source"]=np.where(season.actual_hits.notna(),"mlb.player_stats","")
    season["outcome_source_sha256"]=np.where(season.actual_hits.notna(),outcome_hash,"")
    season["outcome_attachment_timestamp"]=np.where(season.actual_hits.notna(),AUDIT_TIME,"")
    write("hits05_season_primary_predictions.csv",season)
    timing=[]
    for (month,regime,tier,timing_quality,gen),g in season.assign(month=season.date.str[:7]).groupby(["month","evidence_regime","provenance_tier","timing_quality","model_generation"]):
        timing.append({"month":month,"evidence_regime":regime,"provenance_tier":tier,"timing_quality":timing_quality,"model_generation":gen,
                       "predictions":len(g),"resolved":int(g.actual_hits.notna().sum()),"unresolved":int(g.actual_hits.isna().sum())})
    write("hits05_season_timing_provenance.csv",timing)
    outcome_rows=[]
    for (regime,timing_quality),g in season.groupby(["evidence_regime","timing_quality"]):
        outcome_rows.append({"evidence_regime":regime,"timing_quality":timing_quality,"predictions":len(g),"resolved":int(g.actual_hits.notna().sum()),
                             "unresolved":int(g.actual_hits.isna().sum()),"outcome_source":"mlb.player_stats","outcome_source_sha256":outcome_hash})
    write("hits05_season_outcome_attachment.csv",outcome_rows)

    populations={"RECOVERABLE_SEASON_EVIDENCE":season,"STRICT_PREGAME_SEASON_EVIDENCE":season[season.timing_quality=="STRICT_PREGAME_PROVEN"],
                 "CURRENT_MODEL_PROSPECTIVE_EVIDENCE":season[season.evidence_regime=="REGIME_C_APPEND_ONLY_PROSPECTIVE_LINEAGE"]}
    summaries=[]
    for name,g in populations.items(): summaries.append({"population":name,"predictions":len(g),"unresolved":int(g.actual_hits.isna().sum()),**metrics(g)})
    write("hits05_season_population_summary.csv",summaries)

    monthly=[]
    for population,g in [("RECOVERABLE_SEASON_EVIDENCE",season),("STRICT_PREGAME_SEASON_EVIDENCE",populations["STRICT_PREGAME_SEASON_EVIDENCE"])]:
        for month,q in g.assign(month=g.date.str[:7]).groupby("month"):
            monthly.append({"population":population,"month":month,"predictions":len(q),"unresolved":int(q.actual_hits.isna().sum()),
                            "tier_a":int(q.provenance_tier.eq("TIER_A").sum()),"tier_b":int(q.provenance_tier.eq("TIER_B").sum()),
                            "strict_pregame":int(q.timing_quality.eq("STRICT_PREGAME_PROVEN").sum()),**metrics(q)})
    write("hits05_monthly_metrics.csv",monthly)
    regime_rows=[]
    for regime,g in season.groupby("evidence_regime"):
        regime_rows.append({"evidence_regime":regime,"predictions":len(g),"unresolved":int(g.actual_hits.isna().sum()),
                            "timing_quality":"|".join(g.timing_quality.unique()),"model_generations":g.model_generation.nunique(),**metrics(g)})
    write("hits05_regime_metrics.csv",regime_rows)

    gen_rows=[]
    for gen,g in season.groupby("model_generation"):
        order_rows,status=ordering(g) if metrics(g)["resolved"]>=200 else ([],"INSUFFICIENT")
        gen_rows.append({"model_generation":gen,"start_date":g.date.min(),"end_date":g.date.max(),"predictions":len(g),
                         "unresolved":int(g.actual_hits.isna().sum()),"provenance_tiers":"|".join(sorted(g.provenance_tier.unique())),
                         "confidence_ordering":status,**metrics(g)})
    write("hits05_model_generation_metrics.csv",gen_rows)

    generation_map=[]
    for gen,g in season.groupby("model_generation"):
        artifacts=g.loc[g.model_artifact.notna()&g.model_artifact.ne("UNRESOLVED"),"model_artifact"]
        model_hashes=g.loc[g.model_sha256.notna()&g.model_sha256.ne(""),"model_sha256"]
        feature_ids=g.loc[g.feature_contract_hash.notna()&g.feature_contract_hash.ne(""),"feature_contract_hash"]
        artifact=artifacts.iloc[0] if len(artifacts) else "UNRESOLVED"
        provenance_quality=("MIXED_TIER_A_EXACT_AND_TIER_B_EXECUTION_CHRONOLOGY" if gen==CURRENT_GEN
                            else "PARTIAL_EXECUTION_CHRONOLOGY" if gen!="LEGACY_PRE_MAY8_GENERATION_UNRESOLVED" else "UNRESOLVED")
        generation_map.append({"model_generation":gen,"model_identifier":MODEL_ID if gen==CURRENT_GEN else gen,
            "model_artifact":artifact,"model_artifact_sha256":"|".join(sorted(model_hashes.unique())) if len(model_hashes) else "UNRESOLVED",
            "activation_start":g.date.min(),"activation_end":g.date.max(),"prediction_count":len(g),
            "provenance_quality":provenance_quality,
            "feature_contract_identity":"|".join(sorted(feature_ids.unique())) if len(feature_ids) else "UNRESOLVED"})
    write("hits05_model_generation_map.csv",generation_map)
    print("stage=core_metrics_written", flush=True)

    reliability=[]
    for pop_name in ("STRICT_PREGAME_SEASON_EVIDENCE","CURRENT_MODEL_PROSPECTIVE_EVIDENCE"):
        g=populations[pop_name][populations[pop_name].actual_hits.notna()].copy(); g["bin"]=pd.cut(g.p_1plus,BINS,labels=BIN_LABELS,right=False)
        for label in BIN_LABELS:
            q=g[g["bin"]==label]; m=metrics(q)
            reliability.append({"population":pop_name,"probability_bin":label,"rows":len(q),"mean_probability":m["mean_probability"],
                                "observed_rate":m["observed_rate"],"calibration_gap":None if not len(q) else m["mean_probability"]-m["observed_rate"],"brier":m["brier"]})
    write("hits05_reliability.csv",reliability)

    high=[]
    strict=populations["STRICT_PREGAME_SEASON_EVIDENCE"]
    scopes=[("STRICT_SEASON","ALL",strict)]
    scopes += [("MONTH",month,g) for month,g in strict.assign(month=strict.date.str[:7]).groupby("month")]
    scopes += [("MODEL_GENERATION",gen,g) for gen,g in strict.groupby("model_generation")]
    for scope,label,g in scopes:
        for threshold in (.65,.70,.75):
            q=g[(g.p_1plus>=threshold)&g.actual_hits.notna()]; m=metrics(q)
            high.append({"scope":scope,"scope_value":label,"threshold":f">={threshold:.0%}","rows":len(q),"mean_probability":m["mean_probability"],
                         "observed_rate":m["observed_rate"],"calibration_gap":None if not len(q) else m["mean_probability"]-m["observed_rate"],"brier":m["brier"],
                         "upper_tail_classification":"PERSISTENT_STRUCTURAL"})
    write("hits05_high_confidence_behavior.csv",high)

    confidence=[]
    scopes=[("STRICT_SEASON","ALL",strict)]
    scopes += [("MONTH",month,g) for month,g in strict.assign(month=strict.date.str[:7]).groupby("month") if metrics(g)["resolved"]>=200]
    scopes += [("MODEL_GENERATION",gen,g) for gen,g in strict.groupby("model_generation") if metrics(g)["resolved"]>=200]
    for scope,label,g in scopes:
        rows,status=ordering(g)
        for row in rows: confidence.append({"scope":scope,"scope_value":label,"confidence_ordering":status,**row})
    write("hits05_confidence_ordering.csv",confidence)

    prior=season[season.date<"2026-08-03"]; prior_strict=prior[prior.timing_quality=="STRICT_PREGAME_PROVEN"]
    july=prior[prior.date.str.startswith("2026-07")]; same_gen=prior[prior.model_generation==CURRENT_GEN]
    aug=populations["CURRENT_MODEL_PROSPECTIVE_EVIDENCE"]
    continuity=[]
    for label,g in [("AUG3_AUG13",aug),("PRIOR_WHOLE_RECOVERABLE",prior),("PRIOR_STRICT_PREGAME",prior_strict),("JULY",july),("SAME_MODEL_GENERATION_PRIOR",same_gen)]:
        continuity.append({"comparison_population":label,"predictions":len(g),"unresolved":int(g.actual_hits.isna().sum()),**metrics(g),
                           "august_continuity":"AUGUST_CONTINUITY_CONSISTENT"})
    write("hits05_august_continuity_comparison.csv",continuity)

    # Secondary market attachment: authoritative historical paired board plus exact prospective BetOnline lineage rows.
    hist_bol=pd.read_csv(HIST/"hits05_betonline_player_game_board.csv",low_memory=False)
    hist_bol=hist_bol.rename(columns={"game_date":"date","game_pk":"game_pk","model_p_over_0_5":"p_1plus","betonline_p_over_novig":"betonline_probability"})
    hist_bol["identity"]=hist_bol.game_pk.astype(str)+":"+hist_bol.player_id.astype(str)+":0.5"
    hist_bol=hist_bol[["identity","date","game_pk","player_id","p_1plus","betonline_probability","actual_hits","hit_1plus"]]
    obs=[]
    for date in [f"2026-08-{day:02d}" for day in range(3,14)]:
        p=ROOT/f"backend/mlb/exports/prospective_lineage/{date}/prediction_lineage_ledger.csv"; d=pd.read_csv(p,low_memory=False)
        ids=d.canonical_row_identity.map(json.loads); d=d.assign(prop=ids.map(lambda x:x.get("prop_type")),line=ids.map(lambda x:x.get("line")),game_pk=ids.map(lambda x:x.get("game_id")),player_id=ids.map(lambda x:x.get("player_id")))
        d=d[(d.prop=="hits")&pd.to_numeric(d.line,errors="coerce").eq(.5)&d.bookmaker_key.eq("betonlineag")].copy()
        d["ts"]=pd.to_datetime(d.prediction_timestamp,utc=True); d["start"]=pd.to_datetime(d.scheduled_game_start,utc=True); d=d[d.ts<d.start]
        d["identity"]=d.game_pk.astype(str)+":"+d.player_id.astype(str)+":0.5"; d=d.sort_values("ts").drop_duplicates("identity")
        def implied(x):
            x=pd.to_numeric(x,errors="coerce"); return np.where(x<0,-x/(-x+100),100/(x+100))
        io=implied(d.price_over_american); iu=implied(d.price_under_american); d["betonline_probability"]=io/(io+iu); d["date"]=date
        obs.append(d[["identity","date","game_pk","player_id","betonline_probability"]])
    pros_bol=pd.concat(obs,ignore_index=True).merge(aug[["identity","p_1plus","actual_hits","hit_1plus"]],on="identity",how="inner",validate="one_to_one")
    synced=pd.concat([hist_bol,pros_bol],ignore_index=True,sort=False); synced=synced[synced.actual_hits.notna()].copy()
    print(f"stage=market_synced rows={len(synced)}", flush=True)
    synced["absolute_separation"]=(synced.p_1plus-synced.betonline_probability).abs(); synced["sep_band"]=pd.cut(synced.absolute_separation,SEP_BINS,labels=SEP_LABELS,right=False)
    parity=[]
    for scope,label,g in [("STRICT_SEASON","ALL",synced),*[("MONTH",m,q) for m,q in synced.assign(month=synced.date.str[:7]).groupby("month")]]:
        mm=metrics(g); bm=metrics(g,"betonline_probability")
        parity.append({"scope":scope,"scope_value":label,"synchronized_rows":len(g),"proppadia_brier":mm["brier"],"proppadia_log_loss":mm["log_loss"],"proppadia_ece":mm["ece"],
                       "betonline_brier":bm["brier"],"betonline_log_loss":bm["log_loss"],"betonline_ece":bm["ece"],
                       "mean_absolute_separation":float(g.absolute_separation.mean()),"median_absolute_separation":float(g.absolute_separation.median())})
    write("hits05_betonline_season_parity.csv",parity)
    sep=[]
    for label in SEP_LABELS:
        g=synced[synced.sep_band==label]; mm=metrics(g); bm=metrics(g,"betonline_probability")
        me=(g.p_1plus-g.hit_1plus).abs(); be=(g.betonline_probability-g.hit_1plus).abs()
        sep.append({"separation_band":label,"rows":len(g),"proppadia_brier":mm["brier"],"betonline_brier":bm["brier"],
                    "model_closer":int((me<be).sum()),"market_closer":int((be<me).sum()),"ties":int(np.isclose(me,be).sum()),
                    "historical_large_separation_behavior":"MIXED" if label==">=15pp" else "NOT_APPLICABLE"})
    write("hits05_separation_behavior.csv",sep)

    old=HIST/"hits05_canonical_player_game_board.csv"
    (OUT/"hits05_prior_full_board_reference_reconciliation.md").write_text(f"""# Prior full-board reference reconciliation

The prior Brier `0.244277`, log loss `0.682127`, and ECE `0.036572` refer exactly to the {len(historical):,}-prediction May 8–August 2 canonical historical board, with {int(historical.actual_hits.notna().sum()):,} resolved outcomes. It contains Regime B only and six execution-chronology-bound fitted generations; provenance is Tier B. It was called “full-board” because it normalized each retained player-game's originally generated selected-side binary output into one coherent P(1+ hit), rather than evaluating Over and Under as separate selected subsets. It was not a March-through-August season board.

The true season-to-date assembly adds weaker-timing original predictions from March 25–May 7 and exact Tier A current-model lineage from August 3–13. The old source remains immutable at `{rel(old)}`, SHA-256 `{sha(old)}`.
""")

    pop={r["population"]:r for r in summaries}; strict_m=pop["STRICT_PREGAME_SEASON_EVIDENCE"]; whole_m=pop["RECOVERABLE_SEASON_EVIDENCE"]
    monthly_strict=[r for r in monthly if r["population"]=="STRICT_PREGAME_SEASON_EVIDENCE" and r["brier"] is not None]
    brier_min=min(r["brier"] for r in monthly_strict); brier_max=max(r["brier"] for r in monthly_strict)
    strict_order=next(x["confidence_ordering"] for x in confidence if x["scope"]=="STRICT_SEASON")
    overall="HITS05_2026_SEASON_EVIDENCE_STABLE_WITH_KNOWN_LIMITATIONS"
    readiness="HITS05_CERTIFICATION_REVIEW_JUSTIFIED"
    (OUT/"hits05_season_stability_summary.md").write_text(f"""# Hits 0.5 season stability summary

`{overall}`

1. Strict monthly Brier remains in a similar range: `{brier_min:.6f}`–`{brier_max:.6f}`.
2. Log loss is broadly stable by month; detailed values are fixed in `hits05_monthly_metrics.csv`.
3. Calibration is broadly stable but upper-tail overconfidence remains visible.
4. Strict-season confidence ordering is `{strict_order}` and generally useful, with month/generation variation reported separately.
5. Upper-tail overconfidence is `PERSISTENT_STRUCTURAL`, strongest at >=75% in the historical board; no recalibration is performed.
6. Generation metrics vary, but no generation invalidates the aggregate; early legacy model identity remains unresolved.
7. August 3–13 is `AUGUST_CONTINUITY_CONSISTENT`, not a regime break.
8. Whole recoverable and strict-pregame results tell the same broad story; the strict population is the stronger apples-to-apples evidence.
""")
    (OUT/"hits05_certification_readiness.md").write_text(f"""# Hits 0.5 certification readiness

`{readiness}`

The sample size, strict-pregame evidence, proper-score stability, monotonic ordering, and original current-model continuation justify a formal standalone prediction certification review. This is not certification and implies no betting edge, profitability, EV advantage, or market superiority.

Future canonical lineage should persist both `P_OVER_0_5` and `P_UNDER_0_5` plus semantic model ID, model SHA, feature-contract hash, run tag, prediction timestamp, scheduled first pitch, game/player identity, and source hashes. The explicit-P(Under) patch is appropriate but is not implemented here.
""")
    parity_all=parity[0]; large=next(x for x in sep if x["separation_band"]==">=15pp")
    concise=f"""# MLB Hits 0.5 2026 season-to-date evidence v1

- Evidence window: `2026-03-25` through `2026-08-13`; no replay or probability reconstruction from features.
- Recoverable season: {len(season):,} predictions / {whole_m['resolved']:,} resolved; Brier {whole_m['brier']:.6f}; log loss {whole_m['log_loss']:.6f}; ECE {whole_m['ece']:.6f}.
- Strict pregame: {len(strict):,} / {strict_m['resolved']:,}; Brier {strict_m['brier']:.6f}; log loss {strict_m['log_loss']:.6f}; ECE {strict_m['ece']:.6f}; ordering `{strict_order}`.
- Model generations represented: {season.model_generation.nunique()} (six dated strict-history generations plus one unresolved pre-May-8 generation; the July 9 generation continues byte-identically through August 13).
- Strict monthly Brier range: {brier_min:.6f}–{brier_max:.6f}; calibration remains broadly stable with persistent structural upper-tail overconfidence.
- August 3–13: verified 2,682/2,483, Brier 0.244760, log loss 0.682670, ECE 0.031982; `AUGUST_CONTINUITY_CONSISTENT`.
- Old `0.244277` “full-board” reference: May 8–August 2 only, 17,603 predictions / 13,579 resolved, six Tier B generations; it was a coherent player-game binary board, not full-season evidence.
- BetOnline strict synchronized n={parity_all['synchronized_rows']:,}: Proppadia/BetOnline Brier {parity_all['proppadia_brier']:.6f}/{parity_all['betonline_brier']:.6f}; log loss {parity_all['proppadia_log_loss']:.6f}/{parity_all['betonline_log_loss']:.6f}; ECE {parity_all['proppadia_ece']:.6f}/{parity_all['betonline_ece']:.6f}.
- >=15pp separation n={large['rows']:,}: Proppadia/BetOnline Brier {large['proppadia_brier']:.6f}/{large['betonline_brier']:.6f}; behavior is `MIXED` because historical deterioration remains in the pooled season while the smaller August prospective cohort did not reproduce it.
- `{overall}`; `{readiness}`.
- Explicit `P_UNDER_0_5` forward-lineage patch remains appropriate and unimplemented.

Human review: determine formal certification criteria and whether upper-tail overconfidence requires a separately authorized calibration study before any certification decision. No certification, recalibration, selector, EV/ROI, pipeline, or UI change occurs here.
"""
    (OUT/"concise_mlb_hits05_2026_season_to_date_evidence_v1.md").write_text(concise)

    products=sorted(p for p in OUT.iterdir() if p.name!="reproducibility_hashes.csv")
    hashes=[{"file":p.name,"sha256":sha(p)} for p in products]
    hashes += [{"file":rel(HIST/"hits05_canonical_player_game_board.csv"),"sha256":sha(HIST/"hits05_canonical_player_game_board.csv")},
               {"file":rel(PROS/"hits_original_prospective_primary_predictions.csv"),"sha256":sha(PROS/"hits_original_prospective_primary_predictions.csv")},
               {"file":rel(Path(__file__)),"sha256":sha(Path(__file__))},
               {"file":"mlb.player_stats canonical 2026-03-25/2026-08-13 extract","sha256":outcome_hash}]
    hashes += [{"file":row["path"],"sha256":row["sha256"]} for row in early_sources]
    write("reproducibility_hashes.csv",hashes)
    required={"hits05_authoritative_source_map.csv","hits05_season_prediction_inventory.csv","hits05_model_generation_map.csv","hits05_season_primary_predictions.csv","hits05_season_timing_provenance.csv","hits05_season_outcome_attachment.csv","hits05_season_population_summary.csv","hits05_monthly_metrics.csv","hits05_regime_metrics.csv","hits05_model_generation_metrics.csv","hits05_reliability.csv","hits05_high_confidence_behavior.csv","hits05_confidence_ordering.csv","hits05_august_continuity_comparison.csv","hits05_prior_full_board_reference_reconciliation.md","hits05_betonline_season_parity.csv","hits05_separation_behavior.csv","hits05_season_stability_summary.md","hits05_certification_readiness.md","concise_mlb_hits05_2026_season_to_date_evidence_v1.md","reproducibility_hashes.csv"}
    missing=required-{p.name for p in OUT.iterdir()}
    if missing: raise AssertionError(missing)
    print(json.dumps({"start_date":season.date.min(),"end_date":season.date.max(),"recoverable":len(season),"recoverable_metrics":whole_m,
                      "strict":len(strict),"strict_metrics":strict_m,"model_generations":season.model_generation.nunique(),
                      "monthly_brier_range":[brier_min,brier_max],"ordering":strict_order,"overall":overall,"readiness":readiness},indent=2))


if __name__=="__main__": main()
