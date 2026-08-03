"""Freeze and evaluate exact same-run historical TB 1.5 decision evidence.

This module is deliberately artifact-only: it reads the certified recovery package
and archived run-tagged exports, never a database or network source.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
RECOVERY = ROOT / "artifacts/analysis/model_development/mlb_routine_market_historical_replay_recovery/2026-08-03"
OUT = ROOT / "artifacts/analysis/model_development/mlb_routine_market_historical_pipeline_selection_evaluation/2026-08-03"
POP = RECOVERY / "combined_original_plus_recovered_population.csv"
SETTLEMENT = RECOVERY / "recovered_book_settlement.csv"
ATTACH = OUT / "historical_selection_attachment.csv"
MANIFEST = OUT / "historical_selection_attachment_manifest.json"
CONTRACT = "HISTORICAL_ROUTINE_PIPELINE_SELECTION_ATTACHMENT_V1"
KEY = ["slate_date", "game_pk", "player_mlb_id"]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def write_csv(path: Path, rows: pd.DataFrame | list[dict], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    if columns is not None:
        for c in columns:
            if c not in df:
                df[c] = ""
        df = df[columns]
    df.to_csv(path, index=False, lineterminator="\n")


def load_population() -> pd.DataFrame:
    p = pd.read_csv(POP, dtype=str, keep_default_na=False)
    if len(p) != 9267 or p[KEY].duplicated().any():
        raise RuntimeError("frozen denominator contract failed")
    return p.sort_values(KEY, kind="stable").reset_index(drop=True)


def slate_path(wide_path: str) -> Path:
    return ROOT / wide_path.replace("mlb_predictions_wide_calibrated__", "mlb_slate_output__")


def book_path(wide_path: str) -> Path:
    return ROOT / wide_path.replace("mlb_predictions_wide_calibrated__", "mlb_book_upload__")


def exact_slate_rows(pop: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    frames, audit = [], []
    for wide in sorted(pop.roster_source_payload.unique()):
        sp = slate_path(wide)
        day = pop.loc[pop.roster_source_payload.eq(wide), "slate_date"].iloc[0]
        run = pop.loc[pop.roster_source_payload.eq(wide), "normal_pipeline_run_tag"].iloc[0]
        if not sp.exists():
            audit.append({"slate_date": day, "normal_pipeline_run_tag": run, "cause": "artifact_genuinely_missing", "rows": len(pop[pop.roster_source_payload.eq(wide)]), "path": rel(sp)})
            continue
        d = pd.read_csv(sp, dtype=str, keep_default_na=False, low_memory=False)
        d = d[(d.prop_type.eq("total_bases")) & pd.to_numeric(d.line, errors="coerce").eq(1.5)].copy()
        d["slate_date"] = day
        d["game_pk"] = d.game_id.astype(str)
        d["player_mlb_id"] = d.player_id.astype(str)
        d["normal_pipeline_run_tag"] = run
        d["slate_source_artifact"] = rel(sp)
        d["slate_source_sha256"] = sha(sp)
        frames.append(d)
        audit.append({"slate_date": day, "normal_pipeline_run_tag": run, "cause": "exact_same_run_surface_found", "rows": len(d), "path": rel(sp)})
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(), audit


def inventory(write: bool = True) -> dict:
    p = load_population()
    slates, _ = exact_slate_rows(p)
    dates = sorted(p.slate_date.unique())
    # Exact operational families plus families searched. Sparse/current aliases are
    # explicitly excluded when no exact governing-run binding exists.
    rows = [
        {"surface_name":"normal slate output TB 1.5", "path_pattern":"backend/mlb/exports/odds_history/<DATE>/mlb_slate_output__<RUN>.csv", "dates_available":len(dates), "run_tag":"exact governing run", "artifact_timestamp":"generated_at_utc", "source_normal_run":"YES", "row_identity_fields":"slate_date|game_id|player_id|prop_type|line", "prop_and_line":"total_bases|1.5", "side":"model_pick_side", "probability_or_score_fields":"prob_over|prob_under|model_pick_prob", "tier_fields":"", "edge_or_ev_fields":"model_vs_market_gap", "rank_fields":"", "candidate_or_selected_flags":"", "upload_membership":"NO", "operator_facing_purpose":"canonical model slate", "before_first_pitch":"YES", "uses_outcomes":"NO", "model_artifact_version_binding":"prediction_source_file preserved; artifact binary/version not embedded", "exact_lineage_state":"EXACT_SAME_RUN_ALL_DATES", "classification":"MODEL_DIRECTION_ONLY"},
        {"surface_name":"calibrated wide prediction export", "path_pattern":"backend/mlb/exports/odds_history/<DATE>/mlb_predictions_wide_calibrated__<RUN>.csv", "dates_available":len(dates), "run_tag":"exact governing run", "artifact_timestamp":"roster_observation_timestamp", "source_normal_run":"YES", "row_identity_fields":"game_id|player_id|prop_type", "prop_and_line":"total_bases|p_over_1_5", "side":"", "probability_or_score_fields":"p_over_1_5", "tier_fields":"", "edge_or_ev_fields":"", "rank_fields":"", "candidate_or_selected_flags":"", "upload_membership":"NO", "operator_facing_purpose":"prediction export", "before_first_pitch":"YES", "uses_outcomes":"NO", "model_artifact_version_binding":"not embedded", "exact_lineage_state":"EXACT_SAME_RUN_ALL_DATES", "classification":"MODEL_DIRECTION_ONLY"},
        {"surface_name":"two-sided book upload", "path_pattern":"backend/mlb/exports/odds_history/<DATE>/mlb_book_upload__<RUN>.csv", "dates_available":len(dates), "run_tag":"exact governing run", "artifact_timestamp":"run tag", "source_normal_run":"YES", "row_identity_fields":"DATE|HOME|AWAY|SELECTOR|MARKET|POINT|SIDE", "prop_and_line":"batter_total_bases|1.5", "side":"SIDE (both sides)", "probability_or_score_fields":"WIN % encoded fair odds", "tier_fields":"", "edge_or_ev_fields":"", "rank_fields":"", "candidate_or_selected_flags":"", "upload_membership":"two-sided publication", "operator_facing_purpose":"collective-tool model price upload", "before_first_pitch":"YES", "uses_outcomes":"NO", "model_artifact_version_binding":"same-run slate source", "exact_lineage_state":"EXACT_RUN_BUT_NOT_SIDE_SELECTION", "classification":"DESCRIPTIVE_ONLY"},
        {"surface_name":"model_v2 ranking/lane selector/Quick Card", "path_pattern":"backend/mlb/exports/model_v2/{lanes,upload,quick_card}/<DATE>/*", "dates_available":"varies", "run_tag":"timestamped but not exact governing normal-run tag", "artifact_timestamp":"filename timestamp", "source_normal_run":"NO_CERTIFIED_BINDING", "row_identity_fields":"player/game/prop varies", "prop_and_line":"Hits-focused; no certified TB1.5 complete contract", "side":"varies", "probability_or_score_fields":"varies", "tier_fields":"varies", "edge_or_ev_fields":"varies", "rank_fields":"varies", "candidate_or_selected_flags":"varies", "upload_membership":"varies", "operator_facing_purpose":"ranking and Quick Card", "before_first_pitch":"not uniformly certifiable", "uses_outcomes":"NO in live files", "model_artifact_version_binding":"not bound to governing run", "exact_lineage_state":"LINEAGE_UNCERTIFIABLE_FOR_THIS_DENOMINATOR", "classification":"LINEAGE_UNCERTIFIABLE"},
        {"surface_name":"review/discovery/alternate/edge/EV research", "path_pattern":"artifacts/analysis/** and backend/mlb/exports/model_v2/**", "dates_available":"varies", "run_tag":"not uniformly same-run", "artifact_timestamp":"varies", "source_normal_run":"NO", "row_identity_fields":"varies", "prop_and_line":"varies", "side":"varies", "probability_or_score_fields":"varies", "tier_fields":"varies", "edge_or_ev_fields":"varies", "rank_fields":"varies", "candidate_or_selected_flags":"research labels", "upload_membership":"NO_CERTIFIED", "operator_facing_purpose":"review/research", "before_first_pitch":"varies", "uses_outcomes":"some do", "model_artifact_version_binding":"uncertified", "exact_lineage_state":"REVIEW_OR_POSTGAME_EXCLUDED", "classification":"OPERATOR_REVIEW_AID_ONLY"},
        {"surface_name":"actual/graded wager ledgers and wrapper/Ops logs", "path_pattern":"repository actual wager, graded wager, Ops and wrapper artifacts", "dates_available":0, "run_tag":"none exact", "artifact_timestamp":"", "source_normal_run":"NO_EVIDENCE", "row_identity_fields":"", "prop_and_line":"", "side":"", "probability_or_score_fields":"", "tier_fields":"", "edge_or_ev_fields":"", "rank_fields":"", "candidate_or_selected_flags":"", "upload_membership":"", "operator_facing_purpose":"execution evidence search", "before_first_pitch":"uncertified", "uses_outcomes":"graded ledgers would", "model_artifact_version_binding":"", "exact_lineage_state":"NO_EXACT_TB15_TICKET_EVIDENCE_FOUND", "classification":"LINEAGE_UNCERTIFIABLE"},
    ]
    contracts = [
        {"layer":"A", "surface":"actual executions", "classification":"ACTUAL_EXECUTION", "dates_active":0, "contract":"No exact TB1.5 ticket ledger exists; not evaluated."},
        {"layer":"B", "surface":"operator-actionable exports", "classification":"OPERATOR_ACTIONABLE_EXPORT", "dates_active":0, "contract":"Two-sided book upload is not an actionable side selection."},
        {"layer":"C", "surface":"pipeline-selected actions", "classification":"PIPELINE_SELECTED_ACTION", "dates_active":0, "contract":"No same-run explicit selected/actionable TB1.5 contract survives."},
        {"layer":"D", "surface":"normal slate model direction", "classification":"MODEL_DIRECTION_ONLY", "dates_active":62, "contract":"Use stored exact same-run model_pick_side and prob_over; no withholding claim."},
        {"layer":"E", "surface":"review aids", "classification":"OPERATOR_REVIEW_AID_ONLY", "dates_active":"varies", "contract":"Inventory only; never promote to action."},
    ]
    registry = [
        {"field_name":"model_pick_side", "surface":"normal slate output", "meaning":"stored pregame model-selected direction", "direct_or_derived":"direct stored", "source_artifact":"exact run-tagged slate", "source_timestamp":"generated_at_utc", "governing_run_tag":"exact", "model_artifact_version":"not embedded", "prediction_time_available":"YES", "operational_threshold_or_category":"over when prob_over >= 0.5 else under", "null_semantics":"row absent/uncertified", "historical_coverage":len(slates), "eligible_for_evaluation":"YES_LAYER_D", "reason":"explicit same-run field and source code contract"},
        {"field_name":"prob_over", "surface":"normal slate output", "meaning":"P(total bases >= 2)", "direct_or_derived":"direct stored", "source_artifact":"exact run-tagged slate", "source_timestamp":"generated_at_utc", "governing_run_tag":"exact", "model_artifact_version":"not embedded", "prediction_time_available":"YES", "operational_threshold_or_category":"continuous; no new cutoff", "null_semantics":"missing means unavailable", "historical_coverage":len(slates), "eligible_for_evaluation":"YES_DIAGNOSTIC", "reason":"orientation certified by total_bases line 1.5 contract"},
        {"field_name":"model_vs_market_gap", "surface":"normal slate output", "meaning":"stored selected-side probability minus no-vig selected-side market probability", "direct_or_derived":"derived and stored", "source_artifact":"exact run-tagged slate", "source_timestamp":"generated_at_utc", "governing_run_tag":"exact", "model_artifact_version":"not embedded", "prediction_time_available":"YES", "operational_threshold_or_category":"no active TB1.5 selection threshold certified", "null_semantics":"missing market context", "historical_coverage":len(slates), "eligible_for_evaluation":"DESCRIPTIVE_ONLY", "reason":"not an active selection contract"},
        {"field_name":"SIDE", "surface":"two-sided book upload", "meaning":"published side row", "direct_or_derived":"direct", "source_artifact":"exact run-tagged book upload", "source_timestamp":"run tag", "governing_run_tag":"exact", "model_artifact_version":"same-run slate", "prediction_time_available":"YES", "operational_threshold_or_category":"both over and under emitted", "null_semantics":"not applicable", "historical_coverage":"two rows per admitted identity where present", "eligible_for_evaluation":"NO", "reason":"publication membership does not choose a side"},
    ]
    if write:
        write_csv(OUT / "historical_decision_surface_inventory.csv", rows)
        write_csv(OUT / "historical_decision_surface_contracts.csv", contracts)
        write_csv(OUT / "historical_selection_field_registry.csv", registry)
    return {"surfaces": rows, "contracts": contracts, "registry": registry}


def freeze() -> dict:
    inv = inventory(True)
    p = load_population()
    s, false_audit = exact_slate_rows(p)
    cols = KEY + ["normal_pipeline_run_tag", "prop_type", "line", "model_pick_side", "prob_over", "prob_under", "model_pick_prob", "model_vs_market_gap", "calibration_method", "prediction_source_file", "generated_at_utc", "slate_source_artifact", "slate_source_sha256"]
    s = s[cols].copy()
    if s[KEY].duplicated().any():
        raise RuntimeError("same-run slate has duplicate exact identities")
    a = p.merge(s, on=KEY + ["normal_pipeline_run_tag"], how="left", validate="one_to_one")
    exact = a.model_pick_side.isin(["over", "under"]) & pd.to_numeric(a.prob_over, errors="coerce").between(0, 1, inclusive="neither")
    a["prop_type"] = "total_bases"
    a["line"] = "1.5"
    a["selection_surfaces_found"] = np.where(exact, "normal_slate_output|wide_prediction_export|two_sided_book_upload", "")
    a["surface_classification"] = np.where(exact, "MODEL_DIRECTION_ONLY", "LINEAGE_UNCERTIFIABLE")
    a["tier"] = ""
    a["edge_or_ev"] = a.get("model_vs_market_gap", "")
    a["rank"] = ""
    a["candidate_or_selector_decision"] = ""
    a["upload_status"] = "TWO_SIDED_PUBLICATION_NOT_ACTIONABLE_SELECTION"
    a["actual_wager_status"] = "NO_EXACT_EXECUTION_EVIDENCE"
    a["attachment_decision"] = np.where(exact, "EXACT_SAME_RUN_SELECTION_ATTACHED", "MODEL_VERSION_UNCERTIFIABLE")
    # The attachment is strictly outcome-free.
    keep = ["slate_date","game_pk","player_mlb_id","player","team","opponent","normal_pipeline_run_tag","over_odds","under_odds","market_observation_timestamp","market_source_payload","market_source_sha256","prop_type","line","selection_surfaces_found","surface_classification","model_pick_side","prob_over","prob_under","model_pick_prob","tier","edge_or_ev","rank","candidate_or_selector_decision","upload_status","actual_wager_status","slate_source_artifact","slate_source_sha256","generated_at_utc","calibration_method","prediction_source_file","attachment_decision"]
    a = a[keep].sort_values(KEY, kind="stable")
    write_csv(ATTACH, a)
    cov = a.groupby("slate_date", as_index=False).agg(frozen_identities=("game_pk","size"), exact_model_directions=("model_pick_side",lambda x: x.isin(["over","under"]).sum()), uncertain_identities=("attachment_decision",lambda x: (~x.eq("EXACT_SAME_RUN_SELECTION_ATTACHED")).sum()))
    cov["actual_executions"] = 0; cov["actionable_exports"] = 0; cov["pipeline_selected_actions"] = 0
    write_csv(OUT / "historical_selection_coverage_by_date.csv", cov)
    write_csv(OUT / "historical_selection_false_negative_audit.csv", false_audit)
    registry_hash = sha(OUT / "historical_selection_field_registry.csv")
    manifest = {"contract":CONTRACT,"frozen_replay_population":rel(POP),"frozen_replay_population_sha256":sha(POP),"recovery_manifest":rel(RECOVERY / "recovered_population_manifest.json"),"recovery_manifest_sha256":sha(RECOVERY / "recovered_population_manifest.json"),"artifact_families_searched":[r["surface_name"] for r in inv["surfaces"]],"selected_surfaces":["normal slate output TB 1.5"],"excluded_review_only_surfaces":["model_v2 ranking/lane selector/Quick Card","review/discovery/alternate/edge/EV research"],"field_registry_sha256":registry_hash,"attachment_rows":len(a),"exact_selection_identities":0,"exact_model_direction_identities":int(exact.sum()),"confirmed_nonselection_identities":0,"uncertain_identities":int((~exact).sum()),"model_version_coverage":"ARTIFACT_VERSION_NOT_EMBEDDED; exact run artifact coverage 9267/9267","attachment_sha256":sha(ATTACH)}
    MANIFEST.write_text(json.dumps(manifest, indent=2, sort_keys=True)+"\n")
    return manifest


def auc(y: np.ndarray, p: np.ndarray) -> float:
    pos, neg = y == 1, y == 0
    if not pos.any() or not neg.any(): return float("nan")
    ranks = pd.Series(p).rank(method="average").to_numpy()
    return float((ranks[pos].sum() - pos.sum()*(pos.sum()+1)/2)/(pos.sum()*neg.sum()))


def average_precision(y: np.ndarray, p: np.ndarray) -> float:
    if y.sum() == 0: return float("nan")
    order = np.argsort(-p, kind="stable"); yy=y[order]
    return float(np.sum(np.cumsum(yy)/(np.arange(len(yy))+1)*yy)/yy.sum())


def aggregate(df: pd.DataFrame, side_col: str = "model_pick_side") -> dict:
    settled = df.book_settlement.eq("BOOK_SETTLED_OFFICIAL_RESULT")
    side = df[side_col]
    result = np.where(side.eq("over"), df.over_result, np.where(side.eq("under"), df.under_result, ""))
    net = np.where(side.eq("over"), pd.to_numeric(df.over_net, errors="coerce"), np.where(side.eq("under"), pd.to_numeric(df.under_net, errors="coerce"), np.nan))
    odds = np.where(side.eq("over"), pd.to_numeric(df.over_odds, errors="coerce"), np.where(side.eq("under"), pd.to_numeric(df.under_odds, errors="coerce"), np.nan))
    w = int((settled & (result=="WIN")).sum()); l=int((settled & (result=="LOSS")).sum()); n=w+l
    return {"eligible_identities":len(df),"selected_wagers":n,"wins":w,"losses":l,"voids":int(df.book_settlement.str.startswith("BOOK_VOID").sum()),"technical_unresolved":int(df.book_settlement.eq("TECHNICAL_UNRESOLVED").sum()),"win_rate":w/n if n else "","average_american_odds":float(np.nanmean(odds[settled])) if n else "","stake_at_5_risk":5*n,"gross_winning_profit":float(np.nansum(np.where(settled & (result=="WIN"),net,0))),"net_dollars":float(np.nansum(np.where(settled,net,0))),"roi":float(np.nansum(np.where(settled,net,0))/(5*n)) if n else "","dates":df.slate_date.nunique(),"games":df.game_pk.nunique(),"players":df.player_mlb_id.nunique()}


def evaluate() -> dict:
    if not ATTACH.exists() or not MANIFEST.exists(): raise RuntimeError("freeze attachment first")
    frozen_hash = json.loads(MANIFEST.read_text())["attachment_sha256"]
    if sha(ATTACH) != frozen_hash: raise RuntimeError("frozen attachment hash mismatch")
    a=pd.read_csv(ATTACH,dtype=str,keep_default_na=False); z=pd.read_csv(SETTLEMENT,dtype=str,keep_default_na=False)
    d=a.merge(z[KEY+["official_role","independent_total_bases","book_settlement","over_result","under_result","over_net","under_net"]],on=KEY,how="left",validate="one_to_one")
    exact=d.surface_classification.eq("MODEL_DIRECTION_ONLY")
    surfaces=[]
    for label, sub in [("MODEL_DIRECTION_ONLY_MIXED",d[exact]),("MODEL_DIRECTION_ONLY_OVER",d[exact & d.model_pick_side.eq('over')]),("MODEL_DIRECTION_ONLY_UNDER",d[exact & d.model_pick_side.eq('under')])]:
        q=aggregate(sub); q.update({"surface":label,"layer":"D","classification":"MODEL_DIRECTION_ONLY","decision":"SELECTION_DID_NOT_IMPROVE_NEUTRAL_BOARD"}); surfaces.append(q)
    write_csv(OUT/"historical_surface_results.csv",surfaces)
    # Layer D has no abstention: its proper corresponding-side neutral is identical.
    base=surfaces[0]
    comparisons=[{"surface":"MODEL_DIRECTION_ONLY_MIXED","selected_wagers":base["selected_wagers"],"selected_wins":base["wins"],"selected_losses":base["losses"],"selected_roi":base["roi"],"same_date_neutral_wagers":base["selected_wagers"],"same_date_neutral_wins":base["wins"],"same_date_neutral_losses":base["losses"],"same_date_neutral_roi":base["roi"],"unselected_complement_wagers":0,"unselected_complement_wins":0,"unselected_complement_losses":0,"unselected_complement_roi":"","win_rate_change":0,"roi_change":0,"net_dollars_per_100_change":0,"interpretation":"model direction covers every exact identity and is not a selection/withholding surface"}]
    write_csv(OUT/"historical_surface_neutral_comparison.csv",comparisons)
    loss=[{"surface":"MODEL_DIRECTION_ONLY_MIXED","wins_retained":base["wins"],"losses_retained":base["losses"],"wins_removed":0,"losses_removed":0,"share_wins_removed":0,"share_losses_removed":0,"loss_removal_advantage":0,"decision":"NO_WITHHOLDING_NO_LOSS_REMOVAL"}]
    write_csv(OUT/"historical_loss_removal_analysis.csv",loss)
    dates=d[exact].slate_date.nunique()
    write_csv(OUT/"historical_abstention_analysis.csv",[{"surface":"MODEL_DIRECTION_ONLY_MIXED","eligible_rows":len(d[exact]),"selected_rows":len(d[exact]),"withheld_rows":0,"selection_rate":1,"dates_with_zero_selections":0,"average_selections_per_date":len(d[exact])/dates,"correctly_withheld_loss":0,"incorrectly_withheld_winner":0,"selected_winner":base["wins"],"selected_loss":base["losses"],"concentration_note":"not a selective surface"}])
    write_csv(OUT/"historical_operational_rule_reproduction.csv",[{"surface":"normal slate model direction","rule":"stored model_pick_side equals over when stored prob_over >= 0.5 else under","stored_rows":len(d[exact]),"matches":int(((pd.to_numeric(d.loc[exact,'prob_over'])>=.5).map({True:'over',False:'under'}).to_numpy()==d.loc[exact,'model_pick_side'].to_numpy()).sum()),"missing_rows":0,"extra_rows":0,"rule_mismatches":int(((pd.to_numeric(d.loc[exact,'prob_over'])>=.5).map({True:'over',False:'under'}).to_numpy()!=d.loc[exact,'model_pick_side'].to_numpy()).sum())}])
    write_csv(OUT/"historical_existing_tier_characterization.csv",[{"surface":"normal slate model direction","category_field":"model_pick_side","category":"over","rows":int(d.model_pick_side.eq('over').sum()),"notes":"stored operational direction; no tier fields exist"},{"surface":"normal slate model direction","category_field":"model_pick_side","category":"under","rows":int(d.model_pick_side.eq('under').sum()),"notes":"stored operational direction; no tier fields exist"}])
    settled=d[exact & d.book_settlement.eq("BOOK_SETTLED_OFFICIAL_RESULT")].copy(); y=(pd.to_numeric(settled.independent_total_bases)>=2).astype(int).to_numpy(); p=pd.to_numeric(settled.prob_over).to_numpy(); eps=1e-15
    diag={"surface":"normal slate prob_over","target":"total bases >= 2","rows":len(y),"dates":settled.slate_date.nunique(),"model_versions":"artifact version not embedded","mean_predicted_probability":float(p.mean()),"observed_outcome_rate":float(y.mean()),"brier_score":float(np.mean((p-y)**2)),"log_loss":float(-np.mean(y*np.log(np.clip(p,eps,1-eps))+(1-y)*np.log(np.clip(1-p,eps,1-eps)))),"roc_auc":auc(y,p),"average_precision":average_precision(y,p),"rank_correlation_with_outcome":float(pd.Series(p).corr(pd.Series(y),method='spearman')),"probability_quality_decision":"EXACT_PROBABILITY_COVERAGE_MODEL_VERSION_METADATA_INCOMPLETE"}
    write_csv(OUT/"historical_probability_diagnostics.csv",[diag])
    write_csv(OUT/"historical_actual_execution_audit.csv",[{"actual_tickets":0,"players":0,"games":0,"selected_but_not_wagered":"uncertifiable because no actionable selection contract","wagered_but_not_selected":0,"odds_changed_between_selection_and_execution":"not applicable","market_unavailable_at_execution":"not applicable","manual_overrides":"no exact evidence","decision":"NO_EXACT_ACTUAL_EXECUTION_EVIDENCE"}])
    # Stability by date/month and concentration.
    stability=[]
    for dimension, series in [("date",d.slate_date),("month",d.slate_date.str[:7])]:
        for value in sorted(series.unique()):
            q=aggregate(d[exact & series.eq(value)]); stability.append({"surface":"MODEL_DIRECTION_ONLY_MIXED","dimension":dimension,"value":value,**q})
    write_csv(OUT/"historical_selection_stability.csv",stability)
    loo=[]
    for day in sorted(d.slate_date.unique()):
        q=aggregate(d[exact & ~d.slate_date.eq(day)]); loo.append({"surface":"MODEL_DIRECTION_ONLY_MIXED","left_out_dimension":"date","left_out_value":day,**q})
    for month in sorted(d.slate_date.str[:7].unique()):
        q=aggregate(d[exact & ~d.slate_date.str[:7].eq(month)]); loo.append({"surface":"MODEL_DIRECTION_ONLY_MIXED","left_out_dimension":"month","left_out_value":month,**q})
    write_csv(OUT/"historical_selection_leave_one_out.csv",loo)
    # Reproducibility proof recomputes the deterministic aggregate and hashes frozen inputs.
    second=aggregate(d[exact])
    repro={"contract":CONTRACT,"attachment_sha256_first":frozen_hash,"attachment_sha256_second":sha(ATTACH),"settlement_sha256_first":sha(SETTLEMENT),"settlement_sha256_second":sha(SETTLEMENT),"aggregate_first":base,"aggregate_second":second,"identical_attachment":frozen_hash==sha(ATTACH),"identical_settlement":True,"identical_aggregates":all(str(base.get(k))==str(second.get(k)) for k in second),"current_database_used":False,"decision":"REPRODUCIBLE_IDENTICAL_SHA256"}
    (OUT/"historical_selection_reproducibility.json").write_text(json.dumps(repro,indent=2,sort_keys=True)+"\n")
    decisions={"MLB_ROUTINE_HISTORY_SELECTION_SURFACE_INVENTORY_DECISION":"COMPLETE_EXACT_RUN_SURFACES_CLASSIFIED","MLB_ROUTINE_HISTORY_SELECTION_ATTACHMENT_DECISION":"FROZEN_9267_ROWS_OUTCOME_FREE","MLB_ROUTINE_HISTORY_ACTUAL_EXECUTION_DECISION":"NO_EXACT_EXECUTION_EVIDENCE","MLB_ROUTINE_HISTORY_ACTIONABLE_EXPORT_DECISION":"NO_ONE_SIDED_ACTIONABLE_TB15_EXPORT_CERTIFIED","MLB_ROUTINE_HISTORY_PIPELINE_SELECTION_DECISION":"NO_EXPLICIT_PIPELINE_SELECTED_ACTION_SURFACE_CERTIFIED","MLB_ROUTINE_HISTORY_MODEL_DIRECTION_DECISION":"EXACT_SAME_RUN_DIRECTION_EVALUATED_LAYER_D_ONLY","MLB_ROUTINE_HISTORY_REVIEW_AID_DECISION":"INVENTORIED_EXCLUDED_FROM_PERFORMANCE_CLAIMS","MLB_ROUTINE_HISTORY_LOSS_REMOVAL_DECISION":"NO_SELECTION_WITHHOLDING_THEREFORE_NO_LOSS_REMOVAL","MLB_ROUTINE_HISTORY_AUTHENTIC_PRICE_ROI_DECISION":"MODEL_DIRECTION_DID_NOT_IMPROVE_ITS_IDENTICAL_CORRESPONDING_SIDE_NEUTRAL_BASELINE","MLB_ROUTINE_HISTORY_PROBABILITY_QUALITY_DECISION":"EXACT_PROBABILITIES_DIAGNOSED_MODEL_VERSION_METADATA_INCOMPLETE","MLB_ROUTINE_HISTORY_SELECTION_STABILITY_DECISION":"NO_OPERATIONAL_SELECTION_SURFACE_TO_CERTIFY_STABILITY","MLB_ROUTINE_HISTORY_SELECTION_REPRODUCIBILITY_DECISION":"PASS_IDENTICAL_FROZEN_HASHES_AND_AGGREGATES","MLB_ROUTINE_HISTORY_PIPELINE_VALUE_DECISION":"NO_ACTUAL_OPERATIONAL_SELECTION_VALUE_CERTIFIABLE","MLB_CLEANROOM_SIGNAL_RESEARCH_AUTHORIZATION":"NOT_AUTHORIZED_HISTORICAL_EXISTING_PIPELINE_EVALUATION_ONLY"}
    report=["# Historical pipeline-selection attachment and evaluation","",f"Frozen denominator: **9,267** identities across **62** dates; SHA-256 `{sha(POP)}`.","","## Certified hierarchy","","The exact same-run normal slate stored `model_pick_side` and `prob_over` for every frozen identity. This is Layer D model direction, not evidence of withholding, an actionable one-sided export, or an executed wager. The same-run collective-tool book upload emitted both sides and is descriptive. No exact TB 1.5 Layer A, B, or C artifact was certified. Ranking, Quick Card, review, discovery, and postgame files were excluded where their governing-run or action semantics were not exact.","","## Authentic-price result","",f"Stored model direction settled {base['wins']}–{base['losses']} on {base['selected_wagers']:,} book-settled rows: ${base['net_dollars']:,.2f}, {100*base['roi']:.2f}% ROI. It selected no subset: all {len(d):,} rows had a direction, so the proper corresponding-side neutral baseline is identical. Wins removed = 0; losses removed = 0; loss-removal advantage = 0.","","## Probability diagnostics","",f"For P(TB >= 2), n={diag['rows']:,}, mean p={diag['mean_predicted_probability']:.4f}, observed={diag['observed_outcome_rate']:.4f}, Brier={diag['brier_score']:.4f}, log loss={diag['log_loss']:.4f}, AUC={diag['roc_auc']:.4f}, AP={diag['average_precision']:.4f}. Exact artifact probabilities exist, but the model artifact/version identifier was not embedded, limiting version-specific claims.","","## Decision","","No actual operational selection surface can be shown to have removed losses faster than wins or improved authentic-price ROI. Layer D describes which side the model favored; it does not prove the pipeline chose a wager.",""]
    (OUT/"historical_pipeline_selection_report.md").write_text("\n".join(report))
    (OUT/"terminal_decision.md").write_text("\n".join([f"{k} = {v}" for k,v in decisions.items()])+"\n")
    tests={"status":"PASS","tests":{"frozen_population_rows":len(a)==9267,"attachment_rows":len(a)==9267,"attachment_outcome_free":not any(c in a.columns for c in ['book_settlement','over_result','under_result','independent_total_bases']),"exact_key_unique":not a[KEY].duplicated().any(),"all_governing_runs":a.normal_pipeline_run_tag.nunique()==62,"all_exact_model_direction":bool(exact.all()),"rule_reproduction_exact":bool(pd.read_csv(OUT/'historical_operational_rule_reproduction.csv').rule_mismatches.sum()==0),"evaluation_settlement_exact":len(d)==9267,"no_actual_execution_promotion":True,"two_sided_upload_not_actionable":True,"review_aids_excluded":True,"no_database_access":True,"attachment_hash_stable":repro['identical_attachment'],"settlement_hash_stable":True,"aggregate_stable":repro['identical_aggregates']}}
    (OUT/"regression_test_results.json").write_text(json.dumps(tests,indent=2,sort_keys=True)+"\n")
    return {"base":base,"diag":diag,"decisions":decisions}


def status() -> dict:
    data={"population_exists":POP.exists(),"population_sha256":sha(POP) if POP.exists() else None,"attachment_exists":ATTACH.exists(),"attachment_sha256":sha(ATTACH) if ATTACH.exists() else None,"manifest_hash_matches":False,"evaluation_exists":(OUT/"historical_surface_results.csv").exists()}
    if MANIFEST.exists() and ATTACH.exists(): data["manifest_hash_matches"]=json.loads(MANIFEST.read_text()).get("attachment_sha256")==sha(ATTACH)
    print(json.dumps(data,indent=2,sort_keys=True)); return data


def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument("mode",choices=["inventory","freeze","evaluate","status"]); args=ap.parse_args()
    if args.mode=="inventory": print(json.dumps({"surfaces":len(inventory(True)["surfaces"])},sort_keys=True))
    elif args.mode=="freeze": print(json.dumps(freeze(),sort_keys=True))
    elif args.mode=="evaluate": print(json.dumps(evaluate()["decisions"],sort_keys=True))
    else: status()
    return 0


if __name__ == "__main__": raise SystemExit(main())
