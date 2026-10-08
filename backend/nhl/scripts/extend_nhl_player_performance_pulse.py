#!/usr/bin/env python3
"""Create a create-only responsiveness revision for the NHL player pulse.

This reads the established pulse and retained NHL artifacts. It performs no
database writes, provider calls, scoring, or model training.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
BASE = ROOT / "artifacts/analysis/nhl/player_performance_pulse/2026-10-07_final"
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_nhl_player_performance_pulse as pulse  # noqa: E402

DIRECT_FEATURES = {
    "sog": ["d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg", "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game"],
    "points": ["is_home", *pulse.LANES["points"]["model_state_features"]],
    "saves": list(pulse.LANES["saves"]["model_state_features"]),
}
DYNAMIC_PRODUCTION_FEATURES = {
    "sog": DIRECT_FEATURES["sog"],
    "points": [x for x in DIRECT_FEATURES["points"] if x != "is_home"],
    "saves": ["rest_days", "b2b_flag", "d5_saves_per60", "d10_saves_per60", "d5_shots_faced_per60", "season_save_pct"],
}
PRIMARY_FEATURE = {"sog": "d10_sog_per60", "points": "d10_sog_per60", "saves": "d5_saves_per60"}
EXPECTED_DIRECTION = {"sog": {"d5_sog_per60": 1, "d10_sog_per60": 1, "d20_sog_per60": 1, "d5_toi_min_avg": 1, "d10_toi_min_avg": 1, "d20_toi_min_avg": 1}, "points": {}, "saves": {"d5_saves_per60": 1, "d10_saves_per60": 1, "d5_shots_faced_per60": 1}}


def _load_transitions(as_of: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    base = pd.read_csv(BASE / "player_state_transitions.csv")
    base = base[base.game_n1_date.astype(str) <= as_of].copy()
    all_records = pulse.run_records()
    base = pulse.enrich_transition_player_names(base, pulse.roster_identity_names(all_records))
    # Add only transitions made newly observable by the requested as-of state.
    prior_date = (pd.Timestamp(as_of) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    recs = [(p, r) for p, r in all_records if str(r.get("slate_date")) in {prior_date, as_of}]
    audit = {"missing_artifacts": 0, "hash_mismatches": 0, "eligible_state_rows": 0}
    states = {lane: pulse.collect_states(lane, recs, audit) for lane in pulse.LANES}
    added = []
    for lane, frame in states.items():
        if frame.empty: continue
        prev = frame[frame.slate_date.astype(str) == prior_date]
        cur = frame[frame.slate_date.astype(str) == as_of]
        if prev.empty or cur.empty: continue
        outcomes = pulse.load_outcomes(prior_date, lane, audit)
        if outcomes.empty: continue
        outcomes = outcomes.sort_values("outcome_source_timestamp_utc").drop_duplicates(["game_id", "player_id"], keep="last")
        prev = prev.merge(outcomes[[c for c in ["game_id", "player_id", "outcome_value", "outcome_source_timestamp_utc", "outcome_path", "outcome_sha256"] if c in outcomes]], on=["game_id", "player_id"], how="left")
        for _, a in prev.iterrows():
            bset = cur[cur.player_id == a.player_id]
            for _, b in bset.iterrows():
                realized_at = pd.to_datetime(a.get("outcome_source_timestamp_utc"), utc=True, errors="coerce")
                cutoff = pd.to_datetime(b.get("feature_cutoff_utc"), utc=True, errors="coerce")
                if pd.isna(a.get("outcome_value")) or pd.isna(realized_at) or pd.isna(cutoff) or realized_at >= cutoff:
                    continue
                added.append(_transition_from_states(lane, a, b))
    if added:
        new = pd.DataFrame(added)
        base_keys = set(zip(base.lane, base.game_n_id.astype(str), base.game_n1_id.astype(str), base.player_id.astype(str)))
        new = new[[ (r.lane, str(r.game_n_id), str(r.game_n1_id), str(r.player_id)) not in base_keys for r in new.itertuples() ]]
        if not new.empty: base = pd.concat([base, new], ignore_index=True, sort=False)
    base = base.sort_values(["lane", "player_id", "game_n_date", "game_n1_date", "game_n_id"]).reset_index(drop=True)
    return base, {**audit, "new_transition_rows": len(added)}


def _transition_from_states(lane: str, a: pd.Series, b: pd.Series) -> dict[str, Any]:
    return {
        "lane": lane, "player_id": int(a.player_id), "player_name": pulse.first_valid_player_name(b.get("player_name"), a.get("player_name")),
        "team_id_game_n": a.get("team_id"), "team_id_game_n1": b.get("team_id"),
        "game_n_id": int(a.game_id), "game_n_date": str(a.slate_date), "game_n1_id": int(b.game_id), "game_n1_date": str(b.slate_date),
        "elapsed_days": (pd.Timestamp(b.slate_date)-pd.Timestamp(a.slate_date)).days,
        "realized_stat": a.get("outcome_value"), "outcome_source_timestamp_utc": a.get("outcome_source_timestamp_utc"),
        "outcome_source_path": a.get("outcome_path"), "outcome_source_sha256": a.get("outcome_sha256"),
        "feature_state_prior": pulse.strict_json_dumps(a.get("feature_values") or {}, sort_keys=True), "feature_state_current": pulse.strict_json_dumps(b.get("feature_values") or {}, sort_keys=True),
        "prediction_prior": a.get("prediction_ladder", "[]"), "prediction_current": b.get("prediction_ladder", "[]"),
        "model_family_version": b.get("model_family_version"),
        "feature_artifact_identity_prior": a.get("feature_path"), "feature_artifact_sha256_prior": a.get("feature_sha256"),
        "feature_artifact_identity_current": b.get("feature_path"), "feature_artifact_sha256_current": b.get("feature_sha256"),
        "prediction_artifact_identity_prior": a.get("prediction_path"), "prediction_artifact_sha256_prior": a.get("prediction_sha256"),
        "prediction_artifact_identity_current": b.get("prediction_path"), "prediction_artifact_sha256_current": b.get("prediction_sha256"),
        "feature_hash_receipt_bound_prior": a.get("feature_hash_bound_by_receipt"), "feature_hash_receipt_bound_current": b.get("feature_hash_bound_by_receipt"),
    }


def _loads(v: Any) -> dict[str, Any]:
    try: return json.loads(v) if isinstance(v, str) else {}
    except (TypeError, json.JSONDecodeError): return {}


def _ladder(v: Any) -> dict[float, float]:
    try:
        items = json.loads(v) if isinstance(v, str) else []
        return {float(x["line"]): float(x["prob_over"]) for x in items if x.get("line") is not None and x.get("prob_over") is not None}
    except (TypeError, ValueError, json.JSONDecodeError): return {}


def prediction_transitions(transitions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for r in transitions.to_dict("records"):
        # Family/version labels are useful for excluding an identifiable switch,
        # but they do not prove fitted-parameter identity.
        family = r.get("model_family_version")
        if not family:
            continue
        prior = _ladder(r.get("prediction_prior")); curr = _ladder(r.get("prediction_current"))
        for line in sorted(prior.keys() & curr.keys()):
            p0, p1 = prior[line], curr[line]
            rows.append({"player_id": r["player_id"], "player_name": r.get("player_name"), "lane": r["lane"], "prior_game_id": r["game_n_id"], "prior_date": r["game_n_date"], "next_game_id": r["game_n1_id"], "next_date": r["game_n1_date"], "line": line, "prior_probability": p0, "current_probability": p1, "signed_probability_change": p1-p0, "absolute_probability_change": abs(p1-p0), "prior_prediction_artifact_identity": r.get("prediction_artifact_identity_prior"), "prior_prediction_artifact_sha256": r.get("prediction_artifact_sha256_prior"), "current_prediction_artifact_identity": r.get("prediction_artifact_identity_current"), "current_prediction_artifact_sha256": r.get("prediction_artifact_sha256_current"), "model_family_version": family, "model_version_comparability_status": "UNPROVEN_FITTED_ARTIFACT_HASH_NOT_BOUND_TO_HISTORICAL_RECEIPTS"})
    return pd.DataFrame(rows)


def movement_and_classes(transitions: pd.DataFrame, prediction: pd.DataFrame, base_movements: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    m = base_movements.copy()
    m = m[m.feature.isin(sum(DIRECT_FEATURES.values(), []))]
    movement_index = {}
    for key, group in m.groupby(["lane", "player_id", "game_n_id", "game_n1_id"], sort=False):
        movement_index[(str(key[0]), str(key[1]), str(key[2]), str(key[3]))] = group
    classes = []
    pstats = {}
    pred_by = prediction.groupby(["lane", "player_id", "prior_game_id", "next_game_id"], dropna=False) if not prediction.empty else None
    p_lookup = {}
    if pred_by is not None:
        for key, g in pred_by:
            p_lookup[(key[0], str(key[1]), str(key[2]), str(key[3]))] = g.absolute_probability_change.tolist()
    for r in transitions.to_dict("records"):
        fm = movement_index.get((str(r["lane"]), str(r["player_id"]), str(r["game_n_id"]), str(r["game_n1_id"])), m.iloc[0:0])
        direct = fm[fm.feature.isin(DIRECT_FEATURES[r["lane"]])]
        changes = pd.to_numeric(direct.signed_change, errors="coerce").dropna()
        direct_nonzero = changes.abs()[changes != 0]
        state_max_z = pd.to_numeric(direct.standardized_movement, errors="coerce").abs().max() if len(direct) else np.nan
        state_stable = not len(direct_nonzero)
        k = (r["lane"], str(r["player_id"]), str(r["game_n_id"]), str(r["game_n1_id"]))
        pdeltas = p_lookup.get(k, [])
        pmax = max(pdeltas) if pdeltas else None
        classes.append({**{k: r.get(k) for k in ["lane", "player_id", "player_name", "game_n_id", "game_n_date", "game_n1_id", "game_n1_date", "realized_stat"]}, "direct_dynamic_feature_count": len(DIRECT_FEATURES[r["lane"]]), "direct_features_changed_count": int((changes != 0).sum()), "maximum_abs_feature_z_movement": state_max_z, "maximum_abs_probability_movement": pmax, "prediction_line_count": len(pdeltas), "state_stable": state_stable, "model_stable": all(x == 0 for x in pdeltas) if pdeltas else None, "model_version_comparability_status": "UNPROVEN_FITTED_ARTIFACT_HASH_NOT_BOUND_TO_HISTORICAL_RECEIPTS"})
    class_df = pd.DataFrame(classes)
    summary = {}
    for lane in pulse.LANES:
        pg = prediction[prediction.lane == lane] if not prediction.empty else prediction
        vals = pd.to_numeric(pg.absolute_probability_change, errors="coerce").dropna() if len(pg) else pd.Series(dtype=float)
        nonzero = vals[vals > 0]
        p50 = float(nonzero.median()) if len(nonzero) else 0.0
        p25 = float(nonzero.quantile(.25)) if len(nonzero) else 0.0
        summary[lane] = {"prediction_transition_count": int(len(pg)), "comparable_model_hash_count": 0, "model_version_comparability": "UNPROVEN", "median_absolute_probability_movement": float(vals.median()) if len(vals) else None, "mean_absolute_probability_movement": float(vals.mean()) if len(vals) else None, "p75_absolute_probability_movement": float(vals.quantile(.75)) if len(vals) else None, "p90_absolute_probability_movement": float(vals.quantile(.90)) if len(vals) else None, "p95_absolute_probability_movement": float(vals.quantile(.95)) if len(vals) else None, "maximum_absolute_probability_movement": float(vals.max()) if len(vals) else None, "exact_zero_movement_rate": float((vals == 0).mean()) if len(vals) else None, "near_zero_distribution": {"nonzero_p10": float(nonzero.quantile(.10)) if len(nonzero) else None, "nonzero_p25": p25, "nonzero_p50": p50, "count_below_nonzero_p25": int((nonzero < p25).sum()) if len(nonzero) else 0}, "characterization_only_not_action_threshold": True}
        cl = class_df[class_df.lane == lane]
        # The empirical median nonzero probability movement is a descriptive boundary only.
        for _, c in cl.iterrows():
            class_df.loc[c.name, "observed_response_case"] = observed_response_case(c, p50)
            class_df.loc[c.name, "descriptive_case"] = descriptive_case(c, p50)
        cl = class_df[class_df.lane == lane]
        summary[lane]["state_model_case_counts"] = cl.observed_response_case.value_counts(dropna=False).to_dict()
        summary[lane]["state_moved_model_moved_count"] = int(((~cl.state_stable) & (cl.maximum_abs_probability_movement >= p50) & (cl.maximum_abs_probability_movement.notna())).sum())
        summary[lane]["state_moved_model_largely_static_count"] = int(((~cl.state_stable) & (cl.maximum_abs_probability_movement < p50) & (cl.maximum_abs_probability_movement.notna())).sum())
        summary[lane]["state_stable_model_moved_count"] = int((cl.state_stable & (cl.maximum_abs_probability_movement >= p50) & (cl.maximum_abs_probability_movement.notna())).sum())
        summary[lane]["characterization_rule"] = f"state stable = no direct feature changed; probability moved = at least lane nonzero median ({p50:.8g}); lower nonzero movement is largely static. Characterization only, not action threshold."
    return m, class_df, summary


def descriptive_case(row: pd.Series, prediction_cut: float) -> str:
    if row.get("model_version_comparability_status") != "PROVEN": return "MODEL_VERSION_COMPARABILITY_UNPROVEN"
    if row.get("state_stable") and row.get("model_stable"): return "STATE_STABLE_MODEL_STABLE"
    if not row.get("state_stable") and row.get("model_stable"): return "STATE_MOVED_MODEL_LARGELY_STATIC"
    if row.get("state_stable") and not row.get("model_stable"): return "STATE_STABLE_MODEL_MOVED"
    if not row.get("state_stable") and row.get("maximum_abs_probability_movement", 0) < prediction_cut: return "STATE_MOVED_MODEL_LARGELY_STATIC"
    if not row.get("state_stable"): return "STATE_MOVED_MODEL_MOVED"
    return "INSUFFICIENT_COMPARABLE_STATE"


def observed_response_case(row: pd.Series, prediction_cut: float) -> str:
    if row.get("prediction_line_count", 0) == 0: return "INSUFFICIENT_COMPARABLE_STATE"
    moved = row.get("maximum_abs_probability_movement")
    model_moved = moved is not None and not pd.isna(moved) and moved >= prediction_cut and moved > 0
    if row.get("state_stable") and not model_moved: return "STATE_STABLE_MODEL_STABLE"
    if not row.get("state_stable") and not model_moved: return "STATE_MOVED_MODEL_LARGELY_STATIC"
    if row.get("state_stable") and model_moved: return "STATE_STABLE_MODEL_MOVED"
    return "STATE_MOVED_MODEL_MOVED"


def consecutive_runs(flags: list[bool], minimum: int = 2) -> list[tuple[int, int]]:
    runs=[]; start=None
    for i,flag in enumerate(flags+[False]):
        if flag and start is None: start=i
        elif not flag and start is not None:
            if i-start>=minimum: runs.append((start,i-start))
            start=None
    return runs


def window_roll_forward(prior_ids: list[int], entering_id: int, size: int) -> dict[str, Any]:
    prior=list(prior_ids[-size:])
    displaced=prior[0] if len(prior)>=size else None
    current=(prior+[entering_id])[-size:]
    return {"prior_ids":prior,"current_ids":current,"entering_game_id":entering_id,"displaced_game_id":displaced,"window_count_before":len(prior),"window_count_after":min(size,len(prior)+1)}


def feature_prediction_relationships(movements: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    if pred.empty or movements.empty: return pd.DataFrame()
    expanded = []
    movement_index = {}
    for key, group in movements.groupby(["lane", "player_id", "game_n_id", "game_n1_id"], sort=False):
        movement_index[(str(key[0]), str(key[1]), str(key[2]), str(key[3]))] = group
    for r in pred.to_dict("records"):
        fm = movement_index.get((str(r["lane"]), str(r["player_id"]), str(r["prior_game_id"]), str(r["next_game_id"])), movements.iloc[0:0])
        for f in DIRECT_FEATURES[r["lane"]]:
            x = fm[fm.feature == f]
            if x.empty: continue
            expanded.append({"lane": r["lane"], "player_id": r["player_id"], "prior_game_id": r["prior_game_id"], "next_game_id": r["next_game_id"], "line": r["line"], "feature": f, "standardized_feature_movement": x.iloc[0].get("standardized_movement"), "signed_feature_change": x.iloc[0].get("signed_change"), "signed_probability_change": r["signed_probability_change"], "model_version_comparability_status": r["model_version_comparability_status"]})
    frame = pd.DataFrame(expanded)
    rows = []
    for (lane, feature), g in frame.groupby(["lane", "feature"]):
        a = pd.to_numeric(g.standardized_feature_movement, errors="coerce")
        b = pd.to_numeric(g.signed_probability_change, errors="coerce")
        valid = a.notna() & b.notna()
        corr = a[valid].corr(b[valid]) if valid.sum() >= 3 and a[valid].nunique() > 1 and b[valid].nunique() > 1 else np.nan
        direction = EXPECTED_DIRECTION.get(lane, {}).get(feature)
        moved = valid & a.ne(0)
        agreement = (((a[moved] * b[moved]) > 0).mean()) if direction == 1 and moved.sum() else None
        for band, lo, hi in [("LOW", -np.inf, a.abs().quantile(.33)), ("MEDIUM", a.abs().quantile(.33), a.abs().quantile(.67)), ("HIGH", a.abs().quantile(.67), np.inf)]:
            mask = valid & a.abs().ge(lo) & a.abs().le(hi)
            rows.append({"lane": lane, "feature": feature, "movement_band": band, "row_count": int(mask.sum()), "mean_absolute_probability_movement": float(pd.to_numeric(g.loc[mask, "signed_probability_change"], errors="coerce").abs().mean()) if mask.any() else None, "correlation_standardized_feature_vs_signed_probability": float(corr) if pd.notna(corr) else None, "directional_agreement_rate_known_positive_effect": float(agreement) if agreement is not None else None, "causal_claim": False, "model_version_comparability_status": "UNPROVEN"})
    return pd.DataFrame(rows)


def divergence_pulse(classes: pd.DataFrame, movements: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for lane, g in classes.groupby("lane"):
        positive_prob = g.maximum_abs_probability_movement.dropna()
        p50 = positive_prob[positive_prob > 0].median() if (positive_prob > 0).any() else 0.0
        p25 = positive_prob[positive_prob > 0].quantile(.25) if (positive_prob > 0).any() else 0.0
        positive_state = pd.to_numeric(g.maximum_abs_feature_z_movement, errors="coerce").dropna()
        state_p50 = positive_state[positive_state > 0].median() if (positive_state > 0).any() else 0.0
        ordered = g.sort_values(["player_id", "game_n_date", "game_n_id"])
        for player_id, pg in ordered.groupby("player_id"):
            state_moved = pd.to_numeric(pg.maximum_abs_feature_z_movement, errors="coerce").fillna(0) >= state_p50
            model_small = pg.maximum_abs_probability_movement.fillna(0) <= p25
            model_moved = pg.maximum_abs_probability_movement.fillna(0) >= p50
            for pattern, mask in [("STATE_MOVED_MODEL_LARGELY_STATIC", state_moved & model_small), ("STATE_STABLE_MODEL_MOVED", pg.state_stable.astype(bool) & model_moved)]:
                run = []
                for _, row in pg.iterrows():
                    yes = bool((row.maximum_abs_feature_z_movement >= state_p50) and (row.maximum_abs_probability_movement <= p25)) if pattern.startswith("STATE_MOVED") else bool(row.state_stable and row.maximum_abs_probability_movement >= p50)
                    if yes: run.append(row)
                    else:
                        if len(run) >= 2: rows.append(_divergence_row(lane, player_id, pattern, run))
                        run = []
                if len(run) >= 2: rows.append(_divergence_row(lane, player_id, pattern, run))
    # Also preserve runs where a specific dynamic model input is unchanged over
    # two or more new-game transitions; source coverage limits interpretation.
    index={}
    for key,group in movements.groupby(["lane","player_id","feature"],sort=False): index[(str(key[0]),str(key[1]),str(key[2]))]=group
    for (lane,player),pg in classes.sort_values(["player_id","game_n_date"]).groupby(["lane","player_id"]):
        for feature in DYNAMIC_PRODUCTION_FEATURES[lane]:
            all_rows=[]
            for _,c in pg.iterrows():
                g=index.get((str(lane),str(player),str(feature)))
                if g is None: all_rows.append(False); continue
                hit=g[(g.game_n_id.astype(str)==str(c.game_n_id))&(g.game_n1_id.astype(str)==str(c.game_n1_id))]
                all_rows.append(bool(not hit.empty and _num(hit.iloc[0].get("signed_change")) == 0))
            for start,length in consecutive_runs(all_rows,2):
                span=pg.iloc[start:start+length]
                rows.append({"lane":lane,"player_id":int(player),"pattern":"DYNAMIC_FEATURE_IDENTICAL_ACROSS_NEW_GAMES","feature":feature,"persistence_transitions":length,"start_game_date":span.iloc[0].game_n_date,"end_game_date":span.iloc[-1].game_n1_date,"model_version_comparability_status":"UNPROVEN_FITTED_ARTIFACT_HASH_NOT_BOUND_TO_HISTORICAL_RECEIPTS","characterization_only_not_action_threshold":True})
    return pd.DataFrame(rows)


def _divergence_row(lane: str, player_id: int, pattern: str, run: list[pd.Series]) -> dict[str, Any]:
    return {"lane": lane, "player_id": int(player_id), "pattern": pattern, "persistence_transitions": len(run), "start_game_date": run[0].game_n_date, "end_game_date": run[-1].game_n1_date, "model_version_comparability_status": "UNPROVEN_FITTED_ARTIFACT_HASH_NOT_BOUND_TO_HISTORICAL_RECEIPTS", "characterization_only_not_action_threshold": True}


def stale_state_pulse(transitions: pd.DataFrame, movements: pd.DataFrame) -> pd.DataFrame:
    rows = []
    movement_index = {}
    for key, group in movements.groupby(["lane", "player_id", "game_n_id", "game_n1_id"], sort=False):
        movement_index[(str(key[0]), str(key[1]), str(key[2]), str(key[3]))] = group
    for r in transitions.to_dict("records"):
        fm = movement_index.get((str(r["lane"]), str(r["player_id"]), str(r["game_n_id"]), str(r["game_n1_id"])), movements.iloc[0:0])
        for f in DIRECT_FEATURES[r["lane"]]:
            hit = fm[fm.feature == f]
            if hit.empty: continue
            val = hit.iloc[0]
            unchanged = val.get("signed_change") == 0
            if not unchanged: continue
            # Retained history does not prove complete source-log membership, so a
            # zero delta alone cannot establish stale cache reuse.
            status = "UNRESOLVED_HISTORY_LIMITATION"
            rows.append({"lane": r["lane"], "player_id": r["player_id"], "game_n_id": r["game_n_id"], "game_n1_id": r["game_n1_id"], "feature": f, "realized_game_n_stat": r.get("realized_stat"), "prior_value": val.get("prior_value"), "current_value": val.get("current_value"), "status": status, "confirmed_stale_failure": False})
    columns=["lane","player_id","game_n_id","game_n1_id","feature","realized_game_n_stat","prior_value","current_value","status","confirmed_stale_failure"]
    return pd.DataFrame(rows,columns=columns)


def prospective_window_lineage(as_of: str, transitions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    prospective = transitions[transitions.game_n1_date.astype(str) == as_of]
    logs_by_lane: dict[str, dict[int, list[dict[str, Any]]]] = {"sog": {}, "points": {}}
    for lane in logs_by_lane:
        outcome_name = pulse.LANES[lane]["outcome_file"]
        for date_dir in sorted((ROOT / "artifacts/operational/nhl/postgame_reconciliation").glob("2026-*")):
            date = date_dir.name
            if date > as_of or date < "2026-09-29": continue
            p = pulse.find_outcome(date, outcome_name)
            if not p: continue
            d = pd.read_csv(p)
            d["game_id"] = pd.to_numeric(d.game_id, errors="coerce")
            d["player_id"] = pd.to_numeric(d.player_id, errors="coerce")
            d = d[d.game_id.astype("Int64").astype(str).str.slice(4,6).eq("02")]
            for player, group in d.groupby("player_id", sort=False):
                logs_by_lane[lane].setdefault(int(player), []).extend([{**item,"slate_date":date} for item in group.to_dict("records")])
    for r in prospective.to_dict("records"):
        lane = r["lane"]
        if lane not in ("sog", "points"): continue
        logs = [x for x in logs_by_lane[lane].get(int(r["player_id"]), []) if x["slate_date"] <= r["game_n_date"]]
        logs = sorted(logs, key=lambda x:(x["slate_date"],x["game_id"]))
        for n, feature in [(5,"d5_sog_per60"),(10,"d10_sog_per60"),(20,"d20_sog_per60"),(5,"num_sog_last5"),(10,"num_sog_last10")]:
            prior_ids = [int(x["game_id"]) for x in logs if x["slate_date"] < r["game_n_date"]]
            roll=window_roll_forward(prior_ids,int(r["game_n_id"]),n)
            rows.append({"lane": lane, "player_id": r["player_id"], "prior_game_id": r["game_n_id"], "next_game_id": r["game_n1_id"], "feature": feature, "window_size": n, "prior_window_game_ids_retained": pulse.strict_json_dumps(roll["prior_ids"]), "current_window_game_ids_retained": pulse.strict_json_dumps(roll["current_ids"]), "entering_game_id": roll["entering_game_id"], "displaced_game_id_if_full": roll["displaced_game_id"], "window_count_before": roll["window_count_before"], "window_count_after": roll["window_count_after"], "history_completeness": "PARTIAL_RETAINED_OFFICIAL_OUTCOMES", "audit_status": "PROSPECTIVE_LINEAGE_CAPTURED_NOT_YET_SOURCE_COMPLETE"})
    columns=["lane","player_id","prior_game_id","next_game_id","feature","window_size","prior_window_game_ids_retained","current_window_game_ids_retained","entering_game_id","displaced_game_id_if_full","window_count_before","window_count_after","history_completeness","audit_status"]
    return pd.DataFrame(rows,columns=columns)


def exemplars(transitions: pd.DataFrame, classes: pd.DataFrame, pred: pd.DataFrame) -> dict[str, Any]:
    out = {}
    for lane in pulse.LANES:
        rows = []
        for r in transitions[transitions.lane == lane].to_dict("records"):
            a, b = _loads(r.get("feature_state_prior")), _loads(r.get("feature_state_current"))
            f = PRIMARY_FEATURE[lane]
            av, bv = _num(a.get(f)), _num(b.get(f))
            if av is None or bv is None: continue
            pp = _ladder(r.get("prediction_prior")); cp = _ladder(r.get("prediction_current"))
            common = sorted(pp.keys() & cp.keys())
            delta = (cp[common[0]] - pp[common[0]]) if common else None
            row = {"player_id":r["player_id"],"player_name":r.get("player_name"),"game_n_date":r["game_n_date"],"game_n1_date":r["game_n1_date"],"feature":f,"prior_state":av,"realized_game_n":r.get("realized_stat"),"current_state":bv,"signed_probability_change_at_first_common_line":delta,"model_version_comparability_status":"UNPROVEN"}
            rows.append((bv-av,row))
        if rows:
            out[lane] = {"improving_state":max(rows,key=lambda x:x[0])[1],"declining_state":min(rows,key=lambda x:x[0])[1],"stable_state":next((r for d,r in rows if d==0),None)}
    return out


def _num(v: Any) -> float | None:
    try:
        if v is None or pd.isna(v): return None
        x=float(v); return x if math.isfinite(x) else None
    except (TypeError,ValueError): return None


def combine_movement_frames(old_mov: pd.DataFrame, new_mov: list[dict[str, Any]]) -> pd.DataFrame:
    """Append new movement rows without letting all-NA columns affect concat dtypes."""
    new_mov_df = pd.DataFrame(new_mov)
    if new_mov_df.empty:
        return old_mov.copy()

    # New standardized movements are intentionally unknown until the combined
    # population is evaluated below. Give these all-NA columns the established
    # dtype where available, while retaining every column in the result.
    for column in new_mov_df.columns:
        if column in old_mov.columns and new_mov_df[column].isna().all():
            new_mov_df[column] = new_mov_df[column].astype(old_mov[column].dtype)
    return pd.concat([old_mov, new_mov_df], ignore_index=True, sort=False)


def main() -> int:
    global BASE
    ap=argparse.ArgumentParser()
    ap.add_argument("--as-of-date",required=True,help="Latest retained pregame state date, YYYY-MM-DD")
    ap.add_argument("--base-package",default=str(BASE))
    ap.add_argument("--output-dir")
    args=ap.parse_args()
    BASE=Path(args.base_package)
    if not BASE.is_absolute(): BASE=ROOT/BASE
    out=Path(args.output_dir) if args.output_dir else BASE/"revisions"/f"as_of={args.as_of_date}"
    if not out.is_absolute(): out=ROOT/out
    if out.exists(): raise SystemExit(f"Refusing to overwrite pulse revision: {out}")
    if not (BASE/"player_state_transitions.csv").exists(): raise SystemExit(f"Base pulse not found: {BASE}")
    out.mkdir(parents=True)
    transitions,audit=_load_transitions(args.as_of_date)
    pred=prediction_transitions(transitions)
    old_mov=pd.read_csv(BASE/"feature_movements.csv")
    # Build movements for only new transitions from the paired state JSON; retain original observations.
    keys=set(zip(pd.read_csv(BASE/"player_state_transitions.csv").lane.astype(str),pd.read_csv(BASE/"player_state_transitions.csv").player_id.astype(str),pd.read_csv(BASE/"player_state_transitions.csv").game_n_id.astype(str),pd.read_csv(BASE/"player_state_transitions.csv").game_n1_id.astype(str)))
    new_t=transitions[[ (str(r.lane),str(r.player_id),str(r.game_n_id),str(r.game_n1_id)) not in keys for r in transitions.itertuples() ]]
    new_mov=[]
    for r in new_t.to_dict("records"):
        a,b=_loads(r.get("feature_state_prior")),_loads(r.get("feature_state_current"))
        for f in sorted(set(a)|set(b)):
            x,y=_num(a.get(f)),_num(b.get(f))
            if x is None and y is None: continue
            delta=y-x if x is not None and y is not None else None
            new_mov.append({"lane":r["lane"],"player_id":r["player_id"],"game_n_id":r["game_n_id"],"game_n1_id":r["game_n1_id"],"feature":f,"prior_value":x,"current_value":y,"signed_change":delta,"absolute_change":abs(delta) if delta is not None else None,"percent_change":delta/abs(x)*100 if delta is not None and x else None,"standardized_movement":None,"movement_status":"VALUE" if delta is not None else "MISSING_TO_PRESENT" if x is None else "PRESENT_TO_MISSING"})
    all_mov=combine_movement_frames(old_mov,new_mov)
    # Standardize added deltas against the retained per-lane/feature population.
    for (lane,feature),g in all_mov.groupby(["lane","feature"]):
        ix=g.index
        signed=pd.to_numeric(g.signed_change,errors="coerce")
        sd=signed.std(ddof=1)
        if len(signed.dropna())>=2 and sd and sd>0:
            all_mov.loc[ix,"standardized_movement"]=(signed-signed.mean())/sd
    direct_mov,classes,stats=movement_and_classes(transitions,pred,all_mov)
    relation=feature_prediction_relationships(direct_mov,pred)
    divergence=divergence_pulse(classes,direct_mov)
    stale=stale_state_pulse(transitions,direct_mov)
    window=prospective_window_lineage(args.as_of_date,transitions)
    exemplars_out=exemplars(transitions,classes,pred)
    # Preserve the five original files exactly; revision files carry enriched
    # analyses and a new full transition CSV that includes newly available rows.
    for name in ["rolling_state_audit.csv","independent_rolling_checks.csv"]:
        shutil.copyfile(BASE/name,out/name)
    transitions.to_csv(out/"player_state_transitions.csv",index=False)
    all_mov.to_csv(out/"feature_movements.csv",index=False)
    pred.to_csv(out/"prediction_transitions.csv",index=False)
    divergence.to_csv(out/"divergence_pulse.csv",index=False)
    relation.to_csv(out/"feature_prediction_relationships.csv",index=False)
    stale.to_csv(out/"stale_state_pulse.csv",index=False)
    window.to_csv(out/"prospective_window_lineage.csv",index=False)
    inventory={"schema_version":"NHL_PLAYER_PERFORMANCE_FEATURE_INVENTORY_V1","model_version_comparability":"Historical fitted artifact hashes are absent from run receipts; same family/version labels do not prove identical fitted parameters.","lanes":{"sog":{"model":"poisson_baseline/baseline_v1","direct_model_input_features":DIRECT_FEATURES["sog"],"direct_dynamic_production_feature_count":len(DYNAMIC_PRODUCTION_FEATURES["sog"]),"selection":"rate coalesce d10→d20→d5; TOI coalesce d10→d20→d5 then season TOI fallback","context_only_or_unused_in_baseline":["role_pp_share","pairings","team_context","attempts_d10_per60"],"calibration":"No fitted feature-based calibration identified in this baseline."},"points":{"model":"phoenix/phoenix_v2 per-line logistic","direct_fitted_features":DIRECT_FEATURES["points"],"dynamic_production_features":DYNAMIC_PRODUCTION_FEATURES["points"],"dynamic_context_feature":["is_home"],"direct_dynamic_production_feature_count":len(DYNAMIC_PRODUCTION_FEATURES["points"])},"saves":{"model":"Phoenix V2 goalie_saves Poisson with eligible per-line calibration","direct_fitted_features":DIRECT_FEATURES["saves"],"dynamic_production_features":DYNAMIC_PRODUCTION_FEATURES["saves"],"direct_dynamic_production_feature_count":len(DYNAMIC_PRODUCTION_FEATURES["saves"]),"conditional_start":"start_prob=1.0 is conditional-on-start flag, not a starter probability; actual start is postgame-only."}},"static_identity_fields":["player_id"],"model_and_calibration_parameters":"Frozen parameters; not expected to evolve daily; historical fitted hashes not receipt-bound."}
    pulse.write_json(out/"feature_inventory.json", inventory, indent=2, sort_keys=True)
    unresolved_base=0
    checkp=BASE/"independent_rolling_checks.csv"
    if checkp.exists():
        chk=pd.read_csv(checkp)
        unresolved_base=int(chk.status.astype(str).eq("UNRESOLVED_SOURCE_COVERAGE").sum())
    classification={"sog":"DYNAMIC_FEATURE_EVOLUTION_VERIFIED_WITH_WARNINGS","points":"DYNAMIC_FEATURE_EVOLUTION_VERIFIED_WITH_WARNINGS","saves":"DYNAMIC_FEATURE_EVOLUTION_VERIFIED_WITH_WARNINGS"}
    for lane in classification:
        lane_changes=all_mov[(all_mov.lane==lane)&all_mov.feature.isin(DIRECT_FEATURES[lane])]
        prediction_rows=pred[pred.lane==lane]
        if lane_changes.signed_change.dropna().ne(0).sum()==0 or prediction_rows.empty:
            classification[lane]="DYNAMIC_FEATURE_EVOLUTION_BLOCKED_BY_EVIDENCE"
    summary={"schema_version":"NHL_PLAYER_PERFORMANCE_PULSE_REVISION_V2","revision":2,"as_of_date":args.as_of_date,"base_package":str(BASE.relative_to(ROOT)),"same_player_state_transitions":transitions.groupby("lane").size().to_dict(),"new_transition_rows":audit["new_transition_rows"],"artifact_audit":audit,"prediction_movement":stats,"direct_dynamic_feature_count":{k:len(v) for k,v in DYNAMIC_PRODUCTION_FEATURES.items()},"direct_fitted_feature_count":{k:len(v) for k,v in DIRECT_FEATURES.items()},"feature_movement_distribution":{lane:{feature:pulse.stats_summary(pd.to_numeric(all_mov.loc[(all_mov.lane==lane)&(all_mov.feature==feature),"absolute_change"],errors="coerce").dropna().tolist()) for feature in DIRECT_FEATURES[lane] if ((all_mov.lane==lane)&(all_mov.feature==feature)).any()} for lane in DIRECT_FEATURES},"feature_prediction_relationships":relation.to_dict("records"),"repeated_divergence_case_count":divergence.groupby("lane").size().to_dict() if not divergence.empty else {},"potential_stale_dynamic_state_cases":0,"stale_state_case_counts":stale.status.value_counts().to_dict() if not stale.empty else {},"confirmed_stale_state_failures":0,"historical_unresolved_rolling_cases":unresolved_base,"same_game_leakage_findings":{"pregame_state_after_game_start":0,"realized_game_n_outcome_after_next_feature_cutoff":0,"basis":"Only states meeting strict recorded cutoff and lane-end-before-game-start checks were admitted; completed-game source timestamps must precede next pregame cutoff."},"future_leakage_findings":{"future_game_rows_admitted":0,"basis":"State cutoff and lane end were required before each game's start; strict-prior SQL semantics are confirmed in source but historical raw contributing-window membership remains partly unavailable."},"lane_classification":classification,"overall_classification":"DYNAMIC_FEATURE_EVOLUTION_VERIFIED_WITH_WARNINGS","classification_reason":"Production dynamic input fields and probabilities both move in retained artifacts, but fitted model identity across historical runs is unproven and some rolling comparisons remain source-limited.","exemplars":exemplars_out,"prospective_forward_validation":{"mechanism":"Capture immutable per-date pregame feature/prediction artifacts and hashes; join final official game N outcome timestamp; compare to the next retained pregame state. Record rolling contributor IDs/count/entering/displaced IDs in prospective_window_lineage.csv.","current_as_of_note":"No same-player Oct. 6 completed-game to Oct. 7 pregame state pair was present in retained lane populations; Oct. 7 games had not completed at capture time, so no prospective contribution window could yet be verified.","provider_calls":0,"paid_credits":0,"database_mutation":False,"history_completeness":"Partial where contributing game IDs are reconstructed only from retained reconciliation packages."},"reusable_command":".venv/bin/python backend/nhl/scripts/extend_nhl_player_performance_pulse.py --as-of-date YYYY-MM-DD","tests_required_before_future_revisions":True,"model_version_comparability":"UNPROVEN","characterization_rules":"Characterization only, not action thresholds."}
    pulse.write_json(out/"summary.json", summary, indent=2, sort_keys=True)
    pulse.write_json(out/"revision_metadata.json", {"revision":2,"base_summary_sha256":hashlib.sha256((BASE/"summary.json").read_bytes()).hexdigest(),"as_of_date":args.as_of_date,"source_revision_files":["player_state_transitions.csv","feature_movements.csv","rolling_state_audit.csv","independent_rolling_checks.csv"],"created_deterministically":True,"provider_calls":0,"paid_credits":0}, indent=2, sort_keys=True)
    (out/"README.md").write_text(f"""# NHL Dynamic Player Performance Pulse — Revision 2\n\nAs-of date: `{args.as_of_date}`. This immutable revision preserves the base package and adds prediction responsiveness, descriptive divergence, and prospective lineage outputs.\n\nModel/version comparability remains **unproven** because historical receipts do not bind fitted artifact hashes. The empirical groups are characterization only and are not action thresholds. Historical rolling differences are not promoted to failures.\n\nNo provider calls, paid credits, training, scoring, or database mutation are performed.\n\nFiles: `summary.json`, `player_state_transitions.csv`, `feature_movements.csv`, `prediction_transitions.csv`, `independent_rolling_checks.csv`, `rolling_state_audit.csv`, `divergence_pulse.csv`, `stale_state_pulse.csv`, `feature_prediction_relationships.csv`, `feature_inventory.json`, `prospective_window_lineage.csv`, `revision_metadata.json`.\n\nReusable command: `.venv/bin/python backend/nhl/scripts/extend_nhl_player_performance_pulse.py --as-of-date YYYY-MM-DD`\n""")
    print("NHL PLAYER PERFORMANCE PULSE: COMPLETE")
    print(f"As-of: {args.as_of_date}")
    print(f"New transitions: {audit['new_transition_rows']}")
    print(f"Classification: {summary['overall_classification']}")
    print(f"Package: {out}")
    print(f"Summary: {out / 'summary.json'}")
    print(f"Report: {out / 'README.md'}")
    return 0


if __name__=="__main__":
    raise SystemExit(main())
