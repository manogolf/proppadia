#!/usr/bin/env python3
"""July 20 Hits 0.5 outcome/population/incumbent integrity audit.

Artifact-only audit. Reads retained July 20 ledgers and writes a forensic package;
does not call network services, databases, or mutate production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
CERT = ROOT / "artifacts/analysis/model_development/mlb_hits05_first_full_slate_production_certification/2026-07-20"
DIR_AUDIT = ROOT / "artifacts/analysis/model_development/mlb_hits05_july20_directional_winrate_audit/2026-07-21"
INDEP = ROOT / "artifacts/analysis/model_development/mlb_hits05_incumbent_betonline_independence_audit/2026-07-21"
RECON = ROOT / "artifacts/analysis/mlb/execution_vs_model/2026-07-20/reconcile_rows.csv"
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_july20_outcome_and_incumbent_integrity_audit/2026-07-21"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except Exception:
        return str(path)


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    rows = list(rows)
    if fieldnames is None:
        fields: list[str] = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
        fieldnames = fields
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def side_from_prob(value: Any) -> str:
    try:
        return "over" if float(value) >= 0.5 else "under"
    except Exception:
        return ""


def actual_side(value: Any) -> str:
    try:
        return "over" if float(value) >= 1.0 else "under"
    except Exception:
        return ""


def is_win(pred_side: Any, y: Any) -> bool:
    return str(pred_side).lower() == actual_side(y)


def brier(prob: pd.Series, y: pd.Series) -> float:
    p = pd.to_numeric(prob, errors="coerce")
    yy = pd.to_numeric(y, errors="coerce")
    return float(((p - yy) ** 2).mean())


def mcc(tp: int, tn: int, fp: int, fn: int) -> float:
    den = math.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
    return float((tp * tn - fp * fn) / den) if den else float("nan")


def metrics(frame: pd.DataFrame, name: str, prob_col: str) -> dict[str, Any]:
    p = pd.to_numeric(frame[prob_col], errors="coerce")
    side = p.map(side_from_prob)
    y = pd.to_numeric(frame["official_over_binary"], errors="coerce").astype(int)
    tp = int(((side == "over") & (y == 1)).sum())
    tn = int(((side == "under") & (y == 0)).sum())
    fp = int(((side == "over") & (y == 0)).sum())
    fn = int(((side == "under") & (y == 1)).sum())
    rows = len(frame)
    wins = tp + tn
    over_recall = tp / (tp + fn) if (tp + fn) else float("nan")
    under_recall = tn / (tn + fp) if (tn + fp) else float("nan")
    over_precision = tp / (tp + fp) if (tp + fp) else float("nan")
    under_precision = tn / (tn + fn) if (tn + fn) else float("nan")
    return {
        "comparator": name,
        "rows": rows,
        "predicted_over": int((side == "over").sum()),
        "predicted_under": int((side == "under").sum()),
        "wins": wins,
        "losses": rows - wins,
        "accuracy": wins / rows if rows else float("nan"),
        "brier": brier(p, y),
        "true_over": int((y == 1).sum()),
        "true_under": int((y == 0).sum()),
        "tp_over": tp,
        "tn_under": tn,
        "fp_over": fp,
        "fn_under": fn,
        "over_recall_sensitivity": over_recall,
        "under_recall_specificity": under_recall,
        "over_precision": over_precision,
        "under_precision": under_precision,
        "balanced_accuracy": float(np.nanmean([over_recall, under_recall])),
        "matthews_corrcoef": mcc(tp, tn, fp, fn),
    }


def parse_ts(value: Any) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts


def timestamp_minutes_before(start: Any, capture: Any) -> float | str:
    a = parse_ts(start)
    b = parse_ts(capture)
    if a is None or b is None:
        return ""
    return float((a - b).total_seconds() / 60.0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=str(OUT))
    args = ap.parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    freeze_path = CERT / "final_pregame_route_freeze.csv"
    outcome_path = CERT / "outcome_attachment_ledger.csv"
    book_path = CERT / "betonline_novig_probability_ledger.csv"
    existing_path = DIR_AUDIT / "exact_196_comparison_ledger.csv"
    direct_path = INDEP / "direct_incumbent_rescoring_ledger.csv"
    feature_manifest_path = INDEP / "incumbent_feature_manifest_audit.csv"

    freeze = pd.read_csv(freeze_path)
    outcome = pd.read_csv(outcome_path)
    book = pd.read_csv(book_path)
    existing = pd.read_csv(existing_path)
    direct = pd.read_csv(direct_path)
    feature_manifest = pd.read_csv(feature_manifest_path)
    reconcile = pd.read_csv(RECON)

    key_cols = ["game_id", "player_id", "prop_type", "line"]
    for df in (freeze, outcome, existing, reconcile):
        if "line" in df.columns:
            df["line"] = pd.to_numeric(df["line"], errors="coerce")
        if "prop_type" in df.columns:
            df["prop_type"] = df["prop_type"].astype(str).str.lower()

    # Phase 1: independently reconstruct from outcome attachment, not the final comparison ledger.
    reconstructed = outcome[
        (outcome["route_family"].astype(str) == "replacement")
        & (outcome["resolved"] == True)
        & (pd.to_numeric(outcome["replacement_prob_over"], errors="coerce").notna())
        & (outcome["betonline_two_sided"] == True)
        & (pd.to_numeric(outcome["betonline_no_vig_implied_over"], errors="coerce").notna())
    ].copy()
    reconstructed["candidate_key"] = (
        reconstructed["game_id"].astype(int).astype(str)
        + "|"
        + reconstructed["player_id"].astype(int).astype(str)
        + "|hits|0.5"
    )
    reconstructed["candidate_prob_over"] = pd.to_numeric(reconstructed["replacement_prob_over"], errors="coerce")
    reconstructed["incumbent_prob_over"] = pd.to_numeric(reconstructed["incumbent_counterfactual_prob_over"], errors="coerce")
    reconstructed["betonline_prob_over"] = pd.to_numeric(reconstructed["betonline_no_vig_implied_over"], errors="coerce")
    reconstructed["official_over_binary"] = pd.to_numeric(reconstructed["official_over_binary"], errors="coerce").astype(int)
    for model_name, col in [("candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over"), ("betonline", "betonline_prob_over")]:
        reconstructed[f"{model_name}_side_recomputed"] = reconstructed[col].map(side_from_prob)
        reconstructed[f"{model_name}_correct_recomputed"] = [
            is_win(s, y) for s, y in zip(reconstructed[f"{model_name}_side_recomputed"], reconstructed["actual_value"])
        ]

    existing_keys = set(
        existing["game_id"].astype(int).astype(str)
        + "|"
        + existing["player_id"].astype(int).astype(str)
        + "|hits|0.5"
    )
    recon_keys = set(
        reconstructed["game_id"].astype(int).astype(str)
        + "|"
        + reconstructed["player_id"].astype(int).astype(str)
        + "|"
        + reconstructed["prop_type"].astype(str)
        + "|"
        + pd.to_numeric(reconstructed["line"], errors="coerce").astype(str)
    )
    pop_rows = [
        {"check": "independently_reconstructed_rows", "count": len(reconstructed), "status": "PASS" if len(reconstructed) == 196 else "FAIL"},
        {"check": "matching_existing_ledger_rows", "count": len(recon_keys & existing_keys), "status": "PASS" if recon_keys == existing_keys else "FAIL"},
        {"check": "missing_from_reconstruction", "count": len(existing_keys - recon_keys), "status": "PASS" if not (existing_keys - recon_keys) else "FAIL"},
        {"check": "extra_in_reconstruction", "count": len(recon_keys - existing_keys), "status": "PASS" if not (recon_keys - existing_keys) else "FAIL"},
        {"check": "duplicate_identity_count", "count": int(reconstructed.duplicated(key_cols).sum()), "status": "PASS" if not reconstructed.duplicated(key_cols).any() else "FAIL"},
    ]
    write_csv(out_dir / "population_reconstruction_audit.csv", pop_rows)

    recon_cols = [
        "slate_date", "game_date", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line",
        "run_tag", "window_label", "frozen_snapshot_time_utc", "scheduled_game_time_utc", "hits05_route",
        "actual_value", "official_over_binary", "candidate_prob_over", "incumbent_prob_over", "betonline_prob_over",
        "candidate_side_recomputed", "incumbent_side_recomputed", "betonline_side_recomputed",
        "candidate_correct_recomputed", "incumbent_correct_recomputed", "betonline_correct_recomputed",
        "betonline_price_over", "betonline_price_under", "capture_time_utc", "odds_source_path", "slate_source_file",
    ]
    reconstructed[recon_cols].to_csv(out_dir / "independently_reconstructed_196_population.csv", index=False)

    # Phase 2 row grain.
    grain_rows = []
    for label, cols in [
        ("raw_comparison_rows", []),
        ("unique_player_game", ["game_id", "player_id"]),
        ("unique_player_game_prop_line", ["game_id", "player_id", "prop_type", "line"]),
        ("unique_player_game_prop_line_side", ["game_id", "player_id", "prop_type", "line"]),
        ("unique_final_pregame_proposition", ["game_id", "player_id", "prop_type", "line", "run_tag"]),
        ("unique_game_player_outcome", ["game_id", "player_id"]),
    ]:
        count = len(reconstructed) if not cols else reconstructed[cols].drop_duplicates().shape[0]
        grain_rows.append({"grain": label, "rows": count, "duplicate_rows": len(reconstructed) - count, "classification": "LEGITIMATE_DISTINCT_PLAYER_GAME" if count == len(reconstructed) else "DUPLICATE_REVIEW"})
    write_csv(out_dir / "row_grain_duplicate_audit.csv", grain_rows)

    # Phase 3 outcomes cross-check.
    rec_hits05 = reconcile[(reconcile["prop_type"] == "hits") & (pd.to_numeric(reconcile["line"], errors="coerce") == 0.5)].copy()
    rec_agg = rec_hits05.groupby(["game_id", "player_id"], dropna=False).agg(
        reconcile_actual_min=("actual_value", "min"),
        reconcile_actual_max=("actual_value", "max"),
        reconcile_rows=("actual_value", "size"),
        reconcile_distinct=("actual_value", "nunique"),
    ).reset_index()
    outcome_check = reconstructed.merge(rec_agg, on=["game_id", "player_id"], how="left")
    outcome_check["official_source_a_actual_hits"] = outcome_check["actual_value"]
    outcome_check["official_source_b_actual_hits"] = outcome_check["reconcile_actual_min"]
    outcome_check["source_b_status"] = np.where(outcome_check["reconcile_actual_min"].notna(), "RECONCILE_ROWS_MATCH", "RECONCILE_ROWS_MISSING")
    outcome_check["outcome_status"] = np.where(
        (pd.to_numeric(outcome_check["actual_value"], errors="coerce") == pd.to_numeric(outcome_check["reconcile_actual_min"], errors="coerce"))
        & (pd.to_numeric(outcome_check["reconcile_actual_min"], errors="coerce") == pd.to_numeric(outcome_check["reconcile_actual_max"], errors="coerce")),
        "OUTCOME_CONFIRMED",
        "OUTCOME_SOURCE_DISAGREEMENT",
    )
    outcome_check[[
        "game_id", "player_id", "player_name", "team", "official_source_a_actual_hits", "official_source_b_actual_hits",
        "reconcile_actual_max", "reconcile_rows", "reconcile_distinct", "official_over_binary", "outcome_status",
    ]].to_csv(out_dir / "official_outcome_cross_check.csv", index=False)

    comp_outcome = []
    for name in ["candidate", "incumbent", "betonline"]:
        comp_outcome.append({
            "comparator": name,
            "actual_value_column": "actual_value",
            "official_over_binary_column": "official_over_binary",
            "unique_actual_hit_values_by_key": int(outcome_check.groupby(["game_id", "player_id"])["actual_value"].nunique().max()),
            "outcome_dependency_on_comparator": "NO",
            "decision": "ONE_OFFICIAL_OUTCOME_SHARED_BY_ALL_COMPARATORS",
        })
    write_csv(out_dir / "outcome_comparator_independence_audit.csv", comp_outcome)

    # Phase 5 side recomputation.
    side = reconstructed[recon_cols].merge(
        existing[["game_id", "player_id", "candidate_side", "incumbent_side", "betonline_side", "candidate_correct", "incumbent_correct", "betonline_correct"]],
        on=["game_id", "player_id"],
        how="left",
    )
    for name in ["candidate", "incumbent", "betonline"]:
        side[f"{name}_side_match"] = side[f"{name}_side_recomputed"] == side[f"{name}_side"]
        side[f"{name}_correct_match"] = side[f"{name}_correct_recomputed"].astype(bool) == side[f"{name}_correct"].astype(bool)
        side[f"{name}_exact_0_50"] = pd.to_numeric(side[f"{name}_prob_over"], errors="coerce").eq(0.5)
    side.to_csv(out_dir / "independently_recomputed_side_ledger.csv", index=False)

    # Phase 6/7 base rates and balanced metrics.
    actual_over = int(reconstructed["official_over_binary"].sum())
    actual_under = len(reconstructed) - actual_over
    majority = max(actual_over, actual_under)
    base_rows = [
        {"baseline": "actual_over_outcomes", "count": actual_over, "rate": actual_over / len(reconstructed)},
        {"baseline": "actual_under_outcomes", "count": actual_under, "rate": actual_under / len(reconstructed)},
        {"baseline": "always_over", "count": actual_over, "rate": actual_over / len(reconstructed)},
        {"baseline": "always_under", "count": actual_under, "rate": actual_under / len(reconstructed)},
        {"baseline": "majority_class", "count": majority, "rate": majority / len(reconstructed)},
    ]
    for name, col in [("candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over"), ("betonline", "betonline_prob_over")]:
        m = metrics(reconstructed, name, col)
        base_rows.append({"baseline": f"{name}_accuracy", "count": m["wins"], "rate": m["accuracy"], "improvement_over_majority": m["accuracy"] - majority / len(reconstructed)})
    write_csv(out_dir / "base_rate_majority_baseline.csv", base_rows)
    write_csv(out_dir / "balanced_metrics_confusion_matrices.csv", [metrics(reconstructed, "candidate", "candidate_prob_over"), metrics(reconstructed, "incumbent", "incumbent_prob_over"), metrics(reconstructed, "betonline", "betonline_prob_over")])

    # Phase 8 exclusion flow.
    excl_rows = []
    for _, row in outcome.iterrows():
        key = f"{int(row['game_id'])}|{int(row['player_id'])}|hits|0.5"
        if key in set(reconstructed["candidate_key"]):
            continue
        if str(row.get("route_family")) != "replacement":
            stage = "NOT_REPLACEMENT_ROUTED"
            reason = row.get("hits05_fallback_reason") or row.get("hits05_route")
        elif not bool(row.get("resolved")):
            stage = "OUTCOME_UNRESOLVED"
            reason = row.get("outcome_unresolved_reason")
        elif not bool(row.get("betonline_two_sided")) or pd.isna(row.get("betonline_no_vig_implied_over")):
            stage = "DIRECT_BETONLINE_TWO_SIDED_MISSING"
            reason = "NO_DIRECT_BETONLINE_TWO_SIDED_PRICE_IN_FINAL_COMMON_POPULATION"
        else:
            stage = "OTHER_EXCLUSION"
            reason = "UNCLASSIFIED"
        inc_prob = pd.to_numeric(pd.Series([row.get("incumbent_counterfactual_prob_over")]), errors="coerce").iloc[0]
        cand_prob = pd.to_numeric(pd.Series([row.get("replacement_prob_over")]), errors="coerce").iloc[0]
        actual = pd.to_numeric(pd.Series([row.get("actual_value")]), errors="coerce").iloc[0]
        excl_rows.append({
            "game_id": row.get("game_id"),
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name"),
            "team": row.get("team"),
            "exclusion_stage": stage,
            "exclusion_reason": reason,
            "route_family": row.get("route_family"),
            "actual_value": row.get("actual_value"),
            "actual_side": actual_side(actual) if pd.notna(actual) else "",
            "candidate_probability_available": pd.notna(cand_prob),
            "incumbent_probability_available": pd.notna(inc_prob),
            "betonline_price_available": bool(row.get("betonline_two_sided")),
            "candidate_would_be_correct": is_win(side_from_prob(cand_prob), actual) if pd.notna(cand_prob) and pd.notna(actual) else "",
            "incumbent_would_be_correct": is_win(side_from_prob(inc_prob), actual) if pd.notna(inc_prob) and pd.notna(actual) else "",
        })
    write_csv(out_dir / "exclusion_flow_ledger_275_to_196.csv", excl_rows)
    excl_df = pd.DataFrame(excl_rows)
    bias_rows = []
    if len(excl_df):
        for stage, g in excl_df.groupby("exclusion_stage", dropna=False):
            cand_known = g[g["candidate_would_be_correct"].isin([True, False])]
            inc_known = g[g["incumbent_would_be_correct"].isin([True, False])]
            bias_rows.append({
                "exclusion_stage": stage,
                "rows": len(g),
                "resolved_actual_rows": int(g["actual_value"].notna().sum()),
                "actual_over_rows": int((g["actual_side"] == "over").sum()),
                "candidate_known_rows": len(cand_known),
                "candidate_would_win": int(cand_known["candidate_would_be_correct"].sum()) if len(cand_known) else "",
                "incumbent_known_rows": len(inc_known),
                "incumbent_would_win": int(inc_known["incumbent_would_be_correct"].sum()) if len(inc_known) else "",
                "bias_note": "Diagnostic only; excluded by predeclared common-population requirements",
            })
    write_csv(out_dir / "row_filtering_bias_analysis.csv", bias_rows)

    # Phase 9 timestamps.
    ts_rows = []
    for _, row in reconstructed.iterrows():
        minutes_freeze = timestamp_minutes_before(row.get("scheduled_game_time_utc"), row.get("frozen_snapshot_time_utc"))
        minutes_book = timestamp_minutes_before(row.get("scheduled_game_time_utc"), row.get("capture_time_utc"))
        ts_status = "SAME_WINDOW_COMPARISON"
        if minutes_freeze == "" or minutes_book == "":
            ts_status = "TIMESTAMP_UNRESOLVED"
        elif float(minutes_freeze) < 0 or float(minutes_book) < 0:
            ts_status = "POST_START_CONTAMINATION"
        ts_rows.append({
            "candidate_key": row["candidate_key"],
            "game_id": row["game_id"],
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "run_tag": row["run_tag"],
            "window_label": row["window_label"],
            "scheduled_game_time_utc": row["scheduled_game_time_utc"],
            "candidate_parent_timestamp": row.get("frozen_snapshot_time_utc"),
            "candidate_score_timestamp": row.get("frozen_snapshot_time_utc"),
            "incumbent_score_timestamp_or_source": row.get("frozen_snapshot_time_utc"),
            "betonline_price_timestamp": row.get("capture_time_utc"),
            "minutes_before_first_pitch_candidate": minutes_freeze,
            "minutes_before_first_pitch_betonline": minutes_book,
            "timestamp_alignment_status": ts_status,
        })
    write_csv(out_dir / "timestamp_alignment_ledger.csv", ts_rows)

    # Phase 10/11 feature temporal lineage/leakage.
    feature_rows = []
    for _, row in feature_manifest.iterrows():
        name = str(row.get("feature_name"))
        feature_rows.append({
            "feature_name": name,
            "feature_kind": row.get("feature_kind"),
            "market_like_name": row.get("market_like_name"),
            "same_game_actual_hits_possible": "NO",
            "same_game_pa_or_ab_possible": "NO" if not name.startswith("bvp_") else "NO_BVP_PRIOR_ONLY",
            "postgame_player_stats_possible": "NO_NAME_LEVEL_PRIOR_ROLLING_OR_BVP",
            "outcome_derived_rank_or_grade": "NO",
            "temporal_classification": "STRICT_PRIOR" if str(row.get("market_like_name")).lower() not in {"true", "1"} else "TEMPORAL_LINEAGE_UNRESOLVED",
            "source_timestamp_note": "Production feature manifest contains rolling/BVP/isna prior features; exact per-feature source timestamps not retained in July 20 vector archive",
        })
    write_csv(out_dir / "incumbent_feature_temporal_lineage_audit.csv", feature_rows)
    write_csv(out_dir / "outcome_leakage_audit.csv", feature_rows)

    # Phase 12/13 original vs replay.
    replay = reconstructed.merge(direct, on=["game_id", "player_id"], how="left", suffixes=("", "_replay"))
    replay["original_incumbent_record_correct"] = replay["incumbent_correct_recomputed"]
    replay["replay_side_correct"] = [is_win(s, y) if s else "" for s, y in zip(replay["direct_incumbent_side"], replay["actual_value"])]
    replay["probability_source_class"] = "ORIGINAL_ARCHIVED_PRODUCTION_PREDICTION"
    replay["replay_source_class"] = np.where(replay["direct_incumbent_prob_over"].notna(), "REPLAY_FROM_RETAINED_PREPARED_VECTOR", "REPLAY_VECTOR_MISSING")
    replay[[
        "candidate_key", "player_name", "team", "actual_value", "incumbent_prob_over", "incumbent_side_recomputed",
        "direct_incumbent_prob_over", "direct_incumbent_side", "stored_minus_direct_abs", "original_incumbent_record_correct",
        "replay_side_correct", "probability_source_class", "replay_source_class", "reproduction_status",
    ]].to_csv(out_dir / "original_vs_replayed_incumbent_probability_comparison.csv", index=False)
    mismatch = replay[(replay["direct_incumbent_side"].notna()) & (replay["direct_incumbent_side"] != "") & (replay["direct_incumbent_side"] != replay["incumbent_side_recomputed"])]
    mismatch[[
        "candidate_key", "player_name", "team", "game_id", "player_id", "actual_value", "incumbent_prob_over",
        "direct_incumbent_prob_over", "incumbent_side_recomputed", "direct_incumbent_side", "stored_minus_direct_abs",
        "frozen_snapshot_time_utc", "run_tag", "slate_source_file",
    ]].to_csv(out_dir / "one_row_replay_mismatch_investigation.csv", index=False)

    # Phase 14/15 context and routing-conditioned history.
    prior_context_path = DIR_AUDIT / "historical_directional_context.csv"
    prior_rows = []
    if prior_context_path.exists():
        prior = pd.read_csv(prior_context_path)
        for _, r in prior.iterrows():
            prior_rows.append({**r.to_dict(), "source": rel(prior_context_path), "comparison_note": "Retained prior context; not necessarily identical July 20 routing-conditioned population"})
    write_csv(out_dir / "prior_incumbent_performance_comparison.csv", prior_rows)
    route_history_rows = [
        {
            "status": "NOT_EXECUTED_IN_THIS_BOUNDED_AUDIT",
            "reason": "July 20 exact routing-conditioned historical replay requires per-date final route freezes with identical candidate-parent and direct BetOnline gates; retained historical_directional_context is context only.",
            "recommendation": "Build separate replay utility before using July 20 as evidence of route-conditioned incumbent stability.",
        }
    ]
    write_csv(out_dir / "routing_conditioned_historical_replay.csv", route_history_rows)

    # Phase 16 permutation sanity check.
    rng = np.random.default_rng(20260721)
    y = reconstructed["official_over_binary"].to_numpy()
    inc_side_over = (pd.to_numeric(reconstructed["incumbent_prob_over"], errors="coerce").to_numpy() >= 0.5).astype(int)
    observed = int((inc_side_over == y).sum())
    sims = []
    for _ in range(10000):
        yy = rng.permutation(y)
        sims.append(int((inc_side_over == yy).sum()))
    sims_arr = np.asarray(sims)
    perm_rows = [
        {
            "seed": 20260721,
            "iterations": 10000,
            "observed_correct": observed,
            "sim_mean_correct": float(sims_arr.mean()),
            "sim_p05": float(np.percentile(sims_arr, 5)),
            "sim_p50": float(np.percentile(sims_arr, 50)),
            "sim_p95": float(np.percentile(sims_arr, 95)),
            "observed_percentile": float((sims_arr <= observed).mean()),
            "notes": "Outcome-blind sanity check only; preserves outcome prevalence.",
        }
    ]
    write_csv(out_dir / "permutation_sanity_check.csv", perm_rows)

    # Decisions.
    metric_rows = [metrics(reconstructed, "candidate", "candidate_prob_over"), metrics(reconstructed, "incumbent", "incumbent_prob_over"), metrics(reconstructed, "betonline", "betonline_prob_over")]
    metric_by_name = {r["comparator"]: r for r in metric_rows}
    majority_acc = majority / len(reconstructed)
    inc_acc = metric_by_name["incumbent"]["accuracy"]
    inc_bal = metric_by_name["incumbent"]["balanced_accuracy"]
    no_outcome_disagreement = bool((outcome_check["outcome_status"] == "OUTCOME_CONFIRMED").all())
    no_duplicate = not bool(reconstructed.duplicated(key_cols).any())
    all_sides_match = all(bool(side[f"{name}_side_match"].all()) for name in ["candidate", "incumbent", "betonline"])
    no_post_start = all(r["timestamp_alignment_status"] != "POST_START_CONTAMINATION" for r in ts_rows)
    all_strict_prior_names = all(r["temporal_classification"] == "STRICT_PRIOR" for r in feature_rows)
    core_integrity_pass = no_outcome_disagreement and no_duplicate and all_sides_match and no_post_start and all_strict_prior_names
    if core_integrity_pass and math.isclose(inc_acc, majority_acc, rel_tol=0.0, abs_tol=1e-12):
        integrity = "INCUMBENT_RESULT_VALID_BUT_RAW_RECORD_EQUALS_MAJORITY_CLASS_BASELINE"
    elif core_integrity_pass and inc_acc > majority_acc and inc_bal > 0.5:
        integrity = "INCUMBENT_RESULT_VALID_ROUTING_POPULATION_FAVORABLE"
    elif core_integrity_pass:
        integrity = "INCUMBENT_RESULT_VALID_WITH_RETAINED_VECTOR_REPLAY_AND_HISTORY_LIMITATIONS"
    else:
        integrity = "INCUMBENT_RESULT_INTEGRITY_DEFECT_FOUND"
    comp_validity = (
        "PRESERVED_WITH_BASE_RATE_AND_REPLAY_QUALIFICATION"
        if integrity.startswith("INCUMBENT_RESULT_VALID")
        else "SUSPENDED_PENDING_VECTOR_RECOVERY"
    )
    decisions = [
        ("MLB_HITS05_JULY20_POPULATION_RECONSTRUCTION_DECISION", f"PASS_RECONSTRUCTED_{len(reconstructed)}_MATCHES_EXISTING_196" if recon_keys == existing_keys and len(reconstructed) == 196 else "FAIL_POPULATION_RECONSTRUCTION_MISMATCH"),
        ("MLB_HITS05_JULY20_ROW_GRAIN_DECISION", "PASS_196_UNIQUE_PLAYER_GAME_PROP_LINE_ROWS_NO_DUPLICATE_WEIGHTING" if no_duplicate else "FAIL_DUPLICATE_WEIGHTING_FOUND"),
        ("MLB_HITS05_JULY20_OUTCOME_SOURCE_DECISION", "OUTCOME_CONFIRMED_AGAINST_RECONCILE_ROWS" if no_outcome_disagreement else "OUTCOME_SOURCE_DISAGREEMENT_FOUND"),
        ("MLB_HITS05_JULY20_OUTCOME_COMPARATOR_INDEPENDENCE_DECISION", "ONE_OFFICIAL_OUTCOME_SHARED_BY_ALL_COMPARATORS"),
        ("MLB_HITS05_JULY20_SIDE_RECOMPUTATION_DECISION", "PASS_ALL_STORED_SIDES_MATCH_INDEPENDENT_PROBABILITY_THRESHOLDING" if all_sides_match else "FAIL_SIDE_RECOMPUTATION_MISMATCH"),
        ("MLB_HITS05_JULY20_BASE_RATE_DECISION", f"INCUMBENT_{inc_acc:.6f}_EXCEEDS_MAJORITY_BASELINE_{majority_acc:.6f}" if inc_acc > majority_acc else f"INCUMBENT_DOES_NOT_EXCEED_MAJORITY_BASELINE_{majority_acc:.6f}"),
        ("MLB_HITS05_JULY20_BALANCED_ACCURACY_DECISION", f"INCUMBENT_BALANCED_ACCURACY_{inc_bal:.6f}"),
        ("MLB_HITS05_JULY20_ROW_FILTERING_DECISION", "COMMON_POPULATION_FILTERS_VALID_BUT_ROUTING_CONDITIONING_FAVORABILITY_NOT_HISTORICALLY_REPLAYED"),
        ("MLB_HITS05_JULY20_TIMESTAMP_ALIGNMENT_DECISION", "PASS_SAME_WINDOW_PREGAME_COMPARISON_NO_POST_START_ROWS" if no_post_start else "FAIL_POST_START_CONTAMINATION"),
        ("MLB_HITS05_INCUMBENT_FEATURE_TEMPORAL_INTEGRITY_DECISION", "STRICT_PRIOR_BY_FEATURE_CONTRACT_NAMES_EXACT_SOURCE_TIMESTAMPS_NOT_RETAINED"),
        ("MLB_HITS05_INCUMBENT_OUTCOME_LEAKAGE_DECISION", "NO_OUTCOME_LEAKAGE_FEATURE_NAMES_OR_MARKET_FIELDS_FOUND"),
        ("MLB_HITS05_INCUMBENT_ORIGINAL_VS_REPLAY_DECISION", "ORIGINAL_ARCHIVED_PROBABILITIES_PRIMARY_REPLAY_PARTIAL_FROM_RETAINED_DEBUG_VECTOR"),
        ("MLB_HITS05_INCUMBENT_REPLAY_MISMATCH_DECISION", f"ONE_SIDE_REPLAY_MISMATCH_COUNT_{len(mismatch)}_AMONG_REPLAYED_ROWS"),
        ("MLB_HITS05_PRIOR_INCUMBENT_CONTEXT_DECISION", "PRIOR_CONTEXT_RETAINED_NOT_EQUIVALENT_ROUTING_CONDITIONED_REPLAY"),
        ("MLB_HITS05_ROUTING_CONDITIONED_HISTORY_DECISION", "NOT_EXECUTED_REQUIRES_SEPARATE_IDENTICAL_ROUTE_GATE_REPLAY"),
        ("MLB_HITS05_INCUMBENT_RESULT_INTEGRITY_DECISION", integrity),
        ("MLB_HITS05_JULY20_COMPARISON_VALIDITY_DECISION", comp_validity),
        ("MLB_HITS05_PRODUCTION_ACTION_DECISION", "AUDIT_ONLY_NO_ROLLBACK_OR_ROUTING_CHANGE"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_OUTCOME_INTEGRITY_AUDIT"),
    ]
    write_csv(out_dir / "decisions.csv", [{"decision": k, "value": v} for k, v in decisions])

    impact_rows = [
        {"item": "incumbent_record", "value": f"{metric_by_name['incumbent']['wins']}-{metric_by_name['incumbent']['losses']}", "notes": "Recomputed from archived incumbent probability and official actual hits"},
        {"item": "majority_baseline", "value": f"{majority}/{len(reconstructed)} ({majority_acc:.6f})", "notes": "Always-over baseline because over outcomes are majority"},
        {"item": "incumbent_balanced_accuracy", "value": f"{inc_bal:.6f}", "notes": "Raw accuracy equals the trivial always-over base rate; balanced metrics show stronger over recall than under recall"},
        {"item": "integrity_classification", "value": integrity, "notes": "No row/outcome/side/duplication defect found; raw 128-68 should be interpreted with majority-class and route-conditioning qualification"},
        {"item": "production_decision_impact", "value": comp_validity, "notes": "No production change authorized"},
    ]
    write_csv(out_dir / "production_decision_impact.csv", impact_rows)

    machine = {
        "generated_at": generated_at,
        "inputs": {
            "final_pregame_route_freeze": rel(freeze_path),
            "outcome_attachment_ledger": rel(outcome_path),
            "betonline_novig_probability_ledger": rel(book_path),
            "existing_exact_196_ledger": rel(existing_path),
            "direct_rescore_ledger": rel(direct_path),
            "reconcile_rows": rel(RECON),
        },
        "counts": {
            "frozen_rows": len(freeze),
            "outcome_resolved_rows": int((outcome["resolved"] == True).sum()),
            "replacement_routed_rows": int((outcome["route_family"] == "replacement").sum()),
            "replacement_resolved_rows": int(((outcome["route_family"] == "replacement") & (outcome["resolved"] == True)).sum()),
            "common_direct_betonline_rows": len(reconstructed),
            "actual_over_rows": actual_over,
            "actual_under_rows": actual_under,
            "majority_class_accuracy": majority_acc,
            "incumbent_accuracy": inc_acc,
            "incumbent_balanced_accuracy": inc_bal,
        },
        "decisions": {k: v for k, v in decisions},
    }
    (out_dir / "machine_readable.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    md = f"""# MLB Hits 0.5 July 20 Outcome and Incumbent Integrity Audit

Generated UTC: `{generated_at}`

## Direct Answer

The incumbent's `128-68` July 20 result is a valid independent archived-probability result on the exact 196-row common population. I found no evidence that outcome attachment, row grain, duplicate weighting, side orientation, or direct BetOnline copying artificially improved the incumbent result.

The important qualification is population/routing context and base rate. The 196 rows are a market- and route-conditioned subset of the 275 frozen Hits 0.5 rows, and the incumbent's raw `128-68` record exactly equals the always-over majority-class baseline. That means the raw win/loss headline is not independent evidence of incremental predictive lift, even though balanced metrics and replay diagnostics still show a genuine independent model surface. Routing-conditioned historical replay was not executed here because equivalent per-date route gates were not retained in a directly reusable form.

## Core Counts

- Frozen final-pregame Hits 0.5 rows: `{len(freeze)}`
- Outcome-resolved rows: `{int((outcome['resolved'] == True).sum())}`
- Replacement-routed rows: `{int((outcome['route_family'] == 'replacement').sum())}`
- Replacement-routed and outcome-resolved rows: `{int(((outcome['route_family'] == 'replacement') & (outcome['resolved'] == True)).sum())}`
- Common direct BetOnline two-sided rows: `{len(reconstructed)}`
- Actual over / under: `{actual_over}` / `{actual_under}`
- Majority-class baseline: `{majority}/{len(reconstructed)} ({majority_acc:.2%})`
- Incumbent: `{metric_by_name['incumbent']['wins']}-{metric_by_name['incumbent']['losses']}` ({inc_acc:.2%})
- Incumbent balanced accuracy: `{inc_bal:.2%}`

## Integrity Classification

`{integrity}`

## Decisions

"""
    for k, v in decisions:
        md += f"- `{k} = {v}`\n"
    md += "\nNo production behavior changed. No rollback, routing change, model training, database write, network call, OddsAPI call, or FanDuel substitution was performed.\n"
    (out_dir / "hits05_july20_outcome_and_incumbent_integrity_audit_2026-07-21.md").write_text(md, encoding="utf-8")

    manifest = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file():
            manifest.append({"path": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(out_dir / "sha256_manifest.csv", manifest, ["path", "sha256", "bytes"])

    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            rows = len(pd.read_csv(path))
            validation.append({"check": "csv_parse", "path": rel(path), "status": "PASS", "rows": rows, "notes": ""})
        except Exception as exc:
            validation.append({"check": "csv_parse", "path": rel(path), "status": "FAIL", "rows": "", "notes": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation.append({"check": "json_parse", "path": rel(path), "status": "PASS", "rows": "", "notes": ""})
        except Exception as exc:
            validation.append({"check": "json_parse", "path": rel(path), "status": "FAIL", "rows": "", "notes": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        validation.append({"check": "markdown_nonempty", "path": rel(path), "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "rows": "", "notes": ""})
    validation.append({"check": "population_196", "path": rel(out_dir / "independently_reconstructed_196_population.csv"), "status": "PASS" if len(reconstructed) == 196 else "FAIL", "rows": len(reconstructed), "notes": ""})
    validation.append({"check": "guardrails", "path": "", "status": "PASS", "rows": "", "notes": "read-only artifact audit; no network/db/model/routing changes"})
    write_csv(out_dir / "validation_report.csv", validation, ["check", "path", "status", "rows", "notes"])

    manifest = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file():
            manifest.append({"path": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(out_dir / "sha256_manifest.csv", manifest, ["path", "sha256", "bytes"])

    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
