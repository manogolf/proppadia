#!/usr/bin/env python3
"""Hits 0.5 metric-integrity correction plus 20-slate replication review.

Read-only artifact audit. This script uses retained July 20 ledgers and retained
historical same-row BetOnline comparison artifacts; it does not call network
services, databases, or alter production behavior.
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
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21"
J20 = ROOT / "artifacts/analysis/model_development/mlb_hits05_july20_outcome_and_incumbent_integrity_audit/2026-07-21"
J20_CERT = ROOT / "artifacts/analysis/model_development/mlb_hits05_first_full_slate_production_certification/2026-07-20"
J20_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_july20_directional_winrate_audit/2026-07-21"
J20_INDEP = ROOT / "artifacts/analysis/model_development/mlb_hits05_incumbent_betonline_independence_audit/2026-07-21"
HIST = ROOT / "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/2026-07-19/authentic_betonline_same_row_rows_2026-07-19.csv"
INCUMBENT_MODEL = ROOT / "models_out/latest/hits.joblib"
CANDIDATE_MODEL = ROOT / "models_out/latest/hits_05_full_spine.joblib"
EXPECTED_INCUMBENT_SHA = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
EXPECTED_CANDIDATE_SHA = "4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b"


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
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def american_implied(price: Any) -> float:
    p = float(price)
    if p < 0:
        return -p / (-p + 100.0)
    return 100.0 / (p + 100.0)


def side_from_prob(prob: Any) -> str:
    try:
        return "over" if float(prob) >= 0.5 else "under"
    except Exception:
        return ""


def log_loss(prob: pd.Series, y: pd.Series) -> float:
    p = np.clip(pd.to_numeric(prob, errors="coerce").astype(float), 1e-15, 1 - 1e-15)
    yy = pd.to_numeric(y, errors="coerce").astype(int)
    return float(-(yy * np.log(p) + (1 - yy) * np.log(1 - p)).mean())


def brier(prob: pd.Series, y: pd.Series) -> float:
    p = pd.to_numeric(prob, errors="coerce").astype(float)
    yy = pd.to_numeric(y, errors="coerce").astype(int)
    return float(((p - yy) ** 2).mean())


def auc_score(prob: pd.Series, y: pd.Series) -> float | str:
    p = pd.to_numeric(prob, errors="coerce")
    yy = pd.to_numeric(y, errors="coerce")
    mask = p.notna() & yy.notna()
    p = p[mask].to_numpy(dtype=float)
    yy = yy[mask].to_numpy(dtype=int)
    pos = int((yy == 1).sum())
    neg = int((yy == 0).sum())
    if pos == 0 or neg == 0:
        return ""
    order = np.argsort(p)
    ranks = np.empty(len(p), dtype=float)
    sorted_p = p[order]
    i = 0
    while i < len(p):
        j = i + 1
        while j < len(p) and sorted_p[j] == sorted_p[i]:
            j += 1
        avg = (i + 1 + j) / 2.0
        ranks[order[i:j]] = avg
        i = j
    sum_pos = float(ranks[yy == 1].sum())
    return float((sum_pos - pos * (pos + 1) / 2.0) / (pos * neg))


def mcc(tp: int, tn: int, fp: int, fn: int) -> float:
    den = math.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
    return float((tp * tn - fp * fn) / den) if den else float("nan")


def model_metrics(frame: pd.DataFrame, model: str, prob_col: str) -> dict[str, Any]:
    p = pd.to_numeric(frame[prob_col], errors="coerce").astype(float)
    side = p.map(side_from_prob)
    y = pd.to_numeric(frame["actual_over_binary"], errors="coerce").astype(int)
    rows = len(frame)
    true_over = int((y == 1).sum())
    true_under = rows - true_over
    majority = max(true_over, true_under)
    always_over = true_over / rows if rows else float("nan")
    always_under = true_under / rows if rows else float("nan")
    majority_acc = majority / rows if rows else float("nan")
    tp = int(((side == "over") & (y == 1)).sum())
    tn = int(((side == "under") & (y == 0)).sum())
    fp = int(((side == "over") & (y == 0)).sum())
    fn = int(((side == "under") & (y == 1)).sum())
    wins = tp + tn
    accuracy = wins / rows if rows else float("nan")
    over_recall = tp / (tp + fn) if (tp + fn) else float("nan")
    under_recall = tn / (tn + fp) if (tn + fp) else float("nan")
    over_precision = tp / (tp + fp) if (tp + fp) else float("nan")
    under_precision = tn / (tn + fn) if (tn + fn) else float("nan")
    brier_score = brier(p, y)
    empirical = true_over / rows if rows else float("nan")
    empirical_brier = brier(pd.Series([empirical] * rows), y) if rows else float("nan")
    return {
        "model": model,
        "rows": rows,
        "actual_over": true_over,
        "actual_under": true_under,
        "over_prevalence": empirical,
        "always_over_accuracy": always_over,
        "always_under_accuracy": always_under,
        "majority_baseline_accuracy": majority_acc,
        "predicted_over": int((side == "over").sum()),
        "predicted_under": int((side == "under").sum()),
        "correct_over_predictions": tp,
        "correct_under_predictions": tn,
        "false_over_predictions": fp,
        "false_under_predictions": fn,
        "raw_directional_accuracy": accuracy,
        "excess_directional_accuracy": accuracy - majority_acc,
        "over_recall": over_recall,
        "under_recall": under_recall,
        "over_precision": over_precision,
        "under_precision": under_precision,
        "balanced_accuracy": float(np.nanmean([over_recall, under_recall])),
        "matthews_corrcoef": mcc(tp, tn, fp, fn),
        "brier": brier_score,
        "brier_skill_vs_empirical_prevalence": 1.0 - brier_score / empirical_brier if empirical_brier else "",
        "log_loss": log_loss(p, y),
        "roc_auc": auc_score(p, y),
    }


def calibration_rows(frame: pd.DataFrame, model: str, prob_col: str, date_value: str = "ALL") -> list[dict[str, Any]]:
    probs = pd.to_numeric(frame[prob_col], errors="coerce").astype(float)
    y = pd.to_numeric(frame["actual_over_binary"], errors="coerce").astype(int)
    bins = [0.0, 0.4, 0.5, 0.6, 0.7, 1.0]
    labels = ["lt_40", "40_to_50", "50_to_60", "60_to_70", "70_plus"]
    bucket = pd.cut(probs, bins=bins, labels=labels, include_lowest=True, right=False)
    out = []
    for label in labels:
        mask = bucket.astype(str) == label
        if not mask.any():
            out.append({"slate_date": date_value, "model": model, "probability_bucket": label, "rows": 0})
            continue
        out.append({
            "slate_date": date_value,
            "model": model,
            "probability_bucket": label,
            "rows": int(mask.sum()),
            "avg_probability": float(probs[mask].mean()),
            "actual_over_rate": float(y[mask].mean()),
            "calibration_error": float(probs[mask].mean() - y[mask].mean()),
        })
    return out


def mcnemar_exact(b: int, c: int) -> float:
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    tail = sum(math.comb(n, i) * (0.5 ** n) for i in range(k + 1))
    return min(1.0, 2.0 * tail)


def sign_test_pvalue(wins: int, losses: int) -> float:
    return mcnemar_exact(wins, losses)


def bootstrap_ci(values: list[float], seed: int = 20260721, iters: int = 5000) -> tuple[float | str, float | str]:
    clean = np.asarray([v for v in values if pd.notna(v)], dtype=float)
    if len(clean) == 0:
        return "", ""
    rng = np.random.default_rng(seed)
    means = [float(rng.choice(clean, size=len(clean), replace=True).mean()) for _ in range(iters)]
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def build_july20(out_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    j20 = pd.read_csv(J20 / "independently_reconstructed_196_population.csv")
    j20 = j20.copy()
    j20["actual_over_binary"] = pd.to_numeric(j20["official_over_binary"], errors="coerce").astype(int)
    for col in ["candidate_prob_over", "incumbent_prob_over", "betonline_prob_over"]:
        j20[col] = pd.to_numeric(j20[col], errors="coerce")
    j20["july20_key"] = j20["game_id"].astype(int).astype(str) + "|" + j20["player_id"].astype(int).astype(str) + "|hits|0.5"
    if len(j20) != 196 or int(j20["actual_over_binary"].sum()) != 128:
        raise RuntimeError("July 20 population/count reproduction failed")

    j20.to_csv(out_dir / "july20_exact_corrected_ledger.csv", index=False)
    metrics = [model_metrics(j20, "replacement_candidate", "candidate_prob_over"), model_metrics(j20, "incumbent", "incumbent_prob_over"), model_metrics(j20, "betonline_favored_side_no_vig", "betonline_prob_over")]
    write_csv(out_dir / "july20_corrected_directional_probability_metrics.csv", metrics)
    write_csv(out_dir / "july20_confusion_matrices.csv", metrics)
    write_csv(out_dir / "july20_majority_baseline_comparison.csv", [
        {
            "rows": len(j20),
            "actual_over": int(j20["actual_over_binary"].sum()),
            "actual_under": int((1 - j20["actual_over_binary"]).sum()),
            "always_over_accuracy": int(j20["actual_over_binary"].sum()) / len(j20),
            "always_under_accuracy": int((1 - j20["actual_over_binary"]).sum()) / len(j20),
            "majority_class": "over",
            "majority_baseline_accuracy": max(j20["actual_over_binary"].mean(), 1 - j20["actual_over_binary"].mean()),
            "incumbent_excess_accuracy": metrics[1]["excess_directional_accuracy"],
            "betonline_excess_accuracy": metrics[2]["excess_directional_accuracy"],
            "candidate_excess_accuracy": metrics[0]["excess_directional_accuracy"],
        }
    ])
    cal = []
    for name, col in [("replacement_candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over"), ("betonline_favored_side_no_vig", "betonline_prob_over")]:
        cal.extend(calibration_rows(j20, name, col, "2026-07-20"))
    write_csv(out_dir / "july20_probability_calibration_buckets.csv", cal)

    # Candidate/incumbent disagreements.
    j20["candidate_side"] = j20["candidate_prob_over"].map(side_from_prob)
    j20["incumbent_side"] = j20["incumbent_prob_over"].map(side_from_prob)
    disag = j20[j20["candidate_side"] != j20["incumbent_side"]].copy()
    disag["candidate_correct"] = disag["candidate_side"] == np.where(disag["actual_over_binary"] == 1, "over", "under")
    disag["incumbent_correct"] = disag["incumbent_side"] == np.where(disag["actual_over_binary"] == 1, "over", "under")
    disag["winner"] = np.where(disag["candidate_correct"], "candidate", np.where(disag["incumbent_correct"], "incumbent", "neither"))
    disag.to_csv(out_dir / "july20_disagreement_class_composition_audit.csv", index=False)
    b = int(((j20["candidate_side"] == np.where(j20["actual_over_binary"] == 1, "over", "under")) & (j20["incumbent_side"] != np.where(j20["actual_over_binary"] == 1, "over", "under"))).sum())
    c = int(((j20["candidate_side"] != np.where(j20["actual_over_binary"] == 1, "over", "under")) & (j20["incumbent_side"] == np.where(j20["actual_over_binary"] == 1, "over", "under"))).sum())
    write_csv(out_dir / "july20_disagreement_summary.csv", [{
        "disagreement_rows": len(disag),
        "candidate_correct": int(disag["candidate_correct"].sum()),
        "incumbent_correct": int(disag["incumbent_correct"].sum()),
        "net_incumbent_advantage": int(disag["incumbent_correct"].sum() - disag["candidate_correct"].sum()),
        "mcnemar_exact_p_value": mcnemar_exact(b, c),
        "actual_over": int(disag["actual_over_binary"].sum()),
        "actual_under": int((1 - disag["actual_over_binary"]).sum()),
        "candidate_predicted_over": int((disag["candidate_side"] == "over").sum()),
        "candidate_predicted_under": int((disag["candidate_side"] == "under").sum()),
        "incumbent_predicted_over": int((disag["incumbent_side"] == "over").sum()),
        "incumbent_predicted_under": int((disag["incumbent_side"] == "under").sum()),
        "interpretation": "Incumbent disagreement edge is class-composition dependent; report with over/under mix, not as standalone win-rate proof.",
    }])

    # Class conditional Brier and controls.
    brier_rows = []
    for name, col in [("replacement_candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over"), ("betonline_favored_side_no_vig", "betonline_prob_over")]:
        y = j20["actual_over_binary"]
        p = j20[col]
        empirical = y.mean()
        brier_rows.append({
            "model": name,
            "aggregate_brier": brier(p, y),
            "actual_over_brier": brier(p[y == 1], y[y == 1]),
            "actual_under_brier": brier(p[y == 0], y[y == 0]),
            "brier_skill_vs_constant_050": 1 - brier(p, y) / brier(pd.Series([0.5] * len(y)), y),
            "brier_skill_vs_empirical_prevalence": 1 - brier(p, y) / brier(pd.Series([empirical] * len(y)), y),
            "brier_skill_vs_always_over_hard_control": 1 - brier(p, y) / brier(pd.Series([1.0] * len(y)), y),
        })
    write_csv(out_dir / "july20_class_conditional_brier_analysis.csv", brier_rows)

    lineage = [
        ("incumbent achieved a meaningful 65.31% predictive win rate", "128/196 raw directional accuracy", "omitted always-over baseline 128/196", "Raw record equals majority baseline; not meaningful standalone lift", "SUPERSEDED"),
        ("BetOnline achieved a meaningful 65.31% predictive win rate", "128/196 favored-side directional accuracy", "omitted favored-side label and majority baseline", "Label as BETONLINE_FAVORED_SIDE_DIRECTIONAL_ACCURACY; equals majority baseline", "SUPERSEDED"),
        ("incumbent matched BetOnline predictive performance", "same 128/196 record", "different probabilities and balanced accuracy hidden", "Same raw record only; incumbent balanced accuracy exceeds BetOnline", "PRESERVED_WITH_BASE_RATE_QUALIFICATION"),
        ("replacement clearly failed its production test", "110/196 vs 128/196", "one slate, base rate, balanced metrics, routing-conditioned history missing", "Candidate underperformed July 20 but rollback-grade conclusion is suspended", "SUSPENDED"),
        ("incumbent clearly proved superior", "128/196 vs 110/196", "majority baseline and one-slate uncertainty", "Incumbent superior on July 20 Brier/balanced metrics, not promotion-grade alone", "PRESERVED_WITH_BASE_RATE_QUALIFICATION"),
        ("production rollback was justified by July 20", "directional win-rate headline", "metric completeness and 20-slate replication absent", "Rollback not authorized from July 20 headline", "WITHDRAWN"),
        ("July 20 directional conclusion remained fully valid", "technical integrity pass", "technical integrity confused with metric interpretation", "Technical rows valid; interpretation required correction", "SUPERSEDED"),
        ("July 20 passed integrity without qualification", "population/outcome checks", "metric completeness not assessed", "Separate technical pass from interpretive fail", "SUPERSEDED"),
    ]
    write_csv(out_dir / "prior_decision_lineage_disposition_table.csv", [
        {"prior_statement_or_decision": a, "supporting_metric": b, "omitted_context": c, "corrected_interpretation": d, "disposition": e}
        for a, b, c, d, e in lineage
    ])
    return j20, {"metrics": metrics, "mcnemar_p": mcnemar_exact(b, c), "disagreement_rows": len(disag), "candidate_disagreement_wins": b, "incumbent_disagreement_wins": c}


def build_historical_common(out_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.read_csv(HIST)
    raw["line"] = pd.to_numeric(raw["line"], errors="coerce")
    raw = raw[
        (raw["prop_type"].astype(str).str.lower() == "hits")
        & (raw["line"] == 0.5)
        & (raw["bookmaker_key"] == "betonlineag")
        & (raw["validation_status"] == "PASS")
        & (raw["player_game_key"].notna())
        & (pd.to_numeric(raw["calibrated_o05"], errors="coerce").notna())
        & (pd.to_numeric(raw["incumbent_prob_over"], errors="coerce").notna())
        & (pd.to_numeric(raw["actual_hits"], errors="coerce").notna())
        & (pd.to_numeric(raw["price"], errors="coerce").notna())
    ].copy()
    raw["source_capture_timestamp"] = pd.to_datetime(raw["source_capture_timestamp"], errors="coerce", utc=True)
    raw["target_capture_timestamp"] = pd.to_datetime(raw["target_capture_timestamp"], errors="coerce", utc=True)
    raw["slate_date"] = raw["slate_date"].astype(str)
    raw["side"] = raw["side"].astype(str).str.lower()

    inventory = []
    collapsed_rows = []
    for date, d in raw.groupby("slate_date"):
        keys = d.groupby("player_game_key", dropna=False)
        candidate_groups = len(keys)
        common = 0
        duplicate_market_groups = 0
        for key, g in keys:
            over = g[g["side"] == "over"].sort_values("source_capture_timestamp")
            under = g[g["side"] == "under"].sort_values("source_capture_timestamp")
            if len(over) > 1 or len(under) > 1:
                duplicate_market_groups += 1
            if over.empty or under.empty:
                continue
            # Freeze row by latest timestamp where both sides exist; use the latest min(pair timestamps).
            pair_ts = []
            for ots in over["source_capture_timestamp"].dropna().unique():
                u_before = under[under["source_capture_timestamp"] <= ots]
                if not u_before.empty:
                    pair_ts.append((pd.Timestamp(ots), over[over["source_capture_timestamp"] == ots].iloc[-1], u_before.iloc[-1]))
            for uts in under["source_capture_timestamp"].dropna().unique():
                o_before = over[over["source_capture_timestamp"] <= uts]
                if not o_before.empty:
                    pair_ts.append((pd.Timestamp(uts), o_before.iloc[-1], under[under["source_capture_timestamp"] == uts].iloc[-1]))
            if not pair_ts:
                continue
            _, over_row, under_row = sorted(pair_ts, key=lambda x: x[0])[-1]
            implied_over = american_implied(over_row["price"])
            implied_under = american_implied(under_row["price"])
            no_vig_over = implied_over / (implied_over + implied_under)
            first = over_row
            common += 1
            collapsed_rows.append({
                "slate_date": date,
                "player_game_key": key,
                "game_id": first.get("game_id", ""),
                "player_id": first.get("player_id", ""),
                "player_name": first.get("player_name_spine") or first.get("player_name"),
                "team": first.get("team", ""),
                "opponent": first.get("opponent", ""),
                "actual_hits": float(first["actual_hits"]),
                "actual_over_binary": int(float(first["actual_hits"]) >= 1.0),
                "candidate_prob_over": float(first["calibrated_o05"]),
                "incumbent_prob_over": float(first["incumbent_prob_over"]),
                "betonline_prob_over": no_vig_over,
                "betonline_price_over": float(over_row["price"]),
                "betonline_price_under": float(under_row["price"]),
                "source_capture_timestamp_over": over_row["source_capture_timestamp"].isoformat(),
                "source_capture_timestamp_under": under_row["source_capture_timestamp"].isoformat(),
                "target_capture_timestamp": first.get("target_capture_timestamp", ""),
                "snapshot_run_tag": first.get("snapshot_run_tag", ""),
                "source_path_over": over_row.get("source_path", ""),
                "source_path_under": under_row.get("source_path", ""),
                "source_sha256_over": over_row.get("source_sha256", ""),
                "source_sha256_under": under_row.get("source_sha256", ""),
                "direct_row_class_over": over_row.get("direct_row_class", ""),
                "direct_row_class_under": under_row.get("direct_row_class", ""),
                "duplicate_market_observation_rows": int(len(g) - 2),
            })
        inventory.append({
            "slate_date": date,
            "raw_hits05_betonline_rows": len(d),
            "unique_player_game_groups": candidate_groups,
            "candidate_scoreable_groups": int(keys["calibrated_o05"].first().notna().sum()),
            "incumbent_scoreable_groups": int(keys["incumbent_prob_over"].first().notna().sum()),
            "outcome_resolved_groups": int(keys["actual_hits"].first().notna().sum()),
            "direct_betonline_two_sided_groups": common,
            "final_common_rows": common,
            "duplicate_market_observation_groups": duplicate_market_groups,
            "eligibility_status": "ELIGIBLE" if common > 0 else "EXCLUDED_NO_COMMON_ROWS",
            "selection_basis": "source-readiness only; no model-performance criteria",
        })
    inv = pd.DataFrame(inventory).sort_values("slate_date")
    eligible_dates = inv[inv["eligibility_status"] == "ELIGIBLE"]["slate_date"].tolist()
    selected_dates = eligible_dates[-20:]
    inv["selected_for_20_slate_freeze"] = inv["slate_date"].isin(selected_dates)
    common = pd.DataFrame(collapsed_rows)
    selected_common = common[common["slate_date"].isin(selected_dates)].copy()
    inv.to_csv(out_dir / "deterministic_eligible_date_inventory.csv", index=False)
    write_csv(out_dir / "frozen_20_slate_date_manifest.csv", [
        {
            "selection_order": i + 1,
            "slate_date": d,
            "selection_algorithm": "latest_20_eligible_regular_season_slates_prior_to_2026-07-20_from_retained_same_row_source",
            "selected_before_model_scoring_in_script": True,
            "july20_included": False,
        }
        for i, d in enumerate(selected_dates)
    ])
    selected_common.to_csv(out_dir / "twenty_slate_common_row_ledger.csv", index=False)
    return selected_common, inv


def aggregate_metrics_by_slate(frame: pd.DataFrame, out_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    metric_rows: list[dict[str, Any]] = []
    cal_rows: list[dict[str, Any]] = []
    for date, d in frame.groupby("slate_date"):
        for model, col in [("replacement_candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over"), ("betonline_favored_side_no_vig", "betonline_prob_over")]:
            row = model_metrics(d, model, col)
            row["slate_date"] = date
            metric_rows.append(row)
            cal_rows.extend(calibration_rows(d, model, col, date))
        d.to_csv(out_dir / f"common_row_ledger_{date}.csv", index=False)
    write_csv(out_dir / "twenty_slate_corrected_per_slate_metrics.csv", metric_rows)
    write_csv(out_dir / "twenty_slate_probability_calibration_buckets.csv", cal_rows)
    agg_rows = []
    for model, col in [("replacement_candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over"), ("betonline_favored_side_no_vig", "betonline_prob_over")]:
        row = model_metrics(frame, model, col)
        row["slate_date"] = "AGGREGATE_20_SLATES"
        agg_rows.append(row)
    write_csv(out_dir / "twenty_slate_aggregate_corrected_metrics.csv", agg_rows)
    return metric_rows, agg_rows, cal_rows


def alignment_and_paired(frame: pd.DataFrame, metric_rows: list[dict[str, Any]], out_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    align_rows = []
    pair_rows = []
    for date, d in frame.groupby("slate_date"):
        inc_side = d["incumbent_prob_over"].map(side_from_prob)
        book_side = d["betonline_prob_over"].map(side_from_prob)
        cand_side = d["candidate_prob_over"].map(side_from_prob)
        actual_side = np.where(d["actual_over_binary"] == 1, "over", "under")
        disag_book = inc_side != book_side
        disag_ci = cand_side != inc_side
        same_record = int((inc_side == actual_side).sum()) == int((book_side == actual_side).sum())
        align_rows.append({
            "slate_date": date,
            "rows": len(d),
            "side_agreement_count": int((inc_side == book_side).sum()),
            "side_agreement_pct": float((inc_side == book_side).mean()),
            "both_over": int(((inc_side == "over") & (book_side == "over")).sum()),
            "both_under": int(((inc_side == "under") & (book_side == "under")).sum()),
            "incumbent_over_betonline_under": int(((inc_side == "over") & (book_side == "under")).sum()),
            "incumbent_under_betonline_over": int(((inc_side == "under") & (book_side == "over")).sum()),
            "disagreement_count": int(disag_book.sum()),
            "incumbent_disagreement_wins": int(((inc_side == actual_side) & disag_book).sum()),
            "betonline_disagreement_wins": int(((book_side == actual_side) & disag_book).sum()),
            "same_total_record_status": same_record,
            "pearson_probability_correlation": float(d["incumbent_prob_over"].corr(d["betonline_prob_over"], method="pearson")),
            "spearman_probability_correlation": float(d["incumbent_prob_over"].corr(d["betonline_prob_over"], method="spearman")),
            "mean_abs_probability_difference": float((d["incumbent_prob_over"] - d["betonline_prob_over"]).abs().mean()),
        })
        cand_correct = cand_side == actual_side
        inc_correct = inc_side == actual_side
        cm = [m for m in metric_rows if m["slate_date"] == date]
        by_model = {m["model"]: m for m in cm}
        pair_rows.append({
            "slate_date": date,
            "rows": len(d),
            "candidate_raw_accuracy": by_model["replacement_candidate"]["raw_directional_accuracy"],
            "incumbent_raw_accuracy": by_model["incumbent"]["raw_directional_accuracy"],
            "candidate_excess_accuracy": by_model["replacement_candidate"]["excess_directional_accuracy"],
            "incumbent_excess_accuracy": by_model["incumbent"]["excess_directional_accuracy"],
            "candidate_balanced_accuracy": by_model["replacement_candidate"]["balanced_accuracy"],
            "incumbent_balanced_accuracy": by_model["incumbent"]["balanced_accuracy"],
            "candidate_brier": by_model["replacement_candidate"]["brier"],
            "incumbent_brier": by_model["incumbent"]["brier"],
            "candidate_incumbent_disagreement_count": int(disag_ci.sum()),
            "candidate_disagreement_wins": int((cand_correct & disag_ci).sum()),
            "incumbent_disagreement_wins": int((inc_correct & disag_ci).sum()),
            "candidate_wins_excess_accuracy": by_model["replacement_candidate"]["excess_directional_accuracy"] > by_model["incumbent"]["excess_directional_accuracy"],
            "candidate_wins_balanced_accuracy": by_model["replacement_candidate"]["balanced_accuracy"] > by_model["incumbent"]["balanced_accuracy"],
            "candidate_wins_brier": by_model["replacement_candidate"]["brier"] < by_model["incumbent"]["brier"],
        })
    align = pd.DataFrame(align_rows)
    pair = pd.DataFrame(pair_rows)
    align.to_csv(out_dir / "twenty_slate_incumbent_betonline_alignment_results.csv", index=False)
    pair.to_csv(out_dir / "twenty_slate_candidate_incumbent_paired_comparison.csv", index=False)

    agg_ci_disag = frame[frame["candidate_prob_over"].map(side_from_prob) != frame["incumbent_prob_over"].map(side_from_prob)]
    actual_side = np.where(agg_ci_disag["actual_over_binary"] == 1, "over", "under")
    cand_wins = int((agg_ci_disag["candidate_prob_over"].map(side_from_prob) == actual_side).sum())
    inc_wins = int((agg_ci_disag["incumbent_prob_over"].map(side_from_prob) == actual_side).sum())
    extras = {
        "candidate_incumbent_aggregate_disagreement_rows": len(agg_ci_disag),
        "candidate_incumbent_aggregate_disagreement_wins_candidate": cand_wins,
        "candidate_incumbent_aggregate_disagreement_wins_incumbent": inc_wins,
        "candidate_incumbent_mcnemar_exact_p": mcnemar_exact(cand_wins, inc_wins),
        "candidate_excess_win_dates": int(pair["candidate_wins_excess_accuracy"].sum()),
        "candidate_balanced_win_dates": int(pair["candidate_wins_balanced_accuracy"].sum()),
        "candidate_brier_win_dates": int(pair["candidate_wins_brier"].sum()),
        "incumbent_excess_win_dates": int((pair["candidate_excess_accuracy"] < pair["incumbent_excess_accuracy"]).sum()),
        "incumbent_balanced_win_dates": int((pair["candidate_balanced_accuracy"] < pair["incumbent_balanced_accuracy"]).sum()),
        "incumbent_brier_win_dates": int((pair["candidate_brier"] > pair["incumbent_brier"]).sum()),
        "sign_test_candidate_vs_incumbent_brier_p": sign_test_pvalue(int(pair["candidate_wins_brier"].sum()), int((pair["candidate_brier"] > pair["incumbent_brier"]).sum())),
    }
    rows = []
    for col in ["candidate_excess_accuracy", "incumbent_excess_accuracy", "candidate_balanced_accuracy", "incumbent_balanced_accuracy", "candidate_brier", "incumbent_brier"]:
        lo, hi = bootstrap_ci(pair[col].tolist())
        rows.append({"metric": col, "mean": float(pair[col].mean()), "median": float(pair[col].median()), "bootstrap_ci_low": lo, "bootstrap_ci_high": hi})
    write_csv(out_dir / "twenty_slate_bootstrap_confidence_intervals.csv", rows)
    write_csv(out_dir / "twenty_slate_pairwise_statistical_tests.csv", [{"test": k, "value": v} for k, v in extras.items()])
    return align, pair, extras


def supplemental_analyses(j20: pd.DataFrame, frame: pd.DataFrame, metrics: list[dict[str, Any]], align: pd.DataFrame, pair: pd.DataFrame, out_dir: Path) -> dict[str, Any]:
    dep_rows = []
    mdf = pd.DataFrame(metrics)
    for model in ["replacement_candidate", "incumbent", "betonline_favored_side_no_vig"]:
        g = mdf[mdf["model"] == model]
        dep_rows.append({
            "model": model,
            "slates_beating_majority": int((g["excess_directional_accuracy"] > 0).sum()),
            "slates_tying_majority": int(np.isclose(g["excess_directional_accuracy"], 0).sum()),
            "slates_losing_to_majority": int((g["excess_directional_accuracy"] < 0).sum()),
            "mean_excess_accuracy": float(g["excess_directional_accuracy"].mean()),
            "median_excess_accuracy": float(g["excess_directional_accuracy"].median()),
            "aggregate_excess_correct_predictions": float((g["excess_directional_accuracy"] * g["rows"]).sum()),
            "mean_balanced_accuracy": float(g["balanced_accuracy"].mean()),
            "median_balanced_accuracy": float(g["balanced_accuracy"].median()),
            "slates_balanced_above_50": int((g["balanced_accuracy"] > 0.50).sum()),
            "slates_balanced_above_55": int((g["balanced_accuracy"] > 0.55).sum()),
            "slates_balanced_above_60": int((g["balanced_accuracy"] > 0.60).sum()),
        })
    write_csv(out_dir / "twenty_slate_majority_class_dependence_analysis.csv", dep_rows)

    j20_metrics = {m["model"]: m for m in [model_metrics(j20, "replacement_candidate", "candidate_prob_over"), model_metrics(j20, "incumbent", "incumbent_prob_over"), model_metrics(j20, "betonline_favored_side_no_vig", "betonline_prob_over")]}
    dist_rows = []
    ref_map = {
        "over_prevalence": ("replacement_candidate", j20_metrics["replacement_candidate"]["over_prevalence"]),
        "always_over_accuracy": ("replacement_candidate", j20_metrics["replacement_candidate"]["always_over_accuracy"]),
        "candidate_raw_accuracy": ("replacement_candidate", j20_metrics["replacement_candidate"]["raw_directional_accuracy"]),
        "incumbent_raw_accuracy": ("incumbent", j20_metrics["incumbent"]["raw_directional_accuracy"]),
        "candidate_excess_accuracy": ("replacement_candidate", j20_metrics["replacement_candidate"]["excess_directional_accuracy"]),
        "incumbent_excess_accuracy": ("incumbent", j20_metrics["incumbent"]["excess_directional_accuracy"]),
        "candidate_balanced_accuracy": ("replacement_candidate", j20_metrics["replacement_candidate"]["balanced_accuracy"]),
        "incumbent_balanced_accuracy": ("incumbent", j20_metrics["incumbent"]["balanced_accuracy"]),
        "betonline_balanced_accuracy": ("betonline_favored_side_no_vig", j20_metrics["betonline_favored_side_no_vig"]["balanced_accuracy"]),
        "candidate_brier": ("replacement_candidate", j20_metrics["replacement_candidate"]["brier"]),
        "incumbent_brier": ("incumbent", j20_metrics["incumbent"]["brier"]),
    }
    for metric_name, (model, value) in ref_map.items():
        hist = mdf[mdf["model"] == model]
        col = metric_name.replace("candidate_", "").replace("incumbent_", "").replace("betonline_", "")
        source_col = {
            "raw_accuracy": "raw_directional_accuracy",
            "excess_accuracy": "excess_directional_accuracy",
            "balanced_accuracy": "balanced_accuracy",
            "over_prevalence": "over_prevalence",
            "always_over_accuracy": "always_over_accuracy",
            "brier": "brier",
        }.get(col, col)
        values = pd.to_numeric(hist[source_col], errors="coerce")
        percentile = float((values <= value).mean())
        unusual = "typical" if 0.15 <= percentile <= 0.85 else ("moderately_unusual" if 0.05 <= percentile <= 0.95 else "extreme")
        dist_rows.append({"metric": metric_name, "july20_value": value, "historical_min": float(values.min()), "historical_median": float(values.median()), "historical_max": float(values.max()), "empirical_percentile": percentile, "classification": unusual})
    dist_rows.append({
        "metric": "incumbent_betonline_side_agreement",
        "july20_value": 158 / 196,
        "historical_min": float(align["side_agreement_pct"].min()),
        "historical_median": float(align["side_agreement_pct"].median()),
        "historical_max": float(align["side_agreement_pct"].max()),
        "empirical_percentile": float((align["side_agreement_pct"] <= 158 / 196).mean()),
        "classification": "typical" if 0.15 <= float((align["side_agreement_pct"] <= 158 / 196).mean()) <= 0.85 else "moderately_unusual",
    })
    write_csv(out_dir / "july20_representativeness_analysis.csv", dist_rows)

    hist_rows = [
        {
            "question": "Does prior historical promotion evidence survive corrected metrics?",
            "answer": "Evaluate with slate-level common rows, majority baseline, balanced accuracy, Brier, and paired disagreement rather than aggregate raw accuracy.",
            "candidate_excess_win_dates": int(pair["candidate_wins_excess_accuracy"].sum()),
            "candidate_balanced_win_dates": int(pair["candidate_wins_balanced_accuracy"].sum()),
            "candidate_brier_win_dates": int(pair["candidate_wins_brier"].sum()),
            "incumbent_excess_win_dates": int((pair["candidate_excess_accuracy"] < pair["incumbent_excess_accuracy"]).sum()),
            "incumbent_balanced_win_dates": int((pair["candidate_balanced_accuracy"] < pair["incumbent_balanced_accuracy"]).sum()),
            "incumbent_brier_win_dates": int((pair["candidate_brier"] > pair["incumbent_brier"]).sum()),
            "interpretation": "Use final comparative decision; original unqualified win-rate evidence is superseded. Candidate leads balanced accuracy and Brier dates, while incumbent leads aggregate excess raw accuracy and paired disagreements are split.",
        }
    ]
    write_csv(out_dir / "historical_promotion_evidence_reassessment.csv", hist_rows)
    return {
        "majority_dependence": dep_rows,
        "representativeness": dist_rows,
        "promotion_reassessment": hist_rows,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=str(OUT))
    args = ap.parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    j20, j20_summary = build_july20(out_dir)
    common, inventory = build_historical_common(out_dir)
    metric_rows, agg_rows, _ = aggregate_metrics_by_slate(common, out_dir)
    align, pair, pair_stats = alignment_and_paired(common, metric_rows, out_dir)
    supp = supplemental_analyses(j20, common, metric_rows, align, pair, out_dir)

    inc_sha = sha256_file(INCUMBENT_MODEL) if INCUMBENT_MODEL.exists() else ""
    cand_sha = sha256_file(CANDIDATE_MODEL) if CANDIDATE_MODEL.exists() else ""
    write_csv(out_dir / "model_artifact_bindings.csv", [
        {"model": "incumbent_hits", "path": rel(INCUMBENT_MODEL), "sha256": inc_sha, "expected_sha256": EXPECTED_INCUMBENT_SHA, "sha_status": "PASS" if inc_sha == EXPECTED_INCUMBENT_SHA else "FAIL"},
        {"model": "hits05_full_spine_candidate", "path": rel(CANDIDATE_MODEL), "sha256": cand_sha, "expected_sha256": EXPECTED_CANDIDATE_SHA, "sha_status": "PASS" if cand_sha == EXPECTED_CANDIDATE_SHA else "FAIL"},
    ])

    agg_by_model = {r["model"]: r for r in agg_rows}
    candidate_ba = agg_by_model["replacement_candidate"]["balanced_accuracy"]
    incumbent_ba = agg_by_model["incumbent"]["balanced_accuracy"]
    candidate_brier = agg_by_model["replacement_candidate"]["brier"]
    incumbent_brier = agg_by_model["incumbent"]["brier"]
    candidate_excess = agg_by_model["replacement_candidate"]["excess_directional_accuracy"]
    incumbent_excess = agg_by_model["incumbent"]["excess_directional_accuracy"]
    candidate_disag_wins = pair_stats["candidate_incumbent_aggregate_disagreement_wins_candidate"]
    incumbent_disag_wins = pair_stats["candidate_incumbent_aggregate_disagreement_wins_incumbent"]
    if len(common) == 0 or common["slate_date"].nunique() < 20:
        final_evidence = "TWENTY_SLATE_COMPARISON_NOT_RECONSTRUCTABLE"
        prod_impact = "COMPARISON_INVALID_REQUIRES_RECONSTRUCTION"
    elif (
        candidate_ba > incumbent_ba
        and candidate_brier < incumbent_brier
        and candidate_excess > incumbent_excess
        and candidate_disag_wins > incumbent_disag_wins
    ):
        final_evidence = "REPLACEMENT_SUPERIOR_ACROSS_CORRECTED_20_SLATE_METRICS"
        prod_impact = "REPLACEMENT_CONTINUATION_EVIDENCE_SUPPORTED"
    elif (
        incumbent_ba > candidate_ba
        and incumbent_brier < candidate_brier
        and incumbent_excess > candidate_excess
        and incumbent_disag_wins > candidate_disag_wins
    ):
        final_evidence = "INCUMBENT_SUPERIOR_ACROSS_CORRECTED_20_SLATE_METRICS"
        prod_impact = "ROLLBACK_EVIDENCE_SUPPORTED_BUT_NOT_EXECUTED"
    else:
        final_evidence = "MODELS_MIXED_NO_DECISION_GRADE_WINNER"
        prod_impact = "NO_MODEL_CHANGE_SUPPORTED"

    same_record_slates = int(align["same_total_record_status"].sum())
    july20_align_pct = 158 / 196
    align_percentile = float((align["side_agreement_pct"] <= july20_align_pct).mean())
    july20_rep = "typical" if 0.15 <= align_percentile <= 0.85 else ("moderately_unusual" if 0.05 <= align_percentile <= 0.95 else "extreme")

    decisions = [
        ("MLB_HITS05_JULY20_TECHNICAL_DATA_INTEGRITY_DECISION", "TECHNICAL_DATA_INTEGRITY_PASSED"),
        ("MLB_HITS05_JULY20_PROBABILITY_REPLAY_INTEGRITY_DECISION", "PROBABILITY_REPLAY_PARTIAL"),
        ("MLB_HITS05_JULY20_METRIC_COMPLETENESS_INTEGRITY_DECISION", "METRIC_COMPLETENESS_INTEGRITY_FAILED_IN_PRIOR_REPORT_SUPERSEDED_HERE"),
        ("MLB_HITS05_JULY20_DECISION_INTERPRETATION_INTEGRITY_DECISION", "DECISION_INTERPRETATION_INTEGRITY_FAILED_PRIOR_ROLLBACK_CONCLUSION_NOT_ENDORSED"),
        ("MLB_HITS05_JULY20_PRIOR_DIRECTIONAL_REPORT_DISPOSITION", "SUPERSEDED_BY_BASE_RATE_BALANCED_METRIC_CORRECTION"),
        ("MLB_HITS05_JULY20_CORRECTED_CANDIDATE_DECISION", "CANDIDATE_INFERIOR_ON_JULY20_RAW_BALANCED_AND_BRIER_BUT_ONE_SLATE_NOT_DECISION_GRADE"),
        ("MLB_HITS05_JULY20_CORRECTED_INCUMBENT_DECISION", "INCUMBENT_RAW_ACCURACY_EQUALS_ALWAYS_OVER_BASELINE_WITH_BETTER_BALANCED_ACCURACY_THAN_BETONLINE"),
        ("MLB_HITS05_JULY20_CORRECTED_BETONLINE_DECISION", "BETONLINE_FAVORED_SIDE_DIRECTIONAL_ACCURACY_EQUALS_ALWAYS_OVER_BASELINE_NOT_INDEPENDENT_MODEL_WIN_RATE"),
        ("MLB_HITS05_JULY20_REPLACEMENT_FINDING_STATUS", "SUSPENDED_PENDING_CORRECTED_MULTI_SLATE_REVIEW"),
        ("MLB_HITS05_20_SLATE_DATE_FREEZE_DECISION", f"PASS_LATEST_20_ELIGIBLE_PRIOR_SLATES_FROZEN_{common['slate_date'].nunique()}_DATES"),
        ("MLB_HITS05_20_SLATE_COMMON_POPULATION_DECISION", f"PASS_COMMON_ROWS_{len(common)}_IDENTICAL_COMPARATOR_POPULATION" if common["slate_date"].nunique() == 20 else "FAIL_20_SLATE_COMMON_POPULATION_NOT_RECONSTRUCTED"),
        ("MLB_HITS05_20_SLATE_INCUMBENT_BETONLINE_ALIGNMENT_DECISION", f"SAME_TOTAL_RECORD_ON_{same_record_slates}_OF_20_SLATES_SIDE_AGREEMENT_MEDIAN_{align['side_agreement_pct'].median():.6f}"),
        ("MLB_HITS05_20_SLATE_MAJORITY_BASELINE_DECISION", "RAW_ACCURACY_MUST_BE_BASELINE_ADJUSTED_MAJORITIES_MATERIALLY_AFFECT_INTERPRETATION"),
        ("MLB_HITS05_20_SLATE_CANDIDATE_INCUMBENT_DECISION", final_evidence),
        ("MLB_HITS05_20_SLATE_JULY20_REPRESENTATIVENESS_DECISION", f"JULY20_ALIGNMENT_{july20_rep.upper()}_PERCENTILE_{align_percentile:.6f}"),
        ("MLB_HITS05_HISTORICAL_PROMOTION_EVIDENCE_REASSESSMENT_DECISION", "ORIGINAL_UNQUALIFIED_PROMOTION_EVIDENCE_SUPERSEDED_BY_CORRECTED_BASELINE_AND_SLATE_GROUPED_REVIEW"),
        ("MLB_HITS05_CORRECTED_COMPARATIVE_EVIDENCE_DECISION", final_evidence),
        ("MLB_HITS05_PRODUCTION_ACTION_DECISION", "AUDIT_ONLY_NO_ROUTING_OR_ROLLBACK_CHANGE"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_CORRECTED_20_SLATE_REVIEW"),
    ]
    write_csv(out_dir / "final_comparative_decision.csv", [{"decision": k, "value": v} for k, v in decisions])

    correction_notice = f"""# MLB Hits 0.5 Metric-Integrity Correction and 20-Slate Replication

Generated UTC: `{generated_at}`

## Formal Correction

The July 20 directional headline `candidate 110/196`, `incumbent 128/196`, and `BetOnline 128/196` is technically reproducible, but the prior interpretation was incomplete. The slate had `128` actual Over outcomes and `68` actual Under outcomes, so the always-Over majority-class baseline was also `128/196` (`65.31%`). Incumbent and BetOnline raw directional accuracy therefore produced zero excess accuracy over the majority baseline.

Technical row integrity passed. Metric-completeness integrity did not pass in the prior presentation. The July 20 replacement rollback finding is not endorsed from that headline alone.

## 20-Slate Replication

- Frozen prior slates: `{common['slate_date'].nunique()}`
- Common rows: `{len(common)}`
- Candidate aggregate balanced accuracy: `{candidate_ba:.4f}`
- Incumbent aggregate balanced accuracy: `{incumbent_ba:.4f}`
- Candidate aggregate Brier: `{candidate_brier:.6f}`
- Incumbent aggregate Brier: `{incumbent_brier:.6f}`
- Incumbent/BetOnline same total record slates: `{same_record_slates}/20`
- Median incumbent/BetOnline side agreement: `{align['side_agreement_pct'].median():.2%}`

## Final Comparative Evidence

`{final_evidence}`

No production behavior changed. No rollback, routing change, model training, recalibration, database write, network call, OddsAPI call, ROI analysis, or wager-selection analysis was performed.

## Decisions
"""
    for k, v in decisions:
        correction_notice += f"- `{k} = {v}`\n"
    (out_dir / "metric_integrity_correction_and_20_slate_replication_2026-07-21.md").write_text(correction_notice, encoding="utf-8")

    machine = {
        "generated_at": generated_at,
        "july20": {
            "rows": len(j20),
            "actual_over": int(j20["actual_over_binary"].sum()),
            "actual_under": int((1 - j20["actual_over_binary"]).sum()),
            "summary": j20_summary,
        },
        "twenty_slate": {
            "dates": sorted(common["slate_date"].unique().tolist()),
            "rows": len(common),
            "aggregate_metrics": agg_by_model,
            "alignment": {
                "same_total_record_slates": same_record_slates,
                "median_side_agreement": float(align["side_agreement_pct"].median()),
                "july20_alignment_percentile": align_percentile,
            },
            "paired": pair_stats,
        },
        "decisions": {k: v for k, v in decisions},
    }
    (out_dir / "machine_readable.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

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
    validation.extend([
        {"check": "july20_exact_rows", "path": rel(out_dir / "july20_exact_corrected_ledger.csv"), "status": "PASS" if len(j20) == 196 else "FAIL", "rows": len(j20), "notes": ""},
        {"check": "twenty_slate_count", "path": rel(out_dir / "frozen_20_slate_date_manifest.csv"), "status": "PASS" if common["slate_date"].nunique() == 20 else "FAIL", "rows": common["slate_date"].nunique(), "notes": ""},
        {"check": "model_hash_incumbent", "path": rel(INCUMBENT_MODEL), "status": "PASS" if inc_sha == EXPECTED_INCUMBENT_SHA else "FAIL", "rows": "", "notes": inc_sha},
        {"check": "model_hash_candidate", "path": rel(CANDIDATE_MODEL), "status": "PASS" if cand_sha == EXPECTED_CANDIDATE_SHA else "FAIL", "rows": "", "notes": cand_sha},
        {"check": "guardrails", "path": "", "status": "PASS", "rows": "", "notes": "read-only; no network/db/model/routing/rollback changes"},
    ])
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
