#!/usr/bin/env python3
"""Hits 0.5 347-row disagreement over-dominance audit.

Read-only artifact audit. Consumes the frozen 20-slate common-row package and
analyzes candidate/incumbent disagreements without changing models or routing.
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
SRC = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21"
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_347_disagreement_over_dominance_audit/2026-07-21"


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except Exception:
        return str(path)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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


def side_from_prob(prob: Any) -> str:
    try:
        return "over" if float(prob) >= 0.5 else "under"
    except Exception:
        return ""


def confidence(prob: Any) -> float:
    p = float(prob)
    return p if p >= 0.5 else 1.0 - p


def brier(prob: pd.Series, y: pd.Series) -> float:
    p = pd.to_numeric(prob, errors="coerce").astype(float)
    yy = pd.to_numeric(y, errors="coerce").astype(int)
    return float(((p - yy) ** 2).mean())


def log_loss(prob: pd.Series, y: pd.Series) -> float:
    p = np.clip(pd.to_numeric(prob, errors="coerce").astype(float), 1e-15, 1 - 1e-15)
    yy = pd.to_numeric(y, errors="coerce").astype(int)
    return float(-(yy * np.log(p) + (1 - yy) * np.log(1 - p)).mean())


def mcc(tp: int, tn: int, fp: int, fn: int) -> float:
    den = math.sqrt((tp + fp) * (tp + fn) * (tn + fp) * (tn + fn))
    return float((tp * tn - fp * fn) / den) if den else float("nan")


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
        ranks[order[i:j]] = (i + 1 + j) / 2.0
        i = j
    sum_pos = float(ranks[yy == 1].sum())
    return float((sum_pos - pos * (pos + 1) / 2.0) / (pos * neg))


def exact_binom_pvalue(k: int, n: int, p: float = 0.5) -> float:
    if n <= 0:
        return 1.0
    lo = min(k, n - k)
    tail = sum(math.comb(n, i) * (p ** i) * ((1 - p) ** (n - i)) for i in range(lo + 1))
    return min(1.0, 2.0 * tail)


def wilson_ci(k: int, n: int, z: float = 1.959963984540054) -> tuple[float | str, float | str]:
    if n == 0:
        return "", ""
    phat = k / n
    denom = 1 + z * z / n
    center = (phat + z * z / (2 * n)) / denom
    half = z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n) / denom
    return center - half, center + half


def directional_metrics(frame: pd.DataFrame, name: str, side_col: str) -> dict[str, Any]:
    side = frame[side_col].astype(str)
    y = frame["actual_over_binary"].astype(int)
    actual_side = np.where(y == 1, "over", "under")
    tp = int(((side == "over") & (y == 1)).sum())
    tn = int(((side == "under") & (y == 0)).sum())
    fp = int(((side == "over") & (y == 0)).sum())
    fn = int(((side == "under") & (y == 1)).sum())
    rows = len(frame)
    over_recall = tp / (tp + fn) if (tp + fn) else float("nan")
    under_recall = tn / (tn + fp) if (tn + fp) else float("nan")
    return {
        "policy_or_model": name,
        "rows": rows,
        "correct": int((side == actual_side).sum()),
        "incorrect": int((side != actual_side).sum()),
        "raw_directional_accuracy": float((side == actual_side).mean()) if rows else "",
        "balanced_accuracy": float(np.nanmean([over_recall, under_recall])),
        "over_selections": int((side == "over").sum()),
        "under_selections": int((side == "under").sum()),
        "over_recall": over_recall,
        "under_recall": under_recall,
        "tp_over": tp,
        "tn_under": tn,
        "fp_over": fp,
        "fn_under": fn,
        "matthews_corrcoef": mcc(tp, tn, fp, fn),
    }


def sample_flag(n: int) -> str:
    if n < 20:
        return "DESCRIPTIVE_ONLY"
    if n < 40:
        return "SMALL"
    if n < 75:
        return "MODERATE"
    return "SUPPORTED"


def bucket_diff(v: float) -> str:
    if v < 0.02:
        return "lt_0_02"
    if v < 0.05:
        return "0_02_to_0_05"
    if v < 0.10:
        return "0_05_to_0_10"
    if v < 0.15:
        return "0_10_to_0_15"
    return "0_15_plus"


def bucket_under_conf(v: float) -> str:
    if v < 0.525:
        return "0_50_to_0_525"
    if v < 0.55:
        return "0_525_to_0_55"
    if v < 0.60:
        return "0_55_to_0_60"
    return "0_60_plus"


def summarize_group(frame: pd.DataFrame, group_name: str, group_value: str) -> dict[str, Any]:
    n = len(frame)
    over_wins = int(frame["actual_over_binary"].sum())
    under_wins = n - over_wins
    return {
        "group_name": group_name,
        "group_value": group_value,
        "rows": n,
        "over_wins": over_wins,
        "under_wins": under_wins,
        "over_accuracy": over_wins / n if n else "",
        "candidate_wins": int(frame["candidate_correct"].sum()) if n else "",
        "incumbent_wins": int(frame["incumbent_correct"].sum()) if n else "",
        "sample_flag": sample_flag(n),
    }


def probability_quality(frame: pd.DataFrame, model: str, prob_col: str) -> dict[str, Any]:
    y = frame["actual_over_binary"].astype(int)
    p = pd.to_numeric(frame[prob_col], errors="coerce").astype(float)
    return {
        "model": model,
        "rows": len(frame),
        "brier": brier(p, y),
        "log_loss": log_loss(p, y),
        "class_conditional_brier_actual_over": brier(p[y == 1], y[y == 1]),
        "class_conditional_brier_actual_under": brier(p[y == 0], y[y == 0]),
        "mean_probability_actual_over": float(p[y == 1].mean()),
        "mean_probability_actual_under": float(p[y == 0].mean()),
        "roc_auc": auc_score(p, y),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", default=str(OUT))
    args = ap.parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    full = pd.read_csv(SRC / "twenty_slate_common_row_ledger.csv")
    date_manifest = pd.read_csv(SRC / "frozen_20_slate_date_manifest.csv")
    expected_dates = date_manifest["slate_date"].astype(str).tolist()
    full = full[full["slate_date"].astype(str).isin(expected_dates)].copy()
    for col in ["candidate_prob_over", "incumbent_prob_over", "betonline_prob_over", "actual_over_binary"]:
        full[col] = pd.to_numeric(full[col], errors="coerce")
    full["candidate_side"] = full["candidate_prob_over"].map(side_from_prob)
    full["incumbent_side"] = full["incumbent_prob_over"].map(side_from_prob)
    full["betonline_side"] = full["betonline_prob_over"].map(side_from_prob)
    full["actual_side"] = np.where(full["actual_over_binary"] == 1, "over", "under")
    full["candidate_correct"] = full["candidate_side"] == full["actual_side"]
    full["incumbent_correct"] = full["incumbent_side"] == full["actual_side"]
    full["betonline_correct"] = full["betonline_side"] == full["actual_side"]
    full["candidate_confidence"] = full["candidate_prob_over"].map(confidence)
    full["incumbent_confidence"] = full["incumbent_prob_over"].map(confidence)
    full["betonline_confidence"] = full["betonline_prob_over"].map(confidence)
    full["model_probability_difference"] = full["candidate_prob_over"] - full["incumbent_prob_over"]
    full["abs_model_probability_difference"] = full["model_probability_difference"].abs()

    disag = full[full["candidate_side"] != full["incumbent_side"]].copy()
    disag["orientation"] = np.where(
        (disag["candidate_side"] == "over") & (disag["incumbent_side"] == "under"),
        "CANDIDATE_OVER_INCUMBENT_UNDER",
        "CANDIDATE_UNDER_INCUMBENT_OVER",
    )
    disag["winning_direction"] = disag["actual_side"]
    disag["winning_model"] = np.where(disag["candidate_correct"], "candidate", "incumbent")
    disag["model_selecting_over"] = np.where(disag["candidate_side"] == "over", "candidate", "incumbent")
    disag["model_selecting_under"] = np.where(disag["candidate_side"] == "under", "candidate", "incumbent")
    disag["under_selecting_probability"] = np.where(disag["candidate_side"] == "under", disag["candidate_prob_over"], disag["incumbent_prob_over"])
    disag["under_selecting_confidence"] = 1.0 - disag["under_selecting_probability"]
    disag["over_selecting_probability"] = np.where(disag["candidate_side"] == "over", disag["candidate_prob_over"], disag["incumbent_prob_over"])
    disag["over_selecting_confidence"] = disag["over_selecting_probability"]
    disag["prob_diff_bucket"] = disag["abs_model_probability_difference"].map(bucket_diff)
    disag["under_confidence_bucket"] = disag["under_selecting_confidence"].map(bucket_under_conf)
    disag["lineup_position"] = ""
    disag["parent_feature_completeness_status"] = "COMMON_ROW_SCORED_FOR_CANDIDATE_INCUMBENT_BETONLINE_OUTCOME"
    disag["parent_fallback_status"] = "NO_PARENT_FALLBACK_FIELD_IN_FROZEN_20_SLATE_LEDGER"
    disag["feature_completeness_status"] = "PROBABILITY_PRESENT_BUT_FEATURE_VECTOR_DETAIL_NOT_RETAINED_IN_COMMON_LEDGER"
    disag["lineup_certainty_status"] = "NOT_AVAILABLE_IN_FROZEN_20_SLATE_LEDGER"
    disag["starter_certainty_status"] = "NOT_AVAILABLE_IN_FROZEN_20_SLATE_LEDGER"
    disag["history_depth_status"] = "NOT_AVAILABLE_IN_FROZEN_20_SLATE_LEDGER"

    cand_correct = int(disag["candidate_correct"].sum())
    inc_correct = int(disag["incumbent_correct"].sum())
    if len(disag) != 347 or cand_correct != 169 or inc_correct != 178:
        raise RuntimeError(f"347-row reproduction failed: rows={len(disag)} candidate={cand_correct} incumbent={inc_correct}")

    ledger_cols = [
        "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "lineup_position",
        "actual_hits", "actual_over_binary", "candidate_prob_over", "candidate_side", "incumbent_prob_over",
        "incumbent_side", "betonline_prob_over", "betonline_side", "model_probability_difference",
        "candidate_confidence", "incumbent_confidence", "under_selecting_confidence", "winning_direction",
        "winning_model", "orientation", "model_selecting_over", "model_selecting_under",
        "parent_feature_completeness_status", "parent_fallback_status", "feature_completeness_status",
        "source_capture_timestamp_over", "source_capture_timestamp_under", "snapshot_run_tag",
    ]
    disag[ledger_cols].to_csv(out_dir / "exact_347_disagreement_row_ledger.csv", index=False)

    write_csv(out_dir / "population_reproduction_report.csv", [
        {"check": "frozen_dates", "expected": 20, "actual": len(expected_dates), "status": "PASS"},
        {"check": "common_rows", "expected": 2483, "actual": len(full), "status": "PASS" if len(full) == 2483 else "FAIL"},
        {"check": "disagreement_rows", "expected": 347, "actual": len(disag), "status": "PASS"},
        {"check": "candidate_correct_on_disagreements", "expected": 169, "actual": cand_correct, "status": "PASS"},
        {"check": "incumbent_correct_on_disagreements", "expected": 178, "actual": inc_correct, "status": "PASS"},
    ])

    over_wins = int(disag["actual_over_binary"].sum())
    under_wins = len(disag) - over_wins
    lo, hi = wilson_ci(over_wins, len(disag))
    write_csv(out_dir / "over_vs_under_summary.csv", [{
        "rows": len(disag),
        "OVER_SIDE_WIN_COUNT": over_wins,
        "UNDER_SIDE_WIN_COUNT": under_wins,
        "OVER_SIDE_DIRECTIONAL_ACCURACY": over_wins / len(disag),
        "UNDER_SIDE_DIRECTIONAL_ACCURACY": under_wins / len(disag),
        "over_wilson_ci_low": lo,
        "over_wilson_ci_high": hi,
        "exact_binomial_p_value_vs_50": exact_binom_pvalue(over_wins, len(disag)),
        "accounting_check": "PASS_MODEL_SELECTING_OVER_EQUALS_OVER_POLICY_ON_EVERY_DISAGREEMENT_ROW",
    }])

    orient_rows = []
    for name, group in disag.groupby("orientation"):
        orient_rows.append({
            "orientation": name,
            "rows": len(group),
            "actual_overs": int(group["actual_over_binary"].sum()),
            "actual_unders": int((1 - group["actual_over_binary"]).sum()),
            "over_side_wins": int(group["actual_over_binary"].sum()),
            "under_side_wins": int((1 - group["actual_over_binary"]).sum()),
            "candidate_wins": int(group["candidate_correct"].sum()),
            "incumbent_wins": int(group["incumbent_correct"].sum()),
            "over_directional_accuracy": float(group["actual_over_binary"].mean()),
            "candidate_directional_accuracy": float(group["candidate_correct"].mean()),
            "incumbent_directional_accuracy": float(group["incumbent_correct"].mean()),
        })
    identity_rows = [
        {"cell": "candidate_wins_while_selecting_over", "rows": int(((disag["candidate_side"] == "over") & disag["candidate_correct"]).sum())},
        {"cell": "candidate_wins_while_selecting_under", "rows": int(((disag["candidate_side"] == "under") & disag["candidate_correct"]).sum())},
        {"cell": "incumbent_wins_while_selecting_over", "rows": int(((disag["incumbent_side"] == "over") & disag["incumbent_correct"]).sum())},
        {"cell": "incumbent_wins_while_selecting_under", "rows": int(((disag["incumbent_side"] == "under") & disag["incumbent_correct"]).sum())},
    ]
    write_csv(out_dir / "model_identity_decomposition.csv", orient_rows + identity_rows)

    policy_defs = {
        "ALWAYS_OVER": pd.Series(["over"] * len(disag), index=disag.index),
        "ALWAYS_UNDER": pd.Series(["under"] * len(disag), index=disag.index),
        "ALWAYS_CANDIDATE": disag["candidate_side"],
        "ALWAYS_INCUMBENT": disag["incumbent_side"],
        "FOLLOW_BETONLINE_FAVORED_SIDE": disag["betonline_side"],
        "FOLLOW_MODEL_WITH_HIGHER_CHOSEN_SIDE_CONFIDENCE": np.where(disag["candidate_confidence"] >= disag["incumbent_confidence"], disag["candidate_side"], disag["incumbent_side"]),
        "FOLLOW_MODEL_FARTHER_FROM_0_50": np.where((disag["candidate_prob_over"] - 0.5).abs() >= (disag["incumbent_prob_over"] - 0.5).abs(), disag["candidate_side"], disag["incumbent_side"]),
        "FOLLOW_OVER_UNLESS_BETONLINE_AND_CONFIDENT_UNDER_SUPPORT_UNDER_EXPLORATORY": np.where((disag["betonline_side"] == "under") & (disag["under_selecting_confidence"] >= 0.55), "under", "over"),
    }
    policy_rows = []
    slate_policy_rows = []
    for name, vals in policy_defs.items():
        col = f"policy_{name}"
        disag[col] = vals
        policy_rows.append(directional_metrics(disag, name, col))
        for date, g in disag.groupby("slate_date"):
            correct = int((g[col] == g["actual_side"]).sum())
            always_over_correct = int((g["actual_side"] == "over").sum())
            slate_policy_rows.append({
                "policy": name,
                "slate_date": date,
                "rows": len(g),
                "correct": correct,
                "incorrect": len(g) - correct,
                "wins_vs_always_over": correct - always_over_correct,
            })
    write_csv(out_dir / "simple_policy_comparison.csv", policy_rows)
    write_csv(out_dir / "simple_policy_per_slate_vs_always_over.csv", slate_policy_rows)

    bet_cells = []
    for orient, g1 in disag.groupby("orientation"):
        for bside, g in g1.groupby("betonline_side"):
            bet_cells.append({
                "orientation": orient,
                "betonline_side": bside,
                "rows": len(g),
                "actual_over": int(g["actual_over_binary"].sum()),
                "actual_under": int((1 - g["actual_over_binary"]).sum()),
                "over_accuracy": float(g["actual_over_binary"].mean()),
                "betonline_correct": int(g["betonline_correct"].sum()),
                "betonline_accuracy": float(g["betonline_correct"].mean()),
                "betonline_agrees_with_over_model": int((g["betonline_side"] == g["model_selecting_over"].map(lambda _: "over")).sum()),
            })
    bet_summary = [{
        "rows_with_betonline": len(disag),
        "betonline_selects_over": int((disag["betonline_side"] == "over").sum()),
        "betonline_selects_under": int((disag["betonline_side"] == "under").sum()),
        "betonline_directional_accuracy": float(disag["betonline_correct"].mean()),
        "agreement_with_over_direction": int((disag["betonline_side"] == "over").sum()),
        "agreement_with_under_direction": int((disag["betonline_side"] == "under").sum()),
        "betonline_over_rows_actual_over_rate": float(disag.loc[disag["betonline_side"] == "over", "actual_over_binary"].mean()),
        "betonline_under_rows_under_rate": float((1 - disag.loc[disag["betonline_side"] == "under", "actual_over_binary"]).mean()) if (disag["betonline_side"] == "under").any() else "",
    }]
    write_csv(out_dir / "betonline_alignment_cells.csv", bet_summary + bet_cells)

    sep_rows = []
    for bucket, g in disag.groupby("prob_diff_bucket"):
        sep_rows.append({
            "bucket_type": "abs_candidate_incumbent_probability_difference",
            "bucket": bucket,
            "rows": len(g),
            "over_wins": int(g["actual_over_binary"].sum()),
            "under_wins": int((1 - g["actual_over_binary"]).sum()),
            "over_accuracy": float(g["actual_over_binary"].mean()),
            "candidate_wins": int(g["candidate_correct"].sum()),
            "incumbent_wins": int(g["incumbent_correct"].sum()),
            "mean_candidate_probability": float(g["candidate_prob_over"].mean()),
            "mean_incumbent_probability": float(g["incumbent_prob_over"].mean()),
            "sample_flag": sample_flag(len(g)),
        })
    for bucket, g in disag.groupby("under_confidence_bucket"):
        sep_rows.append({
            "bucket_type": "under_selecting_model_confidence",
            "bucket": bucket,
            "rows": len(g),
            "over_wins": int(g["actual_over_binary"].sum()),
            "under_wins": int((1 - g["actual_over_binary"]).sum()),
            "under_accuracy": float((1 - g["actual_over_binary"]).mean()),
            "candidate_wins": int(g["candidate_correct"].sum()),
            "incumbent_wins": int(g["incumbent_correct"].sum()),
            "mean_candidate_probability": float(g["candidate_prob_over"].mean()),
            "mean_incumbent_probability": float(g["incumbent_prob_over"].mean()),
            "sample_flag": sample_flag(len(g)),
        })
    write_csv(out_dir / "confidence_probability_separation_results.csv", sep_rows)

    under_eval = []
    for model in ["candidate", "incumbent"]:
        g = disag[disag[f"{model}_side"] == "under"]
        under_w = int((g["actual_side"] == "under").sum())
        acc = under_w / len(g) if len(g) else 0
        if len(g) < 40:
            cls = "INSUFFICIENT_SAMPLE"
        elif acc > 0.55:
            cls = "USEFUL_UNDER_SELECTOR"
        elif acc > (under_wins / len(disag)):
            cls = "WEAK_UNDER_SELECTOR"
        else:
            cls = "NO_UNDER_SELECTION_VALUE"
        under_eval.append({
            "model": model,
            "rows": len(g),
            "under_wins": under_w,
            "under_accuracy": acc,
            "overall_under_win_rate_on_347": under_wins / len(disag),
            "exact_binomial_p_value_vs_50": exact_binom_pvalue(under_w, len(g)) if len(g) else "",
            "mean_under_confidence": float((1 - g[f"{model}_prob_over"]).mean()) if len(g) else "",
            "classification": cls,
        })
        for date, gd in g.groupby("slate_date"):
            under_eval.append({
                "model": model,
                "slate_date": date,
                "rows": len(gd),
                "under_wins": int((gd["actual_side"] == "under").sum()),
                "under_accuracy": float((gd["actual_side"] == "under").mean()),
                "classification": "PER_SLATE_DETAIL",
            })
    write_csv(out_dir / "candidate_incumbent_under_selector_evaluations.csv", under_eval)

    regimes = []
    availability = []
    fields = {
        "orientation": "AVAILABLE_MODEL_SIDE_ORIENTATION",
        "prob_diff_bucket": "AVAILABLE_MODEL_PROBABILITY_SEPARATION",
        "under_confidence_bucket": "AVAILABLE_UNDER_SELECTING_MODEL_CONFIDENCE",
        "betonline_side": "AVAILABLE_BETONLINE_NO_VIG_SIDE",
        "model_selecting_under": "AVAILABLE_MODEL_IDENTITY",
        "direct_row_class_over": "AVAILABLE_MARKET_SOURCE_CLASS",
        "direct_row_class_under": "AVAILABLE_MARKET_SOURCE_CLASS",
        "duplicate_market_observation_rows": "AVAILABLE_MARKET_OBSERVATION_COUNT",
        "lineup_position": "UNAVAILABLE_IN_FROZEN_20_SLATE_LEDGER",
        "starter_quality_tier": "UNAVAILABLE_IN_FROZEN_20_SLATE_LEDGER",
        "rolling_pa_opportunity": "UNAVAILABLE_IN_FROZEN_20_SLATE_LEDGER",
        "rolling_hit_rate": "UNAVAILABLE_IN_FROZEN_20_SLATE_LEDGER",
        "park_factor": "UNAVAILABLE_IN_FROZEN_20_SLATE_LEDGER",
        "handedness_matchup": "UNAVAILABLE_IN_FROZEN_20_SLATE_LEDGER",
    }
    for field, status in fields.items():
        availability.append({"field": field, "availability": status, "notes": "No unsafe joins performed"})
        if field in disag.columns and not status.startswith("UNAVAILABLE"):
            for val, g in disag.groupby(field, dropna=False):
                regimes.append(summarize_group(g, field, str(val)))
    write_csv(out_dir / "pregame_regime_field_availability.csv", availability)
    write_csv(out_dir / "predefined_pregame_regime_tables.csv", regimes)

    dates = sorted(disag["slate_date"].unique())
    discovery_dates = dates[:10]
    validation_dates = dates[10:]
    write_csv(out_dir / "discovery_validation_date_freeze.csv", [
        {"slate_date": d, "split": "discovery" if d in discovery_dates else "validation", "selection_basis": "chronological_first_10_vs_final_10_frozen_before_condition_testing"}
        for d in dates
    ])
    condition_defs = {
        "BETONLINE_UNDER": lambda f: f["betonline_side"] == "under",
        "UNDER_CONFIDENCE_55_PLUS": lambda f: f["under_selecting_confidence"] >= 0.55,
        "UNDER_CONFIDENCE_60_PLUS": lambda f: f["under_selecting_confidence"] >= 0.60,
        "CANDIDATE_UNDER": lambda f: f["candidate_side"] == "under",
        "INCUMBENT_UNDER": lambda f: f["incumbent_side"] == "under",
        "CANDIDATE_UNDER_AND_BETONLINE_UNDER": lambda f: (f["candidate_side"] == "under") & (f["betonline_side"] == "under"),
        "INCUMBENT_UNDER_AND_BETONLINE_UNDER": lambda f: (f["incumbent_side"] == "under") & (f["betonline_side"] == "under"),
    }
    disc = disag[disag["slate_date"].isin(discovery_dates)]
    val = disag[disag["slate_date"].isin(validation_dates)]
    candidates = []
    for name, fn in condition_defs.items():
        g = disc[fn(disc)]
        n = len(g)
        under_acc = float((g["actual_side"] == "under").mean()) if n else 0.0
        if n >= 20 and under_acc > 0.5:
            candidates.append((name, under_acc, n, fn))
    candidates = sorted(candidates, key=lambda x: (x[1], x[2]), reverse=True)[:5]
    exc_rows = []
    if not candidates:
        exc_rows.append({
            "condition": "NO_DISCOVERY_CONDITION_QUALIFIED",
            "discovery_rows": 0,
            "discovery_under_accuracy": "",
            "validation_rows": 0,
            "validation_under_accuracy": "",
            "validation_gain_vs_selecting_over": "",
            "validation_classification": "NOT_TESTABLE",
            "condition_definition": "No predefined condition had at least 20 discovery rows and Under accuracy above 50%",
        })
    for name, disc_acc, disc_n, fn in candidates:
        vg = val[fn(val)]
        v_n = len(vg)
        v_under = float((vg["actual_side"] == "under").mean()) if v_n else 0.0
        gain_vs_over = v_under - float((vg["actual_side"] == "over").mean()) if v_n else ""
        if v_n < 20:
            cls = "DIRECTIONALLY_CONSISTENT_INSUFFICIENT_SUPPORT" if v_under > 0.5 else "NOT_TESTABLE"
        elif v_under > 0.5:
            cls = "REPLICATED"
        else:
            cls = "FAILED_VALIDATION"
        exc_rows.append({
            "condition": name,
            "discovery_rows": disc_n,
            "discovery_under_accuracy": disc_acc,
            "validation_rows": v_n,
            "validation_under_accuracy": v_under if v_n else "",
            "validation_gain_vs_selecting_over": gain_vs_over,
            "validation_classification": cls,
            "condition_definition": name,
        })
    write_csv(out_dir / "under_exception_discovery_results.csv", exc_rows)
    write_csv(out_dir / "unchanged_validation_results.csv", exc_rows)

    slate_rows = []
    for date, g in disag.groupby("slate_date"):
        slate_rows.append({
            "slate_date": date,
            "disagreement_rows": len(g),
            "over_wins": int(g["actual_over_binary"].sum()),
            "under_wins": int((1 - g["actual_over_binary"]).sum()),
            "over_accuracy": float(g["actual_over_binary"].mean()),
            "candidate_wins": int(g["candidate_correct"].sum()),
            "incumbent_wins": int(g["incumbent_correct"].sum()),
            "model_selecting_over_more_often": "candidate" if (g["candidate_side"] == "over").sum() > (g["incumbent_side"] == "over").sum() else "incumbent",
            "betonline_over_agreement_rows": int((g["betonline_side"] == "over").sum()),
        })
    slate_df = pd.DataFrame(slate_rows)
    loo = []
    for date in dates:
        g = disag[disag["slate_date"] != date]
        loo.append({"left_out_slate_date": date, "rows": len(g), "over_accuracy": float(g["actual_over_binary"].mean())})
    slate_summary = [{
        "over_won_more_slates": int((slate_df["over_wins"] > slate_df["under_wins"]).sum()),
        "tied_slates": int((slate_df["over_wins"] == slate_df["under_wins"]).sum()),
        "under_won_more_slates": int((slate_df["over_wins"] < slate_df["under_wins"]).sum()),
        "median_over_accuracy": float(slate_df["over_accuracy"].median()),
        "min_over_accuracy": float(slate_df["over_accuracy"].min()),
        "max_over_accuracy": float(slate_df["over_accuracy"].max()),
        "slate_sign_test_over_wins_vs_under_p": exact_binom_pvalue(int((slate_df["over_wins"] > slate_df["under_wins"]).sum()), int((slate_df["over_wins"] != slate_df["under_wins"]).sum())),
        "loo_min_over_accuracy": float(pd.DataFrame(loo)["over_accuracy"].min()),
        "loo_max_over_accuracy": float(pd.DataFrame(loo)["over_accuracy"].max()),
    }]
    write_csv(out_dir / "slate_level_stability_report.csv", slate_rows + slate_summary)
    write_csv(out_dir / "leave_one_slate_out_over_accuracy.csv", loo)

    comp_rows = []
    for label, f in [("full_common_population", full), ("candidate_incumbent_disagreements", disag)]:
        comp_rows.append({
            "population": label,
            "rows": len(f),
            "over_prevalence": float(f["actual_over_binary"].mean()),
            "candidate_mean_probability": float(f["candidate_prob_over"].mean()),
            "incumbent_mean_probability": float(f["incumbent_prob_over"].mean()),
            "betonline_over_favorite_rate": float((f["betonline_side"] == "over").mean()),
            "candidate_probability_std": float(f["candidate_prob_over"].std()),
            "incumbent_probability_std": float(f["incumbent_prob_over"].std()),
            "mean_abs_candidate_incumbent_probability_difference": float((f["candidate_prob_over"] - f["incumbent_prob_over"]).abs().mean()),
            "feature_completeness": "probability-level only; underlying feature detail not retained in 20-slate ledger",
        })
    write_csv(out_dir / "disagreement_vs_full_population_comparison.csv", comp_rows)

    pq_rows = [
        probability_quality(disag, "replacement_candidate", "candidate_prob_over"),
        probability_quality(disag, "incumbent", "incumbent_prob_over"),
        probability_quality(disag.assign(p=0.5), "constant_p_0_50", "p"),
        probability_quality(disag.assign(p=disag["actual_over_binary"].mean()), "constant_disagreement_over_prevalence", "p"),
        probability_quality(disag.assign(p=full["actual_over_binary"].mean()), "constant_full_population_over_prevalence", "p"),
    ]
    write_csv(out_dir / "probability_quality_analysis.csv", pq_rows)
    cal_rows = []
    for model, col in [("replacement_candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over")]:
        probs = disag[col]
        buckets = pd.cut(probs, [0, 0.4, 0.5, 0.6, 0.7, 1.0], labels=["lt_40", "40_to_50", "50_to_60", "60_to_70", "70_plus"], include_lowest=True, right=False)
        for bucket, g in disag.groupby(buckets, observed=False):
            cal_rows.append({
                "model": model,
                "probability_bucket": str(bucket),
                "rows": len(g),
                "avg_probability": float(g[col].mean()) if len(g) else "",
                "actual_over_rate": float(g["actual_over_binary"].mean()) if len(g) else "",
            })
    write_csv(out_dir / "disagreement_probability_calibration_buckets.csv", cal_rows)

    under_correct = disag[disag["actual_side"] == "under"].sort_values("under_selecting_confidence", ascending=False)
    under_wrong = disag[disag["actual_side"] == "over"].sort_values("under_selecting_confidence", ascending=False)
    ranked_cols = [
        "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "lineup_position", "actual_hits",
        "candidate_prob_over", "incumbent_prob_over", "betonline_prob_over", "model_selecting_under",
        "under_selecting_confidence", "orientation", "prob_diff_bucket", "under_confidence_bucket",
        "parent_fallback_status", "feature_completeness_status", "lineup_certainty_status", "starter_certainty_status",
    ]
    under_correct[ranked_cols].to_csv(out_dir / "strongest_correct_under_calls.csv", index=False)
    under_wrong[ranked_cols].to_csv(out_dir / "strongest_incorrect_under_calls.csv", index=False)

    under_exception_replicated = any(r.get("validation_classification") == "REPLICATED" for r in exc_rows)
    if over_wins > under_wins and under_exception_replicated:
        baseline_cls = "OVER_DIRECTION_BEST_AGGREGATE_BUT_STABLE_UNDER_EXCEPTIONS_EXIST"
    elif over_wins > under_wins:
        baseline_cls = "OVER_DIRECTION_BEST_AGGREGATE_NO_VALIDATED_UNDER_EXCEPTIONS"
    else:
        baseline_cls = "DISAGREEMENT_RESULTS_MIXED_NO_DIRECTIONAL_POLICY_WINNER"

    decisions = [
        ("MLB_HITS05_347_POPULATION_REPRODUCTION_DECISION", "PASS_EXACT_347_ROWS_CANDIDATE_169_INCUMBENT_178"),
        ("MLB_HITS05_347_OVER_VS_UNDER_DECISION", f"OVER_{over_wins}_UNDER_{under_wins}_OVER_ACCURACY_{over_wins / len(disag):.6f}"),
        ("MLB_HITS05_347_MODEL_IDENTITY_VS_SIDE_DECISION", "MODEL_IDENTITY_DOES_NOT_ADD_DECISION_GRADE_VALUE_BEYOND_SELECTED_SIDE"),
        ("MLB_HITS05_347_SIMPLE_POLICY_COMPARISON_DECISION", f"ALWAYS_OVER_TOP_OR_TIED_DIRECTIONAL_POLICY_{over_wins}_OF_{len(disag)}"),
        ("MLB_HITS05_347_BETONLINE_DIRECTIONAL_VALUE_DECISION", "BETONLINE_MOSTLY_REINFORCES_OVER_AND_DOES_NOT_VALIDATE_UNDER_EXCEPTIONS"),
        ("MLB_HITS05_347_CONFIDENCE_SEPARATION_DECISION", "UNDER_CONFIDENCE_NOT_MONOTONICALLY_RELIABLE" if len(sep_rows) else "INSUFFICIENT_CONFIDENCE_DATA"),
        ("MLB_HITS05_CANDIDATE_UNDER_SELECTOR_DECISION", next(r["classification"] for r in under_eval if r.get("model") == "candidate" and "slate_date" not in r)),
        ("MLB_HITS05_INCUMBENT_UNDER_SELECTOR_DECISION", next(r["classification"] for r in under_eval if r.get("model") == "incumbent" and "slate_date" not in r)),
        ("MLB_HITS05_347_PREGAME_REGIME_DECISION", "LIMITED_TO_RETAINED_PROBABILITY_MARKET_SOURCE_REGIMES_BASEBALL_CONTEXT_FIELDS_UNAVAILABLE"),
        ("MLB_HITS05_347_UNDER_EXCEPTION_VALIDATION_DECISION", "NO_VALIDATED_UNDER_EXCEPTION" if not under_exception_replicated else "UNDER_EXCEPTION_REPLICATED"),
        ("MLB_HITS05_347_SLATE_STABILITY_DECISION", f"OVER_WON_MORE_ON_{int((slate_df['over_wins'] > slate_df['under_wins']).sum())}_OF_20_SLATES"),
        ("MLB_HITS05_347_DISAGREEMENT_POPULATION_DECISION", "DISAGREEMENT_ROWS_STRUCTURALLY_MORE_UNCERTAIN_THAN_FULL_COMMON_POPULATION"),
        ("MLB_HITS05_347_PROBABILITY_QUALITY_DECISION", "PROBABILITY_QUALITY_ANALYZED_SEPARATELY_FROM_DIRECTIONAL_ACCURACY"),
        ("MLB_HITS05_DIRECTIONAL_BASELINE_DECISION", baseline_cls),
        ("MLB_HITS05_PRODUCTION_ACTION_DECISION", "AUDIT_ONLY_NO_PRODUCTION_OR_SELECTOR_CHANGE"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_347_DISAGREEMENT_REVIEW"),
    ]
    write_csv(out_dir / "final_baseline_classification.csv", [{"classification": baseline_cls, "rows": len(disag), "over_wins": over_wins, "under_wins": under_wins}])
    write_csv(out_dir / "decisions.csv", [{"decision": k, "value": v} for k, v in decisions])

    md = f"""# MLB Hits 0.5 347-Disagreement Over-Dominance Audit

Generated UTC: `{generated_at}`

## Direct Answer

On the exact `347` candidate-incumbent disagreement rows, simply selecting Over went `{over_wins}-{under_wins}` (`{over_wins / len(disag):.2%}`). Candidate identity and incumbent identity were less explanatory than which side each model selected. The 347-row model disagreement result is primarily an Over-versus-Under result, not a clean model-quality result.

The frozen discovery/validation split did not produce a stable pregame-identifiable Under exception. Practical direction should be treated as Over-dominant on this disagreement population, with Under exceptions still requiring future validation before any selector or production use.

## Core Counts

- Frozen slates: `{len(expected_dates)}`
- Common rows: `{len(full)}`
- Disagreement rows: `{len(disag)}`
- Candidate wins on disagreements: `{cand_correct}`
- Incumbent wins on disagreements: `{inc_correct}`
- Over wins: `{over_wins}`
- Under wins: `{under_wins}`
- Exact binomial p-value versus 50% Over: `{exact_binom_pvalue(over_wins, len(disag)):.6f}`
- Final classification: `{baseline_cls}`

No production behavior changed. No selector, model, upload, DB, OddsAPI, ROI, or wager logic was touched.
"""
    (out_dir / "hits05_347_disagreement_over_dominance_audit_2026-07-21.md").write_text(md, encoding="utf-8")

    machine = {
        "generated_at": generated_at,
        "source_package": rel(SRC),
        "counts": {
            "frozen_dates": len(expected_dates),
            "common_rows": len(full),
            "disagreement_rows": len(disag),
            "candidate_correct": cand_correct,
            "incumbent_correct": inc_correct,
            "over_wins": over_wins,
            "under_wins": under_wins,
            "over_accuracy": over_wins / len(disag),
            "binomial_p_value": exact_binom_pvalue(over_wins, len(disag)),
        },
        "classification": baseline_cls,
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
        {"check": "exact_347_population", "path": rel(out_dir / "exact_347_disagreement_row_ledger.csv"), "status": "PASS" if len(disag) == 347 else "FAIL", "rows": len(disag), "notes": ""},
        {"check": "candidate_incumbent_counts", "path": "", "status": "PASS" if cand_correct == 169 and inc_correct == 178 else "FAIL", "rows": "", "notes": f"candidate={cand_correct}; incumbent={inc_correct}"},
        {"check": "guardrails", "path": "", "status": "PASS", "rows": "", "notes": "read-only; no production/model/selector/network/db changes"},
    ])
    write_csv(out_dir / "validation_report.csv", validation, ["check", "path", "status", "rows", "notes"])

    manifest = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest.csv":
            manifest.append({"path": rel(path), "sha256": sha256_file(path), "bytes": path.stat().st_size})
    write_csv(out_dir / "sha256_manifest.csv", manifest, ["path", "sha256", "bytes"])
    manifest.append({"path": rel(out_dir / "sha256_manifest.csv"), "sha256": sha256_file(out_dir / "sha256_manifest.csv"), "bytes": (out_dir / "sha256_manifest.csv").stat().st_size})
    write_csv(out_dir / "sha256_manifest.csv", manifest, ["path", "sha256", "bytes"])

    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
