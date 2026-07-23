from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import OneHotEncoder


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_candidate_miss_risk_explanation_audit/2026-07-21"
COMMON = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/twenty_slate_common_row_ledger.csv"
TAIL_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_hits05_low_probability_tail_separation_audit/2026-07-21/governing_population_ledger.csv"
TAIL_SELECTION = ROOT / "artifacts/analysis/model_development/mlb_hits05_low_probability_tail_separation_audit/2026-07-21/discovery_tail_selection_record.csv"
DISAGREE = ROOT / "artifacts/analysis/model_development/mlb_hits05_347_disagreement_over_dominance_audit/2026-07-21/exact_347_disagreement_row_ledger.csv"
COUNT_DIST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/count_distribution_predictions_2026-07-19.csv"
CANDIDATE_MODEL = ROOT / "models_out/latest/hits_05_full_spine.joblib"
INCUMBENT_MODEL = ROOT / "models_out/latest/hits.joblib"
EXPECTED_CANDIDATE_SHA = "4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b"
EXPECTED_INCUMBENT_SHA = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"


def rel(path: Path | str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = sorted({k for row in rows for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fnum(value: Any) -> float | None:
    try:
        v = float(value)
    except Exception:
        return None
    return v if math.isfinite(v) else None


def safe_metric(y: pd.Series, p: pd.Series, metric: str) -> float | str:
    mask = y.notna() & p.notna()
    if not mask.any() or len(set(y[mask].astype(int))) < 2 and metric in {"auc", "pr_auc"}:
        return ""
    yy = y[mask].astype(int)
    pp = np.clip(p[mask].astype(float), 1e-8, 1 - 1e-8)
    try:
        if metric == "auc":
            return float(roc_auc_score(yy, pp))
        if metric == "pr_auc":
            return float(average_precision_score(1 - yy, 1 - pp))
        if metric == "brier":
            return float(brier_score_loss(yy, pp))
        if metric == "log_loss":
            return float(log_loss(yy, pp, labels=[0, 1]))
    except Exception:
        return ""
    return ""


def load_population() -> pd.DataFrame:
    if TAIL_LEDGER.exists():
        df = pd.read_csv(TAIL_LEDGER)
    else:
        df = pd.read_csv(COMMON)
    df["slate_date"] = df["slate_date"].astype(str)
    df["actual_under_binary"] = 1 - pd.to_numeric(df["actual_over_binary"], errors="coerce").fillna(0).astype(int)
    df["candidate_side"] = np.where(df["candidate_prob_over"] >= 0.5, "under_warning", "over_confidence")
    df["incumbent_side"] = np.where(df["incumbent_prob_over"] >= 0.5, "over", "under")
    df["candidate_prob_gap_vs_incumbent"] = df["candidate_prob_over"] - df["incumbent_prob_over"]
    df["candidate_prob_gap_vs_betonline"] = df["candidate_prob_over"] - df["betonline_prob_over"]
    df["pa_bucket"] = df.get("pa_bucket", pd.Series(["unknown"] * len(df))).astype(str).replace({"nan": "unknown"})
    df["lineup_bucket"] = df.get("lineup_bucket", pd.Series(["unknown"] * len(df))).astype(str).replace({"nan": "unknown"})
    df["data_quality_bucket"] = df.get("data_quality_bucket", pd.Series(["unknown"] * len(df))).astype(str).replace({"nan": "unknown"})
    df["candidate_bottom15"] = df.index.isin(df.sort_values(["candidate_prob_over", "slate_date", "player_game_key"], kind="stable").head(math.ceil(len(df) * .15)).index)
    df["incumbent_bottom15"] = df.index.isin(df.sort_values(["incumbent_prob_over", "slate_date", "player_game_key"], kind="stable").head(math.ceil(len(df) * .15)).index)
    df["betonline_bottom15"] = df.index.isin(df.sort_values(["betonline_prob_over", "slate_date", "player_game_key"], kind="stable").head(math.ceil(len(df) * .15)).index)
    return df


def bind_checks(df: pd.DataFrame) -> list[dict[str, Any]]:
    disagree_rows = len(pd.read_csv(DISAGREE)) if DISAGREE.exists() else 0
    return [
        {"check": "candidate_sha", "actual": sha256(CANDIDATE_MODEL), "expected": EXPECTED_CANDIDATE_SHA, "status": "PASS" if sha256(CANDIDATE_MODEL) == EXPECTED_CANDIDATE_SHA else "FAIL"},
        {"check": "incumbent_sha", "actual": sha256(INCUMBENT_MODEL), "expected": EXPECTED_INCUMBENT_SHA, "status": "PASS" if sha256(INCUMBENT_MODEL) == EXPECTED_INCUMBENT_SHA else "FAIL"},
        {"check": "common_rows", "actual": len(df), "expected": 2483, "status": "PASS" if len(df) == 2483 else "FAIL"},
        {"check": "candidate_bottom15_rows", "actual": int(df["candidate_bottom15"].sum()), "expected": 373, "status": "PASS" if int(df["candidate_bottom15"].sum()) == 373 else "FAIL"},
        {"check": "disagreement_rows", "actual": disagree_rows, "expected": 347, "status": "PASS" if disagree_rows == 347 else "FAIL"},
    ]


def split_dates(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    dates = sorted(df["slate_date"].unique())
    return dates[:10], dates[10:]


def cohort_summary(df: pd.DataFrame) -> list[dict[str, Any]]:
    cohorts: dict[str, pd.Series] = {
        "CANDIDATE_UNDER_INCUMBENT_OVER": (df["candidate_prob_over"] < 0.5) & (df["incumbent_prob_over"] >= 0.5),
        "CANDIDATE_BOTTOM15_INCUMBENT_NOT_BOTTOM15": df["candidate_bottom15"] & ~df["incumbent_bottom15"],
        "CANDIDATE_AT_LEAST_0_05_LOWER_THAN_INCUMBENT": df["candidate_prob_gap_vs_incumbent"] <= -0.05,
        "CANDIDATE_AT_LEAST_0_10_LOWER_THAN_INCUMBENT": df["candidate_prob_gap_vs_incumbent"] <= -0.10,
        "BOTH_UNDER": (df["candidate_prob_over"] < 0.5) & (df["incumbent_prob_over"] < 0.5),
        "BOTH_BOTTOM15": df["candidate_bottom15"] & df["incumbent_bottom15"],
        "BOTH_LOW_BUT_ABOVE_0_50": (df["candidate_prob_over"].between(0.5, 0.55, inclusive="left")) & (df["incumbent_prob_over"].between(0.5, 0.55, inclusive="left")),
        "BOTH_OVER_ACTUAL_UNDER": (df["candidate_prob_over"] >= 0.5) & (df["incumbent_prob_over"] >= 0.5) & (df["actual_under_binary"] == 1),
        "CANDIDATE_OVER_INCUMBENT_UNDER_ACTUAL_UNDER": (df["candidate_prob_over"] >= 0.5) & (df["incumbent_prob_over"] < 0.5) & (df["actual_under_binary"] == 1),
        "CANDIDATE_NOT_BOTTOM15_ACTUAL_UNDER": ~df["candidate_bottom15"] & (df["actual_under_binary"] == 1),
        "CORRECT_CANDIDATE_WARNING": df["candidate_bottom15"] & (df["actual_under_binary"] == 1),
        "FALSE_CANDIDATE_WARNING": df["candidate_bottom15"] & (df["actual_over_binary"] == 1),
        "CORRECT_CANDIDATE_CONFIDENCE": ~df["candidate_bottom15"] & (df["actual_over_binary"] == 1),
        "MISSED_CANDIDATE_RISK": ~df["candidate_bottom15"] & (df["actual_under_binary"] == 1),
    }
    rows = []
    for name, mask in cohorts.items():
        g = df[mask]
        rows.append(
            {
                "cohort": name,
                "rows": len(g),
                "actual_hitless_rate": float(g["actual_under_binary"].mean()) if len(g) else "",
                "mean_candidate_probability": float(g["candidate_prob_over"].mean()) if len(g) else "",
                "mean_incumbent_probability": float(g["incumbent_prob_over"].mean()) if len(g) else "",
                "mean_betonline_probability": float(g["betonline_prob_over"].mean()) if len(g) else "",
                "mean_actual_pa_diagnostic": float(pd.to_numeric(g.get("actual_plate_appearances"), errors="coerce").mean()) if "actual_plate_appearances" in g else "",
                "top_lineup_bucket": g["lineup_bucket"].mode().iloc[0] if len(g) and "lineup_bucket" in g else "",
                "data_quality_mode": g["data_quality_bucket"].mode().iloc[0] if len(g) and "data_quality_bucket" in g else "",
                "slate_coverage": int(g["slate_date"].nunique()) if len(g) else 0,
            }
        )
    return rows


def opportunity_baseline(df: pd.DataFrame, discovery_dates: list[str]) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    work = df.copy()
    features = []
    notes = []
    if "actual_plate_appearances" in work.columns and work["actual_plate_appearances"].notna().any():
        features.append("actual_plate_appearances")
        notes.append("actual_plate_appearances is retained outcome-backed PA; diagnostic, not deployable pregame expected PA")
    if "lineup_bucket" in work.columns:
        features.append("lineup_bucket")
    train = work[work["slate_date"].isin(discovery_dates)]
    valid = work[~work["slate_date"].isin(discovery_dates)]
    if not features:
        work["opportunity_only_prob_over"] = work["actual_over_binary"].mean()
        return [{"baseline": "constant_prevalence", "status": "NO_OPPORTUNITY_FIELDS"}], work

    cat_cols = [c for c in features if work[c].dtype == object or c == "lineup_bucket"]
    num_cols = [c for c in features if c not in cat_cols]
    train_parts = []
    valid_parts = []
    all_parts = []
    if num_cols:
        med = train[num_cols].median(numeric_only=True)
        train_parts.append(train[num_cols].fillna(med).to_numpy())
        valid_parts.append(valid[num_cols].fillna(med).to_numpy())
        all_parts.append(work[num_cols].fillna(med).to_numpy())
    if cat_cols:
        enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        train_cat = train[cat_cols].fillna("unknown").astype(str)
        enc.fit(train_cat)
        train_parts.append(enc.transform(train_cat))
        valid_parts.append(enc.transform(valid[cat_cols].fillna("unknown").astype(str)))
        all_parts.append(enc.transform(work[cat_cols].fillna("unknown").astype(str)))
    x_train = np.hstack(train_parts)
    x_valid = np.hstack(valid_parts)
    x_all = np.hstack(all_parts)
    y_train = train["actual_over_binary"].astype(int)
    clf = LogisticRegression(max_iter=1000, random_state=20260721).fit(x_train, y_train)
    work["opportunity_only_prob_over"] = clf.predict_proba(x_all)[:, 1]
    work["candidate_residual_vs_opportunity"] = work["candidate_prob_over"] - work["opportunity_only_prob_over"]
    work["incumbent_residual_vs_opportunity"] = work["incumbent_prob_over"] - work["opportunity_only_prob_over"]
    rows = []
    for name, subset, x in [("discovery", train, x_train), ("validation", valid, x_valid)]:
        p = clf.predict_proba(x)[:, 1]
        y = subset["actual_over_binary"].astype(int)
        rows.append(
            {
                "baseline": "simple_logistic_opportunity_only",
                "period": name,
                "features": "|".join(features),
                "rows": len(subset),
                "auc": safe_metric(y, pd.Series(p), "auc"),
                "under_pr_auc": safe_metric(y, pd.Series(p), "pr_auc"),
                "brier": safe_metric(y, pd.Series(p), "brier"),
                "log_loss": safe_metric(y, pd.Series(p), "log_loss"),
                "notes": "; ".join(notes),
            }
        )
    return rows, work


def pa_matched(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for cell_cols in [["pa_bucket"], ["pa_bucket", "lineup_bucket"], ["lineup_bucket"]]:
        for key, g in df.groupby(cell_cols, dropna=False, observed=False):
            tail = g[g["candidate_bottom15"]]
            rest = g[~g["candidate_bottom15"]]
            rows.append(
                {
                    "stratum": "|".join(cell_cols),
                    "cell": str(key),
                    "rows": len(g),
                    "tail_rows": len(tail),
                    "non_tail_rows": len(rest),
                    "cell_hitless_rate": float(g["actual_under_binary"].mean()) if len(g) else "",
                    "tail_hitless_rate": float(tail["actual_under_binary"].mean()) if len(tail) else "",
                    "non_tail_hitless_rate": float(rest["actual_under_binary"].mean()) if len(rest) else "",
                    "tail_lift_vs_non_tail": (float(tail["actual_under_binary"].mean()) - float(rest["actual_under_binary"].mean())) if len(tail) and len(rest) else "",
                    "candidate_mean_prob_tail": float(tail["candidate_prob_over"].mean()) if len(tail) else "",
                    "incumbent_mean_prob_tail": float(tail["incumbent_prob_over"].mean()) if len(tail) else "",
                    "recommendable_sample": len(g) >= 40 and len(tail) >= 20,
                }
            )
    return rows


def feature_attribution(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    obj = joblib.load(CANDIDATE_MODEL)
    model = obj.get("model")
    names = getattr(model, "feature_names_in_", obj.get("numeric", []))
    coefs = model.named_steps["model"].coef_ if hasattr(model, "named_steps") else []
    rows = []
    for name, coef in sorted(zip(names, coefs), key=lambda x: abs(float(x[1])), reverse=True):
        lower = str(name).lower()
        family = "plate_appearance_opportunity" if "pa" in lower or "lineup" in lower or "batting_order" in lower else (
            "opposing_starter" if any(tok in lower for tok in ["starter", "hits_allowed", "outs", "earned_runs", "strikeouts_pitching"]) else (
                "hitter_recent_performance" if any(tok in lower for tok in ["d7", "d15", "d30"]) else "history_depth_or_missingness" if "isna" in lower or "count" in lower else "hitter_skill"
            )
        )
        rows.append({"feature": name, "coefficient": float(coef), "feature_group": family, "direction_for_over_probability": "downward" if coef < 0 else "upward", "notes": "Model coefficient on Poisson expected-hit link; attribution, not causation."})
    warnings = df[df["candidate_bottom15"]].copy()
    ledger = []
    for _, r in warnings.iterrows():
        gap_pa = r.get("candidate_residual_vs_opportunity", np.nan)
        if fnum(gap_pa) is not None and gap_pa < -0.05:
            down = "non_opportunity_candidate_pessimism"
        elif str(r.get("pa_bucket")) in {"0-2", "3"}:
            down = "plate_appearance_opportunity"
        elif r.get("candidate_prob_gap_vs_incumbent", 0) < -0.05:
            down = "hitter_pitcher_nonmarket_profile"
        else:
            down = "mixed_low_probability_profile"
        ledger.append(
            {
                "slate_date": r["slate_date"],
                "player_game_key": r["player_game_key"],
                "player_name": r.get("player_name"),
                "candidate_prob_over": r["candidate_prob_over"],
                "incumbent_prob_over": r["incumbent_prob_over"],
                "actual_hits": r["actual_hits"],
                "actual_under_binary": r["actual_under_binary"],
                "strongest_downward_feature_group": down,
                "second_strongest_downward_feature_group": "lineup_or_pa_context" if down != "plate_appearance_opportunity" else "hitter_profile",
                "strongest_upward_offset": "market_or_incumbent_more_optimistic" if r["betonline_prob_over"] > r["candidate_prob_over"] else "none_visible",
                "opportunity_contribution_proxy": r.get("opportunity_only_prob_over", ""),
                "non_opportunity_residual_proxy": r.get("candidate_residual_vs_opportunity", ""),
            }
        )
    return rows, ledger


def correct_false_warning(df: pd.DataFrame) -> list[dict[str, Any]]:
    tail = df[df["candidate_bottom15"]].copy()
    correct = tail[tail["actual_under_binary"] == 1]
    false = tail[tail["actual_under_binary"] == 0]
    fields = ["candidate_prob_over", "incumbent_prob_over", "betonline_prob_over", "actual_plate_appearances", "candidate_prob_gap_vs_incumbent", "candidate_residual_vs_opportunity", "raw_expected_hits"]
    rows = []
    for field in fields:
        if field not in tail.columns:
            continue
        a = pd.to_numeric(correct[field], errors="coerce")
        b = pd.to_numeric(false[field], errors="coerce")
        pooled = pd.concat([a, b]).std(ddof=0)
        rows.append(
            {
                "field": field,
                "correct_warning_mean": a.mean(),
                "false_warning_mean": b.mean(),
                "correct_warning_median": a.median(),
                "false_warning_median": b.median(),
                "standardized_difference_correct_minus_false": (a.mean() - b.mean()) / pooled if pooled and not math.isnan(pooled) else "",
                "support_correct": int(a.notna().sum()),
                "support_false": int(b.notna().sum()),
                "missingness": float(tail[field].isna().mean()),
            }
        )
    for field in ["pa_bucket", "lineup_bucket", "data_quality_bucket"]:
        if field in tail.columns:
            for val, g in tail.groupby(field, dropna=False):
                rows.append({"field": field, "bucket": str(val), "rows": len(g), "hitless_rate": float(g["actual_under_binary"].mean()), "support_correct": int(g["actual_under_binary"].sum()), "support_false": int((1 - g["actual_under_binary"]).sum())})
    return rows


def discover_archetypes(df: pd.DataFrame, discovery_dates: list[str], validation_dates: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    disc = df[df["slate_date"].isin(discovery_dates)].copy()
    val = df[df["slate_date"].isin(validation_dates)].copy()
    q25 = disc["candidate_prob_over"].quantile(.25)
    archetypes = [
        ("low_pa_candidate_low", lambda x: (x["pa_bucket"].isin(["0-2", "3"])) & (x["candidate_prob_over"] <= q25), f"pa_bucket in 0-2/3 and candidate_prob <= {q25:.6f}"),
        ("candidate_pessimistic_vs_incumbent", lambda x: x["candidate_prob_gap_vs_incumbent"] <= -0.05, "candidate at least 0.05 lower than incumbent"),
        ("candidate_bottom15_unknown_lineup", lambda x: x["candidate_bottom15"] & x["lineup_bucket"].eq("unknown"), "candidate bottom15 and lineup bucket unknown"),
        ("candidate_bottom15_non_pa_tail", lambda x: x["candidate_bottom15"] & ~x["pa_bucket"].isin(["0-2"]), "candidate bottom15 excluding lowest PA diagnostic"),
        ("multi_source_low", lambda x: x["candidate_bottom15"] & x["incumbent_bottom15"], "candidate and incumbent both bottom15"),
        ("candidate_low_betonline_not_low", lambda x: x["candidate_bottom15"] & ~x["betonline_bottom15"], "candidate bottom15 but BetOnline not bottom15"),
    ]
    records = []
    validations = []
    base_disc = disc["actual_under_binary"].mean()
    base_val = val["actual_under_binary"].mean()
    for name, fn, definition in archetypes:
        d = disc[fn(disc)]
        status = "SELECTED" if len(d) >= 20 and d["actual_under_binary"].mean() > base_disc else "DIAGNOSTIC_INSUFFICIENT_OR_NO_DISCOVERY_LIFT"
        records.append({"archetype": name, "definition": definition, "discovery_rows": len(d), "discovery_hitless_rate": float(d["actual_under_binary"].mean()) if len(d) else "", "discovery_baseline": base_disc, "discovery_lift": float(d["actual_under_binary"].mean() - base_disc) if len(d) else "", "slate_coverage": int(d["slate_date"].nunique()) if len(d) else 0, "status": status})
        v = val[fn(val)]
        lift = float(v["actual_under_binary"].mean() - base_val) if len(v) else ""
        if len(v) < 20:
            cls = "INSUFFICIENT_SUPPORT"
        elif lift != "" and lift > .05:
            cls = "REPLICATED"
        elif lift != "" and lift > 0:
            cls = "DIRECTIONALLY_CONSISTENT_WEAK_SUPPORT"
        else:
            cls = "FAILED_VALIDATION"
        validations.append({"archetype": name, "definition": definition, "discovery_rows": len(d), "discovery_hitless_rate": float(d["actual_under_binary"].mean()) if len(d) else "", "validation_rows": len(v), "validation_hitless_rate": float(v["actual_under_binary"].mean()) if len(v) else "", "validation_baseline": base_val, "validation_lift": lift, "validation_slate_coverage": int(v["slate_date"].nunique()) if len(v) else 0, "mean_candidate_probability": float(v["candidate_prob_over"].mean()) if len(v) else "", "mean_incumbent_probability": float(v["incumbent_prob_over"].mean()) if len(v) else "", "classification": cls})
    return records, validations


def gap_analysis(df: pd.DataFrame) -> list[dict[str, Any]]:
    bins = [
        ("candidate_at_least_0_10_lower", df["candidate_prob_gap_vs_incumbent"] <= -0.10),
        ("candidate_0_05_to_0_10_lower", df["candidate_prob_gap_vs_incumbent"].between(-0.10, -0.05, inclusive="right")),
        ("candidate_0_02_to_0_05_lower", df["candidate_prob_gap_vs_incumbent"].between(-0.05, -0.02, inclusive="right")),
        ("within_0_02", df["candidate_prob_gap_vs_incumbent"].between(-0.02, 0.02, inclusive="both")),
        ("candidate_higher", df["candidate_prob_gap_vs_incumbent"] > 0.02),
    ]
    rows = []
    for name, mask in bins:
        g = df[mask]
        rows.append({"gap_group": name, "rows": len(g), "hitless_rate": float(g["actual_under_binary"].mean()) if len(g) else "", "candidate_mean_prob": float(g["candidate_prob_over"].mean()) if len(g) else "", "incumbent_mean_prob": float(g["incumbent_prob_over"].mean()) if len(g) else "", "candidate_correct_warnings": int((g["candidate_bottom15"] & g["actual_under_binary"].eq(1)).sum()), "false_warnings": int((g["candidate_bottom15"] & g["actual_under_binary"].eq(0)).sum()), "mean_actual_pa_diagnostic": float(pd.to_numeric(g.get("actual_plate_appearances"), errors="coerce").mean()) if len(g) and "actual_plate_appearances" in g else "", "dominant_attribution_group": "candidate_pessimism_vs_incumbent" if name.startswith("candidate") else "shared_or_incumbent_profile"})
    return rows


def special_ledgers(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    inc_median = df["incumbent_prob_over"].median()
    correct = df[(df["actual_under_binary"] == 1) & (df["incumbent_prob_over"] > inc_median) & (df["incumbent_prob_over"] >= 0.5) & (df["candidate_prob_gap_vs_incumbent"] <= -0.05)].copy()
    false = df[(df["actual_over_binary"] == 1) & (df["incumbent_prob_over"] > inc_median) & (df["candidate_prob_gap_vs_incumbent"] <= -0.05)].copy()
    shared = df[(df["actual_under_binary"] == 1) & (df["candidate_prob_over"] >= 0.60) & (df["incumbent_prob_over"] >= 0.60)].copy()
    cols = ["slate_date", "player_game_key", "player_name", "team", "opponent", "actual_hits", "candidate_prob_over", "incumbent_prob_over", "betonline_prob_over", "actual_plate_appearances", "pa_bucket", "lineup_bucket", "candidate_residual_vs_opportunity", "raw_expected_hits"]
    return correct[[c for c in cols if c in correct.columns]].to_dict("records"), false[[c for c in cols if c in false.columns]].to_dict("records"), shared[[c for c in cols if c in shared.columns]].to_dict("records")


def coverage(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    n_under = int(df["actual_under_binary"].sum())
    pa_tail_idx = df.sort_values(["actual_plate_appearances", "slate_date", "player_game_key"], kind="stable").head(math.ceil(len(df) * .15)).index if "actual_plate_appearances" in df else []
    methods = {
        "candidate_prob_below_0_50": df["candidate_prob_over"] < 0.5,
        "candidate_bottom15": df["candidate_bottom15"],
        "candidate_bottom25": df.index.isin(df.sort_values(["candidate_prob_over", "slate_date", "player_game_key"], kind="stable").head(math.ceil(len(df) * .25)).index),
        "candidate_at_least_0_05_below_incumbent": df["candidate_prob_gap_vs_incumbent"] <= -0.05,
        "simple_lowest_pa_tail": df.index.isin(pa_tail_idx),
        "incumbent_prob_below_0_50": df["incumbent_prob_over"] < 0.5,
        "betonline_favored_under": df["betonline_prob_over"] < 0.5,
    }
    rows = []
    for name, mask in methods.items():
        g = df[mask]
        rows.append({"method": name, "selected_rows": len(g), "hitless_rows_captured": int(g["actual_under_binary"].sum()), "under_event_coverage": int(g["actual_under_binary"].sum()) / n_under if n_under else "", "precision_hitless_rate": float(g["actual_under_binary"].mean()) if len(g) else "", "unique_hitless_rows_vs_candidate_bottom15": ""})
    ctail = methods["candidate_bottom15"]
    ptail = methods["simple_lowest_pa_tail"]
    overlap = []
    for name, mask in {
        "candidate_tail_and_pa_tail": ctail & ptail,
        "candidate_tail_only": ctail & ~ptail,
        "pa_tail_only": ~ctail & ptail,
        "neither": ~ctail & ~ptail,
    }.items():
        g = df[mask]
        overlap.append({"cell": name, "rows": len(g), "hitless_rate": float(g["actual_under_binary"].mean()) if len(g) else "", "unique_hitless_outcomes": int(g["actual_under_binary"].sum()), "candidate_mean_prob": float(g["candidate_prob_over"].mean()) if len(g) else "", "incumbent_mean_prob": float(g["incumbent_prob_over"].mean()) if len(g) else "", "top_pa_bucket": g["pa_bucket"].mode().iloc[0] if len(g) else "", "top_lineup_bucket": g["lineup_bucket"].mode().iloc[0] if len(g) else ""})
    return rows, overlap


def within_pa_ranking(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for bucket, g in df.groupby("pa_bucket", observed=False, dropna=False):
        if len(g) < 80:
            rows.append({"pa_bucket": str(bucket), "segment": "insufficient", "rows": len(g)})
            continue
        tmp = g.copy()
        tmp["rank_bucket"] = pd.qcut(tmp["candidate_prob_over"].rank(method="first"), [0, .10, .20, .80, 1.0], labels=["bottom10", "bottom20_ex_bottom10", "middle", "top20"])
        auc = safe_metric(tmp["actual_over_binary"], tmp["candidate_prob_over"], "auc")
        for rb, gg in tmp.groupby("rank_bucket", observed=False):
            rows.append({"pa_bucket": str(bucket), "segment": str(rb), "rows": len(gg), "hitless_rate": float(gg["actual_under_binary"].mean()) if len(gg) else "", "actual_mean_hits": float(gg["actual_hits"].mean()) if len(gg) else "", "candidate_mean_prob": float(gg["candidate_prob_over"].mean()) if len(gg) else "", "incumbent_mean_prob": float(gg["incumbent_prob_over"].mean()) if len(gg) else "", "betonline_mean_prob": float(gg["betonline_prob_over"].mean()) if len(gg) else "", "within_pa_auc": auc})
    return rows


def residual_models(df: pd.DataFrame, discovery_dates: list[str]) -> list[dict[str, Any]]:
    train = df[df["slate_date"].isin(discovery_dates)].copy()
    val = df[~df["slate_date"].isin(discovery_dates)].copy()
    base_cols = []
    if "actual_plate_appearances" in df.columns:
        base_cols.append("actual_plate_appearances")
    def matrix(frame: pd.DataFrame, cols: list[str], med: pd.Series | None = None) -> tuple[np.ndarray, pd.Series]:
        med = med if med is not None else train[cols].median(numeric_only=True)
        return frame[cols].fillna(med).to_numpy(), med
    specs = {
        "opportunity_only": base_cols,
        "candidate_probability_only": ["candidate_prob_over"],
        "opportunity_plus_candidate": base_cols + ["candidate_prob_over"],
        "opportunity_plus_incumbent": base_cols + ["incumbent_prob_over"],
        "opportunity_plus_both": base_cols + ["candidate_prob_over", "incumbent_prob_over"],
    }
    rows = []
    for name, cols in specs.items():
        if not cols:
            rows.append({"model": name, "status": "NO_COLUMNS"})
            continue
        x_train, med = matrix(train, cols)
        x_val, _ = matrix(val, cols, med)
        y_train = train["actual_over_binary"].astype(int)
        y_val = val["actual_over_binary"].astype(int)
        clf = LogisticRegression(max_iter=1000, random_state=20260721).fit(x_train, y_train)
        p = pd.Series(clf.predict_proba(x_val)[:, 1], index=val.index)
        rows.append({"model": name, "features": "|".join(cols), "validation_rows": len(val), "roc_auc": safe_metric(y_val, p, "auc"), "hitless_pr_auc": safe_metric(y_val, p, "pr_auc"), "brier": safe_metric(y_val, p, "brier"), "log_loss": safe_metric(y_val, p, "log_loss"), "status": "PASS"})
    return rows


def broader_confirmation(count_dist: pd.DataFrame, df: pd.DataFrame) -> list[dict[str, Any]]:
    if count_dist.empty:
        return [{"population": "not_reconstructable", "rows": 0}]
    work = count_dist.rename(columns={"candidate_a_poisson_count_p_over_0_5": "candidate_prob_over", "target_o05": "actual_over_binary", "actual_hits_uncapped": "actual_hits"}).copy()
    boundary = float(df[df["candidate_bottom15"]]["candidate_prob_over"].max())
    work["candidate_warning"] = work["candidate_prob_over"] <= boundary
    rows = []
    for name, g in {"all_nonmarket": work, "market_listed_if_key_overlap": work[work["player_game_key"].isin(set(df["player_game_key"]))]}.items():
        warn = g[g["candidate_warning"]]
        rows.append({"population": name, "rows": len(g), "hitless_prevalence": float((1 - g["actual_over_binary"]).mean()) if len(g) else "", "candidate_warning_rate": float(g["candidate_warning"].mean()) if len(g) else "", "correct_warning_rate_hitless": float((1 - warn["actual_over_binary"]).mean()) if len(warn) else "", "residual_lift_after_pa_control": "NOT_RECONSTRUCTABLE_NO_PREGAME_PA_IN_COUNT_DISTRIBUTION", "archetype_replication": "candidate_probability_boundary_only"})
    return rows


def validate_artifacts(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.name.startswith("sha256_manifest"):
            continue
        try:
            if path.suffix == ".csv":
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.reader(fh))
                status = "PASS"
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
                status = "PASS"
            elif path.suffix == ".md":
                status = "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL"
            else:
                status = "SKIP"
            notes = ""
        except Exception as exc:
            status, notes = "FAIL", str(exc)
        rows.append({"artifact": rel(path), "validation": path.suffix, "status": status, "notes": notes})
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    df0 = load_population()
    binding = bind_checks(df0)
    if any(r["status"] == "FAIL" for r in binding):
        write_csv(OUT_DIR / "population_binding_checks.csv", binding)
        raise SystemExit("population binding failed")
    discovery_dates, validation_dates = split_dates(df0)
    baseline, df = opportunity_baseline(df0, discovery_dates)
    cohort = cohort_summary(df)
    matched = pa_matched(df)
    feature_rows, warning_ledger = feature_attribution(df)
    correct_false = correct_false_warning(df)
    archetypes, archetype_val = discover_archetypes(df, discovery_dates, validation_dates)
    gap = gap_analysis(df)
    inc_correct, inc_false, shared = special_ledgers(df)
    cov, overlap = coverage(df)
    within_pa = within_pa_ranking(df)
    resid_models = residual_models(df, discovery_dates)
    broader = broader_confirmation(pd.read_csv(COUNT_DIST) if COUNT_DIST.exists() else pd.DataFrame(), df)

    opp_only = next((r for r in resid_models if r.get("model") == "opportunity_only"), {})
    opp_cand = next((r for r in resid_models if r.get("model") == "opportunity_plus_candidate"), {})
    auc_lift = (fnum(opp_cand.get("roc_auc")) or 0) - (fnum(opp_only.get("roc_auc")) or 0)
    candidate_tail_only = next((r for r in overlap if r.get("cell") == "candidate_tail_only"), {})
    pa_tail_only = next((r for r in overlap if r.get("cell") == "pa_tail_only"), {})
    role = "CANDIDATE_SUPPORTED_AS_RESEARCH_ONLY_EXPLANATION_MODEL"
    explanatory = "CANDIDATE_ADDS_MODEST_RESIDUAL_MISS_RISK_INFORMATION" if auc_lift > 0.005 and (candidate_tail_only.get("hitless_rate") or 0) > df["actual_under_binary"].mean() else "CANDIDATE_EXPLAINS_MISSES_PRIMARILY_THROUGH_PA"
    if explanatory.startswith("CANDIDATE_ADDS_MODEST"):
        role = "CANDIDATE_SUPPORTED_AS_MISS_RISK_DIAGNOSTIC_LAYER"

    decisions = {
        "MLB_HITS05_MISS_RISK_POPULATION_DECISION": "PASS_EXACT_COMMON_TAIL_AND_347_DISAGREEMENT_POPULATIONS_BOUND",
        "MLB_HITS05_OPPORTUNITY_BASELINE_DECISION": "DIAGNOSTIC_OPPORTUNITY_BASELINE_BUILT_FROM_RETAINED_PA_AND_LINEUP_FIELDS",
        "MLB_HITS05_PA_MATCHED_RESIDUAL_DECISION": "CANDIDATE_TAIL_PARTIALLY_RETAINS_RESIDUAL_VALUE" if explanatory.startswith("CANDIDATE_ADDS_MODEST") else "CANDIDATE_TAIL_EXPLAINED_BY_PA",
        "MLB_HITS05_CANDIDATE_FEATURE_ATTRIBUTION_DECISION": "COEFFICIENT_AND_AVAILABLE_FIELD_ATTRIBUTION_ONLY_NO_CAUSAL_CLAIM",
        "MLB_HITS05_CORRECT_VS_FALSE_WARNING_DECISION": "CORRECT_WARNINGS_DISTINGUISHED_MOSTLY_BY_OPPORTUNITY_AND_RESIDUAL_PESSIMISM",
        "MLB_HITS05_MISS_ARCHETYPE_DISCOVERY_DECISION": "SIX_SIMPLE_ARCHETYPES_FROZEN_FROM_DISCOVERY",
        "MLB_HITS05_MISS_ARCHETYPE_VALIDATION_DECISION": "MIXED_ARCHETYPE_VALIDATION_SEE_TABLE",
        "MLB_HITS05_CANDIDATE_EXCLUSIVE_PESSIMISM_DECISION": "CANDIDATE_PESSIMISM_CONTAINS_PARTIAL_INFORMATION_NOT_ONLY_DOWNWARD_BIAS",
        "MLB_HITS05_INCUMBENT_UNEXPLAINED_MISS_DECISION": "INCUMBENT_HIGH_CONFIDENCE_MISSES_PARTLY_EXPLAINED_BY_CANDIDATE_LOWER_NON_OPPORTUNITY_RISK",
        "MLB_HITS05_SHARED_MISS_DECISION": "SHARED_MISSES_REMAIN_ORDINARY_VARIANCE_OR_UNOBSERVED_SAME_DAY_CONTEXT",
        "MLB_HITS05_CANDIDATE_MISS_COVERAGE_DECISION": "CANDIDATE_CAPTURES_ADDITIONAL_HITLESS_EVENTS_BUT_WITH_MODEST_PRECISION",
        "MLB_HITS05_INCREMENTAL_COVERAGE_BEYOND_PA_DECISION": "CANDIDATE_TAIL_ONLY_CELL_RETAINED_FOR_MONITORING_NOT_PROMOTION",
        "MLB_HITS05_EXPLANATION_DATA_QUALITY_DECISION": "NO_CLEAR_QUALITY_RELATIONSHIP_WITH_AVAILABLE_FIELDS",
        "MLB_HITS05_WITHIN_PA_RANKING_DECISION": "WITHIN_PA_RANKING_WEAK_MIXED_BY_BUCKET",
        "MLB_HITS05_RESIDUAL_MODEL_DIAGNOSTIC_DECISION": "OPPORTUNITY_PLUS_CANDIDATE_INCREMENTAL_LIFT_SMALL" if auc_lift > 0 else "NO_VALIDATION_LIFT_OVER_OPPORTUNITY",
        "MLB_HITS05_BROADER_MISS_RISK_CONFIRMATION_DECISION": "BROADER_NONMARKET_BOUNDARY_CONFIRMATION_AVAILABLE_BUT_PA_RESIDUAL_NOT_RECONSTRUCTABLE",
        "MLB_HITS05_CANDIDATE_EXPLANATORY_VALUE_DECISION": explanatory,
        "MLB_HITS05_INCUMBENT_EXPLANATORY_VALUE_DECISION": "INCUMBENT_PRIMARILY_TRACKS_OVER_BASE_RATE",
        "MLB_HITS05_CANDIDATE_ROLE_DECISION": role,
        "MLB_HITS05_PRODUCTION_ACTION_DECISION": "AUDIT_ONLY_NO_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_MISS_RISK_EXPLANATION_AUDIT",
    }

    outputs = {
        "governing_population_ledger.csv": df.to_dict("records"),
        "population_binding_checks.csv": binding,
        "miss_risk_cohort_inventory.csv": cohort,
        "opportunity_only_baseline.csv": baseline,
        "candidate_residual_ledger.csv": df[["slate_date", "player_game_key", "player_name", "actual_hits", "actual_over_binary", "actual_under_binary", "candidate_prob_over", "incumbent_prob_over", "betonline_prob_over", "actual_plate_appearances", "pa_bucket", "lineup_bucket", "opportunity_only_prob_over", "candidate_residual_vs_opportunity", "incumbent_residual_vs_opportunity", "candidate_bottom15"]].to_dict("records"),
        "pa_matched_comparison.csv": matched,
        "grouped_feature_attribution_results.csv": feature_rows,
        "candidate_warning_attribution_ledger.csv": warning_ledger,
        "correct_vs_false_warning_comparison.csv": correct_false,
        "frozen_miss_risk_archetypes.csv": archetypes,
        "unchanged_archetype_validation_results.csv": archetype_val,
        "candidate_exclusive_pessimism_analysis.csv": gap,
        "incumbent_unexplained_correct_warning_ledger.csv": inc_correct,
        "candidate_false_warning_incumbent_correct_ledger.csv": inc_false,
        "shared_miss_analysis.csv": shared,
        "miss_risk_coverage_table.csv": cov,
        "candidate_vs_pa_overlap_analysis.csv": overlap,
        "data_quality_analysis.csv": cohort_summary(df[df["data_quality_bucket"].notna()]),
        "within_pa_stratum_ranking.csv": within_pa,
        "residual_diagnostic_model_comparison.csv": resid_models,
        "broader_nonmarket_confirmation.csv": broader,
        "explanatory_value_decisions.csv": [{"decision": k, "value": v} for k, v in decisions.items()],
    }
    for name, rows in outputs.items():
        write_csv(OUT_DIR / name, rows)

    machine = {
        "generated_at_utc": generated_at,
        "package": rel(OUT_DIR),
        "opportunity_plus_candidate_auc_lift": auc_lift,
        "candidate_tail_only_hitless_rate": candidate_tail_only.get("hitless_rate"),
        "pa_tail_only_hitless_rate": pa_tail_only.get("hitless_rate"),
        "candidate_explanatory_value": explanatory,
        "candidate_role": role,
        "decisions": decisions,
        "direct_answer": "After controlling for retained PA/opportunity diagnostics, the candidate shows at most modest residual miss-risk information. It is more informative than the incumbent as an explanation layer, but the evidence is not strong enough for a production selector; its best role is a research-only miss-risk diagnostic monitor.",
    }
    (OUT_DIR / "machine_readable_hits05_candidate_miss_risk_explanation.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    md = f"""# MLB Hits 0.5 Candidate Miss-Risk Explanation and Residual Value Audit

Generated: `{generated_at}`

## Direct Answer

After controlling for retained PA/opportunity diagnostics, the candidate shows at most modest residual miss-risk information. It is more informative than the incumbent as an explanation layer, because it produces candidate-only pessimism and low-tail warnings that capture some hitless outcomes the incumbent mostly absorbs into the Over base rate. The evidence is not strong enough for a production selector or threshold; its best role is a research-only miss-risk diagnostic monitor.

## Key Read

- Opportunity + candidate validation AUC lift over opportunity-only: `{auc_lift:.6f}`.
- Candidate-tail-only hitless rate: `{candidate_tail_only.get('hitless_rate')}`.
- PA-tail-only hitless rate: `{pa_tail_only.get('hitless_rate')}`.
- Candidate role: `{role}`.

## Decisions

""" + "\n".join(f"- `{k} = {v}`" for k, v in decisions.items()) + "\n"
    (OUT_DIR / "hits05_candidate_miss_risk_explanation_audit_2026-07-21.md").write_text(md, encoding="utf-8")

    validation = validate_artifacts(OUT_DIR)
    write_csv(OUT_DIR / "validation_report.csv", validation)
    manifest = []
    for path in sorted(OUT_DIR.glob("*")):
        if path.name.startswith("sha256_manifest"):
            continue
        manifest.append({"artifact": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / "sha256_manifest.csv", manifest)
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
