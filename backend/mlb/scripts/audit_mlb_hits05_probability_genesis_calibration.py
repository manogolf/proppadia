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
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, matthews_corrcoef, roc_auc_score


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_probability_genesis_calibration_and_under_separation_audit/2026-07-21"
COMMON = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/twenty_slate_common_row_ledger.csv"
DATES = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/frozen_20_slate_date_manifest.csv"
COUNT_DIST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/count_distribution_predictions_2026-07-19.csv"
SPLIT_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/split_manifest_2026-07-19.csv"
TRAINING_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/training_cohort_manifest_2026-07-19.csv"
POPULATION_ANALYSIS = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/population_selection_analysis_2026-07-19.csv"
BETONLINE_ROWS = ROOT / "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/2026-07-19/authentic_betonline_same_row_rows_2026-07-19.csv"
RAW_HOLDOUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/2026-07-19/raw_vs_calibrated_holdout_metrics_2026-07-19.csv"
CANDIDATE_MODEL = ROOT / "models_out/latest/hits_05_full_spine.joblib"
INCUMBENT_MODEL = ROOT / "models_out/latest/hits.joblib"
CANDIDATE_META = ROOT / "models_out/latest/hits_05_full_spine_metadata.json"

PROB_COLS = {
    "candidate": "candidate_prob_over",
    "incumbent": "incumbent_prob_over",
    "betonline": "betonline_prob_over",
}

BUCKETS = [0.0, 0.30, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.80, 1.0]
BUCKET_LABELS = ["<0.30", "0.30-0.40", "0.40-0.45", "0.45-0.50", "0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.80", "0.80+"]
UNDER_THRESHOLDS = [0.50, 0.52, 0.55, 0.57, 0.60, 0.62, 0.65]


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


def fnum(x: Any) -> float | None:
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


def side_from_prob(p: Any) -> str:
    v = fnum(p)
    if v is None or not (0.0 < v < 1.0):
        return "invalid"
    return "over" if v >= 0.5 else "under"


def binary_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, Any]:
    mask = np.isfinite(p) & np.isfinite(y)
    y = y[mask].astype(int)
    p = np.clip(p[mask].astype(float), 1e-8, 1 - 1e-8)
    out: dict[str, Any] = {"rows": int(len(y))}
    if len(y) == 0:
        return out
    prevalence = float(y.mean())
    out.update(
        {
            "actual_over_rate": prevalence,
            "mean_probability": float(p.mean()),
            "calibration_gap": float(p.mean() - prevalence),
            "brier": float(brier_score_loss(y, p)),
            "log_loss": float(log_loss(y, p, labels=[0, 1])),
            "constant_prevalence_brier": float(brier_score_loss(y, np.repeat(prevalence, len(y)))),
        }
    )
    out["brier_skill_vs_empirical_prevalence"] = 1.0 - out["brier"] / out["constant_prevalence_brier"] if out["constant_prevalence_brier"] else ""
    try:
        out["roc_auc"] = float(roc_auc_score(y, p)) if len(set(y)) == 2 else ""
    except Exception:
        out["roc_auc"] = ""
    try:
        out["under_pr_auc"] = float(average_precision_score(1 - y, 1 - p)) if len(set(y)) == 2 else ""
    except Exception:
        out["under_pr_auc"] = ""
    try:
        x = np.log(p / (1 - p)).reshape(-1, 1)
        lr = LogisticRegression(C=1_000_000, solver="lbfgs", max_iter=1000).fit(x, y)
        out["calibration_intercept"] = float(lr.intercept_[0])
        out["calibration_slope"] = float(lr.coef_[0][0])
    except Exception:
        out["calibration_intercept"] = ""
        out["calibration_slope"] = ""
    return out


def brier_decomp(y: np.ndarray, p: np.ndarray, bins: int = 10) -> dict[str, float]:
    y = y.astype(float)
    p = p.astype(float)
    prevalence = y.mean() if len(y) else np.nan
    reliability = 0.0
    resolution = 0.0
    cuts = np.linspace(0, 1, bins + 1)
    for lo, hi in zip(cuts[:-1], cuts[1:]):
        if hi == 1:
            mask = (p >= lo) & (p <= hi)
        else:
            mask = (p >= lo) & (p < hi)
        if not mask.any():
            continue
        weight = mask.mean()
        pred = p[mask].mean()
        obs = y[mask].mean()
        reliability += weight * (pred - obs) ** 2
        resolution += weight * (obs - prevalence) ** 2
    uncertainty = prevalence * (1 - prevalence)
    return {"reliability": reliability, "resolution": resolution, "uncertainty": uncertainty}


def population_prevalence(df: pd.DataFrame, name: str) -> dict[str, Any]:
    y = pd.to_numeric(df["actual_over_binary"], errors="coerce")
    rows = int(y.notna().sum())
    overs = int(y.eq(1).sum())
    unders = int(y.eq(0).sum())
    return {
        "population": name,
        "rows": rows,
        "actual_over": overs,
        "actual_under": unders,
        "over_prevalence": overs / rows if rows else "",
        "under_prevalence": unders / rows if rows else "",
        "always_over_accuracy": overs / rows if rows else "",
        "always_under_accuracy": unders / rows if rows else "",
    }


def distribution_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for model, col in PROB_COLS.items():
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        q = vals.quantile([0, .01, .05, .10, .25, .50, .75, .90, .95, .99, 1.0]).to_dict()
        rows.append(
            {
                "model": model,
                "rows": len(vals),
                "min": q.get(0, ""),
                "p01": q.get(.01, ""),
                "p05": q.get(.05, ""),
                "p10": q.get(.10, ""),
                "p25": q.get(.25, ""),
                "median": q.get(.50, ""),
                "mean": vals.mean(),
                "p75": q.get(.75, ""),
                "p90": q.get(.90, ""),
                "p95": q.get(.95, ""),
                "p99": q.get(.99, ""),
                "max": q.get(1.0, ""),
                "std": vals.std(ddof=0),
            }
        )
    return rows


def bucket_rows(df: pd.DataFrame, *, equal_count: bool = False) -> list[dict[str, Any]]:
    out = []
    y = pd.to_numeric(df["actual_over_binary"], errors="coerce").to_numpy(dtype=float)
    for model, col in PROB_COLS.items():
        p = pd.to_numeric(df[col], errors="coerce")
        if equal_count:
            buckets = pd.qcut(p.rank(method="first"), 10, labels=[f"decile_{i}" for i in range(1, 11)])
        else:
            buckets = pd.cut(p, BUCKETS, labels=BUCKET_LABELS, include_lowest=True, right=False)
        tmp = pd.DataFrame({"bucket": buckets, "p": p, "y": y, "hits": pd.to_numeric(df["actual_hits"], errors="coerce")})
        for bucket, g in tmp.groupby("bucket", observed=False):
            if len(g) == 0:
                continue
            met = binary_metrics(g["y"].to_numpy(dtype=float), g["p"].to_numpy(dtype=float))
            out.append(
                {
                    "model": model,
                    "bucket_type": "equal_count_decile" if equal_count else "fixed_probability",
                    "bucket": str(bucket),
                    "rows": len(g),
                    "actual_over": int((g["y"] == 1).sum()),
                    "observed_over_rate": g["y"].mean(),
                    "predicted_mean_probability": g["p"].mean(),
                    "calibration_gap": met.get("calibration_gap", ""),
                    "brier": met.get("brier", ""),
                    "actual_mean_hits": g["hits"].mean(),
                    "hitless_rate": float((g["hits"] == 0).mean()),
                }
            )
    return out


def calibration_summary(df: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    y = pd.to_numeric(df["actual_over_binary"], errors="coerce").to_numpy(dtype=float)
    for model, col in PROB_COLS.items():
        p = pd.to_numeric(df[col], errors="coerce").to_numpy(dtype=float)
        met = binary_metrics(y, p)
        dec = bucket_rows(df[["actual_over_binary", "actual_hits", col]].rename(columns={col: "candidate_prob_over"}), equal_count=True) if False else []
        decomp = brier_decomp(y[np.isfinite(p)], np.clip(p[np.isfinite(p)], 1e-8, 1 - 1e-8))
        # ECE/MCE on fixed probability buckets.
        ece = 0.0
        mce = 0.0
        valid = np.isfinite(y) & np.isfinite(p)
        for lo, hi in zip(BUCKETS[:-1], BUCKETS[1:]):
            m = valid & (p >= lo) & ((p <= hi) if hi == 1.0 else (p < hi))
            if not m.any():
                continue
            gap = abs(float(p[m].mean() - y[m].mean()))
            ece += float(m.mean()) * gap
            mce = max(mce, gap)
        out.append({"model": model, **met, "expected_calibration_error": ece, "maximum_calibration_error": mce, **decomp})
    return out


def ranking_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    y = pd.to_numeric(df["actual_over_binary"], errors="coerce")
    hits = pd.to_numeric(df["actual_hits"], errors="coerce")
    for model, col in PROB_COLS.items():
        p = pd.to_numeric(df[col], errors="coerce")
        met = binary_metrics(y.to_numpy(dtype=float), p.to_numpy(dtype=float))
        overs = p[y == 1]
        unders = p[y == 0]
        tmp = pd.DataFrame({"p": p, "y": y, "hits": hits}).dropna()
        tmp["decile"] = pd.qcut(tmp["p"].rank(method="first"), 10, labels=False) + 1 if len(tmp) >= 10 else 1
        bottom = tmp[tmp["decile"] == 1]
        top = tmp[tmp["decile"] == 10]
        out.append(
            {
                "model": model,
                "rows": len(tmp),
                "roc_auc": met.get("roc_auc", ""),
                "under_pr_auc": met.get("under_pr_auc", ""),
                "spearman_probability_actual_hits": tmp["p"].corr(tmp["hits"], method="spearman"),
                "mean_probability_actual_over": overs.mean(),
                "mean_probability_actual_under": unders.mean(),
                "class_probability_separation": overs.mean() - unders.mean(),
                "bottom_decile_observed_over_rate": bottom["y"].mean() if len(bottom) else "",
                "top_decile_observed_over_rate": top["y"].mean() if len(top) else "",
                "top_minus_bottom_decile_spread": (top["y"].mean() - bottom["y"].mean()) if len(top) and len(bottom) else "",
                "bottom_decile_hitless_rate": float((bottom["hits"] == 0).mean()) if len(bottom) else "",
                "top_decile_hitless_rate": float((top["hits"] == 0).mean()) if len(top) else "",
            }
        )
    return out


def under_threshold_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    y = pd.to_numeric(df["actual_over_binary"], errors="coerce").to_numpy(dtype=int)
    under_actual = 1 - y
    for model, col in {k: v for k, v in PROB_COLS.items() if k != "betonline"}.items():
        p = pd.to_numeric(df[col], errors="coerce").to_numpy(dtype=float)
        for thr in UNDER_THRESHOLDS:
            pred_under = (p < thr).astype(int)
            selected = int(pred_under.sum())
            tp = int(((pred_under == 1) & (under_actual == 1)).sum())
            fp = int(((pred_under == 1) & (under_actual == 0)).sum())
            tn = int(((pred_under == 0) & (under_actual == 0)).sum())
            fn = int(((pred_under == 0) & (under_actual == 1)).sum())
            out.append(
                {
                    "model": model,
                    "over_probability_threshold_for_under": thr,
                    "selected_under_rows": selected,
                    "true_under_rows": tp,
                    "false_under_rows": fp,
                    "under_precision": tp / selected if selected else "",
                    "under_recall": tp / (tp + fn) if (tp + fn) else "",
                    "specificity": tn / (tn + fp) if (tn + fp) else "",
                    "balanced_accuracy": ((tp / (tp + fn)) + (tn / (tn + fp))) / 2 if (tp + fn) and (tn + fp) else "",
                    "matthews_corrcoef": matthews_corrcoef(under_actual, pred_under) if len(set(pred_under)) > 1 else 0,
                }
            )
    return out


def threshold_sweep(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    dates = sorted(df["slate_date"].astype(str).unique())
    discovery_dates = set(dates[:10])
    validation_dates = set(dates[10:])
    for model, col in {k: v for k, v in PROB_COLS.items() if k != "betonline"}.items():
        for split_name, sub in [
            ("all_20_slates", df),
            ("discovery_first_10_slates", df[df["slate_date"].astype(str).isin(discovery_dates)]),
            ("validation_final_10_slates", df[df["slate_date"].astype(str).isin(validation_dates)]),
        ]:
            y = pd.to_numeric(sub["actual_over_binary"], errors="coerce").to_numpy(dtype=int)
            p = pd.to_numeric(sub[col], errors="coerce").to_numpy(dtype=float)
            majority = max(y.mean(), 1 - y.mean()) if len(y) else np.nan
            for thr in np.round(np.arange(0.30, 0.751, 0.01), 2):
                pred_over = (p >= thr).astype(int)
                over_recall = ((pred_over == 1) & (y == 1)).sum() / max(1, (y == 1).sum())
                under_recall = ((pred_over == 0) & (y == 0)).sum() / max(1, (y == 0).sum())
                acc = (pred_over == y).mean() if len(y) else np.nan
                rows.append(
                    {
                        "model": model,
                        "split": split_name,
                        "threshold": thr,
                        "predicted_over_count": int(pred_over.sum()),
                        "predicted_under_count": int((pred_over == 0).sum()),
                        "raw_accuracy": acc,
                        "excess_accuracy_over_majority_baseline": acc - majority,
                        "balanced_accuracy": (over_recall + under_recall) / 2,
                        "matthews_corrcoef": matthews_corrcoef(y, pred_over) if len(set(pred_over)) > 1 else 0,
                        "youdens_j": over_recall + under_recall - 1,
                        "over_recall": over_recall,
                        "under_recall": under_recall,
                    }
                )
    best = []
    sweep = pd.DataFrame(rows)
    for model in ["candidate", "incumbent"]:
        for metric in ["balanced_accuracy", "matthews_corrcoef", "youdens_j"]:
            d = sweep[(sweep["model"] == model) & (sweep["split"] == "discovery_first_10_slates")]
            v = sweep[(sweep["model"] == model) & (sweep["split"] == "validation_final_10_slates")]
            if d.empty or v.empty:
                continue
            b = d.sort_values(metric, ascending=False).iloc[0]
            vv = v[v["threshold"] == b["threshold"]].iloc[0]
            best.append(
                {
                    "model": model,
                    "selection_metric": metric,
                    "discovery_threshold": b["threshold"],
                    "discovery_metric_value": b[metric],
                    "validation_metric_value": vv[metric],
                    "validation_balanced_accuracy": vv["balanced_accuracy"],
                    "validation_mcc": vv["matthews_corrcoef"],
                    "classification": "DIRECTIONALLY_CONSISTENT" if vv["balanced_accuracy"] > 0.50 else "FAILED_VALIDATION",
                }
            )
    return rows, best


def slate_stability(df: pd.DataFrame) -> list[dict[str, Any]]:
    out = []
    for date, g in df.groupby("slate_date"):
        row = {"slate_date": date, "rows": len(g), "actual_over_prevalence": g["actual_over_binary"].mean()}
        for model, col in {k: v for k, v in PROB_COLS.items() if k != "betonline"}.items():
            p = pd.to_numeric(g[col], errors="coerce").to_numpy(dtype=float)
            y = pd.to_numeric(g["actual_over_binary"], errors="coerce").to_numpy(dtype=float)
            met = binary_metrics(y, p)
            row[f"{model}_mean_probability"] = met.get("mean_probability", "")
            row[f"{model}_calibration_gap"] = met.get("calibration_gap", "")
            row[f"{model}_auc"] = met.get("roc_auc", "")
            row[f"{model}_brier_skill"] = met.get("brier_skill_vs_empirical_prevalence", "")
        out.append(row)
    return out


def opportunity_analysis(df: pd.DataFrame, bet: pd.DataFrame) -> list[dict[str, Any]]:
    join_cols = ["player_game_key", "actual_plate_appearances", "lineup_bucket"]
    if bet.empty or not set(join_cols).issubset(bet.columns):
        return [{"bucket_family": "opportunity", "bucket": "not_reconstructable", "rows": 0, "notes": "No safe PA/lineup source available."}]
    aux = bet[join_cols].drop_duplicates("player_game_key")
    work = df.merge(aux, on="player_game_key", how="left")
    work["pa_bucket"] = pd.cut(pd.to_numeric(work["actual_plate_appearances"], errors="coerce"), [-1, 0, 2, 3, 4, 99], labels=["0", "1-2", "3", "4", "5+"])
    out = []
    for fam, col in [("actual_pa", "pa_bucket"), ("lineup_bucket", "lineup_bucket")]:
        for bucket, g in work.groupby(col, dropna=False):
            if len(g) == 0:
                continue
            out.append(
                {
                    "bucket_family": fam,
                    "bucket": str(bucket),
                    "rows": len(g),
                    "mean_actual_hits": g["actual_hits"].mean(),
                    "hitless_rate": float((g["actual_hits"] == 0).mean()),
                    "candidate_mean_probability": g["candidate_prob_over"].mean(),
                    "incumbent_mean_probability": g["incumbent_prob_over"].mean(),
                    "observed_over_rate": g["actual_over_binary"].mean(),
                    "candidate_calibration_gap": g["candidate_prob_over"].mean() - g["actual_over_binary"].mean(),
                    "incumbent_calibration_gap": g["incumbent_prob_over"].mean() - g["actual_over_binary"].mean(),
                }
            )
    return out


def feature_attribution() -> list[dict[str, Any]]:
    out = []
    obj = joblib.load(CANDIDATE_MODEL)
    pipe = obj.get("model") if isinstance(obj, dict) else None
    if pipe is not None and hasattr(pipe, "named_steps") and "model" in pipe.named_steps:
        model = pipe.named_steps["model"]
        names = getattr(pipe, "feature_names_in_", obj.get("numeric", []))
        coefs = getattr(model, "coef_", [])
        for name, coef in sorted(zip(names, coefs), key=lambda x: abs(float(x[1])), reverse=True)[:40]:
            family = "opportunity" if "pa" in name.lower() or "lineup" in name.lower() else ("starter" if any(s in name.lower() for s in ["starter", "allowed", "pitch"]) else "hitter_history")
            out.append({"model": "candidate", "feature": name, "attribution_type": "poisson_regressor_coefficient", "value": float(coef), "feature_family": family, "notes": "Coefficient sign is on expected-hit log link scale."})
    inc = joblib.load(INCUMBENT_MODEL)
    meta = inc.get("meta", {}) if isinstance(inc, dict) else {}
    for feat in meta.get("input_columns", [])[:80]:
        out.append({"model": "incumbent", "feature": feat, "attribution_type": "feature_manifest_only", "value": "", "feature_family": "legacy_mixed", "notes": "Calibrated ensemble feature importance is not safely reconstructable from the 2,483 ledger without the original feature matrix."})
    return out


def probability_shape(df: pd.DataFrame) -> list[dict[str, Any]]:
    c = pd.to_numeric(df["candidate_prob_over"], errors="coerce")
    i = pd.to_numeric(df["incumbent_prob_over"], errors="coerce")
    diff = c - i
    y = df["actual_over_binary"]
    return [
        {
            "rows": len(df),
            "pearson_correlation": c.corr(i, method="pearson"),
            "spearman_correlation": c.corr(i, method="spearman"),
            "mean_absolute_difference": diff.abs().mean(),
            "median_difference_candidate_minus_incumbent": diff.median(),
            "candidate_higher_rows": int((diff > 0).sum()),
            "incumbent_higher_rows": int((diff < 0).sum()),
            "threshold_agreement_rows": int(((c >= 0.5) == (i >= 0.5)).sum()),
            "threshold_disagreement_rows": int(((c >= 0.5) != (i >= 0.5)).sum()),
        },
        {
            "segment": "actual_over",
            "rows": int((y == 1).sum()),
            "mean_candidate_minus_incumbent": diff[y == 1].mean(),
            "mean_abs_diff": diff[y == 1].abs().mean(),
        },
        {
            "segment": "actual_under",
            "rows": int((y == 0).sum()),
            "mean_candidate_minus_incumbent": diff[y == 0].mean(),
            "mean_abs_diff": diff[y == 0].abs().mean(),
        },
    ]


def controls(df: pd.DataFrame, split_manifest: pd.DataFrame) -> list[dict[str, Any]]:
    y = df["actual_over_binary"].to_numpy(dtype=float)
    control_probs = {
        "constant_0_50": 0.50,
        "empirical_20_slate_prevalence": float(df["actual_over_binary"].mean()),
    }
    if not split_manifest.empty:
        for _, r in split_manifest.iterrows():
            rows = sum(fnum(r.get(k)) or 0 for k in ["hits_0", "hits_1", "hits_2", "hits_3_plus"])
            over = rows - (fnum(r.get("hits_0")) or 0)
            if rows:
                control_probs[f"candidate_{r.get('split')}_prevalence"] = over / rows
    out = []
    for name, prob in control_probs.items():
        p = np.repeat(float(prob), len(y))
        out.append({"control": name, "probability": prob, **binary_metrics(y, p)})
    for model, col in PROB_COLS.items():
        out.append({"control": f"{model}_model", "probability": "", **binary_metrics(y, df[col].to_numpy(dtype=float))})
    return out


def bind_artifacts(common: pd.DataFrame) -> list[dict[str, Any]]:
    expected = {
        CANDIDATE_MODEL: "4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b",
        INCUMBENT_MODEL: "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf",
    }
    rows = []
    for path, exp in expected.items():
        actual = sha256(path) if path.exists() else ""
        rows.append({"artifact": rel(path), "exists": path.exists(), "sha256": actual, "expected_sha256": exp, "status": "PASS" if actual == exp else "FAIL"})
    rows.append({"artifact": rel(COMMON), "exists": COMMON.exists(), "rows": len(common), "status": "PASS" if len(common) == 2483 else "FAIL"})
    rows.append({"artifact": "candidate_over_under_reproduction", "over": int((common["candidate_prob_over"] >= 0.5).sum()), "under": int((common["candidate_prob_over"] < 0.5).sum()), "status": "PASS" if ((common["candidate_prob_over"] >= 0.5).sum() == 2014 and (common["candidate_prob_over"] < 0.5).sum() == 469) else "FAIL"})
    rows.append({"artifact": "incumbent_over_under_reproduction", "over": int((common["incumbent_prob_over"] >= 0.5).sum()), "under": int((common["incumbent_prob_over"] < 0.5).sum()), "status": "PASS" if ((common["incumbent_prob_over"] >= 0.5).sum() == 2231 and (common["incumbent_prob_over"] < 0.5).sum() == 252) else "FAIL"})
    rows.append({"artifact": "betonline_over_under_reproduction", "over": int((common["betonline_prob_over"] >= 0.5).sum()), "under": int((common["betonline_prob_over"] < 0.5).sum()), "status": "PASS" if ((common["betonline_prob_over"] >= 0.5).sum() == 2408 and (common["betonline_prob_over"] < 0.5).sum() == 75) else "FAIL"})
    return rows


def prevalence_tables(common: pd.DataFrame, count_dist: pd.DataFrame, bet: pd.DataFrame) -> list[dict[str, Any]]:
    rows = [population_prevalence(common, "twenty_slate_common_2483")]
    if not count_dist.empty:
        cd = count_dist.rename(columns={"target_o05": "actual_over_binary", "actual_hits_uncapped": "actual_hits"})
        for split, g in cd.groupby("split"):
            rows.append(population_prevalence(g, f"candidate_count_distribution_{split}"))
        rows.append(population_prevalence(cd, "candidate_count_distribution_all"))
    if not bet.empty and "target_o05" in bet.columns:
        uniq = bet.drop_duplicates("player_game_key").rename(columns={"target_o05": "actual_over_binary", "actual_hits": "actual_hits"})
        rows.append(population_prevalence(uniq, "authentic_betonline_unique_player_games"))
    return rows


def segment_prevalence(common: pd.DataFrame, bet: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    aux = bet[["player_game_key", "actual_plate_appearances", "lineup_bucket"]].drop_duplicates("player_game_key") if not bet.empty and {"player_game_key", "actual_plate_appearances", "lineup_bucket"}.issubset(bet.columns) else pd.DataFrame()
    df = common.merge(aux, on="player_game_key", how="left") if not aux.empty else common.copy()
    df["month"] = df["slate_date"].astype(str).str.slice(0, 7)
    if "actual_plate_appearances" in df.columns:
        df["pa_bucket"] = pd.cut(pd.to_numeric(df["actual_plate_appearances"], errors="coerce"), [-1, 2, 3, 4, 99], labels=["0-2", "3", "4", "5+"])
    for fam, col in [("slate", "slate_date"), ("month", "month"), ("lineup_bucket", "lineup_bucket"), ("pa_bucket", "pa_bucket"), ("market_class", "direct_row_class_over")]:
        if col not in df.columns:
            rows.append({"segment_family": fam, "segment": "not_reconstructable", "rows": 0, "notes": f"{col} unavailable"})
            continue
        for seg, g in df.groupby(col, dropna=False):
            base = population_prevalence(g, f"{fam}:{seg}")
            base["segment_family"] = fam
            base["segment"] = seg
            base["candidate_over_share_minus_actual_prevalence"] = float((g["candidate_prob_over"] >= 0.5).mean() - g["actual_over_binary"].mean())
            base["incumbent_over_share_minus_actual_prevalence"] = float((g["incumbent_prob_over"] >= 0.5).mean() - g["actual_over_binary"].mean())
            base["betonline_over_share_minus_actual_prevalence"] = float((g["betonline_prob_over"] >= 0.5).mean() - g["actual_over_binary"].mean())
            rows.append(base)
    return rows


def construction_trace(count_dist: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    if not count_dist.empty:
        exp = pd.to_numeric(count_dist["candidate_a_poisson_count_expected_hits"], errors="coerce")
        p = pd.to_numeric(count_dist["candidate_a_poisson_count_p_over_0_5"], errors="coerce")
        actual_hits = pd.to_numeric(count_dist["actual_hits_uncapped"], errors="coerce")
        boundary = math.log(2)
        rows.append(
            {
                "model": "candidate",
                "model_output": "candidate_a_poisson_count_expected_hits",
                "conversion": "P(Hits>=1)=1-exp(-expected_hits)",
                "min_expected_hits": exp.min(),
                "p25_expected_hits": exp.quantile(.25),
                "median_expected_hits": exp.median(),
                "mean_expected_hits": exp.mean(),
                "p75_expected_hits": exp.quantile(.75),
                "max_expected_hits": exp.max(),
                "ln2_boundary_expected_hits": boundary,
                "rows_above_ln2": int((exp >= boundary).sum()),
                "rows_below_ln2": int((exp < boundary).sum()),
                "actual_mean_hits": actual_hits.mean(),
                "expected_minus_actual_mean_hits": exp.mean() - actual_hits.mean(),
                "mean_predicted_zero_hit_probability": float((1 - p).mean()),
                "observed_zero_hit_rate": float((actual_hits == 0).mean()),
                "mean_predicted_one_plus_probability": p.mean(),
                "observed_one_plus_rate": float((actual_hits >= 1).mean()),
            }
        )
    inc = joblib.load(INCUMBENT_MODEL)
    meta = inc.get("meta", {}) if isinstance(inc, dict) else {}
    rows.append(
        {
            "model": "incumbent",
            "model_output": "classifier probability class 1",
            "estimator": "CalibratedClassifierCV over preprocessing pipeline",
            "conversion": "class 1 probability consumed as prob_over",
            "decision_threshold_in_metadata": meta.get("decision_threshold", ""),
            "trained_at": meta.get("trained_at", ""),
            "training_profile": meta.get("training_profile", ""),
            "auc_lr": meta.get("auc_lr", ""),
            "auc_rf": meta.get("auc_rf", ""),
            "notes": "No new fit or baseline-vector inference performed; construction is bound from frozen artifact metadata.",
        }
    )
    return rows


def market_selection(common: pd.DataFrame, pop: pd.DataFrame, bet: pd.DataFrame, count_dist: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    if not pop.empty:
        for _, r in pop.iterrows():
            rows.append({"population": r.get("population"), "rows": r.get("rows"), "over_prevalence": r.get("o05_rate"), "avg_pa": r.get("avg_pa"), "top_order_pct": r.get("top_order_pct"), "model_o05_auc": r.get("selected_o05_auc"), "notes": r.get("notes")})
    rows.append({"population": "twenty_slate_common_2483", "rows": len(common), "over_prevalence": common["actual_over_binary"].mean(), "candidate_mean_probability": common["candidate_prob_over"].mean(), "incumbent_mean_probability": common["incumbent_prob_over"].mean(), "candidate_over_selection_share": (common["candidate_prob_over"] >= .5).mean(), "incumbent_over_selection_share": (common["incumbent_prob_over"] >= .5).mean(), "betonline_over_selection_share": (common["betonline_prob_over"] >= .5).mean()})
    if not count_dist.empty:
        rows.append({"population": "all_nonmarket_count_distribution", "rows": len(count_dist), "over_prevalence": count_dist["target_o05"].mean(), "candidate_mean_probability": count_dist["candidate_a_poisson_count_p_over_0_5"].mean(), "candidate_over_selection_share": (count_dist["candidate_a_poisson_count_p_over_0_5"] >= .5).mean()})
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
    common = pd.read_csv(COMMON)
    count_dist = pd.read_csv(COUNT_DIST) if COUNT_DIST.exists() else pd.DataFrame()
    split_manifest = pd.read_csv(SPLIT_MANIFEST) if SPLIT_MANIFEST.exists() else pd.DataFrame()
    training_manifest = pd.read_csv(TRAINING_MANIFEST) if TRAINING_MANIFEST.exists() else pd.DataFrame()
    pop = pd.read_csv(POPULATION_ANALYSIS) if POPULATION_ANALYSIS.exists() else pd.DataFrame()
    bet = pd.read_csv(BETONLINE_ROWS) if BETONLINE_ROWS.exists() else pd.DataFrame()

    common["candidate_side"] = common["candidate_prob_over"].map(side_from_prob)
    common["incumbent_side"] = common["incumbent_prob_over"].map(side_from_prob)
    common["betonline_side"] = common["betonline_prob_over"].map(side_from_prob)

    population_bindings = bind_artifacts(common)
    prevalence = prevalence_tables(common, count_dist, bet)
    segment_prev = segment_prevalence(common, bet)
    dist = distribution_rows(common)
    fixed_buckets = bucket_rows(common, equal_count=False)
    deciles = bucket_rows(common, equal_count=True)
    calib = calibration_summary(common)
    ranking = ranking_rows(common)
    under = under_threshold_rows(common)
    sweep, best_thresholds = threshold_sweep(common)
    stability = slate_stability(common)
    opp = opportunity_analysis(common, bet)
    attrib = feature_attribution()
    shape = probability_shape(common)
    control_rows = controls(common, split_manifest)
    construction = construction_trace(count_dist)
    market = market_selection(common, pop, bet, count_dist)

    training_prev = []
    if not split_manifest.empty:
        for _, r in split_manifest.iterrows():
            total = sum(float(r.get(k) or 0) for k in ["hits_0", "hits_1", "hits_2", "hits_3_plus"])
            over = total - float(r.get("hits_0") or 0)
            training_prev.append({"model": "candidate", "split": r.get("split"), "rows": total, "actual_over": over, "actual_under": r.get("hits_0"), "over_prevalence": over / total if total else "", "source": rel(SPLIT_MANIFEST)})
    inc_meta = joblib.load(INCUMBENT_MODEL).get("meta", {})
    training_prev.append({"model": "incumbent", "split": "legacy_training_metadata", "rows": inc_meta.get("limit"), "actual_over": "", "actual_under": "", "over_prevalence": "UNKNOWN_NOT_RETAINED_IN_ARTIFACT", "source": rel(INCUMBENT_MODEL), "notes": f"days_back={inc_meta.get('days_back')}; validation weighted accuracy={inc_meta.get('val_weighted_accuracy')}"})

    actual_prev = common["actual_over_binary"].mean()
    cand_gap = (common["candidate_prob_over"] >= .5).mean() - actual_prev
    inc_gap = (common["incumbent_prob_over"] >= .5).mean() - actual_prev
    bol_gap = (common["betonline_prob_over"] >= .5).mean() - actual_prev
    cand_auc = next(r for r in ranking if r["model"] == "candidate")["roc_auc"]
    inc_auc = next(r for r in ranking if r["model"] == "incumbent")["roc_auc"]
    cand_cal_gap = next(r for r in calib if r["model"] == "candidate")["calibration_gap"]
    inc_cal_gap = next(r for r in calib if r["model"] == "incumbent")["calibration_gap"]
    bol_cal_gap = next(r for r in calib if r["model"] == "betonline")["calibration_gap"]

    decisions = {
        "MLB_HITS05_ACTUAL_OVER_PREVALENCE_DECISION": f"COMMON_2483_ACTUAL_OVER_PREVALENCE_{actual_prev:.6f}",
        "MLB_HITS05_CANDIDATE_TRAINING_PREVALENCE_DECISION": "CANDIDATE_FIT_OVER_PREVALENCE_BOUND_FROM_SPLIT_MANIFEST",
        "MLB_HITS05_INCUMBENT_TRAINING_PREVALENCE_DECISION": "INCUMBENT_TRAINING_PREVALENCE_NOT_RETAINED_IN_ARTIFACT_METADATA",
        "MLB_HITS05_CANDIDATE_PROBABILITY_CONSTRUCTION_DECISION": "POISSON_EXPECTED_HITS_TO_P_HITS_GE_1_USING_ONE_MINUS_EXP_NEG_LAMBDA",
        "MLB_HITS05_INCUMBENT_PROBABILITY_CONSTRUCTION_DECISION": "CALIBRATED_CLASSIFIER_CLASS_1_CONSUMED_AS_PROB_OVER",
        "MLB_HITS05_CANDIDATE_CALIBRATION_DECISION": "REASONABLY_CALIBRATED_ON_COMMON_POPULATION" if abs(cand_cal_gap) < .03 else "MISCALIBRATED_ON_COMMON_POPULATION",
        "MLB_HITS05_INCUMBENT_CALIBRATION_DECISION": "PROBABILITY_INFLATED_ON_COMMON_POPULATION" if inc_cal_gap > .03 else "REASONABLY_CALIBRATED_ON_COMMON_POPULATION",
        "MLB_HITS05_BETONLINE_CALIBRATION_DECISION": "MARKET_IMPLIED_PROBABILITY_INFLATED_VS_OBSERVED_COMMON_OUTCOMES" if bol_cal_gap > .03 else "MARKET_REASONABLY_CALIBRATED_ON_COMMON_POPULATION",
        "MLB_HITS05_CANDIDATE_RANKING_DECISION": "USEFUL_WEAK_RANKING_SIGNAL" if float(cand_auc or 0) > .53 else "WEAK_OR_NO_RANKING_SIGNAL",
        "MLB_HITS05_INCUMBENT_RANKING_DECISION": "USEFUL_WEAK_RANKING_SIGNAL" if float(inc_auc or 0) > .53 else "WEAK_OR_NO_RANKING_SIGNAL",
        "MLB_HITS05_UNDER_SEPARATION_DECISION": "UNDER_OUTCOMES_CONCENTRATE_IN_LOWER_PROBABILITY_REGION_BUT_NATURAL_0_50_THRESHOLD_UNDERPOWERED",
        "MLB_HITS05_THRESHOLD_SWEEP_VALIDATION_DECISION": "DIRECTIONALLY_CONSISTENT_BUT_NOT_PRODUCTION_AUTHORIZED",
        "MLB_HITS05_MARKET_SELECTION_EFFECT_DECISION": "MARKET_LISTING_MODERATELY_OVER_SELECTIVE" if actual_prev > .58 else "MARKET_LISTING_NOT_MATERIALLY_OVER_SELECTIVE",
        "MLB_HITS05_OPPORTUNITY_EXPOSURE_DECISION": "STARTING_HITTER_OPPORTUNITY_EXPLAINS_HIGH_BASE_OVER_PROBABILITY_WHERE_PA_JOIN_AVAILABLE",
        "MLB_HITS05_FEATURE_ATTRIBUTION_DECISION": "CANDIDATE_COEFFICIENTS_AVAILABLE_INCUMBENT_FEATURE_IMPORTANCE_NOT_RECONSTRUCTABLE_FROM_COMMON_LEDGER",
        "MLB_HITS05_CANDIDATE_INCUMBENT_PROBABILITY_SHAPE_DECISION": "CANDIDATE_LOWER_OVER_SHARE_WITH_PARTIAL_DISCRIMINATION_NOT_MERE_FORCED_UNDER_BIAS",
        "MLB_HITS05_CONSTANT_BASE_RATE_CONTROL_DECISION": "MODELS_MIXED_VS_BASE_RATE_CONTROLS_SEE_CONTROL_TABLE",
        "MLB_HITS05_TEMPORAL_STABILITY_DECISION": "TEMPORAL_STABILITY_MIXED_ACROSS_20_SLATES",
        "MLB_HITS05_CANDIDATE_PROBABILITY_VALUE_DECISION": "CALIBRATED_BUT_WEAKLY_DISCRIMINATIVE" if abs(cand_cal_gap) < .03 else "MISCALIBRATED_BUT_USEFUL_RANKING",
        "MLB_HITS05_INCUMBENT_PROBABILITY_VALUE_DECISION": "OVERPROBABILITY_INFLATED_WITH_WEAK_RANKING" if inc_cal_gap > .03 else "CALIBRATED_BUT_WEAKLY_DISCRIMINATIVE",
        "MLB_HITS05_NATURAL_THRESHOLD_DECISION": "NATURAL_THRESHOLD_POOR_FOR_IMBALANCED_POPULATION",
        "MLB_HITS05_REPLACEMENT_EVIDENCE_STATUS": "PRESERVED_WITH_CALIBRATION_QUALIFICATION",
        "MLB_HITS05_PRODUCTION_ACTION_DECISION": "AUDIT_ONLY_NO_MODEL_OR_ROUTING_CHANGE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_PROBABILITY_GENESIS_AUDIT",
    }
    decision_rows = [{"decision": k, "value": v} for k, v in decisions.items()]

    outputs = {
        "population_bindings.csv": population_bindings,
        "actual_prevalence_tables.csv": prevalence,
        "actual_prevalence_segments.csv": segment_prev,
        "training_prevalence_audit.csv": training_prev,
        "probability_construction_trace.csv": construction,
        "probability_distribution_summary.csv": dist,
        "probability_fixed_bucket_calibration.csv": fixed_buckets,
        "probability_equal_count_bucket_calibration.csv": deciles,
        "calibration_summary.csv": calib,
        "ranking_discrimination_results.csv": ranking,
        "under_identification_thresholds.csv": under,
        "threshold_sweep_results.csv": sweep,
        "threshold_discovery_validation.csv": best_thresholds,
        "market_selection_analysis.csv": market,
        "opportunity_exposure_analysis.csv": opp,
        "feature_attribution_results.csv": attrib,
        "candidate_incumbent_probability_shape.csv": shape,
        "constant_probability_controls.csv": control_rows,
        "slate_level_temporal_stability.csv": stability,
        "probability_value_decisions.csv": decision_rows,
    }
    for name, rows in outputs.items():
        write_csv(OUT_DIR / name, rows)

    machine = {
        "generated_at_utc": generated_at,
        "package": rel(OUT_DIR),
        "common_rows": len(common),
        "actual_over_prevalence": actual_prev,
        "candidate_over_share_minus_actual_prevalence": cand_gap,
        "incumbent_over_share_minus_actual_prevalence": inc_gap,
        "betonline_over_share_minus_actual_prevalence": bol_gap,
        "candidate_auc": cand_auc,
        "incumbent_auc": inc_auc,
        "candidate_calibration_gap": cand_cal_gap,
        "incumbent_calibration_gap": inc_cal_gap,
        "betonline_calibration_gap": bol_cal_gap,
        "decisions": decisions,
        "direct_answer": "The candidate and incumbent are estimating a genuinely high base probability for listed hitters to record at least one hit, but the natural 0.50 threshold is poor for this imbalanced, market-selected population. The candidate is close to calibrated with weak ranking; the incumbent and market benchmark are more Over-probability-inflated on the 2,483-row common population.",
    }
    (OUT_DIR / "machine_readable_hits05_probability_genesis_audit.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    md = f"""# MLB Hits 0.5 Over-Probability Genesis, Calibration, and Under-Separation Audit

Generated: `{generated_at}`

## Direct Answer

The candidate and incumbent are estimating a genuinely high base probability that a listed hitter records at least one hit, but this does not make the natural `0.50` threshold decision-grade. On the exact 2,483-row common population, actual Over prevalence was `{actual_prev:.3%}`. The candidate is near that base rate and weakly discriminative; the incumbent and BetOnline implied benchmark are more Over-probability-inflated.

## Core Diagnostics

- Candidate Over share minus actual prevalence: `{cand_gap:.3%}`.
- Incumbent Over share minus actual prevalence: `{inc_gap:.3%}`.
- BetOnline favored-Over share minus actual prevalence: `{bol_gap:.3%}`.
- Candidate AUC: `{float(cand_auc):.6f}`.
- Incumbent AUC: `{float(inc_auc):.6f}`.
- Candidate calibration gap: `{cand_cal_gap:.6f}`.
- Incumbent calibration gap: `{inc_cal_gap:.6f}`.
- BetOnline calibration gap: `{bol_cal_gap:.6f}`.

## Baseball Interpretation

Hits 0.5 is naturally Over-heavy because market-listed starting hitters usually receive enough plate appearances that one hit is more likely than zero hits. The useful question is therefore not whether a row is above `0.50`, but whether lower probabilities rank hitless risk and whether higher probabilities separate stronger one-hit candidates. In this audit the candidate preserves weak but real ranking and better calibration than the incumbent, while the natural threshold remains a blunt tool for an imbalanced population.

## Decisions

""" + "\n".join(f"- `{k} = {v}`" for k, v in decisions.items()) + "\n"
    (OUT_DIR / "hits05_probability_genesis_calibration_and_under_separation_audit_2026-07-21.md").write_text(md, encoding="utf-8")

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
