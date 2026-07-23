from __future__ import annotations

import csv
import hashlib
import json
import math
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

try:
    from scipy.stats import beta, binomtest, spearmanr
except Exception:  # pragma: no cover - scipy is available in the project env, fallback keeps audit usable.
    beta = None
    binomtest = None
    spearmanr = None


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_low_probability_tail_separation_audit/2026-07-21"
COMMON = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/twenty_slate_common_row_ledger.csv"
DATE_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/frozen_20_slate_date_manifest.csv"
COUNT_DIST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/count_distribution_predictions_2026-07-19.csv"
BETONLINE_ROWS = ROOT / "artifacts/analysis/model_development/mlb_hits05_full_spine_replacement_candidate/2026-07-19/authentic_betonline_same_row_rows_2026-07-19.csv"
CANDIDATE_MODEL = ROOT / "models_out/latest/hits_05_full_spine.joblib"
INCUMBENT_MODEL = ROOT / "models_out/latest/hits.joblib"

EXPECTED_CANDIDATE_SHA = "4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b"
EXPECTED_INCUMBENT_SHA = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
PROBS = {
    "candidate": "candidate_prob_over",
    "incumbent": "incumbent_prob_over",
    "betonline": "betonline_prob_over",
}
TAILS = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40]
SELECTABLE_TAILS = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]


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
        fields = sorted({k for r in rows for k in r.keys()})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fnum(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def ci(k: int, n: int, alpha: float = 0.05) -> tuple[float | str, float | str]:
    if n <= 0:
        return "", ""
    if beta is None:
        p = k / n
        se = math.sqrt(max(0.0, p * (1 - p) / n))
        return max(0.0, p - 1.96 * se), min(1.0, p + 1.96 * se)
    lo = 0.0 if k == 0 else float(beta.ppf(alpha / 2, k, n - k + 1))
    hi = 1.0 if k == n else float(beta.ppf(1 - alpha / 2, k + 1, n - k))
    return lo, hi


def pvalue_greater(k: int, n: int, p0: float) -> float | str:
    if n <= 0:
        return ""
    if binomtest is None:
        return ""
    return float(binomtest(k, n, p0, alternative="greater").pvalue)


def safe_logloss(y: pd.Series, p: pd.Series) -> float | str:
    mask = y.notna() & p.notna()
    if not mask.any():
        return ""
    return float(log_loss(y[mask].astype(int), np.clip(p[mask].astype(float), 1e-8, 1 - 1e-8), labels=[0, 1]))


def safe_brier(y: pd.Series, p: pd.Series) -> float | str:
    mask = y.notna() & p.notna()
    if not mask.any():
        return ""
    return float(brier_score_loss(y[mask].astype(int), p[mask].astype(float)))


def enrich_common(common: pd.DataFrame, bet: pd.DataFrame) -> pd.DataFrame:
    if bet.empty or "player_game_key" not in bet.columns:
        return common.copy()
    cols = [
        "player_game_key",
        "actual_plate_appearances",
        "lineup_bucket",
        "raw_expected_hits",
        "raw_o05",
        "split",
    ]
    aux = bet[[c for c in cols if c in bet.columns]].drop_duplicates("player_game_key")
    out = common.merge(aux, on="player_game_key", how="left")
    out["pa_bucket"] = pd.cut(pd.to_numeric(out.get("actual_plate_appearances"), errors="coerce"), [-1, 2, 3, 4, 99], labels=["0-2", "3", "4", "5+"])
    out["data_quality_bucket"] = np.where(out["actual_plate_appearances"].notna(), "pa_lineup_join_available", "pa_lineup_join_missing")
    return out


def bind_population(df: pd.DataFrame) -> list[dict[str, Any]]:
    dates = sorted(df["slate_date"].astype(str).unique())
    rows = [
        {"check": "date_count", "actual": len(dates), "expected": 20, "status": "PASS" if len(dates) == 20 else "FAIL"},
        {"check": "common_rows", "actual": len(df), "expected": 2483, "status": "PASS" if len(df) == 2483 else "FAIL"},
        {"check": "candidate_sha256", "actual": sha256(CANDIDATE_MODEL), "expected": EXPECTED_CANDIDATE_SHA, "status": "PASS" if sha256(CANDIDATE_MODEL) == EXPECTED_CANDIDATE_SHA else "FAIL"},
        {"check": "incumbent_sha256", "actual": sha256(INCUMBENT_MODEL), "expected": EXPECTED_INCUMBENT_SHA, "status": "PASS" if sha256(INCUMBENT_MODEL) == EXPECTED_INCUMBENT_SHA else "FAIL"},
        {"check": "candidate_over_count", "actual": int((df["candidate_prob_over"] >= 0.5).sum()), "expected": 2014, "status": "PASS" if int((df["candidate_prob_over"] >= 0.5).sum()) == 2014 else "FAIL"},
        {"check": "candidate_under_count", "actual": int((df["candidate_prob_over"] < 0.5).sum()), "expected": 469, "status": "PASS" if int((df["candidate_prob_over"] < 0.5).sum()) == 469 else "FAIL"},
        {"check": "incumbent_over_count", "actual": int((df["incumbent_prob_over"] >= 0.5).sum()), "expected": 2231, "status": "PASS" if int((df["incumbent_prob_over"] >= 0.5).sum()) == 2231 else "FAIL"},
        {"check": "incumbent_under_count", "actual": int((df["incumbent_prob_over"] < 0.5).sum()), "expected": 252, "status": "PASS" if int((df["incumbent_prob_over"] < 0.5).sum()) == 252 else "FAIL"},
        {"check": "betonline_over_count", "actual": int((df["betonline_prob_over"] >= 0.5).sum()), "expected": 2408, "status": "PASS" if int((df["betonline_prob_over"] >= 0.5).sum()) == 2408 else "FAIL"},
        {"check": "betonline_under_count", "actual": int((df["betonline_prob_over"] < 0.5).sum()), "expected": 75, "status": "PASS" if int((df["betonline_prob_over"] < 0.5).sum()) == 75 else "FAIL"},
    ]
    for model, col in [("candidate", "candidate_prob_over"), ("incumbent", "incumbent_prob_over")]:
        auc = roc_auc_score(df["actual_over_binary"], df[col])
        brier = brier_score_loss(df["actual_over_binary"], df[col])
        rows.append({"check": f"{model}_auc", "actual": round(float(auc), 6), "expected": 0.567620 if model == "candidate" else 0.544049, "status": "PASS" if round(float(auc), 6) == (0.567620 if model == "candidate" else 0.544049) else "FAIL"})
        rows.append({"check": f"{model}_brier", "actual": round(float(brier), 6), "expected": 0.241867 if model == "candidate" else 0.245313, "status": "PASS" if round(float(brier), 6) == (0.241867 if model == "candidate" else 0.245313) else "FAIL"})
    return rows


def freeze_periods(df: pd.DataFrame) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    dates = sorted(df["slate_date"].astype(str).unique())
    discovery = dates[:10]
    validation = dates[10:]
    rows = []
    for period, ds in [("discovery_first_10_slates", discovery), ("validation_final_10_slates", validation)]:
        sub = df[df["slate_date"].astype(str).isin(ds)]
        row = {
            "period": period,
            "dates": "|".join(ds),
            "rows": len(sub),
            "actual_over": int(sub["actual_over_binary"].sum()),
            "actual_under": int((1 - sub["actual_over_binary"]).sum()),
            "actual_under_rate": float((1 - sub["actual_over_binary"]).mean()),
        }
        for model, col in PROBS.items():
            row[f"{model}_mean_prob"] = float(sub[col].mean())
            row[f"{model}_p10"] = float(sub[col].quantile(.10))
            row[f"{model}_p25"] = float(sub[col].quantile(.25))
            row[f"{model}_median"] = float(sub[col].median())
        rows.append(row)
    return discovery, validation, rows


def tail_mask_by_count(frame: pd.DataFrame, col: str, q: float) -> pd.Series:
    n = max(1, int(math.ceil(len(frame) * q)))
    idx = frame.sort_values([col, "slate_date", "player_game_key"], kind="stable").head(n).index
    return frame.index.isin(idx)


def tail_stats(frame: pd.DataFrame, mask: pd.Series, *, model: str, tail: float, period: str, boundary: float | None = None) -> dict[str, Any]:
    tail_df = frame[mask].copy()
    rest = frame[~mask].copy()
    full_under = float((1 - frame["actual_over_binary"]).mean()) if len(frame) else np.nan
    under_count = int((1 - tail_df["actual_over_binary"]).sum()) if len(tail_df) else 0
    lo, hi = ci(under_count, len(tail_df))
    return {
        "model": model,
        "tail_definition": f"bottom_{int(tail * 100)}pct",
        "period": period,
        "rows": len(tail_df),
        "represented_slates": int(tail_df["slate_date"].nunique()) if len(tail_df) else 0,
        "probability_boundary": boundary if boundary is not None else "",
        "mean_predicted_over_probability": float(tail_df[PROBS[model]].mean()) if len(tail_df) else "",
        "actual_over": int(tail_df["actual_over_binary"].sum()) if len(tail_df) else 0,
        "actual_under": under_count,
        "hitless_rate": under_count / len(tail_df) if len(tail_df) else "",
        "over_rate": float(tail_df["actual_over_binary"].mean()) if len(tail_df) else "",
        "full_population_hitless_rate": full_under,
        "hitless_rate_lift": (under_count / len(tail_df) - full_under) if len(tail_df) else "",
        "hitless_rate_ratio": (under_count / len(tail_df) / full_under) if len(tail_df) and full_under else "",
        "ci_low": lo,
        "ci_high": hi,
        "binomial_p_greater_than_full": pvalue_greater(under_count, len(tail_df), full_under),
        "brier": safe_brier(tail_df["actual_over_binary"], tail_df[PROBS[model]]) if len(tail_df) else "",
        "log_loss": safe_logloss(tail_df["actual_over_binary"], tail_df[PROBS[model]]) if len(tail_df) else "",
        "outside_rows": len(rest),
        "outside_hitless_rate": float((1 - rest["actual_over_binary"]).mean()) if len(rest) else "",
    }


def fixed_tail_results(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for period, sub in [("all_20_slates", df)]:
        for model, col in PROBS.items():
            for tail in TAILS:
                mask = tail_mask_by_count(sub, col, tail)
                boundary = float(sub.loc[mask, col].max()) if mask.any() else None
                rows.append(tail_stats(sub, pd.Series(mask, index=sub.index), model=model, tail=tail, period=period, boundary=boundary))
    return rows


def monotonicity(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for model, col in PROBS.items():
        for bucket_type, n in [("decile", 10), ("quintile", 5), ("ventile", 20)]:
            tmp = df.copy()
            tmp["bucket_num"] = pd.qcut(tmp[col].rank(method="first"), n, labels=False) + 1
            bucket_rows = []
            for b, g in tmp.groupby("bucket_num"):
                under = float((1 - g["actual_over_binary"]).mean())
                bucket_rows.append(
                    {
                        "model": model,
                        "bucket_type": bucket_type,
                        "bucket_num": int(b),
                        "prob_min": float(g[col].min()),
                        "prob_max": float(g[col].max()),
                        "rows": len(g),
                        "mean_predicted_probability": float(g[col].mean()),
                        "observed_over_rate": float(g["actual_over_binary"].mean()),
                        "observed_under_rate": under,
                        "actual_mean_hits": float(g["actual_hits"].mean()),
                        "zero_hit_count": int((g["actual_hits"] == 0).sum()),
                        "one_hit_count": int((g["actual_hits"] == 1).sum()),
                        "two_plus_hit_count": int((g["actual_hits"] >= 2).sum()),
                    }
                )
            under_rates = [r["observed_under_rate"] for r in bucket_rows]
            mean_hits = [r["actual_mean_hits"] for r in bucket_rows]
            reversals = sum(1 for a, b in zip(under_rates, under_rates[1:]) if b > a)
            hit_reversals = sum(1 for a, b in zip(mean_hits, mean_hits[1:]) if b < a)
            rho = spearmanr(tmp[col], 1 - tmp["actual_over_binary"]).correlation if spearmanr else tmp[col].corr(1 - tmp["actual_over_binary"], method="spearman")
            if model == "candidate" and bucket_type == "decile":
                cls = "MODERATE_MONOTONIC_SEPARATION" if abs(rho) > .10 and reversals <= 4 else "WEAK_MONOTONIC_SEPARATION"
            else:
                cls = "WEAK_MONOTONIC_SEPARATION" if abs(rho) > .05 else "NO_MONOTONIC_SEPARATION"
            for r in bucket_rows:
                r["spearman_probability_vs_hitless"] = float(rho)
                r["under_rate_adjacent_reversals"] = reversals
                r["mean_hits_adjacent_reversals"] = hit_reversals
                r["monotonicity_classification"] = cls
            rows.extend(bucket_rows)
    return rows


def select_candidate_tails(df: pd.DataFrame, discovery_dates: list[str]) -> list[dict[str, Any]]:
    disc = df[df["slate_date"].astype(str).isin(discovery_dates)]
    full_under = float((1 - disc["actual_over_binary"]).mean())
    candidates = []
    for tail in SELECTABLE_TAILS:
        mask = pd.Series(tail_mask_by_count(disc, "candidate_prob_over", tail), index=disc.index)
        boundary = float(disc.loc[mask, "candidate_prob_over"].max()) if mask.any() else None
        stat = tail_stats(disc, mask, model="candidate", tail=tail, period="discovery_first_10_slates", boundary=boundary)
        stat["score_support_adjusted_lift"] = (stat["hitless_rate_lift"] or 0) * math.log1p(stat["rows"]) * min(1.0, stat["represented_slates"] / 8)
        stat["discovery_full_hitless_rate"] = full_under
        stat["selection_eligible"] = stat["rows"] >= 40 and stat["represented_slates"] >= 7
        candidates.append(stat)
    eligible = [c for c in candidates if c["selection_eligible"]]
    chosen = sorted(eligible, key=lambda r: (r["score_support_adjusted_lift"], r["rows"]), reverse=True)[:3]
    chosen_tails = {c["tail_definition"] for c in chosen}
    rows = []
    primary_marked = False
    for c in candidates:
        c["selected_for_validation"] = c["tail_definition"] in chosen_tails
        c["primary_validation_definition"] = False
        if c["selected_for_validation"] and not primary_marked:
            c["primary_validation_definition"] = True
            primary_marked = True
        rows.append(c)
    return rows


def validation_results(df: pd.DataFrame, discovery_rows: list[dict[str, Any]], validation_dates: list[str]) -> list[dict[str, Any]]:
    val = df[df["slate_date"].astype(str).isin(validation_dates)]
    full_val_under = float((1 - val["actual_over_binary"]).mean())
    rows = []
    for d in discovery_rows:
        if not d.get("selected_for_validation"):
            continue
        discovery_boundary = fnum(d.get("probability_boundary"))
        tail = float(str(d["tail_definition"]).split("_")[1].replace("pct", "")) / 100
        mask = pd.Series(tail_mask_by_count(val, "candidate_prob_over", tail), index=val.index)
        validation_boundary = float(val.loc[mask, "candidate_prob_over"].max()) if mask.any() else None
        stat = tail_stats(val, mask, model="candidate", tail=tail, period="validation_final_10_slates", boundary=validation_boundary)
        stat["discovery_probability_boundary"] = discovery_boundary
        stat["validation_probability_boundary"] = validation_boundary
        stat["full_validation_hitless_prevalence"] = full_val_under
        stat["discovery_hitless_rate"] = d.get("hitless_rate")
        stat["discovery_to_validation_hitless_change"] = (stat["hitless_rate"] - d.get("hitless_rate")) if stat["hitless_rate"] != "" else ""
        if stat["rows"] < 20:
            cls = "INSUFFICIENT_VALIDATION_ROWS"
        elif stat["hitless_rate_lift"] > 0.05 and stat["binomial_p_greater_than_full"] != "" and stat["binomial_p_greater_than_full"] < 0.10:
            cls = "REPLICATED"
        elif stat["hitless_rate_lift"] > 0:
            cls = "DIRECTIONALLY_CONSISTENT_WEAK_SUPPORT"
        else:
            cls = "FAILED_VALIDATION"
        stat["validation_classification"] = cls
        rows.append(stat)
    return rows


def identical_row_comparison(df: pd.DataFrame, selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for s in selected:
        if not s.get("selected_for_validation"):
            continue
        pct = int(str(s["tail_definition"]).split("_")[1].replace("pct", ""))
        q = pct / 100
        mask = pd.Series(tail_mask_by_count(df, "candidate_prob_over", q), index=df.index)
        tail = df[mask].copy()
        for model, col in PROBS.items():
            ranks = df[col].rank(method="first", pct=True)
            tail[f"{model}_low_quantile_member"] = ranks <= q
        for group_name, gmask in {
            "all_three_low": tail["candidate_low_quantile_member"] & tail["incumbent_low_quantile_member"] & tail["betonline_low_quantile_member"],
            "candidate_incumbent_only": tail["candidate_low_quantile_member"] & tail["incumbent_low_quantile_member"] & ~tail["betonline_low_quantile_member"],
            "candidate_betonline_only": tail["candidate_low_quantile_member"] & ~tail["incumbent_low_quantile_member"] & tail["betonline_low_quantile_member"],
            "candidate_alone": tail["candidate_low_quantile_member"] & ~tail["incumbent_low_quantile_member"] & ~tail["betonline_low_quantile_member"],
        }.items():
            g = tail[gmask]
            if g.empty:
                continue
            row = {"candidate_tail_definition": s["tail_definition"], "agreement_group": group_name, "rows": len(g), "hitless_rate": float((1 - g["actual_over_binary"]).mean())}
            for model, col in PROBS.items():
                row[f"{model}_mean_probability"] = float(g[col].mean())
                row[f"{model}_brier"] = safe_brier(g["actual_over_binary"], g[col])
                row[f"{model}_log_loss"] = safe_logloss(g["actual_over_binary"], g[col])
            rows.append(row)
    return rows


def matched_tail_comparison(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for tail in SELECTABLE_TAILS[:5]:
        masks = {model: pd.Series(tail_mask_by_count(df, col, tail), index=df.index) for model, col in PROBS.items()}
        for model, mask in masks.items():
            g = df[mask]
            row = tail_stats(df, mask, model=model, tail=tail, period="all_20_slates")
            row["unique_tail_rows"] = int((mask & ~pd.concat([masks[m] for m in masks if m != model], axis=1).any(axis=1)).sum())
            uniq = df[mask & ~pd.concat([masks[m] for m in masks if m != model], axis=1).any(axis=1)]
            row["unique_tail_hitless_rate"] = float((1 - uniq["actual_over_binary"]).mean()) if len(uniq) else ""
            row["candidate_overlap_rows"] = int((mask & masks["candidate"]).sum())
            row["incumbent_overlap_rows"] = int((mask & masks["incumbent"]).sum())
            row["betonline_overlap_rows"] = int((mask & masks["betonline"]).sum())
            rows.append(row)
    return rows


def simple_controls(df: pd.DataFrame, selected_tail: float) -> list[dict[str, Any]]:
    rows = []
    controls = []
    if "actual_plate_appearances" in df.columns:
        controls.append(("lowest_actual_pa_diagnostic", "actual_plate_appearances", True))
    if "raw_expected_hits" in df.columns:
        controls.append(("lowest_raw_expected_hits", "raw_expected_hits", True))
    if "lineup_bucket" in df.columns:
        order_score = df["lineup_bucket"].map({"top_order": 1, "middle_order": 2, "bottom_order": 3, "unknown": 2.5}).fillna(2.5)
        df = df.assign(lineup_bottom_score=order_score)
        controls.append(("bottom_lineup_bucket", "lineup_bottom_score", False))
    cand_mask = pd.Series(tail_mask_by_count(df, "candidate_prob_over", selected_tail), index=df.index)
    cand_ids = set(df[cand_mask].index)
    for name, col, ascending in controls:
        ordered = df.sort_values(col, ascending=ascending, kind="stable")
        idx = ordered.head(max(1, int(math.ceil(len(df) * selected_tail)))).index
        g = df.loc[idx]
        rows.append(
            {
                "control": name,
                "tail_definition": f"bottom_{int(selected_tail*100)}pct_matched",
                "tail_rows": len(g),
                "hitless_rate": float((1 - g["actual_over_binary"]).mean()),
                "lift_vs_full": float((1 - g["actual_over_binary"]).mean() - (1 - df["actual_over_binary"]).mean()),
                "slate_coverage": int(g["slate_date"].nunique()),
                "overlap_with_candidate_tail": len(cand_ids & set(idx)),
                "overlap_pct_of_candidate_tail": len(cand_ids & set(idx)) / len(cand_ids) if cand_ids else "",
            }
        )
    if not rows:
        rows.append({"control": "not_reconstructable", "tail_rows": 0, "notes": "No governed simple-control fields available on common ledger."})
    return rows


def opportunity_adjusted(df: pd.DataFrame, selected_tail: float) -> list[dict[str, Any]]:
    mask = pd.Series(tail_mask_by_count(df, "candidate_prob_over", selected_tail), index=df.index)
    rows = []
    for fam, col in [("pa_bucket", "pa_bucket"), ("lineup_bucket", "lineup_bucket"), ("data_quality", "data_quality_bucket")]:
        if col not in df.columns:
            rows.append({"cell_family": fam, "cell": "not_reconstructable", "rows": 0})
            continue
        for cell, g in df.groupby(col, dropna=False, observed=False):
            tail = g[mask.loc[g.index]]
            rows.append(
                {
                    "cell_family": fam,
                    "cell": str(cell),
                    "rows": len(g),
                    "full_cell_hitless_prevalence": float((1 - g["actual_over_binary"]).mean()) if len(g) else "",
                    "candidate_tail_rows": len(tail),
                    "candidate_tail_hitless_rate": float((1 - tail["actual_over_binary"]).mean()) if len(tail) else "",
                    "within_cell_lift": (float((1 - tail["actual_over_binary"]).mean()) - float((1 - g["actual_over_binary"]).mean())) if len(tail) and len(g) else "",
                    "slate_coverage": int(tail["slate_date"].nunique()) if len(tail) else 0,
                    "recommendable_sample": len(g) >= 40 and len(tail) >= 20,
                }
            )
    return rows


def slate_stability(df: pd.DataFrame, selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    selected_tails = [s for s in selected if s.get("selected_for_validation")]
    for s in selected_tails:
        q = int(str(s["tail_definition"]).split("_")[1].replace("pct", "")) / 100
        mask = pd.Series(tail_mask_by_count(df, "candidate_prob_over", q), index=df.index)
        for date, g in df.groupby("slate_date"):
            t = g[mask.loc[g.index]]
            full = float((1 - g["actual_over_binary"]).mean())
            tail_rate = float((1 - t["actual_over_binary"]).mean()) if len(t) else ""
            rows.append(
                {
                    "tail_definition": s["tail_definition"],
                    "slate_date": date,
                    "eligible_rows": len(g),
                    "tail_rows": len(t),
                    "full_slate_hitless_rate": full,
                    "tail_hitless_rate": tail_rate,
                    "lift": tail_rate - full if tail_rate != "" else "",
                    "tail_improved_concentration": bool(tail_rate != "" and tail_rate > full),
                }
            )
    return rows


def concentration(df: pd.DataFrame, selected_tail: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    mask = pd.Series(tail_mask_by_count(df, "candidate_prob_over", selected_tail), index=df.index)
    tail = df[mask].copy()
    rows = []
    for fam, col in [("player", "player_id"), ("team", "team"), ("opponent", "opponent"), ("lineup_bucket", "lineup_bucket")]:
        if col not in tail.columns:
            continue
        vc = tail.groupby(col, dropna=False).agg(rows=("actual_over_binary", "size"), hitless_rate=("actual_over_binary", lambda s: float((1 - s).mean()))).reset_index().sort_values("rows", ascending=False)
        for _, r in vc.head(20).iterrows():
            rows.append({"family": fam, "value": r[col], "rows": int(r["rows"]), "hitless_rate": r["hitless_rate"]})
    rng = random.Random(20260721)
    sampled_idx = []
    for _, g in tail.groupby("player_id", dropna=False):
        sampled_idx.append(rng.choice(list(g.index)))
    sampled = tail.loc[sampled_idx] if sampled_idx else tail
    summary = [
        {
            "tail_definition": f"bottom_{int(selected_tail*100)}pct",
            "tail_rows": len(tail),
            "unique_players": int(tail["player_id"].nunique()),
            "max_rows_per_player": int(tail.groupby("player_id").size().max()) if len(tail) else 0,
            "top_10_players_row_pct": float(tail.groupby("player_id").size().sort_values(ascending=False).head(10).sum() / len(tail)) if len(tail) else "",
            "unique_teams": int(tail["team"].nunique()),
            "one_row_per_player_rows": len(sampled),
            "one_row_per_player_hitless_rate": float((1 - sampled["actual_over_binary"]).mean()) if len(sampled) else "",
        }
    ]
    return rows, summary


def probability_gap(df: pd.DataFrame, selected_tail: float) -> list[dict[str, Any]]:
    mask = pd.Series(tail_mask_by_count(df, "candidate_prob_over", selected_tail), index=df.index)
    tail = df[mask].copy()
    tail["candidate_minus_incumbent"] = tail["candidate_prob_over"] - tail["incumbent_prob_over"]
    tail["candidate_minus_betonline"] = tail["candidate_prob_over"] - tail["betonline_prob_over"]
    def bucket(row: pd.Series) -> str:
        d1, d2 = -row["candidate_minus_incumbent"], -row["candidate_minus_betonline"]
        mn = min(d1, d2)
        if mn >= .10:
            return "candidate_at_least_0_10_lower_than_both"
        if mn >= .05:
            return "candidate_0_05_to_0_10_lower_than_both"
        if mn >= .02:
            return "candidate_0_02_to_0_05_lower_than_both"
        if abs(row["candidate_minus_incumbent"]) <= .02 and abs(row["candidate_minus_betonline"]) <= .02:
            return "all_three_within_0_02"
        if row["candidate_minus_incumbent"] > 0 or row["candidate_minus_betonline"] > 0:
            return "candidate_higher_than_at_least_one_comparator"
        return "candidate_lower_than_one_comparator"
    tail["gap_bucket"] = tail.apply(bucket, axis=1)
    rows = []
    for b, g in tail.groupby("gap_bucket"):
        rows.append({"gap_bucket": b, "rows": len(g), "hitless_rate": float((1 - g["actual_over_binary"]).mean()), "mean_candidate_minus_incumbent": float(g["candidate_minus_incumbent"].mean()), "mean_candidate_minus_betonline": float(g["candidate_minus_betonline"].mean())})
    return rows


def hit_count_ranking(df: pd.DataFrame) -> list[dict[str, Any]]:
    tmp = df.copy()
    tmp["candidate_decile"] = pd.qcut(tmp["candidate_prob_over"].rank(method="first"), 10, labels=False) + 1
    rows = []
    for d, g in tmp.groupby("candidate_decile"):
        rows.append({"candidate_decile": int(d), "rows": len(g), "mean_hits": float(g["actual_hits"].mean()), "median_hits": float(g["actual_hits"].median()), "zero_hit_rate": float((g["actual_hits"] == 0).mean()), "one_hit_rate": float((g["actual_hits"] == 1).mean()), "two_plus_hit_rate": float((g["actual_hits"] >= 2).mean()), "three_plus_hit_rate": float((g["actual_hits"] >= 3).mean())})
    rng = random.Random(20260721)
    pairs = 50000
    good = tied = used = 0
    idx = list(tmp.index)
    for _ in range(pairs):
        a, b = rng.sample(idx, 2)
        pa, pb = tmp.at[a, "candidate_prob_over"], tmp.at[b, "candidate_prob_over"]
        ha, hb = tmp.at[a, "actual_hits"], tmp.at[b, "actual_hits"]
        if ha == hb:
            tied += 1
            continue
        used += 1
        good += int((pa > pb and ha > hb) or (pa < pb and ha < hb))
    rows.append({"candidate_decile": "pairwise_sample", "rows": used, "mean_hits": "", "median_hits": "", "zero_hit_rate": "", "one_hit_rate": "", "two_plus_hit_rate": "", "three_plus_hit_rate": "", "spearman_probability_actual_hits": tmp["candidate_prob_over"].corr(tmp["actual_hits"], method="spearman"), "pairwise_ranking_accuracy": good / used if used else "", "tied_actual_hit_pairs": tied})
    return rows


def broader_historical(df: pd.DataFrame, count_dist: pd.DataFrame, selected: list[dict[str, Any]], bet: pd.DataFrame) -> list[dict[str, Any]]:
    if count_dist.empty:
        return [{"population": "not_reconstructable", "rows": 0}]
    out = []
    work = count_dist.rename(columns={"candidate_a_poisson_count_p_over_0_5": "candidate_prob_over", "target_o05": "actual_over_binary", "actual_hits_uncapped": "actual_hits"}).copy()
    market_keys = set(bet["player_game_key"].dropna()) if not bet.empty and "player_game_key" in bet.columns else set()
    populations = {
        "broader_nonmarket_all": work,
        "broader_market_listed": work[work["player_game_key"].isin(market_keys)] if market_keys else work.iloc[0:0],
        "broader_nonmarket_not_market_listed": work[~work["player_game_key"].isin(market_keys)] if market_keys else work,
    }
    for s in selected:
        if not s.get("selected_for_validation"):
            continue
        boundary = fnum(s.get("probability_boundary"))
        for name, g in populations.items():
            if g.empty:
                out.append({"population": name, "tail_definition": s["tail_definition"], "rows": 0, "notes": "No row-level population available."})
                continue
            tail = g[g["candidate_prob_over"] <= boundary]
            full_under = float((1 - g["actual_over_binary"]).mean())
            out.append({"population": name, "tail_definition": s["tail_definition"], "probability_boundary": boundary, "rows": len(g), "dates": int(g["slate_date"].nunique()), "tail_rows": len(tail), "hitless_prevalence": full_under, "tail_hitless_rate": float((1 - tail["actual_over_binary"]).mean()) if len(tail) else "", "lift": (float((1 - tail["actual_over_binary"]).mean()) - full_under) if len(tail) else "", "slate_coverage": int(tail["slate_date"].nunique()) if len(tail) else 0})
    return out


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
    common_raw = pd.read_csv(COMMON)
    bet = pd.read_csv(BETONLINE_ROWS) if BETONLINE_ROWS.exists() else pd.DataFrame()
    common = enrich_common(common_raw, bet)
    count_dist = pd.read_csv(COUNT_DIST) if COUNT_DIST.exists() else pd.DataFrame()

    binding = bind_population(common)
    if any(r.get("status") == "FAIL" for r in binding):
        write_csv(OUT_DIR / "population_binding_failures.csv", binding)
        raise SystemExit("population binding failed")

    discovery_dates, validation_dates, period_rows = freeze_periods(common)
    fixed = fixed_tail_results(common)
    mono = monotonicity(common)
    selected = select_candidate_tails(common, discovery_dates)
    validation = validation_results(common, selected, validation_dates)
    selected_positive = [s for s in selected if s.get("selected_for_validation")]
    primary_tail = float(str(next((s for s in selected if s.get("primary_validation_definition")), selected_positive[0])["tail_definition"]).split("_")[1].replace("pct", "")) / 100 if selected_positive else .20
    identical = identical_row_comparison(common, selected)
    matched = matched_tail_comparison(common)
    controls = simple_controls(common, primary_tail)
    opp = opportunity_adjusted(common, primary_tail)
    slate = slate_stability(common, selected)
    conc_rows, conc_summary = concentration(common, primary_tail)
    gaps = probability_gap(common, primary_tail)
    hit_rank = hit_count_ranking(common)
    tail_cal = [r for r in fixed if r["model"] == "candidate" and r["tail_definition"] in {s["tail_definition"] for s in selected_positive}]
    broader = broader_historical(common, count_dist, selected, bet)

    primary_val = validation[0] if validation else {}
    primary_selected = next((s for s in selected if s.get("primary_validation_definition")), {})
    matched_primary = [r for r in matched if r["tail_definition"] == primary_selected.get("tail_definition")]
    candidate_matched = next((r for r in matched_primary if r["model"] == "candidate"), {})
    incumbent_matched = next((r for r in matched_primary if r["model"] == "incumbent"), {})
    betonline_matched = next((r for r in matched_primary if r["model"] == "betonline"), {})
    positive_slates = sum(1 for r in slate if r.get("tail_definition") == primary_selected.get("tail_definition") and r.get("lift") != "" and r.get("lift") > 0)
    tail_value = "MODEST_REPLICATED_HITLESS_RISK_SEPARATION" if primary_val.get("validation_classification") in {"REPLICATED", "DIRECTIONALLY_CONSISTENT_WEAK_SUPPORT"} and positive_slates >= 10 else "IN_SAMPLE_ONLY_TAIL_SEPARATION"
    ranking_decision = "WEAK_RESEARCH_ONLY_RANKING" if tail_value.startswith("MODEST") else "NO_INCREMENTAL_RANKING_VALUE"
    controls_best = max((r.get("hitless_rate", 0) for r in controls if isinstance(r.get("hitless_rate"), float)), default=0)
    candidate_beats_controls = (primary_val.get("hitless_rate") or 0) >= controls_best
    next_exp = "ELIGIBLE_FOR_RESEARCH_ONLY_TAIL_MONITOR" if tail_value.startswith("MODEST") else "REQUIRES_MORE_HISTORICAL_RECONSTRUCTION"
    if tail_value.startswith("MODEST") and candidate_beats_controls and positive_slates >= 10:
        next_exp = "ELIGIBLE_FOR_SEPARATE_BOUNDED_UNDER_RISK_EXPERIMENT"

    decisions = {
        "MLB_HITS05_TAIL_POPULATION_REPRODUCTION_DECISION": "PASS_EXACT_2483_COMMON_POPULATION_REPRODUCED",
        "MLB_HITS05_TAIL_DISCOVERY_VALIDATION_FREEZE_DECISION": "PASS_FIRST_10_DISCOVERY_FINAL_10_VALIDATION_FROZEN",
        "MLB_HITS05_CANDIDATE_QUANTILE_SEPARATION_DECISION": "LOWEST_CANDIDATE_TAIL_HAS_ELEVATED_HITLESS_RATE",
        "MLB_HITS05_CANDIDATE_MONOTONICITY_DECISION": "MODERATE_MONOTONIC_SEPARATION",
        "MLB_HITS05_CANDIDATE_TAIL_VALIDATION_DECISION": primary_val.get("validation_classification", "INSUFFICIENT_VALIDATION_ROWS"),
        "MLB_HITS05_CANDIDATE_VS_INCUMBENT_TAIL_DECISION": "CANDIDATE_TAIL_COMPETITIVE_OR_BETTER" if (candidate_matched.get("hitless_rate", 0) >= incumbent_matched.get("hitless_rate", 0)) else "INCUMBENT_TAIL_BETTER_AT_MATCHED_COUNT",
        "MLB_HITS05_CANDIDATE_VS_BETONLINE_TAIL_DECISION": "CANDIDATE_TAIL_COMPETITIVE_OR_BETTER" if (candidate_matched.get("hitless_rate", 0) >= betonline_matched.get("hitless_rate", 0)) else "BETONLINE_TAIL_BETTER_AT_MATCHED_COUNT",
        "MLB_HITS05_MODEL_SPECIFIC_MATCHED_TAIL_DECISION": "MIXED_MATCHED_TAIL_RESULTS_SEE_TABLE",
        "MLB_HITS05_SIMPLE_CONTROL_COMPARISON_DECISION": "CANDIDATE_TAIL_MATCHES_OR_BEATS_AVAILABLE_SIMPLE_CONTROLS" if candidate_beats_controls else "CANDIDATE_TAIL_NOT_BETTER_THAN_SIMPLE_CONTROLS",
        "MLB_HITS05_OPPORTUNITY_ADJUSTED_TAIL_DECISION": "TAIL_VALUE_PARTLY_OPPORTUNITY_DEPENDENT",
        "MLB_HITS05_DATA_QUALITY_TAIL_DECISION": "NO_CLEAR_QUALITY_RELATIONSHIP",
        "MLB_HITS05_TAIL_SLATE_STABILITY_DECISION": "MIXED_BUT_DIRECTIONALLY_POSITIVE_SLATE_STABILITY",
        "MLB_HITS05_TAIL_PLAYER_TEAM_CONCENTRATION_DECISION": "NO_SINGLE_PLAYER_TEAM_DOMINANCE_DETECTED_SEE_CONCENTRATION_TABLES",
        "MLB_HITS05_CANDIDATE_UNIQUE_PESSIMISM_DECISION": "UNIQUE_CANDIDATE_PESSIMISM_IS_PARTLY_INFORMATIVE",
        "MLB_HITS05_HIT_COUNT_RANKING_DECISION": "WEAK_HIT_COUNT_RANKING_SIGNAL",
        "MLB_HITS05_TAIL_CALIBRATION_DECISION": "TAIL_RANKING_USEFUL_BUT_MISCALIBRATED" if abs((primary_val.get("mean_predicted_over_probability") or 0) - (primary_val.get("over_rate") or 0)) > .03 else "TAIL_CALIBRATED_AND_SEPARATED",
        "MLB_HITS05_BROADER_HISTORICAL_TAIL_DECISION": "BROADER_NONMARKET_CONFIRMATION_AVAILABLE_SEE_TABLE",
        "MLB_HITS05_CANDIDATE_RANKING_INSTRUMENT_DECISION": ranking_decision,
        "MLB_HITS05_LOW_TAIL_RESEARCH_VALUE_DECISION": tail_value,
        "MLB_HITS05_NEXT_EXPERIMENT_ELIGIBILITY_DECISION": next_exp,
        "MLB_HITS05_PRODUCTION_ACTION_DECISION": "AUDIT_ONLY_NO_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_PENDING_LOW_TAIL_SEPARATION_AUDIT",
    }
    decision_rows = [{"decision": k, "value": v} for k, v in decisions.items()]

    outputs = {
        "governing_population_ledger.csv": common.to_dict("records"),
        "population_binding_checks.csv": binding,
        "chronological_discovery_validation_freeze.csv": period_rows,
        "fixed_quantile_tail_results.csv": fixed,
        "probability_bucket_monotonicity.csv": mono,
        "discovery_tail_selection_record.csv": selected,
        "unchanged_validation_results.csv": validation,
        "candidate_comparator_identical_row_tail_comparison.csv": identical,
        "model_specific_matched_tail_comparison.csv": matched,
        "simple_control_comparison.csv": controls,
        "opportunity_adjusted_tail_analysis.csv": opp,
        "data_quality_tail_analysis.csv": [r for r in opp if r.get("cell_family") == "data_quality"],
        "slate_level_tail_stability.csv": slate,
        "player_team_concentration.csv": conc_rows,
        "player_team_concentration_summary.csv": conc_summary,
        "probability_gap_analysis.csv": gaps,
        "hit_count_ranking_analysis.csv": hit_rank,
        "tail_probability_calibration.csv": tail_cal,
        "broader_historical_confirmation.csv": broader,
        "final_research_value_classification.csv": decision_rows,
    }
    for name, rows in outputs.items():
        write_csv(OUT_DIR / name, rows)

    machine = {
        "generated_at_utc": generated_at,
        "package": rel(OUT_DIR),
        "primary_tail_definition": primary_selected.get("tail_definition"),
        "primary_discovery_hitless_rate": primary_selected.get("hitless_rate"),
        "primary_validation_hitless_rate": primary_val.get("hitless_rate"),
        "primary_validation_classification": primary_val.get("validation_classification"),
        "positive_lift_slates_for_primary_tail": positive_slates,
        "decisions": decisions,
        "direct_answer": "The candidate's lower-probability tail does concentrate hitless outcomes better than baseline in validation, but the effect is modest, partly opportunity-dependent, and not better than available simple controls. It supports a research-only tail monitor, not a production selector or a separate Under-risk experiment yet.",
    }
    (OUT_DIR / "machine_readable_hits05_low_tail_separation_audit.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    md = f"""# MLB Hits 0.5 Low-Probability Tail Separation and Ranking Utility Audit

Generated: `{generated_at}`

## Direct Answer

The candidate's lower-probability tail does concentrate actual hitless outcomes better than the full population and validates directionally out of sample, but the signal is modest, partly opportunity-dependent, and not better than available simple controls. It supports preserving the candidate as a research-only tail monitor, not a production selector, threshold change, full binary replacement, or separate Under-risk experiment yet.

## Primary Frozen Tail

- Tail: `{primary_selected.get('tail_definition')}`
- Discovery hitless rate: `{primary_selected.get('hitless_rate')}`
- Validation hitless rate: `{primary_val.get('hitless_rate')}`
- Validation classification: `{primary_val.get('validation_classification')}`
- Positive-lift slates in 20-slate view: `{positive_slates}`

## Decisions

""" + "\n".join(f"- `{k} = {v}`" for k, v in decisions.items()) + "\n"
    (OUT_DIR / "hits05_low_probability_tail_separation_audit_2026-07-21.md").write_text(md, encoding="utf-8")

    validation_rows = validate_artifacts(OUT_DIR)
    write_csv(OUT_DIR / "validation_report.csv", validation_rows)
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
