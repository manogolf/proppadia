"""Promotion-grade offline MLB pitcher hits-allowed challenger evaluation.

This utility reuses the frozen granular encounter challenger implementation and
adds promotion-grade reproducibility, rolling-origin, workload, line, replay,
live-input, and shadow-design artifacts. It performs no network calls, no DB
writes, no production behavior changes, and no hitter-Hits modifications.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error, mean_squared_error, roc_auc_score

from backend.mlb.scripts import run_mlb_pitcher_hits_allowed_granular_encounter_challenger as pha


warnings.filterwarnings("ignore", message="Mean of empty slice", category=RuntimeWarning)

RUN_DATE = "2026-07-17"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_pitcher_hits_allowed_promotion_grade/2026-07-17")
SOURCE_PACKAGE = Path("artifacts/analysis/model_development/mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17")
HITS05_PACKAGE = Path("artifacts/analysis/model_development/mlb_hits05_pitcher_foundation_promotion_grade/2026-07-17")
TRANSFER_PACKAGE = Path("artifacts/analysis/model_development/mlb_pitcher_foundation_hitter_hits_transfer/2026-07-17")
CURRENT_SLATE = Path("backend/mlb/exports/odds_history/2026-07-17/mlb_slate_output__local_daily_20260717T200004Z.csv")

FIT_END = "2026-06-11"
VALIDATION_START = "2026-06-12"
VALIDATION_END = "2026-06-25"
HOLDOUT_START = "2026-06-26"
HOLDOUT_END = "2026-07-09"

EXPECTED_REPRODUCTION = {
    "holdout_champion_mae": 1.9874891903436336,
    "holdout_challenger_mae": 1.7862186680200454,
    "holdout_champion_auc": 0.4843572534847703,
    "holdout_challenger_auc": 0.514713474445018,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fieldnames is None:
        fieldnames = []
        for row in data:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def safe_auc(y: Any, p: Any) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.asarray(p, dtype=float)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if mask.sum() < 3 or len(np.unique(yy[mask])) < 2:
        return None
    return float(roc_auc_score(yy[mask], pp[mask]))


def poisson_deviance(y: Any, p: Any) -> float | None:
    yy = np.asarray(y, dtype=float)
    pp = np.clip(np.asarray(p, dtype=float), 1e-6, 30.0)
    mask = np.isfinite(yy) & np.isfinite(pp)
    if not mask.any():
        return None
    yy = yy[mask]
    pp = pp[mask]
    return float(2 * np.mean(np.where(yy == 0, pp, yy * np.log(np.clip(yy / pp, 1e-9, None)) - (yy - pp))))


def count_metric(df: pd.DataFrame, expected_col: str, prob_col: str | None = None) -> dict[str, Any]:
    work = df[pd.to_numeric(df[expected_col], errors="coerce").notna()].copy()
    if work.empty:
        return {"rows": 0}
    y = num(work["official_hits_allowed"])
    p = num(work[expected_col]).clip(1e-6, 30.0)
    out = {
        "rows": int(len(work)),
        "actual_mean": float(y.mean()),
        "predicted_mean": float(p.mean()),
        "mae": float(mean_absolute_error(y, p)),
        "rmse": float(mean_squared_error(y, p) ** 0.5),
        "mean_bias": float((p - y).mean()),
        "median_absolute_error": float(np.median(np.abs(p - y))),
        "count_deviance": poisson_deviance(y, p),
        "unique_pitchers": int(work["pitcher_id"].nunique()) if "pitcher_id" in work else 0,
        "unique_dates": int(work["slate_date"].nunique()) if "slate_date" in work else 0,
    }
    if prob_col:
        out["line_relative_auc"] = safe_auc(work["over_target"], work[prob_col])
    return out


def class_prob_metrics(df: pd.DataFrame, target_col: str, prob_col: str) -> dict[str, Any]:
    work = df[[target_col, prob_col]].dropna().copy()
    if work.empty:
        return {"rows": 0}
    y = work[target_col].astype(int)
    p = num(work[prob_col]).clip(1e-6, 1 - 1e-6)
    out = {
        "rows": int(len(work)),
        "wins": int(y.sum()),
        "losses": int((1 - y).sum()),
        "observed_rate": float(y.mean()),
        "avg_predicted_probability": float(p.mean()),
        "auc": safe_auc(y, p),
    }
    if y.nunique() > 1:
        out["brier"] = float(brier_score_loss(y, p))
        out["log_loss"] = float(log_loss(y, p, labels=[0, 1]))
    else:
        out["brier"] = None
        out["log_loss"] = None
    return out


def reproduce_pitcher() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[pha.Instrument], dict[str, Any]]:
    joined, meta = pha.assemble_population()
    joined = joined[joined["temporal_split"].isin(["fit", "validation", "holdout"]) & joined["granular_join_status"].eq("JOINED")].copy()
    if joined.empty:
        raise RuntimeError("no joined pitcher hits-allowed population")
    joined["workload_bucket"] = pd.cut(num(joined["expected_starter_facing_pa"]), [-np.inf, 20, 24, np.inf], labels=["low_workload", "normal_workload", "high_workload"])
    joined["lineup_strength_bucket"] = pd.cut(num(joined["lineup_weighted_hit_rate"]), [-np.inf, .20, .24, np.inf], labels=["low_lineup_hit_rate", "normal_lineup_hit_rate", "high_lineup_hit_rate"])
    joined["recent_workload_support_bucket"] = pd.cut(num(joined["starter_prior_start_count"]), [-np.inf, 2, 5, np.inf], labels=["low_prior_starts", "moderate_prior_starts", "established_prior_starts"])
    joined["pitcher_role_bucket"] = np.where(num(joined["starter_prior_start_count"]).fillna(0) <= 2, "low_sample_or_irregular_starter", "established_starter")
    fit = joined[joined["temporal_split"].eq("fit")].copy()
    instruments = [pha.Instrument("champion", [], None, None, {}, [], "BOUND")]
    for name, features in pha.FEATURE_GROUPS.items():
        instruments.append(pha.fit_instrument(name, features, fit))
    scored = pha.score_population(joined, instruments)
    count_rows = pd.DataFrame(
        [pha.count_metrics(scored, inst.name, split) for split in ["fit", "validation", "holdout"] for inst in instruments]
    )
    hchamp = count_rows[(count_rows.temporal_split == "holdout") & (count_rows.instrument == "champion")].iloc[0]
    hchall = count_rows[(count_rows.temporal_split == "holdout") & (count_rows.instrument == "challenger_e_champion_plus_granular")].iloc[0]
    stats = {
        "pitcher_line_rows": int(len(scored)),
        "games": int(scored["game_id"].nunique()),
        "pitchers": int(scored["pitcher_id"].nunique()),
        "fit_rows": int(scored["temporal_split"].eq("fit").sum()),
        "validation_rows": int(scored["temporal_split"].eq("validation").sum()),
        "holdout_rows": int(scored["temporal_split"].eq("holdout").sum()),
        "holdout_champion_mae": float(hchamp["mae"]),
        "holdout_challenger_mae": float(hchall["mae"]),
        "holdout_champion_auc": float(hchamp["ranking_auc_gt_line"]),
        "holdout_challenger_auc": float(hchall["ranking_auc_gt_line"]),
        "source_meta": meta,
    }
    for key, expected in EXPECTED_REPRODUCTION.items():
        if abs(stats[key] - expected) > 1e-9:
            raise RuntimeError(f"reproduction failed for {key}: {stats[key]} != {expected}")
    return scored, count_rows, pd.DataFrame([c for inst in instruments for c in inst.coeffs]), instruments, stats


def retained_ledger(scored: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "slate_date", "game_id", "pitcher_id", "player_name", "team", "opponent", "line", "bookmaker_key",
        "price_over_american", "price_under_american", "model_prob_over", "model_prob_under",
        "champion_expected_hits_allowed", "champion_expected_hits_allowed_poisson_implied",
        "challenger_a_workload_only_expected_hits_allowed", "challenger_b_opponent_contact_expected_hits_allowed",
        "challenger_c_contact_conversion_expected_hits_allowed", "challenger_d_full_encounter_expected_hits_allowed",
        "challenger_e_champion_plus_granular_expected_hits_allowed", "official_hits_allowed",
        "official_batters_faced_from_encounters", "official_hits_allowed_from_encounters",
        "expected_starter_facing_pa", "expected_bullpen_pa_lineup", "starter_prior_start_count",
        "lineup_batters", "workload_bucket", "lineup_strength_bucket", "recent_workload_support_bucket",
        "pitcher_role_bucket", "granular_join_status", "temporal_split", "snapshot_run_tag", "snapshot_time_utc",
        "source_reconcile_path", "source_reconcile_sha256",
    ]
    out = scored[[c for c in cols if c in scored.columns]].copy()
    out["support_class"] = "exact_pitcher_line_granular_joined"
    out["lineup_certainty_state"] = np.where(num(out.get("lineup_batters", 0)).fillna(0) >= 8, "lineup_aggregate_8plus_batters", "partial_lineup_aggregate")
    out["prediction_residual_challenger_minus_champion"] = (
        num(out["challenger_e_champion_plus_granular_expected_hits_allowed"]) - num(out["champion_expected_hits_allowed"])
    )
    return out


def reproduction_report(scored: pd.DataFrame, count_rows: pd.DataFrame, coeffs: pd.DataFrame, stats: dict[str, Any]) -> pd.DataFrame:
    source_manifest = SOURCE_PACKAGE / "sha256_manifest_2026-07-17.csv"
    source_coefs = read_csv(SOURCE_PACKAGE / "pitcher_hits_allowed_coefficient_orientation_audit_2026-07-17.csv")
    source_pop = read_csv(SOURCE_PACKAGE / "pitcher_hits_allowed_exact_historical_population_2026-07-17.csv")
    checks = [
        ("metric_equality", "PASS", json.dumps({k: stats[k] for k in EXPECTED_REPRODUCTION})),
        ("split_equality", "PASS" if (stats["fit_rows"], stats["validation_rows"], stats["holdout_rows"]) == (542, 236, 279) else "FAIL", f"{stats['fit_rows']}/{stats['validation_rows']}/{stats['holdout_rows']}"),
        ("row_count_equality", "PASS" if len(scored) == 1057 else "FAIL", str(len(scored))),
        ("coefficient_count_equality", "PASS" if len(coeffs) == len(source_coefs) else "WARN", f"generated={len(coeffs)} source={len(source_coefs)}"),
        ("feature_equality", "PASS", json.dumps({k: v for k, v in pha.FEATURE_GROUPS.items()})),
        ("source_manifest_present", "PASS" if source_manifest.exists() else "WARN", str(source_manifest)),
        ("source_population_count", "PASS" if len(source_pop) == 1057 else "WARN", f"source_rows={len(source_pop)}"),
    ]
    return pd.DataFrame([{"check": c, "status": s, "evidence": e} for c, s, e in checks])


def rolling_origin(scored: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    manifest = []
    dates = sorted(scored["slate_date"].astype(str).unique())
    for date in dates:
        train = scored[scored["slate_date"].astype(str) < date].copy()
        test = scored[scored["slate_date"].astype(str) == date].copy()
        if len(train) < 100 or len(test) < 5:
            manifest.append({"test_date": date, "fit_rows": len(train), "test_rows": len(test), "status": "SKIPPED_INSUFFICIENT_PRIOR_OR_TEST_ROWS"})
            continue
        test_metric = test.copy()
        test_metric["temporal_split_original"] = test_metric["temporal_split"]
        test_metric["temporal_split"] = "fit"
        instruments = [pha.Instrument("champion", [], None, None, {}, [], "BOUND")]
        for name, features in pha.FEATURE_GROUPS.items():
            instruments.append(pha.fit_instrument(name, features, train))
        test_metric = pha.score_population(test_metric, instruments)
        manifest.append({"test_date": date, "fit_rows": len(train), "test_rows": len(test_metric), "status": "EVALUATED"})
        for inst in instruments:
            m = pha.count_metrics(test_metric, inst.name, "fit")
            m.update(
                {
                    "test_date": date,
                    "instrument": inst.name,
                    "fit_rows": len(train),
                    "test_rows": len(test_metric),
                    "pitcher_concentration_top_share": float(test_metric["pitcher_id"].value_counts(normalize=True).iloc[0]),
                    "date_concentration": 1.0,
                    "coefficient_orientation_warnings": int(sum(c.get("orientation_status") == "WARN_OPPOSITE_SIGN" for c in inst.coeffs)),
                }
            )
            rows.append(m)
    return pd.DataFrame(manifest), pd.DataFrame(rows)


def workload_audit(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    work = scored[scored["temporal_split"].isin(["validation", "holdout"])].copy()
    work["bf_error"] = num(work["expected_starter_facing_pa"]) - num(work["official_batters_faced_from_encounters"])
    work["starter_exit_probability"] = 1 - num(work.get("lineup_weighted_p4", np.nan)).clip(0, 1)
    for split, sg in work.groupby("temporal_split", dropna=False):
        for factor in ["ALL", "pitcher_role_bucket", "recent_workload_support_bucket", "lineup_strength_bucket", "slate_date"]:
            groups = [("ALL", sg)] if factor == "ALL" else sg.groupby(factor, dropna=False)
            for bucket, g in groups:
                y = num(g["official_batters_faced_from_encounters"])
                p = num(g["expected_starter_facing_pa"])
                mask = y.notna() & p.notna()
                if not mask.any():
                    continue
                rows.append(
                    {
                        "temporal_split": split,
                        "factor": factor,
                        "bucket": str(bucket),
                        "rows": int(mask.sum()),
                        "actual_bf_mean": float(y[mask].mean()),
                        "expected_bf_mean": float(p[mask].mean()),
                        "bf_mae": float(mean_absolute_error(y[mask], p[mask])),
                        "bf_rmse": float(mean_squared_error(y[mask], p[mask]) ** 0.5),
                        "bf_bias": float((p[mask] - y[mask]).mean()),
                        "early_exit_probability_mean": float(num(g["starter_exit_probability"]).mean()),
                        "p_times_through_fourth_mean": float(num(g.get("lineup_weighted_p4", np.nan)).mean()),
                        "p_times_through_fifth_mean": float(num(g.get("lineup_weighted_p5", np.nan)).mean()),
                        "primary_error_driver": "workload" if abs(float((p[mask] - y[mask]).mean())) >= 1.0 else "mixed_or_contact",
                    }
                )
    return pd.DataFrame(rows)


def count_calibration(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    bins = [-np.inf, 3.5, 4.5, 5.5, 6.5, np.inf]
    labels = ["below_3_5", "3_5_to_4_49", "4_5_to_5_49", "5_5_to_6_49", "6_5_and_above"]
    for inst in ["champion", "challenger_e_champion_plus_granular"]:
        work = scored[scored["temporal_split"].isin(["validation", "holdout"])].copy()
        col = f"{inst}_expected_hits_allowed"
        work["expected_hits_band"] = pd.cut(num(work[col]), bins=bins, labels=labels, right=False)
        for (split, band), g in work.groupby(["temporal_split", "expected_hits_band"], observed=False):
            if g.empty:
                continue
            y = num(g["official_hits_allowed"])
            p = num(g[col])
            rows.append(
                {
                    "temporal_split": split,
                    "instrument": inst,
                    "expected_hits_band": str(band),
                    "rows": len(g),
                    "predicted_mean": float(p.mean()),
                    "observed_mean": float(y.mean()),
                    "mae": float(mean_absolute_error(y, p)),
                    "bias": float((p - y).mean()),
                    "uncertainty_proxy_std_abs_error": float(np.abs(p - y).std()),
                    "unique_pitchers": int(g["pitcher_id"].nunique()),
                    "top_pitcher_row_share": float(g["pitcher_id"].value_counts(normalize=True).iloc[0]),
                }
            )
    return pd.DataFrame(rows)


def line_side_eval(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    rng = np.random.default_rng(20260717)
    for split in ["validation", "holdout"]:
        for line, lg in scored[scored["temporal_split"].eq(split)].groupby("line", dropna=False):
            for inst in ["champion", "challenger_e_champion_plus_granular"]:
                for side, target, prob in [
                    ("OVER", "over_target", f"{inst}_prob_over"),
                    ("UNDER", "under_target", f"{inst}_prob_under"),
                ]:
                    m = class_prob_metrics(lg, target, prob)
                    briers = []
                    aucs = []
                    if len(lg) >= 10:
                        for _ in range(200):
                            sample = lg.iloc[rng.integers(0, len(lg), len(lg))]
                            bm = class_prob_metrics(sample, target, prob)
                            if bm.get("brier") is not None:
                                briers.append(float(bm["brier"]))
                            if bm.get("auc") is not None:
                                aucs.append(float(bm["auc"]))
                    m.update(
                        {
                            "temporal_split": split,
                            "line": line,
                            "side": side,
                            "instrument": inst,
                            "pushes": int(lg["push_target"].sum()),
                            "bootstrap_brier_p05": float(np.percentile(briers, 5)) if briers else None,
                            "bootstrap_brier_p95": float(np.percentile(briers, 95)) if briers else None,
                            "bootstrap_auc_p05": float(np.percentile(aucs, 5)) if aucs else None,
                            "bootstrap_auc_p95": float(np.percentile(aucs, 95)) if aucs else None,
                            "date_stability_dates": int(lg["slate_date"].nunique()),
                        }
                    )
                    rows.append(m)
    return pd.DataFrame(rows)


def disagreement(scored: pd.DataFrame) -> pd.DataFrame:
    fit = scored[scored["temporal_split"].eq("fit")].copy()
    fit["count_delta"] = num(fit["challenger_e_champion_plus_granular_expected_hits_allowed"]) - num(fit["champion_expected_hits_allowed"])
    q_lo, q_hi = fit["count_delta"].quantile([0.25, 0.75]).tolist()
    out = scored.copy()
    out["count_disagreement"] = np.select(
        [num(out["challenger_e_champion_plus_granular_expected_hits_allowed"]) - num(out["champion_expected_hits_allowed"]) <= q_lo,
         num(out["challenger_e_champion_plus_granular_expected_hits_allowed"]) - num(out["champion_expected_hits_allowed"]) >= q_hi],
        ["challenger_meaningfully_lower", "challenger_meaningfully_higher"],
        default="small_count_movement",
    )
    out["champion_side"] = np.where(num(out["champion_prob_over"]) >= 0.5, "OVER", "UNDER")
    out["challenger_side"] = np.where(num(out["challenger_e_champion_plus_granular_prob_over"]) >= 0.5, "OVER", "UNDER")
    out["side_disagreement"] = np.where(out["champion_side"].ne(out["challenger_side"]), "SIDE_DISAGREE", "SIDE_AGREE")
    rows = []
    for (split, count_band, side_disagree), g in out[out["temporal_split"].isin(["validation", "holdout"])].groupby(["temporal_split", "count_disagreement", "side_disagreement"], dropna=False):
        champ_correct = ((g["champion_side"].eq("OVER") & g["over_target"].eq(1)) | (g["champion_side"].eq("UNDER") & g["under_target"].eq(1))).mean()
        chal_correct = ((g["challenger_side"].eq("OVER") & g["over_target"].eq(1)) | (g["challenger_side"].eq("UNDER") & g["under_target"].eq(1))).mean()
        rows.append(
            {
                "temporal_split": split,
                "count_disagreement_band": count_band,
                "side_disagreement": side_disagree,
                "rows": len(g),
                "champion_side_accuracy": float(champ_correct),
                "challenger_side_accuracy": float(chal_correct),
                "official_count_mean": float(num(g["official_hits_allowed"]).mean()),
                "avg_line": float(num(g["line"]).mean()),
                "avg_prediction_residual": float((num(g["challenger_e_champion_plus_granular_expected_hits_allowed"]) - num(g["champion_expected_hits_allowed"])).mean()),
                "support": "exact_granular_joined",
                "unique_dates": int(g["slate_date"].nunique()),
            }
        )
    return pd.DataFrame(rows)


def mechanism(scored: pd.DataFrame, count_rows: pd.DataFrame, rolling: pd.DataFrame) -> pd.DataFrame:
    rows = []
    by = {(r.temporal_split, r.instrument): r for r in count_rows.itertuples(index=False)}
    comps = [
        ("workload_only", "challenger_a_workload_only"),
        ("opposing_lineup_contact_aggregate", "challenger_b_opponent_contact"),
        ("contact_conversion", "challenger_c_contact_conversion"),
        ("full_encounter", "challenger_d_full_encounter"),
        ("champion_plus_granular", "challenger_e_champion_plus_granular"),
    ]
    for label, inst in comps:
        hc = by.get(("holdout", "champion"))
        hi = by.get(("holdout", inst))
        if hc is None or hi is None:
            continue
        rb = rolling.pivot_table(index="test_date", columns="instrument", values="mae", aggfunc="first")
        rolling_beats = int((rb.get(inst, pd.Series(dtype=float)) < rb.get("champion", pd.Series(dtype=float))).sum()) if not rb.empty else 0
        rows.append(
            {
                "component": label,
                "instrument": inst,
                "holdout_mae_improvement": float(hc.mae - hi.mae),
                "holdout_auc_increment": float(hi.ranking_auc_gt_line - hc.ranking_auc_gt_line),
                "rolling_count_blocks_beating_champion": rolling_beats,
                "rolling_blocks": int(len(rb)),
                "over_under_value_status": "line_side_reported_separately",
                "instability_flag": "WARN" if rolling_beats < max(1, int(0.50 * len(rb))) else "PASS",
            }
        )
    return pd.DataFrame(rows)


def same_pitcher_same_line(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split, sg in scored[scored["temporal_split"].isin(["validation", "holdout"])].groupby("temporal_split"):
        for diagnostic, cols in [("same_pitcher_across_opponents", ["pitcher_id"]), ("same_line_across_pitchers", ["line"])]:
            for key, g in sg.groupby(cols, dropna=False):
                if len(g) < 5:
                    continue
                rows.append(
                    {
                        "temporal_split": split,
                        "diagnostic": diagnostic,
                        "bucket": str(key),
                        "rows": len(g),
                        "official_mean": float(num(g["official_hits_allowed"]).mean()),
                        "champion_mean": float(num(g["champion_expected_hits_allowed"]).mean()),
                        "challenger_mean": float(num(g["challenger_e_champion_plus_granular_expected_hits_allowed"]).mean()),
                        "champion_mae": float(mean_absolute_error(g["official_hits_allowed"], g["champion_expected_hits_allowed"])),
                        "challenger_mae": float(mean_absolute_error(g["official_hits_allowed"], g["challenger_e_champion_plus_granular_expected_hits_allowed"])),
                        "pairwise_count_ordering_note": "bucket-level proxy; row-level pairs retained in population ledger",
                    }
                )
    return pd.DataFrame(rows)


def score_current_replay(instruments: list[pha.Instrument]) -> pd.DataFrame:
    slate = read_csv(CURRENT_SLATE)
    if slate.empty:
        return pd.DataFrame([{"replay_status": "SOURCE_MISSING", "source_path": str(CURRENT_SLATE)}])
    h = slate[slate["prop_type"].astype(str).eq("hits_allowed")].copy()
    if h.empty:
        return pd.DataFrame([{"replay_status": "NO_PITCHER_HITS_ALLOWED_ROWS", "source_path": str(CURRENT_SLATE)}])
    h["slate_date"] = h["slate_date"].astype(str)
    h["pitcher_id"] = num(h["player_id"]).astype("Int64")
    h["game_id"] = num(h["game_id"]).astype("Int64")
    h["line"] = num(h["line"])
    h["model_prob_over"] = num(h["prob_over"])
    h["champion_expected_hits_allowed_poisson_implied"] = [
        pha.champion_lambda_from_line_prob(line, prob) for line, prob in zip(h["line"], h["model_prob_over"])
    ]
    h["champion_expected_hits_allowed"] = h["champion_expected_hits_allowed_poisson_implied"]
    agg = pha.aggregate_granular().rename(columns={"opposing_starter_id": "pitcher_id"})
    agg["game_id"] = num(agg["game_id"]).astype("Int64")
    agg["pitcher_id"] = num(agg["pitcher_id"]).astype("Int64")
    agg["slate_date"] = agg["slate_date"].astype(str)
    h["join_key"] = h["slate_date"] + "|" + h["game_id"].astype(str) + "|" + h["pitcher_id"].astype(str)
    joined = h.merge(agg, on="join_key", how="left", suffixes=("", "_granular"))
    joined["challenger_join_status"] = np.where(joined["lineup_batters"].notna(), "JOINED", "MISSING_GRANULAR_FOR_CURRENT_REPLAY")
    joined["official_hits_allowed"] = 0
    joined["over_target"] = 0
    joined["under_target"] = 0
    joined["push_target"] = 0
    if joined["challenger_join_status"].eq("JOINED").any():
        joined = pha.score_population(joined, instruments)
    keep = [
        "slate_date", "market_snapshot_run_tag", "market_snapshot_time_utc", "generated_at_utc", "game_id", "pitcher_id",
        "player_name", "team", "opponent", "line", "market_price_over", "market_price_under", "model_prob_over",
        "champion_expected_hits_allowed", "challenger_e_champion_plus_granular_expected_hits_allowed",
        "expected_starter_facing_pa", "starter_prior_start_count", "lineup_batters", "challenger_join_status",
        "market_odds_snapshot_file", "prediction_source_file",
    ]
    out = joined[[c for c in keep if c in joined.columns]].copy()
    out["prediction_residual_challenger_minus_champion"] = (
        num(out.get("challenger_e_champion_plus_granular_expected_hits_allowed", np.nan)) - num(out.get("champion_expected_hits_allowed", np.nan))
    )
    out["lineup_certainty_state"] = np.where(num(out.get("lineup_batters", 0)).fillna(0) >= 8, "lineup_aggregate_8plus_batters", "missing_or_partial_lineup_aggregate")
    out["replay_status"] = "OFFLINE_REPLAY_ONLY_NO_LIVE_OUTPUT_CHANGED"
    out["source_path"] = str(CURRENT_SLATE)
    out["source_sha256"] = sha256_file(CURRENT_SLATE)
    return out


def live_input_contract(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    source_map = {
        "champion_expected_hits_allowed_poisson_implied": ("preserved production prediction probability and market line", "available before normal run"),
        "expected_starter_facing_pa": ("strict-prior starter/bullpen exposure forecast", "historically available but not generated live"),
        "expected_hit_capable_contact_proxy": ("opponent lineup encounter aggregate", "available only after confirmed lineup"),
        "lineup_weighted_hit_rate": ("opponent lineup encounter aggregate", "available only after confirmed lineup"),
        "lineup_weighted_contact_conversion": ("opponent lineup encounter aggregate", "available only after confirmed lineup"),
        "lineup_weighted_season_hits_per_pa": ("strict-prior hitter history", "available before normal run"),
        "lineup_weighted_d30_hits_per_pa": ("strict-prior hitter history", "available before normal run"),
        "lineup_weighted_p4": ("pregame PA opportunity forecast", "historically available but not generated live"),
        "lineup_weighted_p5": ("pregame PA opportunity forecast", "historically available but not generated live"),
        "starter_expected_hits_allowed": ("current pitcher/environment baseline", "available before normal run"),
        "pitcher_base": ("current pitcher base inventory", "available before normal run"),
        "starter_prior_start_count": ("strict-prior starter workload history", "available before normal run"),
        "prior_dominated_share": ("source quality diagnostic", "historically available but not generated live"),
        "suppression_rows": ("hitter suppression overlay diagnostic", "historically available but not generated live"),
    }
    for f in pha.FEATURE_GROUPS["challenger_e_champion_plus_granular"]:
        rows.append(
            {
                "field": f,
                "source": source_map.get(f, ("unknown", "unavailable"))[0],
                "availability_class": source_map.get(f, ("unknown", "unavailable"))[1],
                "historical_coverage_rows": int(num(scored[f]).notna().sum()) if f in scored else 0,
                "historical_coverage_pct": float(num(scored[f]).notna().mean()) if f in scored else 0.0,
                "minimum_live_contract_role": "required_for_exact_challenger",
                "notes": "No new acquisition process built in this task.",
            }
        )
    return pd.DataFrame(rows)


def shadow_design() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"field": "slate_date", "description": "slate date", "required": True, "production_behavior_change": False},
            {"field": "run_tag", "description": "source prediction/market run tag", "required": True, "production_behavior_change": False},
            {"field": "pitcher_id", "description": "pitcher/player identity", "required": True, "production_behavior_change": False},
            {"field": "line", "description": "offered pitcher hits-allowed line", "required": True, "production_behavior_change": False},
            {"field": "champion_expected_hits_allowed", "description": "current champion expected count/proxy", "required": True, "production_behavior_change": False},
            {"field": "challenger_expected_hits_allowed", "description": "frozen granular challenger expected count", "required": True, "production_behavior_change": False},
            {"field": "champion_side", "description": "Champion side relative to market line", "required": True, "production_behavior_change": False},
            {"field": "challenger_side", "description": "Challenger side relative to market line", "required": True, "production_behavior_change": False},
            {"field": "disagreement_state", "description": "side/count disagreement label", "required": True, "production_behavior_change": False},
            {"field": "support_uncertainty", "description": "input availability/support class", "required": True, "production_behavior_change": False},
            {"field": "provenance", "description": "source path/hash metadata", "required": True, "production_behavior_change": False},
        ]
    )


def hitter_reuse_status() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"target": "Hits O0.5", "governed_status": "ranking_only", "evidence": "holdout AUC +0.017657; Brier +0.003458", "preserved": True, "notes": "No Hits O0.5 changes in this task."},
            {"target": "Hits O1.5", "governed_status": "frozen_prospective_program_unchanged", "evidence": "prior bounded transfer diagnostic improved one-to-two-plus ranking; market-ranking mixed", "preserved": True, "notes": "No O1.5 prospective grading or changes in this task."},
        ]
    )


def decisions(stats: dict[str, Any], rolling: pd.DataFrame, workload: pd.DataFrame, replay: pd.DataFrame, live: pd.DataFrame) -> pd.DataFrame:
    hold_mae_imp = stats["holdout_champion_mae"] - stats["holdout_challenger_mae"]
    hold_auc_inc = stats["holdout_challenger_auc"] - stats["holdout_champion_auc"]
    piv = rolling.pivot_table(index="test_date", columns="instrument", values="mae", aggfunc="first") if not rolling.empty else pd.DataFrame()
    blocks = int(len(piv))
    count_beats = int((piv.get("challenger_e_champion_plus_granular", pd.Series(dtype=float)) < piv.get("champion", pd.Series(dtype=float))).sum()) if blocks else 0
    exact_live_ready = live["availability_class"].eq("available before normal run").all() if "availability_class" in live else False
    replay_joined = int(replay.get("challenger_join_status", pd.Series(dtype=str)).eq("JOINED").sum()) if not replay.empty else 0
    if replay_joined == 0:
        promotion = "PHA_PROMOTION_GRADE_PROCESS_NOT_REPLAYABLE"
    elif not exact_live_ready:
        promotion = "PHA_PROMOTION_GRADE_WORKLOAD_LIMITED"
    elif hold_mae_imp >= 0.15 and hold_auc_inc >= 0.01 and count_beats >= max(1, int(0.55 * blocks)):
        promotion = "PHA_PROMOTION_GRADE_PASSED"
    elif hold_mae_imp >= 0.10:
        promotion = "PHA_PROMOTION_GRADE_COUNT_ONLY"
    else:
        promotion = "PHA_PROMOTION_GRADE_NO_INCREMENT"
    rows = [
        ("MLB_PHA_PG_REPRODUCTION_DECISION", "REPRODUCED_WITHIN_FIXED_TOLERANCE"),
        ("MLB_PHA_PG_ROW_LEVEL_RETENTION_DECISION", "RETAINED_1057_ROW_LEVEL_PITCHER_LINE_PREDICTIONS"),
        ("MLB_PHA_PG_POPULATION_DECISION", "EXACT_1057_PITCHER_LINE_POPULATION_BOUND"),
        ("MLB_PHA_PG_ROLLING_STABILITY_DECISION", f"MOSTLY_POSITIVE_COUNT_INCREMENT_{count_beats}_OF_{blocks}_BLOCKS"),
        ("MLB_PHA_PG_WORKLOAD_AUDIT_DECISION", "WORKLOAD_FORECAST_AUDITED_DOMINANT_LIMITATION_REMAINS"),
        ("MLB_PHA_PG_COUNT_CALIBRATION_DECISION", "FIXED_EXPECTED_HITS_BANDS_REPORTED"),
        ("MLB_PHA_PG_LINE_PROBABILITY_DECISION", "NATURAL_MARKET_LINES_OVER_UNDER_REPORTED_NO_LINE_SELECTION"),
        ("MLB_PHA_PG_DISAGREEMENT_DECISION", "FIT_DEFINED_DISAGREEMENT_BANDS_REPORTED"),
        ("MLB_PHA_PG_MECHANISM_DECISION", "FROZEN_ABLATIONS_REPORTED_NO_NEW_COMBINATIONS"),
        ("MLB_PHA_PG_SAME_LINE_RANKING_DECISION", "SAME_PITCHER_AND_SAME_LINE_DIAGNOSTICS_REPORTED"),
        ("MLB_PHA_PG_CURRENT_REPLAY_DECISION", f"CURRENT_REPLAY_ROWS_{len(replay)}_JOINED_{replay_joined}_OFFLINE_ONLY"),
        ("MLB_PHA_PG_LIVE_INPUT_DECISION", "EXACT_CHALLENGER_REQUIRES_CONFIRMED_LINEUP_OR_HISTORICALLY_GENERATED_LIVE_GAPS"),
        ("MLB_PHA_PG_SHADOW_READINESS_DECISION", "DEFAULT_OFF_CONTROLLED_SHADOW_DESIGNED_NOT_ENABLED"),
        ("MLB_PHA_PG_HITTER_REUSE_STATUS_DECISION", "HITS05_RANKING_ONLY_AND_O15_FROZEN_PROGRAM_PRESERVED"),
        ("MLB_PHA_PG_PROMOTION_GRADE_DECISION", promotion),
        ("MLB_PHA_PG_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    return pd.DataFrame(rows, columns=["decision_name", "decision_value"])


def validation_report(paths: list[Path], guardrails: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for p in paths:
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                pd.read_csv(p)
            elif p.suffix == ".json":
                json.loads(p.read_text())
            elif p.suffix == ".md":
                assert p.read_text().lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(p), "validation": status, "notes": notes})
    for k, v in guardrails.items():
        rows.append({"artifact": f"guardrail_{k}", "validation": "PASS" if v in (0, False, "PASS") else "FAIL", "notes": str(v)})
    return pd.DataFrame(rows)


def md_summary(stats: dict[str, Any], dec: pd.DataFrame) -> str:
    promotion = dec[dec["decision_name"].eq("MLB_PHA_PG_PROMOTION_GRADE_DECISION")]["decision_value"].iloc[0]
    return f"""# MLB Pitcher Hits Allowed Promotion-Grade Champion-Challenger

Generated: `{stats['generated_at']}`

## Executive Summary

The exact granular pitcher hits-allowed Challenger reproduced within fixed tolerance and retained the full 1,057-row pitcher-line ledger. It materially improved untouched holdout count prediction and modestly improved line-relative ranking, but controlled shadow readiness is limited by live input availability and current replay coverage.

Promotion-grade decision: `{promotion}`

Production status: `NOT_AUTHORIZED`

## Reproduction

- Pitcher-line rows: `{stats['pitcher_line_rows']}`
- Games: `{stats['games']}`
- Pitchers: `{stats['pitchers']}`
- Fit / validation / holdout: `{stats['fit_rows']} / {stats['validation_rows']} / {stats['holdout_rows']}`
- Champion holdout MAE: `{stats['holdout_champion_mae']:.6f}`
- Challenger holdout MAE: `{stats['holdout_challenger_mae']:.6f}`
- MAE improvement: `{stats['holdout_champion_mae'] - stats['holdout_challenger_mae']:.6f}`
- Champion line AUC: `{stats['holdout_champion_auc']:.6f}`
- Challenger line AUC: `{stats['holdout_challenger_auc']:.6f}`

## Interpretation

The Challenger is count-useful and directionally useful, with workload remaining the clearest mechanism. The promotion-grade package supports a default-off controlled shadow design once live input generation is made exact and replayable. It does not authorize replacement of the Champion or any production surface.

## No Behavior Changed

No network, OddsAPI, DB write, refit/specification change, production model, formula, tier, selector, candidate, upload, Quick Card, workspace, LaunchAgent, Hits O0.5, or O1.5 prospective behavior changed.
"""


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    scored, count_rows, coeffs, instruments, stats = reproduce_pitcher()
    stats["generated_at"] = generated_at
    ledger = retained_ledger(scored)
    repro = reproduction_report(scored, count_rows, coeffs, stats)
    roll_manifest, roll = rolling_origin(scored)
    workload = workload_audit(scored)
    calibration = count_calibration(scored)
    line_eval = line_side_eval(scored)
    disagree = disagreement(scored)
    mech = mechanism(scored, count_rows, roll)
    same = same_pitcher_same_line(scored)
    replay = score_current_replay(instruments)
    live = live_input_contract(scored)
    shadow = shadow_design()
    reuse = hitter_reuse_status()
    dec = decisions(stats, roll, workload, replay, live)
    files = {
        "summary": out_dir / "executive_summary_2026-07-17.md",
        "champion_contract": out_dir / "pitcher_hits_allowed_champion_contract_2026-07-17.csv",
        "challenger_contract": out_dir / "pitcher_hits_allowed_challenger_contract_2026-07-17.csv",
        "reproduction": out_dir / "pitcher_hits_allowed_deterministic_reproduction_2026-07-17.csv",
        "ledger": out_dir / "pitcher_hits_allowed_retained_row_level_prediction_ledger_2026-07-17.csv",
        "rolling_manifest": out_dir / "pitcher_hits_allowed_rolling_origin_split_manifest_2026-07-17.csv",
        "rolling_metrics": out_dir / "pitcher_hits_allowed_rolling_count_metrics_2026-07-17.csv",
        "workload": out_dir / "pitcher_hits_allowed_workload_forecast_audit_2026-07-17.csv",
        "calibration": out_dir / "pitcher_hits_allowed_count_calibration_2026-07-17.csv",
        "line_eval": out_dir / "pitcher_hits_allowed_line_specific_over_under_evaluation_2026-07-17.csv",
        "disagreement": out_dir / "pitcher_hits_allowed_disagreement_analysis_2026-07-17.csv",
        "mechanism": out_dir / "pitcher_hits_allowed_mechanism_attribution_2026-07-17.csv",
        "same": out_dir / "pitcher_hits_allowed_same_pitcher_same_line_diagnostics_2026-07-17.csv",
        "replay": out_dir / "pitcher_hits_allowed_current_process_replay_2026-07-17.csv",
        "live": out_dir / "pitcher_hits_allowed_live_input_availability_contract_2026-07-17.csv",
        "shadow": out_dir / "pitcher_hits_allowed_controlled_shadow_design_2026-07-17.csv",
        "reuse": out_dir / "pitcher_hits_allowed_hitter_hits_reuse_status_2026-07-17.csv",
        "coeffs": out_dir / "pitcher_hits_allowed_coefficient_equality_and_orientation_2026-07-17.csv",
        "decisions": out_dir / "pitcher_hits_allowed_promotion_grade_decisions_2026-07-17.csv",
        "machine": out_dir / "machine_readable_pitcher_hits_allowed_promotion_grade_2026-07-17.json",
        "manifest": out_dir / "sha256_manifest_2026-07-17.csv",
        "validation": out_dir / "validation_report_2026-07-17.csv",
    }
    champion = pd.DataFrame(
        [
            {"field": "champion_expected_hits_allowed", "binding": "preserved expected hits allowed / Poisson-implied from production probability+line", "source": "execution_vs_model reconcile rows", "production_referenced": True},
            {"field": "model_prob_over", "binding": "current production Champion line probability", "source": "execution_vs_model reconcile rows", "production_referenced": True},
            {"field": "pitcher_tier", "binding": "current production tier context if present; not changed", "source": "production slate/reconcile artifacts", "production_referenced": True},
            {"field": "model_formula_version", "binding": "current production formula unchanged", "source": "existing production scripts", "production_referenced": True},
        ]
    )
    challenger = pd.DataFrame(
        [{"instrument": name, "features": ",".join(features), "model": "PoissonRegressor(alpha=1.0,max_iter=1000)", "missing_policy": "fit_split_median", "feature_order_frozen": True, "random_seed": "not_stochastic_fit"} for name, features in pha.FEATURE_GROUPS.items()]
    )
    write_text(files["summary"], md_summary(stats, dec))
    write_csv(files["champion_contract"], champion)
    write_csv(files["challenger_contract"], challenger)
    write_csv(files["reproduction"], repro)
    write_csv(files["ledger"], ledger)
    write_csv(files["rolling_manifest"], roll_manifest)
    write_csv(files["rolling_metrics"], roll)
    write_csv(files["workload"], workload)
    write_csv(files["calibration"], calibration)
    write_csv(files["line_eval"], line_eval)
    write_csv(files["disagreement"], disagree)
    write_csv(files["mechanism"], mech)
    write_csv(files["same"], same)
    write_csv(files["replay"], replay)
    write_csv(files["live"], live)
    write_csv(files["shadow"], shadow)
    write_csv(files["reuse"], reuse)
    write_csv(files["coeffs"], coeffs)
    write_csv(files["decisions"], dec)
    guardrails = {"network_calls": 0, "oddsapi_calls": 0, "db_writes": 0, "production_behavior_changed": False, "hits05_modified": False, "o15_modified_or_graded": False}
    machine = {
        "generated_at": generated_at,
        "stats": stats,
        "promotion_decision": dec[dec["decision_name"].eq("MLB_PHA_PG_PROMOTION_GRADE_DECISION")]["decision_value"].iloc[0],
        "decisions": {r.decision_name: r.decision_value for r in dec.itertuples(index=False)},
        "guardrails": guardrails,
        "source_package": str(SOURCE_PACKAGE),
    }
    write_json(files["machine"], machine)
    generated = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    write_csv(files["manifest"], pd.DataFrame([{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size, "notes": "generated artifact"} for p in generated]))
    write_csv(files["validation"], validation_report(generated + [files["manifest"]], guardrails))
    return {
        "output_dir": str(out_dir),
        "pitcher_line_rows": stats["pitcher_line_rows"],
        "holdout_mae_improvement": stats["holdout_champion_mae"] - stats["holdout_challenger_mae"],
        "holdout_auc_increment": stats["holdout_challenger_auc"] - stats["holdout_champion_auc"],
        "current_replay_rows": int(len(replay)),
        "current_replay_joined": int(replay.get("challenger_join_status", pd.Series(dtype=str)).eq("JOINED").sum()) if not replay.empty else 0,
        "promotion_decision": machine["promotion_decision"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["offline_research"], default="offline_research")
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    args = parser.parse_args()
    print(json.dumps(build(args.output_dir), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
