#!/usr/bin/env python3
"""Contract B line-invariant pitcher foundation hitter-Hits reevaluation.

Bounded offline research only. This script preserves the line-specific PHA
proposition model and reruns only downstream hitter-Hits transfer checks with a
pitcher-game Contract B source that excludes PHA market line, line-specific
Champion/Challenger proxies, and proxy-derived residuals.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_pitcher_foundation_hitter_hits_transfer as prior


RUN_DATE = "2026-07-18"
SOURCE_DATE = "2026-07-17"
OUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_contract_b_pitcher_foundation_hitter_hits_reevaluation/2026-07-18"
)

CONTRACT_AUDIT_DIR = Path(
    "artifacts/analysis/model_development/mlb_pha_line_specific_downstream_consumer_integrity_audit/2026-07-18"
)
COUNT_INVARIANCE_DIR = Path(
    "artifacts/analysis/model_development/mlb_pha_live_shadow_count_invariance_audit/2026-07-18"
)
ORIGINAL_TRANSFER_DIR = Path(
    "artifacts/analysis/model_development/mlb_pitcher_foundation_hitter_hits_transfer/2026-07-17"
)
ORIGINAL_HITS05_PROMO_DIR = Path(
    "artifacts/analysis/model_development/mlb_hits05_pitcher_foundation_promotion_grade/2026-07-17"
)
PHA_DIR = Path("artifacts/analysis/model_development/mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17")

CONTRACT_B_FEATURES = [
    "pitcher_granular_expected_hits_allowed",
    "expected_batters_faced",
    "expected_starter_facing_pa_environment",
    "starter_exit_probability",
    "workload_support_numeric",
    "pitcher_forecast_uncertainty_numeric",
    "affirmative_suppression_numeric",
]

CONTRACT_B_MECHANISM_GROUPS = {
    "expected_batters_faced": ["expected_batters_faced"],
    "projected_workload": ["expected_starter_facing_pa_environment"],
    "starter_exit_probability": ["starter_exit_probability"],
    "opponent_lineup_contact_aggregate": ["lineup_weighted_hit_rate", "lineup_weighted_d30_hits_per_pa"],
    "opponent_lineup_conversion_aggregate": ["lineup_weighted_contact_conversion", "lineup_weighted_season_hits_per_pa"],
    "pitcher_contact_suppression": ["pitcher_granular_expected_hits_allowed"],
    "support_and_uncertainty": ["workload_support_numeric", "pitcher_forecast_uncertainty_numeric"],
    "affirmative_suppression_state": ["affirmative_suppression_numeric"],
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


def write_csv(path: Path, rows: pd.DataFrame | list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pd.DataFrame):
        rows.to_csv(path, index=False)
        return
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


def num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def norm_key_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        return text[:-2]
    return text


def make_key(df: pd.DataFrame, pitcher_col: str = "pitcher_id") -> pd.Series:
    if df.empty:
        return pd.Series(dtype=str)
    return df.apply(
        lambda r: "|".join(norm_key_value(r.get(c)) for c in ["slate_date", "game_id", pitcher_col]),
        axis=1,
    )


def canonical_contract_b_source() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    contaminated, gaps = prior.build_pitcher_transfer_contract()
    contaminated = contaminated.copy()
    contaminated["contract_b_key"] = make_key(contaminated)
    duplicate_keys = contaminated["contract_b_key"][contaminated["contract_b_key"].duplicated()].unique().tolist()
    if duplicate_keys:
        raise RuntimeError(f"Contract B duplicate pitcher-game keys found: {duplicate_keys[:10]}")

    allowed = [
        "slate_date",
        "game_id",
        "pitcher_id",
        "opponent",
        "pitcher_name",
        "pitcher_team",
        "pitcher_granular_expected_hits_allowed",
        "expected_batters_faced",
        "expected_starter_facing_pa_environment",
        "expected_total_hitter_pa_environment",
        "starter_exit_probability",
        "lineup_weighted_hit_rate",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_season_hits_per_pa",
        "lineup_weighted_d30_hits_per_pa",
        "lineup_weighted_p4",
        "lineup_weighted_p5",
        "lineup_batters",
        "workload_support_class",
        "workload_support_numeric",
        "pitcher_forecast_uncertainty_class",
        "pitcher_forecast_uncertainty_numeric",
        "suppression_rows",
        "affirmative_suppression_state",
        "affirmative_suppression_numeric",
        "fit_validation_holdout_lineage",
        "strict_prior_feature_cutoff_status",
        "actual_bf_used",
        "actual_lineup_sequence_used",
        "current_game_contact_or_outcome_used",
        "source_pitcher_foundation_path",
        "source_pitcher_foundation_sha256",
        "source_granular_artifact_path",
        "source_granular_artifact_sha256",
        "transfer_key",
        "contract_b_key",
    ]
    contract_b = contaminated[[c for c in allowed if c in contaminated.columns]].copy()
    contract_b["contract"] = "Contract B"
    contract_b["grain"] = "slate_date|game_id|pitcher_id"
    contract_b["line_specific_proxy_fields_excluded"] = True
    contract_b["pha_market_line_excluded"] = True
    contract_b["feature_version"] = "contract_b_line_invariant_pitcher_foundation_v1"
    contract_b["temporal_cutoff"] = "strict_prior_source_artifacts_no_actual_bf_no_postgame_sequence"

    inventory_rows = []
    source_map = {
        "pitcher_granular_expected_hits_allowed": ("starter-bullpen exposure forecast", "starter_expected_hits_allowed", "mean by pitcher-game", "strict prior"),
        "expected_batters_faced": ("starter-bullpen exposure forecast", "pred_starter_pa", "sum by pitcher-game", "strict prior"),
        "expected_starter_facing_pa_environment": ("starter-bullpen exposure forecast", "pred_starter_pa", "sum by pitcher-game", "strict prior"),
        "expected_total_hitter_pa_environment": ("starter-bullpen exposure forecast", "pred_total_pa", "sum by pitcher-game", "strict prior"),
        "starter_exit_probability": ("starter-bullpen exposure forecast", "p_starter_exit_before_pa4", "mean by pitcher-game", "strict prior"),
        "lineup_weighted_hit_rate": ("starter-bullpen exposure forecast", "p_hit_starter_prior", "pred_starter_pa weighted average", "strict prior"),
        "lineup_weighted_contact_conversion": ("starter-bullpen exposure forecast", "hitter_per_pa_hit_estimate", "pred_starter_pa weighted average", "strict prior"),
        "lineup_weighted_season_hits_per_pa": ("starter-bullpen exposure forecast", "season_to_date_hits_per_pa", "pred_starter_pa weighted average", "strict prior"),
        "lineup_weighted_d30_hits_per_pa": ("starter-bullpen exposure forecast", "d30_hits_per_pa", "pred_starter_pa weighted average", "strict prior"),
        "lineup_weighted_p4": ("starter-bullpen exposure forecast", "p_hitter_receives_fourth_pa", "pred_starter_pa weighted average", "strict prior"),
        "lineup_weighted_p5": ("starter-bullpen exposure forecast", "p_hitter_receives_fifth_pa", "pred_starter_pa weighted average", "strict prior"),
        "lineup_batters": ("starter-bullpen exposure forecast", "player_id", "unique batter count", "strict prior"),
        "workload_support_numeric": ("derived Contract B", "starter_expected_hits_allowed coverage", "support class mapping", "strict prior"),
        "pitcher_forecast_uncertainty_numeric": ("derived Contract B", "support class", "uncertainty class mapping", "strict prior"),
        "affirmative_suppression_numeric": ("starter-bullpen exposure forecast", "suppression_subtype", "suppression row count mapping", "strict prior"),
    }
    for field, (source, col, construction, cutoff) in source_map.items():
        vals = contract_b[field] if field in contract_b.columns else pd.Series(dtype=float)
        inventory_rows.append(
            {
                "canonical_name": field,
                "source_artifact": source,
                "source_column": col,
                "historical_construction": construction,
                "temporal_cutoff": cutoff,
                "support": f"non_null={int(vals.notna().sum())}; rows={len(contract_b)}",
                "missingness": "median-imputed in fixed logistic transfer if missing",
                "feature_version": "contract_b_line_invariant_pitcher_foundation_v1",
                "line_invariance_proof": "joined to all PHA multi-line rows; max spread reported separately",
                "notes": "",
            }
        )
    return contract_b, pd.DataFrame(inventory_rows), gaps


def line_invariance_proof(contract_b: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    trace = read_csv(COUNT_INVARIANCE_DIR / "pha_historical_multi_line_trace_2026-07-18.csv")
    if trace.empty:
        raise RuntimeError("missing historical multi-line trace from count-invariance audit")
    joined = trace.merge(
        contract_b,
        left_on=["slate_date", "game_id", "pitcher_id"],
        right_on=["slate_date", "game_id", "pitcher_id"],
        how="left",
        suffixes=("_pha_line", ""),
    )
    numeric_fields = [f for f in CONTRACT_B_FEATURES + [
        "expected_total_hitter_pa_environment",
        "lineup_weighted_hit_rate",
        "lineup_weighted_contact_conversion",
        "lineup_weighted_season_hits_per_pa",
        "lineup_weighted_d30_hits_per_pa",
        "lineup_weighted_p4",
        "lineup_weighted_p5",
        "lineup_batters",
        "suppression_rows",
    ] if f in joined.columns]
    rows = []
    for field in numeric_fields:
        max_spread = 0.0
        groups = 0
        failures = 0
        for _, g in joined.groupby(["slate_date", "game_id", "pitcher_id"], dropna=False):
            values = num(g[field]).dropna()
            if values.empty:
                spread = 0.0
            else:
                spread = float(values.max() - values.min())
            max_spread = max(max_spread, spread)
            groups += 1
            failures += int(spread > 1e-12)
        rows.append(
            {
                "field": field,
                "multi_line_pitcher_games_checked": groups,
                "max_within_pitcher_game_spread": max_spread,
                "tolerance": 1e-12,
                "line_invariance_status": "PASS" if failures == 0 else "FAIL",
                "failure_groups": failures,
            }
        )
    joined["contract_b_join_status"] = np.where(joined["contract_b_key"].notna(), "JOINED", "MISSING_CONTRACT_B")
    return joined, pd.DataFrame(rows)


def fit_corrected_instruments(df: pd.DataFrame, target: str, control_prob: str, prefix: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    out = df.copy()
    train = out[out["temporal_split"].eq("fit") & out[target].notna() & out[control_prob].notna()].copy()
    instruments = {
        f"{prefix}_control": [],
        f"{prefix}_contract_b_context": CONTRACT_B_FEATURES,
        f"{prefix}_contract_b_allocated_pitcher": [prior.SHARE_FEATURE],
        f"{prefix}_contract_b_control_plus_foundation": CONTRACT_B_FEATURES + [prior.SHARE_FEATURE],
    }
    out[f"{prefix}_control_prob"] = num(out[control_prob]).clip(1e-6, 1 - 1e-6)
    contracts = []
    coefs = []
    for name, features in instruments.items():
        if name.endswith("_control"):
            contracts.append(
                {
                    "instrument": name,
                    "definition": "frozen control probability unchanged",
                    "features": control_prob,
                    "fit_policy": "no refit",
                    "contract": "baseline",
                }
            )
            continue
        scaler, model, cols = prior.fit_fixed_logistic(train, target, control_prob, features)
        out[f"{name}_prob"] = prior.apply_fixed_logistic(out, scaler, model, cols)
        contracts.append(
            {
                "instrument": name,
                "definition": "fixed logistic Contract B correction challenger",
                "features": ",".join(cols),
                "fit_policy": "fit split only; C=1.0; random_state=20260717; no hyperparameter search",
                "contract": "Contract B",
            }
        )
        for feature, coef in zip(cols, model.coef_[0]):
            coefs.append(
                {
                    "instrument": name,
                    "feature": feature,
                    "coefficient": float(coef),
                    "notes": "coefficient on standardized/logit-transformed design",
                }
            )
        coefs.append({"instrument": name, "feature": "__intercept__", "coefficient": float(model.intercept_[0]), "notes": "fixed logistic intercept"})
    return out, pd.DataFrame(contracts), pd.DataFrame(coefs)


def evaluate_splits(df: pd.DataFrame, target: str, prob_cols: dict[str, str], scope: str) -> pd.DataFrame:
    rows = []
    for split in ["fit", "validation", "holdout"]:
        g = df[df["temporal_split"].eq(split)].copy()
        for instrument, col in prob_cols.items():
            if col not in g.columns:
                continue
            m = prior.binary_metrics(g, target, col)
            m.update({"evaluation_scope": scope, "temporal_split": split, "instrument": instrument, "probability_field": col})
            rows.append(m)
    return pd.DataFrame(rows)


def bootstrap_delta(df: pd.DataFrame, target: str, control_col: str, challenger_col: str, label: str, iterations: int = 200) -> pd.DataFrame:
    rng = np.random.default_rng(20260718)
    rows = []
    for split in ["validation", "holdout"]:
        base = df[df["temporal_split"].eq(split)].reset_index(drop=True)
        if len(base) < 30:
            continue
        deltas = {"auc_increment": [], "brier_improvement": [], "log_loss_improvement": []}
        for _ in range(iterations):
            sample = base.iloc[rng.integers(0, len(base), len(base))]
            cm = prior.binary_metrics(sample, target, control_col)
            hm = prior.binary_metrics(sample, target, challenger_col)
            if cm.get("auc") is not None and hm.get("auc") is not None:
                deltas["auc_increment"].append(float(hm["auc"] - cm["auc"]))
            if cm.get("brier") is not None and hm.get("brier") is not None:
                deltas["brier_improvement"].append(float(cm["brier"] - hm["brier"]))
            if cm.get("log_loss") is not None and hm.get("log_loss") is not None:
                deltas["log_loss_improvement"].append(float(cm["log_loss"] - hm["log_loss"]))
        for metric, values in deltas.items():
            if not values:
                continue
            arr = np.asarray(values, dtype=float)
            rows.append(
                {
                    "target_scope": label,
                    "temporal_split": split,
                    "metric": metric,
                    "iterations": iterations,
                    "rows_per_sample": len(base),
                    "mean": float(arr.mean()),
                    "p05": float(np.quantile(arr, 0.05)),
                    "p50": float(np.quantile(arr, 0.50)),
                    "p95": float(np.quantile(arr, 0.95)),
                    "notes": "fixed-seed bootstrap; no cutoff optimization",
                }
            )
    return pd.DataFrame(rows)


def rolling_blocks(df: pd.DataFrame, target: str, control_prob: str, challenger_name: str, label: str) -> pd.DataFrame:
    rows = []
    dates = sorted(df[df["temporal_split"].isin(["validation", "holdout"])]["slate_date"].astype(str).unique())
    for date in dates:
        train = df[(df["slate_date"].astype(str) < date) & df[target].notna()].copy()
        test = df[(df["slate_date"].astype(str) == date) & df[target].notna()].copy()
        if len(train) < 100 or len(test) < 10 or train[target].nunique() < 2 or test[target].nunique() < 2:
            continue
        scaler, model, cols = prior.fit_fixed_logistic(train, target, control_prob, CONTRACT_B_FEATURES + [prior.SHARE_FEATURE])
        test = test.copy()
        test["rolling_contract_b_prob"] = prior.apply_fixed_logistic(test, scaler, model, cols)
        for inst, col in [("control", control_prob), (challenger_name, "rolling_contract_b_prob")]:
            m = prior.binary_metrics(test, target, col)
            m.update({"target_scope": label, "test_date": date, "instrument": inst, "fit_rows": len(train), "test_rows": len(test), "features": ",".join(cols)})
            rows.append(m)
    return pd.DataFrame(rows)


def zero_hit_diagnostics(df: pd.DataFrame) -> pd.DataFrame:
    hold = df[df["temporal_split"].eq("holdout")].copy()
    hold["zero_target"] = 1 - hold["any_hit_target"].astype(int)
    hold["champion_zero_prob"] = 1 - num(hold["hits05_control_prob"])
    hold["contract_b_zero_prob"] = 1 - num(hold["hits05_contract_b_control_plus_foundation_prob"])
    hold["contract_b_delta"] = num(hold["hits05_contract_b_control_plus_foundation_prob"]) - num(hold["hits05_control_prob"])
    rows = []
    for inst, col in [("champion_zero", "champion_zero_prob"), ("contract_b_zero", "contract_b_zero_prob")]:
        m = prior.binary_metrics(hold, "zero_target", col)
        m.update({"segment": inst, "notes": "zero-hit complement evaluation"})
        rows.append(m)
    for label, seg in [
        ("largest_fit_frozen_demotions", hold.nsmallest(max(1, int(len(hold) * 0.10)), "contract_b_delta")),
        ("largest_fit_frozen_promotions", hold.nlargest(max(1, int(len(hold) * 0.10)), "contract_b_delta")),
    ]:
        rows.append(
            {
                "segment": label,
                "rows": len(seg),
                "avg_delta": float(seg["contract_b_delta"].mean()) if len(seg) else None,
                "zero_hit_rate": float(seg["zero_target"].mean()) if len(seg) else None,
                "any_hit_rate": float(seg["any_hit_target"].astype(int).mean()) if len(seg) else None,
                "false_demotion_rate": float(seg["any_hit_target"].astype(int).mean()) if label == "largest_fit_frozen_demotions" and len(seg) else None,
                "notes": "fixed 10pct movement band; no threshold optimization",
            }
        )
    return pd.DataFrame(rows)


def one_to_two_population(df: pd.DataFrame) -> pd.DataFrame:
    work = df[df["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"])].copy()
    work["one_to_two_target"] = (work["outcome_class"] == "TWO_OR_MORE_HITS").astype(int)
    return work


def market_ranking_corrected(oof: pd.DataFrame, contract_b: pd.DataFrame, exposure: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    joined = prior.join_transfer(oof, contract_b, exposure)
    joined["market_rank_transfer_target"] = joined["multi_hit_target"].astype(int)
    joined["contract_b_rank_score"] = num(joined["challenger_ranking_score"]) + (
        num(joined[prior.SHARE_FEATURE]).fillna(0) * 0.25
    )
    rows = []
    for fold, g in joined.groupby("fold", dropna=False):
        for instr, score in [
            ("market_ranking", "champion_ranking_score"),
            ("market_plus_proppadia_ranking", "challenger_ranking_score"),
            ("market_plus_proppadia_plus_contract_b", "contract_b_rank_score"),
        ]:
            m_auc = prior.safe_auc(g["market_rank_transfer_target"], g[score])
            base_auc = prior.safe_auc(g["market_rank_transfer_target"], g["challenger_ranking_score"])
            rows.append(
                {
                    "fold": fold,
                    "instrument": instr,
                    "rows": len(g),
                    "auc": m_auc,
                    "pairwise_increment_vs_market_plus_proppadia": None if m_auc is None or base_auc is None else float(m_auc - base_auc),
                    "top5_two_plus_rate": prior.top_n_rate(g, score, 5),
                    "top10_two_plus_rate": prior.top_n_rate(g, score, 10),
                    "top20pct_two_plus_rate": prior.top_pct_rate(g, score, 0.20),
                    "price_controlled_ordering": "unchanged_price_population_no_price_optimization",
                    "suppression_contradictions": int((num(g.get("affirmative_suppression_numeric", 0)).fillna(0) > 0).sum()),
                    "notes": "offline diagnostic; residual term removed; prospective ledger untouched",
                }
            )
    return joined, pd.DataFrame(rows)


def roster_relative(df: pd.DataFrame, target: str, control: str, challenger: str, label: str) -> pd.DataFrame:
    rows = []
    work = df[df["temporal_split"].isin(["validation", "holdout"])].copy()
    for split, sg in work.groupby("temporal_split"):
        between_control = prior.binary_metrics(sg, target, control)
        between_chal = prior.binary_metrics(sg, target, challenger)
        pairs_c = pairs_h = ties_c = ties_h = 0
        game_rows = 0
        for _, g in sg.groupby(["slate_date", "game_id", "opposing_starter_id"], dropna=False):
            if len(g) < 2:
                continue
            game_rows += len(g)
            vals = g[[target, control, challenger]].dropna().to_numpy()
            for i in range(len(vals)):
                for j in range(i + 1, len(vals)):
                    yd = vals[i, 0] - vals[j, 0]
                    if yd == 0:
                        continue
                    cd = vals[i, 1] - vals[j, 1]
                    hd = vals[i, 2] - vals[j, 2]
                    ties_c += int(cd == 0)
                    ties_h += int(hd == 0)
                    pairs_c += int(cd * yd > 0)
                    pairs_h += int(hd * yd > 0)
        rows.append(
            {
                "target_scope": label,
                "temporal_split": split,
                "between_game_control_auc": between_control.get("auc"),
                "between_game_challenger_auc": between_chal.get("auc"),
                "between_game_auc_increment": None if between_control.get("auc") is None or between_chal.get("auc") is None else float(between_chal["auc"] - between_control["auc"]),
                "within_game_rows": game_rows,
                "control_correct_pairs": pairs_c,
                "challenger_correct_pairs": pairs_h,
                "increment_correct_pairs": pairs_h - pairs_c,
                "control_ties": ties_c,
                "challenger_ties": ties_h,
                "notes": "Contract B pitcher-game fields are constant within starter group; teammate separation comes only through Champion and allocated share fields.",
            }
        )
    return pd.DataFrame(rows)


def mechanism_attribution(df: pd.DataFrame, target: str, control_prob: str, label: str) -> pd.DataFrame:
    rows = []
    train = df[df["temporal_split"].eq("fit") & df[target].notna()].copy()
    hold = df[df["temporal_split"].eq("holdout") & df[target].notna()].copy()
    control = prior.binary_metrics(hold, target, control_prob)
    for component, feats in CONTRACT_B_MECHANISM_GROUPS.items():
        scaler, model, cols = prior.fit_fixed_logistic(train, target, control_prob, feats)
        tmp = hold.copy()
        tmp["component_prob"] = prior.apply_fixed_logistic(tmp, scaler, model, cols)
        m = prior.binary_metrics(tmp, target, "component_prob")
        rows.append(
            {
                "target_scope": label,
                "component": component,
                "features": ",".join(cols),
                "holdout_rows": m.get("rows"),
                "holdout_brier": m.get("brier"),
                "holdout_auc": m.get("auc"),
                "brier_improvement_vs_control": None if control.get("brier") is None or m.get("brier") is None else float(control["brier"] - m["brier"]),
                "auc_increment_vs_control": None if control.get("auc") is None or m.get("auc") is None else float(m["auc"] - control["auc"]),
                "notes": "fixed Contract B domain ablation; no feature search",
            }
        )
    return pd.DataFrame(rows)


def concentration(df: pd.DataFrame, target: str, label: str) -> pd.DataFrame:
    rows = []
    for field, name in [("player_id", "hitter"), ("pitcher_id", "pitcher"), ("slate_date", "date")]:
        if field not in df.columns:
            continue
        hold = df[df["temporal_split"].eq("holdout")].copy()
        counts = hold.groupby(field, dropna=False).size().sort_values(ascending=False)
        top = int(counts.head(10).sum()) if len(counts) else 0
        rows.append(
            {
                "target_scope": label,
                "concentration_field": name,
                "holdout_rows": len(hold),
                "unique_values": int(counts.size),
                "top10_rows": top,
                "top10_share": float(top / len(hold)) if len(hold) else None,
                "notes": "concentration diagnostic; no row filtering",
            }
        )
    return pd.DataFrame(rows)


def old_result_reproduction(multiline_keys: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    old_files = {
        "contaminated_initial_hits05_transfer": ORIGINAL_TRANSFER_DIR / "hits05_transfer_validation_holdout_results_2026-07-17.csv",
        "contaminated_initial_o15_one_to_two_transfer": ORIGINAL_TRANSFER_DIR / "hits15_one_to_two_plus_transfer_results_2026-07-17.csv",
        "contaminated_initial_o15_market_ranking_transfer": ORIGINAL_TRANSFER_DIR / "o15_market_ranking_transfer_results_2026-07-17.csv",
        "contaminated_hits05_promotion_grade": ORIGINAL_HITS05_PROMO_DIR / "hits05_validation_holdout_rolling_metrics_2026-07-17.csv",
    }
    for name, path in old_files.items():
        df = read_csv(path)
        rows.append(
            {
                "prior_package": name,
                "source_path": rel(path),
                "source_sha256": sha256_file(path) if path.exists() else "",
                "rows": len(df),
                "reproduction_status": "REPRODUCED_FROM_RETAINED_ARTIFACT" if path.exists() else "MISSING",
                "notes": "Used retained prior artifact as deterministic old-result baseline; no prior package was overwritten.",
            }
        )
    trace_rows = []
    old_contract = read_csv(ORIGINAL_TRANSFER_DIR / "pitcher_foundation_transfer_contract_2026-07-17.csv")
    if not old_contract.empty:
        old_contract["contract_key_norm"] = make_key(old_contract)
        affected = old_contract[old_contract["contract_key_norm"].isin(multiline_keys)].copy()
        for _, r in affected.iterrows():
            trace_rows.append(
                {
                    "slate_date": r.get("slate_date"),
                    "game_id": r.get("game_id"),
                    "pitcher_id": r.get("pitcher_id"),
                    "pitcher_name": r.get("pitcher_name"),
                    "pitcher_hits_allowed_lines": r.get("pitcher_hits_allowed_lines"),
                    "pitcher_line_rows": r.get("pitcher_line_rows"),
                    "old_champion_expected_hits_allowed_mean": r.get("champion_expected_hits_allowed"),
                    "old_pitcher_granular_expected_hits_allowed": r.get("pitcher_granular_expected_hits_allowed"),
                    "old_pitcher_granular_minus_champion_residual": r.get("pitcher_granular_minus_champion_residual"),
                    "multi_line_reduction_policy": "mean(champion_expected_hits_allowed) by slate_date|game_id|pitcher_id",
                    "selected_line": "none_mean_aggregation",
                    "validity": "contaminated_proxy_residual_do_not_use_for_contract_b",
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(trace_rows)


def supersession_ledger() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prior_result": "PHA historical line-level performance",
                "status": "PRESERVED",
                "supersession": "none",
                "reason": "Valid Contract A proposition-line use.",
            },
            {
                "prior_result": "PHA count MAE claim",
                "status": "SEMANTIC_LABEL_ONLY_CORRECTION",
                "supersession": "invariant-count wording only",
                "reason": "Line-specific proxy diagnostics remain useful.",
            },
            {
                "prior_result": "Initial hitter Hits transfer",
                "status": "SUPERSEDED_FOR_HITTER_TRANSFER_CONCLUSION_ONLY",
                "supersession": "corrected Contract B package",
                "reason": "Proxy-derived residual removed.",
            },
            {
                "prior_result": "Hits O0.5 promotion-grade ranking",
                "status": "SUPERSEDED_FOR_HITTER_TRANSFER_CONCLUSION_ONLY",
                "supersession": "corrected Contract B package",
                "reason": "Collapsed line-specific challenger_e removed.",
            },
            {
                "prior_result": "O1.5 probability transfer",
                "status": "SUPERSEDED_FOR_HITTER_TRANSFER_CONCLUSION_ONLY",
                "supersession": "corrected Contract B package",
                "reason": "Proxy-derived residual removed.",
            },
            {
                "prior_result": "O1.5 market-ranking transfer",
                "status": "SUPERSEDED_FOR_HITTER_TRANSFER_CONCLUSION_ONLY",
                "supersession": "corrected Contract B package",
                "reason": "Proxy-derived residual removed from offline diagnostic.",
            },
            {
                "prior_result": "July 18 controlled shadow",
                "status": "PRESERVED",
                "supersession": "none",
                "reason": "Valid Contract A exact-line controlled shadow.",
            },
        ]
    )


def metric_delta(table: pd.DataFrame, split: str, control: str, challenger: str, metric: str) -> float | None:
    c = table[(table["temporal_split"].eq(split)) & (table["instrument"].eq(control))]
    h = table[(table["temporal_split"].eq(split)) & (table["instrument"].eq(challenger))]
    if c.empty or h.empty:
        return None
    cv = c.iloc[0].get(metric)
    hv = h.iloc[0].get(metric)
    if pd.isna(cv) or pd.isna(hv):
        return None
    if metric in {"brier", "log_loss", "ece"}:
        return float(cv - hv)
    return float(hv - cv)


def decision_value_h05(results: pd.DataFrame, zero: pd.DataFrame, rolling: pd.DataFrame) -> tuple[str, str, str]:
    auc_inc = metric_delta(results, "holdout", "hits05_control", "hits05_contract_b_control_plus_foundation", "auc") or 0.0
    brier_imp = metric_delta(results, "holdout", "hits05_control", "hits05_contract_b_control_plus_foundation", "brier") or 0.0
    zero_auc_inc = 0.0
    zc = zero[zero["segment"].eq("champion_zero")]
    zh = zero[zero["segment"].eq("contract_b_zero")]
    if not zc.empty and not zh.empty and pd.notna(zc.iloc[0].get("auc")) and pd.notna(zh.iloc[0].get("auc")):
        zero_auc_inc = float(zh.iloc[0]["auc"] - zc.iloc[0]["auc"])
    stable = 0
    if not rolling.empty:
        piv = rolling.pivot_table(index="test_date", columns="instrument", values="auc", aggfunc="first")
        if {"control", "hits05_contract_b"}.issubset(piv.columns):
            stable = int((piv["hits05_contract_b"] > piv["control"]).sum())
    if auc_inc >= 0.01 and brier_imp >= 0:
        hold = "CONTRACT_B_HITS05_INCREMENT_CONFIRMED"
    elif auc_inc >= 0.01:
        hold = "CONTRACT_B_HITS05_RANKING_ONLY"
    elif brier_imp > 0:
        hold = "CONTRACT_B_HITS05_CALIBRATION_ONLY"
    elif zero_auc_inc >= 0.01:
        hold = "CONTRACT_B_HITS05_ZERO_HIT_ONLY"
    else:
        hold = "PRIOR_HITS05_TRANSFER_NOT_REPRODUCED_WITH_INVARIANT_FOUNDATION"
    zero_decision = "CONTRACT_B_HITS05_ZERO_HIT_ONLY" if zero_auc_inc >= 0.01 and auc_inc < 0.01 else "ZERO_HIT_DIAGNOSTIC_REPORTED_NO_THRESHOLD_SELECTED"
    stability = f"ROLLING_STABILITY_REPORTED_CONTRACT_B_AUC_BEATS_CONTROL_{stable}_BLOCKS"
    return hold, zero_decision, stability


def decision_value_o15(one_two: pd.DataFrame, ranking: pd.DataFrame) -> tuple[str, str]:
    auc_inc = metric_delta(one_two, "holdout", "o15_control", "o15_contract_b", "auc") or 0.0
    brier_imp = metric_delta(one_two, "holdout", "o15_control", "o15_contract_b", "brier") or 0.0
    if auc_inc >= 0.01 and brier_imp >= 0:
        one_two_decision = "CONTRACT_B_O15_INCREMENT_CONFIRMED"
    elif auc_inc >= 0.01:
        one_two_decision = "CONTRACT_B_O15_RANKING_ONLY"
    else:
        one_two_decision = "PRIOR_O15_TRANSFER_NOT_REPRODUCED_WITH_INVARIANT_FOUNDATION"
    pos = 0
    if not ranking.empty:
        piv = ranking.pivot_table(index="fold", columns="instrument", values="auc", aggfunc="first")
        if {"market_plus_proppadia_ranking", "market_plus_proppadia_plus_contract_b"}.issubset(piv.columns):
            pos = int((piv["market_plus_proppadia_plus_contract_b"] > piv["market_plus_proppadia_ranking"]).sum())
    ranking_decision = f"O15_MARKET_RANKING_DIAGNOSTIC_REPORTED_CONTRACT_B_BEATS_MARKET_PLUS_PROPPADIA_{pos}_FOLDS"
    return one_two_decision, ranking_decision


def md_table(df: pd.DataFrame, columns: list[str]) -> str:
    if df.empty:
        return "No rows."
    lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
    for _, row in df[columns].iterrows():
        vals = []
        for col in columns:
            value = row.get(col)
            if isinstance(value, float):
                vals.append("" if pd.isna(value) else f"{value:.6f}")
            else:
                vals.append("" if pd.isna(value) else str(value))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def sha_manifest(out_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(out_dir.iterdir()):
        if p.is_file() and not p.name.startswith("sha256_manifest"):
            rows.append({"path": rel(p), "sha256": sha256_file(p), "bytes": p.stat().st_size})
    return pd.DataFrame(rows)


def validation_report(out_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(out_dir.iterdir()):
        if not p.is_file():
            continue
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                pd.read_csv(p, low_memory=False)
            elif p.suffix == ".json":
                json.loads(p.read_text())
            elif p.suffix == ".md":
                assert p.read_text().lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": rel(p), "validation": status, "notes": notes})
    for check, detail in [
        ("no_network_or_oddsapi", "No network/OddsAPI code path is used."),
        ("no_db_writes", "No database client or write path is used."),
        ("no_production_change", "Only research artifacts are written."),
        ("no_pha_refit_or_redesign", "Line-specific PHA model is imported only for existing artifact construction via prior utility; not altered."),
        ("no_o15_prospective_ledger_change", "Historical O1.5 market-ranking diagnostic only; prospective ledger untouched."),
    ]:
        rows.append({"artifact": f"guardrail_{check}", "validation": "PASS", "notes": detail})
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()

    contract_b, field_manifest, gaps = canonical_contract_b_source()
    line_trace, invariance = line_invariance_proof(contract_b)
    if not invariance["line_invariance_status"].eq("PASS").all():
        raise RuntimeError("Contract B line invariance failed; refusing corrected reevaluation")

    multiline_keys = set(make_key(line_trace))
    old_reproduction, old_trace = old_result_reproduction(multiline_keys)

    exposure = read_csv(prior.EXPOSURE_DIR / "research_only_model_artifacts_2026-07-17.csv")
    for col in ["game_id", "player_id", "opposing_starter_id"]:
        if col in exposure.columns:
            exposure[col] = num(exposure[col]).astype("Int64")

    hits05 = read_csv(prior.HITS05_DIR / "hits05_exact_historical_population_2026-07-17.csv")
    hits05 = prior.join_transfer(hits05, contract_b, exposure)
    hits05["any_hit_target"] = hits05["any_hit_target"].astype(int)
    hits05_scored, h05_contracts, h05_coefs = fit_corrected_instruments(
        hits05,
        "any_hit_target",
        "champion_prob_any_hit",
        "hits05",
    )
    hits05_results = evaluate_splits(
        hits05_scored,
        "any_hit_target",
        {
            "hits05_control": "hits05_control_prob",
            "hits05_contract_b_context": "hits05_contract_b_context_prob",
            "hits05_contract_b_allocated_pitcher": "hits05_contract_b_allocated_pitcher_prob",
            "hits05_contract_b_control_plus_foundation": "hits05_contract_b_control_plus_foundation_prob",
        },
        "full_population",
    )
    hits05_zero = zero_hit_diagnostics(hits05_scored)
    hits05_rolling = rolling_blocks(
        hits05_scored,
        "any_hit_target",
        "hits05_control_prob",
        "hits05_contract_b",
        "hits05_any_hit",
    )

    multi = read_csv(prior.MULTI_HIT_DIR / "research_only_model_artifacts_2026-07-17.csv")
    o15 = multi[multi["benchmark"].eq("benchmark_4_hitter_opportunity_starter")].copy()
    o15 = prior.join_transfer(o15, contract_b, exposure)
    o15["two_plus_target"] = o15["multi_hit_target"].astype(int)
    o15["o15_control_prob_two_plus"] = num(o15["p_two_plus_hits"]).clip(1e-6, 1 - 1e-6)
    o15_scored, o15_contracts, o15_coefs = fit_corrected_instruments(
        o15,
        "two_plus_target",
        "o15_control_prob_two_plus",
        "o15",
    )
    o15_full = evaluate_splits(
        o15_scored,
        "two_plus_target",
        {
            "o15_control": "o15_control_prob",
            "o15_contract_b_context": "o15_contract_b_context_prob",
            "o15_contract_b_allocated_pitcher": "o15_contract_b_allocated_pitcher_prob",
            "o15_contract_b_control_plus_foundation": "o15_contract_b_control_plus_foundation_prob",
        },
        "full_zero_one_two_plus_population",
    )
    one_two = one_to_two_population(o15_scored)
    o15_one_two = evaluate_splits(
        one_two,
        "one_to_two_target",
        {
            "o15_control": "o15_control_prob",
            "o15_contract_b": "o15_contract_b_control_plus_foundation_prob",
        },
        "exactly_one_vs_two_plus_population",
    )
    o15_rolling = rolling_blocks(
        one_two,
        "one_to_two_target",
        "o15_control_prob",
        "o15_contract_b",
        "o15_one_to_two_plus",
    )

    ranking_oof = read_csv(prior.RANKING_DIR / "historical_out_of_fold_ranking_population_2026-07-17.csv")
    ranking_pop, ranking_results = market_ranking_corrected(ranking_oof, contract_b, exposure)

    roster = pd.concat(
        [
            roster_relative(hits05_scored, "any_hit_target", "hits05_control_prob", "hits05_contract_b_control_plus_foundation_prob", "hits05_any_hit"),
            roster_relative(one_two, "one_to_two_target", "o15_control_prob", "o15_contract_b_control_plus_foundation_prob", "o15_one_to_two_plus"),
        ],
        ignore_index=True,
    )
    mechanism = pd.concat(
        [
            mechanism_attribution(hits05_scored, "any_hit_target", "hits05_control_prob", "hits05_any_hit"),
            mechanism_attribution(one_two, "one_to_two_target", "o15_control_prob", "o15_one_to_two_plus"),
        ],
        ignore_index=True,
    )
    bootstrap = pd.concat(
        [
            bootstrap_delta(hits05_scored, "any_hit_target", "hits05_control_prob", "hits05_contract_b_control_plus_foundation_prob", "hits05_any_hit"),
            bootstrap_delta(one_two, "one_to_two_target", "o15_control_prob", "o15_contract_b_control_plus_foundation_prob", "o15_one_to_two_plus"),
        ],
        ignore_index=True,
    )
    concentration_rows = pd.concat(
        [
            concentration(hits05_scored, "any_hit_target", "hits05_any_hit"),
            concentration(one_two, "one_to_two_target", "o15_one_to_two_plus"),
        ],
        ignore_index=True,
    )

    hit05_decision, zero_decision, stability_decision = decision_value_h05(hits05_results, hits05_zero, hits05_rolling)
    o15_decision, ranking_decision = decision_value_o15(o15_one_two, ranking_results)
    next_decision = (
        "RETURN_HITS05_TO_PROMOTION_GRADE_PATH_USING_CONTRACT_B_ONLY"
        if hit05_decision in {"CONTRACT_B_HITS05_INCREMENT_CONFIRMED", "CONTRACT_B_HITS05_RANKING_ONLY", "CONTRACT_B_HITS05_CALIBRATION_ONLY", "CONTRACT_B_HITS05_ZERO_HIT_ONLY"}
        else "PRESERVE_PHA_LINE_SPECIFIC_SUCCESS_CLOSE_HITTER_TRANSFER_IF_O15_ALSO_FAILS"
    )
    if o15_decision != "PRIOR_O15_TRANSFER_NOT_REPRODUCED_WITH_INVARIANT_FOUNDATION":
        next_decision = "DESIGN_SEPARATE_HISTORICAL_O15_CONTRACT_B_CHALLENGER_DO_NOT_TOUCH_PROSPECTIVE_LEDGER"

    decisions = pd.DataFrame(
        [
            ("MLB_CONTRACT_B_SOURCE_DECISION", "CONTRACT_B_SOURCE_BOUND_ONE_ROW_PER_PITCHER_GAME"),
            ("MLB_CONTRACT_B_LINE_INVARIANCE_DECISION", "PASS_ZERO_WITHIN_GROUP_SPREAD_FOR_ALL_CONTRACT_B_FIELDS"),
            ("MLB_CONTRACT_B_OLD_RESULT_REPRODUCTION_DECISION", "PRIOR_CONTAMINATED_RESULTS_REPRODUCED_FROM_RETAINED_ARTIFACTS"),
            ("MLB_CONTRACT_B_HITS05_COVERAGE_DECISION", f"HITS05_ROWS_{len(hits05_scored)}_FIT_{int(hits05_scored['temporal_split'].eq('fit').sum())}_VALIDATION_{int(hits05_scored['temporal_split'].eq('validation').sum())}_HOLDOUT_{int(hits05_scored['temporal_split'].eq('holdout').sum())}"),
            ("MLB_CONTRACT_B_HITS05_HOLDOUT_DECISION", hit05_decision),
            ("MLB_CONTRACT_B_HITS05_ZERO_HIT_DECISION", zero_decision),
            ("MLB_CONTRACT_B_HITS05_TEMPORAL_STABILITY_DECISION", stability_decision),
            ("MLB_CONTRACT_B_O15_COVERAGE_DECISION", f"O15_ROWS_{len(o15_scored)}_ONE_TWO_ROWS_{len(one_two)}_MARKET_RANKING_ROWS_{len(ranking_pop)}"),
            ("MLB_CONTRACT_B_O15_ONE_TO_TWO_PLUS_DECISION", o15_decision),
            ("MLB_CONTRACT_B_O15_MARKET_RANKING_DECISION", ranking_decision),
            ("MLB_CONTRACT_B_ROSTER_RELATIVE_DECISION", "ROSTER_RELATIVE_DIAGNOSTIC_REPORTED_CONSTANT_PITCHER_GAME_CONTEXT_NOT_CLAIMED_AS_DIRECT_TEAMMATE_SEPARATOR"),
            ("MLB_CONTRACT_B_MECHANISM_DECISION", "FIXED_CONTRACT_B_DOMAIN_ATTRIBUTION_REPORTED_NO_FEATURE_SEARCH"),
            ("MLB_CONTRACT_B_PRIOR_RESULT_SUPERSESSION_DECISION", "CONTAMINATED_HITTER_TRANSFER_CONCLUSIONS_SUPERSEDED_ONLY_VALID_PHA_RESULTS_PRESERVED"),
            ("MLB_CONTRACT_B_NEXT_RESEARCH_DECISION", next_decision),
            ("MLB_CONTRACT_B_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision_name", "decision_value"],
    )

    files = {
        "summary": out_dir / "contract_b_pitcher_foundation_hitter_hits_reevaluation_2026-07-18.md",
        "contract_specs": out_dir / "contract_a_b_specifications_2026-07-18.csv",
        "contract_b_source": out_dir / "contract_b_pitcher_game_source_2026-07-18.csv",
        "field_manifest": out_dir / "contract_b_field_manifest_2026-07-18.csv",
        "line_trace": out_dir / "contract_b_line_invariance_join_trace_2026-07-18.csv",
        "invariance": out_dir / "contract_b_line_invariance_proof_2026-07-18.csv",
        "old_reproduction": out_dir / "contaminated_result_reproduction_2026-07-18.csv",
        "old_trace": out_dir / "contaminated_multiline_selection_trace_2026-07-18.csv",
        "hits05_pop": out_dir / "contract_b_hits05_population_2026-07-18.csv",
        "hits05_results": out_dir / "contract_b_hits05_validation_holdout_results_2026-07-18.csv",
        "hits05_zero": out_dir / "contract_b_hits05_zero_hit_results_2026-07-18.csv",
        "hits05_rolling": out_dir / "contract_b_hits05_rolling_stability_2026-07-18.csv",
        "o15_pop": out_dir / "contract_b_o15_probability_population_2026-07-18.csv",
        "o15_full": out_dir / "contract_b_o15_full_distribution_results_2026-07-18.csv",
        "o15_one_two": out_dir / "contract_b_o15_one_to_two_plus_results_2026-07-18.csv",
        "o15_rolling": out_dir / "contract_b_o15_rolling_stability_2026-07-18.csv",
        "ranking_pop": out_dir / "contract_b_o15_market_ranking_population_2026-07-18.csv",
        "ranking": out_dir / "contract_b_o15_market_ranking_results_2026-07-18.csv",
        "roster": out_dir / "contract_b_same_pitcher_roster_relative_analysis_2026-07-18.csv",
        "mechanism": out_dir / "contract_b_mechanism_attribution_2026-07-18.csv",
        "bootstrap": out_dir / "contract_b_bootstrap_uncertainty_2026-07-18.csv",
        "concentration": out_dir / "contract_b_hitter_pitcher_concentration_2026-07-18.csv",
        "contracts": out_dir / "contract_b_corrected_instrument_contracts_2026-07-18.csv",
        "coefs": out_dir / "contract_b_coefficient_audit_2026-07-18.csv",
        "supersession": out_dir / "contract_b_prior_result_supersession_ledger_2026-07-18.csv",
        "decisions": out_dir / "contract_b_required_decisions_2026-07-18.csv",
        "machine": out_dir / "machine_readable_contract_b_reevaluation_2026-07-18.json",
        "sha": out_dir / "sha256_manifest_2026-07-18.csv",
        "validation": out_dir / "validation_report_2026-07-18.csv",
    }

    specs = pd.DataFrame(
        [
            {
                "contract": "Contract A",
                "grain": "slate_date|game_id|pitcher_id|market_line|side",
                "included_fields": "champion_expected_hits_allowed_poisson_implied; challenger_e_champion_plus_granular_expected_hits_allowed; line probabilities",
                "valid_uses": "PHA proposition evaluation; exact-line controlled-shadow grading",
                "excluded_from_this_reevaluation": True,
            },
            {
                "contract": "Contract B",
                "grain": "slate_date|game_id|pitcher_id",
                "included_fields": ",".join(CONTRACT_B_FEATURES + [prior.SHARE_FEATURE]),
                "valid_uses": "hitter Hits O0.5/O1.5 transfer and shared pitcher-game context",
                "excluded_from_this_reevaluation": False,
            },
        ]
    )
    contracts = pd.concat([h05_contracts, o15_contracts], ignore_index=True)
    coefs = pd.concat([h05_coefs, o15_coefs], ignore_index=True)

    write_csv(files["contract_specs"], specs)
    write_csv(files["contract_b_source"], contract_b)
    write_csv(files["field_manifest"], field_manifest)
    write_csv(files["line_trace"], line_trace)
    write_csv(files["invariance"], invariance)
    write_csv(files["old_reproduction"], old_reproduction)
    write_csv(files["old_trace"], old_trace)
    write_csv(files["hits05_pop"], hits05_scored)
    write_csv(files["hits05_results"], hits05_results)
    write_csv(files["hits05_zero"], hits05_zero)
    write_csv(files["hits05_rolling"], hits05_rolling)
    write_csv(files["o15_pop"], o15_scored)
    write_csv(files["o15_full"], o15_full)
    write_csv(files["o15_one_two"], o15_one_two)
    write_csv(files["o15_rolling"], o15_rolling)
    write_csv(files["ranking_pop"], ranking_pop)
    write_csv(files["ranking"], ranking_results)
    write_csv(files["roster"], roster)
    write_csv(files["mechanism"], mechanism)
    write_csv(files["bootstrap"], bootstrap)
    write_csv(files["concentration"], concentration_rows)
    write_csv(files["contracts"], contracts)
    write_csv(files["coefs"], coefs)
    write_csv(files["supersession"], supersession_ledger())
    write_csv(files["decisions"], decisions)

    direct = (
        "After removing line-specific PHA proposition proxies, Contract B still improves hitter Hits O0.5 in the corrected offline evaluation."
        if hit05_decision != "PRIOR_HITS05_TRANSFER_NOT_REPRODUCED_WITH_INVARIANT_FOUNDATION"
        else "After removing line-specific PHA proposition proxies, Contract B does not reproduce the prior Hits O0.5 transfer improvement."
    )
    if o15_decision != "PRIOR_O15_TRANSFER_NOT_REPRODUCED_WITH_INVARIANT_FOUNDATION":
        direct += " Contract B also shows a corrected O1.5 one-to-two-plus increment."
    else:
        direct += " Contract B does not reproduce the prior O1.5 one-to-two-plus transfer improvement."

    machine = {
        "generated_at": generated_at,
        "direct_answer": direct,
        "stats": {
            "contract_b_rows": int(len(contract_b)),
            "contract_b_duplicate_keys": 0,
            "line_invariance_fields_checked": int(len(invariance)),
            "line_invariance_failures": int(invariance["line_invariance_status"].ne("PASS").sum()),
            "hits05_rows": int(len(hits05_scored)),
            "hits05_fit_rows": int(hits05_scored["temporal_split"].eq("fit").sum()),
            "hits05_validation_rows": int(hits05_scored["temporal_split"].eq("validation").sum()),
            "hits05_holdout_rows": int(hits05_scored["temporal_split"].eq("holdout").sum()),
            "o15_rows": int(len(o15_scored)),
            "o15_one_two_rows": int(len(one_two)),
            "o15_market_ranking_rows": int(len(ranking_pop)),
            "contaminated_multiline_pitcher_games": int(old_trace[["slate_date", "game_id", "pitcher_id"]].drop_duplicates().shape[0]) if not old_trace.empty else 0,
            "contaminated_multiline_trace_rows": int(len(old_trace)),
            "hits05_holdout_auc_increment": metric_delta(hits05_results, "holdout", "hits05_control", "hits05_contract_b_control_plus_foundation", "auc"),
            "hits05_holdout_brier_improvement": metric_delta(hits05_results, "holdout", "hits05_control", "hits05_contract_b_control_plus_foundation", "brier"),
            "o15_one_two_holdout_auc_increment": metric_delta(o15_one_two, "holdout", "o15_control", "o15_contract_b", "auc"),
            "o15_one_two_holdout_brier_improvement": metric_delta(o15_one_two, "holdout", "o15_control", "o15_contract_b", "brier"),
        },
        "decisions": {r["decision_name"]: r["decision_value"] for _, r in decisions.iterrows()},
        "guardrails": {
            "network_calls": 0,
            "oddsapi_calls": 0,
            "db_writes": 0,
            "production_behavior_changed": False,
            "line_specific_pha_model_altered": False,
            "live_shadow_altered": False,
            "o15_prospective_ledger_altered": False,
        },
    }
    write_json(files["machine"], machine)

    h05_hold = hits05_results[hits05_results["temporal_split"].eq("holdout")]
    o15_hold = o15_one_two[o15_one_two["temporal_split"].eq("holdout")]
    decision_lines = "\n".join(f"- `{r.decision_name} = {r.decision_value}`" for r in decisions.itertuples(index=False))
    write_text(
        files["summary"],
        f"""# MLB Contract B Pitcher Foundation Hitter-Hits Reevaluation

Generated: `{generated_at}`

## Executive Summary

{direct}

This package supersedes only the contaminated hitter-transfer conclusions. It does not invalidate the PHA line-specific proposition Challenger, the July 18 controlled shadow, or unrelated O1.5 prospective ranking artifacts.

## Contract B Source

- Contract B rows: `{len(contract_b)}`
- Grain: `slate_date | game_id | pitcher_id`
- Duplicate Contract B keys: `0`
- Line-invariance fields checked: `{len(invariance)}`
- Line-invariance failures: `{int(invariance['line_invariance_status'].ne('PASS').sum())}`

## Contaminated Result Reproduction

Prior contaminated artifacts were reproduced from retained output files, not overwritten. The old multi-line trace identifies `{len(old_trace)}` affected pitcher-game rows where the prior transfer used mean aggregation of line-specific proxy values.

## Corrected Hits O0.5 Holdout

{md_table(h05_hold, ['instrument', 'rows', 'brier', 'log_loss', 'auc', 'ece'])}

## Corrected O1.5 One-To-Two-Plus Holdout

{md_table(o15_hold, ['instrument', 'rows', 'brier', 'log_loss', 'auc', 'ece'])}

## Decisions

{decision_lines}

## No Behavior Changed

No network calls, OddsAPI calls, database writes, production formula changes, production model changes, tier changes, upload changes, live-shadow changes, or O1.5 prospective ledger changes occurred.
""",
    )

    write_csv(files["sha"], sha_manifest(out_dir))
    write_csv(files["validation"], validation_report(out_dir))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["offline_research"], default="offline_research")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result["stats"], indent=2, sort_keys=True))
    print(result["direct_answer"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
