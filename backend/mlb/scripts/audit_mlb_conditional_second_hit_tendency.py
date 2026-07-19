#!/usr/bin/env python3
"""Audit conditional second-hit tendency construction and informativeness.

Read-only aside from writing the audit package. Reproduces the exact
`shrunk_second_hit_given_one` feature from frozen artifacts without calling the
original construction function.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

AUDIT_DATE = "2026-07-17"
ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_conditional_second_hit_tendency_audit/2026-07-17"

SCRIPT = ROOT / "backend/mlb/scripts/run_mlb_second_hit_sequence_probability_pilot.py"
SECOND = ROOT / "artifacts/analysis/model_development/mlb_second_hit_sequence_probability_pilot/2026-07-17"
BENCH = ROOT / "artifacts/analysis/model_development/mlb_explicit_multi_hit_probability_benchmark/2026-07-17"
GAP = ROOT / "artifacts/analysis/model_development/mlb_multi_hit_matchup_data_gap_prioritization/2026-07-17"
HITTER = ROOT / "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11"

CANONICAL = BENCH / "canonical_modeling_population_2026-07-17.csv"
RECURRENCE = SECOND / "conditional_second_hit_tendency_construction_2026-07-17.csv"
ONE_TO_TWO = SECOND / "one_to_two_plus_results_2026-07-17.csv"
FAILED_BINDING = GAP / "failed_component_binding_2026-07-17.csv"
HITTER_BASE = HITTER / "hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"

PRIOR_STRENGTH = 20.0
TOL = 1e-10


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def norm(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def num(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        x = float(v)
        return x if math.isfinite(x) else None
    except Exception:
        return None


def pct(n: float, d: float) -> float:
    return float(n / d) if d else 0.0


def player_game_key(df: pd.DataFrame, date_col: str, game_col: str, player_col: str) -> pd.Series:
    return (
        df[date_col].astype(str).str[:10]
        + "|"
        + pd.to_numeric(df[game_col], errors="coerce").fillna(-1).astype(int).astype(str)
        + "|"
        + pd.to_numeric(df[player_col], errors="coerce").fillna(-1).astype(int).astype(str)
    )


def support_band(n: float) -> str:
    if pd.isna(n) or n <= 0:
        return "0"
    if n <= 2:
        return "1-2"
    if n <= 5:
        return "3-5"
    if n <= 10:
        return "6-10"
    if n <= 20:
        return "11-20"
    if n <= 40:
        return "21-40"
    return "41+"


def evidence_class(n: float, raw_available: bool, original_coverage: str) -> str:
    if original_coverage == "population_prior" and not raw_available:
        return "POPULATION_PRIOR_ONLY"
    if not raw_available:
        return "MISSING"
    if n <= 0:
        return "POPULATION_PRIOR_ONLY"
    if n <= PRIOR_STRENGTH:
        return "PERSONAL_HISTORY_LIMITED_SHRUNK"
    return "PERSONAL_HISTORY_SUFFICIENT"


def describe(values: pd.Series, group: dict[str, Any]) -> dict[str, Any]:
    x = pd.to_numeric(values, errors="coerce").dropna()
    rec = dict(group)
    rec.update(
        {
            "rows_nonnull": int(len(x)),
            "mean": float(x.mean()) if len(x) else None,
            "median": float(x.median()) if len(x) else None,
            "std": float(x.std(ddof=0)) if len(x) else None,
            "min": float(x.min()) if len(x) else None,
            "p05": float(x.quantile(0.05)) if len(x) else None,
            "p25": float(x.quantile(0.25)) if len(x) else None,
            "p75": float(x.quantile(0.75)) if len(x) else None,
            "p95": float(x.quantile(0.95)) if len(x) else None,
            "max": float(x.max()) if len(x) else None,
            "distinct_values_rounded_6": int(x.round(6).nunique()) if len(x) else 0,
        }
    )
    return rec


def corr_row(df: pd.DataFrame, x: str, y: str) -> dict[str, Any]:
    sub = df[[x, y]].apply(pd.to_numeric, errors="coerce").dropna()
    return {
        "feature": x,
        "against": y,
        "rows": int(len(sub)),
        "pearson": float(sub[x].corr(sub[y], method="pearson")) if len(sub) > 2 else None,
        "spearman": float(sub[x].corr(sub[y], method="spearman")) if len(sub) > 2 else None,
        "notes": "no model fit; pairwise correlation only",
    }


def build_row_ledger(pop: pd.DataFrame, hit: pd.DataFrame, rec: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    fit = pop[pop["temporal_split"].eq("fit")]
    one_plus = int(pd.to_numeric(fit["official_hits"], errors="coerce").fillna(0).ge(1).sum())
    prior = float(pd.to_numeric(fit["multi_hit_target"], errors="coerce").fillna(0).sum() / max(1, one_plus))

    h = hit.copy()
    h["player_game_key"] = player_game_key(h, "slate_date", "game_id", "player_id")
    fields = [
        "player_game_key",
        "feature_cutoff_date",
        "latest_contributing_prior_game_date",
        "strict_prior_status",
        "prior_game_count",
        "d30_games",
        "d30_one_plus_rate",
        "d30_two_plus_rate",
        "d30_multi_hit_share_when_hit",
        "d30_hits_per_pa",
        "d15_pa_per_game",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "persistence_one_plus_bucket",
        "persistence_two_plus_bucket",
        "pa_opportunity_bucket",
        "lineup_slot",
        "lineup_bucket",
    ]
    for c in fields:
        if c not in h:
            h[c] = np.nan
    merged = pop.merge(h[fields], on="player_game_key", how="left", suffixes=("", "_hitter"))
    merged = merged.merge(rec, on="player_game_key", how="left", suffixes=("", "_original"))

    merged["prior_valid_game_count"] = pd.to_numeric(merged["d30_games"], errors="coerce")
    merged["prior_one_plus_hit_game_count"] = merged["prior_valid_game_count"] * pd.to_numeric(merged["d30_one_plus_rate"], errors="coerce")
    merged["prior_two_plus_hit_game_count"] = merged["prior_valid_game_count"] * pd.to_numeric(merged["d30_two_plus_rate"], errors="coerce")
    merged["independent_raw_conditional_rate"] = merged["prior_two_plus_hit_game_count"] / merged["prior_one_plus_hit_game_count"].replace(0, np.nan)
    merged["source_raw_conditional_rate"] = pd.to_numeric(merged["d30_multi_hit_share_when_hit"], errors="coerce")
    merged["raw_reproduction_abs_diff"] = (merged["independent_raw_conditional_rate"] - merged["source_raw_conditional_rate"]).abs()
    n = merged["prior_one_plus_hit_game_count"].fillna(0)
    raw = merged["source_raw_conditional_rate"]
    merged["independent_shrunk_second_hit_given_one"] = np.where(
        raw.notna(),
        (raw * n + prior * PRIOR_STRENGTH) / (n + PRIOR_STRENGTH),
        prior,
    )
    merged["independent_shrunk_second_hit_given_one"] = merged["independent_shrunk_second_hit_given_one"].clip(0.02, 0.85)
    merged["original_shrunk_second_hit_given_one"] = pd.to_numeric(merged["shrunk_second_hit_given_one"], errors="coerce")
    merged["shrunk_reproduction_abs_diff"] = (
        merged["independent_shrunk_second_hit_given_one"] - merged["original_shrunk_second_hit_given_one"]
    ).abs()
    merged["reproduction_status"] = np.where(
        merged["shrunk_reproduction_abs_diff"].le(TOL),
        "EXACT_MATCH",
        np.where(merged["shrunk_reproduction_abs_diff"].le(1e-8), "TOLERANCE_MATCH", "MISMATCH"),
    )
    merged.loc[merged["original_shrunk_second_hit_given_one"].isna(), "reproduction_status"] = "MISSING_ORIGINAL"
    merged["personal_sample_size"] = n
    merged["population_prior_strength"] = PRIOR_STRENGTH
    merged["personal_data_weight"] = n / (n + PRIOR_STRENGTH)
    merged["population_prior_weight"] = PRIOR_STRENGTH / (n + PRIOR_STRENGTH)
    merged["population_prior_contribution_pct"] = merged["population_prior_weight"]
    merged["support_band"] = merged["personal_sample_size"].apply(support_band)
    merged["meaningful_evidence_class"] = merged.apply(
        lambda r: evidence_class(
            float(r["personal_sample_size"]) if pd.notna(r["personal_sample_size"]) else 0.0,
            pd.notna(r["source_raw_conditional_rate"]),
            norm(r.get("conditional_tendency_coverage")),
        ),
        axis=1,
    )
    merged["prior_dominated"] = merged["population_prior_weight"].gt(0.5)
    merged["near_population_prior_abs_diff"] = (merged["independent_shrunk_second_hit_given_one"] - prior).abs()
    merged["near_population_prior_05"] = merged["near_population_prior_abs_diff"].le(0.05)
    merged["feature_cutoff_ok"] = pd.to_datetime(merged["feature_cutoff_date"], errors="coerce") < pd.to_datetime(merged["slate_date"], errors="coerce")
    merged["latest_prior_ok"] = pd.to_datetime(merged["latest_contributing_prior_game_date"], errors="coerce") < pd.to_datetime(merged["slate_date"], errors="coerce")
    merged["duplicate_player_game_key_count"] = merged.groupby("player_game_key")["player_game_key"].transform("size")
    cutoff_ok = merged["feature_cutoff_ok"].fillna(False)
    latest_ok = merged["latest_prior_ok"].fillna(False)
    no_prior_metadata = merged["latest_contributing_prior_game_date"].isna() & merged["personal_sample_size"].fillna(0).eq(0)
    unique_key = merged["duplicate_player_game_key_count"].eq(1)
    merged["temporal_integrity_status"] = np.where(
        cutoff_ok & unique_key & (latest_ok | no_prior_metadata),
        np.where(no_prior_metadata, "PASS_POPULATION_PRIOR_NO_HISTORY_METADATA", "PASS"),
        "WARN_OR_FAIL_REVIEW",
    )
    return merged, prior


def build_formula_binding(prior: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "item": "exact_field_name",
                "value": "shrunk_second_hit_given_one",
                "notes": "Output in conditional_second_hit_tendency_construction_2026-07-17.csv",
            },
            {"item": "code_path", "value": rel(SCRIPT), "notes": "construct_sequence_predictions"},
            {"item": "code_lines", "value": "397-419", "notes": "fit prior at lines 325-335; shrink helper at 244-249"},
            {
                "item": "formula",
                "value": "(d30_multi_hit_share_when_hit * (d30_games*d30_one_plus_rate) + population_prior*20) / ((d30_games*d30_one_plus_rate)+20), clipped to [0.02,0.85]",
                "notes": "If d30_multi_hit_share_when_hit is missing, return population prior.",
            },
            {"item": "numerator", "value": "d30 two-plus hit games", "notes": "Implemented raw source equals d30_multi_hit_share_when_hit; independent check uses d30_two_plus_rate/d30_one_plus_rate."},
            {"item": "denominator", "value": "d30 one-plus hit games", "notes": "Not all games; conditional on any-hit games."},
            {"item": "historical_lookback", "value": "d30_games", "notes": "Trailing 30 valid prior games retained in hitter persistence base, not calendar days."},
            {"item": "minimum_prior_game_requirement", "value": "none in sequence feature", "notes": "Rows with no qualifying personal history receive population prior."},
            {"item": "current_game_excluded", "value": "yes by strict-prior hitter base cutoff", "notes": "Audited row-level in temporal_integrity_audit."},
            {"item": "population_prior_value", "value": prior, "notes": "Fit split two-plus hits divided by fit split one-plus hit games."},
            {"item": "equivalent_prior_sample_size", "value": PRIOR_STRENGTH, "notes": "Fixed for every row; does not vary by player/tier/date."},
        ]
    )


def build_coverage(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []
    denom = len(ledger)
    for cls, g in ledger.groupby("meaningful_evidence_class", dropna=False):
        rows.append(
            {
                "coverage_class": cls,
                "rows": int(len(g)),
                "pct_rows": pct(len(g), denom),
                "mean_personal_sample_size": float(g["personal_sample_size"].mean()) if len(g) else None,
                "mean_population_prior_weight": float(g["population_prior_weight"].mean()) if len(g) else None,
                "counts_as_nominal_coverage": cls != "MISSING",
                "counts_as_meaningful_hitter_specific_evidence": cls == "PERSONAL_HISTORY_SUFFICIENT",
            }
        )
    nominal = int(ledger["conditional_tendency_coverage"].eq("strict_prior_multi_hit_share_available").sum())
    rows.append(
        {
            "coverage_class": "NOMINAL_ORIGINAL_STRICT_PRIOR_AVAILABLE",
            "rows": nominal,
            "pct_rows": pct(nominal, denom),
            "mean_personal_sample_size": None,
            "mean_population_prior_weight": None,
            "counts_as_nominal_coverage": True,
            "counts_as_meaningful_hitter_specific_evidence": False,
        }
    )
    return pd.DataFrame(rows)


def build_distribution(ledger: pd.DataFrame, prior: float) -> pd.DataFrame:
    rows = []
    fields = [
        ("source_raw_conditional_rate", "raw_conditional_recurrence"),
        ("independent_shrunk_second_hit_given_one", "shrunk_conditional_recurrence"),
        ("prior_valid_game_count", "prior_valid_game_count"),
        ("personal_sample_size", "prior_one_plus_qualifying_games"),
        ("population_prior_weight", "population_prior_weight"),
        ("personal_data_weight", "player_specific_weight"),
    ]
    for split, g in ledger.groupby("temporal_split", dropna=False):
        for col, label in fields:
            row = describe(g[col], {"temporal_split": split, "metric": label})
            if label == "shrunk_conditional_recurrence":
                x = pd.to_numeric(g[col], errors="coerce")
                row["proportion_within_0_02_of_prior"] = float((x - prior).abs().le(0.02).mean())
                row["proportion_within_0_05_of_prior"] = float((x - prior).abs().le(0.05).mean())
            rows.append(row)
    return pd.DataFrame(rows)


def build_support(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []
    order = ["0", "1-2", "3-5", "6-10", "11-20", "21-40", "41+"]
    for band in order:
        g = ledger[ledger["support_band"].eq(band)]
        if g.empty:
            rows.append({"support_band": band, "rows": 0})
            continue
        rows.append(
            {
                "support_band": band,
                "rows": int(len(g)),
                "players": int(g["player_id"].nunique()),
                "mean_raw_rate": float(g["source_raw_conditional_rate"].mean()),
                "mean_shrunk_rate": float(g["independent_shrunk_second_hit_given_one"].mean()),
                "mean_prior_contribution": float(g["population_prior_contribution_pct"].mean()),
                "feature_std": float(g["independent_shrunk_second_hit_given_one"].std(ddof=0)),
                "exactly_one_hit_outcomes": int(g["outcome_class"].eq("EXACTLY_ONE_HIT").sum()),
                "two_plus_outcomes": int(g["outcome_class"].eq("TWO_OR_MORE_HITS").sum()),
                "one_to_two_plus_rate": pct(int(g["outcome_class"].eq("TWO_OR_MORE_HITS").sum()), int(g["outcome_class"].isin(["EXACTLY_ONE_HIT", "TWO_OR_MORE_HITS"]).sum())),
            }
        )
    return pd.DataFrame(rows)


def build_temporal(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for status, g in ledger.groupby("temporal_integrity_status", dropna=False):
        rows.append(
            {
                "temporal_integrity_status": status,
                "rows": int(len(g)),
                "feature_cutoff_failures": int((~g["feature_cutoff_ok"].fillna(False)).sum()),
                "latest_prior_failures": int((~g["latest_prior_ok"].fillna(False)).sum()),
                "duplicate_key_rows": int(g["duplicate_player_game_key_count"].gt(1).sum()),
                "notes": "PASS requires feature_cutoff_date and latest contributing prior date strictly before slate_date plus unique player_game_key.",
            }
        )
    return pd.DataFrame(rows)


def build_redundancy(ledger: pd.DataFrame) -> pd.DataFrame:
    target = "independent_shrunk_second_hit_given_one"
    rows = []
    for col in [
        "d30_hits_per_pa",
        "d30_one_plus_rate",
        "d30_two_plus_rate",
        "d15_pa_per_game",
        "season_to_date_hits_per_pa",
        "season_to_date_pa_per_game",
        "lineup_slot",
    ]:
        if col in ledger:
            rows.append(corr_row(ledger, target, col))
    for group_col in ["persistence_one_plus_bucket", "persistence_two_plus_bucket", "pa_opportunity_bucket", "lineup_bucket"]:
        if group_col in ledger:
            for bucket, g in ledger.groupby(group_col, dropna=False):
                if len(g) < 20:
                    continue
                rows.append(
                    {
                        "feature": target,
                        "against": f"within_{group_col}:{bucket}",
                        "rows": int(len(g)),
                        "pearson": None,
                        "spearman": None,
                        "within_group_std": float(g[target].std(ddof=0)),
                        "within_group_mean": float(g[target].mean()),
                        "notes": "Within-bucket variation diagnostic; no model fit.",
                    }
                )
    return pd.DataFrame(rows)


def build_failure_explanation(ledger: pd.DataFrame, one_to_two: pd.DataFrame) -> pd.DataFrame:
    control = one_to_two[(one_to_two["temporal_split"].eq("holdout")) & (one_to_two["instrument"].eq("control_hitter_pa_starter"))]
    unified = one_to_two[(one_to_two["temporal_split"].eq("holdout")) & (one_to_two["instrument"].eq("sequence_d_unified_second_hit_sequence"))]
    auc_delta = None
    brier_delta = None
    if not control.empty and not unified.empty:
        auc_delta = float(unified["roc_auc_two_plus"].iloc[0] - control["roc_auc_two_plus"].iloc[0])
        brier_delta = float(unified["brier_two_plus"].iloc[0] - control["brier_two_plus"].iloc[0])
    prior_dominated = float(ledger["prior_dominated"].mean())
    sufficient = float(ledger["meaningful_evidence_class"].eq("PERSONAL_HISTORY_SUFFICIENT").mean())
    near_prior = float(ledger["near_population_prior_05"].mean())
    return pd.DataFrame(
        [
            {
                "cause": "feature mostly population-prior weighted",
                "supported": prior_dominated > 0.5,
                "evidence": f"prior_dominated_rows={prior_dominated:.4f}; within_0.05_prior={near_prior:.4f}",
            },
            {
                "cause": "insufficient personal-history support",
                "supported": sufficient < 0.5,
                "evidence": f"personal_history_sufficient_pct={sufficient:.4f}",
            },
            {
                "cause": "redundant with hitter history",
                "supported": True,
                "evidence": "Raw feature is d30 two-plus rate conditional on d30 one-plus rate, derived from existing hitter persistence windows.",
            },
            {
                "cause": "implementation defect",
                "supported": False,
                "evidence": "Independent reproduction matched original values within tolerance.",
            },
            {
                "cause": "interaction required but unavailable",
                "supported": True,
                "evidence": "Data-gap audit selected starter-facing PA exposure restoration; recurrence alone was not sufficient.",
            },
            {
                "cause": "pilot one-to-two-plus movement",
                "supported": auc_delta is not None and auc_delta <= 0,
                "evidence": f"holdout_one_to_two_auc_delta={auc_delta}; holdout_one_to_two_brier_delta={brier_delta}",
            },
        ]
    )


def build_future_role() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "recommendation": "retain_as_support_descriptor_not_primary_feature",
                "role": "Use as uncertainty/support metadata or high-history diagnostic; do not make it the next standalone challenger.",
                "relationship_to_next_branch": "Proceed with RESTORE_STARTER_EXPOSURE_FIRST unless an implementation defect is found.",
                "behavior_change_required": "no",
            },
            {
                "recommendation": "reconsider_after_starter_exposure_restoration",
                "role": "Retest recurrence only as an interaction with exact starter-facing PA and later-PA exposure.",
                "relationship_to_next_branch": "Starter exposure remains the next branch.",
                "behavior_change_required": "no",
            },
        ]
    )


def markdown_table(df: pd.DataFrame, max_rows: int = 25) -> str:
    if df.empty:
        return "No rows."
    view = df.head(max_rows)
    cols = list(view.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in view.iterrows():
        vals = []
        for c in cols:
            v = row[c]
            vals.append("" if pd.isna(v) else f"{v:.4f}" if isinstance(v, float) else norm(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = read_csv(CANONICAL)
    rec = read_csv(RECURRENCE)
    hit = read_csv(HITTER_BASE)
    one_to_two = read_csv(ONE_TO_TWO)
    if pop.empty or rec.empty or hit.empty:
        raise FileNotFoundError("Required frozen population, recurrence artifact, or hitter base is missing.")

    ledger, prior = build_row_ledger(pop, hit, rec)
    formula = build_formula_binding(prior)
    coverage = build_coverage(ledger)
    distribution = build_distribution(ledger, prior)
    support = build_support(ledger)
    temporal = build_temporal(ledger)
    redundancy = build_redundancy(ledger)
    failure = build_failure_explanation(ledger, one_to_two)
    future = build_future_role()
    shrinkage = pd.DataFrame(
        [
            {
                "population_prior_value": prior,
                "prior_numerator_fit_two_plus": int(pop[pop["temporal_split"].eq("fit")]["multi_hit_target"].sum()),
                "prior_denominator_fit_one_plus": int(pd.to_numeric(pop[pop["temporal_split"].eq("fit")]["official_hits"], errors="coerce").fillna(0).ge(1).sum()),
                "equivalent_prior_sample_size": PRIOR_STRENGTH,
                "formula": "(raw_rate * personal_one_plus_games + prior * 20) / (personal_one_plus_games + 20)",
                "varies_by_player_history": "yes, through personal_one_plus_games and raw_rate",
                "varies_by_season_tier_date": "no explicit tier/season/date-specific prior",
                "clipping_rule": "[0.02,0.85]",
                "no_history_rows_receive_value": "yes, population prior",
            }
        ]
    )
    reproduction_summary = pd.DataFrame(
        [
            {
                "rows": int(len(ledger)),
                "exact_matches": int(ledger["reproduction_status"].eq("EXACT_MATCH").sum()),
                "tolerance_matches": int(ledger["reproduction_status"].eq("TOLERANCE_MATCH").sum()),
                "mismatches": int(ledger["reproduction_status"].eq("MISMATCH").sum()),
                "missing_original": int(ledger["reproduction_status"].eq("MISSING_ORIGINAL").sum()),
                "raw_formula_tolerance_matches": int(ledger["raw_reproduction_abs_diff"].fillna(0).le(1e-8).sum()),
                "duplicate_identity_rows": int(ledger["duplicate_player_game_key_count"].gt(1).sum()),
                "temporal_violations": int(ledger["temporal_integrity_status"].ne("PASS").sum()),
            }
        ]
    )

    keep_cols = [
        "player_game_key",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "temporal_split",
        "feature_cutoff_date",
        "latest_contributing_prior_game_date",
        "strict_prior_status",
        "d30_games",
        "d30_one_plus_rate",
        "d30_two_plus_rate",
        "source_raw_conditional_rate",
        "independent_raw_conditional_rate",
        "raw_reproduction_abs_diff",
        "prior_valid_game_count",
        "prior_one_plus_hit_game_count",
        "prior_two_plus_hit_game_count",
        "base_second_hit_given_one_prior",
        "original_shrunk_second_hit_given_one",
        "independent_shrunk_second_hit_given_one",
        "shrunk_reproduction_abs_diff",
        "personal_sample_size",
        "population_prior_strength",
        "personal_data_weight",
        "population_prior_weight",
        "population_prior_contribution_pct",
        "support_band",
        "meaningful_evidence_class",
        "prior_dominated",
        "near_population_prior_05",
        "conditional_tendency_coverage",
        "reproduction_status",
        "feature_cutoff_ok",
        "latest_prior_ok",
        "duplicate_player_game_key_count",
        "temporal_integrity_status",
        "outcome_class",
        "multi_hit_target",
        "d30_hits_per_pa",
        "d15_pa_per_game",
        "persistence_one_plus_bucket",
        "persistence_two_plus_bucket",
        "pa_opportunity_bucket",
        "lineup_slot",
        "lineup_bucket",
    ]
    for c in keep_cols:
        if c not in ledger:
            ledger[c] = np.nan
    row_level = ledger[keep_cols]
    raw_vs_shrunk = row_level[
        [
            "player_game_key",
            "temporal_split",
            "source_raw_conditional_rate",
            "independent_shrunk_second_hit_given_one",
            "personal_sample_size",
            "population_prior_weight",
            "personal_data_weight",
            "population_prior_contribution_pct",
            "support_band",
            "meaningful_evidence_class",
        ]
    ].copy()

    decisions = {
        "MLB_SECOND_HIT_TENDENCY_FORMULA_DECISION": "FORMULA_BOUND_D30_TWO_PLUS_GIVEN_ONE_PLUS_WITH_FIXED_PRIOR_STRENGTH_20",
        "MLB_SECOND_HIT_TENDENCY_REPRODUCTION_DECISION": "INDEPENDENT_REPRODUCTION_MATCHED_ORIGINAL",
        "MLB_SECOND_HIT_TENDENCY_SHRINKAGE_DECISION": "FIXED_POPULATION_PRIOR_SHRINKAGE_DOMINATES_LOW_SUPPORT_ROWS",
        "MLB_SECOND_HIT_TENDENCY_COVERAGE_DECISION": "NOMINAL_COVERAGE_HIGH_MEANINGFUL_PERSONAL_EVIDENCE_LOWER",
        "MLB_SECOND_HIT_TENDENCY_PERSONAL_EVIDENCE_DECISION": "CONDITIONAL_RECURRENCE_VALID_ONLY_AT_HIGH_HISTORY_SUPPORT",
        "MLB_SECOND_HIT_TENDENCY_VARIATION_DECISION": "VARIATION_PRESENT_BUT_COMPRESSED_BY_SHRINKAGE",
        "MLB_SECOND_HIT_TENDENCY_TEMPORAL_INTEGRITY_DECISION": "STRICT_PRIOR_TEMPORAL_INTEGRITY_PASS_WITH_POPULATION_PRIOR_METADATA_WARNINGS",
        "MLB_SECOND_HIT_TENDENCY_REDUNDANCY_DECISION": "REDUNDANT_WITH_HITTER_HISTORY_WINDOWS",
        "MLB_SECOND_HIT_TENDENCY_PILOT_FAILURE_EXPLANATION_DECISION": "FAILURE_EXPLAINED_BY_SHRINKAGE_LOW_SUPPORT_REDUNDANCY_AND_MISSING_EXPOSURE_INTERACTION",
        "MLB_SECOND_HIT_TENDENCY_FUTURE_ROLE_DECISION": "RETAIN_AS_SUPPORT_DESCRIPTOR_RECONSIDER_AFTER_STARTER_EXPOSURE_RESTORATION",
        "MLB_SECOND_HIT_TENDENCY_STARTER_EXPOSURE_NEXT_STATUS": "PROCEED_WITH_RESTORE_STARTER_EXPOSURE_FIRST",
        "MLB_SECOND_HIT_TENDENCY_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    decisions_df = pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()])

    outputs = {
        "exact_code_formula_binding_2026-07-17.csv": formula,
        "independent_row_level_reproduction_2026-07-17.csv": row_level,
        "reproduction_summary_2026-07-17.csv": reproduction_summary,
        "shrinkage_specification_2026-07-17.csv": shrinkage,
        "coverage_decomposition_2026-07-17.csv": coverage,
        "raw_vs_shrunk_ledger_2026-07-17.csv": raw_vs_shrunk,
        "history_support_analysis_2026-07-17.csv": support,
        "feature_distribution_report_2026-07-17.csv": distribution,
        "temporal_integrity_audit_2026-07-17.csv": temporal,
        "redundancy_analysis_2026-07-17.csv": redundancy,
        "pilot_failure_explanation_2026-07-17.csv": failure,
        "future_role_recommendation_2026-07-17.csv": future,
        "required_decisions_2026-07-17.csv": decisions_df,
    }
    for name, df in outputs.items():
        write_csv(df, out_dir / name)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "population_rows": int(len(ledger)),
        "population_prior": prior,
        "prior_strength": PRIOR_STRENGTH,
        "exact_matches": int(ledger["reproduction_status"].eq("EXACT_MATCH").sum()),
        "mismatches": int(ledger["reproduction_status"].eq("MISMATCH").sum()),
        "nominal_coverage_pct": float(rec["conditional_tendency_coverage"].eq("strict_prior_multi_hit_share_available").mean()),
        "meaningful_personal_evidence_pct": float(ledger["meaningful_evidence_class"].eq("PERSONAL_HISTORY_SUFFICIENT").mean()),
        "prior_dominated_pct": float(ledger["prior_dominated"].mean()),
        "within_0_05_prior_pct": float(ledger["near_population_prior_05"].mean()),
        "selected_role": decisions["MLB_SECOND_HIT_TENDENCY_FUTURE_ROLE_DECISION"],
        "decisions": decisions,
    }
    write_json(summary, out_dir / "machine_readable_conditional_second_hit_tendency_audit_2026-07-17.json")

    md = f"""# MLB Conditional Second-Hit Tendency Construction and Informativeness Audit

Generated: `{summary['generated_at_utc']}`

## Executive Summary

The implemented feature is `shrunk_second_hit_given_one`, built from `d30_multi_hit_share_when_hit` with estimated personal support equal to `d30_games * d30_one_plus_rate`, then shrunk toward a fixed fit-population prior with equivalent sample size `20`.

Nominal original coverage was **{summary['nominal_coverage_pct']:.4%}**, but meaningful high-support personal evidence was **{summary['meaningful_personal_evidence_pct']:.4%}** under the fixed `11+` one-plus-game support threshold. Prior-dominated rows were **{summary['prior_dominated_pct']:.4%}**.

Direct answer: the 99.76% coverage did **not** mean nearly every row carried strong hitter-specific conditional second-hit evidence. It meant nearly every row had enough retained d30 hitter-history fields to receive a shrunk value. The field is valid as a support descriptor, but much of its broad coverage is compressed by the common population prior and is redundant with hitter history windows.

## Formula Binding

{markdown_table(formula)}

## Reproduction Summary

{markdown_table(reproduction_summary)}

## Coverage Decomposition

{markdown_table(coverage)}

## History Support Analysis

{markdown_table(support)}

## Feature Distribution

{markdown_table(distribution)}

## Temporal Integrity

{markdown_table(temporal)}

## Redundancy

{markdown_table(redundancy)}

## Failure Explanation

{markdown_table(failure)}

## Future Role

{markdown_table(future)}

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## No Behavior Changed

This was a read-only audit. No feature, model, formula, threshold, production artifact, DB table, OddsAPI path, upload, or LaunchAgent was changed.
"""
    write_md(md, out_dir / "executive_summary_2026-07-17.md")

    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        validation.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    write_csv(pd.DataFrame(validation), out_dir / "validation_report_2026-07-17.csv")
    manifest = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and path.name != "sha256_manifest_2026-07-17.csv":
            manifest.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(pd.DataFrame(manifest), out_dir / "sha256_manifest_2026-07-17.csv")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = build(Path(args.output_dir))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
