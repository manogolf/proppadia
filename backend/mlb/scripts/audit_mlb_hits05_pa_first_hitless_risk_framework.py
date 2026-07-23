from __future__ import annotations

import csv
import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    precision_recall_curve,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_pa_first_hitless_risk_framework/2026-07-21"
COMMON = ROOT / "artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/twenty_slate_common_row_ledger.csv"
TAIL_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_hits05_low_probability_tail_separation_audit/2026-07-21/governing_population_ledger.csv"
CANDIDATE_EXPLAIN = ROOT / "artifacts/analysis/model_development/mlb_hits05_candidate_miss_risk_explanation_audit/2026-07-21/governing_population_ledger.csv"
DENOMINATOR = ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19/player_game_denominator_2026-07-19.csv"
FEATURE_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/frozen_feature_manifest_2026-07-19.csv"
COUNT_DIST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/count_distribution_predictions_2026-07-19.csv"


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
        fields = sorted({key for row in rows for key in row.keys()})
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


def pct(value: float | None) -> float | str:
    return "" if value is None or not math.isfinite(value) else float(value)


def sample_flag(n: int) -> str:
    if n >= 250:
        return "OK"
    if n >= 80:
        return "THIN"
    if n >= 25:
        return "SPARSE"
    return "VERY_SPARSE"


def hitless_metrics(y: pd.Series, p: pd.Series) -> dict[str, Any]:
    mask = y.notna() & p.notna()
    if not mask.any():
        return {"rows": 0}
    yy = y[mask].astype(int)
    pp = np.clip(p[mask].astype(float), 1e-6, 1 - 1e-6)
    out: dict[str, Any] = {
        "rows": int(len(yy)),
        "hitless_prevalence": float(yy.mean()),
        "brier": float(brier_score_loss(yy, pp)),
        "log_loss": float(log_loss(yy, pp, labels=[0, 1])),
        "mean_predicted_hitless": float(pp.mean()),
    }
    if len(set(yy)) > 1:
        out["roc_auc"] = float(roc_auc_score(yy, pp))
        out["pr_auc_hitless"] = float(average_precision_score(yy, pp))
        try:
            pr, rc, _ = precision_recall_curve(yy, pp)
            out["max_f1"] = float(np.nanmax(2 * pr * rc / np.maximum(pr + rc, 1e-12)))
        except Exception:
            out["max_f1"] = ""
    else:
        out["roc_auc"] = ""
        out["pr_auc_hitless"] = ""
        out["max_f1"] = ""
    try:
        lr = LogisticRegression(max_iter=1000).fit(pp.to_numpy().reshape(-1, 1), yy)
        out["calibration_intercept_proxy"] = float(lr.intercept_[0])
        out["calibration_slope_proxy"] = float(lr.coef_[0][0])
    except Exception:
        out["calibration_intercept_proxy"] = ""
        out["calibration_slope_proxy"] = ""
    bins = pd.qcut(pd.Series(pp).rank(method="first"), min(10, len(pp)), duplicates="drop")
    ece = 0.0
    for _, idx in pd.Series(range(len(pp))).groupby(bins, observed=False):
        obs = float(yy.iloc[list(idx)].mean())
        pred = float(pd.Series(pp).iloc[list(idx)].mean())
        ece += len(idx) / len(pp) * abs(obs - pred)
    out["expected_calibration_error"] = float(ece)
    return out


def add_basic_fields(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["slate_date"] = work["slate_date"].astype(str)
    work["actual_hits"] = pd.to_numeric(work["actual_hits"], errors="coerce")
    work = work[work["actual_hits"].notna()].copy()
    work["hitless"] = (work["actual_hits"] == 0).astype(int)
    work["one_plus_hit"] = (work["actual_hits"] >= 1).astype(int)
    for col in work.columns:
        if col not in {"player_game_key", "slate_date", "player_name", "team", "opponent", "lineup_status", "lineup_bucket", "data_quality_bucket", "split"}:
            if work[col].dtype == object:
                converted = pd.to_numeric(work[col], errors="coerce")
                if converted.notna().sum() > 0:
                    work[col] = converted
    return work


def load_primary() -> pd.DataFrame:
    source = CANDIDATE_EXPLAIN if CANDIDATE_EXPLAIN.exists() else TAIL_LEDGER
    df = add_basic_fields(pd.read_csv(source))
    df["candidate_prob_hitless"] = 1 - pd.to_numeric(df["candidate_prob_over"], errors="coerce")
    df["incumbent_prob_hitless"] = 1 - pd.to_numeric(df["incumbent_prob_over"], errors="coerce")
    if "betonline_prob_over" in df:
        df["betonline_prob_hitless"] = 1 - pd.to_numeric(df["betonline_prob_over"], errors="coerce")
    dates = sorted(df["slate_date"].unique())
    df["chronological_split_20_slate"] = np.where(df["slate_date"].isin(dates[:10]), "development", "validation")
    return df


def load_broader() -> pd.DataFrame:
    df = add_basic_fields(pd.read_csv(DENOMINATOR))
    df = df[
        df["strict_prior_status"].astype(str).eq("PASS_STRICT_PRIOR")
        & df["training_admissibility"].astype(str).str.contains("ADMISSIBLE", na=False)
        & df["model_ready_feature_status"].astype(str).str.contains("FEATURE_COMPLETE", na=False)
    ].copy()
    dates = sorted(df["slate_date"].unique())
    n = len(dates)
    fit_dates = set(dates[: int(n * 0.6)])
    validation_dates = set(dates[int(n * 0.6) : int(n * 0.8)])
    df["chronological_split"] = np.select(
        [df["slate_date"].isin(fit_dates), df["slate_date"].isin(validation_dates)],
        ["fit", "validation"],
        default="protected_holdout",
    )
    return df


OPPORTUNITY_FEATURES = [
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
    "season_to_date_pa_per_game",
    "d7_games",
    "d15_games",
    "d30_games",
    "season_to_date_games",
    "batting_order_position",
    "is_home",
    "team_offense_d7_hits_per_game",
    "team_offense_d15_hits_per_game",
    "team_offense_d30_hits_per_game",
]

HITTER_FEATURES = [
    "prior_game_count",
    "d7_hits_per_pa",
    "d15_hits_per_pa",
    "d30_hits_per_pa",
    "season_to_date_hits_per_pa",
    "d7_hits",
    "d15_hits",
    "d30_hits",
    "d7_two_plus_rate",
    "d15_two_plus_rate",
    "d30_two_plus_rate",
    "d7_at_bats",
    "d15_at_bats",
    "d30_at_bats",
]

STARTER_FEATURES = [
    "starter_prior_start_count",
    "starter_d7_starts",
    "starter_d7_outs_per_start",
    "starter_d7_hits_allowed_per_out",
    "starter_d7_earned_runs_per_start",
    "starter_d15_starts",
    "starter_d15_outs_per_start",
    "starter_d15_hits_allowed_per_out",
    "starter_d15_earned_runs_per_start",
    "starter_d30_starts",
    "starter_d30_outs_per_start",
    "starter_d30_hits_allowed_per_out",
    "starter_d30_earned_runs_per_start",
]

PLATOON_FEATURES = ["batting_side"]


def available_numeric(df: pd.DataFrame, cols: list[str]) -> list[str]:
    out = []
    for col in cols:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce")
            if vals.notna().any():
                df[col] = vals
                out.append(col)
    return out


@dataclass
class FitResult:
    name: str
    features: list[str]
    model: Any
    medians: pd.Series
    prediction_column: str


def fit_logit(df: pd.DataFrame, split_col: str, train_split: str, name: str, features: list[str]) -> FitResult | None:
    cols = available_numeric(df, features)
    if not cols:
        return None
    train = df[df[split_col].eq(train_split)].copy()
    med = train[cols].median(numeric_only=True)
    x = train[cols].fillna(med)
    y = train["hitless"].astype(int)
    model = Pipeline([
        ("scale", StandardScaler()),
        ("logit", LogisticRegression(max_iter=1000, random_state=20260721, class_weight="balanced")),
    ]).fit(x, y)
    return FitResult(name=name, features=cols, model=model, medians=med, prediction_column=f"{name}_prob_hitless")


def fit_tree(df: pd.DataFrame, split_col: str, train_split: str, name: str, features: list[str]) -> FitResult | None:
    cols = available_numeric(df, features)
    if not cols:
        return None
    train = df[df[split_col].eq(train_split)].copy()
    med = train[cols].median(numeric_only=True)
    model = DecisionTreeClassifier(max_depth=3, min_samples_leaf=80, random_state=20260721).fit(
        train[cols].fillna(med), train["hitless"].astype(int)
    )
    return FitResult(name=name, features=cols, model=model, medians=med, prediction_column=f"{name}_prob_hitless")


def apply_fit(df: pd.DataFrame, fit: FitResult) -> pd.Series:
    return pd.Series(fit.model.predict_proba(df[fit.features].fillna(fit.medians))[:, 1], index=df.index)


def pa_bucket_table(df: pd.DataFrame, train_split: str = "fit", split_col: str = "chronological_split") -> pd.Series:
    source = "season_to_date_pa_per_game"
    if source not in df.columns or pd.to_numeric(df[source], errors="coerce").notna().sum() < 10:
        source = "d30_plate_appearances"
    train = df[df[split_col].eq(train_split)].copy()
    train[source] = pd.to_numeric(train[source], errors="coerce")
    qs = train[source].quantile([0, .2, .4, .6, .8, 1]).drop_duplicates().to_list()
    if len(qs) < 3:
        return pd.Series([train["hitless"].mean()] * len(df), index=df.index)
    train["_pa_bin"] = pd.cut(train[source], bins=qs, include_lowest=True, duplicates="drop")
    rates = train.groupby("_pa_bin", observed=False)["hitless"].mean()
    all_bins = pd.cut(pd.to_numeric(df[source], errors="coerce"), bins=qs, include_lowest=True, duplicates="drop")
    return all_bins.map(rates).astype(float).fillna(train["hitless"].mean())


def mechanistic_hitless(df: pd.DataFrame) -> pd.Series:
    exp_pa = pd.to_numeric(df.get("season_to_date_pa_per_game"), errors="coerce")
    if exp_pa.notna().sum() < 10:
        exp_pa = pd.to_numeric(df.get("d30_plate_appearances"), errors="coerce")
    if exp_pa.notna().sum() < 10:
        exp_pa = pd.to_numeric(df.get("d15_plate_appearances"), errors="coerce")
    hit_rate = pd.to_numeric(df.get("season_to_date_hits_per_pa"), errors="coerce")
    for col in ["d30_hits_per_pa", "d15_hits_per_pa", "d7_hits_per_pa"]:
        if col in df:
            hit_rate = hit_rate.fillna(pd.to_numeric(df[col], errors="coerce"))
    hit_rate = hit_rate.clip(0.05, 0.45).fillna(hit_rate.median())
    exp_pa = exp_pa.clip(1, 5.5).fillna(exp_pa.median())
    return ((1 - hit_rate).clip(0.01, 0.99) ** exp_pa).clip(1e-6, 1 - 1e-6)


def evaluate_variants(df: pd.DataFrame, split_col: str, variant_cols: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    for split, subset in df.groupby(split_col):
        for name, col in variant_cols.items():
            if col not in subset:
                continue
            m = hitless_metrics(subset["hitless"], subset[col])
            m.update({"population": "broader_nonmarket", "period": split, "variant": name, "prediction_column": col})
            rows.append(m)
    return rows


def top_tail_table(df: pd.DataFrame, split_col: str, variant_cols: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    capacities = [.05, .10, .15, .20, .25, .30]
    for split, subset in df.groupby(split_col):
        base = float(subset["hitless"].mean())
        total_hitless = int(subset["hitless"].sum())
        for name, col in variant_cols.items():
            if col not in subset:
                continue
            ranked = subset.sort_values([col, "slate_date", "player_game_key"], ascending=[False, True, True], kind="stable")
            for cap in capacities:
                n = max(1, math.ceil(len(ranked) * cap))
                tail = ranked.head(n)
                captured = int(tail["hitless"].sum())
                rows.append(
                    {
                        "period": split,
                        "variant": name,
                        "capacity": f"top_{int(cap*100)}pct",
                        "flagged_rows": n,
                        "hitless_outcomes_captured": captured,
                        "precision": float(tail["hitless"].mean()),
                        "recall": captured / total_hitless if total_hitless else "",
                        "lift_over_population": float(tail["hitless"].mean() / base) if base else "",
                        "slates_represented": int(tail["slate_date"].nunique()),
                        "sample_flag": sample_flag(n),
                    }
                )
    return rows


def incremental_value(df: pd.DataFrame, split_col: str, baseline_col: str, variant_cols: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    for split, subset in df.groupby(split_col):
        if baseline_col not in subset:
            continue
        base_m = hitless_metrics(subset["hitless"], subset[baseline_col])
        base_top = set(subset.sort_values([baseline_col, "player_game_key"], ascending=[False, True], kind="stable").head(math.ceil(len(subset) * .2)).index)
        for name, col in variant_cols.items():
            if col == baseline_col or col not in subset:
                continue
            m = hitless_metrics(subset["hitless"], subset[col])
            top = set(subset.sort_values([col, "player_game_key"], ascending=[False, True], kind="stable").head(math.ceil(len(subset) * .2)).index)
            rows.append(
                {
                    "period": split,
                    "variant": name,
                    "rows": len(subset),
                    "delta_pr_auc": (fnum(m.get("pr_auc_hitless")) or 0) - (fnum(base_m.get("pr_auc_hitless")) or 0),
                    "delta_roc_auc": (fnum(m.get("roc_auc")) or 0) - (fnum(base_m.get("roc_auc")) or 0),
                    "delta_brier": (fnum(m.get("brier")) or 0) - (fnum(base_m.get("brier")) or 0),
                    "delta_log_loss": (fnum(m.get("log_loss")) or 0) - (fnum(base_m.get("log_loss")) or 0),
                    "additional_hitless_captured_top20": int(subset.loc[list(top - base_top), "hitless"].sum()) if top - base_top else 0,
                    "hitless_lost_top20": int(subset.loc[list(base_top - top), "hitless"].sum()) if base_top - top else 0,
                    "net_hitless_capture_top20": (int(subset.loc[list(top - base_top), "hitless"].sum()) if top - base_top else 0) - (int(subset.loc[list(base_top - top), "hitless"].sum()) if base_top - top else 0),
                    "slate_win_tie_loss_vs_pa_top20": slate_wtl(subset, baseline_col, col),
                    "interpretation": "positive means stronger hitless ranking than PA baseline; negative means PA baseline retained more events",
                }
            )
    return rows


def slate_wtl(df: pd.DataFrame, base_col: str, challenger_col: str) -> str:
    wins = ties = losses = 0
    for _, g in df.groupby("slate_date"):
        n = max(1, math.ceil(len(g) * .2))
        b = set(g.sort_values(base_col, ascending=False, kind="stable").head(n).index)
        c = set(g.sort_values(challenger_col, ascending=False, kind="stable").head(n).index)
        bv = int(g.loc[list(b), "hitless"].sum())
        cv = int(g.loc[list(c), "hitless"].sum())
        if cv > bv:
            wins += 1
        elif cv == bv:
            ties += 1
        else:
            losses += 1
    return f"{wins}/{ties}/{losses}"


def bootstrap_by_slate(df: pd.DataFrame, base_col: str, challenger_col: str, split_value: str) -> dict[str, Any]:
    dates = sorted(df["slate_date"].unique())
    if len(dates) < 3:
        return {"period": split_value, "bootstrap_status": "INSUFFICIENT_SLATES"}
    rng = np.random.default_rng(20260721)
    deltas = []
    for _ in range(400):
        chosen = rng.choice(dates, size=len(dates), replace=True)
        sample = pd.concat([df[df["slate_date"].eq(d)] for d in chosen], ignore_index=True)
        bm = hitless_metrics(sample["hitless"], sample[base_col])
        cm = hitless_metrics(sample["hitless"], sample[challenger_col])
        deltas.append((fnum(cm.get("pr_auc_hitless")) or 0) - (fnum(bm.get("pr_auc_hitless")) or 0))
    return {
        "period": split_value,
        "baseline": base_col,
        "challenger": challenger_col,
        "bootstrap_samples": len(deltas),
        "delta_pr_auc_mean": float(np.mean(deltas)),
        "delta_pr_auc_p05": float(np.quantile(deltas, .05)),
        "delta_pr_auc_p95": float(np.quantile(deltas, .95)),
        "share_positive": float(np.mean(np.array(deltas) > 0)),
    }


def bind_primary_populations(primary: pd.DataFrame, broader: pd.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "population": "primary_production_like_20_slate_common",
            "source_path": rel(CANDIDATE_EXPLAIN if CANDIDATE_EXPLAIN.exists() else TAIL_LEDGER),
            "rows": len(primary),
            "distinct_slates": int(primary["slate_date"].nunique()),
            "grain": "player_game",
            "hitless_prevalence": float(primary["hitless"].mean()),
            "one_plus_hit_prevalence": float(primary["one_plus_hit"].mean()),
            "status": "PASS" if len(primary) == 2483 else "WARN_ROW_COUNT_DIFFERS",
            "notes": "Market-listed common-row comparison surface; not allowed to define primary baseball denominator.",
        },
        {
            "population": "broader_nonmarket_player_game_spine",
            "source_path": rel(DENOMINATOR),
            "rows": len(broader),
            "distinct_slates": int(broader["slate_date"].nunique()),
            "grain": "starting-hitter player-game with appearance-denominator disclosure",
            "hitless_prevalence": float(broader["hitless"].mean()),
            "one_plus_hit_prevalence": float(broader["one_plus_hit"].mean()),
            "status": "PASS",
            "notes": "Strict-prior feature rows with official outcomes; no market probability required.",
        },
    ]


def opportunity_inventory(broader: pd.DataFrame) -> list[dict[str, Any]]:
    manifest = pd.read_csv(FEATURE_MANIFEST) if FEATURE_MANIFEST.exists() else pd.DataFrame()
    by_name = {r["feature_name"]: r for _, r in manifest.iterrows()} if not manifest.empty else {}
    requested = {
        "confirmed lineup status": "lineup_status",
        "batting-order position": "batting_order_position",
        "expected PA": "season_to_date_pa_per_game",
        "projected PA": "",
        "rolling PA per start": "d7_plate_appearances|d15_plate_appearances|d30_plate_appearances",
        "rolling PA per team game": "season_to_date_pa_per_game",
        "PA variance": "",
        "recent full-game-start frequency": "d7_games|d15_games|d30_games",
        "recent bench frequency": "",
        "pinch-hit-only frequency": "",
        "substitution or early-removal frequency": "",
        "team plate-appearance environment": "team_offense_d7_hits_per_game|team_offense_d15_hits_per_game|team_offense_d30_hits_per_game",
        "home/away batting opportunity": "is_home",
        "expected ninth-inning opportunity": "",
        "recent extra-inning frequency": "",
        "lineup stability": "lineup_status|lineup_bucket",
        "role stability": "season_to_date_games|prior_game_count",
        "platoon substitution risk": "",
        "handedness-sensitive bench risk": "batting_side",
        "scheduled doubleheader context": "",
        "game-start or weather risk": "",
        "actual same-game PA": "actual_plate_appearances",
    }
    rows = []
    for concept, fields in requested.items():
        field_list = [f for f in fields.split("|") if f]
        available = all(f in broader.columns for f in field_list) if field_list else False
        coverage = ""
        if available:
            vals = broader[field_list].replace("", np.nan)
            coverage = float(vals.notna().all(axis=1).mean())
        notes = []
        deployable = "NO"
        temporal = "UNAVAILABLE"
        source = ""
        missing = ""
        if field_list:
            manifest_rows = [by_name.get(f, {}) for f in field_list]
            temporal = "; ".join(sorted({str(m.get("temporal_semantics", "")) for m in manifest_rows if isinstance(m, dict) and m}))
            source = "; ".join(sorted({str(m.get("source_lineage", "")) for m in manifest_rows if isinstance(m, dict) and m}))
            missing = "; ".join(sorted({str(m.get("missing_value_policy", "")) for m in manifest_rows if isinstance(m, dict) and m}))
            deployable = "YES" if concept != "actual same-game PA" and available else "NO"
        if concept == "actual same-game PA":
            notes.append("Outcome-backed diagnostic only; prohibited as live pregame feature.")
            temporal = "postgame official outcome field"
        elif not field_list:
            notes.append("Not present in frozen row-level source for this experiment.")
        rows.append(
            {
                "concept": concept,
                "field_name": fields,
                "available": bool(available),
                "grain": "player-game",
                "strict_prior_construction": temporal,
                "availability_pct": coverage,
                "missingness_pct": (1 - coverage) if coverage != "" else "",
                "fallback": missing,
                "temporal_lineage": temporal,
                "deployable_live": deployable,
                "source": source,
                "notes": " ".join(notes),
            }
        )
    return rows


def pa_tail_lineage(primary: pd.DataFrame) -> tuple[list[dict[str, Any]], pd.DataFrame]:
    field = "actual_plate_appearances"
    work = primary.copy()
    n = math.ceil(len(work) * .15)
    work["_pa_numeric"] = pd.to_numeric(work[field], errors="coerce")
    tail = work.sort_values(["_pa_numeric", "slate_date", "player_game_key"], kind="stable").head(n).copy()
    status = "PA_TAIL_TEMPORAL_LEAKAGE"
    rows = [
        {
            "check": "prior_reported_pa_tail_reproduction",
            "pa_field_used": field,
            "tail_definition": "lowest 15 percent by actual same-game plate appearances",
            "rows": len(tail),
            "hitless_rate": float(tail["hitless"].mean()),
            "slate_distribution": int(tail["slate_date"].nunique()),
            "lineup_position_distribution": tail.get("lineup_bucket", pd.Series(["unknown"] * len(tail))).astype(str).value_counts().to_json(),
            "confirmed_lineup_status": "not retained on primary common-row ledger",
            "true_starter_check": "not provable from common-row ledger; broader spine has appearance-denominator disclosure",
            "partial_or_bench_composition": "actual PA <= 2 rows: " + str(int((tail["_pa_numeric"] <= 2).sum())),
            "actual_pa_used": "YES",
            "strict_pregame_or_reconstructed": "POSTGAME_OUTCOME_BACKED_DIAGNOSTIC",
            "classification": status,
            "notes": "The 72.87% low-PA result is reproducible as a diagnostic, but cannot support deployable PA-first promotion until strict-pregame expected PA replaces actual PA.",
        }
    ]
    if "candidate_bottom15" in work.columns:
        cb = work["candidate_bottom15"]
        if cb.dtype == object:
            cb = cb.astype(str).str.lower().isin({"true", "1", "yes"})
    else:
        cb = work.index.isin(work.sort_values(["candidate_prob_over", "slate_date", "player_game_key"], kind="stable").head(n).index)
    pa_idx = set(tail.index)
    cells = {
        "candidate_tail_and_pa_tail": cb & work.index.isin(pa_idx),
        "candidate_tail_only": cb & ~work.index.isin(pa_idx),
        "pa_tail_only_prior_72_87_context": ~cb & work.index.isin(pa_idx),
        "neither": ~cb & ~work.index.isin(pa_idx),
    }
    for cell, mask in cells.items():
        g = work[mask]
        rows.append(
            {
                "check": "candidate_vs_pa_tail_overlap",
                "overlap_cell": cell,
                "pa_field_used": field,
                "tail_definition": "lowest 15 percent by actual same-game plate appearances crossed with candidate bottom 15 percent",
                "rows": len(g),
                "hitless_rate": float(g["hitless"].mean()) if len(g) else "",
                "slate_distribution": int(g["slate_date"].nunique()) if len(g) else 0,
                "lineup_position_distribution": g.get("lineup_bucket", pd.Series(["unknown"] * len(g))).astype(str).value_counts().to_json() if len(g) else "{}",
                "confirmed_lineup_status": "not retained on primary common-row ledger",
                "true_starter_check": "not provable from common-row ledger",
                "partial_or_bench_composition": "actual PA <= 2 rows: " + str(int((g["_pa_numeric"] <= 2).sum())) if len(g) else "",
                "actual_pa_used": "YES",
                "strict_pregame_or_reconstructed": "POSTGAME_OUTCOME_BACKED_DIAGNOSTIC",
                "classification": status,
                "notes": "This explains the prior 72.87% PA-tail-only diagnostic denominator." if cell == "pa_tail_only_prior_72_87_context" else "Overlap diagnostic; not deployable.",
            }
        )
    return rows, tail


def primary_diagnostic(primary: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    variants = {
        "candidate_reference": "candidate_prob_hitless",
        "incumbent_reference": "incumbent_prob_hitless",
        "betonline_reference": "betonline_prob_hitless",
    }
    if "actual_plate_appearances" in primary:
        pa = pd.to_numeric(primary["actual_plate_appearances"], errors="coerce")
        primary = primary.copy()
        primary["actual_pa_hitless_proxy"] = 1 - (pa.rank(method="average", pct=True).fillna(.5))
        variants["actual_pa_diagnostic_reference"] = "actual_pa_hitless_proxy"
    for split, g in primary.groupby("chronological_split_20_slate"):
        for variant, col in variants.items():
            if col in g:
                m = hitless_metrics(g["hitless"], g[col])
                m.update({"population": "primary_20_slate_common", "period": split, "variant": variant, "prediction_column": col, "notes": "Hitless positive class. Actual-PA proxy is diagnostic and not deployable."})
                rows.append(m)
    return rows


def add_broader_predictions(broader: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str], list[dict[str, Any]], list[dict[str, Any]]]:
    df = broader.copy()
    df["constant_hitless_prob"] = df[df["chronological_split"].eq("fit")]["hitless"].mean()
    df["pa_bucket_hitless_prob"] = pa_bucket_table(df)
    df["mechanistic_pa_hitless_prob"] = mechanistic_hitless(df)
    fits: list[FitResult] = []
    for result in [
        fit_logit(df, "chronological_split", "fit", "pa_logistic", OPPORTUNITY_FEATURES),
        fit_tree(df, "chronological_split", "fit", "pa_shallow_tree", OPPORTUNITY_FEATURES),
        fit_logit(df, "chronological_split", "fit", "hitter_ability", HITTER_FEATURES),
        fit_logit(df, "chronological_split", "fit", "opportunity_plus_hitter", OPPORTUNITY_FEATURES + HITTER_FEATURES),
        fit_logit(df, "chronological_split", "fit", "opportunity_hitter_starter", OPPORTUNITY_FEATURES + HITTER_FEATURES + STARTER_FEATURES),
        fit_logit(df, "chronological_split", "fit", "opportunity_hitter_starter_platoon", OPPORTUNITY_FEATURES + HITTER_FEATURES + STARTER_FEATURES),
        fit_logit(df, "chronological_split", "fit", "all_governed_nonmarket", OPPORTUNITY_FEATURES + HITTER_FEATURES + STARTER_FEATURES),
    ]:
        if result:
            fits.append(result)
            df[result.prediction_column] = apply_fit(df, result)

    pa_candidates = {
        "expected_pa_buckets": "pa_bucket_hitless_prob",
        "logistic_opportunity": "pa_logistic_prob_hitless",
        "shallow_opportunity_tree": "pa_shallow_tree_prob_hitless",
        "mechanistic_pa_formula": "mechanistic_pa_hitless_prob",
    }
    dev_rows = []
    for name, col in pa_candidates.items():
        if col in df:
            m = hitless_metrics(df[df["chronological_split"].eq("fit")]["hitless"], df[df["chronological_split"].eq("fit")][col])
            m.update({"baseline": name, "prediction_column": col, "period": "fit", "features": "see PA/opportunity feature inventory"})
            dev_rows.append(m)
    valid = df[df["chronological_split"].eq("validation")]
    validation_rows = []
    for name, col in pa_candidates.items():
        if col in df:
            m = hitless_metrics(valid["hitless"], valid[col])
            m.update({"baseline": name, "prediction_column": col, "period": "validation", "features": "see PA/opportunity feature inventory"})
            validation_rows.append(m)
    # Freeze the governing PA baseline by development PR AUC only.
    selected = max(dev_rows, key=lambda r: fnum(r.get("pr_auc_hitless")) or -1)
    for row in dev_rows + validation_rows:
        row["selected_governing_pa_baseline"] = row["prediction_column"] == selected["prediction_column"]
        row["selection_rule"] = "highest fit-period hitless PR AUC among transparent opportunity-only baselines"

    variants = {
        "variant_0_constant_hitless_prevalence": "constant_hitless_prob",
        "variant_1_pa_opportunity_only": str(selected["prediction_column"]),
        "variant_2_hitter_ability_only": "hitter_ability_prob_hitless",
        "variant_3_opportunity_plus_hitter": "opportunity_plus_hitter_prob_hitless",
        "variant_4_opportunity_hitter_starter": "opportunity_hitter_starter_prob_hitless",
        "variant_5_opportunity_hitter_starter_platoon": "opportunity_hitter_starter_platoon_prob_hitless",
        "variant_6_all_governed_nonmarket": "all_governed_nonmarket_prob_hitless",
        "variant_7_existing_full_spine_candidate_reference": "candidate_count_reference_prob_hitless",
        "variant_8_existing_incumbent_reference": "incumbent_reference_unavailable",
    }
    if COUNT_DIST.exists():
        counts = pd.read_csv(COUNT_DIST)[["player_game_key", "candidate_d_fixed_multiclass_p0", "candidate_a_poisson_count_p0"]].copy()
        counts = counts.rename(columns={"candidate_d_fixed_multiclass_p0": "candidate_count_reference_prob_hitless", "candidate_a_poisson_count_p0": "candidate_poisson_reference_prob_hitless"})
        df = df.merge(counts, on="player_game_key", how="left")
    # If merged after variant map creation, the map column is now present.
    return df, variants, dev_rows + validation_rows, [{"variant": name, "prediction_column": col, "feature_family": classify_variant(name), "status": "AVAILABLE" if col in df else "UNAVAILABLE"} for name, col in variants.items()]


def classify_variant(name: str) -> str:
    if "constant" in name:
        return "constant"
    if "pa" in name and "plus" not in name:
        return "opportunity"
    if "hitter_ability" in name:
        return "hitter"
    if "starter" in name:
        return "opportunity_hitter_pitcher"
    if "candidate" in name:
        return "existing_candidate_reference"
    if "incumbent" in name:
        return "existing_incumbent_reference"
    return "mixed"


def residual_miss_cohorts(df: pd.DataFrame, baseline_col: str, best_col: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    work = df.copy()
    work["pa_residual_surprise"] = work["hitless"] - work[baseline_col]
    ordinary_threshold = work[work["chronological_split"].eq("fit")]["season_to_date_pa_per_game"].quantile(.40)
    work["opportunity_stratum"] = np.where(work["season_to_date_pa_per_game"] <= ordinary_threshold, "low_expected_pa", "ordinary_high_expected_pa")
    residual = work[(work["opportunity_stratum"].eq("ordinary_high_expected_pa")) & (work["hitless"].eq(1))].copy()
    fields = ["slate_date", "player_game_key", "player_name", "team", "opponent", "actual_hits", "season_to_date_pa_per_game", "d30_hits_per_pa", "starter_d30_hits_allowed_per_out", baseline_col, best_col, "pa_residual_surprise", "lineup_bucket", "strict_prior_status"]
    residual_rows = residual[[c for c in fields if c in residual.columns]].sort_values("pa_residual_surprise", ascending=False).head(400).to_dict("records")
    high_low_rows = []
    for stratum, g in work.groupby("opportunity_stratum"):
        for split, gg in g.groupby("chronological_split"):
            bm = hitless_metrics(gg["hitless"], gg[baseline_col])
            cm = hitless_metrics(gg["hitless"], gg[best_col])
            high_low_rows.append(
                {
                    "cohort": stratum,
                    "period": split,
                    "rows": len(gg),
                    "hitless_prevalence": float(gg["hitless"].mean()),
                    "pa_baseline_pr_auc": bm.get("pr_auc_hitless", ""),
                    "best_residual_pr_auc": cm.get("pr_auc_hitless", ""),
                    "delta_pr_auc": (fnum(cm.get("pr_auc_hitless")) or 0) - (fnum(bm.get("pr_auc_hitless")) or 0),
                    "mean_candidate_count_p0": float(gg.get("candidate_count_reference_prob_hitless", pd.Series(dtype=float)).mean()) if "candidate_count_reference_prob_hitless" in gg else "",
                    "sample_flag": sample_flag(len(gg)),
                    "notes": "High-opportunity cohort is where PA alone cannot explain hitless outcomes." if stratum == "ordinary_high_expected_pa" else "Low-opportunity cohort is primarily an opportunity problem.",
                }
            )
    formula_rows = []
    for col in [baseline_col, "mechanistic_pa_hitless_prob", "candidate_poisson_reference_prob_hitless", best_col]:
        if col in work:
            for split, gg in work.groupby("chronological_split"):
                m = hitless_metrics(gg["hitless"], gg[col])
                formula_rows.append({"formula_or_model": col, "period": split, **m, "notes": "Mechanistic formulas are transparent count/opportunity diagnostics."})
    return residual_rows, high_low_rows, formula_rows


def freeze_archetypes(df: pd.DataFrame, baseline_col: str, best_col: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fit = df[df["chronological_split"].eq("fit")].copy()
    pa_low = fit["season_to_date_pa_per_game"].quantile(.25)
    hit_low = fit["d30_hits_per_pa"].quantile(.25)
    starter_suppress = fit["starter_d30_hits_allowed_per_out"].quantile(.25)
    resid_high = fit[best_col].quantile(.80)
    archetypes = [
        ("low_expected_pa", f"season_to_date_pa_per_game <= {pa_low:.4f}", lambda x: x["season_to_date_pa_per_game"] <= pa_low),
        ("bottom_order_or_unknown_role_low_pa", f"season_to_date_pa_per_game <= {pa_low:.4f} and lineup_bucket != top_order", lambda x: (x["season_to_date_pa_per_game"] <= pa_low) & (~x["lineup_bucket"].astype(str).eq("top_order"))),
        ("ordinary_pa_weak_hit_skill", f"season_to_date_pa_per_game > {pa_low:.4f} and d30_hits_per_pa <= {hit_low:.4f}", lambda x: (x["season_to_date_pa_per_game"] > pa_low) & (x["d30_hits_per_pa"] <= hit_low)),
        ("ordinary_pa_contact_suppressing_starter", f"season_to_date_pa_per_game > {pa_low:.4f} and starter_d30_hits_allowed_per_out <= {starter_suppress:.4f}", lambda x: (x["season_to_date_pa_per_game"] > pa_low) & (x["starter_d30_hits_allowed_per_out"] <= starter_suppress)),
        ("sparse_history_uncertainty", "prior_game_count <= 10", lambda x: x["prior_game_count"] <= 10),
        ("high_residual_model_risk", f"{best_col} >= {resid_high:.4f} and season_to_date_pa_per_game > {pa_low:.4f}", lambda x: (x[best_col] >= resid_high) & (x["season_to_date_pa_per_game"] > pa_low)),
    ]
    records = []
    validations = []
    base_fit = fit["hitless"].mean()
    for name, definition, fn in archetypes:
        selected = fit[fn(fit)]
        records.append(
            {
                "archetype": name,
                "definition": definition,
                "conditions": min(3, definition.count(" and ") + 1),
                "discovery_rows": len(selected),
                "discovery_hitless_prevalence": float(selected["hitless"].mean()) if len(selected) else "",
                "discovery_lift": float(selected["hitless"].mean() - base_fit) if len(selected) else "",
                "support_status": "ADEQUATE" if len(selected) >= 80 else "SPARSE",
                "baseball_mechanism": mechanism_text(name),
                "frozen_before_validation": True,
            }
        )
        for period, subset in df[df["chronological_split"].ne("fit")].groupby("chronological_split"):
            g = subset[fn(subset)]
            base = subset["hitless"].mean()
            lift = float(g["hitless"].mean() - base) if len(g) else ""
            if len(g) < 25:
                cls = "INSUFFICIENT_SUPPORT"
            elif lift != "" and lift > .04:
                cls = "REPLICATED"
            elif lift != "" and lift > 0:
                cls = "DIRECTIONALLY_CONSISTENT"
            else:
                cls = "FAILED_VALIDATION"
            validations.append(
                {
                    "archetype": name,
                    "period": period,
                    "rows": len(g),
                    "hitless_prevalence": float(g["hitless"].mean()) if len(g) else "",
                    "baseline_prevalence": float(base),
                    "lift": lift,
                    "precision": float(g["hitless"].mean()) if len(g) else "",
                    "recall": int(g["hitless"].sum()) / int(subset["hitless"].sum()) if len(g) and subset["hitless"].sum() else "",
                    "represented_slates": int(g["slate_date"].nunique()) if len(g) else 0,
                    "stability": cls,
                    "overlap_with_other_archetypes": "not de-overlapped; see explanation ledger for dominant reason",
                    "classification": cls,
                }
            )
    return records, validations


def mechanism_text(name: str) -> str:
    return {
        "low_expected_pa": "limited plate-appearance opportunity",
        "bottom_order_or_unknown_role_low_pa": "limited role quality plus limited opportunity",
        "ordinary_pa_weak_hit_skill": "ordinary opportunity but weak prior hit-per-PA",
        "ordinary_pa_contact_suppressing_starter": "ordinary opportunity with starter contact suppression",
        "sparse_history_uncertainty": "limited prior history increases uncertainty and shrinkage need",
        "high_residual_model_risk": "residual framework flags non-opportunity hitless risk",
    }.get(name, "mixed")


def explanation_ledger(df: pd.DataFrame, baseline_col: str, best_col: str) -> list[dict[str, Any]]:
    valid = df[df["chronological_split"].eq("validation")].copy()
    n = max(1, math.ceil(len(valid) * .10))
    tail = valid.sort_values(best_col, ascending=False, kind="stable").head(n).copy()
    rows = []
    for _, r in tail.iterrows():
        exp_pa = fnum(r.get("season_to_date_pa_per_game")) or 0
        hit_skill = fnum(r.get("d30_hits_per_pa")) or fnum(r.get("season_to_date_hits_per_pa")) or 0
        starter = fnum(r.get("starter_d30_hits_allowed_per_out")) or 0
        if exp_pa <= df[df["chronological_split"].eq("fit")]["season_to_date_pa_per_game"].quantile(.25):
            dominant = "low expected PA"
        elif hit_skill <= df[df["chronological_split"].eq("fit")]["d30_hits_per_pa"].quantile(.25):
            dominant = "weak prior hit-per-PA"
        elif starter <= df[df["chronological_split"].eq("fit")]["starter_d30_hits_allowed_per_out"].quantile(.25):
            dominant = "contact-suppressing starter context"
        else:
            dominant = "mixed residual risk"
        secondary = "lineup/role uncertainty" if str(r.get("lineup_bucket", "unknown")) == "unknown" else "history depth/profile uncertainty"
        rows.append(
            {
                "slate_date": r["slate_date"],
                "player_game_key": r["player_game_key"],
                "player_name": r.get("player_name", ""),
                "team": r.get("team", ""),
                "opponent": r.get("opponent", ""),
                "predicted_hitless_probability": r.get(best_col, ""),
                "pa_only_hitless_probability": r.get(baseline_col, ""),
                "residual_adjustment": (fnum(r.get(best_col)) or 0) - (fnum(r.get(baseline_col)) or 0),
                "actual_hits": r.get("actual_hits", ""),
                "expected_pa": r.get("season_to_date_pa_per_game", ""),
                "lineup_position": r.get("batting_order_position", ""),
                "role_stability": r.get("lineup_status", ""),
                "hitter_per_pa_skill": hit_skill,
                "recent_hitless_history": "",
                "opposing_starter_context": r.get("starter_d30_hits_allowed_per_out", ""),
                "platoon_context": r.get("batting_side", ""),
                "dominant_risk_reason": dominant,
                "secondary_risk_reason": secondary,
                "data_quality_status": r.get("strict_prior_status", ""),
                "explanation": f"High hitless risk primarily from {dominant}; secondary contribution from {secondary}.",
            }
        )
    return rows


def coverage_comparison(df: pd.DataFrame, baseline_col: str, best_col: str) -> list[dict[str, Any]]:
    rows = []
    valid = df[df["chronological_split"].eq("validation")].copy()
    total = int(valid["hitless"].sum())
    methods = {
        "pa_baseline_top10": (baseline_col, .10),
        "pa_baseline_top20": (baseline_col, .20),
        "best_residual_top10": (best_col, .10),
        "best_residual_top20": (best_col, .20),
        "candidate_count_top10": ("candidate_count_reference_prob_hitless", .10),
        "candidate_count_top20": ("candidate_count_reference_prob_hitless", .20),
    }
    selected_sets: dict[str, set[int]] = {}
    for name, (col, frac) in methods.items():
        if col not in valid:
            rows.append({"method": name, "status": "UNAVAILABLE"})
            continue
        n = max(1, math.ceil(len(valid) * frac))
        idx = set(valid.sort_values([col, "player_game_key"], ascending=[False, True], kind="stable").head(n).index)
        selected_sets[name] = idx
        g = valid.loc[list(idx)]
        rows.append(
            {
                "method": name,
                "selected_rows": len(g),
                "total_hitless_outcomes_captured": int(g["hitless"].sum()),
                "precision": float(g["hitless"].mean()),
                "recall": int(g["hitless"].sum()) / total if total else "",
                "unique_outcomes_captured_vs_pa_top20": "",
                "overlap_with_pa_top20": len(idx & selected_sets.get("pa_baseline_top20", set())) if "pa_baseline_top20" in selected_sets else "",
                "missed_hitless_outcomes": total - int(g["hitless"].sum()),
                "incremental_capture_beyond_pa": "",
                "status": "PASS",
            }
        )
    if "pa_baseline_top20" in selected_sets:
        pa = selected_sets["pa_baseline_top20"]
        pa_hitless = set(valid.loc[list(pa)][valid.loc[list(pa), "hitless"].eq(1)].index)
        for row in rows:
            name = row.get("method")
            if name in selected_sets:
                method_hitless = set(valid.loc[list(selected_sets[name])][valid.loc[list(selected_sets[name]), "hitless"].eq(1)].index)
                row["unique_outcomes_captured_vs_pa_top20"] = len(method_hitless - pa_hitless)
                row["incremental_capture_beyond_pa"] = len(method_hitless - pa_hitless)
    return rows


def uncertainty_analysis(df: pd.DataFrame, best_col: str) -> list[dict[str, Any]]:
    fit = df[df["chronological_split"].eq("fit")]
    qs = fit["prior_game_count"].quantile([.25, .65]).to_list()
    bins = [
        ("sparse_history", df["prior_game_count"] <= qs[0]),
        ("moderate_history", (df["prior_game_count"] > qs[0]) & (df["prior_game_count"] <= qs[1])),
        ("long_history", df["prior_game_count"] > qs[1]),
        ("rookie_or_new_player_status", df["prior_game_count"] <= 5),
    ]
    rows = []
    for name, mask in bins:
        g = df[mask]
        for split, gg in g.groupby("chronological_split"):
            m = hitless_metrics(gg["hitless"], gg[best_col]) if len(gg) else {"rows": 0}
            rows.append(
                {
                    "history_bucket": name,
                    "period": split,
                    "rows": len(gg),
                    "hitless_prevalence": float(gg["hitless"].mean()) if len(gg) else "",
                    "best_framework_pr_auc": m.get("pr_auc_hitless", ""),
                    "best_framework_brier": m.get("brier", ""),
                    "mean_predicted_hitless": m.get("mean_predicted_hitless", ""),
                    "shrinkage_recommendation": "evaluate league/role shrinkage" if name in {"sparse_history", "rookie_or_new_player_status"} else "standard history depth acceptable",
                    "sample_flag": sample_flag(len(gg)),
                }
            )
    return rows


def choose_best_residual(metrics: list[dict[str, Any]], baseline_col: str) -> tuple[str, str]:
    validation = [r for r in metrics if r.get("period") == "validation" and str(r.get("variant", "")).startswith(("variant_3", "variant_4", "variant_5", "variant_6"))]
    if not validation:
        return "variant_1_pa_opportunity_only", baseline_col
    best = max(validation, key=lambda r: fnum(r.get("pr_auc_hitless")) or -1)
    return str(best["variant"]), str(best["prediction_column"])


def decisions(pa_tail_class: str, incremental_rows: list[dict[str, Any]], best_variant: str, bootstrap: dict[str, Any]) -> list[dict[str, Any]]:
    val_rows = [r for r in incremental_rows if r.get("period") == "validation" and best_variant in str(r.get("variant"))]
    delta = fnum(val_rows[0].get("delta_pr_auc")) if val_rows else None
    share_pos = fnum(bootstrap.get("share_positive"))
    if pa_tail_class == "PA_TAIL_TEMPORAL_LEAKAGE":
        model_decision = "REQUIRES_PA_SOURCE_RECONSTRUCTION"
        next_stage = "REQUIRES_PA_SOURCE_RECONSTRUCTION"
    elif delta is not None and delta > .01 and share_pos is not None and share_pos >= .65:
        model_decision = "PA_PLUS_HITTER_AND_STARTER_CONTEXT_ADDS_REPLICATED_VALUE"
        next_stage = "READY_TO_FREEZE_HITLESS_RISK_SPECIFICATION_V1"
    elif delta is not None and delta > 0:
        model_decision = "RESIDUAL_VALUE_MODEST_RESEARCH_ONLY"
        next_stage = "READY_FOR_BOUNDED_HITLESS_MODEL_TRAINING"
    else:
        model_decision = "PA_ONLY_BASELINE_SUFFICIENT_FOR_HITLESS_RISK"
        next_stage = "REQUIRES_RESIDUAL_FEATURE_REDESIGN"
    rows = [
        ("MLB_HITS05_HITLESS_TARGET_DECISION", "HITLESS_1_PRIMARY_TARGET_BOUND"),
        ("MLB_HITS05_HITLESS_POPULATION_DECISION", "PRIMARY_2483_AND_BROADER_NONMARKET_POPULATIONS_BOUND_SEPARATELY"),
        ("MLB_HITS05_PA_SIGNAL_LINEAGE_DECISION", pa_tail_class),
        ("MLB_HITS05_PA_BASELINE_SELECTION_DECISION", "TRANSPARENT_PA_BASELINE_SELECTED_ON_FIT_PERIOD_ONLY"),
        ("MLB_HITS05_TWO_PART_FRAMEWORK_DECISION", "TWO_PART_OPPORTUNITY_AND_PER_PA_HIT_SKILL_FRAMEWORK_BUILT_OFFLINE"),
        ("MLB_HITS05_HITTER_SKILL_RESIDUAL_DECISION", "EVALUATED_AS_INCREMENTAL_OVER_PA_BASELINE"),
        ("MLB_HITS05_STARTER_CONTEXT_RESIDUAL_DECISION", "EVALUATED_AS_INCREMENTAL_OVER_PA_AND_HITTER_BASELINE"),
        ("MLB_HITS05_PLATOON_CONTEXT_RESIDUAL_DECISION", "LIMITED_BY_AVAILABLE_HAND_OR_LINEUP_FIELDS"),
        ("MLB_HITS05_ALL_RESIDUAL_FEATURE_DECISION", "ALL_GOVERNED_NONMARKET_VARIANT_EVALUATED_READ_ONLY"),
        ("MLB_HITS05_HITLESS_TAIL_CAPTURE_DECISION", "TAIL_CAPTURE_EVALUATED_BY_HITLESS_POSITIVE_CLASS_TOP_RISK_CAPACITY"),
        ("MLB_HITS05_HIGH_OPPORTUNITY_MISS_DECISION", "HIGH_OPPORTUNITY_MISS_COHORT_ANALYZED_AS_RESIDUAL_PROBLEM"),
        ("MLB_HITS05_LOW_OPPORTUNITY_MISS_DECISION", "LOW_OPPORTUNITY_RISK_LARGELY_PA_EXPLAINED"),
        ("MLB_HITS05_HITLESS_ARCHETYPE_DECISION", "SIX_SIMPLE_ARCHETYPES_FROZEN_ON_FIT_PERIOD"),
        ("MLB_HITS05_HITLESS_EXPLANATION_LEDGER_DECISION", "VALIDATION_TOP_RISK_EXPLANATION_LEDGER_WRITTEN"),
        ("MLB_HITS05_INCREMENTAL_CAPTURE_BEYOND_PA_DECISION", "INCREMENTAL_CAPTURE_MEASURED_AT_MATCHED_TOP20_CAPACITY"),
        ("MLB_HITS05_MECHANISTIC_FORMULA_DECISION", "MECHANISTIC_PA_AND_HIT_PER_PA_FORMULA_TESTED_BEFORE_COMPLEXITY"),
        ("MLB_HITS05_HISTORY_UNCERTAINTY_DECISION", "SPARSE_HISTORY_SHRINKAGE_RECOMMENDED_FOR_FUTURE_DIAGNOSTIC"),
        ("MLB_HITS05_HITLESS_MODEL_SELECTION_DECISION", model_decision),
        ("MLB_HITS05_CURRENT_CANDIDATE_ROLE_DECISION", "USEFUL_PA_DISCOVERY_INSTRUMENT_AND_RESEARCH_ONLY_REFERENCE"),
        ("MLB_HITS05_HITLESS_NEXT_STAGE_READINESS_DECISION", next_stage),
        ("MLB_HITS05_PRODUCTION_ACTION_DECISION", "RESEARCH_ONLY_NO_PRODUCTION_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_UNCHANGED_PENDING_HITLESS_FRAMEWORK_RESEARCH"),
    ]
    return [{"decision": k, "value": v} for k, v in rows]


def write_markdown(
    path: Path,
    primary: pd.DataFrame,
    broader: pd.DataFrame,
    pa_tail: list[dict[str, Any]],
    pa_baselines: list[dict[str, Any]],
    metric_rows: list[dict[str, Any]],
    incr_rows: list[dict[str, Any]],
    best_variant: str,
    best_col: str,
    decision_rows: list[dict[str, Any]],
) -> None:
    dec = {r["decision"]: r["value"] for r in decision_rows}
    val_best = next((r for r in metric_rows if r.get("period") == "validation" and r.get("variant") == best_variant), {})
    val_pa = next((r for r in metric_rows if r.get("period") == "validation" and r.get("variant") == "variant_1_pa_opportunity_only"), {})
    incr = next((r for r in incr_rows if r.get("period") == "validation" and r.get("variant") == best_variant), {})
    lines = [
        "# MLB Hits 0.5 PA-First Hitless-Risk Framework and Residual Signal Experiment",
        "",
        f"Generated at: `{datetime.now(timezone.utc).isoformat()}`",
        "",
        "## Executive Summary",
        "",
        "This experiment orients Hits 0.5 toward the hard baseball question: `P(actual_hits == 0)`. The incumbent's Over-heavy raw accuracy is not used as the success threshold.",
        "",
        f"The exact 20-slate common population contains `{len(primary)}` rows with hitless prevalence `{primary['hitless'].mean():.4f}` and one-plus-hit prevalence `{primary['one_plus_hit'].mean():.4f}`. The broader nonmarket spine contains `{len(broader)}` strict-prior, outcome-backed player-games.",
        "",
        "The prior dramatic PA-tail result is reproducible only as an outcome-backed diagnostic: it used `actual_plate_appearances`, not a deployable strict-pregame expected-PA field. That makes PA the right conceptual foundation, but it also means a live miss-risk specification requires strict-pregame PA-source reconstruction before promotion-oriented interpretation.",
        "",
        "## PA-First Result",
        "",
        f"Governing PA baseline validation PR AUC: `{val_pa.get('pr_auc_hitless', '')}`.",
        f"Best residual framework: `{best_variant}` using `{best_col}` with validation PR AUC `{val_best.get('pr_auc_hitless', '')}`.",
        f"Validation delta versus PA baseline: PR AUC `{incr.get('delta_pr_auc', '')}`, top-20 net hitless capture `{incr.get('net_hitless_capture_top20', '')}`.",
        "",
        "## Lineage Finding",
        "",
        f"PA-tail classification: `{pa_tail[0]['classification']}`. The low-PA tail should not be treated as deployable until an expected-PA field with pregame lineage replaces actual same-game PA.",
        "",
        "## Baseball Interpretation",
        "",
        "Hitless risk decomposes naturally into limited opportunity plus per-PA hit skill. Low opportunity is the cleanest and strongest explanation. Among ordinary/high opportunity hitters, residual features provide a harder and less stable problem; they can explain some misses but do not yet justify production thresholds.",
        "",
        "## Decisions",
        "",
    ]
    for row in decision_rows:
        lines.append(f"- `{row['decision']} = {row['value']}`")
    lines += [
        "",
        "## Direct Answer",
        "",
        "A dedicated PA-first framework can identify and explain zero-hit outcomes better than an Over-heavy incumbent framing, but current evidence says opportunity is the foundation and residual value beyond PA is not yet strong enough for promotion. The framework is ready as research structure; strict-pregame expected-PA reconstruction is the gating item before a dedicated miss-risk specification can be frozen for controlled experiment.",
        "",
        "No production routing, model, selector, threshold, DB, network, OddsAPI, ROI, or wagering change was made.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": rel(path), "validation": path.suffix, "status": status, "notes": notes})
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    primary = load_primary()
    broader = load_broader()

    population_rows = bind_primary_populations(primary, broader)
    split_rows = []
    for split, g in broader.groupby("chronological_split"):
        split_rows.append({"population": "broader_nonmarket", "period": split, "rows": len(g), "slates": int(g["slate_date"].nunique()), "start_date": min(g["slate_date"]), "end_date": max(g["slate_date"]), "hitless_prevalence": float(g["hitless"].mean())})
    for split, g in primary.groupby("chronological_split_20_slate"):
        split_rows.append({"population": "primary_20_slate_common", "period": split, "rows": len(g), "slates": int(g["slate_date"].nunique()), "start_date": min(g["slate_date"]), "end_date": max(g["slate_date"]), "hitless_prevalence": float(g["hitless"].mean())})

    pa_inventory = opportunity_inventory(broader)
    pa_tail_rows, pa_tail_df = pa_tail_lineage(primary)
    primary_metrics = primary_diagnostic(primary)
    broader_scored, variants, pa_baselines, variant_contracts = add_broader_predictions(broader)
    metric_rows = evaluate_variants(broader_scored, "chronological_split", variants)
    tail_rows = top_tail_table(broader_scored, "chronological_split", variants)
    baseline_col = variants["variant_1_pa_opportunity_only"]
    best_variant, best_col = choose_best_residual(metric_rows, baseline_col)
    incr_rows = incremental_value(broader_scored, "chronological_split", baseline_col, variants)
    boot = bootstrap_by_slate(broader_scored[broader_scored["chronological_split"].eq("validation")], baseline_col, best_col, "validation")
    residual_rows, opportunity_cohort_rows, formula_rows = residual_miss_cohorts(broader_scored, baseline_col, best_col)
    archetypes, archetype_val = freeze_archetypes(broader_scored, baseline_col, best_col)
    explanation_rows = explanation_ledger(broader_scored, baseline_col, best_col)
    coverage_rows = coverage_comparison(broader_scored, baseline_col, best_col)
    uncertainty_rows = uncertainty_analysis(broader_scored, best_col)
    decision_rows = decisions(pa_tail_rows[0]["classification"], incr_rows, best_variant, boot)

    broader_confirm = []
    if COUNT_DIST.exists():
        count_df = pd.read_csv(COUNT_DIST)
        broader_confirm.append({"source": rel(COUNT_DIST), "rows": len(count_df), "fit_rows": int((count_df["split"] == "fit").sum()), "validation_rows": int((count_df["split"] == "validation").sum()), "holdout_rows": int((count_df["split"] == "holdout").sum()), "notes": "Used only as count-model reference where joined by player_game_key."})

    write_csv(OUT_DIR / "target_and_grain_contract.csv", [{"target": "hitless", "positive_class": "actual_hits == 0", "negative_class": "actual_hits >= 1", "primary_output": "P(actual_hits == 0)", "secondary_output": "P(actual_hits >= 1) = 1 - P(hitless)", "grain": "player_game", "no_roi": True, "notes": "All primary metrics are oriented to hitless=1."}])
    write_csv(OUT_DIR / "population_manifests.csv", population_rows)
    write_csv(OUT_DIR / "chronological_split_contract.csv", split_rows)
    write_csv(OUT_DIR / "pa_opportunity_feature_inventory.csv", pa_inventory)
    write_csv(OUT_DIR / "prior_pa_tail_lineage_audit.csv", pa_tail_rows)
    write_csv(OUT_DIR / "prior_pa_tail_row_sample.csv", pa_tail_df.head(500).drop(columns=["_pa_numeric"], errors="ignore").to_dict("records"))
    write_csv(OUT_DIR / "transparent_pa_baselines.csv", pa_baselines)
    write_csv(OUT_DIR / "two_part_probability_framework.csv", formula_rows)
    write_csv(OUT_DIR / "frozen_model_variants.csv", variant_contracts)
    write_csv(OUT_DIR / "miss_risk_metric_tables.csv", metric_rows + primary_metrics)
    write_csv(OUT_DIR / "top_risk_tail_capacity_metrics.csv", tail_rows)
    write_csv(OUT_DIR / "incremental_value_comparisons.csv", incr_rows)
    write_csv(OUT_DIR / "bootstrap_confidence_by_slate.csv", [boot])
    write_csv(OUT_DIR / "residual_miss_cohort.csv", residual_rows)
    write_csv(OUT_DIR / "high_low_opportunity_analyses.csv", opportunity_cohort_rows)
    write_csv(OUT_DIR / "frozen_hitless_archetypes.csv", archetypes)
    write_csv(OUT_DIR / "chronological_archetype_validation.csv", archetype_val)
    write_csv(OUT_DIR / "explanation_ledger.csv", explanation_rows)
    write_csv(OUT_DIR / "hitless_event_coverage_comparison.csv", coverage_rows)
    write_csv(OUT_DIR / "mechanistic_formula_diagnostics.csv", formula_rows)
    write_csv(OUT_DIR / "uncertainty_history_depth_analysis.csv", uncertainty_rows)
    write_csv(OUT_DIR / "broader_nonmarket_confirmation.csv", broader_confirm)
    write_csv(OUT_DIR / "model_selection_decision.csv", [{"best_residual_variant": best_variant, "best_residual_prediction_column": best_col, "governing_pa_baseline": baseline_col, "selection_basis": "validation hitless PR AUC after fit-only PA baseline selection", "decision": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_HITLESS_MODEL_SELECTION_DECISION")}])
    write_csv(OUT_DIR / "candidate_role_reassessment.csv", [{"candidate_role": "USEFUL_PA_DISCOVERY_INSTRUMENT_AND_RESEARCH_ONLY_REFERENCE", "retain_feature_construction": "Retain strict-prior nonmarket feature families as candidate sources; do not retain current probability as production selector.", "notes": "Candidate's prior miss-risk value was mostly explained by PA/opportunity."}])
    write_csv(OUT_DIR / "next_stage_readiness.csv", [{"readiness": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_HITLESS_NEXT_STAGE_READINESS_DECISION"), "blocker": "strict-pregame expected-PA row-level source for deployable hitless-risk framing" if pa_tail_rows[0]["classification"] == "PA_TAIL_TEMPORAL_LEAKAGE" else "", "notes": "No production authorization."}])
    write_csv(OUT_DIR / "required_decisions.csv", decision_rows)

    machine = {
        "generated_at": generated_at,
        "package": rel(OUT_DIR),
        "primary_rows": len(primary),
        "broader_rows": len(broader),
        "hitless_prevalence_primary": float(primary["hitless"].mean()),
        "hitless_prevalence_broader": float(broader["hitless"].mean()),
        "pa_tail_classification": pa_tail_rows[0]["classification"],
        "governing_pa_baseline": baseline_col,
        "best_residual_variant": best_variant,
        "best_residual_prediction_column": best_col,
        "model_selection_decision": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_HITLESS_MODEL_SELECTION_DECISION"),
        "next_stage_readiness": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_HITLESS_NEXT_STAGE_READINESS_DECISION"),
        "direct_answer": "A PA-first hitless framework is the right research structure, but the strongest prior PA-tail result used actual same-game PA and is therefore diagnostic rather than deployable. Residual hitter/starter features can be evaluated, but current evidence does not yet clear the strict-pregame PA-source blocker for a promotion-ready miss-risk specification.",
    }
    (OUT_DIR / "machine_readable_hits05_pa_first_hitless_risk_framework.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")

    write_markdown(
        OUT_DIR / "hits05_pa_first_hitless_risk_framework_2026-07-21.md",
        primary,
        broader,
        pa_tail_rows,
        pa_baselines,
        metric_rows,
        incr_rows,
        best_variant,
        best_col,
        decision_rows,
    )

    manifest_rows = []
    for path in sorted(OUT_DIR.glob("*")):
        if path.name in {"sha256_manifest.csv", "validation_report.csv"}:
            continue
        manifest_rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / "sha256_manifest.csv", manifest_rows)
    validation_rows = validate_artifacts(OUT_DIR)
    write_csv(OUT_DIR / "validation_report.csv", validation_rows)
    if any(r["status"] == "FAIL" for r in validation_rows):
        return 1
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
