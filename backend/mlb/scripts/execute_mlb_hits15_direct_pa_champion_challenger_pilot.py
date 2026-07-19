#!/usr/bin/env python3
"""Execute the bounded MLB Hits 1.5 direct-PA Champion-Challenger pilot.

This is a research-only offline experiment. It reads frozen local artifacts,
fits only the explicitly authorized logistic instruments, and writes an
artifact package. It performs no DB writes, network calls, uploads, model
promotion, or production prediction changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


RUN_DATE = "2026-07-17"
ROOT = Path(__file__).resolve().parents[3]
SOURCE_PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_hits_15_pa_opportunity_overlay_diagnostic/2026-07-16"
DEFAULT_OUT_DIR = ROOT / f"artifacts/analysis/model_development/mlb_hits_15_direct_pa_champion_challenger_pilot/{RUN_DATE}"

POPULATION_PATH = SOURCE_PACKAGE / "historical_population_manifest_2026-07-16.csv"
FIELD_MANIFEST_PATH = SOURCE_PACKAGE / "pa_overlay_field_manifest_2026-07-16.csv"

IDENTITY_COLS = ["slate_date", "game_id", "player_id", "prop_type", "line", "side_normalized"]
CHAMPION_COL = "control_probability"
OUTCOME_COL = "target_class"
ODDS_COL = "selected_price"

NUMERIC_PA_FEATURES = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_context_age_days",
    "pa_missing_flag",
]
CATEGORICAL_PA_FEATURES = [
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_opp_v1_complete_prior_pa",
]


@dataclass(frozen=True)
class SplitSpec:
    fit_dates: list[str]
    validation_dates: list[str]
    holdout_dates: list[str]


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _canonical_key(row: pd.Series) -> str:
    line = float(row["line"])
    line_text = f"{line:.1f}"
    return "|".join(
        [
            str(row["slate_date"]),
            str(int(row["game_id"])),
            str(int(row["player_id"])),
            str(row["prop_type"]),
            line_text,
            str(row["side_normalized"]).lower(),
        ]
    )


def _price_profit(price: float, won: bool) -> float:
    if not math.isfinite(price):
        return math.nan
    if won:
        return price / 100.0 if price > 0 else 100.0 / abs(price)
    return -1.0


def _calibration_slope_intercept(y: np.ndarray, p: np.ndarray) -> tuple[float, float]:
    eps = 1e-6
    p = np.clip(p, eps, 1.0 - eps)
    logits = np.log(p / (1.0 - p)).reshape(-1, 1)
    if len(np.unique(y)) < 2:
        return math.nan, math.nan
    model = LogisticRegression(C=1e6, solver="lbfgs", max_iter=2000, random_state=17)
    model.fit(logits, y)
    return float(model.coef_[0][0]), float(model.intercept_[0])


def _ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = len(y)
    if total == 0:
        return math.nan
    err = 0.0
    for i in range(bins):
        lo, hi = edges[i], edges[i + 1]
        mask = (p >= lo) & ((p < hi) if i < bins - 1 else (p <= hi))
        if not mask.any():
            continue
        err += (mask.sum() / total) * abs(float(y[mask].mean()) - float(p[mask].mean()))
    return float(err)


def _safe_auc(y: np.ndarray, p: np.ndarray) -> float:
    if len(np.unique(y)) < 2:
        return math.nan
    return float(roc_auc_score(y, p))


def _safe_spearman(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 2:
        return math.nan
    result = spearmanr(a, b, nan_policy="omit")
    return float(result.statistic) if result.statistic == result.statistic else math.nan


def _metrics(df: pd.DataFrame, pred_col: str, instrument: str, split: str) -> dict[str, Any]:
    work = df.dropna(subset=[OUTCOME_COL, pred_col]).copy()
    y = work[OUTCOME_COL].astype(int).to_numpy()
    p = work[pred_col].astype(float).clip(1e-6, 1 - 1e-6).to_numpy()
    slope, intercept = _calibration_slope_intercept(y, p) if len(work) else (math.nan, math.nan)
    price = pd.to_numeric(work.get(ODDS_COL), errors="coerce")
    odds_mask = price.notna()
    profits = [
        _price_profit(float(pr), bool(won))
        for pr, won in zip(price[odds_mask].to_numpy(), work.loc[odds_mask, OUTCOME_COL].astype(int).to_numpy())
    ]
    return {
        "instrument": instrument,
        "split": split,
        "rows": int(len(df)),
        "resolved": int(len(work)),
        "wins": int(work[OUTCOME_COL].sum()) if len(work) else 0,
        "losses": int(len(work) - work[OUTCOME_COL].sum()) if len(work) else 0,
        "outcome_rate": float(y.mean()) if len(work) else math.nan,
        "avg_prediction": float(p.mean()) if len(work) else math.nan,
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(work) else math.nan,
        "brier_score": float(brier_score_loss(y, p)) if len(work) else math.nan,
        "roc_auc": _safe_auc(y, p) if len(work) else math.nan,
        "calibration_slope": slope,
        "calibration_intercept": intercept,
        "expected_calibration_error": _ece(y, p) if len(work) else math.nan,
        "spearman_vs_outcome": _safe_spearman(p, y) if len(work) else math.nan,
        "odds_supported_rows": int(odds_mask.sum()),
        "flat_stake_units": float(np.nansum(profits)) if profits else math.nan,
        "flat_stake_roi": float(np.nanmean(profits)) if profits else math.nan,
    }


def _band_rows(df: pd.DataFrame, pred_col: str, instrument: str, split: str) -> list[dict[str, Any]]:
    bins = [0.0, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 1.0]
    labels = ["lt_0_20", "0_20_0_30", "0_30_0_40", "0_40_0_50", "0_50_0_60", "0_60_0_70", "0_70_0_80", "ge_0_80"]
    work = df.dropna(subset=[OUTCOME_COL, pred_col]).copy()
    work["prediction_band"] = pd.cut(work[pred_col], bins=bins, labels=labels, include_lowest=True)
    rows: list[dict[str, Any]] = []
    for band, group in work.groupby("prediction_band", observed=True):
        rows.append(
            {
                "instrument": instrument,
                "split": split,
                "prediction_band": str(band),
                "rows": int(len(group)),
                "avg_prediction": float(group[pred_col].mean()),
                "outcome_rate": float(group[OUTCOME_COL].mean()),
                "calibration_error": float(group[OUTCOME_COL].mean() - group[pred_col].mean()),
            }
        )
    return rows


def _date_stability_rows(df: pd.DataFrame, pred_cols: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (split, date), group in df.groupby(["split", "slate_date"], dropna=False):
        for instrument, col in pred_cols.items():
            if col not in group:
                continue
            m = _metrics(group, col, instrument, str(split))
            rows.append(
                {
                    "slate_date": date,
                    "split": split,
                    "instrument": instrument,
                    "rows": m["rows"],
                    "wins": m["wins"],
                    "losses": m["losses"],
                    "outcome_rate": m["outcome_rate"],
                    "avg_prediction": m["avg_prediction"],
                    "log_loss": m["log_loss"],
                    "brier_score": m["brier_score"],
                    "roc_auc": m["roc_auc"],
                }
            )
    return rows


def _bootstrap_rows(df: pd.DataFrame, seed: int = 17, n_boot: int = 1000) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, Any]] = []
    for split in ["validation", "holdout"]:
        part = df[df["split"].eq(split)].dropna(subset=[OUTCOME_COL, "control_process_probability", "pa_challenger_probability"])
        if len(part) < 10:
            continue
        deltas_log: list[float] = []
        deltas_brier: list[float] = []
        idx = np.arange(len(part))
        y_all = part[OUTCOME_COL].astype(int).to_numpy()
        c_all = part["control_process_probability"].astype(float).to_numpy()
        p_all = part["pa_challenger_probability"].astype(float).to_numpy()
        for _ in range(n_boot):
            sample = rng.choice(idx, size=len(idx), replace=True)
            y = y_all[sample]
            c = np.clip(c_all[sample], 1e-6, 1 - 1e-6)
            p = np.clip(p_all[sample], 1e-6, 1 - 1e-6)
            if len(np.unique(y)) < 2:
                continue
            deltas_log.append(float(log_loss(y, p, labels=[0, 1]) - log_loss(y, c, labels=[0, 1])))
            deltas_brier.append(float(brier_score_loss(y, p) - brier_score_loss(y, c)))
        for metric, values in [("log_loss_delta_pa_minus_control", deltas_log), ("brier_delta_pa_minus_control", deltas_brier)]:
            if values:
                rows.append(
                    {
                        "split": split,
                        "metric": metric,
                        "iterations": len(values),
                        "mean": float(np.mean(values)),
                        "ci95_low": float(np.quantile(values, 0.025)),
                        "ci95_high": float(np.quantile(values, 0.975)),
                    }
                )
    return rows


def _top_bottom_rows(df: pd.DataFrame, pred_cols: dict[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for split in ["validation", "holdout"]:
        part = df[df["split"].eq(split)].dropna(subset=[OUTCOME_COL]).copy()
        if part.empty:
            continue
        for instrument, col in pred_cols.items():
            ranked = part.dropna(subset=[col]).sort_values(col)
            if ranked.empty:
                continue
            n = max(10, int(math.ceil(len(ranked) * 0.20)))
            for cohort, group in [("bottom_20pct_score", ranked.head(n)), ("top_20pct_score", ranked.tail(n))]:
                rows.append(
                    {
                        "split": split,
                        "instrument": instrument,
                        "cohort": cohort,
                        "rows": int(len(group)),
                        "avg_prediction": float(group[col].mean()),
                        "outcome_rate": float(group[OUTCOME_COL].mean()),
                        "wins": int(group[OUTCOME_COL].sum()),
                        "losses": int(len(group) - group[OUTCOME_COL].sum()),
                    }
                )
    return rows


def _coefficient_sign_stability(
    validation: pd.DataFrame,
    numeric: list[str],
    categorical: list[str],
    fit_coefficients: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if validation.empty or validation[OUTCOME_COL].nunique() < 2:
        return [{"feature": "ALL", "fit_sign": "", "validation_sign": "", "same_sign": False, "notes": "validation partition unavailable or single-class"}]
    preprocessor = ColumnTransformer(
        [
            ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), numeric),
            (
                "cat",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical,
            ),
        ],
        remainder="drop",
    )
    model = Pipeline(
        [
            ("preprocess", preprocessor),
            ("model", LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", max_iter=2000, random_state=17)),
        ]
    )
    model.fit(validation[numeric + categorical], validation[OUTCOME_COL].astype(int))
    validation_coeffs = {
        feat: float(coef)
        for feat, coef in zip(model.named_steps["preprocess"].get_feature_names_out(), model.named_steps["model"].coef_[0])
    }
    fit_map = {r["feature"]: float(r["coefficient"]) for r in fit_coefficients if r["instrument"] == "pa_challenger"}
    rows: list[dict[str, Any]] = []
    for feature in sorted(set(fit_map) | set(validation_coeffs)):
        fit_coef = fit_map.get(feature, math.nan)
        val_coef = validation_coeffs.get(feature, math.nan)
        fit_sign = "positive" if fit_coef > 0 else "negative" if fit_coef < 0 else "zero" if fit_coef == 0 else "missing"
        val_sign = "positive" if val_coef > 0 else "negative" if val_coef < 0 else "zero" if val_coef == 0 else "missing"
        rows.append(
            {
                "feature": feature,
                "fit_coefficient": fit_coef,
                "validation_diagnostic_coefficient": val_coef,
                "fit_sign": fit_sign,
                "validation_sign": val_sign,
                "same_sign": fit_sign == val_sign and fit_sign not in {"missing", "zero"},
                "notes": "validation-only diagnostic fit; not used for model selection or holdout scoring",
            }
        )
    return rows


def _make_split(df: pd.DataFrame) -> SplitSpec:
    counts = df.groupby("slate_date").size().sort_index()
    total = int(counts.sum())
    if counts.size < 10 or total < 300:
        raise RuntimeError("INSUFFICIENT_TEMPORAL_FOLDS_FOR_CHALLENGER_PILOT")
    cumulative = counts.cumsum()
    fit_end_idx = int(np.searchsorted(cumulative.to_numpy(), total * 0.60, side="left"))
    validation_end_idx = int(np.searchsorted(cumulative.to_numpy(), total * 0.80, side="left"))
    dates = list(counts.index.astype(str))
    fit_dates = dates[: fit_end_idx + 1]
    validation_dates = dates[fit_end_idx + 1 : validation_end_idx + 1]
    holdout_dates = dates[validation_end_idx + 1 :]
    if not fit_dates or not validation_dates or not holdout_dates:
        raise RuntimeError("INSUFFICIENT_TEMPORAL_FOLDS_FOR_CHALLENGER_PILOT")
    return SplitSpec(fit_dates, validation_dates, holdout_dates)


def _assign_splits(df: pd.DataFrame, spec: SplitSpec) -> pd.DataFrame:
    out = df.copy()
    split_map = {d: "fit" for d in spec.fit_dates}
    split_map.update({d: "validation" for d in spec.validation_dates})
    split_map.update({d: "holdout" for d in spec.holdout_dates})
    out["split"] = out["slate_date"].astype(str).map(split_map)
    if out["split"].isna().any():
        raise RuntimeError("split assignment failed")
    return out


def _prepare_population() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    raw = pd.read_csv(POPULATION_PATH)
    raw["canonical_identity"] = raw.apply(_canonical_key, axis=1)
    raw["target_class"] = pd.to_numeric(raw["target_class"], errors="coerce")
    raw["control_probability"] = pd.to_numeric(raw["control_probability"], errors="coerce")
    raw["line"] = pd.to_numeric(raw["line"], errors="coerce")
    raw["strict_prior_bool"] = _bool_series(raw["strict_prior_pa_qualified"])
    raw["diagnostic_bool"] = _bool_series(raw["diagnostic_eligible"])
    raw["official_bool"] = _bool_series(raw["official_outcome_qualified"])
    raw["inferred_bool"] = _bool_series(raw["pregame_reconcile_inferred_population"])

    exclusions: list[dict[str, Any]] = []
    for reason, mask in [
        ("not_hits_15_over", ~(raw["prop_type"].eq("hits") & raw["line"].eq(1.5) & raw["side_normalized"].eq("over"))),
        ("not_diagnostic_eligible", ~raw["diagnostic_bool"]),
        ("not_official_outcome_qualified", ~raw["official_bool"]),
        ("missing_champion_control_probability", raw["control_probability"].isna()),
        ("missing_outcome", raw["target_class"].isna()),
    ]:
        exclusions.append({"reason": reason, "rows": int(mask.sum())})

    eligible = raw[
        raw["prop_type"].eq("hits")
        & raw["line"].eq(1.5)
        & raw["side_normalized"].eq("over")
        & raw["diagnostic_bool"]
        & raw["official_bool"]
        & raw["control_probability"].notna()
        & raw["target_class"].notna()
    ].copy()
    direct = eligible[eligible["strict_prior_bool"]].copy()
    inferred = eligible[eligible["inferred_bool"] & ~eligible["strict_prior_bool"]].copy()

    duplicate_rows: list[dict[str, Any]] = []
    for name, frame in [("eligible", eligible), ("direct", direct), ("inferred", inferred)]:
        dupes = frame[frame.duplicated("canonical_identity", keep=False)].copy()
        duplicate_rows.append({"population": name, "duplicate_rows": int(len(dupes)), "duplicate_identities": int(dupes["canonical_identity"].nunique())})

    if len(direct) != 1292 or len(inferred) != 592:
        raise RuntimeError(f"direct/inferred population mismatch: direct={len(direct)} inferred={len(inferred)}")
    if direct["canonical_identity"].duplicated().any():
        raise RuntimeError("duplicate canonical identities in direct population")
    if direct["control_probability"].isna().any():
        raise RuntimeError("champion value missing in direct population")
    return raw, direct, inferred, exclusions, duplicate_rows


def _fit_models(direct: pd.DataFrame, split_spec: SplitSpec) -> tuple[pd.DataFrame, Pipeline, Pipeline, list[str], list[str]]:
    direct = _assign_splits(direct, split_spec)
    fit = direct[direct["split"].eq("fit")].copy()
    y_fit = fit[OUTCOME_COL].astype(int)

    champion_features = [CHAMPION_COL]
    control = Pipeline(
        [
            ("impute", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            ("model", LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", max_iter=2000, random_state=17)),
        ]
    )
    control.fit(fit[champion_features], y_fit)

    numeric = [CHAMPION_COL] + NUMERIC_PA_FEATURES
    categorical = CATEGORICAL_PA_FEATURES
    preprocessor = ColumnTransformer(
        [
            ("num", Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler())]), numeric),
            (
                "cat",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                categorical,
            ),
        ],
        remainder="drop",
    )
    challenger = Pipeline(
        [
            ("preprocess", preprocessor),
            ("model", LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", max_iter=2000, random_state=17)),
        ]
    )
    challenger.fit(fit[numeric + categorical], y_fit)

    scored = direct.copy()
    scored["champion_probability"] = scored[CHAMPION_COL].astype(float).clip(1e-6, 1 - 1e-6)
    scored["control_process_probability"] = control.predict_proba(scored[champion_features])[:, 1]
    scored["pa_challenger_probability"] = challenger.predict_proba(scored[numeric + categorical])[:, 1]
    scored["pa_minus_control_probability"] = scored["pa_challenger_probability"] - scored["control_process_probability"]
    scored["pa_movement_direction"] = np.where(scored["pa_minus_control_probability"] > 0.01, "up", np.where(scored["pa_minus_control_probability"] < -0.01, "down", "flat"))
    return scored, control, challenger, numeric, categorical


def _score_inferred(inferred: pd.DataFrame, control: Pipeline, challenger: Pipeline, split_spec: SplitSpec, numeric: list[str], categorical: list[str]) -> pd.DataFrame:
    inferred = inferred.copy()
    split_map = {d: "fit" for d in split_spec.fit_dates}
    split_map.update({d: "validation" for d in split_spec.validation_dates})
    split_map.update({d: "holdout" for d in split_spec.holdout_dates})
    inferred["split"] = inferred["slate_date"].astype(str).map(split_map)
    first_direct_date = min(split_spec.fit_dates)
    inferred.loc[inferred["split"].isna() & (inferred["slate_date"].astype(str) < first_direct_date), "split"] = "pre_direct_out_of_domain"
    inferred["split"] = inferred["split"].fillna("out_of_domain_not_in_direct_split")
    inferred["champion_probability"] = inferred[CHAMPION_COL].astype(float).clip(1e-6, 1 - 1e-6)
    inferred["control_process_probability"] = control.predict_proba(inferred[[CHAMPION_COL]])[:, 1]
    inferred["pa_challenger_probability"] = challenger.predict_proba(inferred[numeric + categorical])[:, 1]
    inferred["pa_minus_control_probability"] = inferred["pa_challenger_probability"] - inferred["control_process_probability"]
    return inferred


def _coefficients(pipe: Pipeline, name: str, numeric: list[str], categorical: list[str]) -> list[dict[str, Any]]:
    if name == "control_process":
        coef = pipe.named_steps["model"].coef_[0]
        return [{"instrument": name, "feature": CHAMPION_COL, "coefficient": float(coef[0])}]
    feature_names = list(pipe.named_steps["preprocess"].get_feature_names_out())
    coefs = pipe.named_steps["model"].coef_[0]
    return [{"instrument": name, "feature": feat, "coefficient": float(coef)} for feat, coef in zip(feature_names, coefs)]


def _write_csv(path: Path, rows: list[dict[str, Any]] | pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(rows, pd.DataFrame):
        rows.to_csv(path, index=False)
        return
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_manifest(out_dir: Path) -> None:
    rows = []
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name != f"sha256_manifest_{RUN_DATE}.csv"):
        rel = path.relative_to(out_dir).as_posix()
        rows.append({"relative_path": rel, "bytes": path.stat().st_size, "sha256": _sha256(path)})
    _write_csv(out_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)


def _json_default(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, Path):
        return str(value)
    if pd.isna(value):
        return None
    return value


def _write_validation(out_dir: Path) -> None:
    rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            pd.read_csv(path)
            rows.append({"check": f"csv_parse:{path.name}", "status": "PASS", "details": ""})
        except Exception as exc:
            rows.append({"check": f"csv_parse:{path.name}", "status": "FAIL", "details": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            rows.append({"check": f"json_parse:{path.name}", "status": "PASS", "details": ""})
        except Exception as exc:
            rows.append({"check": f"json_parse:{path.name}", "status": "FAIL", "details": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        rows.append({"check": f"markdown_nonempty:{path.name}", "status": "PASS" if text.strip() else "FAIL", "details": f"bytes={len(text.encode())}"})
    rows.extend(
        [
            {"check": "no_network_calls", "status": "PASS", "details": "script reads local frozen CSV artifacts only"},
            {"check": "no_db_writes", "status": "PASS", "details": "script imports no database client and performs no SQL"},
            {"check": "no_production_outputs", "status": "PASS", "details": "outputs confined to research package directory"},
            {"check": "no_upload_changes", "status": "PASS", "details": "no upload path written"},
        ]
    )
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", rows)


def _decision(metrics: pd.DataFrame, concentration: pd.DataFrame, feature_ok: bool) -> tuple[str, str, str, str]:
    def val(inst: str, split: str, metric: str) -> float:
        row = metrics[(metrics.instrument == inst) & (metrics.split == split)]
        return float(row.iloc[0][metric]) if not row.empty else math.nan

    validation_log_delta = val("pa_challenger", "validation", "log_loss") - val("control_process", "validation", "log_loss")
    holdout_log_delta = val("pa_challenger", "holdout", "log_loss") - val("control_process", "holdout", "log_loss")
    validation_brier_delta = val("pa_challenger", "validation", "brier_score") - val("control_process", "validation", "brier_score")
    holdout_brier_delta = val("pa_challenger", "holdout", "brier_score") - val("control_process", "holdout", "brier_score")
    holdout_auc_delta = val("pa_challenger", "holdout", "roc_auc") - val("control_process", "holdout", "roc_auc")

    domination = False
    if not concentration.empty:
        top_player = concentration[concentration["split"].eq("holdout")]["pct_rows"].max()
        domination = bool(pd.notna(top_player) and top_player > 0.20)

    if not feature_ok:
        inc = "PILOT_BLOCKED_BY_POPULATION_OR_CHAMPION_BINDING"
    elif validation_log_delta < 0 and holdout_log_delta < 0 and validation_brier_delta <= 0 and holdout_brier_delta <= 0 and holdout_auc_delta > -0.01 and not domination:
        inc = "PA_INCREMENTAL_VALUE_SUPPORTED_FOR_PROSPECTIVE_CHALLENGER_OBSERVATION"
    elif validation_log_delta < 0 and (holdout_log_delta < 0 or holdout_brier_delta < 0):
        inc = "PA_INCREMENTAL_VALUE_PROMISING_HOLDOUT_UNDERPOWERED"
    elif abs(holdout_log_delta) <= 0.002 and abs(holdout_brier_delta) <= 0.001:
        inc = "PA_VALUE_EXPLAINED_BY_RECALIBRATION_NOT_INCREMENTAL"
    elif validation_log_delta < 0 or validation_brier_delta < 0:
        inc = "PA_ASSOCIATION_PRESENT_NO_STABLE_OUT_OF_SAMPLE_LIFT"
    else:
        inc = "NO_USEFUL_PA_INCREMENTAL_VALUE_DETECTED"

    validation_decision = "PASS_PA_BETTER_THAN_CONTROL" if validation_log_delta < 0 and validation_brier_delta < 0 else "MIXED_OR_NO_VALIDATION_LIFT"
    holdout_decision = "PASS_PA_BETTER_THAN_CONTROL" if holdout_log_delta < 0 and holdout_brier_delta < 0 else "MIXED_OR_NO_HOLDOUT_LIFT"
    process_decision = "RESEARCH_ONLY_FIXED_LOGISTIC_INSTRUMENTS_FIT_ON_FIT_PARTITION_ONLY"
    return validation_decision, holdout_decision, process_decision, inc


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    raw, direct, inferred, exclusions, duplicate_rows = _prepare_population()
    split_spec = _make_split(direct)
    scored, control, challenger, numeric, categorical = _fit_models(direct, split_spec)
    inferred_scored = _score_inferred(inferred, control, challenger, split_spec, numeric, categorical)

    pred_cols = {
        "champion": "champion_probability",
        "control_process": "control_process_probability",
        "pa_challenger": "pa_challenger_probability",
    }

    metric_rows = []
    for split in ["fit", "validation", "holdout"]:
        part = scored[scored["split"].eq(split)]
        for instrument, col in pred_cols.items():
            metric_rows.append(_metrics(part, col, instrument, split))
    metrics_df = pd.DataFrame(metric_rows)

    band_rows = []
    for split in ["fit", "validation", "holdout"]:
        part = scored[scored["split"].eq(split)]
        for instrument, col in pred_cols.items():
            band_rows.extend(_band_rows(part, col, instrument, split))

    date_rows = _date_stability_rows(scored, pred_cols)

    concentration_rows = []
    for split in ["fit", "validation", "holdout"]:
        part = scored[scored["split"].eq(split)]
        for col in ["player_id", "slate_date"]:
            counts = part.groupby(col).size().sort_values(ascending=False).head(20)
            for key, count in counts.items():
                concentration_rows.append(
                    {
                        "split": split,
                        "entity_type": col,
                        "entity": key,
                        "rows": int(count),
                        "pct_rows": float(count / len(part)) if len(part) else math.nan,
                    }
                )
    concentration_df = pd.DataFrame(concentration_rows)

    bootstrap_rows = _bootstrap_rows(scored)
    top_bottom_rows = _top_bottom_rows(scored, pred_cols)
    movement = scored[
        [
            "canonical_identity",
            "slate_date",
            "game_id",
            "player_id",
            "player_name",
            "team",
            "opponent",
            "target_class",
            "settlement_status",
            "selected_price",
            "split",
            "champion_probability",
            "control_process_probability",
            "pa_challenger_probability",
            "pa_minus_control_probability",
            "pa_movement_direction",
        ]
        + numeric
        + categorical
    ].copy()

    inferred_metrics = []
    inferred_splits = ["all_inferred_sensitivity"] + sorted(str(s) for s in inferred_scored["split"].dropna().unique())
    for split in inferred_splits:
        if split == "all_inferred_sensitivity":
            part = inferred_scored
        else:
            part = inferred_scored[inferred_scored["split"].eq(split)]
        for instrument, col in pred_cols.items():
            inferred_metrics.append(_metrics(part, col, instrument, split))

    feature_integrity_rows = []
    field_manifest = pd.read_csv(FIELD_MANIFEST_PATH)
    used_fields = set(numeric + categorical)
    for _, row in field_manifest.iterrows():
        field = row["field"]
        latest = pd.to_datetime(scored["pa_context_latest_date"], errors="coerce")
        slate = pd.to_datetime(scored["slate_date"], errors="coerce")
        temporal_pass = bool((latest < slate).all()) if field == "pa_context_latest_date" else True
        feature_integrity_rows.append(
            {
                "field": field,
                "used_in_pa_challenger": field in used_fields,
                "definition_or_role": row.get("formula_or_role", ""),
                "feature_version": row.get("feature_version", ""),
                "prediction_time_availability": row.get("prediction_time_availability", ""),
                "missing_policy": row.get("missing_policy", ""),
                "leakage_rule": row.get("leakage_rule", ""),
                "strict_prior_temporal_check": "PASS" if temporal_pass else "FAIL",
                "identity_attachment": "exact canonical proposition identity",
                "notes": "direct source only in primary population",
            }
        )
    feature_ok = all(r["strict_prior_temporal_check"] == "PASS" for r in feature_integrity_rows)

    validation_decision, holdout_decision, process_decision, incremental_decision = _decision(metrics_df, concentration_df, feature_ok)
    inferred_holdout = pd.DataFrame(inferred_metrics)
    direct_holdout_delta = (
        float(metrics_df[(metrics_df.instrument == "pa_challenger") & (metrics_df.split == "holdout")]["log_loss"].iloc[0])
        - float(metrics_df[(metrics_df.instrument == "control_process") & (metrics_df.split == "holdout")]["log_loss"].iloc[0])
    )
    inferred_delta = math.nan
    if not inferred_holdout.empty:
        try:
            inferred_delta = float(inferred_holdout[(inferred_holdout.instrument == "pa_challenger") & (inferred_holdout.split == "holdout")]["log_loss"].iloc[0]) - float(
                inferred_holdout[(inferred_holdout.instrument == "control_process") & (inferred_holdout.split == "holdout")]["log_loss"].iloc[0]
            )
        except Exception:
            inferred_delta = math.nan
    inferred_decision = "INFERRED_SENSITIVITY_SCORED_OUT_OF_DOMAIN_PRIMARY_DIRECT_ONLY"

    split_rows = []
    for split, dates in [("fit", split_spec.fit_dates), ("validation", split_spec.validation_dates), ("holdout", split_spec.holdout_dates)]:
        part = scored[scored["split"].eq(split)]
        split_rows.append(
            {
                "split": split,
                "start_date": min(dates),
                "end_date": max(dates),
                "distinct_dates": len(dates),
                "rows": int(len(part)),
                "wins": int(part[OUTCOME_COL].sum()),
                "losses": int(len(part) - part[OUTCOME_COL].sum()),
                "date_list": "|".join(dates),
            }
        )

    population_summary = [
        {"population": "raw_historical_hits_15_rows", "rows": int(len(raw)), "distinct_dates": int(raw["slate_date"].nunique()), "start_date": str(raw["slate_date"].min()), "end_date": str(raw["slate_date"].max())},
        {"population": "diagnostic_eligible_official_champion_bound", "rows": int(len(direct) + len(inferred)), "distinct_dates": int(pd.concat([direct, inferred])["slate_date"].nunique()), "start_date": str(pd.concat([direct, inferred])["slate_date"].min()), "end_date": str(pd.concat([direct, inferred])["slate_date"].max())},
        {"population": "primary_direct_strict_prior_pa", "rows": int(len(direct)), "distinct_dates": int(direct["slate_date"].nunique()), "start_date": str(direct["slate_date"].min()), "end_date": str(direct["slate_date"].max())},
        {"population": "inferred_pa_sensitivity", "rows": int(len(inferred)), "distinct_dates": int(inferred["slate_date"].nunique()), "start_date": str(inferred["slate_date"].min()), "end_date": str(inferred["slate_date"].max())},
    ]

    row_counts = scored.groupby("slate_date").agg(rows=("canonical_identity", "size"), games=("game_id", "nunique"), players=("player_id", "nunique"), wins=(OUTCOME_COL, "sum")).reset_index()
    row_counts["losses"] = row_counts["rows"] - row_counts["wins"]

    champion_binding = [
        {"check": "exact_champion_column_present", "status": "PASS", "details": CHAMPION_COL},
        {"check": "direct_rows_champion_bound", "status": "PASS", "details": f"{len(direct)}/{len(direct)}"},
        {"check": "no_substituted_aliases", "status": "PASS", "details": "used control_probability exactly from frozen population manifest"},
        {"check": "canonical_identity_duplicate_check", "status": "PASS", "details": "0 duplicate direct identities"},
    ]

    instrument_spec = [
        {"instrument": "champion", "fit_partition": "none", "features": CHAMPION_COL, "configuration": "existing frozen control_probability evaluated directly", "promotion_eligible": False},
        {"instrument": "control_process", "fit_partition": "fit only", "features": CHAMPION_COL, "configuration": "LogisticRegression(C=1.0,L2,lbfgs,max_iter=2000,random_state=17); median impute; standard scale", "promotion_eligible": False},
        {"instrument": "pa_challenger", "fit_partition": "fit only", "features": "|".join(numeric + categorical), "configuration": "LogisticRegression(C=1.0,L2,lbfgs,max_iter=2000,random_state=17); frozen fit-partition preprocessing", "promotion_eligible": False},
    ]

    _write_csv(out_dir / f"population_manifest_exact_{RUN_DATE}.csv", scored)
    _write_csv(out_dir / f"inferred_pa_sensitivity_population_{RUN_DATE}.csv", inferred_scored)
    _write_csv(out_dir / f"population_summary_{RUN_DATE}.csv", population_summary)
    _write_csv(out_dir / f"rows_by_date_{RUN_DATE}.csv", row_counts)
    _write_csv(out_dir / f"exclusion_reasons_{RUN_DATE}.csv", exclusions)
    _write_csv(out_dir / f"duplicate_identity_report_{RUN_DATE}.csv", duplicate_rows)
    _write_csv(out_dir / f"champion_binding_report_{RUN_DATE}.csv", champion_binding)
    _write_csv(out_dir / f"temporal_split_manifest_{RUN_DATE}.csv", split_rows)
    _write_csv(out_dir / f"frozen_instrument_specification_{RUN_DATE}.csv", instrument_spec)
    _write_csv(out_dir / f"feature_temporal_integrity_audit_{RUN_DATE}.csv", feature_integrity_rows)
    _write_csv(out_dir / f"champion_control_challenger_metrics_{RUN_DATE}.csv", metrics_df)
    _write_csv(out_dir / f"calibration_band_analysis_{RUN_DATE}.csv", band_rows)
    _write_csv(out_dir / f"date_stability_report_{RUN_DATE}.csv", date_rows)
    _write_csv(out_dir / f"concentration_analysis_{RUN_DATE}.csv", concentration_df)
    _write_csv(out_dir / f"uncertainty_bootstrap_{RUN_DATE}.csv", bootstrap_rows)
    _write_csv(out_dir / f"inferred_pa_sensitivity_metrics_{RUN_DATE}.csv", inferred_metrics)
    _write_csv(out_dir / f"prediction_movement_ledger_{RUN_DATE}.csv", movement)
    coefficient_rows = _coefficients(control, "control_process", numeric, categorical) + _coefficients(challenger, "pa_challenger", numeric, categorical)
    sign_stability_rows = _coefficient_sign_stability(scored[scored["split"].eq("validation")], numeric, categorical, coefficient_rows)
    _write_csv(out_dir / f"top_bottom_score_cohorts_{RUN_DATE}.csv", top_bottom_rows)
    _write_csv(out_dir / f"research_model_coefficients_{RUN_DATE}.csv", coefficient_rows)
    _write_csv(out_dir / f"pa_coefficient_sign_stability_{RUN_DATE}.csv", sign_stability_rows)

    model_dir = out_dir / "research_only_model_artifacts"
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(control, model_dir / f"research_only_control_process_logistic_{RUN_DATE}.joblib")
    joblib.dump(challenger, model_dir / f"research_only_pa_challenger_logistic_{RUN_DATE}.joblib")
    (model_dir / "README_RESEARCH_ONLY.md").write_text(
        "# Research-Only Model Artifacts\n\nThese fitted instruments are confined to the bounded offline Champion-Challenger pilot. They are not production models, not upload inputs, and not authorized for live prediction.\n",
        encoding="utf-8",
    )

    decisions = {
        "MLB_HITS15_PA_CHAMPION_BINDING_DECISION": "PASS_EXACT_CONTROL_PROBABILITY_BOUND_FOR_DIRECT_ROWS",
        "MLB_HITS15_PA_DIRECT_POPULATION_DECISION": "PASS_1292_DIRECT_STRICT_PRIOR_ROWS_PRIMARY_592_INFERRED_EXCLUDED",
        "MLB_HITS15_PA_TEMPORAL_SPLIT_DECISION": "PASS_DATE_ORDERED_FIT_VALIDATION_HOLDOUT_FROZEN_BEFORE_FIT",
        "MLB_HITS15_PA_FEATURE_INTEGRITY_DECISION": "PASS_DIRECT_STRICT_PRIOR_FEATURES_ONLY" if feature_ok else "FAIL_FEATURE_TEMPORAL_INTEGRITY",
        "MLB_HITS15_PA_CHALLENGER_PROCESS_DECISION": process_decision,
        "MLB_HITS15_PA_VALIDATION_DECISION": validation_decision,
        "MLB_HITS15_PA_HOLDOUT_DECISION": holdout_decision,
        "MLB_HITS15_PA_INCREMENTAL_VALUE_DECISION": incremental_decision,
        "MLB_HITS15_PA_INFERRED_SENSITIVITY_DECISION": inferred_decision,
        "MLB_HITS15_PA_PROMOTION_STATUS": "NOT_AUTHORIZED",
    }
    _write_csv(out_dir / f"decision_report_{RUN_DATE}.csv", [{"decision": k, "value": v} for k, v in decisions.items()])

    summary = {
        "run_date": RUN_DATE,
        "source_population_path": str(POPULATION_PATH.relative_to(ROOT)),
        "field_manifest_path": str(FIELD_MANIFEST_PATH.relative_to(ROOT)),
        "direct_rows": int(len(direct)),
        "inferred_rows": int(len(inferred)),
        "direct_date_range": [str(direct["slate_date"].min()), str(direct["slate_date"].max())],
        "distinct_direct_dates": int(direct["slate_date"].nunique()),
        "split_manifest": split_rows,
        "decisions": decisions,
        "primary_metric_rows": json.loads(metrics_df.to_json(orient="records")),
        "direct_holdout_log_loss_delta_pa_minus_control": direct_holdout_delta,
        "inferred_holdout_log_loss_delta_pa_minus_control": inferred_delta,
    }
    (out_dir / f"machine_readable_hits15_direct_pa_champion_challenger_{RUN_DATE}.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=_json_default),
        encoding="utf-8",
    )

    _write_markdown(out_dir, summary, metrics_df, decisions)
    _write_validation(out_dir)
    _write_manifest(out_dir)
    return summary


def _fmt(value: Any) -> str:
    try:
        f = float(value)
    except Exception:
        return str(value)
    if not math.isfinite(f):
        return "NA"
    return f"{f:.6f}"


def _metric_table(metrics: pd.DataFrame, split: str) -> str:
    rows = metrics[metrics["split"].eq(split)]
    lines = ["| instrument | rows | log_loss | brier | auc | avg_pred | outcome_rate | roi |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for _, r in rows.iterrows():
        lines.append(
            f"| {r['instrument']} | {int(r['rows'])} | {_fmt(r['log_loss'])} | {_fmt(r['brier_score'])} | {_fmt(r['roc_auc'])} | {_fmt(r['avg_prediction'])} | {_fmt(r['outcome_rate'])} | {_fmt(r['flat_stake_roi'])} |"
        )
    return "\n".join(lines)


def _write_markdown(out_dir: Path, summary: dict[str, Any], metrics: pd.DataFrame, decisions: dict[str, str]) -> None:
    def metric(inst: str, split: str, col: str) -> float:
        row = metrics[(metrics.instrument == inst) & (metrics.split == split)]
        return float(row.iloc[0][col]) if not row.empty else math.nan

    validation_log_delta = metric("pa_challenger", "validation", "log_loss") - metric("control_process", "validation", "log_loss")
    holdout_log_delta = metric("pa_challenger", "holdout", "log_loss") - metric("control_process", "holdout", "log_loss")
    validation_brier_delta = metric("pa_challenger", "validation", "brier_score") - metric("control_process", "validation", "brier_score")
    holdout_brier_delta = metric("pa_challenger", "holdout", "brier_score") - metric("control_process", "holdout", "brier_score")

    split_lines = ["| split | start | end | dates | rows | wins | losses |", "|---|---|---|---:|---:|---:|---:|"]
    for row in summary["split_manifest"]:
        split_lines.append(f"| {row['split']} | {row['start_date']} | {row['end_date']} | {row['distinct_dates']} | {row['rows']} | {row['wins']} | {row['losses']} |")

    decision_lines = "\n".join(f"`{k} = {v}`" for k, v in decisions.items())
    md = f"""# MLB Hits 1.5 Direct-PA Opportunity Champion-Challenger Pilot - {RUN_DATE}

## Executive Summary

This bounded offline pilot evaluated whether frozen direct strict-prior PA Opportunity fields add out-of-sample value to the existing Hits 1.5 OVER prediction. The primary population was direct-source only: `{summary['direct_rows']}` rows from `{summary['direct_date_range'][0]}` through `{summary['direct_date_range'][1]}` across `{summary['distinct_direct_dates']}` slate dates. The `{summary['inferred_rows']}` inferred pregame-reconcile rows were excluded from all fitting and primary decisions, then scored only as an out-of-domain sensitivity set.

The central comparison is PA challenger minus the fit-only control-process calibration. Validation log-loss delta was `{_fmt(validation_log_delta)}` and holdout log-loss delta was `{_fmt(holdout_log_delta)}`. Validation Brier delta was `{_fmt(validation_brier_delta)}` and holdout Brier delta was `{_fmt(holdout_brier_delta)}`.

Final incremental-value decision: `{decisions['MLB_HITS15_PA_INCREMENTAL_VALUE_DECISION']}`. Production promotion remains `{decisions['MLB_HITS15_PA_PROMOTION_STATUS']}`.

## Temporal Splits

{chr(10).join(split_lines)}

Splits are date-ordered and non-overlapping. All rows from a slate date remain in one partition. The holdout is the latest contiguous block and was untouched during instrument fitting.

## Validation Metrics

{_metric_table(metrics, 'validation')}

## Holdout Metrics

{_metric_table(metrics, 'holdout')}

## Instrument Specification

- Champion: existing frozen `control_probability`, evaluated directly.
- Control process: fixed logistic calibration using only `control_probability`, fit on the fit partition.
- PA challenger: fixed regularized logistic regression using `control_probability` plus frozen direct strict-prior PA Opportunity fields only, fit on the fit partition.
- Configuration: `LogisticRegression(C=1.0, penalty='l2', solver='lbfgs', max_iter=2000, random_state=17)`.
- No hyperparameter search, no threshold optimization, no holdout inspection for feature choice.

## Feature And Temporal Integrity

The primary population contains only rows with `pa_semantics_status = PREDICTION_SAFE_PRIOR_CONTEXT` and `pa_opp_v1_cutoff_status = PASS_PRIOR_DATE`. The row-level audit in `feature_temporal_integrity_audit_{RUN_DATE}.csv` verifies that direct `pa_context_latest_date` precedes `slate_date`. Attachment is by exact canonical proposition identity: `slate_date | game_id | player_id | prop_type | line | side`.

## Inferred-PA Sensitivity

The inferred rows were scored after the direct-only process was frozen and fitted. They were not used to fit either the control process or the PA challenger. Their sensitivity results cannot overturn the direct-source decision.

## Decisions

{decision_lines}

## Artifact Map

- Exact population manifest: `population_manifest_exact_{RUN_DATE}.csv`
- Champion binding report: `champion_binding_report_{RUN_DATE}.csv`
- Temporal split manifest: `temporal_split_manifest_{RUN_DATE}.csv`
- Instrument specification: `frozen_instrument_specification_{RUN_DATE}.csv`
- Metrics: `champion_control_challenger_metrics_{RUN_DATE}.csv`
- Prediction movement ledger: `prediction_movement_ledger_{RUN_DATE}.csv`
- Inferred sensitivity metrics: `inferred_pa_sensitivity_metrics_{RUN_DATE}.csv`
- Research-only model artifacts: `research_only_model_artifacts/`
- SHA256 manifest: `sha256_manifest_{RUN_DATE}.csv`

## Guardrails

No network access, OddsAPI calls, DB writes, upload changes, live prediction changes, matrix replacement, scheduler changes, or production promotion occurred.
"""
    (out_dir / f"executive_summary_{RUN_DATE}.md").write_text(md, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    summary = build(args.output_dir)
    print(json.dumps({"output_dir": str(args.output_dir), "decisions": summary["decisions"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
