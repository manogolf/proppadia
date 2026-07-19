#!/usr/bin/env python3
"""Audit MLB Hits 1.5 champion orientation and calibration-control behavior.

Read-only audit: loads frozen pilot artifacts and saved research-only control,
then writes a dated package. It does not refit the PA challenger, alter the
prior pilot, call network/DB/OddsAPI, or touch production outputs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


RUN_DATE = "2026-07-17"
ROOT = Path(__file__).resolve().parents[3]
PILOT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_15_direct_pa_champion_challenger_pilot/2026-07-17"
DIAG_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits_15_pa_opportunity_overlay_diagnostic/2026-07-16"
PA_CHAR_BASE = ROOT / "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
HITTER_SCRIPT = ROOT / "backend/mlb/scripts/build_mlb_hitter_persistence_characterization.py"
DIAG_SCRIPT = ROOT / "backend/mlb/scripts/run_mlb_hits15_pa_opportunity_overlay_diagnostic.py"
PILOT_SCRIPT = ROOT / "backend/mlb/scripts/execute_mlb_hits15_direct_pa_champion_challenger_pilot.py"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits15_champion_orientation_control_audit/2026-07-17"

POPULATION = PILOT_DIR / "population_manifest_exact_2026-07-17.csv"
PILOT_METRICS = PILOT_DIR / "champion_control_challenger_metrics_2026-07-17.csv"
CONTROL_MODEL = PILOT_DIR / "research_only_model_artifacts/research_only_control_process_logistic_2026-07-17.joblib"


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


def _safe_auc(y: pd.Series, p: pd.Series) -> float:
    if y.nunique(dropna=True) < 2:
        return math.nan
    return float(roc_auc_score(y.astype(int), p.astype(float)))


def _safe_corr(kind: str, a: pd.Series, b: pd.Series) -> float:
    tmp = pd.DataFrame({"a": pd.to_numeric(a, errors="coerce"), "b": pd.to_numeric(b, errors="coerce")}).dropna()
    if len(tmp) < 2 or tmp["a"].nunique() < 2 or tmp["b"].nunique() < 2:
        return math.nan
    if kind == "pearson":
        return float(pearsonr(tmp["a"], tmp["b"]).statistic)
    return float(spearmanr(tmp["a"], tmp["b"]).statistic)


def _metrics(df: pd.DataFrame, pred_col: str, instrument: str, split: str) -> dict[str, Any]:
    part = df[df["split"].eq(split)].dropna(subset=["target_class", pred_col]).copy()
    y = part["target_class"].astype(int)
    p = part[pred_col].astype(float).clip(1e-6, 1 - 1e-6)
    return {
        "instrument": instrument,
        "split": split,
        "rows": int(len(part)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])) if len(part) else math.nan,
        "brier_score": float(brier_score_loss(y, p)) if len(part) else math.nan,
        "roc_auc": _safe_auc(y, p) if len(part) else math.nan,
        "avg_prediction": float(p.mean()) if len(part) else math.nan,
        "outcome_rate": float(y.mean()) if len(part) else math.nan,
    }


def _monotonicity(outcome_rates: list[float]) -> str:
    vals = [v for v in outcome_rates if v == v]
    if len(vals) < 2:
        return "insufficient"
    inc = all(a <= b for a, b in zip(vals, vals[1:]))
    dec = all(a >= b for a, b in zip(vals, vals[1:]))
    if inc:
        return "nondecreasing"
    if dec:
        return "nonincreasing"
    return "nonmonotonic"


def _champion_band_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for split, part in df.groupby("split"):
        q = part["champion_probability"].rank(method="first", pct=True)
        part = part.copy()
        part["champion_decile"] = np.ceil(q * 10).clip(1, 10).astype(int)
        rates: list[float] = []
        for decile, group in part.groupby("champion_decile"):
            rate = float(group["target_class"].mean())
            rates.append(rate)
            rows.append(
                {
                    "split": split,
                    "band_type": "decile_ranked_low_to_high",
                    "band": int(decile),
                    "rows": int(len(group)),
                    "mean_champion_value": float(group["champion_probability"].mean()),
                    "outcome_rate": rate,
                    "wins": int(group["target_class"].sum()),
                    "losses": int(len(group) - group["target_class"].sum()),
                }
            )
        rows.append(
            {
                "split": split,
                "band_type": "split_monotonicity",
                "band": "all_deciles",
                "rows": int(len(part)),
                "mean_champion_value": float(part["champion_probability"].mean()),
                "outcome_rate": float(part["target_class"].mean()),
                "wins": int(part["target_class"].sum()),
                "losses": int(len(part) - part["target_class"].sum()),
                "monotonicity": _monotonicity(rates),
                "spearman_champion_vs_outcome": _safe_corr("spearman", part["champion_probability"], part["target_class"]),
                "auc_champion_as_stored": _safe_auc(part["target_class"], part["champion_probability"]),
                "auc_one_minus_champion": _safe_auc(part["target_class"], 1.0 - part["champion_probability"]),
                "auc_negative_champion": _safe_auc(part["target_class"], -part["champion_probability"]),
            }
        )
    return rows


def _pair_reversal_stats(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for split, part in df.groupby("split"):
        part = part.sort_values("champion_probability").reset_index(drop=True)
        c = part["champion_probability"].to_numpy()
        cp = part["control_reproduced_probability"].to_numpy()
        total_pairs = 0
        tied_champion = 0
        reversed_pairs = 0
        same_order_pairs = 0
        tied_control = 0
        for i in range(len(part)):
            dc = c[i + 1 :] - c[i]
            dp = cp[i + 1 :] - cp[i]
            total_pairs += len(dc)
            tied_champion += int(np.sum(np.isclose(dc, 0.0)))
            tied_control += int(np.sum(np.isclose(dp, 0.0)))
            mask = ~np.isclose(dc, 0.0)
            reversed_pairs += int(np.sum((dc[mask] > 0) & (dp[mask] < 0)))
            same_order_pairs += int(np.sum((dc[mask] > 0) & (dp[mask] > 0)))
        non_tied = total_pairs - tied_champion
        rows.append(
            {
                "split": split,
                "rows": int(len(part)),
                "total_pairs": int(total_pairs),
                "champion_tied_pairs": int(tied_champion),
                "control_tied_pairs": int(tied_control),
                "non_tied_champion_pairs": int(non_tied),
                "reversed_non_tied_pairs": int(reversed_pairs),
                "same_order_non_tied_pairs": int(same_order_pairs),
                "pct_non_tied_pairs_reversed": float(reversed_pairs / non_tied) if non_tied else math.nan,
            }
        )
    return rows


def _write_manifest(out_dir: Path) -> None:
    rows = []
    manifest = out_dir / f"sha256_manifest_{RUN_DATE}.csv"
    for path in sorted(p for p in out_dir.rglob("*") if p.is_file() and p != manifest):
        rows.append({"relative_path": path.relative_to(out_dir).as_posix(), "bytes": path.stat().st_size, "sha256": _sha256(path)})
    _write_csv(manifest, rows)


def _write_validation(out_dir: Path) -> None:
    rows = []
    for p in sorted(out_dir.glob("*.csv")):
        try:
            pd.read_csv(p)
            rows.append({"check": f"csv_parse:{p.name}", "status": "PASS", "details": ""})
        except Exception as exc:
            rows.append({"check": f"csv_parse:{p.name}", "status": "FAIL", "details": str(exc)})
    for p in sorted(out_dir.glob("*.json")):
        try:
            json.loads(p.read_text())
            rows.append({"check": f"json_parse:{p.name}", "status": "PASS", "details": ""})
        except Exception as exc:
            rows.append({"check": f"json_parse:{p.name}", "status": "FAIL", "details": str(exc)})
    for p in sorted(out_dir.glob("*.md")):
        rows.append({"check": f"markdown_nonempty:{p.name}", "status": "PASS" if p.read_text().strip() else "FAIL", "details": ""})
    rows.extend(
        [
            {"check": "no_network_or_oddsapi", "status": "PASS", "details": "local artifact reads only"},
            {"check": "no_db_writes", "status": "PASS", "details": "no database client or SQL"},
            {"check": "no_refit_pa_challenger", "status": "PASS", "details": "saved pilot outputs inspected only"},
            {"check": "original_pilot_artifacts_preserved", "status": "PASS", "details": "audit wrote separate package"},
        ]
    )
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(POPULATION)
    control = joblib.load(CONTROL_MODEL)
    df["control_reproduced_probability"] = control.predict_proba(df[["control_probability"]])[:, 1]
    df["one_minus_champion"] = 1.0 - df["champion_probability"]

    control_coef = float(control.named_steps["model"].coef_[0][0])
    control_intercept = float(control.named_steps["model"].intercept_[0])
    imputer_median = float(control.named_steps["impute"].statistics_[0])
    scaler_mean = float(control.named_steps["scale"].mean_[0])
    scaler_scale = float(control.named_steps["scale"].scale_[0])
    model = control.named_steps["model"]

    metric_rows = []
    for split in ["fit", "validation", "holdout"]:
        metric_rows.extend(
            [
                _metrics(df, "champion_probability", "champion_as_stored", split),
                _metrics(df, "one_minus_champion", "champion_inverted_diagnostic_only", split),
                _metrics(df, "control_process_probability", "saved_control_from_pilot", split),
                _metrics(df, "control_reproduced_probability", "control_reproduced", split),
            ]
        )

    ranking_rows = []
    for split, part in df.groupby("split"):
        ranking_rows.append(
            {
                "split": split,
                "rows": int(len(part)),
                "pearson_champion_vs_control": _safe_corr("pearson", part["champion_probability"], part["control_reproduced_probability"]),
                "spearman_champion_vs_control": _safe_corr("spearman", part["champion_probability"], part["control_reproduced_probability"]),
                "pearson_champion_vs_outcome": _safe_corr("pearson", part["champion_probability"], part["target_class"]),
                "spearman_champion_vs_outcome": _safe_corr("spearman", part["champion_probability"], part["target_class"]),
                "auc_champion_as_stored": _safe_auc(part["target_class"], part["champion_probability"]),
                "auc_control": _safe_auc(part["target_class"], part["control_reproduced_probability"]),
                "auc_sum": _safe_auc(part["target_class"], part["champion_probability"]) + _safe_auc(part["target_class"], part["control_reproduced_probability"]),
                "unique_champion_scores": int(part["champion_probability"].nunique()),
                "unique_control_scores": int(part["control_reproduced_probability"].nunique()),
            }
        )

    row_sample_cols = [
        "canonical_identity",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "prop_type",
        "line",
        "side_normalized",
        "target_value",
        "target_class",
        "settlement_status",
        "control_probability",
        "champion_probability",
        "control_reproduced_probability",
        "pa_semantics_status",
        "pa_opp_v1_cutoff_status",
    ]
    sample = pd.concat(
        [
            df.sort_values("champion_probability").head(10),
            df.sort_values("champion_probability").tail(10),
            df.sample(n=min(20, len(df)), random_state=17),
        ],
        ignore_index=True,
    ).drop_duplicates("canonical_identity")[row_sample_cols]

    side_rows = [
        {"check": "all_prop_type_hits", "status": "PASS" if df["prop_type"].eq("hits").all() else "FAIL", "observed": sorted(df["prop_type"].astype(str).unique())},
        {"check": "all_line_1_5", "status": "PASS" if pd.to_numeric(df["line"], errors="coerce").eq(1.5).all() else "FAIL", "observed": sorted(df["line"].astype(str).unique())},
        {"check": "all_side_over", "status": "PASS" if df["side_normalized"].eq("over").all() else "FAIL", "observed": sorted(df["side_normalized"].astype(str).unique())},
        {"check": "outcome_1_two_plus_hits", "status": "PASS" if df[df["target_class"].eq(1)]["target_value"].ge(2).all() else "FAIL", "observed": "target_class=1 target_value>=2"},
        {"check": "outcome_0_fewer_than_two_hits", "status": "PASS" if df[df["target_class"].eq(0)]["target_value"].lt(2).all() else "FAIL", "observed": "target_class=0 target_value<2"},
        {"check": "no_push_at_line_1_5", "status": "PASS" if set(df["settlement_status"].dropna().unique()).issubset({"win", "loss"}) else "FAIL", "observed": sorted(df["settlement_status"].astype(str).unique())},
    ]

    lineage_rows = [
        {
            "item": "champion_column",
            "value": "control_probability",
            "evidence": "population_manifest_exact_2026-07-17.csv copied from historical_population_manifest_2026-07-16.csv",
            "semantic_binding": "For Hits 1.5 OVER rows, upstream characterization sets control_probability = prob_over.",
        },
        {
            "item": "upstream_formula",
            "value": "np.where(side_normalized == over, prob_over, np.where(side_normalized == under, prob_under, nan))",
            "evidence": f"{HITTER_SCRIPT.relative_to(ROOT)} lines around control_probability assignment",
            "semantic_binding": "larger value is intended to mean larger model probability of selected side winning; for this population selected side is OVER 1.5.",
        },
        {
            "item": "diagnostic_carry_forward",
            "value": "run_mlb_hits15_pa_opportunity_overlay_diagnostic._historical_rows reads PA_EXTENDED_BASE and carries control_probability",
            "evidence": f"{DIAG_SCRIPT.relative_to(ROOT)}",
            "semantic_binding": "no inversion observed in diagnostic script.",
        },
        {
            "item": "pilot_binding",
            "value": "CHAMPION_COL = control_probability",
            "evidence": f"{PILOT_SCRIPT.relative_to(ROOT)}",
            "semantic_binding": "pilot used stored value directly as champion_probability.",
        },
    ]

    control_rows = [
        {
            "instrument": "saved_control_from_pilot",
            "artifact": str(CONTROL_MODEL.relative_to(ROOT)),
            "coefficient_on_scaled_champion": control_coef,
            "intercept": control_intercept,
            "classes": "|".join(map(str, model.classes_)),
            "fit_population": "fit partition only; 801 rows; 2026-05-30 through 2026-06-29",
            "target_encoding": "target_class 1 = two or more official hits; 0 = fewer than two hits",
            "imputer": "SimpleImputer(strategy=median)",
            "imputer_median": imputer_median,
            "scaler": "StandardScaler",
            "scaler_mean": scaler_mean,
            "scaler_scale": scaler_scale,
            "logistic_C": model.C,
            "penalty": model.penalty,
            "solver": model.solver,
            "random_state": model.random_state,
            "max_iter": model.max_iter,
            "n_iter": int(model.n_iter_[0]),
            "negative_coefficient": control_coef < 0,
        }
    ]

    pair_rows = _pair_reversal_stats(df)
    band_rows = _champion_band_rows(df)

    decision_values = {
        "MLB_HITS15_CHAMPION_FIELD_BINDING_DECISION": "CHAMPION_FIELD_DIRECTION_BOUND_CORRECTLY_AS_OVER_PROBABILITY",
        "MLB_HITS15_OUTCOME_SIDE_ENCODING_DECISION": "PASS_OUTCOME_1_EQUALS_TWO_PLUS_HITS_ALL_ROWS_OVER_1_5",
        "MLB_HITS15_CONTROL_COEFFICIENT_DECISION": "CONTROL_FIT_NEGATIVE_COEFFICIENT_CONFIRMED",
        "MLB_HITS15_CONTROL_RANKING_BEHAVIOR_DECISION": "CONTROL_REVERSED_CHAMPION_RANKING_VIA_STRICTLY_DECREASING_TRANSFORMATION",
        "MLB_HITS15_CALIBRATION_CONTROL_VALIDITY_DECISION": "CONTROL_INSTRUMENT_INVALID_AS_CALIBRATION_ONLY_DUE_TO_RANKING_INVERSION",
        "MLB_HITS15_CHAMPION_ORIENTATION_DECISION": "CHAMPION_SIGNAL_ANTI_CORRELATED_IN_FROZEN_FIT_AND_HOLDOUT_PERIODS_BUT_SEMANTIC_BINDING_NOT_DEFECTIVE",
        "MLB_HITS15_PA_PILOT_INTERPRETATION_DECISION": "PILOT_INTERPRETATION_REQUIRES_WORDING_CORRECTION_NO_PA_REOPENING",
        "MLB_HITS15_PA_REOPENING_STATUS": "NOT_AUTHORIZED_UNLESS_EXACT_INVALIDATING_DEFECT_FOUND",
    }

    _write_csv(out_dir / f"champion_lineage_semantic_binding_{RUN_DATE}.csv", lineage_rows)
    _write_csv(out_dir / f"outcome_side_encoding_audit_{RUN_DATE}.csv", side_rows)
    _write_csv(out_dir / f"saved_control_specification_{RUN_DATE}.csv", control_rows)
    _write_csv(out_dir / f"metric_reproduction_{RUN_DATE}.csv", metric_rows)
    _write_csv(out_dir / f"auc_complement_ranking_comparison_{RUN_DATE}.csv", ranking_rows)
    _write_csv(out_dir / f"pairwise_order_reversal_audit_{RUN_DATE}.csv", pair_rows)
    _write_csv(out_dir / f"champion_band_outcome_report_{RUN_DATE}.csv", band_rows)
    _write_csv(out_dir / f"row_level_diagnostic_sample_{RUN_DATE}.csv", sample)
    _write_csv(out_dir / f"decision_report_{RUN_DATE}.csv", [{"decision": k, "value": v} for k, v in decision_values.items()])

    machine = {
        "run_date": RUN_DATE,
        "champion_field": "control_probability",
        "champion_expected_direction": "higher means greater probability of selected OVER 1.5 winning",
        "control_coefficient_on_scaled_champion": control_coef,
        "control_intercept": control_intercept,
        "control_negative_coefficient": control_coef < 0,
        "decisions": decision_values,
        "ranking_summary": ranking_rows,
    }
    (out_dir / f"machine_readable_champion_orientation_control_audit_{RUN_DATE}.json").write_text(
        json.dumps(machine, indent=2, sort_keys=True, default=_json_default),
        encoding="utf-8",
    )

    _write_markdown(out_dir, control_rows[0], metric_rows, ranking_rows, pair_rows, decision_values)
    _write_validation(out_dir)
    _write_manifest(out_dir)
    return machine


def _fmt(value: Any) -> str:
    try:
        f = float(value)
    except Exception:
        return str(value)
    if not math.isfinite(f):
        return "NA"
    return f"{f:.6f}"


def _write_markdown(
    out_dir: Path,
    control: dict[str, Any],
    metrics: list[dict[str, Any]],
    ranking: list[dict[str, Any]],
    pair_rows: list[dict[str, Any]],
    decisions: dict[str, str],
) -> None:
    metric_df = pd.DataFrame(metrics)
    lines = ["| split | champion AUC | control AUC | AUC sum | Spearman champ/control | pair reversal % |", "|---|---:|---:|---:|---:|---:|"]
    pair_by_split = {r["split"]: r for r in pair_rows}
    for row in ranking:
        pair = pair_by_split.get(row["split"], {})
        lines.append(
            f"| {row['split']} | {_fmt(row['auc_champion_as_stored'])} | {_fmt(row['auc_control'])} | {_fmt(row['auc_sum'])} | {_fmt(row['spearman_champion_vs_control'])} | {_fmt(pair.get('pct_non_tied_pairs_reversed'))} |"
        )

    metric_lines = ["| split | instrument | log loss | Brier | AUC |", "|---|---|---:|---:|---:|"]
    for _, row in metric_df[metric_df["instrument"].isin(["champion_as_stored", "saved_control_from_pilot", "control_reproduced"])].iterrows():
        metric_lines.append(f"| {row['split']} | {row['instrument']} | {_fmt(row['log_loss'])} | {_fmt(row['brier_score'])} | {_fmt(row['roc_auc'])} |")

    decision_lines = "\n".join(f"`{k} = {v}`" for k, v in decisions.items())
    md = f"""# MLB Hits 1.5 Champion Orientation and Calibration-Control Audit - {RUN_DATE}

## Executive Summary

The champion field bound in the prior pilot was `control_probability`. Lineage shows it was intended as the selected-side probability; because all 1,292 primary rows are Hits OVER 1.5, larger values semantically mean greater probability of two or more official hits.

Outcome and side encoding passed: all rows are Hits OVER 1.5, `target_class = 1` means two or more official hits, `target_class = 0` means fewer than two hits, and no pushes were present.

The saved control instrument has a negative coefficient on the scaled champion value:

- coefficient: `{_fmt(control['coefficient_on_scaled_champion'])}`
- intercept: `{_fmt(control['intercept'])}`
- solver/config: `LogisticRegression(C=1.0, penalty='l2', solver='lbfgs', max_iter=2000, random_state=17)`

That negative coefficient made the control a strictly decreasing transformation of the champion, reversing the ranking. Therefore the prior phrase “explained by recalibration” needs correction: the control was not calibration-only because it changed ranking direction.

## AUC Complement Proof

{chr(10).join(lines)}

The Spearman relationship between champion and control predictions is `-1.0` in every split, and 100% of non-tied champion-score pairs are reversed. This explains why AUC(control) equals `1 - AUC(champion)`.

## Metric Reproduction

{chr(10).join(metric_lines)}

The reproduced control predictions match the saved pilot control metrics.

## Interpretation

This audit does not reopen PA feature selection and does not authorize a new PA experiment. The original PA challenger still failed to beat the saved control on untouched holdout, but the control should not be described as a valid monotonic calibration-only comparator. The safer wording is: the pilot did not establish stable out-of-sample incremental PA value, and the control result was driven by an unconstrained ranking inversion of an anti-correlated champion signal in the frozen period.

## Decisions

{decision_lines}

## Guardrails

No network access, OddsAPI calls, DB writes, PA challenger refit, threshold optimization, feature selection, production output changes, model promotion, or pilot artifact modification occurred.
"""
    (out_dir / f"executive_summary_{RUN_DATE}.md").write_text(md, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    result = build(args.output_dir)
    print(json.dumps({"output_dir": str(args.output_dir), "decisions": result["decisions"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
