#!/usr/bin/env python3
"""Audit hits recency feature parity across training and prediction sources.

Reporting only: no model changes, no DB writes, no frontend changes.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_feature_parity_audit.md")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_feature_parity_audit.csv")
DEFAULT_HOLDOUT_FROM = "2026-04-09"
DEFAULT_HOLDOUT_TO = "2026-05-08"

FEATURES = ["rolling_result_avg_7", "d7_hits", "d15_hits", "d30_hits"]


def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise SystemExit("DATABASE_URL or SUPABASE_DB_URL must be set.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _metric_row(source: str, feature: str, rows: int, nonnull: int, **extra: Any) -> dict[str, Any]:
    row = {
        "source": source,
        "feature": feature,
        "rows": int(rows or 0),
        "nonnull_rows": int(nonnull or 0),
        "null_rate": None if not rows else 1.0 - (float(nonnull or 0) / float(rows)),
    }
    row.update(extra)
    return row


def _fetch_training_stats(engine, from_date: str, to_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sql = text(
        """
        WITH base AS (
          SELECT
            mt.game_date::date AS game_date,
            mt.game_id,
            mt.player_id,
            mt.rolling_result_avg_7::float8 AS rolling_result_avg_7,
            pds.d7_hits::float8 AS d7_hits,
            pds.d15_hits::float8 AS d15_hits,
            pds.d30_hits::float8 AS d30_hits
          FROM mlb.model_training_props mt
          LEFT JOIN mlb.player_derived_stats pds
            ON pds.player_id = mt.player_id
           AND pds.game_date = mt.game_date
          WHERE mt.prop_type = 'hits'
            AND mt.game_date BETWEEN :from_date AND :to_date
        )
        SELECT
          COUNT(*)::int AS rows,
          MIN(game_date)::text AS min_date,
          MAX(game_date)::text AS max_date,
          COUNT(*) FILTER (WHERE rolling_result_avg_7 IS NOT NULL)::int AS rolling_nn,
          COUNT(*) FILTER (WHERE d7_hits IS NOT NULL)::int AS d7_nn,
          COUNT(*) FILTER (WHERE d15_hits IS NOT NULL)::int AS d15_nn,
          COUNT(*) FILTER (WHERE d30_hits IS NOT NULL)::int AS d30_nn,
          COUNT(*) FILTER (
            WHERE rolling_result_avg_7 IS NOT NULL
              AND d7_hits IS NOT NULL
              AND abs(rolling_result_avg_7 - d7_hits) < 1e-9
          )::int AS rolling_eq_d7,
          AVG(abs(rolling_result_avg_7 - d7_hits)) FILTER (
            WHERE rolling_result_avg_7 IS NOT NULL AND d7_hits IS NOT NULL
          )::float8 AS mean_abs_rolling_minus_d7,
          CORR(rolling_result_avg_7, d7_hits)::float8 AS corr_rolling_d7,
          CORR(rolling_result_avg_7, d15_hits)::float8 AS corr_rolling_d15,
          CORR(rolling_result_avg_7, d30_hits)::float8 AS corr_rolling_d30
        FROM base
        """
    )
    with engine.connect() as conn:
        row = dict(conn.execute(sql, {"from_date": from_date, "to_date": to_date}).mappings().one())
    rows = int(row["rows"] or 0)
    metrics = [
        _metric_row("training:model_training_props_join_player_derived_stats", "rolling_result_avg_7", rows, row["rolling_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("training:model_training_props_join_player_derived_stats", "d7_hits", rows, row["d7_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("training:model_training_props_join_player_derived_stats", "d15_hits", rows, row["d15_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("training:model_training_props_join_player_derived_stats", "d30_hits", rows, row["d30_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        {
            "source": "training:rolling_vs_d7",
            "feature": "rolling_result_avg_7_vs_d7_hits",
            "rows": rows,
            "nonnull_rows": int(row["rolling_eq_d7"] or 0),
            "null_rate": None,
            "min_date": row["min_date"],
            "max_date": row["max_date"],
            "exact_match_rows": int(row["rolling_eq_d7"] or 0),
            "mean_abs_diff": row["mean_abs_rolling_minus_d7"],
            "corr_rolling_d7": row["corr_rolling_d7"],
            "corr_rolling_d15": row["corr_rolling_d15"],
            "corr_rolling_d30": row["corr_rolling_d30"],
        },
    ]
    return metrics, row


def _fetch_prediction_stats(engine, from_date: str, to_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sql = text(
        """
        WITH base AS (
          SELECT
            pfp.game_date::date AS game_date,
            pfp.game_id,
            pfp.player_id,
            NULLIF(pfp.features::jsonb->>'rolling_result_avg_7', '')::float8 AS pfp_rolling_result_avg_7,
            NULLIF(pfp.features::jsonb->>'d7_hits', '')::float8 AS pfp_d7_hits,
            NULLIF(pfp.features::jsonb->>'d15_hits', '')::float8 AS pfp_d15_hits,
            NULLIF(pfp.features::jsonb->>'d30_hits', '')::float8 AS pfp_d30_hits,
            pds.d7_hits::float8 AS pds_d7_hits,
            pds.d15_hits::float8 AS pds_d15_hits,
            pds.d30_hits::float8 AS pds_d30_hits
          FROM mlb.prop_features_precomputed pfp
          LEFT JOIN mlb.player_derived_stats pds
            ON pds.player_id = pfp.player_id
           AND pds.game_id = pfp.game_id
           AND pds.game_date = pfp.game_date
          WHERE pfp.prop_type = 'hits'
            AND pfp.game_date BETWEEN :from_date AND :to_date
        )
        SELECT
          COUNT(*)::int AS rows,
          MIN(game_date)::text AS min_date,
          MAX(game_date)::text AS max_date,
          COUNT(*) FILTER (WHERE pfp_rolling_result_avg_7 IS NOT NULL)::int AS pfp_rolling_nn,
          COUNT(*) FILTER (WHERE pfp_d7_hits IS NOT NULL)::int AS pfp_d7_nn,
          COUNT(*) FILTER (WHERE pfp_d15_hits IS NOT NULL)::int AS pfp_d15_nn,
          COUNT(*) FILTER (WHERE pfp_d30_hits IS NOT NULL)::int AS pfp_d30_nn,
          COUNT(*) FILTER (WHERE pds_d7_hits IS NOT NULL)::int AS pds_d7_nn,
          COUNT(*) FILTER (WHERE pds_d15_hits IS NOT NULL)::int AS pds_d15_nn,
          COUNT(*) FILTER (WHERE pds_d30_hits IS NOT NULL)::int AS pds_d30_nn,
          CORR(pds_d7_hits, pds_d15_hits)::float8 AS corr_d7_d15,
          CORR(pds_d7_hits, pds_d30_hits)::float8 AS corr_d7_d30,
          AVG(abs(pds_d7_hits - pds_d30_hits)) FILTER (
            WHERE pds_d7_hits IS NOT NULL AND pds_d30_hits IS NOT NULL
          )::float8 AS mean_abs_d7_minus_d30
        FROM base
        """
    )
    with engine.connect() as conn:
        row = dict(conn.execute(sql, {"from_date": from_date, "to_date": to_date}).mappings().one())
    rows = int(row["rows"] or 0)
    metrics = [
        _metric_row("prediction:prop_features_precomputed_json", "rolling_result_avg_7", rows, row["pfp_rolling_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("prediction:prop_features_precomputed_json", "d7_hits", rows, row["pfp_d7_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("prediction:prop_features_precomputed_json", "d15_hits", rows, row["pfp_d15_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("prediction:prop_features_precomputed_json", "d30_hits", rows, row["pfp_d30_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("prediction:player_derived_stats_runtime_hydration", "d7_hits", rows, row["pds_d7_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("prediction:player_derived_stats_runtime_hydration", "d15_hits", rows, row["pds_d15_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        _metric_row("prediction:player_derived_stats_runtime_hydration", "d30_hits", rows, row["pds_d30_nn"], min_date=row["min_date"], max_date=row["max_date"]),
        {
            "source": "prediction:derived_window_comparison",
            "feature": "d7_hits_vs_d15_d30_hits",
            "rows": rows,
            "nonnull_rows": int(row["pds_d7_nn"] or 0),
            "null_rate": None,
            "min_date": row["min_date"],
            "max_date": row["max_date"],
            "corr_d7_d15": row["corr_d7_d15"],
            "corr_d7_d30": row["corr_d7_d30"],
            "mean_abs_d7_minus_d30": row["mean_abs_d7_minus_d30"],
        },
    ]
    return metrics, row


def _metadata_stats() -> list[dict[str, Any]]:
    p = Path("backend/mlb/modeling/feature_metadata.json")
    obj = json.loads(p.read_text())
    hits = obj.get("hits") or {}
    rows = []
    for model_name, features in hits.items():
        feature_set = set(features or [])
        for feature in FEATURES:
            rows.append(
                {
                    "source": f"metadata:{model_name}",
                    "feature": feature,
                    "rows": len(features or []),
                    "nonnull_rows": int(feature in feature_set),
                    "null_rate": None,
                    "metadata_expects_feature": bool(feature in feature_set),
                    "path": str(p),
                }
            )
    return rows


def _artifact_stats() -> list[dict[str, Any]]:
    files = [
        Path("backend/mlb/data/processed/mlb_slate_output.csv"),
        Path("backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv"),
        Path("backend/mlb/data/processed/mlb_book_upload.csv"),
        Path("artifacts/analysis/mlb/execution_vs_model/2026-05-08/reconcile_rows.csv"),
    ]
    rows = []
    for path in files:
        if not path.exists():
            rows.append({"source": f"artifact:{path}", "feature": "FILE", "rows": 0, "nonnull_rows": 0, "null_rate": None, "exists": False})
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as exc:
            rows.append({"source": f"artifact:{path}", "feature": "FILE", "rows": 0, "nonnull_rows": 0, "null_rate": None, "exists": True, "error": str(exc)})
            continue
        if "prop_type" in df.columns:
            df = df[df["prop_type"].astype(str).str.lower().eq("hits")].copy()
        for feature in FEATURES:
            rows.append(
                _metric_row(
                    f"artifact:{path}",
                    feature,
                    len(df),
                    int(pd.to_numeric(df[feature], errors="coerce").notna().sum()) if feature in df.columns else 0,
                    exists=True,
                    column_present=feature in df.columns,
                )
            )
    return rows


def _write_md(path: Path, audit: pd.DataFrame, train: dict[str, Any], pred: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    def val(x: Any) -> str:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return ""
        if isinstance(x, float):
            return f"{x:.4f}"
        return str(x)

    def table(df: pd.DataFrame, cols: list[str]) -> str:
        if df.empty:
            return "_No rows._"
        lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
        for _, row in df.iterrows():
            lines.append("| " + " | ".join(val(row.get(c)).replace("|", "\\|") for c in cols) + " |")
        return "\n".join(lines)

    key = audit[audit["feature"].isin(FEATURES)].copy()
    lines = [
        "# Hits Feature Parity Audit",
        "",
        "Reporting only. No model logic, frontend, or production artifacts were changed.",
        "",
        "## Answer",
        "`rolling_result_avg_7` is a real training/runtime feature name, but for `hits` it is currently an alias of the `d7_hits` recency statistic rather than a distinct rolling calculation. It is present in `mlb.model_training_props`, expected by hits model metadata, and re-created at prediction time by `prepare_prop()` from `d7_hits` when absent from payload. It is not stored in `mlb.prop_features_precomputed` JSON, which appears to hold BvP/PvB payloads rather than the full runtime feature vector.",
        "",
        "So the repair experiment fallback from `d7_hits` was consistent with current runtime behavior, but it exposed a naming/parity smell: persisted prediction feature snapshots do not contain the alias that model metadata expects.",
        "",
        "## Coverage",
        table(
            key[
                key["source"].isin(
                    [
                        "training:model_training_props_join_player_derived_stats",
                        "prediction:prop_features_precomputed_json",
                        "prediction:player_derived_stats_runtime_hydration",
                    ]
                )
            ][["source", "feature", "rows", "nonnull_rows", "null_rate", "min_date", "max_date"]],
            ["source", "feature", "rows", "nonnull_rows", "null_rate", "min_date", "max_date"],
        ),
        "",
        "## Metadata Expectations",
        table(
            audit[audit["source"].astype(str).str.startswith("metadata:")][
                ["source", "feature", "metadata_expects_feature", "path"]
            ],
            ["source", "feature", "metadata_expects_feature", "path"],
        ),
        "",
        "## Artifact Columns",
        table(
            audit[audit["source"].astype(str).str.startswith("artifact:")][
                ["source", "feature", "rows", "column_present", "nonnull_rows", "null_rate"]
            ],
            ["source", "feature", "rows", "column_present", "nonnull_rows", "null_rate"],
        ),
        "",
        "## Feature Comparisons",
        f"- Training rolling vs d7 exact-match rows: `{train.get('rolling_eq_d7')}`",
        f"- Training mean abs rolling_result_avg_7 - d7_hits: `{val(train.get('mean_abs_rolling_minus_d7'))}`",
        f"- Training corr rolling_result_avg_7 vs d7_hits: `{val(train.get('corr_rolling_d7'))}`",
        f"- Training corr rolling_result_avg_7 vs d15_hits: `{val(train.get('corr_rolling_d15'))}`",
        f"- Training corr rolling_result_avg_7 vs d30_hits: `{val(train.get('corr_rolling_d30'))}`",
        f"- Prediction/runtime corr d7_hits vs d15_hits: `{val(pred.get('corr_d7_d15'))}`",
        f"- Prediction/runtime corr d7_hits vs d30_hits: `{val(pred.get('corr_d7_d30'))}`",
        f"- Prediction/runtime mean abs d7_hits - d30_hits: `{val(pred.get('mean_abs_d7_minus_d30'))}`",
        "",
        "## Trace",
        "- Training sync: `backend/mlb/scripts/insert_mlb_stat_derived.py` sets `model_training_props.rolling_result_avg_7 = player_derived_stats.d7_hits` for `hits`.",
        "- Model metadata: `backend/mlb/modeling/feature_metadata.json` includes both `rolling_result_avg_7` and `d7_hits` for hits random forest and logistic regression.",
        "- Prediction preparation: `backend/domains/mlb/prop_workflow.py` hydrates `d7_hits` from `player_derived_stats`; if payload lacks `rolling_result_avg_7`, it sets `rolling_result_avg_7 = d7_hits`.",
        "- Precomputed prediction features: `mlb.prop_features_precomputed.features` does not contain `rolling_result_avg_7`, `d7_hits`, `d15_hits`, or `d30_hits` for the audited holdout; those come from runtime derived-stat hydration.",
        "- Current slate/reconcile/export CSVs do not carry these raw feature columns, so they cannot by themselves prove feature parity.",
        "",
        "## Minimal Patch To Consider Later",
        "Do not change model behavior yet. The minimal reporting/parity fix would be to persist the prepared runtime feature vector, or at least add `rolling_result_avg_7` to any prediction feature snapshot/export by applying the same alias used in `prepare_prop()` (`rolling_result_avg_7 = d7_hits` for hits). A larger cleanup would rename this feature to `d7_hits` in metadata and retrain, but that is a model-contract change.",
    ]
    path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit hits rolling/d7 feature parity.")
    parser.add_argument("--from-date", default=DEFAULT_HOLDOUT_FROM)
    parser.add_argument("--to-date", default=DEFAULT_HOLDOUT_TO)
    parser.add_argument("--train-from-date", default="2024-01-01")
    parser.add_argument("--out-md", type=Path, default=DEFAULT_OUT_MD)
    parser.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)
    args = parser.parse_args()

    engine = create_engine(_db_url())
    training_rows, training_summary = _fetch_training_stats(engine, args.train_from_date, args.to_date)
    prediction_rows, prediction_summary = _fetch_prediction_stats(engine, args.from_date, args.to_date)
    rows = [*training_rows, *prediction_rows, *_metadata_stats(), *_artifact_stats()]
    out = pd.DataFrame(rows)
    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out_csv, index=False)
    _write_md(args.out_md, out, training_summary, prediction_summary)
    print(f"Wrote {args.out_csv}")
    print(f"Wrote {args.out_md}")
    print(out[["source", "feature", "rows", "nonnull_rows", "null_rate"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
