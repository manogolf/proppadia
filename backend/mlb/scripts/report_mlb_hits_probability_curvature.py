#!/usr/bin/env python3
"""Measure probability curve shape distortion for hits line 0.5.

Builds fine-grained feature-bin curves for model probability and actual win
rate, then compares consecutive-bin deltas. Diagnostics only; no DB writes and
no model changes.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_probability_curvature.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_probability_curvature_summary.md")

FEATURES = ["d7_hits", "rolling_result_avg_7"]
BIN_EDGES = [round(i / 10, 1) for i in range(0, 11)] + [np.inf]
BIN_LABELS = [f"{i/10:.2f}-{(i+1)/10:.2f}" for i in range(0, 10)] + ["1.00+"]


def _db_url() -> str:
    url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not url:
        raise SystemExit("DATABASE_URL or SUPABASE_DB_URL must be set.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _discover_reconcile_files(root: Path, from_date: str, to_date: str) -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        if pd.isna(pd.to_datetime(date, errors="coerce")):
            continue
        if from_date and date < from_date:
            continue
        if to_date and date > to_date:
            continue
        files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_hits_reconcile(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    required = {
        "game_date",
        "game_id",
        "player_id",
        "prop_type",
        "line",
        "model_prob_over",
        "model_prob_under",
        "actual_over_outcome",
        "actual_under_outcome",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-curvature] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq("hits")].copy()
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible hits reconcile rows found.")
    return pd.concat(frames, ignore_index=True)


def _fetch_features(engine, from_date: str, to_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT
          mt.game_date,
          mt.game_id,
          mt.player_id,
          mt.rolling_result_avg_7,
          pds.d7_hits,
          pfp.features AS pfp_features,
          pfp.feature_set_tag,
          pfp.model_tag
        FROM mlb.model_training_props mt
        LEFT JOIN mlb.player_derived_stats pds
          ON pds.player_id = mt.player_id
         AND pds.game_id = mt.game_id
         AND pds.game_date = mt.game_date
        LEFT JOIN mlb.prop_features_precomputed pfp
          ON pfp.player_id = mt.player_id
         AND pfp.game_id = mt.game_id
         AND pfp.game_date = mt.game_date
         AND pfp.prop_type = mt.prop_type
        WHERE mt.prop_type = 'hits'
          AND mt.game_date BETWEEN :from_date AND :to_date
        """
    )
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={"from_date": from_date, "to_date": to_date})


def _parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _prep_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    parsed = out["pfp_features"].map(_parse_json_obj) if "pfp_features" in out.columns else pd.Series([{}] * len(out))
    for feature in FEATURES:
        if feature not in out.columns:
            out[feature] = np.nan
        out[feature] = pd.to_numeric(out[feature], errors="coerce")
        mask = out[feature].isna()
        if mask.any():
            out.loc[mask, feature] = parsed[mask].map(lambda obj, key=feature: obj.get(key)).pipe(
                pd.to_numeric, errors="coerce"
            )
    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out = out.sort_values(["date_key", "game_id_key", "player_id_key", "feature_set_tag", "model_tag"]).drop_duplicates(
        ["date_key", "game_id_key", "player_id_key"], keep="last"
    )
    return out[["date_key", "game_id_key", "player_id_key", *FEATURES]]


def _side_rows(reconcile: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    work = reconcile.copy()
    work["date_key"] = work["game_date"].map(_date_key)
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work["line_num"] = pd.to_numeric(work["line"], errors="coerce")
    work = work[work["line_num"].eq(0.5)].copy()
    work = work.merge(features, how="left", on=["date_key", "game_id_key", "player_id_key"])

    pieces = []
    for side in ("over", "under"):
        side_df = pd.DataFrame(
            {
                "side": side,
                "model_prob": pd.to_numeric(work[f"model_prob_{side}"], errors="coerce"),
                "outcome": work[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
            }
        )
        for feature in FEATURES:
            side_df[feature] = pd.to_numeric(work[feature], errors="coerce")
        pieces.append(side_df)
    rows = pd.concat(pieces, ignore_index=True)
    rows = rows[rows["outcome"].isin({"win", "loss"}) & rows["model_prob"].notna()].copy()
    rows["win"] = rows["outcome"].eq("win").astype(float)
    return rows


def build_curvature(rows: pd.DataFrame) -> pd.DataFrame:
    records = []
    for feature in FEATURES:
        data = rows.copy()
        data["feature_value"] = pd.to_numeric(data[feature], errors="coerce")
        data["feature_bin"] = pd.cut(
            data["feature_value"],
            bins=BIN_EDGES,
            labels=BIN_LABELS,
            right=False,
            include_lowest=True,
        )
        data = data[data["feature_value"].notna() & data["feature_bin"].notna()].copy()
        for side, side_df in data.groupby("side", observed=True, dropna=False):
            curve_rows = []
            for bin_index, (feature_bin, group) in enumerate(side_df.groupby("feature_bin", observed=True, dropna=False)):
                curve_rows.append(
                    {
                        "feature": feature,
                        "side": str(side),
                        "feature_bin": str(feature_bin),
                        "bin_index": int(bin_index),
                        "bets": int(len(group)),
                        "avg_feature_value": float(group["feature_value"].mean()),
                        "avg_model_prob": float(group["model_prob"].mean()),
                        "actual_rate": float(group["win"].mean()),
                    }
                )
            curve = pd.DataFrame(curve_rows).sort_values("bin_index").reset_index(drop=True)
            curve["next_feature_bin"] = curve["feature_bin"].shift(-1)
            curve["next_avg_model_prob"] = curve["avg_model_prob"].shift(-1)
            curve["next_actual_rate"] = curve["actual_rate"].shift(-1)
            curve["delta_model"] = curve["next_avg_model_prob"] - curve["avg_model_prob"]
            curve["delta_actual"] = curve["next_actual_rate"] - curve["actual_rate"]
            curve["curvature_error"] = curve["delta_model"] - curve["delta_actual"]
            curve["abs_curvature_error"] = curve["curvature_error"].abs()
            records.append(curve)
    if not records:
        return pd.DataFrame()
    return pd.concat(records, ignore_index=True)


def _fmt(value: Any, digits: int = 4) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value):.{digits}f}"


def _md_table(df: pd.DataFrame, cols: list[str], max_rows: int = 20) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].head(max_rows).fillna("")
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def write_summary(out_md: Path, curvature: pd.DataFrame, rows: pd.DataFrame, from_date: str, to_date: str, files: int) -> None:
    usable = curvature[curvature["delta_model"].notna() & curvature["delta_actual"].notna()].copy()
    usable_pairs = usable[usable["bets"].ge(25)].copy()
    top = usable_pairs.sort_values("abs_curvature_error", ascending=False).head(20).copy()
    for col in [
        "avg_model_prob",
        "actual_rate",
        "delta_model",
        "delta_actual",
        "curvature_error",
        "abs_curvature_error",
    ]:
        top[col] = top[col].map(_fmt)

    by_feature = (
        usable_pairs.groupby(["feature", "side"], dropna=False)
        .agg(
            bins=("feature_bin", "count"),
            max_abs_curvature_error=("abs_curvature_error", "max"),
            mean_abs_curvature_error=("abs_curvature_error", "mean"),
            total_bets=("bets", "sum"),
        )
        .reset_index()
        .sort_values("max_abs_curvature_error", ascending=False)
    )
    for col in ["max_abs_curvature_error", "mean_abs_curvature_error"]:
        by_feature[col] = by_feature[col].map(_fmt)

    lines = [
        "# Hits Probability Curvature Summary",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Scope: `prop_type = hits`, `line = 0.5`.",
        "",
        "Curvature compares consecutive-bin changes:",
        "- `delta_model = next_avg_model_prob - avg_model_prob`",
        "- `delta_actual = next_actual_rate - actual_rate`",
        "- `curvature_error = delta_model - delta_actual`",
        "",
        f"Reconcile files: `{files}`",
        f"Evaluated side rows: `{len(rows)}`",
        f"Curvature rows: `{len(curvature)}`",
        "",
        "## Feature/Side Curvature Strength",
        "",
        _md_table(by_feature, ["feature", "side", "bins", "total_bets", "max_abs_curvature_error", "mean_abs_curvature_error"], 20),
        "",
        "## Largest Curvature Errors",
        "",
        _md_table(
            top,
            [
                "feature",
                "side",
                "feature_bin",
                "next_feature_bin",
                "bets",
                "avg_model_prob",
                "actual_rate",
                "delta_model",
                "delta_actual",
                "curvature_error",
            ],
            20,
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Measure hits probability curve curvature for line 0.5.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_hits_reconcile(paths)
    engine = create_engine(_db_url(), pool_pre_ping=True)
    source = _fetch_features(engine, args.from_date, args.to_date)
    features = _prep_features(source)
    rows = _side_rows(reconcile, features)
    curvature = build_curvature(rows)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    curvature.to_csv(out_csv, index=False)
    write_summary(out_md, curvature, rows, args.from_date, args.to_date, len(paths))

    print(
        "[hits-curvature] "
        f"files={len(paths)} source_rows={len(source)} side_rows={len(rows)} "
        f"curvature_rows={len(curvature)} out_csv={out_csv} out_md={out_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
