#!/usr/bin/env python3
"""Diagnose hits probability failure without price or implied-probability inputs.

This diagnostic intentionally ignores odds, implied probabilities, and price
bucket fields. It uses outcome-backed reconcile rows to evaluate model
probability calibration and DB-backed feature sources for recency/baseline
features. No model changes and no DB writes.
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
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_feature_only_failure_surface.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_feature_only_failure_summary.md")

FEATURES = ["d7_hits", "d15_hits", "d30_hits", "rolling_result_avg_7"]
RECENCY_FEATURES = ["d7_hits", "d15_hits", "rolling_result_avg_7"]
BASELINE_FEATURES = ["d30_hits"]

VALUE_BINS = [0.0, 0.25, 0.50, 0.75, 1.00, np.inf]
VALUE_LABELS = ["0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]
PROB_BINS = [0.60, 0.65, 0.70, 0.75, np.inf]
PROB_LABELS = ["0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75+"]


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
            print(f"[hits-feature-only] skip {path}: missing {missing}")
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
          pds.d15_hits,
          pds.d30_hits,
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
                "line": work["line_num"],
                "line_bucket": "0.5",
                "model_prob": pd.to_numeric(work[f"model_prob_{side}"], errors="coerce"),
                "outcome": work[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
            }
        )
        for feature in FEATURES:
            side_df[feature] = pd.to_numeric(work[feature], errors="coerce")
        pieces.append(side_df)
    sides = pd.concat(pieces, ignore_index=True)
    sides = sides[
        sides["outcome"].isin({"win", "loss"})
        & sides["model_prob"].ge(0.60)
        & sides["model_prob"].notna()
    ].copy()
    sides["win"] = sides["outcome"].eq("win").astype(float)
    sides["model_prob_bucket"] = pd.cut(sides["model_prob"], bins=PROB_BINS, labels=PROB_LABELS, right=False)
    for feature in FEATURES:
        sides[f"{feature}_bucket"] = pd.cut(
            pd.to_numeric(sides[feature], errors="coerce"),
            bins=VALUE_BINS,
            labels=VALUE_LABELS,
            right=False,
            include_lowest=True,
        )
    return sides


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    if bets == 0:
        return {
            "bets": 0,
            "avg_model_prob": np.nan,
            "actual_win_rate": np.nan,
            "calibration_error": np.nan,
        }
    model = float(group["model_prob"].mean())
    actual = float(group["win"].mean())
    return {
        "bets": bets,
        "avg_model_prob": model,
        "actual_win_rate": actual,
        "calibration_error": actual - model,
    }


def _append_group(
    rows: list[dict[str, Any]],
    data: pd.DataFrame,
    group_type: str,
    cols: list[str],
    *,
    notes: str = "",
) -> None:
    for keys, group in data.groupby(cols, observed=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {
            "group_type": group_type,
            "feature": "",
            "feature_bucket": "",
            "recency_feature": "",
            "recency_bucket": "",
            "baseline_feature": "",
            "baseline_bucket": "",
            "side": "ALL",
            "line_bucket": "0.5",
            "model_prob_bucket": "ALL",
            "notes": notes,
        }
        row.update(dict(zip(cols, [str(k) for k in keys])))
        row.update(_metrics(group))
        rows.append(row)


def build_surface(sides: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for feature in FEATURES:
        data = sides[sides[f"{feature}_bucket"].notna()].copy()
        data["feature"] = feature
        data["feature_bucket"] = data[f"{feature}_bucket"].astype(str)
        _append_group(rows, data, "feature_bucket", ["feature", "feature_bucket"])
        _append_group(rows, data, "feature_bucket_side", ["feature", "feature_bucket", "side"])
        _append_group(
            rows,
            data,
            "feature_bucket_side_model_prob",
            ["feature", "feature_bucket", "side", "model_prob_bucket"],
        )

    for recency in RECENCY_FEATURES:
        rec_col = f"{recency}_bucket"
        for baseline in BASELINE_FEATURES:
            base_col = f"{baseline}_bucket"
            data = sides[sides[rec_col].notna() & sides[base_col].notna()].copy()
            data["recency_feature"] = recency
            data["recency_bucket"] = data[rec_col].astype(str)
            data["baseline_feature"] = baseline
            data["baseline_bucket"] = data[base_col].astype(str)
            _append_group(
                rows,
                data,
                "recency_x_baseline",
                ["recency_feature", "recency_bucket", "baseline_feature", "baseline_bucket"],
            )
            _append_group(
                rows,
                data,
                "recency_x_baseline_side",
                ["recency_feature", "recency_bucket", "baseline_feature", "baseline_bucket", "side"],
            )

    for recency in RECENCY_FEATURES:
        rec_col = f"{recency}_bucket"
        data = sides[sides[rec_col].notna()].copy()
        data["recency_feature"] = recency
        data["recency_bucket"] = data[rec_col].astype(str)
        _append_group(
            rows,
            data,
            "recency_x_line",
            ["recency_feature", "recency_bucket", "line_bucket"],
            notes="line_is_constant_0.5_in_filter",
        )
        _append_group(
            rows,
            data,
            "recency_x_line_side",
            ["recency_feature", "recency_bucket", "line_bucket", "side"],
            notes="line_is_constant_0.5_in_filter",
        )

    for baseline in BASELINE_FEATURES:
        base_col = f"{baseline}_bucket"
        data = sides[sides[base_col].notna() & sides["model_prob_bucket"].notna()].copy()
        data["baseline_feature"] = baseline
        data["baseline_bucket"] = data[base_col].astype(str)
        _append_group(
            rows,
            data,
            "baseline_x_model_prob",
            ["baseline_feature", "baseline_bucket", "model_prob_bucket"],
        )
        _append_group(
            rows,
            data,
            "baseline_x_model_prob_side",
            ["baseline_feature", "baseline_bucket", "model_prob_bucket", "side"],
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["abs_calibration_error"] = pd.to_numeric(out["calibration_error"], errors="coerce").abs()
    out["sample_size_flag"] = np.select(
        [out["bets"].ge(75), out["bets"].ge(25)],
        ["strong_sample", "usable"],
        default="low_sample",
    )
    out["usable_sample"] = out["bets"].ge(25)
    return out.sort_values(["usable_sample", "abs_calibration_error", "bets"], ascending=[False, False, False])


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


def write_summary(out_md: Path, surface: pd.DataFrame, sides: pd.DataFrame, from_date: str, to_date: str, files: int) -> None:
    overview_rows = []
    for cols, label in [([], "overall"), (["side"], "by_side"), (["model_prob_bucket"], "by_model_prob_bucket")]:
        if cols:
            for keys, group in sides.groupby(cols, observed=True, dropna=False):
                if not isinstance(keys, tuple):
                    keys = (keys,)
                row = {"group": label, "side": "ALL", "model_prob_bucket": "ALL"}
                row.update(dict(zip(cols, [str(k) for k in keys])))
                row.update(_metrics(group))
                overview_rows.append(row)
        else:
            row = {"group": label, "side": "ALL", "model_prob_bucket": "ALL"}
            row.update(_metrics(sides))
            overview_rows.append(row)
    overview = pd.DataFrame(overview_rows)
    for col in ["avg_model_prob", "actual_win_rate", "calibration_error"]:
        overview[col] = overview[col].map(_fmt)

    top = surface[surface["bets"].ge(25)].copy().head(30)
    for col in ["avg_model_prob", "actual_win_rate", "calibration_error", "abs_calibration_error"]:
        top[col] = top[col].map(_fmt)

    by_type = (
        surface[surface["bets"].ge(25)]
        .groupby("group_type", dropna=False)
        .agg(
            groups=("group_type", "size"),
            max_abs_calibration_error=("abs_calibration_error", "max"),
            mean_abs_calibration_error=("abs_calibration_error", "mean"),
        )
        .reset_index()
        .sort_values("max_abs_calibration_error", ascending=False)
    )
    for col in ["max_abs_calibration_error", "mean_abs_calibration_error"]:
        by_type[col] = by_type[col].map(_fmt)

    lines = [
        "# Hits Feature-Only Failure Summary",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Scope: `prop_type = hits`, `line = 0.5`, side-specific `model_prob >= 0.60`.",
        "",
        "Ignored by design:",
        "- bet odds",
        "- implied probabilities",
        "- price buckets",
        "",
        f"Reconcile files: `{files}`",
        f"Evaluated side rows: `{len(sides)}`",
        "",
        "## Overview",
        "",
        _md_table(overview, ["group", "side", "model_prob_bucket", "bets", "avg_model_prob", "actual_win_rate", "calibration_error"], 20),
        "",
        "## Group Type Error Strength",
        "",
        _md_table(by_type, ["group_type", "groups", "max_abs_calibration_error", "mean_abs_calibration_error"], 20),
        "",
        "## Largest Feature-Only Calibration Errors",
        "",
        _md_table(
            top,
            [
                "group_type",
                "feature",
                "feature_bucket",
                "recency_feature",
                "recency_bucket",
                "baseline_feature",
                "baseline_bucket",
                "side",
                "model_prob_bucket",
                "bets",
                "avg_model_prob",
                "actual_win_rate",
                "calibration_error",
            ],
            30,
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Diagnose hits feature-only probability failure without price influence.")
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
    sides = _side_rows(reconcile, features)
    surface = build_surface(sides)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    surface.to_csv(out_csv, index=False)
    write_summary(out_md, surface, sides, args.from_date, args.to_date, len(paths))

    print(
        "[hits-feature-only] "
        f"files={len(paths)} source_rows={len(source)} side_rows={len(sides)} "
        f"surface_rows={len(surface)} out_csv={out_csv} out_md={out_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
