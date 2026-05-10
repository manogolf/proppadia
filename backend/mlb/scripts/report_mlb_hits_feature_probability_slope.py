#!/usr/bin/env python3
"""Measure feature-to-probability slope for hits line 0.5.

Compares the slope of model probability versus feature value against the
observed outcome slope for the same feature. Diagnostics only; no DB writes and
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
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_feature_slope.csv")
DEFAULT_SURFACE_CSV = Path("backend/mlb/exports/model_diagnostics/hits_feature_slope_surface.csv")

FEATURES = ["d7_hits", "d15_hits", "d30_hits", "rolling_result_avg_7"]
FEATURE_BINS = [0.0, 0.25, 0.50, 0.75, 1.00, np.inf]
FEATURE_LABELS = ["0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]


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
            print(f"[hits-feature-slope] skip {path}: missing {missing}")
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


def _safe_slope(x: pd.Series, y: pd.Series) -> float:
    data = pd.DataFrame({"x": pd.to_numeric(x, errors="coerce"), "y": pd.to_numeric(y, errors="coerce")}).dropna()
    if len(data) < 2 or data["x"].nunique() < 2:
        return np.nan
    return float(np.polyfit(data["x"].astype(float), data["y"].astype(float), deg=1)[0])


def build_surface(rows: pd.DataFrame) -> pd.DataFrame:
    records = []
    for feature in FEATURES:
        data = rows.copy()
        data["feature_value"] = pd.to_numeric(data[feature], errors="coerce")
        data["feature_bucket"] = pd.cut(
            data["feature_value"],
            bins=FEATURE_BINS,
            labels=FEATURE_LABELS,
            right=False,
            include_lowest=True,
        )
        data = data[data["feature_bucket"].notna() & data["feature_value"].notna()].copy()
        for keys, group in data.groupby(["feature_bucket", "side"], observed=True, dropna=False):
            bucket, side = keys
            records.append(
                {
                    "feature": feature,
                    "feature_bucket": str(bucket),
                    "side": str(side),
                    "bets": int(len(group)),
                    "avg_feature_value": float(group["feature_value"].mean()),
                    "avg_model_prob": float(group["model_prob"].mean()),
                    "actual_win_rate": float(group["win"].mean()),
                    "calibration_error": float(group["win"].mean() - group["model_prob"].mean()),
                }
            )
    return pd.DataFrame(records).sort_values(["feature", "side", "feature_bucket"])


def build_slopes(surface: pd.DataFrame) -> pd.DataFrame:
    records = []
    for (feature, side), group in surface.groupby(["feature", "side"], dropna=False):
        usable = group[group["bets"].gt(0)].copy()
        slope_model = _safe_slope(usable["avg_feature_value"], usable["avg_model_prob"])
        slope_actual = _safe_slope(usable["avg_feature_value"], usable["actual_win_rate"])
        if pd.notna(slope_actual) and slope_actual != 0:
            slope_ratio = slope_model / slope_actual
        else:
            slope_ratio = np.nan
        records.append(
            {
                "feature": feature,
                "side": side,
                "buckets": int(len(usable)),
                "bets": int(usable["bets"].sum()),
                "slope_model": slope_model,
                "slope_actual": slope_actual,
                "slope_ratio": slope_ratio,
                "slope_gap": slope_model - slope_actual
                if pd.notna(slope_model) and pd.notna(slope_actual)
                else np.nan,
                "abs_slope_gap": abs(slope_model - slope_actual)
                if pd.notna(slope_model) and pd.notna(slope_actual)
                else np.nan,
            }
        )
    return pd.DataFrame(records).sort_values(["side", "abs_slope_gap"], ascending=[True, False])


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Measure hits feature-to-probability slope for line 0.5.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--surface-csv", default=str(DEFAULT_SURFACE_CSV))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_hits_reconcile(paths)
    engine = create_engine(_db_url(), pool_pre_ping=True)
    source = _fetch_features(engine, args.from_date, args.to_date)
    features = _prep_features(source)
    rows = _side_rows(reconcile, features)

    surface = build_surface(rows)
    slopes = build_slopes(surface)

    out_csv = Path(args.out_csv)
    surface_csv = Path(args.surface_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    slopes.to_csv(out_csv, index=False)
    surface.to_csv(surface_csv, index=False)

    print(
        "[hits-feature-slope] "
        f"files={len(paths)} source_rows={len(source)} side_rows={len(rows)} "
        f"slopes={len(slopes)} out_csv={out_csv} surface_csv={surface_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
