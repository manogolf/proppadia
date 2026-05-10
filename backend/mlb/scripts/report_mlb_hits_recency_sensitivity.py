#!/usr/bin/env python3
"""Measure hits calibration/ROI sensitivity to recency input features.

Uses full-slate reconcile rows for outcomes and DB-backed model input feature
sources for recency features. CSV-only diagnostics; no model changes.
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
DEFAULT_SURFACE_CSV = Path("backend/mlb/exports/model_diagnostics/hits_recency_surface.csv")
DEFAULT_THRESHOLDS_CSV = Path("backend/mlb/exports/model_diagnostics/hits_recency_thresholds.csv")

RECENCY_FEATURES = ["d7_hits", "d15_hits", "d30_hits", "rolling_result_avg_7"]
RECENCY_BINS = [0.0, 0.25, 0.50, 0.75, 1.00, np.inf]
RECENCY_LABELS = ["0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]
PRICE_BINS = [-np.inf, -200, -150, -110, 100, 150, np.inf]
PRICE_LABELS = ["<=-200", "-200_to_-150", "-150_to_-110", "-110_to_+100", "+100_to_+150", "+150+"]
PROB_BINS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, np.inf]
PROB_LABELS = ["0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75+"]


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
        "price_over_american",
        "price_under_american",
        "model_prob_over",
        "model_prob_under",
        "actual_over_outcome",
        "actual_under_outcome",
        "pnl_over_1u",
        "pnl_under_1u",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-recency] skip {path}: missing {missing}")
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
    for feature in RECENCY_FEATURES:
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
    return out[["date_key", "game_id_key", "player_id_key", *RECENCY_FEATURES]]


def _side_rows(reconcile: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    work = reconcile.copy()
    work["date_key"] = work["game_date"].map(_date_key)
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work = work.merge(features, how="left", on=["date_key", "game_id_key", "player_id_key"])
    pieces = []
    for side in ("over", "under"):
        side_df = pd.DataFrame(
            {
                "side": side,
                "line": pd.to_numeric(work["line"], errors="coerce"),
                "line_bucket": pd.to_numeric(work["line"], errors="coerce").map(lambda v: f"{float(v):g}" if pd.notna(v) else ""),
                "price": pd.to_numeric(work[f"price_{side}_american"], errors="coerce"),
                "model_prob": pd.to_numeric(work[f"model_prob_{side}"], errors="coerce"),
                "outcome": work[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
                "pnl": pd.to_numeric(work[f"pnl_{side}_1u"], errors="coerce"),
            }
        )
        for feature in RECENCY_FEATURES:
            side_df[feature] = pd.to_numeric(work[feature], errors="coerce")
        pieces.append(side_df)
    sides = pd.concat(pieces, ignore_index=True)
    sides = sides[
        sides["outcome"].isin({"win", "loss"})
        & sides["price"].notna()
        & sides["model_prob"].notna()
        & sides["line"].isin([0.5, 1.5])
    ].copy()
    sides["win"] = sides["outcome"].eq("win").astype(int)
    sides["price_bucket"] = pd.cut(sides["price"], bins=PRICE_BINS, labels=PRICE_LABELS, right=False)
    sides["prob_bucket"] = pd.cut(sides["model_prob"], bins=PROB_BINS, labels=PROB_LABELS, right=False)
    return sides


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    wins = int(group["win"].sum()) if bets else 0
    profit = float(pd.to_numeric(group["pnl"], errors="coerce").fillna(0.0).sum()) if bets else 0.0
    model = float(group["model_prob"].mean()) if bets else np.nan
    actual = wins / bets if bets else np.nan
    return {
        "bets": bets,
        "avg_model_prob": model,
        "actual_win_rate": actual,
        "calibration_error": actual - model if bets else np.nan,
        "roi": profit / bets if bets else np.nan,
    }


def _add_group(rows: list[dict[str, Any]], data: pd.DataFrame, feature: str, level: str, cols: list[str]) -> None:
    for keys, group in data.groupby(cols, observed=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {
            "feature": feature,
            "feature_bucket": "ALL",
            "group_level": level,
            "side": "ALL",
            "line_bucket": "ALL",
            "price_bucket": "ALL",
            "prob_bucket": "ALL",
        }
        row.update(dict(zip(cols, [str(k) for k in keys])))
        row.update(_metrics(group))
        rows.append(row)


def build_surface(sides: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for feature in RECENCY_FEATURES:
        data = sides.copy()
        data["feature_bucket"] = pd.cut(
            pd.to_numeric(data[feature], errors="coerce"),
            bins=RECENCY_BINS,
            labels=RECENCY_LABELS,
            right=False,
            include_lowest=True,
        )
        data = data[data["feature_bucket"].notna()].copy()
        group_defs = [
            ("feature_bucket", ["feature_bucket"]),
            ("feature_bucket_side", ["feature_bucket", "side"]),
            ("feature_bucket_line", ["feature_bucket", "line_bucket"]),
            ("feature_bucket_price", ["feature_bucket", "price_bucket"]),
            ("feature_bucket_side_line", ["feature_bucket", "side", "line_bucket"]),
            ("feature_bucket_side_line_price", ["feature_bucket", "side", "line_bucket", "price_bucket"]),
            ("feature_bucket_side_line_price_prob", ["feature_bucket", "side", "line_bucket", "price_bucket", "prob_bucket"]),
        ]
        for level, cols in group_defs:
            _add_group(rows, data, feature, level, cols)
    return pd.DataFrame(rows).sort_values(["feature", "group_level", "feature_bucket", "side", "line_bucket", "price_bucket", "prob_bucket"])


def build_thresholds(surface: pd.DataFrame, min_bets: int) -> pd.DataFrame:
    rows = []
    base = surface[
        surface["group_level"].eq("feature_bucket_side_line_price_prob")
        & surface["side"].eq("under")
        & surface["line_bucket"].eq("0.5")
        & surface["price_bucket"].isin({"+100_to_+150", "+150+"})
        & surface["prob_bucket"].isin(set(PROB_LABELS))
        & surface["bets"].ge(min_bets)
    ].copy()
    for (feature, bucket), group in base.groupby(["feature", "feature_bucket"], dropna=False):
        safe = group[(group["calibration_error"].ge(-0.05)) & (group["roi"].ge(0.0))].copy()
        if safe.empty:
            max_safe = ""
            observed = np.nan
            roi = np.nan
        else:
            safe["_prob_order"] = safe["prob_bucket"].map({label: i for i, label in enumerate(PROB_LABELS)})
            best = safe.sort_values(["_prob_order", "roi"], ascending=[False, False]).iloc[0]
            max_safe = best["prob_bucket"]
            observed = best["actual_win_rate"]
            roi = best["roi"]
        rows.append(
            {
                "feature": feature,
                "threshold": bucket,
                "max_model_prob_safe": max_safe,
                "observed_win_rate": observed,
                "ROI": roi,
            }
        )
    return pd.DataFrame(rows).sort_values(["feature", "threshold"])


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Measure hits recency feature sensitivity.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--min-bets", type=int, default=25)
    ap.add_argument("--out-csv", default=str(DEFAULT_SURFACE_CSV))
    ap.add_argument("--thresholds-csv", default=str(DEFAULT_THRESHOLDS_CSV))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_hits_reconcile(paths)
    engine = create_engine(_db_url(), pool_pre_ping=True)
    features = _prep_features(_fetch_features(engine, args.from_date, args.to_date))
    sides = _side_rows(reconcile, features)
    surface = build_surface(sides)
    thresholds = build_thresholds(surface, args.min_bets)

    out = Path(args.out_csv)
    thresholds_out = Path(args.thresholds_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    surface.to_csv(out, index=False)
    thresholds.to_csv(thresholds_out, index=False)
    print(
        "[hits-recency] "
        f"files={len(paths)} side_rows={len(sides)} surface_rows={len(surface)} "
        f"out_csv={out} thresholds_csv={thresholds_out}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
