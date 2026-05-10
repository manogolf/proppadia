#!/usr/bin/env python3
"""Identify interaction drivers of hits probability calibration failure.

Diagnostics only. Uses full-slate reconcile rows plus DB-backed d7/d30 hits
features. No model changes.
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
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_interaction_failure_surface.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_interaction_failure_summary.md")

RECENCY_BINS = [0.0, 0.25, 0.50, 0.75, 1.00, np.inf]
RECENCY_LABELS = ["0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]
BASELINE_BINS = [0.0, 0.50, 0.75, 1.00, np.inf]
BASELINE_LABELS = ["0-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]
PRICE_BINS = [-np.inf, -200, -150, -110, 100, 150, np.inf]
PRICE_LABELS = ["<=-200", "-200_to_-150", "-150_to_-110", "-110_to_+100", "+100_to_+150", "+150+"]


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
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-interaction] skip {path}: missing {missing}")
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
          pds.d7_hits,
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
    for feature in ("d7_hits", "d30_hits"):
        out[feature] = pd.to_numeric(out.get(feature), errors="coerce")
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
    return out[["date_key", "game_id_key", "player_id_key", "d7_hits", "d30_hits"]]


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
                "price": pd.to_numeric(work[f"price_{side}_american"], errors="coerce"),
                "model_prob": pd.to_numeric(work[f"model_prob_{side}"], errors="coerce"),
                "outcome": work[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
                "d7_hits": pd.to_numeric(work["d7_hits"], errors="coerce"),
                "d30_hits": pd.to_numeric(work["d30_hits"], errors="coerce"),
            }
        )
        pieces.append(side_df)
    sides = pd.concat(pieces, ignore_index=True)
    sides = sides[
        sides["outcome"].isin({"win", "loss"})
        & sides["model_prob"].notna()
        & sides["price"].notna()
        & sides["line"].isin([0.5, 1.5])
        & sides["d7_hits"].notna()
        & sides["d30_hits"].notna()
    ].copy()
    sides["actual_win"] = sides["outcome"].eq("win").astype(float)
    sides["recency_bucket"] = pd.cut(sides["d7_hits"], RECENCY_BINS, labels=RECENCY_LABELS, right=False)
    sides["baseline_bucket"] = pd.cut(sides["d30_hits"], BASELINE_BINS, labels=BASELINE_LABELS, right=False)
    sides["price_bucket"] = pd.cut(sides["price"], PRICE_BINS, labels=PRICE_LABELS, right=False)
    sides["line_bucket"] = sides["line"].map(lambda v: f"{float(v):g}")
    sides["bad_zone"] = (
        sides["side"].eq("under")
        & sides["line"].eq(0.5)
        & sides["price"].gt(0)
        & sides["model_prob"].ge(0.60)
    )
    return sides.dropna(subset=["recency_bucket", "baseline_bucket", "price_bucket"]).copy()


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    if bets == 0:
        return {
            "bets": 0,
            "avg_model_prob": np.nan,
            "actual_win_rate": np.nan,
            "calibration_error": np.nan,
            "abs_calibration_error": np.nan,
            "bad_zone_rows": 0,
            "bad_zone_share": np.nan,
        }
    model = float(group["model_prob"].mean())
    actual = float(group["actual_win"].mean())
    error = actual - model
    bad = int(group["bad_zone"].sum())
    return {
        "bets": bets,
        "avg_model_prob": model,
        "actual_win_rate": actual,
        "calibration_error": error,
        "abs_calibration_error": abs(error),
        "bad_zone_rows": bad,
        "bad_zone_share": bad / bets,
    }


def _add_group(rows: list[dict[str, Any]], data: pd.DataFrame, level: str, cols: list[str]) -> None:
    for keys, group in data.groupby(cols, observed=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {
            "interaction_type": level,
            "recency_bucket": "ALL",
            "line_bucket": "ALL",
            "price_bucket": "ALL",
            "side": "ALL",
            "baseline_bucket": "ALL",
        }
        row.update(dict(zip(cols, [str(k) for k in keys])))
        row.update(_metrics(group))
        rows.append(row)


def build_surface(sides: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    group_defs = [
        ("recency_x_line", ["recency_bucket", "line_bucket"]),
        ("recency_x_price", ["recency_bucket", "price_bucket"]),
        ("recency_x_side", ["recency_bucket", "side"]),
        ("recency_x_baseline", ["recency_bucket", "baseline_bucket"]),
        ("recency_x_side_line_price", ["recency_bucket", "side", "line_bucket", "price_bucket"]),
        ("recency_x_side_line_price_baseline", ["recency_bucket", "side", "line_bucket", "price_bucket", "baseline_bucket"]),
    ]
    for level, cols in group_defs:
        _add_group(rows, sides, level, cols)
    bad = sides[sides["bad_zone"]].copy()
    if not bad.empty:
        bad_group_defs = [
            ("bad_zone_recency_x_price", ["recency_bucket", "price_bucket"]),
            ("bad_zone_recency_x_baseline", ["recency_bucket", "baseline_bucket"]),
            ("bad_zone_recency_x_price_baseline", ["recency_bucket", "price_bucket", "baseline_bucket"]),
        ]
        for level, cols in bad_group_defs:
            _add_group(rows, bad, level, cols)
    return pd.DataFrame(rows).sort_values(["abs_calibration_error", "bets"], ascending=[False, False])


def _fmt_pct(value: Any) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value) * 100:.1f}%"


def _md_table(df: pd.DataFrame, cols: list[str], max_rows: int = 25) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].head(max_rows).copy().fillna("")
    for col in ["avg_model_prob", "actual_win_rate", "calibration_error", "abs_calibration_error", "bad_zone_share"]:
        if col in work:
            work[col] = work[col].map(_fmt_pct)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def write_summary(surface: pd.DataFrame, out_md: Path, from_date: str, to_date: str, min_bets: int) -> None:
    ranked = surface[surface["bets"].ge(min_bets)].sort_values("abs_calibration_error", ascending=False)
    bad_related = ranked[ranked["bad_zone_rows"].gt(0)].copy()
    bad_only = surface[
        surface["interaction_type"].astype(str).str.startswith("bad_zone_") & surface["bets"].ge(min_bets)
    ].sort_values("abs_calibration_error", ascending=False)
    lines = [
        "# Hits Interaction Failure Summary",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Recency feature: `d7_hits`; baseline feature: `d30_hits`.",
        "",
        "Bad zone: hits under 0.5, plus-money, model probability >= 0.60.",
        "",
        f"Ranked tables below require at least `{min_bets}` bets.",
        "",
        "## Worst Interaction Calibration Errors",
        "",
        _md_table(
            ranked,
            [
                "interaction_type",
                "recency_bucket",
                "side",
                "line_bucket",
                "price_bucket",
                "baseline_bucket",
                "bets",
                "avg_model_prob",
                "actual_win_rate",
                "calibration_error",
                "bad_zone_rows",
                "bad_zone_share",
            ],
        ),
        "",
        "## Worst Interactions Containing Bad-Zone Rows",
        "",
        _md_table(
            bad_related,
            [
                "interaction_type",
                "recency_bucket",
                "side",
                "line_bucket",
                "price_bucket",
                "baseline_bucket",
                "bets",
                "avg_model_prob",
                "actual_win_rate",
                "calibration_error",
                "bad_zone_rows",
                "bad_zone_share",
            ],
        ),
        "",
        "## Exact Bad-Zone Interactions",
        "",
        _md_table(
            bad_only,
            [
                "interaction_type",
                "recency_bucket",
                "price_bucket",
                "baseline_bucket",
                "bets",
                "avg_model_prob",
                "actual_win_rate",
                "calibration_error",
            ],
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report hits interaction calibration failures.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--min-bets", type=int, default=25)
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_hits_reconcile(paths)
    engine = create_engine(_db_url(), pool_pre_ping=True)
    features = _prep_features(_fetch_features(engine, args.from_date, args.to_date))
    sides = _side_rows(reconcile, features)
    surface = build_surface(sides)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    surface.to_csv(out_csv, index=False)
    write_summary(surface, out_md, args.from_date, args.to_date, args.min_bets)
    print(f"[hits-interaction] files={len(paths)} side_rows={len(sides)} surface_rows={len(surface)} out_csv={out_csv} out_md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
