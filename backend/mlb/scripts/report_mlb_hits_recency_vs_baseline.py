#!/usr/bin/env python3
"""Test whether hits recency overconfidence is missing a baseline skill anchor.

Uses outcome-backed reconcile rows for probabilities/outcomes and DB-backed
model input features for d7/d30 hits. Diagnostics only.
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
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_recency_vs_baseline.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_recency_vs_baseline_summary.md")


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
            print(f"[hits-recency-baseline] skip {path}: missing {missing}")
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
          mt.rolling_result_avg_7,
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
    for feature in ["d7_hits", "d30_hits", "rolling_result_avg_7"]:
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
    return out[["date_key", "game_id_key", "player_id_key", "d7_hits", "d30_hits", "rolling_result_avg_7"]]


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
                "rolling_result_avg_7": pd.to_numeric(work["rolling_result_avg_7"], errors="coerce"),
            }
        )
        pieces.append(side_df)
    sides = pd.concat(pieces, ignore_index=True)
    sides = sides[
        sides["outcome"].isin({"win", "loss"})
        & sides["model_prob"].notna()
        & sides["price"].notna()
        & sides["line"].notna()
        & sides["d7_hits"].notna()
        & sides["d30_hits"].notna()
    ].copy()
    sides["actual_win"] = sides["outcome"].eq("win").astype(float)
    sides["baseline_rate"] = sides["d30_hits"]
    sides["recency_rate"] = sides["d7_hits"]
    sides["recency_deviation"] = sides["recency_rate"] - sides["baseline_rate"]
    sides["price_class"] = np.where(sides["price"].lt(0), "favorite", "plus_money")
    sides["line_bucket"] = sides["line"].map(lambda v: f"{float(v):g}")
    sides["model_prob_group"] = np.where(sides["model_prob"].ge(0.60), "model_prob_ge_060", "model_prob_lt_060")

    low_recency = sides["recency_rate"].lt(0.50)
    high_baseline = sides["baseline_rate"].ge(0.75)
    low_baseline = sides["baseline_rate"].lt(0.50)
    sides["segment"] = np.select(
        [
            low_recency & high_baseline,
            low_recency & low_baseline,
        ],
        [
            "A_low_recency_high_baseline",
            "B_low_recency_low_baseline",
        ],
        default="other",
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
            "avg_baseline_rate": np.nan,
            "avg_recency_rate": np.nan,
            "avg_recency_deviation": np.nan,
        }
    actual = float(group["actual_win"].mean())
    model = float(group["model_prob"].mean())
    return {
        "bets": bets,
        "avg_model_prob": model,
        "actual_win_rate": actual,
        "calibration_error": actual - model,
        "avg_baseline_rate": float(group["baseline_rate"].mean()),
        "avg_recency_rate": float(group["recency_rate"].mean()),
        "avg_recency_deviation": float(group["recency_deviation"].mean()),
    }


def build_report(sides: pd.DataFrame) -> pd.DataFrame:
    rows = []
    group_defs = [
        ("segment", ["segment"]),
        ("segment_side", ["segment", "side"]),
        ("segment_line", ["segment", "line_bucket"]),
        ("segment_price", ["segment", "price_class"]),
        ("segment_side_line_price", ["segment", "side", "line_bucket", "price_class"]),
        (
            "segment_side_line_price_model_prob",
            ["segment", "side", "line_bucket", "price_class", "model_prob_group"],
        ),
    ]
    for level, cols in group_defs:
        for keys, group in sides.groupby(cols, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {
                "group_level": level,
                "segment": "",
                "side": "ALL",
                "line_bucket": "ALL",
                "price_class": "ALL",
                "model_prob_group": "ALL",
            }
            row.update(dict(zip(cols, [str(k) for k in keys])))
            row.update(_metrics(group))
            rows.append(row)
    return pd.DataFrame(rows).sort_values(
        ["group_level", "segment", "side", "line_bucket", "price_class", "model_prob_group"]
    )


def _fmt_pct(value: Any) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value) * 100:.1f}%"


def _fmt_num(value: Any) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value):.3f}"


def _md_table(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].copy().fillna("")
    for col in ["avg_model_prob", "actual_win_rate", "calibration_error"]:
        if col in work:
            work[col] = work[col].map(_fmt_pct)
    for col in ["avg_baseline_rate", "avg_recency_rate", "avg_recency_deviation"]:
        if col in work:
            work[col] = work[col].map(_fmt_num)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def write_summary(report: pd.DataFrame, out_md: Path, from_date: str, to_date: str) -> None:
    seg = report[report["group_level"].eq("segment")].copy()
    focus = report[
        report["group_level"].eq("segment_side_line_price_model_prob")
        & report["side"].eq("under")
        & report["line_bucket"].eq("0.5")
        & report["price_class"].eq("plus_money")
        & report["model_prob_group"].eq("model_prob_ge_060")
        & report["segment"].isin(["A_low_recency_high_baseline", "B_low_recency_low_baseline"])
    ].copy()
    lines = [
        "# Hits Recency Vs Baseline",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "No season/career hits-per-game feature was found in the MLB schema, so this report uses `d30_hits` as the long-window baseline proxy.",
        "",
        "Definitions:",
        "- `baseline_rate = d30_hits`",
        "- `recency_rate = d7_hits`",
        "- `recency_deviation = d7_hits - d30_hits`",
        "- Segment A: `d7_hits < 0.50` and `d30_hits >= 0.75`",
        "- Segment B: `d7_hits < 0.50` and `d30_hits < 0.50`",
        "",
        "## Segment Summary",
        "",
        _md_table(
            seg,
            [
                "segment",
                "bets",
                "avg_model_prob",
                "actual_win_rate",
                "calibration_error",
                "avg_baseline_rate",
                "avg_recency_rate",
                "avg_recency_deviation",
            ],
        ),
        "",
        "## Focus: Under 0.5 Plus-Money With Model Prob >= 0.60",
        "",
        _md_table(
            focus,
            [
                "segment",
                "model_prob_group",
                "bets",
                "avg_model_prob",
                "actual_win_rate",
                "calibration_error",
                "avg_baseline_rate",
                "avg_recency_rate",
                "avg_recency_deviation",
            ],
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Compare hits recency against d30 baseline skill proxy.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_hits_reconcile(paths)
    engine = create_engine(_db_url(), pool_pre_ping=True)
    features = _prep_features(_fetch_features(engine, args.from_date, args.to_date))
    sides = _side_rows(reconcile, features)
    report = build_report(sides)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(out_csv, index=False)
    write_summary(report, out_md, args.from_date, args.to_date)
    print(f"[hits-recency-baseline] files={len(paths)} side_rows={len(sides)} out_csv={out_csv} out_md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
