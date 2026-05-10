#!/usr/bin/env python3
"""Quantify hits model variance mismatch against a Poisson baseline.

Diagnostics only. Uses full-slate reconcile rows and DB-backed d15/d30 hits
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
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_variance_analysis.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_variance_summary.md")

LAMBDA_BINS = [0.0, 0.25, 0.50, 0.75, 1.00, np.inf]
LAMBDA_LABELS = ["0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1.00", "1.00+"]


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
        "actual_over_outcome",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-variance] skip {path}: missing {missing}")
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
    for feature in ("d15_hits", "d30_hits"):
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
    return out[["date_key", "game_id_key", "player_id_key", "d15_hits", "d30_hits"]]


def _joined_rows(reconcile: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    work = reconcile.copy()
    work["date_key"] = work["game_date"].map(_date_key)
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work = work.merge(features, how="left", on=["date_key", "game_id_key", "player_id_key"])
    work["line"] = pd.to_numeric(work["line"], errors="coerce")
    work["model_prob_over_num"] = pd.to_numeric(work["model_prob_over"], errors="coerce")
    work["model_prob_under_num"] = 1.0 - work["model_prob_over_num"]
    work["outcome_over"] = work["actual_over_outcome"].map(lambda v: _clean(v).lower())
    work["outcome_under"] = np.where(work["outcome_over"].eq("win"), "loss", np.where(work["outcome_over"].eq("loss"), "win", ""))
    work = work[
        work["line"].eq(0.5)
        & work["outcome_over"].isin({"win", "loss"})
        & work["model_prob_over_num"].notna()
        & work["d30_hits"].notna()
    ].copy()
    pieces = []
    for side in ("over", "under"):
        side_df = work.copy()
        side_df["side"] = side
        side_df["model_prob"] = side_df[f"model_prob_{side}_num"]
        side_df["actual"] = side_df[f"outcome_{side}"].eq("win").astype(float)
        pieces.append(side_df)
    return pd.concat(pieces, ignore_index=True)


def _metrics(group: pd.DataFrame, lambda_col: str) -> dict[str, Any]:
    bets = int(len(group))
    if bets == 0:
        return {}
    lam = pd.to_numeric(group[lambda_col], errors="coerce")
    if group["side"].nunique(dropna=False) == 1 and str(group["side"].iloc[0]) == "under":
        poisson_prob = np.exp(-lam)
    else:
        poisson_prob = 1.0 - np.exp(-lam)
    actual = float(group["actual"].mean())
    model = float(group["model_prob"].mean())
    poisson = float(poisson_prob.mean())
    return {
        "bets": bets,
        "avg_lambda_estimate": float(lam.mean()),
        "avg_model_prob": model,
        "avg_poisson_prob": poisson,
        "actual_rate": actual,
        "model_error": model - actual,
        "poisson_error": poisson - actual,
        "actual_variance": float(group["actual"].var(ddof=0)),
        "poisson_variance": float(lam.mean()),
        "model_abs_error": abs(model - actual),
        "poisson_abs_error": abs(poisson - actual),
    }


def build_report(rows: pd.DataFrame) -> pd.DataFrame:
    records = []
    for lambda_col in ("d30_hits", "d15_hits"):
        for side in ("over", "under"):
            work = rows[rows[lambda_col].notna() & rows["side"].eq(side)].copy()
            if work.empty:
                continue
            work["lambda_bucket"] = pd.cut(
                pd.to_numeric(work[lambda_col], errors="coerce"),
                bins=LAMBDA_BINS,
                labels=LAMBDA_LABELS,
                right=False,
                include_lowest=True,
            )
            for bucket, group in work.groupby("lambda_bucket", observed=True, dropna=False):
                rec = {
                    "lambda_source": lambda_col,
                    "side": side,
                    "lambda_bucket": str(bucket),
                }
                rec.update(_metrics(group, lambda_col))
                records.append(rec)
            rec = {
                "lambda_source": lambda_col,
                "side": side,
                "lambda_bucket": "ALL",
            }
            rec.update(_metrics(work, lambda_col))
            records.append(rec)
    return pd.DataFrame(records).sort_values(["lambda_source", "side", "lambda_bucket"])


def _fmt_pct(value: Any) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value) * 100:.1f}%"


def _fmt_num(value: Any) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value):.3f}"


def _md_table(df: pd.DataFrame, cols: list[str]) -> str:
    work = df[cols].copy().fillna("")
    for col in ["avg_model_prob", "avg_poisson_prob", "actual_rate", "model_error", "poisson_error", "actual_variance", "model_abs_error", "poisson_abs_error"]:
        if col in work:
            work[col] = work[col].map(_fmt_pct)
    if "poisson_variance" in work:
        work["poisson_variance"] = work["poisson_variance"].map(_fmt_num)
    if "avg_lambda_estimate" in work:
        work["avg_lambda_estimate"] = work["avg_lambda_estimate"].map(_fmt_num)
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def write_summary(report: pd.DataFrame, out_md: Path, from_date: str, to_date: str) -> None:
    d30 = report[report["lambda_source"].eq("d30_hits")].copy()
    all_rows = report[report["lambda_bucket"].eq("ALL")].copy()
    largest_mismatch = d30[d30["lambda_bucket"].ne("ALL")].sort_values("model_error", ascending=False)
    lines = [
        "# Hits Variance Analysis",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "This compares model hit/no-hit probabilities against a simple Poisson baseline:",
        "",
        "- over 0.5: `poisson_prob = 1 - exp(-lambda_estimate)`",
        "- under 0.5: `poisson_prob = exp(-lambda_estimate)`",
        "",
        "Primary lambda estimate: `d30_hits`; alternate: `d15_hits`.",
        "",
        "## Overall",
        "",
        _md_table(
            all_rows,
            [
                "lambda_source",
                "side",
                "bets",
                "avg_lambda_estimate",
                "avg_model_prob",
                "avg_poisson_prob",
                "actual_rate",
                "model_error",
                "poisson_error",
                "actual_variance",
                "poisson_variance",
            ],
        ),
        "",
        "## d30 Hits Buckets",
        "",
        _md_table(
            d30,
            [
                "side",
                "lambda_bucket",
                "bets",
                "avg_lambda_estimate",
                "avg_model_prob",
                "avg_poisson_prob",
                "actual_rate",
                "model_error",
                "poisson_error",
                "actual_variance",
                "poisson_variance",
            ],
        ),
        "",
        "## Largest Model Overstatement Buckets",
        "",
        _md_table(
            largest_mismatch,
            [
                "side",
                "lambda_bucket",
                "bets",
                "avg_model_prob",
                "avg_poisson_prob",
                "actual_rate",
                "model_error",
                "poisson_error",
                "model_abs_error",
                "poisson_abs_error",
            ],
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Quantify hits variance mismatch against Poisson baseline.")
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
    rows = _joined_rows(reconcile, features)
    report = build_report(rows)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(out_csv, index=False)
    write_summary(report, out_md, args.from_date, args.to_date)
    print(f"[hits-variance] files={len(paths)} rows={len(rows)} out_csv={out_csv} out_md={out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
