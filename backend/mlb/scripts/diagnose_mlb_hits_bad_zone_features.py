#!/usr/bin/env python3
"""Compare available reconcile-row feature distributions for hits bad zone.

Diagnostics only. No ROI, betting rules, or calibration changes.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_DIFF_CSV = Path("backend/mlb/exports/model_diagnostics/hits_bad_zone_feature_diff.csv")
DEFAULT_RANK_CSV = Path("backend/mlb/exports/model_diagnostics/hits_bad_zone_feature_rank.csv")
DEFAULT_SUMMARY_MD = Path("backend/mlb/exports/model_diagnostics/hits_bad_zone_feature_summary.md")

OUTCOME_TOKENS = (
    "actual",
    "outcome",
    "pnl",
    "result",
    "win",
    "loss",
    "profit",
)
IDENTIFIER_COLUMNS = {
    "game_id",
    "player_id",
}
TEXT_COLUMNS = {
    "game_date",
    "slate_date",
    "home_team_code",
    "away_team_code",
    "player_name",
    "prop_type",
    "market_key",
    "bookmaker_key",
    "market_player_name",
    "odds_snapshot_file",
    "slate_source_file",
    "snapshot_run_tag",
    "snapshot_time_utc",
    "source_date",
}


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


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


def _load_hits(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    required = {
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
            print(f"[hits-bad-zone-features] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq("hits")].copy()
        if df.empty:
            continue
        df["source_date"] = path.parent.name
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible hits rows found.")
    return pd.concat(frames, ignore_index=True)


def _add_zone_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    line = pd.to_numeric(out["line"], errors="coerce")
    price_over = pd.to_numeric(out["price_over_american"], errors="coerce")
    price_under = pd.to_numeric(out["price_under_american"], errors="coerce")
    prob_over = pd.to_numeric(out["model_prob_over"], errors="coerce")
    prob_under = pd.to_numeric(out["model_prob_under"], errors="coerce")

    out["bad_zone"] = line.eq(0.5) & price_under.gt(0) & prob_under.ge(0.60)
    out["control_under_05_plus_lt_060"] = line.eq(0.5) & price_under.gt(0) & prob_under.lt(0.60)
    out["control_over_05_fav_ge_060"] = line.eq(0.5) & price_over.lt(0) & prob_over.ge(0.60)
    out["control_under_15_fav_ge_060"] = line.eq(1.5) & price_under.lt(0) & prob_under.ge(0.60)
    return out


def _feature_columns(df: pd.DataFrame) -> list[str]:
    cols = []
    for col in df.columns:
        lower = col.lower()
        if lower == "bad_zone" or lower.startswith("control_"):
            continue
        if col in IDENTIFIER_COLUMNS or col in TEXT_COLUMNS:
            continue
        if any(token in lower for token in OUTCOME_TOKENS):
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        if series.notna().any():
            cols.append(col)
    return cols


def _effect_size(bad: pd.Series, control: pd.Series) -> float:
    b = pd.to_numeric(bad, errors="coerce").dropna()
    c = pd.to_numeric(control, errors="coerce").dropna()
    if b.empty or c.empty:
        return np.nan
    pooled = np.sqrt((b.var(ddof=1) + c.var(ddof=1)) / 2.0)
    if not np.isfinite(pooled) or pooled == 0:
        return np.nan
    return float((b.mean() - c.mean()) / pooled)


def build_diff(df: pd.DataFrame) -> pd.DataFrame:
    feature_cols = _feature_columns(df)
    zones = {
        "control_under_05_plus_lt_060": df[df["control_under_05_plus_lt_060"]],
        "control_over_05_fav_ge_060": df[df["control_over_05_fav_ge_060"]],
        "control_under_15_fav_ge_060": df[df["control_under_15_fav_ge_060"]],
    }
    bad = df[df["bad_zone"]]
    records = []
    for feature in feature_cols:
        bad_values = pd.to_numeric(bad[feature], errors="coerce")
        for control_name, control in zones.items():
            control_values = pd.to_numeric(control[feature], errors="coerce")
            mean_bad = float(bad_values.mean()) if bad_values.notna().any() else np.nan
            mean_control = float(control_values.mean()) if control_values.notna().any() else np.nan
            records.append(
                {
                    "feature": feature,
                    "control_group": control_name,
                    "bad_rows": int(len(bad)),
                    "control_rows": int(len(control)),
                    "mean_bad": mean_bad,
                    "mean_control": mean_control,
                    "difference": mean_bad - mean_control
                    if pd.notna(mean_bad) and pd.notna(mean_control)
                    else np.nan,
                    "effect_size": _effect_size(bad_values, control_values),
                    "null_rate_bad": float(bad_values.isna().mean()) if len(bad_values) else np.nan,
                    "null_rate_control": float(control_values.isna().mean()) if len(control_values) else np.nan,
                }
            )
    return pd.DataFrame(records)


def build_rank(diff: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature, group in diff.groupby("feature", dropna=False):
        signed = group["effect_size"].dropna()
        rows.append(
            {
                "feature": feature,
                "comparisons": int(len(group)),
                "valid_effect_comparisons": int(signed.shape[0]),
                "mean_abs_effect_size": float(signed.abs().mean()) if not signed.empty else np.nan,
                "max_abs_effect_size": float(signed.abs().max()) if not signed.empty else np.nan,
                "mean_difference": float(group["difference"].mean(skipna=True)),
                "consistent_direction": _consistent_direction(group["difference"]),
                "mean_bad": float(group["mean_bad"].mean(skipna=True)),
                "mean_control": float(group["mean_control"].mean(skipna=True)),
                "avg_null_rate_bad": float(group["null_rate_bad"].mean(skipna=True)),
                "avg_null_rate_control": float(group["null_rate_control"].mean(skipna=True)),
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values(["mean_abs_effect_size", "max_abs_effect_size"], ascending=[False, False])


def _consistent_direction(series: pd.Series) -> str:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return "unknown"
    if vals.gt(0).all():
        return "higher_in_bad"
    if vals.lt(0).all():
        return "lower_in_bad"
    return "mixed"


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


def write_summary(df: pd.DataFrame, diff: pd.DataFrame, rank: pd.DataFrame, out_md: Path, from_date: str, to_date: str) -> None:
    counts = {
        "bad_zone": int(df["bad_zone"].sum()),
        "control_under_05_plus_lt_060": int(df["control_under_05_plus_lt_060"].sum()),
        "control_over_05_fav_ge_060": int(df["control_over_05_fav_ge_060"].sum()),
        "control_under_15_fav_ge_060": int(df["control_under_15_fav_ge_060"].sum()),
    }
    top = rank.head(20).copy()
    for col in ["mean_abs_effect_size", "max_abs_effect_size", "mean_difference", "mean_bad", "mean_control"]:
        if col in top.columns:
            top[col] = top[col].map(lambda v: _fmt(v))

    consistent = rank[rank["consistent_direction"].isin({"higher_in_bad", "lower_in_bad"})].head(30).copy()
    for col in ["mean_abs_effect_size", "max_abs_effect_size", "mean_difference", "mean_bad", "mean_control"]:
        if col in consistent.columns:
            consistent[col] = consistent[col].map(lambda v: _fmt(v))

    lines = [
        "# Hits Bad Zone Feature Diff",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Scope: `prop_type = hits`.",
        "",
        "Bad zone: `side=under`, `line=0.5`, `price=plus_money`, `model_prob >= 0.60`.",
        "",
        "Controls:",
        "- `control_under_05_plus_lt_060`: hits under 0.5 plus-money with model_prob < 0.60",
        "- `control_over_05_fav_ge_060`: hits over 0.5 favorite with model_prob >= 0.60",
        "- `control_under_15_fav_ge_060`: hits under 1.5 favorite with model_prob >= 0.60",
        "",
        "Note: `reconcile_rows.csv` contains prediction/market surface fields, not the full fit-time feature matrix. This report ranks available non-outcome numeric columns only.",
        "",
        "## Row Counts",
        "",
        f"- Bad zone rows: `{counts['bad_zone']}`",
        f"- Control under 0.5 plus <0.60 rows: `{counts['control_under_05_plus_lt_060']}`",
        f"- Control over 0.5 favorite >=0.60 rows: `{counts['control_over_05_fav_ge_060']}`",
        f"- Control under 1.5 favorite >=0.60 rows: `{counts['control_under_15_fav_ge_060']}`",
        "",
        "## Top Feature Differences",
        "",
        _md_table(
            top,
            [
                "feature",
                "mean_abs_effect_size",
                "max_abs_effect_size",
                "mean_difference",
                "consistent_direction",
                "mean_bad",
                "mean_control",
            ],
            max_rows=20,
        ),
        "",
        "## Consistent Direction Features",
        "",
        _md_table(
            consistent,
            [
                "feature",
                "mean_abs_effect_size",
                "mean_difference",
                "consistent_direction",
                "mean_bad",
                "mean_control",
            ],
            max_rows=30,
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Diagnose available feature drivers of hits bad-zone overconfidence.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-diff-csv", default=str(DEFAULT_DIFF_CSV))
    ap.add_argument("--out-rank-csv", default=str(DEFAULT_RANK_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_SUMMARY_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    hits = _add_zone_flags(_load_hits(paths))
    diff = build_diff(hits)
    rank = build_rank(diff)

    diff_path = Path(args.out_diff_csv)
    rank_path = Path(args.out_rank_csv)
    diff_path.parent.mkdir(parents=True, exist_ok=True)
    diff.to_csv(diff_path, index=False)
    rank.to_csv(rank_path, index=False)
    write_summary(hits, diff, rank, Path(args.out_md), args.from_date, args.to_date)

    print(
        "[hits-bad-zone-features] "
        f"files={len(paths)} hits_rows={len(hits)} bad_rows={int(hits['bad_zone'].sum())} "
        f"features={rank.shape[0]} out_diff={diff_path} out_rank={rank_path} out_md={args.out_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
