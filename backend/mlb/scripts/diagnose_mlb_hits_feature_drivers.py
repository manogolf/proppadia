#!/usr/bin/env python3
"""Diagnose model-input feature drivers for hits under-0.5 overconfidence.

This script uses outcome-backed reconcile rows only to define the cohorts, then
reconstructs available model input features from DB-backed feature sources:
mlb.model_training_props, mlb.player_derived_stats, and
mlb.prop_features_precomputed.features.

Diagnostics only. No ROI, rules, model changes, or DB writes.
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
DEFAULT_METADATA = Path("backend/mlb/modeling/feature_metadata.json")
DEFAULT_DIFF_CSV = Path("backend/mlb/exports/model_diagnostics/hits_feature_driver_diff.csv")
DEFAULT_SUMMARY_MD = Path("backend/mlb/exports/model_diagnostics/hits_feature_driver_summary.md")

CONTROL_GROUPS = [
    "control_under_05_plus_lt_060",
    "control_over_05_fav_ge_060",
]


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
        "player_name",
        "prop_type",
        "line",
        "price_over_american",
        "price_under_american",
        "model_prob_over",
        "model_prob_under",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[hits-feature-drivers] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq("hits")].copy()
        if df.empty:
            continue
        df["source_date"] = path.parent.name
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible hits reconcile rows found.")
    return pd.concat(frames, ignore_index=True)


def _add_cohorts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    out["line_num"] = pd.to_numeric(out["line"], errors="coerce")
    out["price_over"] = pd.to_numeric(out["price_over_american"], errors="coerce")
    out["price_under"] = pd.to_numeric(out["price_under_american"], errors="coerce")
    out["model_prob_over_num"] = pd.to_numeric(out["model_prob_over"], errors="coerce")
    out["model_prob_under_num"] = pd.to_numeric(out["model_prob_under"], errors="coerce")

    out["cohort_bad"] = (
        out["line_num"].eq(0.5) & out["price_under"].gt(0) & out["model_prob_under_num"].ge(0.60)
    )
    out["cohort_control_under_05_plus_lt_060"] = (
        out["line_num"].eq(0.5) & out["price_under"].gt(0) & out["model_prob_under_num"].lt(0.60)
    )
    out["cohort_control_over_05_fav_ge_060"] = (
        out["line_num"].eq(0.5) & out["price_over"].lt(0) & out["model_prob_over_num"].ge(0.60)
    )
    return out


def _feature_names(metadata_path: Path) -> list[str]:
    data = json.loads(metadata_path.read_text(encoding="utf-8"))
    names = data.get("hits", {}).get("random_forest") or data.get("hits", {}).get("logistic_regression")
    if not names:
        raise SystemExit(f"No hits feature list found in {metadata_path}")
    return list(dict.fromkeys(names))


def _fetch_feature_sources(engine, from_date: str, to_date: str) -> pd.DataFrame:
    sql = text(
        """
        SELECT
          mt.game_date,
          mt.game_id,
          mt.player_id,
          mt.player_name,
          mt.prop_type,
          mt.prop_value,
          mt.streak_type,
          mt.streak_count,
          mt.hit_streak,
          mt.win_streak,
          mt.rolling_result_avg_7,
          pds.d7_hits,
          pds.d7_home_runs,
          pds.d7_rbis,
          pds.d7_walks,
          pds.d15_hits,
          pds.d15_home_runs,
          pds.d15_rbis,
          pds.d15_walks,
          pds.d30_hits,
          pds.d30_home_runs,
          pds.d30_rbis,
          pds.d30_walks,
          pds.d7_total_bases,
          pds.d15_total_bases,
          pds.d30_total_bases,
          pds.d7_hits_runs_rbis,
          pds.d15_hits_runs_rbis,
          pds.d30_hits_runs_rbis,
          pds.d7_stolen_bases,
          pds.d15_stolen_bases,
          pds.d30_stolen_bases,
          pds.d7_strikeouts_batting,
          pds.d15_strikeouts_batting,
          pds.d30_strikeouts_batting,
          pds.d7_strikeouts_pitching,
          pds.d15_strikeouts_pitching,
          pds.d30_strikeouts_pitching,
          pds.d7_walks_allowed,
          pds.d15_walks_allowed,
          pds.d30_walks_allowed,
          pds.d7_earned_runs,
          pds.d15_earned_runs,
          pds.d30_earned_runs,
          pds.d7_hits_allowed,
          pds.d15_hits_allowed,
          pds.d30_hits_allowed,
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


def _flatten_features(df: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
    out = df.copy()
    for feature in feature_names:
        if feature == "streak_type":
            continue
        if feature in out.columns:
            out[feature] = pd.to_numeric(out[feature], errors="coerce")
            continue
        out[feature] = np.nan

    if "pfp_features" in out.columns:
        parsed = out["pfp_features"].map(_parse_json_obj)
        for feature in feature_names:
            if feature == "streak_type":
                continue
            mask = out[feature].isna()
            if mask.any():
                out.loc[mask, feature] = parsed[mask].map(lambda obj, key=feature: obj.get(key)).pipe(
                    pd.to_numeric, errors="coerce"
                )

    # Convert categorical feature to model-style flags for comparison.
    if "streak_type" in feature_names:
        streak = out.get("streak_type", pd.Series([""] * len(out), index=out.index)).map(lambda v: _clean(v).lower())
        categories = sorted(v for v in streak.dropna().unique() if v)
        for category in categories:
            out[f"streak_type__{category}"] = streak.eq(category).astype(float)

    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    sort_cols = ["date_key", "game_id_key", "player_id_key", "feature_set_tag", "model_tag"]
    present = [c for c in sort_cols if c in out.columns]
    out = out.sort_values(present).drop_duplicates(["date_key", "game_id_key", "player_id_key"], keep="last")
    return out


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


def _effect_size(bad: pd.Series, control: pd.Series) -> float:
    b = pd.to_numeric(bad, errors="coerce").dropna()
    c = pd.to_numeric(control, errors="coerce").dropna()
    if b.empty or c.empty:
        return np.nan
    pooled = np.sqrt((b.var(ddof=1) + c.var(ddof=1)) / 2.0)
    if not np.isfinite(pooled) or pooled == 0:
        return np.nan
    return float((b.mean() - c.mean()) / pooled)


def _available_feature_columns(features: pd.DataFrame, feature_names: list[str]) -> list[str]:
    cols = [f for f in feature_names if f != "streak_type" and f in features.columns]
    cols.extend([c for c in features.columns if c.startswith("streak_type__")])
    usable = []
    for col in cols:
        numeric = pd.to_numeric(features[col], errors="coerce")
        if numeric.notna().any():
            usable.append(col)
    return list(dict.fromkeys(usable))


def build_diff(cohorts: pd.DataFrame, features: pd.DataFrame, feature_names: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = cohorts.merge(
        features,
        how="left",
        on=["date_key", "game_id_key", "player_id_key"],
        suffixes=("", "_feature"),
    )
    bad = merged[merged["cohort_bad"]].copy()
    controls = {
        "control_under_05_plus_lt_060": merged[merged["cohort_control_under_05_plus_lt_060"]].copy(),
        "control_over_05_fav_ge_060": merged[merged["cohort_control_over_05_fav_ge_060"]].copy(),
    }
    feature_cols = _available_feature_columns(features, feature_names)
    records = []
    for feature in feature_cols:
        bad_values = pd.to_numeric(bad[feature], errors="coerce")
        for control_name, control in controls.items():
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
    return pd.DataFrame(records), merged


def _consistent_direction(series: pd.Series) -> str:
    vals = pd.to_numeric(series, errors="coerce").dropna()
    if vals.empty:
        return "unknown"
    if vals.gt(0).all():
        return "higher_in_bad"
    if vals.lt(0).all():
        return "lower_in_bad"
    return "mixed"


def _rank(diff: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for feature, group in diff.groupby("feature", dropna=False):
        effects = pd.to_numeric(group["effect_size"], errors="coerce").dropna()
        rows.append(
            {
                "feature": feature,
                "comparisons": int(len(group)),
                "valid_effect_comparisons": int(len(effects)),
                "mean_abs_effect_size": float(effects.abs().mean()) if not effects.empty else np.nan,
                "max_abs_effect_size": float(effects.abs().max()) if not effects.empty else np.nan,
                "mean_difference": float(group["difference"].mean(skipna=True)),
                "consistent_direction": _consistent_direction(group["difference"]),
                "mean_bad": float(group["mean_bad"].mean(skipna=True)),
                "mean_control": float(group["mean_control"].mean(skipna=True)),
                "avg_null_rate_bad": float(group["null_rate_bad"].mean(skipna=True)),
                "avg_null_rate_control": float(group["null_rate_control"].mean(skipna=True)),
            }
        )
    return pd.DataFrame(rows).sort_values(["mean_abs_effect_size", "max_abs_effect_size"], ascending=[False, False])


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


def write_summary(
    *,
    out_md: Path,
    rank: pd.DataFrame,
    diff: pd.DataFrame,
    merged: pd.DataFrame,
    from_date: str,
    to_date: str,
    feature_source_rows: int,
    feature_cols_count: int,
) -> None:
    bad_rows = int(merged["cohort_bad"].sum())
    c1_rows = int(merged["cohort_control_under_05_plus_lt_060"].sum())
    c2_rows = int(merged["cohort_control_over_05_fav_ge_060"].sum())
    matched_bad = int(merged[merged["cohort_bad"]]["game_date_feature"].notna().sum()) if "game_date_feature" in merged.columns else 0
    matched_all = int(merged["game_date_feature"].notna().sum()) if "game_date_feature" in merged.columns else 0

    top = rank.head(25).copy()
    for col in ["mean_abs_effect_size", "max_abs_effect_size", "mean_difference", "mean_bad", "mean_control"]:
        top[col] = top[col].map(_fmt)

    by_control = diff.copy()
    by_control["abs_effect_size"] = by_control["effect_size"].abs()
    by_control = by_control.sort_values(["control_group", "abs_effect_size"], ascending=[True, False])
    for col in ["mean_bad", "mean_control", "difference", "effect_size", "null_rate_bad", "null_rate_control"]:
        by_control[col] = by_control[col].map(_fmt)

    lines = [
        "# Hits Feature Driver Summary",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Focus rows: hits under 0.5, plus-money under price, `model_prob_under >= 0.60`.",
        "",
        "Controls:",
        "- hits under 0.5 plus-money with `model_prob_under < 0.60`",
        "- hits over 0.5 favorite with `model_prob_over >= 0.60`",
        "",
        "Feature sources:",
        "- `mlb.model_training_props`",
        "- `mlb.player_derived_stats`",
        "- `mlb.prop_features_precomputed.features`",
        "- expected hits feature names from `backend/mlb/modeling/feature_metadata.json`",
        "",
        "## Coverage",
        "",
        f"- Reconcile cohort rows: `{len(merged)}`",
        f"- DB feature source rows: `{feature_source_rows}`",
        f"- Available numeric/encoded feature columns: `{feature_cols_count}`",
        f"- Bad rows: `{bad_rows}`",
        f"- Control under 0.5 plus <0.60 rows: `{c1_rows}`",
        f"- Control over 0.5 favorite >=0.60 rows: `{c2_rows}`",
        f"- Feature-matched rows: `{matched_all}`",
        f"- Feature-matched bad rows: `{matched_bad}`",
        "",
        "## Ranked Feature Drivers",
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
            max_rows=25,
        ),
        "",
        "## Strongest Differences By Control",
        "",
        _md_table(
            by_control,
            [
                "control_group",
                "feature",
                "mean_bad",
                "mean_control",
                "difference",
                "effect_size",
                "null_rate_bad",
                "null_rate_control",
            ],
            max_rows=40,
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Diagnose hits bad-zone feature drivers from model input sources.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--feature-metadata", default=str(DEFAULT_METADATA))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-csv", default=str(DEFAULT_DIFF_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_SUMMARY_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    cohorts = _add_cohorts(_load_hits_reconcile(paths))
    feature_names = _feature_names(Path(args.feature_metadata))

    engine = create_engine(_db_url(), pool_pre_ping=True)
    source = _fetch_feature_sources(engine, args.from_date, args.to_date)
    features = _flatten_features(source, feature_names)
    diff, merged = build_diff(cohorts, features, feature_names)
    rank = _rank(diff)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    diff.to_csv(out_csv, index=False)
    rank_csv = out_csv.with_name(out_csv.stem.replace("_diff", "_rank") + ".csv")
    rank.to_csv(rank_csv, index=False)

    write_summary(
        out_md=out_md,
        rank=rank,
        diff=diff,
        merged=merged,
        from_date=args.from_date,
        to_date=args.to_date,
        feature_source_rows=len(source),
        feature_cols_count=len(_available_feature_columns(features, feature_names)),
    )

    print(
        "[hits-feature-drivers] "
        f"files={len(paths)} cohort_rows={len(cohorts)} source_rows={len(source)} "
        f"features={rank.shape[0]} out_csv={out_csv} rank_csv={rank_csv} out_md={out_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
