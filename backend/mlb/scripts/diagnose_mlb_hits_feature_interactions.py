#!/usr/bin/env python3
"""Diagnose pairwise feature interactions for hits under-0.5 overconfidence.

Diagnostics only. This script reads outcome-backed reconcile rows to define the
cohorts, reconstructs available model/input features from DB-backed feature
sources, and compares standardized pairwise interaction terms between the bad
zone and controls. It does not write DB rows or change model logic.
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
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_diagnostics/hits_feature_interactions.csv")
DEFAULT_OUT_MD = Path("backend/mlb/exports/model_diagnostics/hits_feature_interactions_summary.md")

RECENCY_FEATURES = [
    "d7_hits",
    "d15_hits",
    "rolling_result_avg_7",
    "d7_total_bases",
    "d15_total_bases",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
]

BASELINE_FEATURES = [
    "d30_hits",
    "d30_total_bases",
    "d30_hits_runs_rbis",
    "hit_streak",
    "win_streak",
    "streak_count",
]

OPPONENT_CONTEXT_FEATURES = [
    "opponent_encoded",
    "opponent_team_id",
    "bvp_at_bats",
    "bvp_hits",
    "bvp_total_bases",
    "bvp_plate_appearances",
    "bvp_strikeouts",
    "bvp_walks",
    "d7_hits_allowed",
    "d15_hits_allowed",
    "d30_hits_allowed",
]

TEAM_CONTEXT_FEATURES = [
    "team_id",
    "is_home",
    "streak_type__cold",
    "streak_type__hot",
]

LINE_FEATURES = ["line_num"]

PRICE_FEATURES = [
    "price_under",
    "price_over",
    "under_implied",
    "over_implied",
    "under_novig",
    "over_novig",
]

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


def _american_to_prob(value: Any) -> float:
    price = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(price) or price == 0:
        return np.nan
    if price > 0:
        return float(100.0 / (price + 100.0))
    return float((-price) / ((-price) + 100.0))


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
            print(f"[hits-feature-interactions] skip {path}: missing {missing}")
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

    out["over_implied"] = out["price_over"].map(_american_to_prob)
    out["under_implied"] = out["price_under"].map(_american_to_prob)
    denom = out["over_implied"] + out["under_implied"]
    out["over_novig"] = np.where(denom.gt(0), out["over_implied"] / denom, np.nan)
    out["under_novig"] = np.where(denom.gt(0), out["under_implied"] / denom, np.nan)

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
          mt.line,
          mt.team_id,
          mt.opponent_team_id,
          mt.opponent_encoded,
          mt.is_home,
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


def _flatten_features(df: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
    out = df.copy()
    numeric_candidates = set(feature_names) | {
        "line",
        "prop_value",
        "team_id",
        "opponent_team_id",
        "opponent_encoded",
        "is_home",
    }
    for feature in numeric_candidates:
        if feature == "streak_type":
            continue
        if feature in out.columns:
            out[feature] = pd.to_numeric(out[feature], errors="coerce")
        else:
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

    streak = out.get("streak_type", pd.Series([""] * len(out), index=out.index)).map(lambda v: _clean(v).lower())
    for category in sorted(v for v in streak.dropna().unique() if v):
        out[f"streak_type__{category}"] = streak.eq(category).astype(float)

    out["prop_value_num"] = pd.to_numeric(out.get("prop_value"), errors="coerce")
    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    present = [c for c in ["date_key", "game_id_key", "player_id_key", "feature_set_tag", "model_tag"] if c in out.columns]
    return out.sort_values(present).drop_duplicates(["date_key", "game_id_key", "player_id_key"], keep="last")


def _zscore(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    mean = numeric.mean(skipna=True)
    std = numeric.std(skipna=True, ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(np.nan, index=series.index)
    return (numeric - mean) / std


def _effect_size(bad: pd.Series, control: pd.Series) -> float:
    b = pd.to_numeric(bad, errors="coerce").dropna()
    c = pd.to_numeric(control, errors="coerce").dropna()
    if b.empty or c.empty:
        return np.nan
    pooled = np.sqrt((b.var(ddof=1) + c.var(ddof=1)) / 2.0)
    if not np.isfinite(pooled) or pooled == 0:
        return np.nan
    return float((b.mean() - c.mean()) / pooled)


def _present(cols: Iterable[str], df: pd.DataFrame) -> list[str]:
    out = []
    for col in cols:
        if col in df.columns and pd.to_numeric(df[col], errors="coerce").notna().any():
            out.append(col)
    return list(dict.fromkeys(out))


def _interaction_specs(df: pd.DataFrame) -> list[tuple[str, str, str]]:
    recency = _present(RECENCY_FEATURES, df)
    specs: list[tuple[str, str, str]] = []
    for category, cols in [
        ("recency_x_baseline", BASELINE_FEATURES),
        ("recency_x_opponent_context", OPPONENT_CONTEXT_FEATURES),
        ("recency_x_team_context", TEAM_CONTEXT_FEATURES),
        ("recency_x_line", LINE_FEATURES),
        ("recency_x_price", PRICE_FEATURES),
    ]:
        for left in recency:
            for right in _present(cols, df):
                if left == right:
                    continue
                specs.append((category, left, right))
    return specs


def build_interactions(cohorts: pd.DataFrame, features: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = cohorts.merge(
        features,
        how="left",
        on=["date_key", "game_id_key", "player_id_key"],
        suffixes=("", "_feature"),
    )

    # Prefer reconcile line/price fields; use DB fields only where they do not collide.
    if "prop_value_num" not in merged.columns and "prop_value_num_feature" in merged.columns:
        merged["prop_value_num"] = merged["prop_value_num_feature"]

    for col in PRICE_FEATURES + LINE_FEATURES + RECENCY_FEATURES + BASELINE_FEATURES + OPPONENT_CONTEXT_FEATURES + TEAM_CONTEXT_FEATURES:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    bad = merged[merged["cohort_bad"]].copy()
    controls = {
        "control_under_05_plus_lt_060": merged[merged["cohort_control_under_05_plus_lt_060"]].copy(),
        "control_over_05_fav_ge_060": merged[merged["cohort_control_over_05_fav_ge_060"]].copy(),
    }

    records = []
    specs = _interaction_specs(merged)
    for category, left, right in specs:
        left_z = _zscore(merged[left])
        right_z = _zscore(merged[right])
        interaction = left_z * right_z
        bad_values = interaction.loc[bad.index]
        for control_name, control in controls.items():
            control_values = interaction.loc[control.index]
            left_bad_raw = pd.to_numeric(bad[left], errors="coerce")
            left_control_raw = pd.to_numeric(control[left], errors="coerce")
            right_bad_raw = pd.to_numeric(bad[right], errors="coerce")
            right_control_raw = pd.to_numeric(control[right], errors="coerce")
            left_unique = pd.concat([left_bad_raw, left_control_raw], ignore_index=True).dropna().nunique()
            right_unique = pd.concat([right_bad_raw, right_control_raw], ignore_index=True).dropna().nunique()
            identifiable = bool(left_unique > 1 and right_unique > 1)
            mean_bad = float(bad_values.mean()) if bad_values.notna().any() else np.nan
            mean_control = float(control_values.mean()) if control_values.notna().any() else np.nan
            difference = mean_bad - mean_control if pd.notna(mean_bad) and pd.notna(mean_control) else np.nan
            effect = _effect_size(bad_values, control_values) if identifiable else np.nan
            records.append(
                {
                    "interaction_category": category,
                    "left_feature": left,
                    "right_feature": right,
                    "interaction": f"{left} x {right}",
                    "control_group": control_name,
                    "interaction_identifiable": identifiable,
                    "bad_rows": int(len(bad)),
                    "control_rows": int(len(control)),
                    "valid_bad_rows": int(bad_values.notna().sum()),
                    "valid_control_rows": int(control_values.notna().sum()),
                    "mean_bad": mean_bad,
                    "mean_control": mean_control,
                    "difference": difference if identifiable else np.nan,
                    "effect_size": effect,
                    "null_rate_bad": float(bad_values.isna().mean()) if len(bad_values) else np.nan,
                    "null_rate_control": float(control_values.isna().mean()) if len(control_values) else np.nan,
                    "left_unique_values": int(left_unique),
                    "right_unique_values": int(right_unique),
                    "non_identifiable_reason": ""
                    if identifiable
                    else "constant_left_or_right_within_bad_control_comparison",
                    "left_mean_bad": float(left_bad_raw.mean(skipna=True)),
                    "left_mean_control": float(left_control_raw.mean(skipna=True)),
                    "right_mean_bad": float(right_bad_raw.mean(skipna=True)),
                    "right_mean_control": float(right_control_raw.mean(skipna=True)),
                }
            )

    out = pd.DataFrame(records)
    if out.empty:
        return out, merged
    out["abs_effect_size"] = pd.to_numeric(out["effect_size"], errors="coerce").abs()
    out["abs_difference"] = pd.to_numeric(out["difference"], errors="coerce").abs()
    out = out.sort_values(["interaction_identifiable", "abs_effect_size", "abs_difference"], ascending=[False, False, False])
    return out, merged


def _fmt(value: Any, digits: int = 4) -> str:
    if pd.isna(value):
        return "NA"
    return f"{float(value):.{digits}f}"


def _md_table(df: pd.DataFrame, cols: list[str], max_rows: int = 25) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].head(max_rows).fillna("")
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def _aggregate_rank(interactions: pd.DataFrame) -> pd.DataFrame:
    if interactions.empty:
        return interactions
    rows = []
    for (category, left, right, name), group in interactions.groupby(
        ["interaction_category", "left_feature", "right_feature", "interaction"], dropna=False
    ):
        identifiable_group = group[group["interaction_identifiable"].astype(bool)]
        effects = pd.to_numeric(group["effect_size"], errors="coerce").dropna()
        diffs = pd.to_numeric(group["difference"], errors="coerce").dropna()
        rows.append(
            {
                "interaction_category": category,
                "left_feature": left,
                "right_feature": right,
                "interaction": name,
                "comparisons": int(len(group)),
                "identifiable_comparisons": int(len(identifiable_group)),
                "mean_abs_effect_size": float(effects.abs().mean()) if not effects.empty else np.nan,
                "max_abs_effect_size": float(effects.abs().max()) if not effects.empty else np.nan,
                "mean_abs_difference": float(diffs.abs().mean()) if not diffs.empty else np.nan,
                "mean_difference": float(diffs.mean()) if not diffs.empty else np.nan,
                "mean_bad": float(group["mean_bad"].mean(skipna=True)),
                "mean_control": float(group["mean_control"].mean(skipna=True)),
                "avg_null_rate_bad": float(group["null_rate_bad"].mean(skipna=True)),
                "avg_null_rate_control": float(group["null_rate_control"].mean(skipna=True)),
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["identifiable_comparisons", "mean_abs_effect_size", "max_abs_effect_size"],
        ascending=[False, False, False],
    )


def write_summary(
    *,
    out_md: Path,
    interactions: pd.DataFrame,
    rank: pd.DataFrame,
    merged: pd.DataFrame,
    from_date: str,
    to_date: str,
    feature_source_rows: int,
) -> None:
    bad_rows = int(merged["cohort_bad"].sum())
    c1_rows = int(merged["cohort_control_under_05_plus_lt_060"].sum())
    c2_rows = int(merged["cohort_control_over_05_fav_ge_060"].sum())
    matched_bad = int(merged[merged["cohort_bad"]]["game_date_feature"].notna().sum()) if "game_date_feature" in merged.columns else 0
    matched_all = int(merged["game_date_feature"].notna().sum()) if "game_date_feature" in merged.columns else 0

    top_rank = rank.head(25).copy()
    for col in ["mean_abs_effect_size", "max_abs_effect_size", "mean_abs_difference", "mean_bad", "mean_control"]:
        top_rank[col] = top_rank[col].map(_fmt)

    top_by_control = interactions.head(40).copy()
    for col in [
        "mean_bad",
        "mean_control",
        "difference",
        "effect_size",
        "null_rate_bad",
        "null_rate_control",
        "left_mean_bad",
        "left_mean_control",
        "right_mean_bad",
        "right_mean_control",
    ]:
        if col in top_by_control.columns:
            top_by_control[col] = top_by_control[col].map(_fmt)

    by_category = (
        rank[rank["identifiable_comparisons"].gt(0)]
        .groupby("interaction_category", dropna=False)
        .agg(
            interactions=("interaction", "count"),
            mean_abs_effect_size=("mean_abs_effect_size", "mean"),
            max_abs_effect_size=("max_abs_effect_size", "max"),
        )
        .reset_index()
        .sort_values("max_abs_effect_size", ascending=False)
    )
    for col in ["mean_abs_effect_size", "max_abs_effect_size"]:
        by_category[col] = by_category[col].map(_fmt)

    lines = [
        "# Hits Feature Interaction Summary",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Focus rows: hits under 0.5, plus-money under price, `model_prob_under >= 0.60`.",
        "",
        "Controls:",
        "- hits under 0.5 plus-money with `model_prob_under < 0.60`",
        "- hits over 0.5 favorite with `model_prob_over >= 0.60`",
        "",
        "Interaction value: z-scored left feature multiplied by z-scored right feature across the matched sample.",
        "Interactions where either side is constant within a bad/control comparison are marked non-identifiable and excluded from the ranking metrics.",
        "",
        "Feature sources:",
        "- `mlb.model_training_props`",
        "- `mlb.player_derived_stats`",
        "- `mlb.prop_features_precomputed.features`",
        "- market line/price/model surface from outcome-backed `reconcile_rows.csv`",
        "",
        "## Coverage",
        "",
        f"- Reconcile cohort rows: `{len(merged)}`",
        f"- DB feature source rows: `{feature_source_rows}`",
        f"- Bad rows: `{bad_rows}`",
        f"- Control under 0.5 plus <0.60 rows: `{c1_rows}`",
        f"- Control over 0.5 favorite >=0.60 rows: `{c2_rows}`",
        f"- Feature-matched rows: `{matched_all}`",
        f"- Feature-matched bad rows: `{matched_bad}`",
        f"- Interaction comparisons: `{len(interactions)}`",
        "",
        "## Category Strength",
        "",
        _md_table(
            by_category,
            ["interaction_category", "interactions", "mean_abs_effect_size", "max_abs_effect_size"],
            max_rows=10,
        ),
        "",
        "## Ranked Interactions",
        "",
        _md_table(
            top_rank,
            [
                "interaction_category",
                "interaction",
                "mean_abs_effect_size",
                "max_abs_effect_size",
                "mean_bad",
                "mean_control",
            ],
            max_rows=25,
        ),
        "",
        "## Strongest Control Comparisons",
        "",
        _md_table(
            top_by_control,
            [
                "control_group",
                "interaction_category",
                "interaction",
                "mean_bad",
                "mean_control",
                "difference",
                "effect_size",
                "left_mean_bad",
                "left_mean_control",
                "right_mean_bad",
                "right_mean_control",
            ],
            max_rows=40,
        ),
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Diagnose hits bad-zone pairwise feature interactions.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--feature-metadata", default=str(DEFAULT_METADATA))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    cohorts = _add_cohorts(_load_hits_reconcile(paths))
    feature_names = _feature_names(Path(args.feature_metadata))

    engine = create_engine(_db_url(), pool_pre_ping=True)
    source = _fetch_feature_sources(engine, args.from_date, args.to_date)
    features = _flatten_features(source, feature_names)
    interactions, merged = build_interactions(cohorts, features)
    rank = _aggregate_rank(interactions)

    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    interactions.to_csv(out_csv, index=False)
    rank_csv = out_csv.with_name(out_csv.stem + "_rank.csv")
    rank.to_csv(rank_csv, index=False)

    write_summary(
        out_md=out_md,
        interactions=interactions,
        rank=rank,
        merged=merged,
        from_date=args.from_date,
        to_date=args.to_date,
        feature_source_rows=len(source),
    )

    print(
        "[hits-feature-interactions] "
        f"files={len(paths)} cohort_rows={len(cohorts)} source_rows={len(source)} "
        f"interactions={len(interactions)} out_csv={out_csv} rank_csv={rank_csv} out_md={out_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
