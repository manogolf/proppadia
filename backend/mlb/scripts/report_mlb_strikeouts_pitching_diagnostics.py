#!/usr/bin/env python3
"""Run a strikeouts_pitching diagnostic chain for calibration/population mix.

Replicates the hits diagnostic family at a practical level for pitcher
strikeouts: feature-only failure surfaces, recency sensitivity, recency versus
baseline, interaction/local base-rate surfaces, slope, and curvature.

Diagnostics only. No DB writes, model changes, ROI optimization, or betting
rules.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_DIR = Path("backend/mlb/exports/model_diagnostics")
DEFAULT_SUMMARY_MD = DEFAULT_OUT_DIR / "strikeouts_diagnostic_summary.md"

PROP_TYPE = "strikeouts_pitching"
RECENCY_FEATURES = ["d15_strikeouts_pitching", "d15_k_per9", "rolling_result_avg_7"]
BASELINE_FEATURES = ["d30_strikeouts_pitching", "d30_k_per9"]
CONTEXT_FEATURES = ["days_rest", "is_home", "d30_bb_per9", "d30_hits_allowed", "d30_earned_runs"]
ALL_FEATURES = list(dict.fromkeys([*RECENCY_FEATURES, *BASELINE_FEATURES, *CONTEXT_FEATURES]))

K_COUNT_BINS = [0.0, 3.0, 4.0, 5.0, 6.0, 7.0, np.inf]
K_COUNT_LABELS = ["0-3", "3-4", "4-5", "5-6", "6-7", "7+"]
RATE_BINS = [0.0, 6.0, 7.5, 9.0, 10.5, 12.0, np.inf]
RATE_LABELS = ["0-6", "6-7.5", "7.5-9", "9-10.5", "10.5-12", "12+"]
ROLLING_BINS = [0.0, 3.0, 4.0, 5.0, 6.0, 7.0, np.inf]
ROLLING_LABELS = K_COUNT_LABELS
LINE_BINS = [0.0, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, np.inf]
LINE_LABELS = ["<=3.5", "4.5", "5.5", "6.5", "7.5", "8.5", "9+"]
PROB_BINS = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, np.inf]
PROB_LABELS = ["0.50-0.55", "0.55-0.60", "0.60-0.65", "0.65-0.70", "0.70-0.75", "0.75+"]


sys.path.append(str(Path(__file__).resolve().parents[2]))
try:
    from mlb.shared.team_name_map import getFullTeamAbbreviationFromID, normalizeTeamAbbreviation
except Exception:  # pragma: no cover
    getFullTeamAbbreviationFromID = None
    normalizeTeamAbbreviation = None


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


def _team_label(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    if raw.replace(".", "", 1).isdigit() and getFullTeamAbbreviationFromID is not None:
        try:
            abbr = getFullTeamAbbreviationFromID(int(float(raw)))
            if abbr:
                return str(abbr).upper()
        except Exception:
            pass
    if normalizeTeamAbbreviation is not None:
        try:
            return str(normalizeTeamAbbreviation(raw)).upper()
        except Exception:
            pass
    return raw.upper()


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


def _load_reconcile(paths: Iterable[Path]) -> pd.DataFrame:
    required = {
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "prop_type",
        "line",
        "bookmaker_key",
        "home_team_code",
        "away_team_code",
        "model_prob_over",
        "model_prob_under",
        "actual_over_outcome",
        "actual_under_outcome",
    }
    frames = []
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[strikeouts-diag] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).eq(PROP_TYPE)].copy()
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible strikeouts_pitching reconcile rows found.")
    return pd.concat(frames, ignore_index=True)


def _table_columns(engine, table: str) -> set[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'mlb'
          AND table_name = :table
        """
    )
    with engine.connect() as conn:
        return {str(r[0]) for r in conn.execute(sql, {"table": table}).fetchall()}


def _select_col(alias: str, col: str, available: set[str]) -> str:
    if col in available:
        return f"{alias}.{col} AS {alias}_{col}"
    return f"NULL AS {alias}_{col}"


def _fetch_features(engine, from_date: str, to_date: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    mt_cols = _table_columns(engine, "model_training_props")
    pds_cols = _table_columns(engine, "player_derived_stats")
    select_cols = [
        "mt.game_date",
        "mt.game_id",
        "mt.player_id",
        "mt.player_name",
        _select_col("mt", "team", mt_cols),
        _select_col("mt", "opponent", mt_cols),
        _select_col("mt", "team_id", mt_cols),
        _select_col("mt", "opponent_team_id", mt_cols),
        _select_col("mt", "opponent_encoded", mt_cols),
        _select_col("mt", "is_home", mt_cols),
        _select_col("mt", "time_of_day_bucket", mt_cols),
        _select_col("mt", "game_day_of_week", mt_cols),
        _select_col("mt", "rolling_result_avg_7", mt_cols),
        _select_col("mt", "days_rest", mt_cols),
    ]
    for col in ALL_FEATURES:
        if col in {"rolling_result_avg_7", "days_rest", "is_home"}:
            continue
        select_cols.append(_select_col("pds", col, pds_cols))
    select_cols.extend(["pfp.features AS pfp_features", "pfp.feature_set_tag", "pfp.model_tag"])
    select_sql = ",\n          ".join(select_cols)
    sql = text(
        f"""
        SELECT
          {select_sql}
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
        WHERE mt.prop_type = :prop_type
          AND mt.game_date BETWEEN :from_date AND :to_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"prop_type": PROP_TYPE, "from_date": from_date, "to_date": to_date})
    meta = {
        "model_training_props_columns_found": sorted(set(["team", "opponent", "team_id", "opponent_team_id", "is_home", "days_rest", "rolling_result_avg_7"]) & mt_cols),
        "player_derived_stats_feature_columns_found": sorted(set(ALL_FEATURES) & pds_cols),
        "feature_columns_missing_from_pds": sorted(set(ALL_FEATURES) - pds_cols - mt_cols),
    }
    return df, meta


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
    for feature in ALL_FEATURES:
        candidates = [feature, f"mt_{feature}", f"pds_{feature}"]
        vals = pd.Series(np.nan, index=out.index, dtype="float64")
        for col in candidates:
            if col in out.columns:
                candidate_vals = pd.to_numeric(out[col], errors="coerce").astype("float64")
                vals = vals.where(vals.notna(), candidate_vals)
        mask = vals.isna()
        if mask.any():
            vals.loc[mask] = parsed[mask].map(lambda obj, key=feature: obj.get(key)).pipe(pd.to_numeric, errors="coerce")
        out[feature] = vals

    out["team"] = out.apply(lambda r: _team_label(r.get("mt_team") or r.get("mt_team_id")), axis=1)
    out["opponent"] = out.apply(lambda r: _team_label(r.get("mt_opponent") or r.get("mt_opponent_team_id") or r.get("mt_opponent_encoded")), axis=1)
    out["home_away"] = pd.to_numeric(out.get("mt_is_home"), errors="coerce").map(
        lambda v: "home" if pd.notna(v) and bool(v) else ("away" if pd.notna(v) else "")
    )
    out["time_of_day_bucket"] = out.get("mt_time_of_day_bucket", pd.Series([""] * len(out))).map(_clean)
    out["game_day_of_week"] = out.get("mt_game_day_of_week", pd.Series([""] * len(out))).map(_clean)
    out["date_key"] = out["game_date"].map(_date_key)
    out["game_id_key"] = pd.to_numeric(out["game_id"], errors="coerce").astype("Int64")
    out["player_id_key"] = pd.to_numeric(out["player_id"], errors="coerce").astype("Int64")
    sort_cols = ["date_key", "game_id_key", "player_id_key", "feature_set_tag", "model_tag"]
    out = out.sort_values([c for c in sort_cols if c in out.columns]).drop_duplicates(
        ["date_key", "game_id_key", "player_id_key"], keep="last"
    )
    keep = [
        "date_key",
        "game_id_key",
        "player_id_key",
        "team",
        "opponent",
        "home_away",
        "time_of_day_bucket",
        "game_day_of_week",
        *ALL_FEATURES,
    ]
    return out[keep]


def _side_rows(reconcile: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    work = reconcile.copy()
    work["date_key"] = work["game_date"].map(_date_key)
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work["line_num"] = pd.to_numeric(work["line"], errors="coerce")
    work = work.merge(features, how="left", on=["date_key", "game_id_key", "player_id_key"])
    work["team"] = work["team"].where(work["team"].map(_clean).ne(""), work["home_team_code"].map(_team_label))
    work["opponent"] = work["opponent"].where(work["opponent"].map(_clean).ne(""), work["away_team_code"].map(_team_label))

    pieces = []
    for side in ("over", "under"):
        side_df = pd.DataFrame(
            {
                "side": side,
                "line": work["line_num"],
                "line_bucket": pd.cut(work["line_num"], LINE_BINS, labels=LINE_LABELS, right=True, include_lowest=True).astype(str),
                "bookmaker_key": work["bookmaker_key"].map(_clean),
                "team": work["team"].map(_clean),
                "opponent": work["opponent"].map(_clean),
                "home_away": work["home_away"].map(_clean),
                "time_of_day_bucket": work["time_of_day_bucket"].map(_clean),
                "game_day_of_week": work["game_day_of_week"].map(_clean),
                "model_prob": pd.to_numeric(work[f"model_prob_{side}"], errors="coerce"),
                "outcome": work[f"actual_{side}_outcome"].map(lambda v: _clean(v).lower()),
            }
        )
        for feature in ALL_FEATURES:
            side_df[feature] = pd.to_numeric(work[feature], errors="coerce")
        pieces.append(side_df)
    rows = pd.concat(pieces, ignore_index=True)
    rows = rows[rows["outcome"].isin({"win", "loss"}) & rows["model_prob"].notna() & rows["line"].notna()].copy()
    rows["win"] = rows["outcome"].eq("win").astype(float)
    rows["model_prob_bucket"] = pd.cut(rows["model_prob"], PROB_BINS, labels=PROB_LABELS, right=False)
    rows["d15_k_bucket"] = pd.cut(rows["d15_strikeouts_pitching"], K_COUNT_BINS, labels=K_COUNT_LABELS, right=False, include_lowest=True)
    rows["d30_k_bucket"] = pd.cut(rows["d30_strikeouts_pitching"], K_COUNT_BINS, labels=K_COUNT_LABELS, right=False, include_lowest=True)
    rows["d15_k_per9_bucket"] = pd.cut(rows["d15_k_per9"], RATE_BINS, labels=RATE_LABELS, right=False, include_lowest=True)
    rows["d30_k_per9_bucket"] = pd.cut(rows["d30_k_per9"], RATE_BINS, labels=RATE_LABELS, right=False, include_lowest=True)
    rows["rolling_result_avg_7_bucket"] = pd.cut(rows["rolling_result_avg_7"], ROLLING_BINS, labels=ROLLING_LABELS, right=False, include_lowest=True)
    return rows


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    model = float(group["model_prob"].mean()) if bets else np.nan
    actual = float(group["win"].mean()) if bets else np.nan
    return {
        "bets": bets,
        "avg_model_prob": model,
        "actual_win_rate": actual,
        "calibration_error": actual - model if bets else np.nan,
    }


def _add_groups(records: list[dict[str, Any]], data: pd.DataFrame, section: str, cols: list[str]) -> None:
    data = data.copy()
    for col in cols:
        if col in data.columns:
            data = data[data[col].map(_clean).ne("")]
    if data.empty:
        return
    for keys, group in data.groupby(cols, observed=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {"section": section}
        row.update(dict(zip(cols, [str(k) for k in keys])))
        row.update(_metrics(group))
        records.append(row)


def build_feature_only_surface(rows: pd.DataFrame) -> pd.DataFrame:
    data = rows[rows["model_prob"].ge(0.60)].copy()
    records: list[dict[str, Any]] = []
    group_defs = [
        ["side"],
        ["side", "line_bucket"],
        ["side", "d15_k_bucket"],
        ["side", "d30_k_bucket"],
        ["side", "d15_k_per9_bucket"],
        ["side", "d30_k_per9_bucket"],
        ["side", "rolling_result_avg_7_bucket"],
        ["side", "model_prob_bucket"],
    ]
    for cols in group_defs:
        _add_groups(records, data, "feature_only_model_prob_ge_060", cols)
    return _finish_surface(pd.DataFrame(records))


def build_recency_surface(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    group_defs = [
        ["side", "d15_k_bucket"],
        ["side", "d30_k_bucket"],
        ["side", "d15_k_per9_bucket"],
        ["side", "d30_k_per9_bucket"],
        ["side", "rolling_result_avg_7_bucket"],
        ["side", "line_bucket", "d15_k_bucket"],
        ["side", "line_bucket", "d30_k_bucket"],
    ]
    for cols in group_defs:
        _add_groups(records, rows, "recency_sensitivity", cols)
    return _finish_surface(pd.DataFrame(records))


def build_recency_vs_baseline(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    group_defs = [
        ["side", "d15_k_bucket", "d30_k_bucket"],
        ["side", "d15_k_per9_bucket", "d30_k_per9_bucket"],
        ["side", "rolling_result_avg_7_bucket", "d30_k_bucket"],
    ]
    for cols in group_defs:
        _add_groups(records, rows, "recency_vs_baseline", cols)
    return _finish_surface(pd.DataFrame(records))


def build_interaction_surface(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    group_defs = [
        ["side", "d15_k_bucket", "line_bucket"],
        ["side", "d30_k_bucket", "line_bucket"],
        ["side", "d15_k_per9_bucket", "line_bucket"],
        ["side", "d15_k_bucket", "home_away"],
        ["side", "d15_k_bucket", "d30_bb_per9_bucket"],
    ]
    data = rows.copy()
    data["d30_bb_per9_bucket"] = pd.cut(data["d30_bb_per9"], RATE_BINS, labels=RATE_LABELS, right=False, include_lowest=True)
    for cols in group_defs:
        _add_groups(records, data, "interaction_failure", cols)
    return _finish_surface(pd.DataFrame(records))


def build_local_base_rate(rows: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    group_defs = [
        ["side", "line_bucket"],
        ["side", "team"],
        ["side", "opponent"],
        ["side", "home_away"],
        ["side", "time_of_day_bucket"],
        ["side", "game_day_of_week"],
        ["side", "bookmaker_key"],
        ["side", "days_rest_bucket"],
    ]
    data = rows.copy()
    data["days_rest_bucket"] = pd.cut(
        data["days_rest"],
        [-np.inf, 3, 4, 5, 6, np.inf],
        labels=["<=3", "4", "5", "6", "7+"],
        right=True,
    )
    for cols in group_defs:
        _add_groups(records, data, "local_base_rate", cols)
    return _finish_surface(pd.DataFrame(records))


def _finish_surface(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["abs_calibration_error"] = pd.to_numeric(df["calibration_error"], errors="coerce").abs()
    df["sample_size_flag"] = np.select(
        [df["bets"].ge(75), df["bets"].ge(25)],
        ["strong_sample", "usable"],
        default="low_sample",
    )
    df["usable_sample"] = df["bets"].ge(25)
    return df.sort_values(["usable_sample", "abs_calibration_error", "bets"], ascending=[False, False, False])


def _safe_slope(x: pd.Series, y: pd.Series) -> float:
    data = pd.DataFrame({"x": pd.to_numeric(x, errors="coerce"), "y": pd.to_numeric(y, errors="coerce")}).dropna()
    if len(data) < 2 or data["x"].nunique() < 2:
        return np.nan
    return float(np.polyfit(data["x"].astype(float), data["y"].astype(float), deg=1)[0])


def build_slope(rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    records = []
    surface_rows = []
    specs = [
        ("d15_strikeouts_pitching", "d15_k_bucket"),
        ("d30_strikeouts_pitching", "d30_k_bucket"),
        ("d15_k_per9", "d15_k_per9_bucket"),
        ("d30_k_per9", "d30_k_per9_bucket"),
        ("rolling_result_avg_7", "rolling_result_avg_7_bucket"),
    ]
    for feature, bucket_col in specs:
        data = rows[rows[bucket_col].notna() & rows[feature].notna()].copy()
        for (side, bucket), group in data.groupby(["side", bucket_col], observed=True, dropna=False):
            surface_rows.append(
                {
                    "feature": feature,
                    "feature_bucket": str(bucket),
                    "side": str(side),
                    "bets": int(len(group)),
                    "avg_feature_value": float(group[feature].mean()),
                    "avg_model_prob": float(group["model_prob"].mean()),
                    "actual_win_rate": float(group["win"].mean()),
                    "calibration_error": float(group["win"].mean() - group["model_prob"].mean()),
                }
            )
    surface = pd.DataFrame(surface_rows)
    if surface.empty:
        return pd.DataFrame(), surface
    for (feature, side), group in surface.groupby(["feature", "side"], dropna=False):
        slope_model = _safe_slope(group["avg_feature_value"], group["avg_model_prob"])
        slope_actual = _safe_slope(group["avg_feature_value"], group["actual_win_rate"])
        records.append(
            {
                "feature": feature,
                "side": side,
                "buckets": int(len(group)),
                "bets": int(group["bets"].sum()),
                "slope_model": slope_model,
                "slope_actual": slope_actual,
                "slope_ratio": slope_model / slope_actual if pd.notna(slope_actual) and slope_actual != 0 else np.nan,
                "slope_gap": slope_model - slope_actual if pd.notna(slope_model) and pd.notna(slope_actual) else np.nan,
            }
        )
    return pd.DataFrame(records).sort_values(["side", "feature"]), surface.sort_values(["feature", "side", "feature_bucket"])


def build_curvature(slope_surface: pd.DataFrame) -> pd.DataFrame:
    records = []
    for (feature, side), group in slope_surface.groupby(["feature", "side"], dropna=False):
        curve = group.sort_values("avg_feature_value").reset_index(drop=True).copy()
        curve["next_feature_bucket"] = curve["feature_bucket"].shift(-1)
        curve["next_avg_model_prob"] = curve["avg_model_prob"].shift(-1)
        curve["next_actual_win_rate"] = curve["actual_win_rate"].shift(-1)
        curve["delta_model"] = curve["next_avg_model_prob"] - curve["avg_model_prob"]
        curve["delta_actual"] = curve["next_actual_win_rate"] - curve["actual_win_rate"]
        curve["curvature_error"] = curve["delta_model"] - curve["delta_actual"]
        curve["abs_curvature_error"] = curve["curvature_error"].abs()
        records.append(curve)
    return pd.concat(records, ignore_index=True) if records else pd.DataFrame()


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


def _format_metrics(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = out[col].map(_fmt)
    return out


def write_summary(
    *,
    out_md: Path,
    rows: pd.DataFrame,
    meta: dict[str, Any],
    outputs: dict[str, Path],
    feature_only: pd.DataFrame,
    recency: pd.DataFrame,
    recency_baseline: pd.DataFrame,
    interactions: pd.DataFrame,
    local: pd.DataFrame,
    slopes: pd.DataFrame,
    curvature: pd.DataFrame,
    from_date: str,
    to_date: str,
    files: int,
) -> None:
    overview_rows = []
    for cols, label in [(["side"], "side"), (["side", "line_bucket"], "side_line"), (["side", "model_prob_bucket"], "side_model_prob")]:
        for keys, group in rows.groupby(cols, observed=True, dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {"group": label}
            row.update(dict(zip(cols, [str(k) for k in keys])))
            row.update(_metrics(group))
            overview_rows.append(row)
    overview = _format_metrics(pd.DataFrame(overview_rows), ["avg_model_prob", "actual_win_rate", "calibration_error"])

    def top(df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
        if df.empty:
            return df
        return _format_metrics(
            df[df["bets"].ge(25)].head(n),
            ["avg_model_prob", "actual_win_rate", "calibration_error", "abs_calibration_error"],
        )

    slopes_fmt = _format_metrics(slopes.copy(), ["slope_model", "slope_actual", "slope_ratio", "slope_gap"])
    curvature_fmt = _format_metrics(
        curvature[curvature["bets"].ge(25)].sort_values("abs_curvature_error", ascending=False).head(15),
        ["avg_model_prob", "actual_win_rate", "delta_model", "delta_actual", "curvature_error", "abs_curvature_error"],
    )
    overall_side = rows.groupby("side", observed=True).apply(lambda g: pd.Series(_metrics(g)), include_groups=False).reset_index()
    over_error = float(overall_side.loc[overall_side["side"].eq("over"), "calibration_error"].iloc[0]) if overall_side["side"].eq("over").any() else np.nan
    under_error = float(overall_side.loc[overall_side["side"].eq("under"), "calibration_error"].iloc[0]) if overall_side["side"].eq("under").any() else np.nan
    high_conf = feature_only[feature_only["bets"].ge(25)].copy()
    high_conf_max = float(high_conf["abs_calibration_error"].max()) if not high_conf.empty else np.nan
    recency_max = float(recency[recency["bets"].ge(25)]["abs_calibration_error"].max()) if not recency.empty else np.nan
    baseline_max = (
        float(recency_baseline[recency_baseline["bets"].ge(25)]["abs_calibration_error"].max())
        if not recency_baseline.empty
        else np.nan
    )
    slope_roll = slopes[slopes["feature"].eq("rolling_result_avg_7")]
    roll_slope_ratio = float(slope_roll["slope_ratio"].abs().mean()) if not slope_roll.empty else np.nan

    lines = [
        "# Strikeouts Pitching Diagnostic Summary",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        f"Reconcile files: `{files}`",
        f"Evaluated side rows: `{len(rows)}`",
        "",
        "## Source Features",
        "",
        f"- model_training_props columns found: `{meta.get('model_training_props_columns_found')}`",
        f"- player_derived_stats feature columns found: `{meta.get('player_derived_stats_feature_columns_found')}`",
        f"- feature columns missing from pds/mtp before JSON fallback: `{meta.get('feature_columns_missing_from_pds')}`",
        "",
        "## Outputs",
        "",
        *[f"- `{name}`: `{path}`" for name, path in outputs.items()],
        "",
        "## Overall Calibration",
        "",
        _md_table(overview, ["group", "side", "line_bucket", "model_prob_bucket", "bets", "avg_model_prob", "actual_win_rate", "calibration_error"], 30),
        "",
        "## Feature-Only Failure Surface Top Errors",
        "",
        _md_table(top(feature_only), list(top(feature_only).columns[:12]), 15),
        "",
        "## Recency Sensitivity Top Errors",
        "",
        _md_table(top(recency), list(top(recency).columns[:12]), 15),
        "",
        "## Recency vs Baseline Top Errors",
        "",
        _md_table(top(recency_baseline), list(top(recency_baseline).columns[:12]), 15),
        "",
        "## Interaction Failure Top Errors",
        "",
        _md_table(top(interactions), list(top(interactions).columns[:12]), 15),
        "",
        "## Local Base-Rate Top Errors",
        "",
        _md_table(top(local), list(top(local).columns[:12]), 15),
        "",
        "## Feature Slopes",
        "",
        _md_table(slopes_fmt, ["feature", "side", "buckets", "bets", "slope_model", "slope_actual", "slope_ratio", "slope_gap"], 20),
        "",
        "## Largest Curvature Errors",
        "",
        _md_table(
            curvature_fmt,
            ["feature", "feature_bucket", "side", "bets", "avg_model_prob", "actual_win_rate", "delta_model", "delta_actual", "curvature_error"],
            15,
        ),
        "",
        "## Population-Mixture Assessment",
        "",
        f"- Overall side calibration is modest: over error `{_fmt(over_error)}`, under error `{_fmt(under_error)}`.",
        f"- High-confidence feature-only pockets still reach max usable abs error `{_fmt(high_conf_max)}`.",
        f"- Recency sensitivity pockets reach max usable abs error `{_fmt(recency_max)}`.",
        f"- Recency-vs-baseline pockets reach max usable abs error `{_fmt(baseline_max)}`.",
        f"- `rolling_result_avg_7` slope ratio averages `{_fmt(roll_slope_ratio)}`, so the curve is flatter than actual bucket movement, not universally over-steep.",
        "",
        "Read: strikeouts_pitching shows a population-mix problem, but it is not the same shape as hits. Hits was dominated by under-0.5 mid-low recency overconfidence. Strikeouts_pitching is more line/context dependent: alternate lines and recency-baseline combinations create large local base-rate errors even when the overall side error is small.",
        "",
        "This report is diagnostic only. It does not create filters, ROI rules, or model changes.",
        "",
    ]
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run strikeouts_pitching diagnostic chain.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--summary-md", default=str(DEFAULT_SUMMARY_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_reconcile(paths)
    engine = create_engine(_db_url(), pool_pre_ping=True)
    source, meta = _fetch_features(engine, args.from_date, args.to_date)
    features = _prep_features(source)
    rows = _side_rows(reconcile, features)

    feature_only = build_feature_only_surface(rows)
    recency = build_recency_surface(rows)
    recency_baseline = build_recency_vs_baseline(rows)
    interactions = build_interaction_surface(rows)
    local = build_local_base_rate(rows)
    slopes, slope_surface = build_slope(rows)
    curvature = build_curvature(slope_surface)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "feature_only_failure_surface": out_dir / "strikeouts_feature_only_failure_surface.csv",
        "recency_sensitivity": out_dir / "strikeouts_recency_sensitivity.csv",
        "recency_vs_baseline": out_dir / "strikeouts_recency_vs_baseline.csv",
        "interaction_failure_surface": out_dir / "strikeouts_interaction_failure_surface.csv",
        "local_base_rate_surface": out_dir / "strikeouts_local_base_rate_surface.csv",
        "feature_slope": out_dir / "strikeouts_feature_slope.csv",
        "feature_slope_surface": out_dir / "strikeouts_feature_slope_surface.csv",
        "probability_curvature": out_dir / "strikeouts_probability_curvature.csv",
    }
    feature_only.to_csv(outputs["feature_only_failure_surface"], index=False)
    recency.to_csv(outputs["recency_sensitivity"], index=False)
    recency_baseline.to_csv(outputs["recency_vs_baseline"], index=False)
    interactions.to_csv(outputs["interaction_failure_surface"], index=False)
    local.to_csv(outputs["local_base_rate_surface"], index=False)
    slopes.to_csv(outputs["feature_slope"], index=False)
    slope_surface.to_csv(outputs["feature_slope_surface"], index=False)
    curvature.to_csv(outputs["probability_curvature"], index=False)

    write_summary(
        out_md=Path(args.summary_md),
        rows=rows,
        meta=meta,
        outputs=outputs,
        feature_only=feature_only,
        recency=recency,
        recency_baseline=recency_baseline,
        interactions=interactions,
        local=local,
        slopes=slopes,
        curvature=curvature,
        from_date=args.from_date,
        to_date=args.to_date,
        files=len(paths),
    )

    print(
        "[strikeouts-diag] "
        f"files={len(paths)} source_rows={len(source)} side_rows={len(rows)} "
        f"summary={args.summary_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
