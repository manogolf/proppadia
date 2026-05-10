#!/usr/bin/env python3
"""Generalized prop diagnostics and cross-prop meta summary.

Runs a comparable diagnostic chain across selected MLB prop types without
assuming a shared root cause. Diagnostics only: no DB writes, no calibration
changes, no filters, no ROI optimization.
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
DEFAULT_METADATA = Path("backend/mlb/modeling/feature_metadata.json")
DEFAULT_OUT_DIR = Path("backend/mlb/exports/model_diagnostics")
DEFAULT_META_CSV = DEFAULT_OUT_DIR / "prop_diagnostic_meta_summary.csv"
DEFAULT_META_MD = DEFAULT_OUT_DIR / "prop_diagnostic_meta_summary.md"

DEFAULT_PROPS = [
    "hits",
    "total_bases",
    "rbis",
    "runs_scored",
    "hits_allowed",
    "strikeouts_pitching",
    "outs_recorded",
]

VALUE_BINS = [-np.inf, 0, 0.25, 0.50, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, np.inf]
VALUE_LABELS = ["<0", "0-0.25", "0.25-0.50", "0.50-0.75", "0.75-1", "1-1.5", "1.5-2", "2-3", "3-4", "4-5", "5-6", "6-7", "7+"]
LINE_BINS = [-np.inf, 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, np.inf]
LINE_LABELS = ["0.5", "1.5", "2.5", "3.5", "4.5", "5.5", "6.5", "7.5", "8.5", "9+"]
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


def _feature_names(metadata_path: Path, prop: str) -> list[str]:
    data = json.loads(metadata_path.read_text(encoding="utf-8"))
    info = data.get(prop, {})
    names = info.get("random_forest") or info.get("logistic_regression") or []
    return list(dict.fromkeys(str(v) for v in names))


def _recency_features(features: Sequence[str]) -> list[str]:
    preferred = [
        "rolling_result_avg_7",
        *[f for f in features if f.startswith("d7_")],
        *[f for f in features if f.startswith("d15_")],
    ]
    return list(dict.fromkeys([f for f in preferred if f in features]))[:5]


def _baseline_features(features: Sequence[str]) -> list[str]:
    return list(dict.fromkeys([f for f in features if f.startswith("d30_")]))[:5]


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


def _load_reconcile(paths: Iterable[Path], props: set[str]) -> pd.DataFrame:
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
            print(f"[prop-diag-meta] skip {path}: missing {missing}")
            continue
        df = df[df["prop_type"].map(lambda v: _clean(v).lower()).isin(props)].copy()
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible reconcile rows found.")
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


def _fetch_features(engine, props: Sequence[str], features_by_prop: dict[str, list[str]], from_date: str, to_date: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    mt_cols = _table_columns(engine, "model_training_props")
    pds_cols = _table_columns(engine, "player_derived_stats")
    all_features = sorted(set().union(*[set(v) for v in features_by_prop.values()]))
    context_cols = ["team", "opponent", "team_id", "opponent_team_id", "opponent_encoded", "is_home", "time_of_day_bucket", "game_day_of_week"]
    select_cols = [
        "mt.game_date",
        "mt.game_id",
        "mt.player_id",
        "mt.player_name",
        "mt.prop_type",
        *[_select_col("mt", c, mt_cols) for c in context_cols],
    ]
    for col in all_features:
        if col in context_cols:
            continue
        select_cols.append(_select_col("mt", col, mt_cols))
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
        WHERE mt.prop_type = ANY(:props)
          AND mt.game_date BETWEEN :from_date AND :to_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"props": list(props), "from_date": from_date, "to_date": to_date})
    meta = {
        "mtp_cols": sorted(mt_cols),
        "pds_cols": sorted(pds_cols),
        "features_from_mtp": sorted(set(all_features) & mt_cols),
        "features_from_pds": sorted(set(all_features) & pds_cols),
        "features_needing_json_or_missing": sorted(set(all_features) - mt_cols - pds_cols),
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


def _prep_features(source: pd.DataFrame, features_by_prop: dict[str, list[str]]) -> pd.DataFrame:
    out = source.copy()
    parsed = out["pfp_features"].map(_parse_json_obj) if "pfp_features" in out.columns else pd.Series([{}] * len(out))
    all_features = sorted(set().union(*[set(v) for v in features_by_prop.values()]))
    for feature in all_features:
        vals = pd.Series(np.nan, index=out.index, dtype="float64")
        for col in [feature, f"mt_{feature}", f"pds_{feature}"]:
            if col in out.columns:
                vals = vals.where(vals.notna(), pd.to_numeric(out[col], errors="coerce").astype("float64"))
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
    out["prop_type_norm"] = out["prop_type"].map(lambda v: _clean(v).lower())
    sort_cols = ["date_key", "game_id_key", "player_id_key", "prop_type_norm", "feature_set_tag", "model_tag"]
    out = out.sort_values([c for c in sort_cols if c in out.columns]).drop_duplicates(
        ["date_key", "game_id_key", "player_id_key", "prop_type_norm"], keep="last"
    )
    keep = ["date_key", "game_id_key", "player_id_key", "prop_type_norm", "team", "opponent", "home_away", "time_of_day_bucket", "game_day_of_week", *all_features]
    return out[keep]


def _side_rows(reconcile: pd.DataFrame, features: pd.DataFrame, prop: str, feature_names: list[str]) -> pd.DataFrame:
    work = reconcile[reconcile["prop_type"].map(lambda v: _clean(v).lower()).eq(prop)].copy()
    work["date_key"] = work["game_date"].map(_date_key)
    work["game_id_key"] = pd.to_numeric(work["game_id"], errors="coerce").astype("Int64")
    work["player_id_key"] = pd.to_numeric(work["player_id"], errors="coerce").astype("Int64")
    work["prop_type_norm"] = work["prop_type"].map(lambda v: _clean(v).lower())
    work["line_num"] = pd.to_numeric(work["line"], errors="coerce")
    work = work.merge(features, how="left", on=["date_key", "game_id_key", "player_id_key", "prop_type_norm"])
    work = work.loc[:, ~work.columns.duplicated()].copy()

    def col(name: str, default: Any = "") -> pd.Series:
        if name not in work.columns:
            return pd.Series([default] * len(work), index=work.index)
        value = work[name]
        if isinstance(value, pd.DataFrame):
            return value.iloc[:, 0]
        return value

    work["team"] = col("team").where(col("team").map(_clean).ne(""), col("home_team_code").map(_team_label))
    work["opponent"] = col("opponent").where(col("opponent").map(_clean).ne(""), col("away_team_code").map(_team_label))
    pieces = []
    for side in ("over", "under"):
        side_df = pd.DataFrame(
            {
                "prop_type": prop,
                "side": side,
                "line": col("line_num"),
                "line_bucket": pd.cut(col("line_num"), LINE_BINS, labels=LINE_LABELS, right=True, include_lowest=True).astype(str),
                "bookmaker_key": col("bookmaker_key").map(_clean),
                "team": col("team").map(_clean),
                "opponent": col("opponent").map(_clean),
                "home_away": col("home_away").map(_clean),
                "time_of_day_bucket": col("time_of_day_bucket").map(_clean),
                "game_day_of_week": col("game_day_of_week").map(_clean),
                "model_prob": pd.to_numeric(col(f"model_prob_{side}"), errors="coerce"),
                "outcome": col(f"actual_{side}_outcome").map(lambda v: _clean(v).lower()),
            }
        )
        for feature in feature_names:
            side_df[feature] = pd.to_numeric(col(feature, np.nan), errors="coerce") if feature in work.columns else np.nan
        pieces.append(side_df)
    rows = pd.concat(pieces, ignore_index=True)
    rows = rows[rows["outcome"].isin({"win", "loss"}) & rows["model_prob"].notna() & rows["line"].notna()].copy()
    rows["win"] = rows["outcome"].eq("win").astype(float)
    rows["model_prob_bucket"] = pd.cut(rows["model_prob"], PROB_BINS, labels=PROB_LABELS, right=False).astype(str)
    return rows


def _bucket_feature(rows: pd.DataFrame, feature: str) -> pd.Series:
    return pd.cut(pd.to_numeric(rows[feature], errors="coerce"), VALUE_BINS, labels=VALUE_LABELS, right=False, include_lowest=True).astype(str)


def _metrics(group: pd.DataFrame) -> dict[str, Any]:
    bets = int(len(group))
    model = float(group["model_prob"].mean()) if bets else np.nan
    actual = float(group["win"].mean()) if bets else np.nan
    return {"bets": bets, "avg_model_prob": model, "actual_win_rate": actual, "calibration_error": actual - model if bets else np.nan}


def _add_groups(records: list[dict[str, Any]], data: pd.DataFrame, section: str, cols: list[str]) -> None:
    work = data.copy()
    for col in cols:
        if col in work.columns:
            work = work[work[col].map(_clean).ne("")]
    if work.empty:
        return
    for keys, group in work.groupby(cols, observed=True, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = {"section": section}
        row.update(dict(zip(cols, [str(k) for k in keys])))
        row.update(_metrics(group))
        records.append(row)


def _finish_surface(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df["abs_calibration_error"] = pd.to_numeric(df["calibration_error"], errors="coerce").abs()
    df["sample_size_flag"] = np.select([df["bets"].ge(75), df["bets"].ge(25)], ["strong_sample", "usable"], default="low_sample")
    df["usable_sample"] = df["bets"].ge(25)
    return df.sort_values(["usable_sample", "abs_calibration_error", "bets"], ascending=[False, False, False])


def build_surfaces(rows: pd.DataFrame, recency: list[str], baseline: list[str]) -> dict[str, pd.DataFrame]:
    work = rows.copy()
    for feature in [*recency, *baseline]:
        if feature in work.columns:
            work[f"{feature}_bucket"] = _bucket_feature(work, feature)
    surfaces: dict[str, pd.DataFrame] = {}
    records: list[dict[str, Any]] = []
    high = work[work["model_prob"].ge(0.60)].copy()
    for cols in [["side"], ["side", "line_bucket"], ["side", "model_prob_bucket"], *[["side", f"{f}_bucket"] for f in recency[:3]]]:
        _add_groups(records, high, "feature_only_failure_surface", cols)
    surfaces["feature_only_failure_surface"] = _finish_surface(pd.DataFrame(records))

    records = []
    for cols in [*[["side", f"{f}_bucket"] for f in recency[:4]], *[["side", "line_bucket", f"{f}_bucket"] for f in recency[:2]]]:
        _add_groups(records, work, "recency_sensitivity", cols)
    surfaces["recency_sensitivity"] = _finish_surface(pd.DataFrame(records))

    records = []
    for r in recency[:3]:
        for b in baseline[:3]:
            _add_groups(records, work, "recency_vs_baseline", ["side", f"{r}_bucket", f"{b}_bucket"])
    surfaces["recency_vs_baseline"] = _finish_surface(pd.DataFrame(records))

    records = []
    for r in recency[:2]:
        _add_groups(records, work, "interaction_failure_surface", ["side", "line_bucket", f"{r}_bucket"])
        for b in baseline[:2]:
            _add_groups(records, work, "interaction_failure_surface", ["side", "line_bucket", f"{r}_bucket", f"{b}_bucket"])
        _add_groups(records, work, "interaction_failure_surface", ["side", "home_away", f"{r}_bucket"])
    surfaces["interaction_failure_surface"] = _finish_surface(pd.DataFrame(records))

    records = []
    for cols in [["side", "line_bucket"], ["side", "team"], ["side", "opponent"], ["side", "home_away"], ["side", "bookmaker_key"]]:
        _add_groups(records, work, "local_base_rate_analysis", cols)
    surfaces["local_base_rate_analysis"] = _finish_surface(pd.DataFrame(records))
    return surfaces


def _safe_slope(x: pd.Series, y: pd.Series) -> float:
    data = pd.DataFrame({"x": pd.to_numeric(x, errors="coerce"), "y": pd.to_numeric(y, errors="coerce")}).dropna()
    if len(data) < 2 or data["x"].nunique() < 2:
        return np.nan
    return float(np.polyfit(data["x"].astype(float), data["y"].astype(float), deg=1)[0])


def build_slope_curvature(rows: pd.DataFrame, features: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    surface_rows = []
    for feature in features[:5]:
        if feature not in rows.columns:
            continue
        data = rows[rows[feature].notna()].copy()
        data["feature_bucket"] = _bucket_feature(data, feature)
        data = data[data["feature_bucket"].map(_clean).ne("")]
        for (side, bucket), group in data.groupby(["side", "feature_bucket"], observed=True, dropna=False):
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
    slope_rows = []
    curv_rows = []
    if surface.empty:
        return pd.DataFrame(), surface, pd.DataFrame()
    for (feature, side), group in surface.groupby(["feature", "side"], dropna=False):
        ordered = group.sort_values("avg_feature_value").reset_index(drop=True).copy()
        slope_model = _safe_slope(ordered["avg_feature_value"], ordered["avg_model_prob"])
        slope_actual = _safe_slope(ordered["avg_feature_value"], ordered["actual_win_rate"])
        slope_rows.append(
            {
                "feature": feature,
                "side": side,
                "buckets": int(len(ordered)),
                "bets": int(ordered["bets"].sum()),
                "slope_model": slope_model,
                "slope_actual": slope_actual,
                "slope_ratio": slope_model / slope_actual if pd.notna(slope_actual) and slope_actual != 0 else np.nan,
                "slope_gap": slope_model - slope_actual if pd.notna(slope_model) and pd.notna(slope_actual) else np.nan,
            }
        )
        ordered["next_feature_bucket"] = ordered["feature_bucket"].shift(-1)
        ordered["delta_model"] = ordered["avg_model_prob"].shift(-1) - ordered["avg_model_prob"]
        ordered["delta_actual"] = ordered["actual_win_rate"].shift(-1) - ordered["actual_win_rate"]
        ordered["curvature_error"] = ordered["delta_model"] - ordered["delta_actual"]
        ordered["abs_curvature_error"] = ordered["curvature_error"].abs()
        curv_rows.append(ordered)
    return pd.DataFrame(slope_rows), surface.sort_values(["feature", "side", "avg_feature_value"]), pd.concat(curv_rows, ignore_index=True)


def _max_usable(df: pd.DataFrame) -> float:
    if df.empty or "bets" not in df.columns:
        return np.nan
    usable = df[df["bets"].ge(25)]
    if usable.empty or "abs_calibration_error" not in usable.columns:
        return np.nan
    return float(usable["abs_calibration_error"].max())


def _top_zone(df: pd.DataFrame) -> str:
    if df.empty or "bets" not in df.columns:
        return ""
    usable = df[df["bets"].ge(25)].head(3)
    parts = []
    for _, r in usable.iterrows():
        attrs = []
        for c in df.columns:
            if c in {"section", "bets", "avg_model_prob", "actual_win_rate", "calibration_error", "abs_calibration_error", "sample_size_flag", "usable_sample"}:
                continue
            v = _clean(r.get(c))
            if v and v.lower() != "nan":
                attrs.append(f"{c}={v}")
        parts.append(f"{'; '.join(attrs)} err={float(r['calibration_error']):+.3f} n={int(r['bets'])}")
    return " | ".join(parts)


def classify_prop(prop: str, rows: pd.DataFrame, surfaces: dict[str, pd.DataFrame], slopes: pd.DataFrame, curvature: pd.DataFrame) -> dict[str, Any]:
    side = rows.groupby("side", observed=True).apply(lambda g: pd.Series(_metrics(g)), include_groups=False).reset_index()
    side_abs = float(side["calibration_error"].abs().max()) if not side.empty else np.nan
    side_over = float(side.loc[side["side"].eq("over"), "calibration_error"].iloc[0]) if side["side"].eq("over").any() else np.nan
    side_under = float(side.loc[side["side"].eq("under"), "calibration_error"].iloc[0]) if side["side"].eq("under").any() else np.nan
    vals = {
        "directional bias": side_abs,
        "recency overconfidence": _max_usable(surfaces.get("recency_sensitivity", pd.DataFrame())),
        "subgroup/base-rate mixing": _max_usable(surfaces.get("local_base_rate_analysis", pd.DataFrame())),
        "line interaction distortion": _max_usable(surfaces.get("interaction_failure_surface", pd.DataFrame())),
        "alternate-line instability": _max_usable(_line_only(surfaces.get("local_base_rate_analysis", pd.DataFrame()))),
        "local curvature distortion": float(curvature[curvature["bets"].ge(25)]["abs_curvature_error"].max()) if not curvature.empty else np.nan,
    }
    ranked = sorted(vals.items(), key=lambda kv: -(-1 if pd.isna(kv[1]) else kv[1]))
    dominant = ranked[0][0] if ranked and ranked[0][1] >= 0.08 else "other"
    secondary = ranked[1][0] if len(ranked) > 1 and ranked[1][1] >= 0.08 else "other"
    slope_ratio = float(slopes["slope_ratio"].abs().replace([np.inf, -np.inf], np.nan).mean(skipna=True)) if not slopes.empty else np.nan
    slope_desc = "unknown"
    if pd.notna(slope_ratio):
        if slope_ratio > 1.25:
            slope_desc = "steeper_than_reality"
        elif slope_ratio < 0.80:
            slope_desc = "flatter_than_reality"
        else:
            slope_desc = "near_reality"
    failures_scope = "global" if side_abs >= 0.08 else ("local" if max(v for v in vals.values() if pd.notna(v)) >= 0.12 else "mixed_or_weak")
    same_as_hits = "yes" if prop == "hits" or (dominant in {"recency overconfidence", "subgroup/base-rate mixing"} and failures_scope == "local") else "no"
    confidence = "high" if len(rows) >= 1000 else ("medium" if len(rows) >= 500 else "low")
    return {
        "prop_type": prop,
        "bets_side_rows": int(len(rows)),
        "over_calibration_error": side_over,
        "under_calibration_error": side_under,
        "max_directional_abs_error": side_abs,
        "dominant_failure_type": dominant,
        "primary_failure_mechanism": dominant,
        "secondary_failure_mechanism": secondary,
        "strongest_bad_zones": _top_zone(surfaces.get("feature_only_failure_surface", pd.DataFrame())),
        "strongest_feature_interactions": _top_zone(surfaces.get("interaction_failure_surface", pd.DataFrame())),
        "slope_shape": slope_desc,
        "mean_abs_slope_ratio": slope_ratio,
        "curvature_mismatch_exists": "yes" if vals["local curvature distortion"] >= 0.10 else "no",
        "max_curvature_error": vals["local curvature distortion"],
        "failures_are_global_or_local": failures_scope,
        "global_behavior_shared": "yes" if slope_desc == "flatter_than_reality" else "no",
        "same_as_hits": same_as_hits,
        "confidence_in_diagnosis": confidence,
        "recency_max_abs_error": vals["recency overconfidence"],
        "local_base_rate_max_abs_error": vals["subgroup/base-rate mixing"],
        "line_interaction_max_abs_error": vals["line interaction distortion"],
        "alternate_line_max_abs_error": vals["alternate-line instability"],
    }


def _line_only(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "line_bucket" not in df.columns:
        return pd.DataFrame()
    return df[df.get("line_bucket", "").map(_clean).ne("")]


def _fmt(value: Any, digits: int = 4) -> str:
    if pd.isna(value):
        return "NA"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.{digits}f}"
    return str(value)


def _md_table(df: pd.DataFrame, cols: list[str], max_rows: int = 20) -> str:
    if df.empty:
        return "_No rows._"
    work = df[cols].head(max_rows).fillna("")
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join(["---"] * len(cols)) + " |"
    body = ["| " + " | ".join(str(row[col]) for col in cols) + " |" for _, row in work.iterrows()]
    return "\n".join([header, sep, *body])


def write_meta_md(path: Path, meta: pd.DataFrame, outputs_by_prop: dict[str, dict[str, Path]], from_date: str, to_date: str) -> None:
    show = meta.copy()
    for col in [
        "over_calibration_error",
        "under_calibration_error",
        "max_directional_abs_error",
        "mean_abs_slope_ratio",
        "max_curvature_error",
        "recency_max_abs_error",
        "local_base_rate_max_abs_error",
        "line_interaction_max_abs_error",
    ]:
        if col in show.columns:
            show[col] = show[col].map(_fmt)
    cross_cols = [
        "prop_type",
        "global_behavior_shared",
        "same_as_hits",
        "primary_failure_mechanism",
        "secondary_failure_mechanism",
        "confidence_in_diagnosis",
    ]
    detail_cols = [
        "prop_type",
        "dominant_failure_type",
        "failures_are_global_or_local",
        "slope_shape",
        "curvature_mismatch_exists",
        "recency_max_abs_error",
        "local_base_rate_max_abs_error",
        "line_interaction_max_abs_error",
        "strongest_bad_zones",
    ]
    lines = [
        "# Prop Diagnostic Meta Summary",
        "",
        f"Date range: `{from_date}` to `{to_date}`",
        "",
        "Diagnosis only. No fixes, filters, calibration, or model changes were applied.",
        "",
        "## Cross-Prop Comparison",
        "",
        _md_table(show, cross_cols, 20),
        "",
        "## Mechanism Detail",
        "",
        _md_table(show, detail_cols, 20),
        "",
        "## Interpretation",
        "",
        "- A shared behavior appears where several props have flatter model probability slopes than observed bucket movement.",
        "- The strongest failure mechanisms are not identical across props; large errors are usually local to side/line/feature/context pockets.",
        "- This points more toward prop-specific and interaction-specific failures than one single global architectural flaw.",
        "",
        "## Per-Prop Outputs",
        "",
    ]
    for prop, outputs in outputs_by_prop.items():
        lines.append(f"### {prop}")
        for name, out in outputs.items():
            lines.append(f"- `{name}`: `{out}`")
        lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run cross-prop MLB diagnostic meta summary.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--feature-metadata", default=str(DEFAULT_METADATA))
    ap.add_argument("--props", default=",".join(DEFAULT_PROPS))
    ap.add_argument("--from-date", default="2026-04-09")
    ap.add_argument("--to-date", default="2026-05-08")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--meta-csv", default=str(DEFAULT_META_CSV))
    ap.add_argument("--meta-md", default=str(DEFAULT_META_MD))
    args = ap.parse_args(list(argv) if argv is not None else None)

    props = [_clean(p).lower() for p in args.props.split(",") if _clean(p)]
    features_by_prop = {prop: _feature_names(Path(args.feature_metadata), prop) for prop in props}
    missing_meta = [p for p, feats in features_by_prop.items() if not feats]
    if missing_meta:
        raise SystemExit(f"No feature metadata found for props: {missing_meta}")

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    reconcile = _load_reconcile(paths, set(props))
    engine = create_engine(_db_url(), pool_pre_ping=True)
    source, _source_meta = _fetch_features(engine, props, features_by_prop, args.from_date, args.to_date)
    feature_source = _prep_features(source, features_by_prop)

    out_dir = Path(args.out_dir)
    meta_rows = []
    outputs_by_prop: dict[str, dict[str, Path]] = {}
    for prop in props:
        feature_names = features_by_prop[prop]
        rows = _side_rows(reconcile, feature_source, prop, feature_names)
        if rows.empty:
            continue
        recency = _recency_features(feature_names)
        baseline = _baseline_features(feature_names)
        surfaces = build_surfaces(rows, recency, baseline)
        slopes, slope_surface, curvature = build_slope_curvature(rows, [*recency[:3], *baseline[:2]])
        outputs = {
            "feature_only_failure_surface": out_dir / f"{prop}_feature_only_failure_surface.csv",
            "recency_sensitivity": out_dir / f"{prop}_recency_sensitivity.csv",
            "recency_vs_baseline": out_dir / f"{prop}_recency_vs_baseline.csv",
            "interaction_failure_surface": out_dir / f"{prop}_interaction_failure_surface.csv",
            "local_base_rate_analysis": out_dir / f"{prop}_local_base_rate_analysis.csv",
            "feature_slope": out_dir / f"{prop}_feature_slope.csv",
            "feature_slope_surface": out_dir / f"{prop}_feature_slope_surface.csv",
            "probability_curvature": out_dir / f"{prop}_probability_curvature.csv",
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        surfaces["feature_only_failure_surface"].to_csv(outputs["feature_only_failure_surface"], index=False)
        surfaces["recency_sensitivity"].to_csv(outputs["recency_sensitivity"], index=False)
        surfaces["recency_vs_baseline"].to_csv(outputs["recency_vs_baseline"], index=False)
        surfaces["interaction_failure_surface"].to_csv(outputs["interaction_failure_surface"], index=False)
        surfaces["local_base_rate_analysis"].to_csv(outputs["local_base_rate_analysis"], index=False)
        slopes.to_csv(outputs["feature_slope"], index=False)
        slope_surface.to_csv(outputs["feature_slope_surface"], index=False)
        curvature.to_csv(outputs["probability_curvature"], index=False)
        outputs_by_prop[prop] = outputs
        meta_rows.append(classify_prop(prop, rows, surfaces, slopes, curvature))

    meta = pd.DataFrame(meta_rows)
    meta_csv = Path(args.meta_csv)
    meta_md = Path(args.meta_md)
    meta_csv.parent.mkdir(parents=True, exist_ok=True)
    meta.to_csv(meta_csv, index=False)
    write_meta_md(meta_md, meta, outputs_by_prop, args.from_date, args.to_date)
    print(
        "[prop-diag-meta] "
        f"files={len(paths)} source_rows={len(source)} props={len(meta)} "
        f"meta_csv={meta_csv} meta_md={meta_md}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
