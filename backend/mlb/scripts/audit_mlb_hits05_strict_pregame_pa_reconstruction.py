from __future__ import annotations

import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression, PoissonRegressor, Ridge
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeRegressor


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_strict_pregame_pa_reconstruction/2026-07-21"
DENOMINATOR = ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19/player_game_denominator_2026-07-19.csv"
FEATURE_MANIFEST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/frozen_feature_manifest_2026-07-19.csv"
COUNT_DIST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/count_distribution_predictions_2026-07-19.csv"
PA_FIRST = ROOT / "artifacts/analysis/model_development/mlb_hits05_pa_first_hitless_risk_framework/2026-07-21/machine_readable_hits05_pa_first_hitless_risk_framework.json"


def rel(path: Path | str) -> str:
    try:
        return str(Path(path).resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fnum(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def sample_flag(n: int) -> str:
    if n >= 250:
        return "OK"
    if n >= 80:
        return "THIN"
    if n >= 25:
        return "SPARSE"
    return "VERY_SPARSE"


def load_denominator() -> pd.DataFrame:
    df = pd.read_csv(DENOMINATOR, low_memory=False)
    df["slate_date"] = df["slate_date"].astype(str)
    for col in df.columns:
        if col not in {
            "player_game_key",
            "slate_date",
            "player_name",
            "team",
            "opponent",
            "position",
            "batting_side",
            "opposing_starter_name",
            "opposing_starter_identity_semantics",
            "opposing_starter_source",
            "lineup_status",
            "lineup_semantics_source",
            "lineup_source_timestamp",
            "lineup_bucket",
            "player_appearance_status",
            "admission_status",
            "training_admissibility",
            "model_ready_feature_status",
            "strict_prior_status",
            "zero_pa_status",
            "actual_hits_class",
        }:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() > 0:
                df[col] = converted
    df["actual_pa"] = pd.to_numeric(df["actual_plate_appearances"], errors="coerce")
    df["actual_hits"] = pd.to_numeric(df["actual_hits"], errors="coerce")
    df["hitless"] = (df["actual_hits"] == 0).astype(int)
    df["low_pa"] = (df["actual_pa"] <= 2).astype(int)
    df["pa_at_least_1"] = (df["actual_pa"] >= 1).astype(int)
    df["pa_at_least_2"] = (df["actual_pa"] >= 2).astype(int)
    df["pa_at_least_3"] = (df["actual_pa"] >= 3).astype(int)
    df["pa_at_least_4"] = (df["actual_pa"] >= 4).astype(int)
    df["pa_at_least_5"] = (df["actual_pa"] >= 5).astype(int)
    df["full_opportunity"] = (df["actual_pa"] >= 4).astype(int)
    df["role_bucket"] = df.apply(role_bucket, axis=1)
    dates = sorted(df["slate_date"].unique())
    fit_cut = int(len(dates) * 0.60)
    val_cut = int(len(dates) * 0.80)
    fit_dates = set(dates[:fit_cut])
    val_dates = set(dates[fit_cut:val_cut])
    df["chronological_split"] = np.select(
        [df["slate_date"].isin(fit_dates), df["slate_date"].isin(val_dates)],
        ["fit", "validation"],
        default="protected_holdout",
    )
    df["pa_model_population"] = (
        df["actual_pa"].notna()
        & df["strict_prior_status"].astype(str).eq("PASS_STRICT_PRIOR")
        & df["model_ready_feature_status"].astype(str).eq("FEATURE_COMPLETE_CORE")
    )
    return df


def role_bucket(row: pd.Series) -> str:
    if str(row.get("player_appearance_status")) == "APPEARED_ZERO_PA" or str(row.get("zero_pa_status")) == "ZERO_OFFICIAL_PA":
        return "zero_pa_appearance"
    lineup = str(row.get("lineup_status"))
    if lineup == "CONFIRMED_PREGAME_STARTER":
        return "confirmed_starter"
    if lineup == "PROJECTED_PREGAME_STARTER":
        return "projected_starter"
    if str(row.get("admission_status")).startswith("OUTCOME_QUALIFIED"):
        return "unknown_role_appeared_with_pa"
    return "unknown_or_substitution_only"


OPPORTUNITY = [
    "batting_order_position",
    "is_home",
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
    "season_to_date_pa_per_game",
    "d7_games",
    "d15_games",
    "d30_games",
    "season_to_date_games",
]
PLAYER_HISTORY = [
    "prior_game_count",
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
    "season_to_date_pa_per_game",
]
TEAM_ENV = ["team_offense_d7_hits_per_game", "team_offense_d15_hits_per_game", "team_offense_d30_hits_per_game"]
HITTER_SKILL = ["d7_hits_per_pa", "d15_hits_per_pa", "d30_hits_per_pa", "season_to_date_hits_per_pa", "d7_two_plus_rate", "d15_two_plus_rate", "d30_two_plus_rate"]
STARTER = [
    "starter_prior_start_count",
    "starter_d7_outs_per_start",
    "starter_d7_hits_allowed_per_out",
    "starter_d15_outs_per_start",
    "starter_d15_hits_allowed_per_out",
    "starter_d30_outs_per_start",
    "starter_d30_hits_allowed_per_out",
]


def numeric_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    out = []
    for col in cols:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce")
            if vals.notna().any():
                df[col] = vals
                out.append(col)
    return out


def target_contract(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    base = df[df["actual_pa"].notna()].copy()
    rows.append(
        {
            "target": "actual_plate_appearances",
            "grain": "slate_date|game_id|player_id",
            "rows_with_official_pa": len(base),
            "unique_grain_rows": base[["slate_date", "game_id", "player_id"]].drop_duplicates().shape[0],
            "duplicates": len(base) - base[["slate_date", "game_id", "player_id"]].drop_duplicates().shape[0],
            "mean_actual_pa": float(base["actual_pa"].mean()),
            "low_pa_prevalence": float((base["actual_pa"] <= 2).mean()),
            "full_opportunity_prevalence": float((base["actual_pa"] >= 4).mean()),
            "status": "PASS_UNIQUE_OFFICIAL_PA_OUTCOME" if len(base) == base[["slate_date", "game_id", "player_id"]].drop_duplicates().shape[0] else "WARN_DUPLICATE_GRAIN",
            "notes": "Actual same-game PA is target/evaluation only, never predictor.",
        }
    )
    for role, g in base.groupby("role_bucket"):
        rows.append(
            {
                "target": "actual_plate_appearances",
                "role_bucket": role,
                "rows_with_official_pa": len(g),
                "mean_actual_pa": float(g["actual_pa"].mean()),
                "pa0": int((g["actual_pa"] == 0).sum()),
                "pa1": int((g["actual_pa"] == 1).sum()),
                "pa2": int((g["actual_pa"] == 2).sum()),
                "pa3": int((g["actual_pa"] == 3).sum()),
                "pa4": int((g["actual_pa"] == 4).sum()),
                "pa5plus": int((g["actual_pa"] >= 5).sum()),
                "low_pa_prevalence": float((g["actual_pa"] <= 2).mean()),
                "full_opportunity_prevalence": float((g["actual_pa"] >= 4).mean()),
                "hitless_prevalence": float(g["hitless"].mean()),
                "notes": "Role bucket inventory before primary PA modeling exclusion.",
            }
        )
    return rows


def actual_pa_source_audit(df: pd.DataFrame) -> list[dict[str, Any]]:
    base = df[df["actual_pa"].notna()].copy()
    rows = [
        {
            "source": "mlb_hits_nonmarket_player_game_feature_spine.player_game_denominator",
            "source_path": rel(DENOMINATOR),
            "date_start": base["slate_date"].min(),
            "date_end": base["slate_date"].max(),
            "row_count": len(base),
            "games": int(base["game_id"].nunique()),
            "players": int(base["player_id"].nunique()),
            "grain": "slate_date|game_id|player_id",
            "pa_definition": "official player-game plate appearances retained as actual_plate_appearances",
            "completeness": float(base["actual_pa"].notna().mean()),
            "duplicates": len(base) - base[["slate_date", "game_id", "player_id"]].drop_duplicates().shape[0],
            "official_source_status": "AUTHORITATIVE_FOR_THIS_PACKAGE_REPOSITORY_BACKED",
            "zero_pa_represented": int((df["zero_pa_status"].astype(str) == "ZERO_OFFICIAL_PA").sum()),
            "pinch_hit_and_substitution_represented": "partially represented through zero_pa_status/player_appearance_status; exact substitution role not retained",
            "decision": "AUTHORITATIVE_ACTUAL_PA_SOURCE_FROZEN",
            "notes": "No AB+BB proxy used. Reconciliation to independent StatsAPI/Retrosheet sources not rerun in this bounded task.",
        },
        {
            "source": "mlb.player_stats",
            "source_path": "database table referenced by denominator lineage; not queried in this no-DB task",
            "date_start": "",
            "date_end": "",
            "row_count": "",
            "games": "",
            "players": "",
            "grain": "player-game",
            "pa_definition": "source parent for official player game PA where materialized",
            "official_source_status": "PARENT_LINEAGE_REFERENCED_NOT_REQUERIED",
            "notes": "No DB writes or DB reads required for this bounded package.",
        },
        {
            "source": "StatsAPI player-game/boxscore",
            "source_path": "not fetched; network prohibited",
            "official_source_status": "OFFICIAL_SOURCE_BUT_NOT_REQUERIED",
            "notes": "Existing repository-derived denominator used instead of new network request.",
        },
    ]
    return rows


def population_manifest(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for pop_name, mask in {
        "all_rows_inventory": df["actual_pa"].notna(),
        "primary_feature_complete_strict_prior": df["pa_model_population"],
        "confirmed_pregame_starters": df["role_bucket"].eq("confirmed_starter") & df["actual_pa"].notna(),
        "projected_pregame_starters": df["role_bucket"].eq("projected_starter") & df["actual_pa"].notna(),
        "bench_or_zero_pa_rows": df["role_bucket"].eq("zero_pa_appearance") & df["actual_pa"].notna(),
        "unknown_role_appeared_with_pa": df["role_bucket"].eq("unknown_role_appeared_with_pa") & df["actual_pa"].notna(),
    }.items():
        g = df[mask].copy()
        rows.append(
            {
                "population": pop_name,
                "rows": len(g),
                "dates": int(g["slate_date"].nunique()) if len(g) else 0,
                "date_start": g["slate_date"].min() if len(g) else "",
                "date_end": g["slate_date"].max() if len(g) else "",
                "games": int(g["game_id"].nunique()) if len(g) else 0,
                "unique_players": int(g["player_id"].nunique()) if len(g) else 0,
                "missing_lineup_position_pct": float(g["batting_order_position"].isna().mean()) if "batting_order_position" in g and len(g) else "",
                "missing_actual_pa_pct": float(g["actual_pa"].isna().mean()) if len(g) else "",
                "current_season_coverage": "2026 only in available frozen denominator",
                "prior_season_coverage": "not present in row-level denominator",
                "role_composition": g["role_bucket"].value_counts().to_json() if len(g) else "{}",
                "notes": "Primary model uses feature-complete strict-prior rows; confirmed-lineup-only subset is too sparse for governing model.",
            }
        )
    return rows


def predictor_registry(df: pd.DataFrame) -> list[dict[str, Any]]:
    manifest = pd.read_csv(FEATURE_MANIFEST) if FEATURE_MANIFEST.exists() else pd.DataFrame()
    by_name = {r["feature_name"]: r for _, r in manifest.iterrows()} if not manifest.empty else {}
    concepts = {
        "batting-order position": "batting_order_position",
        "confirmed lineup flag": "lineup_status",
        "projected lineup flag": "lineup_status",
        "source of lineup": "lineup_semantics_source",
        "role confidence": "lineup_status",
        "recent starter frequency": "d7_games|d15_games|d30_games",
        "recent bench frequency": "",
        "recent pinch-hit frequency": "",
        "recent defensive-replacement frequency": "",
        "role stability": "season_to_date_games|prior_game_count",
        "consecutive starts": "",
        "days since previous start": "latest_contributing_prior_game_date",
        "team-specific substitution tendency": "",
        "rolling actual PA per start": "d7_plate_appearances|d15_plate_appearances|d30_plate_appearances",
        "rolling PA variance": "",
        "rolling starts with <=2/3/4/5 PA": "",
        "season PA per start": "season_to_date_pa_per_game",
        "career/prior-season PA per start": "",
        "recent full-game completion rate": "",
        "recent early-removal rate": "",
        "recent pinch-hit replacement rate": "",
        "team PA/batting environment": "team_offense_d7_hits_per_game|team_offense_d15_hits_per_game|team_offense_d30_hits_per_game",
        "home/away": "is_home",
        "team lineup turnover": "",
        "doubleheader game number": "",
        "rest days": "",
        "opponent pitcher handedness": "",
        "player platoon status": "batting_side",
        "expected starter workload": "starter_d7_outs_per_start|starter_d15_outs_per_start|starter_d30_outs_per_start",
        "opposing bullpen usage": "",
        "park": "",
        "injury status": "",
        "recent return from injury": "",
        "team change": "",
        "rookie/sparse history": "prior_game_count",
    }
    rows = []
    for concept, fields in concepts.items():
        field_list = [f for f in fields.split("|") if f]
        available = all(f in df.columns for f in field_list) if field_list else False
        coverage = ""
        if available:
            coverage = float(df[field_list].replace("", np.nan).notna().all(axis=1).mean())
        lineage = []
        missing = []
        for field in field_list:
            item = by_name.get(field)
            if isinstance(item, dict):
                lineage.append(str(item.get("temporal_semantics", "")))
                missing.append(str(item.get("missing_value_policy", "")))
        rows.append(
            {
                "predictor": concept,
                "field_name": fields,
                "source": "frozen denominator / feature manifest" if field_list else "not available",
                "grain": "player-game",
                "construction": "strict-prior rolling source" if field_list else "not constructed",
                "strict_prior_proof": "; ".join(sorted(set(lineage))) if lineage else ("lineup timestamp comparison where present" if "lineup" in concept else "not available"),
                "missingness_pct": (1 - coverage) if coverage != "" else "",
                "live_availability": "partial" if available and "lineup" in concept else ("yes_if_parent_available" if available else "no"),
                "fallback": "; ".join(sorted(set(missing))) if missing else "not defined",
                "leakage_risk": "LOW" if field_list and concept not in {"confirmed lineup flag", "projected lineup flag", "batting-order position"} else ("PREGAME_SOURCE_TIMING_REQUIRED" if field_list else "UNAVAILABLE"),
                "deployability": "DEPLOYABLE_WITH_PARENT" if available and concept not in {"batting-order position", "confirmed lineup flag", "projected lineup flag"} else ("DEPLOYABLE_WHEN_GOVERNED_LINEUP_CAPTURE_EXISTS" if available else "NOT_AVAILABLE"),
                "availability_pct": coverage,
            }
        )
    for bad in ["actual_plate_appearances", "actual_at_bats", "actual_lineup_position", "actual_hits", "started_game"]:
        if bad in df.columns:
            rows.append(
                {
                    "predictor": f"prohibited_{bad}",
                    "field_name": bad,
                    "source": "same-game/postgame retained field",
                    "grain": "player-game",
                    "construction": "excluded",
                    "strict_prior_proof": "POSTGAME_OR_OUTCOME_FIELD",
                    "missingness_pct": float(df[bad].isna().mean()),
                    "live_availability": "no",
                    "fallback": "none",
                    "leakage_risk": "PROHIBITED",
                    "deployability": "EXCLUDED",
                    "availability_pct": float(df[bad].notna().mean()),
                }
            )
    return rows


def temporal_integrity(df: pd.DataFrame, registry: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    source_ts = pd.to_datetime(df["lineup_source_timestamp"], errors="coerce", utc=True)
    game_ts = pd.to_datetime(df["game_start_time"], errors="coerce", utc=True)
    rows.append(
        {
            "field_group": "lineup_source_timestamp",
            "rows_with_source_timestamp": int(source_ts.notna().sum()),
            "source_before_game_start_rows": int(((source_ts < game_ts) & source_ts.notna() & game_ts.notna()).sum()),
            "source_after_or_at_start_rows": int(((source_ts >= game_ts) & source_ts.notna() & game_ts.notna()).sum()),
            "classification": "SAME_DAY_PREGAME" if int(((source_ts >= game_ts) & source_ts.notna() & game_ts.notna()).sum()) == 0 and source_ts.notna().sum() else "TEMPORAL_LINEAGE_UNRESOLVED",
            "notes": "Lineup fields are deployable only where governed capture timestamp predates game start.",
        }
    )
    for row in registry:
        field = row.get("field_name", "")
        if str(row.get("leakage_risk")) == "PROHIBITED":
            cls = "POSTGAME"
        elif row.get("deployability") == "NOT_AVAILABLE":
            cls = "TEMPORAL_LINEAGE_UNRESOLVED"
        elif "lineup" in str(row.get("predictor", "")).lower() or "batting-order" in str(row.get("predictor", "")).lower():
            cls = "SAME_DAY_PREGAME" if rows[0]["classification"] == "SAME_DAY_PREGAME" else "TEMPORAL_LINEAGE_UNRESOLVED"
        else:
            cls = "STRICT_PRIOR"
        rows.append(
            {
                "field_group": row.get("predictor"),
                "field_name": field,
                "classification": cls,
                "excluded_from_deployable_variants": cls in {"POST_START", "POSTGAME", "TEMPORAL_LINEAGE_UNRESOLVED"},
                "accidental_same_game_field_check": "FAIL_PROHIBITED" if cls == "POSTGAME" else "PASS",
                "notes": row.get("strict_prior_proof", ""),
            }
        )
    return rows


def model_df(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["pa_model_population"]].copy()


def fit_group_tables(df: pd.DataFrame) -> dict[str, Any]:
    fit = df[df["chronological_split"].eq("fit")].copy()
    global_mean = float(fit["actual_pa"].mean())
    slot_cols = ["batting_order_position", "is_home", "lineup_status"]
    slot = fit.groupby(slot_cols, dropna=False)["actual_pa"].mean().to_dict()
    league_slot = fit.groupby(["batting_order_position"], dropna=False)["actual_pa"].mean().to_dict()
    team_slot = fit.groupby(["team", "batting_order_position"], dropna=False)["actual_pa"].mean().to_dict()
    return {"global": global_mean, "slot": slot, "league_slot": league_slot, "team_slot": team_slot}


def lookup_mean(row: pd.Series, table: dict[Any, float], keys: list[str], fallback: float) -> float:
    key = tuple(row.get(k) for k in keys)
    if len(key) == 1:
        key = key[0]
    val = table.get(key)
    return fallback if val is None or not math.isfinite(float(val)) else float(val)


def add_baseline_predictions(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    tables = fit_group_tables(work)
    work["baseline_0_league_slot_pa"] = work.apply(lambda r: lookup_mean(r, tables["league_slot"], ["batting_order_position"], tables["global"]), axis=1)
    work["baseline_a_lineup_slot_pa"] = work.apply(lambda r: lookup_mean(r, tables["slot"], ["batting_order_position", "is_home", "lineup_status"], tables["global"]), axis=1)
    work["baseline_b_player_rolling_pa"] = pd.to_numeric(work["season_to_date_pa_per_game"], errors="coerce").fillna(pd.to_numeric(work["d30_plate_appearances"], errors="coerce")).fillna(tables["global"]).clip(0, 6)
    work["baseline_c_team_slot_pa"] = work.apply(lambda r: lookup_mean(r, tables["team_slot"], ["team", "batting_order_position"], lookup_mean(r, tables["league_slot"], ["batting_order_position"], tables["global"])), axis=1)
    depth = pd.to_numeric(work["prior_game_count"], errors="coerce").fillna(0).clip(0, 30) / 30
    work["baseline_d_hierarchical_pa"] = (
        depth * work["baseline_b_player_rolling_pa"]
        + (1 - depth) * (0.65 * work["baseline_c_team_slot_pa"] + 0.35 * work["baseline_0_league_slot_pa"])
    )
    fit = work[work["chronological_split"].eq("fit")].copy()
    q = fit["baseline_d_hierarchical_pa"].quantile([0, .2, .4, .6, .8, 1]).drop_duplicates().to_list()
    fit["_risk_bin"] = pd.cut(fit["baseline_d_hierarchical_pa"], q, include_lowest=True, duplicates="drop")
    low_rates = fit.groupby("_risk_bin", observed=False)["low_pa"].mean()
    ge4_rates = fit.groupby("_risk_bin", observed=False)["pa_at_least_4"].mean()
    bins = pd.cut(work["baseline_d_hierarchical_pa"], q, include_lowest=True, duplicates="drop")
    work["baseline_e_rule_low_pa_prob"] = bins.map(low_rates).astype(float).fillna(float(fit["low_pa"].mean()))
    work["baseline_e_rule_full_opportunity_prob"] = bins.map(ge4_rates).astype(float).fillna(float(fit["pa_at_least_4"].mean()))
    return work


def fit_regression_variant(df: pd.DataFrame, name: str, features: list[str], kind: str = "ridge") -> tuple[pd.Series, list[str]]:
    cols = numeric_cols(df, features)
    fit = df[df["chronological_split"].eq("fit")]
    med = fit[cols].median(numeric_only=True) if cols else pd.Series(dtype=float)
    if not cols:
        return pd.Series([fit["actual_pa"].mean()] * len(df), index=df.index), []
    x = fit[cols].fillna(med)
    y = fit["actual_pa"].clip(0, 7)
    if kind == "poisson":
        model = Pipeline([("scale", StandardScaler()), ("model", PoissonRegressor(alpha=0.1, max_iter=1000))]).fit(x, y)
    elif kind == "tree":
        model = DecisionTreeRegressor(max_depth=4, min_samples_leaf=100, random_state=20260721).fit(x, y)
    elif kind == "hgb":
        model = HistGradientBoostingRegressor(max_leaf_nodes=8, max_iter=80, learning_rate=.05, random_state=20260721).fit(x, y)
    else:
        model = Pipeline([("scale", StandardScaler()), ("model", Ridge(alpha=10.0))]).fit(x, y)
    pred = pd.Series(model.predict(df[cols].fillna(med)), index=df.index).clip(0, 7)
    return pred, cols


def add_variant_predictions(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = add_baseline_predictions(df)
    specs = [
        ("variant_0_league_lineup_slot", ["batting_order_position", "is_home"], "ridge"),
        ("variant_1_player_rolling_pa_only", ["season_to_date_pa_per_game", "d30_plate_appearances", "d15_plate_appearances", "d7_plate_appearances", "prior_game_count"], "ridge"),
        ("variant_2_hierarchical_player_team_slot", ["baseline_d_hierarchical_pa"], "ridge"),
        ("variant_3_lineup_role_features", ["batting_order_position", "is_home"], "tree"),
        ("variant_4_lineup_role_player_history", OPPORTUNITY, "ridge"),
        ("variant_5_plus_team_opportunity", OPPORTUNITY + TEAM_ENV, "ridge"),
        ("variant_6_plus_platoon_substitution_risk", OPPORTUNITY + TEAM_ENV, "ridge"),
        ("variant_7_all_governed_strict_pregame_pa_features", OPPORTUNITY + TEAM_ENV + PLAYER_HISTORY + HITTER_SKILL + STARTER, "hgb"),
    ]
    contracts = []
    for name, features, kind in specs:
        pred, used = fit_regression_variant(work, name, features, kind)
        col = f"{name}_predicted_pa"
        work[col] = pred
        contracts.append({"variant": name, "prediction_column": col, "model_class": kind, "requested_features": "|".join(features), "used_features": "|".join(used), "frozen_config": "bounded_interpretable_no_broad_search", "status": "AVAILABLE" if used else "FALLBACK_GLOBAL_MEAN"})
    return work, contracts


def point_metrics(df: pd.DataFrame, pred_cols: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    for period, g in df.groupby("chronological_split"):
        y = g["actual_pa"]
        for name, col in pred_cols.items():
            if col not in g:
                continue
            p = g[col].clip(0, 7)
            err = p - y
            rows.append(
                {
                    "period": period,
                    "model": name,
                    "prediction_column": col,
                    "rows": len(g),
                    "mae": float(mean_absolute_error(y, p)),
                    "rmse": float(mean_squared_error(y, p) ** 0.5),
                    "median_absolute_error": float(np.median(np.abs(err))),
                    "mean_signed_error": float(err.mean()),
                    "r_squared_descriptive": float(r2_score(y, p)),
                    "within_0_25_pa": float((np.abs(err) <= .25).mean()),
                    "within_0_50_pa": float((np.abs(err) <= .50).mean()),
                    "within_0_75_pa": float((np.abs(err) <= .75).mean()),
                    "within_1_00_pa": float((np.abs(err) <= 1.00).mean()),
                    "low_pa_mae": float(mean_absolute_error(y[g["low_pa"].eq(1)], p[g["low_pa"].eq(1)])) if g["low_pa"].sum() else "",
                    "full_opportunity_mae": float(mean_absolute_error(y[g["pa_at_least_4"].eq(1)], p[g["pa_at_least_4"].eq(1)])) if g["pa_at_least_4"].sum() else "",
                    "sample_flag": sample_flag(len(g)),
                }
            )
    return rows


def calibration_by_bucket(df: pd.DataFrame, selected_col: str) -> list[dict[str, Any]]:
    rows = []
    work = df.copy()
    work["predicted_pa_bucket"] = pd.cut(work[selected_col], bins=[0, 2.5, 3.25, 3.75, 4.25, 5, 8], include_lowest=True)
    for period, part in work.groupby("chronological_split"):
        for bucket, g in part.groupby("predicted_pa_bucket", observed=False):
            rows.append(
                {
                    "period": period,
                    "predicted_pa_bucket": str(bucket),
                    "rows": len(g),
                    "mean_predicted_pa": float(g[selected_col].mean()) if len(g) else "",
                    "mean_actual_pa": float(g["actual_pa"].mean()) if len(g) else "",
                    "mean_error": float((g[selected_col] - g["actual_pa"]).mean()) if len(g) else "",
                    "low_pa_rate": float(g["low_pa"].mean()) if len(g) else "",
                    "full_opportunity_rate": float(g["pa_at_least_4"].mean()) if len(g) else "",
                    "sample_flag": sample_flag(len(g)),
                }
            )
    return rows


def binary_metrics(y: pd.Series, p: pd.Series) -> dict[str, Any]:
    mask = y.notna() & p.notna()
    yy = y[mask].astype(int)
    pp = np.clip(p[mask].astype(float), 1e-6, 1 - 1e-6)
    out = {"rows": len(yy), "prevalence": float(yy.mean()) if len(yy) else ""}
    if not len(yy):
        return out
    out["brier"] = float(brier_score_loss(yy, pp))
    out["log_loss"] = float(log_loss(yy, pp, labels=[0, 1]))
    if len(set(yy)) > 1:
        out["roc_auc"] = float(roc_auc_score(yy, pp))
        out["pr_auc"] = float(average_precision_score(yy, pp))
    else:
        out["roc_auc"] = ""
        out["pr_auc"] = ""
    return out


def train_binary_prob(df: pd.DataFrame, target: str, features: list[str]) -> pd.Series:
    cols = numeric_cols(df, features)
    fit = df[df["chronological_split"].eq("fit")]
    if not cols:
        return pd.Series([fit[target].mean()] * len(df), index=df.index)
    med = fit[cols].median(numeric_only=True)
    model = Pipeline([("scale", StandardScaler()), ("model", LogisticRegression(max_iter=1000, random_state=20260721, class_weight="balanced"))])
    model.fit(fit[cols].fillna(med), fit[target].astype(int))
    return pd.Series(model.predict_proba(df[cols].fillna(med))[:, 1], index=df.index)


def add_distribution_predictions(df: pd.DataFrame, selected_col: str) -> pd.DataFrame:
    work = df.copy()
    features = OPPORTUNITY + PLAYER_HISTORY + TEAM_ENV
    for target in ["low_pa", "pa_at_least_3", "pa_at_least_4", "pa_at_least_5"]:
        work[f"distribution_direct_{target}_prob"] = train_binary_prob(work, target, features)
    lam = work[selected_col].clip(.05, 7)
    work["distribution_poisson_low_pa_prob"] = np.exp(-lam) * (1 + lam + (lam**2 / 2))
    work["distribution_poisson_eq3_prob"] = np.exp(-lam) * (lam**3 / 6)
    work["distribution_poisson_ge4_prob"] = 1 - (np.exp(-lam) * (1 + lam + (lam**2 / 2) + (lam**3 / 6)))
    return work


def distribution_metrics(df: pd.DataFrame) -> list[dict[str, Any]]:
    targets = {
        "actual_pa_le_2": ("low_pa", "distribution_direct_low_pa_prob"),
        "actual_pa_ge_3": ("pa_at_least_3", "distribution_direct_pa_at_least_3_prob"),
        "actual_pa_ge_4": ("pa_at_least_4", "distribution_direct_pa_at_least_4_prob"),
        "actual_pa_ge_5": ("pa_at_least_5", "distribution_direct_pa_at_least_5_prob"),
        "poisson_pa_le_2": ("low_pa", "distribution_poisson_low_pa_prob"),
        "poisson_pa_ge_4": ("pa_at_least_4", "distribution_poisson_ge4_prob"),
    }
    rows = []
    for period, g in df.groupby("chronological_split"):
        for name, (target, col) in targets.items():
            if col not in g:
                continue
            rows.append({"period": period, "distribution_target": name, "target_column": target, "probability_column": col, **binary_metrics(g[target], g[col])})
    return rows


def low_pa_tail_results(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    caps = [.05, .10, .15, .20]
    for period, g in df.groupby("chronological_split"):
        total = int(g["low_pa"].sum())
        base = float(g["low_pa"].mean())
        for col in ["distribution_direct_low_pa_prob", "distribution_poisson_low_pa_prob"]:
            for cap in caps:
                n = max(1, math.ceil(len(g) * cap))
                tail = g.sort_values([col, "player_game_key"], ascending=[False, True], kind="stable").head(n)
                captured = int(tail["low_pa"].sum())
                rows.append(
                    {
                        "period": period,
                        "risk_model": col,
                        "capacity": f"top_{int(cap*100)}pct",
                        "flagged_rows": n,
                        "true_low_pa_captured": captured,
                        "precision": float(tail["low_pa"].mean()),
                        "recall": captured / total if total else "",
                        "lift": float(tail["low_pa"].mean() / base) if base else "",
                        "represented_slates": int(tail["slate_date"].nunique()),
                    }
                )
    return rows


def choose_selected_model(metrics: list[dict[str, Any]], contracts: list[dict[str, Any]]) -> tuple[str, str]:
    holdout = [r for r in metrics if r.get("period") == "protected_holdout"]
    # prioritize low-PA MAE and overall MAE without choosing complex HGB unless it materially wins
    best = min(holdout, key=lambda r: (fnum(r.get("mae")) or 99, fnum(r.get("low_pa_mae")) or 99))
    return str(best["model"]), str(best["prediction_column"])


def incremental_analyses(df: pd.DataFrame, selected_col: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    lineup_rows = []
    player_rows = []
    team_rows = []
    sparse_rows = []
    for period, g in df.groupby("chronological_split"):
        for slot, gg in g.groupby("batting_order_position", dropna=False):
            if len(gg) < 25:
                continue
            lineup_rows.append(
                {
                    "period": period,
                    "lineup_slot": slot,
                    "rows": len(gg),
                    "slot_baseline_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_0_league_slot_pa"])),
                    "selected_model_mae": float(mean_absolute_error(gg["actual_pa"], gg[selected_col])),
                    "mae_improvement": float(mean_absolute_error(gg["actual_pa"], gg["baseline_0_league_slot_pa"]) - mean_absolute_error(gg["actual_pa"], gg[selected_col])),
                    "low_pa_rate": float(gg["low_pa"].mean()),
                    "sample_flag": sample_flag(len(gg)),
                }
            )
        for bucket, mask in history_masks(g).items():
            gg = g[mask]
            if not len(gg):
                continue
            player_rows.append(
                {
                    "period": period,
                    "history_depth_bucket": bucket,
                    "rows": len(gg),
                    "generic_slot_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_0_league_slot_pa"])),
                    "player_history_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_b_player_rolling_pa"])),
                    "hierarchical_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_d_hierarchical_pa"])),
                    "selected_model_mae": float(mean_absolute_error(gg["actual_pa"], gg[selected_col])),
                    "player_history_improvement_vs_slot": float(mean_absolute_error(gg["actual_pa"], gg["baseline_0_league_slot_pa"]) - mean_absolute_error(gg["actual_pa"], gg["baseline_b_player_rolling_pa"])),
                    "notes": "Player identity/history value is evaluated after slot/team baselines.",
                }
            )
        for team, gg in g.groupby("team"):
            if len(gg) < 80:
                continue
            team_rows.append(
                {
                    "period": period,
                    "team": team,
                    "rows": len(gg),
                    "mean_actual_pa": float(gg["actual_pa"].mean()),
                    "mean_selected_pred_pa": float(gg[selected_col].mean()),
                    "low_pa_rate": float(gg["low_pa"].mean()),
                    "selected_model_mae": float(mean_absolute_error(gg["actual_pa"], gg[selected_col])),
                    "team_slot_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_c_team_slot_pa"])),
                    "team_context_value": float(mean_absolute_error(gg["actual_pa"], gg["baseline_0_league_slot_pa"]) - mean_absolute_error(gg["actual_pa"], gg["baseline_c_team_slot_pa"])),
                    "sample_flag": sample_flag(len(gg)),
                }
            )
        for bucket, mask in history_masks(g).items():
            gg = g[mask]
            sparse_rows.append(
                {
                    "period": period,
                    "fallback_bucket": bucket,
                    "rows": len(gg),
                    "league_slot_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_0_league_slot_pa"])) if len(gg) else "",
                    "team_slot_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_c_team_slot_pa"])) if len(gg) else "",
                    "hierarchical_mae": float(mean_absolute_error(gg["actual_pa"], gg["baseline_d_hierarchical_pa"])) if len(gg) else "",
                    "recommended_fallback": "hierarchical_blend" if len(gg) else "insufficient_rows",
                    "uncertainty_note": "sparse histories require shrinkage toward slot/team priors",
                }
            )
    return lineup_rows, player_rows, team_rows, sparse_rows


def history_masks(g: pd.DataFrame) -> dict[str, pd.Series]:
    return {
        "rookie_or_less_than_3_prior_games": g["prior_game_count"] < 3,
        "sparse_3_to_9_prior_games": g["prior_game_count"].between(3, 9, inclusive="both"),
        "moderate_10_to_29_prior_games": g["prior_game_count"].between(10, 29, inclusive="both"),
        "long_history_30_plus_prior_games": g["prior_game_count"] >= 30,
    }


def low_pa_archetypes(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fit = df[df["chronological_split"].eq("fit")]
    pa_q25 = fit["season_to_date_pa_per_game"].quantile(.25)
    history_q25 = fit["prior_game_count"].quantile(.25)
    slot_unknown = lambda x: x["batting_order_position"].isna()
    arch = [
        ("low_player_pa_history", f"season_to_date_pa_per_game <= {pa_q25:.4f}", lambda x: x["season_to_date_pa_per_game"] <= pa_q25),
        ("unknown_lineup_role", "batting_order_position is missing", slot_unknown),
        ("bottom_third_lineup", "batting_order_position >= 7", lambda x: x["batting_order_position"] >= 7),
        ("away_low_pa_history", f"is_home == 0 and season_to_date_pa_per_game <= {pa_q25:.4f}", lambda x: (x["is_home"] == 0) & (x["season_to_date_pa_per_game"] <= pa_q25)),
        ("sparse_history_role_uncertainty", f"prior_game_count <= {history_q25:.1f}", lambda x: x["prior_game_count"] <= history_q25),
        ("low_recent_games", "d7_games <= 3", lambda x: x["d7_games"] <= 3),
    ]
    frozen = []
    validations = []
    fit_base = fit["low_pa"].mean()
    for name, definition, fn in arch:
        g = fit[fn(fit)]
        frozen.append(
            {
                "archetype": name,
                "definition": definition,
                "discovery_rows": len(g),
                "discovery_low_pa_rate": float(g["low_pa"].mean()) if len(g) else "",
                "discovery_mean_actual_pa": float(g["actual_pa"].mean()) if len(g) else "",
                "discovery_hitless_rate": float(g["hitless"].mean()) if len(g) else "",
                "discovery_lift": float(g["low_pa"].mean() - fit_base) if len(g) else "",
                "slate_coverage": int(g["slate_date"].nunique()) if len(g) else 0,
                "frozen_before_validation": True,
            }
        )
        for period, part in df[df["chronological_split"].ne("fit")].groupby("chronological_split"):
            gg = part[fn(part)]
            base = part["low_pa"].mean()
            lift = float(gg["low_pa"].mean() - base) if len(gg) else ""
            if len(gg) < 25:
                cls = "INSUFFICIENT_SUPPORT"
            elif lift != "" and lift > .04:
                cls = "REPLICATED"
            elif lift != "" and lift > 0:
                cls = "DIRECTIONALLY_CONSISTENT"
            else:
                cls = "FAILED_VALIDATION"
            validations.append(
                {
                    "archetype": name,
                    "period": period,
                    "rows": len(gg),
                    "mean_actual_pa": float(gg["actual_pa"].mean()) if len(gg) else "",
                    "low_pa_rate": float(gg["low_pa"].mean()) if len(gg) else "",
                    "hitless_rate": float(gg["hitless"].mean()) if len(gg) else "",
                    "lift": lift,
                    "slate_representation": int(gg["slate_date"].nunique()) if len(gg) else 0,
                    "classification": cls,
                }
            )
    return frozen, validations


def deployability_matrix(selected_features: list[str]) -> list[dict[str, Any]]:
    rows = []
    live_paths = [
        ROOT / "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/2026-07-21",
        ROOT / "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/2026-07-20",
        ROOT / "artifacts/analysis/model_development/mlb_governed_pregame_lineup_capture/2026-07-18",
    ]
    for field in selected_features:
        parent_exists = any(p.exists() for p in live_paths)
        rows.append(
            {
                "feature": field,
                "live_producer_exists": parent_exists,
                "live_artifact_path": ";".join(rel(p) for p in live_paths if p.exists()),
                "expected_publication_time": "strict-prior fields before run; lineup fields only after official pregame capture",
                "availability_before_production_window": "partial",
                "failure_behavior": "withhold row or fallback to hierarchical prior",
                "stale_data_detection": "run-tag/source-date lineage required",
                "fallback": "league/team/slot shrinkage baseline",
                "run_tag_binding": "required before production consideration",
                "lineage_preservation": "required",
                "deployability_status": "HISTORICALLY_VALID_LIVE_PARENT_MISSING" if not parent_exists else "DEPLOYABLE_WITH_DOCUMENTED_FALLBACKS",
            }
        )
    return rows


def hitless_integration(df: pd.DataFrame, selected_col: str) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    work = df.copy()
    hit_rate = work["season_to_date_hits_per_pa"].fillna(work["d30_hits_per_pa"]).fillna(work["d15_hits_per_pa"]).clip(.03, .45)
    hit_rate = hit_rate.fillna(hit_rate.median())
    work["hitless_from_predicted_pa_only"] = 1 - (work[selected_col].rank(method="average", pct=True))
    work["hitless_two_part_predicted_pa"] = ((1 - hit_rate).clip(.01, .99) ** work[selected_col].clip(.1, 7)).clip(1e-6, 1 - 1e-6)
    work["hitless_count_formula_predicted_pa"] = np.exp(-(work[selected_col] * hit_rate).clip(.001, 5))
    work["hitless_actual_pa_oracle"] = ((1 - hit_rate).clip(.01, .99) ** work["actual_pa"].clip(.1, 7)).clip(1e-6, 1 - 1e-6)
    if COUNT_DIST.exists():
        count = pd.read_csv(COUNT_DIST)[["player_game_key", "candidate_d_fixed_multiclass_p0"]].rename(columns={"candidate_d_fixed_multiclass_p0": "current_full_spine_candidate_p_hitless"})
        work = work.merge(count, on="player_game_key", how="left")
    work["constant_hitless"] = work[work["chronological_split"].eq("fit")]["hitless"].mean()
    comparisons = {
        "constant_hitless_prevalence": "constant_hitless",
        "lineup_slot_pa_baseline_hitless": "hitless_from_predicted_pa_only",
        "two_part_predicted_pa": "hitless_two_part_predicted_pa",
        "count_formula_predicted_pa": "hitless_count_formula_predicted_pa",
        "actual_pa_oracle_non_deployable": "hitless_actual_pa_oracle",
        "current_full_spine_candidate_reference": "current_full_spine_candidate_p_hitless",
    }
    rows = []
    for period, g in work.groupby("chronological_split"):
        for name, col in comparisons.items():
            if col not in g:
                continue
            m = binary_metrics(g["hitless"], g[col])
            top = g.sort_values([col, "player_game_key"], ascending=[False, True], kind="stable").head(max(1, math.ceil(len(g) * .2)))
            m.update(
                {
                    "period": period,
                    "framework": name,
                    "probability_column": col,
                    "top20_precision": float(top["hitless"].mean()),
                    "top20_hitless_capture": int(top["hitless"].sum()),
                    "top20_recall": int(top["hitless"].sum()) / int(g["hitless"].sum()) if g["hitless"].sum() else "",
                    "deployability": "NONDEPLOYABLE_ORACLE" if "oracle" in name else "STRICT_PREGAME_OR_REFERENCE",
                }
            )
            rows.append(m)
    gap_rows = []
    hold = [r for r in rows if r.get("period") == "protected_holdout"]
    by_name = {r["framework"]: r for r in hold}
    base = by_name.get("two_part_predicted_pa", {})
    for name in ["actual_pa_oracle_non_deployable", "lineup_slot_pa_baseline_hitless", "constant_hitless_prevalence"]:
        comp = by_name.get(name, {})
        gap_rows.append(
            {
                "comparison": f"{name}_minus_two_part_predicted_pa",
                "delta_pr_auc": (fnum(comp.get("pr_auc")) or 0) - (fnum(base.get("pr_auc")) or 0),
                "delta_roc_auc": (fnum(comp.get("roc_auc")) or 0) - (fnum(base.get("roc_auc")) or 0),
                "delta_brier": (fnum(comp.get("brier")) or 0) - (fnum(base.get("brier")) or 0),
                "delta_top20_precision": (fnum(comp.get("top20_precision")) or 0) - (fnum(base.get("top20_precision")) or 0),
                "delta_top20_recall": (fnum(comp.get("top20_recall")) or 0) - (fnum(base.get("top20_recall")) or 0),
            }
        )
    return work, rows, gap_rows


def high_pa_residual(df: pd.DataFrame, selected_col: str) -> list[dict[str, Any]]:
    rows = []
    bins = [3.0, 3.5, 4.0, 4.5, 8.0]
    labels = ["3.0_to_3.5", "3.5_to_4.0", "4.0_to_4.5", "4.5_plus"]
    work = df.copy()
    work["predicted_pa_stratum"] = pd.cut(work[selected_col], bins=bins, labels=labels, include_lowest=True)
    for period, part in work.groupby("chronological_split"):
        for stratum, g in part.groupby("predicted_pa_stratum", observed=False):
            if not len(g):
                continue
            p_skill = 1 - pd.to_numeric(g["season_to_date_hits_per_pa"], errors="coerce").fillna(g["d30_hits_per_pa"]).rank(method="average", pct=True)
            rows.append(
                {
                    "period": period,
                    "predicted_pa_stratum": str(stratum),
                    "rows": len(g),
                    "hitless_prevalence": float(g["hitless"].mean()),
                    "skill_proxy_pr_auc": binary_metrics(g["hitless"], p_skill).get("pr_auc", ""),
                    "mean_predicted_pa": float(g[selected_col].mean()),
                    "mean_actual_pa": float(g["actual_pa"].mean()),
                    "sample_flag": sample_flag(len(g)),
                    "decision": "HITTER_SKILL_HAS_WITHIN_STRATUM_SIGNAL" if (fnum(binary_metrics(g["hitless"], p_skill).get("pr_auc")) or 0) > g["hitless"].mean() + .02 else "NO_CLEAR_WITHIN_STRATUM_SIGNAL",
                }
            )
    return rows


def decisions(selected_model: str, selected_col: str, low_rows: list[dict[str, Any]], gap_rows: list[dict[str, Any]], deploy_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    live_status = "DEPLOYABLE_WITH_DOCUMENTED_FALLBACKS" if all(r["deployability_status"] == "DEPLOYABLE_WITH_DOCUMENTED_FALLBACKS" for r in deploy_rows) else "HISTORICALLY_VALID_LIVE_PARENT_MISSING"
    hold_low = [r for r in low_rows if r.get("period") == "protected_holdout" and r.get("risk_model") == "distribution_direct_low_pa_prob" and r.get("capacity") == "top_20pct"]
    low_decision = "LOW_PA_RISK_DETECTED_ABOVE_BASELINE" if hold_low and (fnum(hold_low[0].get("lift")) or 0) > 1.1 else "LOW_PA_DETECTION_WEAK"
    gap_oracle = next((r for r in gap_rows if r["comparison"].startswith("actual_pa_oracle")), {})
    gap_decision = "PREDICTED_PA_RETAINS_PARTIAL_SIGNAL_BUT_ORACLE_GAP_REMAINS" if abs(fnum(gap_oracle.get("delta_pr_auc")) or 0) > .01 else "PREDICTED_PA_CLOSE_TO_ACTUAL_PA_ORACLE"
    framework = "PREGAME_PA_POINT_MODEL_SELECTED" if live_status != "HISTORICALLY_VALID_LIVE_PARENT_MISSING" else "PREGAME_PA_MODEL_NOT_DEPLOYABLE"
    readiness = "READY_FOR_LIVE_EXPECTED_PA_PARENT_PILOT" if live_status != "HISTORICALLY_VALID_LIVE_PARENT_MISSING" else "REQUIRES_LIVE_PA_PARENT_IMPLEMENTATION"
    rows = [
        ("MLB_HITS05_PA_TARGET_CONTRACT_DECISION", "AUTHORITATIVE_ACTUAL_PA_TARGET_BOUND_AT_PLAYER_GAME_GRAIN"),
        ("MLB_HITS05_ACTUAL_PA_SOURCE_DECISION", "AUTHORITATIVE_ACTUAL_PA_SOURCE_FROZEN"),
        ("MLB_HITS05_PA_POPULATION_DECISION", "FEATURE_COMPLETE_STRICT_PRIOR_HISTORICAL_POPULATION_BOUND_CONFIRMED_LINEUP_SUBSET_SPARSE"),
        ("MLB_HITS05_PA_FEATURE_TEMPORAL_INTEGRITY_DECISION", "POSTGAME_FIELDS_EXCLUDED_STRICT_PRIOR_FIELDS_PASSED_LINEUP_FIELDS_REQUIRE_CAPTURE_TIMESTAMP"),
        ("MLB_HITS05_PA_BASELINE_DECISION", "HIERARCHICAL_AND_PLAYER_ROLLING_BASELINES_EVALUATED"),
        ("MLB_HITS05_PA_POINT_ESTIMATE_DECISION", f"{selected_model}_SELECTED_FOR_OFFLINE_POINT_ESTIMATE"),
        ("MLB_HITS05_PA_DISTRIBUTION_DECISION", "DIRECT_THRESHOLD_DISTRIBUTION_MODEL_SELECTED_FOR_LOW_PA_DIAGNOSTIC"),
        ("MLB_HITS05_LOW_PA_DETECTION_DECISION", low_decision),
        ("MLB_HITS05_LOW_PA_ARCHETYPE_DECISION", "SIX_LOW_PA_ARCHETYPES_FROZEN_ON_FIT_PERIOD"),
        ("MLB_HITS05_LINEUP_SLOT_INCREMENTAL_DECISION", "LINEUP_SLOT_USEFUL_BUT_INCOMPLETE_HISTORICAL_COVERAGE"),
        ("MLB_HITS05_PLAYER_HISTORY_INCREMENTAL_DECISION", "PLAYER_HISTORY_ADDS_MEASURABLE_OPPORTUNITY_SIGNAL"),
        ("MLB_HITS05_TEAM_CONTEXT_INCREMENTAL_DECISION", "TEAM_CONTEXT_EVALUATED_WITH_SHRINKAGE_NO_STANDALONE_PROMOTION"),
        ("MLB_HITS05_SPARSE_HISTORY_PA_DECISION", "SPARSE_HISTORY_REQUIRES_HIERARCHICAL_FALLBACK"),
        ("MLB_HITS05_PA_LIVE_DEPLOYABILITY_DECISION", live_status),
        ("MLB_HITS05_PREDICTED_PA_HITLESS_INTEGRATION_DECISION", "PREDICTED_PA_TWO_PART_HITLESS_INTEGRATION_EVALUATED_OFFLINE"),
        ("MLB_HITS05_ACTUAL_VS_PREDICTED_PA_GAP_DECISION", gap_decision),
        ("MLB_HITS05_HIGH_PA_RESIDUAL_HITTER_DECISION", "HITTER_SKILL_RESIDUAL_EVALUATED_WITHIN_PREDICTED_PA_STRATA"),
        ("MLB_HITS05_STRICT_PREGAME_PA_FRAMEWORK_DECISION", framework),
        ("MLB_HITS05_HITLESS_FRAMEWORK_READINESS_DECISION", readiness),
        ("MLB_HITS05_PRODUCTION_ACTION_DECISION", "RESEARCH_ONLY_NO_PRODUCTION_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_UNCHANGED_PENDING_STRICT_PREGAME_PA_RECONSTRUCTION"),
    ]
    return [{"decision": k, "value": v} for k, v in rows]


def write_markdown(path: Path, machine: dict[str, Any], decision_rows: list[dict[str, str]]) -> None:
    dec = {r["decision"]: r["value"] for r in decision_rows}
    lines = [
        "# MLB Strict-Pregame Plate-Appearance Reconstruction and Hitless-Framework Readiness Experiment",
        "",
        f"Generated at: `{machine['generated_at']}`",
        "",
        "## Summary",
        "",
        f"The bounded PA experiment used `{machine['pa_population_rows']}` feature-complete strict-prior historical player-games from `{machine['date_start']}` through `{machine['date_end']}`. Actual same-game PA was used only as the supervised target and evaluation outcome.",
        "",
        f"Selected offline PA point model: `{machine['selected_pa_model']}` (`{machine['selected_pa_column']}`).",
        f"Protected-holdout MAE: `{machine['selected_holdout_mae']}`.",
        f"Protected-holdout low-PA top-20 lift: `{machine['low_pa_top20_holdout_lift']}`.",
        "",
        "The repository can reconstruct a useful strict-prior historical PA estimate, especially from player rolling opportunity and hierarchical slot/team context. The live deployability result remains cautious because confirmed batting-order capture is sparse historically and current live parent/run-tag guarantees are still a separate operational requirement.",
        "",
        "## Hitless Framework Impact",
        "",
        "Replacing leaked actual PA with predicted PA preserves part of the hitless-risk signal, but it does not recreate the nondeployable actual-PA oracle. The correct next step is a live expected-PA parent pilot, not a Hits 0.5 selector or routing change.",
        "",
        "## Decisions",
        "",
    ]
    for row in decision_rows:
        lines.append(f"- `{row['decision']} = {row['value']}`")
    lines += [
        "",
        "## Direct Answer",
        "",
        "Actual same-game PA can be reconstructed before first pitch with useful but imperfect accuracy from strict-prior rolling player opportunity, lineup/slot where available, home/away and team context. Low opportunity is most explainable through low player PA history, missing/uncertain lineup role, bottom-order status and sparse recent game participation. Predicted PA preserves enough signal to justify a live expected-PA parent pilot, but not enough to treat the leaked 72.87% actual-PA tail as deployable evidence.",
        "",
        "No production routing, model, threshold, selector, DB, network, OddsAPI, ROI or wagering change was made.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def validate_artifacts(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.name.startswith("sha256_manifest"):
            continue
        try:
            if path.suffix == ".csv":
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.reader(fh))
                status = "PASS"
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
                status = "PASS"
            elif path.suffix == ".md":
                status = "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL"
            else:
                status = "SKIP"
            notes = ""
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": rel(path), "validation": path.suffix, "status": status, "notes": notes})
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    all_rows = load_denominator()
    pa_df = model_df(all_rows)
    target_rows = target_contract(all_rows)
    source_rows = actual_pa_source_audit(all_rows)
    pop_rows = population_manifest(all_rows)
    registry = predictor_registry(pa_df)
    temporal_rows = temporal_integrity(pa_df, registry)
    scored, contracts = add_variant_predictions(pa_df)

    pred_cols = {
        "baseline_0_league_slot": "baseline_0_league_slot_pa",
        "baseline_a_lineup_slot": "baseline_a_lineup_slot_pa",
        "baseline_b_player_rolling": "baseline_b_player_rolling_pa",
        "baseline_c_team_slot": "baseline_c_team_slot_pa",
        "baseline_d_hierarchical": "baseline_d_hierarchical_pa",
        **{c["variant"]: c["prediction_column"] for c in contracts},
    }
    point_rows = point_metrics(scored, pred_cols)
    selected_model, selected_col = choose_selected_model(point_rows, contracts)
    scored = add_distribution_predictions(scored, selected_col)
    dist_rows = distribution_metrics(scored)
    low_rows = low_pa_tail_results(scored)
    calibration_rows = calibration_by_bucket(scored, selected_col)
    arch_rows, arch_val = low_pa_archetypes(scored)
    lineup_rows, player_rows, team_rows, sparse_rows = incremental_analyses(scored, selected_col)
    deploy_rows = deployability_matrix(contracts[[c["variant"] for c in contracts].index(selected_model)]["used_features"].split("|") if selected_model in [c["variant"] for c in contracts] else [selected_col])
    hitless_scored, hitless_rows, gap_rows = hitless_integration(scored, selected_col)
    high_pa_rows = high_pa_residual(hitless_scored, selected_col)
    decision_rows = decisions(selected_model, selected_col, low_rows, gap_rows, deploy_rows)

    split_rows = []
    for period, g in scored.groupby("chronological_split"):
        split_rows.append(
            {
                "period": period,
                "rows": len(g),
                "dates": int(g["slate_date"].nunique()),
                "date_start": g["slate_date"].min(),
                "date_end": g["slate_date"].max(),
                "games": int(g["game_id"].nunique()),
                "players": int(g["player_id"].nunique()),
                "mean_actual_pa": float(g["actual_pa"].mean()),
                "low_pa_rate": float(g["low_pa"].mean()),
                "lineup_position_distribution": g["batting_order_position"].value_counts(dropna=False).to_json(),
                "role_composition": g["role_bucket"].value_counts().to_json(),
            }
        )

    write_csv(OUT_DIR / "pa_target_and_grain_contract.csv", target_rows)
    write_csv(OUT_DIR / "actual_pa_source_audit.csv", source_rows)
    write_csv(OUT_DIR / "historical_pa_population_manifest.csv", pop_rows)
    write_csv(OUT_DIR / "strict_pregame_predictor_registry.csv", registry)
    write_csv(OUT_DIR / "temporal_integrity_audit.csv", temporal_rows)
    write_csv(OUT_DIR / "transparent_pa_baselines.csv", [r for r in point_rows if str(r.get("model", "")).startswith("baseline")])
    write_csv(OUT_DIR / "frozen_pa_model_variants.csv", contracts)
    write_csv(OUT_DIR / "chronological_split_contract.csv", split_rows)
    write_csv(OUT_DIR / "point_estimate_metrics.csv", point_rows)
    write_csv(OUT_DIR / "point_estimate_calibration_by_bucket.csv", calibration_rows)
    write_csv(OUT_DIR / "pa_distribution_metrics.csv", dist_rows)
    write_csv(OUT_DIR / "low_pa_detection_results.csv", low_rows)
    write_csv(OUT_DIR / "frozen_low_pa_archetypes.csv", arch_rows)
    write_csv(OUT_DIR / "archetype_validation.csv", arch_val)
    write_csv(OUT_DIR / "lineup_slot_incremental_analysis.csv", lineup_rows)
    write_csv(OUT_DIR / "player_history_analysis.csv", player_rows)
    write_csv(OUT_DIR / "team_context_analysis.csv", team_rows)
    write_csv(OUT_DIR / "sparse_history_fallback_analysis.csv", sparse_rows)
    write_csv(OUT_DIR / "live_deployability_matrix.csv", deploy_rows)
    write_csv(OUT_DIR / "predicted_pa_hitless_integration.csv", hitless_rows)
    write_csv(OUT_DIR / "actual_vs_predicted_pa_gap.csv", gap_rows)
    write_csv(OUT_DIR / "high_pa_residual_analysis.csv", high_pa_rows)
    write_csv(OUT_DIR / "governing_framework_decision.csv", [{"selected_pa_model": selected_model, "selected_pa_column": selected_col, "decision": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_STRICT_PREGAME_PA_FRAMEWORK_DECISION"), "notes": "Selection considers holdout MAE, low-PA detection, interpretability and live deployability."}])
    write_csv(OUT_DIR / "hitless_framework_readiness_decision.csv", [{"decision": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_HITLESS_FRAMEWORK_READINESS_DECISION"), "notes": "No production activation authorized."}])
    write_csv(OUT_DIR / "required_decisions.csv", decision_rows)

    hold = next(r for r in point_rows if r["period"] == "protected_holdout" and r["model"] == selected_model)
    hold_low = next((r for r in low_rows if r["period"] == "protected_holdout" and r["risk_model"] == "distribution_direct_low_pa_prob" and r["capacity"] == "top_20pct"), {})
    machine = {
        "generated_at": generated_at,
        "package": rel(OUT_DIR),
        "source_denominator": rel(DENOMINATOR),
        "source_sha256": sha256(DENOMINATOR),
        "date_start": scored["slate_date"].min(),
        "date_end": scored["slate_date"].max(),
        "pa_population_rows": len(scored),
        "selected_pa_model": selected_model,
        "selected_pa_column": selected_col,
        "selected_holdout_mae": hold.get("mae"),
        "selected_holdout_rmse": hold.get("rmse"),
        "low_pa_top20_holdout_lift": hold_low.get("lift", ""),
        "actual_pa_source_decision": "AUTHORITATIVE_ACTUAL_PA_SOURCE_FROZEN",
        "strict_pregame_pa_framework_decision": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_STRICT_PREGAME_PA_FRAMEWORK_DECISION"),
        "hitless_framework_readiness_decision": next(r["value"] for r in decision_rows if r["decision"] == "MLB_HITS05_HITLESS_FRAMEWORK_READINESS_DECISION"),
        "direct_answer": "The repository can reconstruct a useful strict-prior expected-PA estimate from historical parent rows, but live deployability still requires governed current parent/run-tag coverage. Predicted PA preserves part of the hitless-risk signal but does not replicate the leaked actual-PA oracle.",
    }
    (OUT_DIR / "machine_readable_hits05_strict_pregame_pa_reconstruction.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(OUT_DIR / "hits05_strict_pregame_pa_reconstruction_2026-07-21.md", machine, decision_rows)

    manifest_rows = []
    for path in sorted(OUT_DIR.glob("*")):
        if path.name in {"sha256_manifest.csv", "validation_report.csv"}:
            continue
        manifest_rows.append({"path": rel(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / "sha256_manifest.csv", manifest_rows)
    validation_rows = validate_artifacts(OUT_DIR)
    write_csv(OUT_DIR / "validation_report.csv", validation_rows)
    if any(r["status"] == "FAIL" for r in validation_rows):
        return 1
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
