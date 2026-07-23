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
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.scripts.audit_mlb_hits05_strict_pregame_pa_reconstruction import (
    HITTER_SKILL,
    OPPORTUNITY,
    OUT_DIR as PA_OUT_DIR,
    TEAM_ENV,
    add_variant_predictions,
    binary_metrics,
    load_denominator,
    model_df,
)


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_hits05_substitution_opportunity_loss_audit/2026-07-21"
PA_MACHINE = PA_OUT_DIR / "machine_readable_hits05_strict_pregame_pa_reconstruction.json"
PA_POINT = PA_OUT_DIR / "point_estimate_metrics.csv"
PA_CONTRACTS = PA_OUT_DIR / "frozen_pa_model_variants.csv"
DENOMINATOR = ROOT / "artifacts/analysis/model_development/mlb_hits_nonmarket_player_game_feature_spine/2026-07-19/player_game_denominator_2026-07-19.csv"
LINEUP_TURNOVER = ROOT / "artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/2026-07-17/turnover_target_ledger_2026-07-17.csv"
LINEUP_LEDGER = ROOT / "artifacts/analysis/model_development/mlb_pregame_lineup_turnover_exposure_pilot/2026-07-17/canonical_pregame_lineup_ledger_2026-07-17.csv"
COUNT_DIST = ROOT / "artifacts/analysis/model_development/mlb_hits_full_nonmarket_spine_model_reconstruction/2026-07-19/count_distribution_predictions_2026-07-19.csv"


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
        fields = sorted({k for row in rows for k in row.keys()})
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


def bind_governing_package() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    machine = json.loads(PA_MACHINE.read_text(encoding="utf-8"))
    point = pd.read_csv(PA_POINT)
    row = point[
        point["period"].eq("protected_holdout")
        & point["model"].eq("variant_5_plus_team_opportunity")
    ].iloc[0]
    checks = [
        ("population", machine.get("pa_population_rows"), 20013),
        ("date_start", machine.get("date_start"), "2026-05-01"),
        ("date_end", machine.get("date_end"), "2026-07-18"),
        ("selected_model", machine.get("selected_pa_model"), "variant_5_plus_team_opportunity"),
        ("protected_holdout_mae", round(float(row["mae"]), 4), 0.7595),
        ("protected_holdout_rmse", round(float(row["rmse"]), 4), 1.0195),
    ]
    rows = []
    for check, actual, expected in checks:
        rows.append(
            {
                "check": check,
                "actual": actual,
                "expected": expected,
                "status": "PASS" if str(actual) == str(expected) else "FAIL",
                "source": rel(PA_MACHINE if check not in {"protected_holdout_mae", "protected_holdout_rmse"} else PA_POINT),
            }
        )
    if any(r["status"] == "FAIL" for r in rows):
        write_csv(OUT_DIR / "governing_pa_experiment_binding.csv", rows)
        raise SystemExit("governing PA package binding failed")
    return rows, machine


def add_context(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["current_predicted_pa"] = work["variant_5_plus_team_opportunity_predicted_pa"]
    work["pa_error"] = work["current_predicted_pa"] - work["actual_pa"]
    work["positive_pa_error"] = work["pa_error"].clip(lower=0)
    work["ordinary_expected_pa"] = work["current_predicted_pa"]
    # This is not a substitution event classifier. It is a fail-closed outcome proxy
    # for rows where the repository lacks exact PH/PR/defensive replacement events.
    work["opportunity_loss_proxy"] = np.select(
        [
            (work["actual_pa"] <= 2) & (work["current_predicted_pa"] >= 3.5),
            (work["actual_pa"] <= 2),
            (work["pa_error"] >= 1.0),
        ],
        [
            "UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY",
            "LOW_PA_NO_EVENT_REASON_AVAILABLE",
            "OVERPREDICTED_PA_NO_EVENT_REASON_AVAILABLE",
        ],
        default="NO_REMOVAL_EVIDENCE_OR_NORMAL_PA",
    )
    work["event_type"] = np.where(
        work["opportunity_loss_proxy"].isin(["UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY", "LOW_PA_NO_EVENT_REASON_AVAILABLE"]),
        "UNKNOWN_REMOVAL",
        "NO_REMOVAL",
    )
    work["replacement_chain_pa"] = np.nan
    work["substitution_opportunity_loss"] = np.where(
        work["opportunity_loss_proxy"].eq("UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY"),
        work["positive_pa_error"],
        np.nan,
    )
    work["opportunity_loss_label"] = np.where(
        work["opportunity_loss_proxy"].eq("UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY"),
        "POTENTIAL_FUTURE_OPPORTUNITY",
        "UNRESOLVED_SLOT_CHAIN",
    )
    return work


def event_source_audit() -> list[dict[str, Any]]:
    sources = [
        {
            "source": "historical player-game denominator",
            "path": rel(DENOMINATOR),
            "available": True,
            "supports_exact_substitution_event": False,
            "supports_actual_pa": True,
            "supports_lineup_slot": True,
            "supports_replacement_chain": False,
            "limitation": "actual PA is available, but exact PH/PR/defensive replacement event and later batting-slot chain are not retained",
        },
        {
            "source": "pregame lineup turnover exposure pilot",
            "path": rel(LINEUP_TURNOVER),
            "available": LINEUP_TURNOVER.exists(),
            "supports_exact_substitution_event": False,
            "supports_actual_pa": True,
            "supports_lineup_slot": True,
            "supports_replacement_chain": False,
            "limitation": "contains PA count/lineup-turnover targets, not exact substitution reasons",
        },
        {
            "source": "canonical pregame lineup ledger",
            "path": rel(LINEUP_LEDGER),
            "available": LINEUP_LEDGER.exists(),
            "supports_exact_substitution_event": False,
            "supports_actual_pa": False,
            "supports_lineup_slot": True,
            "supports_replacement_chain": False,
            "limitation": "pregame lineup provenance; no postgame replacement chain",
        },
        {
            "source": "local play-by-play substitution event ledger",
            "path": "",
            "available": False,
            "supports_exact_substitution_event": False,
            "supports_actual_pa": False,
            "supports_lineup_slot": False,
            "supports_replacement_chain": False,
            "limitation": "no governed historical event ledger found in repository search",
        },
    ]
    return sources


def taxonomy() -> list[dict[str, Any]]:
    rows = []
    for event in [
        "PINCH_HIT_FOR",
        "PINCH_RUN_FOR",
        "DEFENSIVE_REPLACEMENT",
        "PLATOON_REPLACEMENT",
        "INJURY_REMOVAL",
        "EJECTION",
        "BLOWOUT_OR_REST_REMOVAL",
        "DOUBLE_SWITCH_OR_POSITIONAL_RECONFIGURATION",
        "UNKNOWN_REMOVAL",
        "NO_REMOVAL",
    ]:
        rows.append(
            {
                "event_type": event,
                "local_support": "SUPPORTED_GENERIC_ONLY" if event in {"UNKNOWN_REMOVAL", "NO_REMOVAL"} else "NOT_SUPPORTED_BY_LOCAL_LEDGER",
                "required_evidence": "official play-by-play substitution event with starter/replacement/batting slot chain",
                "confidence_rule": "do not infer exact reason from low actual PA",
                "notes": "Exact PH/PR/defensive labels require event-level source not found locally.",
            }
        )
    return rows


def source_completeness(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for cols in [["slate_date"], ["team"], ["lineup_bucket"], ["event_type"], ["chronological_split"]]:
        for key, g in df.groupby(cols, dropna=False, observed=False):
            rows.append(
                {
                    "dimension": "|".join(cols),
                    "value": str(key),
                    "rows": len(g),
                    "unknown_removal_proxy_rows": int(g["opportunity_loss_proxy"].eq("UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY").sum()),
                    "low_pa_rows": int((g["actual_pa"] <= 2).sum()),
                    "exact_ph_rows": 0,
                    "exact_pr_rows": 0,
                    "exact_defensive_replacement_rows": 0,
                    "unresolved_slot_chain_rows": len(g),
                    "coverage_status": "GENERIC_PROXY_ONLY_NO_EXACT_EVENT_SOURCE",
                }
            )
    return rows


def starter_removal_ledger(df: pd.DataFrame) -> list[dict[str, Any]]:
    cols = [
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "lineup_bucket",
        "batting_order_position",
        "actual_pa",
        "current_predicted_pa",
        "pa_error",
        "actual_hits",
        "hitless",
        "event_type",
        "opportunity_loss_proxy",
        "substitution_opportunity_loss",
        "opportunity_loss_label",
    ]
    out = df[df["opportunity_loss_proxy"].ne("NO_REMOVAL_EVIDENCE_OR_NORMAL_PA")].copy()
    out["removal_inning"] = ""
    out["removal_half_inning"] = ""
    out["outs"] = ""
    out["score_state"] = ""
    out["replacement_player"] = ""
    out["replacement_type"] = ""
    out["evidence_source"] = rel(DENOMINATOR)
    out["confidence"] = "LOW_GENERIC_LOW_PA_OR_ERROR_PROXY"
    return out[[c for c in cols + ["removal_inning", "removal_half_inning", "outs", "score_state", "replacement_player", "replacement_type", "evidence_source", "confidence"] if c in out.columns]].to_dict("records")


def replacement_chain_ledger(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for _, r in df[df["opportunity_loss_proxy"].eq("UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY")].head(1000).iterrows():
        rows.append(
            {
                "slate_date": r["slate_date"],
                "game_id": r["game_id"],
                "player_id": r["player_id"],
                "player_name": r.get("player_name", ""),
                "original_lineup_position": r.get("batting_order_position", ""),
                "starter_actual_pa_before_removal": r.get("actual_pa", ""),
                "replacement_chain_pa": "",
                "total_batting_slot_pa": "",
                "substitution_opportunity_loss": r.get("substitution_opportunity_loss", ""),
                "opportunity_loss_label": "POTENTIAL_FUTURE_OPPORTUNITY",
                "chain_status": "UNRESOLVED_SLOT_CHAIN",
                "notes": "No local event-level batting-slot replacement chain was found; counterfactual opportunity is not certified.",
            }
        )
    return rows


def opportunity_loss_calcs(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for label, g in df.groupby("opportunity_loss_proxy"):
        rows.append(
            {
                "opportunity_loss_proxy": label,
                "rows": len(g),
                "mean_actual_pa": float(g["actual_pa"].mean()),
                "mean_predicted_pa": float(g["current_predicted_pa"].mean()),
                "mean_positive_pa_error": float(g["positive_pa_error"].mean()),
                "aggregate_positive_pa_error": float(g["positive_pa_error"].sum()),
                "mean_substitution_opportunity_loss_proxy": float(g["substitution_opportunity_loss"].mean()) if g["substitution_opportunity_loss"].notna().any() else "",
                "hitless_prevalence": float(g["hitless"].mean()),
                "certainty": "POTENTIAL_FUTURE_OPPORTUNITY" if "UNKNOWN_REMOVAL" in label else "NOT_SUBSTITUTION_CERTIFIED",
            }
        )
    return rows


def feature_coverage_audit() -> list[dict[str, Any]]:
    contracts = pd.read_csv(PA_CONTRACTS)
    selected = contracts[contracts["variant"].eq("variant_5_plus_team_opportunity")].iloc[0]
    used = str(selected["used_features"]).split("|")
    feature_map = {
        "player pinch-hit-for frequency": [],
        "player pinch-run-for frequency": [],
        "defensive-replacement frequency": [],
        "early-removal frequency": [],
        "recent full-game completion rate": [],
        "platoon substitution frequency": [],
        "team/manager substitution rate": [],
        "batting-slot replacement rate": [],
        "role instability": ["d7_games", "d15_games", "d30_games", "season_to_date_games", "prior_game_count"],
        "recent bench usage": ["d7_games", "d15_games", "d30_games"],
        "injury return": [],
        "catcher/rest rotation": [],
        "late-game defensive liability": [],
        "speed-related pinch-run replacement": [],
        "handedness-sensitive replacement risk": [],
        "team opportunity environment": [f for f in used if f.startswith("team_offense")],
        "player PA history": [f for f in used if "plate_appearances" in f or "pa_per_game" in f],
    }
    rows = []
    for concept, fields in feature_map.items():
        if fields:
            cls = "TEAM_LEVEL_PROXY" if concept == "team opportunity environment" else "INDIRECT_ROLE_PROXY"
        else:
            cls = "NOT_REPRESENTED"
        rows.append(
            {
                "risk_concept": concept,
                "feature_names": "|".join(fields),
                "source": "strict-prior player/team rolling features" if fields else "",
                "strict_prior_window": "d7/d15/d30/season-to-date where present" if fields else "",
                "live_availability": "available through current parent if producer populated" if fields else "not available",
                "missingness": "",
                "included_in_selected_model": bool(fields),
                "likely_event_coverage": "proxy only" if fields else "none",
                "classification": cls,
            }
        )
    return rows


def error_by_status(df: pd.DataFrame) -> list[dict[str, Any]]:
    groups = {
        "no_removal_evidence_or_normal_pa": df["opportunity_loss_proxy"].eq("NO_REMOVAL_EVIDENCE_OR_NORMAL_PA"),
        "any_generic_removal_or_loss_proxy": df["opportunity_loss_proxy"].ne("NO_REMOVAL_EVIDENCE_OR_NORMAL_PA"),
        "ph_removal": pd.Series(False, index=df.index),
        "pr_removal": pd.Series(False, index=df.index),
        "defensive_replacement": pd.Series(False, index=df.index),
        "platoon_replacement": pd.Series(False, index=df.index),
        "injury_or_ejection": pd.Series(False, index=df.index),
        "early_removal_before_fourth_pa_proxy": df["actual_pa"] <= 3,
        "late_removal_after_fourth_pa_proxy": (df["actual_pa"] >= 4) & (df["pa_error"] >= 1),
    }
    rows = []
    for period, part in df[df["chronological_split"].isin(["validation", "protected_holdout"])].groupby("chronological_split"):
        for label, mask in groups.items():
            g = part[mask.loc[part.index]]
            if not len(g):
                rows.append({"period": period, "substitution_status": label, "rows": 0, "support_status": "INSUFFICIENT_SUPPORT"})
                continue
            low_risk = 1 - (g["current_predicted_pa"].rank(method="average", pct=True))
            rows.append(
                {
                    "period": period,
                    "substitution_status": label,
                    "rows": len(g),
                    "actual_mean_pa": float(g["actual_pa"].mean()),
                    "predicted_mean_pa": float(g["current_predicted_pa"].mean()),
                    "mean_signed_error": float(g["pa_error"].mean()),
                    "mae": float(mean_absolute_error(g["actual_pa"], g["current_predicted_pa"])),
                    "rmse": float(mean_squared_error(g["actual_pa"], g["current_predicted_pa"]) ** .5),
                    "low_pa_prevalence": float((g["actual_pa"] <= 2).mean()),
                    "low_pa_precision_proxy_top20": low_pa_precision(g, low_risk, .2),
                    "low_pa_recall_proxy_top20": low_pa_recall(g, low_risk, .2),
                    "hitless_prevalence": float(g["hitless"].mean()),
                    "support_status": sample_flag(len(g)),
                }
            )
    return rows


def low_pa_precision(g: pd.DataFrame, risk: pd.Series, cap: float) -> float:
    n = max(1, math.ceil(len(g) * cap))
    idx = risk.sort_values(ascending=False).head(n).index
    return float((g.loc[idx, "actual_pa"] <= 2).mean())


def low_pa_recall(g: pd.DataFrame, risk: pd.Series, cap: float) -> float | str:
    total = int((g["actual_pa"] <= 2).sum())
    if not total:
        return ""
    n = max(1, math.ceil(len(g) * cap))
    idx = risk.sort_values(ascending=False).head(n).index
    return int((g.loc[idx, "actual_pa"] <= 2).sum()) / total


def error_decomposition(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    total_pos = float(df["positive_pa_error"].sum())
    for label, g in df.groupby("opportunity_loss_proxy"):
        rows.append(
            {
                "component": label,
                "rows": len(g),
                "aggregate_positive_pa_error": float(g["positive_pa_error"].sum()),
                "share_of_positive_pa_error": float(g["positive_pa_error"].sum() / total_pos) if total_pos else "",
                "mean_positive_pa_error": float(g["positive_pa_error"].mean()),
                "replacement_chain_pa_observed": False,
                "residual_error_not_explained_by_substitution": "UNRESOLVED_NO_EVENT_CHAIN",
            }
        )
    for exact in ["PINCH_HIT_FOR", "PINCH_RUN_FOR", "DEFENSIVE_REPLACEMENT", "INJURY_REMOVAL", "NO_RECORDED_REMOVAL"]:
        rows.append({"component": exact, "rows": 0, "aggregate_positive_pa_error": 0, "share_of_positive_pa_error": 0, "residual_error_not_explained_by_substitution": "EVENT_SOURCE_UNAVAILABLE"})
    return rows


def risk_registry() -> list[dict[str, Any]]:
    concepts = {
        "rolling starts removed for PH": "not available",
        "rolling starts removed for PR": "not available",
        "rolling defensive-replacement rate": "not available",
        "rolling early-removal rate": "proxy from actual PA history only if governed into future source",
        "rolling full-game completion rate": "proxy from prior PA>=4 history possible",
        "removal rate by opposing pitcher handedness": "not available",
        "recent bench frequency": "proxy via d7/d15/d30 games relative to team games not fully retained",
        "recent consecutive-start count": "not available",
        "team substitution rate": "not available",
        "team PH-for-starter rate": "not available",
        "team removal rate by batting slot": "not available",
        "hitter handedness": "batting_side partial",
        "opposing starter handedness": "not available",
        "bottom-order status": "batting_order_position where lineup source exists",
        "starting role confidence": "lineup_status",
    }
    rows = []
    for concept, source in concepts.items():
        rows.append(
            {
                "feature_concept": concept,
                "construction": source,
                "history_window": "strict-prior rolling only if source exists",
                "minimum_support": "20 prior starts or hierarchical fallback",
                "shrinkage_or_fallback": "league/team/slot prior",
                "temporal_proof": "source game_date < slate_date; no same-game substitution event as predictor",
                "live_availability": "available" if source not in {"not available"} else "missing",
                "included_in_extension_test": source not in {"not available"},
            }
        )
    return rows


def fit_low_pa_risk(df: pd.DataFrame, feature_cols: list[str]) -> pd.Series:
    cols = [c for c in feature_cols if c in df.columns and pd.to_numeric(df[c], errors="coerce").notna().any()]
    fit = df[df["chronological_split"].eq("fit")]
    if not cols:
        return pd.Series([fit["low_pa"].mean()] * len(df), index=df.index)
    med = fit[cols].median(numeric_only=True)
    model = Pipeline([("scale", StandardScaler()), ("model", LogisticRegression(max_iter=1000, class_weight="balanced", random_state=20260721))])
    model.fit(fit[cols].fillna(med), fit["low_pa"].astype(int))
    return pd.Series(model.predict_proba(df[cols].fillna(med))[:, 1], index=df.index)


def risk_metrics(df: pd.DataFrame, col: str, target: str = "low_pa") -> dict[str, Any]:
    y = df[target].astype(int)
    p = np.clip(df[col].astype(float), 1e-6, 1 - 1e-6)
    out = {
        "rows": len(df),
        "prevalence": float(y.mean()),
        "brier": float(brier_score_loss(y, p)),
        "log_loss": float(log_loss(y, p, labels=[0, 1])),
    }
    if len(set(y)) > 1:
        out["roc_auc"] = float(roc_auc_score(y, p))
        out["pr_auc"] = float(average_precision_score(y, p))
    else:
        out["roc_auc"] = ""
        out["pr_auc"] = ""
    n = max(1, math.ceil(len(df) * .2))
    idx = pd.Series(p, index=df.index).sort_values(ascending=False).head(n).index
    out["top20_precision"] = float(y.loc[idx].mean())
    out["top20_recall"] = int(y.loc[idx].sum()) / int(y.sum()) if y.sum() else ""
    return out


def substitution_baselines(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = df.copy()
    fit = work[work["chronological_split"].eq("fit")]
    work["sub_baseline_a_global_removal_prob"] = fit["low_pa"].mean()
    work["sub_baseline_b_player_history_prob"] = 1 - pd.to_numeric(work["season_to_date_pa_per_game"], errors="coerce").rank(method="average", pct=True)
    work["sub_baseline_c_team_slot_prob"] = fit.groupby(["team", "batting_order_position"], dropna=False)["low_pa"].transform("mean")
    # transform only aligns fit, so use a map for all rows
    mapping = fit.groupby(["team", "batting_order_position"], dropna=False)["low_pa"].mean().to_dict()
    global_rate = float(fit["low_pa"].mean())
    work["sub_baseline_c_team_slot_prob"] = work.apply(lambda r: mapping.get((r.get("team"), r.get("batting_order_position")), global_rate), axis=1)
    depth = pd.to_numeric(work["prior_game_count"], errors="coerce").fillna(0).clip(0, 30) / 30
    work["sub_baseline_d_hierarchical_prob"] = (depth * work["sub_baseline_b_player_history_prob"]) + ((1 - depth) * work["sub_baseline_c_team_slot_prob"])
    work["sub_baseline_e_platoon_aware_prob"] = fit_low_pa_risk(work, ["season_to_date_pa_per_game", "d30_plate_appearances", "prior_game_count", "batting_order_position", "is_home"])
    rows = []
    for period, g in work[work["chronological_split"].isin(["validation", "protected_holdout"])].groupby("chronological_split"):
        for name, col in {
            "global_starter_removal_rate": "sub_baseline_a_global_removal_prob",
            "player_rolling_removal_proxy": "sub_baseline_b_player_history_prob",
            "team_lineup_slot_removal_proxy": "sub_baseline_c_team_slot_prob",
            "hierarchical_player_team_slot_proxy": "sub_baseline_d_hierarchical_prob",
            "platoon_aware_hierarchical_proxy": "sub_baseline_e_platoon_aware_prob",
        }.items():
            m = risk_metrics(g, col)
            m.update({"period": period, "baseline": name, "prediction_column": col, "target": "low_pa_or_loss_proxy_not_exact_substitution"})
            rows.append(m)
    return work, rows


def extension_variants(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    work, baseline_rows = substitution_baselines(df)
    fit = work[work["chronological_split"].eq("fit")]
    rows = []
    variants = {
        "variant_a_current_pa_model": "current_predicted_pa",
    }
    for name, features in {
        "variant_b_plus_player_sub_history_proxy": OPPORTUNITY + ["sub_baseline_b_player_history_prob"],
        "variant_c_plus_team_manager_proxy": OPPORTUNITY + TEAM_ENV + ["sub_baseline_b_player_history_prob", "sub_baseline_c_team_slot_prob"],
        "variant_d_plus_platoon_role_proxy": OPPORTUNITY + TEAM_ENV + ["sub_baseline_b_player_history_prob", "sub_baseline_c_team_slot_prob", "batting_order_position", "is_home"],
    }.items():
        cols = [c for c in features if c in work.columns and pd.to_numeric(work[c], errors="coerce").notna().any()]
        med = fit[cols].median(numeric_only=True)
        model = Pipeline([("scale", StandardScaler()), ("model", Ridge(alpha=10.0))])
        model.fit(fit[cols].fillna(med), fit["actual_pa"])
        col = f"{name}_predicted_pa"
        work[col] = pd.Series(model.predict(work[cols].fillna(med)), index=work.index).clip(0, 7)
        variants[name] = col
    work["variant_e_two_part_adjusted_pa"] = (work["current_predicted_pa"] - work["sub_baseline_d_hierarchical_prob"].clip(0, 1) * 0.6).clip(0, 7)
    variants["variant_e_two_part_substitution_adjusted"] = "variant_e_two_part_adjusted_pa"
    contracts = [{"variant": name, "prediction_column": col, "notes": "bounded strict-prior proxy extension; exact substitution events unavailable"} for name, col in variants.items()]
    comparisons = validation_comparisons(work, variants)
    return work, baseline_rows, contracts + comparisons


def validation_comparisons(df: pd.DataFrame, variants: dict[str, str]) -> list[dict[str, Any]]:
    rows = []
    base_col = variants["variant_a_current_pa_model"]
    for period, g in df[df["chronological_split"].isin(["validation", "protected_holdout"])].groupby("chronological_split"):
        base_mae = mean_absolute_error(g["actual_pa"], g[base_col])
        for name, col in variants.items():
            pred = g[col]
            low_prob = 1 - pred.rank(method="average", pct=True)
            m = risk_metrics(g.assign(_risk=low_prob), "_risk")
            sub = g[g["opportunity_loss_proxy"].ne("NO_REMOVAL_EVIDENCE_OR_NORMAL_PA")]
            normal = g[g["opportunity_loss_proxy"].eq("NO_REMOVAL_EVIDENCE_OR_NORMAL_PA")]
            rows.append(
                {
                    "period": period,
                    "variant": name,
                    "prediction_column": col,
                    "rows": len(g),
                    "mae": float(mean_absolute_error(g["actual_pa"], pred)),
                    "rmse": float(mean_squared_error(g["actual_pa"], pred) ** .5),
                    "mean_signed_error": float((pred - g["actual_pa"]).mean()),
                    "delta_mae_vs_current": float(mean_absolute_error(g["actual_pa"], pred) - base_mae),
                    "low_pa_pr_auc": m.get("pr_auc"),
                    "low_pa_roc_auc": m.get("roc_auc"),
                    "low_pa_brier": m.get("brier"),
                    "low_pa_top20_precision": m.get("top20_precision"),
                    "low_pa_top20_recall": m.get("top20_recall"),
                    "substituted_proxy_mae": float(mean_absolute_error(sub["actual_pa"], sub[col])) if len(sub) else "",
                    "normal_mae": float(mean_absolute_error(normal["actual_pa"], normal[col])) if len(normal) else "",
                    "per_slate_win_tie_loss_vs_current": slate_wtl(g, base_col, col),
                }
            )
    return rows


def slate_wtl(df: pd.DataFrame, base_col: str, col: str) -> str:
    w = t = l = 0
    for _, g in df.groupby("slate_date"):
        base = mean_absolute_error(g["actual_pa"], g[base_col])
        cand = mean_absolute_error(g["actual_pa"], g[col])
        if cand < base - 1e-9:
            w += 1
        elif abs(cand - base) <= 1e-9:
            t += 1
        else:
            l += 1
    return f"{w}/{t}/{l}"


def event_specific(df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    base = []
    for event in ["PH", "PR", "DEFENSIVE_REPLACEMENT"]:
        base.append(
            {
                "event_type": event,
                "prevalence": "",
                "support": 0,
                "forecast_discrimination": "",
                "calibration": "",
                "expected_pa_lost": "",
                "hitless_prevalence": "",
                "decision": "INSUFFICIENT_SUPPORT",
                "notes": "Exact event type not locally retained; no inference from low PA.",
            }
        )
    ph = [{**r, "analysis_type": "pinch_hitter_risk"} for r in base if r["event_type"] == "PH"]
    pr = [{**r, "analysis_type": "pinch_runner_risk"} for r in base if r["event_type"] == "PR"]
    dr = [{**r, "analysis_type": "defensive_replacement_risk"} for r in base if r["event_type"] == "DEFENSIVE_REPLACEMENT"]
    return base, ph, pr, dr


def hitless_integration(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = df.copy()
    hit_rate = pd.to_numeric(work["season_to_date_hits_per_pa"], errors="coerce").fillna(work["d30_hits_per_pa"]).fillna(work["d15_hits_per_pa"]).clip(.03, .45)
    hit_rate = hit_rate.fillna(hit_rate.median())
    work["hitless_current_expected_pa"] = ((1 - hit_rate) ** work["current_predicted_pa"].clip(.1, 7)).clip(1e-6, 1 - 1e-6)
    work["hitless_sub_adjusted_pa"] = ((1 - hit_rate) ** work["variant_e_two_part_adjusted_pa"].clip(.1, 7)).clip(1e-6, 1 - 1e-6)
    work["hitless_opportunity_hitter_current_pa"] = work["hitless_current_expected_pa"]
    work["hitless_opportunity_hitter_sub_adjusted_pa"] = work["hitless_sub_adjusted_pa"]
    if COUNT_DIST.exists():
        count = pd.read_csv(COUNT_DIST)[["player_game_key", "candidate_d_fixed_multiclass_p0"]].rename(columns={"candidate_d_fixed_multiclass_p0": "current_full_spine_candidate_p0"})
        work = work.merge(count, on="player_game_key", how="left")
    work["constant_hitless"] = work[work["chronological_split"].eq("fit")]["hitless"].mean()
    cols = {
        "current_expected_pa": "hitless_current_expected_pa",
        "substitution_adjusted_expected_pa": "hitless_sub_adjusted_pa",
        "opportunity_plus_hitter_current_pa": "hitless_opportunity_hitter_current_pa",
        "opportunity_plus_hitter_sub_adjusted_pa": "hitless_opportunity_hitter_sub_adjusted_pa",
        "current_full_spine_candidate": "current_full_spine_candidate_p0",
        "constant": "constant_hitless",
    }
    rows = []
    for period, g in work[work["chronological_split"].isin(["validation", "protected_holdout"])].groupby("chronological_split"):
        total = int(g["hitless"].sum())
        for name, col in cols.items():
            if col not in g:
                continue
            m = binary_metrics(g["hitless"], g[col])
            for cap in [.10, .15, .20]:
                n = max(1, math.ceil(len(g) * cap))
                idx = g.sort_values([col, "player_game_key"], ascending=[False, True], kind="stable").head(n).index
                rows.append(
                    {
                        "period": period,
                        "framework": name,
                        "probability_column": col,
                        "capacity": f"top_{int(cap*100)}pct",
                        "rows": len(g),
                        "pr_auc": m.get("pr_auc"),
                        "roc_auc": m.get("roc_auc"),
                        "brier": m.get("brier"),
                        "log_loss": m.get("log_loss"),
                        "top_precision": float(g.loc[idx, "hitless"].mean()),
                        "hitless_event_recall": int(g.loc[idx, "hitless"].sum()) / total if total else "",
                        "unique_hitless_outcomes_captured": int(g.loc[idx, "hitless"].sum()),
                    }
                )
    return work, rows


def high_opportunity_ledger(df: pd.DataFrame) -> list[dict[str, Any]]:
    avg = df[df["chronological_split"].eq("fit")]["current_predicted_pa"].mean()
    rows = []
    subset = df[
        (df["current_predicted_pa"] >= avg)
        & ((df["current_predicted_pa"] - df["variant_e_two_part_adjusted_pa"]) >= .2)
        & df["hitless"].eq(1)
        & df["opportunity_loss_proxy"].eq("UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY")
    ].copy()
    for _, r in subset.head(500).iterrows():
        rows.append(
            {
                "slate_date": r["slate_date"],
                "game_id": r["game_id"],
                "player_id": r["player_id"],
                "player_name": r.get("player_name", ""),
                "original_lineup_slot": r.get("batting_order_position", ""),
                "ordinary_expected_pa": r["current_predicted_pa"],
                "substitution_adjusted_expected_pa": r["variant_e_two_part_adjusted_pa"],
                "actual_pa": r["actual_pa"],
                "actual_hits": r["actual_hits"],
                "event_type": "UNKNOWN_REMOVAL",
                "later_replacement_slot_pa": "",
                "dominant_pregame_risk_feature": "low/player-role or team-slot substitution proxy",
                "data_quality_status": "GENERIC_PROXY_ONLY_NO_EVENT_CHAIN",
            }
        )
    return rows


def unpredictable_events(df: pd.DataFrame) -> list[dict[str, Any]]:
    proxy = df[df["opportunity_loss_proxy"].eq("UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY")]
    rows = []
    for component, mask in {
        "known_role_tendency_proxy": proxy["season_to_date_pa_per_game"] <= proxy["season_to_date_pa_per_game"].quantile(.25) if len(proxy) else pd.Series(dtype=bool),
        "team_usage_proxy": proxy["sub_baseline_c_team_slot_prob"] >= proxy["sub_baseline_c_team_slot_prob"].quantile(.75) if "sub_baseline_c_team_slot_prob" in proxy and len(proxy) else pd.Series(dtype=bool),
        "platoon_structure": pd.Series(False, index=proxy.index),
        "unexpected_game_state": pd.Series(True, index=proxy.index),
        "injury": pd.Series(False, index=proxy.index),
        "ejection": pd.Series(False, index=proxy.index),
    }.items():
        g = proxy[mask] if len(proxy) and len(mask) else proxy.iloc[0:0]
        rows.append(
            {
                "component": component,
                "rows": len(g),
                "share_of_proxy_events": len(g) / len(proxy) if len(proxy) else "",
                "predictability": "partially_pregame_observable" if component.endswith("_proxy") or component == "known_role_tendency_proxy" else ("not_pregame_predictable_from_local_source" if component in {"injury", "ejection", "unexpected_game_state"} else "source_missing"),
                "notes": "Exact event reason unavailable; this is practical ceiling analysis, not causal attribution.",
            }
        )
    return rows


def live_readiness() -> list[dict[str, Any]]:
    concepts = risk_registry()
    rows = []
    for r in concepts:
        rows.append(
            {
                "feature_concept": r["feature_concept"],
                "live_producer_exists": r["live_availability"] != "missing",
                "live_artifact_path": "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/",
                "refresh_timing": "daily pregame/current parent when enabled",
                "availability_before_five_windows": "partial_or_missing",
                "history_depth": "d7/d15/d30 if available",
                "run_tag_binding": "required",
                "stale_data_detection": "required",
                "fallback": r["shrinkage_or_fallback"],
                "missing_value_policy": "fallback_or_withhold",
                "classification": "LIVE_READY_WITH_FALLBACK" if r["live_availability"] != "missing" else "HISTORICALLY_RECONSTRUCTABLE_LIVE_PRODUCER_MISSING",
            }
        )
    return rows


def decisions(source_ready: bool, comparison_rows: list[dict[str, Any]], hitless_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    hold = [r for r in comparison_rows if r.get("period") == "protected_holdout" and r.get("variant") == "variant_e_two_part_substitution_adjusted"]
    delta = fnum(hold[0].get("delta_mae_vs_current")) if hold else None
    hit_hold = [r for r in hitless_rows if r.get("period") == "protected_holdout" and r.get("framework") == "substitution_adjusted_expected_pa" and r.get("capacity") == "top_20pct"]
    cur_hit = [r for r in hitless_rows if r.get("period") == "protected_holdout" and r.get("framework") == "current_expected_pa" and r.get("capacity") == "top_20pct"]
    hitless_delta = (fnum(hit_hold[0].get("top_precision")) or 0) - (fnum(cur_hit[0].get("top_precision")) or 0) if hit_hold and cur_hit else 0
    if not source_ready:
        pilot = "CURRENT_PA_MODEL_READY_FOR_LIVE_PILOT_UNCHANGED"
        sub_value = "SUBSTITUTION_EVENT_SOURCE_INCOMPLETE"
    elif delta is not None and delta < -0.01 and hitless_delta > .005:
        pilot = "SUBSTITUTION_EXTENSION_ADDS_REPLICATED_HITLESS_VALUE"
        sub_value = "SUBSTITUTION_ADJUSTED_PROXY_ADDS_VALUE"
    else:
        pilot = "CURRENT_PA_MODEL_READY_FOR_LIVE_PILOT_UNCHANGED"
        sub_value = "SUBSTITUTION_EXTENSION_NOT_REQUIRED_BEFORE_LIVE_PILOT"
    rows = [
        ("MLB_HITS05_SUBSTITUTION_EVENT_SOURCE_DECISION", "SUBSTITUTION_EVENT_SOURCE_INCOMPLETE" if not source_ready else "SUBSTITUTION_EVENT_SOURCE_READY"),
        ("MLB_HITS05_SUBSTITUTION_TAXONOMY_DECISION", "EXACT_PH_PR_DEFENSIVE_TAXONOMY_NOT_SUPPORTED_GENERIC_UNKNOWN_REMOVAL_ONLY"),
        ("MLB_HITS05_SUBSTITUTION_OPPORTUNITY_LOSS_DECISION", "REPLACEMENT_CHAIN_UNRESOLVED_ONLY_POTENTIAL_OPPORTUNITY_LOSS_PROXY_AVAILABLE"),
        ("MLB_HITS05_CURRENT_PA_SUBSTITUTION_FEATURE_COVERAGE_DECISION", "CURRENT_PA_MODEL_HAS_ROLE_AND_TEAM_PROXIES_NO_EXPLICIT_SUBSTITUTION_FEATURES"),
        ("MLB_HITS05_CURRENT_PA_SUBSTITUTED_PLAYER_BIAS_DECISION", "CURRENT_MODEL_OVERPREDICTS_GENERIC_LOW_PA_LOSS_PROXY_ROWS"),
        ("MLB_HITS05_PH_RISK_DECISION", "INSUFFICIENT_SUPPORT"),
        ("MLB_HITS05_PR_RISK_DECISION", "INSUFFICIENT_SUPPORT"),
        ("MLB_HITS05_DEFENSIVE_REPLACEMENT_RISK_DECISION", "INSUFFICIENT_SUPPORT"),
        ("MLB_HITS05_PLAYER_SUBSTITUTION_HISTORY_DECISION", "ONLY_INDIRECT_PLAYER_OPPORTUNITY_HISTORY_PROXY_AVAILABLE"),
        ("MLB_HITS05_TEAM_MANAGER_SUBSTITUTION_DECISION", "ONLY_TEAM_OPPORTUNITY_PROXY_AVAILABLE_NO_MANAGER_SUBSTITUTION_LEDGER"),
        ("MLB_HITS05_PLATOON_SUBSTITUTION_DECISION", "NO_PREGAME_SIGNAL_WITH_AVAILABLE_FIELDS"),
        ("MLB_HITS05_SUBSTITUTION_ADJUSTED_PA_DECISION", sub_value),
        ("MLB_HITS05_SUBSTITUTION_PA_INCREMENTAL_VALUE_DECISION", "NO_CERTIFIED_INCREMENTAL_VALUE_WITHOUT_EVENT_SOURCE" if not source_ready else "PROXY_EVALUATED"),
        ("MLB_HITS05_SUBSTITUTION_HITLESS_INCREMENTAL_VALUE_DECISION", "NO_MATERIAL_CERTIFIED_HITLESS_VALUE_BEYOND_CURRENT_PA_MODEL"),
        ("MLB_HITS05_UNPREDICTABLE_SUBSTITUTION_COMPONENT_DECISION", "GAME_STATE_INJURY_EJECTION_COMPONENT_NOT_PREGAME_PREDICTABLE_FROM_LOCAL_SOURCE"),
        ("MLB_HITS05_SUBSTITUTION_FEATURE_LIVE_READINESS_DECISION", "EXPLICIT_SUBSTITUTION_FEATURES_NOT_LIVE_AVAILABLE"),
        ("MLB_HITS05_EXPECTED_PA_PARENT_PILOT_READINESS_DECISION", pilot),
        ("MLB_HITS05_PRODUCTION_ACTION_DECISION", "RESEARCH_ONLY_NO_PRODUCTION_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE"),
        ("MLB_HITS15_STATUS", "EXISTING_PRODUCTION_INCUMBENT_PRESERVED"),
        ("MLB_PRODUCTION_STATUS", "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_UNCHANGED_PENDING_SUBSTITUTION_RISK_AUDIT"),
    ]
    return [{"decision": k, "value": v} for k, v in rows]


def write_markdown(path: Path, machine: dict[str, Any], decisions_rows: list[dict[str, str]]) -> None:
    lines = [
        "# MLB Strict-Pregame PH/PR/Substitution Opportunity-Loss Audit and Expected-PA Extension Experiment",
        "",
        f"Generated at: `{machine['generated_at']}`",
        "",
        "## Summary",
        "",
        "The governing PA package reproduced cleanly. The local repository does not currently retain an authoritative historical PH/PR/defensive-replacement event ledger or batting-slot replacement chain, so exact substitution taxonomy is not certified.",
        "",
        f"Generic opportunity-loss proxy rows: `{machine['generic_loss_proxy_rows']}`.",
        f"Current-model protected-holdout MAE remained bound at `{machine['current_holdout_mae']}`. The tested proxy substitution adjustment is diagnostic only and is not required before the live expected-PA parent pilot.",
        "",
        "## Decisions",
        "",
    ]
    for r in decisions_rows:
        lines.append(f"- `{r['decision']} = {r['value']}`")
    lines += [
        "",
        "## Direct Answer",
        "",
        "The selected expected-PA model does overpredict rows that look like generic opportunity-loss cases, but the repository does not preserve enough event-level evidence to say those were PH, PR or defensive replacements. Strict-prior role/player/team proxies can be tested, yet they do not provide certified event-specific incremental value. The live expected-PA parent pilot should continue unchanged while a true substitution-event source is designed separately.",
        "",
        "No production routing, model, threshold, selector, DB, network, OddsAPI, ROI or wagering change was made.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def validate(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for p in sorted(out_dir.glob("*")):
        if p.name.startswith("sha256_manifest"):
            continue
        try:
            if p.suffix == ".csv":
                with p.open(newline="", encoding="utf-8") as fh:
                    list(csv.reader(fh))
            elif p.suffix == ".json":
                json.loads(p.read_text(encoding="utf-8"))
            elif p.suffix == ".md":
                assert p.read_text(encoding="utf-8").strip()
            status, notes = "PASS", ""
        except Exception as exc:
            status, notes = "FAIL", str(exc)
        rows.append({"artifact": rel(p), "validation": p.suffix, "status": status, "notes": notes})
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    binding, pa_machine = bind_governing_package()
    raw = load_denominator()
    scored, _ = add_variant_predictions(model_df(raw))
    scored = add_context(scored)
    sources = event_source_audit()
    source_ready = any(s["supports_exact_substitution_event"] and s["available"] for s in sources)
    scored, sub_baselines, ext_contracts_and_comps = extension_variants(scored)
    extension_contracts = [r for r in ext_contracts_and_comps if "delta_mae_vs_current" not in r]
    comparisons = [r for r in ext_contracts_and_comps if "delta_mae_vs_current" in r]
    hitless_scored, hitless_rows = hitless_integration(scored)
    event_rows, ph_rows, pr_rows, dr_rows = event_specific(scored)
    decisions_rows = decisions(source_ready, comparisons, hitless_rows)

    write_csv(OUT_DIR / "governing_pa_experiment_binding.csv", binding)
    write_csv(OUT_DIR / "substitution_event_source_audit.csv", sources)
    write_csv(OUT_DIR / "event_taxonomy.csv", taxonomy())
    write_csv(OUT_DIR / "starter_removal_ledger.csv", starter_removal_ledger(scored))
    write_csv(OUT_DIR / "batting_slot_replacement_chain_ledger.csv", replacement_chain_ledger(scored))
    write_csv(OUT_DIR / "opportunity_loss_calculations.csv", opportunity_loss_calcs(scored))
    write_csv(OUT_DIR / "substitution_source_completeness.csv", source_completeness(scored))
    write_csv(OUT_DIR / "selected_pa_feature_coverage_audit.csv", feature_coverage_audit())
    write_csv(OUT_DIR / "current_model_error_by_substitution_type.csv", error_by_status(scored))
    write_csv(OUT_DIR / "pa_error_decomposition.csv", error_decomposition(scored))
    write_csv(OUT_DIR / "strict_prior_substitution_risk_registry.csv", risk_registry())
    write_csv(OUT_DIR / "transparent_substitution_baselines.csv", sub_baselines)
    write_csv(OUT_DIR / "pa_extension_variants.csv", extension_contracts)
    write_csv(OUT_DIR / "validation_holdout_comparisons.csv", comparisons)
    write_csv(OUT_DIR / "event_specific_performance.csv", event_rows)
    write_csv(OUT_DIR / "ph_analysis.csv", ph_rows)
    write_csv(OUT_DIR / "pr_analysis.csv", pr_rows)
    write_csv(OUT_DIR / "defensive_replacement_analysis.csv", dr_rows)
    write_csv(OUT_DIR / "substitution_adjusted_hitless_integration.csv", hitless_rows)
    write_csv(OUT_DIR / "high_opportunity_removal_explanation_ledger.csv", high_opportunity_ledger(hitless_scored))
    write_csv(OUT_DIR / "unpredictable_event_analysis.csv", unpredictable_events(scored))
    write_csv(OUT_DIR / "live_readiness_matrix.csv", live_readiness())
    write_csv(OUT_DIR / "pilot_readiness_decision.csv", [{"decision": next(r["value"] for r in decisions_rows if r["decision"] == "MLB_HITS05_EXPECTED_PA_PARENT_PILOT_READINESS_DECISION"), "notes": "No live implementation authorized."}])
    write_csv(OUT_DIR / "required_decisions.csv", decisions_rows)

    hold = pd.read_csv(PA_POINT)
    current_hold = hold[hold["period"].eq("protected_holdout") & hold["model"].eq("variant_5_plus_team_opportunity")].iloc[0]
    machine = {
        "generated_at": generated_at,
        "package": rel(OUT_DIR),
        "pa_population_rows": len(scored),
        "source_event_ready": source_ready,
        "generic_loss_proxy_rows": int(scored["opportunity_loss_proxy"].eq("UNKNOWN_REMOVAL_OR_OPPORTUNITY_LOSS_PROXY").sum()),
        "current_holdout_mae": float(current_hold["mae"]),
        "current_holdout_rmse": float(current_hold["rmse"]),
        "event_source_decision": next(r["value"] for r in decisions_rows if r["decision"] == "MLB_HITS05_SUBSTITUTION_EVENT_SOURCE_DECISION"),
        "pilot_readiness_decision": next(r["value"] for r in decisions_rows if r["decision"] == "MLB_HITS05_EXPECTED_PA_PARENT_PILOT_READINESS_DECISION"),
        "direct_answer": "The selected expected-PA model overpredicts generic opportunity-loss proxy rows, but exact PH/PR/defensive replacement evidence is not locally retained. No substitution extension is certified as required before the live expected-PA parent pilot.",
    }
    (OUT_DIR / "machine_readable_hits05_substitution_opportunity_loss.json").write_text(json.dumps(machine, indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(OUT_DIR / "hits05_substitution_opportunity_loss_audit_2026-07-21.md", machine, decisions_rows)

    manifest = []
    for p in sorted(OUT_DIR.glob("*")):
        if p.name in {"sha256_manifest.csv", "validation_report.csv"}:
            continue
        manifest.append({"path": rel(p), "sha256": sha256(p), "bytes": p.stat().st_size})
    write_csv(OUT_DIR / "sha256_manifest.csv", manifest)
    validation = validate(OUT_DIR)
    write_csv(OUT_DIR / "validation_report.csv", validation)
    if any(r["status"] == "FAIL" for r in validation):
        return 1
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
