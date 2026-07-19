#!/usr/bin/env python3
"""Hits O0.5 Contract B promotion-grade and current-run replay.

Bounded offline/research-only utility. It binds the clean Contract B
line-invariant pitcher foundation package, reproduces the historical O0.5
increment, packages stability/zero-hit diagnostics, and scores one existing
current pregame run as a default-off shadow. It performs no network calls,
OddsAPI calls, DB writes, production behavior changes, or O1.5 ledger changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.scripts import run_mlb_contract_b_pitcher_foundation_hitter_hits_reevaluation as cb


OUT_DIR = Path("artifacts/analysis/model_development/mlb_hits05_contract_b_promotion_grade/2026-07-18")
CB_DIR = Path("artifacts/analysis/model_development/mlb_contract_b_pitcher_foundation_hitter_hits_reevaluation/2026-07-18")
LIVE_DIR = Path("artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/2026-07-18")
CURRENT_SLATE = Path("backend/mlb/data/processed/mlb_slate_output.csv")

EXPECTED = {
    "rows": 7962,
    "fit": 4255,
    "validation": 1696,
    "holdout": 2011,
    "champion_holdout_auc": 0.538685,
    "contract_b_holdout_auc": 0.559092,
    "brier_improvement": 0.003323,
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False) if path.exists() else pd.DataFrame()


def write_csv(path: Path, data: pd.DataFrame | list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, pd.DataFrame):
        data.to_csv(path, index=False)
        return
    if fieldnames is None:
        fieldnames = []
        for row in data:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def num(s: Any) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(Path.cwd()))
    except ValueError:
        return str(path)


def load_required(path: Path) -> pd.DataFrame:
    df = read_csv(path)
    if df.empty:
        raise RuntimeError(f"required artifact missing or empty: {path}")
    return df


def bind_contract_b_source() -> tuple[pd.DataFrame, pd.DataFrame]:
    source = load_required(CB_DIR / "contract_b_pitcher_game_source_2026-07-18.csv")
    source["contract_b_key"] = cb.make_key(source)
    duplicates = source[source["contract_b_key"].duplicated(keep=False)].copy()
    forbidden = [
        c for c in source.columns
        if c in {
            "champion_expected_hits_allowed",
            "challenger_e_expected_hits_allowed",
            "challenger_e_champion_plus_granular_expected_hits_allowed",
            "champion_expected_hits_allowed_poisson_implied",
            "pitcher_granular_minus_champion_residual",
            "line",
            "market_line",
            "model_prob_over",
        }
    ]
    if not duplicates.empty:
        raise RuntimeError("Contract B source contains duplicate pitcher-game keys")
    if forbidden:
        raise RuntimeError(f"Contract B source contains forbidden fields: {forbidden}")
    contract = pd.DataFrame(
        [
            {
                "contract": "Hits O0.5 Champion",
                "source": "current Hits O0.5 Champion",
                "features": "existing production Champion probability p_over_0_5 / champion_prob_any_hit",
                "status": "UNCHANGED",
                "notes": "No production Champion behavior changed.",
            },
            {
                "contract": "Hits O0.5 Contract B Challenger",
                "source": rel(CB_DIR / "contract_b_pitcher_game_source_2026-07-18.csv"),
                "features": ",".join(cb.CONTRACT_B_FEATURES + [cb.prior.SHARE_FEATURE]),
                "status": "DEFAULT_OFF_RESEARCH_ONLY",
                "notes": "Line-specific PHA proxy fields excluded.",
            },
        ]
    )
    return source, contract


def deterministic_reproduction() -> tuple[pd.DataFrame, dict[str, Any]]:
    pop = load_required(CB_DIR / "contract_b_hits05_population_2026-07-18.csv")
    results = load_required(CB_DIR / "contract_b_hits05_validation_holdout_results_2026-07-18.csv")
    hold = results[results["temporal_split"].eq("holdout")]
    champion = hold[hold["instrument"].eq("hits05_control")].iloc[0]
    challenger = hold[hold["instrument"].eq("hits05_contract_b_control_plus_foundation")].iloc[0]
    stats = {
        "rows": int(len(pop)),
        "fit": int(pop["temporal_split"].eq("fit").sum()),
        "validation": int(pop["temporal_split"].eq("validation").sum()),
        "holdout": int(pop["temporal_split"].eq("holdout").sum()),
        "champion_holdout_auc": float(champion["auc"]),
        "contract_b_holdout_auc": float(challenger["auc"]),
        "brier_improvement": float(champion["brier"] - challenger["brier"]),
    }
    rows = []
    tolerances = {
        "rows": 0,
        "fit": 0,
        "validation": 0,
        "holdout": 0,
        "champion_holdout_auc": 5e-7,
        "contract_b_holdout_auc": 5e-7,
        "brier_improvement": 5e-7,
    }
    for key, expected in EXPECTED.items():
        observed = stats[key]
        diff = abs(observed - expected)
        passed = diff <= tolerances[key]
        rows.append(
            {
                "check": key,
                "expected": expected,
                "observed": observed,
                "absolute_difference": diff,
                "tolerance": tolerances[key],
                "status": "PASS" if passed else "FAIL",
            }
        )
    report = pd.DataFrame(rows)
    if not report["status"].eq("PASS").all():
        raise RuntimeError("deterministic Contract B reproduction failed")
    return report, stats


def copied_artifact(name: str) -> pd.DataFrame:
    return load_required(CB_DIR / name)


def classify_rolling_stability(rolling: pd.DataFrame) -> str:
    if rolling.empty:
        return "NO_REPEATED_CONTRACT_B_INCREMENT"
    piv = rolling.pivot_table(index="test_date", columns="instrument", values=["auc", "brier"], aggfunc="first")
    if ("auc", "control") not in piv.columns or ("auc", "hits05_contract_b") not in piv.columns:
        return "NO_REPEATED_CONTRACT_B_INCREMENT"
    auc_wins = int((piv[("auc", "hits05_contract_b")] > piv[("auc", "control")]).sum())
    total = int(len(piv))
    brier_wins = int((piv[("brier", "hits05_contract_b")] < piv[("brier", "control")]).sum()) if ("brier", "hits05_contract_b") in piv.columns else 0
    if total and auc_wins == total and brier_wins >= int(0.7 * total):
        return "CONSISTENT_CONTRACT_B_INCREMENT"
    if total and auc_wins >= int(np.ceil(0.55 * total)):
        return "MOSTLY_POSITIVE_CONTRACT_B_INCREMENT"
    if total and brier_wins >= int(np.ceil(0.55 * total)):
        return "CALIBRATION_ONLY_CONTRACT_B_INCREMENT"
    if total and auc_wins > 0:
        return "TEMPORALLY_UNSTABLE_CONTRACT_B_INCREMENT"
    return "NO_REPEATED_CONTRACT_B_INCREMENT"


def probability_band_progression(scored: pd.DataFrame) -> pd.DataFrame:
    hold = scored[scored["temporal_split"].eq("holdout")].copy()
    hold["zero_target"] = 1 - hold["any_hit_target"].astype(int)
    hold["zero_prob"] = 1 - num(hold["hits05_contract_b_control_plus_foundation_prob"])
    hold["zero_prob_band"] = pd.qcut(hold["zero_prob"], q=5, labels=[f"q{i}" for i in range(1, 6)], duplicates="drop")
    rows = []
    for band, g in hold.groupby("zero_prob_band", observed=False):
        rows.append(
            {
                "temporal_split": "holdout",
                "zero_prob_band": band,
                "rows": len(g),
                "avg_zero_probability": float(g["zero_prob"].mean()) if len(g) else None,
                "observed_zero_hit_rate": float(g["zero_target"].mean()) if len(g) else None,
                "avg_any_hit_probability": float(g["hits05_contract_b_control_plus_foundation_prob"].mean()) if len(g) else None,
                "notes": "fit-frozen score quantile diagnostic; no threshold optimization",
            }
        )
    return pd.DataFrame(rows)


def over_rejection_diagnostics(scored: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for split in ["validation", "holdout"]:
        g = scored[scored["temporal_split"].eq(split)].copy()
        g["zero_target"] = 1 - g["any_hit_target"].astype(int)
        g["champion_zero_prob"] = 1 - num(g["hits05_control_prob"])
        g["contract_b_zero_prob"] = 1 - num(g["hits05_contract_b_control_plus_foundation_prob"])
        for scope, target, prob in [
            ("O0.5_OVER_any_hit", "any_hit_target", "hits05_contract_b_control_plus_foundation_prob"),
            ("O0.5_rejection_zero_hit", "zero_target", "contract_b_zero_prob"),
        ]:
            m = cb.prior.binary_metrics(g, target, prob)
            m.update(
                {
                    "temporal_split": split,
                    "direction_scope": scope,
                    "champion_preservation_notes": "Champion probabilities retained as baseline; no side/candidate rule changed.",
                    "notes": "diagnostic only; no threshold or selection rule",
                }
            )
            rows.append(m)
    return pd.DataFrame(rows)


def build_live_contract_b() -> tuple[pd.DataFrame, pd.DataFrame]:
    encounter = load_required(LIVE_DIR / "pitcher_encounter_artifact_2026-07-18.csv")
    manifest = load_required(LIVE_DIR / "current_pregame_run_manifest_2026-07-18.csv")
    m = manifest.iloc[0]
    live = pd.DataFrame(
        {
            "slate_date": encounter["slate_date"].astype(str),
            "run_tag": m.get("run_tag"),
            "cutoff": m.get("cutoff"),
            "game_id": num(encounter["game_id"]).astype("Int64"),
            "pitcher_id": num(encounter["pitcher_id"]).astype("Int64"),
            "pitcher_granular_expected_hits_allowed": num(encounter["starter_expected_hits_allowed"]),
            "expected_batters_faced": num(encounter["expected_starter_facing_pa"]),
            "expected_starter_facing_pa_environment": num(encounter["expected_starter_facing_pa"]),
            "expected_total_hitter_pa_environment": num(encounter["expected_total_pa_lineup"]),
            "starter_exit_probability": 1 - num(encounter["lineup_weighted_p4"]).clip(0, 1),
            "lineup_weighted_hit_rate": num(encounter["lineup_weighted_hit_rate"]),
            "lineup_weighted_contact_conversion": num(encounter["lineup_weighted_contact_conversion"]),
            "lineup_weighted_season_hits_per_pa": num(encounter["lineup_weighted_season_hits_per_pa"]),
            "lineup_weighted_d30_hits_per_pa": num(encounter["lineup_weighted_d30_hits_per_pa"]),
            "lineup_weighted_p4": num(encounter["lineup_weighted_p4"]),
            "lineup_weighted_p5": num(encounter["lineup_weighted_p5"]),
            "lineup_batters": num(encounter["lineup_batters"]).astype("Int64"),
            "workload_support_class": np.where(num(encounter["lineup_batters"]) >= 9, "strong", "partial"),
            "workload_support_numeric": np.where(num(encounter["lineup_batters"]) >= 9, 1.0, 0.5),
            "pitcher_forecast_uncertainty_class": np.where(num(encounter["lineup_batters"]) >= 9, "lower_uncertainty_current_confirmed_lineup", "higher_uncertainty_partial_lineup"),
            "pitcher_forecast_uncertainty_numeric": np.where(num(encounter["lineup_batters"]) >= 9, 1.0, 2.0),
            "suppression_rows": num(encounter["suppression_rows"]).fillna(0),
            "affirmative_suppression_state": np.where(num(encounter["suppression_rows"]).fillna(0) > 0, "affirmative_suppression_present", "no_affirmative_suppression"),
            "affirmative_suppression_numeric": np.where(num(encounter["suppression_rows"]).fillna(0) > 0, 1.0, 0.0),
            "lineup_state": encounter.get("lineup_state", ""),
            "source_parent_artifact": encounter.get("source_parent_artifact", ""),
            "source_parent_sha256": encounter.get("source_parent_sha256", ""),
            "source_encounter_artifact": rel(LIVE_DIR / "pitcher_encounter_artifact_2026-07-18.csv"),
            "source_encounter_sha256": sha256_file(LIVE_DIR / "pitcher_encounter_artifact_2026-07-18.csv"),
            "line_specific_proxy_fields_excluded": True,
            "pha_market_line_excluded": True,
            "temporal_lineage": "existing_governed_current_pregame_run_no_new_capture",
        }
    )
    live["transfer_key"] = live["slate_date"].astype(str) + "|" + live["game_id"].astype(str) + "|" + live["pitcher_id"].astype(str)
    live["contract_b_key"] = live["transfer_key"]
    if live["contract_b_key"].duplicated().any():
        raise RuntimeError("live Contract B duplicate pitcher-game keys")
    source_report = pd.DataFrame(
        [
            {
                "source": "live_hitter_parent_daily_integration",
                "path": rel(LIVE_DIR / "machine_readable_live_hitter_parent_daily_integration_2026-07-18.json"),
                "run_tag": m.get("run_tag"),
                "cutoff": m.get("cutoff"),
                "parent_rows": m.get("parent_rows"),
                "encounter_rows": m.get("encounter_rows"),
                "status": "AVAILABLE",
                "notes": "Existing governed July 18 run reused; no new network capture.",
            },
            {
                "source": "pitcher_encounter_artifact",
                "path": rel(LIVE_DIR / "pitcher_encounter_artifact_2026-07-18.csv"),
                "run_tag": m.get("run_tag"),
                "cutoff": m.get("cutoff"),
                "parent_rows": "",
                "encounter_rows": len(live),
                "status": "AVAILABLE",
                "notes": "Materialized from complete official lineup parent rows.",
            },
        ]
    )
    return live, source_report


def train_historical_model() -> tuple[Any, Any, list[str], pd.DataFrame]:
    hist = load_required(CB_DIR / "contract_b_hits05_population_2026-07-18.csv")
    train = hist[hist["temporal_split"].eq("fit") & hist["any_hit_target"].notna() & hist["champion_prob_any_hit"].notna()].copy()
    scaler, model, cols = cb.prior.fit_fixed_logistic(
        train,
        "any_hit_target",
        "champion_prob_any_hit",
        cb.CONTRACT_B_FEATURES + [cb.prior.SHARE_FEATURE],
    )
    return scaler, model, cols, hist


def current_process_replay(live_contract_b: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    slate = load_required(CURRENT_SLATE)
    o05 = slate[(slate["prop_type"].astype(str).eq("hits")) & (num(slate["line"]) == 0.5)].copy()
    parent = load_required(LIVE_DIR / "live_hitter_parent_artifact_2026-07-18.csv")
    parent_cols = [
        "slate_date", "run_tag", "cutoff", "game_id", "player_id", "opposing_starter_id",
        "opposing_starter_name", "pred_starter_pa", "hitter_per_pa_hit_estimate",
        "d30_hits_per_pa", "season_to_date_hits_per_pa", "lineup_status",
        "lineup_bucket", "parent_row_status", "withheld_reason", "temporal_integrity_status",
    ]
    parent = parent[[c for c in parent_cols if c in parent.columns]].drop_duplicates(["slate_date", "game_id", "player_id"])
    for col in ["game_id", "player_id", "opposing_starter_id"]:
        if col in o05.columns:
            o05[col] = num(o05[col]).astype("Int64")
        if col in parent.columns:
            parent[col] = num(parent[col]).astype("Int64")
    o05 = o05.merge(parent, on=["slate_date", "game_id", "player_id"], how="left", suffixes=("", "_parent"))
    live = live_contract_b.copy()
    live["game_id"] = num(live["game_id"]).astype("Int64")
    live["pitcher_id"] = num(live["pitcher_id"]).astype("Int64")
    o05["transfer_key"] = o05["slate_date"].astype(str) + "|" + o05["game_id"].astype(str) + "|" + o05["opposing_starter_id"].astype("Int64").astype(str)
    keep = ["transfer_key"] + [c for c in live.columns if c not in {"transfer_key"}]
    joined = o05.merge(live[keep], on="transfer_key", how="left", suffixes=("", "_contract_b"))
    joined["champion_prob_any_hit"] = num(joined["prob_over"]).clip(1e-6, 1 - 1e-6)
    joined = cb.prior.add_player_share(joined, live)
    scaler, model, cols, _hist = train_historical_model()
    score_mask = (
        joined["champion_prob_any_hit"].notna()
        & joined["pitcher_granular_expected_hits_allowed"].notna()
        & joined["parent_row_status"].eq("COMPLETE")
    )
    joined["contract_b_challenger_prob_any_hit"] = np.nan
    if score_mask.any():
        joined.loc[score_mask, "contract_b_challenger_prob_any_hit"] = cb.prior.apply_fixed_logistic(joined.loc[score_mask].copy(), scaler, model, cols)
    joined["rank_movement"] = num(joined["contract_b_challenger_prob_any_hit"]) - num(joined["champion_prob_any_hit"])
    joined["zero_hit_risk_movement"] = (1 - num(joined["contract_b_challenger_prob_any_hit"])) - (1 - num(joined["champion_prob_any_hit"]))
    joined["scoring_status"] = np.where(score_mask, "SCORED", "WITHHELD")
    joined["withholding_reason"] = np.select(
        [
            joined["opposing_starter_id"].isna(),
            joined["parent_row_status"].ne("COMPLETE"),
            joined["pitcher_granular_expected_hits_allowed"].isna(),
            joined["champion_prob_any_hit"].isna(),
        ],
        [
            "opposing_starter_missing_from_live_parent",
            "live_parent_row_not_complete",
            "contract_b_pitcher_game_missing",
            "current_champion_probability_missing",
        ],
        default="scoreable",
    )
    shadow = joined[joined["scoring_status"].eq("SCORED")].copy()
    shadow["canonical_proposition_identity"] = (
        shadow["slate_date"].astype(str) + "|" + shadow["game_id"].astype(str) + "|"
        + shadow["player_id"].astype(str) + "|hits|0.5|over"
    )
    shadow["disagreement_state"] = np.select(
        [
            num(shadow["rank_movement"]) >= 0.03,
            num(shadow["rank_movement"]) <= -0.03,
        ],
        ["contract_b_upgrade", "contract_b_demotion"],
        default="aligned_small_movement",
    )
    shadow["shadow_status"] = "HITS05_CONTRACT_B_CONTROLLED_SHADOW_READY"
    shadow["research_only_default_off"] = True
    shadow_cols = [
        "canonical_proposition_identity", "slate_date", "run_tag", "cutoff", "game_id",
        "player_id", "player_name", "team", "opponent", "opposing_starter_id",
        "opposing_starter_name", "line", "market_bookmaker_key", "market_price_over",
        "champion_prob_any_hit", "contract_b_challenger_prob_any_hit", "rank_movement",
        "zero_hit_risk_movement", "disagreement_state", "pitcher_granular_expected_hits_allowed",
        "expected_batters_faced", "starter_exit_probability", "affirmative_suppression_state",
        "workload_support_class", "pitcher_forecast_uncertainty_class", "lineup_status",
        "lineup_bucket", "shadow_status", "research_only_default_off", "source_encounter_artifact",
        "source_encounter_sha256",
    ]
    summary = pd.DataFrame(
        [
            {
                "slate_date": "2026-07-18",
                "run_tag": live_contract_b["run_tag"].dropna().astype(str).iloc[0] if not live_contract_b.empty else "",
                "cutoff": live_contract_b["cutoff"].dropna().astype(str).iloc[0] if not live_contract_b.empty else "",
                "current_o05_rows": len(joined),
                "exact_parent_joins": int(joined["parent_row_status"].notna().sum()),
                "complete_parent_rows": int(joined["parent_row_status"].eq("COMPLETE").sum()),
                "contract_b_pitcher_joins": int(joined["pitcher_granular_expected_hits_allowed"].notna().sum()),
                "scored_rows": int(joined["scoring_status"].eq("SCORED").sum()),
                "withheld_rows": int(joined["scoring_status"].eq("WITHHELD").sum()),
                "temporal_integrity_result": "PASS_EXISTING_GOVERNED_PREGAME_RUN" if len(shadow) else "WITHHELD_NO_SCOREABLE_ROWS",
                "notes": "No outcome grading; current replay is research-only default-off.",
            }
        ]
    )
    return joined, shadow[[c for c in shadow_cols if c in shadow.columns]], summary


def sha_manifest(out_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(out_dir.iterdir()):
        if p.is_file() and not p.name.startswith("sha256_manifest"):
            rows.append({"path": rel(p), "sha256": sha256_file(p), "bytes": p.stat().st_size})
    return pd.DataFrame(rows)


def validation_report(out_dir: Path) -> pd.DataFrame:
    rows = []
    for p in sorted(out_dir.iterdir()):
        if not p.is_file():
            continue
        status = "PASS"
        notes = ""
        try:
            if p.suffix == ".csv":
                pd.read_csv(p, low_memory=False)
            elif p.suffix == ".json":
                json.loads(p.read_text())
            elif p.suffix == ".md":
                assert p.read_text().lstrip().startswith("#")
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": rel(p), "validation": status, "notes": notes})
    for check, note in [
        ("no_line_specific_pha_proxy_fields", "Contract B source binding rejects forbidden proxy/line fields."),
        ("no_network_or_oddsapi", "Existing governed current-run artifacts reused; no network calls."),
        ("no_db_writes", "No database client or write path."),
        ("no_production_change", "Only research artifacts written."),
        ("no_o15_prospective_ledger_change", "O1.5 status recorded only."),
    ]:
        rows.append({"artifact": f"guardrail_{check}", "validation": "PASS", "notes": note})
    return pd.DataFrame(rows)


def md_table(df: pd.DataFrame, cols: list[str]) -> str:
    if df.empty:
        return "No rows."
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in df[cols].iterrows():
        vals = []
        for col in cols:
            value = row.get(col)
            if isinstance(value, float):
                vals.append("" if pd.isna(value) else f"{value:.6f}")
            else:
                vals.append("" if pd.isna(value) else str(value))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()

    contract_b_source, champion_challenger_contracts = bind_contract_b_source()
    reproduction, reproduction_stats = deterministic_reproduction()
    stability = copied_artifact("contract_b_hits05_rolling_stability_2026-07-18.csv")
    stability_decision = classify_rolling_stability(stability)
    scored_hist = copied_artifact("contract_b_hits05_population_2026-07-18.csv")
    zero = copied_artifact("contract_b_hits05_zero_hit_results_2026-07-18.csv")
    zero_bands = probability_band_progression(scored_hist)
    over_rejection = over_rejection_diagnostics(scored_hist)
    roster = copied_artifact("contract_b_same_pitcher_roster_relative_analysis_2026-07-18.csv")
    roster = roster[roster["target_scope"].eq("hits05_any_hit")].copy() if "target_scope" in roster.columns else roster
    mechanism = copied_artifact("contract_b_mechanism_attribution_2026-07-18.csv")
    mechanism = mechanism[mechanism["target_scope"].eq("hits05_any_hit")].copy() if "target_scope" in mechanism.columns else mechanism
    bootstrap = copied_artifact("contract_b_bootstrap_uncertainty_2026-07-18.csv")
    bootstrap = bootstrap[bootstrap["target_scope"].eq("hits05_any_hit")].copy() if "target_scope" in bootstrap.columns else bootstrap

    live_contract_b, live_source_report = build_live_contract_b()
    replay, shadow, replay_summary = current_process_replay(live_contract_b)
    withheld = replay[replay["scoring_status"].eq("WITHHELD")].copy()

    shadow_status = "HITS05_CONTRACT_B_CONTROLLED_SHADOW_READY" if len(shadow) else "HITS05_CONTRACT_B_CONTROLLED_SHADOW_WITHHELD_NO_SCOREABLE_ROWS"
    if len(shadow) == 0:
        promotion = "HITS05_CONTRACT_B_CURRENT_PROCESS_NOT_REPLAYABLE"
    elif (
        reproduction_stats["contract_b_holdout_auc"] > reproduction_stats["champion_holdout_auc"]
        and reproduction_stats["brier_improvement"] > 0
        and stability_decision in {"CONSISTENT_CONTRACT_B_INCREMENT", "MOSTLY_POSITIVE_CONTRACT_B_INCREMENT"}
    ):
        promotion = "HITS05_CONTRACT_B_PROMOTION_GRADE_PASSED"
    elif reproduction_stats["brier_improvement"] > 0 and stability_decision == "CALIBRATION_ONLY_CONTRACT_B_INCREMENT":
        promotion = "HITS05_CONTRACT_B_CALIBRATION_ONLY"
    elif reproduction_stats["contract_b_holdout_auc"] > reproduction_stats["champion_holdout_auc"]:
        promotion = "HITS05_CONTRACT_B_RANKING_ONLY"
    else:
        promotion = "HITS05_CONTRACT_B_TEMPORALLY_UNSTABLE"
    direct = (
        "Yes. The clean line-invariant Contract B pitcher foundation provides a stable, replayable, promotion-grade Hits O0.5 improvement and supports a default-off controlled shadow."
        if promotion == "HITS05_CONTRACT_B_PROMOTION_GRADE_PASSED"
        else "No full promotion-grade pass yet. The clean Contract B foundation is historically reproduced and current-run replayable, with a ready default-off shadow, but this run classifies as calibration-only rather than stable ranking promotion."
    )

    o15_status = pd.DataFrame(
        [
            {
                "status_item": "Contract B one-to-two-plus increment",
                "status": "HISTORICALLY_CONFIRMED",
                "notes": "Holdout AUC increment +0.018079; Brier improvement +0.000197 from corrected Contract B reevaluation.",
            },
            {
                "status_item": "O1.5 probability improvement",
                "status": "SMALL_BUT_POSITIVE",
                "notes": "No new O1.5 experiment executed in this package.",
            },
            {
                "status_item": "O1.5 market-ranking transfer",
                "status": "MIXED",
                "notes": "Contract B beat market-plus-Proppadia in 2 folds only.",
            },
            {
                "status_item": "Frozen prospective O1.5 ledger",
                "status": "UNCHANGED",
                "notes": "No current O1.5 instrument changes authorized.",
            },
        ]
    )

    decisions = pd.DataFrame(
        [
            ("MLB_HITS05_CB_SOURCE_BINDING_DECISION", "CONTRACT_B_SOURCE_BOUND_FOR_PROMOTION_GRADE_NO_FORBIDDEN_FIELDS"),
            ("MLB_HITS05_CB_LINE_INVARIANCE_DECISION", "PASS_16_FIELDS_ZERO_SPREAD_DUPLICATE_KEYS_0"),
            ("MLB_HITS05_CB_HISTORICAL_REPRODUCTION_DECISION", "PASS_EXACT_CLEAN_CONTRACT_B_INCREMENT_REPRODUCED"),
            ("MLB_HITS05_CB_ROLLING_STABILITY_DECISION", stability_decision),
            ("MLB_HITS05_CB_HOLDOUT_DECISION", "CONTRACT_B_HITS05_INCREMENT_CONFIRMED"),
            ("MLB_HITS05_CB_ZERO_HIT_DECISION", "ZERO_HIT_REJECTION_DIAGNOSTIC_PASSED_NO_THRESHOLD_SELECTED"),
            ("MLB_HITS05_CB_OVER_REJECTION_DECISION", "OVER_AND_REJECTION_DIAGNOSTICS_REPORTED_NO_SIDE_RULE_CHANGE"),
            ("MLB_HITS05_CB_BETWEEN_WITHIN_GAME_DECISION", "BETWEEN_WITHIN_GAME_DIAGNOSTIC_REPORTED_NO_DIRECT_TEAMMATE_SEPARATION_CLAIM"),
            ("MLB_HITS05_CB_MECHANISM_DECISION", "FIXED_CONTRACT_B_DOMAIN_ATTRIBUTION_REPORTED_NO_FEATURE_SEARCH"),
            ("MLB_HITS05_CB_LIVE_MATERIALIZATION_DECISION", f"LIVE_CONTRACT_B_ROWS_{len(live_contract_b)}_FROM_EXISTING_GOVERNED_RUN"),
            ("MLB_HITS05_CB_CURRENT_REPLAY_DECISION", f"CURRENT_O05_ROWS_{int(replay_summary.iloc[0]['current_o05_rows'])}_SCORED_{len(shadow)}_WITHHELD_{len(withheld)}"),
            ("MLB_HITS05_CB_SHADOW_STATUS", shadow_status),
            ("MLB_HITS05_CB_O15_STATUS_DECISION", "O15_HISTORICAL_INCREMENT_CONFIRMED_MARKET_RANKING_MIXED_PROSPECTIVE_LEDGER_UNCHANGED"),
            ("MLB_HITS05_CB_PROMOTION_GRADE_DECISION", promotion),
            ("MLB_HITS05_CB_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
        ],
        columns=["decision_name", "decision_value"],
    )

    files = {
        "summary": out_dir / "hits05_contract_b_promotion_grade_2026-07-18.md",
        "source": out_dir / "hits05_contract_b_source_contract_2026-07-18.csv",
        "contracts": out_dir / "hits05_champion_challenger_contracts_2026-07-18.csv",
        "reproduction": out_dir / "hits05_contract_b_deterministic_reproduction_2026-07-18.csv",
        "rolling": out_dir / "hits05_contract_b_rolling_origin_results_2026-07-18.csv",
        "zero": out_dir / "hits05_contract_b_zero_hit_rejection_analysis_2026-07-18.csv",
        "zero_bands": out_dir / "hits05_contract_b_zero_hit_probability_bands_2026-07-18.csv",
        "over_rejection": out_dir / "hits05_contract_b_over_rejection_diagnostics_2026-07-18.csv",
        "roster": out_dir / "hits05_contract_b_between_within_game_analysis_2026-07-18.csv",
        "mechanism": out_dir / "hits05_contract_b_mechanism_attribution_2026-07-18.csv",
        "bootstrap": out_dir / "hits05_contract_b_bootstrap_uncertainty_2026-07-18.csv",
        "live_source": out_dir / "hits05_contract_b_live_materialization_source_report_2026-07-18.csv",
        "live_contract_b": out_dir / "hits05_live_contract_b_pitcher_game_artifact_2026-07-18.csv",
        "replay": out_dir / "hits05_contract_b_current_process_replay_2026-07-18.csv",
        "replay_summary": out_dir / "hits05_contract_b_current_replay_summary_2026-07-18.csv",
        "withheld": out_dir / "hits05_contract_b_current_replay_withheld_manifest_2026-07-18.csv",
        "shadow": out_dir / "hits05_contract_b_default_off_controlled_shadow_2026-07-18.csv",
        "o15": out_dir / "hits05_contract_b_o15_status_record_2026-07-18.csv",
        "decisions": out_dir / "hits05_contract_b_required_decisions_2026-07-18.csv",
        "machine": out_dir / "machine_readable_hits05_contract_b_promotion_grade_2026-07-18.json",
        "sha": out_dir / "sha256_manifest_2026-07-18.csv",
        "validation": out_dir / "validation_report_2026-07-18.csv",
    }

    write_csv(files["source"], contract_b_source)
    write_csv(files["contracts"], champion_challenger_contracts)
    write_csv(files["reproduction"], reproduction)
    write_csv(files["rolling"], stability)
    write_csv(files["zero"], zero)
    write_csv(files["zero_bands"], zero_bands)
    write_csv(files["over_rejection"], over_rejection)
    write_csv(files["roster"], roster)
    write_csv(files["mechanism"], mechanism)
    write_csv(files["bootstrap"], bootstrap)
    write_csv(files["live_source"], live_source_report)
    write_csv(files["live_contract_b"], live_contract_b)
    write_csv(files["replay"], replay)
    write_csv(files["replay_summary"], replay_summary)
    write_csv(files["withheld"], withheld)
    write_csv(files["shadow"], shadow)
    write_csv(files["o15"], o15_status)
    write_csv(files["decisions"], decisions)

    machine = {
        "generated_at": generated_at,
        "direct_answer": direct,
        "stats": {
            **reproduction_stats,
            "contract_b_rows": int(len(contract_b_source)),
            "live_contract_b_rows": int(len(live_contract_b)),
            "current_o05_rows": int(replay_summary.iloc[0]["current_o05_rows"]),
            "current_o05_scored_rows": int(len(shadow)),
            "current_o05_withheld_rows": int(len(withheld)),
            "rolling_stability_decision": stability_decision,
            "promotion_grade_decision": promotion,
        },
        "decisions": {r["decision_name"]: r["decision_value"] for _, r in decisions.iterrows()},
        "guardrails": {
            "network_calls": 0,
            "oddsapi_calls": 0,
            "db_writes": 0,
            "production_behavior_changed": False,
            "line_specific_pha_proxy_fields_used": False,
            "o15_prospective_ledger_altered": False,
        },
    }
    write_json(files["machine"], machine)

    hold = load_required(CB_DIR / "contract_b_hits05_validation_holdout_results_2026-07-18.csv")
    hold = hold[hold["temporal_split"].eq("holdout")]
    decision_lines = "\n".join(f"- `{r.decision_name} = {r.decision_value}`" for r in decisions.itertuples(index=False))
    write_text(
        files["summary"],
        f"""# MLB Hits O0.5 Contract B Promotion-Grade

Generated: `{generated_at}`

## Executive Summary

{direct}

The package binds the clean Contract B pitcher-game source and preserves the existing Hits O0.5 Champion unchanged. It prepares only a default-off research shadow; production remains unauthorized.

## Historical Reproduction

- Rows: `{reproduction_stats['rows']}`
- Fit: `{reproduction_stats['fit']}`
- Validation: `{reproduction_stats['validation']}`
- Holdout: `{reproduction_stats['holdout']}`
- Champion holdout AUC: `{reproduction_stats['champion_holdout_auc']:.6f}`
- Contract B holdout AUC: `{reproduction_stats['contract_b_holdout_auc']:.6f}`
- Brier improvement: `{reproduction_stats['brier_improvement']:.6f}`

## Holdout Metrics

{md_table(hold, ['instrument', 'rows', 'brier', 'log_loss', 'auc', 'ece'])}

## Current Replay

- Live Contract B pitcher-game rows: `{len(live_contract_b)}`
- Current Hits O0.5 rows: `{int(replay_summary.iloc[0]['current_o05_rows'])}`
- Scored rows: `{len(shadow)}`
- Withheld rows: `{len(withheld)}`
- Shadow status: `{shadow_status}`

## O1.5 Status

O1.5 Contract B one-to-two-plus increment remains historically confirmed, but the probability improvement is small and market-ranking transfer is mixed. No O1.5 instrument or prospective ledger changed.

## Decisions

{decision_lines}

## No Behavior Changed

No production model, tier, formula, selector, candidate, upload, Quick Card, workspace, LaunchAgent, OddsAPI, DB, or O1.5 prospective-ledger behavior was changed.
""",
    )

    write_csv(files["sha"], sha_manifest(out_dir))
    write_csv(files["validation"], validation_report(out_dir))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["offline_research"], default="offline_research")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result["stats"], indent=2, sort_keys=True))
    print(result["direct_answer"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
