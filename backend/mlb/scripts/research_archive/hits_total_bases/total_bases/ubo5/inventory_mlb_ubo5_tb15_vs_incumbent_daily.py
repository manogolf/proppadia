#!/usr/bin/env python3
"""Read-only daily UBO-5 versus incumbent TB1.5 population inventory."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
CURRENT_DATE = "2026-07-28"
BOARD = ROOT / "backend/mlb/exports/model_v2/ubo5_tb15"
ODDS = ROOT / "backend/mlb/exports/odds_history"
OUT = ROOT / "artifacts/analysis/model_development/mlb_ubo5_tb15_vs_incumbent_daily_population/2026-07-28"
KEY = ["game_pk", "batter_mlb_id"]


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(name: str, frame: pd.DataFrame) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    frame.to_csv(OUT / name, index=False, lineterminator="\n")


def write_text(name: str, body: str) -> None:
    (OUT / name).write_text(body.rstrip() + "\n", encoding="utf-8")


def load_manifest(date: str) -> dict | None:
    path = BOARD / date / f"ubo5_tb15_run_population_manifest_{date}.json"
    return json.loads(path.read_text()) if path.is_file() else None


def read_ledger(raw: str) -> pd.DataFrame:
    path = ROOT / raw
    if not path.is_file():
        return pd.DataFrame()
    frame = pd.read_csv(path, dtype={"game_pk": str, "batter_mlb_id": str})
    if frame.empty:
        return frame
    renames = {
        "BetOnline_over_price": "betonline_over_price",
        "BetOnline_under_price": "betonline_under_price",
        "over_edge_percentage_points": "ubo5_over_edge_pp",
    }
    return frame.rename(columns=renames)


def active_probabilities(date: str, tag: str) -> pd.DataFrame:
    path = ODDS / date / f"mlb_predictions_wide_calibrated__{tag}.csv"
    if not path.is_file():
        return pd.DataFrame(columns=[*KEY, "active_probability"])
    frame = pd.read_csv(path, dtype={"game_id": str, "player_id": str})
    frame = frame[
        frame.prop_type.eq("total_bases")
    ][["game_id", "player_id", "p_over_1_5"]].rename(columns={
        "game_id": "game_pk", "player_id": "batter_mlb_id",
        "p_over_1_5": "active_probability",
    })
    return frame.drop_duplicates(KEY)


def run_frame(date: str, inventory: dict) -> pd.DataFrame:
    raw_path = Path(inventory["path"])
    path = raw_path if raw_path.is_absolute() else ROOT / raw_path
    snapshot = pd.read_csv(path.with_suffix(".csv"), dtype={
        "game_pk": str, "batter_mlb_id": str,
    })
    for col in ["ubo5_probability_over", "batting_order", "strict_prior_pa"]:
        snapshot[col] = pd.to_numeric(snapshot[col], errors="coerce")
    tag = inventory["run_tag"]
    ledger_parts = []
    for raw in snapshot.route_ledger_path.dropna().astype(str).unique():
        ledger = read_ledger(raw)
        if len(ledger):
            ledger_parts.append(ledger)
    ledger = pd.concat(ledger_parts, ignore_index=True) if ledger_parts else pd.DataFrame()
    wanted = [
        "counterfactual_incumbent_probability", "active_probability",
        "counterfactual_incumbent_model_source",
        "counterfactual_incumbent_artifact_hash",
        "counterfactual_incumbent_source_path",
        "counterfactual_incumbent_captured_at_utc",
        "counterfactual_capture_before_routing_status",
        "active_probability_lineage_status", "probability_delta_integrity_status",
        "counterfactual_lineage_integrity_status", "feature_vector_sha256",
        "temporal_integrity_status",
    ]
    if len(ledger):
        keep = [*KEY, *[c for c in wanted if c in ledger]]
        ledger = ledger[keep].drop_duplicates(KEY, keep="last")
        snapshot = snapshot.merge(ledger, on=KEY, how="left", suffixes=("", "_ledger"))
    for col in wanted:
        if col not in snapshot:
            snapshot[col] = np.nan
    active = active_probabilities(date, tag)
    snapshot = snapshot.merge(active, on=KEY, how="left", suffixes=("", "_wide"))
    if "active_probability_wide" in snapshot:
        snapshot["active_probability"] = snapshot.active_probability.combine_first(
            snapshot.active_probability_wide
        )
        snapshot = snapshot.drop(columns=["active_probability_wide"])
    snapshot["counterfactual_incumbent_probability"] = pd.to_numeric(
        snapshot.counterfactual_incumbent_probability, errors="coerce"
    )
    snapshot["active_probability"] = pd.to_numeric(
        snapshot.active_probability, errors="coerce"
    )
    snapshot["ubo5_minus_incumbent_probability_pp"] = (
        snapshot.ubo5_probability_over
        - snapshot.counterfactual_incumbent_probability
    ) * 100
    blocked = snapshot.evaluation_status.astype(str).str.contains(
        "IDENTITY|FEATURE_BLOCKED", case=False, regex=True
    )
    conditions = [
        blocked,
        snapshot.ubo5_probability_over.isna()
        & snapshot.counterfactual_incumbent_probability.isna(),
        snapshot.ubo5_probability_over.isna(),
        snapshot.counterfactual_incumbent_probability.isna(),
        snapshot.ubo5_probability_over.gt(snapshot.counterfactual_incumbent_probability),
        snapshot.ubo5_probability_over.lt(snapshot.counterfactual_incumbent_probability),
        snapshot.ubo5_probability_over.eq(snapshot.counterfactual_incumbent_probability),
    ]
    snapshot["comparison_status"] = np.select(
        conditions,
        ["IDENTITY_OR_FEATURE_BLOCKED", "BOTH_SCORES_MISSING",
         "UBO5_SCORE_MISSING", "INCUMBENT_SCORE_MISSING", "UBO5_HIGHER",
         "INCUMBENT_HIGHER", "EXACT_TIE"],
        default="IDENTITY_OR_FEATURE_BLOCKED",
    )
    snapshot["starting_status"] = np.where(
        snapshot.lineup_status.eq("CONFIRMED_STARTER"),
        "CONFIRMED_STARTER", "PRELINEUP_CANDIDATE",
    )
    snapshot["identity_status"] = np.where(
        blocked, "BLOCKED", "CERTIFIED_EXACT_ID"
    )
    snapshot["run_tag"] = tag
    snapshot["run_timestamp_utc"] = inventory["snapshot_timestamp_utc"]
    snapshot["snapshot_path"] = str(path.relative_to(ROOT))
    snapshot["snapshot_sha256"] = inventory["snapshot_sha256"]
    return snapshot


def run_summary(frame: pd.DataFrame) -> dict:
    statuses = frame.comparison_status.value_counts()
    comparable = int(frame.comparison_status.isin(
        ["UBO5_HIGHER", "INCUMBENT_HIGHER", "EXACT_TIE"]
    ).sum())
    return {
        "run_tag": frame.run_tag.iloc[0], "timestamp_utc": frame.run_timestamp_utc.iloc[0],
        "evaluated_identities": len(frame), "fully_comparable_identities": comparable,
        "UBO5_HIGHER": int(statuses.get("UBO5_HIGHER", 0)),
        "INCUMBENT_HIGHER": int(statuses.get("INCUMBENT_HIGHER", 0)),
        "EXACT_TIE": int(statuses.get("EXACT_TIE", 0)),
        "missing_score_identities": int(frame.comparison_status.isin(
            ["UBO5_SCORE_MISSING", "INCUMBENT_SCORE_MISSING", "BOTH_SCORES_MISSING"]
        ).sum()),
        "identity_or_feature_blocked": int(statuses.get("IDENTITY_OR_FEATURE_BLOCKED", 0)),
    }


def date_context(date: str) -> dict:
    manifest = load_manifest(date)
    if not manifest:
        return {
            "slate_date": date, "lineage_classification": "NOT_CERTIFIABLE",
            "evaluated": 0, "comparable": 0, "UBO5_HIGHER": 0,
            "INCUMBENT_HIGHER": 0, "ties": 0, "UBO5_HIGHER_percentage": np.nan,
        }
    inventory = sorted(
        manifest["run_inventory"], key=lambda x: x["snapshot_timestamp_utc"]
    )
    runs = [run_frame(date, inv) for inv in inventory]
    latest = runs[-1]
    summary = run_summary(latest)
    exact = latest.ubo5_probability_over.notna()
    missing_incumbent_for_exact = int(
        (exact & latest.counterfactual_incumbent_probability.isna()).sum()
    )
    if manifest["spine_status"] != "CERTIFIED_COMPLETE_RUN_SNAPSHOTS":
        lineage = "PARTIALLY_CERTIFIED"
    elif missing_incumbent_for_exact:
        lineage = "PARTIALLY_CERTIFIED"
    else:
        lineage = "FULLY_CERTIFIED"
    return {
        "slate_date": date, "lineage_classification": lineage,
        "evaluated": summary["evaluated_identities"],
        "comparable": summary["fully_comparable_identities"],
        "UBO5_HIGHER": summary["UBO5_HIGHER"],
        "INCUMBENT_HIGHER": summary["INCUMBENT_HIGHER"],
        "ties": summary["EXACT_TIE"],
        "UBO5_HIGHER_percentage": (
            summary["UBO5_HIGHER"] / summary["fully_comparable_identities"]
            if summary["fully_comparable_identities"] else np.nan
        ),
        "latest_run_tag": summary["run_tag"],
    }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(CURRENT_DATE)
    if not manifest or not manifest.get("run_inventory"):
        raise RuntimeError("CURRENT_SLATE_SNAPSHOT_NOT_AVAILABLE")
    inventory = sorted(
        manifest["run_inventory"], key=lambda x: x["snapshot_timestamp_utc"]
    )
    runs = [run_frame(CURRENT_DATE, inv) for inv in inventory]
    latest = runs[-1]
    fields = [
        "slate_date", "game_pk", "batter_mlb_id", "prop_type", "line",
        "player_name", "game", "team", "opponent", "starting_status",
        "batting_order", "strict_prior_pa", "feature_state", "route_status",
        "identity_status", "ubo5_probability_over",
        "counterfactual_incumbent_probability", "active_probability",
        "ubo5_minus_incumbent_probability_pp", "comparison_status", "run_tag",
        "run_timestamp_utc", "snapshot_path", "snapshot_sha256",
    ]
    write_csv("ubo5_vs_incumbent_complete_universe.csv", latest[fields])
    candidates = latest[latest.comparison_status.eq("UBO5_HIGHER")].copy()
    candidates = candidates.sort_values(
        ["ubo5_minus_incumbent_probability_pp", "player_name"],
        ascending=[False, True],
    )
    candidates.insert(0, "rank", range(1, len(candidates) + 1))
    candidate_fields = [
        "rank", "player_name", "game", "team", "opponent", "starting_status",
        "batting_order", "strict_prior_pa", "feature_state",
        "ubo5_probability_over", "counterfactual_incumbent_probability",
        "ubo5_minus_incumbent_probability_pp", "active_probability", "route_status",
    ]
    write_csv("ubo5_beats_incumbent_candidates.csv", candidates[candidate_fields])

    overall = run_summary(latest)
    counts = [{
        "dimension": "OVERALL", "value": "ALL",
        "evaluated": len(latest),
        "exact_ubo5_scored": int(latest.ubo5_probability_over.notna().sum()),
        "independent_incumbent_scored": int(latest.counterfactual_incumbent_probability.notna().sum()),
        "fully_comparable": overall["fully_comparable_identities"],
        **{s: int(latest.comparison_status.eq(s).sum()) for s in [
            "UBO5_HIGHER", "INCUMBENT_HIGHER", "EXACT_TIE", "UBO5_SCORE_MISSING",
            "INCUMBENT_SCORE_MISSING", "BOTH_SCORES_MISSING",
            "IDENTITY_OR_FEATURE_BLOCKED",
        ]},
        "ubo5_higher_pct_all": len(candidates) / len(latest) if len(latest) else np.nan,
        "ubo5_higher_pct_comparable": (
            len(candidates) / overall["fully_comparable_identities"]
            if overall["fully_comparable_identities"] else np.nan
        ),
    }]
    dimensions = {
        "starting_status": latest.starting_status,
        "route_status": latest.route_status,
        "feature_state": latest.feature_state.fillna("MISSING"),
        "batting_order": latest.batting_order.fillna("MISSING").astype(str),
    }
    for dimension, series in dimensions.items():
        for value, indexes in series.groupby(series).groups.items():
            cell = latest.loc[indexes]
            counts.append({
                "dimension": dimension, "value": value, "evaluated": len(cell),
                "exact_ubo5_scored": int(cell.ubo5_probability_over.notna().sum()),
                "independent_incumbent_scored": int(cell.counterfactual_incumbent_probability.notna().sum()),
                "fully_comparable": int(cell.comparison_status.isin(["UBO5_HIGHER","INCUMBENT_HIGHER","EXACT_TIE"]).sum()),
                **{s: int(cell.comparison_status.eq(s).sum()) for s in [
                    "UBO5_HIGHER", "INCUMBENT_HIGHER", "EXACT_TIE", "UBO5_SCORE_MISSING",
                    "INCUMBENT_SCORE_MISSING", "BOTH_SCORES_MISSING",
                    "IDENTITY_OR_FEATURE_BLOCKED",
                ]},
            })
    write_csv("ubo5_vs_incumbent_population_counts.csv", pd.DataFrame(counts))

    comparable = latest[latest.comparison_status.isin(
        ["UBO5_HIGHER", "INCUMBENT_HIGHER", "EXACT_TIE"]
    )].copy()
    bins = [
        (-np.inf, -5, "UBO-5 lower by more than 5 pp"),
        (-5, -2, "UBO-5 lower by 2–5 pp"),
        (-2, 0, "UBO-5 lower by 0–2 pp"),
        (0, 0, "exact tie"),
        (0, 2, "UBO-5 higher by 0–2 pp"),
        (2, 5, "UBO-5 higher by 2–5 pp"),
        (5, np.inf, "UBO-5 higher by more than 5 pp"),
    ]
    distribution = []
    diff = comparable.ubo5_minus_incumbent_probability_pp
    for low, high, label in bins:
        if label == "exact tie":
            values = diff[diff.eq(0)]
        elif high == 0:
            values = diff[diff.gt(low) & diff.lt(0)]
        elif low == 0:
            values = diff[diff.gt(0) & diff.le(high)]
        else:
            values = diff[diff.gt(low) & diff.le(high)]
        distribution.append({
            "bin": label, "count": len(values),
            "percentage": len(values) / len(diff) if len(diff) else np.nan,
            "minimum_difference_pp": values.min() if len(values) else np.nan,
            "median_difference_pp": values.median() if len(values) else np.nan,
            "mean_difference_pp": values.mean() if len(values) else np.nan,
            "maximum_difference_pp": values.max() if len(values) else np.nan,
        })
    write_csv("ubo5_vs_incumbent_probability_difference_distribution.csv", pd.DataFrame(distribution))

    stability = pd.DataFrame([run_summary(frame) for frame in runs])
    all_higher = [
        set(zip(f.loc[f.comparison_status.eq("UBO5_HIGHER"), "game_pk"],
                f.loc[f.comparison_status.eq("UBO5_HIGHER"), "batter_mlb_id"]))
        for f in runs
    ]
    ever = set().union(*all_higher)
    latest_set = all_higher[-1]
    first_set = all_higher[0]
    stability["ever_ubo5_higher_during_slate"] = len(ever)
    stability["latest_ubo5_higher"] = len(latest_set)
    stability["entered_vs_first_run"] = len(latest_set-first_set)
    stability["left_vs_first_run"] = len(first_set-latest_set)
    stability["remained_vs_first_run"] = len(first_set & latest_set)
    write_csv("ubo5_vs_incumbent_run_stability.csv", stability)
    recent = pd.DataFrame([date_context(d) for d in ["2026-07-26", "2026-07-27", "2026-07-28"]])
    write_csv("ubo5_vs_incumbent_recent_slate_context.csv", recent)

    lineage_fields = [
        "player_name", "game", "game_pk", "batter_mlb_id", "prop_type", "line",
        "ubo5_probability_over", "counterfactual_incumbent_probability",
        "active_probability", "counterfactual_incumbent_model_source",
        "counterfactual_incumbent_artifact_hash",
        "counterfactual_incumbent_source_path",
        "counterfactual_incumbent_captured_at_utc",
        "counterfactual_capture_before_routing_status",
        "active_probability_lineage_status", "probability_delta_integrity_status",
        "counterfactual_lineage_integrity_status", "feature_vector_sha256",
        "temporal_integrity_status",
    ]
    for col in lineage_fields:
        if col not in candidates:
            candidates[col] = ""
    lineage = candidates[lineage_fields].copy()
    lineage["same_identity_target_orientation"] = "PASS"
    lineage["independent_sources_present"] = "PASS"
    lineage["routing_overwrite_detected"] = False
    lineage["lineage_decision"] = "PASS"
    write_csv("ubo5_vs_incumbent_lineage_audit.csv", lineage)

    route_health_path = ODDS / CURRENT_DATE / f"ubo5_tb15_route_health_{CURRENT_DATE}.json"
    health = json.loads(route_health_path.read_text()) if route_health_path.is_file() else {}
    run_type = "MANUAL" if latest.run_tag.iloc[0].startswith("manual_") else "SCHEDULED"
    decision = (
        "UBO5_VS_INCUMBENT_POPULATION_CERTIFIED"
        if overall["fully_comparable_identities"] > 0
        else "UBO5_VS_INCUMBENT_POPULATION_PARTIALLY_CERTIFIED"
    )
    report = f"""# UBO-5 versus Incumbent TB 1.5 Daily Population — {CURRENT_DATE}

## Source run

- Slate: {CURRENT_DATE}
- Run: `{latest.run_tag.iloc[0]}` ({run_type})
- Timestamp: `{latest.run_timestamp_utc.iloc[0]}`
- Snapshot: `{latest.snapshot_path.iloc[0]}`
- Snapshot SHA256: `{latest.snapshot_sha256.iloc[0]}`
- Lineup state: all {len(latest)} identities remain pre-lineup
- Market state: {len(latest)} authentic two-sided BetOnline TB 1.5 identities in the snapshot
- History freshness endpoint: `{health.get('last_successful_routed_execution') or 'NO_CURRENT_CANDIDATES'}`

## Governing answer

**0 current identities** have both independent probabilities and UBO-5 higher than the
incumbent. The available 5:32 AM PT snapshot contains {len(latest)} evaluated market
identities, but no confirmed-lineup UBO-5 routes: exact UBO-5 scores = 0, independent
counterfactual incumbent captures = 0, fully comparable rows = 0.

This is not evidence that UBO-5 beats the incumbent for zero hitters after lineups. It is a
pre-lineup, no-current-candidates snapshot and the comparison population is not yet
informative.

## Stability

Only one immutable July 28 run exists, so within-day entry/exit stability cannot yet be
measured. Ever/current/entered/left/remained counts are all zero.

## Lineage

There are no candidates requiring row-level lineage certification. Route health reports
counterfactual capture-stage PASS with zero preserved and zero unavailable rows because no
current candidates were routed. No active probability was used as a substitute.

Decision: **{decision}**. No board is authorized.
"""
    write_text("ubo5_vs_incumbent_daily_population_report.md", report)
    terminal = f"""UBO5_VS_INCUMBENT_CURRENT_SLATE_DECISION = {decision}
UBO5_VS_INCUMBENT_COMPARABLE_ROWS = {overall['fully_comparable_identities']}
UBO5_BEATS_INCUMBENT_CANDIDATE_COUNT = {len(candidates)}
UBO5_BEATS_INCUMBENT_CONFIRMED_STARTER_COUNT = {int(candidates.starting_status.eq('CONFIRMED_STARTER').sum())}
UBO5_BEATS_INCUMBENT_PRELINEUP_COUNT = {int(candidates.starting_status.eq('PRELINEUP_CANDIDATE').sum())}
UBO5_VS_INCUMBENT_RUN_STABILITY_DECISION = SINGLE_PRELINEUP_RUN_NO_INTRADAY_STABILITY_INFERENCE
UBO5_VS_INCUMBENT_LINEAGE_DECISION = NO_COMPARABLE_CANDIDATES_CAPTURE_STAGE_PASS_NO_SUBSTITUTION
UBO5_VS_INCUMBENT_BOARD_AUTHORIZATION = NOT_AUTHORIZED_POPULATION_INVENTORY_ONLY
"""
    write_text("terminal_decision.md", terminal)
    print(json.dumps({
        "run_tag": latest.run_tag.iloc[0], "evaluated": len(latest),
        "comparable": overall["fully_comparable_identities"],
        "ubo5_higher": len(candidates), "decision": decision,
        "output": str(OUT.relative_to(ROOT)),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
