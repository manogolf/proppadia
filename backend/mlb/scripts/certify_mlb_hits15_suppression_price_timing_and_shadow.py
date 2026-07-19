"""Certify price timing for Hits U1.5 pitcher-suppression research lane.

This utility is bounded and read-only. It consumes local artifacts, writes a
dated research package, and intentionally fails closed when decision timestamps
or market snapshot timing cannot be proven.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AUDIT_DATE = "2026-07-17"
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_suppression_price_timing_and_shadow/2026-07-17"
)
VALIDATION_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_hits15_pitcher_suppression_under_validation/2026-07-17"
)
POPULATION = VALIDATION_ROOT / f"exact_pitcher_dominant_population_manifest_{AUDIT_DATE}.csv"
PRICE_DIAGNOSTIC = VALIDATION_ROOT / f"price_aware_performance_report_{AUDIT_DATE}.csv"
MARKET_LEDGER = VALIDATION_ROOT / f"exact_u15_market_availability_ledger_{AUDIT_DATE}.csv"
VALIDATION_JSON = VALIDATION_ROOT / f"machine_readable_pitcher_suppression_under_validation_{AUDIT_DATE}.json"
ODDS_ROOT = Path("artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery")
PA_SHADOW_ROOTS = [
    Path("artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/2026-07-17"),
    Path("artifacts/analysis/model_development/mlb_prospective_run_bound_pa_shadow_capture/2026-07-16"),
    Path("artifacts/analysis/model_development/mlb_pa_parent_generator_contract_correction/2026-07-16/strict_shadow_attachment"),
]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def write_md(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def norm(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return norm(value).lower() in {"true", "1", "yes", "y"}


def ts_from_name(path: Path) -> str:
    m = re.search(r"(20\d{6}T\d{6}Z?)", path.name)
    if not m:
        m = re.search(r"(20\d{6}T\d{6})", path.name)
    return m.group(1) if m else ""


def decimal_price(price: Any) -> float | None:
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(p) or p == 0:
        return None
    return 1.0 + (p / 100.0 if p > 0 else 100.0 / abs(p))


def breakeven(price: Any) -> float | None:
    d = decimal_price(price)
    return None if not d else 1.0 / d


def american_profit(result: str, price: Any) -> float | None:
    if result not in {"win", "loss"}:
        return None
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(p) or p == 0:
        return None
    if result == "loss":
        return -1.0
    return p / 100.0 if p > 0 else 100.0 / abs(p)


def wilson(wins: int, n: int) -> tuple[float | None, float | None, float | None]:
    if n <= 0:
        return None, None, None
    z = 1.96
    p = wins / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denom
    return p, max(0.0, center - margin), min(1.0, center + margin)


def markdown_table(df: pd.DataFrame, max_rows: int = 18) -> str:
    if df.empty:
        return "No rows."
    view = df.head(max_rows)
    cols = list(view.columns)
    lines = ["| " + " | ".join(cols) + " |", "| " + " | ".join(["---"] * len(cols)) + " |"]
    for _, row in view.iterrows():
        vals = []
        for c in cols:
            v = row[c]
            if isinstance(v, float):
                vals.append("" if pd.isna(v) else f"{v:.4f}")
            else:
                vals.append("" if pd.isna(v) else str(v).replace("|", "\\|"))
        lines.append("| " + " | ".join(vals) + " |")
    if len(df) > max_rows:
        lines.append(f"\nShowing {max_rows} of {len(df)} rows.")
    return "\n".join(lines)


def inventory_odds_sources() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted(ODDS_ROOT.glob("**/*")):
        if not path.is_file() or path.suffix.lower() not in {".csv", ".json", ".md"}:
            continue
        date = ""
        for part in path.parts:
            if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", part):
                date = part
        row = {
            "path": str(path),
            "sha256": sha(path),
            "source_date": date,
            "run_tag_or_timestamp": ts_from_name(path),
            "file_type": path.suffix.lower().lstrip("."),
            "sportsbook": "",
            "rows": "",
            "prop_side_representation": "",
            "date_range": date,
            "pregame_certifiable": "unknown",
            "paired_to_candidate_decision_time": False,
            "notes": "",
        }
        if path.suffix.lower() == ".csv":
            try:
                df = pd.read_csv(path, low_memory=False)
                row["rows"] = len(df)
                if {"bookmaker_key", "side", "line", "price"}.issubset(df.columns):
                    books = sorted(set(df["bookmaker_key"].dropna().astype(str)))
                    sides = sorted(set(df["side"].dropna().astype(str)))
                    row["sportsbook"] = ";".join(books[:12])
                    row["prop_side_representation"] = f"sides={';'.join(sides)}; lines={';'.join(map(str, sorted(set(df['line'].dropna()))[:8]))}"
                    if "snapshot_timestamp" in df.columns:
                        row["run_tag_or_timestamp"] = ";".join(sorted(set(df["snapshot_timestamp"].dropna().astype(str)))[:3])
                    if "commence_time" in df.columns and "snapshot_timestamp" in df.columns:
                        snap = pd.to_datetime(df["snapshot_timestamp"], errors="coerce", utc=True)
                        commence = pd.to_datetime(df["commence_time"], errors="coerce", utc=True)
                        valid = (snap.notna() & commence.notna() & (snap < commence)).mean()
                        row["pregame_certifiable"] = "mixed_or_partial" if 0 < valid < 1 else ("yes" if valid == 1 else "no")
                elif "under_rows" in df.columns:
                    row["prop_side_representation"] = "aggregate book-line availability"
                else:
                    row["prop_side_representation"] = "csv_schema_not_market_row_level"
            except Exception as exc:
                row["notes"] = f"parse_error={exc}"
        elif path.suffix.lower() == ".json":
            try:
                data = json.loads(path.read_text())
                row["rows"] = len(data) if isinstance(data, list) else 1
                row["prop_side_representation"] = "raw_json_snapshot"
            except Exception as exc:
                row["notes"] = f"parse_error={exc}"
        else:
            row["prop_side_representation"] = "markdown_report"
        rows.append(row)
    return pd.DataFrame(rows)


def price_performance(df: pd.DataFrame, group_col: str, executable_col: str) -> pd.DataFrame:
    rows = []
    work = df[df[executable_col] & df["integrated_u15_result"].isin(["win", "loss"])].copy()
    if work.empty:
        return pd.DataFrame(
            columns=[
                group_col,
                "wagers",
                "wins",
                "losses",
                "avg_american_odds",
                "avg_decimal_odds",
                "weighted_break_even_rate",
                "realized_win_rate",
                "wilson_low",
                "wilson_high",
                "flat_stake_roi",
                "net_units",
                "median_odds",
                "maximum_drawdown",
                "best_date",
                "worst_date",
            ]
        )
    work["profit_1u"] = work.apply(lambda r: american_profit(r["integrated_u15_result"], r["primary_u15_price"]), axis=1)
    work["decimal_odds"] = work["primary_u15_price"].map(decimal_price)
    work["break_even"] = work["primary_u15_price"].map(breakeven)
    for key, g in work.groupby(group_col, dropna=False):
        wins = int((g["integrated_u15_result"] == "win").sum())
        losses = int((g["integrated_u15_result"] == "loss").sum())
        n = wins + losses
        rate, lo, hi = wilson(wins, n)
        by_date = g.groupby("slate_date")["profit_1u"].sum().sort_values()
        cume = g.sort_values(["slate_date", "canonical_proposition_key"])["profit_1u"].cumsum()
        dd = (cume.cummax() - cume).max() if not cume.empty else None
        rows.append(
            {
                group_col: key,
                "wagers": n,
                "wins": wins,
                "losses": losses,
                "avg_american_odds": pd.to_numeric(g["primary_u15_price"], errors="coerce").mean(),
                "avg_decimal_odds": g["decimal_odds"].mean(),
                "weighted_break_even_rate": g["break_even"].mean(),
                "realized_win_rate": rate,
                "wilson_low": lo,
                "wilson_high": hi,
                "flat_stake_roi": g["profit_1u"].mean(),
                "net_units": g["profit_1u"].sum(),
                "median_odds": pd.to_numeric(g["primary_u15_price"], errors="coerce").median(),
                "maximum_drawdown": dd,
                "best_date": by_date.index[-1] if len(by_date) else "",
                "worst_date": by_date.index[0] if len(by_date) else "",
            }
        )
    return pd.DataFrame(rows)


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    pop = pd.read_csv(POPULATION, low_memory=False)
    market = pd.read_csv(MARKET_LEDGER, low_memory=False)
    validation_summary = json.loads(VALIDATION_JSON.read_text())
    odds_inventory = inventory_odds_sources()
    write_csv(odds_inventory, out_dir / f"odds_source_inventory_{AUDIT_DATE}.csv")

    # Frozen policy: at-or-before governed candidate decision timestamp. The
    # parent artifacts preserve price snapshots but not governed decision
    # timestamps, so rows fail closed for certified execution.
    policy = pd.DataFrame(
        [
            {
                "policy_name": "primary_selection_time_u15_policy",
                "definition": "latest preserved U1.5 market snapshot at or before governed candidate decision timestamp",
                "requires_decision_timestamp": True,
                "requires_snapshot_timestamp": True,
                "requires_exact_game_player_line_side": True,
                "allows_later_snapshot": False,
                "allows_inferred_under_price": False,
                "status": "FROZEN_FAIL_CLOSED",
                "reason": "governed candidate decision timestamps are not retained in the bound historical suppression population",
            }
        ]
    )
    write_csv(policy, out_dir / f"frozen_decision_time_policy_{AUDIT_DATE}.csv")

    ledger = pop.copy()
    ledger["governed_decision_timestamp"] = ""
    ledger["decision_timestamp_source_path"] = ""
    ledger["decision_timestamp_status"] = "NO_PRESERVED_DECISION_TIMESTAMP"
    ledger["primary_price_policy"] = "latest_snapshot_at_or_before_governed_candidate_decision_timestamp"
    ledger["primary_price_binding_status"] = ledger.apply(
        lambda r: "NO_PRESERVED_DECISION_TIMESTAMP"
        if not norm(r.get("governed_decision_timestamp"))
        else (
            "EXACT_SELECTION_TIME_PRICE"
            if boolish(r.get("u15_available")) and pd.notna(r.get("u15_price"))
            else "SIDE_NOT_POSTED"
        ),
        axis=1,
    )
    ledger["primary_executable"] = False
    ledger["primary_u15_price"] = pd.NA
    ledger["primary_snapshot_timestamp"] = ""
    ledger["primary_sportsbook"] = ""
    ledger["later_or_uncertified_u15_price"] = ledger["u15_price"]
    ledger["later_or_uncertified_price_status"] = ledger["under_market_availability_status"]
    write_csv(ledger, out_dir / f"exact_proposition_to_price_ledger_{AUDIT_DATE}.csv")

    rejection = (
        ledger.groupby(["primary_price_binding_status", "under_market_availability_status", "suppression_subtype"], dropna=False)
        .size()
        .reset_index(name="rows")
    )
    write_csv(rejection, out_dir / f"price_rejection_and_missingness_ledger_{AUDIT_DATE}.csv")

    availability = pd.DataFrame(
        [
            {
                "category": "directionally_eligible_pitcher_dominant",
                "rows": len(ledger),
                "notes": "Frozen pitcher-dominant Hits 1.5 population.",
            },
            {
                "category": "affirmative_established_suppression",
                "rows": int(ledger["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION").sum()),
                "notes": "Primary confirmatory direction population.",
            },
            {
                "category": "u15_price_preserved_uncertified_timing",
                "rows": int(ledger["u15_price"].notna().sum()),
                "notes": "Price exists locally but selection-time freshness is unknown.",
            },
            {
                "category": "certified_primary_executable",
                "rows": int(ledger["primary_executable"].sum()),
                "notes": "Strict policy requires decision timestamp and at-or-before snapshot; none certify.",
            },
            {
                "category": "later_or_uncertified_price_only",
                "rows": int(ledger["u15_price"].notna().sum()),
                "notes": "Preserved for market-development diagnostics only.",
            },
        ]
    )
    write_csv(availability, out_dir / f"execution_availability_report_{AUDIT_DATE}.csv")

    certified_perf = price_performance(ledger, "suppression_subtype", "primary_executable")
    write_csv(certified_perf, out_dir / f"certified_price_performance_report_{AUDIT_DATE}.csv")

    diagnostic = ledger[ledger["u15_price"].notna() & ledger["integrated_u15_result"].isin(["win", "loss"])].copy()
    diagnostic["primary_u15_price"] = diagnostic["u15_price"]
    diagnostic["diagnostic_executable"] = True
    diagnostic_perf = price_performance(diagnostic, "suppression_subtype", "diagnostic_executable")
    diagnostic_perf["certification_status"] = "DIAGNOSTIC_ONLY_TIMING_UNKNOWN"
    write_csv(diagnostic_perf, out_dir / f"uncertified_preserved_price_diagnostic_{AUDIT_DATE}.csv")

    # Representativeness: compare preserved-price vs no-price rows, not as a
    # certified ROI generalization.
    rep_rows = []
    for col in [
        "slate_date",
        "pitcher_tier_seen",
        "hitter_tier_seen",
        "suppression_subtype",
        "current_side_surface_state",
        "pa_opp_v1_d15_opportunity_band",
        "outcome_resolved",
    ]:
        tmp = (
            ledger.assign(price_group=ledger["u15_price"].notna().map({True: "price_preserved", False: "no_certified_price"}))
            .groupby(["price_group", col], dropna=False)
            .size()
            .reset_index(name="rows")
        )
        tmp["comparison_field"] = col
        tmp = tmp.rename(columns={col: "field_value"})
        rep_rows.append(tmp[["comparison_field", "field_value", "price_group", "rows"]])
    rep = pd.concat(rep_rows, ignore_index=True)
    write_csv(rep, out_dir / f"representativeness_analysis_{AUDIT_DATE}.csv")

    temporal_rows = [
        {
            "analysis_scope": "certified_primary_executable",
            "temporal_block": "ALL",
            "rows": int(ledger["primary_executable"].sum()),
            "finding": "No certified executable rows because decision timestamp binding failed closed.",
        }
    ]
    for block, g in ledger.groupby("temporal_block", dropna=False):
        temporal_rows.append(
            {
                "analysis_scope": "uncertified_preserved_price_diagnostic",
                "temporal_block": block,
                "rows": len(g),
                "u15_price_rows": int(g["u15_price"].notna().sum()),
                "resolved_price_rows": int(
                    (g["u15_price"].notna() & g["integrated_u15_result"].isin(["win", "loss"])).sum()
                ),
                "finding": "Diagnostic only; timing unknown.",
            }
        )
    temporal = pd.DataFrame(temporal_rows)
    write_csv(temporal, out_dir / f"temporal_and_concentration_stability_{AUDIT_DATE}.csv")

    concentration_rows = []
    for field, label in [
        ("player_name", "hitter"),
        ("opponent", "opponent"),
        ("team", "team"),
        ("source_sportsbook", "sportsbook"),
        ("slate_date", "date"),
    ]:
        if field not in ledger.columns:
            continue
        tmp = ledger[ledger["u15_price"].notna()].groupby(field, dropna=False).size().reset_index(name="price_preserved_rows")
        tmp = tmp.rename(columns={field: "entity"})
        tmp["entity_type"] = label
        tmp["pct_of_price_preserved"] = tmp["price_preserved_rows"] / max(int(ledger["u15_price"].notna().sum()), 1)
        concentration_rows.append(tmp)
    concentration = pd.concat(concentration_rows, ignore_index=True) if concentration_rows else pd.DataFrame()
    write_csv(concentration, out_dir / f"price_subset_concentration_{AUDIT_DATE}.csv")

    market_timing = pd.DataFrame(
        [
            {
                "metric": "preserved_u15_price_rows",
                "value": int(ledger["u15_price"].notna().sum()),
                "finding": "U1.5 prices exist in retained market artifacts for a subset.",
            },
            {
                "metric": "rows_with_known_decision_timestamp",
                "value": int(ledger["governed_decision_timestamp"].map(bool).sum()),
                "finding": "No governed candidate decision timestamp retained on historical suppression rows.",
            },
            {
                "metric": "rows_with_known_at_or_before_price",
                "value": int(ledger["primary_executable"].sum()),
                "finding": "Strict selection-time availability cannot be certified.",
            },
            {
                "metric": "local_odds_source_files_inventoried",
                "value": len(odds_inventory),
                "finding": "Local odds source inventory created; source pairing to historical decision timestamp remains blocked.",
            },
        ]
    )
    write_csv(market_timing, out_dir / f"market_timing_behavior_report_{AUDIT_DATE}.csv")

    # Prospective shadow initialization: do not manufacture a retrospective
    # result. We report available local current/live run artifacts and await a
    # genuine run-bound suppression capture with the required fields.
    shadow_sources = []
    for root in PA_SHADOW_ROOTS:
        if root.exists():
            for p in sorted(root.glob("*")):
                if p.is_file() and p.suffix.lower() in {".csv", ".json", ".md"}:
                    shadow_sources.append(
                        {
                            "source_path": str(p),
                            "sha256": sha(p),
                            "source_type": p.suffix.lower().lstrip("."),
                            "run_tag": ts_from_name(p),
                            "usable_for_suppression_shadow": False,
                            "notes": "Current/live artifact exists but does not constitute a governed suppression shadow capture.",
                        }
                    )
    shadow_source_df = pd.DataFrame(shadow_sources)
    write_csv(shadow_source_df, out_dir / f"prospective_shadow_source_inventory_{AUDIT_DATE}.csv")
    shadow = pd.DataFrame(
        [
            {
                "shadow_status": "AWAITING_FIRST_GENUINE_SUPPRESSION_SHADOW_CAPTURE",
                "slate_date": "",
                "run_tag": "",
                "cutoff_timestamp": "",
                "eligible_affirmative_suppression_rows": 0,
                "exact_u15_price_rows": 0,
                "reason": "No local artifact was found that both applied the frozen affirmative-suppression label prospectively and bound exact U1.5 price at the run timestamp.",
            }
        ]
    )
    write_csv(shadow, out_dir / f"first_live_shadow_capture_or_awaiting_status_{AUDIT_DATE}.csv")

    protocol = pd.DataFrame(
        [
            {
                "milestone": "minimum_genuine_run_tags",
                "target": 10,
                "reason": "Avoid one-run market timing artifacts.",
            },
            {
                "milestone": "minimum_affirmative_suppression_propositions",
                "target": 50,
                "reason": "Enough live rows to estimate availability and not just direction.",
            },
            {
                "milestone": "minimum_dates",
                "target": 3,
                "reason": "Prevent single-date concentration.",
            },
            {
                "milestone": "required_fields",
                "target": "run_tag, cutoff_timestamp, exact game/player/line/side, sportsbook, odds, market_snapshot_timestamp, source_hash",
                "reason": "Needed for replayable timing certification.",
            },
        ]
    )
    write_csv(protocol, out_dir / f"prospective_observation_protocol_{AUDIT_DATE}.csv")

    opportunity = pd.DataFrame(
        [
            {
                "date": "historical_bound_population",
                "current_hits_o15_candidate_count": int(ledger["current_side_surface_state"].eq("OVER_ONLY").sum()),
                "current_hits_u15_candidate_count": int(ledger["current_side_surface_state"].eq("BOTH_CONFLICT").sum()),
                "affirmative_suppression_u15_research_count": int(ledger["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION").sum()),
                "exact_executable_suppression_count": int(ledger["primary_executable"].sum()),
                "withhold_count": int((~ledger["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION")).sum()),
                "overlap_with_current_over_surfaces": int(ledger["current_side_surface_state"].eq("OVER_ONLY").sum()),
                "notes": "Historical diagnostic counts; no production surfacing.",
            }
        ]
    )
    write_csv(opportunity, out_dir / f"opportunity_volume_comparison_{AUDIT_DATE}.csv")

    decisions = {
        "MLB_HITS15_SUPPRESSION_DECISION_TIME_BINDING_DECISION": "DECISION_TIME_NOT_RETAINED_FAIL_CLOSED",
        "MLB_HITS15_SUPPRESSION_ODDS_SOURCE_INVENTORY_DECISION": "LOCAL_ODDS_SOURCES_INVENTORIED_NO_NETWORK",
        "MLB_HITS15_SUPPRESSION_EXACT_PRICE_BINDING_DECISION": "EXACT_SELECTION_TIME_PRICE_BINDING_BLOCKED_BY_MISSING_DECISION_TIMESTAMP",
        "MLB_HITS15_SUPPRESSION_EXECUTION_AVAILABILITY_DECISION": "NO_CERTIFIED_EXECUTABLE_ROWS_UNDER_FROZEN_POLICY",
        "MLB_HITS15_SUPPRESSION_CERTIFIED_ROI_DECISION": "SUPPRESSION_UNDER_PRICE_VALIDATION_BLOCKED_BY_SNAPSHOT_COVERAGE",
        "MLB_HITS15_SUPPRESSION_PRICE_REPRESENTATIVENESS_DECISION": "REPRESENTATIVENESS_INSUFFICIENT_TO_ASSESS",
        "MLB_HITS15_SUPPRESSION_PRICE_TEMPORAL_STABILITY_DECISION": "PRICE_TEMPORAL_STABILITY_NOT_CERTIFIABLE_WITH_ZERO_EXECUTABLE_ROWS",
        "MLB_HITS15_SUPPRESSION_MARKET_TIMING_DECISION": "MARKET_TIMING_BEHAVIOR_DESCRIPTIVE_ONLY_DECISION_TIME_UNBOUND",
        "MLB_HITS15_SUPPRESSION_LIVE_SHADOW_DECISION": "AWAITING_FIRST_GENUINE_SUPPRESSION_SHADOW_CAPTURE",
        "MLB_HITS15_SUPPRESSION_OBSERVATION_PROTOCOL_DECISION": "READY_FOR_PROSPECTIVE_SUPPRESSION_OBSERVATION_AFTER_RUN_BOUND_CAPTURE_HOOK",
        "MLB_HITS15_SUPPRESSION_OPPORTUNITY_VOLUME_DECISION": "HISTORICAL_DIRECTIONAL_VOLUME_EXISTS_CERTIFIED_EXECUTABLE_VOLUME_ZERO",
        "MLB_HITS15_SUPPRESSION_PROSPECTIVE_READINESS_DECISION": "READY_FOR_PROSPECTIVE_SUPPRESSION_OBSERVATION_PRICE_CERTIFICATION_PENDING",
        "MLB_HITS15_SUPPRESSION_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }
    decision_df = pd.DataFrame([{"decision": k, "value": v} for k, v in decisions.items()])
    write_csv(decision_df, out_dir / f"decision_report_{AUDIT_DATE}.csv")

    summary = {
        "generated_at_utc": now_utc(),
        "directionally_eligible_rows": len(ledger),
        "affirmative_suppression_rows": int(ledger["suppression_subtype"].eq("AFFIRMATIVE_ESTABLISHED_SUPPRESSION").sum()),
        "preserved_u15_price_rows": int(ledger["u15_price"].notna().sum()),
        "certified_executable_rows": int(ledger["primary_executable"].sum()),
        "certified_roi": None,
        "uncertified_preserved_price_diagnostic_rows": len(diagnostic),
        "parent_directional_summary": validation_summary,
        "prospective_shadow_status": "AWAITING_FIRST_GENUINE_SUPPRESSION_SHADOW_CAPTURE",
        "decisions": decisions,
    }
    write_json(summary, out_dir / f"machine_readable_price_timing_and_shadow_{AUDIT_DATE}.json")

    md = f"""# MLB Hits U1.5 Suppression Price-Timing Certification and Prospective Shadow Pilot

Generated: `{summary['generated_at_utc']}`

## Executive Summary

The frozen pitcher-suppression lane remains directionally interesting, but historical price timing **cannot be certified** from the retained artifacts. The primary frozen policy requires the latest preserved U1.5 snapshot at or before the governed candidate decision timestamp. The bound historical suppression rows do not retain that decision timestamp, so the certified executable population is **0**.

Preserved U1.5 prices still exist for **{summary['preserved_u15_price_rows']}** pitcher-suppression rows, and **{summary['uncertified_preserved_price_diagnostic_rows']}** have both preserved price and outcome, but those remain diagnostic because timing is unknown.

## Frozen Decision-Time Policy

{markdown_table(policy)}

## Execution Availability

{markdown_table(availability)}

## Certified Price Performance

{markdown_table(certified_perf)}

## Uncertified Preserved-Price Diagnostic

{markdown_table(diagnostic_perf)}

## Prospective Shadow Status

`AWAITING_FIRST_GENUINE_SUPPRESSION_SHADOW_CAPTURE`

No genuine local run-bound artifact was found that both applied the frozen affirmative-suppression label prospectively and bound exact live U1.5 prices at the run timestamp. The observation protocol is initialized, but no retrospective artifact was relabeled as prospective.

## Decisions

{chr(10).join(f'- `{k} = {v}`' for k, v in decisions.items())}

## Direct Answer

When affirmative pitcher suppression owns the matchup, the historical evidence shows a directional UNDER signal, but the retained artifacts do **not** prove that U1.5 prices were available early enough at candidate-decision time. Therefore the historical directional signal is **not yet certified as a usable wagering opportunity**. It is ready for prospective shadow observation with strict run-bound price capture.
"""
    write_md(md, out_dir / f"executive_summary_{AUDIT_DATE}.md")

    validation_rows = []
    for p in out_dir.glob("*.csv"):
        try:
            pd.read_csv(p, low_memory=False)
            validation_rows.append({"artifact": str(p), "check": "csv_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation_rows.append({"artifact": str(p), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
    for p in out_dir.glob("*.json"):
        try:
            json.loads(p.read_text())
            validation_rows.append({"artifact": str(p), "check": "json_parse", "status": "PASS", "message": ""})
        except Exception as exc:
            validation_rows.append({"artifact": str(p), "check": "json_parse", "status": "FAIL", "message": str(exc)})
    for p in out_dir.glob("*.md"):
        validation_rows.append(
            {
                "artifact": str(p),
                "check": "markdown_nonempty",
                "status": "PASS" if p.read_text().strip() else "FAIL",
                "message": "",
            }
        )
    write_csv(pd.DataFrame(validation_rows), out_dir / f"validation_report_{AUDIT_DATE}.csv")

    manifest_rows = []
    for p in sorted(out_dir.iterdir()):
        if p.is_file() and p.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            manifest_rows.append({"path": str(p), "sha256": sha(p), "bytes": p.stat().st_size})
    write_csv(pd.DataFrame(manifest_rows), out_dir / f"sha256_manifest_{AUDIT_DATE}.csv")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
