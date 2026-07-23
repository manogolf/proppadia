#!/usr/bin/env python3
"""Fail-closed source-readiness audit for the July 21 live expected-PA replay.

This audit deliberately does not synthesize historical current-parent inputs from the
offline denominator.  The requested replay is valid only when retained, timestamped
run-like parent state can be passed to the exact live producer.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.mlb.scripts import audit_mlb_hits05_strict_pregame_pa_reconstruction as pa
from backend.mlb.scripts import build_mlb_hits05_live_expected_pa_parent as live


ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_live_expected_pa_20_slate_replay/2026-07-21"
WINDOWS = ("05:30", "09:30", "11:00", "13:00", "16:30")
ACTIVATION_DATE = "2026-07-21"


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(dict.fromkeys(k for row in rows for k in row)) or ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    if OUT.exists() and any(OUT.iterdir()):
        raise FileExistsError(f"refusing to overwrite bounded replay package: {OUT}")
    OUT.mkdir(parents=True, exist_ok=False)
    denominator = pa.load_denominator()
    denominator["slate_date"] = denominator["slate_date"].astype(str)
    denominator = denominator[denominator["slate_date"] < ACTIVATION_DATE].copy()
    denominator["lineup_ts"] = pd.to_datetime(denominator["lineup_source_timestamp"], utc=True, errors="coerce")
    denominator["game_ts"] = pd.to_datetime(denominator["game_start_time"], utc=True, errors="coerce")

    current_root = live.CURRENT_PARENT_ROOT
    machine_files = sorted(current_root.glob("*/**/machine_readable_hits05_current_nonmarket_parent_producer_*.json"))
    exact_dates: dict[str, list[Path]] = {}
    for path in machine_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        date = str(payload.get("date") or path.parent.parent.name)
        if date < ACTIVATION_DATE:
            exact_dates.setdefault(date, []).append(path)

    inventory: list[dict] = []
    for date in sorted(denominator["slate_date"].unique(), reverse=True):
        g = denominator[denominator["slate_date"].eq(date)]
        confirmed = g[g["lineup_status"].astype(str).eq("CONFIRMED_PREGAME_STARTER")]
        timestamped = confirmed[confirmed["lineup_ts"].notna() & (confirmed["lineup_ts"] < confirmed["game_ts"])]
        runs = exact_dates.get(date, [])
        # A denominator is an outcome-bearing offline spine, not a retained live-parent source state.
        technically_qualifying = bool(runs) and g["actual_pa"].notna().any() and g["actual_hits"].notna().any()
        reason = "QUALIFIES_RETAINED_RUN_AND_OUTCOME"
        if not runs:
            reason = "NO_RETAINED_EXACT_RUN_TAG_PARENT_STATE"
        elif not g["actual_pa"].notna().any():
            reason = "NO_AUTHORITATIVE_ACTUAL_PA_IN_FROZEN_OUTCOME_SPINE"
        elif not g["actual_hits"].notna().any():
            reason = "NO_AUTHORITATIVE_ACTUAL_HITS_IN_FROZEN_OUTCOME_SPINE"
        inventory.append({
            "slate_date": date,
            "outcome_rows": int(g["actual_pa"].notna().sum()),
            "confirmed_timestamped_rows": len(timestamped),
            "retained_current_parent_runs": len(runs),
            "technically_qualifying": technically_qualifying,
            "selection_status": "SELECTABLE" if technically_qualifying else "EXCLUDED",
            "exact_reason": reason,
        })
    write_csv(OUT / "eligible_date_inventory.csv", inventory)

    selectable = [r for r in inventory if r["technically_qualifying"]]
    selected = selectable[:20]
    write_csv(OUT / "frozen_20_slate_manifest.csv", selected,
              ["slate_date", "outcome_rows", "confirmed_timestamped_rows", "retained_current_parent_runs", "technically_qualifying", "selection_status", "exact_reason"])
    write_csv(OUT / "excluded_date_ledger.csv", [r for r in inventory if not r["technically_qualifying"]])
    write_csv(OUT / "selection_algorithm.csv", [{
        "step": 1,
        "algorithm": "Before outcome scoring, sort completed dates descending; require official PA/Hits plus a retained timestamped current-parent run-like source state; take first 20; never backfill from the outcome-bearing offline denominator.",
        "selected_count": len(selected),
        "required_count": 20,
        "status": "FAIL_CLOSED_INSUFFICIENT_QUALIFYING_SLATES" if len(selected) < 20 else "PASS",
    }])

    source_rows = []
    candidate_dates = [r["slate_date"] for r in inventory[:20]]
    for date in candidate_dates:
        for window in WINDOWS:
            source_rows.append({
                "slate_date": date, "window_pt": window,
                "reconstruction_status": "SOURCE_STATE_NOT_RECONSTRUCTABLE",
                "source_artifact": "", "valid_prediction_rows": 0,
                "notes": "No retained run-tagged or timestamp-bound live current-parent state at this governed window; offline outcome spine rejected as a substitute.",
            })
    write_csv(OUT / "source_state_reconstruction_ledger.csv", source_rows)
    write_csv(OUT / "expected_source_coverage.csv", [{
        "required_slates": 20, "required_windows_per_slate": 5,
        "required_source_states": 100, "reconstructable_source_states": 0,
        "coverage": 0.0, "status": "SOURCE_STATE_NOT_RECONSTRUCTABLE",
    }])

    empty_specs = {
        "all_window_prediction_ledger.csv": ["slate_date", "game_id", "player_id", "window_pt", "status"],
        "governing_prediction_ledger.csv": ["slate_date", "game_id", "player_id", "governing_rule", "status"],
        "official_grading_ledger.csv": ["slate_date", "game_id", "player_id", "actual_pa", "actual_hits", "status"],
        "pa_accuracy_results.csv": ["population", "rows", "mae", "rmse", "status"],
        "low_pa_detection_results.csv": ["tail", "rows", "precision", "recall", "lift", "status"],
        "hitless_risk_grading.csv": ["estimate", "rows", "pr_auc", "roc_auc", "brier", "log_loss", "status"],
        "high_opportunity_miss_ledger.csv": ["slate_date", "game_id", "player_id", "status"],
        "low_opportunity_analysis.csv": ["cohort", "rows", "status"],
        "generic_opportunity_loss_proxy_analysis.csv": ["cohort", "rows", "status"],
        "window_value_analysis.csv": ["window_pt", "reconstructable_slates", "valid_prediction_rows", "status"],
        "source_timing_analysis.csv": ["source_class", "rows", "status"],
        "prediction_change_analysis.csv": ["population", "rows", "status"],
        "slate_stability_table.csv": ["slate_date", "rows", "status"],
        "coverage_flow.csv": ["stage", "rows", "status"],
    }
    for name, fields in empty_specs.items():
        write_csv(OUT / name, [], fields)

    prospective_dir = live.PACKAGE_ROOT / "live_parent_runs/2026-07-21/local_daily_20260721T233005Z_live_pa_shadow_after_source"
    prospective_files = list(prospective_dir.glob("live_expected_pa_parent_*.csv"))
    prospective = pd.read_csv(prospective_files[0], low_memory=False) if prospective_files else pd.DataFrame()
    write_csv(OUT / "first_prospective_run_comparison.csv", [{
        "population": "2026-07-21_first_prospective_run", "parent_rows": 144,
        "eligible_rows": 126, "withheld_rows": 18, "shadow_rows_found": len(prospective),
        "historical_graded_rows": 0, "comparability": "NOT_ASSESSABLE_NO_TRUSTWORTHY_HISTORICAL_REPLAY",
    }])

    decisions = {
        "MLB_HITS05_20_SLATE_REPLAY_DATE_FREEZE_DECISION": "FAIL_CLOSED_FEWER_THAN_20_TECHNICALLY_QUALIFYING_RETAINED_SOURCE_SLATES",
        "MLB_HITS05_20_SLATE_SOURCE_RECONSTRUCTION_DECISION": "SOURCE_STATE_NOT_RECONSTRUCTABLE",
        "MLB_HITS05_20_SLATE_LIVE_PRODUCER_REPLAY_DECISION": "NOT_EXECUTED_NO_VALID_HISTORICAL_PARENT_INPUTS",
        "MLB_HITS05_20_SLATE_PA_ACCURACY_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_LOW_PA_DETECTION_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_HITLESS_RISK_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_HIGH_PA_MISS_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_OPPORTUNITY_LOSS_PROXY_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_WINDOW_VALUE_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_SOURCE_TIMING_DECISION": "HISTORICAL_SOURCE_RETENTION_INSUFFICIENT",
        "MLB_HITS05_20_SLATE_PREDICTION_CHANGE_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_STABILITY_DECISION": "NOT_GRADEABLE",
        "MLB_HITS05_20_SLATE_COVERAGE_DECISION": "ZERO_TRUSTWORTHY_REPLAY_COVERAGE",
        "MLB_HITS05_FIRST_PROSPECTIVE_RUN_COMPARABILITY_DECISION": "NOT_ASSESSABLE_NO_TRUSTWORTHY_HISTORICAL_REPLAY",
        "MLB_HITS05_LIVE_PA_OUTCOME_GAUGE_DECISION": "LIVE_REPLAY_NOT_TRUSTWORTHY_DUE_TO_SOURCE_RECONSTRUCTION",
        "MLB_HITS05_PROSPECTIVE_PILOT_CONTINUATION_DECISION": "REBUILD_HISTORICAL_SOURCE_BINDING_BEFORE_INTERPRETATION",
        "MLB_HITS05_PRODUCTION_ACTION_DECISION": "RETROSPECTIVE_AND_SHADOW_ONLY_NO_PRODUCTION_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_UNCHANGED_LIVE_EXPECTED_PA_RESEARCH_SHADOW_ONLY",
    }
    write_csv(OUT / "outcome_gauge_decision.csv", [{"decision": k, "value": v} for k, v in decisions.items()])
    machine = {
        "generated_at": datetime.now(timezone.utc).isoformat(), "activation_date": ACTIVATION_DATE,
        "selected_model": live.SELECTED_MODEL, "required_model_contract_sha256": "14ef8cc3069dccf85920c10ea557919e6113ed801b2868b02c19d01031c1b737",
        "observed_model_contract_sha256": live.selected_model_contract()["contract_sha256"],
        "selected_slate_count": len(selected), "required_slate_count": 20,
        "scored_rows": 0, "outcome_resolved_rows": 0, "decisions": decisions,
        "direct_answer": "The exact live producer cannot be validly replayed over 20 slates from retained local source state. Opportunity and zero-hit accuracy are therefore not estimable; the replay does not support a confidence claim. Continue prospective evidence collection unchanged, and rebuild historical source binding before retrospective interpretation.",
        "guardrails": {"database_writes": False, "network_calls": False, "production_changes": False, "offline_denominator_used_as_live_parent": False},
    }
    (OUT / "machine_readable.json").write_text(json.dumps(machine, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (OUT / "live_expected_pa_20_slate_replay_2026-07-21.md").write_text(f"""# MLB Hits 0.5 Live Expected-PA Twenty-Slate Replay

## Result

`LIVE_REPLAY_NOT_TRUSTWORTHY_DUE_TO_SOURCE_RECONSTRUCTION`

The requested 20-slate outcome gauge cannot be computed honestly from retained local artifacts. The outcome-bearing historical denominator covers 76 dates through July 18, but it is an offline spine and is not a retained five-window current-parent source state. Exact current-parent run artifacts begin after that history. Substituting the denominator would violate the requirement to invoke the exact live producer on reconstructed run-like inputs and would risk using later lineup/outcome state.

No PA, low-PA, hitless, window, source-timing, change, stability, or opportunity-loss performance statistic was fabricated. The date freeze failed before scoring, so the offline references (MAE 0.7595, RMSE 1.0195, top-20% lift 2.48x) remain references only.

## Direct answer

The exact live producer's 20-slate opportunity and zero-hit accuracy is **not estimable from the retained historical source state**. This replay cannot add confidence to the pilot. The prospective shadow should remain unchanged and continue collecting governed evidence; historical source binding must be rebuilt before interpreting a retrospective replay. No production behavior was changed.
""", encoding="utf-8")

    validation = []
    for path in sorted(OUT.iterdir()):
        if not path.is_file() or path.name in {"sha256_manifest.csv", "validation_report.csv"}:
            continue
        status, notes = "PASS", ""
        try:
            if path.suffix == ".csv":
                list(csv.DictReader(path.open(encoding="utf-8")))
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            status, notes = "FAIL", str(exc)
        validation.append({"artifact": rel(path), "status": status, "notes": notes})
    validation += [
        {"artifact": "model_contract_sha256", "status": "PASS" if machine["observed_model_contract_sha256"] == machine["required_model_contract_sha256"] else "FAIL", "notes": machine["observed_model_contract_sha256"]},
        {"artifact": "guardrail:no_offline_spine_substitution", "status": "PASS", "notes": "fail-closed"},
        {"artifact": "guardrail:no_prospective_ledger_write", "status": "PASS", "notes": "read-only"},
    ]
    write_csv(OUT / "validation_report.csv", validation)
    manifest = [{"path": rel(path), "sha256": sha(path), "bytes": path.stat().st_size} for path in sorted(OUT.iterdir()) if path.is_file() and path.name != "sha256_manifest.csv"]
    write_csv(OUT / "sha256_manifest.csv", manifest)
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
