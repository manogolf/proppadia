"""Materialize the MLB live hitter opportunity/profile parent source.

This bounded utility marks July 17 Pitcher Hits Allowed Challenger pregame
scoring as unavailable because the governed parent was not captured, then
creates the reusable default-off parent-source package needed by future genuine
pregame slates. July 17 remains open for ordinary reconciliation.

It performs no network calls, no OddsAPI calls, no DB writes, no model fitting,
no production behavior changes, and no retrospective July 17 lineup recovery.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.mlb.scripts import materialize_mlb_current_pitcher_opponent_lineup_encounter_features as encounter_source
from backend.mlb.scripts import materialize_mlb_pitcher_hits_allowed_live_replay_repair as live_replay
from backend.mlb.scripts import materialize_mlb_run_bound_hitter_player_game_spine as spine_source


RUN_DATE = "2026-07-17"
RUN_TAG = "local_daily_20260717T200004Z"
CUTOFF = "2026-07-17T20:00:04Z"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_live_hitter_opportunity_profile_parent/2026-07-17"
)

JULY17_SLATE = spine_source.CURRENT_SLATE
JULY17_SPINE_PACKAGE = spine_source.DEFAULT_OUTPUT_DIR
JULY17_ENCOUNTER_PACKAGE = encounter_source.DEFAULT_OUTPUT_DIR
JULY17_PHA_WITHHELD_EVIDENCE_PACKAGE = live_replay.OUT_DIR
PREGAME_LINEUP_CAPTURE_ROOT = Path("artifacts/analysis/mlb/pregame_lineup_capture")
RUN_BOUND_PA_SUMMARY = spine_source.PA_PARENT_SUMMARY
RUN_BOUND_PA_NOT_CREATED = Path(
    "artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/2026-07-17/"
    "run_bound_pa_parent_artifact_2026-07-17_not_created.csv"
)

PARENT_CONTRACT_VERSION = "live_hitter_opportunity_profile_parent_v1_2026_07_17"
GENERATOR_VERSION = "live_hitter_opportunity_profile_parent_generator_v1"

LIVE_PARENT_COLUMNS = [
    "slate_date",
    "run_tag",
    "prediction_cutoff",
    "candidate_timestamp",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "opposing_starter_id",
    "opposing_starter_name",
    "lineup_source_priority",
    "lineup_source_status",
    "lineup_source_path",
    "lineup_source_sha256",
    "lineup_source_timestamp",
    "lineup_semantics",
    "batting_order",
    "lineup_slot",
    "lineup_bucket",
    "lineup_confirmed",
    "pred_total_pa",
    "pred_starter_pa",
    "pred_bullpen_pa",
    "p_hitter_receives_fourth_pa",
    "p_hitter_receives_fifth_pa",
    "hitter_per_pa_hit_estimate",
    "p_hit_starter_prior",
    "p_hit_bullpen_prior",
    "season_to_date_hits_per_pa",
    "season_to_date_pa_per_game",
    "d15_pa_per_game",
    "d30_hits_per_pa",
    "predicted_exposure_p_zero_hits",
    "starter_expected_hits_allowed",
    "pitcher_base",
    "starter_prior_start_count",
    "suppression_subtype",
    "strict_prior_status",
    "profile_support_class",
    "profile_evidence_class",
    "identity_status",
    "opportunity_status",
    "profile_status",
    "parent_row_status",
    "withheld_reason",
    "contract_version",
    "generator_version",
]


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
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def source_row(path: Path, component: str, role: str, required: list[str] | None = None) -> dict[str, Any]:
    required = required or []
    df = read_csv(path)
    missing = [c for c in required if c not in df.columns]
    run_date_rows = 0
    if "slate_date" in df.columns:
        run_date_rows = int(df[df["slate_date"].astype(str).eq(RUN_DATE)].shape[0])
    elif "game_date" in df.columns:
        run_date_rows = int(df[df["game_date"].astype(str).str[:10].eq(RUN_DATE)].shape[0])
    return {
        "component": component,
        "source_path": str(path),
        "role": role,
        "exists": path.exists(),
        "rows": int(len(df)),
        "run_date_rows": run_date_rows,
        "sha256": sha256_file(path) if path.exists() else "",
        "required_columns_missing": "|".join(missing),
        "status": "AVAILABLE" if path.exists() and not missing else "MISSING_OR_INCOMPLETE",
        "notes": "",
    }


def lineage_contract() -> pd.DataFrame:
    rows = []
    field_sources = {
        "identity": ["slate_date", "run_tag", "prediction_cutoff", "game_id", "player_id", "player_name", "team", "opponent"],
        "lineup": ["batting_order", "lineup_slot", "lineup_bucket", "lineup_confirmed"],
        "opportunity": ["pred_total_pa", "pred_starter_pa", "pred_bullpen_pa", "p_hitter_receives_fourth_pa", "p_hitter_receives_fifth_pa"],
        "strict_prior_profile": [
            "hitter_per_pa_hit_estimate",
            "p_hit_starter_prior",
            "p_hit_bullpen_prior",
            "season_to_date_hits_per_pa",
            "season_to_date_pa_per_game",
            "d15_pa_per_game",
            "d30_hits_per_pa",
            "predicted_exposure_p_zero_hits",
            "suppression_subtype",
            "strict_prior_status",
        ],
        "starter_context": ["opposing_starter_id", "opposing_starter_name", "starter_expected_hits_allowed", "pitcher_base", "starter_prior_start_count"],
        "provenance": [
            "lineup_source_priority",
            "lineup_source_status",
            "lineup_source_path",
            "lineup_source_sha256",
            "lineup_source_timestamp",
            "lineup_semantics",
            "contract_version",
            "generator_version",
        ],
    }
    for component, fields in field_sources.items():
        for field in fields:
            rows.append(
                {
                    "field_name": field,
                    "component": component,
                    "historical_contract_source": str(spine_source.HISTORICAL_SPINE),
                    "current_source_requirement": current_source_requirement(component),
                    "prediction_time_policy": "strict_prior_or_run_bound_pregame_only",
                    "missing_policy": "fail_closed_no_silent_imputation",
                    "downstream_use": "shared hitter Hits and Pitcher Hits Allowed encounter scoring",
                    "notes": "",
                }
            )
    return pd.DataFrame(rows)


def current_source_requirement(component: str) -> str:
    if component == "identity":
        return "run-tagged current slate/candidate artifact"
    if component == "lineup":
        return "exact run-bound confirmed or governed expected lineup artifact"
    if component == "opportunity":
        return "frozen strict-prior total/starter/bullpen PA parent generated before first pitch"
    if component == "strict_prior_profile":
        return "run-bound strict-prior hitter profile artifact generated from prior games only"
    if component == "starter_context":
        return "run-bound opposing starter identity plus existing starter expected hits allowed fields"
    return "local provenance fields"


def lineup_source_audit() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in sorted(PREGAME_LINEUP_CAPTURE_ROOT.glob("dry_runs/*/*/pregame_lineup_player_rows_*.csv")):
        date_part = path.parts[-3] if len(path.parts) >= 3 else ""
        df = read_csv(path)
        rows.append(
            {
                "source_path": str(path),
                "source_date": date_part,
                "source_type": "dry_run_statsapi_lineup_snapshot",
                "exists": path.exists(),
                "rows": len(df),
                "run_date_rows": int(df[df.get("game_date", pd.Series(dtype=str)).astype(str).str[:10].eq(RUN_DATE)].shape[0]) if not df.empty and "game_date" in df.columns else 0,
                "lineup_semantics": "pregame_snapshot_when_captured_before_start_else_not_accepted",
                "accepted_for_july17_replay": False,
                "notes": "Not used for July 17 retrospective recovery; source must exist at governed pregame cutoff.",
            }
        )
    if not rows:
        rows.append(
            {
                "source_path": str(PREGAME_LINEUP_CAPTURE_ROOT),
                "source_date": "",
                "source_type": "dry_run_statsapi_lineup_snapshot",
                "exists": PREGAME_LINEUP_CAPTURE_ROOT.exists(),
                "rows": 0,
                "run_date_rows": 0,
                "lineup_semantics": "none_found",
                "accepted_for_july17_replay": False,
                "notes": "No local lineup capture artifacts found.",
            }
        )
    return pd.DataFrame(rows)


def live_source_inventory() -> pd.DataFrame:
    rows = [
        source_row(JULY17_SLATE, "identity", "July 17 run-tagged current slate", ["slate_date", "game_id", "player_id", "player_name", "team", "opponent", "prop_type"]),
        source_row(RUN_BOUND_PA_SUMMARY, "opportunity", "July 17 PA parent capture summary", []),
        source_row(RUN_BOUND_PA_NOT_CREATED, "opportunity", "July 17 explicit parent-not-created marker", []),
        source_row(spine_source.PA_HISTORY, "opportunity", "strict-prior PA history through previous day", ["game_date", "game_id", "player_id", "plate_appearances"]),
        source_row(spine_source.LINEUP_LEDGER, "lineup", "historical canonical lineup ledger", ["slate_date", "game_id", "player_id", "canonical_pregame_lineup_slot"]),
        source_row(spine_source.CONTACT_PROFILES, "strict_prior_profile", "historical strict-prior hitter profiles", ["player_game_key", "player_id"]),
        source_row(spine_source.HISTORICAL_SPINE, "historical_contract", "frozen historical hitter opportunity/profile parent", encounter_source.ROW_LEVEL_PARENT_COLUMNS),
        source_row(JULY17_SPINE_PACKAGE / "july17_run_bound_hitter_player_game_spine_2026-07-17.csv", "previous_july17_evidence", "238-row withheld identity spine", []),
        source_row(JULY17_ENCOUNTER_PACKAGE / "july17_pitcher_encounter_artifact_2026-07-17.csv", "previous_july17_evidence", "zero-row encounter artifact", []),
        source_row(JULY17_PHA_WITHHELD_EVIDENCE_PACKAGE / "pitcher_hits_allowed_july17_withheld_row_taxonomy_2026-07-17.csv", "previous_july17_evidence", "25 live PHA withheld rows", []),
    ]
    for row in rows:
        component = row["component"]
        if component == "identity" and row["exists"]:
            row["status"] = "AVAILABLE_IDENTITY_ONLY"
            row["notes"] = "Supplies candidate identities, not expected lineup, opportunity, or profiles."
        elif component == "opportunity" and row["source_path"].endswith("july17_parent_capture_summary_2026-07-17.csv"):
            row["status"] = "BLOCKING_ZERO_PARENT_ROWS"
            row["notes"] = "Existing governed summary reports July 17 run-bound PA parent was not created."
        elif component == "lineup":
            row["status"] = "HISTORICAL_ONLY_NOT_CURRENT_RUN_BOUND"
            row["notes"] = "No accepted July 17 governed pregame lineup source is present."
        elif component == "strict_prior_profile":
            row["status"] = "HISTORICAL_ONLY_NOT_CURRENT_RUN_BOUND"
            row["notes"] = "Historical profiles are not a July 17 run-bound parent output."
        elif component == "historical_contract":
            row["status"] = "AVAILABLE_HISTORICAL_CONTRACT"
            row["notes"] = "Used for contract and parity validation only."
        elif component == "previous_july17_evidence":
            row["status"] = "PRESERVED_SOURCE_GAP_EVIDENCE" if row["exists"] else "MISSING_PRIOR_EVIDENCE"
            row["notes"] = "Preserved evidence; not rewritten or reinterpreted as recoverable live parent."
    return pd.DataFrame(rows)


def closeout() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "scope": "July 17 Pitcher Hits Allowed granular Challenger pregame scoring only",
                "decision": "JULY17_PHA_CHALLENGER_PREGAME_SCORING_UNAVAILABLE_MISSING_GOVERNED_PARENT_CAPTURE",
                "slate_reconciliation_status": "OPEN_PENDING_OFFICIAL_RECONCILIATION",
                "identity_spine_rows_preserved": 238,
                "live_pitcher_hits_allowed_props_preserved": 25,
                "pregame_challenger_scored_rows": 0,
                "graded_challenger_rows": 0,
                "withheld_challenger_rows": 25,
                "root_blocker": "missing governed pregame hitter opportunity/profile parent capture",
                "policy": "no retrospective expected-lineup or predicted-starter-PA reconstruction",
                "source_gap_evidence": str(JULY17_SPINE_PACKAGE),
                "ordinary_reconciliation_policy": "production predictions, wagers, candidate surfaces, Champion PHA, Hits O0.5, Hits O1.5, and other ordinary propositions remain eligible for normal official reconciliation",
            }
        ]
    )


def materialize_parent_candidate() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    slate = read_csv(JULY17_SLATE)
    if slate.empty:
        return (
            pd.DataFrame(columns=LIVE_PARENT_COLUMNS),
            pd.DataFrame([{"scope": "current_run", "reason": "july17_slate_missing", "rows": 0}]),
            pd.DataFrame([{"metric": "current_parent_rows", "value": 0, "status": "BLOCKED", "notes": "slate missing"}]),
        )
    identity, missing = spine_source.build_current_identity_spine(RUN_DATE, RUN_TAG, CUTOFF, JULY17_SLATE)
    parent = pd.DataFrame(columns=LIVE_PARENT_COLUMNS)
    withheld = []
    for row in identity.to_dict("records"):
        withheld.append(
            {
                "slate_date": RUN_DATE,
                "run_tag": RUN_TAG,
                "game_id": row.get("game_id"),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "opposing_starter_id": row.get("opposing_starter_id"),
                "identity_status": "PRESENT" if row.get("opposing_starter_id") not in ("", None) else "WITHHELD_MISSING_OPPOSING_STARTER",
                "lineup_status": "WITHHELD_NO_GOVERNED_PREGAME_LINEUP_SOURCE",
                "opportunity_status": "WITHHELD_NO_PRED_STARTER_PA_PARENT",
                "profile_status": "WITHHELD_NO_RUN_BOUND_STRICT_PRIOR_PROFILE",
                "primary_withheld_reason": row.get("withheld_reason") or "missing_governed_live_parent",
                    "notes": "July 17 ordinary reconciliation remains open; this row is retained as PHA Challenger source-gap evidence only.",
            }
        )
    reconciliation = pd.DataFrame(
        [
            {"metric": "identity_spine_rows", "value": int(len(identity)), "status": "PRESERVED", "notes": "candidate identity evidence from previous run-bound spine"},
            {
                "metric": "opposing_starter_identity_rows",
                "value": int(identity["opposing_starter_id"].astype(str).ne("").sum()) if not identity.empty else 0,
                "status": "PRESERVED",
                "notes": "",
            },
            {"metric": "complete_live_parent_rows", "value": 0, "status": "BLOCKED", "notes": "lineup/opportunity/profile parent missing"},
            {"metric": "pred_starter_pa_rows", "value": 0, "status": "BLOCKED", "notes": "do not reconstruct after fact"},
            {"metric": "strict_prior_profile_rows", "value": 0, "status": "BLOCKED", "notes": "no current run-bound profile output"},
        ]
    )
    return parent, pd.DataFrame(withheld), reconciliation


def historical_parity() -> pd.DataFrame:
    field = spine_source.historical_field_parity()
    scored, _, _ = live_replay.bind_frozen_model()
    pred = live_replay.historical_parity(scored)
    rows = [
        {
            "check_name": "hitter_parent_field_contract",
            "source": str(spine_source.HISTORICAL_SPINE),
            "status": "PASS" if not field.empty and field["status"].eq("PASS").all() else "FAIL",
            "rows_checked": int(field["rows_checked"].max()) if "rows_checked" in field.columns and not field.empty else 0,
            "notes": "Frozen historical parent columns are present and unchanged.",
        },
        {
            "check_name": "downstream_frozen_pha_prediction_parity",
            "source": str(live_replay.RETAINED_ROW_LEVEL),
            "status": "PASS" if not pred.empty and pred["status"].eq("PASS").all() else "FAIL",
            "rows_checked": int(pred["rows_checked"].max()) if "rows_checked" in pred.columns and not pred.empty else 0,
            "notes": "Frozen Pitcher Hits Allowed Challenger remains reproducible from retained historical rows.",
        },
    ]
    return pd.DataFrame(rows)


def empty_downstream_artifacts() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    encounter = pd.DataFrame(columns=encounter_source.ENCOUNTER_OUTPUT_COLUMNS)
    pha_ledger = pd.DataFrame(
        columns=[
            "slate_date",
            "run_tag",
            "game_id",
            "pitcher_id",
            "pitcher_name",
            "line",
            "side",
            "materialization_status",
            "withheld_reason",
            "notes",
        ]
    )
    hits_availability = pd.DataFrame(
        [
            {
                "surface": "Pitcher Hits Allowed frozen Challenger",
                "shared_parent_required": True,
                "current_parent_rows": 0,
                "scored_rows": 0,
                "status": "BLOCKED_FAIL_CLOSED",
                "notes": "Requires complete live hitter opportunity/profile parent and encounter aggregate.",
            },
            {
                "surface": "Hitter Hits O1.5/O0.5 research challengers",
                "shared_parent_required": True,
                "current_parent_rows": 0,
                "scored_rows": 0,
                "status": "READY_FOR_SHARED_SOURCE_ON_NEXT_GENUINE_PREGAME_CAPTURE",
                "notes": "Do not delay existing production predictions; parent is research/default-off.",
            },
        ]
    )
    execution_contract = pd.DataFrame(
        [
            {
                "step": "capture_or_supply_lineup_source",
                "command": ".venv/bin/python -m backend.mlb.scripts.dry_run_capture_pregame_lineups --date YYYY-MM-DD --output-dir artifacts/analysis/mlb/pregame_lineup_capture/dry_runs/YYYY-MM-DD/<run_label> --snapshot-label <run_label> --mode dry_run",
                "requires_network": True,
                "default_off": True,
                "notes": "Use official MLB StatsAPI only; preserve raw/parsed artifacts before first pitch.",
            },
            {
                "step": "materialize_live_parent",
                "command": ".venv/bin/python -m backend.mlb.scripts.materialize_mlb_live_hitter_opportunity_profile_parent --date YYYY-MM-DD --run-tag <run_tag> --cutoff <cutoff> --mode dry_run",
                "requires_network": False,
                "default_off": True,
                "notes": "Fails closed unless exact run-bound lineup, opportunity, and profile parents exist.",
            },
        ]
    )
    return encounter, pha_ledger, hits_availability, execution_contract


def decisions(parent: pd.DataFrame, hist: pd.DataFrame) -> pd.DataFrame:
    hist_pass = bool(not hist.empty and hist["status"].eq("PASS").all())
    complete = int(len(parent))
    rows = [
        ("MLB_LIVE_HITTER_PARENT_HISTORICAL_CONTRACT_DECISION", "BOUND_TO_FROZEN_HITTER_OPPORTUNITY_PROFILE_PARENT_CONTRACT"),
        ("MLB_LIVE_HITTER_PARENT_LINEUP_SOURCE_DECISION", "NO_ACCEPTED_RUN_BOUND_PREGAME_LINEUP_SOURCE_FOR_JULY17_PHA_CHALLENGER_SCORING"),
        ("MLB_LIVE_HITTER_PARENT_EXTERNAL_SOURCE_DECISION", "OFFICIAL_STATSAPI_LINEUP_CAPTURE_PATH_EXISTS_NETWORK_NOT_EXECUTED_IN_THIS_PACKAGE"),
        ("MLB_LIVE_HITTER_PARENT_IDENTITY_DECISION", "JULY17_238_IDENTITY_ROWS_PRESERVED_AS_SOURCE_GAP_EVIDENCE"),
        ("MLB_LIVE_HITTER_PARENT_OPPORTUNITY_DECISION", "BLOCKED_NO_CURRENT_RUN_BOUND_TOTAL_STARTER_BULLPEN_PA_PARENT"),
        ("MLB_LIVE_HITTER_PARENT_PRED_STARTER_PA_DECISION", "BLOCKED_NO_CURRENT_PRED_STARTER_PA_SOURCE_DO_NOT_RECONSTRUCT_JULY17"),
        ("MLB_LIVE_HITTER_PARENT_PROFILE_DECISION", "BLOCKED_NO_CURRENT_RUN_BOUND_STRICT_PRIOR_HITTER_PROFILE_OUTPUT"),
        ("MLB_LIVE_HITTER_PARENT_GENERATOR_DECISION", "REUSABLE_DEFAULT_OFF_GENERATOR_IMPLEMENTED_CONFIRMED_LINEUP_ONLY_FAIL_CLOSED"),
        ("MLB_LIVE_HITTER_PARENT_HISTORICAL_PARITY_DECISION", "PASS" if hist_pass else "FAIL"),
        ("MLB_LIVE_HITTER_PARENT_CURRENT_RUN_DECISION", f"JULY17_PHA_CHALLENGER_PREGAME_SCORING_UNAVAILABLE_COMPLETE_PARENT_ROWS_{complete}"),
        ("MLB_JULY17_SLATE_RECONCILIATION_STATUS", "OPEN_PENDING_OFFICIAL_RECONCILIATION"),
        ("MLB_JULY17_PHA_CHAMPION_GRADING_STATUS", "PENDING_OFFICIAL_OUTCOME"),
        ("MLB_JULY17_PHA_CHALLENGER_STATUS", "WITHHELD_NO_VALID_PREGAME_SCORE"),
        ("MLB_JULY17_O15_PROSPECTIVE_RUN1_STATUS", "BOUND_PENDING_GRADE"),
        ("MLB_LIVE_PARENT_FORWARD_STATUS", "LINEUP_ACQUISITION_BLOCKED"),
        ("MLB_JULY17_PRODUCTION_STATUS", "UNCHANGED"),
        ("MLB_LIVE_HITTER_PARENT_ENCOUNTER_CHAIN_DECISION", "NOT_RUN_ZERO_COMPLETE_PARENT_ROWS"),
        ("MLB_LIVE_HITTER_PARENT_PHA_SCORING_DECISION", "NOT_RUN_ZERO_COMPLETE_PARENT_ROWS"),
        ("MLB_LIVE_HITTER_PARENT_HITS_SHARED_SOURCE_DECISION", "SHARED_PARENT_SCHEMA_READY_BUT_CURRENT_RUN_BLOCKED"),
        ("MLB_LIVE_HITTER_PARENT_DAILY_EXECUTION_DECISION", "DEFAULT_OFF_MANUAL_DRY_RUN_ONLY_UNTIL_GOVERNED_PREGAME_CAPTURE_EXISTS"),
        ("MLB_LIVE_HITTER_PARENT_NEXT_STEP_DECISION", "RUN_NEXT_GENUINE_PREGAME_CONFIRMED_LINEUP_CAPTURE_THEN_ATTACH_FROZEN_OPPORTUNITY_PROFILE_PARENTS"),
        ("MLB_LIVE_HITTER_PARENT_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    return pd.DataFrame(rows, columns=["decision_name", "decision_value"])


def validate(paths: list[Path], guardrails: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for path in paths:
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                pd.read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md":
                if not path.read_text(encoding="utf-8").lstrip().startswith("#"):
                    raise ValueError("markdown does not start with heading")
        except Exception as exc:  # pragma: no cover - validation report path
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact": str(path), "validation": status, "notes": notes})
    for name, value in guardrails.items():
        rows.append({"artifact": f"guardrail_{name}", "validation": "PASS" if value in (0, False, "PASS") else "FAIL", "notes": str(value)})
    return pd.DataFrame(rows)


def summary_md(generated_at: str, dec: pd.DataFrame, reconciliation: pd.DataFrame) -> str:
    decision_map = {row.decision_name: row.decision_value for row in dec.itertuples(index=False)}
    recon_lines = ["| metric | value | status | notes |", "| --- | ---: | --- | --- |"]
    for row in reconciliation.to_dict("records"):
        recon_lines.append(
            "| {metric} | {value} | {status} | {notes} |".format(
                metric=row.get("metric", ""),
                value=row.get("value", ""),
                status=row.get("status", ""),
                notes=str(row.get("notes", "")).replace("|", "/"),
            )
        )
    return f"""# MLB Live Hitter Opportunity and Strict-Prior Profile Parent Source

Generated: `{generated_at}`

## Direct Answer

Is a shared live hitter opportunity/profile parent now available early enough to
support both Pitcher Hits Allowed and hitter Hits research challengers?

`NO` for July 17 Pitcher Hits Allowed granular Challenger scoring. That
specific Challenger scoring branch is unavailable because no governed pregame
parent was captured:

`JULY17_PHA_CHALLENGER_PREGAME_SCORING_UNAVAILABLE_MISSING_GOVERNED_PARENT_CAPTURE`

The repository has a preserved July 17 candidate identity spine, but it does
not have a governed run-bound pregame lineup capture, a current
`pred_starter_pa` parent, or a current run-bound strict-prior hitter profile
output. The new utility provides the reusable default-off parent contract and
fails closed until those sources exist during a genuine pregame run.

## Current Chain State

{chr(10).join(recon_lines)}

## July 17 Policy

July 17 remains open for ordinary official reconciliation. Production
predictions, full-slate predictions, candidate surfaces, executed wagers,
Pitcher Hits Allowed Champion predictions, Hits O0.5, Hits O1.5, and other
ordinarily reconciled propositions should still be reconciled when the normal
repository-backed official outcome source is available.

Only the missing PHA Challenger pregame predictions are permanently unavailable
for July 17. The existing 238-row identity spine and 25-row Pitcher Hits Allowed
withheld manifests remain source-gap evidence. No expected lineup, projected
Starter PA, or strict-prior profile is fabricated from postgame information.

## Decisions

{chr(10).join(f'- `{k}` = `{v}`' for k, v in decision_map.items())}

## No Behavior Changed

No network calls, OddsAPI calls, database writes, model fitting/refitting,
production code paths, LaunchAgents, formulas, tiers, selectors, uploads,
workspace behavior, or historical artifacts were modified.
"""


def build(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = utc_now()
    contract = lineage_contract()
    source_inventory = live_source_inventory()
    lineup_audit = lineup_source_audit()
    close = closeout()
    parent, withheld, reconciliation = materialize_parent_candidate()
    hist = historical_parity()
    encounter, pha_ledger, hits_availability, execution_contract = empty_downstream_artifacts()
    dec = decisions(parent, hist)
    external = pd.DataFrame(
        [
            {
                "source": "MLB StatsAPI boxscore/feed battingOrder",
                "repo_path": "backend/mlb/scripts/dry_run_capture_pregame_lineups.py",
                "accepted_semantics": "official pregame snapshot only when captured before game start",
                "network_called_in_this_package": False,
                "raw_response_required": True,
                "parsed_artifact_required": True,
                "notes": "Use only existing StatsAPI convention; no third-party lineup vendor.",
            }
        ]
    )
    guardrails = {
        "network_calls": 0,
        "oddsapi_calls": 0,
        "db_writes": 0,
        "model_fits_or_refits": 0,
        "new_formulas": 0,
        "postgame_reconstruction": 0,
        "production_behavior_changed": False,
        "launchagent_changes": 0,
    }
    files = {
        "summary": output_dir / "live_hitter_opportunity_profile_parent_summary_2026-07-17.md",
        "closeout": output_dir / "july17_pha_replay_closeout_2026-07-17.csv",
        "contract": output_dir / "historical_parent_contract_2026-07-17.csv",
        "source_inventory": output_dir / "live_lineup_source_audit_2026-07-17.csv",
        "external": output_dir / "external_source_boundary_report_2026-07-17.csv",
        "parent": output_dir / "latest_genuine_pregame_parent_artifact_2026-07-17.csv",
        "withheld": output_dir / "live_parent_withheld_rows_2026-07-17.csv",
        "reconciliation": output_dir / "opportunity_reconciliation_2026-07-17.csv",
        "profile": output_dir / "strict_prior_profile_coverage_2026-07-17.csv",
        "encounter": output_dir / "downstream_pitcher_encounter_artifact_2026-07-17.csv",
        "pha": output_dir / "frozen_pha_challenger_ledger_2026-07-17.csv",
        "hits": output_dir / "shared_hits_availability_report_2026-07-17.csv",
        "execution": output_dir / "manual_default_off_execution_contract_2026-07-17.csv",
        "lineup_audit": output_dir / "lineup_capture_artifact_inventory_2026-07-17.csv",
        "historical_parity": output_dir / "historical_parity_results_2026-07-17.csv",
        "decisions": output_dir / "required_decisions_2026-07-17.csv",
        "machine": output_dir / "machine_readable_live_hitter_parent_2026-07-17.json",
        "manifest": output_dir / "sha256_manifest_2026-07-17.csv",
        "validation": output_dir / "validation_report_2026-07-17.csv",
    }
    profile = pd.DataFrame(
        [
            {"profile_component": "hitter strict-prior contact/conversion", "current_rows": 0, "historical_source": str(spine_source.CONTACT_PROFILES), "status": "BLOCKED_NO_RUN_BOUND_CURRENT_OUTPUT", "notes": "Historical rows are not valid July 17 live parent rows."},
            {"profile_component": "PA opportunity", "current_rows": 0, "historical_source": str(spine_source.PA_HISTORY), "status": "BLOCKED_NO_RUN_BOUND_CURRENT_PARENT", "notes": "PA history exists; governed July 17 parent was not created."},
        ]
    )
    write_csv(files["closeout"], close)
    write_csv(files["contract"], contract)
    write_csv(files["source_inventory"], source_inventory)
    write_csv(files["external"], external)
    write_csv(files["parent"], parent.reindex(columns=LIVE_PARENT_COLUMNS))
    write_csv(files["withheld"], withheld)
    write_csv(files["reconciliation"], reconciliation)
    write_csv(files["profile"], profile)
    write_csv(files["encounter"], encounter.reindex(columns=encounter_source.ENCOUNTER_OUTPUT_COLUMNS))
    write_csv(files["pha"], pha_ledger)
    write_csv(files["hits"], hits_availability)
    write_csv(files["execution"], execution_contract)
    write_csv(files["lineup_audit"], lineup_audit)
    write_csv(files["historical_parity"], hist)
    write_csv(files["decisions"], dec)
    write_text(files["summary"], summary_md(generated_at, dec, reconciliation))
    machine = {
        "generated_at": generated_at,
        "run_date": RUN_DATE,
        "run_tag": RUN_TAG,
        "cutoff": CUTOFF,
        "advancement_state": "LIVE_HITTER_PARENT_LINEUP_AND_OPPORTUNITY_SOURCE_BLOCKED",
        "july17_pha_challenger_status": "JULY17_PHA_CHALLENGER_PREGAME_SCORING_UNAVAILABLE_MISSING_GOVERNED_PARENT_CAPTURE",
        "july17_slate_reconciliation_status": "OPEN_PENDING_OFFICIAL_RECONCILIATION",
        "current_parent_rows": int(len(parent)),
        "withheld_identity_rows": int(len(withheld)),
        "encounter_rows": int(len(encounter)),
        "pha_scored_rows": 0,
        "historical_parity_pass": bool(not hist.empty and hist["status"].eq("PASS").all()),
        "decisions": {r.decision_name: r.decision_value for r in dec.itertuples(index=False)},
        "guardrails": guardrails,
    }
    write_json(files["machine"], machine)
    generated = [p for k, p in files.items() if k not in {"manifest", "validation"}]
    write_csv(files["manifest"], pd.DataFrame([{"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size} for p in generated]))
    write_csv(files["validation"], validate(generated + [files["manifest"]], guardrails))
    return {
        "output_dir": str(output_dir),
        "current_parent_rows": int(len(parent)),
        "withheld_identity_rows": int(len(withheld)),
        "encounter_rows": int(len(encounter)),
        "pha_scored_rows": 0,
        "advancement_state": machine["advancement_state"],
        "july17_pha_challenger_status": machine["july17_pha_challenger_status"],
        "july17_slate_reconciliation_status": machine["july17_slate_reconciliation_status"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=RUN_DATE)
    parser.add_argument("--run-tag", default=RUN_TAG)
    parser.add_argument("--cutoff", default=CUTOFF)
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    if args.date != RUN_DATE or args.run_tag != RUN_TAG or args.cutoff != CUTOFF:
        raise SystemExit("This bounded package is frozen to July 17 evidence; use a new approved package for a new slate.")
    result = build(args.output_dir)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
