"""Apply factual corrections to the July 12 prediction sentinel audit package.

This is intentionally a correction-layer artifact builder, not a full rerun of
the sentinel audit. It reads the existing tracker-bound package, preserves prior
files as audit history, and writes corrected prediction-vs-execution,
EV-semantics, warning-classification, decision, validation, and checksum
artifacts.
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


AUDIT_DATE = "2026-07-17"
SLATE_DATE = "2026-07-12"
DEFAULT_PACKAGE = Path(
    "artifacts/analysis/model_development/"
    "mlb_july12_favorite_slate_sentinel_failure_audit/2026-07-17"
)
CURTIS_WAGER_ID = 71500


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
        if not fieldnames:
            fieldnames = ["status"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def row_value(row: pd.Series, column: str, default: Any = "") -> Any:
    value = row.get(column, default)
    if pd.isna(value):
        return default
    return value


def build_corrected_ledgers(package: Path) -> dict[str, Any]:
    manifest_path = package / f"sentinel_15_proppadia_manifest_{AUDIT_DATE}.csv"
    settlement_path = package / f"official_settlement_certification_{AUDIT_DATE}.csv"
    warning_path = package / f"pregame_warning_report_{AUDIT_DATE}.csv"
    if not manifest_path.exists():
        raise FileNotFoundError(manifest_path)
    if not settlement_path.exists():
        raise FileNotFoundError(settlement_path)

    manifest = pd.read_csv(manifest_path)
    settlement = pd.read_csv(settlement_path)
    warnings = pd.read_csv(warning_path) if warning_path.exists() else pd.DataFrame()

    prediction_execution_rows: list[dict[str, Any]] = []
    for _, row in manifest.iterrows():
        wager_id = int(row["wager_id"])
        is_curtis = wager_id == CURTIS_WAGER_ID
        prediction_result = "LOSS" if str(row_value(row, "canonical_settlement")).lower() == "loss" else str(row_value(row, "canonical_settlement")).upper()
        execution_status = "LINE_UNAVAILABLE_NOT_WAGERED" if is_curtis else "EXECUTED_WAGER"
        executed_units = 0.0 if is_curtis else float(row_value(row, "stake_units", 0) or 0)
        executed_result = 0.0 if is_curtis else (-executed_units if prediction_result == "LOSS" else float(row_value(row, "unit_result", 0) or 0))
        prediction_execution_rows.append(
            {
                "wager_id": wager_id,
                "slate_date": row_value(row, "slate_date", SLATE_DATE),
                "player_name": row_value(row, "player_name"),
                "game_id": row_value(row, "game_id"),
                "player_id": row_value(row, "player_id"),
                "prop_type": row_value(row, "prop_type"),
                "side": row_value(row, "side"),
                "line": row_value(row, "line"),
                "odds": row_value(row, "odds"),
                "prediction_status": "ISSUED",
                "prediction_result": prediction_result,
                "execution_status": execution_status,
                "units_risked": executed_units,
                "execution_result_units": executed_result,
                "execution_exclusion_reason": "MARKET_LINE_NO_LONGER_AVAILABLE_AT_SELECTION_TIME" if is_curtis else "",
                "prediction_population_included": True,
                "execution_population_included": not is_curtis,
                "source_binding_status": row_value(row, "source_binding_status"),
                "primary_source_section": row_value(row, "primary_source_section"),
                "notes": "Curtis Mead prediction preserved as issued/loss, excluded from realized wager accounting by user correction."
                if is_curtis
                else "Executed wager retained in realized wager accounting.",
            }
        )

    prediction_wins = sum(1 for r in prediction_execution_rows if r["prediction_result"] == "WIN")
    prediction_losses = sum(1 for r in prediction_execution_rows if r["prediction_result"] == "LOSS")
    prediction_pushes = len(prediction_execution_rows) - prediction_wins - prediction_losses
    executed_rows = [r for r in prediction_execution_rows if r["execution_population_included"]]
    executed_wins = sum(1 for r in executed_rows if r["prediction_result"] == "WIN")
    executed_losses = sum(1 for r in executed_rows if r["prediction_result"] == "LOSS")
    executed_pushes = len(executed_rows) - executed_wins - executed_losses
    executed_units = sum(float(r["units_risked"]) for r in executed_rows)
    executed_net = sum(float(r["execution_result_units"]) for r in executed_rows)

    write_csv(package / f"corrected_prediction_vs_execution_ledger_{AUDIT_DATE}.csv", prediction_execution_rows)
    write_csv(
        package / f"curtis_mead_execution_override_{AUDIT_DATE}.csv",
        [r for r in prediction_execution_rows if r["wager_id"] == CURTIS_WAGER_ID],
    )
    certification_rows = [
        {
            "population": "prediction_performance",
            "rows": len(prediction_execution_rows),
            "wins": prediction_wins,
            "losses": prediction_losses,
            "pushes": prediction_pushes,
            "units_risked": "",
            "net_units": "",
            "interpretation": "All issued Proppadia sentinel predictions are evaluated; Curtis Mead remains included as issued/loss.",
        },
        {
            "population": "executed_wager_performance",
            "rows": len(executed_rows),
            "wins": executed_wins,
            "losses": executed_losses,
            "pushes": executed_pushes,
            "units_risked": executed_units,
            "net_units": executed_net,
            "interpretation": "Curtis Mead excluded from realized wagering exposure because the line was unavailable at selection time.",
        },
    ]
    write_csv(package / f"corrected_units_return_certification_{AUDIT_DATE}.csv", certification_rows)

    ev_context_rows = []
    for _, row in manifest.iterrows():
        wager_id = int(row["wager_id"])
        warning_row = warnings[warnings["wager_id"].astype(int).eq(wager_id)] if not warnings.empty and "wager_id" in warnings else pd.DataFrame()
        ev_value = "" if warning_row.empty else row_value(warning_row.iloc[0], "value")
        if wager_id == 71499:
            ev_interpretation = "raw_o05_shell_not_canonical_o15"
            ev_positive_only_survival = "not_counted_as_canonical_o15_ev_positive"
        elif ev_value == "":
            ev_interpretation = "missing_external_tool_context"
            ev_positive_only_survival = "not_surviving"
        else:
            try:
                ev_float = float(ev_value)
            except Exception:
                ev_float = None
            if ev_float is not None and ev_float > 0:
                ev_interpretation = "positive_external_tool_context"
                ev_positive_only_survival = "raw_external_context_survivor"
            elif ev_float is not None and ev_float <= 0:
                ev_interpretation = "negative_external_tool_context"
                ev_positive_only_survival = "not_surviving"
            else:
                ev_interpretation = "unknown_external_tool_context"
                ev_positive_only_survival = "not_surviving"
        ev_context_rows.append(
            {
                "wager_id": wager_id,
                "player_name": row_value(row, "player_name"),
                "recorded_ev_value": ev_value,
                "ev_semantics": "EXTERNAL_TOOL_PRICE_CONTEXT_NOT_PROPPAEDIA_PREDICTION_SIGNAL",
                "canonical_prediction_signal": False,
                "mandatory_rejection_gate_supported": False,
                "ev_interpretation": ev_interpretation,
                "ev_positive_only_survival": ev_positive_only_survival,
                "notes": "EV is preserved as external market context only and is not native Proppadia prediction evidence.",
            }
        )
    write_csv(package / f"ev_context_reclassification_{AUDIT_DATE}.csv", ev_context_rows)

    raw_positive_survivors = sum(
        1
        for r in ev_context_rows
        if r["ev_positive_only_survival"] == "raw_external_context_survivor"
        or r["ev_interpretation"] == "raw_o05_shell_not_canonical_o15"
    )
    canonical_positive_survivors = 0
    ev_summary = [
        {
            "rule": "raw_external_tool_ev_positive_only",
            "candidate_rows_available_in_package": len(ev_context_rows),
            "surviving_rows": raw_positive_survivors,
            "survival_rate": raw_positive_survivors / len(ev_context_rows) if ev_context_rows else "",
            "qualification": "Valdez is the only raw positive value, but it belongs to the O0.5 tracking shell and is not canonical O1.5 evidence.",
        },
        {
            "rule": "canonical_o15_external_tool_ev_positive_only",
            "candidate_rows_available_in_package": len(ev_context_rows),
            "surviving_rows": canonical_positive_survivors,
            "survival_rate": canonical_positive_survivors / len(ev_context_rows) if ev_context_rows else "",
            "qualification": "No broad EV study was run; from this package alone, no canonical O1.5 row has validated positive EV support.",
        },
    ]
    write_csv(package / f"ev_positive_only_survival_summary_{AUDIT_DATE}.csv", ev_summary)

    warning_matrix = build_warning_matrix()
    write_csv(package / f"native_vs_external_warning_classification_{AUDIT_DATE}.csv", warning_matrix)
    write_csv(package / f"revised_warning_evidence_matrix_{AUDIT_DATE}.csv", warning_matrix)

    decisions = {
        "MLB_JULY12_SENTINEL_POPULATION_DECISION": "EXACT_15_PROPPADIA_PREDICTIONS_BOUND_FROM_ATTACHED_TRACKER",
        "MLB_JULY12_PREDICTION_RESULT_DECISION": "CERTIFIED_0_WINS_15_LOSSES",
        "MLB_JULY12_EXECUTION_RESULT_DECISION": "CERTIFIED_0_WINS_14_LOSSES_70_UNITS_RISKED_MINUS_70_UNITS",
        "MLB_JULY12_CURTIS_MEAD_EXECUTION_DECISION": "LINE_UNAVAILABLE_NOT_WAGERED",
        "MLB_JULY12_TRACKER_NORMALIZATION_DECISION": "VALDEZ_71499_NORMALIZED_TO_HITS_OVER_1_5_LINDOR_71494_EXCLUDED_AS_USER_OR_EXTERNAL_SELECTION",
        "MLB_JULY12_EV_SEMANTICS_DECISION": "EXTERNAL_TOOL_PRICE_CONTEXT_NOT_PROPPAEDIA_PREDICTION_SIGNAL",
        "MLB_JULY12_EV_REJECTION_GATE_DECISION": "NOT_SUPPORTED_AS_MANDATORY_GATE",
        "MLB_JULY12_OUTPUT_SECTION_BINDING_DECISION": "FOURTEEN_OF_FIFTEEN_BOUND_TO_JULY12_OUTPUT_SECTIONS_ONE_TRACKER_ROW_UNBOUND_TO_GENERATED_OUTPUT",
        "MLB_JULY12_CONCENTRATION_WARNING_DECISION": "VISIBLE_BUT_NOT_YET_VALIDATED_AS_REJECTION_SIGNAL",
        "MLB_JULY12_REVIEW_AID_DISCOVERY_SURFACE_DECISION": "VISIBLE_BUT_NOT_VALIDATED_AS_POORER_OUTCOME_SEPARATOR_IN_THIS_PACKAGE",
        "MLB_JULY12_NATIVE_PREGAME_SEPARATOR_DECISION": "NO_VALIDATED_PROPPAEDIA_NATIVE_PREGAME_SEPARATOR_IDENTIFIED_FOR_THE_15_FAILURES",
        "MLB_JULY12_REVISED_PREGAME_WARNING_DECISION": "VISIBLE_CONCENTRATION_AND_EXECUTION_AVAILABILITY_ISSUE_NEGATIVE_EV_EXTERNAL_CONTEXT_ONLY_NO_VALIDATED_NATIVE_AVOIDANCE_RULE",
        "MLB_JULY12_CATASTROPHIC_STATE_DECISION": "CERTIFIED_PREDICTION_FAILURE_AVOIDABILITY_NOT_YET_PROVEN",
        "MLB_JULY12_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }
    write_csv(package / f"corrected_decision_report_{AUDIT_DATE}.csv", [{"decision": k, "value": v} for k, v in decisions.items()])

    summary = corrected_summary(
        prediction_rows=len(prediction_execution_rows),
        prediction_losses=prediction_losses,
        executed_rows=len(executed_rows),
        executed_losses=executed_losses,
        executed_units=executed_units,
        executed_net=executed_net,
        raw_positive_survivors=raw_positive_survivors,
        canonical_positive_survivors=canonical_positive_survivors,
    )
    write_md(package / f"corrected_executive_summary_{AUDIT_DATE}.md", summary)

    payload = {
        "audit_date": AUDIT_DATE,
        "correction_generated_at": datetime.now(timezone.utc).isoformat(),
        "prediction_result": {
            "rows": len(prediction_execution_rows),
            "wins": prediction_wins,
            "losses": prediction_losses,
            "pushes": prediction_pushes,
        },
        "execution_result": {
            "rows": len(executed_rows),
            "wins": executed_wins,
            "losses": executed_losses,
            "pushes": executed_pushes,
            "units_risked": executed_units,
            "net_units": executed_net,
        },
        "curtis_mead": {
            "prediction_status": "ISSUED",
            "prediction_result": "LOSS",
            "execution_status": "LINE_UNAVAILABLE_NOT_WAGERED",
            "units_risked": 0,
            "execution_exclusion_reason": "MARKET_LINE_NO_LONGER_AVAILABLE_AT_SELECTION_TIME",
        },
        "ev_semantics": "EXTERNAL_TOOL_PRICE_CONTEXT_NOT_PROPPAEDIA_PREDICTION_SIGNAL",
        "native_pregame_separator_decision": decisions["MLB_JULY12_NATIVE_PREGAME_SEPARATOR_DECISION"],
        "decisions": decisions,
        "constraints": {
            "network_calls": 0,
            "db_writes": 0,
            "oddsapi_calls": 0,
            "model_changes": 0,
            "production_behavior_changes": 0,
            "broad_ev_study": 0,
        },
    }
    write_json(package / f"machine_readable_corrected_july12_sentinel_audit_{AUDIT_DATE}.json", payload)

    write_validation_and_sha(package)
    return payload


def build_warning_matrix() -> list[dict[str, Any]]:
    return [
        {
            "warning_candidate": "prediction_probability_or_score",
            "category": "Proppadia-native warning candidate",
            "visibility": "available_for_some_bound_rows",
            "classification": "NOT_SUPPORTED",
            "evidence": "This correction did not identify a package-level threshold or score rule that separates the 15 failures from successful predictions.",
            "recommended_use": "Do not claim avoidability from score/probability without matched historical evidence.",
        },
        {
            "warning_candidate": "model_rank",
            "category": "Proppadia-native warning candidate",
            "visibility": "not_consistently_recovered",
            "classification": "NOT_SUPPORTED",
            "evidence": "No complete model-rank spine for the exact 15 was established in the existing package.",
            "recommended_use": "Requires separate matched historical rank study.",
        },
        {
            "warning_candidate": "tier_or_candidate_label",
            "category": "Proppadia-native warning candidate",
            "visibility": "visible_for_bound_rows",
            "classification": "VISIBLE_BUT_NOT_VALIDATED",
            "evidence": "Rows carried A/A, A/B, A/C, A/D, A/U, B/B labels, but no exact evidence here proves those surfaces were poorer than comparable alternatives.",
            "recommended_use": "Use as descriptive lineage, not rejection.",
        },
        {
            "warning_candidate": "candidate_section_lineage",
            "category": "Proppadia-native warning candidate",
            "visibility": "visible",
            "classification": "VISIBLE_BUT_NOT_VALIDATED",
            "evidence": "Fourteen rows bound to Alternate Discovery and subsets also appeared in Layered/Watch/Favorite Audit, but discovery/review-aid origin alone was not proven to underperform.",
            "recommended_use": "Track lineage; do not assume review-aid means low quality.",
        },
        {
            "warning_candidate": "same_prop_same_line_same_side_concentration",
            "category": "Proppadia-native warning candidate",
            "visibility": "visible",
            "classification": "VISIBLE_BUT_NOT_VALIDATED",
            "evidence": "All 15 were Hits OVER 1.5, but existing matched-slate/recurrence artifacts are partial and do not prove concentrated slates underperform less-concentrated slates.",
            "recommended_use": "Exposure/risk context; not a mandatory rejection rule yet.",
        },
        {
            "warning_candidate": "game_and_team_exposure_clustering",
            "category": "Proppadia-native warning candidate",
            "visibility": "visible",
            "classification": "VISIBLE_BUT_NOT_VALIDATED",
            "evidence": "The 15 rows clustered into eight games; no validated threshold links that clustering to failure in this package.",
            "recommended_use": "Exposure/risk context; needs matched-slate validation.",
        },
        {
            "warning_candidate": "recent_directional_discrimination",
            "category": "Proppadia-native warning candidate",
            "visibility": "not_established",
            "classification": "NOT_SUPPORTED",
            "evidence": "No recent model-versus-fade or directional-discrimination signal was certified for these rows in this package.",
            "recommended_use": "Requires separate directionality audit.",
        },
        {
            "warning_candidate": "calibration_health",
            "category": "Proppadia-native warning candidate",
            "visibility": "not_established",
            "classification": "NOT_SUPPORTED",
            "evidence": "No row-level or date-level calibration-health fail state was certified in this correction package.",
            "recommended_use": "Do not infer calibration failure from settlement alone.",
        },
        {
            "warning_candidate": "feature_freshness",
            "category": "Proppadia-native warning candidate",
            "visibility": "not_established",
            "classification": "NOT_SUPPORTED",
            "evidence": "No freshness failure was certified for the exact 15 rows.",
            "recommended_use": "Only use if future freshness ledger proves a fail/warn state.",
        },
        {
            "warning_candidate": "fallback_or_missingness_state",
            "category": "Proppadia-native warning candidate",
            "visibility": "partially_visible",
            "classification": "VISIBLE_BUT_NOT_VALIDATED",
            "evidence": "Wyatt Langford had missing starter context and Curtis Mead was unbound to output sections, but no package evidence proves these states separate failure reliably.",
            "recommended_use": "Audit lineage; do not generalize to rejection without validation.",
        },
        {
            "warning_candidate": "lineup_and_starter_certainty",
            "category": "Proppadia-native warning candidate",
            "visibility": "partially_visible",
            "classification": "VISIBLE_BUT_NOT_VALIDATED",
            "evidence": "Starter certainty/context was visible for many bound rows, but no validated rejection separator was established.",
            "recommended_use": "Research context only.",
        },
        {
            "warning_candidate": "collective_tool_ev",
            "category": "External or execution context",
            "visibility": "tracker_context",
            "classification": "EXTERNAL_CONTEXT_ONLY",
            "evidence": "EV values came from an external collective tool, not Proppadia native prediction logic. Negative EV does not prove Proppadia knew the predictions were bad.",
            "recommended_use": "Preserve as price context; do not impose mandatory EV-positive gate.",
        },
        {
            "warning_candidate": "market_price",
            "category": "External or execution context",
            "visibility": "visible",
            "classification": "EXTERNAL_CONTEXT_ONLY",
            "evidence": "Prices were visible, but no native threshold or price-based separator was validated here.",
            "recommended_use": "Price context only unless separately validated.",
        },
        {
            "warning_candidate": "curtis_mead_line_disappearance",
            "category": "External or execution context",
            "visibility": "execution_time",
            "classification": "EXECUTION_ONLY",
            "evidence": "User correction states the O1.5 line was unavailable at wager-selection time; this excludes realized exposure but does not invalidate the issued prediction.",
            "recommended_use": "Separate prediction evaluation from execution accounting.",
        },
        {
            "warning_candidate": "official_zero_to_fifteen_settlement",
            "category": "Postgame result",
            "visibility": "postgame",
            "classification": "HINDSIGHT_ONLY",
            "evidence": "The 0-15 result is certified after settlement and cannot be used as pregame evidence.",
            "recommended_use": "Use for outcome certification only.",
        },
    ]


def corrected_summary(
    prediction_rows: int,
    prediction_losses: int,
    executed_rows: int,
    executed_losses: int,
    executed_units: float,
    executed_net: float,
    raw_positive_survivors: int,
    canonical_positive_survivors: int,
) -> str:
    return f"""
# Corrected MLB July 12 Proppadia Prediction Sentinel Failure Audit

- Audit date: `{AUDIT_DATE}`
- Correction scope: prediction-versus-execution accounting, Curtis Mead execution status, EV semantics, and pregame-warning qualification.
- Production change status: `NOT_AUTHORIZED`

## Corrected Results

Prediction performance remains certified as a complete Proppadia prediction failure:

- Proppadia predictions: `{prediction_rows}`
- Prediction wins: `0`
- Prediction losses: `{prediction_losses}`
- Prediction pushes: `0`

Executed wager performance is corrected separately:

- Executed wagers: `{executed_rows}`
- Executed wins: `0`
- Executed losses: `{executed_losses}`
- Executed pushes: `0`
- Units risked: `{executed_units:g}`
- Net result: `{executed_net:g}`

Curtis Mead remains in the 15-row prediction population as `prediction_status=ISSUED`
and `prediction_result=LOSS`, but is excluded from realized wager accounting as
`execution_status=LINE_UNAVAILABLE_NOT_WAGERED` with `units_risked=0`.

## EV Semantics Correction

The recorded EV values are reclassified as
`EXTERNAL_TOOL_PRICE_CONTEXT_NOT_PROPPAEDIA_PREDICTION_SIGNAL`. They are preserved
as context, but they were not generated by Proppadia and were not part of the
native candidate-generation logic. Negative external-tool EV is therefore not
proof that Proppadia knew these predictions were bad before play.

From this package alone, a raw external-tool EV-positive-only rule would have
left `{raw_positive_survivors}` row, and that row is Valdez's noncanonical O0.5
tracker shell rather than validated O1.5 EV evidence. A canonical O1.5
EV-positive-only interpretation leaves `{canonical_positive_survivors}` rows.
This correction does not run a broad EV study and does not recommend an
EV-positive mandatory gate; such a rule would materially change the wager
selection mandate and reduce opportunity volume.

## Corrected Pregame-Warning Answer

The July 12 slate displayed visible same-direction concentration and one
execution-availability problem, but the audit has not identified a validated
Proppadia-native pregame rule that reliably distinguishes these fifteen failures
from successful predictions. Negative collective-tool EV was external price
context, not a native prediction warning.

Visible before play:

- all issued predictions were Hits OVER 1.5;
- the slate was clustered into eight games;
- most bound rows appeared through review-aid/discovery surfaces;
- Curtis Mead became an execution-availability problem at selection time;
- external EV and price context existed outside Proppadia's native signal.

Validated as reliable separators in this package:

- none.

External or execution-only context:

- collective-tool EV;
- book/line availability;
- Curtis Mead's unavailable line at selection time.

Known only after settlement:

- the official 0-15 prediction result.

## Governance Note

The prior reopened artifacts are preserved as audit history. The corrected
artifacts should be used for denominator, execution accounting, EV semantics, and
pregame-warning claims.
"""


def write_validation_and_sha(package: Path) -> None:
    generated_names = [
        f"corrected_prediction_vs_execution_ledger_{AUDIT_DATE}.csv",
        f"curtis_mead_execution_override_{AUDIT_DATE}.csv",
        f"corrected_units_return_certification_{AUDIT_DATE}.csv",
        f"ev_context_reclassification_{AUDIT_DATE}.csv",
        f"ev_positive_only_survival_summary_{AUDIT_DATE}.csv",
        f"native_vs_external_warning_classification_{AUDIT_DATE}.csv",
        f"revised_warning_evidence_matrix_{AUDIT_DATE}.csv",
        f"corrected_decision_report_{AUDIT_DATE}.csv",
        f"corrected_executive_summary_{AUDIT_DATE}.md",
        f"machine_readable_corrected_july12_sentinel_audit_{AUDIT_DATE}.json",
    ]
    validation = []
    for name in generated_names:
        path = package / name
        if not path.exists():
            validation.append({"artifact": str(path), "validation": "exists", "status": "FAIL", "message": "missing"})
            continue
        if path.suffix == ".csv":
            try:
                pd.read_csv(path)
                status, message = "PASS", "csv_parses"
            except Exception as exc:
                status, message = "FAIL", f"{type(exc).__name__}: {exc}"
            validation.append({"artifact": str(path), "validation": "csv_parse", "status": status, "message": message})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                status, message = "PASS", "json_parses"
            except Exception as exc:
                status, message = "FAIL", f"{type(exc).__name__}: {exc}"
            validation.append({"artifact": str(path), "validation": "json_parse", "status": status, "message": message})
        elif path.suffix == ".md":
            status = "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL"
            validation.append({"artifact": str(path), "validation": "markdown_nonempty", "status": status, "message": status})
    validation.append(
        {
            "artifact": "runtime",
            "validation": "correction_guardrails",
            "status": "PASS",
            "message": "correction-layer only; no network/db/OddsAPI/model/feature/production changes; no broad EV study",
        }
    )
    write_csv(package / f"corrected_validation_report_{AUDIT_DATE}.csv", validation)

    rows = []
    for name in generated_names + [f"corrected_validation_report_{AUDIT_DATE}.csv"]:
        path = package / name
        if path.exists():
            rows.append({"path": str(path), "size_bytes": path.stat().st_size, "sha256": sha256_path(path)})
    write_csv(package / f"corrected_sha256_manifest_{AUDIT_DATE}.csv", rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-dir", default=str(DEFAULT_PACKAGE))
    args = parser.parse_args()
    payload = build_corrected_ledgers(Path(args.package_dir))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
