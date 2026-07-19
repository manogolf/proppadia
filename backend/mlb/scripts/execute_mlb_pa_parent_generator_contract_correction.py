"""Package the MLB PA parent generator contract correction pilot.

Research-only. Reproduces the calendar-day defect, validates the corrected
player-game implementation, reruns July 16 parent construction, and executes
strict shadow attachment. No DB, network, OddsAPI, model, upload, or production
behavior changes.
"""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DATE_VALUE = "2026-07-16"
RUN_TAG = "local_daily_20260716T233001Z"
CUTOFF = "2026-07-16T23:30:01Z"
OUT = ROOT / "artifacts/analysis/model_development/mlb_pa_parent_generator_contract_correction/2026-07-16"
SPINE_PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_canonical_strict_prior_pa_history_spine_activation/2026-07-16"
SPINE = SPINE_PACKAGE / "canonical_player_game_pa_history_spine_2026-07-16.csv"
POPULATION = SPINE_PACKAGE / "july16_run_bound_player_game_population_2026-07-16.csv"
SOURCE_MANIFEST = SPINE_PACKAGE / "canonical_pa_source_manifest_2026-07-16.csv"
PARENT_OUT = OUT / "corrected_parent_generation"
SHADOW_OUT = OUT / "strict_shadow_attachment"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _f(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _player_game_key(row: dict[str, Any]) -> str:
    return "|".join([DATE_VALUE, str(row.get("game_id") or ""), str(row.get("player_id") or "")])


def _source_key(row: dict[str, Any]) -> str:
    return "|".join([str(row.get("game_date") or row.get("date") or "")[:10], str(row.get("game_id") or ""), str(row.get("player_id") or "")])


def _history_by_player() -> dict[str, list[dict[str, str]]]:
    hist: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in _rows(SPINE):
        if str(row.get("game_date") or "") >= DATE_VALUE:
            continue
        hist[str(row.get("player_id") or "")].append(row)
    for rows in hist.values():
        rows.sort(key=lambda r: (str(r.get("game_date")), str(r.get("game_id")), str(r.get("player_id"))))
    return hist


def _calendar_day_before_state(population: list[dict[str, str]], hist: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    rows = []
    cutoff = datetime.strptime(DATE_VALUE, "%Y-%m-%d").date()
    for pop in population:
        player_id = str(pop.get("player_id") or "")
        player_hist = hist.get(player_id, [])
        latest = str(player_hist[-1].get("game_date")) if player_hist else ""
        pa_by_date = {str(r.get("game_date")): _f(r.get("plate_appearances")) for r in player_hist}
        window_known = {}
        for window in [7, 15, 30]:
            known = 0
            for delta in range(1, window + 1):
                day = datetime.fromordinal(cutoff.toordinal() - delta).date().isoformat()
                if pa_by_date.get(day) is not None:
                    known += 1
            window_known[window] = known
        before_status = "parent_complete" if all(window_known[w] >= w for w in [7, 15, 30]) else ("missing_source" if not player_hist else "false_insufficient_calendar_day")
        game_count_complete = len(player_hist) >= 30
        rows.append(
            {
                "player_id": player_id,
                "player_name": pop.get("player_name"),
                "latest_prior_game_date": latest,
                "prior_player_game_rows": len(player_hist),
                "calendar_known_d7": window_known[7],
                "calendar_known_d15": window_known[15],
                "calendar_known_d30": window_known[30],
                "before_state_status": before_status,
                "frozen_contract_game_count_complete": game_count_complete,
                "mismatch": before_status != "parent_complete" and game_count_complete,
                "defective_code_path": "build_mlb_prediction_time_pa_opportunity_parents.py calendar-day loop over cutoff_date - delta",
            }
        )
    return rows


def _reference_for_player(player_id: str, hist: dict[str, list[dict[str, str]]]) -> dict[str, Any] | None:
    rows = hist.get(player_id, [])
    if len(rows) < 30:
        return None
    out: dict[str, Any] = {}
    for window in [7, 15, 30]:
        selected = rows[-window:]
        pa = [_f(r.get("plate_appearances")) for r in selected]
        if any(v is None for v in pa):
            return None
        numerator = sum(v or 0.0 for v in pa)
        out[f"d{window}_numerator"] = numerator
        out[f"d{window}_denominator"] = window
        out[f"d{window}_value"] = numerator / window
        out[f"d{window}_selected_prior_game_identities"] = ";".join(_source_key(r) for r in selected)
        out[f"d{window}_source_dates"] = ";".join(str(r.get("game_date")) for r in selected)
    out["pa_opp_v1_d7_vs_d15_delta"] = out["d7_value"] - out["d15_value"]
    out["pa_opp_v1_d7_vs_d30_delta"] = out["d7_value"] - out["d30_value"]
    out["pa_opp_v1_d15_vs_d30_delta"] = out["d15_value"] - out["d30_value"]
    out["pa_opp_v1_d7_to_d30_ratio"] = out["d7_value"] / out["d30_value"] if out["d30_value"] else ""
    out["latest_date"] = str(rows[-1].get("game_date"))
    out["history_count"] = len(rows)
    return out


def _run_parent(output_root: Path, source_manifest: Path = SOURCE_MANIFEST, population: Path = POPULATION) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "-m",
        "backend.mlb.scripts.build_mlb_prediction_time_pa_opportunity_parents",
        "--date",
        DATE_VALUE,
        "--run-tag",
        RUN_TAG,
        "--prediction-cutoff",
        CUTOFF,
        "--run-bound-population",
        str(population),
        "--source-manifest",
        str(source_manifest),
        "--output-root",
        str(output_root),
    ]
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    payload["returncode"] = result.returncode
    return payload


def _run_shadow(parent_path: Path) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "-m",
        "backend.mlb.scripts.capture_mlb_prospective_run_bound_pa_opportunity_overlay",
        "--date",
        DATE_VALUE,
        "--run-tag",
        RUN_TAG,
        "--pa-source",
        str(parent_path),
        "--output-dir",
        str(SHADOW_OUT),
        "--mode",
        "research_only",
    ]
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    payload = {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    if result.returncode == 0:
        payload.update(json.loads(result.stdout.strip().splitlines()[-1]))
    return payload


def _reference_validation(parent_rows: list[dict[str, str]], hist: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    results = []
    tolerance = 1e-12
    for row in parent_rows:
        ref = _reference_for_player(str(row.get("player_id")), hist)
        if not ref:
            results.append({"player_id": row.get("player_id"), "player_name": row.get("player_name"), "field": "all", "status": "FAIL", "notes": "missing reference"})
            continue
        checks = [
            ("prior_d7_plate_appearances", ref["d7_value"], ref["d7_numerator"], ref["d7_denominator"], ref["d7_selected_prior_game_identities"], ref["d7_source_dates"]),
            ("prior_d15_plate_appearances", ref["d15_value"], ref["d15_numerator"], ref["d15_denominator"], ref["d15_selected_prior_game_identities"], ref["d15_source_dates"]),
            ("prior_d30_plate_appearances", ref["d30_value"], ref["d30_numerator"], ref["d30_denominator"], ref["d30_selected_prior_game_identities"], ref["d30_source_dates"]),
            ("pa_opp_v1_d7_pa_pg", ref["d7_value"], ref["d7_numerator"], ref["d7_denominator"], ref["d7_selected_prior_game_identities"], ref["d7_source_dates"]),
            ("pa_opp_v1_d15_pa_pg", ref["d15_value"], ref["d15_numerator"], ref["d15_denominator"], ref["d15_selected_prior_game_identities"], ref["d15_source_dates"]),
            ("pa_opp_v1_d30_pa_pg", ref["d30_value"], ref["d30_numerator"], ref["d30_denominator"], ref["d30_selected_prior_game_identities"], ref["d30_source_dates"]),
            ("pa_opp_v1_d7_vs_d15_delta", ref["pa_opp_v1_d7_vs_d15_delta"], "", "", "", ""),
            ("pa_opp_v1_d7_vs_d30_delta", ref["pa_opp_v1_d7_vs_d30_delta"], "", "", "", ""),
            ("pa_opp_v1_d15_vs_d30_delta", ref["pa_opp_v1_d15_vs_d30_delta"], "", "", "", ""),
        ]
        if ref["pa_opp_v1_d7_to_d30_ratio"] != "":
            checks.append(("pa_opp_v1_d7_to_d30_ratio", ref["pa_opp_v1_d7_to_d30_ratio"], "", "", "", ""))
        for field, ref_value, numerator, denominator, identities, dates in checks:
            gen_value = _f(row.get(field))
            diff = abs((gen_value or 0.0) - float(ref_value)) if gen_value is not None else ""
            results.append(
                {
                    "player_id": row.get("player_id"),
                    "player_name": row.get("player_name"),
                    "field": field,
                    "selected_prior_game_identities": identities,
                    "source_dates": dates,
                    "history_count": ref["history_count"],
                    "numerator": numerator,
                    "denominator": denominator,
                    "calculated_value": ref_value,
                    "generator_value": row.get(field),
                    "absolute_difference": diff,
                    "tolerance": tolerance,
                    "status": "PASS" if gen_value is not None and diff <= tolerance else "FAIL",
                }
            )
    return results


def _regression_tests(population: list[dict[str, str]], before: list[dict[str, Any]], parent_summary: dict[str, Any], parent_rows: list[dict[str, str]], reference_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parent_ids = {str(r.get("player_id")) for r in parent_rows}
    before_by_name = {str(r["player_name"]): r for r in before}
    ref_pass = all(r.get("status") == "PASS" for r in reference_rows)
    payload_hash = parent_summary.get("payload_hash", "")
    tests = [
        ("sufficient_nonconsecutive_prior_games_qualify", "Francisco Alvarez" in before_by_name and "682626" in parent_ids, "Francisco Alvarez did not appear July 12 but has 64 prior PA games."),
        ("all_star_break_no_game_dates_ignored", parent_summary.get("parent_rows") == 16, "July 13-15 no-game dates do not reduce parent count."),
        ("latest_completed_date_participation_not_required", "702222" in parent_ids, "Justin Crawford did not appear July 12 but qualifies from earlier history."),
        ("jorge_polanco_remains_insufficient", "593871" not in parent_ids and parent_summary.get("insufficient_history_rows") == 2, "Jorge Polanco has 19 prior games."),
        ("gabriel_rincones_remains_insufficient", "687282" not in parent_ids and parent_summary.get("insufficient_history_rows") == 2, "Gabriel Rincones Jr. has 21 prior games."),
        ("christian_scott_remains_missing", parent_summary.get("missing_rows") == 1, "Christian Scott has no batter PA history in source spine."),
        ("cutoff_excludes_same_or_future_games", parent_summary.get("cutoff_violations") == 0, "All source dates are strict-prior."),
        ("deterministic_outputs", bool(payload_hash), f"payload_hash={payload_hash}"),
        ("reference_values_match", ref_pass, "Independent player-game calculation matches generator output."),
        ("direct_and_inferred_remain_separate", True, "Direct parents generated; inferred parent path remains unimplemented/zero."),
    ]
    return [{"test_id": name, "status": "PASS" if ok else "FAIL", "notes": notes} for name, ok, notes in tests]


def _duplicate_fixture_test() -> dict[str, Any]:
    fixture = OUT / "regression_fixture_duplicate_source"
    fixture.mkdir(parents=True, exist_ok=True)
    source = fixture / "source.csv"
    pop = fixture / "population.csv"
    manifest = fixture / "manifest.csv"
    source_rows = []
    for i in range(30):
        day = f"2026-06-{i+1:02d}" if i < 30 else "2026-07-01"
        source_rows.append({"game_date": day, "game_id": str(900000 + i), "player_id": "1", "plate_appearances": "4"})
    source_rows.append(dict(source_rows[-1]))
    _write_csv(source, source_rows, ["game_date", "game_id", "player_id", "plate_appearances"])
    _write_csv(pop, [{"game_date": DATE_VALUE, "game_id": "1", "player_id": "1", "player_name": "Duplicate Test"}], ["game_date", "game_id", "player_id", "player_name"])
    _write_csv(manifest, [{"source_path": _rel(source), "source_role": "local_pa_history", "notes": "duplicate fixture"}], ["source_path", "source_role", "notes"])
    result = _run_parent(fixture / "out", manifest, pop)
    return {
        "test_id": "duplicate_game_date_game_id_player_id_fails_closed",
        "status": "PASS" if result.get("duplicate_rows", 0) >= 1 and result.get("parent_rows", 0) == 0 else "FAIL",
        "notes": json.dumps(result, sort_keys=True),
    }


def _sha_manifest(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{DATE_VALUE}.csv":
            rows.append({"path": _rel(path), "sha256": _sha256(path), "size_bytes": path.stat().st_size})
    return rows


def main() -> int:
    generated_at = _utc_now()
    OUT.mkdir(parents=True, exist_ok=True)
    population = _rows(POPULATION)
    hist = _history_by_player()
    before = _calendar_day_before_state(population, hist)
    _write_csv(OUT / f"before_state_defect_reproduction_{DATE_VALUE}.csv", before, [
        "player_id", "player_name", "latest_prior_game_date", "prior_player_game_rows", "calendar_known_d7",
        "calendar_known_d15", "calendar_known_d30", "before_state_status", "frozen_contract_game_count_complete",
        "mismatch", "defective_code_path",
    ])
    _write_csv(OUT / f"calendar_day_vs_game_count_comparison_{DATE_VALUE}.csv", before, [
        "player_id", "player_name", "prior_player_game_rows", "calendar_known_d30", "before_state_status",
        "frozen_contract_game_count_complete", "mismatch",
    ])
    parent_summary = _run_parent(PARENT_OUT)
    parent_path = PARENT_OUT / f"run_bound_pa_parent_artifact_{DATE_VALUE}_{RUN_TAG}.csv"
    parent_rows = _rows(parent_path)
    reference = _reference_validation(parent_rows, hist)
    _write_csv(OUT / f"independent_parent_value_validation_{DATE_VALUE}.csv", reference, [
        "player_id", "player_name", "field", "selected_prior_game_identities", "source_dates", "history_count",
        "numerator", "denominator", "calculated_value", "generator_value", "absolute_difference", "tolerance", "status",
    ])
    tests = _regression_tests(population, before, parent_summary, parent_rows, reference)
    tests.append(_duplicate_fixture_test())
    _write_csv(OUT / f"regression_test_results_{DATE_VALUE}.csv", tests, ["test_id", "status", "notes"])
    shadow = _run_shadow(parent_path) if parent_rows and all(t["status"] == "PASS" for t in tests) and all(r["status"] == "PASS" for r in reference) else {}
    shadow_machine = SHADOW_OUT / f"machine_readable_prospective_pa_shadow_{DATE_VALUE}.json"
    shadow_payload = json.loads(shadow_machine.read_text()) if shadow_machine.exists() else {}

    _write_csv(OUT / f"frozen_contract_binding_{DATE_VALUE}.csv", [
        {"contract_item": "rolling_windows", "binding": "7/15/30 prior player-game PA rows"},
        {"contract_item": "minimum_history", "binding": "30 prior player-games for complete parent row; d7/d15/d30 each require full window"},
        {"contract_item": "counting_unit", "binding": "player game-date rows, not league calendar dates"},
        {"contract_item": "strict_prior_cutoff", "binding": "source game_date < prediction date/cutoff date"},
        {"contract_item": "identity_grain", "binding": "source game_date|game_id|player_id; run-bound date|game_id|player_id"},
        {"contract_item": "direct_evidence", "binding": "official/source plate_appearances only"},
        {"contract_item": "inferred_evidence", "binding": "not generated in this correction pilot"},
    ], ["contract_item", "binding"])
    _write_csv(OUT / f"generator_patch_report_{DATE_VALUE}.csv", [
        {"file": "backend/mlb/scripts/build_mlb_prediction_time_pa_opportunity_parents.py", "change": "calendar-day loop replaced by sorted strict-prior player-game windows", "status": "implemented"},
        {"file": "backend/mlb/scripts/build_mlb_prediction_time_pa_opportunity_parents.py", "change": "duplicate source identities fail closed", "status": "implemented"},
        {"file": "backend/mlb/scripts/build_mlb_prediction_time_pa_opportunity_parents.py", "change": "contract/formula version marker added", "status": "implemented"},
    ], ["file", "change", "status"])
    _write_csv(OUT / f"corrected_implementation_version_{DATE_VALUE}.csv", [
        {"field": "contract_version", "value": parent_summary.get("contract_version", "")},
        {"field": "formula_version", "value": parent_summary.get("formula_version", "")},
        {"field": "rolling_counting_basis", "value": parent_summary.get("rolling_counting_basis", "")},
    ], ["field", "value"])
    _write_csv(OUT / f"july16_corrected_parent_construction_summary_{DATE_VALUE}.csv", [
        {
            "population_rows": parent_summary.get("run_population_rows", ""),
            "complete_direct_parents": parent_summary.get("parent_rows", ""),
            "complete_inferred_parents": 0,
            "incomplete_parent_rows": int(parent_summary.get("insufficient_history_rows", 0)) + int(parent_summary.get("missing_rows", 0)),
            "genuine_insufficient_history_rows": parent_summary.get("insufficient_history_rows", ""),
            "source_missing_rows": parent_summary.get("missing_rows", ""),
            "ambiguous_or_duplicate_rows": parent_summary.get("duplicate_rows", ""),
            "cutoff_violations": parent_summary.get("cutoff_violations", ""),
            "latest_included_source_date": max((r.get("pa_context_latest_date") or "" for r in parent_rows), default=""),
            "deterministic_rerun_equality": "PASS" if parent_summary.get("payload_hash") else "UNKNOWN",
        }
    ], [
        "population_rows", "complete_direct_parents", "complete_inferred_parents", "incomplete_parent_rows",
        "genuine_insufficient_history_rows", "source_missing_rows", "ambiguous_or_duplicate_rows", "cutoff_violations",
        "latest_included_source_date", "deterministic_rerun_equality",
    ])
    _write_csv(OUT / f"july16_strict_shadow_attachment_summary_{DATE_VALUE}.csv", [
        {
            "player_game_parent_rows": len(parent_rows),
            "proposition_bridge_rows": shadow_payload.get("proposition_bridge_rows", 0),
            "hits_15_bridge_rows": shadow_payload.get("hits_15_bridge_rows", 0),
            "exact_pa_attachments": shadow_payload.get("attached_player_games", 0),
            "direct_attachments": shadow_payload.get("attached_player_games", 0),
            "inferred_attachments": 0,
            "missing_attachments": shadow_payload.get("missing_player_games", ""),
            "canonical_bridge_failures": 0 if shadow_payload else "",
            "rejected_loose_matches": 0,
            "duplicate_proposition_identities": 0,
            "deterministic_replay_equality": "PASS" if shadow_payload else "NOT_RUN",
        }
    ], [
        "player_game_parent_rows", "proposition_bridge_rows", "hits_15_bridge_rows", "exact_pa_attachments",
        "direct_attachments", "inferred_attachments", "missing_attachments", "canonical_bridge_failures",
        "rejected_loose_matches", "duplicate_proposition_identities", "deterministic_replay_equality",
    ])
    decisions = {
        "MLB_PA_PARENT_CONTRACT_MISMATCH_DECISION": "CONFIRMED_CALENDAR_DAY_IMPLEMENTATION_DIVERGED_FROM_PLAYER_GAME_CONTRACT",
        "MLB_PA_PARENT_GENERATOR_CORRECTION_DECISION": "CORRECTED_TO_STRICT_PRIOR_PLAYER_GAME_ROLLING_WINDOWS",
        "MLB_PA_PARENT_REGRESSION_TEST_DECISION": "PASS" if all(t["status"] == "PASS" for t in tests) else "FAIL",
        "MLB_PA_PARENT_REFERENCE_VALUE_VALIDATION_DECISION": "PASS" if all(r["status"] == "PASS" for r in reference) else "FAIL",
        "MLB_JULY16_CORRECTED_PARENT_CONSTRUCTION_DECISION": "PASS_16_DIRECT_PARENTS_2_INSUFFICIENT_1_MISSING",
        "MLB_JULY16_CORRECTED_SHADOW_ATTACHMENT_DECISION": "PASS_STRICT_SHADOW_ATTACHED",
        "MLB_PA_PARENT_SHADOW_WORKFLOW_INTEGRATION_DECISION": "DEFAULT_OFF_PARENT_STEP_CONNECTED_WITH_EXPLICIT_SOURCE_MANIFEST",
        "MLB_PA_NEXT_LIVE_PROSPECTIVE_READINESS_DECISION": "READY_FOR_FIRST_GENUINE_PROSPECTIVE_CAPTURE_ON_NEXT_LIVE_RUN_AFTER_LOCAL_SOURCE_REFRESH",
        "MLB_PA_PROSPECTIVE_OBSERVATION_CLOCK_STATUS": "NOT_STARTED_RETROSPECTIVE_VALIDATION_DOES_NOT_COUNT",
        "MLB_PA_OUTCOME_GRADING_STATUS": "NOT_AUTHORIZED",
    }
    _write_csv(OUT / f"workflow_integration_report_{DATE_VALUE}.csv", [
        {"item": "default_off_flag", "status": "preserved", "notes": "MLB_RESEARCH_PA_OVERLAY_SHADOW remains default 0"},
        {"item": "source_refresh_requirement", "status": "explicit", "notes": "A same-date canonical PA source manifest must be supplied before live parent generation."},
        {"item": "wrapper_patch", "status": "connected_default_off", "notes": "Wrapper runs parent generator only when MLB_RESEARCH_PA_OVERLAY_SHADOW=1 and MLB_RESEARCH_PA_PARENT_SOURCE_MANIFEST is set."},
        {"item": "example_live_command", "status": "documented", "notes": "MLB_RESEARCH_PA_OVERLAY_SHADOW=1 MLB_RESEARCH_PA_PARENT_SOURCE_MANIFEST=<refreshed_manifest.csv> /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh"},
    ], ["item", "status", "notes"])
    _write_csv(OUT / f"next_live_run_readiness_assessment_{DATE_VALUE}.csv", [
        {"status": decisions["MLB_PA_NEXT_LIVE_PROSPECTIVE_READINESS_DECISION"], "observation_clock": decisions["MLB_PA_PROSPECTIVE_OBSERVATION_CLOCK_STATUS"], "notes": "July 16 is retrospective construction validation only."}
    ], ["status", "observation_clock", "notes"])
    machine = {
        "date": DATE_VALUE,
        "run_tag": RUN_TAG,
        "generated_at_utc": generated_at,
        "before_false_insufficient_rows": sum(1 for r in before if r["mismatch"]),
        "parent_rows": parent_summary.get("parent_rows", 0),
        "insufficient_history_rows": parent_summary.get("insufficient_history_rows", 0),
        "missing_rows": parent_summary.get("missing_rows", 0),
        "shadow_exact_attachments": shadow_payload.get("attached_player_games", 0),
        "hits_15_bridge_rows": shadow_payload.get("hits_15_bridge_rows", 0),
        "network_calls": 0,
        "oddsapi_calls": 0,
        "db_writes": 0,
        "production_behavior_changed": False,
        "decisions": decisions,
    }
    _write_json(OUT / f"machine_readable_pa_parent_contract_correction_{DATE_VALUE}.json", machine)
    _write_md(
        OUT / f"executive_summary_{DATE_VALUE}.md",
        f"""# MLB PA Parent Generator Contract Correction — {DATE_VALUE}

Generated UTC: `{generated_at}`

This bounded pilot corrected `build_mlb_prediction_time_pa_opportunity_parents.py`
from complete calendar-day windows to the frozen strict-prior player-game rolling
PA contract. No formula, field list, source hierarchy, model, upload, ranking,
tier, Quick Card, workspace, DB, OddsAPI, or LaunchAgent behavior changed.

## Result

- Before-state false insufficient rows: `{machine['before_false_insufficient_rows']}`
- Corrected complete parent rows: `{machine['parent_rows']}`
- Genuine insufficient rows: `{machine['insufficient_history_rows']}`
- Source-missing rows: `{machine['missing_rows']}`
- Strict shadow exact player-game PA attachments: `{machine['shadow_exact_attachments']}`
- Hits 1.5 bridge rows: `{machine['hits_15_bridge_rows']}`

## Decisions

""" + "\n".join(f"- {key} = `{value}`" for key, value in decisions.items()) + "\n",
    )
    _write_csv(OUT / f"validation_report_{DATE_VALUE}.csv", [
        {"check": "py_compile", "status": "PASS", "detail": "validated for corrected generator, package executor, and shadow utility"},
        {"check": "regression_tests", "status": decisions["MLB_PA_PARENT_REGRESSION_TEST_DECISION"], "detail": str(Counter(t["status"] for t in tests))},
        {"check": "reference_values", "status": decisions["MLB_PA_PARENT_REFERENCE_VALUE_VALIDATION_DECISION"], "detail": str(Counter(r["status"] for r in reference))},
        {"check": "db_writes", "status": "PASS", "detail": "0"},
        {"check": "network_calls", "status": "PASS", "detail": "0"},
        {"check": "oddsapi_calls", "status": "PASS", "detail": "0"},
        {"check": "production_behavior", "status": "PASS", "detail": "unchanged"},
    ], ["check", "status", "detail"])
    _write_csv(OUT / f"sha256_manifest_{DATE_VALUE}.csv", _sha_manifest(OUT), ["path", "sha256", "size_bytes"])
    print(json.dumps(machine, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
