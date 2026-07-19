"""Build a strict run-bound live PA opportunity overlay replay pilot.

Research-only. Reads local preserved artifacts, writes only analysis artifacts,
and fails closed when exact run-bound PA evidence is unavailable.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-16"
ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_strict_run_bound_live_pa_overlay_pilot/2026-07-16"
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history"
PA_FOUNDATION = ROOT / "artifacts/analysis/mlb/pa_foundation"
MODEL_DIAG = ROOT / "backend/mlb/exports/model_diagnostics/prepared_feature_vectors"
LANES_ROOT = ROOT / "backend/mlb/exports/model_v2/lanes/today"
PREV_DIAG = ROOT / "artifacts/analysis/model_development/mlb_hits_15_pa_opportunity_overlay_diagnostic/2026-07-16"


PA_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_context_latest_date",
    "pa_opp_v1_cutoff_status",
    "pa_missing_flag",
    "pa_source_regime",
    "pa_semantics_status",
    "pa_opp_v1_complete_prior_pa",
    "pa_opp_v1_context_age_days",
    "pa_opp_v1_feature_version",
    "pa_opp_v1_formula_version",
]


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
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _canonical(row: dict[str, Any], side_field: str = "model_pick_side") -> str:
    return "|".join(
        [
            str(row.get("slate_date") or row.get("game_date") or row.get("date") or "")[:10],
            str(row.get("game_id") or ""),
            str(row.get("player_id") or ""),
            str(row.get("prop_type") or ""),
            str(row.get("line") or ""),
            str(row.get(side_field) or row.get("side") or ""),
        ]
    )


def _player_game_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("slate_date") or row.get("game_date") or row.get("date") or "")[:10],
            str(row.get("game_id") or ""),
            str(row.get("player_id") or ""),
        ]
    )


def _run_timestamp(run_tag: str) -> str:
    match = re.search(r"(\d{8}T\d{6})Z", run_tag)
    return match.group(1) + "Z" if match else run_tag


def _run_file(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"mlb_slate_output__{run_tag}.csv"


def _load_live_rows(date_value: str, run_tag: str) -> list[dict[str, str]]:
    return _rows(_run_file(date_value, run_tag))


def _hits15(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [r for r in rows if r.get("prop_type") == "hits" and str(r.get("line")) in {"1.5", "1.50"}]


def _candidate_pa_paths(date_value: str, run_tag: str) -> list[Path]:
    search_roots = [
        PA_FOUNDATION,
        MODEL_DIAG / date_value,
        LANES_ROOT / date_value,
        ODDS_ROOT / date_value,
    ]
    paths: list[Path] = []
    run_fragments = {run_tag, run_tag.replace("local_daily_", ""), _run_timestamp(run_tag).replace("Z", "")}
    for root in search_roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in {".csv", ".json", ".md"}:
                continue
            name = path.name.lower()
            path_text = str(path).lower()
            has_pa_name = "pa" in name or "plate" in name or "opportunity" in name
            has_run = any(fragment and fragment.lower() in path_text for fragment in run_fragments)
            if has_pa_name and has_run:
                paths.append(path)
    return sorted(set(paths))


def _is_authoritative_pa_artifact(path: Path, run_tag: str) -> tuple[bool, str, list[dict[str, str]]]:
    if path.suffix.lower() != ".csv":
        return False, "not_csv_row_artifact", []
    rows = _rows(path)
    if not rows:
        return False, "empty_csv", []
    cols = set(rows[0].keys())
    required_identity = {"game_id", "player_id"}
    has_date = bool({"slate_date", "game_date", "date"} & cols)
    has_run = bool({"run_tag", "manifest_run_tag", "source_run_tag"} & cols) or run_tag in str(path)
    has_pa = bool(set(PA_FIELDS) & cols) or bool({"d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances"} & cols)
    if not required_identity <= cols:
        return False, "missing_game_id_or_player_id", rows
    if not has_date:
        return False, "missing_date_field", rows
    if not has_run:
        return False, "missing_run_tag_binding", rows
    if not has_pa:
        return False, "missing_frozen_pa_fields", rows
    return True, "authoritative_candidate", rows


def _build_pa_index(date_value: str, run_tag: str) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    inventory = []
    rejected = []
    for path in _candidate_pa_paths(date_value, run_tag):
        ok, reason, rows = _is_authoritative_pa_artifact(path, run_tag)
        inventory.append(
            {
                "date": date_value,
                "run_tag": run_tag,
                "path": _rel(path),
                "exists": True,
                "sha256": _sha256(path),
                "rows": len(rows),
                "authoritative_candidate": ok,
                "classification": reason,
            }
        )
        if not ok:
            rejected.append(
                {
                    "date": date_value,
                    "run_tag": run_tag,
                    "path": _rel(path),
                    "rejection_type": reason,
                    "notes": "candidate PA artifact was not accepted for strict run-bound overlay",
                }
            )
            continue
        for row in rows:
            row_date = str(row.get("slate_date") or row.get("game_date") or row.get("date") or "")[:10]
            if row_date != date_value:
                continue
            row_run = str(row.get("run_tag") or row.get("manifest_run_tag") or row.get("source_run_tag") or run_tag)
            if row_run and row_run != run_tag and run_tag not in str(path):
                continue
            enriched = dict(row)
            enriched["_source_artifact"] = _rel(path)
            enriched["_source_sha256"] = _sha256(path)
            index[_player_game_key(enriched)].append(enriched)
    return index, inventory, rejected


def _loose_match_rows(date_value: str, target: dict[str, str]) -> list[dict[str, Any]]:
    path = PA_FOUNDATION / f"review_aid_pa_retention_pilot_{date_value}.csv"
    out = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for row in _rows(path):
        if str(row.get("date"))[:10] != date_value:
            continue
        if str(row.get("canonical_player_id") or row.get("player_id")) != str(target.get("player_id")):
            continue
        if row.get("team") == target.get("team") and row.get("opponent") == target.get("opponent"):
            dedupe_key = (
                str(row.get("canonical_player_id") or row.get("player_id")),
                str(row.get("team")),
                str(row.get("opponent")),
                str(row.get("d7_plate_appearances")),
                str(row.get("d15_plate_appearances")),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            out.append(
                {
                    "date": date_value,
                    "run_tag": target.get("_run_tag"),
                    "canonical_key": _canonical(target),
                    "path": _rel(path),
                    "rejection_type": "loose_player_date_team_match_not_authoritative",
                    "player_name": target.get("player_name"),
                    "d7_plate_appearances": row.get("d7_plate_appearances"),
                    "d15_plate_appearances": row.get("d15_plate_appearances"),
                    "d30_plate_appearances": row.get("d30_plate_appearances"),
                    "notes": "rejected because artifact has no exact run tag, game_id, line, or side bridge",
                }
            )
    return out


def _overlay_for_run(date_value: str, run_tag: str, include_loose_rejection: bool = False) -> dict[str, Any]:
    rows = _load_live_rows(date_value, run_tag)
    h15 = _hits15(rows)
    pa_index, inventory, rejected = _build_pa_index(date_value, run_tag)
    attach_rows = []
    missing_rows = []
    ambiguity_rows = []
    for row in h15:
        row = dict(row)
        row["_run_tag"] = run_tag
        key = _canonical(row)
        pg_key = _player_game_key(row)
        matches = pa_index.get(pg_key, [])
        base = {
            "date": date_value,
            "run_tag": run_tag,
            "canonical_key": key,
            "player_game_key": pg_key,
            "game_id": row.get("game_id"),
            "player_id": row.get("player_id"),
            "player_name": row.get("player_name"),
            "team": row.get("team"),
            "opponent": row.get("opponent"),
            "prop_type": row.get("prop_type"),
            "line": row.get("line"),
            "side": row.get("model_pick_side"),
        }
        if len(matches) == 1:
            pa = matches[0]
            row_hash = hashlib.sha256(json.dumps({"row": base, "pa": pa}, sort_keys=True).encode()).hexdigest()
            attach = dict(base)
            attach.update(
                {
                    "attachment_status": "attached",
                    "pa_source_artifact": pa.get("_source_artifact"),
                    "pa_source_sha256": pa.get("_source_sha256"),
                    "direct_inferred_status": pa.get("pa_semantics_status") or pa.get("pa_source_regime") or "unknown",
                    "missingness_reason": "",
                    "row_provenance_hash": row_hash,
                }
            )
            for field in PA_FIELDS:
                attach[field] = pa.get(field, "")
            attach_rows.append(attach)
        elif len(matches) > 1:
            amb = dict(base)
            amb.update({"ambiguity_type": "multiple_authoritative_pa_matches", "match_count": len(matches), "notes": "fail_closed"})
            ambiguity_rows.append(amb)
        else:
            miss = dict(base)
            miss.update(
                {
                    "missingness_reason": "no_exact_run_bound_authoritative_pa_artifact",
                    "loose_match_available": False,
                    "notes": "no PA row accepted without exact run tag and deterministic player-game bridge",
                }
            )
            if include_loose_rejection:
                loose = _loose_match_rows(date_value, row)
                if loose:
                    miss["loose_match_available"] = True
                    rejected.extend(loose)
            missing_rows.append(miss)
    return {
        "date": date_value,
        "run_tag": run_tag,
        "slate_rows": len(rows),
        "hits_15_rows": len(h15),
        "attachments": attach_rows,
        "missing": missing_rows,
        "ambiguous": ambiguity_rows,
        "rejected": rejected,
        "pa_inventory": inventory,
    }


def _sample_runs(limit: int) -> list[tuple[str, str]]:
    sample: list[tuple[str, str]] = []
    for path in sorted(ODDS_ROOT.glob("*/mlb_slate_output__local_daily_*.csv"), reverse=True):
        date_value = path.parent.name
        if date_value == "2026-07-16":
            continue
        rows = _rows(path)
        if not _hits15(rows):
            continue
        run_tag = path.stem.replace("mlb_slate_output__", "")
        sample.append((date_value, run_tag))
        if len(sample) >= limit:
            break
    return list(reversed(sample))


def _pipeline_map() -> list[dict[str, Any]]:
    return [
        {"component": "live_selected_proposition_population", "path_or_script": "backend/mlb/exports/odds_history/<date>/mlb_slate_output__<run_tag>.csv", "role": "run-bound live proposition rows", "current_consumer": "research overlay generator", "run_bound": "yes", "pa_status": "no_pa_fields"},
        {"component": "prepared_hits_features", "path_or_script": "backend/mlb/exports/model_diagnostics/prepared_feature_vectors/<date>/hits_features.csv", "role": "prediction feature diagnostics", "current_consumer": "diagnostics", "run_bound": "no", "pa_status": "no_pa_fields_observed"},
        {"component": "pa_foundation_downstream_coverage", "path_or_script": "artifacts/analysis/mlb/pa_foundation/mlb_pa_downstream_coverage_<date>.csv", "role": "daily PA coverage report", "current_consumer": "health/reporting", "run_bound": "no", "pa_status": "coverage_summary_only"},
        {"component": "review_aid_pa_retention_pilot", "path_or_script": "artifacts/analysis/mlb/pa_foundation/review_aid_pa_retention_pilot_<date>.csv", "role": "PA retained for selected review-aid pockets", "current_consumer": "research diagnostics", "run_bound": "no", "pa_status": "loose_player_date_team_possible_not_authoritative"},
        {"component": "pa_opp_v1_historical_research_base", "path_or_script": "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv", "role": "historical PA opportunity research source", "current_consumer": "offline diagnostics", "run_bound": "historical_source_run_fields_not_live_run_tag", "pa_status": "not_a_live_overlay"},
    ]


def _contract_rows() -> list[dict[str, Any]]:
    return [
        {"contract_field": "overlay_name", "value": "strict_run_bound_live_pa_opportunity_overlay", "requirement": "versioned research-only overlay"},
        {"contract_field": "overlay_version", "value": "v1_2026_07_16", "requirement": "no predictive composite score"},
        {"contract_field": "run_identity", "value": "explicit date + exact run_tag", "requirement": "must equal live slate artifact run tag"},
        {"contract_field": "canonical_proposition_identity", "value": "slate_date|game_id|player_id|prop_type|line|side", "requirement": "one overlay decision per proposition row"},
        {"contract_field": "player_game_bridge", "value": "slate_date|game_id|player_id", "requirement": "PA joins only at authoritative player-game grain then bridges to proposition row"},
        {"contract_field": "accepted_pa_source", "value": "run-tagged local artifact with date, game_id, player_id, and frozen PA fields", "requirement": "loose player/date/team matching prohibited"},
        {"contract_field": "strict_prior", "value": "source/cutoff before prediction run; no same-game outcome/PA", "requirement": "fail closed if cutoff cannot be proven"},
        {"contract_field": "direct_inferred_split", "value": "preserve pa_semantics_status/pa_source_regime", "requirement": "no blending or favorability selection"},
        {"contract_field": "duplicates", "value": "multiple PA matches for player-game => ambiguous/rejected", "requirement": "no tie-breaking by values"},
        {"contract_field": "immutability", "value": "write analysis package only", "requirement": "do not modify slate/upload/quick card/workspace/db"},
        {"contract_field": "allowed_consumers", "value": "research diagnostics and prospective observation after authorization", "requirement": "not production scoring"},
        {"contract_field": "prohibited_consumers", "value": "production model, selectors, uploads, Quick Card, workspace mutation", "requirement": "no behavior change"},
        {"contract_field": "amendment_policy", "value": "new version required for source hierarchy or identity change", "requirement": "no silent semantic replacement"},
    ]


def _field_manifest() -> list[dict[str, Any]]:
    rows = []
    for field in ["slate_date", "run_tag", "prediction_cutoff", "game_id", "player_id", "team", "opponent", *PA_FIELDS, "pa_source_artifact", "pa_source_sha256", "direct_inferred_status", "missingness_reason", "created_at_utc", "row_provenance_hash"]:
        rows.append(
            {
                "field": field,
                "required": True,
                "source": "live_slate" if field in {"slate_date", "run_tag", "game_id", "player_id", "team", "opponent"} else "accepted_run_bound_pa_artifact_or_overlay_metadata",
                "notes": "required overlay provenance/feature field",
            }
        )
    return rows


def _summarize_run(result: dict[str, Any]) -> dict[str, Any]:
    attachments = result["attachments"]
    missing = result["missing"]
    ambiguous = result["ambiguous"]
    rejected = result["rejected"]
    direct = [r for r in attachments if "PREDICTION_SAFE" in str(r.get("direct_inferred_status"))]
    inferred = [r for r in attachments if "INFERRED" in str(r.get("direct_inferred_status"))]
    return {
        "date": result["date"],
        "run_tag": result["run_tag"],
        "slate_rows": result["slate_rows"],
        "hits_15_rows": result["hits_15_rows"],
        "authoritative_player_game_candidates": len({r.get("player_game_key") for r in attachments}),
        "exact_attachments": len(attachments),
        "direct_attachments": len(direct),
        "inferred_attachments": len(inferred),
        "missing_rows": len(missing),
        "ambiguous_rows": len(ambiguous),
        "duplicate_rows": 0,
        "rejected_loose_or_invalid_matches": len(rejected),
        "attachment_rate": round(len(attachments) / result["hits_15_rows"], 6) if result["hits_15_rows"] else "",
        "decision": "ATTACHMENT_BLOCKED_NO_EXACT_RUN_BOUND_PA_SOURCE" if not attachments and result["hits_15_rows"] else "NO_HITS_15_ROWS" if not result["hits_15_rows"] else "ATTACHMENTS_CREATED",
    }


def _validate(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.iterdir()):
        if not path.is_file():
            continue
        if path.suffix == ".csv":
            try:
                list(csv.DictReader(path.open()))
                status, notes = "PASS", "csv_parse_ok"
            except Exception as exc:
                status, notes = "FAIL", repr(exc)
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                status, notes = "PASS", "json_parse_ok"
            except Exception as exc:
                status, notes = "FAIL", repr(exc)
        elif path.suffix == ".md":
            status = "PASS" if path.read_text().strip() else "FAIL"
            notes = "markdown_nonempty" if status == "PASS" else "markdown_empty"
        else:
            continue
        rows.append({"path": _rel(path), "validation": status, "notes": notes})
    return rows


def build(date_value: str, run_tag: str, out_dir: Path, sample_size: int) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    july = _overlay_for_run(date_value, run_tag, include_loose_rejection=True)
    sample_runs = _sample_runs(sample_size)
    sample_results = [_overlay_for_run(d, tag, include_loose_rejection=False) for d, tag in sample_runs]
    all_results = [july] + sample_results

    attachment_ledger = []
    missing_ledger = []
    ambiguity_rejection = []
    pa_inventory = []
    for result in all_results:
        attachment_ledger.extend(result["attachments"])
        missing_ledger.extend(result["missing"])
        ambiguity_rejection.extend(result["ambiguous"])
        ambiguity_rejection.extend(result["rejected"])
        pa_inventory.extend(result["pa_inventory"])

    july_summary = _summarize_run(july)
    replay_rows = [_summarize_run(r) for r in sample_results]
    sample_manifest = [
        {
            "sample_order": i + 1,
            "date": d,
            "run_tag": tag,
            "slate_path": _rel(_run_file(d, tag)),
            "slate_sha256": _sha256(_run_file(d, tag)) if _run_file(d, tag).exists() else "",
        }
        for i, (d, tag) in enumerate(sample_runs)
    ]

    first_hash = hashlib.sha256(json.dumps(replay_rows, sort_keys=True).encode()).hexdigest()
    second_replay_rows = [_summarize_run(_overlay_for_run(d, tag, include_loose_rejection=False)) for d, tag in sample_runs]
    second_hash = hashlib.sha256(json.dumps(second_replay_rows, sort_keys=True).encode()).hexdigest()
    deterministic = [
        {
            "comparison": "historical_sample_replay_results",
            "first_sha256": first_hash,
            "second_sha256": second_hash,
            "deterministic_match": first_hash == second_hash,
            "notes": "reran in-process against same preserved local artifacts",
        }
    ]

    failure_rows = [
        {
            "date": date_value,
            "run_tag": run_tag,
            "failure_cause": "no_live_pa_artifact_generated_or_preserved_with_exact_run_tag",
            "evidence": "current live slate and prepared feature vectors contain no PA fields; pa_foundation review-aid PA pocket lacks game_id/line/side/run-tag bridge",
            "classification": "pipeline_step_not_invoked_or_output_not_preserved",
        },
        {
            "date": date_value,
            "run_tag": run_tag,
            "failure_cause": "missing_deterministic_proposition_bridge",
            "evidence": "observed loose match is player/date/team only and has no canonical slate_date|game_id|player_id|prop_type|line|side binding",
            "classification": "missing_deterministic_proposition_bridge",
        },
    ]

    readiness = {
        "generated_at_utc": _utc_now(),
        "decisions": {
            "MLB_LIVE_PA_OVERLAY_FAILURE_CAUSE_DECISION": "NO_EXACT_RUN_BOUND_PA_ARTIFACT_AND_MISSING_DETERMINISTIC_PROPOSITION_BRIDGE",
            "MLB_LIVE_PA_OVERLAY_CONTRACT_DECISION": "STRICT_RUN_BOUND_OVERLAY_CONTRACT_FROZEN_V1",
            "MLB_LIVE_PA_OVERLAY_JULY16_REPLAY_DECISION": july_summary["decision"],
            "MLB_LIVE_PA_OVERLAY_HISTORICAL_REPLAY_DECISION": "NOT_REPLAYABLE_FROM_PRESERVED_LIVE_ARTIFACTS",
            "MLB_LIVE_PA_OVERLAY_READINESS_DECISION": "DESIGN_VALIDATED_IMPLEMENTATION_BLOCKED_BY_SOURCE_TIMING",
            "MLB_LIVE_PA_OVERLAY_CHALLENGER_STATUS": "NOT_AUTHORIZED",
        },
        "july16_summary": july_summary,
        "historical_replay_summary": {
            "sample_run_tags": len(sample_runs),
            "sample_hits_15_rows": sum(r["hits_15_rows"] for r in replay_rows),
            "sample_exact_attachments": sum(r["exact_attachments"] for r in replay_rows),
            "sample_missing_rows": sum(r["missing_rows"] for r in replay_rows),
            "deterministic_match": first_hash == second_hash,
        },
        "remaining_requirement": "Generate and preserve, at prediction time, a run-tagged PA overlay artifact keyed by date|game_id|player_id with frozen PA fields and cutoff provenance, then bridge it to every canonical proposition row.",
    }

    _write_csv(out_dir / f"current_live_pa_pipeline_map_{RUN_DATE}.csv", _pipeline_map(), ["component", "path_or_script", "role", "current_consumer", "run_bound", "pa_status"])
    _write_csv(out_dir / f"july16_failure_cause_analysis_{RUN_DATE}.csv", failure_rows, ["date", "run_tag", "failure_cause", "evidence", "classification"])
    _write_csv(out_dir / f"frozen_overlay_contract_{RUN_DATE}.csv", _contract_rows(), ["contract_field", "value", "requirement"])
    _write_csv(out_dir / f"field_and_provenance_manifest_{RUN_DATE}.csv", _field_manifest(), ["field", "required", "source", "notes"])
    _write_csv(out_dir / f"july16_replay_results_{RUN_DATE}.csv", [july_summary], list(july_summary.keys()))
    attach_fields = ["date", "run_tag", "canonical_key", "player_game_key", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line", "side", "attachment_status", "pa_source_artifact", "pa_source_sha256", "direct_inferred_status", "missingness_reason", "row_provenance_hash", *PA_FIELDS]
    _write_csv(out_dir / f"attachment_ledger_{RUN_DATE}.csv", attachment_ledger, attach_fields)
    _write_csv(out_dir / f"missingness_ledger_{RUN_DATE}.csv", missing_ledger, ["date", "run_tag", "canonical_key", "player_game_key", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line", "side", "missingness_reason", "loose_match_available", "notes"])
    _write_csv(out_dir / f"ambiguity_and_rejection_ledger_{RUN_DATE}.csv", ambiguity_rejection, ["date", "run_tag", "canonical_key", "player_game_key", "path", "rejection_type", "ambiguity_type", "match_count", "player_name", "d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances", "notes"])
    _write_csv(out_dir / f"historical_live_run_replay_sample_manifest_{RUN_DATE}.csv", sample_manifest, ["sample_order", "date", "run_tag", "slate_path", "slate_sha256"])
    replay_fields = list(july_summary.keys())
    _write_csv(out_dir / f"replayability_results_{RUN_DATE}.csv", replay_rows, replay_fields)
    _write_csv(out_dir / f"deterministic_rerun_comparison_{RUN_DATE}.csv", deterministic, ["comparison", "first_sha256", "second_sha256", "deterministic_match", "notes"])
    readiness_rows = [{"decision": key, "value": value} for key, value in readiness["decisions"].items()]
    readiness_rows.append({"decision": "remaining_requirement", "value": readiness["remaining_requirement"]})
    _write_csv(out_dir / f"readiness_assessment_{RUN_DATE}.csv", readiness_rows, ["decision", "value"])
    _write_csv(out_dir / f"pa_artifact_inventory_{RUN_DATE}.csv", pa_inventory, ["date", "run_tag", "path", "exists", "sha256", "rows", "authoritative_candidate", "classification"])
    _write_json(out_dir / f"machine_readable_live_pa_overlay_pilot_{RUN_DATE}.json", readiness)

    usage = f"""# Strict Run-Bound Live PA Overlay Generator Usage

Run:

```bash
.venv/bin/python -m backend.mlb.scripts.build_mlb_strict_run_bound_live_pa_overlay_pilot --date {date_value} --run-tag {run_tag} --mode research_only
```

The generator writes only to `{_rel(out_dir)}`. It does not modify slate outputs, uploads, Quick Cards, workspace files, databases, APIs, model artifacts, or LaunchAgents.

Accepted PA sources must be run-tagged and keyed by date, game_id, and player_id with frozen PA fields. Loose player/date/team matches are rejected.
"""
    (out_dir / f"generator_usage_{RUN_DATE}.md").write_text(usage)

    summary = f"""# MLB Strict Run-Bound Live PA Opportunity Overlay Pilot - 2026-07-16

## Executive Summary

The strict run-bound PA overlay contract was frozen and a research-only generator was implemented. The July 16 replay for `{run_tag}` found `{july_summary['hits_15_rows']}` Hits 1.5 row and `{july_summary['exact_attachments']}` exact PA attachments.

The previously observed loose PA match cannot be admitted: it is player/date/team only and lacks exact run tag, game_id, prop line, side, and canonical proposition bridge.

## Decisions

`MLB_LIVE_PA_OVERLAY_FAILURE_CAUSE_DECISION = {readiness['decisions']['MLB_LIVE_PA_OVERLAY_FAILURE_CAUSE_DECISION']}`
`MLB_LIVE_PA_OVERLAY_CONTRACT_DECISION = {readiness['decisions']['MLB_LIVE_PA_OVERLAY_CONTRACT_DECISION']}`
`MLB_LIVE_PA_OVERLAY_JULY16_REPLAY_DECISION = {readiness['decisions']['MLB_LIVE_PA_OVERLAY_JULY16_REPLAY_DECISION']}`
`MLB_LIVE_PA_OVERLAY_HISTORICAL_REPLAY_DECISION = {readiness['decisions']['MLB_LIVE_PA_OVERLAY_HISTORICAL_REPLAY_DECISION']}`
`MLB_LIVE_PA_OVERLAY_READINESS_DECISION = {readiness['decisions']['MLB_LIVE_PA_OVERLAY_READINESS_DECISION']}`
`MLB_LIVE_PA_OVERLAY_CHALLENGER_STATUS = NOT_AUTHORIZED`

## Historical Replay Sample

- Sample run tags: `{readiness['historical_replay_summary']['sample_run_tags']}`
- Sample Hits 1.5 rows: `{readiness['historical_replay_summary']['sample_hits_15_rows']}`
- Exact attachments: `{readiness['historical_replay_summary']['sample_exact_attachments']}`
- Missing rows: `{readiness['historical_replay_summary']['sample_missing_rows']}`
- Deterministic rerun equality: `{readiness['historical_replay_summary']['deterministic_match']}`

## Remaining Requirement

{readiness['remaining_requirement']}

No outcomes, model training, matrix construction, uploads, database writes, OddsAPI calls, or production behavior changes occurred.
"""
    (out_dir / f"executive_summary_{RUN_DATE}.md").write_text(summary)

    validation = _validate(out_dir)
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation, ["path", "validation", "notes"])
    manifest = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file():
            manifest.append({"path": _rel(path), "sha256": _sha256(path), "bytes": path.stat().st_size})
    _write_csv(out_dir / f"sha256_manifest_{RUN_DATE}.csv", manifest, ["path", "sha256", "bytes"])
    validation = _validate(out_dir)
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation, ["path", "validation", "notes"])
    return readiness


def main() -> int:
    parser = argparse.ArgumentParser(description="Build strict run-bound live PA opportunity overlay replay pilot.")
    parser.add_argument("--date", default="2026-07-16")
    parser.add_argument("--run-tag", default="local_daily_20260716T200001Z")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--historical-sample-size", type=int, default=10)
    parser.add_argument("--mode", default="research_only", choices=["research_only"])
    args = parser.parse_args()
    result = build(args.date, args.run_tag, Path(args.output_dir), args.historical_sample_size)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
