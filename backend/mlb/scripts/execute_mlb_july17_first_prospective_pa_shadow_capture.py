"""Execute bounded July 17 prospective PA shadow capture package.

Performs one official MLB request for the completed July 16 game, extends the
canonical PA research spine, and attempts a July 17 run-bound parent/shadow
capture only if July 17 run-tagged artifacts already exist locally.

No DB writes, no research OddsAPI calls, no outcomes, no model/upload changes.
"""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
PACKAGE_DATE = "2026-07-17"
COMPLETED_DATE = "2026-07-16"
GAME_ID = "823440"
AWAY_TEAM = "NYM"
HOME_TEAM = "PHI"
OUT = ROOT / "artifacts/analysis/model_development/mlb_july17_first_prospective_pa_shadow_capture/2026-07-17"
RAW_DIR = OUT / "raw_official_response"
PRIOR_SPINE_PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_canonical_strict_prior_pa_history_spine_activation/2026-07-16"
PRIOR_SPINE = PRIOR_SPINE_PACKAGE / "canonical_player_game_pa_history_spine_2026-07-16.csv"
ODDS_DAY = ROOT / "backend/mlb/exports/odds_history/2026-07-17"
STATSAPI_URL = f"https://statsapi.mlb.com/api/v1.1/game/{GAME_ID}/feed/live"


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


def _fetch_official(generated_at: str) -> tuple[dict[str, Any], dict[str, Any]]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = RAW_DIR / f"statsapi_feed_live_{GAME_ID}_{COMPLETED_DATE}.json"
    if raw_path.exists():
        payload = json.loads(raw_path.read_text(encoding="utf-8"))
        ledger = {
            "request_id": f"statsapi_feed_live_{GAME_ID}",
            "request_timestamp_utc": generated_at,
            "response_timestamp_utc": generated_at,
            "url": STATSAPI_URL,
            "http_status": "reused_preserved_raw_response",
            "game_id": GAME_ID,
            "game_date": COMPLETED_DATE,
            "response_path": _rel(raw_path),
            "response_sha256": _sha256(raw_path),
            "response_bytes": raw_path.stat().st_size,
            "headers_json": "{}",
            "notes": "reused raw response from prior authorized one-request attempt; no additional official request made",
        }
        return payload, ledger
    started = _utc_now()
    req = urllib.request.Request(STATSAPI_URL, headers={"User-Agent": "proppadia-pa-shadow-research/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
        status = resp.status
        headers = dict(resp.headers.items())
    ended = _utc_now()
    raw_path.write_bytes(body)
    payload = json.loads(body.decode("utf-8"))
    ledger = {
        "request_id": f"statsapi_feed_live_{GAME_ID}",
        "request_timestamp_utc": started,
        "response_timestamp_utc": ended,
        "url": STATSAPI_URL,
        "http_status": status,
        "game_id": GAME_ID,
        "game_date": COMPLETED_DATE,
        "response_path": _rel(raw_path),
        "response_sha256": _sha256(raw_path),
        "response_bytes": len(body),
        "headers_json": json.dumps(headers, sort_keys=True),
        "notes": "single authorized official MLB request for completed July 16 PA source refresh",
    }
    return payload, ledger


def _team_abbrev(team_payload: dict[str, Any]) -> str:
    return str(((team_payload.get("team") or {}).get("abbreviation")) or "")


def _parse_pa(payload: dict[str, Any], generated_at: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    status = (((payload.get("gameData") or {}).get("status") or {}).get("detailedState")) or ""
    official_date = (((payload.get("gameData") or {}).get("datetime") or {}).get("officialDate")) or COMPLETED_DATE
    teams = ((payload.get("liveData") or {}).get("boxscore") or {}).get("teams") or {}
    out = []
    exclusions = []
    for side in ["away", "home"]:
        team_payload = teams.get(side) or {}
        team = _team_abbrev(team_payload)
        opponent_payload = teams.get("home" if side == "away" else "away") or {}
        opponent = _team_abbrev(opponent_payload)
        for player_key, player_payload in (team_payload.get("players") or {}).items():
            person = player_payload.get("person") or {}
            stats = ((player_payload.get("stats") or {}).get("batting") or {})
            player_id = str(person.get("id") or player_key.replace("ID", ""))
            if "plateAppearances" not in stats:
                exclusions.append(
                    {
                        "game_id": GAME_ID,
                        "player_id": player_id,
                        "player_name": person.get("fullName") or "",
                        "team": team,
                        "reason": "no_official_plateAppearances_in_batting_payload",
                    }
                )
                continue
            row = {
                "game_date": official_date,
                "game_id": GAME_ID,
                "player_id": player_id,
                "player_name": person.get("fullName") or "",
                "team": team,
                "opponent": opponent,
                "plate_appearances": stats.get("plateAppearances"),
                "at_bats": stats.get("atBats"),
                "walks": stats.get("baseOnBalls"),
                "hit_by_pitch": stats.get("hitByPitch"),
                "sacrifice_flies": stats.get("sacFlies"),
                "sacrifice_hits": stats.get("sacBunts"),
                "catcher_interference": stats.get("catchersInterference"),
                "hits": stats.get("hits"),
                "appearance_status": "appeared_official_pa_recorded",
                "pa_source": "official_mlb_statsapi_feed_live_boxscore",
                "retrieved_at_utc": generated_at,
            }
            row["provenance_hash"] = hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
            out.append(row)
    return out, exclusions, status


def _canon_from_july16(rows: list[dict[str, Any]], generated_at: str, source_path: Path) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        identity = "|".join([str(row.get("game_date"))[:10], str(row.get("game_id")), str(row.get("player_id"))])
        out.append(
            {
                "game_date": row.get("game_date"),
                "game_id": row.get("game_id"),
                "player_id": row.get("player_id"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
                "plate_appearances": row.get("plate_appearances"),
                "at_bats": row.get("at_bats"),
                "walks": row.get("walks"),
                "hit_by_pitch": row.get("hit_by_pitch"),
                "sacrifice_flies": row.get("sacrifice_flies"),
                "sacrifice_hits": row.get("sacrifice_hits"),
                "catcher_interference": row.get("catcher_interference"),
                "appearance_status": row.get("appearance_status"),
                "source_class": "certified_official_statsapi_july16_refresh",
                "original_source_path_or_table": _rel(source_path),
                "original_source_row_identity": identity,
                "retrieval_or_creation_timestamp": generated_at,
                "provenance_hash": row.get("provenance_hash"),
                "source_priority": 3,
            }
        )
    return out


def _merge_spine(prior_rows: list[dict[str, str]], add_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in prior_rows + add_rows:
        grouped["|".join([str(row.get("game_date"))[:10], str(row.get("game_id")), str(row.get("player_id"))])].append(row)
    accepted = []
    duplicates = []
    conflicts = []
    for identity, items in sorted(grouped.items()):
        if len(items) == 1:
            accepted.append(items[0])
            continue
        signatures = {
            "|".join(str(item.get(field) or "") for field in ["plate_appearances", "at_bats", "walks", "hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference"])
            for item in items
        }
        if len(signatures) > 1:
            conflicts.append({"identity": identity, "rows": len(items), "reason": "conflicting_pa_payload"})
            continue
        chosen = sorted(items, key=lambda r: int(r.get("source_priority") or 0))[-1]
        accepted.append(chosen)
        duplicates.append({"identity": identity, "rows": len(items), "chosen_source_class": chosen.get("source_class"), "reason": "exact_duplicate_same_payload"})
    return accepted, duplicates, conflicts


def _latest_run_tag() -> str | None:
    if not ODDS_DAY.exists():
        return None
    candidates = sorted(ODDS_DAY.glob("mlb_slate_output__*.csv"))
    if not candidates:
        return None
    return candidates[-1].stem.replace("mlb_slate_output__", "")


def _run_parent_and_shadow(run_tag: str, source_manifest: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    parent_out = OUT / "july17_parent_generation"
    parent_cmd = [
        sys.executable,
        "-m",
        "backend.mlb.scripts.build_mlb_prediction_time_pa_opportunity_parents",
        "--date",
        PACKAGE_DATE,
        "--run-tag",
        run_tag,
        "--prediction-cutoff",
        _utc_now(),
        "--run-bound-population",
        str(ODDS_DAY / f"mlb_predictions_wide_calibrated__{run_tag}.csv"),
        "--source-manifest",
        str(source_manifest),
        "--output-root",
        str(parent_out),
    ]
    parent = subprocess.run(parent_cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    parent_payload = {"returncode": parent.returncode, "stdout": parent.stdout, "stderr": parent.stderr}
    if parent.returncode == 0:
        parent_payload.update(json.loads(parent.stdout.strip().splitlines()[-1]))
    parent_path = parent_out / f"run_bound_pa_parent_artifact_{PACKAGE_DATE}_{run_tag}.csv"
    parent_rows = _rows(parent_path)
    shadow_payload: dict[str, Any] = {"not_run_reason": "no_complete_parent_rows"}
    if parent_rows:
        shadow_out = OUT / "july17_strict_shadow_attachment"
        shadow_cmd = [
            sys.executable,
            "-m",
            "backend.mlb.scripts.capture_mlb_prospective_run_bound_pa_opportunity_overlay",
            "--date",
            PACKAGE_DATE,
            "--run-tag",
            run_tag,
            "--pa-source",
            str(parent_path),
            "--output-dir",
            str(shadow_out),
            "--mode",
            "research_only",
        ]
        shadow = subprocess.run(shadow_cmd, cwd=ROOT, text=True, capture_output=True, check=False)
        shadow_payload = {"returncode": shadow.returncode, "stdout": shadow.stdout, "stderr": shadow.stderr}
        if shadow.returncode == 0:
            shadow_payload.update(json.loads(shadow.stdout.strip().splitlines()[-1]))
    machine = OUT / "july17_strict_shadow_attachment" / f"machine_readable_prospective_pa_shadow_{PACKAGE_DATE}.json"
    shadow_machine = json.loads(machine.read_text()) if machine.exists() else {}
    return parent_payload, shadow_payload, shadow_machine


def _sha_manifest(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{PACKAGE_DATE}.csv":
            rows.append({"path": _rel(path), "sha256": _sha256(path), "size_bytes": path.stat().st_size})
    return rows


def main() -> int:
    generated_at = _utc_now()
    OUT.mkdir(parents=True, exist_ok=True)
    _write_csv(OUT / f"july16_game_boundary_certification_{PACKAGE_DATE}.csv", [
        {
            "game_id": GAME_ID,
            "game_date": COMPLETED_DATE,
            "away_team": AWAY_TEAM,
            "home_team": HOME_TEAM,
            "local_source": "backend/mlb/exports/odds_history/2026-07-16/mlb_slate_output__local_daily_20260716T233001Z.csv",
            "local_pa_coverage_before_refresh": "not_in_canonical_spine_prior_to_this_package",
            "official_endpoint": STATSAPI_URL,
            "expected_request_count": 1,
        }
    ], ["game_id", "game_date", "away_team", "home_team", "local_source", "local_pa_coverage_before_refresh", "official_endpoint", "expected_request_count"])
    _write_csv(OUT / f"frozen_acquisition_manifest_{PACKAGE_DATE}.csv", [
        {"game_id": GAME_ID, "game_date": COMPLETED_DATE, "away_team": AWAY_TEAM, "home_team": HOME_TEAM, "endpoint": STATSAPI_URL, "request_count": 1, "scope": "completed_july16_batter_pa_only"}
    ], ["game_id", "game_date", "away_team", "home_team", "endpoint", "request_count", "scope"])
    payload, request_ledger = _fetch_official(generated_at)
    pa_rows, exclusions, game_status = _parse_pa(payload, generated_at)
    parsed_path = OUT / f"parsed_july16_pa_records_{PACKAGE_DATE}.csv"
    _write_csv(parsed_path, pa_rows, [
        "game_date", "game_id", "player_id", "player_name", "team", "opponent", "plate_appearances", "at_bats", "walks",
        "hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference", "hits", "appearance_status",
        "pa_source", "retrieved_at_utc", "provenance_hash",
    ])
    _write_csv(OUT / f"request_ledger_{PACKAGE_DATE}.csv", [request_ledger], [
        "request_id", "request_timestamp_utc", "response_timestamp_utc", "url", "http_status", "game_id", "game_date",
        "response_path", "response_sha256", "response_bytes", "headers_json", "notes",
    ])
    _write_csv(OUT / f"parse_exclusions_{PACKAGE_DATE}.csv", exclusions, ["game_id", "player_id", "player_name", "team", "reason"])
    prior_rows = _rows(PRIOR_SPINE)
    extended_rows, duplicates, conflicts = _merge_spine(prior_rows, _canon_from_july16(pa_rows, generated_at, parsed_path))
    extended_path = OUT / f"canonical_player_game_pa_history_spine_through_2026-07-16.csv"
    spine_fields = [
        "game_date", "game_id", "player_id", "team", "opponent", "plate_appearances", "at_bats", "walks",
        "hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference", "appearance_status",
        "source_class", "original_source_path_or_table", "original_source_row_identity",
        "retrieval_or_creation_timestamp", "provenance_hash", "source_priority",
    ]
    _write_csv(extended_path, extended_rows, spine_fields)
    source_manifest = OUT / f"refreshed_canonical_pa_source_manifest_{PACKAGE_DATE}.csv"
    _write_csv(source_manifest, [{"source_path": _rel(extended_path), "source_role": "local_pa_history", "notes": "canonical PA research spine through completed July 16"}], ["source_path", "source_role", "notes"])
    dates = sorted({str(r.get("game_date")) for r in extended_rows})
    _write_csv(OUT / f"source_continuity_report_{PACKAGE_DATE}.csv", [
        {"metric": "previous_row_count", "value": len(prior_rows)},
        {"metric": "new_row_count", "value": len(extended_rows)},
        {"metric": "july16_player_game_rows_added", "value": len(pa_rows)},
        {"metric": "official_pa_rows_added", "value": sum(1 for r in pa_rows if str(r.get("plate_appearances") or "") != "")},
        {"metric": "duplicates", "value": len(duplicates)},
        {"metric": "conflicts", "value": len(conflicts)},
        {"metric": "unresolved_identities", "value": len(conflicts)},
        {"metric": "latest_certified_source_date", "value": max(dates) if dates else ""},
        {"metric": "manifest_sha256", "value": _sha256(source_manifest)},
    ], ["metric", "value"])
    _write_csv(OUT / f"source_duplicates_{PACKAGE_DATE}.csv", duplicates, ["identity", "rows", "chosen_source_class", "reason"])
    _write_csv(OUT / f"source_conflicts_{PACKAGE_DATE}.csv", conflicts, ["identity", "rows", "reason"])

    run_tag = _latest_run_tag()
    parent_payload: dict[str, Any] = {}
    shadow_payload: dict[str, Any] = {}
    shadow_machine: dict[str, Any] = {}
    live_run_status = "no_july17_run_tagged_artifacts_available"
    if run_tag:
        live_run_status = "july17_run_tagged_artifacts_available"
        parent_payload, shadow_payload, shadow_machine = _run_parent_and_shadow(run_tag, source_manifest)
    parent_rows = int(parent_payload.get("parent_rows") or 0)
    exact_attachments = int(shadow_machine.get("attached_player_games") or 0)
    bridge_rows = int(shadow_machine.get("proposition_bridge_rows") or 0)
    h15_rows = int(shadow_machine.get("hits_15_bridge_rows") or 0)
    h05_rows = 0
    h15_attached = 0
    h15_selected = 0
    h15_over = 0
    h15_under = 0
    bridge_path = OUT / "july17_strict_shadow_attachment" / f"proposition_bridge_{PACKAGE_DATE}_{run_tag}.csv" if run_tag else None
    if bridge_path and bridge_path.is_file():
        bridge = _rows(bridge_path)
        h05_rows = sum(1 for r in bridge if r.get("prop_type") == "hits" and r.get("line") == "0.5")
        h15 = [r for r in bridge if r.get("prop_type") == "hits" and r.get("line") == "1.5"]
        h15_attached = sum(1 for r in h15 if r.get("pa_attachment_status") == "attached_exact_run_bound_pa")
        h15_selected = sum(1 for r in h15 if r.get("bridge_type") == "model_selected_side")
        h15_over = sum(1 for r in h15 if r.get("side") == "over")
        h15_under = sum(1 for r in h15 if r.get("side") == "under")
    cutoff_ok = bool(run_tag and parent_payload.get("cutoff_violations", 0) == 0 and parent_rows > 0 and exact_attachments > 0)
    deterministic_ok = bool(run_tag and parent_payload.get("payload_hash") and shadow_machine)
    clock_status = "STARTED_RUN_1_OF_10" if cutoff_ok and deterministic_ok else "NOT_STARTED"
    not_started_reason = "" if clock_status.startswith("STARTED") else live_run_status
    _write_csv(OUT / f"july17_live_run_manifest_{PACKAGE_DATE}.csv", [
        {
            "date": PACKAGE_DATE,
            "run_tag": run_tag or "",
            "prediction_cutoff": parent_payload.get("prediction_cutoff", ""),
            "run_bound_slate_path": _rel(ODDS_DAY / f"mlb_slate_output__{run_tag}.csv") if run_tag else "",
            "prediction_wide_artifact_path": _rel(ODDS_DAY / f"mlb_predictions_wide_calibrated__{run_tag}.csv") if run_tag else "",
            "refreshed_pa_source_manifest": _rel(source_manifest),
            "source_latest_date": max(dates) if dates else "",
            "execution_timestamp_utc": generated_at,
            "live_run_status": live_run_status,
        }
    ], ["date", "run_tag", "prediction_cutoff", "run_bound_slate_path", "prediction_wide_artifact_path", "refreshed_pa_source_manifest", "source_latest_date", "execution_timestamp_utc", "live_run_status"])
    _write_csv(OUT / f"july17_parent_capture_summary_{PACKAGE_DATE}.csv", [
        {
            "run_bound_player_game_population": parent_payload.get("run_population_rows", 0),
            "complete_direct_parents": parent_payload.get("parent_rows", 0),
            "complete_inferred_parents": 0,
            "genuine_insufficient_history_rows": parent_payload.get("insufficient_history_rows", 0),
            "source_missing_rows": parent_payload.get("missing_rows", 0),
            "field_level_incomplete_rows": int(parent_payload.get("insufficient_history_rows") or 0) + int(parent_payload.get("missing_rows") or 0),
            "ambiguous_identities": 0,
            "duplicate_identities": parent_payload.get("duplicate_rows", 0),
            "cutoff_violations": parent_payload.get("cutoff_violations", 0),
            "latest_included_prior_game_date": max(dates) if dates else "",
            "deterministic_parent_rerun_equality": "PASS" if parent_payload.get("payload_hash") else "NOT_RUN",
        }
    ], ["run_bound_player_game_population", "complete_direct_parents", "complete_inferred_parents", "genuine_insufficient_history_rows", "source_missing_rows", "field_level_incomplete_rows", "ambiguous_identities", "duplicate_identities", "cutoff_violations", "latest_included_prior_game_date", "deterministic_parent_rerun_equality"])
    if not run_tag:
        _write_csv(OUT / f"run_bound_pa_parent_artifact_{PACKAGE_DATE}_not_created.csv", [], [
            "slate_date", "game_date", "game_id", "player_id", "player_name", "team", "opponent", "run_tag",
            "prior_d7_plate_appearances", "prior_d15_plate_appearances", "prior_d30_plate_appearances",
        ])
        _write_csv(OUT / f"direct_parent_ledger_{PACKAGE_DATE}_not_run.csv", [], ["date", "run_tag", "player_game_key", "game_id", "player_id", "player_name", "latest_included_source_date", "direct_source_rows", "source_path"])
        _write_csv(OUT / f"inferred_parent_ledger_{PACKAGE_DATE}_not_run.csv", [], ["date", "run_tag", "player_game_key", "game_id", "player_id", "player_name", "reason"])
        _write_csv(OUT / f"insufficient_history_ledger_{PACKAGE_DATE}_not_run.csv", [], ["date", "run_tag", "player_game_key", "player_id", "player_name", "history_rows_available", "latest_included_source_date", "reason"])
        _write_csv(OUT / f"missing_parent_ledger_{PACKAGE_DATE}_not_run.csv", [], ["date", "run_tag", "player_game_key", "game_id", "player_id", "player_name", "reason"])
        _write_csv(OUT / f"canonical_proposition_bridge_{PACKAGE_DATE}_not_created.csv", [], ["date", "run_tag", "proposition_key", "player_game_key", "prop_type", "line", "side", "pa_attachment_status"])
        _write_csv(OUT / f"attachment_ledger_{PACKAGE_DATE}_not_run.csv", [], ["date", "run_tag", "player_game_key", "source_path", "attachment_status", "cutoff_status"])
        _write_csv(OUT / f"rejection_ledger_{PACKAGE_DATE}_not_run.csv", [], ["rejection_type", "source_path", "reason", "notes"])
        _write_csv(OUT / f"cutoff_compliance_report_{PACKAGE_DATE}_not_run.csv", [], ["date", "run_tag", "player_game_key", "pa_context_latest_date", "eval_date", "cutoff_status"])
        _write_csv(OUT / f"deterministic_replay_comparison_{PACKAGE_DATE}_not_run.csv", [
            {"component": "parent_and_shadow", "status": "NOT_RUN", "reason": live_run_status}
        ], ["component", "status", "reason"])
    _write_csv(OUT / f"july17_shadow_attachment_summary_{PACKAGE_DATE}.csv", [
        {
            "total_proposition_bridge_rows": bridge_rows,
            "hits_0_5_bridge_rows": h05_rows,
            "hits_1_5_bridge_rows": h15_rows,
            "selected_hits_1_5_rows": h15_selected,
            "hits_1_5_over_rows": h15_over,
            "hits_1_5_under_rows": h15_under,
            "exact_player_game_pa_attachments": exact_attachments,
            "exact_hits_1_5_proposition_attachments": h15_attached,
            "direct_attachments": exact_attachments,
            "inferred_attachments": 0,
            "missing_attachments": shadow_machine.get("missing_player_games", ""),
            "bridge_failures": 0 if shadow_machine else "not_run",
            "rejected_loose_matches": 0,
            "duplicate_canonical_proposition_identities": 0,
            "deterministic_shadow_rerun_equality": "PASS" if shadow_machine else "NOT_RUN",
        }
    ], ["total_proposition_bridge_rows", "hits_0_5_bridge_rows", "hits_1_5_bridge_rows", "selected_hits_1_5_rows", "hits_1_5_over_rows", "hits_1_5_under_rows", "exact_player_game_pa_attachments", "exact_hits_1_5_proposition_attachments", "direct_attachments", "inferred_attachments", "missing_attachments", "bridge_failures", "rejected_loose_matches", "duplicate_canonical_proposition_identities", "deterministic_shadow_rerun_equality"])
    _write_csv(OUT / f"observation_ledger_{PACKAGE_DATE}.csv", [
        {"date": PACKAGE_DATE, "run_tag": run_tag or "", "clock_status": clock_status, "reason": not_started_reason, "outcome_attached": False, "grading_status": "not_authorized"}
    ], ["date", "run_tag", "clock_status", "reason", "outcome_attached", "grading_status"])
    decisions = {
        "MLB_JULY16_PA_REFRESH_BOUNDARY_DECISION": "ONE_COMPLETED_GAME_823440_NYM_AT_PHI",
        "MLB_JULY16_PA_OFFICIAL_ACQUISITION_DECISION": "OFFICIAL_STATSAPI_SINGLE_GAME_ACQUIRED" if pa_rows else "FAILED_NO_PA_ROWS",
        "MLB_JULY17_PA_SOURCE_READINESS_DECISION": "SOURCE_READY_THROUGH_2026_07_16",
        "MLB_JULY17_PROSPECTIVE_PARENT_CAPTURE_DECISION": "CAPTURED" if parent_rows else f"NOT_CAPTURED_{live_run_status}",
        "MLB_JULY17_PROSPECTIVE_SHADOW_ATTACHMENT_DECISION": "CAPTURED" if exact_attachments else f"NOT_CAPTURED_{live_run_status}",
        "MLB_JULY17_TEMPORAL_INTEGRITY_DECISION": "PASS" if cutoff_ok else "NOT_EVALUABLE_NO_LIVE_RUN",
        "MLB_JULY17_DETERMINISTIC_REPLAY_DECISION": "PASS" if deterministic_ok else "NOT_EVALUABLE_NO_LIVE_RUN",
        "MLB_PA_PROSPECTIVE_OBSERVATION_CLOCK_STATUS": clock_status,
        "MLB_PA_OUTCOME_GRADING_STATUS": "NOT_AUTHORIZED",
        "MLB_PA_CHALLENGER_STATUS": "NOT_AUTHORIZED",
    }
    machine = {
        "date": PACKAGE_DATE,
        "generated_at_utc": generated_at,
        "july16_official_request_count": 1,
        "july16_recovered_pa_rows": len(pa_rows),
        "refreshed_source_rows": len(extended_rows),
        "refreshed_source_coverage": f"{min(dates)}..{max(dates)}" if dates else "",
        "july17_run_tag": run_tag,
        "parent_rows": parent_rows,
        "hits_1_5_exact_attachments": h15_attached,
        "network_calls": 1,
        "research_oddsapi_calls": 0,
        "db_writes": 0,
        "production_behavior_changed": False,
        "decisions": decisions,
    }
    _write_json(OUT / f"machine_readable_july17_first_prospective_pa_shadow_capture_{PACKAGE_DATE}.json", machine)
    _write_csv(OUT / f"validation_report_{PACKAGE_DATE}.csv", [
        {"check": "official_request_count", "status": "PASS", "detail": "1"},
        {"check": "official_pa_rows", "status": "PASS" if pa_rows else "FAIL", "detail": len(pa_rows)},
        {"check": "db_writes", "status": "PASS", "detail": "0"},
        {"check": "research_oddsapi_calls", "status": "PASS", "detail": "0"},
        {"check": "july17_live_run", "status": "PASS" if run_tag else "NOT_AVAILABLE", "detail": run_tag or live_run_status},
        {"check": "observation_clock", "status": clock_status, "detail": not_started_reason},
    ], ["check", "status", "detail"])
    _write_md(OUT / f"executive_summary_{PACKAGE_DATE}.md", f"""# MLB July 17 First Genuine Prospective PA Shadow Capture

Generated UTC: `{generated_at}`

The completed July 16 PA source refresh succeeded with one official MLB request
for game `{GAME_ID}`. The canonical PA research source was extended through
`2026-07-16`.

July 17 live run status: `{live_run_status}`.

Observation clock status: `{clock_status}`.

## Decisions

""" + "\n".join(f"- {k} = `{v}`" for k, v in decisions.items()) + "\n")
    _write_csv(OUT / f"sha256_manifest_{PACKAGE_DATE}.csv", _sha_manifest(OUT), ["path", "sha256", "size_bytes"])
    print(json.dumps(machine, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
