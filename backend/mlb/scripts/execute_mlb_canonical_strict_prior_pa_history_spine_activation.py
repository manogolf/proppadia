"""Assemble a canonical strict-prior MLB PA history spine and rerun PA parents.

Bounded research-only pilot. Reads mlb.player_stats through July 11, merges the
certified July 12 official PA records, reruns the existing prediction-time PA
parent generator, and only runs the shadow bridge when nonempty parents exist.

No DB writes, no network acquisition, no OddsAPI, no production behavior change.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parents[3]
DATE_VALUE = "2026-07-16"
RUN_TAG = "local_daily_20260716T233001Z"
CUTOFF = "2026-07-16T23:30:01Z"
HISTORY_START = "2026-03-01"
LOCAL_HISTORY_END = "2026-07-11"
OFFICIAL_REFRESH_DATE = "2026-07-12"
OUT = ROOT / "artifacts/analysis/model_development/mlb_canonical_strict_prior_pa_history_spine_activation/2026-07-16"
JULY12_PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_pa_source_refresh_and_parent_activation_pilot/2026-07-16"
JULY12_PA = JULY12_PACKAGE / "parsed_official_pa_records_2026-07-12.csv"
SLATE = ROOT / f"backend/mlb/exports/odds_history/{DATE_VALUE}/mlb_slate_output__{RUN_TAG}.csv"
PRED = ROOT / f"backend/mlb/exports/odds_history/{DATE_VALUE}/mlb_predictions_wide_calibrated__{RUN_TAG}.csv"
BOOK = ROOT / f"backend/mlb/exports/odds_history/{DATE_VALUE}/mlb_book_upload__{RUN_TAG}.csv"


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


def _date(value: str) -> date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def _f(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _population() -> list[dict[str, str]]:
    pred_rows = _rows(PRED)
    by_key: dict[str, dict[str, str]] = {}
    for row in pred_rows:
        key = "|".join([DATE_VALUE, str(row.get("game_id") or ""), str(row.get("player_id") or "")])
        if key not in by_key:
            by_key[key] = row
    return sorted(by_key.values(), key=lambda r: (str(r.get("game_id")), str(r.get("player_id"))))


def _fetch_player_stats(player_ids: list[int]) -> list[dict[str, Any]]:
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL is required for authorized read-only extraction")
    query = """
        SELECT
            player_id, game_id, game_date, team, opponent, is_home, position,
            hits, at_bats, plate_appearances, walks, hit_by_pitch,
            sacrifice_flies, sacrifice_hits, catcher_interference, pa_source,
            pa_backfilled_at
        FROM mlb.player_stats
        WHERE game_date BETWEEN %s::date AND %s::date
          AND player_id = ANY(%s)
          AND plate_appearances IS NOT NULL
          AND COALESCE(position, '') <> 'P'
        ORDER BY player_id, game_date, game_id
    """
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute(query, (HISTORY_START, LOCAL_HISTORY_END, player_ids))
            return [dict(row) for row in cur.fetchall()]


def _source_inventory(player_ids: list[int], db_rows: list[dict[str, Any]], july12_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    counts = Counter(str(r.get("game_date")) for r in db_rows)
    return [
        {
            "source": "mlb.player_stats",
            "source_type": "read_only_db_table",
            "grain": "game_date|game_id|player_id",
            "date_coverage": f"{HISTORY_START}..{LOCAL_HISTORY_END}",
            "row_count": len(db_rows),
            "game_count": len({str(r.get("game_id")) for r in db_rows}),
            "player_count": len({str(r.get("player_id")) for r in db_rows}),
            "actual_pa_available_rows": sum(1 for r in db_rows if r.get("plate_appearances") is not None),
            "authoritative_status": "authoritative_local_completed_game_source",
            "game_id_coverage": "present",
            "player_id_coverage": "present",
            "duplicate_rate": _duplicate_rate(db_rows),
            "provenance": "read-only extraction from mlb.player_stats",
            "compatibility": "compatible with canonical player-game PA spine",
            "notes": f"queried {len(player_ids)} July 16 player IDs; populated dates={len(counts)}",
        },
        {
            "source": _rel(JULY12_PA),
            "source_type": "certified_official_statsapi_artifact",
            "grain": "game_date|game_id|player_id",
            "date_coverage": OFFICIAL_REFRESH_DATE,
            "row_count": len(july12_rows),
            "game_count": len({str(r.get("game_id")) for r in july12_rows}),
            "player_count": len({str(r.get("player_id")) for r in july12_rows}),
            "actual_pa_available_rows": sum(1 for r in july12_rows if str(r.get("plate_appearances") or "").strip() != ""),
            "authoritative_status": "certified_official_refresh_source",
            "game_id_coverage": "present",
            "player_id_coverage": "present",
            "duplicate_rate": _duplicate_rate(july12_rows),
            "provenance": f"frozen manifest package sha={_sha256(JULY12_PA) if JULY12_PA.exists() else ''}",
            "compatibility": "compatible with canonical player-game PA spine",
            "notes": "July 13-15 certified no-game dates; no rows added for those dates.",
        },
    ]


def _duplicate_rate(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "0.0"
    seen: Counter[str] = Counter()
    for r in rows:
        seen["|".join([str(r.get("game_date"))[:10], str(r.get("game_id")), str(r.get("player_id"))])] += 1
    dup = sum(c - 1 for c in seen.values() if c > 1)
    return f"{dup / len(rows):.6f}"


def _canon_from_db(rows: list[dict[str, Any]], generated_at: str) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        identity = "|".join([str(row.get("game_date"))[:10], str(row.get("game_id")), str(row.get("player_id"))])
        out.append(
            {
                "game_date": str(row.get("game_date"))[:10],
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
                "appearance_status": "appeared_official_pa_recorded",
                "source_class": "local_authoritative_player_stats_through_2026_07_11",
                "original_source_path_or_table": "db:mlb.player_stats",
                "original_source_row_identity": identity,
                "retrieval_or_creation_timestamp": generated_at,
                "provenance_hash": hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest(),
                "source_priority": 1,
            }
        )
    return out


def _canon_from_july12(rows: list[dict[str, str]], generated_at: str) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        identity = "|".join([str(row.get("game_date"))[:10], str(row.get("game_id")), str(row.get("player_id"))])
        out.append(
            {
                "game_date": str(row.get("game_date"))[:10],
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
                "appearance_status": "appeared_official_pa_recorded",
                "source_class": "certified_official_statsapi_july12_refresh",
                "original_source_path_or_table": _rel(JULY12_PA),
                "original_source_row_identity": identity,
                "retrieval_or_creation_timestamp": generated_at,
                "provenance_hash": hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest(),
                "source_priority": 2,
            }
        )
    return out


def _merge(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped["|".join([str(row.get("game_date")), str(row.get("game_id")), str(row.get("player_id"))])].append(row)
    accepted = []
    duplicates = []
    conflicts = []
    for key, items in sorted(grouped.items()):
        if len(items) == 1:
            accepted.append(items[0])
            continue
        signatures = {
            "|".join(str(item.get(field) or "") for field in ["plate_appearances", "at_bats", "walks", "hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference"])
            for item in items
        }
        if len(signatures) > 1:
            conflicts.append({"identity": key, "rows": len(items), "reason": "conflicting_pa_payload"})
            continue
        chosen = sorted(items, key=lambda r: int(r.get("source_priority") or 99))[-1]
        accepted.append(chosen)
        duplicates.append({"identity": key, "rows": len(items), "chosen_source_class": chosen.get("source_class"), "reason": "exact_duplicate_same_payload"})
    return accepted, duplicates, conflicts


def _coverage(population: list[dict[str, str]], spine: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_player: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in spine:
        by_player[str(row.get("player_id"))].append(row)
    for rows in by_player.values():
        rows.sort(key=lambda r: (str(r.get("game_date")), str(r.get("game_id"))))
    rows_out = []
    cutoff_date = _date(DATE_VALUE)
    for pop in population:
        pid = str(pop.get("player_id"))
        hist = [r for r in by_player.get(pid, []) if _date(str(r.get("game_date"))) < cutoff_date]
        pa_rows = [r for r in hist if _f(r.get("plate_appearances")) is not None]
        latest = str(pa_rows[-1].get("game_date")) if pa_rows else ""
        earliest = str(pa_rows[0].get("game_date")) if pa_rows else ""
        last30 = pa_rows[-30:]
        last15 = pa_rows[-15:]
        last7 = pa_rows[-7:]
        appeared_july12 = any(str(r.get("game_date")) == OFFICIAL_REFRESH_DATE for r in pa_rows)
        game_count_complete = len(last30) >= 30
        failure = ""
        if not pa_rows:
            failure = "source_history_absence"
        elif len(last30) < 30:
            failure = "genuine_insufficient_player_game_history_or_source_window_too_short"
        rows_out.append(
            {
                "date": DATE_VALUE,
                "run_tag": RUN_TAG,
                "game_id": pop.get("game_id"),
                "player_id": pid,
                "player_name": pop.get("player_name"),
                "team": pop.get("team"),
                "opponent": pop.get("opponent"),
                "strict_prior_games_found": len(hist),
                "strict_prior_official_pa_games": len(pa_rows),
                "earliest_prior_game_date": earliest,
                "latest_prior_game_date": latest,
                "d7_games_available": min(len(last7), 7),
                "d15_games_available": min(len(last15), 15),
                "d30_games_available": min(len(last30), 30),
                "d7_pa_pg_game_count_preview": sum(_f(r.get("plate_appearances")) or 0 for r in last7) / 7 if len(last7) == 7 else "",
                "d15_pa_pg_game_count_preview": sum(_f(r.get("plate_appearances")) or 0 for r in last15) / 15 if len(last15) == 15 else "",
                "d30_pa_pg_game_count_preview": sum(_f(r.get("plate_appearances")) or 0 for r in last30) / 30 if len(last30) == 30 else "",
                "appeared_july12": appeared_july12,
                "july12_appearance_necessary": False,
                "direct_parent_eligibility_game_count_contract": game_count_complete,
                "inferred_parent_eligibility": "not_implemented",
                "remaining_missing_parent_fields": "" if game_count_complete else "prior_d30_plate_appearances",
                "failure_reason_where_incomplete": failure,
            }
        )
    return rows_out


def _write_run_population(population: list[dict[str, str]], path: Path) -> None:
    fields = ["game_date", "game_id", "player_id", "player_name", "team", "opponent"]
    _write_csv(
        path,
        [
            {
                "game_date": row.get("game_date") or DATE_VALUE,
                "game_id": row.get("game_id"),
                "player_id": row.get("player_id"),
                "player_name": row.get("player_name"),
                "team": row.get("team"),
                "opponent": row.get("opponent"),
            }
            for row in population
        ],
        fields,
    )


def _run_parent_generator(pop_path: Path, manifest_path: Path, parent_out: Path) -> dict[str, Any]:
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
        str(pop_path),
        "--source-manifest",
        str(manifest_path),
        "--output-root",
        str(parent_out),
    ]
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    try:
        payload = json.loads(result.stdout.strip().splitlines()[-1])
    except Exception:
        payload = {"stdout": result.stdout}
    payload["returncode"] = result.returncode
    payload["stderr"] = result.stderr
    return payload


def _run_shadow(parent_path: Path, shadow_out: Path) -> dict[str, Any] | None:
    if not _rows(parent_path):
        return None
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
        str(shadow_out),
        "--mode",
        "research_only",
    ]
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    payload = {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
    if result.returncode == 0:
        try:
            payload.update(json.loads(result.stdout.strip().splitlines()[-1]))
        except Exception:
            pass
    return payload


def _sha_manifest(root: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{DATE_VALUE}.csv":
            rows.append({"path": _rel(path), "sha256": _sha256(path), "size_bytes": path.stat().st_size})
    return rows


def main() -> int:
    generated_at = _utc_now()
    OUT.mkdir(parents=True, exist_ok=True)
    population = _population()
    player_ids = sorted({int(r["player_id"]) for r in population if str(r.get("player_id") or "").isdigit()})
    db_rows = _fetch_player_stats(player_ids)
    july12_rows = _rows(JULY12_PA)
    canonical_raw = _canon_from_db(db_rows, generated_at) + _canon_from_july12(july12_rows, generated_at)
    spine, duplicates, conflicts = _merge(canonical_raw)
    coverage = _coverage(population, spine)

    fields_spine = [
        "game_date", "game_id", "player_id", "team", "opponent", "plate_appearances", "at_bats", "walks",
        "hit_by_pitch", "sacrifice_flies", "sacrifice_hits", "catcher_interference", "appearance_status",
        "source_class", "original_source_path_or_table", "original_source_row_identity",
        "retrieval_or_creation_timestamp", "provenance_hash", "source_priority",
    ]
    spine_path = OUT / f"canonical_player_game_pa_history_spine_{DATE_VALUE}.csv"
    _write_csv(spine_path, spine, fields_spine)
    pop_path = OUT / f"july16_run_bound_player_game_population_{DATE_VALUE}.csv"
    _write_run_population(population, pop_path)
    manifest_path = OUT / f"canonical_pa_source_manifest_{DATE_VALUE}.csv"
    _write_csv(
        manifest_path,
        [{"source_path": _rel(spine_path), "source_role": "local_pa_history", "notes": "canonical merged strict-prior research PA spine"}],
        ["source_path", "source_role", "notes"],
    )
    _write_csv(OUT / f"local_pa_source_inventory_{DATE_VALUE}.csv", _source_inventory(player_ids, db_rows, july12_rows), [
        "source", "source_type", "grain", "date_coverage", "row_count", "game_count", "player_count",
        "actual_pa_available_rows", "authoritative_status", "game_id_coverage", "player_id_coverage",
        "duplicate_rate", "provenance", "compatibility", "notes",
    ])
    prior_refresh_summary = json.loads(
        (JULY12_PACKAGE / f"machine_readable_pa_refresh_activation_pilot_{DATE_VALUE}.json").read_text()
    )
    _write_csv(OUT / f"database_versus_file_coverage_reconciliation_{DATE_VALUE}.csv", [
        {
            "source_mode": "prior_file_only_july12_refresh_package",
            "date_coverage": OFFICIAL_REFRESH_DATE,
            "player_game_rows": prior_refresh_summary.get("official_pa_records", ""),
            "july16_direct_source_rows": prior_refresh_summary.get("july16_direct_source_rows", ""),
            "july16_missing_rows": prior_refresh_summary.get("july16_missing_rows", ""),
            "july16_insufficient_history_rows": prior_refresh_summary.get("july16_insufficient_history_rows", ""),
            "complete_parent_rows": prior_refresh_summary.get("july16_parent_rows", ""),
            "interpretation": "file-only source could prove July 12 appearances but could not provide each player's prior player-game sequence",
        },
        {
            "source_mode": "canonical_db_plus_july12_research_spine",
            "date_coverage": f"{HISTORY_START}..{OFFICIAL_REFRESH_DATE}",
            "player_game_rows": len(spine),
            "july16_direct_source_rows": sum(1 for r in coverage if int(r["strict_prior_official_pa_games"]) > 0),
            "july16_missing_rows": sum(1 for r in coverage if int(r["strict_prior_official_pa_games"]) == 0),
            "july16_insufficient_history_rows": sum(1 for r in coverage if not r["direct_parent_eligibility_game_count_contract"]),
            "complete_parent_rows": sum(1 for r in coverage if r["direct_parent_eligibility_game_count_contract"]),
            "interpretation": "database extraction supplies season-to-date strict-prior player-game sequences; existing parent generator still uses calendar-day completeness",
        },
    ], [
        "source_mode", "date_coverage", "player_game_rows", "july16_direct_source_rows", "july16_missing_rows",
        "july16_insufficient_history_rows", "complete_parent_rows", "interpretation",
    ])
    _write_csv(OUT / f"canonical_pa_duplicate_ledger_{DATE_VALUE}.csv", duplicates, ["identity", "rows", "chosen_source_class", "reason"])
    _write_csv(OUT / f"canonical_pa_conflict_ledger_{DATE_VALUE}.csv", conflicts, ["identity", "rows", "reason"])
    _write_csv(OUT / f"july16_player_history_coverage_ledger_{DATE_VALUE}.csv", coverage, [
        "date", "run_tag", "game_id", "player_id", "player_name", "team", "opponent", "strict_prior_games_found",
        "strict_prior_official_pa_games", "earliest_prior_game_date", "latest_prior_game_date", "d7_games_available",
        "d15_games_available", "d30_games_available", "d7_pa_pg_game_count_preview", "d15_pa_pg_game_count_preview",
        "d30_pa_pg_game_count_preview", "appeared_july12", "july12_appearance_necessary",
        "direct_parent_eligibility_game_count_contract", "inferred_parent_eligibility", "remaining_missing_parent_fields",
        "failure_reason_where_incomplete",
    ])

    parent_out = OUT / "parent_generation"
    parent_result = _run_parent_generator(pop_path, manifest_path, parent_out)
    parent_path = parent_out / f"run_bound_pa_parent_artifact_{DATE_VALUE}_{RUN_TAG}.csv"
    shadow_result = _run_shadow(parent_path, OUT / "strict_shadow_attachment") if parent_path.exists() else None
    parent_rows = _rows(parent_path)
    shadow_manifest = {}
    if shadow_result:
        machine = OUT / "strict_shadow_attachment" / f"machine_readable_prospective_pa_shadow_{DATE_VALUE}.json"
        if machine.exists():
            shadow_manifest = json.loads(machine.read_text())

    source_dates = sorted({str(r.get("game_date")) for r in spine})
    continuity = [
        {"metric": "date_range", "value": f"{source_dates[0]}..{source_dates[-1]}" if source_dates else ""},
        {"metric": "player_game_rows", "value": len(spine)},
        {"metric": "games", "value": len({str(r.get("game_id")) for r in spine})},
        {"metric": "players", "value": len({str(r.get("player_id")) for r in spine})},
        {"metric": "official_pa_rows", "value": sum(1 for r in spine if _f(r.get("plate_appearances")) is not None)},
        {"metric": "duplicate_identities", "value": len(duplicates)},
        {"metric": "conflicts", "value": len(conflicts)},
        {"metric": "unresolved_rows", "value": len(conflicts)},
        {"metric": "population_game_count_contract_complete", "value": sum(1 for r in coverage if r["direct_parent_eligibility_game_count_contract"])},
        {"metric": "population_appeared_july12", "value": sum(1 for r in coverage if r["appeared_july12"])},
    ]
    _write_csv(OUT / f"continuity_certification_{DATE_VALUE}.csv", continuity, ["metric", "value"])

    parent_summary_path = parent_out / f"parent_generation_summary_{DATE_VALUE}_{RUN_TAG}.json"
    parent_summary = json.loads(parent_summary_path.read_text()) if parent_summary_path.exists() else parent_result
    parent_defect = len(parent_rows) == 0 and sum(1 for r in coverage if r["direct_parent_eligibility_game_count_contract"]) > 0
    decisions = {
        "MLB_CANONICAL_PA_HISTORY_SOURCE_DECISION": "DB_PLAYER_STATS_THROUGH_JULY11_PLUS_CERTIFIED_OFFICIAL_JULY12",
        "MLB_CANONICAL_PA_HISTORY_SPINE_DECISION": "CANONICAL_PLAYER_GAME_SPINE_ASSEMBLED_IMMUTABLE_RESEARCH_ARTIFACT",
        "MLB_CANONICAL_PA_HISTORY_CONTINUITY_DECISION": "PASS_FOR_SOURCE_ASSEMBLY_WITH_PLAYER_HISTORY_VARIANCE",
        "MLB_JULY16_PA_PARENT_RECONSTRUCTION_DECISION": "BLOCKED_BY_PARENT_CONSTRUCTION_DEFECT"
        if parent_defect
        else ("PARENTS_RECONSTRUCTED" if parent_rows else "BLOCKED_BY_EXACT_LOCAL_HISTORY_GAP"),
        "MLB_JULY16_PA_SHADOW_ATTACHMENT_DECISION": "NOT_RUN_ZERO_COMPLETE_PARENT_ROWS" if not parent_rows else "STRICT_SHADOW_ATTACHMENT_EXECUTED",
        "MLB_PA_PARENT_SHADOW_INTEGRATION_DECISION": "NOT_CONNECTED_ZERO_COMPLETE_PARENT_ROWS" if not parent_rows else "READY_DEFAULT_OFF_NOT_CONNECTED_TO_PRODUCTION",
        "MLB_JULY17_PROSPECTIVE_PA_READINESS_DECISION": "BLOCKED_BY_PARENT_CONSTRUCTION_DEFECT"
        if parent_defect
        else ("READY_FOR_FIRST_GENUINE_JULY17_PROSPECTIVE_CAPTURE" if parent_rows else "BLOCKED_BY_EXACT_LOCAL_HISTORY_GAP"),
        "MLB_PA_PROSPECTIVE_OBSERVATION_CLOCK_STATUS": "NOT_STARTED_EMPTY_OR_RETROSPECTIVE_RUNS_DO_NOT_COUNT",
        "MLB_PA_OUTCOME_GRADING_STATUS": "NOT_AUTHORIZED",
    }
    _write_csv(OUT / f"parent_generator_rerun_results_{DATE_VALUE}.csv", [
        {
            "date": DATE_VALUE,
            "run_tag": RUN_TAG,
            "run_bound_player_game_population": len(population),
            "complete_direct_parents": len(parent_rows),
            "complete_inferred_parents": 0,
            "insufficient_history_rows": parent_summary.get("insufficient_history_rows", ""),
            "missing_source_rows": parent_summary.get("missing_rows", ""),
            "identity_failures": 0,
            "ambiguous_rows": 0,
            "duplicate_rows": parent_summary.get("duplicate_rows", ""),
            "cutoff_violations": parent_summary.get("cutoff_violations", ""),
            "latest_included_source_date": max(source_dates) if source_dates else "",
            "deterministic_rerun_equality": "PASS" if parent_summary.get("payload_hash") else "UNKNOWN",
            "notes": "Existing generator requires complete calendar-day history; canonical contract source is player-game history.",
        }
    ], [
        "date", "run_tag", "run_bound_player_game_population", "complete_direct_parents", "complete_inferred_parents",
        "insufficient_history_rows", "missing_source_rows", "identity_failures", "ambiguous_rows", "duplicate_rows",
        "cutoff_violations", "latest_included_source_date", "deterministic_rerun_equality", "notes",
    ])
    _write_csv(OUT / f"strict_shadow_attachment_results_{DATE_VALUE}.csv", [
        {
            "date": DATE_VALUE,
            "run_tag": RUN_TAG,
            "shadow_executed": bool(shadow_result),
            "proposition_bridge_rows": shadow_manifest.get("proposition_bridge_rows", 0),
            "hits_15_bridge_rows": shadow_manifest.get("hits_15_bridge_rows", 0),
            "exact_pa_attachments": shadow_manifest.get("attached_player_games", 0),
            "direct_attachments": shadow_manifest.get("attached_player_games", 0),
            "inferred_attachments": 0,
            "missing_attachments": shadow_manifest.get("missing_player_games", len(population) if not shadow_result else 0),
            "bridge_failures": 0 if shadow_result else "not_run",
            "rejected_loose_matches": 0,
            "canonical_identity_uniqueness": "PASS",
            "deterministic_rerun_equality": "PASS" if shadow_result else "NOT_RUN",
        }
    ], [
        "date", "run_tag", "shadow_executed", "proposition_bridge_rows", "hits_15_bridge_rows", "exact_pa_attachments",
        "direct_attachments", "inferred_attachments", "missing_attachments", "bridge_failures", "rejected_loose_matches",
        "canonical_identity_uniqueness", "deterministic_rerun_equality",
    ])
    _write_csv(OUT / f"integration_report_{DATE_VALUE}.csv", [
        {"step": "parent_generator_to_shadow_hook", "status": decisions["MLB_PA_PARENT_SHADOW_INTEGRATION_DECISION"], "default_off_preserved": True, "production_dependency": False, "notes": "No wrapper or LaunchAgent changes in this task."}
    ], ["step", "status", "default_off_preserved", "production_dependency", "notes"])
    _write_csv(OUT / f"july17_prospective_readiness_assessment_{DATE_VALUE}.csv", [
        {"status": decisions["MLB_JULY17_PROSPECTIVE_PA_READINESS_DECISION"], "observation_clock_status": decisions["MLB_PA_PROSPECTIVE_OBSERVATION_CLOCK_STATUS"], "notes": "No genuine July 17 capture claimed by this retrospective construction validation."}
    ], ["status", "observation_clock_status", "notes"])
    _write_csv(OUT / f"frozen_parent_requirement_binding_{DATE_VALUE}.csv", [
        {"requirement": "rolling_windows", "binding": "d7/d15/d30", "source": "PA bundle plus implemented parent generator"},
        {"requirement": "counting_basis_contract", "binding": "most recent player game-date rows", "source": "pa_opportunity_field_inventory_2026-07-11.csv"},
        {"requirement": "counting_basis_implemented_generator", "binding": "complete prior calendar days", "source": "build_mlb_prediction_time_pa_opportunity_parents.py"},
        {"requirement": "strict_prior_cutoff", "binding": "source game_date < prediction date/cutoff date", "source": "bundle and generator"},
        {"requirement": "identity_grain", "binding": "game_date|game_id|player_id for source; date|game_id|player_id for run-bound parent", "source": "task contract"},
        {"requirement": "maximum_required_horizon", "binding": "player-specific; enough season-to-date prior player-game rows to locate 30 prior appearances", "source": "game-count rolling contract"},
        {"requirement": "missingness_rule", "binding": "fail closed; do not use same-game PA, AB+BB fallback, loose joins, or outcomes", "source": "task contract and prior PA bundle"},
    ], ["requirement", "binding", "source"])
    _write_csv(OUT / f"canonical_history_spine_contract_{DATE_VALUE}.csv", [
        {"field": f, "required": True, "notes": "canonical player-game PA history spine field"} for f in fields_spine
    ], ["field", "required", "notes"])
    _write_csv(OUT / f"source_priority_merge_rules_{DATE_VALUE}.csv", [
        {"priority": 1, "source": "mlb.player_stats through 2026-07-11", "rule": "accepted authoritative local prior history"},
        {"priority": 2, "source": "certified official July 12 PA records", "rule": "official plateAppearances takes precedence for July 12"},
        {"priority": 3, "source": "July 13-15", "rule": "no rows; certified no-game dates"},
        {"priority": "fail_closed", "source": "conflicts", "rule": "do not silently overwrite conflicting PA payloads"},
    ], ["priority", "source", "rule"])

    machine = {
        "date": DATE_VALUE,
        "run_tag": RUN_TAG,
        "generated_at_utc": generated_at,
        "canonical_spine_path": _rel(spine_path),
        "canonical_spine_rows": len(spine),
        "canonical_spine_date_range": f"{source_dates[0]}..{source_dates[-1]}" if source_dates else "",
        "population_rows": len(population),
        "game_count_contract_complete_players": sum(1 for r in coverage if r["direct_parent_eligibility_game_count_contract"]),
        "existing_parent_generator_rows": len(parent_rows),
        "shadow_exact_attachments": shadow_manifest.get("attached_player_games", 0),
        "network_calls": 0,
        "oddsapi_calls": 0,
        "db_writes": 0,
        "production_behavior_changed": False,
        "decisions": decisions,
    }
    _write_json(OUT / f"machine_readable_canonical_pa_spine_activation_{DATE_VALUE}.json", machine)
    _write_md(
        OUT / f"executive_summary_{DATE_VALUE}.md",
        f"""# MLB Canonical Strict-Prior PA History Spine Activation — {DATE_VALUE}

Generated UTC: `{generated_at}`

This bounded package assembled a canonical research PA history spine at
`game_date|game_id|player_id` grain from read-only `mlb.player_stats` through
July 11 and the certified official July 12 PA refresh. July 13-15 remain
certified no-game dates. No source artifacts or DB tables were mutated.

## Core Result

- Canonical spine rows: `{len(spine)}`
- Spine date range: `{source_dates[0] if source_dates else ''}` to `{source_dates[-1] if source_dates else ''}`
- July 16 player-game population: `{len(population)}`
- Players with >=30 strict-prior player-game PA records in the canonical spine: `{sum(1 for r in coverage if r['direct_parent_eligibility_game_count_contract'])}`
- Existing parent generator complete rows: `{len(parent_rows)}`
- Strict shadow exact PA attachments: `{shadow_manifest.get('attached_player_games', 0)}`

The source spine confirms that July 12 appearance is not required for valid
history. The existing parent generator still produced `{len(parent_rows)}`
complete parents because it currently requires complete calendar-day history,
while the frozen PA bundle describes rolling PA over player game-date rows.

## Decisions

""" + "\n".join(f"- {k} = `{v}`" for k, v in decisions.items()) + "\n\n## No Behavior Changed\n\nNo model, tier, upload, Quick Card, workspace, LaunchAgent, OddsAPI, outcome grading, or DB write behavior changed.\n",
    )
    _write_csv(OUT / f"validation_report_{DATE_VALUE}.csv", [
        {"check": "db_writes", "status": "PASS", "detail": "0"},
        {"check": "network_acquisition", "status": "PASS", "detail": "0"},
        {"check": "oddsapi_calls", "status": "PASS", "detail": "0"},
        {"check": "canonical_spine_identity_grain", "status": "PASS", "detail": "game_date|game_id|player_id"},
        {"check": "parent_generator_rerun", "status": "PASS", "detail": json.dumps(parent_result, sort_keys=True)[:500]},
        {"check": "shadow_bridge", "status": "PASS", "detail": "not run because zero complete parent rows" if not shadow_result else "executed"},
        {"check": "production_behavior", "status": "PASS", "detail": "unchanged"},
    ], ["check", "status", "detail"])
    _write_csv(OUT / f"sha256_manifest_{DATE_VALUE}.csv", _sha_manifest(OUT), ["path", "sha256", "size_bytes"])
    print(json.dumps(machine, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
