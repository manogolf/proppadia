"""Write-once 2026 MLB postseason closeout package (offseason conditions 1-3).

The package binds the 40-game postseason population from the active V7
phase authority, proves each game reached one playable final in the pinned
retained schedule, summarises every lane's predictions and grades by exact
gamePk, and totals the postseason paid requests. Postponed-date predictions
are void (never graded). Known exceptions are listed, not hidden.

Reads only: committed authority files, the pinned retained schedule, local
lane stores and, for Moneyline, the database through the restricted research
login. Contract: docs/contracts/mlb_2026_regular_season_close_and_postseason_data_plan_v1/README.md.
"""
from __future__ import annotations

import csv
import glob
import hashlib
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

from backend.mlb.public_game_predictions.finality_v1 import PLAYABLE_TERMINAL, classify_playable_terminal
from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    REPO_ROOT,
    CloseInventoryError,
    canonical_json_bytes,
)
from backend.mlb.season_transition.regular_season_close_operation_v1 import publish_immutable

SCHEMA = "MLB_2026_POSTSEASON_CLOSEOUT_V1"
DEFAULT_OUTPUT = REPO_ROOT / "artifacts/operational/mlb/season_close/2026/postseason_closeout.json"
POSTSEASON_GAME_COUNT = 40

AUTHORITY_SELECTION = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/active_selection.json"
AUTHORITY_DESCRIPTOR_SHA256 = "a67b2fe3a0ae2a13a1d4487524c8996418c95f396faa0a2fbadc66cf3fe05751"  # V7
AUTHORITY_PROPOSAL = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v7/canonical_game_phase_full_snapshot.jsonl"
SCHEDULE = REPO_ROOT / (
    "artifacts/ops/mlb_public_game_moneyline_history_schedules/2026-10-29/"
    "20261029T123006280535Z_2026-08-05_2026-10-29_bca22f86480b875d7633fc24d73e57dfe44b2065561f6ff084e9323a644580c2.json"
)
SCHEDULE_SHA256 = "bca22f86480b875d7633fc24d73e57dfe44b2065561f6ff084e9323a644580c2"

MODEL_V2 = REPO_ROOT / "backend/mlb/exports/model_v2"
RAW_TOTALS = MODEL_V2 / "totals_shadow_v1/totals_shadow_v1.sqlite3"
TOTALS_C = MODEL_V2 / "totals_c_shadow_v1/totals_c_shadow_v1.sqlite3"
HITS05 = MODEL_V2 / "hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3"
AGREEMENT_V4 = MODEL_V2 / "mlb_market_strong_agreement_separation_prospective_v1.sqlite3"
DH_FORWARD = REPO_ROOT / "backend/mlb/data/research/dh_forward_validation/v1"
STARTER_INDEX = REPO_ROOT / (
    "artifacts/analysis/model_development/mlb_starter_skill_workload_prospective_observation/"
    "starter_skill_workload_observation_index.csv"
)
PINNACLE_GLOB = "backend/mlb/exports/market_history/full_game_totals/*/*/pinnacle/pinnacle_capture_summary.json"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _rel(path: Path) -> str:
    return str(Path(path).relative_to(REPO_ROOT))


# --- population -------------------------------------------------------------

def postseason_population() -> dict[int, dict[str, Any]]:
    """The postseason rows of the active authority, which must be V7."""
    selection = json.loads(AUTHORITY_SELECTION.read_text())
    if selection.get("descriptor_sha256") != AUTHORITY_DESCRIPTOR_SHA256:
        raise CloseInventoryError("POSTSEASON_CLOSEOUT_AUTHORITY_NOT_V7")
    rows = [json.loads(line) for line in AUTHORITY_PROPOSAL.read_text().splitlines() if line.strip()]
    population = {int(r["game_pk"]): r for r in rows if r.get("season_phase") == "POSTSEASON"}
    if len(population) != POSTSEASON_GAME_COUNT:
        raise CloseInventoryError("POSTSEASON_CLOSEOUT_POPULATION_NOT_40")
    return population


def final_appearances(schedule: Mapping[str, Any], game_pks: Iterable[int]) -> dict[int, dict[str, Any]]:
    """One playable final per game; earlier postponed appearances are listed."""
    wanted = set(game_pks)
    seen: dict[int, list[dict[str, Any]]] = {pk: [] for pk in wanted}
    for block in schedule.get("dates") or []:
        for game in block.get("games") or []:
            pk = int(game.get("gamePk") or 0)
            if pk in wanted:
                seen[pk].append(game)
    out: dict[int, dict[str, Any]] = {}
    for pk, games in seen.items():
        finals = [g for g in games if classify_playable_terminal(g).classification == PLAYABLE_TERMINAL]
        if len(finals) != 1:
            raise CloseInventoryError(f"POSTSEASON_GAME_NOT_ONE_PLAYABLE_FINAL:{pk}:{len(finals)}")
        final = finals[0]
        out[pk] = {
            "official_date": final.get("officialDate"),
            "game_type": final.get("gameType"),
            "series_description": final.get("seriesDescription"),
            "detailed_state": (final.get("status") or {}).get("detailedState"),
            "away_runs": ((final.get("teams") or {}).get("away") or {}).get("score"),
            "home_runs": ((final.get("teams") or {}).get("home") or {}).get("score"),
            "other_appearances": sorted(
                f"{g.get('officialDate')}:{(g.get('status') or {}).get('detailedState')}"
                for g in games if g is not final),
        }
    return out


# --- lane summaries (pure) --------------------------------------------------

def summarize_lane(
    lane: str,
    predictions: Iterable[tuple[int, str, str]],
    graded_identities: set[str],
    official_dates: Mapping[int, str],
) -> dict[str, Any]:
    """Classify each (game_pk, prediction_date, identity) postseason prediction.

    GRADED, VOID_POSTPONED_DATE (made for a date the game did not finish on,
    left ungraded), GRADED_ON_POSTPONED_DATE (a known exception), or
    UNGRADED_ELIGIBLE (a blocker).
    """
    counts: Counter[str] = Counter()
    games: dict[str, set[int]] = {}
    for pk, prediction_date, identity in predictions:
        on_own_date = prediction_date == official_dates[pk]
        graded = identity in graded_identities
        status = ("GRADED" if graded else "UNGRADED_ELIGIBLE") if on_own_date else (
            "GRADED_ON_POSTPONED_DATE" if graded else "VOID_POSTPONED_DATE")
        counts[status] += 1
        games.setdefault(status, set()).add(pk)
    predicted = set().union(*games.values()) if games else set()
    return {
        "lane": lane,
        "postseason_games_predicted": len(predicted),
        "row_counts": dict(sorted(counts.items())),
        "game_pks_by_status": {k: sorted(v) for k, v in sorted(games.items()) if k != "GRADED"},
        "ungraded_eligible_rows": counts["UNGRADED_ELIGIBLE"],
    }


def summarize_credits(summaries: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(summaries)
    dates = sorted({str(r.get("game_date")) for r in rows})
    return {
        "provider": "THE_ODDS_API (bookmaker pinnacle, main markets)",
        "captures": len(rows),
        "requests": sum(int(r.get("request_count") or 0) for r in rows),
        "credits_charged": sum(int((r.get("request_cost_headers") or {}).get("x-requests-last") or 0) for r in rows),
        "non_200_responses": sum(r.get("http_status") != 200 for r in rows),
        "game_date_range": [dates[0], dates[-1]] if dates else [],
    }


# --- lane readers -----------------------------------------------------------

def _sqlite(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _sqlite_lane(lane: str, path: Path, pred_table: str, out_table: str, date_col: str,
                 population: set[int], official: Mapping[int, str]) -> dict[str, Any]:
    with _sqlite(path) as conn:
        predictions = [(int(g), str(d), str(i)) for g, d, i in conn.execute(
            f"SELECT game_id, {date_col}, canonical_identity FROM {pred_table}") if int(g) in population]
        graded = {str(r[0]) for r in conn.execute(f"SELECT canonical_identity FROM {out_table}")}
    return {**summarize_lane(lane, predictions, graded, official),
            "store": _rel(path), "store_sha256": sha256_file(path)}


def _dh_forward(population: set[int], official: Mapping[int, str]) -> dict[str, Any]:
    pred_path = DH_FORWARD / "forward_prediction_ledger_v1.csv"
    out_path = DH_FORWARD / "forward_outcome_ledger_v1.csv"
    with pred_path.open() as f:
        predictions = [(int(r["game_pk"]), r["game_date"], r["canonical_identity"])
                       for r in csv.DictReader(f) if r["game_pk"] and int(r["game_pk"]) in population]
    with out_path.open() as f:
        graded = {r["canonical_identity"] for r in csv.DictReader(f)}
    return {**summarize_lane("DH_FORWARD", predictions, graded, official),
            "store": [_rel(pred_path), _rel(out_path)],
            "store_sha256": [sha256_file(pred_path), sha256_file(out_path)]}


def _moneyline(population: set[int], official: Mapping[int, str]) -> dict[str, Any]:
    from backend.shared.db.mlb_research import mlb_research_readonly_connection

    pks = sorted(population)
    with mlb_research_readonly_connection() as conn:
        preds = conn.execute(
            "SELECT game_id, game_date::text AS game_date FROM mlb.public_game_moneyline_predictions "
            "WHERE game_id = ANY(%s) AND admission_status = 'ADMITTED_SHADOW'", (pks,)).fetchall()
        outs = conn.execute(
            "SELECT game_id, game_date::text AS game_date FROM mlb.public_game_moneyline_outcomes "
            "WHERE game_id = ANY(%s)", (pks,)).fetchall()

    def key(row: Any) -> tuple[int, str]:
        return (int(row["game_id"]), str(row["game_date"])) if isinstance(row, dict) else (int(row[0]), str(row[1]))

    predictions = [(pk, day, f"{pk}:{day}") for pk, day in map(key, preds)]
    graded = {f"{pk}:{day}" for pk, day in map(key, outs)}
    summary = summarize_lane("MONEYLINE", predictions, graded, official)
    summary["games_never_predicted"] = sorted(population - {pk for pk, _, _ in predictions})
    summary["store"] = "mlb.public_game_moneyline_predictions / mlb.public_game_moneyline_outcomes (research login)"
    summary["query_result_sha256"] = hashlib.sha256(canonical_json_bytes(
        {"predictions": sorted(map(list, map(key, preds))), "outcomes": sorted(map(list, map(key, outs)))})).hexdigest()
    return summary


def _not_run_on_postseason(lane: str, path: Path, table: str, population: set[int], reason: str) -> dict[str, Any]:
    with _sqlite(path) as conn:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
        col = next(c for c in ("game_id", "game_pk") if c in cols)
        found = sorted({int(r[0]) for r in conn.execute(f"SELECT {col} FROM {table}")} & population)
    if found:
        raise CloseInventoryError(f"POSTSEASON_CLOSEOUT_UNEXPECTED_ROWS:{lane}:{found}")
    return {"lane": lane, "postseason_games_predicted": 0, "status": "DID_NOT_RUN_ON_POSTSEASON",
            "reason": reason, "ungraded_eligible_rows": 0, "store": _rel(path), "store_sha256": sha256_file(path)}


def _starter_workload(official: Mapping[int, str]) -> dict[str, Any]:
    days = set(official.values())
    with STARTER_INDEX.open() as f:
        rows = [r for r in csv.DictReader(f) if r["slate_date"] in days]
    return {
        "lane": "STARTER_SKILL_WORKLOAD_OBSERVATION",
        "postseason_slates_observed": len(rows),
        "outcome_binding_status_counts": dict(Counter(r["outcome_binding_status"] for r in rows)),
        "status": "KNOWN_EXCEPTION_OUTCOMES_NEVER_BOUND_ALL_SEASON",
        "ungraded_eligible_rows": 0,
        "store": _rel(STARTER_INDEX), "store_sha256": sha256_file(STARTER_INDEX),
    }


# --- package ----------------------------------------------------------------

KNOWN_EXCEPTIONS = [
    {"game_pk": 849817, "lane": "MONEYLINE",
     "detail": "Predicted for 10-16 (postponed) and 10-18; both graded by the 10-18 final before the "
               "2026-11-04 grader fix. Append-only row kept; see "
               "docs/contracts/mlb_moneyline_postponed_rescheduled_finality_correction_v1/README.md."},
    {"lane": "MONEYLINE",
     "detail": "Seven postseason games were never predicted (failed runs 10-02/10-03, a 10-04 DNS "
               "failure, games already started). Coverage gap; not reconstructable."},
    {"lane": "STARTER_SKILL_WORKLOAD_OBSERVATION",
     "detail": "Outcomes were never bound for any slate all season (lane-wide)."},
]


def build_package() -> dict[str, Any]:
    population = postseason_population()
    if sha256_file(SCHEDULE) != SCHEDULE_SHA256:
        raise CloseInventoryError("POSTSEASON_CLOSEOUT_SCHEDULE_HASH_MISMATCH")
    finals = final_appearances(json.loads(SCHEDULE.read_bytes()), population)
    official = {pk: f["official_date"] for pk, f in finals.items()}
    pks = set(population)
    lanes = [
        _moneyline(pks, official),
        _sqlite_lane("RAW_TOTALS", RAW_TOTALS, "totals_shadow_predictions", "totals_shadow_outcomes",
                     "game_date", pks, official),
        _sqlite_lane("FULL_BOARD_HITS", HITS05, "hits05_full_board_predictions", "hits05_full_board_outcomes",
                     "slate_date", pks, official),
        _dh_forward(pks, official),
        _not_run_on_postseason("TOTALS_C", TOTALS_C, "totals_c_shadow_predictions", pks,
                               "Runs only after RAW Totals completes; last game 2026-09-27."),
        _not_run_on_postseason("MARKET_STRONG_AGREEMENT_V4", AGREEMENT_V4,
                               _first_table_with_game(AGREEMENT_V4), pks,
                               "SKIPPED_OUTSIDE_FROZEN_REGULAR_SEASON_HORIZON (no charged request)."),
        _starter_workload(official),
    ]
    summaries = [json.loads(Path(p).read_text()) for p in sorted(glob.glob(str(REPO_ROOT / PINNACLE_GLOB)))]
    window = (min(official.values()), max(official.values()))
    credits = summarize_credits(s for s in summaries if window[0] <= str(s.get("game_date")) <= window[1])
    blockers = [lane["lane"] for lane in lanes if lane["ungraded_eligible_rows"]]
    if credits["non_200_responses"]:
        blockers.append("PAID_REQUEST_NON_200")
    return {
        "schema": SCHEMA,
        "decision": "POSTSEASON_CLOSEOUT_READY" if not blockers else "POSTSEASON_CLOSEOUT_BLOCKED",
        "blockers": blockers,
        "population": {
            "authority_descriptor_sha256": AUTHORITY_DESCRIPTOR_SHA256,
            "authority_proposal_sha256": sha256_file(AUTHORITY_PROPOSAL),
            "schedule_path": _rel(SCHEDULE), "schedule_sha256": SCHEDULE_SHA256,
            "game_count": len(population),
            "round_counts": dict(sorted(Counter(r["postseason_round"] for r in population.values()).items())),
            "all_playable_final": True,
            "games": {str(pk): {"postseason_round": population[pk]["postseason_round"], **finals[pk]}
                      for pk in sorted(population)},
        },
        "lanes": lanes,
        "paid_requests": credits,
        "known_exceptions": KNOWN_EXCEPTIONS,
        "network_requests": 0,
        "database_writes": 0,
    }


def _first_table_with_game(path: Path) -> str:
    with _sqlite(path) as conn:
        for (table,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if cols & {"game_id", "game_pk"}:
                return table
    raise CloseInventoryError(f"POSTSEASON_CLOSEOUT_NO_GAME_TABLE:{_rel(path)}")


def execute(authorized_by: str, output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    if not authorized_by.strip():
        raise CloseInventoryError("POSTSEASON_CLOSEOUT_AUTHORIZATION_MISSING")
    body = build_package()
    if body["decision"] != "POSTSEASON_CLOSEOUT_READY":
        raise CloseInventoryError("POSTSEASON_CLOSEOUT_NOT_READY:" + ",".join(body["blockers"]))
    body["authorized_by"] = authorized_by
    body["package_sha256"] = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    publish_immutable(output, body)
    return body


def validate_package(path: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    document = json.loads(Path(path).read_text())
    claimed = document.pop("package_sha256", None)
    if claimed != hashlib.sha256(canonical_json_bytes(document)).hexdigest():
        raise CloseInventoryError("POSTSEASON_CLOSEOUT_HASH_MISMATCH")
    if document.get("schema") != SCHEMA or document.get("decision") != "POSTSEASON_CLOSEOUT_READY":
        raise CloseInventoryError("POSTSEASON_CLOSEOUT_SCHEMA_INVALID")
    document["package_sha256"] = claimed
    return document
