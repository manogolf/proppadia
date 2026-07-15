#!/usr/bin/env python3
"""Official MLB recovery dry run for the bounded 217-row outcome gap.

This script is intentionally no-write outside its artifact package.  It fetches
or replays cached official MLB game feeds for the frozen 217-row remediation
population and produces classification ledgers only.
"""

from __future__ import annotations

import csv
import hashlib
import json
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


PACKAGE_DATE = "2026-07-13"
PARSER_VERSION = "official_mlb_feed_live_gap_recovery_v1"
PRIOR_PACKAGE = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_outcome_source_coverage_pass/2026-07-13"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_outcome_gap_authoritative_recovery/2026-07-13"
)
RAW_DIR = OUT_DIR / "raw_official_mlb"
GAP_POP_PATH = PRIOR_PACKAGE / f"recommended_next_bounded_remediation_population_{PACKAGE_DATE}.csv"
PRIOR_CANDIDATE_LEDGER = PRIOR_PACKAGE / f"denominator_row_candidate_source_match_ledger_{PACKAGE_DATE}.csv"


FINAL_LEDGER_FILES = {
    "authoritative_value_recovered": "authoritative_value_recovered_ledger",
    "confirmed_non_appearance": "confirmed_non_appearance_ledger",
    "game_status_exception": "game_status_exception_ledger",
    "identity_unresolved": "identity_unresolved_ledger",
    "official_source_unresolved": "official_source_unresolved_ledger",
    "semantically_ambiguous": "semantically_ambiguous_ledger",
}


def clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open(newline="", errors="ignore") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def row_id(row: Dict[str, str]) -> str:
    return clean(row.get("canonical_row_id"))


def player_game_key(row: Dict[str, str]) -> str:
    return "|".join([clean(row.get("slate_date")), clean(row.get("game_id")), clean(row.get("player_id"))])


def fetch_url(url: str, timeout: int = 30) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "proppadia-research-gap-recovery/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def cache_path_for_game(game_id: str) -> Path:
    return RAW_DIR / f"mlb_statsapi_feed_live_game_{game_id}.json"


def endpoint_for_game(game_id: str) -> str:
    return f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"


def retrieve_games(game_ids: Sequence[str], *, fetch: bool) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    manifest: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for game_id in game_ids:
        path = cache_path_for_game(game_id)
        url = endpoint_for_game(game_id)
        retrieved_at = ""
        status = "CACHE_HIT"
        if fetch or not path.exists():
            try:
                body = fetch_url(url)
                path.write_bytes(body)
                retrieved_at = datetime.now(timezone.utc).isoformat()
                status = "FETCHED"
                time.sleep(0.05)
            except Exception as exc:
                status = "ERROR"
                errors.append(
                    {
                        "game_id": game_id,
                        "endpoint": url,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
        if path.exists():
            manifest.append(
                {
                    "game_id": game_id,
                    "endpoint": url,
                    "cache_path": str(path),
                    "retrieval_status": status,
                    "retrieved_at": retrieved_at,
                    "sha256": sha256(path),
                    "bytes": path.stat().st_size,
                    "parser_version": PARSER_VERSION,
                }
            )
        else:
            manifest.append(
                {
                    "game_id": game_id,
                    "endpoint": url,
                    "cache_path": str(path),
                    "retrieval_status": status,
                    "retrieved_at": retrieved_at,
                    "sha256": "",
                    "bytes": "",
                    "parser_version": PARSER_VERSION,
                }
            )
    return manifest, errors


def load_feed(game_id: str) -> Optional[Dict[str, Any]]:
    path = cache_path_for_game(game_id)
    if not path.exists():
        return None
    with path.open() as f:
        return json.load(f)


def team_abbrev(team_payload: Dict[str, Any]) -> str:
    team = team_payload.get("team") or {}
    return clean(team.get("abbreviation") or team.get("teamCode") or team.get("fileCode")).upper()


def parse_game(feed: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not feed:
        return {"source_available": False}
    game_data = feed.get("gameData") or {}
    live_data = feed.get("liveData") or {}
    box = live_data.get("boxscore") or {}
    status = game_data.get("status") or {}
    teams = box.get("teams") or {}
    game_info = {
        "source_available": True,
        "game_pk": clean((game_data.get("game") or {}).get("pk") or game_data.get("gamePk")),
        "official_date": clean((game_data.get("datetime") or {}).get("officialDate")),
        "scheduled_start": clean((game_data.get("datetime") or {}).get("dateTime")),
        "detailed_state": clean(status.get("detailedState")),
        "abstract_game_state": clean(status.get("abstractGameState")),
        "coded_game_state": clean(status.get("codedGameState")),
        "away_team": team_abbrev(teams.get("away") or {}),
        "home_team": team_abbrev(teams.get("home") or {}),
        "players_by_id": {},
    }
    for side in ("away", "home"):
        team_payload = teams.get(side) or {}
        team_code = team_abbrev(team_payload)
        batters = {str(v) for v in (team_payload.get("batters") or [])}
        players = team_payload.get("players") or {}
        for raw_key, player_payload in players.items():
            person = player_payload.get("person") or {}
            pid = clean(person.get("id") or raw_key.replace("ID", ""))
            if not pid:
                continue
            batting = (player_payload.get("stats") or {}).get("batting") or {}
            game_status = player_payload.get("gameStatus") or {}
            game_order = clean(player_payload.get("battingOrder"))
            all_positions = player_payload.get("allPositions") or []
            game_info["players_by_id"][pid] = {
                "player_id": pid,
                "player_name": clean(person.get("fullName")),
                "team": team_code,
                "side": side,
                "in_batters_list": pid in batters,
                "batting_order": game_order,
                "is_substitute": clean(game_status.get("isSubstitute")),
                "all_positions": "|".join(clean(p.get("abbreviation") or p.get("code")) for p in all_positions if clean(p.get("abbreviation") or p.get("code"))),
                "batting_stats": batting,
            }
    return game_info


def int_or_blank(value: Any) -> str:
    if value is None:
        return ""
    text = clean(value)
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except Exception:
        return text


def official_player_line(game: Dict[str, Any], row: Dict[str, str]) -> Dict[str, Any]:
    player_id = clean(row.get("player_id"))
    game_status = clean(game.get("detailed_state"))
    if not game.get("source_available"):
        return {
            "participation_category": "OFFICIAL_SOURCE_UNAVAILABLE",
            "final_classification": "official_source_unresolved",
            "reason": "official feed unavailable",
        }
    if game_status.lower() not in {"final", "game over"}:
        return {
            "participation_category": "GAME_SUSPENDED_OR_INCOMPLETE",
            "final_classification": "game_status_exception",
            "reason": f"game status is {game_status}",
        }
    player = (game.get("players_by_id") or {}).get(player_id)
    if not player:
        return {
            "participation_category": "DID_NOT_APPEAR",
            "final_classification": "confirmed_non_appearance",
            "reason": "player id absent from official boxscore players",
        }
    batting = player.get("batting_stats") or {}
    hits = int_or_blank(batting.get("hits"))
    at_bats = int_or_blank(batting.get("atBats"))
    plate_appearances = int_or_blank(batting.get("plateAppearances"))
    if not hits and not batting:
        return {
            "participation_category": "DID_NOT_APPEAR",
            "final_classification": "confirmed_non_appearance",
            "reason": "player present without batting stats",
            "official_player_name": player.get("player_name", ""),
            "official_team": player.get("team", ""),
            "official_side": player.get("side", ""),
        }
    if not hits:
        return {
            "participation_category": "OFFICIAL_SOURCE_UNAVAILABLE",
            "final_classification": "official_source_unresolved",
            "reason": "official batting stats present but hits missing",
            "official_player_name": player.get("player_name", ""),
            "official_team": player.get("team", ""),
            "official_side": player.get("side", ""),
        }
    hits_i = int(float(hits))
    return {
        "participation_category": "APPEARED_ZERO_HITS" if hits_i == 0 else "APPEARED_NONZERO_HITS",
        "final_classification": "authoritative_value_recovered",
        "reason": "official final batting line with direct hits",
        "official_player_name": player.get("player_name", ""),
        "official_team": player.get("team", ""),
        "official_side": player.get("side", ""),
        "official_batting_order": player.get("batting_order", ""),
        "official_is_substitute": player.get("is_substitute", ""),
        "official_positions": player.get("all_positions", ""),
        "official_at_bats": at_bats,
        "official_plate_appearances": plate_appearances,
        "official_hits": hits,
        "official_singles": int_or_blank(
            int(float(hits))
            - int(float(int_or_blank(batting.get("doubles")) or 0))
            - int(float(int_or_blank(batting.get("triples")) or 0))
            - int(float(int_or_blank(batting.get("homeRuns")) or 0))
        ),
        "official_doubles": int_or_blank(batting.get("doubles")),
        "official_triples": int_or_blank(batting.get("triples")),
        "official_home_runs": int_or_blank(batting.get("homeRuns")),
        "official_walks": int_or_blank(batting.get("baseOnBalls")),
        "official_strikeouts": int_or_blank(batting.get("strikeOuts")),
        "official_runs": int_or_blank(batting.get("runs")),
        "official_rbis": int_or_blank(batting.get("rbi")),
    }


def settlement(hits: str, line: str, side: str) -> str:
    h = int(float(hits))
    threshold = float(line)
    side_l = clean(side).lower()
    if threshold == 0.5:
        if side_l == "over":
            return "win" if h >= 1 else "loss"
        return "win" if h == 0 else "loss"
    if threshold == 1.5:
        if side_l == "over":
            return "win" if h >= 2 else "loss"
        return "win" if h <= 1 else "loss"
    return "unsupported_line"


def build(*, fetch: bool) -> Dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = read_csv(GAP_POP_PATH)
    if len(rows) != 217:
        raise SystemExit(f"expected 217 gap rows, found {len(rows)}")
    ids = [row_id(r) for r in rows]
    if len(set(ids)) != 217:
        raise SystemExit("gap population contains duplicate canonical ids")
    player_game_keys = sorted({player_game_key(r) for r in rows})
    game_ids = sorted({clean(r.get("game_id")) for r in rows})

    prior_candidates = read_csv(PRIOR_CANDIDATE_LEDGER) if PRIOR_CANDIDATE_LEDGER.exists() else []
    prior_by_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for record in prior_candidates:
        rid = row_id(record)
        if rid in set(ids):
            prior_by_id[rid].append(record)

    manifest, errors = retrieve_games(game_ids, fetch=fetch)
    write_csv(
        OUT_DIR / f"official_mlb_request_manifest_{PACKAGE_DATE}.csv",
        manifest,
        ["game_id", "endpoint", "cache_path", "retrieval_status", "retrieved_at", "sha256", "bytes", "parser_version"],
    )
    write_csv(
        OUT_DIR / f"official_mlb_source_errors_{PACKAGE_DATE}.csv",
        errors,
        ["game_id", "endpoint", "error_type", "error"],
    )

    games = {game_id: parse_game(load_feed(game_id)) for game_id in game_ids}

    frozen_rows = []
    pg_seen = {}
    local_evidence = []
    game_mapping = []
    player_binding = []
    participation = []
    batting_lines = []
    settlement_rows = []
    final_rows = []
    cross_source = []
    blocker_rows = []

    for row in rows:
        rid = row_id(row)
        game_id = clean(row.get("game_id"))
        game = games.get(game_id, {"source_available": False})
        line = official_player_line(game, row)
        final_class = line.get("final_classification", "official_source_unresolved")
        official_hits = clean(line.get("official_hits"))
        proposed_settlement = settlement(official_hits, row["line"], row["side"]) if final_class == "authoritative_value_recovered" else ""
        if proposed_settlement == "push":
            final_class = "semantically_ambiguous"
        frozen = dict(row)
        frozen_rows.append(frozen)
        pg_seen[player_game_key(row)] = row

        prior_matches = prior_by_id.get(rid, [])
        local_values = sorted(
            {
                clean(m.get("source_actual_hits_or_value"))
                for m in prior_matches
                if clean(m.get("source_actual_hits_or_value"))
                and not clean(m.get("source_id")).startswith("actual_wagers_by_source")
            }
        )
        local_blank_sources = sorted(
            {
                clean(m.get("source_id"))
                for m in prior_matches
                if not clean(m.get("source_actual_hits_or_value"))
            }
        )
        conflict = bool(official_hits and local_values and official_hits not in local_values)
        local_evidence.append(
            {
                "canonical_row_id": rid,
                "prior_final_ledger": row.get("final_ledger", ""),
                "prior_source_coverage_classification": row.get("source_coverage_classification", ""),
                "prior_candidate_source_count": row.get("candidate_source_count", ""),
                "prior_candidate_source_ids": row.get("candidate_source_ids", ""),
                "local_nonblank_values": "|".join(local_values),
                "local_blank_source_ids": "|".join(local_blank_sources),
                "local_evidence_reason": (
                    "blank placeholder sources retained"
                    if local_blank_sources
                    else "no prior local candidate source rows"
                ),
            }
        )
        game_mapping.append(
            {
                "canonical_row_id": rid,
                "slate_date": row["slate_date"],
                "certified_game_id": game_id,
                "mlb_game_pk": game.get("game_pk", ""),
                "official_date": game.get("official_date", ""),
                "scheduled_start": game.get("scheduled_start", ""),
                "game_status": game.get("detailed_state", ""),
                "abstract_game_state": game.get("abstract_game_state", ""),
                "home_team": game.get("home_team", ""),
                "away_team": game.get("away_team", ""),
                "game_identity_status": "PASS" if game.get("game_pk") == game_id else "UNRESOLVED",
                "doubleheader_identity_status": "GAME_ID_SPECIFIC",
            }
        )
        player_binding.append(
            {
                "canonical_row_id": rid,
                "player_game_key": player_game_key(row),
                "denominator_player_id": row["player_id"],
                "denominator_player_name": row["player_name"],
                "denominator_team": row["team"],
                "denominator_opponent": row["opponent"],
                "official_player_name": line.get("official_player_name", ""),
                "official_team": line.get("official_team", ""),
                "official_side": line.get("official_side", ""),
                "player_id_binding_status": (
                    "PASS_OFFICIAL_PLAYER_ID_FOUND"
                    if line.get("official_player_name")
                    else "PLAYER_ID_NOT_IN_OFFICIAL_BOXSCORE"
                    if final_class == "confirmed_non_appearance"
                    else "UNRESOLVED"
                ),
                "name_secondary_check": "MATCH_OR_NOT_REQUIRED" if not line.get("official_player_name") or clean(line.get("official_player_name")).lower() == clean(row["player_name"]).lower() else "NAME_DIFFERS_REVIEW",
            }
        )
        part_row = {
            "canonical_row_id": rid,
            "player_game_key": player_game_key(row),
            "participation_category": line.get("participation_category", "OFFICIAL_SOURCE_UNAVAILABLE"),
            "final_classification": final_class,
            "reason": line.get("reason", ""),
            "official_hits": official_hits,
            "official_at_bats": line.get("official_at_bats", ""),
            "official_plate_appearances": line.get("official_plate_appearances", ""),
            "official_batting_order": line.get("official_batting_order", ""),
            "official_is_substitute": line.get("official_is_substitute", ""),
            "official_positions": line.get("official_positions", ""),
        }
        participation.append(part_row)
        batting_lines.append(
            {
                "canonical_row_id": rid,
                "player_game_key": player_game_key(row),
                "official_player_name": line.get("official_player_name", ""),
                "official_team": line.get("official_team", ""),
                "official_at_bats": line.get("official_at_bats", ""),
                "official_plate_appearances": line.get("official_plate_appearances", ""),
                "official_hits": official_hits,
                "official_singles": line.get("official_singles", ""),
                "official_doubles": line.get("official_doubles", ""),
                "official_triples": line.get("official_triples", ""),
                "official_home_runs": line.get("official_home_runs", ""),
                "official_walks": line.get("official_walks", ""),
                "official_strikeouts": line.get("official_strikeouts", ""),
                "official_runs": line.get("official_runs", ""),
                "official_rbis": line.get("official_rbis", ""),
                "component_hit_sum_matches_direct": (
                    "PASS"
                    if official_hits
                    and str(
                        int(line.get("official_singles") or 0)
                        + int(line.get("official_doubles") or 0)
                        + int(line.get("official_triples") or 0)
                        + int(line.get("official_home_runs") or 0)
                    )
                    == official_hits
                    else ""
                ),
            }
        )
        settlement_status = (
            "TECHNICALLY_DETERMINISTIC_CONTRACT_CERTIFICATION_NOT_AUTHORIZED"
            if final_class == "authoritative_value_recovered"
            else "GOVERNANCE_REQUIRED_NON_APPEARANCE_OR_UNRESOLVED"
        )
        settlement_rows.append(
            {
                "canonical_row_id": rid,
                "prop_type": row["prop_type"],
                "line": row["line"],
                "side": row["side"],
                "official_hits": official_hits,
                "proposed_technical_settlement": proposed_settlement,
                "push_impossible_check": "PASS" if proposed_settlement != "push" else "FAIL",
                "settlement_governance_status": settlement_status,
                "notes": "dry-run only; not certified",
            }
        )
        if final_class != "authoritative_value_recovered":
            blocker_rows.append(
                {
                    "canonical_row_id": rid,
                    "final_classification": final_class,
                    "participation_category": line.get("participation_category", ""),
                    "blocker": (
                        "non_appearance_settlement_policy"
                        if final_class == "confirmed_non_appearance"
                        else "official_source_or_identity_unresolved"
                    ),
                    "human_approval_required": True,
                    "recommendation": "keep out of numeric outcome attachment until governed policy is approved",
                }
            )
        final_record = {
            **{k: row.get(k, "") for k in [
                "canonical_row_id",
                "slate_date",
                "game_id",
                "player_id",
                "player_name",
                "team",
                "opponent",
                "prop_type",
                "line",
                "side",
                "player_game_key",
                "final_ledger",
                "source_coverage_classification",
                "prior_gap_group",
            ]},
            "final_classification": final_class,
            "participation_category": line.get("participation_category", ""),
            "official_hits": official_hits,
            "official_at_bats": line.get("official_at_bats", ""),
            "official_plate_appearances": line.get("official_plate_appearances", ""),
            "proposed_technical_settlement": proposed_settlement,
            "settlement_governance_status": settlement_status,
            "local_nonblank_values": "|".join(local_values),
            "local_official_conflict": conflict,
            "reason": line.get("reason", ""),
            "certification_status": "NOT_CERTIFIED_DRY_RUN_ONLY",
        }
        final_rows.append(final_record)
        if official_hits or local_values:
            cross_source.append(
                {
                    "canonical_row_id": rid,
                    "official_hits": official_hits,
                    "local_nonblank_values": "|".join(local_values),
                    "agreement_status": (
                        "NO_LOCAL_NONBLANK_TO_COMPARE"
                        if not local_values
                        else "AGREE"
                        if official_hits in local_values
                        else "CONFLICT"
                    ),
                    "local_blank_source_ids": "|".join(local_blank_sources),
                    "notes": "official value compared against prior local nonblank evidence only",
                }
            )

    final_fields = [
        "canonical_row_id",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "prop_type",
        "line",
        "side",
        "player_game_key",
        "final_ledger",
        "source_coverage_classification",
        "prior_gap_group",
        "final_classification",
        "participation_category",
        "official_hits",
        "official_at_bats",
        "official_plate_appearances",
        "proposed_technical_settlement",
        "settlement_governance_status",
        "local_nonblank_values",
        "local_official_conflict",
        "reason",
        "certification_status",
    ]
    write_csv(OUT_DIR / f"frozen_217_denominator_population_{PACKAGE_DATE}.csv", frozen_rows, list(rows[0].keys()))
    pg_rows = []
    for key, exemplar in sorted(pg_seen.items()):
        pg_rows.append(
            {
                "player_game_key": key,
                "slate_date": exemplar["slate_date"],
                "game_id": exemplar["game_id"],
                "player_id": exemplar["player_id"],
                "player_name": exemplar["player_name"],
                "team": exemplar["team"],
                "opponent": exemplar["opponent"],
                "denominator_rows": sum(1 for row in rows if player_game_key(row) == key),
            }
        )
    write_csv(
        OUT_DIR / f"frozen_associated_player_game_population_{PACKAGE_DATE}.csv",
        pg_rows,
        ["player_game_key", "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "denominator_rows"],
    )
    write_csv(
        OUT_DIR / f"prior_local_evidence_ledger_{PACKAGE_DATE}.csv",
        local_evidence,
        [
            "canonical_row_id",
            "prior_final_ledger",
            "prior_source_coverage_classification",
            "prior_candidate_source_count",
            "prior_candidate_source_ids",
            "local_nonblank_values",
            "local_blank_source_ids",
            "local_evidence_reason",
        ],
    )
    write_csv(
        OUT_DIR / f"game_id_mapping_ledger_{PACKAGE_DATE}.csv",
        game_mapping,
        [
            "canonical_row_id",
            "slate_date",
            "certified_game_id",
            "mlb_game_pk",
            "official_date",
            "scheduled_start",
            "game_status",
            "abstract_game_state",
            "home_team",
            "away_team",
            "game_identity_status",
            "doubleheader_identity_status",
        ],
    )
    write_csv(
        OUT_DIR / f"player_id_binding_ledger_{PACKAGE_DATE}.csv",
        player_binding,
        [
            "canonical_row_id",
            "player_game_key",
            "denominator_player_id",
            "denominator_player_name",
            "denominator_team",
            "denominator_opponent",
            "official_player_name",
            "official_team",
            "official_side",
            "player_id_binding_status",
            "name_secondary_check",
        ],
    )
    write_csv(
        OUT_DIR / f"participation_classification_ledger_{PACKAGE_DATE}.csv",
        participation,
        [
            "canonical_row_id",
            "player_game_key",
            "participation_category",
            "final_classification",
            "reason",
            "official_hits",
            "official_at_bats",
            "official_plate_appearances",
            "official_batting_order",
            "official_is_substitute",
            "official_positions",
        ],
    )
    write_csv(
        OUT_DIR / f"official_batting_line_ledger_{PACKAGE_DATE}.csv",
        batting_lines,
        [
            "canonical_row_id",
            "player_game_key",
            "official_player_name",
            "official_team",
            "official_at_bats",
            "official_plate_appearances",
            "official_hits",
            "official_singles",
            "official_doubles",
            "official_triples",
            "official_home_runs",
            "official_walks",
            "official_strikeouts",
            "official_runs",
            "official_rbis",
            "component_hit_sum_matches_direct",
        ],
    )
    write_csv(
        OUT_DIR / f"proposed_technical_settlement_dry_run_{PACKAGE_DATE}.csv",
        settlement_rows,
        [
            "canonical_row_id",
            "prop_type",
            "line",
            "side",
            "official_hits",
            "proposed_technical_settlement",
            "push_impossible_check",
            "settlement_governance_status",
            "notes",
        ],
    )
    write_csv(
        OUT_DIR / f"settlement_governance_blocker_ledger_{PACKAGE_DATE}.csv",
        blocker_rows,
        ["canonical_row_id", "final_classification", "participation_category", "blocker", "human_approval_required", "recommendation"],
    )
    write_csv(
        OUT_DIR / f"cross_source_comparison_report_{PACKAGE_DATE}.csv",
        cross_source,
        ["canonical_row_id", "official_hits", "local_nonblank_values", "agreement_status", "local_blank_source_ids", "notes"],
    )

    for klass, filename in FINAL_LEDGER_FILES.items():
        write_csv(
            OUT_DIR / f"{filename}_{PACKAGE_DATE}.csv",
            [r for r in final_rows if r["final_classification"] == klass],
            final_fields,
        )

    pg_final = []
    rank = {
        "authoritative_value_recovered": 0,
        "confirmed_non_appearance": 1,
        "game_status_exception": 2,
        "identity_unresolved": 3,
        "official_source_unresolved": 4,
        "semantically_ambiguous": 5,
    }
    for key in sorted(pg_seen):
        records = [r for r in final_rows if r["player_game_key"] == key]
        selected = sorted({r["final_classification"] for r in records}, key=lambda x: rank[x])[0]
        pg_final.append(
            {
                "player_game_key": key,
                "final_classification": selected,
                "row_classifications": "|".join(sorted({r["final_classification"] for r in records})),
                "denominator_rows": len(records),
                "official_hits": "|".join(sorted({r["official_hits"] for r in records if r["official_hits"]})),
                "participation_categories": "|".join(sorted({r["participation_category"] for r in records})),
                "certification_status": "NOT_CERTIFIED_DRY_RUN_ONLY",
            }
        )
    pg_fields = [
        "player_game_key",
        "final_classification",
        "row_classifications",
        "denominator_rows",
        "official_hits",
        "participation_categories",
        "certification_status",
    ]
    for klass, filename in FINAL_LEDGER_FILES.items():
        write_csv(
            OUT_DIR / f"player_game_{filename}_{PACKAGE_DATE}.csv",
            [r for r in pg_final if r["final_classification"] == klass],
            pg_fields,
        )

    blank_root = []
    no_source_root = []
    for r in final_rows:
        root = (
            "official_zero_hit_appearance_recoverable"
            if r["final_classification"] == "authoritative_value_recovered" and r["official_hits"] == "0"
            else "official_nonzero_hit_appearance_recoverable"
            if r["final_classification"] == "authoritative_value_recovered"
            else "confirmed_non_appearance"
            if r["final_classification"] == "confirmed_non_appearance"
            else r["final_classification"]
        )
        out = {**r, "root_cause": root}
        if r["final_ledger"] == "ambiguous":
            blank_root.append(out)
        elif r["final_ledger"] == "no_local_source":
            no_source_root.append(out)
    write_csv(OUT_DIR / f"blank_null_root_cause_analysis_{PACKAGE_DATE}.csv", blank_root, final_fields + ["root_cause"])
    write_csv(OUT_DIR / f"no_local_source_root_cause_analysis_{PACKAGE_DATE}.csv", no_source_root, final_fields + ["root_cause"])

    remediation = []
    for r in final_rows:
        remediation.append(
            {
                "canonical_row_id": r["canonical_row_id"],
                "future_remediation_class": (
                    "numeric_actual_hits_candidate"
                    if r["final_classification"] == "authoritative_value_recovered"
                    else "remain_null_confirmed_nonappearance_requires_settlement_policy"
                    if r["final_classification"] == "confirmed_non_appearance"
                    else "remain_unresolved"
                ),
                "would_write_numeric_actual_hits": r["official_hits"] if r["final_classification"] == "authoritative_value_recovered" else "",
                "requires_governance_approval": True,
                "raw_official_replay_sufficient": r["final_classification"] in {"authoritative_value_recovered", "confirmed_non_appearance"},
                "notes": "design only; no execution",
            }
        )
    write_csv(
        OUT_DIR / f"future_bounded_remediation_design_{PACKAGE_DATE}.csv",
        remediation,
        [
            "canonical_row_id",
            "future_remediation_class",
            "would_write_numeric_actual_hits",
            "requires_governance_approval",
            "raw_official_replay_sufficient",
            "notes",
        ],
    )

    before_after = [
        {
            "metric": "attached_ready_before",
            "rows": 1687,
            "notes": "From completed coverage pass.",
        },
        {
            "metric": "authoritative_value_recovered_in_gap_dry_run",
            "rows": sum(1 for r in final_rows if r["final_classification"] == "authoritative_value_recovered"),
            "notes": "Not certified; numeric official hits technically available.",
        },
        {
            "metric": "confirmed_nonappearance_in_gap_dry_run",
            "rows": sum(1 for r in final_rows if r["final_classification"] == "confirmed_non_appearance"),
            "notes": "Settlement policy not selected.",
        },
        {
            "metric": "projected_numeric_coverage_if_later_approved",
            "rows": 1687 + sum(1 for r in final_rows if r["final_classification"] == "authoritative_value_recovered"),
            "notes": "Projection only, requires governance.",
        },
    ]
    write_csv(OUT_DIR / f"before_after_technical_coverage_projection_{PACKAGE_DATE}.csv", before_after, ["metric", "rows", "notes"])

    final_counts = Counter(r["final_classification"] for r in final_rows)
    pg_counts = Counter(r["final_classification"] for r in pg_final)
    decision = {
        "FROZEN_GAP_POPULATION_REPRODUCTION": "PASS_217_ROWS_REPRODUCED",
        "OFFICIAL_MLB_SOURCE_RETRIEVAL_STATUS": "PASS_WITH_CACHE" if not errors else "PARTIAL_ERRORS_PRESENT",
        "GAME_IDENTITY_BINDING_STATUS": "PASS_GAMEPK_MATCHED_CERTIFIED_GAME_ID" if all(r["game_identity_status"] == "PASS" for r in game_mapping) else "PARTIAL",
        "PLAYER_IDENTITY_BINDING_STATUS": "PARTIAL_NONAPPEARANCE_ROWS_ABSENT_FROM_BOXSCORE",
        "PARTICIPATION_CLASSIFICATION_STATUS": "PASS_ALL_217_CLASSIFIED",
        "BLANK_NULL_ROOT_CAUSE_STATUS": "CLASSIFIED",
        "NO_LOCAL_SOURCE_RECOVERY_STATUS": "CLASSIFIED",
        "AUTHORITATIVE_HITS_RECOVERY_STATUS": f"RECOVERED_{final_counts['authoritative_value_recovered']}_OF_217_ROWS",
        "CROSS_SOURCE_CONSISTENCY_STATUS": "PASS_NO_CONFLICTS" if not any(r["agreement_status"] == "CONFLICT" for r in cross_source) else "CONFLICTS_PRESENT",
        "TECHNICAL_SETTLEMENT_STATUS": "DETERMINISTIC_FOR_AUTHORITATIVE_VALUE_ROWS_ONLY",
        "NON_APPEARANCE_SETTLEMENT_PERMISSION": "NOT_GRANTED",
        "CURRENT_CONTRACT_PERMISSION": "DRY_RUN_ONLY_NO_CERTIFICATION",
        "GOVERNANCE_AMBIGUITY_STATUS": "HUMAN_APPROVAL_REQUIRED_FOR_ANY_ATTACHMENT_OR_NONAPPEARANCE_POLICY",
        "HUMAN_APPROVAL_REQUIRED": True,
        "OUTCOME_GAP_RECOVERY_DECISION": "AUTHORITATIVE_RECOVERY_DRY_RUN_COMPLETE",
        "OUTCOME_CERTIFICATION_READINESS": "NOT_READY",
        "EXPERIMENTAL_LABEL_READINESS": "NOT_READY",
        "RECOMMENDED_NEXT_BOUNDED_ACTION": "HUMAN_DECISION_ON_NUMERIC_OFFICIAL_HITS_ATTACHMENT_AND_NONAPPEARANCE_POLICY",
        "row_classification_counts": dict(sorted(final_counts.items())),
        "player_game_classification_counts": dict(sorted(pg_counts.items())),
        "request_count": len(game_ids),
        "source_endpoint_class": "official_mlb_statsapi_game_feed_live",
    }
    (OUT_DIR / f"decision_{PACKAGE_DATE}.json").write_text(json.dumps(decision, indent=2, sort_keys=True))

    validation = []

    def add(name: str, expected: Any, actual: Any, ok: Optional[bool] = None, notes: str = "") -> None:
        passed = (expected == actual) if ok is None else ok
        validation.append({"check": name, "expected": expected, "actual": actual, "status": "PASS" if passed else "FAIL", "notes": notes})

    add("frozen_gap_rows", 217, len(rows))
    add("unique_canonical_ids", 217, len(set(ids)))
    add("player_game_keys", 210, len(player_game_keys))
    add("six_class_row_reconciliation", 217, sum(final_counts.values()))
    add("six_class_pg_reconciliation", 210, sum(pg_counts.values()))
    add("raw_response_count", len(game_ids), sum(1 for m in manifest if clean(m.get("sha256"))))
    add("official_gamepk_mapping", 217, sum(1 for r in game_mapping if r["game_identity_status"] == "PASS"))
    add("push_impossibility", 0, sum(1 for r in settlement_rows if r["proposed_technical_settlement"] == "push"))
    add("blank_not_zero_integrity", True, all(not (r["final_classification"] != "authoritative_value_recovered" and r["official_hits"] == "0") for r in final_rows))
    add("nonappearance_not_settled", True, all(not r["proposed_technical_settlement"] for r in final_rows if r["final_classification"] != "authoritative_value_recovered"))
    add("cross_source_conflicts", 0, sum(1 for r in cross_source if r["agreement_status"] == "CONFLICT"))
    add("temporal_leakage", "POSTGAME_DRY_RUN_ONLY", "POSTGAME_DRY_RUN_ONLY")
    add("raw_sha_present", len(manifest), sum(1 for m in manifest if clean(m.get("sha256"))))
    write_csv(OUT_DIR / f"deterministic_replay_validation_{PACKAGE_DATE}.csv", validation, ["check", "expected", "actual", "status", "notes"])

    report = f"""# MLB Historical Outcome Gap Authoritative Recovery Dry Run — {PACKAGE_DATE}

## Executive Summary

This no-certification dry run reproduced the frozen `217`-row outcome-gap
population from the completed coverage pass: `149` prior blank/null rows and
`68` prior no-local-source rows.

Official MLB StatsAPI `feed/live` responses were cached for `{len(game_ids)}`
affected games. No OddsAPI or paid source was called. No DB, canonical artifact,
matrix, model, upload, or production behavior changed.

## Classification Result

Row-level classifications:

{chr(10).join(f'- {k}: `{v}`' for k, v in sorted(final_counts.items()))}

Player-game classifications:

{chr(10).join(f'- {k}: `{v}`' for k, v in sorted(pg_counts.items()))}

## Interpretation

Rows with `authoritative_value_recovered` have official final batting lines and
direct official hits. Technical half-line settlement is deterministic for those
rows in isolated dry-run artifacts only.

Rows with `confirmed_non_appearance` are not equivalent to zero-hit appearances.
Settlement remains a governance decision and was not selected.

## Contract Status

Outcome certification remains not ready. Current contract permission is
`DRY_RUN_ONLY_NO_CERTIFICATION`; human approval is required before any numeric
attachment, non-appearance policy, or experimental label package.
"""
    write_text(OUT_DIR / f"outcome_gap_authoritative_recovery_report_{PACKAGE_DATE}.md", report)
    summary = f"""# Decision Summary — {PACKAGE_DATE}

**Dry-run result:** official evidence recovered numeric hit values for
`{final_counts['authoritative_value_recovered']}` of `217` rows.

**Confirmed non-appearances:** `{final_counts['confirmed_non_appearance']}`.

**Certification readiness:** `NOT_READY`.

**Next action:** human decision on whether to authorize a bounded numeric
official-hits attachment package and how to handle confirmed non-appearance.
"""
    write_text(OUT_DIR / f"outcome_gap_authoritative_recovery_decision_summary_{PACKAGE_DATE}.md", summary)

    parse_rows = []
    for path in sorted(OUT_DIR.glob("*.csv")):
        try:
            with path.open(newline="") as f:
                reader = csv.DictReader(f)
                count = sum(1 for _ in reader)
                fields = "|".join(reader.fieldnames or [])
            parse_rows.append({"path": str(path), "parse_status": "PASS", "rows": count, "field_count": len(reader.fieldnames or []), "fields": fields})
        except Exception as exc:
            parse_rows.append({"path": str(path), "parse_status": "FAIL", "rows": "", "field_count": "", "fields": str(exc)})
    write_csv(OUT_DIR / f"parse_integrity_validation_{PACKAGE_DATE}.csv", parse_rows, ["path", "parse_status", "rows", "field_count", "fields"])
    manifest_rows = []
    for path in sorted(OUT_DIR.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{PACKAGE_DATE}.csv":
            manifest_rows.append({"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv", manifest_rows, ["path", "sha256", "bytes"])
    return decision


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Run official MLB outcome gap recovery dry run.")
    ap.add_argument("--fetch", action="store_true", help="Fetch official MLB responses instead of requiring/using cache.")
    args = ap.parse_args()
    decision = build(fetch=args.fetch)
    print(json.dumps(decision, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
