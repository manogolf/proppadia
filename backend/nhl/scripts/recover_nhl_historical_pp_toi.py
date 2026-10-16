#!/usr/bin/env python3
"""Resumable, read-only official-source reconstruction for 2023/24 PP TOI.

The script reuses the two retained shift-overlap implementations. It only
captures official NHL responses and writes local evidence/correction drafts;
it never connects to or mutates a database.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

ROOT = Path(__file__).resolve().parents[3]
PBP_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
SHIFT_URL = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId%3D{game_id}"
DEFAULT_CURRENT = ROOT / "artifacts/analysis/nhl/raw_foundation_integrity_2026-10-16/current_raw_snapshot.csv"
DEFAULT_CHECKPOINT = ROOT / "backups/nhl_schema_and_data.sql"
DEFAULT_REUSE = ROOT / "artifacts/analysis/nhl/pp_toi_source_integrity_2026-10-16/official_sources"

# Historical player/game logs use the application's stable team namespace,
# established by fetch_nhl_to_csv.py. These IDs are keyed by abbreviation and
# intentionally differ from NHL Stats API IDs for many clubs. Never compare a
# raw team_id directly with an official NHL boxscore ID.
LEGACY_TEAM_ID_TO_ABBR = {
    1: "ANA", 2: "ARI", 3: "BOS", 4: "BUF", 5: "CAR", 6: "CBJ", 7: "CGY", 8: "CHI",
    9: "COL", 10: "DAL", 11: "DET", 12: "EDM", 13: "FLA", 14: "LAK", 15: "MIN",
    16: "MTL", 17: "NJD", 18: "NSH", 19: "NYI", 20: "NYR", 21: "OTT", 22: "PHI",
    23: "PIT", 24: "SEA", 25: "SJS", 26: "STL", 27: "TBL", 28: "TOR", 29: "VAN",
    30: "VGK", 31: "WPG", 32: "WSH",
}


def load_legacy_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"LEGACY_RECONSTRUCTION_IMPORT_FAILED:{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


LEGACY_A = load_legacy_module(
    "pp_toi_reconstruction_a",
    ROOT / "backend/_legacy/nhl/scripts/archive/rebuild_pp_toi_from_shiftcharts_and_pbp.py",
)
LEGACY_B = load_legacy_module(
    "pp_toi_reconstruction_b",
    ROOT / "backend/_legacy/nhl/scripts/archive/20260222_runtime_cleanup/approx_pp_toi_from_shiftcharts.py",
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_number(value: Any) -> float | None:
    if value is None or value == "" or value == r"\N":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_pg_copy(path: Path, relation: str):
    raw = path.read_text(encoding="utf-8", errors="strict")
    marker = f"COPY nhl.{relation} ("
    start = raw.index(marker)
    start = raw.index("\n", start) + 1
    end = raw.index("\n\\.\n", start)
    for line in raw[start:end].splitlines():
        yield next(csv.reader([line], delimiter="\t"))


def load_inventory(current_path: Path, checkpoint_path: Path):
    with current_path.open(newline="", encoding="utf-8") as f:
        current_rows = list(csv.DictReader(f))
    target_rows = [r for r in current_rows if r["game_id"].startswith(("202302", "202402"))]
    raw_by_key = {(int(r["game_id"]), int(r["player_id"])): r for r in target_rows}
    games = {}
    for row in read_pg_copy(checkpoint_path, "games"):
        if row[0].startswith(("202302", "202402")):
            games[int(row[0])] = {
                "game_date": row[1], "home_team_id": int(row[3]), "away_team_id": int(row[4]),
            }
    teams = {}
    for row in read_pg_copy(checkpoint_path, "teams"):
        teams[int(row[0])] = {"name": row[1], "abbr": row[2]}
    checkpoint = {}
    for row in read_pg_copy(checkpoint_path, "skater_game_logs_raw"):
        if row[1].startswith(("202302", "202402")):
            checkpoint[(int(row[1]), int(row[0]))] = parse_number(row[5])
    game_ids = sorted({key[0] for key in raw_by_key})
    if set(game_ids) != set(games):
        raise RuntimeError(
            f"GAME_INVENTORY_MISMATCH:current={len(game_ids)}:checkpoint={len(games)}"
        )
    return raw_by_key, games, teams, checkpoint


def parse_mmss(value: Any) -> int | None:
    if not isinstance(value, str) or ":" not in value:
        return None
    try:
        mins, secs = value.split(":", 1)
        return int(mins) * 60 + int(secs)
    except (TypeError, ValueError):
        return None


def boxscore_skaters(box: dict[str, Any]) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    players = {}
    teams = {}
    pgs = box.get("playerByGameStats") or {}
    for side, is_home in (("homeTeam", True), ("awayTeam", False)):
        team = box.get(side) or {}
        team_id = int(team["id"])
        abbr = str(team["abbrev"]).upper()
        if team_id in teams.values() or abbr in teams:
            raise ValueError("DUPLICATE_BOXSCORE_TEAM_IDENTITY")
        teams[abbr] = team_id
        side_stats = pgs.get(side)
        if not isinstance(side_stats, dict):
            raise ValueError("BOXSCORE_SKATER_SECTION_MISSING")
        defense = side_stats.get("defense") or side_stats.get("defensemen") or []
        for group in (side_stats.get("forwards") or [], defense):
            if not isinstance(group, list):
                raise ValueError("BOXSCORE_SKATER_ROWS_INVALID")
            for p in group:
                pid = int(p["playerId"])
                toi = parse_mmss(p.get("toi"))
                if toi is None:
                    raise ValueError(f"BOXSCORE_PLAYER_TOI_MISSING:{pid}")
                if pid in players:
                    raise ValueError(f"DUPLICATE_BOXSCORE_PLAYER:{pid}")
                players[pid] = {
                    "player_id": pid, "team_id": team_id, "team_abbr": abbr,
                    "is_home": is_home, "toi_seconds": toi,
                }
    if len(players) < 10:
        raise ValueError("BOXSCORE_SKATER_COVERAGE_IMPLAUSIBLE")
    return players, teams


def validate_game(game_id: int, pbp: dict[str, Any], box: dict[str, Any], shifts_doc: dict[str, Any]):
    if int(pbp.get("id") or -1) != game_id or int(box.get("id") or -1) != game_id:
        raise ValueError("OFFICIAL_GAME_ID_MISMATCH")
    if pbp.get("gameState") not in ("OFF", "FINAL") or box.get("gameState") not in ("OFF", "FINAL"):
        raise ValueError("OFFICIAL_GAME_NOT_FINAL")
    plays = pbp.get("plays")
    if not isinstance(plays, list) or not plays:
        raise ValueError("PBP_PLAYS_MISSING")
    raw_shifts = shifts_doc.get("data")
    if not isinstance(raw_shifts, list) or not raw_shifts:
        raise ValueError("SHIFTCHARTS_EMPTY")
    players, teams = boxscore_skaters(box)
    official_team_ids = set(teams.values())
    pbp_team_ids = {
        int((pbp.get(side) or {}).get("id") or -1) for side in ("homeTeam", "awayTeam")
    }
    if pbp_team_ids != official_team_ids:
        raise ValueError("PBP_BOXSCORE_TEAM_IDENTITY_MISMATCH")

    by_player: dict[int, list[tuple[int, int]]] = {}
    team_abbrs: dict[int, set[str]] = {}
    usable_count = 0
    malformed_skaters = set()
    zero_length_rows = 0
    for row in raw_shifts:
        if not isinstance(row, dict):
            raise ValueError("SHIFT_ROW_INVALID")
        if row.get("gameId") is not None and int(row["gameId"]) != game_id:
            raise ValueError("SHIFT_GAME_ID_MISMATCH")
        pid_raw = row.get("playerId")
        if pid_raw is None:
            continue
        pid = int(pid_raw)
        if pid not in players:
            continue
        abbr = str(row.get("teamAbbrev") or "").upper()
        if abbr != players[pid]["team_abbr"]:
            raise ValueError(f"SHIFT_PLAYER_TEAM_IDENTITY_MISMATCH:{pid}:{abbr}")
        interval = LEGACY_A.shift_abs_interval(row)
        if interval is None:
            start_time = parse_mmss(row.get("startTime"))
            end_time = parse_mmss(row.get("endTime"))
            if start_time is not None and start_time == end_time:
                zero_length_rows += 1
            else:
                malformed_skaters.add(pid)
            continue
        by_player.setdefault(pid, []).append(interval)
        team_abbrs.setdefault(pid, set()).add(abbr)
        usable_count += 1
    if malformed_skaters:
        raise ValueError(f"SHIFT_INTERVAL_MALFORMED_FOR_SKATERS:{len(malformed_skaters)}")
    missing = [pid for pid, p in players.items() if p["toi_seconds"] > 0 and pid not in by_player]
    if missing:
        raise ValueError(f"SHIFT_PLAYER_COVERAGE_MISSING:{len(missing)}")

    # A complete player record requires all official player shifts to sum near
    # boxscore TOI. The one-minute bound allows NHL shift/boxscore rounding.
    toi_deltas = {}
    for pid, player in players.items():
        shift_seconds = sum(e - s for s, e in by_player.get(pid, []))
        delta = abs(shift_seconds - player["toi_seconds"])
        toi_deltas[pid] = delta
        if delta > 60:
            raise ValueError(f"SHIFT_TOI_RECONCILIATION_FAILED:{pid}:{delta}")

    windows_a = LEGACY_A.build_pp_windows_from_situation(plays, require_goalies_present=True)
    windows_b = LEGACY_B.build_pp_segments_from_pbp(plays)
    if windows_a[0] != windows_b[0] or windows_a[1] != windows_b[1]:
        raise ValueError("PBP_POWERPLAY_WINDOW_METHODS_DISAGREE")
    official_home_abbr = str((box.get("homeTeam") or {}).get("abbrev") or "").upper()
    result = {}
    for pid, player in players.items():
        side_windows_a = windows_a[0] if player["is_home"] else windows_a[1]
        side_windows_b = windows_b[0] if player["is_home"] else windows_b[1]
        shifts_for_player = by_player.get(pid, [])
        first_seconds = LEGACY_A.sum_overlap_seconds(shifts_for_player, side_windows_a)
        second_seconds = LEGACY_B.pp_seconds_for_player(shifts_for_player, side_windows_b)
        first_minutes = round(first_seconds / 60.0, 2)
        second_minutes = round(second_seconds / 60.0, 2)
        if first_minutes != second_minutes:
            raise ValueError(f"PLAYER_PP_TOI_METHODS_DISAGREE:{pid}:{first_minutes}:{second_minutes}")
        if first_minutes * 60 > player["toi_seconds"] + 1:
            raise ValueError(f"PP_TOI_EXCEEDS_OFFICIAL_TOI:{pid}")
        result[pid] = {
            **player,
            "reconstructed_pp_toi_minutes": first_minutes,
            "alternate_pp_toi_minutes": second_minutes,
            "shift_count": len(by_player.get(pid, [])),
            "shift_coverage_seconds": sum(e - s for s, e in by_player.get(pid, [])),
            "shift_toi_delta_seconds": toi_deltas[pid],
        }
    return result, {
        "status": "RECONCILED",
        "game_id": game_id,
        "game_state": pbp.get("gameState"),
        "boxscore_game_state": box.get("gameState"),
        "home_team": box.get("homeTeam"),
        "away_team": box.get("awayTeam"),
        "player_count": len(players),
        "shift_row_count": len(raw_shifts),
        "usable_skaters_shift_row_count": usable_count,
        "zero_length_shift_rows_ignored": zero_length_rows,
        "missing_positive_toi_shifts": 0,
        "maximum_shift_to_boxscore_toi_delta_seconds": max(toi_deltas.values(), default=0),
        "team_pp_wall_seconds": {
            "home": sum(e - s for s, e in windows_a[0]),
            "away": sum(e - s for s, e in windows_a[1]),
        },
        "reconstruction_methods": [
            "archive/rebuild_pp_toi_from_shiftcharts_and_pbp.py",
            "archive/20260222_runtime_cleanup/approx_pp_toi_from_shiftcharts.py",
        ],
    }


def atomic_write(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def source_path(source_root: Path, game_id: int, family: str) -> Path:
    return source_root / f"game={game_id}" / f"{family}.json.gz"


def load_gzip_json(path: Path) -> dict[str, Any] | None:
    try:
        with gzip.open(path, "rb") as f:
            value = json.loads(f.read())
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def existing_response(reuse_root: Path, game_id: int, family: str):
    plain = reuse_root / f"{game_id}_{family}.json"
    if plain.is_file():
        body = plain.read_bytes()
        return json.loads(body), {
            "family": family, "path": str(plain), "sha256": sha256_bytes(body),
            "bytes": len(body), "http_status": 200, "reused": True,
        }
    return None


def fetch_one(session: requests.Session, game_id: int, family: str, url: str, out_path: Path,
              reuse_root: Path, max_backoff: int = 300):
    cached = load_gzip_json(out_path)
    if cached is not None:
        raw = gzip.open(out_path, "rb").read()
        return cached, {
            "family": family, "path": str(out_path), "sha256": sha256_bytes(raw),
            "bytes": len(raw), "compressed_sha256": sha256_bytes(out_path.read_bytes()),
            "http_status": 200, "reused": True,
        }
    reused = existing_response(reuse_root, game_id, family)
    if reused:
        payload, receipt = reused
        body = (reuse_root / f"{game_id}_{family}.json").read_bytes()
        encoded = gzip.compress(body, mtime=0)
        atomic_write(out_path, encoded)
        receipt.update({"compressed_sha256": sha256_bytes(encoded), "resumed": True})
        return payload, receipt

    attempts = 0
    delay = 1
    while True:
        attempts += 1
        started = utc_now()
        try:
            response = session.get(url, timeout=(20, 90))
            received = utc_now()
            if response.status_code == 200:
                body = response.content
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("OFFICIAL_JSON_ROOT_NOT_OBJECT")
                compressed = gzip.compress(body, mtime=0)
                atomic_write(out_path, compressed)
                return payload, {
                    "family": family, "url": url, "path": str(out_path),
                    "sha256": sha256_bytes(body), "compressed_sha256": sha256_bytes(compressed),
                    "bytes": len(body), "http_status": 200, "attempts": attempts,
                    "request_started_at_utc": started, "response_received_at_utc": received,
                    "reused": False,
                }
            if response.status_code == 404:
                body = response.content
                return None, {
                    "family": family, "url": url, "http_status": 404,
                    "sha256": sha256_bytes(body), "bytes": len(body), "attempts": attempts,
                    "request_started_at_utc": started, "response_received_at_utc": received,
                    "error": "OFFICIAL_SOURCE_NOT_FOUND",
                }
            if response.status_code < 500 and response.status_code != 429:
                body = response.content
                return None, {
                    "family": family, "url": url, "http_status": response.status_code,
                    "sha256": sha256_bytes(body), "bytes": len(body), "attempts": attempts,
                    "request_started_at_utc": started, "response_received_at_utc": received,
                    "error": "OFFICIAL_SOURCE_CLIENT_ERROR",
                }
            error = f"HTTP_{response.status_code}"
        except (requests.RequestException, ValueError) as exc:
            received = utc_now()
            error = type(exc).__name__ + ":" + str(exc)
        time.sleep(min(delay, max_backoff))
        delay = min(delay * 2, max_backoff)


def acquire_game(game_id: int, source_root: Path, reuse_root: Path):
    urls = {
        "pbp": PBP_URL.format(game_id=game_id),
        "boxscore": BOXSCORE_URL.format(game_id=game_id),
        "shiftcharts": SHIFT_URL.format(game_id=game_id),
    }
    session = requests.Session()
    session.headers.update({"User-Agent": "proppadia-nhl-pp-toi-integrity-recovery/1"})
    payloads, receipts = {}, []
    for family, url in urls.items():
        payload, receipt = fetch_one(
            session, game_id, family, url, source_path(source_root, game_id, family), reuse_root,
        )
        receipts.append(receipt)
        if payload is None:
            return {
                "game_id": game_id, "status": "SOURCE_UNRESOLVED",
                "reason": receipt.get("error", "OFFICIAL_SOURCE_UNAVAILABLE"),
                "responses": receipts,
            }, None, {}
        payloads[family] = payload
    try:
        reconstructed, summary = validate_game(
            game_id, payloads["pbp"], payloads["boxscore"], payloads["shiftcharts"],
        )
        summary["source_hashes"] = {r["family"]: r["sha256"] for r in receipts}
        return {"game_id": game_id, **summary, "responses": receipts}, reconstructed, payloads
    except Exception as exc:
        return {
            "game_id": game_id, "status": "SOURCE_UNRESOLVED",
            "reason": str(exc), "responses": receipts,
        }, None, payloads


def dump_json(path: Path, value: Any):
    atomic_write(path, (json.dumps(value, sort_keys=True, indent=2, default=str) + "\n").encode())


def write_csv_atomic(path: Path, rows: list[dict[str, Any]], fields: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def assess_equal_team_allocation(game_receipts, reconstructions, raw_by_key):
    """Count uniform team-game values and exact PP-wall-clock/N-skater matches."""
    uniform_groups = 0
    equal_allocation_groups = 0
    checked_groups = 0
    for game_id, player_map in reconstructions.items():
        rec = game_receipts[game_id]
        pp_wall = rec.get("team_pp_wall_seconds", {})
        for side, is_home in (("home", True), ("away", False)):
            rows = [
                raw_by_key.get((game_id, int(pid)))
                for pid, player in player_map.items()
                if player["is_home"] == is_home
            ]
            rows = [r for r in rows if r is not None]
            values = [parse_number(r.get("pp_toi_minutes")) for r in rows]
            if len(values) < 10 or any(v is None for v in values):
                continue
            checked_groups += 1
            rounded_values = {round(v, 2) for v in values}
            if len(rounded_values) != 1:
                continue
            uniform_groups += 1
            expected = round(float(pp_wall.get(side, 0)) / 60 / len(values), 2)
            if next(iter(rounded_values)) == expected:
                equal_allocation_groups += 1
    return {
        "team_game_groups_checked": checked_groups,
        "exactly_uniform_current_pp_toi_groups": uniform_groups,
        "uniform_groups_matching_team_pp_wall_minutes_divided_by_skater_count": equal_allocation_groups,
    }


def write_repair_sql(output_root: Path, candidates: list[dict[str, Any]], run_hash: str):
    repair_rows = [r for r in candidates if r["classification"] in ("VERIFIED_INCORRECT", "RECOVERABLE_NULL")]
    if not repair_rows:
        return {"candidate_count": 0}
    table_rows = ",\n".join(
        "(" + ", ".join([
            str(r["game_id"]), str(r["player_id"]), sql_literal(r["current_pp_toi_minutes"]),
            sql_literal(r["reconstructed_pp_toi_minutes"]), sql_literal(r["source_pbp_sha256"]),
            sql_literal(r["source_boxscore_sha256"]), sql_literal(r["source_shiftcharts_sha256"]),
        ]) + ")" for r in repair_rows
    )
    create_insert = f"""CREATE TEMP TABLE pp_toi_recovery_candidate (
 game_id bigint NOT NULL, player_id bigint NOT NULL, before_value numeric,
 official_value numeric NOT NULL, pbp_sha256 text NOT NULL, boxscore_sha256 text NOT NULL,
 shiftcharts_sha256 text NOT NULL, PRIMARY KEY (game_id, player_id)
) ON COMMIT DROP;
INSERT INTO pp_toi_recovery_candidate VALUES
{table_rows};
"""
    guard = f"""DO $$
DECLARE n integer; eligible integer; changed integer;
BEGIN
 SELECT COUNT(*) INTO n FROM pp_toi_recovery_candidate;
 IF n <> {len(repair_rows)} THEN RAISE EXCEPTION 'PP_TOI_CANDIDATE_COUNT_MISMATCH:%', n; END IF;
 IF EXISTS (SELECT 1 FROM pp_toi_recovery_candidate WHERE game_id::text NOT LIKE '202302%' AND game_id::text NOT LIKE '202402%') THEN
   RAISE EXCEPTION 'PP_TOI_OUT_OF_SCOPE_GAME_ID';
 END IF;
 IF EXISTS (SELECT 1 FROM pp_toi_recovery_candidate c LEFT JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
   WHERE t.game_id IS NULL OR (t.pp_toi_minutes IS DISTINCT FROM c.before_value AND t.pp_toi_minutes IS DISTINCT FROM c.official_value)) THEN
   RAISE EXCEPTION 'PP_TOI_BEFORE_VALUE_GUARD_FAILED';
 END IF;
 SELECT COUNT(*) INTO eligible FROM pp_toi_recovery_candidate c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
   WHERE t.pp_toi_minutes IS DISTINCT FROM c.official_value;
 UPDATE nhl.skater_game_logs_raw t SET pp_toi_minutes = c.official_value
 FROM pp_toi_recovery_candidate c WHERE t.game_id = c.game_id AND t.player_id = c.player_id
   AND t.pp_toi_minutes IS DISTINCT FROM c.official_value;
 GET DIAGNOSTICS changed = ROW_COUNT;
 IF changed <> eligible THEN RAISE EXCEPTION 'PP_TOI_CHANGED_ROW_COUNT_MISMATCH:%:%', changed, eligible; END IF;
 IF EXISTS (SELECT 1 FROM pp_toi_recovery_candidate c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
   WHERE t.pp_toi_minutes IS DISTINCT FROM c.official_value) THEN RAISE EXCEPTION 'PP_TOI_POST_UPDATE_VERIFY_FAILED'; END IF;
 RAISE NOTICE 'PP_TOI_RECOVERY_CHANGED_ROWS=%; candidate_sha256={run_hash}', changed;
END $$;
"""
    forward = "BEGIN;\n" + create_insert + guard + "COMMIT;\n"
    rollback_guard = f"""DO $$
DECLARE n integer; eligible integer; changed integer;
BEGIN
 SELECT COUNT(*) INTO n FROM pp_toi_recovery_candidate;
 IF n <> {len(repair_rows)} THEN RAISE EXCEPTION 'PP_TOI_CANDIDATE_COUNT_MISMATCH:%', n; END IF;
 IF EXISTS (SELECT 1 FROM pp_toi_recovery_candidate c LEFT JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
   WHERE t.game_id IS NULL OR (t.pp_toi_minutes IS DISTINCT FROM c.official_value AND t.pp_toi_minutes IS DISTINCT FROM c.before_value)) THEN
   RAISE EXCEPTION 'PP_TOI_ROLLBACK_CURRENT_VALUE_GUARD_FAILED';
 END IF;
 SELECT COUNT(*) INTO eligible FROM pp_toi_recovery_candidate c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
   WHERE t.pp_toi_minutes IS DISTINCT FROM c.before_value;
 UPDATE nhl.skater_game_logs_raw t SET pp_toi_minutes = c.before_value
 FROM pp_toi_recovery_candidate c WHERE t.game_id = c.game_id AND t.player_id = c.player_id
   AND t.pp_toi_minutes IS DISTINCT FROM c.before_value;
 GET DIAGNOSTICS changed = ROW_COUNT;
 IF changed <> eligible THEN RAISE EXCEPTION 'PP_TOI_ROLLBACK_CHANGED_ROW_COUNT_MISMATCH:%:%', changed, eligible; END IF;
 IF EXISTS (SELECT 1 FROM pp_toi_recovery_candidate c JOIN nhl.skater_game_logs_raw t USING (game_id, player_id)
   WHERE t.pp_toi_minutes IS DISTINCT FROM c.before_value) THEN RAISE EXCEPTION 'PP_TOI_ROLLBACK_VERIFY_FAILED'; END IF;
END $$;
"""
    rollback = "BEGIN;\n" + create_insert + rollback_guard + "COMMIT;\n"
    (output_root / "repair_nhl_2023_2024_pp_toi_from_official_shiftcharts.sql").write_text(forward)
    (output_root / "rollback_nhl_2023_2024_pp_toi_from_official_shiftcharts.sql").write_text(rollback)
    return {"candidate_count": len(repair_rows)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--current-snapshot", type=Path, default=DEFAULT_CURRENT)
    ap.add_argument("--checkpoint-sql", type=Path, default=DEFAULT_CHECKPOINT)
    ap.add_argument("--reuse-dir", type=Path, default=DEFAULT_REUSE)
    ap.add_argument("--output-dir", type=Path, default=ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit-games", type=int, default=0)
    ap.add_argument("--game-id", type=int, action="append", default=[])
    args = ap.parse_args()
    if args.workers < 1:
        ap.error("--workers must be positive")
    raw_by_key, games, teams, checkpoint = load_inventory(args.current_snapshot, args.checkpoint_sql)
    selected = sorted(args.game_id or games)
    if any(g not in games for g in selected):
        raise RuntimeError("REQUESTED_GAME_OUTSIDE_INVENTORY")
    if args.limit_games:
        selected = selected[:args.limit_games]
    source_root = args.output_dir / "official_sources"
    game_receipt_root = args.output_dir / "game_receipts"
    args.output_dir.mkdir(parents=True, exist_ok=True)

    completed = {}
    futures = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for gid in selected:
            receipt_path = game_receipt_root / f"game={gid}.json"
            if receipt_path.is_file():
                prior = json.loads(receipt_path.read_text())
                if prior.get("status") == "RECONCILED":
                    needed = [source_path(source_root, gid, k) for k in ("pbp", "boxscore", "shiftcharts")]
                    if all(p.is_file() for p in needed):
                        completed[gid] = prior
                        continue
            futures[pool.submit(acquire_game, gid, source_root, args.reuse_dir)] = gid
        for future in as_completed(futures):
            gid = futures[future]
            receipt, reconstructed, payloads = future.result()
            if reconstructed is not None:
                recon_path = args.output_dir / "reconstructions" / f"game={gid}.json"
                dump_json(recon_path, reconstructed)
                receipt["reconstruction_path"] = str(recon_path)
            dump_json(game_receipt_root / f"game={gid}.json", receipt)
            completed[gid] = receipt
            print(f"{gid} {receipt['status']} {receipt.get('reason', '')}", flush=True)

    # Re-read all retained per-game outputs, so a resumed run can assemble one
    # complete population from earlier and current acquisition batches.
    all_receipts = {}
    all_reconstructions = {}
    all_selected = sorted(games)
    for gid in all_selected:
        path = game_receipt_root / f"game={gid}.json"
        if not path.is_file():
            continue
        rec = json.loads(path.read_text())
        all_receipts[gid] = rec
        if rec.get("status") == "RECONCILED":
            rp = Path(rec["reconstruction_path"])
            all_reconstructions[gid] = json.loads(rp.read_text())

    candidates = []
    official_by_key = {}
    for gid, player_map in all_reconstructions.items():
        rec = all_receipts[gid]
        hashes = rec.get("source_hashes", {})
        for pid, official in player_map.items():
            official_by_key[(gid, int(pid))] = (official, hashes, rec)
    seen_official_keys = set()
    for (gid, pid), raw in sorted(raw_by_key.items()):
        hit = official_by_key.get((gid, pid))
        game_receipt = all_receipts.get(gid, {})
        if hit:
            official, hashes, game_receipt = hit
            seen_official_keys.add((gid, pid))
            value = parse_number(raw.get("pp_toi_minutes"))
            rec_value = official["reconstructed_pp_toi_minutes"]
            if value is None:
                classification = "RECOVERABLE_NULL"
            elif abs(value - rec_value) <= 0.005:
                classification = "VERIFIED_CORRECT"
            else:
                classification = "VERIFIED_INCORRECT"
            raw_tid = int(raw["team_id"])
            raw_team = LEGACY_TEAM_ID_TO_ABBR.get(raw_tid)
            raw_is_home = str(raw.get("is_home", "")).lower() in ("t", "true", "1")
            checkpoint_expected_team = games[gid]["home_team_id"] if raw_is_home else games[gid]["away_team_id"]
            candidates.append({
                "game_id": gid, "game_date": raw.get("game_date") or games[gid]["game_date"],
                "player_id": pid, "classification": classification,
                "current_pp_toi_minutes": value,
                "checkpoint_pp_toi_minutes": checkpoint.get((gid, pid)),
                "reconstructed_pp_toi_minutes": rec_value,
                "alternate_reconstruction_pp_toi_minutes": official["alternate_pp_toi_minutes"],
                "official_team_id": official["team_id"], "official_team_abbr": official["team_abbr"],
                "raw_team_id": raw_tid, "raw_team_abbr": raw_team,
                "raw_is_home": raw_is_home,
                "checkpoint_game_home_team_id": games[gid]["home_team_id"],
                "checkpoint_game_away_team_id": games[gid]["away_team_id"],
                "checkpoint_game_expected_team_id": checkpoint_expected_team,
                "raw_team_matches_checkpoint_game_side": raw_tid == checkpoint_expected_team,
                "official_is_home": official["is_home"],
                "raw_team_identity_match": raw_team == official["team_abbr"],
                "raw_team_id_matches_official_id": raw_tid == official["team_id"],
                "raw_home_side_match": (str(raw.get("is_home", "")).lower() in ("t", "true", "1")) == official["is_home"],
                "official_boxscore_toi_seconds": official["toi_seconds"],
                "shift_coverage_seconds": official["shift_coverage_seconds"],
                "shift_toi_delta_seconds": official["shift_toi_delta_seconds"],
                "source_pbp_sha256": hashes.get("pbp"),
                "source_boxscore_sha256": hashes.get("boxscore"),
                "source_shiftcharts_sha256": hashes.get("shiftcharts"),
                "validation_status": "RECONCILED_FINAL_SOURCE_AND_DUAL_METHODS",
            })
        else:
            reason = game_receipt.get("reason", "GAME_NOT_YET_ACQUIRED")
            candidates.append({
                "game_id": gid, "game_date": raw.get("game_date") or games[gid]["game_date"],
                "player_id": pid, "classification": "UNRESOLVED",
                "current_pp_toi_minutes": parse_number(raw.get("pp_toi_minutes")),
                "checkpoint_pp_toi_minutes": checkpoint.get((gid, pid)),
                "reconstructed_pp_toi_minutes": None,
                "alternate_reconstruction_pp_toi_minutes": None,
                "raw_team_id": raw.get("team_id"),
                "raw_team_abbr": LEGACY_TEAM_ID_TO_ABBR.get(int(raw["team_id"])),
                "raw_is_home": raw.get("is_home"), "validation_status": reason,
                "source_pbp_sha256": None, "source_boxscore_sha256": None,
                "source_shiftcharts_sha256": None,
            })
    for (gid, pid), (official, hashes, rec) in sorted(official_by_key.items()):
        if (gid, pid) in raw_by_key:
            continue
        candidates.append({
            "game_id": gid, "game_date": games[gid]["game_date"], "player_id": pid,
            "classification": "UNRESOLVED_NO_RAW_PLAYER_GAME_ROW",
            "current_pp_toi_minutes": None,
            "checkpoint_pp_toi_minutes": checkpoint.get((gid, pid)),
            "reconstructed_pp_toi_minutes": official["reconstructed_pp_toi_minutes"],
            "alternate_reconstruction_pp_toi_minutes": official["alternate_pp_toi_minutes"],
            "official_team_id": official["team_id"], "official_team_abbr": official["team_abbr"],
            "official_boxscore_toi_seconds": official["toi_seconds"],
            "source_pbp_sha256": hashes.get("pbp"), "source_boxscore_sha256": hashes.get("boxscore"),
            "source_shiftcharts_sha256": hashes.get("shiftcharts"),
            "validation_status": "OFFICIAL_SOURCES_RECONCILED_BUT_RAW_KEY_ABSENT",
        })
    for (gid, pid), checkpoint_value in sorted(checkpoint.items()):
        if (gid, pid) in raw_by_key or (gid, pid) in official_by_key:
            continue
        candidates.append({
            "game_id": gid, "game_date": games[gid]["game_date"], "player_id": pid,
            "classification": "UNRESOLVED_NO_CURRENT_RAW_ROW",
            "current_pp_toi_minutes": None, "checkpoint_pp_toi_minutes": checkpoint_value,
            "reconstructed_pp_toi_minutes": None, "alternate_reconstruction_pp_toi_minutes": None,
            "validation_status": "CHECKPOINT_IDENTITY_ABSENT_FROM_CURRENT_RAW_AND_RECONCILED_SOURCE",
            "source_pbp_sha256": None, "source_boxscore_sha256": None, "source_shiftcharts_sha256": None,
        })
    fields = list(dict.fromkeys(k for row in candidates for k in row))
    candidate_path = args.output_dir / "pp_toi_recovery_candidates.csv"
    write_csv_atomic(candidate_path, candidates, fields)
    candidate_hash = sha256_bytes(candidate_path.read_bytes())
    summary = {
        "schema_version": "NHL_PP_TOI_HISTORICAL_RECOVERY_V1",
        "generated_at_utc": utc_now(),
        "scope_game_id_prefixes": ["202302", "202402"],
        "inventory": {
            "game_count": len(games), "player_game_rows_current_snapshot": len(raw_by_key),
            "checkpoint_player_game_rows": sum(1 for k in checkpoint if str(k[0]).startswith(("202302", "202402"))),
        },
        "acquisition": {
            "games_with_receipt": len(all_receipts),
            "games_reconciled": sum(r.get("status") == "RECONCILED" for r in all_receipts.values()),
            "games_unresolved": sum(r.get("status") != "RECONCILED" for r in all_receipts.values()),
            "games_not_yet_acquired": len(games) - len(all_receipts),
        },
        "candidate_counts": {},
        "historical_equal_team_allocation_signature": assess_equal_team_allocation(
            all_receipts, all_reconstructions, raw_by_key,
        ),
        "raw_team_identity_comparison": {
            "rows_with_official_source": sum(
                1 for r in candidates if r.get("official_team_id") is not None
            ),
            "raw_team_id_namespace_differences": sum(
                1 for r in candidates if r.get("raw_team_id_matches_official_id") is False
            ),
            "raw_team_abbreviation_mismatches": sum(
                1 for r in candidates if r.get("raw_team_identity_match") is False
            ),
            "raw_home_side_mismatches": sum(
                1 for r in candidates if r.get("raw_home_side_match") is False
            ),
        },
        "team_identity": {
            "raw_team_id_mapping": "legacy fetch_nhl_to_csv.py stable team_id-to-abbreviation mapping; official NHL IDs are compared through abbreviation",
            "checkpoint_game_identity_values_from_dump": True,
        },
        "candidate_path": str(candidate_path), "candidate_sha256": candidate_hash,
        "source_root": str(source_root),
        "game_receipt_root": str(game_receipt_root),
    }
    from collections import Counter
    counts = Counter(r["classification"] for r in candidates)
    summary["candidate_counts"] = dict(counts)
    summary["recoverable_verified_zeros"] = sum(
        r["classification"] in ("VERIFIED_INCORRECT", "VERIFIED_CORRECT", "RECOVERABLE_NULL")
        and r.get("reconstructed_pp_toi_minutes") == 0 for r in candidates
    )
    summary_path = args.output_dir / "recovery_summary.json"
    dump_json(summary_path, summary)
    sql_result = write_repair_sql(args.output_dir, candidates, candidate_hash)
    summary["prepared_database_correction"] = sql_result
    dump_json(summary_path, summary)
    acquisition_manifest = {
        "schema_version": "NHL_PP_TOI_OFFICIAL_ACQUISITION_V1",
        "generated_at_utc": utc_now(), "source_root": str(source_root),
        "inventory_game_count": len(games), "receipts": [all_receipts[g] for g in sorted(all_receipts)],
        "manifest_sha256_excludes_self": True,
    }
    manifest_path = args.output_dir / "official_acquisition_manifest.json"
    dump_json(manifest_path, acquisition_manifest)
    acquisition_manifest["sha256"] = sha256_bytes(manifest_path.read_bytes())
    dump_json(manifest_path, acquisition_manifest)
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
