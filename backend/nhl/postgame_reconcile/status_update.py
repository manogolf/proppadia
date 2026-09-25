"""Inactive, exact-slate NHL postgame status update contract.

This module consumes an already-retained official NHL schedule bundle.  It has
no network or scheduler entry point.  The only database mutation available is
an atomic update of ``nhl.games.status`` for the frozen September 24 slate.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import psycopg

from backend.nhl.official_request_journal import canonical_game_set_hash


CONTRACT_VERSION = "NHL_POSTGAME_STATUS_BUNDLE_V1"
SLATE_DATE = "2026-09-24"
FROZEN_GAME_IDS = tuple(range(2026010037, 2026010048))
FROZEN_GAME_SET_HASH = "92d828be583187109116de1eccda70c2bf8563288ec6ad39ce3976da43a9eec3"
BUNDLE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{7,127}$")
PACIFIC = ZoneInfo("America/Los_Angeles")
ALLOWED_STATUS = {"scheduled", "live", "final", "off"}


@dataclass(frozen=True)
class CanonicalGame:
    game_id: int
    game_date: str
    start_time_utc: str
    season: int
    game_type: int
    home_team_code: str
    away_team_code: str
    status: str


@dataclass(frozen=True)
class OfficialGame:
    game_id: int
    operational_date: str
    start_time_utc: str
    game_type: int
    home_team_code: str
    away_team_code: str
    state: str


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _aware_utc(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field}_MISSING")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field}_INVALID") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field}_MISSING_TIMEZONE")
    return parsed.astimezone(timezone.utc)


def _start_utc(game: dict[str, Any]) -> str:
    value = game.get("startTimeUTC")
    if not isinstance(value, str):
        raise ValueError("OFFICIAL_START_TIME_MISSING")
    return _aware_utc(value, "OFFICIAL_START_TIME").isoformat().replace("+00:00", "Z")


def _game_id(game: dict[str, Any]) -> int:
    try:
        return int(game.get("id"))
    except (TypeError, ValueError) as error:
        raise ValueError("OFFICIAL_GAME_ID_INVALID") from error


def _team_code(game: dict[str, Any], side: str) -> str:
    team = game.get(f"{side}Team")
    code = team.get("abbrev") if isinstance(team, dict) else None
    if not isinstance(code, str) or not code.strip():
        raise ValueError(f"OFFICIAL_{side.upper()}_TEAM_MISSING")
    return code.strip().upper()


def _flatten_schedule(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("gameWeek"), list):
        raise ValueError("OFFICIAL_SCHEDULE_SHAPE_INVALID")
    rows: list[dict[str, Any]] = []
    for day in payload["gameWeek"]:
        if not isinstance(day, dict) or not isinstance(day.get("games"), list):
            raise ValueError("OFFICIAL_SCHEDULE_DAY_SHAPE_INVALID")
        for game in day["games"]:
            if not isinstance(game, dict):
                raise ValueError("OFFICIAL_GAME_SHAPE_INVALID")
            rows.append(game)
    return rows


def _verify_bundle(bundle_dir: Path, expected_manifest_sha256: str) -> tuple[dict, list[OfficialGame]]:
    bundle_dir = bundle_dir.resolve(strict=True)
    if not bundle_dir.is_dir() or not BUNDLE_ID_RE.fullmatch(bundle_dir.name):
        raise ValueError("BUNDLE_ID_OR_DIRECTORY_INVALID")
    manifest_path = bundle_dir / "bundle_manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("BUNDLE_MANIFEST_MISSING_OR_UNSAFE")
    manifest_bytes = manifest_path.read_bytes()
    if _sha256(manifest_bytes) != expected_manifest_sha256:
        raise ValueError("BUNDLE_MANIFEST_HASH_MISMATCH")
    try:
        manifest = json.loads(manifest_bytes)
    except json.JSONDecodeError as error:
        raise ValueError("BUNDLE_MANIFEST_INVALID_JSON") from error
    if not isinstance(manifest, dict):
        raise ValueError("BUNDLE_MANIFEST_INVALID")
    if (manifest.get("contract_version") != CONTRACT_VERSION
            or manifest.get("bundle_id") != bundle_dir.name
            or manifest.get("slate_date") != SLATE_DATE):
        raise ValueError("BUNDLE_PROVENANCE_IDENTITY_MISMATCH")
    declared_ids = manifest.get("game_ids")
    if declared_ids != list(FROZEN_GAME_IDS):
        raise ValueError("BUNDLE_DECLARED_GAME_SET_MISMATCH")
    if (manifest.get("game_set_hash") != FROZEN_GAME_SET_HASH
            or canonical_game_set_hash(declared_ids) != FROZEN_GAME_SET_HASH):
        raise ValueError("BUNDLE_GAME_SET_HASH_MISMATCH")
    created_at = _aware_utc(manifest.get("created_at_utc"), "BUNDLE_CREATED_AT")

    sources = manifest.get("sources")
    if not isinstance(sources, list):
        raise ValueError("BUNDLE_SOURCES_INVALID")
    schedule_sources = [source for source in sources
                        if isinstance(source, dict)
                        and source.get("role") == "OFFICIAL_POSTGAME_SCHEDULE"]
    if len(schedule_sources) != 1 or len(sources) != 1:
        raise ValueError("BUNDLE_REQUIRES_ONE_OFFICIAL_SCHEDULE_SOURCE")
    source = schedule_sources[0]
    expected_url = f"https://api-web.nhle.com/v1/schedule/{SLATE_DATE}"
    if (source.get("provider") != "NHL"
            or source.get("url") != expected_url
            or source.get("http_status") != 200
            or source.get("redirect_count") != 0):
        raise ValueError("OFFICIAL_SCHEDULE_PROVENANCE_INVALID")
    fetched_at = _aware_utc(source.get("fetched_at_utc"), "SOURCE_FETCHED_AT")
    if created_at < fetched_at:
        raise ValueError("BUNDLE_CREATED_BEFORE_SOURCE_CAPTURE")
    rel = source.get("path")
    if not isinstance(rel, str) or Path(rel).is_absolute() or ".." in Path(rel).parts:
        raise ValueError("SOURCE_PATH_UNSAFE")
    source_path = bundle_dir / rel
    try:
        source_path.resolve(strict=True).relative_to(bundle_dir)
    except (OSError, ValueError) as error:
        raise ValueError("OFFICIAL_SCHEDULE_RESPONSE_PATH_ESCAPES_BUNDLE") from error
    if source_path.is_symlink() or not source_path.is_file():
        raise ValueError("OFFICIAL_SCHEDULE_RESPONSE_MISSING_OR_UNSAFE")
    raw_bytes = source_path.read_bytes()
    raw_hash = _sha256(raw_bytes)
    if source.get("sha256") != raw_hash:
        raise ValueError("OFFICIAL_SCHEDULE_HASH_MISMATCH")
    try:
        payload = json.loads(raw_bytes)
    except json.JSONDecodeError as error:
        raise ValueError("OFFICIAL_SCHEDULE_INVALID_JSON") from error

    expected = set(FROZEN_GAME_IDS)
    found: dict[int, OfficialGame] = {}
    seen_ids: set[int] = set()
    for game in _flatten_schedule(payload):
        game_id = _game_id(game)
        if game_id in seen_ids:
            if game_id in expected:
                raise ValueError(f"OFFICIAL_DUPLICATE_GAME_ID:{game_id}")
            continue
        seen_ids.add(game_id)
        start = _start_utc(game)
        start_dt = _aware_utc(start, "OFFICIAL_START_TIME")
        if start_dt.astimezone(PACIFIC).date().isoformat() != SLATE_DATE:
            if game_id in expected:
                raise ValueError(f"OFFICIAL_GAME_DATE_MISMATCH:{game_id}")
            continue
        if game_id not in expected:
            raise ValueError(f"OFFICIAL_EXTRA_GAME_ID:{game_id}")
        state = str(game.get("gameState") or "").strip().upper()
        if state not in {"FINAL", "OFF"}:
            raise ValueError(f"OFFICIAL_GAME_NOT_FINAL:{game_id}:{state or 'MISSING'}")
        if fetched_at <= start_dt:
            raise ValueError(f"OFFICIAL_RESPONSE_NOT_POSTGAME:{game_id}")
        try:
            game_type = int(game.get("gameType"))
        except (TypeError, ValueError) as error:
            raise ValueError(f"OFFICIAL_GAME_TYPE_MISSING:{game_id}") from error
        found[game_id] = OfficialGame(
            game_id=game_id, operational_date=SLATE_DATE, start_time_utc=start,
            game_type=game_type, home_team_code=_team_code(game, "home"),
            away_team_code=_team_code(game, "away"), state=state,
        )
    missing = expected - set(found)
    if missing:
        raise ValueError(f"OFFICIAL_MISSING_GAME_IDS:{','.join(map(str, sorted(missing)))}")
    if set(found) != expected or len(found) != len(FROZEN_GAME_IDS):
        raise ValueError("OFFICIAL_GAME_SET_MISMATCH")
    return manifest, [found[game_id] for game_id in FROZEN_GAME_IDS]


def _canonical_from_row(row: tuple[Any, ...]) -> CanonicalGame:
    return CanonicalGame(
        game_id=int(row[0]), game_date=str(row[1]),
        start_time_utc=_aware_utc(str(row[2]), "CANONICAL_START_TIME").isoformat().replace("+00:00", "Z"),
        season=int(row[3]), game_type=int(row[4]),
        home_team_code=str(row[5]).upper(), away_team_code=str(row[6]).upper(),
        status=str(row[7] or "").strip().lower(),
    )


def _validate_identity(canonical: CanonicalGame, official: OfficialGame) -> None:
    if canonical.game_id != official.game_id:
        raise ValueError(f"CANONICAL_GAME_ID_MISMATCH:{official.game_id}")
    if canonical.game_date != official.operational_date:
        raise ValueError(f"CANONICAL_GAME_DATE_MISMATCH:{official.game_id}")
    if canonical.start_time_utc != official.start_time_utc:
        raise ValueError(f"CANONICAL_START_TIME_MISMATCH:{official.game_id}")
    if (canonical.home_team_code != official.home_team_code
            or canonical.away_team_code != official.away_team_code):
        raise ValueError(f"CANONICAL_TEAM_MISMATCH:{official.game_id}")
    if canonical.season != 2026 or canonical.game_type not in {1, 2, 3}:
        raise ValueError(f"CANONICAL_SEASON_OR_TYPE_INVALID:{official.game_id}")
    if canonical.status not in ALLOWED_STATUS:
        raise ValueError(f"CANONICAL_STATUS_CONFLICT:{official.game_id}:{canonical.status}")


def update_frozen_statuses(
    *, bundle_dir: Path, expected_manifest_sha256: str, dsn: str,
    connect: Callable[[str], Any] = psycopg.connect,
) -> dict[str, Any]:
    """Apply a validated status-only update; callers must explicitly invoke it.

    Evidence is completely validated before connecting.  Canonical rows are
    then locked and checked inside one transaction before any update begins.
    """
    manifest, official_games = _verify_bundle(bundle_dir, expected_manifest_sha256)
    connection = connect(dsn)
    try:
        cursor = connection.cursor()
        try:
            ids = list(FROZEN_GAME_IDS)
            cursor.execute(
                """SELECT game_id, game_date::text, start_time_utc, season,
                          game_type, home_team_code, away_team_code, status
                     FROM nhl.games
                    WHERE game_id = ANY(%s)
                    ORDER BY game_id
                    FOR UPDATE""",
                (ids,),
            )
            rows = cursor.fetchall()
            canonical_rows = [_canonical_from_row(row) for row in rows]
            by_id = {row.game_id: row for row in canonical_rows}
            if len(canonical_rows) != len(FROZEN_GAME_IDS) or set(by_id) != set(FROZEN_GAME_IDS):
                raise ValueError("CANONICAL_GAME_SET_MISMATCH")
            for official in official_games:
                _validate_identity(by_id[official.game_id], official)

            changed = 0
            unchanged = 0
            for official in official_games:
                before = by_id[official.game_id]
                if before.status in {"final", "off"}:
                    unchanged += 1
                    continue
                # Deliberately only updates status and only by the frozen game ID.
                cursor.execute(
                    "UPDATE nhl.games SET status = %s WHERE game_id = %s",
                    ("final", official.game_id),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(f"STATUS_UPDATE_ROWCOUNT_MISMATCH:{official.game_id}")
                changed += 1
            connection.commit()
        finally:
            cursor.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "contract_version": CONTRACT_VERSION,
        "bundle_id": manifest["bundle_id"],
        "slate_date": SLATE_DATE,
        "game_ids": list(FROZEN_GAME_IDS),
        "game_set_hash": FROZEN_GAME_SET_HASH,
        "official_games": len(official_games),
        "status_value": "final",
        "updated_rows": changed,
        "already_final_rows": unchanged,
    }


def validate_retained_bundle(*, bundle_dir: Path,
                             expected_manifest_sha256: str) -> dict[str, Any]:
    """Read-only bundle validation for offline callers; performs no DB access."""
    manifest, games = _verify_bundle(bundle_dir, expected_manifest_sha256)
    return {
        "contract_version": CONTRACT_VERSION,
        "bundle_id": manifest["bundle_id"],
        "slate_date": SLATE_DATE,
        "game_ids": [game.game_id for game in games],
        "game_set_hash": FROZEN_GAME_SET_HASH,
        "all_final_or_off": True,
        "database_writes": 0,
        "network_requests": 0,
    }
