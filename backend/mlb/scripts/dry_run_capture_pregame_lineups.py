#!/usr/bin/env python3
"""Dry-run MLB pregame lineup snapshot capture.

This utility fetches MLB StatsAPI schedule and boxscore payloads for a slate,
classifies current batting-order availability, and writes local artifacts only.
It performs no database writes, schema changes, precompute changes, model
changes, upload changes, or Morning Workbench changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[3]
STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"
VALID_LINEUP_STATUSES = {
    "confirmed_full",
    "partial",
    "missing",
    "source_unavailable",
    "invalid",
}


@dataclass
class FetchResult:
    payload: dict[str, Any]
    error: str
    payload_hash: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run current StatsAPI pregame lineup snapshot capture."
    )
    parser.add_argument("--date", required=True, help="Slate date in YYYY-MM-DD format.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--snapshot-label", required=True)
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    parser.add_argument("--statsapi-timeout-seconds", type=int, default=30)
    return parser.parse_args()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def _int_or_none(value: Any) -> int | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        try:
            return int(float(text))
        except Exception:
            return None


def _iso_to_datetime(value: str) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _fetch_json(url: str, timeout_seconds: int) -> FetchResult:
    try:
        with urlopen(url, timeout=timeout_seconds) as resp:  # nosec B310 - public MLB StatsAPI.
            body = resp.read()
        payload = json.loads(body.decode("utf-8"))
        payload_hash = hashlib.sha256(body).hexdigest()
        return FetchResult(payload if isinstance(payload, dict) else {}, "", payload_hash)
    except HTTPError as exc:
        return FetchResult({}, f"http_{exc.code}", "")
    except URLError as exc:
        return FetchResult({}, f"url_error:{exc.reason}", "")
    except Exception as exc:
        return FetchResult({}, f"{type(exc).__name__}:{exc}", "")


def _team_meta(team_obj: dict[str, Any]) -> dict[str, Any]:
    team = team_obj.get("team") or {}
    return {
        "team_id": _int_or_none(team.get("id")),
        "team": _clean(
            team.get("abbreviation")
            or team.get("teamCode")
            or team.get("fileCode")
            or team.get("name")
        ).upper(),
        "team_name": _clean(team.get("name")),
    }


def _lineup_bucket(slot: int | None) -> str:
    if slot is None:
        return "unknown"
    if 1 <= slot <= 3:
        return "top_order"
    if 4 <= slot <= 6:
        return "middle_order"
    if 7 <= slot <= 9:
        return "bottom_order"
    return "unknown"


def _parse_batting_order(value: Any) -> tuple[int | None, str]:
    raw = _clean(value)
    if not raw:
        return None, ""
    parsed = _int_or_none(raw)
    if parsed is None:
        return None, f"invalid_battingOrder:{raw}"
    slot = parsed // 100
    if 1 <= slot <= 9:
        return slot, ""
    return None, f"battingOrder_outside_1_9:{raw}"


def _schedule_games(date_text: str, timeout_seconds: int) -> tuple[list[dict[str, Any]], str]:
    url = f"{STATSAPI_BASE}/schedule?sportId=1&date={date_text}&hydrate=team,probablePitcher,linescore"
    fetched = _fetch_json(url, timeout_seconds)
    if fetched.error:
        return [], fetched.error
    games: list[dict[str, Any]] = []
    for day in fetched.payload.get("dates") or []:
        official_date = _clean(day.get("date"))
        for game in day.get("games") or []:
            teams = game.get("teams") or {}
            home = _team_meta(teams.get("home") or {})
            away = _team_meta(teams.get("away") or {})
            game_id = _int_or_none(game.get("gamePk"))
            start_dt = _iso_to_datetime(game.get("gameDate"))
            games.append(
                {
                    "game_id": game_id,
                    "game_date": official_date or date_text,
                    "game_start_time_utc": start_dt.isoformat().replace("+00:00", "Z") if start_dt else "",
                    "home_team_id": home["team_id"],
                    "home_team": home["team"],
                    "away_team_id": away["team_id"],
                    "away_team": away["team"],
                    "statsapi_game_status": _clean((game.get("status") or {}).get("detailedState")),
                    "abstract_game_state": _clean((game.get("status") or {}).get("abstractGameState")),
                }
            )
    return games, ""


def _boxscore_url(game_id: int) -> str:
    return f"{STATSAPI_BASE}/game/{game_id}/boxscore"


def _players_for_team(
    *,
    game: dict[str, Any],
    boxscore: dict[str, Any],
    side: str,
    snapshot_id: str,
    capture_run_id: str,
    source_url: str,
    source_fetched_at_utc: str,
    offset_minutes: float | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    teams = boxscore.get("teams") or {}
    team_payload = teams.get(side) or {}
    opponent_side = "home" if side == "away" else "away"
    team_meta = _team_meta(team_payload)
    opponent_meta = _team_meta(teams.get(opponent_side) or {})
    players = team_payload.get("players") or {}

    rows: list[dict[str, Any]] = []
    valid_slots: list[int] = []
    parse_errors: list[str] = []
    player_ids: list[int] = []

    for player_payload in players.values():
        batting_order_raw = _clean(player_payload.get("battingOrder"))
        if not batting_order_raw:
            continue
        person = player_payload.get("person") or {}
        player_id = _int_or_none(person.get("id"))
        slot, slot_error = _parse_batting_order(batting_order_raw)
        if player_id is not None:
            player_ids.append(player_id)
        if slot is not None:
            valid_slots.append(slot)
        if slot_error:
            parse_errors.append(slot_error)
        position = player_payload.get("position") or {}
        rows.append(
            {
                "capture_run_id": capture_run_id,
                "snapshot_id": snapshot_id,
                "game_id": game.get("game_id") or "",
                "game_date": game.get("game_date") or "",
                "game_start_time_utc": game.get("game_start_time_utc") or "",
                "source_fetched_at_utc": source_fetched_at_utc,
                "offset_to_first_pitch_minutes": "" if offset_minutes is None else round(offset_minutes, 2),
                "statsapi_game_status": game.get("statsapi_game_status") or "",
                "team_id": team_meta["team_id"] or "",
                "team": team_meta["team"],
                "opponent": opponent_meta["team"],
                "player_id": player_id or "",
                "player_name": _clean(person.get("fullName")),
                "batting_order_raw": batting_order_raw,
                "lineup_slot": slot or "",
                "lineup_bucket": _lineup_bucket(slot),
                "position": _clean(position.get("abbreviation") or position.get("name")),
                "confirmed_lineup_starter_flag": bool(slot is not None),
                "lineup_slot_semantics": "pregame_source_snapshot",
                "source": "statsapi_boxscore",
                "source_url": source_url,
                "validation_status": "candidate",
                "reject_reason": slot_error,
                "notes": "",
            }
        )

    duplicate_slot_count = len(valid_slots) - len(set(valid_slots))
    valid_slot_count = len(valid_slots)
    unique_player_count = len(set(player_ids))
    missing_slots = sorted(set(range(1, 10)) - set(valid_slots))

    if parse_errors or duplicate_slot_count:
        lineup_status = "invalid"
        status_reason = ";".join(sorted(set(parse_errors + ["duplicate_lineup_slot"] if duplicate_slot_count else parse_errors)))
    elif valid_slot_count == 9 and unique_player_count >= 9 and not missing_slots:
        lineup_status = "confirmed_full"
        status_reason = ""
    elif valid_slot_count == 0:
        lineup_status = "missing"
        status_reason = "no_battingOrder_rows"
    else:
        lineup_status = "partial"
        status_reason = f"valid_slots={valid_slot_count};missing_slots={','.join(map(str, missing_slots))}"

    for row in rows:
        row["validation_status"] = "accepted" if lineup_status == "confirmed_full" else "warning"
        row["team_lineup_status"] = lineup_status
        if not row["reject_reason"]:
            row["reject_reason"] = "" if lineup_status == "confirmed_full" else status_reason

    summary = {
        "capture_run_id": capture_run_id,
        "snapshot_id": snapshot_id,
        "game_id": game.get("game_id") or "",
        "game_date": game.get("game_date") or "",
        "game_start_time_utc": game.get("game_start_time_utc") or "",
        "source_fetched_at_utc": source_fetched_at_utc,
        "offset_to_first_pitch_minutes": "" if offset_minutes is None else round(offset_minutes, 2),
        "statsapi_game_status": game.get("statsapi_game_status") or "",
        "side": side,
        "team_id": team_meta["team_id"] or "",
        "team": team_meta["team"],
        "opponent": opponent_meta["team"],
        "valid_lineup_slot_count": valid_slot_count,
        "unique_player_count": unique_player_count,
        "duplicate_slot_count": duplicate_slot_count,
        "lineup_status": lineup_status,
        "status_reason": status_reason,
        "source": "statsapi_boxscore",
        "source_url": source_url,
    }
    return summary, rows


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _count(rows: list[dict[str, Any]], field: str, value: str) -> int:
    return sum(1 for row in rows if row.get(field) == value)


def main() -> int:
    args = _parse_args()
    date_text = args.date
    snapshot_label = args.snapshot_label
    capture_time = _now_utc()
    source_fetched_at_utc = capture_time.isoformat().replace("+00:00", "Z")
    capture_run_id = f"pregame_lineup_{date_text}_{snapshot_label}"
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    games, schedule_error = _schedule_games(date_text, args.statsapi_timeout_seconds)
    event_rows: list[dict[str, Any]] = []
    team_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []

    if schedule_error:
        error_rows.append(
            {
                "capture_run_id": capture_run_id,
                "snapshot_label": snapshot_label,
                "game_id": "",
                "source": "statsapi_schedule",
                "source_url": f"{STATSAPI_BASE}/schedule?sportId=1&date={date_text}",
                "error": schedule_error,
                "source_fetched_at_utc": source_fetched_at_utc,
            }
        )

    for game in games:
        game_id = game.get("game_id")
        snapshot_id = f"{capture_run_id}_{game_id}"
        start_dt = _iso_to_datetime(game.get("game_start_time_utc") or "")
        offset_minutes = None
        if start_dt is not None:
            offset_minutes = (start_dt - capture_time).total_seconds() / 60.0
        source_url = _boxscore_url(int(game_id)) if game_id else ""
        fetched = _fetch_json(source_url, args.statsapi_timeout_seconds) if game_id else FetchResult({}, "missing_game_id", "")

        event = {
            "capture_run_id": capture_run_id,
            "snapshot_id": snapshot_id,
            "snapshot_label": snapshot_label,
            "game_id": game_id or "",
            "game_date": game.get("game_date") or date_text,
            "game_start_time_utc": game.get("game_start_time_utc") or "",
            "source_fetched_at_utc": source_fetched_at_utc,
            "offset_to_first_pitch_minutes": "" if offset_minutes is None else round(offset_minutes, 2),
            "statsapi_game_status": game.get("statsapi_game_status") or "",
            "abstract_game_state": game.get("abstract_game_state") or "",
            "home_team": game.get("home_team") or "",
            "away_team": game.get("away_team") or "",
            "source": "statsapi_boxscore",
            "source_url": source_url,
            "source_payload_hash": fetched.payload_hash,
            "fetch_status": "ok" if not fetched.error else "source_unavailable",
            "fetch_error": fetched.error,
        }
        event_rows.append(event)

        if fetched.error:
            error_rows.append(
                {
                    "capture_run_id": capture_run_id,
                    "snapshot_label": snapshot_label,
                    "game_id": game_id or "",
                    "source": "statsapi_boxscore",
                    "source_url": source_url,
                    "error": fetched.error,
                    "source_fetched_at_utc": source_fetched_at_utc,
                }
            )
            for side in ("away", "home"):
                team_rows.append(
                    {
                        "capture_run_id": capture_run_id,
                        "snapshot_id": snapshot_id,
                        "game_id": game_id or "",
                        "game_date": game.get("game_date") or date_text,
                        "game_start_time_utc": game.get("game_start_time_utc") or "",
                        "source_fetched_at_utc": source_fetched_at_utc,
                        "offset_to_first_pitch_minutes": "" if offset_minutes is None else round(offset_minutes, 2),
                        "statsapi_game_status": game.get("statsapi_game_status") or "",
                        "side": side,
                        "team_id": game.get(f"{side}_team_id") or "",
                        "team": game.get(f"{side}_team") or "",
                        "opponent": game.get("home_team" if side == "away" else "away_team") or "",
                        "valid_lineup_slot_count": 0,
                        "unique_player_count": 0,
                        "duplicate_slot_count": 0,
                        "lineup_status": "source_unavailable",
                        "status_reason": fetched.error,
                        "source": "statsapi_boxscore",
                        "source_url": source_url,
                    }
                )
            continue

        for side in ("away", "home"):
            summary, rows = _players_for_team(
                game=game,
                boxscore=fetched.payload,
                side=side,
                snapshot_id=snapshot_id,
                capture_run_id=capture_run_id,
                source_url=source_url,
                source_fetched_at_utc=source_fetched_at_utc,
                offset_minutes=offset_minutes,
            )
            team_rows.append(summary)
            player_rows.extend(rows)

    event_fields = [
        "capture_run_id",
        "snapshot_id",
        "snapshot_label",
        "game_id",
        "game_date",
        "game_start_time_utc",
        "source_fetched_at_utc",
        "offset_to_first_pitch_minutes",
        "statsapi_game_status",
        "abstract_game_state",
        "home_team",
        "away_team",
        "source",
        "source_url",
        "source_payload_hash",
        "fetch_status",
        "fetch_error",
    ]
    team_fields = [
        "capture_run_id",
        "snapshot_id",
        "game_id",
        "game_date",
        "game_start_time_utc",
        "source_fetched_at_utc",
        "offset_to_first_pitch_minutes",
        "statsapi_game_status",
        "side",
        "team_id",
        "team",
        "opponent",
        "valid_lineup_slot_count",
        "unique_player_count",
        "duplicate_slot_count",
        "lineup_status",
        "status_reason",
        "source",
        "source_url",
    ]
    player_fields = [
        "capture_run_id",
        "snapshot_id",
        "game_id",
        "game_date",
        "game_start_time_utc",
        "source_fetched_at_utc",
        "offset_to_first_pitch_minutes",
        "statsapi_game_status",
        "team_id",
        "team",
        "opponent",
        "player_id",
        "player_name",
        "batting_order_raw",
        "lineup_slot",
        "lineup_bucket",
        "position",
        "confirmed_lineup_starter_flag",
        "lineup_slot_semantics",
        "team_lineup_status",
        "source",
        "source_url",
        "validation_status",
        "reject_reason",
        "notes",
    ]
    error_fields = [
        "capture_run_id",
        "snapshot_label",
        "game_id",
        "source",
        "source_url",
        "error",
        "source_fetched_at_utc",
    ]

    suffix = f"{date_text}_{snapshot_label}"
    _write_csv(out_dir / f"pregame_lineup_event_summary_{suffix}.csv", event_rows, event_fields)
    _write_csv(out_dir / f"pregame_lineup_game_team_summary_{suffix}.csv", team_rows, team_fields)
    _write_csv(out_dir / f"pregame_lineup_player_rows_{suffix}.csv", player_rows, player_fields)
    _write_csv(out_dir / f"pregame_lineup_source_errors_{suffix}.csv", error_rows, error_fields)

    status_counts = {status: _count(team_rows, "lineup_status", status) for status in sorted(VALID_LINEUP_STATUSES)}
    lines = [
        "# Pregame Lineup Snapshot Dry Run",
        "",
        f"- Date: `{date_text}`",
        f"- Snapshot label: `{snapshot_label}`",
        f"- Mode: `{args.mode}`",
        f"- Generated at: `{source_fetched_at_utc}`",
        f"- Source: `statsapi_boxscore`",
        f"- DB writes: `0`",
        "",
        "## Summary",
        "",
        f"- Games checked: `{len(games)}`",
        f"- Team lineups checked: `{len(team_rows)}`",
        f"- Confirmed full: `{status_counts.get('confirmed_full', 0)}`",
        f"- Partial: `{status_counts.get('partial', 0)}`",
        f"- Missing: `{status_counts.get('missing', 0)}`",
        f"- Invalid: `{status_counts.get('invalid', 0)}`",
        f"- Source unavailable: `{status_counts.get('source_unavailable', 0)}`",
        f"- Player rows captured: `{len(player_rows)}`",
        f"- Source errors: `{len(error_rows)}`",
        "",
        "## Interpretation",
        "",
        "This is a dry-run source snapshot. `confirmed_full` means the current StatsAPI boxscore payload",
        "contained nine unique batting-order slots for that team at capture time. It does not write to",
        "the database and does not alter precompute, model, selector, upload, or Workbench behavior.",
        "",
        "## Outputs",
        "",
        f"- `pregame_lineup_event_summary_{suffix}.csv`",
        f"- `pregame_lineup_game_team_summary_{suffix}.csv`",
        f"- `pregame_lineup_player_rows_{suffix}.csv`",
        f"- `pregame_lineup_source_errors_{suffix}.csv`",
    ]
    (out_dir / f"pregame_lineup_snapshot_summary_{suffix}.md").write_text("\n".join(lines) + "\n")

    print(f"games_checked={len(games)}")
    print(f"team_lineups={len(team_rows)}")
    for status in sorted(status_counts):
        print(f"{status}={status_counts[status]}")
    print(f"player_rows={len(player_rows)}")
    print(f"db_writes=0")
    print(f"output_dir={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
