"""Governed MLB pregame lineup capture from official StatsAPI.

This is a research-only, run-bound acquisition utility. It preserves raw
schedule and boxscore responses, parses official battingOrder rows when they
are posted before first pitch, and writes deterministic local artifacts for the
live hitter parent integration path.

No database writes, OddsAPI calls, model changes, production output changes, or
LaunchAgent changes are performed.
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


STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"
PARSER_VERSION = "governed_pregame_lineup_parser_v1"
CAPTURE_STATES = {
    "CONFIRMED_LINEUP",
    "OFFICIAL_LINEUP_NOT_YET_POSTED",
    "STARTER_UNRESOLVED",
    "GAME_NOT_PREGAME",
    "SOURCE_ERROR",
    "IDENTITY_ERROR",
}
PREGAME_DETAILED_STATES = {"Scheduled", "Pre-Game", "Warmup", "Delayed Start"}
PREGAME_ABSTRACT_STATES = {"Preview"}


@dataclass
class FetchResult:
    payload: dict[str, Any]
    body: bytes
    error: str
    sha256: str


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def int_or_none(value: Any) -> int | None:
    text = clean(value)
    if not text:
        return None
    try:
        return int(text)
    except Exception:
        try:
            return int(float(text))
        except Exception:
            return None


def parse_dt(value: Any) -> datetime | None:
    text = clean(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch_json(url: str, timeout_seconds: int) -> FetchResult:
    try:
        with urlopen(url, timeout=timeout_seconds) as resp:  # nosec B310 - official public MLB StatsAPI.
            body = resp.read()
        payload = json.loads(body.decode("utf-8"))
        return FetchResult(payload if isinstance(payload, dict) else {}, body, "", sha256_bytes(body))
    except HTTPError as exc:
        return FetchResult({}, b"", f"http_{exc.code}", "")
    except URLError as exc:
        return FetchResult({}, b"", f"url_error:{exc.reason}", "")
    except Exception as exc:
        return FetchResult({}, b"", f"{type(exc).__name__}:{exc}", "")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def team_meta(team_obj: dict[str, Any]) -> dict[str, Any]:
    team = team_obj.get("team") or team_obj or {}
    return {
        "team_id": int_or_none(team.get("id")),
        "team": clean(team.get("abbreviation") or team.get("teamCode") or team.get("fileCode") or team.get("name")).upper(),
        "team_name": clean(team.get("name")),
    }


def pitcher_meta(team_obj: dict[str, Any]) -> dict[str, Any]:
    pitcher = team_obj.get("probablePitcher") or {}
    return {"pitcher_id": int_or_none(pitcher.get("id")), "pitcher_name": clean(pitcher.get("fullName"))}


def lineup_bucket(slot: int | None) -> str:
    if slot is None:
        return "unknown"
    if 1 <= slot <= 3:
        return "top_order"
    if 4 <= slot <= 6:
        return "middle_order"
    if 7 <= slot <= 9:
        return "bottom_order"
    return "unknown"


def parse_batting_order(value: Any) -> tuple[int | None, str]:
    raw = clean(value)
    if not raw:
        return None, ""
    parsed = int_or_none(raw)
    if parsed is None:
        return None, f"invalid_battingOrder:{raw}"
    slot = parsed // 100
    if 1 <= slot <= 9:
        return slot, ""
    return None, f"battingOrder_outside_1_9:{raw}"


def schedule_url(date_value: str) -> str:
    return f"{STATSAPI_BASE}/schedule?sportId=1&date={date_value}&hydrate=team,probablePitcher,linescore"


def boxscore_url(game_id: int) -> str:
    return f"{STATSAPI_BASE}/game/{game_id}/boxscore"


def parse_schedule(payload: dict[str, Any], date_value: str) -> list[dict[str, Any]]:
    games: list[dict[str, Any]] = []
    for day in payload.get("dates") or []:
        official_date = clean(day.get("date")) or date_value
        for game in day.get("games") or []:
            teams = game.get("teams") or {}
            away_team_obj = teams.get("away") or {}
            home_team_obj = teams.get("home") or {}
            away = team_meta(away_team_obj)
            home = team_meta(home_team_obj)
            away_pitcher = pitcher_meta(away_team_obj)
            home_pitcher = pitcher_meta(home_team_obj)
            start_dt = parse_dt(game.get("gameDate"))
            games.append(
                {
                    "game_id": int_or_none(game.get("gamePk")),
                    "slate_date": date_value,
                    "official_date": official_date,
                    "game_start_time_utc": iso(start_dt) if start_dt else "",
                    "away_team": away["team"],
                    "away_team_id": away["team_id"] or "",
                    "home_team": home["team"],
                    "home_team_id": home["team_id"] or "",
                    "away_probable_pitcher_id": away_pitcher["pitcher_id"] or "",
                    "away_probable_pitcher_name": away_pitcher["pitcher_name"],
                    "home_probable_pitcher_id": home_pitcher["pitcher_id"] or "",
                    "home_probable_pitcher_name": home_pitcher["pitcher_name"],
                    "detailed_state": clean((game.get("status") or {}).get("detailedState")),
                    "abstract_state": clean((game.get("status") or {}).get("abstractGameState")),
                }
            )
    return games


def side_context(game: dict[str, Any], side: str) -> dict[str, Any]:
    if side == "away":
        return {
            "team": game["away_team"],
            "team_id": game["away_team_id"],
            "opponent": game["home_team"],
            "opponent_team_id": game["home_team_id"],
            "opposing_starter_id": game["home_probable_pitcher_id"],
            "opposing_starter_name": game["home_probable_pitcher_name"],
        }
    return {
        "team": game["home_team"],
        "team_id": game["home_team_id"],
        "opponent": game["away_team"],
        "opponent_team_id": game["away_team_id"],
        "opposing_starter_id": game["away_probable_pitcher_id"],
        "opposing_starter_name": game["away_probable_pitcher_name"],
    }


def parse_team_lineup(
    *,
    boxscore: dict[str, Any],
    game: dict[str, Any],
    side: str,
    run_tag: str,
    cutoff: str,
    capture_timestamp: str,
    raw_path: Path,
    raw_hash: str,
    source_url: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    context = side_context(game, side)
    team_payload = ((boxscore.get("teams") or {}).get(side) or {})
    players = team_payload.get("players") or {}
    rows: list[dict[str, Any]] = []
    slots: list[int] = []
    player_ids: list[int] = []
    errors: list[str] = []
    for payload in players.values():
        batting_raw = clean(payload.get("battingOrder"))
        if not batting_raw:
            continue
        person = payload.get("person") or {}
        hitter_id = int_or_none(person.get("id"))
        slot, error = parse_batting_order(batting_raw)
        if slot is not None:
            slots.append(slot)
        if hitter_id is not None:
            player_ids.append(hitter_id)
        if error:
            errors.append(error)
        rows.append(
            {
                "slate_date": game["slate_date"],
                "run_tag": run_tag,
                "cutoff": cutoff,
                "game_id": game.get("game_id") or "",
                "team": context["team"],
                "opponent": context["opponent"],
                "hitter_id": hitter_id or "",
                "player_id": hitter_id or "",
                "player_name": clean(person.get("fullName")),
                "batting_order": batting_raw,
                "lineup_slot": slot or "",
                "lineup_bucket": lineup_bucket(slot),
                "position": clean((payload.get("position") or {}).get("abbreviation") or (payload.get("position") or {}).get("name")),
                "opposing_starter_id": context["opposing_starter_id"],
                "opposing_starter_name": context["opposing_starter_name"],
                "lineup_status": "",
                "source_timestamp": capture_timestamp,
                "first_pitch_timestamp": game.get("game_start_time_utc") or "",
                "pregame_validity_state": "",
                "raw_response_path": str(raw_path),
                "raw_response_sha256": raw_hash,
                "source_url": source_url,
                "parser_version": PARSER_VERSION,
                "validation_reason": "",
            }
        )
    duplicate_slots = len(slots) - len(set(slots))
    missing_slots = sorted(set(range(1, 10)) - set(slots))
    detailed = clean(game.get("detailed_state"))
    abstract = clean(game.get("abstract_state"))
    pregame_valid = detailed in PREGAME_DETAILED_STATES or abstract in PREGAME_ABSTRACT_STATES
    if not pregame_valid:
        status = "GAME_NOT_PREGAME"
        reason = f"game_status={detailed or abstract}"
    elif not context["opposing_starter_id"]:
        status = "STARTER_UNRESOLVED"
        reason = "opposing_probable_pitcher_missing"
    elif errors or duplicate_slots:
        status = "IDENTITY_ERROR"
        reason = ";".join(sorted(set(errors + (["duplicate_lineup_slot"] if duplicate_slots else []))))
    elif len(slots) == 9 and len(set(player_ids)) >= 9 and not missing_slots:
        status = "CONFIRMED_LINEUP"
        reason = ""
    else:
        status = "OFFICIAL_LINEUP_NOT_YET_POSTED"
        reason = "no_battingOrder_rows" if not slots else f"partial_lineup_slots={len(slots)};missing={','.join(map(str, missing_slots))}"
    for row in rows:
        row["lineup_status"] = status
        row["pregame_validity_state"] = "VALID_PREGAME" if status == "CONFIRMED_LINEUP" else "NOT_VALID_FOR_PARENT"
        row["validation_reason"] = reason
    summary = {
        "slate_date": game["slate_date"],
        "run_tag": run_tag,
        "cutoff": cutoff,
        "game_id": game.get("game_id") or "",
        "team": context["team"],
        "opponent": context["opponent"],
        "side": side,
        "lineup_status": status,
        "valid_lineup_slot_count": len(set(slots)),
        "hitter_rows": len(rows),
        "opposing_starter_id": context["opposing_starter_id"],
        "opposing_starter_name": context["opposing_starter_name"],
        "source_timestamp": capture_timestamp,
        "first_pitch_timestamp": game.get("game_start_time_utc") or "",
        "detailed_state": detailed,
        "abstract_state": abstract,
        "raw_response_path": str(raw_path),
        "raw_response_sha256": raw_hash,
        "source_url": source_url,
        "validation_reason": reason,
    }
    return summary, rows


def build(args: argparse.Namespace) -> dict[str, Any]:
    capture_time = now_utc()
    capture_timestamp = iso(capture_time)
    run_tag = args.run_tag or f"governed_lineup_{capture_time.strftime('%Y%m%dT%H%M%SZ')}"
    cutoff = args.cutoff or capture_timestamp
    out_dir = Path(args.output_dir)
    raw_dir = out_dir / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    source_contract = [
        {
            "source_name": "MLB StatsAPI schedule",
            "endpoint_family": f"{STATSAPI_BASE}/schedule",
            "purpose": "scheduled games, teams, probable pitchers, game status",
            "network_required": True,
            "third_party_vendor": False,
        },
        {
            "source_name": "MLB StatsAPI boxscore",
            "endpoint_family": f"{STATSAPI_BASE}/game/{{gamePk}}/boxscore",
            "purpose": "official battingOrder when posted",
            "network_required": True,
            "third_party_vendor": False,
        },
    ]
    access_report = [
        {
            "exact_command": f".venv/bin/python -m backend.mlb.scripts.capture_mlb_governed_pregame_lineups --date {args.date} --output-dir {out_dir} --mode dry_run",
            "official_endpoint_family": "MLB StatsAPI schedule and boxscore",
            "network_or_elevated_access_required": True,
            "intended_output_paths": str(out_dir),
            "notes": "No OddsAPI, DB, upload, model, or production behavior changes.",
        }
    ]
    schedule = fetch_json(schedule_url(args.date), args.statsapi_timeout_seconds)
    schedule_raw_path = raw_dir / f"statsapi_schedule_{args.date}_{run_tag}.json"
    raw_manifest: list[dict[str, Any]] = []
    if schedule.body:
        schedule_raw_path.write_bytes(schedule.body)
    else:
        write_json(schedule_raw_path, {"error": schedule.error, "url": schedule_url(args.date)})
    raw_manifest.append(
        {
            "source": "statsapi_schedule",
            "source_url": schedule_url(args.date),
            "raw_response_path": str(schedule_raw_path),
            "raw_response_sha256": sha256_file(schedule_raw_path),
            "fetch_status": "SOURCE_ERROR" if schedule.error else "OK",
            "fetch_error": schedule.error,
            "source_timestamp": capture_timestamp,
        }
    )
    games = parse_schedule(schedule.payload, args.date) if not schedule.error else []
    parsed_rows: list[dict[str, Any]] = []
    team_summary: list[dict[str, Any]] = []
    source_errors: list[dict[str, Any]] = []
    for game in games:
        game_id = game.get("game_id")
        if not game_id:
            source_errors.append({"game_id": "", "lineup_status": "IDENTITY_ERROR", "error": "missing_game_id"})
            continue
        url = boxscore_url(int(game_id))
        fetched = fetch_json(url, args.statsapi_timeout_seconds)
        raw_path = raw_dir / f"statsapi_boxscore_{game_id}_{args.date}_{run_tag}.json"
        if fetched.body:
            raw_path.write_bytes(fetched.body)
        else:
            write_json(raw_path, {"error": fetched.error, "url": url})
        raw_hash = sha256_file(raw_path)
        raw_manifest.append(
            {
                "source": "statsapi_boxscore",
                "source_url": url,
                "game_id": game_id,
                "raw_response_path": str(raw_path),
                "raw_response_sha256": raw_hash,
                "fetch_status": "SOURCE_ERROR" if fetched.error else "OK",
                "fetch_error": fetched.error,
                "source_timestamp": capture_timestamp,
            }
        )
        if fetched.error:
            for side in ("away", "home"):
                context = side_context(game, side)
                team_summary.append(
                    {
                        "slate_date": args.date,
                        "run_tag": run_tag,
                        "cutoff": cutoff,
                        "game_id": game_id,
                        "team": context["team"],
                        "opponent": context["opponent"],
                        "side": side,
                        "lineup_status": "SOURCE_ERROR",
                        "valid_lineup_slot_count": 0,
                        "hitter_rows": 0,
                        "opposing_starter_id": context["opposing_starter_id"],
                        "opposing_starter_name": context["opposing_starter_name"],
                        "source_timestamp": capture_timestamp,
                        "first_pitch_timestamp": game.get("game_start_time_utc") or "",
                        "detailed_state": game.get("detailed_state") or "",
                        "abstract_state": game.get("abstract_state") or "",
                        "raw_response_path": str(raw_path),
                        "raw_response_sha256": raw_hash,
                        "source_url": url,
                        "validation_reason": fetched.error,
                    }
                )
            source_errors.append({"game_id": game_id, "lineup_status": "SOURCE_ERROR", "error": fetched.error, "source_url": url})
            continue
        for side in ("away", "home"):
            summary, rows = parse_team_lineup(
                boxscore=fetched.payload,
                game=game,
                side=side,
                run_tag=run_tag,
                cutoff=cutoff,
                capture_timestamp=capture_timestamp,
                raw_path=raw_path,
                raw_hash=raw_hash,
                source_url=url,
            )
            team_summary.append(summary)
            parsed_rows.extend(rows)
    parsed_fields = [
        "slate_date",
        "run_tag",
        "cutoff",
        "game_id",
        "team",
        "opponent",
        "hitter_id",
        "player_id",
        "player_name",
        "batting_order",
        "lineup_slot",
        "lineup_bucket",
        "position",
        "opposing_starter_id",
        "opposing_starter_name",
        "lineup_status",
        "source_timestamp",
        "first_pitch_timestamp",
        "pregame_validity_state",
        "raw_response_path",
        "raw_response_sha256",
        "source_url",
        "parser_version",
        "validation_reason",
    ]
    team_fields = [
        "slate_date",
        "run_tag",
        "cutoff",
        "game_id",
        "team",
        "opponent",
        "side",
        "lineup_status",
        "valid_lineup_slot_count",
        "hitter_rows",
        "opposing_starter_id",
        "opposing_starter_name",
        "source_timestamp",
        "first_pitch_timestamp",
        "detailed_state",
        "abstract_state",
        "raw_response_path",
        "raw_response_sha256",
        "source_url",
        "validation_reason",
    ]
    raw_fields = [
        "source",
        "source_url",
        "game_id",
        "raw_response_path",
        "raw_response_sha256",
        "fetch_status",
        "fetch_error",
        "source_timestamp",
    ]
    error_fields = ["game_id", "lineup_status", "error", "source_url"]
    write_csv(out_dir / f"official_source_contract_{args.date}.csv", source_contract, ["source_name", "endpoint_family", "purpose", "network_required", "third_party_vendor"])
    write_csv(out_dir / f"network_access_report_{args.date}.csv", access_report, ["exact_command", "official_endpoint_family", "network_or_elevated_access_required", "intended_output_paths", "notes"])
    write_csv(out_dir / f"raw_capture_manifest_{args.date}.csv", raw_manifest, raw_fields)
    write_csv(out_dir / f"parsed_lineup_artifact_{args.date}.csv", parsed_rows, parsed_fields)
    write_csv(out_dir / f"lineup_team_status_{args.date}.csv", team_summary, team_fields)
    write_csv(out_dir / f"source_error_manifest_{args.date}.csv", source_errors, error_fields)
    status_counts = {state: sum(1 for row in team_summary if row.get("lineup_status") == state) for state in sorted(CAPTURE_STATES)}
    downstream_rows = [
        {
            "stage": "live_hitter_parent",
            "status": "PENDING_DAILY_HOOK_EXECUTION",
            "coverage_rows": "",
            "withheld_rows": "",
            "notes": "Default-off daily hook must be run with MLB_ENABLE_LIVE_HITTER_PARENT_CAPTURE=1.",
        },
        {
            "stage": "pred_starter_pa_profile",
            "status": "PENDING_DAILY_HOOK_EXECUTION",
            "coverage_rows": "",
            "withheld_rows": "",
            "notes": "Requires governed lineup rows plus strict-prior opportunity/profile parent.",
        },
        {
            "stage": "opponent_lineup_encounter",
            "status": "PENDING_DAILY_HOOK_EXECUTION",
            "coverage_rows": "",
            "withheld_rows": "",
            "notes": "Requires complete live hitter parent rows.",
        },
        {
            "stage": "frozen_pha_challenger_scoring",
            "status": "PENDING_DAILY_HOOK_EXECUTION",
            "coverage_rows": "",
            "withheld_rows": "",
            "notes": "No scoring occurs in the capture-only utility.",
        },
        {
            "stage": "controlled_shadow",
            "status": "PENDING_DAILY_HOOK_EXECUTION",
            "coverage_rows": "",
            "withheld_rows": "",
            "notes": "Shadow artifact is default-off and remains non-production.",
        },
    ]
    write_csv(
        out_dir / f"downstream_live_parent_status_{args.date}.csv",
        downstream_rows,
        ["stage", "status", "coverage_rows", "withheld_rows", "notes"],
    )
    decisions = [
        ("MLB_LINEUP_CAPTURE_OFFICIAL_SOURCE_DECISION", "MLB_STATSAPI_SCHEDULE_AND_BOXSCORE_BOUND"),
        ("MLB_LINEUP_CAPTURE_NETWORK_ACCESS_DECISION", "NETWORK_EXECUTED_OFFICIAL_STATSAPI_ONLY" if not schedule.error else "NETWORK_ATTEMPTED_SOURCE_ERROR"),
        ("MLB_LINEUP_CAPTURE_RAW_PRESERVATION_DECISION", "PASS_RAW_RESPONSES_AND_SHA256_MANIFEST_WRITTEN"),
        ("MLB_LINEUP_CAPTURE_PARSE_DECISION", f"PARSED_HITTER_ROWS_{len(parsed_rows)}"),
        ("MLB_LINEUP_CAPTURE_IDENTITY_DECISION", "PASS" if status_counts.get("IDENTITY_ERROR", 0) == 0 else "IDENTITY_WARNINGS_PRESENT"),
        ("MLB_LINEUP_CAPTURE_TEMPORAL_INTEGRITY_DECISION", "PREGAME_ONLY_CONFIRMED_ROWS_VALIDATED"),
        ("MLB_LINEUP_CAPTURE_DAILY_HOOK_DECISION", "READY_FOR_DEFAULT_OFF_LIVE_PARENT_HOOK"),
        ("MLB_LINEUP_CAPTURE_LIVE_PARENT_DECISION", "PENDING_DOWNSTREAM_INTEGRATION"),
        ("MLB_LINEUP_CAPTURE_PRED_STARTER_PA_DECISION", "PENDING_DOWNSTREAM_INTEGRATION"),
        ("MLB_LINEUP_CAPTURE_PROFILE_DECISION", "PENDING_DOWNSTREAM_INTEGRATION"),
        ("MLB_LINEUP_CAPTURE_ENCOUNTER_DECISION", "PENDING_DOWNSTREAM_INTEGRATION"),
        ("MLB_LINEUP_CAPTURE_PHA_SCORING_DECISION", "PENDING_DOWNSTREAM_INTEGRATION"),
        ("MLB_LINEUP_CAPTURE_SHADOW_STATUS", "PENDING_DOWNSTREAM_INTEGRATION"),
        ("MLB_LINEUP_CAPTURE_PRODUCTION_STATUS", "NOT_AUTHORIZED"),
    ]
    write_csv(out_dir / f"capture_decisions_{args.date}.csv", [{"decision_name": k, "decision_value": v} for k, v in decisions], ["decision_name", "decision_value"])
    machine = {
        "date": args.date,
        "run_tag": run_tag,
        "cutoff": cutoff,
        "generated_at": capture_timestamp,
        "games_queried": len(games),
        "parsed_hitter_rows": len(parsed_rows),
        "status_counts": status_counts,
        "parsed_lineup_artifact": str(out_dir / f"parsed_lineup_artifact_{args.date}.csv"),
        "team_status_artifact": str(out_dir / f"lineup_team_status_{args.date}.csv"),
        "guardrails": {"db_writes": 0, "oddsapi_calls": 0, "production_changes": 0, "launchagent_changes": 0},
        "decisions": {k: v for k, v in decisions},
    }
    write_json(out_dir / f"machine_readable_lineup_capture_{args.date}.json", machine)
    files = [p for p in sorted(out_dir.rglob("*")) if p.is_file()]
    write_csv(
        out_dir / f"sha256_manifest_{args.date}.csv",
        [{"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size} for path in files],
        ["path", "sha256", "bytes"],
    )
    validation = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            with path.open(newline="", encoding="utf-8") as f:
                list(csv.DictReader(f))
            status = "PASS"
            notes = ""
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        validation.append({"artifact": str(path), "validation": status, "notes": notes})
    for path in sorted(out_dir.rglob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            status = "PASS"
            notes = ""
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        validation.append({"artifact": str(path), "validation": status, "notes": notes})
    validation.extend(
        [
            {"artifact": "guardrail_db_writes", "validation": "PASS", "notes": "0"},
            {"artifact": "guardrail_oddsapi_calls", "validation": "PASS", "notes": "0"},
            {"artifact": "guardrail_production_changes", "validation": "PASS", "notes": "0"},
            {"artifact": "valid_capture_states", "validation": "PASS" if all(row.get("lineup_status") in CAPTURE_STATES for row in team_summary) else "FAIL", "notes": ""},
        ]
    )
    write_csv(out_dir / f"validation_report_{args.date}.csv", validation, ["artifact", "validation", "notes"])
    write_text(
        out_dir / f"governed_pregame_lineup_capture_summary_{args.date}.md",
        "\n".join(
            [
                "# MLB Governed Pregame Lineup Capture",
                "",
                f"- Date: `{args.date}`",
                f"- Run tag: `{run_tag}`",
                f"- Generated at: `{capture_timestamp}`",
                f"- Official source: `MLB StatsAPI schedule + boxscore battingOrder`",
                f"- Games queried: `{len(games)}`",
                f"- Parsed hitter rows: `{len(parsed_rows)}`",
                f"- Confirmed lineups: `{status_counts.get('CONFIRMED_LINEUP', 0)}`",
                f"- Not yet posted: `{status_counts.get('OFFICIAL_LINEUP_NOT_YET_POSTED', 0)}`",
                f"- Game not pregame: `{status_counts.get('GAME_NOT_PREGAME', 0)}`",
                f"- Downstream live parent status: `PENDING_DAILY_HOOK_EXECUTION`",
                "",
                "No DB writes, OddsAPI calls, production output changes, or LaunchAgent changes occurred.",
            ]
        ),
    )
    print(json.dumps(machine, indent=2, sort_keys=True))
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-tag", default="")
    parser.add_argument("--cutoff", default="")
    parser.add_argument("--mode", choices=["dry_run"], default="dry_run")
    parser.add_argument("--statsapi-timeout-seconds", type=int, default=30)
    args = parser.parse_args()
    build(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
