#!/usr/bin/env python3
"""Dry-run per-game MLB pregame lineup availability study runner.

Version 2 treats lineup release as a game-specific lifecycle. Each invocation
checks the slate, polls games only inside their pregame window, stops once both
teams are confirmed, and stops once a game has started. It uses MLB StatsAPI
only and writes local research artifacts only.
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts import dry_run_capture_pregame_lineups as snapshot


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = ROOT / "artifacts/analysis/mlb/pregame_lineup_capture/dry_runs"
PREGAME_STATUSES = {"Scheduled", "Pre-Game", "Warmup", "Delayed Start"}
VALID_ACTIONS = {
    "pending",
    "captured",
    "skipped_not_in_window",
    "stopped_confirmed",
    "stopped_started",
    "source_unavailable",
}
VALID_LINEUP_STATUSES = {"confirmed_full", "partial", "missing", "source_unavailable", "invalid"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run dry-run per-game MLB pregame lineup availability study."
    )
    parser.add_argument("--date", required=True, help="Slate date in YYYY-MM-DD format.")
    parser.add_argument("--mode", default="dry_run", choices=["dry_run"])
    parser.add_argument("--poll-mode", default="per_game", choices=["per_game", "slate_window"])
    parser.add_argument("--poll-window", default="auto", choices=["auto"])
    parser.add_argument("--snapshot-label", default="auto")
    parser.add_argument("--start-poll-minutes-before", type=float, default=150.0)
    parser.add_argument("--stop-at-confirmed", default="true", choices=["true", "false"])
    parser.add_argument("--stop-at-game-start", default="true", choices=["true", "false"])
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--statsapi-timeout-seconds", type=int, default=30)
    return parser.parse_args()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _bool_arg(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _append_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def _run_label(now: datetime) -> str:
    return "run_" + now.strftime("%Y%m%dT%H%M%SZ")


def _team_status_for_game(team_rows: list[dict[str, Any]], game_id: Any) -> dict[str, dict[str, Any]]:
    gid = str(game_id)
    out: dict[str, dict[str, Any]] = {}
    for row in team_rows:
        if str(row.get("game_id", "")) == gid:
            side = str(row.get("side", ""))
            out[side] = row
    return out


def _player_count_for_team(player_rows: list[dict[str, Any]], game_id: Any, team: str) -> int:
    gid = str(game_id)
    return sum(
        1
        for row in player_rows
        if str(row.get("game_id", "")) == gid and str(row.get("team", "")) == str(team)
    )


def _write_snapshot_artifacts(
    *,
    day_dir: Path,
    date_text: str,
    run_label: str,
    event_rows: list[dict[str, Any]],
    team_rows: list[dict[str, Any]],
    player_rows: list[dict[str, Any]],
    error_rows: list[dict[str, Any]],
) -> None:
    out_dir = day_dir / run_label
    suffix = f"{date_text}_{run_label}"
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
    _write_csv(out_dir / f"pregame_lineup_event_summary_{suffix}.csv", event_rows, event_fields)
    _write_csv(out_dir / f"pregame_lineup_game_team_summary_{suffix}.csv", team_rows, team_fields)
    _write_csv(out_dir / f"pregame_lineup_player_rows_{suffix}.csv", player_rows, player_fields)
    _write_csv(out_dir / f"pregame_lineup_source_errors_{suffix}.csv", error_rows, error_fields)


def _historical_state(timeline_rows: list[dict[str, str]]) -> dict[str, dict[str, Any]]:
    state: dict[str, dict[str, Any]] = {}
    for row in timeline_rows:
        gid = str(row.get("game_id", ""))
        if not gid:
            continue
        item = state.setdefault(
            gid,
            {
                "first_poll_timestamp": "",
                "first_confirmed_timestamp": "",
                "first_confirmed_minutes_before_first_pitch": "",
                "last_poll_timestamp": "",
                "poll_count": 0,
                "confirmed_full_seen": False,
                "stopped_reason": "",
            },
        )
        action = row.get("action_taken", "")
        capture_timestamp = row.get("capture_timestamp", "")
        if action in {"captured", "source_unavailable"}:
            item["poll_count"] += 1
            if not item["first_poll_timestamp"] and capture_timestamp:
                item["first_poll_timestamp"] = capture_timestamp
            if capture_timestamp:
                item["last_poll_timestamp"] = capture_timestamp
        both = str(row.get("both_teams_confirmed_full", "")).lower() == "true"
        if both:
            item["confirmed_full_seen"] = True
            if not item["first_confirmed_timestamp"] and capture_timestamp:
                item["first_confirmed_timestamp"] = capture_timestamp
                item["first_confirmed_minutes_before_first_pitch"] = row.get("minutes_before_first_pitch", "")
            if not item.get("stopped_reason"):
                item["stopped_reason"] = "stopped_confirmed"
        if action in {"stopped_confirmed", "stopped_started"}:
            item["stopped_reason"] = action
    return state


def _build_game_state(
    *,
    games: list[dict[str, Any]],
    timeline_rows: list[dict[str, str]],
    current_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    state = _historical_state(timeline_rows)
    for row in current_rows:
        gid = str(row.get("game_id", ""))
        if not gid:
            continue
        item = state.setdefault(
            gid,
            {
                "first_poll_timestamp": "",
                "first_confirmed_timestamp": "",
                "first_confirmed_minutes_before_first_pitch": "",
                "last_poll_timestamp": "",
                "poll_count": 0,
                "confirmed_full_seen": False,
                "stopped_reason": "",
            },
        )
        action = row.get("action_taken", "")
        ts = row.get("capture_timestamp", "")
        if action in {"captured", "source_unavailable"}:
            item["poll_count"] += 1
            if ts and not item["first_poll_timestamp"]:
                item["first_poll_timestamp"] = ts
            if ts:
                item["last_poll_timestamp"] = ts
        both = str(row.get("both_teams_confirmed_full", "")).lower() == "true"
        if both:
            item["confirmed_full_seen"] = True
            if ts and not item["first_confirmed_timestamp"]:
                item["first_confirmed_timestamp"] = ts
                item["first_confirmed_minutes_before_first_pitch"] = row.get("minutes_before_first_pitch", "")
            if not item.get("stopped_reason"):
                item["stopped_reason"] = "stopped_confirmed"
        if action in {"stopped_confirmed", "stopped_started"}:
            item["stopped_reason"] = action

    current_by_game = {str(row.get("game_id", "")): row for row in current_rows if row.get("game_id") != ""}
    out: list[dict[str, Any]] = []
    for game in games:
        gid = str(game.get("game_id") or "")
        item = state.get(gid, {})
        cur = current_by_game.get(gid, {})
        stopped_reason = item.get("stopped_reason", "")
        if not stopped_reason:
            action = cur.get("action_taken", "")
            if action in {"stopped_confirmed", "stopped_started"}:
                stopped_reason = action
        out.append(
            {
                "game_id": gid,
                "away_team": game.get("away_team") or "",
                "home_team": game.get("home_team") or "",
                "scheduled_first_pitch": game.get("game_start_time_utc") or "",
                "first_poll_timestamp": item.get("first_poll_timestamp", ""),
                "first_confirmed_timestamp": item.get("first_confirmed_timestamp", ""),
                "first_confirmed_minutes_before_first_pitch": item.get("first_confirmed_minutes_before_first_pitch", ""),
                "last_poll_timestamp": item.get("last_poll_timestamp", ""),
                "final_status": cur.get("game_status") or game.get("statsapi_game_status") or "",
                "poll_count": item.get("poll_count", 0),
                "confirmed_full_seen": bool(item.get("confirmed_full_seen", False)),
                "stopped_reason": stopped_reason,
            }
        )
    return out


def _render_summary(
    *,
    date_text: str,
    day_dir: Path,
    games: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    state_rows: list[dict[str, Any]],
) -> None:
    actions = Counter(str(row.get("action_taken", "")) for row in current_rows)
    confirmed_games = sum(1 for row in state_rows if row.get("confirmed_full_seen") is True)
    stopped_games = sum(1 for row in state_rows if row.get("stopped_reason"))
    pending_games = sum(1 for row in current_rows if row.get("action_taken") in {"pending", "skipped_not_in_window"})
    lines = [
        f"# Pregame Lineup Availability Day Summary - {date_text}",
        "",
        "- Mode: `dry_run_only`",
        "- Poll mode: `per_game`",
        "- Source: `MLB StatsAPI schedule + boxscore`",
        "- OddsAPI calls: `0`",
        "- DB writes: `0`",
        "- Schema changes: `0`",
        "- Production behavior changes: `0`",
        "",
        "## Current Run",
        "",
        f"- Games on slate: `{len(games)}`",
        f"- Captured games this run: `{actions.get('captured', 0)}`",
        f"- Pending/skipped-not-window games this run: `{pending_games}`",
        f"- Stopped confirmed this run: `{actions.get('stopped_confirmed', 0)}`",
        f"- Stopped started this run: `{actions.get('stopped_started', 0)}`",
        f"- Source unavailable this run: `{actions.get('source_unavailable', 0)}`",
        f"- Games with confirmed full seen historically: `{confirmed_games}`",
        f"- Games stopped historically: `{stopped_games}`",
        "",
        "## Per-Game Current Actions",
        "",
        "| game_id | matchup | first_pitch | minutes_before | status | away_lineup | home_lineup | both_confirmed | action | notes |",
        "|---:|---|---:|---:|---|---|---|---|---|---|",
    ]
    for row in current_rows:
        matchup = f"{row.get('away_team')} @ {row.get('home_team')}"
        lines.append(
            f"| `{row.get('game_id')}` | `{matchup}` | `{row.get('scheduled_first_pitch')}` | `{row.get('minutes_before_first_pitch')}` | `{row.get('game_status')}` | `{row.get('away_lineup_status')}` | `{row.get('home_lineup_status')}` | `{row.get('both_teams_confirmed_full')}` | `{row.get('action_taken')}` | `{row.get('notes')}` |"
        )
    lines.extend(
        [
            "",
            "## Game State",
            "",
            "| game_id | matchup | first_poll | first_confirmed | first_confirmed_minutes_before | last_poll | final_status | poll_count | confirmed_seen | stopped_reason |",
            "|---:|---|---:|---:|---:|---:|---|---:|---|---|",
        ]
    )
    for row in state_rows:
        matchup = f"{row.get('away_team')} @ {row.get('home_team')}"
        lines.append(
            f"| `{row.get('game_id')}` | `{matchup}` | `{row.get('first_poll_timestamp')}` | `{row.get('first_confirmed_timestamp')}` | `{row.get('first_confirmed_minutes_before_first_pitch')}` | `{row.get('last_poll_timestamp')}` | `{row.get('final_status')}` | `{row.get('poll_count')}` | `{row.get('confirmed_full_seen')}` | `{row.get('stopped_reason')}` |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "Version 2 polls each game independently. A game is captured only inside its configured pregame window, stops once both teams are confirmed full, and stops once the game leaves pregame/warmup status.",
        ]
    )
    (day_dir / f"lineup_availability_day_summary_{date_text}.md").write_text("\n".join(lines) + "\n")


def run_per_game(args: argparse.Namespace) -> int:
    now = _now_utc()
    capture_timestamp = now.isoformat().replace("+00:00", "Z")
    run_label = _run_label(now)
    day_dir = Path(args.output_root) / args.date
    day_dir.mkdir(parents=True, exist_ok=True)
    stop_confirmed = _bool_arg(args.stop_at_confirmed)
    stop_started = _bool_arg(args.stop_at_game_start)

    games, schedule_error = snapshot._schedule_games(args.date, args.statsapi_timeout_seconds)
    timeline_path = day_dir / "pregame_lineup_per_game_timeline.csv"
    previous_timeline = _read_csv(timeline_path)
    previous_state = _historical_state(previous_timeline)

    event_rows: list[dict[str, Any]] = []
    team_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    error_rows: list[dict[str, Any]] = []
    current_rows: list[dict[str, Any]] = []

    if schedule_error:
        current_rows.append(
            {
                "capture_timestamp": capture_timestamp,
                "game_id": "",
                "game_date": args.date,
                "away_team": "",
                "home_team": "",
                "scheduled_first_pitch": "",
                "minutes_before_first_pitch": "",
                "game_status": "",
                "away_lineup_status": "source_unavailable",
                "home_lineup_status": "source_unavailable",
                "both_teams_confirmed_full": False,
                "away_player_rows": 0,
                "home_player_rows": 0,
                "action_taken": "source_unavailable",
                "notes": f"schedule_error:{schedule_error}",
            }
        )

    for game in games:
        game_id = game.get("game_id")
        gid = str(game_id or "")
        start_dt = snapshot._iso_to_datetime(game.get("game_start_time_utc") or "")
        minutes_before = ""
        if start_dt is not None:
            minutes_before = round((start_dt - now).total_seconds() / 60.0, 2)
        status = str(game.get("statsapi_game_status") or "")
        prior = previous_state.get(gid, {})
        action = "pending"
        notes = ""
        away_status = ""
        home_status = ""
        away_rows = 0
        home_rows = 0
        both_confirmed = False

        if stop_confirmed and prior.get("confirmed_full_seen"):
            action = "stopped_confirmed"
            notes = "both_teams_confirmed_full_seen_in_prior_snapshot"
        elif stop_started and status not in PREGAME_STATUSES:
            action = "stopped_started"
            notes = f"game_status={status}"
        elif start_dt is None:
            action = "pending"
            notes = "missing_scheduled_first_pitch"
        elif isinstance(minutes_before, float) and minutes_before > float(args.start_poll_minutes_before):
            action = "skipped_not_in_window"
            notes = f"outside_{args.start_poll_minutes_before:g}_minute_window"
        elif isinstance(minutes_before, float) and minutes_before < 0:
            action = "stopped_started" if stop_started else "pending"
            notes = "past_scheduled_first_pitch"
        else:
            source_url = snapshot._boxscore_url(int(game_id)) if game_id else ""
            fetched = snapshot._fetch_json(source_url, args.statsapi_timeout_seconds) if game_id else snapshot.FetchResult({}, "missing_game_id", "")
            snapshot_id = f"{run_label}_{game_id}"
            event_rows.append(
                {
                    "capture_run_id": run_label,
                    "snapshot_id": snapshot_id,
                    "snapshot_label": run_label,
                    "game_id": game_id or "",
                    "game_date": game.get("game_date") or args.date,
                    "game_start_time_utc": game.get("game_start_time_utc") or "",
                    "source_fetched_at_utc": capture_timestamp,
                    "offset_to_first_pitch_minutes": minutes_before,
                    "statsapi_game_status": status,
                    "abstract_game_state": game.get("abstract_game_state") or "",
                    "home_team": game.get("home_team") or "",
                    "away_team": game.get("away_team") or "",
                    "source": "statsapi_boxscore",
                    "source_url": source_url,
                    "source_payload_hash": fetched.payload_hash,
                    "fetch_status": "ok" if not fetched.error else "source_unavailable",
                    "fetch_error": fetched.error,
                }
            )
            if fetched.error:
                action = "source_unavailable"
                notes = fetched.error
                error_rows.append(
                    {
                        "capture_run_id": run_label,
                        "snapshot_label": run_label,
                        "game_id": game_id or "",
                        "source": "statsapi_boxscore",
                        "source_url": source_url,
                        "error": fetched.error,
                        "source_fetched_at_utc": capture_timestamp,
                    }
                )
                away_status = "source_unavailable"
                home_status = "source_unavailable"
            else:
                for side in ("away", "home"):
                    team_summary, rows = snapshot._players_for_team(
                        game=game,
                        boxscore=fetched.payload,
                        side=side,
                        snapshot_id=snapshot_id,
                        capture_run_id=run_label,
                        source_url=source_url,
                        source_fetched_at_utc=capture_timestamp,
                        offset_minutes=float(minutes_before) if isinstance(minutes_before, float) else None,
                    )
                    team_rows.append(team_summary)
                    player_rows.extend(rows)
                status_by_side = _team_status_for_game(team_rows, game_id)
                away_status = str(status_by_side.get("away", {}).get("lineup_status", "missing"))
                home_status = str(status_by_side.get("home", {}).get("lineup_status", "missing"))
                away_team = str(status_by_side.get("away", {}).get("team", game.get("away_team") or ""))
                home_team = str(status_by_side.get("home", {}).get("team", game.get("home_team") or ""))
                away_rows = _player_count_for_team(player_rows, game_id, away_team)
                home_rows = _player_count_for_team(player_rows, game_id, home_team)
                both_confirmed = away_status == "confirmed_full" and home_status == "confirmed_full"
                action = "captured"
                notes = "both_teams_confirmed_full" if both_confirmed else "captured_in_poll_window"

        current_rows.append(
            {
                "capture_timestamp": capture_timestamp,
                "game_id": game_id or "",
                "game_date": game.get("game_date") or args.date,
                "away_team": game.get("away_team") or "",
                "home_team": game.get("home_team") or "",
                "scheduled_first_pitch": game.get("game_start_time_utc") or "",
                "minutes_before_first_pitch": minutes_before,
                "game_status": status,
                "away_lineup_status": away_status,
                "home_lineup_status": home_status,
                "both_teams_confirmed_full": bool(both_confirmed),
                "away_player_rows": away_rows,
                "home_player_rows": home_rows,
                "action_taken": action,
                "notes": notes,
            }
        )

    if event_rows or team_rows or player_rows or error_rows:
        _write_snapshot_artifacts(
            day_dir=day_dir,
            date_text=args.date,
            run_label=run_label,
            event_rows=event_rows,
            team_rows=team_rows,
            player_rows=player_rows,
            error_rows=error_rows,
        )

    timeline_fields = [
        "capture_timestamp",
        "game_id",
        "game_date",
        "away_team",
        "home_team",
        "scheduled_first_pitch",
        "minutes_before_first_pitch",
        "game_status",
        "away_lineup_status",
        "home_lineup_status",
        "both_teams_confirmed_full",
        "away_player_rows",
        "home_player_rows",
        "action_taken",
        "notes",
    ]
    _append_csv(timeline_path, current_rows, timeline_fields)
    all_timeline = previous_timeline + [{k: str(v) for k, v in row.items()} for row in current_rows]

    state_rows = _build_game_state(games=games, timeline_rows=all_timeline, current_rows=[])
    state_fields = [
        "game_id",
        "away_team",
        "home_team",
        "scheduled_first_pitch",
        "first_poll_timestamp",
        "first_confirmed_timestamp",
        "first_confirmed_minutes_before_first_pitch",
        "last_poll_timestamp",
        "final_status",
        "poll_count",
        "confirmed_full_seen",
        "stopped_reason",
    ]
    _write_csv(day_dir / "pregame_lineup_game_state.csv", state_rows, state_fields)
    _render_summary(date_text=args.date, day_dir=day_dir, games=games, current_rows=current_rows, state_rows=state_rows)

    actions = Counter(str(row.get("action_taken", "")) for row in current_rows)
    print(f"date={args.date}")
    print(f"poll_mode=per_game")
    print(f"games={len(games)}")
    print(f"captured={actions.get('captured', 0)}")
    print(f"pending={actions.get('pending', 0)}")
    print(f"skipped_not_in_window={actions.get('skipped_not_in_window', 0)}")
    print(f"stopped_confirmed={actions.get('stopped_confirmed', 0)}")
    print(f"stopped_started={actions.get('stopped_started', 0)}")
    print(f"source_unavailable={actions.get('source_unavailable', 0)}")
    print(f"timeline={timeline_path}")
    print(f"game_state={day_dir / 'pregame_lineup_game_state.csv'}")
    print(f"summary={day_dir / f'lineup_availability_day_summary_{args.date}.md'}")
    print("oddsapi_calls=0")
    print("db_writes=0")
    return 0


def main() -> int:
    args = _parse_args()
    if args.poll_mode != "per_game":
        print("slate_window mode is deprecated; use --poll-mode per_game", flush=True)
        return run_per_game(args)
    return run_per_game(args)


if __name__ == "__main__":
    raise SystemExit(main())
