#!/usr/bin/env python3
"""Dry-run starter-only batters-faced write gate.

This utility emits row-level manifests for official MLB StatsAPI starter BF
without writing to the database. It is intentionally conservative: the script
uses StatsAPI's official pitching.battersFaced field only, reconciles starter
rows against local player_stats and pitcher_game_v4_base, and keeps all future
write surfaces as manifest files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import URLError
from urllib.request import urlopen

from backend.shared.db.pg import pg_connect

STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"
SOURCE_NAME = "statsapi_boxscore"
SOURCE_PRIORITY = 1
DEFAULT_TIMEOUT_SECONDS = 30

CORE_FIELDS = (
    "outs_recorded",
    "hits_allowed",
    "walks_allowed",
    "strikeouts",
    "earned_runs",
)


@dataclass
class Candidate:
    source: str
    source_priority: int
    source_url: str
    source_run_at: str
    backfill_run_id: str
    game_date: str
    game_id: int | None
    statsapi_official_date: str
    game_type: str
    game_status: str
    team: str
    opponent: str
    is_home: bool
    pitcher_mlbam_id: int | None
    pitcher_name: str
    pitching_order_index: int
    statsapi_is_starter: bool
    innings_pitched: str
    outs_recorded: int | None
    strikeouts: int | None
    walks_allowed: int | None
    hits_allowed: int | None
    earned_runs: int | None
    runs_allowed: int | None
    home_runs_allowed: int | None
    batters_faced: int | None
    bf_source_field: str
    candidate_status: str = "candidate"
    validation_status: str = ""
    warning_code: str = ""
    reject_reason: str = ""
    conflict_reason: str = ""
    skip_reason: str = ""
    validation_notes: str = ""
    local_player_stats_date: str = ""
    player_stats_join_count: int = 0
    player_stats_position: str = ""
    player_stats_is_starter: str = ""
    v4_join_count: int = 0
    manifest_status: str = ""

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run official StatsAPI starter BF write-gate manifests."
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--source",
        default=SOURCE_NAME,
        choices=[SOURCE_NAME, "mlb_statsapi_boxscore"],
        help="Official BF source. Alias mlb_statsapi_boxscore is accepted for prior pilot naming.",
    )
    parser.add_argument("--backfill-run-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument(
        "--statsapi-timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--dedupe-schedule-dates",
        action="store_true",
        help="Dry-run simulation: skip exact alternate schedule-date duplicates by deterministic policy.",
    )
    parser.add_argument(
        "--filename-tag",
        default="",
        help="Optional tag inserted before the date suffix in output filenames, e.g. dedupe_sim.",
    )
    return parser.parse_args()


def _date_range(start: str, end: str) -> Iterable[date]:
    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    if end_d < start_d:
        raise SystemExit("--end-date must be >= --start-date")
    cur = start_d
    while cur <= end_d:
        yield cur
        cur += timedelta(days=1)


def _fetch_json(url: str, timeout_seconds: int) -> dict[str, Any]:
    try:
        with urlopen(url, timeout=timeout_seconds) as resp:  # nosec B310 - public MLB StatsAPI.
            payload = resp.read()
    except URLError as exc:
        raise RuntimeError(f"StatsAPI fetch failed for {url}: {exc}") from exc
    return json.loads(payload.decode("utf-8"))


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(str(value)))
        except Exception:
            return None


def _outs_from_pitching(pitching: dict[str, Any]) -> int | None:
    outs = _int_or_none(pitching.get("outs"))
    if outs is not None:
        return outs
    ip = pitching.get("inningsPitched")
    if ip is None or ip == "":
        return None
    text = str(ip)
    if "." not in text:
        return (_int_or_none(text) or 0) * 3
    whole, frac = text.split(".", 1)
    return (_int_or_none(whole) or 0) * 3 + (_int_or_none(frac[:1]) or 0)


def _team_abbrev(team_payload: dict[str, Any]) -> str:
    team = team_payload.get("team") or {}
    return str(
        team.get("abbreviation")
        or team.get("abbrev")
        or team.get("teamCode")
        or team.get("fileCode")
        or ""
    )


def _is_statsapi_starter(player_payload: dict[str, Any], pitching_order_index: int) -> bool:
    # In StatsAPI boxscore pitching payloads, gamesStarted can reflect broader
    # season/context stats rather than the single-game role. The game starter is
    # represented by the first pitcher in that team's pitching order and is then
    # independently verified against local player_stats.is_starter.
    return pitching_order_index == 0


def _source_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]


def fetch_statsapi_starter_candidates(
    start: str,
    end: str,
    source_run_at: str,
    backfill_run_id: str,
    timeout_seconds: int,
) -> tuple[list[Candidate], list[dict[str, Any]]]:
    candidates: list[Candidate] = []
    source_rows: list[dict[str, Any]] = []
    for game_date in _date_range(start, end):
        schedule_url = f"{STATSAPI_BASE}/schedule?sportId=1&date={game_date.isoformat()}"
        schedule = _fetch_json(schedule_url, timeout_seconds)
        games = []
        for day in schedule.get("dates") or []:
            games.extend(day.get("games") or [])
        source_rows.append(
            {
                "source_url": schedule_url,
                "source_kind": "schedule",
                "game_date": game_date.isoformat(),
                "fetched_at": source_run_at,
                "payload_hash": _source_hash(json.dumps(schedule, sort_keys=True)),
                "games": len(games),
            }
        )
        for game in games:
            game_id = _int_or_none(game.get("gamePk"))
            if game_id is None:
                continue
            status = (game.get("status") or {}).get("abstractGameState") or ""
            detailed_status = (game.get("status") or {}).get("detailedState") or ""
            official_date = str(game.get("officialDate") or game_date.isoformat())
            game_type = str(game.get("gameType") or "")
            teams = game.get("teams") or {}
            home_team = _team_abbrev(teams.get("home") or {})
            away_team = _team_abbrev(teams.get("away") or {})
            boxscore_url = f"{STATSAPI_BASE}/game/{game_id}/boxscore"
            boxscore = _fetch_json(boxscore_url, timeout_seconds)
            boxscore_teams = boxscore.get("teams") or {}
            box_away_team = _team_abbrev(boxscore_teams.get("away") or {}) or away_team
            box_home_team = _team_abbrev(boxscore_teams.get("home") or {}) or home_team
            source_rows.append(
                {
                    "source_url": boxscore_url,
                    "source_kind": "boxscore",
                    "game_date": game_date.isoformat(),
                    "fetched_at": source_run_at,
                    "payload_hash": _source_hash(json.dumps(boxscore, sort_keys=True)),
                    "games": "",
                }
            )
            for side in ("away", "home"):
                team_payload = (boxscore_teams.get(side) or {})
                players = team_payload.get("players") or {}
                team = box_away_team if side == "away" else box_home_team
                opponent = box_home_team if side == "away" else box_away_team
                pitcher_rows: list[tuple[int, str, dict[str, Any]]] = []
                pitcher_order = [_int_or_none(pid) for pid in (team_payload.get("pitchers") or [])]
                if pitcher_order:
                    for pid in pitcher_order:
                        if pid is None:
                            continue
                        key = f"ID{pid}"
                        player = players.get(key)
                        if not player:
                            continue
                        pitching = (player.get("stats") or {}).get("pitching") or {}
                        if pitching:
                            pitcher_rows.append((pid, key, player))
                else:
                    for key, player in players.items():
                        pitching = (player.get("stats") or {}).get("pitching") or {}
                        if not pitching:
                            continue
                        person = player.get("person") or {}
                        pid = _int_or_none(person.get("id"))
                        if pid is None:
                            continue
                        pitcher_rows.append((pid, key, player))
                for idx, (_pid, _key, player) in enumerate(pitcher_rows):
                    if not _is_statsapi_starter(player, idx):
                        continue
                    pitching = (player.get("stats") or {}).get("pitching") or {}
                    person = player.get("person") or {}
                    candidates.append(
                        Candidate(
                            source=SOURCE_NAME,
                            source_priority=SOURCE_PRIORITY,
                            source_url=boxscore_url,
                            source_run_at=source_run_at,
                            backfill_run_id=backfill_run_id,
                            game_date=game_date.isoformat(),
                            game_id=game_id,
                            statsapi_official_date=official_date,
                            game_type=game_type,
                            game_status=detailed_status or status,
                            team=team,
                            opponent=opponent,
                            is_home=side == "home",
                            pitcher_mlbam_id=_int_or_none(person.get("id")),
                            pitcher_name=str(person.get("fullName") or ""),
                            pitching_order_index=idx,
                            statsapi_is_starter=True,
                            innings_pitched=str(pitching.get("inningsPitched") or ""),
                            outs_recorded=_outs_from_pitching(pitching),
                            strikeouts=_int_or_none(pitching.get("strikeOuts")),
                            walks_allowed=_int_or_none(pitching.get("baseOnBalls")),
                            hits_allowed=_int_or_none(pitching.get("hits")),
                            earned_runs=_int_or_none(pitching.get("earnedRuns")),
                            runs_allowed=_int_or_none(pitching.get("runs")),
                            home_runs_allowed=_int_or_none(pitching.get("homeRuns")),
                            batters_faced=_int_or_none(pitching.get("battersFaced")),
                            bf_source_field="pitching.battersFaced",
                        )
                    )
    return candidates, source_rows


def _fetch_local_rows(start: str, end: str) -> tuple[dict[tuple[int, int], list[dict[str, Any]]], dict[tuple[int, int], list[dict[str, Any]]]]:
    player_stats_sql = """
        SELECT
            game_id::bigint AS game_id,
            game_date::text AS game_date,
            player_id::bigint AS player_id,
            COALESCE(team, '') AS team,
            COALESCE(opponent, '') AS opponent,
            COALESCE(position, '') AS position,
            COALESCE(is_starter, 0)::int AS is_starter,
            outs_recorded,
            hits_allowed,
            walks_allowed,
            strikeouts_pitching,
            earned_runs
        FROM mlb.player_stats
        WHERE game_date >= %s::date
          AND game_date <= %s::date
          AND outs_recorded IS NOT NULL
    """
    v4_sql = """
        SELECT
            game_id::bigint AS game_id,
            game_date::text AS game_date,
            player_id::bigint AS player_id,
            COALESCE(team, '') AS team,
            COALESCE(opponent, '') AS opponent,
            COALESCE(position, '') AS position,
            COALESCE(is_starter, 0)::int AS is_starter,
            outs_recorded,
            hits_allowed,
            walks_allowed,
            strikeouts_pitching,
            earned_runs
        FROM mlb.pitcher_game_v4_base
        WHERE game_date >= %s::date
          AND game_date <= %s::date
    """
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(player_stats_sql, (start, end))
        ps_rows = list(cur.fetchall() or [])
        cur.execute(v4_sql, (start, end))
        v4_rows = list(cur.fetchall() or [])
    return _index_rows(ps_rows), _index_rows(v4_rows)


def _index_rows(rows: Iterable[dict[str, Any]]) -> dict[tuple[int, int], list[dict[str, Any]]]:
    out: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in rows:
        game_id = _int_or_none(row.get("game_id"))
        player_id = _int_or_none(row.get("player_id"))
        if game_id is None or player_id is None:
            continue
        out.setdefault((game_id, player_id), []).append(dict(row))
    return out


def _same_int(a: Any, b: Any) -> bool:
    ai = _int_or_none(a)
    bi = _int_or_none(b)
    return ai == bi


def _core_stats_match(candidate: Candidate, row: dict[str, Any]) -> tuple[bool, list[str]]:
    mismatches: list[str] = []
    checks = {
        "outs_recorded": (candidate.outs_recorded, row.get("outs_recorded")),
        "hits_allowed": (candidate.hits_allowed, row.get("hits_allowed")),
        "walks_allowed": (candidate.walks_allowed, row.get("walks_allowed")),
        "strikeouts": (candidate.strikeouts, row.get("strikeouts_pitching")),
        "earned_runs": (candidate.earned_runs, row.get("earned_runs")),
    }
    for name, (left, right) in checks.items():
        if left is None or right is None:
            continue
        if not _same_int(left, right):
            mismatches.append(f"{name}:statsapi={left}:local={right}")
    return len(mismatches) == 0, mismatches


def _bf_plausible(candidate: Candidate) -> tuple[bool, str]:
    bf = candidate.batters_faced
    if bf is None:
        return False, "missing_official_bf"
    if candidate.outs_recorded is not None and bf < candidate.outs_recorded / 3.0:
        return False, "bf_less_than_innings_floor"
    offensive_events = 0
    for value in (candidate.hits_allowed, candidate.walks_allowed):
        offensive_events += _int_or_none(value) or 0
    if bf < offensive_events:
        return False, "bf_less_than_hits_plus_walks"
    return True, ""


def classify_candidates(
    candidates: list[Candidate],
    player_stats_index: dict[tuple[int, int], list[dict[str, Any]]],
    v4_index: dict[tuple[int, int], list[dict[str, Any]]],
) -> dict[str, list[Candidate]]:
    manifests = {
        "accepted": [],
        "warning_accepted": [],
        "rejected": [],
        "conflict": [],
        "skipped_existing": [],
        "skipped_duplicate": [],
    }
    seen_keys: set[tuple[int | None, int | None]] = set()
    for c in candidates:
        key = (c.game_id, c.pitcher_mlbam_id)
        if key in seen_keys:
            c.validation_status = "conflict"
            c.conflict_reason = "duplicate_candidate_key"
            c.manifest_status = "conflict"
            manifests["conflict"].append(c)
            continue
        seen_keys.add(key)

        if c.game_id is None or c.pitcher_mlbam_id is None:
            _reject(c, "missing_game_id_or_pitcher_mlbam_id", manifests)
            continue
        if c.batters_faced is None:
            _reject(c, "missing_official_statsapi_batters_faced", manifests)
            continue
        if not c.statsapi_is_starter:
            _reject(c, "not_statsapi_starter", manifests)
            continue
        plausible, plausibility_reason = _bf_plausible(c)
        if not plausible:
            _reject(c, plausibility_reason, manifests)
            continue

        ps_rows = player_stats_index.get((c.game_id, c.pitcher_mlbam_id), [])
        c.player_stats_join_count = len(ps_rows)
        if len(ps_rows) != 1:
            _reject(c, f"player_stats_join_count_{len(ps_rows)}", manifests)
            continue
        ps = ps_rows[0]
        c.local_player_stats_date = str(ps.get("game_date") or "")
        c.player_stats_position = str(ps.get("position") or "")
        c.player_stats_is_starter = str(ps.get("is_starter") or 0)
        if _int_or_none(ps.get("is_starter")) != 1:
            _reject(c, "player_stats_not_starter", manifests)
            continue
        ps_match, ps_mismatches = _core_stats_match(c, ps)
        if not ps_match:
            _reject(c, "player_stats_core_mismatch:" + ";".join(ps_mismatches), manifests)
            continue

        v4_rows = v4_index.get((c.game_id, c.pitcher_mlbam_id), [])
        c.v4_join_count = len(v4_rows)
        if len(v4_rows) > 1:
            c.validation_status = "conflict"
            c.conflict_reason = f"v4_join_count_{len(v4_rows)}"
            c.manifest_status = "conflict"
            manifests["conflict"].append(c)
            continue
        if len(v4_rows) == 1:
            v4_match, v4_mismatches = _core_stats_match(c, v4_rows[0])
            if not v4_match:
                _reject(c, "v4_core_mismatch:" + ";".join(v4_mismatches), manifests)
                continue
            c.validation_status = "accepted"
            c.manifest_status = "accepted"
            c.validation_notes = "clean_accept"
            manifests["accepted"].append(c)
            continue

        if (c.player_stats_position or "").upper() != "P":
            c.validation_status = "warning_accepted"
            c.warning_code = "two_way_position_filter_gap"
            c.manifest_status = "warning_accepted"
            c.validation_notes = "v4_missing_but_player_stats_position_non_p_starter_core_stats_match"
            manifests["warning_accepted"].append(c)
            continue

        _reject(c, "v4_missing_unexplained", manifests)
    return manifests


def _reject(c: Candidate, reason: str, manifests: dict[str, list[Candidate]]) -> None:
    c.validation_status = "rejected"
    c.reject_reason = reason
    c.manifest_status = "rejected"
    c.validation_notes = reason
    manifests["rejected"].append(c)


def _duplicate_comparison_key(c: Candidate) -> tuple[Any, ...]:
    return (
        c.team,
        c.opponent,
        c.is_home,
        c.pitcher_mlbam_id,
        c.pitcher_name,
        c.pitching_order_index,
        c.statsapi_is_starter,
        c.innings_pitched,
        c.outs_recorded,
        c.strikeouts,
        c.walks_allowed,
        c.hits_allowed,
        c.earned_runs,
        c.runs_allowed,
        c.home_runs_allowed,
        c.batters_faced,
        c.bf_source_field,
    )


def apply_schedule_date_dedupe(
    candidates: list[Candidate],
    player_stats_index: dict[tuple[int, int], list[dict[str, Any]]],
) -> tuple[list[Candidate], list[Candidate]]:
    """Skip exact alternate schedule-date duplicates without hiding real conflicts."""
    grouped: dict[tuple[int | None, int | None], list[Candidate]] = {}
    for c in candidates:
        grouped.setdefault((c.game_id, c.pitcher_mlbam_id), []).append(c)

    kept: list[Candidate] = []
    skipped: list[Candidate] = []
    for key, rows in grouped.items():
        if len(rows) == 1:
            kept.extend(rows)
            continue
        comparison_keys = {_duplicate_comparison_key(c) for c in rows}
        if len(comparison_keys) != 1:
            # Keep all rows so the normal duplicate detector preserves the conflict.
            kept.extend(rows)
            continue

        ps_rows = player_stats_index.get((key[0], key[1]), []) if key[0] is not None and key[1] is not None else []
        local_date = str(ps_rows[0].get("game_date") or "") if len(ps_rows) == 1 else ""

        def rank(c: Candidate) -> tuple[int, str]:
            if c.statsapi_official_date and c.game_date == c.statsapi_official_date:
                return (0, c.game_date)
            if local_date and c.game_date == local_date:
                return (1, c.game_date)
            return (2, c.game_date)

        canonical = sorted(rows, key=rank)[0]
        canonical.validation_notes = (
            f"canonical_schedule_date_selected_from_{len(rows)}_exact_duplicates"
        )
        kept.append(canonical)
        for c in rows:
            if c is canonical:
                continue
            c.validation_status = "skipped_duplicate"
            c.manifest_status = "skipped_duplicate"
            c.skip_reason = "duplicate_schedule_date_alternate"
            c.local_player_stats_date = local_date
            c.validation_notes = (
                "exact alternate schedule-date duplicate skipped; "
                f"canonical_game_date={canonical.game_date}; "
                f"statsapi_official_date={c.statsapi_official_date}; "
                f"local_player_stats_date={local_date}"
            )
            skipped.append(c)
    return kept, skipped


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _candidate_fields() -> list[str]:
    return list(Candidate(
        source="", source_priority=0, source_url="", source_run_at="", backfill_run_id="",
        game_date="", game_id=None, statsapi_official_date="", game_type="", game_status="", team="", opponent="",
        is_home=False, pitcher_mlbam_id=None, pitcher_name="", pitching_order_index=0,
        statsapi_is_starter=False, innings_pitched="", outs_recorded=None, strikeouts=None,
        walks_allowed=None, hits_allowed=None, earned_runs=None, runs_allowed=None,
        home_runs_allowed=None, batters_faced=None, bf_source_field=""
    ).as_dict().keys())


def write_outputs(
    output_dir: Path,
    start: str,
    end: str,
    candidates: list[Candidate],
    manifests: dict[str, list[Candidate]],
    source_rows: list[dict[str, Any]],
    filename_tag: str = "",
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"{start}_to_{end}"
    tagged_suffix = f"{filename_tag}_{suffix}" if filename_tag else suffix
    fields = _candidate_fields()
    _write_csv(
        output_dir / f"starter_bf_candidate_rows_{tagged_suffix}.csv",
        [c.as_dict() for c in candidates],
        fields,
    )
    for key, filename in [
        ("accepted", "starter_bf_accepted_rows"),
        ("warning_accepted", "starter_bf_warning_accepted_rows"),
        ("rejected", "starter_bf_rejected_rows"),
        ("conflict", "starter_bf_conflict_rows"),
        ("skipped_existing", "starter_bf_skipped_existing_rows"),
        ("skipped_duplicate", "starter_bf_skipped_duplicate_rows"),
    ]:
        _write_csv(
            output_dir / f"{filename}_{tagged_suffix}.csv",
            [c.as_dict() for c in manifests[key]],
            fields,
        )
    manifest_rows: list[dict[str, Any]] = []
    for status, rows in manifests.items():
        for c in rows:
            row = c.as_dict()
            row["write_action"] = "no_write_dry_run"
            row["would_write"] = "yes" if status in {"accepted", "warning_accepted"} else "no"
            manifest_rows.append(row)
    _write_csv(output_dir / f"starter_bf_write_manifest_{tagged_suffix}.csv", manifest_rows, fields + ["write_action", "would_write"])

    summary = build_validation_summary(start, end, candidates, manifests)
    _write_csv(
        output_dir / f"starter_bf_validation_summary_{tagged_suffix}.csv",
        summary,
        [
            "category",
            "check",
            "population",
            "total",
            "passed",
            "failed",
            "warn",
            "status",
            "notes",
        ],
    )
    _write_csv(
        output_dir / f"starter_bf_source_payloads_{tagged_suffix}.csv",
        source_rows,
        ["source_url", "source_kind", "game_date", "fetched_at", "payload_hash", "games"],
    )
    post_write_rows = [
        {
            "check": "post_write_verification",
            "status": "not_applicable_dry_run",
            "expected_written_rows": len(manifests["accepted"]) + len(manifests["warning_accepted"]),
            "actual_written_rows": 0,
            "notes": "Dry-run only. No database writes attempted.",
        }
    ]
    _write_csv(
        output_dir / f"starter_bf_post_write_verification_placeholder_{tagged_suffix}.csv",
        post_write_rows,
        ["check", "status", "expected_written_rows", "actual_written_rows", "notes"],
    )
    (output_dir / f"starter_bf_validation_summary_{tagged_suffix}.json").write_text(
        json.dumps({"summary": summary, "counts": manifest_counts(manifests)}, indent=2) + "\n",
        encoding="utf-8",
    )


def manifest_counts(manifests: dict[str, list[Candidate]]) -> dict[str, int]:
    return {key: len(value) for key, value in manifests.items()}


def build_validation_summary(
    start: str,
    end: str,
    candidates: list[Candidate],
    manifests: dict[str, list[Candidate]],
) -> list[dict[str, Any]]:
    accepted = len(manifests["accepted"])
    warning = len(manifests["warning_accepted"])
    rejected = len(manifests["rejected"])
    conflict = len(manifests["conflict"])
    skipped = len(manifests["skipped_existing"])
    skipped_duplicate = len(manifests["skipped_duplicate"])
    total = len(candidates)
    unique_status_rows = accepted + warning + rejected + conflict + skipped + skipped_duplicate
    hard_failures = rejected + conflict
    return [
        {
            "category": "source",
            "check": "official_statsapi_bf",
            "population": "starter_candidates",
            "total": total,
            "passed": sum(1 for c in candidates if c.batters_faced is not None),
            "failed": sum(1 for c in candidates if c.batters_faced is None),
            "warn": 0,
            "status": "pass" if all(c.batters_faced is not None for c in candidates) else "fail",
            "notes": "Uses StatsAPI pitching.battersFaced only; no aggregate BF approximation.",
        },
        {
            "category": "manifest",
            "check": "mutually_exclusive_statuses",
            "population": "starter_candidates",
            "total": total,
            "passed": unique_status_rows,
            "failed": 0 if unique_status_rows == total else abs(total - unique_status_rows),
            "warn": 0,
            "status": "pass" if unique_status_rows == total else "fail",
            "notes": "Each candidate row must appear in exactly one terminal manifest.",
        },
        {
            "category": "acceptance",
            "check": "clean_accepted",
            "population": "starter_candidates",
            "total": total,
            "passed": accepted,
            "failed": hard_failures,
            "warn": warning + skipped_duplicate,
            "status": "warn" if warning and not hard_failures else ("pass" if not hard_failures else "fail"),
            "notes": f"Pilot window {start} through {end}; warning accepted rows require documented guardrail.",
        },
        {
            "category": "dedupe",
            "check": "duplicate_schedule_date_skips",
            "population": "starter_candidates",
            "total": total,
            "passed": skipped_duplicate,
            "failed": conflict,
            "warn": skipped_duplicate,
            "status": "fail" if conflict else ("warn" if skipped_duplicate else "pass"),
            "notes": "Exact alternate schedule-date duplicates are skipped/audited in dedupe simulation.",
        },
        {
            "category": "write",
            "check": "database_write",
            "population": "starter_candidates",
            "total": total,
            "passed": 0,
            "failed": 0,
            "warn": 0,
            "status": "not_applicable_dry_run",
            "notes": "No database writes are attempted by this utility.",
        },
    ]


def main() -> int:
    args = _parse_args()
    output_dir = Path(args.output_dir)
    source_run_at = datetime.now(timezone.utc).isoformat()
    candidates, source_rows = fetch_statsapi_starter_candidates(
        args.start_date,
        args.end_date,
        source_run_at,
        args.backfill_run_id,
        args.statsapi_timeout_seconds,
    )
    player_stats_index, v4_index = _fetch_local_rows(args.start_date, args.end_date)
    classify_input = candidates
    skipped_duplicates: list[Candidate] = []
    if args.dedupe_schedule_dates:
        classify_input, skipped_duplicates = apply_schedule_date_dedupe(candidates, player_stats_index)
    manifests = classify_candidates(classify_input, player_stats_index, v4_index)
    manifests["skipped_duplicate"].extend(skipped_duplicates)
    write_outputs(
        output_dir,
        args.start_date,
        args.end_date,
        candidates,
        manifests,
        source_rows,
        filename_tag=args.filename_tag,
    )
    counts = manifest_counts(manifests)
    print(
        "starter BF dry-run complete: "
        f"candidates={len(candidates)} "
        f"accepted={counts['accepted']} "
        f"warning_accepted={counts['warning_accepted']} "
        f"rejected={counts['rejected']} "
        f"conflict={counts['conflict']} "
        f"skipped_existing={counts['skipped_existing']} "
        f"skipped_duplicate={counts['skipped_duplicate']} "
        "db_writes=0"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
