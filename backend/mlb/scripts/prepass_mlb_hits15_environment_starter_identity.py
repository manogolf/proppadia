#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_REVIEW_AIDS_DIR = Path("artifacts/analysis/mlb/review_aids")
DEFAULT_PERFORMANCE_DIR = Path("artifacts/analysis/mlb/review_aids/performance")
DEFAULT_EXPANDED_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
DEFAULT_ODDS_HISTORY_DIR = Path("backend/mlb/exports/odds_history")
DEFAULT_ALT_HISTORY_DIR = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_ALT_LIVE_DIR = Path("artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/review_aids")

BOARD_PATTERNS = [
    "hits_o15_simple_filter_*.csv",
    "hits_o15_watch_candidates_*.csv",
    "hits_o15_layered_candidates_*.csv",
    "hits_u15_favorite_audit_*.csv",
    "hits_o15_alternate_discovery_*.csv",
]
PITCHER_PROP_TYPES = {
    "hits_allowed",
    "strikeouts_pitching",
    "outs_recorded",
    "walks_allowed",
    "earned_runs",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _nonblank(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text and text.lower() not in {"nan", "none", "null"})


def _i(value: Any) -> int | None:
    try:
        if not _nonblank(value):
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def _team(value: Any) -> str:
    text = str(value or "").strip().upper()
    aliases = {
        "AZ": "ARI",
        "CHW": "CWS",
        "KCR": "KC",
        "OAK": "ATH",
        "SDP": "SD",
        "SFG": "SF",
        "TBR": "TB",
        "WSN": "WSH",
        "NYA": "NYY",
        "NYN": "NYM",
        "LA": "LAD",
    }
    return aliases.get(text, text)


def _team_from_full_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "arizona diamondbacks": "ARI",
        "athletics": "ATH",
        "oakland athletics": "ATH",
        "atlanta braves": "ATL",
        "baltimore orioles": "BAL",
        "boston red sox": "BOS",
        "chicago cubs": "CHC",
        "chicago white sox": "CWS",
        "cincinnati reds": "CIN",
        "cleveland guardians": "CLE",
        "colorado rockies": "COL",
        "detroit tigers": "DET",
        "houston astros": "HOU",
        "kansas city royals": "KC",
        "los angeles angels": "LAA",
        "los angeles dodgers": "LAD",
        "miami marlins": "MIA",
        "milwaukee brewers": "MIL",
        "minnesota twins": "MIN",
        "new york mets": "NYM",
        "new york yankees": "NYY",
        "philadelphia phillies": "PHI",
        "pittsburgh pirates": "PIT",
        "san diego padres": "SD",
        "san francisco giants": "SF",
        "seattle mariners": "SEA",
        "st. louis cardinals": "STL",
        "st louis cardinals": "STL",
        "tampa bay rays": "TB",
        "texas rangers": "TEX",
        "toronto blue jays": "TOR",
        "washington nationals": "WSH",
    }
    return mapping.get(text) or _team(value)


def _norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _date_from_filename(path: Path) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if match:
        return match.group(1)
    for part in path.parts:
        if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", part):
            return part
    return ""


def _snapshot_timestamp(path: Path) -> str:
    match = re.search(r"__local_daily_(\d{8}T\d{6}Z)", path.name)
    return match.group(1) if match else ""


def _in_window(date_text: str, start: str, end: str) -> bool:
    return bool(date_text and start <= date_text <= end)


def _target_files(review_aids_dir: Path, performance_dir: Path, expanded_dir: Path, start: str, end: str) -> list[tuple[str, Path]]:
    targets: list[tuple[str, Path]] = []
    for pattern in BOARD_PATTERNS:
        for path in sorted(review_aids_dir.glob(pattern)):
            if _in_window(_date_from_filename(path), start, end):
                targets.append(("review_board_rows", path))
    for name in ("hits_o15_tier_backtest_rows.csv", "hits_u15_tier_backtest_rows.csv"):
        path = review_aids_dir / name
        if path.exists():
            targets.append(("tier_audit_rows", path))
    for artifact_type, path in (
        ("manual_unified_rows", performance_dir / "o15_manual_unified_board_universe_rows.csv"),
        ("expanded_universe_rows", expanded_dir / "expanded_o15_universe_rows.csv"),
    ):
        if path.exists():
            targets.append((artifact_type, path))
    return targets


def _load_alt_source_index(alt_history_dir: Path, alt_live_dir: Path, date_text: str) -> dict[tuple[str, str], list[dict[str, Any]]]:
    paths = [
        alt_history_dir / date_text / "live_alternate_book_level_rows.csv",
        alt_live_dir / date_text / "live_alternate_book_level_rows.csv",
    ]
    out: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[str, str, str, str, str]] = set()
    for path in paths:
        rows, _fields = _read_csv(path)
        for row in rows:
            name = _norm_name(row.get("player_name") or row.get("normalized_player_name"))
            line = str(row.get("line") or "").strip()
            if not name:
                continue
            home = _team_from_full_name(row.get("home_team"))
            away = _team_from_full_name(row.get("away_team"))
            team = _team(row.get("team"))
            opponent = _team(row.get("opponent"))
            if not home or not away:
                continue
            key = (name, line)
            event_key = (name, line, home, away, str(row.get("event_id") or ""))
            if event_key in seen:
                continue
            seen.add(event_key)
            out[key].append(
                {
                    "home_team": home,
                    "away_team": away,
                    "team": team,
                    "opponent": opponent,
                    "source_path": _rel(path),
                    "event_id": row.get("event_id") or "",
                }
            )
    return out


def _build_snapshot_source(paths: list[Path], source_name: str, priority: int) -> dict[str, Any]:
    game_rows: dict[int, dict[str, Any]] = {}
    team_games: dict[tuple[str, str], set[int]] = defaultdict(set)
    player_game: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    name_game_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    starter_candidates: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    seen_context: set[tuple[str, int, int | None, str, str, str]] = set()
    seen_starters: set[tuple[int, str, int, str, str]] = set()

    for path in sorted(paths):
        rows, _fields = _read_csv(path)
        timestamp = _snapshot_timestamp(path)
        for row in rows:
            game_id = _i(row.get("game_id"))
            if game_id is None:
                continue
            home = _team(row.get("home_team_code"))
            away = _team(row.get("away_team_code"))
            if home and away:
                game_rows.setdefault(int(game_id), {"game_id": int(game_id), "home_team": home, "away_team": away})
                team_games[(home, away)].add(int(game_id))
                team_games[(away, home)].add(int(game_id))
            player_id = _i(row.get("player_id"))
            player_name = str(row.get("player_name") or row.get("player") or "").strip()
            team = _team(row.get("team") or row.get("team_code"))
            opponent = _team(row.get("opponent"))
            prop_type = str(row.get("prop_type") or "").strip().lower()
            if player_name and team and opponent:
                ctx = {
                    "game_id": int(game_id),
                    "player_id": player_id,
                    "player_name": player_name,
                    "team": team,
                    "opponent": opponent,
                    "source_path": _rel(path),
                    "snapshot_timestamp": timestamp,
                    "starter_identity_source": source_name,
                    "starter_identity_source_priority": priority,
                }
                context_key = (_norm_name(player_name), int(game_id), player_id, team, opponent, _rel(path))
                if context_key not in seen_context:
                    seen_context.add(context_key)
                    name_game_candidates[_norm_name(player_name)].append(ctx)
                    if player_id is not None:
                        player_game[(int(game_id), int(player_id))].append(ctx)
            if prop_type in PITCHER_PROP_TYPES and player_id is not None and player_name and team and opponent:
                starter = {
                    "game_id": int(game_id),
                    "starter_id": int(player_id),
                    "starter_name": player_name,
                    "pitcher_team": team,
                    "offense_team": opponent,
                    "starter_source": source_name,
                    "starter_prop_type": prop_type,
                    "source_path": _rel(path),
                    "snapshot_timestamp": timestamp,
                    "starter_identity_source": source_name,
                    "starter_identity_source_priority": priority,
                }
                starter_key = (int(game_id), team, int(player_id), prop_type, _rel(path))
                if starter_key not in seen_starters:
                    seen_starters.add(starter_key)
                    starter_candidates[(int(game_id), team)].append(starter)
    return {
        "source_name": source_name,
        "priority": priority,
        "files": paths,
        "game_rows": game_rows,
        "team_games": team_games,
        "player_game": player_game,
        "name_game_candidates": name_game_candidates,
        "starter_candidates": starter_candidates,
    }


def _load_sources_for_date(odds_history_dir: Path, alt_history_dir: Path, alt_live_dir: Path, date_text: str) -> dict[str, Any]:
    root = odds_history_dir / date_text
    wide_rows, _wide_fields = _read_csv(root / "mlb_predictions_wide_calibrated.csv")
    slate_rows, _slate_fields = _read_csv(root / "mlb_slate_output.csv")
    all_rows = wide_rows + slate_rows
    timestamped_slate_paths = sorted(path for path in root.glob("mlb_slate_output*.csv") if path.name != "mlb_slate_output.csv")
    timestamped_wide_paths = sorted(
        path for path in root.glob("mlb_predictions_wide_calibrated*.csv") if path.name != "mlb_predictions_wide_calibrated.csv"
    )

    game_rows: dict[int, dict[str, Any]] = {}
    team_games: dict[tuple[str, str], set[int]] = defaultdict(set)
    player_game: dict[tuple[int, int], dict[str, Any]] = {}
    name_game_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    starter_candidates: dict[tuple[int, str], dict[str, Any]] = {}
    starter_conflicts: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)

    for row in all_rows:
        game_id = _i(row.get("game_id"))
        if game_id is None:
            continue
        home = _team(row.get("home_team_code"))
        away = _team(row.get("away_team_code"))
        if home and away:
            game_rows.setdefault(int(game_id), {"game_id": int(game_id), "home_team": home, "away_team": away})
            team_games[(home, away)].add(int(game_id))
            team_games[(away, home)].add(int(game_id))
        player_id = _i(row.get("player_id"))
        player_name = str(row.get("player_name") or row.get("player") or "").strip()
        team = _team(row.get("team") or row.get("team_code"))
        opponent = _team(row.get("opponent"))
        prop_type = str(row.get("prop_type") or "").strip().lower()
        if player_id is not None and team and opponent:
            player_game.setdefault(
                (int(game_id), int(player_id)),
                {
                    "game_id": int(game_id),
                    "player_id": int(player_id),
                    "player_name": player_name,
                    "team": team,
                    "opponent": opponent,
                },
            )
        if player_name and team and opponent:
            name_game_candidates[_norm_name(player_name)].append(
                {
                    "game_id": int(game_id),
                    "player_id": player_id,
                    "player_name": player_name,
                    "team": team,
                    "opponent": opponent,
                }
            )
        if prop_type in PITCHER_PROP_TYPES and player_id is not None and team and opponent:
            key = (int(game_id), team)
            candidate = {
                "game_id": int(game_id),
                "starter_id": int(player_id),
                "starter_name": player_name,
                "pitcher_team": team,
                "offense_team": opponent,
                "starter_source": "wide_pitcher_context" if row in wide_rows else "slate_pitcher_context",
                "starter_prop_type": prop_type,
            }
            cur = starter_candidates.get(key)
            if cur is None or (cur.get("starter_prop_type") != "hits_allowed" and prop_type == "hits_allowed"):
                starter_candidates[key] = candidate
            if cur is not None and int(cur.get("starter_id") or 0) != int(player_id):
                starter_conflicts[key].append(candidate)

    return {
        "root": root,
        "wide_exists": (root / "mlb_predictions_wide_calibrated.csv").exists(),
        "slate_exists": (root / "mlb_slate_output.csv").exists(),
        "odds_exists": (root / "odds_latest_compatible.json").exists(),
        "game_rows": game_rows,
        "team_games": team_games,
        "player_game": player_game,
        "name_game_candidates": name_game_candidates,
        "starter_candidates": starter_candidates,
        "starter_conflicts": starter_conflicts,
        "snapshot_sources": [
            _build_snapshot_source(timestamped_slate_paths, "timestamped_slate_output", 2),
            _build_snapshot_source(timestamped_wide_paths, "timestamped_predictions_wide", 3),
        ],
        "alt_source_index": _load_alt_source_index(alt_history_dir, alt_live_dir, date_text),
    }


def _unique_context(
    *,
    snapshot: dict[str, Any],
    game_id: int | None,
    player_id: int | None,
    player_name: str,
    offense_team: str,
    opponent_team: str,
) -> tuple[int | None, str, str, str, str, str]:
    method = ""
    source_path = ""
    timestamp = ""

    if game_id is None and player_id is not None:
        contexts = []
        for (gid, pid), rows in snapshot["player_game"].items():
            if pid == int(player_id):
                contexts.extend(rows)
        if offense_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("team")) == offense_team]
        if opponent_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("opponent")) == opponent_team]
        game_ids = {int(ctx["game_id"]) for ctx in contexts}
        team_pairs = {(_team(ctx.get("team")), _team(ctx.get("opponent"))) for ctx in contexts}
        team_pairs = {pair for pair in team_pairs if pair[0] and pair[1]}
        if len(game_ids) == 1 and len(team_pairs) == 1:
            ctx = contexts[0]
            game_id = next(iter(game_ids))
            offense_team, opponent_team = next(iter(team_pairs))
            method = f"{snapshot['source_name']}_player_id_unambiguous"
            source_path = str(ctx.get("source_path") or "")
            timestamp = str(ctx.get("snapshot_timestamp") or "")

    if game_id is None and player_name:
        contexts = list(snapshot["name_game_candidates"].get(_norm_name(player_name), []))
        if offense_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("team")) == offense_team]
        if opponent_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("opponent")) == opponent_team]
        game_ids = {int(ctx["game_id"]) for ctx in contexts}
        team_pairs = {(_team(ctx.get("team")), _team(ctx.get("opponent"))) for ctx in contexts}
        team_pairs = {pair for pair in team_pairs if pair[0] and pair[1]}
        if len(game_ids) == 1 and len(team_pairs) == 1:
            ctx = contexts[0]
            game_id = next(iter(game_ids))
            offense_team, opponent_team = next(iter(team_pairs))
            method = f"{snapshot['source_name']}_player_name_unambiguous"
            source_path = str(ctx.get("source_path") or "")
            timestamp = str(ctx.get("snapshot_timestamp") or "")

    if game_id is not None and (not offense_team or not opponent_team) and player_name:
        contexts = [
            ctx
            for ctx in snapshot["name_game_candidates"].get(_norm_name(player_name), [])
            if int(ctx.get("game_id") or 0) == int(game_id)
        ]
        team_pairs = {(_team(ctx.get("team")), _team(ctx.get("opponent"))) for ctx in contexts}
        team_pairs = {pair for pair in team_pairs if pair[0] and pair[1]}
        if len(team_pairs) == 1:
            offense_team, opponent_team = next(iter(team_pairs))
            if not method:
                method = f"{snapshot['source_name']}_game_id_player_name_unambiguous"
                source_path = str(contexts[0].get("source_path") or "") if contexts else ""
                timestamp = str(contexts[0].get("snapshot_timestamp") or "") if contexts else ""

    if game_id is not None and offense_team and not opponent_team:
        game = snapshot["game_rows"].get(int(game_id), {})
        home = _team(game.get("home_team"))
        away = _team(game.get("away_team"))
        if offense_team == home:
            opponent_team = away
        elif offense_team == away:
            opponent_team = home
    elif game_id is not None and opponent_team and not offense_team:
        game = snapshot["game_rows"].get(int(game_id), {})
        home = _team(game.get("home_team"))
        away = _team(game.get("away_team"))
        if opponent_team == home:
            offense_team = away
        elif opponent_team == away:
            offense_team = home

    return game_id, offense_team, opponent_team, method, source_path, timestamp


def _unique_starter(snapshot: dict[str, Any], game_id: int, opponent_team: str) -> tuple[dict[str, Any] | None, str]:
    starters = list(snapshot["starter_candidates"].get((int(game_id), opponent_team), []))
    if not starters:
        return None, "no_opposing_starter_in_local_pitcher_context"
    by_id: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for starter in starters:
        sid = _i(starter.get("starter_id"))
        if sid is not None:
            by_id[sid].append(starter)
    if len(by_id) != 1:
        return None, "starter_conflict_across_sources"
    rows = next(iter(by_id.values()))
    hits_allowed = [row for row in rows if row.get("starter_prop_type") == "hits_allowed"]
    return (hits_allowed or rows)[0], ""


def _recover_from_snapshots(
    *,
    source: dict[str, Any],
    original_game_id: int | None,
    game_id: int | None,
    player_id: int | None,
    player_name: str,
    offense_team: str,
    opponent_team: str,
    starter_id_existing: int | None,
    starter_name_existing: str,
) -> dict[str, Any] | None:
    for snapshot in source.get("snapshot_sources", []):
        snap_game_id, snap_offense_team, snap_opponent_team, snap_method, context_path, context_timestamp = _unique_context(
            snapshot=snapshot,
            game_id=game_id,
            player_id=player_id,
            player_name=player_name,
            offense_team=offense_team,
            opponent_team=opponent_team,
        )
        if snap_game_id is None:
            continue
        if not snap_offense_team or not snap_opponent_team:
            continue
        starter, starter_reason = _unique_starter(snapshot, int(snap_game_id), snap_opponent_team)
        if starter_reason == "starter_conflict_across_sources":
            return {
                "status": "unresolved",
                "reason": starter_reason,
                "game_id": snap_game_id,
                "offense_team": snap_offense_team,
                "opponent_team": snap_opponent_team,
                "starter": None,
                "method": snap_method,
                "snapshot_file": context_path,
                "snapshot_timestamp": context_timestamp,
                "source": snapshot["source_name"],
                "priority": snapshot["priority"],
            }
        if starter is None:
            continue
        starter_id = _i(starter.get("starter_id"))
        starter_name = str(starter.get("starter_name") or "")
        if starter_id_existing is not None and starter_id is not None and int(starter_id) != int(starter_id_existing):
            return {
                "status": "unresolved",
                "reason": "existing_starter_id_conflicts_with_local_context",
                "game_id": snap_game_id,
                "offense_team": snap_offense_team,
                "opponent_team": snap_opponent_team,
                "starter": starter,
                "method": snap_method,
                "snapshot_file": str(starter.get("source_path") or context_path),
                "snapshot_timestamp": str(starter.get("snapshot_timestamp") or context_timestamp),
                "source": snapshot["source_name"],
                "priority": snapshot["priority"],
            }
        if starter_name_existing and starter_name and _norm_name(starter_name_existing) != _norm_name(starter_name):
            return {
                "status": "unresolved",
                "reason": "existing_starter_name_conflicts_with_local_context",
                "game_id": snap_game_id,
                "offense_team": snap_offense_team,
                "opponent_team": snap_opponent_team,
                "starter": starter,
                "method": snap_method,
                "snapshot_file": str(starter.get("source_path") or context_path),
                "snapshot_timestamp": str(starter.get("snapshot_timestamp") or context_timestamp),
                "source": snapshot["source_name"],
                "priority": snapshot["priority"],
            }
        return {
            "status": "resolved",
            "reason": "",
            "game_id": snap_game_id,
            "offense_team": snap_offense_team,
            "opponent_team": snap_opponent_team,
            "starter": starter,
            "method": snap_method or f"{snapshot['source_name']}_game_team_starter_unambiguous",
            "snapshot_file": str(starter.get("source_path") or context_path),
            "snapshot_timestamp": str(starter.get("snapshot_timestamp") or context_timestamp),
            "source": snapshot["source_name"],
            "priority": snapshot["priority"],
            "confidence": "high" if original_game_id is not None else "medium",
        }
    return None


def _row_date(row: dict[str, Any], fallback_date: str = "") -> str:
    return str(row.get("date") or row.get("board_date") or row.get("game_date") or row.get("slate_date") or fallback_date)[:10]


def _resolve_row(
    *,
    row: dict[str, Any],
    row_index: int,
    artifact_type: str,
    path: Path,
    source: dict[str, Any],
    date_text: str,
) -> dict[str, Any]:
    original_game_id = _i(row.get("game_id") or row.get("canonical_game_id"))
    player_id = _i(row.get("player_id") or row.get("canonical_player_id"))
    player_name = str(row.get("player_name") or row.get("player") or "").strip()
    offense_team = _team(row.get("team") or row.get("offense_team") or row.get("canonical_team"))
    opponent_team = _team(row.get("opponent") or row.get("pitcher_team") or row.get("canonical_opponent"))
    game_id = original_game_id
    mapping_key_used = ""
    warnings: list[str] = []

    if game_id is not None and player_id is not None:
        ctx = source["player_game"].get((int(game_id), int(player_id)))
        if ctx:
            offense_team = offense_team or _team(ctx.get("team"))
            opponent_team = opponent_team or _team(ctx.get("opponent"))
            mapping_key_used = "game_id_player_id"

    if game_id is None and player_id is not None and offense_team and opponent_team:
        candidates = [
            ctx
            for (gid, pid), ctx in source["player_game"].items()
            if pid == int(player_id) and _team(ctx.get("team")) == offense_team and _team(ctx.get("opponent")) == opponent_team
        ]
        game_ids = {int(ctx["game_id"]) for ctx in candidates}
        if len(game_ids) == 1:
            game_id = next(iter(game_ids))
            mapping_key_used = "player_id_team_opponent"
        elif len(game_ids) > 1:
            warnings.append("multiple_games_for_player_team_opponent")

    if game_id is None and player_name:
        candidates = [
            ctx
            for ctx in source["name_game_candidates"].get(_norm_name(player_name), [])
            if (not offense_team or _team(ctx.get("team")) == offense_team)
            and (not opponent_team or _team(ctx.get("opponent")) == opponent_team)
        ]
        game_ids = {int(ctx["game_id"]) for ctx in candidates}
        if len(game_ids) == 1:
            ctx = candidates[0]
            game_id = int(ctx["game_id"])
            offense_team = offense_team or _team(ctx.get("team"))
            opponent_team = opponent_team or _team(ctx.get("opponent"))
            mapping_key_used = "player_name_team_opponent_unambiguous"
        elif len(game_ids) > 1:
            warnings.append("multiple_name_matches")

    if game_id is None and player_name:
        line = str(row.get("line") or "").strip()
        alt_candidates = source["alt_source_index"].get((_norm_name(player_name), line), [])
        games: list[tuple[int, dict[str, Any]]] = []
        for alt in alt_candidates:
            home = _team(alt.get("home_team"))
            away = _team(alt.get("away_team"))
            pair_games = source["team_games"].get((home, away), set())
            if len(pair_games) == 1:
                games.append((next(iter(pair_games)), alt))
        game_ids = {gid for gid, _alt in games}
        if len(game_ids) == 1:
            game_id, alt = games[0]
            alt_team = _team(alt.get("team"))
            alt_opp = _team(alt.get("opponent"))
            if alt_team and alt_opp:
                offense_team = offense_team or alt_team
                opponent_team = opponent_team or alt_opp
            mapping_key_used = "alternate_source_event_unambiguous"
        elif len(game_ids) > 1:
            warnings.append("multiple_alternate_source_events")

    if game_id is None and offense_team and opponent_team:
        pair_games = set(source["team_games"].get((offense_team, opponent_team), set()))
        if len(pair_games) == 1:
            game_id = next(iter(pair_games))
            mapping_key_used = "team_opponent_unambiguous_game"
        elif len(pair_games) > 1:
            warnings.append("multi_game_or_doubleheader_without_game_id")

    if game_id is not None and (not offense_team or not opponent_team):
        if player_name:
            candidates = [
                ctx
                for ctx in source["name_game_candidates"].get(_norm_name(player_name), [])
                if int(ctx.get("game_id") or 0) == int(game_id)
            ]
            teams = {(_team(ctx.get("team")), _team(ctx.get("opponent"))) for ctx in candidates}
            teams = {pair for pair in teams if pair[0] and pair[1]}
            if len(teams) == 1:
                team_pair = next(iter(teams))
                offense_team = offense_team or team_pair[0]
                opponent_team = opponent_team or team_pair[1]
                if not mapping_key_used:
                    mapping_key_used = "game_id_player_name_unambiguous"
        game = source["game_rows"].get(int(game_id), {})
        home = _team(game.get("home_team"))
        away = _team(game.get("away_team"))
        if offense_team and not opponent_team:
            opponent_team = away if offense_team == home else home if offense_team == away else ""
        elif opponent_team and not offense_team:
            offense_team = away if opponent_team == home else home if opponent_team == away else ""

    starter_id_existing = _i(row.get("opposing_starter_id") or row.get("starter_player_id"))
    starter_name_existing = str(row.get("opposing_starter") or row.get("starter_name") or "").strip()
    starter = None
    starter_conflict = ""
    if game_id is not None and opponent_team:
        conflicts = source["starter_conflicts"].get((int(game_id), opponent_team), [])
        if conflicts:
            starter_conflict = "starter_conflict_across_sources"
        starter = source["starter_candidates"].get((int(game_id), opponent_team))

    status = "unresolved"
    reason = ""
    confidence = "none"
    starter_id = ""
    starter_name = ""
    starter_source = ""
    starter_identity_source = ""
    starter_identity_source_priority = ""
    starter_identity_snapshot_file = ""
    starter_identity_snapshot_timestamp = ""

    if game_id is None:
        reason = ";".join(warnings) or "missing_game_identity"
    elif not offense_team or not opponent_team:
        reason = "missing_team_or_opponent"
    elif starter_conflict:
        reason = starter_conflict
    elif starter is None:
        reason = "no_opposing_starter_in_local_pitcher_context"
    else:
        starter_id = str(starter.get("starter_id") or "")
        starter_name = str(starter.get("starter_name") or "")
        starter_source = str(starter.get("starter_source") or "")
        if starter_id_existing is not None and starter_id and int(starter_id) != int(starter_id_existing):
            reason = "existing_starter_id_conflicts_with_local_context"
        elif starter_name_existing and starter_name and _norm_name(starter_name_existing) != _norm_name(starter_name):
            reason = "existing_starter_name_conflicts_with_local_context"
        else:
            status = "resolved"
            confidence = "high" if original_game_id is not None else "medium"
            reason = ""
            if not mapping_key_used:
                mapping_key_used = "existing_game_id_team_opponent"
            starter_identity_source = "latest_canonical_wide_slate"
            starter_identity_source_priority = "1"

    if status != "resolved" and reason not in {
        "starter_conflict_across_sources",
        "existing_starter_id_conflicts_with_local_context",
        "existing_starter_name_conflicts_with_local_context",
    }:
        snapshot_resolution = _recover_from_snapshots(
            source=source,
            original_game_id=original_game_id,
            game_id=game_id,
            player_id=player_id,
            player_name=player_name,
            offense_team=offense_team,
            opponent_team=opponent_team,
            starter_id_existing=starter_id_existing,
            starter_name_existing=starter_name_existing,
        )
        if snapshot_resolution is not None:
            game_id = _i(snapshot_resolution.get("game_id")) or game_id
            offense_team = _team(snapshot_resolution.get("offense_team")) or offense_team
            opponent_team = _team(snapshot_resolution.get("opponent_team")) or opponent_team
            mapping_key_used = str(snapshot_resolution.get("method") or mapping_key_used)
            starter_identity_source = str(snapshot_resolution.get("source") or "")
            starter_identity_source_priority = str(snapshot_resolution.get("priority") or "")
            starter_identity_snapshot_file = str(snapshot_resolution.get("snapshot_file") or "")
            starter_identity_snapshot_timestamp = str(snapshot_resolution.get("snapshot_timestamp") or "")
            if snapshot_resolution.get("status") == "resolved":
                snap_starter = snapshot_resolution.get("starter") or {}
                starter_id = str(snap_starter.get("starter_id") or "")
                starter_name = str(snap_starter.get("starter_name") or "")
                starter_source = str(snap_starter.get("starter_source") or starter_identity_source)
                status = "resolved"
                confidence = str(snapshot_resolution.get("confidence") or "medium")
                reason = ""
            else:
                reason = str(snapshot_resolution.get("reason") or reason)

    row_key = "|".join(
        [
            date_text,
            str(row.get("player_id") or ""),
            _norm_name(player_name),
            str(row.get("line") or ""),
            artifact_type,
            str(row_index),
        ]
    )
    return {
        "date": date_text,
        "target_artifact_type": artifact_type,
        "target_path": _rel(path),
        "target_row_index": row_index,
        "target_row_key": row_key,
        "player_id": row.get("player_id") or "",
        "player_name": player_name,
        "input_team": row.get("team") or row.get("offense_team") or row.get("canonical_team") or "",
        "input_opponent": row.get("opponent") or row.get("pitcher_team") or row.get("canonical_opponent") or "",
        "resolved_game_id": game_id or "",
        "resolved_offense_team": offense_team,
        "resolved_pitcher_team": opponent_team,
        "opposing_starter_id": starter_id,
        "opposing_starter_name": starter_name,
        "starter_source": starter_source,
        "starter_identity_source": starter_identity_source,
        "starter_identity_source_priority": starter_identity_source_priority,
        "starter_identity_snapshot_file": starter_identity_snapshot_file,
        "starter_identity_snapshot_timestamp": starter_identity_snapshot_timestamp,
        "starter_confidence": confidence,
        "starter_identity_status": status,
        "mapping_key_used": mapping_key_used,
        "unresolved_reason": reason,
        "doubleheader_or_multigame_risk": "yes" if "multi_game" in reason or "doubleheader" in reason or "multi_game" in ";".join(warnings) else "no",
        "source_wide_exists": source["wide_exists"],
        "source_slate_exists": source["slate_exists"],
        "source_odds_exists": source["odds_exists"],
    }


def _write_report(path: Path, rows: list[dict[str, Any]], start: str, end: str) -> None:
    total = len(rows)
    resolved = sum(1 for row in rows if row.get("starter_identity_status") == "resolved")
    unresolved = total - resolved
    reasons = Counter(row.get("unresolved_reason") or "resolved" for row in rows)
    methods = Counter(row.get("mapping_key_used") or "none" for row in rows if row.get("starter_identity_status") == "resolved")
    doubleheader_risk = sum(1 for row in rows if row.get("doubleheader_or_multigame_risk") == "yes")
    if unresolved == 0 and doubleheader_risk == 0:
        proceed_text = "yes"
    elif resolved > 0 and doubleheader_risk == 0:
        proceed_text = "partial / only resolved rows"
    else:
        proceed_text = "no"
    examples_resolved = [row for row in rows if row.get("starter_identity_status") == "resolved"][:10]
    examples_unresolved = [row for row in rows if row.get("starter_identity_status") != "resolved"][:10]
    lines = [
        "# Offensive Environment v1.1 Starter Identity Prepass",
        "",
        f"- Generated at: `{_now()}`",
        f"- Window: `{start}` through `{end}`",
        "- Scope: identity/provenance only; no Environment v1.1 values were reconstructed or written.",
        "",
        "## Summary",
        "",
        f"- Rows examined: `{total}`",
        f"- Rows resolved: `{resolved}`",
        f"- Rows unresolved: `{unresolved}`",
        f"- Resolution rate: `{(resolved / total * 100.0) if total else 0.0:.2f}%`",
        f"- Doubleheader/multi-game risk rows: `{doubleheader_risk}`",
        f"- Reconstruction can safely proceed: `{proceed_text}`",
        "",
        "## Source Priority Used",
        "",
        "1. Existing row `game_id` + local wide/slate player context.",
        "2. Timestamped `mlb_slate_output*.csv` snapshots, only when game/team/starter identity is unambiguous.",
        "3. Timestamped `mlb_predictions_wide_calibrated*.csv` snapshots, only when game/team/starter identity is unambiguous.",
        "4. Other existing local event/player context already in the direct resolver.",
        "5. Leave unresolved.",
        "",
        "No external probable-starter download was used.",
        "",
        "## Resolved Mapping Methods",
        "",
    ]
    for key, value in methods.most_common():
        lines.append(f"- `{key}`: `{value}`")
    source_counts = Counter(row.get("starter_identity_source") or "unknown" for row in rows if row.get("starter_identity_status") == "resolved")
    lines.extend(["", "## Resolved Source Priority", ""])
    for key, value in source_counts.most_common():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Unresolved Reasons", ""])
    for key, value in reasons.most_common():
        if key == "resolved":
            continue
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Resolved Examples", ""])
    lines.append("| date | player | team | opponent | game_id | starter | starter_id | method |")
    lines.append("|---|---|---|---|---:|---|---:|---|")
    for row in examples_resolved:
        lines.append(
            f"| `{row.get('date')}` | `{row.get('player_name')}` | `{row.get('resolved_offense_team')}` | "
            f"`{row.get('resolved_pitcher_team')}` | `{row.get('resolved_game_id')}` | "
            f"`{row.get('opposing_starter_name')}` | `{row.get('opposing_starter_id')}` | `{row.get('mapping_key_used')}` |"
        )
    lines.extend(["", "## Unresolved Examples", ""])
    lines.append("| date | player | input team | input opponent | game_id | reason |")
    lines.append("|---|---|---|---|---:|---|")
    for row in examples_unresolved:
        lines.append(
            f"| `{row.get('date')}` | `{row.get('player_name')}` | `{row.get('input_team')}` | "
            f"`{row.get('input_opponent')}` | `{row.get('resolved_game_id')}` | `{row.get('unresolved_reason')}` |"
        )
    lines.extend(
        [
            "",
            "## Safety Decision",
            "",
            "A reconstruction write pass should use only rows with `starter_identity_status=resolved`.",
            "Rows with unresolved starter identity, source conflicts, or doubleheader/multi-game ambiguity should remain blank until a stronger source is available.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _index_by_key(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("target_row_key") or ""): row for row in rows if row.get("target_row_key")}


def _write_snapshot_recovery_outputs(
    *,
    report_path: Path,
    csv_path: Path,
    previous_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    start: str,
    end: str,
) -> None:
    prev_by_key = _index_by_key(previous_rows)
    before_total = len(previous_rows) or len(current_rows)
    before_resolved = sum(1 for row in previous_rows if row.get("starter_identity_status") == "resolved")
    after_total = len(current_rows)
    after_resolved = sum(1 for row in current_rows if row.get("starter_identity_status") == "resolved")
    before_reasons = Counter(row.get("unresolved_reason") or "resolved" for row in previous_rows)
    after_reasons = Counter(row.get("unresolved_reason") or "resolved" for row in current_rows)

    recovery_rows: list[dict[str, Any]] = []
    recovered_by_before_reason = Counter()
    recovered_by_source = Counter()
    recovered_by_date = Counter()
    recovered_by_family = Counter()
    for row in current_rows:
        key = str(row.get("target_row_key") or "")
        prev = prev_by_key.get(key, {})
        prev_status = prev.get("starter_identity_status") or ""
        cur_status = row.get("starter_identity_status") or ""
        source = row.get("starter_identity_source") or ""
        recovered = prev_status != "resolved" and cur_status == "resolved"
        if recovered:
            recovered_by_before_reason[prev.get("unresolved_reason") or "unknown"] += 1
            recovered_by_source[source or "unknown"] += 1
            recovered_by_date[row.get("date") or ""] += 1
            recovered_by_family[source or "unknown"] += 1
        if recovered or source.startswith("timestamped"):
            recovery_rows.append(
                {
                    "date": row.get("date") or "",
                    "target_artifact_type": row.get("target_artifact_type") or "",
                    "target_path": row.get("target_path") or "",
                    "target_row_index": row.get("target_row_index") or "",
                    "player_name": row.get("player_name") or "",
                    "before_status": prev_status,
                    "before_unresolved_reason": prev.get("unresolved_reason") or "",
                    "after_status": cur_status,
                    "after_unresolved_reason": row.get("unresolved_reason") or "",
                    "resolved_game_id": row.get("resolved_game_id") or "",
                    "resolved_offense_team": row.get("resolved_offense_team") or "",
                    "resolved_pitcher_team": row.get("resolved_pitcher_team") or "",
                    "opposing_starter_id": row.get("opposing_starter_id") or "",
                    "opposing_starter_name": row.get("opposing_starter_name") or "",
                    "starter_identity_source": source,
                    "starter_identity_source_priority": row.get("starter_identity_source_priority") or "",
                    "starter_identity_snapshot_file": row.get("starter_identity_snapshot_file") or "",
                    "starter_identity_snapshot_timestamp": row.get("starter_identity_snapshot_timestamp") or "",
                    "mapping_key_used": row.get("mapping_key_used") or "",
                    "recovered": "yes" if recovered else "no",
                }
            )
    _write_csv(csv_path, recovery_rows)

    newly_recovered = max(after_resolved - before_resolved, 0)
    lines = [
        "# Offensive Environment v1.1 Starter Identity Snapshot Recovery",
        "",
        f"- Generated at: `{_now()}`",
        f"- Window: `{start}` through `{end}`",
        "- Scope: starter identity/provenance only.",
        "- Environment component values written: `no`",
        "- External APIs called: `no`",
        "",
        "## Before / After",
        "",
        f"- Rows before: `{before_total}`",
        f"- Resolved before: `{before_resolved}`",
        f"- Resolution rate before: `{(before_resolved / before_total * 100.0) if before_total else 0.0:.2f}%`",
        f"- Rows after: `{after_total}`",
        f"- Resolved after: `{after_resolved}`",
        f"- Resolution rate after: `{(after_resolved / after_total * 100.0) if after_total else 0.0:.2f}%`",
        f"- Newly recovered rows: `{newly_recovered}`",
        "",
        "## Recovered By Previous Unresolved Reason",
        "",
    ]
    for key, value in recovered_by_before_reason.most_common():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Recovered By Source Type", ""])
    for key, value in recovered_by_source.most_common():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Recovered By Snapshot Family", ""])
    for key, value in recovered_by_family.most_common():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Recovered By Date", ""])
    for key, value in sorted(recovered_by_date.items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Remaining Unresolved Reasons", ""])
    for key, value in after_reasons.most_common():
        if key == "resolved":
            continue
        before_value = before_reasons.get(key, 0)
        lines.append(f"- `{key}`: `{value}` (before `{before_value}`)")
    if after_total and after_resolved / after_total >= 0.70:
        readiness = "partial / materially improved, but unresolved rows still need local DB or external starter authority before complete reconstruction"
    else:
        readiness = "not ready for broad reconstruction"
    lines.extend(
        [
            "",
            "## Readiness For Environment Reconstruction",
            "",
            f"- Readiness: `{readiness}`",
            "- Safe reconstruction can proceed only for rows with `starter_identity_status=resolved`.",
            "- Conflict rows and unresolved rows must remain blank until a stronger starter identity source is added.",
        ]
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build starter identity map for Environment v1.1 reconstruction.")
    ap.add_argument("--date-from", default="2026-05-13")
    ap.add_argument("--date-to", default="2026-06-15")
    ap.add_argument("--review-aids-dir", type=Path, default=DEFAULT_REVIEW_AIDS_DIR)
    ap.add_argument("--performance-dir", type=Path, default=DEFAULT_PERFORMANCE_DIR)
    ap.add_argument("--expanded-dir", type=Path, default=DEFAULT_EXPANDED_DIR)
    ap.add_argument("--odds-history-dir", type=Path, default=DEFAULT_ODDS_HISTORY_DIR)
    ap.add_argument("--alternate-history-dir", type=Path, default=DEFAULT_ALT_HISTORY_DIR)
    ap.add_argument("--alternate-live-dir", type=Path, default=DEFAULT_ALT_LIVE_DIR)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    date_label = "2026-06-29"
    map_csv = args.out_dir / f"offensive_environment_v1_1_starter_identity_map_{date_label}.csv"
    previous_rows, _previous_fields = _read_csv(map_csv)

    date_cache: dict[str, dict[str, Any]] = {}
    out_rows: list[dict[str, Any]] = []
    for artifact_type, path in _target_files(args.review_aids_dir, args.performance_dir, args.expanded_dir, args.date_from, args.date_to):
        rows, _fields = _read_csv(path)
        fallback_date = _date_from_filename(path)
        for idx, row in enumerate(rows, start=1):
            date_text = _row_date(row, fallback_date)
            if not _in_window(date_text, args.date_from, args.date_to):
                continue
            source = date_cache.get(date_text)
            if source is None:
                source = _load_sources_for_date(args.odds_history_dir, args.alternate_history_dir, args.alternate_live_dir, date_text)
                date_cache[date_text] = source
            out_rows.append(
                _resolve_row(
                    row=row,
                    row_index=idx,
                    artifact_type=artifact_type,
                    path=path,
                    source=source,
                    date_text=date_text,
                )
            )

    report_md = args.out_dir / f"offensive_environment_v1_1_starter_identity_prepass_{date_label}.md"
    recovery_md = args.out_dir / f"offensive_environment_v1_1_starter_identity_snapshot_recovery_{date_label}.md"
    recovery_csv = args.out_dir / "offensive_environment_v1_1_starter_identity_snapshot_recovery.csv"
    _write_csv(map_csv, out_rows)
    _write_report(report_md, out_rows, args.date_from, args.date_to)
    if previous_rows:
        _write_snapshot_recovery_outputs(
            report_path=recovery_md,
            csv_path=recovery_csv,
            previous_rows=previous_rows,
            current_rows=out_rows,
            start=args.date_from,
            end=args.date_to,
        )
    payload = {
        "status": "ok",
        "generated_at": _now(),
        "date_from": args.date_from,
        "date_to": args.date_to,
        "rows_examined": len(out_rows),
        "rows_resolved": sum(1 for row in out_rows if row.get("starter_identity_status") == "resolved"),
        "outputs": {
            "map_csv": _rel(map_csv),
            "report_md": _rel(report_md),
            "snapshot_recovery_md": _rel(recovery_md),
            "snapshot_recovery_csv": _rel(recovery_csv),
        },
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
