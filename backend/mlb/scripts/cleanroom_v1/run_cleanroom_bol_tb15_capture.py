#!/usr/bin/env python3
"""Orchestrate one bounded, fail-closed clean-room BetOnline TB 1.5 capture."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[4]
PYTHON = ROOT / ".venv/bin/python"
RAW_ROOT = ROOT / "backend/mlb/exports/cleanroom_v1/raw"
BOARD_ROOT = ROOT / "backend/mlb/exports/cleanroom_v1/bol_tb15"
EVIDENCE_ROOT = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_cleanroom_bol_tb15_daily_lifecycle_certification"
)
REQUIRED_FILES = {
    "run_manifest.json",
    "bol_tb15_market_sides.csv",
    "bol_tb15_two_sided_markets.csv",
    "lineup_snapshot.csv",
    "identity_audit.csv",
    "source_hash_manifest.csv",
}
TERMINAL_GAME_STATES = {"Final", "Game Over", "Completed Early", "Postponed", "Cancelled"}


def generate_run_tag(at: datetime | None = None) -> str:
    return f"cleanroom_{(at or datetime.now(timezone.utc)):%Y%m%dT%H%M%SZ}"


def validate_date(value: str) -> date:
    parsed = date.fromisoformat(value)
    if str(parsed) != value:
        raise ValueError("MLB_DATE must use YYYY-MM-DD")
    return parsed


def require_credentials(environment: dict[str, str]) -> None:
    for name in ("SUPABASE_DB_URL", "ODDS_API_KEY"):
        if not environment.get(name):
            raise RuntimeError(f"missing required environment variable: {name}")


def ensure_paths_absent(paths: list[Path]) -> None:
    collisions = [path for path in paths if path.exists()]
    if collisions:
        raise RuntimeError(f"run-tag collision: {collisions[0]}")


def select_new_raw_run(day_root: Path, run_tag: str) -> Path:
    expected = day_root / run_tag
    candidates = [path for path in day_root.iterdir() if path.is_dir() and path.name == run_tag]
    if candidates != [expected]:
        raise RuntimeError("new raw-run location cannot be identified uniquely")
    return expected


def assert_one_event_per_game(bindings: list[dict]) -> None:
    admitted = [
        (row["provider_event_id"], row["game_pk"])
        for row in bindings if row["decision"] == "EXACT_UNIQUE_MATCH"
    ]
    if len({event for event, _ in admitted}) != len(admitted):
        raise RuntimeError("provider event reused across game bindings")
    if len({game for _, game in admitted}) != len(admitted):
        raise RuntimeError("multiple provider events bound to one official game")


def resolve_latest_completed_date(slate: date) -> date:
    for offset in range(1, 8):
        candidate = slate - timedelta(days=offset)
        response = requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": str(candidate)},
            timeout=45,
        )
        response.raise_for_status()
        games = [
            game
            for block in response.json().get("dates", [])
            for game in block.get("games", [])
        ]
        if games and all(
            game.get("status", {}).get("detailedState") in TERMINAL_GAME_STATES
            for game in games
        ):
            return candidate
    raise RuntimeError("no completed MLB slate found in preceding seven days")


def run_module(module: str, arguments: list[str], accepted=(0,)) -> dict:
    completed = subprocess.run(
        [str(PYTHON), "-u", "-m", module, *arguments],
        cwd=ROOT,
        env=os.environ.copy(),
        text=True,
        capture_output=True,
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode not in accepted:
        raise RuntimeError(f"{module} failed with exit code {completed.returncode}")
    lines = [line for line in completed.stdout.splitlines() if line.strip().startswith("{")]
    if not lines:
        raise RuntimeError(f"{module} did not emit a JSON result")
    return json.loads(lines[-1])


def validate_snapshot(snapshot: Path) -> dict:
    if {path.name for path in snapshot.iterdir() if path.is_file()} < REQUIRED_FILES:
        missing = sorted(REQUIRED_FILES - {path.name for path in snapshot.iterdir()})
        raise RuntimeError(f"snapshot missing required files: {missing}")
    manifest = json.loads((snapshot / "run_manifest.json").read_text())
    if manifest["identity_rejects"]:
        raise RuntimeError("TB 1.5 identity rejects present")
    for row in csv.DictReader((snapshot / "source_hash_manifest.csv").open()):
        payload = Path(row["raw_payload_path"])
        if not payload.exists():
            raise RuntimeError(f"missing source payload: {payload}")
        actual = hashlib.sha256(payload.read_bytes()).hexdigest()
        if actual != row["sha256"]:
            raise RuntimeError(f"SHA-256 mismatch: {payload}")
    return manifest


def atomic_publish(staged_board: Path, board_dir: Path, staged_index: Path, run_index: Path) -> None:
    for name in (
        f"bol_tb15_cleanroom_market_board_{board_dir.name}.csv",
        f"bol_tb15_cleanroom_market_board_{board_dir.name}.md",
        "population_manifest.json",
    ):
        if not (staged_board / name).is_file():
            raise RuntimeError(f"staged board missing {name}")
    run_index.parent.mkdir(parents=True, exist_ok=True)
    index_publish = run_index.with_name(f".{run_index.name}.publish")
    shutil.copy2(staged_index, index_publish)
    os.replace(index_publish, run_index)
    board_dir.mkdir(parents=True, exist_ok=True)
    for source in staged_board.iterdir():
        if source.is_file():
            os.replace(source, board_dir / source.name)


def game_coverage(pilot_dir: Path, slate: date) -> tuple[list[dict], list[dict]]:
    schedule = json.loads((pilot_dir / "raw/MLB_STATS_API" / f"schedule_{slate}.json").read_text())
    games = [
        {
            "game_pk": game["gamePk"],
            "away": game["teams"]["away"]["team"]["name"],
            "home": game["teams"]["home"]["team"]["name"],
            "start": game["gameDate"],
        }
        for block in schedule.get("dates", [])
        for game in block.get("games", [])
    ]
    bindings = list(csv.DictReader((pilot_dir / "provider_event_to_game_pk_audit.csv").open()))
    return games, bindings


def main() -> int:
    parser = argparse.ArgumentParser(
        epilog=(
            "Bounded source capture only: does not run models, incumbent predictions, "
            "ranking, Quick Card, uploads, Ops Brief, or historical reconciliation."
        )
    )
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    slate = validate_date(args.date)
    require_credentials(os.environ)

    completed_date = resolve_latest_completed_date(slate)
    run_tag = generate_run_tag()
    if not re.fullmatch(r"cleanroom_\d{8}T\d{6}Z", run_tag):
        raise RuntimeError("invalid generated run tag")
    snapshot = BOARD_ROOT / args.date / "snapshots" / run_tag
    ensure_paths_absent([snapshot])
    pilot_dir = EVIDENCE_ROOT / args.date / "runs" / run_tag
    if pilot_dir.exists():
        raise RuntimeError(f"evidence collision: {pilot_dir}")

    with tempfile.TemporaryDirectory(prefix=f"{run_tag}_") as temporary:
        temp = Path(temporary)
        source_result = run_module(
            "backend.mlb.scripts.cleanroom_v1.run_cleanroom_source_cycle",
            [
                "--date", args.date,
                "--completed-date", str(completed_date),
                "--run-tag", run_tag,
                "--evidence-dir", str(temp / "source_evidence"),
            ],
            accepted=(0, 2),
        )
        if source_result.get("run_tag") != run_tag:
            raise RuntimeError("source acquisition returned the wrong run tag")
        raw_run = select_new_raw_run(RAW_ROOT / "THE_ODDS_API" / args.date, run_tag)
        isolated_root = temp / "isolated_odds"
        isolated_root.mkdir()
        (isolated_root / run_tag).symlink_to(raw_run.resolve(), target_is_directory=True)

        pilot_result = run_module(
            "backend.mlb.scripts.cleanroom_v1.pilot_exact_game_roster_identity",
            [
                "--date", args.date, "--line", "1.5",
                "--odds-root", str(isolated_root),
                "--output-dir", str(pilot_dir),
            ],
        )
        if (
            pilot_result.get("ambiguous")
            or pilot_result.get("event_binding_failures")
            or pilot_result.get("unmatched")
            or not pilot_result.get("certifiable")
        ):
            raise RuntimeError(f"identity certification failed: {pilot_result}")

        admitted = run_module(
            "backend.mlb.scripts.cleanroom_v1.admit_exact_roster_bridge",
            ["--pilot-dir", str(pilot_dir), "--board-root", str(temp / "provisional_board")],
        )
        run_id = admitted.get("run_id")
        if not run_id or admitted.get("identity_rejects"):
            raise RuntimeError("admitted ingestion ID missing or identity rejects present")

        board_dir = BOARD_ROOT / args.date
        run_index = EVIDENCE_ROOT / args.date / "cleanroom_intraday_run_index.csv"
        staged_index = temp / "cleanroom_intraday_run_index.csv"
        if run_index.exists():
            shutil.copy2(run_index, staged_index)
        staged_board = temp / "board"
        run_module(
            "backend.mlb.scripts.cleanroom_v1.materialize_capture_snapshot",
            [
                "--date", args.date, "--run-id", run_id, "--run-tag", run_tag,
                "--pilot-dir", str(pilot_dir), "--output-dir", str(snapshot),
                "--run-index", str(staged_index), "--board-dir", str(staged_board),
            ],
        )
        manifest = validate_snapshot(snapshot)
        if not staged_index.is_file():
            raise RuntimeError("run-index append failed")
        atomic_publish(staged_board, board_dir, staged_index, run_index)

    games, bindings = game_coverage(pilot_dir, slate)
    assert_one_event_per_game(bindings)
    bound_pks = {int(row["game_pk"]) for row in bindings if row["decision"] == "EXACT_UNIQUE_MATCH"}
    missing_games = [game for game in games if game["game_pk"] not in bound_pks]
    eastern = ZoneInfo("America/New_York")
    pacific = ZoneInfo("America/Los_Angeles")
    source_at = datetime.strptime(run_tag.removeprefix("cleanroom_"), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    admitted_at = datetime.fromisoformat(manifest["capture_timestamp_utc"])
    summary = {
        **manifest,
        "source_capture_timestamp_utc": source_at.isoformat(),
        "source_capture_timestamp_pt": source_at.astimezone(pacific).isoformat(),
        "admitted_snapshot_timestamp_pt": admitted_at.astimezone(pacific).isoformat(),
        "official_games_without_provider_events": len(missing_games),
        "missing_provider_games": [
            f"{game['away']} @ {game['home']} game_pk={game['game_pk']}" for game in missing_games
        ],
        "snapshot_path": str(snapshot.relative_to(ROOT)),
        "neutral_board_path": str((BOARD_ROOT / args.date /
            f"bol_tb15_cleanroom_market_board_{args.date}.md").relative_to(ROOT)),
    }
    print(json.dumps(summary, indent=2))
    mets_braves = [
        game for game in games
        if {game["away"], game["home"]} == {"Atlanta Braves", "New York Mets"}
    ]
    for number, game in enumerate(sorted(mets_braves, key=lambda row: row["start"]), 1):
        start = datetime.fromisoformat(game["start"].replace("Z", "+00:00"))
        event = next((row["provider_event_id"] for row in bindings
                      if row["game_pk"] == str(game["game_pk"])), "UNBOUND")
        print(
            f"ATL-NYM Game {number}: game_pk={game['game_pk']} "
            f"start_utc={start.isoformat()} start_et={start.astimezone(eastern).isoformat()} "
            f"start_pt={start.astimezone(pacific).isoformat()} provider_event={event}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
