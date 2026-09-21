#!/usr/bin/env python3
"""Build a database-free canonical MLB game-phase backfill proposal.

Inputs must be retained StatsAPI JSON files on disk.  This module has no
network or database client imports and emits only deterministic local files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.season_transition.canonical_phase_v1 import (
    CanonicalGamePhaseIndex,
    canonical_json,
    canonical_phase_record,
)


CONTRACT_NAME = "MLB_2026_CANONICAL_GAME_PHASE_ACTIVATION_V1"
DEFAULT_RETAINED_ROOT = Path("backend/mlb/exports/cleanroom_v1/raw/MLB_STATS_API")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def display_path(path: Path, repo_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        return str(path.resolve())


def discover_inputs(inputs: Iterable[Path]) -> list[Path]:
    discovered: set[Path] = set()
    for item in inputs:
        path = item.resolve()
        if path.is_file():
            discovered.add(path)
        elif path.is_dir():
            discovered.update(candidate.resolve() for candidate in path.rglob("schedule.json"))
        else:
            raise FileNotFoundError(f"RETAINED_INPUT_NOT_FOUND:{item}")
    if not discovered:
        raise RuntimeError("NO_RETAINED_SCHEDULE_FILES")
    return sorted(discovered, key=str)


def schedule_games(payload: Any, *, source_path: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise RuntimeError(f"STATSAPI_SCHEDULE_OBJECT_REQUIRED:{source_path}")
    dates = payload.get("dates")
    if not isinstance(dates, list):
        raise RuntimeError(f"STATSAPI_SCHEDULE_DATES_REQUIRED:{source_path}")
    games: list[dict[str, Any]] = []
    for block in dates:
        if not isinstance(block, dict) or not isinstance(block.get("games", []), list):
            raise RuntimeError(f"STATSAPI_SCHEDULE_GAMES_INVALID:{source_path}")
        for game in block.get("games", []):
            if not isinstance(game, dict):
                raise RuntimeError(f"STATSAPI_SCHEDULE_GAME_INVALID:{source_path}")
            games.append(game)
    return games


def build_proposal(paths: list[Path], *, repo_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    index = CanonicalGamePhaseIndex()
    sources: list[dict[str, Any]] = []
    source_game_rows = 0
    for path in paths:
        raw = path.read_bytes()
        digest = sha256_bytes(raw)
        source_path = display_path(path, repo_root)
        payload = json.loads(raw)
        games = schedule_games(payload, source_path=source_path)
        sources.append(
            {
                "source_path": source_path,
                "source_sha256": digest,
                "source_bytes": len(raw),
                "schedule_game_rows": len(games),
            }
        )
        source_game_rows += len(games)
        for game in games:
            index.add(
                canonical_phase_record(
                    game,
                    source_sha256=digest,
                    source_path=source_path,
                )
            )

    records = index.records()
    summary = {
        "contract_name": CONTRACT_NAME,
        "mode": "OFFLINE_RETAINED_STATSAPI_ONLY",
        "database_writes": 0,
        "network_requests": 0,
        "input_file_count": len(paths),
        "source_game_row_count": source_game_rows,
        "canonical_game_count": len(records),
        "consistent_duplicate_count": index.consistent_duplicate_count,
        "special_excluded_count": sum(row["season_phase"] is None for row in records),
        "phase_counts": {
            phase: sum(row["season_phase"] == phase for row in records)
            for phase in ("PRESEASON", "REGULAR_SEASON", "POSTSEASON")
        },
        "proposal_status": "READY_FOR_REVIEW_NOT_APPLIED",
    }
    return records, sources, summary


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.write_text("".join(canonical_json(row) + "\n" for row in rows), encoding="utf-8")


def write_outputs(
    output_dir: Path,
    records: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=False)
    proposal_path = output_dir / "canonical_game_phase_backfill_proposal.jsonl"
    sources_path = output_dir / "retained_source_manifest.jsonl"
    summary_path = output_dir / "proposal_summary.json"
    write_jsonl(proposal_path, records)
    write_jsonl(sources_path, sources)
    summary_path.write_text(canonical_json(summary) + "\n", encoding="utf-8")
    manifest_paths = (proposal_path, sources_path, summary_path)
    manifest = "".join(
        f"{sha256_bytes(path.read_bytes())}  {path.name}\n" for path in manifest_paths
    )
    (output_dir / "sha256_manifest.txt").write_text(manifest, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        action="append",
        dest="inputs",
        type=Path,
        help="Retained schedule.json file or directory; repeatable",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[3]
    inputs = args.inputs or [repo_root / DEFAULT_RETAINED_ROOT]
    paths = discover_inputs(inputs)
    records, sources, summary = build_proposal(paths, repo_root=repo_root)
    write_outputs(args.output_dir.resolve(), records, sources, summary)
    print(canonical_json(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
