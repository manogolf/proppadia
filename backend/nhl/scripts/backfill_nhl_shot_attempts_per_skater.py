#!/usr/bin/env python3
"""Backfill NULL 2026 shot attempts with the per-skater rule (P2.37).

Default mode is a read-only preview: it lists 2026 regular-season skater-log
rows whose ``shot_attempts`` is NULL (research login), reads each game's
official boxscore and play-by-play (retained diagnostics first, otherwise the
free NHL Gamecenter API), applies
``points_attempts_source.reconcile_attempts_per_skater`` and writes a receipt
under ``artifacts/analysis/nhl/points_attempts_source_validation/per_skater_backfill_<stamp>/``.

``--commit --preview-sha256 <sha>`` (operator-approved only) re-runs the
governed path per affected date with the ingestion login: the daily skater
seeder stages the date, then ``nhl.promote_skater_game_log_protected_fields``
fills NULLs only. Populated values are never changed. Each date is verified
read-only against the preview afterwards.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import requests

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / "backend/.env")  # authoritative environment; never printed
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.points_attempts_source import reconcile_attempts_per_skater  # noqa: E402

OUT_ROOT = ROOT / "artifacts/analysis/nhl/points_attempts_source_validation"
DIAGNOSTICS = ROOT / "artifacts/operational/nhl/shot_attempts_source_diagnostics"
GAMECENTER = "https://api-web.nhle.com/v1/gamecenter"
FIELDS = ["game_date", "game_id", "player_id", "status", "official_attempts", "reason",
          "boxscore_sha256", "pbp_sha256", "source"]

NULL_ROWS_SQL = """
SELECT game_date::text, game_id::bigint, player_id::bigint
  FROM nhl.skater_game_logs_raw
 WHERE shot_attempts IS NULL
   AND game_date >= DATE '2026-09-01'
   AND game_id::text LIKE '202602%%'
 ORDER BY game_date, game_id, player_id
"""


def sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha_file(path: Path) -> str:
    return sha_bytes(path.read_bytes())


def retained_sources(game_id: int) -> tuple[dict, dict, str] | None:
    """Latest retained failed-source boxscore and PBP for a game, if any."""
    boxes = sorted(DIAGNOSTICS.glob(f"slate_date=*/run_id=*/failed_source_{game_id}_boxscore.json"),
                   key=lambda p: p.stat().st_mtime_ns)
    for box_path in reversed(boxes):
        pbp_path = box_path.with_name(f"failed_source_{game_id}_play_by_play.json")
        if pbp_path.is_file():
            return (json.loads(box_path.read_text()), json.loads(pbp_path.read_text()),
                    str(box_path.parent.relative_to(ROOT)))
    return None


def fetch_sources(game_id: int, raw_dir: Path) -> tuple[dict, dict, str]:
    """Official boxscore and PBP from the free Gamecenter API, retained raw."""
    payloads = []
    for endpoint in ("boxscore", "play-by-play"):
        response = requests.get(f"{GAMECENTER}/{game_id}/{endpoint}", timeout=25)
        response.raise_for_status()
        path = raw_dir / f"{game_id}_{endpoint}.json"
        path.write_bytes(response.content)
        payloads.append(json.loads(response.content))
    return payloads[0], payloads[1], str(raw_dir.relative_to(ROOT))


def preview() -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = OUT_ROOT / f"per_skater_backfill_{stamp}"
    raw_dir = out / "official_raw_responses"
    raw_dir.mkdir(parents=True)
    with psycopg.connect(database_url("research")) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute(NULL_ROWS_SQL)
        null_rows = cursor.fetchall()
    by_game: dict[int, list[tuple[str, int]]] = {}
    for game_date, game_id, player_id in null_rows:
        by_game.setdefault(int(game_id), []).append((game_date, int(player_id)))
    rows, game_results = [], []
    for game_id, members in sorted(by_game.items()):
        game_date = members[0][0]
        try:
            sources = retained_sources(game_id) or fetch_sources(game_id, raw_dir)
            box, pbp, source = sources
            box_sha = sha_bytes(json.dumps(box, sort_keys=True).encode())
            pbp_sha = sha_bytes(json.dumps(pbp, sort_keys=True).encode())
            result = reconcile_attempts_per_skater(box, pbp, game_id=game_id)
            game_error = None
        except Exception as error:  # the whole game stays unknown
            result, box_sha, pbp_sha, source = None, "", "", ""
            game_error = str(error).split(":", 1)[0] or type(error).__name__
        for _date, player_id in members:
            if result is None:
                status, value, reason = "UNRESOLVED_GAME", "", game_error
            elif player_id in result["attempts"]:
                status, value, reason = "FILLABLE", result["attempts"][player_id], ""
            elif player_id in result["unresolved"]:
                status, value, reason = "UNRESOLVED_SKATER", "", result["unresolved"][player_id]
            else:
                status, value, reason = "UNRESOLVED_NOT_IN_BOXSCORE", "", ""
            rows.append({"game_date": game_date, "game_id": game_id, "player_id": player_id,
                         "status": status, "official_attempts": value, "reason": reason,
                         "boxscore_sha256": box_sha, "pbp_sha256": pbp_sha, "source": source})
        game_results.append({"game_id": game_id, "game_date": game_date, "null_rows": len(members),
                             "fillable": sum(r["game_id"] == game_id and r["status"] == "FILLABLE" for r in rows),
                             "game_error": game_error, "source": source})
    candidates = out / "candidates.csv"
    with candidates.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    status = Counter(r["status"] for r in rows)
    fillable = [r for r in rows if r["status"] == "FILLABLE"]
    summary = {
        "schema_version": "NHL_SHOT_ATTEMPTS_PER_SKATER_BACKFILL_PREVIEW_V1",
        "generated_at_utc": stamp, "rule": "points_attempts_source.reconcile_attempts_per_skater",
        "database_identity": "research (read-only transaction)", "database_writes": 0,
        "null_rows": len(rows), "games": len(by_game), "status_counts": dict(status),
        "fillable_positive": sum(int(r["official_attempts"]) > 0 for r in fillable),
        "fillable_zero": sum(int(r["official_attempts"]) == 0 for r in fillable),
        "fillable_by_date": dict(sorted(Counter(r["game_date"] for r in fillable).items())),
        "unresolved_reasons": dict(Counter(r["reason"] for r in rows if r["status"] != "FILLABLE")),
        "games_detail": game_results,
        "candidates_sha256": sha_file(candidates),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    sums = [f"{sha_file(path)}  {path.relative_to(out)}" for path in sorted(out.rglob("*"))
            if path.is_file() and path.name != "SHA256SUMS"]
    (out / "SHA256SUMS").write_text("\n".join(sums) + "\n")
    print(json.dumps({key: summary[key] for key in (
        "null_rows", "games", "status_counts", "fillable_positive", "fillable_zero",
        "unresolved_reasons", "candidates_sha256")}, indent=2, sort_keys=True))
    print(f"PREVIEW={out.relative_to(ROOT)}")
    return out


def commit(preview_dir: Path, expected_sha: str) -> None:
    candidates = preview_dir / "candidates.csv"
    if sha_file(candidates) != expected_sha:
        raise SystemExit("PREVIEW_SHA256_MISMATCH")
    fillable = [r for r in csv.DictReader(candidates.open()) if r["status"] == "FILLABLE"]
    dates = sorted({r["game_date"] for r in fillable})
    receipts = []
    for game_date in dates:
        env = {**os.environ, "SLATE_DATE": game_date,
               "NHL_DAILY_RUN_ID": f"nhl_attempts_backfill_{preview_dir.name}"}
        seed = subprocess.run([sys.executable, str(ROOT / "backend/nhl/scripts/seed_skater_logs_for_date.py")],
                              cwd=ROOT, env=env, capture_output=True, text=True)
        filled = None
        if seed.returncode == 0:
            with psycopg.connect(database_url("ingestion")) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT nhl.promote_skater_game_log_protected_fields(%s::date)", (game_date,))
                filled = cursor.fetchone()[0]
        expected = {(int(r["game_id"]), int(r["player_id"])): int(r["official_attempts"])
                    for r in fillable if r["game_date"] == game_date}
        with psycopg.connect(database_url("research")) as connection, connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("SELECT game_id::bigint, player_id::bigint, shot_attempts FROM nhl.skater_game_logs_raw "
                           "WHERE game_date=%s::date", (game_date,))
            actual = {(int(g), int(p)): a for g, p, a in cursor.fetchall()}
        mismatched = sum(actual.get(key) != value for key, value in expected.items())
        receipts.append({"game_date": game_date, "seed_exit": seed.returncode, "filled_nulls": filled,
                         "expected_fills": len(expected), "preview_mismatches": mismatched,
                         "seed_stderr_tail": seed.stderr[-500:]})
        print(json.dumps(receipts[-1], sort_keys=True))
    (preview_dir / "commit_receipts.json").write_text(json.dumps(receipts, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", action="store_true", help="operator-approved write pass")
    parser.add_argument("--preview", type=Path, help="preview folder for --commit")
    parser.add_argument("--preview-sha256", help="candidates.csv SHA-256 from the preview")
    args = parser.parse_args()
    if args.commit:
        if not args.preview or not args.preview_sha256:
            raise SystemExit("--commit needs --preview and --preview-sha256")
        commit(args.preview, args.preview_sha256)
    else:
        preview()


if __name__ == "__main__":
    main()
