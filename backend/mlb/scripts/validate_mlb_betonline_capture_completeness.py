"""Validate retained BetOnline MLB player-prop capture completeness.

Read-only utility. It inspects preserved OddsAPI payloads and reports whether
each governed BetOnline MLB market is present in each snapshot.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.mlb.shared.betonline_market_registry import active_market_rows


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ODDS_ROOT = REPO_ROOT / "backend/mlb/exports/odds_history"
DEFAULT_OUT_DIR = (
    REPO_ROOT
    / "artifacts/analysis/model_development/mlb_betonline_oddsapi_capture_coverage_repair/2026-07-18"
)

STATUS_VALUES = {
    "BETONLINE_MARKET_PRESENT",
    "BETONLINE_BOOK_PRESENT_MARKET_ABSENT",
    "BETONLINE_BOOK_ABSENT",
    "REQUEST_BATCH_FAILED",
    "MARKET_NOT_REQUESTED",
    "PARSER_DROPPED_ROWS",
    "NO_EVENTS_ELIGIBLE",
    "STATUS_UNRESOLVED",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _events_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("events"), list):
        return [ev for ev in payload["events"] if isinstance(ev, dict)]
    if isinstance(payload, list):
        return [ev for ev in payload if isinstance(ev, dict)]
    return []


def _is_betonline(book: dict[str, Any]) -> bool:
    text = f"{book.get('key') or ''} {book.get('title') or ''}".lower()
    return "betonline" in text or str(book.get("key") or "").lower() == "betonlineag"


def _slate_date_for(path: Path, payload: Any) -> str:
    if path.parent.name[:2] == "20":
        return path.parent.name
    value = ""
    if isinstance(payload, dict):
        value = str(payload.get("game_date") or payload.get("slate_date") or "")
    return value


def _captured_at(payload: Any) -> str:
    if isinstance(payload, dict):
        return str(payload.get("captured_at_utc") or payload.get("capture_timestamp_utc") or "")
    return ""


def _iter_snapshot_paths(paths: list[Path], odds_root: Path, date: str | None) -> list[Path]:
    if paths:
        out: list[Path] = []
        for p in paths:
            if p.is_dir():
                out.extend(sorted(p.glob("odds_mlb_playerprops*.json")))
                out.extend(sorted(p.glob("odds_latest_compatible.json")))
            elif p.exists():
                out.append(p)
        return sorted(dict.fromkeys(out))
    root = odds_root / date if date else odds_root
    if root.is_file():
        return [root]
    if root.is_dir() and date:
        return sorted(
            list(root.glob("odds_mlb_playerprops*.json"))
            + list(root.glob("odds_latest_compatible.json"))
        )
    return sorted(odds_root.glob("20??-??-??/odds_mlb_playerprops*.json"))


def validate_snapshot(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    try:
        payload = read_json(path)
    except Exception as exc:
        return (
            [
                {
                    "source_path": str(path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path),
                    "parse_status": "FAIL",
                    "completeness_status": "STATUS_UNRESOLVED",
                    "notes": f"{type(exc).__name__}: {exc}",
                }
            ],
            {"parse_status": "FAIL", "error": f"{type(exc).__name__}: {exc}"},
        )

    events = _events_from_payload(payload)
    sha = sha256_file(path)
    source = str(path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path)
    captured = _captured_at(payload)
    slate_date = _slate_date_for(path, payload)

    rows: list[dict[str, Any]] = []
    betonline_book_count = 0
    betonline_market_keys: set[str] = set()
    for ev in events:
        for book in ev.get("bookmakers", []) or []:
            if not isinstance(book, dict) or not _is_betonline(book):
                continue
            betonline_book_count += 1
            for market in book.get("markets", []) or []:
                if isinstance(market, dict):
                    key = str(market.get("key") or "").strip()
                    if key:
                        betonline_market_keys.add(key)

    for market in active_market_rows():
        raw_key = market["oddsapi_key"]
        games: set[str] = set()
        outcome_rows = 0
        side_values: set[str] = set()
        player_values: set[str] = set()
        for ev in events:
            game_id = str(ev.get("id") or "")
            for book in ev.get("bookmakers", []) or []:
                if not isinstance(book, dict) or not _is_betonline(book):
                    continue
                for item in book.get("markets", []) or []:
                    if not isinstance(item, dict) or str(item.get("key") or "") != raw_key:
                        continue
                    games.add(game_id)
                    for outcome in item.get("outcomes", []) or []:
                        if not isinstance(outcome, dict):
                            continue
                        outcome_rows += 1
                        side = str(outcome.get("name") or "").strip().lower()
                        if side:
                            side_values.add(side)
                        desc = str(outcome.get("description") or "").strip()
                        if desc:
                            player_values.add(desc)

        if not events:
            status = "NO_EVENTS_ELIGIBLE"
        elif betonline_book_count <= 0:
            status = "BETONLINE_BOOK_ABSENT"
        elif raw_key not in betonline_market_keys:
            status = "BETONLINE_BOOK_PRESENT_MARKET_ABSENT"
        elif outcome_rows <= 0:
            status = "STATUS_UNRESOLVED"
        else:
            status = "BETONLINE_MARKET_PRESENT"

        rows.append(
            {
                "slate_date": slate_date,
                "source_path": source,
                "payload_sha256": sha,
                "captured_at_utc": captured,
                "parse_status": "PASS",
                "local_prop_type": market["local_prop_type"],
                "raw_market_key": raw_key,
                "prop_family": market["prop_family"],
                "endpoint_family": market["endpoint_family"],
                "batching_group": market["batching_group"],
                "event_count": len(events),
                "betonline_book_event_count": betonline_book_count,
                "games_with_market": len(games),
                "players_with_market": len(player_values),
                "outcome_rows": outcome_rows,
                "side_values": "|".join(sorted(side_values)),
                "completeness_status": status,
                "notes": "",
            }
        )
    summary = {
        "parse_status": "PASS",
        "source_path": source,
        "payload_sha256": sha,
        "slate_date": slate_date,
        "captured_at_utc": captured,
        "event_count": len(events),
        "betonline_book_event_count": betonline_book_count,
        "markets_present": sorted(betonline_market_keys),
    }
    return rows, summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default="", help="Optional YYYY-MM-DD odds_history date.")
    parser.add_argument("--snapshot", action="append", default=[], help="Snapshot file or directory to validate.")
    parser.add_argument("--odds-root", default=str(DEFAULT_ODDS_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--mode", default="read_only", choices=["read_only", "dry_run"])
    args = parser.parse_args()

    paths = _iter_snapshot_paths(
        [Path(p) for p in args.snapshot],
        Path(args.odds_root),
        args.date.strip() or None,
    )
    all_rows: list[dict[str, Any]] = []
    source_summaries: list[dict[str, Any]] = []
    for path in paths:
        rows, summary = validate_snapshot(path)
        all_rows.extend(rows)
        source_summaries.append(summary)

    out_dir = Path(args.output_dir)
    today = datetime.now(timezone.utc).date().isoformat()
    trace_path = out_dir / f"betonline_capture_completeness_validation_{today}.csv"
    json_path = out_dir / f"machine_readable_betonline_capture_completeness_validation_{today}.json"
    fields = [
        "slate_date",
        "source_path",
        "payload_sha256",
        "captured_at_utc",
        "parse_status",
        "local_prop_type",
        "raw_market_key",
        "prop_family",
        "endpoint_family",
        "batching_group",
        "event_count",
        "betonline_book_event_count",
        "games_with_market",
        "players_with_market",
        "outcome_rows",
        "side_values",
        "completeness_status",
        "notes",
    ]
    write_csv(trace_path, all_rows, fields)
    write_json(
        json_path,
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "mode": args.mode,
            "snapshots_checked": len(paths),
            "rows": len(all_rows),
            "status_values": sorted(STATUS_VALUES),
            "source_summaries": source_summaries,
            "trace_csv": str(trace_path.relative_to(REPO_ROOT) if trace_path.is_relative_to(REPO_ROOT) else trace_path),
        },
    )
    print(json.dumps({"trace_csv": str(trace_path), "machine_json": str(json_path), "rows": len(all_rows)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
