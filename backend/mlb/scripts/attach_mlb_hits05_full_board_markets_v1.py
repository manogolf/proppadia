#!/usr/bin/env python3
"""Append market observations to already-frozen full-board predictions only."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/hits05_full_board_shadow_v1/hits05_full_board_shadow_v1.sqlite3"
LINEAGE_ROOT = ROOT / "backend/mlb/exports/prospective_lineage"


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path.resolve())


def _dt(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _number(value: Any) -> float | None:
    try:
        value = float(value)
        return value if value == value else None
    except Exception:
        return None


def _american_probability(price: float | None) -> float | None:
    if price is None or price == 0:
        return None
    return 100.0 / (price + 100.0) if price > 0 else (-price) / ((-price) + 100.0)


def attach_date(slate_date: str, ledger_path: Path, lineage_path: Path | None = None) -> dict[str, Any]:
    path = lineage_path or LINEAGE_ROOT / slate_date / "prediction_lineage_ledger.csv"
    if not path.exists():
        return {"status": "NO_MARKET_LINEAGE_AVAILABLE", "slate_date": slate_date, "observations_added": 0}
    source_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    connection = ledger.connect_ledger(ledger_path)
    predictions = ledger.prediction_identities(connection, slate_date)
    added = existing = no_prediction = rejected = 0
    by_book: dict[str, int] = {}
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        try:
            identity = json.loads(row.get("canonical_row_identity") or "{}")
            if str(identity.get("prop_type", "")).lower() != "hits" or float(identity.get("line")) != 0.5:
                continue
            if str(identity.get("game_date")) != slate_date or row.get("lineage_status") != "LINEAGE_CERTIFIED":
                rejected += 1
                continue
            game_id, player_id = int(identity["game_id"]), int(identity["player_id"])
            canonical = ledger.canonical_identity(slate_date, game_id, player_id)
            if canonical not in predictions:
                no_prediction += 1
                continue
            observed = _dt(row.get("odds_snapshot_timestamp"))
            start = _dt(row.get("scheduled_game_start"))
            if observed is None or start is None or not observed < start:
                rejected += 1
                continue
            over, under = _number(row.get("price_over_american")), _number(row.get("price_under_american"))
            po, pu = _american_probability(over), _american_probability(under)
            no_vig = po / (po + pu) if po is not None and pu is not None and po + pu else None
            book = str(row.get("bookmaker_key") or "UNKNOWN").strip()
            snapshot_path = str(row.get("odds_snapshot_path") or "").strip() or _display_path(path)
            snapshot_hash = str(row.get("odds_snapshot_sha256") or "").strip() or source_hash
            payload = {
                "slate_date": slate_date,
                "game_id": game_id,
                "player_id": player_id,
                "bookmaker_key": book,
                "market_line": 0.5,
                "price_over_american": over,
                "price_under_american": under,
                "no_vig_probability_over": no_vig,
                "observation_timestamp_utc": observed.isoformat().replace("+00:00", "Z"),
                "scheduled_start_utc": start.isoformat().replace("+00:00", "Z"),
                "minutes_before_start": (start - observed).total_seconds() / 60.0,
                "snapshot_path": snapshot_path,
                "snapshot_sha256": snapshot_hash,
                "lineage_ledger_path": _display_path(path),
                "lineage_ledger_sha256_at_attachment": source_hash,
                "market_provider_origin_family": row.get("market_provider_origin_family") or "",
                "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "population_admission_dependency": False,
            }
            action = ledger.append_market_observation(connection, canonical, payload)
            if action == "APPENDED_NEW":
                added += 1
                by_book[book] = by_book.get(book, 0) + 1
            elif action == "EXISTING_IMMUTABLE":
                existing += 1
        except Exception:
            rejected += 1
    return {
        "status": "PASS",
        "slate_date": slate_date,
        "source_path": _display_path(path),
        "source_sha256": source_hash,
        "observations_added": added,
        "observations_existing": existing,
        "market_rows_without_full_board_prediction": no_prediction,
        "rejected_rows": rejected,
        "observations_added_by_book": by_book,
        "market_required_for_population": False,
        "ledger_counts": ledger.counts(connection),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--lineage", type=Path)
    args = parser.parse_args()
    print(json.dumps(attach_date(args.date, args.ledger, args.lineage), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
