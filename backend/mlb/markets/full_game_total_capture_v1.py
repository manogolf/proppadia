"""Append-only capture and exact-game binding for full-game MLB totals."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

EXPERIMENT = "MLB_FULL_GAME_TOTAL_MARKET_CAPTURE_V1"
MARKET_TYPE = "FULL_GAME_TOTAL"
ATTACHMENT_POLICY = "LATEST_CERTIFIED_AT_OR_BEFORE_PREDICTION_ELSE_EARLIEST_LATER_PREGAME"
ALL_BOOK_ATTACHMENT_POLICY = "ALL_CERTIFIED_PREGAME_BOOK_SNAPSHOTS"
CONSENSUS_POLICY = "MEDIAN_LINE_SAME_LINE_MEDIAN_PRICES_AND_NO_VIG"


def utc(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def normalize_team(value: str) -> str:
    return "".join(ch for ch in str(value or "").casefold() if ch.isalnum())


def sha256_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def connect_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS full_game_total_market_snapshots (
      canonical_market_identity TEXT PRIMARY KEY,
      game_date TEXT NOT NULL,
      game_id INTEGER NOT NULL,
      bookmaker_key TEXT NOT NULL,
      market_type TEXT NOT NULL,
      captured_at_utc TEXT NOT NULL,
      total_line REAL NOT NULL,
      market_payload_json TEXT NOT NULL,
      market_payload_sha256 TEXT NOT NULL,
      raw_source_path TEXT NOT NULL,
      raw_source_sha256 TEXT NOT NULL,
      UNIQUE(game_id, bookmaker_key, market_type, captured_at_utc, total_line)
    );
    CREATE TABLE IF NOT EXISTS totals_shadow_market_bridge (
      canonical_bridge_identity TEXT PRIMARY KEY,
      prediction_identity TEXT NOT NULL,
      market_identity TEXT NOT NULL REFERENCES full_game_total_market_snapshots(canonical_market_identity),
      attachment_policy TEXT NOT NULL,
      timing_relationship TEXT NOT NULL,
      bridge_payload_json TEXT NOT NULL,
      bridge_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL,
      UNIQUE(prediction_identity, market_identity, attachment_policy)
    );
    CREATE TABLE IF NOT EXISTS totals_shadow_all_book_market_bridge (
      canonical_bridge_identity TEXT PRIMARY KEY,
      prediction_identity TEXT NOT NULL,
      market_identity TEXT NOT NULL REFERENCES full_game_total_market_snapshots(canonical_market_identity),
      attachment_policy TEXT NOT NULL,
      timing_relationship TEXT NOT NULL,
      bridge_payload_json TEXT NOT NULL,
      bridge_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL,
      UNIQUE(prediction_identity, market_identity, attachment_policy)
    );
    CREATE TABLE IF NOT EXISTS totals_shadow_market_consensus (
      canonical_consensus_identity TEXT PRIMARY KEY,
      prediction_identity TEXT NOT NULL,
      captured_at_utc TEXT NOT NULL,
      consensus_policy TEXT NOT NULL,
      consensus_payload_json TEXT NOT NULL,
      consensus_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL,
      UNIQUE(prediction_identity, captured_at_utc, consensus_policy)
    );
    CREATE TRIGGER IF NOT EXISTS total_market_no_update BEFORE UPDATE ON full_game_total_market_snapshots
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_TOTAL_MARKET_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS total_market_no_delete BEFORE DELETE ON full_game_total_market_snapshots
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_TOTAL_MARKET_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS total_market_bridge_no_update BEFORE UPDATE ON totals_shadow_market_bridge
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_TOTAL_MARKET_BRIDGE'); END;
    CREATE TRIGGER IF NOT EXISTS total_market_bridge_no_delete BEFORE DELETE ON totals_shadow_market_bridge
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_TOTAL_MARKET_BRIDGE'); END;
    CREATE TRIGGER IF NOT EXISTS total_market_all_book_bridge_no_update BEFORE UPDATE ON totals_shadow_all_book_market_bridge
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_ALL_BOOK_MARKET_BRIDGE'); END;
    CREATE TRIGGER IF NOT EXISTS total_market_all_book_bridge_no_delete BEFORE DELETE ON totals_shadow_all_book_market_bridge
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_ALL_BOOK_MARKET_BRIDGE'); END;
    CREATE TRIGGER IF NOT EXISTS total_market_consensus_no_update BEFORE UPDATE ON totals_shadow_market_consensus
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_TOTAL_MARKET_CONSENSUS'); END;
    CREATE TRIGGER IF NOT EXISTS total_market_consensus_no_delete BEFORE DELETE ON totals_shadow_market_consensus
      BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_TOTAL_MARKET_CONSENSUS'); END;
    """)
    conn.commit()
    return conn


def bind_event(event: dict[str, Any], schedule: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str, list[int]]:
    try:
        event_start = utc(event["commence_time"])
    except Exception:
        return None, "PROVIDER_START_INVALID", []
    candidates = [
        game for game in schedule
        if normalize_team(game.get("away_team_name")) == normalize_team(event.get("away_team"))
        and normalize_team(game.get("home_team_name")) == normalize_team(event.get("home_team"))
        and abs((utc(game["scheduled_start_utc"]) - event_start).total_seconds()) <= 600
    ]
    ids = [int(game["game_pk"]) for game in candidates]
    if len(candidates) != 1:
        return None, "EVENT_IDENTITY_AMBIGUOUS", ids
    return candidates[0], "EXACT_UNIQUE_MATCH", ids


def parse_totals(
    *, events: Iterable[dict[str, Any]], schedule: list[dict[str, Any]], game_date: str,
    captured_at_utc: str, source_run_tag: str, raw_source_path: str, raw_source_sha256: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    captured = utc(captured_at_utc)
    rows: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    for event in events:
        game, decision, candidate_ids = bind_event(event, schedule)
        audit_row = {
            "provider_event_id": event.get("id"), "provider_away_team": event.get("away_team"),
            "provider_home_team": event.get("home_team"), "provider_start_utc": event.get("commence_time"),
            "candidate_game_pks": "|".join(map(str, candidate_ids)), "candidate_count": len(candidate_ids),
            "game_pk": int(game["game_pk"]) if game else None, "identity_certification": decision,
        }
        if not game:
            audit_row["admission_status"] = "REJECTED_IDENTITY"
            audit.append(audit_row)
            continue
        start = utc(game["scheduled_start_utc"])
        if captured >= start:
            audit_row["admission_status"] = "REJECTED_POST_START"
            audit.append(audit_row)
            continue
        event_rows = 0
        for book in event.get("bookmakers", []) or []:
            for market in book.get("markets", []) or []:
                if market.get("key") != "totals":
                    continue
                by_line: dict[float, dict[str, Any]] = {}
                for outcome in market.get("outcomes", []) or []:
                    try:
                        line = float(outcome["point"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    side = str(outcome.get("name") or "").casefold()
                    if side not in {"over", "under"}:
                        continue
                    by_line.setdefault(line, {})[side] = outcome.get("price")
                for line, sides in sorted(by_line.items()):
                    status = "TOTAL_MARKET_CERTIFIED_PAIRED" if sides.get("over") is not None and sides.get("under") is not None else "TOTAL_MARKET_LINE_ONLY"
                    market_timestamp = market.get("last_update") or captured_at_utc
                    row = {
                        "experiment": EXPERIMENT, "league": "MLB", "game_date": game_date,
                        "game_id": int(game["game_pk"]), "away_team": game["away_team_name"], "home_team": game["home_team_name"],
                        "scheduled_start_utc": game["scheduled_start_utc"], "bookmaker": book.get("title"),
                        "bookmaker_key": book.get("key"), "market_type": MARKET_TYPE, "total_line": line,
                        "over_price": sides.get("over"), "under_price": sides.get("under"),
                        "captured_at_utc": captured_at_utc, "provider_market_timestamp_utc": market_timestamp,
                        "lead_time_minutes": (start - captured).total_seconds() / 60.0,
                        "provider_event_id": event.get("id"), "provider_market_id": market.get("id"),
                        "source_run_tag": source_run_tag, "raw_source_path": raw_source_path,
                        "raw_source_sha256": raw_source_sha256, "identity_certification": decision,
                        "timing_status": "PREGAME_CERTIFIED", "market_status": status,
                    }
                    identity = f"{row['game_id']}|{row['bookmaker_key']}|{MARKET_TYPE}|{captured_at_utc}|{line:g}"
                    row["canonical_market_identity"] = identity
                    rows.append(row)
                    event_rows += 1
        audit_row["admission_status"] = "ADMITTED_MARKET_ROWS" if event_rows else "NO_TOTALS_MARKET"
        audit_row["market_rows"] = event_rows
        audit.append(audit_row)
    return rows, audit


def append_market(conn: sqlite3.Connection, row: dict[str, Any]) -> str:
    identity = row["canonical_market_identity"]
    digest = sha256_json(row)
    existing = conn.execute("SELECT market_payload_sha256 FROM full_game_total_market_snapshots WHERE canonical_market_identity=?", (identity,)).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute("INSERT INTO full_game_total_market_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)", (
        identity, row["game_date"], row["game_id"], row["bookmaker_key"], MARKET_TYPE,
        row["captured_at_utc"], row["total_line"], json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        digest, row["raw_source_path"], row["raw_source_sha256"],
    ))
    conn.commit()
    return "APPENDED_NEW"


def market_rows(conn: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    data = conn.execute("SELECT market_payload_json FROM full_game_total_market_snapshots WHERE game_date=? ORDER BY captured_at_utc,game_id,bookmaker_key,total_line", (game_date,)).fetchall()
    return [json.loads(row[0]) for row in data]


def attach_market(conn: sqlite3.Connection, prediction: dict[str, Any], markets: list[dict[str, Any]], created_at_utc: str) -> tuple[dict[str, Any] | None, str]:
    candidates = [row for row in markets if int(row["game_id"]) == int(prediction["game_pk"]) and utc(row["captured_at_utc"]) < utc(row["scheduled_start_utc"])]
    if not candidates:
        return None, "MARKET_UNAVAILABLE"
    book_keys = sorted({str(row.get("bookmaker_key") or "") for row in candidates})
    selected_book = "betonlineag" if "betonlineag" in book_keys else book_keys[0]
    candidates = [row for row in candidates if str(row.get("bookmaker_key") or "") == selected_book]
    prediction_time = utc(prediction["prediction_timestamp_utc"])
    before = [row for row in candidates if utc(row["captured_at_utc"]) <= prediction_time]
    selected = max(before, key=lambda row: row["captured_at_utc"]) if before else min(candidates, key=lambda row: row["captured_at_utc"])
    timing = "AT_OR_BEFORE_PREDICTION" if before else "POST_PREDICTION_MARKET_OBSERVATION"
    bridge = {
        "prediction_identity": f"{prediction['game_date']}|{prediction['game_pk']}|{prediction['model_version']}|{prediction['prediction_snapshot_class']}",
        "market_identity": selected["canonical_market_identity"], "attachment_policy": ATTACHMENT_POLICY,
        "timing_relationship": timing, "prediction_timestamp_utc": prediction["prediction_timestamp_utc"],
        "market_timestamp_utc": selected["captured_at_utc"],
    }
    bridge_identity = f"{bridge['prediction_identity']}|{bridge['market_identity']}|{ATTACHMENT_POLICY}"
    digest = sha256_json(bridge)
    existing = conn.execute("SELECT bridge_payload_sha256 FROM totals_shadow_market_bridge WHERE canonical_bridge_identity=?", (bridge_identity,)).fetchone()
    if not existing:
        conn.execute("INSERT INTO totals_shadow_market_bridge VALUES (?,?,?,?,?,?,?,?)", (
            bridge_identity, bridge["prediction_identity"], bridge["market_identity"], ATTACHMENT_POLICY,
            timing, json.dumps(bridge, sort_keys=True, separators=(",", ":")), digest, created_at_utc,
        ))
        conn.commit()
        action = "APPENDED_NEW"
    else:
        action = "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    return {**selected, **bridge}, action


def prediction_identity(prediction: dict[str, Any]) -> str:
    return f"{prediction['game_date']}|{prediction['game_pk']}|{prediction['model_version']}|{prediction['prediction_snapshot_class']}"


def attach_all_markets(
    conn: sqlite3.Connection, prediction: dict[str, Any], markets: list[dict[str, Any]], created_at_utc: str,
) -> list[dict[str, Any]]:
    candidates = [
        row for row in markets
        if int(row["game_id"]) == int(prediction["game_pk"])
        and utc(row["captured_at_utc"]) < utc(row["scheduled_start_utc"])
    ]
    pred_identity = prediction_identity(prediction)
    prediction_time = utc(prediction["prediction_timestamp_utc"])
    out: list[dict[str, Any]] = []
    for market in sorted(candidates, key=lambda row: (row["captured_at_utc"], row["bookmaker_key"], row["total_line"])):
        timing = "AT_OR_BEFORE_PREDICTION" if utc(market["captured_at_utc"]) <= prediction_time else "POST_PREDICTION_MARKET_OBSERVATION"
        bridge = {
            "prediction_identity": pred_identity, "market_identity": market["canonical_market_identity"],
            "attachment_policy": ALL_BOOK_ATTACHMENT_POLICY, "timing_relationship": timing,
            "prediction_timestamp_utc": prediction["prediction_timestamp_utc"],
            "market_timestamp_utc": market["captured_at_utc"],
        }
        canonical = f"{pred_identity}|{market['canonical_market_identity']}|{ALL_BOOK_ATTACHMENT_POLICY}"
        digest = sha256_json(bridge)
        existing = conn.execute("SELECT bridge_payload_sha256 FROM totals_shadow_all_book_market_bridge WHERE canonical_bridge_identity=?", (canonical,)).fetchone()
        if not existing:
            conn.execute("INSERT INTO totals_shadow_all_book_market_bridge VALUES (?,?,?,?,?,?,?,?)", (
                canonical, pred_identity, market["canonical_market_identity"], ALL_BOOK_ATTACHMENT_POLICY,
                timing, json.dumps(bridge, sort_keys=True, separators=(",", ":")), digest, created_at_utc,
            ))
            conn.commit()
            action = "APPENDED_NEW"
        else:
            action = "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
        out.append({**market, **bridge, "bridge_action": action})
    return out


def american_implied(price: float | int | None) -> float | None:
    if price is None:
        return None
    value = float(price)
    if value == 0:
        return None
    return 100.0 / (value + 100.0) if value > 0 else abs(value) / (abs(value) + 100.0)


def build_consensus(prediction: dict[str, Any], markets: list[dict[str, Any]], captured_at_utc: str) -> dict[str, Any] | None:
    rows = [row for row in markets if int(row["game_id"]) == int(prediction["game_pk"]) and row["captured_at_utc"] == captured_at_utc]
    if not rows:
        return None
    lines = [float(row["total_line"]) for row in rows]
    consensus_line = float(statistics.median(lines))
    modes = statistics.multimode(lines)
    unique_mode = float(modes[0]) if len(modes) == 1 else None
    max_frequency = max(lines.count(value) for value in set(lines))
    same_line = [row for row in rows if float(row["total_line"]) == consensus_line]
    over_prices = [float(row["over_price"]) for row in same_line if row.get("over_price") is not None]
    under_prices = [float(row["under_price"]) for row in same_line if row.get("under_price") is not None]
    no_vig = []
    for row in same_line:
        over, under = american_implied(row.get("over_price")), american_implied(row.get("under_price"))
        if over is not None and under is not None and over + under > 0:
            no_vig.append(over / (over + under))
    return {
        "prediction_identity": prediction_identity(prediction), "game_date": prediction["game_date"],
        "game_id": int(prediction["game_pk"]), "captured_at_utc": captured_at_utc,
        "consensus_policy": CONSENSUS_POLICY, "books_captured": len(rows),
        "distinct_lines": len(set(lines)), "minimum_total_line": min(lines), "maximum_total_line": max(lines),
        "median_total_line": consensus_line, "modal_total_line": unique_mode,
        "line_dispersion_population_sd": statistics.pstdev(lines) if len(lines) > 1 else 0.0,
        "modal_line_book_percentage": 100.0 * max_frequency / len(lines),
        "largest_book_to_book_line_difference": max(lines) - min(lines),
        "books_at_consensus_line": len(same_line),
        "median_over_price_at_consensus_line": statistics.median(over_prices) if over_prices else None,
        "median_under_price_at_consensus_line": statistics.median(under_prices) if under_prices else None,
        "median_no_vig_over_probability_at_consensus_line": statistics.median(no_vig) if no_vig else None,
    }


def append_consensus(conn: sqlite3.Connection, consensus: dict[str, Any], created_at_utc: str) -> str:
    identity = f"{consensus['prediction_identity']}|{consensus['captured_at_utc']}|{CONSENSUS_POLICY}"
    digest = sha256_json(consensus)
    existing = conn.execute("SELECT consensus_payload_sha256 FROM totals_shadow_market_consensus WHERE canonical_consensus_identity=?", (identity,)).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute("INSERT INTO totals_shadow_market_consensus VALUES (?,?,?,?,?,?,?)", (
        identity, consensus["prediction_identity"], consensus["captured_at_utc"], CONSENSUS_POLICY,
        json.dumps(consensus, sort_keys=True, separators=(",", ":")), digest, created_at_utc,
    ))
    conn.commit()
    return "APPENDED_NEW"


def consensus_rows(conn: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT consensus_payload_json FROM totals_shadow_market_consensus WHERE json_extract(consensus_payload_json,'$.game_date')=? ORDER BY captured_at_utc,prediction_identity", (game_date,)).fetchall()
    return [json.loads(row[0]) for row in rows]


def ledger_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "market_rows": conn.execute("SELECT COUNT(*) FROM full_game_total_market_snapshots").fetchone()[0],
        "bridge_rows": conn.execute("SELECT COUNT(*) FROM totals_shadow_market_bridge").fetchone()[0],
        "all_book_bridge_rows": conn.execute("SELECT COUNT(*) FROM totals_shadow_all_book_market_bridge").fetchone()[0],
        "consensus_rows": conn.execute("SELECT COUNT(*) FROM totals_shadow_market_consensus").fetchone()[0],
        "duplicate_market_identities": conn.execute("SELECT COUNT(*) FROM (SELECT canonical_market_identity FROM full_game_total_market_snapshots GROUP BY 1 HAVING COUNT(*)>1)").fetchone()[0],
    }
