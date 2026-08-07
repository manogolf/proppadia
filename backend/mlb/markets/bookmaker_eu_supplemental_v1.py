"""SportsGameOdds Bookmaker.eu supplemental MLB main-market adapter.

This module is deliberately source-specific at acquisition and provider-neutral at
the append-only ledger/consensus boundary.  It never treats Bookmaker.eu as a
replacement for the existing The Odds API capture.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

EXPERIMENT = "MLB_BOOKMAKER_EU_SUPPLEMENTAL_MARKET_ADAPTER_V1"
PROVIDER = "SPORTSGAMEODDS"
BOOKMAKER_ID = "bookmakereu"
BOOKMAKER_NAME = "Bookmaker.eu"
LEDGER_BOOKMAKER_KEY = "sportsgameodds:bookmakereu"
CONSENSUS_WINDOW_MINUTES = 90
CONSENSUS_POLICY = "LATEST_CERTIFIED_PER_PROVIDER_BOOK_WITHIN_90_MINUTES_SAME_LINE_NO_VIG"
RUN_LINE_MODEL_STATUS = "MODEL_COMPARISON_UNAVAILABLE_NO_QUALIFIED_RUN_LINE_MODEL"

MARKETS = {
    "MONEYLINE": {
        "away": "points-away-game-ml-away",
        "home": "points-home-game-ml-home",
    },
    "FULL_GAME_TOTAL": {
        "over": "points-all-game-ou-over",
        "under": "points-all-game-ou-under",
    },
    "RUN_LINE": {
        "away": "points-away-game-sp-away",
        "home": "points-home-game-sp-home",
    },
}
AMERICAN_RE = re.compile(r"^[+-][1-9][0-9]*$")


def utc(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_team(value: str) -> str:
    return "".join(ch for ch in str(value or "").casefold() if ch.isalnum())


def sha256_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def parse_american(raw: Any) -> int:
    text = str(raw)
    if not AMERICAN_RE.fullmatch(text):
        raise ValueError(f"INVALID_AMERICAN_ODDS:{text!r}")
    return int(text)


def american_decimal(price: int | float) -> float:
    value = float(price)
    if value == 0:
        raise ValueError("ZERO_AMERICAN_ODDS")
    return 1.0 + (value / 100.0 if value > 0 else 100.0 / abs(value))


def american_implied(price: int | float | None) -> float | None:
    if price is None:
        return None
    value = float(price)
    if value == 0:
        return None
    return 100.0 / (value + 100.0) if value > 0 else abs(value) / (abs(value) + 100.0)


def no_vig(first: int | float | None, second: int | float | None) -> float | None:
    p1, p2 = american_implied(first), american_implied(second)
    if p1 is None or p2 is None or p1 + p2 <= 0:
        return None
    return p1 / (p1 + p2)


def connect_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS supplemental_main_market_snapshots (
      canonical_market_identity TEXT PRIMARY KEY,
      provider TEXT NOT NULL,
      bookmaker_key TEXT NOT NULL,
      game_date TEXT NOT NULL,
      game_id INTEGER NOT NULL,
      market_type TEXT NOT NULL,
      line_key TEXT NOT NULL,
      captured_at_utc TEXT NOT NULL,
      scheduled_start_utc TEXT NOT NULL,
      timing_status TEXT NOT NULL,
      market_payload_json TEXT NOT NULL,
      market_payload_sha256 TEXT NOT NULL,
      raw_source_path TEXT NOT NULL,
      raw_source_sha256 TEXT NOT NULL,
      UNIQUE(provider,bookmaker_key,game_id,market_type,line_key,captured_at_utc)
    );
    CREATE TABLE IF NOT EXISTS supplemental_main_market_consensus (
      canonical_consensus_identity TEXT PRIMARY KEY,
      game_date TEXT NOT NULL,
      game_id INTEGER NOT NULL,
      market_type TEXT NOT NULL,
      captured_at_utc TEXT NOT NULL,
      consensus_policy TEXT NOT NULL,
      consensus_payload_json TEXT NOT NULL,
      consensus_payload_sha256 TEXT NOT NULL,
      UNIQUE(game_id,market_type,captured_at_utc,consensus_policy)
    );
    CREATE TABLE IF NOT EXISTS main_market_event_discoveries (
      canonical_discovery_identity TEXT PRIMARY KEY, provider TEXT NOT NULL,
      provider_event_id TEXT NOT NULL, game_date TEXT NOT NULL, game_id INTEGER NOT NULL,
      captured_at_utc TEXT NOT NULL, scheduled_start_utc TEXT NOT NULL,
      event_classification TEXT NOT NULL, discovery_payload_json TEXT NOT NULL,
      discovery_payload_sha256 TEXT NOT NULL, raw_source_path TEXT NOT NULL,
      raw_source_sha256 TEXT NOT NULL,
      UNIQUE(provider,provider_event_id,game_id,captured_at_utc)
    );
    CREATE TABLE IF NOT EXISTS bookmaker_eu_totals_shadow_attachments (
      canonical_attachment_identity TEXT PRIMARY KEY,
      prediction_identity TEXT NOT NULL,
      market_identity TEXT NOT NULL,
      timing_relationship TEXT NOT NULL,
      attachment_payload_json TEXT NOT NULL,
      attachment_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL,
      UNIQUE(prediction_identity,market_identity)
    );
    CREATE TABLE IF NOT EXISTS bookmaker_eu_moneyline_shadow_attachments (
      canonical_attachment_identity TEXT PRIMARY KEY,
      prediction_identity TEXT NOT NULL,
      market_identity TEXT NOT NULL,
      timing_relationship TEXT NOT NULL,
      attachment_payload_json TEXT NOT NULL,
      attachment_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL,
      UNIQUE(prediction_identity,market_identity)
    );
    CREATE TABLE IF NOT EXISTS pinnacle_totals_shadow_attachments (
      canonical_attachment_identity TEXT PRIMARY KEY, prediction_identity TEXT NOT NULL,
      market_identity TEXT NOT NULL, timing_relationship TEXT NOT NULL,
      attachment_payload_json TEXT NOT NULL, attachment_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL, UNIQUE(prediction_identity,market_identity)
    );
    CREATE TABLE IF NOT EXISTS pinnacle_moneyline_shadow_attachments (
      canonical_attachment_identity TEXT PRIMARY KEY, prediction_identity TEXT NOT NULL,
      market_identity TEXT NOT NULL, timing_relationship TEXT NOT NULL,
      attachment_payload_json TEXT NOT NULL, attachment_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL, UNIQUE(prediction_identity,market_identity)
    );
    CREATE TRIGGER IF NOT EXISTS supplemental_market_no_update
      BEFORE UPDATE ON supplemental_main_market_snapshots
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_SUPPLEMENTAL_MAIN_MARKET'); END;
    CREATE TRIGGER IF NOT EXISTS supplemental_market_no_delete
      BEFORE DELETE ON supplemental_main_market_snapshots
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_SUPPLEMENTAL_MAIN_MARKET'); END;
    CREATE TRIGGER IF NOT EXISTS supplemental_consensus_no_update
      BEFORE UPDATE ON supplemental_main_market_consensus
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_SUPPLEMENTAL_CONSENSUS'); END;
    CREATE TRIGGER IF NOT EXISTS supplemental_consensus_no_delete
      BEFORE DELETE ON supplemental_main_market_consensus
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_SUPPLEMENTAL_CONSENSUS'); END;
    CREATE TRIGGER IF NOT EXISTS main_market_discovery_no_update BEFORE UPDATE ON main_market_event_discoveries
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_MAIN_MARKET_DISCOVERY'); END;
    CREATE TRIGGER IF NOT EXISTS main_market_discovery_no_delete BEFORE DELETE ON main_market_event_discoveries
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_MAIN_MARKET_DISCOVERY'); END;
    CREATE TRIGGER IF NOT EXISTS bookmaker_totals_attachment_no_update
      BEFORE UPDATE ON bookmaker_eu_totals_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_BOOKMAKER_TOTALS_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS bookmaker_totals_attachment_no_delete
      BEFORE DELETE ON bookmaker_eu_totals_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_BOOKMAKER_TOTALS_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS bookmaker_moneyline_attachment_no_update
      BEFORE UPDATE ON bookmaker_eu_moneyline_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_BOOKMAKER_MONEYLINE_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS bookmaker_moneyline_attachment_no_delete
      BEFORE DELETE ON bookmaker_eu_moneyline_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_BOOKMAKER_MONEYLINE_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS pinnacle_totals_attachment_no_update BEFORE UPDATE ON pinnacle_totals_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PINNACLE_TOTALS_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS pinnacle_totals_attachment_no_delete BEFORE DELETE ON pinnacle_totals_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PINNACLE_TOTALS_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS pinnacle_moneyline_attachment_no_update BEFORE UPDATE ON pinnacle_moneyline_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PINNACLE_MONEYLINE_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS pinnacle_moneyline_attachment_no_delete BEFORE DELETE ON pinnacle_moneyline_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PINNACLE_MONEYLINE_ATTACHMENT'); END;
    """)
    conn.commit()
    return conn


def _provider_game_number(event: dict[str, Any]) -> int | None:
    values = [event.get("gameNumber"), (event.get("info") or {}).get("gameNumber")]
    for value in values:
        try:
            return int(value)
        except (TypeError, ValueError):
            pass
    return None


def bind_event(
    event: dict[str, Any], schedule: list[dict[str, Any]], fetched_at_utc: str,
) -> tuple[dict[str, Any] | None, str, str, list[int]]:
    """Bind one event and classify identity/timing without reading outcomes."""
    try:
        start = utc(event["status"]["startsAt"])
        fetched = utc(fetched_at_utc)
        away = normalize_team(event["teams"]["away"]["names"]["long"])
        home = normalize_team(event["teams"]["home"]["names"]["long"])
    except Exception:
        return None, "TIMING_UNRESOLVED", "INVALID_EVENT_IDENTITY_OR_TIME", []
    candidates = [
        game for game in schedule
        if normalize_team(game.get("away_team_name")) == away
        and normalize_team(game.get("home_team_name")) == home
        and abs((utc(game["scheduled_start_utc"]) - start).total_seconds()) <= 600
    ]
    method = "EXACT_DATE_TEAMS_START_WITHIN_10_MINUTES"
    provider_game_number = _provider_game_number(event)
    if len(candidates) > 1 and provider_game_number is not None:
        numbered = [g for g in candidates if int(g.get("game_number") or 0) == provider_game_number]
        if len(numbered) == 1:
            candidates = numbered
            method += "_AND_GAME_NUMBER"
    ids = [int(game["game_pk"]) for game in candidates]
    if not candidates:
        return None, "GAME_NOT_FOUND", method, []
    if len(candidates) != 1:
        return None, "AMBIGUOUS", method, ids
    game = candidates[0]
    if bool(event.get("status", {}).get("started")) or bool(event.get("status", {}).get("live")) or fetched >= utc(game["scheduled_start_utc"]):
        return game, "POST_START", method, ids
    return game, "CERTIFIED_EXACT_OR_DETERMINISTIC", method, ids


def _book_side(event: dict[str, Any], odd_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    odd = (event.get("odds") or {}).get(odd_id)
    book = ((odd or {}).get("byBookmaker") or {}).get(BOOKMAKER_ID)
    if not odd or not book or not bool(book.get("available")):
        return None
    return odd, book


def _price_fields(prefix: str, book: dict[str, Any]) -> dict[str, Any]:
    raw = str(book["odds"])
    american = parse_american(raw)
    return {
        f"{prefix}_raw_american_price": raw,
        f"{prefix}_american_price": american,
        f"{prefix}_decimal_price": american_decimal(american),
        f"{prefix}_implied_probability": american_implied(american),
        f"{prefix}_provider_updated_at_utc": str(book["lastUpdatedAt"]),
    }


def _pair_market(event: dict[str, Any], market_type: str) -> dict[str, Any] | None:
    ids = MARKETS[market_type]
    pairs = {side: _book_side(event, odd_id) for side, odd_id in ids.items()}
    if any(value is None for value in pairs.values()):
        return None
    result: dict[str, Any] = {}
    for side, value in pairs.items():
        odd, book = value  # type: ignore[misc]
        if bool(odd.get("started")):
            return None
        result.update(_price_fields(side, book))
    if market_type == "FULL_GAME_TOTAL":
        over_line = float(pairs["over"][1]["overUnder"])  # type: ignore[index]
        under_line = float(pairs["under"][1]["overUnder"])  # type: ignore[index]
        if over_line != under_line:
            return None
        result["total_line"] = over_line
        result["line_key"] = f"total={over_line:g}"
        result["no_vig_over_probability"] = no_vig(result["over_american_price"], result["under_american_price"])
    elif market_type == "RUN_LINE":
        away_spread = float(pairs["away"][1]["spread"])  # type: ignore[index]
        home_spread = float(pairs["home"][1]["spread"])  # type: ignore[index]
        if abs(away_spread + home_spread) > 1e-9:
            return None
        result.update({"away_spread": away_spread, "home_spread": home_spread,
                       "line_key": f"home_spread={home_spread:g}",
                       "no_vig_home_probability": no_vig(result["home_american_price"], result["away_american_price"])})
    else:
        result["line_key"] = "moneyline"
        result["no_vig_home_probability"] = no_vig(result["home_american_price"], result["away_american_price"])
        result["no_vig_away_probability"] = no_vig(result["away_american_price"], result["home_american_price"])
    updates = [utc(value) for key, value in result.items() if key.endswith("_provider_updated_at_utc")]
    result["provider_market_updated_at_utc"] = iso(max(updates))
    return result


def parse_events(
    *, events: Iterable[dict[str, Any]], schedule: list[dict[str, Any]], game_date: str,
    fetched_at_utc: str, run_tag: str, raw_source_path: str, raw_source_sha256: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    fetched = utc(fetched_at_utc)
    for event in events:
        game, status, method, candidate_ids = bind_event(event, schedule, fetched_at_utc)
        audit_row = {
            "provider_event_id": event.get("eventID"),
            "provider_away_team": (((event.get("teams") or {}).get("away") or {}).get("names") or {}).get("long"),
            "provider_home_team": (((event.get("teams") or {}).get("home") or {}).get("names") or {}).get("long"),
            "provider_start_utc": (event.get("status") or {}).get("startsAt"),
            "candidate_game_pks": "|".join(map(str, candidate_ids)),
            "candidate_count": len(candidate_ids),
            "game_pk": int(game["game_pk"]) if game else None,
            "identity_method": method,
            "certification_status": status,
        }
        if status != "CERTIFIED_EXACT_OR_DETERMINISTIC" or not game:
            audit_row.update({"admitted_market_rows": 0, "market_status": "REJECTED"})
            audit.append(audit_row)
            continue
        start = utc(game["scheduled_start_utc"])
        event_rows = 0
        market_failures = []
        for market_type in ("MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE"):
            try:
                market = _pair_market(event, market_type)
            except (KeyError, TypeError, ValueError) as exc:
                market = None
                market_failures.append(f"{market_type}:{type(exc).__name__}")
            if market is None:
                market_failures.append(f"{market_type}:UNAVAILABLE_OR_INVALID_PAIR")
                continue
            try:
                update_time = utc(market["provider_market_updated_at_utc"])
            except Exception:
                market_failures.append(f"{market_type}:TIMING_UNRESOLVED")
                continue
            timing = "PREGAME_CERTIFIED" if fetched < start and update_time < start and update_time <= fetched + timedelta(minutes=2) else "TIMING_UNRESOLVED"
            if timing != "PREGAME_CERTIFIED":
                market_failures.append(f"{market_type}:{timing}")
                continue
            row = {
                "experiment": EXPERIMENT,
                "provider": PROVIDER,
                "bookmaker": BOOKMAKER_NAME,
                "bookmaker_key": LEDGER_BOOKMAKER_KEY,
                "bookmaker_provider_id": BOOKMAKER_ID,
                "league": "MLB",
                "game_date": game_date,
                "game_id": int(game["game_pk"]),
                "away_team": game["away_team_name"],
                "home_team": game["home_team_name"],
                "scheduled_start_utc": game["scheduled_start_utc"],
                "provider_event_id": event.get("eventID"),
                "market_type": market_type,
                "captured_at_utc": fetched_at_utc,
                "lead_time_minutes": (start - fetched).total_seconds() / 60.0,
                "identity_method": method,
                "identity_certification": status,
                "timing_status": timing,
                "source_run_tag": run_tag,
                "request_class": "CURRENT_MLB_PREGAME_BOOKMAKER_EU_CANONICAL_MAIN_MARKETS",
                "raw_source_path": raw_source_path,
                "raw_source_sha256": raw_source_sha256,
                **market,
            }
            row["canonical_market_identity"] = (
                f"{PROVIDER}|{BOOKMAKER_ID}|{row['game_id']}|{market_type}|"
                f"{row['line_key']}|{fetched_at_utc}"
            )
            rows.append(row)
            event_rows += 1
        audit_row.update({
            "admitted_market_rows": event_rows,
            "market_status": "ADMITTED" if event_rows else "NO_CERTIFIED_MAIN_MARKET",
            "market_failures": "|".join(sorted(set(market_failures))),
        })
        audit.append(audit_row)
    return rows, audit


def append_market(conn: sqlite3.Connection, row: dict[str, Any]) -> str:
    identity = row["canonical_market_identity"]
    digest = sha256_json(row)
    existing = conn.execute(
        "SELECT market_payload_sha256 FROM supplemental_main_market_snapshots WHERE canonical_market_identity=?",
        (identity,),
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute(
        "INSERT INTO supplemental_main_market_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (identity, row["provider"], row["bookmaker_key"], row["game_date"], row["game_id"],
         row["market_type"], row["line_key"], row["captured_at_utc"], row["scheduled_start_utc"],
         row["timing_status"], json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
         digest, row["raw_source_path"], row["raw_source_sha256"]),
    )
    conn.commit()
    return "APPENDED_NEW"


def append_event_discovery(conn: sqlite3.Connection, payload: dict[str, Any]) -> str:
    identity = (
        f"{payload['provider']}|{payload['provider_event_id']}|{payload['game_id']}|"
        f"{payload['captured_at_utc']}"
    )
    digest = sha256_json(payload)
    existing = conn.execute(
        "SELECT discovery_payload_sha256 FROM main_market_event_discoveries WHERE canonical_discovery_identity=?",
        (identity,),
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute(
        "INSERT INTO main_market_event_discoveries VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (identity, payload["provider"], payload["provider_event_id"], payload["game_date"],
         payload["game_id"], payload["captured_at_utc"], payload["scheduled_start_utc"],
         payload["event_classification"], json.dumps(payload, sort_keys=True, separators=(",", ":")),
         digest, payload["raw_source_path"], payload["raw_source_sha256"]),
    )
    conn.commit()
    return "APPENDED_NEW"


def mark_first_observed_prices(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Label first Proppadia-priced observations without claiming bookmaker openers."""
    output = []
    for row in rows:
        prior = conn.execute(
            """SELECT 1 FROM supplemental_main_market_snapshots
               WHERE provider=? AND bookmaker_key=? AND game_id=? AND market_type=?
               AND captured_at_utc<? LIMIT 1""",
            (row["provider"], row["bookmaker_key"], row["game_id"], row["market_type"], row["captured_at_utc"]),
        ).fetchone()
        output.append({**row, "price_observation_class": (
            "LATER_PROPPADIA_OBSERVED_PREGAME_LINE" if prior
            else "FIRST_PROPPADIA_OBSERVED_PREGAME_LINE"
        )})
    return output


def market_rows(conn: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    values = conn.execute(
        "SELECT market_payload_json FROM supplemental_main_market_snapshots WHERE game_date=? ORDER BY captured_at_utc,game_id,market_type",
        (game_date,),
    ).fetchall()
    return [json.loads(row[0]) for row in values]


def _latest_per_provider_book(rows: list[dict[str, Any]], captured_at_utc: str) -> list[dict[str, Any]]:
    cutoff = utc(captured_at_utc)
    floor = cutoff - timedelta(minutes=CONSENSUS_WINDOW_MINUTES)
    eligible = [
        row for row in rows
        if row.get("timing_status") == "PREGAME_CERTIFIED"
        and floor <= utc(row["captured_at_utc"]) <= cutoff
        and utc(row["captured_at_utc"]) < utc(row["scheduled_start_utc"])
    ]
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for row in eligible:
        key = (str(row.get("provider") or ""), str(row.get("bookmaker_key") or ""))
        if key not in selected or utc(row["captured_at_utc"]) > utc(selected[key]["captured_at_utc"]):
            selected[key] = row
    return list(selected.values())


def build_consensus(
    *, rows: list[dict[str, Any]], game_date: str, game_id: int,
    market_type: str, captured_at_utc: str,
) -> dict[str, Any] | None:
    candidates = _latest_per_provider_book([
        row for row in rows
        if int(row["game_id"]) == int(game_id) and row["market_type"] == market_type
    ], captured_at_utc)
    if not candidates:
        return None
    base = {
        "experiment": EXPERIMENT,
        "game_date": game_date,
        "game_id": int(game_id),
        "market_type": market_type,
        "captured_at_utc": captured_at_utc,
        "consensus_policy": CONSENSUS_POLICY,
        "snapshot_window_minutes": CONSENSUS_WINDOW_MINUTES,
        "books_captured": len(candidates),
        "bookmaker_keys": sorted(row["bookmaker_key"] for row in candidates),
        "consensus_status": "MULTI_BOOK_CONSENSUS" if len(candidates) >= 2 else "SINGLE_BOOK_DESCRIPTIVE_NOT_CONSENSUS",
    }
    if market_type == "MONEYLINE":
        home_probs = [row.get("no_vig_home_probability") for row in candidates if row.get("no_vig_home_probability") is not None]
        away_probs = [row.get("no_vig_away_probability") for row in candidates if row.get("no_vig_away_probability") is not None]
        return {**base,
                "median_no_vig_home_probability": statistics.median(home_probs) if home_probs else None,
                "median_no_vig_away_probability": statistics.median(away_probs) if away_probs else None}
    line_field = "total_line" if market_type == "FULL_GAME_TOTAL" else "home_spread"
    lines = [float(row[line_field]) for row in candidates]
    median_line = float(statistics.median(lines))
    modes = statistics.multimode(lines)
    modal_line = float(modes[0]) if len(modes) == 1 else None
    max_frequency = max(lines.count(line) for line in set(lines))
    same_line = [row for row in candidates if float(row[line_field]) == median_line]
    if market_type == "FULL_GAME_TOTAL":
        probabilities = [row.get("no_vig_over_probability") for row in same_line if row.get("no_vig_over_probability") is not None]
        probability_key = "median_no_vig_over_probability_at_consensus_line"
    else:
        probabilities = [row.get("no_vig_home_probability") for row in same_line if row.get("no_vig_home_probability") is not None]
        probability_key = "median_no_vig_home_probability_at_consensus_spread"
    return {
        **base,
        "line_field": line_field,
        "minimum_line": min(lines),
        "maximum_line": max(lines),
        "median_line": median_line,
        "modal_line": modal_line,
        "distinct_lines": len(set(lines)),
        "line_range": max(lines) - min(lines),
        "modal_line_book_percentage": 100.0 * max_frequency / len(lines),
        "books_at_consensus_line": len(same_line),
        probability_key: statistics.median(probabilities) if probabilities else None,
    }


def append_consensus(conn: sqlite3.Connection, value: dict[str, Any]) -> str:
    identity = (
        f"{value['game_id']}|{value['market_type']}|{value['captured_at_utc']}|{CONSENSUS_POLICY}"
    )
    digest = sha256_json(value)
    existing = conn.execute(
        "SELECT consensus_payload_sha256 FROM supplemental_main_market_consensus WHERE canonical_consensus_identity=?",
        (identity,),
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute(
        "INSERT INTO supplemental_main_market_consensus VALUES (?,?,?,?,?,?,?,?)",
        (identity, value["game_date"], value["game_id"], value["market_type"], value["captured_at_utc"],
         CONSENSUS_POLICY, json.dumps(value, sort_keys=True, separators=(",", ":")), digest),
    )
    conn.commit()
    return "APPENDED_NEW"


def append_attachment(
    conn: sqlite3.Connection, *, table: str, prediction_identity: str,
    market_identity: str, payload: dict[str, Any], created_at_utc: str,
) -> str:
    if table not in {
        "bookmaker_eu_totals_shadow_attachments", "bookmaker_eu_moneyline_shadow_attachments",
        "pinnacle_totals_shadow_attachments", "pinnacle_moneyline_shadow_attachments",
    }:
        raise ValueError("UNSUPPORTED_ATTACHMENT_TABLE")
    identity = f"{prediction_identity}|{market_identity}"
    digest = sha256_json(payload)
    existing = conn.execute(
        f"SELECT attachment_payload_sha256 FROM {table} WHERE canonical_attachment_identity=?", (identity,),
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute(
        f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?)",
        (identity, prediction_identity, market_identity, payload["timing_relationship"],
         json.dumps(payload, sort_keys=True, separators=(",", ":")), digest, created_at_utc),
    )
    conn.commit()
    return "APPENDED_NEW"


def ledger_counts(conn: sqlite3.Connection) -> dict[str, int]:
    return {
        "supplemental_market_rows": conn.execute("SELECT COUNT(*) FROM supplemental_main_market_snapshots").fetchone()[0],
        "supplemental_consensus_rows": conn.execute("SELECT COUNT(*) FROM supplemental_main_market_consensus").fetchone()[0],
        "totals_attachment_rows": conn.execute("SELECT COUNT(*) FROM bookmaker_eu_totals_shadow_attachments").fetchone()[0],
        "moneyline_attachment_rows": conn.execute("SELECT COUNT(*) FROM bookmaker_eu_moneyline_shadow_attachments").fetchone()[0],
        "duplicate_market_identities": conn.execute("SELECT COUNT(*) FROM (SELECT canonical_market_identity FROM supplemental_main_market_snapshots GROUP BY 1 HAVING COUNT(*)>1)").fetchone()[0],
    }
