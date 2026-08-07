"""Provider-wide SportsGameOdds MLB main-market trial primitives.

The trial is deliberately append-only and shadow-only.  It extends the accepted
BookMaker.eu adapter without changing The Odds API, predictions, or public output.
"""
from __future__ import annotations

import json
import math
import sqlite3
import statistics
from datetime import timedelta
from typing import Any, Iterable

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import (
    MARKETS,
    PROVIDER,
    american_decimal,
    bind_event,
    no_vig,
    parse_american,
    sha256_json,
    utc,
    iso,
)
from backend.mlb.markets.pinnacle_main_market_capture_v1 import eastern_date

EXPERIMENT = "MLB_MAIN_MARKET_PROVIDER_REPLACEMENT_TRIAL_V1"
COMPARISON_WINDOW_MINUTES = 90
PRIORITY_BOOKS = {
    "BOOKMAKER_EU": {"display_name": "BookMaker.eu", "live_ids": ("bookmakereu",)},
    "PINNACLE": {"display_name": "Pinnacle", "live_ids": ("pinnacle",)},
    "CIRCA": {"display_name": "Circa", "live_ids": ("circa",)},
    "PRIME_SPORTS": {"display_name": "Prime Sports", "live_ids": ("primesports", "prime")},
}
BOOK_DISPLAY_NAMES = {
    "bookmakereu": "BookMaker.eu", "pinnacle": "Pinnacle", "circa": "Circa",
    "primesports": "Prime Sports", "prime": "Prime Sports", "betonline": "BetOnline",
    "betonlineag": "BetOnline", "mybookie": "MyBookie", "mybookieag": "MyBookie.ag",
    "williamhill": "William Hill", "williamhill_us": "William Hill US",
    "draftkings": "DraftKings", "fanduel": "FanDuel", "betmgm": "BetMGM",
    "betrivers": "BetRivers", "betus": "BetUS", "bovada": "Bovada",
    "lowvig": "LowVig", "caesars": "Caesars", "espnbet": "ESPN BET",
    "hardrockbet": "Hard Rock Bet", "fanatics": "Fanatics",
}
ODDS_API_TO_SGO_BOOK = {
    "betonlineag": "betonline", "mybookieag": "mybookie", "williamhill_us": "williamhill",
}


def display_name(bookmaker_id: str) -> str:
    return BOOK_DISPLAY_NAMES.get(bookmaker_id, bookmaker_id)


def canonical_book_id(provider: str, bookmaker_key: str) -> str:
    value = str(bookmaker_key).split(":", 1)[-1]
    if provider == "THE_ODDS_API":
        return ODDS_API_TO_SGO_BOOK.get(value, value)
    return value


def _book_side(event: dict[str, Any], odd_id: str, bookmaker_id: str):
    odd = (event.get("odds") or {}).get(odd_id)
    book = ((odd or {}).get("byBookmaker") or {}).get(bookmaker_id)
    if not odd or not book or not bool(book.get("available")):
        return None
    return odd, book


def _price_fields(side: str, book: dict[str, Any]) -> dict[str, Any]:
    raw = str(book["odds"])
    price = parse_american(raw)
    return {
        f"{side}_raw_american_price": raw,
        f"{side}_american_price": price,
        f"{side}_decimal_price": american_decimal(price),
        f"{side}_implied_probability": (
            100.0 / (price + 100.0) if price > 0 else abs(price) / (abs(price) + 100.0)
        ),
        f"{side}_provider_updated_at_utc": str(book["lastUpdatedAt"]),
    }


def pair_market(event: dict[str, Any], market_type: str, bookmaker_id: str) -> dict[str, Any] | None:
    pairs = {side: _book_side(event, odd_id, bookmaker_id) for side, odd_id in MARKETS[market_type].items()}
    if any(value is None for value in pairs.values()):
        return None
    result: dict[str, Any] = {}
    for side, value in pairs.items():
        odd, book = value  # type: ignore[misc]
        if bool(odd.get("started")):
            return None
        result.update(_price_fields(side, book))
    if market_type == "FULL_GAME_TOTAL":
        lines = [float(pairs[side][1]["overUnder"]) for side in ("over", "under")]  # type: ignore[index]
        if lines[0] != lines[1]:
            return None
        result.update(total_line=lines[0], line_key=f"total={lines[0]:g}")
        result["no_vig_over_probability"] = no_vig(result["over_american_price"], result["under_american_price"])
    elif market_type == "RUN_LINE":
        away = float(pairs["away"][1]["spread"])  # type: ignore[index]
        home = float(pairs["home"][1]["spread"])  # type: ignore[index]
        if abs(away + home) > 1e-9:
            return None
        result.update(away_spread=away, home_spread=home, line_key=f"home_spread={home:g}")
        result["no_vig_home_probability"] = no_vig(result["home_american_price"], result["away_american_price"])
    else:
        result["line_key"] = "moneyline"
        result["no_vig_home_probability"] = no_vig(result["home_american_price"], result["away_american_price"])
        result["no_vig_away_probability"] = no_vig(result["away_american_price"], result["home_american_price"])
    updates = [utc(value) for key, value in result.items() if key.endswith("_provider_updated_at_utc")]
    result["provider_market_updated_at_utc"] = iso(max(updates))
    return result


def accessible_book_ids(event: dict[str, Any]) -> list[str]:
    values: set[str] = set()
    for market in MARKETS.values():
        for odd_id in market.values():
            odd = (event.get("odds") or {}).get(odd_id) or {}
            values.update(
                book_id for book_id, value in (odd.get("byBookmaker") or {}).items()
                if bool(value.get("available"))
            )
    return sorted(values)


def parse_provider_events(
    *, events: Iterable[dict[str, Any]], schedule: list[dict[str, Any]], game_date: str,
    fetched_at_utc: str, run_tag: str, raw_source_path: str, raw_source_sha256: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []
    fetched = utc(fetched_at_utc)
    for event in events:
        game, status, method, candidate_ids = bind_event(event, schedule, fetched_at_utc)
        event_start = (event.get("status") or {}).get("startsAt")
        actual_date = eastern_date(event_start) if event_start else None
        classification = (
            "PAST_OR_STARTED" if status == "POST_START" else
            "IDENTITY_AMBIGUOUS" if status == "AMBIGUOUS" else
            "UNRESOLVED" if status != "CERTIFIED_EXACT_OR_DETERMINISTIC" else
            "CURRENT_SLATE" if actual_date == game_date else
            "FUTURE_SLATE_PREGAME" if actual_date and actual_date > game_date else "PAST_OR_STARTED"
        )
        books = accessible_book_ids(event)
        base_audit = {
            "provider_event_id": event.get("eventID"), "candidate_game_pks": "|".join(map(str, candidate_ids)),
            "away_team": (((event.get("teams") or {}).get("away") or {}).get("names") or {}).get("long"),
            "home_team": (((event.get("teams") or {}).get("home") or {}).get("names") or {}).get("long"),
            "game_pk": int(game["game_pk"]) if game else None, "identity_method": method,
            "certification_status": status, "accessible_bookmaker_count": len(books),
            "accessible_bookmaker_ids": "|".join(books), "event_classification": classification,
            "scheduled_start_utc": event_start, "scheduled_start_eastern_date": actual_date,
            "observation_timing_class": (
                "CURRENT_SLATE_PREGAME_OBSERVATION" if classification == "CURRENT_SLATE" else
                "EARLY_FUTURE_SLATE_PREGAME_OBSERVATION" if classification == "FUTURE_SLATE_PREGAME" else
                "POST_START_OBSERVATION" if classification == "PAST_OR_STARTED" else "TIMING_UNRESOLVED"),
        }
        if status != "CERTIFIED_EXACT_OR_DETERMINISTIC" or not game:
            audit.append({**base_audit, "admitted_market_rows": 0, "malformed_market_rows": 0})
            continue
        start = utc(game["scheduled_start_utc"])
        admitted = malformed = 0
        for bookmaker_id in books:
            for market_type in ("MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE"):
                try:
                    market = pair_market(event, market_type, bookmaker_id)
                except (KeyError, TypeError, ValueError):
                    market = None
                    malformed += 1
                if market is None:
                    continue
                try:
                    updated = utc(market["provider_market_updated_at_utc"])
                except Exception:
                    malformed += 1
                    continue
                timing = (
                    "PREGAME_CERTIFIED" if fetched < start and updated < start and updated <= fetched + timedelta(minutes=2)
                    else "TIMING_UNRESOLVED"
                )
                if timing != "PREGAME_CERTIFIED":
                    continue
                row = {
                    "experiment": EXPERIMENT, "provider": PROVIDER,
                    "bookmaker": display_name(bookmaker_id),
                    "bookmaker_key": f"sportsgameodds:{bookmaker_id}",
                    "bookmaker_provider_id": bookmaker_id, "league": "MLB", "game_date": actual_date or game_date,
                    "source_request_slate_date": game_date,
                    "game_id": int(game["game_pk"]), "away_team": game["away_team_name"],
                    "home_team": game["home_team_name"], "scheduled_start_utc": game["scheduled_start_utc"],
                    "provider_event_id": event.get("eventID"), "market_type": market_type,
                    "captured_at_utc": fetched_at_utc, "lead_time_minutes": (start - fetched).total_seconds() / 60.0,
                    "identity_method": method, "identity_certification": status, "timing_status": timing,
                    "event_classification": classification,
                    "observation_timing_class": ("CURRENT_SLATE_PREGAME_OBSERVATION" if classification == "CURRENT_SLATE"
                                                 else "EARLY_FUTURE_SLATE_PREGAME_OBSERVATION"),
                    "source_run_tag": run_tag,
                    "request_class": "CURRENT_MLB_PREGAME_PROVIDER_WIDE_CANONICAL_MAIN_MARKETS",
                    "raw_source_path": raw_source_path, "raw_source_sha256": raw_source_sha256, **market,
                }
                row["canonical_market_identity"] = (
                    f"{PROVIDER}|{bookmaker_id}|{row['game_id']}|{market_type}|{row['line_key']}|{fetched_at_utc}"
                )
                rows.append(row)
                admitted += 1
        audit.append({**base_audit, "admitted_market_rows": admitted, "malformed_market_rows": malformed})
    return rows, audit


def percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower, upper = math.floor(position), math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def consensus_metrics(rows: list[dict[str, Any]], provider: str, scope: str) -> list[dict[str, Any]]:
    output = []
    groups: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((int(row["game_id"]), str(row["market_type"])), []).append(row)
    for (game_id, market), values in sorted(groups.items()):
        base = {"provider_view": provider, "consensus_scope": scope, "game_id": game_id,
                "away_team": values[0].get("away_team"), "home_team": values[0].get("home_team"),
                "market_type": market, "book_count": len(values),
                "bookmaker_ids": "|".join(sorted(canonical_book_id(str(v.get("provider")), str(v["bookmaker_key"])) for v in values))}
        if market == "MONEYLINE":
            home = [float(v["no_vig_home_probability"]) for v in values if v.get("no_vig_home_probability") is not None]
            away = [float(v["no_vig_away_probability"]) for v in values if v.get("no_vig_away_probability") is not None]
            output.append({**base, "median_no_vig_home_probability": statistics.median(home) if home else None,
                           "median_no_vig_away_probability": statistics.median(away) if away else None,
                           "probability_dispersion": max(home) - min(home) if home else None})
            continue
        line_field = "total_line" if market == "FULL_GAME_TOTAL" else "home_spread"
        probability_field = "no_vig_over_probability" if market == "FULL_GAME_TOTAL" else "no_vig_home_probability"
        lines = [float(v[line_field]) for v in values]
        median_line = float(statistics.median(lines)); modes = statistics.multimode(lines)
        modal_line = float(modes[0]) if len(modes) == 1 else None
        modal_count = max(lines.count(line) for line in set(lines))
        same_line = [v for v in values if float(v[line_field]) == median_line]
        probs = [float(v[probability_field]) for v in same_line if v.get(probability_field) is not None]
        output.append({**base, "median_line": median_line, "modal_line": modal_line,
                       "modal_share": modal_count / len(lines), "line_range": max(lines) - min(lines),
                       "distinct_lines": len(set(lines)), "same_line_book_count": len(same_line),
                       "same_line_median_no_vig_probability": statistics.median(probs) if probs else None,
                       "probability_dispersion": max(probs) - min(probs) if probs else None})
    return output


def compare_provider_rows(sgo_rows: list[dict[str, Any]], odds_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compare exact common books and retain one-provider-only rows explicitly."""
    def key(row):
        return int(row["game_id"]), canonical_book_id(str(row.get("provider")), str(row["bookmaker_key"])), str(row["market_type"])
    left = {key(row): row for row in sgo_rows}
    right = {key(row): row for row in odds_rows}
    left_books = {identity[1] for identity in left}
    right_books = {identity[1] for identity in right}
    output = []
    for identity in sorted(set(left) | set(right)):
        a, b = left.get(identity), right.get(identity)
        base = {"game_id": identity[0], "canonical_bookmaker_id": identity[1], "market_type": identity[2],
                "sportsgameodds_present": bool(a), "the_odds_api_present": bool(b)}
        if not a or not b:
            row = a or b
            book_exists_other_provider = identity[1] in (right_books if a else left_books)
            output.append({**base, "away_team": row.get("away_team"), "home_team": row.get("home_team"),
                           "classification": ("MARKET_PRESENT_ONE_PROVIDER_ONLY" if book_exists_other_provider
                                              else "BOOK_PRESENT_ONE_PROVIDER_ONLY"),
                           "sportsgameodds_fetch_utc": a.get("captured_at_utc") if a else None,
                           "the_odds_api_fetch_utc": b.get("captured_at_utc") if b else None})
            continue
        base.update(away_team=a.get("away_team"), home_team=a.get("home_team"),
                    sportsgameodds_fetch_utc=a.get("captured_at_utc"), the_odds_api_fetch_utc=b.get("captured_at_utc"),
                    sportsgameodds_update_utc=a.get("provider_market_updated_at_utc"),
                    the_odds_api_update_utc=b.get("provider_market_timestamp_utc"))
        minutes = abs((utc(a["captured_at_utc"]) - utc(b["captured_at_utc"])).total_seconds()) / 60.0
        base["fetch_time_difference_minutes"] = minutes
        if minutes > COMPARISON_WINDOW_MINUTES:
            output.append({**base, "classification": "TIMING_NOT_COMPARABLE"}); continue
        if identity[2] == "FULL_GAME_TOTAL":
            a_line, b_line = float(a["total_line"]), float(b["total_line"])
            a_prices = (int(a["over_american_price"]), int(a["under_american_price"]))
            b_prices = (int(b["over_price"]), int(b["under_price"]))
        elif identity[2] == "RUN_LINE":
            a_line, b_line = float(a["home_spread"]), float(b["home_spread"])
            a_prices = (int(a["away_american_price"]), int(a["home_american_price"]))
            b_prices = (int(b["away_american_price"]), int(b["home_american_price"]))
        else:
            a_line = b_line = 0.0
            a_prices = (int(a["away_american_price"]), int(a["home_american_price"]))
            b_prices = (int(b["away_american_price"]), int(b["home_american_price"]))
        classification = (
            "LINE_DIFFERENCE" if a_line != b_line else
            "SAME_MARKET_SAME_LINE_SAME_PRICE" if a_prices == b_prices else
            "SAME_LINE_PRICE_DIFFERENCE"
        )
        output.append({**base, "classification": classification, "sportsgameodds_line": a_line,
                       "the_odds_api_line": b_line, "sportsgameodds_side_prices": f"{a_prices[0]}|{a_prices[1]}",
                       "the_odds_api_side_prices": f"{b_prices[0]}|{b_prices[1]}"})
    return output


def freshness_metrics(rows: list[dict[str, Any]], provider: str) -> list[dict[str, Any]]:
    output = []
    for market in ("MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE"):
        values = [row for row in rows if row.get("market_type") == market]
        ages = []
        for row in values:
            stamp = row.get("provider_market_updated_at_utc") or row.get("provider_market_timestamp_utc")
            if stamp:
                ages.append(max(0.0, (utc(row["captured_at_utc"]) - utc(stamp)).total_seconds() / 60.0))
        output.append({"provider": provider, "market_type": market, "row_count": len(values),
                       "timestamped_rows": len(ages), "freshness_comparable": bool(ages),
                       "median_age_minutes": statistics.median(ages) if ages else None,
                       "p90_age_minutes": percentile(ages, .9), "maximum_age_minutes": max(ages) if ages else None,
                       **{f"percentage_within_{n}_minutes": (100.0 * sum(v <= n for v in ages) / len(ages)) if ages else None
                          for n in (2, 5, 10, 30)}})
    return output


def ensure_trial_tables(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS provider_trial_reliability_runs (
      canonical_run_identity TEXT PRIMARY KEY, provider TEXT NOT NULL, game_date TEXT NOT NULL,
      captured_at_utc TEXT NOT NULL, payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS provider_trial_shadow_attachments (
      canonical_attachment_identity TEXT PRIMARY KEY, prediction_identity TEXT NOT NULL,
      market_identity TEXT NOT NULL, provider_view TEXT NOT NULL, market_type TEXT NOT NULL,
      timing_relationship TEXT NOT NULL, payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL, UNIQUE(prediction_identity,market_identity)
    );
    CREATE TRIGGER IF NOT EXISTS provider_trial_reliability_no_update BEFORE UPDATE ON provider_trial_reliability_runs
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PROVIDER_TRIAL_RELIABILITY'); END;
    CREATE TRIGGER IF NOT EXISTS provider_trial_reliability_no_delete BEFORE DELETE ON provider_trial_reliability_runs
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PROVIDER_TRIAL_RELIABILITY'); END;
    CREATE TRIGGER IF NOT EXISTS provider_trial_attachment_no_update BEFORE UPDATE ON provider_trial_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PROVIDER_TRIAL_ATTACHMENT'); END;
    CREATE TRIGGER IF NOT EXISTS provider_trial_attachment_no_delete BEFORE DELETE ON provider_trial_shadow_attachments
      BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_PROVIDER_TRIAL_ATTACHMENT'); END;
    """)
    conn.commit()


def append_reliability(conn: sqlite3.Connection, payload: dict[str, Any]) -> str:
    ensure_trial_tables(conn)
    identity = f"{payload['provider']}|{payload['game_date']}|{payload['captured_at_utc']}|{payload['source_run_tag']}"
    digest = sha256_json(payload)
    old = conn.execute("SELECT payload_sha256 FROM provider_trial_reliability_runs WHERE canonical_run_identity=?", (identity,)).fetchone()
    if old:
        return "EXISTING_IMMUTABLE" if old[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute("INSERT INTO provider_trial_reliability_runs VALUES (?,?,?,?,?,?)",
                 (identity, payload["provider"], payload["game_date"], payload["captured_at_utc"], json.dumps(payload, sort_keys=True), digest))
    conn.commit(); return "APPENDED_NEW"


def reliability_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_trial_tables(conn)
    return [json.loads(row[0]) for row in conn.execute("SELECT payload_json FROM provider_trial_reliability_runs ORDER BY captured_at_utc,provider")]


def append_shadow_attachment(conn: sqlite3.Connection, payload: dict[str, Any]) -> str:
    ensure_trial_tables(conn)
    identity = f"{payload['prediction_identity']}|{payload['market_identity']}"
    digest = sha256_json(payload)
    old = conn.execute(
        "SELECT payload_sha256 FROM provider_trial_shadow_attachments WHERE canonical_attachment_identity=?", (identity,)
    ).fetchone()
    if old:
        return "EXISTING_IMMUTABLE" if old[0] == digest else "EXISTING_CONFLICT_PRESERVED"
    conn.execute(
        "INSERT INTO provider_trial_shadow_attachments VALUES (?,?,?,?,?,?,?,?,?)",
        (identity, payload["prediction_identity"], payload["market_identity"], payload["provider_view"],
         payload["market_type"], payload["timing_relationship"], json.dumps(payload, sort_keys=True), digest,
         payload["created_at_utc"]),
    )
    conn.commit()
    return "APPENDED_NEW"
