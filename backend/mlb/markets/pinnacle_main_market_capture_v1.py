"""Canonicalize explicit The Odds API Pinnacle MLB main markets."""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import (
    american_decimal, american_implied, iso, no_vig, normalize_team, utc,
)

PROVIDER = "THE_ODDS_API"
BOOKMAKER_KEY = "pinnacle"
BOOKMAKER_NAME = "Pinnacle"
REQUEST_CLASS = "CURRENT_MLB_PREGAME_PINNACLE_CANONICAL_MAIN_MARKETS"
RUN_LINE_MODEL_STATUS = "MODEL_COMPARISON_UNAVAILABLE_NO_QUALIFIED_RUN_LINE_MODEL"
MARKETS = ("h2h", "totals", "spreads")


def eastern_date(value: str) -> str:
    return utc(value).astimezone(ZoneInfo("America/New_York")).date().isoformat()


def bind_event(event: dict[str, Any], schedule: list[dict[str, Any]], fetched_at_utc: str):
    """Require one exact team/start bridge; a game number resolves doubleheaders."""
    try:
        start = utc(event["commence_time"])
        fetched = utc(fetched_at_utc)
    except Exception:
        return None, "TIMING_UNRESOLVED", []
    candidates = [
        game for game in schedule
        if normalize_team(game.get("away_team_name")) == normalize_team(event.get("away_team"))
        and normalize_team(game.get("home_team_name")) == normalize_team(event.get("home_team"))
        and abs((utc(game["scheduled_start_utc"]) - start).total_seconds()) <= 600
    ]
    provider_number = event.get("game_number") or event.get("gameNumber")
    if len(candidates) > 1 and provider_number is not None:
        candidates = [g for g in candidates if int(g.get("game_number") or 0) == int(provider_number)]
    ids = [int(game["game_pk"]) for game in candidates]
    if not candidates:
        return None, "GAME_NOT_FOUND", ids
    if len(candidates) != 1:
        return None, "AMBIGUOUS", ids
    game = candidates[0]
    if fetched >= utc(game["scheduled_start_utc"]):
        return game, "POST_START", ids
    return game, "CERTIFIED_EXACT_OR_DETERMINISTIC", ids


def _outcomes(market: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("name")): row for row in market.get("outcomes", [])}


def _price(prefix: str, raw: Any) -> dict[str, Any]:
    price = int(raw)
    return {
        f"{prefix}_raw_american_price": str(raw),
        f"{prefix}_american_price": price,
        f"{prefix}_decimal_price": american_decimal(price),
        f"{prefix}_implied_probability": american_implied(price),
    }


def _parse_market(event: dict[str, Any], market: dict[str, Any]) -> dict[str, Any] | None:
    key, sides = market.get("key"), _outcomes(market)
    if key == "h2h" and event.get("away_team") in sides and event.get("home_team") in sides:
        away, home = sides[event["away_team"]], sides[event["home_team"]]
        return {"market_type": "MONEYLINE", "line_key": "moneyline",
                **_price("away", away["price"]), **_price("home", home["price"]),
                "no_vig_away_probability": no_vig(away["price"], home["price"]),
                "no_vig_home_probability": no_vig(home["price"], away["price"])}
    if key == "totals" and "Over" in sides and "Under" in sides:
        over, under = sides["Over"], sides["Under"]
        if float(over["point"]) != float(under["point"]):
            return None
        line = float(over["point"])
        return {"market_type": "FULL_GAME_TOTAL", "line_key": f"total={line:g}", "total_line": line,
                **_price("over", over["price"]), **_price("under", under["price"]),
                "no_vig_over_probability": no_vig(over["price"], under["price"]),
                "no_vig_under_probability": no_vig(under["price"], over["price"])}
    if key == "spreads" and event.get("away_team") in sides and event.get("home_team") in sides:
        away, home = sides[event["away_team"]], sides[event["home_team"]]
        away_spread, home_spread = float(away["point"]), float(home["point"])
        if abs(away_spread + home_spread) > 1e-9:
            return None
        return {"market_type": "RUN_LINE", "line_key": f"home_spread={home_spread:g}",
                "away_spread": away_spread, "home_spread": home_spread,
                **_price("away", away["price"]), **_price("home", home["price"]),
                "no_vig_away_probability": no_vig(away["price"], home["price"]),
                "no_vig_home_probability": no_vig(home["price"], away["price"])}
    return None


def parse_events(*, events: Iterable[dict[str, Any]], schedule: list[dict[str, Any]], game_date: str,
                 fetched_at_utc: str, run_tag: str, raw_source_path: str, raw_source_sha256: str):
    rows, audit = [], []
    fetched = utc(fetched_at_utc)
    for event in events:
        game, status, candidate_ids = bind_event(event, schedule, fetched_at_utc)
        event_date = eastern_date(event["commence_time"]) if event.get("commence_time") else None
        classification = (
            "PAST_OR_STARTED" if status == "POST_START"
            else "IDENTITY_AMBIGUOUS" if status == "AMBIGUOUS"
            else "NON_MLB_OR_INVALID" if event.get("sport_key") not in {None, "baseball_mlb"}
            else "UNRESOLVED" if status != "CERTIFIED_EXACT_OR_DETERMINISTIC"
            else "CURRENT_SLATE" if event_date == game_date
            else "FUTURE_SLATE_PREGAME" if event_date and event_date > game_date
            else "PAST_OR_STARTED"
        )
        audit_row = {"provider_event_id": event.get("id"), "game_pk": game.get("game_pk") if game else None,
                     "away_team": event.get("away_team"), "home_team": event.get("home_team"),
                     "scheduled_start_utc": event.get("commence_time"), "scheduled_start_eastern_date": event_date,
                     "candidate_game_pks": candidate_ids, "original_rejection_reason": "GAME_NOT_FOUND" if event_date != game_date else None,
                     "certification_status": status, "event_classification": classification}
        if status != "CERTIFIED_EXACT_OR_DETERMINISTIC" or not game:
            audit.append({**audit_row, "admitted_market_rows": 0})
            continue
        admitted = 0
        for book in event.get("bookmakers", []):
            if book.get("key") != BOOKMAKER_KEY:
                continue
            for market in book.get("markets", []):
                try:
                    parsed = _parse_market(event, market)
                    updated = utc(market["last_update"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not parsed or updated >= utc(game["scheduled_start_utc"]) or updated > fetched + timedelta(minutes=2):
                    continue
                actual_date = event_date or game_date
                timing_status = "PREGAME_CERTIFIED" if classification == "CURRENT_SLATE" else "EARLY_FUTURE_SLATE_PREGAME_OBSERVATION"
                row = {"provider": PROVIDER, "bookmaker": BOOKMAKER_NAME, "bookmaker_key": BOOKMAKER_KEY,
                       "bookmaker_provider_id": BOOKMAKER_KEY, "league": "MLB",
                       "source_request_slate_date": game_date, "game_date": actual_date,
                       "game_id": int(game["game_pk"]), "away_team": game["away_team_name"],
                       "home_team": game["home_team_name"], "scheduled_start_utc": game["scheduled_start_utc"],
                       "provider_event_id": event.get("id"), "provider_market_id": market.get("key"),
                       "provider_market_updated_at_utc": iso(updated), "captured_at_utc": fetched_at_utc,
                       "lead_time_minutes": (utc(game["scheduled_start_utc"]) - fetched).total_seconds() / 60,
                       "identity_method": "EXACT_DATE_TEAMS_START_WITHIN_10_MINUTES_AND_GAME_NUMBER_WHEN_REQUIRED",
                       "identity_certification": status, "event_classification": classification,
                       "timing_status": timing_status,
                       "source_run_tag": run_tag, "request_class": REQUEST_CLASS,
                       "raw_source_path": raw_source_path, "raw_source_sha256": raw_source_sha256, **parsed}
                row["canonical_market_identity"] = (
                    f"{PROVIDER}|{BOOKMAKER_KEY}|{row['game_id']}|{row['market_type']}|"
                    f"{row['line_key']}|{fetched_at_utc}"
                )
                rows.append(row); admitted += 1
        audit.append({**audit_row, "admitted_market_rows": admitted})
    return rows, audit
