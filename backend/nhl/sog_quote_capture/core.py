"""Pure normalization and create-only archive logic for NHL SOG quotes."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

ALLOWED_RUN_TYPES = {"MIDDAY", "FINAL_PREGAME"}
GAME_TYPES = {1: "PRESEASON", 2: "REGULAR_SEASON", 3: "POSTSEASON"}
SOG_MARKETS = {"player_shots_on_goal", "player_shots_on_goal_alternate"}
QUOTE_COLUMNS = [
    "canonical_season","slate_date","run_id","run_timestamp_utc","game_id","scheduled_start_time_utc",
    "game_type_code","game_type_label","market_evaluation_status","player_id","player_name","source_player_id",
    "source_player_name","team","opponent","sportsbook","sportsbook_name","provider_event_id","provider_market_id",
    "provider_outcome_id","source_market_label","canonical_prop_type","raw_line","line","raw_side","side",
    "raw_price","price_format","decimal_price","provider_quote_timestamp_utc","provider_market_timestamp_utc",
    "source_timestamp_utc","capture_timestamp_utc","market_status","raw_payload_sha256","game_binding_status",
    "player_binding_status","quote_qualification_status","notes",
]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def parse_utc(value: Any) -> pd.Timestamp:
    out = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(out):
        raise ValueError(f"invalid UTC timestamp: {value!r}")
    return out


def optional_utc(value: Any) -> pd.Timestamp | None:
    out = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(out) else out


def iso(value: pd.Timestamp | None) -> str | None:
    return None if value is None else value.isoformat().replace("+00:00", "Z")


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def make_run_id(season: int, slate_date: str, run_timestamp_utc: str, run_type: str) -> str:
    if season != 2026:
        raise ValueError("prospective SOG quote capture requires canonical_season=2026")
    if run_type not in ALLOWED_RUN_TYPES:
        raise ValueError("invalid run_type")
    stamp = parse_utc(run_timestamp_utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"nhlsogquote_s{season}_d{slate_date.replace('-', '')}_t{stamp}_{run_type}_v1"


def american_to_decimal(value: Any) -> float:
    price = float(value)
    if not math.isfinite(price) or price == 0 or abs(price) < 100:
        raise ValueError("invalid American price")
    return 1 + price / 100 if price > 0 else 1 + 100 / abs(price)


def _game_binding(event: dict[str, Any], games: pd.DataFrame, tolerance_minutes: int) -> tuple[pd.Series | None, str, int]:
    event_id = str(event.get("id") or "")
    if "provider_event_id" in games.columns and event_id:
        exact = games[games.provider_event_id.fillna("").astype(str).eq(event_id)]
        if len(exact) == 1:
            return exact.iloc[0], "EXACT_EVENT_CROSSWALK", 1
        if len(exact) > 1:
            return None, "AMBIGUOUS", len(exact)
    home, away = norm(event.get("home_team")), norm(event.get("away_team"))
    commence = optional_utc(event.get("commence_time"))
    candidates = games[
        games.home_team.map(norm).eq(home) & games.away_team.map(norm).eq(away)
    ]
    if commence is not None:
        candidates = candidates[
            (games.loc[candidates.index, "scheduled_start_time_utc"] - commence).abs().dt.total_seconds()
            <= tolerance_minutes * 60
        ]
    else:
        candidates = candidates.iloc[0:0]
    if len(candidates) == 1:
        return candidates.iloc[0], "DETERMINISTIC_TEAM_TIME_BINDING", 1
    return None, "AMBIGUOUS" if len(candidates) > 1 else "UNBOUND", len(candidates)


def _player_binding(outcome: dict[str, Any], game: pd.Series | None, players: pd.DataFrame) -> tuple[pd.Series | None, str, int, str, str]:
    raw_side = str(outcome.get("name") or outcome.get("label") or "")
    description = str(outcome.get("description") or outcome.get("participant") or "")
    # The provider's common player-prop schema keeps the side in ``name`` and
    # player in ``description``. Preserve description as player identity even
    # when the side itself is malformed so SIDE_INVALID remains diagnosable.
    source_name = description or str(outcome.get("name") or "")
    source_id = str(outcome.get("participant_id") or outcome.get("player_id") or outcome.get("id") or "")
    if source_id and "provider_player_id" in players.columns:
        exact = players[players.provider_player_id.fillna("").astype(str).eq(source_id)]
        if len(exact) == 1:
            return exact.iloc[0], "EXACT_PROVIDER_ID", 1, source_name, source_id
        if len(exact) > 1:
            return None, "AMBIGUOUS", len(exact), source_name, source_id
    if source_id and "source_player_id" in players.columns:
        exact = players[players.source_player_id.fillna("").astype(str).eq(source_id)]
        if len(exact) == 1:
            return exact.iloc[0], "DETERMINISTIC_CROSSWALK", 1, source_name, source_id
    if game is None:
        return None, "UNBOUND", 0, source_name, source_id
    teams = {norm(game.home_team), norm(game.away_team)}
    candidates = players[
        players.player_name.map(norm).eq(norm(source_name)) & players.team.map(norm).isin(teams)
    ]
    if "game_id" in players.columns:
        candidates = candidates[pd.to_numeric(candidates.game_id, errors="coerce").eq(int(game.game_id))]
    if len(candidates) == 1:
        return candidates.iloc[0], "EXACT_NAME_TEAM_FALLBACK", 1, source_name, source_id
    return None, "AMBIGUOUS" if len(candidates) > 1 else "UNBOUND", len(candidates), source_name, source_id


def _market_status(book: dict[str, Any], market: dict[str, Any], outcome: dict[str, Any]) -> str:
    if any(x.get("suspended") is True or x.get("active") is False for x in (book, market, outcome)):
        return "SUSPENDED"
    return str(outcome.get("status") or market.get("status") or book.get("status") or "ACTIVE").upper()


def normalize_quotes(payload: Any, games: pd.DataFrame, players: pd.DataFrame, *, canonical_season: int,
                     slate_date: str, run_id: str, run_timestamp_utc: str, capture_timestamp_utc: str,
                     raw_payload_sha256: str, stale_minutes: int = 60, game_tolerance_minutes: int = 15
                     ) -> tuple[pd.DataFrame, pd.DataFrame]:
    capture = parse_utc(capture_timestamp_utc)
    run_stamp = parse_utc(run_timestamp_utc)
    games = games.copy()
    games["scheduled_start_time_utc"] = pd.to_datetime(games.scheduled_start_time_utc, utc=True, errors="coerce")
    events = payload.get("provider_response", []) if isinstance(payload, dict) else payload
    if not isinstance(events, list):
        raise ValueError("provider response must be a list")
    rows: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        game, game_status, game_candidates = _game_binding(event, games, game_tolerance_minutes)
        for book in event.get("bookmakers") or []:
            for market_index, market in enumerate(book.get("markets") or []):
                market_key = str(market.get("key") or "")
                for outcome_index, outcome in enumerate(market.get("outcomes") or []):
                    player, player_status, player_candidates, source_name, source_id = _player_binding(outcome, game, players)
                    raw_side = str(outcome.get("name") or outcome.get("label") or "")
                    side = raw_side.strip().upper() if raw_side.strip().upper() in {"OVER", "UNDER"} else None
                    raw_line = outcome.get("point")
                    try:
                        line = float(raw_line)
                        if not math.isfinite(line) or line < 0:
                            raise ValueError
                    except (TypeError, ValueError):
                        line = None
                    try:
                        decimal = american_to_decimal(outcome.get("price"))
                        price_valid = True
                    except (TypeError, ValueError):
                        decimal, price_valid = None, False
                    provider_quote = optional_utc(outcome.get("last_update") or outcome.get("timestamp"))
                    market_time = optional_utc(market.get("last_update") or book.get("last_update"))
                    source_time = optional_utc(event.get("last_update") or market.get("last_update") or book.get("last_update"))
                    start = optional_utc(game.scheduled_start_time_utc) if game is not None else None
                    status = _market_status(book, market, outcome)
                    notes: list[str] = []
                    if market_key not in SOG_MARKETS:
                        qualification = "MARKET_UNSUPPORTED"
                    elif game_status == "AMBIGUOUS": qualification = "GAME_BINDING_AMBIGUOUS"
                    elif game_status == "UNBOUND": qualification = "UNQUALIFIED_OTHER"
                    elif player_status == "AMBIGUOUS": qualification = "PLAYER_BINDING_AMBIGUOUS"
                    elif player_status == "UNBOUND": qualification = "UNQUALIFIED_OTHER"
                    elif line is None: qualification = "LINE_INVALID"
                    elif side is None: qualification = "SIDE_INVALID"
                    elif status == "SUSPENDED": qualification = "SUSPENDED"
                    elif not price_valid: qualification = "PRICE_INVALID"
                    elif start is None: qualification = "UNQUALIFIED_OTHER"
                    elif capture >= start or any(t is not None and t >= start for t in (provider_quote, market_time, source_time)):
                        qualification = "POST_START_INVALID"
                    elif provider_quote is not None or market_time is not None or source_time is not None:
                        newest = provider_quote or market_time or source_time
                        qualification = "STALE" if (capture - newest).total_seconds() > stale_minutes * 60 else "PREGAME_QUALIFIED_PROVIDER_TIMESTAMP"
                    elif capture < start:
                        qualification = "PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN"
                    else:
                        qualification = "TIMESTAMP_MISSING"
                    if run_stamp < capture:
                        notes.append("capture_after_run_timestamp")
                    code = int(game.game_type_code) if game is not None and pd.notna(game.game_type_code) else None
                    label = GAME_TYPES.get(code, "UNKNOWN_GAME_TYPE")
                    evaluation = {1:"PRESEASON_NON_EVALUATION",2:"REGULAR_SEASON_EVALUATION_ELIGIBILITY_PENDING_OUTCOME",3:"POSTSEASON_NON_REGULAR_SEASON_EVALUATION"}.get(code,"UNKNOWN_GAME_TYPE_NON_EVALUATION")
                    row = {
                        "canonical_season":canonical_season,"slate_date":slate_date,"run_id":run_id,"run_timestamp_utc":iso(run_stamp),
                        "game_id":game.game_id if game is not None else None,"scheduled_start_time_utc":iso(start),"game_type_code":code,
                        "game_type_label":label,"market_evaluation_status":evaluation,"player_id":player.player_id if player is not None else None,
                        "player_name":player.player_name if player is not None else None,"source_player_id":source_id,"source_player_name":source_name,
                        "team":player.team if player is not None else None,"opponent":next((x for x in [game.home_team,game.away_team] if player is not None and norm(x)!=norm(player.team)),None) if game is not None else None,
                        "sportsbook":book.get("key"),"sportsbook_name":book.get("title"),"provider_event_id":event.get("id"),
                        "provider_market_id":market.get("id") or f"{event.get('id')}:{book.get('key')}:{market_key}:{market_index}",
                        "provider_outcome_id":outcome.get("id") or None,"source_market_label":market_key,"canonical_prop_type":"shots_on_goal" if market_key in SOG_MARKETS else None,
                        "raw_line":raw_line,"line":line,"raw_side":raw_side,"side":side,"raw_price":outcome.get("price"),"price_format":"american",
                        "decimal_price":decimal,"provider_quote_timestamp_utc":iso(provider_quote),"provider_market_timestamp_utc":iso(market_time),
                        "source_timestamp_utc":iso(source_time),"capture_timestamp_utc":iso(capture),"market_status":status,
                        "raw_payload_sha256":raw_payload_sha256,"game_binding_status":game_status,"player_binding_status":player_status,
                        "quote_qualification_status":qualification,"notes":";".join(notes),
                    }
                    rows.append(row)
                    audits.append({"provider_event_id":event.get("id"),"sportsbook":book.get("key"),"source_market_label":market_key,
                        "source_player_name":source_name,"game_binding_status":game_status,"game_candidate_count":game_candidates,
                        "game_id":row["game_id"],"player_binding_status":player_status,"player_candidate_count":player_candidates,
                        "player_id":row["player_id"],"quote_qualification_status":qualification})
    return pd.DataFrame(rows, columns=QUOTE_COLUMNS), pd.DataFrame(audits)


def write_manifest(directory: Path) -> None:
    files = sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (directory / "SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))


def capture_run(*, payload_json: Path, games_csv: Path, players_csv: Path, output_root: Path,
                slate_date: str, run_timestamp_utc: str, run_type: str, source: str = "THE_ODDS_API",
                request_metadata: dict[str, Any] | None = None) -> Path:
    run_id = make_run_id(2026, slate_date, run_timestamp_utc, run_type)
    destination = output_root / "2026" / slate_date / run_id
    if destination.exists():
        raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    raw_bytes = payload_json.read_bytes()
    raw_obj = json.loads(raw_bytes)
    capture_text = raw_obj.get("capture_timestamp_utc") if isinstance(raw_obj, dict) else None
    if not capture_text:
        raise ValueError("raw envelope requires actual capture_timestamp_utc")
    provider = raw_obj.get("provider_response", []) if isinstance(raw_obj, dict) else raw_obj
    envelope = {"acquisition_timestamp_utc":capture_text,"capture_timestamp_utc":capture_text,"provider":source,
        "request_metadata":request_metadata or (raw_obj.get("request_metadata", {}) if isinstance(raw_obj, dict) else {}),
        "requested_market_families":sorted(SOG_MARKETS),"provider_response":provider}
    raw_output = (json.dumps(envelope, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    raw_hash = sha256_bytes(raw_output)
    # Claim the run destination and persist the complete provider response
    # before attempting any normalization. A normalization failure therefore
    # leaves auditable raw evidence in a fail-closed, non-reusable run path.
    destination.mkdir(parents=True, exist_ok=False)
    (destination / "raw_odds_response.json").write_bytes(raw_output)
    games, players = pd.read_csv(games_csv), pd.read_csv(players_csv)
    quotes, binding = normalize_quotes(envelope, games, players, canonical_season=2026, slate_date=slate_date,
        run_id=run_id, run_timestamp_utc=run_timestamp_utc, capture_timestamp_utc=capture_text, raw_payload_sha256=raw_hash)
    quotes.to_csv(destination / "sog_quotes.csv", index=False)
    binding.to_csv(destination / "quote_binding_audit.csv", index=False)
    timing_cols=["run_id","game_id","player_id","sportsbook","provider_quote_timestamp_utc","provider_market_timestamp_utc","source_timestamp_utc","capture_timestamp_utc","scheduled_start_time_utc","quote_qualification_status"]
    quotes[timing_cols].to_csv(destination / "quote_timing_audit.csv", index=False)
    qual = quotes.groupby("quote_qualification_status", dropna=False).size().reset_index(name="rows")
    qual.to_csv(destination / "quote_qualification_audit.csv", index=False)
    pregame = quotes.quote_qualification_status.astype(str).str.startswith("PREGAME_")
    post_bad = quotes.quote_qualification_status.eq("POST_START_INVALID") & pregame
    required = ["raw_odds_response.json","sog_quotes.csv","quote_binding_audit.csv","quote_timing_audit.csv","quote_qualification_audit.csv"]
    game_bound = quotes.game_binding_status.isin(["EXACT_EVENT_CROSSWALK","DETERMINISTIC_TEAM_TIME_BINDING"])
    player_bound = quotes.player_binding_status.isin(["EXACT_PROVIDER_ID","DETERMINISTIC_CROSSWALK","EXACT_NAME_TEAM_FALLBACK"])
    critical = bool(sha256_bytes((destination/"raw_odds_response.json").read_bytes()) == raw_hash and not post_bad.any() and quotes.sportsbook.notna().all())
    full_coverage = bool(len(quotes) and game_bound.all() and player_bound.all())
    provider_qualified = int(quotes.quote_qualification_status.eq("PREGAME_QUALIFIED_PROVIDER_TIMESTAMP").sum())
    capture_only = int(quotes.quote_qualification_status.eq("PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN").sum())
    health = "FAIL_CLOSED" if not critical else ("PASS" if full_coverage and provider_qualified and not capture_only else ("PASS_WITH_BOUNDED_TIMESTAMP_LIMITS" if full_coverage and capture_only else "PARTIAL_COVERAGE"))
    events = provider if isinstance(provider, list) else []
    metadata={"run_id":run_id,"canonical_season":2026,"slate_date":slate_date,"run_timestamp_utc":iso(parse_utc(run_timestamp_utc)),
        "run_type":run_type,"source":source,"raw_payload_sha256":raw_hash,"total_source_events":len(events),
        "total_sog_markets":int(sum(1 for e in events for b in (e.get('bookmakers') or []) for m in (b.get('markets') or []) if m.get('key') in SOG_MARKETS)),
        "normalized_quote_rows":len(quotes),"bound_games":int(quotes.loc[game_bound,'game_id'].nunique()),"bound_players":int(quotes.loc[player_bound,'player_id'].nunique()),
        "provider_timestamp_qualified_quotes":provider_qualified,"capture_only_qualified_quotes":capture_only,
        "invalid_post_start_rows":int(quotes.quote_qualification_status.eq('POST_START_INVALID').sum()),"sportsbook_count":int(quotes.sportsbook.nunique()),
        "health_gate_result":health,"candidate_rows":0,"recommendations_generated":0,"execution_rows":0}
    (destination / "run_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    assert all((destination/name).exists() for name in required)
    write_manifest(destination)
    return destination
