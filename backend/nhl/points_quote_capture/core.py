"""Create-only, book-level NHL Points quote capture."""
from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd

ALLOWED_RUN_TYPES = {"MIDDAY", "FINAL_PREGAME"}
GAME_TYPES = {1: "PRESEASON", 2: "REGULAR_SEASON", 3: "POSTSEASON"}
POINTS_MARKETS = {"player_points"}
QUALIFIED = {
    "PREGAME_QUALIFIED_PROVIDER_TIMESTAMP",
    "PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN",
}
QUOTE_COLUMNS = [
    "canonical_season", "slate_date", "run_id", "run_type", "run_timestamp_utc",
    "game_id", "scheduled_start_time_utc", "game_type_code", "game_type_label",
    "market_evaluation_status", "player_id", "player_name", "source_player_id",
    "source_player_name", "team", "opponent", "sportsbook", "sportsbook_name",
    "provider_event_id", "provider_market_id", "provider_outcome_id", "source_market_label",
    "canonical_prop_type", "raw_line", "line", "raw_side", "side", "raw_price",
    "price_format", "decimal_price", "provider_quote_timestamp_utc",
    "provider_market_timestamp_utc", "source_timestamp_utc", "capture_timestamp_utc",
    "market_status", "raw_payload_sha256", "game_binding_status", "player_binding_status",
    "quote_qualification_status", "notes",
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
        raise ValueError("WRONG_CANONICAL_SEASON")
    if run_type not in ALLOWED_RUN_TYPES:
        raise ValueError("INVALID_RUN_TYPE")
    stamp = parse_utc(run_timestamp_utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"nhlpointsquote_s{season}_d{slate_date.replace('-', '')}_t{stamp}_{run_type}_v1"


def american_to_decimal(value: Any) -> float:
    price = float(value)
    if not math.isfinite(price) or price == 0 or abs(price) < 100:
        raise ValueError("invalid American price")
    return 1 + price / 100 if price > 0 else 1 + 100 / abs(price)


def _game_binding(event: dict[str, Any], games: pd.DataFrame, tolerance_minutes: int):
    event_id = str(event.get("id") or "")
    if "provider_event_id" in games and event_id:
        exact = games[games.provider_event_id.fillna("").astype(str).eq(event_id)]
        if len(exact) == 1:
            return exact.iloc[0], "EXACT_EVENT_CROSSWALK", 1
        if len(exact) > 1:
            return None, "AMBIGUOUS", len(exact)
    home, away = norm(event.get("home_team")), norm(event.get("away_team"))
    commence = optional_utc(event.get("commence_time"))
    candidates = games[games.home_team.map(norm).eq(home) & games.away_team.map(norm).eq(away)]
    if commence is None:
        candidates = candidates.iloc[0:0]
    else:
        delta = (games.loc[candidates.index, "scheduled_start_time_utc"] - commence).abs()
        candidates = candidates[delta.dt.total_seconds() <= tolerance_minutes * 60]
    if len(candidates) == 1:
        return candidates.iloc[0], "DETERMINISTIC_TEAM_TIME_BINDING", 1
    return None, "AMBIGUOUS" if len(candidates) > 1 else "UNBOUND", len(candidates)


def _player_binding(outcome: dict[str, Any], game: pd.Series | None, players: pd.DataFrame):
    raw_side = str(outcome.get("name") or outcome.get("label") or "")
    source_name = str(outcome.get("description") or outcome.get("participant") or outcome.get("player") or "")
    source_id = str(outcome.get("participant_id") or outcome.get("player_id") or "")
    if source_id and "provider_player_id" in players:
        exact = players[players.provider_player_id.fillna("").astype(str).eq(source_id)]
        if len(exact) == 1:
            return exact.iloc[0], "EXACT_PROVIDER_ID", 1, source_name, source_id
        if len(exact) > 1:
            return None, "AMBIGUOUS", len(exact), source_name, source_id
    if game is None:
        return None, "UNBOUND", 0, source_name, source_id
    teams = {norm(game.home_team), norm(game.away_team)}
    candidates = players[players.player_name.map(norm).eq(norm(source_name)) & players.team.map(norm).isin(teams)]
    if "game_id" in players:
        candidates = candidates[pd.to_numeric(candidates.game_id, errors="coerce").eq(int(game.game_id))]
    if len(candidates) == 1:
        return candidates.iloc[0], "EXACT_NAME_TEAM_FALLBACK", 1, source_name, source_id
    return None, "AMBIGUOUS" if len(candidates) > 1 else "UNBOUND", len(candidates), source_name, source_id


def _market_status(book: dict[str, Any], market: dict[str, Any], outcome: dict[str, Any]) -> str:
    if any(x.get("suspended") is True or x.get("active") is False for x in (book, market, outcome)):
        return "SUSPENDED"
    return str(outcome.get("status") or market.get("status") or book.get("status") or "ACTIVE").upper()


def normalize_quotes(
    payload: Any,
    games: pd.DataFrame,
    players: pd.DataFrame,
    *,
    slate_date: str,
    run_id: str,
    run_type: str,
    run_timestamp_utc: str,
    capture_timestamp_utc: str,
    raw_payload_sha256: str,
    stale_minutes: int = 60,
    game_tolerance_minutes: int = 15,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    capture, run_stamp = parse_utc(capture_timestamp_utc), parse_utc(run_timestamp_utc)
    games = games.copy()
    games["scheduled_start_time_utc"] = pd.to_datetime(games.scheduled_start_time_utc, utc=True, errors="coerce")
    events = payload.get("provider_response", []) if isinstance(payload, dict) else payload
    if not isinstance(events, list):
        raise ValueError("provider response must be a list")
    rows, audits = [], []
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
                        decimal, price_valid = american_to_decimal(outcome.get("price")), True
                    except (TypeError, ValueError):
                        decimal, price_valid = None, False
                    provider_quote = optional_utc(outcome.get("last_update") or outcome.get("timestamp"))
                    market_time = optional_utc(market.get("last_update") or book.get("last_update"))
                    source_time = optional_utc(event.get("last_update") or market.get("last_update") or book.get("last_update"))
                    start = optional_utc(game.scheduled_start_time_utc) if game is not None else None
                    status = _market_status(book, market, outcome)
                    if market_key not in POINTS_MARKETS:
                        qualification = "MARKET_UNSUPPORTED"
                    elif game_status == "AMBIGUOUS":
                        qualification = "GAME_BINDING_AMBIGUOUS"
                    elif game_status == "UNBOUND":
                        qualification = "GAME_UNBOUND"
                    elif player_status == "AMBIGUOUS":
                        qualification = "PLAYER_BINDING_AMBIGUOUS"
                    elif player_status == "UNBOUND":
                        qualification = "PLAYER_UNBOUND"
                    elif line is None:
                        qualification = "LINE_INVALID"
                    elif side is None:
                        qualification = "SIDE_INVALID"
                    elif status != "ACTIVE":
                        qualification = "SUSPENDED"
                    elif not price_valid:
                        qualification = "PRICE_INVALID"
                    elif start is None:
                        qualification = "SCHEDULE_UNBOUND"
                    elif capture >= start or any(t is not None and t >= start for t in (provider_quote, market_time, source_time)):
                        qualification = "POST_START_INVALID"
                    elif any(t is not None and t > capture for t in (provider_quote, market_time, source_time)):
                        qualification = "SOURCE_TIMESTAMP_AFTER_CAPTURE"
                    elif provider_quote is not None or market_time is not None or source_time is not None:
                        newest = provider_quote or market_time or source_time
                        qualification = "STALE" if (capture - newest).total_seconds() > stale_minutes * 60 else "PREGAME_QUALIFIED_PROVIDER_TIMESTAMP"
                    else:
                        qualification = "PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN"
                    code = int(game.game_type_code) if game is not None and pd.notna(game.game_type_code) else None
                    if code not in GAME_TYPES:
                        qualification = "WRONG_OR_UNKNOWN_GAME_TYPE"
                    label = GAME_TYPES.get(code, "UNKNOWN_GAME_TYPE")
                    evaluation = {1: "PRESEASON_NON_EVALUATION", 2: "REGULAR_SEASON_EVALUATION_ELIGIBILITY_PENDING_OUTCOME", 3: "POSTSEASON_NON_REGULAR_SEASON_EVALUATION"}.get(code, "UNKNOWN_GAME_TYPE_NON_EVALUATION")
                    row = {
                        "canonical_season": 2026, "slate_date": slate_date, "run_id": run_id,
                        "run_type": run_type, "run_timestamp_utc": iso(run_stamp),
                        "game_id": game.game_id if game is not None else None,
                        "scheduled_start_time_utc": iso(start), "game_type_code": code,
                        "game_type_label": label, "market_evaluation_status": evaluation,
                        "player_id": player.player_id if player is not None else None,
                        "player_name": player.player_name if player is not None else None,
                        "source_player_id": source_id, "source_player_name": source_name,
                        "team": player.team if player is not None else None,
                        "opponent": next((x for x in [game.home_team, game.away_team] if player is not None and norm(x) != norm(player.team)), None) if game is not None else None,
                        "sportsbook": book.get("key"), "sportsbook_name": book.get("title"),
                        "provider_event_id": event.get("id"),
                        "provider_market_id": market.get("id") or f"{event.get('id')}:{book.get('key')}:{market_key}:{market_index}",
                        "provider_outcome_id": outcome.get("id") or f"{market_index}:{outcome_index}",
                        "source_market_label": market_key,
                        "canonical_prop_type": "player_points" if market_key in POINTS_MARKETS else None,
                        "raw_line": raw_line, "line": line, "raw_side": raw_side, "side": side,
                        "raw_price": outcome.get("price"), "price_format": "american", "decimal_price": decimal,
                        "provider_quote_timestamp_utc": iso(provider_quote),
                        "provider_market_timestamp_utc": iso(market_time), "source_timestamp_utc": iso(source_time),
                        "capture_timestamp_utc": iso(capture), "market_status": status,
                        "raw_payload_sha256": raw_payload_sha256, "game_binding_status": game_status,
                        "player_binding_status": player_status, "quote_qualification_status": qualification,
                        "notes": "capture_after_declared_run" if capture > run_stamp else "",
                    }
                    rows.append(row)
                    audits.append({
                        "provider_event_id": event.get("id"), "sportsbook": book.get("key"),
                        "source_market_label": market_key, "source_player_name": source_name,
                        "game_binding_status": game_status, "game_candidate_count": game_candidates,
                        "game_id": row["game_id"], "player_binding_status": player_status,
                        "player_candidate_count": player_candidates, "player_id": row["player_id"],
                        "quote_qualification_status": qualification,
                    })
    return pd.DataFrame(rows, columns=QUOTE_COLUMNS), pd.DataFrame(audits)


def write_manifest(directory: Path, *, complete_only: bool = False) -> None:
    files = sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    if complete_only and not (directory / "RUN_COMPLETE.json").exists():
        raise RuntimeError("INCOMPLETE_RUN_CANNOT_BE_MANIFESTED")
    (directory / "SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))


def capture_run(
    *, payload_json: Path, games_csv: Path, players_csv: Path, parent_manifest: Path,
    output_root: Path, slate_date: str, run_timestamp_utc: str, run_type: str,
    source: str = "THE_ODDS_API",
) -> Path:
    run_id = make_run_id(2026, slate_date, run_timestamp_utc, run_type)
    destination = output_root / "2026" / slate_date / run_id
    staging = destination.with_name(destination.name + ".incomplete")
    if destination.exists() or staging.exists():
        raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    expected = {}
    for entry in parent_manifest.read_text().splitlines():
        digest, name = entry.split("  ", 1)
        expected[name] = digest
    if games_csv.name not in expected or sha256_file(games_csv) != expected[games_csv.name]:
        raise RuntimeError("PARENT_HASH_MISMATCH_OR_MUTABLE_GAME_SPINE")
    if players_csv.name not in expected or sha256_file(players_csv) != expected[players_csv.name]:
        raise RuntimeError("PARENT_HASH_MISMATCH_OR_MUTABLE_PLAYER_SPINE")
    raw_obj = json.loads(payload_json.read_text())
    capture_text = raw_obj.get("capture_timestamp_utc") if isinstance(raw_obj, dict) else None
    if not capture_text:
        raise ValueError("RAW_ENVELOPE_REQUIRES_CAPTURE_TIMESTAMP")
    if parse_utc(capture_text) > parse_utc(run_timestamp_utc):
        raise RuntimeError("QUOTE_CAPTURE_AFTER_DECLARED_RUN_TIMESTAMP")
    provider = raw_obj.get("provider_response", []) if isinstance(raw_obj, dict) else raw_obj
    envelope = {
        "capture_timestamp_utc": capture_text, "provider": source,
        "request_metadata": raw_obj.get("request_metadata", {}) if isinstance(raw_obj, dict) else {},
        "requested_market_families": sorted(POINTS_MARKETS), "provider_response": provider,
    }
    raw_output = (json.dumps(envelope, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
    raw_hash = sha256_bytes(raw_output)
    games, players = pd.read_csv(games_csv), pd.read_csv(players_csv)
    if games.empty or not games.canonical_season.eq(2026).all() or not games.slate_date.astype(str).eq(slate_date).all():
        raise RuntimeError("GAME_SPINE_IDENTITY_MISMATCH")
    if games.game_id.duplicated().any() or not games.game_type_code.isin(GAME_TYPES).all():
        raise RuntimeError("GAME_SPINE_DUPLICATE_OR_UNKNOWN_GAME_TYPE")
    staging.mkdir(parents=True, exist_ok=False)
    try:
        (staging / "raw_odds_response.json").write_bytes(raw_output)
        quotes, binding = normalize_quotes(
            envelope, games, players, slate_date=slate_date, run_id=run_id, run_type=run_type,
            run_timestamp_utc=run_timestamp_utc, capture_timestamp_utc=capture_text,
            raw_payload_sha256=raw_hash,
        )
        quotes.to_csv(staging / "points_quotes.csv", index=False)
        binding.to_csv(staging / "quote_binding_audit.csv", index=False)
        timing = ["run_id", "game_id", "player_id", "sportsbook", "provider_quote_timestamp_utc", "provider_market_timestamp_utc", "source_timestamp_utc", "capture_timestamp_utc", "scheduled_start_time_utc", "quote_qualification_status"]
        quotes[timing].to_csv(staging / "quote_timing_audit.csv", index=False)
        quotes.groupby("quote_qualification_status", dropna=False).size().reset_index(name="rows").to_csv(staging / "quote_qualification_audit.csv", index=False)
        metadata = {
            "schema_version": "nhl_points_quote_capture_v1", "run_id": run_id,
            "canonical_season": 2026, "slate_date": slate_date, "run_type": run_type,
            "run_timestamp_utc": iso(parse_utc(run_timestamp_utc)), "capture_timestamp_utc": iso(parse_utc(capture_text)),
            "source": source, "raw_payload_sha256": raw_hash,
            "parent_game_spine_sha256": sha256_file(games_csv), "parent_manifest_sha256": sha256_file(parent_manifest),
            "normalized_quote_rows": len(quotes), "qualified_quote_rows": int(quotes.quote_qualification_status.isin(QUALIFIED).sum()),
            "post_start_rows": int(quotes.quote_qualification_status.eq("POST_START_INVALID").sum()),
            "sportsbook_count": int(quotes.sportsbook.nunique()), "candidate_rows": 0,
            "upload_rows": 0, "execution_rows": 0,
        }
        (staging / "run_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        (staging / "RUN_COMPLETE.json").write_text(json.dumps({"run_id": run_id, "status": "COMPLETE"}, sort_keys=True) + "\n")
        write_manifest(staging, complete_only=True)
        staging.rename(destination)
    except BaseException:
        raise
    return destination
