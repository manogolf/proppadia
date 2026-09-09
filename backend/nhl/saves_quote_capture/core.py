"""Create-only NHL Saves quote capture with deterministic goalie binding."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

RUN_TYPES = {"MIDDAY", "FINAL_PREGAME"}
GAME_TYPES = {1: "PRESEASON", 2: "REGULAR_SEASON", 3: "POSTSEASON"}
SAVES_MARKETS = {"player_total_saves", "goalie_saves"}
QUALIFIED = {
    "PREGAME_QUALIFIED_PROVIDER_TIMESTAMP",
    "PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN",
}
UNAVAILABLE = {"SUSPENDED", "CLOSED", "INACTIVE", "UNAVAILABLE"}
QUOTE_COLUMNS = [
    "canonical_season", "slate_date", "run_id", "run_type", "run_timestamp_utc",
    "game_id", "scheduled_start_time_utc", "game_type_code", "game_type_label",
    "market_evaluation_status", "source", "goalie_id", "goalie_name", "received_goalie_name",
    "source_goalie_id", "team", "opponent", "sportsbook", "sportsbook_name",
    "provider_event_id", "provider_market_id", "provider_outcome_id", "source_market_label",
    "canonical_prop_type", "raw_line", "line", "raw_side", "side", "raw_price",
    "price_format", "decimal_price", "provider_quote_timestamp_utc",
    "provider_market_timestamp_utc", "source_timestamp_utc", "capture_timestamp_utc",
    "market_status", "raw_payload_sha256", "game_binding_status", "goalie_binding_status",
    "quote_qualification_status", "notes",
]


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def parse_utc(value: Any) -> pd.Timestamp:
    stamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(stamp):
        raise ValueError(f"INVALID_UTC_TIMESTAMP:{value!r}")
    return stamp


def optional_utc(value: Any) -> pd.Timestamp | None:
    stamp = pd.to_datetime(value, utc=True, errors="coerce")
    return None if pd.isna(stamp) else stamp


def iso(value: pd.Timestamp | None) -> str | None:
    return None if value is None else value.isoformat().replace("+00:00", "Z")


def norm(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", text.lower())


def make_run_id(season: int, slate_date: str, run_timestamp_utc: str, run_type: str) -> str:
    if season != 2026:
        raise ValueError("WRONG_CANONICAL_SEASON")
    if run_type not in RUN_TYPES:
        raise ValueError("INVALID_RUN_TYPE")
    stamp = parse_utc(run_timestamp_utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"nhlsavesquote_s{season}_d{slate_date.replace('-', '')}_t{stamp}_{run_type}_v1"


def american_to_decimal(value: Any) -> float:
    price = float(value)
    if not math.isfinite(price) or price == 0 or abs(price) < 100:
        raise ValueError("INVALID_AMERICAN_PRICE")
    return 1 + price / 100 if price > 0 else 1 + 100 / abs(price)


def _game_binding(event: dict[str, Any], games: pd.DataFrame, tolerance_minutes: int):
    event_id = str(event.get("id") or "")
    if event_id and "provider_event_id" in games:
        found = games[games.provider_event_id.fillna("").astype(str).eq(event_id)]
        if len(found) == 1:
            return found.iloc[0], "EXACT_EVENT_CROSSWALK", 1
        if len(found) > 1:
            return None, "AMBIGUOUS", len(found)
    commence = optional_utc(event.get("commence_time"))
    found = games[
        games.home_team.map(norm).eq(norm(event.get("home_team")))
        & games.away_team.map(norm).eq(norm(event.get("away_team")))
    ]
    if commence is None:
        found = found.iloc[0:0]
    else:
        delta = (games.loc[found.index, "scheduled_start_time_utc"] - commence).abs()
        found = found[delta.dt.total_seconds() <= tolerance_minutes * 60]
    if len(found) == 1:
        return found.iloc[0], "DETERMINISTIC_TEAM_TIME_BINDING", 1
    return None, "AMBIGUOUS" if len(found) > 1 else "UNBOUND", len(found)


def _goalie_aliases(row: pd.Series) -> set[str]:
    values = {norm(row.goalie_name)}
    for item in str(row.get("goalie_aliases", "") or "").split("|"):
        if norm(item):
            values.add(norm(item))
    return values


def _goalie_binding(outcome: dict[str, Any], game: pd.Series | None, goalies: pd.DataFrame):
    source_name = str(outcome.get("description") or outcome.get("participant") or outcome.get("player") or "")
    source_id = str(outcome.get("participant_id") or outcome.get("player_id") or "")
    if game is None:
        return None, "UNBOUND", 0, source_name, source_id
    pool = goalies[pd.to_numeric(goalies.game_id, errors="coerce").eq(int(game.game_id))]
    if source_id and "provider_goalie_id" in pool:
        exact = pool[pool.provider_goalie_id.fillna("").astype(str).eq(source_id)]
        if len(exact) == 1:
            return exact.iloc[0], "EXACT_PROVIDER_ID", 1, source_name, source_id
        if len(exact) > 1:
            return None, "AMBIGUOUS", len(exact), source_name, source_id
    target = norm(source_name)
    matches = [index for index, row in pool.iterrows() if target and target in _goalie_aliases(row)]
    if len(matches) == 1:
        row = pool.loc[matches[0]]
        status = "EXACT_CANONICAL_NAME" if target == norm(row.goalie_name) else "DETERMINISTIC_UNIQUE_ALIAS"
        return row, status, 1, source_name, source_id
    return None, "AMBIGUOUS" if len(matches) > 1 else "UNBOUND", len(matches), source_name, source_id


def _status(book: dict[str, Any], market: dict[str, Any], outcome: dict[str, Any]) -> str:
    if any(x.get("suspended") is True or x.get("active") is False for x in (book, market, outcome)):
        return "SUSPENDED"
    return str(outcome.get("status") or market.get("status") or book.get("status") or "ACTIVE").upper()


def normalize_quotes(
    payload: Any, games: pd.DataFrame, goalies: pd.DataFrame, *, slate_date: str,
    run_id: str, run_type: str, run_timestamp_utc: str, capture_timestamp_utc: str,
    raw_payload_sha256: str, source: str = "THE_ODDS_API", stale_minutes: int = 60, game_tolerance_minutes: int = 15,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    capture, run_stamp = parse_utc(capture_timestamp_utc), parse_utc(run_timestamp_utc)
    games = games.copy()
    games["scheduled_start_time_utc"] = pd.to_datetime(games.scheduled_start_time_utc, utc=True, errors="coerce")
    events = payload.get("provider_response", []) if isinstance(payload, dict) else payload
    if not isinstance(events, list):
        raise ValueError("PROVIDER_RESPONSE_NOT_LIST")
    rows: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        game, game_status, game_count = _game_binding(event, games, game_tolerance_minutes)
        for book in event.get("bookmakers") or []:
            for mi, market in enumerate(book.get("markets") or []):
                market_key = str(market.get("key") or "")
                for oi, outcome in enumerate(market.get("outcomes") or []):
                    goalie, goalie_status, goalie_count, source_name, source_id = _goalie_binding(outcome, game, goalies)
                    raw_side = str(outcome.get("name") or outcome.get("label") or "")
                    side = raw_side.upper().strip() if raw_side.upper().strip() in {"OVER", "UNDER"} else None
                    try:
                        line = float(outcome.get("point"))
                        if not math.isfinite(line) or line < 0:
                            raise ValueError
                    except (TypeError, ValueError):
                        line = None
                    try:
                        decimal, valid_price = american_to_decimal(outcome.get("price")), True
                    except (TypeError, ValueError):
                        decimal, valid_price = None, False
                    qtime = optional_utc(outcome.get("last_update") or outcome.get("timestamp"))
                    mtime = optional_utc(market.get("last_update") or book.get("last_update"))
                    stime = optional_utc(event.get("last_update") or market.get("last_update") or book.get("last_update"))
                    start = optional_utc(game.scheduled_start_time_utc) if game is not None else None
                    status = _status(book, market, outcome)
                    known_times = [x for x in (qtime, mtime, stime) if x is not None]
                    newest = max(known_times) if known_times else None
                    if market_key not in SAVES_MARKETS: qualification = "MARKET_UNSUPPORTED"
                    elif game_status == "AMBIGUOUS": qualification = "EVENT_MISMATCH_AMBIGUOUS"
                    elif game_status == "UNBOUND": qualification = "EVENT_MISMATCH_UNBOUND"
                    elif goalie_status == "AMBIGUOUS": qualification = "GOALIE_IDENTITY_AMBIGUOUS"
                    elif goalie_status == "UNBOUND": qualification = "GOALIE_IDENTITY_UNBOUND"
                    elif line is None: qualification = "LINE_INVALID"
                    elif side is None: qualification = "SIDE_INVALID"
                    elif status in UNAVAILABLE or status != "ACTIVE": qualification = "UNAVAILABLE_STATUS"
                    elif not valid_price: qualification = "PRICE_INVALID"
                    elif start is None: qualification = "TIMING_INDETERMINATE"
                    elif capture >= start or any(x is not None and x >= start for x in (qtime, mtime, stime)): qualification = "POST_START_INVALID"
                    elif any(x is not None and x > capture for x in (qtime, mtime, stime)): qualification = "SOURCE_TIMESTAMP_AFTER_CAPTURE"
                    elif newest is not None: qualification = "STALE" if (capture-newest).total_seconds() > stale_minutes*60 else "PREGAME_QUALIFIED_PROVIDER_TIMESTAMP"
                    else: qualification = "PREGAME_CAPTURE_QUALIFIED_SOURCE_TIMESTAMP_UNKNOWN"
                    code = int(game.game_type_code) if game is not None and pd.notna(game.game_type_code) else None
                    if code not in GAME_TYPES:
                        qualification = "WRONG_OR_UNKNOWN_GAME_TYPE"
                    evaluation = {1:"PRESEASON_NON_EVALUATION",2:"REGULAR_SEASON_EVALUATION_ELIGIBILITY_PENDING_OUTCOME",3:"POSTSEASON_NON_REGULAR_SEASON_EVALUATION"}.get(code,"UNKNOWN_GAME_TYPE_NON_EVALUATION")
                    opponent = None
                    if game is not None and goalie is not None:
                        opponent = game.away_team if norm(goalie.team) == norm(game.home_team) else game.home_team
                    row = {
                        "canonical_season":2026,"slate_date":slate_date,"run_id":run_id,"run_type":run_type,
                        "run_timestamp_utc":iso(run_stamp),"game_id":None if game is None else game.game_id,
                        "scheduled_start_time_utc":iso(start),"game_type_code":code,"game_type_label":GAME_TYPES.get(code,"UNKNOWN"),
                        "market_evaluation_status":evaluation,"source":source,"goalie_id":None if goalie is None else goalie.goalie_id,
                        "goalie_name":None if goalie is None else goalie.goalie_name,"received_goalie_name":source_name,
                        "source_goalie_id":source_id,"team":None if goalie is None else goalie.team,"opponent":opponent,
                        "sportsbook":book.get("key"),"sportsbook_name":book.get("title"),"provider_event_id":event.get("id"),
                        "provider_market_id":market.get("id") or f"{event.get('id')}:{book.get('key')}:{market_key}:{mi}",
                        "provider_outcome_id":outcome.get("id") or f"{mi}:{oi}","source_market_label":market_key,
                        "canonical_prop_type":"goalie_saves" if market_key in SAVES_MARKETS else None,
                        "raw_line":outcome.get("point"),"line":line,"raw_side":raw_side,"side":side,
                        "raw_price":outcome.get("price"),"price_format":"american","decimal_price":decimal,
                        "provider_quote_timestamp_utc":iso(qtime),"provider_market_timestamp_utc":iso(mtime),
                        "source_timestamp_utc":iso(stime),"capture_timestamp_utc":iso(capture),"market_status":status,
                        "raw_payload_sha256":raw_payload_sha256,"game_binding_status":game_status,
                        "goalie_binding_status":goalie_status,"quote_qualification_status":qualification,
                        "notes":"capture_after_declared_run" if capture > run_stamp else "",
                    }
                    rows.append(row)
                    audits.append({"provider_event_id":event.get("id"),"sportsbook":book.get("key"),"received_goalie_name":source_name,
                        "game_binding_status":game_status,"game_candidate_count":game_count,"game_id":row["game_id"],
                        "goalie_binding_status":goalie_status,"goalie_candidate_count":goalie_count,"goalie_id":row["goalie_id"],
                        "quote_qualification_status":qualification})
    return pd.DataFrame(rows, columns=QUOTE_COLUMNS), pd.DataFrame(audits)


def write_manifest(directory: Path, complete_only: bool = False) -> None:
    if complete_only and not (directory / "RUN_COMPLETE.json").exists():
        raise RuntimeError("INCOMPLETE_RUN_CANNOT_BE_MANIFESTED")
    files = sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (directory / "SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))


def _verify_parent(path: Path, required: list[Path]) -> None:
    entries = {name: digest for digest, name in (line.split("  ", 1) for line in path.read_text().splitlines())}
    for item in required:
        if entries.get(item.name) != sha256_file(item):
            raise RuntimeError(f"PARENT_HASH_MISMATCH_OR_MUTABLE:{item.name}")


def capture_run(*, payload_json: Path, games_csv: Path, goalies_csv: Path, parent_manifest: Path,
                output_root: Path, slate_date: str, run_timestamp_utc: str, run_type: str,
                source: str = "THE_ODDS_API") -> Path:
    run_id = make_run_id(2026, slate_date, run_timestamp_utc, run_type)
    destination = output_root/"2026"/slate_date/run_id
    staging = destination.with_name(destination.name+".incomplete")
    lock_dir = output_root/"locks"; lock_dir.mkdir(parents=True, exist_ok=True)
    lock = (lock_dir/f"{slate_date}_{run_type}.lock").open("a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError("SAVES_QUOTE_CAPTURE_ALREADY_RUNNING") from exc
    if destination.exists() or staging.exists(): raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    _verify_parent(parent_manifest, [games_csv, goalies_csv])
    raw = json.loads(payload_json.read_text()); capture_text = raw.get("capture_timestamp_utc") if isinstance(raw,dict) else None
    if not capture_text: raise ValueError("RAW_ENVELOPE_REQUIRES_CAPTURE_TIMESTAMP")
    if parse_utc(capture_text)>parse_utc(run_timestamp_utc): raise RuntimeError("QUOTE_CAPTURE_AFTER_DECLARED_RUN_TIMESTAMP")
    envelope={"capture_timestamp_utc":capture_text,"provider":source,"request_metadata":raw.get("request_metadata",{}),
              "requested_market_families":["player_total_saves"],"provider_response":raw.get("provider_response",[])}
    raw_bytes=(json.dumps(envelope,sort_keys=True,separators=(",",":"),ensure_ascii=False)+"\n").encode(); raw_hash=sha256_bytes(raw_bytes)
    games,goalies=pd.read_csv(games_csv),pd.read_csv(goalies_csv)
    if games.empty or games.game_id.duplicated().any() or not games.canonical_season.eq(2026).all() or not games.slate_date.astype(str).eq(slate_date).all(): raise RuntimeError("GAME_SPINE_IDENTITY_MISMATCH")
    if not games.game_type_code.isin(GAME_TYPES).all(): raise RuntimeError("UNKNOWN_GAME_TYPE")
    staging.mkdir(parents=True,exist_ok=False)
    (staging/"raw_odds_response.json").write_bytes(raw_bytes)
    quotes,audit=normalize_quotes(envelope,games,goalies,slate_date=slate_date,run_id=run_id,run_type=run_type,run_timestamp_utc=run_timestamp_utc,capture_timestamp_utc=capture_text,raw_payload_sha256=raw_hash,source=source)
    quotes.to_csv(staging/"saves_quotes.csv",index=False); audit.to_csv(staging/"quote_binding_audit.csv",index=False)
    quotes.groupby("quote_qualification_status",dropna=False).size().reset_index(name="rows").to_csv(staging/"quote_qualification_audit.csv",index=False)
    timing=["run_id","game_id","goalie_id","sportsbook","provider_quote_timestamp_utc","provider_market_timestamp_utc","source_timestamp_utc","capture_timestamp_utc","scheduled_start_time_utc","quote_qualification_status"]
    quotes[timing].to_csv(staging/"quote_timing_audit.csv",index=False)
    metadata={"schema_version":"nhl_saves_quote_capture_v1","run_id":run_id,"canonical_season":2026,"slate_date":slate_date,"run_type":run_type,
              "run_timestamp_utc":iso(parse_utc(run_timestamp_utc)),"capture_timestamp_utc":iso(parse_utc(capture_text)),"source":source,
              "raw_payload_sha256":raw_hash,"parent_game_spine_sha256":sha256_file(games_csv),"parent_goalie_spine_sha256":sha256_file(goalies_csv),
              "parent_manifest_sha256":sha256_file(parent_manifest),"normalized_quote_rows":len(quotes),"qualified_quote_rows":int(quotes.quote_qualification_status.isin(QUALIFIED).sum()),
              "post_start_rows":int(quotes.quote_qualification_status.eq("POST_START_INVALID").sum()),"sportsbook_count":int(quotes.sportsbook.nunique()),
              "quote_capture_code_sha256":sha256_file(Path(__file__)),"candidate_rows":0,"upload_rows":0,"execution_rows":0}
    (staging/"run_metadata.json").write_text(json.dumps(metadata,indent=2,sort_keys=True)+"\n")
    (staging/"RUN_COMPLETE.json").write_text(json.dumps({"run_id":run_id,"status":"COMPLETE"},sort_keys=True)+"\n")
    write_manifest(staging,complete_only=True); staging.rename(destination); return destination
