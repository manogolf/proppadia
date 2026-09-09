"""Pure helpers for the season-2025 player-prop canonical market index.

This module is intentionally outside every live lane.  It reads retained evidence,
constructs deterministic tables, and never fetches markets or mutates source data.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

MARKET_TO_LANE = {
    "player_shots_on_goal": "SOG",
    "player_shots_on_goal_alternate": "SOG",
    "player_points": "POINTS",
    "player_total_saves": "SAVES",
}
PROP_TO_LANE = {
    "shots_on_goal": "SOG",
    "player_points": "POINTS",
    "goalie_saves": "SAVES",
}
IDENTITY_ACCEPTED = {"CANONICAL_EXACT", "CANONICAL_CORROBORATED", "ALIAS_RESOLVED"}
TIMING_VALUES = {
    "PREGAME_QUALIFIED", "AT_OR_POST_START", "TIMING_INDETERMINATE", "START_TIME_UNRESOLVED"
}
TEAM_ALIASES = {
    "anaheimducks": "ANA", "bostonbruins": "BOS", "buffalosabres": "BUF",
    "calgaryflames": "CGY", "carolinahurricanes": "CAR", "chicagoblackhawks": "CHI",
    "coloradoavalanche": "COL", "columbusbluejackets": "CBJ", "dallasstars": "DAL",
    "detroitredwings": "DET", "edmontonoilers": "EDM", "floridapanthers": "FLA",
    "losangeleskings": "LAK", "minnesotawild": "MIN", "montrealcanadiens": "MTL",
    "newjerseydevils": "NJD", "newyorkislanders": "NYI", "newyorkrangers": "NYR",
    "nashvillepredators": "NSH", "ottawasenators": "OTT", "philadelphiaflyers": "PHI",
    "pittsburghpenguins": "PIT", "sanjosesharks": "SJS", "seattlekraken": "SEA",
    "stlouisblues": "STL", "tampabaylightning": "TBL", "torontomapleleafs": "TOR",
    "utahhockeyclub": "UTA", "utahmammoth": "UTA", "vancouvercanucks": "VAN",
    "vegasgoldenknights": "VGK", "washingtoncapitals": "WSH", "winnipegjets": "WPG",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_hash(parts: Iterable[Any]) -> str:
    payload = "\x1f".join("" if x is None else str(x) for x in parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def normalize_name(value: Any) -> str:
    value = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode().lower()
    value = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", value)
    return " ".join(re.findall(r"[a-z0-9]+", value))


def compact(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", normalize_name(value))


def player_initial_last(value: Any) -> str:
    parts = normalize_name(value).split()
    return "" if len(parts) < 2 else f"{parts[0][0]} {parts[-1]}"


def team_code(value: Any) -> str | None:
    text = compact(value)
    if text in TEAM_ALIASES:
        return TEAM_ALIASES[text]
    upper = str(value or "").strip().upper()
    return upper if upper in set(TEAM_ALIASES.values()) else None


def parse_utc(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    if isinstance(value, pd.Timestamp):
        parsed = value
    elif isinstance(value, datetime):
        parsed = pd.Timestamp(value)
    elif isinstance(value, str):
        try:
            parsed = pd.Timestamp(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            parsed = pd.to_datetime(value, utc=True, errors="coerce")
    else:
        parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if not pd.isna(parsed) and parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    elif not pd.isna(parsed):
        parsed = parsed.tz_convert("UTC")
    return None if pd.isna(parsed) else parsed


def iso(value: Any) -> str | None:
    parsed = parse_utc(value)
    return None if parsed is None else parsed.isoformat().replace("+00:00", "Z")


def american_implied(price: Any) -> float | None:
    try:
        value = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value == 0 or abs(value) < 100:
        return None
    return 100.0 / (value + 100.0) if value > 0 else abs(value) / (abs(value) + 100.0)


def american_decimal(price: Any) -> float | None:
    implied = american_implied(price)
    return None if implied is None else 1.0 / implied


def market_status(book: dict[str, Any], market: dict[str, Any], outcome: dict[str, Any]) -> str:
    if any(x.get("suspended") is True or x.get("active") is False for x in (book, market, outcome)):
        return "SUSPENDED"
    value = outcome.get("status") or market.get("status") or book.get("status")
    return str(value).upper() if value not in (None, "") else "NOT_PROVIDED"


def timing_classification(effective: Any, scheduled_start: Any) -> str:
    effective_ts, start_ts = parse_utc(effective), parse_utc(scheduled_start)
    if start_ts is None:
        return "START_TIME_UNRESOLVED"
    if effective_ts is None:
        return "TIMING_INDETERMINATE"
    return "PREGAME_QUALIFIED" if effective_ts < start_ts else "AT_OR_POST_START"


@dataclass(frozen=True)
class SourceSpec:
    path: Path
    relative_path: str
    family: str
    slate_date: str
    size: int
    sha256: str
    file_timestamp_utc: str
    capture_timestamp_utc: str
    capture_timestamp_provenance: str
    parse_status: str
    parse_error: str | None


def discover_sources(root: Path, repo_root: Path) -> list[SourceSpec]:
    specs: list[SourceSpec] = []
    patterns = [("odds_event_wrappers.json", "HISTORICAL_BUNDLE"), ("odds_latest.json", "CONTEMPORANEOUS_LATEST")]
    for filename, family in patterns:
        for path in sorted(root.glob(f"*/{filename}")):
            status, error = "PARSED", None
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, list):
                    raise ValueError("top level is not a list")
            except Exception as exc:  # retained corrupt sources remain manifested
                status, error = "PARSE_FAILED", f"{type(exc).__name__}: {exc}"
            stamp = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            specs.append(SourceSpec(
                path=path, relative_path=str(path.relative_to(repo_root)), family=family,
                slate_date=path.parent.name, size=path.stat().st_size, sha256=sha256_file(path),
                file_timestamp_utc=stamp, capture_timestamp_utc=stamp,
                capture_timestamp_provenance="FILESYSTEM_MTIME_RETROACTIVE_RETRIEVAL" if family == "HISTORICAL_BUNDLE" else "FILESYSTEM_MTIME_LOCAL_ARCHIVE",
                parse_status=status, parse_error=error,
            ))
    return specs


def source_events(spec: SourceSpec) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Return (event, response metadata) pairs while preserving wrapper times."""
    payload = json.loads(spec.path.read_text(encoding="utf-8"))
    rows: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for index, raw in enumerate(payload):
        if not isinstance(raw, dict):
            continue
        if spec.family == "HISTORICAL_BUNDLE":
            event = raw.get("data")
            meta = {
                "response_timestamp_utc": iso(raw.get("timestamp")),
                "previous_response_timestamp_utc": iso(raw.get("previous_timestamp")),
                "next_response_timestamp_utc": iso(raw.get("next_timestamp")),
                "event_index": index,
            }
        else:
            event, meta = raw, {"response_timestamp_utc": None, "previous_response_timestamp_utc": None, "next_response_timestamp_utc": None, "event_index": index}
        if isinstance(event, dict):
            rows.append((event, meta))
    return rows


def prepare_games(games: pd.DataFrame) -> pd.DataFrame:
    out = games.copy()
    out["start_ts"] = pd.to_datetime(out["start_time_utc"], utc=True, errors="coerce")
    out["game_date"] = out["game_date"].astype(str)
    return out.sort_values(["game_date", "game_id"]).reset_index(drop=True)


def bind_event(event: dict[str, Any], games: pd.DataFrame, slate_date: str, tolerance_minutes: int = 180) -> dict[str, Any]:
    event_id = str(event.get("id") or "")
    home, away = team_code(event.get("home_team")), team_code(event.get("away_team"))
    start = parse_utc(event.get("commence_time"))
    candidates = games[(games.home_team_code == home) & (games.away_team_code == away)] if home and away else games.iloc[0:0]
    exact_date = candidates[candidates.game_date.eq(str(slate_date))]
    if start is not None and not candidates.empty:
        diff = (candidates.start_ts - start).abs().dt.total_seconds() / 60
        timed = candidates[diff <= tolerance_minutes]
    else:
        timed = candidates.iloc[0:0]
    chosen = None
    if len(timed) == 1:
        chosen, status, method, confidence = timed.iloc[0], "CANONICAL_CORROBORATED", "EXACT_TEAM_ORIENTATION_AND_SCHEDULE_TIME", "HIGH"
    elif len(exact_date) == 1 and start is not None:
        chosen, status, method, confidence = exact_date.iloc[0], "CANONICAL_CORROBORATED", "EXACT_TEAM_ORIENTATION_AND_SLATE_DATE_FALLBACK", "MEDIUM"
    elif len(timed) > 1 or len(exact_date) > 1:
        pool = timed if len(timed) > 1 else exact_date
        status, method, confidence = "AMBIGUOUS", "MULTIPLE_TEAM_TIME_CANDIDATES", "NONE"
    else:
        reversed_rows = games[(games.home_team_code == away) & (games.away_team_code == home)] if home and away else games.iloc[0:0]
        if len(reversed_rows[reversed_rows.game_date.eq(str(slate_date))]):
            status, method, confidence = "CONFLICT", "TEAM_ORIENTATION_MISMATCH", "NONE"
        else:
            status, method, confidence = "UNRESOLVED", "NO_TEAM_TIME_CANDIDATE", "NONE"
        pool = exact_date
    pool = timed if len(timed) else exact_date
    competitors = ";".join(str(x) for x in sorted(pool.game_id.astype(str).tolist()))
    return {
        "provider_event_id": event_id, "canonical_game_id": None if chosen is None else int(chosen.game_id),
        "canonical_season": None if chosen is None else int(chosen.season),
        "canonical_game_date": None if chosen is None else str(chosen.game_date),
        "canonical_start_time_utc": None if chosen is None else iso(chosen.start_time_utc),
        "canonical_home_team_code": None if chosen is None else str(chosen.home_team_code),
        "canonical_away_team_code": None if chosen is None else str(chosen.away_team_code),
        "provider_home_team": event.get("home_team"), "provider_away_team": event.get("away_team"),
        "provider_start_time_utc": iso(event.get("commence_time")), "identity_classification": status,
        "binding_method": method, "binding_evidence": f"home={home};away={away};slate={slate_date};provider_start={iso(event.get('commence_time'))}",
        "confidence": confidence, "ambiguity": bool(status == "AMBIGUOUS"),
        "competing_candidates": competitors, "final_disposition": "BOUND" if status in IDENTITY_ACCEPTED else "REJECTED",
    }


def prepare_player_candidates(rosters: pd.DataFrame, predictions: pd.DataFrame) -> dict[int, pd.DataFrame]:
    columns = ["game_id", "player_id", "full_name", "team_code", "position"]
    base = rosters[columns].copy() if not rosters.empty else pd.DataFrame(columns=columns)
    pred = predictions[["game_id", "player_id", "player_name"]].drop_duplicates().rename(columns={"player_name": "full_name"})
    pred["team_code"], pred["position"] = None, None
    joined = pd.concat([base, pred[columns]], ignore_index=True).drop_duplicates(["game_id", "player_id"])
    joined["normalized_name"] = joined.full_name.map(normalize_name)
    joined["initial_last"] = joined.full_name.map(player_initial_last)
    return {int(k): v.sort_values("player_id").reset_index(drop=True) for k, v in joined.groupby("game_id", sort=True)}


def bind_player(source_name: Any, game_id: int | None, candidates_by_game: dict[int, pd.DataFrame], source_player_id: Any = None) -> dict[str, Any]:
    source_name_text = str(source_name or "").strip()
    norm_name = normalize_name(source_name_text)
    if game_id is None or game_id not in candidates_by_game:
        return {"canonical_player_id": None, "canonical_player_name": None, "team_code": None,
                "identity_classification": "UNRESOLVED", "binding_method": "NO_BOUND_GAME_PLAYER_POPULATION",
                "binding_evidence": f"normalized_name={norm_name}", "confidence": "NONE", "ambiguity": False,
                "competing_candidates": "", "final_disposition": "REJECTED"}
    frame = candidates_by_game[game_id]
    if source_player_id not in (None, ""):
        matches = frame[frame.player_id.astype(str).eq(str(source_player_id))]
        if len(matches) == 1:
            row = matches.iloc[0]
            return _player_result(row, "CANONICAL_EXACT", "CANONICAL_PROVIDER_PLAYER_ID", "HIGH", f"source_player_id={source_player_id}")
    matches = frame[frame.normalized_name.eq(norm_name)]
    if len(matches) == 1:
        return _player_result(matches.iloc[0], "CANONICAL_CORROBORATED", "EXACT_NORMALIZED_NAME_WITHIN_BOUND_GAME", "HIGH", f"normalized_name={norm_name};game_id={game_id}")
    alias = player_initial_last(source_name_text) if len(norm_name.split()) > 1 else norm_name
    alias_matches = frame[frame.initial_last.eq(alias)] if alias else frame.iloc[0:0]
    if len(alias_matches) == 1:
        return _player_result(alias_matches.iloc[0], "ALIAS_RESOLVED", "UNIQUE_INITIAL_LAST_WITHIN_BOUND_GAME", "MEDIUM", f"alias={alias};game_id={game_id}")
    pool = matches if len(matches) else alias_matches
    status = "AMBIGUOUS" if len(pool) > 1 else "UNRESOLVED"
    return {"canonical_player_id": None, "canonical_player_name": None, "team_code": None,
            "identity_classification": status, "binding_method": "MULTIPLE_GAME_SCOPED_NAME_CANDIDATES" if status == "AMBIGUOUS" else "NO_GAME_SCOPED_NAME_CANDIDATE",
            "binding_evidence": f"normalized_name={norm_name};alias={alias};game_id={game_id}", "confidence": "NONE",
            "ambiguity": status == "AMBIGUOUS", "competing_candidates": ";".join(str(x) for x in sorted(pool.player_id.astype(str).tolist())),
            "final_disposition": "REJECTED"}


def _player_result(row: pd.Series, status: str, method: str, confidence: str, evidence: str) -> dict[str, Any]:
    return {"canonical_player_id": int(row.player_id), "canonical_player_name": row.full_name,
            "team_code": row.team_code if pd.notna(row.team_code) else None,
            "identity_classification": status, "binding_method": method, "binding_evidence": evidence,
            "confidence": confidence, "ambiguity": False, "competing_candidates": "", "final_disposition": "BOUND"}


def effective_timestamp(outcome: dict[str, Any], market: dict[str, Any], book: dict[str, Any], response_timestamp: Any, file_timestamp: Any) -> tuple[str | None, str]:
    precedence = [
        (outcome.get("last_update") or outcome.get("timestamp"), "OUTCOME_PROVIDER_TIMESTAMP"),
        (market.get("last_update"), "MARKET_PROVIDER_TIMESTAMP"),
        (book.get("last_update"), "BOOK_PROVIDER_TIMESTAMP"),
        (response_timestamp, "RESPONSE_LEVEL_TIMESTAMP"),
        (file_timestamp, "FILESYSTEM_ARCHIVE_TIMESTAMP"),
    ]
    for value, provenance in precedence:
        parsed = iso(value)
        if parsed is not None:
            return parsed, provenance
    return None, "MISSING"


def qualification(event_binding: str, player_binding: str, timing: str, status: str, side: str | None, line: Any, price: Any) -> tuple[str, str | None]:
    if event_binding not in IDENTITY_ACCEPTED:
        return "EXCLUDED", f"GAME_IDENTITY_{event_binding}"
    if player_binding not in IDENTITY_ACCEPTED:
        return "EXCLUDED", f"PLAYER_IDENTITY_{player_binding}"
    if timing != "PREGAME_QUALIFIED":
        return "EXCLUDED", timing
    if status in {"SUSPENDED", "CLOSED", "INACTIVE"}:
        return "EXCLUDED", f"MARKET_STATUS_{status}"
    if side not in {"OVER", "UNDER"}:
        return "EXCLUDED", "SIDE_INVALID"
    try:
        line_value = float(line)
    except (TypeError, ValueError):
        line_value = math.nan
    if not math.isfinite(line_value):
        return "EXCLUDED", "LINE_INVALID"
    if american_implied(price) is None:
        return "EXCLUDED", "PRICE_INVALID"
    return "QUALIFIED", None


def classify_duplicates(observations: pd.DataFrame) -> pd.DataFrame:
    out = observations.copy()
    out["duplicate_classification"] = "BOOK_LINE_SIDE_DISTINCT_OBSERVATION"
    semantic = ["source_file_sha256", "provider_event_id", "sportsbook", "provider_market_key", "source_player_name", "line", "side", "american_price", "effective_observation_timestamp_utc"]
    exact = out.duplicated(semantic, keep=False)
    out.loc[exact, "duplicate_classification"] = "EXACT_RAW_DUPLICATE"
    conflict_key = semantic[:-2] + ["effective_observation_timestamp_utc"]
    conflict = out.groupby(conflict_key, dropna=False)["american_price"].transform("nunique").gt(1)
    out.loc[conflict, "duplicate_classification"] = "CONFLICTING_OBSERVATION"
    repeat_key = ["provider_event_id", "sportsbook", "provider_market_key", "source_player_name", "line", "side", "american_price"]
    repeated = out.groupby(repeat_key, dropna=False)["source_file_sha256"].transform("nunique").gt(1)
    out.loc[repeated & ~exact & ~conflict, "duplicate_classification"] = "REPEATED_CAPTURE"
    return out


def build_pairs(qualified: pd.DataFrame) -> pd.DataFrame:
    fields = ["source_file_sha256", "source_file_path", "archive_family", "slate_date", "canonical_season", "canonical_game_id",
              "provider_event_id", "sportsbook", "sportsbook_name", "provider_market_key", "market_family", "canonical_player_id",
              "canonical_player_name", "line", "market_object_locator"]
    rows: list[dict[str, Any]] = []
    for key, group in qualified.groupby(fields, dropna=False, sort=True):
        over, under = group[group.side.eq("OVER")], group[group.side.eq("UNDER")]
        base = dict(zip(fields, key))
        complete = len(over) == 1 and len(under) == 1
        over_row, under_row = (over.iloc[0] if len(over) else None), (under.iloc[0] if len(under) else None)
        over_p = None if over_row is None else american_implied(over_row.american_price)
        under_p = None if under_row is None else american_implied(under_row.american_price)
        aligned = bool(complete and over_row.effective_observation_timestamp_utc == under_row.effective_observation_timestamp_utc)
        denom = (over_p + under_p) if over_p is not None and under_p is not None else None
        rows.append({**base,
            "over_observation_id": None if over_row is None else over_row.observation_id,
            "under_observation_id": None if under_row is None else under_row.observation_id,
            "over_american_price": None if over_row is None else over_row.american_price,
            "under_american_price": None if under_row is None else under_row.american_price,
            "over_implied_probability": over_p, "under_implied_probability": under_p,
            "no_vig_over_probability": over_p / denom if complete and denom else None,
            "no_vig_under_probability": under_p / denom if complete and denom else None,
            "pair_complete": complete, "temporally_aligned": aligned,
            "pair_status": "COMPLETE_ALIGNED" if complete and aligned else ("COMPLETE_TIMESTAMP_MISMATCH" if complete else "INCOMPLETE_OPPOSITE_SIDE"),
            "over_rows": len(over), "under_rows": len(under),
        })
    return pd.DataFrame(rows)


def link_predictions(predictions: pd.DataFrame, qualified: pd.DataFrame, lane: str, ladder: pd.DataFrame | None = None) -> pd.DataFrame:
    preds = predictions[predictions.lane.eq(lane)].copy()
    quotes = qualified[qualified.market_family.eq(lane)].copy()
    quote_groups = {k: v for k, v in quotes.groupby(["canonical_game_id", "canonical_player_id", "line"], sort=True)}
    player_game_keys = set(zip(quotes.canonical_game_id, quotes.canonical_player_id))
    ladder_map: dict[tuple[int, int], dict[str, Any]] = {}
    if lane == "POINTS" and ladder is not None:
        for row in ladder.to_dict("records"):
            ladder_map[(int(row["game_id"]), int(row["player_id"]))] = row
    rows: list[dict[str, Any]] = []
    for pred in preds.sort_values("prediction_id").to_dict("records"):
        key = (int(pred["game_id"]), int(pred["player_id"]), float(pred["line"]))
        matched = quote_groups.get(key)
        base = {k: pred.get(k) for k in ["prediction_id", "lane", "game_id", "player_id", "player_name", "line", "p_over", "model_family", "model_version", "feature_hash", "model_params_sha256", "created_at", "updated_at"]}
        base["prediction_run_identity"] = stable_hash([pred.get("model_family"), pred.get("model_version"), pred.get("feature_hash"), pred.get("created_at")])
        ladder_row = ladder_map.get((int(pred["game_id"]), int(pred["player_id"])), {})
        ladder_values = {"points_ladder_coherence_state": ladder_row.get("coherence_state"),
                         "points_material_non_monotonic_1pp": ladder_row.get("material_non_monotonic_1pp")}
        if matched is None or matched.empty:
            reason = "PREDICTION_MARKET_LINE_MISMATCH" if (key[0], key[1]) in player_game_keys else "NO_QUALIFIED_PLAYER_GAME_MARKET"
            rows.append({**base, **ladder_values, "linked_raw_quote_identity": None, "quote_side": None,
                         "model_probability_for_quote_side": None, "binding_method": None,
                         "timing_qualification": None, "linkage_status": "UNMATCHED", "unmatched_reason": reason})
        else:
            for quote in matched.sort_values("observation_id").to_dict("records"):
                rows.append({**base, **ladder_values, "linked_raw_quote_identity": quote["observation_id"], "quote_side": quote["side"],
                             "model_probability_for_quote_side": pred["p_over"] if quote["side"] == "OVER" else 1.0 - pred["p_over"],
                             "binding_method": "CANONICAL_GAME_PLAYER_LINE_EXACT",
                             "timing_qualification": quote["timing_classification"], "linkage_status": "LINKED", "unmatched_reason": None})
    return pd.DataFrame(rows)


def verify_parent_hashes(manifest: pd.DataFrame, repo_root: Path) -> None:
    for row in manifest.to_dict("records"):
        path = repo_root / row["source_file_path"]
        if not path.is_file() or sha256_file(path) != row["sha256"]:
            raise RuntimeError(f"PARENT_HASH_DRIFT_REJECTED:{row['source_file_path']}")
