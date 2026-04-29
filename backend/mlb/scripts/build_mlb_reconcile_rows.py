#!/usr/bin/env python3
"""
Build MLB row-level reconcile dataset from archived daily artifacts.

Inputs per slate date (under --odds-root/YYYY-MM-DD):
- mlb_slate_output.csv
- odds_latest_compatible.json

Output:
- one CSV with model probabilities/fair odds + executable market prices + optional outcomes
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

from backend.app.services.mlb.market_odds_service import (
    get_prop_market_candidates,
)
from backend.mlb.shared.team_name_map import teamIdMap
from backend.shared.db.pg import pg_fetchall

_PLAYER_STATS_FALLBACK_PROPS = {
    "hits",
    "singles",
    "doubles",
    "triples",
    "home_runs",
    "total_bases",
    "hits_runs_rbis",
    "runs_scored",
    "rbis",
    "walks",
    "strikeouts_batting",
    "stolen_bases",
    "strikeouts_pitching",
    "outs_recorded",
    "walks_allowed",
    "hits_allowed",
    "earned_runs",
}


def _norm_name(value: object) -> str:
    text = str(value or "").strip().lower()
    keep = []
    for ch in text:
        if ch.isalnum() or ch.isspace():
            keep.append(ch)
    return " ".join("".join(keep).split())


def _clean_str(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _line_key(v: object) -> Optional[float]:
    try:
        return round(float(v), 3)
    except Exception:
        return None


def _american_to_implied_probability(price: Optional[float]) -> Optional[float]:
    if price is None:
        return None
    try:
        p = float(price)
    except Exception:
        return None
    if p == 0:
        return None
    if p > 0:
        return 100.0 / (p + 100.0)
    return abs(p) / (abs(p) + 100.0)


def _profit_per_1u(*, outcome: Optional[str], price_american: Optional[float]) -> Optional[float]:
    if outcome is None or price_american is None:
        return None
    side_outcome = str(outcome).strip().lower()
    if side_outcome == "push":
        return 0.0
    if side_outcome == "loss":
        return -1.0
    if side_outcome != "win":
        return None
    try:
        p = float(price_american)
    except Exception:
        return None
    if p > 0:
        return p / 100.0
    if p < 0:
        return 100.0 / abs(p)
    return None


def _date_range(from_date: str, to_date: str) -> List[str]:
    start = date.fromisoformat(str(from_date))
    end = date.fromisoformat(str(to_date))
    if start > end:
        raise ValueError("--from-date must be <= --to-date")
    cur = start
    out: List[str] = []
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _build_team_name_reverse() -> Dict[str, str]:
    rev: Dict[str, str] = {}
    for _team_id, info in teamIdMap.items():
        abbr = str(info.get("abbr") or "").strip().upper()
        full = str(info.get("fullName") or "").strip()
        if abbr and full:
            rev[_norm_name(full)] = abbr
    # Explicit aliases seen in OddsAPI feeds.
    rev[_norm_name("Athletics")] = "OAK"
    rev[_norm_name("Arizona Diamondbacks")] = "ARI"
    rev[_norm_name("Kansas City Royals")] = "KC"
    rev[_norm_name("San Diego Padres")] = "SD"
    rev[_norm_name("San Francisco Giants")] = "SF"
    rev[_norm_name("Tampa Bay Rays")] = "TB"
    rev[_norm_name("Washington Nationals")] = "WSH"
    rev[_norm_name("Chicago White Sox")] = "CWS"
    rev[_norm_name("St. Louis Cardinals")] = "STL"
    return rev


def _load_events(path: Path) -> List[Dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        events = raw.get("events")
        if isinstance(events, list):
            return [x for x in events if isinstance(x, dict)]
    return []


def _build_market_index(
    *,
    events: Iterable[Dict[str, Any]],
    team_name_rev: Dict[str, str],
) -> Dict[Tuple[str, str, str, str, float], Dict[str, Dict[str, Any]]]:
    # key -> bookmaker -> side payload
    idx: Dict[Tuple[str, str, str, str, float], Dict[str, Dict[str, Any]]] = {}
    for ev in events:
        home_name = _clean_str(ev.get("home_team"))
        away_name = _clean_str(ev.get("away_team"))
        if not home_name or not away_name:
            continue
        home_abbr = team_name_rev.get(_norm_name(home_name))
        away_abbr = team_name_rev.get(_norm_name(away_name))
        if not home_abbr or not away_abbr:
            continue

        for book in ev.get("bookmakers") or []:
            book_key = _clean_str(book.get("key")) or _clean_str(book.get("title")) or "book"
            for market in book.get("markets") or []:
                market_key = _clean_str(market.get("key"))
                if not market_key:
                    continue
                for outcome in market.get("outcomes") or []:
                    side = str(outcome.get("name") or "").strip().lower()
                    if side not in {"over", "under"}:
                        continue
                    player_name = _clean_str(outcome.get("description"))
                    if not player_name:
                        continue
                    line = _line_key(outcome.get("point"))
                    if line is None:
                        continue
                    try:
                        price = int(round(float(outcome.get("price"))))
                    except Exception:
                        continue

                    k = (home_abbr, away_abbr, market_key, _norm_name(player_name), line)
                    by_book = idx.setdefault(k, {})
                    book_row = by_book.setdefault(book_key, {"bookmaker_key": book_key})
                    book_row[side] = int(price)
                    if not book_row.get("player_name"):
                        book_row["player_name"] = player_name
    return idx


def _choose_book(
    *,
    by_book: Dict[str, Dict[str, Any]],
    bookmaker: Optional[str],
) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[str]]:
    if not by_book:
        return None, None, None, None

    if bookmaker:
        target = str(bookmaker).strip().lower()
        for key, row in by_book.items():
            if str(key).strip().lower() == target:
                return str(key), row.get("over"), row.get("under"), row.get("player_name")
        return None, None, None, None

    ranked: List[Tuple[int, int, float, str, Dict[str, Any]]] = []
    for key, row in by_book.items():
        over = row.get("over")
        under = row.get("under")
        has_over = int(over is not None)
        has_under = int(under is not None)
        has_both = int(has_over and has_under)
        price_sum = float((over or 0) + (under or 0))
        ranked.append((has_both, has_over + has_under, price_sum, str(key), row))
    ranked.sort(reverse=True)
    _, _, _, key, row = ranked[0]
    return key, row.get("over"), row.get("under"), row.get("player_name")


def _load_actual_values(
    *,
    from_date: str,
    to_date: str,
) -> Dict[Tuple[int, int, str], Dict[str, Any]]:
    sql = """
    SELECT
      game_id::bigint AS game_id,
      player_id::bigint AS player_id,
      lower(trim(prop_type)) AS prop_type,
      AVG(NULLIF(btrim(prop_value::text), '')::numeric)::float8 AS actual_value,
      COUNT(*)::int AS sample_rows,
      COUNT(DISTINCT NULLIF(btrim(prop_value::text), ''))::int AS distinct_actual_values
    FROM mlb.model_training_props
    WHERE game_date::date BETWEEN %s::date AND %s::date
      AND lower(trim(coalesce(prop_source, ''))) = 'mlb_api'
      AND game_id IS NOT NULL
      AND player_id IS NOT NULL
      AND prop_type IS NOT NULL
      AND NULLIF(btrim(prop_value::text), '') IS NOT NULL
    GROUP BY 1,2,3
    """
    rows = pg_fetchall(sql, (from_date, to_date))
    out: Dict[Tuple[int, int, str], Dict[str, Any]] = {}
    for r in rows:
        try:
            key = (int(r.get("game_id")), int(r.get("player_id")), str(r.get("prop_type")))
            out[key] = {
                "actual_value": float(r.get("actual_value")) if r.get("actual_value") is not None else None,
                "sample_rows": int(r.get("sample_rows") or 0),
                "distinct_actual_values": int(r.get("distinct_actual_values") or 0),
            }
        except Exception:
            continue

    # Reconcile fallback for stat props where training rows can be sparse:
    # when model_training_props has no resolved value, use player_stats for rows with
    # batter or pitcher participation evidence on that game.
    fallback_sql = """
    WITH ps AS (
      SELECT
        game_id::bigint AS game_id,
        player_id::bigint AS player_id,
        lower(trim(coalesce(position, ''))) AS position_norm,
        COALESCE(hits, 0)::float8 AS hits,
        COALESCE(singles, 0)::float8 AS singles,
        COALESCE(doubles, 0)::float8 AS doubles,
        COALESCE(triples, 0)::float8 AS triples,
        COALESCE(home_runs, 0)::float8 AS home_runs,
        COALESCE(total_bases, 0)::float8 AS total_bases,
        COALESCE(runs_scored, 0)::float8 AS runs_scored,
        COALESCE(rbis, 0)::float8 AS rbis,
        COALESCE(walks, 0)::float8 AS walks,
        COALESCE(strikeouts_batting, 0)::float8 AS strikeouts_batting,
        COALESCE(stolen_bases, 0)::float8 AS stolen_bases,
        COALESCE(strikeouts_pitching, 0)::float8 AS strikeouts_pitching,
        COALESCE(outs_recorded, 0)::float8 AS outs_recorded,
        COALESCE(walks_allowed, 0)::float8 AS walks_allowed,
        COALESCE(hits_allowed, 0)::float8 AS hits_allowed,
        COALESCE(earned_runs, 0)::float8 AS earned_runs,
        COALESCE(at_bats, 0)::float8 AS at_bats
      FROM mlb.player_stats
      WHERE game_date::date BETWEEN %s::date AND %s::date
        AND game_id IS NOT NULL
        AND player_id IS NOT NULL
    ),
    batter AS (
      SELECT *
      FROM ps
      WHERE position_norm <> 'p'
         OR at_bats > 0
         OR hits > 0
         OR walks > 0
         OR strikeouts_batting > 0
         OR runs_scored > 0
         OR rbis > 0
         OR stolen_bases > 0
    ),
    pitcher AS (
      SELECT *
      FROM ps
      WHERE position_norm = 'p'
         OR outs_recorded > 0
         OR strikeouts_pitching > 0
         OR walks_allowed > 0
         OR hits_allowed > 0
         OR earned_runs > 0
    ),
    expanded AS (
      SELECT game_id, player_id, 'hits'::text AS prop_type, hits AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'singles'::text AS prop_type, singles AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'doubles'::text AS prop_type, doubles AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'triples'::text AS prop_type, triples AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'home_runs'::text AS prop_type, home_runs AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'total_bases'::text AS prop_type, total_bases AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'hits_runs_rbis'::text AS prop_type, hits + runs_scored + rbis AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'runs_scored'::text AS prop_type, runs_scored AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'rbis'::text AS prop_type, rbis AS actual_value FROM batter
      UNION ALL
      SELECT
        game_id,
        player_id,
        'walks'::text AS prop_type,
        walks AS actual_value
      FROM batter
      UNION ALL
      SELECT
        game_id,
        player_id,
        'strikeouts_batting'::text AS prop_type,
        strikeouts_batting AS actual_value
      FROM batter
      UNION ALL
      SELECT game_id, player_id, 'stolen_bases'::text AS prop_type, stolen_bases AS actual_value FROM batter
      UNION ALL
      SELECT game_id, player_id, 'strikeouts_pitching'::text AS prop_type, strikeouts_pitching AS actual_value FROM pitcher
      UNION ALL
      SELECT game_id, player_id, 'outs_recorded'::text AS prop_type, outs_recorded AS actual_value FROM pitcher
      UNION ALL
      SELECT game_id, player_id, 'walks_allowed'::text AS prop_type, walks_allowed AS actual_value FROM pitcher
      UNION ALL
      SELECT game_id, player_id, 'hits_allowed'::text AS prop_type, hits_allowed AS actual_value FROM pitcher
      UNION ALL
      SELECT game_id, player_id, 'earned_runs'::text AS prop_type, earned_runs AS actual_value FROM pitcher
    )
    SELECT
      game_id,
      player_id,
      prop_type,
      AVG(actual_value)::float8 AS actual_value,
      COUNT(*)::int AS sample_rows,
      COUNT(DISTINCT actual_value)::int AS distinct_actual_values
    FROM expanded
    GROUP BY 1,2,3
    """
    fallback_rows = pg_fetchall(fallback_sql, (from_date, to_date))
    for r in fallback_rows:
        try:
            prop_type = str(r.get("prop_type") or "").strip().lower()
            if prop_type not in _PLAYER_STATS_FALLBACK_PROPS:
                continue
            key = (int(r.get("game_id")), int(r.get("player_id")), prop_type)
            current = out.get(key)
            if current and current.get("actual_value") is not None:
                continue
            out[key] = {
                "actual_value": float(r.get("actual_value")) if r.get("actual_value") is not None else None,
                "sample_rows": int(r.get("sample_rows") or 0),
                "distinct_actual_values": int(r.get("distinct_actual_values") or 0),
            }
        except Exception:
            continue
    return out


def _load_mtp_lines(
    *,
    from_date: str,
    to_date: str,
    prop_types: Iterable[str],
) -> List[Dict[str, Any]]:
    wanted = [str(p).strip().lower() for p in (prop_types or []) if str(p).strip()]
    if not wanted:
        return []
    placeholders = ", ".join(["%s"] * len(wanted))
    sql = f"""
    SELECT
      m.game_date::date AS game_date,
      m.game_id::bigint AS game_id,
      m.player_id::bigint AS player_id,
      lower(trim(m.prop_type)) AS prop_type,
      AVG(NULLIF(btrim(m.line::text), '')::numeric)::float8 AS line,
      MAX(gi.home_team_abbr)::text AS home_team_code,
      MAX(gi.away_team_abbr)::text AS away_team_code,
      MAX(pi.player_name)::text AS player_name
    FROM mlb.model_training_props m
    LEFT JOIN mlb.game_info gi
      ON gi.game_id = m.game_id
    LEFT JOIN mlb.player_ids pi
      ON pi.player_id = m.player_id
    WHERE m.game_date::date BETWEEN %s::date AND %s::date
      AND lower(trim(coalesce(m.prop_source, ''))) = 'mlb_api'
      AND lower(trim(m.prop_type)) IN ({placeholders})
      AND m.game_id IS NOT NULL
      AND m.player_id IS NOT NULL
      AND NULLIF(btrim(m.line::text), '') IS NOT NULL
    GROUP BY 1,2,3,4
    """
    params: List[Any] = [from_date, to_date, *wanted]
    return list(pg_fetchall(sql, tuple(params)) or [])


def _side_outcome(*, actual_value: Optional[float], line: float, side: str) -> Optional[str]:
    if actual_value is None:
        return None
    if abs(float(actual_value) - float(line)) < 1e-12:
        return "push"
    win_side = "over" if float(actual_value) > float(line) else "under"
    return "win" if str(side).lower() == win_side else "loss"


def main() -> int:
    ap = argparse.ArgumentParser(description="Build MLB row-level reconcile dataset from archived slate artifacts.")
    ap.add_argument("--odds-root", default="backend/mlb/exports/odds_history")
    ap.add_argument("--from-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--to-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--bookmaker", default="betonlineag", help="Bookmaker key to use (empty = best available per row)")
    ap.add_argument("--slate-filename", default="mlb_slate_output.csv")
    ap.add_argument("--odds-filename", default="odds_latest_compatible.json")
    ap.add_argument(
        "--odds-filename-fallback",
        default="",
        help=(
            "Optional alternate odds filename to use when --odds-filename is missing for a date. "
            "Default auto-maps between odds_latest_compatible.json and odds_mlb_playerprops.json."
        ),
    )
    ap.add_argument(
        "--derive-props-from-mtp",
        default="runs_rbis",
        help=(
            "Comma-separated prop types to synthesize from mlb.model_training_props "
            "when missing from archived slate rows (default: runs_rbis). Use empty string to disable."
        ),
    )
    ap.add_argument("--out-csv", default="tmp/mlb_base_vs_market_rows.csv")
    ap.add_argument("--out-summary-json", default="tmp/mlb_base_vs_market_summary.json")
    ap.add_argument("--skip-outcomes", action="store_true", help="Skip DB outcome join")
    ap.add_argument(
        "--require-outcomes",
        action="store_true",
        help="Fail if outcomes could not be loaded from DB.",
    )
    ap.add_argument(
        "--require-outcome-rows-min",
        type=int,
        default=0,
        help="When >0, fail if rows_with_outcomes is below this minimum.",
    )
    ap.add_argument(
        "--require-two-sided",
        action="store_true",
        default=str(os.environ.get("MLB_RECONCILE_REQUIRE_TWO_SIDED", "1")).strip().lower() in {"1", "true", "yes", "on"},
        help="Keep only rows where both over and under market prices are present.",
    )
    args = ap.parse_args()

    odds_root = Path(str(args.odds_root)).expanduser()
    out_csv = Path(str(args.out_csv)).expanduser()
    out_summary_json = Path(str(args.out_summary_json)).expanduser()
    bookmaker = str(args.bookmaker or "").strip() or None

    dates = _date_range(args.from_date, args.to_date)
    team_name_rev = _build_team_name_reverse()
    prop_market_candidates_cache: Dict[str, List[str]] = {}

    actual_by_key: Dict[Tuple[int, int, str], Dict[str, Any]] = {}
    outcomes_loaded = False
    outcomes_error: Optional[str] = None
    if not bool(args.skip_outcomes):
        try:
            actual_by_key = _load_actual_values(from_date=args.from_date, to_date=args.to_date)
            outcomes_loaded = True
        except Exception as e:
            outcomes_error = f"{type(e).__name__}: {e}"

    output_columns = [
        "game_date",
        "slate_date",
        "game_id",
        "home_team_code",
        "away_team_code",
        "player_id",
        "player_name",
        "prop_type",
        "market_key",
        "line",
        "bookmaker_key",
        "market_player_name",
        "price_over_american",
        "price_under_american",
        "implied_over",
        "implied_under",
        "implied_over_novig",
        "implied_under_novig",
        "market_hold",
        "model_prob_over",
        "model_prob_under",
        "model_fair_over_american",
        "model_fair_under_american",
        "model_pick_side",
        "model_pick_prob",
        "actual_value",
        "actual_over_outcome",
        "actual_under_outcome",
        "actual_model_pick_outcome",
        "pnl_over_1u",
        "pnl_under_1u",
        "pnl_model_pick_1u",
        "actual_sample_rows",
        "actual_distinct_values",
        "odds_snapshot_file",
        "slate_source_file",
    ]

    rows: List[Dict[str, Any]] = []
    rows_by_key: set[Tuple[int, int, str, float]] = set()
    skipped_missing_artifacts = 0
    skipped_missing_columns = 0
    processed_dates = 0
    derived_rows_added = 0
    fallback_dates_used: List[str] = []

    odds_filename = str(args.odds_filename or "").strip()
    fallback_filename = str(args.odds_filename_fallback or "").strip()
    if not fallback_filename:
        if odds_filename == "odds_latest_compatible.json":
            fallback_filename = "odds_mlb_playerprops.json"
        elif odds_filename == "odds_mlb_playerprops.json":
            fallback_filename = "odds_latest_compatible.json"

    required_cols = {
        "slate_date",
        "game_date",
        "game_id",
        "home_team_code",
        "away_team_code",
        "player_id",
        "player_name",
        "prop_type",
        "market_key",
        "line",
        "prob_over",
        "prob_under",
        "fair_odds_over_american",
        "fair_odds_under_american",
        "model_pick_side",
    }

    for day in dates:
        day_dir = odds_root / day
        slate_csv = day_dir / str(args.slate_filename)
        odds_json = day_dir / odds_filename
        if not odds_json.exists() and fallback_filename:
            fallback_path = day_dir / fallback_filename
            if fallback_path.exists():
                odds_json = fallback_path
                fallback_dates_used.append(day)

        if not slate_csv.exists() or not odds_json.exists():
            skipped_missing_artifacts += 1
            continue

        try:
            slate_df = pd.read_csv(slate_csv)
        except Exception:
            skipped_missing_artifacts += 1
            continue

        missing_cols = sorted(required_cols - set(slate_df.columns))
        if missing_cols:
            skipped_missing_columns += 1
            continue

        events = _load_events(odds_json)
        market_idx = _build_market_index(events=events, team_name_rev=team_name_rev)
        processed_dates += 1

        for _, row in slate_df.iterrows():
            prop_type = str(row.get("prop_type") or "").strip().lower()
            market_key_raw = str(row.get("market_key") or "").strip()

            home = str(row.get("home_team_code") or "").strip().upper()
            away = str(row.get("away_team_code") or "").strip().upper()
            player_name = str(row.get("player_name") or "").strip()
            line = _line_key(row.get("line"))
            if not home or not away or not player_name or line is None:
                continue

            prop_market_candidates = prop_market_candidates_cache.get(prop_type)
            if prop_market_candidates is None:
                prop_market_candidates = get_prop_market_candidates(prop_type=prop_type, include_aliases=True)
                prop_market_candidates_cache[prop_type] = list(prop_market_candidates)

            market_key_candidates: List[str] = []
            if market_key_raw:
                market_key_candidates.append(market_key_raw)
            for mk in prop_market_candidates:
                k = str(mk or "").strip()
                if k and k not in market_key_candidates:
                    market_key_candidates.append(k)
            if not market_key_candidates:
                continue

            market_key = market_key_candidates[0]
            by_book: Dict[str, Dict[str, Any]] = {}
            for candidate_market_key in market_key_candidates:
                key = (home, away, candidate_market_key, _norm_name(player_name), float(line))
                maybe_by_book = market_idx.get(key, {})
                if maybe_by_book:
                    market_key = candidate_market_key
                    by_book = maybe_by_book
                    break

            used_book, over_price, under_price, market_player_name = _choose_book(by_book=by_book, bookmaker=bookmaker)

            over_implied = _american_to_implied_probability(over_price)
            under_implied = _american_to_implied_probability(under_price)
            hold = None
            over_implied_novig = None
            under_implied_novig = None
            if over_implied is not None and under_implied is not None and (over_implied + under_implied) > 0:
                hold = (over_implied + under_implied) - 1.0
                denom = over_implied + under_implied
                over_implied_novig = over_implied / denom
                under_implied_novig = under_implied / denom

            game_id = int(row.get("game_id"))
            player_id = int(row.get("player_id"))
            actual_payload = actual_by_key.get((game_id, player_id, prop_type), {})
            actual_value = actual_payload.get("actual_value")
            over_outcome = _side_outcome(actual_value=actual_value, line=float(line), side="over")
            under_outcome = _side_outcome(actual_value=actual_value, line=float(line), side="under")
            model_pick_side = str(row.get("model_pick_side") or "").strip().lower()
            model_pick_outcome = over_outcome if model_pick_side == "over" else under_outcome if model_pick_side == "under" else None
            model_pick_price = over_price if model_pick_side == "over" else under_price if model_pick_side == "under" else None

            row_payload = {
                "game_date": str(row.get("game_date")),
                "slate_date": str(row.get("slate_date")),
                "game_id": game_id,
                "home_team_code": home,
                "away_team_code": away,
                "player_id": player_id,
                "player_name": player_name,
                "prop_type": prop_type,
                "market_key": market_key,
                "line": float(line),
                "bookmaker_key": used_book,
                "market_player_name": market_player_name,
                "price_over_american": over_price,
                "price_under_american": under_price,
                "implied_over": over_implied,
                "implied_under": under_implied,
                "implied_over_novig": over_implied_novig,
                "implied_under_novig": under_implied_novig,
                "market_hold": hold,
                "model_prob_over": float(row.get("prob_over")),
                "model_prob_under": float(row.get("prob_under")),
                "model_fair_over_american": int(row.get("fair_odds_over_american")),
                "model_fair_under_american": int(row.get("fair_odds_under_american")),
                "model_pick_side": model_pick_side,
                "model_pick_prob": float(row.get("model_pick_prob")) if row.get("model_pick_prob") is not None else None,
                "actual_value": actual_value,
                "actual_over_outcome": over_outcome,
                "actual_under_outcome": under_outcome,
                "actual_model_pick_outcome": model_pick_outcome,
                "pnl_over_1u": _profit_per_1u(outcome=over_outcome, price_american=over_price),
                "pnl_under_1u": _profit_per_1u(outcome=under_outcome, price_american=under_price),
                "pnl_model_pick_1u": _profit_per_1u(outcome=model_pick_outcome, price_american=model_pick_price),
                "actual_sample_rows": actual_payload.get("sample_rows"),
                "actual_distinct_values": actual_payload.get("distinct_actual_values"),
                "odds_snapshot_file": str(odds_json),
                "slate_source_file": str(slate_csv),
            }
            rows.append(row_payload)
            rows_by_key.add((game_id, player_id, prop_type, float(line)))

    derive_props = [str(p).strip().lower() for p in str(args.derive_props_from_mtp or "").split(",") if str(p).strip()]
    if derive_props:
        try:
            mtp_rows = _load_mtp_lines(from_date=args.from_date, to_date=args.to_date, prop_types=derive_props)
        except Exception as e:
            print(f"[mlb-reconcile] derive-props failed: {type(e).__name__}: {e}")
            mtp_rows = []
        for r in mtp_rows:
            try:
                game_id = int(r.get("game_id"))
                player_id = int(r.get("player_id"))
                prop_type = str(r.get("prop_type") or "").strip().lower()
                line = _line_key(r.get("line"))
                if not prop_type or line is None:
                    continue
                dedupe_key = (game_id, player_id, prop_type, float(line))
                if dedupe_key in rows_by_key:
                    continue
                game_date = _clean_str(r.get("game_date")) or args.to_date
                actual_payload = actual_by_key.get((game_id, player_id, prop_type), {})
                actual_value = actual_payload.get("actual_value")
                over_outcome = _side_outcome(actual_value=actual_value, line=float(line), side="over")
                under_outcome = _side_outcome(actual_value=actual_value, line=float(line), side="under")
                rows.append(
                    {
                        "game_date": game_date,
                        "slate_date": game_date,
                        "game_id": game_id,
                        "home_team_code": _clean_str(r.get("home_team_code")),
                        "away_team_code": _clean_str(r.get("away_team_code")),
                        "player_id": player_id,
                        "player_name": _clean_str(r.get("player_name")),
                        "prop_type": prop_type,
                        "market_key": f"derived:{prop_type}",
                        "line": float(line),
                        "bookmaker_key": None,
                        "market_player_name": None,
                        "price_over_american": None,
                        "price_under_american": None,
                        "implied_over": None,
                        "implied_under": None,
                        "implied_over_novig": None,
                        "implied_under_novig": None,
                        "market_hold": None,
                        "model_prob_over": None,
                        "model_prob_under": None,
                        "model_fair_over_american": None,
                        "model_fair_under_american": None,
                        "model_pick_side": None,
                        "model_pick_prob": None,
                        "actual_value": actual_value,
                        "actual_over_outcome": over_outcome,
                        "actual_under_outcome": under_outcome,
                        "actual_model_pick_outcome": None,
                        "pnl_over_1u": None,
                        "pnl_under_1u": None,
                        "pnl_model_pick_1u": None,
                        "actual_sample_rows": actual_payload.get("sample_rows"),
                        "actual_distinct_values": actual_payload.get("distinct_actual_values"),
                        "odds_snapshot_file": None,
                        "slate_source_file": "derived_from_mtp",
                    }
                )
                rows_by_key.add(dedupe_key)
                derived_rows_added += 1
            except Exception:
                continue

    # Keep a stable header even when no rows are produced, so downstream reads do
    # not fail with pandas EmptyDataError.
    out_df = pd.DataFrame(rows, columns=output_columns)
    rows_filtered_non_two_sided = 0
    if bool(args.require_two_sided) and not out_df.empty:
        two_sided_mask = out_df[["price_over_american", "price_under_american"]].notna().all(axis=1)
        rows_filtered_non_two_sided = int((~two_sided_mask).sum())
        out_df = out_df.loc[two_sided_mask].copy()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_summary_json.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_csv, index=False)

    summary = {
        "from_date": str(args.from_date),
        "to_date": str(args.to_date),
        "odds_root": str(odds_root),
        "bookmaker": bookmaker,
        "rows": int(len(out_df)),
        "processed_dates": int(processed_dates),
        "requested_dates": int(len(dates)),
        "skipped_missing_artifacts": int(skipped_missing_artifacts),
        "skipped_missing_columns": int(skipped_missing_columns),
        "matched_with_any_market_price": int(
            out_df[["price_over_american", "price_under_american"]].notna().any(axis=1).sum()
        )
        if not out_df.empty
        else 0,
        "matched_two_sided_prices": int(
            out_df[["price_over_american", "price_under_american"]].notna().all(axis=1).sum()
        )
        if not out_df.empty
        else 0,
        "rows_with_outcomes": int(out_df["actual_value"].notna().sum()) if ("actual_value" in out_df.columns and not out_df.empty) else 0,
        "outcomes_loaded": bool(outcomes_loaded),
        "outcomes_error": outcomes_error,
        "require_two_sided": bool(args.require_two_sided),
        "rows_filtered_non_two_sided": int(rows_filtered_non_two_sided),
        "generated_at_utc": datetime.now(ZoneInfo("UTC")).isoformat(),
        "derived_rows_added": int(derived_rows_added),
        "derived_props_from_mtp": derive_props,
        "odds_filename_requested": odds_filename,
        "odds_filename_fallback": fallback_filename or None,
        "odds_fallback_dates_used": fallback_dates_used,
        "odds_fallback_dates_used_count": int(len(fallback_dates_used)),
    }
    if not out_df.empty:
        summary["by_date"] = (
            out_df.groupby("game_date", as_index=False)
            .agg(rows=("game_id", "count"))
            .sort_values("game_date")
            .to_dict(orient="records")
        )
        summary["by_prop_type"] = (
            out_df.groupby("prop_type", as_index=False)
            .agg(rows=("game_id", "count"))
            .sort_values("rows", ascending=False)
            .to_dict(orient="records")
        )
    else:
        summary["by_date"] = []
        summary["by_prop_type"] = []

    out_summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"[mlb-reconcile] wrote rows csv: {out_csv} rows={len(out_df)}")
    print(f"[mlb-reconcile] wrote summary json: {out_summary_json}")
    print(
        f"[mlb-reconcile] processed_dates={processed_dates}/{len(dates)} "
        f"missing_artifacts={skipped_missing_artifacts} missing_columns={skipped_missing_columns}"
    )
    if outcomes_error:
        print(f"[mlb-reconcile] outcomes unavailable: {outcomes_error}")

    rows_with_outcomes = int(summary.get("rows_with_outcomes") or 0)
    if bool(args.require_outcomes) and not outcomes_loaded:
        print("[mlb-reconcile] ERROR outcomes are required but DB outcomes were unavailable.")
        return 2
    if int(args.require_outcome_rows_min or 0) > 0 and rows_with_outcomes < int(args.require_outcome_rows_min):
        print(
            "[mlb-reconcile] ERROR outcomes row minimum not met: "
            f"rows_with_outcomes={rows_with_outcomes} require_outcome_rows_min={int(args.require_outcome_rows_min)}"
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
