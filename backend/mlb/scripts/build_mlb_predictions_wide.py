#!/usr/bin/env python3
"""
Build MLB daily WIDE predictions CSV from live OddsAPI player-prop markets.

This is the MLB counterpart to NHL's daily slate prediction artifact generation:
- fetch one-date market snapshot (OddsAPI)
- resolve player identities against mlb.player_ids
- run MLB prepare->predict workflow per unique player/game/prop/line
- write one WIDE CSV with p_over_* columns for downstream slate/book outputs

Default output:
- backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv

Notes:
- "calibrated" is a filename compatibility convention for downstream consumers.
- MLB currently uses the production prediction workflow; no extra calibration pass is
  applied in this script.
"""

from __future__ import annotations

import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.services.mlb import market_odds_service
from backend.domains.mlb import prop_workflow
from backend.mlb.shared.mlb_api_v2 import GameLite, fetch_schedule_by_date
from backend.mlb.shared.team_name_map import getFullTeamAbbreviationFromID, getTeamIdFromAbbr, normalizeTeamAbbreviation
from backend.mlb.shared.time_utils_backend import get_time_of_day_bucket_et
from backend.shared.db.pg import pg_connect


ET = ZoneInfo("America/New_York")
BASE_DIR = Path(__file__).resolve().parents[2]  # .../backend
DEFAULT_OUT_CSV = BASE_DIR / "mlb" / "data" / "processed" / "mlb_predictions_wide_calibrated.csv"

_ALLOWED_LINE_FRAC = {0.0, 0.5}
_NAME_RE = re.compile(r"[^a-z0-9 ]+")


def _norm_name(value: object) -> str:
    text = str(value or "").strip().lower()
    text = _NAME_RE.sub("", text)
    text = " ".join(text.split())
    return text


def _clean_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _line_to_pcol(line: float) -> Optional[str]:
    try:
        x = float(line)
    except Exception:
        return None
    frac = round(x - math.floor(x), 2)
    if frac not in _ALLOWED_LINE_FRAC:
        return None
    whole = int(math.floor(x))
    frac_digit = "5" if abs(frac - 0.5) < 1e-9 else "0"
    return f"p_over_{whole}_{frac_digit}"


def _date_et_today() -> str:
    return datetime.now(ET).date().isoformat()


def _invert_market_map() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for prop_type, market_key in (market_odds_service.PROP_TO_ODDS_MARKET or {}).items():
        out[str(market_key)] = str(prop_type)
    return out


def _parse_prop_types_csv(raw: str) -> Optional[set[str]]:
    vals = [prop_workflow.normalize_prop_type(x) for x in str(raw or "").split(",")]
    vals = [v for v in vals if v]
    return set(vals) if vals else None


@dataclass
class PlayerRow:
    player_id: int
    player_name: str
    team_abbr: Optional[str]
    team_id: Optional[int]
    active: Optional[bool]


@dataclass
class Offer:
    event_id: str
    commence_time: Optional[str]
    home_team_name: str
    away_team_name: str
    home_team_abbr: str
    away_team_abbr: str
    prop_type: str
    player_name: str
    line: float
    books_seen: int


@dataclass
class ResolvedOffer:
    offer: Offer
    player: PlayerRow
    team_abbr: str
    team_id: int
    is_home: bool
    game: GameLite


def _load_player_rows(*, active_only: bool) -> tuple[Dict[int, PlayerRow], Dict[Tuple[str, str], List[PlayerRow]]]:
    # Active filter only if the column exists, otherwise the SELECT above returns NULLs and the
    # Python-side filter becomes a no-op.
    by_id: Dict[int, PlayerRow] = {}
    by_name_team: Dict[Tuple[str, str], List[PlayerRow]] = defaultdict(list)

    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name::text
            FROM information_schema.columns
            WHERE table_schema='mlb' AND table_name='player_ids'
            """
        )
        cols = {str((r if isinstance(r, str) else r.get("column_name") if isinstance(r, dict) else r[0])) for r in (cur.fetchall() or [])}
        sel_team = "CAST(team AS text) AS team_abbr" if "team" in cols else "NULL::text AS team_abbr"
        sel_team_id = "CASE WHEN team_id IS NULL THEN NULL ELSE team_id::int END AS team_id" if "team_id" in cols else "NULL::int AS team_id"
        sel_active = "active AS active" if "active" in cols else "NULL::boolean AS active"
        sql = f"""
            SELECT
              player_id::bigint AS player_id,
              player_name::text AS player_name,
              {sel_team},
              {sel_team_id},
              {sel_active}
            FROM mlb.player_ids
            WHERE player_name IS NOT NULL
        """
        cur.execute(sql)
        rows = cur.fetchall() or []

    for r in rows:
        row = dict(r)
        try:
            pid = int(row.get("player_id"))
        except Exception:
            continue
        name = _clean_str(row.get("player_name"))
        if not name:
            continue
        team_abbr = normalizeTeamAbbreviation(_clean_str(row.get("team_abbr")))
        team_id = row.get("team_id")
        try:
            team_id_i = int(team_id) if team_id is not None else None
        except Exception:
            team_id_i = None
        active_raw = row.get("active")
        active = bool(active_raw) if active_raw is not None else None
        if active_only and active is False:
            continue

        prow = PlayerRow(
            player_id=pid,
            player_name=name,
            team_abbr=team_abbr,
            team_id=team_id_i,
            active=active,
        )
        by_id[pid] = prow
        if team_abbr:
            by_name_team[(_norm_name(name), str(team_abbr))].append(prow)

    return by_id, by_name_team


def _build_team_name_reverse() -> Dict[str, str]:
    # Use authoritative IDs to build full-name -> normalized abbreviation mapping.
    rev: Dict[str, str] = {}
    for team_id in range(1, 1000):
        abbr = getFullTeamAbbreviationFromID(team_id)
        if not abbr:
            continue
        norm_abbr = normalizeTeamAbbreviation(abbr)
        # Known MLB names come from team_name_map internals via getFullTeamAbbreviationFromID only
        # for abbrs. We also add common display-name variants observed from OddsAPI.
        if norm_abbr:
            rev[_norm_name(norm_abbr)] = norm_abbr
    # Explicit full names used by OddsAPI.
    explicit = {
        "athletics": "OAK",
        "arizona diamondbacks": "ARI",
        "los angeles angels": "LAA",
        "los angeles dodgers": "LAD",
        "san diego padres": "SD",
        "san francisco giants": "SF",
        "tampa bay rays": "TB",
        "kansas city royals": "KC",
        "washington nationals": "WSH",
        "new york yankees": "NYY",
        "new york mets": "NYM",
        "chicago cubs": "CHC",
        "chicago white sox": "CWS",
        "st louis cardinals": "STL",
        "toronto blue jays": "TOR",
        "boston red sox": "BOS",
        "cincinnati reds": "CIN",
        "cleveland guardians": "CLE",
        "colorado rockies": "COL",
        "detroit tigers": "DET",
        "houston astros": "HOU",
        "miami marlins": "MIA",
        "milwaukee brewers": "MIL",
        "minnesota twins": "MIN",
        "philadelphia phillies": "PHI",
        "pittsburgh pirates": "PIT",
        "seattle mariners": "SEA",
        "texas rangers": "TEX",
        "atlanta braves": "ATL",
        "baltimore orioles": "BAL",
    }
    for k, v in explicit.items():
        rev[_norm_name(k)] = normalizeTeamAbbreviation(v)
    return rev


def _parse_event_team_abbrs(event: Dict[str, Any], team_name_rev: Dict[str, str]) -> tuple[Optional[str], Optional[str]]:
    home_name = _clean_str(event.get("home_team"))
    away_name = _clean_str(event.get("away_team"))
    home_abbr = team_name_rev.get(_norm_name(home_name)) if home_name else None
    away_abbr = team_name_rev.get(_norm_name(away_name)) if away_name else None
    return home_abbr, away_abbr


def _build_schedule_maps(slate_date: str) -> tuple[Dict[int, Dict[str, Any]], Dict[Tuple[str, str], List[GameLite]]]:
    games = fetch_schedule_by_date(slate_date)
    by_team: Dict[int, Dict[str, Any]] = {}
    by_pair: Dict[Tuple[str, str], List[GameLite]] = defaultdict(list)

    def _gkey(g: GameLite) -> str:
        return str(g.game_time or f"{g.game_date}T00:00:00-05:00")

    games_sorted = sorted(games, key=_gkey)
    for g in games_sorted:
        home_abbr = normalizeTeamAbbreviation(g.home_abbr or getFullTeamAbbreviationFromID(g.home_team_id))
        away_abbr = normalizeTeamAbbreviation(g.away_abbr or getFullTeamAbbreviationFromID(g.away_team_id))
        if home_abbr and away_abbr:
            by_pair[(str(home_abbr), str(away_abbr))].append(g)

        def _ctx_for(team_id: int, is_home: bool) -> Dict[str, Any]:
            opp_team_id = int(g.away_team_id if is_home else g.home_team_id)
            game_time = g.game_time
            day_of_week: Optional[int] = None
            time_bucket: Optional[str] = None
            if game_time:
                try:
                    dt = datetime.fromisoformat(game_time)
                    day_of_week = dt.weekday()
                    time_bucket = get_time_of_day_bucket_et(dt)
                except Exception:
                    pass
            opposing_pitcher_id = g.sp_away_id if is_home else g.sp_home_id
            return {
                "team_id": int(team_id),
                "team_abbr": normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(team_id)),
                "for_date": slate_date,
                "game_id": int(g.game_id),
                "game_type": _clean_str(g.game_type),
                "game_time": game_time,
                "is_home": bool(is_home),
                "opponent_team_id": opp_team_id,
                "opponent": normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(opp_team_id)),
                "opponent_encoded": opp_team_id,
                "game_day_of_week": day_of_week,
                "time_of_day_bucket": time_bucket,
                "starting_pitcher_id": int(opposing_pitcher_id) if opposing_pitcher_id else None,
            }

        # Keep the earliest game for prepare_prop's team/date semantics (matches current behavior).
        by_team.setdefault(int(g.home_team_id), _ctx_for(int(g.home_team_id), True))
        by_team.setdefault(int(g.away_team_id), _ctx_for(int(g.away_team_id), False))

    return by_team, by_pair


def _choose_game_for_event(
    *,
    pair_games: List[GameLite],
    commence_time: Optional[str],
) -> Optional[GameLite]:
    if not pair_games:
        return None
    if len(pair_games) == 1 or not commence_time:
        return pair_games[0]
    try:
        event_dt = datetime.fromisoformat(str(commence_time).replace("Z", "+00:00"))
    except Exception:
        return pair_games[0]

    def _dist(g: GameLite) -> float:
        if not g.game_time:
            return float("inf")
        try:
            gdt = datetime.fromisoformat(g.game_time)
            return abs((gdt - event_dt.astimezone(gdt.tzinfo)).total_seconds())
        except Exception:
            return float("inf")

    return sorted(pair_games, key=_dist)[0]


def _flatten_market_snapshot(
    *,
    events: Sequence[Dict[str, Any]],
    market_to_prop: Dict[str, str],
    team_name_rev: Dict[str, str],
    prop_filter: Optional[set[str]],
) -> tuple[List[Offer], Dict[str, int]]:
    counts: Dict[str, int] = defaultdict(int)
    grouped_books: Dict[Tuple[str, str, str, str, str, float], set[str]] = defaultdict(set)
    grouped_names: Dict[Tuple[str, str, str, str, str, float], str] = {}
    event_meta: Dict[str, Dict[str, Any]] = {}

    for ev in events:
        event_id = _clean_str(ev.get("id"))
        if not event_id:
            counts["skip_no_event_id"] += 1
            continue
        home_abbr, away_abbr = _parse_event_team_abbrs(ev, team_name_rev)
        if not home_abbr or not away_abbr:
            counts["skip_unknown_team_name"] += 1
            continue
        event_meta[event_id] = {
            "commence_time": _clean_str(ev.get("commence_time")),
            "home_team_name": _clean_str(ev.get("home_team")) or "",
            "away_team_name": _clean_str(ev.get("away_team")) or "",
            "home_team_abbr": home_abbr,
            "away_team_abbr": away_abbr,
        }

        for book in ev.get("bookmakers") or []:
            book_key = _clean_str(book.get("key")) or _clean_str(book.get("title")) or "book"
            for market in book.get("markets") or []:
                market_key = _clean_str(market.get("key"))
                if not market_key:
                    continue
                prop_type = market_to_prop.get(market_key)
                if not prop_type:
                    counts["skip_unsupported_market"] += 1
                    continue
                if prop_filter and prop_type not in prop_filter:
                    counts["skip_prop_filter"] += 1
                    continue
                for outcome in market.get("outcomes") or []:
                    player_name = _clean_str(outcome.get("description"))
                    if not player_name:
                        counts["skip_no_description"] += 1
                        continue
                    try:
                        line = float(outcome.get("point"))
                    except Exception:
                        counts["skip_no_line"] += 1
                        continue
                    pcol = _line_to_pcol(line)
                    if not pcol:
                        counts["skip_non_half_line"] += 1
                        continue
                    side = str(outcome.get("name") or "").strip().lower()
                    if side not in {"over", "under"}:
                        counts["skip_non_ou_side"] += 1
                        continue
                    key = (
                        event_id,
                        prop_type,
                        _norm_name(player_name),
                        home_abbr,
                        away_abbr,
                        float(line),
                    )
                    grouped_books[key].add(str(book_key))
                    grouped_names.setdefault(key, str(player_name))
                    counts["raw_outcomes"] += 1

    offers: List[Offer] = []
    for (event_id, prop_type, norm_player_name, home_abbr, away_abbr, line), books in grouped_books.items():
        meta = event_meta.get(event_id) or {}
        display_name = grouped_names.get((event_id, prop_type, norm_player_name, home_abbr, away_abbr, line))
        if not display_name:
            display_name = " ".join(w.capitalize() for w in norm_player_name.split())
        offers.append(
            Offer(
                event_id=event_id,
                commence_time=_clean_str(meta.get("commence_time")),
                home_team_name=str(meta.get("home_team_name") or ""),
                away_team_name=str(meta.get("away_team_name") or ""),
                home_team_abbr=str(home_abbr),
                away_team_abbr=str(away_abbr),
                prop_type=str(prop_type),
                player_name=display_name,
                line=float(line),
                books_seen=len(books),
            )
        )
    offers.sort(key=lambda o: (o.home_team_abbr, o.away_team_abbr, o.prop_type, o.player_name, o.line))
    counts["offers_unique"] = len(offers)
    return offers, dict(counts)


def _resolve_offers(
    *,
    offers: Sequence[Offer],
    by_name_team: Dict[Tuple[str, str], List[PlayerRow]],
    by_pair_games: Dict[Tuple[str, str], List[GameLite]],
) -> tuple[List[ResolvedOffer], Dict[str, int]]:
    counts: Dict[str, int] = defaultdict(int)
    resolved: List[ResolvedOffer] = []

    for off in offers:
        key_home = (_norm_name(off.player_name), off.home_team_abbr)
        key_away = (_norm_name(off.player_name), off.away_team_abbr)
        cand_home = by_name_team.get(key_home, [])
        cand_away = by_name_team.get(key_away, [])
        candidates = [(True, c) for c in cand_home] + [(False, c) for c in cand_away]

        if not candidates:
            counts["skip_player_not_found"] += 1
            continue
        if len(candidates) > 1:
            # If exactly one active candidate exists, prefer it.
            active = [(is_home, c) for (is_home, c) in candidates if c.active is True]
            if len(active) == 1:
                candidates = active
            else:
                counts["skip_player_ambiguous"] += 1
                continue

        is_home, player = candidates[0]
        team_abbr = off.home_team_abbr if is_home else off.away_team_abbr
        team_id = int(player.team_id) if player.team_id is not None else int(getTeamIdFromAbbr(team_abbr) or 0)
        if not team_id:
            counts["skip_missing_team_id"] += 1
            continue

        pair_games = by_pair_games.get((off.home_team_abbr, off.away_team_abbr), [])
        game = _choose_game_for_event(pair_games=pair_games, commence_time=off.commence_time)
        if game is None:
            counts["skip_game_not_found"] += 1
            continue

        resolved.append(
            ResolvedOffer(
                offer=off,
                player=player,
                team_abbr=team_abbr,
                team_id=team_id,
                is_home=bool(is_home),
                game=game,
            )
        )
        counts["resolved"] += 1

    return resolved, dict(counts)


def _monkeypatch_prepare_runtime(
    *,
    by_team_ctx: Dict[int, Dict[str, Any]],
    by_player_id: Dict[int, PlayerRow],
):
    orig_build_game_context = prop_workflow.build_game_context
    orig_resolve_player_candidate = prop_workflow.resolve_player_candidate

    def _local_build_game_context(*, team_id: int, game_date: str) -> Optional[Dict[str, Any]]:
        # Force prepare_prop() to use the exact row payload fallback context so we preserve
        # event-level game selection (including doubleheaders) without per-row StatsAPI calls.
        _ = (team_id, game_date, by_team_ctx)
        return None

    def _local_resolve_player_candidate(*, player_id: Optional[int], name: Optional[str], team_abbr: Optional[str]):
        if player_id is not None:
            row = by_player_id.get(int(player_id))
            if row:
                return {
                    "player_id": int(row.player_id),
                    "player_name": row.player_name,
                    "team_id": row.team_id,
                    "team_abbr": row.team_abbr or normalizeTeamAbbreviation(team_abbr),
                }
        return orig_resolve_player_candidate(player_id=player_id, name=name, team_abbr=team_abbr)

    prop_workflow.build_game_context = _local_build_game_context  # type: ignore[assignment]
    prop_workflow.resolve_player_candidate = _local_resolve_player_candidate  # type: ignore[assignment]

    def _restore():
        prop_workflow.build_game_context = orig_build_game_context  # type: ignore[assignment]
        prop_workflow.resolve_player_candidate = orig_resolve_player_candidate  # type: ignore[assignment]

    return _restore


def _predict_rows(
    resolved_offers: Sequence[ResolvedOffer],
    *,
    by_team_ctx: Dict[int, Dict[str, Any]],
    by_player_id: Dict[int, PlayerRow],
) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    counts: Dict[str, int] = defaultdict(int)
    rows: List[Dict[str, Any]] = []

    restore = _monkeypatch_prepare_runtime(by_team_ctx=by_team_ctx, by_player_id=by_player_id)
    try:
        for item in resolved_offers:
            off = item.offer
            g = item.game
            team_abbr = item.team_abbr
            home_abbr = normalizeTeamAbbreviation(g.home_abbr or getFullTeamAbbreviationFromID(g.home_team_id)) or off.home_team_abbr
            away_abbr = normalizeTeamAbbreviation(g.away_abbr or getFullTeamAbbreviationFromID(g.away_team_id)) or off.away_team_abbr

            payload = {
                "player_id": item.player.player_id,
                "player_name": item.player.player_name,
                "team_id": item.team_id,
                "team_abbr": team_abbr,
                "game_date": str(g.game_date),
                "game_id": int(g.game_id),
                "game_type": _clean_str(g.game_type),
                "game_time": _clean_str(g.game_time),
                "is_home": bool(item.is_home),
                "opponent_team_id": int(g.away_team_id if item.is_home else g.home_team_id),
                "opponent": away_abbr if item.is_home else home_abbr,
                "opponent_encoded": int(g.away_team_id if item.is_home else g.home_team_id),
                "game_day_of_week": (
                    datetime.fromisoformat(g.game_time).weekday() if g.game_time else None
                ),
                "time_of_day_bucket": (
                    get_time_of_day_bucket_et(datetime.fromisoformat(g.game_time)) if g.game_time else None
                ),
                "starting_pitcher_id": int(g.sp_away_id if item.is_home else g.sp_home_id) if (g.sp_away_id if item.is_home else g.sp_home_id) else None,
                "prop_type": off.prop_type,
                "prop_value": float(off.line),
                "over_under": "over",
            }
            try:
                prepared = prop_workflow.prepare_prop(payload)
                pred = prop_workflow.predict_prop(off.prop_type, prepared)
            except Exception:
                counts["skip_predict_error"] += 1
                continue

            try:
                prob_over = float(pred.get("probability_over"))
            except Exception:
                counts["skip_missing_probability"] += 1
                continue
            pcol = _line_to_pcol(off.line)
            if not pcol:
                counts["skip_non_half_line_late"] += 1
                continue

            rows.append(
                {
                    "player_id": int(item.player.player_id),
                    "player_name": item.player.player_name,
                    "team_id": int(item.team_id),
                    "team": team_abbr,
                    "opponent_id": int(g.away_team_id if item.is_home else g.home_team_id),
                    "opponent": away_abbr if item.is_home else home_abbr,
                    "is_home": bool(item.is_home),
                    "game_id": int(g.game_id),
                    "game_date": str(g.game_date),
                    "game_type": _clean_str(g.game_type),
                    "game_time": _clean_str(g.game_time),
                    "home_team_code": home_abbr,
                    "away_team_code": away_abbr,
                    "prop_type": off.prop_type,
                    "line": float(off.line),
                    "prob_over": prob_over,
                    "prob_col": pcol,
                    "books_seen": int(off.books_seen),
                }
            )
            counts["predicted"] += 1
    finally:
        restore()

    return rows, dict(counts)


def _to_wide(pred_rows: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    if not pred_rows:
        return pd.DataFrame()
    df = pd.DataFrame(pred_rows)
    id_cols = [
        "player_id",
        "player_name",
        "team_id",
        "team",
        "opponent_id",
        "opponent",
        "is_home",
        "game_id",
        "game_date",
        "game_type",
        "game_time",
        "home_team_code",
        "away_team_code",
        "prop_type",
    ]
    df["rank_books_seen"] = pd.to_numeric(df["books_seen"], errors="coerce").fillna(0).astype(int)
    # If duplicate player/game/prop/line rows occur, keep the version seen across more books.
    df = df.sort_values(by=["rank_books_seen"], ascending=False, kind="stable")
    df = df.drop_duplicates(subset=id_cols + ["prob_col"], keep="first")

    wide = df.pivot_table(
        index=id_cols,
        columns="prob_col",
        values="prob_over",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None

    prob_cols = sorted([c for c in wide.columns if str(c).startswith("p_over_")], key=str)
    ordered = id_cols + prob_cols
    return wide[ordered].sort_values(by=["game_date", "game_id", "prop_type", "player_name"], kind="stable").reset_index(drop=True)


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Build MLB daily WIDE predictions CSV from OddsAPI market snapshot.")
    ap.add_argument("--slate-date", default=None, help="YYYY-MM-DD (ET). Defaults to SLATE_DATE or ET today.")
    ap.add_argument("--output", default=os.environ.get("MLB_PRED_CSV", str(DEFAULT_OUT_CSV)))
    ap.add_argument(
        "--prop-types",
        default=os.environ.get("MLB_PREDICT_PROP_TYPES", ""),
        help="Optional CSV of internal MLB prop types to include (default: all supported OddsAPI O/U props).",
    )
    ap.add_argument("--include-inactive", action="store_true", help="Include inactive player_ids rows in name resolution.")
    ap.add_argument("--require-min-rows", type=int, default=1, help="Fail if fewer than N wide rows are produced.")
    ap.add_argument("--strict", action="store_true", help="Fail when any rows are skipped for resolution/prediction reasons.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    slate_date = _clean_str(args.slate_date) or _clean_str(os.environ.get("SLATE_DATE")) or _date_et_today()
    out_csv = Path(str(args.output)).expanduser()
    prop_filter = _parse_prop_types_csv(str(args.prop_types or ""))

    print(f"[mlb-wide-pred] slate_date (ET) = {slate_date}")
    print(f"[mlb-wide-pred] output = {out_csv}")
    if prop_filter:
        print(f"[mlb-wide-pred] prop filter = {sorted(prop_filter)}")

    try:
        by_player_id, by_name_team = _load_player_rows(active_only=not bool(args.include_inactive))
        print(f"[mlb-wide-pred] player index rows={len(by_player_id)}")

        by_team_ctx, by_pair_games = _build_schedule_maps(str(slate_date))
        print(f"[mlb-wide-pred] schedule contexts={len(by_team_ctx)} pair_games={len(by_pair_games)}")

        events = market_odds_service._fetch_market_snapshot(game_date=str(slate_date))
        print(f"[mlb-wide-pred] odds snapshot events={len(events)}")

        market_to_prop = _invert_market_map()
        team_name_rev = _build_team_name_reverse()
        offers, flatten_counts = _flatten_market_snapshot(
            events=events,
            market_to_prop=market_to_prop,
            team_name_rev=team_name_rev,
            prop_filter=prop_filter,
        )
        print(f"[mlb-wide-pred] offers_unique={len(offers)} flatten_counts={flatten_counts}")

        resolved_offers, resolve_counts = _resolve_offers(
            offers=offers,
            by_name_team=by_name_team,
            by_pair_games=by_pair_games,
        )
        print(f"[mlb-wide-pred] resolved_offers={len(resolved_offers)} resolve_counts={resolve_counts}")

        pred_rows, pred_counts = _predict_rows(
            resolved_offers,
            by_team_ctx=by_team_ctx,
            by_player_id=by_player_id,
        )
        print(f"[mlb-wide-pred] predicted_rows={len(pred_rows)} pred_counts={pred_counts}")

        wide = _to_wide(pred_rows)
        if wide.empty:
            print("[mlb-wide-pred] ERROR: no wide rows produced", file=sys.stderr)
            return 1
        if len(wide) < int(args.require_min_rows):
            print(
                f"[mlb-wide-pred] ERROR: produced {len(wide)} rows < require-min-rows={int(args.require_min_rows)}",
                file=sys.stderr,
            )
            return 1

        skipped_total = sum(v for k, v in {**flatten_counts, **resolve_counts, **pred_counts}.items() if str(k).startswith("skip_"))
        if args.strict and skipped_total > 0:
            print(f"[mlb-wide-pred] ERROR: strict mode and skipped_total={skipped_total}", file=sys.stderr)
            return 1

        out_csv.parent.mkdir(parents=True, exist_ok=True)
        wide.to_csv(out_csv, index=False)
        prop_counts = wide["prop_type"].value_counts(dropna=False).sort_index().to_dict() if "prop_type" in wide.columns else {}
        print(f"[mlb-wide-pred] wrote {len(wide)} wide rows to {out_csv}")
        print(f"[mlb-wide-pred] prop_counts={prop_counts}")
        return 0
    except Exception as exc:
        print(f"[mlb-wide-pred] ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
