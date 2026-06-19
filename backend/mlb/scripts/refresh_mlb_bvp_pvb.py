#!/usr/bin/env python3
"""Refresh MLB batter-vs-pitcher (BvP/PvB) features into prop_features_precomputed.

This script pulls today's (or a date range's) scheduled games from MLB StatsAPI,
collects active hitters by team, and computes hitter-vs-opposing-probable-starter
career stats via the `vsPlayer` endpoint.

Rows are upserted into:
  mlb.prop_features_precomputed
keyed by:
  (prop_type, player_id, game_id, feature_set_tag)

Feature payloads are merged on conflict so existing rolling fields are preserved.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests

from backend.shared.db.pg import pg_connect


ET = ZoneInfo("America/New_York")
STATS_BASE = "https://statsapi.mlb.com/api/v1"

BATTER_PROPS: tuple[str, ...] = (
    "doubles",
    "hits",
    "hits_runs_rbis",
    "home_runs",
    "rbis",
    "runs_rbis",
    "runs_scored",
    "singles",
    "stolen_bases",
    "strikeouts_batting",
    "total_bases",
    "triples",
    "walks",
)


@dataclass(frozen=True)
class GameRow:
    game_id: int
    game_date: str
    home_team_id: int
    away_team_id: int
    prob_sp_home: Optional[int]
    prob_sp_away: Optional[int]


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> Iterable[date]:
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _fetch_json(url: str, *, timeout_sec: int, retries: int) -> Dict[str, Any]:
    attempts = max(1, int(retries))
    last_exc: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, timeout=timeout_sec)
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, dict) else {}
        except requests.RequestException as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            sleep_sec = min(10.0, 1.5 * attempt)
            print(
                f"[bvp-refresh] statsapi fetch retry attempt={attempt + 1}/{attempts} "
                f"sleep_sec={sleep_sec:.1f} url={url} error={type(exc).__name__}:{exc}",
                file=sys.stderr,
            )
            time.sleep(sleep_sec)
    if last_exc is not None:
        raise last_exc
    return {}


def _fetch_schedule_games(game_date: str, *, timeout_sec: int, retries: int) -> List[GameRow]:
    data = _fetch_json(
        f"{STATS_BASE}/schedule?sportId=1&date={game_date}&hydrate=probablePitcher",
        timeout_sec=timeout_sec,
        retries=retries,
    )
    out: List[GameRow] = []
    for day in data.get("dates") or []:
        for g in day.get("games") or []:
            teams = (g or {}).get("teams") or {}
            home = ((teams.get("home") or {}).get("team") or {})
            away = ((teams.get("away") or {}).get("team") or {})
            game_pk = (g or {}).get("gamePk")
            if not game_pk:
                continue
            try:
                game_id = int(game_pk)
                home_id = int(home.get("id"))
                away_id = int(away.get("id"))
            except Exception:
                continue

            sp_home_raw = ((teams.get("home") or {}).get("probablePitcher") or {}).get("id")
            sp_away_raw = ((teams.get("away") or {}).get("probablePitcher") or {}).get("id")
            try:
                sp_home = int(sp_home_raw) if sp_home_raw is not None else None
            except Exception:
                sp_home = None
            try:
                sp_away = int(sp_away_raw) if sp_away_raw is not None else None
            except Exception:
                sp_away = None

            out.append(
                GameRow(
                    game_id=game_id,
                    game_date=game_date,
                    home_team_id=home_id,
                    away_team_id=away_id,
                    prob_sp_home=sp_home,
                    prob_sp_away=sp_away,
                )
            )
    return out


def _augment_games_with_db_starters(games: Sequence[GameRow], game_date: str) -> Tuple[List[GameRow], Dict[str, int]]:
    """Fill missing probable starters from DB-side starter refs when available.

    Keeps script resilient when StatsAPI schedule omits probablePitcher fields
    (common for historical dates and occasionally early-day slates).
    """
    counters: Dict[str, int] = defaultdict(int)
    by_game: Dict[int, GameRow] = {int(g.game_id): g for g in games}

    need_fill = any(g.prob_sp_home is None or g.prob_sp_away is None for g in games)
    if not need_fill:
        return list(games), counters

    game_ids = [int(gid) for gid in by_game.keys()]
    if not game_ids:
        return list(games), counters

    try:
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
SELECT
  gi.game_id,
  gi.starting_pitcher_id_home AS home_starter_id,
  gi.starting_pitcher_id_away AS away_starter_id
FROM mlb.game_info gi
WHERE gi.game_id = ANY(%s)
""",
                (game_ids,),
            )
            rows = list(cur.fetchall() or [])
    except Exception as exc:
        counters["db_starter_query_errors"] += 1
        if str(os.getenv("MLB_BVP_DB_FALLBACK_WARN", "0")).strip() == "1":
            print(
                f"[bvp-refresh] WARN db starter fallback unavailable for {game_date}: "
                f"{type(exc).__name__}: {exc}"
            )
        return list(games), counters

    counters["db_starter_rows"] = len(rows)

    for r in rows:
        try:
            game_id = int(r["game_id"])
        except Exception:
            continue

        try:
            sp_home = int(r["home_starter_id"]) if r.get("home_starter_id") not in (None, "") else None
        except Exception:
            sp_home = None
        try:
            sp_away = int(r["away_starter_id"]) if r.get("away_starter_id") not in (None, "") else None
        except Exception:
            sp_away = None

        existing = by_game.get(game_id)
        if existing is None:
            continue

        new_home = existing.prob_sp_home if existing.prob_sp_home is not None else sp_home
        new_away = existing.prob_sp_away if existing.prob_sp_away is not None else sp_away
        if new_home != existing.prob_sp_home or new_away != existing.prob_sp_away:
            by_game[game_id] = GameRow(
                game_id=existing.game_id,
                game_date=existing.game_date,
                home_team_id=existing.home_team_id,
                away_team_id=existing.away_team_id,
                prob_sp_home=new_home,
                prob_sp_away=new_away,
            )
            counters["db_games_filled"] += 1

    return list(by_game.values()), counters


def _map_games_to_local_game_ids(games: Sequence[GameRow], game_date: str) -> Tuple[List[GameRow], Dict[str, int]]:
    """Align StatsAPI gamePk IDs to local mlb.game_info IDs by date + matchup.

    Local prediction flow keys by mlb.game_info.game_id. StatsAPI gamePk may
    differ for the same matchup/date, so this remap keeps precomputed features
    joinable in prepare_prop/predict paths.
    """
    counters: Dict[str, int] = defaultdict(int)
    if not games:
        return list(games), counters

    try:
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
SELECT game_id, home_team_id, away_team_id
FROM mlb.game_info
WHERE game_date = %s::date
""",
                (str(game_date),),
            )
            rows = list(cur.fetchall() or [])
    except Exception:
        counters["local_game_id_query_errors"] += 1
        return list(games), counters

    counters["local_game_id_candidate_rows"] = len(rows)
    by_matchup: Dict[Tuple[int, int], int] = {}
    for r in rows:
        try:
            key = (int(r["home_team_id"]), int(r["away_team_id"]))
            by_matchup[key] = int(r["game_id"])
        except Exception:
            continue

    mapped: List[GameRow] = []
    for g in games:
        local_game_id = by_matchup.get((int(g.home_team_id), int(g.away_team_id)))
        if local_game_id is None:
            counters["local_game_id_unmapped"] += 1
            mapped.append(g)
            continue
        if int(local_game_id) != int(g.game_id):
            counters["local_game_id_mapped"] += 1
            mapped.append(
                GameRow(
                    game_id=int(local_game_id),
                    game_date=g.game_date,
                    home_team_id=g.home_team_id,
                    away_team_id=g.away_team_id,
                    prob_sp_home=g.prob_sp_home,
                    prob_sp_away=g.prob_sp_away,
                )
            )
        else:
            counters["local_game_id_already_aligned"] += 1
            mapped.append(g)
    return mapped, counters


def _active_hitters(team_id: int, game_date: str, *, timeout_sec: int, retries: int) -> List[int]:
    data = _fetch_json(
        f"{STATS_BASE}/teams/{int(team_id)}/roster?rosterType=active&date={game_date}",
        timeout_sec=timeout_sec,
        retries=retries,
    )
    out: List[int] = []
    for row in data.get("roster") or []:
        person = (row or {}).get("person") or {}
        pos = (row or {}).get("position") or {}
        pid = person.get("id")
        if pid is None:
            continue
        pos_abbr = str(pos.get("abbreviation") or "").upper()
        pos_code = str(pos.get("code") or "").upper()
        pos_name = str(pos.get("name") or "").upper()
        if pos_abbr == "P" or pos_code == "1" or "PITCHER" in pos_name:
            continue
        try:
            out.append(int(pid))
        except Exception:
            continue
    return out


def _extract_vs_player_stats(payload: Dict[str, Any]) -> Dict[str, float]:
    stats = payload.get("stats") or []
    if not stats:
        return {}
    splits = (stats[0] or {}).get("splits") or []
    if not splits:
        return {}
    stat = (splits[0] or {}).get("stat") or {}

    pa = _to_float(stat.get("plateAppearances"))
    ab = _to_float(stat.get("atBats"))
    hits = _to_float(stat.get("hits"))
    hr = _to_float(stat.get("homeRuns"))
    rbi = _to_float(stat.get("rbi"))
    so = _to_float(stat.get("strikeOuts"))
    bb = _to_float(stat.get("baseOnBalls"))
    tb = _to_float(stat.get("totalBases"))

    # Canonical names used in feature metadata.
    out = {
        "bvp_plate_appearances": pa,
        "bvp_at_bats": ab,
        "bvp_hits": hits,
        "bvp_home_runs": hr,
        "bvp_rbi": rbi,
        "bvp_strikeouts": so,
        "bvp_walks": bb,
        "bvp_total_bases": tb,
    }

    # Keep legacy aliases for compatibility with historical payloads.
    out.update(
        {
            "bvp_pa_prior": pa,
            "bvp_ab_prior": ab,
            "bvp_hits_prior": hits,
            "bvp_hr_prior": hr,
            "bvp_so_prior": so,
            "bvp_bb_prior": bb,
            "bvp_tb_prior": tb,
        }
    )

    # Smoothed priors are occasionally useful in experimentation lanes.
    if ab > 0:
        out["bvp_avg_prior_sm"] = (hits + 1.0) / (ab + 2.0)
        out["bvp_tb_per_ab_prior_sm"] = (tb + 1.0) / (ab + 2.0)
    if pa > 0:
        out["bvp_bb_rate_prior_sm"] = (bb + 1.0) / (pa + 2.0)
        out["bvp_so_rate_prior_sm"] = (so + 1.0) / (pa + 2.0)
    return out


def _bvp_stats(hitter_id: int, pitcher_id: int, *, timeout_sec: int, retries: int) -> Dict[str, float]:
    url = (
        f"{STATS_BASE}/people/{int(hitter_id)}/stats"
        f"?group=hitting&stats=vsPlayer&opposingPlayerId={int(pitcher_id)}"
    )
    payload = _fetch_json(url, timeout_sec=timeout_sec, retries=retries)
    return _extract_vs_player_stats(payload)


def _upsert_rows(
    rows: Sequence[Tuple[str, int, int, str, Dict[str, float], str, str]],
    *,
    batch_size: int,
) -> int:
    if not rows:
        return 0
    sql = """
INSERT INTO mlb.prop_features_precomputed (
  prop_type,
  player_id,
  game_id,
  game_date,
  features,
  feature_set_tag,
  model_tag,
  computed_at
)
VALUES (%s, %s, %s, %s::date, %s::jsonb, %s, %s, NOW())
ON CONFLICT (prop_type, player_id, game_id, feature_set_tag)
DO UPDATE SET
  game_date = EXCLUDED.game_date,
  features = COALESCE(mlb.prop_features_precomputed.features, '{}'::jsonb) || EXCLUDED.features,
  model_tag = EXCLUDED.model_tag,
  computed_at = NOW()
"""

    written = 0
    with pg_connect() as conn:
        with conn.cursor() as cur:
            buf: List[Tuple[Any, ...]] = []
            for row in rows:
                buf.append(
                    (
                        row[0],
                        int(row[1]),
                        int(row[2]),
                        row[3],
                        json.dumps(row[4], separators=(",", ":")),
                        row[5],
                        row[6],
                    )
                )
                if len(buf) >= batch_size:
                    cur.executemany(sql, buf)
                    written += len(buf)
                    buf = []
            if buf:
                cur.executemany(sql, buf)
                written += len(buf)
        conn.commit()
    return written


def _build_rows_for_date(
    game_date: str,
    *,
    feature_set_tag: str,
    model_tag: str,
    timeout_sec: int,
    retries: int,
) -> Tuple[List[Tuple[str, int, int, str, Dict[str, float], str, str]], Dict[str, int]]:
    counters: Dict[str, int] = defaultdict(int)
    rows: List[Tuple[str, int, int, str, Dict[str, float], str, str]] = []

    games = _fetch_schedule_games(game_date, timeout_sec=timeout_sec, retries=retries)
    games, local_id_counters = _map_games_to_local_game_ids(games, game_date)
    for k, v in local_id_counters.items():
        counters[k] += int(v)
    games, db_counters = _augment_games_with_db_starters(games, game_date)
    for k, v in db_counters.items():
        counters[k] += int(v)
    counters["games"] = len(games)

    roster_cache: Dict[Tuple[int, str], List[int]] = {}
    bvp_cache: Dict[Tuple[int, int], Dict[str, float]] = {}

    for g in games:
        sides = (
            (g.home_team_id, g.prob_sp_away),
            (g.away_team_id, g.prob_sp_home),
        )
        for team_id, opp_sp in sides:
            if opp_sp is None:
                counters["skip_no_opp_sp"] += 1
                continue
            roster_key = (team_id, game_date)
            if roster_key not in roster_cache:
                try:
                    roster_cache[roster_key] = _active_hitters(
                        team_id,
                        game_date,
                        timeout_sec=timeout_sec,
                        retries=retries,
                    )
                    counters["roster_fetches"] += 1
                except Exception:
                    counters["roster_fetch_errors"] += 1
                    roster_cache[roster_key] = []
            hitters = roster_cache.get(roster_key) or []
            for hitter_id in hitters:
                bvp_key = (hitter_id, int(opp_sp))
                if bvp_key not in bvp_cache:
                    try:
                        bvp_cache[bvp_key] = _bvp_stats(
                            hitter_id,
                            int(opp_sp),
                            timeout_sec=timeout_sec,
                            retries=retries,
                        )
                        counters["bvp_fetches"] += 1
                    except Exception:
                        counters["bvp_fetch_errors"] += 1
                        bvp_cache[bvp_key] = {}
                feats = bvp_cache.get(bvp_key) or {}
                if not feats:
                    counters["empty_bvp_rows"] += 1
                    continue
                for prop in BATTER_PROPS:
                    rows.append(
                        (
                            prop,
                            int(hitter_id),
                            int(g.game_id),
                            game_date,
                            feats,
                            feature_set_tag,
                            model_tag,
                        )
                    )
                    counters["rows"] += 1
    return rows, counters


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Refresh MLB BvP/PvB features into prop_features_precomputed.")
    ap.add_argument("--date", help="Single ET date (YYYY-MM-DD). Default: today ET.")
    ap.add_argument("--from-date", help="Optional ET start date (YYYY-MM-DD).")
    ap.add_argument("--to-date", help="Optional ET end date (YYYY-MM-DD).")
    ap.add_argument("--feature-set-tag", default="v1", help="feature_set_tag upsert key (default: v1).")
    ap.add_argument("--model-tag", default="bvp_pvb_refresh_v1", help="model_tag marker (default: bvp_pvb_refresh_v1).")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--request-timeout-sec", type=int, default=20)
    ap.add_argument("--request-retries", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)

    if args.from_date or args.to_date:
        if not args.from_date or not args.to_date:
            raise SystemExit("--from-date and --to-date must be provided together")
        start = _parse_date(args.from_date)
        end = _parse_date(args.to_date)
        if end < start:
            raise SystemExit("--to-date must be >= --from-date")
        dates = [d.isoformat() for d in _date_range(start, end)]
    else:
        single = args.date or datetime.now(ET).date().isoformat()
        dates = [_parse_date(single).isoformat()]

    all_rows: List[Tuple[str, int, int, str, Dict[str, float], str, str]] = []
    total: Dict[str, int] = defaultdict(int)

    for d_iso in dates:
        rows, counters = _build_rows_for_date(
            d_iso,
            feature_set_tag=str(args.feature_set_tag),
            model_tag=str(args.model_tag),
            timeout_sec=int(args.request_timeout_sec),
            retries=int(args.request_retries),
        )
        all_rows.extend(rows)
        for k, v in counters.items():
            total[k] += int(v)
        print(
            f"[bvp-refresh] {d_iso} games={counters.get('games', 0)} "
            f"rows={counters.get('rows', 0)} bvp_fetches={counters.get('bvp_fetches', 0)} "
            f"bvp_fetch_errors={counters.get('bvp_fetch_errors', 0)} "
            f"local_game_id_mapped={counters.get('local_game_id_mapped', 0)} "
            f"db_games_filled={counters.get('db_games_filled', 0)} "
            f"skip_no_opp_sp={counters.get('skip_no_opp_sp', 0)}"
        )

    written = 0
    if args.dry_run:
        print(f"[bvp-refresh] dry-run: prepared_rows={len(all_rows)}")
    else:
        written = _upsert_rows(all_rows, batch_size=max(1, int(args.batch_size)))
        print(f"[bvp-refresh] upserted_rows={written}")

    print(
        "[bvp-refresh] summary "
        f"dates={len(dates)} games={total.get('games', 0)} rows_prepared={len(all_rows)} "
        f"rows_written={written} roster_fetch_errors={total.get('roster_fetch_errors', 0)} "
        f"bvp_fetch_errors={total.get('bvp_fetch_errors', 0)} empty_bvp_rows={total.get('empty_bvp_rows', 0)} "
        f"local_game_id_mapped={total.get('local_game_id_mapped', 0)} "
        f"local_game_id_unmapped={total.get('local_game_id_unmapped', 0)} "
        f"db_starter_rows={total.get('db_starter_rows', 0)} db_games_filled={total.get('db_games_filled', 0)} "
        f"db_starter_query_errors={total.get('db_starter_query_errors', 0)} "
        f"skip_no_opp_sp={total.get('skip_no_opp_sp', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
