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
import errno
import json
import os
import queue
import socket
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from pathlib import Path
import uuid
from zoneinfo import ZoneInfo

import requests
from urllib3.exceptions import NameResolutionError, NewConnectionError
from urllib3.util import Timeout

from backend.shared.db.pg import pg_connect
from backend.mlb.shared import bvp_identity as identity


ET = ZoneInfo("America/New_York")
STATS_BASE = "https://statsapi.mlb.com/api/v1"

# Initial schedule only. Other StatsAPI reads retain their existing contract.
INITIAL_SCHEDULE_RETRY_CONTRACT = "BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1"
INITIAL_SCHEDULE_WAITS = (10.0, 20.0)
INITIAL_SCHEDULE_BUDGET_SEC = 60.0


class InitialScheduleAcquisitionError(RuntimeError):
    """Sanitized, nonzero acquisition failure; never a downstream governed skip."""


class _UnfinishedHeaderRequest(RuntimeError):
    """An uninterruptible request must not overlap a replacement request."""


def _request_schedule_headers(url: str, timeout_sec: float) -> requests.Response:
    # requests' timeout alone does not bound OS getaddrinfo. A daemon worker
    # bounds the caller's wait. If it has not returned, fail closed: NEVER start
    # another attempt. This worker can only read public schedule headers, not
    # parse a schedule, begin BvP acquisition, publish artifacts or write rows.
    result: queue.Queue = queue.Queue(maxsize=1)
    abandoned = threading.Event()

    def request() -> None:
        try:
            response = requests.get(
                url, stream=True,
                timeout=Timeout(total=timeout_sec, connect=timeout_sec, read=timeout_sec),
            )
            result.put((response, None))
            if abandoned.is_set():
                response.close()
        except Exception as exc:
            result.put((None, exc))

    threading.Thread(target=request, daemon=True, name="bvp-schedule-headers").start()
    try:
        response, exc = result.get(timeout=timeout_sec)
    except queue.Empty:
        abandoned.set()
        # Close a response queued concurrently with the deadline, if present.
        try:
            response, _ = result.get_nowait()
            if response is not None:
                response.close()
        except queue.Empty:
            pass
        raise _UnfinishedHeaderRequest() from None
    if exc is not None:
        raise exc
    return response


def _initial_transient_class(exc: Exception) -> Optional[str]:
    if isinstance(exc, (requests.HTTPError, requests.exceptions.SSLError,
                        requests.exceptions.ProxyError)):
        return None
    if getattr(exc, "response", None) is not None:
        return None
    if isinstance(exc, requests.Timeout):
        return "PRE_RESPONSE_TIMEOUT"
    if not isinstance(exc, requests.ConnectionError):
        return None
    pending = [exc]
    seen: set[int] = set()
    connection_error = False
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        if isinstance(item, (socket.gaierror, NameResolutionError)):
            return "DNS_NAME_RESOLUTION_FAILURE"
        if isinstance(item, OSError) and item.errno in {
            errno.ENETDOWN, errno.ENETUNREACH, errno.EHOSTUNREACH,
            errno.ECONNREFUSED, errno.ECONNRESET, errno.ECONNABORTED, errno.ETIMEDOUT,
        }:
            connection_error = True
        if isinstance(item, NewConnectionError):
            connection_error = True
        for nested in (getattr(item, "reason", None), getattr(item, "__cause__", None),
                       getattr(item, "__context__", None), *getattr(item, "args", ())):
            if isinstance(nested, BaseException):
                pending.append(nested)
    return "CONNECTION_ESTABLISHMENT_FAILURE" if connection_error else None


def _initial_schedule_event(status: str, **fields: Any) -> str:
    timestamp = datetime.now(ZoneInfo("UTC")).isoformat()
    # No URL, exception message, response body, network identifier or credential.
    print("[bvp-refresh] " + json.dumps({
        "status": status, "contract": INITIAL_SCHEDULE_RETRY_CONTRACT,
        "stage": "INITIAL_SCHEDULE_FETCH",
        "timestamp_utc": timestamp, **fields,
    }, sort_keys=True), file=sys.stderr)
    return timestamp


def _fetch_initial_schedule_json(url: str, *, timeout_sec: int, retries: int) -> Dict[str, Any]:
    attempts = min(3, max(1, int(retries)))
    started = time.monotonic()
    first_failure = None
    retry_delay = 0.0
    for attempt in range(1, attempts + 1):
        remaining = INITIAL_SCHEDULE_BUDGET_SEC - (time.monotonic() - started)
        if remaining <= 0:
            _initial_schedule_event("ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED",
                                    attempts_used=attempt - 1, total_retry_delay_sec=retry_delay)
            raise InitialScheduleAcquisitionError("ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED")
        # Preserve first-attempt normal-awake timeout (default 20s); shorter
        # subsequent attempts keep 20 + 10 + 5 + 20 + 5 within 60s.
        attempt_timeout = min(max(0.001, float(timeout_sec)),
                              20.0 if attempt == 1 else 5.0, remaining)
        attempt_started_at = datetime.now(ZoneInfo("UTC")).isoformat()
        try:
            response = _request_schedule_headers(url, attempt_timeout)
        except Exception as exc:
            classification = _initial_transient_class(exc)
            next_wait = INITIAL_SCHEDULE_WAITS[attempt - 1] if attempt < attempts else 0.0
            if classification is None:
                next_wait = 0.0
            if time.monotonic() - started + next_wait >= INITIAL_SCHEDULE_BUDGET_SEC:
                next_wait = 0.0
            failure_time = _initial_schedule_event(
                "INITIAL_SCHEDULE_REQUEST_FAILED", attempt=attempt,
                classification=classification or (
                    "UNFINISHED_REQUEST_FAIL_CLOSED" if isinstance(exc, _UnfinishedHeaderRequest)
                    else "NON_RETRYABLE_PRE_RESPONSE_FAILURE"),
                next_wait_sec=next_wait,
                http_response_received="UNKNOWN" if isinstance(exc, _UnfinishedHeaderRequest)
                else getattr(exc, "response", None) is not None,
            )
            first_failure = first_failure or failure_time
            if classification is None:
                raise InitialScheduleAcquisitionError("INITIAL_SCHEDULE_NON_RETRYABLE_FAILURE") from None
            if next_wait == 0:
                _initial_schedule_event("ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED",
                                        attempts_used=attempt, total_retry_delay_sec=retry_delay)
                raise InitialScheduleAcquisitionError("ACQUISITION_FAILED_TRANSIENT_NETWORK_EXHAUSTED") from None
            time.sleep(next_wait)
            retry_delay += next_wait
            continue
        # Receiving headers is the retry boundary. HTTP, body-read, JSON and
        # schema failures below MUST NOT re-enter the request loop.
        try:
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or not isinstance(data.get("dates"), list):
                raise ValueError("INVALID_SCHEDULE_SCHEMA")
        except Exception as exc:
            _initial_schedule_event(
                "INITIAL_SCHEDULE_REQUEST_FAILED", attempt=attempt,
                classification="HTTP_STATUS_FAILURE" if isinstance(exc, requests.HTTPError)
                else "POST_RESPONSE_PARSE_OR_CONTRACT_FAILURE",
                next_wait_sec=0.0, http_response_received=True,
            )
            raise InitialScheduleAcquisitionError("INITIAL_SCHEDULE_POST_RESPONSE_FAILURE") from None
        finally:
            response.close()
        _initial_schedule_event(
            "ACQUISITION_SUCCESS_AFTER_TRANSIENT_NETWORK_RETRY" if first_failure
            else "INITIAL_SCHEDULE_REQUEST_SUCCESS",
            attempts_used=attempt, total_retry_delay_sec=retry_delay,
            first_failure_timestamp_utc=first_failure,
            successful_attempt_timestamp_utc=attempt_started_at,
            http_response_received=True,
        )
        return data
    raise AssertionError("unreachable initial schedule retry state")

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
    official_game_id: Optional[int] = field(default=None, compare=False)
    official_date: Optional[str] = field(default=None, compare=False)
    scheduled_start_utc: Optional[str] = field(default=None, compare=False)
    schedule_date: Optional[str] = field(default=None, compare=False)
    game_state: str = field(default="", compare=False)
    source_observed_at_utc: Optional[str] = field(default=None, compare=False)


class IdentityCounters(defaultdict):
    audit_path: Optional[Path] = None


class IdentityAudit:
    """Private, exclusive per-acquisition journal; fsync before feature admission."""
    def __init__(self, slate_date: str):
        folder = identity.ROOT / "artifacts/ops/bvp_identity_v1" / slate_date
        folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = folder / f"bvp_identity_{datetime.now(ZoneInfo('UTC')).strftime('%Y%m%dT%H%M%S%fZ')}_{os.getpid()}_{uuid.uuid4().hex}.jsonl"
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self.file = os.fdopen(descriptor, "w", encoding="utf-8")
        self.slate_date = slate_date

    def record(self, reason: str, game: Optional[GameRow] = None, **fields: Any) -> None:
        record = {"contract": identity.CONTRACT, "slate_date": self.slate_date,
                  "acquisition_timestamp_utc": datetime.now(ZoneInfo("UTC")).isoformat(),
                  "game_id": None, "batter_id": None, "pitcher_id": None,
                  "team_id": None, "opponent_team_id": None,
                  "reason_code": reason, **fields}
        if game is not None:
            record.update({"game_id": game.game_id, "official_game_id": game.official_game_id,
                           "official_date": game.official_date, "schedule_date": game.schedule_date,
                           "scheduled_start_utc": game.scheduled_start_utc,
                           "home_team_id": game.home_team_id, "away_team_id": game.away_team_id,
                           "source_observed_at_utc": game.source_observed_at_utc})
        self.file.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())

    def close(self) -> None:
        self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


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
    data = _fetch_initial_schedule_json(
        f"{STATS_BASE}/schedule?sportId=1&date={game_date}&hydrate=probablePitcher",
        timeout_sec=timeout_sec,
        retries=retries,
    )
    out: List[GameRow] = []
    observed = datetime.now(ZoneInfo("UTC")).isoformat()
    for day in data.get("dates") or []:
        if not isinstance(day, dict) or not isinstance(day.get("games"), list):
            raise identity.CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_SCHEDULE_DAY")
        if not day["games"] and day.get("date") != game_date:
            raise identity.CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_EMPTY_DAY_AUTHORITY")
        for g in day.get("games") or []:
            teams = (g or {}).get("teams") or {}
            home = ((teams.get("home") or {}).get("team") or {})
            away = ((teams.get("away") or {}).get("team") or {})
            game_pk = (g or {}).get("gamePk")
            if not game_pk:
                raise identity.CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_MISSING_GAME_ID")
            try:
                game_id = int(game_pk)
                home_id = int(home.get("id"))
                away_id = int(away.get("id"))
            except Exception:
                raise identity.CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_GAME_TEAMS") from None

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
                    official_game_id=game_id,
                    official_date=g.get("officialDate"),
                    scheduled_start_utc=g.get("gameDate"),
                    schedule_date=day.get("date"),
                    game_state=str((g.get("status") or {}).get("detailedState") or ""),
                    source_observed_at_utc=observed,
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
            by_game[game_id] = replace(existing,
                prob_sp_home=new_home,
                prob_sp_away=new_away,
            )
            counters["db_games_filled"] += 1

    return list(by_game.values()), counters


def _map_games_to_local_game_ids(games: Sequence[GameRow], game_date: str) -> Tuple[List[GameRow], Dict[str, int]]:
    """Optional local coverage only. NEVER replace the official MLB gamePk.

    Legacy telemetry name is retained; mapped is always zero. Date/team matches
    cannot prove game identity, especially repeated series games/doubleheaders.
    """
    counters: Dict[str, int] = IdentityCounters(int)
    counters.unmapped_game_ids = []
    if not games:
        return list(games), counters

    try:
        with pg_connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
SELECT game_id, home_team_id, away_team_id
FROM mlb.game_info
WHERE game_date = %s::date OR game_id = ANY(%s)
""",
                (str(game_date), [g.game_id for g in games]),
            )
            rows = list(cur.fetchall() or [])
    except Exception:
        counters["local_game_id_query_errors"] += 1
        counters["local_game_id_unmapped"] = len(games)
        counters.unmapped_game_ids = [g.game_id for g in games]
        return list(games), counters

    counters["local_game_id_candidate_rows"] = len(rows)
    by_id: Dict[int, Tuple[int, int]] = {}
    for r in rows:
        try:
            by_id[int(r["game_id"])] = (int(r["home_team_id"]), int(r["away_team_id"]))
        except Exception:
            continue

    mapped: List[GameRow] = []
    for g in games:
        if by_id.get(g.game_id) != (g.home_team_id, g.away_team_id):
            counters["local_game_id_unmapped"] += 1
            counters.unmapped_game_ids.append(g.game_id)
            mapped.append(g)
            continue
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


def _bvp_stats(hitter_id: int, pitcher_id: int, *, timeout_sec: int, retries: int,
               evidence: Optional[Dict[str, Any]] = None) -> Dict[str, float]:
    url = (
        f"{STATS_BASE}/people/{int(hitter_id)}/stats"
        f"?group=hitting&stats=vsPlayer&opposingPlayerId={int(pitcher_id)}"
    )
    payload = _fetch_json(url, timeout_sec=timeout_sec, retries=retries)
    features = _extract_vs_player_stats(payload)
    if evidence is not None:
        evidence.update({"response_sha256": identity.stable_hash(payload),
                         "response_observed_at_utc": datetime.now(ZoneInfo("UTC")).isoformat(),
                         "successful_response": True})
        if not features:
            evidence["empty_response_payload"] = payload
            stats = payload.get("stats")
            evidence["successful_empty_response_verified"] = isinstance(stats, list) and (
                not stats or (isinstance(stats[0], dict) and isinstance(stats[0].get("splits"), list)
                              and not stats[0]["splits"]))
    return features


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
    counters: Dict[str, int] = IdentityCounters(int)
    rows: List[Tuple[str, int, int, str, Dict[str, float], str, str]] = []

    games = _fetch_schedule_games(game_date, timeout_sec=timeout_sec, retries=retries)
    try:
        games, rejected_games = identity.validate_slate(games, game_date)
    except identity.CanonicalSlateIdentityError:
        # Preserve failed authority without performing a roster/vsPlayer read.
        with IdentityAudit(game_date) as audit:
            for game in games:
                audit.record("CANONICAL_IDENTITY_UNRESOLVED", game,
                             source_branch="AUTHORITATIVE_SLATE_CONSTRUCTION_FAILED")
            print(f"[bvp-refresh] identity_audit_path={audit.path.relative_to(identity.ROOT)} certification_status=CANONICAL_IDENTITY_UNRESOLVED")
        raise
    counters["intended_games"] = len(games) + sum(reason != "OFF_DATE_GAME_REJECTED" for _, reason in rejected_games)
    counters["canonically_verified_games"] = len(games)
    counters["off_date_rejected_games"] = sum(reason == "OFF_DATE_GAME_REJECTED" for _, reason in rejected_games)
    counters["canonical_identity_unresolved_games"] = sum(reason == "CANONICAL_IDENTITY_UNRESOLVED" for _, reason in rejected_games)
    source_games = {g.game_id: g for g in games}
    games, local_id_counters = _map_games_to_local_game_ids(games, game_date)
    for k, v in local_id_counters.items():
        counters[k] += int(v)
    games, db_counters = _augment_games_with_db_starters(games, game_date)
    for k, v in db_counters.items():
        counters[k] += int(v)
    counters["games"] = len(games)
    if not games and not rejected_games:
        return rows, counters  # Retain legitimate empty-slate behavior.
    with IdentityAudit(game_date) as audit:
        counters.audit_path = audit.path
        print(f"[bvp-refresh] identity_audit_path={audit.path.relative_to(identity.ROOT)} contract={identity.CONTRACT}")
        for game, reason in rejected_games:
            audit.record(reason, game, source_branch="STATSAPI_CANONICAL_SCHEDULE", rows_rejected=0)
        for game in games:
            audit.record("CANONICAL_SLATE_IDENTITY_VALID", game, source_branch="STATSAPI_CANONICAL_SCHEDULE")
            if game.game_id in getattr(local_id_counters, "unmapped_game_ids", []):
                audit.record("OPTIONAL_LOCAL_ID_UNMAPPED", game, source_branch=(
                    "LOCAL_ID_QUERY_FAILED_OFFICIAL_ID_RETAINED" if local_id_counters.get("local_game_id_query_errors")
                    else "NO_EXACT_LOCAL_ID_OFFICIAL_ID_RETAINED"))
            else:
                # Coverage is optional; source membership never depends on it.
                audit.record("OFFICIAL_GAME_ID_RETAINED", game, source_branch="NO_TEAM_DATE_SUBSTITUTION")
        rows = _collect_rows_for_games(games, source_games, game_date, feature_set_tag,
                                      model_tag, timeout_sec, retries, counters, audit)
        rows, rejected_rows = identity.filter_prepared_rows(rows, games, game_date)
        for rejection in rejected_rows:
            audit.record(rejection["reason_code"], source_branch="PRE_WRITE_CANONICAL_BOUNDARY",
                         **{k: v for k, v in rejection.items() if k != "reason_code"})
        counters["off_date_rejected_rows"] = sum(r["reason_code"] == "OFF_DATE_GAME_REJECTED" for r in rejected_rows)
        counters["canonical_identity_unresolved_rows"] = sum(r["reason_code"] == "CANONICAL_IDENTITY_UNRESOLVED" for r in rejected_rows)
        counters["rows"] = len(rows)
        audit.record("PRE_WRITE_IDENTITY_VALIDATION_COMPLETE", source_branch="PRE_WRITE_CANONICAL_BOUNDARY",
                     rows_prepared=len(rows), feature_set_tag=feature_set_tag, model_tag=model_tag,
                     prepared_row_stream_sha256=identity.stable_hash(rows), counters=dict(counters))
    return rows, counters


def _collect_rows_for_games(games, source_games, game_date, feature_set_tag, model_tag,
                            timeout_sec, retries, counters, audit):
    rows = []

    roster_cache: Dict[Tuple[int, str], List[int]] = {}
    bvp_cache: Dict[Tuple[int, int], Dict[str, float]] = {}
    evidence_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}
    unresolved_games = set()

    for g in games:
        sides = (
            (g.home_team_id, g.prob_sp_away),
            (g.away_team_id, g.prob_sp_home),
        )
        for team_id, opp_sp in sides:
            opponent_id = g.away_team_id if team_id == g.home_team_id else g.home_team_id
            request_identity = {"team_id": team_id, "opponent_team_id": opponent_id,
                                "feature_set_tag": feature_set_tag, "model_tag": model_tag}
            if opp_sp is None:
                counters["skip_no_opp_sp"] += 1
                unresolved_games.add(g.game_id)
                audit.record("OPPOSING_STARTER_UNRESOLVED", g, pitcher_id=None, batter_id=None,
                             source_branch="STATSAPI_THEN_EXACT_OFFICIAL_ID_DB_FALLBACK",
                             starter_resolution_status="FAILED_DB_QUERY" if counters.get("db_starter_query_errors") else "UNRESOLVED_AFTER_FALLBACK",
                             **request_identity)
                continue
            if int(opp_sp) <= 0:
                raise identity.CanonicalSlateIdentityError("CANONICAL_IDENTITY_UNRESOLVED_PITCHER_ID")
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
                    audit.record("ROSTER_FETCH_FAILED", g, pitcher_id=opp_sp,
                                 source_branch="STATSAPI_ACTIVE_ROSTER", **request_identity)
            hitters = roster_cache.get(roster_key) or []
            for hitter_id in hitters:
                bvp_key = (hitter_id, int(opp_sp))
                fresh_request = bvp_key not in bvp_cache
                if bvp_key not in bvp_cache:
                    evidence_cache[bvp_key] = {}
                    try:
                        bvp_cache[bvp_key] = _bvp_stats(
                            hitter_id,
                            int(opp_sp),
                            timeout_sec=timeout_sec,
                            retries=retries,
                            evidence=evidence_cache[bvp_key],
                        )
                        counters["bvp_fetches"] += 1
                    except Exception:
                        counters["bvp_fetch_errors"] += 1
                        bvp_cache[bvp_key] = {}
                        evidence_cache[bvp_key] = {"successful_response": False}
                feats = bvp_cache.get(bvp_key) or {}
                evidence = evidence_cache[bvp_key]
                source = source_games[g.game_id]
                source_pitcher = source.prob_sp_away if team_id == g.home_team_id else source.prob_sp_home
                audit.record("BVP_RESPONSE_NONEMPTY" if feats else (
                    "EMPTY_BVP_RESPONSE" if evidence.get("successful_empty_response_verified") else
                    "EMPTY_BVP_RESPONSE_UNVERIFIABLE" if evidence.get("successful_response") else "BVP_REQUEST_FAILED"),
                    g, batter_id=hitter_id, pitcher_id=int(opp_sp),
                    source_branch="STATSAPI_VSPLAYER_REQUEST" if fresh_request else "IN_RUN_REQUEST_CACHE",
                    starter_source_branch="STATSAPI_PROBABLE_STARTER" if source_pitcher is not None else "EXACT_OFFICIAL_ID_DB_FALLBACK",
                    feature_payload_sha256=identity.stable_hash(feats),
                    empty_response_is_zero_history=False, **request_identity, **evidence)
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
    counters["starter_unresolved_games"] = len(unresolved_games)
    return rows


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Refresh MLB BvP/PvB features into prop_features_precomputed.")
    ap.add_argument("--date", help="Single America/Los_Angeles slate date (YYYY-MM-DD). Default: today PT.")
    ap.add_argument("--from-date", help="Optional PT slate start date (YYYY-MM-DD).")
    ap.add_argument("--to-date", help="Optional PT slate end date (YYYY-MM-DD).")
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
        single = args.date or datetime.now(identity.PT).date().isoformat()
        dates = [_parse_date(single).isoformat()]

    all_rows: List[Tuple[str, int, int, str, Dict[str, float], str, str]] = []
    total: Dict[str, int] = defaultdict(int)
    audit_paths = []

    for d_iso in dates:
        rows, counters = _build_rows_for_date(
            d_iso,
            feature_set_tag=str(args.feature_set_tag),
            model_tag=str(args.model_tag),
            timeout_sec=int(args.request_timeout_sec),
            retries=int(args.request_retries),
        )
        all_rows.extend(rows)
        if getattr(counters, "audit_path", None):
            audit_paths.append(counters.audit_path)
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
        for path in audit_paths:
            with path.open("a",encoding="utf-8") as audit:
                audit.write(json.dumps({"contract":identity.CONTRACT,"reason_code":"DATABASE_WRITE_COMMITTED",
                                        "acquisition_timestamp_utc":datetime.now(ZoneInfo("UTC")).isoformat(),
                                        "rows_written_total":written},sort_keys=True)+"\n")
                audit.flush()
                os.fsync(audit.fileno())

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
    rejected = sum(total.get(k, 0) for k in ("off_date_rejected_games", "canonical_identity_unresolved_games",
                                             "off_date_rejected_rows", "canonical_identity_unresolved_rows"))
    print("[bvp-refresh] identity_summary " + json.dumps({
        "contract": identity.CONTRACT,
        "certification_status": "PARTIAL_IDENTITY_REJECTIONS" if rejected else "CANONICAL_SLATE_IDENTITY_VALID",
        "intended_games": total.get("intended_games", 0),
        "canonically_verified_games": total.get("canonically_verified_games", 0),
        "optional_local_id_unmapped_games": total.get("local_game_id_unmapped", 0),
        "starter_unresolved_games": total.get("starter_unresolved_games", 0),
        **{k: total.get(k, 0) for k in ("off_date_rejected_games", "canonical_identity_unresolved_games",
                                       "off_date_rejected_rows", "canonical_identity_unresolved_rows")},
        "rows_prepared": len(all_rows), "rows_written": written,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
