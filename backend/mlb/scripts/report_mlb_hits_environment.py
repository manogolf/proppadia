#!/usr/bin/env python3
"""Track MLB hits environment and pitcher-hits context for daily operations."""

from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.request
from datetime import date, datetime, timedelta
from math import sqrt
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.shared.db.pg import pg_fetchall
from backend.mlb.identity import (
    GameIdentityInput,
    GameIdentityResolver,
    MarketIdentityInput,
    PlayerIdentityInput,
    PlayerIdentityResolver,
    canonical_team_code as resolve_canonical_team_code,
    resolve_market_identity,
)
from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    normalizeTeamAbbreviation,
    teamIdMap,
    teamNameMap,
)
from backend.mlb.shared.name_normalization import normalize_player_name_key


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _to_iso(d: date) -> str:
    return d.isoformat()


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _as_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _ensure_parent(path: Path) -> None:
    if path.parent and str(path.parent) not in {".", ""}:
        path.parent.mkdir(parents=True, exist_ok=True)


def _write_generic_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    _ensure_parent(path)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _canonical_team_code(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.isdigit():
        abbr = getFullTeamAbbreviationFromID(int(text))
        return str(normalizeTeamAbbreviation(abbr) or "").strip()
    return str(normalizeTeamAbbreviation(text) or "").strip()


def _identity_team_code(value: Any) -> str:
    return resolve_canonical_team_code(value).canonical_team


def _norm_player_name(value: Any) -> str:
    return normalize_player_name_key(value)


def _team_name_reverse() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for abbr, full_name in teamNameMap.items():
        norm_abbr = _canonical_team_code(abbr)
        if norm_abbr:
            out[str(abbr).strip().lower()] = norm_abbr
            out[str(norm_abbr).strip().lower()] = norm_abbr
        if full_name:
            out[str(full_name).strip().lower()] = norm_abbr
    for info in teamIdMap.values():
        abbr = _canonical_team_code(info.get("abbr"))
        full_name = str(info.get("fullName") or "").strip()
        if abbr:
            out[str(abbr).strip().lower()] = abbr
        if full_name:
            out[full_name.lower()] = abbr
    out["athletics"] = "OAK"
    return out


def _team_abbr_from_schedule_team(team: Dict[str, Any]) -> str:
    raw_id = _as_int(team.get("id"))
    if raw_id is not None:
        return _canonical_team_code(raw_id)
    return _canonical_team_code(team.get("abbreviation") or team.get("name"))


def _blend_weighted(
    components: Sequence[Tuple[Optional[float], float]],
) -> Optional[float]:
    num = 0.0
    den = 0.0
    for value, weight in components:
        v = _as_float(value)
        w = _as_float(weight)
        if v is None or w is None:
            continue
        if w <= 0:
            continue
        num += v * w
        den += w
    if den <= 0:
        return None
    return num / den


def _clamp(value: Optional[float], min_value: float, max_value: float) -> Optional[float]:
    v = _as_float(value)
    if v is None:
        return None
    if min_value > max_value:
        min_value, max_value = max_value, min_value
    return max(min_value, min(max_value, v))


def _fetch_daily_game_hits(from_date: str, to_date: str) -> List[Dict[str, Any]]:
    rows = pg_fetchall(
        """
WITH team_hits AS (
  SELECT
    ps.game_date::date AS game_date,
    ps.game_id,
    ps.team,
    SUM(COALESCE(ps.hits, 0))::float8 AS team_hits
  FROM mlb.player_stats ps
  WHERE ps.game_date >= %s::date
    AND ps.game_date <= %s::date
  GROUP BY 1, 2, 3
),
game_hits AS (
  SELECT
    game_date,
    game_id,
    SUM(team_hits)::float8 AS game_hits
  FROM team_hits
  GROUP BY 1, 2
)
SELECT
  game_date,
  COUNT(*)::int AS games,
  SUM(game_hits)::float8 AS total_hits,
  AVG(game_hits)::float8 AS hits_per_game
FROM game_hits
GROUP BY game_date
ORDER BY game_date ASC
""",
        (from_date, to_date),
    )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        d_raw = row.get("game_date")
        d_text = str(d_raw)
        out.append(
            {
                "game_date": d_text,
                "games": int(row.get("games") or 0),
                "total_hits": float(row.get("total_hits") or 0.0),
                "hits_per_game": float(row.get("hits_per_game") or 0.0),
            }
        )
    return out


def _resolve_evaluation_date(rows: Sequence[Dict[str, Any]], as_of_date: str) -> Optional[str]:
    if not rows:
        return None
    candidates = [str(r.get("game_date") or "") for r in rows if str(r.get("game_date") or "")]
    if not candidates:
        return None
    if as_of_date in candidates:
        return as_of_date
    return max([d for d in candidates if d <= as_of_date], default=None)


def _summarize_league_environment(
    rows: Sequence[Dict[str, Any]],
    eval_date: str,
    lookback_days: int,
    recent_days: int,
) -> Dict[str, Any]:
    ordered = [dict(r) for r in rows if str(r.get("game_date") or "")]
    by_date = {str(r.get("game_date")): r for r in ordered}
    if eval_date not in by_date:
        return {"status": "fail", "reason": "evaluation_date_missing"}

    date_order = [str(r.get("game_date")) for r in ordered]
    idx = date_order.index(eval_date)
    eval_row = by_date[eval_date]
    eval_hpg = float(eval_row.get("hits_per_game") or 0.0)

    prior_rows = ordered[max(0, idx - lookback_days) : idx]
    prior_hpg = [float(r.get("hits_per_game") or 0.0) for r in prior_rows]
    baseline_mean = float(mean(prior_hpg)) if prior_hpg else None
    baseline_std = float(pstdev(prior_hpg)) if len(prior_hpg) >= 2 else None

    zscore: Optional[float] = None
    if baseline_mean is not None and baseline_std is not None and baseline_std > 0:
        zscore = (eval_hpg - baseline_mean) / baseline_std

    percentile: Optional[float] = None
    dist = sorted(prior_hpg + [eval_hpg]) if prior_hpg else []
    if dist:
        le_count = len([v for v in dist if v <= eval_hpg])
        percentile = (100.0 * le_count) / float(len(dist))

    recent_rows = ordered[max(0, idx - recent_days + 1) : idx + 1]
    prior_recent_rows = ordered[max(0, idx - (2 * recent_days) + 1) : max(0, idx - recent_days + 1)]
    recent_mean = float(mean([float(r.get("hits_per_game") or 0.0) for r in recent_rows])) if recent_rows else None
    prior_recent_mean = (
        float(mean([float(r.get("hits_per_game") or 0.0) for r in prior_recent_rows]))
        if prior_recent_rows
        else None
    )
    recent_delta = None
    if recent_mean is not None and prior_recent_mean is not None:
        recent_delta = recent_mean - prior_recent_mean

    if zscore is None:
        signal = "unknown"
    elif zscore >= 1.0:
        signal = "elevated"
    elif zscore <= -1.0:
        signal = "suppressed"
    else:
        signal = "normal"

    return {
        "status": "pass",
        "evaluation_row": eval_row,
        "baseline": {
            "rows": len(prior_rows),
            "mean_hits_per_game": baseline_mean,
            "std_hits_per_game": baseline_std,
        },
        "today_vs_baseline": {
            "hits_per_game": eval_hpg,
            "zscore": zscore,
            "percentile": percentile,
            "signal": signal,
        },
        "recent_trend": {
            "recent_days": int(recent_days),
            "recent_mean_hits_per_game": recent_mean,
            "prior_recent_mean_hits_per_game": prior_recent_mean,
            "delta_recent_minus_prior": recent_delta,
            "recent_rows": len(recent_rows),
            "prior_recent_rows": len(prior_recent_rows),
        },
    }


def _fetch_starter_hits_allowed_rows(eval_date: str) -> List[Dict[str, Any]]:
    rows = pg_fetchall(
        """
SELECT
  ps.game_id,
  ps.player_id,
  ps.team AS pitcher_team,
  ps.opponent AS offense_team,
  COALESCE(ps.hits_allowed, 0)::float8 AS hits_allowed_actual,
  COALESCE(ps.outs_recorded, 0)::int AS outs_recorded,
  pds.d7_hits_allowed::float8 AS d7_hits_allowed,
  pds.d15_hits_allowed::float8 AS d15_hits_allowed,
  pds.d30_hits_allowed::float8 AS d30_hits_allowed
FROM mlb.player_stats ps
LEFT JOIN mlb.player_derived_stats pds
  ON pds.player_id = ps.player_id
 AND pds.game_id = ps.game_id
WHERE ps.game_date = %s::date
  AND COALESCE(ps.is_starter, 0) = 1
  AND (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
  AND COALESCE(ps.outs_recorded, 0) > 0
ORDER BY ps.game_id, ps.team, ps.player_id
""",
        (eval_date,),
    )
    out: List[Dict[str, Any]] = []
    for row in rows or []:
        actual = _as_float(row.get("hits_allowed_actual")) or 0.0
        outs = _as_int(row.get("outs_recorded")) or 0
        d7 = _as_float(row.get("d7_hits_allowed"))
        d15 = _as_float(row.get("d15_hits_allowed"))
        d30 = _as_float(row.get("d30_hits_allowed"))
        residual_d7 = None if d7 is None else (actual - d7)
        out.append(
            {
                "game_id": _as_int(row.get("game_id")),
                "player_id": _as_int(row.get("player_id")),
                "pitcher_team": _canonical_team_code(row.get("pitcher_team")),
                "offense_team": _canonical_team_code(row.get("offense_team")),
                "hits_allowed_actual": actual,
                "outs_recorded": outs,
                "d7_hits_allowed": d7,
                "d15_hits_allowed": d15,
                "d30_hits_allowed": d30,
                "residual_vs_d7": residual_d7,
            }
        )
    return out


def _fetch_starter_flag_diagnostics(eval_date: str) -> Dict[str, Any]:
    rows = pg_fetchall(
        """
SELECT
  COUNT(*) FILTER (
    WHERE (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
      AND COALESCE(ps.outs_recorded, 0) > 0
  )::int AS pitcher_rows_with_outs,
  COUNT(*) FILTER (
    WHERE (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
      AND COALESCE(ps.outs_recorded, 0) > 0
      AND COALESCE(ps.is_starter, 0) = 1
  )::int AS starter_rows_flagged,
  COUNT(*) FILTER (
    WHERE (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
      AND COALESCE(ps.outs_recorded, 0) > 0
      AND COALESCE(ps.is_starter, 0) = 0
  )::int AS nonstarter_or_missing_flag_rows,
  COUNT(DISTINCT (ps.game_id, ps.team)) FILTER (
    WHERE (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
      AND COALESCE(ps.outs_recorded, 0) > 0
  )::int AS game_team_pitcher_rows_with_outs,
  COUNT(DISTINCT (ps.game_id, ps.team)) FILTER (
    WHERE (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
      AND COALESCE(ps.outs_recorded, 0) > 0
      AND COALESCE(ps.is_starter, 0) = 1
  )::int AS game_team_starter_rows_flagged
FROM mlb.player_stats ps
WHERE ps.game_date = %s::date
""",
        (eval_date,),
    )
    row = (rows or [{}])[0]
    return {
        "pitcher_rows_with_outs": _as_int(row.get("pitcher_rows_with_outs")) or 0,
        "starter_rows_flagged": _as_int(row.get("starter_rows_flagged")) or 0,
        "nonstarter_or_missing_flag_rows": _as_int(row.get("nonstarter_or_missing_flag_rows")) or 0,
        "game_team_pitcher_rows_with_outs": _as_int(row.get("game_team_pitcher_rows_with_outs")) or 0,
        "game_team_starter_rows_flagged": _as_int(row.get("game_team_starter_rows_flagged")) or 0,
    }


def _fetch_multi_season_starter_baselines(
    *,
    eval_date: str,
    seasons_back: int,
    season_weight_decay: float,
    min_starts: int,
) -> Tuple[Dict[int, Dict[str, Any]], Dict[str, Any]]:
    eval_d = _parse_date(eval_date)
    eval_year = int(eval_d.year)
    seasons_back = max(1, int(seasons_back))
    min_starts = max(1, int(min_starts))
    season_weight_decay = float(season_weight_decay)
    if season_weight_decay <= 0.0:
        season_weight_decay = 0.01
    if season_weight_decay > 1.0:
        season_weight_decay = 1.0
    min_season_year = int(eval_year - seasons_back + 1)

    rows = pg_fetchall(
        """
SELECT
  ps.player_id,
  EXTRACT(YEAR FROM ps.game_date)::int AS season_year,
  COUNT(*)::int AS starts,
  AVG(COALESCE(ps.hits_allowed, 0))::float8 AS hits_allowed_avg,
  AVG(COALESCE(ps.outs_recorded, 0))::float8 AS outs_recorded_avg
FROM mlb.player_stats ps
WHERE ps.game_date < %s::date
  AND EXTRACT(YEAR FROM ps.game_date)::int >= %s::int
  AND EXTRACT(MONTH FROM ps.game_date)::int BETWEEN 3 AND 11
  AND COALESCE(ps.is_starter, 0) = 1
  AND (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
  AND COALESCE(ps.outs_recorded, 0) > 0
GROUP BY ps.player_id, EXTRACT(YEAR FROM ps.game_date)::int
ORDER BY ps.player_id, season_year
""",
        (eval_date, min_season_year),
    )

    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows or []:
        pid = _as_int(row.get("player_id"))
        season_year = _as_int(row.get("season_year"))
        starts = _as_int(row.get("starts")) or 0
        h_avg = _as_float(row.get("hits_allowed_avg"))
        outs_avg = _as_float(row.get("outs_recorded_avg"))
        if pid is None or season_year is None or starts <= 0 or h_avg is None:
            continue
        grouped.setdefault(pid, []).append(
            {
                "season_year": int(season_year),
                "starts": int(starts),
                "hits_allowed_avg": float(h_avg),
                "outs_recorded_avg": outs_avg,
            }
        )

    out: Dict[int, Dict[str, Any]] = {}
    for pid, season_rows in grouped.items():
        weighted_num = 0.0
        weighted_den = 0.0
        total_starts = 0
        seasons_used = 0
        for season_row in sorted(season_rows, key=lambda x: int(x["season_year"]), reverse=True):
            season_year = int(season_row["season_year"])
            starts = int(season_row["starts"])
            h_avg = float(season_row["hits_allowed_avg"])
            distance = max(0, eval_year - season_year)
            season_weight = float(season_weight_decay ** distance)
            weighted_starts = float(starts) * season_weight
            weighted_num += h_avg * weighted_starts
            weighted_den += weighted_starts
            total_starts += starts
            seasons_used += 1

        expected = None
        if total_starts >= min_starts and weighted_den > 0:
            expected = weighted_num / weighted_den

        out[int(pid)] = {
            "player_id": int(pid),
            "total_starts": int(total_starts),
            "seasons_used": int(seasons_used),
            "expected_hits_allowed_weighted": expected,
        }

    meta = {
        "eval_year": int(eval_year),
        "seasons_back": int(seasons_back),
        "from_season_year": int(min_season_year),
        "season_weight_decay": float(season_weight_decay),
        "min_starts": int(min_starts),
        "players_with_history": int(len(out)),
    }
    return out, meta


def _summarize_starter_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    baseline_by_player: Optional[Dict[int, Dict[str, Any]]] = None,
    baseline_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not rows:
        return {"rows": 0}
    baseline_by_player = baseline_by_player or {}
    baseline_meta = baseline_meta or {}
    actuals = [float(r.get("hits_allowed_actual") or 0.0) for r in rows]
    outs_vals = [int(r.get("outs_recorded") or 0) for r in rows]
    d7_pairs = [
        (float(r.get("hits_allowed_actual") or 0.0), float(r.get("d7_hits_allowed")))
        for r in rows
        if r.get("d7_hits_allowed") is not None
    ]
    residuals = [a - d7 for a, d7 in d7_pairs]
    weighted_pairs: List[Tuple[float, float]] = []
    weighted_residual_rows: List[Dict[str, Any]] = []
    row_player_ids = {int(r.get("player_id") or 0) for r in rows if int(r.get("player_id") or 0) > 0}
    players_with_any_history = 0
    players_with_min_starts = 0
    for pid in row_player_ids:
        baseline = baseline_by_player.get(int(pid)) or {}
        if int(baseline.get("total_starts") or 0) > 0:
            players_with_any_history += 1
        if baseline.get("expected_hits_allowed_weighted") is not None:
            players_with_min_starts += 1

    for r in rows:
        pid = int(r.get("player_id") or 0)
        baseline = baseline_by_player.get(pid) or {}
        expected_weighted = _as_float(baseline.get("expected_hits_allowed_weighted"))
        if expected_weighted is None:
            continue
        actual = float(r.get("hits_allowed_actual") or 0.0)
        weighted_pairs.append((actual, expected_weighted))
        weighted_residual_rows.append(
            {
                "player_id": r.get("player_id"),
                "pitcher_team": r.get("pitcher_team"),
                "offense_team": r.get("offense_team"),
                "hits_allowed_actual": actual,
                "expected_hits_allowed_weighted": expected_weighted,
                "residual_vs_weighted": actual - expected_weighted,
            }
        )

    weighted_residuals = [a - e for a, e in weighted_pairs]
    top_weighted = sorted(
        weighted_residual_rows,
        key=lambda x: float(x.get("residual_vs_weighted") or 0.0),
        reverse=True,
    )[:5]
    top_resid = sorted(
        [
            {
                "player_id": r.get("player_id"),
                "pitcher_team": r.get("pitcher_team"),
                "offense_team": r.get("offense_team"),
                "hits_allowed_actual": r.get("hits_allowed_actual"),
                "d7_hits_allowed": r.get("d7_hits_allowed"),
                "residual_vs_d7": r.get("residual_vs_d7"),
            }
            for r in rows
            if r.get("residual_vs_d7") is not None
        ],
        key=lambda x: float(x.get("residual_vs_d7") or 0.0),
        reverse=True,
    )[:5]
    return {
        "rows": len(rows),
        "outs_recorded_avg": float(mean(outs_vals)) if outs_vals else None,
        "actual_hits_allowed_avg": float(mean(actuals)) if actuals else None,
        "actual_hits_allowed_total": float(sum(actuals)),
        "d7_pairs": len(d7_pairs),
        "expected_d7_hits_allowed_avg": float(mean([d7 for _, d7 in d7_pairs])) if d7_pairs else None,
        "residual_vs_d7_avg": float(mean(residuals)) if residuals else None,
        "residual_vs_d7_total": float(sum(residuals)) if residuals else None,
        "weighted_baseline": {
            "seasons_back": _as_int(baseline_meta.get("seasons_back")),
            "from_season_year": _as_int(baseline_meta.get("from_season_year")),
            "season_weight_decay": _as_float(baseline_meta.get("season_weight_decay")),
            "min_starts": _as_int(baseline_meta.get("min_starts")),
            "players_with_history": int(players_with_any_history),
            "players_meeting_min_starts": int(players_with_min_starts),
            "rows_with_weighted_expectation": int(len(weighted_pairs)),
            "expected_hits_allowed_weighted_avg": float(mean([e for _, e in weighted_pairs])) if weighted_pairs else None,
            "residual_vs_weighted_avg": float(mean(weighted_residuals)) if weighted_residuals else None,
            "residual_vs_weighted_total": float(sum(weighted_residuals)) if weighted_residuals else None,
            "top_positive_residuals_weighted": top_weighted,
        },
        "top_positive_residuals": top_resid,
    }


def _fetch_team_hits_form(as_of_date: str) -> Dict[str, Dict[str, Any]]:
    rows = pg_fetchall(
        """
WITH team_game_hits AS (
  SELECT
    ps.team,
    ps.game_date::date AS game_date,
    ps.game_id,
    SUM(COALESCE(ps.hits, 0))::float8 AS team_hits
  FROM mlb.player_stats ps
  WHERE ps.game_date <= %s::date
  GROUP BY 1, 2, 3
),
ranked AS (
  SELECT
    team,
    game_date,
    game_id,
    team_hits,
    ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC, game_id DESC) AS rn
  FROM team_game_hits
)
SELECT
  team,
  AVG(team_hits) FILTER (WHERE rn <= 7)::float8 AS hits_pg_last7,
  AVG(team_hits) FILTER (WHERE rn <= 15)::float8 AS hits_pg_last15,
  AVG(team_hits) FILTER (WHERE rn <= 30)::float8 AS hits_pg_last30,
  COUNT(*) FILTER (WHERE rn <= 7)::int AS n7,
  COUNT(*) FILTER (WHERE rn <= 15)::int AS n15,
  COUNT(*) FILTER (WHERE rn <= 30)::int AS n30
FROM ranked
GROUP BY team
ORDER BY team
""",
        (as_of_date,),
    )
    out: Dict[str, Dict[str, Any]] = {}

    def _weighted_merge(cur: Dict[str, Any], incoming: Dict[str, Any], val_key: str, n_key: str) -> None:
        cur_n = int(cur.get(n_key) or 0)
        inc_n = int(incoming.get(n_key) or 0)
        cur_v = _as_float(cur.get(val_key))
        inc_v = _as_float(incoming.get(val_key))
        total_n = cur_n + inc_n
        if total_n <= 0:
            cur[n_key] = 0
            cur[val_key] = None
            return
        cur_weighted = 0.0 if cur_v is None else (cur_v * cur_n)
        inc_weighted = 0.0 if inc_v is None else (inc_v * inc_n)
        cur[n_key] = total_n
        cur[val_key] = (cur_weighted + inc_weighted) / float(total_n)

    for row in rows or []:
        team = _canonical_team_code(row.get("team"))
        if not team:
            continue
        incoming = {
            "hits_pg_last7": _as_float(row.get("hits_pg_last7")),
            "hits_pg_last15": _as_float(row.get("hits_pg_last15")),
            "hits_pg_last30": _as_float(row.get("hits_pg_last30")),
            "n7": int(row.get("n7") or 0),
            "n15": int(row.get("n15") or 0),
            "n30": int(row.get("n30") or 0),
        }
        cur = out.get(team)
        if cur is None:
            out[team] = incoming
            continue
        _weighted_merge(cur, incoming, "hits_pg_last7", "n7")
        _weighted_merge(cur, incoming, "hits_pg_last15", "n15")
        _weighted_merge(cur, incoming, "hits_pg_last30", "n30")
    return out


def _fetch_team_bullpen_hits_allowed_form(as_of_date: str) -> Dict[str, Dict[str, Any]]:
    rows = pg_fetchall(
        """
WITH team_game_bullpen_hits_allowed AS (
  SELECT
    ps.team,
    ps.game_date::date AS game_date,
    ps.game_id,
    SUM(COALESCE(ps.hits_allowed, 0))::float8 AS bullpen_hits_allowed
  FROM mlb.player_stats ps
  WHERE ps.game_date <= %s::date
    AND (ps.position = 'P' OR ps.hits_allowed IS NOT NULL)
    AND COALESCE(ps.outs_recorded, 0) > 0
    AND COALESCE(ps.is_starter, 0) = 0
  GROUP BY 1, 2, 3
),
ranked AS (
  SELECT
    team,
    game_date,
    game_id,
    bullpen_hits_allowed,
    ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC, game_id DESC) AS rn
  FROM team_game_bullpen_hits_allowed
)
SELECT
  team,
  AVG(bullpen_hits_allowed) FILTER (WHERE rn <= 7)::float8 AS bullpen_hits_allowed_pg_last7,
  AVG(bullpen_hits_allowed) FILTER (WHERE rn <= 15)::float8 AS bullpen_hits_allowed_pg_last15,
  AVG(bullpen_hits_allowed) FILTER (WHERE rn <= 30)::float8 AS bullpen_hits_allowed_pg_last30,
  COUNT(*) FILTER (WHERE rn <= 7)::int AS n7,
  COUNT(*) FILTER (WHERE rn <= 15)::int AS n15,
  COUNT(*) FILTER (WHERE rn <= 30)::int AS n30
FROM ranked
GROUP BY team
ORDER BY team
""",
        (as_of_date,),
    )
    out: Dict[str, Dict[str, Any]] = {}

    def _weighted_merge(cur: Dict[str, Any], incoming: Dict[str, Any], val_key: str, n_key: str) -> None:
        cur_n = int(cur.get(n_key) or 0)
        inc_n = int(incoming.get(n_key) or 0)
        cur_v = _as_float(cur.get(val_key))
        inc_v = _as_float(incoming.get(val_key))
        total_n = cur_n + inc_n
        if total_n <= 0:
            cur[n_key] = 0
            cur[val_key] = None
            return
        cur_weighted = 0.0 if cur_v is None else (cur_v * cur_n)
        inc_weighted = 0.0 if inc_v is None else (inc_v * inc_n)
        cur[n_key] = total_n
        cur[val_key] = (cur_weighted + inc_weighted) / float(total_n)

    for row in rows or []:
        team = _canonical_team_code(row.get("team"))
        if not team:
            continue
        incoming = {
            "bullpen_hits_allowed_pg_last7": _as_float(row.get("bullpen_hits_allowed_pg_last7")),
            "bullpen_hits_allowed_pg_last15": _as_float(row.get("bullpen_hits_allowed_pg_last15")),
            "bullpen_hits_allowed_pg_last30": _as_float(row.get("bullpen_hits_allowed_pg_last30")),
            "n7": int(row.get("n7") or 0),
            "n15": int(row.get("n15") or 0),
            "n30": int(row.get("n30") or 0),
        }
        cur = out.get(team)
        if cur is None:
            out[team] = incoming
            continue
        _weighted_merge(cur, incoming, "bullpen_hits_allowed_pg_last7", "n7")
        _weighted_merge(cur, incoming, "bullpen_hits_allowed_pg_last15", "n15")
        _weighted_merge(cur, incoming, "bullpen_hits_allowed_pg_last30", "n30")
    return out


def _fetch_actual_offense_hits_by_game_team(eval_date: str) -> Dict[Tuple[int, str], float]:
    rows = pg_fetchall(
        """
WITH team_game_hits AS (
  SELECT
    ps.game_id,
    ps.team AS offense_team,
    SUM(COALESCE(ps.hits, 0))::float8 AS offense_hits_actual
  FROM mlb.player_stats ps
  WHERE ps.game_date = %s::date
  GROUP BY ps.game_id, ps.team
)
SELECT game_id, offense_team, offense_hits_actual
FROM team_game_hits
ORDER BY game_id, offense_team
""",
        (eval_date,),
    )
    out: Dict[Tuple[int, str], float] = {}
    for row in rows or []:
        game_id = _as_int(row.get("game_id"))
        offense_team = _canonical_team_code(row.get("offense_team"))
        actual = _as_float(row.get("offense_hits_actual"))
        if game_id is None or not offense_team or actual is None:
            continue
        out[(int(game_id), offense_team)] = float(actual)
    return out


def _evaluate_team_hits_allowed_expectation(
    rows: Sequence[Dict[str, Any]],
    *,
    actual_offense_hits_by_game_team: Dict[Tuple[int, str], float],
) -> Dict[str, Any]:
    unique_expected: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    for row in rows:
        expected_team = _as_float(row.get("expected_team_hits_allowed_matchup"))
        if expected_team is None:
            continue
        game_id = _as_int(row.get("game_id"))
        pitcher_team = _canonical_team_code(row.get("pitcher_team"))
        offense_team = _canonical_team_code(row.get("offense_team"))
        if game_id is None or not pitcher_team or not offense_team:
            continue
        key = (int(game_id), pitcher_team, offense_team)
        if key in unique_expected:
            continue
        unique_expected[key] = {
            "game_id": int(game_id),
            "pitcher_team": pitcher_team,
            "offense_team": offense_team,
            "player_id": _as_int(row.get("player_id")),
            "player_name": str(row.get("player_name") or "").strip(),
            "expected_team_hits_allowed_matchup": expected_team,
            "expected_hits_allowed_matchup": _as_float(row.get("expected_hits_allowed_matchup")),
            "bullpen_hits_allowed_form_blended": _as_float(row.get("bullpen_hits_allowed_form_blended")),
        }

    rows_with_expected = len(unique_expected)
    if rows_with_expected == 0:
        return {
            "rows_with_expected": 0,
            "rows_with_actual": 0,
            "rows_missing_actual": 0,
        }

    eval_rows: List[Dict[str, Any]] = []
    for expected_row in unique_expected.values():
        game_id = int(expected_row.get("game_id") or 0)
        offense_team = str(expected_row.get("offense_team") or "").strip()
        actual = actual_offense_hits_by_game_team.get((game_id, offense_team))
        if actual is None:
            continue
        expected_team = float(expected_row.get("expected_team_hits_allowed_matchup") or 0.0)
        starter_expected = _as_float(expected_row.get("expected_hits_allowed_matchup"))
        residual = float(actual) - expected_team
        starter_only_residual = None if starter_expected is None else (float(actual) - starter_expected)
        eval_rows.append(
            {
                "game_id": game_id,
                "pitcher_team": expected_row.get("pitcher_team"),
                "offense_team": offense_team,
                "player_id": expected_row.get("player_id"),
                "player_name": expected_row.get("player_name"),
                "actual_offense_hits": float(actual),
                "expected_team_hits_allowed_matchup": expected_team,
                "expected_hits_allowed_matchup": starter_expected,
                "bullpen_hits_allowed_form_blended": _as_float(expected_row.get("bullpen_hits_allowed_form_blended")),
                "residual_actual_minus_expected_team": residual,
                "residual_actual_minus_expected_starter_only": starter_only_residual,
            }
        )

    rows_with_actual = len(eval_rows)
    if rows_with_actual == 0:
        return {
            "rows_with_expected": int(rows_with_expected),
            "rows_with_actual": 0,
            "rows_missing_actual": int(rows_with_expected),
            "coverage_pct": 0.0,
        }

    expected_vals = [float(r.get("expected_team_hits_allowed_matchup") or 0.0) for r in eval_rows]
    actual_vals = [float(r.get("actual_offense_hits") or 0.0) for r in eval_rows]
    residual_vals = [float(r.get("residual_actual_minus_expected_team") or 0.0) for r in eval_rows]
    mae_vals = [abs(v) for v in residual_vals]
    rmse = sqrt(sum((v * v) for v in residual_vals) / float(len(residual_vals)))
    starter_only_residual_vals = [
        float(r.get("residual_actual_minus_expected_starter_only"))
        for r in eval_rows
        if r.get("residual_actual_minus_expected_starter_only") is not None
    ]

    by_pitcher_team: Dict[str, List[float]] = {}
    for row in eval_rows:
        team = str(row.get("pitcher_team") or "").strip()
        if not team:
            continue
        by_pitcher_team.setdefault(team, []).append(float(row.get("residual_actual_minus_expected_team") or 0.0))

    by_pitcher_team_rows = sorted(
        [
            {
                "pitcher_team": team,
                "rows": int(len(vals)),
                "residual_avg": float(mean(vals)),
                "residual_total": float(sum(vals)),
            }
            for team, vals in by_pitcher_team.items()
            if vals
        ],
        key=lambda x: float(x.get("residual_avg") or 0.0),
        reverse=True,
    )

    top_over = sorted(
        eval_rows,
        key=lambda x: float(x.get("residual_actual_minus_expected_team") or 0.0),
        reverse=True,
    )[:5]
    top_under = sorted(
        eval_rows,
        key=lambda x: float(x.get("residual_actual_minus_expected_team") or 0.0),
    )[:5]

    return {
        "rows_with_expected": int(rows_with_expected),
        "rows_with_actual": int(rows_with_actual),
        "rows_missing_actual": int(rows_with_expected - rows_with_actual),
        "coverage_pct": float((100.0 * rows_with_actual) / float(rows_with_expected)),
        "expected_team_hits_allowed_avg": float(mean(expected_vals)),
        "actual_offense_hits_avg": float(mean(actual_vals)),
        "residual_avg": float(mean(residual_vals)),
        "residual_total": float(sum(residual_vals)),
        "mae": float(mean(mae_vals)),
        "rmse": float(rmse),
        "starter_only_residual_avg": float(mean(starter_only_residual_vals)) if starter_only_residual_vals else None,
        "starter_only_residual_total": float(sum(starter_only_residual_vals)) if starter_only_residual_vals else None,
        "top_over_expected_matchups": top_over,
        "top_under_expected_matchups": top_under,
        "top_positive_residual_pitcher_teams": by_pitcher_team_rows[:5],
        "top_negative_residual_pitcher_teams": sorted(
            by_pitcher_team_rows,
            key=lambda x: float(x.get("residual_avg") or 0.0),
        )[:5],
    }


def _load_wide_context(path: Path, slate_date: str) -> Dict[Tuple[int, int, str], Dict[str, Any]]:
    if not path.exists():
        return {}
    out: Dict[Tuple[int, int, str], Dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            game_date = str(row.get("game_date") or "").strip()
            if slate_date and game_date != slate_date:
                continue
            prop_type = str(row.get("prop_type") or "").strip().lower()
            if prop_type != "hits_allowed":
                continue
            game_id = _as_int(row.get("game_id"))
            player_id = _as_int(row.get("player_id"))
            if game_id is None or player_id is None:
                continue
            out[(game_id, player_id, prop_type)] = row
    return out


def _load_wide_pitcher_context(path: Path, slate_date: str) -> Dict[Tuple[int, int], Dict[str, Any]]:
    if not path.exists():
        return {}
    allowed_prop_types = {
        "hits_allowed",
        "strikeouts_pitching",
        "outs_recorded",
        "walks_allowed",
        "earned_runs",
    }
    out: Dict[Tuple[int, int], Dict[str, Any]] = {}
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            game_date = str(row.get("game_date") or "").strip()
            if slate_date and game_date != slate_date:
                continue
            prop_type = str(row.get("prop_type") or "").strip().lower()
            if prop_type not in allowed_prop_types:
                continue
            game_id = _as_int(row.get("game_id"))
            player_id = _as_int(row.get("player_id"))
            if game_id is None or player_id is None:
                continue
            key = (game_id, player_id)
            cur = out.get(key)
            if cur is None:
                out[key] = row
                continue
            # Prefer canonical hits_allowed row when multiple pitcher props exist.
            cur_prop = str(cur.get("prop_type") or "").strip().lower()
            if cur_prop != "hits_allowed" and prop_type == "hits_allowed":
                out[key] = row
    return out


def _load_odds_pitcher_hits_allowed_rows(path: Path, slate_date: str) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    events = raw.get("events") if isinstance(raw, dict) else raw
    if not isinstance(events, list):
        return []
    team_rev = _team_name_reverse()
    out: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for ev in events:
        if not isinstance(ev, dict):
            continue
        commence = str(ev.get("commence_time") or "")
        if slate_date and commence[:10] and commence[:10] not in {slate_date, str(_parse_date(slate_date) + timedelta(days=1))}:
            # Keep this broad enough for UTC late games, but avoid unrelated archive data.
            continue
        home = team_rev.get(str(ev.get("home_team") or "").strip().lower()) or _canonical_team_code(ev.get("home_team"))
        away = team_rev.get(str(ev.get("away_team") or "").strip().lower()) or _canonical_team_code(ev.get("away_team"))
        if not home or not away:
            continue
        for bm in ev.get("bookmakers") or []:
            for market in bm.get("markets") or []:
                if str(market.get("key") or "").strip() != "pitcher_hits_allowed":
                    continue
                for outcome in market.get("outcomes") or []:
                    name = str(outcome.get("description") or "").strip()
                    if not name:
                        continue
                    line = _as_float(outcome.get("point"))
                    key = (_norm_player_name(name), home, away)
                    cur = out.setdefault(
                        key,
                        {
                            "player_name": name,
                            "home_team_code": home,
                            "away_team_code": away,
                            "odds_lines": set(),
                            "odds_books_seen": set(),
                        },
                    )
                    if line is not None:
                        cur["odds_lines"].add(line)
                    book = str(bm.get("key") or "").strip()
                    if book:
                        cur["odds_books_seen"].add(book)
    rows: List[Dict[str, Any]] = []
    for row in out.values():
        books = row.get("odds_books_seen")
        lines = row.get("odds_lines")
        row["odds_books_seen"] = len(books) if isinstance(books, set) else 0
        if isinstance(lines, set):
            row["line"] = ",".join(str(x).rstrip("0").rstrip(".") if isinstance(x, float) else str(x) for x in sorted(lines))
        else:
            row["line"] = ""
        row.pop("odds_lines", None)
        rows.append(row)
    return rows


def _load_probable_starter_rows(slate_date: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    source_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={slate_date}&hydrate=probablePitcher"
    try:
        with urllib.request.urlopen(source_url, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return [], {
            "probable_starter_source": source_url,
            "probable_starter_status": "error",
            "probable_starter_error": f"{type(exc).__name__}: {exc}",
            "probable_starters_total": 0,
        }

    out: List[Dict[str, Any]] = []
    for date_block in payload.get("dates") or []:
        for game in date_block.get("games") or []:
            if str(game.get("officialDate") or "")[:10] != slate_date:
                continue
            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team = _team_abbr_from_schedule_team(home.get("team") or {})
            away_team = _team_abbr_from_schedule_team(away.get("team") or {})
            if not home_team or not away_team:
                continue
            for pitcher_team, offense_team, side in (
                (away_team, home_team, away),
                (home_team, away_team, home),
            ):
                pitcher = side.get("probablePitcher") or {}
                player_id = _as_int(pitcher.get("id"))
                player_name = str(pitcher.get("fullName") or pitcher.get("name") or "").strip()
                if player_id is None and not player_name:
                    continue
                out.append(
                    {
                        "game_id": _as_int(game.get("gamePk")),
                        "game_date": slate_date,
                        "home_team_code": home_team,
                        "away_team_code": away_team,
                        "pitcher_team": pitcher_team,
                        "offense_team": offense_team,
                        "player_id": player_id,
                        "player_name": player_name,
                        "probable_starter_source": source_url,
                    }
                )
    return out, {
        "probable_starter_source": source_url,
        "probable_starter_status": "ok",
        "probable_starters_total": len(out),
    }


def _probable_by_name_game(probable_rows: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for row in probable_rows:
        name = _norm_player_name(row.get("player_name"))
        home = _canonical_team_code(row.get("home_team_code"))
        away = _canonical_team_code(row.get("away_team_code"))
        if name and home and away:
            out[(name, home, away)] = dict(row)
    return out


def _name_tokens_for_identity(value: Any) -> Tuple[str, str]:
    tokens = _norm_player_name(value).split()
    if not tokens:
        return "", ""
    return tokens[0], tokens[-1]


def _probable_name_alias_compatible(provider_name: Any, probable_name: Any) -> bool:
    provider_first, provider_last = _name_tokens_for_identity(provider_name)
    probable_first, probable_last = _name_tokens_for_identity(probable_name)
    if not provider_first or not probable_first or not provider_last or not probable_last:
        return False
    if provider_last != probable_last:
        return False
    if provider_first == probable_first:
        return True
    return provider_first.startswith(probable_first) or probable_first.startswith(provider_first)


def _find_probable_by_name_alias_game(
    *,
    player_name: Any,
    home_team: Any,
    away_team: Any,
    probable_rows: Sequence[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    home = _canonical_team_code(home_team)
    away = _canonical_team_code(away_team)
    matches: List[Dict[str, Any]] = []
    for row in probable_rows:
        row_home = _canonical_team_code(row.get("home_team_code"))
        row_away = _canonical_team_code(row.get("away_team_code"))
        if home and away and {row_home, row_away} != {home, away}:
            continue
        if _probable_name_alias_compatible(player_name, row.get("player_name")):
            matches.append(dict(row))
    unique_by_id: Dict[str, Dict[str, Any]] = {}
    for row in matches:
        player_id = str(_as_int(row.get("player_id")) or "")
        key = player_id or _norm_player_name(row.get("player_name"))
        if key:
            unique_by_id[key] = row
    if len(unique_by_id) == 1:
        return next(iter(unique_by_id.values()))
    return None


def _fetch_prior_starter_counts(player_ids: Sequence[int], slate_date: str) -> Dict[int, int]:
    ids = sorted({int(pid) for pid in player_ids if _as_int(pid) is not None})
    if not ids:
        return {}
    starter_rows = pg_fetchall(
        """
        SELECT player_id,
               COUNT(DISTINCT game_id)::int AS starter_games
        FROM mlb.player_stats
        WHERE player_id = ANY(%s::bigint[])
          AND game_date < %s::date
          AND COALESCE(is_starter, 0) = 1
          AND COALESCE(outs_recorded, 0) >= 1
        GROUP BY player_id
        """,
        (ids, slate_date),
    )
    return {int(r.get("player_id")): int(r.get("starter_games") or 0) for r in starter_rows or []}


def _resolve_odds_pitcher_rows(
    rows: Sequence[Dict[str, Any]],
    slate_date: str,
    probable_rows: Sequence[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    if not rows:
        return []
    probable_lookup = _probable_by_name_game(probable_rows or [])
    player_rows = pg_fetchall(
        """
        SELECT player_id, player_name, team, team_id
        FROM mlb.player_ids
        WHERE player_name IS NOT NULL
        """
    )
    by_name: Dict[str, List[Dict[str, Any]]] = {}
    for raw in player_rows or []:
        row = dict(raw)
        key = _norm_player_name(row.get("player_name"))
        if not key:
            continue
        by_name.setdefault(key, []).append(row)

    resolved_player_ids: List[int] = []
    resolved: List[Dict[str, Any]] = []
    for row in rows:
        key = _norm_player_name(row.get("player_name"))
        candidates = by_name.get(key, [])
        home = _canonical_team_code(row.get("home_team_code"))
        away = _canonical_team_code(row.get("away_team_code"))
        probable = probable_lookup.get((key, home, away))
        if probable is None:
            probable = _find_probable_by_name_alias_game(
                player_name=row.get("player_name"),
                home_team=home,
                away_team=away,
                probable_rows=probable_rows or [],
            )
        if not candidates:
            item = dict(row)
            if probable is not None:
                item.update(
                    {
                        "game_id": probable.get("game_id"),
                        "player_id": _as_int(probable.get("player_id")),
                        "raw_provider_player_name": str(row.get("player_name") or "").strip(),
                        "player_name": str(probable.get("player_name") or row.get("player_name") or "").strip(),
                        "pitcher_team": _canonical_team_code(probable.get("pitcher_team")),
                        "offense_team": _canonical_team_code(probable.get("offense_team")),
                        "resolve_status": "resolved_by_probable_starter",
                    }
                )
                if _as_int(item.get("player_id")) is not None:
                    resolved_player_ids.append(int(item["player_id"]))
            else:
                item.update({"player_id": None, "pitcher_team": "", "offense_team": "", "resolve_status": "unresolved_player_name"})
            resolved.append(item)
            continue
        if len(candidates) > 1:
            filtered = []
            for cand in candidates:
                cand_team = _canonical_team_code(cand.get("team_id") or cand.get("team"))
                if cand_team in {home, away}:
                    filtered.append(cand)
            candidates = filtered or candidates
        if len(candidates) > 1 and probable is not None and _as_int(probable.get("player_id")) is not None:
            probable_id = int(probable.get("player_id"))
            id_filtered = [cand for cand in candidates if _as_int(cand.get("player_id")) == probable_id]
            if id_filtered:
                candidates = id_filtered
        if len(candidates) > 1:
            item = dict(row)
            item.update({"player_id": None, "pitcher_team": "", "offense_team": "", "resolve_status": "ambiguous_player_name"})
            resolved.append(item)
            continue
        cand = candidates[0]
        player_id = _as_int(cand.get("player_id"))
        pitcher_team = _canonical_team_code(cand.get("team_id") or cand.get("team"))
        resolve_status = "resolved"
        if probable is not None and player_id == _as_int(probable.get("player_id")):
            pitcher_team = _canonical_team_code(probable.get("pitcher_team")) or pitcher_team
            resolve_status = "resolved_by_probable_starter" if len(by_name.get(key, [])) > 1 else "resolved"
        offense_team = away if pitcher_team == home else home if pitcher_team == away else ""
        if probable is not None:
            offense_team = _canonical_team_code(probable.get("offense_team")) or offense_team
        item = dict(row)
        item.update(
            {
                "game_id": probable.get("game_id") if probable is not None else None,
                "player_id": player_id,
                "pitcher_team": pitcher_team,
                "offense_team": offense_team,
                "resolve_status": resolve_status,
            }
        )
        if player_id is not None:
            resolved_player_ids.append(int(player_id))
        resolved.append(item)

    starts = _fetch_prior_starter_counts(resolved_player_ids, slate_date)
    for row in resolved:
        player_id = _as_int(row.get("player_id"))
        row["prior_starter_games"] = starts.get(int(player_id), 0) if player_id is not None else None
    return resolved


def _append_odds_pitcher_coverage_rows(
    rows: List[Dict[str, Any]],
    *,
    odds_snapshot: Path,
    slate_date: str,
    starter_baseline_min_starts: int,
    team_form: Dict[str, Dict[str, Any]],
    bullpen_form: Dict[str, Dict[str, Any]],
) -> None:
    odds_rows = _resolve_odds_pitcher_rows(
        _load_odds_pitcher_hits_allowed_rows(odds_snapshot, slate_date),
        slate_date,
    )
    if not odds_rows:
        return
    existing_player_ids = {
        int(r.get("player_id"))
        for r in rows
        if _as_int(r.get("player_id")) is not None
    }
    existing_names = {
        (_norm_player_name(r.get("player_name")), _canonical_team_code(r.get("pitcher_team")))
        for r in rows
    }
    for odds in odds_rows:
        player_id = _as_int(odds.get("player_id"))
        pitcher_team = _canonical_team_code(odds.get("pitcher_team"))
        player_key = (_norm_player_name(odds.get("player_name")), pitcher_team)
        if (player_id is not None and int(player_id) in existing_player_ids) or player_key in existing_names:
            continue
        resolve_status = str(odds.get("resolve_status") or "")
        prior_starts = _as_int(odds.get("prior_starter_games"))
        if resolve_status != "resolved":
            forecast_note = resolve_status
        elif prior_starts is not None and prior_starts < int(starter_baseline_min_starts):
            forecast_note = "insufficient_pitcher_history"
        else:
            forecast_note = "present_in_odds_but_missing_from_slate_output"
        offense_team = _canonical_team_code(odds.get("offense_team"))
        form = team_form.get(offense_team, {})
        bullpen = bullpen_form.get(pitcher_team, {})
        rows.append(
            {
                "slate_date": slate_date,
                "game_date": slate_date,
                "game_id": None,
                "player_id": player_id,
                "player_name": str(odds.get("player_name") or "").strip(),
                "prop_type": "hits_allowed",
                "line": str(odds.get("line") or "").strip(),
                "model_pick_side": "",
                "model_pick_prob": None,
                "pitcher_team": pitcher_team,
                "offense_team": offense_team,
                "offense_hits_pg_last7": form.get("hits_pg_last7"),
                "offense_hits_pg_last15": form.get("hits_pg_last15"),
                "offense_hits_pg_last30": form.get("hits_pg_last30"),
                "offense_hits_samples_last7": form.get("n7"),
                "offense_hits_samples_last15": form.get("n15"),
                "offense_hits_samples_last30": form.get("n30"),
                "offense_hits_form_blended": None,
                "league_offense_hits_pg_last7": None,
                "league_offense_hits_pg_last15": None,
                "league_offense_hits_pg_last30": None,
                "league_offense_hits_form_blended": None,
                "bullpen_hits_allowed_pg_last7": bullpen.get("bullpen_hits_allowed_pg_last7"),
                "bullpen_hits_allowed_pg_last15": bullpen.get("bullpen_hits_allowed_pg_last15"),
                "bullpen_hits_allowed_pg_last30": bullpen.get("bullpen_hits_allowed_pg_last30"),
                "bullpen_hits_allowed_samples_last7": bullpen.get("n7"),
                "bullpen_hits_allowed_samples_last15": bullpen.get("n15"),
                "bullpen_hits_allowed_samples_last30": bullpen.get("n30"),
                "bullpen_hits_allowed_form_blended": None,
                "league_bullpen_hits_allowed_pg_last7": None,
                "league_bullpen_hits_allowed_pg_last15": None,
                "league_bullpen_hits_allowed_pg_last30": None,
                "league_bullpen_hits_allowed_form_blended": None,
                "offense_factor_vs_league": None,
                "offense_factor_vs_league_clamped": None,
                "pitcher_baseline_total_starts": prior_starts,
                "pitcher_baseline_seasons_used": None,
                "pitcher_expected_hits_allowed_weighted": None,
                "expected_hits_allowed_matchup": None,
                "expected_team_hits_allowed_matchup": None,
                "expected_hits_allowed_delta_vs_pitcher_baseline": None,
                "line_minus_expected_hits_allowed_matchup": None,
                "forecast_status": "unavailable",
                "forecast_note": forecast_note,
                "odds_market_present": True,
                "prior_starter_games": prior_starts,
                "odds_books_seen": _as_int(odds.get("odds_books_seen")),
            }
        )


def _build_slate_hits_allowed_rows(
    *,
    slate_csv: Path,
    wide_csv: Path,
    odds_snapshot: Path,
    slate_date: str,
    team_form: Dict[str, Dict[str, Any]],
    bullpen_form: Dict[str, Dict[str, Any]],
    starter_baseline_by_player: Dict[int, Dict[str, Any]],
    starter_baseline_min_starts: int,
    offense_weight_last7: float,
    offense_weight_last15: float,
    offense_weight_last30: float,
    offense_factor_min: float,
    offense_factor_max: float,
) -> List[Dict[str, Any]]:
    if not slate_csv.exists():
        return []
    wide_ctx = _load_wide_context(wide_csv, slate_date)
    wide_pitcher_ctx = _load_wide_pitcher_context(wide_csv, slate_date)
    league_offense_hits_pg_last7 = _blend_weighted(
        [
            (f.get("hits_pg_last7"), 1.0)
            for f in team_form.values()
            if f.get("hits_pg_last7") is not None
        ]
    )
    league_offense_hits_pg_last15 = _blend_weighted(
        [
            (f.get("hits_pg_last15"), 1.0)
            for f in team_form.values()
            if f.get("hits_pg_last15") is not None
        ]
    )
    league_offense_hits_pg_last30 = _blend_weighted(
        [
            (f.get("hits_pg_last30"), 1.0)
            for f in team_form.values()
            if f.get("hits_pg_last30") is not None
        ]
    )
    league_offense_hits_form_blended = _blend_weighted(
        [
            (league_offense_hits_pg_last7, offense_weight_last7),
            (league_offense_hits_pg_last15, offense_weight_last15),
            (league_offense_hits_pg_last30, offense_weight_last30),
        ]
    )
    league_bullpen_hits_allowed_pg_last7 = _blend_weighted(
        [
            (f.get("bullpen_hits_allowed_pg_last7"), 1.0)
            for f in bullpen_form.values()
            if f.get("bullpen_hits_allowed_pg_last7") is not None
        ]
    )
    league_bullpen_hits_allowed_pg_last15 = _blend_weighted(
        [
            (f.get("bullpen_hits_allowed_pg_last15"), 1.0)
            for f in bullpen_form.values()
            if f.get("bullpen_hits_allowed_pg_last15") is not None
        ]
    )
    league_bullpen_hits_allowed_pg_last30 = _blend_weighted(
        [
            (f.get("bullpen_hits_allowed_pg_last30"), 1.0)
            for f in bullpen_form.values()
            if f.get("bullpen_hits_allowed_pg_last30") is not None
        ]
    )
    league_bullpen_hits_allowed_form_blended = _blend_weighted(
        [
            (league_bullpen_hits_allowed_pg_last7, offense_weight_last7),
            (league_bullpen_hits_allowed_pg_last15, offense_weight_last15),
            (league_bullpen_hits_allowed_pg_last30, offense_weight_last30),
        ]
    )

    probable_rows, _probable_meta = _load_probable_starter_rows(slate_date)
    probable_player_ids = [
        int(row["player_id"])
        for row in probable_rows
        if _as_int(row.get("player_id")) is not None
    ]
    probable_starts = _fetch_prior_starter_counts(probable_player_ids, slate_date)

    def make_context_row(
        *,
        game_date: str,
        game_id: Optional[int],
        player_id: Optional[int],
        player_name: str,
        line: Any,
        model_pick_side: str = "",
        model_pick_prob: Any = None,
        pitcher_team: str,
        offense_team: str,
        forecast_source: str,
        forecast_note: str = "",
        odds_market_present: Any = "",
        odds_books_seen: Any = "",
        hits_allowed_market_present: bool = False,
        probable_starter_context_present: bool = False,
        prior_starter_games: Any = None,
        raw_provider_player_name: Any = "",
    ) -> Dict[str, Any]:
        pitcher_team = _canonical_team_code(pitcher_team)
        offense_team = _canonical_team_code(offense_team)
        form = team_form.get(offense_team, {})
        bullpen = bullpen_form.get(pitcher_team, {})
        offense_hits_pg_last7 = form.get("hits_pg_last7")
        offense_hits_pg_last15 = form.get("hits_pg_last15")
        offense_hits_pg_last30 = form.get("hits_pg_last30")
        offense_hits_form_blended = _blend_weighted(
            [
                (offense_hits_pg_last7, offense_weight_last7),
                (offense_hits_pg_last15, offense_weight_last15),
                (offense_hits_pg_last30, offense_weight_last30),
            ]
        )
        offense_factor_vs_league = None
        if (
            offense_hits_form_blended is not None
            and league_offense_hits_form_blended is not None
            and league_offense_hits_form_blended > 0
        ):
            offense_factor_vs_league = offense_hits_form_blended / league_offense_hits_form_blended
        offense_factor_clamped = _clamp(
            offense_factor_vs_league,
            float(offense_factor_min),
            float(offense_factor_max),
        )
        starter_baseline = starter_baseline_by_player.get(int(player_id), {}) if player_id is not None else {}
        prior_starts = starter_baseline.get("total_starts")
        if prior_starts is None:
            prior_starts = _as_int(prior_starter_games)
        if prior_starts is None and player_id is not None:
            prior_starts = probable_starts.get(int(player_id))
        pitcher_expected_hits_allowed_weighted = _as_float(
            starter_baseline.get("expected_hits_allowed_weighted")
        )
        expected_hits_allowed_matchup = None
        expected_hits_allowed_delta_vs_pitcher_baseline = None
        if (
            pitcher_expected_hits_allowed_weighted is not None
            and offense_factor_clamped is not None
        ):
            expected_hits_allowed_matchup = pitcher_expected_hits_allowed_weighted * offense_factor_clamped
            expected_hits_allowed_delta_vs_pitcher_baseline = (
                expected_hits_allowed_matchup - pitcher_expected_hits_allowed_weighted
            )
        bullpen_hits_allowed_pg_last7 = bullpen.get("bullpen_hits_allowed_pg_last7")
        bullpen_hits_allowed_pg_last15 = bullpen.get("bullpen_hits_allowed_pg_last15")
        bullpen_hits_allowed_pg_last30 = bullpen.get("bullpen_hits_allowed_pg_last30")
        bullpen_hits_allowed_form_blended = _blend_weighted(
            [
                (bullpen_hits_allowed_pg_last7, offense_weight_last7),
                (bullpen_hits_allowed_pg_last15, offense_weight_last15),
                (bullpen_hits_allowed_pg_last30, offense_weight_last30),
            ]
        )
        expected_team_hits_allowed_matchup = None
        if (
            expected_hits_allowed_matchup is not None
            and bullpen_hits_allowed_form_blended is not None
        ):
            expected_team_hits_allowed_matchup = (
                expected_hits_allowed_matchup + bullpen_hits_allowed_form_blended
            )
        line_float = _as_float(line)
        line_minus_expected_hits_allowed_matchup = None
        if line_float is not None and expected_hits_allowed_matchup is not None:
            line_minus_expected_hits_allowed_matchup = line_float - expected_hits_allowed_matchup
        note = str(forecast_note or "").strip()
        status = "available" if expected_hits_allowed_matchup is not None else "unavailable"
        if status == "unavailable":
            if prior_starts is not None and int(prior_starts) < int(starter_baseline_min_starts):
                note = "insufficient_pitcher_history"
            elif not note:
                if not hits_allowed_market_present:
                    note = "no_hits_allowed_market_context_only"
                elif probable_starter_context_present:
                    note = "probable_starter_market_missing_source_stats"
                else:
                    note = "odds_market_without_probable_starter_match"
        return {
            "slate_date": slate_date,
            "game_date": game_date,
            "game_id": game_id,
            "player_id": player_id,
            "player_name": str(player_name or "").strip(),
            "raw_provider_player_name": str(raw_provider_player_name or "").strip(),
            "prop_type": "hits_allowed",
            "line": line_float if line_float is not None else str(line or "").strip(),
            "model_pick_side": str(model_pick_side or "").strip().lower(),
            "model_pick_prob": _as_float(model_pick_prob),
            "pitcher_team": pitcher_team,
            "offense_team": offense_team,
            "offense_hits_pg_last7": offense_hits_pg_last7,
            "offense_hits_pg_last15": offense_hits_pg_last15,
            "offense_hits_pg_last30": offense_hits_pg_last30,
            "offense_hits_samples_last7": form.get("n7"),
            "offense_hits_samples_last15": form.get("n15"),
            "offense_hits_samples_last30": form.get("n30"),
            "offense_hits_form_blended": offense_hits_form_blended,
            "league_offense_hits_pg_last7": league_offense_hits_pg_last7,
            "league_offense_hits_pg_last15": league_offense_hits_pg_last15,
            "league_offense_hits_pg_last30": league_offense_hits_pg_last30,
            "league_offense_hits_form_blended": league_offense_hits_form_blended,
            "bullpen_hits_allowed_pg_last7": bullpen_hits_allowed_pg_last7,
            "bullpen_hits_allowed_pg_last15": bullpen_hits_allowed_pg_last15,
            "bullpen_hits_allowed_pg_last30": bullpen_hits_allowed_pg_last30,
            "bullpen_hits_allowed_samples_last7": bullpen.get("n7"),
            "bullpen_hits_allowed_samples_last15": bullpen.get("n15"),
            "bullpen_hits_allowed_samples_last30": bullpen.get("n30"),
            "bullpen_hits_allowed_form_blended": bullpen_hits_allowed_form_blended,
            "league_bullpen_hits_allowed_pg_last7": league_bullpen_hits_allowed_pg_last7,
            "league_bullpen_hits_allowed_pg_last15": league_bullpen_hits_allowed_pg_last15,
            "league_bullpen_hits_allowed_pg_last30": league_bullpen_hits_allowed_pg_last30,
            "league_bullpen_hits_allowed_form_blended": league_bullpen_hits_allowed_form_blended,
            "offense_factor_vs_league": offense_factor_vs_league,
            "offense_factor_vs_league_clamped": offense_factor_clamped,
            "pitcher_baseline_total_starts": starter_baseline.get("total_starts"),
            "pitcher_baseline_seasons_used": starter_baseline.get("seasons_used"),
            "pitcher_expected_hits_allowed_weighted": pitcher_expected_hits_allowed_weighted,
            "expected_hits_allowed_matchup": expected_hits_allowed_matchup,
            "expected_team_hits_allowed_matchup": expected_team_hits_allowed_matchup,
            "expected_hits_allowed_delta_vs_pitcher_baseline": expected_hits_allowed_delta_vs_pitcher_baseline,
            "line_minus_expected_hits_allowed_matchup": line_minus_expected_hits_allowed_matchup,
            "forecast_status": status,
            "forecast_note": note,
            "forecast_source": forecast_source,
            "odds_market_present": bool(odds_market_present) if isinstance(odds_market_present, bool) else odds_market_present,
            "hits_allowed_market_present": bool(hits_allowed_market_present),
            "trusted_forecast": bool(expected_hits_allowed_matchup is not None),
            "probable_starter_context_present": bool(probable_starter_context_present),
            "prior_starter_games": prior_starts,
            "odds_books_seen": _as_int(odds_books_seen),
        }

    out: List[Dict[str, Any]] = []
    with slate_csv.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            prop_type = str(row.get("prop_type") or "").strip().lower()
            if prop_type != "hits_allowed":
                continue
            game_date = str(row.get("game_date") or "").strip()
            if slate_date and game_date != slate_date:
                continue
            game_id = _as_int(row.get("game_id"))
            player_id = _as_int(row.get("player_id"))
            if game_id is None or player_id is None:
                continue
            wide = wide_ctx.get((game_id, player_id, prop_type), {})
            offense_team = _canonical_team_code(wide.get("opponent"))
            pitcher_team = _canonical_team_code(wide.get("team"))
            if not offense_team:
                offense_team = _canonical_team_code(wide.get("opponent_id"))
            if not pitcher_team:
                pitcher_team = _canonical_team_code(wide.get("team_id"))
            out.append(
                make_context_row(
                    game_date=game_date,
                    game_id=game_id,
                    player_id=player_id,
                    player_name=str(row.get("player_name") or "").strip(),
                    line=row.get("line"),
                    model_pick_side=str(row.get("model_pick_side") or "").strip().lower(),
                    model_pick_prob=row.get("model_pick_prob"),
                    pitcher_team=pitcher_team,
                    offense_team=offense_team,
                    forecast_source="slate_hits_allowed_market",
                    odds_market_present=True,
                    hits_allowed_market_present=True,
                )
            )

    existing_player_ids = {
        int(r.get("player_id"))
        for r in out
        if _as_int(r.get("player_id")) is not None
    }
    existing_pairs = {
        (_canonical_team_code(r.get("pitcher_team")), _canonical_team_code(r.get("offense_team")))
        for r in out
        if _canonical_team_code(r.get("pitcher_team")) and _canonical_team_code(r.get("offense_team"))
    }

    odds_rows = _resolve_odds_pitcher_rows(
        _load_odds_pitcher_hits_allowed_rows(odds_snapshot, slate_date),
        slate_date,
        probable_rows=probable_rows,
    )
    for odds in odds_rows:
        player_id = _as_int(odds.get("player_id"))
        pitcher_team = _canonical_team_code(odds.get("pitcher_team"))
        offense_team = _canonical_team_code(odds.get("offense_team"))
        pair_key = (pitcher_team, offense_team)
        if (
            (player_id is not None and int(player_id) in existing_player_ids)
            or (pitcher_team and offense_team and pair_key in existing_pairs)
        ):
            for existing in out:
                if player_id is not None and _as_int(existing.get("player_id")) == player_id:
                    existing["hits_allowed_market_present"] = True
                    existing["odds_market_present"] = True
                    existing["odds_books_seen"] = _as_int(odds.get("odds_books_seen"))
                    if not existing.get("line"):
                        existing["line"] = str(odds.get("line") or "").strip()
                    break
            continue
        out.append(
            make_context_row(
                game_date=slate_date,
                game_id=_as_int(odds.get("game_id")),
                player_id=player_id,
                player_name=str(odds.get("player_name") or "").strip(),
                line=str(odds.get("line") or "").strip(),
                pitcher_team=pitcher_team,
                offense_team=offense_team,
                forecast_source="odds_pitcher_hits_allowed_market",
                forecast_note="" if str(odds.get("resolve_status") or "") in {"resolved", "resolved_by_probable_starter"} else str(odds.get("resolve_status") or ""),
                odds_market_present=True,
                odds_books_seen=_as_int(odds.get("odds_books_seen")),
                hits_allowed_market_present=True,
                probable_starter_context_present=str(odds.get("resolve_status") or "") == "resolved_by_probable_starter",
                prior_starter_games=_as_int(odds.get("prior_starter_games")),
                raw_provider_player_name=str(odds.get("raw_provider_player_name") or "").strip(),
            )
        )
        if player_id is not None:
            existing_player_ids.add(int(player_id))
        if pitcher_team and offense_team:
            existing_pairs.add(pair_key)

    for probable in probable_rows:
        player_id = _as_int(probable.get("player_id"))
        pitcher_team = _canonical_team_code(probable.get("pitcher_team"))
        offense_team = _canonical_team_code(probable.get("offense_team"))
        pair_key = (pitcher_team, offense_team)
        matched_existing = False
        for existing in out:
            if (
                (player_id is not None and _as_int(existing.get("player_id")) == player_id)
                or (
                    pitcher_team
                    and offense_team
                    and _canonical_team_code(existing.get("pitcher_team")) == pitcher_team
                    and _canonical_team_code(existing.get("offense_team")) == offense_team
                )
            ):
                existing["probable_starter_context_present"] = True
                if not existing.get("game_id"):
                    existing["game_id"] = _as_int(probable.get("game_id"))
                if not existing.get("pitcher_team"):
                    existing["pitcher_team"] = pitcher_team
                if not existing.get("offense_team"):
                    existing["offense_team"] = offense_team
                matched_existing = True
                break
        if matched_existing:
            continue

        # Add same-date probable starters that had no hits_allowed market row.
        # This is context visibility only unless the existing baseline/source-stat
        # policy can compute expected_hits_allowed_matchup.
        out.append(
            make_context_row(
                game_date=slate_date,
                game_id=_as_int(probable.get("game_id")),
                player_id=player_id,
                player_name=str(probable.get("player_name") or "").strip(),
                line="",
                pitcher_team=pitcher_team,
                offense_team=offense_team,
                forecast_source="probable_starter_context",
                forecast_note="no_hits_allowed_market_context_only",
                odds_market_present=False,
                odds_books_seen="",
                hits_allowed_market_present=False,
                probable_starter_context_present=True,
            )
        )
        if player_id is not None:
            existing_player_ids.add(int(player_id))
        if pitcher_team and offense_team:
            existing_pairs.add(pair_key)

    return out


IDENTITY_FIELDNAMES = [
    "canonical_player_id",
    "canonical_game_id",
    "canonical_team",
    "canonical_opponent",
    "canonical_market_key",
    "canonical_game_key",
    "identity_status",
    "identity_method",
    "fallback_used",
    "identity_warning",
    "identity_confidence",
    "forecast_diagnostic",
]


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "y", "market", "available"}


def _build_hits_environment_player_resolver(rows: Sequence[Dict[str, Any]]) -> PlayerIdentityResolver:
    refs: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str]] = set()
    for row in rows:
        player_id = _as_int(row.get("player_id"))
        player_name = str(row.get("player_name") or "").strip()
        team = _identity_team_code(row.get("pitcher_team"))
        if player_id is None or not player_name:
            continue
        key = (str(player_id), team)
        if key in seen:
            continue
        seen.add(key)
        refs.append(
            {
                "player_id": player_id,
                "player_name": player_name,
                "team": team,
            }
        )
    return PlayerIdentityResolver(refs)


def _forecast_diagnostic(row: Dict[str, Any]) -> str:
    identity_status = str(row.get("identity_status") or "").strip()
    note = str(row.get("forecast_note") or "").strip()
    status = str(row.get("forecast_status") or "").strip()
    source = str(row.get("forecast_source") or "").strip()
    has_market = _truthy(row.get("hits_allowed_market_present")) or _truthy(row.get("odds_market_present"))
    if identity_status == "ambiguous":
        return "ambiguous_identity"
    if identity_status == "unresolved":
        return "unresolved_identity"
    if note == "insufficient_pitcher_history":
        return "resolved_identity_insufficient_history"
    if note in {"no_hits_allowed_market", "no_hits_allowed_market_context_only"}:
        return "resolved_identity_no_market"
    if source == "probable_starter_context" and not has_market:
        return "context_only_forecast"
    if note == "present_in_odds_but_missing_from_slate_output":
        return "provider_market_no_probable_match"
    if note == "odds_market_without_probable_starter_match":
        return "provider_market_no_probable_match"
    if status == "unavailable" and not has_market:
        return "context_only_forecast"
    if status == "available":
        return "trusted_forecast"
    return note or "unknown"


def _combine_identity_status(
    player_status: str,
    game_status: str,
    market_status: str,
    market_required: bool,
) -> str:
    statuses = [player_status, game_status]
    if market_required:
        statuses.append(market_status)
    if any(status == "ambiguous" for status in statuses):
        return "ambiguous"
    if any(status == "unresolved" for status in statuses):
        return "unresolved"
    if any(status in {"resolved_by_name_fallback", "resolved_by_game", "resolved_by_provider_id"} for status in statuses):
        return "fallback_identity"
    return "resolved_by_id"


def _apply_hits_environment_identity(rows: List[Dict[str, Any]], slate_date: str) -> Dict[str, Any]:
    player_resolver = _build_hits_environment_player_resolver(rows)
    game_resolver = GameIdentityResolver()
    for row in rows:
        team = _identity_team_code(row.get("pitcher_team"))
        opponent = _identity_team_code(row.get("offense_team"))
        row["canonical_team"] = team
        row["canonical_opponent"] = opponent

        player = player_resolver.resolve(
            PlayerIdentityInput(
                player_id=row.get("player_id"),
                player_name=str(row.get("player_name") or "").strip(),
                team=team,
                opponent=opponent,
                game_id=row.get("game_id"),
            )
        )
        game = game_resolver.resolve(
            GameIdentityInput(
                date=slate_date,
                game_id=row.get("game_id"),
                team=team,
                opponent=opponent,
            )
        )
        market_required = _truthy(row.get("hits_allowed_market_present")) or _truthy(row.get("odds_market_present"))
        market = resolve_market_identity(
            MarketIdentityInput(
                date=slate_date,
                game_id=game.canonical_game_id or row.get("game_id"),
                player_id=player.canonical_player_id or row.get("player_id"),
                player_name=str(row.get("player_name") or "").strip(),
                team=team,
                opponent=opponent,
                prop_type="hits_allowed",
                side="market",
                line=row.get("line"),
            )
        )
        row["canonical_player_id"] = player.canonical_player_id
        row["canonical_game_id"] = game.canonical_game_id
        row["canonical_game_key"] = game.canonical_game_key
        row["canonical_market_key"] = (market.canonical_market_key or market.fallback_market_key) if market_required else ""
        row["identity_status"] = _combine_identity_status(
            player.identity_status,
            game.identity_status,
            market.identity_status,
            market_required,
        )
        methods = [player.identity_method, game.identity_method]
        if market_required:
            methods.append(market.identity_method)
        row["identity_method"] = "+".join([m for m in methods if m])
        row["fallback_used"] = bool(player.fallback_used or game.fallback_used or (market_required and market.fallback_used))
        warnings = [
            reason
            for reason in (
                player.ambiguity_reason,
                game.ambiguity_reason,
                market.ambiguity_reason if market_required else "",
            )
            if reason
        ]
        row["identity_warning"] = ";".join(warnings)
        confidences = [player.identity_confidence, game.identity_confidence]
        if market_required:
            confidences.append(market.identity_confidence)
        row["identity_confidence"] = min(confidences) if confidences else 0.0
        row["forecast_diagnostic"] = _forecast_diagnostic(row)
        if str(row.get("forecast_note") or "") == "present_in_odds_but_missing_from_slate_output":
            row["forecast_note"] = row["forecast_diagnostic"]

    return _hits_environment_identity_health(rows)


def _pct(num: int, den: int) -> float:
    return round((float(num) / float(den) * 100.0), 2) if den else 0.0


def _hits_environment_identity_health(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(rows)
    market_rows = [r for r in rows if _truthy(r.get("hits_allowed_market_present")) or _truthy(r.get("odds_market_present"))]
    status_counts: Dict[str, int] = {}
    for row in rows:
        status = str(row.get("identity_status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    blank_team = len([r for r in rows if not str(r.get("canonical_team") or r.get("pitcher_team") or "").strip()])
    blank_opponent = len([r for r in rows if not str(r.get("canonical_opponent") or r.get("offense_team") or "").strip()])
    blank_starter = len([r for r in rows if not str(r.get("player_name") or "").strip()])
    blank_game_id = len([r for r in rows if not str(r.get("canonical_game_id") or r.get("game_id") or "").strip()])
    player_id_rows = len([r for r in rows if str(r.get("canonical_player_id") or "").strip()])
    game_id_rows = len([r for r in rows if str(r.get("canonical_game_id") or "").strip()])
    market_key_rows = len([r for r in market_rows if str(r.get("canonical_market_key") or "").strip()])
    context_only_rows = len([r for r in rows if not (_truthy(r.get("hits_allowed_market_present")) or _truthy(r.get("odds_market_present")))])
    trusted_forecast_rows = len([r for r in rows if _truthy(r.get("trusted_forecast"))])
    likely_duplicate_alias_rows = _likely_duplicate_identity_alias_rows(rows)
    player_pct = _pct(player_id_rows, total)
    game_pct = _pct(game_id_rows, total)
    market_pct = _pct(market_key_rows, len(market_rows))
    warnings = []
    if player_pct < 95:
        warnings.append("player_id_coverage_below_95")
    if game_pct < 95:
        warnings.append("game_id_coverage_below_95")
    if blank_team:
        warnings.append("blank_teams_present")
    if blank_opponent:
        warnings.append("blank_opponents_present")
    if blank_starter:
        warnings.append("blank_starters_present")
    if blank_game_id:
        warnings.append("blank_game_ids_present")
    if likely_duplicate_alias_rows:
        warnings.append("likely_duplicate_identity_alias_rows_present")
    return {
        "rows": total,
        "player_id_rows": player_id_rows,
        "player_id_coverage_pct": player_pct,
        "game_id_rows": game_id_rows,
        "game_id_coverage_pct": game_pct,
        "market_rows": len(market_rows),
        "market_key_rows": market_key_rows,
        "market_key_coverage_pct": market_pct,
        "resolved_identity_rows": status_counts.get("resolved_by_id", 0),
        "fallback_identity_rows": status_counts.get("fallback_identity", 0),
        "ambiguous_identity_rows": status_counts.get("ambiguous", 0),
        "unresolved_identity_rows": status_counts.get("unresolved", 0),
        "context_only_rows": context_only_rows,
        "trusted_forecast_rows": trusted_forecast_rows,
        "likely_duplicate_identity_alias_rows": len(likely_duplicate_alias_rows),
        "blank_team_rows": blank_team,
        "blank_opponent_rows": blank_opponent,
        "blank_starter_rows": blank_starter,
        "blank_game_id_rows": blank_game_id,
        "status": "warn" if warnings else "pass",
        "warnings": warnings,
        "identity_status_counts": status_counts,
    }


def _likely_duplicate_identity_alias_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    resolved = [
        row
        for row in rows
        if str(row.get("canonical_player_id") or row.get("player_id") or "").strip()
        and str(row.get("player_name") or "").strip()
    ]
    unresolved = [
        row
        for row in rows
        if not str(row.get("canonical_player_id") or row.get("player_id") or "").strip()
        and str(row.get("player_name") or "").strip()
        and (
            "unresolved" in str(row.get("identity_status") or row.get("forecast_diagnostic") or "").lower()
            or "ambiguous" in str(row.get("identity_status") or row.get("forecast_diagnostic") or "").lower()
        )
    ]
    out: List[Dict[str, Any]] = []
    for bad in unresolved:
        bad_date = str(bad.get("slate_date") or bad.get("game_date") or "").strip()
        for good in resolved:
            good_date = str(good.get("slate_date") or good.get("game_date") or "").strip()
            if bad_date and good_date and bad_date != good_date:
                continue
            if not _probable_name_alias_compatible(bad.get("player_name"), good.get("player_name")):
                continue
            out.append(
                {
                    "unresolved_player_name": bad.get("player_name"),
                    "resolved_player_name": good.get("player_name"),
                    "resolved_player_id": good.get("canonical_player_id") or good.get("player_id"),
                    "resolved_game_id": good.get("canonical_game_id") or good.get("game_id"),
                    "line": bad.get("line"),
                    "books": bad.get("odds_books_seen"),
                    "unresolved_status": bad.get("identity_status") or bad.get("forecast_diagnostic"),
                }
            )
            break
    return out


def _write_hits_environment_identity_artifacts(
    *,
    rows: Sequence[Dict[str, Any]],
    slate_date: str,
    generated_at_utc: str,
    out_dir: Path = Path("artifacts/analysis/mlb/identity"),
) -> Dict[str, str]:
    health = _hits_environment_identity_health(rows)
    out_dir.mkdir(parents=True, exist_ok=True)
    health_csv = out_dir / "hits_environment_identity_health.csv"
    health_md = out_dir / "hits_environment_identity_health.md"
    examples_csv = out_dir / "hits_environment_identity_examples.csv"
    migration_md = out_dir / "hits_environment_identity_migration.md"

    health_row = {
        "generated_at_utc": generated_at_utc,
        "slate_date": slate_date,
        **{k: v for k, v in health.items() if k not in {"warnings", "identity_status_counts"}},
        "warnings": ";".join(health.get("warnings") or []),
        "identity_status_counts_json": json.dumps(health.get("identity_status_counts") or {}, sort_keys=True),
    }
    _write_generic_csv(health_csv, [health_row])

    example_names = {"jared jones", "chase burns", "alan rangel", "sam aldegheri", "samuel aldegheri"}
    examples = [
        {
            "slate_date": slate_date,
            "player_name": row.get("player_name"),
            "player_id": row.get("player_id"),
            "canonical_player_id": row.get("canonical_player_id"),
            "game_id": row.get("game_id"),
            "canonical_game_id": row.get("canonical_game_id"),
            "pitcher_team": row.get("pitcher_team"),
            "offense_team": row.get("offense_team"),
            "canonical_team": row.get("canonical_team"),
            "canonical_opponent": row.get("canonical_opponent"),
            "canonical_market_key": row.get("canonical_market_key"),
            "identity_status": row.get("identity_status"),
            "identity_method": row.get("identity_method"),
            "fallback_used": row.get("fallback_used"),
            "identity_warning": row.get("identity_warning"),
            "identity_confidence": row.get("identity_confidence"),
            "forecast_status": row.get("forecast_status"),
            "forecast_note": row.get("forecast_note"),
            "forecast_diagnostic": row.get("forecast_diagnostic"),
            "forecast_source": row.get("forecast_source"),
            "hits_allowed_market_present": row.get("hits_allowed_market_present"),
            "probable_starter_context_present": row.get("probable_starter_context_present"),
        }
        for row in rows
        if _norm_player_name(row.get("player_name")) in example_names
    ]
    _write_generic_csv(examples_csv, examples)

    lines = [
        "# Hits Environment Identity Health",
        "",
        f"- Generated UTC: `{generated_at_utc}`",
        f"- Slate date: `{slate_date}`",
        f"- Status: `{health['status']}`",
        "- Scope: identity/provenance only; forecast behavior is unchanged.",
        "",
        "## Coverage",
        "",
        f"- Rows: `{health['rows']}`",
        f"- Canonical player ID coverage: `{health['player_id_rows']}/{health['rows']}` = `{health['player_id_coverage_pct']}%`",
        f"- Canonical game ID coverage: `{health['game_id_rows']}/{health['rows']}` = `{health['game_id_coverage_pct']}%`",
        f"- Market key coverage on market rows: `{health['market_key_rows']}/{health['market_rows']}` = `{health['market_key_coverage_pct']}%`",
        f"- Fallback identity rows: `{health['fallback_identity_rows']}`",
        f"- Ambiguous identity rows: `{health['ambiguous_identity_rows']}`",
        f"- Unresolved identity rows: `{health['unresolved_identity_rows']}`",
        f"- Context-only rows: `{health['context_only_rows']}`",
        f"- Trusted forecast rows: `{health['trusted_forecast_rows']}`",
        f"- Likely duplicate alias rows: `{health['likely_duplicate_identity_alias_rows']}`",
        "",
        "## Blank Context Gates",
        "",
        f"- Blank teams: `{health['blank_team_rows']}`",
        f"- Blank opponents: `{health['blank_opponent_rows']}`",
        f"- Blank starters: `{health['blank_starter_rows']}`",
        f"- Blank game IDs: `{health['blank_game_id_rows']}`",
        "",
        "## Diagnostics",
        "",
        "| diagnostic | rows |",
        "|---|---:|",
    ]
    diag_counts: Dict[str, int] = {}
    for row in rows:
        diag = str(row.get("forecast_diagnostic") or "unknown")
        diag_counts[diag] = diag_counts.get(diag, 0) + 1
    for key, value in sorted(diag_counts.items()):
        lines.append(f"| `{key}` | `{value}` |")
    if examples:
        lines.extend(["", "## Named Examples", "", "| player | identity | forecast diagnostic | teams | game |", "|---|---|---|---|---|"])
        for row in examples:
            lines.append(
                f"| {row['player_name']} | `{row['identity_status']}` | `{row['forecast_diagnostic']}` | `{row['canonical_team']} vs {row['canonical_opponent']}` | `{row['canonical_game_id']}` |"
            )
    health_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    migration_lines = [
        "# Hits Environment Identity Migration",
        "",
        f"- Date validated: `{slate_date}`",
        "- Migrated caller: `backend/mlb/scripts/report_mlb_hits_environment.py`.",
        "- Shared resolver package: `backend/mlb/identity/`.",
        "- Forecast behavior changed: `no`.",
        "- Candidate inclusion changed: `no`.",
        "",
        "## What Changed",
        "",
        "Hits-environment row outputs now preserve canonical player, game, team, opponent, market, identity status, identity method, fallback usage, warning, confidence, and structured forecast diagnostic fields.",
        "",
        "Identity and forecast are intentionally separate. A row can have resolved identity with an unavailable forecast, context-only forecast, or insufficient starter history. Conversely, fallback identity does not change forecast calculations.",
        "",
        "## Current Health",
        "",
        f"- Rows: `{health['rows']}`",
        f"- Canonical player ID coverage: `{health['player_id_coverage_pct']}%`",
        f"- Canonical game ID coverage: `{health['game_id_coverage_pct']}%`",
        f"- Market key coverage on market rows: `{health['market_key_coverage_pct']}%`",
        f"- Status: `{health['status']}`",
        "",
    ]
    migration_md.write_text("\n".join(migration_lines), encoding="utf-8")

    return {
        "hits_environment_identity_health_md": str(health_md),
        "hits_environment_identity_health_csv": str(health_csv),
        "hits_environment_identity_examples_csv": str(examples_csv),
        "hits_environment_identity_migration_md": str(migration_md),
    }


def _load_hits_environment_snapshot_rows(snapshot_dir: Path, slate_date: str) -> List[Dict[str, Any]]:
    date_dir = snapshot_dir / slate_date
    if not date_dir.exists():
        return []
    rows: List[Dict[str, Any]] = []
    for path in sorted(date_dir.glob(f"mlb_hits_environment_hits_allowed_rows_{slate_date}__*.csv")):
        stamp_match = re.search(r"__(.+)\.csv$", path.name)
        stamp = stamp_match.group(1) if stamp_match else ""
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    item = dict(row)
                    item["_snapshot_path"] = str(path)
                    item["_snapshot_stamp"] = stamp
                    rows.append(item)
        except Exception:
            continue
    return rows


def _fetch_actual_pitcher_usage(slate_date: str, player_ids: Sequence[Any]) -> Dict[int, Dict[str, Any]]:
    ids = sorted({int(pid) for pid in (_as_int(pid) for pid in player_ids) if pid is not None})
    if not ids:
        return {}
    rows = pg_fetchall(
        """
        SELECT player_id,
               game_id,
               game_date,
               team,
               opponent,
               COALESCE(is_starter, 0)::int AS is_starter,
               COALESCE(outs_recorded, 0)::int AS outs_recorded,
               COALESCE(hits_allowed, 0)::int AS hits_allowed,
               COALESCE(earned_runs, 0)::int AS earned_runs,
               COALESCE(strikeouts_pitching, 0)::int AS strikeouts_pitching
        FROM mlb.player_stats
        WHERE game_date = %s::date
          AND player_id = ANY(%s::bigint[])
        """,
        (slate_date, ids),
    )
    out: Dict[int, Dict[str, Any]] = {}
    for row in rows or []:
        player_id = _as_int(row.get("player_id"))
        if player_id is None:
            continue
        out[int(player_id)] = dict(row)
    return out


def _role_from_usage(row: Dict[str, Any] | None) -> str:
    if not row:
        return "unknown"
    outs = _as_int(row.get("outs_recorded")) or 0
    if outs <= 0:
        return "did_not_appear"
    if _as_int(row.get("is_starter")) == 1:
        return "actual_starter"
    return "reliever"


def _known_lifecycle_usage_observation(slate_date: str, player_id: str, player_name: str) -> Dict[str, Any]:
    if slate_date == "2026-06-27" and (player_id == "660604" or _norm_player_name(player_name) == "alan rangel"):
        return {
            "actual_usage_status": "reliever",
            "actual_usage_source": "user_reported_lifecycle_fact",
            "actual_usage_note": "Rain delay changed probable-starter role; replacement starter lasted 1.1 innings; Alan Rangel later entered as reliever.",
        }
    return {}


def _build_starter_market_lifecycle_audit(
    *,
    current_rows: Sequence[Dict[str, Any]],
    snapshot_rows: Sequence[Dict[str, Any]],
    slate_date: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    current_keys = {
        (
            str(row.get("canonical_player_id") or row.get("player_id") or "").strip(),
            str(row.get("canonical_game_id") or row.get("game_id") or "").strip(),
        )
        for row in current_rows
    }
    historical_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in snapshot_rows:
        player_id = str(row.get("canonical_player_id") or row.get("player_id") or "").strip()
        game_id = str(row.get("canonical_game_id") or row.get("game_id") or "").strip()
        if not player_id:
            continue
        historical_by_key.setdefault((player_id, game_id), []).append(row)

    player_ids = [key[0] for key in historical_by_key.keys()]
    try:
        usage_by_player = _fetch_actual_pitcher_usage(slate_date, player_ids)
    except Exception:
        usage_by_player = {}

    audit_rows: List[Dict[str, Any]] = []
    for key, history in sorted(historical_by_key.items(), key=lambda item: (item[0][1], item[0][0])):
        player_id_text, game_id_text = key
        latest = sorted(history, key=lambda row: str(row.get("_snapshot_stamp") or ""))[-1]
        earliest = sorted(history, key=lambda row: str(row.get("_snapshot_stamp") or ""))[0]
        in_current = key in current_keys or any(k[0] == player_id_text and k[1] == game_id_text for k in current_keys)
        player_id = _as_int(player_id_text)
        usage = usage_by_player.get(int(player_id)) if player_id is not None else None
        actual_usage_status = _role_from_usage(usage)
        actual_usage_source = "player_stats" if usage else ""
        known_usage = _known_lifecycle_usage_observation(slate_date, player_id_text, str(latest.get("player_name") or earliest.get("player_name") or ""))
        if known_usage and actual_usage_status == "unknown":
            actual_usage_status = str(known_usage.get("actual_usage_status") or actual_usage_status)
            actual_usage_source = str(known_usage.get("actual_usage_source") or actual_usage_source)
        had_market = any(_truthy(row.get("hits_allowed_market_present")) or _truthy(row.get("odds_market_present")) for row in history)
        had_probable = any(_truthy(row.get("probable_starter_context_present")) for row in history)
        forecast_status = str(latest.get("forecast_status") or "")
        forecast_diag = str(latest.get("forecast_diagnostic") or latest.get("forecast_note") or "")
        identity_status = str(latest.get("identity_status") or ("resolved_by_id" if player_id_text else "unresolved"))
        role_status = "probable_starter" if had_probable and in_current else "unknown"
        lifecycle_warning = ""
        if had_probable and not in_current:
            role_status = "replaced_probable"
            lifecycle_warning = "probable_starter_removed_after_market_or_context"
        if actual_usage_status == "reliever" and had_probable:
            role_status = "reliever"
            lifecycle_warning = "probable_starter_later_used_as_reliever"
        elif actual_usage_status == "actual_starter":
            role_status = "actual_starter"
        elif actual_usage_status == "did_not_appear" and had_probable:
            role_status = "did_not_appear"
            lifecycle_warning = lifecycle_warning or "probable_starter_did_not_appear"
        if lifecycle_warning or not in_current:
            audit_rows.append(
                {
                    "slate_date": slate_date,
                    "player_id": player_id_text,
                    "player_name": latest.get("player_name") or earliest.get("player_name"),
                    "game_id": game_id_text or latest.get("game_id") or earliest.get("game_id"),
                    "pitcher_team": latest.get("pitcher_team") or earliest.get("pitcher_team"),
                    "offense_team": latest.get("offense_team") or earliest.get("offense_team"),
                    "identity_status": identity_status,
                    "role_status": role_status,
                    "starter_status": "not_active_trusted_starter_forecast" if lifecycle_warning else str(latest.get("forecast_status") or ""),
                    "market_status": "prior_market_existed" if had_market and not in_current else "market_current" if had_market else "no_market",
                    "forecast_status": forecast_status,
                    "forecast_diagnostic": forecast_diag,
                    "actual_usage_status": actual_usage_status,
                    "actual_usage_source": actual_usage_source,
                    "actual_usage_note": known_usage.get("actual_usage_note", "") if known_usage else "",
                    "game_status": "role_changed_after_probable_context" if lifecycle_warning else "normal",
                    "lifecycle_warning": lifecycle_warning,
                    "line": latest.get("line") or earliest.get("line"),
                    "odds_books_seen": latest.get("odds_books_seen") or earliest.get("odds_books_seen"),
                    "earliest_snapshot": earliest.get("_snapshot_stamp"),
                    "latest_snapshot": latest.get("_snapshot_stamp"),
                    "earliest_snapshot_path": earliest.get("_snapshot_path"),
                    "latest_snapshot_path": latest.get("_snapshot_path"),
                    "current_hits_environment_row_present": bool(in_current),
                    "actual_outs_recorded": usage.get("outs_recorded") if usage else "",
                    "actual_is_starter": usage.get("is_starter") if usage else "",
                    "actual_hits_allowed": usage.get("hits_allowed") if usage else "",
                    "actual_earned_runs": usage.get("earned_runs") if usage else "",
                    "actual_strikeouts": usage.get("strikeouts_pitching") if usage else "",
                }
            )

    meta = {
        "rows": len(audit_rows),
        "warnings": len([row for row in audit_rows if row.get("lifecycle_warning")]),
        "players_with_prior_context": len(historical_by_key),
        "players_in_current_context": len(current_keys),
    }
    return audit_rows, meta


def _write_starter_market_lifecycle_audit(
    *,
    rows: Sequence[Dict[str, Any]],
    meta: Dict[str, Any],
    slate_date: str,
    out_dir: Path = Path("artifacts/analysis/mlb/pitcher_expectations"),
) -> Dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"starter_market_lifecycle_audit_{slate_date}.csv"
    md_path = out_dir / f"starter_market_lifecycle_audit_{slate_date}.md"
    _write_generic_csv(csv_path, list(rows))

    lines = [
        f"# Starter / Market Lifecycle Audit - {slate_date}",
        "",
        "Scope: diagnostic only. Identity, role, market availability, forecast trust, and actual usage are separate lifecycle layers.",
        "",
        "## Doctrine",
        "",
        "- Identity is stable.",
        "- Role is transient.",
        "- Market availability is transient.",
        "- Forecast trust is separate.",
        "- Actual usage may differ from probable role.",
        "",
        "## Summary",
        "",
        f"- Prior-context players inspected: `{meta.get('players_with_prior_context', 0)}`",
        f"- Current-context players: `{meta.get('players_in_current_context', 0)}`",
        f"- Lifecycle warning rows: `{meta.get('warnings', 0)}`",
        "",
    ]
    warning_rows = [row for row in rows if row.get("lifecycle_warning")]
    rangel_rows = [row for row in warning_rows if _norm_player_name(row.get("player_name")) == "alan rangel"]
    if not rangel_rows:
        rangel_rows = [row for row in rows if _norm_player_name(row.get("player_name")) == "alan rangel"]
    if rangel_rows:
        r = rangel_rows[0]
        lines.extend(
            [
                "## Alan Rangel Timeline",
                "",
                "- Originally probable starter for `PHI vs NYM` on `2026-06-27`.",
                "- OddsAPI had `pitcher_hits_allowed` markets for him.",
                "- Rain delay occurred before first pitch.",
                "- Rangel was no longer the active proposed starter after the delay.",
                "- Game resumed with a replacement starter.",
                "- Replacement starter lasted 1.1 innings.",
                "- Rangel later entered as a reliever.",
                "",
                "## Alan Rangel Lifecycle Classification",
                "",
                f"- Identity: `{r.get('identity_status')}` (`player_id={r.get('player_id')}`, `game_id={r.get('game_id')}`)",
                f"- Role: `{r.get('role_status')}`",
                f"- Market: `{r.get('market_status')}`",
                f"- Forecast: `{r.get('forecast_status')}` / `{r.get('forecast_diagnostic')}`",
                f"- Actual usage: `{r.get('actual_usage_status')}` (`outs_recorded={r.get('actual_outs_recorded')}`)",
                f"- Lifecycle warning: `{r.get('lifecycle_warning')}`",
                "",
                "Display expectation: Alan Rangel should not appear as an active trusted starter forecast after the role change, but his identity/game/team/market history should remain visible with a lifecycle warning.",
                "",
            ]
        )
    if warning_rows:
        lines.extend(["## Warning Rows", "", "| player | teams | role | market | forecast | actual usage | warning |", "|---|---|---|---|---|---|---|"])
        for row in warning_rows:
            lines.append(
                f"| {row.get('player_name')} | {row.get('pitcher_team')} vs {row.get('offense_team')} | `{row.get('role_status')}` | `{row.get('market_status')}` | `{row.get('forecast_status')}` / `{row.get('forecast_diagnostic')}` | `{row.get('actual_usage_status')}` | `{row.get('lifecycle_warning')}` |"
            )
    elif rows:
        lines.append("No active lifecycle warnings were detected; historical lifecycle trace rows are preserved in the CSV.")
    else:
        lines.append("No starter/market lifecycle warnings were detected.")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "starter_market_lifecycle_audit_md": str(md_path),
        "starter_market_lifecycle_audit_csv": str(csv_path),
    }


def _summarize_slate_hits_allowed(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"rows": 0}
    vals7 = [float(r.get("offense_hits_pg_last7")) for r in rows if r.get("offense_hits_pg_last7") is not None]
    vals15 = [float(r.get("offense_hits_pg_last15")) for r in rows if r.get("offense_hits_pg_last15") is not None]
    vals30 = [float(r.get("offense_hits_pg_last30")) for r in rows if r.get("offense_hits_pg_last30") is not None]
    blended = [float(r.get("offense_hits_form_blended")) for r in rows if r.get("offense_hits_form_blended") is not None]
    baseline_expected = [
        float(r.get("pitcher_expected_hits_allowed_weighted"))
        for r in rows
        if r.get("pitcher_expected_hits_allowed_weighted") is not None
    ]
    matchup_expected = [
        float(r.get("expected_hits_allowed_matchup"))
        for r in rows
        if r.get("expected_hits_allowed_matchup") is not None
    ]
    factors = [
        float(r.get("offense_factor_vs_league_clamped"))
        for r in rows
        if r.get("offense_factor_vs_league_clamped") is not None
    ]
    line_gap = [
        float(r.get("line_minus_expected_hits_allowed_matchup"))
        for r in rows
        if r.get("line_minus_expected_hits_allowed_matchup") is not None
    ]
    bullpen_blended = [
        float(r.get("bullpen_hits_allowed_form_blended"))
        for r in rows
        if r.get("bullpen_hits_allowed_form_blended") is not None
    ]
    team_matchup_expected = [
        float(r.get("expected_team_hits_allowed_matchup"))
        for r in rows
        if r.get("expected_team_hits_allowed_matchup") is not None
    ]
    forecast_unavailable_rows = [r for r in rows if str(r.get("forecast_status") or "") == "unavailable"]
    unavailable_by_reason = {}
    for r in forecast_unavailable_rows:
        reason = str(r.get("forecast_diagnostic") or r.get("forecast_note") or "unknown")
        unavailable_by_reason[reason] = int(unavailable_by_reason.get(reason, 0)) + 1
    probable_rows = [r for r in rows if bool(r.get("probable_starter_context_present"))]
    probable_total = len(
        {
            (
                r.get("game_id"),
                r.get("player_id"),
                _canonical_team_code(r.get("pitcher_team")),
                _canonical_team_code(r.get("offense_team")),
            )
            for r in probable_rows
        }
    )
    probable_with_market = len(
        {
            (
                r.get("game_id"),
                r.get("player_id"),
                _canonical_team_code(r.get("pitcher_team")),
                _canonical_team_code(r.get("offense_team")),
            )
            for r in probable_rows
            if bool(r.get("hits_allowed_market_present") or r.get("odds_market_present"))
        }
    )
    probable_context_only = len(
        {
            (
                r.get("game_id"),
                r.get("player_id"),
                _canonical_team_code(r.get("pitcher_team")),
                _canonical_team_code(r.get("offense_team")),
            )
            for r in probable_rows
            if not bool(r.get("hits_allowed_market_present") or r.get("odds_market_present"))
        }
    )
    probable_missing_forecast = len(
        {
            (
                r.get("game_id"),
                r.get("player_id"),
                _canonical_team_code(r.get("pitcher_team")),
                _canonical_team_code(r.get("offense_team")),
            )
            for r in probable_rows
            if r.get("expected_hits_allowed_matchup") is None
        }
    )
    ambiguous_resolved = len(
        [
            r
            for r in rows
            if str(r.get("forecast_source") or "") == "odds_pitcher_hits_allowed_market"
            and bool(r.get("probable_starter_context_present"))
            and r.get("player_id") is not None
        ]
    )
    ambiguous_remaining = len(
        [
            r
            for r in rows
            if str(r.get("forecast_note") or "") == "ambiguous_player_name"
        ]
    )
    unavailable_pitchers = [
        {
            "player_id": r.get("player_id"),
            "player_name": r.get("player_name"),
            "pitcher_team": r.get("pitcher_team"),
            "offense_team": r.get("offense_team"),
            "line": r.get("line"),
            "prior_starter_games": r.get("prior_starter_games"),
            "forecast_note": r.get("forecast_note"),
            "forecast_diagnostic": r.get("forecast_diagnostic"),
            "forecast_source": r.get("forecast_source"),
            "canonical_player_id": r.get("canonical_player_id"),
            "canonical_game_id": r.get("canonical_game_id"),
            "canonical_team": r.get("canonical_team"),
            "canonical_opponent": r.get("canonical_opponent"),
            "identity_status": r.get("identity_status"),
            "identity_method": r.get("identity_method"),
            "fallback_used": r.get("fallback_used"),
            "identity_warning": r.get("identity_warning"),
            "hits_allowed_market_present": r.get("hits_allowed_market_present"),
            "probable_starter_context_present": r.get("probable_starter_context_present"),
            "odds_books_seen": r.get("odds_books_seen"),
        }
        for r in forecast_unavailable_rows
    ]

    unique_matchup_rows: Dict[Tuple[Any, Any], Dict[str, Any]] = {}
    for r in rows:
        if r.get("expected_hits_allowed_matchup") is None:
            continue
        key = (r.get("game_id"), r.get("player_id"))
        if key in unique_matchup_rows:
            continue
        unique_matchup_rows[key] = {
            "game_id": r.get("game_id"),
            "player_id": r.get("player_id"),
            "player_name": r.get("player_name"),
            "pitcher_team": r.get("pitcher_team"),
            "offense_team": r.get("offense_team"),
            "expected_hits_allowed_matchup": r.get("expected_hits_allowed_matchup"),
            "pitcher_expected_hits_allowed_weighted": r.get("pitcher_expected_hits_allowed_weighted"),
            "offense_factor_vs_league_clamped": r.get("offense_factor_vs_league_clamped"),
        }

    highest_expected_rows = sorted(
        list(unique_matchup_rows.values()),
        key=lambda x: float(x.get("expected_hits_allowed_matchup") or 0.0),
        reverse=True,
    )
    lowest_expected_rows = sorted(
        list(unique_matchup_rows.values()),
        key=lambda x: float(x.get("expected_hits_allowed_matchup") or 0.0),
    )

    unique_team_expected_rows: Dict[Tuple[Any, Any], Dict[str, Any]] = {}
    for r in rows:
        if r.get("expected_team_hits_allowed_matchup") is None:
            continue
        key = (r.get("game_id"), r.get("player_id"))
        if key in unique_team_expected_rows:
            continue
        unique_team_expected_rows[key] = {
            "game_id": r.get("game_id"),
            "player_id": r.get("player_id"),
            "player_name": r.get("player_name"),
            "pitcher_team": r.get("pitcher_team"),
            "offense_team": r.get("offense_team"),
            "expected_team_hits_allowed_matchup": r.get("expected_team_hits_allowed_matchup"),
            "expected_hits_allowed_matchup": r.get("expected_hits_allowed_matchup"),
            "bullpen_hits_allowed_form_blended": r.get("bullpen_hits_allowed_form_blended"),
        }

    highest_team_expected_rows = sorted(
        list(unique_team_expected_rows.values()),
        key=lambda x: float(x.get("expected_team_hits_allowed_matchup") or 0.0),
        reverse=True,
    )
    lowest_team_expected_rows = sorted(
        list(unique_team_expected_rows.values()),
        key=lambda x: float(x.get("expected_team_hits_allowed_matchup") or 0.0),
    )

    return {
        "rows": len(rows),
        "rows_with_offense_form_last7": len(vals7),
        "rows_with_offense_form_last15": len(vals15),
        "rows_with_offense_form_last30": len(vals30),
        "avg_offense_hits_pg_last7": float(mean(vals7)) if vals7 else None,
        "avg_offense_hits_pg_last15": float(mean(vals15)) if vals15 else None,
        "avg_offense_hits_pg_last30": float(mean(vals30)) if vals30 else None,
        "rows_with_offense_form_blended": len(blended),
        "avg_offense_hits_form_blended": float(mean(blended)) if blended else None,
        "rows_with_pitcher_expected_hits_allowed_weighted": len(baseline_expected),
        "avg_pitcher_expected_hits_allowed_weighted": float(mean(baseline_expected)) if baseline_expected else None,
        "rows_with_expected_hits_allowed_matchup": len(matchup_expected),
        "avg_expected_hits_allowed_matchup": float(mean(matchup_expected)) if matchup_expected else None,
        "avg_offense_factor_vs_league_clamped": float(mean(factors)) if factors else None,
        "rows_with_line_minus_expected": len(line_gap),
        "avg_line_minus_expected_hits_allowed_matchup": float(mean(line_gap)) if line_gap else None,
        "rows_with_bullpen_hits_allowed_form_blended": len(bullpen_blended),
        "avg_bullpen_hits_allowed_form_blended": float(mean(bullpen_blended)) if bullpen_blended else None,
        "rows_with_expected_team_hits_allowed_matchup": len(team_matchup_expected),
        "avg_expected_team_hits_allowed_matchup": float(mean(team_matchup_expected)) if team_matchup_expected else None,
        "top_expected_hits_allowed_matchups": highest_expected_rows,
        "lowest_expected_hits_allowed_matchups": lowest_expected_rows,
        "top_expected_team_hits_allowed_matchups": highest_team_expected_rows,
        "lowest_expected_team_hits_allowed_matchups": lowest_team_expected_rows,
        "forecast_available_rows": len([r for r in rows if str(r.get("forecast_status") or "available") == "available"]),
        "forecast_unavailable_rows": len(forecast_unavailable_rows),
        "forecast_unavailable_by_reason": unavailable_by_reason,
        "forecast_unavailable_pitchers": unavailable_pitchers,
        "probable_starters_total": probable_total,
        "probable_starters_with_hits_allowed_market": probable_with_market,
        "probable_starters_context_only": probable_context_only,
        "probable_starters_missing_forecast": probable_missing_forecast,
        "ambiguous_names_resolved_by_probable_starter": ambiguous_resolved,
        "ambiguous_names_remaining": ambiguous_remaining,
    }


def _write_rows_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    fieldnames = [
        "slate_date",
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "raw_provider_player_name",
        "prop_type",
        "line",
        "model_pick_side",
        "model_pick_prob",
        "pitcher_team",
        "offense_team",
        "offense_hits_pg_last7",
        "offense_hits_pg_last15",
        "offense_hits_pg_last30",
        "offense_hits_samples_last7",
        "offense_hits_samples_last15",
        "offense_hits_samples_last30",
        "offense_hits_form_blended",
        "league_offense_hits_pg_last7",
        "league_offense_hits_pg_last15",
        "league_offense_hits_pg_last30",
        "league_offense_hits_form_blended",
        "bullpen_hits_allowed_pg_last7",
        "bullpen_hits_allowed_pg_last15",
        "bullpen_hits_allowed_pg_last30",
        "bullpen_hits_allowed_samples_last7",
        "bullpen_hits_allowed_samples_last15",
        "bullpen_hits_allowed_samples_last30",
        "bullpen_hits_allowed_form_blended",
        "league_bullpen_hits_allowed_pg_last7",
        "league_bullpen_hits_allowed_pg_last15",
        "league_bullpen_hits_allowed_pg_last30",
        "league_bullpen_hits_allowed_form_blended",
        "offense_factor_vs_league",
        "offense_factor_vs_league_clamped",
        "pitcher_baseline_total_starts",
        "pitcher_baseline_seasons_used",
        "pitcher_expected_hits_allowed_weighted",
        "expected_hits_allowed_matchup",
        "expected_team_hits_allowed_matchup",
        "expected_hits_allowed_delta_vs_pitcher_baseline",
        "line_minus_expected_hits_allowed_matchup",
        "forecast_status",
        "forecast_note",
        "forecast_source",
        "odds_market_present",
        "hits_allowed_market_present",
        "trusted_forecast",
        "probable_starter_context_present",
        "prior_starter_games",
        "odds_books_seen",
        *IDENTITY_FIELDNAMES,
    ]
    _ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _archive_rows_csv(
    *,
    out_csv: Path,
    rows: Sequence[Dict[str, Any]],
    snapshot_dir: Path,
    slate_date: str,
    generated_at_utc: str,
) -> Path | None:
    if not rows:
        return None
    stamp = re.sub(r"[^0-9A-Za-z]+", "", generated_at_utc.replace("Z", "Z"))
    if not stamp:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = snapshot_dir / slate_date / f"mlb_hits_environment_hits_allowed_rows_{slate_date}__{stamp}.csv"
    _write_rows_csv(path, rows)
    return path


def _append_history(path: Path, payload: Dict[str, Any]) -> None:
    _ensure_parent(path)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=True))
        fh.write("\n")


def _team_eval_tracker_fieldnames() -> List[str]:
    return [
        "evaluation_date",
        "requested_as_of_date",
        "generated_at_utc",
        "status",
        "ok",
        "warnings_count",
        "failures_count",
        "league_signal",
        "league_hits_per_game",
        "league_zscore",
        "team_eval_context_as_of_date",
        "team_eval_rows_in_snapshot",
        "team_eval_rows_with_expected",
        "team_eval_rows_with_actual",
        "team_eval_coverage_pct",
        "team_eval_expected_avg",
        "team_eval_actual_avg",
        "team_eval_residual_avg",
        "team_eval_residual_total",
        "team_eval_mae",
        "team_eval_rmse",
        "team_eval_starter_residual_avg",
        "team_eval_starter_residual_total",
        "eval_snapshot_slate_csv",
        "eval_snapshot_wide_csv",
    ]


def _build_team_eval_tracker_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    league = (payload.get("league_hits_environment") or {}).get("today_vs_baseline") or {}
    team_eval = payload.get("team_hits_allowed_matchup_evaluation") or {}
    snapshot_paths = team_eval.get("snapshot_paths") or {}
    warnings = payload.get("warnings") if isinstance(payload.get("warnings"), list) else []
    failures = payload.get("failures") if isinstance(payload.get("failures"), list) else []
    return {
        "evaluation_date": str(payload.get("evaluation_date") or ""),
        "requested_as_of_date": str(payload.get("requested_as_of_date") or ""),
        "generated_at_utc": str(payload.get("generated_at_utc") or ""),
        "status": str(payload.get("status") or ""),
        "ok": bool(payload.get("ok")),
        "warnings_count": int(len(warnings)),
        "failures_count": int(len(failures)),
        "league_signal": str(league.get("signal") or ""),
        "league_hits_per_game": _as_float(league.get("hits_per_game")),
        "league_zscore": _as_float(league.get("zscore")),
        "team_eval_context_as_of_date": str(team_eval.get("context_as_of_date") or ""),
        "team_eval_rows_in_snapshot": _as_int(team_eval.get("rows_in_eval_snapshot")),
        "team_eval_rows_with_expected": _as_int(team_eval.get("rows_with_expected")),
        "team_eval_rows_with_actual": _as_int(team_eval.get("rows_with_actual")),
        "team_eval_coverage_pct": _as_float(team_eval.get("coverage_pct")),
        "team_eval_expected_avg": _as_float(team_eval.get("expected_team_hits_allowed_avg")),
        "team_eval_actual_avg": _as_float(team_eval.get("actual_offense_hits_avg")),
        "team_eval_residual_avg": _as_float(team_eval.get("residual_avg")),
        "team_eval_residual_total": _as_float(team_eval.get("residual_total")),
        "team_eval_mae": _as_float(team_eval.get("mae")),
        "team_eval_rmse": _as_float(team_eval.get("rmse")),
        "team_eval_starter_residual_avg": _as_float(team_eval.get("starter_only_residual_avg")),
        "team_eval_starter_residual_total": _as_float(team_eval.get("starter_only_residual_total")),
        "eval_snapshot_slate_csv": str(snapshot_paths.get("slate_csv") or ""),
        "eval_snapshot_wide_csv": str(snapshot_paths.get("wide_csv") or ""),
    }


def _upsert_team_eval_tracker(path: Path, payload: Dict[str, Any]) -> None:
    fieldnames = _team_eval_tracker_fieldnames()
    key = str(payload.get("evaluation_date") or "")
    if not key:
        return
    row = _build_team_eval_tracker_row(payload)

    existing: List[Dict[str, Any]] = []
    if path.exists():
        try:
            with path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for prev in reader:
                    prev_key = str((prev or {}).get("evaluation_date") or "")
                    if prev_key and prev_key != key:
                        existing.append({k: prev.get(k, "") for k in fieldnames})
        except Exception:
            existing = []

    existing.append({k: row.get(k, "") for k in fieldnames})
    existing.sort(key=lambda r: str(r.get("evaluation_date") or ""))

    _ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for item in existing:
            writer.writerow(item)


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Report MLB hits environment and hits_allowed context.")
    ap.add_argument("--as-of-date", default=date.today().isoformat(), help="Requested evaluation date (YYYY-MM-DD).")
    ap.add_argument("--lookback-days", type=int, default=30, help="Number of prior game dates for baseline.")
    ap.add_argument("--recent-days", type=int, default=7, help="Recent game-date window for trend delta.")
    ap.add_argument(
        "--starter-baseline-seasons",
        type=int,
        default=3,
        help="Starter residual baseline seasons (including eval year).",
    )
    ap.add_argument(
        "--starter-baseline-min-starts",
        type=int,
        default=5,
        help="Minimum historical starts required to compute weighted starter expectation.",
    )
    ap.add_argument(
        "--starter-baseline-season-weight-decay",
        type=float,
        default=0.70,
        help="Per-season recency decay for starter baseline (1.0=no decay, 0.7=older seasons down-weighted).",
    )
    ap.add_argument(
        "--slate-offense-weight-last7",
        type=float,
        default=0.50,
        help="Weight for opponent-team hits/game last-7 in matchup expectation blend.",
    )
    ap.add_argument(
        "--slate-offense-weight-last15",
        type=float,
        default=0.30,
        help="Weight for opponent-team hits/game last-15 in matchup expectation blend.",
    )
    ap.add_argument(
        "--slate-offense-weight-last30",
        type=float,
        default=0.20,
        help="Weight for opponent-team hits/game last-30 in matchup expectation blend.",
    )
    ap.add_argument(
        "--slate-offense-factor-min",
        type=float,
        default=0.70,
        help="Lower clamp bound for opponent form factor applied to pitcher expected hits allowed.",
    )
    ap.add_argument(
        "--slate-offense-factor-max",
        type=float,
        default=1.30,
        help="Upper clamp bound for opponent form factor applied to pitcher expected hits allowed.",
    )
    ap.add_argument("--slate-date", default="", help="Slate date for hits_allowed context (defaults to as-of-date).")
    ap.add_argument("--slate-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--wide-csv", default="backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv")
    ap.add_argument(
        "--odds-snapshot",
        default="",
        help="Local odds snapshot used only to surface pitcher coverage gaps; no network fetch is performed.",
    )
    ap.add_argument("--out-json", default="artifacts/analysis/mlb/mlb_hits_environment_latest.json")
    ap.add_argument("--out-csv", default="tmp/analysis/mlb_hits_environment_hits_allowed_rows.csv")
    ap.add_argument(
        "--snapshot-dir",
        default="artifacts/analysis/mlb/hits_environment_snapshots",
        help="Directory for timestamped full-row hits-environment snapshots.",
    )
    ap.add_argument("--history-jsonl", default="artifacts/analysis/mlb/mlb_hits_environment_history.jsonl")
    ap.add_argument(
        "--eval-tracker-csv",
        default="artifacts/analysis/mlb/mlb_hits_environment_team_eval_daily_tracker.csv",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    lookback_days = max(7, int(args.lookback_days))
    recent_days = max(3, int(args.recent_days))
    starter_baseline_seasons = max(1, int(args.starter_baseline_seasons))
    starter_baseline_min_starts = max(1, int(args.starter_baseline_min_starts))
    starter_baseline_decay = float(args.starter_baseline_season_weight_decay)
    if starter_baseline_decay <= 0.0:
        starter_baseline_decay = 0.01
    if starter_baseline_decay > 1.0:
        starter_baseline_decay = 1.0
    slate_offense_weight_last7 = max(0.0, float(args.slate_offense_weight_last7))
    slate_offense_weight_last15 = max(0.0, float(args.slate_offense_weight_last15))
    slate_offense_weight_last30 = max(0.0, float(args.slate_offense_weight_last30))
    if (slate_offense_weight_last7 + slate_offense_weight_last15 + slate_offense_weight_last30) <= 0:
        slate_offense_weight_last7 = 0.50
        slate_offense_weight_last15 = 0.30
        slate_offense_weight_last30 = 0.20
    slate_offense_factor_min = float(args.slate_offense_factor_min)
    slate_offense_factor_max = float(args.slate_offense_factor_max)
    if slate_offense_factor_min > slate_offense_factor_max:
        slate_offense_factor_min, slate_offense_factor_max = slate_offense_factor_max, slate_offense_factor_min
    as_of = _parse_date(str(args.as_of_date))
    from_date = as_of - timedelta(days=max(120, (lookback_days * 4)))

    failures: List[str] = []
    warnings: List[str] = []

    try:
        daily_rows = _fetch_daily_game_hits(_to_iso(from_date), _to_iso(as_of))
    except Exception as exc:
        failures.append(f"daily_hits_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        payload = {
            "generated_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ok": False,
            "status": "fail",
            "requested_as_of_date": _to_iso(as_of),
            "evaluation_date": None,
            "failures": failures,
            "warnings": warnings,
        }
        out_json = Path(args.out_json)
        _ensure_parent(out_json)
        out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 2
    eval_date = _resolve_evaluation_date(daily_rows, _to_iso(as_of))
    if not eval_date:
        failures.append("no_daily_hits_rows")
        payload = {
            "generated_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ok": False,
            "status": "fail",
            "requested_as_of_date": _to_iso(as_of),
            "evaluation_date": None,
            "failures": failures,
            "warnings": warnings,
        }
        out_json = Path(args.out_json)
        _ensure_parent(out_json)
        out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 2

    league_summary = _summarize_league_environment(
        rows=daily_rows,
        eval_date=eval_date,
        lookback_days=lookback_days,
        recent_days=recent_days,
    )
    if league_summary.get("status") != "pass":
        failures.append(f"league_summary:{league_summary.get('reason')}")

    try:
        starter_rows = _fetch_starter_hits_allowed_rows(eval_date)
    except Exception as exc:
        failures.append(f"starter_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        starter_rows = []
    try:
        starter_flag_diag = _fetch_starter_flag_diagnostics(eval_date)
    except Exception as exc:
        failures.append(f"starter_flag_diag_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        starter_flag_diag = {}
    starter_baseline_by_player: Dict[int, Dict[str, Any]] = {}
    starter_baseline_meta: Dict[str, Any] = {}
    try:
        starter_baseline_by_player, starter_baseline_meta = _fetch_multi_season_starter_baselines(
            eval_date=eval_date,
            seasons_back=starter_baseline_seasons,
            season_weight_decay=starter_baseline_decay,
            min_starts=starter_baseline_min_starts,
        )
    except Exception as exc:
        failures.append(f"starter_baseline_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        starter_baseline_by_player = {}
        starter_baseline_meta = {
            "seasons_back": int(starter_baseline_seasons),
            "season_weight_decay": float(starter_baseline_decay),
            "min_starts": int(starter_baseline_min_starts),
        }
    starter_summary = _summarize_starter_rows(
        starter_rows,
        baseline_by_player=starter_baseline_by_player,
        baseline_meta=starter_baseline_meta,
    )
    if int(starter_summary.get("rows") or 0) == 0:
        warnings.append("no_starter_hits_allowed_rows_for_evaluation_date")
        if int((starter_flag_diag or {}).get("pitcher_rows_with_outs") or 0) > 0:
            warnings.append("starter_flags_missing_or_unset_for_pitcher_rows")

    try:
        team_form = _fetch_team_hits_form(eval_date)
    except Exception as exc:
        failures.append(f"team_form_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        team_form = {}
    try:
        bullpen_form = _fetch_team_bullpen_hits_allowed_form(eval_date)
    except Exception as exc:
        failures.append(f"bullpen_form_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        bullpen_form = {}
    slate_date = str(args.slate_date or "").strip() or _to_iso(as_of)
    odds_snapshot = Path(str(args.odds_snapshot or "").strip()) if str(args.odds_snapshot or "").strip() else (
        Path("backend/mlb/exports/odds_history") / slate_date / "odds_latest_compatible.json"
    )
    slate_rows = _build_slate_hits_allowed_rows(
        slate_csv=Path(args.slate_csv),
        wide_csv=Path(args.wide_csv),
        odds_snapshot=odds_snapshot,
        slate_date=slate_date,
        team_form=team_form,
        bullpen_form=bullpen_form,
        starter_baseline_by_player=starter_baseline_by_player,
        starter_baseline_min_starts=starter_baseline_min_starts,
        offense_weight_last7=slate_offense_weight_last7,
        offense_weight_last15=slate_offense_weight_last15,
        offense_weight_last30=slate_offense_weight_last30,
        offense_factor_min=slate_offense_factor_min,
        offense_factor_max=slate_offense_factor_max,
    )
    slate_identity_health = _apply_hits_environment_identity(slate_rows, slate_date)
    slate_summary = _summarize_slate_hits_allowed(slate_rows)
    slate_summary["identity_health"] = slate_identity_health
    if int(slate_summary.get("rows") or 0) == 0:
        warnings.append("no_hits_allowed_rows_found_in_slate_csv")

    team_hits_allowed_eval: Dict[str, Any] = {}
    eval_context_date = _to_iso(_parse_date(eval_date) - timedelta(days=1))
    eval_snapshot_root = Path("backend/mlb/exports/odds_history") / eval_date
    eval_slate_csv = eval_snapshot_root / "mlb_slate_output.csv"
    eval_wide_csv = eval_snapshot_root / "mlb_predictions_wide_calibrated.csv"
    if not eval_slate_csv.exists():
        eval_slate_csv = Path(args.slate_csv)
    if not eval_wide_csv.exists():
        eval_wide_csv = Path(args.wide_csv)
    try:
        eval_team_form = _fetch_team_hits_form(eval_context_date)
    except Exception as exc:
        failures.append(f"team_eval_form_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        eval_team_form = {}
    try:
        eval_bullpen_form = _fetch_team_bullpen_hits_allowed_form(eval_context_date)
    except Exception as exc:
        failures.append(f"team_eval_bullpen_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        eval_bullpen_form = {}
    try:
        eval_rows = _build_slate_hits_allowed_rows(
            slate_csv=eval_slate_csv,
            wide_csv=eval_wide_csv,
            odds_snapshot=eval_snapshot_root / "odds_latest_compatible.json",
            slate_date=eval_date,
            team_form=eval_team_form,
            bullpen_form=eval_bullpen_form,
            starter_baseline_by_player=starter_baseline_by_player,
            starter_baseline_min_starts=starter_baseline_min_starts,
            offense_weight_last7=slate_offense_weight_last7,
            offense_weight_last15=slate_offense_weight_last15,
            offense_weight_last30=slate_offense_weight_last30,
            offense_factor_min=slate_offense_factor_min,
            offense_factor_max=slate_offense_factor_max,
        )
    except Exception as exc:
        failures.append(f"team_eval_build_rows_error:{type(exc).__name__}")
        warnings.append(str(exc))
        eval_rows = []
    try:
        actual_offense_hits = _fetch_actual_offense_hits_by_game_team(eval_date)
    except Exception as exc:
        failures.append(f"team_eval_actual_hits_query_error:{type(exc).__name__}")
        warnings.append(str(exc))
        actual_offense_hits = {}
    team_hits_allowed_eval = _evaluate_team_hits_allowed_expectation(
        eval_rows,
        actual_offense_hits_by_game_team=actual_offense_hits,
    )
    team_hits_allowed_eval["evaluation_date"] = eval_date
    team_hits_allowed_eval["context_as_of_date"] = eval_context_date
    team_hits_allowed_eval["rows_in_eval_snapshot"] = int(len(eval_rows))
    team_hits_allowed_eval["snapshot_paths"] = {
        "slate_csv": str(eval_slate_csv),
        "wide_csv": str(eval_wide_csv),
    }
    if int(team_hits_allowed_eval.get("rows_with_expected") or 0) == 0:
        warnings.append("no_team_eval_expected_rows_for_evaluation_date")
    elif int(team_hits_allowed_eval.get("rows_with_actual") or 0) == 0:
        warnings.append("no_team_eval_rows_with_actual_for_evaluation_date")

    generated_at_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_rows_csv(Path(args.out_csv), slate_rows)
    snapshot_csv = _archive_rows_csv(
        out_csv=Path(args.out_csv),
        rows=slate_rows,
        snapshot_dir=Path(args.snapshot_dir),
        slate_date=slate_date,
        generated_at_utc=generated_at_utc,
    )
    lifecycle_rows, lifecycle_meta = _build_starter_market_lifecycle_audit(
        current_rows=slate_rows,
        snapshot_rows=_load_hits_environment_snapshot_rows(Path(args.snapshot_dir), slate_date),
        slate_date=slate_date,
    )
    lifecycle_outputs = _write_starter_market_lifecycle_audit(
        rows=lifecycle_rows,
        meta=lifecycle_meta,
        slate_date=slate_date,
    )
    identity_outputs = _write_hits_environment_identity_artifacts(
        rows=slate_rows,
        slate_date=slate_date,
        generated_at_utc=generated_at_utc,
    )

    ok = len(failures) == 0
    payload: Dict[str, Any] = {
        "generated_at_utc": generated_at_utc,
        "requested_as_of_date": _to_iso(as_of),
        "evaluation_date": eval_date,
        "window": {
            "lookback_days": lookback_days,
            "recent_days": recent_days,
            "from_date": _to_iso(from_date),
            "to_date": _to_iso(as_of),
        },
        "starter_baseline_config": {
            "seasons_back": int(starter_baseline_seasons),
            "min_starts": int(starter_baseline_min_starts),
            "season_weight_decay": float(starter_baseline_decay),
            "from_season_year": _as_int(starter_baseline_meta.get("from_season_year")),
        },
        "slate_matchup_config": {
            "offense_weight_last7": float(slate_offense_weight_last7),
            "offense_weight_last15": float(slate_offense_weight_last15),
            "offense_weight_last30": float(slate_offense_weight_last30),
            "offense_factor_min": float(slate_offense_factor_min),
            "offense_factor_max": float(slate_offense_factor_max),
        },
        "league_hits_environment": league_summary,
        "starter_hits_allowed_residual": starter_summary,
        "starter_flag_diagnostics": starter_flag_diag,
        "slate_hits_allowed_context": slate_summary,
        "starter_market_lifecycle": {
            "summary": lifecycle_meta,
        "warnings": [row for row in lifecycle_rows if row.get("lifecycle_warning")],
        },
        "team_hits_allowed_matchup_evaluation": team_hits_allowed_eval,
        "outputs": {
            "out_json": str(Path(args.out_json)),
            "out_csv": str(Path(args.out_csv)),
            "snapshot_csv": str(snapshot_csv) if snapshot_csv else "",
            "snapshot_dir": str(Path(args.snapshot_dir)),
            "history_jsonl": str(Path(args.history_jsonl)),
            "eval_tracker_csv": str(Path(args.eval_tracker_csv)),
            "odds_snapshot": str(odds_snapshot),
            **identity_outputs,
            **lifecycle_outputs,
        },
        "ok": ok,
        "status": "pass" if ok else "fail",
        "failures": failures,
        "warnings": warnings,
    }

    out_json = Path(args.out_json)
    _ensure_parent(out_json)
    out_json.write_text(json.dumps(payload, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    _append_history(Path(args.history_jsonl), payload)
    _upsert_team_eval_tracker(Path(args.eval_tracker_csv), payload)

    signal = (
        ((payload.get("league_hits_environment") or {}).get("today_vs_baseline") or {}).get("signal")
        if isinstance(payload.get("league_hits_environment"), dict)
        else None
    )
    hpg = (
        ((payload.get("league_hits_environment") or {}).get("today_vs_baseline") or {}).get("hits_per_game")
        if isinstance(payload.get("league_hits_environment"), dict)
        else None
    )
    z = (
        ((payload.get("league_hits_environment") or {}).get("today_vs_baseline") or {}).get("zscore")
        if isinstance(payload.get("league_hits_environment"), dict)
        else None
    )
    starter_resid = payload.get("starter_hits_allowed_residual") or {}
    starter_weighted = starter_resid.get("weighted_baseline") or {}
    team_eval = payload.get("team_hits_allowed_matchup_evaluation") or {}
    print(
        f"[hits-environment] eval_date={eval_date} "
        f"signal={signal} hits_per_game={hpg} zscore={z} "
        f"starter_residual_d7_avg={starter_resid.get('residual_vs_d7_avg')} "
        f"starter_residual_weighted_avg={starter_weighted.get('residual_vs_weighted_avg')} "
        f"team_eval_rows={team_eval.get('rows_with_actual')} "
        f"team_eval_residual_avg={team_eval.get('residual_avg')} "
        f"slate_hits_allowed_rows={slate_summary.get('rows')}"
    )
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
