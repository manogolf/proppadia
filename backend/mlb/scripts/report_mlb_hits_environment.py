#!/usr/bin/env python3
"""Track MLB hits environment and pitcher-hits context for daily operations."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime, timedelta
from math import sqrt
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.shared.db.pg import pg_fetchall
from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    normalizeTeamAbbreviation,
)


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


def _build_slate_hits_allowed_rows(
    *,
    slate_csv: Path,
    wide_csv: Path,
    slate_date: str,
    team_form: Dict[str, Dict[str, Any]],
    bullpen_form: Dict[str, Dict[str, Any]],
    starter_baseline_by_player: Dict[int, Dict[str, Any]],
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
            starter_baseline = starter_baseline_by_player.get(int(player_id), {})
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
            line = _as_float(row.get("line"))
            line_minus_expected_hits_allowed_matchup = None
            if line is not None and expected_hits_allowed_matchup is not None:
                line_minus_expected_hits_allowed_matchup = line - expected_hits_allowed_matchup
            out.append(
                {
                    "slate_date": slate_date,
                    "game_date": game_date,
                    "game_id": game_id,
                    "player_id": player_id,
                    "player_name": str(row.get("player_name") or "").strip(),
                    "prop_type": prop_type,
                    "line": line,
                    "model_pick_side": str(row.get("model_pick_side") or "").strip().lower(),
                    "model_pick_prob": _as_float(row.get("model_pick_prob")),
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
                }
            )
    if out:
        return out

    # Fallback: if slate has no hits_allowed rows, synthesize matchup context
    # from available pitcher props in the wide file for the same slate date.
    for (game_id, player_id), wide in sorted(wide_pitcher_ctx.items()):
        game_date = str(wide.get("game_date") or "").strip() or slate_date
        prop_type = str(wide.get("prop_type") or "").strip().lower() or "hits_allowed"
        offense_team = _canonical_team_code(wide.get("opponent"))
        pitcher_team = _canonical_team_code(wide.get("team"))
        if not offense_team:
            offense_team = _canonical_team_code(wide.get("opponent_id"))
        if not pitcher_team:
            pitcher_team = _canonical_team_code(wide.get("team_id"))
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
        starter_baseline = starter_baseline_by_player.get(int(player_id), {})
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
        out.append(
            {
                "slate_date": slate_date,
                "game_date": game_date,
                "game_id": game_id,
                "player_id": player_id,
                "player_name": str(wide.get("player_name") or "").strip(),
                "prop_type": prop_type,
                "line": None,
                "model_pick_side": "",
                "model_pick_prob": None,
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
                "line_minus_expected_hits_allowed_matchup": None,
            }
        )
    return out


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
    }


def _write_rows_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    fieldnames = [
        "slate_date",
        "game_date",
        "game_id",
        "player_id",
        "player_name",
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
    ]
    _ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


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
    ap.add_argument("--out-json", default="artifacts/analysis/mlb/mlb_hits_environment_latest.json")
    ap.add_argument("--out-csv", default="tmp/analysis/mlb_hits_environment_hits_allowed_rows.csv")
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
    slate_rows = _build_slate_hits_allowed_rows(
        slate_csv=Path(args.slate_csv),
        wide_csv=Path(args.wide_csv),
        slate_date=slate_date,
        team_form=team_form,
        bullpen_form=bullpen_form,
        starter_baseline_by_player=starter_baseline_by_player,
        offense_weight_last7=slate_offense_weight_last7,
        offense_weight_last15=slate_offense_weight_last15,
        offense_weight_last30=slate_offense_weight_last30,
        offense_factor_min=slate_offense_factor_min,
        offense_factor_max=slate_offense_factor_max,
    )
    slate_summary = _summarize_slate_hits_allowed(slate_rows)
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
            slate_date=eval_date,
            team_form=eval_team_form,
            bullpen_form=eval_bullpen_form,
            starter_baseline_by_player=starter_baseline_by_player,
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

    _write_rows_csv(Path(args.out_csv), slate_rows)

    ok = len(failures) == 0
    payload: Dict[str, Any] = {
        "generated_at_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
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
        "team_hits_allowed_matchup_evaluation": team_hits_allowed_eval,
        "outputs": {
            "out_json": str(Path(args.out_json)),
            "out_csv": str(Path(args.out_csv)),
            "history_jsonl": str(Path(args.history_jsonl)),
            "eval_tracker_csv": str(Path(args.eval_tracker_csv)),
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
