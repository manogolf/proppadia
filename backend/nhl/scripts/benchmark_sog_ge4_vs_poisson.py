#!/usr/bin/env python3
"""Benchmark NHL SOG 3.5 predictions against a simple expected-SOG Poisson baseline."""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Sequence

from backend.shared.db.pg import pg_fetchall, pg_fetchone


BASE_CTE = """
WITH base AS (
  SELECT
    g.game_date::date AS game_date,
    p.player_id::bigint AS player_id,
    COALESCE(pl.full_name, concat_ws(' ', pl.first_name, pl.last_name), p.player_id::text) AS player_name,
    COALESCE(NULLIF(BTRIM(pl.position), ''), 'UNK') AS position_raw,
    p.line::float8 AS line,
    p.p_over::float8 AS model_p_over,
    s.shots_on_goal::int AS shots_on_goal,
    f.d10_sog_per60::float8 AS d10_sog_per60,
    f.d10_toi_min_avg::float8 AS d10_toi_min_avg,
    ((f.d10_sog_per60 * f.d10_toi_min_avg) / 60.0)::float8 AS expected_sog
  FROM nhl.predictions p
  JOIN nhl.games g
    ON g.game_id = p.game_id
  LEFT JOIN nhl.skater_game_logs_raw s
    ON s.game_id = p.game_id
   AND s.player_id = p.player_id
  LEFT JOIN nhl.training_features_nhl_sog_enriched_pregame_v2 f
    ON f.game_id = p.game_id
   AND f.player_id = p.player_id
  LEFT JOIN nhl.players pl
    ON pl.player_id = p.player_id
  WHERE p.prop = 'shots_on_goal'
    AND p.model_family = %s
    AND p.model_version = %s
    AND p.line = %s::float8
    AND g.game_date BETWEEN %s::date AND %s::date
    AND s.shots_on_goal IS NOT NULL
)
"""


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(v)
    except Exception:
        return 0


def _round(v: float | None, digits: int = 4) -> float | None:
    if v is None:
        return None
    return round(float(v), digits)


def _require_iso(raw: str, label: str) -> str:
    out = str(raw).strip()
    date.fromisoformat(out)
    return out


def _pick_latest_game_date(model_family: str, model_version: str, line: float) -> str:
    row = pg_fetchone(
        """
        SELECT MAX(g.game_date)::text AS to_date
        FROM nhl.predictions p
        JOIN nhl.games g ON g.game_id = p.game_id
        WHERE p.prop = 'shots_on_goal'
          AND p.model_family = %s
          AND p.model_version = %s
          AND p.line = %s::float8
        """,
        (model_family, model_version, line),
    )
    to_date = (row or {}).get("to_date")
    if not to_date:
        raise RuntimeError("No matching NHL SOG predictions found for the selected model/line.")
    return str(to_date)


@dataclass
class Window:
    from_date: str
    to_date: str


def _resolve_window(from_date_raw: str | None, to_date_raw: str | None, lookback_days: int, model_family: str, model_version: str, line: float) -> Window:
    to_date = _require_iso(to_date_raw, "to-date") if to_date_raw else _pick_latest_game_date(model_family, model_version, line)
    to_d = date.fromisoformat(to_date)
    if from_date_raw:
        from_date = _require_iso(from_date_raw, "from-date")
    else:
        from_date = (to_d.fromordinal(to_d.toordinal() - max(1, lookback_days) + 1)).isoformat()
    from_d = date.fromisoformat(from_date)
    if from_d > to_d:
        raise ValueError("from-date must be <= to-date")
    return Window(from_date=from_d.isoformat(), to_date=to_d.isoformat())


def _role_bucket(pos: str | None) -> str:
    if not pos:
        return "UNK"
    p = str(pos).strip().upper()
    return "D" if p == "D" else ("UNK" if p == "UNK" else "F")


def _toi_bucket(v: float | None) -> str:
    if v is None:
        return "missing"
    if v < 12:
        return "<12"
    if v < 16:
        return "12-16"
    if v < 20:
        return "16-20"
    return "20+"


def _expected_bucket(v: float | None) -> str:
    if v is None:
        return "missing"
    if v < 1.5:
        return "<1.5"
    if v < 2.5:
        return "1.5-2.5"
    if v < 3.5:
        return "2.5-3.5"
    return "3.5+"


def _poisson_tail(lam: float | None, threshold: int) -> float | None:
    if lam is None or lam < 0:
        return None
    # P(X >= threshold) = 1 - P(X <= threshold-1)
    cutoff = max(0, threshold - 1)
    cdf = 0.0
    for k in range(cutoff + 1):
        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
    return max(0.0, min(1.0, 1.0 - cdf))


def _fetch_rows(model_family: str, model_version: str, line: float, w: Window) -> List[Dict[str, Any]]:
    rows = pg_fetchall(
        BASE_CTE
        + """
        SELECT
          game_date,
          player_id,
          player_name,
          position_raw,
          line,
          model_p_over,
          shots_on_goal,
          d10_sog_per60,
          d10_toi_min_avg,
          expected_sog
        FROM base
        ORDER BY game_date, player_id
        """,
        (model_family, model_version, line, w.from_date, w.to_date),
    )
    return list(rows or [])


def _metric_rows(rows: Sequence[Dict[str, Any]], prob_key: str, threshold: int) -> Dict[str, Any]:
    probs: List[float] = []
    ys: List[int] = []
    for row in rows:
        p = _to_float(row.get(prob_key))
        shots = _to_float(row.get("shots_on_goal"))
        if p is None or shots is None:
            continue
        probs.append(p)
        ys.append(1 if shots >= threshold else 0)

    n = len(probs)
    if n == 0:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}

    avg_p = sum(probs) / n
    hit_rate = sum(ys) / n
    brier = sum((p - y) ** 2 for p, y in zip(probs, ys)) / n
    return {
        "n": n,
        "avg_p": _round(avg_p),
        "hit_rate": _round(hit_rate),
        "gap": _round(avg_p - hit_rate),
        "brier": _round(brier),
    }


def _segment_stats(rows: Sequence[Dict[str, Any]], key_fn, threshold: int) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = key_fn(row)
        buckets.setdefault(key, []).append(row)

    out: List[Dict[str, Any]] = []
    for key, group in sorted(buckets.items(), key=lambda item: item[0]):
        model_stats = _metric_rows(group, "model_p_over", threshold)
        poisson_stats = _metric_rows(group, "poisson_p_over", threshold)
        out.append(
            {
                "segment_value": key,
                "n": model_stats["n"],
                "model": model_stats,
                "poisson": poisson_stats,
                "poisson_minus_model_gap": _round(
                    (_to_float(poisson_stats.get("gap")) or 0.0) - (_to_float(model_stats.get("gap")) or 0.0)
                ),
            }
        )
    return out


def _player_extremes(rows: Sequence[Dict[str, Any]], threshold: int, min_n: int, top_n: int) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_to_int(row.get("player_id")), []).append(row)

    scored: List[Dict[str, Any]] = []
    for _, group in grouped.items():
        if len(group) < min_n:
            continue
        model_stats = _metric_rows(group, "model_p_over", threshold)
        poisson_stats = _metric_rows(group, "poisson_p_over", threshold)
        scored.append(
            {
                "player_id": _to_int(group[0].get("player_id")),
                "player_name": group[0].get("player_name"),
                "role": _role_bucket(group[0].get("position_raw")),
                "avg_expected_sog": _round(
                    sum(_to_float(r.get("expected_sog")) or 0.0 for r in group) / len(group)
                ),
                "n": len(group),
                "model_gap": model_stats.get("gap"),
                "poisson_gap": poisson_stats.get("gap"),
                "model_avg_p": model_stats.get("avg_p"),
                "poisson_avg_p": poisson_stats.get("avg_p"),
                "hit_rate": model_stats.get("hit_rate"),
            }
        )

    under = sorted(scored, key=lambda r: _to_float(r.get("model_gap")) if _to_float(r.get("model_gap")) is not None else 999.0)
    poisson_better = sorted(
        scored,
        key=lambda r: abs(_to_float(r.get("model_gap")) or 0.0) - abs(_to_float(r.get("poisson_gap")) or 0.0),
        reverse=True,
    )
    return {
        "most_model_underpredicted": under[:top_n],
        "largest_poisson_improvement": poisson_better[:top_n],
    }


def analyze(args: argparse.Namespace) -> Dict[str, Any]:
    line = float(args.line)
    threshold = int(math.floor(line) + 1)
    window = _resolve_window(
        from_date_raw=args.from_date,
        to_date_raw=args.to_date,
        lookback_days=max(1, int(args.lookback_days)),
        model_family=args.model_family,
        model_version=args.model_version,
        line=line,
    )

    rows = _fetch_rows(args.model_family, args.model_version, line, window)
    for row in rows:
        row["poisson_p_over"] = _poisson_tail(_to_float(row.get("expected_sog")), threshold)

    return {
        "ok": True,
        "config": {
            "model_family": args.model_family,
            "model_version": args.model_version,
            "line": line,
            "threshold": threshold,
            "from_date": window.from_date,
            "to_date": window.to_date,
            "lookback_days": int(args.lookback_days),
        },
        "span": {"rows": len(rows)},
        "overall": {
            "model": _metric_rows(rows, "model_p_over", threshold),
            "poisson": _metric_rows(rows, "poisson_p_over", threshold),
        },
        "by_expected_sog_bucket": _segment_stats(rows, lambda r: _expected_bucket(_to_float(r.get("expected_sog"))), threshold),
        "by_role": _segment_stats(rows, lambda r: _role_bucket(r.get("position_raw")), threshold),
        "by_toi": _segment_stats(rows, lambda r: _toi_bucket(_to_float(r.get("d10_toi_min_avg"))), threshold),
        "player_extremes": _player_extremes(
            rows,
            threshold=threshold,
            min_n=max(1, int(args.player_min_n)),
            top_n=max(1, int(args.player_top_n)),
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark NHL SOG 3.5 model output vs a simple Poisson baseline.")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--line", type=float, default=3.5)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--player-min-n", type=int, default=5)
    ap.add_argument("--player-top-n", type=int, default=10)
    args = ap.parse_args()

    print(json.dumps(analyze(args), indent=2))


if __name__ == "__main__":
    main()
