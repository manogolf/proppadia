#!/usr/bin/env python3
"""Layered NHL SOG quality analysis from nhl.predictions + game truth."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Sequence

from backend.shared.db.pg import pg_fetchall, pg_fetchone


BASE_CTE = """
WITH base AS (
  SELECT
    g.game_date::date AS game_date,
    p.line::float8 AS line,
    p.p_over::float8 AS p_over,
    CASE
      WHEN s.shots_on_goal IS NULL THEN NULL
      WHEN s.shots_on_goal >= (floor(p.line)::int + 1) THEN 1
      ELSE 0
    END::int AS y,
    p.player_id::bigint AS player_id,
    COALESCE(pl.full_name, concat_ws(' ', pl.first_name, pl.last_name), p.player_id::text) AS player_name,
    COALESCE(NULLIF(BTRIM(pl.position), ''), 'UNK') AS position_raw,
    f.d10_toi_min_avg::float8 AS d10_toi_min_avg,
    f.d10_sog_per60::float8 AS d10_sog_per60,
    COALESCE(f.pp_role_source, 'missing') AS pp_role_source
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
    AND p.line = ANY(string_to_array(%s, ',')::float8[])
    AND g.game_date BETWEEN %s::date AND %s::date
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


def _line_sort(v: Any) -> float:
    f = _to_float(v)
    return f if f is not None else 9999.0


def _require_iso(value: str | None, label: str) -> str | None:
    if value is None:
        return None
    out = str(value).strip()
    date.fromisoformat(out)
    return out


def _parse_lines(raw: str) -> List[float]:
    out: List[float] = []
    for token in (raw or "").split(","):
        token = token.strip()
        if not token:
            continue
        out.append(float(token))
    if not out:
        raise ValueError("No valid lines parsed from --lines")
    return sorted(set(out))


def _pick_latest_game_date(model_family: str, model_version: str, lines_csv: str) -> str:
    row = pg_fetchone(
        """
        SELECT MAX(g.game_date)::text AS to_date
        FROM nhl.predictions p
        JOIN nhl.games g ON g.game_id = p.game_id
        WHERE p.prop = 'shots_on_goal'
          AND p.model_family = %s
          AND p.model_version = %s
          AND p.line = ANY(string_to_array(%s, ',')::float8[])
        """,
        (model_family, model_version, lines_csv),
    )
    to_date = (row or {}).get("to_date")
    if not to_date:
        raise RuntimeError("No matching NHL SOG predictions found for the selected model/lines.")
    return str(to_date)


@dataclass
class Window:
    from_date: str
    to_date: str
    recent_from_date: str
    recent_days: int


def _resolve_window(
    from_date_raw: str | None,
    to_date_raw: str | None,
    lookback_days: int,
    recent_days: int,
    model_family: str,
    model_version: str,
    lines_csv: str,
) -> Window:
    from_date = _require_iso(from_date_raw, "from-date") if from_date_raw else None
    to_date = _require_iso(to_date_raw, "to-date") if to_date_raw else None

    if to_date is None:
        to_date = _pick_latest_game_date(model_family, model_version, lines_csv)

    to_d = date.fromisoformat(to_date)
    if from_date is None:
        from_date = (to_d - timedelta(days=max(1, lookback_days))).isoformat()

    from_d = date.fromisoformat(from_date)
    if from_d > to_d:
        raise ValueError("from-date must be <= to-date")

    rec_from = max(from_d, to_d - timedelta(days=max(0, recent_days - 1)))
    return Window(
        from_date=from_d.isoformat(),
        to_date=to_d.isoformat(),
        recent_from_date=rec_from.isoformat(),
        recent_days=max(1, recent_days),
    )


def _base_params(
    model_family: str,
    model_version: str,
    lines_csv: str,
    w: Window,
) -> tuple[Any, ...]:
    return (model_family, model_version, lines_csv, w.from_date, w.to_date)


def _format_metric_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        avg_p = _to_float(row.get("avg_p"))
        hit_rate = _to_float(row.get("hit_rate"))
        n_pred = _to_int(row.get("n_pred"))
        n_eval = _to_int(row.get("n_eval"))
        out.append(
            {
                **row,
                "line": _to_float(row.get("line")),
                "n_pred": n_pred,
                "n_eval": n_eval,
                "truth_coverage": _round((n_eval / n_pred) if n_pred else 0.0, 4),
                "avg_p": _round(avg_p),
                "hit_rate": _round(hit_rate),
                "gap": _round((avg_p - hit_rate) if avg_p is not None and hit_rate is not None else None),
                "brier": _round(_to_float(row.get("brier"))),
            }
        )
    return out


def _fetch_span_stats(base_params: Sequence[Any]) -> Dict[str, Any]:
    row = pg_fetchone(
        BASE_CTE
        + """
        SELECT
          COUNT(*)::int AS total_pred_rows,
          COUNT(*) FILTER (WHERE y IS NOT NULL)::int AS total_eval_rows,
          COUNT(DISTINCT game_date)::int AS game_dates,
          MIN(game_date)::text AS min_game_date,
          MAX(game_date)::text AS max_game_date
        FROM base
        """,
        tuple(base_params),
    ) or {}
    return {
        "total_pred_rows": _to_int(row.get("total_pred_rows")),
        "total_eval_rows": _to_int(row.get("total_eval_rows")),
        "game_dates": _to_int(row.get("game_dates")),
        "min_game_date": row.get("min_game_date"),
        "max_game_date": row.get("max_game_date"),
    }


def _fetch_overall_by_line(base_params: Sequence[Any]) -> List[Dict[str, Any]]:
    rows = pg_fetchall(
        BASE_CTE
        + """
        SELECT
          line,
          COUNT(*)::int AS n_pred,
          COUNT(*) FILTER (WHERE y IS NOT NULL)::int AS n_eval,
          AVG(p_over) FILTER (WHERE y IS NOT NULL) AS avg_p,
          AVG(y::float8) FILTER (WHERE y IS NOT NULL) AS hit_rate,
          AVG((p_over - y)^2) FILTER (WHERE y IS NOT NULL) AS brier
        FROM base
        GROUP BY line
        ORDER BY line
        """,
        tuple(base_params),
    )
    return _format_metric_rows(rows)


def _fetch_monthly_by_line(base_params: Sequence[Any]) -> List[Dict[str, Any]]:
    rows = pg_fetchall(
        BASE_CTE
        + """
        SELECT
          to_char(date_trunc('month', game_date), 'YYYY-MM') AS month,
          line,
          COUNT(*)::int AS n_pred,
          COUNT(*) FILTER (WHERE y IS NOT NULL)::int AS n_eval,
          AVG(p_over) FILTER (WHERE y IS NOT NULL) AS avg_p,
          AVG(y::float8) FILTER (WHERE y IS NOT NULL) AS hit_rate,
          AVG((p_over - y)^2) FILTER (WHERE y IS NOT NULL) AS brier
        FROM base
        GROUP BY month, line
        ORDER BY month, line
        """,
        tuple(base_params),
    )
    out = _format_metric_rows(rows)
    return sorted(out, key=lambda r: (str(r.get("month") or ""), _line_sort(r.get("line"))))


def _fetch_recent_segments(
    base_params: Sequence[Any],
    recent_from_date: str,
    to_date: str,
    segment_min_n: int,
) -> List[Dict[str, Any]]:
    rows = pg_fetchall(
        BASE_CTE
        + """
        , recent AS (
          SELECT *
          FROM base
          WHERE game_date BETWEEN %s::date AND %s::date
            AND y IS NOT NULL
        ),
        segmented AS (
          SELECT
            'role'::text AS segment_type,
            line,
            CASE
              WHEN position_raw = 'D' THEN 'D'
              WHEN position_raw = 'UNK' THEN 'UNK'
              ELSE 'F'
            END AS segment_value,
            p_over,
            y
          FROM recent
          UNION ALL
          SELECT
            'toi'::text AS segment_type,
            line,
            CASE
              WHEN d10_toi_min_avg IS NULL THEN 'missing'
              WHEN d10_toi_min_avg < 12 THEN '<12'
              WHEN d10_toi_min_avg < 16 THEN '12-16'
              WHEN d10_toi_min_avg < 20 THEN '16-20'
              ELSE '20+'
            END AS segment_value,
            p_over,
            y
          FROM recent
          UNION ALL
          SELECT
            'pp_role_source'::text AS segment_type,
            line,
            pp_role_source AS segment_value,
            p_over,
            y
          FROM recent
          UNION ALL
          SELECT
            'expected_sog_bucket'::text AS segment_type,
            line,
            CASE
              WHEN d10_sog_per60 IS NULL OR d10_toi_min_avg IS NULL THEN 'missing'
              WHEN ((d10_sog_per60 * d10_toi_min_avg) / 60.0) < 1.5 THEN '<1.5'
              WHEN ((d10_sog_per60 * d10_toi_min_avg) / 60.0) < 2.5 THEN '1.5-2.5'
              WHEN ((d10_sog_per60 * d10_toi_min_avg) / 60.0) < 3.5 THEN '2.5-3.5'
              ELSE '3.5+'
            END AS segment_value,
            p_over,
            y
          FROM recent
        )
        SELECT
          segment_type,
          line,
          segment_value,
          COUNT(*)::int AS n_pred,
          COUNT(*)::int AS n_eval,
          AVG(p_over) AS avg_p,
          AVG(y::float8) AS hit_rate,
          AVG((p_over - y)^2) AS brier
        FROM segmented
        GROUP BY segment_type, line, segment_value
        HAVING COUNT(*) >= %s
        ORDER BY segment_type, line, segment_value
        """,
        tuple(base_params) + (recent_from_date, to_date, segment_min_n),
    )
    out = _format_metric_rows(rows)
    return sorted(
        out,
        key=lambda r: (str(r.get("segment_type") or ""), _line_sort(r.get("line")), str(r.get("segment_value") or "")),
    )


def _fetch_recent_deciles(
    base_params: Sequence[Any],
    recent_from_date: str,
    to_date: str,
    decile_min_n: int,
) -> List[Dict[str, Any]]:
    rows = pg_fetchall(
        BASE_CTE
        + """
        SELECT
          line,
          width_bucket(p_over, 0.0, 1.0, 10) AS decile,
          COUNT(*)::int AS n_pred,
          COUNT(*)::int AS n_eval,
          MIN(p_over) AS min_p,
          MAX(p_over) AS max_p,
          AVG(p_over) AS avg_p,
          AVG(y::float8) AS hit_rate,
          AVG((p_over - y)^2) AS brier
        FROM base
        WHERE game_date BETWEEN %s::date AND %s::date
          AND y IS NOT NULL
        GROUP BY line, decile
        HAVING COUNT(*) >= %s
        ORDER BY line, decile
        """,
        tuple(base_params) + (recent_from_date, to_date, decile_min_n),
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        avg_p = _to_float(row.get("avg_p"))
        hit_rate = _to_float(row.get("hit_rate"))
        out.append(
            {
                "line": _to_float(row.get("line")),
                "decile": _to_int(row.get("decile")),
                "n_pred": _to_int(row.get("n_pred")),
                "n_eval": _to_int(row.get("n_eval")),
                "min_p": _round(_to_float(row.get("min_p"))),
                "max_p": _round(_to_float(row.get("max_p"))),
                "avg_p": _round(avg_p),
                "hit_rate": _round(hit_rate),
                "gap": _round((avg_p - hit_rate) if avg_p is not None and hit_rate is not None else None),
                "brier": _round(_to_float(row.get("brier"))),
            }
        )
    return sorted(out, key=lambda r: (_line_sort(r.get("line")), _to_int(r.get("decile"))))


def _fetch_recent_player_extremes(
    base_params: Sequence[Any],
    recent_from_date: str,
    to_date: str,
    player_min_n: int,
    player_top_n: int,
) -> Dict[str, List[Dict[str, Any]]]:
    rows = pg_fetchall(
        BASE_CTE
        + """
        SELECT
          line,
          player_id,
          player_name,
          CASE
            WHEN position_raw = 'D' THEN 'D'
            WHEN position_raw = 'UNK' THEN 'UNK'
            ELSE 'F'
          END AS role,
          COUNT(*)::int AS n_pred,
          COUNT(*)::int AS n_eval,
          AVG(p_over) AS avg_p,
          AVG(y::float8) AS hit_rate,
          AVG((p_over - y)^2) AS brier,
          AVG((d10_sog_per60 * d10_toi_min_avg) / 60.0) AS avg_expected_sog
        FROM base
        WHERE game_date BETWEEN %s::date AND %s::date
          AND y IS NOT NULL
        GROUP BY line, player_id, player_name, role
        HAVING COUNT(*) >= %s
        ORDER BY line, AVG(p_over - y) ASC
        """,
        tuple(base_params) + (recent_from_date, to_date, player_min_n),
    )

    formatted: List[Dict[str, Any]] = []
    for row in rows:
        avg_p = _to_float(row.get("avg_p"))
        hit_rate = _to_float(row.get("hit_rate"))
        formatted.append(
            {
                "line": _to_float(row.get("line")),
                "player_id": _to_int(row.get("player_id")),
                "player_name": row.get("player_name"),
                "role": row.get("role"),
                "n_pred": _to_int(row.get("n_pred")),
                "n_eval": _to_int(row.get("n_eval")),
                "avg_p": _round(avg_p),
                "hit_rate": _round(hit_rate),
                "gap": _round((avg_p - hit_rate) if avg_p is not None and hit_rate is not None else None),
                "brier": _round(_to_float(row.get("brier"))),
                "avg_expected_sog": _round(_to_float(row.get("avg_expected_sog"))),
            }
        )

    under = sorted(
        formatted,
        key=lambda r: (_line_sort(r.get("line")), _to_float(r.get("gap")) if _to_float(r.get("gap")) is not None else 999),
    )
    over = sorted(
        formatted,
        key=lambda r: (_line_sort(r.get("line")), -(_to_float(r.get("gap")) if _to_float(r.get("gap")) is not None else -999)),
    )

    def _cap_by_line(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen: Dict[float, int] = {}
        for row in items:
            line = _to_float(row.get("line"))
            if line is None:
                continue
            count = seen.get(line, 0)
            if count >= player_top_n:
                continue
            out.append(row)
            seen[line] = count + 1
        return out

    return {
        "most_underpredicted_by_line": _cap_by_line(under),
        "most_overpredicted_by_line": _cap_by_line(over),
    }


def _pick_worst(rows: Sequence[Dict[str, Any]], limit: int = 8) -> List[Dict[str, Any]]:
    scored = []
    for row in rows:
        gap = _to_float(row.get("gap"))
        if gap is None:
            continue
        scored.append((abs(gap), row))
    scored.sort(key=lambda t: t[0], reverse=True)
    return [r for _, r in scored[:limit]]


def analyze(args: argparse.Namespace) -> Dict[str, Any]:
    lines = _parse_lines(args.lines)
    lines_csv = ",".join(f"{x:g}" for x in lines)
    window = _resolve_window(
        from_date_raw=args.from_date,
        to_date_raw=args.to_date,
        lookback_days=max(1, int(args.lookback_days)),
        recent_days=max(1, int(args.recent_days)),
        model_family=args.model_family,
        model_version=args.model_version,
        lines_csv=lines_csv,
    )
    params = _base_params(args.model_family, args.model_version, lines_csv, window)

    span = _fetch_span_stats(params)
    overall = _fetch_overall_by_line(params)
    monthly = _fetch_monthly_by_line(params)
    recent_segments = _fetch_recent_segments(
        params,
        recent_from_date=window.recent_from_date,
        to_date=window.to_date,
        segment_min_n=max(1, int(args.segment_min_n)),
    )
    recent_deciles = _fetch_recent_deciles(
        params,
        recent_from_date=window.recent_from_date,
        to_date=window.to_date,
        decile_min_n=max(1, int(args.decile_min_n)),
    )
    recent_player_extremes = _fetch_recent_player_extremes(
        params,
        recent_from_date=window.recent_from_date,
        to_date=window.to_date,
        player_min_n=max(1, int(args.player_min_n)),
        player_top_n=max(1, int(args.player_top_n)),
    )

    worst_segments = _pick_worst(recent_segments, limit=max(1, int(args.worst_limit)))
    worst_deciles = _pick_worst(recent_deciles, limit=max(1, int(args.worst_limit)))

    return {
        "ok": True,
        "status": "pass",
        "config": {
            "model_family": args.model_family,
            "model_version": args.model_version,
            "lines": lines,
            "from_date": window.from_date,
            "to_date": window.to_date,
            "lookback_days": int(args.lookback_days),
            "recent_days": int(args.recent_days),
            "recent_from_date": window.recent_from_date,
            "segment_min_n": int(args.segment_min_n),
            "decile_min_n": int(args.decile_min_n),
            "player_min_n": int(args.player_min_n),
            "player_top_n": int(args.player_top_n),
        },
        "span": span,
        "overall_by_line": overall,
        "monthly_by_line": monthly,
        "recent_segments": recent_segments,
        "recent_deciles": recent_deciles,
        "recent_player_extremes": recent_player_extremes,
        "worst_recent_segments_by_abs_gap": worst_segments,
        "worst_recent_deciles_by_abs_gap": worst_deciles,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Layered quality analysis for NHL SOG predictions.")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--lines", default="1.5,2.5,3.5")
    ap.add_argument("--from-date", default=None, help="YYYY-MM-DD (inclusive). Default: latest_date - lookback_days.")
    ap.add_argument("--to-date", default=None, help="YYYY-MM-DD (inclusive). Default: latest available date.")
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--recent-days", type=int, default=14)
    ap.add_argument("--segment-min-n", type=int, default=80)
    ap.add_argument("--decile-min-n", type=int, default=25)
    ap.add_argument("--player-min-n", type=int, default=4)
    ap.add_argument("--player-top-n", type=int, default=10)
    ap.add_argument("--worst-limit", type=int, default=8)
    ap.add_argument("--output", default="", help="Optional output path for JSON report.")
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        payload = analyze(args)
    except Exception as exc:
        payload = {
            "ok": False,
            "status": "fail",
            "error": str(exc),
        }
        print(json.dumps(payload, indent=2, default=str))
        return 1

    rendered = json.dumps(payload, indent=2, default=str)
    print(rendered)

    out_path = (args.output or "").strip()
    if out_path:
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(rendered + "\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
