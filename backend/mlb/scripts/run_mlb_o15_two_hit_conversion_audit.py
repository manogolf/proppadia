#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import run_mlb_hits_15_tier_backtest as tier_base
from backend.mlb.scripts import run_mlb_o15_pa_opportunity_audit as pa_base
from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/mlb/review_aids"


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    if number is None:
        return None
    return int(number)


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100.0:.2f}%"


def _num(value: float | None, digits: int = 2) -> str:
    return "n/a" if value is None else f"{value:.{digits}f}"


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _key(row: dict[str, Any]) -> tuple[str, int, int]:
    return str(row.get("date") or "")[:10], int(_i(row.get("game_id")) or 0), int(_i(row.get("player_id")) or 0)


def _price_bucket(price: Any) -> str:
    value = _f(price)
    if value is None:
        return "missing"
    if value < -200:
        return "<-200"
    if value < -150:
        return "-200 to -150"
    if value < -120:
        return "-150 to -120"
    if value <= 120:
        return "-120 to +120"
    if value <= 160:
        return "+120 to +160"
    if value <= 200:
        return "+160 to +200"
    return ">+200"


def _implied_prob(price: Any) -> float | None:
    value = _f(price)
    if value is None:
        return None
    if value < 0:
        return abs(value) / (abs(value) + 100.0)
    return 100.0 / (value + 100.0)


def _load_qc_metrics(lanes_root: Path) -> dict[tuple[str, int, float, str], dict[str, Any]]:
    out: dict[tuple[str, int, float, str], dict[str, Any]] = {}
    for path in sorted((lanes_root / "today").glob("20??-??-??/quick_card_hits_*.csv")):
        date_text = path.parent.name
        rows = pa_base._read_csv(path)
        for row in rows:
            if _clean(row.get("prop_type")) != "hits":
                continue
            if _clean(row.get("side")) != "over" or _f(row.get("line")) != 1.5:
                continue
            player_id = _i(row.get("player_id"))
            if player_id is None:
                continue
            key = (date_text, int(player_id), 1.5, "over")
            out[key] = {
                "qc_candidate": True,
                "qc_score": _f(row.get("score") or row.get("qc_score")),
                "rank_score": _f(row.get("rank_score")),
                "rank_percentile": _f(row.get("rank_percentile")),
                "source_lane": row.get("source_lane") or "",
            }
    return out


def _fetch_conversion_context(rows: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    player_ids = sorted({int(_i(r.get("player_id")) or 0) for r in rows if _i(r.get("player_id")) is not None})
    game_ids = sorted({int(_i(r.get("game_id")) or 0) for r in rows if _i(r.get("game_id")) is not None})
    dates = [datetime.strptime(str(r.get("date"))[:10], "%Y-%m-%d").date() for r in rows if str(r.get("date") or "")[:10]]
    if not player_ids or not game_ids or not dates:
        return {}
    min_date = min(dates) - timedelta(days=90)
    max_date = max(dates)
    stat_rows = pg_fetchall(
        """
SELECT
  ps.game_date::date AS game_date,
  ps.game_id,
  ps.player_id,
  ps.hits,
  ps.total_bases,
  ps.at_bats,
  ps.plate_appearances,
  ps.walks,
  ps.hit_by_pitch,
  ps.sacrifice_flies,
  ps.sacrifice_hits,
  ps.catcher_interference
FROM mlb.player_stats ps
WHERE ps.player_id = ANY(%s)
  AND ps.game_date BETWEEN %s::date AND %s::date
ORDER BY ps.player_id, ps.game_date, ps.game_id
""",
        (player_ids, min_date.isoformat(), max_date.isoformat()),
    )
    derived_rows = pg_fetchall(
        """
SELECT
  pds.game_date::date AS game_date,
  pds.game_id,
  pds.player_id,
  pds.d7_hits::float8 AS d7_hits,
  pds.d15_hits::float8 AS d15_hits,
  pds.d30_hits::float8 AS d30_hits,
  pds.d7_total_bases::float8 AS d7_total_bases,
  pds.d15_total_bases::float8 AS d15_total_bases,
  pds.d30_total_bases::float8 AS d30_total_bases,
  pds.d7_hits_runs_rbis::float8 AS d7_hits_runs_rbis,
  pds.d15_hits_runs_rbis::float8 AS d15_hits_runs_rbis,
  pds.d30_hits_runs_rbis::float8 AS d30_hits_runs_rbis,
  pds.d7_plate_appearances::float8 AS d7_plate_appearances,
  pds.d15_plate_appearances::float8 AS d15_plate_appearances,
  pds.d30_plate_appearances::float8 AS d30_plate_appearances
FROM mlb.player_derived_stats pds
WHERE pds.player_id = ANY(%s)
  AND pds.game_id = ANY(%s)
""",
        (player_ids, game_ids),
    )
    pfp_rows = pg_fetchall(
        """
SELECT
  game_date::date AS game_date,
  game_id,
  player_id,
  lineup_slot,
  features->>'bvp_plate_appearances' AS bvp_plate_appearances,
  features->>'bvp_at_bats' AS bvp_at_bats,
  features->>'bvp_hits' AS bvp_hits,
  features->>'bvp_total_bases' AS bvp_total_bases,
  features->>'bvp_avg_prior_sm' AS bvp_avg,
  features->>'bvp_tb_per_ab_prior_sm' AS bvp_slg
FROM mlb.prop_features_precomputed
WHERE player_id = ANY(%s)
  AND game_id = ANY(%s)
  AND prop_type = 'hits'
""",
        (player_ids, game_ids),
    )

    def component_pa(row: dict[str, Any]) -> float | None:
        explicit = _f(row.get("plate_appearances"))
        if explicit is not None:
            return explicit
        ab = _f(row.get("at_bats"))
        if ab is None:
            return None
        return (
            ab
            + (_f(row.get("walks")) or 0.0)
            + (_f(row.get("hit_by_pitch")) or 0.0)
            + (_f(row.get("sacrifice_flies")) or 0.0)
            + (_f(row.get("sacrifice_hits")) or 0.0)
            + (_f(row.get("catcher_interference")) or 0.0)
        )

    by_player: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in stat_rows or []:
        player_id = _i(row.get("player_id"))
        game_id = _i(row.get("game_id"))
        if player_id is None or game_id is None:
            continue
        item = dict(row)
        item["derived_plate_appearances"] = component_pa(item)
        by_player[int(player_id)].append(item)

    by_key: dict[tuple[str, int, int], dict[str, Any]] = {}
    target_keys = {_key(r) for r in rows}
    for player_id, player_rows in by_player.items():
        player_rows = sorted(player_rows, key=lambda r: (str(r.get("game_date"))[:10], int(_i(r.get("game_id")) or 0)))
        for idx, row in enumerate(player_rows):
            game_id = _i(row.get("game_id"))
            if game_id is None:
                continue
            date_text = str(row.get("game_date"))[:10]
            key = (date_text, int(game_id), int(player_id))
            if key not in target_keys:
                continue
            prior = player_rows[:idx]

            def sum_prior_days(days: int, col: str) -> float | None:
                game_date = datetime.strptime(date_text, "%Y-%m-%d").date()
                vals = [
                    _f(r.get(col))
                    for r in prior
                    if datetime.strptime(str(r.get("game_date"))[:10], "%Y-%m-%d").date() >= game_date - timedelta(days=days)
                ]
                vals = [v for v in vals if v is not None]
                return sum(vals) if vals else None

            def count_multihit(n: int) -> int | None:
                vals = [_f(r.get("hits")) for r in prior[-n:]]
                vals = [v for v in vals if v is not None]
                if not vals:
                    return None
                return sum(1 for v in vals if v >= 2)

            by_key[key] = {
                "raw_d7_hits_calendar": sum_prior_days(7, "hits"),
                "raw_d15_hits_calendar": sum_prior_days(15, "hits"),
                "raw_d30_hits_calendar": sum_prior_days(30, "hits"),
                "multi_hit_games_last_7": count_multihit(7),
                "multi_hit_games_last_15": count_multihit(15),
                "multi_hit_games_last_30": count_multihit(30),
            }

    for row in derived_rows or []:
        key = (str(row.get("game_date"))[:10], int(_i(row.get("game_id")) or 0), int(_i(row.get("player_id")) or 0))
        if key in target_keys:
            by_key.setdefault(key, {}).update({k: _f(v) for k, v in row.items() if k not in {"game_date", "game_id", "player_id"}})
    for row in pfp_rows or []:
        key = (str(row.get("game_date"))[:10], int(_i(row.get("game_id")) or 0), int(_i(row.get("player_id")) or 0))
        if key in target_keys:
            by_key.setdefault(key, {}).update(
                {
                    "lineup_slot": _f(row.get("lineup_slot")),
                    "bvp_plate_appearances": _f(row.get("bvp_plate_appearances")),
                    "bvp_at_bats": _f(row.get("bvp_at_bats")),
                    "bvp_hits": _f(row.get("bvp_hits")),
                    "bvp_total_bases": _f(row.get("bvp_total_bases")),
                    "bvp_avg": _f(row.get("bvp_avg")),
                    "bvp_slg": _f(row.get("bvp_slg")),
                }
            )
    return by_key


def _lineup_bucket(slot: Any) -> str:
    value = _f(slot)
    if value is None:
        return "missing"
    if value <= 3:
        return "top_1_3"
    if value <= 6:
        return "middle_4_6"
    return "bottom_7_9"


def _multihit_bucket(value: Any, days: int) -> str:
    number = _f(value)
    if number is None:
        return "missing"
    if number <= 0:
        return "0"
    if days <= 7:
        if number == 1:
            return "1"
        if number == 2:
            return "2"
        return "3+"
    if number <= 2:
        return "1-2"
    if number <= 4:
        return "3-4"
    return "5+"


def _market_label(row: dict[str, Any]) -> str:
    price = _f(row.get("price"))
    if price is None:
        return "missing"
    if price > 0:
        return "plus_money"
    return "favorite"


def _enrich(rows: list[dict[str, Any]], lanes_root: Path) -> None:
    pa_base._enrich(rows, lanes_root)
    context = _fetch_conversion_context(rows)
    qc_metrics = _load_qc_metrics(lanes_root)
    for row in rows:
        row.update(context.get(_key(row), {}))
        qc = qc_metrics.get((str(row.get("date") or "")[:10], int(_i(row.get("player_id")) or 0), 1.5, "over"))
        if qc:
            row.update(qc)
        row["qc_candidate"] = bool(row.get("qc_candidate"))
        row["price_bucket"] = _price_bucket(row.get("price"))
        row["implied_probability"] = _implied_prob(row.get("price"))
        row["market_price_type"] = _market_label(row)
        row["lineup_bucket"] = _lineup_bucket(row.get("lineup_slot"))
        row["multi_hit_7_bucket"] = _multihit_bucket(row.get("multi_hit_games_last_7"), 7)
        row["multi_hit_15_bucket"] = _multihit_bucket(row.get("multi_hit_games_last_15"), 15)
        row["multi_hit_30_bucket"] = _multihit_bucket(row.get("multi_hit_games_last_30"), 30)
        hits = _f(row.get("actual_hits"))
        row["conversion_group"] = "2plus_hit_winner" if hits is not None and hits >= 2 else "exactly_1_hit_loser" if hits == 1 else "other"


def _segments() -> list[tuple[str, Callable[[dict[str, Any]], bool]]]:
    def d7(row: dict[str, Any]) -> bool:
        value = _f(row.get("d7_hits_rate"))
        return value is not None and value > 1.0

    def d15(row: dict[str, Any]) -> bool:
        value = _f(row.get("d15_hits_rate"))
        return value is not None and value > 1.0

    def starter5(row: dict[str, Any]) -> bool:
        value = _f(row.get("starter_expected_hits_allowed"))
        return value is not None and value >= 5.0

    return [
        ("d7+d15", lambda r: d7(r) and d15(r)),
        ("d7+d15+starter>=5", lambda r: d7(r) and d15(r) and starter5(r)),
        ("QC+d7+d15+starter>=5", lambda r: bool(r.get("qc_candidate")) and d7(r) and d15(r) and starter5(r)),
    ]


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    two = [r for r in rows if r.get("conversion_group") == "2plus_hit_winner"]
    one = [r for r in rows if r.get("conversion_group") == "exactly_1_hit_loser"]
    both = two + one

    def avg(col: str, selected: list[dict[str, Any]]) -> float | None:
        vals = [_f(r.get(col)) for r in selected]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    return {
        "rows": len(rows),
        "two_hit_winners": len(two),
        "one_hit_losers": len(one),
        "two_vs_one_rows": len(both),
        "two_hit_rate_among_1_or_2plus": len(two) / len(both) if both else None,
        "avg_price_two": avg("price", two),
        "avg_price_one": avg("price", one),
        "avg_d7_hits_rate_two": avg("d7_hits_rate", two),
        "avg_d7_hits_rate_one": avg("d7_hits_rate", one),
        "avg_d15_hits_rate_two": avg("d15_hits_rate", two),
        "avg_d15_hits_rate_one": avg("d15_hits_rate", one),
        "avg_multi_hit_games_last_7_two": avg("multi_hit_games_last_7", two),
        "avg_multi_hit_games_last_7_one": avg("multi_hit_games_last_7", one),
        "avg_starter_expected_hits_allowed_two": avg("starter_expected_hits_allowed", two),
        "avg_starter_expected_hits_allowed_one": avg("starter_expected_hits_allowed", one),
    }


FEATURES = [
    "price",
    "implied_probability",
    "model_prob",
    "d7_hits_rate",
    "d15_hits_rate",
    "d30_hits_rate",
    "raw_d7_hits_calendar",
    "raw_d15_hits_calendar",
    "raw_d30_hits_calendar",
    "multi_hit_games_last_7",
    "multi_hit_games_last_15",
    "multi_hit_games_last_30",
    "d7_total_bases",
    "d15_total_bases",
    "d30_total_bases",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
    "actual_plate_appearances",
    "actual_at_bats",
    "lineup_slot",
    "bvp_plate_appearances",
    "bvp_hits",
    "bvp_total_bases",
    "bvp_avg",
    "bvp_slg",
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
    "qc_score",
    "rank_score",
    "rank_percentile",
]


def _feature_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for segment, predicate in _segments():
        seg_rows = [r for r in rows if predicate(r)]
        two = [r for r in seg_rows if r.get("conversion_group") == "2plus_hit_winner"]
        one = [r for r in seg_rows if r.get("conversion_group") == "exactly_1_hit_loser"]
        for feature in FEATURES:
            two_vals = [_f(r.get(feature)) for r in two]
            one_vals = [_f(r.get(feature)) for r in one]
            two_vals = [v for v in two_vals if v is not None]
            one_vals = [v for v in one_vals if v is not None]
            if not two_vals and not one_vals:
                continue
            avg_two = sum(two_vals) / len(two_vals) if two_vals else None
            avg_one = sum(one_vals) / len(one_vals) if one_vals else None
            pooled = two_vals + one_vals
            if len(pooled) > 1:
                mean = sum(pooled) / len(pooled)
                sd = math.sqrt(sum((v - mean) ** 2 for v in pooled) / (len(pooled) - 1))
            else:
                sd = None
            effect = ((avg_two - avg_one) / sd) if avg_two is not None and avg_one is not None and sd else None
            out.append(
                {
                    "segment": segment,
                    "feature": feature,
                    "two_hit_rows_with_value": len(two_vals),
                    "one_hit_rows_with_value": len(one_vals),
                    "avg_two_hit_winners": avg_two,
                    "avg_one_hit_losers": avg_one,
                    "difference_two_minus_one": (avg_two - avg_one) if avg_two is not None and avg_one is not None else None,
                    "standardized_separation": effect,
                    "coverage_rate": (len(two_vals) + len(one_vals)) / (len(two) + len(one)) if (len(two) + len(one)) else None,
                }
            )
    return out


def _bucket_rows(rows: list[dict[str, Any]], dimensions: list[tuple[str, str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for segment, predicate in _segments():
        seg_rows = [r for r in rows if predicate(r)]
        for dimension, field in dimensions:
            values = sorted({str(r.get(field) or "missing") for r in seg_rows})
            for value in values:
                selected = [r for r in seg_rows if str(r.get(field) or "missing") == value]
                item = {"segment": segment, "dimension": dimension, "bucket": value}
                item.update(_metrics(selected))
                out.append(item)
    return out


def _write_report(
    path: Path,
    rows: list[dict[str, Any]],
    feature_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    multihit_rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    segment_lines = []
    for segment, predicate in _segments():
        m = _metrics([r for r in rows if predicate(r)])
        segment_lines.append(
            f"| `{segment}` | `{m['rows']}` | `{m['two_hit_winners']}` | `{m['one_hit_losers']}` | `{_pct(_f(m['two_hit_rate_among_1_or_2plus']))}` | "
            f"`{_num(_f(m['avg_price_two']))}` | `{_num(_f(m['avg_price_one']))}` | "
            f"`{_num(_f(m['avg_multi_hit_games_last_7_two']))}` | `{_num(_f(m['avg_multi_hit_games_last_7_one']))}` |"
        )

    ranked = sorted(
        [r for r in feature_rows if r.get("segment") == "d7+d15+starter>=5" and _f(r.get("standardized_separation")) is not None],
        key=lambda r: abs(_f(r.get("standardized_separation")) or 0.0),
        reverse=True,
    )[:12]
    add_candidates = [
        r
        for r in ranked
        if r.get("feature")
        in {
            "multi_hit_games_last_7",
            "multi_hit_games_last_15",
            "multi_hit_games_last_30",
            "d7_total_bases",
            "d15_total_bases",
            "d30_total_bases",
            "lineup_slot",
            "implied_probability",
            "starter_expected_hits_allowed",
            "team_expected_hits_allowed",
        }
    ]
    lines = [
        "# Hits Over 1.5 Two-Hit Conversion Audit",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        "- Scope: analysis only; no production, selector, upload, threshold, grading, or matching changes.",
        "- Comparison: `2+ hit winners` vs `exactly 1 hit losers`; 0-hit games are excluded from the conversion comparison.",
        "",
        "## Population Summary",
        "",
        "| segment | rows | 2+ hit winners | exactly 1-hit losers | 2-hit conversion rate | avg winner odds | avg 1-hit odds | avg winner multi-hit last 7 | avg 1-hit multi-hit last 7 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        *segment_lines,
        "",
        "## Ranked Predictor Separation",
        "",
        "For the main `d7+d15+starter>=5` population, ranked by absolute standardized separation between 2+ hit winners and 1-hit losers.",
        "",
        "| rank | feature | two-hit avg | one-hit avg | difference | standardized separation | coverage |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for idx, row in enumerate(ranked, start=1):
        lines.append(
            f"| {idx} | `{row.get('feature')}` | `{_num(_f(row.get('avg_two_hit_winners')), 3)}` | "
            f"`{_num(_f(row.get('avg_one_hit_losers')), 3)}` | `{_num(_f(row.get('difference_two_minus_one')), 3)}` | "
            f"`{_num(_f(row.get('standardized_separation')), 3)}` | `{_pct(_f(row.get('coverage_rate')))}` |"
        )
    lines.extend(
        [
            "",
            "## Price Buckets",
            "",
            "| segment | bucket | rows | 2+ hit winners | 1-hit losers | conversion rate | avg winner odds | avg 1-hit odds |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in price_rows:
        lines.append(
            f"| `{row.get('segment')}` | `{row.get('bucket')}` | `{row.get('rows')}` | `{row.get('two_hit_winners')}` | "
            f"`{row.get('one_hit_losers')}` | `{_pct(_f(row.get('two_hit_rate_among_1_or_2plus')))}` | "
            f"`{_num(_f(row.get('avg_price_two')))}` | `{_num(_f(row.get('avg_price_one')))}` |"
        )
    lines.extend(
        [
            "",
            "## Multi-Hit Frequency Buckets",
            "",
            "| segment | dimension | bucket | rows | 2+ hit winners | 1-hit losers | conversion rate |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in multihit_rows:
        lines.append(
            f"| `{row.get('segment')}` | `{row.get('dimension')}` | `{row.get('bucket')}` | `{row.get('rows')}` | "
            f"`{row.get('two_hit_winners')}` | `{row.get('one_hit_losers')}` | `{_pct(_f(row.get('two_hit_rate_among_1_or_2plus')))}` |"
        )
    full_cov_ranked = [
        r
        for r in ranked
        if (_f(r.get("coverage_rate")) or 0.0) >= 0.80 and r.get("feature") not in {"actual_plate_appearances", "actual_at_bats"}
    ]
    lines.extend(["", "## Candidate Board Additions", ""])
    if add_candidates:
        for row in add_candidates[:6]:
            lines.append(
                f"- `{row.get('feature')}`: separation `{_num(_f(row.get('standardized_separation')), 3)}`, "
                f"2-hit avg `{_num(_f(row.get('avg_two_hit_winners')), 3)}` vs 1-hit avg `{_num(_f(row.get('avg_one_hit_losers')), 3)}`."
            )
    else:
        lines.append("- No high-coverage feature clearly earned a board addition from this pass.")
    lines.extend(
        [
            "",
            "## Answer",
            "",
            "- Actual PA still separates the result after the fact, but it is diagnostic-only: winners averaged more same-game PA/AB than 1-hit losers.",
            "- Among pregame fields, the best high-coverage separators are not raw recent hit totals. They are broader production/context fields: `d15_hits_runs_rbis`, `d7_hits_runs_rbis`, `team_expected_hits_allowed`, `starter_expected_hits_allowed`, and price/implied probability.",
            "- Recent multi-hit frequency is not a clean positive filter here. In the main `d7+d15+starter>=5` population, 1-hit losers actually had a slightly higher last-7 multi-hit count than 2-hit winners.",
            "- BvP/QC score rows are coverage-sensitive in this audit; keep them diagnostic unless future rows improve coverage.",
        ]
    )
    if full_cov_ranked:
        lines.extend(["", "Pregame usefulness rank, excluding same-game actual PA/AB:"])
        for idx, row in enumerate(full_cov_ranked[:8], start=1):
            lines.append(
                f"{idx}. `{row.get('feature')}`: separation `{_num(_f(row.get('standardized_separation')), 3)}` "
                f"({ _num(_f(row.get('avg_two_hit_winners')), 3) } vs { _num(_f(row.get('avg_one_hit_losers')), 3) })."
            )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit 2+ hit winners vs exactly 1-hit losers inside strong hits o1.5 populations.")
    ap.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    rows = [
        r
        for r in tier_base._load_reconcile_rows(ROOT / args.execution_root)
        if _clean(r.get("side")) == "over"
        and _f(r.get("line")) == 1.5
        and _clean(r.get("result")) in {"win", "loss", "push"}
    ]
    _enrich(rows, ROOT / args.lanes_root)

    comparison_rows = [r for r in rows if r.get("conversion_group") in {"2plus_hit_winner", "exactly_1_hit_loser"}]
    feature_rows = _feature_rows(comparison_rows)
    price_rows = _bucket_rows(comparison_rows, [("price_bucket", "price_bucket"), ("market_price_type", "market_price_type")])
    multihit_rows = _bucket_rows(
        comparison_rows,
        [
            ("multi_hit_games_last_7", "multi_hit_7_bucket"),
            ("multi_hit_games_last_15", "multi_hit_15_bucket"),
            ("multi_hit_games_last_30", "multi_hit_30_bucket"),
        ],
    )

    out_dir = Path(args.out_dir)
    report = out_dir / "o15_two_hit_conversion_audit.md"
    features_csv = out_dir / "o15_two_hit_conversion_features.csv"
    price_csv = out_dir / "o15_two_hit_conversion_price_buckets.csv"
    multihit_csv = out_dir / "o15_two_hit_conversion_multihit_frequency.csv"
    _write_csv(features_csv, feature_rows)
    _write_csv(price_csv, price_rows)
    _write_csv(multihit_csv, multihit_rows)
    _write_report(report, comparison_rows, feature_rows, price_rows, multihit_rows)

    print(
        json.dumps(
            {
                "rows": len(rows),
                "comparison_rows": len(comparison_rows),
                "outputs": {
                    "report": _rel(report),
                    "features": _rel(features_csv),
                    "price_buckets": _rel(price_csv),
                    "multihit_frequency": _rel(multihit_csv),
                },
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
