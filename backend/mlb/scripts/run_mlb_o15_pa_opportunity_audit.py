#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import run_mlb_hits_15_tier_backtest as tier_base
from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
OUT_DIR = ROOT / "artifacts/analysis/mlb/review_aids"
WINDOWS = ("full_history", "last_30", "last_14", "last_7")


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


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


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


def _window_labels(date_text: str, latest: str) -> list[str]:
    out = ["full_history"]
    try:
        d = datetime.strptime(date_text, "%Y-%m-%d").date()
        latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
    except Exception:
        return out
    delta = (latest_d - d).days
    if delta <= 29:
        out.append("last_30")
    if delta <= 13:
        out.append("last_14")
    if delta <= 6:
        out.append("last_7")
    return out


def _key(row: dict[str, Any]) -> tuple[str, int, int]:
    return str(row.get("date") or "")[:10], int(_i(row.get("game_id")) or 0), int(_i(row.get("player_id")) or 0)


def _line_key(value: Any) -> str:
    line = _f(value)
    return f"{line:.1f}" if line is not None else ""


def _load_qc_flags(lanes_root: Path) -> set[tuple[str, str, str]]:
    flags: set[tuple[str, str, str]] = set()
    for path in sorted((lanes_root / "today").glob("20??-??-??/quick_card_hits_*.csv")):
        date_text = path.parent.name
        for row in _read_csv(path):
            if _clean(row.get("prop_type")) != "hits":
                continue
            if _clean(row.get("side")) != "over" or _line_key(row.get("line")) != "1.5":
                continue
            pid = _i(row.get("player_id"))
            if pid is None:
                continue
            flags.add((date_text, str(pid), "1.5"))
    return flags


def _fetch_opportunity_context(rows: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    player_ids = sorted({int(_i(r.get("player_id")) or 0) for r in rows if _i(r.get("player_id")) is not None})
    game_ids = sorted({int(_i(r.get("game_id")) or 0) for r in rows if _i(r.get("game_id")) is not None})
    if not player_ids or not game_ids:
        return {}
    dates = [datetime.strptime(str(r.get("date"))[:10], "%Y-%m-%d").date() for r in rows if str(r.get("date") or "")[:10]]
    min_date = min(dates) - timedelta(days=90)
    max_date = max(dates)
    db_rows = pg_fetchall(
        """
SELECT
  ps.game_date::date AS game_date,
  ps.game_id,
  ps.player_id,
  ps.hits,
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
    pfp_rows = pg_fetchall(
        """
SELECT game_date::date AS game_date, game_id, player_id, lineup_slot
FROM mlb.prop_features_precomputed
WHERE player_id = ANY(%s)
  AND game_id = ANY(%s)
  AND prop_type = 'hits'
""",
        (player_ids, game_ids),
    )
    lineup_by_key: dict[tuple[str, int, int], Any] = {}
    for row in pfp_rows or []:
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        if game_id is None or player_id is None:
            continue
        lineup_by_key[(str(row.get("game_date"))[:10], game_id, player_id)] = row.get("lineup_slot")

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

    raw_by_player: dict[int, list[dict[str, Any]]] = {}
    for row in db_rows or []:
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        if game_id is None or player_id is None:
            continue
        item = dict(row)
        item["derived_plate_appearances"] = component_pa(item)
        raw_by_player.setdefault(player_id, []).append(item)

    out: dict[tuple[str, int, int], dict[str, Any]] = {}
    target_keys = {(str(r.get("date"))[:10], int(_i(r.get("game_id")) or 0), int(_i(r.get("player_id")) or 0)) for r in rows}
    for player_id, player_rows in raw_by_player.items():
        player_rows = sorted(player_rows, key=lambda r: (str(r.get("game_date"))[:10], int(_i(r.get("game_id")) or 0)))
        for idx, row in enumerate(player_rows):
            game_id = _i(row.get("game_id"))
            if game_id is None:
                continue
            date_text = str(row.get("game_date"))[:10]
            key = (date_text, game_id, player_id)
            if key not in target_keys:
                continue
            prior = [r for r in player_rows[:idx] if _f(r.get("derived_plate_appearances")) is not None]

            def avg_prior(n: int, col: str) -> float | None:
                vals = [_f(r.get(col)) for r in prior[-n:]]
                vals = [v for v in vals if v is not None]
                return sum(vals) / len(vals) if vals else None

            item = dict(row)
            item.update(
                {
                    "d7_plate_appearances": avg_prior(7, "derived_plate_appearances"),
                    "d15_plate_appearances": avg_prior(15, "derived_plate_appearances"),
                    "d30_plate_appearances": avg_prior(30, "derived_plate_appearances"),
                    "d7_at_bats": avg_prior(7, "at_bats"),
                    "d15_at_bats": avg_prior(15, "at_bats"),
                    "d30_at_bats": avg_prior(30, "at_bats"),
                    "lineup_slot": lineup_by_key.get(key),
                    "pa_derivation_source": "plate_appearances_column"
                    if _f(row.get("plate_appearances")) is not None
                    else "components_ab_bb_hbp_sf_sh_ci",
                }
            )
            out[key] = item
    return out


def _actual_pa_bucket(pa: float | None) -> str:
    if pa is None:
        return "missing"
    if pa <= 3:
        return "<=3"
    if abs(pa - 4) < 1e-9:
        return "4"
    return ">=5"


def _lineup_bucket(slot: float | None) -> str:
    if slot is None:
        return "unknown"
    if slot <= 3:
        return "top_1_3"
    if slot <= 6:
        return "middle_4_6"
    return "bottom_7_9"


def _pa_rate_bucket(value: float | None) -> str:
    if value is None:
        return "missing"
    if value < 3.5:
        return "<3.5"
    if value < 4.0:
        return "3.5-3.99"
    if value < 4.5:
        return "4.0-4.49"
    return ">=4.5"


def _pa_stability_bucket(row: dict[str, Any]) -> str:
    vals = [_f(row.get("d7_plate_appearances")), _f(row.get("d15_plate_appearances")), _f(row.get("d30_plate_appearances"))]
    vals = [v for v in vals if v is not None]
    if len(vals) < 2:
        return "missing"
    spread = max(vals) - min(vals)
    if min(vals) >= 4.0 and spread <= 0.5:
        return "stable_4plus"
    if min(vals) >= 4.0:
        return "4plus_volatile"
    if spread <= 0.5:
        return "stable_below4"
    return "mixed_or_volatile"


def _loss_bucket(row: dict[str, Any]) -> str:
    result = _clean(row.get("result"))
    if result != "loss":
        return "not_loss"
    hits = _f(row.get("actual_hits"))
    pa = _f(row.get("actual_plate_appearances"))
    if hits == 0:
        return "0_hits"
    if hits == 1:
        if pa is None:
            return "exactly_1_hit_pa_missing"
        if pa <= 3:
            return "exactly_1_hit_pa_le_3"
        if abs(pa - 4) < 1e-9:
            return "exactly_1_hit_pa_4"
        return "exactly_1_hit_pa_ge_5"
    return "other_loss"


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for r in rows if _clean(r.get("result")) == "win")
    losses = sum(1 for r in rows if _clean(r.get("result")) == "loss")
    pushes = sum(1 for r in rows if _clean(r.get("result")) == "push")
    resolved = wins + losses + pushes
    units = sum(_f(r.get("units")) or 0.0 for r in rows if _clean(r.get("result")) in {"win", "loss", "push"})

    def avg(col: str) -> float | None:
        vals = [_f(r.get(col)) for r in rows]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    return {
        "rows": len(rows),
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / resolved if resolved else None,
        "units": units,
        "avg_odds": avg("price"),
        "avg_actual_pa": avg("actual_plate_appearances"),
        "avg_actual_hits": avg("actual_hits"),
        "avg_d7_hits_rate": avg("d7_hits_rate"),
        "avg_d15_hits_rate": avg("d15_hits_rate"),
        "avg_d7_plate_appearances": avg("d7_plate_appearances"),
        "avg_d15_plate_appearances": avg("d15_plate_appearances"),
        "avg_d30_plate_appearances": avg("d30_plate_appearances"),
        "avg_starter_expected_hits_allowed": avg("starter_expected_hits_allowed"),
    }


def _summarize_dimension(rows: list[dict[str, Any]], latest: str, dimension: str, field: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for window in WINDOWS:
        wrows = [r for r in rows if window in _window_labels(str(r.get("date") or ""), latest)]
        for value in sorted({str(r.get(field) or "missing") for r in wrows}):
            selected = [r for r in wrows if str(r.get(field) or "missing") == value]
            item = {"window": window, "dimension": dimension, "bucket": value}
            item.update(_metrics(selected))
            out.append(item)
    return out


def _summarize_segments(
    rows: list[dict[str, Any]],
    latest: str,
    specs: list[tuple[str, Callable[[dict[str, Any]], bool]]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for window in WINDOWS:
        wrows = [r for r in rows if window in _window_labels(str(r.get("date") or ""), latest)]
        for label, predicate in specs:
            selected = [r for r in wrows if predicate(r)]
            item = {"window": window, "segment": label}
            item.update(_metrics(selected))
            out.append(item)
    return out


def _enrich(rows: list[dict[str, Any]], lanes_root: Path) -> None:
    context = _fetch_opportunity_context(rows)
    qc_flags = _load_qc_flags(lanes_root)
    for row in rows:
        ctx = context.get(_key(row), {})
        row["actual_hits"] = _f(ctx.get("hits") if ctx else row.get("actual_value"))
        row["actual_at_bats"] = _f(ctx.get("at_bats"))
        row["actual_plate_appearances"] = _f(ctx.get("derived_plate_appearances"))
        row["pa_derivation_source"] = ctx.get("pa_derivation_source") or ""
        row["actual_pa_bucket"] = _actual_pa_bucket(_f(row.get("actual_plate_appearances")))
        row["d7_plate_appearances"] = _f(ctx.get("d7_plate_appearances"))
        row["d15_plate_appearances"] = _f(ctx.get("d15_plate_appearances"))
        row["d30_plate_appearances"] = _f(ctx.get("d30_plate_appearances"))
        row["d7_at_bats"] = _f(ctx.get("d7_at_bats"))
        row["d15_at_bats"] = _f(ctx.get("d15_at_bats"))
        row["d30_at_bats"] = _f(ctx.get("d30_at_bats"))
        row["lineup_slot"] = _f(ctx.get("lineup_slot"))
        row["lineup_bucket"] = _lineup_bucket(_f(row.get("lineup_slot")))
        row["d7_pa_bucket"] = _pa_rate_bucket(_f(row.get("d7_plate_appearances")))
        row["d15_pa_bucket"] = _pa_rate_bucket(_f(row.get("d15_plate_appearances")))
        row["d30_pa_bucket"] = _pa_rate_bucket(_f(row.get("d30_plate_appearances")))
        row["pa_stability_bucket"] = _pa_stability_bucket(row)
        row["loss_bucket"] = _loss_bucket(row)
        row["qc_candidate"] = (
            str(row.get("date") or "")[:10],
            str(int(_i(row.get("player_id")) or 0)),
            "1.5",
        ) in qc_flags


def _write_report(
    path: Path,
    rows: list[dict[str, Any]],
    bucket_rows: list[dict[str, Any]],
    funnel_rows: list[dict[str, Any]],
    latest: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    def line(row: dict[str, Any], label_key: str) -> str:
        return (
            f"| `{row.get(label_key)}` | `{row.get('rows')}` | `{row.get('resolved')}` | "
            f"`{row.get('wins')}` | `{row.get('losses')}` | `{_pct(_f(row.get('wr')))}` | "
            f"`{_pct(_f(row.get('roi')))}` | `{_num(_f(row.get('units')))}` | "
            f"`{_num(_f(row.get('avg_odds')))}` | `{_num(_f(row.get('avg_actual_pa')))}` | "
            f"`{_num(_f(row.get('avg_d7_plate_appearances')))}` | "
            f"`{_num(_f(row.get('avg_d15_plate_appearances')))}` |"
        )

    all_metrics = _metrics(rows)
    loss_counts = Counter(str(r.get("loss_bucket") or "missing") for r in rows if _clean(r.get("result")) == "loss")
    actual_pa_full = [r for r in bucket_rows if r.get("window") == "full_history" and r.get("dimension") == "actual_pa_bucket"]
    loss_bucket_full = [r for r in bucket_rows if r.get("window") == "full_history" and r.get("dimension") == "loss_bucket"]
    funnel_full = [r for r in funnel_rows if r.get("window") == "full_history"]
    funnel_last14 = [r for r in funnel_rows if r.get("window") == "last_14"]

    lines = [
        "# Hits Over 1.5 PA / Opportunity Audit",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Latest completed slate: `{latest or 'n/a'}`",
        "- Candidate universe: all reconciled `prop_type=hits`, `side=over`, `line=1.5` rows.",
        "- Scope: analysis only; no board, selector, upload, threshold, grading, or matching changes.",
        f"- Rows: `{all_metrics['rows']}` | resolved `{all_metrics['resolved']}` | WR `{_pct(_f(all_metrics['wr']))}` | ROI `{_pct(_f(all_metrics['roi']))}`.",
        "",
        "## Actual PA Buckets",
        "",
        "| bucket | rows | resolved | wins | losses | WR | ROI | units | avg odds | avg actual PA | avg d7 PA | avg d15 PA |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    lines.extend(line(row, "bucket") for row in actual_pa_full)
    lines.extend(
        [
            "",
            "## o1.5 Loss Buckets",
            "",
            f"- Loss count by bucket: `{dict(sorted(loss_counts.items()))}`",
            "",
            "| bucket | rows | resolved | wins | losses | WR | ROI | units | avg odds | avg actual PA | avg d7 PA | avg d15 PA |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(line(row, "bucket") for row in loss_bucket_full)
    lines.extend(
        [
            "",
            "## Funnel With PA Filters - Full History",
            "",
            "| segment | rows | resolved | wins | losses | WR | ROI | units | avg odds | avg actual PA | avg d7 PA | avg d15 PA |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(line(row, "segment") for row in funnel_full)
    lines.extend(
        [
            "",
            "## Funnel With PA Filters - Last 14",
            "",
            "| segment | rows | resolved | wins | losses | WR | ROI | units | avg odds | avg actual PA | avg d7 PA | avg d15 PA |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    lines.extend(line(row, "segment") for row in funnel_last14)

    # Lightweight interpretation; the numbers above are the source of truth.
    base = next((r for r in funnel_full if r.get("segment") == "d7+d15+starter>=5"), {})
    pa4 = next((r for r in funnel_full if r.get("segment") == "d7+d15+starter>=5 + d15_PA>=4"), {})
    stable = next((r for r in funnel_full if r.get("segment") == "d7+d15+starter>=5 + stable_4plus_PA"), {})
    lines.extend(["", "## Preliminary Answer", ""])
    lines.append(
        f"- Actual PA is measurable for `{sum(1 for r in rows if _f(r.get('actual_plate_appearances')) is not None)}` of `{len(rows)}` rows."
    )
    if base and pa4:
        lines.append(
            f"- Existing d7+d15+starter funnel ROI `{_pct(_f(base.get('roi')))}`; adding `d15_PA>=4` changes it to `{_pct(_f(pa4.get('roi')))}`."
        )
    if base and stable:
        lines.append(
            f"- Adding stable 4+ PA history changes that same funnel to `{_pct(_f(stable.get('roi')))}`."
        )
    lines.append("- Use player case studies only from the largest or clearest failure bucket after reviewing this population table.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Population PA/opportunity audit for hits over 1.5 misses.")
    ap.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    rows = [
        r
        for r in tier_base._load_reconcile_rows(ROOT / args.execution_root)
        if _clean(r.get("side")) == "over" and _f(r.get("line")) == 1.5 and _clean(r.get("result")) in {"win", "loss", "push"}
    ]
    latest = max([str(r.get("date") or "") for r in rows], default="")
    _enrich(rows, ROOT / args.lanes_root)

    bucket_rows: list[dict[str, Any]] = []
    for dimension, field in [
        ("actual_pa_bucket", "actual_pa_bucket"),
        ("loss_bucket", "loss_bucket"),
        ("d7_pa_bucket", "d7_pa_bucket"),
        ("d15_pa_bucket", "d15_pa_bucket"),
        ("d30_pa_bucket", "d30_pa_bucket"),
        ("pa_stability_bucket", "pa_stability_bucket"),
        ("lineup_bucket", "lineup_bucket"),
    ]:
        bucket_rows.extend(_summarize_dimension(rows, latest, dimension, field))

    def d7(row: dict[str, Any]) -> bool:
        v = _f(row.get("d7_hits_rate"))
        return v is not None and v > 1.0

    def d15(row: dict[str, Any]) -> bool:
        v = _f(row.get("d15_hits_rate"))
        return v is not None and v > 1.0

    def starter5(row: dict[str, Any]) -> bool:
        v = _f(row.get("starter_expected_hits_allowed"))
        return v is not None and v >= 5.0

    def d15_pa4(row: dict[str, Any]) -> bool:
        v = _f(row.get("d15_plate_appearances"))
        return v is not None and v >= 4.0

    def d30_pa4(row: dict[str, Any]) -> bool:
        v = _f(row.get("d30_plate_appearances"))
        return v is not None and v >= 4.0

    specs = [
        ("all_o15", lambda r: True),
        ("d7>1", d7),
        ("d7+d15>1", lambda r: d7(r) and d15(r)),
        ("d7+d15+starter>=5", lambda r: d7(r) and d15(r) and starter5(r)),
        ("QC", lambda r: bool(r.get("qc_candidate"))),
        ("QC+d7+d15+starter>=5", lambda r: bool(r.get("qc_candidate")) and d7(r) and d15(r) and starter5(r)),
        ("d7+d15+starter>=5 + d15_PA>=4", lambda r: d7(r) and d15(r) and starter5(r) and d15_pa4(r)),
        ("d7+d15+starter>=5 + d30_PA>=4", lambda r: d7(r) and d15(r) and starter5(r) and d30_pa4(r)),
        (
            "d7+d15+starter>=5 + stable_4plus_PA",
            lambda r: d7(r) and d15(r) and starter5(r) and str(r.get("pa_stability_bucket")) == "stable_4plus",
        ),
        (
            "QC+d7+d15+starter>=5 + d15_PA>=4",
            lambda r: bool(r.get("qc_candidate")) and d7(r) and d15(r) and starter5(r) and d15_pa4(r),
        ),
    ]
    funnel_rows = _summarize_segments(rows, latest, specs)

    out_dir = Path(args.out_dir)
    detail_csv = out_dir / "o15_pa_opportunity_audit_rows.csv"
    buckets_csv = out_dir / "o15_pa_opportunity_buckets.csv"
    funnel_csv = out_dir / "o15_pa_opportunity_funnel.csv"
    report_md = out_dir / "o15_pa_opportunity_audit.md"
    _write_csv(detail_csv, rows)
    _write_csv(buckets_csv, bucket_rows)
    _write_csv(funnel_csv, funnel_rows)
    _write_report(report_md, rows, bucket_rows, funnel_rows, latest)
    print(
        json.dumps(
            {
                "latest_completed_slate": latest,
                "rows": len(rows),
                "actual_pa_rows": sum(1 for r in rows if _f(r.get("actual_plate_appearances")) is not None),
                "outputs": {
                    "report": _rel(report_md),
                    "detail_csv": _rel(detail_csv),
                    "buckets_csv": _rel(buckets_csv),
                    "funnel_csv": _rel(funnel_csv),
                },
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
