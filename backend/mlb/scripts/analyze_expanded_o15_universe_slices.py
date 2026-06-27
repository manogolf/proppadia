#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_ROWS_CSV = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
DEFAULT_BACKFILL_ROOT = Path("artifacts/analysis/mlb/review_aids/alternate_history/backfill")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")


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


def _f(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        number = float(value)
        if math.isnan(number):
            return None
        return number
    except Exception:
        return None


def _b(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _norm_name(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^0-9A-Za-z ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def _date(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("board_date") or "")[:10]


def _american_units(price: float | None, win: bool, loss: bool, push: bool) -> float:
    if push:
        return 0.0
    if loss:
        return -1.0
    if not win or price is None:
        return 0.0
    if price >= 0:
        return price / 100.0
    return 100.0 / abs(price)


def _avg(values: list[float | None]) -> float | None:
    nums = [float(v) for v in values if v is not None]
    return sum(nums) / len(nums) if nums else None


def _bucket_number(value: Any, cuts: list[tuple[str, float | None, float | None]], missing: str = "missing") -> str:
    number = _f(value)
    if number is None:
        return missing
    for label, low, high in cuts:
        if low is None and high is None:
            continue
        if low is not None and number < low:
            continue
        if high is not None and number >= high:
            continue
        return label
    return "other"


def _price_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<=150", None, 151),
            ("151-200", 151, 201),
            ("201-300", 201, 301),
            ("301-400", 301, 401),
            ("401-600", 401, 601),
            (">600", 601, None),
        ],
    )


def _implied_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<20%", None, 0.20),
            ("20-25%", 0.20, 0.25),
            ("25-30%", 0.25, 0.30),
            ("30-35%", 0.30, 0.35),
            (">=35%", 0.35, None),
        ],
    )


def _rate_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<=1.0", None, 1.0000001),
            ("1.0-1.1", 1.0000001, 1.1000001),
            ("1.1-1.3", 1.1000001, 1.3000001),
            (">1.3", 1.3000001, None),
        ],
    )


def _hrr_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<2.5", None, 2.5),
            ("2.5-3.0", 2.5, 3.0),
            ("3.0-3.5", 3.0, 3.5),
            (">=3.5", 3.5, None),
        ],
    )


def _expected_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<4.5", None, 4.5),
            ("4.5-5.0", 4.5, 5.0),
            ("5.0-5.5", 5.0, 5.5),
            (">=5.5", 5.5, None),
        ],
    )


def _team_expected_bucket(value: Any) -> str:
    return _bucket_number(
        value,
        [
            ("<7.0", None, 7.0),
            ("7.0-8.0", 7.0, 8.0),
            ("8.0-9.0", 8.0, 9.0),
            (">=9.0", 9.0, None),
        ],
    )


def _book_count(row: dict[str, Any]) -> int:
    return len([x for x in str(row.get("bookmaker_list") or "").split(",") if x.strip()])


def _metrics(rows: list[dict[str, Any]], *, price_col: str = "expanded_price") -> dict[str, Any]:
    resolved = [row for row in rows if _b(row.get("resolved"))]
    wins = sum(1 for row in resolved if _b(row.get("win")))
    losses = sum(1 for row in resolved if _b(row.get("loss")))
    pushes = sum(1 for row in resolved if _b(row.get("push")))
    units = sum(_american_units(_f(row.get(price_col)), _b(row.get("win")), _b(row.get("loss")), _b(row.get("push"))) for row in resolved)
    return {
        "candidates": len(rows),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / len(resolved) if resolved else None,
        "units": units,
        "avg_odds": _avg([_f(row.get(price_col)) for row in rows]),
        "avg_implied": _avg([_f(row.get("selected_side_implied_probability")) for row in rows]),
        "avg_d7": _avg([_f(row.get("d7_hits_rate")) for row in rows]),
        "avg_d15": _avg([_f(row.get("d15_hits_rate")) for row in rows]),
        "avg_d7_hrr": _avg([_f(row.get("d7_hits_runs_rbis")) for row in rows]),
        "avg_d15_hrr": _avg([_f(row.get("d15_hits_runs_rbis")) for row in rows]),
        "avg_starter_expected_hits": _avg([_f(row.get("starter_expected_hits_allowed")) for row in rows]),
        "avg_team_expected_hits": _avg([_f(row.get("team_expected_hits_allowed")) for row in rows]),
    }


def _metric_row(slice_type: str, slice_value: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = {"slice_type": slice_type, "slice_value": slice_value}
    out.update(_metrics(rows))
    return out


def _group(rows: list[dict[str, Any]], slice_type: str, func: Callable[[dict[str, Any]], str]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[func(row)].append(row)
    return [_metric_row(slice_type, key, groups[key]) for key in sorted(groups)]


def _load_book_prices(backfill_root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for path in sorted(backfill_root.glob("*/live_alternate_book_level_rows.csv")):
        date_text = path.parent.name
        for row in _read_csv(path):
            if str(row.get("market_key") or "") != "batter_hits_alternate":
                continue
            if str(row.get("side") or "").lower() != "over":
                continue
            if abs((_f(row.get("line")) or -999) - 1.5) > 1e-9:
                continue
            key = (date_text, _norm_name(row.get("player_name")))
            grouped[key].append(row)
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for key, rows in grouped.items():
        prices = [(_f(row.get("price")), str(row.get("bookmaker_key") or "")) for row in rows if _f(row.get("price")) is not None]
        price_values = [p for p, _book in prices if p is not None]
        betonline = [p for p, book in prices if book == "betonlineag" and p is not None]
        out[key] = {
            "book_count_from_source": len({book for _p, book in prices if book}),
            "best_available_over_price": max(price_values) if price_values else None,
            "median_available_over_price": statistics.median(price_values) if price_values else None,
            "betonline_over_price": betonline[0] if betonline else None,
        }
    return out


def _enrich_prices(rows: list[dict[str, Any]], backfill_root: Path) -> None:
    prices = _load_book_prices(backfill_root)
    for row in rows:
        key = (_date(row), _norm_name(row.get("player_name")))
        info = prices.get(key, {})
        for col in ("book_count_from_source", "best_available_over_price", "median_available_over_price", "betonline_over_price"):
            row[col] = info.get(col)


def _slice_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    slice_rows: list[dict[str, Any]] = []
    specs: list[tuple[str, Callable[[dict[str, Any]], str]]] = [
        ("alternate_layer", lambda r: str(r.get("alternate_layer") or "missing")),
        ("combined_tier", lambda r: str(r.get("combined_tier") or "missing")),
        ("hitter_tier", lambda r: str(r.get("hitter_tier") or "missing")),
        ("pitcher_tier", lambda r: str(r.get("pitcher_tier") or "missing")),
        ("source_type", lambda r: "overlap" if _b(r.get("from_both")) else "alternate_only"),
        ("book_count", lambda r: str(r.get("book_count_from_source") or _book_count(r) or 0)),
        ("selected_book", lambda r: str(r.get("book") or "missing")),
        ("price_bucket", lambda r: _price_bucket(r.get("expanded_price"))),
        ("implied_bucket", lambda r: _implied_bucket(r.get("selected_side_implied_probability"))),
        ("d7_hits_rate_bucket", lambda r: _rate_bucket(r.get("d7_hits_rate"))),
        ("d15_hits_rate_bucket", lambda r: _rate_bucket(r.get("d15_hits_rate"))),
        ("d7_hrr_bucket", lambda r: _hrr_bucket(r.get("d7_hits_runs_rbis"))),
        ("d15_hrr_bucket", lambda r: _hrr_bucket(r.get("d15_hits_runs_rbis"))),
        ("starter_expected_bucket", lambda r: _expected_bucket(r.get("starter_expected_hits_allowed"))),
        ("team_expected_bucket", lambda r: _team_expected_bucket(r.get("team_expected_hits_allowed"))),
        ("same_game_tier_a_cluster", lambda r: ">0" if (_f(r.get("same_game_teammate_tier_a_count")) or 0) > 0 else "0_or_missing"),
        ("team_game", lambda r: f"{_date(r)} {r.get('team') or ''} vs {r.get('opponent') or ''}".strip()),
    ]
    for slice_type, func in specs:
        slice_rows.extend(_group(rows, slice_type, func))
    return slice_rows


def _interaction_funnels(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def has_layer(row: dict[str, Any], layers: set[str]) -> bool:
        return str(row.get("alternate_layer") or "") in layers

    funnels: list[tuple[str, Callable[[dict[str, Any]], bool]]] = [
        ("alternate_source_all", lambda r: True),
        ("alternate_only", lambda r: not _b(r.get("from_both"))),
        ("alternate_only_layer_a", lambda r: not _b(r.get("from_both")) and has_layer(r, {"alternate_layer_a_d7_d15_starter"})),
        ("alternate_only_layer_a_b", lambda r: not _b(r.get("from_both")) and has_layer(r, {"alternate_layer_a_d7_d15_starter", "alternate_layer_b_d7_d15"})),
        ("alternate_only_hitter_tier_a", lambda r: not _b(r.get("from_both")) and str(r.get("hitter_tier")) == "A"),
        ("alternate_only_hitter_a_pitcher_a_b", lambda r: not _b(r.get("from_both")) and str(r.get("hitter_tier")) == "A" and str(r.get("pitcher_tier")) in {"A", "B"}),
        ("alternate_only_d7_hrr_ge_3", lambda r: not _b(r.get("from_both")) and (_f(r.get("d7_hits_runs_rbis")) or -999) >= 3),
        ("alternate_only_team_expected_ge_8", lambda r: not _b(r.get("from_both")) and (_f(r.get("team_expected_hits_allowed")) or -999) >= 8),
        ("alternate_only_same_game_tier_a_cluster", lambda r: not _b(r.get("from_both")) and (_f(r.get("same_game_teammate_tier_a_count")) or 0) > 0),
        ("alternate_only_excluding_d7_hrr_lt_3", lambda r: not _b(r.get("from_both")) and not ((_f(r.get("d7_hits_runs_rbis")) is not None) and (_f(r.get("d7_hits_runs_rbis")) or 0) < 3)),
    ]
    return [_metric_row("interaction_funnel", label, [row for row in rows if pred(row)]) for label, pred in funnels]


def _price_sanity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    variants = [
        ("current_selected_price", "expanded_price"),
        ("best_available_over_price", "best_available_over_price"),
        ("betonline_over_price", "betonline_over_price"),
        ("median_available_over_price", "median_available_over_price"),
    ]
    for label, col in variants:
        priced = [row for row in rows if _f(row.get(col)) is not None]
        item = {"price_variant": label}
        item.update(_metrics(priced, price_col=col))
        item["priced_candidates"] = len(priced)
        item["priced_resolved"] = sum(1 for row in priced if _b(row.get("resolved")))
        out.append(item)
    return out


def _top_bottom(slice_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for min_resolved in (20, 50):
        eligible = [row for row in slice_rows if int(row.get("resolved") or 0) >= min_resolved]
        for direction, ordered in (
            ("top", sorted(eligible, key=lambda r: (_f(r.get("roi")) if _f(r.get("roi")) is not None else -999), reverse=True)),
            ("bottom", sorted(eligible, key=lambda r: (_f(r.get("roi")) if _f(r.get("roi")) is not None else 999))),
        ):
            for rank, row in enumerate(ordered[:15], start=1):
                out.append({"min_resolved": min_resolved, "direction": direction, "rank": rank, **row})
    return out


def _fmt_pct(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number * 100:.2f}%"


def _fmt_num(value: Any) -> str:
    number = _f(value)
    return "n/a" if number is None else f"{number:.2f}"


def _write_report(path: Path, slice_rows: list[dict[str, Any]], funnels: list[dict[str, Any]], prices: list[dict[str, Any]], top_bottom: list[dict[str, Any]]) -> None:
    def find(slice_type: str, slice_value: str) -> dict[str, Any]:
        return next((row for row in slice_rows if row.get("slice_type") == slice_type and row.get("slice_value") == slice_value), {})

    layer_rows = [row for row in slice_rows if row.get("slice_type") == "alternate_layer"]
    source_rows = [row for row in slice_rows if row.get("slice_type") == "source_type"]
    lines = [
        "# Expanded O1.5 Universe Slice Analysis",
        "",
        "Scope: resolved alternate-source rows inside the Expanded O1.5 Universe. Production remains unchanged.",
        "",
        "## Source Type",
        "",
        "| source | candidates | resolved | W-L-P | WR | ROI | units | avg odds |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in source_rows:
        lines.append(
            f"| {row.get('slice_value')} | {row.get('candidates')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('wr'))} | "
            f"{_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    lines.extend(["", "## Alternate Layer", "", "| layer | candidates | resolved | W-L-P | WR | ROI | units | avg odds |", "|---|---:|---:|---:|---:|---:|---:|---:|"])
    for row in layer_rows:
        lines.append(
            f"| {row.get('slice_value')} | {row.get('candidates')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('wr'))} | "
            f"{_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    lines.extend(["", "## Interaction Funnels", "", "| funnel | candidates | resolved | W-L-P | WR | ROI | units | avg odds |", "|---|---:|---:|---:|---:|---:|---:|---:|"])
    for row in funnels:
        lines.append(
            f"| {row.get('slice_value')} | {row.get('candidates')} | {row.get('resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('wr'))} | "
            f"{_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    lines.extend(["", "## Price Sanity", "", "| price basis | priced candidates | priced resolved | W-L-P | ROI | units | avg odds |", "|---|---:|---:|---:|---:|---:|---:|"])
    for row in prices:
        lines.append(
            f"| {row.get('price_variant')} | {row.get('priced_candidates')} | {row.get('priced_resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_num(row.get('units'))} | {_fmt_num(row.get('avg_odds'))} |"
        )

    top20 = [row for row in top_bottom if row.get("min_resolved") == 20 and row.get("direction") == "top"][:5]
    bottom20 = [row for row in top_bottom if row.get("min_resolved") == 20 and row.get("direction") == "bottom"][:5]
    lines.extend(["", "## Top Positive Carriers >=20 Resolved", "", "| slice | resolved | W-L-P | ROI | units |", "|---|---:|---:|---:|---:|"])
    for row in top20:
        lines.append(f"| {row.get('slice_type')}={row.get('slice_value')} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} |")
    lines.extend(["", "## Worst Negative Carriers >=20 Resolved", "", "| slice | resolved | W-L-P | ROI | units |", "|---|---:|---:|---:|---:|"])
    for row in bottom20:
        lines.append(f"| {row.get('slice_type')}={row.get('slice_value')} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} |")

    all_alt = find("source_type", "alternate_only")
    overlap = find("source_type", "overlap")
    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"- Alternate-only carries most resolved volume: `{all_alt.get('resolved', 0)}` resolved at `{_fmt_pct(all_alt.get('roi'))}` ROI.",
            f"- Overlap is weaker in this sample: `{overlap.get('resolved', 0)}` resolved at `{_fmt_pct(overlap.get('roi'))}` ROI.",
            "- Layer A/B/C should be interpreted from this report, not from the previous side-board framing.",
            "- Price-selection sensitivity is reported separately because bookmaker is collapsed in the universe identity.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze slices carrying Expanded O1.5 Universe alternate-source ROI.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = _read_csv(Path(args.rows_csv))
    alt_rows = [row for row in rows if _b(row.get("from_alternate"))]
    _enrich_prices(alt_rows, Path(args.backfill_root))

    slice_rows = _slice_summary(alt_rows)
    funnels = _interaction_funnels(alt_rows)
    prices = _price_sanity(alt_rows)
    top_bottom = _top_bottom(slice_rows)

    out_dir = Path(args.out_dir)
    _write_csv(out_dir / "expanded_o15_universe_slice_summary.csv", slice_rows)
    _write_csv(out_dir / "expanded_o15_universe_interaction_funnels.csv", funnels)
    _write_csv(out_dir / "expanded_o15_universe_price_sanity.csv", prices)
    _write_csv(out_dir / "expanded_o15_universe_top_bottom_slices.csv", top_bottom)
    _write_report(out_dir / "expanded_o15_universe_slice_analysis.md", slice_rows, funnels, prices, top_bottom)
    print(
        {
            "alternate_source_rows": len(alt_rows),
            "slice_summary": str(out_dir / "expanded_o15_universe_slice_summary.csv"),
            "report": str(out_dir / "expanded_o15_universe_slice_analysis.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
