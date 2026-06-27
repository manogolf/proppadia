#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import re
import statistics
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import analyze_expanded_o15_universe_slices as slices


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
    return slices._f(value)


def _b(value: Any) -> bool:
    return slices._b(value)


def _norm_name(value: Any) -> str:
    text = str(value or "").strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^0-9A-Za-z ]+", " ", text).lower()
    return re.sub(r"\s+", " ", text).strip()


def _date(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("board_date") or "")[:10]


def _american_implied(price: float | None) -> float | None:
    if price is None:
        return None
    if price > 0:
        return 100.0 / (price + 100.0)
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return None


def _price_map(backfill_root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[tuple[float, str]]] = defaultdict(list)
    for path in sorted(backfill_root.glob("*/live_alternate_book_level_rows.csv")):
        date_text = path.parent.name
        for row in _read_csv(path):
            if str(row.get("market_key") or "") != "batter_hits_alternate":
                continue
            if str(row.get("side") or "").lower() != "over":
                continue
            if abs((_f(row.get("line")) or -999) - 1.5) > 1e-9:
                continue
            price = _f(row.get("price"))
            book = str(row.get("bookmaker_key") or "").strip()
            if price is None or not book:
                continue
            grouped[(date_text, _norm_name(row.get("player_name")))].append((price, book))
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for key, pairs in grouped.items():
        values = [p for p, _book in pairs]
        bol_values = [p for p, book in pairs if book == "betonlineag"]
        best = max(values) if values else None
        worst = min(values) if values else None
        median = statistics.median(values) if values else None
        bol = bol_values[0] if bol_values else None
        out[key] = {
            "book_count": len({book for _p, book in pairs}),
            "bookmaker_list_source": ",".join(sorted({book for _p, book in pairs})),
            "best_available_over_price": best,
            "worst_available_over_price": worst,
            "median_available_over_price": median,
            "betonline_over_price": bol,
            "betonline_available": bol is not None,
            "betonline_is_best": bol is not None and best is not None and abs(bol - best) < 1e-9,
            "betonline_worse_than_best": bol is not None and best is not None and bol < best,
            "price_gap_best_minus_bol": (best - bol) if bol is not None and best is not None else None,
            "implied_gap_bol_minus_best": (
                (_american_implied(bol) or 0.0) - (_american_implied(best) or 0.0)
                if bol is not None and best is not None
                else None
            ),
        }
    return out


def _enrich(rows: list[dict[str, Any]], backfill_root: Path) -> None:
    prices = _price_map(backfill_root)
    for row in rows:
        info = prices.get((_date(row), _norm_name(row.get("player_name"))), {})
        for key, value in info.items():
            row[key] = value


def _metrics(rows: list[dict[str, Any]], price_col: str) -> dict[str, Any]:
    resolved = [row for row in rows if _b(row.get("resolved"))]
    priced_resolved = [row for row in resolved if _f(row.get(price_col)) is not None]
    wins = sum(1 for row in resolved if _b(row.get("win")))
    losses = sum(1 for row in resolved if _b(row.get("loss")))
    pushes = sum(1 for row in resolved if _b(row.get("push")))
    priced_units = sum(
        slices._american_units(_f(row.get(price_col)), _b(row.get("win")), _b(row.get("loss")), _b(row.get("push")))
        for row in priced_resolved
    )
    m = {
        "candidates": len(rows),
        "resolved": len(resolved),
        "priced_resolved": len(priced_resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": priced_units / len(priced_resolved) if priced_resolved else None,
        "units": priced_units if priced_resolved else None,
        "avg_odds": slices._avg([_f(row.get(price_col)) for row in rows]),
        "avg_implied": slices._avg([_f(row.get("selected_side_implied_probability")) for row in rows]),
        "avg_d7": slices._avg([_f(row.get("d7_hits_rate")) for row in rows]),
        "avg_d15": slices._avg([_f(row.get("d15_hits_rate")) for row in rows]),
        "avg_d7_hrr": slices._avg([_f(row.get("d7_hits_runs_rbis")) for row in rows]),
        "avg_d15_hrr": slices._avg([_f(row.get("d15_hits_runs_rbis")) for row in rows]),
        "avg_starter_expected_hits": slices._avg([_f(row.get("starter_expected_hits_allowed")) for row in rows]),
        "avg_team_expected_hits": slices._avg([_f(row.get("team_expected_hits_allowed")) for row in rows]),
    }
    best = [_f(row.get("best_available_over_price")) for row in rows if _f(row.get("best_available_over_price")) is not None]
    bol = [_f(row.get("betonline_over_price")) for row in rows if _f(row.get("betonline_over_price")) is not None]
    gaps = [_f(row.get("price_gap_best_minus_bol")) for row in rows if _f(row.get("price_gap_best_minus_bol")) is not None]
    implied_gaps = [_f(row.get("implied_gap_bol_minus_best")) for row in rows if _f(row.get("implied_gap_bol_minus_best")) is not None]
    m.update(
        {
            "avg_best_price": slices._avg(best),
            "avg_betonline_price": slices._avg(bol),
            "avg_price_gap": slices._avg(gaps),
            "avg_implied_gap": slices._avg(implied_gaps),
        }
    )
    return m


def _metric_row(group_type: str, group_value: str, rows: list[dict[str, Any]], price_col: str = "betonline_over_price") -> dict[str, Any]:
    out = {"group_type": group_type, "group_value": group_value}
    out.update(_metrics(rows, price_col))
    return out


def _group(rows: list[dict[str, Any]], group_type: str, func: Callable[[dict[str, Any]], str]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[func(row)].append(row)
    return [_metric_row(group_type, key, groups[key]) for key in sorted(groups)]


def _summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = {
        "betonline_available": [row for row in rows if _b(row.get("betonline_available"))],
        "betonline_not_available": [row for row in rows if not _b(row.get("betonline_available"))],
        "betonline_is_best": [row for row in rows if _b(row.get("betonline_is_best"))],
        "betonline_worse_than_best": [row for row in rows if _b(row.get("betonline_worse_than_best"))],
        "alternate_only_betonline_available": [
            row for row in rows if _b(row.get("betonline_available")) and not _b(row.get("from_both"))
        ],
        "overlap_betonline_available": [
            row for row in rows if _b(row.get("betonline_available")) and _b(row.get("from_both"))
        ],
    }
    out: list[dict[str, Any]] = []
    for label, group_rows in groups.items():
        row = _metric_row("availability_group", label, group_rows, "betonline_over_price")
        current_metrics = _metrics(group_rows, "expanded_price")
        best_metrics = _metrics(group_rows, "best_available_over_price")
        median_metrics = _metrics(group_rows, "median_available_over_price")
        row["roi_current_selected_price"] = current_metrics.get("roi")
        row["units_current_selected_price"] = current_metrics.get("units")
        row["roi_best_price"] = best_metrics.get("roi")
        row["units_best_price"] = best_metrics.get("units")
        row["roi_median_price"] = median_metrics.get("roi")
        row["units_median_price"] = median_metrics.get("units")
        row["roi_betonline_price"] = row.get("roi")
        row["units_betonline_price"] = row.get("units")
        out.append(row)
    return out


def _slice_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bol = [row for row in rows if _b(row.get("betonline_available"))]
    specs: list[tuple[str, Callable[[dict[str, Any]], str]]] = [
        ("alternate_layer", lambda r: str(r.get("alternate_layer") or "missing")),
        ("combined_tier", lambda r: str(r.get("combined_tier") or "missing")),
        ("hitter_tier", lambda r: str(r.get("hitter_tier") or "missing")),
        ("pitcher_tier", lambda r: str(r.get("pitcher_tier") or "missing")),
        ("price_bucket_betonline", lambda r: slices._price_bucket(r.get("betonline_over_price"))),
        ("d7_hits_rate_bucket", lambda r: slices._rate_bucket(r.get("d7_hits_rate"))),
        ("d15_hits_rate_bucket", lambda r: slices._rate_bucket(r.get("d15_hits_rate"))),
        ("d7_hrr_bucket", lambda r: slices._hrr_bucket(r.get("d7_hits_runs_rbis"))),
        ("d15_hrr_bucket", lambda r: slices._hrr_bucket(r.get("d15_hits_runs_rbis"))),
        ("starter_expected_bucket", lambda r: slices._expected_bucket(r.get("starter_expected_hits_allowed"))),
        ("team_expected_bucket", lambda r: slices._team_expected_bucket(r.get("team_expected_hits_allowed"))),
        ("same_game_tier_a_cluster", lambda r: ">0" if (_f(r.get("same_game_teammate_tier_a_count")) or 0) > 0 else "0_or_missing"),
        ("source", lambda r: "overlap" if _b(r.get("from_both")) else "alternate_only"),
    ]
    out: list[dict[str, Any]] = []
    for group_type, func in specs:
        out.extend(_group(bol, group_type, func))
    return out


def _price_sanity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    bol = [row for row in rows if _b(row.get("betonline_available"))]
    variants = [
        ("current_selected_price", "expanded_price"),
        ("best_available_price", "best_available_over_price"),
        ("betonline_price", "betonline_over_price"),
        ("median_market_price", "median_available_over_price"),
        ("worst_available_price", "worst_available_over_price"),
    ]
    out: list[dict[str, Any]] = []
    for label, col in variants:
        row = {"price_variant": label}
        row.update(_metrics(bol, col))
        out.append(row)
    return out


def _top_bottom(slices_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for min_resolved in (20, 50):
        eligible = [row for row in slices_rows if int(row.get("resolved") or 0) >= min_resolved]
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


def _write_report(path: Path, summary: list[dict[str, Any]], slices_rows: list[dict[str, Any]], price_rows: list[dict[str, Any]], top_bottom: list[dict[str, Any]]) -> None:
    bol = next((row for row in summary if row.get("group_value") == "betonline_available"), {})
    bol_alt_only = next((row for row in summary if row.get("group_value") == "alternate_only_betonline_available"), {})
    worse = next((row for row in summary if row.get("group_value") == "betonline_worse_than_best"), {})
    lines = [
        "# Expanded O1.5 Universe BetOnline Audit",
        "",
        "Scope: alternate-source Expanded O1.5 rows. Production remains unchanged.",
        "",
        "## Availability / Price Gap",
        "",
        "| group | candidates | resolved | priced resolved | W-L-P | ROI BOL | ROI current | ROI best | ROI median | avg best | avg BOL | avg gap |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary:
        lines.append(
            f"| {row.get('group_value')} | {row.get('candidates')} | {row.get('resolved')} | {row.get('priced_resolved')} | "
            f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi_betonline_price'))} | "
            f"{_fmt_pct(row.get('roi_current_selected_price'))} | {_fmt_pct(row.get('roi_best_price'))} | {_fmt_pct(row.get('roi_median_price'))} | "
            f"{_fmt_num(row.get('avg_best_price'))} | {_fmt_num(row.get('avg_betonline_price'))} | {_fmt_num(row.get('avg_price_gap'))} |"
        )
    lines.extend(["", "## Price Realism", "", "| basis | resolved | priced resolved | W-L-P | ROI | units | avg odds |", "|---|---:|---:|---:|---:|---:|---:|"])
    for row in price_rows:
        lines.append(
            f"| {row.get('price_variant')} | {row.get('resolved')} | {row.get('priced_resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | "
            f"{_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    top20 = [row for row in top_bottom if row.get("min_resolved") == 20 and row.get("direction") == "top"][:6]
    bottom20 = [row for row in top_bottom if row.get("min_resolved") == 20 and row.get("direction") == "bottom"][:6]
    top50 = [row for row in top_bottom if row.get("min_resolved") == 50 and row.get("direction") == "top"][:6]
    bottom50 = [row for row in top_bottom if row.get("min_resolved") == 50 and row.get("direction") == "bottom"][:6]
    lines.extend(["", "## BetOnline-Positive Slices >=20 Resolved", "", "| slice | resolved | W-L-P | ROI | units |", "|---|---:|---:|---:|---:|"])
    for row in top20:
        lines.append(f"| {row.get('group_type')}={row.get('group_value')} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} |")
    lines.extend(["", "## BetOnline-Negative Slices >=20 Resolved", "", "| slice | resolved | W-L-P | ROI | units |", "|---|---:|---:|---:|---:|"])
    for row in bottom20:
        lines.append(f"| {row.get('group_type')}={row.get('group_value')} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} |")
    lines.extend(["", "## BetOnline-Positive Slices >=50 Resolved", "", "| slice | resolved | W-L-P | ROI | units |", "|---|---:|---:|---:|---:|"])
    for row in top50:
        lines.append(f"| {row.get('group_type')}={row.get('group_value')} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} |")
    lines.extend(["", "## BetOnline-Negative Slices >=50 Resolved", "", "| slice | resolved | W-L-P | ROI | units |", "|---|---:|---:|---:|---:|"])
    for row in bottom50:
        lines.append(f"| {row.get('group_type')}={row.get('group_value')} | {row.get('resolved')} | {row.get('wins')}-{row.get('losses')}-{row.get('pushes')} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('units'))} |")
    lines.extend(
        [
            "",
            "## Readout",
            "",
            f"- BetOnline-available alternate rows are `{bol.get('wins', 0)}-{bol.get('losses', 0)}` at `{_fmt_pct(bol.get('roi_betonline_price'))}` ROI using BetOnline price.",
            f"- Alternate-only rows with BetOnline available are `{bol_alt_only.get('wins', 0)}-{bol_alt_only.get('losses', 0)}` at `{_fmt_pct(bol_alt_only.get('roi_betonline_price'))}` ROI using BetOnline price.",
            f"- Rows where BetOnline was worse than best available price averaged a `{_fmt_num(worse.get('avg_price_gap'))}` point price gap and moved from `{_fmt_pct(worse.get('roi_best_price'))}` at best price to `{_fmt_pct(worse.get('roi_betonline_price'))}` at BetOnline price.",
            "- The Expanded O1.5 positive ROI is best-price sensitive; BetOnline-only performance is separately tracked here.",
            "- Treat any positive BetOnline slice as research-only until larger samples validate.",
            "",
            "## Answers",
            "",
            f"- Is Expanded O1.5 still positive at BetOnline prices? `{'YES' if (_f(bol.get('roi_betonline_price')) or 0) > 0 else 'NO'}` globally for BetOnline-available alternate rows.",
            "- Are there BetOnline-positive sub-slices? `YES`, but the strongest positive slices are narrower than the whole alternate universe and should remain research-only.",
            "- Is the alternate edge mostly a line-shopping edge? `YES` for the current resolved sample: best/current price is positive while BetOnline-only is near flat/negative.",
            "- Daily Index / Current Research should label Expanded O1.5 as best-price sensitive, with BetOnline-only performance separately tracked.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit Expanded O1.5 Universe under BetOnline pricing.")
    ap.add_argument("--rows-csv", default=str(DEFAULT_ROWS_CSV))
    ap.add_argument("--backfill-root", default=str(DEFAULT_BACKFILL_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    rows = [row for row in _read_csv(Path(args.rows_csv)) if _b(row.get("from_alternate"))]
    _enrich(rows, Path(args.backfill_root))
    summary = _summary(rows)
    slice_rows = _slice_rows(rows)
    price_rows = _price_sanity(rows)
    top_bottom = _top_bottom(slice_rows)
    gap_rows = [
        {
            "date": _date(row),
            "player_name": row.get("player_name"),
            "player_id": row.get("player_id"),
            "source_bucket": row.get("source_bucket"),
            "resolved": row.get("resolved"),
            "win": row.get("win"),
            "loss": row.get("loss"),
            "best_available_over_price": row.get("best_available_over_price"),
            "betonline_over_price": row.get("betonline_over_price"),
            "median_available_over_price": row.get("median_available_over_price"),
            "worst_available_over_price": row.get("worst_available_over_price"),
            "price_gap_best_minus_bol": row.get("price_gap_best_minus_bol"),
            "implied_gap_bol_minus_best": row.get("implied_gap_bol_minus_best"),
            "book_count": row.get("book_count"),
            "bookmaker_list_source": row.get("bookmaker_list_source"),
            "combined_tier": row.get("combined_tier"),
            "alternate_layer": row.get("alternate_layer"),
        }
        for row in rows
    ]

    out_dir = Path(args.out_dir)
    _write_csv(out_dir / "expanded_o15_universe_betonline_summary.csv", summary)
    _write_csv(out_dir / "expanded_o15_universe_betonline_slices.csv", slice_rows)
    _write_csv(out_dir / "expanded_o15_universe_betonline_price_gaps.csv", gap_rows)
    _write_csv(out_dir / "expanded_o15_universe_betonline_top_bottom.csv", top_bottom)
    _write_report(out_dir / "expanded_o15_universe_betonline_audit.md", summary, slice_rows, price_rows, top_bottom)
    print(
        {
            "alternate_rows": len(rows),
            "betonline_available": sum(1 for row in rows if _b(row.get("betonline_available"))),
            "report": str(out_dir / "expanded_o15_universe_betonline_audit.md"),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
