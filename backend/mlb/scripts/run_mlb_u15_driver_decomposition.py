#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts import run_mlb_hits_15_tier_backtest as tier_base


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
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


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


def _date_from_path(path: Path) -> str:
    for part in path.parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _key(date: Any, player_id: Any, line: Any, side: Any) -> tuple[str, str, str, str]:
    pid = _i(player_id)
    line_v = _f(line)
    return (
        str(date or "")[:10],
        str(pid or ""),
        f"{line_v:.1f}" if line_v is not None else "",
        _clean(side),
    )


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


def _price_bucket(price: float | None) -> str:
    if price is None:
        return "missing"
    if price <= -300:
        return "<= -300"
    if price <= -250:
        return "-299 to -250"
    if price <= -200:
        return "-249 to -200"
    if price <= -150:
        return "-199 to -150"
    return "> -150"


def _result_state(row: dict[str, Any]) -> str:
    result = _clean(row.get("result"))
    if result in {"win", "won", "true", "1", "w"}:
        return "win"
    if result in {"loss", "lost", "false", "0", "l"}:
        return "loss"
    if result in {"push", "void", "refund", "tie"}:
        return "push"
    units = _f(row.get("units"))
    if units is not None:
        if units > 0:
            return "win"
        if units < 0:
            return "loss"
        return "push"
    return "unresolved"


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved_rows = [r for r in rows if _result_state(r) in {"win", "loss", "push"}]
    wins = sum(1 for r in resolved_rows if _result_state(r) == "win")
    losses = sum(1 for r in resolved_rows if _result_state(r) == "loss")
    pushes = sum(1 for r in resolved_rows if _result_state(r) == "push")
    resolved = len(resolved_rows)
    units = sum(_f(r.get("units")) or 0.0 for r in resolved_rows)

    def avg(col: str) -> float | None:
        vals = [_f(r.get(col)) for r in rows]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    if resolved < 10:
        sample_warning = "small_sample_lt_10"
    elif resolved < 25:
        sample_warning = "small_sample_lt_25"
    else:
        sample_warning = "ok"

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
        "avg_d7_hits_rate": avg("d7_hits_rate"),
        "avg_d15_hits_rate": avg("d15_hits_rate"),
        "avg_starter_expected_hits_allowed": avg("starter_expected_hits_allowed"),
        "sample_warning": sample_warning,
    }


def _load_lane_flags(lanes_root: Path) -> tuple[set[tuple[str, str, str, str]], set[tuple[str, str, str, str]]]:
    qc: set[tuple[str, str, str, str]] = set()
    ranking: set[tuple[str, str, str, str]] = set()
    for path_text in glob.glob(str(lanes_root / "today" / "*" / "*.csv")):
        path = Path(path_text)
        name = path.name
        if not (name.startswith("quick_card_hits_") or name.startswith("hits_lane_selector_")):
            continue
        if "_environment_diagnostics" in name:
            continue
        is_qc_file = name.startswith("quick_card_hits_")
        is_ranking_file = name.startswith("hits_lane_selector_") and "ranking_upload_input" not in name
        if not (is_qc_file or is_ranking_file):
            continue
        date_text = _date_from_path(path)
        for row in _read_csv(path):
            if _clean(row.get("prop_type")) != "hits":
                continue
            if _f(row.get("line")) != 1.5 or _clean(row.get("side")) != "under":
                continue
            key = _key(row.get("date") or date_text, row.get("player_id"), row.get("line"), row.get("side"))
            if not key[1]:
                continue
            if is_qc_file or _clean(row.get("source_lane")) == "quick_card_hits":
                qc.add(key)
            else:
                ranking.add(key)
    return qc, ranking


def _assign_context(rows: list[dict[str, Any]], qc_flags: set[tuple[str, str, str, str]], ranking_flags: set[tuple[str, str, str, str]]) -> None:
    for row in rows:
        d7 = _f(row.get("d7_hits_rate"))
        d15 = _f(row.get("d15_hits_rate"))
        starter = _f(row.get("starter_expected_hits_allowed"))
        key = _key(row.get("date"), row.get("player_id"), row.get("line"), row.get("side"))
        hitter = tier_base._u15_hitter_tier(d7, d15)
        pitcher = tier_base._u15_pitcher_tier(starter)
        row["hitter_tier"] = hitter
        row["pitcher_tier"] = pitcher
        row["combined_tier"] = f"{hitter}/{pitcher}"
        row["price_bucket"] = _price_bucket(_f(row.get("price")))
        row["qc_candidate"] = key in qc_flags
        row["ranking_candidate"] = key in ranking_flags
        row["d7_under_1"] = d7 is not None and d7 < 1.0
        row["d15_under_1"] = d15 is not None and d15 < 1.0
        row["d7_d15_under_1"] = bool(row["d7_under_1"] and row["d15_under_1"])
        row["starter_under_4_5"] = starter is not None and starter < 4.5
        row["starter_under_5_0"] = starter is not None and starter < 5.0


def _summarize_windows(
    rows: list[dict[str, Any]],
    latest: str,
    label: str,
    predicate: Callable[[dict[str, Any]], bool],
    extra: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for window in WINDOWS:
        wrows = [r for r in rows if window in _window_labels(str(r.get("date") or ""), latest)]
        selected = [r for r in wrows if predicate(r)]
        item = {"window": window, "segment": label}
        if extra:
            item.update(extra)
        item.update(_metrics(selected))
        out.append(item)
    return out


def _build_funnel(rows: list[dict[str, Any]], latest: str) -> list[dict[str, Any]]:
    specs: list[tuple[str, Callable[[dict[str, Any]], bool]]] = [
        ("All u1.5", lambda r: True),
        ("All u1.5 + d7 < 1.0", lambda r: bool(r.get("d7_under_1"))),
        ("All u1.5 + d15 < 1.0", lambda r: bool(r.get("d15_under_1"))),
        ("All u1.5 + d7 < 1.0 + d15 < 1.0", lambda r: bool(r.get("d7_d15_under_1"))),
        ("All u1.5 + starter < 4.5", lambda r: bool(r.get("starter_under_4_5"))),
        ("All u1.5 + starter < 5.0", lambda r: bool(r.get("starter_under_5_0"))),
        (
            "All u1.5 + d7+d15 < 1.0 + starter < 4.5",
            lambda r: bool(r.get("d7_d15_under_1") and r.get("starter_under_4_5")),
        ),
        ("QC only", lambda r: bool(r.get("qc_candidate"))),
        ("QC + d7 < 1.0", lambda r: bool(r.get("qc_candidate") and r.get("d7_under_1"))),
        ("QC + d15 < 1.0", lambda r: bool(r.get("qc_candidate") and r.get("d15_under_1"))),
        (
            "QC + d7+d15 < 1.0",
            lambda r: bool(r.get("qc_candidate") and r.get("d7_d15_under_1")),
        ),
        (
            "QC + d7+d15 < 1.0 + starter < 4.5",
            lambda r: bool(r.get("qc_candidate") and r.get("d7_d15_under_1") and r.get("starter_under_4_5")),
        ),
        ("Ranking only", lambda r: bool(r.get("ranking_candidate"))),
        (
            "Ranking + d7+d15 < 1.0 + starter < 4.5",
            lambda r: bool(r.get("ranking_candidate") and r.get("d7_d15_under_1") and r.get("starter_under_4_5")),
        ),
    ]
    out: list[dict[str, Any]] = []
    for label, predicate in specs:
        out.extend(_summarize_windows(rows, latest, label, predicate))
    return out


def _build_tier_rows(rows: list[dict[str, Any]], latest: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    dimensions = [
        ("combined_tier", "combined_tier"),
        ("hitter_tier", "hitter_tier"),
        ("pitcher_tier", "pitcher_tier"),
    ]
    for window in WINDOWS:
        wrows = [r for r in rows if window in _window_labels(str(r.get("date") or ""), latest)]
        for dimension, field in dimensions:
            for value in sorted({str(r.get(field) or "") for r in wrows}):
                selected = [r for r in wrows if str(r.get(field) or "") == value]
                item = {"window": window, "dimension": dimension, "tier": value}
                item.update(_metrics(selected))
                out.append(item)
    return out


def _build_price_rows(rows: list[dict[str, Any]], latest: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for window in WINDOWS:
        wrows = [r for r in rows if window in _window_labels(str(r.get("date") or ""), latest)]
        for bucket in ("<= -300", "-299 to -250", "-249 to -200", "-199 to -150", "> -150", "missing"):
            selected = [r for r in wrows if r.get("price_bucket") == bucket]
            item = {"window": window, "price_bucket": bucket}
            item.update(_metrics(selected))
            out.append(item)
    return out


def _top(rows: list[dict[str, Any]], key: str = "roi", min_resolved: int = 5, limit: int = 8) -> list[dict[str, Any]]:
    candidates = [r for r in rows if int(r.get("resolved") or 0) >= min_resolved and _f(r.get(key)) is not None]
    return sorted(candidates, key=lambda r: (_f(r.get(key)) or -999, int(r.get("resolved") or 0)), reverse=True)[:limit]


def _write_report(
    path: Path,
    rows: list[dict[str, Any]],
    funnel: list[dict[str, Any]],
    tiers: list[dict[str, Any]],
    prices: list[dict[str, Any]],
    latest: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    def table_line(row: dict[str, Any], label_col: str = "segment") -> str:
        return (
            f"| `{row.get(label_col)}` | `{row.get('rows')}` | `{row.get('resolved')}` | "
            f"`{row.get('wins')}` | `{row.get('losses')}` | `{row.get('pushes')}` | "
            f"`{_pct(_f(row.get('wr')))}` | `{_pct(_f(row.get('roi')))}` | "
            f"`{_num(_f(row.get('units')))}` | `{_num(_f(row.get('avg_odds')))}` | "
            f"`{_num(_f(row.get('avg_d7_hits_rate')))}` | `{_num(_f(row.get('avg_d15_hits_rate')))}` | "
            f"`{_num(_f(row.get('avg_starter_expected_hits_allowed')))}` | `{row.get('sample_warning')}` |"
        )

    full_funnel = [r for r in funnel if r.get("window") == "full_history"]
    recent_funnel = [r for r in funnel if r.get("window") == "last_14"]
    top_recent_tiers = _top(
        [r for r in tiers if r.get("window") in {"last_14", "last_7"} and r.get("dimension") == "combined_tier"],
        limit=10,
    )
    top_price = _top([r for r in prices if r.get("window") == "full_history"], limit=6)

    all_row = next((r for r in full_funnel if r.get("segment") == "All u1.5"), {})
    hitter_combo = next((r for r in full_funnel if r.get("segment") == "All u1.5 + d7 < 1.0 + d15 < 1.0"), {})
    starter = next((r for r in full_funnel if r.get("segment") == "All u1.5 + starter < 4.5"), {})
    combo = next((r for r in full_funnel if r.get("segment") == "All u1.5 + d7+d15 < 1.0 + starter < 4.5"), {})
    qc_combo = next((r for r in full_funnel if r.get("segment") == "QC + d7+d15 < 1.0 + starter < 4.5"), {})

    driver_lines = [
        "## What Appears To Drive u1.5 Value?",
        "",
        "- This is a review-aid decomposition only; no selector, upload, threshold, grading, or matching logic changed.",
    ]
    if all_row:
        driver_lines.append(f"- Baseline all u1.5: ROI `{_pct(_f(all_row.get('roi')))}` over `{all_row.get('resolved')}` resolved rows.")
    if hitter_combo:
        driver_lines.append(
            f"- Hitter weakness consistency (`d7 < 1.0` and `d15 < 1.0`): ROI `{_pct(_f(hitter_combo.get('roi')))}`."
        )
    if starter:
        driver_lines.append(f"- Tough starter alone (`starter < 4.5`): ROI `{_pct(_f(starter.get('roi')))}`.")
    if combo:
        driver_lines.append(
            f"- Hitter weakness + tough starter: ROI `{_pct(_f(combo.get('roi')))}` over `{combo.get('resolved')}` resolved rows."
        )
    if qc_combo and int(qc_combo.get("resolved") or 0):
        driver_lines.append(
            f"- QC agreement within that intersection: ROI `{_pct(_f(qc_combo.get('roi')))}` over `{qc_combo.get('resolved')}` resolved rows."
        )
    else:
        driver_lines.append("- QC/ranking intersections are sparse for u1.5 line 1.5 in the available lane artifacts.")

    lines = [
        "# Hits Under 1.5 Driver Decomposition",
        "",
        f"- Generated: `{datetime.now(timezone.utc).isoformat()}`",
        f"- Latest completed slate in source rows: `{latest or 'n/a'}`",
        f"- Candidate universe: `prop_type=hits`, `side=under`, `line=1.5` from execution-vs-model reconcile rows.",
        "- Starter context uses the same reconstruction path as the hits 1.5 tier backtest.",
        "",
        "## Full-History Funnel",
        "",
        "| segment | rows | resolved | wins | losses | pushes | WR | ROI | units | avg odds | avg d7 | avg d15 | avg starter exp | sample |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    lines.extend(table_line(row) for row in full_funnel)
    lines.extend(
        [
            "",
            "## Last 14 Funnel",
            "",
            "| segment | rows | resolved | wins | losses | pushes | WR | ROI | units | avg odds | avg d7 | avg d15 | avg starter exp | sample |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    lines.extend(table_line(row) for row in recent_funnel)
    lines.extend(
        [
            "",
            "## Top Recent Combined Tiers",
            "",
            "| tier | window | rows | resolved | wins | losses | pushes | WR | ROI | units | avg odds | sample |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in top_recent_tiers:
        lines.append(
            f"| `{row.get('tier')}` | `{row.get('window')}` | `{row.get('rows')}` | `{row.get('resolved')}` | "
            f"`{row.get('wins')}` | `{row.get('losses')}` | `{row.get('pushes')}` | "
            f"`{_pct(_f(row.get('wr')))}` | `{_pct(_f(row.get('roi')))}` | "
            f"`{_num(_f(row.get('units')))}` | `{_num(_f(row.get('avg_odds')))}` | `{row.get('sample_warning')}` |"
        )
    lines.extend(
        [
            "",
            "## Full-History Price Buckets",
            "",
            "| price_bucket | rows | resolved | wins | losses | pushes | WR | ROI | units | avg odds | sample |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in top_price:
        lines.append(
            f"| `{row.get('price_bucket')}` | `{row.get('rows')}` | `{row.get('resolved')}` | "
            f"`{row.get('wins')}` | `{row.get('losses')}` | `{row.get('pushes')}` | "
            f"`{_pct(_f(row.get('wr')))}` | `{_pct(_f(row.get('roi')))}` | "
            f"`{_num(_f(row.get('units')))}` | `{_num(_f(row.get('avg_odds')))}` | `{row.get('sample_warning')}` |"
        )
    lines.extend([""] + driver_lines)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Decompose outcome-backed hits under 1.5 favorite drivers.")
    ap.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    rows = [
        r
        for r in tier_base._load_reconcile_rows(ROOT / args.execution_root)
        if _clean(r.get("side")) == "under" and _f(r.get("line")) == 1.5
    ]
    latest = max([str(r.get("date") or "") for r in rows], default="")
    qc_flags, ranking_flags = _load_lane_flags(ROOT / args.lanes_root)
    _assign_context(rows, qc_flags, ranking_flags)

    funnel = _build_funnel(rows, latest)
    tiers = _build_tier_rows(rows, latest)
    prices = _build_price_rows(rows, latest)

    funnel_csv = out_dir / "u15_driver_decomposition_funnel.csv"
    tier_csv = out_dir / "u15_driver_tier_performance.csv"
    price_csv = out_dir / "u15_driver_price_buckets.csv"
    report_md = out_dir / "u15_driver_decomposition.md"
    _write_csv(funnel_csv, funnel)
    _write_csv(tier_csv, tiers)
    _write_csv(price_csv, prices)
    _write_report(report_md, rows, funnel, tiers, prices, latest)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_completed_slate": latest,
        "source_rows": len(rows),
        "qc_under_15_keys": len(qc_flags),
        "ranking_under_15_keys": len(ranking_flags),
        "outputs": {
            "report": _rel(report_md),
            "funnel_csv": _rel(funnel_csv),
            "tier_csv": _rel(tier_csv),
            "price_csv": _rel(price_csv),
        },
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
