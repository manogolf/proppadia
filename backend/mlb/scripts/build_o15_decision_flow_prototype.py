#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
PERF_DIR = Path("artifacts/analysis/mlb/review_aids/performance")
REVIEW_DIR = Path("artifacts/analysis/mlb/review_aids")
EXPANDED_ROWS = Path("artifacts/analysis/mlb/expanded_o15_universe/expanded_o15_universe_rows.csv")
USER_PROXY = Path("artifacts/analysis/mlb/user_over_15_filter_watch.csv")


@dataclass(frozen=True)
class Surface:
    label: str
    item_type: str
    universe: str
    population: str
    csv_path: Path
    performance_source: str
    performance_key: tuple[str, str, str] | None
    launch_csv: bool
    proxy_population: str = ""
    suggests_today: str = ""
    notes: str = ""


SURFACES = [
    Surface(
        label="Main O1.5 Watch Population",
        item_type="decision_surface",
        universe="main",
        population="watch",
        csv_path=REVIEW_DIR / "hits_o15_watch_candidates_{date}.csv",
        performance_source="population",
        performance_key=("main", "watch", "hits_o15_watch_candidates"),
        launch_csv=True,
        suggests_today="Use as a narrow main-market check after broader surfaces.",
        notes="Narrow main-market watch population.",
    ),
    Surface(
        label="Main O1.5 Expanded Review Population",
        item_type="decision_surface",
        universe="main",
        population="expanded_review",
        csv_path=REVIEW_DIR / "hits_o15_layered_candidates_{date}.csv",
        performance_source="population",
        performance_key=("main", "expanded_review", "hits_o15_layered_candidates"),
        launch_csv=True,
        suggests_today="Inspect main-market candidates by tier, expected hits context, and provenance.",
        notes="Main-market broader review surface.",
    ),
    Surface(
        label="Alternate O1.5 Discovery Universe",
        item_type="decision_surface",
        universe="alternate",
        population="alternate_discovery",
        csv_path=REVIEW_DIR / "hits_o15_alternate_discovery_{date}.csv",
        performance_source="population",
        performance_key=("alternate", "alternate_discovery", "hits_o15_alternate_discovery"),
        launch_csv=True,
        suggests_today="Primary plus-money discovery surface for morning pivots.",
        notes="Manual/research-only over-only alternate market surface.",
    ),
    Surface(
        label="Historical Context-Supported O1.5 Proxy Population",
        item_type="evidence_signal",
        universe="main",
        population="user_proxy",
        csv_path=USER_PROXY,
        performance_source="proxy",
        performance_key=None,
        launch_csv=False,
        proxy_population="user_filter_proxy_segment",
        suggests_today="Use as evidence supporting inspection of today's alternate and expanded decision surfaces.",
        notes="Historical proxy summary, not a current-slate candidate CSV.",
    ),
    Surface(
        label="Expanded O1.5 Universe",
        item_type="decision_surface",
        universe="expanded",
        population="expanded_universe",
        csv_path=EXPANDED_ROWS,
        performance_source="universe",
        performance_key=("expanded", "all", ""),
        launch_csv=True,
        suggests_today="Filter to the current slate date first, then pivot across source, tier, price, and context.",
        notes="Canonical research universe. Filter the CSV to today's date before pivoting.",
    ),
]

PIVOT_FIELDS = [
    ("Tier", "classification_value"),
    ("Price Bucket", "price_bucket"),
    ("Team", "team"),
    ("Opponent", "opponent"),
    ("Starter Expected Hits", "starter_expected_hits_allowed"),
    ("Team Expected Hits", "team_expected_hits_allowed"),
    ("d7 Hits", "d7_hits_rate"),
    ("d15 Hits", "d15_hits_rate"),
    ("Opportunity Type, advanced", "opportunity_type"),
    ("Provenance Layer, advanced", "provenance_layer"),
]


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


def _fmt_pct(value: Any) -> str:
    try:
        if value in (None, ""):
            return "n/a"
        return f"{float(value) * 100.0:.2f}%"
    except Exception:
        return "n/a"


def _fmt_int(value: Any) -> str:
    try:
        if value in (None, ""):
            return "n/a"
        return str(int(float(value)))
    except Exception:
        return "n/a"


def _rel_link(from_path: Path, to_path: Path, label: str) -> str:
    if not to_path.exists():
        return "`missing`"
    rel = Path("../../..")
    try:
        rel = Path(__import__("os").path.relpath(to_path, start=from_path.parent))
    except Exception:
        rel = to_path
    return f"[{label}]({rel.as_posix()})"


def _row_count_for_surface(path: Path, date_text: str, surface: Surface) -> str:
    rows = _read_csv(path)
    if not rows:
        return "0" if path.exists() else "MISSING"
    if surface.csv_path == EXPANDED_ROWS:
        return str(sum(1 for row in rows if str(row.get("date") or row.get("board_date") or "")[:10] == date_text))
    if surface.performance_source == "proxy":
        return "n/a"
    return str(len(rows))


def _population_perf(rows: list[dict[str, Any]], surface: Surface) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if surface.performance_key is None:
        return out
    universe, population, board_name = surface.performance_key
    for row in rows:
        if str(row.get("universe") or "") != universe:
            continue
        if str(row.get("population") or "") != population:
            continue
        if board_name and str(row.get("board_name") or "") != board_name:
            continue
        out[str(row.get("window") or "")] = row
    return out


def _proxy_perf(rows: list[dict[str, Any]], surface: Surface) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("population") or "") == surface.proxy_population:
            out[str(row.get("window") or "")] = {
                **row,
                "resolved": row.get("resolved_rows"),
                "roi": row.get("roi"),
            }
    return out


def _status(perf: dict[str, dict[str, Any]]) -> str:
    last30 = perf.get("last_30") or {}
    last14 = perf.get("last_14") or {}
    resolved = float(last30.get("resolved") or 0)
    roi30 = float(last30.get("roi") or 0) if last30.get("roi") not in (None, "") else None
    roi14 = float(last14.get("roi") or 0) if last14.get("roi") not in (None, "") else None
    if resolved < 20:
        return "too thin"
    if roi30 is not None and roi14 is not None and roi30 > 0.10 and roi14 > 0:
        return "strong"
    if roi30 is not None and roi30 < -0.05:
        return "weak"
    return "monitor"


def _surface_rows(date_text: str) -> list[dict[str, Any]]:
    population_perf = _read_csv(PERF_DIR / "decision_performance_population.csv")
    universe_perf = _read_csv(PERF_DIR / "decision_performance_universe.csv")
    proxy_perf_rows = _read_csv(USER_PROXY)
    rows: list[dict[str, Any]] = []
    for surface in SURFACES:
        path = Path(str(surface.csv_path).format(date=date_text))
        if surface.performance_source == "population":
            perf = _population_perf(population_perf, surface)
        elif surface.performance_source == "universe":
            perf = _population_perf(universe_perf, surface)
        else:
            perf = _proxy_perf(proxy_perf_rows, surface)
        full = perf.get("full_history") or {}
        last30 = perf.get("last_30") or {}
        last14 = perf.get("last_14") or {}
        last7 = perf.get("last_7") or {}
        rows.append(
            {
                "item_name": surface.label,
                "item_type": surface.item_type,
                "universe": surface.universe,
                "population": surface.population,
                "current_slate_row_count": _row_count_for_surface(path, date_text, surface),
                "historical_resolved_rows": full.get("resolved") or "",
                "full_history_roi": full.get("roi") or "",
                "last_30_roi": last30.get("roi") or "",
                "last_14_roi": last14.get("roi") or "",
                "last_7_roi": last7.get("roi") or "",
                "status": _status(perf),
                "launch_csv": "true" if surface.launch_csv else "false",
                "launch_csv_path": path.as_posix() if surface.launch_csv else "",
                "evidence_artifact_path": path.as_posix() if not surface.launch_csv else "",
                "suggested_pivot_fields": ";".join(f"{label} ({column})" for label, column in PIVOT_FIELDS),
                "what_it_suggests_inspecting_today": surface.suggests_today,
                "notes": surface.notes,
            }
        )
    return rows


def _write_md(path: Path, date_text: str, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ranked = sorted(
        rows,
        key=lambda row: (
            {"strong": 0, "monitor": 1, "weak": 2, "too thin": 3}.get(str(row.get("status")), 9),
            -(float(row.get("last_30_roi") or 0) if row.get("last_30_roi") not in ("", None) else -99),
        ),
    )
    evidence_rows = [row for row in ranked if row.get("item_type") == "evidence_signal"]
    decision_rows = [row for row in ranked if row.get("item_type") == "decision_surface"]
    lines = [
        "# O1.5 Decision Flow Prototype",
        "",
        f"- Current slate date: `{date_text}`",
        f"- Generated at: `{datetime.utcnow().replace(microsecond=0).isoformat()}Z`",
        "- Purpose: usability prototype only; no report replacement.",
        "",
        "Reports summarize. CSVs explore.",
        "",
        "Evidence tells us where to look. Decision surfaces are the CSVs we open and pivot.",
        "",
        "## A. Evidence Signals",
        "",
        "Historical confidence only. Do not use historical/evaluation CSVs as morning pivot launch points.",
        "",
    ]
    lines.extend(
        [
            "| Evidence Signal | status | resolved rows | full ROI | last 30 ROI | last 14 ROI | last 7 ROI | what it suggests inspecting today |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in evidence_rows:
        lines.append(
            f"| {row['item_name']} | `{row['status']}` | `{_fmt_int(row.get('historical_resolved_rows'))}` | `{_fmt_pct(row.get('full_history_roi'))}` | `{_fmt_pct(row.get('last_30_roi'))}` | `{_fmt_pct(row.get('last_14_roi'))}` | `{_fmt_pct(row.get('last_7_roi'))}` | {row.get('what_it_suggests_inspecting_today')} |"
        )
    lines.extend(
        [
            "",
            "Additional evidence sources: Review Aid Decision Performance and Expanded O1.5 research signals should guide which daily CSV to inspect, not replace slate review.",
            "",
            "## B. Today's Decision Surfaces",
            "",
            "These are CSVs intended for daily pivot/exploration.",
            "",
            "| Decision Surface | universe | population | current rows | launch CSV | historical confidence | suggested first pivots | caution |",
            "|---|---|---|---:|---|---|---|---|",
        ]
    )
    for row in decision_rows:
        link = _rel_link(path, Path(str(row["launch_csv_path"])), "open")
        pivots = ", ".join((row.get("suggested_pivot_fields") or "").split(";")[:6])
        confidence = f"{row['status']}; last 30 ROI {_fmt_pct(row.get('last_30_roi'))}; resolved {_fmt_int(row.get('historical_resolved_rows'))}"
        caution = "Filter to current date first." if row.get("population") == "expanded_universe" else row.get("notes")
        lines.append(
            f"| {row['item_name']} | `{row['universe']}` | `{row['population']}` | `{row['current_slate_row_count']}` | {link} | {confidence} | {pivots} | {caution} |"
        )
    lines.extend(
        [
            "",
            "## Suggested Pivot Fields",
            "",
            "Friendly names first, with actual column names in parentheses:",
            "",
        ]
    )
    lines.extend(f"- {label} (`{column}`)" for label, column in PIVOT_FIELDS)
    lines.extend(
        [
            "",
            "## Decision Caution",
            "",
            "- Do not pivot historical evidence files during morning decision flow unless you are doing research. Use them to decide which daily CSV to inspect.",
            "- Historical performance can guide attention but should not force today's decision.",
            "- Today's slate composition may differ from the historical winner profile.",
            "- Use history to choose where to look, then use today's CSV to decide whether today actually matches the profile.",
            "",
            "## What I Would Click First Today",
            "",
        ]
    )
    launchable = [
        row
        for row in decision_rows
        if str(row.get("launch_csv") or "").lower() == "true"
        and str(row.get("current_slate_row_count") or "").isdigit()
        and int(str(row.get("current_slate_row_count") or "0")) > 0
    ]
    for index, row in enumerate(launchable[:3], start=1):
        link = _rel_link(path, Path(str(row["launch_csv_path"])), row["item_name"])
        lines.append(
            f"{index}. {link}: `{row['status']}`, current rows `{row['current_slate_row_count']}`, last 30 ROI `{_fmt_pct(row.get('last_30_roi'))}`."
        )
    lines.extend(
        [
            "",
            "## Prototype Question",
            "",
            "Evidence-first is better for choosing where to spend attention. Decision-surface-first is better for actual morning execution once the slate is open. This prototype keeps both visible without making evidence artifacts look like launch CSVs.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build O1.5 decision-flow prototype report.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--out-dir", default=str(PERF_DIR))
    args = ap.parse_args()
    date_text = str(args.date)[:10]
    out_dir = Path(args.out_dir)
    rows = _surface_rows(date_text)
    _write_csv(out_dir / "o15_decision_flow_surfaces.csv", rows)
    _write_md(out_dir / "o15_decision_flow_prototype.md", date_text, rows)
    print(f"decision_flow_surfaces={len(rows)}")
    print(f"report={(out_dir / 'o15_decision_flow_prototype.md').as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
