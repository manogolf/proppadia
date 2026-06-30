#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import accumulate
from pathlib import Path
from statistics import mean, median
from typing import Any


DEFAULT_ROWS = Path("artifacts/analysis/mlb/review_aids/offensive_environment_v2_beta_dashboard_rows_2026-06-29.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/review_aids")
DATE_LABEL = "2026-06-29"

WINDOWS = [
    ("full_available", None),
    ("last_60", 60),
    ("last_45", 45),
    ("last_30", 30),
    ("last_21", 21),
    ("last_14", 14),
    ("last_7", 7),
]

CORE_FAMILIES = [
    "aligned_high_environment",
    "starter_led_with_bullpen_drag",
    "team_high_starter_mediocre",
    "starter_high_team_mediocre",
]

BASELINE_SPECS = [
    ("all_o15_baseline", "All O1.5 baseline", lambda r: True),
    ("combined_tier_A/A", "Current A/A", lambda r: str(r.get("combined_tier")) == "A/A"),
    ("combined_tier_A/B", "Current A/B", lambda r: str(r.get("combined_tier")) == "A/B"),
    ("combined_tier_C/A", "Current C/A", lambda r: str(r.get("combined_tier")) == "C/A"),
    ("aligned_high_environment", "Aligned High Environment", lambda r: str(r.get("env_v2_beta_profile_family")) == "aligned_high_environment"),
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        writer = csv.DictWriter(fh, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _f(value: Any) -> float | None:
    try:
        if value is None:
            return None
        text = str(value).strip()
        if not text or text.lower() in {"nan", "none", "null"}:
            return None
        return float(text)
    except Exception:
        return None


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def _date(value: Any) -> datetime | None:
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d")
    except Exception:
        return None


def _week_start(d: datetime) -> datetime:
    return d - timedelta(days=d.weekday())


def _american_implied(price: float | None) -> float | None:
    if price is None:
        return None
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return 100.0 / (price + 100.0)


def _window_rows(rows: list[dict[str, Any]], days: int | None, max_date: datetime) -> list[dict[str, Any]]:
    if days is None:
        return rows
    cutoff = max_date - timedelta(days=days - 1)
    return [row for row in rows if (d := _date(row.get("date"))) is not None and d >= cutoff]


def _sample_flag(resolved: int) -> str:
    if resolved < 25:
        return "small_lt_25"
    if resolved < 50:
        return "thin_lt_50"
    return "ok"


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if _clean(row.get("result")) in {"win", "loss", "push"}]
    wins = sum(1 for row in resolved if _clean(row.get("result")) == "win")
    losses = sum(1 for row in resolved if _clean(row.get("result")) == "loss")
    pushes = sum(1 for row in resolved if _clean(row.get("result")) == "push")
    graded = wins + losses
    units = sum((_f(row.get("units")) or 0.0) for row in resolved)
    odds = [_f(row.get("price")) for row in resolved if _f(row.get("price")) is not None]
    avg_odds = mean(odds) if odds else None
    be = _american_implied(avg_odds)
    wr = wins / graded if graded else None
    return {
        "rows": len(rows),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wr if wr is not None else "",
        "roi": (units / len(resolved)) if resolved else "",
        "units": units,
        "avg_odds": avg_odds if avg_odds is not None else "",
        "median_odds": median(odds) if odds else "",
        "break_even_wr": be if be is not None else "",
        "wr_minus_break_even": (wr - be) if wr is not None and be is not None else "",
        "sample_flag": _sample_flag(len(resolved)),
    }


def _max_drawdown(rows: list[dict[str, Any]]) -> float | str:
    resolved = [
        row
        for row in sorted(rows, key=lambda r: (str(r.get("date")), str(r.get("player_id")), str(r.get("player_name"))))
        if _clean(row.get("result")) in {"win", "loss", "push"}
    ]
    if not resolved:
        return ""
    cum = list(accumulate((_f(row.get("units")) or 0.0) for row in resolved))
    peak = 0.0
    max_dd = 0.0
    for value in cum:
        peak = max(peak, value)
        max_dd = min(max_dd, value - peak)
    return max_dd


def _largest_date_contribution(rows: list[dict[str, Any]]) -> tuple[str, float, int]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("date") or "")[:10]].append(row)
    if not by_date:
        return "", 0.0, 0
    best_date, best_rows = max(by_date.items(), key=lambda item: abs(sum((_f(r.get("units")) or 0.0) for r in item[1])))
    return best_date, sum((_f(r.get("units")) or 0.0) for r in best_rows), len(best_rows)


def _concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total_units = sum((_f(row.get("units")) or 0.0) for row in rows if _clean(row.get("result")) in {"win", "loss", "push"})
    date_text, date_units, date_rows = _largest_date_contribution(rows)
    return {
        "max_drawdown_units": _max_drawdown(rows),
        "largest_contributing_date": date_text,
        "largest_contributing_date_units": date_units,
        "largest_contributing_date_rows": date_rows,
        "largest_date_units_share_abs": (abs(date_units) / abs(total_units)) if total_units else "",
    }


def _positive_window_count(perf_rows: list[dict[str, Any]]) -> tuple[int, int]:
    usable = [row for row in perf_rows if int(row.get("resolved") or 0) >= 25 and _f(row.get("roi")) is not None]
    positive = [row for row in usable if (_f(row.get("roi")) or 0.0) > 0.0]
    return len(positive), len(usable)


def _window_stability(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [d for d in (_date(row.get("date")) for row in rows) if d is not None]
    max_date = max(dates) if dates else datetime(2026, 6, 29)
    out: list[dict[str, Any]] = []
    by_family = sorted({str(row.get("env_v2_beta_profile_family") or "missing") for row in rows if str(row.get("env_v2_beta_profile_family")) in CORE_FAMILIES})
    family_rows_by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for family in by_family:
            group = [row for row in wrows if str(row.get("env_v2_beta_profile_family")) == family]
            metrics = _metrics(group)
            family_rows_by_window[family].append({"window": window, **metrics})
            out.append(
                {
                    "window": window,
                    "profile_family": family,
                    "profile_label": str(group[0].get("env_v2_beta_profile_label") if group else family),
                    **metrics,
                    **_concentration(group),
                    "research_only_note": "Environment v2-beta stability validation; not a production rule",
                }
            )
    for family, perf_rows in family_rows_by_window.items():
        pos, usable = _positive_window_count(perf_rows)
        for row in out:
            if row.get("profile_family") == family:
                row["positive_usable_windows"] = pos
                row["usable_windows"] = usable
                row["window_stability_status"] = (
                    "positive_across_all_usable_windows"
                    if usable and pos == usable
                    else "mostly_positive"
                    if usable and pos >= max(1, usable - 1)
                    else "mixed_or_negative"
                )
    return out


def _slice_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for slice_kind in ("week", "date"):
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            family = str(row.get("env_v2_beta_profile_family") or "")
            if family not in CORE_FAMILIES:
                continue
            d = _date(row.get("date"))
            if d is None:
                continue
            key = _week_start(d).strftime("%Y-%m-%d") if slice_kind == "week" else d.strftime("%Y-%m-%d")
            grouped[(family, key)].append(row)
        for (family, key), group in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
            metrics = _metrics(group)
            out.append(
                {
                    "slice_kind": slice_kind,
                    "slice": key,
                    "profile_family": family,
                    "profile_label": str(group[0].get("env_v2_beta_profile_label") or family),
                    **metrics,
                    "research_only_note": "Weekly/date slice for concentration diagnostics only",
                }
            )
    return out


def _baseline_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [d for d in (_date(row.get("date")) for row in rows) if d is not None]
    max_date = max(dates) if dates else datetime(2026, 6, 29)
    out: list[dict[str, Any]] = []
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for key, label, pred in BASELINE_SPECS:
            group = [row for row in wrows if pred(row)]
            metrics = _metrics(group)
            out.append(
                {
                    "window": window,
                    "comparison_key": key,
                    "comparison_label": label,
                    **metrics,
                    "research_only_note": "Baseline comparison only; current tiers are unchanged",
                }
            )
    return out


def _fmt_pct(value: Any) -> str:
    v = _f(value)
    if v is None:
        return ""
    return f"{v:.2%}"


def _fmt(value: Any, digits: int = 2) -> str:
    v = _f(value)
    if v is None:
        return ""
    return f"{v:.{digits}f}"


def _get(rows: list[dict[str, Any]], family: str, window: str = "full_available") -> dict[str, Any]:
    for row in rows:
        if row.get("profile_family") == family and row.get("window") == window:
            return row
    return {}


def _write_report(path: Path, windows: list[dict[str, Any]], slices: list[dict[str, Any]], baselines: list[dict[str, Any]]) -> None:
    aligned = _get(windows, "aligned_high_environment")
    drag = _get(windows, "starter_led_with_bullpen_drag")
    team_disagree = _get(windows, "team_high_starter_mediocre")
    starter_disagree = _get(windows, "starter_high_team_mediocre")
    full_baselines = [row for row in baselines if row.get("window") == "full_available"]
    lines = [
        "# Offensive Environment v2-beta Stability Validation",
        "",
        f"- Generated at: `{_now()}`",
        "- Scope: research-only stability validation.",
        "- Production behavior changed: `no`",
        "- Morning Workbench/Ops Brief exposure changed: `no`",
        "- Tier assignment changed: `no`",
        "",
        "## Rolling Window Stability",
        "",
        "| window | family | resolved | W-L-P | WR | ROI | units | WR-BE | max drawdown | positive windows | status |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in windows:
        if row.get("profile_family") not in CORE_FAMILIES:
            continue
        lines.append(
            f"| `{row.get('window')}` | `{row.get('profile_label')}` | `{row.get('resolved')}` | "
            f"`{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | `{_fmt_pct(row.get('wr'))}` | "
            f"`{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('units'))}` | `{_fmt_pct(row.get('wr_minus_break_even'))}` | "
            f"`{_fmt(row.get('max_drawdown_units'))}` | `{row.get('positive_usable_windows')}/{row.get('usable_windows')}` | "
            f"`{row.get('window_stability_status')}` |"
        )
    lines.extend(
        [
            "",
            "## Baseline Comparison",
            "",
            "| comparison | resolved | W-L-P | WR | ROI | units | avg odds |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in full_baselines:
        lines.append(
            f"| `{row.get('comparison_label')}` | `{row.get('resolved')}` | `{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | "
            f"`{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('units'))}` | `{_fmt(row.get('avg_odds'))}` |"
        )
    lines.extend(
        [
            "",
            "## Concentration Read",
            "",
        ]
    )
    if aligned:
        lines.append(
            f"- Aligned High Environment full-window units: `{_fmt(aligned.get('units'))}` with max drawdown `{_fmt(aligned.get('max_drawdown_units'))}`. Largest contributing date: `{aligned.get('largest_contributing_date')}` for `{_fmt(aligned.get('largest_contributing_date_units'))}` units."
        )
        lines.append(
            f"- Aligned High Environment usable-window positivity: `{aligned.get('positive_usable_windows')}/{aligned.get('usable_windows')}`."
        )
    if drag:
        lines.append(
            f"- Starter-Led With Bullpen Drag remains positive full-window at `{_fmt_pct(drag.get('roi'))}` ROI, but weaker than aligned high."
        )
    if team_disagree and starter_disagree:
        lines.append(
            f"- Team High / Starter Mediocre remains a negative warning profile at `{_fmt_pct(team_disagree.get('roi'))}` ROI. Starter High / Team Mediocre is closer to flat at `{_fmt_pct(starter_disagree.get('roi'))}` ROI."
        )
    lines.extend(
        [
            "",
            "## Weekly / Date Slices",
            "",
            f"- Slice rows written: `{len(slices)}`",
            "- Use the slice CSV to inspect week-by-week and date-level concentration before any future promotion discussion.",
            "",
            "## Final Recommendation",
            "",
            "- Aligned High Environment is promising but still concentrated enough to require continued research visibility only.",
            "- Bullpen drag effect is directionally stable enough for continued beta tracking, not production use.",
            "- Disagreement families should remain warnings/context, especially Team High / Starter Mediocre.",
            "- v2-beta deserves continued research.",
            "- It is too early for Morning Workbench exposure.",
            "- No production migration is recommended.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate research-only MLB Hits 1.5 Environment v2-beta profile stability.")
    ap.add_argument("--rows", type=Path, default=DEFAULT_ROWS)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    rows = _read_csv(args.rows)
    windows = _window_stability(rows)
    slices = _slice_rows(rows)
    baselines = _baseline_rows(rows)

    report_md = args.out_dir / f"offensive_environment_v2_beta_stability_validation_{DATE_LABEL}.md"
    windows_csv = args.out_dir / f"offensive_environment_v2_beta_stability_windows_{DATE_LABEL}.csv"
    slices_csv = args.out_dir / f"offensive_environment_v2_beta_stability_slices_{DATE_LABEL}.csv"
    baseline_csv = args.out_dir / f"offensive_environment_v2_beta_baseline_comparison_{DATE_LABEL}.csv"
    _write_csv(windows_csv, windows)
    _write_csv(slices_csv, slices)
    _write_csv(baseline_csv, baselines)
    _write_report(report_md, windows, slices, baselines)
    print(
        {
            "report_md": str(report_md),
            "window_stability_csv": str(windows_csv),
            "weekly_date_slice_csv": str(slices_csv),
            "baseline_comparison_csv": str(baseline_csv),
            "window_rows": len(windows),
            "slice_rows": len(slices),
            "baseline_rows": len(baselines),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
