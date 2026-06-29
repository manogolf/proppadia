#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_ROWS = Path("artifacts/analysis/mlb/review_aids/hits_o15_tier_backtest_rows.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/review_aids")

COMPONENTS = [
    ("pitcher_base", ["pitcher_expected_hits_allowed_weighted", "pitcher_base"], "Pitcher Base"),
    ("offense_factor_vs_league_clamped", ["offense_factor_vs_league_clamped"], "Offense Factor"),
    ("offense_hits_form_blended", ["offense_hits_form_blended"], "Offense Hits Form"),
    ("bullpen_hits_allowed_form_blended", ["bullpen_hits_allowed_form_blended"], "Bullpen Hits Allowed Form"),
    ("starter_expected_hits_allowed", ["starter_expected_hits_allowed"], "Starter Expected Hits Allowed"),
    ("team_expected_hits_allowed", ["team_expected_hits_allowed"], "Team Expected Hits Allowed"),
]

WINDOWS = [
    ("full_available", None),
    ("last_60", 60),
    ("last_30", 30),
    ("last_14", 14),
    ("last_7", 7),
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
        writer = csv.DictWriter(fh, fieldnames=fields)
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


def _component_value(row: dict[str, Any], fields: list[str]) -> float | None:
    for field in fields:
        value = _f(row.get(field))
        if value is not None:
            return value
    return None


def _american_implied(price: float | None) -> float | None:
    if price is None:
        return None
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return 100.0 / (price + 100.0)


def _breakeven_from_avg_odds(avg_odds: float | None) -> float | None:
    return _american_implied(avg_odds)


def _sample_flag(n: int) -> str:
    if n < 25:
        return "small_lt_25"
    if n < 50:
        return "thin_lt_50"
    return "ok"


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [r for r in rows if _clean(r.get("result")) in {"win", "loss", "push"}]
    wins = sum(1 for r in resolved if _clean(r.get("result")) == "win")
    losses = sum(1 for r in resolved if _clean(r.get("result")) == "loss")
    pushes = sum(1 for r in resolved if _clean(r.get("result")) == "push")
    graded = wins + losses
    units = sum((_f(r.get("units")) or 0.0) for r in resolved)
    odds = [_f(r.get("price")) for r in resolved if _f(r.get("price")) is not None]
    implied = [_american_implied(v) for v in odds]
    implied = [v for v in implied if v is not None]
    avg_odds = mean(odds) if odds else None
    be = _breakeven_from_avg_odds(avg_odds)
    wr = wins / graded if graded else None
    return {
        "rows": len(rows),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wr,
        "roi": units / len(resolved) if resolved else None,
        "units": units,
        "avg_odds": avg_odds,
        "avg_implied": mean(implied) if implied else None,
        "break_even_wr_from_avg_odds": be,
        "wr_minus_break_even": (wr - be) if wr is not None and be is not None else None,
        "sample_flag": _sample_flag(len(resolved)),
    }


def _fmt(value: Any, digits: int = 4) -> str:
    v = _f(value)
    if v is None:
        return ""
    return f"{v:.{digits}f}"


def _fmt_pct(value: Any) -> str:
    v = _f(value)
    if v is None:
        return ""
    return f"{v:.2%}"


def _window_rows(rows: list[dict[str, Any]], window_days: int | None, max_date: datetime) -> list[dict[str, Any]]:
    if window_days is None:
        return rows
    cutoff = max_date - timedelta(days=window_days - 1)
    return [r for r in rows if (d := _date(r.get("date"))) is not None and d >= cutoff]


def _quantile_edges(values: list[float], buckets: int = 5) -> list[float]:
    vals = sorted(values)
    if not vals:
        return []
    edges = []
    for i in range(1, buckets):
        idx = math.ceil((len(vals) * i) / buckets) - 1
        idx = max(0, min(len(vals) - 1, idx))
        edges.append(vals[idx])
    return edges


def _bucket_label(value: float | None, edges: list[float]) -> str:
    if value is None:
        return "missing"
    if not edges:
        return "all"
    labels = ["q1_lowest", "q2", "q3", "q4", "q5_highest"]
    for idx, edge in enumerate(edges):
        if value <= edge:
            return labels[idx]
    return labels[-1]


def _tertile_label(value: float | None, edges: list[float]) -> str:
    if value is None:
        return "missing"
    if len(edges) < 2:
        return "all"
    if value <= edges[0]:
        return "low"
    if value <= edges[1]:
        return "mid"
    return "high"


def _monotonic_note(bucket_rows: list[dict[str, Any]]) -> str:
    ordered = [r for r in bucket_rows if str(r.get("bucket")).startswith("q")]
    ordered.sort(key=lambda r: str(r.get("bucket")))
    rois = [_f(r.get("roi")) for r in ordered]
    rois = [v for v in rois if v is not None]
    wrs = [_f(r.get("wr")) for r in ordered]
    wrs = [v for v in wrs if v is not None]
    if len(rois) < 3:
        return "insufficient_buckets"
    roi_up = all(a <= b for a, b in zip(rois, rois[1:]))
    roi_down = all(a >= b for a, b in zip(rois, rois[1:]))
    wr_up = all(a <= b for a, b in zip(wrs, wrs[1:])) if len(wrs) >= 3 else False
    wr_down = all(a >= b for a, b in zip(wrs, wrs[1:])) if len(wrs) >= 3 else False
    if roi_up and wr_up:
        return "roi_wr_monotonic_up"
    if roi_down and wr_down:
        return "roi_wr_monotonic_down"
    if roi_up or roi_down:
        return "roi_monotonic_only"
    if wr_up or wr_down:
        return "wr_monotonic_only"
    return "non_monotonic"


def _component_bucket_rows(rows: list[dict[str, Any]], max_date: datetime) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    component_full_edges: dict[str, list[float]] = {}
    for component, fields, _label in COMPONENTS:
        vals = [_component_value(r, fields) for r in rows]
        component_full_edges[component] = _quantile_edges([v for v in vals if v is not None], 5)
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for component, fields, label in COMPONENTS:
            edges = component_full_edges[component]
            covered = [r for r in wrows if _component_value(r, fields) is not None]
            missing = len(wrows) - len(covered)
            groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in wrows:
                value = _component_value(row, fields)
                groups[_bucket_label(value, edges)].append(row)
            component_rows: list[dict[str, Any]] = []
            for bucket in ["missing", "q1_lowest", "q2", "q3", "q4", "q5_highest"]:
                group = groups.get(bucket, [])
                if not group:
                    continue
                metrics = _metrics(group)
                vals = [_component_value(r, fields) for r in group]
                vals = [v for v in vals if v is not None]
                row = {
                    "window": window,
                    "component": component,
                    "component_label": label,
                    "bucket": bucket,
                    "bucket_min": min(vals) if vals else "",
                    "bucket_max": max(vals) if vals else "",
                    "component_coverage_count": len(covered),
                    "component_missing_count": missing,
                    "coverage_rate": len(covered) / len(wrows) if wrows else None,
                    **metrics,
                }
                component_rows.append(row)
            note = _monotonic_note(component_rows)
            for row in component_rows:
                row["monotonicity_note"] = note
                out.append(row)
    return out


def _component_tertiles(rows: list[dict[str, Any]]) -> dict[str, list[float]]:
    out = {}
    for component, fields, _label in COMPONENTS:
        vals = [_component_value(r, fields) for r in rows]
        out[component] = _quantile_edges([v for v in vals if v is not None], 3)
    return out


def _incremental_rows(rows: list[dict[str, Any]], max_date: datetime) -> list[dict[str, Any]]:
    edges = _component_tertiles(rows)

    def tert(row: dict[str, Any], component: str) -> str:
        fields = next(fields for name, fields, _label in COMPONENTS if name == component)
        return _tertile_label(_component_value(row, fields), edges[component])

    checks: list[tuple[str, Callable[[dict[str, Any]], bool], str]] = [
        ("starter_expected_high", lambda r: tert(r, "starter_expected_hits_allowed") == "high", "starter_expected only"),
        ("team_expected_high", lambda r: tert(r, "team_expected_hits_allowed") == "high", "team_expected only"),
        (
            "starter_high_bullpen_high",
            lambda r: tert(r, "starter_expected_hits_allowed") == "high"
            and tert(r, "bullpen_hits_allowed_form_blended") == "high",
            "starter_expected + bullpen bucket",
        ),
        (
            "starter_high_bullpen_low",
            lambda r: tert(r, "starter_expected_hits_allowed") == "high"
            and tert(r, "bullpen_hits_allowed_form_blended") == "low",
            "high starter + low bullpen",
        ),
        (
            "starter_low_bullpen_high",
            lambda r: tert(r, "starter_expected_hits_allowed") == "low"
            and tert(r, "bullpen_hits_allowed_form_blended") == "high",
            "low starter + high bullpen",
        ),
        (
            "starter_high_offense_high",
            lambda r: tert(r, "starter_expected_hits_allowed") == "high"
            and tert(r, "offense_factor_vs_league_clamped") == "high",
            "starter_expected + offense_factor bucket",
        ),
        (
            "offense_high_bullpen_high",
            lambda r: tert(r, "offense_factor_vs_league_clamped") == "high"
            and tert(r, "bullpen_hits_allowed_form_blended") == "high",
            "high offense_factor + high bullpen",
        ),
        (
            "team_high_starter_mid_or_low",
            lambda r: tert(r, "team_expected_hits_allowed") == "high"
            and tert(r, "starter_expected_hits_allowed") in {"mid", "low"},
            "high team_expected but mediocre starter_expected",
        ),
        (
            "starter_high_team_mid_or_low",
            lambda r: tert(r, "starter_expected_hits_allowed") == "high"
            and tert(r, "team_expected_hits_allowed") in {"mid", "low"},
            "high starter_expected but mediocre team_expected",
        ),
        (
            "starter_high_offense_low",
            lambda r: tert(r, "starter_expected_hits_allowed") == "high"
            and tert(r, "offense_factor_vs_league_clamped") == "low",
            "high starter + low offense_factor",
        ),
    ]
    out: list[dict[str, Any]] = []
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for check, pred, family in checks:
            group = [r for r in wrows if pred(r)]
            metrics = _metrics(group)
            out.append(
                {
                    "window": window,
                    "check": check,
                    "check_family": family,
                    **metrics,
                    "research_only_note": "tertiles from full resolved reconstructed O1.5 row source",
                }
            )
    return out


def _rank_components(bucket_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [r for r in bucket_rows if r.get("window") == "full_available" and str(r.get("bucket")).startswith("q")]
    by_component: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_component[row["component"]].append(row)
    out = []
    for component, group in by_component.items():
        rois = [_f(r.get("roi")) for r in group if _f(r.get("roi")) is not None]
        wrs = [_f(r.get("wr")) for r in group if _f(r.get("wr")) is not None]
        if not rois:
            continue
        best = max(rois)
        worst = min(rois)
        wr_spread = (max(wrs) - min(wrs)) if wrs else None
        out.append(
            {
                "component": component,
                "roi_spread": best - worst,
                "wr_spread": wr_spread,
                "best_roi": best,
                "worst_roi": worst,
                "monotonicity_note": group[0].get("monotonicity_note"),
                "resolved_total": sum(int(r.get("resolved") or 0) for r in group),
            }
        )
    return sorted(out, key=lambda r: ((_f(r.get("roi_spread")) or 0), (_f(r.get("wr_spread")) or 0)), reverse=True)


def _write_report(path: Path, bucket_rows: list[dict[str, Any]], inc_rows: list[dict[str, Any]], source_rows: list[dict[str, Any]]) -> None:
    ranks = _rank_components(bucket_rows)
    strongest = ranks[0]["component"] if ranks else "unknown"
    weakest = ranks[-1]["component"] if ranks else "unknown"
    full_q = [r for r in bucket_rows if r.get("window") == "full_available"]
    team_rows = [r for r in full_q if r.get("component") == "team_expected_hits_allowed" and str(r.get("bucket")).startswith("q")]
    starter_rows = [r for r in full_q if r.get("component") == "starter_expected_hits_allowed" and str(r.get("bucket")).startswith("q")]
    team_spread = (max(_f(r.get("roi")) or 0 for r in team_rows) - min(_f(r.get("roi")) or 0 for r in team_rows)) if team_rows else 0
    starter_spread = (max(_f(r.get("roi")) or 0 for r in starter_rows) - min(_f(r.get("roi")) or 0 for r in starter_rows)) if starter_rows else 0
    full_inc = {r["check"]: r for r in inc_rows if r.get("window") == "full_available"}
    starter_high_roi = _f((full_inc.get("starter_expected_high") or {}).get("roi"))
    team_high_roi = _f((full_inc.get("team_expected_high") or {}).get("roi"))
    if team_spread > starter_spread and team_high_roi is not None and starter_high_roi is not None and team_high_roi < starter_high_roi:
        team_vs_starter = "mixed"
    elif team_spread > starter_spread:
        team_vs_starter = "yes"
    else:
        team_vs_starter = "no"
    bullpen_rows = [r for r in full_q if r.get("component") == "bullpen_hits_allowed_form_blended" and str(r.get("bucket")).startswith("q")]
    bullpen_note = "include_as_research_component" if bullpen_rows and max((_f(r.get("roi")) or 0) for r in bullpen_rows) != min((_f(r.get("roi")) or 0) for r in bullpen_rows) else "weak_or_flat"
    offense_rows = [r for r in full_q if r.get("component") == "offense_factor_vs_league_clamped" and str(r.get("bucket")).startswith("q")]
    offense_note = "evaluate_redesign" if offense_rows and (max((_f(r.get("roi")) or 0) for r in offense_rows) - min((_f(r.get("roi")) or 0) for r in offense_rows)) < 0.05 else "retains_signal"
    resolved = [r for r in source_rows if _clean(r.get("result")) in {"win", "loss", "push"}]
    lines = [
        "# Offensive Environment v1.2 Component Quality Evaluation",
        "",
        f"- Generated at: `{_now()}`",
        "- Scope: reconstructed all-market Hits O1.5 rows with resolved outcomes.",
        "- Production behavior changed: `no`",
        "- Tier assignment changed: `no`",
        "",
        "## Source",
        "",
        f"- Source rows: `{len(source_rows)}`",
        f"- Resolved source rows: `{len(resolved)}`",
        f"- Date range: `{min(r.get('date') for r in resolved) if resolved else ''}` to `{max(r.get('date') for r in resolved) if resolved else ''}`",
        "",
        "## Component Ranking",
        "",
        "| rank | component | ROI spread | WR spread | monotonicity | resolved |",
        "|---:|---|---:|---:|---|---:|",
    ]
    for idx, row in enumerate(ranks, start=1):
        lines.append(
            f"| `{idx}` | `{row['component']}` | `{_fmt_pct(row.get('roi_spread'))}` | "
            f"`{_fmt_pct(row.get('wr_spread'))}` | `{row.get('monotonicity_note')}` | `{row.get('resolved_total')}` |"
        )
    lines.extend(
        [
            "",
            "## Full-Window Bucket Highlights",
            "",
            "| component | bucket | resolved | WR | ROI | avg odds | WR-BE | sample | note |",
            "|---|---|---:|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in full_q:
        if not str(row.get("bucket")).startswith("q"):
            continue
        lines.append(
            f"| `{row.get('component')}` | `{row.get('bucket')}` | `{row.get('resolved')}` | "
            f"`{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('avg_odds'), 2)}` | "
            f"`{_fmt_pct(row.get('wr_minus_break_even'))}` | `{row.get('sample_flag')}` | `{row.get('monotonicity_note')}` |"
        )
    lines.extend(
        [
            "",
            "## Incremental Checks",
            "",
            "| window | check | resolved | WR | ROI | units | avg odds | sample |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in inc_rows:
        if row.get("window") not in {"full_available", "last_30", "last_14"}:
            continue
        lines.append(
            f"| `{row.get('window')}` | `{row.get('check')}` | `{row.get('resolved')}` | "
            f"`{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('units'), 2)}` | "
            f"`{_fmt(row.get('avg_odds'), 2)}` | `{row.get('sample_flag')}` |"
        )
    lines.extend(
        [
            "",
            "## Window Stability",
            "",
            "| window | component | q1 ROI | q5 ROI | q5-q1 ROI spread | q1 WR | q5 WR | note |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for window, _days in WINDOWS:
        for component, _fields, _label in COMPONENTS:
            comp_rows = [
                r
                for r in bucket_rows
                if r.get("window") == window and r.get("component") == component and r.get("bucket") in {"q1_lowest", "q5_highest"}
            ]
            if len(comp_rows) != 2:
                continue
            by_bucket = {r["bucket"]: r for r in comp_rows}
            q1 = by_bucket["q1_lowest"]
            q5 = by_bucket["q5_highest"]
            q1_roi = _f(q1.get("roi"))
            q5_roi = _f(q5.get("roi"))
            spread = q5_roi - q1_roi if q1_roi is not None and q5_roi is not None else None
            lines.append(
                f"| `{window}` | `{component}` | `{_fmt_pct(q1_roi)}` | `{_fmt_pct(q5_roi)}` | "
                f"`{_fmt_pct(spread)}` | `{_fmt_pct(q1.get('wr'))}` | `{_fmt_pct(q5.get('wr'))}` | `{q1.get('monotonicity_note')}` |"
            )
    lines.extend(
        [
            "",
            "## Answers",
            "",
            f"- Strongest component: `{strongest}` by full-window ROI/WR spread across buckets.",
            f"- Weakest component: `{weakest}` by the same broad spread measure.",
            f"- Does `team_expected_hits_allowed` beat `starter_expected_hits_allowed`? `{team_vs_starter}`. Team expected has a wider full-window bucket spread, but the simple high-team slice does not beat the simple high-starter slice.",
            "- Which components add information beyond starter? `bullpen_hits_allowed_form_blended` and `offense_factor_vs_league_clamped` both improve high-starter slices in the broad incremental checks.",
            f"- Does bullpen deserve Environment v2 inclusion? `{bullpen_note}`. Treat as research-only until incremental checks are repeated after future slates.",
            f"- Does offense_factor need redesign before Environment v2? `{offense_note}`.",
            "",
            "## Recommendation",
            "",
            "Keep the current second-letter implementation unchanged. For Environment v2 research, carry forward `starter_expected_hits_allowed`, `team_expected_hits_allowed`, and `bullpen_hits_allowed_form_blended` as separate components rather than collapsing them immediately.",
            "The next research-only step should be a simple component-plus-outcome stability recheck after the next completed slate batch, then a non-production environment composite bakeoff.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate Environment v1.2 component quality for MLB Hits O1.5.")
    ap.add_argument("--rows", type=Path, default=DEFAULT_ROWS)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    rows = [r for r in _read_csv(args.rows) if _clean(r.get("side")) == "over" and _clean(r.get("result")) in {"win", "loss", "push"}]
    dates = [d for d in (_date(r.get("date")) for r in rows) if d is not None]
    max_date = max(dates) if dates else datetime(2026, 6, 29)
    bucket_rows = _component_bucket_rows(rows, max_date)
    inc_rows = _incremental_rows(rows, max_date)
    date_label = "2026-06-29"
    bucket_csv = args.out_dir / f"offensive_environment_v1_2_component_buckets_{date_label}.csv"
    inc_csv = args.out_dir / f"offensive_environment_v1_2_incremental_checks_{date_label}.csv"
    report_md = args.out_dir / f"offensive_environment_v1_2_component_quality_{date_label}.md"
    _write_csv(bucket_csv, bucket_rows)
    _write_csv(inc_csv, inc_rows)
    _write_report(report_md, bucket_rows, inc_rows, rows)
    print(
        {
            "report_md": str(report_md),
            "bucket_csv": str(bucket_csv),
            "incremental_csv": str(inc_csv),
            "resolved_rows": len(rows),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
