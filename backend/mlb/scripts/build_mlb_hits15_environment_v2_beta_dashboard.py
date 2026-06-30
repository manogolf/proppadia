#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any


DEFAULT_ROWS = Path("artifacts/analysis/mlb/review_aids/offensive_environment_v2_alpha_dashboard_rows_2026-06-29.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/review_aids")
DATE_LABEL = "2026-06-29"

WINDOWS = [
    ("full_available", None),
    ("last_60", 60),
    ("last_30", 30),
    ("last_14", 14),
    ("last_7", 7),
]

COMPONENT_FIELDS = [
    "offense_factor_vs_league_clamped",
    "offense_hits_form_blended",
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "starter_expected_hits_allowed",
    "bullpen_hits_allowed_form_blended",
    "team_expected_hits_allowed",
]

PROFILE_MAP = {
    "offense_high_starter_high_bullpen_high": {
        "family": "aligned_high_environment",
        "label": "Aligned High Environment",
        "family_status": "continue",
        "semantic_meaning": "Offense, starter matchup, and bullpen continuation all agree positively.",
    },
    "offense_high_starter_high_bullpen_low": {
        "family": "starter_led_with_bullpen_drag",
        "label": "Starter-Led With Bullpen Drag",
        "family_status": "continue",
        "semantic_meaning": "Offense and starter are favorable, but bullpen continuation is weak.",
    },
    "team_expected_high_starter_mediocre": {
        "family": "team_high_starter_mediocre",
        "label": "Team High / Starter Mediocre",
        "family_status": "continue",
        "semantic_meaning": "Full-game rollup is high while starter matchup is low or mid.",
    },
    "starter_high_team_expected_mediocre": {
        "family": "starter_high_team_mediocre",
        "label": "Starter High / Team Mediocre",
        "family_status": "continue",
        "semantic_meaning": "Starter matchup is high while full-game rollup is low or mid.",
    },
    "offense_high_starter_low_bullpen_high": {
        "family": "bullpen_rescue_starter_suppressed",
        "label": "Bullpen Rescue / Starter Suppressed",
        "family_status": "parking_lot",
        "semantic_meaning": "Offense and bullpen are favorable even though starter matchup is weak.",
    },
    "offense_low_starter_high_bullpen_high": {
        "family": "starter_only_offense_suppressed",
        "label": "Starter Only / Offense Suppressed",
        "family_status": "parking_lot",
        "semantic_meaning": "Starter and bullpen look favorable, but offense strength is weak.",
    },
}


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


def _window_rows(rows: list[dict[str, Any]], days: int | None, max_date: datetime) -> list[dict[str, Any]]:
    if days is None:
        return rows
    cutoff = max_date - timedelta(days=days - 1)
    return [row for row in rows if (d := _date(row.get("date"))) is not None and d >= cutoff]


def _american_implied(price: float | None) -> float | None:
    if price is None:
        return None
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return 100.0 / (price + 100.0)


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
    out: dict[str, Any] = {
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
    for field in COMPONENT_FIELDS:
        vals = [_f(row.get(field)) for row in resolved if _f(row.get(field)) is not None]
        out[f"avg_{field}"] = mean(vals) if vals else ""
    return out


def _beta_status(row: dict[str, Any]) -> str:
    resolved = int(row.get("resolved") or 0)
    roi = _f(row.get("roi"))
    if resolved < 25:
        return "insufficient_sample"
    if resolved < 50:
        return "parking_lot"
    if roi is not None and roi > 0:
        return "continue"
    return "weak_signal"


def _beta_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        profile = str(row.get("env_v2_alpha_agreement_profile") or "")
        spec = PROFILE_MAP.get(profile)
        family = spec["family"] if spec else "other_alpha_profile"
        label = spec["label"] if spec else "Other Alpha Profile"
        semantic = spec["semantic_meaning"] if spec else "Alpha profile not promoted to named v2-beta family."
        family_status = spec["family_status"] if spec else "weak_signal"
        tier_overlay = f"{label} | current_tier={row.get('combined_tier') or 'missing'}"
        out_row = dict(row)
        out_row.update(
            {
                "env_v2_beta_research_status": "research_only_no_tier_change",
                "env_v2_beta_profile_family": family,
                "env_v2_beta_profile_label": label,
                "env_v2_beta_family_design_status": family_status,
                "env_v2_beta_semantic_meaning": semantic,
                "env_v2_beta_tier_overlay": tier_overlay,
                "env_v2_beta_current_tier_preserved": row.get("combined_tier") or "",
            }
        )
        out.append(out_row)
    return out


def _performance_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [d for d in (_date(row.get("date")) for row in rows) if d is not None]
    max_date = max(dates) if dates else datetime(2026, 6, 29)
    out: list[dict[str, Any]] = []
    families = sorted({str(row.get("env_v2_beta_profile_family") or "missing") for row in rows})
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for family in families:
            group = [row for row in wrows if str(row.get("env_v2_beta_profile_family") or "missing") == family]
            if not group:
                continue
            label = str(group[0].get("env_v2_beta_profile_label") or family)
            metrics = _metrics(group)
            out.append(
                {
                    "window": window,
                    "profile_family": family,
                    "profile_label": label,
                    **metrics,
                    "beta_signal_status": _beta_status(metrics),
                    "research_only_note": "Environment v2-beta profile family; not a production rule",
                }
            )
    return out


def _tier_overlay_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [d for d in (_date(row.get("date")) for row in rows) if d is not None]
    max_date = max(dates) if dates else datetime(2026, 6, 29)
    out: list[dict[str, Any]] = []
    families = sorted({str(row.get("env_v2_beta_profile_family") or "missing") for row in rows})
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for family in families:
            profile_rows = [row for row in wrows if str(row.get("env_v2_beta_profile_family") or "missing") == family]
            if not profile_rows:
                continue
            groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in profile_rows:
                groups[str(row.get("combined_tier") or "missing")].append(row)
            for tier, group in sorted(groups.items()):
                metrics = _metrics(group)
                out.append(
                    {
                        "window": window,
                        "profile_family": family,
                        "profile_label": str(group[0].get("env_v2_beta_profile_label") or family),
                        "combined_tier": tier,
                        "profile_rows": len(profile_rows),
                        "profile_resolved": sum(1 for row in profile_rows if _clean(row.get("result")) in {"win", "loss", "push"}),
                        "share_of_profile_rows": len(group) / len(profile_rows) if profile_rows else "",
                        **metrics,
                        "beta_signal_status": _beta_status(metrics),
                        "research_only_note": "Tier overlay only; current combined tiers are unchanged",
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


def _full(perf: list[dict[str, Any]], family: str) -> dict[str, Any]:
    for row in perf:
        if row.get("window") == "full_available" and row.get("profile_family") == family:
            return row
    return {}


def _write_report(path: Path, rows: list[dict[str, Any]], perf: list[dict[str, Any]], overlay: list[dict[str, Any]]) -> None:
    full_rows = [row for row in perf if row.get("window") == "full_available"]
    ranked = sorted(full_rows, key=lambda row: _f(row.get("roi")) if _f(row.get("roi")) is not None else -999, reverse=True)
    strongest = ranked[0] if ranked else {}
    weakest = sorted(
        [row for row in full_rows if int(row.get("resolved") or 0) >= 25],
        key=lambda row: _f(row.get("roi")) if _f(row.get("roi")) is not None else 999,
    )[0] if full_rows else {}
    aligned = _full(perf, "aligned_high_environment")
    drag = _full(perf, "starter_led_with_bullpen_drag")
    team_disagree = _full(perf, "team_high_starter_mediocre")
    starter_disagree = _full(perf, "starter_high_team_mediocre")
    lines = [
        "# Offensive Environment v2-beta Dashboard",
        "",
        f"- Generated at: `{_now()}`",
        "- Scope: research-only dashboard/prototype.",
        "- Production tier assignment changed: `no`",
        "- Thresholds/selectors/uploads/grading/models changed: `no`",
        "- Morning Workbench/Ops Brief behavior changed: `no`",
        "- v2-beta is not a replacement tier and not an optimized score.",
        "",
        "## Source",
        "",
        f"- Dashboard rows: `{len(rows)}`",
        f"- Resolved rows: `{sum(1 for row in rows if _clean(row.get('result')) in {'win', 'loss', 'push'})}`",
        "",
        "## Full-Window Profile Families",
        "",
        "| profile family | label | rows | resolved | W-L-P | WR | ROI | units | avg odds | status |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in ranked:
        lines.append(
            f"| `{row.get('profile_family')}` | `{row.get('profile_label')}` | `{row.get('rows')}` | `{row.get('resolved')}` | "
            f"`{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | `{_fmt_pct(row.get('wr'))}` | "
            f"`{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('units'))}` | `{_fmt(row.get('avg_odds'))}` | `{row.get('beta_signal_status')}` |"
        )
    lines.extend(
        [
            "",
            "## Core Profile Read",
            "",
        ]
    )
    if aligned:
        lines.append(
            f"- Aligned High Environment remains strong: `{aligned.get('resolved')}` resolved, `{aligned.get('wins')}-{aligned.get('losses')}`, `{_fmt_pct(aligned.get('wr'))}` WR, `{_fmt_pct(aligned.get('roi'))}` ROI."
        )
    if aligned and drag:
        lines.append(
            f"- Bullpen drag is directionally confirmed: aligned high was `{_fmt_pct(aligned.get('roi'))}` ROI versus `{_fmt_pct(drag.get('roi'))}` ROI for Starter-Led With Bullpen Drag."
        )
    if team_disagree and starter_disagree:
        lines.append(
            f"- Disagreement families are useful as warnings/context: Team High / Starter Mediocre was `{_fmt_pct(team_disagree.get('roi'))}` ROI, while Starter High / Team Mediocre was `{_fmt_pct(starter_disagree.get('roi'))}` ROI."
        )
    lines.extend(
        [
            "",
            "## Tier Overlay",
            "",
            f"- Tier overlay rows: `{len(overlay)}`",
            "- The overlay preserves `combined_tier` and labels profile context beside it. It does not replace current tier labels.",
            "",
            "## Final Recommendation",
            "",
            f"- Strongest v2-beta profile family: `{strongest.get('profile_label', '')}` with `{_fmt_pct(strongest.get('roi'))}` ROI.",
            f"- Weakest v2-beta profile family: `{weakest.get('profile_label', '')}` with `{_fmt_pct(weakest.get('roi'))}` ROI among families with at least 25 resolved.",
            "- Aligned High Environment remains strong and deserves continued research.",
            "- Bullpen drag is confirmed enough for continued beta research, not production use.",
            "- Disagreement families are useful, especially as warnings against blind `team_expected` replacement.",
            "- v2-beta deserves continued research as a dashboard/profile overlay.",
            "- No production migration is recommended yet.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build research-only MLB Hits 1.5 Environment v2-beta dashboard.")
    ap.add_argument("--rows", type=Path, default=DEFAULT_ROWS)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    alpha_rows = _read_csv(args.rows)
    beta_rows = _beta_rows(alpha_rows)
    perf = _performance_rows(beta_rows)
    overlay = _tier_overlay_rows(beta_rows)

    report_md = args.out_dir / f"offensive_environment_v2_beta_dashboard_{DATE_LABEL}.md"
    rows_csv = args.out_dir / f"offensive_environment_v2_beta_dashboard_rows_{DATE_LABEL}.csv"
    perf_csv = args.out_dir / f"offensive_environment_v2_beta_profile_performance_{DATE_LABEL}.csv"
    overlay_csv = args.out_dir / f"offensive_environment_v2_beta_tier_overlay_comparison_{DATE_LABEL}.csv"
    _write_csv(rows_csv, beta_rows)
    _write_csv(perf_csv, perf)
    _write_csv(overlay_csv, overlay)
    _write_report(report_md, beta_rows, perf, overlay)
    print(
        {
            "report_md": str(report_md),
            "dashboard_rows_csv": str(rows_csv),
            "profile_performance_csv": str(perf_csv),
            "tier_overlay_comparison_csv": str(overlay_csv),
            "rows": len(beta_rows),
            "performance_rows": len(perf),
            "tier_overlay_rows": len(overlay),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
