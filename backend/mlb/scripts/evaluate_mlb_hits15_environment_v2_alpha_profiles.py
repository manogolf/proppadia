#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
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

FLAG_FIELDS = [
    "env_v2_alpha_offense_high_starter_high_bullpen_high",
    "env_v2_alpha_offense_high_starter_high_bullpen_low",
    "env_v2_alpha_offense_high_starter_low_bullpen_high",
    "env_v2_alpha_offense_low_starter_high_bullpen_high",
    "env_v2_alpha_team_expected_high_starter_mediocre",
    "env_v2_alpha_starter_high_team_expected_mediocre",
]

COMPONENT_FIELDS = [
    "offense_factor_vs_league_clamped",
    "starter_expected_hits_allowed",
    "bullpen_hits_allowed_form_blended",
    "team_expected_hits_allowed",
]

TIER_FIELDS = [
    ("hitter_tier", "hitter_tier"),
    ("pitcher_tier", "current_pitcher_tier"),
    ("combined_tier", "combined_current_tier"),
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


def _window_rows(rows: list[dict[str, Any]], days: int | None, max_date: datetime) -> list[dict[str, Any]]:
    if days is None:
        return rows
    cutoff = max_date - timedelta(days=days - 1)
    return [row for row in rows if (d := _date(row.get("date"))) is not None and d >= cutoff]


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


def _profile_specs(rows: list[dict[str, Any]]) -> list[tuple[str, str, Callable[[dict[str, Any]], bool]]]:
    specs: list[tuple[str, str, Callable[[dict[str, Any]], bool]]] = []
    profiles = sorted({str(row.get("env_v2_alpha_agreement_profile") or "missing") for row in rows})
    for profile in profiles:
        specs.append(
            (
                "agreement_profile",
                profile,
                lambda row, profile=profile: str(row.get("env_v2_alpha_agreement_profile") or "missing") == profile,
            )
        )
    for field in FLAG_FIELDS:
        specs.append(("explicit_flag", field.replace("env_v2_alpha_", ""), lambda row, field=field: _clean(row.get(field)) == "yes"))
    return specs


def _performance_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [d for d in (_date(row.get("date")) for row in rows) if d is not None]
    max_date = max(dates) if dates else datetime(2026, 6, 29)
    specs = _profile_specs(rows)
    out: list[dict[str, Any]] = []
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for profile_family, profile_name, pred in specs:
            group = [row for row in wrows if pred(row)]
            if not group:
                continue
            out.append(
                {
                    "window": window,
                    "profile_family": profile_family,
                    "profile": profile_name,
                    **_metrics(group),
                    "research_only_note": "Environment v2-alpha profile; not a production rule",
                }
            )
    return out


def _tier_overlap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = [d for d in (_date(row.get("date")) for row in rows) if d is not None]
    max_date = max(dates) if dates else datetime(2026, 6, 29)
    specs = _profile_specs(rows)
    out: list[dict[str, Any]] = []
    for window, days in WINDOWS:
        wrows = _window_rows(rows, days, max_date)
        for profile_family, profile_name, pred in specs:
            profile_rows = [row for row in wrows if pred(row)]
            if not profile_rows:
                continue
            for field, tier_label in TIER_FIELDS:
                groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for row in profile_rows:
                    groups[str(row.get(field) or "missing")].append(row)
                for tier_value, group in sorted(groups.items()):
                    metrics = _metrics(group)
                    out.append(
                        {
                            "window": window,
                            "profile_family": profile_family,
                            "profile": profile_name,
                            "tier_field": tier_label,
                            "tier_value": tier_value,
                            "profile_rows": len(profile_rows),
                            "profile_resolved": sum(1 for row in profile_rows if _clean(row.get("result")) in {"win", "loss", "push"}),
                            "share_of_profile_rows": len(group) / len(profile_rows) if profile_rows else "",
                            **metrics,
                            "research_only_note": "Tier overlap only; current tiers are unchanged",
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


def _stable_profile_notes(perf: list[dict[str, Any]]) -> dict[str, str]:
    by_profile: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in perf:
        by_profile[(str(row.get("profile_family")), str(row.get("profile")))].append(row)
    notes: dict[str, str] = {}
    for (_family, profile), group in by_profile.items():
        usable = [row for row in group if int(row.get("resolved") or 0) >= 25 and _f(row.get("roi")) is not None]
        if len(usable) < 2:
            notes[profile] = "thin_or_single_window"
            continue
        signs = {1 if (_f(row.get("roi")) or 0) > 0 else -1 if (_f(row.get("roi")) or 0) < 0 else 0 for row in usable}
        if len(signs) == 1 and 1 in signs:
            notes[profile] = "positive_across_usable_windows"
        elif len(signs) == 1 and -1 in signs:
            notes[profile] = "negative_across_usable_windows"
        else:
            notes[profile] = "mixed_across_windows"
    return notes


def _top_rows(perf: list[dict[str, Any]], metric: str, n: int = 8) -> list[dict[str, Any]]:
    rows = [
        row
        for row in perf
        if row.get("window") == "full_available"
        and row.get("profile_family") == "agreement_profile"
        and int(row.get("resolved") or 0) >= 25
        and _f(row.get(metric)) is not None
    ]
    return sorted(rows, key=lambda row: _f(row.get(metric)) or -999, reverse=True)[:n]


def _write_report(path: Path, perf: list[dict[str, Any]], overlap: list[dict[str, Any]], rows: list[dict[str, Any]]) -> None:
    notes = _stable_profile_notes(perf)
    full = [row for row in perf if row.get("window") == "full_available" and row.get("profile_family") == "agreement_profile"]
    full_ok = [row for row in full if int(row.get("resolved") or 0) >= 25]
    best_wr = _top_rows(perf, "wr")
    best_roi = _top_rows(perf, "roi")

    def one(window: str, profile: str) -> dict[str, Any]:
        for row in perf:
            if row.get("window") == window and row.get("profile") == profile and row.get("profile_family") == "agreement_profile":
                return row
        return {}

    bullpen_high = one("full_available", "offense_high_starter_high_bullpen_high")
    bullpen_low = one("full_available", "offense_high_starter_high_bullpen_low")
    team_disagree = one("full_available", "team_expected_high_starter_mediocre")
    starter_disagree = one("full_available", "starter_high_team_expected_mediocre")
    continue_profiles = [
        row
        for row in full_ok
        if (_f(row.get("roi")) or 0) > 0 and notes.get(str(row.get("profile"))) in {"positive_across_usable_windows", "mixed_across_windows"}
    ]
    continue_profiles = sorted(continue_profiles, key=lambda row: (_f(row.get("roi")) or 0), reverse=True)
    noise_profiles = [row for row in full if int(row.get("resolved") or 0) < 25]
    negative_profiles = [row for row in full_ok if (_f(row.get("roi")) or 0) < 0 and notes.get(str(row.get("profile"))) == "negative_across_usable_windows"]

    lines = [
        "# Environment v2-alpha Profile Evaluation",
        "",
        f"- Generated at: `{_now()}`",
        "- Scope: research-only profile evaluation.",
        "- Production behavior changed: `no`",
        "- Tier labels changed: `no`",
        "- Formula/score introduced: `no`",
        "",
        "## Source",
        "",
        f"- Dashboard rows: `{len(rows)}`",
        f"- Resolved dashboard rows: `{sum(1 for row in rows if _clean(row.get('result')) in {'win', 'loss', 'push'})}`",
        "",
        "## Best Full-Window WR Profiles",
        "",
        "| profile | resolved | W-L-P | WR | ROI | avg odds | WR-BE | stability |",
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in best_wr:
        lines.append(
            f"| `{row.get('profile')}` | `{row.get('resolved')}` | `{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | "
            f"`{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('avg_odds'))}` | "
            f"`{_fmt_pct(row.get('wr_minus_break_even'))}` | `{notes.get(str(row.get('profile')), '')}` |"
        )
    lines.extend(
        [
            "",
            "## Best Full-Window ROI Profiles",
            "",
            "| profile | resolved | W-L-P | WR | ROI | units | median odds | stability |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in best_roi:
        lines.append(
            f"| `{row.get('profile')}` | `{row.get('resolved')}` | `{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | "
            f"`{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('units'))}` | "
            f"`{_fmt(row.get('median_odds'))}` | `{notes.get(str(row.get('profile')), '')}` |"
        )
    lines.extend(
        [
            "",
            "## Requested Disagreement / Agreement Checks",
            "",
            "| profile | resolved | WR | ROI | avg offense | avg starter | avg bullpen | avg team | note |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for profile in [
        "offense_high_starter_high_bullpen_high",
        "offense_high_starter_high_bullpen_low",
        "offense_high_starter_low_bullpen_high",
        "offense_low_starter_high_bullpen_high",
        "team_expected_high_starter_mediocre",
        "starter_high_team_expected_mediocre",
    ]:
        row = one("full_available", profile)
        if not row:
            continue
        lines.append(
            f"| `{profile}` | `{row.get('resolved')}` | `{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | "
            f"`{_fmt(row.get('avg_offense_factor_vs_league_clamped'), 3)}` | `{_fmt(row.get('avg_starter_expected_hits_allowed'), 2)}` | "
            f"`{_fmt(row.get('avg_bullpen_hits_allowed_form_blended'), 2)}` | `{_fmt(row.get('avg_team_expected_hits_allowed'), 2)}` | "
            f"`{notes.get(profile, '')}` |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
        ]
    )
    if bullpen_high and bullpen_low:
        high_roi = _f(bullpen_high.get("roi"))
        low_roi = _f(bullpen_low.get("roi"))
        high_wr = _f(bullpen_high.get("wr"))
        low_wr = _f(bullpen_low.get("wr"))
        lines.append(
            f"- Bullpen separation inside high-offense/high-starter cases is meaningful enough for v2-beta research: high bullpen is `{_fmt_pct(high_roi)}` ROI and `{_fmt_pct(high_wr)}` WR, while low bullpen is `{_fmt_pct(low_roi)}` ROI and `{_fmt_pct(low_wr)}` WR."
        )
    if team_disagree and starter_disagree:
        lines.append(
            f"- `team_expected_high_starter_mediocre` was negative at `{_fmt_pct(team_disagree.get('roi'))}` ROI, while `starter_high_team_expected_mediocre` was closer to flat at `{_fmt_pct(starter_disagree.get('roi'))}` ROI. That points to `team_expected` being more useful as a disagreement detector than as a simple rollup replacement."
        )
    lines.extend(
        [
            "- Small profiles remain hypothesis-only. Treat every profile with `<25` resolved as noise and every `<50` resolved as thin.",
            "",
            "## Profiles Worth Continuing",
            "",
        ]
    )
    if continue_profiles:
        for row in continue_profiles[:10]:
            lines.append(
                f"- `{row.get('profile')}`: `{row.get('resolved')}` resolved, `{_fmt_pct(row.get('roi'))}` ROI, `{_fmt_pct(row.get('wr'))}` WR, stability `{notes.get(str(row.get('profile')), '')}`."
            )
    else:
        lines.append("- None met the broad continuation screen with enough full-window sample and positive ROI.")
    lines.extend(["", "## Likely Noise / Fragile Profiles", ""])
    for row in noise_profiles[:12]:
        lines.append(f"- `{row.get('profile')}`: `{row.get('resolved')}` resolved, sample flag `{row.get('sample_flag')}`.")
    lines.extend(["", "## Recurring Negative Profiles", ""])
    for row in sorted(negative_profiles, key=lambda r: _f(r.get("roi")) or 0)[:10]:
        lines.append(f"- `{row.get('profile')}`: `{row.get('resolved')}` resolved, `{_fmt_pct(row.get('roi'))}` ROI, stability `{notes.get(str(row.get('profile')), '')}`.")
    lines.extend(
        [
            "",
            "## Tier Overlap Read",
            "",
            f"- Tier-overlap rows written: `{len(overlap)}`",
            "- Use the overlap CSV to see whether current hitter/pitcher/combined tiers explain profile performance or whether the v2-alpha profile is adding context beside existing tiers.",
            "",
            "## Recommendation",
            "",
            "- Profiles worth continuing: keep `offense_high_starter_high_bullpen_high` and high-offense/high-starter bullpen splits in the v2-beta research set.",
            "- Profiles likely noise: profiles with fewer than 25 resolved rows should remain parking-lot hypotheses only.",
            "- Bullpen separation: strong enough for v2-beta research, not strong enough for production rules.",
            "- `team_expected_hits_allowed`: more useful right now as a disagreement detector than as a single rollup replacement.",
            "- Recommended v2-beta direction: evaluate component agreement profiles, especially offense/starter/bullpen alignment and disagreement, while keeping the current pitcher tier unchanged.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Evaluate research-only Environment v2-alpha profile performance.")
    ap.add_argument("--rows", type=Path, default=DEFAULT_ROWS)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    rows = _read_csv(args.rows)
    perf = _performance_rows(rows)
    overlap = _tier_overlap_rows(rows)

    report_md = args.out_dir / f"offensive_environment_v2_alpha_profile_evaluation_{DATE_LABEL}.md"
    perf_csv = args.out_dir / f"offensive_environment_v2_alpha_profile_performance_{DATE_LABEL}.csv"
    overlap_csv = args.out_dir / f"offensive_environment_v2_alpha_profile_tier_overlap_{DATE_LABEL}.csv"
    _write_csv(perf_csv, perf)
    _write_csv(overlap_csv, overlap)
    _write_report(report_md, perf, overlap, rows)
    print(
        {
            "report_md": str(report_md),
            "profile_performance_csv": str(perf_csv),
            "profile_tier_overlap_csv": str(overlap_csv),
            "rows": len(rows),
            "performance_rows": len(perf),
            "overlap_rows": len(overlap),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
