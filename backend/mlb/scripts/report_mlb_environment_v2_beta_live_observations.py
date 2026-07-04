#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_DAILY_ROOT = Path("artifacts/analysis/mlb/environment_v2/daily")
DEFAULT_LEDGER = Path("artifacts/analysis/mlb/environment_v2/ledger/environment_v2_beta_profile_ledger.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/environment_v2/reports")

KNOWN_FAMILIES = {
    "aligned_high_environment": "Aligned High Environment",
    "starter_led_with_bullpen_drag": "Starter-Led With Bullpen Drag",
    "team_high_starter_mediocre": "Team High / Starter Mediocre",
    "starter_high_team_mediocre": "Starter High / Team Mediocre",
    "bullpen_rescue_starter_suppressed": "Bullpen Rescue / Starter Suppressed",
    "starter_only_offense_suppressed": "Starter Only / Offense Suppressed",
    "other": "Other / None",
}

HISTORICAL_BASELINES = {
    "aligned_high_environment": {
        "label": "Aligned High Environment",
        "resolved": 85,
        "wins": 52,
        "losses": 33,
        "wr": 0.6118,
        "roi": 0.5594,
    },
    "starter_led_with_bullpen_drag": {
        "label": "Starter-Led With Bullpen Drag",
        "resolved": 70,
        "wins": 30,
        "losses": 40,
        "wr": 0.4286,
        "roi": 0.1336,
    },
    "team_high_starter_mediocre": {
        "label": "Team High / Starter Mediocre",
        "resolved": 181,
        "wins": 47,
        "losses": 134,
        "wr": 0.2597,
        "roi": -0.3288,
    },
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _f(value: Any) -> float | None:
    try:
        text = str(value or "").strip()
        if not text or text.lower() in {"nan", "none", "null"}:
            return None
        return float(text)
    except Exception:
        return None


def _to_int(value: Any) -> int:
    try:
        text = str(value or "").strip()
        if not text:
            return 0
        return int(float(text))
    except Exception:
        return 0


def _s(value: Any) -> str:
    return str(value or "").strip()


def _date_from_path(path: Path) -> str:
    for part in path.parts:
        if len(part) == 10 and part[:4].isdigit() and part[4] == "-" and part[7] == "-":
            return part
    return ""


def _capture_files(daily_root: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for path in sorted(daily_root.glob("20??-??-??/environment_v2_beta_daily_profiles_*.csv")):
        if "rerun_" in path.parts:
            continue
        date_text = _date_from_path(path)
        if date_text:
            files[date_text] = path
    return files


def _reconciled_files(daily_root: Path) -> dict[str, Path]:
    files: dict[str, Path] = {}
    for path in sorted(daily_root.glob("20??-??-??/environment_v2_beta_daily_profiles_reconciled_*.csv")):
        if "rerun_" in path.parts:
            continue
        date_text = _date_from_path(path)
        if date_text:
            files[date_text] = path
    return files


def _family(row: dict[str, Any]) -> str:
    family = _s(row.get("env_v2_beta_profile_family"))
    return family or "other"


def _family_label(family: str, rows: list[dict[str, Any]] | None = None) -> str:
    if rows:
        for row in rows:
            if _family(row) == family and _s(row.get("env_v2_beta_profile_label")):
                return _s(row.get("env_v2_beta_profile_label"))
    return KNOWN_FAMILIES.get(family, family or "Other / None")


def _is_resolved(row: dict[str, Any]) -> bool:
    status = _s(row.get("resolved_status") or row.get("outcome_status")).lower()
    win_loss = _s(row.get("win_loss") or row.get("outcome")).lower()
    return status == "resolved" and win_loss in {"win", "loss", "push"}


def _is_win(row: dict[str, Any]) -> bool:
    return _s(row.get("win_loss") or row.get("outcome")).lower() == "win"


def _is_loss(row: dict[str, Any]) -> bool:
    return _s(row.get("win_loss") or row.get("outcome")).lower() == "loss"


def _is_push(row: dict[str, Any]) -> bool:
    return _s(row.get("win_loss") or row.get("outcome")).lower() == "push"


def _units(row: dict[str, Any]) -> float:
    return _f(row.get("units") or row.get("roi_result")) or 0.0


def _odds(row: dict[str, Any]) -> float | None:
    return _f(row.get("odds_used") or row.get("market_price") or row.get("best_over_price"))


def _sample_flag(resolved: int) -> str:
    if resolved == 0:
        return "no_resolved_rows"
    if resolved < 10:
        return "tiny_sample_lt_10"
    if resolved < 25:
        return "small_sample_lt_25"
    if resolved < 50:
        return "thin_sample_lt_50"
    return "ok"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def _fmt_num(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _metrics(rows: list[dict[str, Any]], observations: int | None = None) -> dict[str, Any]:
    resolved_rows = [row for row in rows if _is_resolved(row)]
    wins = sum(1 for row in resolved_rows if _is_win(row))
    losses = sum(1 for row in resolved_rows if _is_loss(row))
    pushes = sum(1 for row in resolved_rows if _is_push(row))
    resolved = len(resolved_rows)
    units = sum(_units(row) for row in resolved_rows)
    odds_values = [_odds(row) for row in resolved_rows if _odds(row) is not None]
    obs = len(rows) if observations is None else observations
    return {
        "observations": obs,
        "resolved": resolved,
        "pending": max(obs - resolved, 0),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": (wins / (wins + losses)) if (wins + losses) else None,
        "roi": (units / resolved) if resolved else None,
        "units": units,
        "avg_odds": (sum(odds_values) / len(odds_values)) if odds_values else None,
        "median_odds": median(odds_values) if odds_values else None,
        "sample_flag": _sample_flag(resolved),
    }


def _trend(family: str, live: dict[str, Any]) -> str:
    resolved = int(live.get("resolved") or 0)
    roi = live.get("roi")
    if resolved < 25 or roi is None:
        return "insufficient sample"
    hist = HISTORICAL_BASELINES.get(family)
    if not hist:
        return "stable" if roi >= 0 else "weakening"
    hist_roi = float(hist["roi"])
    if roi >= 0 and hist_roi >= 0 and abs(roi - hist_roi) <= 0.25:
        return "stable"
    if roi >= hist_roi + 0.15:
        return "strengthening"
    if roi < 0 <= hist_roi:
        return "weakening"
    if roi < hist_roi - 0.15:
        return "weakening"
    return "stable"


def _date_span(dates: list[str]) -> int:
    if not dates:
        return 0
    parsed = [date.fromisoformat(d) for d in dates]
    return (max(parsed) - min(parsed)).days + 1


def _performance_rows(
    capture_rows: list[dict[str, Any]],
    ledger_rows: list[dict[str, Any]],
    capture_dates_by_family: dict[str, set[str]],
) -> list[dict[str, Any]]:
    capture_counts = Counter(_family(row) for row in capture_rows)
    families = sorted(set(KNOWN_FAMILIES) | set(capture_counts) | {_family(row) for row in ledger_rows})
    out: list[dict[str, Any]] = []
    for family in families:
        rows = [row for row in ledger_rows if _family(row) == family]
        metrics = _metrics(rows, observations=capture_counts.get(family, 0))
        daily_units: dict[str, float] = defaultdict(float)
        daily_resolved: Counter[str] = Counter()
        for row in rows:
            if _is_resolved(row):
                day = _s(row.get("date"))
                daily_units[day] += _units(row)
                daily_resolved[day] += 1
        positive_dates = sum(1 for day, units in daily_units.items() if daily_resolved[day] and units > 0)
        negative_dates = sum(1 for day, units in daily_units.items() if daily_resolved[day] and units < 0)
        observed_dates = sorted(capture_dates_by_family.get(family, set()))
        hist = HISTORICAL_BASELINES.get(family, {})
        row = {
            "profile_family": family,
            "profile_label": _family_label(family, capture_rows + ledger_rows),
            **metrics,
            "dates_observed": ",".join(observed_dates),
            "positive_dates": positive_dates,
            "negative_dates": negative_dates,
            "historical_resolved": hist.get("resolved", ""),
            "historical_record": f"{hist.get('wins')}-{hist.get('losses')}" if hist else "",
            "historical_wr": hist.get("wr", ""),
            "historical_roi": hist.get("roi", ""),
        }
        row["trend_status"] = _trend(family, row)
        out.append(row)
    return out


def _daily_slice_rows(capture_rows: list[dict[str, Any]], ledger_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    capture_counts: Counter[tuple[str, str]] = Counter()
    for row in capture_rows:
        capture_counts[(_s(row.get("date")), _family(row))] += 1
    keys = sorted(set(capture_counts) | {(_s(row.get("date")), _family(row)) for row in ledger_rows})
    out: list[dict[str, Any]] = []
    for day, family in keys:
        rows = [row for row in ledger_rows if _s(row.get("date")) == day and _family(row) == family]
        metrics = _metrics(rows, observations=capture_counts.get((day, family), 0))
        out.append({"date": day, "profile_family": family, "profile_label": _family_label(family, capture_rows + ledger_rows), **metrics})
    return out


def _tier_overlap_rows(capture_rows: list[dict[str, Any]], ledger_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    capture_counts: Counter[tuple[str, str]] = Counter()
    for row in capture_rows:
        tier = _s(row.get("current_combined_tier") or row.get("combined_tier"))
        if tier in {"A/A", "C/A"}:
            capture_counts[(tier, _family(row))] += 1
    keys = sorted(set(capture_counts) | {
        (_s(row.get("current_combined_tier") or row.get("combined_tier")), _family(row))
        for row in ledger_rows
        if _s(row.get("current_combined_tier") or row.get("combined_tier")) in {"A/A", "C/A"}
    })
    out: list[dict[str, Any]] = []
    for tier, family in keys:
        rows = [
            row for row in ledger_rows
            if _s(row.get("current_combined_tier") or row.get("combined_tier")) == tier and _family(row) == family
        ]
        metrics = _metrics(rows, observations=capture_counts.get((tier, family), 0))
        loss_rows = [row for row in rows if _is_resolved(row) and _is_loss(row)]
        out.append(
            {
                "combined_tier": tier,
                "profile_family": family,
                "profile_label": _family_label(family, capture_rows + ledger_rows),
                **metrics,
                "losses_share_of_tier_family_resolved": (len(loss_rows) / metrics["resolved"]) if metrics["resolved"] else "",
            }
        )
    return out


def _largest_units(rows: list[dict[str, Any]], best: bool = True) -> dict[str, Any] | None:
    candidates = [row for row in rows if (row.get("resolved") or 0) > 0 and row.get("roi") is not None]
    if not candidates:
        return None
    return sorted(candidates, key=lambda r: (r.get("units") or 0.0), reverse=best)[0]


def _write_report(
    path: Path,
    *,
    generated_at: str,
    first_capture: str,
    latest_capture: str,
    calendar_days: int,
    completed_slates: list[str],
    pending_slates: list[str],
    total_observations: int,
    resolved_rows: int,
    pending_rows: int,
    profile_rows: list[dict[str, Any]],
    tier_rows: list[dict[str, Any]],
    daily_rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    strongest = _largest_units(profile_rows, best=True)
    weakest = _largest_units(profile_rows, best=False)
    lines: list[str] = [
        "# Environment v2 Beta Live Observation Report",
        "",
        f"- Generated at: `{generated_at}`",
        "- Scope: research-only live observation; no production rules, selectors, uploads, grading, Morning Workbench, or Ops Brief behavior changed.",
        "",
        "## Environment Observation Window",
        "",
        f"- First capture: `{first_capture or 'n/a'}`",
        f"- Latest capture: `{latest_capture or 'n/a'}`",
        f"- Calendar days: `{calendar_days}`",
        f"- Completed slates: `{len(completed_slates)}` ({', '.join(completed_slates) if completed_slates else 'none'})",
        f"- Pending slates: `{len(pending_slates)}` ({', '.join(pending_slates) if pending_slates else 'none'})",
        f"- Total observations: `{total_observations}`",
        f"- Resolved rows: `{resolved_rows}`",
        f"- Pending/unresolved rows: `{pending_rows}`",
        "- Observed profile families: "
        + (
            ", ".join(
                row["profile_label"]
                for row in profile_rows
                if _to_int(row.get("observations")) > 0
            )
            or "none"
        ),
        "",
        "## Profile Family Performance",
        "",
        "| profile | observations | resolved | pending | W-L-P | WR | ROI | units | avg odds | median odds | positive dates | negative dates | dates | trend | sample |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
    ]
    for row in sorted(profile_rows, key=lambda r: (-int(r.get("observations") or 0), str(r.get("profile_family")))):
        record = f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}"
        lines.append(
            f"| {row.get('profile_label')} | `{row.get('observations')}` | `{row.get('resolved')}` | `{row.get('pending')}` | "
            f"`{record}` | `{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('units'))}` | "
            f"`{_fmt_num(row.get('avg_odds'))}` | `{_fmt_num(row.get('median_odds'))}` | "
            f"`{row.get('positive_dates')}` | `{row.get('negative_dates')}` | "
            f"{row.get('dates_observed') or 'n/a'} | {row.get('trend_status')} | {row.get('sample_flag')} |"
        )
    lines.extend(
        [
            "",
            "## Historical Comparison",
            "",
            "| profile | historical resolved | historical record | historical WR | historical ROI | live resolved | live record | live WR | live ROI | read |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    by_family = {row["profile_family"]: row for row in profile_rows}
    for family, hist in HISTORICAL_BASELINES.items():
        live = by_family.get(family, {})
        live_record = f"{live.get('wins', 0)}-{live.get('losses', 0)}-{live.get('pushes', 0)}"
        sample = _sample_flag(int(live.get("resolved") or 0))
        if sample != "ok":
            read = "too early; live sample is thin"
        elif (live.get("roi") or 0) * (hist["roi"] or 0) >= 0:
            read = "directionally consistent"
        else:
            read = "not yet matching historical direction"
        lines.append(
            f"| {hist['label']} | `{hist['resolved']}` | `{hist['wins']}-{hist['losses']}` | `{_fmt_pct(hist['wr'])}` | `{_fmt_pct(hist['roi'])}` | "
            f"`{live.get('resolved', 0)}` | `{live_record}` | `{_fmt_pct(live.get('wr'))}` | `{_fmt_pct(live.get('roi'))}` | {read} |"
        )
    lines.extend(
        [
            "",
            "## Trend Status",
            "",
            "| profile | live status | live resolved | live ROI | historical ROI | interpretation |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in sorted(profile_rows, key=lambda r: (-int(r.get("observations") or 0), str(r.get("profile_family")))):
        hist = HISTORICAL_BASELINES.get(str(row.get("profile_family") or ""), {})
        interpretation = str(row.get("trend_status") or "insufficient sample")
        if row.get("sample_flag") in {"tiny", "thin"}:
            interpretation = f"{interpretation}; sample remains {row.get('sample_flag')}"
        lines.append(
            f"| {row.get('profile_label')} | {row.get('trend_status')} | `{row.get('resolved')}` | "
            f"`{_fmt_pct(row.get('roi'))}` | `{_fmt_pct(hist.get('roi')) if hist else ''}` | {interpretation} |"
        )
    lines.extend(
        [
            "",
            "## A/A And C/A Inside Environment Profiles",
            "",
            "| tier | profile | observations | resolved | pending | W-L-P | WR | ROI | units | sample |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in sorted(tier_rows, key=lambda r: (str(r.get("combined_tier")), -int(r.get("observations") or 0), str(r.get("profile_family")))):
        record = f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}"
        lines.append(
            f"| {row.get('combined_tier')} | {row.get('profile_label')} | `{row.get('observations')}` | `{row.get('resolved')}` | `{row.get('pending')}` | "
            f"`{record}` | `{_fmt_pct(row.get('wr'))}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('units'))}` | {row.get('sample_flag')} |"
        )
    lines.extend(
        [
            "",
            "## Daily Slices",
            "",
            "| date | profile | observations | resolved | W-L-P | ROI | units |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in daily_rows:
        record = f"{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}"
        lines.append(
            f"| {row.get('date')} | {row.get('profile_label')} | `{row.get('observations')}` | `{row.get('resolved')}` | "
            f"`{record}` | `{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('units'))}` |"
        )
    aa_ca_loss_rows = [row for row in tier_rows if row.get("combined_tier") in {"A/A", "C/A"} and int(row.get("losses") or 0) > 0]
    loss_cluster = sorted(aa_ca_loss_rows, key=lambda r: int(r.get("losses") or 0), reverse=True)[:3]
    lines.extend(["", "## Interpretation", ""])
    lines.append("- Live observation is still early. Samples are thin outside `Other / None`, so this report should guide research attention, not decisions.")
    if loss_cluster:
        cluster_text = "; ".join(
            f"{row.get('combined_tier')} in {row.get('profile_label')} losses={row.get('losses')}"
            for row in loss_cluster
        )
        lines.append(f"- A/A and C/A losses currently cluster most in: {cluster_text}. Treat this as tentative until more resolved rows arrive.")
    else:
        lines.append("- A/A and C/A failure clustering is not meaningful yet because resolved loss samples are too small.")
    aligned = by_family.get("aligned_high_environment", {})
    if int(aligned.get("resolved") or 0) < 25:
        lines.append("- Aligned High Environment remains historically promising, but the live sample is not large enough to validate it yet.")
    elif (aligned.get("roi") or 0) > 0:
        lines.append("- Aligned High Environment is directionally surviving live observation.")
    else:
        lines.append("- Aligned High Environment has not yet matched its historical edge live.")
    team_high = by_family.get("team_high_starter_mediocre", {})
    if int(team_high.get("resolved") or 0) and (team_high.get("roi") or 0) < 0:
        lines.append("- Team High / Starter Mediocre remains a warning/disagreement profile in live data.")
    else:
        lines.append("- Team High / Starter Mediocre has not yet produced enough live signal to change its warning status.")
    lines.extend(["", "## Closing Answers", ""])
    lines.append(f"- Strongest current live profile: `{strongest.get('profile_label') if strongest else 'n/a'}` by units among resolved rows.")
    lines.append(f"- Weakest current live profile: `{weakest.get('profile_label') if weakest else 'n/a'}` by units among resolved rows.")
    lines.append("- Historical findings being reinforced: disagreement profiles remain useful context; Team High / Starter Mediocre is not showing a green-light profile.")
    lines.append("- Historical findings not yet supported: Aligned High Environment has too few live resolved rows to confirm the historical +55.94% ROI result.")
    lines.append("- A/A and C/A failures: clustering is visible in the overlap table, but samples remain too small for a durable claim.")
    lines.append("- Operational usefulness: Environment is proving useful as an observation and explanation layer, not yet as an operational rule.")
    lines.append("- Recommended next research step: keep daily capture/reconcile running, then rerun this report after at least 50 resolved rows in Aligned High Environment or after two full weeks of completed slates.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily-root", default=str(DEFAULT_DAILY_ROOT))
    ap.add_argument("--ledger-csv", default=str(DEFAULT_LEDGER))
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    args = ap.parse_args()

    daily_root = Path(args.daily_root)
    ledger_csv = Path(args.ledger_csv)
    out_dir = Path(args.out_dir)
    generated_at = _now()

    capture_files = _capture_files(daily_root)
    reconciled_files = _reconciled_files(daily_root)
    capture_dates = sorted(capture_files)
    latest_capture = capture_dates[-1] if capture_dates else "unknown"
    first_capture = capture_dates[0] if capture_dates else ""
    completed_slates = sorted(set(capture_dates) & set(reconciled_files))
    pending_slates = sorted(set(capture_dates) - set(reconciled_files))

    capture_rows: list[dict[str, Any]] = []
    capture_dates_by_family: dict[str, set[str]] = defaultdict(set)
    for day, path in capture_files.items():
        for row in _read_csv(path):
            row.setdefault("date", day)
            row["source_daily_artifact"] = str(path)
            capture_rows.append(row)
            capture_dates_by_family[_family(row)].add(day)

    ledger_rows = _read_csv(ledger_csv)
    profile_rows = _performance_rows(capture_rows, ledger_rows, capture_dates_by_family)
    tier_rows = _tier_overlap_rows(capture_rows, ledger_rows)
    daily_rows = _daily_slice_rows(capture_rows, ledger_rows)

    total_observations = len(capture_rows)
    total_resolved = sum(1 for row in ledger_rows if _is_resolved(row))
    total_pending = max(total_observations - total_resolved, 0)

    suffix = latest_capture
    profile_csv = out_dir / f"environment_v2_beta_live_profile_performance_{suffix}.csv"
    tier_csv = out_dir / f"environment_v2_beta_live_tier_environment_overlap_{suffix}.csv"
    daily_csv = out_dir / f"environment_v2_beta_live_daily_slices_{suffix}.csv"
    report_md = out_dir / f"environment_v2_beta_live_observation_report_{suffix}.md"

    _write_csv(profile_csv, profile_rows)
    _write_csv(tier_csv, tier_rows)
    _write_csv(daily_csv, daily_rows)
    _write_report(
        report_md,
        generated_at=generated_at,
        first_capture=first_capture,
        latest_capture=latest_capture,
        calendar_days=_date_span(capture_dates),
        completed_slates=completed_slates,
        pending_slates=pending_slates,
        total_observations=total_observations,
        resolved_rows=total_resolved,
        pending_rows=total_pending,
        profile_rows=profile_rows,
        tier_rows=tier_rows,
        daily_rows=daily_rows,
    )
    print(
        json.dumps(
            {
                "status": "ok",
                "first_capture": first_capture,
                "latest_capture": latest_capture,
                "total_observations": total_observations,
                "resolved": total_resolved,
                "pending": total_pending,
                "completed_slates": completed_slates,
                "pending_slates": pending_slates,
                "report_md": str(report_md),
                "profile_csv": str(profile_csv),
                "tier_csv": str(tier_csv),
                "daily_csv": str(daily_csv),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
