#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_ROWS = Path("artifacts/analysis/mlb/review_aids/hits_o15_tier_backtest_rows.csv")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/review_aids")

DATE_LABEL = "2026-06-29"

COMPONENT_FIELDS = [
    "offense_factor_vs_league_clamped",
    "offense_hits_form_blended",
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "starter_expected_hits_allowed",
    "bullpen_hits_allowed_form_blended",
    "team_expected_hits_allowed",
]

DASHBOARD_FIELDS = [
    "env_v2_alpha_research_status",
    "env_v2_alpha_bucket_schema",
    "env_v2_alpha_offense_strength_bucket",
    "env_v2_alpha_offense_form_bucket",
    "env_v2_alpha_pitcher_base_bucket",
    "env_v2_alpha_starter_matchup_bucket",
    "env_v2_alpha_bullpen_support_bucket",
    "env_v2_alpha_team_rollup_bucket",
    "env_v2_alpha_agreement_profile",
    "env_v2_alpha_offense_high_starter_high_bullpen_high",
    "env_v2_alpha_offense_high_starter_high_bullpen_low",
    "env_v2_alpha_offense_high_starter_low_bullpen_high",
    "env_v2_alpha_offense_low_starter_high_bullpen_high",
    "env_v2_alpha_team_expected_high_starter_mediocre",
    "env_v2_alpha_starter_high_team_expected_mediocre",
    "env_v2_alpha_component_missing_count",
    "env_v2_alpha_component_coverage",
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


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


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


def _nonblank(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text and text.lower() not in {"nan", "none", "null"})


def _bucket(value: float | None, low_max: float, high_min: float) -> str:
    if value is None:
        return "missing"
    if value < low_max:
        return "low"
    if value >= high_min:
        return "high"
    return "mid"


def _flag(value: bool) -> str:
    return "yes" if value else "no"


def _american_implied(price: float | None) -> float | None:
    if price is None:
        return None
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return 100.0 / (price + 100.0)


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
    return {
        "rows": len(rows),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": (wins / graded) if graded else "",
        "roi": (units / len(resolved)) if resolved else "",
        "units": units,
        "avg_odds": mean(odds) if odds else "",
        "avg_implied": mean(implied) if implied else "",
    }


def _sample_flag(resolved: int) -> str:
    if resolved < 25:
        return "small_lt_25"
    if resolved < 50:
        return "thin_lt_50"
    return "ok"


def _add_dashboard(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    offense_factor = _f(row.get("offense_factor_vs_league_clamped"))
    offense_form = _f(row.get("offense_hits_form_blended"))
    pitcher_base = _f(row.get("pitcher_expected_hits_allowed_weighted"))
    if pitcher_base is None:
        pitcher_base = _f(row.get("pitcher_base"))
    starter = _f(row.get("starter_expected_hits_allowed"))
    bullpen = _f(row.get("bullpen_hits_allowed_form_blended"))
    team = _f(row.get("team_expected_hits_allowed"))

    offense_bucket = _bucket(offense_factor, 0.95, 1.05)
    offense_form_bucket = _bucket(offense_form, 8.0, 9.0)
    pitcher_base_bucket = _bucket(pitcher_base, 4.5, 5.5)
    starter_bucket = _bucket(starter, 4.5, 5.5)
    bullpen_bucket = _bucket(bullpen, 3.5, 4.5)
    team_bucket = _bucket(team, 8.0, 9.0)

    offense_high_starter_high_bullpen_high = offense_bucket == "high" and starter_bucket == "high" and bullpen_bucket == "high"
    offense_high_starter_high_bullpen_low = offense_bucket == "high" and starter_bucket == "high" and bullpen_bucket == "low"
    offense_high_starter_low_bullpen_high = offense_bucket == "high" and starter_bucket == "low" and bullpen_bucket == "high"
    offense_low_starter_high_bullpen_high = offense_bucket == "low" and starter_bucket == "high" and bullpen_bucket == "high"
    team_expected_high_starter_mediocre = team_bucket == "high" and starter_bucket in {"low", "mid"}
    starter_high_team_expected_mediocre = starter_bucket == "high" and team_bucket in {"low", "mid"}

    if offense_high_starter_high_bullpen_high:
        profile = "offense_high_starter_high_bullpen_high"
    elif offense_high_starter_high_bullpen_low:
        profile = "offense_high_starter_high_bullpen_low"
    elif offense_high_starter_low_bullpen_high:
        profile = "offense_high_starter_low_bullpen_high"
    elif offense_low_starter_high_bullpen_high:
        profile = "offense_low_starter_high_bullpen_high"
    elif team_expected_high_starter_mediocre:
        profile = "team_expected_high_starter_mediocre"
    elif starter_high_team_expected_mediocre:
        profile = "starter_high_team_expected_mediocre"
    elif "missing" in {offense_bucket, starter_bucket, bullpen_bucket, team_bucket}:
        profile = "component_missing"
    else:
        profile = f"offense_{offense_bucket}_starter_{starter_bucket}_bullpen_{bullpen_bucket}"

    missing = sum(1 for field in COMPONENT_FIELDS if _f(row.get(field)) is None)
    out.update(
        {
            "env_v2_alpha_research_status": "research_only_no_tier_change",
            "env_v2_alpha_bucket_schema": "fixed_research_bins_not_optimized",
            "env_v2_alpha_offense_strength_bucket": offense_bucket,
            "env_v2_alpha_offense_form_bucket": offense_form_bucket,
            "env_v2_alpha_pitcher_base_bucket": pitcher_base_bucket,
            "env_v2_alpha_starter_matchup_bucket": starter_bucket,
            "env_v2_alpha_bullpen_support_bucket": bullpen_bucket,
            "env_v2_alpha_team_rollup_bucket": team_bucket,
            "env_v2_alpha_agreement_profile": profile,
            "env_v2_alpha_offense_high_starter_high_bullpen_high": _flag(offense_high_starter_high_bullpen_high),
            "env_v2_alpha_offense_high_starter_high_bullpen_low": _flag(offense_high_starter_high_bullpen_low),
            "env_v2_alpha_offense_high_starter_low_bullpen_high": _flag(offense_high_starter_low_bullpen_high),
            "env_v2_alpha_offense_low_starter_high_bullpen_high": _flag(offense_low_starter_high_bullpen_high),
            "env_v2_alpha_team_expected_high_starter_mediocre": _flag(team_expected_high_starter_mediocre),
            "env_v2_alpha_starter_high_team_expected_mediocre": _flag(starter_high_team_expected_mediocre),
            "env_v2_alpha_component_missing_count": missing,
            "env_v2_alpha_component_coverage": f"{len(COMPONENT_FIELDS) - missing}/{len(COMPONENT_FIELDS)}",
        }
    )
    return out


def _summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs = [
        ("agreement_profile", "env_v2_alpha_agreement_profile"),
        ("offense_strength_bucket", "env_v2_alpha_offense_strength_bucket"),
        ("starter_matchup_bucket", "env_v2_alpha_starter_matchup_bucket"),
        ("bullpen_support_bucket", "env_v2_alpha_bullpen_support_bucket"),
        ("team_rollup_bucket", "env_v2_alpha_team_rollup_bucket"),
        ("offense_form_bucket", "env_v2_alpha_offense_form_bucket"),
        ("pitcher_base_bucket", "env_v2_alpha_pitcher_base_bucket"),
    ]
    out: list[dict[str, Any]] = []
    for family, field in specs:
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[str(row.get(field) or "missing")].append(row)
        for value, group in sorted(groups.items()):
            metrics = _metrics(group)
            out.append(
                {
                    "summary_family": family,
                    "summary_value": value,
                    **metrics,
                    "sample_flag": _sample_flag(int(metrics["resolved"] or 0)),
                    "research_only_note": "Environment v2-alpha dashboard descriptive bucket; not a production rule",
                }
            )
    return out


def _health_rows(rows: list[dict[str, Any]], source_rows: list[dict[str, Any]], source_path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for field in COMPONENT_FIELDS + DASHBOARD_FIELDS:
        present = bool(rows and field in rows[0])
        nonblank = sum(1 for row in rows if _nonblank(row.get(field)))
        out.append(
            {
                "check_group": "component" if field in COMPONENT_FIELDS else "dashboard",
                "field": field,
                "field_present": present,
                "rows": len(rows),
                "nonblank_rows": nonblank,
                "coverage_pct": (nonblank / len(rows)) if rows else "",
                "status": "PASS" if present and (field not in DASHBOARD_FIELDS or nonblank == len(rows)) else "WARN",
                "source_rows": len(source_rows),
                "source_path": _rel(source_path),
            }
        )
    return out


def _write_health_report(path: Path, health: list[dict[str, Any]], rows: list[dict[str, Any]]) -> None:
    pass_count = sum(1 for row in health if row.get("status") == "PASS")
    warn_count = sum(1 for row in health if row.get("status") != "PASS")
    missing_profiles = Counter(row.get("env_v2_alpha_agreement_profile") for row in rows)
    lines = [
        "# Environment v2-alpha Dashboard Health",
        "",
        f"- Generated at: `{_now()}`",
        "- Scope: research-only dashboard coverage.",
        "- Production behavior changed: `no`",
        "- Tier assignment changed: `no`",
        "",
        "## Summary",
        "",
        f"- Dashboard rows: `{len(rows)}`",
        f"- Checks PASS: `{pass_count}`",
        f"- Checks WARN: `{warn_count}`",
        "",
        "## Field Coverage",
        "",
        "| group | field | nonblank | rows | coverage | status |",
        "|---|---|---:|---:|---:|---|",
    ]
    for row in health:
        pct = row.get("coverage_pct")
        pct_text = f"{float(pct):.2%}" if pct != "" else ""
        lines.append(
            f"| `{row.get('check_group')}` | `{row.get('field')}` | `{row.get('nonblank_rows')}` | "
            f"`{row.get('rows')}` | `{pct_text}` | `{row.get('status')}` |"
        )
    lines.extend(
        [
            "",
            "## Agreement Profiles",
            "",
            "| profile | rows |",
            "|---|---:|",
        ]
    )
    for profile, count in missing_profiles.most_common():
        lines.append(f"| `{profile}` | `{count}` |")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def _write_report(path: Path, rows: list[dict[str, Any]], summary: list[dict[str, Any]], health: list[dict[str, Any]]) -> None:
    resolved = [r for r in rows if _clean(r.get("result")) in {"win", "loss", "push"}]
    health_status = "PASS" if all(r.get("status") == "PASS" for r in health) else "WARN"
    top_profiles = [r for r in summary if r.get("summary_family") == "agreement_profile"]
    top_profiles.sort(key=lambda r: int(r.get("resolved") or 0), reverse=True)
    lines = [
        "# Environment v2-alpha Component Dashboard",
        "",
        f"- Generated at: `{_now()}`",
        "- Scope: research-only Hits O1.5 component dashboard.",
        "- v2-alpha is not a new formula.",
        "- v2-alpha is not a replacement tier.",
        "- v2-alpha is not an optimized score.",
        "- Current pitcher tier remains unchanged.",
        "- Production behavior changed: `no`",
        "- Selectors/uploads/grading/thresholds/model predictions changed: `no`",
        "- Morning Workbench/Ops Brief behavior changed: `no`",
        "",
        "## Purpose",
        "",
        "Environment v2-alpha exposes the four retained environment components side by side so research can study agreement and disagreement patterns without collapsing them into a score.",
        "",
        "## Component Dashboard",
        "",
        "| dashboard area | retained fields | descriptive bucket | role |",
        "|---|---|---|---|",
        "| Offense strength | `offense_factor_vs_league_clamped`, `offense_hits_form_blended` | low / mid / high | independent offensive-strength signal |",
        "| Starter matchup | `pitcher_expected_hits_allowed_weighted`, `pitcher_base`, `starter_expected_hits_allowed` | low / mid / high | current pitcher-tier anchor, unchanged |",
        "| Bullpen support | `bullpen_hits_allowed_form_blended` | low / mid / high | full-game continuation context |",
        "| Full-game rollup | `team_expected_hits_allowed` | low / mid / high | disagreement detector / rollup |",
        "",
        "## Research-Only Bucket Definitions",
        "",
        "These are transparent fixed bins for inspection. They are not tuned to ROI and are not production thresholds.",
        "",
        "| field | low | mid | high |",
        "|---|---|---|---|",
        "| `offense_factor_vs_league_clamped` | `< 0.95` | `0.95 to < 1.05` | `>= 1.05` |",
        "| `offense_hits_form_blended` | `< 8.0` | `8.0 to < 9.0` | `>= 9.0` |",
        "| `pitcher_expected_hits_allowed_weighted` / `pitcher_base` | `< 4.5` | `4.5 to < 5.5` | `>= 5.5` |",
        "| `starter_expected_hits_allowed` | `< 4.5` | `4.5 to < 5.5` | `>= 5.5` |",
        "| `bullpen_hits_allowed_form_blended` | `< 3.5` | `3.5 to < 4.5` | `>= 4.5` |",
        "| `team_expected_hits_allowed` | `< 8.0` | `8.0 to < 9.0` | `>= 9.0` |",
        "",
        "## Source Coverage",
        "",
        f"- Dashboard rows: `{len(rows)}`",
        f"- Resolved dashboard rows: `{len(resolved)}`",
        f"- Dashboard health: `{health_status}`",
        "",
        "## Agreement / Disagreement Profiles",
        "",
        "| profile | rows | resolved | W-L-P | WR | ROI | units | avg odds | sample |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in top_profiles:
        lines.append(
            f"| `{row.get('summary_value')}` | `{row.get('rows')}` | `{row.get('resolved')}` | "
            f"`{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | `{_fmt_pct(row.get('wr'))}` | "
            f"`{_fmt_pct(row.get('roi'))}` | `{_fmt(row.get('units'))}` | `{_fmt(row.get('avg_odds'))}` | `{row.get('sample_flag')}` |"
        )
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- The dashboard preserves all component values beside every bucket and flag.",
            "- `starter_expected_hits_allowed` remains the current second-letter input.",
            "- No tier labels are recalculated here.",
            "- No candidate inclusion, ranking, upload, or grading path consumes these fields.",
            "",
            "## Next Research Use",
            "",
            "Use `offensive_environment_v2_alpha_dashboard_rows_2026-06-29.csv` to study whether agreement profiles remain stable across future completed slates. Do not promote any profile into a rule without a separate bakeoff and doctrine checklist.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build research-only Environment v2-alpha component dashboard for MLB Hits O1.5.")
    ap.add_argument("--rows", type=Path, default=DEFAULT_ROWS)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    source_rows = [r for r in _read_csv(args.rows) if _clean(r.get("side")) in {"", "over"}]
    dashboard_rows = [_add_dashboard(r) for r in source_rows]
    summary = _summary_rows(dashboard_rows)
    health = _health_rows(dashboard_rows, source_rows, args.rows)

    out_dir = args.out_dir
    report_md = out_dir / f"offensive_environment_v2_alpha_component_dashboard_{DATE_LABEL}.md"
    rows_csv = out_dir / f"offensive_environment_v2_alpha_dashboard_rows_{DATE_LABEL}.csv"
    summary_csv = out_dir / f"offensive_environment_v2_alpha_dashboard_summary_{DATE_LABEL}.csv"
    health_csv = out_dir / f"offensive_environment_v2_alpha_dashboard_health_{DATE_LABEL}.csv"
    health_md = out_dir / f"offensive_environment_v2_alpha_dashboard_health_{DATE_LABEL}.md"
    latest_json = out_dir / "offensive_environment_v2_alpha_dashboard_health_latest.json"

    _write_csv(rows_csv, dashboard_rows)
    _write_csv(summary_csv, summary)
    _write_csv(health_csv, health)
    _write_health_report(health_md, health, dashboard_rows)
    _write_report(report_md, dashboard_rows, summary, health)

    payload = {
        "status": "pass" if all(r.get("status") == "PASS" for r in health) else "warn",
        "generated_at": _now(),
        "source_rows": len(source_rows),
        "dashboard_rows": len(dashboard_rows),
        "dashboard_fields": DASHBOARD_FIELDS,
        "component_fields": COMPONENT_FIELDS,
        "outputs": {
            "report_md": _rel(report_md),
            "rows_csv": _rel(rows_csv),
            "summary_csv": _rel(summary_csv),
            "health_csv": _rel(health_csv),
            "health_md": _rel(health_md),
        },
        "research_only": True,
        "tier_replacement": False,
        "formula_or_score_introduced": False,
    }
    latest_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
