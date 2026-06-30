#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_DAILY_ROOT = Path("artifacts/analysis/mlb/environment_v2/daily")
DEFAULT_LEDGER_CSV = Path("artifacts/analysis/mlb/environment_v2/ledger/environment_v2_beta_profile_ledger.csv")
DEFAULT_RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")

RECONCILE_FIELDS = [
    "reconciled_at",
    "resolved_status",
    "outcome_status",
    "outcome",
    "win_loss",
    "odds_used",
    "units",
    "roi_result",
    "actual_value",
    "source_reconcile_artifact_path",
    "reconcile_match_method",
    "reconcile_unresolved_reason",
]

LEDGER_FIELDS = [
    "date",
    "player_name",
    "player_id",
    "team",
    "opponent",
    "game_id",
    "canonical_player_id",
    "canonical_game_id",
    "prop_type",
    "side",
    "line",
    "market_price",
    "current_hitter_tier",
    "current_pitcher_tier",
    "current_combined_tier",
    "offense_factor_vs_league_clamped",
    "offense_hits_form_blended",
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "starter_expected_hits_allowed",
    "bullpen_hits_allowed_form_blended",
    "team_expected_hits_allowed",
    "env_v2_beta_profile_family",
    "env_v2_beta_profile_label",
    "env_v2_beta_research_status",
    "resolved_status",
    "outcome_status",
    "outcome",
    "win_loss",
    "odds_used",
    "units",
    "roi_result",
    "actual_value",
    "source_artifact_path",
    "source_daily_artifact",
    "source_reconcile_artifact_path",
]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _s(value: Any) -> str:
    return str(value or "").strip()


def _norm(value: Any) -> str:
    return _s(value).lower()


def _line_key(value: Any) -> str:
    text = _s(value)
    if not text:
        return ""
    try:
        return f"{float(text):.3f}"
    except Exception:
        return text


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _as_float(value: Any) -> float | None:
    try:
        text = _s(value)
        if not text or text.lower() in {"nan", "none", "null"}:
            return None
        return float(text)
    except Exception:
        return None


def _american_profit(price: float | None) -> float | None:
    if price is None:
        return None
    if price > 0:
        return price / 100.0
    if price < 0:
        return 100.0 / abs(price)
    return None


def _price_gap(a: Any, b: Any) -> float:
    af = _as_float(a)
    bf = _as_float(b)
    if af is None or bf is None:
        return 999999.0
    return abs(af - bf)


def _daily_profiles_path(daily_root: Path, date_text: str) -> Path:
    return daily_root / date_text / f"environment_v2_beta_daily_profiles_{date_text}.csv"


def _reconcile_path(reconcile_root: Path, date_text: str) -> Path:
    return reconcile_root / date_text / "reconcile_rows.csv"


def _date_value(row: dict[str, Any]) -> str:
    return _s(row.get("slate_date") or row.get("game_date") or row.get("date"))[:10]


def _player_id(row: dict[str, Any]) -> str:
    return _s(row.get("canonical_player_id") or row.get("player_id"))


def _game_id(row: dict[str, Any]) -> str:
    return _s(row.get("canonical_game_id") or row.get("game_id"))


def _team(row: dict[str, Any]) -> str:
    return _s(row.get("canonical_team") or row.get("team"))


def _opponent(row: dict[str, Any]) -> str:
    return _s(row.get("canonical_opponent") or row.get("opponent"))


def _reconcile_candidate_keys(row: dict[str, Any]) -> list[tuple[str, tuple[str, ...]]]:
    date_text = _date_value(row)
    prop = _norm(row.get("prop_type") or "hits")
    line = _line_key(row.get("line"))
    pid = _player_id(row)
    gid = _game_id(row)
    name = _norm(row.get("player_name") or row.get("market_player_name"))
    team = _norm(_team(row))
    opp = _norm(_opponent(row))
    keys: list[tuple[str, tuple[str, ...]]] = []
    if date_text and pid and gid:
        keys.append(("date_player_game_prop_line", (date_text, pid, gid, prop, line)))
    if date_text and pid:
        keys.append(("date_player_prop_line", (date_text, pid, prop, line)))
    if date_text and name and team and opp:
        keys.append(("date_name_team_opp_prop_line", (date_text, name, team, opp, prop, line)))
    if date_text and name and team:
        keys.append(("date_name_team_prop_line", (date_text, name, team, prop, line)))
    return keys


def _build_reconcile_indexes(rows: list[dict[str, Any]]) -> dict[str, dict[tuple[str, ...], list[dict[str, Any]]]]:
    indexes: dict[str, dict[tuple[str, ...], list[dict[str, Any]]]] = {
        "date_player_game_prop_line": {},
        "date_player_prop_line": {},
        "date_name_team_opp_prop_line": {},
        "date_name_team_prop_line": {},
    }
    for row in rows:
        if _norm(row.get("prop_type")) != "hits":
            continue
        for name, key in _reconcile_candidate_keys(row):
            indexes[name].setdefault(key, []).append(row)
    return indexes


def _choose_match(profile: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if len(candidates) == 1:
        return candidates[0]
    price_fields = ["market_price", "best_over_price", "odds_used"]
    target_price = next((_s(profile.get(field)) for field in price_fields if _s(profile.get(field))), "")
    return sorted(
        candidates,
        key=lambda r: (
            _price_gap(target_price, r.get("price_over_american") or r.get("selected_side_price")),
            -(_as_float(r.get("price_over_american") or r.get("selected_side_price")) or -999999),
        ),
    )[0]


def _match_reconcile(
    profile: dict[str, Any],
    indexes: dict[str, dict[tuple[str, ...], list[dict[str, Any]]]],
) -> tuple[dict[str, Any] | None, str, int]:
    for method, key in _reconcile_candidate_keys(profile):
        matches = indexes.get(method, {}).get(key, [])
        if matches:
            return _choose_match(profile, matches), method, len(matches)
    return None, "", 0


def _apply_outcome(profile: dict[str, Any], match: dict[str, Any] | None, method: str, match_count: int, reconcile_path: Path) -> dict[str, Any]:
    out = dict(profile)
    out["reconciled_at"] = _now()
    out["source_reconcile_artifact_path"] = str(reconcile_path) if reconcile_path.exists() else ""
    if match is None:
        out["resolved_status"] = "unresolved"
        out["outcome_status"] = "pending"
        out["outcome"] = ""
        out["win_loss"] = ""
        out["odds_used"] = _s(out.get("odds_used") or out.get("market_price") or out.get("best_over_price"))
        out["units"] = ""
        out["roi_result"] = ""
        out["actual_value"] = ""
        out["reconcile_match_method"] = ""
        out["reconcile_unresolved_reason"] = "missing_reconcile_match" if reconcile_path.exists() else "missing_reconcile_artifact"
        return out

    outcome = _norm(match.get("actual_over_outcome") or match.get("outcome"))
    actual = _s(match.get("actual_value"))
    price = _s(match.get("price_over_american") or match.get("selected_side_price") or out.get("market_price") or out.get("best_over_price"))
    units = _s(match.get("pnl_over_1u"))
    if not units:
        price_num = _as_float(price)
        if outcome == "win":
            profit = _american_profit(price_num)
            units = "" if profit is None else f"{profit:.6f}"
        elif outcome == "loss":
            units = "-1.000000"
        elif outcome == "push":
            units = "0.000000"

    if outcome in {"win", "loss", "push"}:
        resolved_status = "resolved"
        outcome_status = "resolved"
        reason = ""
    else:
        resolved_status = "unresolved"
        outcome_status = "pending"
        reason = "matched_reconcile_without_resolved_outcome"

    out["resolved_status"] = resolved_status
    out["outcome_status"] = outcome_status
    out["outcome"] = outcome
    out["win_loss"] = outcome
    out["odds_used"] = price
    out["units"] = units
    out["roi_result"] = units
    out["actual_value"] = actual
    out["reconcile_match_method"] = f"{method}:{match_count}"
    out["reconcile_unresolved_reason"] = reason
    return out


def _reconcile_rows(date_text: str, daily_path: Path, reconcile_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    profiles = _read_csv(daily_path)
    reconcile_rows = _read_csv(reconcile_path)
    indexes = _build_reconcile_indexes(reconcile_rows) if reconcile_rows else {}
    out: list[dict[str, Any]] = []
    for profile in profiles:
        match, method, match_count = _match_reconcile(profile, indexes) if indexes else (None, "", 0)
        out.append(_apply_outcome(profile, match, method, match_count, reconcile_path))
    counts = Counter(row.get("resolved_status") or "missing" for row in out)
    outcome_counts = Counter(row.get("outcome_status") or "missing" for row in out)
    reasons = Counter(row.get("reconcile_unresolved_reason") or "resolved" for row in out)
    summary = {
        "date": date_text,
        "generated_at": _now(),
        "research_only": True,
        "production_behavior_changed": False,
        "daily_profiles_csv": str(daily_path),
        "reconcile_rows_csv": str(reconcile_path),
        "reconcile_rows_exists": reconcile_path.exists(),
        "daily_rows": len(profiles),
        "reconcile_rows_loaded": len(reconcile_rows),
        "resolved_status_counts": dict(counts),
        "outcome_status_counts": dict(outcome_counts),
        "unresolved_reason_counts": dict(reasons),
    }
    return out, summary


def _write_summary_md(path: Path, summary: dict[str, Any], reconciled_path: Path, ledger_path: Path) -> None:
    lines = [
        "# Environment v2-beta Daily Reconcile Summary",
        "",
        f"- Date: `{summary['date']}`",
        f"- Generated at: `{summary['generated_at']}`",
        "- Scope: research-only postgame outcome attachment.",
        "- Production behavior changed: `no`",
        "",
        "## Inputs",
        "",
        f"- Daily profiles: `{summary['daily_profiles_csv']}`",
        f"- Reconcile rows: `{summary['reconcile_rows_csv']}`",
        f"- Reconcile artifact exists: `{summary['reconcile_rows_exists']}`",
        "",
        "## Outputs",
        "",
        f"- Reconciled profiles: `{reconciled_path}`",
        f"- Ledger: `{ledger_path}`",
        "",
        "## Status Counts",
        "",
        "| status | rows |",
        "|---|---:|",
    ]
    for status, count in sorted(summary["resolved_status_counts"].items()):
        lines.append(f"| `{status}` | `{count}` |")
    lines.extend(["", "## Unresolved Reasons", "", "| reason | rows |", "|---|---:|"])
    for reason, count in sorted(summary["unresolved_reason_counts"].items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append(f"| `{reason}` | `{count}` |")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- The original daily profile artifact is immutable and was not overwritten.",
            "- Missing same-day reconcile rows leave profiles pending with an explicit reason.",
            "- This output is for research ledgering only and does not affect production grading or uploads.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_health(path: Path, summary: dict[str, Any], reconciled_path: Path) -> None:
    status = "PASS" if reconciled_path.exists() and summary.get("daily_rows", 0) > 0 else "WARN"
    if not summary.get("reconcile_rows_exists"):
        status = "WARN"
    lines = [
        "# Environment v2-beta Daily Reconcile Health",
        "",
        f"- Date: `{summary['date']}`",
        f"- Generated at: `{_now()}`",
        f"- Status: `{status}`",
        "",
        "| check | value | status |",
        "|---|---:|---|",
        f"| daily profiles rows | `{summary.get('daily_rows', 0)}` | `{'PASS' if summary.get('daily_rows', 0) else 'WARN'}` |",
        f"| reconcile artifact exists | `{summary.get('reconcile_rows_exists')}` | `{'PASS' if summary.get('reconcile_rows_exists') else 'WARN'}` |",
        f"| reconciled CSV exists | `{reconciled_path.exists()}` | `{'PASS' if reconciled_path.exists() else 'WARN'}` |",
        "",
        "A missing reconcile artifact is expected before outcomes are available and keeps rows pending.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _build_ledger(daily_root: Path, ledger_csv: Path) -> int:
    rows: list[dict[str, Any]] = []
    for path in sorted(daily_root.glob("20??-??-??/environment_v2_beta_daily_profiles_reconciled_*.csv")):
        for row in _read_csv(path):
            entry = {field: _s(row.get(field)) for field in LEDGER_FIELDS}
            entry["source_daily_artifact"] = _s(row.get("source_daily_artifact") or row.get("source_artifact_path"))
            entry["source_reconciled_artifact"] = str(path)
            rows.append(entry)
    fieldnames = LEDGER_FIELDS + ["source_reconciled_artifact"]
    _write_csv(ledger_csv, rows, fieldnames)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Attach postgame outcomes to Environment v2-beta daily research profiles.")
    ap.add_argument("--date", required=True)
    ap.add_argument("--daily-root", type=Path, default=DEFAULT_DAILY_ROOT)
    ap.add_argument("--reconcile-root", type=Path, default=DEFAULT_RECONCILE_ROOT)
    ap.add_argument("--reconcile-csv", type=Path, default=None)
    ap.add_argument("--ledger-csv", type=Path, default=DEFAULT_LEDGER_CSV)
    args = ap.parse_args()

    date_text = str(args.date)[:10]
    daily_path = _daily_profiles_path(args.daily_root, date_text)
    if not daily_path.exists():
        raise SystemExit(f"daily Environment v2-beta profile artifact not found: {daily_path}")
    reconcile_path = args.reconcile_csv or _reconcile_path(args.reconcile_root, date_text)
    out_dir = args.daily_root / date_text
    reconciled_path = out_dir / f"environment_v2_beta_daily_profiles_reconciled_{date_text}.csv"
    summary_md = out_dir / f"environment_v2_beta_daily_reconcile_summary_{date_text}.md"
    summary_json = out_dir / f"environment_v2_beta_daily_reconcile_summary_{date_text}.json"
    health_md = out_dir / f"environment_v2_beta_daily_reconcile_health_{date_text}.md"

    rows, summary = _reconcile_rows(date_text, daily_path, reconcile_path)
    source_daily = str(daily_path)
    for row in rows:
        row["source_daily_artifact"] = source_daily
    fieldnames = list(dict.fromkeys(list(rows[0].keys()) + RECONCILE_FIELDS + ["source_daily_artifact"])) if rows else RECONCILE_FIELDS
    _write_csv(reconciled_path, rows, fieldnames)
    ledger_rows = _build_ledger(args.daily_root, args.ledger_csv)
    summary["reconciled_profiles_csv"] = str(reconciled_path)
    summary["ledger_csv"] = str(args.ledger_csv)
    summary["ledger_rows"] = ledger_rows
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _write_summary_md(summary_md, summary, reconciled_path, args.ledger_csv)
    _write_health(health_md, summary, reconciled_path)
    print(
        json.dumps(
            {
                "date": date_text,
                "daily_rows": summary["daily_rows"],
                "reconcile_rows_loaded": summary["reconcile_rows_loaded"],
                "resolved_status_counts": summary["resolved_status_counts"],
                "unresolved_reason_counts": summary["unresolved_reason_counts"],
                "reconciled_profiles_csv": str(reconciled_path),
                "ledger_csv": str(args.ledger_csv),
                "ledger_rows": ledger_rows,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
