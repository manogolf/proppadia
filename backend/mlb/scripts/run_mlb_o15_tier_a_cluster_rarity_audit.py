#!/usr/bin/env python3
"""Audit rarity and stability of same-team Tier A clustering.

Analysis only. Reads corrected execution reconcile rows plus the offensive-heat
Tier A audit rows and writes rarity, concentration, comparison, and sensitivity
artifacts. It does not change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts.run_mlb_o15_tier_a_failure_audit import (
    DEFAULT_EXEC_ROOT,
    DEFAULT_REVIEW_DIR,
    _f,
    _i,
    _metrics,
    _num,
    _pct,
    _read_csv,
    _window,
)
from backend.mlb.scripts.run_mlb_o15_tier_a_offensive_heat_audit import (
    _fill_team_context,
    _team_code,
)


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_HEAT_ROWS = DEFAULT_REVIEW_DIR / "tier_a_offensive_heat_rows.csv"


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


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def _bucket(value: float | None, cuts: list[tuple[str, Callable[[float], bool]]]) -> str:
    if value is None:
        return "missing"
    for label, pred in cuts:
        if pred(value):
            return label
    return "other"


def _load_all_o15(exec_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file in sorted(exec_root.glob("20??-??-??/reconcile_rows.csv")):
        date_fallback = file.parent.name
        for raw in _read_csv(file):
            if _clean(raw.get("prop_type")) != "hits" or _f(raw.get("line")) != 1.5:
                continue
            result = _clean(raw.get("actual_over_outcome"))
            rows.append(
                {
                    "date": str(raw.get("game_date") or raw.get("slate_date") or date_fallback)[:10],
                    "game_id": raw.get("game_id"),
                    "player_id": raw.get("player_id"),
                    "player_name": raw.get("player_name"),
                    "team": _team_code(raw.get("team")),
                    "opponent": _team_code(raw.get("opponent")),
                    "prop_type": "hits",
                    "line": 1.5,
                    "side": "over",
                    "result": result if result in {"win", "loss", "push"} else "",
                    "actual_hits": _f(raw.get("actual_value")),
                }
            )
    _fill_team_context(rows)
    return rows


def _load_tier_a_rows(path: Path) -> list[dict[str, Any]]:
    rows = _read_csv(path)
    for row in rows:
        row["team"] = _team_code(row.get("team"))
        row["opponent"] = _team_code(row.get("opponent"))
        row["cluster_flag"] = (_f(row.get("same_game_teammate_tier_a_count")) or 0) > 0
        row["o15_teammate_no_tier_a_flag"] = (
            (_f(row.get("same_game_teammate_o15_candidate_count")) or 0) > 0
            and not row["cluster_flag"]
        )
        row["price_bucket"] = _bucket(
            _f(row.get("price")),
            [
                ("plus_le_125", lambda v: v <= 125),
                ("plus_126_150", lambda v: v <= 150),
                ("plus_151_180", lambda v: v <= 180),
                ("plus_gt_180", lambda v: v > 180),
            ],
        )
        row["starter_bucket"] = _bucket(
            _f(row.get("starter_expected_hits_allowed")),
            [
                ("starter_lt_5", lambda v: v < 5),
                ("starter_5_5.5", lambda v: v < 5.5),
                ("starter_ge_5.5", lambda v: v >= 5.5),
            ],
        )
        row["team_expected_high_low"] = (
            "high_team_expected"
            if (_f(row.get("team_expected_hits_allowed")) is not None and (_f(row.get("team_expected_hits_allowed")) or 0) >= 9.20)
            else "low_or_missing_team_expected"
        )
        row["team_d7_runs_high_low"] = (
            "high_team_d7_runs"
            if (_f(row.get("team_d7_runs_per_game")) is not None and (_f(row.get("team_d7_runs_per_game")) or 0) >= 5.14)
            else "low_or_missing_team_d7_runs"
        )
        row["player_d7_hrr_high_low"] = (
            "player_d7_hrr_ge_3"
            if (_f(row.get("d7_hits_runs_rbis")) is not None and (_f(row.get("d7_hits_runs_rbis")) or 0) >= 3)
            else "player_d7_hrr_lt_3_or_missing"
        )
    return rows


def _metrics_row(label: str, rows: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    m = _metrics(rows)
    out = {
        "segment": label,
        "rows": m["rows"],
        "wins": m["wins"],
        "losses": m["losses"],
        "pushes": m["pushes"],
        "wr": m["wr"],
        "roi": m["roi"],
        "units": m["units"],
        "avg_odds": m["avg_odds"],
        "avg_d7_hrr": m["avg_d7_hits_runs_rbis"],
        "avg_d15_hrr": m["avg_d15_hits_runs_rbis"],
        "avg_team_d7_runs_per_game": _avg(rows, "team_d7_runs_per_game"),
        "avg_team_expected_hits_allowed": m["avg_team_expected_hits_allowed"],
    }
    if extra:
        out.update(extra)
    return out


def _avg(rows: list[dict[str, Any]], col: str) -> float | None:
    vals = [_f(r.get(col)) for r in rows]
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def _by_date_rows(all_o15: list[dict[str, Any]], tier_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dates = sorted({str(r.get("date")) for r in all_o15} | {str(r.get("date")) for r in tier_rows})
    out: list[dict[str, Any]] = []
    for date in dates:
        o15_date = [r for r in all_o15 if r.get("date") == date]
        tier_date = [r for r in tier_rows if r.get("date") == date]
        o15_teams = {str(r.get("team") or "") for r in o15_date if str(r.get("team") or "")}
        team_counts: Counter[str] = Counter()
        for row in tier_date:
            team = str(row.get("team") or "")
            if team:
                team_counts[team] += 1
        out.append(
            {
                "date": date,
                "total_o15_rows": len(o15_date),
                "tier_a_rows": len(tier_date),
                "unique_tier_a_players": len({str(r.get("player_id")) for r in tier_date if str(r.get("player_id") or "")}),
                "teams_with_o15_rows": len(o15_teams),
                "teams_with_0_tier_a_players": len(o15_teams - set(team_counts)),
                "teams_with_1_tier_a_player": sum(1 for v in team_counts.values() if v == 1),
                "teams_with_2_plus_tier_a_players": sum(1 for v in team_counts.values() if v >= 2),
                "max_tier_a_players_on_same_team": max(team_counts.values()) if team_counts else 0,
                "rows_with_same_game_teammate_tier_a": sum(1 for r in tier_date if r.get("cluster_flag")),
            }
        )
    return out


def _concentration_rows(tier_rows: list[dict[str, Any]], window: str) -> list[dict[str, Any]]:
    clusters = [r for r in tier_rows if r.get("cluster_flag")]
    total = len(clusters)
    rows: list[dict[str, Any]] = [
        {"window": window, "category": "summary", "name": "cluster_rows", "rows": total, "row_share": 1.0 if total else None},
        {"window": window, "category": "summary", "name": "dates_represented", "rows": len({r.get("date") for r in clusters}), "row_share": None},
        {"window": window, "category": "summary", "name": "teams_represented", "rows": len({r.get("team") for r in clusters}), "row_share": None},
        {"window": window, "category": "summary", "name": "players_represented", "rows": len({r.get("player_id") for r in clusters}), "row_share": None},
    ]
    for category, key in [("date", "date"), ("team", "team"), ("player", "player_name")]:
        counter = Counter(str(r.get(key) or "") for r in clusters)
        for name, count in counter.most_common():
            if not name:
                continue
            rows.append({"window": window, "category": category, "name": name, "rows": count, "row_share": count / total if total else None})
        top = counter.most_common()
        if top:
            rows.append({"window": window, "category": f"{category}_share", "name": "top_1", "rows": top[0][1], "row_share": top[0][1] / total if total else None})
            rows.append({"window": window, "category": f"{category}_share", "name": "top_3", "rows": sum(v for _, v in top[:3]), "row_share": sum(v for _, v in top[:3]) / total if total else None})
    return rows


def _comparison_rows(tier_rows: list[dict[str, Any]], window: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.append(_metrics_row("tier_a_with_same_team_tier_a_teammate", [r for r in tier_rows if r.get("cluster_flag")], {"window": window}))
    rows.append(_metrics_row("tier_a_without_same_team_tier_a_teammate", [r for r in tier_rows if not r.get("cluster_flag")], {"window": window}))
    rows.append(_metrics_row("tier_a_with_o15_teammate_but_not_tier_a_teammate", [r for r in tier_rows if r.get("o15_teammate_no_tier_a_flag")], {"window": window}))
    rows.append(_metrics_row("tier_a_without_any_o15_teammate", [r for r in tier_rows if (_f(r.get("same_game_teammate_o15_candidate_count")) or 0) == 0], {"window": window}))

    controls = [
        "team_expected_high_low",
        "team_d7_runs_high_low",
        "player_d7_hrr_high_low",
        "price_bucket",
        "starter_bucket",
    ]
    for control in controls:
        for value in sorted({str(r.get(control) or "missing") for r in tier_rows}):
            sub = [r for r in tier_rows if str(r.get(control) or "missing") == value]
            rows.append(_metrics_row(f"{control}={value}", sub, {"window": window, "control": control, "control_value": value}))
            clustered = [r for r in sub if r.get("cluster_flag")]
            if clustered:
                rows.append(
                    _metrics_row(
                        f"{control}={value} + cluster",
                        clustered,
                        {"window": window, "control": control, "control_value": value, "cluster": True},
                    )
                )
    return rows


def _sensitivity_rows(tier_rows: list[dict[str, Any]], window: str) -> list[dict[str, Any]]:
    clusters = [r for r in tier_rows if r.get("cluster_flag")]
    base = _metrics(clusters)
    out = [
        {
            "sensitivity_type": "base_cluster",
            "window": window,
            "excluded_value": "",
            "remaining_rows": base["rows"],
            "wr": base["wr"],
            "roi": base["roi"],
            "units": base["units"],
        }
    ]
    for kind, key in [("leave_one_date", "date"), ("leave_one_team", "team")]:
        for value in sorted({str(r.get(key) or "") for r in clusters if str(r.get(key) or "")}):
            kept = [r for r in clusters if str(r.get(key) or "") != value]
            m = _metrics(kept)
            out.append(
                {
                    "sensitivity_type": kind,
                    "window": window,
                    "excluded_value": value,
                    "excluded_rows": len(clusters) - len(kept),
                    "remaining_rows": m["rows"],
                    "wr": m["wr"],
                    "roi": m["roi"],
                    "units": m["units"],
                    "roi_delta_vs_base": m["roi"] - base["roi"] if m["roi"] is not None and base["roi"] is not None else None,
                }
            )
    return out


def _write_report(path: Path, date_rows: list[dict[str, Any]], concentration: list[dict[str, Any]], comparison: list[dict[str, Any]], sensitivity: list[dict[str, Any]], tier_rows: list[dict[str, Any]]) -> None:
    clusters = [r for r in tier_rows if r.get("cluster_flag")]
    base = _metrics(clusters)
    no_cluster = _metrics([r for r in tier_rows if not r.get("cluster_flag")])
    last30 = _window(tier_rows, "last_30")
    last30_clusters = [r for r in last30 if r.get("cluster_flag")]
    last30_base = _metrics(last30_clusters)
    last30_no_cluster = _metrics([r for r in last30 if not r.get("cluster_flag")])
    total_dates = len(date_rows)
    cluster_dates = sum(1 for r in date_rows if int(r.get("rows_with_same_game_teammate_tier_a") or 0) > 0)
    multi_team_dates = sum(1 for r in date_rows if int(r.get("teams_with_2_plus_tier_a_players") or 0) > 0)
    season_conc = [r for r in concentration if r.get("window") == "season_2026"]
    last30_conc = [r for r in concentration if r.get("window") == "last_30"]
    top_date = next((r for r in season_conc if r.get("category") == "date_share" and r.get("name") == "top_1"), {})
    top_team = next((r for r in season_conc if r.get("category") == "team_share" and r.get("name") == "top_1"), {})
    top_date_30 = next((r for r in last30_conc if r.get("category") == "date_share" and r.get("name") == "top_1"), {})
    top_team_30 = next((r for r in last30_conc if r.get("category") == "team_share" and r.get("name") == "top_1"), {})
    worst_date = min(
        [r for r in sensitivity if r.get("window") == "season_2026" and r.get("sensitivity_type") == "leave_one_date" and r.get("roi") is not None],
        key=lambda r: float(r.get("roi") or 999),
        default={},
    )
    worst_team = min(
        [r for r in sensitivity if r.get("window") == "season_2026" and r.get("sensitivity_type") == "leave_one_team" and r.get("roi") is not None],
        key=lambda r: float(r.get("roi") or 999),
        default={},
    )
    worst_date_30 = min(
        [r for r in sensitivity if r.get("window") == "last_30" and r.get("sensitivity_type") == "leave_one_date" and r.get("roi") is not None],
        key=lambda r: float(r.get("roi") or 999),
        default={},
    )
    worst_team_30 = min(
        [r for r in sensitivity if r.get("window") == "last_30" and r.get("sensitivity_type") == "leave_one_team" and r.get("roi") is not None],
        key=lambda r: float(r.get("roi") or 999),
        default={},
    )
    lines = [
        "# Tier A Cluster Rarity Audit",
        "",
        "- Scope: hits over 1.5 Hitter Tier A rows from corrected execution reconcile artifacts.",
        "- Cluster definition: `same_game_teammate_tier_a_count > 0`.",
        "- Analysis only; no production, board, selector, threshold, upload, or grading changes.",
        "",
        "## Rarity",
        "",
        f"- Dates audited: `{total_dates}`",
        f"- Dates with at least one team having 2+ Tier A players: `{multi_team_dates}`",
        f"- Dates with clustered Tier A rows: `{cluster_dates}`",
        f"- Cluster rows: `{base['rows']}` of `{len(tier_rows)}` Tier A rows.",
        "",
        "## Cluster vs Non-Cluster",
        "",
        "| window | segment | rows | WR | ROI | units | avg odds |",
        "|---|---|---:|---:|---:|---:|---:|",
        f"| `last_30` | `cluster` | `{last30_base['rows']}` | `{_pct(last30_base['wr'])}` | `{_pct(last30_base['roi'])}` | `{_num(last30_base['units'])}` | `{_num(last30_base['avg_odds'])}` |",
        f"| `last_30` | `non_cluster` | `{last30_no_cluster['rows']}` | `{_pct(last30_no_cluster['wr'])}` | `{_pct(last30_no_cluster['roi'])}` | `{_num(last30_no_cluster['units'])}` | `{_num(last30_no_cluster['avg_odds'])}` |",
        f"| `season_2026` | `cluster` | `{base['rows']}` | `{_pct(base['wr'])}` | `{_pct(base['roi'])}` | `{_num(base['units'])}` | `{_num(base['avg_odds'])}` |",
        f"| `season_2026` | `non_cluster` | `{no_cluster['rows']}` | `{_pct(no_cluster['wr'])}` | `{_pct(no_cluster['roi'])}` | `{_num(no_cluster['units'])}` | `{_num(no_cluster['avg_odds'])}` |",
        "",
        "## Concentration",
        "",
        f"- Last 30 top 1 date share: `{_pct(_f(top_date_30.get('row_share')) if top_date_30 else None)}`",
        f"- Last 30 top 1 team share: `{_pct(_f(top_team_30.get('row_share')) if top_team_30 else None)}`",
        f"- Season top 1 date share: `{_pct(_f(top_date.get('row_share')) if top_date else None)}`",
        f"- Season top 1 team share: `{_pct(_f(top_team.get('row_share')) if top_team else None)}`",
        "",
        "## Leave-One Sensitivity",
        "",
        f"- Last 30 worst leave-one-date remaining ROI: `{_pct(worst_date_30.get('roi'))}` after excluding `{worst_date_30.get('excluded_value', '')}`.",
        f"- Last 30 worst leave-one-team remaining ROI: `{_pct(worst_team_30.get('roi'))}` after excluding `{worst_team_30.get('excluded_value', '')}`.",
        f"- Season worst leave-one-date remaining ROI: `{_pct(worst_date.get('roi'))}` after excluding `{worst_date.get('excluded_value', '')}`.",
        f"- Season worst leave-one-team remaining ROI: `{_pct(worst_team.get('roi'))}` after excluding `{worst_team.get('excluded_value', '')}`.",
        "",
        "## Answer",
        "",
    ]
    if base["rows"] < 30:
        lines.append("Same-team Tier A clustering is rare enough that it should be treated as a watch-only boost/context field, not a rule.")
    else:
        lines.append("Same-team Tier A clustering has enough rows for directional review context, but still needs ongoing tracking before it becomes a rule.")
    if worst_date.get("roi") is not None and float(worst_date["roi"]) > 0 and worst_team.get("roi") is not None and float(worst_team["roi"]) > 0:
        lines.append("The signal remains directionally positive in leave-one-date and leave-one-team checks, so it is not entirely carried by a single slate or team.")
    else:
        lines.append("The sensitivity checks show the signal can collapse when specific clusters are removed, so it is likely sample-sensitive.")
    lines.append("Recommended handling: expose `same_game_teammate_tier_a_count` as a review boost/attention flag and keep tracking, but do not use it as a hard filter yet.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execution-root", default=str(DEFAULT_EXEC_ROOT))
    ap.add_argument("--review-aids-dir", default=str(DEFAULT_REVIEW_DIR))
    ap.add_argument("--heat-rows", default=str(DEFAULT_HEAT_ROWS))
    args = ap.parse_args()

    review_dir = Path(args.review_aids_dir)
    tier_rows = _load_tier_a_rows(Path(args.heat_rows))
    all_o15 = _load_all_o15(Path(args.execution_root))
    date_rows = _by_date_rows(all_o15, tier_rows)
    concentration: list[dict[str, Any]] = []
    comparison: list[dict[str, Any]] = []
    sensitivity: list[dict[str, Any]] = []
    for window in ["last_30", "season_2026"]:
        wrows = _window(tier_rows, window)
        concentration.extend(_concentration_rows(wrows, window))
        comparison.extend(_comparison_rows(wrows, window))
        sensitivity.extend(_sensitivity_rows(wrows, window))

    _write_csv(review_dir / "tier_a_cluster_rarity_by_date.csv", date_rows)
    _write_csv(review_dir / "tier_a_cluster_rarity_concentration.csv", concentration)
    _write_csv(review_dir / "tier_a_cluster_rarity_comparison.csv", comparison)
    _write_csv(review_dir / "tier_a_cluster_rarity_sensitivity.csv", sensitivity)
    _write_report(
        review_dir / "tier_a_cluster_rarity_audit.md",
        date_rows,
        concentration,
        comparison,
        sensitivity,
        tier_rows,
    )
    print(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "tier_a_rows": len(tier_rows),
                "cluster_rows": sum(1 for r in tier_rows if r.get("cluster_flag")),
                "dates": len(date_rows),
                "outputs": [
                    str(review_dir / "tier_a_cluster_rarity_audit.md"),
                    str(review_dir / "tier_a_cluster_rarity_by_date.csv"),
                    str(review_dir / "tier_a_cluster_rarity_concentration.csv"),
                    str(review_dir / "tier_a_cluster_rarity_sensitivity.csv"),
                    str(review_dir / "tier_a_cluster_rarity_comparison.csv"),
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
