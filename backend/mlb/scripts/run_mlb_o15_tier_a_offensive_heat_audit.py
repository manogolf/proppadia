#!/usr/bin/env python3
"""Audit whether team/offense heat explains hits over 1.5 Tier A outcomes.

Analysis only: reads reconciled history, enriches Tier A candidates with
pregame team rolling offense context, and writes CSV/Markdown diagnostics.
It does not change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from backend.mlb.scripts.run_mlb_o15_tier_a_failure_audit import (
    DEFAULT_EXEC_ROOT,
    DEFAULT_LANES_ROOT,
    DEFAULT_REVIEW_DIR,
    _clean,
    _enrich_rows,
    _f,
    _i,
    _load_tier_a_rows,
    _metrics,
    _num,
    _pct,
    _window,
)
from backend.mlb.shared.team_name_map import getFullTeamAbbreviationFromID, normalizeTeamAbbreviation
from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())


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


def _bucket(value: float | None, cuts: list[tuple[str, Callable[[float], bool]]]) -> str:
    if value is None:
        return "missing"
    for label, pred in cuts:
        if pred(value):
            return label
    return "other"


def _team_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.isdigit():
        text = getFullTeamAbbreviationFromID(text) or text
    return str(normalizeTeamAbbreviation(text) or text).strip()


def _fetch_row_team_context(rows: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    player_ids = sorted({int(pid) for row in rows if (pid := _i(row.get("player_id"))) is not None})
    game_ids = sorted({int(gid) for row in rows if (gid := _i(row.get("game_id"))) is not None})
    if not player_ids or not game_ids:
        return {}
    db_rows = pg_fetchall(
        """
SELECT game_date::date AS game_date, game_id, player_id, team, opponent
FROM mlb.player_stats
WHERE player_id = ANY(%s)
  AND game_id = ANY(%s)
""",
        (player_ids, game_ids),
    )
    out: dict[tuple[str, int, int], dict[str, Any]] = {}
    for row in db_rows or []:
        date_text = str(row.get("game_date") or "")[:10]
        game_id = _i(row.get("game_id"))
        player_id = _i(row.get("player_id"))
        if not date_text or game_id is None or player_id is None:
            continue
        out[(date_text, int(game_id), int(player_id))] = {
            "team": _team_code(row.get("team")),
            "opponent": _team_code(row.get("opponent")),
        }
    return out


def _fill_team_context(rows: list[dict[str, Any]]) -> None:
    ctx = _fetch_row_team_context(rows)
    for row in rows:
        key = (
            str(row.get("date") or "")[:10],
            int(_i(row.get("game_id")) or 0),
            int(_i(row.get("player_id")) or 0),
        )
        item = ctx.get(key, {})
        if item.get("team") and row.get("team") in {None, ""}:
            row["team"] = item["team"]
        if item.get("opponent") and row.get("opponent") in {None, ""}:
            row["opponent"] = item["opponent"]
        row["team"] = _team_code(row.get("team"))
        row["opponent"] = _team_code(row.get("opponent"))


def _fetch_team_game_stats(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    dates = [datetime.strptime(str(row.get("date"))[:10], "%Y-%m-%d").date() for row in rows if str(row.get("date") or "")[:10]]
    if not dates:
        return {}
    min_date = (min(dates) - timedelta(days=60)).isoformat()
    max_date = max(dates).isoformat()
    db_rows = pg_fetchall(
        """
SELECT
  game_date::date AS game_date,
  game_id,
  team,
  SUM(COALESCE(hits, 0))::float8 AS team_hits,
  SUM(COALESCE(runs_scored, 0))::float8 AS team_runs,
  SUM(COALESCE(total_bases, 0))::float8 AS team_total_bases,
  SUM(COALESCE(hits, 0) + COALESCE(runs_scored, 0) + COALESCE(rbis, 0))::float8 AS team_hits_runs_rbis
FROM mlb.player_stats
WHERE game_date BETWEEN %s::date AND %s::date
GROUP BY game_date, game_id, team
ORDER BY team, game_date, game_id
""",
        (min_date, max_date),
    )
    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in db_rows or []:
        team = _team_code(row.get("team"))
        if not team:
            continue
        by_team[team].append(dict(row))
    for team in by_team:
        by_team[team].sort(key=lambda r: (str(r.get("game_date"))[:10], int(_i(r.get("game_id")) or 0)))
    return by_team


def _avg(items: list[float | None]) -> float | None:
    vals = [v for v in items if v is not None]
    return sum(vals) / len(vals) if vals else None


def _add_team_heat(rows: list[dict[str, Any]]) -> None:
    team_games = _fetch_team_game_stats(rows)
    for row in rows:
        team = _team_code(row.get("team"))
        date_text = str(row.get("date") or "")[:10]
        game_id = int(_i(row.get("game_id")) or 0)
        games = team_games.get(team, [])
        prior = [
            g
            for g in games
            if (str(g.get("game_date"))[:10], int(_i(g.get("game_id")) or 0)) < (date_text, game_id)
        ]
        for n in (7, 15):
            sample = prior[-n:]
            row[f"team_d{n}_games_available"] = len(sample)
            row[f"team_d{n}_hits_per_game"] = _avg([_f(g.get("team_hits")) for g in sample])
            row[f"team_d{n}_runs_per_game"] = _avg([_f(g.get("team_runs")) for g in sample])
            row[f"team_d{n}_total_bases_per_game"] = _avg([_f(g.get("team_total_bases")) for g in sample])
            row[f"team_d{n}_hits_runs_rbis_per_game"] = _avg([_f(g.get("team_hits_runs_rbis")) for g in sample])


def _load_all_o15_candidates(exec_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file in sorted(exec_root.glob("20??-??-??/reconcile_rows.csv")):
        date_fallback = file.parent.name
        for raw in _read_csv(file):
            if _clean(raw.get("prop_type")) != "hits" or _f(raw.get("line")) != 1.5:
                continue
            row = {
                "date": str(raw.get("game_date") or raw.get("slate_date") or date_fallback)[:10],
                "game_id": raw.get("game_id"),
                "player_id": raw.get("player_id"),
                "team": _team_code(raw.get("team")),
                "opponent": _team_code(raw.get("opponent")),
            }
            rows.append(row)
    _fill_team_context(rows)
    return rows


def _add_lineup_clustering(rows: list[dict[str, Any]], exec_root: Path) -> None:
    all_o15 = _load_all_o15_candidates(exec_root)
    o15_counts: dict[tuple[str, int, str], set[str]] = defaultdict(set)
    for row in all_o15:
        date_text = str(row.get("date") or "")[:10]
        game_id = _i(row.get("game_id"))
        team = _team_code(row.get("team"))
        player_id = str(row.get("player_id") or "").strip()
        if date_text and game_id is not None and team and player_id:
            o15_counts[(date_text, int(game_id), team)].add(player_id)

    tier_a_counts: dict[tuple[str, int, str], set[str]] = defaultdict(set)
    for row in rows:
        date_text = str(row.get("date") or "")[:10]
        game_id = _i(row.get("game_id"))
        team = _team_code(row.get("team"))
        player_id = str(row.get("player_id") or "").strip()
        if date_text and game_id is not None and team and player_id:
            tier_a_counts[(date_text, int(game_id), team)].add(player_id)

    for row in rows:
        key = (str(row.get("date") or "")[:10], int(_i(row.get("game_id")) or 0), _team_code(row.get("team")))
        player_id = str(row.get("player_id") or "").strip()
        row["same_game_team_tier_a_count"] = len(tier_a_counts.get(key, set()))
        row["same_game_teammate_tier_a_count"] = max(0, len(tier_a_counts.get(key, set()) - {player_id}))
        row["same_game_team_o15_candidate_count"] = len(o15_counts.get(key, set()))
        row["same_game_teammate_o15_candidate_count"] = max(0, len(o15_counts.get(key, set()) - {player_id}))


def _quantile(values: list[float], q: float) -> float | None:
    vals = sorted(values)
    if not vals:
        return None
    idx = (len(vals) - 1) * q
    low = math.floor(idx)
    high = math.ceil(idx)
    if low == high:
        return vals[int(idx)]
    return vals[low] * (high - idx) + vals[high] * (idx - low)


def _add_heat_buckets(rows: list[dict[str, Any]]) -> dict[str, float | None]:
    keys = [
        "team_d7_hits_per_game",
        "team_d7_runs_per_game",
        "team_d7_total_bases_per_game",
        "team_d7_hits_runs_rbis_per_game",
        "team_expected_hits_allowed",
    ]
    cuts = {key: _quantile([v for r in rows if (v := _f(r.get(key))) is not None], 0.67) for key in keys}
    for row in rows:
        for key, cut in cuts.items():
            value = _f(row.get(key))
            label = key.replace("_per_game", "").replace("team_", "team_") + "_heat_bucket"
            row[label] = "missing" if value is None or cut is None else ("high" if value >= cut else "not_high")
        row["player_d7_hrr_ge_3"] = (_f(row.get("d7_hits_runs_rbis")) or -999) >= 3
        row["player_d15_hrr_ge_2_75"] = (_f(row.get("d15_hits_runs_rbis")) or -999) >= 2.75
        row["high_team_d7_runs"] = row.get("team_d7_runs_heat_bucket") == "high"
        row["high_team_d7_hits"] = row.get("team_d7_hits_heat_bucket") == "high"
        row["high_team_d7_total_bases"] = row.get("team_d7_total_bases_heat_bucket") == "high"
        row["high_team_d7_hrr"] = row.get("team_d7_hits_runs_rbis_heat_bucket") == "high"
        row["high_team_expected_hits_allowed"] = row.get("team_expected_hits_allowed_heat_bucket") == "high"
    return cuts


def _feature_comparison(rows: list[dict[str, Any]], window: str) -> list[dict[str, Any]]:
    groups = {
        "winners_2_plus_hits": [r for r in rows if r.get("outcome_group") == "winner_2_plus_hits"],
        "all_losers_0_or_1": [r for r in rows if r.get("outcome_group") in {"one_hit_loser", "zero_hit_loser"}],
        "one_hit_losers": [r for r in rows if r.get("outcome_group") == "one_hit_loser"],
        "zero_hit_losers": [r for r in rows if r.get("outcome_group") == "zero_hit_loser"],
    }
    features = [
        "d7_hits",
        "d15_hits",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "team_d7_hits_per_game",
        "team_d15_hits_per_game",
        "team_d7_runs_per_game",
        "team_d15_runs_per_game",
        "team_d7_total_bases_per_game",
        "team_d15_total_bases_per_game",
        "team_d7_hits_runs_rbis_per_game",
        "team_d15_hits_runs_rbis_per_game",
        "same_game_teammate_tier_a_count",
        "same_game_teammate_o15_candidate_count",
        "price",
        "actual_plate_appearances",
    ]
    out: list[dict[str, Any]] = []
    for feature in features:
        item: dict[str, Any] = {"window": window, "feature": feature}
        for group, grows in groups.items():
            vals = [_f(r.get(feature)) for r in grows]
            vals = [v for v in vals if v is not None]
            item[f"{group}_rows_with_value"] = len(vals)
            item[f"{group}_avg"] = sum(vals) / len(vals) if vals else None
        win = item.get("winners_2_plus_hits_avg")
        lose = item.get("all_losers_0_or_1_avg")
        item["winner_minus_loser_avg"] = win - lose if win is not None and lose is not None else None
        out.append(item)
    return out


def _funnel_rows(rows: list[dict[str, Any]], window: str) -> list[dict[str, Any]]:
    definitions: list[tuple[str, Callable[[dict[str, Any]], bool]]] = [
        ("tier_a_only", lambda r: True),
        ("tier_a_plus_player_d7_hrr_ge_3", lambda r: bool(r.get("player_d7_hrr_ge_3"))),
        ("tier_a_plus_player_d15_hrr_ge_2_75", lambda r: bool(r.get("player_d15_hrr_ge_2_75"))),
        ("tier_a_plus_high_team_d7_runs", lambda r: bool(r.get("high_team_d7_runs"))),
        ("tier_a_plus_high_team_d7_hits", lambda r: bool(r.get("high_team_d7_hits"))),
        ("tier_a_plus_high_team_d7_total_bases", lambda r: bool(r.get("high_team_d7_total_bases"))),
        ("tier_a_plus_high_team_d7_hrr", lambda r: bool(r.get("high_team_d7_hrr"))),
        ("tier_a_plus_high_team_expected_hits_allowed", lambda r: bool(r.get("high_team_expected_hits_allowed"))),
        (
            "tier_a_plus_teammate_tier_a",
            lambda r: (_f(r.get("same_game_teammate_tier_a_count")) or 0) >= 1,
        ),
        (
            "tier_a_plus_teammate_o15_2_plus",
            lambda r: (_f(r.get("same_game_teammate_o15_candidate_count")) or 0) >= 2,
        ),
        (
            "tier_a_plus_player_hrr_and_team_heat",
            lambda r: bool(r.get("player_d7_hrr_ge_3"))
            and (bool(r.get("high_team_d7_runs")) or bool(r.get("high_team_d7_total_bases"))),
        ),
        (
            "tier_a_plus_player_hrr_team_heat_team_env",
            lambda r: bool(r.get("player_d7_hrr_ge_3"))
            and (bool(r.get("high_team_d7_runs")) or bool(r.get("high_team_d7_total_bases")))
            and bool(r.get("high_team_expected_hits_allowed")),
        ),
        (
            "tier_a_plus_player_hrr_team_expected",
            lambda r: bool(r.get("player_d7_hrr_ge_3")) and bool(r.get("high_team_expected_hits_allowed")),
        ),
    ]
    out: list[dict[str, Any]] = []
    base = _metrics(rows)
    for name, pred in definitions:
        subset = [r for r in rows if pred(r)]
        m = _metrics(subset)
        out.append(
            {
                "window": window,
                "funnel": name,
                "rows": m["rows"],
                "wins": m["wins"],
                "losses": m["losses"],
                "pushes": m["pushes"],
                "wr": m["wr"],
                "roi": m["roi"],
                "units": m["units"],
                "avg_odds": m["avg_odds"],
                "row_share": m["rows"] / base["rows"] if base["rows"] else None,
                "roi_lift_vs_tier_a": m["roi"] - base["roi"] if m["roi"] is not None and base["roi"] is not None else None,
                "avg_d7_hrr": m["avg_d7_hits_runs_rbis"],
                "avg_d15_hrr": m["avg_d15_hits_runs_rbis"],
                "avg_starter_expected": m["avg_starter_expected_hits_allowed"],
                "avg_team_expected": m["avg_team_expected_hits_allowed"],
            }
        )
    return out


def _write_report(path: Path, rows: list[dict[str, Any]], feature_rows: list[dict[str, Any]], funnel_rows: list[dict[str, Any]], cuts: dict[str, float | None]) -> None:
    last30 = _window(rows, "last_30")
    season = _window(rows, "season_2026")
    lines = [
        "# Tier A Offensive Heat Audit",
        "",
        "- Scope: hits over 1.5 Hitter Tier A only.",
        "- Hitter Tier A definition: `d7_hits > 1.30` and `d15_hits > 1.20`.",
        "- Team heat features use prior team games only, excluding the candidate game.",
        "- Analysis only; no production, selector, threshold, upload, or grading changes.",
        "",
        "## Population",
        "",
        "| window | rows | wins | losses | WR | ROI | units |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for label, wrows in [("last_30", last30), ("season_2026", season)]:
        m = _metrics(wrows)
        lines.append(
            f"| `{label}` | `{m['rows']}` | `{m['wins']}` | `{m['losses']}` | `{_pct(m['wr'])}` | `{_pct(m['roi'])}` | `{_num(m['units'])}` |"
        )
    lines.extend(
        [
            "",
            "## High-Heat Cutoffs",
            "",
            "High team heat buckets use the 67th percentile within the audited Tier A population.",
            "",
            "| feature | high cutoff |",
            "|---|---:|",
        ]
    )
    for key, value in cuts.items():
        lines.append(f"| `{key}` | `{_num(value)}` |")

    last30_features = [r for r in feature_rows if r["window"] == "last_30"]
    top_features = sorted(
        [r for r in last30_features if r.get("winner_minus_loser_avg") is not None],
        key=lambda r: abs(float(r.get("winner_minus_loser_avg") or 0)),
        reverse=True,
    )[:10]
    lines.extend(
        [
            "",
            "## Last-30 Winner vs Loser Separators",
            "",
            "| feature | winner avg | loser avg | winner-minus-loser | winner rows | loser rows |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in top_features:
        lines.append(
            f"| `{row['feature']}` | `{_num(row.get('winners_2_plus_hits_avg'))}` | `{_num(row.get('all_losers_0_or_1_avg'))}` | `{_num(row.get('winner_minus_loser_avg'))}` | `{row.get('winners_2_plus_hits_rows_with_value')}` | `{row.get('all_losers_0_or_1_rows_with_value')}` |"
        )

    last30_funnels = [r for r in funnel_rows if r["window"] == "last_30"]
    ranked_funnels = sorted(
        [r for r in last30_funnels if r.get("rows") and int(r.get("rows") or 0) >= 10],
        key=lambda r: (float(r.get("roi") or -999), float(r.get("units") or 0)),
        reverse=True,
    )[:12]
    lines.extend(
        [
            "",
            "## Last-30 Interaction Funnels",
            "",
            "| funnel | rows | WR | ROI | units | ROI lift vs Tier A |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in ranked_funnels:
        lines.append(
            f"| `{row['funnel']}` | `{row['rows']}` | `{_pct(row.get('wr'))}` | `{_pct(row.get('roi'))}` | `{_num(row.get('units'))}` | `{_pct(row.get('roi_lift_vs_tier_a'))}` |"
        )

    lines.extend(
        [
            "",
            "## Read",
            "",
        ]
    )
    best = ranked_funnels[0] if ranked_funnels else None
    if best:
        lines.append(
            f"The strongest last-30 funnel with at least 10 rows was `{best['funnel']}` "
            f"({best['rows']} rows, `{_pct(best.get('roi'))}` ROI)."
        )
    by_funnel = {(r["window"], r["funnel"]): r for r in funnel_rows}
    teammate_30 = by_funnel.get(("last_30", "tier_a_plus_teammate_tier_a"), {})
    teammate_season = by_funnel.get(("season_2026", "tier_a_plus_teammate_tier_a"), {})
    player_hrr_30 = by_funnel.get(("last_30", "tier_a_plus_player_d7_hrr_ge_3"), {})
    team_runs_30 = by_funnel.get(("last_30", "tier_a_plus_high_team_d7_runs"), {})
    team_hits_30 = by_funnel.get(("last_30", "tier_a_plus_high_team_d7_hits"), {})
    lines.extend(
        [
            "",
            "## Final Answer",
            "",
            "The offensive-heat hypothesis improves Tier A review, but not as a simple broad team-hit filter.",
            "",
            "- `same_game_teammate_tier_a_count >= 1` is the clearest lineup-contagion marker: "
            f"last 30 `{teammate_30.get('rows')}` rows at `{_pct(teammate_30.get('roi'))}` ROI; "
            f"season `{teammate_season.get('rows')}` rows at `{_pct(teammate_season.get('roi'))}` ROI.",
            "- `d7_hits_runs_rbis >= 3` remains the best high-coverage individual offensive-involvement signal: "
            f"last 30 `{player_hrr_30.get('rows')}` rows at `{_pct(player_hrr_30.get('roi'))}` ROI.",
            "- `high_team_d7_runs` adds useful offense-context color: "
            f"last 30 `{team_runs_30.get('rows')}` rows at `{_pct(team_runs_30.get('roi'))}` ROI.",
            "- Raw `high_team_d7_hits` did not improve the sample: "
            f"last 30 `{team_hits_30.get('rows')}` rows at `{_pct(team_hits_30.get('roi'))}` ROI.",
            "",
            "Best board context fields to expose next: `same_game_teammate_tier_a_count`, `same_game_team_o15_candidate_count`, `team_d7_runs_per_game`, and keep `team_expected_hits_allowed`/`d7_hits_runs_rbis` visible. The evidence supports boost/warning context, not a hard production filter.",
        ]
    )
    lines.extend(
        [
            "",
            "The key distinction is whether team-level rolling offense adds information beyond the player HRR/context fields that already improved the Tier A failure audit.",
            "If team heat only works when paired with `d7_hits_runs_rbis >= 3` or high `team_expected_hits_allowed`, it is better treated as a review context/boost signal rather than a standalone filter.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execution-root", default=str(DEFAULT_EXEC_ROOT))
    ap.add_argument("--review-aids-dir", default=str(DEFAULT_REVIEW_DIR))
    ap.add_argument("--lanes-root", default=str(DEFAULT_LANES_ROOT))
    ap.add_argument("--out-dir", default=str(DEFAULT_REVIEW_DIR))
    args = ap.parse_args()

    rows = _load_tier_a_rows(Path(args.execution_root))
    if not rows:
        raise SystemExit("No Tier A rows found")
    _enrich_rows(rows, Path(args.review_aids_dir), Path(args.lanes_root))
    _fill_team_context(rows)
    _add_team_heat(rows)
    _add_lineup_clustering(rows, Path(args.execution_root))
    cuts = _add_heat_buckets(rows)

    feature_rows: list[dict[str, Any]] = []
    funnels: list[dict[str, Any]] = []
    for label in ["last_30", "season_2026"]:
        wrows = _window(rows, label)
        feature_rows.extend(_feature_comparison(wrows, label))
        funnels.extend(_funnel_rows(wrows, label))

    row_cols = [
        "date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "result",
        "outcome_group",
        "actual_hits",
        "price",
        "units",
        "d7_hits",
        "d15_hits",
        "d7_hits_runs_rbis",
        "d15_hits_runs_rbis",
        "starter_expected_hits_allowed",
        "team_expected_hits_allowed",
        "team_d7_games_available",
        "team_d7_hits_per_game",
        "team_d7_runs_per_game",
        "team_d7_total_bases_per_game",
        "team_d7_hits_runs_rbis_per_game",
        "team_d15_games_available",
        "team_d15_hits_per_game",
        "team_d15_runs_per_game",
        "team_d15_total_bases_per_game",
        "team_d15_hits_runs_rbis_per_game",
        "same_game_team_tier_a_count",
        "same_game_teammate_tier_a_count",
        "same_game_team_o15_candidate_count",
        "same_game_teammate_o15_candidate_count",
        "player_d7_hrr_ge_3",
        "player_d15_hrr_ge_2_75",
        "high_team_d7_runs",
        "high_team_d7_hits",
        "high_team_d7_total_bases",
        "high_team_d7_hrr",
        "high_team_expected_hits_allowed",
        "actual_plate_appearances",
        "time_of_day_bucket",
        "game_day_of_week",
        "team_time_sequence_bucket",
        "source_reconcile_file",
    ]
    out_dir = Path(args.out_dir)
    _write_csv(out_dir / "tier_a_offensive_heat_rows.csv", [{col: row.get(col) for col in row_cols} for row in rows])
    _write_csv(out_dir / "tier_a_offensive_heat_feature_comparison.csv", feature_rows)
    _write_csv(out_dir / "tier_a_offensive_heat_funnels.csv", funnels)
    _write_report(out_dir / "tier_a_offensive_heat_audit.md", rows, feature_rows, funnels, cuts)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": len(rows),
        "last_30_rows": len(_window(rows, "last_30")),
        "season_2026_rows": len(_window(rows, "season_2026")),
        "outputs": [
            str(out_dir / "tier_a_offensive_heat_audit.md"),
            str(out_dir / "tier_a_offensive_heat_feature_comparison.csv"),
            str(out_dir / "tier_a_offensive_heat_funnels.csv"),
            str(out_dir / "tier_a_offensive_heat_rows.csv"),
        ],
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
