#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts import run_mlb_review_aid_performance_tracker as tracker


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
WINDOWS = ("full_history", "last_30", "last_14", "last_7", "latest_completed_slate")
BOARD_ORDER = ("o15_watch", "o15_layered", "o15_simple_filter", "o15_alternate_discovery")
ENVIRONMENT_COMPONENT_COLUMNS = [
    "pitcher_expected_hits_allowed_weighted",
    "pitcher_base",
    "offense_hits_pg_last7",
    "offense_hits_pg_last15",
    "offense_hits_pg_last30",
    "offense_hits_form_blended",
    "league_offense_hits_form_blended",
    "offense_factor_vs_league",
    "offense_factor_vs_league_clamped",
    "bullpen_hits_allowed_pg_last7",
    "bullpen_hits_allowed_pg_last15",
    "bullpen_hits_allowed_pg_last30",
    "bullpen_hits_allowed_form_blended",
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    return None if number is None else int(number)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _norm_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return " ".join(text.split())


def _team(value: Any) -> str:
    text = _clean(value).upper()
    if text == "AZ":
        return "ARI"
    if text in {"ATH", "LV", "VIL"}:
        return "OAK"
    return text


def _line_key(value: Any) -> str:
    line = _f(value)
    return f"{line:.1f}" if line is not None else ""


def _date_from_filename(path: Path) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    return match.group(1) if match else ""


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


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _window_labels(date_text: str, latest: str) -> list[str]:
    labels = ["full_history"]
    try:
        d = datetime.strptime(date_text, "%Y-%m-%d").date()
        latest_d = datetime.strptime(latest, "%Y-%m-%d").date()
    except Exception:
        return labels
    delta = (latest_d - d).days
    if delta < 0:
        return labels
    if delta <= 29:
        labels.append("last_30")
    if delta <= 13:
        labels.append("last_14")
    if delta <= 6:
        labels.append("last_7")
    if delta == 0:
        labels.append("latest_completed_slate")
    return labels


def _row_key(row: dict[str, Any]) -> str:
    date_text = str(row.get("board_date") or row.get("date") or "")[:10]
    line = _line_key(row.get("line") or 1.5)
    side = _clean(row.get("side") or "over").lower()
    player_id = _i(row.get("player_id"))
    if player_id is not None:
        return "|".join((date_text, str(player_id), line, side))
    return "|".join(
        (
            date_text,
            _norm_name(row.get("player_name") or row.get("player")),
            _team(row.get("team")),
            _team(row.get("opponent")),
            line,
            side,
        )
    )


def _prefer(current: Any, candidate: Any) -> Any:
    if current not in (None, ""):
        return current
    return candidate


def _source_price(row: dict[str, Any]) -> float | None:
    return _f(row.get("market_price") or row.get("best_over_price") or row.get("board_price"))


def _load_o15_board_rows(review_dir: Path, lanes_root: Path) -> list[dict[str, Any]]:
    board_rows = tracker._load_board_rows(review_dir, lanes_root)
    return [
        row
        for row in board_rows
        if str(row.get("board") or "").startswith("o15_")
        and _clean(row.get("side")).lower() == "over"
        and _line_key(row.get("line")) == "1.5"
    ]


def _merge_rows(board_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in board_rows:
        key = _row_key(row)
        if key:
            groups[key].append(row)

    merged: list[dict[str, Any]] = []
    for key, rows in groups.items():
        rows = sorted(rows, key=lambda r: BOARD_ORDER.index(str(r.get("board"))) if str(r.get("board")) in BOARD_ORDER else 99)
        base = dict(rows[0])
        source_boards = sorted({str(row.get("board")) for row in rows}, key=lambda b: BOARD_ORDER.index(b) if b in BOARD_ORDER else 99)
        in_simple = "o15_simple_filter" in source_boards
        in_watch = "o15_watch" in source_boards
        in_layered = "o15_layered" in source_boards
        in_alternate = "o15_alternate_discovery" in source_boards
        for row in rows[1:]:
            for col in [
                "player_id",
                "player_name",
                "team",
                "opponent",
                "market_price",
                "best_over_price",
                "selected_side_implied_probability",
                "model_prob",
                "d7_hits_rate",
                "d15_hits_rate",
                "d7_hits_runs_rbis",
                "d15_hits_runs_rbis",
                "d30_hits_runs_rbis",
                *ENVIRONMENT_COMPONENT_COLUMNS,
                "hitter_tier",
                "pitcher_tier",
                "combined_tier",
                "layer_label",
                "watch_candidate",
                "alternate_layer",
                "same_game_teammate_tier_a_count",
                "game_time",
                "time_of_day_bucket",
                "game_day_of_week",
                "opposing_starter",
            ]:
                base[col] = _prefer(base.get(col), row.get(col))

        source_list = ",".join(source_boards)
        if in_alternate and not (in_simple or in_watch or in_layered):
            population = "alternate_only"
        elif in_alternate and (in_simple or in_watch or in_layered):
            population = "overlap_main_alternate"
        else:
            population = "main_only"
        base.update(
            {
                "manual_universe_key": key,
                "in_o15_simple": in_simple,
                "in_o15_watch": in_watch,
                "in_o15_layered": in_layered,
                "in_o15_alternate": in_alternate,
                "source_count": len(source_boards),
                "source_list": source_list,
                "manual_population": population,
                "watch_alternate_overlap": bool(in_watch and in_alternate),
                "alternate_layer": base.get("alternate_layer") or "",
                "market_price": _f(base.get("market_price")),
                "best_over_price": _f(base.get("best_over_price")),
                "manual_price": _source_price(base),
                "line": _line_key(base.get("line") or 1.5),
                "side": "over",
                "date": str(base.get("board_date") or base.get("date") or "")[:10],
                "discovery_only_note": "alternate rows are manual/research-only and not production-safe" if in_alternate else "",
            }
        )
        merged.append(base)
    return sorted(merged, key=lambda r: (str(r.get("date")), str(r.get("player_name"))))


def _join_to_reconcile(rows: list[dict[str, Any]], reconcile_root: Path) -> tuple[list[dict[str, Any]], str]:
    reconcile_rows, dates = tracker._load_reconcile_rows(reconcile_root)
    latest = dates[-1] if dates else ""
    indexes = tracker._build_reconcile_indexes(reconcile_rows)
    joined_raw = tracker._join_board_rows(rows, indexes)
    joined: list[dict[str, Any]] = []
    for row in joined_raw:
        out = dict(row)
        win = row.get("win") is True
        loss = row.get("loss") is True
        push = row.get("push") is True
        out["result"] = "win" if win else "loss" if loss else "push" if push else ""
        joined.append(out)
    return joined, latest


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved") is True]
    wins = sum(1 for row in resolved if row.get("win") is True)
    losses = sum(1 for row in resolved if row.get("loss") is True)
    pushes = sum(1 for row in resolved if row.get("push") is True)
    units = sum((_f(row.get("units")) or 0.0) for row in resolved)
    return {
        "rows": len(rows),
        "matched": sum(1 for row in rows if row.get("join_status") == "matched"),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / len(resolved) if resolved else None,
        "units": units,
        "avg_odds": _avg(_f(row.get("manual_price")) for row in rows),
    }


def _avg(values: Any) -> float | None:
    nums = [float(v) for v in values if v is not None]
    return sum(nums) / len(nums) if nums else None


def _summary(joined: list[dict[str, Any]], latest: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary_rows: list[dict[str, Any]] = []
    overlap_rows: list[dict[str, Any]] = []
    population_defs = {
        "main_only": lambda r: r.get("manual_population") == "main_only",
        "alternate_only": lambda r: r.get("manual_population") == "alternate_only",
        "overlap_main_alternate": lambda r: r.get("manual_population") == "overlap_main_alternate",
        "watch_alternate_overlap": lambda r: bool(r.get("watch_alternate_overlap")),
        "all_unified_manual_o15": lambda r: True,
    }
    for window in WINDOWS:
        window_rows = [
            row
            for row in joined
            if window in _window_labels(str(row.get("date") or ""), latest)
            and str(row.get("date") or "") <= latest
        ]
        for population, predicate in population_defs.items():
            rows = [row for row in window_rows if predicate(row)]
            if not rows:
                continue
            item = {"window": window, "population": population}
            item.update(_metrics(rows))
            summary_rows.append(item)
            overlap_rows.append(
                {
                    **item,
                    "source_description": {
                        "main_only": "simple/watch/layered only",
                        "alternate_only": "alternate discovery only; research/manual-only",
                        "overlap_main_alternate": "appears in at least one main board and alternate discovery",
                        "watch_alternate_overlap": "appears in watch candidates and alternate discovery",
                        "all_unified_manual_o15": "deduped union of main boards plus alternate discovery",
                    }.get(population, population),
                }
            )
        for tier in sorted({str(row.get("combined_tier") or "missing") for row in window_rows}):
            rows = [row for row in window_rows if str(row.get("combined_tier") or "missing") == tier]
            if not rows:
                continue
            item = {"window": window, "population": "combined_tier", "tier": tier}
            item.update(_metrics(rows))
            summary_rows.append(item)
    return summary_rows, overlap_rows


def _fmt_pct(value: Any) -> str:
    num = _f(value)
    return "n/a" if num is None else f"{num * 100.0:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    num = _f(value)
    return "n/a" if num is None else f"{num:.{digits}f}"


def _write_report(path: Path, joined: list[dict[str, Any]], summary_rows: list[dict[str, Any]], latest: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    def row_for(window: str, population: str, tier: str = "") -> dict[str, Any]:
        for row in summary_rows:
            if row.get("window") == window and row.get("population") == population and str(row.get("tier") or "") == tier:
                return row
        return {}

    watch_alt = [row for row in joined if row.get("watch_alternate_overlap")]
    alt_layer_a = [row for row in joined if row.get("alternate_layer") == "alternate_layer_a_d7_d15_starter"]
    alt_layer_a_main = [row for row in alt_layer_a if row.get("manual_population") == "overlap_main_alternate"]
    lines = [
        "# O1.5 Manual Unified Board Universe",
        "",
        f"- Generated at: `{_now()}`",
        f"- Latest completed slate: `{latest or 'n/a'}`",
        "- Label: Manual o1.5 decision universe.",
        "- Includes main o1.5 boards plus alternate discovery.",
        "- Alternate discovery remains research-only / over-only / not production-safe; it is not production scoring, uploads, selection, or grading.",
        "",
        "## Population Summary",
        "",
        "| window | population | rows | matched | resolved | W-L-P | WR | ROI | units | avg odds |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for window in WINDOWS:
        for population in ["main_only", "alternate_only", "overlap_main_alternate", "watch_alternate_overlap", "all_unified_manual_o15"]:
            row = row_for(window, population)
            if not row:
                continue
            lines.append(
                f"| {window} | `{population}` | `{row.get('rows')}` | `{row.get('matched')}` | `{row.get('resolved')}` | "
                f"`{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | `{_fmt_pct(row.get('wr'))}` | "
                f"`{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('units'))}` | `{_fmt_num(row.get('avg_odds'))}` |"
            )
    lines.extend(
        [
            "",
            "## Specific Overlap Answers",
            "",
            f"- Watch candidates also appearing in alternate discovery: `{len(watch_alt)}` rows.",
            f"- Alternate Layer A candidates also appearing in main boards: `{len(alt_layer_a_main)}` of `{len(alt_layer_a)}` rows.",
            "- Use `o15_manual_unified_board_universe_rows.csv` for row-level source flags.",
            "",
            "## Tier Performance In Unified Manual Universe",
            "",
            "| window | tier | rows | matched | resolved | W-L-P | WR | ROI | units |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    focus = ["A/A", "A/B", "A/D", "A/U", "B/A", "C/A", "C/U", "C/D"]
    for window in WINDOWS:
        for tier in focus:
            row = row_for(window, "combined_tier", tier)
            if not row:
                continue
            lines.append(
                f"| {window} | `{tier}` | `{row.get('rows')}` | `{row.get('matched')}` | `{row.get('resolved')}` | "
                f"`{row.get('wins')}-{row.get('losses')}-{row.get('pushes')}` | `{_fmt_pct(row.get('wr'))}` | "
                f"`{_fmt_pct(row.get('roi'))}` | `{_fmt_num(row.get('units'))}` |"
            )
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "- Use this report for manual-review population accounting because it includes alternate discovery while preserving source flags.",
            "- Use `review_aid_performance_summary.json` for actual generated board artifact performance by individual board.",
            "- Use reconstructed all-market tier audit only for research/hypothesis generation.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build unified manual o1.5 board universe including alternate discovery.")
    ap.add_argument("--review-aids-dir", default="artifacts/analysis/mlb/review_aids")
    ap.add_argument("--reconcile-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/review_aids/performance")
    args = ap.parse_args()

    review_dir = ROOT / args.review_aids_dir
    reconcile_root = ROOT / args.reconcile_root
    lanes_root = ROOT / args.lanes_root
    out_dir = ROOT / args.out_dir

    board_rows = _load_o15_board_rows(review_dir, lanes_root)
    merged = _merge_rows(board_rows)
    joined, latest = _join_to_reconcile(merged, reconcile_root)
    summary_rows, overlap_rows = _summary(joined, latest)

    rows_csv = out_dir / "o15_manual_unified_board_universe_rows.csv"
    summary_csv = out_dir / "o15_manual_unified_board_universe_summary.csv"
    overlap_csv = out_dir / "o15_manual_unified_board_universe_overlap.csv"
    report_md = out_dir / "o15_manual_unified_board_universe.md"
    _write_csv(rows_csv, joined)
    _write_csv(summary_csv, summary_rows)
    _write_csv(overlap_csv, overlap_rows)
    _write_report(report_md, joined, summary_rows, latest)

    print(
        json.dumps(
            {
                "status": "ok",
                "generated_at": _now(),
                "latest_completed_slate": latest,
                "board_rows_loaded": len(board_rows),
                "unified_rows": len(joined),
                "outputs": {
                    "report_md": _rel(report_md),
                    "rows_csv": _rel(rows_csv),
                    "summary_csv": _rel(summary_csv),
                    "overlap_csv": _rel(overlap_csv),
                },
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
