#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts import build_mlb_o15_manual_unified_board_universe as manual


ROOT = manual.ROOT
OUT_DIR = Path("artifacts/analysis/mlb/expanded_o15_universe")
BVP_FIELDS = [
    "bvp_plate_appearances",
    "bvp_at_bats",
    "bvp_hits",
    "bvp_total_bases",
    "bvp_avg",
    "bvp_slg",
    "bvp_payload_present",
    "bvp_source",
]


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(value: Any) -> float | None:
    return manual._f(value)


def _i(value: Any) -> int | None:
    number = _f(value)
    return None if number is None else int(number)


def _norm_name(value: Any) -> str:
    return manual._norm_name(value)


def _team(value: Any) -> str:
    return manual._team(value)


def _rel(path: Path) -> str:
    return manual._rel(path)


def _date_text(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("board_date") or "")[:10]


def _in_range(row: dict[str, Any], start: date | None, end: date | None) -> bool:
    text = _date_text(row)
    try:
        d = datetime.strptime(text, "%Y-%m-%d").date()
    except Exception:
        return False
    if start and d < start:
        return False
    if end and d > end:
        return False
    return True


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


def _source_from_raw_board(row: dict[str, Any]) -> str:
    board = str(row.get("board") or "")
    if board == "o15_alternate_discovery":
        return "alternate_source"
    if board in {"o15_simple_filter", "o15_watch", "o15_layered"}:
        return "main_source"
    return "other"


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("resolved") is True]
    wins = sum(1 for row in resolved if row.get("win") is True)
    losses = sum(1 for row in resolved if row.get("loss") is True)
    pushes = sum(1 for row in resolved if row.get("push") is True)
    units = sum((_f(row.get("units")) or 0.0) for row in resolved)
    odds = [_f(row.get("expanded_price")) for row in rows if _f(row.get("expanded_price")) is not None]
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
        "avg_odds": sum(odds) / len(odds) if odds else None,
        "players": len({str(row.get("player_id") or row.get("player_name") or "") for row in rows if str(row.get("player_id") or row.get("player_name") or "").strip()}),
        "games": len({str(row.get("date") or "") + "|" + str(row.get("team") or "") + "|" + str(row.get("opponent") or "") for row in rows}),
        "books": len(_book_set(rows)),
    }


def _book_set(rows: list[dict[str, Any]]) -> set[str]:
    books: set[str] = set()
    for row in rows:
        for col in ("bookmaker_list", "book", "bookmaker_key"):
            for token in str(row.get(col) or "").split(","):
                token = token.strip()
                if token:
                    books.add(token)
    return books


def _fmt_pct(value: Any) -> str:
    num = _f(value)
    return "n/a" if num is None else f"{num * 100.0:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    num = _f(value)
    return "n/a" if num is None else f"{num:.{digits}f}"


def _key_from_expanded(row: dict[str, Any]) -> str:
    return str(row.get("manual_universe_key") or "").strip()


def _key_from_raw_board(row: dict[str, Any]) -> str:
    return manual._row_key(row)


def _expanded_row(row: dict[str, Any]) -> dict[str, Any]:
    from_alternate = bool(row.get("in_o15_alternate"))
    from_main = bool(row.get("in_o15_simple") or row.get("in_o15_watch") or row.get("in_o15_layered"))
    if from_main and from_alternate:
        source_bucket = "shared"
    elif from_alternate:
        source_bucket = "alternate_only"
    else:
        source_bucket = "main_only"
    book_list = str(row.get("bookmaker_list") or "").strip()
    return {
        **row,
        "expanded_universe": True,
        "from_main": from_main,
        "from_alternate": from_alternate,
        "from_both": from_main and from_alternate,
        "source_bucket": source_bucket,
        "expanded_price": _f(row.get("manual_price") or row.get("best_over_price") or row.get("market_price")),
        "book": book_list.split(",")[0] if book_list else "",
        "bookmaker_list": book_list,
        "market": "batter_hits_alternate" if from_alternate else "batter_hits",
        "production_board_is_source": from_main,
        "research_note": "Expanded O1.5 Universe research row; not production selection/upload/grading.",
    }


def _bvp_present(row: dict[str, Any]) -> bool:
    text = str(row.get("bvp_payload_present") or "").strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    return any(_f(row.get(col)) is not None for col in ("bvp_plate_appearances", "bvp_at_bats", "bvp_hits", "bvp_total_bases"))


def _bvp_sort_key(row: dict[str, Any]) -> tuple[int, float, float]:
    return (
        1 if _bvp_present(row) else 0,
        _f(row.get("bvp_plate_appearances")) or 0.0,
        _f(row.get("bvp_at_bats")) or 0.0,
    )


def _add_bvp_candidate(index: dict[str, dict[str, Any]], key: str, row: dict[str, Any], source_path: Path) -> None:
    if not key or key.endswith("||||"):
        return
    candidate = {field: row.get(field) for field in BVP_FIELDS}
    candidate["bvp_source_path"] = _rel(source_path)
    candidate["bvp_join_key"] = key
    current = index.get(key)
    if current is None or _bvp_sort_key(candidate) > _bvp_sort_key(current):
        index[key] = candidate


def _bvp_lookup_keys(row: dict[str, Any]) -> list[tuple[str, str]]:
    date_text = str(row.get("date") or "")[:10]
    player_id = _i(row.get("player_id"))
    name = _norm_name(row.get("player_name") or row.get("player"))
    team = _team(row.get("team"))
    opponent = _team(row.get("opponent"))
    keys: list[tuple[str, str]] = []
    if player_id is not None:
        keys.append(("date_player_team_opponent", "|".join((date_text, str(player_id), team, opponent))))
        keys.append(("date_player", "|".join((date_text, str(player_id)))))
    if name:
        keys.append(("date_name_team_opponent", "|".join((date_text, name, team, opponent))))
        keys.append(("date_name", "|".join((date_text, name))))
    return keys


def _load_bvp_index(slate_root: Path, dates: set[str]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for date_text in sorted(date for date in dates if date):
        path = slate_root / date_text / "mlb_slate_output.csv"
        for row in manual._read_csv(path):
            if str(row.get("prop_type") or "").strip().lower() != "hits":
                continue
            player_id = _i(row.get("player_id"))
            name = _norm_name(row.get("player_name"))
            team = _team(row.get("team"))
            opponent = _team(row.get("opponent"))
            slate_date = str(row.get("slate_date") or row.get("game_date") or date_text)[:10]
            if player_id is not None:
                _add_bvp_candidate(index, "|".join((slate_date, str(player_id), team, opponent)), row, path)
                _add_bvp_candidate(index, "|".join((slate_date, str(player_id))), row, path)
            if name:
                _add_bvp_candidate(index, "|".join((slate_date, name, team, opponent)), row, path)
                _add_bvp_candidate(index, "|".join((slate_date, name)), row, path)
    return index


def _hydrate_bvp(rows: list[dict[str, Any]], slate_root: Path) -> None:
    dates = {str(row.get("date") or "")[:10] for row in rows if str(row.get("date") or "")[:10]}
    index = _load_bvp_index(slate_root, dates)
    for row in rows:
        matched: dict[str, Any] | None = None
        mode = ""
        for mode_name, key in _bvp_lookup_keys(row):
            if key in index:
                matched = index[key]
                mode = mode_name
                break
        for field in BVP_FIELDS:
            row[field] = matched.get(field) if matched else ""
        row["bvp_payload_present"] = bool(_bvp_present(row)) if matched else False
        row["bvp_join_mode"] = mode or "missing"
        row["bvp_source_path"] = matched.get("bvp_source_path") if matched else ""
        row["bvp_source_date"] = str(row.get("date") or "")[:10] if matched else ""


def _summary_rows(rows: list[dict[str, Any]], latest: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    populations = {
        "main_total": lambda r: bool(r.get("from_main")),
        "alternate_total": lambda r: bool(r.get("from_alternate")),
        "main_only": lambda r: r.get("source_bucket") == "main_only",
        "alternate_only": lambda r: r.get("source_bucket") == "alternate_only",
        "shared": lambda r: r.get("source_bucket") == "shared",
        "expanded_total": lambda r: True,
    }
    for window in manual.WINDOWS:
        window_rows = [
            row
            for row in rows
            if window in manual._window_labels(str(row.get("date") or ""), latest)
            and (not latest or str(row.get("date") or "") <= latest)
        ]
        for population, predicate in populations.items():
            sub = [row for row in window_rows if predicate(row)]
            item = {"window": window, "population": population}
            item.update(_metrics(sub))
            out.append(item)
        for tier in sorted({str(row.get("combined_tier") or "missing") for row in window_rows}):
            sub = [row for row in window_rows if str(row.get("combined_tier") or "missing") == tier]
            item = {"window": window, "population": "combined_tier", "tier": tier}
            item.update(_metrics(sub))
            out.append(item)
    return out


def _daily_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_date[str(row.get("date") or "")].append(row)
    populations = {
        "main_total": lambda r: bool(r.get("from_main")),
        "alternate_total": lambda r: bool(r.get("from_alternate")),
        "main_only": lambda r: r.get("source_bucket") == "main_only",
        "alternate_only": lambda r: r.get("source_bucket") == "alternate_only",
        "shared": lambda r: r.get("source_bucket") == "shared",
        "expanded_total": lambda r: True,
    }
    for date_text in sorted(by_date):
        date_rows = by_date[date_text]
        for population, predicate in populations.items():
            sub = [row for row in date_rows if predicate(row)]
            item = {"date": date_text, "population": population}
            item.update(_metrics(sub))
            out.append(item)
    return out


def _overlap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = (
            str(row.get("player_id") or row.get("player_name") or ""),
            str(row.get("player_name") or ""),
            str(row.get("date") or ""),
        )
        grouped[key].append(row)
    out: list[dict[str, Any]] = []
    for (_player_key, player_name, date_text), group in sorted(grouped.items(), key=lambda kv: (kv[0][2], kv[0][1])):
        buckets = Counter(str(row.get("source_bucket") or "") for row in group)
        source_flags = sorted({str(row.get("source_list") or "") for row in group if str(row.get("source_list") or "").strip()})
        out.append(
            {
                "date": date_text,
                "player_id": next((row.get("player_id") for row in group if row.get("player_id")), ""),
                "player_name": player_name,
                "team": next((row.get("team") for row in group if row.get("team")), ""),
                "opponent": next((row.get("opponent") for row in group if row.get("opponent")), ""),
                "opportunity_count": len(group),
                "main_only_rows": buckets.get("main_only", 0),
                "alternate_only_rows": buckets.get("alternate_only", 0),
                "shared_rows": buckets.get("shared", 0),
                "overlap_class": "both" if buckets.get("shared", 0) else "alternate_only" if buckets.get("alternate_only", 0) else "main_only",
                "source_lists": ";".join(source_flags),
                "best_price": max((_f(row.get("expanded_price")) or -9999.0 for row in group), default=None),
                "combined_tiers": ",".join(sorted({str(row.get("combined_tier") or "") for row in group if str(row.get("combined_tier") or "")})),
            }
        )
    return out


def _coverage_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    main = [row for row in rows if row.get("from_main")]
    expanded = rows
    return {
        "main_rows": len(main),
        "expanded_rows": len(expanded),
        "row_multiplier": len(expanded) / len(main) if main else None,
        "main_players": _metrics(main)["players"],
        "expanded_players": _metrics(expanded)["players"],
        "main_games": _metrics(main)["games"],
        "expanded_games": _metrics(expanded)["games"],
        "main_books": _metrics(main)["books"],
        "expanded_books": _metrics(expanded)["books"],
    }


def _waterfall_rows(raw_board_rows: list[dict[str, Any]], expanded_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs = {
        "main_source": {
            "raw": lambda r: _source_from_raw_board(r) == "main_source",
            "expanded": lambda r: bool(r.get("from_main")),
        },
        "alternate_source": {
            "raw": lambda r: _source_from_raw_board(r) == "alternate_source",
            "expanded": lambda r: bool(r.get("from_alternate")),
        },
        "main_only": {
            "raw": lambda r: _source_from_raw_board(r) == "main_source",
            "expanded": lambda r: r.get("source_bucket") == "main_only",
        },
        "alternate_only": {
            "raw": lambda r: _source_from_raw_board(r) == "alternate_source",
            "expanded": lambda r: r.get("source_bucket") == "alternate_only",
        },
        "from_both_overlap": {
            "raw": lambda r: _source_from_raw_board(r) in {"main_source", "alternate_source"},
            "expanded": lambda r: bool(r.get("from_both")),
        },
        "expanded_total": {
            "raw": lambda r: _source_from_raw_board(r) in {"main_source", "alternate_source"},
            "expanded": lambda r: True,
        },
    }
    out: list[dict[str, Any]] = []
    for population, spec in specs.items():
        raw_rows = [row for row in raw_board_rows if spec["raw"](row)]
        candidate_rows = [row for row in expanded_rows if spec["expanded"](row)]
        raw_keys = {_key_from_raw_board(row) for row in raw_rows if _key_from_raw_board(row)}
        candidate_keys = {_key_from_expanded(row) for row in candidate_rows if _key_from_expanded(row)}
        metrics = _metrics(candidate_rows)
        out.append(
            {
                "population": population,
                "total_raw_rows_loaded": len(raw_rows),
                "total_candidate_rows_after_normalization": len(candidate_rows),
                "duplicate_rows_removed": max(0, len(raw_rows) - len(candidate_keys)),
                "unique_date_player_line_rows": len(candidate_keys),
                "matched_to_reconcile": metrics["matched"],
                "resolved_rows": metrics["resolved"],
                "wins": metrics["wins"],
                "losses": metrics["losses"],
                "pushes": metrics["pushes"],
                "unresolved": metrics["matched"] - metrics["resolved"],
                "unmatched": len(candidate_rows) - metrics["matched"],
                "roi": metrics["roi"],
                "units": metrics["units"],
                "average_odds": metrics["avg_odds"],
                "raw_unique_keys": len(raw_keys),
            }
        )
    return out


def _write_waterfall_report(path: Path, rows: list[dict[str, Any]]) -> None:
    by_pop = {str(row.get("population")): row for row in rows}
    lines = [
        "# Expanded O1.5 Universe Count Waterfall",
        "",
        "## Definitions",
        "",
        "- `main_source`: rows from the main production-derived O1.5 review boards: simple, watch, and layered.",
        "- `alternate_source`: rows from historical `batter_hits_alternate` discovery boards.",
        "- `expanded_total`: deduped union of main and alternate sources.",
        "- `main_only`: deduped opportunities present only in main-source boards.",
        "- `alternate_only`: deduped opportunities present only in alternate-source boards.",
        "- `from_both_overlap`: deduped opportunities present in both source families.",
        "",
        "## Dedupe / Price Semantics",
        "",
        "- Dedupe key: `date + player_id + line + side` when `player_id` exists.",
        "- Fallback dedupe key: `date + normalized player name + team + opponent + line + side`.",
        "- `prop_type` is implicit as `hits`; `side` is fixed to `over`; `line` is fixed to `1.5`.",
        "- Bookmaker is not part of the dedupe key. Multiple books/sources collapse into one research opportunity.",
        "- Source priority for duplicate context follows the board order already used by the manual universe: watch, layered, simple, alternate.",
        "- Price is selected from the retained row's available `manual_price`, `best_over_price`, or `market_price`; alternate rows generally use best over price across captured books.",
        "",
        "## Waterfall",
        "",
        "| population | raw rows | candidates | duplicates removed | unique keys | matched | resolved | W-L-P | unresolved | unmatched | ROI | units | avg odds |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for population in ("main_source", "alternate_source", "expanded_total", "main_only", "alternate_only", "from_both_overlap"):
        row = by_pop.get(population, {})
        lines.append(
            f"| {population} | {row.get('total_raw_rows_loaded', 0)} | {row.get('total_candidate_rows_after_normalization', 0)} | "
            f"{row.get('duplicate_rows_removed', 0)} | {row.get('unique_date_player_line_rows', 0)} | "
            f"{row.get('matched_to_reconcile', 0)} | {row.get('resolved_rows', 0)} | "
            f"{row.get('wins', 0)}-{row.get('losses', 0)}-{row.get('pushes', 0)} | "
            f"{row.get('unresolved', 0)} | {row.get('unmatched', 0)} | {_fmt_pct(row.get('roi'))} | "
            f"{_fmt_num(row.get('units'))} | {_fmt_num(row.get('average_odds'))} |"
        )
    lines.extend(
        [
            "",
            "## Why The Counts Differ",
            "",
            "- `Expanded rows` is the number of deduped candidate opportunities, including unresolved and unmatched rows.",
            "- The W-L record uses only resolved rows after joining to reconcile outcomes.",
            "- `expanded_total` is not `main_source + alternate_source` because rows appearing in both sources are deduped into one opportunity.",
            "- The main source record is small because only main-source O1.5 board rows that matched and resolved in the current artifact history are counted in the W-L record.",
            "- The row multiplier compares deduped expanded candidate rows to deduped main-source candidate rows, not resolved rows.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_manifest(path: Path, rows: list[dict[str, Any]], summary: list[dict[str, Any]], latest: str, waterfall: list[dict[str, Any]]) -> None:
    coverage = _coverage_summary(rows)
    full = next((row for row in summary if row.get("window") == "full_history" and row.get("population") == "expanded_total"), {})
    main_total = next((row for row in summary if row.get("window") == "full_history" and row.get("population") == "main_total"), {})
    alt_total = next((row for row in summary if row.get("window") == "full_history" and row.get("population") == "alternate_total"), {})
    alt = next((row for row in summary if row.get("window") == "full_history" and row.get("population") == "alternate_only"), {})
    main = next((row for row in summary if row.get("window") == "full_history" and row.get("population") == "main_only"), {})
    shared = next((row for row in summary if row.get("window") == "full_history" and row.get("population") == "shared"), {})
    lines = [
        "# Expanded O1.5 Universe Manifest",
        "",
        "## Philosophy",
        "",
        "The Expanded O1.5 Universe is the canonical research universe for hits Over 1.5.",
        "It combines every legitimate O1.5 opportunity source we can safely observe, then treats production boards, alternate markets, BvP, PvB, Tier A, HRR, offensive heat, lineup clustering, bullpen path, overlap, and future signals as annotations on the same candidate universe.",
        "",
        "This is not a production board. It does not replace production scoring, selectors, uploads, thresholds, grading, or matching.",
        "",
        "The production board is now one source inside this research universe, not the boundary of research.",
        "",
        "## Current Sources",
        "",
        "- Main production O1.5 universe: simple/watch/layered o1.5 board artifacts.",
        "- Alternate historical O1.5 universe: `batter_hits_alternate` historical source-derived alternate boards.",
        "",
        "## Current Range",
        "",
        f"- Latest completed slate used for resolved performance: `{latest or 'n/a'}`",
        f"- Generated at: `{_now()}`",
        "",
        "## Completed-Date Performance Summary",
        "",
        "This table is limited to candidate dates on or before the latest completed slate. It excludes current-slate candidates that cannot yet have outcomes.",
        "",
        "| population | rows | resolved | W-L-P | WR | ROI | avg odds |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for label, row in (
        ("main_total", main_total),
        ("alternate_total", alt_total),
        ("main_only_exclusive", main),
        ("alternate_only_exclusive", alt),
        ("shared", shared),
        ("expanded_total", full),
    ):
        lines.append(
            f"| {label} | {row.get('rows', 0)} | {row.get('resolved', 0)} | "
            f"{row.get('wins', 0)}-{row.get('losses', 0)}-{row.get('pushes', 0)} | "
            f"{_fmt_pct(row.get('wr'))} | {_fmt_pct(row.get('roi'))} | {_fmt_num(row.get('avg_odds'))} |"
        )
    lines.extend(
        [
            "",
            "## How To Read These Numbers",
            "",
            "- `rows` means deduped candidate opportunities in the Expanded O1.5 research universe, not graded bets.",
            "- The completed-date performance table excludes current-slate rows after the latest completed slate.",
            "- The coverage section includes all generated candidate rows in the current artifact set, including current slate.",
            "- `resolved` means the candidate joined to completed reconcile output and has a win/loss/push result.",
            "- ROI and W-L records are calculated only from resolved rows.",
            "- `main_total` means every deduped opportunity that appeared in any main production-derived O1.5 board.",
            "- `alternate_total` means every deduped opportunity that appeared in historical alternate O1.5 discovery.",
            "- `main_only_exclusive` and `alternate_only_exclusive` remove shared rows.",
            "- `shared` means the same deduped date/player/line opportunity appeared in both main and alternate sources.",
            "- `expanded_total` dedupes source overlap, so it is not the arithmetic sum of `main_total + alternate_total`.",
            "- Bookmaker is collapsed for universe identity; price is retained as research context and generally uses the retained/best available over price.",
            "- See `expanded_o15_universe_count_waterfall.md` for raw rows, dedupe, matched, resolved, unmatched, and unresolved accounting.",
            "",
            "## Coverage",
            "",
            f"- Main rows: `{coverage['main_rows']}`",
            f"- Expanded rows: `{coverage['expanded_rows']}`",
            f"- Row multiplier: `{_fmt_num(coverage['row_multiplier'])}x`",
            f"- Main players: `{coverage['main_players']}`",
            f"- Expanded players: `{coverage['expanded_players']}`",
            f"- Main games: `{coverage['main_games']}`",
            f"- Expanded games: `{coverage['expanded_games']}`",
            f"- Main books: `{coverage['main_books']}`",
            f"- Expanded books: `{coverage['expanded_books']}`",
            "",
            "## Research Note",
            "",
            "Research will now begin from the Expanded O1.5 Universe. Future features should be added as annotations to this universe rather than as separate side boards unless there is a strong reason to keep them isolated.",
            "",
            "Expanded O1.5 current positive ROI is best-price sensitive; BetOnline-only performance is separately tracked in `expanded_o15_universe_betonline_audit.md`.",
            "",
            "## Context Hydration Health",
            "",
            "Expanded O1.5 context hydration is expected to run daily through `make mlb-expanded-o15-universe`, followed by `make mlb-expanded-o15-context-health DATE=<DATE>`.",
            "",
            "Current health gates:",
            "",
            "- `game_id >= 95%`",
            "- `game_time >= 95%`",
            "- `time_of_day_bucket >= 95%`",
            "- `game_day_of_week >= 95%`",
            "- `is_home >= 80%`",
            "- `team offense context >= 80%`",
            "- `same-game cluster context >= 80%`",
            "- `rest context >= 75%`",
            "",
            "BvP payload coverage is reported but not fail-gated. Park/venue and batting-order/lineup-slot are currently documented as source-unavailable and are not fail-gated.",
            "",
            "Canonical `time_of_day_bucket` definitions are ET-based and mutually exclusive: `morning` = 00:00-11:59 ET, `afternoon` = 12:00-15:59 ET, `evening` = 16:00-19:59 ET, and `late` = 20:00-23:59 ET. Expanded O1.5, slate output, review boards, and reconcile should use the shared MLB helper for this field.",
            "",
            "`team_expected_hits_allowed` is currently a context signal for player-prop research, not a direct team-hits wagering lane. A direct team-hits prop lane would require separate market capture, reconcile/outcome tracking, and validation.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the Expanded O1.5 Universe research layer.")
    ap.add_argument("--date", default="")
    ap.add_argument("--date-from", default="")
    ap.add_argument("--date-to", default="")
    ap.add_argument("--review-aids-dir", default="artifacts/analysis/mlb/review_aids")
    ap.add_argument("--reconcile-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--lanes-root", default="backend/mlb/exports/model_v2/lanes")
    ap.add_argument("--slate-history-root", default="backend/mlb/exports/odds_history")
    ap.add_argument("--out-dir", default=str(OUT_DIR))
    args = ap.parse_args()

    if args.date and not args.date_from and not args.date_to:
        args.date_from = args.date
        args.date_to = args.date
    start = _parse_date(args.date_from)
    end = _parse_date(args.date_to)

    board_rows = manual._load_o15_board_rows(ROOT / args.review_aids_dir, ROOT / args.lanes_root)
    merged = manual._merge_rows(board_rows)
    filtered = [row for row in merged if _in_range(row, start, end)] if (start or end) else merged
    expanded = [_expanded_row(row) for row in filtered]
    _hydrate_bvp(expanded, ROOT / args.slate_history_root)
    joined, latest = manual._join_to_reconcile(expanded, ROOT / args.reconcile_root)
    summary = _summary_rows(joined, latest)
    daily = _daily_summary(joined)
    overlap = _overlap_rows(joined)
    raw_board_filtered = [row for row in board_rows if _in_range(row, start, end)] if (start or end) else board_rows
    waterfall = _waterfall_rows(raw_board_filtered, joined)

    out_dir = ROOT / args.out_dir
    manifest = out_dir / "expanded_o15_universe_manifest.md"
    rows_csv = out_dir / "expanded_o15_universe_rows.csv"
    summary_csv = out_dir / "expanded_o15_universe_summary.csv"
    daily_csv = out_dir / "expanded_o15_universe_daily_summary.csv"
    overlap_csv = out_dir / "expanded_o15_universe_overlap.csv"
    waterfall_csv = out_dir / "expanded_o15_universe_count_waterfall.csv"
    waterfall_md = out_dir / "expanded_o15_universe_count_waterfall.md"
    _write_csv(rows_csv, joined)
    _write_csv(summary_csv, summary)
    _write_csv(daily_csv, daily)
    _write_csv(overlap_csv, overlap)
    _write_csv(waterfall_csv, waterfall)
    _write_waterfall_report(waterfall_md, waterfall)
    _write_manifest(manifest, joined, summary, latest, waterfall)

    print(
        json.dumps(
            {
                "status": "ok",
                "expanded_rows": len(joined),
                "latest_completed_slate": latest,
                "outputs": {
                    "manifest": str(manifest.relative_to(ROOT)),
                    "summary": str(summary_csv.relative_to(ROOT)),
                    "daily_summary": str(daily_csv.relative_to(ROOT)),
                    "overlap": str(overlap_csv.relative_to(ROOT)),
                    "waterfall": str(waterfall_csv.relative_to(ROOT)),
                    "waterfall_md": str(waterfall_md.relative_to(ROOT)),
                    "rows": str(rows_csv.relative_to(ROOT)),
                },
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
