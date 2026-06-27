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

from backend.mlb.scripts import run_mlb_hits_15_tier_backtest as tier_backtest


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
WINDOWS = ("full_history", "last_30", "last_14", "last_7")
FOCUS_TIERS = ("A/A", "A/B", "A/D", "A/U", "B/A", "C/A")

BOARD_SPECS = {
    "o15_simple_filter": {
        "label": "O1.5 Simple Filter",
        "pattern": "hits_o15_simple_filter_*.csv",
        "discovery_only": False,
        "price_col": "market_price",
    },
    "o15_watch": {
        "label": "O1.5 Watch Candidates",
        "pattern": "hits_o15_watch_candidates_*.csv",
        "discovery_only": False,
        "price_col": "market_price",
    },
    "o15_layered": {
        "label": "O1.5 Layered Candidates",
        "pattern": "hits_o15_layered_candidates_*.csv",
        "discovery_only": False,
        "price_col": "market_price",
    },
    "o15_alternate_discovery": {
        "label": "O1.5 Alternate Discovery",
        "pattern": "hits_o15_alternate_discovery_*.csv",
        "discovery_only": True,
        "price_col": "best_over_price",
    },
}


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


def _date_from_path(path: Path) -> str:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", path.name)
    if match:
        return match.group(1)
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", str(path))
    return match.group(1) if match else ""


def _line_key(value: Any) -> str:
    line = _f(value)
    return f"{line:.1f}" if line is not None else ""


def _identity(date_text: Any, player_id: Any, line: Any = 1.5, side: Any = "over") -> tuple[str, str, str, str]:
    pid = _i(player_id)
    return (str(date_text or "")[:10], str(pid or ""), _line_key(line), str(side or "").strip().lower())


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
    return labels


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    wins = sum(1 for row in rows if row.get("result") == "win")
    losses = sum(1 for row in rows if row.get("result") == "loss")
    pushes = sum(1 for row in rows if row.get("result") == "push")
    resolved = wins + losses + pushes
    units = sum(float(row.get("units") or 0.0) for row in rows if row.get("result") in {"win", "loss", "push"})
    return {
        "rows": len(rows),
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "wr": wins / (wins + losses) if wins + losses else None,
        "roi": units / resolved if resolved else None,
        "units": units,
    }


def _load_reconstructed_rows(execution_root: Path) -> list[dict[str, Any]]:
    rows = tier_backtest._load_reconcile_rows(execution_root)
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get("side") != "over":
            continue
        item = dict(row)
        tier_backtest._assign_tiers(item, "o15")
        item["source_population"] = "reconstructed_all_market"
        item["identity_key"] = "|".join(_identity(item.get("date"), item.get("player_id"), item.get("line"), item.get("side")))
        item["name_key"] = "|".join(
            (
                str(item.get("date") or "")[:10],
                _norm_name(item.get("player_name")),
                _line_key(item.get("line")),
                str(item.get("side") or "").strip().lower(),
            )
        )
        out.append(item)
    return out


def _load_board_rows(review_aids_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for board, spec in BOARD_SPECS.items():
        for path in sorted(review_aids_dir.glob(str(spec["pattern"]))):
            date_text = _date_from_path(path)
            for raw in _read_csv(path):
                side = str(raw.get("side") or "over").strip().lower()
                line = _line_key(raw.get("line") or 1.5)
                if side != "over" or line != "1.5":
                    continue
                date_value = str(raw.get("date") or date_text)[:10]
                player_id = _i(raw.get("player_id"))
                player_name = raw.get("player_name") or raw.get("player") or ""
                rows.append(
                    {
                        **raw,
                        "board": board,
                        "board_label": spec["label"],
                        "board_source_file": _rel(path),
                        "board_date": date_value,
                        "player_id": player_id if player_id is not None else "",
                        "player_name": player_name,
                        "line": line,
                        "side": side,
                        "board_price": _f(raw.get(str(spec["price_col"]))),
                        "combined_tier": str(raw.get("combined_tier") or ""),
                        "hitter_tier": str(raw.get("hitter_tier") or ""),
                        "pitcher_tier": str(raw.get("pitcher_tier") or ""),
                        "discovery_only": bool(spec["discovery_only"]),
                        "identity_key": "|".join(_identity(date_value, player_id, line, side)),
                        "name_key": "|".join((date_value, _norm_name(player_name), line, side)),
                    }
                )
    return rows


def _pick_board_union(rows: list[dict[str, Any]], *, include_discovery: bool) -> dict[str, dict[str, Any]]:
    priority = {
        "o15_watch": 0,
        "o15_layered": 1,
        "o15_simple_filter": 2,
        "o15_alternate_discovery": 3,
    }
    out: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda r: (priority.get(str(r.get("board")), 9), str(r.get("board_source_file")))):
        if bool(row.get("discovery_only")) and not include_discovery:
            continue
        key = str(row.get("identity_key") or "")
        if not key or "||" in key:
            key = str(row.get("name_key") or "")
        if not key:
            continue
        out.setdefault(key, row)
    return out


def _build_overlap_rows(
    reconstructed: list[dict[str, Any]],
    board_rows: list[dict[str, Any]],
    latest: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recon_by_key = {str(row.get("identity_key")): row for row in reconstructed if row.get("identity_key")}
    recon_by_name = {str(row.get("name_key")): row for row in reconstructed if row.get("name_key")}
    board_unions = {
        "actual_board_union_non_alternate": _pick_board_union(board_rows, include_discovery=False),
        "actual_board_union_with_alternate": _pick_board_union(board_rows, include_discovery=True),
    }
    board_by_board: dict[str, dict[str, dict[str, Any]]] = {}
    for board in sorted({str(row.get("board")) for row in board_rows}):
        board_by_board[f"actual_board_{board}"] = _pick_board_union(
            [row for row in board_rows if str(row.get("board")) == board],
            include_discovery=True,
        )
    populations = {**board_unions, **board_by_board}

    overlap_summary: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []

    def matched_recon(board_row: dict[str, Any]) -> tuple[str, dict[str, Any] | None, str]:
        key = str(board_row.get("identity_key") or "")
        if key and "||" not in key and key in recon_by_key:
            return key, recon_by_key[key], "date+player_id+line+side"
        name_key = str(board_row.get("name_key") or "")
        row = recon_by_name.get(name_key)
        if row is not None:
            return str(row.get("identity_key") or ""), row, "date+normalized_player_name+line+side"
        return "", None, "unmatched"

    for population_name, board_by_key in populations.items():
        for window in WINDOWS:
            recon_window = [
                row
                for row in reconstructed
                if window in _window_labels(str(row.get("date") or ""), latest)
                and str(row.get("combined_tier")) in FOCUS_TIERS
            ]
            board_window = [
                row
                for row in board_by_key.values()
                if window in _window_labels(str(row.get("board_date") or ""), latest)
                and str(row.get("combined_tier")) in FOCUS_TIERS
            ]
            board_match_info: dict[int, tuple[str, dict[str, Any] | None, str]] = {
                id(row): matched_recon(row) for row in board_window
            }
            for tier in FOCUS_TIERS:
                recon_tier = [row for row in recon_window if row.get("combined_tier") == tier]
                board_tier = [row for row in board_window if row.get("combined_tier") == tier]
                recon_keys = {str(row.get("identity_key")) for row in recon_tier}
                board_same_tier_matches: set[str] = set()
                board_only_rows: list[dict[str, Any]] = []
                board_only_perf_rows: list[dict[str, Any]] = []
                for board_row in board_tier:
                    recon_key, recon_row, _join_key = board_match_info.get(id(board_row), ("", None, "unmatched"))
                    if recon_row is not None and recon_row.get("combined_tier") == tier:
                        board_same_tier_matches.add(recon_key)
                    else:
                        board_only_rows.append(board_row)
                        if recon_row is not None:
                            board_only_perf_rows.append(recon_row)
                overlap_keys = recon_keys & board_same_tier_matches
                recon_only_keys = recon_keys - overlap_keys
                overlap_rows = [recon_by_key[key] for key in overlap_keys if key in recon_by_key]
                recon_only_rows = [recon_by_key[key] for key in recon_only_keys if key in recon_by_key]
                overlap_m = _metrics(overlap_rows)
                recon_only_m = _metrics(recon_only_rows)
                board_only_m = _metrics(board_only_perf_rows)
                overlap_summary.append(
                    {
                        "window": window,
                        "tier": tier,
                        "actual_population": population_name,
                        "reconstructed_rows": len(recon_tier),
                        "actual_board_rows": len(board_tier),
                        "overlap_count": len(overlap_keys),
                        "reconstructed_only_count": len(recon_only_keys),
                        "board_only_count": len(board_only_rows),
                        "overlap_pct_of_reconstructed": len(overlap_keys) / len(recon_tier) if recon_tier else None,
                        "overlap_pct_of_board": len(overlap_keys) / len(board_tier) if board_tier else None,
                        "overlap_resolved": overlap_m["resolved"],
                        "overlap_wins": overlap_m["wins"],
                        "overlap_losses": overlap_m["losses"],
                        "overlap_roi": overlap_m["roi"],
                        "reconstructed_only_resolved": recon_only_m["resolved"],
                        "reconstructed_only_wins": recon_only_m["wins"],
                        "reconstructed_only_losses": recon_only_m["losses"],
                        "reconstructed_only_roi": recon_only_m["roi"],
                        "board_only_resolved": board_only_m["resolved"],
                        "board_only_wins": board_only_m["wins"],
                        "board_only_losses": board_only_m["losses"],
                        "board_only_roi": board_only_m["roi"],
                    }
                )

                if window == "full_history":
                    for key in sorted(recon_only_keys):
                        row = recon_by_key.get(key, {})
                        mismatches.append(
                            {
                                "population": population_name,
                                "mismatch_type": "reconstructed_only",
                                "reason": "reconstructed_all_market_row_not_present_on_actual_board_population",
                                "date": row.get("date"),
                                "player_id": row.get("player_id"),
                                "player_name": row.get("player_name"),
                                "tier": row.get("combined_tier"),
                                "price": row.get("price"),
                                "result": row.get("result"),
                            }
                        )
                    for brow in sorted(board_only_rows, key=lambda r: (str(r.get("board_date")), str(r.get("player_name")))):
                        _recon_key, rrow, join_key = board_match_info.get(id(brow), ("", None, "unmatched"))
                        reason = "board_row_not_found_in_reconstructed_reconcile"
                        if rrow and rrow.get("combined_tier") != brow.get("combined_tier"):
                            reason = "tier_mismatch_between_board_and_reconstructed_enrichment"
                        mismatches.append(
                            {
                                "population": population_name,
                                "mismatch_type": "board_only",
                                "reason": reason,
                                "date": brow.get("board_date"),
                                "player_id": brow.get("player_id"),
                                "player_name": brow.get("player_name"),
                                "board": brow.get("board"),
                                "tier": brow.get("combined_tier"),
                                "reconstructed_tier": rrow.get("combined_tier") if rrow else "",
                                "join_key_used": join_key,
                                "board_price": brow.get("board_price"),
                                "board_source_file": brow.get("board_source_file"),
                            }
                        )
    return overlap_summary, mismatches


def _fmt_pct(value: Any) -> str:
    num = _f(value)
    return "n/a" if num is None else f"{num * 100.0:.2f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    num = _f(value)
    return "n/a" if num is None else f"{num:.{digits}f}"


def _write_reports(
    out_md: Path,
    out_recommendations: Path,
    *,
    overlap_rows: list[dict[str, Any]],
    mismatch_rows: list[dict[str, Any]],
    reconstructed_count: int,
    board_count: int,
    latest: str,
) -> None:
    non_alt = [
        row
        for row in overlap_rows
        if row["actual_population"] == "actual_board_union_non_alternate"
        and row["window"] in {"full_history", "last_30", "last_14", "last_7"}
    ]
    lines = [
        "# O1.5 Reconstructed Vs Actual Board Population Audit",
        "",
        f"- Generated at: `{_now()}`",
        f"- Latest reconstructed completed slate: `{latest}`",
        f"- Reconstructed o1.5 rows loaded: `{reconstructed_count}`",
        f"- Actual o1.5 board rows loaded: `{board_count}`",
        "- Scope: source-alignment/reporting only; no selector, threshold, upload, grading, or model changes.",
        "",
        "## Source Definitions",
        "",
        "- Reconstructed source: `make mlb-refresh-hits-15-tier-backtest` -> `backend/mlb/scripts/run_mlb_hits_15_tier_backtest.py`.",
        "- Reconstructed inputs: `artifacts/analysis/mlb/execution_vs_model/*/reconcile_rows.csv` plus DB enrichment for rolling/player/starter context.",
        "- Reconstructed row logic: all reconciled `hits`, `line=1.5`, both sides expanded, then o1.5 keeps `side=over` and assigns tiers from d7/d15 plus starter expected hits allowed.",
        "- Actual board sources: generated CSV artifacts under `artifacts/analysis/mlb/review_aids/`.",
        "- Alternate discovery is kept separate because it is discovery-only, alternate-market, and over-only.",
        "",
        "## Non-Alternate Board Union Overlap",
        "",
        "| window | tier | reconstructed rows | board rows | overlap | overlap % of recon | overlap % of board | overlap ROI | reconstructed-only ROI | board-only ROI |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in non_alt:
        lines.append(
            f"| {row['window']} | `{row['tier']}` | `{row['reconstructed_rows']}` | `{row['actual_board_rows']}` | "
            f"`{row['overlap_count']}` | `{_fmt_pct(row['overlap_pct_of_reconstructed'])}` | "
            f"`{_fmt_pct(row['overlap_pct_of_board'])}` | `{_fmt_pct(row['overlap_roi'])}` | "
            f"`{_fmt_pct(row['reconstructed_only_roi'])}` | `{_fmt_pct(row['board_only_roi'])}` |"
        )
    mismatch_counts = Counter(str(row.get("reason")) for row in mismatch_rows)
    lines.extend(["", "## Mismatch Reason Counts", ""])
    for reason, count in mismatch_counts.most_common():
        lines.append(f"- `{reason}`: `{count}`")
    lines.extend(
        [
            "",
            "## Answers",
            "",
            "- Reconstructed tier rows are not assumed equivalent to actual generated board rows.",
            "- Operational board decisions should use actual generated board artifact performance.",
            "- Reconstructed all-market tier audits remain useful for research, market-wide drift, and hypothesis generation.",
            "- Any Ops Brief or handoff language should label this source as `Reconstructed all-market tier audit` when it uses `hits_15_tier_backtest_summary.json`.",
        ]
    )
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    rec_lines = [
        "# O1.5 Reconstructed Vs Board Population Recommendations",
        "",
        "## Operational Recommendation",
        "",
        "Use actual generated board artifact performance for daily decision-making whenever the question is about what Jerry saw on a board.",
        "",
        "Use reconstructed all-market tier audits only for research questions such as broad market behavior, tier drift, or finding candidate signals beyond current board surfaces.",
        "",
        "## Reporting Labels",
        "",
        "- `hits_15_tier_backtest_summary.json`: label as `Reconstructed all-market hits 1.5 tier audit`.",
        "- `review_aid_performance_summary.json`: label as `Actual generated board artifact performance`.",
        "- `hits_o15_alternate_discovery_*`: label as `Discovery-only alternate-market board`; do not mix with production-safe boards.",
        "",
        "## Trust Boundary",
        "",
        "A reconstructed tier cliff is actionable as a research warning only until confirmed in actual board artifact performance.",
    ]
    out_recommendations.write_text("\n".join(rec_lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare reconstructed o1.5 tier backtests with actual daily board artifacts.")
    ap.add_argument("--execution-root", default="artifacts/analysis/mlb/execution_vs_model")
    ap.add_argument("--review-aids-dir", default="artifacts/analysis/mlb/review_aids")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/review_aids/performance")
    args = ap.parse_args()

    execution_root = ROOT / args.execution_root
    review_aids_dir = ROOT / args.review_aids_dir
    out_dir = ROOT / args.out_dir

    reconstructed = _load_reconstructed_rows(execution_root)
    latest = max([str(row.get("date") or "") for row in reconstructed], default="")
    board_rows = _load_board_rows(review_aids_dir)
    overlap_rows, mismatch_rows = _build_overlap_rows(reconstructed, board_rows, latest)

    overlap_csv = out_dir / "o15_reconstructed_vs_board_population_overlap.csv"
    mismatch_csv = out_dir / "o15_reconstructed_vs_board_population_mismatches.csv"
    report_md = out_dir / "o15_reconstructed_vs_board_population_audit.md"
    recommendations_md = out_dir / "o15_reconstructed_vs_board_population_recommendations.md"
    _write_csv(overlap_csv, overlap_rows)
    _write_csv(mismatch_csv, mismatch_rows)
    _write_reports(
        report_md,
        recommendations_md,
        overlap_rows=overlap_rows,
        mismatch_rows=mismatch_rows,
        reconstructed_count=len(reconstructed),
        board_count=len(board_rows),
        latest=latest,
    )
    payload = {
        "status": "ok",
        "generated_at": _now(),
        "latest_completed_slate": latest,
        "reconstructed_rows": len(reconstructed),
        "board_rows": len(board_rows),
        "outputs": {
            "report_md": _rel(report_md),
            "overlap_csv": _rel(overlap_csv),
            "mismatches_csv": _rel(mismatch_csv),
            "recommendations_md": _rel(recommendations_md),
        },
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
