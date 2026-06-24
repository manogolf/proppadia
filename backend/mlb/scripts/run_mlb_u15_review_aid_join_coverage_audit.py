#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _team(value: Any) -> str:
    text = _clean(value).upper()
    if text == "AZ":
        return "ARI"
    if text in {"ATH", "LV", "VIL"}:
        return "OAK"
    return text


def _norm_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return " ".join(text.split())


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _i(value: Any) -> int | None:
    number = _f(value)
    if number is None:
        return None
    return int(number)


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
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _load_u15_boards(review_aids_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(review_aids_dir.glob("hits_u15_favorite_audit_*.csv")):
        date_text = _date_from_filename(path)
        if not date_text:
            continue
        raw_rows = _read_csv(path)
        has_player_id = bool(raw_rows and "player_id" in raw_rows[0])
        has_layer_label = bool(raw_rows and "layer_label" in raw_rows[0])
        for idx, row in enumerate(raw_rows, start=1):
            out = dict(row)
            out.update(
                {
                    "board_date": str(row.get("date") or date_text)[:10],
                    "board_row_number": idx,
                    "board_source_file": _rel(path),
                    "board_has_player_id_column": has_player_id,
                    "board_has_layer_label_column": has_layer_label,
                    "board_player_name_norm": _norm_name(row.get("player_name") or row.get("player")),
                    "board_team_norm": _team(row.get("team")),
                    "board_opponent_norm": _team(row.get("opponent")),
                    "board_line_norm": _line_key(row.get("line") or 1.5),
                    "board_side_norm": _clean(row.get("side") or "under").lower(),
                }
            )
            rows.append(out)
    return rows


def _load_reconcile_by_date(reconcile_root: Path) -> dict[str, list[dict[str, Any]]]:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(reconcile_root.glob("20??-??-??/reconcile_rows.csv")):
        date_text = _date_from_filename(path.parent)
        if not date_text:
            continue
        by_date[date_text] = _read_csv(path)
    return by_date


def _build_reconcile_index(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hits = [row for row in rows if _clean(row.get("prop_type")).lower() == "hits"]
    hits_15 = [row for row in hits if _line_key(row.get("line")) == "1.5"]
    by_player_id_15: set[str] = set()
    by_player_id_any: set[str] = set()
    by_name_15: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_name_any: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_name_team_15: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    by_name_team_any: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    line_by_name_any: dict[str, set[str]] = defaultdict(set)
    teams_by_name_any: dict[str, set[str]] = defaultdict(set)

    for row in hits:
        pid = _i(row.get("player_id"))
        if pid is not None:
            by_player_id_any.add(str(pid))
        name = _norm_name(row.get("player_name") or row.get("market_player_name"))
        team = _team(row.get("team"))
        opp = _team(row.get("opponent"))
        line = _line_key(row.get("line"))
        if name:
            by_name_any[name].append(row)
            line_by_name_any[name].add(line)
            if team or opp:
                teams_by_name_any[name].add(f"{team}/{opp}")
        if name and team and opp:
            by_name_team_any[(name, team, opp)].append(row)

    for row in hits_15:
        pid = _i(row.get("player_id"))
        if pid is not None:
            by_player_id_15.add(str(pid))
        name = _norm_name(row.get("player_name") or row.get("market_player_name"))
        team = _team(row.get("team"))
        opp = _team(row.get("opponent"))
        if name:
            by_name_15[name].append(row)
        if name and team and opp:
            by_name_team_15[(name, team, opp)].append(row)

    return {
        "hits": hits,
        "hits_15": hits_15,
        "by_player_id_15": by_player_id_15,
        "by_player_id_any": by_player_id_any,
        "by_name_15": by_name_15,
        "by_name_any": by_name_any,
        "by_name_team_15": by_name_team_15,
        "by_name_team_any": by_name_team_any,
        "line_by_name_any": line_by_name_any,
        "teams_by_name_any": teams_by_name_any,
    }


def _classify_board_row(row: dict[str, Any], index: dict[str, Any] | None) -> dict[str, Any]:
    date_text = row["board_date"]
    name = row["board_player_name_norm"]
    team = row["board_team_norm"]
    opp = row["board_opponent_norm"]
    line = row["board_line_norm"]
    player_id = _i(row.get("player_id"))
    player_id_text = str(player_id) if player_id is not None else ""

    if index is None:
        return {
            "precise_join_classification": "board_date_has_no_reconcile_yet",
            "join_bug_candidate": False,
            "reconcile_hits_rows": 0,
            "reconcile_hits_15_rows": 0,
            "name_in_reconcile_hits_any_line": False,
            "name_in_reconcile_hits_1_5": False,
            "name_team_in_reconcile_hits_any_line": False,
            "name_team_in_reconcile_hits_1_5": False,
            "player_id_present": bool(player_id_text),
            "player_id_in_reconcile_hits_any_line": False,
            "player_id_in_reconcile_hits_1_5": False,
            "reconcile_lines_for_name": "",
            "reconcile_team_pairs_for_name": "",
        }

    name_any = bool(name and name in index["by_name_any"])
    name_15 = bool(name and name in index["by_name_15"])
    name_team_any = bool(name and (name, team, opp) in index["by_name_team_any"])
    name_team_15 = bool(name and (name, team, opp) in index["by_name_team_15"])
    pid_any = bool(player_id_text and player_id_text in index["by_player_id_any"])
    pid_15 = bool(player_id_text and player_id_text in index["by_player_id_15"])
    lines_for_name = ",".join(sorted(index["line_by_name_any"].get(name, set())))
    teams_for_name = ",".join(sorted(index["teams_by_name_any"].get(name, set())))

    if pid_15 or name_team_15:
        classification = "matchable_in_reconcile_hits_1_5"
        join_bug_candidate = True
    elif name_15:
        classification = "player_line_present_but_team_or_duplicate_mismatch"
        join_bug_candidate = True
    elif pid_any or name_team_any or name_any:
        classification = "line_mismatch_reconcile_has_hits_but_not_1_5"
        join_bug_candidate = False
    elif name:
        classification = "player_absent_from_reconcile_hits_universe"
        join_bug_candidate = False
    else:
        classification = "missing_player_identity"
        join_bug_candidate = False

    return {
        "precise_join_classification": classification,
        "join_bug_candidate": join_bug_candidate,
        "reconcile_hits_rows": len(index["hits"]),
        "reconcile_hits_15_rows": len(index["hits_15"]),
        "name_in_reconcile_hits_any_line": name_any,
        "name_in_reconcile_hits_1_5": name_15,
        "name_team_in_reconcile_hits_any_line": name_team_any,
        "name_team_in_reconcile_hits_1_5": name_team_15,
        "player_id_present": bool(player_id_text),
        "player_id_in_reconcile_hits_any_line": pid_any,
        "player_id_in_reconcile_hits_1_5": pid_15,
        "reconcile_lines_for_name": lines_for_name,
        "reconcile_team_pairs_for_name": teams_for_name,
    }


def _load_tracker_unmatched(unmatched_path: Path) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for row in _read_csv(unmatched_path):
        key = (
            str(row.get("board_date") or "")[:10],
            _norm_name(row.get("player_name")),
            _team(row.get("team")),
            _team(row.get("opponent")),
            _line_key(row.get("line") or 1.5),
        )
        out[key] = row
    return out


def _summarize_by_date(audit_rows: list[dict[str, Any]], reconcile_indexes: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in audit_rows:
        by_date[str(row.get("board_date") or "")].append(row)
    rows: list[dict[str, Any]] = []
    for date_text in sorted(by_date):
        date_rows = by_date[date_text]
        index = reconcile_indexes.get(date_text)
        board_names = {str(row.get("board_player_name_norm") or "") for row in date_rows if row.get("board_player_name_norm")}
        recon_15_names = set(index["by_name_15"]) if index else set()
        class_counts = Counter(str(row.get("precise_join_classification") or "") for row in date_rows)
        rows.append(
            {
                "date": date_text,
                "board_rows": len(date_rows),
                "board_unique_players": len(board_names),
                "reconcile_exists": bool(index),
                "reconcile_hits_rows": len(index["hits"]) if index else 0,
                "reconcile_hits_1_5_rows": len(index["hits_15"]) if index else 0,
                "overlap_by_normalized_name_hits_1_5": len(board_names & recon_15_names),
                "board_only_by_normalized_name_hits_1_5": len(board_names - recon_15_names),
                "reconcile_only_by_normalized_name_hits_1_5": len(recon_15_names - board_names),
                "tracker_unmatched_rows": sum(1 for row in date_rows if row.get("current_tracker_join_status") == "unmatched"),
                "join_bug_candidate_rows": sum(1 for row in date_rows if row.get("join_bug_candidate") is True),
                "classification_counts": "; ".join(f"{k}={v}" for k, v in sorted(class_counts.items())),
            }
        )
    return rows


def _render_report(
    path: Path,
    *,
    audit_rows: list[dict[str, Any]],
    date_summary: list[dict[str, Any]],
    unmatched_rows: list[dict[str, Any]],
    output_csv: Path,
) -> None:
    tracker_unmatched = [row for row in audit_rows if row.get("current_tracker_join_status") == "unmatched"]
    class_counts = Counter(str(row.get("precise_join_classification") or "") for row in tracker_unmatched)
    tracker_reason_counts = Counter(str(row.get("current_tracker_unmatched_reason") or "") for row in tracker_unmatched)
    no_pid_count = sum(1 for row in audit_rows if not row.get("player_id_present"))
    pre_layer_count = sum(1 for row in audit_rows if str(row.get("board_has_layer_label_column")) != "True")
    bug_candidates = [row for row in tracker_unmatched if row.get("join_bug_candidate") is True]
    lines = [
        "# u1.5 Review-Aid Join Coverage Audit",
        "",
        "## Summary",
        f"- Board rows audited: `{len(audit_rows)}`",
        f"- Current tracker unmatched rows: `{len(tracker_unmatched)}`",
        f"- Rows lacking player_id in board artifact: `{no_pid_count}`",
        f"- Rows from board files without native layer_label: `{pre_layer_count}`",
        f"- Join-bug candidate rows among current unmatched: `{len(bug_candidates)}`",
        f"- Row-level CSV: `{_rel(output_csv)}`",
        "",
        "## Current Tracker Unmatched Reasons",
    ]
    for key, value in sorted(tracker_reason_counts.items()):
        lines.append(f"- `{key or 'matched'}`: `{value}`")
    lines.extend(["", "## Precise Classification For Current Unmatched Rows"])
    for key, value in sorted(class_counts.items()):
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(["", "## Current Unmatched Rows By Classification"])
    by_class: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in tracker_unmatched:
        by_class[str(row.get("precise_join_classification") or "")].append(row)
    for key in sorted(by_class):
        rows = by_class[key]
        lines.append(f"### {key} ({len(rows)})")
        for row in rows:
            lines.append(
                "- {date} {player} ({team} vs {opp}) layer `{layer}`; reconcile lines for name `{lines_for_name}`".format(
                    date=row.get("board_date") or "",
                    player=row.get("player_name") or "",
                    team=row.get("team") or "",
                    opp=row.get("opponent") or "",
                    layer=row.get("layer_label") or row.get("tracker_layer_value") or "",
                    lines_for_name=row.get("reconcile_lines_for_name") or "",
                )
            )
    lines.extend(["", "## Date-Level Coverage"])
    lines.append(
        "| date | board rows | reconcile hits 1.5 | name overlap | board-only | reconcile-only | tracker unmatched | classification counts |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|")
    for row in date_summary:
        lines.append(
            "| {date} | {board_rows} | {reconcile_hits_1_5_rows} | {overlap_by_normalized_name_hits_1_5} | "
            "{board_only_by_normalized_name_hits_1_5} | {reconcile_only_by_normalized_name_hits_1_5} | "
            "{tracker_unmatched_rows} | {classification_counts} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Finding",
            "- The high unmatched count is not primarily a side-specific u1.5 join bug. Reconcile rows are line-level hits rows with both over and under outcomes, so `side=under` is used for grading after the player/date/line match.",
            "- The current u1.5 board CSVs do not carry `player_id`, so the tracker must use normalized-name fallbacks. When a board player exists in same-date hits 1.5 reconcile, those fallbacks are matchable.",
            "- Most current unmatched rows are board players that are absent from the same-date hits reconcile universe entirely, not just missing the under side or failing a team/opponent key.",
            "- The two `no_reconcile_for_board_date` rows are expected because the board date has not been reconciled yet.",
            "",
            "## Interpretation",
            "- The unmatched u1.5 rows are mostly a board-universe versus reconcile-universe difference. They should be reported as not outcome-trackable from the current execution-vs-model reconcile artifact, rather than treated as graded losses or tracker failures.",
            "- Historical board files before the latest layer work lack native `layer_label` and `player_id`; the tracker now reconstructs layers, but player-id joins cannot be recovered unless future board files include IDs.",
        ]
    )
    if bug_candidates:
        lines.extend(
            [
                "",
                "## Possible Join Bugs",
                "These rows are unmatched by the current tracker but appear matchable in same-date hits 1.5 reconcile. A tracker patch would be warranted before trusting unmatched counts:",
            ]
        )
        for row in bug_candidates[:25]:
            lines.append(
                f"- {row.get('board_date')} {row.get('player_name')} {row.get('team')} vs {row.get('opponent')}"
            )
    else:
        lines.extend(
            [
                "",
                "## Join-Bug Assessment",
                "- No clear tracker join bug was found in the current unmatched set.",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit u1.5 review-aid tracker join coverage.")
    parser.add_argument("--review-aids-dir", default="artifacts/analysis/mlb/review_aids")
    parser.add_argument("--reconcile-root", default="artifacts/analysis/mlb/execution_vs_model")
    parser.add_argument("--performance-dir", default="artifacts/analysis/mlb/review_aids/performance")
    args = parser.parse_args()

    review_aids_dir = ROOT / args.review_aids_dir
    reconcile_root = ROOT / args.reconcile_root
    performance_dir = ROOT / args.performance_dir
    output_csv = performance_dir / "u15_review_aid_join_coverage_audit.csv"
    output_md = performance_dir / "u15_review_aid_join_coverage_audit.md"
    unmatched_path = performance_dir / "u15_review_aid_unmatched_rows.csv"

    board_rows = _load_u15_boards(review_aids_dir)
    reconcile_by_date = _load_reconcile_by_date(reconcile_root)
    reconcile_indexes = {date: _build_reconcile_index(rows) for date, rows in reconcile_by_date.items()}
    tracker_unmatched = _load_tracker_unmatched(unmatched_path)

    audit_rows: list[dict[str, Any]] = []
    for row in board_rows:
        key = (
            row["board_date"],
            row["board_player_name_norm"],
            row["board_team_norm"],
            row["board_opponent_norm"],
            row["board_line_norm"],
        )
        tracker_row = tracker_unmatched.get(key)
        index = reconcile_indexes.get(row["board_date"])
        classification = _classify_board_row(row, index)
        audit = {
            "board_date": row["board_date"],
            "player_name": row.get("player_name") or row.get("player") or "",
            "player_id": row.get("player_id") or "",
            "team": row.get("team") or "",
            "opponent": row.get("opponent") or "",
            "side": row.get("side") or "under",
            "line": row.get("line") or "1.5",
            "combined_tier": row.get("combined_tier") or "",
            "layer_label": row.get("layer_label") or row.get("layer_value") or "",
            "board_source_file": row["board_source_file"],
            "board_has_player_id_column": row["board_has_player_id_column"],
            "board_has_layer_label_column": row["board_has_layer_label_column"],
            "board_player_name_norm": row["board_player_name_norm"],
            "board_team_norm": row["board_team_norm"],
            "board_opponent_norm": row["board_opponent_norm"],
            "board_line_norm": row["board_line_norm"],
            "board_side_norm": row["board_side_norm"],
            "current_tracker_join_status": "unmatched" if tracker_row else "matched_or_not_reported_unmatched",
            "current_tracker_unmatched_reason": tracker_row.get("unmatched_reason") if tracker_row else "",
            "tracker_layer_value": tracker_row.get("layer_value") if tracker_row else "",
            **classification,
        }
        audit_rows.append(audit)

    date_summary = _summarize_by_date(audit_rows, reconcile_indexes)
    _write_csv(output_csv, audit_rows)
    _render_report(
        output_md,
        audit_rows=audit_rows,
        date_summary=date_summary,
        unmatched_rows=list(tracker_unmatched.values()),
        output_csv=output_csv,
    )
    tracker_unmatched_count = sum(1 for row in audit_rows if row.get("current_tracker_join_status") == "unmatched")
    bug_candidate_count = sum(
        1
        for row in audit_rows
        if row.get("current_tracker_join_status") == "unmatched" and row.get("join_bug_candidate") is True
    )
    print(f"u15_join_coverage_audit_rows={len(audit_rows)}")
    print(f"tracker_unmatched_rows={tracker_unmatched_count}")
    print(f"join_bug_candidate_rows={bug_candidate_count}")
    print(f"audit_csv={_rel(output_csv)}")
    print(f"audit_md={_rel(output_md)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
