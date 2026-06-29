#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())
DEFAULT_MAP = Path("artifacts/analysis/mlb/review_aids/offensive_environment_v1_1_starter_identity_map_2026-06-29.csv")
DEFAULT_ODDS_HISTORY = Path("backend/mlb/exports/odds_history")
DEFAULT_OUT_DIR = Path("artifacts/analysis/mlb/review_aids")
PITCHER_PROP_TYPES = {
    "hits_allowed",
    "strikeouts_pitching",
    "outs_recorded",
    "walks_allowed",
    "earned_runs",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _read_csv(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists() or path.stat().st_size == 0:
        return [], []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader], list(reader.fieldnames or [])


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


def _nonblank(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text and text.lower() not in {"nan", "none", "null"})


def _i(value: Any) -> int | None:
    try:
        if not _nonblank(value):
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def _team(value: Any) -> str:
    text = str(value or "").strip().upper()
    aliases = {
        "AZ": "ARI",
        "CHW": "CWS",
        "KCR": "KC",
        "OAK": "ATH",
        "SDP": "SD",
        "SFG": "SF",
        "TBR": "TB",
        "WSN": "WSH",
        "NYA": "NYY",
        "NYN": "NYM",
        "LA": "LAD",
    }
    return aliases.get(text, text)


def _norm_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


class SnapshotIndex:
    def __init__(self) -> None:
        self.files: list[Path] = []
        self.game_rows: dict[int, dict[str, Any]] = {}
        self.team_games: dict[tuple[str, str], set[int]] = defaultdict(set)
        self.player_game: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
        self.name_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.starters: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)


def _snapshot_files(date_root: Path) -> list[Path]:
    if not date_root.exists():
        return []
    files = []
    for pattern in ("mlb_predictions_wide_calibrated*.csv", "mlb_slate_output*.csv"):
        files.extend(date_root.glob(pattern))
    return sorted(set(files))


def _build_snapshot_index(odds_history: Path, date_text: str) -> SnapshotIndex:
    idx = SnapshotIndex()
    root = odds_history / date_text
    idx.files = _snapshot_files(root)
    seen_context: set[tuple[str, int, str, int | None, str, str]] = set()
    seen_starters: set[tuple[int, str, int, str]] = set()
    for path in idx.files:
        rows, _fields = _read_csv(path)
        source_type = "wide_all_snapshots" if "predictions_wide" in path.name else "slate_all_snapshots"
        for row in rows:
            game_id = _i(row.get("game_id"))
            if game_id is None:
                continue
            home = _team(row.get("home_team_code"))
            away = _team(row.get("away_team_code"))
            if home and away:
                idx.game_rows.setdefault(int(game_id), {"game_id": int(game_id), "home_team": home, "away_team": away})
                idx.team_games[(home, away)].add(int(game_id))
                idx.team_games[(away, home)].add(int(game_id))
            player_id = _i(row.get("player_id"))
            player_name = str(row.get("player_name") or row.get("player") or "").strip()
            team = _team(row.get("team") or row.get("team_code"))
            opponent = _team(row.get("opponent"))
            prop_type = str(row.get("prop_type") or "").strip().lower()
            if player_name and team and opponent:
                context = {
                    "game_id": int(game_id),
                    "player_id": player_id or "",
                    "player_name": player_name,
                    "team": team,
                    "opponent": opponent,
                    "source_path": _rel(path),
                    "source_type": source_type,
                    "prop_type": prop_type,
                }
                key = (_norm_name(player_name), int(game_id), team, player_id, opponent, source_type)
                if key not in seen_context:
                    seen_context.add(key)
                    idx.name_game[_norm_name(player_name)].append(context)
                    if player_id is not None:
                        idx.player_game[(int(game_id), int(player_id))].append(context)
            if prop_type in PITCHER_PROP_TYPES and player_id is not None and player_name and team and opponent:
                starter = {
                    "game_id": int(game_id),
                    "starter_id": int(player_id),
                    "starter_name": player_name,
                    "pitcher_team": team,
                    "offense_team": opponent,
                    "starter_source": source_type,
                    "starter_prop_type": prop_type,
                    "source_path": _rel(path),
                }
                key = (int(game_id), team, int(player_id), prop_type)
                if key not in seen_starters:
                    seen_starters.add(key)
                    idx.starters[(int(game_id), team)].append(starter)
    return idx


def _choose_starter(candidates: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str]:
    if not candidates:
        return None, "no_starter_in_all_snapshots"
    by_id: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        sid = _i(row.get("starter_id"))
        if sid is not None:
            by_id[sid].append(row)
    if len(by_id) != 1:
        return None, "starter_conflict_across_all_snapshots"
    rows = next(iter(by_id.values()))
    hits_allowed = [row for row in rows if row.get("starter_prop_type") == "hits_allowed"]
    return (hits_allowed or rows)[0], "all_snapshots_exact_game_pitcher_team"


def _recover_with_all_snapshots(row: dict[str, Any], idx: SnapshotIndex) -> dict[str, Any]:
    player_id = _i(row.get("player_id"))
    player_name = str(row.get("player_name") or "").strip()
    game_id = _i(row.get("resolved_game_id"))
    offense_team = _team(row.get("resolved_offense_team") or row.get("input_team"))
    opponent_team = _team(row.get("resolved_pitcher_team") or row.get("input_opponent"))
    method = ""
    evidence_path = ""
    evidence_note = ""

    if game_id is None and player_id is not None:
        contexts = []
        for (gid, pid), rows in idx.player_game.items():
            if pid == int(player_id):
                contexts.extend(rows)
        if offense_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("team")) == offense_team]
        if opponent_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("opponent")) == opponent_team]
        game_ids = {int(ctx["game_id"]) for ctx in contexts}
        if len(game_ids) == 1:
            ctx = contexts[0]
            game_id = next(iter(game_ids))
            offense_team = offense_team or _team(ctx.get("team"))
            opponent_team = opponent_team or _team(ctx.get("opponent"))
            method = "all_snapshots_player_id_unambiguous"
            evidence_path = str(ctx.get("source_path") or "")

    if game_id is None and player_name:
        contexts = list(idx.name_game.get(_norm_name(player_name), []))
        if offense_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("team")) == offense_team]
        if opponent_team:
            contexts = [ctx for ctx in contexts if _team(ctx.get("opponent")) == opponent_team]
        game_ids = {int(ctx["game_id"]) for ctx in contexts}
        team_pairs = {(_team(ctx.get("team")), _team(ctx.get("opponent"))) for ctx in contexts}
        team_pairs = {pair for pair in team_pairs if pair[0] and pair[1]}
        if len(game_ids) == 1 and len(team_pairs) == 1:
            ctx = contexts[0]
            game_id = next(iter(game_ids))
            offense_team, opponent_team = next(iter(team_pairs))
            method = "all_snapshots_player_name_team_game_unambiguous"
            evidence_path = str(ctx.get("source_path") or "")

    if game_id is not None and (not offense_team or not opponent_team) and player_name:
        contexts = [ctx for ctx in idx.name_game.get(_norm_name(player_name), []) if int(ctx.get("game_id") or 0) == int(game_id)]
        team_pairs = {(_team(ctx.get("team")), _team(ctx.get("opponent"))) for ctx in contexts}
        team_pairs = {pair for pair in team_pairs if pair[0] and pair[1]}
        if len(team_pairs) == 1:
            offense_team, opponent_team = next(iter(team_pairs))
            method = method or "all_snapshots_game_id_player_name_team_unambiguous"
            evidence_path = evidence_path or str(contexts[0].get("source_path") or "")

    if game_id is not None and offense_team and not opponent_team:
        game = idx.game_rows.get(int(game_id), {})
        home = _team(game.get("home_team"))
        away = _team(game.get("away_team"))
        if offense_team == home:
            opponent_team = away
        elif offense_team == away:
            opponent_team = home
    elif game_id is not None and opponent_team and not offense_team:
        game = idx.game_rows.get(int(game_id), {})
        home = _team(game.get("home_team"))
        away = _team(game.get("away_team"))
        if opponent_team == home:
            offense_team = away
        elif opponent_team == away:
            offense_team = home

    starter = None
    starter_method = ""
    if game_id is not None and opponent_team:
        starter, starter_method = _choose_starter(idx.starters.get((int(game_id), opponent_team), []))
    if starter:
        if not method:
            method = starter_method
        evidence_path = evidence_path or str(starter.get("source_path") or "")
        return {
            "recoverable_locally": "yes",
            "suggested_method": method,
            "suggested_game_id": game_id,
            "suggested_team": offense_team,
            "suggested_opponent": opponent_team,
            "suggested_starter_id": starter.get("starter_id") or "",
            "suggested_starter_name": starter.get("starter_name") or "",
            "suggested_source": starter.get("starter_source") or "",
            "evidence_path": evidence_path,
            "remaining_blocker": "",
        }
    if game_id is None:
        evidence_note = "no unambiguous game found in all timestamped wide/slate snapshots"
    elif not offense_team or not opponent_team:
        evidence_note = "game found but hitter team/opponent remains unavailable"
    elif starter_method == "starter_conflict_across_all_snapshots":
        evidence_note = "all timestamped snapshots disagree on opposing starter"
    else:
        evidence_note = "opposing starter not present in all timestamped wide/slate snapshots"
    return {
        "recoverable_locally": "no",
        "suggested_method": "",
        "suggested_game_id": game_id or "",
        "suggested_team": offense_team,
        "suggested_opponent": opponent_team,
        "suggested_starter_id": "",
        "suggested_starter_name": "",
        "suggested_source": "",
        "evidence_path": evidence_path,
        "remaining_blocker": evidence_note,
    }


def _missing_profile(row: dict[str, Any]) -> dict[str, str]:
    return {
        "has_player_id": "yes" if _nonblank(row.get("player_id")) else "no",
        "has_player_name": "yes" if _nonblank(row.get("player_name")) else "no",
        "has_input_team": "yes" if _nonblank(row.get("input_team")) else "no",
        "has_input_opponent": "yes" if _nonblank(row.get("input_opponent")) else "no",
        "has_resolved_game_id": "yes" if _nonblank(row.get("resolved_game_id")) else "no",
        "has_resolved_team": "yes" if _nonblank(row.get("resolved_offense_team")) else "no",
        "has_resolved_opponent": "yes" if _nonblank(row.get("resolved_pitcher_team")) else "no",
    }


def _write_report(path: Path, rows: list[dict[str, Any]], sample_rows: list[dict[str, Any]], inventory: list[dict[str, Any]]) -> None:
    total = len(rows)
    resolved = sum(1 for row in rows if row.get("starter_identity_status") == "resolved")
    unresolved = total - resolved
    reasons = Counter(row.get("unresolved_reason") or "resolved" for row in rows)
    locally_recoverable = Counter()
    by_reason_recoverable = Counter()
    for row in sample_rows:
        if row.get("recoverable_locally") == "yes":
            by_reason_recoverable[row.get("unresolved_reason") or ""] += 1
    for row in rows:
        if row.get("starter_identity_status") != "resolved":
            locally_recoverable[row.get("unresolved_reason") or ""] += 0
    estimated = {
        str(row.get("source_or_strategy")): row
        for row in inventory
        if row.get("source_or_strategy") == "all_timestamped_wide_slate_snapshots"
    }
    safe_recoverable = int(estimated.get("all_timestamped_wide_slate_snapshots", {}).get("safe_recoverable_rows") or 0)
    conflict_recoverable = int(estimated.get("all_timestamped_wide_slate_snapshots", {}).get("conflict_bucket_rows_with_candidate") or 0)
    revised_safe = resolved + safe_recoverable
    revised_with_conflicts = revised_safe + conflict_recoverable
    lines = [
        "# Offensive Environment v1.1 Starter Identity Gap Analysis",
        "",
        f"- Generated at: `{_now()}`",
        "- Window: `2026-05-13` through `2026-06-15`",
        "- Scope: investigation/enrichment only.",
        "- Environment component values written: `no`",
        "",
        "## Baseline",
        "",
        f"- Rows examined: `{total}`",
        f"- Rows resolved by current prepass: `{resolved}`",
        f"- Rows unresolved: `{unresolved}`",
        f"- Current resolution rate: `{(resolved / total * 100.0) if total else 0.0:.2f}%`",
        "",
        "## Unresolved Buckets",
        "",
        "| unresolved reason | rows | what is missing | local recoverability |",
        "|---|---:|---|---|",
    ]
    descriptions = {
        "no_opposing_starter_in_local_pitcher_context": "game/team/opponent are present, but latest local pitcher context has no opponent starter row",
        "missing_game_identity": "game_id, team, and opponent are usually blank; many rows are provider/manual rows with player name only",
        "missing_team_or_opponent": "game_id is often present, but hitter side/team and opponent are blank",
        "existing_starter_id_conflicts_with_local_context": "row already carries a starter id that disagrees with local pitcher context",
        "starter_conflict_across_sources": "local pitcher sources disagree on starter for the same game/team",
    }
    for reason, count in reasons.most_common():
        if reason == "resolved":
            continue
        local = "recoverable in part from timestamped snapshots" if reason in {
            "no_opposing_starter_in_local_pitcher_context",
            "missing_game_identity",
            "missing_team_or_opponent",
        } else "requires conflict policy before use"
        lines.append(f"| `{reason}` | `{count}` | {descriptions.get(reason, 'see sample CSV')} | {local} |")
    missing_fields_by_reason: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        reason = row.get("unresolved_reason") or ""
        if not reason:
            continue
        profile = _missing_profile(row)
        for field, present in profile.items():
            if present == "no":
                missing_fields_by_reason[reason][field.replace("has_", "missing_")] += 1
    lines.extend(
        [
            "",
            "## Missing Field Profile",
            "",
            "| unresolved reason | top missing fields |",
            "|---|---|",
        ]
    )
    for reason, _count in reasons.most_common():
        if reason == "resolved":
            continue
        pieces = [f"`{field}` `{count}`" for field, count in missing_fields_by_reason[reason].most_common()]
        lines.append(f"| `{reason}` | {', '.join(pieces) if pieces else 'none'} |")
    lines.extend(
        [
            "",
            "## Additional Local Source Found",
            "",
            "The current prepass indexes only the canonical latest `mlb_predictions_wide_calibrated.csv` and `mlb_slate_output.csv` for each date.",
            "The odds-history folders also contain timestamped intraday snapshots such as `mlb_predictions_wide_calibrated__local_daily_*.csv` and `mlb_slate_output__local_daily_*.csv`.",
            "Those snapshots preserve earlier player/game/starter context that the latest canonical file no longer carries.",
            "",
            "## Estimated Resolution Lift",
            "",
            f"- Safe locally recoverable rows from all timestamped wide/slate snapshots: `{safe_recoverable}`",
            f"- Current resolved + safe recoverable: `{revised_safe}` / `{total}` (`{(revised_safe / total * 100.0) if total else 0.0:.2f}%`)",
            f"- Conflict bucket rows with local candidates: `{conflict_recoverable}`",
            f"- If a separate conflict policy accepts those rows: `{revised_with_conflicts}` / `{total}` (`{(revised_with_conflicts / total * 100.0) if total else 0.0:.2f}%`)",
            "",
            "Conflict rows should not be auto-resolved by the reconstruction pass. They need an explicit starter authority rule or historical probable-starter source.",
            "",
            "## Source Inventory",
            "",
            "| source / strategy | available | expected contribution | limitation |",
            "|---|---|---:|---|",
        ]
    )
    for row in inventory:
        contribution = (
            row.get("safe_recoverable_rows")
            if _i(row.get("safe_recoverable_rows")) not in (None, 0)
            else row.get("estimated_contribution_rows")
        )
        lines.append(
            f"| `{row.get('source_or_strategy')}` | `{row.get('available')}` | "
            f"`{contribution or 0}` | {row.get('limitation') or ''} |"
        )
    lines.extend(
        [
            "",
            "## Conflict Examples",
            "",
            "The conflict buckets are small but important. They are not formula problems; they are role/source authority problems.",
            "Examples in the sample CSV include `existing_starter_id_conflicts_with_local_context` and `starter_conflict_across_sources` rows with a local snapshot candidate.",
            "",
            "## Recoverability Conclusion",
            "",
            "- Recoverable locally now: the three large non-conflict buckets can be materially improved by indexing all timestamped local wide/slate snapshots.",
            "- Requires local DB or a richer identity source: remaining name-only rows that do not appear in timestamped snapshots, especially rows without team/opponent.",
            "- Requires external MLB StatsAPI historical probable-starter calls or an explicit authority policy: conflict buckets and rows where no local starter source exists for the game/team.",
            "",
            "## Recommended Next Implementation Step",
            "",
            "Update the starter identity prepass to index all timestamped `mlb_predictions_wide_calibrated*.csv` and `mlb_slate_output*.csv` files per date.",
            "Use them only when game/team/opponent/starter identity is unambiguous, and preserve the exact source snapshot path in the map.",
            "Then rerun the prepass and run a second investigation on the remaining unresolved rows before writing any Environment v1.1 component values.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Investigate unresolved starter identity gaps for Environment v1.1 reconstruction.")
    ap.add_argument("--starter-map", type=Path, default=DEFAULT_MAP)
    ap.add_argument("--odds-history-dir", type=Path, default=DEFAULT_ODDS_HISTORY)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--date-label", default="2026-06-29")
    args = ap.parse_args()

    rows, _fields = _read_csv(args.starter_map)
    unresolved = [row for row in rows if row.get("starter_identity_status") != "resolved"]
    date_cache: dict[str, SnapshotIndex] = {}
    recovered_by_reason = Counter()
    conflict_candidates = Counter()
    sample_rows: list[dict[str, Any]] = []
    source_files_by_date: dict[str, int] = {}
    source_file_total = 0
    for row in unresolved:
        date_text = str(row.get("date") or "")[:10]
        idx = date_cache.get(date_text)
        if idx is None:
            idx = _build_snapshot_index(args.odds_history_dir, date_text)
            date_cache[date_text] = idx
            source_files_by_date[date_text] = len(idx.files)
            source_file_total += len(idx.files)
        recovery = _recover_with_all_snapshots(row, idx)
        reason = row.get("unresolved_reason") or ""
        if recovery.get("recoverable_locally") == "yes":
            if reason in {"existing_starter_id_conflicts_with_local_context", "starter_conflict_across_sources"}:
                conflict_candidates[reason] += 1
            else:
                recovered_by_reason[reason] += 1
        profiles = _missing_profile(row)
        include = False
        existing_for_reason = sum(1 for r in sample_rows if r.get("unresolved_reason") == reason)
        if existing_for_reason < 10:
            include = True
        if recovery.get("recoverable_locally") == "yes" and existing_for_reason < 20:
            include = True
        if include:
            sample_rows.append(
                {
                    "date": row.get("date") or "",
                    "target_artifact_type": row.get("target_artifact_type") or "",
                    "target_path": row.get("target_path") or "",
                    "target_row_index": row.get("target_row_index") or "",
                    "player_id": row.get("player_id") or "",
                    "player_name": row.get("player_name") or "",
                    "input_team": row.get("input_team") or "",
                    "input_opponent": row.get("input_opponent") or "",
                    "resolved_game_id_before": row.get("resolved_game_id") or "",
                    "resolved_team_before": row.get("resolved_offense_team") or "",
                    "resolved_opponent_before": row.get("resolved_pitcher_team") or "",
                    "unresolved_reason": reason,
                    **profiles,
                    **recovery,
                }
            )

    safe_recoverable = sum(recovered_by_reason.values())
    conflict_recoverable = sum(conflict_candidates.values())
    inventory = [
        {
            "source_or_strategy": "latest_canonical_wide_slate",
            "available": "yes",
            "estimated_contribution_rows": "6111",
            "safe_recoverable_rows": "0",
            "conflict_bucket_rows_with_candidate": "0",
            "limitation": "already used by current prepass; loses earlier intraday context after starter/market changes",
        },
        {
            "source_or_strategy": "all_timestamped_wide_slate_snapshots",
            "available": "yes",
            "estimated_contribution_rows": str(safe_recoverable + conflict_recoverable),
            "safe_recoverable_rows": str(safe_recoverable),
            "conflict_bucket_rows_with_candidate": str(conflict_recoverable),
            "limitation": "must require unambiguous game/team/starter; conflicts remain unsafe without authority policy",
        },
        {
            "source_or_strategy": "book_upload_csvs",
            "available": "yes",
            "estimated_contribution_rows": "unknown",
            "safe_recoverable_rows": "0",
            "conflict_bucket_rows_with_candidate": "0",
            "limitation": "contains home/away and selected player id but not hitter team side; useful supporting source, not enough alone",
        },
        {
            "source_or_strategy": "odds_latest_compatible_json",
            "available": "yes",
            "estimated_contribution_rows": "unknown",
            "safe_recoverable_rows": "0",
            "conflict_bucket_rows_with_candidate": "0",
            "limitation": "contains event/home/away/player market names but not player side/team for batter props",
        },
        {
            "source_or_strategy": "local_db_player_team_game_tables",
            "available": "not_verified_in_this_pass",
            "estimated_contribution_rows": "unknown",
            "safe_recoverable_rows": "0",
            "conflict_bucket_rows_with_candidate": "0",
            "limitation": "likely needed for remaining name-only/team-missing rows; requires separate DB-backed audit",
        },
        {
            "source_or_strategy": "historical_mlb_statsapi_probable_starters",
            "available": "external_not_used",
            "estimated_contribution_rows": "remaining_after_local_sources",
            "safe_recoverable_rows": "0",
            "conflict_bucket_rows_with_candidate": "0",
            "limitation": "needed only after local sources are exhausted or for starter conflict authority",
        },
    ]
    reason_summary = []
    reason_counts = Counter(row.get("unresolved_reason") or "" for row in unresolved)
    for reason, count in reason_counts.most_common():
        reason_summary.append(
            {
                "source_or_strategy": f"recoverable_{reason}",
                "available": "derived",
                "estimated_contribution_rows": str(recovered_by_reason.get(reason, 0) + conflict_candidates.get(reason, 0)),
                "safe_recoverable_rows": str(recovered_by_reason.get(reason, 0)),
                "conflict_bucket_rows_with_candidate": str(conflict_candidates.get(reason, 0)),
                "limitation": f"baseline unresolved rows={count}",
            }
        )
    inventory.extend(reason_summary)
    inventory.append(
        {
            "source_or_strategy": "timestamped_snapshot_file_count",
            "available": "yes",
            "estimated_contribution_rows": str(source_file_total),
            "safe_recoverable_rows": "0",
            "conflict_bucket_rows_with_candidate": "0",
            "limitation": f"files across {len(source_files_by_date)} dates; this is file count, not row recovery",
        }
    )

    sample_csv = args.out_dir / f"offensive_environment_v1_1_starter_identity_unresolved_sample_{args.date_label}.csv"
    inventory_csv = args.out_dir / f"offensive_environment_v1_1_starter_identity_source_inventory_{args.date_label}.csv"
    report_md = args.out_dir / f"offensive_environment_v1_1_starter_identity_gap_analysis_{args.date_label}.md"
    _write_csv(sample_csv, sample_rows)
    _write_csv(inventory_csv, inventory)
    _write_report(report_md, rows, sample_rows, inventory)
    print(
        {
            "report_md": _rel(report_md),
            "sample_csv": _rel(sample_csv),
            "inventory_csv": _rel(inventory_csv),
            "safe_recoverable": safe_recoverable,
            "conflict_candidates": conflict_recoverable,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
