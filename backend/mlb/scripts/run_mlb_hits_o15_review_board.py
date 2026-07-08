#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from backend.mlb.identity import (
    GameIdentityInput,
    GameIdentityResolver,
    MarketIdentityInput,
    PlayerIdentityInput,
    PlayerIdentityResolver,
    canonical_team_code,
    resolve_market_identity,
)
from backend.mlb.ontology import ONTOLOGY_FIELDS, apply_o15_board_ontology
from backend.mlb.shared.time_utils_backend import get_time_of_day_bucket_et


IDENTITY_OUTPUT_COLUMNS = [
    "game_id",
    "canonical_player_id",
    "canonical_game_id",
    "canonical_team",
    "canonical_opponent",
    "canonical_market_key",
    "fallback_market_key",
    "identity_status",
    "identity_method",
    "fallback_used",
    "identity_warning",
]

O15_ONTOLOGY_OUTPUT_COLUMNS = [
    *ONTOLOGY_FIELDS,
]

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
]

OFFENSE_FACTOR_LINEAGE_COLUMNS = [
    "offense_context_as_of_date",
    "offense_window_excludes_eval_date",
    "offense_window_max_source_game_date",
    "local_team_hits_parity_status",
    "team_hits_mismatch_count",
    "team_hits_rescheduled_outside_window_count",
    "offense_factor_lineage_health_generated_at",
]

OUTPUT_COLUMNS = [
    "date",
    "player_id",
    "player_name",
    "team",
    "opponent",
    *IDENTITY_OUTPUT_COLUMNS,
    *O15_ONTOLOGY_OUTPUT_COLUMNS,
    "line",
    "side",
    "model_prob",
    "market_price",
    "selected_side_implied_probability",
    "d7_hits_rate",
    "d15_hits_rate",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "raw_d7_hits_calendar",
    "raw_d15_hits_calendar",
    *ENVIRONMENT_COMPONENT_COLUMNS,
    *OFFENSE_FACTOR_LINEAGE_COLUMNS,
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
    "opposing_starter",
    "opposing_starter_id",
    "starter_context_status",
    "starter_context_source",
    "starter_context_updated_at",
    "starter_context_unavailable_reason",
    "starter_min_start_policy_applied",
    "starter_starts_count",
    "starter_required_min_starts",
    "environment_artifact_timestamp",
    "environment_artifact_row_count",
    "environment_snapshot_policy",
    "hitter_tier",
    "pitcher_tier",
    "combined_tier",
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
]
WATCH_OUTPUT_COLUMNS = OUTPUT_COLUMNS + [
    "qc_score",
    "qc_selected_side",
    "qc_source_file",
    "ranking_score",
    "ranking_source_lane",
]
U15_BASE_OUTPUT_COLUMNS = [col for col in OUTPUT_COLUMNS if col not in set(O15_ONTOLOGY_OUTPUT_COLUMNS)]
U15_OUTPUT_COLUMNS = U15_BASE_OUTPUT_COLUMNS + [
    "qc_candidate",
    "qc_score",
    "qc_selected_side",
    "qc_source_file",
    "ranking_score",
    "ranking_source_lane",
    "d7_cold_candidate",
    "d15_cold_consistent_candidate",
    "tough_starter_candidate",
    "watch_candidate",
    "layer_label",
]
LAYERED_OUTPUT_COLUMNS = [
    "date",
    "player",
    "player_name",
    "player_id",
    "team",
    "opponent",
    *IDENTITY_OUTPUT_COLUMNS,
    *O15_ONTOLOGY_OUTPUT_COLUMNS,
    "line",
    "side",
    "market_price",
    "selected_side_implied_probability",
    "model_prob",
    "d7_hits_rate",
    "d15_hits_rate",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "raw_d7_hits_calendar",
    "raw_d15_hits_calendar",
    *ENVIRONMENT_COMPONENT_COLUMNS,
    *OFFENSE_FACTOR_LINEAGE_COLUMNS,
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
    "hitter_tier",
    "pitcher_tier",
    "combined_tier",
    "qc_candidate",
    "qc_score",
    "qc_selected_side",
    "ranking_score",
    "d7_hot_candidate",
    "d15_consistent_candidate",
    "favorable_starter_candidate",
    "watch_candidate",
    "layer_label",
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
    "opposing_starter",
    "opposing_starter_id",
    "starter_context_status",
    "starter_context_source",
    "starter_context_updated_at",
    "starter_context_unavailable_reason",
    "starter_min_start_policy_applied",
    "starter_starts_count",
    "starter_required_min_starts",
    "environment_artifact_timestamp",
    "environment_artifact_row_count",
    "environment_snapshot_policy",
]
ALTERNATE_DISCOVERY_COLUMNS = [
    "date",
    "player",
    "player_name",
    "player_id",
    "team",
    "opponent",
    *IDENTITY_OUTPUT_COLUMNS,
    *O15_ONTOLOGY_OUTPUT_COLUMNS,
    "bookmaker_list",
    "best_over_price",
    "selected_side_implied_probability",
    "line",
    "side",
    "d7_hits_rate",
    "d15_hits_rate",
    "d7_hits_runs_rbis",
    "d15_hits_runs_rbis",
    "d30_hits_runs_rbis",
    "raw_d7_hits_calendar",
    "raw_d15_hits_calendar",
    *ENVIRONMENT_COMPONENT_COLUMNS,
    *OFFENSE_FACTOR_LINEAGE_COLUMNS,
    "starter_expected_hits_allowed",
    "team_expected_hits_allowed",
    "hitter_tier",
    "pitcher_tier",
    "combined_tier",
    "d7_hot_candidate",
    "d15_consistent_candidate",
    "favorable_starter_candidate",
    "alternate_layer",
    "game_time",
    "time_of_day_bucket",
    "game_day_of_week",
    "opposing_starter",
    "opposing_starter_id",
    "starter_context_status",
    "starter_context_source",
    "starter_context_updated_at",
    "starter_context_unavailable_reason",
    "starter_min_start_policy_applied",
    "starter_starts_count",
    "starter_required_min_starts",
    "environment_artifact_timestamp",
    "environment_artifact_row_count",
    "environment_snapshot_policy",
]

HITTER_TIER_RANK = {"A": 0, "B": 1, "C": 2}
PITCHER_TIER_RANK = {"A": 0, "B": 1, "C": 2, "D": 3, "U": 4}
U15_COMBINED_TIER_ORDER = {
    "A/A": 0,
    "A/B": 1,
    "B/A": 2,
    "C/A": 3,
    "B/B": 4,
    "A/C": 5,
    "C/B": 6,
    "B/C": 7,
    "A/D": 8,
    "C/C": 9,
    "B/D": 10,
    "C/D": 11,
}


def _today_et() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def _parse_game_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _derive_time_of_day_bucket(game_time: Any) -> str:
    dt = _parse_game_time(game_time)
    if dt is None:
        return ""
    try:
        return get_time_of_day_bucket_et(dt)
    except Exception:
        return ""


def _derive_game_day_of_week(date_value: Any, game_time: Any = "") -> str:
    text = str(date_value or "").strip()
    if text:
        try:
            return datetime.fromisoformat(text[:10]).strftime("%A").lower()
        except Exception:
            pass
    dt = _parse_game_time(game_time)
    if dt is None:
        return ""
    return dt.strftime("%A").lower()


def _time_context(row: dict[str, Any], *, fallback_date: Any = "") -> dict[str, str]:
    game_time = str(row.get("game_time") or "").strip()
    bucket = str(row.get("time_of_day_bucket") or "").strip().lower()
    day = str(row.get("game_day_of_week") or "").strip().lower()
    if not bucket:
        bucket = _derive_time_of_day_bucket(game_time)
    if not day:
        day = _derive_game_day_of_week(row.get("game_date") or row.get("slate_date") or fallback_date, game_time)
    return {"game_time": game_time, "time_of_day_bucket": bucket, "game_day_of_week": day}


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except Exception:
        return None


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _american_implied_probability(value: Any) -> float | None:
    price = _f(value)
    if price is None:
        return None
    if price < 0:
        return abs(price) / (abs(price) + 100.0)
    return 100.0 / (price + 100.0)


def _clean_team(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"AZ"}:
        return "ARI"
    if raw in {"ATH", "LV", "VIL"}:
        return "OAK"
    return raw


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = columns or OUTPUT_COLUMNS
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in fieldnames})


def _canonical_team(value: Any) -> str:
    return canonical_team_code(value).canonical_team


def _id_text(value: Any) -> str:
    number = _f(value)
    if number is not None:
        return str(int(number))
    return str(value or "").strip()


def _slate_identity_indexes(slate_rows: list[dict[str, Any]], slate_date: str) -> tuple[dict[tuple[str, str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]], list[dict[str, Any]]]:
    by_player_team: dict[tuple[str, str, str], dict[str, Any]] = {}
    by_name_team: dict[tuple[str, str], dict[str, Any]] = {}
    refs: list[dict[str, Any]] = []
    for row in slate_rows:
        row_date = str(row.get("slate_date") or row.get("game_date") or "")[:10]
        if row_date != slate_date:
            continue
        player_id = _id_text(row.get("player_id"))
        name_key = _norm_player_name(row.get("player_name"))
        team = _canonical_team(row.get("team"))
        opponent = _canonical_team(row.get("opponent"))
        if player_id or row.get("player_name"):
            refs.append(
                {
                    "player_id": player_id,
                    "player_name": row.get("player_name") or "",
                    "team": team,
                    "opponent": opponent,
                }
            )
        if not team or not opponent:
            continue
        current_line = _line_key(row.get("line"))
        if player_id:
            key = (player_id, team, opponent)
            existing = by_player_team.get(key)
            if existing is None or current_line == "1.5":
                by_player_team[key] = row
        if name_key:
            key2 = (name_key, team)
            existing = by_name_team.get(key2)
            if existing is None or current_line == "1.5":
                by_name_team[key2] = row
    return by_player_team, by_name_team, refs


def _find_slate_identity_row(
    row: dict[str, Any],
    by_player_team: dict[tuple[str, str, str], dict[str, Any]],
    by_name_team: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    player_id = _id_text(row.get("player_id"))
    team = _canonical_team(row.get("team"))
    opponent = _canonical_team(row.get("opponent"))
    if player_id and team and opponent:
        found = by_player_team.get((player_id, team, opponent))
        if found:
            return found
    name_key = _norm_player_name(row.get("player_name") or row.get("player"))
    if name_key and team:
        found = by_name_team.get((name_key, team))
        if found:
            return found
    return {}


def _apply_canonical_identity(rows: list[dict[str, Any]], slate_rows: list[dict[str, Any]], slate_date: str) -> dict[str, Any]:
    by_player_team, by_name_team, refs = _slate_identity_indexes(slate_rows, slate_date)
    player_resolver = PlayerIdentityResolver(refs)
    game_resolver = GameIdentityResolver()
    status_counts: Counter[str] = Counter()
    method_counts: Counter[str] = Counter()
    rows_with_fallback = 0
    rows_unresolved = 0
    rows_ambiguous = 0
    rows_with_market_key = 0
    for row in rows:
        slate = _find_slate_identity_row(row, by_player_team, by_name_team)
        team = _canonical_team(row.get("team") or slate.get("team"))
        opponent = _canonical_team(row.get("opponent") or slate.get("opponent"))
        player_result = player_resolver.resolve(
            PlayerIdentityInput(
                player_id=row.get("player_id") or slate.get("player_id"),
                player_name=str(row.get("player_name") or row.get("player") or slate.get("player_name") or ""),
                team=team,
                opponent=opponent,
                game_id=row.get("game_id") or slate.get("game_id"),
            )
        )
        game_result = game_resolver.resolve(
            GameIdentityInput(
                date=slate_date,
                game_id=row.get("game_id") or slate.get("game_id"),
                home_team=slate.get("home_team_code") or "",
                away_team=slate.get("away_team_code") or "",
                team=team,
                opponent=opponent,
            )
        )
        canonical_player_id = player_result.canonical_player_id or _id_text(row.get("player_id"))
        canonical_game_id = game_result.canonical_game_id or _id_text(row.get("game_id") or slate.get("game_id"))
        market_result = resolve_market_identity(
            MarketIdentityInput(
                date=slate_date,
                game_id=canonical_game_id,
                player_id=canonical_player_id,
                player_name=str(row.get("player_name") or row.get("player") or ""),
                team=team,
                opponent=opponent,
                prop_type="hits",
                side=row.get("side"),
                line=row.get("line"),
            )
        )
        statuses = [player_result.identity_status, game_result.identity_status, market_result.identity_status]
        fallback_used = player_result.fallback_used or game_result.fallback_used or market_result.fallback_used
        warning_parts = [
            part
            for part in [
                player_result.ambiguity_reason,
                game_result.ambiguity_reason,
                market_result.ambiguity_reason,
            ]
            if part
        ]
        if any(status == "ambiguous" for status in statuses):
            identity_status = "ambiguous"
        elif any(status == "unresolved" for status in statuses):
            identity_status = "unresolved"
        elif fallback_used:
            identity_status = "fallback_identity"
        else:
            identity_status = "resolved_by_id"
        method = "+".join([player_result.identity_method, game_result.identity_method, market_result.identity_method])
        row["game_id"] = canonical_game_id or row.get("game_id") or slate.get("game_id") or ""
        row["canonical_player_id"] = canonical_player_id
        row["canonical_game_id"] = canonical_game_id
        row["canonical_team"] = team
        row["canonical_opponent"] = opponent
        row["canonical_market_key"] = market_result.canonical_market_key
        row["fallback_market_key"] = market_result.fallback_market_key
        row["identity_status"] = identity_status
        row["identity_method"] = method
        row["fallback_used"] = bool(fallback_used)
        row["identity_warning"] = ";".join(warning_parts)
        status_counts[identity_status] += 1
        method_counts[method] += 1
        rows_with_fallback += int(bool(fallback_used))
        rows_unresolved += int(identity_status == "unresolved")
        rows_ambiguous += int(identity_status == "ambiguous")
        rows_with_market_key += int(bool(market_result.canonical_market_key))
    return {
        "identity_rows": len(rows),
        "identity_rows_with_canonical_player_id": sum(1 for row in rows if row.get("canonical_player_id")),
        "identity_rows_with_canonical_game_id": sum(1 for row in rows if row.get("canonical_game_id")),
        "identity_rows_with_canonical_market_key": rows_with_market_key,
        "identity_rows_using_fallback": rows_with_fallback,
        "identity_rows_unresolved": rows_unresolved,
        "identity_rows_ambiguous": rows_ambiguous,
        "identity_status_counts": dict(status_counts),
        "identity_method_counts": dict(method_counts),
    }


def _identity_coverage(rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    total = len(rows)
    def count(col: str) -> int:
        return sum(1 for row in rows if str(row.get(col) or "").strip())

    fallback = sum(1 for row in rows if str(row.get("fallback_used") or "").strip().lower() in {"1", "true", "yes"})
    ambiguous = sum(1 for row in rows if str(row.get("identity_status") or "").strip() == "ambiguous")
    unresolved = sum(1 for row in rows if str(row.get("identity_status") or "").strip() == "unresolved")
    return {
        "label": label,
        "rows": total,
        "player_id_rows": count("player_id"),
        "game_id_rows": count("game_id") or count("canonical_game_id"),
        "canonical_player_id_rows": count("canonical_player_id"),
        "canonical_game_id_rows": count("canonical_game_id"),
        "canonical_market_key_rows": count("canonical_market_key"),
        "fallback_rows": fallback,
        "ambiguous_rows": ambiguous,
        "unresolved_rows": unresolved,
        "player_id_coverage_pct": round((count("player_id") / total * 100.0) if total else 0.0, 2),
        "game_id_coverage_pct": round(((count("game_id") or count("canonical_game_id")) / total * 100.0) if total else 0.0, 2),
        "canonical_market_key_coverage_pct": round((count("canonical_market_key") / total * 100.0) if total else 0.0, 2),
    }


def _append_identity_migration_report(
    *,
    out_dir: Path,
    date_text: str,
    board: str,
    out_csv: Path,
    before_rows: list[dict[str, Any]],
    after_rows: list[dict[str, Any]],
    identity_meta: dict[str, Any],
) -> None:
    report_csv = out_dir.parent / "identity" / f"review_board_identity_coverage_{date_text}.csv"
    report_md = out_dir.parent / "identity" / f"review_board_identity_migration_{date_text}.md"
    report_csv.parent.mkdir(parents=True, exist_ok=True)
    before = _identity_coverage(before_rows, "before")
    after = _identity_coverage(after_rows, "after")
    row = {
        "date": date_text,
        "board": board,
        "artifact": out_csv.as_posix(),
        "rows_before": before["rows"],
        "rows_after": after["rows"],
        "row_count_changed": before["rows"] != after["rows"],
        "player_id_coverage_before_pct": before["player_id_coverage_pct"],
        "player_id_coverage_after_pct": after["player_id_coverage_pct"],
        "game_id_coverage_before_pct": before["game_id_coverage_pct"],
        "game_id_coverage_after_pct": after["game_id_coverage_pct"],
        "canonical_market_key_coverage_before_pct": before["canonical_market_key_coverage_pct"],
        "canonical_market_key_coverage_after_pct": after["canonical_market_key_coverage_pct"],
        "fallback_rows_after": after["fallback_rows"],
        "ambiguous_rows_after": after["ambiguous_rows"],
        "unresolved_rows_after": after["unresolved_rows"],
        "identity_status_counts": json.dumps(identity_meta.get("identity_status_counts") or {}, sort_keys=True),
        "identity_method_counts": json.dumps(identity_meta.get("identity_method_counts") or {}, sort_keys=True),
    }
    rows = []
    if report_csv.exists() and report_csv.stat().st_size > 0:
        rows = _read_csv(report_csv)
        rows = [existing for existing in rows if not (existing.get("date") == date_text and existing.get("board") == board)]
    rows.append(row)
    rows.sort(key=lambda r: (str(r.get("date") or ""), str(r.get("board") or "")))
    with report_csv.open("w", encoding="utf-8", newline="") as fh:
        fieldnames = list(row.keys())
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for item in rows:
            writer.writerow({col: item.get(col, "") for col in fieldnames})
    date_rows = [r for r in rows if r.get("date") == date_text]
    lines = [
        f"# Review Board Identity Migration - {date_text}",
        "",
        "- Scope: review-board output identity columns only.",
        "- Production/model/selector/upload/grading behavior changed: `no`.",
        "",
        "| board | rows before | rows after | player ID before | player ID after | game ID before | game ID after | market key after | fallback | ambiguous | unresolved |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in date_rows:
        lines.append(
            f"| `{item.get('board')}` | `{item.get('rows_before')}` | `{item.get('rows_after')}` | "
            f"`{item.get('player_id_coverage_before_pct')}` | `{item.get('player_id_coverage_after_pct')}` | "
            f"`{item.get('game_id_coverage_before_pct')}` | `{item.get('game_id_coverage_after_pct')}` | "
            f"`{item.get('canonical_market_key_coverage_after_pct')}` | `{item.get('fallback_rows_after')}` | "
            f"`{item.get('ambiguous_rows_after')}` | `{item.get('unresolved_rows_after')}` |"
        )
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _iter_matchup_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _starter_status(
    *,
    expected: float | None,
    source_date: str,
    slate_date: str,
    source_note: str,
) -> str:
    if expected is None:
        return "missing"
    if not source_date:
        return "unknown"
    if slate_date and source_date != slate_date:
        return "stale"
    if "fallback" in source_note.lower() or source_note:
        return "projected"
    return "projected"


def _generated_sort_key(value: Any) -> str:
    return str(value or "")


def _copy_context_row(row: dict[str, Any], *, policy_suffix: str) -> dict[str, Any]:
    out = dict(row)
    base_policy = str(out.get("environment_snapshot_policy") or "fullest_valid_projected_starter_artifact")
    if policy_suffix and policy_suffix not in base_policy:
        out["environment_snapshot_policy"] = f"{base_policy}+{policy_suffix}"
    return out


def _starter_identity(row: dict[str, Any]) -> str:
    starter_id = str(row.get("opposing_starter_id") or "").strip()
    if starter_id:
        return f"id:{starter_id}"
    return "name:" + str(row.get("opposing_starter") or "").strip().lower()


def _offense_factor_lineage_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    lineage = payload.get("offense_factor_lineage") if isinstance(payload, dict) else {}
    if not isinstance(lineage, dict):
        lineage = {}
    return {
        "offense_context_as_of_date": _cell(lineage.get("offense_context_as_of_date")),
        "offense_window_excludes_eval_date": _cell(lineage.get("offense_window_excludes_eval_date")),
        "offense_window_max_source_game_date": _cell(lineage.get("offense_window_max_source_game_date")),
        "local_team_hits_parity_status": _cell(lineage.get("local_team_hits_parity_status")) or "unknown",
        "team_hits_mismatch_count": _cell(lineage.get("team_hits_mismatch_count")),
        "team_hits_rescheduled_outside_window_count": _cell(lineage.get("team_hits_rescheduled_outside_window_count")),
        "offense_factor_lineage_health_generated_at": _cell(lineage.get("offense_factor_lineage_health_generated_at")),
    }


def _offense_factor_lineage_from_row(
    row: dict[str, Any],
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fallback = fallback or {}
    out: dict[str, Any] = {}
    for field in OFFENSE_FACTOR_LINEAGE_COLUMNS:
        value = row.get(field)
        if value is None or str(value).strip() == "":
            value = fallback.get(field)
        out[field] = _cell(value)
    if not out.get("local_team_hits_parity_status"):
        out["local_team_hits_parity_status"] = "unknown"
    return out


def _offense_factor_lineage_output_fields(context: dict[str, Any]) -> dict[str, Any]:
    out = {field: context.get(field, "") for field in OFFENSE_FACTOR_LINEAGE_COLUMNS}
    if not str(out.get("local_team_hits_parity_status") or "").strip():
        out["local_team_hits_parity_status"] = "unknown"
    return out


def _environment_component_context(row: dict[str, Any], offense_factor_lineage: dict[str, Any] | None = None) -> dict[str, Any]:
    pitcher_base = _f(row.get("pitcher_expected_hits_allowed_weighted"))
    return {
        "pitcher_expected_hits_allowed_weighted": pitcher_base,
        "pitcher_base": pitcher_base,
        "offense_hits_pg_last7": _f(row.get("offense_hits_pg_last7")),
        "offense_hits_pg_last15": _f(row.get("offense_hits_pg_last15")),
        "offense_hits_pg_last30": _f(row.get("offense_hits_pg_last30")),
        "offense_hits_form_blended": _f(row.get("offense_hits_form_blended")),
        "league_offense_hits_form_blended": _f(row.get("league_offense_hits_form_blended")),
        "offense_factor_vs_league": _f(row.get("offense_factor_vs_league")),
        "offense_factor_vs_league_clamped": _f(row.get("offense_factor_vs_league_clamped")),
        "bullpen_hits_allowed_pg_last7": _f(row.get("bullpen_hits_allowed_pg_last7")),
        "bullpen_hits_allowed_pg_last15": _f(row.get("bullpen_hits_allowed_pg_last15")),
        "bullpen_hits_allowed_pg_last30": _f(row.get("bullpen_hits_allowed_pg_last30")),
        "bullpen_hits_allowed_form_blended": _f(row.get("bullpen_hits_allowed_form_blended")),
        **_offense_factor_lineage_from_row(row, offense_factor_lineage),
    }


def _context_from_matchup_rows(
    *,
    rows: list[dict[str, Any]],
    slate_date: str,
    source_date: str,
    generated_at: str,
    source_label: str,
    artifact_row_count: int,
    snapshot_policy: str,
    offense_factor_lineage: dict[str, Any] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    by_team_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        offense_team = _clean_team(row.get("offense_team"))
        pitcher_team = _clean_team(row.get("pitcher_team"))
        expected = _f(row.get("expected_hits_allowed_matchup"))
        if not offense_team or not pitcher_team or expected is None:
            continue
        key = (offense_team, pitcher_team)
        current = by_team_pair.get(key)
        if current is None or expected > (_f(current.get("starter_expected_hits_allowed")) or -1.0):
            note = str(row.get("forecast_note") or "").strip()
            by_team_pair[key] = {
                **_environment_component_context(row, offense_factor_lineage),
                "starter_expected_hits_allowed": expected,
                "team_expected_hits_allowed": _f(row.get("expected_team_hits_allowed_matchup")),
                "opposing_starter": row.get("pitcher_name") or row.get("starter_name") or row.get("player_name") or "",
                "opposing_starter_id": row.get("player_id") or "",
                "starter_context_status": _starter_status(
                    expected=expected,
                    source_date=source_date,
                    slate_date=slate_date,
                    source_note=note,
                ),
                "starter_context_source": f"{source_label}:{note or 'projected_internal_hits_allowed_forecast'}",
                "starter_context_updated_at": generated_at,
                "environment_artifact_timestamp": generated_at,
                "environment_artifact_row_count": artifact_row_count,
                "environment_snapshot_policy": snapshot_policy,
            }
    return by_team_pair


def _unavailable_reason_from_note(note: Any) -> str:
    text = str(note or "").strip()
    if text == "insufficient_pitcher_history":
        return "starter projected but failed minimum-start requirement"
    if text == "present_in_odds_but_missing_from_slate_output":
        return "starter projected but missing source stats"
    if text in {"unresolved_player_name", "ambiguous_player_name", "unresolved"}:
        return "no projected starter"
    if text in {"no_hits_allowed_market", "no_hits_allowed_market_context_only"}:
        return "no hits-allowed market context only"
    if text:
        return "unknown"
    return "no projected starter"


def _unavailable_context_from_rows(
    *,
    rows: list[dict[str, Any]],
    slate_date: str,
    source_date: str,
    generated_at: str,
    source_label: str,
    artifact_row_count: int,
    snapshot_policy: str,
    required_min_starts: int | None,
    offense_factor_lineage: dict[str, Any] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    by_team_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        offense_team = _clean_team(row.get("offense_team"))
        pitcher_team = _clean_team(row.get("pitcher_team"))
        if not offense_team or not pitcher_team:
            continue
        expected = _f(row.get("expected_hits_allowed_matchup"))
        forecast_status = str(row.get("forecast_status") or "").strip().lower()
        note = str(row.get("forecast_note") or "").strip()
        if expected is not None:
            continue
        if forecast_status != "unavailable" and not note:
            continue
        prior_starts = _f(row.get("prior_starter_games") or row.get("pitcher_baseline_total_starts"))
        min_applied = note == "insufficient_pitcher_history"
        key = (offense_team, pitcher_team)
        current = by_team_pair.get(key)
        if current is not None and min_applied is False:
            continue
        by_team_pair[key] = {
            **_environment_component_context(row, offense_factor_lineage),
            "starter_expected_hits_allowed": None,
            "team_expected_hits_allowed": _f(row.get("expected_team_hits_allowed_matchup")),
            "opposing_starter": row.get("pitcher_name") or row.get("starter_name") or row.get("player_name") or "",
            "opposing_starter_id": row.get("player_id") or "",
            "starter_context_status": _starter_status(
                expected=None,
                source_date=source_date,
                slate_date=slate_date,
                source_note=note,
            ),
            "starter_context_source": f"{source_label}:{note or 'projected_internal_hits_allowed_forecast'}",
            "starter_context_updated_at": generated_at,
            "starter_context_unavailable_reason": _unavailable_reason_from_note(note),
            "starter_min_start_policy_applied": min_applied,
            "starter_starts_count": int(prior_starts) if prior_starts is not None else "",
            "starter_required_min_starts": required_min_starts if required_min_starts is not None else "",
            "environment_artifact_timestamp": generated_at,
            "environment_artifact_row_count": artifact_row_count,
            "environment_snapshot_policy": snapshot_policy,
        }
    return by_team_pair


def _payload_matchup_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    slate_context = payload.get("slate_hits_allowed_context") if isinstance(payload, dict) else {}
    if not isinstance(slate_context, dict):
        slate_context = {}
    candidates: list[dict[str, Any]] = []
    for key in (
        "top_expected_hits_allowed_matchups",
        "lowest_expected_hits_allowed_matchups",
        "top_expected_team_hits_allowed_matchups",
        "lowest_expected_team_hits_allowed_matchups",
        "matchups",
        "rows",
    ):
        candidates.extend(_iter_matchup_rows(slate_context.get(key)))
    return candidates


def _payload_unavailable_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    slate_context = payload.get("slate_hits_allowed_context") if isinstance(payload, dict) else {}
    if not isinstance(slate_context, dict):
        return []
    return _iter_matchup_rows(slate_context.get("forecast_unavailable_pitchers"))


def _candidate_from_payload(path: Path, payload: dict[str, Any], slate_date: str, source_kind: str) -> dict[str, Any] | None:
    source_date = str(
        payload.get("requested_as_of_date") or payload.get("slate_date") or payload.get("date") or ""
    )[:10]
    if source_date != slate_date:
        return None
    generated_at = str(payload.get("generated_at_utc") or payload.get("generated_at") or "")
    slate_context = payload.get("slate_hits_allowed_context") if isinstance(payload, dict) else {}
    if not isinstance(slate_context, dict):
        slate_context = {}
    rows = _payload_matchup_rows(payload)
    coverage = int(
        slate_context.get("rows_with_expected_hits_allowed_matchup")
        or slate_context.get("forecast_available_rows")
        or 0
    )
    if coverage <= 0:
        coverage = sum(1 for row in rows if _f(row.get("expected_hits_allowed_matchup")) is not None)
    artifact_row_count = int(slate_context.get("rows") or len(rows) or coverage)
    policy = "fullest_valid_projected_starter_artifact"
    required_min_starts = _f((payload.get("starter_baseline_config") or {}).get("min_starts"))
    offense_factor_lineage = _offense_factor_lineage_from_payload(payload)
    context = _context_from_matchup_rows(
        rows=rows,
        slate_date=slate_date,
        source_date=source_date,
        generated_at=generated_at,
        source_label=f"{path}:{source_kind}",
        artifact_row_count=artifact_row_count,
        snapshot_policy=policy,
        offense_factor_lineage=offense_factor_lineage,
    )
    unavailable_context = _unavailable_context_from_rows(
        rows=_payload_unavailable_rows(payload),
        slate_date=slate_date,
        source_date=source_date,
        generated_at=generated_at,
        source_label=f"{path}:{source_kind}",
        artifact_row_count=artifact_row_count,
        snapshot_policy=policy,
        required_min_starts=int(required_min_starts) if required_min_starts is not None else None,
        offense_factor_lineage=offense_factor_lineage,
    )
    return {
        "path": str(path),
        "source_kind": source_kind,
        "source_date": source_date,
        "generated_at": generated_at,
        "coverage": coverage,
        "artifact_row_count": artifact_row_count,
        "team_pair_count": len(context),
        "context": context,
        "unavailable_context": unavailable_context,
        "valid": True,
        "reject_reason": "",
        "conflict_count": 0,
    }


def _candidate_from_snapshot_csv(path: Path, slate_date: str, required_min_starts: int | None) -> dict[str, Any] | None:
    rows = _read_csv(path)
    if not rows:
        return None
    source_dates = {str(row.get("slate_date") or row.get("game_date") or "")[:10] for row in rows}
    source_dates.discard("")
    if slate_date not in source_dates:
        return None
    date_rows = [row for row in rows if str(row.get("slate_date") or row.get("game_date") or "")[:10] == slate_date]
    if not date_rows:
        return None
    generated_at = ""
    marker = path.stem.split("__")[-1] if "__" in path.stem else ""
    if marker:
        generated_at = marker
    coverage = sum(1 for row in date_rows if _f(row.get("expected_hits_allowed_matchup")) is not None)
    policy = "fullest_valid_projected_starter_artifact"
    context = _context_from_matchup_rows(
        rows=date_rows,
        slate_date=slate_date,
        source_date=slate_date,
        generated_at=generated_at,
        source_label=f"{path}:full_row_snapshot",
        artifact_row_count=len(date_rows),
        snapshot_policy=policy,
        offense_factor_lineage=None,
    )
    unavailable_context = _unavailable_context_from_rows(
        rows=date_rows,
        slate_date=slate_date,
        source_date=slate_date,
        generated_at=generated_at,
        source_label=f"{path}:full_row_snapshot",
        artifact_row_count=len(date_rows),
        snapshot_policy=policy,
        required_min_starts=required_min_starts,
        offense_factor_lineage=None,
    )
    return {
        "path": str(path),
        "source_kind": "full_row_snapshot_csv",
        "source_date": slate_date,
        "generated_at": generated_at,
        "coverage": coverage,
        "artifact_row_count": len(date_rows),
        "team_pair_count": len(context),
        "context": context,
        "unavailable_context": unavailable_context,
        "valid": True,
        "reject_reason": "",
        "conflict_count": 0,
    }


def _load_history_candidates(history_path: Path, slate_date: str) -> list[dict[str, Any]]:
    if not history_path.exists() or history_path.stat().st_size == 0:
        return []
    out: list[dict[str, Any]] = []
    for line in history_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        candidate = _candidate_from_payload(history_path, payload, slate_date, "history_summary")
        if candidate is not None:
            out.append(candidate)
    return out


def _apply_starter_conflict_rejections(candidates: list[dict[str, Any]]) -> None:
    ordered = sorted(candidates, key=lambda c: _generated_sort_key(c.get("generated_at")))
    for idx, candidate in enumerate(ordered):
        conflicts = 0
        context = candidate.get("context") or {}
        if not isinstance(context, dict):
            continue
        for key, row in context.items():
            identity = _starter_identity(row)
            if identity in {"name:", "id:"}:
                continue
            for later in ordered[idx + 1 :]:
                later_context = later.get("context") or {}
                if not isinstance(later_context, dict) or key not in later_context:
                    continue
                later_identity = _starter_identity(later_context[key])
                if later_identity in {"name:", "id:"}:
                    continue
                if later_identity != identity:
                    conflicts += 1
                    break
        if conflicts:
            candidate["valid"] = False
            candidate["reject_reason"] = "starter_change_conflict_with_later_artifact"
            candidate["conflict_count"] = conflicts


def _overlay_later_snapshot_context(
    *,
    selected: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    selected_generated = _generated_sort_key(selected.get("generated_at"))
    by_team_pair = {
        key: dict(value)
        for key, value in (selected.get("context") or {}).items()
        if isinstance(key, tuple) and isinstance(value, dict)
    }
    unavailable_by_team_pair = {
        key: dict(value)
        for key, value in (selected.get("unavailable_context") or {}).items()
        if isinstance(key, tuple) and isinstance(value, dict)
    }
    overlay_expected_pairs: list[str] = []
    overlay_unavailable_pairs: list[str] = []

    for candidate in sorted(candidates, key=lambda c: _generated_sort_key(c.get("generated_at"))):
        if not candidate.get("valid"):
            continue
        candidate_generated = _generated_sort_key(candidate.get("generated_at"))
        if candidate_generated <= selected_generated:
            continue
        for key, row in (candidate.get("context") or {}).items():
            if not isinstance(key, tuple) or not isinstance(row, dict):
                continue
            if key in by_team_pair:
                continue
            by_team_pair[key] = _copy_context_row(row, policy_suffix="later_snapshot_context_overlay")
            overlay_expected_pairs.append(f"{key[0]}@{key[1]}")
        for key, row in (candidate.get("unavailable_context") or {}).items():
            if not isinstance(key, tuple) or not isinstance(row, dict):
                continue
            if key in by_team_pair or key in unavailable_by_team_pair:
                continue
            unavailable_by_team_pair[key] = _copy_context_row(
                row,
                policy_suffix="later_snapshot_unavailable_overlay",
            )
            overlay_unavailable_pairs.append(f"{key[0]}@{key[1]}")

    return by_team_pair, unavailable_by_team_pair, {
        "later_snapshot_expected_overlay_count": len(overlay_expected_pairs),
        "later_snapshot_expected_overlay_pairs": sorted(overlay_expected_pairs),
        "later_snapshot_unavailable_overlay_count": len(overlay_unavailable_pairs),
        "later_snapshot_unavailable_overlay_pairs": sorted(overlay_unavailable_pairs),
    }


def _load_starter_context(
    path: Path,
    slate_date: str,
    *,
    history_path: Path | None = None,
    snapshot_dir: Path | None = None,
    policy: str = "fullest_valid_projected_starter_artifact",
    required_min_starts: int | None = None,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return {}, {}, {"path": str(path), "exists": path.exists(), "row_count": 0}

    payload = json.loads(path.read_text(encoding="utf-8"))
    latest_candidate = _candidate_from_payload(path, payload, slate_date, "latest_json")
    candidates: list[dict[str, Any]] = []
    if latest_candidate is not None:
        candidates.append(latest_candidate)
    if history_path is not None:
        candidates.extend(_load_history_candidates(history_path, slate_date))
    if snapshot_dir is not None and snapshot_dir.exists():
        for snap in sorted((snapshot_dir / slate_date).glob("mlb_hits_environment_hits_allowed_rows_*.csv")):
            candidate = _candidate_from_snapshot_csv(snap, slate_date, required_min_starts)
            if candidate is not None:
                candidates.append(candidate)

    if not candidates:
        return {}, {}, {"path": str(path), "exists": True, "row_count": 0, "source_date": "", "generated_at": ""}

    _apply_starter_conflict_rejections(candidates)
    valid = [c for c in candidates if c.get("valid")]
    if policy == "latest":
        selected = max(valid or candidates, key=lambda c: _generated_sort_key(c.get("generated_at")))
    else:
        selected = max(
            valid or candidates,
            key=lambda c: (
                int(c.get("coverage") or 0),
                int(c.get("team_pair_count") or 0),
                _generated_sort_key(c.get("generated_at")),
            ),
        )
    latest = max(candidates, key=lambda c: _generated_sort_key(c.get("generated_at")))
    by_team_pair, unavailable_by_team_pair, overlay_meta = _overlay_later_snapshot_context(
        selected=selected,
        candidates=candidates,
    )
    recovered_pairs = sorted(set(by_team_pair) - set((latest.get("context") or {})))
    meta = {
        "path": str(path),
        "exists": True,
        "row_count": int(selected.get("artifact_row_count") or 0),
        "team_pair_count": len(by_team_pair),
        "source_date": selected.get("source_date") or "",
        "generated_at": selected.get("generated_at") or "",
        "environment_snapshot_policy": policy,
        "environment_artifact_timestamp": selected.get("generated_at") or "",
        "environment_artifact_row_count": int(selected.get("artifact_row_count") or 0),
        "selected_artifact_path": selected.get("path") or "",
        "selected_artifact_kind": selected.get("source_kind") or "",
        "selected_artifact_coverage": int(selected.get("coverage") or 0),
        "latest_artifact_path": latest.get("path") or "",
        "latest_artifact_kind": latest.get("source_kind") or "",
        "latest_artifact_timestamp": latest.get("generated_at") or "",
        "latest_artifact_coverage": int(latest.get("coverage") or 0),
        "latest_artifact_team_pair_count": int(latest.get("team_pair_count") or 0),
        "selected_artifact_team_pair_count": int(selected.get("team_pair_count") or 0),
        "post_overlay_team_pair_count": len(by_team_pair),
        "post_overlay_unavailable_team_pair_count": len(unavailable_by_team_pair),
        "candidate_artifact_count": len(candidates),
        "valid_candidate_artifact_count": len(valid),
        "recovered_team_pair_count": len(recovered_pairs),
        "recovered_team_pairs": [f"{a}@{h}" for a, h in recovered_pairs],
        "rejected_artifacts": [
            {
                "path": c.get("path"),
                "generated_at": c.get("generated_at"),
                "coverage": c.get("coverage"),
                "reason": c.get("reject_reason"),
                "conflict_count": c.get("conflict_count"),
            }
            for c in candidates
            if not c.get("valid")
        ],
    }
    meta.update(overlay_meta)
    return by_team_pair, unavailable_by_team_pair, meta


def _fetch_raw_hit_totals(rows: list[dict[str, Any]]) -> tuple[dict[tuple[str, str], dict[str, float]], dict[str, Any]]:
    player_ids = sorted({int(row["player_id"]) for row in rows if _f(row.get("player_id")) is not None})
    dates = sorted({str(row.get("date") or "")[:10] for row in rows if str(row.get("date") or "")[:10]})
    if not player_ids or not dates:
        return {}, {
            "raw_hit_total_source": "mlb.player_stats",
            "raw_hit_total_status": "skipped_no_player_ids_or_dates",
            "raw_hit_total_rows": 0,
        }

    min_date = min(datetime.strptime(d, "%Y-%m-%d").date() for d in dates) - timedelta(days=15)
    max_date = max(datetime.strptime(d, "%Y-%m-%d").date() for d in dates)
    try:
        from backend.shared.db.pg import pg_fetchall

        db_rows = pg_fetchall(
            """
SELECT player_id, game_date::date AS game_date, COALESCE(hits, 0)::float8 AS hits
FROM mlb.player_stats
WHERE player_id = ANY(%s)
  AND game_date >= %s::date
  AND game_date < %s::date
  AND COALESCE(position, '') <> 'P'
ORDER BY player_id, game_date
""",
            (player_ids, min_date.isoformat(), max_date.isoformat()),
        )
    except Exception as exc:
        return {}, {
            "raw_hit_total_source": "mlb.player_stats",
            "raw_hit_total_status": "error",
            "raw_hit_total_error": f"{type(exc).__name__}: {exc}",
            "raw_hit_total_rows": 0,
        }

    by_player: dict[int, list[tuple[str, float]]] = defaultdict(list)
    for row in db_rows or []:
        pid = _f(row.get("player_id"))
        date_text = str(row.get("game_date") or "")[:10]
        if pid is None or not date_text:
            continue
        by_player[int(pid)].append((date_text, _f(row.get("hits")) or 0.0))

    out: dict[tuple[str, str], dict[str, float]] = {}
    for item in rows:
        pid = _f(item.get("player_id"))
        date_text = str(item.get("date") or "")[:10]
        if pid is None or not date_text:
            continue
        slate_date = datetime.strptime(date_text, "%Y-%m-%d").date()
        history = by_player.get(int(pid), [])
        raw_d7 = 0.0
        raw_d15 = 0.0
        for hist_date_text, hits in history:
            hist_date = datetime.strptime(hist_date_text, "%Y-%m-%d").date()
            if slate_date - timedelta(days=7) <= hist_date < slate_date:
                raw_d7 += hits
            if slate_date - timedelta(days=15) <= hist_date < slate_date:
                raw_d15 += hits
        out[(date_text, str(int(pid)))] = {"raw_d7_hits": raw_d7, "raw_d15_hits": raw_d15}

    return out, {
        "raw_hit_total_source": "mlb.player_stats",
        "raw_hit_total_status": "ok",
        "raw_hit_total_rows": len(db_rows or []),
        "raw_hit_total_player_count": len(by_player),
        "raw_hit_total_min_source_date": min_date.isoformat(),
        "raw_hit_total_max_source_date_exclusive": max_date.isoformat(),
        "raw_hit_total_semantics": "calendar-day sums from player_stats, excluding the slate date; context only",
    }


def _team_abbr_from_schedule_team(team: dict[str, Any]) -> str:
    abbr = _clean_team(team.get("abbreviation"))
    if abbr:
        return abbr
    team_id = _f(team.get("id"))
    if team_id is None:
        return ""
    try:
        from backend.mlb.shared.team_name_map import getFullTeamAbbreviationFromID

        return _clean_team(getFullTeamAbbreviationFromID(int(team_id)))
    except Exception:
        return ""


def _probable_starter_display_row(
    *,
    slate_date: str,
    offense_team: str,
    pitcher_team: str,
    pitcher: dict[str, Any],
    source_url: str,
    required_min_starts: int | None,
) -> dict[str, Any] | None:
    pitcher_id = _f(pitcher.get("id"))
    pitcher_name = str(pitcher.get("fullName") or pitcher.get("name") or "").strip()
    if pitcher_id is None and not pitcher_name:
        return None
    return {
        "starter_expected_hits_allowed": None,
        "opposing_starter": pitcher_name,
        "opposing_starter_id": int(pitcher_id) if pitcher_id is not None else "",
        "starter_context_status": "projected",
        "starter_context_source": f"{source_url}:mlb_probable_starter_display_fallback",
        "starter_context_updated_at": "",
        "starter_context_unavailable_reason": "starter projected but missing source stats",
        "starter_min_start_policy_applied": False,
        "starter_starts_count": "",
        "starter_required_min_starts": required_min_starts if required_min_starts is not None else "",
        "environment_artifact_timestamp": "",
        "environment_artifact_row_count": "",
        "environment_snapshot_policy": "mlb_probable_starter_display_fallback",
    }


def _load_probable_starter_display_fallback(
    *,
    slate_date: str,
    starter_context: dict[tuple[str, str], dict[str, Any]],
    unavailable_starter_context: dict[tuple[str, str], dict[str, Any]],
    required_min_starts: int | None,
) -> tuple[dict[tuple[str, str], dict[str, Any]], dict[str, Any]]:
    """Display-only probable starter fallback for review boards.

    This intentionally does not produce expected_hits_allowed and therefore cannot
    assign a trusted pitcher tier. Hits-environment remains the only trusted
    source for expected hits allowed.
    """
    source_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={slate_date}&hydrate=probablePitcher"
    if str(os.getenv("MLB_HITS_REVIEW_DISABLE_PROBABLE_STARTER_FALLBACK", "0")).strip() == "1":
        return {}, {
            "probable_starter_display_fallback_status": "disabled",
            "probable_starter_display_fallback_source": source_url,
            "probable_starter_display_fallback_rows": 0,
        }
    try:
        import requests

        response = requests.get(source_url, timeout=15)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return {}, {
            "probable_starter_display_fallback_status": "error",
            "probable_starter_display_fallback_source": source_url,
            "probable_starter_display_fallback_error": f"{type(exc).__name__}: {exc}",
            "probable_starter_display_fallback_rows": 0,
        }

    out: dict[tuple[str, str], dict[str, Any]] = {}
    for date_block in payload.get("dates") or []:
        for game in date_block.get("games") or []:
            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team = home.get("team") or {}
            away_team = away.get("team") or {}
            home_abbr = _team_abbr_from_schedule_team(home_team)
            away_abbr = _team_abbr_from_schedule_team(away_team)
            if not home_abbr or not away_abbr:
                continue
            home_pitcher = home.get("probablePitcher") or {}
            away_pitcher = away.get("probablePitcher") or {}
            candidates = [
                ((away_abbr, home_abbr), home_pitcher),
                ((home_abbr, away_abbr), away_pitcher),
            ]
            for key, pitcher in candidates:
                if key in starter_context or key in unavailable_starter_context:
                    continue
                row = _probable_starter_display_row(
                    slate_date=slate_date,
                    offense_team=key[0],
                    pitcher_team=key[1],
                    pitcher=pitcher,
                    source_url=source_url,
                    required_min_starts=required_min_starts,
                )
                if row is not None:
                    out[key] = row

    return out, {
        "probable_starter_display_fallback_status": "ok",
        "probable_starter_display_fallback_source": source_url,
        "probable_starter_display_fallback_rows": len(out),
        "probable_starter_display_fallback_pairs": [f"{a}@{h}" for a, h in sorted(out)],
    }


def _o15_hitter_tier(d7_hits_rate: float | None, d15_hits_rate: float | None) -> str:
    if d7_hits_rate is not None and d15_hits_rate is not None:
        if d7_hits_rate > 1.30 and d15_hits_rate > 1.20:
            return "A"
        if d7_hits_rate > 1.10 and d15_hits_rate > 1.10:
            return "B"
    return "C"


def _trusted_starter_status(status: Any) -> bool:
    return str(status or "").strip().lower() in {"confirmed", "projected"}


def _o15_pitcher_tier(starter_expected_hits_allowed: float | None, status: Any = "") -> str:
    if starter_expected_hits_allowed is None or not _trusted_starter_status(status):
        return "U"
    if starter_expected_hits_allowed >= 5.5:
        return "A"
    if starter_expected_hits_allowed >= 5.0:
        return "B"
    if starter_expected_hits_allowed >= 4.5:
        return "C"
    return "D"


def _u15_hitter_tier(d7_hits_rate: float | None, d15_hits_rate: float | None) -> str:
    if d7_hits_rate is not None and d15_hits_rate is not None:
        if d7_hits_rate < 1.0 and d15_hits_rate < 1.0:
            return "A"
        if d7_hits_rate < 1.1 and d15_hits_rate < 1.1:
            return "B"
    return "C"


def _u15_pitcher_tier(starter_expected_hits_allowed: float | None, status: Any = "") -> str:
    if starter_expected_hits_allowed is None or not _trusted_starter_status(status):
        return "U"
    if starter_expected_hits_allowed < 4.5:
        return "A"
    if starter_expected_hits_allowed < 5.0:
        return "B"
    if starter_expected_hits_allowed < 5.5:
        return "C"
    return "D"


def _tier_sort_key(row: dict[str, Any], board: str = "o15") -> tuple[Any, ...]:
    hitter_rank = HITTER_TIER_RANK.get(str(row.get("hitter_tier") or "C"), 9)
    pitcher_rank = PITCHER_TIER_RANK.get(str(row.get("pitcher_tier") or "U"), 9)
    model_prob = _f(row.get("model_prob"))
    qc_score = _f(row.get("qc_score"))
    if board == "watch_o15":
        return (
            hitter_rank,
            pitcher_rank,
            0,
            -(qc_score if qc_score is not None else -1.0),
            str(row.get("player_name") or ""),
        )
    if board == "layered_o15":
        layer_rank = {
            "layer_4_qc_d7_d15_starter": 0,
            "layer_3_d7_d15_starter_non_qc": 1,
            "layer_2_d7_d15_no_favorable_starter": 2,
            "layer_1_d7_hot_not_d15_consistent": 3,
            "all_o15_other": 4,
        }.get(str(row.get("layer_label") or ""), 9)
        d7_hits = _f(row.get("d7_hits_rate"))
        d15_hits = _f(row.get("d15_hits_rate"))
        starter_expected = _f(row.get("starter_expected_hits_allowed"))
        market_price = _f(row.get("market_price"))
        return (
            layer_rank,
            hitter_rank,
            pitcher_rank,
            -(d7_hits if d7_hits is not None else -1.0),
            -(d15_hits if d15_hits is not None else -1.0),
            -(starter_expected if starter_expected is not None else -1.0),
            -(market_price if market_price is not None else -9999.0),
            str(row.get("player_name") or ""),
        )
    if board == "u15":
        layer_rank = {
            "layer_4_qc_d7_d15_tough_starter": 0,
            "layer_3_d7_d15_tough_starter_non_qc": 1,
            "layer_2_d7_d15_no_tough_starter": 2,
            "layer_1_d7_cold_not_d15_consistent": 3,
            "all_u15_other": 4,
        }.get(str(row.get("layer_label") or ""), 9)
        combined = str(row.get("combined_tier") or "")
        return (
            layer_rank,
            U15_COMBINED_TIER_ORDER.get(combined, 100 + hitter_rank * 10 + pitcher_rank),
            hitter_rank,
            pitcher_rank,
            -(model_prob if model_prob is not None else -1.0),
            str(row.get("player_name") or ""),
        )
    return (
        hitter_rank + pitcher_rank,
        hitter_rank,
        pitcher_rank,
        -(model_prob if model_prob is not None else -1.0),
        str(row.get("player_name") or ""),
    )


def _line_key(value: Any) -> str:
    line = _f(value)
    return f"{line:.1f}" if line is not None else ""


def _player_line_key(row: dict[str, Any]) -> tuple[str, str]:
    player_id = _f(row.get("player_id"))
    player_key = str(int(player_id)) if player_id is not None else ""
    return player_key, _line_key(row.get("line"))


def _norm_player_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(ch for ch in text if ch.isalnum() or ch.isspace())
    return " ".join(text.split())


def _load_qc_watch_context(date_text: str, lanes_root: Path | None = None) -> dict[tuple[str, str], dict[str, Any]]:
    root = lanes_root or Path("backend/mlb/exports/model_v2/lanes")
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted((root / "today" / date_text).glob("quick_card_hits_*.csv")):
        if not path.exists() or path.stat().st_size == 0:
            continue
        for row in _read_csv(path):
            if str(row.get("prop_type") or "").strip().lower() != "hits":
                continue
            if _line_key(row.get("line")) != "1.5":
                continue
            key = _player_line_key(row)
            if not key[0]:
                continue
            out[key] = {
                "qc_score": _f(row.get("score") or row.get("rank_score")),
                "qc_selected_side": str(row.get("side") or "").strip().lower(),
                "qc_source_file": str(path),
            }
    return out


def _load_ranking_context(date_text: str, lanes_root: Path | None = None) -> dict[tuple[str, str], dict[str, Any]]:
    root = lanes_root or Path("backend/mlb/exports/model_v2/lanes")
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for path in sorted((root / "today" / date_text).glob(f"hits_lane_selector_{date_text}.csv")):
        if not path.exists() or path.stat().st_size == 0:
            continue
        for row in _read_csv(path):
            if str(row.get("prop_type") or "").strip().lower() != "hits":
                continue
            if str(row.get("side") or "").strip().lower() != "over":
                continue
            if _line_key(row.get("line")) != "1.5":
                continue
            source_lane = str(row.get("source_lane") or "").strip()
            if source_lane == "quick_card_hits":
                continue
            key = _player_line_key(row)
            if not key[0]:
                continue
            out[key] = {
                "ranking_score": _f(row.get("rank_score") or row.get("score")),
                "ranking_source_lane": source_lane,
            }
    return out


def _filter_rows(
    *,
    slate_rows: list[dict[str, Any]],
    starter_context: dict[tuple[str, str], dict[str, Any]],
    unavailable_starter_context: dict[tuple[str, str], dict[str, Any]],
    starter_meta: dict[str, Any],
    raw_hit_totals: dict[tuple[str, str], dict[str, float]],
    qc_context: dict[tuple[str, str], dict[str, Any]] | None = None,
    ranking_context: dict[tuple[str, str], dict[str, Any]] | None = None,
    slate_date: str,
    board: str,
    source_artifact_exists: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    considered: list[dict[str, Any]] = []
    starter_context_rows = 0

    for row in slate_rows:
        row_date = str(row.get("slate_date") or row.get("game_date") or "")[:10]
        if row_date != slate_date:
            continue
        if str(row.get("prop_type") or "").strip().lower() != "hits":
            continue
        line = _f(row.get("line"))
        if line is None or abs(line - 1.5) > 1e-9:
            continue
        player_line_key = _player_line_key(row)
        qc_item = (qc_context or {}).get(player_line_key, {})
        ranking_item = (ranking_context or {}).get(player_line_key, {})
        if board == "watch_o15" and not qc_item:
            continue

        team = _clean_team(row.get("team"))
        opponent = _clean_team(row.get("opponent"))
        context = starter_context.get((team, opponent), {})
        unavailable_context = unavailable_starter_context.get((team, opponent), {})
        starter_expected = _f(context.get("starter_expected_hits_allowed"))
        if starter_expected is not None:
            starter_context_rows += 1
        team_expected = _f(context.get("team_expected_hits_allowed"))
        starter_status = str(context.get("starter_context_status") or "missing")
        if not context and unavailable_context:
            context = unavailable_context
            starter_status = str(context.get("starter_context_status") or "missing")
        selected_artifact_path = str(starter_meta.get("selected_artifact_path") or "")
        selected_artifact_timestamp = str(starter_meta.get("environment_artifact_timestamp") or "")
        selected_artifact_row_count = starter_meta.get("environment_artifact_row_count") or ""
        selected_snapshot_policy = str(starter_meta.get("environment_snapshot_policy") or "")
        if not source_artifact_exists or not selected_artifact_path:
            default_unavailable_reason = "source artifact missing"
        elif starter_status == "stale":
            default_unavailable_reason = "stale starter context"
        elif starter_status == "unknown":
            default_unavailable_reason = "unknown"
        elif starter_expected is None:
            default_unavailable_reason = "no projected starter"
        else:
            default_unavailable_reason = ""
        player_id = _f(row.get("player_id"))
        raw = raw_hit_totals.get((row_date, str(int(player_id)))) if player_id is not None else {}
        if raw is None:
            raw = {}
        time_context = _time_context(row, fallback_date=row_date)

        item = {
            "date": row_date,
            "player": row.get("player_name") or "",
            "player_name": row.get("player_name") or "",
            "player_id": int(player_id) if player_id is not None else "",
            "team": team,
            "opponent": opponent,
            "line": 1.5,
            "side": "under" if board == "u15" else "over",
            "model_prob": _f(row.get("prob_under" if board == "u15" else "prob_over")),
            "market_price": _f(row.get("market_price_under" if board == "u15" else "market_price_over")),
            "selected_side_implied_probability": _american_implied_probability(
                row.get("market_price_under" if board == "u15" else "market_price_over")
            ),
            "d7_hits_rate": _f(row.get("d7_hits")),
            "d15_hits_rate": _f(row.get("d15_hits")),
            "d7_hits_runs_rbis": _f(row.get("d7_hits_runs_rbis")),
            "d15_hits_runs_rbis": _f(row.get("d15_hits_runs_rbis")),
            "d30_hits_runs_rbis": _f(row.get("d30_hits_runs_rbis")),
            "raw_d7_hits_calendar": _f(raw.get("raw_d7_hits")),
            "raw_d15_hits_calendar": _f(raw.get("raw_d15_hits")),
            **{field: context.get(field) for field in ENVIRONMENT_COMPONENT_COLUMNS},
            **_offense_factor_lineage_output_fields(context),
            "starter_expected_hits_allowed": starter_expected,
            "team_expected_hits_allowed": team_expected,
            "opposing_starter": context.get("opposing_starter") or "",
            "opposing_starter_id": context.get("opposing_starter_id") or "",
            "starter_context_status": starter_status,
            "starter_context_source": context.get("starter_context_source") or "",
            "starter_context_updated_at": context.get("starter_context_updated_at") or "",
            "starter_context_unavailable_reason": context.get("starter_context_unavailable_reason")
            or default_unavailable_reason,
            "starter_min_start_policy_applied": context.get("starter_min_start_policy_applied") or False,
            "starter_starts_count": context.get("starter_starts_count") or "",
            "starter_required_min_starts": context.get("starter_required_min_starts") or "",
            "environment_artifact_timestamp": context.get("environment_artifact_timestamp")
            or selected_artifact_timestamp,
            "environment_artifact_row_count": context.get("environment_artifact_row_count")
            or selected_artifact_row_count,
            "environment_snapshot_policy": context.get("environment_snapshot_policy") or selected_snapshot_policy,
            "game_time": time_context["game_time"],
            "time_of_day_bucket": time_context["time_of_day_bucket"],
            "game_day_of_week": time_context["game_day_of_week"],
            "qc_score": qc_item.get("qc_score", ""),
            "qc_selected_side": qc_item.get("qc_selected_side", ""),
            "qc_source_file": qc_item.get("qc_source_file", ""),
            "ranking_score": ranking_item.get("ranking_score", ""),
            "ranking_source_lane": ranking_item.get("ranking_source_lane", ""),
        }
        if board == "watch_o15":
            d7 = _f(item.get("d7_hits_rate"))
            if d7 is None or d7 <= 1.0:
                continue
            if starter_expected is None or starter_expected < 5.0:
                continue
        if board == "u15":
            d7 = _f(item.get("d7_hits_rate"))
            d15 = _f(item.get("d15_hits_rate"))
            d7_cold = d7 is not None and d7 < 1.0
            d15_cold_consistent = d15 is not None and d15 < 1.0
            tough_starter = starter_expected is not None and starter_expected < 4.5
            qc_candidate = bool(qc_item) and str(qc_item.get("qc_selected_side") or "").strip().lower() == "under"
            watch_candidate = qc_candidate and d7_cold and d15_cold_consistent and tough_starter
            item["d7_cold_candidate"] = d7_cold
            item["d15_cold_consistent_candidate"] = d15_cold_consistent
            item["tough_starter_candidate"] = tough_starter
            item["qc_candidate"] = qc_candidate
            item["watch_candidate"] = watch_candidate
            if watch_candidate:
                item["layer_label"] = "layer_4_qc_d7_d15_tough_starter"
            elif d7_cold and d15_cold_consistent and tough_starter:
                item["layer_label"] = "layer_3_d7_d15_tough_starter_non_qc"
            elif d7_cold and d15_cold_consistent:
                item["layer_label"] = "layer_2_d7_d15_no_tough_starter"
            elif d7_cold and not d15_cold_consistent:
                item["layer_label"] = "layer_1_d7_cold_not_d15_consistent"
            else:
                item["layer_label"] = "all_u15_other"
        if board == "layered_o15":
            d7 = _f(item.get("d7_hits_rate"))
            d15 = _f(item.get("d15_hits_rate"))
            d7_hot = d7 is not None and d7 > 1.0
            d15_consistent = d15 is not None and d15 > 1.0
            favorable_starter = starter_expected is not None and starter_expected >= 5.0
            qc_candidate = bool(qc_item)
            watch_candidate = qc_candidate and d7_hot and d15_consistent and favorable_starter
            item["d7_hot_candidate"] = d7_hot
            item["d15_consistent_candidate"] = d15_consistent
            item["favorable_starter_candidate"] = favorable_starter
            item["qc_candidate"] = qc_candidate
            item["watch_candidate"] = watch_candidate
            if watch_candidate:
                item["layer_label"] = "layer_4_qc_d7_d15_starter"
            elif d7_hot and d15_consistent and favorable_starter:
                item["layer_label"] = "layer_3_d7_d15_starter_non_qc"
            elif d7_hot and d15_consistent:
                item["layer_label"] = "layer_2_d7_d15_no_favorable_starter"
            elif d7_hot and not d15_consistent:
                item["layer_label"] = "layer_1_d7_hot_not_d15_consistent"
            else:
                item["layer_label"] = "all_o15_other"
        considered.append(item)

    for row in considered:
        if board == "u15":
            hitter_tier = _u15_hitter_tier(_f(row.get("d7_hits_rate")), _f(row.get("d15_hits_rate")))
            pitcher_tier = _u15_pitcher_tier(
                _f(row.get("starter_expected_hits_allowed")),
                row.get("starter_context_status"),
            )
        else:
            hitter_tier = _o15_hitter_tier(_f(row.get("d7_hits_rate")), _f(row.get("d15_hits_rate")))
            pitcher_tier = _o15_pitcher_tier(
                _f(row.get("starter_expected_hits_allowed")),
                row.get("starter_context_status"),
            )
        row["hitter_tier"] = hitter_tier
        row["pitcher_tier"] = pitcher_tier
        row["combined_tier"] = f"{hitter_tier}/{pitcher_tier}"

    considered.sort(key=lambda r: _tier_sort_key(r, board=board))

    max_d7 = max([_f(r.get("d7_hits_rate")) or 0.0 for r in considered], default=0.0)
    max_d15 = max([_f(r.get("d15_hits_rate")) or 0.0 for r in considered], default=0.0)
    max_raw_d7 = max([_f(r.get("raw_d7_hits_calendar")) or 0.0 for r in considered], default=0.0)
    max_raw_d15 = max([_f(r.get("raw_d15_hits_calendar")) or 0.0 for r in considered], default=0.0)
    diagnostics = {
        "slate_hits_o15_rows_considered": len(considered),
        "rows_with_starter_context": starter_context_rows,
        "rows_with_raw_hit_totals": sum(
            1
            for r in considered
            if _f(r.get("raw_d7_hits_calendar")) is not None and _f(r.get("raw_d15_hits_calendar")) is not None
        ),
        "starter_context_status_counts": dict(
            sorted(
                {
                    status: sum(1 for r in considered if str(r.get("starter_context_status") or "missing") == status)
                    for status in {str(r.get("starter_context_status") or "missing") for r in considered}
                }.items()
            )
        ),
        "max_raw_d7_hits_calendar_in_considered_rows": max_raw_d7,
        "max_raw_d15_hits_calendar_in_considered_rows": max_raw_d15,
        "max_d7_hits_rate_in_considered_rows": max_d7,
        "max_d15_hits_rate_in_considered_rows": max_d15,
        "d7_d15_unit_note": (
            "filter uses slate d7_hits/d15_hits as last-N-player-game rolling averages/rates; raw calendar totals are context only"
            if considered and max_d7 <= 5.0 and max_d15 <= 5.0
            else "filter uses slate d7_hits/d15_hits as rates; raw calendar totals are context only"
        ),
    }
    return considered, diagnostics


def _slate_context_by_player(slate_rows: list[dict[str, Any]], slate_date: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in slate_rows:
        row_date = str(row.get("slate_date") or row.get("game_date") or "")[:10]
        if row_date != slate_date:
            continue
        if str(row.get("prop_type") or "").strip().lower() != "hits":
            continue
        keys: list[str] = []
        player_id = _f(row.get("player_id"))
        if player_id is not None:
            keys.append(str(int(player_id)))
        name_key = _norm_player_name(row.get("player_name"))
        if name_key:
            keys.append(f"name:{name_key}")
        for key in keys:
            existing = out.get(key)
            if existing is None or _line_key(row.get("line")) == "1.5":
                out[key] = row
    return out


def _context_lookup_key(row: dict[str, Any]) -> str:
    player_id = _f(row.get("player_id"))
    if player_id is not None:
        return str(int(player_id))
    name_key = _norm_player_name(row.get("player_name") or row.get("player"))
    return f"name:{name_key}" if name_key else ""


def _aggregate_alternate_rows(path: Path) -> list[dict[str, Any]]:
    rows = _read_csv(path)
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if str(row.get("market_key") or "").strip() != "batter_hits_alternate":
            continue
        if str(row.get("side") or "").strip().lower() != "over":
            continue
        if _line_key(row.get("line")) != "1.5":
            continue
        player_id = str(row.get("player_id") or "").strip()
        player_name = str(row.get("player_name") or "").strip()
        name_key = _norm_player_name(player_name)
        event_id = str(row.get("event_id") or "").strip()
        key = (event_id, player_id or name_key, "1.5")
        item = grouped.setdefault(
            key,
            {
                "player_id": player_id,
                "player_name": player_name,
                "team": _clean_team(row.get("team")),
                "opponent": _clean_team(row.get("opponent")),
                "line": 1.5,
                "game_time": str(row.get("commence_time") or "").strip(),
                "bookmakers": set(),
                "best_over_price": None,
            },
        )
        if not item.get("game_time"):
            item["game_time"] = str(row.get("commence_time") or "").strip()
        book = str(row.get("bookmaker_key") or "").strip()
        if book:
            item["bookmakers"].add(book)
        price = _f(row.get("price"))
        current = _f(item.get("best_over_price"))
        if price is not None and (current is None or price > current):
            item["best_over_price"] = price
    out: list[dict[str, Any]] = []
    for item in grouped.values():
        cp = dict(item)
        books = cp.pop("bookmakers", set())
        cp["bookmaker_list"] = ",".join(sorted(str(book) for book in books if str(book).strip()))
        out.append(cp)
    return out


def _alternate_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    layer_rank = {
        "alternate_layer_a_d7_d15_starter": 0,
        "alternate_layer_b_d7_d15": 1,
        "alternate_layer_c_d7_hot": 2,
        "alternate_other": 3,
    }.get(str(row.get("alternate_layer") or ""), 9)
    hitter_rank = HITTER_TIER_RANK.get(str(row.get("hitter_tier") or "C"), 9)
    pitcher_rank = PITCHER_TIER_RANK.get(str(row.get("pitcher_tier") or "U"), 9)
    d7 = _f(row.get("d7_hits_rate"))
    d15 = _f(row.get("d15_hits_rate"))
    price = _f(row.get("best_over_price"))
    return (
        layer_rank,
        hitter_rank,
        pitcher_rank,
        -(d7 if d7 is not None else -1.0),
        -(d15 if d15 is not None else -1.0),
        -(price if price is not None else -9999.0),
        str(row.get("player_name") or ""),
    )


def _build_alternate_discovery_rows(
    *,
    alternate_book_level_csv: Path,
    slate_rows: list[dict[str, Any]],
    starter_context: dict[tuple[str, str], dict[str, Any]],
    unavailable_starter_context: dict[tuple[str, str], dict[str, Any]],
    starter_meta: dict[str, Any],
    raw_hit_totals: dict[tuple[str, str], dict[str, float]],
    slate_date: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not alternate_book_level_csv.exists() or alternate_book_level_csv.stat().st_size == 0:
        raise SystemExit(
            "Missing alternate discovery source CSV. Build the live OddsAPI batter_hits_alternate source first.\n"
            f"Expected source CSV: {alternate_book_level_csv}\n"
            f"Run source-only: make mlb-oddsapi-batter-hits-alternate-live-discovery DATE={slate_date}\n"
            f"Or run full workflow: make mlb-hits-o15-alternate-discovery-full DATE={slate_date}"
        )
    alt_rows = _aggregate_alternate_rows(alternate_book_level_csv)
    slate_by_player = _slate_context_by_player(slate_rows, slate_date)
    selected_artifact_path = str(starter_meta.get("selected_artifact_path") or "")
    selected_artifact_timestamp = str(starter_meta.get("environment_artifact_timestamp") or "")
    selected_artifact_row_count = starter_meta.get("environment_artifact_row_count") or ""
    selected_snapshot_policy = str(starter_meta.get("environment_snapshot_policy") or "")
    out: list[dict[str, Any]] = []
    for alt in alt_rows:
        lookup = _context_lookup_key(alt)
        slate = slate_by_player.get(lookup, {})
        if not slate:
            name_key = _norm_player_name(alt.get("player_name"))
            slate = slate_by_player.get(f"name:{name_key}", {})
        team = _clean_team(alt.get("team") or slate.get("team"))
        opponent = _clean_team(alt.get("opponent") or slate.get("opponent"))
        context = starter_context.get((team, opponent), {})
        unavailable_context = unavailable_starter_context.get((team, opponent), {})
        starter_expected = _f(context.get("starter_expected_hits_allowed"))
        team_expected = _f(context.get("team_expected_hits_allowed"))
        starter_status = str(context.get("starter_context_status") or "missing")
        if not context and unavailable_context:
            context = unavailable_context
            starter_status = str(context.get("starter_context_status") or "missing")
        if not selected_artifact_path:
            default_unavailable_reason = "source artifact missing"
        elif starter_status == "stale":
            default_unavailable_reason = "stale starter context"
        elif starter_status == "unknown":
            default_unavailable_reason = "unknown"
        elif starter_expected is None:
            default_unavailable_reason = "no projected starter"
        else:
            default_unavailable_reason = ""
        player_id = _f(alt.get("player_id") or slate.get("player_id"))
        raw = raw_hit_totals.get((slate_date, str(int(player_id)))) if player_id is not None else {}
        time_context = _time_context({**slate, **alt}, fallback_date=slate_date)
        d7 = _f(slate.get("d7_hits"))
        d15 = _f(slate.get("d15_hits"))
        d7_hot = d7 is not None and d7 > 1.0
        d15_consistent = d15 is not None and d15 > 1.0
        favorable_starter = starter_expected is not None and starter_expected >= 5.0
        if d7_hot and d15_consistent and favorable_starter:
            layer = "alternate_layer_a_d7_d15_starter"
        elif d7_hot and d15_consistent:
            layer = "alternate_layer_b_d7_d15"
        elif d7_hot:
            layer = "alternate_layer_c_d7_hot"
        else:
            layer = "alternate_other"
        hitter_tier = _o15_hitter_tier(d7, d15)
        pitcher_tier = _o15_pitcher_tier(starter_expected, starter_status)
        out.append(
            {
                "date": slate_date,
                "player": alt.get("player_name") or slate.get("player_name") or "",
                "player_name": alt.get("player_name") or slate.get("player_name") or "",
                "player_id": int(player_id) if player_id is not None else "",
                "team": team,
                "opponent": opponent,
                "bookmaker_list": alt.get("bookmaker_list") or "",
                "best_over_price": _f(alt.get("best_over_price")),
                "selected_side_implied_probability": _american_implied_probability(alt.get("best_over_price")),
                "line": 1.5,
                "side": "over",
                "d7_hits_rate": d7,
                "d15_hits_rate": d15,
                "d7_hits_runs_rbis": _f(slate.get("d7_hits_runs_rbis")),
                "d15_hits_runs_rbis": _f(slate.get("d15_hits_runs_rbis")),
                "d30_hits_runs_rbis": _f(slate.get("d30_hits_runs_rbis")),
                "raw_d7_hits_calendar": _f((raw or {}).get("raw_d7_hits")),
                "raw_d15_hits_calendar": _f((raw or {}).get("raw_d15_hits")),
                **{field: context.get(field) for field in ENVIRONMENT_COMPONENT_COLUMNS},
                **_offense_factor_lineage_output_fields(context),
                "starter_expected_hits_allowed": starter_expected,
                "team_expected_hits_allowed": team_expected,
                "hitter_tier": hitter_tier,
                "pitcher_tier": pitcher_tier,
                "combined_tier": f"{hitter_tier}/{pitcher_tier}",
                "d7_hot_candidate": d7_hot,
                "d15_consistent_candidate": d15_consistent,
                "favorable_starter_candidate": favorable_starter,
                "alternate_layer": layer,
                "game_time": time_context["game_time"],
                "time_of_day_bucket": time_context["time_of_day_bucket"],
                "game_day_of_week": time_context["game_day_of_week"],
                "opposing_starter": context.get("opposing_starter") or "",
                "opposing_starter_id": context.get("opposing_starter_id") or "",
                "starter_context_status": starter_status,
                "starter_context_source": context.get("starter_context_source") or "",
                "starter_context_updated_at": context.get("starter_context_updated_at") or "",
                "starter_context_unavailable_reason": context.get("starter_context_unavailable_reason")
                or default_unavailable_reason,
                "starter_min_start_policy_applied": context.get("starter_min_start_policy_applied") or False,
                "starter_starts_count": context.get("starter_starts_count") or "",
                "starter_required_min_starts": context.get("starter_required_min_starts") or "",
                "environment_artifact_timestamp": context.get("environment_artifact_timestamp")
                or selected_artifact_timestamp,
                "environment_artifact_row_count": context.get("environment_artifact_row_count")
                or selected_artifact_row_count,
                "environment_snapshot_policy": context.get("environment_snapshot_policy") or selected_snapshot_policy,
            }
        )
    out.sort(key=_alternate_sort_key)
    return out, {
        "alternate_book_level_csv": str(alternate_book_level_csv),
        "alternate_total_rows": len(out),
        "alternate_layer_a": sum(1 for row in out if row.get("alternate_layer") == "alternate_layer_a_d7_d15_starter"),
        "alternate_layer_b": sum(1 for row in out if row.get("alternate_layer") == "alternate_layer_b_d7_d15"),
        "alternate_layer_c": sum(1 for row in out if row.get("alternate_layer") == "alternate_layer_c_d7_hot"),
    }


def _fmt(value: Any) -> str:
    number = _f(value)
    if number is None:
        return ""
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:.4f}".rstrip("0").rstrip(".")


def _tier_counts(rows: list[dict[str, Any]], board: str = "o15") -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get("combined_tier") or "U/U")] += 1
    return dict(
        sorted(
            counts.items(),
            key=lambda kv: _tier_sort_key(
                {"combined_tier": kv[0], "hitter_tier": kv[0].split("/")[0], "pitcher_tier": kv[0].split("/")[-1]},
                board=board,
            ),
        )
    )


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row.get("starter_context_status") or "missing")] += 1
    return dict(sorted(counts.items()))


def _tier_counts_by_status(rows: list[dict[str, Any]], board: str) -> list[tuple[str, str, int]]:
    items: list[tuple[str, str, int]] = []
    statuses = sorted({str(r.get("starter_context_status") or "missing") for r in rows})
    tiers = list(_tier_counts(rows, board=board))
    for status in statuses:
        for tier in tiers:
            count = sum(
                1
                for row in rows
                if str(row.get("starter_context_status") or "missing") == status
                and str(row.get("combined_tier") or "") == tier
            )
            if count:
                items.append((status, tier, count))
    return items


def _u_reason_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        if str(row.get("pitcher_tier") or "") != "U":
            continue
        reason = str(row.get("starter_context_unavailable_reason") or "unknown")
        counts[reason] += 1
    return dict(sorted(counts.items()))


def _u_reason_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    out = {
        "missing_starter": 0,
        "min_start_policy": 0,
        "stale_unknown": 0,
        "other": 0,
    }
    for row in rows:
        if str(row.get("pitcher_tier") or "") != "U":
            continue
        reason = str(row.get("starter_context_unavailable_reason") or "unknown")
        if reason == "starter projected but failed minimum-start requirement":
            out["min_start_policy"] += 1
        elif reason == "no projected starter":
            out["missing_starter"] += 1
        elif reason in {"stale starter context", "unknown"}:
            out["stale_unknown"] += 1
        else:
            out["other"] += 1
    return out


def _starter_reason_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "projected": 0,
        "confirmed": 0,
        "missing_starter": 0,
        "failed_min_start_policy": 0,
        "missing_source_stats": 0,
        "stale": 0,
        "unknown": 0,
        "source_artifact_missing": 0,
        "other": 0,
    }
    for row in rows:
        status = str(row.get("starter_context_status") or "missing")
        reason = str(row.get("starter_context_unavailable_reason") or "")
        if status == "projected":
            counts["projected"] += 1
        elif status == "confirmed":
            counts["confirmed"] += 1
        elif reason == "no projected starter":
            counts["missing_starter"] += 1
        elif reason == "starter projected but failed minimum-start requirement":
            counts["failed_min_start_policy"] += 1
        elif reason == "starter projected but missing source stats":
            counts["missing_source_stats"] += 1
        elif reason == "stale starter context" or status == "stale":
            counts["stale"] += 1
        elif reason == "source artifact missing":
            counts["source_artifact_missing"] += 1
        elif reason == "unknown" or status == "unknown":
            counts["unknown"] += 1
        else:
            counts["other"] += 1
    return {key: value for key, value in counts.items() if value}


def _write_md(path: Path, rows: list[dict[str, Any]], meta: dict[str, Any], board: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = _tier_counts(rows, board=board)
    statuses = _status_counts(rows)
    reason_counts = _starter_reason_counts(rows)
    u_summary = _u_reason_summary(rows)
    if board == "watch_o15":
        title = "Hits Over 1.5 Watch Candidate Board"
    else:
        title = "Hits Under 1.5 Favorite Audit Board" if board == "u15" else "Hits Over 1.5 Tiered Review Aid"
    side = "under" if board == "u15" else "over"
    hitter_lines = (
        [
            "- Hitter tier A: `d7_hits_rate < 1.0` and `d15_hits_rate < 1.0`.",
            "- Hitter tier B: `d7_hits_rate < 1.1` and `d15_hits_rate < 1.1`.",
            "- Hitter tier C: all remaining candidates.",
        ]
        if board == "u15"
        else [
            "- Hitter tier A: `d7_hits_rate > 1.30` and `d15_hits_rate > 1.20`.",
            "- Hitter tier B: `d7_hits_rate > 1.10` and `d15_hits_rate > 1.10`.",
            "- Hitter tier C: all remaining candidates.",
        ]
    )
    pitcher_lines = (
        [
            "- Pitcher tier A: `starter_expected_hits_allowed < 4.5`.",
            "- Pitcher tier B: `4.5 <= starter_expected_hits_allowed < 5.0`.",
            "- Pitcher tier C: `5.0 <= starter_expected_hits_allowed < 5.5`.",
            "- Pitcher tier D: `starter_expected_hits_allowed >= 5.5`.",
        "- Pitcher tier U: starter context unavailable.",
        ]
        if board == "u15"
        else [
            "- Pitcher tier A: `starter_expected_hits_allowed >= 5.5`.",
            "- Pitcher tier B: `5.0 <= starter_expected_hits_allowed < 5.5`.",
            "- Pitcher tier C: `4.5 <= starter_expected_hits_allowed < 5.0`.",
            "- Pitcher tier D: `starter_expected_hits_allowed < 4.5`.",
            "- Pitcher tier U: starter context unavailable.",
        ]
    )
    lines = [
        f"# {title}",
        "",
        f"- Date: `{meta.get('date')}`",
        "- Scope: review aid only; no production selector/upload/threshold/grading changes.",
        f"- Candidate universe: `prop_type = hits`, `side = {side}`, `line = 1.5`.",
        *(
            [
                "- Watch subset: Quick Card candidate + `d7_hits_rate > 1.0` + `starter_expected_hits_allowed >= 5.0`.",
                "- Purpose: historically positive, outcome-backed subset; keep separate from the broader discovery board.",
            ]
            if board == "watch_o15"
            else []
        ),
        *hitter_lines,
        *pitcher_lines,
        f"- Candidate rows: `{len(rows)}`",
        f"- Slate rows considered: `{meta.get('slate_hits_o15_rows_considered')}`",
        f"- Rows with starter context: `{meta.get('rows_with_starter_context')}`",
        f"- Confirmed starter rows: `{statuses.get('confirmed', 0)}`",
        f"- Projected starter rows: `{statuses.get('projected', 0)}`",
        f"- Unavailable/untrusted starter rows: `{len(rows) - statuses.get('confirmed', 0) - statuses.get('projected', 0)}`",
        f"- Pitcher tier U due to missing starter: `{u_summary.get('missing_starter', 0)}`",
        f"- Pitcher tier U due to min-start policy: `{u_summary.get('min_start_policy', 0)}`",
        f"- Pitcher tier U due to stale/unknown: `{u_summary.get('stale_unknown', 0)}`",
        f"- Pitcher tier U due to other: `{u_summary.get('other', 0)}`",
        f"- Rows with raw hit totals: `{meta.get('rows_with_raw_hit_totals')}`",
        f"- Raw calendar hit total source: `{meta.get('raw_hit_total_source')}` | status `{meta.get('raw_hit_total_status')}`",
        *(
            [f"- Raw hit total error: `{meta.get('raw_hit_total_error')}`"]
            if meta.get("raw_hit_total_error")
            else []
        ),
        f"- Hits environment latest source: `{meta.get('hits_environment_json')}`",
        f"- Environment snapshot policy: `{meta.get('environment_snapshot_policy')}`",
        f"- Selected environment artifact: `{meta.get('selected_artifact_path')}`",
        f"- Selected environment coverage: `{meta.get('selected_artifact_coverage')}` rows / `{meta.get('selected_artifact_team_pair_count')}` team pairs",
        f"- Latest environment coverage: `{meta.get('latest_artifact_coverage')}` rows / `{meta.get('latest_artifact_team_pair_count')}` team pairs",
        f"- Recovered team pairs versus latest: `{meta.get('recovered_team_pair_count')}`",
        f"- Slate output source: `{meta.get('slate_output_csv')}`",
        f"- Note: {meta.get('d7_d15_unit_note')}",
        "- Pitcher tier `U` means starter context was unavailable or untrusted at this run.",
        "",
    ]
    if rows:
        lines.extend(["## Count By Combined Tier", ""])
        for tier, count in counts.items():
            lines.append(f"- `{tier}`: `{count}`")
        lines.append("")

        lines.extend(["## Starter Context Status", ""])
        for status, count in statuses.items():
            lines.append(f"- `{status}`: `{count}`")
        lines.append("")

        lines.extend(["## Starter Context Reason Counts", ""])
        for reason, count in reason_counts.items():
            lines.append(f"- `{reason}`: `{count}`")
        lines.append("")

        lines.extend(["## Tier Counts By Starter Context Status", ""])
        for status, tier, count in _tier_counts_by_status(rows, board=board):
            lines.append(f"- `{status}` / `{tier}`: `{count}`")
        lines.append("")

        lines.extend(["## Pitcher Tier U Reasons", ""])
        u_reason_counts = _u_reason_counts(rows)
        if u_reason_counts:
            for reason, count in u_reason_counts.items():
                lines.append(f"- `{reason}`: `{count}`")
        else:
            lines.append("- None")
        lines.append("")

        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row.get("combined_tier") or "U/U")].append(row)

        lines.extend(["## Players By Combined Tier", ""])
        for tier in counts:
            lines.extend(
                [
                    f"### {tier}",
                    "",
                    "| player | player_id | team | opp | model_prob | market_price | implied | d7 | d15 | d7 HRR | d15 HRR | raw_d7 | raw_d15 | starter exp | team exp | qc_score | ranking_score | starter_status | starter_unavailable_reason | starts/min | tod | dow | game_time | opposing_starter |",
                    "|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|---|---|",
                ]
            )
            for row in grouped.get(tier, []):
                starts = _fmt(row.get("starter_starts_count"))
                required = _fmt(row.get("starter_required_min_starts"))
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("player_name") or ""),
                            str(row.get("player_id") or ""),
                            str(row.get("team") or ""),
                            str(row.get("opponent") or ""),
                            _fmt(row.get("model_prob")),
                            _fmt(row.get("market_price")),
                            _fmt(row.get("selected_side_implied_probability")),
                            _fmt(row.get("d7_hits_rate")),
                            _fmt(row.get("d15_hits_rate")),
                            _fmt(row.get("d7_hits_runs_rbis")),
                            _fmt(row.get("d15_hits_runs_rbis")),
                            _fmt(row.get("raw_d7_hits_calendar")),
                            _fmt(row.get("raw_d15_hits_calendar")),
                            _fmt(row.get("starter_expected_hits_allowed")),
                            _fmt(row.get("team_expected_hits_allowed")),
                            _fmt(row.get("qc_score")),
                            _fmt(row.get("ranking_score")),
                            str(row.get("starter_context_status") or ""),
                            str(row.get("starter_context_unavailable_reason") or ""),
                            f"{starts}/{required}" if starts or required else "",
                            str(row.get("time_of_day_bucket") or ""),
                            str(row.get("game_day_of_week") or ""),
                            str(row.get("game_time") or ""),
                            str(row.get("opposing_starter") or ""),
                        ]
                    )
                    + " |"
                )
            lines.append("")
    else:
        lines.append("No hits over 1.5 rows were available for tiering.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _boolish(row: dict[str, Any], key: str) -> bool:
    value = row.get(key)
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _layered_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    layers = {
        "all_o15": len(rows),
        "d7_hot": sum(1 for row in rows if _boolish(row, "d7_hot_candidate")),
        "d7_d15": sum(
            1
            for row in rows
            if _boolish(row, "d7_hot_candidate") and _boolish(row, "d15_consistent_candidate")
        ),
        "d7_d15_plus_favorable_starter": sum(
            1
            for row in rows
            if _boolish(row, "d7_hot_candidate")
            and _boolish(row, "d15_consistent_candidate")
            and _boolish(row, "favorable_starter_candidate")
        ),
        "qc_watch_candidate": sum(1 for row in rows if _boolish(row, "watch_candidate")),
    }
    tier_by_layer: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        layer = str(row.get("layer_label") or "all_o15_other")
        tier = str(row.get("combined_tier") or "missing")
        tier_by_layer[layer][tier] += 1
    return {
        **layers,
        "tier_by_layer": {layer: dict(sorted(counts.items())) for layer, counts in tier_by_layer.items()},
    }


def _u15_layered_counts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    layers = {
        "all_u15": len(rows),
        "d7_cold": sum(1 for row in rows if _boolish(row, "d7_cold_candidate")),
        "d7_d15_cold": sum(
            1
            for row in rows
            if _boolish(row, "d7_cold_candidate") and _boolish(row, "d15_cold_consistent_candidate")
        ),
        "d7_d15_tough_starter": sum(
            1
            for row in rows
            if _boolish(row, "d7_cold_candidate")
            and _boolish(row, "d15_cold_consistent_candidate")
            and _boolish(row, "tough_starter_candidate")
        ),
        "qc_watch_candidate": sum(1 for row in rows if _boolish(row, "watch_candidate")),
    }
    tier_by_layer: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        layer = str(row.get("layer_label") or "all_u15_other")
        tier = str(row.get("combined_tier") or "missing")
        tier_by_layer[layer][tier] += 1
    return {
        **layers,
        "tier_by_layer": {layer: dict(sorted(counts.items())) for layer, counts in tier_by_layer.items()},
    }


def _write_layered_md(path: Path, rows: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = _layered_counts(rows)
    ordered_layers = [
        ("layer_4_qc_d7_d15_starter", "Layer 4: QC + d7 + d15 + Favorable Starter"),
        ("layer_3_d7_d15_starter_non_qc", "Layer 3: d7 + d15 + Favorable Starter, Not Layer 4"),
        ("layer_2_d7_d15_no_favorable_starter", "Layer 2: d7 + d15, Not Favorable Starter"),
        ("layer_1_d7_hot_not_d15_consistent", "Layer 1: d7 Hot, Not d15 Consistent"),
    ]
    lines = [
        "# Hits Over 1.5 Layered Candidate Board",
        "",
        f"- Date: `{meta.get('date')}`",
        "- Scope: review aid only; no production selector/upload/threshold/grading changes.",
        "- Candidate universe: `prop_type = hits`, `side = over`, `line = 1.5`.",
        "- Layer 4: Quick Card candidate + `d7_hits_rate > 1.0` + `d15_hits_rate > 1.0` + `starter_expected_hits_allowed >= 5.0`.",
        "- Layer 3: `d7_hits_rate > 1.0` + `d15_hits_rate > 1.0` + `starter_expected_hits_allowed >= 5.0`, excluding Layer 4.",
        "- Layer 2: `d7_hits_rate > 1.0` + `d15_hits_rate > 1.0`, without favorable starter context.",
        "- Layer 1: `d7_hits_rate > 1.0` but `d15_hits_rate <= 1.0` or unavailable.",
        "",
        "## Summary Counts",
        "",
        f"- All o1.5 rows: `{counts.get('all_o15', 0)}`",
        f"- d7_hot rows: `{counts.get('d7_hot', 0)}`",
        f"- d7 + d15 rows: `{counts.get('d7_d15', 0)}`",
        f"- d7 + d15 + favorable starter rows: `{counts.get('d7_d15_plus_favorable_starter', 0)}`",
        f"- QC + d7 + d15 + favorable starter watch candidates: `{counts.get('qc_watch_candidate', 0)}`",
        f"- Excluded all-o1.5 rows outside useful layers: `{sum(1 for row in rows if row.get('layer_label') == 'all_o15_other')}`",
        f"- Environment snapshot policy: `{meta.get('environment_snapshot_policy')}`",
        f"- Selected environment artifact: `{meta.get('selected_artifact_path')}`",
        f"- Selected environment coverage: `{meta.get('selected_artifact_coverage')}` rows / `{meta.get('selected_artifact_team_pair_count')}` team pairs",
        f"- Latest environment coverage: `{meta.get('latest_artifact_coverage')}` rows / `{meta.get('latest_artifact_team_pair_count')}` team pairs",
        "",
        "## A/A And A/B Counts By Layer",
        "",
    ]
    tier_by_layer = counts.get("tier_by_layer") if isinstance(counts.get("tier_by_layer"), dict) else {}
    for layer, title in ordered_layers:
        layer_counts = tier_by_layer.get(layer, {}) if isinstance(tier_by_layer.get(layer), dict) else {}
        lines.append(
            f"- {title}: A/A `{layer_counts.get('A/A', 0)}`, A/B `{layer_counts.get('A/B', 0)}`"
        )
    lines.append("")

    for layer, title in ordered_layers:
        layer_rows = [row for row in rows if str(row.get("layer_label") or "") == layer]
        lines.extend([f"## {title}", ""])
        if not layer_rows:
            lines.extend(["- None", ""])
            continue
        lines.append("| player | player_id | team | opp | tier | odds | implied | model_prob | d7 | d15 | d7 HRR | d15 HRR | starter exp | team exp | QC | QC score | ranking score | starter status | tod | dow | game_time | opposing starter |")
        lines.append("|---|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---|---|---|---|---|")
        for row in layer_rows:
            lines.append(
                f"| {row.get('player') or row.get('player_name') or ''} | {row.get('player_id') or ''} | "
                f"{row.get('team') or ''} | "
                f"{row.get('opponent') or ''} | `{row.get('combined_tier') or ''}` | "
                f"`{_fmt(row.get('market_price'))}` | `{_fmt(row.get('selected_side_implied_probability'))}` | "
                f"`{_fmt(row.get('model_prob'))}` | "
                f"`{_fmt(row.get('d7_hits_rate'))}` | `{_fmt(row.get('d15_hits_rate'))}` | "
                f"`{_fmt(row.get('d7_hits_runs_rbis'))}` | `{_fmt(row.get('d15_hits_runs_rbis'))}` | "
                f"`{_fmt(row.get('starter_expected_hits_allowed'))}` | `{_fmt(row.get('team_expected_hits_allowed'))}` | "
                f"`{str(row.get('qc_candidate')).lower()}` | `{_fmt(row.get('qc_score'))}` | "
                f"`{_fmt(row.get('ranking_score'))}` | `{row.get('starter_context_status') or ''}` | "
                f"{row.get('time_of_day_bucket') or ''} | {row.get('game_day_of_week') or ''} | "
                f"{row.get('game_time') or ''} | {row.get('opposing_starter') or ''} |"
            )
        lines.append("")

    lines.extend(["## Excluded All-o1.5 Summary", ""])
    excluded = [row for row in rows if str(row.get("layer_label") or "") == "all_o15_other"]
    lines.append(f"- Rows outside the four listed layers: `{len(excluded)}`")
    lines.append("- These rows remain in the CSV with `layer_label = all_o15_other` for auditability.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_u15_layered_md(path: Path, rows: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    layer_counts = _u15_layered_counts(rows)
    tier_counts = _tier_counts(rows, board="u15")
    statuses = _status_counts(rows)
    reason_counts = _starter_reason_counts(rows)
    u_summary = _u_reason_summary(rows)
    ordered_layers = [
        ("layer_4_qc_d7_d15_tough_starter", "Layer 4: QC + d7 + d15 + starter_expected_hits_allowed < 4.5"),
        (
            "layer_3_d7_d15_tough_starter_non_qc",
            "Layer 3: d7 + d15 + starter_expected_hits_allowed < 4.5, excluding Layer 4",
        ),
        (
            "layer_2_d7_d15_no_tough_starter",
            "Layer 2: d7 + d15, without tough starter",
        ),
        ("layer_1_d7_cold_not_d15_consistent", "Layer 1: d7 Cold, Not d15 Consistent"),
        ("all_u15_other", "all_u15_other"),
    ]

    lines = [
        "# Hits Under 1.5 Favorite Audit Board",
        "",
        f"- Date: `{meta.get('date')}`",
        "- Scope: review aid only; no production selector/upload/threshold/grading changes.",
        "- Candidate universe: `prop_type = hits`, `side = under`, `line = 1.5`.",
        "- Hitter tier A: `d7_hits_rate < 1.0` and `d15_hits_rate < 1.0`.",
        "- Hitter tier B: `d7_hits_rate < 1.1` and `d15_hits_rate < 1.1`.",
        "- Hitter tier C: all remaining candidates.",
        "- Pitcher tier A: `starter_expected_hits_allowed < 4.5`.",
        "- Pitcher tier B: `4.5 <= starter_expected_hits_allowed < 5.0`.",
        "- Pitcher tier C: `5.0 <= starter_expected_hits_allowed < 5.5`.",
        "- Pitcher tier D: `starter_expected_hits_allowed >= 5.5`.",
        "- Pitcher tier U: starter context unavailable/untrusted.",
        "",
        "## Summary Counts",
        "",
        f"- All u1.5 rows: `{layer_counts.get('all_u15', 0)}`",
        f"- d7 cold rows: `{layer_counts.get('d7_cold', 0)}`",
        f"- d7 + d15 cold rows: `{layer_counts.get('d7_d15_cold', 0)}`",
        f"- d7 + d15 + starter_expected_hits_allowed < 4.5 rows: `{layer_counts.get('d7_d15_tough_starter', 0)}`",
        f"- QC + d7 + d15 + starter_expected_hits_allowed < 4.5 watch candidates: `{layer_counts.get('qc_watch_candidate', 0)}`",
        f"- Excluded all-u1.5 rows outside useful layers: `{sum(1 for row in rows if row.get('layer_label') == 'all_u15_other')}`",
        f"- Rows with starter context: `{meta.get('rows_with_starter_context')}`",
        f"- Confirmed starter rows: `{statuses.get('confirmed', 0)}`",
        f"- Projected starter rows: `{statuses.get('projected', 0)}`",
        f"- Unavailable/untrusted starter rows: `{len(rows) - statuses.get('confirmed', 0) - statuses.get('projected', 0)}`",
        f"- Pitcher tier U due to missing starter: `{u_summary.get('missing_starter', 0)}`",
        f"- Pitcher tier U due to min-start policy: `{u_summary.get('min_start_policy', 0)}`",
        f"- Pitcher tier U due to stale/unknown: `{u_summary.get('stale_unknown', 0)}`",
        f"- Pitcher tier U due to other: `{u_summary.get('other', 0)}`",
        f"- Rows with raw hit totals: `{meta.get('rows_with_raw_hit_totals')}`",
        f"- Raw calendar hit total source: `{meta.get('raw_hit_total_source')}` | status `{meta.get('raw_hit_total_status')}`",
        *(
            [f"- Raw hit total error: `{meta.get('raw_hit_total_error')}`"]
            if meta.get("raw_hit_total_error")
            else []
        ),
        f"- Hits environment latest source: `{meta.get('hits_environment_json')}`",
        f"- Environment snapshot policy: `{meta.get('environment_snapshot_policy')}`",
        f"- Selected environment artifact: `{meta.get('selected_artifact_path')}`",
        f"- Selected environment coverage: `{meta.get('selected_artifact_coverage')}` rows / `{meta.get('selected_artifact_team_pair_count')}` team pairs",
        f"- Latest environment coverage: `{meta.get('latest_artifact_coverage')}` rows / `{meta.get('latest_artifact_team_pair_count')}` team pairs",
        f"- Slate output source: `{meta.get('slate_output_csv')}`",
        f"- Note: {meta.get('d7_d15_unit_note')}",
        "",
        "## Count By Combined Tier",
        "",
    ]
    if rows:
        for tier, count in tier_counts.items():
            lines.append(f"- `{tier}`: `{count}`")
        lines.append("")

        lines.extend(["## Starter Context Status", ""])
        for status, count in statuses.items():
            lines.append(f"- `{status}`: `{count}`")
        lines.append("")

        lines.extend(["## Starter Context Reason Counts", ""])
        for reason, count in reason_counts.items():
            lines.append(f"- `{reason}`: `{count}`")
        lines.append("")

        lines.extend(["## Tier Counts By Starter Context Status", ""])
        for status, tier, count in _tier_counts_by_status(rows, board="u15"):
            lines.append(f"- `{status}` / `{tier}`: `{count}`")
        lines.append("")

        lines.extend(["## Pitcher Tier U Reasons", ""])
        u_reason_counts = _u_reason_counts(rows)
        if u_reason_counts:
            for reason, count in u_reason_counts.items():
                lines.append(f"- `{reason}`: `{count}`")
        else:
            lines.append("- None")
        lines.append("")

        lines.extend(["## Layer Summary", ""])
        for layer, title in ordered_layers:
            layer_total = sum(1 for row in rows if str(row.get("layer_label") or "") == layer)
            lines.append(f"- {title}: `{layer_total}`")
        lines.append("")
        lines.append("- Layer labels remain in the CSV for tracking and performance aggregation.")
        lines.append("- Layer 4/3 are Pitcher Tier `A` by definition; Layer 2 can contain Pitcher Tier `B`, `C`, `D`, or `U`.")
        lines.append("")

        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row.get("combined_tier") or "U/U")].append(row)

        lines.extend(["## Players By Combined Tier", ""])
        for tier in tier_counts:
            lines.extend(
                [
                    f"### {tier}",
                    "",
                    "| player | player_id | team | opp | model_prob | market_price | implied | d7 | d15 | d7 HRR | d15 HRR | raw_d7 | raw_d15 | starter exp | team exp | qc_score | ranking_score | starter_status | starter_unavailable_reason | starts/min | layer | watch | tod | dow | game_time | opposing_starter |",
                    "|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|---|---|---|---|",
                ]
            )
            for row in grouped.get(tier, []):
                starts = _fmt(row.get("starter_starts_count"))
                required = _fmt(row.get("starter_required_min_starts"))
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("player_name") or row.get("player") or ""),
                            str(row.get("player_id") or ""),
                            str(row.get("team") or ""),
                            str(row.get("opponent") or ""),
                            _fmt(row.get("model_prob")),
                            _fmt(row.get("market_price")),
                            _fmt(row.get("selected_side_implied_probability")),
                            _fmt(row.get("d7_hits_rate")),
                            _fmt(row.get("d15_hits_rate")),
                            _fmt(row.get("d7_hits_runs_rbis")),
                            _fmt(row.get("d15_hits_runs_rbis")),
                            _fmt(row.get("raw_d7_hits_calendar")),
                            _fmt(row.get("raw_d15_hits_calendar")),
                            _fmt(row.get("starter_expected_hits_allowed")),
                            _fmt(row.get("team_expected_hits_allowed")),
                            _fmt(row.get("qc_score")),
                            _fmt(row.get("ranking_score")),
                            str(row.get("starter_context_status") or ""),
                            str(row.get("starter_context_unavailable_reason") or ""),
                            f"{starts}/{required}" if starts or required else "",
                            str(row.get("layer_label") or ""),
                            str(row.get("watch_candidate") or "").lower(),
                            str(row.get("time_of_day_bucket") or ""),
                            str(row.get("game_day_of_week") or ""),
                            str(row.get("game_time") or ""),
                            str(row.get("opposing_starter") or ""),
                        ]
                    )
                    + " |"
                )
            lines.append("")
    else:
        lines.append("No hits under 1.5 rows were available for tiering.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_alternate_discovery_md(path: Path, rows: list[dict[str, Any]], meta: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    layer_counts = Counter(str(row.get("alternate_layer") or "alternate_other") for row in rows)
    d7_count = sum(1 for row in rows if row.get("d7_hot_candidate") is True)
    d7_d15_count = sum(
        1 for row in rows if row.get("d7_hot_candidate") is True and row.get("d15_consistent_candidate") is True
    )
    d7_d15_starter_count = sum(
        1
        for row in rows
        if row.get("d7_hot_candidate") is True
        and row.get("d15_consistent_candidate") is True
        and row.get("favorable_starter_candidate") is True
    )
    ordered_layers = [
        ("alternate_layer_a_d7_d15_starter", "Alternate Layer A: d7 + d15 + Starter >= 5.0"),
        ("alternate_layer_b_d7_d15", "Alternate Layer B: d7 + d15"),
        ("alternate_layer_c_d7_hot", "Alternate Layer C: d7 Hot"),
    ]
    lines = [
        "# Hits Over 1.5 Alternate Discovery",
        "",
        "**DISCOVERY ONLY**",
        "",
        "**ALTERNATE MARKET**",
        "",
        "**OVER-ONLY FEED**",
        "",
        "**NOT INCLUDED IN PRODUCTION SCORING**",
        "",
        "**NOT INCLUDED IN UPLOADS**",
        "",
        "**NOT INCLUDED IN GRADING**",
        "",
        f"- Date: `{meta.get('date')}`",
        "- Source: OddsAPI `batter_hits_alternate` only.",
        f"- Alternate source CSV: `{meta.get('alternate_book_level_csv')}`",
        f"- Environment snapshot policy: `{meta.get('environment_snapshot_policy')}`",
        f"- Selected environment artifact: `{meta.get('selected_artifact_path')}`",
        "",
        "## Summary Counts",
        "",
        f"- Total alternate rows: `{len(rows)}`",
        f"- d7 rows: `{d7_count}`",
        f"- d7+d15 rows: `{d7_d15_count}`",
        f"- d7+d15+starter rows: `{d7_d15_starter_count}`",
        f"- Layer A: `{layer_counts.get('alternate_layer_a_d7_d15_starter', 0)}`",
        f"- Layer B: `{layer_counts.get('alternate_layer_b_d7_d15', 0)}`",
        f"- Layer C: `{layer_counts.get('alternate_layer_c_d7_hot', 0)}`",
        "",
    ]
    for layer, title in ordered_layers:
        layer_rows = [row for row in rows if str(row.get("alternate_layer") or "") == layer]
        lines.extend([f"## {title}", ""])
        if not layer_rows:
            lines.extend(["- None", ""])
            continue
        lines.append(
            "| player | team | opp | tier | best over | implied | d7 | d15 | d7 HRR | d15 HRR | starter exp | team exp | tod | dow | books | starter status | opposing starter |"
        )
        lines.append("|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|")
        for row in layer_rows:
            lines.append(
                f"| {row.get('player_name') or row.get('player') or ''} | {row.get('team') or ''} | "
                f"{row.get('opponent') or ''} | `{row.get('combined_tier') or ''}` | "
                f"`{_fmt(row.get('best_over_price'))}` | `{_fmt(row.get('selected_side_implied_probability'))}` | "
                f"`{_fmt(row.get('d7_hits_rate'))}` | "
                f"`{_fmt(row.get('d15_hits_rate'))}` | `{_fmt(row.get('d7_hits_runs_rbis'))}` | "
                f"`{_fmt(row.get('d15_hits_runs_rbis'))}` | "
                f"`{_fmt(row.get('starter_expected_hits_allowed'))}` | `{_fmt(row.get('team_expected_hits_allowed'))}` | "
                f"{row.get('time_of_day_bucket') or ''} | {row.get('game_day_of_week') or ''} | "
                f"{row.get('bookmaker_list') or ''} | `{row.get('starter_context_status') or ''}` | "
                f"{row.get('opposing_starter') or ''} |"
            )
        lines.append("")
    lines.extend(
        [
            "## Recommendation",
            "",
            "- Use this board for manual discovery/research only.",
            "- Because the captured alternate market is Over-only, it should remain outside production scoring and uploads.",
            "- Daily generation is justified if Layer A keeps producing a meaningful number of candidates; today's Layer A count is "
            f"`{layer_counts.get('alternate_layer_a_d7_d15_starter', 0)}`.",
            "",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_policy_doc(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Hits Review Board Environment Snapshot Policy",
        "",
        "Scope: review aids only. This policy does not change production model scoring, selector logic, uploads, grading, or wager matching.",
        "",
        "Policy: use the fullest valid projected-starter artifact for the requested slate date.",
        "",
        "Selection rules:",
        "- Reject artifacts whose source date does not match the review slate date.",
        "- Prefer timestamped full-row hits-environment snapshots when available.",
        "- Include same-slate history summaries as fallback when full-row snapshots were not archived yet.",
        "- Select the valid artifact with maximum projected starter coverage, breaking ties by team-pair coverage and then latest timestamp.",
        "- Reject an earlier artifact if a later same-slate artifact has a nonblank starter contradiction for the same team matchup.",
        "",
        "Row provenance fields:",
        "- `environment_artifact_timestamp`",
        "- `environment_artifact_row_count`",
        "- `environment_snapshot_policy`",
        "",
        "Archival note: `mlb-hits-environment-report` writes timestamped full-row CSV snapshots under `artifacts/analysis/mlb/hits_environment_snapshots/<DATE>/` so future audits can reconstruct row-level changes.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_policy_audit(path: Path, meta: dict[str, Any], rows: list[dict[str, Any]], board: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    recovered = meta.get("recovered_team_pairs") if isinstance(meta.get("recovered_team_pairs"), list) else []
    recovered_set = set(str(pair) for pair in recovered)
    affected = [
        row
        for row in rows
        if row.get("starter_expected_hits_allowed") not in (None, "")
        and f"{row.get('team')}@{row.get('opponent')}" in recovered_set
    ]
    lines = [
        "# Hits Review Board Environment Snapshot Audit",
        "",
        f"- Date: `{meta.get('date')}`",
        f"- Board: `{board}`",
        f"- Policy: `{meta.get('environment_snapshot_policy')}`",
        f"- Latest artifact: `{meta.get('latest_artifact_path')}`",
        f"- Latest artifact timestamp: `{meta.get('latest_artifact_timestamp')}`",
        f"- Latest artifact coverage: `{meta.get('latest_artifact_coverage')}` rows / `{meta.get('latest_artifact_team_pair_count')}` team pairs",
        f"- Selected artifact: `{meta.get('selected_artifact_path')}`",
        f"- Selected artifact timestamp: `{meta.get('environment_artifact_timestamp')}`",
        f"- Selected artifact coverage: `{meta.get('selected_artifact_coverage')}` rows / `{meta.get('selected_artifact_team_pair_count')}` team pairs",
        f"- Recovered team pairs versus latest: `{meta.get('recovered_team_pair_count')}`",
        f"- Candidate artifacts inspected: `{meta.get('candidate_artifact_count')}`",
        f"- Valid candidate artifacts: `{meta.get('valid_candidate_artifact_count')}`",
        f"- Rejected artifacts: `{len(meta.get('rejected_artifacts') or [])}`",
        "",
        "## Recovered Team Pairs",
        "",
    ]
    if recovered:
        lines.extend([f"- `{pair}`" for pair in recovered])
    else:
        lines.append("- None")
    lines.extend(["", "## Review Rows Recovered Versus Latest", ""])
    if affected:
        lines.append("| player | team | opp | combined_tier | starter_expected_hits_allowed | opposing_starter |")
        lines.append("|---|---|---|---|---:|---|")
        for row in affected:
            lines.append(
                f"| {row.get('player_name') or ''} | {row.get('team') or ''} | {row.get('opponent') or ''} | "
                f"{row.get('combined_tier') or ''} | {_fmt(row.get('starter_expected_hits_allowed'))} | "
                f"{row.get('opposing_starter') or ''} |"
            )
    else:
        lines.append("- None")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build hits 1.5 review-aid artifacts.")
    ap.add_argument("--date", default=_today_et(), help="Slate date to review, YYYY-MM-DD.")
    ap.add_argument("--board", choices=("o15", "u15", "watch_o15", "layered_o15", "alternate_o15"), default="o15")
    ap.add_argument("--slate-output-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--hits-environment-json", default="artifacts/analysis/mlb/mlb_hits_environment_latest.json")
    ap.add_argument("--hits-environment-history-jsonl", default="artifacts/analysis/mlb/mlb_hits_environment_history.jsonl")
    ap.add_argument("--hits-environment-snapshot-dir", default="artifacts/analysis/mlb/hits_environment_snapshots")
    ap.add_argument(
        "--alternate-book-level-csv",
        default="artifacts/analysis/mlb/review_aids/oddsapi_batter_hits_alternate_live_discovery/{date}/live_alternate_book_level_rows.csv",
    )
    ap.add_argument("--starter-required-min-starts", type=int, default=5)
    ap.add_argument(
        "--environment-snapshot-policy",
        choices=("fullest_valid_projected_starter_artifact", "latest"),
        default="fullest_valid_projected_starter_artifact",
    )
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/review_aids")
    args = ap.parse_args()

    date_text = str(args.date)[:10]
    slate_path = Path(args.slate_output_csv)
    hits_env_path = Path(args.hits_environment_json)
    out_dir = Path(args.out_dir)

    slate_rows = _read_csv(slate_path)
    starter_context, unavailable_starter_context, starter_meta = _load_starter_context(
        hits_env_path,
        date_text,
        history_path=Path(args.hits_environment_history_jsonl),
        snapshot_dir=Path(args.hits_environment_snapshot_dir),
        policy=args.environment_snapshot_policy,
        required_min_starts=int(args.starter_required_min_starts),
    )
    probable_fallback_context, probable_fallback_meta = _load_probable_starter_display_fallback(
        slate_date=date_text,
        starter_context=starter_context,
        unavailable_starter_context=unavailable_starter_context,
        required_min_starts=int(args.starter_required_min_starts),
    )
    for key, value in probable_fallback_context.items():
        if key not in starter_context and key not in unavailable_starter_context:
            unavailable_starter_context[key] = value
    starter_meta.update(probable_fallback_meta)
    alternate_book_level_csv = Path(str(args.alternate_book_level_csv).format(date=date_text))
    if args.board == "alternate_o15":
        alt_source_rows = _aggregate_alternate_rows(alternate_book_level_csv) if alternate_book_level_csv.exists() else []
        candidate_rows = [
            {"date": date_text, "player_id": int(_f(row.get("player_id")) or 0)}
            for row in alt_source_rows
            if _f(row.get("player_id")) is not None
        ]
    else:
        candidate_rows = [
            {
                "date": str(row.get("slate_date") or row.get("game_date") or "")[:10],
                "player_id": row.get("player_id"),
            }
            for row in slate_rows
            if str(row.get("slate_date") or row.get("game_date") or "")[:10] == date_text
            and str(row.get("prop_type") or "").strip().lower() == "hits"
            and (_f(row.get("line")) is not None and abs((_f(row.get("line")) or 0.0) - 1.5) <= 1e-9)
        ]
    raw_hit_totals, raw_meta = _fetch_raw_hit_totals(candidate_rows)
    qc_context = _load_qc_watch_context(date_text) if args.board in {"watch_o15", "layered_o15", "u15"} else {}
    ranking_context = _load_ranking_context(date_text) if args.board in {"watch_o15", "layered_o15", "u15"} else {}
    if args.board == "alternate_o15":
        rows, diagnostics = _build_alternate_discovery_rows(
            alternate_book_level_csv=alternate_book_level_csv,
            slate_rows=slate_rows,
            starter_context=starter_context,
            unavailable_starter_context=unavailable_starter_context,
            starter_meta=starter_meta,
            raw_hit_totals=raw_hit_totals,
            slate_date=date_text,
        )
        diagnostics.update(
            {
                "slate_hits_o15_rows_considered": len(rows),
                "rows_with_starter_context": sum(
                    1 for row in rows if _f(row.get("starter_expected_hits_allowed")) is not None
                ),
                "rows_with_raw_hit_totals": sum(
                    1
                    for row in rows
                    if _f(row.get("raw_d7_hits_calendar")) is not None
                    and _f(row.get("raw_d15_hits_calendar")) is not None
                ),
                "d7_d15_unit_note": "alternate board uses slate d7_hits/d15_hits as rates; raw calendar totals are context only",
            }
        )
    else:
        rows, diagnostics = _filter_rows(
            slate_rows=slate_rows,
            starter_context=starter_context,
            unavailable_starter_context=unavailable_starter_context,
            starter_meta=starter_meta,
            raw_hit_totals=raw_hit_totals,
            qc_context=qc_context,
            ranking_context=ranking_context,
            slate_date=date_text,
            board=args.board,
            source_artifact_exists=bool(starter_meta.get("exists")),
        )

    if args.board == "watch_o15":
        prefix = "hits_o15_watch_candidates"
    elif args.board == "layered_o15":
        prefix = "hits_o15_layered_candidates"
    elif args.board == "alternate_o15":
        prefix = "hits_o15_alternate_discovery"
    else:
        prefix = "hits_u15_favorite_audit" if args.board == "u15" else "hits_o15_simple_filter"
    out_csv = out_dir / f"{prefix}_{date_text}.csv"
    out_md = out_dir / f"{prefix}_{date_text}.md"
    before_rows = _read_csv(out_csv)
    identity_meta = _apply_canonical_identity(rows, slate_rows, date_text)
    apply_o15_board_ontology(rows, args.board)
    if args.board == "watch_o15":
        columns = WATCH_OUTPUT_COLUMNS
    elif args.board == "layered_o15":
        columns = LAYERED_OUTPUT_COLUMNS
    elif args.board == "u15":
        columns = U15_OUTPUT_COLUMNS
    elif args.board == "alternate_o15":
        columns = ALTERNATE_DISCOVERY_COLUMNS
    else:
        columns = OUTPUT_COLUMNS
    _write_csv(out_csv, rows, columns=columns)
    _append_identity_migration_report(
        out_dir=out_dir,
        date_text=date_text,
        board=args.board,
        out_csv=out_csv,
        before_rows=before_rows,
        after_rows=rows,
        identity_meta=identity_meta,
    )

    meta = {
        "date": date_text,
        "slate_output_csv": str(slate_path),
        "hits_environment_json": str(hits_env_path),
        "hits_environment_history_jsonl": str(Path(args.hits_environment_history_jsonl)),
        "hits_environment_snapshot_dir": str(Path(args.hits_environment_snapshot_dir)),
        **starter_meta,
        **raw_meta,
        **diagnostics,
        **identity_meta,
    }
    if args.board == "layered_o15":
        _write_layered_md(out_md, rows, meta)
    elif args.board == "u15":
        _write_u15_layered_md(out_md, rows, meta)
    elif args.board == "alternate_o15":
        _write_alternate_discovery_md(out_md, rows, meta)
    else:
        _write_md(out_md, rows, meta, board=args.board)
    _write_policy_doc(out_dir / "hits_environment_snapshot_policy.md")
    _write_policy_audit(out_dir / f"hits_environment_snapshot_policy_audit_{date_text}_{args.board}.md", meta, rows, args.board)

    counts = _tier_counts(rows, board=args.board)
    statuses = _status_counts(rows)
    u_reason_counts = _u_reason_counts(rows)
    starter_reason_counts = _starter_reason_counts(rows)
    if args.board == "watch_o15":
        print("hits_o15_watch_candidates")
    elif args.board == "layered_o15":
        print("hits_o15_layered_candidates")
    elif args.board == "alternate_o15":
        print("hits_o15_alternate_discovery")
    else:
        print("hits_u15_favorite_audit" if args.board == "u15" else "hits_o15_tiered_review_aid")
    print(f"date={date_text}")
    print(f"candidate_rows={len(rows)}")
    print("tier_counts=" + ",".join(f"{tier}:{count}" for tier, count in counts.items()))
    if args.board == "layered_o15":
        layer_counts = _layered_counts(rows)
        print(
            "layer_counts="
            + ",".join(
                f"{key}:{layer_counts.get(key, 0)}"
                for key in (
                    "all_o15",
                    "d7_hot",
                    "d7_d15",
                    "d7_d15_plus_favorable_starter",
                    "qc_watch_candidate",
                )
            )
        )
    if args.board == "u15":
        layer_counts = _u15_layered_counts(rows)
        print(
            "layer_counts="
            + ",".join(
                f"{key}:{layer_counts.get(key, 0)}"
                for key in (
                    "all_u15",
                    "d7_cold",
                    "d7_d15_cold",
                    "d7_d15_tough_starter",
                    "qc_watch_candidate",
                )
            )
        )
    if args.board == "alternate_o15":
        print(
            "alternate_layer_counts="
            + ",".join(
                f"{key}:{diagnostics.get(key, 0)}"
                for key in ("alternate_total_rows", "alternate_layer_a", "alternate_layer_b", "alternate_layer_c")
            )
        )
    print("starter_context_status_counts=" + ",".join(f"{status}:{count}" for status, count in statuses.items()))
    print(
        "starter_context_reason_counts="
        + ",".join(f"{reason}:{count}" for reason, count in starter_reason_counts.items())
    )
    print("pitcher_tier_u_reason_counts=" + ",".join(f"{reason}:{count}" for reason, count in u_reason_counts.items()))
    print(f"slate_hits_15_rows_considered={diagnostics['slate_hits_o15_rows_considered']}")
    print(f"rows_with_starter_context={diagnostics['rows_with_starter_context']}")
    print(f"environment_snapshot_policy={starter_meta.get('environment_snapshot_policy')}")
    print(f"latest_artifact_coverage={starter_meta.get('latest_artifact_coverage')}")
    print(f"selected_artifact_coverage={starter_meta.get('selected_artifact_coverage')}")
    print(f"recovered_team_pair_count={starter_meta.get('recovered_team_pair_count')}")
    print(f"selected_artifact_path={starter_meta.get('selected_artifact_path')}")
    print(f"rows_with_raw_hit_totals={diagnostics['rows_with_raw_hit_totals']}")
    print(f"raw_hit_total_status={raw_meta.get('raw_hit_total_status')}")
    if raw_meta.get("raw_hit_total_error"):
        print(f"raw_hit_total_error={raw_meta.get('raw_hit_total_error')}")
    print(f"csv={out_csv}")
    print(f"md={out_md}")
    print(f"note={diagnostics['d7_d15_unit_note']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
