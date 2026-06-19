#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


OUTPUT_COLUMNS = [
    "date",
    "player_name",
    "team",
    "opponent",
    "line",
    "side",
    "model_prob",
    "market_price",
    "d7_hits_rate",
    "d15_hits_rate",
    "raw_d7_hits_calendar",
    "raw_d15_hits_calendar",
    "starter_expected_hits_allowed",
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


def _f(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    except Exception:
        return None


def _clean_team(value: Any) -> str:
    return str(value or "").strip().upper()


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in OUTPUT_COLUMNS})


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


def _starter_identity(row: dict[str, Any]) -> str:
    starter_id = str(row.get("opposing_starter_id") or "").strip()
    if starter_id:
        return f"id:{starter_id}"
    return "name:" + str(row.get("opposing_starter") or "").strip().lower()


def _context_from_matchup_rows(
    *,
    rows: list[dict[str, Any]],
    slate_date: str,
    source_date: str,
    generated_at: str,
    source_label: str,
    artifact_row_count: int,
    snapshot_policy: str,
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
                "starter_expected_hits_allowed": expected,
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
            "starter_expected_hits_allowed": None,
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
    context = _context_from_matchup_rows(
        rows=rows,
        slate_date=slate_date,
        source_date=source_date,
        generated_at=generated_at,
        source_label=f"{path}:{source_kind}",
        artifact_row_count=artifact_row_count,
        snapshot_policy=policy,
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
    by_team_pair = selected.get("context") if isinstance(selected.get("context"), dict) else {}
    unavailable_by_team_pair = (
        selected.get("unavailable_context") if isinstance(selected.get("unavailable_context"), dict) else {}
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


def _tier_sort_key(row: dict[str, Any], board: str = "o15") -> tuple[int, int, int, float, str]:
    hitter_rank = HITTER_TIER_RANK.get(str(row.get("hitter_tier") or "C"), 9)
    pitcher_rank = PITCHER_TIER_RANK.get(str(row.get("pitcher_tier") or "U"), 9)
    model_prob = _f(row.get("model_prob"))
    if board == "u15":
        combined = str(row.get("combined_tier") or "")
        return (
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


def _filter_rows(
    *,
    slate_rows: list[dict[str, Any]],
    starter_context: dict[tuple[str, str], dict[str, Any]],
    unavailable_starter_context: dict[tuple[str, str], dict[str, Any]],
    starter_meta: dict[str, Any],
    raw_hit_totals: dict[tuple[str, str], dict[str, float]],
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

        team = _clean_team(row.get("team"))
        opponent = _clean_team(row.get("opponent"))
        context = starter_context.get((team, opponent), {})
        unavailable_context = unavailable_starter_context.get((team, opponent), {})
        starter_expected = _f(context.get("starter_expected_hits_allowed"))
        if starter_expected is not None:
            starter_context_rows += 1
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

        item = {
            "date": row_date,
            "player_name": row.get("player_name") or "",
            "team": team,
            "opponent": opponent,
            "line": 1.5,
            "side": "under" if board == "u15" else "over",
            "model_prob": _f(row.get("prob_under" if board == "u15" else "prob_over")),
            "market_price": _f(row.get("market_price_under" if board == "u15" else "market_price_over")),
            "d7_hits_rate": _f(row.get("d7_hits")),
            "d15_hits_rate": _f(row.get("d15_hits")),
            "raw_d7_hits_calendar": _f(raw.get("raw_d7_hits")),
            "raw_d15_hits_calendar": _f(raw.get("raw_d15_hits")),
            "starter_expected_hits_allowed": starter_expected,
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
            "game_time": row.get("game_time") or "",
        }
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
                    "| player | team | opp | model_prob | market_price | d7_hits_rate | d15_hits_rate | raw_d7_hits_calendar | raw_d15_hits_calendar | starter_expected_hits_allowed | starter_status | starter_unavailable_reason | starts/min | game_time | opposing_starter |",
                    "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|",
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
                            str(row.get("team") or ""),
                            str(row.get("opponent") or ""),
                            _fmt(row.get("model_prob")),
                            _fmt(row.get("market_price")),
                            _fmt(row.get("d7_hits_rate")),
                            _fmt(row.get("d15_hits_rate")),
                            _fmt(row.get("raw_d7_hits_calendar")),
                            _fmt(row.get("raw_d15_hits_calendar")),
                            _fmt(row.get("starter_expected_hits_allowed")),
                            str(row.get("starter_context_status") or ""),
                            str(row.get("starter_context_unavailable_reason") or ""),
                            f"{starts}/{required}" if starts or required else "",
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
    ap.add_argument("--board", choices=("o15", "u15"), default="o15")
    ap.add_argument("--slate-output-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--hits-environment-json", default="artifacts/analysis/mlb/mlb_hits_environment_latest.json")
    ap.add_argument("--hits-environment-history-jsonl", default="artifacts/analysis/mlb/mlb_hits_environment_history.jsonl")
    ap.add_argument("--hits-environment-snapshot-dir", default="artifacts/analysis/mlb/hits_environment_snapshots")
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
    rows, diagnostics = _filter_rows(
        slate_rows=slate_rows,
        starter_context=starter_context,
        unavailable_starter_context=unavailable_starter_context,
        starter_meta=starter_meta,
        raw_hit_totals=raw_hit_totals,
        slate_date=date_text,
        board=args.board,
        source_artifact_exists=bool(starter_meta.get("exists")),
    )

    prefix = "hits_u15_favorite_audit" if args.board == "u15" else "hits_o15_simple_filter"
    out_csv = out_dir / f"{prefix}_{date_text}.csv"
    out_md = out_dir / f"{prefix}_{date_text}.md"
    _write_csv(out_csv, rows)

    meta = {
        "date": date_text,
        "slate_output_csv": str(slate_path),
        "hits_environment_json": str(hits_env_path),
        "hits_environment_history_jsonl": str(Path(args.hits_environment_history_jsonl)),
        "hits_environment_snapshot_dir": str(Path(args.hits_environment_snapshot_dir)),
        **starter_meta,
        **raw_meta,
        **diagnostics,
    }
    _write_md(out_md, rows, meta, board=args.board)
    _write_policy_doc(out_dir / "hits_environment_snapshot_policy.md")
    _write_policy_audit(out_dir / f"hits_environment_snapshot_policy_audit_{date_text}_{args.board}.md", meta, rows, args.board)

    counts = _tier_counts(rows, board=args.board)
    statuses = _status_counts(rows)
    u_reason_counts = _u_reason_counts(rows)
    starter_reason_counts = _starter_reason_counts(rows)
    print("hits_u15_favorite_audit" if args.board == "u15" else "hits_o15_tiered_review_aid")
    print(f"date={date_text}")
    print(f"candidate_rows={len(rows)}")
    print("tier_counts=" + ",".join(f"{tier}:{count}" for tier, count in counts.items()))
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
