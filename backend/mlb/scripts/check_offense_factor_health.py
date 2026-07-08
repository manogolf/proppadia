#!/usr/bin/env python3
"""WARN-only health checks for MLB offense factor source parity and date context."""

from __future__ import annotations

import argparse
import csv
import json
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.mlb.shared.team_name_map import getFullTeamAbbreviationFromID, normalizeTeamAbbreviation
from backend.shared.db.pg import pg_fetchall


ROOT = next(p for p in Path(__file__).resolve().parents if (p / "Makefile").exists())


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_text(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value or "").strip()


def _canonical_team(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.isdigit():
        try:
            return str(normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(int(text))) or "").strip()
        except Exception:
            pass
    return str(normalizeTeamAbbreviation(text) or text).strip()


def _fetch_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _latest_local_completed_dates(as_of_date: date, sample_days: int) -> list[date]:
    rows = pg_fetchall(
        """
        SELECT ps.game_date::date AS game_date,
               COUNT(DISTINCT ps.game_id)::int AS games
        FROM mlb.player_stats ps
        WHERE ps.game_date <= %s::date
        GROUP BY 1
        HAVING COUNT(DISTINCT ps.game_id) > 0
        ORDER BY 1 DESC
        LIMIT %s
        """,
        (as_of_date.isoformat(), int(sample_days)),
    )
    dates = [_parse_date(str(row.get("game_date"))) for row in rows or [] if row.get("game_date")]
    return sorted(dates)


def _local_team_hits(sample_dates: list[date]) -> dict[tuple[int, str], dict[str, Any]]:
    if not sample_dates:
        return {}
    rows = pg_fetchall(
        """
        SELECT ps.game_date::date AS game_date,
               ps.game_id,
               ps.team,
               SUM(COALESCE(ps.hits, 0))::int AS local_team_hits,
               COUNT(*)::int AS local_player_rows,
               COUNT(*) FILTER (WHERE COALESCE(ps.hits, 0) > 0)::int AS local_players_with_hits
        FROM mlb.player_stats ps
        WHERE ps.game_date = ANY(%s::date[])
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        """,
        ([d.isoformat() for d in sample_dates],),
    )
    out: dict[tuple[int, str], dict[str, Any]] = {}
    for row in rows or []:
        game_id = row.get("game_id")
        team = _canonical_team(row.get("team"))
        if game_id is None or not team:
            continue
        out[(int(game_id), team)] = {
            "local_game_date": _date_text(row.get("game_date")),
            "local_team_hits": int(row.get("local_team_hits") or 0),
            "local_player_rows": int(row.get("local_player_rows") or 0),
            "local_players_with_hits": int(row.get("local_players_with_hits") or 0),
        }
    return out


def _game_info_dates(game_ids: list[int]) -> dict[int, str]:
    if not game_ids:
        return {}
    rows = pg_fetchall(
        """
        SELECT game_id, game_date::date AS game_info_game_date
        FROM mlb.game_info
        WHERE game_id = ANY(%s::bigint[])
        """,
        (game_ids,),
    )
    return {
        int(row.get("game_id")): _date_text(row.get("game_info_game_date"))
        for row in rows or []
        if row.get("game_id") is not None
    }


def _official_team_hits(sample_dates: list[date]) -> tuple[dict[tuple[int, str], dict[str, Any]], dict[int, set[str]], list[dict[str, Any]]]:
    official_rows: list[dict[str, Any]] = []
    seen_dates: dict[int, set[str]] = defaultdict(set)
    source_errors: list[dict[str, Any]] = []
    box_cache: dict[int, dict[str, Any]] = {}

    for sample_date in sample_dates:
        schedule_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={sample_date.isoformat()}"
        try:
            schedule = _fetch_json(schedule_url)
        except Exception as exc:
            source_errors.append(
                {
                    "sample_date": sample_date.isoformat(),
                    "url": schedule_url,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        for block in schedule.get("dates") or []:
            schedule_query_date = str(block.get("date") or sample_date.isoformat())
            for game in block.get("games") or []:
                game_id = game.get("gamePk")
                if game_id is None:
                    continue
                game_id = int(game_id)
                seen_dates[game_id].add(schedule_query_date)
                official_date = str(game.get("officialDate") or "")[:10]
                status = str(((game.get("status") or {}).get("detailedState") or "")).strip()
                box_url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
                try:
                    if game_id not in box_cache:
                        box_cache[game_id] = _fetch_json(box_url)
                    boxscore = box_cache[game_id]
                except Exception as exc:
                    source_errors.append(
                        {
                            "sample_date": sample_date.isoformat(),
                            "url": box_url,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    continue
                for side in ("home", "away"):
                    side_payload = ((boxscore.get("teams") or {}).get(side) or {})
                    team_obj = side_payload.get("team") or {}
                    team = _canonical_team(team_obj.get("id") or team_obj.get("abbreviation") or team_obj.get("name"))
                    batting = ((side_payload.get("teamStats") or {}).get("batting") or {})
                    hits = batting.get("hits")
                    if not team:
                        continue
                    official_rows.append(
                        {
                            "sample_date": sample_date.isoformat(),
                            "schedule_query_date": schedule_query_date,
                            "statsapi_official_date": official_date,
                            "game_id": int(game_id),
                            "team": team,
                            "side": side,
                            "game_status": status,
                            "statsapi_team_hits": "" if hits is None else int(hits),
                            "statsapi_boxscore_url": box_url,
                        }
                    )

    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in official_rows:
        grouped[(int(row["game_id"]), str(row["team"]))].append(row)

    def choose(candidates: list[dict[str, Any]]) -> dict[str, Any]:
        def key(row: dict[str, Any]) -> tuple[int, str]:
            official_date = str(row.get("statsapi_official_date") or "9999-99-99")
            schedule_date = str(row.get("schedule_query_date") or "9999-99-99")
            return (0 if schedule_date == official_date else 1, schedule_date)

        return dict(sorted(candidates, key=key)[0])

    return {key: choose(candidates) for key, candidates in grouped.items()}, seen_dates, source_errors


def _max_local_source_game_date(context_as_of_date: date) -> str:
    rows = pg_fetchall(
        """
        SELECT MAX(ps.game_date)::date AS max_game_date
        FROM mlb.player_stats ps
        WHERE ps.game_date <= %s::date
        """,
        (context_as_of_date.isoformat(),),
    )
    return _date_text((rows or [{}])[0].get("max_game_date"))


def _context_date_check(
    *,
    as_of_date: date,
    eval_date: date | None,
    offense_context_as_of_date: date,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    max_source_date = _max_local_source_game_date(offense_context_as_of_date)
    excludes_eval_date = ""
    status = "PASS"
    severity = "PASS"
    reason = "eval_date_not_provided_context_guard_not_applicable"
    if eval_date is not None:
        excludes_eval_date = offense_context_as_of_date < eval_date
        if offense_context_as_of_date >= eval_date:
            status = "FAIL"
            severity = "FAIL"
            reason = "offense_context_as_of_date_not_before_eval_date"
        else:
            reason = "offense_context_as_of_date_before_eval_date"
    row = {
        "as_of_date": as_of_date.isoformat(),
        "eval_date": "" if eval_date is None else eval_date.isoformat(),
        "offense_context_as_of_date": offense_context_as_of_date.isoformat(),
        "offense_window_excludes_eval_date": excludes_eval_date,
        "max_local_source_game_date_used": max_source_date,
        "status": status,
        "severity": severity,
        "reason": reason,
    }
    return [row], {
        "status": status,
        "severity": severity,
        "reason": reason,
        "max_local_source_game_date_used": max_source_date,
        "offense_window_excludes_eval_date": excludes_eval_date,
    }


def _parity_trace(sample_dates: list[date]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    local = _local_team_hits(sample_dates)
    official, official_seen_dates, source_errors = _official_team_hits(sample_dates)
    game_info_dates = _game_info_dates(sorted({int(game_id) for game_id, _team in set(local) | set(official)}))
    sample_start = sample_dates[0].isoformat() if sample_dates else ""
    sample_end = sample_dates[-1].isoformat() if sample_dates else ""
    sample_date_set = {d.isoformat() for d in sample_dates}
    rows: list[dict[str, Any]] = []
    for game_id, team in sorted(set(local) | set(official)):
        local_row = local.get((game_id, team), {})
        official_row = official.get((game_id, team), {})
        local_hits = local_row.get("local_team_hits")
        official_hits = official_row.get("statsapi_team_hits")
        official_date = str(official_row.get("statsapi_official_date") or "")
        if local_hits is None:
            if official_date and official_date not in sample_date_set:
                classification = "INFO"
                status = "INFO_RESCHEDULED_OR_OUTSIDE_WINDOW"
                root_cause = "rescheduled_or_postponed_official_date_outside_sample_window"
            else:
                classification = "WARN"
                status = "WARN_MISSING_LOCAL_COMPLETED_ROW"
                root_cause = "local_rows_missing_for_official_boxscore_team"
            diff: int | str = ""
        elif official_hits in (None, ""):
            classification = "WARN"
            status = "WARN_MISSING_OFFICIAL_ROW"
            root_cause = "official_boxscore_missing_from_sample"
            diff = ""
        else:
            diff_i = int(local_hits) - int(official_hits)
            diff = diff_i
            if diff_i == 0:
                classification = "PASS"
                status = "PASS_EXACT_MATCH"
                root_cause = ""
            else:
                classification = "WARN"
                status = "WARN_DIRECT_LOCAL_OFFICIAL_MISMATCH"
                root_cause = "likely_local_stale_or_uncorrected_or_boxscore_correction"
        rows.append(
            {
                "sample_start_date": sample_start,
                "sample_end_date": sample_end,
                "game_id": int(game_id),
                "team": team,
                "local_game_date": local_row.get("local_game_date", ""),
                "game_info_game_date": game_info_dates.get(int(game_id), ""),
                "statsapi_official_date": official_date,
                "statsapi_schedule_query_dates_seen": "|".join(sorted(official_seen_dates.get(int(game_id), set()))),
                "local_team_hits": "" if local_hits is None else local_hits,
                "statsapi_team_hits": "" if official_hits in (None, "") else official_hits,
                "hit_diff_local_minus_statsapi": diff,
                "local_player_rows": local_row.get("local_player_rows", ""),
                "local_players_with_hits": local_row.get("local_players_with_hits", ""),
                "game_status": official_row.get("game_status", ""),
                "classification": classification,
                "parity_status": status,
                "root_cause_hint": root_cause,
                "statsapi_boxscore_url": official_row.get("statsapi_boxscore_url", ""),
            }
        )
    counts = Counter(row["classification"] for row in rows)
    status_counts = Counter(row["parity_status"] for row in rows)
    root_counts = Counter(row["root_cause_hint"] for row in rows if row.get("root_cause_hint"))
    summary = {
        "sample_start_date": sample_start,
        "sample_end_date": sample_end,
        "sample_dates": [d.isoformat() for d in sample_dates],
        "rows": len(rows),
        "pass_count": int(counts.get("PASS", 0)),
        "warn_count": int(counts.get("WARN", 0)),
        "info_count": int(counts.get("INFO", 0)),
        "direct_mismatch_count": int(status_counts.get("WARN_DIRECT_LOCAL_OFFICIAL_MISMATCH", 0)),
        "missing_local_completed_count": int(status_counts.get("WARN_MISSING_LOCAL_COMPLETED_ROW", 0)),
        "missing_official_count": int(status_counts.get("WARN_MISSING_OFFICIAL_ROW", 0)),
        "rescheduled_outside_window_count": int(status_counts.get("INFO_RESCHEDULED_OR_OUTSIDE_WINDOW", 0)),
        "source_error_count": len(source_errors),
        "status_counts": dict(status_counts),
        "root_cause_counts": dict(root_counts),
    }
    return rows, source_errors, summary


def _lineage_summary_rows(
    *,
    generated_at: str,
    as_of_date: date,
    sample_days: int,
    mode: str,
    context_summary: dict[str, Any],
    parity_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "generated_at": generated_at,
            "as_of_date": as_of_date.isoformat(),
            "mode": mode,
            "sample_days": int(sample_days),
            "sample_start_date": parity_summary.get("sample_start_date", ""),
            "sample_end_date": parity_summary.get("sample_end_date", ""),
            "offense_context_as_of_date": context_summary.get("offense_context_as_of_date", ""),
            "offense_window_excludes_eval_date": context_summary.get("offense_window_excludes_eval_date", ""),
            "max_local_source_game_date_used": context_summary.get("max_local_source_game_date_used", ""),
            "context_date_guard_status": context_summary.get("status", ""),
            "parity_rows": parity_summary.get("rows", 0),
            "pass_count": parity_summary.get("pass_count", 0),
            "warn_count": parity_summary.get("warn_count", 0),
            "info_count": parity_summary.get("info_count", 0),
            "team_hits_mismatch_count": parity_summary.get("direct_mismatch_count", 0),
            "missing_local_count": parity_summary.get("missing_local_completed_count", 0),
            "rescheduled_outside_window_count": parity_summary.get("rescheduled_outside_window_count", 0),
            "source_error_count": parity_summary.get("source_error_count", 0),
        }
    ]


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = payload["counts"]
    lineage = payload["lineage_summary"]
    lines = [
        "# Offense Factor Health Summary",
        "",
        f"- Generated at: `{payload['generated_at']}`",
        f"- As-of date: `{payload['as_of_date']}`",
        f"- Mode: `{payload['mode']}`",
        f"- Health status: `{payload['health_status']}`",
        f"- Sample dates: `{payload['sample_start_date']}` through `{payload['sample_end_date']}`",
        "",
        "## Counts",
        "",
        f"- PASS rows: `{counts.get('PASS', 0)}`",
        f"- WARN rows: `{counts.get('WARN', 0)}`",
        f"- INFO rows: `{counts.get('INFO', 0)}`",
        f"- Direct local-vs-official team-hit mismatches: `{lineage.get('team_hits_mismatch_count', 0)}`",
        f"- Missing local completed rows: `{lineage.get('missing_local_count', 0)}`",
        f"- Rescheduled/outside-window rows: `{lineage.get('rescheduled_outside_window_count', 0)}`",
        "",
        "## Context Date Guard",
        "",
        f"- Eval date: `{payload.get('eval_date') or 'not_provided'}`",
        f"- Offense context as-of date: `{payload['offense_context_as_of_date']}`",
        f"- Guard status: `{payload['context_date_guard']['status']}`",
        f"- Guard reason: `{payload['context_date_guard']['reason']}`",
        f"- Max local source game date used: `{payload['context_date_guard']['max_local_source_game_date_used']}`",
        "",
        "Same-day leakage risk is guarded in this report by making the inclusive date invariant explicit. "
        "When `eval_date` is provided, the report marks `FAIL` if `offense_context_as_of_date >= eval_date`. "
        "In `warn_only` mode the utility still exits successfully and does not alter production behavior.",
        "",
        "## Outputs",
        "",
    ]
    for key, value in payload["outputs"].items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## No Behavior Changed",
            "",
            "This utility is read-only and WARN-only. It writes health artifacts only. It does not write to the database, change schemas, formulas, tiers, uploads, selectors, models, grading, or production environment generation.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    generated_at = _utc_now()
    as_of = _parse_date(args.as_of_date)
    eval_date = _parse_date(args.eval_date) if args.eval_date else None
    if args.offense_context_as_of_date:
        offense_context_as_of_date = _parse_date(args.offense_context_as_of_date)
    elif eval_date is not None:
        offense_context_as_of_date = eval_date - timedelta(days=1)
    else:
        offense_context_as_of_date = as_of

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sample_dates = _latest_local_completed_dates(as_of, int(args.sample_days))
    context_rows, context_summary = _context_date_check(
        as_of_date=as_of,
        eval_date=eval_date,
        offense_context_as_of_date=offense_context_as_of_date,
    )
    context_summary["offense_context_as_of_date"] = offense_context_as_of_date.isoformat()
    parity_rows, source_errors, parity_summary = _parity_trace(sample_dates)
    if source_errors:
        parity_summary["source_errors"] = source_errors

    counts = Counter(row.get("classification", "") for row in parity_rows)
    health_status = "pass"
    if context_summary.get("status") == "FAIL":
        health_status = "fail"
    elif int(parity_summary.get("warn_count") or 0) > 0 or int(parity_summary.get("source_error_count") or 0) > 0:
        health_status = "warn"

    lineage_rows = _lineage_summary_rows(
        generated_at=generated_at,
        as_of_date=as_of,
        sample_days=int(args.sample_days),
        mode=str(args.mode),
        context_summary=context_summary,
        parity_summary=parity_summary,
    )
    lineage_summary = dict(lineage_rows[0])

    date_suffix = "2026-07-05"
    md_path = output_dir / f"offense_factor_health_summary_{date_suffix}.md"
    parity_path = output_dir / f"offense_factor_team_hits_parity_trace_{date_suffix}.csv"
    context_path = output_dir / f"offense_factor_context_date_check_{date_suffix}.csv"
    lineage_path = output_dir / f"offense_factor_health_lineage_summary_{date_suffix}.csv"
    json_path = output_dir / f"offense_factor_health_summary_{date_suffix}.json"

    _write_csv(
        parity_path,
        parity_rows,
        [
            "sample_start_date",
            "sample_end_date",
            "game_id",
            "team",
            "local_game_date",
            "game_info_game_date",
            "statsapi_official_date",
            "statsapi_schedule_query_dates_seen",
            "local_team_hits",
            "statsapi_team_hits",
            "hit_diff_local_minus_statsapi",
            "local_player_rows",
            "local_players_with_hits",
            "game_status",
            "classification",
            "parity_status",
            "root_cause_hint",
            "statsapi_boxscore_url",
        ],
    )
    _write_csv(
        context_path,
        context_rows,
        [
            "as_of_date",
            "eval_date",
            "offense_context_as_of_date",
            "offense_window_excludes_eval_date",
            "max_local_source_game_date_used",
            "status",
            "severity",
            "reason",
        ],
    )
    _write_csv(
        lineage_path,
        lineage_rows,
        [
            "generated_at",
            "as_of_date",
            "mode",
            "sample_days",
            "sample_start_date",
            "sample_end_date",
            "offense_context_as_of_date",
            "offense_window_excludes_eval_date",
            "max_local_source_game_date_used",
            "context_date_guard_status",
            "parity_rows",
            "pass_count",
            "warn_count",
            "info_count",
            "team_hits_mismatch_count",
            "missing_local_count",
            "rescheduled_outside_window_count",
            "source_error_count",
        ],
    )

    payload = {
        "generated_at": generated_at,
        "as_of_date": as_of.isoformat(),
        "eval_date": "" if eval_date is None else eval_date.isoformat(),
        "mode": str(args.mode),
        "health_status": health_status,
        "sample_dates": [d.isoformat() for d in sample_dates],
        "sample_start_date": parity_summary.get("sample_start_date", ""),
        "sample_end_date": parity_summary.get("sample_end_date", ""),
        "offense_context_as_of_date": offense_context_as_of_date.isoformat(),
        "context_date_guard": context_summary,
        "parity_summary": parity_summary,
        "counts": dict(counts),
        "lineage_summary": lineage_summary,
        "outputs": {
            "summary_md": _rel(md_path),
            "summary_json": _rel(json_path),
            "parity_trace_csv": _rel(parity_path),
            "context_date_check_csv": _rel(context_path),
            "lineage_summary_csv": _rel(lineage_path),
        },
    }
    _write_json(json_path, payload)
    _write_markdown(md_path, payload)

    print(
        "[offense-factor-health] "
        f"as_of={as_of.isoformat()} status={health_status} "
        f"pass={counts.get('PASS', 0)} warn={counts.get('WARN', 0)} info={counts.get('INFO', 0)} "
        f"out={_rel(md_path)}"
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of-date", required=True)
    parser.add_argument("--sample-days", type=int, default=7)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--mode", choices=["warn_only"], default="warn_only")
    parser.add_argument("--eval-date", default="")
    parser.add_argument("--offense-context-as-of-date", default="")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
