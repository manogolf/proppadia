#!/usr/bin/env python3
"""Backfill actual MLB hitter plate appearance components from StatsAPI.

This script is intentionally schema-aware and progress-safe:
- dry-run mode fetches/parses/validates without writing;
- write mode refuses to run until the reviewed PA columns exist;
- updates are scoped to mlb.player_stats rows keyed by player_id + game_id;
- rolling PA refresh only runs when reviewed player_derived_stats columns exist.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from backend.shared.db.pg import pg_connect


ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/mlb/research_gap_analysis"
VALIDATION_MD = OUT_DIR / "pa_backfill_validation.md"
VALIDATION_JSON = OUT_DIR / "pa_backfill_validation.json"
VALIDATION_CSV = OUT_DIR / "pa_backfill_validation_rows.csv"
STATSAPI = "https://statsapi.mlb.com/api/v1"
FINAL_STATUSES = {"F", "O", "D"}
IN_SEASON_GAME_TYPES = {"R", "P", "F", "D", "L", "W"}

PLAYER_STATS_PA_COLUMNS = [
    "plate_appearances",
    "hit_by_pitch",
    "sacrifice_flies",
    "sacrifice_hits",
    "catcher_interference",
    "pa_source",
    "pa_backfilled_at",
]
PLAYER_DERIVED_PA_COLUMNS = [
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
]


@dataclass
class PaRow:
    game_date: str
    game_id: int
    player_id: int
    player_name: str
    team: str
    opponent: str
    at_bats: int | None
    walks: int | None
    hit_by_pitch: int | None
    sacrifice_flies: int | None
    sacrifice_hits: int | None
    catcher_interference: int | None
    plate_appearances_direct: int | None
    plate_appearances_formula: int | None
    plate_appearances: int | None
    formula_matches_direct: bool | None
    used_direct_pa: bool
    missing_components: str
    existing_player_stats_row: bool = False
    existing_plate_appearances: int | None = None


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def _iter_dates(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _num0(value: int | None) -> int:
    return int(value or 0)


def _get_json(url: str, *, timeout: int, retries: int, sleep_seconds: float) -> tuple[dict[str, Any], str]:
    last_error = ""
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 404:
                return {}, "http_404"
            resp.raise_for_status()
            payload = resp.json()
            return payload if isinstance(payload, dict) else {}, ""
        except Exception as exc:
            last_error = f"{type(exc).__name__}:{exc}"
            if attempt < retries:
                time.sleep(sleep_seconds)
    return {}, last_error or "fetch_failed"


def _fetch_schedule(day: str, args: argparse.Namespace) -> tuple[list[dict[str, Any]], str]:
    url = f"{STATSAPI}/schedule?sportId=1&date={day}"
    payload, error = _get_json(url, timeout=args.timeout, retries=args.retries, sleep_seconds=args.sleep_seconds)
    if error:
        return [], error
    dates = payload.get("dates") or []
    if not dates:
        return [], ""
    return (dates[0] or {}).get("games", []) or [], ""


def _fetch_boxscore(game_id: int, args: argparse.Namespace) -> tuple[dict[str, Any], str]:
    url = f"{STATSAPI}/game/{int(game_id)}/boxscore"
    return _get_json(url, timeout=args.timeout, retries=args.retries, sleep_seconds=args.sleep_seconds)


def _is_final_game(game: dict[str, Any], *, require_regular_season: bool) -> bool:
    status = ((game.get("status") or {}).get("codedGameState") or "").upper()
    if status not in FINAL_STATUSES:
        return False
    if require_regular_season:
        return str(game.get("gameType") or "").upper() == "R"
    return str(game.get("gameType") or "").upper() in IN_SEASON_GAME_TYPES


def _team_meta(box: dict[str, Any], side: str) -> dict[str, str]:
    team = (((box.get("teams") or {}).get(side) or {}).get("team") or {})
    return {
        "abbr": _clean(team.get("abbreviation") or team.get("teamCode")).upper(),
        "name": _clean(team.get("name")),
    }


def _first_int(batting: dict[str, Any], keys: list[str]) -> tuple[int | None, str]:
    for key in keys:
        if key in batting and batting.get(key) is not None:
            return _to_int(batting.get(key)), key
    return None, ""


def _extract_pa_rows(box: dict[str, Any], game_id: int, game_date: str) -> tuple[list[PaRow], Counter[str]]:
    rows: list[PaRow] = []
    counters: Counter[str] = Counter()
    home = _team_meta(box, "home")
    away = _team_meta(box, "away")
    teams = box.get("teams") or {}
    for side in ("home", "away"):
        team = home if side == "home" else away
        opp = away if side == "home" else home
        players = ((teams.get(side) or {}).get("players") or {})
        for pdata in players.values():
            person = pdata.get("person") or {}
            pid = _to_int(person.get("id"))
            if pid is None:
                continue
            batting = ((pdata.get("stats") or {}).get("batting") or {})
            if not batting:
                counters["no_batting_stats"] += 1
                continue
            ab, _ = _first_int(batting, ["atBats", "at_bats"])
            bb, _ = _first_int(batting, ["baseOnBalls", "walks"])
            hbp, hbp_key = _first_int(batting, ["hitByPitch", "hit_by_pitch", "hbp"])
            sf, sf_key = _first_int(batting, ["sacFlies", "sacrificeFlies", "sacrifice_flies", "sf"])
            sh, sh_key = _first_int(batting, ["sacBunts", "sacrificeBunts", "sacrificeHits", "sacrifice_hits", "sh"])
            ci, ci_key = _first_int(
                batting,
                [
                    "catcherInterference",
                    "catchersInterference",
                    "reachedOnInterference",
                    "reached_on_interference",
                    "catcher_interference",
                ],
            )
            direct, direct_key = _first_int(batting, ["plateAppearances", "plate_appearances"])
            for component, key in [("hit_by_pitch", hbp_key), ("sacrifice_flies", sf_key), ("sacrifice_hits", sh_key), ("catcher_interference", ci_key), ("plate_appearances_direct", direct_key)]:
                counters[f"{component}_key:{key or 'missing'}"] += 1

            missing_components = []
            if ab is None:
                missing_components.append("at_bats")
            if bb is None:
                missing_components.append("walks")
            # HBP/SF/SH/CI default to zero when absent; StatsAPI often omits zero-valued keys.
            if ab is None or bb is None:
                formula = None
            else:
                formula = _num0(ab) + _num0(bb) + _num0(hbp) + _num0(sf) + _num0(sh) + _num0(ci)
            if direct is not None:
                pa = direct
                used_direct = True
            else:
                pa = formula
                used_direct = False
            formula_matches = None if direct is None or formula is None else int(direct) == int(formula)
            if formula_matches is False:
                counters["formula_direct_mismatch"] += 1
            rows.append(
                PaRow(
                    game_date=game_date,
                    game_id=int(game_id),
                    player_id=int(pid),
                    player_name=_clean(person.get("fullName")),
                    team=team["abbr"] or team["name"],
                    opponent=opp["abbr"] or opp["name"],
                    at_bats=ab,
                    walks=bb,
                    hit_by_pitch=_num0(hbp),
                    sacrifice_flies=_num0(sf),
                    sacrifice_hits=_num0(sh),
                    catcher_interference=_num0(ci),
                    plate_appearances_direct=direct,
                    plate_appearances_formula=formula,
                    plate_appearances=pa,
                    formula_matches_direct=formula_matches,
                    used_direct_pa=used_direct,
                    missing_components=",".join(missing_components),
                )
            )
    return rows, counters


def _table_columns(conn, table_name: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'mlb'
              AND table_name = %s
            """,
            (table_name,),
        )
        return {str(r["column_name"]) for r in cur.fetchall() or []}


def _existing_player_stats(conn, rows: list[PaRow]) -> dict[tuple[int, int], int | None]:
    if not rows:
        return {}
    keys = [(r.player_id, r.game_id) for r in rows]
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH wanted(player_id, game_id) AS (
                SELECT * FROM unnest(%s::bigint[], %s::bigint[])
            )
            SELECT ps.player_id, ps.game_id, ps.plate_appearances
            FROM mlb.player_stats ps
            JOIN wanted w
              ON w.player_id = ps.player_id
             AND w.game_id = ps.game_id
            """,
            ([p for p, _ in keys], [g for _, g in keys]),
        )
        return {(int(r["player_id"]), int(r["game_id"])): _to_int(r.get("plate_appearances")) for r in cur.fetchall() or []}


def _update_player_stats_pa(conn, rows: list[PaRow], *, source: str, only_missing_pa: bool = False) -> int:
    if not rows:
        return 0
    missing_clause = "AND ps.plate_appearances IS NULL" if only_missing_pa else ""
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            UPDATE mlb.player_stats ps
               SET plate_appearances = %s,
                   hit_by_pitch = %s,
                   sacrifice_flies = %s,
                   sacrifice_hits = %s,
                   catcher_interference = %s,
                   pa_source = %s,
                   pa_backfilled_at = now()
             WHERE ps.player_id = %s
               AND ps.game_id = %s
               AND COALESCE(ps.position, '') <> 'P'
               {missing_clause}
            """,
            [
                (
                    r.plate_appearances,
                    r.hit_by_pitch,
                    r.sacrifice_flies,
                    r.sacrifice_hits,
                    r.catcher_interference,
                    source,
                    r.player_id,
                    r.game_id,
                )
                for r in rows
                if r.plate_appearances is not None
            ],
        )
        return int(cur.rowcount or 0)


def _refresh_rolling_pa(conn, start: str, end: str) -> int:
    sql = """
        WITH target_players AS (
            SELECT DISTINCT player_id
            FROM mlb.player_stats
            WHERE game_date >= %s::date
              AND game_date <= %s::date
        ),
        daily AS (
            SELECT
               ps.player_id,
               MAX(ps.game_id)::bigint AS game_id,
               ps.game_date::date AS game_date,
               SUM(COALESCE(ps.plate_appearances, 0))::numeric AS plate_appearances
            FROM mlb.player_stats ps
            JOIN target_players tp
              ON tp.player_id = ps.player_id
            WHERE COALESCE(ps.position, '') <> 'P'
            GROUP BY ps.player_id, ps.game_date
        ),
        rolled AS (
            SELECT
             d.player_id,
             d.game_date,
             AVG(d.plate_appearances) OVER w7 AS d7_plate_appearances,
             AVG(d.plate_appearances) OVER w15 AS d15_plate_appearances,
             AVG(d.plate_appearances) OVER w30 AS d30_plate_appearances
            FROM daily d
            WINDOW
              w7 AS (PARTITION BY d.player_id ORDER BY d.game_date, d.game_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
              w15 AS (PARTITION BY d.player_id ORDER BY d.game_date, d.game_id ROWS BETWEEN 14 PRECEDING AND CURRENT ROW),
              w30 AS (PARTITION BY d.player_id ORDER BY d.game_date, d.game_id ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        ),
        target AS (
            SELECT *
            FROM rolled
            WHERE game_date >= %s::date
              AND game_date <= %s::date
        ),
        upd AS (
            UPDATE mlb.player_derived_stats pds
               SET d7_plate_appearances = target.d7_plate_appearances,
                   d15_plate_appearances = target.d15_plate_appearances,
                   d30_plate_appearances = target.d30_plate_appearances,
                   updated_at = now()
              FROM target
             WHERE pds.player_id = target.player_id
               AND pds.game_date = target.game_date
            RETURNING 1
        )
        SELECT COUNT(*)::int AS n FROM upd
    """
    with conn.cursor() as cur:
        cur.execute(sql, (start, end, start, end))
        row = cur.fetchone()
        return int((row or {}).get("n") or 0)


def _coverage_before_after(conn, start: str, end: str) -> dict[str, Any]:
    cols = _table_columns(conn, "player_stats")
    pa_expr = "plate_appearances" if "plate_appearances" in cols else "NULL::integer"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              COUNT(*)::int AS batter_rows,
              MIN(game_date)::text AS min_date,
              MAX(game_date)::text AS max_date,
              COUNT(*) FILTER (WHERE {pa_expr} IS NOT NULL)::int AS pa_rows,
              COUNT(*) FILTER (WHERE {pa_expr} IS NULL)::int AS pa_null_rows,
              ROUND(100.0 * COUNT(*) FILTER (WHERE {pa_expr} IS NULL) / NULLIF(COUNT(*),0), 3)::float AS pa_null_pct
            FROM mlb.player_stats
            WHERE game_date >= %s::date
              AND game_date <= %s::date
              AND COALESCE(position, '') <> 'P'
            """,
            (start, end),
        )
        return dict(cur.fetchone() or {})


def _write_validation(rows: list[PaRow], summary: dict[str, Any]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([asdict(r) for r in rows])
    if not df.empty:
        df["ab_bb_proxy"] = pd.to_numeric(df["at_bats"], errors="coerce").fillna(0) + pd.to_numeric(df["walks"], errors="coerce").fillna(0)
        df["true_pa_minus_ab_bb"] = pd.to_numeric(df["plate_appearances"], errors="coerce") - df["ab_bb_proxy"]
        df.to_csv(VALIDATION_CSV, index=False)
    else:
        pd.DataFrame().to_csv(VALIDATION_CSV, index=False)
    VALIDATION_JSON.write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")

    diff_desc = {}
    if not df.empty and "true_pa_minus_ab_bb" in df.columns:
        diff = pd.to_numeric(df["true_pa_minus_ab_bb"], errors="coerce").dropna()
        if not diff.empty:
            diff_desc = {
                "min": float(diff.min()),
                "p25": float(diff.quantile(0.25)),
                "median": float(diff.median()),
                "p75": float(diff.quantile(0.75)),
                "max": float(diff.max()),
                "nonzero_rows": int(diff.ne(0).sum()),
            }
    def _md_table(table: pd.DataFrame) -> str:
        if table.empty:
            return "No rows."
        work = table.fillna("").astype(str)
        lines = [
            "| " + " | ".join(work.columns) + " |",
            "| " + " | ".join(["---"] * len(work.columns)) + " |",
        ]
        for _, r in work.iterrows():
            lines.append("| " + " | ".join(str(r[c]).replace("|", "\\|") for c in work.columns) + " |")
        return "\n".join(lines)

    sample_cols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "at_bats",
        "walks",
        "hit_by_pitch",
        "sacrifice_flies",
        "sacrifice_hits",
        "catcher_interference",
        "plate_appearances_direct",
        "plate_appearances_formula",
        "plate_appearances",
        "true_pa_minus_ab_bb",
        "formula_matches_direct",
        "existing_player_stats_row",
    ]
    sample = df[[c for c in sample_cols if c in df.columns]].head(20) if not df.empty else pd.DataFrame()
    lines = [
        "# PA Backfill Validation",
        "",
        "Foundation/data integrity only. No production model logic, thresholds, retraining, uploads, or filters changed.",
        "",
        "## Run Summary",
        "",
    ]
    for key, value in summary.items():
        if key in {"component_key_counts", "missing_component_counts", "warnings", "outputs"}:
            continue
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## AB+BB Proxy vs True PA",
            "",
            *(f"- {k}: `{v}`" for k, v in diff_desc.items()),
            "",
            "## Component Key Counts",
            "",
            "```json",
            json.dumps(summary.get("component_key_counts", {}), indent=2, sort_keys=True),
            "```",
            "",
            "## Missing Component Counts",
            "",
            "```json",
            json.dumps(summary.get("missing_component_counts", {}), indent=2, sort_keys=True),
            "```",
            "",
            "## Sample Rows",
            "",
            _md_table(sample),
            "",
            "## Outputs",
            "",
            f"- `{VALIDATION_CSV.relative_to(ROOT)}`",
            f"- `{VALIDATION_JSON.relative_to(ROOT)}`",
            f"- `{VALIDATION_MD.relative_to(ROOT)}`",
        ]
    )
    VALIDATION_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    start = args.start_date.isoformat()
    end = args.end_date.isoformat()
    if args.start_date > args.end_date:
        raise SystemExit("--start-date must be <= --end-date")

    all_rows: list[PaRow] = []
    component_counts: Counter[str] = Counter()
    schedule_errors: dict[str, str] = {}
    boxscore_errors: dict[str, str] = {}
    games_processed = 0
    games_seen = 0
    for day in _iter_dates(args.start_date, args.end_date):
        day_s = day.isoformat()
        games, error = _fetch_schedule(day_s, args)
        if error:
            schedule_errors[day_s] = error
            continue
        final_games = [g for g in games if _is_final_game(g, require_regular_season=args.require_regular_season)]
        for game in final_games:
            if args.limit_games and games_processed >= args.limit_games:
                break
            gid = _to_int(game.get("gamePk"))
            if gid is None:
                continue
            games_seen += 1
            box, box_error = _fetch_boxscore(gid, args)
            if box_error or not box:
                boxscore_errors[str(gid)] = box_error or "empty_boxscore"
                continue
            rows, counters = _extract_pa_rows(box, gid, day_s)
            all_rows.extend(rows)
            component_counts.update(counters)
            games_processed += 1
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
        if args.limit_games and games_processed >= args.limit_games:
            break

    writes_applied = 0
    rolling_rows_updated = 0
    coverage_before: dict[str, Any] = {}
    coverage_after: dict[str, Any] = {}
    schema_missing: dict[str, list[str]] = {}
    existing_count = 0
    with pg_connect() as conn:
        player_cols = _table_columns(conn, "player_stats")
        derived_cols = _table_columns(conn, "player_derived_stats")
        missing_player = [c for c in PLAYER_STATS_PA_COLUMNS if c not in player_cols]
        missing_derived = [c for c in PLAYER_DERIVED_PA_COLUMNS if c not in derived_cols]
        if missing_player:
            schema_missing["player_stats"] = missing_player
        if missing_derived:
            schema_missing["player_derived_stats"] = missing_derived
        coverage_before = _coverage_before_after(conn, start, end)
        if not missing_player:
            existing = _existing_player_stats(conn, all_rows)
            for row in all_rows:
                key = (row.player_id, row.game_id)
                row.existing_player_stats_row = key in existing
                row.existing_plate_appearances = existing.get(key)
            existing_count = sum(1 for row in all_rows if row.existing_player_stats_row)
        else:
            # Without columns, still mark whether core rows exist.
            with conn.cursor() as cur:
                if all_rows:
                    cur.execute(
                        """
                        WITH wanted(player_id, game_id) AS (
                            SELECT * FROM unnest(%s::bigint[], %s::bigint[])
                        )
                        SELECT ps.player_id, ps.game_id
                        FROM mlb.player_stats ps
                        JOIN wanted w
                          ON w.player_id = ps.player_id
                         AND w.game_id = ps.game_id
                        """,
                        ([r.player_id for r in all_rows], [r.game_id for r in all_rows]),
                    )
                    existing_keys = {(int(r["player_id"]), int(r["game_id"])) for r in cur.fetchall() or []}
                else:
                    existing_keys = set()
            for row in all_rows:
                row.existing_player_stats_row = (row.player_id, row.game_id) in existing_keys
            existing_count = sum(1 for row in all_rows if row.existing_player_stats_row)

        if not args.dry_run:
            if missing_player:
                raise SystemExit(
                    "PA columns are missing; review/apply the SQL in pa_storage_plan.md before write mode. "
                    f"Missing: {schema_missing}"
                )
            valid_rows = [r for r in all_rows if r.existing_player_stats_row and r.plate_appearances is not None]
            for idx in range(0, len(valid_rows), max(1, args.batch_size)):
                writes_applied += _update_player_stats_pa(
                    conn,
                    valid_rows[idx : idx + args.batch_size],
                    source=args.source,
                    only_missing_pa=args.only_missing_pa,
                )
                conn.commit()
            if not missing_derived and not args.skip_rolling:
                rolling_rows_updated = _refresh_rolling_pa(conn, start, end)
                conn.commit()
        coverage_after = _coverage_before_after(conn, start, end)

    missing_component_counts = Counter()
    for row in all_rows:
        for comp in filter(None, row.missing_components.split(",")):
            missing_component_counts[comp] += 1
    formula_mismatch = sum(1 for row in all_rows if row.formula_matches_direct is False)
    summary = {
        "start_date": start,
        "end_date": end,
        "dry_run": bool(args.dry_run),
        "only_missing_pa": bool(args.only_missing_pa),
        "source": args.source,
        "games_seen": games_seen,
        "games_processed": games_processed,
        "batter_pa_rows_parsed": len(all_rows),
        "existing_player_stats_rows_matched": existing_count,
        "writes_applied": writes_applied,
        "rolling_rows_updated": rolling_rows_updated,
        "direct_pa_rows": sum(1 for row in all_rows if row.plate_appearances_direct is not None),
        "formula_pa_rows": sum(1 for row in all_rows if row.plate_appearances_formula is not None),
        "formula_direct_mismatch_rows": formula_mismatch,
        "schema_missing": schema_missing,
        "coverage_before": coverage_before,
        "coverage_after": coverage_after,
        "schedule_error_dates": schedule_errors,
        "boxscore_error_games": boxscore_errors,
        "component_key_counts": dict(component_counts),
        "missing_component_counts": dict(missing_component_counts),
        "outputs": {
            "validation_md": str(VALIDATION_MD),
            "validation_json": str(VALIDATION_JSON),
            "validation_csv": str(VALIDATION_CSV),
        },
    }
    _write_validation(all_rows, summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill MLB actual hitter PA components from StatsAPI boxscores.")
    parser.add_argument("--start-date", required=True, type=_parse_date)
    parser.add_argument("--end-date", required=True, type=_parse_date)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only-missing-pa", action="store_true")
    parser.add_argument("--limit-games", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--source", choices=["statsapi"], default="statsapi")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--require-regular-season", action="store_true")
    parser.add_argument("--skip-rolling", action="store_true")
    return parser.parse_args()


def main() -> int:
    summary = run(parse_args())
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
