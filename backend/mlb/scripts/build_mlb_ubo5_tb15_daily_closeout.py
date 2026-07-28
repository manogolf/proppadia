#!/usr/bin/env python3
"""Build a revisioned UBO-5 TB 1.5 observation closeout."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import implied, number
from backend.mlb.scripts.build_mlb_ubo5_tb15_provisional_tracker import market_rows

ROOT = Path(__file__).resolve().parents[3]
IDENTITY = ["slate_date", "game_pk", "batter_mlb_id", "prop_type", "line"]
CLOSEOUT_FIELDS = [
    "slate_date", "game_pk", "batter_mlb_id", "player_name", "game",
    "morning_run_tag", "morning_ubo5_status", "morning_lineup_status",
    "eventual_starting_status", "confirmed_batting_order", "first_confirmed_run_tag",
    "exact_ubo5_over_probability", "first_positive_edge_timestamp",
    "first_positive_over_edge_pp", "maximum_positive_over_edge_pp",
    "final_pregame_over_edge_pp", "final_betonline_over_price",
    "final_betonline_under_price", "market_disappeared_flag", "intraday_edge_status",
    "total_bases", "over_15_result", "outcome_status", "closeout_status",
]
RECORD_FIELDS = [
    "slate_date", "closeout_revision", "generated_at_utc", "morning_market_count",
    "morning_confirm_count", "morning_likely_confirm_count", "morning_pass_count",
    "morning_wait_count", "confirm_likely_eventual_starters",
    "confirm_likely_nonstarters", "confirmed_positive_edge_count",
    "final_pregame_positive_edge_count", "wins", "losses", "voids", "unresolved",
    "win_rate", "edges_grew", "edges_persisted", "edges_shrank_positive",
    "edges_disappeared", "markets_removed", "closeout_status", "source_run_tags",
    "is_current", "source_fingerprint",
]
AUDIT_FIELDS = [
    "slate_date", "game_pk", "player_name", "batter_mlb_id", "game",
    "morning_ubo5_status", "eventual_starting_status", "confirmed_batting_order",
    "final_betonline_over_price", "first_positive_edge_timestamp",
    "final_pregame_over_edge_pp", "game_final_status", "player_appeared",
    "plate_appearances", "at_bats", "singles", "doubles", "triples", "home_runs",
    "calculated_total_bases", "existing_outcome_value", "existing_outcome_status",
    "current_closeout_status", "unresolved_reason", "recovered_classification",
]


def read_rows(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def key(row: dict, date: str) -> tuple[str, int, int, str, float]:
    return (
        date, int(float(row.get("game_pk") or row.get("game_id"))),
        int(float(row.get("batter_mlb_id") or row.get("player_id"))),
        "total_bases", 1.5,
    )


def clean_status(value: str) -> str:
    value = (value or "").replace("_", " ").strip().upper()
    return {
        "LIKELY CONFIRM IF STARTING": "LIKELY CONFIRM IF STARTING",
        "WAIT FOR LINEUP": "WAIT FOR LINEUP",
    }.get(value, value)


def run_tag(path: Path) -> str:
    match = re.search(r"(local_daily_\d{8}T\d{6}Z)", path.name)
    return match.group(1) if match else ""


def run_time(tag: str) -> pd.Timestamp:
    return pd.to_datetime(tag.removeprefix("local_daily_"), format="%Y%m%dT%H%M%SZ", utc=True)


def select_morning_audit(day_dir: Path) -> tuple[Path, str]:
    files = list(day_dir.glob("ubo5_tb15_prelineup_confirmation_audit_*.csv"))
    if not files:
        raise FileNotFoundError(f"no pre-lineup audit under {day_dir}")
    earliest = min(run_tag(p) for p in files if run_tag(p))
    same = [p for p in files if run_tag(p) == earliest]
    hybrid = [p for p in same if "hybrid_validation" in p.name]
    return (hybrid[0] if hybrid else same[0]), earliest


def load_transitions(day_dir: Path, date: str) -> tuple[dict, dict, set[str]]:
    by_run: dict[str, dict] = {}
    lifecycle: dict[tuple, dict] = {}
    tags: set[str] = set()
    for path in sorted(day_dir.glob("ubo5_tb15_prelineup_confirmation_transitions_*.csv")):
        tag = run_tag(path)
        if not tag or "hybrid_validation" in path.name:
            continue
        tags.add(tag)
        per_run = by_run.setdefault(tag, {})
        for row in read_rows(path):
            ident = key(row, date)
            slot = number(row.get("confirmed_batting_order"))
            prob = number(row.get("confirmed_route_ubo5_probability"))
            per_run[ident] = {"slot": slot, "prob": prob}
            life = lifecycle.setdefault(ident, {"slots": [], "probs": [], "tags": [], "not_start": False})
            if slot is not None:
                life["slots"].append(slot)
                life["tags"].append(tag)
            if prob is not None:
                life["probs"].append((tag, prob))
            if row.get("transition_outcome") == "PLAYER_NOT_IN_STARTING_LINEUP":
                life["not_start"] = True
    return by_run, lifecycle, tags


def load_markets(date: str, odds_root: Path) -> tuple[dict, set[str]]:
    market_by_run: dict[str, dict] = {}
    tags: set[str] = set()
    date_dir = odds_root / date
    for odds_path in sorted(date_dir.glob("odds_mlb_playerprops__local_daily_*.json")):
        tag = run_tag(odds_path)
        wide_path = date_dir / f"mlb_predictions_wide_calibrated__{tag}.csv"
        if not wide_path.is_file():
            continue
        snapshot = json.loads(odds_path.read_text(encoding="utf-8"))
        matched, _ = market_rows(snapshot, pd.read_csv(wide_path))
        per_run = {}
        for row in matched:
            ident = key(row, date)
            over, under = number(row.get("over_price")), number(row.get("under_price"))
            oi, ui = implied(over), implied(under)
            if oi is None or ui is None or oi + ui <= 0:
                continue
            start = pd.to_datetime(row.get("game_time"), utc=True, errors="coerce")
            if pd.isna(start) or run_time(tag) >= start:
                continue
            per_run[ident] = {
                "over": over, "under": under, "novig": oi / (oi + ui),
                "timestamp": str(row.get("price_timestamp") or run_time(tag).isoformat()),
                "start": start,
            }
        market_by_run[tag] = per_run
        tags.add(tag)
    return market_by_run, tags


def load_outcomes(path: Path, date: str) -> dict:
    outcomes: dict[tuple, dict] = {}
    for row in read_rows(path):
        if row.get("prop_type") != "total_bases" or number(row.get("line")) != 1.5:
            continue
        ident = key(row, date)
        value = number(row.get("actual_value"))
        if value is None:
            outcomes.setdefault(ident, {
                "value": None, "conflict": False, "source": "RECONCILE", "stats": {},
            })
            continue
        prior = outcomes.get(ident)
        conflict = bool(prior and prior["value"] is not None and prior["value"] != value)
        outcomes[ident] = {
            "value": value, "conflict": conflict or bool(prior and prior["conflict"]),
            "source": "RECONCILE", "stats": {},
        }
    return outcomes


def load_certified_player_stats(date: str) -> tuple[dict, str]:
    """Read the canonical official-derived outcome table; fail open to reconciliation."""
    dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        return {}, "DATABASE_URL_UNAVAILABLE"
    try:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(dsn, row_factory=dict_row) as conn:
            rows = conn.execute(
                """
                SELECT game_id, player_id, plate_appearances, at_bats, hits,
                       singles, doubles, triples, home_runs, total_bases
                FROM mlb.player_stats
                WHERE game_date = %s
                  AND (plate_appearances IS NOT NULL OR at_bats IS NOT NULL
                       OR total_bases IS NOT NULL)
                """,
                (date,),
            ).fetchall()
    except Exception as exc:
        return {}, f"DATABASE_READ_FAILED:{type(exc).__name__}:{exc}"
    outcomes = {}
    for row in rows:
        stats = dict(row)
        parts = [number(stats.get(name)) for name in ("singles", "doubles", "triples", "home_runs")]
        calculated = None if any(value is None for value in parts) else (
            parts[0] + 2 * parts[1] + 3 * parts[2] + 4 * parts[3]
        )
        stored = number(stats.get("total_bases"))
        conflict = calculated is not None and stored is not None and calculated != stored
        value = calculated if calculated is not None else stored
        outcomes[(date, int(row["game_id"]), int(row["player_id"]), "total_bases", 1.5)] = {
            "value": value, "conflict": conflict, "source": "MLB_PLAYER_STATS",
            "stats": stats | {"calculated_total_bases": calculated},
        }
    return outcomes, "PASS"


def load_official_game_statuses(date: str, cache_path: Path) -> tuple[dict[int, str], str]:
    """Refresh official schedule status; retain a same-date cached response on failure."""
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
    payload = None
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "proppadia-ubo5-closeout/1"})
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        cache_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        source = "STATSAPI_REFRESH"
    except Exception as exc:
        if cache_path.is_file():
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            source = f"STATSAPI_SAME_DATE_CACHE_AFTER:{type(exc).__name__}"
        else:
            return {}, f"STATSAPI_UNAVAILABLE:{type(exc).__name__}:{exc}"
    statuses = {}
    for date_row in payload.get("dates", []):
        for game in date_row.get("games", []):
            status = game.get("status") or {}
            statuses[int(game["gamePk"])] = str(
                status.get("detailedState") or status.get("abstractGameState") or ""
            )
    return statuses, source


def edge_class(points: list[dict], later_run_without_market: bool) -> str:
    if not points:
        return ""
    positive = [p for p in points if p["edge"] > 0]
    if not positive:
        return ""
    first, final = positive[0], points[-1]
    if later_run_without_market:
        return "MARKET_REMOVED"
    if len(points) == 1:
        return "NO_LATER_PRICE"
    if final["edge"] <= 0:
        return "EDGE_DISAPPEARED"
    delta = final["edge"] - first["edge"]
    if abs(delta) < 0.005:
        return "EDGE_PERSISTED"
    return "EDGE_GREW" if delta > 0 else "EDGE_SHRANK_REMAINED_POSITIVE"


def fmt_pp(value: object) -> str:
    parsed = number(value)
    return "—" if parsed is None else f"{parsed:+.2f} pp"


def fmt_tb(value: object) -> str:
    parsed = number(value)
    return "—" if parsed is None else f"{parsed:g}"


def render_closeout(path: Path, summary: dict, rows: list[dict]) -> None:
    lines = [
        f"# UBO-5 TB 1.5 Broad Intraday Ever-Positive Closeout — {summary['slate_date']}",
        "",
        f"Status: **{summary['closeout_status']}**  ",
        f"Revision: **{summary['closeout_revision']}**  ",
        "Observation record only; listed players are not represented as user wagers.",
        "",
        "## Summary",
        "",
        f"- Morning BetOnline TB 1.5 markets: {summary['morning_market_count']}",
        f"- CONFIRM: {summary['morning_confirm_count']}",
        f"- LIKELY CONFIRM IF STARTING: {summary['morning_likely_confirm_count']}",
        f"- PASS: {summary['morning_pass_count']}",
        f"- WAIT FOR LINEUP: {summary['morning_wait_count']}",
        f"- CONFIRM/LIKELY players who started: {summary['confirm_likely_eventual_starters']}",
        f"- CONFIRM/LIKELY players who did not start: {summary['confirm_likely_nonstarters']}",
        f"- Confirmed positive-edge candidates: {summary['confirmed_positive_edge_count']}",
        f"- Final pregame positive-edge candidates: {summary['final_pregame_positive_edge_count']}",
        f"- Over 1.5 wins: {summary['wins']}",
        f"- Over 1.5 losses: {summary['losses']}",
        f"- Unresolved or void: {int(summary['unresolved']) + int(summary['voids'])}",
        "",
        "## Player closeout",
        "",
        "| Player | Game | Morning UBO-5 | Started | Batting | Confirmed edge | Final edge | Total bases | Result |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        started = {"STARTED": "Yes", "DID NOT START": "DID NOT START"}.get(
            row["eventual_starting_status"], "Unresolved"
        )
        lines.append(
            f"| {row['player_name']} | {row['game']} | {row['morning_ubo5_status']} | "
            f"{started} | {row['confirmed_batting_order'] or '—'} | "
            f"{fmt_pp(row['first_positive_over_edge_pp'])} | "
            f"{fmt_pp(row['final_pregame_over_edge_pp'])} | "
            f"{fmt_tb(row['total_bases'])} | {row['over_15_result']} |"
        )
    edge_rows = [row for row in rows if row["first_positive_edge_timestamp"]]
    lines.extend([
        "", "## Intraday confirmed-edge summary", "",
        "| Player | Game | First positive | Largest positive | At confirmation | Last pregame | Movement |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ])
    if not edge_rows:
        lines.append("| *None* | | | | | | |")
    for row in edge_rows:
        lines.append(
            f"| {row['player_name']} | {row['game']} | "
            f"{fmt_pp(row['first_positive_over_edge_pp'])} | "
            f"{fmt_pp(row['maximum_positive_over_edge_pp'])} | "
            f"{fmt_pp(row.get('_lineup_edge_pp'))} | "
            f"{fmt_pp(row['final_pregame_over_edge_pp'])} | "
            f"{row['intraday_edge_status']} |"
        )
    lines.extend(["", "## Source runs", "", summary["source_run_tags"], ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def render_record(path: Path, rows: list[dict]) -> None:
    lines = [
        "# UBO-5 Broad Positive-Edge Record", "",
        "Observation record only. Win rate excludes void, no-action, and unresolved rows.", "",
        "| Slate | Rev | Current | Status | Confirmed +edge | Final +edge | W | L | Void | Unresolved | Win rate |",
        "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            f"| {row['slate_date']} | {row['closeout_revision']} | {row['is_current']} | "
            f"{row['closeout_status']} | {row['confirmed_positive_edge_count']} | "
            f"{row['final_pregame_positive_edge_count']} | {row['wins']} | {row['losses']} | "
            f"{row['voids']} | {row['unresolved']} | {row['win_rate'] or '—'} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15")
    parser.add_argument("--odds-root", default="backend/mlb/exports/odds_history")
    parser.add_argument("--reconcile-csv", default="")
    args = parser.parse_args()
    date = args.date
    output_root, odds_root = Path(args.output_root), Path(args.odds_root)
    day_dir = output_root / date
    reconcile = Path(args.reconcile_csv) if args.reconcile_csv else (
        ROOT / f"artifacts/analysis/mlb/execution_vs_model/{date}/reconcile_rows.csv"
    )
    dated_csv = day_dir / f"ubo5_tb15_closeout_{date}.csv"
    previous_rows = read_rows(dated_csv)
    previous_unresolved = {
        key(row, date): row for row in previous_rows if row.get("over_15_result") == "UNRESOLVED"
    }
    audit_path, morning_tag = select_morning_audit(day_dir)
    morning = read_rows(audit_path)
    by_run, lifecycle, transition_tags = load_transitions(day_dir, date)
    markets, market_tags = load_markets(date, odds_root)
    reconcile_outcomes = load_outcomes(reconcile, date)
    outcomes = dict(reconcile_outcomes)
    certified_outcomes, certified_status = load_certified_player_stats(date)
    for ident, certified in certified_outcomes.items():
        existing = outcomes.get(ident)
        if existing and existing.get("value") is not None:
            certified["conflict"] = bool(
                certified["conflict"] or number(existing["value"]) != number(certified["value"])
            )
        outcomes[ident] = certified
    official_statuses, game_status_source = load_official_game_statuses(
        date, day_dir / f"ubo5_tb15_official_game_status_{date}.json"
    )
    all_tags = sorted(market_tags | transition_tags, key=run_time)

    rows = []
    for source in morning:
        ident = key(source, date)
        life = lifecycle.get(ident, {"slots": [], "probs": [], "tags": [], "not_start": False})
        slot = int(life["slots"][-1]) if life["slots"] else ""
        first_confirmed = min(life["tags"], key=run_time) if life["tags"] else ""
        exact_probs = life["probs"]
        exact_prob = exact_probs[-1][1] if exact_probs else None
        points = []
        current_prob = None
        for tag in all_tags:
            transition = by_run.get(tag, {}).get(ident, {})
            prob = transition.get("prob")
            if prob is not None:
                current_prob = prob
            market = markets.get(tag, {}).get(ident)
            if current_prob is not None and market:
                points.append({
                    "tag": tag, "timestamp": market["timestamp"], "prob": current_prob,
                    "edge": (current_prob - market["novig"]) * 100, **market,
                })
        positive = [point for point in points if point["edge"] > 0]
        first = positive[0] if positive else None
        final = points[-1] if points else None
        later_missing = False
        if final:
            later_missing = any(
                run_time(tag) > run_time(final["tag"])
                and run_time(tag) < final["start"]
                and ident not in markets.get(tag, {})
                for tag in all_tags
            )
        actual = outcomes.get(ident, {
            "value": None, "conflict": False, "source": "NONE", "stats": {},
        })
        game_is_final = official_statuses.get(ident[1], "").strip().casefold() in {
            "final", "game over",
        }
        outcome_is_settleable = actual.get("source") == "RECONCILE" or game_is_final
        started = "STARTED" if slot else ("DID NOT START" if life["not_start"] else "UNRESOLVED")
        # This record observes the positive-Over board; an exact route score
        # without a positive BetOnline edge is not an action in this ledger.
        action = bool(positive)
        if actual["conflict"]:
            result, outcome_status = "UNRESOLVED", "CONFLICTING_AUTHORITATIVE_OUTCOMES"
        elif action and actual["value"] is not None and outcome_is_settleable:
            result = "WIN" if actual["value"] >= 2 else "LOSS"
            outcome_status = "RESOLVED"
        elif started == "DID NOT START" and not action:
            result, outcome_status = "NO_ACTION", "NONSTARTER_NO_ACTION"
        elif not action:
            reason = "NO_POSITIVE_CONFIRMED_EDGE" if exact_probs else "EXACT_PRODUCTION_ROUTE_NOT_SCORED"
            result, outcome_status = "NO_ACTION", reason
        else:
            result, outcome_status = "UNRESOLVED", "OUTCOME_PENDING"
        rows.append({
            "slate_date": date, "game_pk": ident[1], "batter_mlb_id": ident[2],
            "player_name": source.get("player_name", ""), "game": source.get("game", ""),
            "morning_run_tag": morning_tag,
            "morning_ubo5_status": clean_status(source.get("hybrid_display_status") or source.get("provisional_classification")),
            "morning_lineup_status": source.get("lineup_status", ""),
            "eventual_starting_status": started, "confirmed_batting_order": slot,
            "first_confirmed_run_tag": first_confirmed,
            "exact_ubo5_over_probability": "" if exact_prob is None else f"{exact_prob:.10f}",
            "first_positive_edge_timestamp": first["timestamp"] if first else "",
            "first_positive_over_edge_pp": "" if not first else f"{first['edge']:.10f}",
            "maximum_positive_over_edge_pp": "" if not positive else f"{max(p['edge'] for p in positive):.10f}",
            "final_pregame_over_edge_pp": "" if not final else f"{final['edge']:.10f}",
            "final_betonline_over_price": "" if not final else final["over"],
            "final_betonline_under_price": "" if not final else final["under"],
            "market_disappeared_flag": str(later_missing).lower(),
            "intraday_edge_status": edge_class(points, later_missing),
            "_lineup_edge_pp": "" if not points else f"{points[0]['edge']:.10f}",
            "total_bases": "" if actual["value"] is None else actual["value"],
            "over_15_result": result, "outcome_status": outcome_status,
        })
    rows.sort(key=lambda r: (r["game"], r["player_name"], int(r["batter_mlb_id"])))

    actionable_unresolved = sum(r["over_15_result"] == "UNRESOLVED" for r in rows)
    overall_status = "FINAL" if actionable_unresolved == 0 else "PARTIAL_PENDING_OUTCOMES"
    for row in rows:
        row["closeout_status"] = overall_status

    counts = pd.Series([r["morning_ubo5_status"] for r in rows]).value_counts().to_dict()
    confirm_like = [r for r in rows if r["morning_ubo5_status"] in {"CONFIRM", "LIKELY CONFIRM IF STARTING"}]
    edge_rows = [r for r in rows if r["first_positive_edge_timestamp"]]
    wins = sum(r["over_15_result"] == "WIN" for r in rows)
    losses = sum(r["over_15_result"] == "LOSS" for r in rows)
    voids = sum(r["over_15_result"] == "VOID" for r in rows)
    fingerprint_payload = {
        "rows": [{k: v for k, v in row.items() if k != "closeout_status"} for row in rows],
        "runs": all_tags, "reconcile": str(reconcile),
        "certified_outcome_status": certified_status,
        "official_game_statuses": official_statuses,
    }
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    manifest_path = day_dir / "ubo5_tb15_closeout_current.json"
    prior = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
    if prior.get("closeout_status") == "FINAL" and overall_status != "FINAL":
        print(json.dumps({
            "slate_date": date,
            "closeout_revision": prior.get("closeout_revision"),
            "closeout_status": "FINAL",
            "attempted_status": overall_status,
            "decision": "PRESERVED_EXISTING_FINAL_SOURCE_DEGRADED",
            "certified_outcome_source": certified_status,
            "official_game_status_source": game_status_source,
        }, indent=2))
        return 0
    unchanged = prior.get("source_fingerprint") == fingerprint
    revision = int(prior.get("closeout_revision", 0)) if unchanged else int(prior.get("closeout_revision", 0)) + 1
    generated = prior.get("generated_at_utc") if unchanged else datetime.now(timezone.utc).isoformat()
    classes = [r["intraday_edge_status"] for r in edge_rows]
    summary = {
        "slate_date": date, "closeout_revision": revision, "generated_at_utc": generated,
        "morning_market_count": len(rows), "morning_confirm_count": counts.get("CONFIRM", 0),
        "morning_likely_confirm_count": counts.get("LIKELY CONFIRM IF STARTING", 0),
        "morning_pass_count": counts.get("PASS", 0),
        "morning_wait_count": counts.get("WAIT FOR LINEUP", 0),
        "confirm_likely_eventual_starters": sum(r["eventual_starting_status"] == "STARTED" for r in confirm_like),
        "confirm_likely_nonstarters": sum(r["eventual_starting_status"] == "DID NOT START" for r in confirm_like),
        "confirmed_positive_edge_count": len(edge_rows),
        "final_pregame_positive_edge_count": sum(number(r["final_pregame_over_edge_pp"]) is not None and number(r["final_pregame_over_edge_pp"]) > 0 for r in edge_rows),
        "wins": wins, "losses": losses, "voids": voids, "unresolved": actionable_unresolved,
        "win_rate": "" if wins + losses == 0 else f"{wins / (wins + losses):.6f}",
        "edges_grew": classes.count("EDGE_GREW"), "edges_persisted": classes.count("EDGE_PERSISTED"),
        "edges_shrank_positive": classes.count("EDGE_SHRANK_REMAINED_POSITIVE"),
        "edges_disappeared": classes.count("EDGE_DISAPPEARED"),
        "markets_removed": classes.count("MARKET_REMOVED"), "closeout_status": overall_status,
        "source_run_tags": "|".join(all_tags), "is_current": "true", "source_fingerprint": fingerprint,
    }

    dated_md = day_dir / f"ubo5_tb15_closeout_{date}.md"
    revision_dir = day_dir / "revisions" / f"revision_{revision:03d}"
    if not unchanged:
        write_rows(dated_csv, rows, CLOSEOUT_FIELDS)
        render_closeout(dated_md, summary, rows)
        revision_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(dated_csv, revision_dir / dated_csv.name)
        shutil.copy2(dated_md, revision_dir / dated_md.name)
        manifest_path.write_text(json.dumps({
            "slate_date": date, "closeout_revision": revision, "generated_at_utc": generated,
            "source_fingerprint": fingerprint, "closeout_status": overall_status,
        }, indent=2) + "\n", encoding="utf-8")

    audit_rows = []
    current_by_id = {key(row, date): row for row in rows}
    for ident, previous in sorted(previous_unresolved.items(), key=lambda item: (item[1]["game"], item[1]["player_name"])):
        current = current_by_id[ident]
        certified = certified_outcomes.get(ident, {})
        stats = certified.get("stats", {})
        existing = reconcile_outcomes.get(ident, {})
        appeared = number(stats.get("plate_appearances")) is not None and number(stats.get("plate_appearances")) > 0
        final_state = official_statuses.get(ident[1], "")
        recovered = current["over_15_result"]
        if certified.get("value") is not None and existing.get("value") is None:
            reason = "CLOSEOUT_FILTER_EXCLUDED_VALID_OUTCOME"
        elif certified.get("value") is None:
            reason = "PLAYER_OUTCOME_NOT_ACQUIRED"
        else:
            reason = "OTHER"
        audit_rows.append({
            "slate_date": date, "game_pk": ident[1], "player_name": current["player_name"],
            "batter_mlb_id": ident[2], "game": current["game"],
            "morning_ubo5_status": current["morning_ubo5_status"],
            "eventual_starting_status": current["eventual_starting_status"],
            "confirmed_batting_order": current["confirmed_batting_order"],
            "final_betonline_over_price": current["final_betonline_over_price"],
            "first_positive_edge_timestamp": current["first_positive_edge_timestamp"],
            "final_pregame_over_edge_pp": current["final_pregame_over_edge_pp"],
            "game_final_status": final_state, "player_appeared": str(bool(appeared)).lower(),
            "plate_appearances": stats.get("plate_appearances", ""),
            "at_bats": stats.get("at_bats", ""), "singles": stats.get("singles", ""),
            "doubles": stats.get("doubles", ""), "triples": stats.get("triples", ""),
            "home_runs": stats.get("home_runs", ""),
            "calculated_total_bases": stats.get("calculated_total_bases", ""),
            "existing_outcome_value": existing.get("value", ""),
            "existing_outcome_status": previous.get("outcome_status", ""),
            "current_closeout_status": overall_status, "unresolved_reason": reason,
            "recovered_classification": recovered,
        })
    if audit_rows:
        write_rows(
            day_dir / f"ubo5_tb15_unresolved_outcome_audit_{date}.csv",
            audit_rows, AUDIT_FIELDS,
        )

    record_dir = output_root / "daily_record"
    record_csv = record_dir / "ubo5_tb15_daily_record.csv"
    record_md = record_dir / "ubo5_tb15_daily_record.md"
    records = read_rows(record_csv)
    if not unchanged:
        for record in records:
            if record["slate_date"] == date:
                record["is_current"] = "false"
        records.append({k: summary.get(k, "") for k in RECORD_FIELDS})
        records.sort(key=lambda r: (r["slate_date"], int(r["closeout_revision"])))
        write_rows(record_csv, records, RECORD_FIELDS)
        render_record(record_md, records)
    elif not record_md.is_file():
        render_record(record_md, records)

    latest = output_root / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    for source_path, name in [
        (dated_md, "ubo5_tb15_latest_closeout.md"), (dated_csv, "ubo5_tb15_latest_closeout.csv"),
        (record_md, "ubo5_tb15_daily_record.md"), (record_csv, "ubo5_tb15_daily_record.csv"),
    ]:
        shutil.copy2(source_path, latest / name)
    print(json.dumps(summary, indent=2))
    print(f"certified_outcome_source={certified_status}")
    print(f"official_game_status_source={game_status_source}")
    print(f"rerun_unchanged={str(unchanged).lower()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
