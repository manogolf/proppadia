#!/usr/bin/env python3
"""Freeze and reconcile clean-room BetOnline TB 1.5 captures without inherited data."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[4]
EXPORT_ROOT = ROOT / "backend/mlb/exports/cleanroom_v1/bol_tb15"
RAW_ROOT = ROOT / "backend/mlb/exports/cleanroom_v1/raw/MLB_STATS_API"
EVIDENCE_ROOT = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_cleanroom_bol_tb15_daily_lifecycle_certification"
)
PT = ZoneInfo("America/Los_Angeles")
REQUIRED = {
    "run_manifest.json", "bol_tb15_market_sides.csv",
    "bol_tb15_two_sided_markets.csv", "lineup_snapshot.csv",
    "identity_audit.csv", "source_hash_manifest.csv",
}


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00").replace(" ", "T"))


def csv_bytes(fields: list[str], rows: list[dict]) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode()


def american_profit(stake: float, odds: int) -> float:
    return stake * odds / 100 if odds > 0 else stake * 100 / abs(odds)


def load_runs(slate: str) -> tuple[list[dict], list[dict]]:
    snapshot_root = EXPORT_ROOT / slate / "snapshots"
    index_path = EVIDENCE_ROOT / slate / "cleanroom_intraday_run_index.csv"
    index = list(csv.DictReader(index_path.open())) if index_path.exists() else []
    runs = []
    failures = []
    seen_paths = set()
    for snapshot in sorted(path for path in snapshot_root.iterdir() if path.is_dir()):
        problem = []
        names = {path.name for path in snapshot.iterdir() if path.is_file()}
        if not REQUIRED.issubset(names):
            problem.append(f"missing_snapshot_files={sorted(REQUIRED - names)}")
        manifest = json.loads((snapshot / "run_manifest.json").read_text())
        hash_rows = list(csv.DictReader((snapshot / "source_hash_manifest.csv").open()))
        missing = 0
        mismatches = 0
        for row in hash_rows:
            payload = Path(row["raw_payload_path"])
            if not payload.exists():
                missing += 1
            elif hashlib.sha256(payload.read_bytes()).hexdigest() != row["sha256"]:
                mismatches += 1
        index_rows = [row for row in index if row["run_tag"] == snapshot.name]
        if len(index_rows) != 1:
            problem.append(f"run_index_rows={len(index_rows)}")
        if snapshot.resolve() in seen_paths:
            problem.append("duplicate_snapshot_path")
        seen_paths.add(snapshot.resolve())
        if missing:
            problem.append(f"missing_payloads={missing}")
        if mismatches:
            problem.append(f"sha_mismatches={mismatches}")
        item = {
            "run_tag": snapshot.name,
            "source_capture_timestamp_utc": datetime.strptime(
                snapshot.name.removeprefix("cleanroom_"), "%Y%m%dT%H%M%SZ"
            ).replace(tzinfo=timezone.utc),
            "admitted_timestamp_utc": parse_timestamp(manifest["capture_timestamp_utc"]),
            "snapshot": snapshot,
            "manifest": manifest,
            "hash_count": len(hash_rows),
            "missing_payloads": missing,
            "sha_mismatches": mismatches,
            "index_row_exists": len(index_rows) == 1,
            "trusted": not problem,
            "problems": problem,
        }
        (runs if not problem else failures).append(item)
    return runs, failures


def latest_schedule(slate: str, runs: list[dict]) -> tuple[list[dict], Path]:
    for run in reversed(runs):
        path = (
            EVIDENCE_ROOT / slate / "runs" / run["run_tag"] /
            "raw/MLB_STATS_API" / f"schedule_{slate}.json"
        )
        if path.exists():
            payload = json.loads(path.read_text())
            return [
                game for block in payload.get("dates", []) for game in block.get("games", [])
            ], path
    raise RuntimeError("official schedule not found in trusted capture evidence")


def binding_map(slate: str, runs: list[dict]) -> dict[tuple[str, int], dict]:
    result = {}
    for run in runs:
        path = EVIDENCE_ROOT / slate / "runs" / run["run_tag"] / "provider_event_to_game_pk_audit.csv"
        for row in csv.DictReader(path.open()):
            if row["decision"] == "EXACT_UNIQUE_MATCH":
                result[(run["run_tag"], int(row["game_pk"]))] = row
    return result


def audit_and_freeze(slate: str, runs: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    games, _ = latest_schedule(slate, runs)
    bindings = binding_map(slate, runs)
    frozen = {}
    coverage_rows = []
    for game in sorted(games, key=lambda row: row["gameDate"]):
        game_pk = int(game["gamePk"])
        pitch = parse_timestamp(game["gameDate"])
        captures = []
        last_lineup = None
        for run in runs:
            snapshot = run["snapshot"]
            markets = [
                row for row in csv.DictReader((snapshot / "bol_tb15_two_sided_markets.csv").open())
                if int(row["game_pk"]) == game_pk
                and parse_timestamp(row["market_timestamp_utc"]) < pitch
            ]
            lineups = [
                row for row in csv.DictReader((snapshot / "lineup_snapshot.csv").open())
                if int(row["game_pk"]) == game_pk
                and parse_timestamp(row["snapshot_timestamp_utc"]) < pitch
            ]
            if lineups:
                observed = max(parse_timestamp(row["snapshot_timestamp_utc"]) for row in lineups)
                if last_lineup is None or observed > last_lineup[0]:
                    last_lineup = (observed, run["run_tag"], len(lineups))
            if not markets:
                continue
            side_rows = list(csv.DictReader((snapshot / "bol_tb15_market_sides.csv").open()))
            sides = {
                (int(row["game_pk"]), int(row["player_mlb_id"]), row["side"]): row
                for row in side_rows if int(row["game_pk"]) == game_pk
            }
            identities = list(csv.DictReader((snapshot / "identity_audit.csv").open()))
            identity_by_key = {
                (int(row["game_pk"]), int(row["player_mlb_id"]), row["raw_payload_sha256"]): row
                for row in identities
            }
            hash_paths = {
                row["sha256"]: row["raw_payload_path"]
                for row in csv.DictReader((snapshot / "source_hash_manifest.csv").open())
            }
            for market in markets:
                player_id = int(market["player_mlb_id"])
                over = sides[(game_pk, player_id, "Over")]
                under = sides[(game_pk, player_id, "Under")]
                market_at = parse_timestamp(market["market_timestamp_utc"])
                identity = identity_by_key[(game_pk, player_id, over["source_payload_sha256"])]
                roster_path = (
                    EVIDENCE_ROOT / slate / "runs" / run["run_tag"] /
                    "raw/MLB_STATS_API" / f"game_{game_pk}.json"
                )
                roster_feed = json.loads(roster_path.read_text())
                player_team_id = None
                for roster_side in ("away", "home"):
                    roster_players = roster_feed["liveData"]["boxscore"]["teams"][
                        roster_side
                    ].get("players", {})
                    if f"ID{player_id}" in roster_players:
                        player_team_id = int(
                            roster_feed["gameData"]["teams"][roster_side]["id"]
                        )
                        break
                away_team = game["teams"]["away"]["team"]
                home_team = game["teams"]["home"]["team"]
                if player_team_id == int(away_team["id"]):
                    team, opponent = away_team["name"], home_team["name"]
                elif player_team_id == int(home_team["id"]):
                    team, opponent = home_team["name"], away_team["name"]
                else:
                    raise RuntimeError(
                        f"exact roster team unavailable for {game_pk}/{player_id}"
                    )
                key = (game_pk, player_id)
                candidate = {
                    "slate_date": slate, "game_pk": game_pk,
                    "player_mlb_id": player_id, "player": market["player"],
                    "team": team, "opponent": opponent,
                    "prop_type": "Total Bases", "line": 1.5,
                    "governing_run_tag": run["run_tag"],
                    "market_timestamp_utc": market_at.isoformat(),
                    "over_odds": int(market["over_odds"]),
                    "under_odds": int(market["under_odds"]),
                    "lineup_status": market["lineup_status"],
                    "batting_order": market["batting_order"],
                    "provider_event_id": identity["provider_event_id"],
                    "identity_decision": identity["decision"],
                    "source_payload_path": hash_paths[over["source_payload_sha256"]],
                    "source_sha256": over["source_payload_sha256"],
                    "population_status": "ACTIONABLE",
                }
                if key not in frozen or market_at > parse_timestamp(frozen[key]["market_timestamp_utc"]):
                    frozen[key] = candidate
            captures.append({
                "run_tag": run["run_tag"],
                "market_timestamp": max(parse_timestamp(row["market_timestamp_utc"]) for row in markets),
                "market_count": len(markets),
                "confirmed": any(row["lineup_status"] == "CONFIRMED" for row in markets),
            })
        away = game["teams"]["away"]["team"]["name"]
        home = game["teams"]["home"]["team"]["name"]
        binding = bindings.get((captures[-1]["run_tag"], game_pk)) if captures else None
        if not captures:
            status = "PROVIDER_EVENT_MISSING" if not any(
                key[1] == game_pk for key in bindings
            ) else "MARKET_NOT_AVAILABLE"
        elif captures[-1]["confirmed"]:
            status = "FINAL_PREGAME_CERTIFIED"
        else:
            status = "PREGAME_CAPTURE_AVAILABLE_LINEUP_UNCONFIRMED"
        coverage_rows.append({
            "game_pk": game_pk, "game": f"{away} @ {home}",
            "scheduled_first_pitch_utc": pitch.isoformat(),
            "scheduled_first_pitch_pt": pitch.astimezone(PT).isoformat(),
            "doubleheader_game_number": game.get("gameNumber", 1),
            "provider_event_id": binding["provider_event_id"] if binding else "",
            "first_market_capture": captures[0]["market_timestamp"].isoformat() if captures else "",
            "last_market_capture_before_first_pitch": captures[-1]["market_timestamp"].isoformat() if captures else "",
            "last_lineup_capture_before_first_pitch": last_lineup[0].isoformat() if last_lineup else "",
            "latest_two_sided_market_count": captures[-1]["market_count"] if captures else 0,
            "confirmed_lineup_status": "CONFIRMED" if captures and captures[-1]["confirmed"] else "UNCONFIRMED",
            "final_pregame_capture_run_tag": captures[-1]["run_tag"] if captures else "",
            "coverage_status": status,
        })
    game_lookup = {int(row["gamePk"]): row for row in games}
    for row in frozen.values():
        game = game_lookup[row["game_pk"]]
        away_id = int(game["teams"]["away"]["team"]["id"])
        home_id = int(game["teams"]["home"]["team"]["id"])
        row["_away_id"] = away_id
        row["_home_id"] = home_id
    return sorted(frozen.values(), key=lambda row: (row["game_pk"], row["player_mlb_id"])), coverage_rows, games


def preserve_official_outcomes(slate: str, games: list[dict]) -> tuple[dict, Path]:
    run_tag = f"closeout_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    raw_dir = RAW_ROOT / slate / run_tag
    raw_dir.mkdir(parents=True, exist_ok=False)
    outcomes = {}
    for game in games:
        game_pk = int(game["gamePk"])
        response = requests.get(
            f"https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live", timeout=45
        )
        response.raise_for_status()
        path = raw_dir / f"game_{game_pk}.json"
        path.write_bytes(response.content)
        sha = hashlib.sha256(response.content).hexdigest()
        feed = response.json()
        status = feed["gameData"]["status"]["detailedState"]
        outcomes[(game_pk, None)] = {
            "game_status": status, "outcome_source": str(path),
            "outcome_source_sha256": sha,
        }
        for side in ("away", "home"):
            team_id = int(feed["gameData"]["teams"][side]["id"])
            opponent_id = int(feed["gameData"]["teams"]["home" if side == "away" else "away"]["id"])
            for key, entry in feed["liveData"]["boxscore"]["teams"][side].get("players", {}).items():
                player_id = int(key.removeprefix("ID"))
                batting = (entry.get("stats") or {}).get("batting") or {}
                hits = int(batting.get("hits") or 0)
                doubles = int(batting.get("doubles") or 0)
                triples = int(batting.get("triples") or 0)
                homers = int(batting.get("homeRuns") or 0)
                singles = hits - doubles - triples - homers
                outcomes[(game_pk, player_id)] = {
                    "game_status": status, "team_id": team_id, "opponent_id": opponent_id,
                    "plate_appearances": int(batting.get("plateAppearances") or 0),
                    "at_bats": int(batting.get("atBats") or 0), "hits": hits,
                    "singles": singles, "doubles": doubles, "triples": triples,
                    "home_runs": homers,
                    "total_bases": singles + 2*doubles + 3*triples + 4*homers,
                    "outcome_source": str(path), "outcome_source_sha256": sha,
                }
    return outcomes, raw_dir


def confirmed_starters_before_pitch(
    runs: list[dict], games: list[dict]
) -> dict[int, set[int]]:
    result = {}
    for game in games:
        game_pk = int(game["gamePk"])
        pitch = parse_timestamp(game["gameDate"])
        latest = None
        for run in runs:
            rows = [
                row for row in csv.DictReader(
                    (run["snapshot"] / "lineup_snapshot.csv").open()
                )
                if int(row["game_pk"]) == game_pk
                and row["lineup_status"] == "CONFIRMED"
                and parse_timestamp(row["snapshot_timestamp_utc"]) < pitch
            ]
            if not rows:
                continue
            observed = max(parse_timestamp(row["snapshot_timestamp_utc"]) for row in rows)
            if latest is None or observed > latest[0]:
                latest = (observed, {int(row["player_mlb_id"]) for row in rows})
        if latest:
            result[game_pk] = latest[1]
    return result


def baseline(rows: list[dict], side: str) -> dict:
    settled = [row for row in rows if row["settlement_status"] == "SETTLED"]
    wins = sum(row["outcome"] == f"{side.upper()}_WIN" for row in settled)
    losses = len(settled) - wins
    odds_field = f"final_pregame_{side.lower()}_odds"
    odds = [int(row[odds_field]) for row in settled]
    gross = sum(
        american_profit(5, int(row[odds_field]))
        for row in settled if row["outcome"] == f"{side.upper()}_WIN"
    )
    stake = 5 * len(settled)
    net = gross - 5 * losses
    return {
        "side": side, "wagers": len(settled), "wins": wins, "losses": losses,
        "win_rate": wins / len(settled) if settled else None,
        "total_stake": stake, "gross_winning_profit": gross,
        "net_dollars": net, "roi": net / stake if stake else None,
        "average_american_odds": sum(odds) / len(odds) if odds else None,
    }


def status_only(slate: str) -> int:
    out = EXPORT_ROOT / slate
    runs, failures = load_runs(slate)
    manifest_path = out / "closeout_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    result = {
        "capture_count": len(runs), "failed_or_incomplete_captures": len(failures),
        "game_coverage": manifest.get("game_coverage"),
        "frozen_population_count": manifest.get("frozen_population_count", 0),
        "actionable_rows": manifest.get("actionable_rows", 0),
        "wins": manifest.get("over_wins", 0), "losses": manifest.get("over_losses", 0),
        "no_action": manifest.get("no_action", 0), "pending": manifest.get("pending", 0),
        "technical_unresolved": manifest.get("technical_unresolved", 0),
        "closeout_revision": manifest.get("closeout_revision", 0),
        "closeout_status": manifest.get("closeout_status", "NOT_PREPARED"),
    }
    print(json.dumps(result, indent=2))
    return 0


def exclusion_manifest(slate: str) -> Path:
    return EXPORT_ROOT / slate / "neutral_lifecycle_exclusion_manifest.json"


def freeze_neutral_population(slate: str) -> dict:
    excluded = exclusion_manifest(slate)
    if excluded.exists():
        decision = json.loads(excluded.read_text()).get("decision", "INELIGIBLE")
        raise SystemExit(f"neutral freeze refused: {decision}")
    runs, failures = load_runs(slate)
    if failures:
        raise SystemExit(f"untrusted immutable captures: {[r['run_tag'] for r in failures]}")
    population, coverage, _ = audit_and_freeze(slate, runs)
    if not population:
        raise SystemExit("PREGAME_FREEZE_REQUIRED: no valid pre-first-pitch two-sided markets")
    out = EXPORT_ROOT / slate
    out.mkdir(parents=True, exist_ok=True)
    public = [{k: v for k, v in row.items() if not k.startswith("_")} for row in population]
    fields = list(public[0])
    content = csv_bytes(fields, public)
    digest = hashlib.sha256(content).hexdigest()
    population_path = out / f"bol_tb15_final_pregame_actionable_{slate}.csv"
    manifest_path = out / "final_population_manifest.json"
    if manifest_path.exists():
        prior = json.loads(manifest_path.read_text())
        if prior["population_sha256"] != digest or not population_path.exists() or hashlib.sha256(population_path.read_bytes()).hexdigest() != digest:
            raise SystemExit("immutable final population differs from existing freeze")
        return {"freeze_status": "ALREADY_FROZEN_IDENTICAL", **prior}
    population_path.write_bytes(content)
    manifest = {
        "population_id": f"BOL_TB15_FINAL_PREGAME_ACTIONABLE_{slate}",
        "slate_date": slate, "frozen_at_utc": datetime.now(timezone.utc).isoformat(),
        "membership_uses_outcomes": False,
        "identity_key": ["slate_date", "game_pk", "player_mlb_id", "total_bases", "1.5"],
        "actionable_rows": len(public), "population_sha256": digest,
        "game_coverage": coverage,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {"freeze_status": "PREGAME_POPULATION_FROZEN", **manifest}


def lifecycle_status(slate: str) -> dict:
    out = EXPORT_ROOT / slate
    excluded = exclusion_manifest(slate)
    if excluded.exists():
        state = "INELIGIBLE_FREEZE_MISSED"
    elif (out / "neutral_closeout/neutral_closeout_manifest.json").exists():
        historical = json.loads((out / "neutral_closeout/neutral_closeout_manifest.json").read_text())
        state = "FINAL" if historical.get("status") == "FINAL" else "OUTCOME_CLOSEOUT_PENDING"
    elif (out / "closeout_manifest.json").exists():
        closeout = json.loads((out / "closeout_manifest.json").read_text())
        state = "FINAL" if closeout.get("closeout_status") == "FINAL" else "OUTCOME_CLOSEOUT_PENDING"
    elif (out / "final_population_manifest.json").exists():
        state = "OUTCOME_CLOSEOUT_PENDING"
    elif (out / "snapshots").exists():
        state = "PREGAME_FREEZE_REQUIRED"
    else:
        state = "CAPTURES_IN_PROGRESS"
    return {"slate_date": slate, "lifecycle_state": state,
            "historical_exceptions_visible": [
                "PASQUANTINO_JULY29_UNSUPPORTED_VOID",
                "JULY31_NEUTRAL_POPULATION_NOT_FROZEN",
                "JULY29_JULY30_H1_TEMPORAL_LINEAGE_VOID",
            ]}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--status-only", action="store_true")
    parser.add_argument("--freeze-only", action="store_true")
    parser.add_argument("--lifecycle-status", action="store_true")
    args = parser.parse_args()
    date.fromisoformat(args.date)
    if args.status_only:
        return status_only(args.date)

    if args.lifecycle_status:
        print(json.dumps(lifecycle_status(args.date), indent=2)); return 0
    if args.freeze_only:
        print(json.dumps(freeze_neutral_population(args.date), indent=2)); return 0

    out = EXPORT_ROOT / args.date
    population_path = out / f"bol_tb15_final_pregame_actionable_{args.date}.csv"
    population_manifest_path = out / "final_population_manifest.json"
    if not population_manifest_path.exists() or not population_path.exists():
        raise SystemExit("PREGAME_FREEZE_REQUIRED: closeout requires an existing immutable neutral population")
    runs, failures = load_runs(args.date)
    if failures:
        raise SystemExit(f"untrusted immutable captures: {[r['run_tag'] for r in failures]}")
    population_manifest = json.loads(population_manifest_path.read_text())
    if hashlib.sha256(population_path.read_bytes()).hexdigest() != population_manifest["population_sha256"]:
        raise SystemExit("immutable neutral population hash mismatch")
    population = list(csv.DictReader(population_path.open()))
    for row in population:
        row["game_pk"] = int(row["game_pk"]); row["player_mlb_id"] = int(row["player_mlb_id"])
        row["over_odds"] = int(row["over_odds"]); row["under_odds"] = int(row["under_odds"])
    games, _ = latest_schedule(args.date, runs)
    coverage = population_manifest["game_coverage"]

    official, raw_dir = preserve_official_outcomes(args.date, games)
    game_lookup = {int(game["gamePk"]): game for game in games}
    closeout = []
    technical = []
    for selected in population:
        game = game_lookup[selected["game_pk"]]
        result = official.get((selected["game_pk"], selected["player_mlb_id"]))
        game_source = official.get((selected["game_pk"], None), {})
        game_statuses = {
            value["game_status"] for (game_pk, _), value in official.items()
            if game_pk == selected["game_pk"]
        }
        game_status = next(iter(game_statuses), "")
        if result is None and game_status == "Final":
            outcome, settlement = "TECHNICAL_UNRESOLVED", "UNRESOLVED"
            technical.append((selected["game_pk"], selected["player_mlb_id"]))
            result = {}
        elif result is None or result.get("game_status") != "Final":
            outcome, settlement = "PENDING", "PENDING"
            result = result or game_source
        elif int(result["plate_appearances"]) == 0:
            outcome, settlement = "NO_ACTION", "VOID"
        elif int(result["total_bases"]) > 1.5:
            outcome, settlement = "OVER_WIN", "SETTLED"
        else:
            outcome, settlement = "OVER_LOSS", "SETTLED"
        away = game["teams"]["away"]["team"]
        home = game["teams"]["home"]["team"]
        team_id = result.get("team_id")
        selected["team"] = away["name"] if team_id == away["id"] else home["name"] if team_id == home["id"] else ""
        selected["opponent"] = home["name"] if team_id == away["id"] else away["name"] if team_id == home["id"] else ""
        closeout.append({
            "game_pk": selected["game_pk"], "player_mlb_id": selected["player_mlb_id"],
            "player": selected["player"], "game": f"{away['name']} @ {home['name']}",
            "governing_run_tag": selected["governing_run_tag"],
            "final_pregame_over_odds": selected["over_odds"],
            "final_pregame_under_odds": selected["under_odds"],
            "market_timestamp_utc": selected["market_timestamp_utc"],
            "lineup_status": selected["lineup_status"], "batting_order": selected["batting_order"],
            "plate_appearances": result.get("plate_appearances", ""),
            "at_bats": result.get("at_bats", ""), "hits": result.get("hits", ""),
            "singles": result.get("singles", ""), "doubles": result.get("doubles", ""),
            "triples": result.get("triples", ""), "home_runs": result.get("home_runs", ""),
            "total_bases": result.get("total_bases", ""), "outcome": outcome,
            "settlement_status": settlement,
            "outcome_source": result.get("outcome_source", ""),
            "source_sha256": result.get("outcome_source_sha256", ""),
        })
    counts = Counter(row["outcome"] for row in closeout)
    pending = counts["PENDING"]
    status = (
        "FAILED_TECHNICAL_UNRESOLVED" if technical
        else "PREPARED_PENDING_GAME_COMPLETION" if pending else "FINAL"
    )
    over = baseline(closeout, "Over")
    under_rows = [
        {**row, "outcome": (
            "UNDER_WIN" if row["outcome"] == "OVER_LOSS"
            else "UNDER_LOSS" if row["outcome"] == "OVER_WIN"
            else row["outcome"]
        )} for row in closeout
    ]
    under = baseline(under_rows, "Under")
    fields = list(closeout[0])
    content = csv_bytes(fields, closeout)
    content_sha = hashlib.sha256(content).hexdigest()
    settlement_fields = [field for field in fields if field != "outcome_source"]
    settlement_rows = [
        {field: row[field] for field in settlement_fields} for row in closeout
    ]
    settlement_sha = hashlib.sha256(
        csv_bytes(settlement_fields, settlement_rows)
    ).hexdigest()
    manifest_path = out / "closeout_manifest.json"
    prior = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    prior_settlement_sha = prior.get("settlement_sha256")
    if prior and not prior_settlement_sha:
        prior_csv = out / f"bol_tb15_cleanroom_closeout_{args.date}.csv"
        prior_rows = list(csv.DictReader(prior_csv.open()))
        prior_settlement_sha = hashlib.sha256(
            csv_bytes(
                settlement_fields,
                [{field: row[field] for field in settlement_fields} for row in prior_rows],
            )
        ).hexdigest()
    if prior_settlement_sha == settlement_sha:
        print(json.dumps({"status": status, "revision": prior["closeout_revision"],
                          "changed": False, "over": over, "under": under}, indent=2))
        return 0
    revision = int(prior.get("closeout_revision", 0)) + 1
    csv_path = out / f"bol_tb15_cleanroom_closeout_{args.date}.csv"
    md_path = out / f"bol_tb15_cleanroom_closeout_{args.date}.md"
    csv_path.write_bytes(content)
    md = [
        f"# Clean-room BetOnline TB 1.5 Closeout — {args.date}", "",
        f"Status: `{status}`", f"Revision: `{revision}`", "",
        "## Neutral flat-$5 baselines", "",
        "| Side | Wagers | Wins | Losses | Win rate | Stake | Gross win profit | Net | ROI | Average odds |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in (over, under):
        md.append(
            f"| {item['side']} 1.5 TB | {item['wagers']} | {item['wins']} | {item['losses']} | "
            f"{item['win_rate']:.2%} | ${item['total_stake']:.2f} | "
            f"${item['gross_winning_profit']:.2f} | ${item['net_dollars']:.2f} | "
            f"{item['roi']:.2%} | {item['average_american_odds']:+.2f} |"
        )
    md.extend(["", "## Outcome counts", ""])
    for key, value in sorted(counts.items()):
        md.append(f"- {key}: {value}")
    md_path.write_text("\n".join(md) + "\n")
    manifest = {
        "slate_date": args.date, "closeout_revision": revision,
        "closeout_status": status, "content_sha256": content_sha,
        "settlement_sha256": settlement_sha,
        "frozen_population_count": len(population),
        "actionable_rows": sum(row["settlement_status"] == "SETTLED" for row in closeout),
        "over_wins": counts["OVER_WIN"], "over_losses": counts["OVER_LOSS"],
        "no_action": counts["NO_ACTION"], "pending": pending,
        "technical_unresolved": counts["TECHNICAL_UNRESOLVED"],
        "game_coverage": Counter(row["coverage_status"] for row in coverage),
        "over_baseline": over, "under_baseline": under,
        "official_outcome_raw_directory": str(raw_dir),
        "uses_inherited_derived_objects": False,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    revision_dir = out / "revisions" / f"revision_{revision:03d}"
    revision_dir.mkdir(parents=True, exist_ok=False)
    for path in (csv_path, md_path, manifest_path, population_manifest_path, population_path):
        shutil.copy2(path, revision_dir / path.name)
    print(json.dumps({"status": status, "revision": revision, "changed": True,
                      "population": len(population), "counts": counts,
                      "over": over, "under": under}, indent=2, default=dict))
    return 1 if status == "FAILED_TECHNICAL_UNRESOLVED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
