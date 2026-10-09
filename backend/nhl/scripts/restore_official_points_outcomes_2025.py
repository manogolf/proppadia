#!/usr/bin/env python3
"""Reconcile retained official NHL Gamecenter boxscores to 2025-26 skater logs."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
RAW_ROOT = ROOT / "artifacts/operational/nhl/moneyline_team_history/raw/season=2025"
ENDPOINT = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_boxscore(data: dict[str, Any], source_path: str, digest: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    gid = int(data["id"])
    season = int(str(data["season"])[:4])
    if season != 2025 or int(data.get("gameType", 0)) != 2:
        raise ValueError(f"NOT_CANONICAL_2025_REGULAR_SEASON:{gid}")
    if data.get("gameState") not in {"OFF", "FINAL"}:
        raise ValueError(f"GAME_NOT_FINAL:{gid}:{data.get('gameState')}")
    teams = data.get("playerByGameStats") or {}
    rows = []
    seen: set[int] = set()
    for side in ("awayTeam", "homeTeam"):
        team_id = int(data[side]["id"])
        team_score = int(data[side]["score"])
        player_stats = teams.get(side) or {}
        team_goal_sum = 0
        team_assist_sum = 0
        for group in ("forwards", "defense"):
            for player in player_stats.get(group) or []:
                pid = int(player["playerId"])
                if pid in seen:
                    raise ValueError(f"DUPLICATE_PLAYER_IN_GAME:{gid}:{pid}")
                seen.add(pid)
                goals, assists = player.get("goals"), player.get("assists")
                if goals is None or assists is None:
                    raise ValueError(f"MISSING_TARGET:{gid}:{pid}")
                goals, assists = int(goals), int(assists)
                if goals < 0 or assists < 0:
                    raise ValueError(f"NEGATIVE_TARGET:{gid}:{pid}")
                team_goal_sum += goals
                team_assist_sum += assists
                rows.append({
                    "canonical_season": 2025, "game_date": data["gameDate"], "game_id": gid,
                    "player_id": pid, "team_id": team_id, "goals": goals, "assists": assists,
                    "realized_points": goals + assists, "participation_status": "SKATER_IN_OFFICIAL_BOXSCORE",
                    "source_endpoint": ENDPOINT.format(game_id=gid), "source_artifact": source_path,
                    "source_sha256": digest, "reconstruction_status": "OFFICIAL_EXACT_PLAYER_ID",
                })
        period_type = (data.get("gameOutcome") or {}).get("lastPeriodType")
        shootout_adjustment = int(period_type == "SO" and team_score == max(int(data["homeTeam"]["score"]), int(data["awayTeam"]["score"])))
        if team_goal_sum + shootout_adjustment != team_score:
            raise ValueError(f"TEAM_GOAL_TOTAL_MISMATCH:{gid}:{side}:{team_goal_sum}:{team_score}:SO_ADJ={shootout_adjustment}")
        if team_assist_sum > 2 * team_goal_sum:
            raise ValueError(f"IMPLAUSIBLE_ASSIST_TOTAL:{gid}:{side}:{team_assist_sum}:{team_goal_sum}")
    game_check = {"game_id": gid, "game_date": data["gameDate"], "game_state": data["gameState"],
                  "home_team_id": int(data["homeTeam"]["id"]), "home_final_goals": int(data["homeTeam"]["score"]),
                  "home_player_goal_sum": sum(r["goals"] for r in rows if r["team_id"] == int(data["homeTeam"]["id"])),
                  "away_team_id": int(data["awayTeam"]["id"]), "away_final_goals": int(data["awayTeam"]["score"]),
                  "away_player_goal_sum": sum(r["goals"] for r in rows if r["team_id"] == int(data["awayTeam"]["id"])),
                  "last_period_type": (data.get("gameOutcome") or {}).get("lastPeriodType"),
                  "shootout_adjustment_home": int((data.get("gameOutcome") or {}).get("lastPeriodType") == "SO" and int(data["homeTeam"]["score"]) > int(data["awayTeam"]["score"])),
                  "shootout_adjustment_away": int((data.get("gameOutcome") or {}).get("lastPeriodType") == "SO" and int(data["awayTeam"]["score"]) > int(data["homeTeam"]["score"]))}
    return rows, game_check


def read_target(path: Path) -> dict[tuple[int, int], dict[str, Any]]:
    result = {}
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            key = (int(row["game_id"]), int(row["player_id"]))
            if key in result:
                raise ValueError(f"DUPLICATE_TARGET_KEY:{key}")
            result[key] = row
    return result


def recover(target_csv: Path, output_dir: Path, pbp_dir: Path | None = None) -> dict[str, Any]:
    target = read_target(target_csv)
    all_rows, game_checks, source_manifest = [], [], []
    box_paths = sorted(RAW_ROOT.glob("game=*/boxscore.json"))
    for path in box_paths:
        body = path.read_bytes(); digest = sha(body); data = json.loads(body)
        rel = str(path.relative_to(ROOT))
        rows, game = parse_boxscore(data, rel, digest)
        all_rows.extend(rows); game_checks.append(game)
        source_manifest.append({"game_id": game["game_id"], "game_date": game["game_date"],
            "canonical_season": 2025, "endpoint": ENDPOINT.format(game_id=game["game_id"]),
            "method": "RETAINED_LOCAL_REUSE", "raw_path": rel, "sha256": digest,
            "bytes": len(body), "acquisition_timestamp_utc": None,
            "acquisition_timestamp_basis": "NOT_RECORDED; parent build summary is inventory time, not response acquisition time"})
    recovered = {(int(r["game_id"]), int(r["player_id"])): r for r in all_rows}
    if len(recovered) != len(all_rows):
        raise ValueError("DUPLICATE_RECOVERED_PLAYER_GAME")
    missing = sorted(set(target) - set(recovered))
    pbp_manifest = []
    if missing and pbp_dir is not None:
        missing_games = sorted({gid for gid, _ in missing})
        for gid in missing_games:
            path = pbp_dir / f"game={gid}.play-by-play.json"
            body = path.read_bytes(); digest = sha(body); data = json.loads(body)
            if int(data.get("id", -1)) != gid or data.get("gameState") not in {"OFF", "FINAL"}:
                raise ValueError(f"PBP_GAME_ID_OR_STATE_MISMATCH:{gid}")
            event_counts: dict[int, list[int]] = {}
            team_goal_events: dict[int, int] = {}
            for play in data.get("plays") or []:
                if play.get("typeDescKey") != "goal":
                    continue
                if (play.get("periodDescriptor") or {}).get("periodType") == "SO":
                    continue  # shootout attempts do not count as official player goals
                details = play.get("details") or {}
                team_id = int(details["eventOwnerTeamId"])
                team_goal_events[team_id] = team_goal_events.get(team_id, 0) + 1
                scorer = details.get("scoringPlayerId")
                if scorer is not None:
                    event_counts.setdefault(int(scorer), [0, 0])[0] += 1
                for field in ("assist1PlayerId", "assist2PlayerId"):
                    assister = details.get(field)
                    if assister is not None:
                        event_counts.setdefault(int(assister), [0, 0])[1] += 1
            for side in ("awayTeam", "homeTeam"):
                team_id, score = int(data[side]["id"]), int(data[side]["score"])
                expected = score - int((data.get("gameOutcome") or {}).get("lastPeriodType") == "SO" and score > int(data["homeTeam"]["score"] if side == "awayTeam" else data["awayTeam"]["score"]))
                if team_goal_events.get(team_id, 0) != expected:
                    raise ValueError(f"PBP_GOAL_TOTAL_MISMATCH:{gid}:{team_id}:{team_goal_events.get(team_id,0)}:{expected}")
            pbp_manifest.append({"game_id":gid,"endpoint":f"https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play","raw_path":str(path.resolve().relative_to(ROOT)),"sha256":digest,"bytes":len(body),"method":"GET"})
            for key in [k for k in missing if k[0] == gid]:
                _, pid = key
                goals, assists = event_counts.get(pid, [0, 0])
                target_row = target[key]
                team_id = int(target_row["team_id"])
                row = {"canonical_season":2025,"game_date":data["gameDate"],"game_id":gid,"player_id":pid,
                       "team_id":team_id,"goals":goals,"assists":assists,"realized_points":goals+assists,
                       "participation_status":"SKATER_LOG_ROW_PRESENT","source_endpoint":f"https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play",
                       "source_artifact":str(path.resolve().relative_to(ROOT)),"source_sha256":digest,
                       "reconstruction_status":"OFFICIAL_GOAL_EVENT_IDENTITY"}
                recovered[key]=row; all_rows.append(row)
        missing = sorted(set(target) - set(recovered))
    partial_targets = [r for r in target.values() if r.get("goals") or r.get("assists")]
    comparisons = []
    for row in partial_targets:
        key = int(row["game_id"]), int(row["player_id"])
        if key in recovered:
            comparisons.append({"game_id": key[0], "player_id": key[1], "stored_goals": row["goals"],
                "stored_assists": row["assists"], "official_goals": recovered[key]["goals"],
                "official_assists": recovered[key]["assists"], "match": str(row["goals"]) == str(recovered[key]["goals"]) and str(row["assists"]) == str(recovered[key]["assists"])})
    unresolved = [{"game_id": g, "player_id": p, "reason": "TARGET_KEY_NOT_IN_OFFICIAL_BOXSCORE"} for g, p in missing]
    output_dir.mkdir(parents=True, exist_ok=False)
    fields = list(all_rows[0]) if all_rows else []
    with (output_dir / "canonical_player_game_outcomes.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n"); writer.writeheader(); writer.writerows(sorted(all_rows, key=lambda r:(r["game_id"],r["player_id"])))
    (output_dir / "source_manifest.json").write_text(json.dumps(source_manifest, indent=2, sort_keys=True)+"\n")
    (output_dir / "pbp_source_manifest.json").write_text(json.dumps(pbp_manifest, indent=2, sort_keys=True)+"\n")
    check_fields = ["game_id","game_date","game_state","last_period_type","home_team_id","home_final_goals","home_player_goal_sum","shootout_adjustment_home","away_team_id","away_final_goals","away_player_goal_sum","shootout_adjustment_away"]
    with (output_dir / "game_scoring_integrity.csv").open("w",newline="") as f:
        writer=csv.DictWriter(f,fieldnames=check_fields,lineterminator="\n"); writer.writeheader(); writer.writerows(sorted(game_checks,key=lambda x:x["game_id"]))
    comparison_report={"compared_rows":len(comparisons),"exact_matches":sum(c["match"] for c in comparisons),"mismatches":[c for c in comparisons if not c["match"]],"comparison_rows":comparisons}
    (output_dir / "retained_target_comparisons.json").write_text(json.dumps(comparison_report,indent=2)+"\n")
    (output_dir / "mismatch_report.json").write_text(json.dumps(comparison_report,indent=2)+"\n")
    (output_dir / "unresolved.json").write_text(json.dumps(unresolved,indent=2)+"\n")
    matched = len(set(target) & set(recovered))
    summary = {"schema_version":"NHL_OFFICIAL_POINTS_OUTCOME_RESTORATION_V1","canonical_season":2025,
        "season_label":"2025-26","regular_season_date_range":["2025-10-07","2026-04-16"],
        "source_table":"nhl.skater_game_logs_raw","source_grain":"one row per player_id,game_id",
        "source_primary_key":["player_id","game_id"],"source_rows":len(target),"source_games":len({k[0] for k in target}),
        "source_goals_present":sum(bool(r.get("goals")) for r in target.values()),"source_assists_present":sum(bool(r.get("assists")) for r in target.values()),
        "source_both_present":sum(bool(r.get("goals")) and bool(r.get("assists")) for r in target.values()),
        "source_both_missing":sum(not r.get("goals") and not r.get("assists") for r in target.values()),
        "official_schedule_games":len(game_checks),"official_games_with_final_boxscore":sum(g["game_state"] in ("OFF","FINAL") for g in game_checks),
        "restored_official_player_game_rows":len(all_rows),"restored_goals_present":sum(r["goals"] is not None for r in all_rows),
        "restored_assists_present":sum(r["assists"] is not None for r in all_rows),
        "restored_both_present":sum(r["goals"] is not None and r["assists"] is not None for r in all_rows),
        "restored_either_missing":sum(r["goals"] is None or r["assists"] is None for r in all_rows),
        "exact_matched_source_rows":matched,"exact_match_rate":matched/len(target) if target else None,
        "unmatched_source_rows":len(missing),"duplicate_rows":0,"ambiguous_rows":0,"partial_target_comparisons":len(comparisons),
        "partial_target_exact_matches":sum(c["match"] for c in comparisons),"partial_target_mismatches":sum(not c["match"] for c in comparisons),
        "game_total_checks":len(game_checks)*2,"shootout_score_adjustments":sum(g["shootout_adjustment_home"]+g["shootout_adjustment_away"] for g in game_checks),"game_total_mismatches":0,"negative_targets":0,
        "database_mutations":0,"provider_requests_planned":8 if pbp_dir is not None else 0,"provider_requests_executed":len(pbp_manifest),"paid_credits":0,
        "acquisition":"RETAINED_OFFICIAL_GAMECENTER_BOXSCORES_PLUS_TARGETED_OFFICIAL_PBP","endpoint":ENDPOINT,
        "readiness":"READY_FOR_FROZEN_HGB_SECOND_SEASON_VALIDATION" if not missing else "NOT_READY_FOR_SECOND_SEASON_VALIDATION",
        "frozen_validation_run":"NOT_RUN"}
    (output_dir / "recovery_summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    coverage={k:summary[k] for k in ("canonical_season","season_label","regular_season_date_range","official_schedule_games","official_games_with_final_boxscore","source_table","source_grain","source_primary_key","source_rows","source_games","source_goals_present","source_assists_present","source_both_present","source_both_missing","restored_official_player_game_rows","restored_goals_present","restored_assists_present","restored_both_present","restored_either_missing","exact_matched_source_rows","exact_match_rate","unmatched_source_rows","duplicate_rows","ambiguous_rows","game_total_checks","game_total_mismatches")}
    (output_dir / "coverage_report.json").write_text(json.dumps(coverage,indent=2,sort_keys=True)+"\n")
    # Include a manifest over all package files except itself.
    lines=[]
    for path in sorted(output_dir.iterdir()):
        lines.append(f"{sha(path.read_bytes())}  {path.name}")
    (output_dir / "SHA256SUMS").write_text("\n".join(lines)+"\n")
    return summary


def main() -> None:
    p=argparse.ArgumentParser(); p.add_argument("--target-csv",type=Path,required=True); p.add_argument("--output-dir",type=Path,required=True); p.add_argument("--pbp-dir",type=Path)
    args=p.parse_args(); print(json.dumps(recover(args.target_csv,args.output_dir,args.pbp_dir),indent=2,sort_keys=True))


if __name__ == "__main__": main()
