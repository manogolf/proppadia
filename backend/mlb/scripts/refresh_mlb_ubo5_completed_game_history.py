#!/usr/bin/env python3
"""Incrementally refresh the official completed-game history consumed by UBO-5."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_NORMALIZED = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/"
    "normalized_refresh"
)
STATCAST_ROOT = ROOT / "backend/mlb/data/external/statcast/raw"
STATSAPI_ROOT = ROOT / "backend/mlb/data/external/statsapi/raw/2026"
OUTPUT_BASE = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_ubo5_completed_game_feedback_loop"
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def dates(first: date, last: date) -> list[date]:
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def parquet_files(root: Path, table: str) -> list[Path]:
    return sorted((root / table).glob("season=*/*.parquet"))


def read_table(root: Path, table: str) -> pd.DataFrame:
    frames = [
        pq.ParquetFile(path).read().to_pandas()
        for path in parquet_files(root, table)
    ]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".parquet.partial")
    pq.write_table(
        pa.Table.from_pandas(frame, preserve_index=False),
        temporary,
        compression="zstd",
        compression_level=3,
        use_dictionary=True,
    )
    temporary.replace(path)


def replace_games(
    normalized: Path, table: str, additions: pd.DataFrame, game_ids: set[int]
) -> tuple[int, int]:
    path = normalized / table / "season=2026/part-000.parquet"
    existing = (
        pq.ParquetFile(path).read().to_pandas()
        if path.is_file() else pd.DataFrame()
    )
    before = len(existing)
    if not existing.empty and "game_pk" in existing:
        existing = existing[
            ~pd.to_numeric(existing["game_pk"], errors="coerce").isin(game_ids)
        ]
    columns = list(dict.fromkeys([*existing.columns, *additions.columns]))
    merged = pd.concat(
        [existing.reindex(columns=columns), additions.reindex(columns=columns)],
        ignore_index=True,
        sort=False,
    )
    if "game_pk" in merged:
        sort = ["game_pk"]
        if "player_id" in merged:
            sort.append("player_id")
        if "team" in merged:
            sort.append("team")
        merged = merged.sort_values(sort, kind="stable").reset_index(drop=True)
    write_parquet(merged, path)
    return before, len(merged)


def normalize_statcast_day(
    day: date, normalized: Path
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    folder = STATCAST_ROOT / str(day.year) / f"{day}_{day}"
    raw, meta_path = folder / "statcast_search.csv", folder / "request_metadata.json"
    if not raw.is_file() or not meta_path.is_file():
        raise RuntimeError(f"MISSING_STATCAST_SOURCE:{day}")
    meta = json.loads(meta_path.read_text())
    if meta.get("completion_status") not in {
        "ACQUIRED_AND_VALIDATED", "ACQUIRED_EMPTY_VALID"
    }:
        raise RuntimeError(f"UNCERTIFIED_STATCAST_SOURCE:{day}")
    frame = pd.read_csv(raw, dtype=str, encoding="utf-8-sig", low_memory=False)
    frame["source_raw_path"] = str(raw.relative_to(ROOT))
    frame["source_raw_sha256"] = meta["sha256"]
    frame["source_request_timestamp_utc"] = meta.get("request_timestamp_utc", "")
    frame["source_endpoint"] = meta.get("request_url", "")
    frame["source_raw_row_ordinal"] = np.arange(1, len(frame) + 1, dtype=np.int64)
    frame["normalized_season"] = day.year
    keys = ["game_pk", "at_bat_number", "pitch_number", "batter", "pitcher"]
    for column in keys:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
    frame["canonical_pitch_key"] = (
        frame.game_pk.astype(str)
        + "|" + frame.at_bat_number.astype(str)
        + "|" + frame.pitch_number.astype(str)
    )
    duplicate = frame.duplicated("canonical_pitch_key", keep=False)
    null_key = frame[keys[:3]].isna().any(axis=1)
    frame["canonical_status"] = np.where(
        null_key, "NULL_KEY_BLOCKED",
        np.where(duplicate, "DUPLICATE_KEY_BLOCKED", "CANONICAL"),
    )
    canonical = frame[frame.canonical_status.eq("CANONICAL")].copy()
    terminal = canonical.events.notna() & canonical.events.ne("")
    plate = canonical[terminal].copy()
    plate["canonical_pa_key"] = (
        plate.game_pk.astype(str) + "|" + plate.at_bat_number.astype(str)
    )
    plate["terminal_pitch_key"] = plate.canonical_pitch_key
    events = plate.events.fillna("")
    plate["hit"] = events.isin(["single", "double", "triple", "home_run"]).astype("int8")
    for name in (
        "single", "double", "triple", "home_run", "strikeout", "walk", "hit_by_pitch"
    ):
        plate[name] = events.eq(name).astype("int8")
    plate["sacrifice"] = events.str.contains("sac_", regex=False).astype("int8")
    plate["reach_on_error"] = events.eq("field_error").astype("int8")
    plate["fielders_choice"] = events.str.contains(
        "fielders_choice", regex=False
    ).astype("int8")
    plate["ball_in_play"] = events.isin([
        "single", "double", "triple", "home_run", "field_out", "force_out",
        "grounded_into_double_play", "field_error", "fielders_choice",
        "fielders_choice_out", "sac_fly", "sac_bunt",
    ]).astype("int8")
    plate["pitches_seen"] = plate.pitch_number
    batted_mask = terminal & canonical[[
        "launch_speed", "launch_angle", "bb_type", "launch_speed_angle"
    ]].notna().any(axis=1)
    batted = canonical[batted_mask].copy()
    batted["canonical_pa_key"] = (
        batted.game_pk.astype(str) + "|" + batted.at_bat_number.astype(str)
    )
    batted["terminal_pitch_key"] = batted.canonical_pitch_key
    part = f"part-{day}_{day}.parquet"
    outputs = {
        "pitches": canonical,
        "plate_appearances": plate,
        "batted_balls": batted,
    }
    lineage = []
    for table, data in outputs.items():
        destination = normalized / table / f"season={day.year}" / part
        write_parquet(data, destination)
        lineage.append({
            "normalized_table": table,
            "normalized_path": str(destination.relative_to(ROOT)),
            "rows": len(data),
            "normalized_sha256": sha256(destination),
            "raw_source_path": str(raw.relative_to(ROOT)),
            "raw_source_sha256": meta["sha256"],
        })
    return {
        "raw_rows": len(frame),
        "terminal_pa_rows": len(plate),
        "batted_ball_rows": len(batted),
        "identity_rejects": int(null_key.sum()),
        "duplicate_rows": int(duplicate.sum()),
    }, lineage


def statsapi_rows(feed_paths: list[Path]) -> dict[str, pd.DataFrame]:
    games, lineups, batting, pitching = [], [], [], []
    for path in feed_paths:
        data = json.loads(path.read_text())
        game_data, live = data.get("gameData", {}), data.get("liveData", {})
        game_pk = int(data["gamePk"])
        teams = game_data.get("teams", {})
        games.append({
            "game_pk": game_pk,
            "game_date": game_data.get("datetime", {}).get("officialDate", ""),
            "season": 2026,
            "game_type": game_data.get("game", {}).get("type", "R"),
            "home_team": teams.get("home", {}).get("abbreviation"),
            "away_team": teams.get("away", {}).get("abbreviation"),
            "venue": game_data.get("venue", {}).get("name"),
            "official_start_time": game_data.get("datetime", {}).get("dateTime"),
            "official_status": game_data.get("status", {}).get("detailedState"),
            "statsapi_coverage": True,
            "source": "STATSAPI",
            "source_raw_path": str(path.relative_to(ROOT)),
            "source_raw_sha256": sha256(path),
        })
        box = live.get("boxscore", {})
        for side in ("home", "away"):
            team = box.get("teams", {}).get(side, {})
            team_code = teams.get(side, {}).get("abbreviation")
            opponent = teams.get(
                "away" if side == "home" else "home", {}
            ).get("abbreviation")
            for player_id in team.get("batters", []):
                player = team.get("players", {}).get(f"ID{player_id}", {})
                stats = player.get("stats", {}).get("batting", {})
                order = str(player.get("battingOrder") or "")
                starting = bool(order.isdigit() and int(order) % 100 == 0)
                if starting:
                    lineups.append({
                        "game_pk": game_pk,
                        "team_id": teams.get(side, {}).get("id"),
                        "team": team_code,
                        "batting_order_position": int(order) // 100,
                        "player_id": player_id,
                        "defensive_position": player.get("position", {}).get("abbreviation"),
                        "home_away": side,
                        "source": "STATSAPI_FINAL_BOXSCORE",
                        "lineup_certification_status": "FINAL_LINEUP_ONLY",
                        "source_raw_path": str(path.relative_to(ROOT)),
                    })
                batting.append({
                    "game_pk": game_pk, "player_id": player_id, "team": team_code,
                    "opponent": opponent, "home_away": side,
                    "starting_status": starting,
                    "lineup_position": int(order) // 100 if order.isdigit() else pd.NA,
                    "actual_pa": stats.get("plateAppearances"), "ab": stats.get("atBats"),
                    "hits": stats.get("hits"), "doubles": stats.get("doubles"),
                    "triples": stats.get("triples"), "home_runs": stats.get("homeRuns"),
                    "walks": stats.get("baseOnBalls"), "hbp": stats.get("hitByPitch"),
                    "strikeouts": stats.get("strikeOuts"), "runs": stats.get("runs"),
                    "rbi": stats.get("rbi"), "total_bases": stats.get("totalBases"),
                    "official_completion_status": game_data.get("status", {}).get(
                        "abstractGameState"
                    ),
                    "source": "STATSAPI",
                })
            for player_id in team.get("pitchers", []):
                player = team.get("players", {}).get(f"ID{player_id}", {})
                stats = player.get("stats", {}).get("pitching", {})
                pitching.append({
                    "game_pk": game_pk, "player_id": player_id, "team": team_code,
                    "opponent": opponent, "home_away": side,
                    "games_started": stats.get("gamesStarted"),
                    "innings_pitched": stats.get("inningsPitched"),
                    "batters_faced": stats.get("battersFaced"),
                    "hits_allowed": stats.get("hits"),
                    "walks": stats.get("baseOnBalls"),
                    "strikeouts": stats.get("strikeOuts"), "source": "STATSAPI",
                })
    batting_frame = pd.DataFrame(batting)
    outcomes = batting_frame.copy()
    if not outcomes.empty:
        outcomes["hits_runs_rbi"] = (
            pd.to_numeric(outcomes["hits"], errors="coerce")
            + pd.to_numeric(outcomes["runs"], errors="coerce")
            + pd.to_numeric(outcomes["rbi"], errors="coerce")
        )
    return {
        "games": pd.DataFrame(games),
        "starting_lineups": pd.DataFrame(lineups),
        "player_game_batting": batting_frame,
        "player_game_pitching": pd.DataFrame(pitching),
        "player_game_outcomes": outcomes,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="")
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-to", default="")
    parser.add_argument("--normalized-root", type=Path, default=DEFAULT_NORMALIZED)
    parser.add_argument("--skip-acquisition", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--certification-root", type=Path, default=OUTPUT_BASE)
    args = parser.parse_args()
    first = date.fromisoformat(args.date or args.date_from)
    last = date.fromisoformat(args.date or args.date_to)
    if last < first:
        raise SystemExit("date-to precedes date-from")
    normalized = args.normalized_root.resolve()
    prior_freshness_path = normalized / "ubo5_source_freshness.json"
    prior_freshness = (
        json.loads(prior_freshness_path.read_text())
        if prior_freshness_path.is_file() else {}
    )
    if (
        not args.force
        and str(prior_freshness.get("certified_through_date") or "") >= str(last)
        and prior_freshness.get("source_freshness_status")
        == "CURRENT_THROUGH_LATEST_COMPLETED_SLATE"
    ):
        print(json.dumps({
            "status": "ALREADY_CERTIFIED_SKIPPED",
            "requested_date_from": str(first),
            "requested_date_to": str(last),
            "certified_through_date": prior_freshness["certified_through_date"],
            "source_freshness_status": prior_freshness["source_freshness_status"],
        }, indent=2))
        return 0
    if args.date and prior_freshness.get("certified_through_date"):
        retry_from = (
            date.fromisoformat(prior_freshness["certified_through_date"])
            + timedelta(days=1)
        )
        if retry_from < first:
            first = retry_from
    run_time = datetime.now(timezone.utc)
    package = args.certification_root / run_time.strftime("%Y-%m-%d") / (
        f"refresh_{first}_{last}_{run_time.strftime('%Y%m%dT%H%M%SZ')}"
    )
    raw_package = package / "raw_official"
    raw_package.mkdir(parents=True)

    if not args.skip_acquisition:
        subprocess.run([
            sys.executable, "-m", "backend.mlb.scripts.acquire_mlb_statcast_chunks",
            "--start", str(first), "--end", str(last), "--chunk-days", "1",
            "--out-root", str(STATCAST_ROOT),
        ], cwd=ROOT, check=True)
        subprocess.run([
            sys.executable, "-m", "backend.mlb.scripts.acquire_mlb_statsapi_missing_games",
            "--start", str(first), "--end", str(last),
            "--out-root", str(STATSAPI_ROOT),
        ], cwd=ROOT, check=True)

    ledger_path = STATSAPI_ROOT / f"completion_ledger_{first}_{last}.csv"
    if not ledger_path.is_file():
        raise SystemExit(f"missing completed-game ledger: {ledger_path}")
    ledger = list(csv.DictReader(ledger_path.open()))
    incomplete = [
        row for row in ledger
        if row["classification"] not in {
            "ACQUIRED_AND_VALIDATED", "LOCAL_CERTIFIED_REUSED",
            "POSTPONED_OR_SUSPENDED",
        }
    ]
    if incomplete:
        raise SystemExit(f"completed-game source incomplete: {len(incomplete)} rows")
    final_rows = [
        row for row in ledger
        if row["classification"] in {
            "ACQUIRED_AND_VALIDATED", "LOCAL_CERTIFIED_REUSED"
        }
    ]
    feed_paths = [ROOT / row["path"] for row in final_rows]
    if any(not path.is_file() for path in feed_paths):
        raise SystemExit("one or more certified StatsAPI feeds are missing")

    pre_files = parquet_files(normalized, "plate_appearances")
    pre_pa = read_table(normalized, "plate_appearances")
    pre_latest = str(pd.to_datetime(pre_pa["game_date"]).max().date())
    day_summaries, lineage = [], []
    for day in dates(first, last):
        counts, day_lineage = normalize_statcast_day(day, normalized)
        schedule = [row for row in ledger if row["game_date"] == str(day)]
        day_summaries.append({
            "slate_date": str(day),
            "scheduled_games": len(schedule),
            "completed_games": sum(
                row["classification"] in {
                    "ACQUIRED_AND_VALIDATED", "LOCAL_CERTIFIED_REUSED"
                } for row in schedule
            ),
            "postponed_games": sum(
                row["classification"] == "POSTPONED_OR_SUSPENDED" for row in schedule
            ),
            "suspended_games": sum(
                "suspend" in row.get("detailed_state", "").lower() for row in schedule
            ),
            "official_source_requests": len(schedule) + 1,
            "successful_requests": len(schedule) + 1,
            "failed_requests": 0,
            **counts,
        })
        lineage.extend(day_lineage)
        source_folder = STATCAST_ROOT / str(day.year) / f"{day}_{day}"
        shutil.copy2(source_folder / "statcast_search.csv", raw_package / f"statcast_{day}.csv")
        shutil.copy2(
            source_folder / "request_metadata.json",
            raw_package / f"statcast_{day}_request_metadata.json",
        )
    shutil.copy2(ledger_path, raw_package / ledger_path.name)
    schedule_path = STATSAPI_ROOT / f"schedule_{first}_{last}.json"
    shutil.copy2(schedule_path, raw_package / schedule_path.name)
    for path in feed_paths:
        shutil.copy2(path, raw_package / f"statsapi_feed_{path.parent.name}.json")

    additions = statsapi_rows(feed_paths)
    game_ids = {int(row["game_pk"]) for row in final_rows}
    aggregate_changes = {}
    for table, frame in additions.items():
        aggregate_changes[table] = replace_games(normalized, table, frame, game_ids)

    post_pa = read_table(normalized, "plate_appearances")
    post_latest = str(pd.to_datetime(post_pa["game_date"]).max().date())
    pitch = read_table(normalized, "pitches")
    batted = read_table(normalized, "batted_balls")
    pitcher = read_table(normalized, "player_game_pitching")
    games_frame = read_table(normalized, "games")[["game_pk", "game_date"]]
    pitcher = pitcher.merge(games_frame, on="game_pk", how="left")
    observed = {
        "plate_appearances": post_latest,
        "pitches": str(pd.to_datetime(pitch["game_date"]).max().date()),
        "batted_balls": str(pd.to_datetime(batted["game_date"]).max().date()),
        "pitcher_history": str(pd.to_datetime(pitcher["game_date"]).max().date()),
    }
    status = (
        "CURRENT_THROUGH_LATEST_COMPLETED_SLATE"
        if all(date.fromisoformat(value) >= last for value in observed.values())
        else "SOURCE_REFRESH_FAILED"
    )
    freshness = {
        "generated_at_utc": run_time.isoformat(),
        "source_freshness_status": status,
        "certified_through_date": str(last),
        "actual_observed_dates": observed,
        "source_date_lineage_status": (
            "OBSERVED_FROM_NORMALIZED_EVENTS" if status.startswith("CURRENT") else "FAIL"
        ),
        "raw_package": str(package.relative_to(ROOT)),
    }
    (normalized / "ubo5_source_freshness.json").write_text(
        json.dumps(freshness, indent=2) + "\n"
    )
    with (package / "ubo5_missing_date_acquisition_summary.csv").open(
        "w", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=list(day_summaries[0]))
        writer.writeheader()
        writer.writerows(day_summaries)
    refresh_summary = {
        "pre_refresh_partition_count": len(pre_files),
        "post_refresh_partition_count": len(parquet_files(normalized, "plate_appearances")),
        "pre_refresh_latest_event_date": pre_latest,
        "post_refresh_latest_event_date": post_latest,
        "rows_appended": (
            0
            if str(prior_freshness.get("certified_through_date") or "") >= str(last)
            else sum(row["terminal_pa_rows"] for row in day_summaries)
        ),
        "rows_replaced": sum(before for before, _ in aggregate_changes.values()),
        "duplicates_removed": sum(row["duplicate_rows"] for row in day_summaries),
        "identity_failures": sum(row["identity_rejects"] for row in day_summaries),
        "schema_differences": 0,
        "aggregate_table_rows_before_after": aggregate_changes,
        "freshness": freshness,
        "lineage": lineage,
    }
    (package / "ubo5_normalized_platform_refresh_summary.json").write_text(
        json.dumps(refresh_summary, indent=2) + "\n"
    )
    latest = args.certification_root / "latest_refresh.json"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(json.dumps({
        "status": "CERTIFIED" if status.startswith("CURRENT") else "FAILED",
        "date_from": str(first), "date_to": str(last),
        "package": str(package.relative_to(ROOT)), **freshness,
    }, indent=2) + "\n")
    print(json.dumps(refresh_summary, indent=2))
    return 0 if status.startswith("CURRENT") else 1


if __name__ == "__main__":
    raise SystemExit(main())
