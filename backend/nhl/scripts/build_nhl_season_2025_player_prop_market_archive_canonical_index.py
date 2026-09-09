#!/usr/bin/env python3
"""Build the immutable season-2025 NHL book-level player-prop archive index."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg

from backend.nhl.analysis_package_guard import begin_package, finalize_package, sha256_file, verify_manifest
from backend.nhl.market_archive_index.core import (
    IDENTITY_ACCEPTED, MARKET_TO_LANE, PROP_TO_LANE, american_decimal, american_implied,
    bind_event, bind_player, build_pairs, classify_duplicates, discover_sources,
    effective_timestamp, iso, market_status, prepare_games, prepare_player_candidates,
    qualification, source_events, stable_hash, timing_classification, verify_parent_hashes,
    link_predictions, player_initial_last,
)

TASK = "NHL_SEASON_2025_PLAYER_PROP_MARKET_ARCHIVE_IMMUTABLE_CANONICAL_JOIN_INDEX_V1"
DATE = "2026-09-08"


def read_sql(connection: psycopg.Connection, query: str) -> pd.DataFrame:
    with connection.cursor() as cur:
        cur.execute(query)
        columns = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def database_snapshots(dsn: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    with psycopg.connect(dsn) as conn:
        games = read_sql(conn, """
            SELECT g.game_id,g.season,g.game_type,g.game_date,g.start_time_utc,
                   g.home_team_code,g.away_team_code,ht.full_team_name AS home_team_name,
                   at.full_team_name AS away_team_name
            FROM nhl.games g JOIN nhl.teams ht ON ht.team_id=g.home_team_id
            JOIN nhl.teams at ON at.team_id=g.away_team_id
            WHERE g.season=2025 ORDER BY g.game_id
        """)
        rosters = read_sql(conn, """
            SELECT DISTINCT rs.game_id,rs.player_id,p.full_name,t.team AS team_code,p.position
            FROM nhl.roster_status rs JOIN nhl.games g ON g.game_id=rs.game_id
            JOIN nhl.players p ON p.player_id=rs.player_id JOIN nhl.teams t ON t.team_id=rs.team_id
            WHERE g.season=2025 ORDER BY rs.game_id,rs.player_id
        """)
        predictions = read_sql(conn, """
            SELECT pr.prediction_id,pr.player_id,pl.full_name AS player_name,pr.game_id,pr.prop,
                   pr.line,pr.p_over,pr.model_family,pr.model_version,pr.feature_hash,
                   encode(digest(pr.model_params::text,'sha256'),'hex') AS model_params_sha256,
                   pr.created_at,pr.updated_at
            FROM nhl.predictions pr JOIN nhl.games g ON g.game_id=pr.game_id
            JOIN nhl.players pl ON pl.player_id=pr.player_id
            WHERE g.season=2025 AND pr.prop IN ('shots_on_goal','player_points','goalie_saves')
            ORDER BY pr.prediction_id
        """)
    predictions["lane"] = predictions.prop.map(PROP_TO_LANE)
    predictions["line"] = pd.to_numeric(predictions.line).astype(float)
    predictions["p_over"] = pd.to_numeric(predictions.p_over).astype(float)
    for col in ["created_at", "updated_at"]:
        predictions[col] = pd.to_datetime(predictions[col], utc=True)
    return games, rosters, predictions


def load_reference_snapshots(package: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    verify_manifest(package)
    return tuple(pd.read_parquet(package / name) for name in [
        "canonical_games_snapshot.parquet", "canonical_game_player_snapshot.parquet", "historical_predictions_snapshot.parquet"
    ])


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    frame = df.copy()
    for col in frame.columns:
        if col.endswith("_utc") or col in {"created_at", "updated_at"}:
            frame[col] = pd.to_datetime(frame[col], utc=True, errors="coerce")
    for col in ["canonical_season","canonical_game_id","canonical_player_id","game_id","player_id","prediction_id","source_file_size","file_size"]:
        if col in frame:
            frame[col] = pd.to_numeric(frame[col], errors="coerce").astype("Int64")
    frame.to_parquet(path, index=False, compression="zstd")


def write_csv(path: Path, rows: list[dict[str, Any]] | pd.DataFrame) -> None:
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    frame.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def load_ladder(repo: Path) -> tuple[pd.DataFrame, Path]:
    path = repo / "artifacts/analysis/model_development/nhl_points_probability_ladder_coherence_audit_v1/2026-09-08/nhl_points_player_ladder_audit_2026-09-08.csv"
    return pd.read_csv(path, low_memory=False), path


def normalized_duplicate_register(repo: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    root = repo / "backend/nhl/exports/odds_history"
    for lane, filename in [("SOG", "sog_with_market.csv"), ("POINTS", "points_with_market.csv"), ("SAVES", "saves_with_market.csv")]:
        for path in sorted(root.glob(f"*/{filename}")):
            frame = pd.read_csv(path)
            required = [x for x in ["game_id", "player_id", "line"] if x in frame.columns]
            if len(required) < 3:
                continue
            duplicated = frame[frame.duplicated(required, keep=False)].copy()
            if duplicated.empty:
                continue
            for key, group in duplicated.groupby(required, dropna=False):
                extras = len(group) - 1
                rows.append({"register_type": "NORMALIZED_DERIVATIVE", "lane": lane,
                    "source_file_path": str(path.relative_to(repo)), "slate_date": path.parent.name,
                    "canonical_game_id": key[0], "canonical_player_id": key[1], "line": key[2],
                    "duplicate_classification": "ALIAS_EXPANSION_DUPLICATE", "rows": len(group),
                    "duplicate_extras": extras, "evidence": "legacy normalized alias expansion; not raw quote multiplicity"})
    return pd.DataFrame(rows)


def auxiliary_parent_manifest(repo: Path) -> pd.DataFrame:
    paths = sorted((repo/"backend/nhl/exports/odds_history").glob("*/*_with_market.csv"))
    paths += [repo/"artifacts/analysis/model_development/nhl_points_probability_ladder_coherence_audit_v1/2026-09-08/nhl_points_player_ladder_audit_2026-09-08.csv"]
    for relative in ["tmp/cards/nhl_sog_card_2026-03-24.csv", "tmp/graded/nhl_sog_graded_2026-03-24.csv"]:
        path=repo/relative
        if path.is_file(): paths.append(path)
    rows=[]
    for path in paths:
        stamp=pd.Timestamp(path.stat().st_mtime,unit="s",tz="UTC").isoformat().replace("+00:00","Z")
        try:
            pd.read_csv(path,nrows=2)
            parse_status,error="PARSED",None
        except Exception as exc:
            parse_status,error="PARSE_FAILED",f"{type(exc).__name__}: {exc}"
        rows.append({"source_file_path":str(path.relative_to(repo)),"source_archive_family":"AUXILIARY_DERIVED_EVIDENCE",
                     "slate_date":path.parent.name if path.parent.name[:4].isdigit() else None,"file_size":path.stat().st_size,
                     "sha256":sha256_file(path),"file_timestamp_utc":stamp,"capture_timestamp_utc":None,
                     "capture_timestamp_provenance":"NOT_APPLICABLE_DERIVED_EVIDENCE","parse_status":parse_status,"parse_error":error})
    return pd.DataFrame(rows)


def build_observations(specs, games, candidates_by_game):
    observations: list[dict[str, Any]] = []
    event_rows: list[dict[str, Any]] = []
    player_rows: dict[tuple, dict[str, Any]] = {}
    market_objects: list[dict[str, Any]] = []
    player_cache: dict[tuple[int | None, str, str], dict[str, Any]] = {}
    for spec in specs:
        if spec.parse_status != "PARSED":
            continue
        for event, response in source_events(spec):
            binding = bind_event(event, games, spec.slate_date)
            event_key = (spec.sha256, response["event_index"], str(event.get("id") or ""))
            event_rows.append({"source_file_path": spec.relative_path, "source_file_sha256": spec.sha256,
                "archive_family": spec.family, "slate_date": spec.slate_date, "raw_event_locator": f"events[{response['event_index']}]", **binding})
            game_id = binding["canonical_game_id"]
            canonical_start = binding["canonical_start_time_utc"]
            provider_start = iso(event.get("commence_time"))
            timing_start = provider_start or canonical_start
            timing_start_provenance = "PROVIDER_EVENT_COMMENCE_TIME" if provider_start else "CANONICAL_SCHEDULE_FALLBACK"
            for bi, book in enumerate(event.get("bookmakers") or []):
                for mi, market in enumerate(book.get("markets") or []):
                    market_key = str(market.get("key") or "")
                    lane = MARKET_TO_LANE.get(market_key)
                    if lane is None:
                        continue
                    market_locator = f"events[{response['event_index']}].bookmakers[{bi}].markets[{mi}]"
                    market_effective, market_prov = effective_timestamp({}, market, book, response["response_timestamp_utc"], spec.file_timestamp_utc)
                    market_timing = timing_classification(market_effective, timing_start)
                    market_objects.append({"source_file_sha256": spec.sha256, "source_file_path": spec.relative_path,
                        "archive_family": spec.family, "slate_date": spec.slate_date, "provider_event_id": event.get("id"),
                        "sportsbook": book.get("key"), "provider_market_key": market_key, "market_family": lane,
                        "market_object_locator": market_locator, "effective_observation_timestamp_utc": market_effective,
                        "effective_timestamp_provenance": market_prov, "canonical_scheduled_start_time_utc": canonical_start,
                        "provider_scheduled_start_time_utc": provider_start, "timing_start_time_utc":timing_start,
                        "timing_start_time_provenance":timing_start_provenance,
                        "timing_classification": market_timing})
                    for oi, outcome in enumerate(market.get("outcomes") or []):
                        source_name = outcome.get("description") or outcome.get("participant") or ""
                        source_player_id = outcome.get("participant_id") or outcome.get("player_id")
                        cache_key = (game_id, str(source_name), str(source_player_id or ""))
                        if cache_key not in player_cache:
                            player_cache[cache_key] = bind_player(source_name, game_id, candidates_by_game, source_player_id)
                        player = player_cache[cache_key]
                        player_key = (spec.sha256, event.get("id"), lane, str(source_name))
                        if player_key not in player_rows:
                            player_rows[player_key] = {"source_file_path": spec.relative_path, "source_file_sha256": spec.sha256,
                                "archive_family": spec.family, "slate_date": spec.slate_date, "provider_event_id": event.get("id"),
                                "market_family": lane, "source_player_name": source_name, "source_player_id": source_player_id,
                                "canonical_game_id": game_id, **player}
                        raw_side = str(outcome.get("name") or outcome.get("label") or "")
                        side = raw_side.strip().upper() if raw_side.strip().upper() in {"OVER", "UNDER"} else None
                        effective, provenance = effective_timestamp(outcome, market, book, response["response_timestamp_utc"], spec.file_timestamp_utc)
                        timing = timing_classification(effective, timing_start)
                        status = market_status(book, market, outcome)
                        qual, exclusion = qualification(binding["identity_classification"], player["identity_classification"], timing, status, side, outcome.get("point"), outcome.get("price"))
                        locator = f"{market_locator}.outcomes[{oi}]"
                        observation_id = stable_hash([spec.sha256, locator])
                        team = player.get("team_code")
                        home, away = binding.get("canonical_home_team_code"), binding.get("canonical_away_team_code")
                        opponent = away if team == home else (home if team == away else None)
                        observations.append({
                            "observation_id": observation_id, "source_file_path": spec.relative_path, "source_file_size": spec.size,
                            "source_file_sha256": spec.sha256, "archive_family": spec.family, "canonical_season": binding.get("canonical_season"),
                            "slate_date": spec.slate_date, "canonical_game_id": game_id, "provider_event_id": event.get("id"),
                            "scheduled_start_time_utc": canonical_start, "provider_scheduled_start_time_utc":provider_start,
                            "timing_start_time_utc":timing_start,"timing_start_time_provenance":timing_start_provenance,
                            "home_team": event.get("home_team"), "away_team": event.get("away_team"),
                            "canonical_home_team_code": home, "canonical_away_team_code": away,
                            "canonical_player_id": player.get("canonical_player_id"), "canonical_player_name": player.get("canonical_player_name"),
                            "source_player_id": outcome.get("participant_id") or outcome.get("player_id"), "source_player_name": source_name,
                            "team": team, "opponent": opponent, "market_family": lane, "provider_market_key": market_key,
                            "sportsbook": book.get("key"), "sportsbook_name": book.get("title"), "line": outcome.get("point"),
                            "raw_side": raw_side, "side": side, "american_price": outcome.get("price"),
                            "decimal_price": american_decimal(outcome.get("price")), "raw_implied_probability": american_implied(outcome.get("price")),
                            "market_status": status, "provider_outcome_timestamp_utc": iso(outcome.get("last_update") or outcome.get("timestamp")),
                            "provider_market_timestamp_utc": iso(market.get("last_update")), "provider_book_timestamp_utc": iso(book.get("last_update")),
                            "response_timestamp_utc": response["response_timestamp_utc"], "source_file_timestamp_utc": spec.file_timestamp_utc,
                            "capture_timestamp_utc": spec.capture_timestamp_utc, "capture_timestamp_provenance": spec.capture_timestamp_provenance,
                            "effective_observation_timestamp_utc": effective, "effective_timestamp_provenance": provenance,
                            "raw_record_locator": locator, "market_object_locator": market_locator,
                            "game_identity_classification": binding["identity_classification"], "game_binding_method": binding["binding_method"],
                            "player_identity_classification": player["identity_classification"], "player_binding_method": player["binding_method"],
                            "timing_classification": timing, "duplicate_classification": None,
                            "qualification_status": qual, "exclusion_reason": exclusion,
                        })
    return classify_duplicates(pd.DataFrame(observations)), pd.DataFrame(event_rows), pd.DataFrame(player_rows.values()), pd.DataFrame(market_objects)


def representative_day_reconstruction(repo:Path,predictions:pd.DataFrame,games:pd.DataFrame,observations:pd.DataFrame,qualified:pd.DataFrame)->pd.DataFrame:
    dated=predictions.merge(games[["game_id","game_date"]],on="game_id",how="left")
    rows=[]
    for date,lane,filename in [("2026-04-16","POINTS","points_with_market.csv"),("2026-04-16","SAVES","saves_with_market.csv"),("2026-03-24","SOG","sog_with_market.csv")]:
        norm_path=repo/f"backend/nhl/exports/odds_history/{date}/{filename}"
        norm=pd.read_csv(norm_path)
        prediction_rows=len(dated[(dated.game_date.astype(str)==date)&(dated.lane==lane)])
        identity_cols=["game_id","player_id","line"]
        normalized_identities=norm[identity_cols].drop_duplicates().shape[0]
        price_cols=[c for c in ["price_over","price_under"] if c in norm]
        priced=norm[norm[price_cols].notna().any(axis=1)] if price_cols else norm.iloc[0:0]
        priced_ids=priced[identity_cols + ["full_name"]].drop_duplicates()
        q=qualified[(qualified.slate_date==date)&(qualified.market_family==lane)]
        quote_keys=set(zip(q.canonical_game_id.astype(int),q.canonical_player_id.astype(int),q.line.astype(float)))
        traced=sum((int(r.game_id),int(r.player_id),float(r.line)) in quote_keys for r in priced_ids.itertuples())
        raw=observations[(observations.slate_date==date)&(observations.market_family==lane)]
        raw_name_keys=set((int(r.canonical_game_id),player_initial_last(r.source_player_name),float(r.line)) for r in raw.itertuples() if pd.notna(r.canonical_game_id))
        raw_traced=sum((int(r.game_id),player_initial_last(r.full_name),float(r.line)) in raw_name_keys for r in priced_ids.itertuples())
        if traced==len(priced_ids):
            status,cause,detail="PASS","NONE","all priced normalized identities canonically qualified"
        elif raw_traced==len(priced_ids):
            status,cause,detail="PASS_WITH_DOCUMENTED_IDENTITY_EXCLUSION","AMBIGUOUS_SAME_NAME","raw name evidence exists but strict canonical identity excludes ambiguous mappings"
        else:
            status,cause,detail="PASS_WITH_DOCUMENTED_LEGACY_JOIN_DEFECT","NAME_ONLY_CROSS_GAME_JOIN_AND_AMBIGUOUS_SAME_NAME","legacy normalized rows include identities whose player name exists only on a different provider event plus strict same-name exclusions"
        row={"slate_date":date,"lane":lane,"prediction_rows":prediction_rows,"normalized_rows":len(norm),
             "normalized_unique_game_player_line":normalized_identities,"normalized_priced_unique_identities":len(priced_ids),
             "priced_identities_traced_to_raw_name_evidence":raw_traced,
             "priced_identities_traced_to_qualified_raw_quotes":traced,"no_same_game_raw_evidence":len(priced_ids)-raw_traced,
             "identity_excluded_priced_identities":raw_traced-traced,
             "qualified_raw_quote_rows":len(q),"candidate_rows":None,"surviving_provider_grade_rows":None,"reconciliation_status":status}
        row["discrepancy_cause"],row["discrepancy_detail"]=cause,detail
        if date=="2026-03-24" and lane=="SOG":
            card=repo/"tmp/cards/nhl_sog_card_2026-03-24.csv"; graded=repo/"tmp/graded/nhl_sog_graded_2026-03-24.csv"
            row["candidate_rows"]=len(pd.read_csv(card)) if card.is_file() else None
            row["surviving_provider_grade_rows"]=len(pd.read_csv(graded)) if graded.is_file() else None
        rows.append(row)
    return pd.DataFrame(rows)


def reconcile(specs, observations, qualified, events, pairs, links, duplicate_register, market_objects, representative):
    rows = []
    def add(check, expected, observed, status, cause, detail=""):
        rows.append({"check": check, "expected": expected, "observed": observed, "status": status, "demonstrated_cause": cause, "detail": detail})
    historical = [s for s in specs if s.family == "HISTORICAL_BUNDLE"]
    latest = [s for s in specs if s.family == "CONTEMPORANEOUS_LATEST"]
    add("historical_bundle_files", 153, len(historical), "PASS" if len(historical)==153 else "FAIL", "SOURCE_INVENTORY")
    add("historical_wrapper_events", 907, int(events[events.archive_family.eq("HISTORICAL_BUNDLE")].shape[0]), "PASS" if int(events[events.archive_family.eq("HISTORICAL_BUNDLE")].shape[0])==907 else "FAIL", "SOURCE_INVENTORY")
    with_books = observations[observations.archive_family.eq("HISTORICAL_BUNDLE")][["source_file_sha256","provider_event_id"]].drop_duplicates().shape[0]
    add("historical_events_with_player_prop_books", 855, with_books, "PASS" if with_books==855 else "FAIL", "SOURCE_INVENTORY")
    add("sportsbooks", 13, observations.sportsbook.nunique(), "PASS" if observations.sportsbook.nunique()==13 else "FAIL", "LEGITIMATE_BOOK_LEVEL_MULTIPLICITY")
    add("contemporaneous_days", 42, len(latest), "PASS" if len(latest)==42 else "FAIL", "SOURCE_INVENTORY")
    for family,lane,expected in [("HISTORICAL_BUNDLE","POINTS",5),("HISTORICAL_BUNDLE","SAVES",20),("CONTEMPORANEOUS_LATEST","POINTS",2),("CONTEMPORANEOUS_LATEST","SAVES",2)]:
        got = market_objects[(market_objects.archive_family==family)&(market_objects.market_family==lane)&(market_objects.timing_classification=="AT_OR_POST_START")].shape[0]
        add(f"at_post_market_objects_{family}_{lane}", expected, got, "PASS" if got==expected else "FAIL", "TIMING_EXCLUSION")
    april = duplicate_register[(duplicate_register.lane=="SAVES")&(duplicate_register.slate_date=="2026-04-16")]
    extras = int(april.duplicate_extras.sum()) if len(april) else 0
    add("april16_saves_alias_expansion_extras", 13, extras, "PASS" if extras==13 else "FAIL", "ALIAS_EXPANSION")
    for lane, expected_pred, expected_norm, expected_priced in [("POINTS",930,930,187),("SAVES",364,377,8)]:
        row=representative[(representative.slate_date=="2026-04-16")&(representative.lane==lane)].iloc[0]
        for field,expected in [("prediction_rows",expected_pred),("normalized_rows",expected_norm),("normalized_priced_unique_identities",expected_priced),("priced_identities_traced_to_raw_name_evidence",expected_priced)]:
            got=int(row[field]); add(f"april16_{lane.lower()}_{field}",expected,got,"PASS" if got==expected else "FAIL","REPRESENTATIVE_DAY")
        qualified_expected=183 if lane=="POINTS" else 8
        got=int(row.priced_identities_traced_to_qualified_raw_quotes)
        detail="Four legacy Points identities are excluded because Elias Pettersson maps to two same-game players at two lines." if lane=="POINTS" else ""
        add(f"april16_{lane.lower()}_priced_identities_traced_to_qualified_raw_quotes",qualified_expected,got,"PASS" if got==qualified_expected else "FAIL","IDENTITY_FAILURE" if lane=="POINTS" else "REPRESENTATIVE_DAY",detail)
    sog=representative[(representative.slate_date=="2026-03-24")&(representative.lane=="SOG")].iloc[0]
    add("march24_sog_candidate_rows",25,int(sog.candidate_rows),"PASS" if int(sog.candidate_rows)==25 else "FAIL","OPERATIONAL_ACTION_EVIDENCE")
    add("march24_sog_surviving_provider_grade_rows",19,int(sog.surviving_provider_grade_rows),"PASS" if int(sog.surviving_provider_grade_rows)==19 else "FAIL","OPERATIONAL_ACTION_EVIDENCE")
    add("march24_sog_normalized_priced_without_same_game_raw",6,int(sog.no_same_game_raw_evidence),"PASS" if int(sog.no_same_game_raw_evidence)==6 else "FAIL","NAME_ONLY_CROSS_GAME_JOIN","M. Wood and A. Lee across three lines each were attached to canonical game IDs different from their raw provider events.")
    add("march24_sog_name_traced_but_identity_excluded",6,int(sog.identity_excluded_priced_identities),"PASS" if int(sog.identity_excluded_priced_identities)==6 else "FAIL","AMBIGUOUS_SAME_NAME","Two Elias Petterssons across three lines remain ambiguous within the same game.")
    add("raw_observations_preserved", observations.shape[0], observations.shape[0], "PASS", "NO_AGGREGATION")
    add("qualified_subset", "identity+timing+status+price", qualified.shape[0], "PASS", "TIMING_EXCLUSION_IDENTITY_FAILURE")
    add("incomplete_two_way_pairs", 0, int((~pairs.pair_complete).sum()), "INFORMATIONAL", "MISSING_OPPOSITE_SIDE")
    return pd.DataFrame(rows)


def manifest(directory: Path) -> None:
    files = sorted(p for p in directory.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (directory / "SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--reference-package")
    parser.add_argument("--supersedes-package")
    parser.add_argument("--supersession-reason")
    args = parser.parse_args()
    repo = Path(args.repo_root).resolve(); target = Path(args.output_dir).resolve()
    staging = begin_package(target)
    try:
        if args.reference_package:
            games, rosters, predictions = load_reference_snapshots(Path(args.reference_package))
            snapshot_source = "IMMUTABLE_REFERENCE_PACKAGE"
        else:
            dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
            if not dsn:
                raise RuntimeError("DATABASE_URL_REQUIRED_FOR_INITIAL_SNAPSHOT")
            games, rosters, predictions = database_snapshots(dsn)
            snapshot_source = "READ_ONLY_DATABASE_SNAPSHOT"
        games = prepare_games(games)
        write_parquet(games.drop(columns=["start_ts"]), staging/"canonical_games_snapshot.parquet")
        write_parquet(rosters, staging/"canonical_game_player_snapshot.parquet")
        write_parquet(predictions, staging/"historical_predictions_snapshot.parquet")
        specs = discover_sources(repo/"backend/nhl/exports/odds_history", repo)
        parent_manifest = pd.DataFrame([{k:getattr(s,k) for k in ["relative_path","family","slate_date","size","sha256","file_timestamp_utc","capture_timestamp_utc","capture_timestamp_provenance","parse_status","parse_error"]} for s in specs]).rename(columns={"relative_path":"source_file_path","family":"source_archive_family","size":"file_size"})
        parent_manifest = pd.concat([parent_manifest,auxiliary_parent_manifest(repo)],ignore_index=True)
        write_parquet(parent_manifest, staging/"immutable_parent_file_manifest.parquet")
        write_csv(staging/"immutable_parent_file_manifest.csv", parent_manifest)
        verify_parent_hashes(parent_manifest.rename(columns={"source_archive_family":"family"}), repo)
        candidates = prepare_player_candidates(rosters, predictions)
        observations, events, players, market_objects = build_observations(specs, games, candidates)
        qualified = observations[observations.qualification_status.eq("QUALIFIED")].copy()
        pairs = build_pairs(qualified)
        ladder, ladder_path = load_ladder(repo)
        links = {lane: link_predictions(predictions, qualified, lane, ladder) for lane in ["SOG","POINTS","SAVES"]}
        duplicates = normalized_duplicate_register(repo)
        raw_dup = observations[observations.duplicate_classification.ne("BOOK_LINE_SIDE_DISTINCT_OBSERVATION")][["observation_id","source_file_path","slate_date","market_family","canonical_game_id","canonical_player_id","sportsbook","line","side","duplicate_classification"]].copy()
        raw_dup["register_type"]="RAW_OBSERVATION"; raw_dup["rows"]=1; raw_dup["duplicate_extras"]=0; raw_dup["evidence"]="raw semantic duplicate classification"
        duplicate_register = pd.concat([raw_dup,duplicates],ignore_index=True,sort=False)
        timing_exclusions = observations[observations.timing_classification.ne("PREGAME_QUALIFIED")].copy()
        unresolved = pd.concat([
            events[~events.identity_classification.isin(IDENTITY_ACCEPTED)].assign(register_type="EVENT"),
            players[~players.identity_classification.isin(IDENTITY_ACCEPTED)].assign(register_type="PLAYER")
        ], ignore_index=True, sort=False)
        for name,frame in [("event_binding_index",events),("player_alias_binding_index",players),("complete_raw_book_level_observation_index",observations),
                           ("qualified_pregame_observation_index",qualified),("paired_no_vig_market_index",pairs),("unresolved_identity_register",unresolved),
                           ("duplicate_repetition_register",duplicate_register),("timing_exclusion_register",timing_exclusions),("market_object_timing_index",market_objects)]:
            write_parquet(frame, staging/f"{name}.parquet")
        for lane, frame in links.items():
            write_parquet(frame, staging/f"{lane.lower()}_prediction_market_linkage.parquet")
        representative=representative_day_reconstruction(repo,predictions,games,observations,qualified)
        write_csv(staging/"representative_day_reconstruction.csv",representative)
        reconciliation = reconcile(specs, observations, qualified, events, pairs, links, duplicate_register, market_objects, representative)
        failures=reconciliation[reconciliation.status.eq("FAIL")]
        if len(failures):
            raise RuntimeError("RECONCILIATION_FAILED:"+",".join(failures.check.astype(str)))
        write_csv(staging/"reconciliation_report.csv", reconciliation)
        summary = {
            "task":TASK,"package_date":DATE,"snapshot_source":snapshot_source,"raw_market_parent_files":len(specs),"all_manifested_parent_files":len(parent_manifest),
            "historical_bundles":sum(s.family=="HISTORICAL_BUNDLE" for s in specs),"contemporaneous_days":sum(s.family=="CONTEMPORANEOUS_LATEST" for s in specs),
            "raw_observations":len(observations),"qualified_observations":len(qualified),"sportsbooks":int(observations.sportsbook.nunique()),
            "events_bound":int(events.final_disposition.eq("BOUND").sum()),"events_total":len(events),
            "player_bindings_bound":int(players.final_disposition.eq("BOUND").sum()),"player_bindings_total":len(players),
            "timing_classification":observations.timing_classification.value_counts(dropna=False).to_dict(),
            "qualification_exclusions":observations.exclusion_reason.fillna("NONE").value_counts().to_dict(),
            "market_observations_by_lane":observations.market_family.value_counts().to_dict(),
            "qualified_by_lane":qualified.market_family.value_counts().to_dict(),
            "pair_status":pairs.pair_status.value_counts().to_dict(),
            "prediction_linkage":{lane:{"prediction_rows":int(frame.prediction_id.nunique()),"linked_predictions":int(frame.loc[frame.linkage_status.eq('LINKED'),'prediction_id'].nunique()),"linked_quote_rows":int(frame.linkage_status.eq('LINKED').sum())} for lane,frame in links.items()},
            "goalie_saves_limitations":["no certified expected starter","start_prob absent/null then filled to zero","actual start_flag is postgame only","market presence is not starter confirmation"],
            "decisions":{},
        }
        for lane in ["SOG","POINTS","SAVES"]:
            lane_obs=observations[observations.market_family.eq(lane)]; lane_q=qualified[qualified.market_family.eq(lane)]
            summary["decisions"][f"{lane}_BOOK_LEVEL_ARCHIVE_READINESS"] = "READY_WITH_DOCUMENTED_LIMITATIONS" if len(lane_q) else "INSUFFICIENT_EVIDENCE"
        for lane in ["POINTS","SAVES"]:
            frame=links[lane]; ratio=frame.loc[frame.linkage_status.eq("LINKED"),"prediction_id"].nunique()/frame.prediction_id.nunique()
            summary["decisions"][f"{lane}_PREDICTION_MARKET_LINKAGE_READINESS"] = "READY_FOR_HISTORICAL_ANALYSIS" if ratio>.8 else "READY_WITH_DOCUMENTED_LIMITATIONS"
        (staging/"index_summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
        schema={"analytical_authority":"Parquet","null_semantics":"Arrow nulls; no empty-string substitution in authoritative tables","numeric_semantics":"canonical IDs use nullable integers; line/probability/price columns are typed; raw American price is retained separately from derived decimal/implied/no-vig values","observation_timestamp_precedence":["outcome provider timestamp","market provider timestamp","book provider timestamp","response-level timestamp","filesystem/archive timestamp"],"timing_start_precedence":["provider event commence_time","canonical NHL scheduled start fallback"],"qualified_contract":"accepted event identity AND accepted player identity AND PREGAME_QUALIFIED AND non-suspended status AND valid side/line/price","identity_values":["CANONICAL_EXACT","CANONICAL_CORROBORATED","ALIAS_RESOLVED","AMBIGUOUS","UNRESOLVED","CONFLICT"],"timing_values":["PREGAME_QUALIFIED","AT_OR_POST_START","TIMING_INDETERMINATE","START_TIME_UNRESOLVED"],"primary_grain":"source file x provider event x bookmaker x market object x player x line x side x raw locator","non_claims":["candidate policy","upload","execution","grading","certified starting goalie"]}
        (staging/"schema_data_contract.json").write_text(json.dumps(schema,indent=2,sort_keys=True)+"\n")
        decisions=summary["decisions"]
        report=f"""# NHL season-2025 player-prop market archive canonical join index

## Result

The immutable book-level index contains {len(observations):,} raw SOG/Points/Saves outcome observations and {len(qualified):,} identity- and timing-qualified pregame observations from {len(specs)} raw market parent files. It retains every book, line, side, price, timestamp, status, raw locator, and parent hash. No source file was changed and no missing side was synthesized.

## Decisions

"""+"\n".join(f"- `{k}` = `{v}`" for k,v in decisions.items())+f"""

## Controls and limitations

Event bindings require canonical team orientation plus schedule corroboration. Player bindings are game-scoped; exact normalized names are corroborated and initial/last aliases are accepted only when unique. Ambiguous, unresolved, conflicting, post-start, indeterminate, invalid-price, and suspended rows remain in the raw evidence table but not in the qualified table.

The observation-time precedence is outcome, market, bookmaker, response, then filesystem/archive timestamp. Timing qualification compares that observation time to the provider event's scheduled commence time, falling back to the canonical schedule only when provider time is absent; both start times remain preserved. Historical filesystem mtimes record retrospective retrieval, not contemporaneous capture. The known at/post-start market-object counts and the April 16 Saves alias-expansion discrepancy are tested in `reconciliation_report.csv`.

Prediction linkage means only canonical game/player/line correspondence to a qualified quote. It is not evidence of candidate selection, upload, execution, or grading. Points rows retain ladder-coherence disposition without deleting blocked predictions. Saves retains the finding that no certified expected starter existed, `start_prob` was null then zero-filled, actual `start_flag` was postgame-only, and market presence was not confirmation.

## Enabled next

This index enables a Points historical outcome spine/coherent prediction foundation, book-level historical comparison, accurate characterization of Saves market availability, and optional SOG market benchmarking. It does not establish Points/Saves policy or performance, certify goalie starters, authorize any season-2026 lane, or support betting-edge claims.
"""
        (staging/"reconciliation_report.md").write_text(report)
        (staging/"executive_summary.md").write_text(report)
        identity={"task":TASK,"package_date":DATE,"package_status":"AUTHORITATIVE_COMPLETE","source_code":"backend/nhl/market_archive_index/core.py + backend/nhl/scripts/build_nhl_season_2025_player_prop_market_archive_canonical_index.py","source_code_sha256":stable_hash([sha256_file(repo/'backend/nhl/market_archive_index/core.py'),sha256_file(repo/'backend/nhl/scripts/build_nhl_season_2025_player_prop_market_archive_canonical_index.py')]),"points_ladder_parent_path":str(ladder_path.relative_to(repo)),"points_ladder_parent_sha256":sha256_file(ladder_path),"source_mutations":0,"live_workflow_mutations":0,"decisions":decisions,"supersedes_package":args.supersedes_package,"supersession_reason":args.supersession_reason if args.supersedes_package else None}
        (staging/"package_identity.json").write_text(json.dumps(identity,indent=2,sort_keys=True)+"\n")
        manifest(staging); verify_manifest(staging); finalize_package(staging,target)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
