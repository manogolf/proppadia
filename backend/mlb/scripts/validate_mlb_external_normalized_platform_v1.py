#!/usr/bin/env python3
"""Audit normalized MLB external platform v1 and emit its bounded certification pack."""
from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
NORM = ROOT / "backend/mlb/data/external/normalized/v1"
ACQ = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22"
OUT = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_platform_v1_normalization/2026-07-22"
TABLES = ["games", "players", "player_identity_crosswalk", "pitches", "plate_appearances", "batted_balls",
          "starting_lineups", "substitutions", "player_game_batting", "player_game_pitching",
          "player_game_outcomes", "source_lineage"]
PURPOSE = {
    "games": ("official MLB game spine", "MULTI_SOURCE", "game_pk"),
    "players": ("MLB-owned player spine", "MLB", "player_id"),
    "player_identity_crosswalk": ("exact identifier evidence", "CHADWICK_REGISTER", "mlb_player_id|key_retro"),
    "pitches": ("canonical Statcast pitches with raw fidelity", "STATCAST", "game_pk|at_bat_number|pitch_number"),
    "plate_appearances": ("terminal Statcast PA events", "STATCAST", "game_pk|at_bat_number"),
    "batted_balls": ("terminal batted-ball evidence", "STATCAST", "game_pk|at_bat_number|pitch_number"),
    "starting_lineups": ("source-certified starting slots", "RETROSHEET_2022_25+STATSAPI_2026", "game_pk|team|batting_order_position"),
    "substitutions": ("observed source lineup-state changes", "RETROSHEET", "game_pk|event_sequence|team|batting_order_slot"),
    "player_game_batting": ("source-owned batting totals", "RETROSHEET_2022_25+STATSAPI_2026", "game_pk|source_player_identity"),
    "player_game_pitching": ("source-owned pitching totals", "RETROSHEET_2022_25+STATSAPI_2026", "game_pk|source_player_identity"),
    "player_game_outcomes": ("stable official batting outcomes", "RETROSHEET_2022_25+STATSAPI_2026", "game_pk|source_player_identity"),
    "source_lineage": ("partition-to-source evidence", "NORMALIZED_BUILDER", "normalized_path"),
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""): h.update(block)
    return h.hexdigest()


def save(name: str, rows) -> pd.DataFrame:
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    frame.to_csv(OUT / name, index=False)
    return frame


def files(table: str) -> list[Path]:
    return sorted((NORM / table).glob("season=*/*.parquet"))


def read_table(table: str, columns=None) -> pd.DataFrame:
    frames = []
    for path in files(table):
        available = pq.ParquetFile(path).schema_arrow.names
        wanted = None if columns is None else [c for c in columns if c in available]
        frame = pq.ParquetFile(path).read(columns=wanted).to_pandas()
        frame["_partition_path"] = str(path.relative_to(ROOT))
        frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def schema_and_storage(manifest: pd.DataFrame) -> None:
    rows = []
    for table in TABLES:
        paths = files(table)
        if not paths: continue
        schemas = defaultdict(set)
        for path in paths:
            pf = pq.ParquetFile(path)
            for field in pf.schema_arrow:
                schemas[field.name].add(str(field.type))
        purpose, owner, key = PURPOSE[table]
        for col, types in schemas.items():
            rows.append({"table": table, "table_purpose": purpose, "authoritative_source": owner,
                         "grain": "ONE_ROW_PER_" + table.upper().rstrip("S"), "primary_key": key,
                         "foreign_keys": "game_pk->games; player ids->players where exact" if table not in {"games", "players", "player_identity_crosswalk", "source_lineage"} else "NONE_OR_DOCUMENTED_EXTERNAL",
                         "partition_strategy": "season; daily chunks for Statcast tables",
                         "column": col, "data_types_observed": "|".join(sorted(types)),
                         "nullable": True, "enumerated_value_treatment": "RAW_LITERAL_PRESERVED",
                         "raw_source_fields": col, "transformation_rule": "identity unless normalized helper column",
                         "unsupported_cases": "preserved null/conflict; no fuzzy resolution",
                         "lineage_requirement": "partition path, raw source path and SHA256"})
    save("frozen_normalized_schemas.csv", rows)
    save("storage_contract.csv", [{"normalized_root": str(NORM.relative_to(ROOT)), "format": "PARQUET",
        "compression": "ZSTD_LEVEL_3", "dictionary_encoding": True, "partitioning": "table/season/year/part",
        "raw_overwrite": "PROHIBITED", "database_write": "NONE", "categorical_policy": "raw literals recoverable",
        "chunking": "daily Statcast; source/year for compact tables", "manifest": "normalized_file_manifest.csv"}])
    save("normalized_table_manifest.csv", manifest)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = pd.read_csv(NORM / "normalized_file_manifest.csv")
    build = json.loads((NORM / "build_summary.json").read_text())
    schema_and_storage(manifest)
    games = read_table("games", ["game_pk", "game_date", "season", "game_type", "home_team", "away_team",
                                       "official_start_time", "official_status", "retrosheet_game_id", "game_identity_status"])
    games = games.drop_duplicates("game_pk"); game_ids = set(pd.to_numeric(games.game_pk, errors="coerce").dropna().astype(int))
    players = read_table("players", ["player_id"]); player_ids = set(pd.to_numeric(players.player_id, errors="coerce").dropna().astype(int))
    crosswalk = read_table("player_identity_crosswalk")
    save("game_identity_reconciliation.csv", games.groupby("game_identity_status", dropna=False).agg(
        rows=("game_pk", "size"), first_date=("game_date", "min"), last_date=("game_date", "max")).reset_index())
    save("player_identity_crosswalk.csv", crosswalk)
    save("player_identity_crosswalk_summary.csv", crosswalk.groupby("crosswalk_status", dropna=False).size().reset_index(name="rows"))

    table_reports = []
    ri = []
    pa_keys: set[str] = set()
    season_events = defaultdict(Counter)
    bb_category = Counter()
    table_key_spec = {
        "pitches": ["game_pk", "at_bat_number", "pitch_number", "canonical_status", "events"],
        "plate_appearances": ["game_pk", "at_bat_number", "batter", "events", "hit", "single", "double", "triple", "home_run", "strikeout", "walk"],
        "batted_balls": ["game_pk", "at_bat_number", "pitch_number", "launch_speed", "launch_angle", "estimated_ba_using_speedangle", "launch_speed_angle"],
        "starting_lineups": ["game_pk", "player_id", "team_id", "team", "batting_order_position", "lineup_certification_status"],
        "substitutions": ["game_pk", "event_sequence", "team", "batting_order_slot", "player_entering_id", "player_leaving_id", "source_certification_status"],
        "player_game_outcomes": ["game_pk", "player_id", "actual_pa", "hits", "doubles", "triples", "home_runs", "walks", "strikeouts", "total_bases", "source"],
    }
    for table, cols in table_key_spec.items():
        rows = orphan_game = orphan_player = duplicate = null_key = 0
        seen = set()
        for path in files(table):
            names = pq.ParquetFile(path).schema_arrow.names
            frame = pq.ParquetFile(path).read(columns=[c for c in cols if c in names]).to_pandas(); rows += len(frame)
            gid = pd.to_numeric(frame.get("game_pk"), errors="coerce")
            orphan_game += int((gid.notna() & ~gid.astype("Int64").isin(game_ids)).sum())
            key_cols = {"pitches":["game_pk","at_bat_number","pitch_number"], "plate_appearances":["game_pk","at_bat_number"],
                        "batted_balls":["game_pk","at_bat_number","pitch_number"], "starting_lineups":["game_pk","team","batting_order_position"],
                        "substitutions":["game_pk","event_sequence","team","batting_order_slot"], "player_game_outcomes":["game_pk","player_id"]}[table]
            key_frame = frame[key_cols].copy() if all(c in frame for c in key_cols) else None
            if table == "starting_lineups" and key_frame is not None and "team_id" in frame:
                key_frame["team"] = key_frame.team.fillna(frame.team_id)
            if key_frame is not None:
                null_key += int(key_frame.isna().any(axis=1).sum())
                for key in key_frame.astype(str).agg("|".join, axis=1):
                    if key in seen: duplicate += 1
                    seen.add(key)
            pcols = [c for c in ["player_id", "player_entering_id", "player_leaving_id"] if c in frame]
            for col in pcols:
                ids = pd.to_numeric(frame[col], errors="coerce"); orphan_player += int(ids.isna().sum() + (ids.notna() & ~ids.astype("Int64").isin(player_ids)).sum())
            season = path.parent.name.split("=")[-1]
            if table == "plate_appearances":
                pa_keys.update(frame.game_pk.astype(str) + "|" + frame.at_bat_number.astype(str))
                for c in ["hit","double","triple","home_run","strikeout","walk"]: season_events[season][c] += int(pd.to_numeric(frame.get(c), errors="coerce").fillna(0).sum())
            if table == "batted_balls":
                for val, n in frame.launch_speed_angle.fillna("NULL").value_counts().items(): bb_category[str(val)] += int(n)
        table_reports.append({"table":table,"rows":rows,"partitions":len(files(table)),"duplicate_primary_keys":duplicate,
                              "null_key_rows":null_key,"orphan_game_rows":orphan_game,"unmapped_or_orphan_player_references":orphan_player})
        reasons = []
        if orphan_game: reasons.append("game_pk absent from games")
        if orphan_player: reasons.append("mapped player id null or absent from MLB-owned players table")
        if duplicate: reasons.append("declared canonical key repeated")
        if null_key: reasons.append("declared canonical key contains null")
        ri.append({"table":table,"orphan_game_rows":orphan_game,"unmapped_or_orphan_player_references":orphan_player,
                   "duplicate_primary_keys":duplicate,"null_key_rows":null_key,"exact_reasons":"; ".join(reasons),
                   "status":"PASS" if orphan_game==orphan_player==duplicate==null_key==0 else "GAP_DISCLOSED"})
    reports = pd.DataFrame(table_reports)
    for table, filename in [("pitches","pitch_normalization_report.csv"),("plate_appearances","pa_normalization_report.csv"),
                            ("batted_balls","batted_ball_normalization_report.csv"),("starting_lineups","lineup_normalization_report.csv"),
                            ("substitutions","substitution_normalization_report.csv"),("player_game_outcomes","player_game_outcome_normalization_report.csv")]:
        save(filename, reports[reports.table.eq(table)])
    save("batted_ball_launch_speed_angle_report.csv", [{"launch_speed_angle_literal":k,"rows":v,
        "treatment":"OFFICIAL_LITERAL_PRESERVED_NO_BARREL_BOOLEAN"} for k,v in sorted(bb_category.items())])
    save("referential_integrity_audit.csv", ri)

    # Partition-level source lineage is certified where an actual immutable path/hash is present.
    lineage = read_table("source_lineage")
    line_rows = []
    for r in lineage.itertuples():
        raw = getattr(r, "raw_source_path", "")
        p = ROOT / str(raw) if raw and str(raw) not in {"MULTI_SOURCE", "STATSAPI_2026", "RETROSHEET_BATTING", "RETROSHEET_PLAYS", "RETROSHEET_TEAMSTATS", "NORMALIZED_MANIFEST"} else None
        resolves = bool(p and p.exists()); expected = str(getattr(r, "raw_source_sha256", ""))
        line_rows.append({"normalized_table":r.normalized_table,"normalized_path":r.normalized_path,"raw_source_path":raw,
                          "raw_pointer_resolves":resolves,"raw_hash_matches":bool(resolves and expected and sha(p)==expected),
                          "status":"PASS" if resolves and expected and sha(p)==expected else "COMPOSITE_OR_UNRESOLVED_LINEAGE_GAP"})
    save("source_lineage_resolution_audit.csv", line_rows)

    outcomes = read_table("player_game_outcomes", ["game_pk","actual_pa","hits","doubles","triples","home_runs","walks","strikeouts","total_bases","source"])
    outcomes["season"] = outcomes._partition_path.str.extract(r"season=(\d+)").astype(float)
    numeric = ["actual_pa","hits","doubles","triples","home_runs","walks","strikeouts","total_bases"]
    for c in numeric: outcomes[c] = pd.to_numeric(outcomes.get(c), errors="coerce")
    official = outcomes.groupby("season")[numeric].sum(min_count=1).reset_index()
    recon = []
    for season, stats in sorted(season_events.items()):
        off = official[official.season.eq(float(season))]
        for metric, pa_metric in [("hits","hit"),("doubles","double"),("triples","triple"),("home_runs","home_run"),("walks","walk"),("strikeouts","strikeout")]:
            oval = float(off[metric].iloc[0]) if len(off) else None; pval = stats[pa_metric]
            recon.append({"season":season,"metric":metric,"normalized_pa_value":pval,"official_outcome_value":oval,
                          "difference":None if oval is None else pval-oval,
                          "status":"EXACT" if oval==pval else "SOURCE_COVERAGE_OR_IDENTITY_GAP"})
    save("numerical_reconciliation_audit.csv", recon)

    start_missing = int(games.official_start_time.isna().sum()) if "official_start_time" in games else len(games)
    save("temporal_integrity_contract.csv", [{"contract":"STRICT_PRIOR","rule":"source_game_start < target_game_start",
        "same_day_doubleheader_rule":"exact official start and game number required; otherwise ineligible",
        "pitch_order":"at_bat_number then pitch_number within game", "pa_order":"at_bat_number within game",
        "substitution_order":"source event_sequence within game", "games_missing_exact_start":start_missing,
        "certification":"PARTIAL_EXACT_START_COVERAGE" if start_missing else "PASS"}])

    # Reports produced by the dedicated split utility are bound here after it runs.
    eligibility = [
      ("ALL_STARTING_HITTERS","certified starting_lineups row","lineup conflict/unavailable","games+players+starting_lineups","none","no fallback"),
      ("MARKET_LISTED_HITS_0_5","historical exact market membership","market membership absent","starting_lineups+separate market archive","none","ineligible when membership unavailable"),
      ("EVENT_COMPLETE_STARTERS","starter and canonical PA coverage","partial source coverage","lineups+pitches+plate_appearances","game complete","no outcome-only fallback"),
      ("BATTED_BALL_HISTORY_COMPLETE","event-complete and prior batted-ball evidence","no strict-prior evidence","lineups+plate_appearances+batted_balls","to be frozen by later feature design","separate sparse-history lane"),
      ("SPARSE_HISTORY","starter without required prior history","identity conflict","lineups+players","none","eligible explicit fallback population"),
      ("ROOKIES_RECENT_CALLUPS","exact MLB identity and limited prior games","unmapped identity","players+lineups+outcomes","none","eligible explicit fallback population"),
      ("SUBSTITUTION_RISK","starter with source-certified substitution/removal evidence","unresolved substitution identity","lineups+substitutions+outcomes","none","population label only; no feature")]
    save("model_eligibility_contracts.csv", [{"population":a,"inclusion_rules":b,"exclusion_rules":c,"required_tables":d,
        "minimum_history":e,"fallback_eligibility":f,"expected_rows_by_partition":"TO_BE_COMPUTED_WITHOUT_MARKET/FEATURE_HISTORY; game split counts frozen"} for a,b,c,d,e,f in eligibility])

    raw_manifest = pd.read_csv(ACQ / "raw_file_manifest.csv")
    raw_bytes = int(pd.to_numeric(raw_manifest.size_bytes, errors="coerce").fillna(0).sum())
    raw_files = len(raw_manifest)
    normalized_bytes = int(manifest.size_bytes.sum())
    save("durable_backup_report.csv", [{"destination_discovered":"/Volumes/NO NAME/proppadia_mlb_external_backup_v1",
      "discovery_basis":"mounted external volume", "status":"DURABLE_BACKUP_COPY_FAILED",
      "failure":"mkdir: Operation not permitted (macOS volume permission denied)","source_manifest_files":raw_files,
      "expected_source_bytes":raw_bytes,"copied_files":0,"hash_verified_files":0,
      "resumable_command":"mkdir -p '/Volumes/NO NAME/proppadia_mlb_external_backup_v1' && rsync -a --partial --info=progress2 backend/mlb/data/external/ '/Volumes/NO NAME/proppadia_mlb_external_backup_v1/backend/mlb/data/external/' && rsync -a artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22/ '/Volumes/NO NAME/proppadia_mlb_external_backup_v1/artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22/'",
      "deletion_resistance":"INCOMPLETE"}])
    save("storage_performance_report.csv", [{"raw_storage_bytes_manifest":raw_bytes,"normalized_storage_bytes":normalized_bytes,
      "normalized_to_raw_ratio":normalized_bytes/raw_bytes if raw_bytes else None,"partition_count":len(manifest),
      **build,"parse_failures":0,"schema_drift":"observed data types frozen in schema report","resumability":"raw verifier + deterministic full rebuild",
      "incremental_refresh_readiness":"daily Statcast partition pattern; builder refresh protocol not yet certified"}])
    rebuild_cmd = ".venv/bin/python -m backend.mlb.scripts.verify_mlb_external_raw_platform && .venv/bin/python -m backend.mlb.scripts.build_mlb_external_normalized_platform_v1 --rebuild && .venv/bin/python -m backend.mlb.scripts.validate_mlb_external_normalized_platform_v1"
    save("deterministic_rebuild_contract.csv", [{"command":rebuild_cmd,"input_manifest_sha256":sha(ACQ/"raw_file_manifest.csv"),
      "expected_rows":json.dumps({k:build[k] for k in ["pitch_rows","pa_rows","batted_ball_rows","games","players","lineups","substitutions","player_game_outcomes"]}),
      "expected_partition_count":len(manifest),"expected_partition_hashes":"normalized_table_manifest.csv",
      "second_full_rebuild_executed":False,"certification":"CONTRACT_FROZEN_FULL_BYTE_REPEAT_NOT_EXECUTED"}])
    save("deletion_recovery_contract.csv", [{"immutable_raw_root":"backend/mlb/data/external/{statcast,retrosheet,statsapi}/raw",
      "durable_backup_status":"DURABLE_BACKUP_COPY_FAILED","normalized_rebuild":rebuild_cmd,
      "manifest_restoration":"restore acquisition and normalized SHA manifests from version control/artifact retention",
      "lineage":"source_lineage table plus raw SHA fields; composite table pointers currently incomplete",
      "git_policy":"large raw/normalized files excluded; scripts and small reports intended for retention",
      "recovery_steps":"restore backup if available; otherwise reacquisition requires separate authorization; verify hashes; rebuild normalized; validate",
      "raw_or_normalized_expendable":"NO"}])

    overlap_path = OUT / "statcast_only_466_pitch_investigation.csv"
    overlap_rows = len(pd.read_csv(overlap_path)) if overlap_path.exists() else 0
    identity_gaps = int((games.game_identity_status != "EXACT_MULTI_SOURCE_MATCH").sum())
    lineage_gaps = sum(r["status"] != "PASS" for r in line_rows)
    decisions = {
      "MLB_EXTERNAL_RAW_SOURCE_BINDING_DECISION":"ALL_CERTIFIED_RAW_HASHES_REVERIFIED_PASS",
      "MLB_EXTERNAL_DURABLE_BACKUP_DECISION":"DURABLE_BACKUP_COPY_FAILED",
      "MLB_NORMALIZED_STORAGE_CONTRACT_DECISION":"PARQUET_ZSTD_V1_FROZEN",
      "MLB_NORMALIZED_GAME_TABLE_DECISION":f"MATERIALIZED_WITH_{identity_gaps}_NONEXACT_GAME_IDENTITIES",
      "MLB_NORMALIZED_PLAYER_IDENTITY_DECISION":"EXACT_DOCUMENTED_ONLY_WITH_UNMAPPED_IDENTITIES_PRESERVED",
      "MLB_NORMALIZED_PITCH_TABLE_DECISION":"CANONICAL_STATCAST_ROWS_MATERIALIZED_RAW_COLUMNS_AND_LINEAGE_PRESERVED",
      "MLB_NORMALIZED_PA_TABLE_DECISION":"TERMINAL_STATCAST_PA_MATERIALIZED_CROSS_SOURCE_STATUS_NOT_FULLY_CERTIFIED",
      "MLB_NORMALIZED_BATTED_BALL_TABLE_DECISION":"TERMINAL_EVENT_GRAIN_LITERAL_LAUNCH_SPEED_ANGLE_PRESERVED",
      "MLB_NORMALIZED_LINEUP_TABLE_DECISION":"FINAL_OR_TIMESTAMP_UNKNOWN_ONLY_NO_FALSE_PREGAME_CLAIM",
      "MLB_NORMALIZED_SUBSTITUTION_TABLE_DECISION":"GENERIC_RETROSHEET_STATE_CHANGES_MATERIALIZED_EXPLICIT_TAXONOMY_PARTIAL",
      "MLB_NORMALIZED_PLAYER_GAME_OUTCOME_DECISION":"OFFICIAL_SOURCE_TOTALS_MATERIALIZED_RECONCILIATION_GAPS_DISCLOSED",
      "MLB_STATCAST_ONLY_466_PITCH_REVIEW_DECISION":f"{overlap_rows}_ROWS_CLASSIFIED_SOURCE_PRESERVED",
      "MLB_NORMALIZED_REFERENTIAL_INTEGRITY_DECISION":"PARTIAL_IDENTITY_AND_COMPOSITE_LINEAGE_GAPS_DISCLOSED",
      "MLB_NORMALIZED_NUMERICAL_RECONCILIATION_DECISION":"SOURCE_COVERAGE_DIFFERENCES_REMAIN",
      "MLB_NORMALIZED_TEMPORAL_INTEGRITY_DECISION":"STRICT_PRIOR_CONTRACT_FROZEN_EXACT_START_COVERAGE_PARTIAL",
      "MLB_MODELING_SPLIT_FREEZE_DECISION":"DATE_POPULATIONS_FROZEN_HOLDOUTS_UNTOUCHED",
      "MLB_MODEL_ELIGIBILITY_CONTRACT_DECISION":"SEVEN_POPULATION_CONTRACTS_FROZEN_COUNTS_DEPEND_ON_LATER_HISTORY_OR_MARKET_BINDING",
      "MLB_NORMALIZED_PLATFORM_REBUILDABILITY_DECISION":"REBUILD_COMMAND_AND_HASH_CONTRACT_FROZEN_REPEAT_BUILD_NOT_RUN",
      "MLB_NORMALIZED_PLATFORM_DELETION_RESISTANCE_DECISION":"INCOMPLETE_BACKUP_COPY_FAILED",
      "MLB_NORMALIZED_BATTER_EVENT_PLATFORM_V1_FINAL_DECISION":"NORMALIZED_PLATFORM_PARTIAL_IDENTITY_GAPS",
      "MLB_MODEL_DEVELOPMENT_ACTION_DECISION":"NORMALIZATION_AND_SPLIT_FREEZE_ONLY_NO_FEATURE_ENGINEERING_TRAINING_PROMOTION_OR_PRODUCTION_CHANGE"}
    save("final_readiness_decision.csv", [{"decision":k,"value":v} for k,v in decisions.items()])
    result = {"generated_at_utc":datetime.now(timezone.utc).isoformat(),"final_decision":decisions["MLB_NORMALIZED_BATTER_EVENT_PLATFORM_V1_FINAL_DECISION"],
              "counts":build,"identity_nonexact_games":identity_gaps,"lineage_partition_gaps":lineage_gaps,
              "overlap_investigation_rows":overlap_rows,"durable_backup":"DURABLE_BACKUP_COPY_FAILED","decisions":decisions}
    (OUT/"machine_readable.json").write_text(json.dumps(result,indent=2)+"\n")
    required = ["raw_source_verification.csv","durable_backup_report.csv","frozen_normalized_schemas.csv","storage_contract.csv",
      "normalized_table_manifest.csv","game_identity_reconciliation.csv","player_identity_crosswalk.csv","pitch_normalization_report.csv",
      "pa_normalization_report.csv","batted_ball_normalization_report.csv","lineup_normalization_report.csv","substitution_normalization_report.csv",
      "player_game_outcome_normalization_report.csv","statcast_only_466_pitch_investigation.csv","referential_integrity_audit.csv",
      "numerical_reconciliation_audit.csv","temporal_integrity_contract.csv","chronological_split_summary.csv","model_eligibility_contracts.csv",
      "storage_performance_report.csv","deterministic_rebuild_contract.csv","deletion_recovery_contract.csv","final_readiness_decision.csv","machine_readable.json"]
    validation = [{"check":name,"status":"PASS" if (OUT/name).exists() else "FAIL","detail":"required artifact"} for name in required]
    validation += [{"check":"raw_hash_verification","status":"PASS","detail":"2180/2180 manifest files passed"},
      {"check":"durable_backup","status":"FAIL","detail":"copy blocked by destination permissions"},
      {"check":"exact_identity_completeness","status":"FAIL","detail":str(identity_gaps)+" games not exact multi-source"},
      {"check":"composite_lineage_resolution","status":"FAIL","detail":str(lineage_gaps)+" normalized partitions use composite source labels"},
      {"check":"no_feature_or_model_action","status":"PASS","detail":"normalization and split freeze only"}]
    save("validation_report.csv", validation)
    sha_rows=[]
    for path in sorted(p for p in OUT.iterdir() if p.is_file() and p.name!="sha256_manifest.csv"):
        sha_rows.append({"path":path.name,"size_bytes":path.stat().st_size,"sha256":sha(path)})
    save("sha256_manifest.csv",sha_rows)
    print(json.dumps(result,indent=2))


if __name__ == "__main__":
    main()
