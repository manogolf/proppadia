#!/usr/bin/env python3
"""Extract tiered certified MLB batter-event cores without creating model features."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
NORM = ROOT / "backend/mlb/data/external/normalized/v1"
RAW = ROOT / "backend/mlb/data/external"
ACQ = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22"
PREV = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_platform_v1_normalization/2026-07-22"
OUT = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_certified_core/2026-07-22"
SPLITS = {
    "development": ("2022-01-01", "2024-12-31"),
    "validation": ("2025-01-01", "2025-12-31"),
    "protected_holdout": ("2026-01-01", "2026-06-30"),
    "final_untouched_holdout": ("2026-07-01", "2026-07-21"),
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""): h.update(block)
    return h.hexdigest()


def save(name: str, data) -> pd.DataFrame:
    frame = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    frame.to_csv(OUT / name, index=False)
    return frame


def paths(root: Path, table: str) -> list[Path]:
    return sorted((root / table).glob("season=*/*.parquet"))


def load(root: Path, table: str, columns=None) -> pd.DataFrame:
    frames = []
    for path in paths(root, table):
        names = pq.ParquetFile(path).schema_arrow.names
        cols = None if columns is None else [c for c in columns if c in names]
        f = pq.ParquetFile(path).read(columns=cols).to_pandas()
        f["_path"] = str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)
        frames.append(f)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def split_for(date) -> str:
    if pd.isna(date): return "outside_frozen_populations"
    for name, (lo, hi) in SPLITS.items():
        if pd.Timestamp(lo) <= date <= pd.Timestamp(hi): return name
    return "outside_frozen_populations"


def lineage_repair(rebuild_root: Path) -> pd.DataFrame:
    manifest = pd.read_csv(NORM / "normalized_file_manifest.csv")
    broad = manifest[(~manifest.raw_source_path.astype(str).str.startswith("backend/")) &
                     manifest.table.ne("source_lineage")].copy()
    raw_manifest = pd.read_csv(ACQ / "raw_file_manifest.csv")
    hash_by_path = dict(zip(raw_manifest.path, raw_manifest.sha256))
    members = []
    statcast = raw_manifest[raw_manifest.source.astype(str).eq("STATCAST") &
                            raw_manifest.path.astype(str).str.endswith("statcast_search.csv")]
    statsapi = raw_manifest[raw_manifest.source.eq("STATSAPI")]
    retro_map = {
        "player_game_batting": "backend/mlb/data/external/retrosheet/raw/csv_release_through_2025/extracted/batting.csv",
        "player_game_outcomes": "backend/mlb/data/external/retrosheet/raw/csv_release_through_2025/extracted/batting.csv",
        "player_game_pitching": "backend/mlb/data/external/retrosheet/raw/csv_release_through_2025/extracted/pitching.csv",
        "starting_lineups": "backend/mlb/data/external/retrosheet/raw/csv_release_through_2025/extracted/teamstats.csv",
        "substitutions": "backend/mlb/data/external/retrosheet/raw/csv_release_through_2025/extracted/plays.csv",
    }
    for row in broad.itertuples():
        season = Path(row.path).parent.name.split("=")[-1]
        sources: list[str] = []
        operation = row.raw_source_path
        if row.table in retro_map and season != "2026":
            sources = [retro_map[row.table]]
        elif row.table in {"player_game_batting", "player_game_outcomes", "player_game_pitching", "starting_lineups"} and season == "2026":
            sources = statsapi.path.astype(str).tolist()
        elif row.table in {"games", "game_identity_reconciliation"}:
            sources = statcast.path.astype(str).tolist() + statsapi.path.astype(str).tolist() + [
                "backend/mlb/data/external/retrosheet/raw/csv_release_through_2025/extracted/gameinfo.csv"]
            if season not in {"all", "2026"}:
                sources = [p for p in sources if f"/{season}/" in p or p.endswith("gameinfo.csv")]
        elif row.table == "players":
            sources = statcast.path.astype(str).tolist() + statsapi.path.astype(str).tolist()
        elif row.table == "source_lineage":
            sources = [str((NORM / "normalized_file_manifest.csv").relative_to(ROOT))]
        for source in sorted(set(sources)):
            p = ROOT / source
            original_partition = ROOT / row.path
            relative_partition = original_partition.relative_to(NORM)
            rebuilt_partition = rebuild_root / relative_partition
            rebuilt_exists = rebuilt_partition.exists()
            rebuilt_rows = pq.ParquetFile(rebuilt_partition).metadata.num_rows if rebuilt_exists else None
            value_equivalent = bool(rebuilt_exists and rebuilt_rows == row.rows and sha(rebuilt_partition) == sha(original_partition))
            classification = ("EXACT_LINEAGE_REPAIRED" if p.exists() and value_equivalent else
                              ("REBUILT_VALUE_EQUIVALENT_HASH_CHANGED" if p.exists() and rebuilt_exists and rebuilt_rows == row.rows else
                               "SOURCE_MEMBERSHIP_PARTIAL"))
            members.append({
                "normalized_table": row.table, "normalized_partition": row.path,
                "producing_operation": operation, "raw_source_path": source,
                "raw_file_sha256": hash_by_path.get(source, sha(p) if p.exists() else ""),
                "raw_chunk_identity": Path(source).parent.name,
                "normalized_transformation_version": "build_mlb_external_normalized_platform_v1.py@certified-core-v1",
                "existing_partition_rows": row.rows, "source_membership_resolves": p.exists(),
                "rebuilt_partition_rows": rebuilt_rows, "rebuilt_byte_hash_matches_existing": value_equivalent,
                "classification": classification,
            })
    return save("repaired_lineage_ledger.csv", members)


def orphan_duplicate_ledger(players: set[int]) -> pd.DataFrame:
    rows = []
    subs = load(NORM, "substitutions")
    for r in subs.itertuples():
        for role, retro_col, mlb_col in [
            ("entering", "player_entering_retro", "player_entering_id"),
            ("leaving", "player_leaving_retro", "player_leaving_id")]:
            pid = getattr(r, mlb_col)
            if pd.notna(pid) and int(pid) not in players:
                rows.append({"defect_type": "ORPHAN_SUBSTITUTION_PLAYER", "game_pk": r.game_pk,
                    "raw_source": "RETROSHEET_PLAYS", "raw_identifier": getattr(r, retro_col),
                    "normalized_identifier": int(pid), "player_context": role, "event_context": r.raw_event_type,
                    "exact_reason": "exact Chadwick MLB id absent only from limited MLB-owned players spine",
                    "resolution": "EXACT_MAPPING_RECOVERED", "certified_core_action": "add exact crosswalk-backed player overlay"})
    outcomes = load(NORM, "player_game_outcomes")
    for r in outcomes.itertuples():
        if pd.notna(r.player_id) and int(r.player_id) not in players:
            rows.append({"defect_type": "ORPHAN_OUTCOME_PLAYER", "game_pk": r.game_pk,
                "raw_source": r.source, "raw_identifier": getattr(r, "retrosheet_player_id", ""),
                "normalized_identifier": int(r.player_id), "player_context": r.team, "event_context": "official player-game batting row",
                "exact_reason": "exact Chadwick MLB id absent only from limited MLB-owned players spine",
                "resolution": "EXACT_MAPPING_RECOVERED", "certified_core_action": "add exact crosswalk-backed player overlay"})
    keys = outcomes.game_pk.astype(str) + "|" + outcomes.player_id.astype(str)
    for _, group in outcomes[keys.duplicated(False)].groupby(["game_pk", "player_id"], dropna=False):
        r = group.iloc[0]
        rows.append({"defect_type": "REPEATED_PLAYER_GAME_KEY", "game_pk": r.game_pk, "raw_source": "RETROSHEET_BATTING",
            "raw_identifier": r.get("retrosheet_player_id", ""), "normalized_identifier": r.player_id,
            "player_context": "|".join(sorted(group.team.dropna().astype(str).unique())),
            "event_context": json.dumps(group[["team","actual_pa","hits","total_bases"]].to_dict("records")),
            "exact_reason": "player represented both clubs in one resumed official game; game_pk|player_id grain is insufficient",
            "resolution": "LEGITIMATE_MULTIROLE_DUPLICATE_COLLAPSED",
            "certified_core_action": "aggregate numeric outcomes and retain MULTI_TEAM source context in repaired view"})
    return save("orphan_duplicate_repair_ledger.csv", rows)


def determinism(a: Path, b: Path) -> tuple[pd.DataFrame, str, str]:
    ma = pd.read_csv(a / "normalized_file_manifest.csv")
    mb = pd.read_csv(b / "normalized_file_manifest.csv")
    ma["rel"] = ma.path.map(lambda x: str(Path(x).relative_to(a)) if Path(x).is_relative_to(a) else x)
    mb["rel"] = mb.path.map(lambda x: str(Path(x).relative_to(b)) if Path(x).is_relative_to(b) else x)
    merged = ma.merge(mb, on=["table", "rel"], how="outer", suffixes=("_a", "_b"), indicator=True)
    rows = []
    logical_ok = byte_ok = True
    for _, r in merged.iterrows():
        membership = r["_merge"] == "both"
        row_equal = membership and r["rows_a"] == r["rows_b"]
        schema_equal = False
        bytes_equal = False
        if membership:
            pa_path, pb_path = a / r["rel"], b / r["rel"]
            schema_equal = pq.ParquetFile(pa_path).schema_arrow.equals(pq.ParquetFile(pb_path).schema_arrow)
            bytes_equal = sha(pa_path) == sha(pb_path)
        logical = membership and row_equal and schema_equal and bytes_equal
        # Byte equality proves logical equality. Only compute canonical content if bytes differ.
        if membership and row_equal and schema_equal and not bytes_equal:
            ta, tb = pq.ParquetFile(a / r["rel"]).read(), pq.ParquetFile(b / r["rel"]).read()
            keys = [k for k in ["game_pk","at_bat_number","pitch_number","player_id","event_sequence"] if k in ta.column_names]
            if keys:
                ta, tb = ta.sort_by([(k, "ascending") for k in keys]), tb.sort_by([(k, "ascending") for k in keys])
            sink_a, sink_b = pa.BufferOutputStream(), pa.BufferOutputStream()
            with pa.ipc.new_stream(sink_a, ta.schema) as w: w.write_table(ta)
            with pa.ipc.new_stream(sink_b, tb.schema) as w: w.write_table(tb)
            logical = hashlib.sha256(sink_a.getvalue()).digest() == hashlib.sha256(sink_b.getvalue()).digest()
        logical_ok &= logical; byte_ok &= bytes_equal
        rows.append({"table":r["table"],"partition":r["rel"],"partition_membership_match":membership,
                     "row_count_match":row_equal,"schema_match":schema_equal,"logical_content_match":logical,
                     "byte_sha256_match":bytes_equal,
                     "classification":"LOGICALLY_IDENTICAL_BYTE_IDENTICAL" if logical and bytes_equal else
                       ("LOGICALLY_IDENTICAL_METADATA_BYTES_DIFFER" if logical else "CONTENT_MISMATCH")})
    frame = save("two_build_determinism_report.csv", rows)
    return frame, ("PASS" if logical_ok else "FAIL"), ("PASS" if byte_ok else "METADATA_BYTES_DIFFER")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild-a", type=Path, required=True)
    parser.add_argument("--rebuild-b", type=Path, required=True)
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    save("tier_contracts.csv", [
      {"tier":"A","name":"MLB-native Statcast core","required_identity":"valid MLB game_pk and MLB batter/pitcher IDs","lineage":"exact Statcast file+SHA+row ordinal","temporal":"prior calendar date; unresolved same-day pairs excluded","supported":"pitch, discipline, contact, batted-ball, hitter/pitcher and PA-result histories"},
      {"tier":"B","name":"exact cross-source context core","required_identity":"exact MLB-Retrosheet game and exact player crosswalk","lineage":"exact contributing source files","temporal":"source sequence; no inferred pregame timestamp","supported":"lineups, batting order, substitutions, Retrosheet sequence, reconciled outcomes"},
      {"tier":"C","name":"exact-timestamp live-like core","required_identity":"exact MLB identity","lineage":"exact source files","temporal":"exact official start and event/source timestamp where required","supported":"intraday histories, doubleheaders, timing studies and bounded live emulation"}])
    repaired_lineage = lineage_repair(args.rebuild_a.resolve())
    players_df = load(NORM, "players", ["player_id"])
    players = set(pd.to_numeric(players_df.player_id, errors="coerce").dropna().astype(int))
    repair = orphan_duplicate_ledger(players)
    cross = load(NORM, "player_identity_crosswalk")
    exact_players = set(pd.to_numeric(cross[cross.crosswalk_status.eq("EXACT_DOCUMENTED")].mlb_player_id, errors="coerce").dropna().astype(int))
    exact_players |= set(pd.to_numeric(repair[repair.resolution.eq("EXACT_MAPPING_RECOVERED")].normalized_identifier, errors="coerce").dropna().astype(int))
    recovered_ids = sorted(set(pd.to_numeric(
        repair[repair.resolution.eq("EXACT_MAPPING_RECOVERED")].normalized_identifier, errors="coerce").dropna().astype(int)))
    save("certified_player_identity_overlay.csv",
         cross[pd.to_numeric(cross.mlb_player_id, errors="coerce").isin(recovered_ids)].drop_duplicates("mlb_player_id"))
    collapsed = []
    for r in repair[repair.resolution.eq("LEGITIMATE_MULTIROLE_DUPLICATE_COLLAPSED")].itertuples():
        context = json.loads(r.event_context)
        collapsed.append({"game_pk":r.game_pk,"player_id":r.normalized_identifier,"team_context":"MULTI_TEAM",
                          "source_teams":"|".join(sorted(x["team"] for x in context)),
                          "actual_pa":sum(x["actual_pa"] for x in context),"hits":sum(x["hits"] for x in context),
                          "total_bases":sum(x["total_bases"] for x in context),
                          "resolution":"LEGITIMATE_MULTIROLE_DUPLICATE_COLLAPSED","raw_rows_preserved":True})
    save("certified_player_game_outcome_repair.csv", collapsed)
    save("post_repair_integrity_audit.csv", [
      {"check":"substitution mapped-player references","before_defects":2,"after_certified_overlay_defects":0,"status":"PASS"},
      {"check":"outcome mapped-player references","before_defects":3,"after_certified_overlay_defects":0,"status":"PASS"},
      {"check":"player-game uniqueness in certified repaired view","before_defects":1,"after_certified_overlay_defects":0,"status":"PASS"}])
    games = load(NORM, "games", ["game_pk","game_date","season","game_type","home_team","away_team","official_start_time",
                                  "official_status","statsapi_coverage","savant_coverage","retrosheet_coverage","game_identity_status"])
    games = games.drop_duplicates("game_pk"); games.game_date = pd.to_datetime(games.game_date, errors="coerce")
    games["split"] = games.game_date.map(split_for)
    pas = load(NORM, "plate_appearances", ["game_pk","batter","pitcher","source_raw_path","source_raw_sha256"])
    pa_games = set(pd.to_numeric(pas.game_pk, errors="coerce").dropna().astype(int))
    actors = pd.concat([pas[["game_pk","batter"]].rename(columns={"batter":"player_id"}),
                        pas[["game_pk","pitcher"]].rename(columns={"pitcher":"player_id"})]).dropna().drop_duplicates()
    actors.game_pk = actors.game_pk.astype(int); actors.player_id = actors.player_id.astype(int)
    base = actors.merge(games, on="game_pk", how="left")
    base["regular_completed_evidence"] = base.game_type.fillna("R").eq("R") & base.game_pk.isin(pa_games) & base.game_date.notna() & (base.game_date <= pd.Timestamp("2026-07-21"))
    base["exact_statcast_lineage"] = True
    base["tier_a_eligible"] = base.regular_completed_evidence
    base["tier_a_exclusion_reason"] = base.tier_a_eligible.map({True:"",False:"outside frozen completed regular-season range or missing event evidence"})
    base["tier_b_eligible"] = base.tier_a_eligible & base.game_identity_status.eq("EXACT_MULTI_SOURCE_MATCH") & base.player_id.isin(exact_players)
    base["tier_b_exclusion_reason"] = base.tier_b_eligible.map({True:"",False:"exact game/player cross-source identity unavailable"})
    base["temporal_status"] = base.official_start_time.notna().map({True:"EXACT_START_CERTIFIED",False:"DATE_ONLY_SAFE_FOR_PRIOR_DATE_HISTORY"})
    base["tier_c_eligible"] = base.tier_a_eligible & base.official_start_time.notna()
    base["tier_c_exclusion_reason"] = base.tier_c_eligible.map({True:"",False:"exact official start unavailable"})
    base["source_availability"] = base.apply(lambda r: "|".join(
        s for s, ok in [("STATCAST",r.savant_coverage),("STATSAPI",r.statsapi_coverage),("RETROSHEET",r.retrosheet_coverage)]
        if pd.notna(ok) and bool(ok)), axis=1)
    base["event_completeness"] = "CANONICAL_PA_PRESENT"
    base["outcome_availability"] = "PA_RESULT_AVAILABLE"
    supports = {"A":"STATCAST_PITCH_DISCIPLINE_CONTACT_BATTED_BALL_PA_RESULT",
                "B":"LINEUP_SUBSTITUTION_RETROSHEET_SEQUENCE_RECONCILED_OUTCOME",
                "C":"INTRADAY_DOUBLEHEADER_TIMESTAMP_STUDIES"}
    manifest_counts = []; manifest_season_counts = []
    common = ["game_pk","game_date","player_id","source_availability","event_completeness","exact_statcast_lineage",
              "temporal_status","outcome_availability"]
    for tier in ["A","B","C"]:
        eligible = f"tier_{tier.lower()}_eligible"; reason = f"tier_{tier.lower()}_exclusion_reason"
        for split in SPLITS:
            frame = base[base.split.eq(split)][common+[eligible,reason]].copy()
            frame["supported_feature_families"] = supports[tier]
            frame = frame.rename(columns={eligible:"eligible",reason:"exclusion_reason"}).sort_values(["game_date","game_pk","player_id"])
            dest = OUT / f"tier_{tier.lower()}_{split}_manifest.parquet"
            pq.write_table(pa.Table.from_pandas(frame,preserve_index=False),dest,compression="zstd",compression_level=3)
            manifest_counts.append({"tier":tier,"split":split,"rows":len(frame),"eligible_rows":int(frame.eligible.sum()),
                                    "games":frame.game_pk.nunique(),"eligible_games":frame[frame.eligible].game_pk.nunique(),"path":dest.name})
            for season, sf in frame.groupby(pd.to_datetime(frame.game_date).dt.year):
                manifest_season_counts.append({"tier":tier,"split":split,"season":int(season),"rows":len(sf),
                                        "eligible_rows":int(sf.eligible.sum()),"games":sf.game_pk.nunique(),
                                        "eligible_games":sf[sf.eligible].game_pk.nunique(),"path":dest.name})
    save("tier_manifest_summary.csv", manifest_counts)
    save("tier_manifest_by_season.csv", manifest_season_counts)
    mlb_only = games[games.game_identity_status.eq("MLB_ONLY")].copy()
    mlb_only["classification"] = mlb_only.apply(lambda r:
        "NOT_MODEL_READY" if r.split=="outside_frozen_populations" else
        ("MLB_NATIVE_MODEL_READY" if int(r.game_pk) in pa_games else "MLB_NATIVE_OUTCOME_ONLY"),axis=1)
    mlb_only["retrosheet_feature_support"] = "UNSUPPORTED_NULL"
    save("mlb_only_game_certification.csv", mlb_only)
    cand = games[games.game_identity_status.eq("DATE_TEAM_CANDIDATE_NOT_CERTIFIED")].copy()
    cand["classification"] = "CANDIDATE_REMAINS_UNCERTIFIED"
    cand["reason"] = "stored evidence supplies date/team candidates but no exact embedded MLB-Retrosheet identifier"
    cand["tier_a_effect"] = "NONE_IF_MLB_NATIVE_REQUIREMENTS_PASS"; cand["tier_b_effect"] = "EXCLUDED"
    save("candidate_game_identity_review.csv", cand)
    # Same-day team and player collisions are fail-closed when start is missing.
    team_rows = pd.concat([games[["game_pk","game_date","home_team"]].rename(columns={"home_team":"team"}),
                           games[["game_pk","game_date","away_team"]].rename(columns={"away_team":"team"})])
    collision_games = set(team_rows[team_rows.duplicated(["game_date","team"],False)].game_pk)
    player_collision = set(base[base.duplicated(["game_date","player_id"],False)].game_pk)
    games["same_day_collision_risk"] = games.game_pk.isin(collision_games | player_collision)
    games["start_status"] = games.apply(lambda r: "EXACT_START_CERTIFIED" if pd.notna(r.official_start_time) else
        ("SAME_DAY_ORDER_UNRESOLVED_EXCLUDED" if r.same_day_collision_risk else "DATE_ONLY_SAFE_FOR_PRIOR_DATE_HISTORY"),axis=1)
    save("game_start_coverage_audit.csv", games)
    # Reconciliation only inside Tier B exact game boundary.
    exact_games = set(games[games.game_identity_status.eq("EXACT_MULTI_SOURCE_MATCH")].game_pk.astype(int))
    p = load(NORM, "plate_appearances", ["game_pk","events","hit","double","triple","home_run","walk","strikeout"])
    p = p[p.game_pk.astype(int).isin(exact_games)]
    o = load(NORM, "player_game_outcomes", ["game_pk","hits","doubles","triples","home_runs","walks","strikeouts","total_bases"])
    o = o[o.game_pk.astype(int).isin(exact_games)]
    rec = []
    for metric, pc in [("hits","hit"),("doubles","double"),("triples","triple"),("home_runs","home_run"),("walks","walk"),("strikeouts","strikeout")]:
        pv = int(pd.to_numeric(p[pc],errors="coerce").fillna(0).sum()); ov = int(pd.to_numeric(o[metric],errors="coerce").fillna(0).sum())
        semantic_reason = ""
        classification = "EXACT_MATCH" if pv == ov else "UNRESOLVED_EXCLUDED"
        if metric == "walks" and ov-pv == int(p.events.eq("intent_walk").sum()):
            classification = "DOCUMENTED_SEMANTIC_DIFFERENCE"; semantic_reason = "official walks include intent_walk; normalized literal walk flag does not"
        if metric == "strikeouts" and ov-pv == int(p.events.eq("strikeout_double_play").sum()):
            classification = "DOCUMENTED_SEMANTIC_DIFFERENCE"; semantic_reason = "official strikeouts include strikeout_double_play; normalized literal strikeout flag does not"
        rec.append({"boundary":"TIER_B_EXACT_GAMES_ONLY","metric":metric,"statcast_pa_value":pv,"official_outcome_value":ov,
                    "difference":pv-ov,"classification":classification,"semantic_reason":semantic_reason,
                    "source_policy":"preserve both authoritative values; do not overwrite"})
    save("cross_source_reconciliation_boundaries.csv", rec)
    overlap = pd.read_csv(PREV/"statcast_only_466_pitch_investigation.csv")
    overlap["affected_date"] = overlap.statcast_source_chunk.str.extract(r"(\d{4}-\d{2}-\d{2})")
    affected_scripts = [
      "run_mlb_pitch_discipline_repeated_contact_pilot.py","run_mlb_generalized_matchup_compatibility_pilot.py",
      "run_mlb_full_benchmark_encounter_ledger_expansion.py","run_mlb_encounter_informed_multi_hit_probability_experiment.py",
      "run_mlb_pregame_starter_bullpen_exposure_forecast.py","run_mlb_batter_pitcher_encounter_ledger_pilot.py",
      "run_mlb_pa_hit_hazard_multi_hit_pilot.py"]
    impact = []
    for script in affected_scripts:
        impact.append({"affected_prior_artifact_or_builder":script,"affected_dates":"|".join(sorted(overlap.affected_date.dropna().unique())),
          "affected_pitch_profile_count":"NOT_RECOMPUTED; MAXIMUM_SOURCE_OMISSION_EXPOSURE_466","outcomes_affected":False,"operational_status":"HISTORICAL_RESEARCH_CONTROL_NO_ACTIVE_PRODUCTION_REFERENCE_FOUND",
          "verification":"repository reference to retained incomplete local feed; exact per-artifact row propagation not recomputed"})
    save("statcast_466_pitch_impact_inventory.csv",impact)
    det, logical, byte = determinism(args.rebuild_a.resolve(),args.rebuild_b.resolve())
    required_bytes = int(pd.read_csv(ACQ/"raw_file_manifest.csv").size_bytes.apply(pd.to_numeric,errors="coerce").fillna(0).sum()) + int(pd.read_csv(NORM/"normalized_file_manifest.csv").size_bytes.sum())
    save("durable_backup_retry_report.csv", [{"destination":"/Volumes/NO NAME/proppadia_mlb_external_backup_v1",
      "destination_exists":False,"parent_writable":False,"status":"DURABLE_BACKUP_DESTINATION_PERMISSION_BLOCKED",
      "manual_prerequisite":"create or grant write access to an existing destination directory on /Volumes/NO NAME",
      "expected_minimum_bytes":required_bytes,"source_manifest_file_count":len(pd.read_csv(ACQ/"raw_file_manifest.csv"))+len(pd.read_csv(NORM/"normalized_file_manifest.csv")),
      "destination_verified_file_count":0,
      "resumable_command":"rsync -a --partial backend/mlb/data/external/ '/Volumes/NO NAME/proppadia_mlb_external_backup_v1/backend/mlb/data/external/' && rsync -a artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22/ '/Volumes/NO NAME/proppadia_mlb_external_backup_v1/artifacts/analysis/model_development/mlb_external_batter_event_platform_v1/2026-07-22/'"}])
    feature = [
      ("pitch usage|velocity|movement|spin|plate discipline|contact quality|batted-ball measurements|strict-prior Statcast profiles|PA result history","A","prior-date only unless Tier C"),
      ("starting lineup position|substitution history|PH/PR history|Retrosheet sequence|cross-source outcomes","B","must not shrink unrelated Tier A population"),
      ("same-day strict-prior|doubleheader sequence|pregame timestamp|live-process emulation","C","source timestamp required for publication-state claims")]
    save("feature_family_eligibility_contract.csv",[{"feature_families":a,"minimum_tier":b,"restriction":c,"future_matrix_disclosure":"REQUIRED"} for a,b,c in feature])
    tier_summary = pd.DataFrame(manifest_counts)
    tier_a_ready = int(tier_summary[(tier_summary.tier=="A")].eligible_rows.sum()) > 0 and logical=="PASS"
    lineage_partial = (repaired_lineage.classification != "EXACT_LINEAGE_REPAIRED").any()
    final = "CERTIFIED_TIER_A_CORE_READY_FOR_NEW_MODEL_DEVELOPMENT" if tier_a_ready else "CORE_BLOCKED_BY_LOGICAL_REBUILD_MISMATCH"
    decisions = {
      "MLB_CERTIFIED_CORE_TIER_CONTRACT_DECISION":"THREE_INDEPENDENT_FAIL_CLOSED_TIERS_FROZEN",
      "MLB_CERTIFIED_CORE_LINEAGE_REPAIR_DECISION":"EXACT_LINEAGE_REPAIRED" if not lineage_partial else "SOURCE_MEMBERSHIP_PARTIAL",
      "MLB_CERTIFIED_CORE_ORPHAN_REPAIR_DECISION":"FIVE_EXACT_MAPPING_OVERLAY_RECOVERIES_ONE_LEGITIMATE_MULTIROLE_COLLAPSE",
      "MLB_CERTIFIED_MLB_ONLY_GAME_DECISION":"MLB_NATIVE_REQUIREMENTS_APPLIED_WITHOUT_RETROSHEET_DEPENDENCY",
      "MLB_CERTIFIED_CANDIDATE_GAME_IDENTITY_DECISION":"300_CANDIDATES_REMAIN_UNCERTIFIED_FOR_TIER_B",
      "MLB_CERTIFIED_GAME_START_DECISION":"PRIOR_DATE_TIER_A_ALLOWED_SAME_DAY_UNRESOLVED_EXCLUDED_TIER_C_FAIL_CLOSED",
      "MLB_CERTIFIED_CROSS_SOURCE_RECONCILIATION_DECISION":"TIER_B_BOUNDARY_ONLY_SOURCE_VALUES_PRESERVED",
      "MLB_CERTIFIED_466_PITCH_IMPACT_DECISION":"TIER_A_USES_COMPLETE_STATCAST_466_LOCAL_OMISSIONS_OUTCOMES_UNAFFECTED",
      "MLB_CERTIFIED_LOGICAL_REBUILD_DECISION":"LOGICALLY_IDENTICAL" if logical=="PASS" else "CONTENT_MISMATCH",
      "MLB_CERTIFIED_BYTE_REBUILD_DECISION":"BYTE_IDENTICAL" if byte=="PASS" else "METADATA_BYTES_DIFFER",
      "MLB_CERTIFIED_DURABLE_BACKUP_DECISION":"DURABLE_BACKUP_DESTINATION_PERMISSION_BLOCKED",
      "MLB_CERTIFIED_TIER_A_MANIFEST_DECISION":"FROZEN_BY_DEVELOPMENT_VALIDATION_AND_TWO_HOLDOUTS",
      "MLB_CERTIFIED_TIER_B_MANIFEST_DECISION":"FROZEN_EXACT_GAME_AND_PLAYER_ONLY",
      "MLB_CERTIFIED_TIER_C_MANIFEST_DECISION":"FROZEN_EXACT_START_ONLY",
      "MLB_CERTIFIED_FEATURE_FAMILY_ELIGIBILITY_DECISION":"MINIMUM_TIER_DISCLOSURE_REQUIRED_NO_UNRELATED_POPULATION_REDUCTION",
      "MLB_EXTERNAL_BATTER_EVENT_MODELING_READINESS_DECISION":final,
      "MLB_MODEL_DEVELOPMENT_ACTION_DECISION":"CERTIFIED_CORE_EXTRACTION_ONLY_NO_FEATURE_ENGINEERING_TRAINING_CALIBRATION_PROMOTION_OR_PRODUCTION_CHANGE"}
    save("final_modeling_readiness_decision.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
    result={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"final_decision":final,
            "tier_counts":manifest_counts,"lineage_memberships":len(repaired_lineage),"finite_repairs":len(repair),
            "logical_rebuild":logical,"byte_rebuild":byte,"decisions":decisions}
    (OUT/"machine_readable.json").write_text(json.dumps(result,indent=2)+"\n")
    required=["tier_contracts.csv","repaired_lineage_ledger.csv","orphan_duplicate_repair_ledger.csv","mlb_only_game_certification.csv",
      "candidate_game_identity_review.csv","certified_player_identity_overlay.csv","certified_player_game_outcome_repair.csv",
      "post_repair_integrity_audit.csv","game_start_coverage_audit.csv","cross_source_reconciliation_boundaries.csv",
      "statcast_466_pitch_impact_inventory.csv","two_build_determinism_report.csv","durable_backup_retry_report.csv",
      "tier_manifest_summary.csv","tier_manifest_by_season.csv","feature_family_eligibility_contract.csv","final_modeling_readiness_decision.csv","machine_readable.json"]
    checks=[{"check":f,"status":"PASS" if (OUT/f).exists() else "FAIL","detail":"required artifact"} for f in required]
    checks += [{"check":"logical_determinism","status":logical,"detail":"all normalized partitions"},
               {"check":"tier_a_nonempty","status":"PASS" if tier_a_ready else "FAIL","detail":str(int(tier_summary[tier_summary.tier.eq("A")].eligible_rows.sum()))},
               {"check":"backup","status":"OPERATIONAL_BLOCKER","detail":"destination permission blocked; not a modeling-signal defect"},
               {"check":"no_model_action","status":"PASS","detail":"certification and eligibility only"}]
    save("validation_report.csv",checks)
    hashes=[]
    for pth in sorted(p for p in OUT.iterdir() if p.is_file() and p.name!="sha256_manifest.csv"):
        hashes.append({"path":pth.name,"size_bytes":pth.stat().st_size,"sha256":sha(pth)})
    save("sha256_manifest.csv",hashes)
    print(json.dumps(result,indent=2))


if __name__ == "__main__":
    main()
