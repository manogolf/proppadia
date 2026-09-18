"""Bounded historical BvP audit. SQL is read-only; no HTTP/acquisition/model fit.

snapshot: retain a transaction-consistent, hashable local evidence snapshot.
analyze: deterministic offline ledger generation from that snapshot and files.
validate: verify package bytes and row/lineage invariants, without a database.
"""
from __future__ import annotations

import argparse
import ast
import csv
import gzip
import hashlib
import io
import json
import math
import os
import plistlib
import re
import socket
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.mlb.shared.bvp_identity import (
    EXCLUSION_PATH, certified_rows, exclusion_receipt, stable_hash,
    utc_time, validate_exclusion_set,
)

ROOT = Path(__file__).resolve().parents[3]
PT = ZoneInfo("America/Los_Angeles")
BASE = ROOT / "artifacts/analysis/mlb/operational_reconciliation/2026-09-18"
OUT = ROOT / "artifacts/analysis/mlb/bvp_historical_downstream_materiality_v1/2026-09-18"
CONTRACT = "BVP_HISTORICAL_DOWNSTREAM_MATERIALITY_V1"
FIX_COMMIT = "1c36620785837e057f89b263d3ab88f28c723469"
CLASSES = (
    "CONFIRMED_UTC_LOCAL_DATE_IDENTITY_DEFECT",
    "VERIFIED_LEGITIMATE_RESCHEDULE_OR_SUSPENSION",
    "OTHER_CONFIRMED_IDENTITY_DEFECT",
    "UNRESOLVED_IDENTITY_PROVENANCE",
    "SEPTEMBER_18_HASH_PINNED_QUARANTINE",
)
CONFIRMED = {CLASSES[0], CLASSES[2], CLASSES[4]}
KEY = ("prop_type", "player_id", "game_id", "feature_set_tag")
ALIASES = {
    "bvp_pa_prior": "bvp_plate_appearances", "bvp_ab_prior": "bvp_at_bats",
    "bvp_hits_prior": "bvp_hits", "bvp_hr_prior": "bvp_home_runs",
    "bvp_bb_prior": "bvp_walks", "bvp_so_prior": "bvp_strikeouts",
    "bvp_tb_prior": "bvp_total_bases",
}
TOKEN = re.compile(r"prop_features_precomputed|bvp_pvb_refresh_v1|\bbvp_\w+|BvP|BVP_|manual_bvp_|local_prewarm_", re.I)
AUDIT_SOURCE = Path(__file__).resolve()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def relative(path):
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def put_json(out, name, value):
    (out / name).write_text(json.dumps(value, sort_keys=True, indent=2, default=str) + "\n")


def put_jsonl(out, name, values):
    content="".join(canonical(v) + "\n" for v in values).encode()
    compressed={"artifact_inventory.jsonl","artifact_row_lineage_matches.jsonl","row_disposition_ledger.jsonl","database_key_lineage_matches.jsonl"}
    if name in compressed:
        (out/(name+".gz")).write_bytes(gzip.compress(content,mtime=0))
        # Replace only duplicate, newly generated audit files; never source evidence.
        if (out/name).exists():
            assert (out/name).read_bytes()==content, "REFUSE_REMOVAL_OF_DIFFERENT_AUDIT_EVIDENCE"
            (out/name).unlink()
    else:
        (out / name).write_bytes(content)


def read_jsonl(path):
    if path.exists():
        content=path.read_text()
    else:
        content=gzip.decompress(Path(str(path)+".gz").read_bytes()).decode()
    return [json.loads(line) for line in content.splitlines() if line.strip()]


def read_snapshot(out):
    return json.loads(gzip.decompress((out / "database_evidence_snapshot.json.gz").read_bytes()))


def snapshot(out):
    from backend.shared.db.pg import pg_connect
    from psycopg import sql
    seed = json.loads((BASE / "bvp_identity_forward_correction_manifest.json").read_text())
    groups = seed["historical_scope"]["off_date_details"]
    ids = sorted({g["game_id"] for g in groups})
    rows_query = """SELECT prop_type,player_id,game_id,game_date,features,feature_set_tag,
        model_tag,lineup_slot,is_probable_sp,computed_at FROM mlb.prop_features_precomputed
        WHERE features ? 'bvp_hits' AND (game_date,game_id) IN (""" + ",".join("(%s::date,%s)" for _ in groups) + ") ORDER BY prop_type,player_id,game_id,feature_set_tag"
    params = tuple(v for g in groups for v in (g["slate_date"],g["game_id"]))
    if (out / "database_evidence_snapshot.json.gz").exists():
        raise RuntimeError("SNAPSHOT_ALREADY_EXISTS_DO_NOT_OVERWRITE")
    with pg_connect() as conn, conn.cursor() as q:
        q.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        q.execute("SET LOCAL statement_timeout='90s'")
        q.execute("SELECT current_timestamp AS observed_at, current_setting('transaction_read_only') AS read_only")
        meta = q.fetchone()
        assert meta["read_only"] == "on"
        q.execute("""SELECT count(*) AS rows,md5(string_agg(md5(row_to_json(t)::text),',' ORDER BY md5(row_to_json(t)::text))) AS row_stream_md5
            FROM mlb.prop_features_precomputed t WHERE features ? 'bvp_hits'""")
        all_before = q.fetchone()
        assert all_before["rows"] == 270582, "HISTORICAL_SCOPE_CHANGED"
        q.execute(rows_query,params)
        rows = list(q.fetchall())
        assert len(rows) == 8021, "OFFDATE_POPULATION_CHANGED"
        q.execute("""SELECT prop_type,player_id,game_id,game_date,features,feature_set_tag,
            model_tag,lineup_slot,is_probable_sp,computed_at FROM mlb.prop_features_precomputed
            WHERE game_date='2026-09-18' AND feature_set_tag='v1' AND model_tag='bvp_pvb_refresh_v1'
            ORDER BY prop_type,player_id,game_id,feature_set_tag""")
        sep18 = list(q.fetchall())
        preservation = validate_exclusion_set(sep18,exclusion_receipt())
        q.execute("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema='mlb' ORDER BY table_name,ordinal_position")
        schema = list(q.fetchall())
        tables = defaultdict(list)
        for r in schema:
            tables[r["table_name"]].append(r["column_name"])
        evidence = {}
        selected = ["game_info","bvp_stats","model_training_props","player_props","player_derived_stats",
                    "public_game_moneyline_predictions","public_game_moneyline_outcomes","today_slate_rows","today_wide_rows"]
        for table in selected:
            if table not in tables:
                evidence[table] = {"status":"ABSENT"}
                continue
            cols = [c for c in tables[table] if c != "user_id"]
            query = sql.SQL("SELECT {} FROM mlb.{} WHERE game_id = ANY(%s)").format(
                sql.SQL(",").join(map(sql.Identifier,cols)),sql.Identifier(table))
            q.execute(query,(ids,))
            data = sorted(list(q.fetchall()),key=canonical)
            evidence[table] = {"query":"SELECT noncredential columns WHERE game_id = ANY(43 affected IDs)","rows":data,"row_stream_sha256":stable_hash(data)}
        q.execute("SELECT schemaname,viewname,definition FROM pg_views WHERE schemaname IN ('mlb','public') AND (definition ILIKE '%prop_features_precomputed%' OR definition ILIKE '%bvp_%') ORDER BY schemaname,viewname")
        views = list(q.fetchall())
        q.execute("""SELECT count(*) AS rows,md5(string_agg(md5(row_to_json(t)::text),',' ORDER BY md5(row_to_json(t)::text))) AS row_stream_md5
            FROM mlb.prop_features_precomputed t WHERE features ? 'bvp_hits'""")
        all_after = q.fetchone()
        assert all_before == all_after
    result = {"contract":CONTRACT,"read_only":meta,"all_bvp":all_before,"offdate_rows":rows,
              "offdate_row_stream_sha256":stable_hash(rows),"september18":sep18,"preservation":preservation,
              "schema":schema,"database_consumers":evidence,"views":views,"database_writes":0,"http_requests":0}
    out.mkdir(parents=True,exist_ok=True)
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer,mode="wb",mtime=0,filename="") as f:
        f.write(canonical(result).encode())
    with (out / "database_evidence_snapshot.json.gz").open("xb") as f:
        f.write(buffer.getvalue())
    print(canonical({"snapshot":"PASS","offdate_rows":len(rows),"database_writes":0,"http_requests":0,"tables":{k:len(v.get("rows",[])) for k,v in evidence.items()}}))


def schedule_evidence(ids):
    """Preserve ALL occurrences, including original and makeup schedule entries."""
    result = defaultdict(list)
    paths = sorted((ROOT / "artifacts/raw/mlb/totals_feature_spine_v1/schedule").glob("*.json.gz"))
    for path in paths:
        digest = sha(path)
        for day in json.loads(gzip.decompress(path.read_bytes())).get("dates",[]):
            for game in day.get("games",[]):
                if game.get("gamePk") not in ids:
                    continue
                record = {"path":relative(path),"file_sha256":digest,"schedule_date":day["date"],
                          "official_date":game.get("officialDate"),"start_utc":game.get("gameDate"),
                          "state":game.get("status",{}).get("detailedState"),"game_id":game["gamePk"],
                          "home_team_id":game.get("teams",{}).get("home",{}).get("team",{}).get("id"),
                          "away_team_id":game.get("teams",{}).get("away",{}).get("team",{}).get("id"),
                          "schedule_game_sha256":stable_hash(game)}
                for k in ("rescheduledFrom","rescheduledFromDate","rescheduleDate","rescheduleGameDate",
                          "resumedFrom","resumedFromDate","resumeDate","resumeGameDate","description"):
                    if k in game:
                        record[k] = game[k]
                result[game["gamePk"]].append(record)
    return result


def run_evidence():
    path = ROOT / "artifacts/ops/mlb_bvp_prewarm_daily.out.log"
    text = path.read_text()
    starts = list(re.finditer(r"\[(\d{4}-\d\d-\d\dT[^\]]+)\] START local MLB BvP prewarm \(MLB_DATE_ET=(\d{4}-\d\d-\d\d)",text))
    events = []
    for n,start in enumerate(starts):
        chunk = text[start.start():starts[n+1].start() if n+1<len(starts) else len(text)]
        summary = re.search(r"\[bvp-refresh\] summary (.+)",chunk)
        done = re.search(r"\[([^\]]+)\] DONE local MLB BvP prewarm",chunk)
        fields = dict(re.findall(r"(\w+)=([0-9]+)",summary[1])) if summary else {}
        events.append({"slate_date":start[2],"start_utc":start[1],"end_utc":done[1] if done else None,
                       "summary":{k:int(v) for k,v in fields.items()},"run_identity":"TIMESTAMP_IDENTIFIED_LEGACY_WRAPPER_INVOCATION",
                       "log_path":relative(path),"segment_sha256":hashlib.sha256(chunk.encode()).hexdigest()})
    return events


def disposition(row, detail, schedules, runs, quarantined):
    """No fingerprint-only promotion; legitimate reschedule != full as-of proof."""
    if stable_hash(row) in quarantined:
        return CLASSES[4], "HASH_PINNED_COMPLETE_ROW_MATCH"
    day = str(row["game_date"])[:10]
    observed = utc_time(row["computed_at"])
    matching = [s for s in schedules if s["schedule_date"] == day and s.get("start_utc")
                and utc_time(s["start_utc"]).astimezone(PT).date().isoformat() == day]
    rescheduled = [s for s in matching if any(k in s for k in ("rescheduleDate","rescheduleGameDate","resumedFrom","resumedFromDate"))]
    if rescheduled and any(observed < utc_time(s["start_utc"]) for s in rescheduled):
        return CLASSES[1], "OFFICIAL_REQUESTED_DAY_ENTRY_WITH_EXPLICIT_POSTPONEMENT_OR_RESUMPTION_AND_PRESTART_ACQUISITION"
    boundary = detail["classification"] == "PREVIOUS_LOCAL_DATE_UTC_BOUNDARY_FINGERPRINT"
    # April 15 provides independent mapping-count and row-count reproduction.
    compatible = [r for r in runs if r["slate_date"] == day and r["end_utc"]
                  and utc_time(r["start_utc"]) <= observed <= utc_time(r["end_utc"])
                  and r["summary"].get("local_game_id_mapped",0)>0]
    if boundary and day == "2026-04-15" and any(
        r["summary"].get("local_game_id_mapped")==4
        and r["summary"].get("rows_written")==2613 for r in compatible
    ):
        return CLASSES[0], "RETAINED_RUN_TIMESTAMP_AND_FOUR_MAPPING_SUBSTITUTIONS_MATCH_FOUR_PRIOR_NIGHT_GAMES_741_ROWS"
    return CLASSES[3], "FINGERPRINT_OR_OFFDATE_ONLY_OR_MUTABLE_PROVENANCE_NO_ORIGINAL_REQUEST_IDENTITY"


def partition(snapshot_data, seed, schedules, runs):
    detail = {(g["slate_date"],g["game_id"]):g for g in seed["historical_scope"]["off_date_details"]}
    banned = {r["row_sha256"] for r in exclusion_receipt()["excluded_rows"]}
    ledger = []
    for row in snapshot_data["offdate_rows"]:
        game = detail[(str(row["game_date"])[:10],row["game_id"])]
        evidence = schedules.get(row["game_id"],[])
        group,reason = disposition(row,game,evidence,runs,banned)
        ledger.append({"key":{k:row[k] for k in KEY},"stored_game_date":str(row["game_date"])[:10],
                       "computed_at":str(row["computed_at"]),"acquisition_date_pt":utc_time(row["computed_at"]).astimezone(PT).date().isoformat(),
                       "model_tag":row["model_tag"],"row_sha256":stable_hash(row),"features_sha256":stable_hash(row["features"]),
                       "official_date":game["official_date"],"scheduled_start_utc":game["start_utc"],"scheduled_start_pt":game["start_pt"],
                       "classification":group,"reason":reason,"utc_local_fingerprint":game["classification"].startswith("PREVIOUS_LOCAL"),
                       "opposing_pitcher_id":None,"pitcher_provenance":"NOT_RETAINED_IN_ORIGINAL_FEATURE_ROW",
                       "authority_records":evidence,"retained_run_matches":[r for r in runs if r["slate_date"]==str(row["game_date"])[:10]
                            and r["end_utc"] and utc_time(r["start_utc"])<=utc_time(row["computed_at"])<=utc_time(r["end_utc"])]})
    return ledger


def source_inventory():
    paths = []
    for root in (ROOT / "backend",ROOT / "bin",ROOT / "docs",ROOT / "scripts",ROOT / "tmp/analysis",ROOT / "Makefile",Path("/Users/jerrystrain/bin"),Path("/Users/jerrystrain/Library/LaunchAgents")):
        if root.is_file():
            paths.append(root)
        elif root.exists():
            for path in root.rglob("*"):
                if path.is_file() and (path.suffix in {".py",".sql",".sh",".zsh",".md",".plist"}) and not any(p in path.parts for p in ("data","exports","__pycache__","node_modules",".git")):
                    paths.append(path)
    result = []
    for path in sorted(set(paths)):
        if path in {AUDIT_SOURCE,ROOT/"backend/mlb/tests/test_bvp_downstream_materiality_audit.py"}:
            continue  # This audit is evidence generation, not a historical consumer.
        text = path.read_text(errors="replace")
        lines = text.splitlines()
        matches = [(n+1,l.strip()) for n,l in enumerate(lines) if TOKEN.search(l)]
        if not matches:
            continue
        guarded = path.name in {"prop_workflow.py","model_trainer.py","v2_write_training_from_pfp.py"} and "certified_rows" in text
        direct = bool(re.search(r"(?:FROM|JOIN|from_|table\()[^\n]*prop_features_precomputed",text,re.I))
        research = any(t in path.name for t in ("report_","audit_","diagnos","experiment","research","backfill","preflight"))
        kind = "DOCUMENTATION" if path.suffix==".md" else "SCHEDULER_OR_WRAPPER" if path.suffix in {".sh",".zsh",".plist"} or path.name=="Makefile" else "DIRECT_QUERY_CONSUMER" if direct else "DERIVED_FEATURE_OR_ARTIFACT_CONSUMER"
        classification = "POTENTIALLY_AFFECTED_LINEAGE_INCOMPLETE" if (direct or "bvp_" in text.lower()) else "UNAFFECTED_BY_CONSTRUCTION"
        result.append({"consumer":relative(path),"sha256":sha(path),"consumer_type":kind,
                       "query_or_join_evidence":[{"line":n,"text":l[:700]} for n,l in matches if "prop_features_precomputed" in l][:15],
                       "bvp_fields":sorted(set(re.findall(r"\bbvp_\w+",text))),
                       "official_game_id_required":"VERIFIED_OFFICIAL_ID_AND_CANONICAL_MEMBERSHIP" if guarded else "STORED_ID_OR_DATE_FILTER_NOT_OFFICIAL_VERIFICATION" if direct else "NO_DIRECT_CANONICAL_ADMISSION",
                       "raw_date_sufficient_possible":direct and bool(re.search(r"(?:eq\(\"game_date|WHERE[^\n]*game_date|game_date::date <=)",text,re.I)),
                       "corrected_gate":guarded,"reachable":"YES" if direct or guarded else "TRANSITIVE_OR_NO_DIRECT_QUERY",
                       "actual_consumption":"NOT_PROVEN_BY_SOURCE_REACHABILITY","classification":classification,
                       "date_range":"DYNAMIC_OR_DEFINED_BY_CALLER; source hash identifies exact query",
                       "research_or_legacy":research or "_legacy" in path.parts,"matching_line_count":len(matches)})
    return result


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (TypeError,ValueError):
        return None


def artifact_matches(row, candidates):
    """Payload equality is a clue, NOT source-row admission proof without lineage."""
    found = []
    for source in candidates:
        if row.get("prop_type") and str(row["prop_type"]).strip().lower()!=source["prop_type"]:
            continue
        features = source["features"]
        compared = []
        for field,value in features.items():
            if not field.startswith("bvp_"):
                continue
            actual = number(row.get(field))
            if actual is None:
                actual = number(row.get(ALIASES.get(field,"")))
            if actual is not None and number(value) is not None:
                compared.append(abs(actual-float(value))<=1e-10)
        if compared and all(compared):
            explicit_hash = row.get("bvp_source_row_sha256") or row.get("pfp_source_row_sha256")
            explicit_time = row.get("bvp_source_computed_at") or row.get("pfp_computed_at")
            exact_receipt = explicit_hash == stable_hash(source) or (explicit_time and utc_time(explicit_time)==utc_time(source["computed_at"]) and len(compared)>=15)
            # An explicit compact source key/date/tag proves substantive admission,
            # even when the original execution timestamp was not preserved.
            compact = (number(row.get("bvp_source_game_id"))==source["game_id"]
                       and row.get("bvp_source_date")==str(source["game_date"])[:10]
                       and row.get("bvp_feature_set_tag")==source["feature_set_tag"]
                       and all(number(row.get(k))==number(features.get(k)) for k in
                           ("bvp_plate_appearances","bvp_at_bats","bvp_hits","bvp_total_bases")))
            found.append({"source_row_sha256":stable_hash(source),"matched_fields":len(compared),"proven_source_admission":bool(exact_receipt or compact),
                          "proof_level":"EXACT_ROW_RECEIPT" if exact_receipt else "EXPLICIT_SUBSTANTIVE_SOURCE_KEY_DATE_TAG_AND_FOUR_VALUES" if compact else "VALUE_MATCH_ONLY",
                          "original_source_timestamp_proven":bool(exact_receipt)})
    return found


def artifact_inventory(out, data, ledger):
    """Complete CSV header inventory; row scan only BvP or lineage-carrying CSVs.

    Do not inspect captures, credentials, HTTP raw responses or browser evidence.
    Binary models are hashed, never unpickled or refitted.
    """
    by_pair = defaultdict(list)
    disposition_by_hash = {r["row_sha256"]:r for r in ledger}
    for r in data["offdate_rows"]:
        by_pair[(r["game_id"],r["player_id"])].append(r)
    inventory, hits, gaps, model_inventory = [], [], [], []
    roots = (ROOT/"artifacts/analysis/model_development",ROOT/"artifacts/analysis/mlb",ROOT/"backend/mlb/data/processed",ROOT/"backend/mlb/exports",ROOT/"backend/mlb/models",ROOT/"models_out/latest",ROOT/"tmp/analysis")
    paths = sorted({p for root in roots if root.exists() for p in root.rglob("*") if p.is_file() and p.suffix in {".csv",".joblib",".pkl"} and not p.is_relative_to(out)})
    for index,path in enumerate(paths):
        if path.suffix in {".joblib",".pkl"}:
            model_inventory.append({"path":relative(path),"sha256":sha(path),"classification":"POTENTIALLY_AFFECTED_LINEAGE_INCOMPLETE",
                                    "training_row_lineage":"SEE_ASSOCIATED_MATRIX_OR_MANIFEST; binary alone does not establish consumption","deserialized":False})
            continue
        try:
            with path.open(newline="",errors="replace") as f:
                reader = csv.DictReader(f)
                fields = reader.fieldnames or []
                bvp = [c for c in fields if "bvp" in c.lower()]
                serialized = [c for c in fields if c in {"features","features_json","canonical_feature_serialization","feature_payload","feature_snapshot"}]
                keys = "player_id" in fields and "game_id" in fields
                # Indexed but never declare an outcome-only CSV unaffected just because it lacks features.
                entry = {"path":relative(path),"fields":fields,"bvp_columns":bvp,"serialized_columns":serialized,
                         "has_exact_player_game_key":keys,"scan":"HEADER_ONLY"}
                if not (keys and (bvp or serialized)):
                    inventory.append(entry)
                    continue
                entry.update({"sha256":sha(path),"scan":"FULL_ROWS","rows":0,"key_overlaps":0,"payload_matches":0,"proven_source_matches":0})
                for n,row in enumerate(reader,2):
                    entry["rows"] += 1
                    gid,pid=number(row.get("game_id")),number(row.get("player_id"))
                    if gid is None or pid is None:
                        continue
                    source_gid=number(row.get("bvp_source_game_id"))
                    candidates = by_pair.get((int(source_gid if source_gid is not None else gid),int(pid)),[])
                    if not candidates:
                        continue
                    entry["key_overlaps"] += 1
                    extracted = dict(row)
                    for column in serialized:
                        try:
                            blob=json.loads(row[column])
                            if isinstance(blob,dict):
                                extracted.update(blob.get("features",blob))
                        except (ValueError,TypeError):
                            pass
                    matches=artifact_matches(extracted,candidates)
                    entry["payload_matches"] += bool(matches)
                    entry["proven_source_matches"] += any(m["proven_source_admission"] for m in matches)
                    selected_class=[disposition_by_hash[m["source_row_sha256"]]["classification"] for m in matches]
                    hits.append({"artifact":relative(path),"artifact_sha256":entry["sha256"],"row_number":n,"artifact_row_sha256":stable_hash(row),
                                 "game_id":int(gid),"player_id":int(pid),"prop_type":row.get("prop_type"),
                                 "artifact_date":row.get("slate_date") or row.get("game_date") or row.get("date"),
                                 "explicit_bvp_source_game_id":source_gid,"explicit_bvp_source_date":row.get("bvp_source_date"),
                                 "run_tag":row.get("snapshot_run_tag") or row.get("run_tag") or row.get("manifest_run_tag"),
                                 "feature_matches":matches,"source_classes":selected_class,
                                 "classification":"DEFECT_ROWS_CONSUMED_METRIC_IMPACT" if any(m["proven_source_admission"] for m in matches) and any(c in CONFIRMED for c in selected_class)
                                     else "POTENTIALLY_AFFECTED_LINEAGE_INCOMPLETE",
                                 "limitation":"Explicit compact source identity proves substantive admission, not original pitcher/as-of lineage; key/value equality alone is not proof. No prediction impact inferred."})
                inventory.append(entry)
        except (OSError,csv.Error,ValueError) as e:
            gaps.append({"path":relative(path),"exception_type":type(e).__name__})
        if index and index%5000==0:
            print(canonical({"artifact_scan_progress":index,"paths":len(paths)}),flush=True)
    return inventory,hits,gaps,model_inventory


def metrics(rows):
    """Only explicit stored binary targets/probabilities; no outcome grading."""
    pairs=[]
    for r in rows:
        p=next((number(r.get(k)) for k in ("prob_over","model_prob_over","predicted_probability") if number(r.get(k)) is not None),None)
        y=number(r.get("target_over"))
        if y is None:
            y={"win":1,"loss":0}.get(str(r.get("actual_over_outcome")).lower())
        if p is not None and y in (0,1) and 0<=p<=1:
            pairs.append((p,y))
    if not pairs:
        return {"rows":len(rows),"scored_rows":0,"accuracy":None,"brier":None,"log_loss":None,"ece":None,"roi":None,
                "accuracy_contract":"P_OVER >= 0.5 diagnostic classification; not original wager-rule accuracy",
                "ece_contract":"10 fixed equal-width P_OVER bins; diagnostic only"}
    n=len(pairs)
    bins=defaultdict(list)
    for p,y in pairs:
        bins[min(int(p*10),9)].append((p,y))
    return {"rows":len(rows),"scored_rows":n,"accuracy":sum((p>=.5)==bool(y) for p,y in pairs)/n,
            "brier":sum((p-y)**2 for p,y in pairs)/n,
            "log_loss":-sum(y*math.log(max(p,1e-15))+(1-y)*math.log(max(1-p,1e-15)) for p,y in pairs)/n,
            "ece":sum(abs(sum(p for p,y in b)/len(b)-sum(y for p,y in b)/len(b))*len(b)/n for b in bins.values()),
            "roi":None,"accuracy_contract":"P_OVER >= 0.5 diagnostic classification; not original wager-rule accuracy",
            "ece_contract":"10 fixed equal-width P_OVER bins; diagnostic only"}


def analyze(out):
    data=read_snapshot(out)
    seed=json.loads((BASE/"bvp_identity_forward_correction_manifest.json").read_text())
    schedules=schedule_evidence({r["game_id"] for r in data["offdate_rows"]})
    runs=run_evidence()
    rows=partition(data,seed,schedules,runs)
    put_jsonl(out,"row_disposition_ledger.jsonl",rows)
    put_json(out,"retained_schedule_evidence.json",schedules)
    put_json(out,"retained_acquisition_runs.json",runs)
    consumers=source_inventory()
    put_jsonl(out,"consumer_inventory.jsonl",consumers)
    inventory,hits,gaps,models=artifact_inventory(out,data,rows)
    put_jsonl(out,"artifact_inventory.jsonl",inventory)
    put_jsonl(out,"artifact_row_lineage_matches.jsonl",hits)
    put_jsonl(out,"serialized_model_inventory.jsonl",models)
    put_json(out,"scan_gaps.json",gaps)
    counts={}
    for group in CLASSES:
        cohort=[r for r in rows if r["classification"]==group]
        counts[group]={"rows":len(cohort),"games":len({r["key"]["game_id"] for r in cohort}),
                       "slate_dates":len({r["stored_game_date"] for r in cohort}),
                       "acquisition_dates":len({r["acquisition_date_pt"] for r in cohort}),
                       "batters":len({r["key"]["player_id"] for r in cohort}),
                       "known_original_pitchers":0,"original_pitcher_count":"UNRECOVERABLE_FROM_RETAINED_ROWS"}
    impacts=[]
    proven_by_artifact=defaultdict(list)
    for hit in hits:
        if any(m["proven_source_admission"] for m in hit["feature_matches"]) and any(c in CONFIRMED for c in hit["source_classes"]):
            proven_by_artifact[hit["artifact"]].append(hit)
    for entry in inventory:
        if entry.get("key_overlaps",0):
            impacts.append({"item":entry["path"],"sha256":entry["sha256"],"key_overlaps":entry["key_overlaps"],
                            "payload_matches":entry["payload_matches"],"proven_source_matches":entry["proven_source_matches"],
                            "classification":"DEFECT_ROWS_CONSUMED_METRIC_IMPACT" if entry["path"] in proven_by_artifact else "POTENTIALLY_AFFECTED_LINEAGE_INCOMPLETE",
                            "confirmed_defect_admitted_rows":len(proven_by_artifact.get(entry["path"],[])),
                            "decision_impact":"BVP_COVERAGE_AND_CERTIFICATION_METRICS; original probabilities are not recomputed by context hydration" if entry["path"] in proven_by_artifact else "UNMEASURED_NO_ORIGINAL_SOURCE_ROW_RECEIPT"})
    # Read-only DB join is reachability, not historical source use.
    source_keys={(r["prop_type"],r["player_id"],r["game_id"]):r for r in data["offdate_rows"]}
    db_matches=[]
    for table,value in data["database_consumers"].items():
        selected=[]
        for r in value.get("rows",[]):
            key=(r.get("prop_type"),r.get("player_id"),r.get("game_id"))
            if key in source_keys:
                selected.append({"table":table,"derived_row_sha256":stable_hash(r),"source_row_sha256":stable_hash(source_keys[key]),
                                 "prop_type":key[0],"player_id":key[1],"game_id":key[2],"derived_date":str(r.get("game_date")),
                                 "created_at":str(r.get("created_at")),"updated_at":str(r.get("updated_at")),
                                 "classification":"POTENTIALLY_AFFECTED_LINEAGE_INCOMPLETE","reason":"KEY_JOIN_ONLY_NO_SOURCE_FEATURE_RECEIPT"})
        db_matches.extend(selected)
        impacts.append({"item":"mlb."+table,"snapshot_stream_sha256":value.get("row_stream_sha256"),"matched_keys":len(selected),
                        "classification":"UNAFFECTED_BY_CONSTRUCTION" if table.startswith("public_game_moneyline") or table=="player_derived_stats" else "POTENTIALLY_AFFECTED_LINEAGE_INCOMPLETE",
                        "basis":"Independent official/team/player-stat source; no BvP formula dependency" if table.startswith("public_game_moneyline") or table=="player_derived_stats" else "Retained key overlap does not prove historical feature admission"})
    put_jsonl(out,"database_key_lineage_matches.jsonl",db_matches)
    put_jsonl(out,"downstream_impact_ledger.jsonl",impacts)
    # No speculative removal from prediction ledgers: missing source provenance is explicit.
    comparisons=[]
    for path,admitted in sorted(proven_by_artifact.items()):
        original=list(csv.DictReader((ROOT/path).open()))
        bad={h["row_number"] for h in admitted}
        kept=[r for n,r in enumerate(original,2) if n not in bad]
        source_rows={m["source_row_sha256"] for h in admitted for m in h["feature_matches"] if m["proven_source_admission"]
                     and next(r["classification"] for r in rows if r["row_sha256"]==m["source_row_sha256"]) in CONFIRMED}
        comparisons.append({"artifact":path,"sha256":sha(ROOT/path),"original_rows":len(original),"confirmed_defect_selected_context_rows":len(bad),
                            "unique_confirmed_source_rows":len(source_rows),"players":len({h["player_id"] for h in admitted}),
                            "target_games":len({h["game_id"] for h in admitted}),"target_dates":len({h["artifact_date"] for h in admitted}),
                            "context_masked_rows":len(bad),"prediction_rows_removed_by_context_only_hydration":0,
                            "prediction_probability_change_in_existing_hydration_package":0,"candidate_change":"NOT_EVALUATED_NO_EXACT_AUTHORIZED_CANDIDATE_PACKAGE",
                            "before":metrics(original),"admission_filtered_sensitivity":metrics(kept),
                            "interpretation":"EXACT_SELECTED_CONTEXT_EXCLUSION; filtered scores measure population sensitivity, not rescored prediction improvement. Correct alternative source fallback cannot be replayed from this mutable snapshot.",
                            "actual_outcomes_unchanged":True,"original_pitcher_asof_lineage":"UNRESOLVED"})
    comparison={"confirmed_defect_exclusion":{"source_rows":sum(v["rows"] for k,v in counts.items() if k in CONFIRMED),"downstream_removed_rows":None,
                 "probability_change":None,"candidate_change":None,"accuracy_change":None,"brier_change":None,"log_loss_change":None,"ece_change":None,"roi_change":None,
                 "reason":"Specific compact-source admissions are proved below; original model training/fallback decisions remain unmeasurable, not zero impact."},
                "exact_retained_derived_populations":comparisons,
                "all_offdate_sensitivity_bound":{"source_rows":8021,"certified_population_change":False,"unresolved_and_legitimate_not_blanket_excluded":True},
                "other_offdate_2067":{"rows":2067,"not_automatically_defects":True},"model_refits":0}
    put_json(out,"counterfactual_metric_comparison.json",comparison)
    authority={}
    for table in ("public_game_moneyline_predictions",):
        for r in data["database_consumers"][table].get("rows",[]):
            if r.get("prediction_snapshot_class")=="DESIGNATED_DAILY_PUBLIC_SNAPSHOT" and r.get("admission_status")=="ADMITTED_SHADOW":
                authority.setdefault(r["game_id"],set()).add(str(r["game_date"])[:10])
    # The DB snapshot above includes affected IDs, not all current valid games: seed slate is immutable evidence from prior read-only reconciliation.
    for g in seed["reconstructed_september18_slate_detail"]:
        authority.setdefault(g["game_id"],set()).add(g["official_date"])
    preserved=validate_exclusion_set(data["september18"],exclusion_receipt())
    eligible=certified_rows(data["september18"],authority=authority,exclusions=exclusion_receipt()["excluded_rows"])
    assert len(eligible)==1846
    preserved.update({"eligible_batters":len({r["player_id"] for r in eligible}),"eligible_games":len({r["game_id"] for r in eligible}),
                      "legacy_pitcher_feature_certification":"NOT_RECONSTRUCTABLE","all_three_readers_guarded":all("certified_rows" in (ROOT/p).read_text() for p in
                         ("backend/domains/mlb/prop_workflow.py","backend/mlb/model_trainer.py","backend/mlb/v2_write_training_from_pfp.py")),
                      "raw_date_alone_sufficient":False,"other_active_reader_bypass":"STATIC_SCAN_AND_ACTIVE_WRAPPER_REVIEW_REQUIRED; research direct SQL is not certified"})
    put_json(out,"september18_preservation_validation.json",preserved)
    remediation=[{"item":i["item"],"authority":"HISTORICAL_RESEARCH_OR_RETAINED_RAW; NO_RETROACTIVE_CERTIFICATION",
                  "exposure":i["classification"],"materiality":i.get("decision_impact","NO_PROVEN_FEATURE_ADMISSION"),
                  "action":"NO_ACTION" if i["classification"]=="UNAFFECTED_BY_CONSTRUCTION" else "DETERMINISTIC_REBUILD_REQUIRED" if i["classification"]=="DEFECT_ROWS_CONSUMED_METRIC_IMPACT" else "ANNOTATE_LIMITATION",
                  "rebuild_from_retained_inputs":"UNRESOLVED_ORIGINAL_PFP_PICK_LINEAGE_MISSING","rebuild_cost":"NOT_ESTIMABLE_WITHOUT_EXACT_PACKAGE",
                  "replacement_policy":"SUPERSEDE_ONLY_NEVER_OVERWRITE"} for i in impacts]
    remediation.append({"item":"Sept18 hash-pinned 91 rows","authority":"EXCLUDED_CERTIFIED_EVIDENCE","action":"QUARANTINE_FROM_CERTIFIED_USE",
                        "rebuild_from_retained_inputs":False,"rebuild_cost":"No historical recollection authorized or same-asof possible","replacement_policy":"PRESERVE_ORIGINAL_ROWS"})
    put_jsonl(out,"remediation_matrix.jsonl",remediation)
    summary={"contract":CONTRACT,"fix_commit":FIX_COMMIT,"classification":"HISTORICAL_BVP_DEFECT_BOUNDED_REBUILD_REQUIRED + HISTORICAL_BVP_DEFECT_LINEAGE_INCOMPLETE" if proven_by_artifact else "HISTORICAL_BVP_DEFECT_LINEAGE_INCOMPLETE",
             "audited_rows":270582,"offdate_rows":8021,"classes":counts,"consumers":len(consumers),"csv_artifacts_indexed":len(inventory),
             "csv_artifacts_full_scan":sum(e["scan"]=="FULL_ROWS" for e in inventory),"artifacts_with_key_overlap":len([i for i in impacts if "sha256" in i]),
             "artifact_row_key_overlaps":len(hits),"proven_artifact_source_admissions":sum(any(m["proven_source_admission"] for m in h["feature_matches"]) for h in hits),
             "confirmed_defect_artifact_admissions":{k:len(v) for k,v in proven_by_artifact.items()},
             "serialized_models":len(models),"scan_gaps":len(gaps),"current_qualified_model":json.loads((ROOT/"backend/mlb/config/model_authority.json").read_text()),
             "database_writes":0,"external_api_requests":0,"model_refits":0,"prediction_acquisition_reruns":0,"historical_rebuilds":0,
             "historical_certification":"PRE_FIX_ARTIFACTS_NOT_RETROACTIVELY_CERTIFIED","source_snapshot_sha256":sha(out/"database_evidence_snapshot.json.gz")}
    put_json(out,"summary.json",summary)
    print(canonical({"analyze":"PASS","classification":summary["classification"],"classes":counts,"artifact_key_overlaps":len(hits)}))


def manifest(out):
    files=[p for p in sorted(out.iterdir()) if p.is_file() and p.name!="sha256_manifest.json"]
    sources=[AUDIT_SOURCE,ROOT/"backend/mlb/tests/test_bvp_downstream_materiality_audit.py"]
    supporting=[BASE/"bvp_identity_forward_correction_manifest.json",EXCLUSION_PATH,
                ROOT/"backend/mlb/config/model_authority.json",
                ROOT/"backend/mlb/scripts/refresh_mlb_bvp_pvb.py",ROOT/"backend/mlb/shared/bvp_identity.py"]
    supporting.extend(ROOT/p for p in ("backend/domains/mlb/prop_workflow.py","backend/mlb/model_trainer.py",
        "backend/mlb/v2_write_training_from_pfp.py","backend/mlb/scripts/launchagent_lock.zsh",
        "bin/mlb_predictive_command_guarded.sh","tmp/analysis/run_total_bases_canonical_spine_dry_run.py","Makefile"))
    supporting.extend(ROOT/p for p in json.loads((out/"summary.json").read_text())["related_reports_requiring_superseding_context_correction"])
    supporting.extend((Path("/Users/jerrystrain/bin/proppadia_mlb_bvp_prewarm.sh"),
        Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist")))
    supporting.extend(ROOT/r["artifact"] for r in json.loads((out/"counterfactual_metric_comparison.json").read_text())["exact_retained_derived_populations"])
    supporting.extend(sorted((ROOT/"artifacts/raw/mlb/totals_feature_spine_v1/schedule").glob("*.json.gz")))
    entries={relative(p):sha(p) for p in files+sources+supporting if p.exists()}
    put_json(out,"sha256_manifest.json",{"contract":CONTRACT,"files":entries,"manifest_self_excluded":True})


def forensic(out):
    """Read retained model metadata with network disabled; never fit or score."""
    import joblib
    import warnings
    inventory=read_jsonl(out/"serialized_model_inventory.jsonl")
    previous=socket.socket.connect
    previous_resolver=socket.getaddrinfo
    previous_connection=socket.create_connection
    def forbidden(*args,**kwargs):
        raise RuntimeError("FORENSIC_NETWORK_FORBIDDEN")
    records=[]
    socket.socket.connect=forbidden
    socket.getaddrinfo=forbidden
    socket.create_connection=forbidden
    try:
        for record in inventory:
            path=ROOT/record["path"]
            evidence={**record,"deserialized":True,"model_refitted":False,"model_scored":False}
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    obj=joblib.load(path)
                meta=obj.get("meta",{}) if isinstance(obj,dict) else {}
                names=[]
                if isinstance(meta,dict):
                    for key in ("input_columns","features","feature_names","features_num","features_cat"):
                        value=meta.get(key)
                        if isinstance(value,(list,tuple)):
                            names.extend(str(v) for v in value)
                if isinstance(obj,dict):
                    for key in ("feature_names","features","input_columns"):
                        if isinstance(obj.get(key),(list,tuple)):
                            names.extend(str(v) for v in obj[key])
                for model in (list(obj.values()) if isinstance(obj,dict) else [obj]):
                    if hasattr(model,"feature_names_in_"):
                        names.extend(str(v) for v in model.feature_names_in_)
                    steps=getattr(model,"named_steps",{})
                    if isinstance(steps,dict):
                        for pre in steps.values():
                            for entry in getattr(pre,"transformers",[]) or []:
                                if isinstance(entry,tuple) and len(entry)==3 and isinstance(entry[2],(list,tuple)):
                                    names.extend(str(v) for v in entry[2])
                names=sorted(set(names))
                evidence.update({"status":"METADATA_READ","model_type":type(obj).__name__,"declared_input_columns":names,
                                 "direct_bvp_columns":[v for v in names if "bvp" in v.lower()],
                                 "training_timestamp":str(meta.get("trained_at")) if isinstance(meta,dict) else None,
                                 "source_training_row_receipts":"NOT_PRESENT_IN_SERIALIZED_MODEL_METADATA"})
            except Exception as error:
                evidence.update({"status":"METADATA_UNREADABLE","exception_type":type(error).__name__,"deserialized":False})
            records.append(evidence)
    finally:
        socket.socket.connect=previous
        socket.getaddrinfo=previous_resolver
        socket.create_connection=previous_connection
    put_jsonl(out,"serialized_model_forensic_metadata.jsonl",records)
    history=read_jsonl(ROOT/"artifacts/analysis/mlb/mlb_bvp_impact_history.jsonl")
    dates={r["stored_game_date"] for r in read_jsonl(out/"row_disposition_ledger.jsonl")}
    relevant=[r for r in history if r.get("label_date") in dates]
    put_json(out,"retained_bvp_toggle_evidence.json",{
        "source":"artifacts/analysis/mlb/mlb_bvp_impact_history.jsonl","sha256":sha(ROOT/"artifacts/analysis/mlb/mlb_bvp_impact_history.jsonl"),
        "evidence":relevant,"qualification":"Observed historical with/without-BvP tests, NOT confirmed-defect-only tests. Missing source row/pitcher/model hash prevents causal attribution. No live report re-invoked."})
    print(canonical({"forensic":"PASS","models":len(records),"readable":sum(r["status"]=="METADATA_READ" for r in records),
                     "with_declared_direct_bvp":sum(bool(r.get("direct_bvp_columns")) for r in records),"retained_toggle_reports":len(relevant),"model_refits":0,"http_requests":0}))


def readiness(out):
    label="com.proppadia.mlb.bvp.prewarm.daily"
    plist=Path("/Users/jerrystrain/Library/LaunchAgents")/(label+".plist")
    config=plistlib.loads(plist.read_bytes())
    wrapper=Path(config["ProgramArguments"][0])
    text=wrapper.read_text()
    launch=subprocess.run(["launchctl","print",f"gui/{os.getuid()}/{label}"],capture_output=True,text=True,check=False)
    state=re.search(r"\bstate = ([^\n]+)",launch.stdout)
    exit_status=re.search(r"last exit code = ([^\n]+)",launch.stdout)
    source=ROOT/"backend/mlb/scripts/refresh_mlb_bvp_pvb.py"
    before=subprocess.run(["git","show",FIX_COMMIT+":backend/mlb/scripts/refresh_mlb_bvp_pvb.py"],capture_output=True,text=True,check=True).stdout
    tree=ast.parse(source.read_text()); old=ast.parse(before)
    retry_functions=("_request_schedule_headers","_initial_transient_class","_initial_schedule_event","_fetch_initial_schedule_json")
    def extracted(tree,text):
        return {n.name:ast.get_source_segment(text,n) for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in retry_functions}
    retry_unchanged=extracted(tree,source.read_text())==extracted(old,before)
    assert retry_unchanged and config["StartCalendarInterval"]=={"Hour":3,"Minute":30}
    assert 'acquire_launchagent_lock "mlb-bvp-prewarm"' in text and 'acquire_launchagent_lock "mlb-pipeline"' in text
    assert "SKIPPED_NO_QUALIFIED_MODEL" in text
    power=subprocess.run(["pmset","-g","sched"],capture_output=True,text=True,check=False)
    result={"label":label,"plist_path":str(plist),"plist_sha256":sha(plist),"schedule":config["StartCalendarInterval"],
            "next_intended_local_dispatch":"2026-09-19T03:30:00-07:00","next_intended_dispatch_utc":"2026-09-19T10:30:00Z",
            "loaded":launch.returncode==0,"state":state[1] if state else "UNKNOWN","last_exit_code":exit_status[1] if exit_status else "UNKNOWN",
            "wrapper":str(wrapper),"wrapper_sha256":sha(wrapper),"collector_sha256":sha(source),"makefile_sha256":sha(ROOT/"Makefile"),
            "retry_contract":"BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1","retry_byte_source_unchanged":retry_unchanged,
            "identity_gate":"BVP_CANONICAL_SLATE_IDENTITY_V1","empty_and_starter_journal":"artifacts/ops/bvp_identity_v1/2026-09-19/*.jsonl",
            "governed_locks":["artifacts/ops/locks/mlb-bvp-prewarm.lock","artifacts/ops/locks/mlb-pipeline.lock"],
            "stdout":config.get("StandardOutPath"),"stderr":config.get("StandardErrorPath"),
            "no_qualified_model_skip_preserved":True,"manual_run_invoked":False,
            "repeating_power_schedule":power.stdout.split("Scheduled power events:")[0].strip(),
            "dispatch_caveat":"StartCalendarInterval does not wake the Mac. 05:27 repeating wake is later than 03:30; manual system sleep can defer dispatch. Code readiness is not a promise of timely dispatch or network health.",
            "after_next_run_require":["actual wrapper START/run tag/DONE/BVP_PREWARM_RUN_END wrapper_rc",
                "INITIAL_SCHEDULE evidence attempt count/timestamps and retry outcome",
                "identity_summary intended/verified/unmapped/rejected/prepared/written counts",
                "per-request batter/pitcher/teams/raw-response hash and actual UTC observation time",
                "starter side and reason identities; successful empty response identity retained",
                "DATABASE_WRITE_COMMITTED receipt matching admitted row payloads",
                "both lock acquisition and release; acquisition SUCCESS separately from downstream/impact model skip"]}
    gui=subprocess.run(["launchctl","print",f"gui/{os.getuid()}"],capture_output=True,text=True,check=False)
    result["loaded_proppadia_gui_jobs"]=[{"pid":int(m[1]),"last_exit":int(m[2]),"label":m[3]}
        for m in re.finditer(r"^\s+(\d+)\s+(-?\d+)\s+(com\.proppadia\.[\w.-]+)\s*$",gui.stdout,re.M)]
    result["disabled_proppadia_labels"]=[m[1] for m in re.finditer(r'"(com\.proppadia\.[^"]+)" => disabled',gui.stdout)]
    result["gui_inventory_command_exit"]=gui.returncode
    put_json(out,"tomorrow_natural_run_readiness.json",result)
    print(canonical({"readiness":"CODE_READY_WITH_POWER_DISPATCH_CAVEAT","loaded":result["loaded"],"retry_unchanged":retry_unchanged,"network_probe":False}))


def supplement(out):
    """Finite lineage/materiality additions from already retained audit evidence."""
    data=read_snapshot(out); ledger=read_jsonl(out/"row_disposition_ledger.jsonl")
    hashes={r["row_sha256"]:r for r in ledger}
    hits=read_jsonl(out/"artifact_row_lineage_matches.jsonl")
    impacts=read_jsonl(out/"downstream_impact_ledger.jsonl")
    comparisons=json.loads((out/"counterfactual_metric_comparison.json").read_text())
    for comp in comparisons["exact_retained_derived_populations"]:
        original=list(csv.DictReader((ROOT/comp["artifact"]).open()))
        artifact_hits=[h for h in hits if h["artifact"]==comp["artifact"]]
        broad={h["row_number"] for h in artifact_hits if any(m["proven_source_admission"] for m in h["feature_matches"])}
        bad={h["row_number"] for h in artifact_hits if any(m["proven_source_admission"] and hashes[m["source_row_sha256"]]["classification"] in CONFIRMED for m in h["feature_matches"])}
        before=sum(str(r.get("has_bvp_context")).lower()=="true" for r in original)
        comp["bvp_context_coverage_before"]=before
        comp["bvp_context_coverage_confirmed_source_mask_after"]=before-len(bad)
        comp["replay_needed_newly_flagged_without_alternative_source"]=sum(str(original[n-2].get("replay_needed")).lower()=="false" for n in bad)
        comp["all_offdate_selected_source_sensitivity_rows"]=len(broad)
        comp["all_offdate_selected_source_sensitivity_metrics"]=metrics([r for n,r in enumerate(original,2) if n not in broad])
        comp["all_offdate_sensitivity_certification"]=False
        comp["confirmed_source_masked_row_identities_sha256"]=stable_hash(sorted((h["row_number"],h["artifact_row_sha256"]) for h in artifact_hits if h["row_number"] in bad))
    put_json(out,"counterfactual_metric_comparison.json",comparisons)
    traces=[]
    for day in sorted({r["acquisition_date_pt"] for r in ledger}):
        selected=[r for r in ledger if r["acquisition_date_pt"]==day]
        selected_hashes={r["row_sha256"] for r in selected}
        proof=[h for h in hits if any(m["proven_source_admission"] and m["source_row_sha256"] in selected_hashes for m in h["feature_matches"])]
        traces.append({"acquisition_date_pt":day,"source_rows":len(selected),"source_games":sorted({r["key"]["game_id"] for r in selected}),
                       "slate_dates":sorted({r["stored_game_date"] for r in selected}),"classes":dict(Counter(r["classification"] for r in selected)),
                       "proven_derived_artifacts":sorted({h["artifact"] for h in proof}),"proven_derived_row_admissions":len(proof),
                       "training_model_prediction_candidate_wager_causal_impact":"UNRESOLVED_SOURCE_PICK_AND_PITCHER_LINEAGE_MISSING"})
    put_jsonl(out,"acquisition_date_downstream_trace.jsonl",traces)
    independent={
        "CANONICAL_MONEYLINE":("backend/mlb/public_game_predictions/pythagorean_log5_v1.py","backend/mlb/public_game_predictions/state_v1.py"),
        "RAW_TOTALS":("backend/mlb/totals_predictions/live_context_bridge_v1.py",),
        "TOTALS_C":("backend/mlb/scripts/run_mlb_totals_c_shadow_v1.py",),
        "HITS05_NONMARKET_PARENT":("backend/mlb/scripts/build_mlb_hits05_current_nonmarket_parent_producer.py",),
    }
    independent_records=[]
    for lane,paths in independent.items():
        record={"item":lane,"classification":"UNAFFECTED_BY_CONSTRUCTION","action":"NO_ACTION",
                "basis":"Official/team-history or player_stats strict-prior sources, not the defective PFP BvP feature family",
                "source_hashes":{p:sha(ROOT/p) for p in paths},"scope":"BvP-row defect only; not a certification of unrelated shared game_info bugs"}
        assert all("prop_features_precomputed" not in (ROOT/p).read_text() and not re.search(r"\bbvp_\w+",(ROOT/p).read_text()) for p in paths)
        independent_records.append(record)
    impacts.extend(independent_records)
    related_reports=[]
    for comp in comparisons["exact_retained_derived_populations"]:
        parent=(ROOT/comp["artifact"]).parent
        for path in sorted(parent.iterdir()):
            if path.suffix==".md" or path.name.endswith("summary.json"):
                related_reports.append({"item":relative(path),"sha256":sha(path),
                    "classification":"DEFECT_ROWS_CONSUMED_METRIC_IMPACT",
                    "basis":"Report of the source-key-proven context matrix; certification/coverage claims require a superseding correction, not rescoring",
                    "linked_context_matrix":comp["artifact"],"decision_impact":"CONTEXT_LINEAGE_COVERAGE_ONLY; no causal model-quality inference"})
    impacts.extend(related_reports)
    manual_log=ROOT/"artifacts/ops/manual_bvp_20260918T171242Z_29198.log"
    manual_text=manual_log.read_text()
    assert "SKIPPED_NO_QUALIFIED_MODEL" in manual_text and "DONE" in manual_text
    impacts.append({"item":"SEPTEMBER18_MANUAL_BVP_INVOCATION_DOWNSTREAM","sha256":sha(manual_log),
        "log_path":relative(manual_log),"classification":"DEFECT_ROWS_NOT_CONSUMED",
        "basis":"Retained invocation explicitly skipped predictive slate generation and impact reporting; no model/prediction run was invoked",
        "scope":"This manual invocation only; not proof that no independent later reader accessed its rows"})
    put_jsonl(out,"downstream_impact_ledger.jsonl",sorted({r["item"]:r for r in impacts}.values(),key=lambda r:r["item"]))
    models=read_jsonl(out/"serialized_model_forensic_metadata.jsonl")
    remediation=read_jsonl(out/"remediation_matrix.jsonl")
    for record in models:
        remediation.append({"item":record["path"],"sha256":record["sha256"],"authority":"RETAINED_RESEARCH_OR_RETIRED_UNQUALIFIED_MODEL",
                            "exposure":"DECLARED_BVP_FEATURES_NO_SOURCE_TRAINING_RECEIPT" if record.get("direct_bvp_columns") else "NO_DIRECT_BVP_FEATURE_FOUND_BUT_FULL_TRAINING_LINEAGE_NOT_PROVEN",
                            "materiality":"UNRESOLVED; do not infer coefficients or model-quality impact from schema reachability",
                            "action":"OBSOLETE_RETAIN_ONLY" if "/latest/" in record["path"] else "ANNOTATE_LIMITATION",
                            "rebuild_from_retained_inputs":"EXACT_TRAINING_PFP_SOURCE_PICKS_AND_ASOF_SNAPSHOT_NOT_RETAINED",
                            "rebuild_cost":"UNDETERMINED; NO_REFIT_AUTHORIZED_OR_PERFORMED","replacement_policy":"SUPERSEDE_IF_SEPARATELY_AUTHORIZED_NEVER_OVERWRITE"})
    remediation.extend({"item":r["item"],"authority":"AUTHORIZED_PASSIVE_OR_SHADOW_LANE","exposure":r["classification"],"materiality":"NONE_BY_SOURCE_CONSTRUCTION",
                        "action":"NO_ACTION","rebuild_from_retained_inputs":"NOT_REQUIRED","rebuild_cost":0,"replacement_policy":"PRESERVE_EXISTING"} for r in independent_records)
    remediation.extend({"item":r["item"],"sha256":r["sha256"],"authority":"HISTORICAL_CONTEXT_CERTIFICATION_REPORT",
        "exposure":r["classification"],"materiality":"PROVEN_CONTEXT_COVERAGE_AND_LINEAGE_CLAIMS",
        "action":"DETERMINISTIC_REBUILD_REQUIRED","replacement_policy":"SUPERSEDE_ONLY_NEVER_OVERWRITE"} for r in related_reports)
    for record in remediation:
        if record["action"]=="DETERMINISTIC_REBUILD_REQUIRED":
            record["rebuild_from_retained_inputs"]="YES_SELECTED_CONTEXT_CERTIFICATION_AND_COVERAGE; alternative-source rescoring is NOT exactly reconstructable"
            record["rebuild_cost"]="24,780 retained rows; zero API credits, zero refits; superseding annotation/context-mask report only"
    put_jsonl(out,"remediation_matrix.jsonl",sorted({r["item"]:r for r in remediation}.values(),key=lambda r:r["item"]))
    summary=json.loads((out/"summary.json").read_text())
    consumers=source_inventory()
    put_jsonl(out,"consumer_inventory.jsonl",consumers)
    summary.update({"model_metadata_readable":sum(r["status"]=="METADATA_READ" for r in models),
                    "consumers":len(consumers),
                    "database_evidence_cutoff_utc":data["read_only"]["observed_at"],
                    "historical_scope":{"games":43,"slate_dates":18,"acquisition_dates":17,"fingerprints":5954,"other_offdate_rows":2067},
                    "certification_boundary":{"forward":"PROSPECTIVE_IDENTITY_AND_REQUEST_JOURNALS_REQUIRE_NATURAL_RUN_VALIDATION",
                        "september18":"1846_GAME_IDENTITY_ELIGIBLE_NOT_FULL_PITCHER_FEATURE_CERTIFICATION; 91_QUARANTINED",
                        "confirmed_historical":"832_CONFIRMED_OR_QUARANTINED_ROWS; NO_RETROACTIVE_CERTIFICATION",
                        "legitimate_reschedules":"1937_DATE_IDENTITY_LEGITIMATE_ONLY",
                        "unresolved":"5252_NOT_AUTOMATICALLY_DEFECTIVE_OR_CERTIFIED",
                        "legacy_artifacts":"LIMITED_BY_ORIGINAL_SOURCE_PICK_AND_PITCHER_ASOF_PROVENANCE"},
                    "models_declaring_direct_bvp_features":sum(bool(r.get("direct_bvp_columns")) for r in models),
                    "proven_changed_coefficients":None,"proven_changed_published_or_executed_picks":None,
                    "confirmed_defect_source_rows":sum(r["classification"] in CONFIRMED for r in ledger),
                    "required_bounded_rebuilds":sorted(comp["artifact"] for comp in comparisons["exact_retained_derived_populations"]),
                    "related_reports_requiring_superseding_context_correction":sorted(r["item"] for r in related_reports),
                    "independent_current_lanes":[r["item"] for r in independent_records],
                    "inventory_limits":"Complete bounded CSV header inventory and selected feature-row scans; JSON/JSONL report and source-reference review plus forensic model metadata. Not an assertion that absent legacy DB revisions or discarded in-memory picks were recovered.",
                    "active_certified_bypass":"NONE_FOUND_IN_LOADED_PROPPADIA_GUI_AND_REVIEWED_WRAPPER_GRAPH; raw/manual archived research utilities remain noncertified"})
    put_json(out,"summary.json",summary)
    preserved=json.loads((out/"september18_preservation_validation.json").read_text())
    preserved["other_active_reader_bypass"]=summary["active_certified_bypass"]
    put_json(out,"september18_preservation_validation.json",preserved)
    # Store lineage carrying JSON/JSONL report metadata separately, without raw network payloads.
    retained_catalog=out/"report_and_metadata_inventory.jsonl"
    report_inventory=read_jsonl(retained_catalog) if retained_catalog.exists() else []
    assert all(sha(ROOT/r["path"])==r["sha256"] for r in report_inventory), "RETAINED_REPORT_CATALOG_CHANGED"
    # Reuse the first bounded discovery catalog, not an ever-moving host snapshot.
    for root in (() if report_inventory else (ROOT/"artifacts/analysis/mlb",ROOT/"artifacts/analysis/model_development")):
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.is_relative_to(out) or path.suffix not in {".md",".json",".jsonl"}:
                continue
            if path.stat().st_size>20_000_000:
                continue
            text=path.read_text(errors="replace")
            if TOKEN.search(text):
                report_inventory.append({"path":relative(path),"sha256":sha(path),"type":path.suffix,
                                         "classification":"POTENTIALLY_AFFECTED_LINEAGE_INCOMPLETE_OR_DOCUMENTATION_REFERENCE_ONLY",
                                         "reason":"Text reference alone is not proof of row admission; use source-key ledgers for actual consumption"})
    put_jsonl(out,"report_and_metadata_inventory.jsonl",report_inventory)
    summary["bvp_referencing_report_metadata_files"]=len(report_inventory)
    put_json(out,"summary.json",summary)
    print(canonical({"supplement":"PASS","acquisition_dates":len(traces),"independent_lanes":len(independent_records),"metadata_reports":len(report_inventory)}))


def validate(out):
    entries=json.loads((out/"sha256_manifest.json").read_text())["files"]
    assert entries and all(sha(ROOT/path)==digest for path,digest in entries.items()), "MANIFEST_MISMATCH"
    data=read_snapshot(out)
    rows=read_jsonl(out/"row_disposition_ledger.jsonl")
    assert len(rows)==8021 and len({tuple(r["key"][k] for k in KEY) for r in rows})==8021
    assert {r["row_sha256"] for r in rows}=={stable_hash(r) for r in data["offdate_rows"]}
    assert stable_hash(data["offdate_rows"])==data["offdate_row_stream_sha256"]
    assert sum(r["classification"]==CLASSES[4] for r in rows)==91
    assert all(r["classification"] in CLASSES for r in rows)
    seed=json.loads((BASE/"bvp_identity_forward_correction_manifest.json").read_text())
    schedules={int(k):v for k,v in json.loads((out/"retained_schedule_evidence.json").read_text()).items()}
    runs=json.loads((out/"retained_acquisition_runs.json").read_text())
    assert stable_hash(rows)==stable_hash(partition(data,seed,schedules,runs)), "DISPOSITION_REPRODUCTION_MISMATCH"
    assert validate_exclusion_set(data["september18"],exclusion_receipt())["excluded_rows"]==91
    summary=json.loads((out/"summary.json").read_text())
    assert sum(v["rows"] for v in summary["classes"].values())==8021
    assert summary["consumers"]==len(read_jsonl(out/"consumer_inventory.jsonl"))
    assert data["read_only"]["read_only"]=="on" and summary["database_writes"]==summary["external_api_requests"]==summary["model_refits"]==0
    for comp in json.loads((out/"counterfactual_metric_comparison.json").read_text())["exact_retained_derived_populations"]:
        original=list(csv.DictReader((ROOT/comp["artifact"]).open()))
        assert metrics(original)==comp["before"], "METRIC_REPRODUCTION_MISMATCH"
        assert comp["confirmed_defect_selected_context_rows"]==56 and comp["unique_confirmed_source_rows"]==46
        hashes={r["row_sha256"]:r for r in rows}
        hits=[h for h in read_jsonl(out/"artifact_row_lineage_matches.jsonl") if h["artifact"]==comp["artifact"]]
        bad={h["row_number"] for h in hits if any(m["proven_source_admission"] and hashes[m["source_row_sha256"]]["classification"] in CONFIRMED for m in h["feature_matches"])}
        assert len(bad)==56
        assert metrics([r for n,r in enumerate(original,2) if n not in bad])==comp["admission_filtered_sensitivity"]
        assert comp["bvp_context_coverage_before"]-len(bad)==comp["bvp_context_coverage_confirmed_source_mask_after"]
    print(canonical({"validation":"PASS","manifest_files":len(entries),"unique_row_dispositions":len(rows),"database_writes":0,"http_requests":0}))


def database_recheck(out):
    from backend.shared.db.pg import pg_connect
    from backend.mlb.scripts.validate_mlb_bvp_identity_v1 import validate as validate_sep18
    data=read_snapshot(out)
    with pg_connect() as conn,conn.cursor() as q:
        q.execute("SET TRANSACTION READ ONLY")
        q.execute("SET LOCAL statement_timeout='90s'")
        q.execute("""SELECT count(*) AS rows,md5(string_agg(md5(row_to_json(t)::text),',' ORDER BY md5(row_to_json(t)::text))) AS row_stream_md5
            FROM mlb.prop_features_precomputed t WHERE features ? 'bvp_hits'""")
        after=q.fetchone()
    assert after==data["all_bvp"], "HISTORICAL_BVP_POPULATION_CHANGED"
    current=validate_sep18()
    put_json(out,"final_read_only_database_integrity_check.json",{"classification":"PASS","before":data["all_bvp"],"after":after,
          "september18":current,"database_writes":0,"external_api_requests":0,"natural_host_jobs_may_change_unrelated_artifacts":True})
    print(canonical({"database_recheck":"PASS","all_historical_bvp_rows_unchanged":after["rows"],"september18_unchanged":1937,"database_writes":0}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation",choices=("snapshot","analyze","forensic","readiness","supplement","database-recheck","manifest","validate"))
    parser.add_argument("--output",type=Path,default=OUT)
    args=parser.parse_args()
    if not args.output.resolve().is_relative_to(ROOT/"artifacts/analysis/mlb/bvp_historical_downstream_materiality_v1"):
        raise RuntimeError("OUTPUT_SCOPE_VIOLATION")
    {"snapshot":snapshot,"analyze":analyze,"forensic":forensic,"readiness":readiness,"supplement":supplement,"database-recheck":database_recheck,"manifest":manifest,"validate":validate}[args.operation](args.output)


if __name__=="__main__":
    try:
        main()
    except Exception as error:
        print(canonical({"audit":"FAIL","exception_type":type(error).__name__,"reason":"BVP_AUDIT_TECHNICAL_FAILURE_NO_CREDENTIAL_DETAILS"}))
        raise SystemExit(1) from None
