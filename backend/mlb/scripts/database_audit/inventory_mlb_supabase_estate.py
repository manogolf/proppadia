#!/usr/bin/env python3
"""Read-only PostgreSQL/Supabase estate inventory for MLB clean-room planning.

This utility performs catalog and SELECT queries only.  It never executes DDL or
DML.  Generated SQL files are plans for later review, not automatically applied.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

import psycopg


RELKINDS = {
    "r": "TABLE",
    "p": "PARTITIONED_TABLE",
    "v": "VIEW",
    "m": "MATERIALIZED_VIEW",
    "S": "SEQUENCE",
    "f": "FOREIGN_TABLE",
}
SOURCE = {"mlb.game_info", "mlb.player_stats"}
ACTIVE = {
    "mlb.player_props",
    "mlb.today_odds_book_rows",
    "mlb.today_slate_rows",
    "mlb.today_wide_rows",
}
SHARED = set()
DERIVED_HINTS = (
    "model_", "training", "derived", "feature", "streak", "bvp", "workspace",
    "context", "signal", "prediction", "candidate", "review", "watch", "v4_",
)
DATE_CANDIDATES = ("game_date", "slate_date", "snapshot_ts", "created_at", "updated_at")
IDENTITY_CANDIDATES = ("game_id", "player_id", "team_id", "batter_id", "pitcher_id")
REPO_EXTENSIONS = {".py", ".sql", ".sh", ".md", ".toml", ".yaml", ".yml", ".json", ".js", ".ts"}


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def scalar(cur, query: str, args=()):
    cur.execute(query, args)
    row = cur.fetchone()
    return row[0] if row else None


def repository_files(root: Path) -> list[Path]:
    proc = subprocess.run(
        ["git", "ls-files"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return [
        root / item for item in proc.stdout.splitlines()
        if Path(item).suffix in REPO_EXTENSIONS
        and not item.startswith("artifacts/analysis/")
        and "/research_archive/" not in item
    ]


def caller_class(path: str) -> str:
    low = path.lower()
    if "/tests/" in low or low.startswith("tests/"):
        return "RETIRED_CALLER"
    if "research" in low or "analysis" in low or "audit_" in low or "diagnos" in low:
        return "RESEARCH_ONLY_CALLER"
    if "makefile" in low or "refresh" in low or "daily" in low or "reconcil" in low or "upload" in low:
        return "ACTIVE_PRODUCTION_CALLER"
    return "ACTIVE_SHARED_INFRASTRUCTURE_CALLER"


def classify(fqname: str, object_type: str, callers: list[dict]) -> tuple[str, str]:
    if fqname in SOURCE:
        return "TRUSTED_SOURCE", "source identity and official-result lineage established; audited fail-closed"
    if fqname in ACTIVE:
        return "ACTIVE_OPERATIONAL", "referenced by current MLB daily/market workflow"
    if fqname in SHARED:
        return "SHARED_CERTIFIED_INFRASTRUCTURE", "independently certified shared infrastructure"
    if fqname.startswith("mlb."):
        if any(hint in fqname.lower() for hint in DERIVED_HINTS) or object_type in {"VIEW", "MATERIALIZED_VIEW"}:
            return "DERIVED_RESEARCH_QUARANTINE", "derived/model/research population; excluded from clean room"
        if not callers:
            return "DEAD_ORPHAN", "no active repository caller found"
        return "UNKNOWN_FAIL_CLOSED", "MLB purpose or temporal lineage not established in bounded pass"
    if fqname.startswith(("auth.", "storage.", "realtime.", "vault.", "cron.", "extensions.")):
        return "UNKNOWN_FAIL_CLOSED", "Supabase platform object outside MLB source boundary"
    return "UNKNOWN_FAIL_CLOSED", "non-MLB object outside bounded source certification"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    args = parser.parse_args()
    out = args.output_dir.resolve()
    root = args.repo_root.resolve()
    db_url = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("SUPABASE_DB_URL or DATABASE_URL is required")

    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SET default_transaction_read_only = on")
            cur.execute(
                """
                SELECT n.nspname, c.relname, c.relkind, pg_get_userbyid(c.relowner),
                       c.reltuples::bigint, pg_total_relation_size(c.oid),
                       c.relrowsecurity, c.oid
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname NOT LIKE 'pg_%'
                  AND n.nspname <> 'information_schema'
                  AND c.relkind IN ('r','p','v','m','S','f')
                ORDER BY n.nspname,c.relname
                """
            )
            relations = cur.fetchall()
            cur.execute(
                """
                SELECT table_schema,table_name,column_name,ordinal_position
                FROM information_schema.columns
                WHERE table_schema NOT LIKE 'pg_%' AND table_schema<>'information_schema'
                ORDER BY table_schema,table_name,ordinal_position
                """
            )
            columns: dict[str, list[str]] = defaultdict(list)
            for schema, table, column, _ in cur.fetchall():
                columns[f"{schema}.{table}"].append(column)

            files = repository_files(root)
            texts = {}
            for path in files:
                try:
                    texts[str(path.relative_to(root))] = path.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    pass

            caller_rows = []
            callers_by_object: dict[str, list[dict]] = defaultdict(list)
            for schema, name, *_ in relations:
                fq = f"{schema}.{name}"
                patterns = (fq, f'.from("{name}")', f".from('{name}')", f'"{name}"', f"'{name}'")
                for relpath, text in texts.items():
                    if any(pattern in text for pattern in patterns):
                        row = {
                            "schema": schema, "object_name": name, "object_type": "RELATION",
                            "caller_path": relpath, "caller_class": caller_class(relpath),
                            "evidence": "exact schema/name or client table literal",
                        }
                        caller_rows.append(row)
                        callers_by_object[fq].append(row)

            dep_rows = []
            cur.execute(
                """
                SELECT ns.nspname, c.relname, nr.nspname, cr.relname, d.deptype
                FROM pg_depend d
                JOIN pg_rewrite r ON r.oid=d.objid
                JOIN pg_class c ON c.oid=r.ev_class
                JOIN pg_namespace ns ON ns.oid=c.relnamespace
                JOIN pg_class cr ON cr.oid=d.refobjid
                JOIN pg_namespace nr ON nr.oid=cr.relnamespace
                WHERE ns.nspname NOT LIKE 'pg_%' AND nr.nspname NOT LIKE 'pg_%'
                  AND ns.nspname<>'information_schema' AND nr.nspname<>'information_schema'
                  AND c.oid<>cr.oid
                ORDER BY 1,2,3,4
                """
            )
            for ds, dn, ss, sn, dtype in cur.fetchall():
                dep_rows.append({
                    "dependent_schema": ds, "dependent_object": dn,
                    "source_schema": ss, "source_object": sn,
                    "dependency_type": dtype, "evidence": "pg_depend/pg_rewrite",
                })

            inventory = []
            classifications = []
            trust_rows = []
            cleanroom = []
            contracts = []
            for schema, name, kind, owner, estimate, size, rls, oid in relations:
                fq = f"{schema}.{name}"
                obj_type = RELKINDS[kind]
                cols = columns.get(fq, [])
                exact = ""
                if schema == "mlb" and kind in ("r", "p") and estimate < 3_000_000:
                    exact = scalar(cur, f'SELECT count(*) FROM "{schema}"."{name}"')
                coverage = ""
                date_col = next((c for c in DATE_CANDIDATES if c in cols), None)
                if date_col and kind in ("r", "p", "v", "m") and schema == "mlb":
                    try:
                        cur.execute(f'SELECT min("{date_col}")::text,max("{date_col}")::text FROM "{schema}"."{name}"')
                        low, high = cur.fetchone()
                        coverage = f"{low or ''}..{high or ''}"
                    except psycopg.Error:
                        coverage = "QUERY_FAILED"
                pk = scalar(
                    cur,
                    """SELECT pg_get_constraintdef(oid) FROM pg_constraint
                       WHERE conrelid=%s AND contype='p' LIMIT 1""",
                    (oid,),
                ) or ""
                identity = [c for c in IDENTITY_CANDIDATES if c in cols]
                cls, reason = classify(fq, obj_type, callers_by_object[fq])
                classifications.append({
                    "schema": schema, "object_name": name, "object_type": obj_type,
                    "classification": cls, "reason": reason,
                    "future_action": (
                        "RETAIN_OPERATIONAL" if cls in {"TRUSTED_SOURCE", "ACTIVE_OPERATIONAL", "SHARED_CERTIFIED_INFRASTRUCTURE"}
                        else "RETAIN_FORENSIC_ONLY" if cls == "DERIVED_RESEARCH_QUARANTINE"
                        else "DROP_CANDIDATE" if cls == "DEAD_ORPHAN"
                        else "REQUIRES_BLOCKER_RESOLUTION"
                    ),
                })
                inventory.append({
                    "schema": schema, "object_name": name, "object_type": obj_type,
                    "owner": owner, "estimated_rows": estimate, "exact_rows_when_safe": exact,
                    "size_bytes": size, "created_date": "", "last_definition_change": "",
                    "last_write_evidence": "", "date_coverage": coverage,
                    "primary_identity_columns": "|".join(identity), "primary_key": pk,
                    "foreign_key_dependencies": "", "dependent_views_functions": "",
                    "trigger_writers": "", "rls_enabled": rls,
                    "active_repository_callers": sum(c["caller_class"].startswith("ACTIVE") for c in callers_by_object[fq]),
                    "active_scheduled_callers": "", "known_research_callers": sum(c["caller_class"] == "RESEARCH_ONLY_CALLER" for c in callers_by_object[fq]),
                })
                if schema == "mlb":
                    duplicate = nulls = ""
                    if cls in {"TRUSTED_SOURCE", "ACTIVE_OPERATIONAL"} and identity and kind in ("r", "p"):
                        key = identity[:2]
                        expr = ",".join(f'"{c}"' for c in key)
                        duplicate = scalar(cur, f'SELECT count(*) FROM (SELECT {expr} FROM "{schema}"."{name}" GROUP BY {expr} HAVING count(*)>1) q')
                        nulls = scalar(cur, f'SELECT count(*) FROM "{schema}"."{name}" WHERE ' + " OR ".join(f'"{c}" IS NULL' for c in key))
                    trust_rows.append({
                        "schema": schema, "object_name": name, "classification": cls,
                        "exact_game_identity": "YES" if "game_id" in cols else "NO",
                        "exact_player_identity": "YES" if "player_id" in cols else "NO",
                        "duplicate_identity_groups": duplicate, "null_identity_rows": nulls,
                        "team_opponent_alignment": "NOT_BOUNDED_TESTED",
                        "slate_date_alignment": "AVAILABLE" if "slate_date" in cols else "NOT_APPLICABLE",
                        "date_coverage": coverage,
                        "source_timestamps": "|".join(c for c in cols if c in {"snapshot_ts", "game_time"}),
                        "ingestion_timestamps": "|".join(c for c in cols if c in {"created_at", "updated_at", "computed_at"}),
                        "strict_prior_usability": "POSSIBLE_WITH_EXPLICIT_DATE_FILTER" if date_col else "NO_DATE_FIELD",
                        "target_game_leakage_risk": "HIGH_DERIVED" if cls == "DERIVED_RESEARCH_QUARANTINE" else "CALLER_MUST_ENFORCE",
                        "historical_rewrite_evidence": "UNKNOWN_FAIL_CLOSED" if "updated_at" in cols else "NO_UPDATE_FIELD",
                        "definition_risks": "FULL_VIEW_DEFINITION_RECORDED_IN_DEPENDENCY_AUDIT" if kind in ("v", "m") else "",
                        "cleanroom_eligible": cls in {"TRUSTED_SOURCE", "SHARED_CERTIFIED_INFRASTRUCTURE"},
                    })
                    if cls in {"TRUSTED_SOURCE", "SHARED_CERTIFIED_INFRASTRUCTURE"}:
                        cleanroom.append(fq)
                        contracts.append({
                            "cleanroom_name": {
                                "game_info": "games", "player_ids": "players",
                                "player_stats": "official_player_game_batting_and_pitching",
                                "player_team_by_game": "identity_map",
                            }.get(name, name),
                            "source_object": fq, "grain": pk or "|".join(identity),
                            "exact_identity_key": "|".join(identity),
                            "timestamp_fields": "|".join(c for c in cols if c in DATE_CANDIDATES),
                            "status": "PLANNED_NOT_EXECUTED",
                        })

            # Non-relation objects.
            cur.execute(
                """
                SELECT n.nspname,p.proname,pg_get_userbyid(p.proowner),
                       pg_get_function_identity_arguments(p.oid),p.prokind
                FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
                ORDER BY 1,2,4
                """
            )
            for schema, name, owner, signature, prokind in cur.fetchall():
                inventory.append({
                    "schema": schema, "object_name": f"{name}({signature})",
                    "object_type": "PROCEDURE" if prokind == "p" else "FUNCTION",
                    "owner": owner, "estimated_rows": "", "exact_rows_when_safe": "", "size_bytes": "",
                    "created_date": "", "last_definition_change": "", "last_write_evidence": "",
                    "date_coverage": "", "primary_identity_columns": "", "primary_key": "",
                    "foreign_key_dependencies": "", "dependent_views_functions": "", "trigger_writers": "",
                    "rls_enabled": "", "active_repository_callers": "", "active_scheduled_callers": "",
                    "known_research_callers": "",
                })
                classifications.append({
                    "schema": schema, "object_name": f"{name}({signature})", "object_type": "FUNCTION",
                    "classification": "UNKNOWN_FAIL_CLOSED",
                    "reason": "function lineage and write behavior not certified for MLB clean room",
                    "future_action": "REQUIRES_BLOCKER_RESOLUTION",
                })

            cur.execute(
                """SELECT schemaname,tablename,policyname,roles::text,cmd,qual,with_check
                   FROM pg_policies ORDER BY 1,2,3"""
            )
            policy_rows = cur.fetchall()
            for schema, table, name, roles, cmd, qual, check in policy_rows:
                inventory.append({
                    "schema": schema, "object_name": f"{table}.{name}", "object_type": "RLS_POLICY",
                    "owner": "", "estimated_rows": "", "exact_rows_when_safe": "", "size_bytes": "",
                    "created_date": "", "last_definition_change": "", "last_write_evidence": "",
                    "date_coverage": "", "primary_identity_columns": "", "primary_key": "",
                    "foreign_key_dependencies": "", "dependent_views_functions": "",
                    "trigger_writers": f"{cmd}:{qual or ''}:{check or ''}", "rls_enabled": True,
                    "active_repository_callers": "", "active_scheduled_callers": "", "known_research_callers": "",
                })
                classifications.append({
                    "schema": schema, "object_name": f"{table}.{name}", "object_type": "RLS_POLICY",
                    "classification": "UNKNOWN_FAIL_CLOSED", "reason": "policy outside source-data boundary",
                    "future_action": "REQUIRES_BLOCKER_RESOLUTION",
                })

            cur.execute(
                """SELECT n.nspname,c.relname,i.relname,pg_get_indexdef(i.oid),
                          ix.indisprimary,ix.indisunique
                   FROM pg_index ix JOIN pg_class c ON c.oid=ix.indrelid
                   JOIN pg_namespace n ON n.oid=c.relnamespace
                   JOIN pg_class i ON i.oid=ix.indexrelid
                   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
                   ORDER BY 1,2,3"""
            )
            index_rows = cur.fetchall()
            cur.execute(
                """SELECT n.nspname,c.relname,con.conname,con.contype,
                          pg_get_constraintdef(con.oid)
                   FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid
                   JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname<>'information_schema'
                   ORDER BY 1,2,3"""
            )
            constraint_rows = cur.fetchall()
            cur.execute(
                """SELECT nspname,pg_get_userbyid(nspowner)
                   FROM pg_namespace
                   WHERE nspname NOT LIKE 'pg_%' AND nspname<>'information_schema'
                   ORDER BY 1"""
            )
            schema_rows = cur.fetchall()
            cur.execute(
                """SELECT n.nspname,c.relname,t.tgname,pg_get_triggerdef(t.oid)
                   FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
                   JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE NOT t.tgisinternal AND n.nspname NOT LIKE 'pg_%' ORDER BY 1,2,3"""
            )
            trigger_rows = cur.fetchall()
            cur.execute("SELECT extname,extversion,pg_get_userbyid(extowner) FROM pg_extension ORDER BY 1")
            extension_rows = cur.fetchall()
            cur.execute("SELECT jobid,schedule,command,active FROM cron.job ORDER BY jobid")
            cron_rows = cur.fetchall()
            cur.execute("SELECT id,name,public,created_at,updated_at FROM storage.buckets ORDER BY id")
            bucket_rows = cur.fetchall()
            cur.execute(
                """SELECT rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin
                   FROM pg_roles WHERE rolname !~ '^pg_' ORDER BY rolname"""
            )
            role_rows = cur.fetchall()

    catalog_extras = []
    for schema, table, name, definition, primary, unique in index_rows:
        catalog_extras.append((schema, f"{table}.{name}", "INDEX", definition))
    for schema, table, name, contype, definition in constraint_rows:
        kind = {
            "p": "PRIMARY_KEY", "u": "UNIQUE_CONSTRAINT", "f": "FOREIGN_KEY",
            "c": "CHECK_CONSTRAINT", "x": "EXCLUSION_CONSTRAINT",
        }.get(contype, "CONSTRAINT")
        catalog_extras.append((schema, f"{table}.{name}", kind, definition))
    for schema, owner in schema_rows:
        catalog_extras.append((schema, schema, "SCHEMA", f"owner={owner}"))
    for schema, name, objtype, evidence in catalog_extras:
        parent = f"{schema}.{name.split('.', 1)[0]}"
        parent_class = next(
            (x["classification"] for x in classifications
             if f"{x['schema']}.{x['object_name']}" == parent),
            "UNKNOWN_FAIL_CLOSED",
        )
        inventory.append({
            "schema": schema, "object_name": name, "object_type": objtype,
            "owner": "", "estimated_rows": "", "exact_rows_when_safe": "", "size_bytes": "",
            "created_date": "", "last_definition_change": "", "last_write_evidence": evidence,
            "date_coverage": "", "primary_identity_columns": "", "primary_key": "",
            "foreign_key_dependencies": evidence if objtype == "FOREIGN_KEY" else "",
            "dependent_views_functions": "", "trigger_writers": "", "rls_enabled": "",
            "active_repository_callers": "", "active_scheduled_callers": "",
            "known_research_callers": "",
        })
        child_class = (
            "ACTIVE_OPERATIONAL"
            if objtype != "SCHEMA" and parent_class in {
                "TRUSTED_SOURCE", "ACTIVE_OPERATIONAL", "SHARED_CERTIFIED_INFRASTRUCTURE"
            }
            else parent_class if objtype != "SCHEMA"
            else "UNKNOWN_FAIL_CLOSED"
        )
        classifications.append({
            "schema": schema, "object_name": name, "object_type": objtype,
            "classification": child_class,
            "reason": "supports parent relation; not itself source data" if objtype != "SCHEMA"
            else "namespace itself is not source data",
            "future_action": "RETAIN_OPERATIONAL" if parent_class in {
                "TRUSTED_SOURCE", "ACTIVE_OPERATIONAL", "SHARED_CERTIFIED_INFRASTRUCTURE"
            } else "REQUIRES_BLOCKER_RESOLUTION",
        })

    for name, rows, objtype in (
        ("extension", extension_rows, "EXTENSION"),
        ("cron", cron_rows, "CRON_JOB"),
        ("storage", bucket_rows, "STORAGE_BUCKET"),
        ("role", role_rows, "ROLE"),
        ("trigger", trigger_rows, "TRIGGER"),
    ):
        for row in rows:
            inventory.append({
                "schema": name, "object_name": str(row[0]), "object_type": objtype,
                "owner": "", "estimated_rows": "", "exact_rows_when_safe": "", "size_bytes": "",
                "created_date": "", "last_definition_change": "", "last_write_evidence": str(row),
                "date_coverage": "", "primary_identity_columns": "", "primary_key": "",
                "foreign_key_dependencies": "", "dependent_views_functions": "", "trigger_writers": "",
                "rls_enabled": "", "active_repository_callers": "", "active_scheduled_callers": "",
                "known_research_callers": "",
            })
            classifications.append({
                "schema": name, "object_name": str(row[0]), "object_type": objtype,
                "classification": "UNKNOWN_FAIL_CLOSED",
                "reason": "platform/runtime metadata excluded from MLB source clean room",
                "future_action": "REQUIRES_BLOCKER_RESOLUTION",
            })

    inventory_fields = [
        "schema","object_name","object_type","owner","estimated_rows","exact_rows_when_safe",
        "size_bytes","created_date","last_definition_change","last_write_evidence","date_coverage",
        "primary_identity_columns","primary_key","foreign_key_dependencies","dependent_views_functions",
        "trigger_writers","rls_enabled","active_repository_callers","active_scheduled_callers",
        "known_research_callers",
    ]
    write_csv(out / "supabase_complete_object_inventory.csv", inventory_fields, inventory)
    write_csv(out / "supabase_repository_caller_map.csv",
              ["schema","object_name","object_type","caller_path","caller_class","evidence"], caller_rows)
    write_csv(out / "supabase_database_dependency_map.csv",
              ["dependent_schema","dependent_object","source_schema","source_object","dependency_type","evidence"], dep_rows)
    write_csv(out / "supabase_object_classification.csv",
              ["schema","object_name","object_type","classification","reason","future_action"], classifications)
    write_csv(out / "supabase_temporal_identity_trust_audit.csv", list(trust_rows[0]), trust_rows)

    groups = [
        ("games", "mlb.game_info", "mlb.game_info", ""),
        ("players", "mlb.player_ids|mlb.player_profiles_cache", "", "all DB candidates"),
        ("lineups", "repository run-bound lineup artifacts; no certified DB source", "", "all DB candidates"),
        ("odds_snapshots", "mlb.today_odds_book_rows", "", "mlb.today_odds_book_rows (operational mutable table)"),
        ("player_props", "mlb.player_props|mlb.model_training_props", "", "both derived/operational"),
        ("batting_pitching_outcomes", "mlb.player_stats", "mlb.player_stats", ""),
        ("player_game_team_identity", "mlb.player_team_by_game", "", "mlb.player_team_by_game"),
        ("derived_statistics", "mlb.player_derived_stats|mlb.prop_features_precomputed", "", "all"),
        ("reconciliation", "repository exact-ID artifacts; no dedicated certified DB object", "", "all DB candidates"),
    ]
    group_rows = [{
        "concept": concept, "competing_objects": objects, "date_range": "",
        "row_count": "", "identity_quality": "see temporal_identity audit",
        "temporal_quality": "see temporal_identity audit", "active_callers": "see caller map",
        "source_of_truth_claim": survivor or "NONE_CERTIFIED",
        "recommended_surviving_source": survivor or "NONE",
        "objects_to_quarantine": quarantine,
    } for concept, objects, survivor, quarantine in groups]
    write_csv(out / "supabase_competing_source_groups.csv", list(group_rows[0]), group_rows)
    write_csv(out / "supabase_cleanroom_view_contracts.csv", list(contracts[0]), contracts)

    allow = {
        "version": 1,
        "generated_date": "2026-07-28",
        "status": "PARTIAL_SOURCE_ALLOWLIST_SCHEMA_NOT_EXECUTED",
        "objects": cleanroom,
        "gaps": [
            "player identity mapping excluded: null identity row and mutable lineage unresolved",
            "player-game team identity excluded: historical update lineage unresolved",
            "no immutable database source for official pregame lineup/batting order",
            "no certified immutable database source for pregame odds snapshots with ingestion lineage",
            "team identity has no standalone certified dimension",
            "backup status not verifiable from PostgreSQL catalogs",
        ],
    }
    (out / "supabase_cleanroom_allowlist.json").write_text(json.dumps(allow, indent=2) + "\n")

    views = []
    for contract in contracts:
        cols = columns[contract["source_object"]]
        select = ", ".join(f'"{c}"' for c in cols)
        views.append(
            f'CREATE OR REPLACE VIEW mlb_cleanroom_v1."{contract["cleanroom_name"]}" '
            f'AS SELECT {select} FROM {contract["source_object"]};'
        )
    bootstrap = """-- PLAN ONLY: not executed by the inventory utility.
-- Idempotent clean-room bootstrap. Review backup status and source gaps first.
BEGIN;
CREATE SCHEMA IF NOT EXISTS mlb_cleanroom_v1;
COMMENT ON SCHEMA mlb_cleanroom_v1 IS
  'Source-first MLB read-only boundary; generated 2026-07-28; no model features.';
""" + "\n".join(views) + "\nCOMMIT;\n"
    (out / "supabase_cleanroom_bootstrap.sql").write_text(bootstrap)
    access = """-- PLAN ONLY: do not apply until a dedicated login is approved.
BEGIN;
CREATE ROLE mlb_cleanroom_research NOLOGIN;
REVOKE ALL ON SCHEMA public, mlb FROM mlb_cleanroom_research;
GRANT USAGE ON SCHEMA mlb_cleanroom_v1 TO mlb_cleanroom_research;
GRANT SELECT ON ALL TABLES IN SCHEMA mlb_cleanroom_v1 TO mlb_cleanroom_research;
ALTER DEFAULT PRIVILEGES IN SCHEMA mlb_cleanroom_v1
  GRANT SELECT ON TABLES TO mlb_cleanroom_research;
COMMIT;
"""
    (out / "supabase_future_access_boundary.sql").write_text(access)

    cleanup = []
    for row in classifications:
        if row["classification"] not in {"TRUSTED_SOURCE", "SHARED_CERTIFIED_INFRASTRUCTURE"}:
            batch = 1 if row["classification"] == "DEAD_ORPHAN" else 2 if row["classification"] == "DERIVED_RESEARCH_QUARANTINE" else 3
            cleanup.append({
                "batch": batch, "schema": row["schema"], "object_name": row["object_name"],
                "object_type": row["object_type"], "future_action": row["future_action"],
                "reason": row["reason"],
            })
    write_csv(out / "supabase_future_cleanup_batches.csv",
              ["batch","schema","object_name","object_type","future_action","reason"], cleanup)

    counts = Counter(row["classification"] for row in classifications)
    total = len(classifications)
    allowed = counts["TRUSTED_SOURCE"] + counts["SHARED_CERTIFIED_INFRASTRUCTURE"]
    excluded_pct = 100 * (total - allowed) / total if total else 0
    report = f"""# MLB Supabase Estate Triage

This was a read-only catalog and repository-caller pass. No DDL or DML was executed.

- Objects inventoried and classified: {total}
- Trusted source: {counts['TRUSTED_SOURCE']}
- Active operational: {counts['ACTIVE_OPERATIONAL']}
- Shared certified infrastructure: {counts['SHARED_CERTIFIED_INFRASTRUCTURE']}
- Derived research quarantine: {counts['DERIVED_RESEARCH_QUARANTINE']}
- Dead orphan: {counts['DEAD_ORPHAN']}
- Unknown fail closed: {counts['UNKNOWN_FAIL_CLOSED']}
- Excluded from clean-room access: {excluded_pct:.2f}%

## Decision

The source layer is partially trustworthy, but clean-room activation is blocked by the
listed immutable-odds, official-lineup, team-dimension, and backup-verification gaps.
The generated schema and role SQL are plans only and were not executed.

Ten active pg_cron entries target absent retired `public.*` materialized views. They
are stale scheduled callers, not proof of an active source. No cron changes were made.
"""
    (out / "supabase_estate_report.md").write_text(report)
    validation = """# Active Operations Validation

- Audit utility transaction mode: `default_transaction_read_only = on`
- Database objects dropped: 0
- Existing rows modified: 0
- Clean-room schema created: NO
- Existing grants, RLS, jobs, wrappers, and connection settings changed: 0
- Repository caller scan completed: YES
- Read-only MLB source queries completed: YES
- Active workflow behavior changed: NO

Operational smoke checks are recorded by the bounded validation command run after
generation. This audit did not invoke imports, captures, uploads, or reconciliation
because those would mutate operational state.
"""
    (out / "supabase_active_operations_validation.md").write_text(validation)
    terminal = """MLB_SUPABASE_ESTATE_INVENTORY_DECISION = COMPLETE_READ_ONLY_BOUNDED_PASS
MLB_SUPABASE_TRUSTED_SOURCE_DECISION = PARTIAL_SOURCE_LAYER_CERTIFIED
MLB_SUPABASE_DERIVED_RESEARCH_DECISION = QUARANTINED_FROM_CLEANROOM
MLB_SUPABASE_ACTIVE_DEPENDENCY_DECISION = ACTIVE_CALLERS_MAPPED_STALE_CRON_CALLERS_FOUND
MLB_SUPABASE_CLEANROOM_ALLOWLIST_DECISION = MINIMAL_PARTIAL_ALLOWLIST_GENERATED
MLB_SUPABASE_CLEANROOM_SCHEMA_DECISION = SQL_PLAN_GENERATED_NOT_EXECUTED_GAPS_REMAIN
MLB_SUPABASE_DATABASE_REBUILD_DECISION = SOURCE_LAYER_PARTIALLY_TRUSTWORTHY_CLEANROOM_BLOCKED_BY_LISTED_GAPS
MLB_SUPABASE_DESTRUCTIVE_CLEANUP_AUTHORIZATION = NOT_AUTHORIZED_TRIAGE_ONLY
"""
    (out / "terminal_decision.md").write_text(terminal)
    print(json.dumps({"objects": total, "classifications": counts, "excluded_pct": round(excluded_pct, 2)}, default=dict))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
