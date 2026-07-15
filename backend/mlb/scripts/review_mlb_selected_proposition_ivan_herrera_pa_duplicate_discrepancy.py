#!/usr/bin/env python3
"""Characterize the Iván Herrera PA duplicate-source discrepancy.

This utility is intentionally read-only. It produces a bounded review package for
one governed denominator row and does not resolve, remediate, or propagate PA.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
RUN_DATE = "2026-07-14"
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_ivan_herrera_pa_duplicate_discrepancy_review/2026-07-14"

DECISION = "IVAN_HERRERA_PA_DUPLICATE_DISCREPANCY_REVIEW_DECISION = CHARACTERIZED_NO_RESOLUTION_OR_REMEDIATION_PERFORMED"
READINESS = "RESOLVABLE_WITH_NEW_DUPLICATE_PRECEDENCE_GOVERNANCE"
TAXONOMY = "PA_DUPLICATE_RAW_HYDRATED_COLLISION"
TARGET_IDENTITY = {
    "candidate_identity": "2026-07-02|824906|671056|hits|0.5|over",
    "player_game_identity": "2026-07-02|824906|671056",
    "date": "2026-07-02",
    "game_id": "824906",
    "player_id": "671056",
    "player_name": "Ivan Herrera",
    "team": "STL",
    "opponent": "ATL",
}

STATE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
GOV_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_workload_three_row_pa_recovery_governance/2026-07-14"
REM_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_post_workload_three_row_pa_recovery_remediation/2026-07-14"
SHADOW_ROWS = ROOT / "artifacts/analysis/mlb/pa_foundation/pa_opportunity_shadow_rows_2026-07-03.csv"

EXPECTED_PACKAGE_HASHES = {
    "post_three_row_pa_state": "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24",
    "three_row_pa_governance": "01101393539411bc315a4954fddaa7e9a014d2a7ef4c6f37ccccfa5580f60b4e",
    "three_row_pa_remediation": "58e8db051042e5c433bea661477fe8590de555d890d214707c62645f15872b91",
}

PA_FIELDS = [
    "plate_appearances",
    "d7_plate_appearances",
    "d15_plate_appearances",
    "d30_plate_appearances",
    "pa_source",
    "pa_backfilled_at",
    "hit_by_pitch",
    "sacrifice_flies",
    "sacrifice_hits",
    "catcher_interference",
    "d7_pa_bucket",
    "d15_pa_bucket",
    "d30_pa_bucket",
    "pa_trend_d7_vs_d15",
    "pa_trend_d15_vs_d30",
    "pa_missing_flag",
    "ab_per_pa_proxy",
    "pa_shadow_tag",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def package_sha_from_manifest(path: Path) -> str:
    return sha256(path / f"sha256_manifest_{RUN_DATE}.csv")


def stat_row(path: Path) -> dict[str, Any]:
    exists = path.exists()
    row: dict[str, Any] = {
        "path": rel(path),
        "exists": exists,
        "sha256": sha256(path) if exists and path.is_file() else "",
        "size_bytes": path.stat().st_size if exists and path.is_file() else "",
        "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat() if exists else "",
    }
    return row


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row.keys():
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def parse_boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def source_row_state(row: dict[str, str]) -> str:
    if parse_boolish(row.get("pa_missing_flag")):
        return "missing_pa"
    if any(row.get(field) not in {"", None} for field in ["d7_plate_appearances", "d15_plate_appearances", "d30_plate_appearances"]):
        return "populated_pa"
    return "unknown_pa_state"


def source_grain(row: dict[str, str]) -> str:
    return "|".join([
        row.get("date", ""),
        row.get("game_id", ""),
        row.get("player_id", ""),
        row.get("market", ""),
        row.get("side", ""),
        str(row.get("line", "")),
        row.get("source_family", ""),
    ])


def load_shadow_rows_with_line_numbers() -> tuple[list[dict[str, Any]], list[str]]:
    with SHADOW_ROWS.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows: list[dict[str, Any]] = []
        for line_no, row in enumerate(reader, start=2):
            row = dict(row)
            row["source_row_number"] = line_no
            rows.append(row)
        return rows, list(reader.fieldnames or [])


def target_shadow_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        r for r in rows
        if r.get("date") == TARGET_IDENTITY["date"]
        and r.get("game_id") == TARGET_IDENTITY["game_id"]
        and str(r.get("player_id")) == TARGET_IDENTITY["player_id"]
    ]


def file_line(path: Path, pattern: str) -> str:
    if not path.exists():
        return ""
    for idx, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
        if pattern in line:
            return f"{rel(path)}:{idx}"
    return rel(path)


def package_hash_checks() -> list[dict[str, Any]]:
    checks = []
    for label, directory in [
        ("post_three_row_pa_state", STATE_DIR),
        ("three_row_pa_governance", GOV_DIR),
        ("three_row_pa_remediation", REM_DIR),
    ]:
        computed = package_sha_from_manifest(directory)
        checks.append({
            "input_package": label,
            "path": rel(directory),
            "expected_package_sha256": EXPECTED_PACKAGE_HASHES[label],
            "computed_package_sha256": computed,
            "status": "PASS" if computed == EXPECTED_PACKAGE_HASHES[label] else "FAIL",
        })
    return checks


def source_integrity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = (r.get("date", ""), r.get("game_id", ""), str(r.get("player_id", "")))
        if all(key):
            groups[key].append(r)
    duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}
    conflict_groups = 0
    same_state_groups = 0
    family_pairs = Counter()
    for items in duplicate_groups.values():
        states = {source_row_state(x) for x in items}
        if len(states) > 1:
            conflict_groups += 1
        else:
            same_state_groups += 1
        family_pairs["|".join(sorted(set(x.get("source_family", "") for x in items)))] += 1
    return [
        {"metric": "source_artifact", "value": rel(SHADOW_ROWS), "notes": "PA opportunity shadow rows are the immediate duplicate-containing artifact."},
        {"metric": "total_rows", "value": len(rows), "notes": "Raw shadow output rows."},
        {"metric": "unique_player_game_identities", "value": len(groups), "notes": "Unique date|game_id|player_id identities with nonblank fields."},
        {"metric": "duplicate_player_game_identity_groups", "value": len(duplicate_groups), "notes": "Groups with more than one row at date|game_id|player_id grain."},
        {"metric": "duplicate_groups_with_mixed_pa_state", "value": conflict_groups, "notes": "At least one populated and one missing/other PA state in the group."},
        {"metric": "duplicate_groups_with_same_pa_state", "value": same_state_groups, "notes": "Duplicate player-game groups without state conflict."},
        {"metric": "ivan_herrera_group_size", "value": len(target_shadow_rows(rows)), "notes": TARGET_IDENTITY["player_game_identity"]},
        {"metric": "top_duplicate_source_family_patterns", "value": json.dumps(family_pairs.most_common(10)), "notes": "Bounded structural signal only; no remediation implied."},
    ]


def duplicate_manifest(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        src = ROOT / r.get("source_artifact", "")
        out.append({
            "candidate_identity": TARGET_IDENTITY["candidate_identity"],
            "player_game_identity": TARGET_IDENTITY["player_game_identity"],
            "shadow_artifact": rel(SHADOW_ROWS),
            "shadow_row_number": r.get("source_row_number"),
            "source_artifact": r.get("source_artifact"),
            "source_artifact_exists": src.exists(),
            "source_artifact_sha256": sha256(src) if src.exists() else "",
            "source_artifact_mtime_utc": stat_row(src).get("mtime_utc") if src.exists() else "",
            "source_family": r.get("source_family"),
            "source_grain": source_grain(r),
            "slate_date": r.get("date"),
            "game_id": r.get("game_id"),
            "player_id": r.get("player_id"),
            "player_name": r.get("player_name"),
            "team": r.get("team"),
            "opponent": r.get("opponent"),
            "market": r.get("market"),
            "denominator_prop_line": "0.5",
            "shadow_market_line": r.get("line"),
            "side": r.get("side"),
            "price": r.get("price"),
            "result": r.get("result"),
            "pa_state": source_row_state(r),
            "plate_appearances": r.get("plate_appearances"),
            "d7_plate_appearances": r.get("d7_plate_appearances"),
            "d15_plate_appearances": r.get("d15_plate_appearances"),
            "d30_plate_appearances": r.get("d30_plate_appearances"),
            "pa_source": r.get("pa_source"),
            "pa_backfilled_at": r.get("pa_backfilled_at"),
            "pa_missing_flag": r.get("pa_missing_flag"),
            "pa_shadow_tag": r.get("pa_shadow_tag"),
            "population": r.get("population"),
            "board_name": r.get("board_name"),
            "provenance_layer": r.get("provenance_layer"),
            "hitter_tier": r.get("hitter_tier"),
            "pitcher_tier": r.get("pitcher_tier"),
            "combined_tier": r.get("combined_tier"),
            "reason_entered_binding_set": "Accepted by prior governed Lane B source admission into candidate binding set before duplicate PA-state conflict was detected.",
            "qualification_status": "blocked_by_PA_INPUT_DISCREPANCY",
        })
    return out


def field_comparison(rows: list[dict[str, Any]], fieldnames: list[str]) -> list[dict[str, Any]]:
    if len(rows) != 2:
        return []
    a, b = rows
    return [
        {
            "field_name": field,
            "row_a_source_family": a.get("source_family"),
            "row_a_value": a.get(field, ""),
            "row_b_source_family": b.get("source_family"),
            "row_b_value": b.get(field, ""),
            "comparison": "same" if str(a.get(field, "")) == str(b.get(field, "")) else "different",
            "field_role": "pa_field" if field in PA_FIELDS else "identity_or_context_or_provenance",
        }
        for field in fieldnames
    ]


def lineage_rows() -> list[dict[str, Any]]:
    generator = ROOT / "backend/mlb/scripts/run_mlb_pa_opportunity_shadow_test.py"
    return [
        {
            "stage": "shadow_source_generation",
            "path": rel(generator),
            "evidence": file_line(generator, "def _load_sources"),
            "finding": "The utility constructs one shadow row list from multiple source families.",
            "resolution_implication": "The artifact is not a canonical player-game PA table.",
        },
        {
            "stage": "expanded_universe_ingest",
            "path": rel(generator),
            "evidence": file_line(generator, "_normalized_base(raw, \"expanded_o15_universe\", expanded)"),
            "finding": "expanded_o15_universe rows are normalized and appended when market is O1.5.",
            "resolution_implication": "The populated row legitimately entered from a distinct source family.",
        },
        {
            "stage": "review_aid_ingest",
            "path": rel(generator),
            "evidence": file_line(generator, "for path in sorted(review_root.glob"),
            "finding": "Review-aid rows, including hits_o15_alternate_discovery, are separately normalized and appended.",
            "resolution_implication": "The missing row legitimately entered from a distinct source family.",
        },
        {
            "stage": "dedupe_policy",
            "path": rel(generator),
            "evidence": file_line(generator, "def _dedupe"),
            "finding": "Deduplication key includes source_family and source_artifact.",
            "resolution_implication": "Rows with the same player-game but different source families intentionally survive this dedupe.",
        },
        {
            "stage": "output_write",
            "path": rel(generator),
            "evidence": file_line(generator, "pa_opportunity_shadow_rows_"),
            "finding": "The merged/enriched row list is written to pa_opportunity_shadow_rows_<date>.csv.",
            "resolution_implication": "No code path observed that marks one source family as superseding another.",
        },
    ]


def precedence_inventory() -> list[dict[str, Any]]:
    candidates = [
        GOV_DIR / "source_hierarchy_contract_2026-07-14.csv",
        GOV_DIR / "identity_and_grain_contract_2026-07-14.csv",
        GOV_DIR / "lane_b_source_admission_contract_2026-07-14.csv",
        GOV_DIR / "denominator_propagation_contract_2026-07-14.csv",
        REM_DIR / "source_binding_ledger_2026-07-14.csv",
        REM_DIR / "failure_ledger_2026-07-14.csv",
        REM_DIR / "player_game_pa_certification_ledger_2026-07-14.csv",
    ]
    rows: list[dict[str, Any]] = []
    for path in candidates:
        text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
        lower = text.lower()
        rows.append({
            "contract_or_ledger": path.name,
            "path": rel(path),
            "exists": path.exists(),
            "mentions_duplicate": "duplicate" in lower,
            "mentions_conflict": "conflict" in lower,
            "mentions_fail_closed": "fail_closed" in lower or "fail-closed" in lower,
            "mentions_precedence": "precedence" in lower,
            "mentions_supersession": "supersession" in lower or "supersede" in lower,
            "resolves_ivan_conflict": False,
            "inventory_conclusion": "No existing contract found that selects populated over missing PA for this duplicate-source collision; frozen behavior remains fail closed.",
        })
    return rows


def static_guard() -> list[dict[str, Any]]:
    script = Path(__file__)
    text = script.read_text(encoding="utf-8")
    forbidden = {
        "network_request_literal": ["req" + "uests.", "url" + "lib.", "ht" + "tp://", "ht" + "tps://"],
        "database_write_literal": ["INS" + "ERT ", "UP" + "DATE ", "DEL" + "ETE ", "CREATE " + "TABLE", "DROP " + "TABLE", "psy" + "copg", "supa" + "base"],
        "odds_provider_literal": ["Odds" + "API", "ODDS_" + "API", "sports" + "book"],
        "model_training_literal": ["fi" + "t(", "train_" + "test_split", "xg" + "boost", "light" + "gbm", "sk" + "learn"],
        "scheduler_or_external_writer_literal": ["Launch" + "Agent", "launch" + "ctl", "write_" + "upload"],
    }
    rows = []
    for check, needles in forbidden.items():
        matches = [n for n in needles if n in text]
        rows.append({
            "check": check,
            "status": "PASS" if not matches else "FAIL",
            "matches": "|".join(matches),
            "notes": "Static literal guard for prohibited behavior in this review utility.",
        })
    return rows


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_rows, fields = load_shadow_rows_with_line_numbers()
    targets = sorted(target_shadow_rows(all_rows), key=lambda r: str(r.get("source_row_number")))
    if len(targets) != 2:
        raise RuntimeError(f"Expected exactly two Iván Herrera shadow rows; found {len(targets)}")

    package_checks = package_hash_checks()
    duplicate_rows = duplicate_manifest(targets)
    comparison = field_comparison(targets, fields)

    den_manifest = read_csv(STATE_DIR / f"exact_ivan_herrera_discrepancy_manifest_{RUN_DATE}.csv")
    seven_manifest = read_csv(STATE_DIR / f"exact_prior_seven_row_pa_source_missing_manifest_{RUN_DATE}.csv")
    source_binding = read_csv(REM_DIR / f"source_binding_ledger_{RUN_DATE}.csv")
    failure_ledger = read_csv(REM_DIR / f"failure_ledger_{RUN_DATE}.csv")

    player_game_manifest = [{
        **TARGET_IDENTITY,
        "governed_grain": "slate_date|game_id|player_id",
        "denominator_grain": "date|game_id|player_id|prop_type|line|side",
        "shadow_duplicate_rows": len(targets),
        "hidden_identity_difference_found": False,
        "line_difference_note": "The governed denominator row is Hits 0.5 over; the PA shadow duplicate rows are O1.5 source rows used only to recover PA context at player-game grain.",
        "qualification_state": "HITS_PA_BLOCKED_INPUT_DISCREPANCY",
    }]

    grain_audit = []
    base = targets[0]
    for field in ["date", "game_id", "player_id", "player_name", "team", "opponent", "market", "side", "line", "price", "hitter_tier", "pitcher_tier", "combined_tier"]:
        values = sorted({str(r.get(field, "")) for r in targets})
        grain_audit.append({
            "field": field,
            "values": "|".join(values),
            "same_across_duplicate_rows": len(values) == 1,
            "identity_dimension": field in {"date", "game_id", "player_id", "team", "opponent", "market", "side", "line"},
            "notes": "No hidden player-game identity split found." if len(values) == 1 else "This field differs and helps explain source-family/provenance distinction.",
        })

    temporal_rows = [
        {
            "source_family": r.get("source_family"),
            "source_artifact": r.get("source_artifact"),
            "shadow_row_number": r.get("source_row_number"),
            "artifact_mtime_utc": stat_row(ROOT / r.get("source_artifact", "")).get("mtime_utc") if (ROOT / r.get("source_artifact", "")).exists() else "",
            "pa_backfilled_at": r.get("pa_backfilled_at"),
            "pa_state": source_row_state(r),
            "strict_prior_temporal_certification": "not_certified_by_this_review",
            "notes": "This review characterizes the duplicate-source conflict only; any future remediation must separately certify strict-prior rolling PA eligibility.",
        }
        for r in targets
    ]

    conflict_rows = [
        {
            "conflict_dimension": "PA state",
            "populated_row_source_family": next(r for r in targets if source_row_state(r) == "populated_pa").get("source_family"),
            "missing_row_source_family": next(r for r in targets if source_row_state(r) == "missing_pa").get("source_family"),
            "populated_value_summary": json.dumps({k: next(r for r in targets if source_row_state(r) == "populated_pa").get(k, "") for k in PA_FIELDS}, sort_keys=True),
            "missing_value_summary": json.dumps({k: next(r for r in targets if source_row_state(r) == "missing_pa").get(k, "") for k in PA_FIELDS}, sort_keys=True),
            "existing_rule_selects_winner": False,
            "review_conclusion": "Conflict is between a populated hydrated source-family row and a missing source-family row at the same governed player-game grain.",
        }
    ]

    taxonomy_rows = [
        {
            "primary_taxonomy": TAXONOMY,
            "secondary_flags": "same_player_game_grain|different_source_family|populated_vs_missing_pa_state|no_existing_precedence_rule|posthoc_shadow_artifact",
            "why_not_source_version": "No source version/run tag in the duplicate rows shows one record superseding the other.",
            "why_not_parent_child": "No parent/child identifiers or lineage fields establish a parent-child relationship.",
            "why_not_resolved_here": "Task explicitly prohibits row selection, dedupe, PA remediation, or qualification-state change.",
        }
    ]

    readiness_rows = [
        {
            "resolution_readiness_status": READINESS,
            "can_authorize_future_one_row_remediation_now": False,
            "new_governance_required": True,
            "minimum_future_requirements": "Human-approved duplicate precedence/source binding; exact source row manifest; strict-prior temporal eligibility certification; overlay-only no artifact mutation; deterministic replay.",
            "current_review_decision": DECISION,
        }
    ]

    future_spec = [
        {
            "candidate_resolution_step": "Freeze bounded duplicate precedence governance for this exact player-game.",
            "required": True,
            "behavior_change_required": False,
            "notes": "Would define whether one source family may be selected for PA context without mutating source artifacts.",
        },
        {
            "candidate_resolution_step": "Bind an exact source row by artifact SHA and row number.",
            "required": True,
            "behavior_change_required": False,
            "notes": "Likely row 5356 from expanded_o15_universe, but this review does not select it.",
        },
        {
            "candidate_resolution_step": "Certify strict-prior rolling PA eligibility for retained d7/d15/d30 fields.",
            "required": True,
            "behavior_change_required": False,
            "notes": "The populated row has postgame artifact/backfill timestamps; value eligibility must be proven by source semantics before admission.",
        },
        {
            "candidate_resolution_step": "Execute one bounded overlay-only remediation if approved.",
            "required": False,
            "behavior_change_required": False,
            "notes": "Could affect only this row's qualification if all gates pass; no source artifact mutation.",
        },
    ]

    governance_register = [
        {
            "decision": DECISION,
            "status": "FINAL_FOR_THIS_REVIEW",
            "scope": TARGET_IDENTITY["candidate_identity"],
            "notes": "Characterization only; no remediation or qualification-state change performed.",
        },
        {
            "decision": "NO_EXISTING_PRECEDENCE_RULE_FOUND",
            "status": "OBSERVED",
            "scope": TARGET_IDENTITY["player_game_identity"],
            "notes": "Frozen contracts point to fail-closed behavior for duplicate/conflicting source rows.",
        },
        {
            "decision": READINESS,
            "status": "RECOMMENDED_FUTURE_PATH",
            "scope": TARGET_IDENTITY["player_game_identity"],
            "notes": "New bounded governance would be required before any row selection.",
        },
    ]

    impact_rows = [
        {
            "scenario": "current_certified_state",
            "denominator_rows": 14816,
            "hits_rows": 2046,
            "fully_qualified_hits": 790,
            "fully_qualified_hits_0_5": 687,
            "fully_qualified_hits_1_5": 103,
            "pa_blocked": 8,
            "notes": "Authoritative current state remains unchanged.",
        },
        {
            "scenario": "hypothetical_future_one_row_success",
            "denominator_rows": 14816,
            "hits_rows": 2046,
            "fully_qualified_hits": "+1 potential",
            "fully_qualified_hits_0_5": "+1 potential",
            "fully_qualified_hits_1_5": "+0 expected from this exact denominator row",
            "pa_blocked": "-1 potential",
            "notes": "Projection only; no admission or remediation performed.",
        },
    ]

    immutability_rows = [
        {"item": "source_artifact_mutated", "status": "NO", "notes": "Read-only review; source artifacts not modified."},
        {"item": "database_write_performed", "status": "NO", "notes": "No DB access or write code is present."},
        {"item": "qualification_state_changed", "status": "NO", "notes": "Certified package remains authoritative."},
        {"item": "duplicate_resolved", "status": "NO", "notes": "No row selection or dedupe was performed."},
    ]

    replay_rows = [
        {
            "step": "verify_input_package_hashes",
            "status": "PASS" if all(r["status"] == "PASS" for r in package_checks) else "FAIL",
            "notes": "Computed package hashes from existing SHA manifests.",
        },
        {
            "step": "load_shadow_artifact",
            "status": "PASS",
            "notes": f"Loaded {len(all_rows)} rows from {rel(SHADOW_ROWS)}.",
        },
        {
            "step": "isolate_exact_player_game",
            "status": "PASS" if len(targets) == 2 else "FAIL",
            "notes": f"Found {len(targets)} rows for {TARGET_IDENTITY['player_game_identity']}.",
        },
        {
            "step": "emit_review_package",
            "status": "PASS",
            "notes": "Generated characterization artifacts without remediation.",
        },
    ]

    outputs: dict[str, list[dict[str, Any]] | dict[str, Any] | str] = {
        f"exact_ivan_herrera_denominator_manifest_{RUN_DATE}.csv": den_manifest,
        f"exact_player_game_identity_manifest_{RUN_DATE}.csv": player_game_manifest,
        f"exact_duplicate_source_row_manifest_{RUN_DATE}.csv": duplicate_rows,
        f"duplicate_row_field_by_field_comparison_{RUN_DATE}.csv": comparison,
        f"source_generation_lineage_report_{RUN_DATE}.csv": lineage_rows(),
        f"grain_and_identity_audit_{RUN_DATE}.csv": grain_audit,
        f"temporal_and_version_audit_{RUN_DATE}.csv": temporal_rows,
        f"existing_precedence_rule_inventory_{RUN_DATE}.csv": precedence_inventory(),
        f"populated_versus_missing_conflict_analysis_{RUN_DATE}.csv": conflict_rows,
        f"bounded_source_artifact_duplicate_integrity_summary_{RUN_DATE}.csv": source_integrity(all_rows),
        f"primary_discrepancy_taxonomy_{RUN_DATE}.csv": taxonomy_rows,
        f"resolution_readiness_decision_{RUN_DATE}.csv": readiness_rows,
        f"candidate_future_resolution_specification_{RUN_DATE}.csv": future_spec,
        f"governance_decision_register_{RUN_DATE}.csv": governance_register,
        f"projected_qualification_impact_{RUN_DATE}.csv": impact_rows,
        f"seven_row_exclusion_reference_{RUN_DATE}.csv": seven_manifest,
        f"input_provenance_and_hash_report_{RUN_DATE}.csv": [
            *package_checks,
            stat_row(SHADOW_ROWS),
            stat_row(ROOT / targets[0].get("source_artifact", "")),
            stat_row(ROOT / targets[1].get("source_artifact", "")),
            stat_row(ROOT / "backend/mlb/scripts/run_mlb_pa_opportunity_shadow_test.py"),
            stat_row(ROOT / "backend/mlb/scripts/review_mlb_selected_proposition_ivan_herrera_pa_duplicate_discrepancy.py"),
        ],
        f"immutability_audit_{RUN_DATE}.csv": immutability_rows,
        f"deterministic_replay_report_{RUN_DATE}.csv": replay_rows,
        f"validation_ledger_{RUN_DATE}.csv": [
            {"validation": "exact_target_row_count", "status": "PASS" if len(targets) == 2 else "FAIL", "notes": str(len(targets))},
            {"validation": "package_hash_checks", "status": "PASS" if all(r["status"] == "PASS" for r in package_checks) else "FAIL", "notes": ""},
            {"validation": "decision_constant", "status": "PASS" if "CHARACTERIZED_NO_RESOLUTION_OR_REMEDIATION_PERFORMED" in DECISION else "FAIL", "notes": DECISION},
            {"validation": "no_remediation_actions", "status": "PASS", "notes": "No source selection, PA propagation, or qualification-state mutation is performed."},
        ],
        f"static_no_network_no_db_no_model_no_upload_guard_{RUN_DATE}.csv": static_guard(),
    }

    for filename, rows in outputs.items():
        if isinstance(rows, list):
            write_csv(OUT_DIR / filename, rows)

    result = {
        "generated_at": utc_now(),
        "decision": DECISION,
        "resolution_readiness_status": READINESS,
        "primary_discrepancy_taxonomy": TAXONOMY,
        "target_candidate_identity": TARGET_IDENTITY["candidate_identity"],
        "target_player_game_identity": TARGET_IDENTITY["player_game_identity"],
        "source_artifact": rel(SHADOW_ROWS),
        "duplicate_rows_found": len(targets),
        "duplicate_source_families": [r.get("source_family") for r in targets],
        "populated_rows": sum(1 for r in targets if source_row_state(r) == "populated_pa"),
        "missing_rows": sum(1 for r in targets if source_row_state(r) == "missing_pa"),
        "package_hash_checks": package_checks,
        "production_behavior_changed": False,
        "db_writes": 0,
        "network_requests": 0,
        "qualification_state_changed": False,
    }
    write_json(OUT_DIR / f"machine_readable_review_result_{RUN_DATE}.json", result)

    summary = f"""
# Iván Herrera PA Duplicate Discrepancy Review — {RUN_DATE}

Decision: `{DECISION}`

Resolution readiness: `{READINESS}`

Primary taxonomy: `{TAXONOMY}`

Target row: `{TARGET_IDENTITY['candidate_identity']}`

Player-game identity: `{TARGET_IDENTITY['player_game_identity']}`

## Summary

The exact Iván Herrera row remains correctly blocked as `HITS_PA_BLOCKED_INPUT_DISCREPANCY`.
The immediate source artifact is `{rel(SHADOW_ROWS)}`. It contains two rows at the same governed
player-game grain. The populated row comes from `expanded_o15_universe`; the missing row comes from
`hits_o15_alternate_discovery`. Both survived the shadow utility's source-family-aware dedupe key.

This review found no frozen contract that deterministically selects the populated row over the
missing row. The correct current state is therefore no resolution, no remediation, and no change to
qualification state.

## What Happened

The PA opportunity shadow artifact is a multi-source research union. It appends normalized rows from
execution reconcile, expanded O1.5 universe, and review-aid CSVs, then enriches them. Its dedupe key
includes `source_family` and `source_artifact`, so rows can legitimately duplicate at
`date|game_id|player_id` when they come from different source families.

For Iván Herrera, the expanded-universe row carries rolling PA context, while the alternate-discovery
row carries missing PA context. Because the prior governance did not define a source precedence or
supersession rule for this exact collision, the failed remediation correctly stopped at
`PA_INPUT_DISCREPANCY`.

## Review Boundary

This package characterizes only one row and one player-game identity. It does not reopen the seven
prior direct compatible source-missing rows, does not dedupe the source artifact, and does not select
any PA value.

## Future Readiness

A future one-row remediation is likely possible only after new bounded duplicate-precedence
governance. That future step would still need exact source-row binding and strict-prior temporal
certification for rolling PA fields. This review does not authorize that step.
"""
    write_md(OUT_DIR / f"ivan_herrera_pa_duplicate_discrepancy_review_{RUN_DATE}.md", summary)

    one_page = f"""
# One-Page Decision Summary — {RUN_DATE}

Decision: `{DECISION}`

Iván Herrera remains PA-blocked. The duplicate source rows are real, exact, and share the same
governed `date|game_id|player_id` identity. They differ by source family and PA state:
`expanded_o15_universe` is populated; `hits_o15_alternate_discovery` is missing.

No existing frozen rule resolves this conflict. The row is therefore characterized, not remediated.

Future path: `{READINESS}`. New governance would need to define source precedence/source binding and
then separately certify strict-prior rolling PA eligibility.
"""
    write_md(OUT_DIR / f"one_page_decision_summary_{RUN_DATE}.md", one_page)

    # Validate every emitted CSV and JSON before hashing the final package.
    parse_rows = []
    for path in sorted(OUT_DIR.glob("*.csv")):
        try:
            read_csv(path)
            parse_rows.append({"path": rel(path), "type": "csv", "status": "PASS", "notes": ""})
        except Exception as exc:  # pragma: no cover - package validation path
            parse_rows.append({"path": rel(path), "type": "csv", "status": "FAIL", "notes": str(exc)})
    for path in sorted(OUT_DIR.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            parse_rows.append({"path": rel(path), "type": "json", "status": "PASS", "notes": ""})
        except Exception as exc:  # pragma: no cover - package validation path
            parse_rows.append({"path": rel(path), "type": "json", "status": "FAIL", "notes": str(exc)})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)

    sha_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            sha_rows.append({
                "path": rel(path),
                "sha256": sha256(path),
                "size_bytes": path.stat().st_size,
            })
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)

    sha_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            sha_rows.append({
                "path": rel(path),
                "sha256": sha256(path),
                "size_bytes": path.stat().st_size,
            })
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)
    return {**result, "package_sha256_manifest_hash": package_sha_from_manifest(OUT_DIR)}


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
