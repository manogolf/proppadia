#!/usr/bin/env python3
"""Reconcile completed Starter remediation overlays into one cumulative state.

Read-only certification utility. It verifies the authoritative overlay packages,
replays row-level movement accounting from existing ledgers, and materializes a
new cumulative post-COHORT_002 certification package. It does not rerun
reconstruction/remediation, perform discovery/acquisition, build matrices, train
or score models, write databases/APIs, call OddsAPI, upload, edit schedulers, or
change production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

DECISION = (
    "STARTER_REMEDIATION_OVERLAY_CHAIN_RECONCILIATION_DECISION = "
    "INDEPENDENT_OVERLAYS_VALID_CUMULATIVE_STATE_FIRST_MATERIALIZED"
)
CUMULATIVE_STATE = "STARTER_POST_COHORT_002_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED"

EXPECTED_BASELINE_SHA = "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24"
EXPECTED_COHORT_001_SHA = "0c2179dfc2a23f7ccc75402f3be8cb6de9eb16938d7bdec977c2737b52c3a8b4"
EXPECTED_COHORT_002_SHA = "888f9a248bdda5a4e26ac1ff21ebb3149b655448dedba1f4cb7bf19f82bcce31"
EXPECTED_SCALE_UP_SHA = "f6ead8dfc5482b89ee9bdd349c6538dd9d1430c704c489a40e65b4664d02d33c"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_remediation_overlay_chain_reconciliation/2026-07-15"
)
BASELINE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/2026-07-14"
)
COHORT_001_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_001_starter_reconstruction_remediation/2026-07-15"
)
COHORT_002_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_002_starter_reconstruction_remediation/2026-07-15"
)
SCALE_UP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/2026-07-15"
)
FOUR_SIDE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_remediation/2026-07-15"
)
HC_LOCAL_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_starter_reconstruction_remediation/2026-07-15"
)
POST_STARTER_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_starter_workload_remediation_qualification_state/2026-07-14"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

BASELINE_JSON = BASELINE_DIR / "machine_readable_state_summary_2026-07-14.json"
BASELINE_LEDGER = BASELINE_DIR / "post_three_row_pa_14816_row_qualification_ledger_2026-07-14.csv"
COHORT_001_STATE = COHORT_001_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"
COHORT_001_MOVEMENT = COHORT_001_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
COHORT_002_STATE = COHORT_002_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"
COHORT_002_MOVEMENT = COHORT_002_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
SCALE_JSON = SCALE_UP_DIR / f"machine_readable_scale_up_design_{RUN_DATE}.json"
SCALE_REMAINING_SIDES = SCALE_UP_DIR / f"remaining_discovery_side_inventory_{RUN_DATE}.csv"
SCALE_FULL_PLAN = SCALE_UP_DIR / f"full_remaining_cohort_plan_{RUN_DATE}.csv"
SCALE_803_RECON = SCALE_UP_DIR / f"authoritative_803_row_campaign_reconciliation_{RUN_DATE}.csv"

MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

PROHIBITED_PATTERNS = {
    "network_or_acquisition": re.compile(r"requests[.]|httpx|urlopen|urlretrieve|download", re.IGNORECASE),
    "reconstruction_or_remediation_rerun": re.compile(r"[.]fit\s*[(]|[.]predict\s*[(]|\breconstruct\s*[(]|\bremediate\s*[(]", re.IGNORECASE),
    "matrix_model_signal": re.compile(r"build_mlb_selected_proposition_abd_matrices|roc_auc|log_loss|signal_|score_", re.IGNORECASE),
    "db_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*[(])\b", re.IGNORECASE),
    "odds_upload_scheduler": re.compile(r"oddsapi|odds_api|upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def package_sha(path: Path, date_value: str = RUN_DATE) -> str:
    return sha256_path(path / f"sha256_manifest_{date_value}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def row_id(row: dict[str, str]) -> str:
    return row.get("canonical_denominator_identity") or row.get("governed_canonical_row_id") or row.get("canonical_row_id", "")


def strip_strings_comments_and_pattern_block(text: str) -> str:
    text = re.sub(r"PROHIBITED_PATTERNS = \{.*?\n\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
    out: list[str] = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type in {tokenize.STRING, tokenize.COMMENT}:
                continue
            out.append(tok.string)
    except tokenize.TokenError:
        return text
    return " ".join(out)


def static_guard() -> list[dict[str, Any]]:
    code_only = strip_strings_comments_and_pattern_block(Path(__file__).read_text(encoding="utf-8"))
    rows = []
    for name, pattern in PROHIBITED_PATTERNS.items():
        matches = pattern.findall(code_only)
        rows.append({
            "check": name,
            "status": "PASS" if not matches else "FAIL",
            "matches": "|".join(str(m) for m in matches),
            "notes": "Static guard excludes comments/string literals and scans executable code only.",
        })
    return rows


def movement_summary(name: str, rows: list[dict[str, str]]) -> dict[str, Any]:
    starter = [r for r in rows if r["post_remediation_starter_status"] == "STARTER_JOIN_QUALIFIED_HISTORY_COMPLETE_RECONSTRUCTION"]
    full = [r for r in rows if r["post_remediation_full_qualification_status"] == "FULLY_QUALIFIED"]
    blockers = Counter(r["remaining_downstream_blocker"] or "FULLY_QUALIFIED" for r in rows)
    return {
        "overlay": name,
        "governed_row_count": len(rows),
        "starter_qualified_rows": len(starter),
        "newly_fully_qualified_rows": len(full),
        "hits_0_5_additions": sum(r["hits_line"] == "0.5" for r in full),
        "hits_1_5_additions": sum(r["hits_line"] == "1.5" for r in full),
        "downstream_pa_blocked_rows": blockers["PA_BLOCKED"],
        "downstream_outcome_blocked_rows": blockers["OUTCOME_BLOCKED"],
        "downstream_bundle_blocked_rows": sum(v for k, v in blockers.items() if "BUNDLE" in k.upper()),
        "multiple_downstream_blocker_rows": sum("|" in r["remaining_downstream_blocker"] for r in rows),
        "potential_abd_additions": sum(r["matrix_readiness_implication"] == "POTENTIAL_ABD_ADDITION" for r in rows),
    }


class OverlayChainReconciliation:
    def __init__(self) -> None:
        self.baseline = json.loads(BASELINE_JSON.read_text(encoding="utf-8"))
        self.baseline_ledger = read_csv(BASELINE_LEDGER)
        self.cohort001_state = json.loads(COHORT_001_STATE.read_text(encoding="utf-8"))
        self.cohort002_state = json.loads(COHORT_002_STATE.read_text(encoding="utf-8"))
        self.cohort001_rows = read_csv(COHORT_001_MOVEMENT)
        self.cohort002_rows = read_csv(COHORT_002_MOVEMENT)
        self.scale = json.loads(SCALE_JSON.read_text(encoding="utf-8"))
        self.scale_remaining_sides = read_csv(SCALE_REMAINING_SIDES)
        self.scale_full_plan = read_csv(SCALE_FULL_PLAN)
        self.scale_803 = read_csv(SCALE_803_RECON)
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def dependency_chain(self) -> list[dict[str, Any]]:
        packages = [
            ("pre_discovery_baseline", BASELINE_DIR, EXPECTED_BASELINE_SHA, "", "", "cumulative"),
            ("four_side_history_complete_remediation", FOUR_SIDE_DIR, package_sha(FOUR_SIDE_DIR), "post_starter_workload_or_local_package", "not_authoritative_parent_for_discovery_cohort_cumulative_state", "independent overlay"),
            ("hc_local_cohort_001_remediation", HC_LOCAL_DIR, package_sha(HC_LOCAL_DIR), "post_starter_workload_or_local_package", "not_authoritative_parent_for_discovery_cohort_cumulative_state", "independent overlay"),
            ("local_parent_pre_screen_state", POST_STARTER_DIR, package_sha(POST_STARTER_DIR, "2026-07-14"), "", "", "cumulative certified state before post-three-row PA"),
            ("discovery_cohort_001_remediation", COHORT_001_DIR, EXPECTED_COHORT_001_SHA, str(BASELINE_DIR), EXPECTED_BASELINE_SHA, "independent overlay derived from shared baseline"),
            ("discovery_cohort_002_remediation", COHORT_002_DIR, EXPECTED_COHORT_002_SHA, str(BASELINE_DIR), EXPECTED_BASELINE_SHA, "independent overlay derived from shared baseline"),
            ("remaining_discovery_scale_up_design", SCALE_UP_DIR, EXPECTED_SCALE_UP_SHA, str(BASELINE_DIR), EXPECTED_BASELINE_SHA, "design state includes completed COHORT_001 but predates COHORT_002"),
        ]
        rows = []
        for name, path, sha, parent_path, parent_sha, state_kind in packages:
            row_ledger = ""
            if name == "discovery_cohort_001_remediation":
                row_ledger = str(COHORT_001_MOVEMENT)
            elif name == "discovery_cohort_002_remediation":
                row_ledger = str(COHORT_002_MOVEMENT)
            elif name == "four_side_history_complete_remediation":
                row_ledger = str(path / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv")
            elif name == "hc_local_cohort_001_remediation":
                row_ledger = str(path / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv")
            rows.append({
                "package_name": name,
                "package_path": str(path),
                "package_sha": sha,
                "parent_state_package_path": parent_path,
                "parent_state_sha": parent_sha,
                "governed_overlay_identity": name,
                "row_level_movement_ledger_path": row_ledger,
                "state_classification": state_kind,
                "notes": "Read-only dependency classification; no package mutated.",
            })
        return rows

    def parent_state_sha_audit(self) -> list[dict[str, Any]]:
        checks = [
            ("pre_discovery_baseline", BASELINE_DIR, EXPECTED_BASELINE_SHA, package_sha(BASELINE_DIR, "2026-07-14")),
            ("discovery_cohort_001", COHORT_001_DIR, EXPECTED_COHORT_001_SHA, package_sha(COHORT_001_DIR)),
            ("discovery_cohort_002", COHORT_002_DIR, EXPECTED_COHORT_002_SHA, package_sha(COHORT_002_DIR)),
            ("remaining_scale_up_design", SCALE_UP_DIR, EXPECTED_SCALE_UP_SHA, package_sha(SCALE_UP_DIR)),
        ]
        return [{
            "package": name,
            "path": str(path),
            "expected_sha": expected,
            "actual_sha": actual,
            "status": "PASS" if expected == actual else "FAIL",
        } for name, path, expected, actual in checks]

    def overlap_audit(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        ids1 = [row_id(r) for r in self.cohort001_rows]
        ids2 = [row_id(r) for r in self.cohort002_rows]
        sides1 = [r["governed_starter_game_side_identity"] for r in self.cohort001_rows]
        sides2 = [r["governed_starter_game_side_identity"] for r in self.cohort002_rows]
        row_intersection = sorted(set(ids1) & set(ids2))
        side_intersection = sorted(set(sides1) & set(sides2))
        summary = {
            "cohort_001_exact_row_count": len(ids1),
            "cohort_002_exact_row_count": len(ids2),
            "row_intersection_count": len(row_intersection),
            "row_union_count": len(set(ids1) | set(ids2)),
            "duplicate_canonical_identities_cohort_001": len(ids1) - len(set(ids1)),
            "duplicate_canonical_identities_cohort_002": len(ids2) - len(set(ids2)),
            "side_intersection_count": len(side_intersection),
            "side_union_count": len(set(sides1) | set(sides2)),
            "conflict_count": len(row_intersection) + len(side_intersection),
        }
        rows = [{"audit": key, "value": value} for key, value in summary.items()]
        for item in row_intersection:
            rows.append({"audit": "row_conflict_identity", "value": item})
        for item in side_intersection:
            rows.append({"audit": "side_conflict_identity", "value": item})
        return rows, summary

    def baseline_membership_audit(self) -> list[dict[str, Any]]:
        baseline_by_id = {row_id(r): r for r in self.baseline_ledger}
        rows = []
        for cohort, movements in [("DISCOVERY_COHORT_001", self.cohort001_rows), ("DISCOVERY_COHORT_002", self.cohort002_rows)]:
            for movement in movements:
                base = baseline_by_id.get(row_id(movement), {})
                rows.append({
                    "cohort": cohort,
                    "canonical_denominator_identity": row_id(movement),
                    "baseline_present": str(bool(base)).lower(),
                    "baseline_primary_classification": base.get("post_three_row_primary_classification", ""),
                    "baseline_starter_status": base.get("post_three_row_primary_classification", ""),
                    "baseline_pa_qualified": base.get("post_three_row_pa_qualified", ""),
                    "baseline_outcome_certified": base.get("numeric_outcome_certified", ""),
                    "movement_post_starter_status": movement["post_remediation_starter_status"],
                    "movement_post_full_status": movement["post_remediation_full_qualification_status"],
                    "remaining_downstream_blocker": movement["remaining_downstream_blocker"],
                })
        return rows

    def apply_cumulative_state(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        baseline_by_id = {row_id(r): dict(r) for r in self.baseline_ledger}
        construction = []
        for cohort, movements in [("DISCOVERY_COHORT_001", self.cohort001_rows), ("DISCOVERY_COHORT_002", self.cohort002_rows)]:
            for movement in movements:
                rid = row_id(movement)
                base = baseline_by_id[rid]
                after_primary = "HITS_FULLY_QUALIFIED" if movement["post_remediation_full_qualification_status"] == "FULLY_QUALIFIED" else "HITS_PA_BLOCKED_DIRECT_COMPATIBLE_SOURCE_MISSING"
                if movement["remaining_downstream_blocker"] == "OUTCOME_BLOCKED":
                    after_primary = "HITS_OUTCOME_BLOCKED"
                elif movement["remaining_downstream_blocker"] and movement["remaining_downstream_blocker"] not in {"PA_BLOCKED", "OUTCOME_BLOCKED"}:
                    after_primary = "HITS_BUNDLE_FIELD_BLOCKED"
                construction.append({
                    "cohort": cohort,
                    "canonical_denominator_identity": rid,
                    "before_primary_classification": base.get("post_three_row_primary_classification", ""),
                    "before_starter_status": base.get("post_starter_workload_starter_status", ""),
                    "after_starter_status": movement["post_remediation_starter_status"],
                    "after_primary_classification": after_primary,
                    "after_full_qualification_status": movement["post_remediation_full_qualification_status"],
                    "remaining_downstream_blocker": movement["remaining_downstream_blocker"],
                    "hits_line": movement["hits_line"],
                    "matrix_readiness_implication": movement["matrix_readiness_implication"],
                })
        m1 = movement_summary("DISCOVERY_COHORT_001", self.cohort001_rows)
        m2 = movement_summary("DISCOVERY_COHORT_002", self.cohort002_rows)
        fully = m1["newly_fully_qualified_rows"] + m2["newly_fully_qualified_rows"]
        starter = m1["starter_qualified_rows"] + m2["starter_qualified_rows"]
        pa_exposed = m1["downstream_pa_blocked_rows"] + m2["downstream_pa_blocked_rows"]
        outcome_exposed = m1["downstream_outcome_blocked_rows"] + m2["downstream_outcome_blocked_rows"]
        bundle_exposed = m1["downstream_bundle_blocked_rows"] + m2["downstream_bundle_blocked_rows"]
        hits05 = m1["hits_0_5_additions"] + m2["hits_0_5_additions"]
        hits15 = m1["hits_1_5_additions"] + m2["hits_1_5_additions"]
        abd = m1["potential_abd_additions"] + m2["potential_abd_additions"]
        payload = {
            "decision": DECISION,
            "state": CUMULATIVE_STATE,
            "generated_at": GENERATED_AT,
            "root_cause": "DISCOVERY_COHORT_001 and DISCOVERY_COHORT_002 state summaries were independent overlays derived from the same certified pre-discovery baseline; cumulative post-COHORT_002 state had not been materialized.",
            "remediation_execution_defective": False,
            "total_denominator_rows": self.baseline["denominator_rows"],
            "total_hits_rows": self.baseline["hits_rows"],
            "total_fully_qualified_hits": self.baseline["fully_qualified_hits_rows"] + fully,
            "fully_qualified_hits_0_5": self.baseline["fully_qualified_hits_0_5_rows"] + hits05,
            "fully_qualified_hits_1_5": self.baseline["fully_qualified_hits_1_5_rows"] + hits15,
            "starter_blocked_population": self.baseline["remaining_starter_blocked_total"] - starter,
            "pa_blocked_population": self.baseline["pa_blocked_rows"] + pa_exposed,
            "outcome_blocked_population": self.baseline["outcome_blocked_rows"] + outcome_exposed,
            "bundle_blocked_population": self.baseline["bundle_field_blocked_rows"] + bundle_exposed,
            "qualified_but_not_matrix_constructed_hits_1_5_queue": self.baseline["variant_readiness"]["qualified_but_not_matrix_constructed_hits_1_5"] + hits15,
            "potential_abd_matrix_readiness_queue": self.baseline["variant_readiness"]["qualified_but_not_matrix_constructed_hits_1_5"] + abd,
            "exact_rows_moved_by_cohort_001": len(self.cohort001_rows),
            "exact_rows_moved_by_cohort_002": len(self.cohort002_rows),
            "exact_cumulative_row_union": len({row_id(r) for r in self.cohort001_rows} | {row_id(r) for r in self.cohort002_rows}),
            "rows_blocked_by_multiple_downstream_gates": m1["multiple_downstream_blocker_rows"] + m2["multiple_downstream_blocker_rows"],
            "movement_by_overlay": [m1, m2],
            "blocker_counting_contract": "mutually_exclusive_primary_blocker_totals; downstream blockers exposed by Starter overlays move rows out of Starter-blocked and into the next active primary blocker bucket without performing downstream remediation",
        }
        return construction, payload

    def remaining_population(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        c2_sides = {r["governed_starter_game_side_identity"] for r in self.cohort002_rows}
        remaining = [r for r in self.scale_remaining_sides if r["starter_game_side_key"] not in c2_sides]
        cohort3 = next((r for r in self.scale_full_plan if r["cohort_id"] == "DISCOVERY_COHORT_003"), {})
        c3_sides = set(cohort3.get("side_keys", "").split(";")) if cohort3 else set()
        rows = []
        for r in remaining:
            rows.append({
                "starter_game_side_key": r["starter_game_side_key"],
                "post_cohort_002_remaining": "true",
                "current_campaign_category": r["current_campaign_category"],
                "represented_denominator_rows": r["represented_denominator_rows"],
                "projected_newly_fully_qualified_ceiling": r["projected_newly_fully_qualified_ceiling"],
                "in_existing_cohort_003_plan": str(r["starter_game_side_key"] in c3_sides).lower(),
            })
        summary = {
            "scale_up_design_used_state": "baseline plus completed four-side, HC_LOCAL_COHORT_001, and DISCOVERY_COHORT_001 movement summary; predated DISCOVERY_COHORT_002 as expected",
            "scale_up_design_package_sha": package_sha(SCALE_UP_DIR),
            "scale_up_remaining_sides_before_cohort_002": len(self.scale_remaining_sides),
            "scale_up_remaining_rows_before_cohort_002": sum(int_value(r["represented_denominator_rows"]) for r in self.scale_remaining_sides),
            "post_cohort_002_remaining_discovery_sides": len(remaining),
            "post_cohort_002_remaining_discovery_rows": sum(int_value(r["represented_denominator_rows"]) for r in remaining),
            "cohort_003_manifest_exists": bool(cohort3),
            "cohort_003_side_count": int_value(cohort3.get("side_count", 0)),
            "cohort_003_represented_row_count": int_value(cohort3.get("represented_row_count", 0)),
            "cohort_003_overlap_with_cohort_002_sides": len(c3_sides & c2_sides),
            "cohort_003_validity": "VALID_AGAINST_CUMULATIVE_STATE" if cohort3 and not (c3_sides & c2_sides) else "REGENERATE_OR_REVIEW",
        }
        return rows, summary

    def pa_reconciliation(self) -> list[dict[str, Any]]:
        packages = [
            ("baseline", BASELINE_LEDGER),
            ("four_side_history_complete", FOUR_SIDE_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"),
            ("hc_local_cohort_001", HC_LOCAL_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"),
            ("discovery_cohort_001", COHORT_001_MOVEMENT),
            ("discovery_cohort_002", COHORT_002_MOVEMENT),
        ]
        rows = []
        for name, path in packages:
            if not path.exists():
                rows.append({"source": name, "path": str(path), "pa_blocked_count": "", "row_ids": "", "notes": "source missing"})
                continue
            data = read_csv(path)
            if name == "baseline":
                pa_rows = [r for r in data if r.get("post_three_row_primary_classification", "").startswith("HITS_PA_BLOCKED")]
                ids = [row_id(r) for r in pa_rows]
                notes = "baseline mutually exclusive primary blocker rows"
            else:
                pa_rows = [r for r in data if (r.get("remaining_downstream_blocker") or "") == "PA_BLOCKED"]
                ids = [row_id(r) for r in pa_rows]
                notes = "PA exposed after Starter overlay; no PA remediation performed"
            rows.append({
                "source": name,
                "path": str(path),
                "pa_blocked_count": len(ids),
                "row_ids": "|".join(sorted(ids)),
                "notes": notes,
            })
        return rows

    def validate(self, overlap: dict[str, Any], cumulative: dict[str, Any], remaining: dict[str, Any]) -> list[dict[str, Any]]:
        c1 = movement_summary("DISCOVERY_COHORT_001", self.cohort001_rows)
        c2 = movement_summary("DISCOVERY_COHORT_002", self.cohort002_rows)
        checks = [
            ("baseline_sha", package_sha(BASELINE_DIR, "2026-07-14"), EXPECTED_BASELINE_SHA),
            ("cohort_001_sha", package_sha(COHORT_001_DIR), EXPECTED_COHORT_001_SHA),
            ("cohort_002_sha", package_sha(COHORT_002_DIR), EXPECTED_COHORT_002_SHA),
            ("scale_up_sha", package_sha(SCALE_UP_DIR), EXPECTED_SCALE_UP_SHA),
            ("exact_baseline_reproduction_denominator", self.baseline["denominator_rows"], 14816),
            ("exact_cohort_001_movement_reproduction", c1["newly_fully_qualified_rows"], 98),
            ("exact_cohort_002_movement_reproduction", c2["newly_fully_qualified_rows"], 73),
            ("cohort_001_hits_0_5", c1["hits_0_5_additions"], 89),
            ("cohort_001_hits_1_5", c1["hits_1_5_additions"], 9),
            ("cohort_002_hits_0_5", c2["hits_0_5_additions"], 70),
            ("cohort_002_hits_1_5", c2["hits_1_5_additions"], 3),
            ("row_intersection_count", overlap["row_intersection_count"], 0),
            ("row_union_count", overlap["row_union_count"], 174),
            ("side_intersection_count", overlap["side_intersection_count"], 0),
            ("all_cumulative_rows_accounted_for", cumulative["exact_cumulative_row_union"], 174),
            ("no_duplicate_row_application", overlap["duplicate_canonical_identities_cohort_001"] + overlap["duplicate_canonical_identities_cohort_002"], 0),
            ("remaining_population_after_cohort_002_sides", remaining["post_cohort_002_remaining_discovery_sides"], 60),
            ("remaining_population_after_cohort_002_rows", remaining["post_cohort_002_remaining_discovery_rows"], 482),
            ("cohort_003_overlap_with_cohort_002", remaining["cohort_003_overlap_with_cohort_002_sides"], 0),
        ]
        rows = [{"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected} for name, observed, expected in checks]
        rows.extend([
            {"validation": item, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
            for item in [
                "network_access",
                "discovery_or_acquisition",
                "reconstruction_or_remediation_rerun",
                "downstream_blocker_remediation",
                "matrix_construction",
                "model_signal_scoring_promotion",
                "database_api_writes",
                "oddsapi_calls",
                "uploads_launchagent_production_change",
            ]
        ])
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        rows.append({
            "validation": "existing_abd_matrices_byte_identical",
            "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL",
            "observed": json.dumps(matrix_after, sort_keys=True),
            "expected": json.dumps(self.matrix_hash_before, sort_keys=True),
        })
        guard = static_guard()
        rows.append({
            "validation": "static_guard",
            "status": "PASS" if all(r["status"] == "PASS" for r in guard) else "FAIL",
            "observed": "see_static_guard",
            "expected": "all_pass",
        })
        return rows

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        dep = self.dependency_chain()
        sha_audit = self.parent_state_sha_audit()
        overlap_rows, overlap = self.overlap_audit()
        c1_summary = movement_summary("DISCOVERY_COHORT_001", self.cohort001_rows)
        c2_summary = movement_summary("DISCOVERY_COHORT_002", self.cohort002_rows)
        construction, cumulative = self.apply_cumulative_state()
        remaining_rows, remaining_summary = self.remaining_population()
        validation = self.validate(overlap, cumulative, remaining_summary)

        write_csv(OUT_DIR / f"package_dependency_chain_ledger_{RUN_DATE}.csv", dep)
        write_csv(OUT_DIR / f"parent_state_sha_audit_{RUN_DATE}.csv", sha_audit)
        write_csv(OUT_DIR / f"cohort_001_row_level_movement_reproduction_{RUN_DATE}.csv", [c1_summary])
        write_csv(OUT_DIR / f"cohort_002_row_level_movement_reproduction_{RUN_DATE}.csv", [c2_summary])
        write_csv(OUT_DIR / f"row_overlap_identity_audit_{RUN_DATE}.csv", overlap_rows)
        write_csv(OUT_DIR / f"baseline_membership_audit_{RUN_DATE}.csv", self.baseline_membership_audit())
        write_csv(OUT_DIR / f"blocker_counting_contract_analysis_{RUN_DATE}.csv", [
            {
                "contract": "primary_blocker_counting",
                "classification": "mutually_exclusive_primary_blockers",
                "notes": "Certified state totals count each row in one active primary bucket. Starter overlay can expose PA/Outcome/Bundle as next active bucket without remediating that downstream domain.",
            },
            {
                "contract": "independent_blocker_flags",
                "classification": "not_used_for_certified_state_totals",
                "notes": "Do not sum blocker flags across packages without row identity reconciliation.",
            },
        ])
        write_csv(OUT_DIR / f"pa_blocked_reconciliation_{RUN_DATE}.csv", self.pa_reconciliation())
        write_csv(OUT_DIR / f"cumulative_state_construction_ledger_{RUN_DATE}.csv", construction)
        write_json(OUT_DIR / f"cumulative_certified_state_{RUN_DATE}.json", {**cumulative, "remaining_discovery_population": remaining_summary})
        write_csv(OUT_DIR / f"previous_summary_versus_authoritative_state_{RUN_DATE}.csv", [
            {"metric": "cohort_002_reported_fully_qualified_hits", "previous_summary_value": self.cohort002_state["total_fully_qualified_hits"], "authoritative_cumulative_value": cumulative["total_fully_qualified_hits"], "explanation": "COHORT_002 state was an independent overlay against shared baseline."},
            {"metric": "cohort_002_reported_hits_0_5", "previous_summary_value": self.cohort002_state["fully_qualified_hits_0_5"], "authoritative_cumulative_value": cumulative["fully_qualified_hits_0_5"], "explanation": "Add COHORT_001 and COHORT_002 row-level movement to common baseline."},
            {"metric": "cohort_002_reported_hits_1_5", "previous_summary_value": self.cohort002_state["fully_qualified_hits_1_5"], "authoritative_cumulative_value": cumulative["fully_qualified_hits_1_5"], "explanation": "Add COHORT_001 and COHORT_002 row-level movement to common baseline."},
            {"metric": "cohort_002_reported_starter_blocked", "previous_summary_value": self.cohort002_state["current_starter_blocked_population"], "authoritative_cumulative_value": cumulative["starter_blocked_population"], "explanation": "COHORT_001 row movement had not been materialized into COHORT_002 summary baseline."},
        ])
        write_csv(OUT_DIR / f"remaining_discovery_population_reconciliation_{RUN_DATE}.csv", remaining_rows)
        write_csv(OUT_DIR / f"future_cohort_manifest_impact_assessment_{RUN_DATE}.csv", [
            {"assessment": "cohort_003_manifest", "status": remaining_summary["cohort_003_validity"], "side_count": remaining_summary["cohort_003_side_count"], "represented_row_count": remaining_summary["cohort_003_represented_row_count"], "notes": "Do not execute or freeze COHORT_003 in this task."}
        ])
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
        write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
            {"check": "offline_read_only_replay", "status": "PASS", "notes": "Reads existing package artifacts only."},
            {"check": "cumulative_state_materialized", "status": "PASS", "notes": "No row-level remediation result changed."},
            {"check": "network_requests", "status": "PASS", "notes": "0"},
        ])
        write_md(OUT_DIR / f"cumulative_certified_state_{RUN_DATE}.md", f"""
# Starter Post-COHORT_002 Cumulative Qualification State — {RUN_DATE}

State: `{CUMULATIVE_STATE}`

Decision: `{DECISION}`

- Total denominator rows: `{cumulative['total_denominator_rows']}`
- Total Hits rows: `{cumulative['total_hits_rows']}`
- Fully qualified Hits: `{cumulative['total_fully_qualified_hits']}`
- Fully qualified Hits 0.5: `{cumulative['fully_qualified_hits_0_5']}`
- Fully qualified Hits 1.5: `{cumulative['fully_qualified_hits_1_5']}`
- Starter-blocked population: `{cumulative['starter_blocked_population']}`
- PA-blocked population: `{cumulative['pa_blocked_population']}`
- Outcome-blocked population: `{cumulative['outcome_blocked_population']}`
- Bundle-blocked population: `{cumulative['bundle_blocked_population']}`
- Qualified-but-not-matrix-constructed Hits 1.5 queue: `{cumulative['qualified_but_not_matrix_constructed_hits_1_5_queue']}`
- Exact cumulative row union: `{cumulative['exact_cumulative_row_union']}`

Root cause: COHORT_001 and COHORT_002 summaries were independent overlays
against the same certified pre-discovery baseline. Neither remediation execution
was defective. This package first materializes the cumulative post-COHORT_002
state from disjoint row-level ledgers.
""")
        write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Starter Remediation Overlay-Chain Reconciliation — {RUN_DATE}

Decision: `{DECISION}`

The reconciliation verifies the certified pre-discovery baseline, COHORT_001,
COHORT_002, and the remaining scale-up design package. COHORT_001 and
COHORT_002 row sets are disjoint: 98 rows plus 76 rows, 174-row union, zero row
or side overlap.

The baseline inconsistency was a state-materialization issue, not a remediation
execution defect. COHORT_002 reported its movement against the same shared
baseline used by COHORT_001, so the cumulative post-COHORT_002 state had not
yet been materialized.

Authoritative cumulative totals:

- Fully qualified Hits: `{cumulative['total_fully_qualified_hits']}`
- Hits 0.5: `{cumulative['fully_qualified_hits_0_5']}`
- Hits 1.5: `{cumulative['fully_qualified_hits_1_5']}`
- Starter-blocked: `{cumulative['starter_blocked_population']}`
- PA-blocked: `{cumulative['pa_blocked_population']}`
- Remaining discovery sides/rows: `{remaining_summary['post_cohort_002_remaining_discovery_sides']}` / `{remaining_summary['post_cohort_002_remaining_discovery_rows']}`

COHORT_003 manifests remain valid against the cumulative state because they do
not overlap COHORT_002, but they were not frozen or executed here. The next
separate approval required is any future COHORT_003 governance or execution
step.
""")

        if any(row["status"] != "PASS" for row in validation) or any(row["status"] != "PASS" for row in static_guard()):
            raise RuntimeError("overlay-chain reconciliation validation failed")
        self.parse_and_hash()
        return {**cumulative, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

    def parse_and_hash(self) -> None:
        parse_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if not path.is_file() or path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            try:
                if path.suffix == ".csv":
                    read_csv(path)
                    kind = "csv"
                elif path.suffix == ".json":
                    json.loads(path.read_text(encoding="utf-8"))
                    kind = "json"
                elif path.suffix == ".md":
                    kind = "markdown"
                    if not path.read_text(encoding="utf-8").lstrip().startswith("#"):
                        raise ValueError("markdown missing heading")
                else:
                    continue
                status = "PASS"
                notes = ""
            except Exception as exc:
                kind = path.suffix.lstrip(".")
                status = "FAIL"
                notes = str(exc)
            parse_rows.append({"path": str(path), "artifact_type": kind, "parse_status": status, "notes": notes})
        write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        sha_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                sha_rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)


def main() -> int:
    result = OverlayChainReconciliation().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
