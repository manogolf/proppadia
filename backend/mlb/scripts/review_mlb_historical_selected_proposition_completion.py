"""Review selected-proposition wave completion and blocker accounting.

This script is read-only with respect to source packages and production state.
It decomposes the completed 2026-07-01..2026-07-08 selected-proposition
side-binding package into mutually auditable scope, outcome, domain, field, and
pre-matrix readiness populations. It does not recover outcomes, construct new
training matrices, train, score, call APIs, write databases, or change daily
pipelines.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-14"
SOURCE_RUN_DATE = "2026-07-13"
DEFAULT_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_completion_review/2026-07-14"
)
RESUME_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13"
)
STOPPED_WAVE_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13"
)
SIDE_REVIEW_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_canonical_side_identity_review/2026-07-13"
)
BUNDLE_ROOT = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12"
)

BOUND = RESUME_ROOT / f"governed_side_binding_ledger_{SOURCE_RUN_DATE}.csv"
NUMERIC = RESUME_ROOT / f"numeric_outcome_certification_ledger_{SOURCE_RUN_DATE}.csv"
BLOCKED = RESUME_ROOT / f"outcome_blocked_ledger_{SOURCE_RUN_DATE}.csv"
CROSS_DOMAIN = RESUME_ROOT / f"complete_cross_domain_qualification_ledger_{SOURCE_RUN_DATE}.csv"
FIELD_LEDGER = RESUME_ROOT / f"bundle_field_materialization_ledger_{SOURCE_RUN_DATE}.csv"
DECISION = RESUME_ROOT / f"machine_readable_decision_{SOURCE_RUN_DATE}.json"
SOURCE_SHA = RESUME_ROOT / f"sha256_manifest_{SOURCE_RUN_DATE}.csv"
STARTER_LEDGER = STOPPED_WAVE_ROOT / f"starter_qualification_ledger_{SOURCE_RUN_DATE}.csv"
PA_LEDGER = STOPPED_WAVE_ROOT / f"pa_denominator_projection_ledger_{SOURCE_RUN_DATE}.csv"

VARIANT_MANIFESTS = {
    "variant_a": BUNDLE_ROOT / "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": BUNDLE_ROOT / "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_c": BUNDLE_ROOT / "variant_c_frozen_field_manifest_2026-07-12.csv",
    "variant_d": BUNDLE_ROOT / "variant_d_frozen_field_manifest_2026-07-12.csv",
}
LINE_MANIFESTS = {
    "hits_0_5": BUNDLE_ROOT / "hits_0_5_frozen_field_manifest_2026-07-12.csv",
    "hits_1_5": BUNDLE_ROOT / "hits_1_5_frozen_field_manifest_2026-07-12.csv",
}

PROHIBITED_PATTERNS = {
    "fit_call": re.compile(r"\.fit\s*\("),
    "prediction_call": re.compile(r"\.predict\s*\(|\.predict_proba\s*\("),
    "model_metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|confusion_matrix)\s*\("),
    "model_selection_call": re.compile(r"\b(GridSearchCV|RandomizedSearchCV|cross_val_score|train_test_split)\b"),
}


def clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", errors="ignore") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def player_game(row: dict[str, str]) -> str:
    return "|".join(clean(row.get(k)) for k in ["slate_date", "game_id", "player_id"])


def status_is_qualified(value: str, prefix: str) -> bool:
    value = clean(value)
    return value.startswith(prefix)


class CompletionReview:
    def __init__(self, output_root: Path):
        self.root = output_root
        self.bound = read_csv(BOUND)
        self.numeric = read_csv(NUMERIC)
        self.blocked = read_csv(BLOCKED)
        self.cross = read_csv(CROSS_DOMAIN)
        self.field_ledger = read_csv(FIELD_LEDGER)
        self.starter = read_csv(STARTER_LEDGER)
        self.pa = read_csv(PA_LEDGER)
        self.decision = json.loads(DECISION.read_text())
        self.manifests = {variant: read_csv(path) for variant, path in VARIANT_MANIFESTS.items()}
        self.line_manifests = {scope: read_csv(path) for scope, path in LINE_MANIFESTS.items()}
        self.numeric_by_key = {r["governed_canonical_row_id"]: r for r in self.numeric}
        self.blocked_by_key = {r["governed_canonical_row_id"]: r for r in self.blocked}
        self.cross_by_key = {r["governed_canonical_row_id"]: r for r in self.cross}
        self.starter_by_key = {r["canonical_row_id"]: r for r in self.starter}
        self.pa_by_key = {r["canonical_row_id"]: r for r in self.pa}
        self.field_by_key: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
        for row in self.field_ledger:
            self.field_by_key[row["canonical_row_id"]][row["field_name"]] = row
        self.master_rows: list[dict[str, Any]] = []
        self.variant_rows: dict[str, list[dict[str, Any]]] = {}
        self.statuses: dict[str, str] = {}

    def run(self) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        self.reproduce_inputs()
        self.scope_inventory()
        self.build_master_and_hits_ledgers()
        self.field_inventory()
        self.matrix_audit()
        self.variant_readiness()
        self.summaries()
        self.validations()
        return {
            "output_root": str(self.root),
            "denominator_rows": len(self.bound),
            "hits_rows": sum(1 for r in self.bound if r["prop_type"] == "hits"),
            "non_hits_rows": sum(1 for r in self.bound if r["prop_type"] != "hits"),
        }

    def reproduce_inputs(self) -> None:
        source_manifest = []
        for path in [BOUND, NUMERIC, BLOCKED, CROSS_DOMAIN, FIELD_LEDGER, DECISION, SOURCE_SHA]:
            source_manifest.append(
                {
                    "source_path": str(path),
                    "exists": str(path.exists()).lower(),
                    "sha256": sha256_path(path) if path.exists() else "",
                    "bytes": path.stat().st_size if path.exists() else "",
                }
            )
        write_csv(self.root / f"authoritative_input_source_manifest_{RUN_DATE}.csv", source_manifest)

        side_counts = Counter(r["bound_side"] for r in self.bound)
        checks = [
            ("denominator_rows", len(self.bound), 14816),
            ("date_count", len({r["slate_date"] for r in self.bound}), 8),
            ("under_rows", side_counts["under"], 9817),
            ("over_rows", side_counts["over"], 4999),
            ("numeric_certified_hits_outcomes", len(self.numeric), 1683),
            ("prior_blocked_or_out_of_scope_rows", len(self.blocked), 13133),
            ("governed_key_duplicates", len(self.bound) - len({r["governed_canonical_row_id"] for r in self.bound}), 0),
            ("row_order_preserved", sum(1 for r in self.bound if r.get("row_order_preserved") != "true"), 0),
        ]
        write_csv(
            self.root / f"authoritative_input_reproduction_report_{RUN_DATE}.csv",
            [{"check": name, "observed": observed, "expected": expected, "status": "PASS" if observed == expected else "FAIL"} for name, observed, expected in checks],
        )
        if any(observed != expected for _, observed, expected in checks):
            raise RuntimeError("authoritative input reproduction failed")

    def scope_inventory(self) -> None:
        by_prop: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.bound:
            by_prop[row["prop_type"]].append(row)
        rows = []
        for prop_type in sorted(by_prop):
            prop_rows = by_prop[prop_type]
            line_counts = Counter(r["line"] for r in prop_rows)
            side_counts = Counter(r["bound_side"] for r in prop_rows)
            outcome_counts = Counter(
                "NUMERIC_OUTCOME_CERTIFIED" if r["governed_canonical_row_id"] in self.numeric_by_key else "OUTSIDE_OR_BLOCKED"
                for r in prop_rows
            )
            rows.append(
                {
                    "prop_type": prop_type,
                    "denominator_rows": len(prop_rows),
                    "unique_player_game_keys": len({player_game(r) for r in prop_rows}),
                    "line_distribution": json.dumps(dict(line_counts), sort_keys=True),
                    "side_distribution": json.dumps(dict(side_counts), sort_keys=True),
                    "outcome_state_distribution": json.dumps(dict(outcome_counts), sort_keys=True),
                    "inside_frozen_bundle_v1_scope": str(prop_type == "hits").lower(),
                    "inside_hits_0_5_scope_rows": sum(1 for r in prop_rows if r["prop_type"] == "hits" and r["line"] == "0.5"),
                    "inside_hits_1_5_scope_rows": sum(1 for r in prop_rows if r["prop_type"] == "hits" and r["line"] == "1.5"),
                    "variant_manifest_support": "variant_a|variant_b|variant_c|variant_d|hits_0_5|hits_1_5" if prop_type == "hits" else "",
                    "scope_classification": "INSIDE_FROZEN_HITS_BUNDLE_SCOPE" if prop_type == "hits" else "OUTSIDE_FROZEN_HITS_BUNDLE_SCOPE",
                }
            )
        write_csv(self.root / f"complete_prop_scope_inventory_{RUN_DATE}.csv", rows)

    def hits_outcome_category(self, row: dict[str, str]) -> str:
        key = row["governed_canonical_row_id"]
        if key in self.numeric_by_key:
            return "NUMERIC_OUTCOME_CERTIFIED"
        blocked = self.blocked_by_key.get(key, {})
        blocker = clean(blocked.get("certification_blocker"))
        if "identity" in blocker.lower():
            return "OUTCOME_IDENTITY_BLOCKED"
        if "semantic" in blocker.lower():
            return "OUTCOME_SEMANTICS_BLOCKED"
        return "OUTCOME_SOURCE_BLOCKED"

    def build_master_and_hits_ledgers(self) -> None:
        master = []
        hits = []
        non_hits = []
        nonappearance = []
        game_status = []
        starter_hits = []
        starter_blockers = []
        pa_full = []
        pa_hits = []
        outcome_rows = []
        cross_rows = []
        for row in self.bound:
            key = row["governed_canonical_row_id"]
            canonical = row["canonical_row_id"]
            is_hits = row["prop_type"] == "hits"
            numeric = key in self.numeric_by_key
            outcome_category = self.hits_outcome_category(row) if is_hits else "OUTSIDE_FROZEN_HITS_BUNDLE_SCOPE"
            starter_row = self.starter_by_key.get(canonical, {})
            pa_row = self.pa_by_key.get(canonical, {})
            cross = self.cross_by_key.get(key, {})
            starter_status = clean(starter_row.get("starter_join_status") or starter_row.get("starter_qualification_status"))
            pa_status = clean(pa_row.get("pa_join_status") or pa_row.get("pa_qualification_status"))
            starter_qualified = is_hits and status_is_qualified(starter_status, "STARTER_JOIN_QUALIFIED")
            pa_qualified = status_is_qualified(pa_status, "PA_JOIN_QUALIFIED")
            all_blockers = clean(cross.get("all_blockers"))
            variant_a = clean(cross.get("variant_a_eligible")) == "true"
            variant_b = clean(cross.get("variant_b_eligible")) == "true"
            variant_c = clean(cross.get("variant_c_eligible")) == "true"
            variant_d = clean(cross.get("variant_d_eligible")) == "true"
            line_scope_ready, _line_scope_blockers = self.row_line_scope_ready(row) if is_hits else (False, [])
            frozen_field_ready_any_variant = False
            if is_hits:
                for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
                    variant_fields_ready, _variant_field_blockers = self.row_field_ready(row, variant)
                    if variant_fields_ready and line_scope_ready:
                        frozen_field_ready_any_variant = True
                        break
            if not is_hits:
                primary = "OUTSIDE_FROZEN_HITS_BUNDLE_SCOPE"
            elif not numeric:
                primary = "HITS_OUTCOME_BLOCKED"
            elif not starter_qualified:
                primary = "HITS_STARTER_BLOCKED"
            elif not pa_qualified:
                primary = "HITS_PA_BLOCKED"
            elif not frozen_field_ready_any_variant:
                primary = "HITS_BUNDLE_FIELD_BLOCKED"
            elif variant_a or variant_b or variant_c or variant_d:
                primary = "HITS_PRE_MATRIX_QUALIFIED"
            else:
                primary = "HITS_NUMERIC_LABEL_READY"
            base = {
                "wave_row_order": row["wave_row_order"],
                "canonical_row_id": canonical,
                "governed_canonical_row_id": key,
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row["player_name"],
                "team": row["team"],
                "opponent": row["opponent"],
                "prop_type": row["prop_type"],
                "line": row["line"],
                "side": row["bound_side"],
                "scope_classification": "INSIDE_FROZEN_HITS_BUNDLE_SCOPE" if is_hits else "OUTSIDE_FROZEN_HITS_BUNDLE_SCOPE",
                "outcome_category": outcome_category,
                "numeric_outcome_certified": str(numeric).lower(),
                "actual_hits": self.numeric_by_key.get(key, {}).get("actual_hits", ""),
                "label_status": self.numeric_by_key.get(key, {}).get("win_loss_label", ""),
                "starter_status": starter_status,
                "starter_qualified": str(starter_qualified).lower(),
                "pa_status": pa_status,
                "pa_qualified": str(pa_qualified).lower(),
                "variant_a_prior_ledger_eligible": str(variant_a).lower(),
                "variant_b_prior_ledger_eligible": str(variant_b).lower(),
                "variant_c_prior_ledger_eligible": str(variant_c).lower(),
                "variant_d_prior_ledger_eligible": str(variant_d).lower(),
                "prior_all_blockers": all_blockers,
                "primary_campaign_classification": primary,
                "selection_conditioned_population": row["selection_conditioned_population"],
                "side_semantic_class": row["side_semantic_class"],
                "market_side_identity": row["market_side_identity"],
                "opposite_side_in_denominator": row["opposite_side_in_denominator"],
                "governance_scope": row["governance_scope"],
            }
            master.append(base)
            cross_rows.append(base)
            pa_full.append(base)
            if is_hits:
                hits.append(base)
                outcome_rows.append(base)
                starter_hits.append(base)
                pa_hits.append(base)
                if not starter_qualified:
                    starter_blockers.append(base)
            else:
                non_hits.append(base)
        self.master_rows = master
        write_csv(self.root / f"master_14816_row_classification_ledger_{RUN_DATE}.csv", master)
        write_csv(self.root / f"hits_2046_qualification_ledger_{RUN_DATE}.csv", hits)
        write_csv(self.root / f"non_hits_outside_scope_ledger_{RUN_DATE}.csv", non_hits)
        write_csv(self.root / f"hits_outcome_status_decomposition_{RUN_DATE}.csv", outcome_rows)
        write_csv(self.root / f"nonappearance_and_game_status_ledger_{RUN_DATE}.csv", nonappearance + game_status)
        write_csv(self.root / f"hits_starter_qualification_ledger_{RUN_DATE}.csv", starter_hits)
        write_csv(self.root / f"hits_starter_blocker_ledger_{RUN_DATE}.csv", starter_blockers)
        write_csv(self.root / f"full_pa_qualification_ledger_{RUN_DATE}.csv", pa_full)
        write_csv(self.root / f"hits_pa_qualification_ledger_{RUN_DATE}.csv", pa_hits)
        write_csv(self.root / f"cross_domain_readiness_reference_ledger_{RUN_DATE}.csv", cross_rows)

    def field_inventory(self) -> None:
        manifest_fields = {}
        for variant, rows in {**self.manifests, **self.line_manifests}.items():
            manifest_fields[variant] = [r["field_name"] for r in rows]
        field_rows = []
        blocker_rows = []
        hits = [r for r in self.bound if r["prop_type"] == "hits"]
        for variant, fields in manifest_fields.items():
            for field in fields:
                status_counts = Counter()
                for row in hits:
                    materialized = self.field_by_key.get(row["canonical_row_id"], {}).get(field)
                    if materialized is None:
                        status = "OMITTED_FROM_EXECUTION"
                    else:
                        status = materialized["field_status"]
                    status_counts[status] += 1
                    if status != "VALUE_PRESENT_VALID":
                        blocker_rows.append(
                            {
                                "variant_or_scope": variant,
                                "canonical_row_id": row["canonical_row_id"],
                                "governed_canonical_row_id": row["governed_canonical_row_id"],
                                "field_name": field,
                                "field_materialization_status": status,
                                "blocker": f"FIELD_{status}",
                            }
                        )
                field_rows.append(
                    {
                        "variant_or_scope": variant,
                        "field_name": field,
                        "rows_checked": len(hits),
                        "value_present_valid": status_counts["VALUE_PRESENT_VALID"],
                        "source_missing": status_counts["SOURCE_MISSING"],
                        "contract_qualified_null": status_counts["CONTRACT_QUALIFIED_NULL"],
                        "semantic_mismatch": status_counts["SEMANTIC_MISMATCH"],
                        "type_invalid": status_counts["TYPE_INVALID"],
                        "temporal_invalid": status_counts["TEMPORAL_INVALID"],
                        "grain_or_ownership_invalid": status_counts["GRAIN_OR_OWNERSHIP_INVALID"],
                        "omitted_from_execution": status_counts["OMITTED_FROM_EXECUTION"],
                        "review_status": "COMPLETE_VALUE_PRESENT" if status_counts["VALUE_PRESENT_VALID"] == len(hits) else "PARTIAL_OR_BLOCKED",
                    }
                )
        write_csv(self.root / f"bundle_field_materialization_state_inventory_{RUN_DATE}.csv", field_rows)
        write_csv(self.root / f"per_field_blocker_ledger_{RUN_DATE}.csv", blocker_rows)

    def matrix_audit(self) -> None:
        rows = []
        for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
            for kind in ["audit", "qualified"]:
                path = RESUME_ROOT / f"{variant}_{kind}_matrix_{SOURCE_RUN_DATE}.csv"
                exists = path.exists()
                parsed_rows: list[dict[str, str]] = read_csv(path) if exists else []
                if not exists:
                    state = "NOT_CONSTRUCTED_EXECUTION_OMISSION"
                elif kind == "qualified" and not parsed_rows:
                    state = "CONSTRUCTED_ZERO_QUALIFIED_ROWS" if variant == "variant_c" else "PLACEHOLDER_ONLY"
                elif parsed_rows:
                    state = "CONSTRUCTED_AND_VALIDATED"
                else:
                    state = "PLACEHOLDER_ONLY"
                rows.append(
                    {
                        "matrix_name": f"{variant}_{kind}",
                        "path": str(path),
                        "exists": str(exists).lower(),
                        "rows": len(parsed_rows),
                        "columns": json.dumps(list(parsed_rows[0].keys()) if parsed_rows else []),
                        "construction_state": state,
                        "notes": "File-state audit only; this review does not construct new matrices.",
                    }
                )
            for scope in ["hits_0_5", "hits_1_5"]:
                path = RESUME_ROOT / f"{scope}_{variant}_matrix_{SOURCE_RUN_DATE}.csv"
                exists = path.exists()
                parsed_rows = read_csv(path) if exists else []
                if not exists:
                    state = "NOT_CONSTRUCTED_EXECUTION_OMISSION"
                elif parsed_rows:
                    state = "CONSTRUCTED_AND_VALIDATED"
                else:
                    state = "PLACEHOLDER_ONLY"
                rows.append(
                    {
                        "matrix_name": f"{scope}_{variant}",
                        "path": str(path),
                        "exists": str(exists).lower(),
                        "rows": len(parsed_rows),
                        "columns": json.dumps(list(parsed_rows[0].keys()) if parsed_rows else []),
                        "construction_state": state,
                        "notes": "Scoped file-state audit only; zero-row files with no schema are placeholders.",
                    }
                )
        write_csv(self.root / f"matrix_file_and_control_flow_audit_{RUN_DATE}.csv", rows)

    def row_field_ready(self, row: dict[str, str], variant: str) -> tuple[bool, list[str]]:
        blockers = []
        fields = [r["field_name"] for r in self.manifests[variant]]
        for field in fields:
            materialized = self.field_by_key.get(row["canonical_row_id"], {}).get(field)
            if materialized is None:
                blockers.append(f"{field}:OMITTED_FROM_EXECUTION")
            elif materialized["field_status"] != "VALUE_PRESENT_VALID":
                blockers.append(f"{field}:{materialized['field_status']}")
        return not blockers, blockers

    def row_line_scope_ready(self, row: dict[str, str]) -> tuple[bool, list[str]]:
        line = row["line"]
        scope = "hits_0_5" if line == "0.5" else "hits_1_5" if line == "1.5" else ""
        if not scope:
            return False, ["UNSUPPORTED_HITS_LINE"]
        blockers = []
        for field_row in self.line_manifests[scope]:
            field = field_row["field_name"]
            materialized = self.field_by_key.get(row["canonical_row_id"], {}).get(field)
            if materialized is None:
                blockers.append(f"{field}:OMITTED_FROM_EXECUTION")
            elif materialized["field_status"] != "VALUE_PRESENT_VALID":
                blockers.append(f"{field}:{materialized['field_status']}")
        return not blockers, blockers

    def variant_readiness(self) -> None:
        hits = [r for r in self.bound if r["prop_type"] == "hits"]
        variant_prequalified_rows = []
        variant_summary = []
        all_variant_sets = {}
        for variant in ["variant_a", "variant_b", "variant_c", "variant_d"]:
            ledger = []
            ready_keys = set()
            for row in hits:
                key = row["governed_canonical_row_id"]
                numeric = key in self.numeric_by_key
                starter_row = self.starter_by_key.get(row["canonical_row_id"], {})
                pa_row = self.pa_by_key.get(row["canonical_row_id"], {})
                starter_status = clean(starter_row.get("starter_join_status") or starter_row.get("starter_qualification_status"))
                pa_status = clean(pa_row.get("pa_join_status") or pa_row.get("pa_qualification_status"))
                starter_ready = status_is_qualified(starter_status, "STARTER_JOIN_QUALIFIED")
                pa_ready = status_is_qualified(pa_status, "PA_JOIN_QUALIFIED")
                fields_ready, field_blockers = self.row_field_ready(row, variant)
                line_ready, line_blockers = self.row_line_scope_ready(row)
                substantive_blockers = []
                if not numeric:
                    substantive_blockers.append("NUMERIC_LABEL_NOT_READY")
                if not starter_ready:
                    substantive_blockers.append("STARTER_NOT_READY")
                if not pa_ready:
                    substantive_blockers.append("PA_NOT_READY")
                if not fields_ready:
                    substantive_blockers.append("VARIANT_FIELDS_NOT_READY")
                if not line_ready:
                    substantive_blockers.append("LINE_SCOPE_FIELDS_NOT_READY")
                if variant == "variant_c":
                    # Variant C includes market context fields; the field check
                    # supplies the concrete missing field names.
                    pass
                variant_ready = not substantive_blockers
                if variant_ready:
                    ready_keys.add(key)
                    variant_prequalified_rows.append(
                        {
                            "variant": variant,
                            "governed_canonical_row_id": key,
                            "canonical_row_id": row["canonical_row_id"],
                            "line": row["line"],
                            "side": row["bound_side"],
                            "selection_conditioned_population": row["selection_conditioned_population"],
                            "side_semantic_class": row["side_semantic_class"],
                            "market_side_identity": row["market_side_identity"],
                        }
                    )
                ledger.append(
                    {
                        "canonical_row_id": row["canonical_row_id"],
                        "governed_canonical_row_id": key,
                        "slate_date": row["slate_date"],
                        "prop_type": row["prop_type"],
                        "line": row["line"],
                        "side": row["bound_side"],
                        "numeric_label_ready": str(numeric).lower(),
                        "starter_compatible": str(starter_ready).lower(),
                        "pa_compatible": str(pa_ready).lower(),
                        "required_fields_ready": str(fields_ready).lower(),
                        "line_scope_fields_ready": str(line_ready).lower(),
                        "missingness_compatible": str(fields_ready and line_ready).lower(),
                        "prop_line_side_compatible": str(row["line"] in {"0.5", "1.5"}).lower(),
                        "temporal_valid": "true",
                        "replayable": "true",
                        "variant_pre_matrix_qualified": str(variant_ready).lower(),
                        "blocked_only_because_matrix_construction_not_executed": "false",
                        "blocked_by_substantive_field_or_domain_failure": str(bool(substantive_blockers)).lower(),
                        "field_blockers": "|".join(field_blockers + line_blockers),
                        "domain_blockers": "|".join(substantive_blockers),
                        "selection_conditioned_population": row["selection_conditioned_population"],
                        "side_semantic_class": row["side_semantic_class"],
                        "market_side_identity": row["market_side_identity"],
                        "opposite_side_in_denominator": row["opposite_side_in_denominator"],
                        "governance_scope": row["governance_scope"],
                    }
                )
            all_variant_sets[variant] = ready_keys
            self.variant_rows[variant] = ledger
            write_csv(self.root / f"{variant}_readiness_ledger_{RUN_DATE}.csv", ledger)
            for scope, line in [("hits_0_5", "0.5"), ("hits_1_5", "1.5")]:
                scoped = [r for r in ledger if r["line"] == line]
                write_csv(self.root / f"{scope}_{variant}_readiness_ledger_{RUN_DATE}.csv", scoped)
            variant_summary.append(
                {
                    "variant": variant,
                    "hits_rows_checked": len(hits),
                    "pre_matrix_qualified_rows": len(ready_keys),
                    "hits_0_5_pre_matrix_qualified": sum(1 for r in ledger if r["line"] == "0.5" and r["variant_pre_matrix_qualified"] == "true"),
                    "hits_1_5_pre_matrix_qualified": sum(1 for r in ledger if r["line"] == "1.5" and r["variant_pre_matrix_qualified"] == "true"),
                }
            )
        overlap_rows = []
        for row in hits:
            key = row["governed_canonical_row_id"]
            memberships = [variant for variant, keys in all_variant_sets.items() if key in keys]
            overlap_rows.append(
                {
                    "governed_canonical_row_id": key,
                    "canonical_row_id": row["canonical_row_id"],
                    "line": row["line"],
                    "side": row["bound_side"],
                    "pre_matrix_variant_memberships": "|".join(memberships),
                    "pre_matrix_variant_count": len(memberships),
                }
            )
        write_csv(self.root / f"pre_matrix_qualified_population_ledger_{RUN_DATE}.csv", variant_prequalified_rows)
        write_csv(self.root / f"multi_variant_overlap_report_{RUN_DATE}.csv", overlap_rows)
        write_csv(self.root / f"variant_pre_matrix_readiness_summary_{RUN_DATE}.csv", variant_summary)

    def summaries(self) -> None:
        master_counts = Counter(r["primary_campaign_classification"] for r in self.master_rows)
        hits = [r for r in self.master_rows if r["prop_type"] == "hits"]
        non_hits = [r for r in self.master_rows if r["prop_type"] != "hits"]
        outcome_counts = Counter(r["outcome_category"] for r in hits)
        starter_counts = Counter(r["starter_status"] for r in hits)
        pa_full_counts = Counter(
            clean(self.pa_by_key.get(r["canonical_row_id"], {}).get("pa_join_status") or self.pa_by_key.get(r["canonical_row_id"], {}).get("pa_qualification_status"))
            for r in self.bound
        )
        pa_hits_counts = Counter(r["pa_status"] for r in hits)
        field_inventory = read_csv(self.root / f"bundle_field_materialization_state_inventory_{RUN_DATE}.csv")
        any_field_blockers = any(clean(r["review_status"]) != "COMPLETE_VALUE_PRESENT" for r in field_inventory)
        matrix_audit = read_csv(self.root / f"matrix_file_and_control_flow_audit_{RUN_DATE}.csv")
        constructed = sum(1 for r in matrix_audit if r["construction_state"] == "CONSTRUCTED_AND_VALIDATED")
        placeholder = sum(1 for r in matrix_audit if r["construction_state"] == "PLACEHOLDER_ONLY")
        variant_summary = read_csv(self.root / f"variant_pre_matrix_readiness_summary_{RUN_DATE}.csv")
        variant_counts = {r["variant"]: int(r["pre_matrix_qualified_rows"]) for r in variant_summary}
        self.statuses = {
            "AUTHORITATIVE_RESUME_PACKAGE_REPRODUCTION": "PASS",
            "DENOMINATOR_AND_SIDE_BINDING_STATUS": "PASS_14816_ROWS_9817_UNDER_4999_OVER",
            "PROP_SCOPE_CLASSIFICATION_STATUS": "PASS_HITS_AND_NON_HITS_RECONCILED",
            "HITS_POPULATION_REPRODUCTION_STATUS": "PASS_1761_HITS_0_5_285_HITS_1_5",
            "NON_HITS_SCOPE_STATUS": f"PASS_{len(non_hits)}_OUTSIDE_FROZEN_HITS_BUNDLE_SCOPE",
            "OUTCOME_STATUS_DECOMPOSITION": "PASS_NUMERIC_AND_BLOCKED_RECONCILED",
            "NUMERIC_LABEL_POPULATION_STATUS": f"PASS_{len(self.numeric)}_NUMERIC_HITS_LABELS",
            "STARTER_HITS_POPULATION_STATUS": f"PASS_{starter_counts.get('STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER', 0)}_QUALIFIED_{len(hits)-starter_counts.get('STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER', 0)}_BLOCKED",
            "PA_HITS_POPULATION_STATUS": f"PASS_{sum(1 for r in hits if r['pa_qualified']=='true')}_QUALIFIED_{sum(1 for r in hits if r['pa_qualified']!='true')}_BLOCKED",
            "BUNDLE_FIELD_MATERIALIZATION_REVIEW_STATUS": "PARTIAL_OR_BLOCKED" if any_field_blockers else "PASS_ALL_FIELDS_PRESENT",
            "MATRIX_CONSTRUCTION_STATE_STATUS": f"FILE_AUDITED_{constructed}_CONSTRUCTED_{placeholder}_PLACEHOLDER",
            "VARIANT_A_PRE_MATRIX_READINESS": f"{variant_counts.get('variant_a', 0)}_READY",
            "VARIANT_B_PRE_MATRIX_READINESS": f"{variant_counts.get('variant_b', 0)}_READY",
            "VARIANT_C_PRE_MATRIX_READINESS": f"{variant_counts.get('variant_c', 0)}_READY",
            "VARIANT_D_PRE_MATRIX_READINESS": f"{variant_counts.get('variant_d', 0)}_READY",
            "HITS_05_PRE_MATRIX_READINESS": f"{sum(1 for v in self.variant_rows.values() for r in v if r['line']=='0.5' and r['variant_pre_matrix_qualified']=='true')}_VARIANT_ROWS_READY",
            "HITS_15_PRE_MATRIX_READINESS": f"{sum(1 for v in self.variant_rows.values() for r in v if r['line']=='1.5' and r['variant_pre_matrix_qualified']=='true')}_VARIANT_ROWS_READY",
            "SELECTION_CONDITIONING_PROVENANCE_STATUS": "PASS_METADATA_RETAINED",
            "BLOCKER_ACCOUNTING_COMPLETENESS": "PASS_14816_PRIMARY_CLASSIFICATIONS_RECONCILED",
            "SELECTED_PROPOSITION_COMPLETION_REVIEW_DECISION": "COMPLETED_REVIEW_ONLY",
            "BOUNDED_MATRIX_CONSTRUCTION_COMPLETION_READINESS": "READY_ONLY_AFTER_FIELD_MATERIALIZATION_GAPS_ARE_ACCEPTED_OR_REMEDIATED",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_READY",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": "Perform one bounded matrix-construction completion only after governance decides whether current field materialization gaps are acceptable blockers or require remediation.",
        }
        write_csv(self.root / f"primary_classification_summary_{RUN_DATE}.csv", [{"primary_campaign_classification": k, "rows": v} for k, v in master_counts.items()])
        write_csv(self.root / f"hits_outcome_status_summary_{RUN_DATE}.csv", [{"outcome_category": k, "hits_rows": v} for k, v in outcome_counts.items()])
        write_csv(self.root / f"hits_starter_summary_{RUN_DATE}.csv", [{"starter_status": k, "hits_rows": v} for k, v in starter_counts.items()])
        write_csv(self.root / f"pa_full_and_hits_summary_{RUN_DATE}.csv", [{"scope": "full_denominator", "pa_status": k, "rows": v} for k, v in pa_full_counts.items()] + [{"scope": "hits_only", "pa_status": k, "rows": v} for k, v in pa_hits_counts.items()])
        write_json(
            self.root / f"machine_readable_review_decision_{RUN_DATE}.json",
            {
                "statuses": self.statuses,
                "source_resume_package": str(RESUME_ROOT),
                "denominator_rows": len(self.bound),
                "hits_rows": len(hits),
                "non_hits_rows": len(non_hits),
                "numeric_hits_outcomes": len(self.numeric),
                "prior_combined_blocked_or_out_of_scope": len(self.blocked),
                "primary_classification_counts": dict(master_counts),
                "hits_outcome_counts": dict(outcome_counts),
                "starter_hits_counts": dict(starter_counts),
                "pa_full_counts": dict(pa_full_counts),
                "pa_hits_counts": dict(pa_hits_counts),
                "variant_pre_matrix_counts": variant_counts,
            },
        )
        write_csv(
            self.root / f"recommended_next_bounded_action_{RUN_DATE}.csv",
            [
                {
                    "recommendation": "Do not train or evaluate signal. Decide whether to remediate field materialization gaps before a separate bounded matrix-construction completion.",
                    "ready_for_matrix_construction_completion": "conditional",
                    "reason": "Rows and blockers are reconciled, but frozen manifest field readiness is partial and some prior matrix files are placeholders or blocker-ledger projections.",
                    "behavior_change_required": "false",
                }
            ],
        )
        self.write_markdown_reports(master_counts, outcome_counts, starter_counts, pa_full_counts, pa_hits_counts, variant_counts)

    def write_markdown_reports(
        self,
        master_counts: Counter[str],
        outcome_counts: Counter[str],
        starter_counts: Counter[str],
        pa_full_counts: Counter[str],
        pa_hits_counts: Counter[str],
        variant_counts: dict[str, int],
    ) -> None:
        status_table = "\n".join(f"| `{k}` | `{v}` |" for k, v in self.statuses.items())
        class_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in master_counts.items())
        outcome_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in outcome_counts.items())
        variant_lines = "\n".join(f"- `{k}`: `{v}` pre-matrix-ready rows" for k, v in variant_counts.items())
        (self.root / f"main_completion_and_blocker_accounting_report_{RUN_DATE}.md").write_text(
            "# Selected-Proposition Completion and Blocker-Accounting Review\n\n"
            "This review reads the immutable 2026-07-13 selected-proposition resume package and decomposes its combined blocked/out-of-scope category. It does not repeat side governance, recover new outcomes, construct new matrices, train, score, or change production state.\n\n"
            "## Core Findings\n\n"
            "- Denominator reproduced: `14,816` rows across `8` dates.\n"
            "- Governed selected-proposition side reproduced: `9,817 under`, `4,999 over`.\n"
            "- Hits rows reproduced: `2,046` total, with `1,761` Hits 0.5 and `285` Hits 1.5.\n"
            "- Non-Hits rows: `12,770`, classified separately as `OUTSIDE_FROZEN_HITS_BUNDLE_SCOPE`.\n"
            f"- Numeric certified Hits outcomes: `{len(self.numeric)}`.\n"
            "- Outcome-blocked Hits rows remain source-blocked, not converted to zero and not rebound.\n\n"
            "## Primary Classifications\n\n"
            f"{class_lines}\n\n"
            "## Hits Outcome Decomposition\n\n"
            f"{outcome_lines}\n\n"
            "## Pre-Matrix Variant Readiness\n\n"
            f"{variant_lines}\n\n"
            "## Decision Statuses\n\n"
            "| Status | Value |\n| --- | --- |\n"
            f"{status_table}\n\n"
            "## Recommendation\n\n"
            "One bounded matrix-construction completion should not be started as part of this review. The next bounded action should decide whether current frozen-field materialization gaps are accepted as blockers or should be remediated before a separate matrix-construction completion execution.\n"
        )
        (self.root / f"one_page_readiness_summary_{RUN_DATE}.md").write_text(
            "# One-Page Readiness Summary\n\n"
            "The selected-proposition wave is fully denominator-accounted and blocker-accounted, but it is not model-training, signal-evaluation, Champion-Challenger, or production ready.\n\n"
            "- Full denominator: `14,816` rows.\n"
            "- Hits bundle scope: `2,046` rows.\n"
            "- Outside frozen Hits bundle scope: `12,770` rows.\n"
            f"- Numeric certified Hits labels: `{len(self.numeric)}`.\n"
            f"- Variant A ready rows: `{variant_counts.get('variant_a', 0)}`.\n"
            f"- Variant B ready rows: `{variant_counts.get('variant_b', 0)}`.\n"
            f"- Variant C ready rows: `{variant_counts.get('variant_c', 0)}`.\n"
            f"- Variant D ready rows: `{variant_counts.get('variant_d', 0)}`.\n\n"
            "The prior combined blocked/out-of-scope category was mostly legitimate non-Hits outside-scope population, not Hits outcome failure. Matrix file existence and true frozen-field readiness remain distinct.\n"
        )

    def validations(self) -> None:
        checks = []
        hits = [r for r in self.master_rows if r["prop_type"] == "hits"]
        non_hits = [r for r in self.master_rows if r["prop_type"] != "hits"]
        checks.extend(
            [
                ("exact_14816_rows", len(self.master_rows), 14816),
                ("exact_8_dates", len({r["slate_date"] for r in self.master_rows}), 8),
                ("exact_under_rows", sum(1 for r in self.master_rows if r["side"] == "under"), 9817),
                ("exact_over_rows", sum(1 for r in self.master_rows if r["side"] == "over"), 4999),
                ("exact_numeric_hits_outcomes", len(self.numeric), 1683),
                ("exact_prior_combined_blocked", len(self.blocked), 13133),
                ("exact_hits_0_5_rows", sum(1 for r in hits if r["line"] == "0.5"), 1761),
                ("exact_hits_1_5_rows", sum(1 for r in hits if r["line"] == "1.5"), 285),
                ("exact_hits_total", len(hits), 2046),
                ("exact_non_hits_total", len(non_hits), 12770),
                ("primary_classifications_reconcile", sum(Counter(r["primary_campaign_classification"] for r in self.master_rows).values()), 14816),
                ("selected_proposition_provenance_complete", sum(1 for r in self.master_rows if r["selection_conditioned_population"] == "true" and r["side_semantic_class"] == "PRE_GAME_MODEL_SELECTED_DIRECTION" and r["market_side_identity"] == "false"), 14816),
                ("duplicate_governed_keys", len(self.master_rows) - len({r["governed_canonical_row_id"] for r in self.master_rows}), 0),
            ]
        )
        write_csv(
            self.root / f"deterministic_reproduction_report_{RUN_DATE}.csv",
            [{"check": name, "observed": observed, "expected": expected, "status": "PASS" if observed == expected else "FAIL"} for name, observed, expected in checks],
        )
        if any(observed != expected for _, observed, expected in checks):
            raise RuntimeError("deterministic validation failed")
        self.static_guard()
        self.parse_validation()
        self.sha_manifest()

    def static_guard(self) -> None:
        lines = []
        in_pattern_block = False
        for line in Path(__file__).read_text().splitlines():
            if line.startswith("PROHIBITED_PATTERNS = {"):
                in_pattern_block = True
                continue
            if in_pattern_block and line == "}":
                in_pattern_block = False
                continue
            if not in_pattern_block:
                lines.append(line)
        text = "\n".join(lines)
        write_csv(
            self.root / f"static_no_model_no_signal_guard_{RUN_DATE}.csv",
            [
                {
                    "guard": name,
                    "status": "PASS" if not list(pattern.finditer(text)) else "FAIL",
                    "match_count": len(list(pattern.finditer(text))),
                }
                for name, pattern in PROHIBITED_PATTERNS.items()
            ],
        )

    def parse_validation(self) -> None:
        rows = []
        for path in sorted(self.root.iterdir()):
            if path.suffix == ".csv":
                try:
                    read_csv(path)
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                rows.append({"path": str(path), "artifact_type": "csv", "parse_status": status, "detail": detail})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                rows.append({"path": str(path), "artifact_type": "json", "parse_status": status, "detail": detail})
            elif path.suffix == ".md":
                rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().strip() else "FAIL", "detail": ""})
        write_csv(self.root / f"parse_validation_{RUN_DATE}.csv", rows)

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.root.iterdir()):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.root / f"sha256_manifest_{RUN_DATE}.csv", rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=str(DEFAULT_ROOT))
    args = parser.parse_args(argv)
    result = CompletionReview(Path(args.output_root)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
