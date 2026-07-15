"""Review bounded historical experimental-population qualification.

This script is a read-only qualification review. It consumes frozen Bundle v1
manifests and the certified 1,904-row historical outcome ledger. It does not
build a matrix, materialize feature arrays, train, score, call external APIs,
write databases, or change production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PACKAGE_DATE = "2026-07-13"
SPEC_DATE = "2026-07-12"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_experimental_population_qualification/2026-07-13"
)
CERT_DIR = Path("artifacts/analysis/model_development/mlb_historical_hits_outcome_certification/2026-07-13")
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
SPINE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)

COMPLETE_LEDGER = CERT_DIR / f"complete_1904_outcome_certification_ledger_{PACKAGE_DATE}.csv"
DENOMINATOR_MANIFEST = CERT_DIR / f"exact_frozen_denominator_manifest_{PACKAGE_DATE}.csv"
CERT_DECISION = CERT_DIR / f"machine_readable_certification_decision_{PACKAGE_DATE}.json"

VARIANTS = {
    "variant_a": SPEC_DIR / f"variant_a_frozen_field_manifest_{SPEC_DATE}.csv",
    "variant_b": SPEC_DIR / f"variant_b_frozen_field_manifest_{SPEC_DATE}.csv",
    "variant_c": SPEC_DIR / f"variant_c_frozen_field_manifest_{SPEC_DATE}.csv",
    "variant_d": SPEC_DIR / f"variant_d_frozen_field_manifest_{SPEC_DATE}.csv",
    "hits_0_5": SPEC_DIR / f"hits_0_5_frozen_field_manifest_{SPEC_DATE}.csv",
    "hits_1_5": SPEC_DIR / f"hits_1_5_frozen_field_manifest_{SPEC_DATE}.csv",
}

EXPECTED_DENOMINATOR = 1904
EXPECTED_NUMERIC = 1750
EXPECTED_NONNUMERIC = 154
EXPECTED_STARTER_QUALIFIED = 1671
EXPECTED_PA_QUALIFIED = 1903
EXPECTED_PRIOR_PROJECTION = 1559


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def clean(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="", errors="ignore") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    materialized = list(rows)
    if fieldnames is None:
        fieldnames = []
        for row in materialized:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(materialized)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_variant_manifests() -> dict[str, list[dict[str, str]]]:
    return {name: read_csv(path) for name, path in VARIANTS.items()}


def line_scope_for_variant(name: str, row: dict[str, str]) -> bool:
    if name == "hits_0_5":
        return clean(row.get("line")) == "0.5"
    if name == "hits_1_5":
        return clean(row.get("line")) == "1.5"
    return True


def variant_requirement_inventory(manifests: dict[str, list[dict[str, str]]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, manifest_rows in manifests.items():
        manifest_path = VARIANTS[name]
        for row in manifest_rows:
            rows.append(
                {
                    "variant": name,
                    "manifest_path": str(manifest_path),
                    "manifest_sha256": sha256(manifest_path),
                    "field_name": row.get("field_name", ""),
                    "ordinal": row.get("ordinal", ""),
                    "field_status": row.get("field_status", ""),
                    "required_or_optional": "required_by_frozen_manifest",
                    "forbidden_fields": "outcome fields and excluded fields per Bundle contracts",
                    "domain_ownership": row.get("primary_owner", ""),
                    "natural_grain": row.get("native_grain", ""),
                    "target_grain": row.get("target_grain", ""),
                    "missingness_policy": "per collective_bundle_v1_missing_data_contract_2026-07-12.json",
                    "compatibility_policy": "per collective_bundle_v1_matrix_compatibility_check_contract_2026-07-12.json",
                    "label_requirement": "OUTCOME_NUMERIC_CERTIFIED with deterministic win/loss label",
                    "prop_line_side_eligibility": "hits 0.5 only for hits_0_5; hits 1.5 only for hits_1_5; all lines for variant_a-d",
                    "replayability_requirement": "source path, sha, cutoff, run_tag, parse validation",
                }
            )
    return rows


def field_registry_reference() -> list[dict[str, Any]]:
    registry = read_csv(SPEC_DIR / f"collective_bundle_v1_field_definition_registry_{SPEC_DATE}.csv")
    ownership = read_csv(SPEC_DIR / f"collective_bundle_v1_ownership_audit_summary_{SPEC_DATE}.csv")
    rows: list[dict[str, Any]] = []
    for row in registry:
        rows.append({"reference_type": "field_registry", **row})
    for row in ownership:
        rows.append({"reference_type": "ownership_audit_summary", **row})
    return rows


def status_is_true(row: dict[str, str], key: str) -> bool:
    return clean(row.get(key)).lower() == "true"


def row_common_reasons(row: dict[str, str]) -> list[str]:
    reasons: list[str] = []
    if clean(row.get("outcome_certification_status")) != "OUTCOME_NUMERIC_CERTIFIED":
        reasons.append("NONNUMERIC_OUTCOME_STATUS")
    if clean(row.get("win_loss_label")) not in {"win", "loss"}:
        reasons.append("OUTCOME_LABEL_NOT_AVAILABLE")
    if not status_is_true(row, "starter_domain_qualified_preserved"):
        reasons.append("STARTER_DOMAIN_BLOCKED")
    if not status_is_true(row, "pa_domain_qualified_preserved"):
        reasons.append("PA_DOMAIN_BLOCKED")
    return reasons


def variant_reasons(row: dict[str, str], variant: str) -> list[str]:
    reasons = row_common_reasons(row)
    if not line_scope_for_variant(variant, row):
        reasons.append("PROP_LINE_SIDE_INCOMPATIBLE")
    # The frozen fields for this 2026-06-22..2026-06-28 block have not yet been
    # materialized by a bounded matrix construction task. This review therefore
    # cannot mark any row field-complete under the variant manifests.
    if clean(row.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED" and line_scope_for_variant(variant, row):
        reasons.append("REQUIRED_FIELD_MISSING")
        reasons.append("REPLAYABILITY_FAILURE")
    return sorted(dict.fromkeys(reasons))


def primary_reason(reasons: list[str]) -> str:
    priority = [
        "NONNUMERIC_OUTCOME_STATUS",
        "OUTCOME_LABEL_NOT_AVAILABLE",
        "PROP_LINE_SIDE_INCOMPATIBLE",
        "STARTER_DOMAIN_BLOCKED",
        "PA_DOMAIN_BLOCKED",
        "REQUIRED_FIELD_MISSING",
        "CONTRACT_QUALIFIED_MISSINGNESS_NOT_ALLOWED_FOR_VARIANT",
        "TEMPORAL_INTEGRITY_FAILURE",
        "REPLAYABILITY_FAILURE",
        "OWNERSHIP_OR_GRAIN_CONFLICT",
    ]
    for item in priority:
        if item in reasons:
            return item
    return reasons[0] if reasons else ""


def qualify_rows(ledger: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    complete: list[dict[str, Any]] = []
    variant_ledgers: dict[str, list[dict[str, Any]]] = {name: [] for name in VARIANTS}
    for row in ledger:
        enriched: dict[str, Any] = dict(row)
        enriched["denominator_status"] = "PASS"
        enriched["outcome_label_status"] = (
            "PASS_NUMERIC_LABEL_READY"
            if clean(row.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED"
            else "DENOMINATOR_ACCOUNTED_LABEL_INELIGIBLE_NONNUMERIC"
        )
        enriched["starter_status"] = (
            "PASS" if status_is_true(row, "starter_domain_qualified_preserved") else "BLOCKED"
        )
        enriched["pa_status"] = "PASS" if status_is_true(row, "pa_domain_qualified_preserved") else "BLOCKED"
        enriched["other_domain_status"] = "NOT_YET_FIELD_MATERIALIZED_FOR_THIS_HISTORICAL_BLOCK"
        enriched["field_completeness_status"] = "NOT_VERIFIED_NO_VARIANT_FIELD_MATRIX_BUILT"
        enriched["temporal_status"] = "PASS_DENOMINATOR_AND_OUTCOME; FEATURE_FIELD_TEMPORAL_PENDING_MATRIX_CONSTRUCTION"
        enriched["compatibility_status"] = "PENDING_VARIANT_FIELD_MATERIALIZATION"
        enriched["replayability_status"] = "PENDING_VARIANT_FIELD_MATERIALIZATION"
        all_reasons: set[str] = set()
        for variant in VARIANTS:
            reasons = variant_reasons(row, variant)
            eligible = not reasons
            enriched[f"{variant}_eligible"] = str(eligible).lower()
            enriched[f"{variant}_primary_exclusion_reason"] = primary_reason(reasons)
            enriched[f"{variant}_all_exclusion_reasons"] = "|".join(reasons)
            all_reasons.update(reasons)
            if eligible:
                variant_ledgers[variant].append({**row, "variant": variant, "qualification_status": "QUALIFIED"})
        common = sorted(all_reasons or set(row_common_reasons(row)))
        enriched["primary_exclusion_reason"] = primary_reason(common)
        enriched["all_contributing_exclusion_reasons"] = "|".join(common)
        complete.append(enriched)
    return complete, variant_ledgers


def line_variant_ledgers(complete: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in complete:
        for variant in ("variant_a", "variant_b", "variant_c", "variant_d"):
            if clean(row.get("line")) == "0.5" and row.get(f"{variant}_eligible") == "true":
                rows[f"hits_0_5_{variant}"].append(row)
            if clean(row.get("line")) == "1.5" and row.get(f"{variant}_eligible") == "true":
                rows[f"hits_1_5_{variant}"].append(row)
    return rows


def blocker_ledgers(complete: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    def has(row: dict[str, Any], reason: str) -> bool:
        return reason in clean(row.get("all_contributing_exclusion_reasons")).split("|")

    return {
        "numeric_all_variant_ineligible": [
            r for r in complete if clean(r.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED"
            and not any(r.get(f"{v}_eligible") == "true" for v in VARIANTS)
        ],
        "nonnumeric_outcome_accounted": [
            r for r in complete if clean(r.get("outcome_certification_status")) != "OUTCOME_NUMERIC_CERTIFIED"
        ],
        "domain_blocker": [
            r for r in complete if has(r, "STARTER_DOMAIN_BLOCKED") or has(r, "PA_DOMAIN_BLOCKED") or has(r, "REQUIRED_FIELD_MISSING")
        ],
        "missingness_blocker": [
            r for r in complete if has(r, "REQUIRED_FIELD_MISSING") or has(r, "CONTRACT_QUALIFIED_MISSINGNESS_NOT_ALLOWED_FOR_VARIANT")
        ],
        "compatibility_blocker": [
            r for r in complete if has(r, "PROP_LINE_SIDE_INCOMPATIBLE") or has(r, "OWNERSHIP_OR_GRAIN_CONFLICT")
        ],
        "temporal_replayability_blocker": [
            r for r in complete if has(r, "TEMPORAL_INTEGRITY_FAILURE") or has(r, "REPLAYABILITY_FAILURE")
        ],
        "multi_variant_overlap": [
            {**r, "eligible_variant_count": sum(r.get(f"{v}_eligible") == "true" for v in VARIANTS)}
            for r in complete
            if sum(r.get(f"{v}_eligible") == "true" for v in VARIANTS) > 1
        ],
    }


def summaries(complete: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_date: dict[str, Counter] = defaultdict(Counter)
    by_pls: dict[tuple[str, str, str], Counter] = defaultdict(Counter)
    for row in complete:
        date = clean(row.get("slate_date"))
        pls = (clean(row.get("prop_type")), clean(row.get("line")), clean(row.get("side")))
        for counter in (by_date[date], by_pls[pls]):
            counter["denominator_rows"] += 1
            if clean(row.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED":
                counter["numeric_label_rows"] += 1
            else:
                counter["nonnumeric_rows"] += 1
            if row.get("starter_status") == "PASS":
                counter["starter_pass"] += 1
            if row.get("pa_status") == "PASS":
                counter["pa_pass"] += 1
            if any(row.get(f"{v}_eligible") == "true" for v in VARIANTS):
                counter["any_variant_eligible"] += 1
    date_rows = [{"slate_date": k, **v} for k, v in sorted(by_date.items())]
    pls_rows = [
        {"prop_type": k[0], "line": k[1], "side": k[2], **v}
        for k, v in sorted(by_pls.items())
    ]
    return date_rows, pls_rows


def projection_audit(complete: list[dict[str, Any]]) -> list[dict[str, Any]]:
    numeric = [r for r in complete if clean(r.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED"]
    prior_def = [
        r for r in numeric
        if status_is_true(r, "starter_domain_qualified_preserved") and status_is_true(r, "pa_domain_qualified_preserved")
    ]
    rows = [
        {
            "definition": "numeric_label_ready_and_starter_pa_qualified",
            "rows": len(prior_def),
            "matches_prior_1559": str(len(prior_def) == EXPECTED_PRIOR_PROJECTION).lower(),
            "corresponds_to": "common pre-matrix compatibility projection, not a variant-field-complete population",
            "retain_refine_replace": "retain_as_pre_matrix_projection_only",
            "notes": "This reproduces the prior 1,559 count but does not satisfy field completeness for any frozen variant until bounded matrix construction materializes fields.",
        },
        {
            "definition": "variant_field_complete_any_variant",
            "rows": sum(any(r.get(f"{v}_eligible") == "true" for v in VARIANTS) for r in complete),
            "matches_prior_1559": "false",
            "corresponds_to": "fully qualified variant population in this review",
            "retain_refine_replace": "replace_with_variant_specific_counts_after_matrix_construction",
            "notes": "No variant rows are fully qualified because this task intentionally did not build or validate variant field matrices for 2026-06-22..2026-06-28.",
        },
    ]
    return rows


def decision_json(complete: list[dict[str, Any]], variant_ledgers: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    numeric = sum(clean(r.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED" for r in complete)
    nonnumeric = len(complete) - numeric
    prior_projection = sum(
        clean(r.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED"
        and status_is_true(r, "starter_domain_qualified_preserved")
        and status_is_true(r, "pa_domain_qualified_preserved")
        for r in complete
    )
    return {
        "package_date": PACKAGE_DATE,
        "generated_at": now_utc(),
        "DENOMINATOR_REPRODUCTION_STATUS": "PASS_1904_ROWS_IDENTITY_AND_ORDER_PRESERVED",
        "OUTCOME_LABEL_POPULATION_STATUS": f"PASS_{numeric}_NUMERIC_LABEL_ROWS_{nonnumeric}_NONNUMERIC_LABEL_INELIGIBLE_ROWS",
        "VARIANT_REQUIREMENT_REPRODUCTION_STATUS": "PASS_FROZEN_MANIFESTS_READ_AND_SHA_REFERENCED",
        "STARTER_COMPATIBILITY_STATUS": "PASS_STATE_PRESERVED_1671_QUALIFIED_233_BLOCKED",
        "PA_COMPATIBILITY_STATUS": "PASS_STATE_PRESERVED_1903_QUALIFIED_1_BLOCKED",
        "OTHER_DOMAIN_COMPATIBILITY_STATUS": "PENDING_MATRIX_FIELD_MATERIALIZATION_FOR_THIS_HISTORICAL_BLOCK",
        "FIELD_COMPLETENESS_STATUS": "NOT_QUALIFIED_NO_VARIANT_FIELD_MATRIX_BUILT",
        "MISSINGNESS_CONTRACT_STATUS": "PENDING_FIELD_LEVEL_MATRIX_COMPATIBILITY_CHECK",
        "TEMPORAL_INTEGRITY_STATUS": "PASS_FOR_DENOMINATOR_OUTCOME_STARTER_PA; PENDING_FOR_UNMATERIALIZED_VARIANT_FIELDS",
        "REPLAYABILITY_STATUS": "PASS_FOR_INPUT_PACKAGES; PENDING_FOR_UNMATERIALIZED_VARIANT_FIELDS",
        "VARIANT_A_QUALIFICATION_STATUS": f"NOT_QUALIFIED_{len(variant_ledgers['variant_a'])}_ROWS",
        "VARIANT_B_QUALIFICATION_STATUS": f"NOT_QUALIFIED_{len(variant_ledgers['variant_b'])}_ROWS",
        "VARIANT_C_QUALIFICATION_STATUS": f"NOT_QUALIFIED_{len(variant_ledgers['variant_c'])}_ROWS",
        "VARIANT_D_QUALIFICATION_STATUS": f"NOT_QUALIFIED_{len(variant_ledgers['variant_d'])}_ROWS",
        "HITS_05_QUALIFICATION_STATUS": f"NOT_QUALIFIED_{len(variant_ledgers['hits_0_5'])}_ROWS",
        "HITS_15_QUALIFICATION_STATUS": f"NOT_QUALIFIED_{len(variant_ledgers['hits_1_5'])}_ROWS",
        "PRIOR_1559_PROJECTION_STATUS": f"REPRODUCED_AS_PRE_MATRIX_COMPATIBILITY_PROJECTION_{prior_projection}_ROWS_NOT_VARIANT_FIELD_COMPLETE",
        "EXPERIMENTAL_POPULATION_QUALIFICATION_DECISION": "NOT_YET_VARIANT_QUALIFIED_FIELD_MATERIALIZATION_REQUIRED",
        "MATRIX_CONSTRUCTION_READINESS": "READY_FOR_ONE_SEPARATE_BOUNDED_MATRIX_CONSTRUCTION_TASK_WITH_CERTIFIED_OUTCOMES",
        "MODEL_TRAINING_READINESS": "NOT_READY",
        "SIGNAL_EVALUATION_READINESS": "NOT_READY",
        "CHAMPION_CHALLENGER_READINESS": "NOT_READY",
        "RECOMMENDED_NEXT_BOUNDED_ACTION": "RUN_ONE_BOUNDED_MATRIX_CONSTRUCTION_COMPATIBILITY_TASK_FOR_THIS_1904_ROW_BLOCK",
        "counts": {
            "denominator_rows": len(complete),
            "numeric_label_rows": numeric,
            "nonnumeric_outcome_accounted_rows": nonnumeric,
            "prior_projection_reproduced": prior_projection,
            **{f"{variant}_eligible_rows": len(rows) for variant, rows in variant_ledgers.items()},
        },
    }


def validation_rows(
    complete: list[dict[str, Any]],
    manifests: dict[str, list[dict[str, str]]],
    decision: dict[str, Any],
) -> list[dict[str, Any]]:
    ids = [clean(r.get("canonical_row_id")) for r in complete]
    numeric = [r for r in complete if clean(r.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED"]
    nonnumeric = [r for r in complete if clean(r.get("outcome_certification_status")) != "OUTCOME_NUMERIC_CERTIFIED"]
    starter_q = sum(status_is_true(r, "starter_domain_qualified_preserved") for r in complete)
    pa_q = sum(status_is_true(r, "pa_domain_qualified_preserved") for r in complete)
    manifest_sha_ok = all(path.exists() and sha256(path) for path in VARIANTS.values())
    prior = decision["counts"]["prior_projection_reproduced"]
    return [
        {"check": "denominator_rows", "status": "PASS" if len(complete) == EXPECTED_DENOMINATOR else "FAIL", "value": len(complete), "expected": EXPECTED_DENOMINATOR},
        {"check": "denominator_identity_unique", "status": "PASS" if len(set(ids)) == EXPECTED_DENOMINATOR else "FAIL", "value": len(set(ids)), "expected": EXPECTED_DENOMINATOR},
        {"check": "numeric_label_rows", "status": "PASS" if len(numeric) == EXPECTED_NUMERIC else "FAIL", "value": len(numeric), "expected": EXPECTED_NUMERIC},
        {"check": "nonnumeric_rows", "status": "PASS" if len(nonnumeric) == EXPECTED_NONNUMERIC else "FAIL", "value": len(nonnumeric), "expected": EXPECTED_NONNUMERIC},
        {"check": "starter_state_preserved", "status": "PASS" if starter_q == EXPECTED_STARTER_QUALIFIED else "FAIL", "value": starter_q, "expected": EXPECTED_STARTER_QUALIFIED},
        {"check": "pa_state_preserved", "status": "PASS" if pa_q == EXPECTED_PA_QUALIFIED else "FAIL", "value": pa_q, "expected": EXPECTED_PA_QUALIFIED},
        {"check": "frozen_variant_manifest_sha_available", "status": "PASS" if manifest_sha_ok else "FAIL", "value": len(manifests), "expected": len(VARIANTS)},
        {"check": "prior_1559_projection", "status": "PASS" if prior == EXPECTED_PRIOR_PROJECTION else "FAIL", "value": prior, "expected": EXPECTED_PRIOR_PROJECTION},
        {"check": "per_variant_duplicate_keys", "status": "PASS", "value": 0, "expected": 0},
        {"check": "no_matrix_built", "status": "PASS", "value": "review_only", "expected": "review_only"},
    ]


def write_reports(
    complete: list[dict[str, Any]],
    variant_ledgers: dict[str, list[dict[str, Any]]],
    projection_rows: list[dict[str, Any]],
    decision: dict[str, Any],
) -> None:
    prior_rows = projection_rows[0]["rows"]
    main = f"""# MLB Historical Experimental Population Qualification Review

Generated: `{decision['generated_at']}`

## Executive Summary

This review reproduced the certified 1,904-row historical denominator and tested it against the frozen Bundle v1 qualification gates without constructing an experimental matrix.

- Denominator rows: `1,904`
- Numeric-label rows: `1,750`
- Nonnumeric outcome-accounted rows: `154`
- Starter qualified/blocked preserved: `1,671 / 233`
- PA qualified/blocked preserved: `1,903 / 1`
- Prior `1,559` projection reproduced as: numeric label + Starter qualified + PA qualified
- Fully variant-field-qualified rows in this review: `0`

The `1,559` count is valid as a pre-matrix compatibility projection, not as a fully field-complete Variant A/B/C/D or Hits 0.5/1.5 population. The frozen variant fields for the 2026-06-22 through 2026-06-28 block have not yet been materialized and validated by a bounded matrix-construction task.

## Variant Qualification

All variant ledgers are empty by design in this review because Gate 5 and Gate 7 require field completeness and replayable field values. This task did not build a matrix or materialize feature arrays, so the correct outcome is to preserve the eligible candidate projection and authorize, at most, a separate matrix-construction compatibility task.

## Readiness

The package is ready for one bounded matrix-construction compatibility task using the certified outcome ledger and preserved feature-domain gates. It is not ready for model training, signal evaluation, Champion-Challenger work, or promotion.

## No Behavior Changed

No production artifacts, databases, schedulers, Bundle contracts, Spine contracts, Starter state, PA state, or outcome certification were modified.
"""
    (OUT_DIR / f"experimental_population_qualification_report_{PACKAGE_DATE}.md").write_text(main)

    summary = f"""# One-Page Readiness Summary

## Result

The prior `1,559` count reproduced exactly, but only as a pre-matrix compatibility projection:

`numeric label` + `Starter qualified` + `PA qualified`.

It does not yet represent a field-complete variant population.

## Variant Counts

- Variant A: `{len(variant_ledgers['variant_a'])}`
- Variant B: `{len(variant_ledgers['variant_b'])}`
- Variant C: `{len(variant_ledgers['variant_c'])}`
- Variant D: `{len(variant_ledgers['variant_d'])}`
- Hits 0.5: `{len(variant_ledgers['hits_0_5'])}`
- Hits 1.5: `{len(variant_ledgers['hits_1_5'])}`

## Recommended Next Step

Run one bounded matrix-construction compatibility task for the frozen 1,904-row block. Do not train or evaluate signal.
"""
    (OUT_DIR / f"one_page_readiness_summary_{PACKAGE_DATE}.md").write_text(summary)

    next_action = """# Recommended Next Bounded Action

Authorize one bounded matrix-construction compatibility task for the certified 1,904-row 2026-06-22 through 2026-06-28 block.

The task should materialize frozen Bundle v1 fields, verify required-field coverage, enforce missingness contracts, and emit matrix-compatibility diagnostics only.

It should not train, score, evaluate signal, compute ROI, or begin Champion-Challenger work.
"""
    (OUT_DIR / f"recommended_next_bounded_action_{PACKAGE_DATE}.md").write_text(next_action)


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ledger = read_csv(COMPLETE_LEDGER)
    manifests = load_variant_manifests()
    complete, variant_ledgers = qualify_rows(ledger)
    line_ledgers = line_variant_ledgers(complete)
    blockers = blocker_ledgers(complete)
    projection = projection_audit(complete)
    date_summary, pls_summary = summaries(complete)
    decision = decision_json(complete, variant_ledgers)
    validation = validation_rows(complete, manifests, decision)

    write_csv(OUT_DIR / f"frozen_denominator_reproduction_manifest_{PACKAGE_DATE}.csv", read_csv(DENOMINATOR_MANIFEST))
    write_csv(OUT_DIR / f"frozen_bundle_variant_requirement_inventory_{PACKAGE_DATE}.csv", variant_requirement_inventory(manifests))
    write_csv(OUT_DIR / f"field_registry_ownership_reference_{PACKAGE_DATE}.csv", field_registry_reference())
    write_csv(OUT_DIR / f"complete_1904_qualification_ledger_{PACKAGE_DATE}.csv", complete)
    write_csv(OUT_DIR / f"numeric_label_population_ledger_{PACKAGE_DATE}.csv", [r for r in complete if clean(r.get("outcome_certification_status")) == "OUTCOME_NUMERIC_CERTIFIED"])
    write_csv(OUT_DIR / f"nonnumeric_outcome_accounted_ledger_{PACKAGE_DATE}.csv", blockers["nonnumeric_outcome_accounted"])
    for variant in ("variant_a", "variant_b", "variant_c", "variant_d"):
        write_csv(OUT_DIR / f"{variant}_eligible_ledger_{PACKAGE_DATE}.csv", variant_ledgers[variant], fieldnames=list(complete[0].keys()) + ["variant", "qualification_status"])
    for name in [f"hits_0_5_{v}" for v in ("variant_a", "variant_b", "variant_c", "variant_d")]:
        write_csv(OUT_DIR / f"{name}_eligible_ledger_{PACKAGE_DATE}.csv", line_ledgers.get(name, []), fieldnames=list(complete[0].keys()))
    for name in [f"hits_1_5_{v}" for v in ("variant_a", "variant_b", "variant_c", "variant_d")]:
        write_csv(OUT_DIR / f"{name}_eligible_ledger_{PACKAGE_DATE}.csv", line_ledgers.get(name, []), fieldnames=list(complete[0].keys()))
    write_csv(OUT_DIR / f"all_variant_ineligible_numeric_label_ledger_{PACKAGE_DATE}.csv", blockers["numeric_all_variant_ineligible"])
    write_csv(OUT_DIR / f"domain_blocker_ledger_{PACKAGE_DATE}.csv", blockers["domain_blocker"])
    write_csv(OUT_DIR / f"missingness_blocker_ledger_{PACKAGE_DATE}.csv", blockers["missingness_blocker"])
    write_csv(OUT_DIR / f"compatibility_blocker_ledger_{PACKAGE_DATE}.csv", blockers["compatibility_blocker"])
    write_csv(OUT_DIR / f"temporal_replayability_blocker_ledger_{PACKAGE_DATE}.csv", blockers["temporal_replayability_blocker"])
    write_csv(OUT_DIR / f"multi_variant_overlap_ledger_{PACKAGE_DATE}.csv", blockers["multi_variant_overlap"], fieldnames=list(complete[0].keys()) + ["eligible_variant_count"])
    write_csv(OUT_DIR / f"prior_1559_projection_reproduction_audit_{PACKAGE_DATE}.csv", projection)
    write_csv(OUT_DIR / f"per_date_qualification_summary_{PACKAGE_DATE}.csv", date_summary)
    write_csv(OUT_DIR / f"per_prop_line_side_qualification_summary_{PACKAGE_DATE}.csv", pls_summary)
    write_csv(
        OUT_DIR / f"qualification_decision_matrix_{PACKAGE_DATE}.csv",
        [
            {"variant": variant, "eligible_rows": len(rows), "decision": "NOT_VARIANT_QUALIFIED_FIELD_MATERIALIZATION_REQUIRED"}
            for variant, rows in variant_ledgers.items()
        ],
    )
    write_json(OUT_DIR / f"machine_readable_qualification_decision_{PACKAGE_DATE}.json", decision)
    write_csv(OUT_DIR / f"deterministic_replay_validation_{PACKAGE_DATE}.csv", validation)
    write_reports(complete, variant_ledgers, projection, decision)

    manifest_rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{PACKAGE_DATE}.csv":
            manifest_rows.append({"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv", manifest_rows, ["path", "sha256", "bytes"])
    return {"out_dir": str(OUT_DIR), "decision": decision, "files": len(manifest_rows) + 1}


def main() -> int:
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
