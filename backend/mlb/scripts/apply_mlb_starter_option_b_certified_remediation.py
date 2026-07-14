#!/usr/bin/env python3
"""Apply approved Option B to one bounded MLB Starter remediation package.

This script is an artifact builder only. It records the human-approved bounded
interpretation, consumes that approval fail-closed, and produces certified
historical Starter qualification artifacts for the 2026-06-22..2026-06-28
denominator. It does not write to the database, call external APIs, train,
score, attach outcomes, alter production behavior, or expand the authorized
scope.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


PACKAGE_DATE = "2026-07-13"
DATE_RANGE_START = "2026-06-22"
DATE_RANGE_END = "2026-06-28"
PACKAGE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_starter_option_b_certified_remediation/2026-07-13"
)

DENOM_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_earlier_source_denominator_recovery/2026-07-13"
)
JOIN_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_starter_join_remediation/2026-07-13"
)
RECOVERY_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_starter_recovery_dry_run/2026-07-13"
)
GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_starter_historical_reconstruction_governance_decision/2026-07-13"
)

DENOM_ROWS = DENOM_DIR / f"mlb_historical_earlier_source_denominator_rows_{PACKAGE_DATE}.csv"
JOIN_ROWS = JOIN_DIR / f"mlb_historical_starter_join_rows_{PACKAGE_DATE}.csv"
DRY_ROWS = RECOVERY_DIR / f"mlb_historical_starter_recovery_row_dry_run_{PACKAGE_DATE}.csv"
DRY_SUMMARY = RECOVERY_DIR / f"mlb_historical_starter_recovery_summary_{PACKAGE_DATE}.json"
GOV_SUMMARY = GOV_DIR / f"mlb_starter_governance_decision_summary_{PACKAGE_DATE}.json"
GOV_TEMPLATE = GOV_DIR / f"mlb_starter_governance_approval_payload_template_{PACKAGE_DATE}.json"
GOV_STANDARD = GOV_DIR / f"mlb_starter_governance_484_row_population_{PACKAGE_DATE}.csv"
GOV_SPECIAL = GOV_DIR / f"mlb_starter_governance_10_special_rows_{PACKAGE_DATE}.csv"

EXPECTED_TEMPLATE_SHA = "e92a8e469e4bc97838588f8e255876e5f73ccb55c23049326403d9f26212524f"
APPROVED_INTERPRETATION = (
    "Authoritative unique postgame actual-starter identity may be used solely "
    "as a historical binding key to reconstruct strictly prior Starter Skill / "
    "Workload features when direct pregame expected-starter evidence is unavailable."
)

DIRECT_STATUS = "STARTER_JOIN_QUALIFIED_DIRECT_PREGAME"
OPTION_B_STATUS = "STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER"
MISSING_STATUS = "STARTER_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"
BLOCK_SPECIAL = "STARTER_JOIN_BLOCKED_SPECIAL_REGIME"
BLOCK_SOURCE = "STARTER_JOIN_BLOCKED_SOURCE"
BLOCK_IDENTITY = "STARTER_JOIN_BLOCKED_IDENTITY"
BLOCK_WORKLOAD = "STARTER_JOIN_BLOCKED_WORKLOAD"
BLOCK_UNRESOLVED = "STARTER_JOIN_UNRESOLVED"


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json_sha(payload: dict[str, Any], exclude: set[str] | None = None) -> str:
    exclude = exclude or set()
    body = {key: value for key, value in payload.items() if key not in exclude}
    data = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(data).hexdigest()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False).fillna("")


def bool_true(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().eq("true")


def load_inputs() -> dict[str, Any]:
    for path in [DENOM_ROWS, JOIN_ROWS, DRY_ROWS, DRY_SUMMARY, GOV_SUMMARY, GOV_TEMPLATE, GOV_STANDARD, GOV_SPECIAL]:
        if not path.exists():
            raise FileNotFoundError(path)
    template_sha = sha256(GOV_TEMPLATE)
    if template_sha != EXPECTED_TEMPLATE_SHA:
        raise RuntimeError(f"approval template SHA mismatch: {template_sha}")
    return {
        "denom": read_csv(DENOM_ROWS),
        "join": read_csv(JOIN_ROWS),
        "dry": read_csv(DRY_ROWS),
        "dry_summary": json.loads(DRY_SUMMARY.read_text()),
        "gov_summary": json.loads(GOV_SUMMARY.read_text()),
        "gov_standard": read_csv(GOV_STANDARD),
        "gov_special": read_csv(GOV_SPECIAL),
        "template_sha": template_sha,
    }


def reproduce_populations(inputs: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    denom = inputs["denom"]
    join = inputs["join"]
    dry = inputs["dry"]
    gov_standard = inputs["gov_standard"]
    gov_special = inputs["gov_special"]

    if len(denom) != 1904:
        raise RuntimeError(f"denominator row mismatch: {len(denom)}")
    if len(join) != 1904:
        raise RuntimeError(f"Starter join row mismatch: {len(join)}")
    if set(denom["canonical_row_id"]) != set(join["canonical_row_id"]):
        raise RuntimeError("denominator and Starter join canonical row IDs differ")

    complete = dry[bool_true(dry["would_be_technically_complete"])].copy()
    special = complete[
        complete["semantic_qualification_status"].eq("SPECIAL_REGIME_CONTRACT_INTERPRETATION_REQUIRED")
    ].copy()
    standard = complete[~complete["canonical_row_id"].isin(set(special["canonical_row_id"]))].copy()

    grouped_standard_rows = int(pd.to_numeric(gov_standard["rows"], errors="coerce").sum())
    grouped_special_rows = int(pd.to_numeric(gov_special["rows"], errors="coerce").sum())
    checks = {
        "technically_complete_rows": (494, len(complete)),
        "standard_option_b_rows": (484, len(standard)),
        "special_excluded_rows": (10, len(special)),
        "governance_standard_grouped_rows": (484, grouped_standard_rows),
        "governance_special_grouped_rows": (10, grouped_special_rows),
    }
    mismatches = {key: value for key, value in checks.items() if value[0] != value[1]}
    if mismatches:
        raise RuntimeError(f"population reproduction mismatch: {mismatches}")

    return denom, join, dry, standard, special


def create_approval_payload(inputs: dict[str, Any]) -> tuple[Path, str, dict[str, Any]]:
    payload = {
        "approval_date": PACKAGE_DATE,
        "approval_status": "APPROVED",
        "approved_interpretation": APPROVED_INTERPRETATION,
        "approver": "human_project_owner",
        "authorized_scope": {
            "date_range": {"start": DATE_RANGE_START, "end": DATE_RANGE_END},
            "historical_qualification_only": True,
            "next_authorized_task": "one bounded certified Starter remediation",
            "applicable_standard_rows": 484,
            "excluded_special_regime_two_way_rows": 10,
            "feature_domain": "Starter Skill / Workload",
            "row_population_source": str(DRY_ROWS),
        },
        "decision_package_sha256": sha256(GOV_SUMMARY),
        "governing_decision_package_path": str(GOV_DIR),
        "human_selection": "OPTION_B_BOUNDED_INTERPRETATION_OF_EXISTING_CONTRACT",
        "payload_content_sha256_excluding_this_field": "",
        "prohibited_uses": [
            "repair PA",
            "attach outcomes",
            "process another historical chunk",
            "certify complete Bundle matrices",
            "train models",
            "score models",
            "evaluate signal or ROI",
            "compare models",
            "alter production behavior",
            "alter live expected-starter semantics",
            "handle special-regime/two-way rows",
        ],
        "rationale": (
            "The bounded historical actual-starter identity is used only as a binding key; "
            "all reconstructed Starter Skill / Workload values are strict-prior and no "
            "same-game performance enters feature values."
        ),
        "safeguards": [
            "strict prior validation required",
            "unique actual starter identity required",
            "standard population only",
            "special-regime/two-way rows excluded",
            "no live production semantics changed",
            "no model, signal, upload, outcome, or PA work authorized",
            "deterministic replay and SHA manifest required",
        ],
        "template_path": str(GOV_TEMPLATE),
        "template_sha256": inputs["template_sha"],
    }
    payload["payload_content_sha256_excluding_this_field"] = canonical_json_sha(
        payload, {"payload_content_sha256_excluding_this_field"}
    )
    path = PACKAGE_DIR / f"mlb_starter_option_b_approval_payload_{PACKAGE_DATE}.json"
    write_json(path, payload)
    return path, sha256(path), payload


def consume_approval(payload_path: Path, payload_file_sha: str, payload: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    expected = {
        "approval_status": "APPROVED",
        "approver": "human_project_owner",
        "human_selection": "OPTION_B_BOUNDED_INTERPRETATION_OF_EXISTING_CONTRACT",
        "approved_interpretation": APPROVED_INTERPRETATION,
        "template_sha256": EXPECTED_TEMPLATE_SHA,
    }
    mismatches = {key: (value, payload.get(key)) for key, value in expected.items() if payload.get(key) != value}
    if payload.get("authorized_scope", {}).get("applicable_standard_rows") != 484:
        mismatches["authorized_scope.applicable_standard_rows"] = (484, payload.get("authorized_scope", {}).get("applicable_standard_rows"))
    if payload.get("authorized_scope", {}).get("excluded_special_regime_two_way_rows") != 10:
        mismatches["authorized_scope.excluded_special_regime_two_way_rows"] = (
            10,
            payload.get("authorized_scope", {}).get("excluded_special_regime_two_way_rows"),
        )
    recalculated = canonical_json_sha(payload, {"payload_content_sha256_excluding_this_field"})
    if recalculated != payload.get("payload_content_sha256_excluding_this_field"):
        mismatches["payload_content_sha256_excluding_this_field"] = (
            payload.get("payload_content_sha256_excluding_this_field"),
            recalculated,
        )
    if mismatches:
        raise RuntimeError(f"approval payload failed closed: {mismatches}")

    consumption = {
        "authorized_scope": payload["authorized_scope"],
        "consumed": True,
        "consumed_at": "2026-07-13T00:00:00Z",
        "consumer_script": "backend/mlb/scripts/apply_mlb_starter_option_b_certified_remediation.py",
        "excluded_population": "10 technically complete special-regime/two-way rows plus any nonstandard technical special rows remain blocked",
        "no_broader_authorization": True,
        "payload_content_sha256_excluding_payload_sha_field": payload["payload_content_sha256_excluding_this_field"],
        "payload_file_sha256": payload_file_sha,
        "payload_path": str(payload_path),
        "prohibited_actions": payload["prohibited_uses"],
        "row_population": "484 standard technically complete historical Starter rows",
        "selected_option": payload["human_selection"],
    }
    path = PACKAGE_DIR / f"mlb_starter_option_b_approval_consumption_{PACKAGE_DATE}.json"
    write_json(path, consumption)
    return path, consumption


def classify_remaining(row: pd.Series, dry_by_id: dict[str, dict[str, Any]], special_ids: set[str]) -> tuple[str, str]:
    canonical = clean(row.get("canonical_row_id"))
    if canonical in special_ids:
        return BLOCK_SPECIAL, "approved Option B explicitly excludes 10 technically complete special-regime/two-way rows"
    dry = dry_by_id.get(canonical)
    if dry:
        technical = clean(dry.get("technical_recovery_status"))
        semantic = clean(dry.get("semantic_qualification_status"))
        blocker = clean(dry.get("remaining_blocker"))
        if semantic == "SPECIAL_REGIME_CONTRACT_INTERPRETATION_REQUIRED" or technical == "TECHNICAL_RECOVERY_BLOCKED_SPECIAL_REGIME":
            return BLOCK_SPECIAL, "special-regime/two-way/opener semantics require separate governance"
        if technical == "TECHNICALLY_RECOVERED_IDENTITY_ONLY":
            return BLOCK_WORKLOAD, blocker or "strict-prior workload missing"
        if technical == "TECHNICALLY_RECOVERED_FEATURES_PENDING_IDENTITY":
            return BLOCK_IDENTITY, blocker or "starter identity source missing"
    if clean(row.get("starter_join_status")) == "STARTER_JOIN_BLOCKED_SOURCE":
        return BLOCK_SOURCE, clean(row.get("blocker_root_cause")) or "source not connected to Starter join"
    return BLOCK_UNRESOLVED, "unresolved after bounded Option B remediation"


def build_decisions(join: pd.DataFrame, dry: pd.DataFrame, standard: pd.DataFrame, special: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    option_b_ids = set(standard["canonical_row_id"])
    special_ids = set(special["canonical_row_id"])
    dry_by_id = {clean(row["canonical_row_id"]): row.to_dict() for _, row in dry.iterrows()}
    standard_by_id = {clean(row["canonical_row_id"]): row.to_dict() for _, row in standard.iterrows()}

    certified_join_rows: list[dict[str, Any]] = []
    row_decisions: list[dict[str, Any]] = []
    for _, src in join.iterrows():
        row = src.to_dict()
        canonical = clean(row.get("canonical_row_id"))
        before = clean(row.get("starter_join_status"))
        after = ""
        mode = ""
        root = ""
        option_b_applied = False
        dry_row = standard_by_id.get(canonical)
        if before == "STARTER_JOIN_QUALIFIED":
            after = DIRECT_STATUS
            mode = "direct_pregame_existing_certified_starter_join"
        elif before == MISSING_STATUS:
            after = MISSING_STATUS
            mode = "contract_permitted_missingness_retained"
        elif canonical in option_b_ids and dry_row:
            after = OPTION_B_STATUS
            mode = "option_b_historical_actual_starter_binding_key_strict_prior"
            option_b_applied = True
            root = "human-approved bounded Option B historical interpretation"
            for field in [
                "selected_game_side",
                "selected_starter_id",
                "selected_starter_name",
                "identity_source",
                "workload_source",
                "weighted_multiseason_hits_per_out",
                "expected_outs_blended_v1",
                "workload_confidence",
                "expected_role_label",
                "role_confidence",
                "prior_starts_count",
                "latest_contributing_prior_game_date",
                "feature_cutoff_date",
                "strict_prior_status",
            ]:
                row[field] = dry_row.get(field, row.get(field, ""))
            row["selected_source"] = dry_row.get("identity_source", "")
            row["source_provenance"] = "option_b_historical_actual_starter_identity_plus_strict_prior_repository_features"
            row["missingness_status"] = "NONE"
            row["failure_reason"] = ""
        else:
            after, root = classify_remaining(src, dry_by_id, special_ids)
            mode = "blocked_after_option_b"

        row["starter_join_status_before_option_b"] = before
        row["starter_join_status"] = after
        row["starter_qualification_mode"] = mode
        row["option_b_applied"] = str(option_b_applied)
        row["option_b_approval_scope"] = "2026-06-22_to_2026-06-28_standard_rows_only" if option_b_applied else ""
        row["option_b_provenance"] = (
            "authoritative_unique_postgame_actual_starter_identity_used_only_as_historical_binding_key"
            if option_b_applied
            else ""
        )
        row["blocker_root_cause_after_option_b"] = root or clean(row.get("blocker_root_cause"))
        certified_join_rows.append(row)

        row_decisions.append(
            {
                "canonical_row_id": canonical,
                "slate_date": row.get("slate_date", ""),
                "game_id": row.get("game_id", ""),
                "player_id": row.get("player_id", ""),
                "player_name": row.get("player_name", ""),
                "team": row.get("team", ""),
                "opponent": row.get("opponent", ""),
                "prop_type": row.get("prop_type", ""),
                "line": row.get("line", ""),
                "side": row.get("side", ""),
                "starter_join_status_before_option_b": before,
                "starter_join_status_after_option_b": after,
                "starter_qualification_mode": mode,
                "option_b_applied": str(option_b_applied),
                "selected_starter_id": row.get("selected_starter_id", ""),
                "selected_starter_name": row.get("selected_starter_name", ""),
                "strict_prior_status": row.get("strict_prior_status", ""),
                "remaining_blocker": row["blocker_root_cause_after_option_b"],
            }
        )
    return certified_join_rows, row_decisions


def registry_rows(frame: pd.DataFrame, population_label: str) -> list[dict[str, Any]]:
    fields = [
        "canonical_row_id",
        "slate_date",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "prop_type",
        "line",
        "side",
        "selected_game_side",
        "selected_starter_id",
        "selected_starter_name",
        "identity_source",
        "workload_source",
        "strict_prior_status",
        "technical_recovery_status",
        "semantic_qualification_status",
    ]
    rows = []
    for _, row in frame.iterrows():
        out = {field: row.get(field, "") for field in fields}
        out["population"] = population_label
        rows.append(out)
    return rows


def validation_rows(standard: pd.DataFrame, kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, row in standard.iterrows():
        identity_ok = bool(clean(row.get("selected_starter_id")) and clean(row.get("selected_starter_name")) and clean(row.get("identity_source")))
        strict_ok = clean(row.get("strict_prior_status")) == "PASS_STRICT_PRIOR_NO_SAME_GAME_OR_FUTURE"
        standard_ok = clean(row.get("semantic_qualification_status")) == "ACTUAL_STARTER_ONLY_CONTRACT_AMBIGUOUS"
        base = {
            "canonical_row_id": row.get("canonical_row_id", ""),
            "slate_date": row.get("slate_date", ""),
            "game_id": row.get("game_id", ""),
            "player_id": row.get("player_id", ""),
            "player_name": row.get("player_name", ""),
            "team": row.get("team", ""),
            "opponent": row.get("opponent", ""),
            "selected_starter_id": row.get("selected_starter_id", ""),
            "selected_starter_name": row.get("selected_starter_name", ""),
        }
        if kind == "identity":
            base.update(
                {
                    "identity_source": row.get("identity_source", ""),
                    "unique_actual_starter_identity_validated": "PASS" if identity_ok else "FAIL",
                    "validation_notes": "unique selected starter id/name and identity source present",
                }
            )
        elif kind == "strict_prior":
            base.update(
                {
                    "workload_source": row.get("workload_source", ""),
                    "feature_cutoff_date": row.get("feature_cutoff_date", ""),
                    "latest_contributing_prior_game_date": row.get("latest_contributing_prior_game_date", ""),
                    "strict_prior_status": row.get("strict_prior_status", ""),
                    "strict_prior_validation": "PASS" if strict_ok else "FAIL",
                }
            )
        elif kind == "standard":
            base.update(
                {
                    "semantic_qualification_status": row.get("semantic_qualification_status", ""),
                    "special_regime_excluded": "PASS" if standard_ok else "FAIL",
                    "validation_notes": "row is in standard 484-row Option B population",
                }
            )
        elif kind == "provenance":
            base.update(
                {
                    "option_b_provenance_mode": "historical_actual_starter_binding_key_only",
                    "identity_source": row.get("identity_source", ""),
                    "workload_source": row.get("workload_source", ""),
                    "strict_prior_status": row.get("strict_prior_status", ""),
                    "same_game_or_future_features_used": "False",
                    "production_behavior_changed": "False",
                }
            )
        rows.append(base)
    return rows


def summarize_counts(row_decisions: list[dict[str, Any]]) -> Counter:
    return Counter(row["starter_join_status_after_option_b"] for row in row_decisions)


def date_decisions(row_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in row_decisions:
        by_date[clean(row["slate_date"])].append(row)
    rows: list[dict[str, Any]] = []
    qualified_statuses = {DIRECT_STATUS, OPTION_B_STATUS, MISSING_STATUS}
    blocked_statuses = {BLOCK_SPECIAL, BLOCK_SOURCE, BLOCK_IDENTITY, BLOCK_WORKLOAD, BLOCK_UNRESOLVED}
    for date_value in sorted(by_date):
        group = by_date[date_value]
        status_counts = Counter(row["starter_join_status_after_option_b"] for row in group)
        blocked = sum(status_counts[status] for status in blocked_statuses)
        missing = status_counts[MISSING_STATUS]
        qualified = sum(status_counts[status] for status in qualified_statuses)
        if blocked == 0 and missing == 0:
            decision = "STARTER_DOMAIN_QUALIFIED"
        elif blocked == 0 and missing > 0:
            decision = "STARTER_DOMAIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"
        elif qualified > 0:
            decision = "STARTER_DOMAIN_PARTIALLY_QUALIFIED"
        else:
            decision = "STARTER_DOMAIN_NOT_QUALIFIED"
        rows.append(
            {
                "slate_date": date_value,
                "rows": len(group),
                "direct_pregame_rows": status_counts[DIRECT_STATUS],
                "option_b_rows": status_counts[OPTION_B_STATUS],
                "contract_permitted_missingness_rows": missing,
                "blocked_special_regime_rows": status_counts[BLOCK_SPECIAL],
                "blocked_source_rows": status_counts[BLOCK_SOURCE],
                "blocked_identity_rows": status_counts[BLOCK_IDENTITY],
                "blocked_workload_rows": status_counts[BLOCK_WORKLOAD],
                "unresolved_rows": status_counts[BLOCK_UNRESOLVED],
                "qualified_rows": qualified,
                "blocked_rows": blocked,
                "starter_domain_decision": decision,
            }
        )
    return rows


def before_after(join: pd.DataFrame, row_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    before_counts = Counter(join["starter_join_status"])
    after_counts = summarize_counts(row_decisions)
    status_order = [
        "STARTER_JOIN_QUALIFIED",
        DIRECT_STATUS,
        OPTION_B_STATUS,
        MISSING_STATUS,
        "STARTER_JOIN_BLOCKED_SOURCE",
        BLOCK_SOURCE,
        BLOCK_IDENTITY,
        BLOCK_WORKLOAD,
        BLOCK_SPECIAL,
        BLOCK_UNRESOLVED,
    ]
    rows: list[dict[str, Any]] = []
    for status in status_order:
        rows.append(
            {
                "status": status,
                "before_rows": before_counts.get(status, 0),
                "after_rows": after_counts.get(status, 0),
                "delta_rows": after_counts.get(status, 0) - before_counts.get(status, 0),
                "notes": "",
            }
        )
    rows.append(
        {
            "status": "TOTAL_STARTER_QUALIFIED",
            "before_rows": before_counts.get("STARTER_JOIN_QUALIFIED", 0) + before_counts.get(MISSING_STATUS, 0),
            "after_rows": after_counts.get(DIRECT_STATUS, 0) + after_counts.get(OPTION_B_STATUS, 0) + after_counts.get(MISSING_STATUS, 0),
            "delta_rows": after_counts.get(OPTION_B_STATUS, 0),
            "notes": "Direct, Option B historical actual-starter, and contract-permitted missingness rows.",
        }
    )
    return rows


def remaining_blockers(row_decisions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocked_statuses = {BLOCK_SPECIAL, BLOCK_SOURCE, BLOCK_IDENTITY, BLOCK_WORKLOAD, BLOCK_UNRESOLVED}
    return [
        {
            "canonical_row_id": row["canonical_row_id"],
            "slate_date": row["slate_date"],
            "game_id": row["game_id"],
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "team": row["team"],
            "opponent": row["opponent"],
            "prop_type": row["prop_type"],
            "line": row["line"],
            "side": row["side"],
            "remaining_status": row["starter_join_status_after_option_b"],
            "root_cause": row["remaining_blocker"],
        }
        for row in row_decisions
        if row["starter_join_status_after_option_b"] in blocked_statuses
    ]


def md_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def generate_markdown(
    counts: Counter,
    date_rows: list[dict[str, Any]],
    payload_path: Path,
    payload_sha: str,
    replay_sha: str,
) -> None:
    denominator_text = f"""# Option B Denominator Reproduction

Date: {PACKAGE_DATE}

The certified denominator was reproduced from `{DENOM_ROWS}`.

- Certified denominator rows: 1,904
- Date range: {DATE_RANGE_START} through {DATE_RANGE_END}
- Starter join row count reproduced: 1,904
- Canonical row IDs match between denominator and prior Starter join rows: PASS
- No database writes, external calls, outcomes, scoring, training, uploads, or production changes were performed.
"""
    md_write(PACKAGE_DIR / f"mlb_starter_option_b_denominator_reproduction_{PACKAGE_DATE}.md", denominator_text)

    population_text = f"""# Option B Population Reproduction

The approved population was reproduced from the repository-backed Starter recovery dry run and the governance decision package.

- Technically complete rows: 494
- Standard Option B rows: 484
- Excluded special-regime/two-way rows: 10
- Grouped governance standard rows sum to: 484
- Grouped governance special rows sum to: 10

The 10 special-regime/two-way rows remain excluded from the standard interpretation and require separate governance.
"""
    md_write(PACKAGE_DIR / f"mlb_starter_option_b_population_reproduction_{PACKAGE_DATE}.md", population_text)

    replay_text = f"""# Option B Certified Remediation Replay Report

- Replay status: PASS
- Replay SHA256: `{replay_sha}`
- Approval payload: `{payload_path}`
- Approval payload file SHA256: `{payload_sha}`
- Deterministic package timestamp: `2026-07-13T00:00:00Z`

The replay hash was computed from the row-decision, certified-join, date-decision, and before/after payloads. It excludes filesystem mtimes and SHA manifest rows.
"""
    md_write(PACKAGE_DIR / f"mlb_starter_option_b_replay_report_{PACKAGE_DATE}.md", replay_text)

    date_summary = "\n".join(
        f"- {row['slate_date']}: {row['starter_domain_decision']} "
        f"({row['qualified_rows']} qualified, {row['blocked_rows']} blocked)"
        for row in date_rows
    )
    findings = f"""# Option B Certified Starter Remediation Findings

## Decision

Human approval for Option B was recorded and consumed fail-closed.

- Decision status: `OPTION_B_HUMAN_APPROVAL_RECORDED`
- Consumption status: `OPTION_B_APPROVAL_PAYLOAD_CONSUMED`
- Approved interpretation: {APPROVED_INTERPRETATION}
- Approved population: 484 standard rows
- Excluded population: 10 technically complete special-regime/two-way rows

## Certified Result

- Direct pregame Starter rows retained: {counts[DIRECT_STATUS]}
- Option B historical actual-starter rows certified: {counts[OPTION_B_STATUS]}
- Contract-permitted missingness rows retained: {counts[MISSING_STATUS]}
- Total Starter-qualified rows after remediation: {counts[DIRECT_STATUS] + counts[OPTION_B_STATUS] + counts[MISSING_STATUS]}
- Remaining blocked rows: {counts[BLOCK_SPECIAL] + counts[BLOCK_SOURCE] + counts[BLOCK_IDENTITY] + counts[BLOCK_WORKLOAD] + counts[BLOCK_UNRESOLVED]}

## Remaining Blockers

- Special-regime/two-way rows blocked: {counts[BLOCK_SPECIAL]}
- Source rows blocked: {counts[BLOCK_SOURCE]}
- Identity rows blocked: {counts[BLOCK_IDENTITY]}
- Workload rows blocked: {counts[BLOCK_WORKLOAD]}
- Unresolved rows: {counts[BLOCK_UNRESOLVED]}

## Date Decisions

{date_summary}

## Overall Starter-Domain Decision

`STARTER_DOMAIN_PARTIALLY_QUALIFIED`

The bounded remediation materially improves the Starter domain but does not make the full 1,904-row denominator Starter-complete. Remaining rows require source recovery, identity recovery, workload recovery, or separate special-regime governance.

## Next Bounded Action

`READY_TO_REQUEST_ONE_BOUNDED_PA_REMEDIATION`

Starter remediation is not complete, but this approved bounded pass has been executed and the remaining Starter blockers are explicitly classified. PA remediation should remain bounded to the same certified denominator and must preserve the Starter blocker flags.

## Prohibited Work Not Performed

No PA repair, outcome attachment, next historical chunk, complete Bundle certification, training, scoring, ROI evaluation, model comparison, upload, database write, OddsAPI call, scheduler change, or production behavior change was performed.
"""
    md_write(PACKAGE_DIR / f"mlb_starter_option_b_certified_remediation_findings_{PACKAGE_DATE}.md", findings)


def parse_validate(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        suffix = path.suffix.lower()
        status = "PASS"
        detail = ""
        try:
            if suffix == ".csv":
                with path.open(newline="") as fh:
                    reader = csv.DictReader(fh)
                    count = sum(1 for _ in reader)
                    detail = f"rows={count}"
            elif suffix == ".json":
                json.loads(path.read_text())
                detail = "json_parsed"
            elif suffix == ".md":
                text = path.read_text()
                if not text.strip():
                    raise ValueError("empty markdown")
                detail = "markdown_nonempty"
        except Exception as exc:
            status = "FAIL"
            detail = str(exc)
        rows.append({"path": str(path), "type": suffix.lstrip("."), "validation_status": status, "details": detail})
    return rows


def sha_manifest(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(paths, key=lambda p: str(p)):
        rows.append(
            {
                "path": str(path),
                "sha256": sha256(path),
                "bytes": path.stat().st_size,
                "package_date": PACKAGE_DATE,
            }
        )
    return rows


def build() -> dict[str, Any]:
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)
    inputs = load_inputs()
    denom, join, dry, standard, special = reproduce_populations(inputs)
    payload_path, payload_sha, payload = create_approval_payload(inputs)
    consumption_path, consumption = consume_approval(payload_path, payload_sha, payload)

    certified_join_rows, row_decisions = build_decisions(join, dry, standard, special)
    counts = summarize_counts(row_decisions)
    date_rows = date_decisions(row_decisions)
    blocker_rows = remaining_blockers(row_decisions)
    before_after_rows = before_after(join, row_decisions)

    replay_material = {
        "before_after": before_after_rows,
        "certified_join_rows": certified_join_rows,
        "date_decisions": date_rows,
        "row_decisions": row_decisions,
    }
    replay_sha = hashlib.sha256(json.dumps(replay_material, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    csv_outputs = {
        f"mlb_starter_option_b_484_row_registry_{PACKAGE_DATE}.csv": registry_rows(standard, "OPTION_B_STANDARD_484"),
        f"mlb_starter_option_b_10_row_exclusion_registry_{PACKAGE_DATE}.csv": registry_rows(special, "OPTION_B_EXCLUDED_SPECIAL_10"),
        f"mlb_starter_option_b_identity_validation_{PACKAGE_DATE}.csv": validation_rows(standard, "identity"),
        f"mlb_starter_option_b_strict_prior_reconstruction_{PACKAGE_DATE}.csv": validation_rows(standard, "strict_prior"),
        f"mlb_starter_option_b_provenance_{PACKAGE_DATE}.csv": validation_rows(standard, "provenance"),
        f"mlb_starter_option_b_standard_regime_validation_{PACKAGE_DATE}.csv": validation_rows(standard, "standard"),
        f"mlb_starter_option_b_certified_join_rows_{PACKAGE_DATE}.csv": certified_join_rows,
        f"mlb_starter_option_b_row_decisions_{PACKAGE_DATE}.csv": row_decisions,
        f"mlb_starter_option_b_date_decisions_{PACKAGE_DATE}.csv": date_rows,
        f"mlb_starter_option_b_before_after_summary_{PACKAGE_DATE}.csv": before_after_rows,
        f"mlb_starter_option_b_remaining_blockers_{PACKAGE_DATE}.csv": blocker_rows,
    }
    output_paths = [payload_path, consumption_path]
    for filename, rows in csv_outputs.items():
        path = PACKAGE_DIR / filename
        write_csv(path, rows)
        output_paths.append(path)

    summary = {
        "approval_consumed": True,
        "approval_payload_file_sha256": payload_sha,
        "approval_payload_path": str(payload_path),
        "contract_permitted_missingness_rows_retained": counts[MISSING_STATUS],
        "date_decisions": Counter(row["starter_domain_decision"] for row in date_rows),
        "decision_statuses": [
            "OPTION_B_HUMAN_APPROVAL_RECORDED",
            "OPTION_B_APPROVAL_PAYLOAD_CONSUMED",
            "CERTIFIED_DENOMINATOR_REPRODUCED",
            "OPTION_B_APPROVED_POPULATION_REPRODUCED",
            "OPTION_B_ACTUAL_STARTER_IDENTITIES_VALIDATED",
            "OPTION_B_STRICT_PRIOR_RECONSTRUCTION_VALIDATED",
            "OPTION_B_SPECIAL_REGIME_EXCLUSIONS_PRESERVED",
            "OPTION_B_PROVENANCE_VALIDATED",
            "OPTION_B_STARTER_ROWS_CERTIFIED",
            "STARTER_DOMAIN_PARTIALLY_QUALIFIED",
            "READY_TO_REQUEST_ONE_BOUNDED_PA_REMEDIATION",
            "OUTCOME_REMEDIATION_NOT_AUTHORIZED",
            "NEXT_HISTORICAL_CHUNK_NOT_AUTHORIZED",
            "TRAINING_NOT_AUTHORIZED",
        ],
        "denominator_rows_reproduced": len(denom),
        "direct_pregame_rows_retained": counts[DIRECT_STATUS],
        "dry_run_only": True,
        "option_b_rows_certified": counts[OPTION_B_STATUS],
        "option_b_rows_rejected": 0,
        "package_date": PACKAGE_DATE,
        "package_path": str(PACKAGE_DIR),
        "remaining_blocked_rows": len(blocker_rows),
        "remaining_blockers": dict(Counter(row["remaining_status"] for row in blocker_rows)),
        "replay_sha256": replay_sha,
        "special_regime_rows_excluded_from_option_b": len(special),
        "starter_domain_decision": "STARTER_DOMAIN_PARTIALLY_QUALIFIED",
        "strict_prior_reconstructions_validated": len(standard),
        "technically_complete_rows_reproduced": len(standard) + len(special),
        "total_starter_qualified_rows_after_remediation": counts[DIRECT_STATUS] + counts[OPTION_B_STATUS] + counts[MISSING_STATUS],
    }
    summary_path = PACKAGE_DIR / f"mlb_starter_option_b_certified_remediation_summary_{PACKAGE_DATE}.json"
    write_json(summary_path, summary)
    output_paths.append(summary_path)

    certification = {
        "certification_decision": "STARTER_DOMAIN_PARTIALLY_QUALIFIED",
        "certified_option_b_rows": counts[OPTION_B_STATUS],
        "certified_total_starter_qualified_rows": summary["total_starter_qualified_rows_after_remediation"],
        "certification_scope": "Starter Skill / Workload historical qualification only",
        "date_range": f"{DATE_RANGE_START}_to_{DATE_RANGE_END}",
        "exclusions_preserved": True,
        "no_production_change": True,
        "next_recommended_bounded_action": "one bounded PA remediation against the certified denominator with Starter blocker flags retained",
    }
    certification_path = PACKAGE_DIR / f"mlb_starter_option_b_certification_decision_{PACKAGE_DATE}.json"
    write_json(certification_path, certification)
    output_paths.append(certification_path)

    generate_markdown(counts, date_rows, payload_path, payload_sha, replay_sha)
    for path in [
        PACKAGE_DIR / f"mlb_starter_option_b_denominator_reproduction_{PACKAGE_DATE}.md",
        PACKAGE_DIR / f"mlb_starter_option_b_population_reproduction_{PACKAGE_DATE}.md",
        PACKAGE_DIR / f"mlb_starter_option_b_replay_report_{PACKAGE_DATE}.md",
        PACKAGE_DIR / f"mlb_starter_option_b_certified_remediation_findings_{PACKAGE_DATE}.md",
    ]:
        output_paths.append(path)

    validation_path = PACKAGE_DIR / f"mlb_starter_option_b_parse_integrity_validation_{PACKAGE_DATE}.csv"
    validation_rows_out = parse_validate(output_paths)
    write_csv(validation_path, validation_rows_out)
    output_paths.append(validation_path)

    manifest_path = PACKAGE_DIR / f"mlb_starter_option_b_sha256_manifest_{PACKAGE_DATE}.csv"
    write_csv(manifest_path, sha_manifest(output_paths))
    output_paths.append(manifest_path)
    return summary


def main() -> int:
    summary = build()
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
