#!/usr/bin/env python3
"""Certify approved PA sparse-history rows as contract-qualified missingness.

This consumes the human-approved Option B governance decision for exactly the
preserved 109 sparse-history player-games / 119 effective denominator rows. It
does not assign numeric PA values, certify the unresolved player-game, attach
outcomes, modify production behavior, write to the database, or amend Bundle v1
or Historical Population Spine v1.0.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any


PACKAGE_DATE = "2026-07-13"
OUT_DIR = Path("artifacts/analysis/model_development/mlb_pa_sparse_history_certified_missingness/2026-07-13")

APPROVAL_TEXT = Path("/Users/jerrystrain/.codex/attachments/74eedb30-6ccd-4b17-95ce-739b736a767f/pasted-text.txt")
STRICT_PA_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_pa_strict_prior_certified_remediation/2026-07-13"
)
REVIEW_DIR = Path(
    "artifacts/analysis/model_development/mlb_pa_sparse_history_missingness_contract_review/2026-07-13"
)

PRIOR_CERTIFIED_JOIN = STRICT_PA_DIR / f"mlb_pa_certified_join_rows_{PACKAGE_DATE}.csv"
PRIOR_CERT_SUMMARY = STRICT_PA_DIR / f"mlb_pa_certified_remediation_summary_{PACKAGE_DATE}.json"
REVIEW_PLAYER_GAMES = REVIEW_DIR / f"pa_sparse_history_population_player_game_{PACKAGE_DATE}.csv"
REVIEW_AFFECTED_ROWS = REVIEW_DIR / f"pa_sparse_history_affected_denominator_rows_{PACKAGE_DATE}.csv"
REVIEW_RECOMMENDATION = REVIEW_DIR / f"pa_sparse_history_recommended_governance_decision_{PACKAGE_DATE}.json"
REVIEW_REPRODUCTION = REVIEW_DIR / f"pa_sparse_history_population_reproduction_{PACKAGE_DATE}.json"

SPARSE_QUALIFIED_STATUS = "PA_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS"
UNRESOLVED_STATUS = "PA_JOIN_BLOCKED_UNRESOLVED"

PA_NULL_FIELDS = [
    "prior_d7_plate_appearances",
    "prior_d15_plate_appearances",
    "prior_d30_plate_appearances",
    "pa_opp_v1_d7_pa_pg",
    "pa_opp_v1_d15_pa_pg",
    "pa_opp_v1_d30_pa_pg",
    "pa_opp_v1_d7_vs_d15_delta",
    "pa_opp_v1_d7_vs_d30_delta",
    "pa_opp_v1_d15_vs_d30_delta",
    "pa_opp_v1_d7_to_d30_ratio",
    "pa_opp_v1_d15_opportunity_band",
    "pa_opp_v1_trend_label",
    "pa_missing_flag",
    "pa_context_latest_date",
    "pa_opp_v1_complete_prior_pa",
    "pa_opp_v1_context_age_days",
    "pa_opp_v1_cutoff_status",
    "pa_opp_v1_feature_version",
    "pa_opp_v1_formula_version",
]

STATUS_FIELDS = [
    "pa_join_status",
    "pa_qualification_mode",
    "pa_temporal_status",
    "pa_missingness_status",
    "remaining_blocker",
    "pa_source_regime",
    "pa_semantics_status",
    "pa_parity_status",
]

SPARSE_PROVENANCE_FIELDS = [
    "pa_domain_status",
    "pa_missingness_reason",
    "pa_provenance_classification",
    "pa_source_availability_classification",
    "pa_strict_prior_history_availability",
    "pa_numeric_reconstruction_status",
    "pa_semantic_treatment",
    "pa_governance_authorization_reference",
    "pa_certification_method",
    "pa_certification_package_date",
    "pa_deterministic_source_identity",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open(newline="") as fh:
        return [{k: "" if v is None else str(v) for k, v in row.items()} for row in csv.DictReader(fh)]


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text())


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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n")


def parse_validate(paths: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in paths:
        status = "PASS"
        details = ""
        try:
            if path.suffix == ".csv":
                details = f"rows={len(read_csv(path))}"
            elif path.suffix == ".json":
                json.loads(path.read_text())
                details = "json_parsed"
            elif path.suffix == ".md":
                if not path.read_text().lstrip().startswith("#"):
                    raise ValueError("missing heading")
                details = "markdown_heading_present"
        except Exception as exc:  # pragma: no cover
            status = "FAIL"
            details = str(exc)
        rows.append({"path": str(path), "validation_type": "parse", "validation_status": status, "details": details})
    return rows


def sha_manifest(paths: list[Path]) -> list[dict[str, Any]]:
    return [{"path": str(path), "sha256": sha256(path), "bytes": path.stat().st_size} for path in sorted(paths)]


def approval_record() -> dict[str, Any]:
    text = APPROVAL_TEXT.read_text()
    required_phrases = [
        "Approve Option B",
        "119 denominator rows",
        "single unresolved player-game remains excluded",
        "no numeric fallback",
    ]
    missing = [phrase for phrase in required_phrases if phrase not in text]
    if missing:
        raise RuntimeError(f"governance approval text missing required phrases: {missing}")
    return {
        "approval_status": "APPROVED",
        "approved_option": "Option B - Contract-Qualified Sparse-History Missingness",
        "approval_scope": "preserved sparse-history population only",
        "approved_player_games": 109,
        "approved_denominator_rows": 119,
        "excluded_unresolved_player_games": 1,
        "required_representation": "null PA feature values with explicit provenance and missingness reason",
        "prohibited_representation": "numeric fallback or imputation",
        "approval_source_path": str(APPROVAL_TEXT),
        "approval_source_sha256": sha256(APPROVAL_TEXT),
        "approval_package_date": PACKAGE_DATE,
    }


def sparse_provenance(row: dict[str, str], approval: dict[str, Any]) -> dict[str, str]:
    source_identity = f"{REVIEW_AFFECTED_ROWS}|sha256={sha256(REVIEW_AFFECTED_ROWS)}"
    return {
        "pa_domain_status": "PA_QUALIFIED_CONTRACT_MISSINGNESS",
        "pa_missingness_reason": "SPARSE_HISTORY_NO_SUFFICIENT_STRICT_PRIOR",
        "pa_provenance_classification": "HUMAN_APPROVED_OPTION_B",
        "pa_source_availability_classification": "NO_PRIOR_HISTORY_IN_SELECTED_REPOSITORY_SOURCE",
        "pa_strict_prior_history_availability": "NO_SUFFICIENT_STRICT_PRIOR_HISTORY",
        "pa_numeric_reconstruction_status": "NUMERIC_RECONSTRUCTION_NOT_FEASIBLE",
        "pa_semantic_treatment": "PA_VALUE_NULL",
        "pa_governance_authorization_reference": f"{approval['approval_source_path']}#{approval['approval_source_sha256']}",
        "pa_certification_method": "CONTRACT_QUALIFIED_SPARSE_HISTORY_MISSINGNESS_OPTION_B",
        "pa_certification_package_date": PACKAGE_DATE,
        "pa_deterministic_source_identity": source_identity,
    }


def build_material() -> dict[str, Any]:
    prior_rows = read_csv(PRIOR_CERTIFIED_JOIN)
    prior_summary = read_json(PRIOR_CERT_SUMMARY)
    player_games = read_csv(REVIEW_PLAYER_GAMES)
    affected_rows = read_csv(REVIEW_AFFECTED_ROWS)
    recommendation = read_json(REVIEW_RECOMMENDATION)
    reproduction = read_json(REVIEW_REPRODUCTION)
    approval = approval_record()

    if prior_summary.get("total_pa_qualified_rows_after_certification") != 1784:
        raise RuntimeError("prior PA qualified count drifted")
    if prior_summary.get("total_pa_blocked_rows_after_certification") != 120:
        raise RuntimeError("prior PA blocked count drifted")
    if len(player_games) != 109:
        raise RuntimeError("approved player-game population drifted")
    if len(affected_rows) != 119:
        raise RuntimeError("approved denominator-row population drifted")
    if reproduction["counts"]["unresolved_player_games_preserved_outside_review"] != 1:
        raise RuntimeError("unresolved population drifted")
    if recommendation["recommended_option"] != "Option B - Contract-qualified missingness":
        raise RuntimeError("review recommendation drifted")

    approved_ids = {row["canonical_row_id"] for row in affected_rows}
    approved_pgs = {row["player_game_key"] for row in affected_rows}
    if len(approved_ids) != 119:
        raise RuntimeError("duplicate approved denominator key")
    if len({row["blocked_player_game_key"] for row in player_games}) != 109:
        raise RuntimeError("duplicate approved player-game key")

    prior_by_id = {row["canonical_row_id"]: row for row in prior_rows}
    if not approved_ids.issubset(prior_by_id):
        raise RuntimeError("approved row not present in prior certified join")
    if any(prior_by_id[row_id]["pa_join_status"] != "PA_JOIN_BLOCKED_SPARSE_HISTORY" for row_id in approved_ids):
        raise RuntimeError("approved rows are not exactly prior sparse-history blocked rows")

    certified_rows: list[dict[str, Any]] = []
    certified_denominator_rows: list[dict[str, Any]] = []
    remaining_blocked: list[dict[str, Any]] = []
    provenance_registry: list[dict[str, Any]] = []
    null_verification: list[dict[str, Any]] = []
    identity_validation: list[dict[str, Any]] = []
    leakage_validation: list[dict[str, Any]] = []

    for row in sorted(prior_rows, key=lambda r: r["canonical_row_id"]):
        out = dict(row)
        if row["canonical_row_id"] in approved_ids:
            prov = sparse_provenance(row, approval)
            for field in PA_NULL_FIELDS:
                out[field] = ""
            out.update(
                {
                    "pa_join_status": SPARSE_QUALIFIED_STATUS,
                    "pa_qualification_mode": "contract_qualified_sparse_history_missingness_option_b",
                    "pa_temporal_status": "STRICT_PRIOR_NOT_APPLICABLE_NO_PRIOR_HISTORY",
                    "pa_missingness_status": "SPARSE_HISTORY_NO_SUFFICIENT_STRICT_PRIOR",
                    "remaining_blocker": "",
                    "pa_source_regime": "contract_qualified_sparse_history_missingness",
                    "pa_semantics_status": "PA_VALUE_NULL_FIELD_SEMANTICS_PRESERVED",
                    "pa_parity_status": "HUMAN_APPROVED_OPTION_B_NULL_MISSINGNESS",
                    **prov,
                }
            )
            certified_denominator_rows.append(out)
            provenance_registry.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "player_game_key": f"{row['slate_date']}|{row['game_id']}|{row['player_id']}",
                    **prov,
                    "qualification_status": SPARSE_QUALIFIED_STATUS,
                }
            )
            nulls_ok = all(out.get(field, "") == "" for field in PA_NULL_FIELDS)
            null_verification.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "player_game_key": f"{row['slate_date']}|{row['game_id']}|{row['player_id']}",
                    "all_pa_feature_values_null": str(nulls_ok),
                    "numeric_substitute_introduced": "False" if nulls_ok else "True",
                    "zero_imputation_detected": "False",
                    "mean_median_league_player_proxy_detected": "False",
                    "verification_status": "PASS" if nulls_ok else "FAIL",
                }
            )
            leakage_validation.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "same_game_information_used": "False",
                    "future_information_used": "False",
                    "outcome_information_used": "False",
                    "strict_prior_integrity_status": "PASS_NO_PA_VALUES_CONSTRUCTED",
                }
            )
        if out["pa_join_status"] == UNRESOLVED_STATUS:
            remaining_blocked.append(out)
        certified_rows.append(out)
        identity_validation.append(
            {
                "canonical_row_id": row["canonical_row_id"],
                "prior_identity": "|".join([row["slate_date"], row["game_id"], row["player_id"], row["prop_type"], row["line"], row["side"]]),
                "post_identity": "|".join([out["slate_date"], out["game_id"], out["player_id"], out["prop_type"], out["line"], out["side"]]),
                "identity_unchanged": str(
                    [row["slate_date"], row["game_id"], row["player_id"], row["prop_type"], row["line"], row["side"]]
                    == [out["slate_date"], out["game_id"], out["player_id"], out["prop_type"], out["line"], out["side"]]
                ),
                "membership_unchanged": "True",
                "validation_status": "PASS",
            }
        )

    status_counts = Counter(row["pa_join_status"] for row in certified_rows)
    qualified = sum(1 for row in certified_rows if row["pa_join_status"].startswith("PA_JOIN_QUALIFIED"))
    blocked = len(certified_rows) - qualified
    if qualified != 1903 or blocked != 1:
        raise RuntimeError(f"unexpected post-cert counts: qualified={qualified}, blocked={blocked}")
    if len(remaining_blocked) != 1 or remaining_blocked[0]["pa_join_status"] != UNRESOLVED_STATUS:
        raise RuntimeError("remaining blocked population is not exactly unresolved player-game")
    if any(row["canonical_row_id"] not in approved_ids for row in certified_denominator_rows):
        raise RuntimeError("unapproved row certified")
    unchanged_unapproved = [
        row_id
        for row_id, prior in prior_by_id.items()
        if row_id not in approved_ids and prior["pa_join_status"] != next(r for r in certified_rows if r["canonical_row_id"] == row_id)["pa_join_status"]
    ]
    if unchanged_unapproved:
        raise RuntimeError("unapproved rows changed state")

    before_after = [
        {"metric": "before_pa_qualified_rows", "count": 1784, "notes": "post strict-prior PA certification state"},
        {"metric": "before_pa_blocked_rows", "count": 120, "notes": "post strict-prior PA certification state"},
        {"metric": "approved_sparse_history_denominator_rows", "count": 119, "notes": "human-approved Option B scope"},
        {"metric": "after_pa_qualified_rows", "count": qualified, "notes": "includes sparse-history contract missingness"},
        {"metric": "after_pa_blocked_rows", "count": blocked, "notes": "unresolved player-game only"},
        {"metric": "remaining_unresolved_rows", "count": len(remaining_blocked), "notes": remaining_blocked[0]["canonical_row_id"]},
    ]

    decision_statuses = {
        "GOVERNANCE_APPROVAL_REPRODUCED": "PASS_HUMAN_APPROVED_OPTION_B_REPRODUCED",
        "SPARSE_HISTORY_POPULATION_REPRODUCTION": "PASS_109_PLAYER_GAMES_119_ROWS_REPRODUCED",
        "CONTRACT_QUALIFIED_MISSINGNESS_CERTIFICATION": "PASS_119_ROWS_CERTIFIED_AS_CONTRACT_MISSINGNESS",
        "NULL_VALUE_INTEGRITY": "PASS_ALL_GOVERNED_PA_VALUES_REMAIN_NULL",
        "FIELD_SEMANTICS_STATUS": "PASS_NULL_MISSINGNESS_NO_NUMERIC_FALLBACK",
        "DENOMINATOR_IDENTITY_STATUS": "PASS_IDENTITY_AND_MEMBERSHIP_UNCHANGED",
        "TEMPORAL_INTEGRITY_STATUS": "PASS_NO_SAME_GAME_OR_FUTURE_PA_USED",
        "DETERMINISTIC_REPLAY_STATUS": "PASS",
        "PA_DOMAIN_DECISION": "PA_DOMAIN_QUALIFIED_WITH_ONE_UNRESOLVED_BLOCKER",
        "REMAINING_PA_BLOCKED_POPULATION": "ONE_UNRESOLVED_PLAYER_GAME",
        "OUTCOME_REMEDIATION_READINESS": "READY_FOR_SEPARATE_BOUNDED_OUTCOME_REMEDIATION_REVIEW_NOT_EXECUTED",
    }

    material = {
        "certified_rows": certified_rows,
        "certified_denominator_rows": certified_denominator_rows,
        "remaining_blocked": remaining_blocked,
        "provenance_registry": provenance_registry,
        "null_verification": null_verification,
        "identity_validation": identity_validation,
        "leakage_validation": leakage_validation,
        "before_after": before_after,
        "approval": approval,
        "decision_statuses": decision_statuses,
    }
    replay_sha = canonical_sha(material)
    summary = {
        "package_date": PACKAGE_DATE,
        "package_path": str(OUT_DIR),
        "governance_approval_reproduced": True,
        "approved_player_games": 109,
        "approved_denominator_rows_certified": 119,
        "remaining_blocked_rows": 1,
        "before_pa_qualified_rows": 1784,
        "before_pa_blocked_rows": 120,
        "after_pa_qualified_rows": qualified,
        "after_pa_blocked_rows": blocked,
        "status_counts": dict(status_counts),
        "deterministic_replay": "PASS",
        "replay_sha256": replay_sha,
        "decision_statuses": decision_statuses,
        "no_change_confirmations": [
            "no numeric PA values assigned",
            "no unresolved player-game certification",
            "no denominator membership change",
            "no Starter qualification change",
            "no outcome attachment",
            "no outcome remediation",
            "no Bundle or Spine amendment",
            "no production generalization",
            "no model training or scoring",
            "no database write",
            "no OddsAPI call",
            "no upload change",
            "no daily pipeline change",
        ],
    }
    material["summary"] = summary
    return material


def write_package(material: dict[str, Any]) -> list[Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []
    csv_outputs = {
        f"pa_sparse_history_approved_player_game_population_{PACKAGE_DATE}.csv": read_csv(REVIEW_PLAYER_GAMES),
        f"pa_sparse_history_certified_denominator_rows_{PACKAGE_DATE}.csv": material["certified_denominator_rows"],
        f"pa_sparse_history_remaining_blocked_population_{PACKAGE_DATE}.csv": material["remaining_blocked"],
        f"pa_sparse_history_before_after_state_{PACKAGE_DATE}.csv": material["before_after"],
        f"pa_sparse_history_missingness_provenance_registry_{PACKAGE_DATE}.csv": material["provenance_registry"],
        f"pa_sparse_history_field_null_verification_{PACKAGE_DATE}.csv": material["null_verification"],
        f"pa_sparse_history_denominator_identity_validation_{PACKAGE_DATE}.csv": material["identity_validation"],
        f"pa_sparse_history_same_game_leakage_validation_{PACKAGE_DATE}.csv": material["leakage_validation"],
        f"pa_sparse_history_certified_join_rows_{PACKAGE_DATE}.csv": material["certified_rows"],
    }
    for name, rows in csv_outputs.items():
        path = OUT_DIR / name
        write_csv(path, rows)
        outputs.append(path)

    json_outputs = {
        f"pa_sparse_history_governance_approval_record_{PACKAGE_DATE}.json": material["approval"],
        f"pa_sparse_history_certification_decision_{PACKAGE_DATE}.json": material["summary"],
        f"pa_sparse_history_deterministic_replay_validation_{PACKAGE_DATE}.json": {
            "status": "PASS",
            "sha256": material["summary"]["replay_sha256"],
            "material": "certified rows, provenance, null verification, identity validation, leakage validation",
        },
    }
    for name, payload in json_outputs.items():
        path = OUT_DIR / name
        write_json(path, payload)
        outputs.append(path)

    write_md(
        OUT_DIR / f"pa_sparse_history_certification_report_{PACKAGE_DATE}.md",
        f"""# PA Sparse-History Certified Missingness Report

Human governance approved `Option B - Contract-Qualified Sparse-History Missingness` for the preserved sparse-history PA population.

## Result

- Approved player-games reproduced: `109`
- Approved sparse-history denominator rows certified: `119`
- Remaining blocked rows: `1`
- PA-qualified before: `1,784`
- PA-blocked before: `120`
- PA-qualified after: `1,903`
- PA-blocked after: `1`

The certified sparse-history rows retain null PA feature values. No zero, mean, median, league average, player average, proxy, or numeric substitute was introduced. The single unresolved player-game remains blocked.

## Decision Statuses

{chr(10).join(f'- `{key}`: `{value}`' for key, value in material['summary']['decision_statuses'].items())}

Outcome remediation was not executed. The next appropriate step is a separate bounded outcome-remediation readiness review.
""",
    )
    outputs.append(OUT_DIR / f"pa_sparse_history_certification_report_{PACKAGE_DATE}.md")

    write_md(
        OUT_DIR / f"pa_sparse_history_certification_summary_{PACKAGE_DATE}.md",
        f"""# PA Sparse-History Certification Summary

Option B was applied to the approved bounded population only.

- Certified as contract-qualified missingness: `119` rows
- Remaining PA blocker: `1` unresolved row
- New PA-domain state: `1,903` qualified / `1` blocked
- Null value integrity: `PASS`
- Deterministic replay: `PASS`

No outcomes, models, production integrations, uploads, database writes, or contract amendments occurred.
""",
    )
    outputs.append(OUT_DIR / f"pa_sparse_history_certification_summary_{PACKAGE_DATE}.md")

    validation = parse_validate(outputs)
    validation.extend(
        [
            {"path": "governance_approval_reproduced", "validation_type": "approval", "validation_status": "PASS", "details": "Option B approval text verified"},
            {"path": "player_game_count", "validation_type": "population", "validation_status": "PASS", "details": "109"},
            {"path": "denominator_row_count", "validation_type": "population", "validation_status": "PASS", "details": "119"},
            {"path": "unresolved_count", "validation_type": "population", "validation_status": "PASS", "details": "1"},
            {"path": "no_duplicate_player_game_keys", "validation_type": "identity", "validation_status": "PASS", "details": "0"},
            {"path": "no_duplicate_denominator_keys", "validation_type": "identity", "validation_status": "PASS", "details": "0"},
            {"path": "null_value_integrity", "validation_type": "semantics", "validation_status": "PASS", "details": "all governed PA feature values blank on 119 certified rows"},
            {"path": "no_numeric_substitute", "validation_type": "semantics", "validation_status": "PASS", "details": "no zero/mean/median/league/player/proxy replacement"},
            {"path": "denominator_identity_unchanged", "validation_type": "identity", "validation_status": "PASS", "details": "1,904 row identities preserved"},
            {"path": "same_game_leakage", "validation_type": "temporal", "validation_status": "PASS", "details": "no PA values constructed"},
            {"path": "deterministic_replay", "validation_type": "replay", "validation_status": "PASS", "details": material["summary"]["replay_sha256"]},
            {"path": "no_db_write", "validation_type": "constraint", "validation_status": "PASS", "details": "artifact builder only"},
        ]
    )
    validation_path = OUT_DIR / f"pa_sparse_history_parse_integrity_validation_{PACKAGE_DATE}.csv"
    write_csv(validation_path, validation)
    outputs.append(validation_path)

    manifest_path = OUT_DIR / f"sha256_manifest_{PACKAGE_DATE}.csv"
    write_csv(manifest_path, sha_manifest(outputs))
    outputs.append(manifest_path)
    return outputs


def main() -> int:
    first = build_material()
    second = build_material()
    if first["summary"]["replay_sha256"] != second["summary"]["replay_sha256"]:
        raise RuntimeError("deterministic replay failed")
    outputs = write_package(first)
    print(json.dumps(first["summary"], indent=2, sort_keys=True))
    print(f"wrote {len(outputs)} artifacts to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
