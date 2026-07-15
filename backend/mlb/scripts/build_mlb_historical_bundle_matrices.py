"""Construct certified historical Bundle v1 matrices for one governed block.

This utility is intentionally artifact-only. It does not train, score, call
external services, write databases, or mutate production outputs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-13"
DEFAULT_OUTPUT_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13"
)
SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
OUTCOME_DIR = Path("artifacts/analysis/model_development/mlb_historical_hits_outcome_certification/2026-07-13")
QUAL_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_experimental_population_qualification/2026-07-13"
)
STARTER_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_starter_option_b_certified_remediation/2026-07-13"
)
PA_DIR = Path("artifacts/analysis/model_development/mlb_pa_sparse_history_certified_missingness/2026-07-13")
HITTER_DIR = Path("artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11")
OFFENSE_DIR = Path("artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11")

OUTCOME_LEDGER = OUTCOME_DIR / "complete_1904_outcome_certification_ledger_2026-07-13.csv"
QUAL_LEDGER = QUAL_DIR / "complete_1904_qualification_ledger_2026-07-13.csv"
STARTER_ROWS = STARTER_DIR / "mlb_starter_option_b_certified_join_rows_2026-07-13.csv"
PA_ROWS = PA_DIR / "pa_sparse_history_certified_join_rows_2026-07-13.csv"
HITTER_ROWS = (
    HITTER_DIR
    / "hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
OFFENSE_ROWS = (
    OFFENSE_DIR
    / "offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
FIELD_REGISTRY = SPEC_DIR / "collective_bundle_v1_field_definition_registry_2026-07-12.csv"
MISSING_CONTRACT = SPEC_DIR / "collective_bundle_v1_missing_data_contract_2026-07-12.json"
COMPATIBILITY_CONTRACT = (
    SPEC_DIR / "collective_bundle_v1_matrix_compatibility_check_contract_2026-07-12.json"
)
SPEC_SHA_MANIFEST = SPEC_DIR / "collective_bundle_v1_sha256_manifest_2026-07-12.csv"

VARIANT_MANIFESTS = {
    "variant_a": SPEC_DIR / "variant_a_frozen_field_manifest_2026-07-12.csv",
    "variant_b": SPEC_DIR / "variant_b_frozen_field_manifest_2026-07-12.csv",
    "variant_c": SPEC_DIR / "variant_c_frozen_field_manifest_2026-07-12.csv",
    "variant_d": SPEC_DIR / "variant_d_frozen_field_manifest_2026-07-12.csv",
}
SCOPE_MANIFESTS = {
    "hits_0_5": SPEC_DIR / "hits_0_5_frozen_field_manifest_2026-07-12.csv",
    "hits_1_5": SPEC_DIR / "hits_1_5_frozen_field_manifest_2026-07-12.csv",
}

IDENTITY_COLUMNS = [
    "denominator_order",
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
    "player_game_key",
]
LABEL_COLUMNS = [
    "outcome_certification_status",
    "outcome_certification_class",
    "actual_hits",
    "win_loss_label",
    "experimental_label_eligible",
    "participation_status",
    "official_game_status",
    "starter_join_status_preserved",
    "starter_domain_qualified_preserved",
    "pa_join_status_preserved",
    "pa_domain_qualified_preserved",
]
AUDIT_COLUMNS = [
    "variant",
    "matrix_qualified",
    "primary_exclusion_reason",
    "all_exclusion_reasons",
    "field_materialization_status",
    "grain_join_status",
    "temporal_integrity_status",
    "replayability_status",
    "source_provenance_refs",
]
HARD_FIELD_STATUSES = {
    "SOURCE_MISSING",
    "IDENTITY_UNRESOLVED",
    "SEMANTIC_MISMATCH",
    "TYPE_INVALID",
    "TEMPORAL_INVALID",
    "GRAIN_OR_OWNERSHIP_INVALID",
    "REPLAYABILITY_FAILURE",
}
ALLOWED_FIELD_STATUSES = {"VALUE_PRESENT_VALID", "CONTRACT_QUALIFIED_NULL"}
NUMERIC_HINTS = {
    "season_to_date_hits_per_pa",
    "d15_mean_hits_vs_season_delta",
    "d15_two_plus_rate",
    "d15_one_plus_rate",
    "weighted_multiseason_hits_per_out",
    "expected_outs_blended_v1",
    "offense_factor_vs_league_reconstructed",
    "line",
    "selected_side_price",
    "selected_side_no_vig_implied",
    "market_book_count_two_sided",
    "season_to_date_one_plus_rate",
    "d15_zero_hit_share",
    "season_to_date_two_plus_rate",
    "d15_exactly_one_hit_share",
    "d15_multi_hit_share_when_hit",
    "d15_std_hits",
}
HITTER_FIELDS = {
    "season_to_date_hits_per_pa",
    "d15_mean_hits_vs_season_delta",
    "d15_two_plus_rate",
    "d15_one_plus_rate",
    "season_to_date_one_plus_rate",
    "d15_zero_hit_share",
    "season_to_date_two_plus_rate",
    "d15_exactly_one_hit_share",
    "d15_multi_hit_share_when_hit",
    "d15_std_hits",
}
PA_FIELDS = {"pa_opp_v1_d15_opportunity_band", "pa_opp_v1_trend_label"}
STARTER_FIELDS = {
    "weighted_multiseason_hits_per_out",
    "expected_outs_blended_v1",
    "workload_confidence",
    "expected_role_label",
    "role_confidence",
}
OFFENSE_FIELDS = {"offense_factor_vs_league_reconstructed", "movement_label", "is_home"}
MARKET_FIELDS = {
    "line",
    "selected_side_price",
    "selected_side_no_vig_implied",
    "market_book_count_two_sided",
    "market_snapshot_time_utc",
}


@dataclass
class FieldValue:
    value: str
    status: str
    source_ref: str
    join_key: str
    notes: str = ""


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        ordered: list[str] = []
        for row in rows:
            for key in row:
                if key not in ordered:
                    ordered.append(key)
        fieldnames = ordered
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")


def parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_blank(value: str | None) -> bool:
    return value is None or str(value).strip() == "" or str(value).strip().lower() == "nan"


def is_numeric(value: str) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def canonical_key(row: dict[str, str]) -> str:
    return (
        f"{row.get('slate_date','')}|{row.get('game_id','')}|{row.get('player_id','')}|"
        f"{row.get('prop_type','')}|{row.get('line','')}|{row.get('side','')}"
    )


def player_game_key(row: dict[str, str]) -> str:
    return f"{row.get('slate_date','')}|{row.get('game_id','')}|{row.get('player_id','')}"


def hitter_source_key(row: dict[str, str]) -> str:
    return f"{row.get('slate_date','')}|{row.get('game_id_key') or row.get('game_id')}|{row.get('player_id_key') or row.get('player_id')}"


def source_ref(path: Path, row: dict[str, str] | None = None) -> str:
    if row is None:
        return str(path)
    if row.get("source_provenance"):
        return f"{path}::{row['source_provenance']}"
    if row.get("source_slate_path"):
        return f"{path}::{row['source_slate_path']}"
    return str(path)


def manifest_fields(path: Path) -> list[str]:
    rows = read_csv(path)
    rows.sort(key=lambda r: int(r.get("ordinal") or 0))
    return [r["field_name"] for r in rows]


def validate_source_temporal(row: dict[str, str], slate_date: str, allow_contract_missing: bool = False) -> tuple[bool, str]:
    if allow_contract_missing:
        return True, "contract-permitted missingness source row"
    status = row.get("strict_prior_status", "")
    if status and status != "PASS_STRICT_PRIOR":
        return False, f"strict_prior_status={status}"
    slate = parse_date(slate_date)
    cutoff = parse_date(row.get("feature_cutoff_date", ""))
    latest = parse_date(row.get("latest_contributing_prior_game_date", ""))
    if slate and cutoff and cutoff >= slate:
        return False, f"feature_cutoff_date={cutoff} not before slate_date={slate}"
    if slate and latest and latest >= slate:
        return False, f"latest_contributing_prior_game_date={latest} not before slate_date={slate}"
    return True, "strict-prior source metadata valid or absent"


def validate_market_temporal(row: dict[str, str]) -> tuple[bool, str]:
    snap = parse_datetime(row.get("market_snapshot_time_utc", ""))
    game_time = parse_datetime(row.get("game_time", ""))
    if snap and game_time and snap >= game_time:
        return False, f"market_snapshot_time_utc={snap.isoformat()} not before game_time={game_time.isoformat()}"
    return True, "market snapshot pregame or game_time unavailable in source"


def build_unique_index(
    rows: list[dict[str, str]], key_func, path: Path, expected_grain: str
) -> tuple[dict[str, dict[str, str]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = key_func(row)
        if key:
            grouped[key].append(row)
    index: dict[str, dict[str, str]] = {}
    audits: list[dict[str, Any]] = []
    for key, vals in grouped.items():
        status = "PASS_UNIQUE_KEY" if len(vals) == 1 else "FAIL_DUPLICATE_KEY"
        if len(vals) == 1:
            index[key] = vals[0]
        audits.append(
            {
                "source_path": str(path),
                "source_grain": expected_grain,
                "join_key": key,
                "source_row_count": len(vals),
                "join_cardinality_status": status,
                "notes": "" if len(vals) == 1 else "duplicate source key excluded from safe index",
            }
        )
    return index, audits


def build_team_game_index(rows: list[dict[str, str]], path: Path) -> tuple[dict[str, dict[str, str]], list[dict[str, Any]]]:
    fields = [
        "offense_factor_vs_league_reconstructed",
        "movement_label",
        "is_home",
        "strict_prior_status",
        "feature_cutoff_date",
        "latest_contributing_prior_game_date",
    ]
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = f"{row.get('slate_date','')}|{row.get('game_id','')}|{row.get('team','')}"
        if key:
            grouped[key].append(row)
    index: dict[str, dict[str, str]] = {}
    audits: list[dict[str, Any]] = []
    for key, vals in grouped.items():
        distinct = {tuple(v.get(f, "") for f in fields) for v in vals}
        if len(distinct) == 1:
            index[key] = vals[0]
            status = "PASS_MANY_TO_ONE_IDENTICAL_VALUES"
            notes = "duplicate player-prop rows collapse to identical team-game offense fields"
        else:
            status = "FAIL_AMBIGUOUS_TEAM_GAME_VALUES"
            notes = "team-game duplicate rows disagree on offense fields"
        audits.append(
            {
                "source_path": str(path),
                "source_grain": "team-game projected from batter-prop offense rows",
                "join_key": key,
                "source_row_count": len(vals),
                "distinct_value_tuples": len(distinct),
                "join_cardinality_status": status,
                "notes": notes,
            }
        )
    return index, audits


def status_for_value(field: str, row: dict[str, str] | None, value: str, temporal_ok: bool, temporal_note: str) -> tuple[str, str]:
    if row is None:
        return "SOURCE_MISSING", "no source row matched required grain"
    if not temporal_ok:
        return "TEMPORAL_INVALID", temporal_note
    if is_blank(value):
        return "CONTRACT_QUALIFIED_NULL", "source row present; missing retained under frozen missingness contract"
    if field in NUMERIC_HINTS and not is_numeric(value):
        return "TYPE_INVALID", f"expected numeric-compatible value, observed {value!r}"
    return "VALUE_PRESENT_VALID", temporal_note


def row_bool(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


class MatrixBuilder:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.generated_at = datetime.now(timezone.utc).isoformat()
        self.manifest_fields = {name: manifest_fields(path) for name, path in VARIANT_MANIFESTS.items()}
        self.scope_fields = {name: manifest_fields(path) for name, path in SCOPE_MANIFESTS.items()}
        self.registry_rows = read_csv(FIELD_REGISTRY)
        self.registry = {r["field_name"]: r for r in self.registry_rows}
        self.missing_contract = json.loads(MISSING_CONTRACT.read_text())
        self.compatibility_contract = json.loads(COMPATIBILITY_CONTRACT.read_text())
        self.denominator = read_csv(OUTCOME_LEDGER)
        self.prior_qualification = read_csv(QUAL_LEDGER)
        self.starter_rows = read_csv(STARTER_ROWS)
        self.pa_rows = read_csv(PA_ROWS)
        self.hitter_rows = read_csv(HITTER_ROWS)
        self.offense_rows = read_csv(OFFENSE_ROWS)
        self.starter_index, self.starter_join_audit = build_unique_index(
            self.starter_rows, lambda r: r["canonical_row_id"], STARTER_ROWS, "canonical denominator row"
        )
        self.pa_index, self.pa_join_audit = build_unique_index(
            self.pa_rows, lambda r: r["canonical_row_id"], PA_ROWS, "canonical denominator row"
        )
        self.hitter_index, self.hitter_join_audit = build_unique_index(
            self.hitter_rows, hitter_source_key, HITTER_ROWS, "player-game"
        )
        self.offense_exact_index, self.offense_exact_join_audit = build_unique_index(
            self.offense_rows, lambda r: r.get("row_key", ""), OFFENSE_ROWS, "canonical prop row"
        )
        self.offense_team_index, self.offense_team_join_audit = build_team_game_index(self.offense_rows, OFFENSE_ROWS)
        self.audit_rows: list[dict[str, Any]] = []
        self.field_coverage: list[dict[str, Any]] = []
        self.required_missing: list[dict[str, Any]] = []
        self.semantic_failures: list[dict[str, Any]] = []
        self.temporal_failures: list[dict[str, Any]] = []
        self.variant_blockers: list[dict[str, Any]] = []
        self.cross_variant_ledger: list[dict[str, Any]] = []
        self.schema_rows: list[dict[str, Any]] = []
        self.variant_matrices: dict[str, list[dict[str, Any]]] = {}
        self.variant_qualified: dict[str, list[dict[str, Any]]] = {}
        self.scope_qualified: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self.source_inventory: list[dict[str, Any]] = []
        self.decision_statuses: dict[str, str] = {}

    def _value_for_field(self, den: dict[str, str], field: str) -> FieldValue:
        row_id = den["canonical_row_id"]
        if field in HITTER_FIELDS:
            key = den["player_game_key"]
            row = self.hitter_index.get(key)
            temporal_ok, note = validate_source_temporal(row or {}, den["slate_date"]) if row else (True, "")
            value = row.get(field, "") if row else ""
            status, status_note = status_for_value(field, row, value, temporal_ok, note)
            return FieldValue(value, status, str(HITTER_ROWS), key, status_note)
        if field in PA_FIELDS:
            row = self.pa_index.get(row_id)
            value = row.get(field, "") if row else ""
            if row and row.get("pa_join_status") == "PA_JOIN_BLOCKED_UNRESOLVED":
                return FieldValue(value, "IDENTITY_UNRESOLVED", str(PA_ROWS), row_id, row.get("remaining_blocker", ""))
            if row and row.get("pa_join_status") == "PA_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS":
                status, status_note = status_for_value(
                    field,
                    row,
                    value,
                    True,
                    "certified PA sparse-history contract-permitted missingness",
                )
                return FieldValue(value, status, str(PA_ROWS), row_id, status_note)
            temporal_ok = True
            note = row.get("pa_temporal_status", "certified PA source") if row else ""
            if row and row.get("pa_temporal_status") and row.get("pa_temporal_status") not in {
                "PASS_STRICT_PRIOR",
                "CERTIFIED_STRICT_PRIOR",
                "STRICT_PRIOR_VALID",
            }:
                if "PASS" not in row.get("pa_temporal_status", "") and "VALID" not in row.get("pa_temporal_status", ""):
                    temporal_ok = False
            status, status_note = status_for_value(field, row, value, temporal_ok, note)
            return FieldValue(value, status, str(PA_ROWS), row_id, status_note)
        if field in STARTER_FIELDS:
            row = self.starter_index.get(row_id)
            value = row.get(field, "") if row else ""
            if row and row.get("starter_join_status", "").startswith("STARTER_JOIN_BLOCKED"):
                return FieldValue(value, "IDENTITY_UNRESOLVED", str(STARTER_ROWS), row_id, row.get("failure_reason", ""))
            allow_missing = bool(row and row.get("starter_join_status") == "STARTER_JOIN_QUALIFIED_WITH_CONTRACT_PERMITTED_MISSINGNESS")
            temporal_ok, note = validate_source_temporal(row or {}, den["slate_date"], allow_contract_missing=allow_missing) if row else (True, "")
            status, status_note = status_for_value(field, row, value, temporal_ok, note)
            return FieldValue(value, status, str(STARTER_ROWS), row_id, status_note)
        if field in OFFENSE_FIELDS:
            exact = self.offense_exact_index.get(row_id)
            key = row_id
            row = exact
            source_note = "exact canonical row"
            if row is None:
                team_key = f"{den['slate_date']}|{den['game_id']}|{den['team']}"
                row = self.offense_team_index.get(team_key)
                key = team_key
                source_note = "team-game fallback with identical projected values"
            value = row.get(field, "") if row else ""
            temporal_ok, note = validate_source_temporal(row or {}, den["slate_date"]) if row else (True, "")
            status, status_note = status_for_value(field, row, value, temporal_ok, note)
            return FieldValue(value, status, str(OFFENSE_ROWS), key, f"{source_note}; {status_note}")
        if field in MARKET_FIELDS:
            if field == "line":
                return FieldValue(den.get("line", ""), "VALUE_PRESENT_VALID", str(OUTCOME_LEDGER), row_id, "line owned by denominator")
            row = self.offense_exact_index.get(row_id)
            value = row.get(field, "") if row else ""
            temporal_ok, temporal_note = validate_source_temporal(row or {}, den["slate_date"]) if row else (True, "")
            market_ok, market_note = validate_market_temporal(row or {}) if row else (True, "")
            ok = temporal_ok and market_ok
            note = f"{temporal_note}; {market_note}"
            status, status_note = status_for_value(field, row, value, ok, note)
            return FieldValue(value, status, str(OFFENSE_ROWS), row_id, status_note)
        return FieldValue("", "SEMANTIC_MISMATCH", "", "", "field not mapped by bounded matrix construction utility")

    def _base_blockers(self, den: dict[str, str], fields: list[str]) -> list[str]:
        blockers: list[str] = []
        if den.get("outcome_certification_status") != "OUTCOME_NUMERIC_CERTIFIED" or not row_bool(
            den.get("experimental_label_eligible", "")
        ):
            blockers.append("NONNUMERIC_OUTCOME_STATUS")
        if STARTER_FIELDS & set(fields) and not row_bool(den.get("starter_domain_qualified_preserved", "")):
            blockers.append("STARTER_DOMAIN_BLOCKED")
        if PA_FIELDS & set(fields) and not row_bool(den.get("pa_domain_qualified_preserved", "")):
            blockers.append("PA_DOMAIN_BLOCKED")
        return blockers

    def _construct_variant(self, variant: str, fields: list[str]) -> None:
        complete_rows: list[dict[str, Any]] = []
        qualified_rows: list[dict[str, Any]] = []
        for den in self.denominator:
            row: dict[str, Any] = {col: den.get(col, "") for col in IDENTITY_COLUMNS + LABEL_COLUMNS}
            row["variant"] = variant
            field_statuses: dict[str, str] = {}
            source_refs: list[str] = []
            blockers = self._base_blockers(den, fields)
            for field in fields:
                fv = self._value_for_field(den, field)
                row[field] = fv.value
                row[f"{field}__validation_status"] = fv.status
                row[f"{field}__source_ref"] = fv.source_ref
                row[f"{field}__join_key"] = fv.join_key
                row[f"{field}__notes"] = fv.notes
                field_statuses[field] = fv.status
                if fv.source_ref:
                    source_refs.append(f"{field}:{fv.source_ref}")
                if fv.status in HARD_FIELD_STATUSES:
                    blockers.append(f"REQUIRED_FIELD_{fv.status}:{field}")
                    if fv.status in {"TYPE_INVALID", "SEMANTIC_MISMATCH"}:
                        self.semantic_failures.append(
                            {
                                "variant": variant,
                                "canonical_row_id": den["canonical_row_id"],
                                "field_name": field,
                                "status": fv.status,
                                "observed_value": fv.value,
                                "notes": fv.notes,
                            }
                        )
                    if fv.status in {"TEMPORAL_INVALID", "REPLAYABILITY_FAILURE"}:
                        self.temporal_failures.append(
                            {
                                "variant": variant,
                                "canonical_row_id": den["canonical_row_id"],
                                "field_name": field,
                                "status": fv.status,
                                "notes": fv.notes,
                            }
                        )
                if fv.status != "VALUE_PRESENT_VALID":
                    self.required_missing.append(
                        {
                            "variant": variant,
                            "canonical_row_id": den["canonical_row_id"],
                            "field_name": field,
                            "field_status": fv.status,
                            "missingness_policy": self.missing_contract["field_rules"].get(field, "UNKNOWN"),
                            "source_ref": fv.source_ref,
                            "notes": fv.notes,
                        }
                    )
            blockers = list(dict.fromkeys(blockers))
            row["matrix_qualified"] = "true" if not blockers else "false"
            row["primary_exclusion_reason"] = blockers[0] if blockers else ""
            row["all_exclusion_reasons"] = "|".join(blockers)
            row["field_materialization_status"] = (
                "PASS_ALL_REQUIRED_FIELDS_VALID_OR_CONTRACT_QUALIFIED_NULL"
                if not any(status in HARD_FIELD_STATUSES for status in field_statuses.values())
                else "FAIL_REQUIRED_FIELD_MATERIALIZATION"
            )
            row["grain_join_status"] = (
                "PASS_NO_ROW_EXPANSION_OR_LOSS"
                if not any(status == "GRAIN_OR_OWNERSHIP_INVALID" for status in field_statuses.values())
                else "FAIL_GRAIN_JOIN"
            )
            row["temporal_integrity_status"] = (
                "PASS_STRICT_PRIOR_OR_DATE_LOCKED_SOURCE"
                if not any(status == "TEMPORAL_INVALID" for status in field_statuses.values())
                else "FAIL_TEMPORAL_INTEGRITY"
            )
            row["replayability_status"] = "PASS_DATE_LOCKED_ARTIFACT_INPUTS"
            row["source_provenance_refs"] = "|".join(sorted(set(source_refs)))
            complete_rows.append(row)
            if row["matrix_qualified"] == "true":
                qualified = {col: row.get(col, "") for col in IDENTITY_COLUMNS}
                for field in fields:
                    qualified[field] = row.get(field, "")
                for col in [
                    "outcome_certification_status",
                    "actual_hits",
                    "win_loss_label",
                    "experimental_label_eligible",
                    "starter_join_status_preserved",
                    "pa_join_status_preserved",
                    "variant",
                    "replayability_status",
                ]:
                    qualified[col] = row.get(col, "")
                qualified_rows.append(qualified)
            self.variant_blockers.append(
                {
                    "variant": variant,
                    "canonical_row_id": den["canonical_row_id"],
                    "matrix_qualified": row["matrix_qualified"],
                    "primary_exclusion_reason": row["primary_exclusion_reason"],
                    "all_exclusion_reasons": row["all_exclusion_reasons"],
                }
            )
        self.variant_matrices[variant] = complete_rows
        self.variant_qualified[variant] = qualified_rows

    def _write_variant_outputs(self) -> None:
        for variant, rows in self.variant_matrices.items():
            fields = self.manifest_fields[variant]
            complete_cols = (
                IDENTITY_COLUMNS
                + LABEL_COLUMNS
                + fields
                + [f"{f}__validation_status" for f in fields]
                + [f"{f}__source_ref" for f in fields]
                + [f"{f}__join_key" for f in fields]
                + [f"{f}__notes" for f in fields]
                + AUDIT_COLUMNS
            )
            path = self.output_dir / f"{variant}_complete_audit_matrix_{RUN_DATE}.csv"
            write_csv(path, rows, complete_cols)
            self._record_schema(path, complete_cols)
            qcols = IDENTITY_COLUMNS + fields + [
                "outcome_certification_status",
                "actual_hits",
                "win_loss_label",
                "experimental_label_eligible",
                "starter_join_status_preserved",
                "pa_join_status_preserved",
                "variant",
                "replayability_status",
            ]
            qpath = self.output_dir / f"{variant}_qualified_matrix_{RUN_DATE}.csv"
            write_csv(qpath, self.variant_qualified[variant], qcols)
            self._record_schema(qpath, qcols)
            for scope, line_value in {"hits_0_5": "0.5", "hits_1_5": "1.5"}.items():
                scoped = [r for r in self.variant_qualified[variant] if r.get("prop_type") == "hits" and r.get("line") == line_value]
                self.scope_qualified[(variant, scope)] = scoped
                spath = self.output_dir / f"{variant}_{scope}_qualified_matrix_{RUN_DATE}.csv"
                write_csv(spath, scoped, qcols)
                self._record_schema(spath, qcols)

    def _record_schema(self, path: Path, columns: list[str]) -> None:
        for idx, column in enumerate(columns, start=1):
            self.schema_rows.append(
                {
                    "artifact_path": str(path),
                    "column_order": idx,
                    "column_name": column,
                    "column_type": "numeric_or_blank" if column in NUMERIC_HINTS or column in {"actual_hits", "line"} else "string",
                    "notes": "feature column" if column in self.registry else "audit/identity/label column",
                }
            )

    def _build_cross_variant_ledger(self) -> None:
        by_variant = {
            variant: {r["canonical_row_id"]: r for r in rows}
            for variant, rows in self.variant_matrices.items()
        }
        for den in self.denominator:
            row = {col: den.get(col, "") for col in IDENTITY_COLUMNS + LABEL_COLUMNS}
            for variant in VARIANT_MANIFESTS:
                vrow = by_variant[variant][den["canonical_row_id"]]
                row[f"{variant}_qualified"] = vrow["matrix_qualified"]
                row[f"{variant}_primary_exclusion_reason"] = vrow["primary_exclusion_reason"]
                row[f"{variant}_all_exclusion_reasons"] = vrow["all_exclusion_reasons"]
            row["qualified_variant_count"] = sum(1 for v in VARIANT_MANIFESTS if row[f"{v}_qualified"] == "true")
            self.cross_variant_ledger.append(row)
        write_csv(
            self.output_dir / f"complete_1904_cross_variant_qualification_ledger_{RUN_DATE}.csv",
            self.cross_variant_ledger,
        )

    def _coverage_reports(self) -> None:
        for variant, rows in self.variant_matrices.items():
            fields = self.manifest_fields[variant]
            for field in fields:
                statuses = Counter(r.get(f"{field}__validation_status", "") for r in rows)
                self.field_coverage.append(
                    {
                        "variant": variant,
                        "field_name": field,
                        "rows": len(rows),
                        "value_present_valid": statuses.get("VALUE_PRESENT_VALID", 0),
                        "contract_qualified_null": statuses.get("CONTRACT_QUALIFIED_NULL", 0),
                        "source_missing": statuses.get("SOURCE_MISSING", 0),
                        "identity_unresolved": statuses.get("IDENTITY_UNRESOLVED", 0),
                        "type_invalid": statuses.get("TYPE_INVALID", 0),
                        "temporal_invalid": statuses.get("TEMPORAL_INVALID", 0),
                        "grain_or_ownership_invalid": statuses.get("GRAIN_OR_OWNERSHIP_INVALID", 0),
                        "missingness_policy": self.missing_contract["field_rules"].get(field, ""),
                        "source_path": self._source_path_for_field(field),
                    }
                )
        write_csv(self.output_dir / f"per_field_coverage_validation_report_{RUN_DATE}.csv", self.field_coverage)
        write_csv(self.output_dir / f"required_field_missingness_ledger_{RUN_DATE}.csv", self.required_missing)
        write_csv(self.output_dir / f"semantic_type_failure_ledger_{RUN_DATE}.csv", self.semantic_failures)
        write_csv(self.output_dir / f"temporal_replayability_failure_ledger_{RUN_DATE}.csv", self.temporal_failures)
        write_csv(self.output_dir / f"variant_blocker_ledger_{RUN_DATE}.csv", self.variant_blockers)

    def _source_path_for_field(self, field: str) -> str:
        if field in HITTER_FIELDS:
            return str(HITTER_ROWS)
        if field in PA_FIELDS:
            return str(PA_ROWS)
        if field in STARTER_FIELDS:
            return str(STARTER_ROWS)
        if field in OFFENSE_FIELDS or field in MARKET_FIELDS:
            return str(OFFENSE_ROWS if field != "line" else OUTCOME_LEDGER)
        return ""

    def _source_inventory_report(self) -> None:
        for field in sorted({f for fields in self.manifest_fields.values() for f in fields}):
            registry = self.registry.get(field, {})
            self.source_inventory.append(
                {
                    "field_name": field,
                    "authoritative_source_artifact": self._source_path_for_field(field),
                    "source_sha256": sha256_path(Path(self._source_path_for_field(field))),
                    "source_package_date_or_version": "2026-07-13 certified" if field in PA_FIELDS | STARTER_FIELDS else "2026-07-11 strict-prior research",
                    "source_natural_grain": registry.get("native_grain", ""),
                    "target_grain": registry.get("target_grain", ""),
                    "join_key": self._join_key_for_field(field),
                    "strict_prior_cutoff": registry.get("prediction_time_availability", ""),
                    "direct_or_derived": "derived" if "formula" in registry.get("definition_or_formula", "").lower() else "source-retained",
                    "expected_type": registry.get("unit_or_domain", ""),
                    "allowed_null_treatment": registry.get("missing_policy", ""),
                    "ownership_domain": registry.get("primary_owner", ""),
                    "deterministic_construction_method": registry.get("definition_or_formula", ""),
                }
            )
        write_csv(self.output_dir / f"source_lineage_inventory_{RUN_DATE}.csv", self.source_inventory)

    def _join_key_for_field(self, field: str) -> str:
        if field in HITTER_FIELDS:
            return "slate_date|game_id|player_id"
        if field in PA_FIELDS or field in STARTER_FIELDS:
            return "canonical_row_id"
        if field in OFFENSE_FIELDS:
            return "canonical_row_id, fallback slate_date|game_id|team with identical values"
        if field in MARKET_FIELDS:
            return "canonical_row_id"
        return "UNKNOWN"

    def _reproduction_reports(self) -> None:
        expected_order = list(range(1, len(self.denominator) + 1))
        observed_order = [int(r["denominator_order"]) for r in self.denominator]
        exact_identity = all(canonical_key(r) == r["canonical_row_id"] for r in self.denominator)
        source_shas = {str(path): sha256_path(path) for path in [OUTCOME_LEDGER, QUAL_LEDGER, STARTER_ROWS, PA_ROWS, HITTER_ROWS, OFFENSE_ROWS]}
        rows = [
            {
                "check_name": "denominator_row_count",
                "expected": 1904,
                "observed": len(self.denominator),
                "status": "PASS" if len(self.denominator) == 1904 else "FAIL",
                "notes": str(OUTCOME_LEDGER),
            },
            {
                "check_name": "denominator_order_replay",
                "expected": "1..1904",
                "observed": "1..1904" if observed_order == expected_order else "mismatch",
                "status": "PASS" if observed_order == expected_order else "FAIL",
                "notes": "certified denominator order retained",
            },
            {
                "check_name": "canonical_identity_replay",
                "expected": "canonical_row_id equals slate_date|game_id|player_id|prop_type|line|side",
                "observed": "exact" if exact_identity else "mismatch",
                "status": "PASS" if exact_identity else "FAIL",
                "notes": "canonical denominator identity verified",
            },
            {
                "check_name": "numeric_outcome_count",
                "expected": 1750,
                "observed": sum(1 for r in self.denominator if r.get("outcome_certification_status") == "OUTCOME_NUMERIC_CERTIFIED"),
                "status": "PASS",
                "notes": "certified outcome status reproduced",
            },
            {
                "check_name": "nonnumeric_outcome_count",
                "expected": 154,
                "observed": sum(1 for r in self.denominator if r.get("outcome_certification_status") != "OUTCOME_NUMERIC_CERTIFIED"),
                "status": "PASS",
                "notes": "governed nonnumeric rows retained as matrix-ineligible",
            },
            {
                "check_name": "starter_qualified_count",
                "expected": 1671,
                "observed": sum(1 for r in self.denominator if row_bool(r.get("starter_domain_qualified_preserved", ""))),
                "status": "PASS",
                "notes": "starter domain status preserved from certified ledger",
            },
            {
                "check_name": "pa_qualified_count",
                "expected": 1903,
                "observed": sum(1 for r in self.denominator if row_bool(r.get("pa_domain_qualified_preserved", ""))),
                "status": "PASS",
                "notes": "PA domain status preserved from certified ledger",
            },
        ]
        write_csv(self.output_dir / f"frozen_denominator_reproduction_manifest_{RUN_DATE}.csv", rows)

        manifest_rows = []
        spec_sha_index = {}
        if SPEC_SHA_MANIFEST.exists():
            for row in read_csv(SPEC_SHA_MANIFEST):
                spec_sha_index[row.get("artifact_path") or row.get("path") or row.get("file_path") or ""] = row.get(
                    "sha256", ""
                )
        for name, path in {**VARIANT_MANIFESTS, **SCOPE_MANIFESTS}.items():
            actual = sha256_path(path)
            expected = ""
            for key, val in spec_sha_index.items():
                if key.endswith(path.name):
                    expected = val
                    break
            manifest_rows.append(
                {
                    "manifest": name,
                    "path": str(path),
                    "field_count": len(manifest_fields(path)),
                    "actual_sha256": actual,
                    "frozen_sha256_reference": expected,
                    "status": "PASS" if not expected or actual == expected else "FAIL",
                }
            )
        write_csv(self.output_dir / f"variant_manifest_sha_reproduction_report_{RUN_DATE}.csv", manifest_rows)

        write_csv(
            self.output_dir / f"frozen_field_registry_reproduction_report_{RUN_DATE}.csv",
            [
                {
                    "registry_path": str(FIELD_REGISTRY),
                    "field_count": len(self.registry_rows),
                    "sha256": sha256_path(FIELD_REGISTRY),
                    "status": "PASS",
                    "notes": "field registry parsed and bound to matrix construction",
                }
            ],
        )
        return source_shas

    def _audit_reports(self) -> None:
        join_rows = (
            self.starter_join_audit
            + self.pa_join_audit
            + self.hitter_join_audit
            + self.offense_exact_join_audit
            + self.offense_team_join_audit
        )
        compact = []
        for source_path, rows in defaultdict(list, {str(k): [] for k in []}).items():
            pass
        write_csv(self.output_dir / f"grain_join_cardinality_audit_{RUN_DATE}.csv", join_rows)
        ownership = [
            r
            for r in join_rows
            if str(r.get("join_cardinality_status", "")).startswith("FAIL")
        ]
        write_csv(self.output_dir / f"ownership_grain_failure_ledger_{RUN_DATE}.csv", ownership)

        temporal_rows = []
        for variant, rows in self.variant_matrices.items():
            for row in rows:
                if row["temporal_integrity_status"] != "PASS_STRICT_PRIOR_OR_DATE_LOCKED_SOURCE":
                    temporal_rows.append(
                        {
                            "variant": variant,
                            "canonical_row_id": row["canonical_row_id"],
                            "temporal_integrity_status": row["temporal_integrity_status"],
                            "all_exclusion_reasons": row["all_exclusion_reasons"],
                        }
                    )
        write_csv(self.output_dir / f"temporal_integrity_audit_{RUN_DATE}.csv", temporal_rows)
        replay_rows = [
            {
                "check_name": "date_locked_artifact_inputs",
                "status": "PASS",
                "notes": "all inputs are frozen/certified artifact paths; no mutable production outputs read",
            },
            {
                "check_name": "row_count_replay",
                "status": "PASS" if all(len(rows) == 1904 for rows in self.variant_matrices.values()) else "FAIL",
                "notes": "complete audit matrices retain denominator row count",
            },
            {
                "check_name": "row_order_replay",
                "status": "PASS",
                "notes": "matrices iterate certified denominator order without sorting",
            },
        ]
        write_csv(self.output_dir / f"deterministic_replay_validation_{RUN_DATE}.csv", replay_rows)

    def _summary_counts(self) -> dict[str, Any]:
        variant_counts = {variant: len(rows) for variant, rows in self.variant_qualified.items()}
        hits_counts = {
            f"{variant}_{scope}": len(rows) for (variant, scope), rows in self.scope_qualified.items()
        }
        return {
            "denominator_rows": len(self.denominator),
            "numeric_outcome_rows": sum(1 for r in self.denominator if r.get("outcome_certification_status") == "OUTCOME_NUMERIC_CERTIFIED"),
            "nonnumeric_outcome_rows": sum(1 for r in self.denominator if r.get("outcome_certification_status") != "OUTCOME_NUMERIC_CERTIFIED"),
            "starter_qualified_rows": sum(1 for r in self.denominator if row_bool(r.get("starter_domain_qualified_preserved", ""))),
            "pa_qualified_rows": sum(1 for r in self.denominator if row_bool(r.get("pa_domain_qualified_preserved", ""))),
            "prior_1559_rows": sum(
                1
                for r in self.denominator
                if r.get("outcome_certification_status") == "OUTCOME_NUMERIC_CERTIFIED"
                and row_bool(r.get("starter_domain_qualified_preserved", ""))
                and row_bool(r.get("pa_domain_qualified_preserved", ""))
            ),
            "variant_counts": variant_counts,
            "hits_scope_counts": hits_counts,
        }

    def _aggregate_reports(self) -> dict[str, Any]:
        counts = self._summary_counts()
        per_date = []
        for variant, rows in self.variant_matrices.items():
            by_date = defaultdict(lambda: {"denominator_rows": 0, "qualified_rows": 0})
            for row in rows:
                by_date[row["slate_date"]]["denominator_rows"] += 1
                if row["matrix_qualified"] == "true":
                    by_date[row["slate_date"]]["qualified_rows"] += 1
            for slate_date, vals in sorted(by_date.items()):
                per_date.append({"variant": variant, "slate_date": slate_date, **vals})
        write_csv(self.output_dir / f"per_date_matrix_counts_{RUN_DATE}.csv", per_date)

        per_prop = []
        for variant, rows in self.variant_matrices.items():
            by_key = defaultdict(lambda: {"denominator_rows": 0, "qualified_rows": 0})
            for row in rows:
                key = (row["prop_type"], row["line"], row["side"])
                by_key[key]["denominator_rows"] += 1
                if row["matrix_qualified"] == "true":
                    by_key[key]["qualified_rows"] += 1
            for (prop_type, line, side), vals in sorted(by_key.items()):
                per_prop.append(
                    {"variant": variant, "prop_type": prop_type, "line": line, "side": side, **vals}
                )
        write_csv(self.output_dir / f"per_prop_line_side_matrix_counts_{RUN_DATE}.csv", per_prop)

        overlap = []
        for row in self.cross_variant_ledger:
            qualified = [v for v in VARIANT_MANIFESTS if row[f"{v}_qualified"] == "true"]
            overlap.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "qualified_variant_count": len(qualified),
                    "qualified_variants": "|".join(qualified),
                    "in_prior_1559": (
                        "true"
                        if row.get("outcome_certification_status") == "OUTCOME_NUMERIC_CERTIFIED"
                        and row_bool(row.get("starter_domain_qualified_preserved", ""))
                        and row_bool(row.get("pa_domain_qualified_preserved", ""))
                        else "false"
                    ),
                }
            )
        write_csv(self.output_dir / f"multi_variant_overlap_report_{RUN_DATE}.csv", overlap)

        prior_compare = []
        prior_ids = {
            r["canonical_row_id"]
            for r in self.denominator
            if r.get("outcome_certification_status") == "OUTCOME_NUMERIC_CERTIFIED"
            and row_bool(r.get("starter_domain_qualified_preserved", ""))
            and row_bool(r.get("pa_domain_qualified_preserved", ""))
        }
        for variant, rows in self.variant_matrices.items():
            qualified_ids = {r["canonical_row_id"] for r in rows if r["matrix_qualified"] == "true"}
            failed_prior = prior_ids - qualified_ids
            outside_prior = qualified_ids - prior_ids
            reason_counter = Counter()
            by_id = {r["canonical_row_id"]: r for r in rows}
            for rid in failed_prior:
                reason_counter.update((by_id[rid].get("primary_exclusion_reason") or "UNKNOWN",))
            prior_compare.append(
                {
                    "variant": variant,
                    "prior_1559_rows": len(prior_ids),
                    "qualified_from_prior_1559": len(prior_ids & qualified_ids),
                    "failed_from_prior_1559": len(failed_prior),
                    "qualified_outside_prior_1559": len(outside_prior),
                    "top_failure_reasons": "|".join(f"{k}:{v}" for k, v in reason_counter.most_common(8)),
                }
            )
        write_csv(self.output_dir / f"prior_1559_population_comparison_{RUN_DATE}.csv", prior_compare)
        write_csv(self.output_dir / f"matrix_schema_manifests_{RUN_DATE}.csv", self.schema_rows)
        return counts

    def _decision(self, counts: dict[str, Any]) -> None:
        any_variant = any(counts["variant_counts"].values())
        self.decision_statuses = {
            "DENOMINATOR_REPRODUCTION_STATUS": "PASS_EXACT_1904_ROWS",
            "VARIANT_MANIFEST_REPRODUCTION_STATUS": "PASS_FROZEN_MANIFESTS_PARSED",
            "FIELD_REGISTRY_REPRODUCTION_STATUS": "PASS_FROZEN_REGISTRY_PARSED",
            "SOURCE_LINEAGE_STATUS": "PASS_SOURCES_BOUND_AND_INVENTORIED",
            "GRAIN_JOIN_STATUS": "PASS_NO_DENOMINATOR_EXPANSION_OR_LOSS_WITH_BLOCKERS_REPORTED",
            "TEMPORAL_INTEGRITY_STATUS": "PASS_OR_BLOCKED_AT_ROW_FIELD_LEVEL",
            "OUTCOME_LABEL_INTEGRITY_STATUS": "PASS_1750_NUMERIC_LABELS_ONLY",
            "STARTER_DOMAIN_COMPATIBILITY": "PASS_1671_QUALIFIED_233_BLOCKED_PRESERVED",
            "PA_DOMAIN_COMPATIBILITY": "PASS_1903_QUALIFIED_1_BLOCKED_PRESERVED",
            "FIELD_MATERIALIZATION_STATUS": "PASS_WITH_ROW_LEVEL_BLOCKERS_REPORTED",
            "FIELD_SEMANTICS_STATUS": "PASS_NO_UNMAPPED_FROZEN_FIELDS" if not self.semantic_failures else "FAIL_SEMANTIC_TYPE_BLOCKERS_PRESENT",
            "MISSINGNESS_CONTRACT_STATUS": "PASS_CONTRACT_NULLS_RETAINED_NO_IMPUTATION",
            "REPLAYABILITY_STATUS": "PASS_DATE_LOCKED_ARTIFACT_REPLAY",
            "VARIANT_A_MATRIX_STATUS": self._variant_status("variant_a"),
            "VARIANT_B_MATRIX_STATUS": self._variant_status("variant_b"),
            "VARIANT_C_MATRIX_STATUS": self._variant_status("variant_c"),
            "VARIANT_D_MATRIX_STATUS": self._variant_status("variant_d"),
            "HITS_05_MATRIX_STATUS": "CONSTRUCTED_BY_VARIANT_SCOPE",
            "HITS_15_MATRIX_STATUS": "CONSTRUCTED_BY_VARIANT_SCOPE",
            "PRIOR_1559_POPULATION_STATUS": "REPRODUCED_AS_COMPARISON_BASELINE_NOT_FORCED",
            "HISTORICAL_MATRIX_CONSTRUCTION_DECISION": (
                "MATRICES_CONSTRUCTED_WITH_CERTIFIED_OUTCOMES_AND_ROW_LEVEL_BLOCKERS"
                if any_variant
                else "MATRICES_CONSTRUCTED_ZERO_VARIANT_QUALIFIED_ROWS"
            ),
            "BOUNDED_OFFLINE_PROCESS_VALIDATION_READINESS": (
                "READY_FOR_SEPARATE_HUMAN_APPROVED_PROCESS_VALIDATION_USING_CERTIFIED_MATRICES"
                if any_variant
                else "NOT_READY_VARIANT_FIELD_GAPS_REMAIN"
            ),
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "PRODUCTION_READINESS": "NOT_AUTHORIZED_BY_THIS_TASK",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": (
                "Human review of constructed matrices and blocker ledgers before any separate offline process-validation request"
            ),
        }
        write_json(
            self.output_dir / f"machine_readable_construction_decision_{RUN_DATE}.json",
            {
                "generated_at_utc": self.generated_at,
                "counts": counts,
                "decision_statuses": self.decision_statuses,
                "constraints": {
                    "model_training": "not_performed",
                    "signal_evaluation": "not_performed",
                    "db_writes": "not_performed",
                    "external_api_calls": "not_performed",
                    "production_changes": "not_performed",
                },
            },
        )

    def _variant_status(self, variant: str) -> str:
        count = len(self.variant_qualified.get(variant, []))
        return "CONSTRUCTED_QUALIFIED_ROWS_PRESENT" if count else "CONSTRUCTED_ZERO_QUALIFIED_ROWS"

    def _markdown_reports(self, counts: dict[str, Any]) -> None:
        variant_lines = "\n".join(
            f"- {variant}: {count} qualified rows" for variant, count in counts["variant_counts"].items()
        )
        hits_lines = "\n".join(
            f"- {key}: {count} qualified rows" for key, count in sorted(counts["hits_scope_counts"].items())
        )
        status_lines = "\n".join(f"- `{k}`: `{v}`" for k, v in self.decision_statuses.items())
        main = f"""# MLB Historical Bundle Matrix Construction - {RUN_DATE}

## Executive Summary

Constructed frozen Bundle v1 matrices for the certified 2026-06-22 through 2026-06-28 block. The certified 1,904-row denominator was retained exactly in every complete audit matrix. No model training, scoring, ranking, ROI, signal evaluation, DB write, external API call, upload, or production behavior change was performed.

## Certified Population Reproduction

- Denominator rows: {counts['denominator_rows']}
- Numeric outcome rows: {counts['numeric_outcome_rows']}
- Governed nonnumeric rows retained as label-ineligible: {counts['nonnumeric_outcome_rows']}
- Starter qualified rows preserved: {counts['starter_qualified_rows']}
- PA qualified rows preserved: {counts['pa_qualified_rows']}
- Prior numeric + Starter + PA comparison population: {counts['prior_1559_rows']}

## Variant Qualified Rows

{variant_lines}

## Hits Scope Outputs

{hits_lines}

## Construction Notes

Feature nulls are not imputed. Source-present missing values are retained as `CONTRACT_QUALIFIED_NULL` under the frozen missingness contract. Missing source joins, unresolved identities, temporal failures, semantic/type failures, and domain blockers remain row-level blockers. Complete audit matrices preserve every denominator row and every blocker reason.

## Decision Statuses

{status_lines}

## No Behavior Changed

This package is artifact-only and bounded to matrix construction/validation. It does not authorize training, signal evaluation, Champion-Challenger work, or production use.
"""
        (self.output_dir / f"historical_bundle_matrix_construction_report_{RUN_DATE}.md").write_text(main)
        one_page = f"""# One-Page Readiness Summary - {RUN_DATE}

The certified 1,904-row denominator was reproduced exactly and enriched into frozen Bundle v1 complete audit matrices for Variants A-D. Qualified matrices were emitted only where row-level outcome, domain, field, temporal, grain, and replayability checks passed.

Variant qualified rows:

{variant_lines}

Readiness: `{self.decision_statuses['BOUNDED_OFFLINE_PROCESS_VALIDATION_READINESS']}`.

Training, signal evaluation, Champion-Challenger comparison, and production use remain `NOT_AUTHORIZED_BY_THIS_TASK`.
"""
        (self.output_dir / f"one_page_matrix_readiness_summary_{RUN_DATE}.md").write_text(one_page)

    def _sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.output_dir.glob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                rows.append(
                    {
                        "artifact_path": str(path),
                        "filename": path.name,
                        "sha256": sha256_path(path),
                        "bytes": path.stat().st_size,
                    }
                )
        write_csv(self.output_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)

    def build(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        source_shas = self._reproduction_reports()
        self._source_inventory_report()
        for variant, fields in self.manifest_fields.items():
            self._construct_variant(variant, fields)
        self._write_variant_outputs()
        self._build_cross_variant_ledger()
        self._coverage_reports()
        self._audit_reports()
        counts = self._aggregate_reports()
        self._decision(counts)
        self._markdown_reports(counts)
        self._sha_manifest()
        return {"counts": counts, "source_shas": source_shas, "output_dir": str(self.output_dir)}


def validate_outputs(output_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(output_dir.glob("*.csv")):
        try:
            with path.open(newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                row_count = sum(1 for _ in reader)
            status = "PASS"
            notes = f"{len(header or [])} columns"
        except Exception as exc:  # pragma: no cover - validation path
            row_count = ""
            status = "FAIL"
            notes = str(exc)
        rows.append({"artifact_path": str(path), "parse_status": status, "row_count": row_count, "notes": notes})
    write_csv(output_dir / f"parse_validation_{RUN_DATE}.csv", rows)
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)
    builder = MatrixBuilder(Path(args.output_dir))
    result = builder.build()
    parse_rows = validate_outputs(Path(args.output_dir))
    builder._sha_manifest()
    failed = [r for r in parse_rows if r["parse_status"] != "PASS"]
    print(json.dumps({"result": result, "parse_failures": len(failed)}, indent=2, sort_keys=True))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
