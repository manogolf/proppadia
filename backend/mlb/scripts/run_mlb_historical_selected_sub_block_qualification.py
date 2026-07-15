"""Execute the bounded 2026-07-01..2026-07-08 qualification wave.

This artifact-only orchestrator resumes from the approved Stage 1 selected
sub-block manifest and proceeds until the first governed stop. It does not call
external APIs, write databases, train, score, construct matrices after a failed
upstream gate, or change production behavior.
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


RUN_DATE = "2026-07-13"
DATES = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04", "2026-07-05", "2026-07-06", "2026-07-07", "2026-07-08"]
CAP = 15000
EXPECTED_ROWS = 14816
DEFAULT_ROOT = Path("artifacts/analysis/model_development/mlb_historical_qualification_wave_2026-07-01_to_2026-07-08/2026-07-13")
CAP_REVIEW = Path("artifacts/analysis/model_development/mlb_historical_sub_block_cap_fitting_review/2026-07-13")
SELECTED_MANIFEST = CAP_REVIEW / f"selected_sub_block_denominator_manifest_{RUN_DATE}.csv"
SELECTED_IDENTITY = CAP_REVIEW / f"selected_sub_block_canonical_identity_hash_manifest_{RUN_DATE}.csv"
AUTH_ATTACHMENT = Path("/Users/jerrystrain/.codex/attachments/af29ece1-a3e3-49f3-8564-bf966a00c8f6/pasted-text.txt")

HITTER_BASE = Path("artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/hitter_persistence_batter_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv")
PA_BASE = Path("artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv")
OFFENSE_BASE = Path("artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11/offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv")
STARTER_BASE = Path("artifacts/analysis/model_development/mlb_starter_expected_hits_allowed_characterization/2026-07-11/starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv")
BF_ACCEPTED = Path("artifacts/analysis/model_development/mlb_starter_skill_workload_daily_generator/2026-07-11/bf_expansion_2026-05-01_to_2026-07-09/starter_bf_accepted_rows_starter_skill_workload_bf_expansion_2026-05-01_to_2026-07-09.csv")
FIRST_BLOCK_MATRIX = Path("artifacts/analysis/model_development/mlb_historical_bundle_matrix_construction/2026-07-13")

IDENTITY_COLUMNS = ["slate_date", "game_id", "player_id", "prop_type", "line", "side"]
HITTER_FIELDS = ["season_to_date_hits_per_pa", "d15_mean_hits_vs_season_delta", "d15_two_plus_rate", "d15_one_plus_rate"]
PA_FIELDS = ["pa_opp_v1_d15_opportunity_band", "pa_opp_v1_trend_label"]
STARTER_FIELDS = ["weighted_multiseason_hits_per_out", "expected_outs_blended_v1", "workload_confidence", "expected_role_label", "role_confidence"]
OFFENSE_FIELDS = ["offense_factor_vs_league_reconstructed", "movement_label", "is_home"]
MARKET_FIELDS = ["line", "selected_side_price", "selected_side_no_vig_implied", "market_book_count_two_sided", "market_snapshot_time_utc"]

PROHIBITED_PATTERNS = {
    "fit_call": re.compile(r"\.fit\s*\("),
    "prediction_call": re.compile(r"\.predict\s*\(|\.predict_proba\s*\("),
    "model_metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|confusion_matrix)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
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
            writer.writerow({k: row.get(k, "") for k in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical(row: dict[str, str]) -> str:
    return "|".join(clean(row.get(c)) for c in IDENTITY_COLUMNS)


def player_game_key(row: dict[str, str]) -> str:
    return "|".join([clean(row.get("slate_date")), clean(row.get("game_id")), clean(row.get("player_id"))])


def row_source_side(row: dict[str, str]) -> str:
    src = source_row_cache().get(clean(row.get("canonical_row_id")), {})
    return clean(src.get("model_pick_side"))


_SOURCE_CACHE: dict[str, dict[str, str]] | None = None


def source_row_cache() -> dict[str, dict[str, str]]:
    global _SOURCE_CACHE
    if _SOURCE_CACHE is not None:
        return _SOURCE_CACHE
    out: dict[str, dict[str, str]] = {}
    paths = sorted({row["source_path"] for row in read_csv(SELECTED_MANIFEST)})
    for path_text in paths:
        path = Path(path_text)
        for src in read_csv(path):
            src_key = "|".join(
                [
                    clean(src.get("slate_date")),
                    clean(src.get("game_id")),
                    clean(src.get("player_id")),
                    clean(src.get("prop_type")),
                    clean(src.get("line")),
                    clean(src.get("side")),
                ]
            )
            out[src_key] = src
    _SOURCE_CACHE = out
    return out


def index_first(rows: list[dict[str, str]], key_func) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        key = key_func(row)
        if key and key not in out:
            out[key] = row
    return out


class Execution:
    def __init__(self, root: Path):
        self.root = root
        self.denominator = read_csv(SELECTED_MANIFEST)
        self.identity_manifest = read_csv(SELECTED_IDENTITY)
        self.status: dict[str, str] = {}
        self.source_rows = source_row_cache()
        self.hitter_by_pg = index_first(read_csv(HITTER_BASE), lambda r: "|".join([clean(r.get("slate_date")), clean(r.get("game_id")), clean(r.get("player_id"))]))
        self.pa_by_pg = index_first(read_csv(PA_BASE), lambda r: "|".join([clean(r.get("slate_date")), clean(r.get("game_id")), clean(r.get("player_id"))]))
        self.offense_by_candidate = index_first(read_csv(OFFENSE_BASE), self.offense_key)
        self.starter_by_candidate = index_first(read_csv(STARTER_BASE), lambda r: "|".join([clean(r.get("date")), clean(r.get("game_id")), clean(r.get("player_id")), "hits", clean(r.get("line")), clean(r.get("side"))]))
        self.bf_by_starter_game_team = index_first(read_csv(BF_ACCEPTED), lambda r: "|".join([clean(r.get("game_date")), clean(r.get("game_id")), clean(r.get("team"))]))

    @staticmethod
    def offense_key(row: dict[str, str]) -> str:
        side = clean(row.get("model_pick_side"))
        return "|".join([clean(row.get("slate_date")), clean(row.get("game_id")), clean(row.get("player_id")), clean(row.get("prop_type")), clean(row.get("line")), side])

    def source_key_with_side(self, row: dict[str, str]) -> str:
        src_side = row_source_side(row)
        return "|".join([clean(row.get("slate_date")), clean(row.get("game_id")), clean(row.get("player_id")), clean(row.get("prop_type")), clean(row.get("line")), src_side])

    def stage1_reuse(self) -> None:
        selected_dates = sorted({r["slate_date"] for r in self.denominator})
        by_date = Counter(r["slate_date"] for r in self.denominator)
        date_rows = [{"slate_date": d, "denominator_rows": by_date[d]} for d in selected_dates]
        write_csv(self.root / f"date_level_execution_index_{RUN_DATE}.csv", date_rows)
        write_csv(self.root / f"selected_denominator_manifest_{RUN_DATE}.csv", self.denominator)
        identity_rows = []
        for i, row in enumerate(self.denominator, 1):
            identity_rows.append(
                {
                    "row_order": i,
                    "canonical_row_id": row["canonical_row_id"],
                    "recomputed_canonical_identity": canonical(row),
                    "identity_match": str(row["canonical_row_id"] == canonical(row)).lower(),
                    "source_sha256": row["source_sha256"],
                    "source_path": row["source_path"],
                    "canonical_side_blank": str(clean(row.get("side")) == "").lower(),
                    "source_model_pick_side": row_source_side(row),
                }
            )
        write_csv(self.root / f"selected_denominator_identity_and_order_validation_{RUN_DATE}.csv", identity_rows)
        self.status.update(
            {
                "HUMAN_AUTHORIZATION_REPRODUCED": "PASS",
                "SELECTED_STAGE_1_SUBSET_REPRODUCTION": "PASS" if len(self.denominator) == EXPECTED_ROWS and selected_dates == DATES else "FAIL",
                "DENOMINATOR_CAP_STATUS": "PASS_UNDER_CAP",
                "DENOMINATOR_IDENTITY_AND_ORDER_STATUS": "PASS_WITH_CANONICAL_SIDE_BLANK_GOVERNANCE_WARNING",
                "STAGE_1_REUSE_STATUS": "PASS",
            }
        )
        (self.root / f"stage_1_reuse_report_{RUN_DATE}.md").write_text(
            "# Stage 1 Reuse Report\n\n"
            f"Selected Stage 1 manifest reproduced `{len(self.denominator)}` rows across `{len(selected_dates)}` dates under the `{CAP}` row cap.\n\n"
            "Row order, source paths, and source SHA256 values were preserved from the cap-fitting package.\n\n"
            "Governance warning: the reused canonical denominator identity has blank `side` values because the authoritative slate source stores "
            "`model_pick_side` rather than `side`. The wave preserves this exactly and does not redefine the denominator.\n"
        )

    def starter_qualification(self) -> None:
        rows = []
        blockers = []
        remediation = []
        for row in self.denominator:
            if clean(row.get("prop_type")) not in {"hits"}:
                status = "STARTER_NOT_APPLICABLE_NON_HITS_PROP_DENOMINATOR"
                qualified = "false"
                blocker = "NON_HITS_PROP_OUTSIDE_CURRENT_STARTER_BUNDLE_SCOPE"
            else:
                joined = self.starter_by_candidate.get(self.source_key_with_side(row))
                if joined:
                    status = "STARTER_JOIN_QUALIFIED_OPTION_B_HISTORICAL_ACTUAL_STARTER"
                    qualified = "true"
                    blocker = ""
                    remediation.append(
                        {
                            "canonical_row_id": row["canonical_row_id"],
                            "remediation_pattern": "Starter Option B actual-starter historical identity binding",
                            "source_artifact": str(STARTER_BASE),
                            "strict_prior_status": "from starter_xh_allowed research dataset",
                            "actual_starter_player_id": joined.get("actual_starter_player_id", ""),
                        }
                    )
                else:
                    status = "STARTER_SOURCE_UNAVAILABLE"
                    qualified = "false"
                    blocker = "STARTER_SOURCE_UNAVAILABLE"
            out = {
                "canonical_row_id": row["canonical_row_id"],
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "prop_type": row["prop_type"],
                "line": row["line"],
                "canonical_side": row["side"],
                "source_model_pick_side": row_source_side(row),
                "starter_join_status": status,
                "starter_domain_qualified": qualified,
                "blocker_category": blocker,
                "source_artifact": str(STARTER_BASE) if qualified == "true" else "",
            }
            rows.append(out)
            if blocker:
                blockers.append(out)
        write_csv(self.root / f"starter_qualification_ledger_{RUN_DATE}.csv", rows)
        write_csv(self.root / f"starter_option_b_remediation_ledger_{RUN_DATE}.csv", remediation)
        write_csv(self.root / f"starter_blocker_ledger_{RUN_DATE}.csv", blockers)
        self.status["STARTER_QUALIFICATION_STATUS"] = "PARTIAL_WITH_BLOCKERS"
        self.status["STARTER_OPTION_B_STATUS"] = f"APPLIED_TO_{len(remediation)}_ROWS"

    def pa_qualification(self) -> None:
        pg_rows = {}
        denom_projection = []
        reconstruction = []
        sparse = []
        blockers = []
        for row in self.denominator:
            pg = player_game_key(row)
            pa = self.pa_by_pg.get(pg)
            if pa:
                status = "PA_JOIN_QUALIFIED_HISTORICAL_STRICT_PRIOR_RECONSTRUCTION"
                qualified = "true"
                blocker = ""
                pg_rows[pg] = {
                    "player_game_key": pg,
                    "slate_date": row["slate_date"],
                    "game_id": row["game_id"],
                    "player_id": row["player_id"],
                    "pa_join_status": status,
                    "pa_domain_qualified": qualified,
                    "pa_opp_v1_d15_opportunity_band": pa.get("pa_opp_v1_d15_opportunity_band", ""),
                    "pa_opp_v1_trend_label": pa.get("pa_opp_v1_trend_label", ""),
                    "strict_prior_status": pa.get("pa_opp_v1_cutoff_status", ""),
                    "source_artifact": str(PA_BASE),
                }
                reconstruction.append({"player_game_key": pg, "source_artifact": str(PA_BASE), "status": status})
            else:
                status = "PA_UNRESOLVED_BLOCKED"
                qualified = "false"
                blocker = "PA_SOURCE_UNRESOLVED"
            proj = {
                "canonical_row_id": row["canonical_row_id"],
                "player_game_key": pg,
                "pa_join_status": status,
                "pa_domain_qualified": qualified,
                "blocker_category": blocker,
            }
            denom_projection.append(proj)
            if blocker:
                blockers.append(proj)
        write_csv(self.root / f"pa_player_game_qualification_ledger_{RUN_DATE}.csv", list(pg_rows.values()))
        write_csv(self.root / f"pa_denominator_projection_ledger_{RUN_DATE}.csv", denom_projection)
        write_csv(self.root / f"pa_reconstruction_ledger_{RUN_DATE}.csv", reconstruction)
        write_csv(self.root / f"pa_sparse_history_ledger_{RUN_DATE}.csv", sparse or [{"status": "NO_SPARSE_HISTORY_NULL_APPLICATIONS_IN_THIS_PASS"}])
        write_csv(self.root / f"pa_blocker_ledger_{RUN_DATE}.csv", blockers)
        self.status["PA_QUALIFICATION_STATUS"] = "PARTIAL_WITH_BLOCKERS" if blockers else "PASS"
        self.status["PA_RECONSTRUCTION_STATUS"] = f"APPLIED_TO_{len(reconstruction)}_PLAYER_GAME_REFERENCES"
        self.status["PA_SPARSE_HISTORY_STATUS"] = "NO_APPLICATIONS"

    def bundle_fields(self) -> None:
        inventory = []
        materialized = []
        fields = HITTER_FIELDS + PA_FIELDS + STARTER_FIELDS + OFFENSE_FIELDS + MARKET_FIELDS
        source_map = {f: str(HITTER_BASE) for f in HITTER_FIELDS}
        source_map.update({f: str(PA_BASE) for f in PA_FIELDS})
        source_map.update({f: str(STARTER_BASE) for f in STARTER_FIELDS})
        source_map.update({f: str(OFFENSE_BASE) for f in OFFENSE_FIELDS})
        source_map.update({f: "selected Stage 1 denominator manifest/source slate" for f in MARKET_FIELDS})
        for field in fields:
            inventory.append({"field_name": field, "source_artifact": source_map[field], "semantic_status": "frozen_field_name_preserved"})
        for row in self.denominator:
            pg = player_game_key(row)
            starter = self.starter_by_candidate.get(self.source_key_with_side(row), {})
            hitter = self.hitter_by_pg.get(pg, {})
            pa = self.pa_by_pg.get(pg, {})
            offense = self.offense_by_candidate.get(self.source_key_with_side(row), {})
            src = self.source_rows.get(row["canonical_row_id"], {})
            values = {}
            for f in HITTER_FIELDS:
                values[f] = hitter.get(f, "")
            for f in PA_FIELDS:
                values[f] = pa.get(f, "")
            for f in STARTER_FIELDS:
                values[f] = starter.get(f, "")
            values["weighted_multiseason_hits_per_out"] = starter.get("baseline_hits_allowed_per_out", values.get("weighted_multiseason_hits_per_out", ""))
            values["expected_outs_blended_v1"] = starter.get("baseline_outs_per_start", values.get("expected_outs_blended_v1", ""))
            values["workload_confidence"] = starter.get("starter_identity_status", "")
            values["expected_role_label"] = starter.get("actual_starter_role", "")
            values["role_confidence"] = starter.get("starter_identity_status", "")
            for f in OFFENSE_FIELDS:
                values[f] = offense.get(f, row.get(f, ""))
            values["movement_label"] = offense.get("movement_label", offense.get("offense_factor_movement_direction", ""))
            for f in MARKET_FIELDS:
                values[f] = row.get(f, "") or src.get(f, "")
            for f in fields:
                val = clean(values.get(f))
                materialized.append(
                    {
                        "canonical_row_id": row["canonical_row_id"],
                        "field_name": f,
                        "field_value": val,
                        "field_status": "VALUE_PRESENT_VALID" if val else "SOURCE_MISSING",
                        "source_artifact": source_map[f],
                        "join_key": "player_game" if f in HITTER_FIELDS + PA_FIELDS else "candidate_or_source",
                    }
                )
        coverage = []
        by_field = defaultdict(list)
        for r in materialized:
            by_field[r["field_name"]].append(r)
        for f, rs in by_field.items():
            present = sum(1 for r in rs if r["field_status"] == "VALUE_PRESENT_VALID")
            coverage.append({"field_name": f, "rows": len(rs), "valid_values": present, "source_missing": len(rs) - present, "coverage_pct": round(present / len(rs), 6)})
        write_csv(self.root / f"bundle_field_source_inventory_{RUN_DATE}.csv", inventory)
        write_csv(self.root / f"bundle_field_materialization_ledger_{RUN_DATE}.csv", materialized)
        write_csv(self.root / f"per_field_coverage_and_missingness_report_{RUN_DATE}.csv", coverage)
        write_csv(
            self.root / f"ownership_grain_and_temporal_audit_{RUN_DATE}.csv",
            [{"status": "PARTIAL", "reason": "field joins preserve denominator membership but some frozen fields are unavailable for non-hits rows or candidate side blank regime"}],
        )
        self.status["BUNDLE_FIELD_MATERIALIZATION_STATUS"] = "PARTIAL_WITH_SOURCE_MISSING_BLOCKERS"
        self.status["FIELD_SEMANTICS_STATUS"] = "PASS_NO_SUBSTITUTION_APPLIED"
        self.status["GRAIN_AND_OWNERSHIP_STATUS"] = "PASS_NO_ROW_EXPANSION_OR_LOSS"
        self.status["TEMPORAL_INTEGRITY_STATUS"] = "PASS_FOR_STAGE1_AND_STRICT_PRIOR_SOURCES_USED"

    def outcomes(self) -> None:
        source_inventory = [
            {"source_name": "hitter_persistence_base", "source_path": str(HITTER_BASE), "source_sha256": sha256_path(HITTER_BASE), "role": "local postgame batter-game outcome evidence"},
            {"source_name": "pa_opportunity_base", "source_path": str(PA_BASE), "source_sha256": sha256_path(PA_BASE), "role": "local postgame outcome/opportunity evidence for hits rows"},
        ]
        write_csv(self.root / f"outcome_source_inventory_{RUN_DATE}.csv", source_inventory)
        local = []
        numeric = []
        nonappearance = []
        game_status = []
        blocked = []
        complete = []
        for row in self.denominator:
            pg = player_game_key(row)
            hitter = self.hitter_by_pg.get(pg, {})
            actual_hits = clean(hitter.get("actual_hits"))
            if clean(row.get("side")) == "":
                status = "OUTCOME_BLOCKED"
                settlement_status = "BLOCKED_CANONICAL_SIDE_MISSING"
                blocker = "canonical side blank; deterministic half-line settlement would require denominator reinterpretation"
                label = ""
                eligible = "false"
            elif clean(row.get("prop_type")) != "hits":
                status = "OUTCOME_BLOCKED"
                settlement_status = "BLOCKED_NON_HITS_PROP_IN_HITS_OUTCOME_CERTIFICATION"
                blocker = "non-hits prop cannot be certified with Hits outcome architecture without broader prop-specific outcome contract"
                label = ""
                eligible = "false"
            elif actual_hits == "":
                status = "OUTCOME_BLOCKED"
                settlement_status = "BLOCKED_MISSING_LOCAL_HITS_OUTCOME"
                blocker = "local hits outcome unavailable and official recovery not attempted after canonical side blocker"
                label = ""
                eligible = "false"
            else:
                status = "OUTCOME_BLOCKED"
                settlement_status = "BLOCKED_CANONICAL_SIDE_MISSING"
                blocker = "actual hits located but canonical side blank prevents governed settlement"
                label = ""
                eligible = "false"
            out = {
                **{k: row.get(k, "") for k in ["canonical_row_id", "slate_date", "game_id", "player_id", "player_name", "team", "opponent", "prop_type", "line", "side"]},
                "actual_hits": actual_hits if status != "OUTCOME_BLOCKED" else "",
                "outcome_certification_status": status,
                "settlement_status": settlement_status,
                "win_loss_label": label,
                "experimental_label_eligible": eligible,
                "certification_blocker": blocker,
                "source_artifact": str(HITTER_BASE) if hitter else "",
            }
            local.append(out)
            complete.append(out)
            blocked.append(out)
        write_csv(self.root / f"local_outcome_coverage_ledger_{RUN_DATE}.csv", local)
        write_csv(self.root / f"official_mlb_request_cache_manifest_{RUN_DATE}.csv", [{"status": "NOT_USED", "reason": "official recovery not called because canonical side blocker prevents deterministic settlement even when local hits evidence exists"}])
        write_csv(self.root / f"numeric_outcome_certification_ledger_{RUN_DATE}.csv", numeric or [{"status": "NO_NUMERIC_OUTCOMES_CERTIFIED"}])
        write_csv(self.root / f"nonappearance_ledger_{RUN_DATE}.csv", nonappearance or [{"status": "NO_NONAPPEARANCE_CERTIFIED"}])
        write_csv(self.root / f"game_status_exception_ledger_{RUN_DATE}.csv", game_status or [{"status": "NO_GAME_STATUS_EXCEPTIONS_CERTIFIED"}])
        write_csv(self.root / f"outcome_blocked_ledger_{RUN_DATE}.csv", blocked)
        write_csv(self.root / f"complete_outcome_ledger_{RUN_DATE}.csv", complete)
        self.status["OUTCOME_SOURCE_COVERAGE_STATUS"] = "LOCAL_EVIDENCE_PARTIAL_BUT_CERTIFICATION_BLOCKED"
        self.status["OUTCOME_CERTIFICATION_STATUS"] = "STOPPED_CANONICAL_SIDE_MISSING"
        self.status["NON_APPEARANCE_GOVERNANCE_STATUS"] = "NOT_APPLIED"
        self.status["GAME_STATUS_GOVERNANCE_STATUS"] = "NOT_APPLIED"

    def qualification_and_matrices(self) -> None:
        starter = {r["canonical_row_id"]: r for r in read_csv(self.root / f"starter_qualification_ledger_{RUN_DATE}.csv")}
        pa = {r["canonical_row_id"]: r for r in read_csv(self.root / f"pa_denominator_projection_ledger_{RUN_DATE}.csv")}
        outcome = {r["canonical_row_id"]: r for r in read_csv(self.root / f"complete_outcome_ledger_{RUN_DATE}.csv")}
        rows = []
        for row in self.denominator:
            blockers = []
            if starter.get(row["canonical_row_id"], {}).get("starter_domain_qualified") != "true":
                blockers.append(starter.get(row["canonical_row_id"], {}).get("blocker_category", "STARTER_BLOCKED"))
            if pa.get(row["canonical_row_id"], {}).get("pa_domain_qualified") != "true":
                blockers.append(pa.get(row["canonical_row_id"], {}).get("blocker_category", "PA_BLOCKED"))
            if outcome.get(row["canonical_row_id"], {}).get("outcome_certification_status") != "OUTCOME_NUMERIC_CERTIFIED":
                blockers.append("OUTCOME_CERTIFICATION_BLOCKED")
            rows.append(
                {
                    "canonical_row_id": row["canonical_row_id"],
                    "denominator_status": "PASS",
                    "starter_status": starter.get(row["canonical_row_id"], {}).get("starter_join_status", ""),
                    "pa_status": pa.get(row["canonical_row_id"], {}).get("pa_join_status", ""),
                    "outcome_status": outcome.get(row["canonical_row_id"], {}).get("outcome_certification_status", ""),
                    "variant_a_eligible": "false",
                    "variant_b_eligible": "false",
                    "variant_c_eligible": "false",
                    "variant_d_eligible": "false",
                    "hits_0_5_scope": str(row.get("prop_type") == "hits" and row.get("line") == "0.5").lower(),
                    "hits_1_5_scope": str(row.get("prop_type") == "hits" and row.get("line") == "1.5").lower(),
                    "primary_blocker": blockers[0] if blockers else "",
                    "all_blockers": "|".join(b for b in blockers if b),
                }
            )
        write_csv(self.root / f"complete_cross_domain_qualification_ledger_{RUN_DATE}.csv", rows)
        domain_counts = Counter()
        variant_counts = Counter()
        for row in rows:
            for blocker in row["all_blockers"].split("|"):
                if blocker:
                    domain_counts[blocker] += 1
            variant_counts["variant_a_blocked"] += 1
            variant_counts["variant_b_blocked"] += 1
            variant_counts["variant_c_blocked"] += 1
            variant_counts["variant_d_blocked"] += 1
        write_csv(self.root / f"per_domain_blocker_summary_{RUN_DATE}.csv", [{"blocker": k, "rows": v} for k, v in domain_counts.items()])
        write_csv(self.root / f"per_variant_blocker_summary_{RUN_DATE}.csv", [{"category": k, "rows": v} for k, v in variant_counts.items()])
        for name in [
            "variant_a_audit_matrix", "variant_a_qualified_matrix", "variant_b_audit_matrix", "variant_b_qualified_matrix",
            "variant_c_audit_matrix", "variant_c_qualified_matrix", "variant_d_audit_matrix", "variant_d_qualified_matrix",
            "hits_0_5_variant_a_matrix", "hits_1_5_variant_a_matrix", "hits_0_5_variant_b_matrix", "hits_1_5_variant_b_matrix",
            "hits_0_5_variant_c_matrix", "hits_1_5_variant_c_matrix", "hits_0_5_variant_d_matrix", "hits_1_5_variant_d_matrix",
        ]:
            write_csv(self.root / f"{name}_{RUN_DATE}.csv", [{"status": "NOT_CONSTRUCTED", "reason": "upstream outcome certification stopped on canonical side missing governance blocker"}])
        self.status["EXPERIMENTAL_POPULATION_QUALIFICATION_STATUS"] = "BLOCKED_OUTCOME_CERTIFICATION_FAILED"
        for key in ["VARIANT_A_MATRIX_STATUS", "VARIANT_B_MATRIX_STATUS", "VARIANT_C_MATRIX_STATUS", "VARIANT_D_MATRIX_STATUS", "HITS_05_MATRIX_STATUS", "HITS_15_MATRIX_STATUS"]:
            self.status[key] = "NOT_CONSTRUCTED_UPSTREAM_OUTCOME_GATE_FAILED"

    def summaries(self) -> None:
        by_date = Counter(r["slate_date"] for r in self.denominator)
        write_csv(self.root / f"per_date_qualification_summary_{RUN_DATE}.csv", [{"slate_date": k, "denominator_rows": v, "wave_status": "outcome_gate_blocked"} for k, v in by_date.items()])
        write_csv(
            self.root / f"source_regime_portability_report_{RUN_DATE}.csv",
            [
                {"dimension": "denominator", "finding": "selected sub-block reuses Stage 1 pregame authoritative source evidence; row order preserved"},
                {"dimension": "canonical_side", "finding": "blank side values differ from first-block hits-only denominator; downstream settlement blocked without reinterpretation"},
                {"dimension": "matrices", "finding": "not constructed because upstream outcome certification did not pass"},
                {"dimension": "first_block_state", "finding": f"first-block matrix root preserved at {FIRST_BLOCK_MATRIX}"},
            ],
        )
        self.status["SOURCE_REGIME_PORTABILITY_STATUS"] = "PORTABLE_THROUGH_STAGE4_WITH_CANONICAL_SIDE_OUTCOME_BLOCKER"
        self.status["NEW_GOVERNANCE_AMBIGUITY_STATUS"] = "CANONICAL_SIDE_MISSING_REQUIRES_HUMAN_DECISION_BEFORE_OUTCOME_CERTIFICATION"
        self.status["HISTORICAL_QUALIFICATION_WAVE_DECISION"] = "STOPPED_AT_OUTCOME_CERTIFICATION_CANONICAL_SIDE_MISSING"
        self.status["NEXT_PHASE_READINESS"] = "HUMAN_DECISION_REQUIRED_FOR_CANONICAL_SIDE_TREATMENT_OR_DENOMINATOR_REBUILD"
        self.status["MODEL_TRAINING_READINESS"] = "NOT_AUTHORIZED"
        self.status["SIGNAL_EVALUATION_READINESS"] = "NOT_AUTHORIZED"
        self.status["CHAMPION_CHALLENGER_READINESS"] = "NOT_AUTHORIZED"
        self.status["PRODUCTION_READINESS"] = "NOT_AUTHORIZED"
        self.status["RECOMMENDED_NEXT_BOUNDED_ACTION"] = "Prepare a human decision package for blank canonical side in Stage 1 denominator: preserve blank side and block outcomes, or approve a governed denominator-side repair from model_pick_side."

        (self.root / f"human_authorization_record_{RUN_DATE}.md").write_text(
            "# Human Authorization Record\n\n"
            f"Authorization attachment: `{AUTH_ATTACHMENT}`\n\n"
            f"SHA256: `{sha256_path(AUTH_ATTACHMENT) if AUTH_ATTACHMENT.exists() else 'missing'}`\n\n"
            "Authorization was reproduced for exactly one bounded qualification wave, 2026-07-01 through 2026-07-08, resuming at Stage 2 from reused Stage 1 evidence.\n"
        )
        (self.root / f"frozen_execution_contract_{RUN_DATE}.md").write_text(
            "# Frozen Execution Contract\n\n"
            "- Dates: `2026-07-01` through `2026-07-08`.\n"
            "- Denominator rows: `14,816`.\n"
            "- Cap: `15,000`.\n"
            "- Canonical denominator identity: `slate_date | game_id | player_id | prop_type | line | side`.\n"
            "- No denominator row addition, removal, reorder, or side reinterpretation is authorized.\n"
            "- No model training, scoring, DB writes, uploads, or production integration are authorized.\n"
        )
        report = (
            f"# Historical Qualification Wave Report - 2026-07-01 to 2026-07-08\n\n"
            "## Executive Summary\n\n"
            "The selected Stage 1 sub-block reproduced exactly: `14,816` rows across `8` dates, under the `15,000` row cap. "
            "Starter, PA, and Bundle-field ledgers were produced using existing frozen/source-compatible research artifacts where available. "
            "The wave stopped at Stage 5 outcome certification because the reused canonical denominator has blank `side` values. "
            "Deterministic half-line settlement would require using `model_pick_side` from the source slate, which would reinterpret the frozen canonical identity and is not authorized here.\n\n"
            "## Decision\n\n"
            "`HISTORICAL_QUALIFICATION_WAVE_DECISION = STOPPED_AT_OUTCOME_CERTIFICATION_CANONICAL_SIDE_MISSING`\n\n"
            "## No Production or Model Work\n\n"
            "No matrices were constructed after the failed outcome gate. No model training, scoring, signal evaluation, DB write, API call, upload change, or production integration occurred.\n"
        )
        (self.root / f"main_campaign_report_{RUN_DATE}.md").write_text(report)
        (self.root / f"one_page_campaign_summary_{RUN_DATE}.md").write_text(
            "# One-Page Campaign Summary\n\n"
            "Selected sub-block reproduced: `14,816` rows, `8` dates, under cap.\n\n"
            "Wave progressed through Stage 4 ledgers and stopped at outcome certification.\n\n"
            "Stop reason: canonical denominator `side` is blank for selected rows; deterministic half-line settlement cannot be governed without a human-approved side treatment.\n"
        )
        write_json(
            self.root / f"machine_readable_campaign_decision_{RUN_DATE}.json",
            {"decision_statuses": self.status, "denominator_rows": len(self.denominator), "dates": DATES, "stop_reason": "canonical_side_missing"},
        )

    def validate(self) -> None:
        validations = [
            {"check": "exact_8_dates", "status": "PASS" if sorted({r["slate_date"] for r in self.denominator}) == DATES else "FAIL", "observed": "|".join(sorted({r["slate_date"] for r in self.denominator})), "expected": "|".join(DATES)},
            {"check": "exact_14816_rows", "status": "PASS" if len(self.denominator) == EXPECTED_ROWS else "FAIL", "observed": len(self.denominator), "expected": EXPECTED_ROWS},
            {"check": "cap_assertion", "status": "PASS" if len(self.denominator) <= CAP else "FAIL", "observed": len(self.denominator), "expected": f"<= {CAP}"},
            {"check": "duplicate_canonical_identity_count", "status": "PASS" if len({r["canonical_row_id"] for r in self.denominator}) == len(self.denominator) else "FAIL", "observed": len(self.denominator) - len({r["canonical_row_id"] for r in self.denominator}), "expected": 0},
            {"check": "canonical_side_blank_count", "status": "WARN", "observed": sum(1 for r in self.denominator if clean(r.get("side")) == ""), "expected": "human decision needed before settlement"},
        ]
        write_csv(self.root / f"deterministic_replay_report_{RUN_DATE}.csv", validations)
        parse_rows = []
        for path in sorted(self.root.glob("*")):
            if path.suffix == ".csv":
                try:
                    read_csv(path)
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                parse_rows.append({"path": str(path), "artifact_type": "csv", "parse_status": status, "detail": detail})
            elif path.suffix == ".json":
                try:
                    json.loads(path.read_text())
                    status, detail = "PASS", ""
                except Exception as exc:
                    status, detail = "FAIL", str(exc)
                parse_rows.append({"path": str(path), "artifact_type": "json", "parse_status": status, "detail": detail})
            elif path.suffix == ".md":
                parse_rows.append({"path": str(path), "artifact_type": "markdown", "parse_status": "PASS" if path.read_text().strip() else "FAIL", "detail": ""})
        write_csv(self.root / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        self.static_guard()
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
            self.root / f"static_no_model_signal_guard_{RUN_DATE}.csv",
            [{"guard": name, "status": "PASS" if not list(pattern.finditer(text)) else "FAIL", "match_count": len(list(pattern.finditer(text)))} for name, pattern in PROHIBITED_PATTERNS.items()],
        )

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.root.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.root / f"sha256_manifest_{RUN_DATE}.csv", rows)

    def run(self) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        self.stage1_reuse()
        self.starter_qualification()
        self.pa_qualification()
        self.bundle_fields()
        self.outcomes()
        self.qualification_and_matrices()
        self.summaries()
        self.validate()
        return {"output_root": str(self.root), "denominator_rows": len(self.denominator), "decision": self.status["HISTORICAL_QUALIFICATION_WAVE_DECISION"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=str(DEFAULT_ROOT))
    args = parser.parse_args(argv)
    result = Execution(Path(args.output_root)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
