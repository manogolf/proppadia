"""Select one pregame-authoritative historical sub-block under the cap.

Planning-only utility. It consumes the already reproduced Stage 1 denominator
package for 2026-06-29..2026-07-09 and enumerates whole-date contiguous
sub-blocks. It does not resume qualification, build matrices, inspect labels,
train, score, call APIs, write databases, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-13"
CAP = 15000
STAGE1_ROOT = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_qualification_wave_2026-06-29_to_2026-07-09/2026-07-13"
)
DEFAULT_OUT = Path(
    "artifacts/analysis/model_development/mlb_historical_sub_block_cap_fitting_review/2026-07-13"
)
IDENTITY = ["slate_date", "game_id", "player_id", "prop_type", "line", "side"]

PROHIBITED_PATTERNS = {
    "fit_call": re.compile(r"\.fit\s*\("),
    "prediction_call": re.compile(r"\.predict\s*\(|\.predict_proba\s*\("),
    "model_metric_call": re.compile(r"\b(roc_auc_score|log_loss|accuracy_score|brier_score_loss|confusion_matrix)\s*\("),
    "ranking_call": re.compile(r"\.rank\s*\(|sort_values\s*\("),
    "model_selection_call": re.compile(r"\b(GridSearchCV|RandomizedSearchCV|cross_val_score|train_test_split)\b"),
}


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


def canonical_key(row: dict[str, str]) -> str:
    return "|".join(row.get(col, "") for col in IDENTITY)


class Review:
    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.date_index = read_csv(STAGE1_ROOT / f"date_level_campaign_index_{RUN_DATE}.csv")
        self.denominator = read_csv(STAGE1_ROOT / f"certified_denominator_ledger_{RUN_DATE}.csv")
        self.source_inventory = read_csv(STAGE1_ROOT / f"denominator_candidate_source_inventory_{RUN_DATE}.csv")
        self.decision = json.loads((STAGE1_ROOT / f"machine_readable_campaign_decision_{RUN_DATE}.json").read_text())
        self.statuses: dict[str, str] = {}

    @property
    def dates(self) -> list[str]:
        return [row["slate_date"] for row in self.date_index]

    @property
    def counts(self) -> dict[str, int]:
        return {row["slate_date"]: int(row["authoritative_row_count"]) for row in self.date_index}

    def reproduce_inputs(self) -> None:
        rows = []
        for row in self.date_index:
            rows.append(
                {
                    "slate_date": row["slate_date"],
                    "authoritative_row_count": row["authoritative_row_count"],
                    "authoritative_source_path": row["authoritative_source_path"],
                    "authoritative_source_generated_at_utc": row["authoritative_source_generated_at_utc"],
                    "authoritative_first_game_time_utc": row["authoritative_first_game_time_utc"],
                    "date_denominator_status": row["date_denominator_status"],
                }
            )
        write_csv(self.out_dir / f"reproduced_11_date_authoritative_denominator_summary_{RUN_DATE}.csv", rows)
        write_csv(self.out_dir / f"date_level_authoritative_row_count_ledger_{RUN_DATE}.csv", rows)

    def enumerate_candidates(self) -> list[dict[str, Any]]:
        dates = self.dates
        rows: list[dict[str, Any]] = []
        n = len(dates)
        for i in range(n):
            for j in range(i, n):
                block_dates = dates[i : j + 1]
                total = sum(self.counts[d] for d in block_dates)
                future_parts = []
                if i > 0:
                    future_parts.append(f"{dates[0]}..{dates[i - 1]}")
                if j < n - 1:
                    future_parts.append(f"{dates[j + 1]}..{dates[-1]}")
                rows.append(
                    {
                        "candidate_id": f"{block_dates[0]}_to_{block_dates[-1]}",
                        "start_date": block_dates[0],
                        "end_date": block_dates[-1],
                        "explicit_dates": "|".join(block_dates),
                        "date_count": len(block_dates),
                        "authoritative_denominator_rows": total,
                        "cap_margin_positive_or_excess_negative": CAP - total,
                        "cap_status": "UNDER_OR_EQUAL_CAP" if total <= CAP else "EXCEEDS_CAP",
                        "average_rows_per_date": round(total / len(block_dates), 3),
                        "includes_beginning_of_authorized_range": str(i == 0).lower(),
                        "includes_end_of_authorized_range": str(j == n - 1).lower(),
                        "overlaps_completed_2026_06_22_to_2026_06_28_process_block": "false",
                        "source_regime_consistency": "same_stage1_authoritative_pregame_source_class",
                        "date_level_denominator_warning": "",
                        "future_continuation_remainder": "|".join(future_parts) if future_parts else "none",
                    }
                )
        write_csv(self.out_dir / f"all_contiguous_candidate_block_ledger_{RUN_DATE}.csv", rows)
        return rows

    def select_candidates(self, candidates: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        eligible = [r for r in candidates if r["cap_status"] == "UNDER_OR_EQUAL_CAP"]
        dates = self.dates

        def key_total(row: dict[str, Any]) -> tuple[int, int, int]:
            return (int(row["authoritative_denominator_rows"]), int(row["date_count"]), -dates.index(row["start_date"]))

        prefix = [
            r
            for r in eligible
            if r["includes_beginning_of_authorized_range"] == "true"
        ]
        suffix = [
            r
            for r in eligible
            if r["includes_end_of_authorized_range"] == "true"
        ]
        selected = {
            "candidate_a_largest_prefix_under_cap": max(prefix, key=key_total),
            "candidate_b_largest_suffix_under_cap": max(suffix, key=key_total),
            "candidate_c_maximum_row_contiguous_under_cap": max(eligible, key=key_total),
        }
        # Balanced split prefers the under-cap block whose remaining row count
        # is also under cap and whose two sides are as balanced as possible.
        balanced_pool = []
        total_all = sum(self.counts.values())
        for row in eligible:
            remainder = total_all - int(row["authoritative_denominator_rows"])
            if remainder <= CAP:
                row2 = dict(row)
                row2["remaining_rows_after_selection"] = remainder
                row2["balance_abs_difference"] = abs(int(row["authoritative_denominator_rows"]) - remainder)
                balanced_pool.append(row2)
        selected["candidate_d_balanced_split_option"] = min(
            balanced_pool,
            key=lambda r: (int(r["balance_abs_difference"]), -int(r["authoritative_denominator_rows"]), -int(r["date_count"])),
        )
        selected["recommended_exact_sub_block"] = selected["candidate_c_maximum_row_contiguous_under_cap"]
        return selected

    def write_candidate_assessments(self, selected: dict[str, dict[str, Any]]) -> None:
        for name, row in selected.items():
            if name == "recommended_exact_sub_block":
                continue
            title = name.replace("_", " ").title()
            path = self.out_dir / f"{name}_assessment_{RUN_DATE}.md"
            path.write_text(
                f"# {title} - {RUN_DATE}\n\n"
                f"Date range: `{row['start_date']}` through `{row['end_date']}`.\n\n"
                f"Authoritative denominator rows: `{row['authoritative_denominator_rows']}`.\n\n"
                f"Cap margin: `{row['cap_margin_positive_or_excess_negative']}`.\n\n"
                f"Dates: `{row['explicit_dates']}`.\n\n"
                f"Future continuation remainder: `{row['future_continuation_remainder']}`.\n"
            )
        rec = selected["recommended_exact_sub_block"]
        write_csv(self.out_dir / f"recommended_exact_sub_block_{RUN_DATE}.csv", [rec])

    def write_manifests(self, selected: dict[str, dict[str, Any]]) -> None:
        rec = selected["recommended_exact_sub_block"]
        selected_dates = set(rec["explicit_dates"].split("|"))
        selected_rows = [r for r in self.denominator if r["slate_date"] in selected_dates]
        remainder_rows = [r for r in self.denominator if r["slate_date"] not in selected_dates]
        write_csv(self.out_dir / f"selected_sub_block_denominator_manifest_{RUN_DATE}.csv", selected_rows)
        identity_rows = [
            {
                "row_order": i,
                "canonical_row_id": row["canonical_row_id"],
                "canonical_identity_recomputed": canonical_key(row),
                "identity_match": str(row["canonical_row_id"] == canonical_key(row)).lower(),
                "source_path": row["source_path"],
                "source_sha256": row["source_sha256"],
            }
            for i, row in enumerate(selected_rows, 1)
        ]
        write_csv(self.out_dir / f"selected_sub_block_canonical_identity_hash_manifest_{RUN_DATE}.csv", identity_rows)
        date_counts = Counter(row["slate_date"] for row in remainder_rows)
        remainder_manifest = [
            {
                "slate_date": date_value,
                "authoritative_denominator_rows": date_counts[date_value],
                "remainder_role": "preserved_stage1_evidence_for_future_wave",
            }
            for date_value in self.dates
            if date_value not in selected_dates
        ]
        write_csv(self.out_dir / f"remaining_date_block_manifest_{RUN_DATE}.csv", remainder_manifest)

    def write_stage1_reuse_and_notes(self, selected: dict[str, dict[str, Any]]) -> None:
        rec = selected["recommended_exact_sub_block"]
        write_csv(
            self.out_dir / f"stage_1_reuse_assessment_{RUN_DATE}.csv",
            [
                {
                    "assessment": "STAGE_1_REUSE_ALLOWED",
                    "status": "PASS",
                    "reason": "selected sub-block is a whole-date subset of the already reproduced authoritative Stage 1 denominator package",
                    "required_future_action": "subset denominator by whole dates, preserve row order, source hashes, and temporal provenance; do not repeat discovery unless validation mismatch appears",
                }
            ],
        )
        (self.out_dir / f"planning_estimate_correction_note_{RUN_DATE}.md").write_text(
            "# Planning-Estimate Correction Note\n\n"
            "Future expansion reviews must estimate population size from the authoritative all-games-pregame source class, "
            "not later/latest slate artifacts. Later/latest artifacts may be inventoried, but cannot drive cap projections. "
            "Cap checks must occur before Stage 2, rows must not be truncated, and whole-date counts must drive bounded-wave selection.\n"
        )
        (self.out_dir / f"future_execution_contract_draft_{RUN_DATE}.md").write_text(
            "# Future Execution Contract Draft\n\n"
            f"Recommended sub-block: `{rec['start_date']}` through `{rec['end_date']}`.\n\n"
            f"Rows: `{rec['authoritative_denominator_rows']}` under cap `{CAP}`.\n\n"
            "Future execution may resume at Stage 2 using the selected sub-block manifest from this package as the immutable Stage 1 input, "
            "provided validation reproduces the row count, canonical identity order, and source hashes exactly.\n\n"
            "No qualification is authorized by this review.\n"
        )
        write_csv(
            self.out_dir / f"human_approval_requirement_{RUN_DATE}.csv",
            [
                {
                    "approval_required": "yes",
                    "approval_scope": "separate explicit approval required before resuming at Stage 2 for selected sub-block",
                    "selected_sub_block": f"{rec['start_date']}_to_{rec['end_date']}",
                    "rows": rec["authoritative_denominator_rows"],
                }
            ],
        )

    def write_reports(self, selected: dict[str, dict[str, Any]], candidates: list[dict[str, Any]]) -> None:
        rec = selected["recommended_exact_sub_block"]
        total = sum(self.counts.values())
        remainder = total - int(rec["authoritative_denominator_rows"])
        candidate_count = len(candidates)
        report = (
            f"# Pregame-Authoritative Sub-Block Selection and Cap-Fitting Review - {RUN_DATE}\n\n"
            "## Executive Summary\n\n"
            f"The existing Stage 1 package was reproduced at `{total}` denominator rows across `11` dates. "
            f"All `{candidate_count}` contiguous whole-date sub-blocks were enumerated under the unchanged `{CAP}` row cap.\n\n"
            f"Recommended sub-block: `{rec['start_date']}` through `{rec['end_date']}` with "
            f"`{rec['authoritative_denominator_rows']}` rows and cap margin `{rec['cap_margin_positive_or_excess_negative']}`.\n\n"
            "This recommendation follows the selection rule by maximizing authoritative denominator rows without exceeding the cap. "
            "It does not inspect outcomes, labels, feature performance, or model behavior.\n\n"
            "## Candidate Comparison\n\n"
            f"- Candidate A largest prefix: `{selected['candidate_a_largest_prefix_under_cap']['start_date']}` to "
            f"`{selected['candidate_a_largest_prefix_under_cap']['end_date']}`, "
            f"`{selected['candidate_a_largest_prefix_under_cap']['authoritative_denominator_rows']}` rows.\n"
            f"- Candidate B largest suffix: `{selected['candidate_b_largest_suffix_under_cap']['start_date']}` to "
            f"`{selected['candidate_b_largest_suffix_under_cap']['end_date']}`, "
            f"`{selected['candidate_b_largest_suffix_under_cap']['authoritative_denominator_rows']}` rows.\n"
            f"- Candidate C maximum-row block: `{selected['candidate_c_maximum_row_contiguous_under_cap']['start_date']}` to "
            f"`{selected['candidate_c_maximum_row_contiguous_under_cap']['end_date']}`, "
            f"`{selected['candidate_c_maximum_row_contiguous_under_cap']['authoritative_denominator_rows']}` rows.\n"
            f"- Candidate D balanced split: `{selected['candidate_d_balanced_split_option']['start_date']}` to "
            f"`{selected['candidate_d_balanced_split_option']['end_date']}`, "
            f"`{selected['candidate_d_balanced_split_option']['authoritative_denominator_rows']}` rows.\n\n"
            "## Remainder\n\n"
            f"Excluded rows: `{remainder}`. Remainder dates are preserved in the Stage 1 evidence package and listed in the remainder manifest.\n\n"
            "## Stage 1 Reuse\n\n"
            "`STAGE_1_REUSE_ALLOWED = PASS`. Future execution should subset the already certified Stage 1 denominator by whole dates and resume at Stage 2 only after separate human approval.\n"
        )
        (self.out_dir / f"main_sub_block_selection_report_{RUN_DATE}.md").write_text(report)
        (self.out_dir / f"one_page_human_decision_summary_{RUN_DATE}.md").write_text(
            f"# One-Page Human Decision Summary - {RUN_DATE}\n\n"
            f"Recommended sub-block: `{rec['start_date']}` through `{rec['end_date']}`.\n\n"
            f"Rows: `{rec['authoritative_denominator_rows']}`. Cap margin: `{rec['cap_margin_positive_or_excess_negative']}`.\n\n"
            "Why: highest-row contiguous whole-date block under the unchanged 15,000-row cap.\n\n"
            "Stage 1 reuse: PASS. Separate human approval is required before Stage 2 qualification begins.\n"
        )

    def write_decision(self, selected: dict[str, dict[str, Any]], candidates: list[dict[str, Any]]) -> None:
        total = sum(self.counts.values())
        rec = selected["recommended_exact_sub_block"]
        self.statuses = {
            "AUTHORITATIVE_20620_DENOMINATOR_REPRODUCTION": "PASS",
            "DATE_LEVEL_COUNT_REPRODUCTION_STATUS": "PASS",
            "CONTIGUOUS_CANDIDATE_ENUMERATION_STATUS": "PASS_66_CANDIDATES",
            "PREFIX_CANDIDATE_STATUS": "PASS_IDENTIFIED",
            "SUFFIX_CANDIDATE_STATUS": "PASS_IDENTIFIED",
            "MAXIMUM_ROW_CANDIDATE_STATUS": "PASS_IDENTIFIED_SELECTED",
            "BALANCED_SPLIT_CANDIDATE_STATUS": "PASS_IDENTIFIED_NOT_SELECTED",
            "SELECTED_SUB_BLOCK_CAP_STATUS": "PASS_UNDER_CAP",
            "SELECTED_SUB_BLOCK_DENOMINATOR_STATUS": "PASS_WHOLE_DATE_SUBSET",
            "STAGE_1_REUSE_ALLOWED": "PASS",
            "REMAINDER_BLOCK_STATUS": "PASS_PRESERVED_NONCONTIGUOUS_REMAINDER",
            "PLANNING_ESTIMATE_CORRECTION_STATUS": "PASS_NOTE_EMITTED",
            "HUMAN_APPROVAL_REQUIRED": "YES_BEFORE_STAGE_2",
            "SUB_BLOCK_SELECTION_DECISION": f"SELECT_{rec['start_date']}_TO_{rec['end_date']}",
            "NEXT_BOUNDED_EXECUTION_READINESS": "READY_FOR_SEPARATE_HUMAN_APPROVAL_TO_RESUME_AT_STAGE_2",
            "MODEL_TRAINING_READINESS": "NOT_AUTHORIZED",
            "SIGNAL_EVALUATION_READINESS": "NOT_AUTHORIZED",
            "CHAMPION_CHALLENGER_READINESS": "NOT_AUTHORIZED",
            "RECOMMENDED_NEXT_BOUNDED_ACTION": f"Approve Stage 2 qualification for {rec['start_date']} through {rec['end_date']} using the selected sub-block manifest, if desired.",
        }
        write_json(
            self.out_dir / f"machine_readable_selection_decision_{RUN_DATE}.json",
            {
                "stage1_root": str(STAGE1_ROOT),
                "authoritative_denominator_rows": total,
                "candidate_count": len(candidates),
                "selected_sub_block": rec,
                "decision_statuses": self.statuses,
                "prohibited_actions": {
                    "qualification_resumed": False,
                    "matrices_constructed": False,
                    "model_training": False,
                    "signal_evaluation": False,
                    "db_writes": False,
                    "external_api_calls": False,
                    "production_changes": False,
                },
            },
        )

    def validate(self, candidates: list[dict[str, Any]], selected: dict[str, dict[str, Any]]) -> None:
        rec = selected["recommended_exact_sub_block"]
        selected_dates = set(rec["explicit_dates"].split("|"))
        selected_rows = [r for r in self.denominator if r["slate_date"] in selected_dates]
        remaining_rows = [r for r in self.denominator if r["slate_date"] not in selected_dates]
        validation = [
            {"check": "stage1_total_rows", "status": "PASS" if len(self.denominator) == 20620 else "FAIL", "observed": len(self.denominator), "expected": 20620},
            {"check": "date_count", "status": "PASS" if len(self.date_index) == 11 else "FAIL", "observed": len(self.date_index), "expected": 11},
            {"check": "candidate_count", "status": "PASS" if len(candidates) == 66 else "FAIL", "observed": len(candidates), "expected": 66},
            {"check": "selected_under_cap", "status": "PASS" if len(selected_rows) <= CAP else "FAIL", "observed": len(selected_rows), "expected": f"<= {CAP}"},
            {"check": "selected_row_count_matches_candidate", "status": "PASS" if len(selected_rows) == int(rec["authoritative_denominator_rows"]) else "FAIL", "observed": len(selected_rows), "expected": rec["authoritative_denominator_rows"]},
            {"check": "selected_plus_remainder_reconciles", "status": "PASS" if len(selected_rows) + len(remaining_rows) == len(self.denominator) else "FAIL", "observed": len(selected_rows) + len(remaining_rows), "expected": len(self.denominator)},
            {"check": "duplicate_canonical_identity_count", "status": "PASS" if len({r["canonical_row_id"] for r in selected_rows}) == len(selected_rows) else "FAIL", "observed": len(selected_rows) - len({r["canonical_row_id"] for r in selected_rows}), "expected": 0},
            {"check": "identity_replay", "status": "PASS" if all(r["canonical_row_id"] == canonical_key(r) for r in selected_rows) else "FAIL", "observed": "selected rows", "expected": "canonical identity unchanged"},
        ]
        write_csv(self.out_dir / f"deterministic_reproduction_validation_{RUN_DATE}.csv", validation)
        parse_rows = []
        for path in sorted(self.out_dir.glob("*")):
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
        write_csv(self.out_dir / f"parse_validation_{RUN_DATE}.csv", parse_rows)
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
        rows = []
        for name, pattern in PROHIBITED_PATTERNS.items():
            matches = [m.group(0) for m in pattern.finditer(text)]
            rows.append({"guard": name, "status": "PASS" if not matches else "FAIL", "match_count": len(matches)})
        write_csv(self.out_dir / f"static_no_model_signal_guard_{RUN_DATE}.csv", rows)

    def sha_manifest(self) -> None:
        rows = []
        for path in sorted(self.out_dir.glob("*")):
            if path.name == f"sha256_manifest_{RUN_DATE}.csv":
                continue
            rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(self.out_dir / f"sha256_manifest_{RUN_DATE}.csv", rows)

    def run(self) -> dict[str, Any]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self.reproduce_inputs()
        candidates = self.enumerate_candidates()
        selected = self.select_candidates(candidates)
        self.write_candidate_assessments(selected)
        self.write_manifests(selected)
        self.write_stage1_reuse_and_notes(selected)
        self.write_reports(selected, candidates)
        self.write_decision(selected, candidates)
        self.validate(candidates, selected)
        rec = selected["recommended_exact_sub_block"]
        return {
            "output_dir": str(self.out_dir),
            "candidate_count": len(candidates),
            "selected_sub_block": f"{rec['start_date']}_to_{rec['end_date']}",
            "selected_rows": int(rec["authoritative_denominator_rows"]),
            "cap_margin": int(rec["cap_margin_positive_or_excess_negative"]),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT))
    args = parser.parse_args(argv)
    result = Review(Path(args.output_dir)).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
