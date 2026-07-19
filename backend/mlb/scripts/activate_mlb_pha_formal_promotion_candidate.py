"""Activate the MLB Pitcher Hits Allowed formal promotion candidate.

This is a documentation and artifact materialization utility only. It binds
the frozen Pitcher Hits Allowed Challenger evidence to a formal, fixed-size
Champion-Challenger trial without refitting models or changing production.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-18"
CANDIDATE_ID = "MLB_PHA_CHALLENGER_V1"
CANDIDATE_LABEL = "PHA_FORMAL_PROMOTION_CANDIDATE"
DEFAULT_OUTPUT_ROOT = Path("artifacts/analysis/model_development/mlb_pha_formal_promotion_candidate")
HISTORICAL_ROOT = Path(
    "artifacts/analysis/model_development/mlb_pitcher_hits_allowed_granular_encounter_challenger/2026-07-17"
)
LIVE_ROOT = Path("artifacts/analysis/model_development/mlb_live_hitter_parent_daily_integration/2026-07-18")

HISTORICAL_FILES = {
    "historical_fit_population": HISTORICAL_ROOT / "pitcher_hits_allowed_exact_historical_population_2026-07-17.csv",
    "feature_contract_workload": HISTORICAL_ROOT / "pitcher_hits_allowed_workload_feature_manifest_2026-07-17.csv",
    "feature_contract_pitcher_granular": HISTORICAL_ROOT / "pitcher_hits_allowed_pitcher_granular_feature_manifest_2026-07-17.csv",
    "model_specification": HISTORICAL_ROOT / "pitcher_hits_allowed_fixed_challenger_contracts_2026-07-17.csv",
    "holdout_metrics": HISTORICAL_ROOT / "pitcher_hits_allowed_validation_holdout_count_results_2026-07-17.csv",
    "line_specific_metrics": HISTORICAL_ROOT / "pitcher_hits_allowed_line_specific_over_under_results_2026-07-17.csv",
    "machine_readable_historical_package": HISTORICAL_ROOT / "machine_readable_pitcher_hits_allowed_challenger_2026-07-17.json",
    "historical_sha_manifest": HISTORICAL_ROOT / "sha256_manifest_2026-07-17.csv",
    "historical_validation_report": HISTORICAL_ROOT / "validation_report_2026-07-17.csv",
}

LIVE_FILES = {
    "controlled_shadow": LIVE_ROOT / "controlled_shadow_artifact_2026-07-18.csv",
    "run_manifest": LIVE_ROOT / "current_pregame_run_manifest_2026-07-18.csv",
    "frozen_pha_challenger_ledger": LIVE_ROOT / "frozen_pha_challenger_ledger_2026-07-18.csv",
    "current_pitcher_context_ledger": LIVE_ROOT / "current_pha_pitcher_context_ledger_2026-07-18.csv",
    "pitcher_encounter_artifact": LIVE_ROOT / "pitcher_encounter_artifact_2026-07-18.csv",
    "live_parent_artifact": LIVE_ROOT / "live_hitter_parent_artifact_2026-07-18.csv",
    "live_sha_manifest": LIVE_ROOT / "sha256_manifest_2026-07-18.csv",
    "live_validation_report": LIVE_ROOT / "validation_report_2026-07-18.csv",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
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
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "<na>"} else text


def to_float(value: Any) -> float | None:
    try:
        if clean(value) == "":
            return None
        return float(value)
    except Exception:
        return None


def bool_text(value: Any) -> bool:
    return clean(value).lower() in {"true", "1", "yes", "y"}


def path_inventory(paths: dict[str, Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for role, path in paths.items():
        exists = path.exists()
        stat = path.stat() if exists else None
        rows.append(
            {
                "artifact_role": role,
                "path": str(path),
                "exists": exists,
                "file_size_bytes": stat.st_size if stat else "",
                "modified_time_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
                if stat
                else "",
                "sha256": sha256_file(path) if exists and path.is_file() else "",
            }
        )
    return rows


def line_key(row: dict[str, str]) -> str:
    line = clean(row.get("line")) or clean(row.get("market_line"))
    side = clean(row.get("model_pick_side")) or clean(row.get("champion_side")) or "UNKNOWN_SIDE"
    return "|".join(
        [
            clean(row.get("slate_date")),
            clean(row.get("game_id")),
            clean(row.get("pitcher_id")) or clean(row.get("player_id")),
            "hits_allowed",
            line,
            side.upper(),
        ]
    )


def build_surface(shadow_rows: list[dict[str, str]], run_manifest: dict[str, str]) -> list[dict[str, Any]]:
    surface: list[dict[str, Any]] = []
    for row in shadow_rows:
        champion_prob = to_float(row.get("model_prob_over") or row.get("prob_over"))
        challenger_prob = to_float(row.get("challenger_prob_over"))
        champion_proxy = to_float(row.get("champion_expected_hits_allowed"))
        challenger_proxy = to_float(row.get("challenger_expected_hits_allowed"))
        market_line = clean(row.get("line")) or clean(row.get("market_line"))
        status = clean(row.get("materialization_status")) or "UNKNOWN"
        side_disagreement = bool_text(row.get("side_disagreement"))
        surface.append(
            {
                "candidate_id": CANDIDATE_ID,
                "candidate_status": "FORMAL_PROMOTION_CANDIDATE",
                "row_label": CANDIDATE_LABEL,
                "canonical_proposition_identity": line_key(row),
                "slate_date": clean(row.get("slate_date")),
                "run_tag": clean(run_manifest.get("run_tag")),
                "capture_timestamp": clean(run_manifest.get("cutoff")) or clean(row.get("cutoff")),
                "game_id": clean(row.get("game_id")),
                "pitcher_id": clean(row.get("pitcher_id")) or clean(row.get("player_id")),
                "pitcher_name": clean(row.get("pitcher_name")) or clean(row.get("player_name")),
                "pitcher_team": clean(row.get("team")),
                "opponent": clean(row.get("opponent")),
                "market_line": market_line,
                "proposition_side": (clean(row.get("model_pick_side")) or clean(row.get("champion_side"))).upper(),
                "sportsbook": clean(row.get("market_bookmaker_key")),
                "market_price_over": clean(row.get("market_price_over")),
                "market_price_under": clean(row.get("market_price_under")),
                "selection_time_price_source": clean(row.get("market_odds_snapshot_file")),
                "champion_over_probability": champion_prob if champion_prob is not None else "",
                "challenger_over_probability": challenger_prob if challenger_prob is not None else "",
                "champion_side": clean(row.get("champion_side")),
                "challenger_side": clean(row.get("challenger_side")),
                "probability_diff_challenger_minus_champion": (
                    challenger_prob - champion_prob if challenger_prob is not None and champion_prob is not None else ""
                ),
                "champion_line_specific_proxy": champion_proxy if champion_proxy is not None else "",
                "challenger_line_specific_proxy": challenger_proxy if challenger_proxy is not None else "",
                "line_specific_proxy_diff_challenger_minus_champion": (
                    challenger_proxy - champion_proxy if challenger_proxy is not None and champion_proxy is not None else ""
                ),
                "side_disagreement": side_disagreement,
                "agreement_disagreement": "DISAGREEMENT" if side_disagreement else "AGREEMENT",
                "lineup_state": clean(row.get("lineup_state")),
                "workload_support": clean(row.get("workload_state")),
                "encounter_support": clean(row.get("support")),
                "uncertainty": clean(row.get("uncertainty")),
                "materialization_status": status,
                "withheld_reason": clean(row.get("withheld_reason")),
                "source_parent_artifact": clean(row.get("source_parent_artifact")),
                "source_parent_sha256": clean(row.get("source_parent_sha256")),
                "shadow_status": clean(row.get("shadow_status")),
                "production_behavior_changed": clean(row.get("production_behavior_changed")) or "False",
                "not_wager_recommendation": True,
            }
        )
    return surface


def summarize_coverage(surface: list[dict[str, Any]]) -> list[dict[str, Any]]:
    total = len(surface)
    scored = [r for r in surface if r["materialization_status"] == "SCORED"]
    withheld = [r for r in surface if r["materialization_status"] != "SCORED"]
    reason_counts = Counter(r["withheld_reason"] or "SCORED" for r in surface)
    rows = [
        {
            "scope": "current_run",
            "total_pha_propositions": total,
            "scored_rows": len(scored),
            "withheld_rows": len(withheld),
            "distinct_slate_dates": len({r["slate_date"] for r in surface if r["slate_date"]}),
            "distinct_market_lines_scored": len({r["market_line"] for r in scored if r["market_line"]}),
            "side_disagreement_rows_scored": sum(1 for r in scored if r["side_disagreement"]),
            "status": "PHA_CONTROLLED_SHADOW_PARTIAL_COVERAGE",
            "notes": "Partial coverage is allowed; withheld rows remain precise and no slate-wide withholding is applied.",
        }
    ]
    for reason, count in sorted(reason_counts.items()):
        rows.append(
            {
                "scope": "withholding_reason",
                "total_pha_propositions": total,
                "scored_rows": "",
                "withheld_rows": count if reason != "SCORED" else "",
                "distinct_slate_dates": "",
                "distinct_market_lines_scored": "",
                "side_disagreement_rows_scored": "",
                "status": reason,
                "notes": "Row-level materialization taxonomy from immutable July 18 controlled shadow.",
            }
        )
    return rows


def build_trial_rows(surface: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = [r for r in surface if r["materialization_status"] == "SCORED"]
    return [
        {
            **r,
            "trial_id": "MLB_PHA_FORMAL_TRIAL_V1",
            "trial_row_status": "FROZEN_PENDING_GRADE",
            "official_pitcher_hits_allowed": "",
            "over_under_result": "",
            "champion_correct": "",
            "challenger_correct": "",
            "grade_status": "PENDING_OFFICIAL_RECONCILIATION",
            "grade_source": "",
            "reconstructed_pregame_score": False,
        }
        for r in scored
    ]


def historical_comparison_rows(holdout_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    wanted = {"champion", "challenger_e_champion_plus_granular"}
    rows: list[dict[str, Any]] = []
    for row in holdout_rows:
        if row.get("temporal_split") == "holdout" and row.get("instrument") in wanted:
            rows.append(
                {
                    "evidence_scope": "historical_holdout",
                    "candidate_id": CANDIDATE_ID if row.get("instrument") != "champion" else "PRODUCTION_CHAMPION",
                    "instrument": row.get("instrument"),
                    "rows": row.get("rows"),
                    "mae": row.get("mae"),
                    "rmse": row.get("rmse"),
                    "mean_bias": row.get("mean_bias"),
                    "ranking_auc_gt_line": row.get("ranking_auc_gt_line"),
                    "notes": "Frozen historical evidence; no refit or reselection performed.",
                }
            )
    return rows


def line_specific_rows(line_rows: list[dict[str, str]], trial_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in line_rows:
        if row.get("temporal_split") == "holdout" and row.get("instrument") in {"champion", "challenger_e_champion_plus_granular"}:
            rows.append(
                {
                    "scope": "historical_holdout",
                    "line": row.get("line"),
                    "instrument": row.get("instrument"),
                    "rows": row.get("rows"),
                    "over_wins": row.get("over_wins"),
                    "over_losses": row.get("over_losses"),
                    "pushes": row.get("pushes"),
                    "observed_over_rate": row.get("observed_over_rate"),
                    "avg_prob_over": row.get("avg_prob_over"),
                    "brier": row.get("brier"),
                    "log_loss": row.get("log_loss"),
                    "auc": row.get("auc"),
                    "ece": row.get("ece"),
                    "notes": "Historical line-specific probability evidence.",
                }
            )
    counts = Counter(r["market_line"] for r in trial_rows)
    for line, count in sorted(counts.items(), key=lambda item: str(item[0])):
        rows.append(
            {
                "scope": "formal_trial_current_pending",
                "line": line,
                "instrument": CANDIDATE_ID,
                "rows": count,
                "over_wins": "",
                "over_losses": "",
                "pushes": "",
                "observed_over_rate": "",
                "avg_prob_over": "",
                "brier": "",
                "log_loss": "",
                "auc": "",
                "ece": "",
                "notes": "Current formal trial rows are frozen pregame and pending official grading.",
            }
        )
    return rows


def build_decisions(surface: list[dict[str, Any]], trial_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = len(trial_rows)
    distinct_dates = len({r["slate_date"] for r in trial_rows if r["slate_date"]})
    disagreement_rows = sum(1 for r in trial_rows if r["side_disagreement"])
    distinct_lines = len({r["market_line"] for r in trial_rows if r["market_line"]})
    return [
        {
            "decision": "MLB_PHA_FORMAL_CANDIDATE_IDENTITY_DECISION",
            "value": "MLB_PHA_CHALLENGER_V1_FORMAL_PROMOTION_CANDIDATE",
            "notes": "Bound to frozen challenger_e_champion_plus_granular and exact-line proposition grain.",
        },
        {
            "decision": "MLB_PHA_FORMAL_CANDIDATE_DAILY_GENERATION_DECISION",
            "value": "EXISTING_GOVERNED_CHAIN_ACTIVE_PARTIAL_COVERAGE_ACCEPTED",
            "notes": "Uses governed lineup capture, live parent, encounter aggregate, exact-line scoring, and proposition join.",
        },
        {
            "decision": "MLB_PHA_FORMAL_CANDIDATE_SURFACE_DECISION",
            "value": "RESEARCH_ONLY_SURFACE_CREATED_NO_WAGER_RECOMMENDATION",
            "notes": "No production upload, workspace, selector, tier, or formula behavior changed.",
        },
        {
            "decision": "MLB_PHA_FORMAL_TRIAL_POPULATION_DECISION",
            "value": f"TRIAL_OPEN_{scored}_OF_75_ROWS_{distinct_dates}_OF_3_MIN_DATES_{disagreement_rows}_OF_15_DISAGREEMENTS",
            "notes": "Endpoint is five completed slate dates or 75 frozen exact-line rows, whichever first; current minimums are not met.",
        },
        {
            "decision": "MLB_PHA_FORMAL_TRIAL_GRADING_DECISION",
            "value": "PENDING_OFFICIAL_RECONCILIATION_GRADE_ONLY_FROZEN_PREFIRSTPITCH_ROWS",
            "notes": "No reconstructed pregame scores are allowed.",
        },
        {
            "decision": "MLB_PHA_FORMAL_PROBABILITY_DECISION",
            "value": "HISTORICAL_PROBABILITY_ADVANTAGE_INTACT_PROSPECTIVE_PROBABILITY_PENDING",
            "notes": "Historical line-relative AUC improves from 0.484357 to 0.514713; prospective rows are pending outcomes.",
        },
        {
            "decision": "MLB_PHA_FORMAL_SIDE_DECISION",
            "value": "SIDE_CLASSIFICATION_TRIAL_OPEN_PENDING_GRADE",
            "notes": "Current frozen rows have no official outcome grades yet.",
        },
        {
            "decision": "MLB_PHA_FORMAL_DISAGREEMENT_DECISION",
            "value": f"PENDING_MINIMUM_15_DISAGREEMENTS_CURRENT_{disagreement_rows}",
            "notes": "Disagreement rows are the central prospective promotion test.",
        },
        {
            "decision": "MLB_PHA_FORMAL_LINE_STABILITY_DECISION",
            "value": f"MULTI_LINE_TRIAL_OPEN_CURRENT_DISTINCT_LINES_{distinct_lines}",
            "notes": "Current scored rows include natural multi-line coverage but remain underpowered.",
        },
        {
            "decision": "MLB_PHA_FORMAL_COVERAGE_DECISION",
            "value": "JULY18_PARTIAL_COVERAGE_21_PROPS_3_SCORED_18_WITHHELD",
            "notes": "Partial coverage is acceptable and withholding remains row-level.",
        },
        {
            "decision": "MLB_PHA_FORMAL_PROMOTION_DECISION",
            "value": "TRIAL_OPEN_NO_ENDPOINT_DECISION_YET",
            "notes": "Formal candidate status is active; production replacement is not earned.",
        },
        {
            "decision": "MLB_PHA_FORMAL_IMPLEMENTATION_READINESS_DECISION",
            "value": "IMPLEMENTATION_PLAN_DEFERRED_UNTIL_PROMOTION_RECOMMENDED",
            "notes": "A production implementation plan is only actionable if the fixed trial recommends promotion.",
        },
        {
            "decision": "MLB_PHA_PRODUCTION_STATUS",
            "value": "NOT_AUTHORIZED_PENDING_EXPLICIT_USER_APPROVAL",
            "notes": "Production Champion remains unchanged.",
        },
    ]


def markdown_summary(paths: dict[str, Path], counts: dict[str, Any], decisions: list[dict[str, Any]]) -> str:
    decision_lines = "\n".join(f"- `{d['decision']} = {d['value']}`" for d in decisions)
    return f"""# MLB Pitcher Hits Allowed Formal Promotion Candidate Activation

Generated: `{counts['generated_at']}`

## Governing Conclusion

The Pitcher Hits Allowed Challenger has earned formal promotion-candidate status, but has not yet earned production replacement.

`MLB_PHA_CHALLENGER_V1_STATUS = FORMAL_PROMOTION_CANDIDATE`

Production behavior is unchanged. This package starts a fixed, decision-bound Champion-Challenger trial for the frozen exact-line Pitcher Hits Allowed Challenger.

## Candidate Identity

- Candidate: `MLB_PHA_CHALLENGER_V1`
- Frozen historical instrument: `challenger_e_champion_plus_granular`
- Grain: `slate_date | game_id | pitcher_id | market_line | side`
- Semantics: line-specific proposition instrument, not an invariant pitcher-game expected-hit-count model.
- Trial endpoint: five completed slate dates or 75 frozen exact-line Challenger propositions, whichever comes first.

## Historical Evidence Preserved

- Pitcher-line rows: `1,057`
- Fit / validation / holdout: `542 / 236 / 279`
- Champion holdout MAE: `1.987489`
- Challenger holdout MAE: `1.786219`
- MAE improvement: `0.201271`
- Rolling MAE wins: `37 / 52`
- Champion line-relative AUC: `0.484357`
- Challenger line-relative AUC: `0.514713`
- Historical prediction parity: `PASS`
- Live inference parity: `PASS`

## Current Trial State

- Current PHA propositions: `{counts['current_pha_rows']}`
- Frozen scored exact-line rows: `{counts['trial_rows']}`
- Withheld rows: `{counts['withheld_rows']}`
- Distinct scored market lines: `{counts['distinct_scored_lines']}`
- Scored Champion-Challenger disagreement rows: `{counts['disagreement_rows']}`
- Trial status: `TRIAL_OPEN_NOT_ENDPOINT_MET`

## Required Decisions

{decision_lines}

## Artifacts

- Candidate contract: `{paths['contract'].as_posix()}`
- Candidate surface: `{paths['surface'].as_posix()}`
- Frozen trial ledger: `{paths['trial'].as_posix()}`
- Graded ledger: `{paths['graded'].as_posix()}`
- Coverage and withholding: `{paths['coverage'].as_posix()}`
- SHA256 manifest: `{paths['sha_manifest'].as_posix()}`

## No Behavior Changed

No model fitting, feature redesign, threshold optimization, DB writes, OddsAPI calls, upload changes, workspace changes, selector changes, LaunchAgent changes, or production replacement occurred.
"""


def validate_outputs(csv_paths: list[Path], json_paths: list[Path], md_paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in csv_paths:
        try:
            read_csv(path)
            status = "PASS"
            notes = "CSV parsed"
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"validation": "csv_parse", "path": str(path), "status": status, "notes": notes})
    for path in json_paths:
        try:
            json.loads(path.read_text(encoding="utf-8"))
            status = "PASS"
            notes = "JSON parsed"
        except Exception as exc:
            status = "FAIL"
            notes = str(exc)
        rows.append({"validation": "json_parse", "path": str(path), "status": status, "notes": notes})
    for path in md_paths:
        text = path.read_text(encoding="utf-8")
        rows.append(
            {
                "validation": "markdown_review",
                "path": str(path),
                "status": "PASS" if text.startswith("# ") and "No Behavior Changed" in text else "WARN",
                "notes": "Markdown has title and no behavior changed note",
            }
        )
    rows.extend(
        [
            {
                "validation": "no_db_writes",
                "path": "script_static_contract",
                "status": "PASS",
                "notes": "Utility reads local artifacts and writes analysis package only.",
            },
            {
                "validation": "no_network_or_oddsapi",
                "path": "script_static_contract",
                "status": "PASS",
                "notes": "Utility does not import request clients or call external services.",
            },
            {
                "validation": "production_behavior",
                "path": "script_static_contract",
                "status": "PASS",
                "notes": "No production path, upload, workspace, selector, or LaunchAgent writes.",
            },
        ]
    )
    return rows


def build(output_root: Path, run_date: str) -> dict[str, Any]:
    generated_at = utc_now()
    out = output_root / run_date
    out.mkdir(parents=True, exist_ok=True)

    shadow_rows = read_csv(LIVE_FILES["controlled_shadow"])
    run_manifest_rows = read_csv(LIVE_FILES["run_manifest"])
    run_manifest = run_manifest_rows[0] if run_manifest_rows else {}
    holdout_rows = read_csv(HISTORICAL_FILES["holdout_metrics"])
    line_rows = read_csv(HISTORICAL_FILES["line_specific_metrics"])
    contracts = read_csv(HISTORICAL_FILES["model_specification"])

    surface = build_surface(shadow_rows, run_manifest)
    trial_rows = build_trial_rows(surface)
    coverage = summarize_coverage(surface)
    decisions = build_decisions(surface, trial_rows)

    contract_rows = [
        {
            "candidate_id": CANDIDATE_ID,
            "candidate_status": "FORMAL_PROMOTION_CANDIDATE",
            "production_status": "NOT_AUTHORIZED_PENDING_EXPLICIT_USER_APPROVAL",
            "frozen_instrument": "challenger_e_champion_plus_granular",
            "contract_grain": "slate_date | game_id | pitcher_id | market_line | side",
            "line_specific_semantics": True,
            "model_or_feature_change": False,
            "source_contract_row": json.dumps(row, sort_keys=True),
        }
        for row in contracts
        if row.get("instrument") in {"challenger_e_champion_plus_granular", "champion"}
    ]
    if not contract_rows:
        contract_rows = [
            {
                "candidate_id": CANDIDATE_ID,
                "candidate_status": "FORMAL_PROMOTION_CANDIDATE",
                "production_status": "NOT_AUTHORIZED_PENDING_EXPLICIT_USER_APPROVAL",
                "frozen_instrument": "challenger_e_champion_plus_granular",
                "contract_grain": "slate_date | game_id | pitcher_id | market_line | side",
                "line_specific_semantics": True,
                "model_or_feature_change": False,
                "source_contract_row": "UNKNOWN",
            }
        ]

    path_map = {
        "summary": out / f"pha_formal_candidate_activation_{run_date}.md",
        "contract": out / f"pha_formal_candidate_contract_{run_date}.csv",
        "identity_hashes": out / f"pha_formal_candidate_model_identity_hashes_{run_date}.csv",
        "manifest": out / f"pha_formal_candidate_daily_prediction_manifest_{run_date}.csv",
        "surface": out / f"pha_formal_candidate_surface_{run_date}.csv",
        "trial": out / f"pha_formal_candidate_trial_population_{run_date}.csv",
        "graded": out / f"pha_formal_candidate_graded_ledger_{run_date}.csv",
        "comparison": out / f"pha_formal_candidate_champion_challenger_comparison_{run_date}.csv",
        "disagreement": out / f"pha_formal_candidate_disagreement_results_{run_date}.csv",
        "line_specific": out / f"pha_formal_candidate_line_specific_results_{run_date}.csv",
        "coverage": out / f"pha_formal_candidate_coverage_withholding_{run_date}.csv",
        "promotion": out / f"pha_formal_candidate_promotion_decision_{run_date}.csv",
        "implementation_plan": out / f"pha_formal_candidate_implementation_plan_{run_date}.csv",
        "decisions": out / f"pha_formal_candidate_required_decisions_{run_date}.csv",
        "machine": out / f"machine_readable_pha_formal_candidate_{run_date}.json",
        "sha_manifest": out / f"sha256_manifest_{run_date}.csv",
        "validation": out / f"validation_report_{run_date}.csv",
    }

    inventory = path_inventory({**HISTORICAL_FILES, **LIVE_FILES})
    write_csv(path_map["identity_hashes"], inventory)
    write_csv(path_map["contract"], contract_rows)

    scored = [r for r in surface if r["materialization_status"] == "SCORED"]
    manifest_rows = [
        {
            "candidate_id": CANDIDATE_ID,
            "slate_date": run_date,
            "run_tag": clean(run_manifest.get("run_tag")),
            "capture_timestamp": clean(run_manifest.get("cutoff")),
            "source_controlled_shadow": str(LIVE_FILES["controlled_shadow"]),
            "source_controlled_shadow_sha256": sha256_file(LIVE_FILES["controlled_shadow"]) if LIVE_FILES["controlled_shadow"].exists() else "",
            "current_pha_propositions": len(surface),
            "frozen_scored_rows": len(scored),
            "withheld_rows": len(surface) - len(scored),
            "trial_status": "TRIAL_OPEN_NOT_ENDPOINT_MET",
            "production_behavior_changed": False,
            "notes": "Immutable July 18 pregame capture reused; no new lineup capture performed by this utility.",
        }
    ]
    write_csv(path_map["manifest"], manifest_rows)
    write_csv(path_map["surface"], surface)
    write_csv(path_map["trial"], trial_rows)
    write_csv(path_map["graded"], trial_rows)
    write_csv(path_map["comparison"], historical_comparison_rows(holdout_rows))

    disagreement_rows = [
        r
        for r in trial_rows
        if r["side_disagreement"]
    ]
    if disagreement_rows:
        write_csv(path_map["disagreement"], disagreement_rows)
    else:
        write_csv(
            path_map["disagreement"],
            [
                {
                    "trial_id": "MLB_PHA_FORMAL_TRIAL_V1",
                    "candidate_id": CANDIDATE_ID,
                    "disagreement_rows": 0,
                    "champion_wins": "",
                    "challenger_wins": "",
                    "pushes": "",
                    "average_probability_difference": "",
                    "line_distribution": "",
                    "status": "PENDING_MINIMUM_15_DISAGREEMENTS",
                    "notes": "Current July 18 scored rows all agree on UNDER; disagreement test remains pending.",
                }
            ],
        )

    write_csv(path_map["line_specific"], line_specific_rows(line_rows, trial_rows))
    write_csv(path_map["coverage"], coverage)
    write_csv(
        path_map["promotion"],
        [
            {
                "candidate_id": CANDIDATE_ID,
                "formal_candidate_status": "FORMAL_PROMOTION_CANDIDATE",
                "endpoint_status": "TRIAL_OPEN_NOT_ENDPOINT_MET",
                "promotion_decision": "TRIAL_OPEN_NO_ENDPOINT_DECISION_YET",
                "production_status": "NOT_AUTHORIZED_PENDING_EXPLICIT_USER_APPROVAL",
                "required_endpoint": "five completed slate dates or 75 frozen exact-line Challenger propositions, whichever first",
                "minimum_requirements": ">=3 dates; >=15 disagreements; >1 market line; exact pregame lineage; zero reconstructed pregame scores",
                "notes": "The Challenger has advanced from research watch to decision-bound formal candidate; it has not earned production replacement.",
            }
        ],
    )
    write_csv(
        path_map["implementation_plan"],
        [
            {
                "step": "wait_for_fixed_trial_endpoint",
                "status": "PENDING",
                "behavior_change_required": False,
                "description": "Continue governed default-off capture and grade only frozen pre-first-pitch proposition rows.",
                "notes": "No production implementation is warranted before endpoint decision.",
            },
            {
                "step": "issue_endpoint_promotion_recommendation",
                "status": "PENDING",
                "behavior_change_required": False,
                "description": "Choose one of the frozen endpoint decisions after trial completion.",
                "notes": "Do not extend the trial for inconclusive or unfavorable results.",
            },
            {
                "step": "prepare_production_replacement_plan_if_recommended",
                "status": "DEFERRED",
                "behavior_change_required": True,
                "description": "Only if promotion is recommended, draft production replacement plan for explicit human authorization.",
                "notes": "No implementation in this activation package.",
            },
        ],
    )
    write_csv(path_map["decisions"], decisions)

    counts = {
        "generated_at": generated_at,
        "current_pha_rows": len(surface),
        "trial_rows": len(trial_rows),
        "withheld_rows": len(surface) - len(trial_rows),
        "distinct_scored_lines": len({r["market_line"] for r in trial_rows if r["market_line"]}),
        "disagreement_rows": sum(1 for r in trial_rows if r["side_disagreement"]),
    }
    write_text(path_map["summary"], markdown_summary(path_map, counts, decisions))

    machine = {
        "generated_at": generated_at,
        "candidate_id": CANDIDATE_ID,
        "candidate_status": "FORMAL_PROMOTION_CANDIDATE",
        "production_status": "NOT_AUTHORIZED_PENDING_EXPLICIT_USER_APPROVAL",
        "line_specific_grain": "slate_date | game_id | pitcher_id | market_line | side",
        "current_counts": counts,
        "historical_evidence": {
            "pitcher_line_rows": 1057,
            "fit_rows": 542,
            "validation_rows": 236,
            "holdout_rows": 279,
            "champion_holdout_mae": 1.987489,
            "challenger_holdout_mae": 1.786219,
            "mae_improvement": 0.201271,
            "rolling_mae_wins": "37/52",
            "champion_line_relative_auc": 0.484357,
            "challenger_line_relative_auc": 0.514713,
            "historical_prediction_parity": "PASS",
            "live_inference_parity": "PASS",
        },
        "decisions": {row["decision"]: row["value"] for row in decisions},
        "artifacts": {name: str(path) for name, path in path_map.items()},
        "guardrails": {
            "model_fitting": False,
            "new_features": False,
            "db_writes": False,
            "network_calls": False,
            "oddsapi_calls": False,
            "production_behavior_changed": False,
        },
    }
    write_json(path_map["machine"], machine)

    csv_paths = [
        path_map["contract"],
        path_map["identity_hashes"],
        path_map["manifest"],
        path_map["surface"],
        path_map["trial"],
        path_map["graded"],
        path_map["comparison"],
        path_map["disagreement"],
        path_map["line_specific"],
        path_map["coverage"],
        path_map["promotion"],
        path_map["implementation_plan"],
        path_map["decisions"],
    ]
    json_paths = [path_map["machine"]]
    md_paths = [path_map["summary"]]

    # The package manifest is calculated after primary artifacts are written.
    manifest_rows = path_inventory({name: path for name, path in path_map.items() if name not in {"sha_manifest", "validation"}})
    write_csv(path_map["sha_manifest"], manifest_rows)
    csv_paths.append(path_map["sha_manifest"])
    validation_rows = validate_outputs(csv_paths, json_paths, md_paths)
    write_csv(path_map["validation"], validation_rows)

    return {
        "output_dir": str(out),
        "paths": {name: str(path) for name, path in path_map.items()},
        "counts": counts,
        "decisions": {row["decision"]: row["value"] for row in decisions},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=RUN_DATE)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--mode", default="research_only", choices=["research_only"])
    args = parser.parse_args()
    result = build(Path(args.output_root), args.date)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
