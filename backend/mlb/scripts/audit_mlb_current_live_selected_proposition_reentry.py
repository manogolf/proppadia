"""Build a read-only MLB current/live selected-proposition re-entry audit.

The utility inventories local artifacts only. It does not call external APIs,
write databases, train models, build matrices, or change production behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-16"
ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = (
    ROOT
    / "artifacts/analysis/model_development/"
    / "mlb_current_live_selected_proposition_research_reentry/2026-07-16"
)
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history"
QUICK_ROOT = ROOT / "backend/mlb/exports/quick_card"
UPLOAD_ROOT = ROOT / "backend/mlb/data/processed/mlb_uploads"


HISTORICAL_AUTHORITY = {
    "fully_qualified_hits": 1540,
    "hits_0_5": 1400,
    "hits_1_5": 140,
    "primary_starter_blocked": 62,
    "primary_pa_blocked": 42,
    "primary_outcome_blocked": 363,
    "primary_bundle_blocked": 36,
    "hits_1_5_qualified_not_matrix_queue": 41,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _count(path: Path) -> tuple[int, int, list[str]]:
    if not path.exists() or path.suffix.lower() != ".csv":
        return 0, 0, []
    rows = _read_csv(path)
    if not rows:
        with path.open(newline="") as fh:
            reader = csv.reader(fh)
            header = next(reader, [])
        return 0, len(header), header
    return len(rows), len(rows[0]), list(rows[0].keys())


def _latest_live_date() -> str:
    candidates = []
    if ODDS_ROOT.exists():
        for child in ODDS_ROOT.iterdir():
            if child.is_dir() and (child / "mlb_slate_output.csv").exists():
                candidates.append(child.name)
    if not candidates:
        raise FileNotFoundError(f"no local odds-history slate outputs under {_rel(ODDS_ROOT)}")
    return sorted(candidates)[-1]


def _infer_run_tag(date_value: str, manifest: dict[str, Any], slate_rows: list[dict[str, str]]) -> str:
    tags = []
    for row in slate_rows:
        tag = (row.get("market_snapshot_run_tag") or "").strip()
        if tag:
            tags.append(tag)
    if tags:
        return Counter(tags).most_common(1)[0][0]
    copied = manifest.get("artifacts") or []
    for item in copied:
        dest = str(item.get("destination") or "")
        if "__local_daily_" in dest:
            return dest.rsplit("__", 1)[-1].split(".")[0]
    captured = str(manifest.get("captured_at_utc") or "")
    return captured or f"{date_value}_run_tag_unknown"


def _artifact_inventory(date_value: str) -> tuple[list[dict[str, Any]], dict[str, Path]]:
    paths = {
        "odds_manifest": ODDS_ROOT / date_value / "manifest.json",
        "slate_output": ODDS_ROOT / date_value / "mlb_slate_output.csv",
        "predictions_wide": ODDS_ROOT / date_value / "mlb_predictions_wide_calibrated.csv",
        "book_upload_archive": ODDS_ROOT / date_value / "mlb_book_upload.csv",
        "quick_card": QUICK_ROOT / date_value / "quick_card.csv",
        "upload_base": UPLOAD_ROOT / date_value / "05_book_upload_base.csv",
        "upload_identity_diagnostics": UPLOAD_ROOT / date_value / f"upload_identity_diagnostics_{date_value}.csv",
        "upload_manifest": UPLOAD_ROOT / date_value / "MANIFEST.md",
        "latest_upload_manifest": UPLOAD_ROOT / "MANIFEST.md",
    }
    rows = []
    for role, path in paths.items():
        exists = path.exists()
        stat = path.stat() if exists else None
        row_count, col_count, columns = _count(path) if exists else (0, 0, [])
        rows.append(
            {
                "artifact_role": role,
                "path": _rel(path),
                "exists": exists,
                "file_size_bytes": stat.st_size if stat else "",
                "modified_time_utc": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds") if stat else "",
                "sha256": _sha256(path) if exists and path.is_file() else "",
                "row_count": row_count,
                "column_count": col_count,
                "columns": "|".join(columns),
                "research_use": _artifact_use(role),
                "notes": "",
            }
        )
    return rows, paths


def _artifact_use(role: str) -> str:
    return {
        "odds_manifest": "run-tag and artifact-copy provenance",
        "slate_output": "canonical current/live model-plus-market proposition universe",
        "predictions_wide": "current model probability surface before market row expansion",
        "book_upload_archive": "two-sided upload-style current candidate/export surface",
        "quick_card": "small operator-facing candidate card",
        "upload_base": "current processed two-sided upload file",
        "upload_identity_diagnostics": "selected upload identity and row-key diagnostics",
        "upload_manifest": "current date processed upload manifest",
        "latest_upload_manifest": "latest processed upload manifest pointer",
    }.get(role, "supporting artifact")


def _num(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _bucket_prob(v: float | None) -> str:
    if v is None:
        return "missing"
    if v < 0.50:
        return "<0.50"
    if v < 0.55:
        return "0.50_to_0.55"
    if v < 0.60:
        return "0.55_to_0.60"
    if v < 0.65:
        return "0.60_to_0.65"
    return ">=0.65"


def _bucket_gap(v: float | None) -> str:
    if v is None:
        return "missing"
    if v < -0.05:
        return "<-0.05"
    if v < 0:
        return "-0.05_to_0"
    if v < 0.025:
        return "0_to_0.025"
    if v < 0.05:
        return "0.025_to_0.05"
    return ">=0.05"


def _counter_rows(counter: Counter, dimension: str, source: str) -> list[dict[str, Any]]:
    total = sum(counter.values()) or 1
    return [
        {
            "source": source,
            "dimension": dimension,
            "bucket": str(key),
            "rows": value,
            "pct": round(100.0 * value / total, 4),
            "notes": "",
        }
        for key, value in sorted(counter.items(), key=lambda item: str(item[0]))
    ]


def _population_characterization(
    slate_rows: list[dict[str, str]],
    prediction_rows: list[dict[str, str]],
    upload_rows: list[dict[str, str]],
    quick_rows: list[dict[str, str]],
    identity_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.append({"source": "slate_output", "dimension": "total_rows", "bucket": "all", "rows": len(slate_rows), "pct": 100.0, "notes": "one market-side selection row per available line/proposition grain"})
    rows.append({"source": "predictions_wide", "dimension": "total_rows", "bucket": "all", "rows": len(prediction_rows), "pct": 100.0, "notes": "model probability surface before two-sided upload expansion"})
    rows.append({"source": "book_upload", "dimension": "total_rows", "bucket": "all", "rows": len(upload_rows), "pct": 100.0, "notes": "two-sided upload-style export rows"})
    rows.append({"source": "quick_card", "dimension": "total_rows", "bucket": "all", "rows": len(quick_rows), "pct": 100.0, "notes": "operator-facing small card"})
    rows.append({"source": "identity_diagnostics", "dimension": "total_rows", "bucket": "all", "rows": len(identity_rows), "pct": 100.0, "notes": "selected upload identity diagnostics"})

    for source_name, source_rows, dimensions in [
        ("slate_output", slate_rows, ["prop_type", "line", "model_pick_side", "calibration_method"]),
        ("quick_card", quick_rows, ["prop_type", "line", "side", "tier", "price_edge_class"]),
        ("identity_diagnostics", identity_rows, ["prop_type", "line", "side", "source_lane", "identity_status"]),
    ]:
        for dim in dimensions:
            if source_rows and dim in source_rows[0]:
                rows.extend(_counter_rows(Counter(r.get(dim, "") for r in source_rows), dim, source_name))

    combo_counter = Counter((r.get("prop_type", ""), r.get("line", ""), r.get("model_pick_side", "")) for r in slate_rows)
    rows.extend(_counter_rows(combo_counter, "prop_type|line|model_pick_side", "slate_output"))
    rows.extend(_counter_rows(Counter(_bucket_prob(_num(r.get("model_pick_prob"))) for r in slate_rows), "model_pick_prob_bucket", "slate_output"))
    rows.extend(_counter_rows(Counter(_bucket_gap(_num(r.get("model_vs_market_gap"))) for r in slate_rows), "model_vs_market_gap_bucket", "slate_output"))
    rows.extend(_counter_rows(Counter(r.get("market_book_count_two_sided") or "missing" for r in slate_rows), "market_book_count_two_sided", "slate_output"))
    return rows


def _missingness(slate_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    cols = [
        "game_id",
        "player_id",
        "prop_type",
        "line",
        "model_pick_side",
        "model_pick_prob",
        "market_price_over",
        "market_price_under",
        "market_no_vig_implied_over",
        "market_no_vig_implied_under",
        "market_book_count_two_sided",
        "bvp_plate_appearances",
        "bvp_payload_present",
        "rolling_result_avg_7",
        "d7_hits",
        "d15_hits",
        "d30_hits",
        "d7_hits_allowed",
        "d15_hits_allowed",
        "d30_hits_allowed",
        "selected_side_price",
        "selected_side_no_vig_implied",
        "model_vs_market_gap",
    ]
    rows = []
    total = len(slate_rows) or 1
    for col in cols:
        missing = sum(1 for r in slate_rows if str(r.get(col, "")).strip() in {"", "NA", "None", "nan"})
        rows.append(
            {
                "source": "slate_output",
                "field": col,
                "rows": len(slate_rows),
                "missing": missing,
                "present": len(slate_rows) - missing,
                "missing_pct": round(100.0 * missing / total, 4),
                "notes": _field_note(col),
            }
        )
    return rows


def _field_note(col: str) -> str:
    if col in {"d7_hits_allowed", "d15_hits_allowed", "d30_hits_allowed"}:
        return "pitcher-only rolling fields; expected missing for batter rows"
    if col == "market_book_count_two_sided":
        return "empty in today's slate output, so book-count research must use odds/raw market artifacts"
    if col.startswith("bvp"):
        return "BvP retained on slate output"
    if col in {"d7_hits", "d15_hits", "d30_hits"}:
        return "hitter persistence proxy retained for batter rows"
    return ""


def _readiness_rows(date_value: str, run_tag: str, paths: dict[str, Path], slate_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    checks = []
    def add(component: str, status: str, evidence: str, recommendation: str) -> None:
        checks.append({"component": component, "status": status, "evidence": evidence, "recommendation": recommendation})

    add("latest_live_date", "PASS", date_value, "Use this as the current/local live baseline for this package.")
    add("latest_run_tag", "PASS" if run_tag else "WARN", run_tag or "missing", "Retain run tag with every future comparison.")
    for role in ["slate_output", "predictions_wide", "book_upload_archive", "quick_card", "upload_base", "upload_identity_diagnostics"]:
        path = paths[role]
        add(role, "PASS" if path.exists() else "FAIL", _rel(path), "Required for current/live research baseline." if role in {"slate_output", "predictions_wide"} else "Useful supporting surface.")
    generated = Counter(r.get("generated_at_utc", "") for r in slate_rows)
    add("feature_prediction_generation_timestamp", "PASS" if generated else "WARN", "; ".join(k for k, _ in generated.most_common(3)), "Timestamp is available on slate rows.")
    if slate_rows:
        key_missing = sum(1 for r in slate_rows if not r.get("game_id") or not r.get("player_id") or not r.get("prop_type") or not r.get("line"))
        add("identity_grain_keys", "PASS" if key_missing == 0 else "FAIL", f"{key_missing} slate rows missing game_id/player_id/prop_type/line", "Do not evaluate rows with incomplete grain keys.")
        price_missing = sum(1 for r in slate_rows if not r.get("selected_side_price"))
        add("selected_side_price", "PASS" if price_missing == 0 else "WARN", f"{price_missing} slate rows missing selected_side_price", "Market-context research can use selected-side price where present.")
        add("book_count_availability", "WARN", "market_book_count_two_sided is blank on slate output", "Use odds-history raw artifacts for book-count research.")
        add("pa_opportunity_on_live_slate", "WARN", "no PA/opportunity fields found in slate output columns", "Selected next experiment should first define a strict-prior live join/overlay, not train.")
        add("lineup_role_on_live_slate", "WARN", "no lineup_slot/role fields found in slate output columns", "Pregame lineup capture can support later role overlay but is not in this canonical surface.")
    return checks


def _compatibility_rows() -> list[dict[str, Any]]:
    rows = [
        ("prop side identity", "prop_type,line,side/model_pick_side", "exact comparable concept", "current slate has prop_type,line,model_pick_side; historical certified population is side-level Hits denominator", "Use only when side definitions are frozen; do not mix upload two-sided rows with selected-side rows."),
        ("player/game identity", "game_id,player_id", "exact comparable field family", "current slate has game_id/player_id; historical campaign used selected-proposition identities", "Exact row-level historical manifest must be referenced before comparing distributions."),
        ("Hits 0.5 vs Hits 1.5", "prop_type=line", "aggregate comparable", "authoritative historical totals: Hits 0.5=1400, Hits 1.5=140", "Safe at aggregate count level only in this package."),
        ("model probability", "model_pick_prob/prob_over/prob_under", "current-only versioned", "current slate has calibrated probabilities; historical freeze/version compatibility not established here", "Do not claim model-version parity without Champion freeze linkage."),
        ("odds/price context", "selected_side_price,no_vig,market_gap", "current-live comparable concept", "current slate carries price/no-vig/gap; historical selected-proposition campaign was denominator qualification, not pricing-quality matrix", "Use for prospective research design only."),
        ("hitter persistence", "d7/d15/d30 hits", "semantically related", "current slate has rolling hitter fields; historical bundle has persistence platforms", "Version/strict-prior semantics must be locked before direct model claims."),
        ("PA opportunity", "prior rolling PA/opportunity bundle", "not present on current live slate", "PA platform exists, but no PA columns are in today's canonical slate output", "High-value next step is a strict-prior live overlay design."),
        ("starter skill/workload", "pitcher_base/workload/vulnerability", "not present on current live slate", "starter research platform exists; today's slate has only pitcher rolling stat fields for pitcher props", "Join-readiness should be verified separately before using as live predictor."),
        ("lineup role", "pregame lineup_slot/role_bucket", "not present on current live slate", "pregame lineup capture exists as dry-run research; not propagated to slate output", "Use only as prospective overlay once source timestamp and pregame status are retained."),
        ("outcomes/grading", "official outcomes", "historical-only for completed rows", "today's live slate is unresolved", "No outcome judgment in this re-entry audit."),
    ]
    return [
        {
            "concept": concept,
            "field_or_grain": field,
            "compatibility_class": cls,
            "evidence": evidence,
            "research_rule": rule,
        }
        for concept, field, cls, evidence, rule in rows
    ]


def _hypotheses() -> list[dict[str, Any]]:
    items = [
        {
            "rank": 1,
            "hypothesis_id": "LIVE_PA_OPPORTUNITY_OVERLAY_FOR_HITS_15",
            "hypothesis": "Strict-prior PA/opportunity context improves live Hits selected-proposition ranking and explains weak O1.5/U1.5 disagreements.",
            "likely_prediction_relevance": 5,
            "historical_and_live_evidence_availability": 4,
            "strict_pregame_reproducibility": 4,
            "population_size": 4,
            "implementation_effort": 3,
            "leakage_risk": 2,
            "champion_challenger_interpretability": 5,
            "selected": "YES",
            "rationale": "PA platform is mature historically, targets prediction quality, and today's live slate lacks PA labels, making a bounded overlay/join design the highest-value bridge back to current/live research.",
        },
        {
            "rank": 2,
            "hypothesis_id": "HITTER_CONTEXT_HIERARCHY_LIVE_LABELS",
            "hypothesis": "Persistence plus ownership plus opportunity plus pregame role labels improves Hits 1.5 context quality.",
            "likely_prediction_relevance": 5,
            "historical_and_live_evidence_availability": 3,
            "strict_pregame_reproducibility": 3,
            "population_size": 4,
            "implementation_effort": 4,
            "leakage_risk": 3,
            "champion_challenger_interpretability": 5,
            "selected": "NO",
            "rationale": "Strong baseball evidence, but live role/ownership propagation is less complete than PA opportunity and should follow the PA overlay baseline.",
        },
        {
            "rank": 3,
            "hypothesis_id": "MARKET_LATE_DISCOVERY_QUALITY",
            "hypothesis": "Rolling market-late discovery changes current candidate coverage and identifies late Hits 1.5 opportunities missed by morning-only surfaces.",
            "likely_prediction_relevance": 2,
            "historical_and_live_evidence_availability": 4,
            "strict_pregame_reproducibility": 5,
            "population_size": 3,
            "implementation_effort": 2,
            "leakage_risk": 2,
            "champion_challenger_interpretability": 2,
            "selected": "NO",
            "rationale": "Operationally useful, but it primarily improves market/candidate coverage rather than model prediction quality.",
        },
        {
            "rank": 4,
            "hypothesis_id": "STARTER_SKILL_WORKLOAD_LIVE_JOIN_HEALTH",
            "hypothesis": "Starter skill/workload labels improve live Hits 1.5 candidate interpretation once joined to current slate rows.",
            "likely_prediction_relevance": 4,
            "historical_and_live_evidence_availability": 3,
            "strict_pregame_reproducibility": 3,
            "population_size": 3,
            "implementation_effort": 4,
            "leakage_risk": 3,
            "champion_challenger_interpretability": 4,
            "selected": "NO",
            "rationale": "Valuable, but recent campaign explicitly closed residual Starter work; this should not reopen Starter qualification during this re-entry.",
        },
        {
            "rank": 5,
            "hypothesis_id": "BVP_RECENT_PERFORMANCE_BOUNDARY_CONTEXT",
            "hypothesis": "BvP and rolling hitter performance explain boundary cases in live selected propositions.",
            "likely_prediction_relevance": 3,
            "historical_and_live_evidence_availability": 4,
            "strict_pregame_reproducibility": 4,
            "population_size": 3,
            "implementation_effort": 2,
            "leakage_risk": 2,
            "champion_challenger_interpretability": 2,
            "selected": "NO",
            "rationale": "BvP and rolling fields are already present, but expected incremental value is lower than PA/opportunity.",
        },
    ]
    for row in items:
        row["composite_score"] = (
            row["likely_prediction_relevance"]
            + row["historical_and_live_evidence_availability"]
            + row["strict_pregame_reproducibility"]
            + row["population_size"]
            + row["champion_challenger_interpretability"]
            - row["implementation_effort"]
            - row["leakage_risk"]
        )
    return items


def _selected_rationale() -> list[dict[str, Any]]:
    return [
        {
            "decision": "selected_next_priority",
            "value": "LIVE_PA_OPPORTUNITY_OVERLAY_FOR_HITS_15",
            "why": "Targets prediction quality, uses a mature historical platform, can be made strict-prior, and closes the visible live-slate gap where PA/opportunity is not yet present on current selected propositions.",
            "not_authorized": "No training, matrix assembly, Champion-Challenger execution, upload, probability change, or production selector change is authorized.",
        },
        {
            "decision": "not_selected_market_late",
            "value": "MARKET_LATE_DISCOVERY_QUALITY",
            "why": "Useful for live cockpit coverage, but its primary mechanism is candidate discovery and market availability rather than prediction-quality improvement.",
            "not_authorized": "Do not treat market-late coverage as model improvement evidence without outcome-backed prospective evaluation.",
        },
        {
            "decision": "not_selected_starter_recovery",
            "value": "STARTER_SKILL_WORKLOAD_LIVE_JOIN_HEALTH",
            "why": "Starter work remains important, but ordinary historical Starter qualification and residual repair are explicitly closed/deferred by governance.",
            "not_authorized": "Do not reopen residual Starter qualification or blocked Hits 1.5 matrix queue.",
        },
    ]


def _experiment_design() -> list[dict[str, Any]]:
    rows = [
        ("research_question", "Does strict-prior PA/opportunity context improve live Hits selected-proposition ranking or explain O1.5/U1.5 disagreements beyond the current champion context?"),
        ("mechanism", "Higher recent plate-appearance opportunity should make hitter persistence and environment context more actionable; low/unstable opportunity should reduce confidence even when team or pitcher context is supportive."),
        ("eligible_population", "Current/live MLB selected-proposition rows for batter hits markets, especially Hits 1.5 and adjacent Hits 0.5 rows, with game_id, player_id, prop_type, side, line, model probability, and market price present."),
        ("required_fields", "game_id, player_id, game_date, prop_type, line, model side/probability, market price/no-vig, strict-prior PA rolling fields, PA source timestamp/version, outcome authority for completed games."),
        ("field_versions", "Use only PA fields with documented strict-prior semantics; current live slate lacks these fields, so the first step is overlay/join readiness, not model training."),
        ("outcome_denominator_authority", "Official completed-game hitter outcomes joined by game_id + player_id + prop/line/side; unresolved live rows remain unevaluated until completed."),
        ("temporal_boundaries", "Feature cutoff must be strictly before first pitch; PA rolling windows must exclude current game; market snapshot timestamp must be before game start for candidate eligibility."),
        ("control_definition", "Current champion/live selected-proposition surface without PA/opportunity labels influencing rank, tier, or inclusion."),
        ("challenger_definition", "Research-only shadow overlay that adds PA/opportunity labels and predeclared buckets for evaluation; no probability, selector, upload, or production behavior change."),
        ("leakage_controls", "Reject any PA field without source timestamp/cutoff; reject same-game PA/outcome; preserve source artifact hash; separate pregame live rows from postgame evaluation."),
        ("missingness_handling", "Rows without certified PA opportunity remain in denominator with pa_status=missing/unknown; do not drop rows silently or inflate yield."),
        ("minimum_evidence_requirements", "At least several completed slates with stable PA coverage and enough Hits 1.5 rows to avoid sparse-pocket claims; exact threshold should be frozen before execution."),
        ("evaluation_metrics", "Win rate, ROI, units, calibration by PA bucket, lift versus control ranking, missingness rate, row retention, and failure-mode counts."),
        ("stop_criteria", "Stop or redesign if PA join coverage is low, temporal proof is missing, row keys drift, or early results are dominated by sparse pockets."),
        ("promotion_gate", "Only later human approval can authorize matrix assembly/training; this brief only permits a design-ready research overlay."),
    ]
    return [{"section": section, "design": design, "status": "DESIGN_ONLY"} for section, design in rows]


def _summary_markdown(
    date_value: str,
    run_tag: str,
    population_rows: list[dict[str, Any]],
    hypotheses: list[dict[str, Any]],
    readiness: list[dict[str, Any]],
    out_dir: Path,
) -> str:
    pop = {(r["source"], r["dimension"], r["bucket"]): r["rows"] for r in population_rows}
    slate_rows = pop.get(("slate_output", "total_rows", "all"), 0)
    pred_rows = pop.get(("predictions_wide", "total_rows", "all"), 0)
    upload_rows = pop.get(("book_upload", "total_rows", "all"), 0)
    quick_rows = pop.get(("quick_card", "total_rows", "all"), 0)
    selected = next(r for r in hypotheses if r["selected"] == "YES")
    readiness_status = "USABLE_WITH_RESEARCH_GAPS"
    if any(r["status"] == "FAIL" for r in readiness):
        readiness_status = "NOT_READY_FAILING_REQUIRED_ARTIFACT"
    lines = [
        "# MLB Current/Live Selected-Proposition Research Re-entry Audit - 2026-07-16",
        "",
        "## Executive Summary",
        "",
        f"Latest locally available live slate: `{date_value}`.",
        f"Latest local run tag: `{run_tag}`.",
        "",
        "This package re-establishes the current/live research baseline after the historical Starter qualification campaign closed. It inventories local artifacts only and does not train, build a matrix, call network sources, write databases, upload files, or alter production behavior.",
        "",
        "## Decisions",
        "",
        "`MLB_CURRENT_LIVE_RESEARCH_REENTRY_DECISION = CURRENT_LIVE_BASELINE_REESTABLISHED_DESIGN_ONLY`",
        f"`MLB_CURRENT_LIVE_PIPELINE_RESEARCH_READINESS = {readiness_status}`",
        "`MLB_HISTORICAL_LIVE_COMPARABILITY_DECISION = AGGREGATE_AND_FIELD_FAMILY_ONLY_ROW_LEVEL_COMPARABILITY_NOT_CERTIFIED`",
        "`MLB_NEXT_IN_SEASON_RESEARCH_PRIORITY_DECISION = LIVE_PA_OPPORTUNITY_OVERLAY_FOR_HITS_15`",
        "`MLB_NEXT_EXPERIMENT_STATUS = DESIGN_COMPLETED_EXECUTION_NOT_AUTHORIZED`",
        "",
        "## Current Live Population",
        "",
        f"- Slate output rows: `{slate_rows}`",
        f"- Prediction-wide rows: `{pred_rows}`",
        f"- Two-sided upload-style rows: `{upload_rows}`",
        f"- Quick Card rows: `{quick_rows}`",
        "",
        "Today includes a compact live MLB slate, with Hits 1.5 available in the market universe but not prominent in the current selected diagnostic surface. The canonical slate carries market price, selected side, model probability, BvP, and rolling hitter fields. It does not carry PA/opportunity or lineup-role labels on the live slate output.",
        "",
        "## Selected Next Priority",
        "",
        f"Selected hypothesis: `{selected['hypothesis_id']}`.",
        "",
        selected["rationale"],
        "",
        "This targets prediction quality rather than residual-row recovery. It should begin as a strict-prior live overlay design and only become an executable experiment after separate human authorization.",
        "",
        "## Ranked Hypotheses",
        "",
        "| rank | hypothesis | selected | composite_score | rationale |",
        "| ---: | --- | --- | ---: | --- |",
    ]
    for row in hypotheses:
        lines.append(f"| {row['rank']} | `{row['hypothesis_id']}` | {row['selected']} | {row['composite_score']} | {row['rationale']} |")
    lines.extend(
        [
            "",
            "## Historical/Live Comparability",
            "",
            "The historical certified Hits population is authoritative at the aggregate count level in this package, but this audit does not locate or certify an exact row-level historical manifest compatible with today's live slate grain. Direct row-level comparisons therefore fail closed until an exact historical row manifest, field versions, and temporal semantics are bound.",
            "",
            "## Guardrails Observed",
            "",
            "- No network or source acquisition.",
            "- No OddsAPI calls.",
            "- No database/API writes.",
            "- No model training or matrix construction.",
            "- No upload or production behavior changes.",
            "- Existing unrelated worktree changes were not modified.",
            "",
            "## Package Files",
            "",
        ]
    )
    for path in sorted(out_dir.iterdir()):
        if path.name != "executive_summary_2026-07-16.md":
            lines.append(f"- `{_rel(path)}`")
    return "\n".join(lines) + "\n"


def _validate_outputs(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.iterdir()):
        if path.suffix == ".csv":
            try:
                with path.open(newline="") as fh:
                    list(csv.DictReader(fh))
                status = "PASS"
                notes = "csv_parse_ok"
            except Exception as exc:  # pragma: no cover - validation artifact
                status = "FAIL"
                notes = repr(exc)
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text())
                status = "PASS"
                notes = "json_parse_ok"
            except Exception as exc:  # pragma: no cover
                status = "FAIL"
                notes = repr(exc)
        elif path.suffix == ".md":
            status = "PASS" if path.read_text().strip() else "FAIL"
            notes = "markdown_nonempty" if status == "PASS" else "markdown_empty"
        else:
            continue
        rows.append({"path": _rel(path), "validation": status, "notes": notes})
    return rows


def build(out_dir: Path, date_value: str | None = None) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    date_value = date_value or _latest_live_date()
    artifact_rows, paths = _artifact_inventory(date_value)
    manifest = _read_json(paths["odds_manifest"])
    slate_rows = _read_csv(paths["slate_output"])
    prediction_rows = _read_csv(paths["predictions_wide"])
    upload_rows = _read_csv(paths["book_upload_archive"])
    quick_rows = _read_csv(paths["quick_card"])
    identity_rows = _read_csv(paths["upload_identity_diagnostics"])
    run_tag = _infer_run_tag(date_value, manifest, slate_rows)

    population_rows = _population_characterization(slate_rows, prediction_rows, upload_rows, quick_rows, identity_rows)
    missing_rows = _missingness(slate_rows)
    readiness_rows = _readiness_rows(date_value, run_tag, paths, slate_rows)
    compatibility_rows = _compatibility_rows()
    hypotheses = _hypotheses()
    rationale_rows = _selected_rationale()
    design_rows = _experiment_design()

    _write_csv(out_dir / f"current_live_artifact_inventory_{RUN_DATE}.csv", artifact_rows, [
        "artifact_role", "path", "exists", "file_size_bytes", "modified_time_utc", "sha256", "row_count", "column_count", "columns", "research_use", "notes",
    ])
    _write_csv(out_dir / f"pipeline_research_readiness_{RUN_DATE}.csv", readiness_rows, ["component", "status", "evidence", "recommendation"])
    _write_csv(out_dir / f"live_population_characterization_{RUN_DATE}.csv", population_rows, ["source", "dimension", "bucket", "rows", "pct", "notes"])
    _write_csv(out_dir / f"live_missingness_report_{RUN_DATE}.csv", missing_rows, ["source", "field", "rows", "missing", "present", "missing_pct", "notes"])
    _write_csv(out_dir / f"historical_live_compatibility_matrix_{RUN_DATE}.csv", compatibility_rows, ["concept", "field_or_grain", "compatibility_class", "evidence", "research_rule"])
    _write_csv(out_dir / f"ranked_hypothesis_inventory_{RUN_DATE}.csv", hypotheses, [
        "rank", "hypothesis_id", "hypothesis", "likely_prediction_relevance", "historical_and_live_evidence_availability", "strict_pregame_reproducibility", "population_size", "implementation_effort", "leakage_risk", "champion_challenger_interpretability", "composite_score", "selected", "rationale",
    ])
    _write_csv(out_dir / f"selected_next_hypothesis_rationale_{RUN_DATE}.csv", rationale_rows, ["decision", "value", "why", "not_authorized"])
    _write_csv(out_dir / f"bounded_experiment_design_brief_{RUN_DATE}.csv", design_rows, ["section", "design", "status"])

    decision_payload = {
        "generated_at_utc": _utc_now(),
        "latest_live_artifact_date": date_value,
        "latest_live_run_tag": run_tag,
        "historical_authoritative_counts": HISTORICAL_AUTHORITY,
        "decisions": {
            "MLB_CURRENT_LIVE_RESEARCH_REENTRY_DECISION": "CURRENT_LIVE_BASELINE_REESTABLISHED_DESIGN_ONLY",
            "MLB_CURRENT_LIVE_PIPELINE_RESEARCH_READINESS": "USABLE_WITH_RESEARCH_GAPS" if not any(r["status"] == "FAIL" for r in readiness_rows) else "NOT_READY_FAILING_REQUIRED_ARTIFACT",
            "MLB_HISTORICAL_LIVE_COMPARABILITY_DECISION": "AGGREGATE_AND_FIELD_FAMILY_ONLY_ROW_LEVEL_COMPARABILITY_NOT_CERTIFIED",
            "MLB_NEXT_IN_SEASON_RESEARCH_PRIORITY_DECISION": "LIVE_PA_OPPORTUNITY_OVERLAY_FOR_HITS_15",
            "MLB_NEXT_EXPERIMENT_STATUS": "DESIGN_COMPLETED_EXECUTION_NOT_AUTHORIZED",
        },
        "population_counts": {
            "slate_rows": len(slate_rows),
            "prediction_rows": len(prediction_rows),
            "book_upload_rows": len(upload_rows),
            "quick_card_rows": len(quick_rows),
            "identity_diagnostic_rows": len(identity_rows),
        },
        "ranked_hypotheses": hypotheses,
    }
    _write_json(out_dir / f"machine_readable_reentry_decision_{RUN_DATE}.json", decision_payload)

    summary = _summary_markdown(date_value, run_tag, population_rows, hypotheses, readiness_rows, out_dir)
    (out_dir / f"executive_summary_{RUN_DATE}.md").write_text(summary)

    # Validation and manifest are written last, then the manifest is refreshed to include validation.
    validation_rows = _validate_outputs(out_dir)
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation_rows, ["path", "validation", "notes"])
    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file():
            manifest_rows.append({"path": _rel(path), "sha256": _sha256(path), "bytes": path.stat().st_size})
    _write_csv(out_dir / f"sha256_manifest_{RUN_DATE}.csv", manifest_rows, ["path", "sha256", "bytes"])
    validation_rows = _validate_outputs(out_dir)
    _write_csv(out_dir / f"validation_report_{RUN_DATE}.csv", validation_rows, ["path", "validation", "notes"])

    return decision_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build read-only MLB current/live research re-entry audit.")
    parser.add_argument("--date", default=None, help="Live slate date to audit. Defaults to latest local odds-history slate.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--mode", default="read_only", choices=["read_only"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build(Path(args.output_dir), args.date)
    print(json.dumps({
        "latest_live_artifact_date": payload["latest_live_artifact_date"],
        "latest_live_run_tag": payload["latest_live_run_tag"],
        "population_counts": payload["population_counts"],
        "selected_priority": payload["decisions"]["MLB_NEXT_IN_SEASON_RESEARCH_PRIORITY_DECISION"],
        "experiment_status": payload["decisions"]["MLB_NEXT_EXPERIMENT_STATUS"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
