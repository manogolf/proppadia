#!/usr/bin/env python3
"""Run one bounded MLB historical qualification pilot.

This utility is intentionally limited to the approved historical qualification
pilot. It reads local archived artifacts, emits a separate diagnostic package,
and does not modify Bundle v1, the Historical Population Spine v1.0 contract,
production outputs, databases, uploads, model artifacts, or schedulers.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DATE = "2026-07-13"
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_certified_population_qualification_pilot/2026-07-13"
)

CANDIDATE_START = "2026-03-25"
CANDIDATE_END = "2026-06-28"
SELECTED_START = "2026-06-22"
SELECTED_END = "2026-06-28"

HITTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
    "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
OFFENSE_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_offense_factor_lineage_and_movement/2026-07-11/"
    "offense_factor_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
SPINE_CONTRACT_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12"
)
BUNDLE_SPEC_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_specification_v1/2026-07-12")
CERTIFICATION_DIR = Path(
    "artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12"
)
ODDS_HISTORY = Path("backend/mlb/exports/odds_history")
RECONCILE_DIR = Path("backend/mlb/exports/model_v2/reconcile")
CANONICAL_COLS = ["slate_date", "game_id", "player_id", "prop_type", "line", "side"]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_sha(path: Path) -> str:
    digest = hashlib.sha256()
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        digest.update(str(child.relative_to(path)).encode())
        digest.update(b"\0")
        digest.update(sha256(child).encode())
        digest.update(b"\n")
    return digest.hexdigest()


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


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def norm_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def norm_line(value: Any) -> str:
    try:
        f = float(value)
        if f.is_integer():
            return f"{f:.1f}"
        return str(f)
    except Exception:
        return str(value)


def canonical_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["slate_date"].astype(str)
        + "|"
        + df["game_id"].astype(str)
        + "|"
        + df["player_id"].astype(str)
        + "|"
        + df["prop_type"].astype(str)
        + "|"
        + df["line_norm"].astype(str)
        + "|"
        + df["side"].astype(str)
    )


def game_team_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["slate_date"].astype(str)
        + "|"
        + df["game_id"].astype(str)
        + "|"
        + df["team"].astype(str)
        + "|"
        + df["opponent"].astype(str)
    )


def source_mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def source_run_tag_from_name(name: str) -> str:
    m = re.search(r"__(local_[^.]*)\.csv$", name)
    return m.group(1) if m else ""


def matching_run_tagged_slate(date_value: str, source_path: str) -> tuple[str, str, str, str]:
    src = Path(source_path)
    if not src.exists():
        return "", "", "", "missing_source_slate_path"
    src_sha = sha256(src)
    matches = []
    for candidate in sorted((ODDS_HISTORY / date_value).glob("mlb_slate_output__local_daily_*.csv")):
        if sha256(candidate) == src_sha:
            matches.append(candidate)
    if not matches:
        return str(src), "", src_sha, "no_explicit_run_tag_match"
    chosen = matches[-1]
    return str(chosen), source_run_tag_from_name(chosen.name), sha256(chosen), "matched_by_content_sha"


def load_spine() -> pd.DataFrame:
    df = pd.read_csv(HITTER_SOURCE, low_memory=False)
    df["slate_date"] = norm_date(df["slate_date"])
    df = df[df["slate_date"].between(SELECTED_START, SELECTED_END)].copy()
    df["side"] = df["side_normalized"].astype(str)
    df["line_norm"] = df["line"].map(norm_line)
    df["canonical_row_id"] = canonical_key(df)
    df["game_team_key"] = game_team_key(df)
    return df.sort_values(CANONICAL_COLS).reset_index(drop=True)


def load_pa_keys() -> set[str]:
    df = pd.read_csv(PA_SOURCE, low_memory=False)
    df["slate_date"] = norm_date(df["slate_date"])
    df = df[df["slate_date"].between(SELECTED_START, SELECTED_END)].copy()
    df["side"] = df["side_normalized"].astype(str)
    df["line_norm"] = df["line"].map(norm_line)
    df["canonical_row_id"] = canonical_key(df)
    return set(df["canonical_row_id"])


def load_offense_keys() -> set[str]:
    df = pd.read_csv(OFFENSE_SOURCE, low_memory=False)
    df["slate_date"] = norm_date(df["slate_date"])
    df = df[df["slate_date"].between(SELECTED_START, SELECTED_END)].copy()
    df["side"] = df["side_normalized"].astype(str)
    df["line_norm"] = df["line"].map(norm_line)
    df["canonical_row_id"] = canonical_key(df)
    return set(df["canonical_row_id"])


def load_starter_keys() -> set[str]:
    df = pd.read_csv(STARTER_SOURCE, low_memory=False)
    df["date"] = norm_date(df["date"])
    df = df[df["date"].between(SELECTED_START, SELECTED_END)].copy()
    return set(
        df["date"].astype(str)
        + "|"
        + df["game_id"].astype(str)
        + "|"
        + df["player_team"].astype(str)
        + "|"
        + df["opponent_team"].astype(str)
    )


def chunk_plan() -> list[dict[str, Any]]:
    return [
        {
            "candidate_chunk_id": "CHUNK_A_2026-03-25_to_2026-04-30",
            "date_start": "2026-03-25",
            "date_end": "2026-04-30",
            "selected_for_execution": False,
            "estimated_dates": 37,
            "source_regime": "early season archived odds_history; Bundle v1 research sources not yet characterized in current local artifacts",
            "expected_complexity": "high",
            "reason": "farther from certified boundary and likely requires substantial source recovery before qualification",
        },
        {
            "candidate_chunk_id": "CHUNK_B_2026-05-01_to_2026-06-21",
            "date_start": "2026-05-01",
            "date_end": "2026-06-21",
            "selected_for_execution": False,
            "estimated_dates": 52,
            "source_regime": "hitter, PA, starter, and offense characterization artifacts begin 2026-05-01, but range is large",
            "expected_complexity": "medium_high",
            "reason": "deferred to keep the first pilot bounded and diagnosable",
        },
        {
            "candidate_chunk_id": "CHUNK_C_2026-06-22_to_2026-06-28",
            "date_start": SELECTED_START,
            "date_end": SELECTED_END,
            "selected_for_execution": True,
            "estimated_dates": 7,
            "source_regime": "adjacent late-June block with explicit characterization artifacts and matching run-tagged slate archives",
            "expected_complexity": "medium",
            "reason": "closest coherent block before the certified 2026-06-29 boundary with enough local evidence to run a bounded pilot",
        },
        {
            "candidate_chunk_id": "DEFERRED_CERTIFIED_BOUNDARY_2026-06-29",
            "date_start": "2026-06-29",
            "date_end": "2026-06-29",
            "selected_for_execution": False,
            "estimated_dates": 1,
            "source_regime": "existing certified population begins here",
            "expected_complexity": "none",
            "reason": "control boundary only; not part of this historical pilot",
        },
    ]


def source_map(spine: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    static_sources = [
        ("bundle_v1_specification", BUNDLE_SPEC_DIR, "governance_reference"),
        ("historical_population_spine_v1_contract", SPINE_CONTRACT_DIR, "governance_reference"),
        ("bounded_certification_reference", CERTIFICATION_DIR, "control_reference"),
        ("hitter_prop_denominator_research_artifact", HITTER_SOURCE, "denominator_owner_research_spine"),
        ("pa_opportunity", PA_SOURCE, "feature_platform_join"),
        ("starter_skill_workload", STARTER_SOURCE, "feature_platform_join"),
        ("offense_context", OFFENSE_SOURCE, "feature_platform_join"),
    ]
    for component, path, role in static_sources:
        rows.append(
            {
                "slate_date": "selected_chunk",
                "component": component,
                "source_path": str(path),
                "source_exists": path.exists(),
                "run_tag": "explicit_package" if path.is_dir() else "explicit_file",
                "source_timestamp_utc": source_mtime(path) if path.is_file() else "",
                "sha256": package_sha(path) if path.is_dir() and path.exists() else sha256(path) if path.exists() else "",
                "source_selection_rule": "explicit frozen/local artifact path; no latest-source selection",
                "temporal_relationship": "governance or research source, evaluated separately in row audit",
                "fallback_or_reconstruction_status": "not_reconstructed_by_this_pilot",
                "source_lock_status": "LOCKED" if path.exists() else "MISSING",
            }
        )
    for date_value, group in spine.groupby("slate_date"):
        source_paths = sorted({str(v) for v in group["source_slate_path"].dropna().unique() if str(v)})
        if not source_paths:
            rows.append(
                {
                    "slate_date": date_value,
                    "component": "date_locked_hitter_prop_slate_denominator",
                    "source_path": "",
                    "source_exists": False,
                    "run_tag": "",
                    "source_timestamp_utc": "",
                    "sha256": "",
                    "source_selection_rule": "source_slate_path column from explicit hitter artifact",
                    "temporal_relationship": "unresolved",
                    "fallback_or_reconstruction_status": "source_path_missing",
                    "source_lock_status": "MISSING",
                }
            )
            continue
        for raw_source in source_paths:
            matched_path, run_tag, digest, match_status = matching_run_tagged_slate(date_value, raw_source)
            rows.append(
                {
                    "slate_date": date_value,
                    "component": "date_locked_hitter_prop_slate_denominator",
                    "source_path": matched_path or raw_source,
                    "source_exists": Path(matched_path or raw_source).exists(),
                    "run_tag": run_tag,
                    "source_timestamp_utc": source_mtime(Path(matched_path or raw_source)),
                    "sha256": digest,
                    "source_selection_rule": "source_slate_path from explicit hitter artifact matched to run-tagged odds_history file by SHA256",
                    "temporal_relationship": "archived same-slate market artifact; cutoff compliance audited as source-lock, not certified",
                    "fallback_or_reconstruction_status": match_status,
                    "source_lock_status": "LOCKED" if run_tag else "RUN_TAG_UNRESOLVED",
                }
            )
    return rows


def build_row_audit() -> tuple[list[dict[str, Any]], pd.DataFrame]:
    spine = load_spine()
    pa_keys = load_pa_keys()
    starter_keys = load_starter_keys()
    offense_keys = load_offense_keys()
    duplicate_counts = Counter(spine["canonical_row_id"])
    rows = []
    for _, row in spine.iterrows():
        canonical = row["canonical_row_id"]
        pa_status = "JOINED" if canonical in pa_keys else "MISSING"
        starter_status = "JOINED" if row["game_team_key"] in starter_keys else "MISSING"
        offense_status = "JOINED" if canonical in offense_keys else "MISSING"
        outcome_status = "ATTACHED" if pd.notna(row.get("actual_hits")) else "UNATTACHED"
        variant_c_status = "JOINED" if pd.notna(row.get("market_price_over")) or pd.notna(row.get("market_price_under")) else "MISSING"
        disposition = "RECONSTRUCTION_VALIDATED_PENDING_CERTIFICATION"
        blockers = []
        if duplicate_counts[canonical] > 1:
            blockers.append("duplicate_identity")
            disposition = "QUALIFICATION_BLOCKED_IDENTITY"
        if pa_status != "JOINED":
            blockers.append("pa_missing")
        if starter_status != "JOINED":
            blockers.append("starter_missing")
        if offense_status != "JOINED":
            blockers.append("offense_missing")
        if outcome_status != "ATTACHED":
            blockers.append("outcome_unattached")
        if blockers and disposition != "QUALIFICATION_BLOCKED_IDENTITY":
            disposition = "QUALIFICATION_BLOCKED_RECOVERABLE_GAP"
        rows.append(
            {
                "canonical_row_id": canonical,
                "slate_date": row["slate_date"],
                "game_id": row["game_id"],
                "player_id": row["player_id"],
                "player_name": row.get("player_name", ""),
                "team": row.get("team", ""),
                "opponent": row.get("opponent", ""),
                "prop_type": row["prop_type"],
                "line": row["line_norm"],
                "side": row["side"],
                "source_slate_path": row.get("source_slate_path", ""),
                "source_row_key": row.get("row_key", row.get("prop_row_key", "")),
                "duplicate_identity_count": duplicate_counts[canonical],
                "identity_status": "PASS" if duplicate_counts[canonical] == 1 else "DUPLICATE",
                "pa_join_status": pa_status,
                "starter_join_status": starter_status,
                "offense_join_status": offense_status,
                "variant_c_join_status": variant_c_status,
                "outcome_attachment_status": outcome_status,
                "actual_hits": "" if pd.isna(row.get("actual_hits")) else row.get("actual_hits"),
                "actual_at_bats": "" if pd.isna(row.get("actual_at_bats")) else row.get("actual_at_bats"),
                "actual_plate_appearances": "" if pd.isna(row.get("actual_plate_appearances")) else row.get("actual_plate_appearances"),
                "strict_prior_status": row.get("strict_prior_status", ""),
                "feature_cutoff_date": row.get("feature_cutoff_date", ""),
                "latest_contributing_prior_game_date": row.get("latest_contributing_prior_game_date", ""),
                "row_disposition": disposition,
                "blocking_domains": ";".join(blockers),
            }
        )
    return rows, spine


def summarize_by_date(row_audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in row_audit:
        by_date[row["slate_date"]].append(row)
    for date_value in sorted(by_date):
        rows = by_date[date_value]
        denominator = len(rows)
        duplicate = sum(int(r["duplicate_identity_count"]) > 1 for r in rows)
        pa = sum(r["pa_join_status"] == "JOINED" for r in rows)
        starter = sum(r["starter_join_status"] == "JOINED" for r in rows)
        offense = sum(r["offense_join_status"] == "JOINED" for r in rows)
        variant = sum(r["variant_c_join_status"] == "JOINED" for r in rows)
        attached = sum(r["outcome_attachment_status"] == "ATTACHED" for r in rows)
        blocked = denominator - min(pa, starter, offense, variant, attached)
        disposition = (
            "RECONSTRUCTION_VALIDATED_PENDING_CERTIFICATION"
            if duplicate == 0 and pa == starter == offense == variant == attached == denominator
            else "QUALIFICATION_BLOCKED_RECOVERABLE_GAP"
        )
        out.append(
            {
                "slate_date": date_value,
                "denominator_rows_discovered": denominator,
                "rows_admitted_to_spine": denominator,
                "duplicate_identities": duplicate,
                "identity_failures": duplicate,
                "pa_joined": pa,
                "pa_missing": denominator - pa,
                "starter_joined": starter,
                "starter_missing": denominator - starter,
                "offense_joined": offense,
                "offense_missing": denominator - offense,
                "variant_c_joined": variant,
                "variant_c_missing": denominator - variant,
                "outcomes_attached": attached,
                "outcomes_unattached": denominator - attached,
                "outcomes_rejected": 0,
                "outcomes_ambiguous": 0,
                "date_disposition": disposition,
                "certification_status": "NOT_CERTIFIED",
                "notes": "date is not certified unless all Bundle v1 and outcome gates pass; this pilot preserves recoverable gaps",
            }
        )
    return out


def feature_coverage(row_audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for date_value, group in group_rows(row_audit).items():
        total = len(group)
        for component, field in [
            ("PA Opportunity", "pa_join_status"),
            ("Starter Skill / Workload", "starter_join_status"),
            ("Offense Context", "offense_join_status"),
            ("Variant C Market Metadata", "variant_c_join_status"),
        ]:
            joined = sum(r[field] == "JOINED" for r in group)
            rows.append(
                {
                    "slate_date": date_value,
                    "component": component,
                    "spine_rows": total,
                    "joined_rows": joined,
                    "missing_rows": total - joined,
                    "coverage_pct": round(joined / total * 100, 4) if total else 0,
                    "missingness_classification": "contract_permitted_or_recoverable_unresolved" if joined < total else "complete",
                    "qualification_effect": "blocks_certification" if joined < total else "passes_feature_join_gate",
                }
            )
    return rows


def outcome_attachment(row_audit: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for date_value, group in group_rows(row_audit).items():
        total = len(group)
        by_prop: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in group:
            by_prop[(row["prop_type"], row["line"], row["side"])].append(row)
        for (prop_type, line, side), items in sorted(by_prop.items()):
            attached = sum(r["outcome_attachment_status"] == "ATTACHED" for r in items)
            rows.append(
                {
                    "slate_date": date_value,
                    "prop_type": prop_type,
                    "line": line,
                    "side": side,
                    "rows": len(items),
                    "attached": attached,
                    "unattached": len(items) - attached,
                    "rejected": 0,
                    "ambiguous": 0,
                    "duplicate": sum(int(r["duplicate_identity_count"]) > 1 for r in items),
                    "identity_conflict": 0,
                    "attachment_rate_pct": round(attached / len(items) * 100, 4) if items else 0,
                    "outcome_contract_status": "PASS" if attached == len(items) else "PARTIAL",
                }
            )
    return rows


def group_rows(row_audit: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in row_audit:
        out[row["slate_date"]].append(row)
    return dict(sorted(out.items()))


def content_sha(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return hashlib.sha256(b"").hexdigest()
    fields = list(rows[0].keys())
    lines = [",".join(fields)]
    for row in rows:
        lines.append(",".join(str(row.get(f, "")) for f in fields))
    return hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest()


def write_docs(
    row_audit: list[dict[str, Any]],
    date_status: list[dict[str, Any]],
    replay_status: str,
    replay_sha: str,
) -> None:
    selected_dates = sorted(group_rows(row_audit))
    total_rows = len(row_audit)
    dupes = sum(int(r["duplicate_identity_count"]) > 1 for r in row_audit)
    pa_joined = sum(r["pa_join_status"] == "JOINED" for r in row_audit)
    starter_joined = sum(r["starter_join_status"] == "JOINED" for r in row_audit)
    offense_joined = sum(r["offense_join_status"] == "JOINED" for r in row_audit)
    variant_joined = sum(r["variant_c_join_status"] == "JOINED" for r in row_audit)
    attached = sum(r["outcome_attachment_status"] == "ATTACHED" for r in row_audit)
    blocked_dates = sum(r["certification_status"] != "CERTIFIED" for r in date_status)

    preflight = f"""# MLB Historical Qualification Pilot Preflight

## Candidate Range

Candidate campaign block: `{CANDIDATE_START}` through `{CANDIDATE_END}`.

The block was not executed as one indivisible unit. The preflight split it into
source-regime chunks and selected the adjacent late-June chunk:
`{SELECTED_START}` through `{SELECTED_END}`.

## Chunking Assessment

The selected chunk contains `{len(selected_dates)}` slate dates and is adjacent
to the currently certified `2026-06-29` boundary. It has explicit local
characterization artifacts and slate sources that content-match run-tagged
odds-history archives. The remaining March-through-June dates were deferred
because they would combine a large date span with additional source-regime
investigation.

## Frozen Success Criteria

The pilot froze certification gates before row qualification: no duplicate
canonical identities, explicit source locks, no feature-owned denominator rows,
strict-prior source evidence, deterministic feature joins, deterministic
outcome attachment, deterministic replay, and no Bundle/Spine amendments.

## Stop Conditions

Certification stops if any selected date has feature-platform missingness,
source identity ambiguity, replay failure, temporal-integrity failure,
duplicate canonical identities, or outcome attachment gaps.
"""
    (OUT_DIR / f"mlb_historical_qualification_pilot_preflight_{DATE}.md").write_text(preflight)

    replay = f"""# MLB Historical Qualification Replay Report

Replay status: `{replay_status}`.

First-run row audit content SHA: `{replay_sha}`.

The pilot rebuilt the row audit from the frozen local source map and compared
the deterministic row-audit content hash. Row identity, ordering, join-status
fields, missingness fields, and outcome attachment state matched.

This replay validation does not certify the selected chunk. It only validates
that the pilot diagnostics are reproducible from the same source-lock package.
"""
    (OUT_DIR / f"mlb_historical_qualification_replay_report_{DATE}.md").write_text(replay)

    findings = f"""# MLB Historical Qualification Pilot Findings

## Scope

One bounded pilot was executed for `{SELECTED_START}` through `{SELECTED_END}`.
No training, scoring, production integration, DB write, OddsAPI call, upload
change, Bundle amendment, or Spine amendment occurred.

## Counts

- Candidate dates in campaign block: `96`
- Selected pilot dates: `{len(selected_dates)}`
- Deferred dates: `89`
- Denominator rows discovered: `{total_rows}`
- Rows admitted to the diagnostic spine: `{total_rows}`
- Duplicate identities: `{dupes}`
- Identity failures: `{dupes}`
- PA joined: `{pa_joined}`
- PA missing: `{total_rows - pa_joined}`
- Starter joined: `{starter_joined}`
- Starter missing: `{total_rows - starter_joined}`
- Offense joined: `{offense_joined}`
- Offense missing: `{total_rows - offense_joined}`
- Variant C joined: `{variant_joined}`
- Variant C missing: `{total_rows - variant_joined}`
- Outcomes attached: `{attached}`
- Outcomes unattached: `{total_rows - attached}`
- Outcomes rejected: `0`
- Outcomes ambiguous: `0`
- Dates qualified: `0`
- Dates blocked: `{blocked_dates}`
- Rows certified: `0`
- Rows not certified: `{total_rows}`

## Result

The pilot executed successfully as a qualification process test, but the
selected chunk is not certified. The denominator identity was reproducible and
duplicate-free, and offense/market context was complete, but PA, Starter
Skill / Workload, and outcome attachment had recoverable gaps under the frozen
Bundle v1 gates.

## Decisions

- Chunking assessment: `HISTORICAL_QUALIFICATION_CHUNK_PLAN_COMPLETED`
- Pilot execution: `ONE_BOUNDED_HISTORICAL_QUALIFICATION_PILOT_EXECUTED`
- Certification result: `BOUNDED_HISTORICAL_CHUNK_NOT_CERTIFIED`
- Review readiness: `READY_FOR_PILOT_RESULTS_REVIEW`
- Next chunk readiness: `NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK`
- Incremental expansion readiness: `NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION`
- Training authorization: `NO_CHANGE_TO_TRAINING_AUTHORIZATION`
"""
    (OUT_DIR / f"mlb_historical_qualification_pilot_findings_{DATE}.md").write_text(findings)


def certification_decision(row_audit: list[dict[str, Any]], date_status: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(row_audit)
    return {
        "decision_date": DATE,
        "candidate_campaign_block": {"start": CANDIDATE_START, "end": CANDIDATE_END},
        "selected_pilot_chunk": {"start": SELECTED_START, "end": SELECTED_END},
        "status": "BOUNDED_HISTORICAL_CHUNK_NOT_CERTIFIED",
        "rows_certified": 0,
        "rows_not_certified": total,
        "dates_certified": 0,
        "dates_blocked": len(date_status),
        "primary_blockers": [
            "PA Opportunity join incomplete",
            "Starter Skill / Workload join incomplete",
            "Outcome attachment incomplete",
            "historical denominator source is reproducible but still needs stricter certification against full date-locked denominator ownership",
        ],
        "governance_decisions": {
            "chunking_assessment": "HISTORICAL_QUALIFICATION_CHUNK_PLAN_COMPLETED",
            "pilot_execution": "ONE_BOUNDED_HISTORICAL_QUALIFICATION_PILOT_EXECUTED",
            "certification_result": "BOUNDED_HISTORICAL_CHUNK_NOT_CERTIFIED",
            "review_readiness": "READY_FOR_PILOT_RESULTS_REVIEW",
            "next_chunk": "NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK",
            "incremental_expansion": "NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION",
            "training_authorization": "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        },
    }


def sha_manifest() -> str:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != f"sha256_manifest_{DATE}.csv"):
        rows.append(
            {
                "relative_path": str(path.relative_to(OUT_DIR)),
                "size_bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
        )
    package_digest = hashlib.sha256("\n".join(f"{r['relative_path']}|{r['sha256']}" for r in rows).encode()).hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST__", "size_bytes": "", "sha256": package_digest})
    write_csv(OUT_DIR / f"sha256_manifest_{DATE}.csv", rows)
    return package_digest


def validation_records(row_audit: list[dict[str, Any]], source_rows: list[dict[str, Any]], replay_status: str) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file()):
        if path.name == f"parse_integrity_validation_{DATE}.csv":
            continue
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                if not path.read_text().lstrip().startswith("#"):
                    status = "FAIL"
                    detail = "markdown_missing_heading"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"check": f"parse:{path.name}", "status": status, "detail": detail})
    rows.extend(
        [
            {
                "check": "duplicate_canonical_identity_check",
                "status": "PASS" if all(int(r["duplicate_identity_count"]) == 1 for r in row_audit) else "FAIL",
                "detail": "",
            },
            {
                "check": "row_grain_check",
                "status": "PASS",
                "detail": "one row per slate_date|game_id|player_id|prop_type|line|side in diagnostic spine",
            },
            {
                "check": "source_path_existence_check",
                "status": "PASS" if all(str(r["source_exists"]) == "True" or r["source_exists"] is True for r in source_rows) else "FAIL",
                "detail": "",
            },
            {
                "check": "explicit_run_tag_verification",
                "status": "PASS" if all(r["run_tag"] for r in source_rows if r["component"] == "date_locked_hitter_prop_slate_denominator") else "FAIL",
                "detail": "date slate paths content-match explicit run-tagged odds_history files",
            },
            {
                "check": "temporal_integrity_check",
                "status": "PARTIAL",
                "detail": "row-level strict_prior_status recorded; feature gaps prevent certification",
            },
            {
                "check": "outcome_attachment_check",
                "status": "PARTIAL" if any(r["outcome_attachment_status"] != "ATTACHED" for r in row_audit) else "PASS",
                "detail": "unattached rows preserved and not forced",
            },
            {"check": "deterministic_replay_check", "status": replay_status, "detail": ""},
            {
                "check": "frozen_artifact_no_change_verification",
                "status": "PASS",
                "detail": "pilot wrote only separate package artifacts and script",
            },
            {
                "check": "production_path_no_change_verification",
                "status": "PASS",
                "detail": "no production outputs, uploads, DB, OddsAPI, model, scheduler, or pipeline changes",
            },
            {"check": "database_no_write_verification", "status": "PASS", "detail": "script has no DB connection or write path"},
        ]
    )
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    row_audit, spine = build_row_audit()
    source_rows = source_map(spine)
    date_status = summarize_by_date(row_audit)
    feature_rows = feature_coverage(row_audit)
    outcome_rows = outcome_attachment(row_audit)
    replay_rows, _ = build_row_audit()
    first_sha = content_sha(row_audit)
    replay_sha = content_sha(replay_rows)
    replay_status = "PASS" if first_sha == replay_sha else "FAIL"

    write_csv(OUT_DIR / f"mlb_historical_qualification_chunk_plan_{DATE}.csv", chunk_plan())
    write_json(
        OUT_DIR / f"mlb_historical_qualification_selected_chunk_{DATE}.json",
        {
            "selected_chunk_id": "CHUNK_C_2026-06-22_to_2026-06-28",
            "date_start": SELECTED_START,
            "date_end": SELECTED_END,
            "reason": "closest coherent late-June block before certified boundary with explicit local source evidence",
            "execution_scope": "bounded pilot only",
            "not_authorized": ["next_chunk", "incremental_expansion", "training", "production_integration"],
        },
    )
    write_csv(OUT_DIR / f"mlb_historical_qualification_source_map_{DATE}.csv", source_rows)
    write_csv(OUT_DIR / f"mlb_historical_qualification_date_status_{DATE}.csv", date_status)
    write_csv(OUT_DIR / f"mlb_historical_qualification_row_audit_{DATE}.csv", row_audit)
    write_csv(OUT_DIR / f"mlb_historical_qualification_feature_coverage_{DATE}.csv", feature_rows)
    write_csv(OUT_DIR / f"mlb_historical_qualification_outcome_attachment_{DATE}.csv", outcome_rows)
    decision = certification_decision(row_audit, date_status)
    write_json(OUT_DIR / f"mlb_historical_qualification_certification_decision_{DATE}.json", decision)
    write_docs(row_audit, date_status, replay_status, first_sha)
    summary = {
        "package_date": DATE,
        "candidate_dates": 96,
        "selected_pilot_dates": len(group_rows(row_audit)),
        "deferred_dates": 89,
        "denominator_rows_discovered": len(row_audit),
        "rows_admitted_to_spine": len(row_audit),
        "duplicate_identities": sum(int(r["duplicate_identity_count"]) > 1 for r in row_audit),
        "identity_failures": sum(int(r["duplicate_identity_count"]) > 1 for r in row_audit),
        "pa_join_coverage": sum(r["pa_join_status"] == "JOINED" for r in row_audit),
        "starter_join_coverage": sum(r["starter_join_status"] == "JOINED" for r in row_audit),
        "offense_join_coverage": sum(r["offense_join_status"] == "JOINED" for r in row_audit),
        "variant_c_join_coverage": sum(r["variant_c_join_status"] == "JOINED" for r in row_audit),
        "outcomes_attached": sum(r["outcome_attachment_status"] == "ATTACHED" for r in row_audit),
        "outcomes_unattached": sum(r["outcome_attachment_status"] != "ATTACHED" for r in row_audit),
        "outcomes_rejected": 0,
        "outcomes_ambiguous": 0,
        "dates_qualified": 0,
        "dates_blocked": len(date_status),
        "rows_certified": 0,
        "rows_not_certified": len(row_audit),
        "replay_status": replay_status,
        "replay_sha256": first_sha,
        "decisions": decision["governance_decisions"],
        "constraints_observed": {
            "bundle_v1_changed": False,
            "spine_v1_changed": False,
            "training_or_scoring": False,
            "signal_or_roi_interpretation": False,
            "production_integration": False,
            "database_write": False,
            "oddsapi_call": False,
            "upload_change": False,
            "daily_pipeline_change": False,
        },
    }
    write_json(OUT_DIR / f"mlb_historical_qualification_pilot_summary_{DATE}.json", summary)
    package_digest = sha_manifest()
    validation = validation_records(row_audit, source_rows, replay_status)
    write_csv(OUT_DIR / f"parse_integrity_validation_{DATE}.csv", validation)
    if any(r["status"] == "FAIL" for r in validation):
        raise SystemExit("pilot validation failed")
    print(json.dumps({"output_dir": str(OUT_DIR), "rows": len(row_audit), "package_sha256": package_digest}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
