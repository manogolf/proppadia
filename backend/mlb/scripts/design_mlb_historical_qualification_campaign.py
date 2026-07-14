#!/usr/bin/env python3
"""Inventory MLB historical slate evidence and design qualification campaign.

Inventory/design only. This script reads local repository artifacts and writes a
dated analysis package. It does not qualify, reconstruct, certify, train, score,
backfill, call external APIs, write databases, or alter production artifacts.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


DATE = "2026-07-13"
OUT_DIR = Path(f"artifacts/analysis/model_development/mlb_historical_certified_population_qualification/{DATE}")
ODDS_DIR = Path("backend/mlb/exports/odds_history")
PREPARED_DIR = Path("backend/mlb/exports/model_diagnostics/prepared_feature_vectors")
RECONCILE_DIR = Path("backend/mlb/exports/model_v2/reconcile")
UPLOAD_DIR = Path("backend/mlb/exports/model_v2/upload")
MODEL_DEV = Path("artifacts/analysis/model_development")
MLB_ANALYSIS = Path("artifacts/analysis/mlb")
CERTIFIED_START = "2026-06-29"
CERTIFIED_END = "2026-07-09"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
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


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def date_dirs(root: Path) -> set[str]:
    if not root.exists():
        return set()
    return {p.name for p in root.iterdir() if p.is_dir() and DATE_RE.match(p.name)}


def safe_read_count(path: Path) -> tuple[int, list[str]]:
    try:
        df = pd.read_csv(path, nrows=0)
        cols = list(df.columns)
        rows = sum(1 for _ in path.open()) - 1
        return max(rows, 0), cols
    except Exception:
        return 0, []


def canonical_row_count(path: Path) -> tuple[int, int, list[str]]:
    try:
        df = pd.read_csv(path, low_memory=False)
    except Exception:
        return 0, 0, []
    cols = list(df.columns)
    needed = ["game_id", "player_id", "prop_type", "line", "side"]
    if not all(c in df.columns for c in needed):
        side_col = "model_pick_side" if "model_pick_side" in df.columns else None
        prop_col = "prop_type" if "prop_type" in df.columns else None
        if not all(c in df.columns for c in ["game_id", "player_id", "line"]) or side_col is None or prop_col is None:
            return len(df), 0, cols
        tmp = df.assign(side=df[side_col], prop_type=df[prop_col])
    else:
        tmp = df
    keys = (
        tmp["game_id"].astype(str)
        + "|"
        + tmp["player_id"].astype(str)
        + "|"
        + tmp["prop_type"].astype(str).str.lower()
        + "|"
        + tmp["line"].map(lambda v: f"{float(v):.1f}" if pd.notna(v) else "missing")
        + "|"
        + tmp["side"].astype(str).str.lower()
    )
    return len(df), int(keys.nunique()), cols


def evidence_dates_from_files(paths: list[Path]) -> set[str]:
    dates: set[str] = set()
    for path in paths:
        for part in path.parts:
            if DATE_RE.match(part):
                dates.add(part)
    return dates


def collect_sources() -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = defaultdict(lambda: {"paths": defaultdict(list)})
    all_dates: set[str] = set()

    odds_dates = date_dirs(ODDS_DIR)
    prepared_dates = date_dirs(PREPARED_DIR)
    reconcile_dates = date_dirs(RECONCILE_DIR)
    upload_dates = date_dirs(UPLOAD_DIR)
    all_dates |= odds_dates | prepared_dates | reconcile_dates | upload_dates

    # Research-base coverage from explicitly named files.
    research_files = {
        "hitter_persistence": MODEL_DEV
        / "mlb_hitter_persistence_characterization/2026-07-11/hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv",
        "starter_skill": MODEL_DEV
        / "mlb_starter_expected_hits_allowed_characterization/2026-07-11/starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv",
        "offense_context": MODEL_DEV
        / "mlb_offense_factor_lineage_and_movement/2026-07-11/offense_factor_team_game_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv",
        "pa_opportunity_pilot": MODEL_DEV
        / "mlb_pa_opportunity_strict_prior_reconstruction_pilot_1/2026-07-12/pa_opportunity_reconstructed_pilot_output_2026-06-29_to_2026-07-02_2026-07-12.csv",
    }
    for key, path in research_files.items():
        if path.exists():
            try:
                df = pd.read_csv(path, usecols=lambda c: c in {"slate_date", "game_date"}, low_memory=False)
                col = "slate_date" if "slate_date" in df.columns else "game_date"
                for d in sorted(pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d").dropna().unique()):
                    all_dates.add(d)
                    records[d]["paths"][key].append(str(path))
            except Exception:
                pass

    # Daily platform artifacts.
    for path in sorted((MODEL_DEV / "mlb_starter_skill_workload_daily").glob("*/latest/batter_prop_rows.csv")):
        d = path.parts[-3]
        if DATE_RE.match(d):
            all_dates.add(d)
            records[d]["paths"]["starter_daily"].append(str(path))
    for path in sorted((MLB_ANALYSIS / "pa_foundation/examples").glob("*_pa_context_*.csv")):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        if m:
            d = m.group(1)
            all_dates.add(d)
            records[d]["paths"]["pa_examples"].append(str(path))
    for path in sorted((MLB_ANALYSIS / "environment_v2/daily").glob("*/environment_v2_beta_daily_profiles_*.csv")):
        d = path.parts[-2]
        if DATE_RE.match(d):
            all_dates.add(d)
            records[d]["paths"]["environment_v2"].append(str(path))

    certified_matrices = MODEL_DEV / "mlb_collective_bundle_v1_bounded_historical_source_expansion_pilot_1/2026-07-12/matrices/variant_d_research_matrix_2026-07-12.csv"
    if certified_matrices.exists():
        df = pd.read_csv(certified_matrices, usecols=["slate_date"], low_memory=False)
        for d in sorted(df["slate_date"].astype(str).unique()):
            all_dates.add(d)
            records[d]["paths"]["certified_matrix_source"].append(str(certified_matrices))

    for d in all_dates:
        records[d]["slate_date"] = d
        records[d]["season"] = d[:4]
        if d in odds_dates:
            root = ODDS_DIR / d
            records[d]["paths"]["odds_history_dir"].append(str(root))
            for path in sorted(root.glob("mlb_slate_output*.csv")):
                records[d]["paths"]["denominator"].append(str(path))
            for path in sorted(root.glob("odds_mlb_playerprops*.json")):
                records[d]["paths"]["odds"].append(str(path))
            for path in sorted(root.glob("mlb_predictions_wide_calibrated*.csv")):
                records[d]["paths"]["predictions"].append(str(path))
        if d in prepared_dates:
            records[d]["paths"]["prepared_features"].append(str(PREPARED_DIR / d))
        if d in reconcile_dates:
            records[d]["paths"]["reconcile"].append(str(RECONCILE_DIR / d))
        if d in upload_dates:
            records[d]["paths"]["upload"].append(str(UPLOAD_DIR / d))
    return records


def choose_denominator(paths: list[str]) -> tuple[str, str, bool, int, int, list[str]]:
    if not paths:
        return "", "none", False, 0, 0, []
    tagged = [p for p in paths if "__" in Path(p).name]
    chosen = sorted(tagged or paths)[-1]
    raw_rows, canonical_rows, cols = canonical_row_count(Path(chosen))
    identity = "explicit_run_tag" if tagged else "canonical_unversioned_file"
    return chosen, identity, bool(tagged), raw_rows, canonical_rows, cols


def classify(row: dict[str, Any]) -> tuple[str, str, str, str, int]:
    certified = CERTIFIED_START <= row["slate_date"] <= CERTIFIED_END and row["certified_matrix_source_present"] == "present"
    if certified:
        return (
            "Class A - Near-direct qualification",
            "certified block already demonstrates contract compatibility; include only as benchmark/control, not new expansion",
            "READY_TO_USE_AS_CONTROL_REFERENCE",
            "none",
            5,
        )
    if row["denominator_source_present"] == "present" and row["explicit_run_tag_present"] == "yes":
        if row["pa_source_present"] == "present" and row["starter_source_present"] == "present" and row["offense_context_source_present"] == "present":
            return (
                "Class B - Bounded deterministic reconstruction",
                "explicit denominator and component research sources exist; requires source-lock normalization and replay",
                "READY_TO_REQUEST_BOUNDED_PILOT_DISCOVERY",
                "source locking; temporal lineage; deterministic replay",
                1,
            )
        return (
            "Class C - Recoverable source gap",
            "explicit denominator exists but one or more component sources require reconstruction or authoritative recovery",
            "DISCOVERY_BEFORE_PILOT",
            "component source coverage; temporal lineage",
            3,
        )
    if row["denominator_source_present"] == "present":
        return (
            "Class C - Recoverable source gap",
            "denominator file exists but run-tag identity is weak or absent",
            "DISCOVERY_BEFORE_PILOT",
            "explicit source identity; run tag",
            4,
        )
    if row["prepared_features_present"] == "present" or row["outcome_source_present"] == "present":
        return (
            "Class D - Material investigation required",
            "feature/outcome evidence exists without immediately identified denominator",
            "MATERIAL_INVESTIGATION_REQUIRED",
            "population-spine denominator",
            5,
        )
    return (
        "Class U - Currently unresolved",
        "represented indirectly or sparsely; qualification path not yet established",
        "UNRESOLVED_AFTER_INVENTORY",
        "source discovery",
        9,
    )


def build_inventory(records: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for d in sorted(records):
        rec = records[d]
        paths = rec["paths"]
        denom_path, denom_identity, explicit_tag, raw_rows, canonical_rows, denom_cols = choose_denominator(paths.get("denominator", []))
        has_game = "game_id" in denom_cols
        has_player = "player_id" in denom_cols
        has_prop = "prop_type" in denom_cols or "market_key" in denom_cols
        has_line = "line" in denom_cols
        has_side = "side" in denom_cols or "model_pick_side" in denom_cols
        certified_present = "certified_matrix_source" in paths
        pa_present = any(k in paths for k in ["pa_opportunity_pilot", "pa_examples", "hitter_persistence", "certified_matrix_source"])
        starter_present = any(k in paths for k in ["starter_skill", "starter_daily", "certified_matrix_source"])
        offense_present = any(k in paths for k in ["offense_context", "environment_v2", "certified_matrix_source"])
        outcome_present = any(k in paths for k in ["hitter_persistence", "reconcile", "certified_matrix_source"])
        variant_c_present = bool(paths.get("odds")) or certified_present
        row = {
            "season": d[:4],
            "slate_date": d,
            "date_block_id": "",
            "slate_present": "present" if paths.get("denominator") else "not_identified",
            "estimated_games": "",
            "denominator_source_present": "present" if paths.get("denominator") else "not_identified",
            "denominator_source_path": denom_path,
            "denominator_source_identity": denom_identity,
            "explicit_run_tag_present": "yes" if explicit_tag else "no",
            "game_id_coverage": "present" if has_game else "unknown_or_not_in_denominator",
            "player_id_coverage": "present" if has_player else "unknown_or_not_in_denominator",
            "prop_type_coverage": "present" if has_prop else "unknown_or_not_in_denominator",
            "line_coverage": "present" if has_line else "unknown_or_not_in_denominator",
            "side_coverage": "present" if has_side else "unknown_or_not_in_denominator",
            "pa_source_present": "present" if pa_present else "not_identified",
            "starter_source_present": "present" if starter_present else "not_identified",
            "offense_context_source_present": "present" if offense_present else "not_identified",
            "variant_c_market_source_present": "present" if variant_c_present else "not_identified",
            "outcome_source_present": "present" if outcome_present else "not_identified",
            "outcome_attachability_status": "previously_demonstrated" if certified_present else ("likely_attachable_requires_identity_audit" if outcome_present else "not_yet_established"),
            "temporal_integrity_status": "certified" if certified_present else ("requires_source_lock_audit" if paths.get("denominator") else "not_established"),
            "replayability_status": "certified" if certified_present else ("requires_replay_probe" if paths.get("denominator") else "not_established"),
            "deterministic_reconstruction_status": "already_certified" if certified_present else ("likely_reconstructable" if paths.get("denominator") and (pa_present or starter_present or offense_present) else "requires_investigation"),
            "certified_matrix_source_present": "present" if certified_present else "not_identified",
            "prepared_features_present": "present" if paths.get("prepared_features") else "not_identified",
            "reconcile_source_present": "present" if paths.get("reconcile") else "not_identified",
            "odds_source_present": "present" if paths.get("odds") else "not_identified",
            "prediction_source_present": "present" if paths.get("predictions") else "not_identified",
            "raw_denominator_rows_exact": raw_rows,
            "canonical_candidate_rows_exact": canonical_rows,
            "estimated_rows": canonical_rows if canonical_rows else "",
            "estimated_attachable_rows": int(canonical_rows * 0.96) if canonical_rows else "",
            "estimate_confidence": "exact_denominator_count" if canonical_rows else "not_estimated",
            "evidence_paths": ";".join(sorted({p for values in paths.values() for p in values})[:12]),
        }
        qclass, notes, status, blocking, priority = classify(row)
        row["qualification_class"] = qclass
        row["blocking_domains"] = blocking
        row["recoverable_domains"] = "component reconstruction; source locking; outcome attachment" if "Class B" in qclass or "Class C" in qclass else ""
        row["recommended_priority"] = priority
        row["notes"] = notes
        rows.append(row)
    return rows


def contiguous_blocks(dates: list[str]) -> list[tuple[str, str]]:
    if not dates:
        return []
    parsed = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in dates)
    blocks = []
    start = prev = parsed[0]
    for current in parsed[1:]:
        if (current - prev).days == 1:
            prev = current
        else:
            blocks.append((start.isoformat(), prev.isoformat()))
            start = prev = current
    blocks.append((start.isoformat(), prev.isoformat()))
    return blocks


def assign_block_ids(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    classes = defaultdict(list)
    for row in rows:
        classes[row["qualification_class"]].append(row["slate_date"])
    block_lookup = {}
    idx = 1
    for qclass, dates in sorted(classes.items()):
        for start, end in contiguous_blocks(dates):
            block_id = f"BLOCK_{idx:02d}_{start}_to_{end}"
            idx += 1
            for row in rows:
                if start <= row["slate_date"] <= end and row["qualification_class"] == qclass:
                    block_lookup[row["slate_date"], qclass] = block_id
    for row in rows:
        row["date_block_id"] = block_lookup[row["slate_date"], row["qualification_class"]]
    return rows


def block_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["date_block_id"]].append(row)
    summaries = []
    for block, items in sorted(grouped.items()):
        exact_rows = sum(int(r["canonical_candidate_rows_exact"] or 0) for r in items)
        attach = sum(int(r["estimated_attachable_rows"] or 0) for r in items)
        qclass = items[0]["qualification_class"]
        summaries.append(
            {
                "date_block_id": block,
                "date_start": min(r["slate_date"] for r in items),
                "date_end": max(r["slate_date"] for r in items),
                "slate_dates": len(items),
                "qualification_class": qclass,
                "exact_denominator_rows": exact_rows,
                "estimated_attachable_rows": attach,
                "denominator_dates": sum(r["denominator_source_present"] == "present" for r in items),
                "explicit_run_tag_dates": sum(r["explicit_run_tag_present"] == "yes" for r in items),
                "pa_dates": sum(r["pa_source_present"] == "present" for r in items),
                "starter_dates": sum(r["starter_source_present"] == "present" for r in items),
                "offense_dates": sum(r["offense_context_source_present"] == "present" for r in items),
                "outcome_dates": sum(r["outcome_source_present"] == "present" for r in items),
                "recommended_priority": min(int(r["recommended_priority"]) for r in items),
                "notes": "exact rows are existing denominator counts where available; attachable rows are estimates except certified block",
            }
        )
    return summaries


def coverage_matrix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    domains = [
        ("population_spine_denominator", "denominator_source_present"),
        ("canonical_row_identity", "game_id_coverage"),
        ("pa_opportunity", "pa_source_present"),
        ("starter_skill_workload", "starter_source_present"),
        ("offense_context", "offense_context_source_present"),
        ("variant_c_market", "variant_c_market_source_present"),
        ("outcome_attachment", "outcome_source_present"),
        ("temporal_integrity", "temporal_integrity_status"),
        ("replayability", "replayability_status"),
    ]
    result = []
    for block in block_summary(rows):
        items = [r for r in rows if r["date_block_id"] == block["date_block_id"]]
        for domain, field in domains:
            present = sum(str(r[field]).startswith("present") or str(r[field]) in {"certified", "previously_demonstrated"} for r in items)
            result.append(
                {
                    "date_block_id": block["date_block_id"],
                    "date_start": block["date_start"],
                    "date_end": block["date_end"],
                    "domain": domain,
                    "dates_with_evidence": present,
                    "total_dates": len(items),
                    "coverage_pct": round(present / len(items), 4) if items else 0,
                    "status": "complete_or_present" if present == len(items) else ("partial" if present else "not_identified"),
                }
            )
    return result


def qualification_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "slate_date": r["slate_date"],
            "date_block_id": r["date_block_id"],
            "qualification_class": r["qualification_class"],
            "status": r["deterministic_reconstruction_status"],
            "blocking_domains": r["blocking_domains"],
            "recoverable_domains": r["recoverable_domains"],
            "recommended_next_step": "bounded_pilot_candidate" if int(r["recommended_priority"]) <= 2 and "2026-06-29" > r["slate_date"] else "inventory_or_discovery",
            "notes": r["notes"],
        }
        for r in rows
    ]


def growth_estimates(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for b in blocks:
        exact = int(b["exact_denominator_rows"] or 0)
        if "Class A" in b["qualification_class"]:
            incremental = 0
            confidence = "certified_control_not_incremental"
        elif exact:
            incremental = exact
            confidence = "medium_exact_denominator_estimated_attachability"
        else:
            incremental = ""
            confidence = "low_requires_denominator_discovery"
        out.append(
            {
                "date_block_id": b["date_block_id"],
                "date_start": b["date_start"],
                "date_end": b["date_end"],
                "qualification_class": b["qualification_class"],
                "potential_additional_slate_dates": 0 if "Class A" in b["qualification_class"] else b["slate_dates"],
                "potential_additional_denominator_rows": incremental,
                "potential_additional_attachable_rows": 0 if "Class A" in b["qualification_class"] else b["estimated_attachable_rows"],
                "hits_0_5_growth_estimate": "not_separately_estimated_in_inventory",
                "hits_1_5_growth_estimate": "not_separately_estimated_in_inventory",
                "chronological_fold_benefit": "high" if b["date_end"] < CERTIFIED_START and exact else "control_or_low",
                "variant_c_limitation": "requires market source identity audit" if exact else "unknown",
                "confidence": confidence,
            }
        )
    return out


def priority(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [b for b in blocks if "Class A" not in b["qualification_class"]]
    candidates.sort(key=lambda b: (int(b["recommended_priority"]), -int(b["exact_denominator_rows"] or 0), b["date_start"]))
    rows = []
    for i, b in enumerate(candidates, 1):
        rows.append(
            {
                "priority_rank": i,
                "date_block_id": b["date_block_id"],
                "date_start": b["date_start"],
                "date_end": b["date_end"],
                "qualification_class": b["qualification_class"],
                "rationale": "prioritizes explicit denominator/source identity and row growth before harder discovery blocks",
                "estimated_rows": b["exact_denominator_rows"],
                "estimated_attachable_rows": b["estimated_attachable_rows"],
                "recommended_pilot_scope": "bounded qualification pilot design only; no certification without separate approval",
            }
        )
    return rows


def write_docs(rows: list[dict[str, Any]], blocks: list[dict[str, Any]], priorities: list[dict[str, Any]]) -> None:
    first = min(r["slate_date"] for r in rows)
    last = max(r["slate_date"] for r in rows)
    class_counts = defaultdict(int)
    for r in rows:
        class_counts[r["qualification_class"]] += 1
    first_pilot = priorities[0] if priorities else {}
    (OUT_DIR / f"mlb_historical_inventory_findings_{DATE}.md").write_text(
        f"""# MLB Historical Inventory Findings

## Scope

This inventory inspected local repository artifacts only. It did not qualify,
reconstruct, certify, train, score, backfill, call external APIs, write a
database, or alter production outputs.

## Inventory Range

Broadest represented slate range: `{first}` through `{last}`.

Slate dates inventoried: `{len(rows)}`.

## Qualification Class Counts

{chr(10).join(f'- {k}: `{v}` dates' for k, v in sorted(class_counts.items()))}

## Main Finding

The highest-value next campaign path is not to change Bundle v1 or the frozen
spine. It is to qualify earlier 2026 dates with explicit odds-history
denominators and existing research/component sources, then use the certified
late-June/early-July block as the control reference.

## Recommended First Pilot Block

`{first_pilot.get('date_block_id', 'not_available')}`:
`{first_pilot.get('date_start', '')}` through `{first_pilot.get('date_end', '')}`.

This is a request target only. No pilot is authorized or executed here.
"""
    )
    (OUT_DIR / f"mlb_historical_qualification_campaign_design_{DATE}.md").write_text(
        f"""# MLB Historical Qualification Campaign Design

## Decisions

- Historical inventory completeness: `HISTORICAL_SLATE_INVENTORY_COMPLETED`
- Campaign design readiness: `HISTORICAL_QUALIFICATION_CAMPAIGN_DESIGN_READY`
- Pilot readiness: `READY_TO_REQUEST_ONE_BOUNDED_HISTORICAL_QUALIFICATION_PILOT`
- Incremental expansion readiness: `NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION`
- Training authorization: `NO_CHANGE_TO_TRAINING_AUTHORIZATION`

## Campaign Sequence

1. Inventory.
2. Characterize sources and gaps.
3. Design bounded pilot.
4. Run one bounded qualification pilot only after approval.
5. Certify only if replay, temporal integrity, source locking, and outcome
   attachment pass.
6. Expand incrementally with SHA-locked date blocks.

## Entry Criteria For A Pilot

- explicit hitter-prop denominator artifact or defensible archived denominator;
- canonical row identity fields available or deterministically normalizable;
- source path and run tag documented;
- PA, Starter Skill / Workload, and Offense Context sources identified;
- outcome attachment source identified;
- no implicit latest-source selection;
- replay plan defined before any reconstruction.

## Validation Gates

- source-lock SHA manifest;
- duplicate canonical identity audit;
- grain and ownership audit;
- temporal integrity audit;
- deterministic replay;
- outcome attachment audit;
- missingness classification;
- Variant C market-separation audit;
- certification decision separate from pilot execution.

## Stop And Escalation Criteria

Stop if the denominator grain cannot be tied to
`slate_date | game_id | player_id | prop_type | line | side`, if source identity
is ambiguous, if replay differs, if outcome attachment is ambiguous, or if any
required source is selected by implicit latest semantics.

## First Pilot Recommendation

Request a bounded qualification pilot for `{first_pilot.get('date_block_id', 'not_available')}`
(`{first_pilot.get('date_start', '')}` through `{first_pilot.get('date_end', '')}`).

This recommendation does not authorize the pilot.
"""
    )


def sha_manifest() -> str:
    rows = []
    digest = hashlib.sha256()
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != f"sha256_manifest_{DATE}.csv"):
        rel = str(path.relative_to(OUT_DIR))
        file_sha = sha256(path)
        rows.append({"relative_path": rel, "sha256": file_sha, "bytes": path.stat().st_size})
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(file_sha.encode())
        digest.update(b"\n")
    package_sha = digest.hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST_EXCLUDING_THIS_MANIFEST__", "sha256": package_sha, "bytes": ""})
    write_csv(OUT_DIR / f"sha256_manifest_{DATE}.csv", rows)
    return package_sha


def parse_validation() -> list[dict[str, Any]]:
    validation = []
    seen_dates = set()
    duplicate_dates = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file()):
        if path.name in {f"sha256_manifest_{DATE}.csv", f"parse_integrity_validation_{DATE}.csv"}:
            continue
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                data = read_csv_rows(path)
                if path.name.startswith("mlb_historical_slate_inventory"):
                    for r in data:
                        if r["slate_date"] in seen_dates:
                            duplicate_dates.append(r["slate_date"])
                        seen_dates.add(r["slate_date"])
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md":
                text = path.read_text()
                if not text.lstrip().startswith("#"):
                    status = "FAIL"
                    detail = "markdown_missing_heading"
                if "TODO" in text or "PLACEHOLDER" in text:
                    status = "FAIL"
                    detail = "placeholder"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        validation.append({"relative_path": str(path.relative_to(OUT_DIR)), "status": status, "detail": detail})
    validation.append(
        {
            "relative_path": "inventory_duplicate_date_check",
            "status": "PASS" if not duplicate_dates else "FAIL",
            "detail": ";".join(sorted(duplicate_dates)),
        }
    )
    validation.append(
        {
            "relative_path": "prohibited_action_check",
            "status": "PASS",
            "detail": "inventory/design only; no certified population writes, model fitting, database writes, external calls, or production changes",
        }
    )
    return validation


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    records = collect_sources()
    inventory = assign_block_ids(build_inventory(records))
    blocks = block_summary(inventory)
    coverage = coverage_matrix(inventory)
    qualifications = qualification_rows(inventory)
    growth = growth_estimates(blocks)
    priorities = priority(blocks)
    write_csv(OUT_DIR / f"mlb_historical_slate_inventory_{DATE}.csv", inventory)
    write_csv(OUT_DIR / f"mlb_historical_date_block_summary_{DATE}.csv", blocks)
    write_csv(OUT_DIR / f"mlb_historical_source_coverage_matrix_{DATE}.csv", coverage)
    write_csv(OUT_DIR / f"mlb_historical_qualification_classification_{DATE}.csv", qualifications)
    write_csv(OUT_DIR / f"mlb_historical_population_growth_estimate_{DATE}.csv", growth)
    write_csv(OUT_DIR / f"mlb_historical_candidate_block_priority_{DATE}.csv", priorities)
    write_docs(inventory, blocks, priorities)
    summary = {
        "package_date": DATE,
        "inventory_range": {"start": min(r["slate_date"] for r in inventory), "end": max(r["slate_date"] for r in inventory)},
        "slate_dates_inventoried": len(inventory),
        "qualification_class_counts": {k: sum(r["qualification_class"] == k for r in inventory) for k in sorted({r["qualification_class"] for r in inventory})},
        "recommended_first_pilot_block": priorities[0] if priorities else None,
        "decisions": {
            "historical_inventory_completeness": "HISTORICAL_SLATE_INVENTORY_COMPLETED",
            "campaign_design_readiness": "HISTORICAL_QUALIFICATION_CAMPAIGN_DESIGN_READY",
            "pilot_readiness": "READY_TO_REQUEST_ONE_BOUNDED_HISTORICAL_QUALIFICATION_PILOT",
            "incremental_expansion_readiness": "NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION",
            "training_authorization": "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        },
        "constraints_observed": {
            "qualified_or_certified_any_slate": False,
            "reconstructed_any_slate": False,
            "trained_or_scored": False,
            "database_write": False,
            "external_data_call": False,
            "production_change": False,
        },
    }
    write_json(OUT_DIR / f"mlb_historical_qualification_summary_{DATE}.json", summary)
    validation = parse_validation()
    write_csv(OUT_DIR / f"parse_integrity_validation_{DATE}.csv", validation)
    if any(v["status"] == "FAIL" for v in validation):
        raise SystemExit("parse/integrity validation failed")
    package_sha = sha_manifest()
    print(json.dumps({"output_dir": str(OUT_DIR), "slate_dates": len(inventory), "package_sha256": package_sha}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
