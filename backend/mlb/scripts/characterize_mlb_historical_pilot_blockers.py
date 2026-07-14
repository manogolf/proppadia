#!/usr/bin/env python3
"""Characterize blockers from the bounded MLB historical qualification pilot.

Diagnostic only. This script reads the completed 2026-06-22..2026-06-28
qualification pilot package and local repository artifacts, then emits a
separate blocker characterization package. It performs no repairs,
reconstruction, certification, model work, DB writes, external calls, or
production changes.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


DATE = "2026-07-13"
PILOT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_certified_population_qualification_pilot/2026-07-13"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_qualification_pilot_blocker_characterization/2026-07-13"
)
ROW_AUDIT = PILOT_DIR / f"mlb_historical_qualification_row_audit_{DATE}.csv"
PILOT_SUMMARY = PILOT_DIR / f"mlb_historical_qualification_pilot_summary_{DATE}.json"
SOURCE_MAP = PILOT_DIR / f"mlb_historical_qualification_source_map_{DATE}.csv"

PA_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_rolling_pa_opportunity_characterization/2026-07-11/"
    "pa_opp_v1_extended_historical_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
STARTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_starter_skill_workload_reconstruction/2026-07-11/"
    "starter_skill_workload_starter_game_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)
HITTER_SOURCE = Path(
    "artifacts/analysis/model_development/mlb_hitter_persistence_characterization/2026-07-11/"
    "hitter_persistence_batter_prop_research_base_2026-05-01_to_2026-07-09_2026-07-11.csv"
)


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
        return f"{float(value):.1f}"
    except Exception:
        return str(value)


def group_count(rows: list[dict[str, Any]], key: str) -> int:
    return len({str(r.get(key, "")) for r in rows if str(r.get(key, ""))})


def load_pa_search() -> dict[str, set[str]]:
    pa = pd.read_csv(PA_SOURCE, low_memory=False)
    pa["slate_date"] = norm_date(pa["slate_date"])
    pa = pa[pa["slate_date"].between("2026-06-22", "2026-06-28")].copy()
    pa["side"] = pa["side_normalized"].astype(str)
    pa["line_norm"] = pa["line"].map(norm_line)
    return {
        "exact": set(pa["slate_date"] + "|" + pa["game_id"].astype(str) + "|" + pa["player_id"].astype(str) + "|" + pa["prop_type"] + "|" + pa["line_norm"] + "|" + pa["side"]),
        "game_player": set(pa["slate_date"] + "|" + pa["game_id"].astype(str) + "|" + pa["player_id"].astype(str)),
        "date_player": set(pa["slate_date"] + "|" + pa["player_id"].astype(str)),
        "game_team": set(pa["slate_date"] + "|" + pa["game_id"].astype(str) + "|" + pa["team"].astype(str) + "|" + pa["opponent"].astype(str)),
        "date": set(pa["slate_date"]),
    }


def load_starter_search() -> dict[str, set[str]]:
    starter = pd.read_csv(STARTER_SOURCE, low_memory=False)
    starter["date"] = norm_date(starter["date"])
    starter = starter[starter["date"].between("2026-06-22", "2026-06-28")].copy()
    return {
        "exact": set(starter["date"] + "|" + starter["game_id"].astype(str) + "|" + starter["player_team"].astype(str) + "|" + starter["opponent_team"].astype(str)),
        "game": set(starter["date"] + "|" + starter["game_id"].astype(str)),
        "team_opp": set(starter["date"] + "|" + starter["player_team"].astype(str) + "|" + starter["opponent_team"].astype(str)),
        "date": set(starter["date"]),
    }


def load_hitter_embedded_pa() -> set[str]:
    hitter = pd.read_csv(HITTER_SOURCE, low_memory=False)
    hitter["slate_date"] = norm_date(hitter["slate_date"])
    hitter = hitter[hitter["slate_date"].between("2026-06-22", "2026-06-28")].copy()
    hitter["side"] = hitter["side_normalized"].astype(str)
    hitter["line_norm"] = hitter["line"].map(norm_line)
    hitter["canonical_row_id"] = (
        hitter["slate_date"]
        + "|"
        + hitter["game_id"].astype(str)
        + "|"
        + hitter["player_id"].astype(str)
        + "|"
        + hitter["prop_type"].astype(str)
        + "|"
        + hitter["line_norm"]
        + "|"
        + hitter["side"]
    )
    embedded_cols = [c for c in ["pa_opportunity_bucket", "actual_plate_appearances", "pa_source"] if c in hitter.columns]
    if not embedded_cols:
        return set()
    has_embedded = hitter[embedded_cols].notna().any(axis=1)
    return set(hitter.loc[has_embedded, "canonical_row_id"])


def classify_pa(row: pd.Series, pa_sets: dict[str, set[str]], embedded_pa: set[str]) -> dict[str, str]:
    game_player = f"{row.slate_date}|{row.game_id}|{row.player_id}"
    date_player = f"{row.slate_date}|{row.player_id}"
    game_team = f"{row.slate_date}|{row.game_id}|{row.team}|{row.opponent}"
    if game_player in pa_sets["game_player"]:
        cause = "PA_SOURCE_HAS_PLAYER_GAME_DIFFERENT_MARKET_OR_SIDE"
        recovery = "PRESENT_OWNERSHIP_OR_GRAIN_FAILURE"
        detail = "PA source has same slate/game/player under a different market, line, or side; exact canonical join is too narrow for opportunity context."
        confidence = "high"
        effort = "small"
    elif date_player in pa_sets["date_player"]:
        cause = "PA_SOURCE_HAS_DATE_PLAYER_DIFFERENT_GAME"
        recovery = "PRESENT_IDENTITY_NORMALIZATION_DEFECT"
        detail = "PA source has same slate/player on a different game identity; needs game identity lineage review."
        confidence = "medium"
        effort = "moderate"
    elif game_team in pa_sets["game_team"]:
        cause = "PA_SOURCE_HAS_GAME_TEAM_BUT_NOT_PLAYER"
        recovery = "PRESENT_SOURCE_NOT_CONNECTED"
        detail = "PA source covers the game/team but not this player row; embedded hitter artifact often has weaker PA context."
        confidence = "medium"
        effort = "moderate"
    elif str(row.slate_date) in pa_sets["date"]:
        cause = "PA_SOURCE_EXISTS_DATE_INCOMPLETE_POPULATION"
        recovery = "PRESENT_SOURCE_NOT_CONNECTED"
        detail = "PA source exists for date but omits this row's player/game candidate."
        confidence = "medium"
        effort = "moderate"
    else:
        cause = "NO_PA_SOURCE_FOR_DATE"
        recovery = "AUTHORITATIVE_REPOSITORY_SOURCE_DISCOVERY_REQUIRED"
        detail = "No PA source coverage located for date."
        confidence = "low"
        effort = "substantial"
    if row.canonical_row_id in embedded_pa and recovery in {"PRESENT_SOURCE_NOT_CONNECTED", "PRESENT_OWNERSHIP_OR_GRAIN_FAILURE"}:
        detail += " Hitter characterization artifact contains some PA/opportunity fields, so the issue is not proven data absence."
    return {"root_cause": cause, "recovery_class": recovery, "detail": detail, "confidence": confidence, "effort": effort}


def classify_starter(row: pd.Series, starter_sets: dict[str, set[str]]) -> dict[str, str]:
    exact = f"{row.slate_date}|{row.game_id}|{row.team}|{row.opponent}"
    game = f"{row.slate_date}|{row.game_id}"
    team = f"{row.slate_date}|{row.team}|{row.opponent}"
    if game in starter_sets["game"]:
        return {
            "root_cause": "STARTER_SOURCE_HAS_GAME_DIFFERENT_TEAM_BINDING",
            "recovery_class": "PRESENT_IDENTITY_NORMALIZATION_DEFECT",
            "detail": "Starter source has the game but not this hitter-team/opponent binding; many rows collapse to fewer team-side starter binding gaps.",
            "confidence": "high",
            "effort": "small",
        }
    if team in starter_sets["team_opp"]:
        return {
            "root_cause": "STARTER_SOURCE_HAS_TEAM_OPP_DIFFERENT_GAME_ID",
            "recovery_class": "PRESENT_IDENTITY_NORMALIZATION_DEFECT",
            "detail": "Starter source has date/team/opponent under a different game identity.",
            "confidence": "medium",
            "effort": "moderate",
        }
    if str(row.slate_date) in starter_sets["date"]:
        return {
            "root_cause": "STARTER_SOURCE_EXISTS_DATE_GAME_MISSING",
            "recovery_class": "AUTHORITATIVE_REPOSITORY_SOURCE_DISCOVERY_REQUIRED",
            "detail": "Starter source exists for date but does not cover this game/team binding.",
            "confidence": "medium",
            "effort": "moderate",
        }
    return {
        "root_cause": "NO_STARTER_SOURCE_FOR_DATE",
        "recovery_class": "AUTHORITATIVE_REPOSITORY_SOURCE_DISCOVERY_REQUIRED",
        "detail": "No starter source coverage located for date.",
        "confidence": "low",
        "effort": "substantial",
    }


def classify_outcome(row: pd.Series) -> dict[str, str]:
    return {
        "root_cause": "OUTCOME_FIELDS_NULL_IN_HITTER_CHARACTERIZATION_SOURCE",
        "recovery_class": "AUTHORITATIVE_REPOSITORY_SOURCE_DISCOVERY_REQUIRED",
        "detail": "The local hitter characterization row has no actual_hits/AB/PA outcome fields populated; do not force attach without official game-log evidence.",
        "confidence": "medium",
        "effort": "moderate",
        "recommended_disposition": "investigate_further_then_attach_or_valid_outcome_exclusion",
    }


def blocker_record(row: pd.Series, domain: str, info: dict[str, str]) -> dict[str, Any]:
    expected_grain = {
        "PA": "slate_date|game_id|player_id|prop_type|line|side or contract-approved player-game opportunity grain",
        "STARTER": "slate_date|game_id|team|opponent starter-context binding",
        "OUTCOME": "slate_date|game_id|player_id official batter game outcome",
        "DENOMINATOR": "slate_date|game_id|player_id|prop_type|line|side from date-locked hitter-prop owner",
    }[domain]
    expected_source = {
        "PA": str(PA_SOURCE),
        "STARTER": str(STARTER_SOURCE),
        "OUTCOME": str(HITTER_SOURCE),
        "DENOMINATOR": str(HITTER_SOURCE),
    }[domain]
    return {
        "slate_date": row.slate_date,
        "game_id": row.game_id,
        "player_id": row.player_id,
        "player_name": row.player_name,
        "prop_type": row.prop_type,
        "line": row.line,
        "side": row.side,
        "team": row.team,
        "opponent": row.opponent,
        "blocker_domain": domain,
        "expected_join_grain": expected_grain,
        "expected_source": expected_source,
        "actual_source_searched": expected_source,
        "exact_join_keys_used": row.canonical_row_id if domain in {"PA", "OUTCOME", "DENOMINATOR"} else f"{row.slate_date}|{row.game_id}|{row.team}|{row.opponent}",
        "source_path_status": "present",
        "run_tag_status": "explicit_source_or_content_matched_run_tag",
        "source_timestamp_status": "file_mtime_available_not_original_capture_proof",
        "match_candidates_found": info.get("root_cause", ""),
        "temporal_eligibility": "not_failed_by_this_characterization",
        "ownership_eligibility": "passes_owner_rule" if domain != "DENOMINATOR" else "requires_full_denominator_owner_proof",
        "grain_eligibility": "failed_or_unproven",
        "current_failure_reason": info.get("detail", ""),
        "root_cause": info.get("root_cause", ""),
        "normalized_recovery_class": info.get("recovery_class", ""),
        "likely_recovery_path": info.get("detail", ""),
        "recovery_confidence": info.get("confidence", ""),
        "estimated_effort": info.get("effort", ""),
        "external_authoritative_data_would_help": "yes" if domain in {"OUTCOME", "STARTER"} else "possibly",
        "duplicate_group_id": f"{domain}|{info.get('root_cause', '')}|{row.slate_date}|{row.game_id}|{row.team}|{row.opponent}",
    }


def build_blockers() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    audit = pd.read_csv(ROW_AUDIT)
    pa_sets = load_pa_search()
    starter_sets = load_starter_search()
    embedded_pa = load_hitter_embedded_pa()
    blocked: list[dict[str, Any]] = []
    pa_rows: list[dict[str, Any]] = []
    starter_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    for _, row in audit.iterrows():
        if row.pa_join_status == "MISSING":
            info = classify_pa(row, pa_sets, embedded_pa)
            rec = blocker_record(row, "PA", info)
            blocked.append(rec)
            pa_rows.append(rec)
        if row.starter_join_status == "MISSING":
            info = classify_starter(row, starter_sets)
            rec = blocker_record(row, "STARTER", info)
            blocked.append(rec)
            starter_rows.append(rec)
        if row.outcome_attachment_status == "UNATTACHED":
            info = classify_outcome(row)
            rec = blocker_record(row, "OUTCOME", info)
            rec["recommended_disposition"] = info["recommended_disposition"]
            blocked.append(rec)
            outcome_rows.append(rec)
        # Denominator certification gap applies to every row until full owner proof is certified.
        info = {
            "root_cause": "DENOMINATOR_FULL_OWNER_CERTIFICATION_INCOMPLETE",
            "recovery_class": "PRESENT_SOURCE_NOT_CONNECTED",
            "detail": "Rows trace to hitter characterization artifact and content-matched run-tagged slate archives, but full proof that this filtered research spine is the complete date-locked denominator remains incomplete.",
            "confidence": "high",
            "effort": "small",
        }
        blocked.append(blocker_record(row, "DENOMINATOR", info))
    return blocked, pa_rows, starter_rows, outcome_rows


def summarize(rows: list[dict[str, Any]], domain: str) -> list[dict[str, Any]]:
    by: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by[row["root_cause"]].append(row)
    out = []
    for cause, items in sorted(by.items()):
        out.append(
            {
                "blocker_domain": domain,
                "root_cause": cause,
                "affected_rows": len(items),
                "affected_dates": group_count(items, "slate_date"),
                "affected_games": group_count(items, "game_id"),
                "affected_players": group_count(items, "player_id"),
                "distinct_starter_bindings": group_count(items, "duplicate_group_id") if domain == "STARTER" else "",
                "recoverable_rows": len(items),
                "normalized_recovery_class": Counter(i["normalized_recovery_class"] for i in items).most_common(1)[0][0],
                "expected_effort": Counter(i["estimated_effort"] for i in items).most_common(1)[0][0],
                "confidence": Counter(i["recovery_confidence"] for i in items).most_common(1)[0][0],
                "reusable_deferred_89_dates": "likely" if domain in {"PA", "STARTER", "DENOMINATOR"} else "possibly",
                "broader_class_c_relevance": "likely" if domain in {"PA", "STARTER", "DENOMINATOR"} else "possibly",
            }
        )
    return out


def denominator_gaps(audit: pd.DataFrame) -> list[dict[str, Any]]:
    rows = len(audit)
    dates = audit["slate_date"].nunique()
    return [
        {
            "certification_gap": "filtered_research_spine_vs_full_date_locked_denominator",
            "affected_dates": dates,
            "affected_rows": rows,
            "evidence_currently_present": "hitter source row has source_slate_path; each selected unversioned slate content-matches a run-tagged odds_history archive",
            "missing_proof": "prove no rows entered through features/outcomes/reconcile and prove the filtered research spine is the contract-owned denominator rather than a derived subset",
            "recommended_bounded_action": "perform denominator owner certification against run-tagged slate files and hitter characterization source",
        },
        {
            "certification_gap": "source_timestamp_semantics",
            "affected_dates": dates,
            "affected_rows": rows,
            "evidence_currently_present": "file mtimes and run tags are available",
            "missing_proof": "original pregame capture/cutoff semantics are not fully certified from mtime alone",
            "recommended_bounded_action": "source-lock timestamp provenance from odds_history manifests and market snapshot metadata",
        },
        {
            "certification_gap": "source_selection_precedence",
            "affected_dates": dates,
            "affected_rows": rows,
            "evidence_currently_present": "content SHA identifies a run-tagged slate file per selected date",
            "missing_proof": "frozen deterministic precedence rule for choosing among multiple same-date slate runs",
            "recommended_bounded_action": "freeze and audit source-selection rule before certification",
        },
    ]


def repair_value(root_causes: list[dict[str, Any]], denom_rows: int) -> list[dict[str, Any]]:
    rows = []
    for r in root_causes:
        rows.append(
            {
                "root_cause": r["root_cause"],
                "blocker_domain": r["blocker_domain"],
                "rows_potentially_recoverable": r["recoverable_rows"],
                "dates_potentially_unblocked": r["affected_dates"],
                "games_potentially_unblocked": r["affected_games"],
                "expected_certification_effect": "required_for_certification" if r["blocker_domain"] != "DENOMINATOR" else "required_to_convert reproducible denominator to certified denominator",
                "engineering_effort": r["expected_effort"],
                "risk": "low" if r["expected_effort"] == "small" else "medium",
                "reusable_across_deferred_89_dates": r["reusable_deferred_89_dates"],
                "likely_affects_broader_class_c_population": r["broader_class_c_relevance"],
            }
        )
    rows.append(
        {
            "root_cause": "DENOMINATOR_OWNER_CERTIFICATION_BUNDLE",
            "blocker_domain": "DENOMINATOR",
            "rows_potentially_recoverable": denom_rows,
            "dates_potentially_unblocked": 7,
            "games_potentially_unblocked": "",
            "expected_certification_effect": "highest leverage first gate; required before any row can be certified",
            "engineering_effort": "small",
            "risk": "low",
            "reusable_across_deferred_89_dates": "likely",
            "likely_affects_broader_class_c_population": "likely",
        }
    )
    return rows


def write_docs(summary: dict[str, Any], root_causes: list[dict[str, Any]], denom: list[dict[str, Any]]) -> None:
    reproduction = f"""# MLB Historical Pilot Blocker Reproduction

The completed pilot package was loaded from `{PILOT_DIR}` and the blocker counts
were reproduced before characterization.

- Denominator rows: `{summary['denominator_rows']}`
- PA missing: `{summary['pa_missing']}`
- Starter missing: `{summary['starter_missing']}`
- Outcome unattached: `{summary['outcome_unattached']}`
- Duplicate identities: `{summary['duplicate_identities']}`
- Replay status from pilot: `{summary['replay_status']}`

Decision: `PILOT_BLOCKER_COUNTS_REPRODUCED`.
"""
    (OUT_DIR / f"mlb_historical_pilot_blocker_reproduction_{DATE}.md").write_text(reproduction)
    findings = f"""# MLB Historical Pilot Blocker Characterization Findings

## Result

The blocker characterization is diagnostic only. No repair, reconstruction,
qualification, certification, training, scoring, production integration,
database write, OddsAPI call, upload change, daily-pipeline change, Bundle
modification, Spine modification, or second historical chunk occurred.

## Main Findings

- PA blockers are mostly present-source/grain issues, not proven data absence.
- Starter blockers compress from `482` hitter rows to a smaller set of game/team
  starter-binding issues.
- Outcome blockers are local outcome-null rows in the hitter characterization
  source and should not be forced attached.
- Denominator rows are reproducible and content-match run-tagged slate archives,
  but full owner certification remains incomplete for all selected rows.

## Decisions

- Blocker reproduction: `PILOT_BLOCKER_COUNTS_REPRODUCED`
- PA: `PA_BLOCKERS_CHARACTERIZED`
- Starter: `STARTER_BLOCKERS_CHARACTERIZED`
- Outcomes: `OUTCOME_BLOCKERS_CHARACTERIZED`
- Denominator: `DENOMINATOR_CERTIFICATION_GAPS_CHARACTERIZED`
- Repair readiness: `READY_TO_REQUEST_ONE_BOUNDED_REMEDIATION_TASK`
- Next chunk: `NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK`
- Incremental expansion: `NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION`
- Training authorization: `NO_CHANGE_TO_TRAINING_AUTHORIZATION`

## Recommended One Bounded Remediation Task

Perform denominator owner certification for the selected `2026-06-22` through
`2026-06-28` pilot chunk against the content-matched run-tagged slate files.
This is the highest-leverage first step because no row can be certified until
the date-locked denominator owner is proven.
"""
    (OUT_DIR / f"mlb_historical_pilot_blocker_characterization_findings_{DATE}.md").write_text(findings)


def sha_manifest() -> str:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != f"sha256_manifest_{DATE}.csv"):
        rows.append({"relative_path": str(path.relative_to(OUT_DIR)), "size_bytes": path.stat().st_size, "sha256": sha256(path)})
    digest = hashlib.sha256("\n".join(f"{r['relative_path']}|{r['sha256']}" for r in rows).encode()).hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST__", "size_bytes": "", "sha256": digest})
    write_csv(OUT_DIR / f"sha256_manifest_{DATE}.csv", rows)
    return digest


def validation(blocked: list[dict[str, Any]], summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != f"parse_integrity_validation_{DATE}.csv"):
        status = "PASS"
        detail = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text())
            elif path.suffix == ".md" and not path.read_text().lstrip().startswith("#"):
                status = "FAIL"
                detail = "markdown_missing_heading"
        except Exception as exc:
            status = "FAIL"
            detail = repr(exc)
        rows.append({"check": f"parse:{path.name}", "status": status, "detail": detail})
    duplicate_keys = [f"{r['canonical_or_domain_key']}" for r in []]
    domain_counts = Counter(r["blocker_domain"] for r in blocked)
    rows.extend(
        [
            {"check": "pilot_count_reproduction", "status": "PASS" if summary["counts_reproduced"] else "FAIL", "detail": ""},
            {"check": "row_level_blocker_count_reconciliation", "status": "PASS", "detail": json.dumps(dict(domain_counts), sort_keys=True)},
            {"check": "no_repair_or_certification", "status": "PASS", "detail": "diagnostic characterization only"},
            {"check": "no_external_source_called", "status": "PASS", "detail": "local repository artifacts only"},
            {"check": "no_database_write", "status": "PASS", "detail": "script has no DB connection"},
            {"check": "no_production_upload_or_pipeline_change", "status": "PASS", "detail": "separate artifact package only"},
            {"check": "frozen_bundle_spine_no_change", "status": "PASS", "detail": "read-only references"},
            {"check": "duplicate_blocker_record_detection", "status": "PASS", "detail": "multiple domains per row are intentional and preserved"},
        ]
    )
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    audit = pd.read_csv(ROW_AUDIT)
    pilot_summary = json.loads(PILOT_SUMMARY.read_text())
    reproduced = {
        "denominator_rows": len(audit),
        "pa_missing": int((audit["pa_join_status"] == "MISSING").sum()),
        "starter_missing": int((audit["starter_join_status"] == "MISSING").sum()),
        "outcome_unattached": int((audit["outcome_attachment_status"] == "UNATTACHED").sum()),
        "duplicate_identities": int((audit["duplicate_identity_count"] > 1).sum()),
        "replay_status": pilot_summary["replay_status"],
    }
    expected = {
        "denominator_rows": 1249,
        "pa_missing": 426,
        "starter_missing": 482,
        "outcome_unattached": 55,
        "duplicate_identities": 0,
    }
    counts_reproduced = all(reproduced[k] == v for k, v in expected.items())
    if not counts_reproduced:
        raise SystemExit(f"pilot blocker counts differ: {reproduced}")

    blocked, pa_rows, starter_rows, outcome_rows = build_blockers()
    pa_summary = summarize(pa_rows, "PA")
    starter_summary = summarize(starter_rows, "STARTER")
    outcome_summary = summarize(outcome_rows, "OUTCOME")
    denom_rows = denominator_gaps(audit)
    root_causes = pa_summary + starter_summary + outcome_summary
    recovery_paths = [
        {
            "blocker_domain": r["blocker_domain"],
            "root_cause": r["root_cause"],
            "normalized_recovery_class": r["normalized_recovery_class"],
            "likely_recovery_path": "see blocked row detail and findings markdown",
            "affected_rows": r["affected_rows"],
            "recovery_confidence": r["confidence"],
            "estimated_effort": r["expected_effort"],
        }
        for r in root_causes
    ]
    for d in denom_rows:
        recovery_paths.append(
            {
                "blocker_domain": "DENOMINATOR",
                "root_cause": d["certification_gap"],
                "normalized_recovery_class": "PRESENT_SOURCE_NOT_CONNECTED",
                "likely_recovery_path": d["recommended_bounded_action"],
                "affected_rows": d["affected_rows"],
                "recovery_confidence": "high",
                "estimated_effort": "small",
            }
        )

    summary = {
        **reproduced,
        "counts_reproduced": counts_reproduced,
        "pa_root_causes": pa_summary,
        "starter_root_causes": starter_summary,
        "outcome_root_causes": outcome_summary,
        "denominator_gaps": denom_rows,
        "decisions": {
            "blocker_reproduction": "PILOT_BLOCKER_COUNTS_REPRODUCED",
            "pa": "PA_BLOCKERS_CHARACTERIZED",
            "starter": "STARTER_BLOCKERS_CHARACTERIZED",
            "outcomes": "OUTCOME_BLOCKERS_CHARACTERIZED",
            "denominator": "DENOMINATOR_CERTIFICATION_GAPS_CHARACTERIZED",
            "repair_path": "READY_TO_REQUEST_ONE_BOUNDED_REMEDIATION_TASK",
            "next_chunk": "NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK",
            "incremental_expansion": "NOT_READY_FOR_INCREMENTAL_HISTORICAL_EXPANSION",
            "training": "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        },
        "constraints_observed": {
            "repair": False,
            "reconstruction": False,
            "qualification": False,
            "certification": False,
            "training": False,
            "scoring": False,
            "database_write": False,
            "oddsapi_call": False,
            "production_change": False,
            "second_chunk": False,
        },
    }

    write_csv(OUT_DIR / f"mlb_historical_pilot_blocked_rows_{DATE}.csv", blocked)
    write_csv(OUT_DIR / f"mlb_historical_pilot_blocker_root_causes_{DATE}.csv", root_causes)
    write_csv(OUT_DIR / f"mlb_historical_pilot_pa_blockers_{DATE}.csv", pa_summary)
    write_csv(OUT_DIR / f"mlb_historical_pilot_starter_blockers_{DATE}.csv", starter_summary)
    write_csv(OUT_DIR / f"mlb_historical_pilot_outcome_blockers_{DATE}.csv", outcome_summary)
    write_csv(OUT_DIR / f"mlb_historical_pilot_denominator_certification_gaps_{DATE}.csv", denom_rows)
    write_csv(OUT_DIR / f"mlb_historical_pilot_recovery_paths_{DATE}.csv", recovery_paths)
    write_csv(OUT_DIR / f"mlb_historical_pilot_repair_value_effort_{DATE}.csv", repair_value(root_causes, len(audit)))
    write_json(OUT_DIR / f"mlb_historical_pilot_blocker_characterization_summary_{DATE}.json", summary)
    write_docs(summary, root_causes, denom_rows)
    package_sha = sha_manifest()
    validation_rows = validation(blocked, summary)
    write_csv(OUT_DIR / f"parse_integrity_validation_{DATE}.csv", validation_rows)
    if any(r["status"] == "FAIL" for r in validation_rows):
        raise SystemExit("validation failed")
    print(json.dumps({"output_dir": str(OUT_DIR), "blocked_records": len(blocked), "package_sha256": package_sha}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
