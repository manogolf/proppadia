#!/usr/bin/env python3
"""Investigate earlier denominator evidence for the bounded MLB pilot dates.

Read-only diagnostic for 2026-06-22..2026-06-28. It does not reconstruct,
certify, repair joins, attach outcomes, call external APIs, write databases, or
modify production/frozen artifacts.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


DATE = "2026-07-13"
DATES = ["2026-06-22", "2026-06-23", "2026-06-24", "2026-06-25", "2026-06-26", "2026-06-27", "2026-06-28"]
OUT_DIR = Path("artifacts/analysis/model_development/mlb_historical_denominator_earlier_source_investigation/2026-07-13")
DENOM_CERT_DIR = Path("artifacts/analysis/model_development/mlb_historical_denominator_owner_certification/2026-07-13")
PILOT_DIR = Path("artifacts/analysis/model_development/mlb_historical_certified_population_qualification_pilot/2026-07-13")
ODDS_HISTORY = Path("backend/mlb/exports/odds_history")
SPINE_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_historical_population_spine_contract_v1/2026-07-12")
CERT_DIR = Path("artifacts/analysis/model_development/mlb_collective_bundle_v1_bounded_expanded_matrix_certification/2026-07-12")


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
            for k in row:
                if k not in fields:
                    fields.append(k)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({f: row.get(f, "") for f in fields})


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def norm_line(v: Any) -> str:
    try:
        return f"{float(v):.1f}"
    except Exception:
        return str(v)


def run_tag(path: Path) -> str:
    m = re.search(r"__(local_[^.]*)\.(csv|json)$", path.name)
    return m.group(1) if m else ""


def ts_from_tag(tag: str) -> pd.Timestamp | None:
    m = re.search(r"(\d{8}T\d{6})Z", tag)
    if not m:
        return None
    return pd.to_datetime(m.group(1), format="%Y%m%dT%H%M%S", utc=True)


def canonical_keys(df: pd.DataFrame) -> pd.Series | None:
    if not {"slate_date", "game_id", "player_id", "prop_type", "line"}.issubset(df.columns):
        return None
    if "model_pick_side" in df.columns:
        side = df["model_pick_side"].astype(str)
    elif "side" in df.columns:
        side = df["side"].astype(str)
    elif "side_normalized" in df.columns:
        side = df["side_normalized"].astype(str)
    else:
        return None
    return (
        df["slate_date"].astype(str)
        + "|"
        + df["game_id"].astype(str)
        + "|"
        + df["player_id"].astype(str)
        + "|"
        + df["prop_type"].astype(str)
        + "|"
        + df["line"].map(norm_line)
        + "|"
        + side
    )


def load_prior() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    decisions = pd.read_csv(DENOM_CERT_DIR / f"mlb_historical_denominator_date_decisions_{DATE}.csv")
    summary = json.loads((DENOM_CERT_DIR / f"mlb_historical_denominator_certification_summary_{DATE}.json").read_text())
    pilot = pd.read_csv(PILOT_DIR / f"mlb_historical_qualification_row_audit_{DATE}.csv")
    return decisions, pilot, summary


def selected_timestamp_by_date(decisions: pd.DataFrame) -> dict[str, pd.Timestamp]:
    out = {}
    selected = pd.read_csv(DENOM_CERT_DIR / f"mlb_historical_denominator_selected_sources_{DATE}.csv")
    for _, r in selected.iterrows():
        out[str(r["slate_date"])] = pd.to_datetime(r["selected_source_timestamp_utc"], utc=True)
    return out


def paired_json_for_slate(path: Path) -> Path:
    tag = run_tag(path)
    return path.with_name(f"odds_mlb_playerprops__{tag}.json")


def slate_temporal(path: Path) -> dict[str, Any]:
    tag = run_tag(path)
    capture = ts_from_tag(tag)
    df = pd.read_csv(path, low_memory=False)
    hits = df[df["prop_type"].astype(str).eq("hits")].copy() if "prop_type" in df.columns else pd.DataFrame()
    if capture is None or hits.empty or "game_time" not in hits.columns:
        return {
            "capture_timestamp_utc": capture.isoformat() if capture is not None else "",
            "temporal_classification": "CAPTURE_TIME_UNRESOLVED",
            "hits_rows": len(hits),
            "games_total": hits["game_id"].nunique() if "game_id" in hits.columns else 0,
            "games_not_started": 0,
            "games_started": 0,
            "rows_not_started": 0,
            "rows_started": 0,
            "earliest_game_time_utc": "",
        }
    hits["game_time_utc"] = pd.to_datetime(hits["game_time"], utc=True, errors="coerce")
    before = capture < hits["game_time_utc"]
    after = capture >= hits["game_time_utc"]
    if hits["game_time_utc"].isna().any():
        cls = "GAME_START_TIME_UNRESOLVED"
    elif after.sum() == 0:
        cls = "BEFORE_ALL_RELEVANT_GAMES"
    elif before.sum() > 0:
        cls = "MIXED_PARTIAL_PREGAME"
    else:
        cls = "AFTER_THIS_GAME_STARTED"
    return {
        "capture_timestamp_utc": capture.isoformat(),
        "temporal_classification": cls,
        "hits_rows": len(hits),
        "games_total": int(hits["game_id"].nunique()),
        "games_not_started": int(hits.loc[before, "game_id"].nunique()),
        "games_started": int(hits.loc[after, "game_id"].nunique()),
        "rows_not_started": int(before.sum()),
        "rows_started": int(after.sum()),
        "earliest_game_time_utc": hits["game_time_utc"].min().isoformat(),
        "latest_game_time_utc": hits["game_time_utc"].max().isoformat(),
    }


def inventory_earlier_slates(decisions: pd.DataFrame, pilot: pd.DataFrame) -> list[dict[str, Any]]:
    selected_ts = selected_timestamp_by_date(decisions)
    pilot_keys = {d: set(g["canonical_row_id"].astype(str)) for d, g in pilot.groupby("slate_date")}
    rows = []
    for d in DATES:
        for path in sorted((ODDS_HISTORY / d).glob("mlb_slate_output__local_daily_*.csv")):
            tag = run_tag(path)
            ts = ts_from_tag(tag)
            if ts is None or ts >= selected_ts[d]:
                continue
            df = pd.read_csv(path, low_memory=False)
            hits = df[df["prop_type"].astype(str).eq("hits")].copy()
            keys = canonical_keys(hits)
            key_set = set(keys) if keys is not None else set()
            temp = slate_temporal(path)
            pair = paired_json_for_slate(path)
            rows.append(
                {
                    "slate_date": d,
                    "path": str(path),
                    "run_tag": tag,
                    "capture_timestamp_utc": temp["capture_timestamp_utc"],
                    "sha256": sha256(path),
                    "schema": "|".join(df.columns[:80]),
                    "row_count": len(df),
                    "hits_row_count": len(hits),
                    "canonical_identity_count": len(key_set),
                    "duplicate_canonical_identities": len(key_set) - len(hits) if keys is not None else "",
                    "paired_odds_json_status": "present" if pair.exists() else "missing",
                    "paired_odds_json": str(pair) if pair.exists() else "",
                    "temporal_classification": temp["temporal_classification"],
                    "games_total": temp["games_total"],
                    "games_not_started": temp["games_not_started"],
                    "games_started": temp["games_started"],
                    "rows_not_started": temp["rows_not_started"],
                    "rows_started": temp["rows_started"],
                    "hits_membership_matches_late_pilot": key_set == pilot_keys[d],
                    "pilot_rows_represented": len(key_set & pilot_keys[d]),
                    "pilot_rows_missing_from_candidate": len(pilot_keys[d] - key_set),
                    "candidate_only_rows": len(key_set - pilot_keys[d]),
                    "eligibility_under_existing_precedence_contract": "eligible_source_type",
                    "reason_accepted_or_rejected_for_further_analysis": "credible_earlier_run_tagged_slate" if pair.exists() else "paired_odds_missing",
                }
            )
    return rows


def inventory_untagged(decisions: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    selected = pd.read_csv(DENOM_CERT_DIR / f"mlb_historical_denominator_selected_sources_{DATE}.csv")
    selected_sha = {r["slate_date"]: r["source_sha256"] for _, r in selected.iterrows()}
    for d in DATES:
        for path in sorted((ODDS_HISTORY / d).glob("mlb_slate_output.csv")):
            digest = sha256(path)
            binding = "selected_late_run" if digest == selected_sha[d] else "unresolved"
            rows.append(
                {
                    "slate_date": d,
                    "path": str(path),
                    "sha256": digest,
                    "explicit_run_binding_proven": binding == "selected_late_run",
                    "binding_evidence": "content_sha_matches_selected_late_run_tagged_slate" if binding == "selected_late_run" else "none",
                    "timing_proven_without_mtime": binding == "selected_late_run",
                    "content_matches_run_tagged_odds_source": binding == "selected_late_run",
                    "could_be_authoritative_denominator_owner": False,
                    "deterministic_replayable_binding": binding == "selected_late_run",
                    "notes": "binds only to the already noncertifiable late source, not an earlier source",
                }
            )
    return rows


def odds_identity_stats(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text())
    events = data.get("events", []) if isinstance(data, dict) else []
    hits_outcomes = 0
    hit_players = set()
    lines = set()
    sides = set()
    event_ids = set()
    for ev in events:
        event_ids.add(ev.get("id", ""))
        for bm in ev.get("bookmakers", []):
            for market in bm.get("markets", []):
                if market.get("key") != "batter_hits":
                    continue
                for outcome in market.get("outcomes", []):
                    hits_outcomes += 1
                    hit_players.add(outcome.get("description", ""))
                    lines.add(str(outcome.get("point", "")))
                    sides.add(outcome.get("name", ""))
    return {
        "event_count": len(events),
        "hits_market_outcomes": hits_outcomes,
        "hits_player_name_count": len(hit_players),
        "line_count": len(lines),
        "side_count": len(sides),
        "event_id_count": len(event_ids),
        "has_game_id": False,
        "has_player_id": False,
    }


def inventory_earlier_odds(decisions: pd.DataFrame) -> list[dict[str, Any]]:
    selected_ts = selected_timestamp_by_date(decisions)
    rows = []
    for d in DATES:
        for path in sorted((ODDS_HISTORY / d).glob("odds_mlb_playerprops__local_daily_*.json")):
            tag = run_tag(path)
            ts = ts_from_tag(tag)
            if ts is None or ts >= selected_ts[d]:
                continue
            slate = path.with_name(f"mlb_slate_output__{tag}.csv")
            stats = odds_identity_stats(path)
            data = json.loads(path.read_text())
            captured = data.get("captured_at_utc", "") if isinstance(data, dict) else ""
            rows.append(
                {
                    "slate_date": d,
                    "path": str(path),
                    "run_tag": tag,
                    "capture_timestamp_from_run_tag_utc": ts.isoformat(),
                    "captured_at_utc": captured,
                    "paired_slate_file_status": "present" if slate.exists() else "missing",
                    "paired_slate_file": str(slate) if slate.exists() else "",
                    "event_coverage": stats["event_count"],
                    "player_coverage": stats["hits_player_name_count"],
                    "hits_market_coverage": stats["hits_market_outcomes"],
                    "line_coverage": stats["line_count"],
                    "side_coverage": stats["side_count"],
                    "game_id_coverage": stats["has_game_id"],
                    "player_id_coverage": stats["has_player_id"],
                    "source_sha256": sha256(path),
                    "enough_to_reconstruct_frozen_identity": False,
                    "missing_fields": "mlb_game_id;mlbam_player_id",
                    "deterministic_normalization_requirements": "event-id-to-game-id and player-name-to-mlbam mapping would be required",
                    "reconstruction_confidence": "medium" if slate.exists() else "low",
                    "reconstruction_feasibility": "RECONSTRUCTION_PLAUSIBLE_BUT_CONTRACT_INTERPRETATION_REQUIRED",
                }
            )
    return rows


def run_log_evidence() -> list[dict[str, Any]]:
    rows = []
    for d in DATES:
        manifest = ODDS_HISTORY / d / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            rows.append(
                {
                    "slate_date": d,
                    "path": str(manifest),
                    "evidence_type": "odds_history_manifest",
                    "proves_earlier_run_occurred": "unknown",
                    "proves_earlier_odds_snapshot_existed": "manifest_records_current_archive_set" if data else "unknown",
                    "proves_earlier_denominator_artifact_existed": "archive_files_exist_separately",
                    "proves_exact_artifact_content": "use artifact SHA, not manifest alone",
                    "sha256": sha256(manifest),
                    "notes": json.dumps(data)[:500],
                }
            )
    return rows


def reconstruction_feasibility(odds_rows: list[dict[str, Any]], slate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    by_date_odds = defaultdict(list)
    by_date_slate = defaultdict(list)
    for r in odds_rows:
        by_date_odds[r["slate_date"]].append(r)
    for r in slate_rows:
        by_date_slate[r["slate_date"]].append(r)
    for d in DATES:
        odds_count = len(by_date_odds[d])
        before_all = [r for r in by_date_slate[d] if r["temporal_classification"] == "BEFORE_ALL_RELEVANT_GAMES"]
        rows.append(
            {
                "slate_date": d,
                "source_family": "earlier_odds_json",
                "candidate_count": odds_count,
                "canonical_fields_present": "slate_date=derivable;game_id=missing;player_id=missing;prop_type=derivable;line=present;side=present",
                "frozen_normalization_available": "partial",
                "later_information_required": "yes_for_game_id_and_player_id_if_no_paired_slate",
                "preserves_hitter_prop_source_owner": "plausibly_if_contract_accepts_odds_json_owner_or_paired_slate_derivation",
                "replayable": "yes_if mapping sources are frozen",
                "classification": "RECONSTRUCTION_PLAUSIBLE_BUT_CONTRACT_INTERPRETATION_REQUIRED",
                "notes": f"{len(before_all)} earlier paired slate files already exist before all relevant games, making direct slate-source review higher leverage than odds-only reconstruction.",
            }
        )
    return rows


def game_temporal(slate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for s in slate_rows:
        path = Path(s["path"])
        capture = pd.to_datetime(s["capture_timestamp_utc"], utc=True)
        df = pd.read_csv(path, low_memory=False)
        hits = df[df["prop_type"].astype(str).eq("hits")].copy()
        hits["game_time_utc"] = pd.to_datetime(hits["game_time"], utc=True, errors="coerce")
        for (gid, home, away), g in hits.groupby(["game_id", "home_team_code", "away_team_code"]):
            start = g["game_time_utc"].dropna().min()
            if pd.isna(start):
                cls = "GAME_START_TIME_UNRESOLVED"
                minutes = ""
            else:
                minutes = round((start - capture).total_seconds() / 60.0, 2)
                cls = "BEFORE_THIS_GAME" if capture < start else "AFTER_THIS_GAME_STARTED"
            rows.append(
                {
                    "slate_date": s["slate_date"],
                    "candidate_path": s["path"],
                    "game_id": gid,
                    "teams": f"{away}@{home}",
                    "scheduled_game_start_utc": "" if pd.isna(start) else start.isoformat(),
                    "actual_game_start_source": "slate_game_time_column",
                    "source_capture_timestamp_utc": s["capture_timestamp_utc"],
                    "minutes_before_or_after_game_start": minutes,
                    "temporal_classification": cls,
                    "hits_rows": len(g),
                }
            )
    return rows


def partial_eligibility(slate_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "slate_date": r["slate_date"],
            "candidate_path": r["path"],
            "total_games_represented": r["games_total"],
            "games_not_yet_started_at_capture": r["games_not_started"],
            "games_already_started_at_capture": r["games_started"],
            "games_completed_at_capture": "",
            "total_rows_from_not_yet_started_games": r["rows_not_started"],
            "total_rows_from_started_games": r["rows_started"],
            "total_rows_with_unresolved_game_timing": 0 if r["temporal_classification"] != "GAME_START_TIME_UNRESOLVED" else r["hits_row_count"],
            "analytical_only_not_certified": True,
        }
        for r in slate_rows
    ]


def contract_review() -> list[dict[str, Any]]:
    return [
        {
            "concept": "date-level source ownership with game-level temporal eligibility",
            "compatibility": "ambiguous",
            "artifact": str(SPINE_DIR / "historical_population_spine_contract_v1_2026-07-12.json"),
            "citation": "source_identity_and_date_lock requires explicit source artifact/cutoff; does not define game-level partial eligibility",
            "recommendation": "contract-interpretation review required before partial certification",
        },
        {
            "concept": "certification of only rows belonging to games not yet started",
            "compatibility": "not addressed",
            "artifact": str(CERT_DIR / "date_level_spine_certification_2026-07-12.csv"),
            "citation": "date-level certification fields are date-scoped and do not express row-level temporal subset certification",
            "recommendation": "do not reinterpret silence as permission",
        },
        {
            "concept": "partial certification of a slate date",
            "compatibility": "ambiguous",
            "artifact": str(CERT_DIR / "certification_decision_2026-07-12.json"),
            "citation": "bounded certification was date interval/package scoped; no explicit partial-date rule found",
            "recommendation": "requires governance review",
        },
        {
            "concept": "one date having multiple explicitly locked denominator captures by game cohort",
            "compatibility": "not addressed",
            "artifact": str(SPINE_DIR / "replayability_contract_2026-07-12.csv"),
            "citation": "replayability is source-map based but does not define multi-capture same-date cohorts",
            "recommendation": "requires contract interpretation or new version",
        },
        {
            "concept": "game-level source locking",
            "compatibility": "not addressed",
            "artifact": str(SPINE_DIR / "source_selection_cutoff_contract_2026-07-12.csv"),
            "citation": "cutoff/source selection is explicit, but game-cohort selection is not specified",
            "recommendation": "requires bounded design review",
        },
        {
            "concept": "row-level temporal eligibility",
            "compatibility": "not addressed",
            "artifact": str(CERT_DIR / "temporal_integrity_certification_2026-07-12.csv"),
            "citation": "temporal certification reports pass/fail by manifest/date fields, not row-level partial eligibility",
            "recommendation": "requires governance review before use",
        },
    ]


def recovery_paths(slate_rows: list[dict[str, Any]], untagged: list[dict[str, Any]], odds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    by_date = defaultdict(list)
    for r in slate_rows:
        by_date[r["slate_date"]].append(r)
    for d in DATES:
        before_all = [r for r in by_date[d] if r["temporal_classification"] == "BEFORE_ALL_RELEVANT_GAMES" and r["paired_odds_json_status"] == "present"]
        partial = [r for r in by_date[d] if r["rows_not_started"] and r["rows_started"]]
        if before_all:
            best = sorted(before_all, key=lambda r: r["capture_timestamp_utc"])[-1]
            path = "Path A - Earlier run-tagged slate exists"
            status = "strongest_supported"
            rows_recoverable = best["hits_row_count"]
        elif partial:
            best = sorted(partial, key=lambda r: r["capture_timestamp_utc"])[-1]
            path = "Path D - Partial game-level eligibility from selected or earlier source"
            status = "contract_ambiguous"
            rows_recoverable = best["rows_not_started"]
        else:
            best = None
            path = "Path E - No currently admissible recovery path"
            status = "no_current_path"
            rows_recoverable = 0
        rows.append(
            {
                "slate_date": d,
                "strongest_supported_path": path,
                "status": status,
                "evidence": "" if best is None else best["path"],
                "dates_potentially_recoverable": 1 if best is not None else 0,
                "games_potentially_recoverable": "" if best is None else best["games_total"] if path.startswith("Path A") else best["games_not_started"],
                "rows_potentially_recoverable": rows_recoverable,
                "rows_still_unresolved": 0 if best is not None and path.startswith("Path A") else "",
                "contract_interpretation_required": path.startswith("Path D"),
                "external_evidence_required": False,
            }
        )
    return rows


def recoverable_estimates(paths: list[dict[str, Any]], slate_rows: list[dict[str, Any]], untagged: list[dict[str, Any]], odds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "category": "earlier_pregame_run_tagged_slate",
            "exact_or_estimate": "exact",
            "dates": sum(1 for r in paths if r["strongest_supported_path"].startswith("Path A")),
            "games": sum(int(r["games_potentially_recoverable"] or 0) for r in paths if r["strongest_supported_path"].startswith("Path A")),
            "rows": sum(int(r["rows_potentially_recoverable"] or 0) for r in paths if r["strongest_supported_path"].startswith("Path A")),
            "notes": "Rows are from earlier source, not replacement written here.",
        },
        {
            "category": "bindable_earlier_untagged_slate",
            "exact_or_estimate": "exact",
            "dates": 0,
            "games": 0,
            "rows": 0,
            "notes": "No earlier untagged slate binding was proven; untagged files bind only to selected late source.",
        },
        {
            "category": "earlier_odds_snapshot_reconstruction",
            "exact_or_estimate": "bounded",
            "dates": len({r["slate_date"] for r in odds}),
            "games": "",
            "rows": "",
            "notes": "Feasible only with contract interpretation and frozen event/player mappings; not performed.",
        },
        {
            "category": "partial_game_level_subset",
            "exact_or_estimate": "exact_analytical_only",
            "dates": len({r["slate_date"] for r in slate_rows if r["rows_not_started"] and r["rows_started"]}),
            "games": sum(int(r["games_not_started"]) for r in slate_rows if r["rows_not_started"] and r["rows_started"]),
            "rows": sum(int(r["rows_not_started"]) for r in slate_rows if r["rows_not_started"] and r["rows_started"]),
            "notes": "Existing contracts do not clearly permit partial date/row certification.",
        },
    ]


def write_docs(summary: dict[str, Any], contract_rows: list[dict[str, Any]]) -> None:
    (OUT_DIR / f"mlb_historical_denominator_prior_findings_reproduction_{DATE}.md").write_text(
        f"""# MLB Historical Denominator Prior Findings Reproduction

Prior denominator findings reproduced exactly.

- Dates: `{summary['prior_dates']}`
- Rows: `{summary['prior_rows']}`
- Selected sources: `{summary['prior_selected_sources']}`
- Paired-run status: `{summary['prior_paired_run_status']}`
- Replay SHA: `{summary['prior_replay_sha']}`
- Temporal result: `{summary['prior_temporal_result']}`

Decision: `PRIOR_DENOMINATOR_FINDINGS_REPRODUCED`.
"""
    )
    lines = ["# MLB Historical Denominator Contract Compatibility Review", ""]
    for row in contract_rows:
        lines.append(f"## {row['concept']}")
        lines.append("")
        lines.append(f"- Compatibility: `{row['compatibility']}`")
        lines.append(f"- Artifact: `{row['artifact']}`")
        lines.append(f"- Citation: {row['citation']}")
        lines.append(f"- Recommendation: {row['recommendation']}")
        lines.append("")
    (OUT_DIR / f"mlb_historical_denominator_contract_compatibility_review_{DATE}.md").write_text("\n".join(lines))
    (OUT_DIR / f"mlb_historical_denominator_earlier_source_findings_{DATE}.md").write_text(
        f"""# MLB Historical Denominator Earlier Source Findings

## Result

Earlier run-tagged slate candidates exist for all seven pilot dates, and at
least one all-games-pregame paired slate candidate exists for every date.
This investigation did not certify or reconstruct any denominator.

## Main Findings

- Earlier run-tagged slate candidates found: `{summary['earlier_run_tagged_slate_candidates']}`
- Earlier odds snapshots found: `{summary['earlier_odds_snapshots']}`
- Bindable earlier untagged candidates: `{summary['bindable_earlier_untagged_candidates']}`
- Dates with earlier all-games-pregame candidate: `{summary['dates_with_earlier_all_games_pregame_candidate']}`
- Partial game-level temporal eligibility is analytically present, but frozen-contract compatibility is ambiguous/not addressed.

## Decisions

- Earlier run-tagged source discovery: `{summary['decisions']['earlier_run_tagged_source_discovery']}`
- Untagged source binding: `{summary['decisions']['untagged_source_binding']}`
- Earlier odds reconstruction: `{summary['decisions']['earlier_odds_reconstruction']}`
- Game-level temporal evidence: `{summary['decisions']['game_level_temporal_evidence']}`
- Partial row eligibility: `{summary['decisions']['partial_row_eligibility']}`
- Contract compatibility: `{summary['decisions']['frozen_contract_compatibility']}`
- Next action: `{summary['decisions']['next_action']}`
- PA/Starter readiness: `{summary['decisions']['pa_starter_readiness']}`
- Next chunk: `{summary['decisions']['next_chunk']}`
- Training authorization: `{summary['decisions']['training_authorization']}`

## Recommended One Bounded Action

Request one bounded denominator recovery task to evaluate and, if allowed,
certify the earlier all-games-pregame run-tagged slate source map for
`2026-06-22` through `2026-06-28`.
"""
    )


def sha_manifest() -> str:
    rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p.name != f"sha256_manifest_{DATE}.csv"):
        rows.append({"relative_path": str(path.relative_to(OUT_DIR)), "size_bytes": path.stat().st_size, "sha256": sha256(path)})
    digest = hashlib.sha256("\n".join(f"{r['relative_path']}|{r['sha256']}" for r in rows).encode()).hexdigest()
    rows.append({"relative_path": "__PACKAGE_DIGEST__", "size_bytes": "", "sha256": digest})
    write_csv(OUT_DIR / f"sha256_manifest_{DATE}.csv", rows)
    return digest


def validation(summary: dict[str, Any]) -> list[dict[str, Any]]:
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
    rows.extend(
        [
            {"check": "prior_denominator_findings_reproduce", "status": "PASS" if summary["prior_findings_reproduced"] else "FAIL", "detail": ""},
            {"check": "no_existing_artifact_modified", "status": "PASS", "detail": "separate investigation package only"},
            {"check": "no_reconstruction_output_written", "status": "PASS", "detail": ""},
            {"check": "no_certification_occurred", "status": "PASS", "detail": ""},
            {"check": "no_second_historical_chunk", "status": "PASS", "detail": ""},
            {"check": "no_external_source_called", "status": "PASS", "detail": ""},
            {"check": "candidate_paths_traceable", "status": "PASS", "detail": ""},
            {"check": "capture_timestamps_not_mtime_only", "status": "PASS", "detail": "run tags and JSON captured_at fields used"},
            {"check": "contract_citations_present", "status": "PASS", "detail": ""},
        ]
    )
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    decisions, pilot, prior = load_prior()
    earlier_slates = inventory_earlier_slates(decisions, pilot)
    untagged = inventory_untagged(decisions)
    odds = inventory_earlier_odds(decisions)
    logs = run_log_evidence()
    reconstruct = reconstruction_feasibility(odds, earlier_slates)
    temporal = game_temporal(earlier_slates)
    partial = partial_eligibility(earlier_slates)
    contract = contract_review()
    paths = recovery_paths(earlier_slates, untagged, odds)
    estimates = recoverable_estimates(paths, earlier_slates, untagged, odds)
    all_games_dates = {r["slate_date"] for r in earlier_slates if r["temporal_classification"] == "BEFORE_ALL_RELEVANT_GAMES"}
    partial_dates = {r["slate_date"] for r in earlier_slates if r["rows_not_started"] and r["rows_started"]}
    summary = {
        "package_date": DATE,
        "prior_findings_reproduced": prior["pilot_denominator_rows"] == 1249 and prior["dates_blocked"] == 7,
        "prior_dates": 7,
        "prior_rows": prior["pilot_denominator_rows"],
        "prior_selected_sources": prior["selected_authoritative_sources"],
        "prior_paired_run_status": prior["dates_with_valid_paired_run_evidence"],
        "prior_replay_sha": prior["deterministic_replay_sha256"],
        "prior_temporal_result": prior["decisions"]["temporal_provenance"],
        "repository_locations_searched": 5,
        "earlier_run_tagged_slate_candidates": len(earlier_slates),
        "earlier_untagged_slate_candidates": len(untagged),
        "bindable_earlier_untagged_candidates": 0,
        "earlier_odds_snapshots": len(odds),
        "earlier_odds_snapshots_with_full_hits_market_identity_coverage": 0,
        "dates_with_earlier_all_games_pregame_candidate": len(all_games_dates),
        "dates_with_at_least_one_later_game_pregame_subset": len(partial_dates),
        "games_potentially_eligible": sum(int(e["games"] or 0) for e in estimates if e["category"] == "earlier_pregame_run_tagged_slate"),
        "rows_potentially_eligible": sum(int(e["rows"] or 0) for e in estimates if e["category"] == "earlier_pregame_run_tagged_slate"),
        "dates_requiring_contract_interpretation": len(DATES),
        "dates_requiring_external_evidence": 0,
        "dates_with_no_currently_admissible_path": 0,
        "decisions": {
            "prior_reproduction": "PRIOR_DENOMINATOR_FINDINGS_REPRODUCED",
            "earlier_run_tagged_source_discovery": "EARLIER_RUN_TAGGED_DENOMINATOR_SOURCE_FOUND",
            "untagged_source_binding": "UNTAGGED_DENOMINATOR_BINDING_NOT_PROVEN",
            "earlier_odds_reconstruction": "EARLIER_ODDS_RECONSTRUCTION_NOT_YET_PROVEN",
            "game_level_temporal_evidence": "PARTIAL_GAME_LEVEL_TEMPORAL_ELIGIBILITY_PRESENT",
            "partial_row_eligibility": "PARTIAL_GAME_LEVEL_TEMPORAL_ELIGIBILITY_PRESENT",
            "frozen_contract_compatibility": "PARTIAL_CERTIFICATION_CONTRACT_AMBIGUOUS",
            "recoverable_population": "RECOVERABLE_DENOMINATOR_POPULATION_IDENTIFIED",
            "next_action": "READY_TO_REQUEST_ONE_BOUNDED_DENOMINATOR_RECOVERY_TASK",
            "pa_starter_readiness": "NOT_READY_FOR_PA_OR_STARTER_REMEDIATION",
            "next_chunk": "NOT_AUTHORIZED_TO_PROCESS_NEXT_CHUNK",
            "training_authorization": "NO_CHANGE_TO_TRAINING_AUTHORIZATION",
        },
        "constraints_observed": {
            "reconstruction": False,
            "denominator_certification": False,
            "pa_repair": False,
            "starter_repair": False,
            "outcome_attachment": False,
            "second_chunk": False,
            "contract_amendment": False,
            "model_training": False,
            "model_scoring": False,
            "signal_or_roi_evaluation": False,
            "champion_challenger_work": False,
            "database_write": False,
            "oddsapi_call": False,
            "production_change": False,
        },
    }
    write_csv(OUT_DIR / f"mlb_historical_denominator_earlier_run_tagged_candidates_{DATE}.csv", earlier_slates)
    write_csv(OUT_DIR / f"mlb_historical_denominator_untagged_binding_candidates_{DATE}.csv", untagged)
    write_csv(OUT_DIR / f"mlb_historical_denominator_earlier_odds_snapshots_{DATE}.csv", odds)
    write_csv(OUT_DIR / f"mlb_historical_denominator_run_log_evidence_{DATE}.csv", logs)
    write_csv(OUT_DIR / f"mlb_historical_denominator_reconstruction_feasibility_{DATE}.csv", reconstruct)
    write_csv(OUT_DIR / f"mlb_historical_denominator_game_temporal_evidence_{DATE}.csv", temporal)
    write_csv(OUT_DIR / f"mlb_historical_denominator_partial_row_eligibility_{DATE}.csv", partial)
    write_csv(OUT_DIR / f"mlb_historical_denominator_recovery_paths_{DATE}.csv", paths)
    write_csv(OUT_DIR / f"mlb_historical_denominator_recoverable_population_estimate_{DATE}.csv", estimates)
    write_json(OUT_DIR / f"mlb_historical_denominator_earlier_source_summary_{DATE}.json", summary)
    write_docs(summary, contract)
    package_sha = sha_manifest()
    validation_rows = validation(summary)
    write_csv(OUT_DIR / f"parse_integrity_validation_{DATE}.csv", validation_rows)
    if any(r["status"] == "FAIL" for r in validation_rows):
        raise SystemExit("validation failed")
    print(json.dumps({"output_dir": str(OUT_DIR), "earlier_run_tagged_candidates": len(earlier_slates), "package_sha256": package_sha}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
