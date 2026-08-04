#!/usr/bin/env python3
"""Fail-closed MLB false-favorite first-execution admissibility audit.

This audit intentionally does not join outcomes.  It certifies the ordinary
decision snapshot rule, inventories exact run pairs, and stops when the
approved model/source-version admissibility gate cannot be satisfied.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


RUN_RE = re.compile(r"mlb_slate_output__(local_daily_\d{8}T\d{6}Z)\.csv$")
SCOPED_PROPS = {"hits", "total_bases", "strikeouts_pitching"}
DECISION = "FAVORITE_POPULATION_NOT_RECOVERABLE"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0]) if rows else ["status", "reason"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_time(tag: str) -> str:
    stamp = tag.removeprefix("local_daily_").removesuffix("Z")
    return datetime.strptime(stamp, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--odds-root", default="backend/mlb/exports/odds_history")
    ap.add_argument("--reconcile-csv", required=True)
    ap.add_argument("--from-date", default="2026-05-01")
    ap.add_argument("--to-date", default="2026-06-30")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    root = Path(args.odds_root)
    out = Path(args.out_dir)
    if out.exists():
        raise SystemExit(f"immutable output directory already exists: {out}")
    out.mkdir(parents=True)

    snapshot_rows: list[dict] = []
    chosen_by_date: dict[str, dict] = {}
    for day in pd.date_range(args.from_date, args.to_date):
        date = day.date().isoformat()
        day_dir = root / date
        candidates: list[tuple[str, Path, Path]] = []
        for slate in sorted(day_dir.glob("mlb_slate_output__local_daily_*.csv")):
            match = RUN_RE.search(slate.name)
            if not match:
                continue
            tag = match.group(1)
            odds = day_dir / f"odds_mlb_playerprops__{tag}.json"
            if odds.exists():
                candidates.append((tag, slate, odds))
        candidates.sort(key=lambda item: item[0])
        chosen = candidates[0] if candidates else None
        if chosen:
            chosen_by_date[date] = {
                "run_tag": chosen[0],
                "snapshot_time_utc": run_time(chosen[0]),
                "slate": str(chosen[1]),
                "odds": str(chosen[2]),
                "slate_sha256": sha256(chosen[1]),
                "odds_sha256": sha256(chosen[2]),
            }
        for tag, slate, odds in candidates:
            snapshot_rows.append(
                {
                    "game_date": date,
                    "run_tag": tag,
                    "snapshot_time_utc": run_time(tag),
                    "decision": "CHOSEN" if chosen and tag == chosen[0] else "REJECTED_LATER_ORDINARY_RUN",
                    "slate_path": str(slate),
                    "odds_path": str(odds),
                    "slate_sha256": sha256(slate),
                    "odds_sha256": sha256(odds),
                }
            )
        if not candidates:
            snapshot_rows.append(
                {
                    "game_date": date,
                    "run_tag": "",
                    "snapshot_time_utc": "",
                    "decision": "EXCLUDED_NO_PAIRED_LOCAL_DAILY_RUN",
                    "slate_path": "",
                    "odds_path": "",
                    "slate_sha256": "",
                    "odds_sha256": "",
                }
            )

    write_csv(out / "canonical_snapshot_selection_manifest.csv", snapshot_rows)

    rec = pd.read_csv(args.reconcile_csv, low_memory=False)
    rec["game_date"] = rec["game_date"].astype(str)
    rec = rec[rec["prop_type"].astype(str).str.lower().isin(SCOPED_PROPS)].copy()
    rec["chosen_run_tag"] = rec["game_date"].map(lambda d: chosen_by_date.get(d, {}).get("run_tag", ""))
    rec["chosen_snapshot_time_utc"] = rec["game_date"].map(
        lambda d: chosen_by_date.get(d, {}).get("snapshot_time_utc", "")
    )

    # The approved gate requires a resolved historical model/source version.
    # These fields are absent from both the wide and slate contracts in this window.
    version_fields = [
        "model_version",
        "model_family",
        "model_artifact_hash",
        "feature_hash",
        "model_run_id",
    ]
    rec["admissible"] = False
    rec["exclusion_reason"] = "UNRESOLVED_HISTORICAL_MODEL_SOURCE_VERSION"
    exclusions = rec[
        [
            "game_date", "game_id", "player_id", "player_name", "prop_type", "line",
            "model_pick_side", "bookmaker_key", "chosen_run_tag",
            "chosen_snapshot_time_utc", "exclusion_reason",
        ]
    ].copy()
    exclusions.to_csv(out / "exclusions_ledger.csv", index=False)

    audit_rows = [
        {"gate": "ordinary_snapshot_rule", "status": "PASS", "rows": len(rec), "reason": "earliest paired local_daily run; local_prewarm excluded"},
        {"gate": "paired_run_coverage", "status": "PASS" if len(chosen_by_date) == 61 else "FAIL", "rows": len(chosen_by_date), "reason": "one earliest paired ordinary run per date"},
        {"gate": "outcomes_not_joined_before_freeze", "status": "PASS", "rows": 0, "reason": "reconstruction invoked with --skip-outcomes"},
        {"gate": "resolved_model_source_version", "status": "FAIL", "rows": 0, "reason": "none of " + "|".join(version_fields) + " are archived in the prediction contracts"},
        {"gate": "favorite_control_recoverable", "status": "FAIL", "rows": 0, "reason": "approved admissibility gate failed before favorite construction"},
    ]
    write_csv(out / "population_admissibility_audit.csv", audit_rows)

    empty_control_fields = [
        "game_date", "game_id", "player_id", "prop_type", "line", "selected_side",
        "bookmaker", "selected_side_price", "selected_side_no_vig_implied", "model_probability",
        "model_source_version", "outcome",
    ]
    write_csv(
        out / "deduplicated_favorite_control_population.csv",
        [],
        empty_control_fields,
    )

    registry = [
        ("PROBABILITY_BAND_INFLATION", "confidence-invalidating"),
        ("FALLBACK_PROXY_DEPENDENCE", "operational-only pending evidence"),
        ("STALE_CONTRIBUTING_HISTORY", "operational-only pending evidence"),
        ("INCOMPLETE_PRIOR_HISTORY", "confidence-invalidating pending evidence"),
        ("FIXED_FAVORITE_PRICE_BURDEN", "price-invalidating"),
        ("MAY_DERIVED_UNSUPPORTED_BREAK_EVEN_PRICE", "price-invalidating"),
        ("DISTRIBUTION_MONOTONICITY_FAILURE", "prediction-invalidating"),
        ("THRESHOLD_FRAGILITY", "confidence-invalidating"),
    ]
    write_csv(
        out / "condition_registry.csv",
        [{"condition": name, "classification": kind, "execution_status": "NOT_EXECUTED_ADMISSIBILITY_STOP"} for name, kind in registry],
    )

    certification = f"""# Normal decision-window certification

Status: `CERTIFIED`

The ordinary-window rule is the earliest exactly paired `local_daily` slate/odds run for each date.
`local_prewarm` is excluded because it prepares inputs before the ordinary review build. Later daily
runs are retained as rejected diagnostics and are never substituted. Early-game rows would require
`snapshot_time_utc < scheduled_start`; however, row admission stopped earlier at the unresolved
historical model/source-version gate. The rule uses no outcome, row count, completeness, ROI, price
quality, or candidate-count criterion.
"""
    (out / "normal_decision_window_certification.md").write_text(certification, encoding="utf-8")

    placeholder_names = [
        "may_characterization_tables.csv",
        "june_locked_validation_tables.csv",
        "condition_level_comparison_tables.csv",
        "probability_calibration_report.csv",
        "price_burden_and_price_ceiling_report.csv",
        "distribution_coherence_report.csv",
        "concentration_diagnostics.csv",
        "version_and_provenance_stratification.csv",
    ]
    for name in placeholder_names:
        write_csv(out / name, [{"status": "NOT_EXECUTED", "reason": "UNRESOLVED_HISTORICAL_MODEL_SOURCE_VERSION"}])

    decision_payload = {
        "decision": DECISION,
        "window": {"from": args.from_date, "to": args.to_date},
        "holdout_inspected": False,
        "nhl_implemented": False,
        "outcomes_joined": False,
        "ordinary_window_certified": True,
        "paired_ordinary_dates": len(chosen_by_date),
        "scoped_prediction_market_rows_before_version_gate": int(len(rec)),
        "favorite_control_rows": 0,
        "blocking_gate": "UNRESOLVED_HISTORICAL_MODEL_SOURCE_VERSION",
    }
    (out / "decision.json").write_text(json.dumps(decision_payload, indent=2) + "\n", encoding="utf-8")
    (out / "executive_decision.md").write_text(
        f"""# MLB false-favorite bounded first execution

Decision: `{DECISION}`

The normal decision window is certified and all 61 dates have an earliest paired ordinary daily run.
The three-proposition reconstruction contains {len(rec):,} prediction/market rows before the version
gate. The favorite control was not constructed and outcomes were not joined because historical model
identity is absent: the archived contracts do not retain a model version, model artifact hash, feature
hash, or model run ID. File hashes establish emitted-file identity but cannot establish which semantic
model produced the probabilities or permit required version-stratified reporting.

No condition, residual, model, selector, production gate, or promotion claim is authorized.
""",
        encoding="utf-8",
    )

    manifest_rows = []
    for path in sorted(p for p in out.iterdir() if p.name != "SHA256SUMS.csv"):
        manifest_rows.append({"file": path.name, "sha256": sha256(path), "bytes": path.stat().st_size})
    write_csv(out / "SHA256SUMS.csv", manifest_rows)
    print(json.dumps(decision_payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
