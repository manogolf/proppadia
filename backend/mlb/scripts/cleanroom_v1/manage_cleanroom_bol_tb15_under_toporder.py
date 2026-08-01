#!/usr/bin/env python3
"""Final prospective clean-room TB Under top-order replication lifecycle."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from backend.mlb.scripts.cleanroom_v1.closeout_cleanroom_bol_tb15 import (
    EXPORT_ROOT,
    american_profit,
    confirmed_starters_before_pitch,
    latest_schedule,
    load_runs,
    parse_timestamp,
    preserve_official_outcomes,
)
from backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_hypotheses import (
    ROOT,
    build_final_population,
    csv_content,
    write_csv,
)

RULE_VERSION = "h1_top_order_final_replication_v1"
SIGNAL_RESEARCH_PAUSED = True
FIXED_COHORT_ACTIVATION_DATE = "2026-08-01"
CLOSURE_TIMESTAMP = "2026-07-31T17:08:08Z"
CLOSED_HYPOTHESES = [
    {
        "hypothesis": "H2_PRICE_BAND_REJECTION",
        "original_rule": "reject final-pregame BetOnline Under odds -199 through -150 inclusive",
        "july29_result": "CLEAR_ONE_SLATE_REJECTION_CANDIDATE",
        "july30_result": "FAILED_TO_REPLICATE_CLOSE_HYPOTHESIS",
        "terminal_decision": "CLOSED",
        "closure_timestamp_utc": CLOSURE_TIMESTAMP,
        "reason": "Failed the frozen July 30 prospective replication rule",
    },
    {
        "hypothesis": "H3_PERSISTENT_MARKET_REJECTION",
        "original_rule": "reject PERSISTENT markets appearing in at least half of eligible pregame captures",
        "july29_result": "CLEAR_ONE_SLATE_REJECTION_CANDIDATE",
        "july30_result": "FAILED_TO_REPLICATE_CLOSE_HYPOTHESIS",
        "terminal_decision": "CLOSED",
        "closure_timestamp_utc": CLOSURE_TIMESTAMP,
        "reason": "Failed the frozen July 30 prospective replication rule",
    },
]


def evidence_root(slate: str) -> Path:
    return (
        ROOT / "artifacts/analysis/model_development/"
        "mlb_cleanroom_bol_tb15_under_top_order_final_replication" / slate
    )


def lifecycle_root(slate: str) -> Path:
    return EXPORT_ROOT / slate / "under_toporder_final"


def population_summary(rows: list[dict]) -> dict:
    settled = [row for row in rows if row["settlement_status"] == "SETTLED"]
    wins = sum(row["under_outcome"] == "UNDER_WIN" for row in settled)
    losses = sum(row["under_outcome"] == "UNDER_LOSS" for row in settled)
    stake = len(settled) * 5
    net = sum(
        american_profit(5, int(row["final_under_odds"]))
        if row["under_outcome"] == "UNDER_WIN" else -5
        for row in settled
    )
    return {
        "frozen_rows": len(rows), "actionable_wagers": len(settled),
        "wins": wins, "losses": losses,
        "no_action": sum(row["settlement_status"] == "VOID" for row in rows),
        "pending": sum(row["settlement_status"] == "PENDING" for row in rows),
        "technical_unresolved": sum(
            row["settlement_status"] == "UNRESOLVED" for row in rows
        ),
        "win_rate": wins / len(settled) if settled else None,
        "stake": stake, "net_dollars": net, "roi": net / stake if stake else None,
        "average_under_odds": (
            sum(int(row["final_under_odds"]) for row in settled) / len(settled)
            if settled else None
        ),
        "games": len({row["game_pk"] for row in settled}),
    }


def finalize_july30_audit(target: Path) -> list[dict]:
    source = EXPORT_ROOT / "2026-07-30" / "under_hypotheses"
    graded = {
        (row["game_pk"], row["player_mlb_id"]): row
        for row in csv.DictReader((source / "under_closeout_rows.csv").open())
    }
    files = {
        "BASELINE_ALL_UNDER": "baseline_all_under.csv",
        "REJECTED_TOP_ORDER": "rejected_top_order.csv",
        "RETAIN_AFTER_TOP_ORDER_REJECTION": "retained_after_top_order.csv",
    }
    audit = []
    for name, filename in files.items():
        members = list(csv.DictReader((source / filename).open()))
        rows = [graded[(row["game_pk"], row["player_mlb_id"])] for row in members]
        audit.append({"population": name, **population_summary(rows)})
    target.mkdir(parents=True, exist_ok=True)
    write_csv(target / "july30_final_result_audit.csv", audit)
    no_action = [row for row in graded.values() if row["settlement_status"] == "VOID"]
    if len(no_action) != 3:
        raise RuntimeError(f"July 30 expected exactly 3 NO_ACTION rows, found {len(no_action)}")
    (target / "closed_hypothesis_manifest.json").write_text(
        json.dumps({"closed_hypotheses": CLOSED_HYPOTHESES}, indent=2) + "\n"
    )
    return audit


def capture_coverage(slate: str, runs: list[dict], games: list[dict]) -> list[dict]:
    rows = []
    for game in games:
        game_pk = int(game["gamePk"])
        pitch = parse_timestamp(game["gameDate"])
        eligible = [run for run in runs if run["source_capture_timestamp_utc"] < pitch]
        market_runs = []
        for run in eligible:
            markets = list(csv.DictReader(
                (run["snapshot"] / "bol_tb15_two_sided_markets.csv").open()
            ))
            if any(int(row["game_pk"]) == game_pk for row in markets):
                market_runs.append(run["run_tag"])
        rows.append({
            "game_pk": game_pk, "first_pitch_utc": pitch.isoformat(),
            "eligible_capture_count": len(eligible),
            "valid_market_capture_count": len(market_runs),
            "valid_pre_first_pitch_capture": bool(market_runs),
            "source_run_tags": "|".join(market_runs),
        })
    return rows


def freeze(slate: str) -> dict:
    if slate >= FIXED_COHORT_ACTIVATION_DATE:
        raise RuntimeError("H1_FULL_SLATE_PATH_RETIRED_USE_FIXED_COHORT_V1")
    from backend.mlb.scripts.cleanroom_v1.lifecycle_guards import assert_signal_eligible
    assert_signal_eligible(slate)
    neutral_manifest = EXPORT_ROOT / slate / "final_population_manifest.json"
    if not neutral_manifest.exists():
        raise RuntimeError("PREGAME_FREEZE_REQUIRED: signal freeze requires neutral population freeze")
    if SIGNAL_RESEARCH_PAUSED:
        raise RuntimeError("SIGNAL_RESEARCH_PAUSED_PENDING_PROSPECTIVE_EVIDENCE_LINEAGE_CERTIFICATION")
    population, runs, games = build_final_population(slate)
    coverage = capture_coverage(slate, runs, games)
    if not coverage or not all(row["valid_pre_first_pitch_capture"] for row in coverage):
        raise RuntimeError("SOURCE_LIFECYCLE_FAILURE_NO_H1_DECISION")
    rejected = [row for row in population if row["h1_action"] == "REJECT_TOP_ORDER"]
    retained = [
        row for row in population if row["h1_action"] == "RETAIN_CONFIRMED_NON_TOP_ORDER"
    ]
    unknown = [row for row in population if row["h1_action"] == "ORDER_NOT_CONFIRMED"]
    definitions = {
        "baseline_all_under.csv": population,
        "rejected_top_order.csv": rejected,
        "retained_after_top_order.csv": retained,
        "order_not_confirmed.csv": unknown,
    }
    freeze_at = datetime.now(timezone.utc).isoformat()
    fields = list(population[0])
    manifests = {}
    for filename, rows in definitions.items():
        payload = csv_content(rows, fields)
        manifests[filename] = {
            "population_name": filename.removesuffix(".csv").upper(),
            "rule_version": RULE_VERSION, "slate_date": slate,
            "freeze_timestamp_utc": freeze_at,
            "source_capture_run_tags": [run["run_tag"] for run in runs],
            "identity_count": len(rows), "actionable_count": len(rows),
            "excluded_count": len(population) - len(rows),
            "membership_reason": "OUTCOME_INDEPENDENT_FROZEN_H1_MEMBERSHIP",
            "sha256": hashlib.sha256(payload).hexdigest(),
        }
    package_sha = hashlib.sha256(json.dumps(
        {key: value["sha256"] for key, value in manifests.items()}, sort_keys=True
    ).encode()).hexdigest()
    root = lifecycle_root(slate)
    if root.exists():
        prior = json.loads((root / "toporder_hypothesis_manifest.json").read_text())
        if prior["package_sha256"] == package_sha:
            print(json.dumps({"freeze_status": "ALREADY_FROZEN_IDENTICAL", **prior}, indent=2))
            return prior
        raise RuntimeError("top-order freeze collision with different content")
    root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=root.parent, prefix=".toporder_freeze_"))
    try:
        for filename, rows in definitions.items():
            write_csv(staging / filename, rows, fields)
        manifest = {
            "rule_version": RULE_VERSION, "slate_date": slate,
            "freeze_timestamp_utc": freeze_at, "outcomes_accessed_for_membership": False,
            "source_lifecycle_status": "COMPLETE_CERTIFIABLE_CAPTURE_SPINE",
            "official_games": len(games), "identity_rejects": 0,
            "populations": manifests, "package_sha256": package_sha,
        }
        (staging / "toporder_hypothesis_manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )
        os.replace(staging, root)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    evidence = evidence_root(slate)
    finalize_july30_audit(evidence)
    write_csv(evidence / "test_slate_capture_coverage.csv", coverage)
    write_csv(evidence / "test_slate_population_freeze.csv", [
        {"file": filename, **item} for filename, item in manifests.items()
    ])
    print(json.dumps(manifest, indent=2))
    return manifest


def grade_baseline(slate: str, baseline: list[dict], runs: list[dict], games: list[dict]) -> list[dict]:
    outcomes, raw_dir = preserve_official_outcomes(slate, games)
    starters = confirmed_starters_before_pitch(runs, games)
    graded = []
    for row in baseline:
        game_pk, player_id = int(row["game_pk"]), int(row["player_mlb_id"])
        result = outcomes.get((game_pk, player_id))
        game_source = outcomes.get((game_pk, None), {})
        if result is None and game_source.get("game_status") == "Final":
            if game_pk in starters and player_id not in starters[game_pk]:
                result, outcome, settlement = game_source, "NO_ACTION", "VOID"
            else:
                result, outcome, settlement = game_source, "TECHNICAL_UNRESOLVED", "UNRESOLVED"
        elif result is None or result.get("game_status") != "Final":
            result, outcome, settlement = result or game_source, "PENDING", "PENDING"
        elif int(result["plate_appearances"]) == 0:
            outcome, settlement = "NO_ACTION", "VOID"
        elif int(result["total_bases"]) <= 1:
            outcome, settlement = "UNDER_WIN", "SETTLED"
        else:
            outcome, settlement = "UNDER_LOSS", "SETTLED"
        graded.append({
            **row, "under_outcome": outcome, "settlement_status": settlement,
            "plate_appearances": result.get("plate_appearances", ""),
            "total_bases": result.get("total_bases", ""),
            "outcome_source": result.get("outcome_source", ""),
            "outcome_sha256": result.get("outcome_source_sha256", ""),
            "official_outcome_raw_directory": str(raw_dir),
        })
    return graded


def closeout(slate: str) -> dict:
    if slate >= FIXED_COHORT_ACTIVATION_DATE:
        raise RuntimeError("H1_FULL_SLATE_PATH_RETIRED_USE_FIXED_COHORT_V1")
    if SIGNAL_RESEARCH_PAUSED:
        raise RuntimeError("SIGNAL_RESEARCH_PAUSED_PENDING_PROSPECTIVE_EVIDENCE_LINEAGE_CERTIFICATION")
    root = lifecycle_root(slate)
    manifest_path = root / "toporder_hypothesis_manifest.json"
    if not manifest_path.exists():
        raise RuntimeError("top-order populations are not frozen")
    manifest = json.loads(manifest_path.read_text())
    baseline = list(csv.DictReader((root / "baseline_all_under.csv").open()))
    runs, failures = load_runs(slate)
    if failures:
        raise RuntimeError("SOURCE_LIFECYCLE_FAILURE_NO_H1_DECISION")
    games, _ = latest_schedule(slate, runs)
    graded = grade_baseline(slate, baseline, runs, games)
    by_key = {(row["game_pk"], row["player_mlb_id"]): row for row in graded}
    files = {
        "BASELINE_ALL_UNDER": "baseline_all_under.csv",
        "REJECTED_TOP_ORDER": "rejected_top_order.csv",
        "RETAIN_AFTER_TOP_ORDER_REJECTION": "retained_after_top_order.csv",
    }
    summaries = []
    population_rows = {}
    for name, filename in files.items():
        members = list(csv.DictReader((root / filename).open()))
        rows = [by_key[(row["game_pk"], row["player_mlb_id"])] for row in members]
        population_rows[name] = rows
        summaries.append({"population": name, **population_summary(rows)})
    base, rejected, retained = summaries
    rejected_rows = population_rows["REJECTED_TOP_ORDER"]
    counts = Counter(row["game_pk"] for row in rejected_rows)
    loss_share = rejected["losses"] / base["losses"] if base["losses"] else 0
    winner_share = rejected["wins"] / base["wins"] if base["wins"] else 0
    impact = {
        "hypothesis": "H1_TOP_ORDER_REJECTION",
        "rows_rejected": rejected["frozen_rows"],
        "losses_removed": rejected["losses"], "winners_sacrificed": rejected["wins"],
        "loss_share_removed": loss_share, "winner_share_removed": winner_share,
        "removal_advantage": loss_share - winner_share,
        "retained_win_rate_change": (
            retained["win_rate"] - base["win_rate"]
            if retained["win_rate"] is not None and base["win_rate"] is not None else None
        ),
        "retained_roi_change": (
            retained["roi"] - base["roi"]
            if retained["roi"] is not None and base["roi"] is not None else None
        ),
        "games_represented": len(counts),
        "largest_single_game_share": max(counts.values()) / len(rejected_rows) if rejected_rows else 0,
    }
    incomplete = any(row["pending"] or row["technical_unresolved"] for row in summaries)
    if incomplete:
        decision = "SOURCE_LIFECYCLE_FAILURE_NO_DECISION"
    elif rejected["frozen_rows"] < 20 or impact["games_represented"] < 6:
        decision = "INSUFFICIENT_VOLUME_CLOSE_H1"
    elif (
        impact["loss_share_removed"] > impact["winner_share_removed"]
        and retained["win_rate"] > base["win_rate"]
        and retained["roi"] > base["roi"]
        and impact["largest_single_game_share"] <= 0.25
    ):
        decision = "REPLICATED_THIRD_SLATE_READY_FOR_BOUNDED_OPERATOR_REVIEW"
    else:
        decision = "FAILED_THIRD_SLATE_CLOSE_H1"
    status_value = "FINAL" if not incomplete else "PENDING_OR_UNRESOLVED"
    content_sha = hashlib.sha256(
        csv_content(summaries) + csv_content([impact])
    ).hexdigest()
    closeout_manifest_path = root / "toporder_closeout_manifest.json"
    prior = json.loads(closeout_manifest_path.read_text()) if closeout_manifest_path.exists() else {}
    if prior.get("content_sha256") == content_sha:
        print(json.dumps({"changed": False, **prior}, indent=2))
        return prior
    revision = int(prior.get("revision", 0)) + 1
    write_csv(root / "toporder_closeout_rows.csv", graded)
    write_csv(root / "toporder_closeout_results.csv", summaries)
    write_csv(root / "toporder_rejection_impact.csv", [impact])
    closeout_manifest = {
        "slate_date": slate, "revision": revision, "status": status_value,
        "terminal_decision": decision, "content_sha256": content_sha,
        "frozen_package_sha256": manifest["package_sha256"],
    }
    closeout_manifest_path.write_text(json.dumps(closeout_manifest, indent=2) + "\n")
    evidence = evidence_root(slate)
    evidence.mkdir(parents=True, exist_ok=True)
    finalize_july30_audit(evidence)
    write_csv(evidence / "test_slate_closeout_results.csv", summaries)
    write_csv(evidence / "test_slate_rejection_impact.csv", [impact])
    if status_value == "FINAL":
        cumulative = cumulative_results(slate, summaries)
        write_csv(evidence / "three_slate_cumulative_results.csv", cumulative)
        write_report(evidence, slate, summaries, impact, cumulative, decision)
    print(json.dumps({**closeout_manifest, "results": summaries, "impact": impact}, indent=2))
    return closeout_manifest


def historical_slate(date: str) -> list[dict]:
    if date == "2026-07-29":
        rows = list(csv.DictReader(
            (ROOT / "artifacts/analysis/model_development/"
             "mlb_cleanroom_bol_tb15_under_fail_fast_audit/2026-07-30/"
             "july29_under_canonical_218.csv").open()
        ))
        baseline = [{**row, "settlement_status": "SETTLED",
                     "final_under_odds": row["final_under_odds"]} for row in rows]
        rejected = [row for row in baseline if row["h1_batting_order"] == "POSITIONS_1_TO_3"]
        retained = [row for row in baseline if row["h1_batting_order"] in {"POSITIONS_4_TO_6", "POSITIONS_7_TO_9"}]
    else:
        root = EXPORT_ROOT / date / "under_hypotheses"
        graded = {(row["game_pk"], row["player_mlb_id"]): row for row in csv.DictReader((root / "under_closeout_rows.csv").open())}
        def members(filename: str) -> list[dict]:
            return [graded[(row["game_pk"], row["player_mlb_id"])] for row in csv.DictReader((root / filename).open())]
        baseline = members("baseline_all_under.csv")
        rejected = members("rejected_top_order.csv")
        # The immutable July 30 operator population included explicit unknown-order
        # rows. The cross-slate 4-9 comparison excludes those rows without
        # rewriting the historical freeze or closeout.
        retained = [
            row for row in baseline
            if row["h1_action"] == "RETAIN_CONFIRMED_NON_TOP_ORDER"
        ]
    return [
        {"slate_date": date, "population": name, **population_summary(rows)}
        for name, rows in [
            ("BASELINE_ALL_UNDER", baseline), ("REJECTED_TOP_ORDER", rejected),
            ("RETAIN_AFTER_TOP_ORDER_REJECTION", retained),
        ]
    ]


def cumulative_results(slate: str, current: list[dict]) -> list[dict]:
    per_slate = historical_slate("2026-07-29") + historical_slate("2026-07-30")
    per_slate += [{"slate_date": slate, **row} for row in current]
    output = list(per_slate)
    for population in {row["population"] for row in per_slate}:
        rows = [row for row in per_slate if row["population"] == population]
        wagers, wins, losses = (sum(int(row[key]) for row in rows) for key in ("actionable_wagers", "wins", "losses"))
        stake = sum(float(row["stake"]) for row in rows)
        net = sum(float(row["net_dollars"]) for row in rows)
        output.append({
            "slate_date": "CUMULATIVE", "population": population,
            "actionable_wagers": wagers, "wins": wins, "losses": losses,
            "win_rate": wins / wagers if wagers else None, "stake": stake,
            "net_dollars": net, "roi": net / stake if stake else None,
            "slates_improved": "CALCULATED_IN_REPORT", "slates_worsened": "CALCULATED_IN_REPORT",
        })
    return output


def write_report(target: Path, slate: str, summaries: list[dict], impact: dict, cumulative: list[dict], decision: str) -> None:
    lines = [f"# TB Under top-order final replication — {slate}", "", f"Decision: `{decision}`", "", "## Test-slate results", "", "| Population | Wagers | W-L | Win rate | Net | ROI |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in summaries:
        lines.append(f"| {row['population']} | {row['actionable_wagers']} | {row['wins']}-{row['losses']} | {row['win_rate']:.2%} | ${row['net_dollars']:.2f} | {row['roi']:.2%} |")
    lines.extend(["", "## Rejection impact", "", f"- Loss share removed: {impact['loss_share_removed']:.2%}", f"- Winner share removed: {impact['winner_share_removed']:.2%}", f"- Removal advantage: {impact['removal_advantage']:.2%}"])
    (target / "top_order_final_replication_report.md").write_text("\n".join(lines) + "\n")
    (target / "terminal_decision.md").write_text(f"MLB_CLEANROOM_UNDER_TOP_ORDER_THIRD_SLATE_DECISION = {decision}\n")


def status(slate: str) -> dict:
    runs, failures = load_runs(slate)
    games, _ = latest_schedule(slate, runs) if runs and not failures else ([], {})
    coverage = capture_coverage(slate, runs, games) if games else []
    root = lifecycle_root(slate)
    freeze_manifest = json.loads((root / "toporder_hypothesis_manifest.json").read_text()) if (root / "toporder_hypothesis_manifest.json").exists() else {}
    closeout_manifest = json.loads((root / "toporder_closeout_manifest.json").read_text()) if (root / "toporder_closeout_manifest.json").exists() else {}
    results = list(csv.DictReader((root / "toporder_closeout_results.csv").open())) if (root / "toporder_closeout_results.csv").exists() else []
    result = {
        "capture_coverage": {"captures": len(runs), "official_games": len(games), "games_with_valid_pre_first_pitch_capture": sum(row["valid_pre_first_pitch_capture"] for row in coverage), "identity_rejects": sum(int(run["manifest"].get("identity_rejects", 0)) for run in runs), "untrusted_captures": len(failures)},
        "freeze_status": "FROZEN" if freeze_manifest else "NOT_FROZEN",
        "baseline_rows": freeze_manifest.get("populations", {}).get("baseline_all_under.csv", {}).get("identity_count", 0),
        "top_order_rejected_rows": freeze_manifest.get("populations", {}).get("rejected_top_order.csv", {}).get("identity_count", 0),
        "retained_rows": freeze_manifest.get("populations", {}).get("retained_after_top_order.csv", {}).get("identity_count", 0),
        "results": [{key: row[key] for key in ("population", "no_action", "wins", "losses", "pending", "technical_unresolved", "win_rate", "net_dollars", "roi")} for row in results],
        "closeout_revision": closeout_manifest.get("revision", 0),
        "terminal_decision": closeout_manifest.get("terminal_decision", "PENDING"),
    }
    print(json.dumps(result, indent=2))
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--mode", required=True, choices=("freeze", "closeout", "status", "july30-audit"))
    args = parser.parse_args()
    if args.mode == "freeze": freeze(args.date)
    elif args.mode == "closeout": closeout(args.date)
    elif args.mode == "status": status(args.date)
    else:
        print(json.dumps(finalize_july30_audit(evidence_root(args.date)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
