#!/usr/bin/env python3
"""Prospective clean-room TB Under hypothesis freeze, closeout, and status."""

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

ROOT = Path(__file__).resolve().parents[4]
ANALYSIS_ROOT = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_cleanroom_bol_tb15_under_next_slate_replication/2026-07-30"
)
RULE_VERSION = "july29_frozen_rules_commit_3abb963d"
MULTI_HYPOTHESIS_LAST_ALLOWED_SLATE = "2026-07-30"
SIGNAL_RESEARCH_PAUSED = True


def csv_content(rows: list[dict], fields: list[str] | None = None) -> bytes:
    fields = fields or list(rows[0])
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode()


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(csv_content(rows, fields))


def sha_rows(rows: list[dict], fields: list[str]) -> str:
    return hashlib.sha256(csv_content(rows, fields)).hexdigest()


def july29_overlap() -> dict:
    source = (
        ROOT / "artifacts/analysis/model_development/"
        "mlb_cleanroom_bol_tb15_under_fail_fast_audit/2026-07-30/"
        "july29_under_canonical_218.csv"
    )
    rows = list(csv.DictReader(source.open()))
    for row in rows:
        row["identity"] = (
            f"{row['slate_date']}|{row['game_pk']}|{row['player_mlb_id']}|"
            "Total Bases|1.5"
        )
        row["H1_TOP_ORDER_REJECTION"] = (
            row["h1_batting_order"] == "POSITIONS_1_TO_3"
        )
        row["H2_PRICE_BAND_REJECTION"] = (
            row["h2_final_under_price"] == "-199_TO_-150"
        )
        row["H3_PERSISTENT_MARKET_REJECTION"] = (
            row["h4_market_persistence"] == "PERSISTENT"
        )
    names = [
        "H1_TOP_ORDER_REJECTION", "H2_PRICE_BAND_REJECTION",
        "H3_PERSISTENT_MARKET_REJECTION",
    ]
    sets = {name: {row["identity"] for row in rows if row[name]} for name in names}
    audit = []
    combinations = [
        ("H1_H2", names[:2]), ("H1_H3", [names[0], names[2]]),
        ("H2_H3", names[1:]), ("H1_H2_H3", names),
    ]
    by_id = {row["identity"]: row for row in rows}
    for label, members in combinations:
        intersection = set.intersection(*(sets[name] for name in members))
        union = set.union(*(sets[name] for name in members))
        item = {
            "comparison": label,
            **{f"{name}_rows": len(sets[name]) for name in members},
            "intersection_count": len(intersection), "union_count": len(union),
            "jaccard_similarity": len(intersection) / len(union),
            "intersection_wins": sum(
                by_id[key]["under_outcome"] == "UNDER_WIN" for key in intersection
            ),
            "intersection_losses": sum(
                by_id[key]["under_outcome"] == "UNDER_LOSS" for key in intersection
            ),
        }
        for name in members:
            unique = sets[name] - set.union(
                *(sets[other] for other in members if other != name)
            )
            item[f"{name}_unique_rows"] = len(unique)
            item[f"{name}_unique_wins"] = sum(
                by_id[key]["under_outcome"] == "UNDER_WIN" for key in unique
            )
            item[f"{name}_unique_losses"] = sum(
                by_id[key]["under_outcome"] == "UNDER_LOSS" for key in unique
            )
        audit.append(item)
    matrix = [{
        "identity": row["identity"], "slate_date": row["slate_date"],
        "game_pk": row["game_pk"], "player_mlb_id": row["player_mlb_id"],
        "player": row["player"], "under_outcome": row["under_outcome"],
        **{name: row[name] for name in names},
        "membership_count": sum(row[name] for name in names),
    } for row in rows]
    ANALYSIS_ROOT.mkdir(parents=True, exist_ok=True)
    fields = sorted({key for row in audit for key in row})
    write_csv(ANALYSIS_ROOT / "july29_hypothesis_overlap_audit.csv", audit, fields)
    write_csv(ANALYSIS_ROOT / "july29_hypothesis_unique_rows.csv", matrix)
    result = {
        "H1_rows": len(sets[names[0]]), "H2_rows": len(sets[names[1]]),
        "H3_rows": len(sets[names[2]]), "H1_H2_exact_duplicate": sets[names[0]] == sets[names[1]],
        "audit": audit,
    }
    print(json.dumps(result, indent=2))
    return result


def build_final_population(slate: str) -> tuple[list[dict], list[dict], list[dict]]:
    runs, failures = load_runs(slate)
    if failures or not runs:
        raise RuntimeError("capture spine unavailable or untrusted")
    games, _ = latest_schedule(slate, runs)
    now = datetime.now(timezone.utc)
    unstarted = [
        game for game in games if parse_timestamp(game["gameDate"]) > now
    ]
    if unstarted:
        latest = max(parse_timestamp(game["gameDate"]) for game in unstarted)
        raise RuntimeError(
            f"final freeze refused: {len(unstarted)} games have not started; "
            f"last first pitch={latest.isoformat()}"
        )
    selected = {}
    for game in games:
        game_pk = int(game["gamePk"])
        pitch = parse_timestamp(game["gameDate"])
        eligible_runs = [
            run for run in runs if run["source_capture_timestamp_utc"] < pitch
        ]
        if not eligible_runs:
            raise RuntimeError(f"no eligible capture for game {game_pk}")
        for run in eligible_runs:
            snapshot = run["snapshot"]
            markets = [
                row for row in csv.DictReader(
                    (snapshot / "bol_tb15_two_sided_markets.csv").open()
                )
                if int(row["game_pk"]) == game_pk
                and parse_timestamp(row["market_timestamp_utc"]) < pitch
            ]
            for market in markets:
                key = (game_pk, int(market["player_mlb_id"]))
                observed = parse_timestamp(market["market_timestamp_utc"])
                candidate = {
                    "slate_date": slate, "game_pk": game_pk,
                    "player_mlb_id": int(market["player_mlb_id"]),
                    "player": market["player"], "prop_type": "Total Bases", "line": 1.5,
                    "governing_run_tag": run["run_tag"],
                    "market_timestamp_utc": observed.isoformat(),
                    "final_under_odds": int(market["under_odds"]),
                    "final_over_odds": int(market["over_odds"]),
                    "lineup_status": market["lineup_status"],
                    "batting_order": market["batting_order"],
                    "lineup_observed_at_utc": market.get("lineup_observed_at_utc", ""),
                    "lineup_ingestion_run_id": market.get("lineup_ingestion_run_id", ""),
                    "lineup_temporal_classification": market.get(
                        "lineup_temporal_classification", "LINEUP_NOT_RUN_VISIBLE"
                    ),
                }
                if key not in selected or observed > parse_timestamp(
                    selected[key]["market_timestamp_utc"]
                ):
                    selected[key] = candidate
    population = sorted(selected.values(), key=lambda row: (row["game_pk"], row["player_mlb_id"]))
    if not population:
        raise RuntimeError("no actionable final-pregame BetOnline TB 1.5 markets")
    first_slate_capture = min(run["source_capture_timestamp_utc"] for run in runs)
    for row in population:
        game = next(game for game in games if int(game["gamePk"]) == row["game_pk"])
        pitch = parse_timestamp(game["gameDate"])
        eligible_runs = [
            run for run in runs if run["source_capture_timestamp_utc"] < pitch
        ]
        appearances = []
        for run in eligible_runs:
            matching = [
                market for market in csv.DictReader(
                    (run["snapshot"] / "bol_tb15_two_sided_markets.csv").open()
                )
                if (
                int(market["game_pk"]) == row["game_pk"]
                and int(market["player_mlb_id"]) == row["player_mlb_id"]
                and parse_timestamp(market["market_timestamp_utc"]) < pitch
                )
            ]
            appearances.extend(
                (parse_timestamp(market["market_timestamp_utc"]), run["run_tag"])
                for market in matching
            )
        presence = len({tag for _, tag in appearances})
        first_market_at = min(observed for observed, _ in appearances)
        midpoint = first_slate_capture + (pitch - first_slate_capture) / 2
        row["eligible_pregame_captures"] = len(eligible_runs)
        row["captures_containing_market"] = presence
        row["persistence_percentage"] = presence / len(eligible_runs)
        row["persistence_classification"] = (
            "LATE_APPEARING" if first_market_at > midpoint
            else "PERSISTENT" if row["persistence_percentage"] >= 0.5
            else "INTERMITTENT"
        )
        order = int(row["batting_order"]) if row["batting_order"] else None
        row["h1_action"] = (
            "REJECT_TOP_ORDER"
            if row["lineup_temporal_classification"] == "LINEUP_VALID_PREGAME"
            and row["lineup_status"] == "CONFIRMED" and order in (1, 2, 3)
            else "RETAIN_CONFIRMED_NON_TOP_ORDER"
            if row["lineup_temporal_classification"] == "LINEUP_VALID_PREGAME"
            and row["lineup_status"] == "CONFIRMED" and order is not None
            else "ORDER_NOT_CONFIRMED"
        )
        row["h2_action"] = (
            "REJECT_PRICE_BAND" if -199 <= row["final_under_odds"] <= -150
            else "RETAIN_OUTSIDE_PRICE_BAND"
        )
        row["h3_action"] = (
            "REJECT_PERSISTENT" if row["persistence_classification"] == "PERSISTENT"
            else "RETAIN_NON_PERSISTENT"
        )
    return population, runs, games


def freeze(slate: str) -> dict:
    if SIGNAL_RESEARCH_PAUSED:
        raise RuntimeError("SIGNAL_RESEARCH_PAUSED_PENDING_PROSPECTIVE_EVIDENCE_LINEAGE_CERTIFICATION")
    if slate > MULTI_HYPOTHESIS_LAST_ALLOWED_SLATE:
        raise RuntimeError(
            "H2 price-band and H3 persistence hypotheses are closed after "
            "2026-07-30; use the H1-only top-order lifecycle"
        )
    population, runs, games = build_final_population(slate)
    root = EXPORT_ROOT / slate / "under_hypotheses"
    freeze_timestamp = datetime.now(timezone.utc).isoformat()
    definitions = {
        "baseline_all_under.csv": population,
        "rejected_top_order.csv": [r for r in population if r["h1_action"] == "REJECT_TOP_ORDER"],
        "retained_after_top_order.csv": [r for r in population if r["h1_action"] != "REJECT_TOP_ORDER"],
        "rejected_price_band.csv": [r for r in population if r["h2_action"] == "REJECT_PRICE_BAND"],
        "retained_after_price_band.csv": [r for r in population if r["h2_action"] != "REJECT_PRICE_BAND"],
        "rejected_persistent.csv": [r for r in population if r["h3_action"] == "REJECT_PERSISTENT"],
        "retained_after_persistent.csv": [r for r in population if r["h3_action"] != "REJECT_PERSISTENT"],
    }
    overlap = [{
        "game_pk": row["game_pk"], "player_mlb_id": row["player_mlb_id"],
        "player": row["player"],
        "H1_rejected": row["h1_action"] == "REJECT_TOP_ORDER",
        "H2_rejected": row["h2_action"] == "REJECT_PRICE_BAND",
        "H3_rejected": row["h3_action"] == "REJECT_PERSISTENT",
        "H1_action": row["h1_action"],
    } for row in population]
    definitions["hypothesis_overlap_matrix.csv"] = overlap
    population_fields = list(population[0])
    overlap_fields = list(overlap[0])
    manifests = {}
    for name, rows in definitions.items():
        fields = overlap_fields if name == "hypothesis_overlap_matrix.csv" else population_fields
        manifests[name] = {
            "population_name": name.removesuffix(".csv").upper(),
            "rule_version": RULE_VERSION, "slate_date": slate,
            "freeze_timestamp_utc": freeze_timestamp,
            "source_capture_run_tags": [run["run_tag"] for run in runs],
            "identity_count": len(rows), "actionable_count": len(rows),
            "excluded_count": len(population) - len(rows),
            "membership_reason": (
                "OUTCOME_INDEPENDENT_PREDECLARED_RULE_MEMBERSHIP"
            ),
            "sha256": sha_rows(rows, fields),
        }
    package_identity = hashlib.sha256(json.dumps(
        {name: item["sha256"] for name, item in manifests.items()}, sort_keys=True
    ).encode()).hexdigest()
    if root.exists():
        prior = json.loads((root / "under_hypothesis_manifest.json").read_text())
        if prior["package_sha256"] == package_identity:
            print(json.dumps({"freeze_status": "ALREADY_FROZEN_IDENTICAL", **prior}, indent=2))
            return prior
        raise RuntimeError("prospective freeze collision with different content")
    root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=root.parent, prefix=".under_freeze_"))
    try:
        for name, rows in definitions.items():
            fields = overlap_fields if name == "hypothesis_overlap_matrix.csv" else population_fields
            write_csv(staging / name, rows, fields)
        manifest = {
            "rule_version": RULE_VERSION, "slate_date": slate,
            "freeze_timestamp_utc": freeze_timestamp,
            "outcomes_accessed_for_membership": False,
            "baseline_identity_count": len(population),
            "order_not_confirmed_count": sum(
                row["h1_action"] == "ORDER_NOT_CONFIRMED" for row in population
            ),
            "populations": manifests, "package_sha256": package_identity,
        }
        (staging / "under_hypothesis_manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )
        os.replace(staging, root)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    ANALYSIS_ROOT.mkdir(parents=True, exist_ok=True)
    write_csv(ANALYSIS_ROOT / "july30_under_population_freeze_audit.csv", [
        {"population": name, **item} for name, item in manifests.items()
    ])
    write_csv(ANALYSIS_ROOT / "july30_under_hypothesis_overlap.csv", overlap)
    print(json.dumps(manifest, indent=2))
    return manifest


def population_summary(rows: list[dict]) -> dict:
    settled = [row for row in rows if row["settlement_status"] == "SETTLED"]
    wins = sum(row["under_outcome"] == "UNDER_WIN" for row in settled)
    losses = len(settled) - wins
    stake = len(settled) * 5
    net = sum(
        american_profit(5, int(row["final_under_odds"]))
        if row["under_outcome"] == "UNDER_WIN" else -5
        for row in settled
    )
    return {
        "wagers": len(settled), "wins": wins, "losses": losses,
        "no_action": sum(row["settlement_status"] == "VOID" for row in rows),
        "pending": sum(row["settlement_status"] == "PENDING" for row in rows),
        "technical_unresolved": sum(
            row["settlement_status"] == "UNRESOLVED" for row in rows
        ),
        "win_rate": wins / len(settled) if settled else None,
        "average_under_odds": (
            sum(int(row["final_under_odds"]) for row in settled) / len(settled)
            if settled else None
        ),
        "stake": stake, "net_dollars": net, "roi": net / stake if stake else None,
    }


def closeout(slate: str) -> dict:
    if SIGNAL_RESEARCH_PAUSED:
        raise RuntimeError("SIGNAL_RESEARCH_PAUSED_PENDING_PROSPECTIVE_EVIDENCE_LINEAGE_CERTIFICATION")
    root = EXPORT_ROOT / slate / "under_hypotheses"
    if not root.exists():
        raise RuntimeError("prospective populations are not frozen")
    manifest = json.loads((root / "under_hypothesis_manifest.json").read_text())
    baseline = list(csv.DictReader((root / "baseline_all_under.csv").open()))
    runs, failures = load_runs(slate)
    if failures:
        raise RuntimeError("untrusted capture spine")
    games, _ = latest_schedule(slate, runs)
    outcomes, raw_dir = preserve_official_outcomes(slate, games)
    starters = confirmed_starters_before_pitch(runs, games)
    graded = []
    for row in baseline:
        game_pk, player_id = int(row["game_pk"]), int(row["player_mlb_id"])
        result = outcomes.get((game_pk, player_id))
        game_source = outcomes.get((game_pk, None), {})
        if result is None and game_source.get("game_status") == "Final":
            if game_pk in starters and player_id not in starters[game_pk]:
                result = {**game_source, "plate_appearances": 0, "total_bases": 0}
                outcome, settlement = "NO_ACTION", "VOID"
            else:
                result = game_source
                outcome, settlement = "TECHNICAL_UNRESOLVED", "UNRESOLVED"
        elif result is None or result.get("game_status") != "Final":
            result = result or game_source
            outcome, settlement = "PENDING", "PENDING"
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
        })
    by_key = {(row["game_pk"], row["player_mlb_id"]): row for row in graded}
    files = {
        "BASELINE_ALL_UNDER": "baseline_all_under.csv",
        "RETAIN_AFTER_TOP_ORDER_REJECTION": "retained_after_top_order.csv",
        "RETAIN_AFTER_PRICE_BAND_REJECTION": "retained_after_price_band.csv",
        "RETAIN_AFTER_PERSISTENT_REJECTION": "retained_after_persistent.csv",
    }
    summaries = []
    for population, filename in files.items():
        members = list(csv.DictReader((root / filename).open()))
        rows = [by_key[(row["game_pk"], row["player_mlb_id"])] for row in members]
        summaries.append({"population": population, **population_summary(rows)})
    baseline_summary = summaries[0]
    impacts = []
    decisions = {}
    hypotheses = {
        "TOP_ORDER": "rejected_top_order.csv",
        "PRICE_BAND": "rejected_price_band.csv",
        "PERSISTENT": "rejected_persistent.csv",
    }
    retained_by_hypothesis = {row["population"].split("RETAIN_AFTER_")[1].removesuffix("_REJECTION"): row for row in summaries[1:]}
    for hypothesis, filename in hypotheses.items():
        members = list(csv.DictReader((root / filename).open()))
        rows = [by_key[(row["game_pk"], row["player_mlb_id"])] for row in members]
        rejected = population_summary(rows)
        retained = retained_by_hypothesis[hypothesis]
        games_count = len({row["game_pk"] for row in rows})
        by_game = Counter(row["game_pk"] for row in rows)
        largest_share = by_game.most_common(1)[0][1] / len(rows) if rows else 0
        loss_share = rejected["losses"] / baseline_summary["losses"] if baseline_summary["losses"] else 0
        winner_share = rejected["wins"] / baseline_summary["wins"] if baseline_summary["wins"] else 0
        if rejected["pending"] or rejected["technical_unresolved"]:
            decision = "SOURCE_LIFECYCLE_FAILURE_NO_DECISION"
        elif len(rows) < 20 or games_count < 6:
            decision = "INSUFFICIENT_PROSPECTIVE_VOLUME_CLOSE_HYPOTHESIS"
        elif (
            loss_share > winner_share
            and retained["win_rate"] > baseline_summary["win_rate"]
            and retained["roi"] > baseline_summary["roi"]
            and largest_share <= 0.25
        ):
            decision = "REPLICATED_DIRECTIONALLY_ON_JULY30"
        else:
            decision = "FAILED_TO_REPLICATE_CLOSE_HYPOTHESIS"
        decisions[hypothesis] = decision
        impacts.append({
            "hypothesis": hypothesis, "rows_rejected": len(rows),
            "under_losses_removed": rejected["losses"],
            "under_winners_sacrificed": rejected["wins"],
            "loss_share_removed": loss_share, "winner_share_removed": winner_share,
            "removal_advantage": loss_share - winner_share,
            "games_represented": games_count,
            "largest_single_game_share": largest_share,
            "retained_win_rate": retained["win_rate"], "retained_roi": retained["roi"],
            "baseline_win_rate": baseline_summary["win_rate"],
            "baseline_roi": baseline_summary["roi"], "decision": decision,
        })
    status = (
        "FAILED_TECHNICAL_UNRESOLVED" if any(row["technical_unresolved"] for row in summaries)
        else "PREPARED_PENDING_GAME_COMPLETION" if any(row["pending"] for row in summaries)
        else "FINAL"
    )
    content_sha = hashlib.sha256(csv_content(summaries) + csv_content(impacts)).hexdigest()
    closeout_manifest_path = root / "under_closeout_manifest.json"
    prior = json.loads(closeout_manifest_path.read_text()) if closeout_manifest_path.exists() else {}
    if prior.get("content_sha256") == content_sha:
        print(json.dumps({"changed": False, **prior}, indent=2))
        return prior
    revision = int(prior.get("revision", 0)) + 1
    write_csv(root / "under_closeout_rows.csv", graded)
    write_csv(root / "under_closeout_results.csv", summaries)
    write_csv(root / "under_rejection_impact.csv", impacts)
    closeout_manifest = {
        "slate_date": slate, "revision": revision, "status": status,
        "content_sha256": content_sha, "decisions": decisions,
        "official_outcome_raw_directory": str(raw_dir),
        "frozen_package_sha256": manifest["package_sha256"],
    }
    closeout_manifest_path.write_text(json.dumps(closeout_manifest, indent=2) + "\n")
    ANALYSIS_ROOT.mkdir(parents=True, exist_ok=True)
    write_csv(ANALYSIS_ROOT / "july30_under_closeout_results.csv", summaries)
    write_csv(ANALYSIS_ROOT / "july30_under_rejection_impact.csv", impacts)
    h1 = {row["player_mlb_id"] + "|" + row["game_pk"] for row in csv.DictReader((root / "rejected_top_order.csv").open())}
    h2 = {row["player_mlb_id"] + "|" + row["game_pk"] for row in csv.DictReader((root / "rejected_price_band.csv").open())}
    duplication = (
        "TOP_ORDER_AND_PRICE_BAND_OPERATIONALLY_DUPLICATE_TWO_SLATES"
        if h1 == h2 else "TOP_ORDER_AND_PRICE_BAND_DIVERGED_ON_JULY30"
    )
    report = [
        "# July 30 clean-room TB Under 1.5 prospective replication", "",
        f"Closeout status: `{status}`", f"H1/H2: `{duplication}`", "",
        "## Population results", "",
        "| Population | Wagers | W-L | Win rate | Net | ROI |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summaries:
        win_rate = f"{row['win_rate']:.2%}" if row["win_rate"] is not None else "N/A"
        roi = f"{row['roi']:.2%}" if row["roi"] is not None else "N/A"
        report.append(
            f"| {row['population']} | {row['wagers']} | {row['wins']}-{row['losses']} | "
            f"{win_rate} | ${row['net_dollars']:.2f} | {roi} |"
        )
    report.extend(["", "## Decisions", ""])
    for key, value in decisions.items():
        report.append(f"- {key}: `{value}`")
    (ANALYSIS_ROOT / "july30_under_replication_report.md").write_text("\n".join(report) + "\n")
    (ANALYSIS_ROOT / "terminal_decision.md").write_text(
        "\n".join(f"{key}={value}" for key, value in decisions.items()) + "\n"
    )
    print(json.dumps({"status": status, "revision": revision, "summaries": summaries,
                      "impacts": impacts, "decisions": decisions,
                      "H1_H2_duplication": duplication}, indent=2))
    return closeout_manifest


def status(slate: str) -> dict:
    runs, failures = load_runs(slate)
    games = []
    if runs and not failures:
        games, _ = latest_schedule(slate, runs)
    now = datetime.now(timezone.utc)
    games_started = sum(
        parse_timestamp(game["gameDate"]) <= now for game in games
    )
    root = EXPORT_ROOT / slate / "under_hypotheses"
    freeze_manifest = (
        json.loads((root / "under_hypothesis_manifest.json").read_text())
        if (root / "under_hypothesis_manifest.json").exists() else {}
    )
    closeout_manifest = (
        json.loads((root / "under_closeout_manifest.json").read_text())
        if (root / "under_closeout_manifest.json").exists() else {}
    )
    closeout_results = []
    if (root / "under_closeout_results.csv").exists():
        closeout_results = list(
            csv.DictReader((root / "under_closeout_results.csv").open())
        )
    result = {
        "capture_count": len(runs), "untrusted_captures": len(failures),
        "final_game_coverage": {
            "status": "FROZEN" if freeze_manifest else "PENDING_FINAL_CAPTURES",
            "games_started": games_started,
            "official_games": len(games),
        },
        "population_freeze_status": "FROZEN" if freeze_manifest else "NOT_FROZEN",
        "baseline_row_count": freeze_manifest.get("baseline_identity_count", 0),
        "population_counts": {
            key: value["identity_count"]
            for key, value in freeze_manifest.get("populations", {}).items()
        },
        "hypothesis_counts": {
            hypothesis: {
                "rejected": freeze_manifest.get("populations", {}).get(
                    f"rejected_{filename}.csv", {}
                ).get("identity_count", 0),
                "retained": freeze_manifest.get("populations", {}).get(
                    f"retained_after_{filename}.csv", {}
                ).get("identity_count", 0),
            }
            for hypothesis, filename in {
                "H1_TOP_ORDER": "top_order",
                "H2_PRICE_BAND": "price_band",
                "H3_PERSISTENT": "persistent",
            }.items()
        },
        "closeout_revision": closeout_manifest.get("revision", 0),
        "closeout_status": closeout_manifest.get("status", "NOT_PREPARED"),
        "closeout_results": [
            {
                "population": row["population"],
                "wins": int(row["wins"]),
                "losses": int(row["losses"]),
                "pending": int(row["pending"]),
                "no_action": int(row["no_action"]),
                "technical_unresolved": int(row["technical_unresolved"]),
            }
            for row in closeout_results
        ],
        "terminal_decisions": closeout_manifest.get("decisions", {}),
    }
    print(json.dumps(result, indent=2))
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--mode", choices=("overlap", "freeze", "closeout", "status"), required=True)
    args = parser.parse_args()
    if args.mode == "overlap":
        july29_overlap()
    elif args.mode == "freeze":
        freeze(args.date)
    elif args.mode == "closeout":
        closeout(args.date)
    else:
        status(args.date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
