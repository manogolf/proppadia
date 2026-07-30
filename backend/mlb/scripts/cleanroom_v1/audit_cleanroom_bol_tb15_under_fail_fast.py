#!/usr/bin/env python3
"""Read-only, predeclared July 29 clean-room TB Under 1.5 weak-row audit."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from backend.mlb.scripts.cleanroom_v1.closeout_cleanroom_bol_tb15 import (
    EXPORT_ROOT,
    EVIDENCE_ROOT,
    american_profit,
    latest_schedule,
    load_runs,
    parse_timestamp,
)

ROOT = Path(__file__).resolve().parents[4]


def implied_probability(odds: int) -> float:
    return abs(odds) / (abs(odds) + 100) if odds < 0 else 100 / (odds + 100)


def price_band(odds: int) -> str:
    if odds <= -250:
        return "-250_OR_SHORTER"
    if odds <= -200:
        return "-249_TO_-200"
    if odds <= -150:
        return "-199_TO_-150"
    if odds <= -110:
        return "-149_TO_-110"
    if odds <= 100:
        return "-109_TO_+100"
    return "ABOVE_+100"


def order_band(value: str) -> str:
    if not value:
        return "UNAVAILABLE"
    order = int(value)
    return "POSITIONS_1_TO_3" if order <= 3 else "POSITIONS_4_TO_6" if order <= 6 else "POSITIONS_7_TO_9"


def proximity_band(minutes: float) -> str:
    if minutes <= 15:
        return "0_TO_15_MINUTES"
    if minutes <= 30:
        return "16_TO_30_MINUTES"
    if minutes <= 60:
        return "31_TO_60_MINUTES"
    return "MORE_THAN_60_MINUTES"


def under_profit(row: dict) -> float:
    return (
        american_profit(5, int(row["final_under_odds"]))
        if row["under_outcome"] == "UNDER_WIN" else -5
    )


def summarize(rows: list[dict]) -> dict:
    wins = sum(row["under_outcome"] == "UNDER_WIN" for row in rows)
    losses = len(rows) - wins
    stake = len(rows) * 5
    net = sum(under_profit(row) for row in rows)
    return {
        "rows": len(rows), "games_represented": len({row["game_pk"] for row in rows}),
        "under_wins": wins, "under_losses": losses,
        "under_win_rate": wins / len(rows) if rows else None,
        "average_final_under_odds": (
            sum(int(row["final_under_odds"]) for row in rows) / len(rows) if rows else None
        ),
        "total_stake": stake, "net_dollars": net,
        "roi": net / stake if stake else None,
    }


def build_canonical(slate: str) -> tuple[list[dict], list[dict], dict]:
    out = EXPORT_ROOT / slate
    population = list(csv.DictReader(
        (out / f"bol_tb15_final_pregame_actionable_{slate}.csv").open()
    ))
    closeout = {
        (row["game_pk"], row["player_mlb_id"]): row
        for row in csv.DictReader(
            (out / f"bol_tb15_cleanroom_closeout_{slate}.csv").open()
        )
    }
    runs, failures = load_runs(slate)
    if failures:
        raise RuntimeError("untrusted capture present")
    games, _ = latest_schedule(slate, runs)
    game_by_pk = {int(game["gamePk"]): game for game in games}
    first_slate_capture = min(run["source_capture_timestamp_utc"] for run in runs)
    canonical = []
    for selected in population:
        settled = closeout[(selected["game_pk"], selected["player_mlb_id"])]
        if settled["settlement_status"] != "SETTLED":
            continue
        game_pk = int(selected["game_pk"])
        player_id = int(selected["player_mlb_id"])
        pitch = parse_timestamp(game_by_pk[game_pk]["gameDate"])
        eligible_runs = [
            run for run in runs if run["source_capture_timestamp_utc"] < pitch
        ]
        appearances = []
        for run in eligible_runs:
            rows = [
                row for row in csv.DictReader(
                    (run["snapshot"] / "bol_tb15_two_sided_markets.csv").open()
                )
                if int(row["game_pk"]) == game_pk
                and int(row["player_mlb_id"]) == player_id
                and parse_timestamp(row["market_timestamp_utc"]) < pitch
            ]
            for row in rows:
                appearances.append((parse_timestamp(row["market_timestamp_utc"]), run["run_tag"], row))
        appearances.sort(key=lambda item: (item[0], item[1]))
        if not appearances:
            raise RuntimeError(f"settled identity lacks pregame market: {game_pk}/{player_id}")
        first_at, first_tag, first = appearances[0]
        final_at = parse_timestamp(selected["market_timestamp_utc"])
        first_under = int(first["under_odds"])
        final_under = int(selected["under_odds"])
        first_prob = implied_probability(first_under)
        final_prob = implied_probability(final_under)
        movement = final_prob - first_prob
        midpoint = first_slate_capture + (pitch - first_slate_capture) / 2
        capture_count = len({tag for _, tag, _ in appearances})
        persistence = (
            "LATE_APPEARING" if first_at > midpoint
            else "PERSISTENT" if capture_count / len(eligible_runs) >= 0.5
            else "INTERMITTENT"
        )
        minutes = (pitch - final_at).total_seconds() / 60
        canonical.append({
            "slate_date": slate, "player": selected["player"],
            "game": settled["game"], "team": selected["team"],
            "opponent": selected["opponent"], "game_pk": game_pk,
            "player_mlb_id": player_id,
            "governing_run_tag": selected["governing_run_tag"],
            "first_valid_market_run_tag": first_tag,
            "first_market_timestamp_utc": first_at.isoformat(),
            "final_pregame_market_timestamp_utc": final_at.isoformat(),
            "captures_containing_market": capture_count,
            "eligible_pregame_captures": len(eligible_runs),
            "eligible_capture_presence_percentage": capture_count / len(eligible_runs),
            "first_under_odds": first_under, "final_under_odds": final_under,
            "first_under_implied_probability": first_prob,
            "final_under_implied_probability": final_prob,
            "under_implied_probability_movement": movement,
            "first_over_odds": int(first["over_odds"]),
            "final_over_odds": int(selected["over_odds"]),
            "lineup_status": selected["lineup_status"],
            "batting_order": selected["batting_order"],
            "minutes_final_capture_before_first_pitch": minutes,
            "plate_appearances": int(settled["plate_appearances"]),
            "at_bats": int(settled["at_bats"]), "hits": int(settled["hits"]),
            "singles": int(settled["singles"]), "doubles": int(settled["doubles"]),
            "triples": int(settled["triples"]), "home_runs": int(settled["home_runs"]),
            "total_bases": int(settled["total_bases"]),
            "under_outcome": "UNDER_WIN" if settled["outcome"] == "OVER_LOSS" else "UNDER_LOSS",
            "h1_batting_order": order_band(selected["batting_order"]),
            "h2_final_under_price": price_band(final_under),
            "h3_market_movement": (
                "MOVED_TOWARD_UNDER" if movement > 0
                else "MOVED_AGAINST_UNDER" if movement < 0 else "UNCHANGED"
            ),
            "h4_market_persistence": persistence,
            "h5_final_capture_proximity": proximity_band(minutes),
        })
    return canonical, runs, game_by_pk


def loss_mechanism(row: dict) -> str:
    if row["hits"] == 1 and row["doubles"] == 1 and row["total_bases"] == 2:
        return "ONE_DOUBLE_ONLY"
    if row["hits"] == 1 and row["triples"] == 1:
        return "ONE_TRIPLE_ONLY"
    if row["hits"] == 1 and row["home_runs"] == 1:
        return "ONE_HOME_RUN_ONLY"
    if row["hits"] >= 2 and row["singles"] >= 2 and not (
        row["doubles"] or row["triples"] or row["home_runs"]
    ):
        return "TWO_OR_MORE_SINGLES"
    if row["hits"] >= 2 and (row["doubles"] or row["triples"] or row["home_runs"]):
        return "MULTIPLE_HITS_WITH_EXTRA_BASE_PRODUCTION"
    return "OTHER_EXACT_MECHANISM"


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    canonical, runs, games = build_canonical(args.date)
    baseline = summarize(canonical)
    expected = json.loads((EXPORT_ROOT / args.date / "closeout_manifest.json").read_text())[
        "under_baseline"
    ]
    checks = {
        "wagers_match": baseline["rows"] == expected["wagers"] == 218,
        "wins_match": baseline["under_wins"] == expected["wins"] == 139,
        "losses_match": baseline["under_losses"] == expected["losses"] == 79,
        "stake_match": baseline["total_stake"] == expected["total_stake"] == 1090,
        "net_match": math.isclose(baseline["net_dollars"], expected["net_dollars"], abs_tol=1e-9),
        "roi_match": math.isclose(baseline["roi"], expected["roi"], abs_tol=1e-12),
    }
    if not all(checks.values()):
        raise SystemExit(f"baseline reproduction failed: {checks} {baseline} {expected}")

    hypothesis_fields = {
        "H1_BATTING_ORDER": "h1_batting_order",
        "H2_FINAL_UNDER_PRICE": "h2_final_under_price",
        "H3_MARKET_MOVEMENT": "h3_market_movement",
        "H4_MARKET_PERSISTENCE": "h4_market_persistence",
        "H5_CAPTURE_PROXIMITY": "h5_final_capture_proximity",
    }
    hypothesis_levels = {
        "H1_BATTING_ORDER": [
            "POSITIONS_1_TO_3", "POSITIONS_4_TO_6", "POSITIONS_7_TO_9",
            "UNAVAILABLE",
        ],
        "H2_FINAL_UNDER_PRICE": [
            "-250_OR_SHORTER", "-249_TO_-200", "-199_TO_-150",
            "-149_TO_-110", "-109_TO_+100", "ABOVE_+100",
        ],
        "H3_MARKET_MOVEMENT": [
            "MOVED_TOWARD_UNDER", "MOVED_AGAINST_UNDER", "UNCHANGED",
        ],
        "H4_MARKET_PERSISTENCE": [
            "PERSISTENT", "INTERMITTENT", "LATE_APPEARING",
        ],
        "H5_CAPTURE_PROXIMITY": [
            "0_TO_15_MINUTES", "16_TO_30_MINUTES", "31_TO_60_MINUTES",
            "MORE_THAN_60_MINUTES",
        ],
    }
    grouping_rows = []
    impact_rows = []
    concentration_rows = []
    passing = []
    total_wins, total_losses = baseline["under_wins"], baseline["under_losses"]
    for hypothesis, field in hypothesis_fields.items():
        levels = hypothesis_levels[hypothesis]
        for level in levels:
            group = [row for row in canonical if row[field] == level]
            metrics = summarize(group)
            loss_share = metrics["under_losses"] / total_losses
            winner_share = metrics["under_wins"] / total_wins
            grouping_rows.append({
                "hypothesis": hypothesis, "group": level, **metrics,
                "share_of_all_79_losses": loss_share,
                "share_of_all_139_winners": winner_share,
            })
            retained = [row for row in canonical if row[field] != level]
            retained_metrics = summarize(retained)
            by_game = Counter(row["game_pk"] for row in group)
            largest_game, largest_rows = by_game.most_common(1)[0] if by_game else ("", 0)
            removal_gap = loss_share - winner_share
            passes = (
                len(group) >= 20 and metrics["games_represented"] >= 6
                and removal_gap >= 0.05
                and retained_metrics["under_win_rate"] - baseline["under_win_rate"] >= 0.02
                and retained_metrics["roi"] - baseline["roi"] >= 0.01
                and largest_rows / len(group) <= 0.25
                and level != "UNAVAILABLE"
            )
            classification = (
                "CLEAR_ONE_SLATE_REJECTION_CANDIDATE" if passes
                else "INSUFFICIENT_ROWS" if len(group) < 20 or metrics["games_represented"] < 6
                else "WEAK_OR_MIXED" if removal_gap > 0 or retained_metrics["roi"] > baseline["roi"]
                else "NOT_USEFUL"
            )
            if passes:
                passing.append((hypothesis, level))
            impact_rows.append({
                "hypothesis": hypothesis, "group": level,
                "rows_rejected": len(group),
                "under_losses_removed": metrics["under_losses"],
                "under_winners_sacrificed": metrics["under_wins"],
                "rejected_row_loss_rate": (
                    metrics["under_losses"] / len(group) if group else None
                ),
                "percentage_all_losses_removed": loss_share,
                "percentage_all_winners_removed": winner_share,
                "loss_removal_minus_winner_removal": removal_gap,
                "rows_retained": retained_metrics["rows"],
                "retained_wins": retained_metrics["under_wins"],
                "retained_losses": retained_metrics["under_losses"],
                "retained_win_rate": retained_metrics["under_win_rate"],
                "retained_net_dollars": retained_metrics["net_dollars"],
                "retained_roi": retained_metrics["roi"],
                "roi_change": retained_metrics["roi"] - baseline["roi"],
                "win_rate_change": retained_metrics["under_win_rate"] - baseline["under_win_rate"],
                "largest_game_pk": largest_game,
                "largest_single_game_share": largest_rows / len(group) if group else 0,
                "classification": classification,
            })
            for game_pk, count in sorted(by_game.items()):
                game_rows = [row for row in group if row["game_pk"] == game_pk]
                without = [row for row in group if row["game_pk"] != game_pk]
                without_metrics = summarize(without)
                concentration_rows.append({
                    "hypothesis": hypothesis, "group": level, "game_pk": game_pk,
                    "game": game_rows[0]["game"], "rows": count,
                    "wins": sum(row["under_outcome"] == "UNDER_WIN" for row in game_rows),
                    "losses": sum(row["under_outcome"] == "UNDER_LOSS" for row in game_rows),
                    "share_of_group": count / len(group),
                    "group_wins_without_game": without_metrics["under_wins"],
                    "group_losses_without_game": without_metrics["under_losses"],
                    "group_win_rate_without_game": without_metrics["under_win_rate"],
                    "group_roi_without_game": without_metrics["roi"],
                })

    mechanism_counter = Counter(
        loss_mechanism(row) for row in canonical if row["under_outcome"] == "UNDER_LOSS"
    )
    mechanism_rows = [
        {
            "mechanism": mechanism, "under_losses": count,
            "share_of_79_losses": count / 79,
            "broad_cause": (
                "ONE_EVENT_EXTRA_BASE_POWER" if mechanism in {
                    "ONE_DOUBLE_ONLY", "ONE_TRIPLE_ONLY", "ONE_HOME_RUN_ONLY"
                } else "MULTIPLE_HIT_VOLUME" if mechanism == "TWO_OR_MORE_SINGLES"
                else "MIXED_PRODUCTION"
            ),
        }
        for mechanism, count in sorted(mechanism_counter.items())
    ]
    broad = Counter()
    for row in mechanism_rows:
        broad[row["broad_cause"]] += row["under_losses"]
    movement = [row["under_implied_probability_movement"] for row in canonical]
    movement_sorted = sorted(movement)
    movement_summary = {
        "minimum": min(movement), "q25": movement_sorted[len(movement)//4],
        "median": statistics.median(movement),
        "mean": statistics.mean(movement),
        "q75": movement_sorted[(3*len(movement))//4], "maximum": max(movement),
    }
    decision = (
        "NO_SIMPLE_UNDER_REJECTION_CONDITION_FOUND_CLOSE_BRANCH" if not passing
        else "ONE_SIMPLE_UNDER_REJECTION_CANDIDATE_EARNS_NEXT_SLATE_TEST" if len(passing) == 1
        else "MULTIPLE_CANDIDATES_MEET_STANDARD_REPORT_ALL_NO_SELECTION"
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(args.output_dir / "july29_under_canonical_218.csv", list(canonical[0]), canonical)
    write_csv(args.output_dir / "july29_under_baseline_reproduction.csv",
              list({**baseline, **checks}.keys()), [{**baseline, **checks}])
    write_csv(args.output_dir / "july29_under_natural_groupings.csv",
              list(grouping_rows[0]), grouping_rows)
    write_csv(args.output_dir / "july29_under_rejection_impact.csv",
              list(impact_rows[0]), impact_rows)
    write_csv(args.output_dir / "july29_under_loss_mechanisms.csv",
              list(mechanism_rows[0]), mechanism_rows)
    write_csv(args.output_dir / "july29_under_game_concentration.csv",
              list(concentration_rows[0]), concentration_rows)

    hypothesis_decisions = {}
    for hypothesis in hypothesis_fields:
        relevant = [row for row in impact_rows if row["hypothesis"] == hypothesis]
        hypothesis_decisions[hypothesis] = (
            "CLEAR_ONE_SLATE_REJECTION_CANDIDATE" if any(
                row["classification"] == "CLEAR_ONE_SLATE_REJECTION_CANDIDATE" for row in relevant
            ) else "WEAK_OR_MIXED" if any(
                row["classification"] == "WEAK_OR_MIXED" for row in relevant
            ) else "INSUFFICIENT_ROWS" if all(
                row["classification"] == "INSUFFICIENT_ROWS" for row in relevant
            ) else "NOT_USEFUL"
        )
    report = [
        "# July 29 clean-room BetOnline TB Under 1.5 fail-fast audit", "",
        "## Baseline reproduction", "",
        f"- Wagers: {baseline['rows']}",
        f"- Record: {baseline['under_wins']}-{baseline['under_losses']}",
        f"- Win rate: {baseline['under_win_rate']:.4%}",
        f"- Net: ${baseline['net_dollars']:.2f}",
        f"- ROI: {baseline['roi']:.4%}", "",
        "## Frozen hypothesis decisions", "",
    ]
    for key, value in hypothesis_decisions.items():
        report.append(f"- {key}: `{value}`")
    report.extend([
        "", "## Passing predeclared groups", "",
        "| Hypothesis | Group | Rejected | Losses removed | Winners sacrificed | Removal gap | Retained record | Retained win rate | Retained ROI | ROI change | Largest-game share |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for hypothesis, level in passing:
        row = next(
            item for item in impact_rows
            if item["hypothesis"] == hypothesis and item["group"] == level
        )
        report.append(
            f"| {hypothesis} | {level} | {row['rows_rejected']} | "
            f"{row['under_losses_removed']} | {row['under_winners_sacrificed']} | "
            f"{row['loss_removal_minus_winner_removal']:.2%} | "
            f"{row['retained_wins']}-{row['retained_losses']} | "
            f"{row['retained_win_rate']:.2%} | {row['retained_roi']:.2%} | "
            f"{row['roi_change']:+.2%} | {row['largest_single_game_share']:.2%} |"
        )
    if not passing:
        report.append("| None | — | — | — | — | — | — | — | — | — | — |")
    report.extend([
        "", "## All natural groupings", "",
        "| Hypothesis | Group | Rows | Games | W-L | Win rate | Net | ROI | Loss share | Winner share |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in grouping_rows:
        win_rate = f"{row['under_win_rate']:.2%}" if row["under_win_rate"] is not None else "—"
        roi = f"{row['roi']:.2%}" if row["roi"] is not None else "—"
        report.append(
            f"| {row['hypothesis']} | {row['group']} | {row['rows']} | "
            f"{row['games_represented']} | {row['under_wins']}-{row['under_losses']} | "
            f"{win_rate} | ${row['net_dollars']:.2f} | "
            f"{roi} | {row['share_of_all_79_losses']:.2%} | "
            f"{row['share_of_all_139_winners']:.2%} |"
        )
    report.extend([
        "", "## Continuous implied-probability movement", "",
        f"```json\n{json.dumps(movement_summary, indent=2)}\n```", "",
        "## Under-loss mechanisms", "",
    ])
    for row in mechanism_rows:
        report.append(
            f"- {row['mechanism']}: {row['under_losses']} "
            f"({row['share_of_79_losses']:.2%})"
        )
    report.extend(["", "Broad causes:"])
    for key, value in broad.most_common():
        report.append(f"- {key}: {value} ({value/79:.2%})")
    report.extend(["", "## Terminal decision", "", f"`{decision}`", ""])
    (args.output_dir / "july29_under_fail_fast_report.md").write_text("\n".join(report))
    (args.output_dir / "terminal_decision.md").write_text(
        f"{decision}\n\nPassing groups: {passing if passing else 'None'}\n"
    )
    print(json.dumps({
        "baseline": baseline, "checks": checks, "movement": movement_summary,
        "loss_mechanisms": mechanism_counter, "broad_causes": broad,
        "hypothesis_decisions": hypothesis_decisions,
        "passing_groups": passing, "terminal_decision": decision,
    }, indent=2, default=dict))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
