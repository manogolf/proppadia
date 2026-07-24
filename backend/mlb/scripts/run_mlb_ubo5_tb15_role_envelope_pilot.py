#!/usr/bin/env python3
"""Research-only plausible role and batting-order envelope pilot for UBO-5 TB1.5."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.mlb.scripts.build_mlb_ubo5_tb15_prelineup_confirmation_board import classify
from backend.mlb.scripts.build_mlb_ubo5_tb15_provisional_tracker import market_rows
from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import implied
from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import FEATURES

ROOT = Path(__file__).resolve().parents[3]
METHOD = "PREVIOUS_10_UNION_SEASON_95_PERCENT"
ROLE_LABELS = {
    "ESTABLISHED_PRIMARY_STARTER": "Primary starter",
    "ROTATION_OR_PLATOON_STARTER": "Rotation/platoon",
    "POSITION_COMPETITION": "Position competition",
    "UTILITY_OR_DH_CANDIDATE": "Utility/DH candidate",
    "LIKELY_BENCH": "Likely bench",
    "ROLE_UNRESOLVED": "Role unresolved",
}


def read_normalized(root: Path, table: str) -> pd.DataFrame:
    parts = sorted((root / table).glob("season=*/*.parquet"))
    return pd.concat([pd.read_parquet(path) for path in parts], ignore_index=True, sort=False)


def plausible_slots(history: list[int]) -> list[int]:
    if not history:
        return list(range(1, 10))
    result = set(history[-10:])
    counts = Counter(history)
    cumulative = 0
    total = len(history)
    for position, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        result.add(int(position))
        cumulative += count
        if cumulative / total >= 0.95:
            break
    return sorted(result)


def role_context(
    lineups: pd.DataFrame, outcomes: pd.DataFrame, games: pd.DataFrame,
    target_date: pd.Timestamp, team: str, player_id: int,
) -> dict:
    team_games = games[
        (games.game_date < target_date)
        & ((games.home_team == team) | (games.away_team == team))
    ].sort_values(["game_date", "game_pk"]).tail(10)
    game_ids = set(pd.to_numeric(team_games.game_pk, errors="coerce").dropna().astype(int))
    recent = lineups[lineups.game_pk.isin(game_ids) & lineups.team.eq(team)].copy()
    player_recent = recent[recent.player_id.eq(player_id)].sort_values(["game_date", "game_pk"])
    season = lineups[
        (lineups.game_date.dt.year == target_date.year)
        & (lineups.game_date < target_date)
        & lineups.player_id.eq(player_id)
    ].sort_values(["game_date", "game_pk"])
    positions = [
        str(value).upper() for value in player_recent.defensive_position.dropna()
        if str(value).upper() not in {"", "NAN", "NONE"}
    ]
    position_counts = Counter(positions)
    primary = position_counts.most_common(1)[0][0] if position_counts else ""
    secondary = sorted(position for position in position_counts if position != primary)
    competitors = set()
    for position in position_counts:
        counts = recent.loc[
            recent.defensive_position.astype(str).str.upper().eq(position)
            & ~recent.player_id.eq(player_id), "player_id"
        ].value_counts()
        competitors.update(int(pid) for pid, count in counts.items() if count >= 3)
    appearances = outcomes[
        outcomes.game_pk.isin(game_ids) & outcomes.player_id.eq(player_id)
    ]
    start_games = set(player_recent.game_pk.astype(int))
    bench_appearances = sum(int(game) not in start_games for game in appearances.game_pk)
    starts = len(player_recent)
    recent_team_games = len(team_games)
    dh_starts = sum(position == "DH" for position in positions)
    distinct_positions = len(set(positions))
    if recent_team_games < 5 or (starts == 0 and len(season) == 0):
        role = "ROLE_UNRESOLVED"
    elif starts >= 8:
        role = "ESTABLISHED_PRIMARY_STARTER"
    elif competitors:
        role = "POSITION_COMPETITION"
    elif starts >= 4 and (dh_starts > 0 or distinct_positions >= 2):
        role = "UTILITY_OR_DH_CANDIDATE"
    elif starts >= 4:
        role = "ROTATION_OR_PLATOON_STARTER"
    elif starts <= 2:
        role = "LIKELY_BENCH"
    else:
        role = "ROTATION_OR_PLATOON_STARTER"
    order_history = season.batting_order_position.dropna().astype(int).tolist()
    slots = plausible_slots(order_history)
    return {
        "role_class": role, "recent_team_games": recent_team_games, "recent_starts": starts,
        "recent_start_frequency": starts / recent_team_games if recent_team_games else np.nan,
        "primary_position": primary, "secondary_positions": "|".join(secondary),
        "dh_starts_recent10": dh_starts, "bench_appearances_recent10": int(bench_appearances),
        "competitor_player_ids": "|".join(map(str, sorted(competitors))),
        "season_prior_starts": len(season), "plausible_positions": "|".join(map(str, slots)),
        "plausible_position_count": len(slots), "full_1_9_fallback": slots == list(range(1, 10)),
        "_slots": slots,
    }


def load_authentic_markets(odds_root: Path) -> pd.DataFrame:
    rows = []
    for day in pd.date_range("2026-07-16", "2026-07-22"):
        folder = odds_root / day.strftime("%Y-%m-%d")
        odds_files = sorted(folder.glob("odds_mlb_playerprops__local_daily_*.json"))
        if not odds_files:
            continue
        odds = odds_files[0]
        tag = odds.stem.split("__", 1)[1]
        wide = folder / f"mlb_predictions_wide_calibrated__{tag}.csv"
        if not wide.is_file():
            continue
        matched, _ = market_rows(json.loads(odds.read_text()), pd.read_csv(wide))
        for row in matched:
            oi, ui = implied(row["over_price"]), implied(row["under_price"])
            rows.append({
                "slate_date": day.strftime("%Y-%m-%d"), "game_pk": int(row["game_id"]),
                "batter_mlb_id": int(row["player_id"]), "team": row["team"],
                "player_name": row["player_name"], "game": row["game"],
                "no_vig_over_probability": oi / (oi + ui),
            })
    return pd.DataFrame(rows).drop_duplicates(["game_pk", "batter_mlb_id"])


def historical_validation(
    features: pd.DataFrame, lineups: pd.DataFrame, outcomes: pd.DataFrame, games: pd.DataFrame,
    markets: pd.DataFrame, model,
) -> tuple[pd.DataFrame, dict]:
    population = markets.merge(
        features, on=["game_pk", "batter_mlb_id"], how="left", suffixes=("_market", "")
    )
    rows = []
    classes = list(model.classes_)
    actual_lineup = {
        (int(row.game_pk), int(row.player_id)): int(row.batting_order_position)
        for row in lineups.itertuples()
    }
    for source in population.to_dict("records"):
        target_date = pd.Timestamp(source["slate_date"])
        player_id = int(source["batter_mlb_id"])
        context = role_context(lineups, outcomes, games, target_date, source["team_market"], player_id)
        actual_position = actual_lineup.get((int(source["game_pk"]), player_id))
        actual_starter = actual_position is not None
        probabilities = []
        feature_ready = pd.notna(source.get("history_depth_pa"))
        if feature_ready:
            base = pd.Series(source)
            for slot in range(1, 10):
                vector = base.copy()
                vector["batting_order_position"] = slot
                raw = model.predict_proba(pd.DataFrame([vector[FEATURES]]))[0]
                probabilities.append(float(1 - dict(zip(classes, raw)).get(0, 0) - dict(zip(classes, raw)).get(1, 0)))
        slots = context.pop("_slots")
        full_class = plausible_class = ""
        exact_positive = None
        if probabilities:
            full_class = classify(probabilities, source["no_vig_over_probability"])
            selected = [probabilities[slot - 1] for slot in slots]
            plausible_class = classify(selected, source["no_vig_over_probability"])
            exact_positive = probabilities[actual_position - 1] > source["no_vig_over_probability"]
        role = context["role_class"]
        decision = (
            "WAIT_FOR_ROLE" if role in {"LIKELY_BENCH", "ROLE_UNRESOLVED"} else
            {
                "ROBUST_CONFIRM": "CONFIRM_IF_STARTING", "ROBUST_PASS": "PASS_IF_STARTING",
                "ORDER_SENSITIVE_WAIT": "WAIT_FOR_ORDER",
            }.get(plausible_class, "WAIT_FOR_ROLE")
        )
        rows.append({
            **{key: source.get(key) for key in ["slate_date", "game_pk", "batter_mlb_id", "player_name", "game"]},
            **context, "actual_starter": actual_starter, "actual_batting_position": actual_position,
            "actual_position_covered": actual_position in slots if actual_position else False,
            "full_1_9_classification": full_class, "plausible_classification": plausible_class,
            "pilot_decision": decision, "exact_positive_edge": exact_positive,
            "false_robust_confirm": decision == "CONFIRM_IF_STARTING" and exact_positive is False,
            "false_robust_pass": decision == "PASS_IF_STARTING" and exact_positive is True,
        })
    frame = pd.DataFrame(rows)
    starters = frame[frame.actual_starter]
    full_wait = starters.full_1_9_classification.eq("ORDER_SENSITIVE_WAIT").sum()
    pilot_wait = starters.pilot_decision.isin(["WAIT_FOR_ORDER", "WAIT_FOR_ROLE"]).sum()
    role_rates = frame.groupby("role_class").actual_starter.agg(["size", "mean"]).reset_index().to_dict("records")
    summary = {
        "market_rows": len(frame), "starter_classification_coverage": float(starters.pilot_decision.ne("").mean()),
        "eventual_starter_rate_by_role_class": role_rates,
        "eventual_nonstarter_rate": float((~frame.actual_starter).mean()),
        "actual_position_coverage": float(starters.actual_position_covered.mean()),
        "average_plausible_positions": float(frame.plausible_position_count.mean()),
        "median_plausible_positions": float(frame.plausible_position_count.median()),
        "full_1_9_fallback_rate": float(frame.full_1_9_fallback.mean()),
        "full_1_9_wait_rows": int(full_wait), "pilot_wait_rows": int(pilot_wait),
        "wait_reduction_rows": int(full_wait - pilot_wait),
        "wait_reduction_rate": float((full_wait - pilot_wait) / full_wait) if full_wait else 0,
        "false_robust_confirm_count": int(frame.false_robust_confirm.sum()),
        "false_robust_pass_count": int(frame.false_robust_pass.sum()),
    }
    robust = frame[frame.pilot_decision.isin(["CONFIRM_IF_STARTING", "PASS_IF_STARTING"])]
    summary["robust_decision_rows"] = int(len(robust))
    summary["confirm_pass_exact_order_agreement"] = float(
        (~robust.false_robust_confirm & ~robust.false_robust_pass).mean()
    ) if len(robust) else np.nan
    return frame, summary


def july24(
    audit: pd.DataFrame, lineups: pd.DataFrame, outcomes: pd.DataFrame, games: pd.DataFrame,
    transitions: pd.DataFrame, market_teams: dict[tuple[int, int], str],
) -> tuple[pd.DataFrame, dict]:
    rows = []
    for source in audit.to_dict("records"):
        context = role_context(
            lineups, outcomes, games, pd.Timestamp("2026-07-24"),
            market_teams.get((int(source["game_pk"]), int(source["batter_mlb_id"])), ""),
            int(source["batter_mlb_id"]),
        )
        slots = context.pop("_slots")
        probabilities = [source.get(f"ubo5_probability_batting_{slot}") for slot in range(1, 10)]
        valid = all(pd.notna(value) for value in probabilities)
        if valid:
            selected = [float(probabilities[slot - 1]) for slot in slots]
            provisional = classify(selected, float(source["no_vig_over_probability"]))
        else:
            provisional = ""
        role = context["role_class"]
        decision = (
            "WAIT_FOR_ROLE" if role in {"LIKELY_BENCH", "ROLE_UNRESOLVED"} else
            {"ROBUST_CONFIRM": "CONFIRM_IF_STARTING", "ROBUST_PASS": "PASS_IF_STARTING",
             "ORDER_SENSITIVE_WAIT": "WAIT_FOR_ORDER"}.get(provisional, "WAIT_FOR_ROLE")
        )
        transition = transitions[
            transitions.batter_mlb_id.eq(int(source["batter_mlb_id"]))
            & transitions.game_pk.eq(int(source["game_pk"]))
        ]
        transition_row = transition.iloc[0] if len(transition) else {}
        rows.append({
            **source, **context, "plausible_classification": provisional, "pilot_decision": decision,
            "active_roster_match": False, "active_roster_reason": "NO_RUN_TAGGED_DATED_ROSTER_SNAPSHOT",
            "later_transition_outcome": transition_row.get("transition_outcome", ""),
            "actual_batting_position": transition_row.get("confirmed_batting_order", ""),
            "actual_position_covered": (
                int(transition_row.get("confirmed_batting_order")) in slots
                if pd.notna(transition_row.get("confirmed_batting_order", np.nan)) else ""
            ),
        })
    frame = pd.DataFrame(rows)
    counts = frame.role_class.value_counts()
    decisions = frame.pilot_decision.value_counts()
    summary = {
        "market_rows": len(frame), "active_roster_matches": int(frame.active_roster_match.sum()),
        "established_primary_starters": int(counts.get("ESTABLISHED_PRIMARY_STARTER", 0)),
        "rotation_platoon_players": int(counts.get("ROTATION_OR_PLATOON_STARTER", 0) + counts.get("UTILITY_OR_DH_CANDIDATE", 0)),
        "position_competition_players": int(counts.get("POSITION_COMPETITION", 0)),
        "likely_bench_players": int(counts.get("LIKELY_BENCH", 0)),
        "role_unresolved_players": int(counts.get("ROLE_UNRESOLVED", 0)),
        "average_plausible_positions": float(frame.plausible_position_count.mean()),
        "full_1_9_fallbacks": int(frame.full_1_9_fallback.sum()),
        "CONFIRM_IF_STARTING": int(decisions.get("CONFIRM_IF_STARTING", 0)),
        "PASS_IF_STARTING": int(decisions.get("PASS_IF_STARTING", 0)),
        "WAIT_FOR_ORDER": int(decisions.get("WAIT_FOR_ORDER", 0)),
        "WAIT_FOR_ROLE": int(decisions.get("WAIT_FOR_ROLE", 0)),
    }
    return frame, summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--normalized-root", required=True, type=Path)
    ap.add_argument("--feature-parquet", required=True, type=Path)
    ap.add_argument("--artifact", required=True, type=Path)
    ap.add_argument("--odds-root", required=True, type=Path)
    ap.add_argument("--july24-audit", required=True, type=Path)
    ap.add_argument("--july24-transitions", required=True, type=Path)
    ap.add_argument("--july24-wide", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    games = read_normalized(args.normalized_root, "games")
    games["game_date"] = pd.to_datetime(games.game_date)
    lineups = read_normalized(args.normalized_root, "starting_lineups")
    lineups = lineups.merge(
        games[["game_pk", "game_date", "home_team", "away_team"]], on="game_pk", how="left"
    )
    missing_team = lineups.team.isna() | lineups.team.astype(str).str.strip().eq("")
    lineups.loc[missing_team & lineups.home_away.astype(str).isin(["home", "h"]), "team"] = lineups["home_team"]
    lineups.loc[missing_team & lineups.home_away.astype(str).isin(["away", "v"]), "team"] = lineups["away_team"]
    lineups["player_id"] = pd.to_numeric(lineups.player_id, errors="coerce")
    lineups = lineups.dropna(subset=["player_id", "game_date", "batting_order_position"]).copy()
    lineups["player_id"] = lineups.player_id.astype(int)
    outcomes = read_normalized(args.normalized_root, "player_game_outcomes")
    outcomes["player_id"] = pd.to_numeric(outcomes.player_id, errors="coerce")
    outcomes = outcomes.dropna(subset=["player_id"]).copy()
    outcomes["player_id"] = outcomes.player_id.astype(int)
    features = pd.read_parquet(args.feature_parquet)
    features = features[pd.to_numeric(features.history_depth_pa, errors="coerce").ge(100)].copy()
    model = joblib.load(args.artifact)["model"]
    markets = load_authentic_markets(args.odds_root)
    historical, historical_summary = historical_validation(features, lineups, outcomes, games, markets, model)
    historical.to_csv(args.output_dir / "historical_role_envelope_validation.csv", index=False)
    audit = pd.read_csv(args.july24_audit)
    transition = pd.read_csv(args.july24_transitions)
    july_wide = pd.read_csv(args.july24_wide)
    market_teams = {
        (int(row.game_id), int(row.player_id)): str(row.team)
        for row in july_wide[july_wide.prop_type.eq("total_bases")].itertuples()
    }
    july, july_summary = july24(audit, lineups, outcomes, games, transition, market_teams)
    july.to_csv(args.output_dir / "july24_role_envelope_reconstruction.csv", index=False)
    activation = (
        historical_summary["actual_position_coverage"] >= 0.975
        and historical_summary["wait_reduction_rows"] > 0
        and historical_summary["false_robust_confirm_count"] == 0
        and historical_summary["false_robust_pass_count"] == 0
    )
    result = {
        "method": METHOD, "historical": historical_summary, "july24": july_summary,
        "active_roster_source": {
            "decision": "NO_RUN_TAGGED_DATED_ROSTER_SNAPSHOT",
            "mutable_source": "mlb.player_ids via refresh_mlb_players_rosters.py",
            "reconstruction_usable": False,
        },
        "activation_gate_passed": activation,
        "morning_board_action": "ACTIVATE_ROLE_ENVELOPE" if activation else "PILOT_ONLY_KEEP_FULL_1_9_BOARD",
    }
    board = july[["player_name", "game", "pilot_decision", "role_class"]].copy()
    board["line"] = "Over 1.5 TB"
    board["ubo5_status"] = board.pilot_decision.str.replace("_", " ")
    board["start_outlook"] = board.role_class.map(ROLE_LABELS)
    board = board[["player_name", "game", "line", "ubo5_status", "start_outlook"]]
    order = {"CONFIRM IF STARTING": 0, "WAIT FOR ORDER": 1, "WAIT FOR ROLE": 2, "PASS IF STARTING": 3}
    board["_sort"] = board.ubo5_status.map(order)
    board = board.sort_values(["_sort", "game", "player_name"]).drop(columns="_sort")
    board.to_csv(args.output_dir / "ubo5_tb15_role_envelope_pilot_board_2026-07-24.csv", index=False)
    lines = [
        "# UBO-5 TB 1.5 Plausible Role-and-Order Envelope Pilot — 2026-07-24", "",
        "**PILOT ONLY · NOT ACTIVE · LINEUP UNCONFIRMED · CONDITIONAL ON STARTING**", "",
        "| Player | Game | Line | UBO-5 | Start outlook |",
        "| --- | --- | --- | --- | --- |",
    ]
    lines.extend(
        f"| {row.player_name} | {row.game} | {row.line} | {row.ubo5_status} | {row.start_outlook} |"
        for row in board.itertuples()
    )
    (args.output_dir / "ubo5_tb15_role_envelope_pilot_board_2026-07-24.md").write_text("\n".join(lines) + "\n")
    source_inventory = pd.DataFrame([
        {
            "source": "mlb.player_ids active roster refresh", "classification": "MUTABLE_CURRENT_STATE",
            "timestamp": "updated_at when schema supports it", "july24_reconstruction_usable": False,
            "reliability": "not run-tagged or date-versioned; cannot prove 5:30 state",
        },
        {
            "source": "normalized starting_lineups", "classification": "STRICT_PRIOR_COMPLETED_GAMES",
            "timestamp": "game_date strictly before target", "july24_reconstruction_usable": True,
            "reliability": "certified prior starts/order; 2026 StatsAPI defensive positions",
        },
        {
            "source": "normalized player_game_outcomes", "classification": "STRICT_PRIOR_COMPLETED_GAMES",
            "timestamp": "game_date strictly before target", "july24_reconstruction_usable": True,
            "reliability": "prior appearance/bench proxy only",
        },
        {
            "source": "BetOnline market population", "classification": "MARKET_LISTING_ONLY",
            "timestamp": "run-tagged snapshot timestamp", "july24_reconstruction_usable": False,
            "reliability": "does not certify active roster or starting status",
        },
    ])
    source_inventory.to_csv(args.output_dir / "strict_prior_role_source_inventory.csv", index=False)
    (args.output_dir / "role_envelope_pilot_results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
