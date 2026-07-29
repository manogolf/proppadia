#!/usr/bin/env python3
"""Render a presentation-only UBO-5 TB1.5 nine-position confirmation board."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import implied, number
from backend.mlb.scripts.build_mlb_ubo5_tb15_provisional_tracker import (
    bool_value, lineup_context, market_rows, status_for,
)
from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import (
    FEATURES, MODEL_SUPPORTED_NULL_FEATURES,
)
from backend.mlb.scripts.research_archive.hits_total_bases.total_bases.ubo5.run_mlb_ubo5_tb15_role_envelope_pilot import (
    HYBRID_PROMOTION_ROLES, ROLE_LABELS, read_normalized, role_context,
)
from backend.mlb.shared.ubo5_tb15_production_route import ARTIFACT_SHA256, sha256_file
from backend.mlb.shared.ubo5_tb15_run_snapshot_spine import freeze_complete_run

ROOT = Path(__file__).resolve().parents[3]
CLASS_LABELS = {
    "ROBUST_CONFIRM": "CONFIRM",
    "ORDER_SENSITIVE_WAIT": "WAIT FOR LINEUP",
    "ROBUST_PASS": "PASS",
}
SORT_ORDER = {"CONFIRM": 0, "LIKELY CONFIRM IF STARTING": 1, "WAIT FOR LINEUP": 2, "PASS": 3}
BOARD_FIELDS = ["player_name", "game", "line", "ubo5_status"]
AUDIT_FIELDS = [
    "slate_date", "run_tag", "snapshot_timestamp_utc", "game_pk", "batter_mlb_id",
    "player_name", "game", "lineup_status",
    *[f"ubo5_probability_batting_{slot}" for slot in range(1, 10)],
    "minimum_ubo5_over_probability", "maximum_ubo5_over_probability",
    "BetOnline_over_price", "BetOnline_under_price", "no_vig_over_probability",
    "provisional_classification", "full_1_to_9_classification", "role_class",
    "start_outlook", "plausible_batting_positions", "plausible_envelope_classification",
    "hybrid_display_status", "unscored_reason",
]
TRANSITION_FIELDS = [
    "slate_date", "origin_run_tag", "comparison_run_tag", "game_pk", "batter_mlb_id",
    "player_name", "game", "origin_classification", "confirmed_batting_order",
    "origin_envelope_probability_at_confirmed_order", "origin_no_vig_over_probability",
    "confirmed_route_ubo5_probability", "transition_outcome", "integrity_status",
]


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def immutable_write(path: Path, rows: list[dict], fields: list[str]) -> None:
    candidate = path.with_suffix(path.suffix + ".candidate")
    write_csv(candidate, rows, fields)
    if path.exists():
        if path.read_bytes() != candidate.read_bytes():
            candidate.unlink()
            raise RuntimeError(f"IMMUTABLE_RUN_TAG_COLLISION:{path.name}")
        candidate.unlink()
    else:
        candidate.replace(path)


def lineup_as_of(player_path: Path, team_path: Path, captured: pd.Timestamp) -> tuple[dict, dict]:
    players, teams = lineup_context(player_path, team_path)
    filtered_players = {}
    for key, row in players.items():
        fetched = pd.to_datetime(row.get("source_fetched_at_utc"), utc=True, errors="coerce")
        if pd.notna(fetched) and fetched <= captured:
            filtered_players[key] = row
    filtered_teams = {}
    for key, row in teams.items():
        fetched = pd.to_datetime(row.get("source_fetched_at_utc"), utc=True, errors="coerce")
        if pd.notna(fetched) and fetched <= captured:
            filtered_teams[key] = row
    return filtered_players, filtered_teams


def candidates_for_slot(
    date: str, run_tag: str, captured: pd.Timestamp, markets: list[dict], slot: int,
) -> pd.DataFrame:
    rows = []
    for market in markets:
        start = pd.to_datetime(market.get("game_time"), utc=True, errors="coerce")
        if pd.isna(start) or captured >= start:
            continue
        rows.append({
            "slate_date": date, "game_pk": int(market["game_id"]),
            "batter_mlb_id": int(market["player_id"]), "team": market["team"],
            "opponent": market["opponent"],
            "home_away": "home" if bool_value(market.get("is_home")) else "away",
            "prediction_timestamp_utc": captured.isoformat(),
            "scheduled_start_utc": start.isoformat(), "lineup_certified": False,
            "lineup_certified_at_utc": "", "batting_order_position": slot,
            "line": 1.5, "run_tag": run_tag, "opposing_starter_id": "",
            "batter_identity_certified": True, "identity_ambiguous": False,
            "source_lineage_pointer": "run-tagged BetOnline market plus strict-prior normalized platform",
            "market_row_certified": True,
        })
    return pd.DataFrame(rows)


def materialize_envelope(
    date: str, run_tag: str, captured: pd.Timestamp, markets: list[dict],
    normalized_root: Path, artifact: Path, work_dir: Path,
) -> tuple[dict[tuple[int, int], list[float | None]], dict[tuple[int, int], str]]:
    if not artifact.is_file() or sha256_file(artifact) != ARTIFACT_SHA256:
        return {}, {
            (int(row["game_id"]), int(row["player_id"])): "ARTIFACT_MISSING_OR_HASH_MISMATCH"
            for row in markets
        }
    bundle = joblib.load(artifact)
    if list(bundle.get("features", [])) != list(FEATURES):
        raise RuntimeError("FROZEN_FEATURE_ORDER_MISMATCH")
    indicators = list(bundle["model"].named_steps["simpleimputer"].indicator_.features_)
    if indicators != [FEATURES.index(name) for name in MODEL_SUPPORTED_NULL_FEATURES]:
        raise RuntimeError("FROZEN_NULL_INDICATOR_CONTRACT_MISMATCH")
    model = bundle["model"]
    classes = list(model.classes_)
    probabilities: dict[tuple[int, int], list[float | None]] = {
        (int(row["game_id"]), int(row["player_id"])): [None] * 9 for row in markets
    }
    reasons: dict[tuple[int, int], str] = {}
    invariant_columns: dict[tuple[int, int], pd.Series] = {}
    allowed_slot_conditioned = {"batting_order_position", "prior_slot_pa_per_start"}
    comparison_columns = [name for name in FEATURES if name not in allowed_slot_conditioned]
    for slot in range(1, 10):
        slot_dir = work_dir / f"batting_{slot}"
        slot_dir.mkdir(parents=True, exist_ok=True)
        candidate_path, feature_path = slot_dir / "candidates.csv", slot_dir / "features.parquet"
        candidates_for_slot(date, run_tag, captured, markets, slot).to_csv(candidate_path, index=False)
        subprocess.run([
            sys.executable, "-m", "backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features",
            "--normalized-root", str(normalized_root), "--candidate-file", str(candidate_path),
            "--output", str(feature_path), "--allow-unconfirmed-provisional",
        ], cwd=ROOT, check=True, capture_output=True, text=True)
        features = pd.read_parquet(feature_path)
        for _, row in features.iterrows():
            key = (int(row.game_pk), int(row.batter_mlb_id))
            if key in invariant_columns:
                left = invariant_columns[key]
                right = row[comparison_columns]
                equal = (left.eq(right) | (left.isna() & right.isna())).all()
                if not equal:
                    reasons[key] = "NON_ORDER_FEATURE_INVARIANCE_FAILURE"
                    continue
            else:
                invariant_columns[key] = row[comparison_columns].copy()
            reason = str(row.get("exclusion_reason") or "")
            if not reason and number(row.get("history_depth_pa")) is not None and number(row["history_depth_pa"]) < 100:
                reason = "STRICT_PRIOR_PA_LT_100"
            if not reason and row.get("feature_completeness_status") not in {"COMPLETE", "COMPLETE_WITH_MODEL_SUPPORTED_NULLS"}:
                reason = str(row.get("feature_completeness_status") or "INCOMPLETE_REQUIRED_FEATURE")
            if not reason and row.get("temporal_integrity_status") != "PASS":
                reason = str(row.get("temporal_integrity_status") or "TEMPORAL_INTEGRITY_FAILURE")
            if reason:
                reasons.setdefault(key, reason)
                continue
            vector = pd.DataFrame([row[FEATURES]])
            raw = model.predict_proba(vector)[0]
            probability = 1 - dict(zip(classes, raw)).get(0, 0) - dict(zip(classes, raw)).get(1, 0)
            if not np.isfinite(probability) or not 0 <= probability <= 1:
                reasons[key] = "INVALID_UBO5_PROBABILITY"
                continue
            probabilities[key][slot - 1] = float(probability)
    for key, values in probabilities.items():
        if key in reasons:
            probabilities[key] = [None] * 9
        elif any(value is None for value in values):
            reasons[key] = "INCOMPLETE_NINE_POSITION_ENVELOPE"
            probabilities[key] = [None] * 9
    return probabilities, reasons


def classify(values: list[float], no_vig: float) -> str:
    minimum, maximum = min(values), max(values)
    if minimum > no_vig:
        return "ROBUST_CONFIRM"
    if maximum <= no_vig:
        return "ROBUST_PASS"
    return "ORDER_SENSITIVE_WAIT"


def read_routes(path: Path) -> dict[tuple[int, int], dict]:
    if not path.is_file():
        return {}
    frame = pd.read_csv(path)
    return {
        (int(row["game_pk"]), int(row["batter_mlb_id"])): row
        for row in frame.to_dict("records")
        if number(row.get("game_pk")) is not None and number(row.get("batter_mlb_id")) is not None
    }


def transitions(
    date: str, run_tag: str, current_audit_path: Path, audit_dir: Path,
    routes: dict, player_map: dict, team_map: dict, current_markets: dict[tuple[int, int], dict],
) -> list[dict]:
    prior_paths = sorted(
        path for path in audit_dir.glob("ubo5_tb15_prelineup_confirmation_audit_*.csv")
        if path != current_audit_path
    )
    if not prior_paths:
        return []
    audits = [pd.read_csv(path) for path in prior_paths]
    origin = pd.concat(audits, ignore_index=True).sort_values("snapshot_timestamp_utc").drop_duplicates(
        ["game_pk", "batter_mlb_id"], keep="first"
    )
    rows = []
    for source in origin.to_dict("records"):
        key = (int(source["game_pk"]), int(source["batter_mlb_id"]))
        origin_class = str(source.get("provisional_classification") or "")
        if not origin_class:
            continue
        player = player_map.get(key)
        route = routes.get(key)
        current_market = current_markets.get(key, {})
        team_code = str((route or {}).get("team") or current_market.get("team") or "")
        team = team_map.get((key[0], team_code))
        confirmed_slot = number((player or {}).get("lineup_slot"))
        selected = number(source.get(f"ubo5_probability_batting_{int(confirmed_slot)}")) if confirmed_slot else None
        market = number(source.get("no_vig_over_probability"))
        exact_positive = selected is not None and market is not None and selected > market
        if confirmed_slot is not None:
            if origin_class == "ROBUST_CONFIRM":
                outcome = "ROBUST_CONFIRM remained positive" if exact_positive else "ROBUST_CONFIRM_REVERSED"
            elif origin_class == "ROBUST_PASS":
                outcome = "ROBUST_PASS remained nonpositive" if not exact_positive else "ROBUST_PASS_REVERSED"
            else:
                outcome = "ORDER_SENSITIVE_WAIT resolved to positive" if exact_positive else "ORDER_SENSITIVE_WAIT resolved to nonpositive"
        elif team and str(team.get("lineup_status")) == "confirmed_full":
            outcome = "player was not in the starting lineup"
        elif key not in current_markets:
            outcome = "market disappeared before confirmation"
        else:
            outcome = "PENDING_LINEUP_CONFIRMATION"
        integrity = "DEFECT" if outcome.endswith("_REVERSED") else ("PASS" if confirmed_slot is not None else "PENDING")
        rows.append({
            "slate_date": date, "origin_run_tag": source.get("run_tag"), "comparison_run_tag": run_tag,
            "game_pk": key[0], "batter_mlb_id": key[1], "player_name": source.get("player_name"),
            "game": source.get("game"), "origin_classification": origin_class,
            "confirmed_batting_order": "" if confirmed_slot is None else int(confirmed_slot),
            "origin_envelope_probability_at_confirmed_order": "" if selected is None else f"{selected:.10f}",
            "origin_no_vig_over_probability": "" if market is None else f"{market:.10f}",
            "confirmed_route_ubo5_probability": "" if route is None or number(route.get("ubo5_probability_over")) is None else f"{number(route['ubo5_probability_over']):.10f}",
            "transition_outcome": outcome, "integrity_status": integrity,
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--run-tag", required=True)
    ap.add_argument("--odds-json", required=True, type=Path)
    ap.add_argument("--wide-csv", required=True, type=Path)
    ap.add_argument("--lineup-csv", required=True, type=Path)
    ap.add_argument("--lineup-team-summary", required=True, type=Path)
    ap.add_argument("--route-ledger", required=True, type=Path)
    ap.add_argument("--normalized-root", required=True, type=Path)
    ap.add_argument("--artifact", required=True, type=Path)
    ap.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15", type=Path)
    ap.add_argument("--skip-run-snapshot", action="store_true")
    args = ap.parse_args()
    snapshot = json.loads(args.odds_json.read_text())
    captured = pd.to_datetime(snapshot.get("captured_at_utc"), utc=True, errors="coerce")
    if pd.isna(captured):
        raise RuntimeError("SNAPSHOT_TIMESTAMP_MISSING")
    markets, identity_rejects = market_rows(snapshot, pd.read_csv(args.wide_csv))
    unstarted = [
        row for row in markets
        if captured < pd.to_datetime(row.get("game_time"), utc=True, errors="coerce")
    ]
    day_dir = args.output_root / args.date
    safe_tag = "".join(c for c in args.run_tag if c.isalnum() or c in "._-")
    work_dir = day_dir / ".prelineup_confirmation_work" / safe_tag
    probabilities, reasons = materialize_envelope(
        args.date, args.run_tag, captured, unstarted, args.normalized_root, args.artifact, work_dir
    )
    player_map, team_map = lineup_as_of(args.lineup_csv, args.lineup_team_summary, captured)
    full_player_map, full_team_map = lineup_context(args.lineup_csv, args.lineup_team_summary)
    games = read_normalized(args.normalized_root, "games")
    games["game_date"] = pd.to_datetime(games.game_date)
    role_lineups = read_normalized(args.normalized_root, "starting_lineups")
    role_lineups = role_lineups.merge(
        games[["game_pk", "game_date", "home_team", "away_team"]], on="game_pk", how="left"
    )
    missing_team = role_lineups.team.isna() | role_lineups.team.astype(str).str.strip().eq("")
    role_lineups.loc[missing_team & role_lineups.home_away.astype(str).isin(["home", "h"]), "team"] = role_lineups["home_team"]
    role_lineups.loc[missing_team & role_lineups.home_away.astype(str).isin(["away", "v"]), "team"] = role_lineups["away_team"]
    role_lineups["player_id"] = pd.to_numeric(role_lineups.player_id, errors="coerce")
    role_lineups = role_lineups.dropna(subset=["player_id", "game_date", "batting_order_position"]).copy()
    role_lineups["player_id"] = role_lineups.player_id.astype(int)
    role_outcomes = read_normalized(args.normalized_root, "player_game_outcomes")
    role_outcomes["player_id"] = pd.to_numeric(role_outcomes.player_id, errors="coerce")
    role_outcomes = role_outcomes.dropna(subset=["player_id"]).copy()
    role_outcomes["player_id"] = role_outcomes.player_id.astype(int)
    audit_rows, board_rows = [], []
    for market in unstarted:
        key = (int(market["game_id"]), int(market["player_id"]))
        status, _, _ = status_for(market, player_map, team_map)
        oi, ui = implied(market["over_price"]), implied(market["under_price"])
        no_vig = oi / (oi + ui) if oi is not None and ui is not None and oi + ui else None
        values = probabilities.get(key, [None] * 9)
        reason = reasons.get(key, "")
        classification = classify(values, no_vig) if not reason and no_vig is not None else ""
        context = role_context(
            role_lineups, role_outcomes, games, pd.Timestamp(args.date), str(market["team"]), key[1]
        )
        plausible_slots = context.pop("_slots")
        plausible_classification = (
            classify([values[slot - 1] for slot in plausible_slots], no_vig)
            if classification and no_vig is not None else ""
        )
        if classification == "ROBUST_CONFIRM":
            hybrid_status = "CONFIRM"
        elif classification == "ROBUST_PASS":
            hybrid_status = "PASS"
        elif (
            classification == "ORDER_SENSITIVE_WAIT"
            and plausible_classification == "ROBUST_CONFIRM"
            and context["role_class"] in HYBRID_PROMOTION_ROLES
        ):
            hybrid_status = "LIKELY CONFIRM IF STARTING"
        else:
            hybrid_status = "WAIT FOR LINEUP"
        row = {
            "slate_date": args.date, "run_tag": args.run_tag,
            "snapshot_timestamp_utc": captured.isoformat(), "game_pk": key[0],
            "batter_mlb_id": key[1], "player_name": market["player_name"],
            "game": market["game"], "lineup_status": status,
            **{
                f"ubo5_probability_batting_{slot}": "" if values[slot - 1] is None else f"{values[slot - 1]:.10f}"
                for slot in range(1, 10)
            },
            "minimum_ubo5_over_probability": "" if reason else f"{min(values):.10f}",
            "maximum_ubo5_over_probability": "" if reason else f"{max(values):.10f}",
            "BetOnline_over_price": market["over_price"], "BetOnline_under_price": market["under_price"],
            "no_vig_over_probability": "" if no_vig is None else f"{no_vig:.10f}",
            "provisional_classification": classification,
            "full_1_to_9_classification": classification,
            "role_class": context["role_class"],
            "start_outlook": ROLE_LABELS[context["role_class"]],
            "plausible_batting_positions": "|".join(map(str, plausible_slots)),
            "plausible_envelope_classification": plausible_classification,
            "hybrid_display_status": hybrid_status,
            "unscored_reason": reason or ("" if no_vig is not None else "CURRENT_TWO_SIDED_PRICE_UNAVAILABLE"),
        }
        audit_rows.append(row)
        current_status, _, _ = status_for(market, full_player_map, full_team_map)
        if current_status in {"LINEUP_UNCONFIRMED", "LINEUP_STATUS_UNKNOWN"}:
            board_rows.append({
                "player_name": market["player_name"], "game": market["game"],
                "line": "Over 1.5 TB", "ubo5_status": hybrid_status,
            })
    board_rows.sort(key=lambda row: (SORT_ORDER[row["ubo5_status"]], row["game"], row["player_name"]))
    audit_path = day_dir / f"ubo5_tb15_prelineup_confirmation_audit_{safe_tag}.csv"
    immutable_write(audit_path, audit_rows, AUDIT_FIELDS)
    board_csv = day_dir / f"ubo5_tb15_prelineup_confirmation_board_{args.date}.csv"
    write_csv(board_csv, board_rows, BOARD_FIELDS)
    board_md = day_dir / f"ubo5_tb15_prelineup_confirmation_board_{args.date}.md"
    lines = [
        f"# UBO-5 TB 1.5 Pre-Lineup Confirmation Board — {args.date}", "",
        "**LINEUP UNCONFIRMED · PRESENTATION ONLY**", "",
        f"Run tag: `{args.run_tag}`  ", f"Snapshot: `{captured.isoformat()}`", "",
        "| Player | Game | Line | UBO-5 |", "| --- | --- | --- | --- |",
    ]
    if board_rows:
        lines.extend(f"| {row['player_name']} | {row['game']} | {row['line']} | {row['ubo5_status']} |" for row in board_rows)
    else:
        lines.append("| *None* |  |  |  |")
    counts = pd.Series([row["provisional_classification"] for row in audit_rows]).value_counts()
    hybrid_counts = pd.Series([row["hybrid_display_status"] for row in audit_rows]).value_counts()
    lines += [
        "", "CONFIRM and PASS are governed exclusively by the complete batting-position 1–9 envelope. LIKELY CONFIRM IF STARTING is an asymmetric presentation-only promotion; a narrowed-envelope PASS can never produce PASS.", "",
        f"Scored markets: **{sum(bool(row['provisional_classification']) for row in audit_rows)}**  ",
        f"Unscored markets: **{sum(bool(row['unscored_reason']) for row in audit_rows)}**  ",
        f"Exact identity rejects: **{len(identity_rejects)}**",
    ]
    board_md.write_text("\n".join(lines) + "\n")
    routes = read_routes(args.route_ledger)
    transition_rows = transitions(
        args.date, args.run_tag, audit_path, day_dir, routes, full_player_map, full_team_map,
        {
            (int(row["game_id"]), int(row["player_id"])): row
            for row in unstarted
        },
    )
    transition_path = day_dir / f"ubo5_tb15_prelineup_confirmation_transitions_{safe_tag}.csv"
    immutable_write(transition_path, transition_rows, TRANSITION_FIELDS)
    run_population_manifest = None
    if not args.skip_run_snapshot:
        run_population_manifest = freeze_complete_run(
            repository_root=ROOT,
            output_root=args.output_root,
            date=args.date,
            run_tag=args.run_tag,
            market_snapshot_path=args.odds_json,
            identity_source_path=args.wide_csv,
            route_ledger_path=args.route_ledger,
            prelineup_audit_path=audit_path,
            identity_rejects=identity_rejects,
        )
    latest = args.output_root / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    shutil.copy2(board_md, latest / "ubo5_tb15_prelineup_confirmation_board.md")
    shutil.copy2(board_csv, latest / "ubo5_tb15_prelineup_confirmation_board.csv")
    shutil.rmtree(work_dir, ignore_errors=True)
    payload = {
        "run_tag": args.run_tag, "market_rows": len(unstarted),
        "base_feature_vectors_successfully_materialized": sum(bool(row["provisional_classification"]) for row in audit_rows),
        "rows_blocked_other_than_batting_order": sum(bool(row["unscored_reason"]) for row in audit_rows),
        "ROBUST_CONFIRM": int(counts.get("ROBUST_CONFIRM", 0)),
        "ROBUST_PASS": int(counts.get("ROBUST_PASS", 0)),
        "ORDER_SENSITIVE_WAIT": int(counts.get("ORDER_SENSITIVE_WAIT", 0)),
        "hybrid_CONFIRM": int(hybrid_counts.get("CONFIRM", 0)),
        "hybrid_LIKELY_CONFIRM_IF_STARTING": int(hybrid_counts.get("LIKELY CONFIRM IF STARTING", 0)),
        "hybrid_PASS": int(hybrid_counts.get("PASS", 0)),
        "hybrid_WAIT_FOR_LINEUP": int(hybrid_counts.get("WAIT FOR LINEUP", 0)),
        "identity_rejects": len(identity_rejects), "transition_rows": len(transition_rows),
        "transition_defects": sum(row["integrity_status"] == "DEFECT" for row in transition_rows),
        "complete_run_snapshot_rows": 0 if args.skip_run_snapshot else len(audit_rows),
        "complete_run_snapshot_count": (
            0 if run_population_manifest is None
            else run_population_manifest["run_snapshot_count"]
        ),
    }
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
