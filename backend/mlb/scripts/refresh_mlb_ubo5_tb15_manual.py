#!/usr/bin/env python3
"""Run a narrow, fresh, operator-initiated UBO-5 TB 1.5 refresh."""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.app.services.mlb import market_odds_service
from backend.mlb.scripts import build_mlb_predictions_wide as identity
from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import implied, number
from backend.mlb.scripts.build_mlb_ubo5_tb15_prelineup_confirmation_board import (
    AUDIT_FIELDS as PRELINEUP_AUDIT_FIELDS,
    BOARD_FIELDS as PRELINEUP_BOARD_FIELDS,
)
from backend.mlb.scripts.build_mlb_ubo5_tb15_provisional_tracker import (
    lineup_context,
    market_rows,
    score_candidates,
)
from backend.mlb.shared.ubo5_tb15_production_route import (
    ARTIFACT_SHA256,
    COUNTERFACTUAL_SOURCE,
    counterfactual_row_hash,
    sha256_file,
)
from backend.mlb.shared.ubo5_tb15_consensus_selection import freeze as freeze_consensus
from backend.mlb.shared.ubo5_tb15_run_snapshot_spine import freeze_complete_run

ROOT = Path(__file__).resolve().parents[3]
OUTPUT_ROOT = ROOT / "backend/mlb/exports/model_v2/ubo5_tb15"
NORMALIZED_ROOT = (
    ROOT
    / "artifacts/analysis/model_development/"
    "mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/normalized_refresh"
)
ARTIFACT = (
    ROOT
    / "artifacts/analysis/model_development/"
    "mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/"
    "original_ubo5_total_bases_multinomial.joblib"
)
BOARD_FIELDS = [
    "player_name",
    "game",
    "line",
    "ubo5_over_probability",
    "no_vig_over_probability",
    "over_edge_percentage_points",
]
MARKET_FIELDS = [
    "slate_date",
    "run_tag",
    "capture_timestamp_utc",
    "game_pk",
    "batter_mlb_id",
    "player_name",
    "game",
    "line",
    "BetOnline_over_price",
    "BetOnline_under_price",
    "no_vig_over_probability",
    "game_time_utc",
]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def stage_timer(timings: dict[str, float], name: str, started: float) -> None:
    timings[name] = round(time.monotonic() - started, 3)


def safe_error(exc: Exception) -> str:
    text = re.sub(r"(?i)(apiKey=)[^&\s]+", r"\1[REDACTED]", str(exc))
    return f"{type(exc).__name__}: {text}"


def narrow_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for event in events:
        books = []
        for book in event.get("bookmakers") or []:
            if str(book.get("key") or "").lower() != "betonlineag":
                continue
            markets = [
                market
                for market in book.get("markets") or []
                if market.get("key") == "batter_total_bases"
            ]
            if markets:
                books.append({**book, "markets": markets})
        if books:
            result.append({**event, "bookmakers": books})
    return result


def build_identity_rows(
    date: str,
    events: list[dict[str, Any]],
    *,
    source_path: Path,
    captured_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int]]:
    by_player_id, by_name_team = identity._load_player_rows(active_only=True)
    by_team_ctx, by_pair_games = identity._build_schedule_maps(date)
    offers, flatten_counts = identity._flatten_market_snapshot(
        events=events,
        market_to_prop={"batter_total_bases": "total_bases"},
        team_name_rev=identity._build_team_name_reverse(),
        prop_filter={"total_bases"},
        require_two_sided=True,
        two_sided_bookmaker="betonlineag",
        optional_target_book_props=set(),
    )
    offers = [offer for offer in offers if abs(float(offer.line) - 1.5) < 1e-9]
    resolved, resolve_counts = identity._resolve_offers(
        offers=offers, by_name_team=by_name_team, by_pair_games=by_pair_games
    )
    incumbent_predictions, prediction_counts, _ = identity._predict_rows(
        resolved, by_team_ctx=by_team_ctx, by_player_id=by_player_id
    )
    resolve_counts.update({
        f"incumbent_{key}": int(value) for key, value in prediction_counts.items()
    })
    incumbent_by_identity = {
        (
            int(row["game_id"]), int(row["player_id"]),
            str(row["prop_type"]), float(row["line"]),
        ): row
        for row in incumbent_predictions
    }
    incumbent_artifact = (
        Path(os.environ.get("MODEL_DIR", "/var/data/proppadia/models"))
        / "latest/total_bases.joblib"
    )
    incumbent_artifact_hash = (
        sha256_file(incumbent_artifact) if incumbent_artifact.is_file() else ""
    )
    captured_at_utc = captured_at.isoformat()
    rows = []
    for item in resolved:
        offer = item.offer
        prediction = incumbent_by_identity.get(
            (int(item.game.game_id), int(item.player.player_id), "total_bases", 1.5)
        ) or {}
        incumbent_probability = number(prediction.get("prob_over"))
        strategy = str(prediction.get("prediction_model_strategy") or "")
        lineage_certified = bool(
            incumbent_probability is not None
            and strategy == "model_pipeline"
            and incumbent_artifact_hash
            and incumbent_artifact_hash != ARTIFACT_SHA256
        )
        rows.append(
            {
                "slate_date": date,
                "game_id": int(item.game.game_id),
                "player_id": int(item.player.player_id),
                "player_name": item.player.player_name,
                "prop_type": "total_bases",
                "line": 1.5,
                "p_over_1_5": incumbent_probability if lineage_certified else "",
                "counterfactual_incumbent_probability": (
                    incumbent_probability if lineage_certified else ""
                ),
                "counterfactual_incumbent_model_source": (
                    COUNTERFACTUAL_SOURCE if lineage_certified else ""
                ),
                "counterfactual_incumbent_artifact_hash": (
                    incumbent_artifact_hash if lineage_certified else ""
                ),
                "counterfactual_incumbent_source_path": (
                    str(source_path) if lineage_certified else ""
                ),
                "counterfactual_incumbent_captured_at_utc": (
                    captured_at_utc if lineage_certified else ""
                ),
                "counterfactual_incumbent_status": (
                    "PRESERVED" if lineage_certified
                    else "COUNTERFACTUAL_INCUMBENT_UNAVAILABLE"
                ),
                "counterfactual_capture_before_routing_status": (
                    "PASS" if lineage_certified else "FAIL"
                ),
                "active_probability_lineage_status": (
                    "PASS" if lineage_certified else "FAIL"
                ),
                "probability_delta_integrity_status": (
                    "PASS" if lineage_certified else "FAIL"
                ),
                "counterfactual_compatibility_alias_status": (
                    "PASS" if lineage_certified else "FAIL"
                ),
                "counterfactual_lineage_integrity_status": (
                    "PASS" if lineage_certified else "FAIL"
                ),
                "counterfactual_incumbent_feature_or_row_hash": "",
                "team": item.team_abbr,
                "opponent": offer.away_team_abbr if item.is_home else offer.home_team_abbr,
                "home_team_code": offer.home_team_abbr,
                "away_team_code": offer.away_team_abbr,
                "home_team": offer.home_team_name,
                "away_team": offer.away_team_name,
                "is_home": item.is_home,
                "game_time": offer.commence_time,
                "bookmaker_key": "betonlineag",
                "price_over_american": offer.price_over_american,
                "price_under_american": offer.price_under_american,
            }
        )
        if lineage_certified:
            row = rows[-1]
            hash_input = pd.Series({
                "slate_date": date, "game_pk": row["game_id"],
                "batter_mlb_id": row["player_id"], "prop_type": "total_bases",
                "line": 1.5,
                **{
                    key: row[key] for key in (
                        "counterfactual_incumbent_probability",
                        "counterfactual_incumbent_model_source",
                        "counterfactual_incumbent_artifact_hash",
                        "counterfactual_incumbent_source_path",
                    )
                },
            })
            row["counterfactual_incumbent_feature_or_row_hash"] = (
                counterfactual_row_hash(hash_input)
            )
    return rows, flatten_counts, resolve_counts


def render_confirmed_board(
    date: str,
    run_tag: str,
    captured: pd.Timestamp,
    prediction_time: pd.Timestamp,
    markets: list[dict[str, Any]],
    player_map: dict,
    team_map: dict,
    package: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int]:
    scored, contexts = score_candidates(
        date,
        run_tag,
        prediction_time,
        markets,
        player_map,
        team_map,
        NORMALIZED_ROOT,
        ARTIFACT,
        package / "exact_order_work",
    )
    routes: list[dict[str, Any]] = []
    board: list[dict[str, Any]] = []
    consensus_board: list[dict[str, Any]] = []
    confirmed = 0
    unconfirmed = 0
    for market in markets:
        key = (int(market["game_id"]), int(market["player_id"]))
        lineup_status, slot = contexts.get(key, ("LINEUP_STATUS_UNKNOWN", None))
        if lineup_status == "LINEUP_CONFIRMED":
            confirmed += 1
        elif lineup_status in {"LINEUP_UNCONFIRMED", "LINEUP_STATUS_UNKNOWN"}:
            unconfirmed += 1
        else:
            continue
        if lineup_status != "LINEUP_CONFIRMED":
            continue
        feature = scored.get(key) or {}
        probability = number(feature.get("ubo5_over_probability"))
        over_price = number(market.get("over_price"))
        under_price = number(market.get("under_price"))
        oi, ui = implied(int(over_price) if over_price is not None else None), implied(
            int(under_price) if under_price is not None else None
        )
        no_vig = oi / (oi + ui) if oi is not None and ui is not None and oi + ui else None
        exclusion = str(feature.get("exclusion_reason") or "")
        edge = probability - no_vig if probability is not None and no_vig is not None else None
        counterfactual = number(market.get("counterfactual_incumbent_probability"))
        incumbent_edge = (
            counterfactual - no_vig
            if counterfactual is not None and no_vig is not None else None
        )
        lineage_ok = all(
            str(market.get(field) or "") == expected
            for field, expected in {
                "counterfactual_incumbent_status": "PRESERVED",
                "counterfactual_capture_before_routing_status": "PASS",
                "active_probability_lineage_status": "PASS",
                "probability_delta_integrity_status": "PASS",
                "counterfactual_compatibility_alias_status": "PASS",
                "counterfactual_lineage_integrity_status": "PASS",
            }.items()
        )
        consensus_positive = bool(
            not exclusion and lineage_ok
            and edge is not None and edge > 0
            and incumbent_edge is not None and incumbent_edge > 0
        )
        route = {
            "slate_date": date,
            "run_tag": run_tag,
            "snapshot_timestamp_utc": captured.isoformat(),
            "game_pk": key[0],
            "batter_mlb_id": key[1],
            "player_name": market["player_name"],
            "game": market["game"],
            "team": market["team"],
            "opponent": market["opponent"],
            "line": 1.5,
            "confirmed_batting_order": "" if slot is None else int(slot),
            "scheduled_start_utc": market.get("game_time", ""),
            "selection_timestamp_utc": prediction_time.isoformat(),
            "ubo5_probability_over": "" if probability is None else f"{probability:.10f}",
            "BetOnline_over_price": "" if over_price is None else int(over_price),
            "BetOnline_under_price": "" if under_price is None else int(under_price),
            "no_vig_over_probability": "" if no_vig is None else f"{no_vig:.10f}",
            "over_edge_percentage_points": "" if edge is None else f"{edge * 100:.10f}",
            "counterfactual_incumbent_probability": (
                "" if counterfactual is None else f"{counterfactual:.10f}"
            ),
            "incumbent_over_edge": (
                "" if incumbent_edge is None else f"{incumbent_edge:.10f}"
            ),
            "ubo5_over_edge": "" if edge is None else f"{edge:.10f}",
            "consensus_positive_flag": consensus_positive,
            **{
                field: market.get(field, "")
                for field in (
                    "counterfactual_incumbent_model_source",
                    "counterfactual_incumbent_artifact_hash",
                    "counterfactual_incumbent_source_path",
                    "counterfactual_incumbent_captured_at_utc",
                    "counterfactual_incumbent_feature_or_row_hash",
                    "counterfactual_incumbent_status",
                    "counterfactual_capture_before_routing_status",
                    "active_probability_lineage_status",
                    "probability_delta_integrity_status",
                    "counterfactual_compatibility_alias_status",
                    "counterfactual_lineage_integrity_status",
                )
            },
            "feature_vector_sha256": feature.get("feature_vector_sha256", ""),
            "ubo5_artifact_hash": ARTIFACT_SHA256,
            "temporal_integrity_status": feature.get("temporal_integrity_status", ""),
            "exclusion_reason": exclusion,
        }
        routes.append(route)
        if not exclusion and edge is not None and edge > 0:
            board.append(
                {
                    "player_name": market["player_name"],
                    "game": market["game"],
                    "line": "Over 1.5 TB",
                    "ubo5_over_probability": f"{probability * 100:.2f}",
                    "no_vig_over_probability": f"{no_vig * 100:.2f}",
                    "over_edge_percentage_points": f"{edge * 100:+.2f}",
                }
            )
        if consensus_positive:
            consensus_board.append({
                "player_name": market["player_name"],
                "game": market["game"],
                "line": "Over 1.5 TB",
                "consensus": "UBO-5 + Incumbent",
                "_ubo5_over_edge": edge,
            })
    board.sort(key=lambda row: float(row["over_edge_percentage_points"]), reverse=True)
    consensus_board.sort(
        key=lambda row: float(row["_ubo5_over_edge"]), reverse=True
    )
    write_csv(package / "confirmed_route_ledger.csv", routes, list(routes[0]) if routes else [
        "slate_date", "run_tag", "snapshot_timestamp_utc", "game_pk", "batter_mlb_id",
        "player_name", "game", "team", "opponent", "line", "confirmed_batting_order",
        "ubo5_probability_over", "BetOnline_over_price", "BetOnline_under_price",
        "no_vig_over_probability", "over_edge_percentage_points",
        "counterfactual_incumbent_probability", "incumbent_over_edge", "ubo5_over_edge",
        "consensus_positive_flag", "counterfactual_incumbent_model_source",
        "counterfactual_incumbent_artifact_hash", "counterfactual_incumbent_source_path",
        "counterfactual_incumbent_captured_at_utc",
        "counterfactual_incumbent_feature_or_row_hash", "counterfactual_incumbent_status",
        "counterfactual_capture_before_routing_status",
        "active_probability_lineage_status", "probability_delta_integrity_status",
        "counterfactual_compatibility_alias_status",
        "counterfactual_lineage_integrity_status", "feature_vector_sha256",
        "temporal_integrity_status", "exclusion_reason",
    ])
    write_csv(package / "confirmed_positive_edge_board.csv", board, BOARD_FIELDS)
    lines = [
        f"# UBO-5 TB 1.5 Positive Over Board — {date}",
        "",
        f"Run tag: `{run_tag}`  ",
        f"Snapshot: `{captured.isoformat()}`",
        "",
        "| Player | Game | Line | UBO-5 Over | No-vig Over | Over edge |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    if board:
        lines.extend(
            f"| {row['player_name']} | {row['game']} | {row['line']} | "
            f"{row['ubo5_over_probability']}% | {row['no_vig_over_probability']}% | "
            f"{row['over_edge_percentage_points']} pp |"
            for row in board
        )
    else:
        lines.append("| *None* |  |  |  |  |  |")
    lines.extend([
        "",
        "## Consensus Positive — UBO-5 + Incumbent",
        "",
        "| Player | Game | Line | Consensus |",
        "| --- | --- | --- | --- |",
    ])
    if consensus_board:
        lines.extend(
            f"| {row['player_name']} | {row['game']} | {row['line']} | "
            f"{row['consensus']} |"
            for row in consensus_board
        )
    else:
        lines.extend(["", "*None*"])
    (package / "confirmed_positive_edge_board.md").write_text("\n".join(lines) + "\n")
    shutil.rmtree(package / "exact_order_work", ignore_errors=True)
    return routes, board, confirmed, unconfirmed


def run(args: argparse.Namespace) -> int:
    total_started = time.monotonic()
    now = datetime.now(timezone.utc)
    run_tag = f"manual_ubo5_tb15_{now.strftime('%Y%m%dT%H%M%S%fZ')}"
    package = OUTPUT_ROOT / args.date / "manual_refresh" / run_tag
    package.mkdir(parents=True, exist_ok=False)
    timings: dict[str, float] = {}
    status = "FAILED"
    summary: dict[str, Any] = {
        "slate_date": args.date,
        "run_tag": run_tag,
        "capture_timestamp_utc": now.isoformat(),
        "status": status,
    }
    failure: Exception | None = None
    try:
        started = time.monotonic()
        os.environ["MLB_ODDS_MARKETS"] = "batter_total_bases"
        os.environ["MLB_ODDS_BOOKMAKERS"] = "betonlineag"
        raw_events = market_odds_service._fetch_market_snapshot(game_date=args.date)
        events = narrow_events(raw_events)
        captured = pd.Timestamp.now(tz="UTC")
        snapshot = {
            "slate_date": args.date,
            "captured_at_utc": captured.isoformat(),
            "run_tag": run_tag,
            "capture_scope": "betonlineag:batter_total_bases",
            "events": events,
        }
        snapshot_path = package / "betonline_tb15_snapshot.json"
        snapshot_path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
        wide_path = package / "identity_binding.csv"
        identity_rows, flatten_counts, resolve_counts = build_identity_rows(
            args.date,
            events,
            source_path=wide_path,
            captured_at=captured.to_pydatetime(),
        )
        write_csv(wide_path, identity_rows, list(identity_rows[0]) if identity_rows else [
            "slate_date", "game_id", "player_id", "player_name", "prop_type", "line",
            "p_over_1_5", "team", "opponent", "home_team_code", "away_team_code",
            "home_team", "away_team", "is_home", "game_time",
        ])
        identity_frame = pd.DataFrame(identity_rows)
        if identity_frame.empty:
            identity_frame = pd.DataFrame(
                columns=[
                    "game_id", "player_id", "player_name", "prop_type", "line",
                    "p_over_1_5", "team", "opponent", "home_team_code",
                    "away_team_code", "home_team", "away_team", "is_home", "game_time",
                ]
            )
        markets, identity_rejects = market_rows(snapshot, identity_frame)
        unstarted = [
            row for row in markets
            if captured < pd.to_datetime(row.get("game_time"), utc=True, errors="coerce")
        ]
        market_evidence = []
        for row in unstarted:
            oi, ui = implied(int(row["over_price"])), implied(int(row["under_price"]))
            no_vig = oi / (oi + ui) if oi is not None and ui is not None and oi + ui else None
            market_evidence.append({
                "slate_date": args.date, "run_tag": run_tag,
                "capture_timestamp_utc": captured.isoformat(), "game_pk": int(row["game_id"]),
                "batter_mlb_id": int(row["player_id"]), "player_name": row["player_name"],
                "game": row["game"], "line": 1.5, "BetOnline_over_price": row["over_price"],
                "BetOnline_under_price": row["under_price"],
                "no_vig_over_probability": "" if no_vig is None else f"{no_vig:.10f}",
                "game_time_utc": row.get("game_time", ""),
            })
        write_csv(package / "betonline_tb15_markets.csv", market_evidence, MARKET_FIELDS)
        stage_timer(timings, "odds_capture_seconds", started)

        started = time.monotonic()
        label = run_tag
        subprocess.run([
            sys.executable, "-m", "backend.mlb.scripts.dry_run_capture_pregame_lineups",
            "--date", args.date, "--output-dir", str(package), "--snapshot-label", label,
            "--mode", "dry_run",
        ], cwd=ROOT, check=True)
        player_path = package / f"pregame_lineup_player_rows_{args.date}_{label}.csv"
        team_path = package / f"pregame_lineup_game_team_summary_{args.date}_{label}.csv"
        shutil.copy2(team_path, package / "lineup_status.csv")
        player_map, team_map = lineup_context(player_path, team_path)
        prediction_time = pd.Timestamp.now(tz="UTC")
        stage_timer(timings, "lineup_capture_seconds", started)

        started = time.monotonic()
        staging_root = package / "staged_boards"
        empty_routes = package / "prelineup_route_context.csv"
        write_csv(empty_routes, [], ["game_pk", "batter_mlb_id", "ubo5_probability_over"])
        staged_day = staging_root / args.date
        staged_day.mkdir(parents=True, exist_ok=True)
        pre_audit = staged_day / f"ubo5_tb15_prelineup_confirmation_audit_{run_tag}.csv"
        staged_pre_md = staged_day / f"ubo5_tb15_prelineup_confirmation_board_{args.date}.md"
        staged_pre_csv = staged_day / f"ubo5_tb15_prelineup_confirmation_board_{args.date}.csv"
        if unstarted:
            subprocess.run([
                sys.executable, "-m",
                "backend.mlb.scripts.build_mlb_ubo5_tb15_prelineup_confirmation_board",
                "--date", args.date, "--run-tag", run_tag, "--odds-json", str(snapshot_path),
                "--wide-csv", str(wide_path), "--lineup-csv", str(player_path),
                "--lineup-team-summary", str(team_path), "--route-ledger", str(empty_routes),
                "--normalized-root", str(NORMALIZED_ROOT), "--artifact", str(ARTIFACT),
                "--output-root", str(staging_root), "--skip-run-snapshot",
            ], cwd=ROOT, check=True)
        else:
            write_csv(pre_audit, [], PRELINEUP_AUDIT_FIELDS)
            write_csv(staged_pre_csv, [], PRELINEUP_BOARD_FIELDS)
            staged_pre_md.write_text(
                f"# UBO-5 TB 1.5 Pre-Lineup Confirmation Board — {args.date}\n\n"
                f"Run tag: `{run_tag}`  \nSnapshot: `{captured.isoformat()}`\n\n"
                "**NO CURRENT UNSTARTED TWO-SIDED BETONLINE TB 1.5 MARKETS**\n\n"
                "| Player | Game | Line | UBO-5 |\n| --- | --- | --- | --- |\n"
                "| *None* |  |  |  |\n"
            )
        shutil.copy2(pre_audit, package / "prelineup_confirmation_audit.csv")
        stage_timer(timings, "feature_materialization_seconds", started)

        started = time.monotonic()
        routes, confirmed_board, confirmed, unconfirmed = render_confirmed_board(
            args.date, run_tag, captured, prediction_time,
            unstarted, player_map, team_map, package
        )
        selection_rows = []
        for route in routes:
            if str(route.get("consensus_positive_flag")).lower() != "true":
                continue
            selection_rows.append({
                **route, "batting_order": route["confirmed_batting_order"],
                "prop_type": "total_bases", "side": "OVER",
                "betonline_over_price": route["BetOnline_over_price"],
                "betonline_under_price": route["BetOnline_under_price"],
                "ubo5_over_edge_pp": float(route["ubo5_over_edge"]) * 100,
                "incumbent_over_edge_pp": float(route["incumbent_over_edge"]) * 100,
                "counterfactual_lineage_status": "CERTIFIED_SAME_RUN_INDEPENDENT",
                "market_snapshot_path": str(snapshot_path.relative_to(ROOT)),
                "route_ledger_path": str((package / "confirmed_route_ledger.csv").relative_to(ROOT)),
            })
        consensus_manifest = freeze_consensus(
            OUTPUT_ROOT, args.date, run_tag, selection_rows
        )
        run_population_manifest = freeze_complete_run(
            repository_root=ROOT,
            output_root=OUTPUT_ROOT,
            date=args.date,
            run_tag=run_tag,
            market_snapshot_path=snapshot_path,
            identity_source_path=wide_path,
            route_ledger_path=package / "confirmed_route_ledger.csv",
            prelineup_audit_path=package / "prelineup_confirmation_audit.csv",
            identity_rejects=identity_rejects,
        )
        shutil.copy2(staged_pre_md, package / "prelineup_confirmation_board.md")
        shutil.copy2(staged_pre_csv, package / "prelineup_confirmation_board.csv")
        audit = pd.read_csv(package / "prelineup_confirmation_audit.csv")
        counts = audit["hybrid_display_status"].fillna("").value_counts()
        team_status = pd.read_csv(team_path)
        confirmed_teams = int(team_status["lineup_status"].eq("confirmed_full").sum())
        all_teams = len(team_status)
        if not unstarted:
            status = "NO_CURRENT_TWO_SIDED_MARKETS"
        elif confirmed_teams and confirmed_teams < all_teams:
            status = "PARTIAL_LINEUPS_AVAILABLE"
        elif audit["unscored_reason"].fillna("").ne("").any():
            status = "FEATURE_MATERIALIZATION_PARTIAL"
        else:
            status = "READY"
        aliases_updated = status in {
            "READY", "PARTIAL_LINEUPS_AVAILABLE", "FEATURE_MATERIALIZATION_PARTIAL"
        }
        if aliases_updated:
            latest = OUTPUT_ROOT / "latest"
            latest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(staged_pre_md, latest / "ubo5_tb15_prelineup_confirmation_board.md")
            shutil.copy2(staged_pre_csv, latest / "ubo5_tb15_prelineup_confirmation_board.csv")
            shutil.copy2(
                package / "confirmed_positive_edge_board.md", latest / "ubo5_tb15_board.md"
            )
            shutil.copy2(
                package / "confirmed_positive_edge_board.csv", latest / "ubo5_tb15_board.csv"
            )
        stage_timer(timings, "scoring_and_board_seconds", started)
        summary.update({
            "status": status,
            "snapshot_timestamp_utc": captured.isoformat(),
            "unstarted_games": len({int(row["game_id"]) for row in unstarted}),
            "two_sided_betonline_tb15_markets": len(unstarted),
            "confirmed_order_rows": confirmed,
            "unconfirmed_order_rows": unconfirmed,
            "CONFIRM": int(counts.get("CONFIRM", 0)),
            "LIKELY_CONFIRM_IF_STARTING": int(counts.get("LIKELY CONFIRM IF STARTING", 0)),
            "PASS": int(counts.get("PASS", 0)),
            "WAIT_FOR_LINEUP": int(counts.get("WAIT FOR LINEUP", 0)),
            "confirmed_positive_over_edge_rows": len(confirmed_board),
            "consensus_positive_rows": sum(
                str(row.get("consensus_positive_flag")).lower() == "true"
                for row in routes
            ),
            "consensus_governed_population_rows": consensus_manifest["selection_count"],
            "complete_run_snapshot_rows": len(routes) + unconfirmed,
            "complete_run_snapshot_count": run_population_manifest["run_snapshot_count"],
            "identity_rejects": identity_rejects,
            "flatten_counts": flatten_counts,
            "resolve_counts": resolve_counts,
            "latest_aliases_updated": aliases_updated,
        })
    except Exception as exc:
        failure = exc
        summary["status"] = "FAILED"
        summary["error"] = safe_error(exc)
    finally:
        timings["total_runtime_seconds"] = round(time.monotonic() - total_started, 3)
        summary["timings"] = timings
        (package / "refresh_summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n"
        )
    print(f"Run tag: {run_tag}")
    print(f"Snapshot time: {summary.get('snapshot_timestamp_utc', summary['capture_timestamp_utc'])}")
    print(f"Runtime: {timings['total_runtime_seconds']:.3f} seconds")
    print(f"Status: {summary['status']}")
    if failure is not None:
        print(f"Error: {summary['error']}", file=sys.stderr)
    for label, key in (
        ("Unstarted games", "unstarted_games"),
        ("Two-sided BetOnline TB 1.5 markets", "two_sided_betonline_tb15_markets"),
        ("Confirmed-order rows", "confirmed_order_rows"),
        ("Unconfirmed-order rows", "unconfirmed_order_rows"),
        ("CONFIRM", "CONFIRM"),
        ("LIKELY CONFIRM IF STARTING", "LIKELY_CONFIRM_IF_STARTING"),
        ("PASS", "PASS"),
        ("WAIT FOR LINEUP", "WAIT_FOR_LINEUP"),
        ("Confirmed positive-Over-edge rows", "confirmed_positive_over_edge_rows"),
    ):
        print(f"{label}: {summary.get(key, 0)}")
    for key in (
        "odds_capture_seconds", "lineup_capture_seconds",
        "feature_materialization_seconds", "scoring_and_board_seconds",
        "total_runtime_seconds",
    ):
        print(f"{key.replace('_', ' ')}: {timings.get(key, 0):.3f}")
    if summary.get("latest_aliases_updated"):
        print(
            "Pre-lineup board: "
            "backend/mlb/exports/model_v2/ubo5_tb15/latest/"
            "ubo5_tb15_prelineup_confirmation_board.md"
        )
        print(
            "Confirmed positive-edge board: "
            "backend/mlb/exports/model_v2/ubo5_tb15/latest/ubo5_tb15_board.md"
        )
    else:
        print(f"Pre-lineup board: {package.relative_to(ROOT)}/prelineup_confirmation_board.md")
        print(
            "Confirmed positive-edge board: "
            f"{package.relative_to(ROOT)}/confirmed_positive_edge_board.md"
        )
        print("Latest aliases: PRESERVED (invocation did not evaluate current markets)")
    return 1 if failure is not None else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="MLB slate date, YYYY-MM-DD")
    args = parser.parse_args()
    datetime.strptime(args.date, "%Y-%m-%d")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
