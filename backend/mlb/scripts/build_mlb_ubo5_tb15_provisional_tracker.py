#!/usr/bin/env python3
"""Build the presentation-only UBO-5 TB1.5 pre-lineup watchlist and history."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import (
    canonical_game, canonical_player_name, implied, number, price_index, team_name_map,
)
from backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features import (
    FEATURES, MODEL_SUPPORTED_NULL_FEATURES,
)
from backend.mlb.shared.ubo5_tb15_production_route import ARTIFACT_SHA256, sha256_file

ROOT = Path(__file__).resolve().parents[3]
OBSERVATION_FIELDS = [
    "slate_date", "run_tag", "snapshot_timestamp_utc", "player_name", "game_pk",
    "game", "batter_mlb_id", "lineup_status", "batting_order_if_known",
    "ubo5_over_probability", "BetOnline_over_price", "BetOnline_under_price",
    "no_vig_over_probability", "over_edge_percentage_points", "positive_edge_flag",
    "first_pitch_timestamp", "minutes_before_first_pitch", "score_status",
    "exclusion_reason", "feature_vector_sha256",
]
BOARD_FIELDS = [
    "player_name", "game", "lineup_status", "ubo5_over_probability",
    "no_vig_over_probability", "over_edge_percentage_points",
]


def utc(value: object) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True, errors="coerce")


def bool_value(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def fmt_probability(value: object) -> str:
    parsed = number(value)
    return "—" if parsed is None else f"{parsed * 100:.2f}%"


def fmt_edge(value: object) -> str:
    parsed = number(value)
    return "—" if parsed is None else f"{parsed:+.2f} pp"


def market_rows(snapshot: dict, wide: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    records = wide.loc[wide["prop_type"].eq("total_bases")].copy()
    records = records.drop_duplicates(["game_id", "player_id"], keep=False)
    names = team_name_map(records.fillna("").astype(str).to_dict("records"))
    prices = price_index(snapshot, names)
    by_exact = {}
    for row in records.to_dict("records"):
        game = canonical_game(f"{row.get('away_team_code')} @ {row.get('home_team_code')}")
        key = (game, canonical_player_name(row.get("player_name")), "1.5")
        by_exact.setdefault(key, []).append(row)
    matched, rejected = [], []
    for (game, name, line), price in sorted(prices.items()):
        if line != "1.5" or price.get("over") is None or price.get("under") is None:
            continue
        identities = by_exact.get((canonical_game(game), canonical_player_name(name), line), [])
        if len(identities) != 1:
            rejected.append({"player_name": name, "game": game, "reason": "EXACT_WIDE_IDENTITY_NOT_UNIQUE"})
            continue
        row = identities[0] | {"game": game, "over_price": price["over"], "under_price": price["under"],
                               "price_timestamp": price.get("timestamp", "")}
        matched.append(row)
    return matched, rejected


def lineup_context(player_path: Path, team_path: Path) -> tuple[dict, dict]:
    players = pd.read_csv(player_path) if player_path.is_file() else pd.DataFrame()
    teams = pd.read_csv(team_path) if team_path.is_file() else pd.DataFrame()
    player_map, team_map = {}, {}
    for row in players.to_dict("records"):
        player_map[(int(row["game_id"]), int(row["player_id"]))] = row
    for row in teams.to_dict("records"):
        team_map[(int(row["game_id"]), str(row["team"]))] = row
    return player_map, team_map


def status_for(row: dict, player_map: dict, team_map: dict) -> tuple[str, float | None, dict | None]:
    key = (int(row["game_id"]), int(row["player_id"]))
    player = player_map.get(key)
    if player:
        slot = number(player.get("lineup_slot"))
        confirmed = bool_value(player.get("confirmed_lineup_starter_flag"))
        return ("LINEUP_CONFIRMED" if confirmed else "LINEUP_UNCONFIRMED"), slot, player
    team = team_map.get((int(row["game_id"]), str(row["team"])))
    if team and str(team.get("lineup_status")) == "confirmed_full":
        return "LINEUP_NOT_STARTING", None, None
    return "LINEUP_STATUS_UNKNOWN", None, None


def score_candidates(
    date: str, run_tag: str, now: pd.Timestamp, markets: list[dict], player_map: dict,
    team_map: dict, normalized_root: Path, artifact: Path, work_dir: Path,
) -> tuple[dict[tuple[int, int], dict], dict[tuple[int, int], tuple[str, float | None]]]:
    candidates, contexts = [], {}
    for row in markets:
        status, slot, player = status_for(row, player_map, team_map)
        key = (int(row["game_id"]), int(row["player_id"]))
        contexts[key] = (status, slot)
        start = utc(row.get("game_time"))
        if status == "LINEUP_NOT_STARTING" or slot is None or pd.isna(start) or now >= start:
            continue
        certified = status == "LINEUP_CONFIRMED"
        candidates.append({
            "slate_date": date, "game_pk": key[0], "batter_mlb_id": key[1],
            "team": row["team"], "opponent": row["opponent"],
            "home_away": "home" if bool_value(row.get("is_home")) else "away",
            "prediction_timestamp_utc": now.isoformat(), "scheduled_start_utc": start.isoformat(),
            "lineup_certified": certified,
            "lineup_certified_at_utc": (player or {}).get("source_fetched_at_utc", "") if certified else "",
            "batting_order_position": slot, "line": 1.5, "run_tag": run_tag,
            "opposing_starter_id": (player or {}).get("opposing_starter_id", ""),
            "batter_identity_certified": True, "identity_ambiguous": False,
            "source_lineage_pointer": str(work_dir), "market_row_certified": True,
        })
    if not candidates:
        return {}, contexts
    work_dir.mkdir(parents=True, exist_ok=True)
    candidate_path, feature_path = work_dir / "candidate_ledger.csv", work_dir / "feature_ledger.parquet"
    pd.DataFrame(candidates).to_csv(candidate_path, index=False)
    subprocess.run([
        sys.executable, "-m", "backend.mlb.scripts.materialize_mlb_ubo5_strict_prior_features",
        "--normalized-root", str(normalized_root), "--candidate-file", str(candidate_path),
        "--output", str(feature_path), "--allow-unconfirmed-provisional",
    ], cwd=ROOT, check=True)
    features = pd.read_parquet(feature_path)
    scored: dict[tuple[int, int], dict] = {}
    if not artifact.is_file() or sha256_file(artifact) != ARTIFACT_SHA256:
        for row in features.to_dict("records"):
            row["exclusion_reason"] = "ARTIFACT_MISSING_OR_HASH_MISMATCH"
            scored[(int(row["game_pk"]), int(row["batter_mlb_id"]))] = row
        return scored, contexts
    eligible = (
        features["exclusion_reason"].fillna("").eq("")
        & pd.to_numeric(features["history_depth_pa"], errors="coerce").ge(100)
        & features["feature_completeness_status"].isin(["COMPLETE", "COMPLETE_WITH_MODEL_SUPPORTED_NULLS"])
        & features["temporal_integrity_status"].eq("PASS")
    )
    bundle = joblib.load(artifact)
    if list(bundle.get("features", [])) != list(FEATURES):
        raise RuntimeError("FROZEN_FEATURE_ORDER_MISMATCH")
    indicators = list(bundle["model"].named_steps["simpleimputer"].indicator_.features_)
    if indicators != [FEATURES.index(name) for name in MODEL_SUPPORTED_NULL_FEATURES]:
        raise RuntimeError("FROZEN_NULL_INDICATOR_CONTRACT_MISMATCH")
    if eligible.any():
        probs = bundle["model"].predict_proba(features.loc[eligible, FEATURES])
        classes = list(bundle["model"].classes_)
        over = [1 - dict(zip(classes, p)).get(0, 0) - dict(zip(classes, p)).get(1, 0) for p in probs]
        features.loc[eligible, "ubo5_over_probability"] = over
    for row in features.to_dict("records"):
        if not row.get("exclusion_reason") and number(row.get("history_depth_pa")) is not None and number(row["history_depth_pa"]) < 100:
            row["exclusion_reason"] = "STRICT_PRIOR_PA_LT_100"
        scored[(int(row["game_pk"]), int(row["batter_mlb_id"]))] = row
    return scored, contexts


def classification(group: pd.DataFrame) -> str:
    first = group.iloc[0]
    later = group.iloc[1:]
    if (group.lineup_status == "LINEUP_NOT_STARTING").any():
        return "PLAYER_NOT_IN_STARTING_LINEUP"
    if later.empty:
        return "GAME_STARTED_BEFORE_COMPARISON"
    valid = later[pd.to_numeric(later.over_edge_percentage_points, errors="coerce").notna()]
    if valid.empty:
        return "PRICE_COVERAGE_LOST"
    confirmed = group[group.lineup_status == "LINEUP_CONFIRMED"]
    before_confirm = valid if confirmed.empty else valid[valid.snapshot_timestamp_utc < confirmed.iloc[0].snapshot_timestamp_utc]
    if not before_confirm.empty and number(before_confirm.iloc[-1].over_edge_percentage_points) <= 0:
        return "EDGE_DISAPPEARED_BEFORE_LINEUP"
    if not confirmed.empty:
        edge = number(confirmed.iloc[0].over_edge_percentage_points)
        if edge is not None and edge <= 0:
            return "EDGE_DISAPPEARED_AFTER_LINEUP"
        if edge is not None and edge < number(first.over_edge_percentage_points):
            return "EDGE_SHRANK_BUT_REMAINED_POSITIVE"
        if edge is not None and edge > 0:
            return "EDGE_PERSISTED_AFTER_LINEUP"
    return "MARKET_REMOVED"


def aggregate_summary(history: pd.DataFrame) -> tuple[str, list[dict]]:
    history = history.sort_values(["game_pk", "batter_mlb_id", "snapshot_timestamp_utc"])
    transitions = []
    for _, group in history.groupby(["game_pk", "batter_mlb_id"], sort=False):
        first = group.iloc[0]
        if not bool_value(first.positive_edge_flag):
            continue
        valid = group[pd.to_numeric(group.over_edge_percentage_points, errors="coerce").notna()]
        confirmed = group[group.lineup_status == "LINEUP_CONFIRMED"]
        pre = group[group.lineup_status != "LINEUP_CONFIRMED"]
        last = valid.iloc[-1] if not valid.empty else group.iloc[-1]
        conf = confirmed.iloc[0] if not confirmed.empty else None
        transitions.append({
            "player": first.player_name, "game": first.game, "classification": classification(group),
            "first_edge": number(first.over_edge_percentage_points),
            "largest_edge": pd.to_numeric(valid.over_edge_percentage_points, errors="coerce").max() if not valid.empty else np.nan,
            "last_prelineup_edge": number(pre.iloc[-1].over_edge_percentage_points) if not pre.empty else None,
            "lineup_edge": number(conf.over_edge_percentage_points) if conf is not None else None,
            "final_edge": number(last.over_edge_percentage_points),
            "over_price_change": number(last.BetOnline_over_price) - number(first.BetOnline_over_price),
            "under_price_change": number(last.BetOnline_under_price) - number(first.BetOnline_under_price),
            "no_vig_change_pp": (number(last.no_vig_over_probability) - number(first.no_vig_over_probability)) * 100,
            "ubo5_change_pp": (number(last.ubo5_over_probability) - number(first.ubo5_over_probability)) * 100,
        })
    firsts = history.groupby(["game_pk", "batter_mlb_id"], sort=False).first().reset_index()
    scorable = firsts[pd.to_numeric(firsts.ubo5_over_probability, errors="coerce").notna()]
    positive = firsts[firsts.positive_edge_flag.map(bool_value)]
    confirmed_ids = set(zip(history.loc[history.lineup_status.eq("LINEUP_CONFIRMED"), "game_pk"], history.loc[history.lineup_status.eq("LINEUP_CONFIRMED"), "batter_mlb_id"]))
    absent_ids = set(zip(history.loc[history.lineup_status.eq("LINEUP_NOT_STARTING"), "game_pk"], history.loc[history.lineup_status.eq("LINEUP_NOT_STARTING"), "batter_mlb_id"]))
    t = pd.DataFrame(transitions)
    def mean_median(column: str) -> tuple[str, str]:
        values = pd.to_numeric(t.get(column, pd.Series(dtype=float)), errors="coerce").dropna()
        return (f"{values.median():+.2f} pp", f"{values.mean():+.2f} pp") if len(values) else ("N/A", "N/A")
    first_med, first_mean = mean_median("first_edge")
    conf_med, conf_mean = mean_median("lineup_edge")
    final_med, final_mean = mean_median("final_edge")
    classes = t.classification.value_counts().to_dict() if len(t) else {}
    plus = sum(number(v) is not None and number(v) > 0 for v in positive.BetOnline_over_price)
    favorite = sum(number(v) is not None and number(v) < 0 for v in positive.BetOnline_over_price)
    over_move = pd.to_numeric(t.get("over_price_change", pd.Series(dtype=float)), errors="coerce").dropna()
    lines = [
        "# UBO-5 TB 1.5 Intraday Edge Summary", "",
        "This is a provisional observation ledger. The confirmed-lineup board remains the production authority.", "",
        f"- Early two-sided BetOnline markets: **{len(firsts)}**",
        f"- Early scorable UBO-5 rows: **{len(scorable)}**",
        f"- Early positive-edge rows: **{len(positive)}**",
        f"- Players later confirmed starting: **{len(confirmed_ids)}**",
        f"- Players later absent from lineup: **{len(absent_ids)}**",
        f"- Edges still positive at lineup confirmation: **{classes.get('EDGE_PERSISTED_AFTER_LINEUP', 0) + classes.get('EDGE_SHRANK_BUT_REMAINED_POSITIVE', 0)}**",
        f"- Edges lost before lineup confirmation: **{classes.get('EDGE_DISAPPEARED_BEFORE_LINEUP', 0)}**",
        f"- Edges lost after lineup confirmation: **{classes.get('EDGE_DISAPPEARED_AFTER_LINEUP', 0)}**",
        f"- First-observation edge median / mean: **{first_med} / {first_mean}**",
        f"- Lineup-confirmation edge median / mean: **{conf_med} / {conf_mean}**",
        f"- Final-pregame edge median / mean: **{final_med} / {final_mean}**",
        f"- Median Over price movement: **{over_move.median():+.0f}**" if len(over_move) else "- Median Over price movement: **N/A**",
        f"- Initially positive plus-money Over rows: **{plus}**",
        f"- Initially positive favorite-price Over rows: **{favorite}**", "",
        "## Initially positive row transitions", "",
    ]
    if not transitions:
        lines.append("*None*")
    else:
        lines += [
            "| Player | Game | Classification | First edge | Largest edge | Last pre-lineup | Lineup edge | Final edge | Over Δ | Under Δ | No-vig Δ | UBO-5 Δ |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
        for row in transitions:
            lines.append(
                f"| {row['player']} | {row['game']} | {row['classification']} | "
                f"{fmt_edge(row['first_edge'])} | {fmt_edge(row['largest_edge'])} | "
                f"{fmt_edge(row['last_prelineup_edge'])} | {fmt_edge(row['lineup_edge'])} | "
                f"{fmt_edge(row['final_edge'])} | {row['over_price_change']:+.0f} | "
                f"{row['under_price_change']:+.0f} | {fmt_edge(row['no_vig_change_pp'])} | "
                f"{fmt_edge(row['ubo5_change_pp'])} |"
            )
    return "\n".join(lines) + "\n", transitions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--run-tag", required=True)
    ap.add_argument("--odds-json", required=True, type=Path)
    ap.add_argument("--wide-csv", required=True, type=Path)
    ap.add_argument("--lineup-csv", required=True, type=Path)
    ap.add_argument("--lineup-team-summary", required=True, type=Path)
    ap.add_argument("--normalized-root", required=True, type=Path)
    ap.add_argument("--artifact", required=True, type=Path)
    ap.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15", type=Path)
    args = ap.parse_args()
    snapshot = json.loads(args.odds_json.read_text())
    captured = utc(snapshot.get("captured_at_utc") or datetime.now(timezone.utc).isoformat())
    wide = pd.read_csv(args.wide_csv)
    markets, identity_rejects = market_rows(snapshot, wide)
    player_map, team_map = lineup_context(args.lineup_csv, args.lineup_team_summary)
    day_dir = args.output_root / args.date
    immutable_dir = day_dir / "intraday_snapshots"
    safe_tag = "".join(c for c in args.run_tag if c.isalnum() or c in "._-")
    snapshot_path = immutable_dir / f"ubo5_tb15_intraday_observation_{safe_tag}.csv"
    work_dir = immutable_dir / f".work_{safe_tag}"
    scored, contexts = score_candidates(
        args.date, args.run_tag, captured, markets, player_map, team_map,
        args.normalized_root, args.artifact, work_dir,
    )
    observations = []
    for row in markets:
        key = (int(row["game_id"]), int(row["player_id"]))
        status, slot = contexts[key]
        feature = scored.get(key, {})
        ubo = number(feature.get("ubo5_over_probability"))
        oi, ui = implied(row["over_price"]), implied(row["under_price"])
        nv = oi / (oi + ui) if oi is not None and ui is not None and oi + ui else None
        edge = (ubo - nv) * 100 if ubo is not None and nv is not None else None
        start = utc(row.get("game_time"))
        if status == "LINEUP_NOT_STARTING":
            reason = "PLAYER_NOT_IN_STARTING_LINEUP"
        elif slot is None:
            reason = "MISSING_REQUIRED_BATTING_ORDER_POSITION"
        elif captured >= start:
            reason = "GAME_ALREADY_STARTED"
        else:
            reason = str(feature.get("exclusion_reason") or "")
        observations.append({
            "slate_date": args.date, "run_tag": args.run_tag,
            "snapshot_timestamp_utc": captured.isoformat(), "player_name": row["player_name"],
            "game_pk": key[0], "game": row["game"], "batter_mlb_id": key[1],
            "lineup_status": status, "batting_order_if_known": "" if slot is None else int(slot),
            "ubo5_over_probability": "" if ubo is None else f"{ubo:.10f}",
            "BetOnline_over_price": row["over_price"], "BetOnline_under_price": row["under_price"],
            "no_vig_over_probability": "" if nv is None else f"{nv:.10f}",
            "over_edge_percentage_points": "" if edge is None else f"{edge:.6f}",
            "positive_edge_flag": bool(edge is not None and edge > 0),
            "first_pitch_timestamp": "" if pd.isna(start) else start.isoformat(),
            "minutes_before_first_pitch": "" if pd.isna(start) else f"{(start-captured).total_seconds()/60:.2f}",
            "score_status": "SCORED" if ubo is not None else "NOT_SCORABLE",
            "exclusion_reason": reason, "feature_vector_sha256": feature.get("feature_vector_sha256", ""),
        })
    immutable_dir.mkdir(parents=True, exist_ok=True)
    if snapshot_path.exists():
        existing = snapshot_path.read_bytes()
        temp = snapshot_path.with_suffix(".candidate")
        write_csv(temp, observations, OBSERVATION_FIELDS)
        if existing != temp.read_bytes():
            temp.unlink()
            raise RuntimeError(f"IMMUTABLE_RUN_TAG_COLLISION:{args.run_tag}")
        temp.unlink()
    else:
        write_csv(snapshot_path, observations, OBSERVATION_FIELDS)
    snapshots = sorted(immutable_dir.glob("ubo5_tb15_intraday_observation_*.csv"))
    history = pd.concat([pd.read_csv(path, dtype=str) for path in snapshots], ignore_index=True)
    history = history.sort_values(["snapshot_timestamp_utc", "game_pk", "batter_mlb_id"])
    history_path = day_dir / f"ubo5_tb15_intraday_edge_history_{args.date}.csv"
    history.to_csv(history_path, index=False, columns=OBSERVATION_FIELDS)
    current = pd.DataFrame(observations)
    positive = current[current.positive_edge_flag].copy()
    positive["_edge"] = pd.to_numeric(positive.over_edge_percentage_points)
    positive = positive.sort_values("_edge", ascending=False)
    board_rows = positive[BOARD_FIELDS].to_dict("records")
    board_csv = day_dir / f"ubo5_tb15_provisional_board_{args.date}.csv"
    write_csv(board_csv, board_rows, BOARD_FIELDS)
    board_md = day_dir / f"ubo5_tb15_provisional_board_{args.date}.md"
    lines = [
        f"# UBO-5 TB 1.5 Provisional Watchlist — {args.date}", "",
        f"Run tag: `{args.run_tag}`  ", f"Snapshot: `{captured.isoformat()}`  ",
        "**Provisional only. Confirmed-lineup board remains the production authority.**", "",
        "| Player | Game | Lineup | UBO-5 Over | No-vig Over | Over edge |",
        "| --- | --- | --- | ---: | ---: | ---: |",
    ]
    if not board_rows:
        lines.append("| *None* |  |  |  |  |  |")
    for row in board_rows:
        lines.append(f"| {row['player_name']} | {row['game']} | {row['lineup_status']} | {fmt_probability(row['ubo5_over_probability'])} | {fmt_probability(row['no_vig_over_probability'])} | {fmt_edge(row['over_edge_percentage_points'])} |")
    not_scorable = sum(row["score_status"] != "SCORED" for row in observations)
    lines += ["", "## Readiness", "", f"- Two-sided exact-price markets: **{len(observations)}**",
              f"- Scorable rows: **{len(observations)-not_scorable}**",
              f"- Positive-edge rows: **{len(board_rows)}**",
              f"- Not scorable: **{not_scorable}**",
              f"- Exact identity rejects: **{len(identity_rejects)}**"]
    board_md.write_text("\n".join(lines) + "\n")
    summary_md, _ = aggregate_summary(history)
    summary_path = day_dir / f"ubo5_tb15_intraday_edge_summary_{args.date}.md"
    summary_path.write_text(summary_md)
    latest = args.output_root / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    for source, name in [
        (board_md, "ubo5_tb15_provisional_board.md"), (board_csv, "ubo5_tb15_provisional_board.csv"),
        (history_path, "ubo5_tb15_intraday_edge_history.csv"), (summary_path, "ubo5_tb15_intraday_edge_summary.md"),
    ]:
        shutil.copy2(source, latest / name)
    shutil.rmtree(work_dir, ignore_errors=True)
    payload = {
        "run_tag": args.run_tag, "snapshot_timestamp_utc": captured.isoformat(),
        "two_sided_markets": len(observations),
        "scorable_rows": len(observations) - not_scorable,
        "positive_over_rows": len(board_rows), "identity_rejects": len(identity_rejects),
        "immutable_observation": str(snapshot_path),
    }
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
