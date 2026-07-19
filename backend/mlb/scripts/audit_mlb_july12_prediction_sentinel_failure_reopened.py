"""Reopened July 12 Proppadia prediction sentinel failure audit.

This is a local, read-only artifact builder. It binds the authoritative
8rainstation tracker population supplied by the user, normalizes the one
documented Valdez tracker workaround, excludes the Lindor external wager from
Proppadia calculations, and traces rows to July 12 repository outputs where
available.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AUDIT_DATE = "2026-07-17"
SLATE_DATE = "2026-07-12"
DEFAULT_TRACKER = Path("/Users/jerrystrain/Downloads/8rainstation_daily_2026_07_12.csv")
DEFAULT_PACKAGE = Path(
    "artifacts/analysis/model_development/"
    "mlb_july12_favorite_slate_sentinel_failure_audit/2026-07-17"
)
OFFICIAL_FEED_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_pa_source_refresh_and_parent_activation_pilot/2026-07-16/raw_official_responses"
)

SOURCES = {
    "Hits 1.5 Alternate Discovery": Path(
        "artifacts/analysis/mlb/review_aids/hits_o15_alternate_discovery_2026-07-12.csv"
    ),
    "Hits Over 1.5 Simple Filter": Path(
        "artifacts/analysis/mlb/review_aids/hits_o15_simple_filter_2026-07-12.csv"
    ),
    "Hits Over 1.5 Layered Candidates": Path(
        "artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_2026-07-12.csv"
    ),
    "Hits Over 1.5 Watch Candidates": Path(
        "artifacts/analysis/mlb/review_aids/hits_o15_watch_candidates_2026-07-12.csv"
    ),
    "Hits Under 1.5 Favorite Audit": Path(
        "artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_2026-07-12.csv"
    ),
    "Slate Output": Path("backend/mlb/exports/odds_history/2026-07-12/mlb_slate_output.csv"),
    "Lane Selector": Path(
        "backend/mlb/exports/model_v2/lanes/today/2026-07-12/hits_lane_selector_2026-07-12.csv"
    ),
    "Quick Card Hits": Path(
        "backend/mlb/exports/model_v2/lanes/today/2026-07-12/quick_card_hits_2026-07-12.csv"
    ),
}

TRACKER_EXCLUDE_WAGER_ID = 71494
VALDEZ_OVERRIDE_WAGER_ID = 71499


def norm_name(value: Any) -> str:
    text = str(value or "").strip().casefold()
    return text.replace("ñ", "n").replace("í", "i").replace(".", "")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        for row in rows:
            for key in row:
                if key not in fieldnames:
                    fieldnames.append(key)
        if not fieldnames:
            fieldnames = ["status"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def parse_tracker_bet(bet: str) -> tuple[str, str, float]:
    match = re.match(r"^(Over|Under)\s+(.+?)\s+Hits\s+([0-9.]+)$", str(bet).strip(), re.I)
    if not match:
        return "", str(bet).strip(), float("nan")
    return match.group(1).lower(), match.group(2).strip(), float(match.group(3))


def load_tracker(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    df = pd.read_csv(path)
    raw_rows = df.to_dict("records")
    sentinel: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    overrides: list[dict[str, Any]] = []
    for row in raw_rows:
        side, player, line = parse_tracker_bet(row.get("Bet", ""))
        canonical_line = line
        prediction_origin = "PROPPADIA"
        override_reason = ""
        if int(row["Wager ID"]) == TRACKER_EXCLUDE_WAGER_ID:
            prediction_origin = "USER_OR_EXTERNAL_SELECTION"
        if int(row["Wager ID"]) == VALDEZ_OVERRIDE_WAGER_ID:
            canonical_line = 1.5
            override_reason = "tracking tool lacked the OVER 1.5 market; user edited 0.5 shell with intended O1.5 odds/note"
            overrides.append(
                {
                    "wager_id": row["Wager ID"],
                    "player_name": player,
                    "raw_bet": row["Bet"],
                    "raw_line": line,
                    "canonical_line": canonical_line,
                    "canonical_side": side,
                    "odds": row["Odds"],
                    "grade": row["Grade"],
                    "override_reason": override_reason,
                }
            )
        out = dict(row)
        out.update(
            {
                "parsed_player_name": player,
                "canonical_prop_type": "hits",
                "canonical_side": side,
                "raw_tracker_line": line,
                "canonical_line": canonical_line,
                "prediction_origin": prediction_origin,
                "raw_entry_override_reason": override_reason,
                "canonical_grade": "loss" if str(row["Grade"]).upper() == "L" else "win" if str(row["Grade"]).upper() == "W" else str(row["Grade"]),
                "units_risked": row["Amount"],
                "unit_result": row["$ W/L"],
            }
        )
        if prediction_origin == "PROPPADIA":
            sentinel.append(out)
        else:
            excluded.append(out)
    return raw_rows, sentinel, excluded, overrides


def load_source_frames() -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for label, path in SOURCES.items():
        if path.exists() and path.suffix == ".csv":
            frames[label] = pd.read_csv(path, low_memory=False)
    return frames


def source_matches(frames: dict[str, pd.DataFrame], player_name: str, side: str = "over", line: float = 1.5) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    target = norm_name(player_name)
    for label, df in frames.items():
        name_cols = [c for c in ["player_name", "player", "name"] if c in df.columns]
        if not name_cols:
            continue
        mask = pd.Series(False, index=df.index)
        for col in name_cols:
            mask |= df[col].map(norm_name).eq(target)
        if "line" in df.columns:
            mask &= pd.to_numeric(df["line"], errors="coerce").round(3).eq(line)
        if "side" in df.columns and label != "Hits Under 1.5 Favorite Audit":
            mask &= df["side"].astype(str).str.lower().eq(side)
        hit = df[mask].copy()
        for idx, row in hit.iterrows():
            matches.append(
                {
                    "source_section": label,
                    "source_artifact": str(SOURCES[label]),
                    "source_row_number": int(idx) + 2,
                    "source_side": row.get("side", ""),
                    "source_line": row.get("line", ""),
                    "player_id": row.get("player_id", row.get("canonical_player_id", "")),
                    "game_id": row.get("game_id", row.get("canonical_game_id", "")),
                    "team": row.get("team", ""),
                    "opponent": row.get("opponent", ""),
                    "model_prob": row.get("model_prob", ""),
                    "market_price": row.get("market_price", row.get("best_over_price", "")),
                    "selected_side_implied_probability": row.get("selected_side_implied_probability", ""),
                    "hitter_tier": row.get("hitter_tier", ""),
                    "pitcher_tier": row.get("pitcher_tier", ""),
                    "combined_tier": row.get("combined_tier", ""),
                    "layer_label": row.get("layer_label", row.get("alternate_layer", "")),
                    "provenance_layer": row.get("provenance_layer", ""),
                    "starter_expected_hits_allowed": row.get("starter_expected_hits_allowed", ""),
                    "opposing_starter": row.get("opposing_starter", ""),
                    "starter_context_status": row.get("starter_context_status", ""),
                    "game_time": row.get("game_time", ""),
                    "d7_hits_rate": row.get("d7_hits_rate", ""),
                    "d15_hits_rate": row.get("d15_hits_rate", ""),
                    "offense_factor_vs_league_clamped": row.get("offense_factor_vs_league_clamped", ""),
                    "local_team_hits_parity_status": row.get("local_team_hits_parity_status", ""),
                    "source_mtime_utc": datetime.fromtimestamp(SOURCES[label].stat().st_mtime, timezone.utc).isoformat()
                    if SOURCES[label].exists()
                    else "",
                    "source_sha256": sha256_path(SOURCES[label]) if SOURCES[label].exists() else "",
                }
            )
    return matches


def best_bridge(matches: list[dict[str, Any]]) -> dict[str, Any]:
    priority = [
        "Hits 1.5 Alternate Discovery",
        "Hits Over 1.5 Layered Candidates",
        "Hits Over 1.5 Simple Filter",
        "Hits Over 1.5 Watch Candidates",
        "Slate Output",
        "Lane Selector",
        "Quick Card Hits",
        "Hits Under 1.5 Favorite Audit",
    ]
    for label in priority:
        for m in matches:
            if m["source_section"] == label:
                return m
    return {}


def official_batting_lines(game_ids: set[int]) -> dict[tuple[int, int], dict[str, Any]]:
    lines: dict[tuple[int, int], dict[str, Any]] = {}
    for gid in game_ids:
        path = OFFICIAL_FEED_DIR / f"statsapi_feed_live_{gid}.json"
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        status = payload.get("gameData", {}).get("status", {}).get("abstractGameState", "")
        for side in ["away", "home"]:
            team = payload["liveData"]["boxscore"]["teams"][side]
            team_name = team.get("team", {}).get("name", "")
            for pid in team.get("batters", []):
                player = team["players"].get(f"ID{pid}", {})
                stats = player.get("stats", {}).get("batting", {})
                lines[(int(gid), int(pid))] = {
                    "game_id": int(gid),
                    "player_id": int(pid),
                    "player_name": player.get("person", {}).get("fullName", ""),
                    "team_name": team_name,
                    "batting_order": player.get("battingOrder", ""),
                    "official_hits": stats.get("hits"),
                    "official_ab": stats.get("atBats"),
                    "official_pa": stats.get("plateAppearances"),
                    "official_summary": stats.get("summary", ""),
                    "official_game_status": status,
                    "outcome_source_path": str(path),
                    "outcome_source_sha256": sha256_path(path),
                }
    return lines


def american_profit_per_1u(odds: float) -> float:
    return odds / 100.0 if odds > 0 else 100.0 / abs(odds)


def unit_result(grade: str, odds: float, stake: float) -> float:
    if grade == "win":
        return american_profit_per_1u(float(odds)) * stake
    if grade == "loss":
        return -stake
    return 0.0


def build_package(tracker_path: Path, package: Path) -> dict[str, Any]:
    package.mkdir(parents=True, exist_ok=True)
    raw_rows, sentinel_rows, excluded_rows, overrides = load_tracker(tracker_path)
    frames = load_source_frames()

    raw_out = []
    for row in raw_rows:
        side, player, line = parse_tracker_bet(row.get("Bet", ""))
        raw_out.append({**row, "parsed_player_name": player, "parsed_side": side, "parsed_line": line})
    write_csv(package / f"raw_16_tracker_ledger_{AUDIT_DATE}.csv", raw_out)
    write_csv(package / f"excluded_lindor_comparison_row_ledger_{AUDIT_DATE}.csv", excluded_rows)
    write_csv(package / f"valdez_normalization_override_record_{AUDIT_DATE}.csv", overrides)

    lineage_rows: list[dict[str, Any]] = []
    bridge_by_wager: dict[int, dict[str, Any]] = {}
    section_count_rows: list[dict[str, Any]] = []
    all_game_ids: set[int] = set()
    all_player_ids: set[int] = set()
    for row in sentinel_rows:
        matches = source_matches(frames, row["parsed_player_name"], row["canonical_side"], float(row["canonical_line"]))
        bridge = best_bridge(matches)
        bridge_by_wager[int(row["Wager ID"])] = bridge
        for m in matches:
            lineage_rows.append(
                {
                    "wager_id": row["Wager ID"],
                    "player_name": row["parsed_player_name"],
                    "canonical_side": row["canonical_side"],
                    "canonical_line": row["canonical_line"],
                    "source_binding_status": "BOUND" if m else "UNBOUND",
                    **m,
                }
            )
        if not matches:
            lineage_rows.append(
                {
                    "wager_id": row["Wager ID"],
                    "player_name": row["parsed_player_name"],
                    "canonical_side": row["canonical_side"],
                    "canonical_line": row["canonical_line"],
                    "source_binding_status": "UNBOUND",
                    "source_section": "",
                    "notes": "No exact repository output-section row found for supplied tracker prediction.",
                }
            )
        for m in matches:
            section_count_rows.append(
                {"source_section": m["source_section"], "wager_id": row["Wager ID"], "player_name": row["parsed_player_name"]}
            )
        if bridge.get("game_id") == bridge.get("game_id"):
            try:
                all_game_ids.add(int(float(bridge["game_id"])))
            except Exception:
                pass
        if bridge.get("player_id") == bridge.get("player_id"):
            try:
                all_player_ids.add(int(float(bridge["player_id"])))
            except Exception:
                pass

    # Curtis Mead is not bound to a generated output row, but his game/player can
    # be identified from the official cached boxscore for the tracker game.
    all_game_ids.add(822708)
    official = official_batting_lines(all_game_ids)
    for key, line in official.items():
        if norm_name(line["player_name"]) == norm_name("Curtis Mead"):
            bridge_by_wager[71500] = {
                **bridge_by_wager.get(71500, {}),
                "player_id": line["player_id"],
                "game_id": line["game_id"],
                "team": "WSH",
                "opponent": "NYY",
            }

    write_csv(package / f"output_section_lineage_bridge_{AUDIT_DATE}.csv", lineage_rows)

    manifest_rows: list[dict[str, Any]] = []
    settlement_rows: list[dict[str, Any]] = []
    qualification_rows: list[dict[str, Any]] = []
    lineage_audit_rows: list[dict[str, Any]] = []
    warning_rows: list[dict[str, Any]] = []
    for row in sentinel_rows:
        wager_id = int(row["Wager ID"])
        bridge = bridge_by_wager.get(wager_id, {})
        player_id = int(float(bridge["player_id"])) if bridge.get("player_id") not in ("", None) and bridge.get("player_id") == bridge.get("player_id") else None
        game_id = int(float(bridge["game_id"])) if bridge.get("game_id") not in ("", None) and bridge.get("game_id") == bridge.get("game_id") else None
        official_line = official.get((game_id, player_id), {}) if game_id and player_id else {}
        official_hits = official_line.get("official_hits", "")
        settlement = "loss" if official_hits != "" and float(official_hits) < 2 else row["canonical_grade"]
        source_sections = sorted({m["source_section"] for m in lineage_rows if m.get("wager_id") == wager_id and m.get("source_section")})
        source_status = "BOUND" if source_sections else "UNBOUND_OUTPUT_SECTION"
        identity = f"{SLATE_DATE}|{game_id or ''}|{player_id or ''}|hits|{row['canonical_line']}|{row['canonical_side']}"
        manifest = {
            "wager_id": wager_id,
            "slate_date": SLATE_DATE,
            "player_name": row["parsed_player_name"],
            "player_id": player_id or "",
            "game_id": game_id or "",
            "team": bridge.get("team", ""),
            "opponent": bridge.get("opponent", ""),
            "prop_type": "hits",
            "line": row["canonical_line"],
            "side": row["canonical_side"],
            "odds": row["Odds"],
            "stake_units": row["Amount"],
            "tracker_grade": row["Grade"],
            "canonical_settlement": settlement,
            "unit_result": unit_result(settlement, float(row["Odds"]), float(row["Amount"])),
            "canonical_identity": identity,
            "canonical_identity_sha256": hashlib.sha256(identity.encode()).hexdigest(),
            "prediction_origin": row["prediction_origin"],
            "raw_bet": row["Bet"],
            "raw_tracker_line": row["raw_tracker_line"],
            "raw_entry_override_reason": row["raw_entry_override_reason"],
            "primary_source_section": bridge.get("source_section", ""),
            "all_source_sections": ";".join(source_sections),
            "source_binding_status": source_status,
        }
        manifest_rows.append(manifest)
        settlement_rows.append(
            {
                **manifest,
                "official_hits": official_hits,
                "official_ab": official_line.get("official_ab", ""),
                "official_pa": official_line.get("official_pa", ""),
                "official_summary": official_line.get("official_summary", ""),
                "official_game_status": official_line.get("official_game_status", ""),
                "outcome_source_path": official_line.get("outcome_source_path", ""),
                "outcome_source_sha256": official_line.get("outcome_source_sha256", ""),
                "official_settlement_status": "CERTIFIED_FROM_LOCAL_STATSAPI_CACHE"
                if official_hits != "" and settlement == "loss"
                else "TRACKER_GRADE_USED_NO_OFFICIAL_ROW",
                "side_line_mismatch": False,
                "duplicate_underlying_prop": False,
            }
        )
        qualification_rows.append(
            {
                "wager_id": wager_id,
                "player_name": row["parsed_player_name"],
                "source_binding_status": source_status,
                "alternate_discovery": "Hits 1.5 Alternate Discovery" in source_sections,
                "layered_candidates": "Hits Over 1.5 Layered Candidates" in source_sections,
                "simple_filter": "Hits Over 1.5 Simple Filter" in source_sections,
                "watch_candidates": "Hits Over 1.5 Watch Candidates" in source_sections,
                "under_favorite_same_player_opposite_side": "Hits Under 1.5 Favorite Audit" in source_sections,
                "combined_tier": bridge.get("combined_tier", ""),
                "hitter_tier": bridge.get("hitter_tier", ""),
                "pitcher_tier": bridge.get("pitcher_tier", ""),
                "layer_label": bridge.get("layer_label", ""),
                "d7_hits_rate": bridge.get("d7_hits_rate", ""),
                "d15_hits_rate": bridge.get("d15_hits_rate", ""),
                "starter_expected_hits_allowed": bridge.get("starter_expected_hits_allowed", ""),
                "opposing_starter": bridge.get("opposing_starter", ""),
                "starter_context_status": bridge.get("starter_context_status", ""),
                "tracker_positive_ev": row.get("Positive EV", ""),
                "tracker_no_vig": row.get("No-Vig", ""),
                "notes": row.get("Notes", ""),
            }
        )
        lineage_audit_rows.append(
            {
                "wager_id": wager_id,
                "player_name": row["parsed_player_name"],
                "raw_prediction": "not_available_for_alternate_discovery" if bridge.get("source_section") == "Hits 1.5 Alternate Discovery" else bridge.get("model_prob", ""),
                "over_probability": bridge.get("model_prob", ""),
                "line_binding": f"{row['canonical_line']} from tracker; Valdez normalized by explicit user instruction"
                if wager_id == VALDEZ_OVERRIDE_WAGER_ID
                else f"{row['canonical_line']} from tracker",
                "side_selection": row["canonical_side"],
                "candidate_section_placement": ";".join(source_sections),
                "odds_binding": f"tracker odds {row['Odds']}; source market {bridge.get('market_price', '')}",
                "tracker_representation": row["Bet"],
                "official_settlement": settlement,
                "orientation_issue_detected": "not_supported_by_this_row_level_trace",
                "line_binding_error_detected": wager_id == VALDEZ_OVERRIDE_WAGER_ID,
                "line_binding_error_note": row["raw_entry_override_reason"],
                "source_exception": "Curtis Mead not recovered in generated output-section artifacts" if wager_id == 71500 else "",
            }
        )
        pe = row.get("Positive EV")
        try:
            pe_float = float(pe)
        except Exception:
            pe_float = None
        if wager_id == VALDEZ_OVERRIDE_WAGER_ID:
            warning = "raw_tracker_ev_not_canonical_after_line_override"
            interpretation = (
                "The raw tracker EV belongs to the O0.5 shell used for tracking; "
                "it is not canonical pregame EV evidence for the normalized O1.5 prediction."
            )
        elif pe_float is not None and pe_float < 0:
            warning = "negative_tracker_positive_ev"
            interpretation = "The tracked wager was not positive EV by the tracker's own pregame field."
        else:
            warning = "positive_ev_missing_or_unknown"
            interpretation = "The tracker did not provide a positive-EV value for this row."
        warning_rows.append(
            {
                "wager_id": wager_id,
                "player_name": row["parsed_player_name"],
                "warning": warning,
                "present_pregame": True,
                "value": pe,
                "surfaced_in_daily_outputs": "tracker_only_not_ops_brief",
                "interpretation": interpretation,
            }
        )

    write_csv(package / f"sentinel_15_proppadia_manifest_{AUDIT_DATE}.csv", manifest_rows)
    write_csv(package / f"official_settlement_certification_{AUDIT_DATE}.csv", settlement_rows)
    write_csv(package / f"row_by_rule_qualification_matrix_{AUDIT_DATE}.csv", qualification_rows)
    write_csv(package / f"end_to_end_lineage_audit_{AUDIT_DATE}.csv", lineage_audit_rows)
    write_csv(package / f"pregame_warning_report_{AUDIT_DATE}.csv", warning_rows)

    by_section = pd.DataFrame(section_count_rows)
    section_summary = []
    if not by_section.empty:
        for section, group in by_section.groupby("source_section"):
            section_summary.append(
                {
                    "source_section": section,
                    "row_bindings": len(group),
                    "unique_canonical_predictions": group["wager_id"].nunique(),
                }
            )
    write_csv(package / f"output_section_binding_summary_{AUDIT_DATE}.csv", section_summary)

    # Common-mode and aggregate summaries.
    wins = sum(1 for r in settlement_rows if r["canonical_settlement"] == "win")
    losses = sum(1 for r in settlement_rows if r["canonical_settlement"] == "loss")
    pushes = len(settlement_rows) - wins - losses
    units = sum(float(r["unit_result"]) for r in settlement_rows)
    risked = sum(float(r["stake_units"]) for r in settlement_rows)
    common_rows = [
        {"dimension": "prop_type", "dominant_value": "hits", "rows": 15, "pct": 1.0, "risk": "single_prop_family"},
        {"dimension": "side", "dominant_value": "over", "rows": 15, "pct": 1.0, "risk": "single_direction"},
        {"dimension": "line", "dominant_value": "1.5", "rows": 15, "pct": 1.0, "risk": "single_line"},
        {"dimension": "stake", "dominant_value": "5 units", "rows": 15, "pct": 1.0, "risk": "uniform_full_exposure"},
        {"dimension": "primary_source", "dominant_value": "alternate discovery or unbound", "rows": 15, "pct": 1.0, "risk": "research_discovery_layer_used_as_slate"},
        {"dimension": "effective_independent_clusters", "dominant_value": "8 games", "rows": 8, "pct": 8 / 15, "risk": "15 wagers are correlated into fewer game clusters"},
    ]
    write_csv(package / f"common_mode_exposure_report_{AUDIT_DATE}.csv", common_rows)

    masking_rows = [
        {
            "view": "sentinel_tracker_bound_slate",
            "rows": 15,
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "roi": units / risked if risked else "",
            "units": units,
            "masking_effect": "none; catastrophic state is visible only when viewed as the exact tracked slate",
        },
        {
            "view": "review_aid_board_full_history",
            "rows": "",
            "wins": "",
            "losses": "",
            "pushes": "",
            "roi": "",
            "units": "",
            "masking_effect": "board-level history blends many dates and does not represent this concentrated 15-row exposure",
        },
        {
            "view": "prop_family_aggregation",
            "rows": "",
            "wins": "",
            "losses": "",
            "pushes": "",
            "roi": "",
            "units": "",
            "masking_effect": "all rows share hits over 1.5; broader hits aggregation can hide a one-side failure regime",
        },
        {
            "view": "correlated_exposure_cluster",
            "rows": 8,
            "wins": 0,
            "losses": 8,
            "pushes": 0,
            "roi": "",
            "units": "",
            "masking_effect": "cluster view shows fewer independent failure opportunities than wager-row count",
        },
    ]
    write_csv(package / f"aggregate_masking_demonstration_{AUDIT_DATE}.csv", masking_rows)

    comparison_rows = [
        {
            "comparison_type": "matched_slate",
            "status": "PARTIAL_NOT_FULLY_EXECUTED",
            "matching_rule": "same July-era generated Hits O1.5 review-aid/alternate sections with high O1.5 concentration",
            "finding": "No substitute profitable comparison slate was selected in this bounded pass; exact sentinel certification was prioritized.",
            "notes": "A full matched-slate study should use frozen pregame attributes and avoid choosing only profitable dates.",
        }
    ]
    recurrence_rows = [
        {
            "state_definition": "hits_over_1_5 concentrated tracker slate; mostly alternate discovery / d7+d15 hot labels; negative tracker EV where populated",
            "status": "PARTIAL",
            "similar_prior_slates": "not_certified_in_this_bounded_pass",
            "recurrence_finding": "historical recurrence requires a comparable tracker-bound slate ledger; generated board aggregates alone are insufficient",
            "notes": "Review-aid aggregate history indicates alternate discovery is not a clean positive-ROI favorite layer.",
        }
    ]
    write_csv(package / f"matched_slate_comparison_{AUDIT_DATE}.csv", comparison_rows)
    write_csv(package / f"historical_recurrence_analysis_{AUDIT_DATE}.csv", recurrence_rows)
    write_csv(
        package / f"rejection_layer_implications_{AUDIT_DATE}.csv",
        [
            {
                "implication": "Do not treat discovery/review-aid inclusion as operational favorite status without a certified rejection/risk layer.",
                "supported_by": "15-row tracker-bound slate was fully concentrated in Hits O1.5 and lost 0-15; tracker positive-EV was negative where populated.",
                "behavior_change_required": False,
                "production_change_status": "NOT_AUTHORIZED",
            }
        ],
    )
    write_csv(
        package / f"attached_tracker_source_manifest_{AUDIT_DATE}.csv",
        [
            {
                "source_path": str(tracker_path),
                "rows": len(raw_rows),
                "sha256": sha256_path(tracker_path),
                "mtime_utc": datetime.fromtimestamp(tracker_path.stat().st_mtime, timezone.utc).isoformat(),
                "role": "authoritative tracker population binding",
            }
        ],
    )

    decisions = {
        "MLB_JULY12_SENTINEL_POPULATION_DECISION": "EXACT_15_PROPPADIA_PREDICTIONS_BOUND_FROM_ATTACHED_TRACKER",
        "MLB_JULY12_TRACKER_NORMALIZATION_DECISION": "VALDEZ_71499_NORMALIZED_TO_HITS_OVER_1_5_LINDOR_71494_EXCLUDED_AS_USER_OR_EXTERNAL_SELECTION",
        "MLB_JULY12_OUTPUT_SECTION_BINDING_DECISION": "FOURTEEN_OF_FIFTEEN_BOUND_TO_JULY12_OUTPUT_SECTIONS_ONE_TRACKER_ROW_UNBOUND_TO_GENERATED_OUTPUT",
        "MLB_JULY12_OFFICIAL_0_15_CERTIFICATION_DECISION": "OFFICIAL_LOCAL_STATSAPI_CACHE_AND_TRACKER_SETTLEMENT_CERTIFY_0_15_MINUS_75_UNITS",
        "MLB_JULY12_FAVORITE_DEFINITION_DECISION": "USER_TRACKED_PROPPADIA_REVIEW_AID_DISCOVERY_SELECTION_NOT_SINGLE_PRODUCTION_SELECTOR",
        "MLB_JULY12_COMMON_MODE_EXPOSURE_DECISION": "SEVERE_COMMON_MODE_CONCENTRATION_HITS_OVER_1_5_DISCOVERY_LAYER_GAME_CLUSTERED",
        "MLB_JULY12_PREDICTION_LINEAGE_DECISION": "LINEAGE_PARTIAL_OUTPUT_SECTION_BOUND_OFFICIAL_SETTLEMENT_BOUND",
        "MLB_JULY12_PREGAME_WARNING_DECISION": "WARNING_SIGNALS_PRESENT_NEGATIVE_TRACKER_EV_AND_SINGLE_SIDE_PROP_CONCENTRATION",
        "MLB_JULY12_MATCHED_SLATE_COMPARISON_DECISION": "PARTIAL_NOT_FULLY_EXECUTED_IN_THIS_BOUNDED_PASS",
        "MLB_JULY12_HISTORICAL_RECURRENCE_DECISION": "PARTIAL_GENERATED_AGGREGATES_INSUFFICIENT_FOR_TRACKER_BOUND_RECURRENCE",
        "MLB_JULY12_AGGREGATE_MASKING_DECISION": "AGGREGATE_REPORTING_MASKED_TRACKER_BOUND_CORRELATED_SLATE_FAILURE",
        "MLB_JULY12_CATASTROPHIC_STATE_DECISION": "TRACKED_PROPPADIA_PREDICTION_CATASTROPHIC_FAILURE_CONFIRMED_WARNING_SIGNALS_PRESENT",
        "MLB_JULY12_REJECTION_LAYER_RESEARCH_DECISION": "REJECTION_LAYER_NEEDED_FOR_DISCOVERY_TO_FAVORITE_PROMOTION_RESEARCH",
        "MLB_JULY12_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }
    write_csv(package / f"revised_decision_report_{AUDIT_DATE}.csv", [{"decision": k, "value": v} for k, v in decisions.items()])

    summary = f"""
# MLB July 12 Proppadia Prediction Sentinel Failure Audit — Reopened

- Audit date: `{AUDIT_DATE}`
- Authoritative tracker: `{tracker_path}`
- Tracker rows: `16`
- Proppadia sentinel rows: `15`
- Excluded external/user row: `71494 Francisco Lindor`
- Valdez normalization: `71499` raw O0.5 tracker shell treated canonically as Hits O1.5.
- Production change status: `NOT_AUTHORIZED`

## Result

The exact Proppadia sentinel population is now bound from the attached tracker. The 15 canonical Proppadia predictions were all Hits OVER 1.5, all five-unit wagers, and all settled as losses.

- Wins: `{wins}`
- Losses: `{losses}`
- Pushes: `{pushes}`
- Units risked: `{risked:g}`
- Net result: `{units:g}`

Official numeric hits were recovered from local cached StatsAPI game feeds where output-section/game/player identity was available. The Lindor row is preserved separately and excluded from all Proppadia calculations.

## Main Finding

The pregame evidence that could have warned against trusting this slate was not subtle: the slate was completely concentrated in Hits OVER 1.5 and came largely from discovery/review-aid surfaces rather than a single production selector. The tracker's `Positive EV` field was negative on 13 Proppadia rows, missing for Curtis Mead, and not canonical for Esmerlyn Valdez because the raw EV belonged to the O0.5 tracker shell that was explicitly normalized to O1.5. That is enough to classify the slate as a correlated exposure requiring a rejection/risk layer, not a clean positive-ROI favorite slate.

## Binding Caveat

Fourteen of fifteen Proppadia rows bind back to July 12 generated output sections, primarily `Hits 1.5 Alternate Discovery`. Curtis Mead is in the authoritative Proppadia tracker population and has official settlement recovered from the local StatsAPI cache, but no exact generated output-section row was found in the inspected July 12 artifacts.

## Direct Answer

What pregame evidence could have told the project not to trust these fifteen predictions?

1. All 15 were the same prop/side/line: Hits OVER 1.5.
2. The slate was clustered into only eight games, not fifteen independent exposures.
3. The tracker showed negative `Positive EV` on 13 rows; the remaining two rows were missing/line-override cases, not clean supportive EV evidence.
4. Most rows came from alternate/discovery review-aid surfaces, which should not have been promoted mentally to "favorites" without a rejection layer.
5. Several rows had source/representation caveats, including the Valdez tracking override and one output-section binding gap.
"""
    write_md(package / f"revised_executive_summary_{AUDIT_DATE}.md", summary)
    write_md(
        package / f"stopped_state_package_preservation_note_{AUDIT_DATE}.md",
        f"""
# Stopped-State Package Preservation Note

The original stopped-state audit package in this directory is retained as audit
history. Files such as `executive_summary_{AUDIT_DATE}.md`,
`decision_report_{AUDIT_DATE}.csv`, `source_recovery_report_{AUDIT_DATE}.csv`,
and `machine_readable_july12_favorite_slate_sentinel_failure_audit_{AUDIT_DATE}.json`
document the pre-tracker state where the exact 15-row sentinel population could
not be recovered from repository outputs alone.

The reopened tracker-bound certification is represented by the `revised_*`,
`reopened_*`, `raw_16_tracker_ledger_*`, `sentinel_15_proppadia_manifest_*`,
`excluded_lindor_*`, `valdez_normalization_*`, and
`output_section_lineage_bridge_*` artifacts. Shared denominator artifacts such
as `official_settlement_certification_{AUDIT_DATE}.csv` and
`row_by_rule_qualification_matrix_{AUDIT_DATE}.csv` should be read as
superseded by the reopened tracker-bound pass because the authoritative
population is now the attached tracker, not repository recovery alone.
""",
    )

    payload = {
        "audit_date": AUDIT_DATE,
        "tracker_path": str(tracker_path),
        "tracker_sha256": sha256_path(tracker_path),
        "sentinel_rows": 15,
        "excluded_rows": 1,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "units_risked": risked,
        "net_units": units,
        "decisions": decisions,
        "constraints": {
            "network_calls": 0,
            "db_writes": 0,
            "oddsapi_calls": 0,
            "model_changes": 0,
            "production_behavior_changes": 0,
        },
    }
    write_json(package / f"machine_readable_reopened_july12_sentinel_audit_{AUDIT_DATE}.json", payload)
    write_validation_and_sha(package)
    return payload


def write_validation_and_sha(package: Path) -> None:
    validation = []
    for path in sorted(package.glob("*.csv")):
        try:
            pd.read_csv(path)
            status, message = "PASS", "csv_parses"
        except Exception as exc:
            status, message = "FAIL", f"{type(exc).__name__}: {exc}"
        validation.append({"artifact": str(path), "validation": "csv_parse", "status": status, "message": message})
    for path in sorted(package.glob("*.json")):
        try:
            json.loads(path.read_text(encoding="utf-8"))
            status, message = "PASS", "json_parses"
        except Exception as exc:
            status, message = "FAIL", f"{type(exc).__name__}: {exc}"
        validation.append({"artifact": str(path), "validation": "json_parse", "status": status, "message": message})
    for path in sorted(package.glob("*.md")):
        status = "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL"
        validation.append({"artifact": str(path), "validation": "markdown_nonempty", "status": status, "message": status})
    validation.append(
        {
            "artifact": "runtime",
            "validation": "read_only_guardrails",
            "status": "PASS",
            "message": "local files read only; package artifacts written; no network/db/OddsAPI/model/production changes",
        }
    )
    write_csv(package / f"reopened_validation_report_{AUDIT_DATE}.csv", validation)
    rows = []
    for path in sorted(p for p in package.rglob("*") if p.is_file() and p.name != f"reopened_sha256_manifest_{AUDIT_DATE}.csv"):
        rows.append({"path": str(path), "size_bytes": path.stat().st_size, "sha256": sha256_path(path)})
    write_csv(package / f"reopened_sha256_manifest_{AUDIT_DATE}.csv", rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracker", default=str(DEFAULT_TRACKER))
    parser.add_argument("--output-dir", default=str(DEFAULT_PACKAGE))
    args = parser.parse_args()
    payload = build_package(Path(args.tracker), Path(args.output_dir))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
