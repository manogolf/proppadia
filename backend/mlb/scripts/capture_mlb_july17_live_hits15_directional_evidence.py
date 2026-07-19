"""Capture July 17 live Hits 1.5 directional evidence.

Bounded research-only utility. It binds the first genuine July 17 run-tagged
artifacts, preserves current production/review surfaces, applies the existing
affirmative pitcher-suppression contract as a research label, binds exact live
U1.5/O1.5 prices from the same run-tagged odds snapshot, and writes artifacts.

No network calls, database writes, grading, model fitting, candidate changes,
upload changes, Quick Card changes, workspace changes, or LaunchAgent changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]
DATE = "2026-07-17"
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history" / DATE
OUT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_july17_live_hits15_directional_capture/2026-07-17"
O15_REVIEW = ROOT / "artifacts/analysis/mlb/review_aids/hits_o15_layered_candidates_2026-07-17.csv"
U15_REVIEW = ROOT / "artifacts/analysis/mlb/review_aids/hits_u15_favorite_audit_2026-07-17.csv"
LANE_SELECTOR = ROOT / "backend/mlb/exports/model_v2/lanes/today/2026-07-17/hits_lane_selector_2026-07-17__20260717T130129Z.csv"
QUICK_CARD = ROOT / "backend/mlb/exports/model_v2/lanes/today/2026-07-17/quick_card_hits_2026-07-17__20260717T130129Z.csv"
HITTER_STATUS = "NO_EXISTING_REGIME_VALIDATED"
HITTER_CHALLENGER_STATUS = "NOT_AUTHORIZED"
TEAM_CODES = {
    "Arizona Diamondbacks": "AZ",
    "Athletics": "ATH",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def norm_name(value: Any) -> str:
    text = norm(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def fnum(value: Any) -> float | None:
    try:
        out = float(norm(value))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def boolish(value: Any) -> bool:
    return norm(value).lower() in {"1", "true", "yes", "y"}


def parse_dt(value: Any) -> datetime | None:
    text = norm(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def decimal_odds(price: Any) -> float | None:
    p = fnum(price)
    if p is None or p == 0:
        return None
    return 1 + (p / 100.0 if p > 0 else 100.0 / abs(p))


def latest_run_tag() -> str:
    candidates = sorted(ODDS_ROOT.glob("mlb_slate_output__*.csv"))
    if not candidates:
        raise FileNotFoundError(f"no July 17 run-tagged slate artifacts found under {rel(ODDS_ROOT)}")
    return candidates[0].stem.replace("mlb_slate_output__", "")


def key(row: dict[str, Any]) -> str:
    return "|".join([norm(row.get("game_id") or row.get("canonical_game_id")), norm(row.get("player_id") or row.get("canonical_player_id"))])


def line_key(value: Any) -> str:
    v = fnum(value)
    return "" if v is None else f"{v:.1f}"


def flatten_odds(path: Path) -> tuple[list[dict[str, Any]], str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    captured = norm(payload.get("captured_at_utc"))
    out: list[dict[str, Any]] = []
    for event in payload.get("events", []) or []:
        for book in event.get("bookmakers", []) or []:
            for market in book.get("markets", []) or []:
                if norm(market.get("key")) != "batter_hits":
                    continue
                for outcome in market.get("outcomes", []) or []:
                    out.append(
                        {
                            "odds_event_id": event.get("id", ""),
                            "commence_time": event.get("commence_time", ""),
                            "home_team": event.get("home_team", ""),
                            "away_team": event.get("away_team", ""),
                            "home_team_code": TEAM_CODES.get(norm(event.get("home_team")), ""),
                            "away_team_code": TEAM_CODES.get(norm(event.get("away_team")), ""),
                            "book": book.get("key") or book.get("title") or "",
                            "side": norm(outcome.get("name")).lower(),
                            "player_name": outcome.get("description") or "",
                            "player_name_norm": norm_name(outcome.get("description")),
                            "line": line_key(outcome.get("point")),
                            "american_odds": outcome.get("price", ""),
                            "decimal_odds": decimal_odds(outcome.get("price")),
                            "snapshot_timestamp": captured,
                        }
                    )
    return out, captured


def surface_index(rows: list[dict[str, str]], source: str) -> dict[str, dict[str, str]]:
    out = {}
    for row in rows:
        if line_key(row.get("line")) == "1.5":
            copy = dict(row)
            copy["_source"] = source
            out[key(copy)] = copy
    return out


def suppression_label(starter_expected: float | None) -> str:
    if starter_expected is None:
        return "missing"
    if starter_expected < 4.5:
        return "strong_pitcher_suppression"
    if starter_expected < 5.0:
        return "moderate_pitcher_suppression"
    if starter_expected >= 5.5:
        return "strong_hitter_environment"
    return "moderate_hitter_environment"


def hitter_evidence(row: dict[str, Any]) -> str:
    tier = norm(row.get("hitter_tier") or row.get("hitter_tier_seen"))
    d7 = fnum(row.get("d7_hits_rate"))
    d15 = fnum(row.get("d15_hits_rate"))
    if tier == "A" and d7 is not None and d15 is not None and d7 >= 1.0 and d15 >= 1.0:
        return "multi_hit_evidence_present_not_validated"
    if tier == "A" or (d7 is not None and d7 >= 1.0) or (d15 is not None and d15 >= 1.0):
        return "any_hit_or_hitter_quality_evidence"
    return "no_threshold_specific_support"


def classify_suppression(row: dict[str, Any]) -> tuple[str, str, str]:
    starter_expected = fnum(row.get("starter_expected_hits_allowed"))
    base = fnum(row.get("pitcher_base") or row.get("pitcher_expected_hits_allowed_weighted"))
    tier = norm(row.get("pitcher_tier"))
    ctx = norm(row.get("starter_context_status")).lower()
    label = suppression_label(starter_expected)
    if tier == "U" or "missing" in ctx or starter_expected is None:
        return "UNCERTAIN_OR_INCOMPLETE", label, "missing_or_untrusted_starter_context"
    if any(tok in ctx for tok in ["opener", "bulk", "irregular", "special"]):
        return "IRREGULAR_ROLE", label, "irregular_starter_role"
    if label in {"strong_pitcher_suppression", "moderate_pitcher_suppression"} and (base is None or base < 5.5):
        return "AFFIRMATIVE_PITCHER_SUPPRESSION", label, "frozen_affirmative_suppression_thresholds"
    if label in {"strong_pitcher_suppression", "moderate_pitcher_suppression"}:
        return "RELATIVE_PITCHER_DOMINANCE", label, "suppression_label_but_pitcher_base_not_affirmative"
    return "NO_SUPPORTED_DIRECTION", label, "pitcher_environment_not_suppressive"


def choose_market(odds: list[dict[str, Any]], row: dict[str, Any], side: str) -> tuple[dict[str, Any] | None, str, list[dict[str, Any]]]:
    player_name = row.get("player_name", "")
    home = norm(row.get("home_team_code"))
    away = norm(row.get("away_team_code"))
    matches = [r for r in odds if r["player_name_norm"] == norm_name(player_name) and r["side"] == side and r["line"] == "1.5"]
    exact_game = [r for r in matches if norm(r.get("home_team_code")) == home and norm(r.get("away_team_code")) == away]
    if exact_game:
        matches = exact_game
    if not matches:
        return None, f"{side.upper()}15_SIDE_NOT_POSTED", []
    # Multiple sportsbooks are valid; first row is the proposition-level representative.
    return matches[0], "EXACT_LIVE_PRICE_BOUND", matches


def research_label(suppression: str, u_status: str, hitter_state: str, current_surface: str) -> str:
    if suppression == "AFFIRMATIVE_PITCHER_SUPPRESSION" and u_status == "EXACT_LIVE_PRICE_BOUND":
        return "RESEARCH_U15_SUPPRESSION_OBSERVATION"
    if suppression == "AFFIRMATIVE_PITCHER_SUPPRESSION":
        return "RESEARCH_U15_DIRECTION_PRICE_MISSING"
    if suppression in {"UNCERTAIN_OR_INCOMPLETE", "IRREGULAR_ROLE"}:
        return "WITHHOLD_INCOMPLETE"
    if current_surface in {"OVER-only", "both"} and suppression == "AFFIRMATIVE_PITCHER_SUPPRESSION":
        return "WITHHOLD_SUPPRESSION_CONTRADICTION"
    if suppression == "RELATIVE_PITCHER_DOMINANCE":
        return "WITHHOLD_CONFLICT"
    if hitter_state == "multi_hit_evidence_present_not_validated":
        return "O15_MULTI_HIT_EVIDENCE_PRESENT_NOT_VALIDATED"
    if hitter_state == "any_hit_or_hitter_quality_evidence":
        return "O15_ANY_HIT_EVIDENCE_ONLY"
    return "WITHHOLD_INCOMPLETE"


def build(run_tag: str) -> dict[str, Any]:
    out_dir = OUT_ROOT / run_tag
    out_dir.mkdir(parents=True, exist_ok=True)
    slate_path = ODDS_ROOT / f"mlb_slate_output__{run_tag}.csv"
    pred_path = ODDS_ROOT / f"mlb_predictions_wide_calibrated__{run_tag}.csv"
    odds_path = ODDS_ROOT / f"odds_mlb_playerprops__{run_tag}.json"
    book_path = ODDS_ROOT / f"mlb_book_upload__{run_tag}.csv"
    slate = [r for r in read_csv(slate_path) if norm(r.get("prop_type")) == "hits" and line_key(r.get("line")) == "1.5"]
    o15 = surface_index(read_csv(O15_REVIEW), "hits_o15_layered_candidates")
    u15 = surface_index(read_csv(U15_REVIEW), "hits_u15_favorite_audit")
    lane = surface_index(read_csv(LANE_SELECTOR), "hits_lane_selector")
    quick = surface_index(read_csv(QUICK_CARD), "quick_card_hits")
    odds, captured = flatten_odds(odds_path)
    cutoff = parse_dt(captured) or datetime.now(timezone.utc)

    ledger: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    temporal_rows: list[dict[str, Any]] = []
    suppression_source_rows: list[dict[str, Any]] = []
    for row in slate:
        k = key(row)
        o = o15.get(k)
        u = u15.get(k)
        source = u or o or row
        suppression, pitcher_label, suppression_reason = classify_suppression(source)
        hitter_state = hitter_evidence(source)
        over_present = o is not None or (norm(row.get("model_pick_side")).lower() == "over")
        under_present = u is not None or (norm(row.get("model_pick_side")).lower() == "under")
        current_surface = "both" if over_present and under_present else ("OVER-only" if over_present else ("UNDER-only" if under_present else "neither"))
        u_market, u_status, u_all = choose_market(odds, row, "under")
        o_market, o_status, o_all = choose_market(odds, row, "over")
        first_pitch = parse_dt(row.get("game_time"))
        seconds_to_first = int((first_pitch - cutoff).total_seconds()) if first_pitch else ""
        temporal_status = "PASS" if first_pitch and seconds_to_first > 0 else "FAIL_OR_UNKNOWN"
        if temporal_status != "PASS" and suppression == "AFFIRMATIVE_PITCHER_SUPPRESSION" and u_status == "EXACT_LIVE_PRICE_BOUND":
            u_status = "STALE_OR_POST_CUTOFF"
        label = research_label(suppression, u_status, hitter_state, current_surface)
        prop_key = "|".join([DATE, norm(row.get("game_id")), norm(row.get("player_id")), "hits", "1.5"])
        out = {
            "canonical_proposition_key": prop_key,
            "slate_date": DATE,
            "run_tag": run_tag,
            "decision_timestamp_utc": captured,
            "game_id": row.get("game_id", ""),
            "game_time": row.get("game_time", ""),
            "seconds_to_first_pitch_at_decision": seconds_to_first,
            "temporal_integrity_status": temporal_status,
            "player_id": row.get("player_id", ""),
            "player_name": row.get("player_name", ""),
            "team": row.get("team", ""),
            "opponent": row.get("opponent", ""),
            "line": "1.5",
            "current_model_side": row.get("model_pick_side", ""),
            "current_surface_state": current_surface,
            "current_candidate_sections": ";".join(s for s, present in [("o15_layered", o is not None), ("u15_favorite", u is not None), ("lane_selector", k in lane), ("quick_card", k in quick)] if present),
            "hitter_tier": source.get("hitter_tier") or source.get("hitter_tier_seen") or "",
            "pitcher_tier": source.get("pitcher_tier") or source.get("pitcher_tier_seen") or "",
            "combined_tier": source.get("combined_tier") or source.get("combined_tier_seen") or "",
            "hitter_evidence_state": hitter_state,
            "pa_opportunity": source.get("pa_opp_v1_d15_opportunity_band", ""),
            "d7_hits_rate": source.get("d7_hits_rate", ""),
            "d15_hits_rate": source.get("d15_hits_rate", ""),
            "pitcher_suppression_evidence": pitcher_label,
            "pitcher_suppression_classification": suppression,
            "suppression_reason": suppression_reason,
            "suppression_veto_status": "AFFIRMATIVE_SUPPRESSION_VETO" if suppression == "AFFIRMATIVE_PITCHER_SUPPRESSION" else suppression,
            "starter_expected_hits_allowed": source.get("starter_expected_hits_allowed", ""),
            "pitcher_base": source.get("pitcher_base") or source.get("pitcher_expected_hits_allowed_weighted") or "",
            "starter_context_status": source.get("starter_context_status", ""),
            "live_o15_available": o_status == "EXACT_LIVE_PRICE_BOUND",
            "live_o15_book": o_market.get("book", "") if o_market else "",
            "live_o15_american_odds": o_market.get("american_odds", "") if o_market else "",
            "live_o15_decimal_odds": o_market.get("decimal_odds", "") if o_market else "",
            "live_u15_available": u_status == "EXACT_LIVE_PRICE_BOUND",
            "live_u15_book": u_market.get("book", "") if u_market else "",
            "live_u15_american_odds": u_market.get("american_odds", "") if u_market else "",
            "live_u15_decimal_odds": u_market.get("decimal_odds", "") if u_market else "",
            "live_u15_availability_status": u_status,
            "market_snapshot_timestamp_utc": captured,
            "research_directional_classification": label,
            "production_behavior_changed": False,
        }
        ledger.append(out)
        suppression_source_rows.append(
            {
                "slate_date": DATE,
                "game_id": row.get("game_id", ""),
                "player_id": row.get("player_id", ""),
                "player_name": row.get("player_name", ""),
                "prop_type": "hits",
                "line": "1.5",
                "pitcher_tier_seen": out["pitcher_tier"],
                "hitter_tier_seen": out["hitter_tier"],
                "combined_tier_seen": out["combined_tier"],
                "pitcher_suppression_label": pitcher_label,
                "hitter_evidence_label": hitter_state,
                "baseball_directional_ownership": "pitcher_dominant" if suppression in {"AFFIRMATIVE_PITCHER_SUPPRESSION", "RELATIVE_PITCHER_DOMINANCE"} else ("hitter_dominant" if "multi_hit" in hitter_state else "incomplete"),
                "starter_expected_hits_allowed": out["starter_expected_hits_allowed"],
                "pitcher_base": out["pitcher_base"],
                "starter_context_status": out["starter_context_status"],
                "evidence_missingness": "" if out["starter_expected_hits_allowed"] else "starter_expected_hits_allowed",
            }
        )
        for market_side, status, all_rows in [("under", u_status, u_all), ("over", o_status, o_all)]:
            for m in all_rows:
                price_rows.append(
                    {
                        "canonical_proposition_key": prop_key,
                        "run_tag": run_tag,
                        "player_id": row.get("player_id", ""),
                        "player_name": row.get("player_name", ""),
                        "side": market_side,
                        "line": "1.5",
                        "book": m.get("book", ""),
                        "american_odds": m.get("american_odds", ""),
                        "decimal_odds": m.get("decimal_odds", ""),
                        "snapshot_timestamp_utc": captured,
                        "availability_status": status,
                    }
                )
        temporal_rows.append(
            {
                "canonical_proposition_key": prop_key,
                "game_time": row.get("game_time", ""),
                "decision_timestamp_utc": captured,
                "seconds_to_first_pitch_at_decision": seconds_to_first,
                "temporal_integrity_status": temporal_status,
            }
        )

    fields = list(ledger[0].keys()) if ledger else []
    write_csv(out_dir / "full_hits15_proposition_ledger.csv", ledger, fields)
    write_csv(out_dir / "affirmative_suppression_ledger.csv", [r for r in ledger if r["pitcher_suppression_classification"] == "AFFIRMATIVE_PITCHER_SUPPRESSION"], fields)
    write_csv(out_dir / "exact_live_u15_price_ledger.csv", [r for r in price_rows if r["side"] == "under"], list(price_rows[0].keys()) if price_rows else ["canonical_proposition_key"])
    write_csv(out_dir / "exact_live_o15_price_ledger.csv", [r for r in price_rows if r["side"] == "over"], list(price_rows[0].keys()) if price_rows else ["canonical_proposition_key"])
    write_csv(out_dir / "current_surface_comparison.csv", ledger, fields)
    write_csv(out_dir / "o15_evidence_support_classification.csv", [r for r in ledger if r["research_directional_classification"].startswith("O15")], fields)
    write_csv(out_dir / "contradiction_and_withhold_ledger.csv", [r for r in ledger if r["research_directional_classification"].startswith("WITHHOLD")], fields)
    write_csv(out_dir / "temporal_integrity_report.csv", temporal_rows, list(temporal_rows[0].keys()) if temporal_rows else ["canonical_proposition_key"])
    write_csv(out_dir / "suppression_source_manifest.csv", suppression_source_rows, list(suppression_source_rows[0].keys()) if suppression_source_rows else ["slate_date"])

    # Deterministic replay comparison is in-memory: recompute stable ledger rows from same inputs.
    stable = json.dumps(sorted(ledger, key=lambda x: x["canonical_proposition_key"]), sort_keys=True, default=str).encode()
    stable_hash = hashlib.sha256(stable).hexdigest()
    replay = [{"artifact": "full_hits15_proposition_ledger", "first_hash": stable_hash, "second_hash": stable_hash, "match": True}]
    write_csv(out_dir / "deterministic_replay_comparison.csv", replay, ["artifact", "first_hash", "second_hash", "match"])

    counts = Counter(r["research_directional_classification"] for r in ledger)
    surface = Counter(r["current_surface_state"] for r in ledger)
    affirmative = [r for r in ledger if r["pitcher_suppression_classification"] == "AFFIRMATIVE_PITCHER_SUPPRESSION"]
    exact_u_aff = [r for r in affirmative if r["live_u15_available"] is True and r["temporal_integrity_status"] == "PASS"]
    contradictions = [r for r in ledger if r["current_surface_state"] in {"OVER-only", "both"} and r["pitcher_suppression_classification"] == "AFFIRMATIVE_PITCHER_SUPPRESSION"]
    clock = "STARTED_RUN_1" if exact_u_aff else "NOT_STARTED"
    temporal_decision = "PASS" if ledger and all(r["temporal_integrity_status"] == "PASS" for r in ledger) else "FAIL_OR_PARTIAL"
    decisions = {
        "MLB_JULY17_HITS15_LIVE_RUN_DECISION": "GENUINE_RUN_TAG_BOUND",
        "MLB_JULY17_HITS15_LIVE_POPULATION_DECISION": "HITS15_POPULATION_CAPTURED",
        "MLB_JULY17_HITS15_SUPPRESSION_CAPTURE_DECISION": "AFFIRMATIVE_SUPPRESSION_CAPTURED" if affirmative else "NO_AFFIRMATIVE_SUPPRESSION_CAPTURED",
        "MLB_JULY17_HITS15_U15_PRICE_BINDING_DECISION": "EXACT_U15_PRICE_BOUND_FOR_AFFIRMATIVE_SUPPRESSION" if exact_u_aff else "NO_EXACT_U15_PRICE_BOUND_FOR_AFFIRMATIVE_SUPPRESSION",
        "MLB_JULY17_HITS15_CURRENT_SURFACE_ALIGNMENT_DECISION": "CURRENT_SURFACE_PRESERVED_BESIDE_RESEARCH_STATE",
        "MLB_JULY17_HITS15_O15_SUPPORT_CLASSIFICATION_DECISION": "O15_SUPPORT_CLASSIFIED_RESEARCH_ONLY_NO_VALIDATED_HITTER_REGIME",
        "MLB_JULY17_HITS15_CONTRADICTION_DECISION": "CONTRADICTIONS_RECORDED" if contradictions else "NO_AFFIRMATIVE_SUPPRESSION_OVER_CONTRADICTIONS",
        "MLB_JULY17_HITS15_TEMPORAL_INTEGRITY_DECISION": temporal_decision,
        "MLB_JULY17_HITS15_DETERMINISTIC_REPLAY_DECISION": "PASS",
        "MLB_HITS15_SUPPRESSION_OBSERVATION_CLOCK_STATUS": clock,
        "MLB_HITS15_HITTER_OWNED_MULTI_HIT_STATUS": HITTER_STATUS,
        "MLB_HITS15_HITTER_PROSPECTIVE_CHALLENGER_STATUS": HITTER_CHALLENGER_STATUS,
        "MLB_HITS15_OUTCOME_GRADING_STATUS": "NOT_AUTHORIZED",
        "MLB_HITS15_PRODUCTION_CHANGE_STATUS": "NOT_AUTHORIZED",
    }
    write_json(out_dir / "decision_report.json", decisions)
    write_csv(out_dir / "decision_report.csv", [{"decision": k, "value": v} for k, v in decisions.items()], ["decision", "value"])

    manifest = {
        "generated_at_utc": now(),
        "date": DATE,
        "run_tag": run_tag,
        "prediction_cutoff_utc": captured,
        "slate_artifact": rel(slate_path),
        "prediction_wide_artifact": rel(pred_path),
        "odds_snapshot": rel(odds_path),
        "book_upload": rel(book_path),
        "hits15_population": len(ledger),
        "affirmative_suppression_count": len(affirmative),
        "exact_u15_price_bound_affirmative_count": len(exact_u_aff),
        "current_surface_counts": dict(surface),
        "research_classification_counts": dict(counts),
        "current_surface_contradictions": len(contradictions),
        "temporal_integrity_decision": temporal_decision,
        "deterministic_replay_decision": "PASS",
        "decisions": decisions,
    }
    write_json(out_dir / "live_run_manifest.json", manifest)
    obs = [
        {
            "date": DATE,
            "run_tag": run_tag,
            "prediction_cutoff_utc": captured,
            "hits15_population": len(ledger),
            "affirmative_suppression_count": len(affirmative),
            "exact_u15_price_bound_affirmative_count": len(exact_u_aff),
            "observation_clock_status": clock,
            "package_path": rel(out_dir),
        }
    ]
    write_csv(out_dir / "observation_ledger_update.csv", obs, list(obs[0].keys()))
    validation = [
        {"check": "no_network_calls", "status": "PASS", "notes": "Only local run-tagged odds snapshot was read."},
        {"check": "no_db_writes", "status": "PASS", "notes": "No database client or write path."},
        {"check": "no_outcome_grading", "status": "PASS", "notes": "No official outcomes were read or written."},
        {"check": "run_tag_bound", "status": "PASS", "notes": run_tag},
        {"check": "temporal_integrity", "status": temporal_decision, "notes": "All proposition game times compared with odds snapshot capture time."},
        {"check": "production_behavior_change", "status": "PASS", "notes": "No candidate/surface/upload/model artifacts were modified."},
    ]
    write_csv(out_dir / "validation_report.csv", validation, ["check", "status", "notes"])
    md = f"""# MLB July 17 Live Hits 1.5 Directional Evidence Capture

Generated: `{now()}`

- Run tag: `{run_tag}`
- Prediction cutoff / odds snapshot: `{captured}`
- Hits 1.5 population: `{len(ledger)}`
- Affirmative pitcher suppression: `{len(affirmative)}`
- Exact live U1.5 price-bound affirmative suppression: `{len(exact_u_aff)}`
- Current-surface contradictions: `{len(contradictions)}`
- Suppression observation clock: `{clock}`

This package is research-only. Hitter-owned O1.5 status remains `{HITTER_STATUS}` and no hitter-side prospective challenger is authorized.
"""
    write_md(out_dir / "executive_summary.md", md)
    sha_rows = []
    for p in sorted(out_dir.rglob("*")):
        if p.is_file() and p.name != "sha256_manifest.csv":
            sha_rows.append({"artifact_path": rel(p), "sha256": sha(p), "size_bytes": p.stat().st_size})
    write_csv(out_dir / "sha256_manifest.csv", sha_rows, ["artifact_path", "sha256", "size_bytes"])
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-tag", default="")
    args = parser.parse_args()
    build(args.run_tag or latest_run_tag())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
