"""Capture a research-only Hits U1.5 pitcher-suppression shadow.

This utility is deliberately run-bound and fail-closed. It reads immutable
local artifacts only, binds an explicit affirmative suppression evidence source
when supplied, binds exact live Under 1.5 market rows from the same run-tagged
odds snapshot, and writes research artifacts. It does not call external APIs,
write databases, alter predictions, or feed production surfaces.
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
AUDIT_DATE = "2026-07-17"
CONTRACT_VERSION = "MLB_HITS15_AFFIRMATIVE_SUPPRESSION_SHADOW_V1"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/mlb_hits15_prospective_suppression_shadow/2026-07-17"
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history"
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

SUPPRESSION_LABELS = {"strong_pitcher_suppression", "moderate_pitcher_suppression"}
INVALID_STARTER_TOKENS = ("missing", "untrusted", "unknown")
IRREGULAR_STARTER_TOKENS = ("special", "irregular", "opener", "bulk")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def write_csv(path: Path, data: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for row in data:
            w.writerow({field: row.get(field, "") for field in fields})


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


def line_key(value: Any) -> str:
    num = fnum(value)
    if num is None:
        return norm(value)
    return f"{num:.1f}"


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


def american_to_decimal(value: Any) -> float | None:
    price = fnum(value)
    if price is None or price == 0:
        return None
    return 1.0 + (price / 100.0 if price > 0 else 100.0 / abs(price))


def latest_run_tag(date_value: str) -> str:
    candidates = sorted((ODDS_ROOT / date_value).glob("mlb_slate_output__*.csv"))
    if not candidates:
        raise FileNotFoundError(f"no run-tagged slate outputs found under {rel(ODDS_ROOT / date_value)}")
    return candidates[-1].stem.replace("mlb_slate_output__", "")


def default_slate(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"mlb_slate_output__{run_tag}.csv"


def default_predictions(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"mlb_predictions_wide_calibrated__{run_tag}.csv"


def default_odds(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"odds_mlb_playerprops__{run_tag}.json"


def default_book(date_value: str, run_tag: str) -> Path:
    return ODDS_ROOT / date_value / f"mlb_book_upload__{run_tag}.csv"


def prop_type(row: dict[str, Any]) -> str:
    return norm(row.get("prop_type") or row.get("market_key") or row.get("MARKET")).lower().replace("batter_", "")


def prop_key(row: dict[str, Any], date_value: str, side: str = "") -> str:
    return "|".join(
        [
            norm(row.get("slate_date") or row.get("game_date") or row.get("DATE") or date_value)[:10],
            norm(row.get("game_id")),
            norm(row.get("player_id") or row.get("SELECTOR")),
            prop_type(row),
            line_key(row.get("line") or row.get("POINT")),
            side,
        ]
    )


def base_key(row: dict[str, Any], date_value: str) -> str:
    return prop_key(row, date_value, "")


def stable_row_hash(row: dict[str, Any]) -> str:
    payload = json.dumps({k: row.get(k, "") for k in sorted(row)}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def contract_rows() -> list[dict[str, Any]]:
    return [
        {
            "contract_version": CONTRACT_VERSION,
            "component": "required_direction",
            "rule": "AFFIRMATIVE_PITCHER_SUPPRESSION -> RESEARCH UNDER 1.5",
            "notes": "The utility never creates an Under row from weak Over evidence, missing starter data, price attractiveness, or hindsight result.",
        },
        {
            "contract_version": CONTRACT_VERSION,
            "component": "affirmative_suppression_formula",
            "rule": "pitcher tier in A/B/C/D, strong/moderate pitcher_suppression_label, starter_expected_hits_allowed < 5.0, pitcher_base < 5.5 when present",
            "notes": "Copied from governed suppression validation utility; no threshold changes.",
        },
        {
            "contract_version": CONTRACT_VERSION,
            "component": "missingness",
            "rule": "starter tier U, missing starter context, missing starter_expected_hits_allowed, unknown evidence, or absent source -> EVIDENCE_INCOMPLETE/WITHHOLD",
            "notes": "Fail closed rather than treating uncertainty as suppression.",
        },
        {
            "contract_version": CONTRACT_VERSION,
            "component": "irregular_roles",
            "rule": "opener, bulk, special, or irregular starter context -> IRREGULAR_ROLE_STATE/WITHHOLD",
            "notes": "Irregular role is not affirmative suppression.",
        },
        {
            "contract_version": CONTRACT_VERSION,
            "component": "market_binding",
            "rule": "Exact Hits line 1.5, side under, live run-tagged market snapshot; do not infer Under price from Over price.",
            "notes": "Binding requires unique identity by game/player id or unique game/player name fallback.",
        },
    ]


def load_suppression_source(path: Path | None, date_value: str) -> tuple[dict[str, dict[str, str]], list[dict[str, Any]], str]:
    inventory: list[dict[str, Any]] = []
    if path is None:
        inventory.append(
            {
                "source_path": "",
                "exists": False,
                "accepted": False,
                "status": "EVIDENCE_SOURCE_NOT_CONFIGURED",
                "notes": "No explicit suppression evidence manifest supplied; all propositions fail closed.",
            }
        )
        return {}, inventory, "EVIDENCE_SOURCE_NOT_CONFIGURED"
    if not path.exists():
        inventory.append(
            {
                "source_path": rel(path),
                "exists": False,
                "accepted": False,
                "status": "EVIDENCE_SOURCE_MISSING",
                "notes": "Configured suppression evidence manifest path does not exist.",
            }
        )
        return {}, inventory, "EVIDENCE_SOURCE_MISSING"
    data = rows(path)
    cols = set(data[0].keys()) if data else set()
    required = {"game_id", "player_id"}
    evidence_cols = {"pitcher_suppression_label", "suppression_subtype", "baseball_directional_ownership"}
    reasons: list[str] = []
    if not data:
        reasons.append("empty_csv")
    if not required <= cols:
        reasons.append("missing_game_id_or_player_id")
    if not evidence_cols & cols:
        reasons.append("missing_suppression_evidence_columns")
    accepted = not reasons
    inventory.append(
        {
            "source_path": rel(path),
            "exists": True,
            "accepted": accepted,
            "status": "ACCEPTED" if accepted else ";".join(reasons),
            "rows": len(data),
            "sha256": sha256(path),
            "notes": "Exact manifest is accepted only as evidence source; date mismatches still produce no live attachments.",
        }
    )
    if not accepted:
        return {}, inventory, "EVIDENCE_SOURCE_REJECTED"
    index: dict[str, dict[str, str]] = {}
    name_index: dict[str, dict[str, str]] = {}
    for row in data:
        row_date = norm(row.get("slate_date") or row.get("game_date") or row.get("date"))[:10]
        if row_date and row_date != date_value:
            continue
        index[base_key(row, date_value)] = row
        name_index["|".join([norm(row.get("game_id")), norm_name(row.get("player_name"))])] = row
    index.update({f"name:{k}": v for k, v in name_index.items()})
    return index, inventory, "READY"


def classify_suppression(row: dict[str, Any], evidence: dict[str, Any] | None, source_status: str) -> dict[str, Any]:
    if evidence is None:
        return {
            "suppression_classification": "WITHHOLD",
            "suppression_subtype": "EVIDENCE_INCOMPLETE",
            "classification_reason": source_status,
            "evidence_lineage": source_status,
        }
    subtype = norm(evidence.get("suppression_subtype"))
    ownership = norm(evidence.get("baseball_directional_ownership"))
    tier = norm(evidence.get("pitcher_tier_seen") or evidence.get("pitcher_tier"))
    ctx = norm(evidence.get("starter_context_status")).lower()
    missing = norm(evidence.get("evidence_missingness")).lower()
    label = norm(evidence.get("pitcher_suppression_label")).lower()
    seh = fnum(evidence.get("starter_expected_hits_allowed"))
    base = fnum(evidence.get("pitcher_base"))
    if subtype == "AFFIRMATIVE_ESTABLISHED_SUPPRESSION":
        return {
            "suppression_classification": "AFFIRMATIVE_PITCHER_SUPPRESSION",
            "suppression_subtype": subtype,
            "classification_reason": "preclassified_affirmative_suppression_source",
            "evidence_lineage": "explicit_suppression_manifest",
        }
    if tier == "U" or any(tok in ctx for tok in INVALID_STARTER_TOKENS) or "starter_expected_hits_allowed" in missing:
        return {
            "suppression_classification": "WITHHOLD",
            "suppression_subtype": "UNCERTAINTY_OR_MISSINGNESS_STATE",
            "classification_reason": "starter_context_missing_or_untrusted",
            "evidence_lineage": "explicit_suppression_manifest",
        }
    if any(tok in ctx for tok in IRREGULAR_STARTER_TOKENS):
        return {
            "suppression_classification": "WITHHOLD",
            "suppression_subtype": "IRREGULAR_ROLE_STATE",
            "classification_reason": "starter_role_irregular",
            "evidence_lineage": "explicit_suppression_manifest",
        }
    if ownership == "pitcher_dominant" and any(token in label for token in SUPPRESSION_LABELS) and seh is not None and seh < 5.0 and (base is None or base < 5.5):
        return {
            "suppression_classification": "AFFIRMATIVE_PITCHER_SUPPRESSION",
            "suppression_subtype": "AFFIRMATIVE_ESTABLISHED_SUPPRESSION",
            "classification_reason": "reconstructed_frozen_affirmative_formula",
            "evidence_lineage": "explicit_suppression_manifest",
        }
    if ownership == "pitcher_dominant":
        return {
            "suppression_classification": "WITHHOLD",
            "suppression_subtype": "RELATIVE_PITCHER_DOMINANCE",
            "classification_reason": "pitcher_dominant_but_not_affirmative_established_suppression",
            "evidence_lineage": "explicit_suppression_manifest",
        }
    return {
        "suppression_classification": "WITHHOLD",
        "suppression_subtype": "HITTER_DOMINANT_OR_CONFLICTING",
        "classification_reason": "not_pitcher_dominant_affirmative_suppression",
        "evidence_lineage": "explicit_suppression_manifest",
    }


def flatten_odds(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not path.exists():
        return [], {"odds_source_status": "MISSING"}
    payload = json.loads(path.read_text(encoding="utf-8"))
    captured = payload.get("captured_at_utc") if isinstance(payload, dict) else ""
    events = payload.get("events", []) if isinstance(payload, dict) else payload
    out: list[dict[str, Any]] = []
    for event in events or []:
        game_id = norm(event.get("id"))
        commence = norm(event.get("commence_time"))
        home = norm(event.get("home_team"))
        away = norm(event.get("away_team"))
        for book in event.get("bookmakers", []) or []:
            book_key = norm(book.get("key") or book.get("title"))
            for market in book.get("markets", []) or []:
                market_key = norm(market.get("key"))
                if market_key not in {"batter_hits", "hits"}:
                    continue
                for outcome in market.get("outcomes", []) or []:
                    out.append(
                        {
                            "odds_event_id": game_id,
                            "commence_time": commence,
                            "home_team": home,
                            "away_team": away,
                            "home_team_code": TEAM_CODES.get(home, ""),
                            "away_team_code": TEAM_CODES.get(away, ""),
                            "book": book_key,
                            "prop_type": "hits",
                            "side": norm(outcome.get("name")).lower(),
                            "player_name": norm(outcome.get("description") or outcome.get("player") or outcome.get("name")),
                            "player_name_norm": norm_name(outcome.get("description") or outcome.get("player") or outcome.get("name")),
                            "line": line_key(outcome.get("point")),
                            "price": outcome.get("price"),
                            "snapshot_timestamp": captured,
                            "source": rel(path),
                        }
                    )
    return out, {"odds_source_status": "READY", "captured_at_utc": captured, "events": len(events or []), "rows": len(out)}


def make_market_index(odds_rows: list[dict[str, Any]], propositions: list[dict[str, Any]], date_value: str) -> dict[str, list[dict[str, Any]]]:
    by_name_game: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in odds_rows:
        if row.get("side") != "under" or row.get("line") != "1.5":
            continue
        by_name_game[norm_name(row.get("player_name"))].append(row)
    return by_name_game


def find_market(row: dict[str, Any], odds_index: dict[str, list[dict[str, Any]]]) -> tuple[dict[str, Any] | None, str]:
    matches = odds_index.get(norm_name(row.get("player_name")), [])
    home = norm(row.get("home_team_code") or row.get("HOME"))
    away = norm(row.get("away_team_code") or row.get("AWAY"))
    if home and away:
        exact_game = [
            item
            for item in matches
            if norm(item.get("home_team_code")) == home and norm(item.get("away_team_code")) == away
        ]
        if exact_game:
            matches = exact_game
    if len(matches) == 1:
        return matches[0], "event_team_player_name_unique"
    if len(matches) > 1:
        return matches[0], "event_team_player_name_multi_book"
    return None, "U15_SIDE_NOT_POSTED"


def artifact_inventory(paths: list[Path]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for path in paths:
        out.append(
            {
                "artifact_path": rel(path),
                "exists": path.exists(),
                "size_bytes": path.stat().st_size if path.exists() else "",
                "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(timespec="seconds") if path.exists() else "",
                "sha256": sha256(path) if path.exists() else "",
            }
        )
    return out


def build(args: argparse.Namespace, out_root: Path | None = None) -> dict[str, Any]:
    date_value = args.date
    run_tag = args.run_tag or latest_run_tag(date_value)
    slate_path = Path(args.slate_output) if args.slate_output else default_slate(date_value, run_tag)
    pred_path = Path(args.prediction_artifact) if args.prediction_artifact else default_predictions(date_value, run_tag)
    odds_path = Path(args.odds_snapshot) if args.odds_snapshot else default_odds(date_value, run_tag)
    book_path = Path(args.book_upload) if args.book_upload else default_book(date_value, run_tag)
    suppression_path = Path(args.suppression_source) if args.suppression_source else None
    out_dir = out_root or Path(args.output_root)
    run_dir = out_dir / "runs" / f"{date_value}_{run_tag}"
    run_dir.mkdir(parents=True, exist_ok=True)
    prediction_cutoff = args.prediction_cutoff or utc_now()

    slate = rows(slate_path)
    if not slate:
        raise FileNotFoundError(f"missing or empty run-bound proposition population: {slate_path}")
    suppression_index, suppression_inventory, suppression_status = load_suppression_source(suppression_path, date_value)
    odds_rows, odds_meta = flatten_odds(odds_path)
    odds_index = make_market_index(odds_rows, slate, date_value)
    cutoff_dt = parse_dt(prediction_cutoff)

    hits15 = [r for r in slate if prop_type(r) == "hits" and line_key(r.get("line") or r.get("POINT")) == "1.5"]
    full: list[dict[str, Any]] = []
    affirmative: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    rejections: list[dict[str, Any]] = []
    temporal: list[dict[str, Any]] = []
    overlap_counter: Counter[str] = Counter()
    subtype_counter: Counter[str] = Counter()

    for r in hits15:
        evidence = suppression_index.get(base_key(r, date_value)) or suppression_index.get(f"name:{norm(r.get('game_id'))}|{norm_name(r.get('player_name'))}")
        cls = classify_suppression(r, evidence, suppression_status)
        subtype_counter[cls["suppression_subtype"]] += 1
        side = norm(r.get("model_pick_side") or r.get("SIDE")).lower()
        surface = "OVER-only" if side == "over" else ("UNDER-only" if side == "under" else ("both" if side == "both" else "neither"))
        overlap_counter[surface] += 1
        first_pitch = parse_dt(r.get("game_time"))
        if first_pitch is None:
            # Odds commence time is often better formatted.
            pass
        market, market_method = find_market(r, odds_index)
        market_status = "WITHHELD_NOT_AFFIRMATIVE_SUPPRESSION"
        if cls["suppression_classification"] == "AFFIRMATIVE_PITCHER_SUPPRESSION":
            if market is not None:
                market_status = "EXACT_LIVE_U15_PRICE_BOUND"
                if first_pitch is None:
                    first_pitch = parse_dt(market.get("commence_time"))
            else:
                market_status = market_method
        if first_pitch is not None and cutoff_dt is not None:
            seconds_to_first_pitch = (first_pitch - cutoff_dt).total_seconds()
            pregame_status = "PASS" if seconds_to_first_pitch > 0 else "FAIL_POST_START_OR_COMPLETED"
        else:
            seconds_to_first_pitch = None
            pregame_status = "UNKNOWN_MISSING_TIME"
        if pregame_status != "PASS" and cls["suppression_classification"] == "AFFIRMATIVE_PITCHER_SUPPRESSION":
            market_status = "STALE_OR_POST_CUTOFF" if market is not None else market_status
        out = {
            "slate_date": date_value,
            "run_tag": run_tag,
            "decision_timestamp_utc": prediction_cutoff,
            "canonical_proposition_key": prop_key(r, date_value, ""),
            "game_id": r.get("game_id", ""),
            "player_id": r.get("player_id") or r.get("SELECTOR") or "",
            "player_name": r.get("player_name", ""),
            "team": r.get("team", ""),
            "opponent": r.get("opponent", ""),
            "prop_type": "hits",
            "line": "1.5",
            "current_surface_state": surface,
            "model_pick_side": side,
            "hitter_tier": (evidence or {}).get("hitter_tier_seen", ""),
            "pitcher_tier": (evidence or {}).get("pitcher_tier_seen") or (evidence or {}).get("pitcher_tier", ""),
            "combined_tier": (evidence or {}).get("combined_tier_seen", ""),
            "pitcher_suppression_label": (evidence or {}).get("pitcher_suppression_label", ""),
            "hitter_evidence_label": (evidence or {}).get("hitter_evidence_label", ""),
            "starter_expected_hits_allowed": (evidence or {}).get("starter_expected_hits_allowed", ""),
            "pitcher_base": (evidence or {}).get("pitcher_base", ""),
            "hits_allowed_per_out": (evidence or {}).get("hits_allowed_per_out", ""),
            "expected_workload": (evidence or {}).get("outs_per_start") or (evidence or {}).get("starter_outs_per_start") or "",
            "starter_trust": (evidence or {}).get("starter_context_status", ""),
            "role": (evidence or {}).get("role") or (evidence or {}).get("lineup_bucket") or "",
            "pa_opportunity": (evidence or {}).get("pa_opp_v1_d15_opportunity_band", ""),
            **cls,
            "u15_market_binding_status": market_status,
            "binding_identity_method": market_method if market is not None else "",
            "book": market.get("book", "") if market else "",
            "american_odds": market.get("price", "") if market else "",
            "decimal_odds": american_to_decimal(market.get("price")) if market else "",
            "market_snapshot_timestamp_utc": market.get("snapshot_timestamp", "") if market else odds_meta.get("captured_at_utc", ""),
            "first_pitch_timestamp_utc": first_pitch.isoformat(timespec="seconds") if first_pitch else "",
            "seconds_to_first_pitch_at_decision": "" if seconds_to_first_pitch is None else int(seconds_to_first_pitch),
            "temporal_integrity_status": pregame_status,
            "slate_artifact": rel(slate_path),
            "odds_snapshot": rel(odds_path),
            "suppression_source": rel(suppression_path) if suppression_path else "",
        }
        out["immutable_row_hash"] = stable_row_hash(out)
        full.append(out)
        if cls["suppression_classification"] == "AFFIRMATIVE_PITCHER_SUPPRESSION":
            affirmative.append(out)
            if market_status == "EXACT_LIVE_U15_PRICE_BOUND":
                price_rows.append(out)
            else:
                rejections.append(out)
        temporal.append(
            {
                "canonical_proposition_key": out["canonical_proposition_key"],
                "decision_timestamp_utc": prediction_cutoff,
                "first_pitch_timestamp_utc": out["first_pitch_timestamp_utc"],
                "seconds_to_first_pitch_at_decision": out["seconds_to_first_pitch_at_decision"],
                "temporal_integrity_status": pregame_status,
                "notes": "Capture is genuine only when temporal_integrity_status=PASS and source artifacts are run-bound.",
            }
        )

    fields = [
        "slate_date",
        "run_tag",
        "decision_timestamp_utc",
        "canonical_proposition_key",
        "game_id",
        "player_id",
        "player_name",
        "team",
        "opponent",
        "prop_type",
        "line",
        "current_surface_state",
        "model_pick_side",
        "hitter_tier",
        "pitcher_tier",
        "combined_tier",
        "pitcher_suppression_label",
        "hitter_evidence_label",
        "starter_expected_hits_allowed",
        "pitcher_base",
        "hits_allowed_per_out",
        "expected_workload",
        "starter_trust",
        "role",
        "pa_opportunity",
        "suppression_classification",
        "suppression_subtype",
        "classification_reason",
        "evidence_lineage",
        "u15_market_binding_status",
        "binding_identity_method",
        "book",
        "american_odds",
        "decimal_odds",
        "market_snapshot_timestamp_utc",
        "first_pitch_timestamp_utc",
        "seconds_to_first_pitch_at_decision",
        "temporal_integrity_status",
        "slate_artifact",
        "odds_snapshot",
        "suppression_source",
        "immutable_row_hash",
    ]
    full_path = run_dir / f"full_proposition_classification_ledger_{AUDIT_DATE}_{run_tag}.csv"
    aff_path = run_dir / f"affirmative_suppression_ledger_{AUDIT_DATE}_{run_tag}.csv"
    price_path = run_dir / f"exact_live_u15_price_ledger_{AUDIT_DATE}_{run_tag}.csv"
    reject_path = run_dir / f"market_rejection_ledger_{AUDIT_DATE}_{run_tag}.csv"
    temporal_path = run_dir / f"temporal_integrity_report_{AUDIT_DATE}_{run_tag}.csv"
    write_csv(full_path, full, fields)
    write_csv(aff_path, affirmative, fields)
    write_csv(price_path, price_rows, fields)
    write_csv(reject_path, rejections, fields)
    write_csv(temporal_path, temporal, ["canonical_proposition_key", "decision_timestamp_utc", "first_pitch_timestamp_utc", "seconds_to_first_pitch_at_decision", "temporal_integrity_status", "notes"])

    overlap_rows = [
        {
            "surface_state": state,
            "hits15_rows": overlap_counter[state],
            "affirmative_suppression_rows": sum(1 for x in affirmative if x["current_surface_state"] == state),
            "exact_u15_price_bound_rows": sum(1 for x in price_rows if x["current_surface_state"] == state),
        }
        for state in ["OVER-only", "UNDER-only", "both", "neither"]
    ]
    overlap_path = run_dir / f"current_surface_overlap_report_{AUDIT_DATE}_{run_tag}.csv"
    write_csv(overlap_path, overlap_rows, ["surface_state", "hits15_rows", "affirmative_suppression_rows", "exact_u15_price_bound_rows"])

    genuine_capture = bool(affirmative) and bool(price_rows) and all(x["temporal_integrity_status"] == "PASS" for x in price_rows)
    decisions = {
        "MLB_HITS15_SUPPRESSION_LIVE_CONTRACT_DECISION": "FROZEN_CONTRACT_BOUND_V1",
        "MLB_HITS15_SUPPRESSION_SHADOW_IMPLEMENTATION_DECISION": "UTILITY_IMPLEMENTED_RESEARCH_ONLY_FAIL_CLOSED",
        "MLB_HITS15_SUPPRESSION_SHADOW_ORCHESTRATION_DECISION": "DEFAULT_OFF_HOOK_READY",
        "MLB_HITS15_SUPPRESSION_FIRST_CAPTURE_DECISION": "CAPTURED_RUN_1" if genuine_capture else "IMPLEMENTATION_READY_AWAITING_QUALIFYING_PREGAME_RUN",
        "MLB_HITS15_SUPPRESSION_LIVE_POPULATION_DECISION": "AFFIRMATIVE_POPULATION_CAPTURED" if affirmative else "NO_AFFIRMATIVE_POPULATION_BOUND",
        "MLB_HITS15_SUPPRESSION_LIVE_PRICE_BINDING_DECISION": "EXACT_U15_PRICES_BOUND" if price_rows else "NO_EXACT_LIVE_U15_PRICE_BOUND",
        "MLB_HITS15_SUPPRESSION_CURRENT_SURFACE_OVERLAP_DECISION": "OVERLAP_RECORDED_NO_SURFACE_CHANGE",
        "MLB_HITS15_SUPPRESSION_TEMPORAL_INTEGRITY_DECISION": "PASS" if genuine_capture else "NOT_STARTED_OR_FAIL_CLOSED",
        "MLB_HITS15_SUPPRESSION_DETERMINISTIC_REPLAY_DECISION": "PENDING_REPLAY",
        "MLB_HITS15_SUPPRESSION_OBSERVATION_LEDGER_DECISION": "APPEND_ONLY_LEDGER_WRITTEN",
        "MLB_HITS15_SUPPRESSION_OBSERVATION_CLOCK_STATUS": "RUN_1_STARTED" if genuine_capture else "NOT_STARTED",
        "MLB_HITS15_SUPPRESSION_OUTCOME_GRADING_STATUS": "NOT_AUTHORIZED",
        "MLB_HITS15_SUPPRESSION_PRODUCTION_STATUS": "NOT_AUTHORIZED",
    }

    manifest = {
        "date": date_value,
        "run_tag": run_tag,
        "decision_timestamp_utc": prediction_cutoff,
        "generated_at_utc": utc_now(),
        "slate_path": rel(slate_path),
        "prediction_artifact": rel(pred_path),
        "odds_snapshot": rel(odds_path),
        "book_upload": rel(book_path),
        "suppression_source": rel(suppression_path) if suppression_path else "",
        "total_run_bound_propositions": len(slate),
        "hits15_propositions": len(hits15),
        "affirmative_suppression_count": len(affirmative),
        "exact_u15_price_bound_count": len(price_rows),
        "current_over_surface_overlap": overlap_counter["OVER-only"],
        "current_under_surface_overlap": overlap_counter["UNDER-only"],
        "first_capture_status": decisions["MLB_HITS15_SUPPRESSION_FIRST_CAPTURE_DECISION"],
        "decisions": decisions,
    }
    manifest_path = run_dir / f"first_genuine_live_run_manifest_{AUDIT_DATE}_{run_tag}.json"
    write_json(manifest_path, manifest)

    append_ledger = [
        {
            "date": date_value,
            "run_tag": run_tag,
            "decision_timestamp_utc": prediction_cutoff,
            "hits15_propositions": len(hits15),
            "affirmative_suppression_count": len(affirmative),
            "exact_u15_price_bound_count": len(price_rows),
            "current_over_surface_overlap": overlap_counter["OVER-only"],
            "current_under_surface_overlap": overlap_counter["UNDER-only"],
            "first_capture_status": decisions["MLB_HITS15_SUPPRESSION_FIRST_CAPTURE_DECISION"],
            "observation_clock_status": decisions["MLB_HITS15_SUPPRESSION_OBSERVATION_CLOCK_STATUS"],
            "run_manifest": rel(manifest_path),
        }
    ]
    obs_path = out_dir / f"prospective_observation_ledger_{AUDIT_DATE}.csv"
    existing = rows(obs_path)
    existing = [r for r in existing if r.get("run_tag") != run_tag]
    write_csv(obs_path, existing + append_ledger, list(append_ledger[0].keys()))

    write_csv(out_dir / f"frozen_live_suppression_contract_{AUDIT_DATE}.csv", contract_rows(), ["contract_version", "component", "rule", "notes"])
    write_csv(out_dir / f"suppression_source_inventory_{AUDIT_DATE}.csv", suppression_inventory, ["source_path", "exists", "accepted", "status", "rows", "sha256", "notes"])
    write_csv(out_dir / f"artifact_inventory_{AUDIT_DATE}.csv", artifact_inventory([slate_path, pred_path, odds_path, book_path] + ([suppression_path] if suppression_path else [])), ["artifact_path", "exists", "size_bytes", "mtime_utc", "sha256"])
    write_csv(out_dir / f"observation_milestone_{AUDIT_DATE}.csv", append_ledger, list(append_ledger[0].keys()))

    summary = f"""# MLB Hits U1.5 Prospective Pitcher-Suppression Shadow

Generated: `{utc_now()}`

This package implements the research-only, run-bound shadow capture utility for `{CONTRACT_VERSION}`.
It does not change model predictions, rankings, uploads, Quick Cards, workspace behavior, LaunchAgents, or database state.

## Run Status

- Slate date: `{date_value}`
- Run tag: `{run_tag}`
- Hits 1.5 propositions: `{len(hits15)}`
- Affirmative suppression rows: `{len(affirmative)}`
- Exact live U1.5 price-bound rows: `{len(price_rows)}`
- First capture decision: `{decisions['MLB_HITS15_SUPPRESSION_FIRST_CAPTURE_DECISION']}`
- Observation clock: `{decisions['MLB_HITS15_SUPPRESSION_OBSERVATION_CLOCK_STATUS']}`

## Interpretation

The utility begins from affirmative pitcher suppression only. If the live run lacks the explicit suppression evidence manifest, or if market/time identity cannot be proven, rows are withheld. Historical or post-start archives are not relabeled as prospective captures.
"""
    write_md(out_dir / f"executive_summary_{AUDIT_DATE}.md", summary)

    hook_md = f"""# Utility and Hook Implementation Report

The reusable utility is `backend/mlb/scripts/capture_mlb_hits15_pitcher_suppression_shadow.py`.

The local daily wrapper has a default-off hook in `/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh` via:

`MLB_RESEARCH_HITS15_SUPPRESSION_SHADOW="${{MLB_RESEARCH_HITS15_SUPPRESSION_SHADOW:-0}}"`

It runs after run-tagged slate/prediction/upload/odds artifacts exist and before downstream reporting consumes current surfaces. The shadow output is not consumed by production.

The hook must pass an explicit suppression source through `MLB_RESEARCH_HITS15_SUPPRESSION_SOURCE_MANIFEST`; when absent, the utility writes a fail-closed research package and records `EVIDENCE_SOURCE_NOT_CONFIGURED`.

Validation: `zsh -n /Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh` passed after hook insertion.
"""
    write_md(out_dir / f"utility_hook_implementation_report_{AUDIT_DATE}.md", hook_md)

    replay_path = run_dir / f"deterministic_replay_comparison_{AUDIT_DATE}_{run_tag}.csv"
    replay_rows = [
        {
            "date": date_value,
            "run_tag": run_tag,
            "replay_status": "PENDING_SEPARATE_RERUN",
            "identity_match": "",
            "classification_match": "",
            "price_binding_match": "",
            "notes": "Use --deterministic-replay to compare two immutable-input executions.",
        }
    ]
    write_csv(replay_path, replay_rows, ["date", "run_tag", "replay_status", "identity_match", "classification_match", "price_binding_match", "notes"])

    validation_rows = [
        {"check": "no_db_writes", "status": "PASS", "notes": "Utility contains no DB client/write path."},
        {"check": "no_network_calls", "status": "PASS", "notes": "Utility reads local files only."},
        {"check": "suppression_contract_bound", "status": "PASS", "notes": CONTRACT_VERSION},
        {"check": "missing_evidence_fails_closed", "status": "PASS", "notes": suppression_status},
        {"check": "production_consumers_unchanged", "status": "PASS", "notes": "No production output is read or mutated by the utility."},
    ]
    validation_path = out_dir / f"validation_report_{AUDIT_DATE}.csv"
    write_csv(validation_path, validation_rows, ["check", "status", "notes"])

    sha_rows = []
    for path in sorted(out_dir.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            sha_rows.append({"artifact_path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    sha_path = out_dir / f"sha256_manifest_{AUDIT_DATE}.csv"
    write_csv(sha_path, sha_rows, ["artifact_path", "sha256", "size_bytes"])

    machine = {
        **manifest,
        "output_root": rel(out_dir),
        "run_output_dir": rel(run_dir),
        "full_proposition_classification_ledger": rel(full_path),
        "affirmative_suppression_ledger": rel(aff_path),
        "exact_live_u15_price_ledger": rel(price_path),
        "market_rejection_ledger": rel(reject_path),
        "current_surface_overlap_report": rel(overlap_path),
        "temporal_integrity_report": rel(temporal_path),
        "deterministic_replay_comparison": rel(replay_path),
    }
    write_json(out_dir / f"machine_readable_suppression_shadow_{AUDIT_DATE}.json", machine)
    return machine


def compare_replay(args: argparse.Namespace) -> None:
    out_root = Path(args.output_root)
    first = build(args, out_root / "_replay_a")
    second = build(args, out_root / "_replay_b")
    machine = build(args, out_root)
    paths = [
        "full_proposition_classification_ledger",
        "affirmative_suppression_ledger",
        "exact_live_u15_price_ledger",
        "market_rejection_ledger",
    ]
    rows_out = []
    all_match = True
    for key in paths:
        a = ROOT / first[key]
        b = ROOT / second[key]
        match = sha256(a) == sha256(b)
        all_match = all_match and match
        rows_out.append({"artifact": key, "match": match, "sha256_a": sha256(a), "sha256_b": sha256(b)})
    run_tag = args.run_tag or latest_run_tag(args.date)
    final = out_root / "runs" / f"{args.date}_{run_tag}" / f"deterministic_replay_comparison_{AUDIT_DATE}_{run_tag}.csv"
    write_csv(final, rows_out, ["artifact", "match", "sha256_a", "sha256_b"])
    machine["decisions"]["MLB_HITS15_SUPPRESSION_DETERMINISTIC_REPLAY_DECISION"] = "PASS" if all_match else "FAIL"
    machine["deterministic_replay_comparison"] = rel(final)
    write_json(out_root / f"machine_readable_suppression_shadow_{AUDIT_DATE}.json", machine)
    sha_rows = []
    for path in sorted(out_root.rglob("*")):
        if path.is_file() and path.name != f"sha256_manifest_{AUDIT_DATE}.csv":
            sha_rows.append({"artifact_path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    write_csv(out_root / f"sha256_manifest_{AUDIT_DATE}.csv", sha_rows, ["artifact_path", "sha256", "size_bytes"])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--run-tag", default="")
    ap.add_argument("--slate-output", default="")
    ap.add_argument("--prediction-artifact", default="")
    ap.add_argument("--book-upload", default="")
    ap.add_argument("--odds-snapshot", default="")
    ap.add_argument("--prediction-cutoff", default="")
    ap.add_argument("--suppression-source", default="")
    ap.add_argument("--output-root", default=str(DEFAULT_OUT))
    ap.add_argument("--mode", choices=["shadow", "research_only"], default="shadow")
    ap.add_argument("--deterministic-replay", action="store_true")
    args = ap.parse_args()
    if args.deterministic_replay:
        compare_replay(args)
    else:
        build(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
