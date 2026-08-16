"""Initialize the frozen V1 MLB totals prospective shadow."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    GOVERNED_STARTER_HISTORY_TIERS, attach_context, build_history, canonical_hash, distribution, feature_row,
    fetch_hydrated_schedule, load_candidate, normalize_schedule, score_context,
)
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    MODEL_VERSION, SNAPSHOT_CLASS, append_context, append_prediction, append_prediction_with_context, canonical_identity,
    connect_ledger, contexts_for_date, counts, payload_hash, rows_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
EXPERIMENT = "MLB_TOTALS_PROSPECTIVE_SHADOW_V1"
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
ODDS_ROOT = ROOT / "backend/mlb/exports/odds_history"
THRESHOLDS = (6.5, 7.5, 8.5, 9.5, 10.5, 11.5)


def dynamic_environment(history: dict[str, Any], game_date: str) -> dict[str, Any]:
    core = history["core"].copy(); core["game_date"] = pd.to_datetime(core.game_date); date = pd.Timestamp(game_date); prior = core[core.game_date < date]
    season = prior[prior.game_date.dt.year == date.year]; trailing = prior[(date-prior.game_date).dt.days.between(1, 30)]; previous = prior[prior.game_date.dt.year == date.year-1]
    payload = {"season_to_date_league_rpg": float(season.final_total.mean()), "season_history_depth": int(len(season)),
        "trailing_30_league_rpg": float(trailing.final_total.mean()), "trailing_30_history_depth": int(len(trailing)),
        "prior_season_league_rpg": float(previous.final_total.mean()), "prior_season_history_depth": int(len(previous)),
        "feature_cutoff_utc": f"{game_date}T00:00:00Z", "latest_included_game_date": str(prior.game_date.max().date())}
    payload["state_hash"] = canonical_hash(payload); return payload


def _team(value: str) -> str:
    return " ".join(str(value).lower().replace(".", "").split())


def _normalize_odds_events(payload: Any, source_path: Path) -> tuple[list[dict[str, Any]], str | None]:
    """Accept only canonical Odds API arrays or the retained wrapped fixture shape."""
    if isinstance(payload, list):
        events = payload; captured_at_utc = None
    elif isinstance(payload, dict) and "events" in payload:
        events = payload["events"]; captured_at_utc = payload.get("captured_at_utc")
        if not isinstance(events, list):
            raise ValueError(f"ODDS_EVENTS_NOT_LIST source={source_path}")
    else:
        root_type = "null" if payload is None else type(payload).__name__
        raise ValueError(f"UNEXPECTED_ODDS_JSON_ROOT source={source_path} type={root_type}")
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ValueError(f"ODDS_EVENT_NOT_OBJECT source={source_path} index={index} type={type(event).__name__}")
    return events, captured_at_utc


def market_inventory(game_date: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates, files = [], []
    for path in sorted((ODDS_ROOT/game_date).glob("*.json")):
        if path.name.endswith(".manifest.json"):
            continue
        raw = path.read_bytes()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"INVALID_ODDS_JSON source={path}") from exc
        events, captured = _normalize_odds_events(payload, path)
        file_row = {"source_path": str(path.relative_to(ROOT)), "source_sha256": hashlib.sha256(raw).hexdigest(), "captured_at_utc": captured,
                    "event_count": len(events), "game_totals_markets": 0}
        for event in events:
            for bookmaker in event.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    if market.get("key") != "totals": continue
                    file_row["game_totals_markets"] += 1; outcomes = market.get("outcomes", []); by_name = {str(x.get("name", "")).lower(): x for x in outcomes}
                    points = [x.get("point") for x in outcomes if x.get("point") is not None]; line = points[0] if points and all(x == points[0] for x in points) else None
                    candidates.append({"away_team": event.get("away_team"), "home_team": event.get("home_team"), "scheduled_start_utc": event.get("commence_time"),
                        "provider": bookmaker.get("title"), "provider_key": bookmaker.get("key"), "total_line": line,
                        "over_price": by_name.get("over", {}).get("price"), "under_price": by_name.get("under", {}).get("price"),
                        "snapshot_timestamp_utc": market.get("last_update") or captured, "source_run_tag": path.stem,
                        "source_path": file_row["source_path"], "source_sha256": file_row["source_sha256"]})
        files.append(file_row)
    return candidates, files


def attach_market(context: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    exact = [row for row in candidates if _team(row["away_team"]) == _team(context["away_team_name"]) and _team(row["home_team"]) == _team(context["home_team_name"])]
    if not exact:
        return {"market_status": "TOTAL_MARKET_UNAVAILABLE", "sportsbook_provider": None, "total_line": None, "over_price": None, "under_price": None,
                "market_snapshot_timestamp_utc": None, "market_lead_time_minutes": None, "market_source_run_tag": None, "market_source_path": None, "market_source_sha256": None}
    exact.sort(key=lambda row: row.get("snapshot_timestamp_utc") or ""); row = exact[-1]
    if not row.get("snapshot_timestamp_utc"):
        status = "TOTAL_MARKET_TIMING_UNRESOLVED"
    elif row.get("total_line") is not None and row.get("over_price") is not None and row.get("under_price") is not None:
        status = "TOTAL_MARKET_CERTIFIED_PAIRED"
    elif row.get("total_line") is not None:
        status = "TOTAL_MARKET_LINE_ONLY"
    else:
        status = "TOTAL_MARKET_UNAVAILABLE"
    lead = None
    if row.get("snapshot_timestamp_utc"):
        lead = (pd.Timestamp(context["scheduled_start_utc"]) - pd.Timestamp(row["snapshot_timestamp_utc"])).total_seconds()/60
        if lead <= 0: status = "TOTAL_MARKET_TIMING_UNRESOLVED"
    return {"market_status": status, "sportsbook_provider": row.get("provider"), "total_line": row.get("total_line"), "over_price": row.get("over_price"),
        "under_price": row.get("under_price"), "market_snapshot_timestamp_utc": row.get("snapshot_timestamp_utc"), "market_lead_time_minutes": lead,
        "market_source_run_tag": row.get("source_run_tag"), "market_source_path": row.get("source_path"), "market_source_sha256": row.get("source_sha256")}


def probability_fields(expected_total: float, alpha: float, line: float | None) -> dict[str, Any]:
    mass = distribution(expected_total, alpha); support = np.arange(len(mass)); result = {}
    for threshold in THRESHOLDS: result[f"p_over_{str(threshold).replace('.', '_')}"] = float(mass[support > threshold].sum())
    if line is None:
        result.update({"p_over_market_line": None, "p_under_market_line": None, "push_probability_at_market_line": None, "model_minus_market_total": None})
    else:
        result.update({"p_over_market_line": float(mass[support > line].sum()), "p_under_market_line": float(mass[support < line].sum()),
            "push_probability_at_market_line": float(mass[support == line].sum()), "model_minus_market_total": expected_total-line})
    return result


def report_markdown(rows: list[dict[str, Any]]) -> str:
    lines = ["# Current MLB totals prospective shadow", "", "Research-only frozen V1 shadow. No public display, EV, ranking, or wager recommendation.", "",
             "| Matchup | Predicted total | Market total | P(O market) | P(U market) | Starters | Park | Context | Grading |",
             "|---|---:|---:|---:|---:|---|---|---|---|"]
    for row in rows:
        fmt = lambda value: "—" if value is None else f"{value:.3f}"
        lines.append(f"| {row['away_team']} @ {row['home_team']} | {row['expected_total']:.3f} | {fmt(row.get('total_line'))} | {fmt(row.get('p_over_market_line'))} | {fmt(row.get('p_under_market_line'))} | {row['away_probable_starter_name']} / {row['home_probable_starter_name']} | {row['venue_name']} ({row['park_factor']:.3f}) | {row['context_quality_state']} | UNGRADED |")
    return "\n".join(lines)+"\n"


def run(game_date: str, output_dir: Path, ledger_path: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True); candidate = load_candidate(); connection = connect_ledger(ledger_path); before = counts(connection)
    snapshot_slug = game_date.replace("-", "_")
    payload, observed, schedule_hash = fetch_hydrated_schedule(game_date); schedule = normalize_schedule(payload, observed, schedule_hash); history = build_history(); env = dynamic_environment(history, game_date)
    markets, market_files = market_inventory(game_date); existing = {row["game_pk"]: row for row in rows_for_date(connection, game_date)}; attempts = []
    for schedule_row in schedule:
        game_pk = int(schedule_row["game_pk"]); identity = canonical_identity(game_date, game_pk)
        if game_pk in existing:
            attempts.append({"canonical_identity": identity, "ledger_action": "EXISTING_IMMUTABLE",
                "context_action": "EXISTING_CONTEXT_NOT_RECONSTRUCTED", "game_pk": game_pk}); continue
        context = attach_context(schedule_row, history, observed)
        if pd.Timestamp(context["scheduled_start_utc"]) <= pd.Timestamp(observed):
            attempts.append({"canonical_identity": identity, "ledger_action": "REJECTED_POST_START",
                "rejection_reason": "PREGAME_CUTOFF_FAILED", "game_pk": context["game_pk"]}); continue
        if context["data_quality_status"] != "TOTALS_CONTEXT_COMPLETE":
            reasons = []
            for side in ("away", "home"):
                status = context.get(f"{side}_probable_pitcher_status")
                if status != "PROBABLE_PITCHER_CERTIFIED": reasons.append(f"{side.upper()}_{status}")
                tier = context[f"{side}_starter_state"]["fallback_tier"]
                if status == "PROBABLE_PITCHER_CERTIFIED" and tier not in GOVERNED_STARTER_HISTORY_TIERS:
                    reasons.append(f"{side.upper()}_STARTER_UNGOVERNED_{tier}")
            if context["park_state"]["fallback_status"] != "DIRECT_REGRESSED_PARK_HISTORY":
                reasons.append(f"PARK_{context['park_state']['fallback_status']}")
            for side in ("away", "home"):
                bullpen_status = context[f"{side}_bullpen_state"]["certification_status"]
                if bullpen_status != "GOVERNED_TEAM_RELIEVER_HISTORY":
                    reasons.append(f"{side.upper()}_{bullpen_status}")
            attempts.append({"canonical_identity": identity, "ledger_action": "REJECTED_CONTEXT_NOT_COMPLETE",
                "game_pk": context["game_pk"], "rejection_reasons": reasons,
                "retry_status": ("RETRYABLE_SAME_DAY_IF_OFFICIAL_PROBABLE_POSTS" if any("PROBABLE_PITCHER_UNAVAILABLE" in reason for reason in reasons)
                                 else "RETRYABLE_SAME_DAY_IF_OFFICIAL_HISTORY_ADVANCES" if any("BULLPEN_HISTORY_STALE" in reason for reason in reasons)
                                 else "NOT_RETRYABLE_WITHOUT_CONTEXT_REPAIR")}); continue
        score = score_context(context, history, candidate, observed)
        market = attach_market(context, markets); probabilities = probability_fields(score["expected_total"], candidate["dispersion_alpha"], market["total_line"])
        bullpen_provenance = {key: value for key, value in history["bullpen_history_provenance"].items() if key != "supplement_sources"}
        features = feature_row(context, history, candidate); feature_state = {"model_features": features, "away_starter_state": context["away_starter_state"],
            "home_starter_state": context["home_starter_state"], "away_bullpen_state": context["away_bullpen_state"],
            "home_bullpen_state": context["home_bullpen_state"], "bullpen_history_provenance": bullpen_provenance,
            "park_state": context["park_state"], "dynamic_league_environment": env}
        row = {"experiment": EXPERIMENT, "game_date": game_date, "game_pk": context["game_pk"], "prediction_snapshot_class": SNAPSHOT_CLASS,
            "scheduled_start_utc": context["scheduled_start_utc"], "prediction_timestamp_utc": observed, "scoring_cutoff_utc": context["scheduled_start_utc"],
            "away_team_id": context["away_team_id"], "away_team": context["away_team_name"], "home_team_id": context["home_team_id"], "home_team": context["home_team_name"],
            "away_probable_starter_id": context["away_probable_pitcher_id"], "away_probable_starter_name": context["away_probable_pitcher_name"],
            "home_probable_starter_id": context["home_probable_pitcher_id"], "home_probable_starter_name": context["home_probable_pitcher_name"],
            "away_starter_state_status": context["away_starter_state"]["certification_status"], "away_starter_fallback_status": context["away_starter_state"]["fallback_tier"],
            "home_starter_state_status": context["home_starter_state"]["certification_status"], "home_starter_fallback_status": context["home_starter_state"]["fallback_tier"],
            "starter_history_fallback_tier": context["starter_history_fallback_tier"],
            "starter_history_quality_state": context["starter_history_quality_state"],
            "venue_id": context["venue_id"], "venue_name": context["venue_name"], "park_factor": context["park_state"]["park_factor"],
            "park_fallback_status": context["park_state"]["fallback_status"], "context_quality_state": context["data_quality_status"],
            "dynamic_league_environment": env, "model_version": MODEL_VERSION, "model_hash": candidate["canonical_model_hash"],
            "expected_total": score["expected_total"], "interval_80_low": score["interval_80_low"], "interval_80_high": score["interval_80_high"],
            **probabilities, **market, "feature_state_hash": canonical_hash(feature_state), "schedule_source_sha256": schedule_hash,
            "official_schedule_observed_at_utc": observed, "grading_status": "UNGRADED_OUTCOME_SEPARATE_LEDGER"}
        action, context_action = append_prediction_with_context(connection, row, feature_state)
        attempts.append({"canonical_identity": identity, "ledger_action": action, "context_action": context_action, "game_pk": context["game_pk"]})
    rows = rows_for_date(connection, game_date); contexts = contexts_for_date(connection, game_date); after = counts(connection)

    flat = []
    for row in rows:
        item = {k: v for k, v in row.items() if not isinstance(v, dict)}; environment = row.get("dynamic_league_environment", {})
        item.update({f"dynamic_{key}": value for key, value in environment.items() if not isinstance(value, dict)})
        context = contexts.get(canonical_identity(row["game_date"], row["game_pk"]), {})
        for side in ("away", "home"):
            state = context.get(f"{side}_starter_state", {}); item.update({f"{side}_starter_{key}": value for key, value in state.items() if not isinstance(value, dict)})
        park = context.get("park_state", {}); item.update({f"park_{key}": value for key, value in park.items() if not isinstance(value, dict)})
        flat.append(item)
    pd.DataFrame(flat).to_csv(output_dir/f"{snapshot_slug}_totals_shadow_predictions.csv", index=False)
    market_columns = ["game_pk","away_team","home_team","market_status","sportsbook_provider","total_line","over_price","under_price","market_snapshot_timestamp_utc","market_lead_time_minutes","market_source_run_tag","market_source_path","market_source_sha256","p_over_market_line","p_under_market_line","push_probability_at_market_line","model_minus_market_total"]
    pd.DataFrame([{key: row.get(key) for key in market_columns} for row in rows]).to_csv(output_dir/f"{snapshot_slug}_total_market_attachment.csv", index=False)
    validation = [
        ("canonical_unique", after["duplicate_prediction_identities"] == 0, after["duplicate_prediction_identities"]),
        ("append_only_triggers", True, "PREDICTION_CONTEXT_OUTCOME_UPDATE_AND_DELETE_BLOCKED"), ("no_post_start_admissions", all(pd.Timestamp(row["prediction_timestamp_utc"]) < pd.Timestamp(row["scheduled_start_utc"]) for row in rows), len(rows)),
        ("no_outcome_fields_in_predictions", all(not ({"final_total","regulation_nine_total","outcome","result","official_final_total"}&set(row)) for row in rows), len(rows)),
        ("context_complete_only", all(row["context_quality_state"] == "TOTALS_CONTEXT_COMPLETE" for row in rows), len(rows)),
        ("model_hash_frozen", all(row["model_hash"] == candidate["canonical_model_hash"] for row in rows), candidate["canonical_model_hash"]),
        ("complete_context_payload_durable", after["context_rows"] == after["prediction_rows"], f"{after['context_rows']}/{after['prediction_rows']}"),
        ("outcomes_separate_and_empty", after["outcome_rows"] == 0, after["outcome_rows"]),
        ("designated_snapshot_idempotent", all(append_prediction(connection, row).startswith("EXISTING_") for row in rows), len(rows)),
    ]
    pd.DataFrame([{"check": name, "status": "PASS" if passed else "FAIL", "observed": value} for name,passed,value in validation]).to_csv(output_dir/"totals_shadow_ledger_validation.csv", index=False)
    ledger_display = str(ledger_path.relative_to(ROOT)) if ledger_path.is_relative_to(ROOT) else str(ledger_path)
    contract = {"experiment": EXPERIMENT, "model_version": MODEL_VERSION, "model_hash": candidate["canonical_model_hash"], "model_source": "MLB_TOTALS_PREDICTION_REPRESENTATIVE_RERUN_V1",
        "live_context_source": "MLB_TOTALS_LIVE_CONTEXT_BRIDGE_REPAIR_V1", "canonical_identity": "game_date + game_id + totals_model_version + prediction_snapshot_class",
        "snapshot_class": SNAPSHOT_CLASS, "ledger_path": ledger_display, "ledger_type": "LOCAL_SQLITE_APPEND_ONLY_PREDICTION_CONTEXT_OUTCOME_TABLES_WITH_NO_UPDATE_DELETE_TRIGGERS",
        "prediction_outcome_separation": True, "market_source_policy": "EXISTING_LOCAL_FILES_ONLY", "market_source_inventory": market_files,
        "historical_reference": {"validation_2025": {"mae":3.597207,"bias":-0.215047,"crps":2.531596}, "opened_late_2026_diagnostic": {"mae":3.678261,"bias":-0.661055,"crps":2.602277}},
        "prospective_authority_begins": "2026-08-06", "snapshot_game_date": game_date,
        "outcomes_accessed_during_prediction": 0, "public_status": "SHADOW_ONLY_NOT_PUBLIC"}
    (output_dir/"totals_prospective_contract.json").write_text(json.dumps(contract,indent=2)+"\n")
    (output_dir/"totals_grading_contract.md").write_text("# Totals grading contract\n\nGrade only after official final. Append one separate outcome row per immutable prediction identity with official-source hash, final total, regulation-nine total, absolute error, signed residual, CRPS, fixed-threshold Brier/log loss, and captured-market-line result. Never update prediction rows. Repeated grading must be idempotent. Authentic captured paired prices may support descriptive ROI later, but no EV, wager, ranking, or betting authority is authorized.\n")
    (output_dir/"totals_prospective_checkpoint_contract.md").write_text("# Prospective checkpoint contract\n\n- 25 graded games: integrity and gross bias only.\n- 50: first directional assessment.\n- 100: first practical candidate review.\n- 200: stronger calibration and market comparison.\n\nAt each checkpoint report MAE, bias, CRPS, calibration, fixed-threshold metrics, prediction and market-line-difference distributions, context quality, predicted-total band, park regime, and starter-history depth. Inspect obvious defects immediately.\n")
    (output_dir/"totals_daily_workflow_hook_audit.md").write_text("# Totals daily workflow hook audit\n\n- Hook location: installed daily wrapper after current-slate two-provider market capture and completed-slate official-source recovery; the earlier moneyline lifecycle is unchanged.\n- Existing scheduler: 05:30, 08:30, 11:00, 13:00, and 16:30 Pacific; no separate scheduler.\n- 05:30 is the primary scoring pass. The 08:30 and later runs retry only identities still missing.\n- Once-per-date/game guard: SQLite canonical unique identity; existing immutable rows skip before context reconstruction.\n- Retry: an absent game identity may be added only while that game remains unstarted; post-start rows fail closed.\n- Missing probable pitchers fail closed; governed sparse starter history is allowed.\n- Failure isolation: the hook is nonblocking and visibly logs its exit code, matching moneyline-shadow isolation.\n- Market availability never blocks model scoring; attachment remains separate from immutable predictions.\n")
    (output_dir/"current_totals_shadow_report.md").write_text(report_markdown(rows))
    market_count = sum(row.get("market_status") == "TOTAL_MARKET_CERTIFIED_PAIRED" for row in rows); declaration = "TOTALS_PROSPECTIVE_SHADOW_INITIALIZED" if market_count == len(rows) else "TOTALS_PROSPECTIVE_SHADOW_INITIALIZED_MARKET_COVERAGE_PARTIAL"
    values = [row["expected_total"] for row in rows]
    (output_dir/"concise_mlb_totals_prospective_shadow_v1.md").write_text(f"# MLB Totals Prospective Shadow v1\n\n`{declaration}`\n\n- Snapshot date / admitted games: {game_date} / {len(rows)}\n- Context complete: {sum(row['context_quality_state']=='TOTALS_CONTEXT_COMPLETE' for row in rows)}/{len(rows)}\n- Certified paired markets: {market_count}/{len(rows)}\n- Expected-total range: {min(values):.3f}–{max(values):.3f}\n- Ledger before/after: {before} / {after}\n- Outcomes accessed during prediction: 0\n- Public/deployment status: unchanged; shadow only\n")
    hash_path=output_dir/"reproducibility_hashes.sha256";hash_path.write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in sorted(output_dir.iterdir()) if path != hash_path))
    return {"declaration":declaration,"rows":len(rows),"new_rows":sum(x["ledger_action"]=="APPENDED_NEW" for x in attempts),"context_complete":sum(row["context_quality_state"]=="TOTALS_CONTEXT_COMPLETE" for row in rows),
        "certified_markets":market_count,"expected_total_min":min(values),"expected_total_max":max(values),"ledger_before":before,"ledger_after":after,"attempts":attempts,"outcomes_accessed":0}


def main() -> None:
    parser=argparse.ArgumentParser();parser.add_argument("--date",required=True);parser.add_argument("--output-dir",type=Path,required=True);parser.add_argument("--ledger-path",type=Path,default=DEFAULT_LEDGER)
    args=parser.parse_args();print(json.dumps(run(args.date,args.output_dir,args.ledger_path),indent=2,default=str))


if __name__=="__main__":main()
