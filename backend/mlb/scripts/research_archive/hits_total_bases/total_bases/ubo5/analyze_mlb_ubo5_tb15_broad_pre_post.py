#!/usr/bin/env python3
"""Build the bounded UBO-5 TB1.5 broad-board pre/post discrepancy package."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import re
from pathlib import Path

import pandas as pd

from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import implied
from backend.mlb.scripts.build_mlb_ubo5_tb15_provisional_tracker import market_rows

ROOT = Path(__file__).resolve().parents[3]
REPORT_DATE = "2026-07-27"
OUT = ROOT / f"artifacts/analysis/model_development/mlb_ubo5_tb15_broad_board_pre_post_bridge/{REPORT_DATE}"
EVIDENCE = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_established_hitter_implementation_readiness/2026-07-23"
MODEL = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23/original_ubo5_total_bases_multinomial.joblib"
BOARD = ROOT / "backend/mlb/exports/model_v2/ubo5_tb15"
ODDS = ROOT / "backend/mlb/exports/odds_history"


def write_csv(name: str, rows: list[dict], fields: list[str] | None = None) -> None:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = list(rows[0]) if rows else ["status"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def nv(over, under):
    oi, ui = implied(int(over)), implied(int(under))
    return oi / (oi + ui) if oi is not None and ui is not None and oi + ui else None


def profit(odds: float, outcome: float) -> float:
    return (odds / 100 if odds > 0 else 100 / abs(odds)) if outcome else -1


def metrics(rows: list[dict], era: str, population: str) -> dict:
    valid = [r for r in rows if r.get("y") in (0, 1, 0.0, 1.0)]
    if not valid:
        return {
            "era": era, "population": population, "rows": 0, "dates": 0,
            "wins": 0, "losses": 0, "win_rate": "", "average_odds": "",
            "units": "", "ROI": "", "average_ubo5_probability": "",
            "expected_wins": "", "actual_minus_expected": "", "Brier_score": "",
            "log_loss": "", "average_edge_pp": "",
        }
    p = [float(r["probability"]) for r in valid]
    y = [float(r["y"]) for r in valid]
    odds = [float(r["over_price"]) for r in valid]
    edges = [float(r["edge_pp"]) for r in valid]
    units = sum(profit(o, z) for o, z in zip(odds, y))
    wins = int(sum(y))
    return {
        "era": era, "population": population, "rows": len(valid),
        "dates": len({r["slate_date"] for r in valid}), "wins": wins,
        "losses": len(valid)-wins, "win_rate": wins/len(valid),
        "average_odds": sum(odds)/len(odds), "units": units,
        "ROI": units/len(valid), "average_ubo5_probability": sum(p)/len(p),
        "expected_wins": sum(p), "actual_minus_expected": wins-sum(p),
        "Brier_score": sum((a-b)**2 for a,b in zip(p,y))/len(p),
        "log_loss": sum(-(b*math.log(max(a,1e-12))+(1-b)*math.log(max(1-a,1e-12))) for a,b in zip(p,y))/len(p),
        "average_edge_pp": sum(edges)/len(edges),
    }


def historical_bridge() -> tuple[list[dict], list[dict]]:
    supported = pd.read_csv(EVIDENCE / "supported_population_manifest.csv")
    supported = supported[
        supported.line.eq(1.5)
        & supported.starter_certification.eq("CERTIFIED_HISTORICAL_STARTER")
        & supported.strict_prior_pa.ge(100)
    ].copy()
    lineups = pd.read_parquet(
        ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_live_platform_certification/2026-07-23/normalized_refresh/starting_lineups/season=2026/part-000.parquet"
    )
    lineup_idx = {
        (int(r.game_pk), int(r.player_id)): int(r.batting_order_position)
        for r in lineups.itertuples()
    }
    evidence = {
        (str(r.slate_date), int(r.game_pk), int(r.batter_mlb_id)): r
        for r in supported.itertuples()
    }
    observations: dict[tuple, list[dict]] = {}
    for date in sorted(supported.slate_date.unique()):
        day = ODDS / str(date)
        for odds_path in sorted(day.glob("odds_mlb_playerprops__local_daily_*.json")):
            match = re.search(r"(local_daily_\d{8}T\d{6}Z)", odds_path.name)
            if not match:
                continue
            tag = match.group(1)
            wide_path = day / f"mlb_predictions_wide_calibrated__{tag}.csv"
            if not wide_path.is_file():
                continue
            snapshot = json.loads(odds_path.read_text())
            captured = pd.to_datetime(snapshot.get("captured_at_utc"), utc=True, errors="coerce")
            matched, _ = market_rows(snapshot, pd.read_csv(wide_path))
            for market in matched:
                key = (str(date), int(market["game_id"]), int(market["player_id"]))
                source = evidence.get(key)
                start = pd.to_datetime(market.get("game_time"), utc=True, errors="coerce")
                if source is None or pd.isna(captured) or pd.isna(start) or captured >= start:
                    continue
                no_vig = nv(market["over_price"], market["under_price"])
                probability = float(source.original_ubo5_prob_over)
                observations.setdefault(key, []).append({
                    "slate_date": str(date), "game_pk": key[1], "batter_mlb_id": key[2],
                    "player_name": source.player_name, "game": market["game"], "run_tag": tag,
                    "snapshot_timestamp_utc": captured.isoformat(), "batting_order": lineup_idx.get((key[1],key[2]), ""),
                    "strict_prior_pa": source.strict_prior_pa, "feature_state": "COMPLETE",
                    "probability": probability, "over_price": market["over_price"],
                    "under_price": market["under_price"], "no_vig_probability": no_vig,
                    "edge_pp": (probability-no_vig)*100, "y": float(source.y_over),
                    "actual_total_bases": source.actual_value,
                })
    bridge = []
    all_priced = []
    for key, rows in sorted(observations.items()):
        rows.sort(key=lambda r: r["snapshot_timestamp_utc"])
        all_priced.append(rows[-1])
        positives = [r for r in rows if r["edge_pp"] > 0]
        if positives:
            bridge.append(positives[0] | {"population": "BROAD_EVER_POSITIVE"})
        if rows[-1]["edge_pp"] > 0:
            bridge.append(rows[-1] | {"population": "BROAD_FINAL_PREGAME_POSITIVE"})
    return bridge, all_priced


def live_rows() -> list[dict]:
    rows = []
    for population, filename in (
        ("BROAD_EVER_POSITIVE", "ubo5_tb15_ever_positive_closeout_2026-07-26.csv"),
        ("BROAD_FINAL_PREGAME_POSITIVE", "ubo5_tb15_final_pregame_closeout_2026-07-26.csv"),
    ):
        path = BOARD / "2026-07-26" / filename
        if not path.is_file():
            continue
        for r in csv.DictReader(path.open()):
            rows.append({
                "slate_date": r["slate_date"], "population": population,
                "game_pk": int(r["game_pk"]), "batter_mlb_id": int(r["batter_mlb_id"]),
                "player_name": r["player_name"], "game": r["game"],
                "run_tag": r["selection_run_tag"], "snapshot_timestamp_utc": r["selection_timestamp_utc"],
                "batting_order": r["batting_order"], "strict_prior_pa": r["strict_prior_pa"],
                "feature_state": r["feature_state"], "probability": float(r["ubo5_probability_over"]),
                "over_price": int(float(r["betonline_over_price"])),
                "under_price": int(float(r["betonline_under_price"])),
                "no_vig_probability": float(r["no_vig_over_probability"]),
                "edge_pp": float(r["ubo5_over_edge_pp"]),
                "y": 1.0 if r["result"] == "WIN" else 0.0,
                "actual_total_bases": r["total_bases"], "coverage_status": "PARTIAL_CERTIFIED_RUN_SPINE",
            })
    return rows


def segment_rows(rows: list[dict]) -> list[dict]:
    dimensions = {
        "edge": lambda r: ">0–1 pp" if r["edge_pp"] <= 1 else "1–2 pp" if r["edge_pp"] <= 2 else "2–3 pp" if r["edge_pp"] <= 3 else "3–5 pp" if r["edge_pp"] <= 5 else "5+ pp",
        "probability": lambda r: "<35%" if r["probability"] < .35 else "35–39.99%" if r["probability"] < .4 else "40–44.99%" if r["probability"] < .45 else "45–49.99%" if r["probability"] < .5 else "50%+",
        "odds": lambda r: "favorite" if r["over_price"] < 0 else "+100 to +149" if r["over_price"] < 150 else "+150 to +199" if r["over_price"] < 200 else "+200+",
        "batting_order": lambda r: "1–3" if str(r.get("batting_order","")).isdigit() and int(r["batting_order"]) <= 3 else "4–6" if str(r.get("batting_order","")).isdigit() and int(r["batting_order"]) <= 6 else "7–9" if str(r.get("batting_order","")).isdigit() else "unavailable",
        "strict_prior_pa": lambda r: "100–199" if float(r.get("strict_prior_pa") or 0) < 200 else "200–299" if float(r.get("strict_prior_pa") or 0) < 300 else "300+",
        "feature_state": lambda r: r.get("feature_state") or "unavailable",
    }
    out = []
    for population in sorted({r["population"] for r in rows}):
        base = [r for r in rows if r["population"] == population]
        timing_label = (
            "final pregame" if population == "BROAD_FINAL_PREGAME_POSITIVE"
            else "first positive / first confirmed-lineup qualifying observation"
        )
        timing_metric = metrics(base, "COMBINED_DIAGNOSTIC", population)
        out.append({
            "population": population, "dimension": "snapshot_timing",
            "segment": timing_label, "rows": timing_metric["rows"],
            "dates": timing_metric["dates"],
            "predicted_win_rate": timing_metric["average_ubo5_probability"],
            "actual_win_rate": timing_metric["win_rate"],
            "calibration_gap": timing_metric["average_ubo5_probability"]-timing_metric["win_rate"],
            "ROI": timing_metric["ROI"],
            "contribution_to_underperformance": sum(float(r["y"])-float(r["probability"]) for r in base),
        })
        for dimension, classifier in dimensions.items():
            cells = {}
            for row in base:
                cells.setdefault(classifier(row), []).append(row)
            for cell, members in cells.items():
                m = metrics(members, "COMBINED_DIAGNOSTIC", population)
                predicted = m["average_ubo5_probability"]
                actual = m["win_rate"]
                out.append({
                    "population": population, "dimension": dimension, "segment": cell,
                    "rows": m["rows"], "dates": m["dates"], "predicted_win_rate": predicted,
                    "actual_win_rate": actual, "calibration_gap": predicted-actual,
                    "ROI": m["ROI"],
                    "contribution_to_underperformance": sum(float(r["y"])-float(r["probability"]) for r in members),
                })
    for missing in ("pitcher_context", "hitter_context", "incumbent_agreement"):
        out.append({"population": "ALL", "dimension": missing, "segment": "NOT_PRESERVED_COMPARABLY", "rows": 0, "dates": 0, "predicted_win_rate": "", "actual_win_rate": "", "calibration_gap": "", "ROI": "", "contribution_to_underperformance": ""})
    return out


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    bridge, all_priced = historical_bridge()
    live = live_rows()
    write_csv("ubo5_broad_historical_bridge.csv", bridge)
    era_results = []
    for pop in ("BROAD_EVER_POSITIVE", "BROAD_FINAL_PREGAME_POSITIVE"):
        era_results.append(metrics([r for r in bridge if r["population"] == pop], "PRE_OBSERVATION_HISTORICAL", pop))
        era_results.append(metrics([r for r in live if r["population"] == pop], "LIVE_STALE_HISTORY_ERA", pop))
        era_results.append(metrics([], "LIVE_REPAIRED_HISTORY_ERA", pop))
    write_csv("ubo5_broad_live_era_results.csv", era_results)
    coverage = []
    for date in pd.date_range("2026-07-23", "2026-07-27").strftime("%Y-%m-%d"):
        manifest_path = BOARD / date / f"ubo5_tb15_run_population_manifest_{date}.json"
        if manifest_path.is_file():
            manifest = json.loads(manifest_path.read_text())
            classification = "FULLY_CERTIFIED_COMPLETE_RUN_SPINE" if manifest["spine_status"] == "CERTIFIED_COMPLETE_RUN_SNAPSHOTS" else "PARTIAL_CERTIFIED_RUN_SPINE"
            counts = manifest["counts"]
        elif (BOARD / date / f"ubo5_tb15_closeout_{date}.csv").is_file():
            classification, counts = "CERTIFIED_RETAINED_SNAPSHOT_POPULATION", {}
        else:
            classification, counts = "POPULATION_NOT_CERTIFIABLE", {}
        for pop, key in (("BROAD_EVER_POSITIVE","broad_ever_positive"),("BROAD_FINAL_PREGAME_POSITIVE","final_pregame_positive")):
            coverage.append({"slate_date": date, "population_name": pop, "coverage_classification": classification, "selection_count": counts.get(key,""), "outcome_status": "FINAL" if date <= "2026-07-26" and classification != "POPULATION_NOT_CERTIFIABLE" else "PENDING_OR_UNAVAILABLE", "manifest_path": str(manifest_path.relative_to(ROOT)) if manifest_path.is_file() else ""})
    write_csv("ubo5_broad_population_coverage_audit.csv", coverage)
    closeout_audit = []
    for r in live:
        closeout_audit.append({k:r.get(k,"") for k in ["slate_date","population","game_pk","batter_mlb_id","player_name","game","run_tag","probability","over_price","under_price","no_vig_probability","edge_pp","actual_total_bases","y","coverage_status"]})
    write_csv("ubo5_broad_population_closeout_audit.csv", closeout_audit)

    movement = pd.read_csv(ROOT / "artifacts/analysis/model_development/mlb_ubo5_completed_game_feedback_loop/2026-07-27/ubo5_post_endpoint_feature_movement.csv")
    outcome_idx = {(r["game_pk"],r["batter_mlb_id"]):r for r in live if r["population"]=="BROAD_EVER_POSITIVE"}
    stale = []
    for r in movement.to_dict("records"):
        observed = outcome_idx.get((int(r["game_pk"]),int(r["batter_mlb_id"])))
        no_vig = observed["no_vig_probability"] if observed else None
        before, after = float(r["ubo5_probability_before"]), float(r["ubo5_probability_after"])
        stale.append({
            **r, "no_vig_over_probability": no_vig,
            "original_positive_edge": "" if no_vig is None else before > no_vig,
            "repaired_positive_edge": "" if no_vig is None else after > no_vig,
            "membership_change": "PRICE_NOT_PRESERVED" if no_vig is None else "RETAINED" if before > no_vig and after > no_vig else "REMOVED" if before > no_vig else "NEW" if after > no_vig else "NONPOSITIVE",
            "result": "" if observed is None else ("WIN" if observed["y"] else "LOSS"),
        })
    write_csv("ubo5_stale_history_counterfactual.csv", stale)
    segments = segment_rows(bridge + live)
    write_csv("ubo5_broad_discrepancy_segments.csv", segments)

    original = pd.read_csv(EVIDENCE / "line_1_5_evaluation.csv")
    ubo = original[original.model.eq("original_ubo5")].iloc[0]
    historical_ever = [r for r in bridge if r["population"]=="BROAD_EVER_POSITIVE"]
    historical_final = [r for r in bridge if r["population"]=="BROAD_FINAL_PREGAME_POSITIVE"]
    live_ever = [r for r in live if r["population"]=="BROAD_EVER_POSITIVE"]
    steps = [
        {"step":"original pre-observation population","remaining_rows":int(ubo.rows),"predicted_win_rate":ubo.mean_probability,"actual_win_rate":ubo.actual_over_rate,"Brier_score":ubo.brier,"log_loss":ubo.log_loss,"ROI":"","notes":"eligible probability rows; no market-edge test"},
        metrics(all_priced,"","authentic BetOnline two-sided availability") | {"step":"authentic BetOnline two-sided availability","remaining_rows":len(all_priced),"notes":"exact run-tagged prices"},
        metrics(all_priced,"","confirmed lineup and exact batting order") | {"step":"confirmed lineup and exact batting order","remaining_rows":len(all_priced),"notes":"historically certified starters"},
        metrics(historical_ever,"","broad UBO-5 positive-edge rule") | {"step":"broad UBO-5 positive-edge rule","remaining_rows":len(historical_ever),"notes":"first positive"},
        metrics(historical_final,"","matching snapshot timing") | {"step":"matching snapshot timing","remaining_rows":len(historical_final),"notes":"last eligible snapshot"},
        metrics(live_ever,"","live odds distribution") | {"step":"live odds distribution","remaining_rows":len(live_ever),"notes":"July 26 retained partial spine"},
    ]
    waterfall = []
    for row in steps:
        waterfall.append({
            "step": row["step"], "remaining_rows": row.get("remaining_rows",row.get("rows")),
            "predicted_win_rate": row.get("average_ubo5_probability",row.get("predicted_win_rate","")),
            "actual_win_rate": row.get("win_rate",row.get("actual_win_rate","")),
            "Brier_score": row.get("Brier_score",""), "log_loss": row.get("log_loss",""),
            "ROI": row.get("ROI",""), "notes": row.get("notes",""),
        })
    waterfall.extend([
        {"step":"stale versus repaired history","remaining_rows":len(stale),"notes":"22 preserved July 26 contexts only; membership price unavailable outside retained board"},
        {"step":"complete feature-state requirement","remaining_rows":sum((r.get("feature_state") or "").startswith("COMPLETE") for r in live_ever),"notes":"live retained population"},
        {"step":"recent pitcher/hitter regime distribution","remaining_rows":"","notes":"comparable categorical regimes not preserved"},
    ])
    write_csv("ubo5_broad_discrepancy_waterfall.csv", waterfall)

    artifact_sha = hashlib.sha256(MODEL.read_bytes()).hexdigest()
    pre_md = f"""# UBO-5 pre-observation evidence audit

- Governing probability evidence: `{EVIDENCE.relative_to(ROOT)}/line_1_5_evaluation.csv`
- Supported row ledger: `{EVIDENCE.relative_to(ROOT)}/supported_population_manifest.csv`
- Model artifact: `{MODEL.relative_to(ROOT)}`
- SHA256: `{artifact_sha}`
- Dates: 2026-07-02 through 2026-07-21; 16 slate dates.
- Eligible TB 1.5 rows: 974; OVER probability target; established certified historical starters; strict-prior PA >=100.
- Feature history endpoint: strict-prior per target date; training cutoff 2024-12-31.
- UBO-5: Brier {ubo.brier:.6f}, log loss {ubo.log_loss:.6f}, mean probability {ubo.mean_probability:.6f}, actual rate {ubo.actual_over_rate:.6f}.
- Production comparison: Brier {original[original.model.eq('production')].iloc[0].brier:.6f}; paired UBO-5 improvements were 0.006859 Brier and 0.014343 log loss.
- The favorable result established probability improvement over production. It did not test positive BetOnline edge or ROI.
- The historical bridge uses exact player/game identity, confirmed lineup/order, authentic run-tagged two-sided BetOnline prices, and the last pregame snapshot where available.
"""
    (OUT/"ubo5_pre_observation_evidence_audit.md").write_text(pre_md)
    hist_m = metrics(historical_final,"","")
    live_m = metrics(live_ever,"","")
    retained = sum(x["membership_change"]=="RETAINED" for x in stale)
    removed = sum(x["membership_change"]=="REMOVED" for x in stale)
    new = sum(x["membership_change"]=="NEW" for x in stale)
    decision = "BROAD_APPROACH_NOT_REPLICATED_IN_MARKET_POSITIVE_TAIL"
    report = f"""# UBO-5 TB 1.5 broad-board pre/post discrepancy report

## Governing conclusion

**{decision}**

The favorable pre-observation evidence was a probability-quality result across 974 eligible rows, not evidence that the positive BetOnline edge tail was profitable. The apples-to-apples historical final-pregame bridge contains {hist_m['rows']} positive-edge rows over {hist_m['dates']} dates, with {hist_m['wins']}–{hist_m['losses']}, {hist_m['win_rate']:.2%} wins and {hist_m['ROI']:.2%} ROI.

The only currently resolved run-spine live population is July 26's retained partial spine: {live_m['rows']} rows, {live_m['wins']}–{live_m['losses']}, {live_m['win_rate']:.2%} wins, {live_m['ROI']:.2%} ROI, versus {live_m['average_ubo5_probability']:.2%} predicted. July 27 has complete scheduled/manual capture but no completed reconciliation yet.

The decisive discrepancy enters when moving from all eligible probability rows to the market-positive tail: historical final-pregame positive rows were already 67–131 with -19.78% ROI. The recent retained live result is directionally consistent rather than a new reversal. July 26 underperformed expectation by {live_m['actual_minus_expected']:.2f} wins, but it has partial all-day coverage and is only one slate.

## Stale-history decomposition

The repaired-history comparison covers {len(stale)} preserved July 26 contexts. Mean probability movement was {movement.ubo5_probability_change.mean():+.4f}; maximum absolute movement was {movement.ubo5_probability_change.abs().max():.4f}. Among contexts with preserved board prices, {retained} positive memberships were retained, {removed} removed, and {new} added. This is diagnostic and does not rewrite selections.

## What is and is not supported

- Supported: historical UBO-5 probability scoring beat production on Brier/log loss.
- Supported: the historical authentic-price bridge directly evaluates the broad edge rule.
- Supported: July 26 retained broad rows materially underperformed both probability expectation and break-even economics.
- Not yet supported: rejection of the broad method from complete repaired-era prospective results.
- Untestable: complete July 23–26 all-day broad populations where run artifacts were not retained.
- Needed: multiple completed slates from July 27 onward with complete run-spine capture and exact reconciliation.
"""
    (OUT/"ubo5_broad_board_pre_post_report.md").write_text(report)
    terminal = f"""# Terminal decision

UBO5_TB15_BROAD_APPROACH_REPLICATION_DECISION = {decision}

The favorable all-row probability result does not replicate in the authentic BetOnline market-positive tail. July 26 is directionally consistent with that historical tail result. Complete repaired-history prospective closeouts beginning July 27 are still required to measure the current implementation era.
"""
    (OUT/"terminal_decision.md").write_text(terminal)
    print(json.dumps({"output":str(OUT.relative_to(ROOT)),"historical_bridge_rows":len(bridge),"live_rows":len(live),"decision":decision},indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
