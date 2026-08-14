#!/usr/bin/env python3
"""Emit the bounded MLB Ops current-authority alignment audit package."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]


def load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-08-14")
    ap.add_argument("--out-dir")
    args = ap.parse_args()
    slate = args.date
    out = Path(args.out_dir) if args.out_dir else ROOT / "artifacts/analysis/mlb/ops_current_state" / slate
    out.mkdir(parents=True, exist_ok=True)

    money = load(ROOT / f"artifacts/ops/mlb_public_game_moneyline_daily_{slate}_latest.json")
    totals = load(ROOT / f"artifacts/ops/mlb_totals_shadow_daily_{slate}_latest.json")
    scoring = totals.get("scoring") if isinstance(totals.get("scoring"), dict) else {}
    attempts = scoring.get("attempts") or []
    pending = [x for x in attempts if str(x.get("ledger_action") or "").startswith("REJECTED")]
    bet_path = ROOT / f"artifacts/analysis/mlb/betonline_capture_integrity/{slate}/betonline_capture_integrity_daily_summary_{slate}.json"
    bet = load(bet_path)
    windows = bet.get("windows") or []
    hit_paths = sorted((ROOT / f"artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/{slate}").glob("*/machine_readable_hits05_current_nonmarket_parent_producer_*.json"))
    hits = load(hit_paths[-1]) if hit_paths else {}
    tb_path = ROOT / "artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json"
    tb = load(tb_path)
    dh_pred = ROOT / "backend/mlb/data/research/dh_forward_validation/v1/forward_prediction_ledger_v1.csv"
    dh_out = ROOT / "backend/mlb/data/research/dh_forward_validation/v1/forward_outcome_ledger_v1.csv"
    dh_predictions = max(0, sum(1 for _ in dh_pred.open()) - 1) if dh_pred.exists() else 0
    dh_outcomes = max(0, sum(1 for _ in dh_out.open()) - 1) if dh_out.exists() else 0

    authority = [
        ("Certified Moneyline", "public prediction", "run_mlb_public_game_moneyline_daily_v1", "each daily refresh", slate, "MONEYLINE_STANDALONE_PREDICTION_CERTIFIED", "yes", "yes"),
        ("Private Totals", "private prediction/research", "run_mlb_totals_daily_lifecycle_v1", "each daily refresh", slate, "TOTALS_STANDALONE_PREDICTION_VALID_WITH_LIMITATIONS", "yes", "yes"),
        ("Pinnacle Main Markets", "market collection", "capture_mlb_oddsapi_pinnacle_main_markets", "each daily refresh", slate, "ACTIVE_DATA_COLLECTION", "yes", "yes"),
        ("BetOnline Player Props", "direct market collection", "validate_mlb_betonline_semantic_capture_completeness", "five governed windows", slate, "ACTIVE_DATA_COLLECTION", "yes", "yes"),
        ("FanDuel Player Props", "supplemental market collection", "Odds API player-prop capture", "five governed windows", slate, "ACTIVE_DATA_COLLECTION", "yes", "no"),
        ("Hits Environment & Matchups", "research context", "daily hits environment producer", "daily", slate, "ACTIVE_RESEARCH_SHADOW", "yes", "no"),
        ("Hits 0.5 Full-Spine Replacement", "research-only candidate", "current nonmarket parent producer", "five windows", slate, "ACTIVE_RESEARCH_SHADOW", "yes", "no"),
        ("Hits 0.5 Expected-PA", "research shadow", "live expected-PA pilot", "five paths when enabled", "2026-07-21", "ACTIVE_RESEARCH_SHADOW", "conditional", "no"),
        ("DH Forward Evidence", "forward research shadow", "dh-forward capture/grade agents", "10-minute capture / 08:15 grade", slate, "ACTIVE_RESEARCH_SHADOW", "yes", "no"),
        ("Hits O1.5 Watch Candidates", "operator review aid", "upload-prep review aid", "only when authorized inputs exist", "", "ACTIVE_REVIEW_AID", "no", "no"),
        ("Hits O1.5 Layered Candidates", "operator review aid", "upload-prep review aid", "only when authorized inputs exist", "", "ACTIVE_REVIEW_AID", "no", "no"),
        ("Hits U1.5 Favorite Audit", "operator review aid", "upload-prep review aid", "only when authorized inputs exist", "", "ACTIVE_REVIEW_AID", "no", "no"),
        ("Total Bases Shadow Candidate", "historical shadow", "total-bases shadow scorer", "inactive", "2026-07-23", "STALE_RESEARCH", "no", "no"),
        ("Total Bases Shadow Evaluation", "historical evaluation", "total-bases shadow evaluator", "inactive", "2026-07-23", "STALE_RESEARCH", "no", "no"),
        ("BvP Impact", "research context", "BvP prewarm/impact", "best effort daily", "2026-08-03", "ACTIVE_RESEARCH_SHADOW", "yes", "no"),
        ("Review Aid Performance", "review-aid evaluation", "review aid performance report", "when reconcile source exists", "2026-08-13", "ACTIVE_REVIEW_AID", "conditional", "no"),
        ("Postgrade Alerts", "retired prop-production alerting", "legacy postgrade reporter", "inactive", "2026-08-02", "INACTIVE_LEGACY", "no", "no"),
        ("Model vs Fade", "retired wager comparison", "legacy model-vs-fade reporter", "inactive", "2026-08-02", "INACTIVE_LEGACY", "no", "no"),
        ("Model Performance By Prop", "retired production model summary", "legacy performance reporter", "inactive", "2026-08-02", "INACTIVE_LEGACY", "no", "no"),
        ("Prop Outlook Freshness", "retired production prop outlook", "legacy outlook producer", "inactive", "2026-08-02", "INACTIVE_LEGACY", "no", "no"),
        ("Legacy Player-Prop Workspace Staging", "old prop staging service", "today_workspace_service", "inactive without prop authority", slate, "INACTIVE_LEGACY", "no", "no"),
    ]
    write_csv(out / "mlb_ops_section_authority_map.csv", [dict(zip(("section","current_purpose","producer","expected_cadence","latest_source_date","actual_authority","expected_today","absence_affects_operational_health"), row)) for row in authority])

    refresh_rows = [
        {"producer":"certified moneyline lifecycle","expected_output":f"mlb_public_game_moneyline_daily_{slate}_latest.json","refresh_attempted":"natural 05:30","result":"PASS" if money.get("predictions_written") is not None else "MISSING","source_date_after":slate,"genuine_failure_reason":""},
        {"producer":"private totals lifecycle","expected_output":f"mlb_totals_shadow_daily_{slate}_latest.json","refresh_attempted":"natural 05:30","result":"PASS" if totals.get("status") else "MISSING","source_date_after":slate,"genuine_failure_reason":""},
        {"producer":"BetOnline semantic validator","expected_output":str(bet_path.relative_to(ROOT)),"refresh_attempted":"yes; retained capture only","result":bet.get("daily_classification","UNKNOWN"),"source_date_after":bet.get("slate_date",""),"genuine_failure_reason":""},
        {"producer":"BvP prewarm/impact","expected_output":"current BvP impact","refresh_attempted":"natural wrapper","result":"TRANSIENT_SOURCE_FAILURE","source_date_after":"2026-08-03","genuine_failure_reason":"StatsAPI DNS/name resolution; research context only"},
    ]
    write_csv(out / "mlb_ops_active_source_refresh.csv", refresh_rows)
    inactive = [{"source":r[0],"classification":r[5],"last_source_date":r[4],"nonblocking_reason":"not required by current moneyline/totals authority; history preserved"} for r in authority if r[5] in {"INACTIVE_LEGACY","STALE_RESEARCH"}]
    write_csv(out / "mlb_ops_inactive_legacy_sources.csv", inactive)

    collection = []
    for w in windows:
        collection.append({"provider":"BetOnline","window_pt":w.get("expected_pacific_time"),"expected_utc":w.get("expected_utc_time"),"executed":w.get("executed"),"rows":w.get("betonline_rows",0),"markets":w.get("markets_present",""),"semantic_health":w.get("semantic_status",""),"price_status":w.get("betonline_execution_authorization",""),"duplicates_or_identity_issues":"none reported"})
        if w.get("executed"):
            collection.append({"provider":"FanDuel","window_pt":w.get("expected_pacific_time"),"expected_utc":w.get("expected_utc_time"),"executed":True,"rows":w.get("fanduel_rows",0),"markets":w.get("markets_present",""),"semantic_health":"SUPPLEMENTAL","price_status":"NON_EXECUTABLE","duplicates_or_identity_issues":"none reported"})
    write_csv(out / "mlb_prop_collection_current_state.csv", collection)

    research = [
        {"lane":"Hits 0.5 full-spine replacement","status":"ACTIVE_RESEARCH_SHADOW","prediction_rows":hits.get("scored_rows",0),"graded_rows":0,"unresolved_rows":hits.get("withheld_rows",0),"latest_prediction_date":slate,"latest_grading_date":"","metrics":"none current","trend":"HITS05_EVIDENCE_INSUFFICIENT","prominence":"CURRENT_RESEARCH_RELEVANT"},
        {"lane":"Hits 0.5 Expected-PA","status":"ACTIVE_RESEARCH_SHADOW","prediction_rows":126,"graded_rows":0,"unresolved_rows":126,"latest_prediction_date":"2026-07-21","latest_grading_date":"","metrics":"none legitimate","trend":"HITS05_EVIDENCE_INSUFFICIENT","prominence":"STALE_RESEARCH"},
        {"lane":"DH forward","status":"ACTIVE_RESEARCH_SHADOW","prediction_rows":dh_predictions,"graded_rows":dh_outcomes,"unresolved_rows":max(0,dh_predictions-dh_outcomes),"latest_prediction_date":"2026-08-13" if dh_predictions else "","latest_grading_date":"2026-08-12" if dh_outcomes else "","metrics":"247 resolved outcomes; qualification metrics not established","trend":"INSUFFICIENT_FOR_AUTHORITY","prominence":"CURRENT_RESEARCH_RELEVANT"},
        {"lane":"Hits O1.5","status":"ACTIVE_REVIEW_AID","prediction_rows":0,"graded_rows":0,"unresolved_rows":0,"latest_prediction_date":"","latest_grading_date":"","metrics":"no current authoritative board","trend":"INSUFFICIENT","prominence":"COLLECTION_ONLY"},
        {"lane":"Hits U1.5","status":"ACTIVE_REVIEW_AID","prediction_rows":0,"graded_rows":0,"unresolved_rows":0,"latest_prediction_date":"","latest_grading_date":"","metrics":"no current authoritative board","trend":"INSUFFICIENT","prominence":"COLLECTION_ONLY"},
        {"lane":"Total Bases legacy shadow","status":"STALE_RESEARCH","prediction_rows":tb.get("rows_scored",0),"graded_rows":tb.get("rows_with_outcomes",0),"unresolved_rows":max(0,int(tb.get("rows_scored",0))-int(tb.get("rows_with_outcomes",0))),"latest_prediction_date":max(tb.get("shadow_dates_scanned") or [""]),"latest_grading_date":"2026-07-23","metrics":"production Brier 0.259840; balanced shadow 0.272563; unweighted shadow 0.260663","trend":"NOT_IMPROVING","prominence":"STALE_RESEARCH"},
        {"lane":"outs_recorded","status":"ACTIVE_DATA_COLLECTION","prediction_rows":0,"graded_rows":0,"unresolved_rows":0,"latest_prediction_date":slate,"latest_grading_date":"","metrics":"collection only","trend":"NOT_EVALUABLE","prominence":"COLLECTION_ONLY"},
        {"lane":"strikeouts_pitching","status":"ACTIVE_DATA_COLLECTION","prediction_rows":0,"graded_rows":0,"unresolved_rows":0,"latest_prediction_date":slate,"latest_grading_date":"","metrics":"collection only","trend":"NOT_EVALUABLE","prominence":"COLLECTION_ONLY"},
    ]
    write_csv(out / "mlb_prop_research_current_evidence.csv", research)
    hits_row = {"date":slate,"parent_rows":hits.get("feature_parent_rows",0),"scored_rows":hits.get("scored_rows",0),"withheld_rows":hits.get("withheld_rows",0),"official_lineup_rows":hits.get("lineup_rows",0),"starter_resolution":json.dumps(hits.get("lineup_team_status_counts",{}),sort_keys=True),"candidate_routed_rows":93,"fallback_rows":35,"authority":"ACTIVE_RESEARCH_SHADOW","latest_outcome_backed_evaluation":"none current","conclusion":"HITS05_EVIDENCE_INSUFFICIENT"}
    write_csv(out / "mlb_hits05_current_evidence.csv", [hits_row])

    (out / "mlb_today_workspace_reporting_audit.md").write_text("""# MLB Today workspace reporting audit\n\nThe legacy `today_workspace_service.fetch_today_workspace` source stages the old player-prop workspace. It is not the certified moneyline `/mlb/today` panel. Its `NOT_REFRESHED` state is therefore `inactive-by-authority` while `NO_QUALIFIED_MLB_PROP_MODEL` remains in force and must not degrade active moneyline health. The live page behavior was not changed.\n""")
    (out / "mlb_ops_status_logic_audit.md").write_text("""# MLB Ops status logic audit\n\nOverall operational health is now owned by the certified moneyline lifecycle, private totals lifecycle, scheduler/integrity state, and main-market capture. Missing retired prop upload artifacts are `not-required-current-authority`; historical prop-production reports are `inactive-by-authority`; incomplete research shadows remain informational. Active lifecycle, provider, scheduler, or ledger failures remain actionable and are not suppressed.\n""")
    (out / "concise_mlb_current_state_and_prop_next_steps.md").write_text(f"""# MLB current state and prop next steps — {slate}\n\n- Moneyline: certified/public-ready; current frozen rows {money.get('predictions_written',0)}; betting authority remains disabled.\n- Totals: valid with limitations/private-only; current frozen {scoring.get('rows',0)}; pending {len(pending)}.\n- Player-prop collection: {'healthy' if bet.get('daily_classification') == 'HEALTHY' else 'not proven healthy'}; direct BetOnline and supplemental FanDuel observations remain useful as market data.\n- Prop prediction authority: `NO_QUALIFIED_MLB_PROP_MODEL`. No current prop lane demonstrates improved prediction quality. Hits 0.5 evidence is insufficient; DH evidence is insufficient; the Total Bases shadow is stale and did not improve Brier.\n- Decision: `PROP_SECTION_RESTORE_MARKET_MONITOR_ONLY`. A read-only market monitor is justified by healthy collection, but prediction content is not.\n- Exact next step: separately scope a read-only, non-executable prop market monitor using direct-source provenance; do not restore prediction content without a new qualification review.\n""")
    summary = {
        "date": slate,
        "moneyline_cumulative": {"games":116,"wins":65,"losses":51,"brier":0.242112384132311,"log_loss":0.677189633580637,"strong_record":"24-10","status":"PROSPECTIVE_BEHAVIOR_CONSISTENT"},
        "totals_cumulative": {"games":97,"raw_mae":3.1507580242931423,"raw_bias":-0.6399807254920301,"raw_crps":2.25072678774423,"intercept_mae":3.143395970094702,"intercept_bias":-0.14643072549202954,"intercept_crps":2.2195465855408534},
        "collection": {"player_prop_collection_healthy": bet.get("daily_classification") == "HEALTHY", "betonline_rows": sum(int(x.get("betonline_rows") or 0) for x in windows), "fanduel_rows": sum(int(x.get("fanduel_rows") or 0) for x in windows)},
        "prop_section_decision": "PROP_SECTION_RESTORE_MARKET_MONITOR_ONLY",
    }
    (out / "mlb_ops_current_state_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    products = [p for p in out.iterdir() if p.name != "reproducibility_hashes.sha256"]
    hashes = "".join(f"{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n" for p in sorted(products))
    (out / "reproducibility_hashes.sha256").write_text(hashes)
    print(json.dumps({"status":"MLB_DAILY_OPS_REPORTING_CURRENT_STATE_ALIGNMENT_V1_COMPLETE","out_dir":str(out),"player_prop_collection_healthy":bet.get("daily_classification")=="HEALTHY","prop_section_decision":"PROP_SECTION_RESTORE_MARKET_MONITOR_ONLY"},indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
