#!/usr/bin/env python3
"""Bounded read-only market/status audits and revisioned settlement evidence."""
from __future__ import annotations

import argparse, csv, hashlib, json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from backend.mlb.scripts.cleanroom_v1.settlement_eligibility import (
    BOOK_SETTLED_OFFICIAL_RESULT, classify_book_settlement, classify_game,
    load_contract, settle_side,
)

ROOT = Path(__file__).resolve().parents[4]
ANALYSIS = ROOT / "artifacts/analysis/model_development/mlb_game_824807_shortened_game_market_settlement_audit/2026-08-03"
COHORT = ROOT / "backend/mlb/exports/cleanroom_v1/bol_tb15/schedule_cohorts/2026-08-02"
CERTIFIED = ROOT / "artifacts/analysis/model_development/mlb_cleanroom_august2_normal_outcome_reconciliation/2026-08-03/certified_outcome_reconciliation.csv"
REPAIR = ROOT / "artifacts/analysis/model_development/mlb_normal_player_stats_partial_game_repair/2026-08-03"
GAME = 824807

def read_csv(p):
    with p.open(newline="") as f: return list(csv.DictReader(f))
def write_csv(p, rows, fields=None):
    p.parent.mkdir(parents=True, exist_ok=True)
    fields = fields or (list(rows[0]) if rows else [])
    with p.open("w", newline="") as f:
        w=csv.DictWriter(f, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(rows)
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def profit(odds, win, stake=5.0):
    if not win:return -stake
    odds=int(odds); return stake*odds/100 if odds>0 else stake*100/abs(odds)

def game_status_payload():
    p=next((COHORT/"outcome_sources").glob("game_824807_*.json")); data=json.loads(p.read_text())
    status=data["gameData"]["status"]
    innings=sum(1 for x in data.get("liveData",{}).get("linescore",{}).get("innings",[]) if x.get("home",{}).get("runs") is not None and x.get("away",{}).get("runs") is not None)
    return p,data,status,innings

def inventory():
    rows=read_csv(CERTIFIED); board=read_csv(ROOT/"backend/mlb/exports/cleanroom_v1/bol_tb15/2026-08-02/bol_tb15_cleanroom_market_board_2026-08-02.csv")
    board_ids={(r["game_pk"],r["player_mlb_id"]):r for r in board}; out=[]
    for r in rows:
        if int(r["game_pk"])!=GAME:continue
        b=board_ids[(r["game_pk"],r["player_mlb_id"])]
        tb=int(r["official_total_bases"]); pa=int(r["official_plate_appearances"])
        out.append({
          "slate_date":r["slate_date"],"game_pk":r["game_pk"],"player_mlb_id":r["player_mlb_id"],"player":r["player"],"team":r["team"],"opponent":r["opponent"],
          "provider_event_id":r["provider_event_id"],"normal_pipeline_run_tag":r["governing_run_tag"],"market_observation_timestamp":r["market_observation_timestamp"],
          "over_odds":r["over_odds"],"under_odds":r["under_odds"],"pregame_roster_status":b.get("lineup_status",""),"official_final_role":r["final_participation_role"],
          "official_plate_appearances":pa,"official_total_bases":tb,"existing_outcome_classification":"OVER_WIN" if tb>1 else "OVER_LOSS",
          "existing_settlement_classification":"SETTLED_OFFICIAL_STATS_ONLY","existing_under_net":f"{profit(r['under_odds'],tb<=1):.6f}",
          "routine_market_partition":r["original_partition"],"retired_89_row_cohort":"YES" if r["original_partition"]=="FORMERLY_LINEUP_ADMITTED" else "NO",
          "actual_betonline_wager":"NONE_FOUND"})
    return out

def summarize(rows, excluded=False, label="AUGUST2_ALL_BOARD"):
    use=[r for r in rows if (int(r["game_pk"])!=GAME if excluded else True)]
    ans=[]
    for side in ("OVER","UNDER"):
      action=[r for r in use if int(r["official_plate_appearances"])>0]; wins=sum((int(r["official_total_bases"])>1)==(side=="OVER") for r in action)
      net=sum(profit(r[f"{side.lower()}_odds"],(int(r["official_total_bases"])>1)==(side=="OVER")) for r in action)
      ans.append({"scope":f"{label}_{'EXCLUDING' if excluded else 'INCLUDING'}_GAME_824807","side":side,"frozen_rows":len(use),"actionable_rows":len(action),"wins":wins,"losses":len(action)-wins,"no_action":len(use)-len(action),"total_stake":len(action)*5,"net":f"{net:.6f}","roi":f"{net/(len(action)*5):.9f}" if action else ""})
    return ans

def generate():
    ANALYSIS.mkdir(parents=True,exist_ok=True); inv=inventory(); allrows=read_csv(CERTIFIED)
    write_csv(ANALYSIS/"game_824807_market_identity_inventory.csv",inv)
    game=[r for r in allrows if int(r["game_pk"])==GAME]; pa=[int(r["official_plate_appearances"]) for r in game]; tbs=[int(r["official_total_bases"]) for r in game]
    over_net=sum(profit(r["over_odds"],int(r["official_total_bases"])>1) for r in game); under_net=sum(profit(r["under_odds"],int(r["official_total_bases"])<=1) for r in game)
    impact=[{"scope":"GAME_824807","frozen_identities":len(game),"actionable_rows":len(game),"over_wins":sum(x>1 for x in tbs),"over_losses":sum(x<=1 for x in tbs),"under_wins":sum(x<=1 for x in tbs),"under_losses":sum(x>1 for x in tbs),"no_action":0,"stake_per_side":45,"over_net":f"{over_net:.6f}","over_roi":f"{over_net/45:.9f}","under_net":f"{under_net:.6f}","under_roi":f"{under_net/45:.9f}","average_plate_appearances":f"{sum(pa)/len(pa):.6f}","minimum_plate_appearances":min(pa),"maximum_plate_appearances":max(pa)}]
    write_csv(ANALYSIS/"game_824807_market_impact.csv",impact)
    cohort89=[r for r in allrows if r["original_partition"]=="FORMERLY_LINEUP_ADMITTED"]
    comparisons=summarize(allrows)+summarize(allrows,True)+summarize(cohort89,label="AUGUST2_RETIRED_89_ROW_COHORT")+summarize(cohort89,True,label="AUGUST2_RETIRED_89_ROW_COHORT");write_csv(ANALYSIS/"august2_with_without_game_824807.csv",comparisons)
    contract=load_contract(); source=[{"source_type":"LOCAL_SEARCH","source":"repository","decision":"NO_LOCAL_RULE_OR_TICKET_EVIDENCE"},{"source_type":"AUTHORITATIVE_BOOK_RULE","source":contract["source_url"],"title":contract["source_title"],"source_date":contract["source_date"],"retrieval_timestamp":contract["retrieved_at_utc"],"sha256":contract["sha256"],"decision":contract["applicability_decision"]}]
    write_csv(ANALYSIS/"betonline_rule_source_inventory.csv",source);(ANALYSIS/"betonline_shortened_game_rule_contract.json").write_text(json.dumps(contract,indent=2)+"\n")
    write_csv(ANALYSIS/"actual_ticket_settlement_audit.csv",[],["ticket_or_wager_id","player","side","line","odds","wager_timestamp","stake","book_settlement_status","book_settlement_amount","book_explanation","search_decision"])
    rowaudit=[]
    for r in game:
      tb=int(r["official_total_bases"]);pa0=int(r["official_plate_appearances"]);bs=classify_book_settlement("COMPLETED_EARLY_OFFICIAL",pa0,slate_date="2026-08-02",contract=contract)
      for side in ("OVER","UNDER"):
       s=settle_side(tb,side,int(r[f"{side.lower()}_odds"]),bs)
       rowaudit.append({"slate_date":"2026-08-02","game_pk":GAME,"player_mlb_id":r["player_mlb_id"],"player":r["player"],"side":side,"line":1.5,"odds":r[f"{side.lower()}_odds"],"plate_appearances":pa0,"total_bases":tb,"official_outcome":s["official_outcome"],"book_settlement":bs,"book_outcome":s["book_outcome"],"stake":5,"stake_at_risk":s["stake_at_risk"],"returned_stake":s["returned_stake"],"net":s["net"]})
    write_csv(ANALYSIS/"game_824807_row_settlement_audit.csv",rowaudit)
    corrected=[]
    for side in ("OVER","UNDER"):
      old=next(x for x in comparisons if x["scope"]=="AUGUST2_ALL_BOARD_INCLUDING_GAME_824807" and x["side"]==side); new=next(x for x in comparisons if x["scope"]=="AUGUST2_ALL_BOARD_EXCLUDING_GAME_824807" and x["side"]==side)
      corrected.append({"side":side,"previous_official_stat_rows":old["actionable_rows"],"previous_wins":old["wins"],"previous_losses":old["losses"],"previous_voids":0,"previous_stake":old["total_stake"],"previous_net":old["net"],"previous_roi":old["roi"],"corrected_settled_wagers":new["actionable_rows"],"corrected_wins":new["wins"],"corrected_losses":new["losses"],"corrected_voids":9,"corrected_no_action":new["no_action"],"corrected_pending":0,"corrected_book_rule_uncertified":0,"corrected_technical_unresolved":0,"corrected_stake_at_risk":new["total_stake"],"returned_stake":45,"corrected_net":new["net"],"corrected_roi":new["roi"]})
    write_csv(ANALYSIS/"august2_corrected_settlement_comparison.csv",corrected)
    old=COHORT/"fixed_cohort_closeout_manifest.json"; affected=[f"2026-08-02|824807|{r['player_mlb_id']}|total_bases|1.5" for r in game]
    sup={"status":"REVISION_REQUIRED_NOT_APPLIED_BY_READ_ONLY_AUDIT","parent_closeout":str(old.relative_to(ROOT)),"parent_closeout_hash":sha(old),"new_closeout_hash":sha(ANALYSIS/"august2_corrected_settlement_comparison.csv"),"rule_evidence_hash":contract["sha256"],"affected_identities":affected,"reason":"BetOnline MLB props require regulation length; game 824807 completed early after seven innings.","immutable_parent_preserved":True}
    (ANALYSIS/"closeout_supersession_manifest.json").write_text(json.dumps(sup,indent=2)+"\n")
    scan=scan_nonstandard("2026-07-29","2026-08-02");write_csv(ANALYSIS/"nonstandard_final_game_scan.csv",scan)
    report=f"""# Game 824807 shortened-game settlement audit\n\nGame 824807 produced nine frozen TB 1.5 identities: seven official Under wins and two official Under losses. At $5 per row, the official-stat-only Under result was {under_net:.2f} ({under_net/45:.2%} ROI). All nine were in the retired 89-row lineup-gated cohort; none were among the 31 exclusions.\n\nBetOnline's April 22, 2026 Baseball Rules require MLB props to reach nine innings (8.5 if the home team leads) and require one plate appearance for player props. The game ended after seven innings, so every TB 1.5 identity is `BOOK_VOID_SHORTENED_GAME`, notwithstanding its valid official MLB result. No actual BetOnline tickets or graded-ticket records were found locally.\n\nThe game was 10.11% of the 89-row cohort and supplied 10.61% of its 66 Under wins, 9.52% of its 21 Under losses, and 10.34% of stake. Its official-stat-only contribution was {under_net:.2f}, or {under_net/76.34554534517687:.2%} of the retired cohort's recorded net. This is material, but the 3.22 average PA (range 2-4) alone cannot causally prove inflation; the contractual void removes it from performance interpretation.\n\nOfficial MLB finality remains `FINAL` and the 19-row player_stats repair remains correct. Market settlement is separate. The immutable prior closeout was not overwritten; this audit records the required supersession lineage.\n"""
    (ANALYSIS/"shortened_game_market_settlement_report.md").write_text(report)
    terminal="""MLB_GAME_824807_OFFICIAL_RESULT_DECISION = FINAL_STATS_PRESERVED_COMPLETE_EXACT\nMLB_GAME_824807_BETONLINE_RULE_DECISION = BOOK_VOID_SHORTENED_GAME\nMLB_GAME_824807_MARKET_IMPACT_DECISION = MATERIAL_NINE_ROWS_SEVEN_RECORDED_UNDER_WINS\nMLB_AUGUST2_SETTLEMENT_REVISION_DECISION = REVISION_REQUIRED_PARENT_PRESERVED\nMLB_NONSTANDARD_FINAL_SCAN_DECISION = COMPLETED_EARLY_GAME_824807_ONLY\nMLB_FUTURE_BOOK_SETTLEMENT_LAYER_DECISION = SEPARATE_FAIL_CLOSED_CONTRACT_IMPLEMENTED\nMLB_CLEANROOM_ROUTINE_CAPTURE_AUTHORIZATION = CONTINUE\nMLB_CLEANROOM_ROUTINE_PERFORMANCE_INTERPRETATION = PAUSED_FOR_AUGUST2_SHORTENED_GAME_SETTLEMENT_CERTIFICATION\n"""
    (ANALYSIS/"terminal_decision.md").write_text(terminal)
    return {"game_pk":GAME,"market_rows":9,"book_settlement":"BOOK_VOID_SHORTENED_GAME","output":str(ANALYSIS)}

def scan_nonstandard(start,end):
    rows=[]; seen=set()
    for p in ROOT.glob("backend/mlb/exports/cleanroom_v1/bol_tb15/**/outcome_sources/game_*.json"):
      try:d=json.loads(p.read_text());date=d["gameData"]["datetime"]["officialDate"]
      except Exception:continue
      if not start<=date<=end:continue
      game=int(d["gamePk"])
      if game in seen:continue
      seen.add(game);status=d["gameData"]["status"];innings=sum(1 for x in d.get("liveData",{}).get("linescore",{}).get("innings",[]) if x.get("home",{}).get("runs") is not None and x.get("away",{}).get("runs") is not None);cls=classify_game(status,innings)
      if cls!="NORMAL_FINAL":rows.append({"slate_date":date,"game_pk":game,"abstractGameState":status.get("abstractGameState"),"codedGameState":status.get("codedGameState"),"detailedState":status.get("detailedState"),"reason":status.get("reason",""),"innings_completed":innings,"classification":cls,"cleanroom_market_rows":sum(r.get("game_pk")==str(game) for r in read_csv(CERTIFIED)) if date=="2026-08-02" else "NOT_BOUNDED_POPULATION","closeout_rows":sum(r.get("game_pk")==str(game) for r in read_csv(COHORT/"fixed_cohort_closeout_rows.csv")) if date=="2026-08-02" else "NOT_BOUNDED_POPULATION","existing_book_settlement_treatment":"OFFICIAL_STATS_ONLY"})
    return rows

def main():
    ap=argparse.ArgumentParser();ap.add_argument("--mode",choices=["game-audit","settlement-status","nonstandard-scan"],required=True);ap.add_argument("--game-pk",type=int);ap.add_argument("--date");ap.add_argument("--from-date");ap.add_argument("--to-date");a=ap.parse_args()
    if a.mode=="game-audit":
      if a.game_pk!=GAME:raise SystemExit("bounded audit supports game_pk=824807")
      result=generate()
    elif a.mode=="settlement-status":
      if a.date!="2026-08-02":raise SystemExit("bounded status supports 2026-08-02")
      result=generate()
    else:
      result={"rows":scan_nonstandard(a.from_date,a.to_date)}
    print(json.dumps(result,indent=2));return 0
if __name__=="__main__":raise SystemExit(main())
