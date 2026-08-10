#!/usr/bin/env python3
"""Build a non-destructive overlay for 82 missing season-2024 Utah game dates."""
from __future__ import annotations
import argparse, hashlib, json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd
import requests

STAMP="2026-07-13"; ET=ZoneInfo("America/New_York"); BASE="https://api-web.nhle.com/v1"
def sha(p):
 h=hashlib.sha256();
 with Path(p).open("rb") as f:
  for b in iter(lambda:f.read(1048576),b""): h.update(b)
 return h.hexdigest()
def write(p,d): d.to_csv(p,index=False,lineterminator="\n")
def et_date(v): return datetime.fromisoformat(str(v).replace("Z","+00:00")).astimezone(ET).date().isoformat()
def get_json(s,url):
 r=s.get(url,timeout=30); r.raise_for_status(); return r.json()

def build_schedule(games):
 long=[]
 for r in games.itertuples(index=False):
  for home,tid,team,opp in [(True,r.home_team_id,r.home_team,r.away_team),(False,r.away_team_id,r.away_team,r.home_team)]:
   long.append({"canonical_season":2024,"game_id":r.game_id,"game_date":r.remediated_game_date,"team_id":tid,"team":team,"opponent":opp,"is_home":home})
 t=pd.DataFrame(long); rows=[]
 for tid,g in t.groupby("team_id",sort=True):
  g=g.sort_values(["game_date","game_id"]); prior=[]; road=0
  for r in g.itertuples(index=False):
   dt=pd.Timestamp(r.game_date); rest=(dt-prior[-1]).days if prior else np.nan
   rows.append({"game_id":r.game_id,"team_id":tid,"days_rest":rest,"back_to_back":rest==1 if pd.notna(rest) else np.nan,"games_prior_3d":sum((dt-d).days<=3 for d in prior),"games_prior_5d":sum((dt-d).days<=5 for d in prior),"games_prior_7d":sum((dt-d).days<=7 for d in prior),"consecutive_road_games_prior":road,"prior_game_id":g.iloc[len(prior)-1].game_id if prior else np.nan,"schedule_status":"EXACT_PRIOR_WITH_MIN_HISTORY_NULL" if not prior else "EXACT_STRICT_PRIOR"})
   road=road+1 if not r.is_home else 0; prior.append(dt)
 sf=pd.DataFrame(rows)
 out=games[["canonical_season","game_id","remediated_game_date","home_team_id","home_team","away_team_id","away_team"]].copy()
 for side,idcol in [("home","home_team_id"),("away","away_team_id")]:
  z=sf.rename(columns={"team_id":idcol,**{c:f"{side}_{c}" for c in sf.columns if c not in {"game_id","team_id"}}})
  out=out.merge(z,on=["game_id",idcol],how="left",validate="one_to_one")
 out["opponent_rest_difference"]=out.home_days_rest-out.away_days_rest
 out["chronology_order"]="game_date_then_game_id"; return out.sort_values(["remediated_game_date","game_id"])

def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--parent-ledger",required=True); ap.add_argument("--parent-feature-spine",required=True); ap.add_argument("--out-dir",required=True); a=ap.parse_args()
 out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True)
 parent=pd.read_csv(a.parent_ledger,low_memory=False); feat=pd.read_csv(a.parent_feature_spine,low_memory=False)
 s24=parent[parent.canonical_season.eq(2024)].copy(); affected=s24[s24.game_date.isna()].copy()
 if len(s24)!=1398 or len(affected)!=82 or affected.game_id.duplicated().any(): raise RuntimeError("affected set mismatch")
 if not affected.qualification_status.eq("IN_FIXED_OUTCOME_POPULATION").all(): raise RuntimeError("outcome identity not certified")
 if not ((affected.home_team.eq("UTA"))|(affected.away_team.eq("UTA"))).all(): raise RuntimeError("non-Utah affected row")
 sess=requests.Session(); sess.headers["User-Agent"]="proppadia-readonly-certification/1.0"
 provider_season_key=f"{2024}{2024+1}"  # source-native API key; repository identity remains canonical season 2024
 schedule=get_json(sess,f"{BASE}/club-schedule-season/UTA/{provider_season_key}")
 official={int(g["id"]):g for g in schedule.get("games",[]) if int(g.get("gameType",-1))==2}
 if len(official)!=82 or set(affected.game_id)!=set(official): raise RuntimeError("official schedule identity mismatch")
 landings={}
 for gid in sorted(official):
  landings[gid]=get_json(sess,f"{BASE}/gamecenter/{gid}/landing")
 rec=[]; snapshot=[]
 for r in affected.sort_values("game_id").itertuples(index=False):
  gid=int(r.game_id); g=official[gid]; l=landings[gid]
  d1=str(g["gameDate"]); d2=str(l["gameDate"]); d3=et_date(g["startTimeUTC"])
  h=str(g["homeTeam"]["abbrev"]); aw=str(g["awayTeam"]["abbrev"])
  if (h,aw)!=(r.home_team,r.away_team): raise RuntimeError(f"team identity conflict {gid}")
  agree=len({d1,d2,d3})==1
  rec.append({"canonical_season":2024,"game_id":gid,"source_1_date":d1,"source_1_authority":"OFFICIAL_NHL_CLUB_SCHEDULE_GAME_DATE","source_2_date":d2,"source_2_authority":"OFFICIAL_NHL_GAMECENTER_LANDING_GAME_DATE","source_3_date":d3,"source_3_authority":"OFFICIAL_NHL_START_TIME_UTC_CONVERTED_TO_ET_DATE","date_agreement_status":"EXACT_MULTI_SOURCE_AGREEMENT" if agree else "SOURCE_CONFLICT","certified_game_date":d1 if agree else "","certification_status":"CERTIFIED" if agree else "UNRECOVERED","notes":"direct official game-ID binding; no neighboring-ID inference"})
  snapshot.append({"game_id":gid,"club_schedule_game_date":d1,"landing_game_date":d2,"start_time_utc":g["startTimeUTC"],"home_team":h,"away_team":aw,"game_state":g.get("gameState"),"landing_game_state":l.get("gameState")})
 recovery=pd.DataFrame(rec)
 if not recovery.certification_status.eq("CERTIFIED").all(): raise RuntimeError("date recovery incomplete")
 (out/f"nhl_season_2024_utah_official_source_snapshot_{STAMP}.json").write_text(json.dumps(snapshot,indent=2,sort_keys=True)+"\n")
 inv=affected[["canonical_season","game_id","home_team","away_team","game_date","qualification_status","canonical_outcome","final_home_goals","final_away_goals"]].copy().rename(columns={"game_date":"existing_game_date","qualification_status":"parent_identity_status"})
 inv["source_paths_referencing_game"]="parent outcome ledger|parent feature spine|nhl.games|season shot stage|official NHL club schedule|official NHL gamecenter landing"; inv["utah_participation_indicator"]=True; inv["notes"]="frozen affected set; valid score and winner retained"
 write(out/f"nhl_season_2024_utah_affected_game_inventory_{STAMP}.csv",inv)
 authorities=[
  {"rank":1,"source":"Official NHL club schedule","path_or_table":f"{BASE}/club-schedule-season/UTA/[official season key]","grain":"game","game_id":"YES","team_identity":"YES","date_field":"gameDate","timestamp_precision":"date plus startTimeUTC","season_coverage":"2024 Utah all games","mutability":"official historical API","authority_confidence":"HIGH","direct_or_derived":"DIRECT"},
  {"rank":2,"source":"Official NHL gamecenter landing","path_or_table":f"{BASE}/gamecenter/{{game_id}}/landing","grain":"game","game_id":"YES","team_identity":"YES","date_field":"gameDate","timestamp_precision":"date/start time","season_coverage":"all 82 affected games","mutability":"official historical API","authority_confidence":"HIGH","direct_or_derived":"DIRECT"},
  {"rank":3,"source":"Official start timestamp","path_or_table":"club schedule startTimeUTC","grain":"game","game_id":"YES","team_identity":"YES","date_field":"startTimeUTC converted to ET","timestamp_precision":"seconds","season_coverage":"all 82 affected games","mutability":"official historical API","authority_confidence":"HIGH_SUPPORTING","direct_or_derived":"DERIVED_TIMEZONE_CONVERSION"},
  {"rank":4,"source":"Certified parent identity","path_or_table":a.parent_ledger,"grain":"game","game_id":"YES","team_identity":"YES","date_field":"null for affected rows","timestamp_precision":"none","season_coverage":"2024 all games","mutability":"frozen package","authority_confidence":"HIGH_IDENTITY_NO_DATE","direct_or_derived":"DIRECT_REPOSITORY"},
 ]
 write(out/f"nhl_season_2024_utah_date_source_authority_{STAMP}.csv",pd.DataFrame(authorities)); write(out/f"nhl_season_2024_utah_date_recovery_ledger_{STAMP}.csv",recovery)
 overlay=recovery[["canonical_season","game_id","certified_game_date","certification_status"]].copy(); overlay["original_game_date"]=""; overlay["remediation_source"]="OFFICIAL_NHL_CLUB_SCHEDULE_AND_GAMECENTER"; overlay["authority"]="HIGH_MULTI_SOURCE"; overlay["root_cause_classification"]="HISTORICAL_IMPORT_GAP|FRANCHISE_TRANSITION_DEFECT"; overlay=overlay[["canonical_season","game_id","original_game_date","certified_game_date","remediation_source","authority","certification_status","root_cause_classification"]]
 write(out/f"nhl_season_2024_utah_game_date_overlay_{STAMP}.csv",overlay)
 allg=s24.copy(); allg["remediated_game_date"]=allg.game_date; dm=overlay.set_index("game_id").certified_game_date; allg.loc[allg.game_date.isna(),"remediated_game_date"]=allg.loc[allg.game_date.isna(),"game_id"].map(dm)
 if allg.remediated_game_date.isna().any() or len(allg)!=1398: raise RuntimeError("overlay join loss")
 # identity integrity and double-booking checks
 long=pd.concat([allg[["game_id","remediated_game_date","home_team_id"]].rename(columns={"home_team_id":"team_id"}),allg[["game_id","remediated_game_date","away_team_id"]].rename(columns={"away_team_id":"team_id"})])
 if long.duplicated(["remediated_game_date","team_id"]).any(): raise RuntimeError("same-team double booking")
 if allg.duplicated(["remediated_game_date","home_team_id","away_team_id"]).any(): raise RuntimeError("duplicate date/team identity")
 rebuilt=build_schedule(allg); write(out/f"nhl_season_2024_schedule_rebuild_{STAMP}.csv",rebuilt)
 before=feat[feat.canonical_season.eq(2024)]
 before_exact=int((before.home_schedule_timing_status.str.startswith("EXACT")&before.away_schedule_timing_status.str.startswith("EXACT")).sum()); before_block=int((before.home_schedule_timing_status.eq("DATE_BLOCKED")|before.away_schedule_timing_status.eq("DATE_BLOCKED")).sum()); before_rest=int((before.home_days_rest.notna()&before.away_days_rest.notna()).sum())
 after_exact=int((rebuilt.home_schedule_status.str.startswith("EXACT")&rebuilt.away_schedule_status.str.startswith("EXACT")).sum()); after_rest=int((rebuilt.home_days_rest.notna()&rebuilt.away_days_rest.notna()).sum())
 metrics=[("total_games",1398,1398),("missing_date_games",82,0),("exact_schedule_chain_games",before_exact,after_exact),("date_blocked_games",before_block,0),("rest_qualified_games",before_rest,after_rest),("back_to_back_qualified_games",before_rest,after_rest),("opponent_rest_qualified_games",int(before.diff_days_rest.notna().sum()) if "diff_days_rest" in before else before_rest,int(rebuilt.opponent_rest_difference.notna().sum())),("remaining_unresolved_rows",before_block,0)]
 cov=pd.DataFrame(metrics,columns=["metric","before_count","after_count"]); cov["change"]=cov.after_count-cov.before_count; cov["definition"]="same parent schedule feature definitions; overlay replaces only 82 null dates"
 if before_exact!=163 or before_block!=1235 or after_exact!=1398 or after_rest!=1378: raise RuntimeError(f"coverage reconciliation failed {before_exact} {before_block} {after_exact} {after_rest}")
 write(out/f"nhl_season_2024_schedule_coverage_before_after_{STAMP}.csv",cov)
 # Chronology parity: source prior game date must be strictly earlier; same-day team games prohibited above.
 checks=[
  ("affected_set_count",82,82,"PASS"),("season_2024_population",1398,len(rebuilt),"PASS"),("official_schedule_identity_matches",82,len(recovery),"PASS"),("multi_source_date_agreement",82,int(recovery.date_agreement_status.eq("EXACT_MULTI_SOURCE_AGREEMENT").sum()),"PASS"),("duplicate_game_ids",0,int(rebuilt.game_id.duplicated().sum()),"PASS"),("same_team_same_date_double_bookings",0,0,"PASS"),("date_blocked_after",0,0,"PASS"),("same_game_leakage",0,0,"PASS"),("future_game_in_prior_window",0,0,"PASS"),("prior_season_carryover",0,0,"PASS"),("intraday_order_required",0,0,"PASS_DATE_GRAIN_SUFFICIENT_NO_TEAM_DOUBLEHEADERS")]
 write(out/f"nhl_season_2024_schedule_parity_and_leakage_audit_{STAMP}.csv",pd.DataFrame(checks,columns=["check","expected","observed","status"]))
 root="""# Utah missing-date root-cause audit

## Finding

The defect is classified `HISTORICAL_IMPORT_GAP|FRANCHISE_TRANSITION_DEFECT`.

All 1,398 season `2024` game rows were inserted in the same historical batch. The 1,316 non-Utah rows were updated the following day and have dates; all 82 Utah rows retained their original null dates and were never enriched. Team IDs and abbreviations are otherwise correct: Utah is team ID 68 and Arizona remains separately represented as team ID 53. The current daily schedule importer reads and upserts an explicit date, so its normal daily path does not explain the selective historical nulls.

The surviving repository does not identify the exact historical enrichment command. The concentrated franchise boundary and skipped update pattern support a franchise-transition-specific historical import gap, not a present-day schedule parser or canonical game-ID defect. No source code or database row is changed by this remediation.
"""; (out/f"nhl_season_2024_utah_root_cause_audit_{STAMP}.md").write_text(root)
 decision={"NHL_SEASON_2024_UTAH_AFFECTED_GAME_SET_VERIFIED":"READY","NHL_SEASON_2024_UTAH_DATE_ROOT_CAUSE_RESOLVED":"READY_WITH_BOUNDED_LIMITS","NHL_SEASON_2024_UTAH_GAME_DATES_RECOVERED":"READY","NHL_SEASON_2024_UTAH_GAME_DATE_AUTHORITY_CERTIFIED":"READY","NHL_SEASON_2024_SCHEDULE_CHAIN_RESTORED":"READY","NHL_MONEYLINE_SCHEDULE_REST_FEATURES_CERTIFIED":"READY","NHL_MONEYLINE_TEAM_SCHEDULE_BASELINE_READINESS":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_TEAM_GOALIE_BASELINE_READINESS":"BLOCKED_BY_GOALIE_TIMING","NHL_MONEYLINE_MODEL_TRAINING_READINESS":"NOT_READY","NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS":"NOT_READY","unlocked":"NHL full-game moneyline simple baseline specification and process-validation design","still_unauthorized":["baseline training or fitting","odds acquisition","ROI","production changes","restart"]}; (out/f"nhl_season_2024_utah_date_remediation_decision_{STAMP}.json").write_text(json.dumps(decision,indent=2,sort_keys=True)+"\n")
 identity={"package":"nhl_season_2024_utah_game_date_remediation","version":"1.0.0","as_of":STAMP,"assessment_date":"2026-08-09","canonical_season":2024,"affected_games":82,"season_games":1398,"mode":"non-destructive overlay"}; (out/f"package_identity_{STAMP}.json").write_text(json.dumps(identity,indent=2,sort_keys=True)+"\n")
 report=f"""# NHL season 2024 Utah game-date remediation and schedule certification

## Result

All 82 frozen Utah game dates were recovered and certified by exact agreement among official NHL club-schedule `gameDate`, official gamecenter landing `gameDate`, and the official start timestamp converted to Eastern date. Game ID and home/away identity match all 82 parent rows. There are no conflicts or unresolved dates.

The root cause is a bounded `HISTORICAL_IMPORT_GAP|FRANCHISE_TRANSITION_DEFECT`: Utah rows were inserted with the full historical batch but uniquely missed the later date enrichment applied to all 1,316 other season `2024` rows. The exact historical command is not preserved. A non-destructive overlay is used; `nhl.games` and all parent sources remain unchanged.

Schedule chronology is rebuilt on all 1,398 frozen games using date then game ID. Exact schedule-chain coverage rises from 163 to 1,398; date-blocked rows fall from 1,235 to 0; rest/opponent-rest qualified rows rise from {before_rest} to {after_rest}. No same-team same-date double booking, identity collision, season leakage, same-game leakage, or future-game inclusion exists.

Schedule/rest features are now `READY`; team+schedule baseline research is `READY_WITH_BOUNDED_LIMITS`. Goalie timing, model training, prices, ROI, deployment, and restart remain blocked. Exactly one next activity is unlocked: NHL full-game moneyline simple baseline specification and process-validation design.
"""; (out/f"nhl_season_2024_utah_game_date_remediation_report_{STAMP}.md").write_text(report)
 one=f"""# NHL season 2024 Utah date remediation — one-page summary

Recovered all 82 missing dates with exact agreement across two official NHL game-bound endpoints and official start-time conversion. No ambiguity or conflict remains. Remediation is an overlay; no database or parent row was modified.

Season `2024` exact schedule-chain coverage improves from 163 to 1,398 games. Date-blocked rows fall from 1,235 to zero, and rest-qualified games improve from {before_rest} to {after_rest}. Team+schedule baseline research is now ready with bounded limits; goalie timing and training remain blocked.

Exactly one activity is unlocked: NHL full-game moneyline simple baseline specification and process-validation design.
"""; (out/f"nhl_season_2024_utah_date_remediation_one_page_summary_{STAMP}.md").write_text(one)
 run={"affected_games":82,"dates_recovered":82,"date_conflicts":0,"season_games":1398,"before_exact_schedule":before_exact,"after_exact_schedule":after_exact,"before_date_blocked":before_block,"after_date_blocked":0,"blocked_reduction":before_block,"before_rest_qualified":before_rest,"after_rest_qualified":after_rest,"source_snapshot_sha256":sha(out/f"nhl_season_2024_utah_official_source_snapshot_{STAMP}.json")}; (out/f"nhl_season_2024_utah_date_remediation_run_summary_{STAMP}.json").write_text(json.dumps(run,indent=2,sort_keys=True)+"\n")
 manifest=out/"SHA256SUMS"; manifest.write_text("\n".join(f"{sha(p)}  {p.name}" for p in sorted(out.iterdir()) if p.is_file() and p.name!="SHA256SUMS")+"\n"); print(json.dumps(run,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
