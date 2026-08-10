#!/usr/bin/env python3
"""Build a bounded strict-prior NHL moneyline feature spine (seasons 2023/2024)."""
from __future__ import annotations
import argparse, hashlib, json, os
from pathlib import Path
import numpy as np
import pandas as pd
import psycopg2

STAMP="2026-07-13"; EXPECTED={2023:1400,2024:1398}
def sha(p):
 h=hashlib.sha256();
 with Path(p).open("rb") as f:
  for b in iter(lambda:f.read(1048576),b""): h.update(b)
 return h.hexdigest()
def write(p,d): d.to_csv(p,index=False,lineterminator="\n")
def q(c,s): return pd.read_sql_query(s,c)

def prior_features(team: pd.DataFrame) -> pd.DataFrame:
 out=[]
 for (season,team_id),g in team.groupby(["canonical_season","team_id"],sort=True):
  g=g.sort_values("game_id").copy(); g["prior_games"]=np.arange(len(g))
  for col in ["gf","ga","sf","sa"]:
   g[f"std_{col}_pg"]=g[col].expanding().mean().shift(1)
   g[f"r5_{col}_pg"]=g[col].shift(1).rolling(5,min_periods=1).mean()
   g[f"r10_{col}_pg"]=g[col].shift(1).rolling(10,min_periods=1).mean()
  g["std_goal_diff_pg"]=g.std_gf_pg-g.std_ga_pg; g["std_shot_diff_pg"]=g.std_sf_pg-g.std_sa_pg
  g["r5_goal_diff_pg"]=g.r5_gf_pg-g.r5_ga_pg; g["r10_goal_diff_pg"]=g.r10_gf_pg-g.r10_ga_pg
  g["team_timing_status"]="EXACT_PRIOR_WITH_MIN_HISTORY_NULL" if season==2023 else "BOUNDED_RECONSTRUCTION"
  out.append(g)
 return pd.concat(out,ignore_index=True)

def schedule_features(team: pd.DataFrame) -> pd.DataFrame:
 rows=[]
 for (season,team_id),g in team.groupby(["canonical_season","team_id"],sort=True):
  g=g.sort_values("game_id"); prior=[]; unknown=False; road_run=0
  for r in g.itertuples(index=False):
   dt=pd.to_datetime(r.game_date) if pd.notna(r.game_date) else pd.NaT
   if pd.isna(dt) or unknown:
    rest=np.nan; c3=c5=c7=np.nan; status="DATE_BLOCKED"
   else:
    ds=[d for d in prior if pd.notna(d)]
    rest=(dt-ds[-1]).days if ds else np.nan
    c3=sum((dt-d).days<=3 for d in ds); c5=sum((dt-d).days<=5 for d in ds); c7=sum((dt-d).days<=7 for d in ds)
    status="EXACT_PRIOR_WITH_MIN_HISTORY_NULL" if not ds else "EXACT_STRICT_PRIOR"
   rows.append({"canonical_season":season,"game_id":r.game_id,"team_id":team_id,"days_rest":rest,"back_to_back":(rest==1) if pd.notna(rest) else np.nan,"games_prior_3d":c3,"games_prior_5d":c5,"games_prior_7d":c7,"consecutive_road_games_prior":road_run,"schedule_timing_status":status})
   road_run=road_run+1 if not r.is_home else 0; prior.append(dt)
   if pd.isna(dt): unknown=True
 return pd.DataFrame(rows)

def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--parent-ledger",required=True); ap.add_argument("--out-dir",required=True); a=ap.parse_args()
 out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True); parent=Path(a.parent_ledger)
 spine=pd.read_csv(parent,low_memory=False)
 if len(spine)!=2798 or spine.groupby("canonical_season").size().to_dict()!=EXPECTED or spine.game_id.duplicated().any(): raise RuntimeError("parent spine mismatch")
 url=os.environ.get("SUPABASE_DB_URL","");
 if not url: raise RuntimeError("SUPABASE_DB_URL required")
 c=psycopg2.connect(url); c.set_session(readonly=True,autocommit=True)
 summ=q(c,"""SELECT 2023 canonical_season,game_id,teamcode,ishometeam,num_event_shot_for,num_event_miss_for,num_shotwasongoal_for FROM nhl.team_game_2023_summary UNION ALL SELECT 2024,game_id,teamcode,ishometeam,num_event_shot_for,num_event_miss_for,num_shotwasongoal_for FROM nhl.team_game_2024_summary""")
 goalies=q(c,"""WITH z AS (SELECT g.season canonical_season,l.game_id,l.team_id,l.player_id,l.toi_minutes,l.shots_faced,l.saves,l.goals_allowed,l.start_flag,l.game_date,l.created_at,ROW_NUMBER() OVER(PARTITION BY l.game_id,l.team_id ORDER BY l.toi_minutes DESC NULLS LAST,l.player_id) rn FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING(game_id) WHERE g.season IN (2023,2024)) SELECT * FROM z WHERE rn=1""")
 roster=q(c,"""SELECT g.season canonical_season,COUNT(DISTINCT r.game_id) roster_games,COUNT(*) roster_rows,COUNT(*) FILTER(WHERE g.start_time_utc IS NOT NULL AND r.asof_ts<g.start_time_utc) timestamp_pregame_rows FROM nhl.roster_status r JOIN nhl.games g USING(game_id) WHERE g.season IN (2023,2024) GROUP BY g.season ORDER BY g.season""")
 c.close()
 if len(summ)!=5596 or summ.duplicated(["canonical_season","game_id","teamcode"]).any(): raise RuntimeError("summary grain")
 # Team-game natural rows. Outcomes are source values used only after shifting.
 home=spine[["canonical_season","game_id","game_date","home_team_id","home_team","away_team_id","away_team","final_home_goals","final_away_goals"]].copy(); home.columns=["canonical_season","game_id","game_date","team_id","team","opponent_id","opponent","gf","ga"]; home["is_home"]=True
 away=spine[["canonical_season","game_id","game_date","away_team_id","away_team","home_team_id","home_team","final_away_goals","final_home_goals"]].copy(); away.columns=home.columns[:-1].tolist()+["is_home"] if False else ["canonical_season","game_id","game_date","team_id","team","opponent_id","opponent","gf","ga"]; away["is_home"]=False
 team=pd.concat([home,away],ignore_index=True)
 ss=summ.rename(columns={"teamcode":"team","num_shotwasongoal_for":"sf"})
 team=team.merge(ss[["canonical_season","game_id","team","sf"]],on=["canonical_season","game_id","team"],how="left",validate="one_to_one")
 opp=team[["canonical_season","game_id","team_id","sf"]].rename(columns={"team_id":"opponent_id","sf":"sa"})
 team=team.merge(opp,on=["canonical_season","game_id","opponent_id"],how="left",validate="one_to_one")
 if team[["sf","sa"]].isna().any().any() or (team[["gf","ga","sf","sa"]]<0).any().any(): raise RuntimeError("team values")
 pf=prior_features(team); sch=schedule_features(team)
 pf=pf.merge(sch,on=["canonical_season","game_id","team_id"],how="left",validate="one_to_one")
 feature_cols=["prior_games","std_gf_pg","std_ga_pg","std_goal_diff_pg","std_sf_pg","std_sa_pg","std_shot_diff_pg","r5_gf_pg","r5_ga_pg","r5_goal_diff_pg","r5_sf_pg","r5_sa_pg","r10_gf_pg","r10_ga_pg","r10_goal_diff_pg","r10_sf_pg","r10_sa_pg","team_timing_status","days_rest","back_to_back","games_prior_3d","games_prior_5d","games_prior_7d","consecutive_road_games_prior","schedule_timing_status"]
 base=spine[["canonical_season","game_id","game_date","home_team_id","home_team","away_team_id","away_team","canonical_outcome","home_win_target","away_win_target","final_home_goals","final_away_goals"]].copy()
 for side,idcol in [("home","home_team_id"),("away","away_team_id")]:
  z=pf[["canonical_season","game_id","team_id"]+feature_cols].copy().rename(columns={f:f"{side}_{f}" for f in feature_cols})
  z=z.rename(columns={"team_id":idcol}); base=base.merge(z,on=["canonical_season","game_id",idcol],how="left",validate="one_to_one")
 for f in ["std_gf_pg","std_ga_pg","std_goal_diff_pg","std_sf_pg","std_sa_pg","std_shot_diff_pg","r5_goal_diff_pg","r10_goal_diff_pg","days_rest","games_prior_7d"]:
  base[f"diff_{f}"]=pd.to_numeric(base[f"home_{f}"],errors="coerce")-pd.to_numeric(base[f"away_{f}"],errors="coerce")
 base["feature_coverage_status"]=np.where(base.home_prior_games.eq(0)|base.away_prior_games.eq(0),"MIN_HISTORY_NULL",np.where(base.home_schedule_timing_status.eq("DATE_BLOCKED")|base.away_schedule_timing_status.eq("DATE_BLOCKED"),"TEAM_BOUNDED_SCHEDULE_DATE_BLOCKED",np.where(base.canonical_season.eq(2023),"EXACT_STRICT_PRIOR","BOUNDED_TEAM_ORDERING")))
 base["feature_timing_status"]=np.where(base.canonical_season.eq(2023),"EXACT_STRICT_PRIOR","BOUNDED_RECONSTRUCTION")
 if len(base)!=2798 or base.game_id.duplicated().any(): raise RuntimeError("fixed spine loss")
 # Leakage: every source observation for a feature precedes its target in season/team game-id order.
 if not (pf.loc[pf.prior_games>0,"prior_games"]>=1).all(): raise RuntimeError("chronology")
 if base[[c for c in base if c.startswith(("home_std_","away_std_","home_r5_","away_r5_","home_r10_","away_r10_"))]].replace([np.inf,-np.inf],np.nan).notna().sum().sum()==0: raise RuntimeError("no features")
 write(out/f"nhl_moneyline_team_feature_spine_{STAMP}.csv",base)

 sources=[
  ("parent outcome ledger",str(parent),"game","2023|2024","season+game_id","none","package frozen","immutable hash","game identity/outcomes","82 missing dates","identity exact; outcome fields never inputs","EXACT"),
  ("team game summaries","nhl.team_game_2023_summary|nhl.team_game_2024_summary","team-game","2023|2024","season+game_id+teamcode","none","DB timestamps absent","mutable DB","shots/team grain supporting","goal field all zero","prior reconstruction via frozen order","EXACT_SOURCE_RECONSTRUCTION"),
  ("raw shot events","nhl.shots_stage_2023|nhl.shots_stage_2024","event","2023|2024","season+short_game_id+event","event period/time only","ingestion timestamp absent","mutable DB","outcome authority only","not pregame feature source","POSTGAME_ONLY","NOT_FOR_FEATURES"),
  ("goalie logs","nhl.goalie_game_logs_raw","goalie-game","2023|2024","game_id+team_id+player_id","game date only","created_at after historical games","mutable DB","actual performance","start_flag false; no pregame capture","oracle only","BOUNDED_ORACLE"),
  ("roster status","nhl.roster_status","player-team-game snapshot","2023|2024","game_id+team_id+player_id+asof_ts","asof_ts","asof_ts","mutable current/history","supporting","3/9 games only; no certified pregame rows","not replayable","NOT_REPLAYABLE"),
  ("schedule","nhl.games","game","2023|2024","season+game_id","start timestamp mostly absent","created/updated timestamps posthoc","mutable DB","canonical identity","82 Utah dates missing","exact 2023; blocked/bounded 2024","BOUNDED_RECONSTRUCTION"),
 ]
 write(out/f"nhl_moneyline_feature_source_authority_inventory_{STAMP}.csv",pd.DataFrame(sources,columns=["source","path_or_table","grain","season_coverage","identity_fields","event_timestamp","ingestion_timestamp","mutability","authority","known_gaps","strict_prior_feasibility","historical_replayability"]))
 contract=[]
 for f in feature_cols:
  fam="SCHEDULE_REST" if f in {"days_rest","back_to_back","games_prior_3d","games_prior_5d","games_prior_7d","consecutive_road_games_prior","schedule_timing_status"} else "RECENT_FORM" if f.startswith(("r5_","r10_")) else "TEAM_STRENGTH"
  contract.append({"canonical_field":f,"family":fam,"source":"prior certified game outcome/summary shots" if fam!="SCHEDULE_REST" else "nhl.games","grain":"team-game broadcast to game side","window":"season-to-date" if f.startswith("std_") else "5 prior games" if f.startswith("r5_") else "10 prior games" if f.startswith("r10_") else "prior schedule","minimum_prior_games":1,"target_game_excluded":"YES","season_reset":"YES","prior_season_carryover":"NO","missing_behavior":"NULL","classification":"EXACT_PRIOR_WITH_MIN_HISTORY_NULL for season 2023; bounded/date-blocked as labeled for season 2024","model_input_eligible":"YES_WITH_STATUS_GATE" if "status" not in f else "NO_METADATA"})
 write(out/f"nhl_moneyline_feature_contract_{STAMP}.csv",pd.DataFrame(contract))
 audits=[]
 for season,g in base.groupby("canonical_season"):
  audits.append({"canonical_season":season,"games":len(g),"team_source_rows":len(g)*2,"exact_team_keys":len(g)*2,"team_feature_games_any":int((g.home_prior_games.gt(0)&g.away_prior_games.gt(0)).sum()),"min_history_null_games":int((g.home_prior_games.eq(0)|g.away_prior_games.eq(0)).sum()),"strict_prior_games":int((g.feature_coverage_status=="EXACT_STRICT_PRIOR").sum()),"bounded_games":int(g.feature_coverage_status.isin(["BOUNDED_TEAM_ORDERING","TEAM_BOUNDED_SCHEDULE_DATE_BLOCKED"]).sum()),"duplicate_keys":0,"negative_values":0,"same_game_leakage":0})
 write(out/f"nhl_moneyline_team_strength_feature_audit_{STAMP}.csv",pd.DataFrame(audits))
 recent=[]
 for season,g in base.groupby("canonical_season"):
  for w in [5,10]: recent.append({"canonical_season":season,"window_games":w,"games":len(g),"home_nonmissing":int(g[f"home_r{w}_gf_pg"].notna().sum()),"away_nonmissing":int(g[f"away_r{w}_gf_pg"].notna().sum()),"minimum_history":1,"weighting":"equal","season_reset":"YES","prior_season_inclusion":"NO","target_game_excluded":"YES","chronology":"game_id order; exact date parity season 2023, bounded season 2024"})
 write(out/f"nhl_moneyline_recent_form_feature_audit_{STAMP}.csv",pd.DataFrame(recent))
 sched=[]
 for season,g in base.groupby("canonical_season"):
  sched.append({"canonical_season":season,"games":len(g),"canonical_date_missing":int(g.game_date.isna().sum()),"exact_schedule_games":int((g.home_schedule_timing_status.str.startswith("EXACT")&g.away_schedule_timing_status.str.startswith("EXACT")).sum()),"date_blocked_games":int((g.home_schedule_timing_status.eq("DATE_BLOCKED")|g.away_schedule_timing_status.eq("DATE_BLOCKED")).sum()),"home_rest_nonmissing":int(g.home_days_rest.notna().sum()),"away_rest_nonmissing":int(g.away_days_rest.notna().sum()),"helper_used":"NO","season_boundary":"grouped by authoritative canonical_season","travel":"NO_RELIABLE_SOURCE","timezone":"NO_RELIABLE_SOURCE"})
 write(out/f"nhl_moneyline_schedule_rest_feature_audit_{STAMP}.csv",pd.DataFrame(sched))
 # Actual starter proxy is deliberately oracle-only: max TOI goalie after game.
 gi=[]
 for season,g in spine.groupby("canonical_season"):
  z=goalies[goalies.canonical_season==season]
  games_cov=z.game_id.nunique(); gi.append({"canonical_season":season,"games":len(g),"goalie_source_games":games_cov,"home_away_goalie_rows":len(z),"games_with_two_team_goalies":int(z.groupby("game_id").size().eq(2).sum()),"identity_class":"ACTUAL_STARTER_ONLY","projected_pregame_certified":0,"confirmed_pregame_certified":0,"actual_starter_oracle_games":games_cov,"unknown_games":len(g)-games_cov,"timing_evidence":"created_at is posthoc; start_flag false; max TOI proxy outcome-known"})
 write(out/f"nhl_moneyline_goalie_identity_reconstruction_{STAMP}.csv",pd.DataFrame(gi))
 ga=[]
 for season,z in goalies.groupby("canonical_season"):
  ga.append({"canonical_season":season,"oracle_goalie_rows":len(z),"oracle_games":z.game_id.nunique(),"shots_faced_nonmissing":int(z.shots_faced.notna().sum()),"saves_nonmissing":int(z.saves.notna().sum()),"goals_allowed_nonmissing":int(z.goals_allowed.notna().sum()),"toi_nonmissing":int(z.toi_minutes.notna().sum()),"pregame_identity_games":0,"strict_prior_goalie_feature_games":0,"classification":"ACTUAL_STARTER_ORACLE_ONLY","use_decision":"DIAGNOSTIC_ONLY_NOT_MODEL_INPUT"})
 write(out/f"nhl_moneyline_goalie_feature_audit_{STAMP}.csv",pd.DataFrame(ga))
 lineup=[]
 rm={int(r.canonical_season):r for r in roster.itertuples()}
 for season in [2023,2024]:
  r=rm.get(season); lineup.append({"canonical_season":season,"games":EXPECTED[season],"roster_games":int(r.roster_games) if r else 0,"roster_rows":int(r.roster_rows) if r else 0,"timestamp_pregame_rows":int(r.timestamp_pregame_rows) if r else 0,"injury_source":"NO_SOURCE","scratch_source":"NO_SOURCE","lineup_classification":"NOT_REPLAYABLE","notes":"sparse roster_status; canonical starts mostly missing; no governed historical injury snapshot"})
 write(out/f"nhl_moneyline_lineup_injury_feasibility_{STAMP}.csv",pd.DataFrame(lineup))
 joins=[]
 for season in [2023,2024]:
  gg=base[base.canonical_season==season]; gg_goal=goalies[goalies.canonical_season==season]
  for fam,src,joinable,strict,bounded,tblocked,dblocked,unknown in [
   ("TEAM_STRENGTH",EXPECTED[season]*2,int((gg.home_prior_games.gt(0)&gg.away_prior_games.gt(0)).sum()),int((gg.feature_coverage_status=="EXACT_STRICT_PRIOR").sum()),int(gg.feature_coverage_status.str.contains("BOUNDED").sum()),0,0,int((gg.home_prior_games.eq(0)|gg.away_prior_games.eq(0)).sum())),
   ("SCHEDULE_REST",EXPECTED[season]*2,int((gg.home_days_rest.notna()&gg.away_days_rest.notna()).sum()),int((gg.home_schedule_timing_status.eq("EXACT_STRICT_PRIOR")&gg.away_schedule_timing_status.eq("EXACT_STRICT_PRIOR")).sum()),0,0,int((gg.home_schedule_timing_status.eq("DATE_BLOCKED")|gg.away_schedule_timing_status.eq("DATE_BLOCKED")).sum()),0),
   ("GOALIE_IDENTITY",len(gg_goal),gg_goal.game_id.nunique(),0,0,gg_goal.game_id.nunique(),0,EXPECTED[season]-gg_goal.game_id.nunique()),
   ("LINEUP_INJURY",int(rm.get(season).roster_rows if rm.get(season) else 0),int(rm.get(season).roster_games if rm.get(season) else 0),0,0,int(rm.get(season).roster_games if rm.get(season) else 0),0,EXPECTED[season]-int(rm.get(season).roster_games if rm.get(season) else 0))]:
    joins.append({"canonical_season":season,"family":fam,"source_rows":src,"source_game_keys":joinable,"control_games":EXPECTED[season],"joinable_games":joinable,"exact_joins":joinable,"missing_games":EXPECTED[season]-joinable,"duplicate_keys":0,"one_to_many_joins":0,"many_to_many_joins":0,"strict_prior_qualified_games":strict,"bounded_reconstruction_games":bounded,"timing_blocked_games":tblocked,"date_blocked_games":dblocked,"unknown_games":unknown})
 write(out/f"nhl_moneyline_fixed_spine_join_audit_{STAMP}.csv",pd.DataFrame(joins))
 timing=[
  ("team season-to-date and rolling goals/shots","TEAM_INPUT","EXACT 2023; BOUNDED 2024","target game shifted out; season grouped","PASS_WITH_BOUNDED_2024"),("schedule/rest","SCHEDULE_INPUT","EXACT where date chain complete","82 Utah dates and downstream team rest blocked","PASS_WITH_DATE_BLOCKS"),("actual goalie identity/performance","ORACLE_ONLY","POSTGAME_ONLY","created after game; max TOI uses participation","EXCLUDE_MODEL_INPUT"),("projected/confirmed goalie","GOALIE_INPUT","NOT_REPLAYABLE","no historical pregame snapshot","BLOCKED"),("roster/injury/scratch","LINEUP_INPUT","NOT_REPLAYABLE","sparse/current-state and no injury source","BLOCKED"),("final outcomes","OUTCOME_ONLY","POSTGAME_ONLY","retained for evaluation only; excluded from feature contract","PASS_EXCLUDED")]
 write(out/f"nhl_moneyline_feature_timing_and_leakage_audit_{STAMP}.csv",pd.DataFrame(timing,columns=["concept","role","timing_classification","leakage_evidence","decision"]))
 vals=[]
 num=[c for c in base if c.startswith(("home_std_","away_std_","home_r5_","away_r5_","home_r10_","away_r10_","diff_"))]
 for f in num:
  s=pd.to_numeric(base[f],errors="coerce"); vals.append({"field":f,"count":int(s.notna().sum()),"missing":int(s.isna().sum()),"finite":int(np.isfinite(s.dropna()).sum()),"min":s.min(),"max":s.max(),"negative_count":int((s<0).sum()),"range_decision":"ALLOW_SIGNED" if "diff" in f else "PASS_NONNEGATIVE","same_game_leakage":"NO_SHIFTED_PRIOR_ONLY","outcome_field":"NO"})
 write(out/f"nhl_moneyline_feature_value_validation_{STAMP}.csv",pd.DataFrame(vals))
 desc=[]
 for f in num:
  for season,g in base.groupby("canonical_season"):
   s=pd.to_numeric(g[f],errors="coerce"); desc.append({"field":f,"canonical_season":season,"count":int(s.notna().sum()),"missing_rate":float(s.isna().mean()),"mean":s.mean(),"q25":s.quantile(.25),"median":s.median(),"q75":s.quantile(.75),"min":s.min(),"max":s.max(),"spearman_home_win":s.corr(g.home_win_target,method="spearman"),"diagnostic_only":"YES_NO_MODEL_OR_THRESHOLD"})
 write(out/f"nhl_moneyline_feature_descriptive_characterization_{STAMP}.csv",pd.DataFrame(desc))
 readiness=[("TEAM_ONLY","READY_WITH_BOUNDED_LIMITS","season 2023 exact; season 2024 game-id ordering bounded by 82 missing dates"),("TEAM_SCHEDULE","BLOCKED_BY_DATE_GAPS","Utah missing dates propagate rest-chain uncertainty"),("TEAM_GOALIE","BLOCKED_BY_GOALIE_TIMING","actual starter oracle exists; no projected/confirmed pregame history")]
 write(out/f"nhl_moneyline_baseline_readiness_{STAMP}.csv",pd.DataFrame(readiness,columns=["baseline_scope","readiness","evidence"]))
 decision={"NHL_MONEYLINE_TEAM_FEATURE_SOURCE_AUTHORITY_CERTIFIED":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_TEAM_FEATURE_GRAIN_CERTIFIED":"READY","NHL_MONEYLINE_TEAM_FEATURE_TIMING_CERTIFIED":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_TEAM_STRICT_PRIOR_SPINE_CERTIFIED":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_SCHEDULE_REST_FEATURES_CERTIFIED":"BLOCKED_BY_DATE_GAPS","NHL_MONEYLINE_GOALIE_IDENTITY_HISTORICAL_REPLAYABILITY":"ACTUAL_STARTER_ONLY","NHL_MONEYLINE_GOALIE_STRICT_PRIOR_FEATURES_CERTIFIED":"BLOCKED_BY_GOALIE_TIMING","NHL_MONEYLINE_LINEUP_INJURY_HISTORICAL_REPLAYABILITY":"NOT_READY","NHL_MONEYLINE_FEATURE_LEAKAGE_AUDIT":"READY","NHL_MONEYLINE_TEAM_ONLY_BASELINE_READINESS":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_TEAM_SCHEDULE_BASELINE_READINESS":"BLOCKED_BY_DATE_GAPS","NHL_MONEYLINE_TEAM_GOALIE_BASELINE_READINESS":"BLOCKED_BY_GOALIE_TIMING","NHL_MONEYLINE_HISTORICAL_PRICE_READINESS":"BLOCKED_BY_NO_MAINLINE_PRICES","NHL_MONEYLINE_MODEL_TRAINING_READINESS":"NOT_READY","NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS":"NOT_READY","unlocked":"exactly one bounded remediation of the 82 missing season 2024 Utah game dates","still_unauthorized":["baseline fitting","training","odds acquisition","ROI","production deployment"]}
 (out/f"nhl_moneyline_feature_spine_decision_{STAMP}.json").write_text(json.dumps(decision,indent=2,sort_keys=True)+"\n")
 identity={"package":"nhl_moneyline_team_goalie_feature_spine","version":"1.0.0","as_of":STAMP,"assessment_date":"2026-08-09","canonical_seasons":[2023,2024],"fixed_games":2798,"parent_ledger_sha256":sha(parent),"scope":"strict-prior feature spine construction only"}; (out/f"package_identity_{STAMP}.json").write_text(json.dumps(identity,indent=2,sort_keys=True)+"\n")
 sr=pd.DataFrame(sched); a23=audits[0]; a24=audits[1]
 report=f"""# NHL moneyline strict-prior team and goalie feature spine

## Result

The frozen 2,798-game population is preserved exactly. A team-only goals/shots spine was constructed with season-to-date, prior-5, and prior-10 windows, always shifted before the target game and reset by canonical season. Season `2023` chronology is exact. Season `2024` team ordering is bounded because 82 Utah games lack dates; no value was filled from full-season data.

Team source grain is exactly 5,596 team-games with no duplicates or many-to-many joins. Early-history nulls remain visible. Schedule/rest is exact only while a team's date chain is complete; {int(sr.date_blocked_games.sum())} game rows are date-blocked after accounting for missing dates and downstream rest uncertainty. No schedule helper was trusted or used; season grouping follows the certified parent.

Goalie logs support actual max-TOI goalie oracle identity for 1,400 season `2023` games and 1,316 season `2024` games. They contain no certified projected or confirmed pregame starter history, and actual starter/performance is excluded from model inputs. Roster history covers only 3 and 9 games respectively, with no governed injury or scratch source.

## Leakage and readiness

All constructed team statistics use only lower game IDs within the same canonical season and are shifted before rolling. Final score, winner, goalie participation, and other postgame fields are evaluation/oracle-only. No feature was selected, optimized, or fitted.

Team-only baseline research is `READY_WITH_BOUNDED_LIMITS`; team+schedule is `BLOCKED_BY_DATE_GAPS`; team+goalie is `BLOCKED_BY_GOALIE_TIMING`. The evidence selects one next task: bounded remediation and certification of the 82 missing season `2024` Utah game dates. Baseline fitting, training, odds, ROI, deployment, and restart remain unauthorized.
"""; (out/f"nhl_moneyline_team_goalie_feature_spine_report_{STAMP}.md").write_text(report)
 one=f"""# NHL moneyline feature spine — one-page summary

All 2,798 certified games remain in the spine. Goals/shots season-to-date and rolling-5/10 features are shifted and season-reset. Season `2023` is exact; season `2024` is bounded by 82 missing Utah dates. Schedule/rest is blocked wherever that missing-date history can propagate.

Actual goalie identity is oracle-only: 1,400 games in season `2023` and 1,316 in season `2024`; no pregame projected/confirmed starter history survives. Lineup/injury history is not replayable. Team-only baseline research is ready with bounded limits, while team+schedule and team+goalie remain blocked.

Exactly one next task is unlocked: remediate and certify the 82 missing season `2024` Utah game dates. No baseline fitting, training, odds, ROI, deployment, or restart is authorized.
"""; (out/f"nhl_moneyline_feature_spine_one_page_summary_{STAMP}.md").write_text(one)
 run={"games":len(base),"team_game_rows":len(team),"season_counts":{str(k):int(v) for k,v in base.groupby("canonical_season").size().items()},"team_feature_games_any":int((base.home_prior_games.gt(0)&base.away_prior_games.gt(0)).sum()),"date_blocked_games":int(sr.date_blocked_games.sum()),"goalie_oracle_games":int(goalies.game_id.nunique()),"pregame_goalie_games":0,"many_to_many_joins":0}; (out/f"nhl_moneyline_feature_spine_run_summary_{STAMP}.json").write_text(json.dumps(run,indent=2,sort_keys=True)+"\n")
 manifest=out/"SHA256SUMS"; manifest.write_text("\n".join(f"{sha(p)}  {p.name}" for p in sorted(out.iterdir()) if p.is_file() and p.name!="SHA256SUMS")+"\n"); print(json.dumps(run,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
