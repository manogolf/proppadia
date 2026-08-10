#!/usr/bin/env python3
"""Certify the season 2023/2024 NHL full-game moneyline outcome spine.

Read-only database access. No odds, models, ROI, schema, or production mutations.
"""
from __future__ import annotations

import argparse, hashlib, json, os
from pathlib import Path
import pandas as pd
import psycopg2

STAMP = "2026-07-13"
EXPECTED = {2023: 1400, 2024: 1398}

def sha(p: Path) -> str:
    h=hashlib.sha256();
    with p.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def write(p: Path, d: pd.DataFrame) -> None:
    d.to_csv(p,index=False,lineterminator="\n")

def query(conn, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql,conn)

def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument("--out-dir",required=True); args=ap.parse_args()
    out=Path(args.out_dir); out.mkdir(parents=True,exist_ok=True)
    url=os.environ.get("SUPABASE_DB_URL","").strip()
    if not url: raise RuntimeError("SUPABASE_DB_URL is required")
    conn=psycopg2.connect(url); conn.set_session(readonly=True,autocommit=True)
    games=query(conn,"""SELECT season AS canonical_season,game_id,short_game_id,game_date,
      start_time_utc AS scheduled_start_time_utc,home_team_id,home_team_code AS home_team,
      away_team_id,away_team_code AS away_team,game_type,status AS game_status
      FROM nhl.games WHERE season IN (2023,2024) ORDER BY season,game_id""")
    summaries=query(conn,"""SELECT 2023 AS canonical_season,game_id,teamcode,ishometeam,
      num_event_goal_for,num_shotwasongoal_for FROM nhl.team_game_2023_summary
      UNION ALL SELECT 2024,game_id,teamcode,ishometeam,num_event_goal_for,
      num_shotwasongoal_for FROM nhl.team_game_2024_summary""")
    shots=query(conn,"""WITH u AS (
      SELECT season::int canonical_season,game_id::int short_game_id,period::int period,
        time::int event_time,hometeamcode,awayteamcode,hometeamgoals::int home_before,
        awayteamgoals::int away_before,goal::int goal,ishometeam::numeric is_home,
        hometeamwon::int home_won FROM nhl.shots_stage_2023
      UNION ALL
      SELECT season::int,game_id::int,period::int,time::int,hometeamcode,awayteamcode,
        hometeamgoals::int,awayteamgoals::int,goal::int,ishometeam::numeric,
        hometeamwon::int FROM nhl.shots_stage_2024),
    a AS (SELECT canonical_season,short_game_id,
      MAX(home_before + CASE WHEN goal=1 AND is_home=1 THEN 1 ELSE 0 END) final_home_before_so,
      MAX(away_before + CASE WHEN goal=1 AND is_home=0 THEN 1 ELSE 0 END) final_away_before_so,
      MAX(home_before + CASE WHEN goal=1 AND is_home=1 THEN 1 ELSE 0 END) FILTER (WHERE period<=3) regulation_home_goals,
      MAX(away_before + CASE WHEN goal=1 AND is_home=0 THEN 1 ELSE 0 END) FILTER (WHERE period<=3) regulation_away_goals,
      MAX(period) max_period,MIN(home_won) min_home_won,MAX(home_won) max_home_won,
      COUNT(*) source_event_rows,COUNT(*) FILTER (WHERE goal=1) source_goal_rows,
      COUNT(DISTINCT hometeamcode) home_code_variants,COUNT(DISTINCT awayteamcode) away_code_variants,
      MIN(hometeamcode) source_home_team,MIN(awayteamcode) source_away_team
      FROM u GROUP BY canonical_season,short_game_id)
    SELECT * FROM a ORDER BY canonical_season,short_game_id""")
    conn.close()

    if games.groupby("canonical_season").size().to_dict()!=EXPECTED or len(games)!=2798: raise RuntimeError("game count mismatch")
    if games.game_id.duplicated().any(): raise RuntimeError("duplicate game_id")
    if games[["game_id","home_team_id","away_team_id","home_team","away_team"]].isna().any().any(): raise RuntimeError("missing core identity")
    if int(games.game_date.isna().sum()) != 82: raise RuntimeError("unexpected missing-date count")
    if (games.home_team_id==games.away_team_id).any() or (games.home_team==games.away_team).any(): raise RuntimeError("home/away collision")
    card=summaries.groupby(["canonical_season","game_id"]).agg(summary_rows=("teamcode","size"),summary_teams=("teamcode","nunique"),summary_home_rows=("ishometeam","sum"),summary_goal_min=("num_event_goal_for","min"),summary_goal_max=("num_event_goal_for","max")).reset_index()
    if len(card)!=2798 or not ((card.summary_rows==2)&(card.summary_teams==2)&(card.summary_home_rows==1)).all(): raise RuntimeError("team summary grain defect")
    if not ((card.summary_goal_min==0)&(card.summary_goal_max==0)).all(): raise RuntimeError("team summary goal semantic changed; re-audit")
    x=games.merge(shots,on=["canonical_season","short_game_id"],how="left",validate="one_to_one").merge(card,on=["canonical_season","game_id"],how="left",validate="one_to_one")
    if len(x)!=2798 or x.final_home_before_so.isna().any(): raise RuntimeError("score-source join loss")
    if not ((x.home_team==x.source_home_team)&(x.away_team==x.source_away_team)).all(): raise RuntimeError("shot-source team conflict")
    if not (x.min_home_won==x.max_home_won).all(): raise RuntimeError("winner flag unstable within game")
    x["regulation_tied"]=(x.regulation_home_goals==x.regulation_away_goals)
    x["went_to_overtime"]=x.max_period.gt(3)
    x["went_to_shootout"]=x.went_to_overtime & x.final_home_before_so.eq(x.final_away_before_so)
    x["decision_type"]="REGULATION"; x.loc[x.went_to_overtime,"decision_type"]="OVERTIME"; x.loc[x.went_to_shootout,"decision_type"]="SHOOTOUT"
    x["final_home_goals"]=x.final_home_before_so.astype(int); x["final_away_goals"]=x.final_away_before_so.astype(int)
    x.loc[x.went_to_shootout & x.max_home_won.eq(1),"final_home_goals"]+=1
    x.loc[x.went_to_shootout & x.max_home_won.eq(0),"final_away_goals"]+=1
    x["goal_differential"]=x.final_home_goals-x.final_away_goals; x["total_goals"]=x.final_home_goals+x.final_away_goals
    if x.goal_differential.eq(0).any(): raise RuntimeError("final tie")
    x["score_winner_home"]=x.goal_differential.gt(0).astype(int)
    x["winner_flag_agreement"]=x.score_winner_home.eq(x.max_home_won)
    conflict=x.loc[~x.winner_flag_agreement,["canonical_season","game_id"]]
    if conflict.to_dict("records") != [{"canonical_season":2024,"game_id":2024020002}]: raise RuntimeError(f"unexpected winner conflicts: {conflict}")
    x["canonical_outcome"]=x.score_winner_home.map({1:"HOME_WIN",0:"AWAY_WIN"})
    x["home_win_target"]=x.score_winner_home; x["away_win_target"]=1-x.score_winner_home
    x["identity_status"]="QUALIFIED"; x.loc[x.game_date.isna(),"identity_status"]="QUALIFIED_WITH_MISSING_GAME_DATE"
    x["source_status"]="FINAL_SCORE_RECONSTRUCTED_FROM_RAW_SHOT_STATE"
    x["final_status_status"]="QUALIFIED_BY_COMPLETE_DECISIVE_FINAL_SCORE"
    x["score_status"]="QUALIFIED"; x["winner_status"]="QUALIFIED"
    x["qualification_status"]="IN_FIXED_OUTCOME_POPULATION"
    x["exclusion_reason"]=""
    x["venue"]="UNKNOWN_NOT_IN_NHL_GAMES"
    x["notes"]="team summary supplies grain/identity support; its goal field is zero and is not score authority"
    x.loc[x.game_date.isna(),"notes"] += "; nhl.games game_date missing for Utah season 2024 row; retained by canonical game_id"
    x=x.sort_values(["canonical_season","game_id"]).reset_index(drop=True)

    spine_cols=["canonical_season","game_id","game_date","scheduled_start_time_utc","home_team_id","home_team","away_team_id","away_team","game_type","venue","game_status","source_status","identity_status","notes"]
    write(out/f"nhl_full_game_moneyline_game_spine_{STAMP}.csv",x[spine_cols])
    score=x[["canonical_season","game_id","summary_rows","summary_teams","summary_home_rows","summary_goal_min","summary_goal_max","source_event_rows","source_goal_rows","source_home_team","source_away_team","final_home_before_so","final_away_before_so","final_home_goals","final_away_goals","score_status"]].copy()
    score["authority_decision"]="RAW_SHOT_STAGE_SCORE_STATE_PLUS_GOAL_EVENTS; SHOOTOUT_WINNER_FLAG_ADDS_ONE"
    score["summary_semantic_defect"]="num_event_goal_for=0 on both team rows; not used as score"
    write(out/f"nhl_full_game_moneyline_score_authority_audit_{STAMP}.csv",score)
    extra=x[["canonical_season","game_id","regulation_home_goals","regulation_away_goals","regulation_tied","went_to_overtime","went_to_shootout","max_period","decision_type","final_home_goals","final_away_goals"]]
    write(out/f"nhl_full_game_moneyline_extra_time_audit_{STAMP}.csv",extra)
    ledger_cols=spine_cols[:10]+["final_status_status","score_status","winner_status","regulation_home_goals","regulation_away_goals","decision_type","final_home_goals","final_away_goals","goal_differential","total_goals","canonical_outcome","home_win_target","away_win_target","qualification_status","exclusion_reason"]
    write(out/f"nhl_full_game_moneyline_outcome_qualification_ledger_{STAMP}.csv",x[ledger_cols])
    waterfall=[]
    for season in [2023,2024,"ALL"]:
        n=EXPECTED.get(season,2798)
        for gate,label in [(0,"RAW_GAMES"),(1,"IDENTITY_QUALIFIED"),(2,"FINAL_STATUS_QUALIFIED"),(3,"SCORE_QUALIFIED"),(4,"WINNER_QUALIFIED"),(5,"FIXED_MONEYLINE_OUTCOME_POPULATION")]:
            waterfall.append({"canonical_season":season,"gate":gate,"gate_name":label,"input_rows":n,"qualified_rows":n,"excluded_rows":0,"exclusion_reason":"NONE"})
    write(out/f"nhl_full_game_moneyline_population_waterfall_{STAMP}.csv",pd.DataFrame(waterfall))
    cons=x[["canonical_season","game_id","winner_flag_agreement","final_home_goals","final_away_goals","max_home_won","score_winner_home"]].copy()
    cons["consistency_class"]="EXACT_AGREEMENT"; cons.loc[~cons.winner_flag_agreement,"consistency_class"]="SOURCE_CONFLICT_RESOLVED_BY_DECISIVE_SCORE"
    cons["resolution"]="NONE"; cons.loc[~cons.winner_flag_agreement,"resolution"]="raw event score state and four goal events establish NJD 3 BUF 1; homeTeamWon flag is erroneous"
    write(out/f"nhl_full_game_moneyline_outcome_consistency_audit_{STAMP}.csv",cons)
    fields=[
      ("canonical_season","IDENTITY_METADATA","nhl.games.season authoritative"),("game_id","IDENTITY_METADATA","canonical identity"),("game_date","IDENTITY_METADATA","schedule identity"),("scheduled_start_time_utc","TIMING_UNVERIFIED","missing for all but three season 2023/2024 rows"),("home_team","IDENTITY_METADATA","canonical home side"),("away_team","IDENTITY_METADATA","canonical away side"),("game_type","IDENTITY_METADATA","competition type"),("final_home_goals","OUTCOME_ONLY","never model input"),("final_away_goals","OUTCOME_ONLY","never model input"),("goal_differential","OUTCOME_ONLY","derived final outcome"),("total_goals","OUTCOME_ONLY","derived final outcome"),("canonical_outcome","OUTCOME_ONLY","settlement target"),("decision_type","OUTCOME_ONLY","postgame result type"),("team rolling strength","POTENTIAL_STRICT_PRIOR_FEATURE","not constructed here"),("goalie state","TIMING_UNVERIFIED","requires pregame certification"),("rest and schedule","POTENTIAL_STRICT_PRIOR_FEATURE","future construction only"),("lineup context","TIMING_UNVERIFIED","requires snapshot timing"),("team_summary.num_event_goal_for","NOT_FOR_MODEL_INPUT","semantic defect: zero on both team rows")]
    write(out/f"nhl_full_game_moneyline_field_classification_{STAMP}.csv",pd.DataFrame(fields,columns=["field_or_concept","classification","notes"]))
    sums=[]
    for season,g in x.groupby("canonical_season"):
        home=int(g.home_win_target.sum()); away=len(g)-home
        sums.append({"canonical_season":season,"raw_games":len(g),"completed_games":len(g),"score_qualified_games":len(g),"winner_qualified_games":len(g),"home_wins":home,"away_wins":away,"overtime_games":int(g.decision_type.eq("OVERTIME").sum()),"shootout_games":int(g.decision_type.eq("SHOOTOUT").sum()),"unresolved_extra_time_type":0,"cancelled_postponed_exclusions":0,"score_conflicts":int((~g.winner_flag_agreement).sum()),"identity_conflicts":0,"fixed_population_count":len(g),"home_win_rate":home/len(g),"away_win_rate":away/len(g)})
    summary=pd.DataFrame(sums); write(out/f"nhl_full_game_moneyline_season_summary_{STAMP}.csv",summary)
    ledger=out/f"nhl_full_game_moneyline_outcome_qualification_ledger_{STAMP}.csv"
    contract={"name":"NHL_FULL_GAME_MONEYLINE_OUTCOME_SPINE_SEASONS_2023_2024","canonical_seasons":[2023,2024],"row_count":2798,"natural_grain":"one row per canonical NHL game","identity":["canonical_season","game_id"],"game_authority":"nhl.games","score_authority":"raw shots_stage score-before-event plus goal event; add one to shootout winner","supporting_authority":"team summary two-team cardinality only; num_event_goal_for rejected","winner":"HOME_WIN iff final_home_goals > final_away_goals, else AWAY_WIN","exclusions":"nonfinal, cancelled, tied final, missing/malformed/conflicting unresolved score","duplicate_policy":"game_id duplicates fail certification","date_policy":"game_date is metadata, not identity; 82 Utah season 2024 rows retain null date visibly","final_status_policy":"complete decisive score source qualifies historical final where nhl.games.status is blank","score_conflict_policy":"remain visible; only 2024020002 resolved by decisive 3-1 event reconstruction over erroneous homeTeamWon flag","ordering":["canonical_season","game_id"],"ledger_sha256":sha(ledger)}
    (out/f"nhl_full_game_moneyline_population_contract_{STAMP}.json").write_text(json.dumps(contract,indent=2,sort_keys=True)+"\n")
    decision={"NHL_MONEYLINE_GAME_IDENTITY_CERTIFIED":"READY","NHL_MONEYLINE_FINAL_STATUS_CERTIFIED":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_SCORE_AUTHORITY_CERTIFIED":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_FINAL_WINNER_CERTIFIED":"READY","NHL_MONEYLINE_EXTRA_TIME_CLASSIFICATION":"READY","NHL_MONEYLINE_SEASON_2023_POPULATION_CERTIFIED":"READY","NHL_MONEYLINE_SEASON_2024_POPULATION_CERTIFIED":"READY","NHL_MONEYLINE_FIXED_OUTCOME_SPINE_CERTIFIED":"READY","NHL_MONEYLINE_STRICT_PRIOR_FEATURE_RESEARCH_READINESS":"READY_FOR_BOUNDED_SPINE_CONSTRUCTION","NHL_MONEYLINE_HISTORICAL_PRICE_RESEARCH_READINESS":"BLOCKED_BY_NO_MAINLINE_PRICES","NHL_MONEYLINE_BASELINE_RESEARCH_READINESS":"BLOCKED_BY_NO_BASELINE","NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS":"NOT_READY","unlocked":"exactly one bounded strict-prior team/goalie feature-spine construction task on the certified 2798-game population","still_unauthorized":["odds acquisition","training","model fitting","ROI","production changes","restart"]}
    (out/f"nhl_full_game_moneyline_population_decision_{STAMP}.json").write_text(json.dumps(decision,indent=2,sort_keys=True)+"\n")
    identity={"package":"nhl_full_game_moneyline_population_certification","version":"1.0.0","as_of":STAMP,"assessment_date":"2026-08-09","canonical_seasons":[2023,2024],"games":2798,"scope":"outcome/population only"}
    (out/f"package_identity_{STAMP}.json").write_text(json.dumps(identity,indent=2,sort_keys=True)+"\n")
    s23=summary.iloc[0]; s24=summary.iloc[1]
    report=f"""# NHL full-game moneyline population and outcome certification

## Result

The fixed outcome spine is certified at 2,798 games: 1,400 for canonical season `2023` and 1,398 for canonical season `2024`. Every game has unique identity, aligned home/away teams, a decisive reconstructed final score, and one neutral HOME_WIN or AWAY_WIN target. There are no exclusions. Eighty-two Utah rows in season `2024` have null `nhl.games.game_date`; the gap is retained visibly and does not alter canonical `season + game_id` identity.

## Authority correction and hierarchy

`nhl.games` is authoritative for season, game ID, date, and home/away identity. The season team-summary tables are authoritative only for two-team grain: exactly two distinct, correctly aligned team rows exist for every game. Their `num_event_goal_for` is zero on both rows for every game and therefore is **not** score authority, correcting the feasibility package's preliminary interpretation.

Final scores are reconstructed from the raw season shot-stage fields: score before each event plus the event's goal indicator. For shootouts, the score remains tied in shot events, so one goal is added to the winner identified by the stable game-level `homeTeamWon` flag. Score state overrides that flag for one visible conflict, game `2024020002`, where four goal events establish NJD 3–1 BUF while the flag incorrectly says away win.

## Outcomes

Season `2023`: {int(s23.home_wins)} home wins, {int(s23.away_wins)} away wins, {int(s23.overtime_games)} overtime decisions, and {int(s23.shootout_games)} shootout decisions. Season `2024`: {int(s24.home_wins)} home wins, {int(s24.away_wins)} away wins, {int(s24.overtime_games)} overtime decisions, and {int(s24.shootout_games)} shootout decisions. Rates are descriptive only.

Raw periods distinguish regulation from extra time; tied extra-time score plus winner flag identifies shootouts. All remaining extra-time decisions are overtime. No final tie remains after neutral settlement.

## Contract and boundary

Natural grain is one row per game; identity is `canonical_season + game_id`; ordering is season then game ID. Final goals, winner, differential, total, and decision type are outcome-only. Team strength, rest, goalie, and lineup concepts are not constructed here and require strict-prior timing certification.

This certification unlocks exactly one bounded strict-prior team/goalie feature-spine construction task on these 2,798 games. Prices, odds acquisition, baseline creation, training, fitting, ROI, production changes, and restart remain blocked or unauthorized.
"""
    (out/f"nhl_full_game_moneyline_population_certification_report_{STAMP}.md").write_text(report)
    one=f"""# NHL full-game moneyline population — one-page summary

Certified 2,798 outcome-only game rows: 1,400 in season `2023` and 1,398 in season `2024`, with no exclusions. Game identity comes from `nhl.games`; scores come from raw shot-stage score state plus goal events. Team summaries validate two-team grain but their all-zero goal field is rejected as score evidence. Eighty-two Utah rows have a visible missing-date metadata gap.

Every row has a decisive final score and HOME_WIN/AWAY_WIN target. Extra time is classified as regulation, overtime, or shootout. One erroneous winner flag for game `2024020002` remains visible and is resolved by the decisive NJD 3–1 BUF event score.

Exactly one next task is unlocked: bounded strict-prior team/goalie feature-spine construction on the frozen population. Historical prices, a baseline, training, ROI, production changes, and restart remain blocked.
"""
    (out/f"nhl_full_game_moneyline_population_one_page_summary_{STAMP}.md").write_text(one)
    run={"games":len(x),"season_counts":{str(k):int(v) for k,v in x.groupby("canonical_season").size().items()},"home_wins":int(x.home_win_target.sum()),"away_wins":int(x.away_win_target.sum()),"overtime":int(x.decision_type.eq("OVERTIME").sum()),"shootouts":int(x.decision_type.eq("SHOOTOUT").sum()),"winner_flag_conflicts":int((~x.winner_flag_agreement).sum()),"missing_game_dates":int(x.game_date.isna().sum()),"exclusions":0}
    (out/f"nhl_full_game_moneyline_population_run_summary_{STAMP}.json").write_text(json.dumps(run,indent=2,sort_keys=True)+"\n")
    manifest=out/"SHA256SUMS"; manifest.write_text("\n".join(f"{sha(p)}  {p.name}" for p in sorted(out.iterdir()) if p.is_file() and p.name!="SHA256SUMS")+"\n")
    print(json.dumps(run,sort_keys=True)); return 0

if __name__=="__main__": raise SystemExit(main())
