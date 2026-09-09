#!/usr/bin/env python3
"""Assess market-listed goalies against postgame actual starters, read-only."""
from __future__ import annotations

import argparse
import json
import math
import os
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg

from backend.nhl.analysis_package_guard import begin_package, finalize_package, sha256_file, verify_manifest

TASK="NHL_SEASON_2025_SAVES_MARKET_LISTED_GOALIE_ACTUAL_STARTER_CONCORDANCE_V1"
DATE="2026-09-08"
PARENT_MANIFEST_SHA256="f5d3affdc3a79585fa82593237c923154460033f894bdaf5442bd2b189d7881d"


def read_sql(connection:psycopg.Connection,query:str)->pd.DataFrame:
    with connection.cursor() as cur:
        cur.execute(query); cols=[x.name for x in cur.description]
        return pd.DataFrame(cur.fetchall(),columns=cols)


def actual_snapshot(dsn:str)->pd.DataFrame:
    with psycopg.connect(dsn) as conn:
        frame=read_sql(conn,"""
            SELECT l.game_id,l.team_id,t.team AS team_code,l.player_id,p.full_name AS player_name,
                   l.start_flag,l.toi_minutes,l.saves,l.shots_faced,l.goals_allowed,l.game_date,
                   l.created_at AS outcome_recorded_at_utc
            FROM nhl.goalie_game_logs_raw l
            JOIN nhl.games g ON g.game_id=l.game_id
            JOIN nhl.teams t ON t.team_id=l.team_id
            JOIN nhl.players p ON p.player_id=l.player_id
            WHERE g.season=2025
            ORDER BY l.game_id,l.team_id,l.player_id
        """)
    frame["outcome_recorded_at_utc"]=pd.to_datetime(frame.outcome_recorded_at_utc,utc=True,errors="coerce")
    return frame


def write_parquet(frame:pd.DataFrame,path:Path)->None:
    out=frame.copy()
    for col in out.columns:
        if col.endswith("_utc"):
            out[col]=pd.to_datetime(out[col],utc=True,errors="coerce")
    for col in ["game_id","team_id","player_id","canonical_game_id","canonical_player_id","actual_starter_player_id","listed_goalie_count","book_count","line_count"]:
        if col in out: out[col]=pd.to_numeric(out[col],errors="coerce").astype("Int64")
    out.to_parquet(path,index=False,compression="zstd")


def lead_bucket(minutes:float)->str:
    if minutes <= 30:return "00_TO_30_MIN"
    if minutes <= 60:return "31_TO_60_MIN"
    if minutes <= 120:return "61_TO_120_MIN"
    if minutes <= 180:return "121_TO_180_MIN"
    if minutes <= 360:return "181_TO_360_MIN"
    return "OVER_360_MIN"


def rate_table(frame:pd.DataFrame,dimensions:list[str],name:str)->pd.DataFrame:
    if frame.empty:
        return pd.DataFrame([{"metric_scope":name,**{d:"NO_ROWS" for d in dimensions},"listing_rows":0,"starter_matches":0,"nonstarter_listings":0,"concordance_rate":None,"mismatch_rate":None}])
    rows=[]
    grouped=frame.groupby(dimensions,dropna=False,sort=True) if dimensions else [((),frame)]
    for key,g in grouped:
        if not isinstance(key,tuple):key=(key,)
        evaluable=g[g.actual_starter_player_id.notna()]
        matches=int(evaluable.is_actual_starter.sum()) if len(evaluable) else 0
        rows.append({"metric_scope":name,**dict(zip(dimensions,key)),"listing_rows":len(g),"evaluable_listing_rows":len(evaluable),
                     "starter_matches":matches,"nonstarter_listings":len(evaluable)-matches,
                     "concordance_rate":matches/len(evaluable) if len(evaluable) else None,
                     "mismatch_rate":1-matches/len(evaluable) if len(evaluable) else None,
                     "team_games":g[["canonical_game_id","team"]].drop_duplicates().shape[0],
                     "evaluable_team_games":evaluable[["canonical_game_id","team"]].drop_duplicates().shape[0]})
    return pd.DataFrame(rows)


def latest_snapshot_states(listings:pd.DataFrame)->pd.DataFrame:
    rows=[]
    for (game,team),group in listings.groupby(["canonical_game_id","team"],sort=True):
        snapshots=group.groupby("source_file_sha256").agg(snapshot_time=("listing_observation_time_utc","max")).reset_index()
        chosen=snapshots.sort_values(["snapshot_time","source_file_sha256"]).iloc[-1]
        state=group[group.source_file_sha256.eq(chosen.source_file_sha256)]
        goalies=sorted(int(x) for x in state.canonical_player_id.unique())
        actuals=state.actual_starter_player_id.dropna().unique()
        actual=int(actuals[0]) if len(actuals)==1 else None
        support=state.groupby("canonical_player_id").sportsbook.nunique().sort_values(ascending=False)
        top=int(support.iloc[0]) if len(support) else 0
        leaders=sorted(int(x) for x in support[support.eq(top)].index)
        consensus=leaders[0] if top>=2 and len(leaders)==1 else None
        rows.append({"canonical_game_id":int(game),"team":team,"latest_source_file_sha256":chosen.source_file_sha256,
                     "latest_observation_time_utc":chosen.snapshot_time,"minutes_before_start":float(state.minutes_before_start.min()),
                     "phase_equivalent":state.phase_equivalent.iloc[0],"listed_goalie_count":len(goalies),
                     "listed_goalie_ids":";".join(map(str,goalies)),"one_or_multiple":"ONE_LISTED" if len(goalies)==1 else "MULTIPLE_LISTED",
                     "actual_starter_player_id":actual,"actual_outcome_available":actual is not None,
                     "actual_starter_listed":actual in goalies if actual is not None else None,
                     "uniquely_identified_actual_starter":len(goalies)==1 and goalies[0]==actual if actual is not None else None,
                     "consensus_goalie_player_id":consensus,"consensus_book_count":top if consensus is not None else None,
                     "consensus_matches_actual":consensus==actual if consensus is not None and actual is not None else None,
                     "book_count":int(state.sportsbook.nunique()),"line_count":int(state.line_count.max()),
                     "listing_rows":len(state)})
    return pd.DataFrame(rows)


def transitions(listings:pd.DataFrame)->pd.DataFrame:
    rows=[]
    for (game,team),group in listings.groupby(["canonical_game_id","team"],sort=True):
        snaps=[]
        for source,state in group.groupby("source_file_sha256"):
            snaps.append((state.listing_observation_time_utc.max(),source,state))
        snaps.sort(key=lambda x:(x[0],x[1]))
        for index in range(1,len(snaps)):
            early_t,early_id,early=snaps[index-1]; late_t,late_id,late=snaps[index]
            egoal=set(int(x) for x in early.canonical_player_id.unique()); lgoal=set(int(x) for x in late.canonical_player_id.unique())
            equotes=set(zip(early.canonical_player_id,early.sportsbook,early.quote_states))
            lquotes=set(zip(late.canonical_player_id,late.sportsbook,late.quote_states))
            actuals=late.actual_starter_player_id.dropna().unique(); actual=int(actuals[0]) if len(actuals)==1 else None
            disappeared=sorted(egoal-lgoal); appeared=sorted(lgoal-egoal)
            if actual is None: disposition="OUTCOME_UNAVAILABLE"
            elif actual in appeared: disposition="POSSIBLE_STARTER_CHANGE_MARKET_ONLY_NOT_CERTIFIED"
            elif actual in egoal and actual in lgoal: disposition="MARKET_COVERAGE_CHANGE_NOT_STARTER_CHANGE"
            else: disposition="DISCORDANT_CAUSE_UNRESOLVED_NO_CONFIRMED_PREGAME_SOURCE"
            rows.append({"canonical_game_id":int(game),"team":team,"earlier_source_file_sha256":early_id,"later_source_file_sha256":late_id,
                         "earlier_time_utc":early_t,"later_time_utc":late_t,"earlier_goalie_ids":";".join(map(str,sorted(egoal))),
                         "later_goalie_ids":";".join(map(str,sorted(lgoal))),"goalie_set_changed":egoal!=lgoal,
                         "disappeared_goalie_ids":";".join(map(str,disappeared)),"appeared_goalie_ids":";".join(map(str,appeared)),
                         "quote_set_changed":equotes!=lquotes,"removed_quote_states":len(equotes-lquotes),"added_quote_states":len(lquotes-equotes),
                         "actual_starter_player_id":actual,"transition_disposition":disposition,
                         "identity_join_assessment":"CANONICAL_QUALIFIED_IDENTITIES_NO_JOIN_ERROR_INDICATED",
                         "true_starter_change_certified":False})
    return pd.DataFrame(rows)


def policy_table(latest:pd.DataFrame,actual_team_games:int)->pd.DataFrame:
    e=latest[latest.actual_outcome_available].copy()
    rows=[]
    # A: every listed goalie is a prediction.
    selected=[]
    for r in e.itertuples():
        for goalie in map(int,str(r.listed_goalie_ids).split(";")):
            selected.append((int(r.canonical_game_id),r.team,goalie,int(r.actual_starter_player_id)))
    covered=e[["canonical_game_id","team"]].drop_duplicates().shape[0]
    mismatches=sum(g!=a for _,_,g,a in selected)
    rows.append({"policy":"A_EVERY_ACTIVE_MARKET_LISTED_GOALIE","selection_contract":"predict every canonically qualified listed goalie; NOT starter confirmation","selected_team_games":covered,"selected_goalie_predictions":len(selected),"actual_starter_team_game_universe":actual_team_games,"historical_coverage_rate":covered/actual_team_games,"mismatches":mismatches,"mismatch_rate":mismatches/len(selected) if selected else None})
    # B: exactly one listed goalie.
    b=e[e.listed_goalie_count.eq(1)]; bm=int(b.uniquely_identified_actual_starter.eq(False).sum())
    rows.append({"policy":"B_EXACTLY_ONE_LISTED_GOALIE_PER_TEAM","selection_contract":"select only one-listed team-game state","selected_team_games":len(b),"selected_goalie_predictions":len(b),"actual_starter_team_game_universe":actual_team_games,"historical_coverage_rate":len(b)/actual_team_games,"mismatches":bm,"mismatch_rate":bm/len(b) if len(b) else None})
    # C: unique book-count leader supported by at least two books.
    c=e[e.consensus_goalie_player_id.notna()]; cm=int(c.consensus_matches_actual.eq(False).sum())
    rows.append({"policy":"C_MULTIPLE_BOOKS_UNIQUE_GOALIE_AGREEMENT","selection_contract":"unique top-supported goalie with at least two distinct books; other singly-listed goalies may exist","selected_team_games":len(c),"selected_goalie_predictions":len(c),"actual_starter_team_game_universe":actual_team_games,"historical_coverage_rate":len(c)/actual_team_games,"mismatches":cm,"mismatch_rate":cm/len(c) if len(c) else None})
    rows.append({"policy":"D_EXTERNAL_PROJECTED_CONFIRMED_SOURCE","selection_contract":"retain independent timestamp-certified projected/confirmed goalie source","selected_team_games":0,"selected_goalie_predictions":0,"actual_starter_team_game_universe":actual_team_games,"historical_coverage_rate":None,"mismatches":None,"mismatch_rate":None})
    return pd.DataFrame(rows)


def manifest(directory:Path)->None:
    files=sorted(p for p in directory.iterdir() if p.is_file() and p.name!="SHA256SUMS")
    (directory/"SHA256SUMS").write_text("".join(f"{sha256_file(p)}  {p.name}\n" for p in files))


def main()->int:
    ap=argparse.ArgumentParser();ap.add_argument("--parent-package",type=Path,required=True);ap.add_argument("--output-dir",type=Path,required=True)
    ap.add_argument("--actual-snapshot-package",type=Path);ap.add_argument("--supersedes-package")
    args=ap.parse_args(); parent=args.parent_package.resolve(); target=args.output_dir.resolve()
    parent_manifest=verify_manifest(parent,PARENT_MANIFEST_SHA256); staging=begin_package(target)
    try:
        if args.actual_snapshot_package:
            actual_parent=args.actual_snapshot_package.resolve(); actual_parent_manifest=verify_manifest(actual_parent)
            all_actual=pd.read_parquet(actual_parent/"postgame_goalie_outcome_snapshot.parquet")
            actual_source=f"IMMUTABLE_PACKAGE:{actual_parent}"
        else:
            dsn=os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
            if not dsn: raise RuntimeError("DATABASE_URL_REQUIRED_FOR_READ_ONLY_ACTUAL_SNAPSHOT")
            all_actual=actual_snapshot(dsn);actual_parent_manifest=None;actual_source="READ_ONLY_DATABASE_SNAPSHOT"
        starters=all_actual[all_actual.start_flag.eq(True)].copy()
        counts=starters.groupby(["game_id","team_code"]).size(); valid=set(counts[counts.eq(1)].index)
        starters=starters[[ (int(r.game_id),r.team_code) in valid for r in starters.itertuples() ]].copy()
        starter_map={(int(r.game_id),r.team_code):int(r.player_id) for r in starters.itertuples()}
        q=pd.read_parquet(parent/"qualified_pregame_observation_index.parquet")
        q=q[q.market_family.eq("SAVES")].copy()
        # Pregame table remains outcome-free.
        q["listing_observation_time_utc"]=pd.to_datetime(q.effective_observation_timestamp_utc,utc=True)
        q["timing_start_time_utc"]=pd.to_datetime(q.timing_start_time_utc,utc=True)
        q["minutes_before_start"]=(q.timing_start_time_utc-q.listing_observation_time_utc).dt.total_seconds()/60
        q["lead_time_bucket"]=q.minutes_before_start.map(lead_bucket)
        q["phase_equivalent"]=q.minutes_before_start.map(lambda x:"FINAL_PREGAME_EQUIVALENT" if x<=180 else "MIDDAY_EQUIVALENT")
        q["listing_status_class"]=q.market_status.map(lambda x:"SUSPENDED_OR_CLOSED" if x in {"SUSPENDED","CLOSED","INACTIVE"} else "ACTIVE_OR_LISTED_NO_EXPLICIT_STATUS")
        q["market_listing_is_starter_confirmation"]=False
        q["quote_state"]=q.apply(lambda r:f"{r.line}|{r.side}|{r.american_price}|{r.market_status}",axis=1)
        pregame_cols=[c for c in q.columns if not c.startswith("actual_")]
        write_parquet(q[pregame_cols],staging/"qualified_pregame_saves_observations.parquet")
        write_parquet(all_actual,staging/"postgame_goalie_outcome_snapshot.parquet")
        # One listing per source snapshot/game/team/goalie/book, preserving full observations separately.
        group_cols=["source_file_sha256","source_file_path","archive_family","canonical_game_id","team","canonical_player_id","canonical_player_name","sportsbook","sportsbook_name"]
        listings=q.groupby(group_cols,dropna=False,sort=True).agg(
            listing_observation_time_utc=("listing_observation_time_utc","max"),minutes_before_start=("minutes_before_start","min"),
            phase_equivalent=("phase_equivalent","last"),lead_time_bucket=("lead_time_bucket","last"),market_status=("market_status","last"),
            listing_status_class=("listing_status_class","last"),line_count=("line","nunique"),observation_rows=("observation_id","size"),
            first_observation_time_utc=("listing_observation_time_utc","min"),last_observation_time_utc=("listing_observation_time_utc","max"),
            quote_states=("quote_state",lambda s:";".join(sorted(set(s))))
        ).reset_index()
        team_counts=q.groupby(["source_file_sha256","canonical_game_id","team"]).canonical_player_id.nunique().rename("listed_goalie_count").reset_index()
        listings=listings.merge(team_counts,on=["source_file_sha256","canonical_game_id","team"],how="left")
        listings["one_or_multiple"]=listings.listed_goalie_count.map(lambda x:"ONE_LISTED" if x==1 else "MULTIPLE_LISTED")
        listings["actual_starter_player_id"]=[starter_map.get((int(g),t)) for g,t in zip(listings.canonical_game_id,listings.team)]
        listings["actual_outcome_available"]=listings.actual_starter_player_id.notna()
        listings["is_actual_starter"]=(listings.canonical_player_id==listings.actual_starter_player_id).where(listings.actual_outcome_available)
        listings["evaluation_join_method"]="CANONICAL_GAME_TEAM_PLAYER_POSTGAME_START_FLAG"
        listings["discordance_disposition"]=listings.apply(lambda r:"CONCORDANT_ACTUAL_STARTER" if r.is_actual_starter is True else ("NONSTARTER_LISTING_CAUSE_UNRESOLVED_SINGLE_PREGAME_SNAPSHOT" if r.is_actual_starter is False else "ACTUAL_STARTER_OUTCOME_UNAVAILABLE"),axis=1)
        write_parquet(listings,staging/"market_listing_actual_starter_evaluation.parquet")
        metrics=pd.concat([
            rate_table(listings,[],"OVERALL_BOOK_LISTING"),rate_table(listings,["lead_time_bucket"],"TIME_BEFORE_PUCK_DROP"),
            rate_table(listings,["phase_equivalent"],"PHASE_EQUIVALENT"),rate_table(listings,["sportsbook"],"SPORTSBOOK"),
            rate_table(listings,["team"],"TEAM"),rate_table(listings,["one_or_multiple"],"ONE_VS_MULTIPLE"),
            rate_table(listings,["listing_status_class"],"STATUS"),
        ],ignore_index=True,sort=False)
        metrics.to_csv(staging/"concordance_metrics.csv",index=False)
        latest=latest_snapshot_states(listings);write_parquet(latest,staging/"latest_team_market_state_evaluation.parquet")
        change=transitions(listings);write_parquet(change,staging/"pregame_listing_transition_register.parquet")
        actual_team_games=len(valid); policies=policy_table(latest,actual_team_games);policies.to_csv(staging/"prospective_policy_evaluation.csv",index=False)
        evaluable_latest=latest[latest.actual_outcome_available]
        total_listed=int(evaluable_latest.listed_goalie_count.sum()); nonstarter=total_listed-int(evaluable_latest.actual_starter_listed.sum())
        extra=(evaluable_latest.listed_goalie_count-1).clip(lower=0)
        nonstarter_summary={"actual_starter_team_game_universe":actual_team_games,"latest_market_team_games_with_actual":len(evaluable_latest),
            "latest_listed_goalies":total_listed,"latest_nonstarter_listings":nonstarter,"latest_nonstarter_listing_rate":nonstarter/total_listed if total_listed else None,
            "team_games_with_multiple_listed":int(evaluable_latest.listed_goalie_count.gt(1).sum()),"mean_extra_listed_goalies_per_evaluable_team_game":float(extra.mean()) if len(extra) else None,
            "median_extra_listed_goalies_per_evaluable_team_game":float(extra.median()) if len(extra) else None,"maximum_extra_listed_goalies":int(extra.max()) if len(extra) else None,
            "latest_market_uniquely_identified_actual_starter":int(evaluable_latest.uniquely_identified_actual_starter.sum()),
            "latest_market_unique_actual_rate":float(evaluable_latest.uniquely_identified_actual_starter.mean()) if len(evaluable_latest) else None,
            "status_note":"Qualified parent has zero suspended/closed rows; all retained rows have source status NOT_PROVIDED and represent listed availability, not explicit active certification.",
            "true_change_note":"No timestamp-certified projected/confirmed pregame source exists; market transitions cannot certify true starter changes."}
        (staging/"nonstarter_frequency_and_magnitude.json").write_text(json.dumps(nonstarter_summary,indent=2,sort_keys=True)+"\n")
        a=policies.set_index("policy").loc["A_EVERY_ACTIVE_MARKET_LISTED_GOALIE"]
        b=policies.set_index("policy").loc["B_EXACTLY_ONE_LISTED_GOALIE_PER_TEAM"]
        c=policies.set_index("policy").loc["C_MULTIPLE_BOOKS_UNIQUE_GOALIE_AGREEMENT"]
        # The decision is evidence-driven: a market-only gate may qualify shadow population, but it is never confirmation.
        decision="MARKET_LISTING_SUFFICIENT_WITH_ADDITIONAL_GATE" if len(evaluable_latest)>=100 and float(c.mismatch_rate)<=float(a.mismatch_rate) else "MARKET_LISTING_TOO_AMBIGUOUS"
        summary={"task":TASK,"parent_manifest_sha256":parent_manifest,"qualified_saves_observations":len(q),"market_games":int(q.canonical_game_id.nunique()),
            "market_game_team_states":int(q[["canonical_game_id","team"]].drop_duplicates().shape[0]),"actual_goalie_rows":len(all_actual),
            "authoritative_actual_starter_rows":len(starters),"authoritative_actual_starter_team_games":actual_team_games,
            "latest_evaluable_market_team_games":len(evaluable_latest),"latest_unique_actual_count":nonstarter_summary["latest_market_uniquely_identified_actual_starter"],
            "latest_unique_actual_rate":nonstarter_summary["latest_market_unique_actual_rate"],"transition_comparisons":len(change),
            "transition_goalie_set_changes":int(change.goalie_set_changed.sum()) if len(change) else 0,"final_decision":decision,
            "candidate_upload_execution_authorization":"NOT_AUTHORIZED","market_listing_label":"PREGAME_AVAILABILITY_SIGNAL_NOT_STARTER_CONFIRMATION"}
        (staging/"assessment_summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
        schema={"pregame_outcome_separation":"qualified_pregame_saves_observations contains no actual starter fields; postgame outcome snapshot and evaluation tables are separate","listing_grain":"source snapshot x canonical game x team x canonical goalie x sportsbook","latest_state":"derived by maximum qualified observation time per canonical game/team; source rows remain immutable","phase_equivalence":{"FINAL_PREGAME_EQUIVALENT":"0 to 180 minutes before provider event commence","MIDDAY_EQUIVALENT":"more than 180 minutes before provider event commence"},"policy_C":"unique goalie with strictly top distinct-book support and at least two books","status_limit":"all qualified source rows have NOT_PROVIDED; suspended/closed rows are absent by parent qualification","no_claims":["starter confirmation","candidate","upload","execution","true starter change"]}
        (staging/"schema_and_evaluation_contract.json").write_text(json.dumps(schema,indent=2,sort_keys=True)+"\n")
        report=f"""# NHL season-2025 Saves market-listed goalie / actual-starter concordance

## Decision

`{decision}`

The analysis uses {len(q):,} qualified pregame Saves outcome observations across {q.canonical_game_id.nunique():,} games. Actual starter is postgame-only and available for {actual_team_games:,} team-games ({len(starters):,} starter rows); it never enters the pregame table. The latest qualified market state overlaps {len(evaluable_latest):,} evaluable team-games and uniquely lists the actual starter in {nonstarter_summary['latest_market_uniquely_identified_actual_starter']:,} ({nonstarter_summary['latest_market_unique_actual_rate']:.1%}).

Market listing is an availability signal, not starter confirmation. All qualified rows have source status `NOT_PROVIDED`; suspended/closed comparison is not estimable from the qualified population. Most dates contain one surviving snapshot, so discordant listings cannot be labeled true starter changes. Canonical identity gates exclude known join errors; remaining discordance is retained as unresolved without a confirmed pregame source.

## Policy comparison

"""+"\n".join(f"- {r.policy}: coverage={r.historical_coverage_rate if pd.notna(r.historical_coverage_rate) else 'NOT_MEASURABLE'}, mismatch={r.mismatch_rate if pd.notna(r.mismatch_rate) else 'NOT_MEASURABLE'}" for r in policies.itertuples())+"""

Policy A is too permissive because every additional listed goalie becomes a nonstarter prediction. Policy B is the simplest fail-closed market-only shadow gate. Policy C tests independent book agreement and preserves broader states only when one goalie has unique multi-book support. Policy D remains the requirement for any future projected/confirmed starter state; it has no historical repository coverage to score here.

No candidate, upload, execution, production authorization, or starter-confirmation designation is created by this assessment.
"""
        (staging/"main_assessment_report.md").write_text(report);(staging/"executive_summary.md").write_text(report)
        identity={"task":TASK,"package_date":DATE,"parent_package":str(parent),"parent_manifest_sha256":parent_manifest,
                  "actual_snapshot_source":actual_source,"actual_snapshot_parent_manifest_sha256":actual_parent_manifest,
                  "actual_starter_contract":"postgame nhl.goalie_game_logs_raw start_flag=true only","source_code":"backend/nhl/scripts/assess_nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance.py",
                  "source_code_sha256":sha256_file(Path(__file__)),"source_mutations":0,"live_authorization_changes":0,"final_decision":decision,
                  "supersedes_package":args.supersedes_package,"supersession_reason":"Corrects Policy B/C mismatch counting for nullable/object booleans; pregame and outcome populations are unchanged." if args.supersedes_package else None}
        (staging/"package_identity.json").write_text(json.dumps(identity,indent=2,sort_keys=True)+"\n")
        # Fail closed on structural boundaries before publication.
        assert q.timing_classification.eq("PREGAME_QUALIFIED").all()
        assert q.market_family.eq("SAVES").all()
        assert "actual_starter_player_id" not in q[pregame_cols]
        assert not starters.duplicated(["game_id","team_code"]).any()
        manifest(staging);verify_manifest(staging);finalize_package(staging,target)
    except BaseException:
        if staging.exists():shutil.rmtree(staging)
        raise
    print(target);return 0


if __name__=="__main__":raise SystemExit(main())
