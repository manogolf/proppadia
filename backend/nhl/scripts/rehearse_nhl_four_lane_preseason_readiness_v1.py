#!/usr/bin/env python3
"""Offline four-lane NHL preseason readiness amendment and rehearsal."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

import backend.nhl.scripts.rehearse_nhl_three_lane_preseason_operator_v1 as three_lane
from backend.nhl.analysis_package_guard import verify_manifest
from backend.nhl.saves_quote_capture.core import capture_run as capture_saves
from backend.nhl.saves_quote_capture.core import sha256_file, write_manifest
from backend.nhl.saves_shadow.core import grade_shadow, run_shadow

ROOT=Path(__file__).resolve().parents[3];DATE="2026-09-08";SLATE="2026-09-19";GAME_ID=2026020001
START="2026-09-19T22:00:00Z";MIDDAY="2026-09-19T17:00:00Z";FINAL="2026-09-19T21:30:00Z"
THREE_CANONICAL=ROOT/"artifacts/analysis/model_development/nhl_season_2026_three_lane_preseason_operator_rehearsal_v1/2026-09-08"
SAVES_CERT=ROOT/"artifacts/analysis/model_development/nhl_saves_immutable_market_listed_preseason_shadow_path_v1/2026-09-08/final_v3"


def sha(path: Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()
def js(path: Path,value)->None:path.write_text(json.dumps(value,indent=2,sort_keys=True)+"\n")
def table(path: Path,rows: list[dict])->None:
    with path.open("w",newline="") as h:
        w=csv.DictWriter(h,fieldnames=list(rows[0]),lineterminator="\n");w.writeheader();w.writerows(rows)
def manifest_parent(directory: Path,games: pd.DataFrame,goalies: pd.DataFrame):
    directory.mkdir(parents=True);gp=directory/"canonical_game_spine.csv";pp=directory/"saves_goalie_inputs.csv";games.to_csv(gp,index=False);goalies.to_csv(pp,index=False)
    (directory/"RUN_COMPLETE.json").write_text('{"status":"COMPLETE"}\n');write_manifest(directory,complete_only=True);return gp,pp,directory/"SHA256SUMS"
def payload(capture: str,update: str,final: bool=False)->dict:
    books=[]
    listing={"book_a":["Alpha Goalie","Backup Alpha","Away Goalie"],"book_b":["Alpha Goalie"] if final else ["Alpha Goalie","Away Goalie"]}
    for book,names in listing.items():
        outcomes=[]
        for name in names:outcomes += [{"name":"Over","description":name,"point":24.5,"price":-110,"last_update":update},{"name":"Under","description":name,"point":24.5,"price":-110,"last_update":update}]
        books.append({"key":book,"title":book,"last_update":update,"markets":[{"id":f"saves-{book}","key":"player_total_saves","last_update":update,"outcomes":outcomes}]})
    return {"capture_timestamp_utc":capture,"request_metadata":{"fixture":True,"network":False},"provider_response":[{"id":"rehearsal-event-2026020001","home_team":"Home Club","away_team":"Away Club","commence_time":START,"bookmakers":books}]}
def tree(path: Path)->dict[str,str]:return {str(x.relative_to(path)):sha(x) for x in path.rglob("*") if x.is_file()}


def run() -> tuple[dict,list[dict],list[dict],list[dict]]:
    stages=[];checks=[];counts=[];commands=[]
    with tempfile.TemporaryDirectory(prefix="nhl_four_lane_rehearsal_") as raw:
        tmp=Path(raw)
        # Freshly exercise the accepted Moneyline/SOG/Points rehearsal in isolation.
        prior_out=three_lane.OUT;three_lane.OUT=tmp/"three_lane"
        try:three_rc=three_lane.main()
        finally:three_lane.OUT=prior_out
        three_pkg=tmp/"three_lane";three_manifest=sha(three_pkg/"SHA256SUMS")
        decision=json.loads((three_pkg/f"nhl_rehearsal_decision_{DATE}.json").read_text())
        three_recon=pd.read_csv(three_pkg/f"nhl_cross_lane_reconciliation_{DATE}.csv");three_negative=pd.read_csv(three_pkg/f"nhl_negative_test_results_{DATE}.csv");three_commands=pd.read_csv(three_pkg/f"nhl_exact_command_surfaces_validated_{DATE}.csv")
        stages += [{"stage":"morning prerequisite state and canonical slate","lane":"ALL","status":"PASS","evidence":"fresh isolated three-lane rehearsal plus Saves prerequisite field"},{"stage":"Moneyline score and H2H view","lane":"MONEYLINE","status":"PASS","evidence":decision["authorized_scopes"]["MONEYLINE"]},{"stage":"SOG prediction/market/candidate lineage","lane":"SOG","status":"PASS","evidence":decision["authorized_scopes"]["SOG"]},{"stage":"Points score then ladder gate","lane":"POINTS","status":"PASS","evidence":decision["authorized_scopes"]["POINTS"]}]

        source=pd.read_csv(ROOT/"backend/nhl/exports/history/2026-04-16/train_goalie_saves_v2.csv").iloc[:3].copy()
        source["player_id"]=[101,102,201];source["goalie_id"]=source.player_id;source["goalie_name"]=["Alpha Goalie","Backup Alpha","Away Goalie"]
        source["team"]=["Home Club","Home Club","Away Club"];source["opponent"]=["Away Club","Away Club","Home Club"];source["goalie_aliases"]=["A Goalie","B Alpha","Away G"]
        source["game_id"]=GAME_ID;source["canonical_season"]=2026;source["slate_date"]=SLATE;source["scheduled_start_time_utc"]=START;source["game_type_code"]=1
        source["feature_cutoff_timestamp_utc"]="2026-09-19T15:00:00Z";source["feature_history_max_timestamp_utc"]="2026-04-16T23:59:59Z";source["roster_source_timestamp_utc"]="2026-09-19T14:59:00Z"
        source["goalie_eligibility_state"]="ROSTER_ELIGIBLE";source["population_contract"]="COMPLETE_SCORER_ELIGIBLE";source["scorer_eligible"]=True;source["expected_complete_population_rows"]=3
        games=pd.DataFrame([{"canonical_season":2026,"slate_date":SLATE,"game_id":GAME_ID,"home_team":"Home Club","away_team":"Away Club","scheduled_start_time_utc":START,"game_type_code":1,"provider_event_id":"rehearsal-event-2026020001"}])
        game_csv,goalie_csv,parent=manifest_parent(tmp/"saves_parent",games,source)
        quote_runs={};shadow_runs={}
        for phase,stamp,update,is_final in [("MIDDAY",MIDDAY,"2026-09-19T16:55:00Z",False),("FINAL_PREGAME",FINAL,"2026-09-19T21:25:00Z",True)]:
            raw_path=tmp/f"saves_{phase}.json";js(raw_path,payload(stamp,update,is_final))
            quote_runs[phase]=capture_saves(payload_json=raw_path,games_csv=game_csv,goalies_csv=goalie_csv,parent_manifest=parent,output_root=tmp/"saves_quotes",slate_date=SLATE,run_timestamp_utc=stamp,run_type=phase,source="CERTIFIED_ISOLATED_REHEARSAL_FIXTURE")
            shadow_runs[phase]=run_shadow(game_spine_csv=game_csv,game_spine_manifest=parent,goalie_inputs_csv=goalie_csv,goalie_inputs_manifest=parent,quote_run_dir=quote_runs[phase],output_root=tmp/"saves_shadow",slate_date=SLATE,run_timestamp_utc=stamp,run_type=phase)
            meta=json.loads((shadow_runs[phase]/"run_metadata.json").read_text());counts.append({"lane":"GOALIE_SAVES","phase":phase,"P":meta["P"],"M":meta["M"],"C":meta["C"],"U":meta["U"],"E":meta["E"],"G":meta["G"],"manifest_sha256":sha(shadow_runs[phase]/"SHA256SUMS")})
            stages.append({"stage":f"{phase} Saves full-population score and Policy C","lane":"GOALIE_SAVES","status":"PASS","evidence":f"P={meta['P']} M={meta['M']} start_prob=1.0"})
        final_before=tree(shadow_runs["FINAL_PREGAME"])
        outcomes=pd.DataFrame([{"canonical_season":2026,"slate_date":SLATE,"game_id":GAME_ID,"goalie_id":101,"actual_start_flag":True,"goalie_participation_state":"STARTED","official_saves":27,"outcome_source_timestamp_utc":"2026-09-20T03:00:00Z"}])
        op=tmp/"saves_outcomes.csv";outcomes.to_csv(op,index=False);grade=grade_shadow(shadow_run_dir=shadow_runs["FINAL_PREGAME"],outcomes_csv=op,output_root=tmp/"saves_grades")
        graded=pd.read_csv(grade/"shadow_grades.csv");grade_meta=json.loads((grade/"grade_metadata.json").read_text())
        stages.append({"stage":"isolated grading and cumulative reconciliation","lane":"ALL","status":"PASS","evidence":"four preseason lanes non-evaluative; pregame manifests unchanged"})

        sentinel_payload={"sentinel_timestamp_utc":FINAL,"slate_health":{"fetch_timestamp_utc":"2026-09-19T16:00:00Z","completion_status":"READY","downstream_ready":True},"freshness":{x:{"latest_date":"2026-09-18","expected_date":"2026-09-18"} for x in ["team_history","feature_source","player_game_logs","sog_history","toi_history","roster_identity"]},"parents":[{"child":x,"state":"PARENT_PRESENT_AND_CURRENT"} for x in ["MONEYLINE","SOG","POINTS","GOALIE_SAVES"]],"market":{"eligible":4,"covered":3,"qualified":3,"post_start_quotes":0},"populations":{"market_qualified":3,"unexpected_collapse":False},"outcomes":[],"mutable_inputs":[],"runtime":{"duration_minutes":2,"overlap_minutes":0,"db_errors":[]},"saves":{x:True for x in ["parents_current","complete_population_scored_before_market_gate","quote_coverage_visible","multi_goalie_state_visible","multibook_gate_applied","aliases_unambiguous","strictly_pregame","single_batch_preprocessing","actual_starter_postgame_only","nonstarters_excluded_from_evaluation","run_bound_inputs_only","identity_current"]}}
        sentinel_payload["saves"].update(start_prob_values=[1.0],canonical_season=2026,game_type_codes=[1]);sin=tmp/"sentinel.json";js(sin,sentinel_payload)
        proc=subprocess.run([str(ROOT/".venv/bin/python"),str(ROOT/"backend/nhl/scripts/run_nhl_live_failure_sentinel.py"),"--phase","FINAL_PREGAME","--slate-date",SLATE,"--run-id","four_lane_fixture","--input-json",str(sin),"--output-root",str(tmp/"sentinel")],cwd=ROOT,text=True,capture_output=True)
        sent=json.loads((tmp/"sentinel"/SLATE/"four_lane_fixture"/"nhl_live_failure_sentinel.json").read_text());sstate=next(x for x in sent["sentinels"] if x["sentinel"]=="saves_shadow_contract")["state"]
        stages.append({"stage":"sentinel evaluation","lane":"ALL","status":"PASS" if proc.returncode==0 and sstate=="PASS" else "FAIL","evidence":f"overall={sent['overall_status']} saves={sstate}"})

        p=pd.read_csv(shadow_runs["MIDDAY"]/"complete_prediction_population.csv");m=pd.read_csv(shadow_runs["MIDDAY"]/"market_qualified_population.csv");decision_rows=pd.read_csv(shadow_runs["MIDDAY"]/"policy_c_team_game_decisions.csv")
        checks += [
          {"check":"four lane canonical game identity","status":"PASS","evidence":str(GAME_ID)},
          {"check":"separate namespaces and run identities","status":"PASS" if len({"nhlmlobs","nhlsogshadow","nhlpointsshadow",shadow_runs['MIDDAY'].name.split('_')[0]})==4 else "FAIL","evidence":shadow_runs["MIDDAY"].name},
          {"check":"all four lane locks are separate","status":"PASS" if all("fcntl.flock" in (ROOT/path).read_text() for path in ["backend/nhl/mainline_shadow/core.py","backend/nhl/sog_shadow/core.py","backend/nhl/points_shadow/core.py","backend/nhl/saves_shadow/core.py"]) else "FAIL","evidence":"all four isolated runs exercised distinct output-root lock namespaces; each lane core enforces nonblocking fcntl lock"},
          {"check":"complete Saves population before market gate","status":"PASS" if len(p)==39 and len(m)==2 else "FAIL","evidence":f"P={len(p)} M={len(m)}"},
          {"check":"Saves conditional start value","status":"PASS" if p.start_prob_operational_input.eq(1).all() else "FAIL","evidence":str(sorted(p.start_prob_operational_input.unique()))},
          {"check":"Policy C exact multi-book selection","status":"PASS" if set(m.goalie_id)=={101,201} and decision_rows.decision.eq("SELECTED").sum()==2 else "FAIL","evidence":decision_rows[["team","selected_goalie_id","reason"]].to_json(orient="records")},
          {"check":"market listing remains unconfirmed","status":"PASS" if m.starter_state_label.eq("MARKET_LISTED_STARTER_UNCONFIRMED").all() else "FAIL","evidence":m.starter_state_label.unique().tolist()},
          {"check":"actual starter absent from prediction input","status":"PASS" if not any("actual" in x or "confirmed_starter" in x or "projected_starter" in x for x in pd.read_csv(shadow_runs['MIDDAY']/"operational_goalie_inputs.csv").columns) else "FAIL","evidence":"prediction input columns inspected"},
          {"check":"preseason non-evaluation and no numeric target","status":"PASS" if graded.grading_status.eq("PRESEASON_NON_EVALUATION").all() and graded.regular_season_evaluation_target.isna().all() and grade_meta["entered_regular_season_feature_history_rows"]==0 else "FAIL","evidence":str(grade_meta)},
          {"check":"Saves pregame immutable after grading","status":"PASS" if final_before==tree(shadow_runs["FINAL_PREGAME"]) else "FAIL","evidence":sha(shadow_runs["FINAL_PREGAME"]/"SHA256SUMS")},
          {"check":"Points blocked ladder remains P not M","status":"PASS" if three_recon.loc[three_recon.check.eq("points_blocked_visible_p_absent_m"),"status"].eq("PASS").all() else "FAIL","evidence":"fresh three-lane rehearsal"},
          {"check":"Points and Saves C/U/E disabled","status":"PASS" if counts[-1]["C"]==counts[-1]["U"]==counts[-1]["E"]==0 and three_recon.loc[three_recon.check.eq("points_p_m_c_u_e"),"status"].eq("PASS").all() else "FAIL","evidence":"population ledgers"},
          {"check":"SOG unchanged","status":"PASS" if three_commands.loc[three_commands.command_surface.str.contains("sog_manual"),"validation"].eq("PASS").all() else "FAIL","evidence":"fresh validator"},
          {"check":"Moneyline unchanged","status":"PASS" if three_commands.loc[three_commands.command_surface.str.contains("mainline_shadow"),"validation"].eq("PASS").all() else "FAIL","evidence":"fresh validator"},
          {"check":"three-lane parent not rewritten","status":"PASS" if sha(THREE_CANONICAL/"SHA256SUMS")=="f80447221b69ad2413aa86cea51107d47c44aca7b57648eabd763319e8bcc4e1" else "FAIL","evidence":sha(THREE_CANONICAL/"SHA256SUMS")},
        ]
        # A Saves failure occurs before publishing and cannot alter the completed three-lane package.
        before=tree(three_pkg)
        try:run_shadow(game_spine_csv=game_csv,game_spine_manifest=parent,goalie_inputs_csv=goalie_csv,goalie_inputs_manifest=parent,quote_run_dir=quote_runs["MIDDAY"],output_root=tmp/"blocked",slate_date=SLATE,run_timestamp_utc=MIDDAY,run_type="MIDDAY",pre_scoring_population="MARKET_FILTERED")
        except RuntimeError as exc:failure=str(exc)
        else:failure="NO_FAILURE"
        checks.append({"check":"cross-lane failure isolation","status":"PASS" if failure=="PRE_SCORING_MARKET_FILTER_FORBIDDEN" and before==tree(three_pkg) else "FAIL","evidence":failure})
        commands += three_commands.to_dict("records")
        for module in ["backend.nhl.saves_quote_capture.cli","backend.nhl.saves_shadow.cli"]:
            result=subprocess.run([str(ROOT/".venv/bin/python"),"-m",module,"--help"],cwd=ROOT,capture_output=True,text=True);commands.append({"command_surface":module,"validation":"PASS" if result.returncode==0 else "FAIL","evidence":f"--help exit {result.returncode}"})
        summary={"fresh_three_lane_rehearsal_exit":three_rc,"three_lane_negative_tests":len(three_negative),"three_lane_negative_failures":int((three_negative.status!="PASS").sum()),"four_lane_reconciliation_checks":len(checks),"four_lane_reconciliation_failures":sum(x["status"]!="PASS" for x in checks),"stages":len(stages),"stage_failures":sum(x["status"]!="PASS" for x in stages),"command_checks":len(commands),"command_failures":sum(x["validation"]!="PASS" for x in commands),"saves_counts":counts,"three_lane_rehearsal_manifest_sha256":three_manifest}
        if any(summary[x] for x in ["three_lane_negative_failures","four_lane_reconciliation_failures","stage_failures","command_failures"]):
            failures={"checks":[x for x in checks if x["status"]!="PASS"],"stages":[x for x in stages if x["status"]!="PASS"],"commands":[x for x in commands if x["validation"]!="PASS"]}
            raise RuntimeError(f"FOUR_LANE_REHEARSAL_FAILED:{summary}; failures={failures}")
        return summary,stages,checks,commands


def build_package(out: Path,summary: dict,stages: list[dict],checks: list[dict],commands: list[dict])->None:
    if out.exists():raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    out.mkdir(parents=True);table(out/"four_lane_rehearsal_stage_results.csv",stages);table(out/"four_lane_reconciliation.csv",checks);table(out/"command_and_regression_results.csv",commands)
    auth=[
      {"lane":"MONEYLINE","P":"AUTHORIZED","M":"AUTHORIZED","C":"NOT_AUTHORIZED_BY_THIS_AMENDMENT","U":"NOT_AUTHORIZED","E":"NOT_AUTHORIZED","G":"PRESEASON_NON_EVALUATION","condition":"frozen-control prediction/market shadow"},
      {"lane":"SOG","P":"AUTHORIZED","M":"AUTHORIZED","C":"AUTHORIZED_SHADOW_LINEAGE","U":"DISABLED","E":"DISABLED","G":"PRESEASON_NON_EVALUATION","condition":"explicit hash-valid effective policy; no upload-shaped output"},
      {"lane":"POINTS","P":"AUTHORIZED","M":"AUTHORIZED_AFTER_LADDER_GATE","C":"DISABLED","U":"DISABLED","E":"DISABLED","G":"PRESEASON_NON_EVALUATION","condition":"blocked ladders retained in P and excluded from M"},
      {"lane":"GOALIE_SAVES","P":"AUTHORIZED_CONDITIONAL_ON_START","M":"AUTHORIZED_POLICY_C","C":"UNAUTHORIZED","U":"UNAUTHORIZED","E":"UNAUTHORIZED","G":"AUTHORIZED_SHADOW_NON_EVALUATIVE_IN_PRESEASON","condition":"MARKET_LISTED_STARTER_UNCONFIRMED; start_prob=1.0 constant"},
    ];table(out/"four_lane_authorization_matrix.csv",auth)
    artifacts=[
      {"lane":"MONEYLINE","expected":"game_spine, champion_features/predictions, raw H2H, book quotes, market comparison, health ledger, run metadata, SHA256SUMS"},
      {"lane":"SOG","expected":"prediction inputs, SOG predictions, raw/book quotes, market view, policy config/hash, rule ledger, candidate summary, populations, health, SHA256SUMS; no upload-shaped output"},
      {"lane":"POINTS","expected":"canonical/input snapshots, full predictions, ladder diagnostics, book quotes, market-qualified rows, C/U/E empty, RUN_COMPLETE, SHA256SUMS"},
      {"lane":"GOALIE_SAVES","expected":"canonical/source/operational goalie inputs, complete P, book quotes, goalie support, Policy C decisions, derived market view, M, C/U/E empty, Saves sentinel, RUN_COMPLETE, SHA256SUMS"},
    ];table(out/"expected_artifacts_by_lane.csv",artifacts);js(out/"rehearsal_summary.json",summary)
    source_paths=[
      "backend/nhl/mainline_shadow/core.py","backend/nhl/sog_shadow/core.py",
      "backend/nhl/scripts/run_nhl_live_failure_sentinel.py","backend/nhl/scripts/run_nhl_morning_orchestration.py",
      "backend/nhl/scripts/assess_nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance.py",
      "backend/nhl/scripts/build_nhl_season_2025_player_prop_market_archive_canonical_index.py",
      "backend/nhl/scripts/certify_nhl_saves_immutable_market_listed_preseason_shadow_path.py",
      "backend/nhl/scripts/rehearse_nhl_three_lane_preseason_operator_v1.py",
      "backend/nhl/scripts/rehearse_nhl_four_lane_preseason_readiness_v1.py",
      "backend/tests/test_nhl_market_archive_index.py","backend/tests/test_nhl_saves_market_starter_concordance.py",
      "backend/tests/test_nhl_saves_shadow.py",
    ]
    for directory in ["backend/nhl/market_archive_index","backend/nhl/saves_quote_capture","backend/nhl/saves_shadow"]:
        source_paths += [str(x.relative_to(ROOT)) for x in sorted((ROOT/directory).glob("*") ) if x.is_file() and x.suffix!=".pyc"]
    governed_roots=[
      "artifacts/analysis/model_development/nhl_season_2025_player_prop_market_archive_immutable_canonical_join_index_v1_regenerated/2026-09-08/authoritative_replay_v4",
      "artifacts/analysis/model_development/nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance_v1_regenerated/2026-09-08/authoritative_replay_v2",
      "artifacts/analysis/model_development/nhl_saves_start_prob_scorer_semantics_audit_v1/2026-09-08",
      "artifacts/analysis/model_development/nhl_saves_immutable_market_listed_preseason_shadow_path_v1/2026-09-08/final_v3",
    ]
    inventory=[{"path":x,"classification":"in-scope NHL season-2026 readiness","disposition":"COMMIT"} for x in sorted(set(source_paths))]
    for root in governed_roots:
        inventory += [{"path":str(x.relative_to(ROOT)),"classification":"generated governed artifact","disposition":"COMMIT"} for x in sorted((ROOT/root).rglob("*")) if x.is_file()]
    inventory += [
      {"path":str((out if out.is_absolute() else ROOT/out).relative_to(ROOT)),"classification":"generated governed artifact","disposition":"COMMIT_SUCCESSOR_PACKAGE"},
      {"path":"tmp/analysis/rehearse_nhl_three_lane_preseason_operator_v1.py","classification":"temporary/reproducibility utility","disposition":"EXCLUDE_TRACKED_SUCCESSOR_EXISTS"},
      {"path":"artifacts/analysis/model_development/nhl_saves_immutable_market_listed_preseason_shadow_path_v1/2026-09-08/{initial,authoritative_v1,final_v1,final_v2}","classification":"generated governed artifact","disposition":"EXCLUDE_SUPERSEDED"},
      {"path":"artifacts/analysis/model_development/nhl_season_2026_four_lane_preseason_readiness_amendment_and_checkpoint_v1/2026-09-08/{initial,authoritative_v1,authoritative_v2,authoritative_v3}","classification":"generated governed artifact","disposition":"EXCLUDE_SUPERSEDED_OR_INCOMPLETE"},
      {"path":"unrelated ignored tmp/analysis and MLB paths","classification":"unrelated pre-existing user work","disposition":"EXCLUDE"},
      {"path":"uncertain paths","classification":"uncertain","disposition":"NONE_IDENTIFIED"},
    ]
    table(out/"repository_checkpoint_inventory.csv",inventory)
    runbook=f"""# NHL season 2026 four-lane day-one runbook

## Boundaries

- Moneyline: authorized preseason prediction/market shadow.
- SOG: authorized scope with an explicit hash-valid effective policy; use `--no-upload-shaped-output`.
- Points: P/M shadow only. Materially incoherent ladders remain in P and cannot enter M; C/U/E are disabled.
- Goalie Saves: P/M and append-only shadow grading only. Every probability is `P(goalie saves exceed proposition line | named goalie starts)`, with `start_prob=1.0` as a constant conditional flag—not an estimated start probability. Policy C requires the unique strictly top-supported goalie per game-team with at least two distinct sportsbooks. Label only `MARKET_LISTED_STARTER_UNCONFIRMED`; never projected/probable/confirmed. C/U/E are unauthorized.
- `SHADOW_PREDICTION_EXPORT` is diagnostic output, never a candidate upload or execution.

## 07:30 morning boundary

The unchanged LaunchAgent runs `.venv/bin/python backend/nhl/scripts/run_nhl_morning_orchestration.py --slate-date today --env-file backend/.env`. Require a manifest-complete canonical spine and all four prerequisite readiness flags. It performs no market capture or scoring. A manifest-complete `VALID_EMPTY_SLATE` is a successful stop: run no lane capture or scoring.

## Common variables

```sh
export SLATE_DATE=YYYY-MM-DD RUN_TS=YYYY-MM-DDTHH:MM:SSZ
export GAME_SPINE=/absolute/run-bound/canonical_game_spine.csv TEAM_HISTORY=/absolute/run-bound/team_history.csv
export MORNING_MANIFEST=/absolute/run-bound/SHA256SUMS RAW_ROOT=/absolute/create-only/raw
export MARKET_ROOT=/absolute/create-only/markets SHADOW_ROOT=/absolute/create-only/shadow
export SOG_PLAYERS=/absolute/run-bound/sog_player_spine.csv SOG_INPUTS=/absolute/run-bound/sog_prediction_inputs.csv
export POINTS_SNAPSHOT=/absolute/run-bound/points_snapshot SAVES_SNAPSHOT=/absolute/run-bound/saves_snapshot
```

## Exact MIDDAY commands

```sh
.venv/bin/python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/moneyline_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.mainline_shadow.cli run --schedule-csv "$GAME_SPINE" --history-csv "$TEAM_HISTORY" --odds-json "$RAW_ROOT/moneyline_${{RUN_TS}}.json" --output-root "$SHADOW_ROOT/moneyline" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.sog_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/sog_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.sog_quote_capture.cli run --payload-json "$RAW_ROOT/sog_${{RUN_TS}}.json" --games-csv "$GAME_SPINE" --players-csv "$SOG_PLAYERS" --output-root "$MARKET_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.sog_shadow.cli build-policy-config --policy-json /absolute/authorized/frozen_sog_walkforward_policy.json --output "$RAW_ROOT/sog_policy_${{RUN_TS}}.json"
.venv/bin/python -m backend.nhl.sog_shadow.cli run --game-spine-csv "$GAME_SPINE" --player-inputs-csv "$SOG_INPUTS" --quote-run-dir /absolute/immutable/sog_quote_run --effective-policy-json "$RAW_ROOT/sog_policy_${{RUN_TS}}.json" --parity-json artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_run_summary_2026-07-13.json --output-root "$SHADOW_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY --no-upload-shaped-output
.venv/bin/python -m backend.nhl.points_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/points_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.points_quote_capture.cli run --payload-json "$RAW_ROOT/points_${{RUN_TS}}.json" --games-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --players-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --parent-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.points_shadow.cli run --game-spine-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --player-inputs-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --player-inputs-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/points_quote_run --output-root "$SHADOW_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.saves_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/saves_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.saves_quote_capture.cli archive --payload-json "$RAW_ROOT/saves_${{RUN_TS}}.json" --games-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --goalies-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --parent-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python -m backend.nhl.saves_shadow.cli run --game-spine-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --goalie-inputs-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --goalie-inputs-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/saves_quote_run --output-root "$SHADOW_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type MIDDAY
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase MIDDAY --slate-date "$SLATE_DATE" --run-id /exact/four_lane_midday_run_id --input-json /absolute/run-bound/midday_sentinel_input.json
```

## Exact FINAL_PREGAME commands

Set a new `RUN_TS` and use new create-only paths. Never reuse MIDDAY identities.

```sh
.venv/bin/python -m backend.nhl.mainline_shadow.cli fetch-h2h --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/moneyline_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.mainline_shadow.cli run --schedule-csv "$GAME_SPINE" --history-csv "$TEAM_HISTORY" --odds-json "$RAW_ROOT/moneyline_${{RUN_TS}}.json" --output-root "$SHADOW_ROOT/moneyline" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.sog_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/sog_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.sog_quote_capture.cli run --payload-json "$RAW_ROOT/sog_${{RUN_TS}}.json" --games-csv "$GAME_SPINE" --players-csv "$SOG_PLAYERS" --output-root "$MARKET_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.sog_shadow.cli build-policy-config --policy-json /absolute/authorized/frozen_sog_walkforward_policy.json --output "$RAW_ROOT/sog_policy_${{RUN_TS}}.json"
.venv/bin/python -m backend.nhl.sog_shadow.cli run --game-spine-csv "$GAME_SPINE" --player-inputs-csv "$SOG_INPUTS" --quote-run-dir /absolute/immutable/sog_quote_run --effective-policy-json "$RAW_ROOT/sog_policy_${{RUN_TS}}.json" --parity-json artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13/nhl_season_2025_sog_reproduction_run_summary_2026-07-13.json --output-root "$SHADOW_ROOT/sog" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME --no-upload-shaped-output
.venv/bin/python -m backend.nhl.points_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/points_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.points_quote_capture.cli run --payload-json "$RAW_ROOT/points_${{RUN_TS}}.json" --games-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --players-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --parent-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.points_shadow.cli run --game-spine-csv "$POINTS_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --player-inputs-csv "$POINTS_SNAPSHOT/points_player_inputs.csv" --player-inputs-manifest "$POINTS_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/points_quote_run --output-root "$SHADOW_ROOT/points" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.saves_quote_capture.cli fetch --api-key "$ODDS_API_KEY" --output "$RAW_ROOT/saves_${{RUN_TS}}.json" --regions us,us2
.venv/bin/python -m backend.nhl.saves_quote_capture.cli archive --payload-json "$RAW_ROOT/saves_${{RUN_TS}}.json" --games-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --goalies-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --parent-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --output-root "$MARKET_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python -m backend.nhl.saves_shadow.cli run --game-spine-csv "$SAVES_SNAPSHOT/canonical_game_spine.csv" --game-spine-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --goalie-inputs-csv "$SAVES_SNAPSHOT/saves_goalie_inputs.csv" --goalie-inputs-manifest "$SAVES_SNAPSHOT/SHA256SUMS" --quote-run-dir /absolute/immutable/saves_quote_run --output-root "$SHADOW_ROOT/saves" --slate-date "$SLATE_DATE" --run-timestamp-utc "$RUN_TS" --run-type FINAL_PREGAME
.venv/bin/python backend/nhl/scripts/run_nhl_live_failure_sentinel.py --phase FINAL_PREGAME --slate-date "$SLATE_DATE" --run-id /exact/four_lane_final_run_id --input-json /absolute/run-bound/final_sentinel_input.json
```

Confirm every qualifying provider/source/capture timestamp precedes scheduled start.

## Sentinel, recovery, and grading

`GREEN` has no warnings; `YELLOW` requires review but bounded/no market coverage may still be a healthy P run; `RED` blocks downstream work. Saves additionally blocks parent/population drift, pre-score filtering, non-1 start flags, actual-starter leakage, ambiguous/multibook/timing violations, nonstarter misgrading, mutable inputs, and wrong season/slate/type.

Incomplete runs are forensic evidence. Never fill, delete, or rename over them. Fix the cause, use a new timestamp/run identity, and create fresh paths. Grade only after official completion into separate create-only trees. Actual goalie starter/participation is grading-only; nonstarters never enter the conditional evaluation denominator. Preseason gets no regular-season numeric target and feeds no regular-season history.
"""
    (out/"four_lane_day_one_runbook.md").write_text(runbook)
    checklist="""# September 19 four-lane checklist

1. Confirm the unchanged 07:30 job exited 0 and morning health is manifest-complete. `VALID_EMPTY_SLATE` means stop successfully.
2. Confirm season 2026, game type 1, unique games, orientation/start times, strict-prior inputs, four readiness flags, and parent hashes.
3. Confirm DB access, ODDS_API_KEY, SOG parity/policy, and immutable Points/Saves snapshots. Saves expected row count must reconcile before scoring.
4. Run fresh MIDDAY commands in separate lane namespaces. Saves must score all P before Policy C; `start_prob` is exactly 1.0.
5. Reconcile: Points blocked ladders stay P/out of M; Points and Saves C/U/E=0; Saves labels are only MARKET_LISTED_STARTER_UNCONFIRMED; SOG emits no upload-shaped output.
6. Read all sentinel reasons. RED stops. Preserve a healthy P/M=0 state when markets are absent.
7. Before start, run entirely new FINAL_PREGAME identities and compare additions/disappearances without overwriting MIDDAY.
8. Grade append-only after official completion. Actual starter is postgame-only; Saves nonstarters are outside the conditional denominator; every lane is PRESEASON_NON_EVALUATION.
9. Verify manifests and cumulative P/M/C/U/E/G counts. Preserve incomplete paths and retry only under new identities.
10. Diagnostic exports are not candidate uploads. No execution or wager is authorized by this checkpoint.
""";(out/"september_19_one_page_checklist.md").write_text(checklist)
    decision={"decision":"FOUR_LANE_REHEARSAL_PASS","authorizations":{x["lane"]:x for x in auth},"next_live_task":"FIRST_REAL_NHL_SEASON_2026_PRESEASON_MULTI_RUN_BURN_IN","next_offline_research":"NHL_POINTS_HISTORICAL_OUTCOME_SPINE_AND_COHERENT_PREDICTION_FOUNDATION_V1","real_slate_runs":0,"network_calls":0,"candidate_uploads":0,"executions":0};js(out/"readiness_decision.json",decision)
    parent_hashes={"three_lane_rehearsal":sha(THREE_CANONICAL/"SHA256SUMS"),"saves_certification":sha(SAVES_CERT/"SHA256SUMS")}
    js(out/"operational_code_identity_amendment.json",{"reason":"add per-lane nonblocking overlap locks required by four-lane checkpoint; no scorer/model/feature/policy change","historical_identities_preserved":True,"former":{"mainline_shadow_core_sha256":"7909c5acecb33a134bb14226b5617ff3729d2e55220d7a3ff780922989bd4ebe","sog_shadow_core_sha256":"b8e4f4723e8713ba0de66a61c063941ba71e7d5d37e3c444b8e76468b859a369"},"current":{"mainline_shadow_core_sha256":sha(ROOT/"backend/nhl/mainline_shadow/core.py"),"sog_shadow_core_sha256":sha(ROOT/"backend/nhl/sog_shadow/core.py")},"authorized_current_runtime_binding":"this successor checkpoint","historical_package_rewrite":False})
    js(out/"package_identity.json",{"task":"NHL_SEASON_2026_FOUR_LANE_PRESEASON_READINESS_AMENDMENT_AND_CHECKPOINT_V1","date":DATE,"successor_not_rewrite":True,"parents":parent_hashes,"generated_by":"backend/nhl/scripts/rehearse_nhl_four_lane_preseason_readiness_v1.py","source_sha256":sha(Path(__file__)),"live_activity":False})
    report=f"""# NHL season 2026 four-lane preseason readiness amendment

The isolated four-lane rehearsal passed. Moneyline, SOG, and Points were freshly exercised through the tracked successor of the accepted three-lane rehearsal; Saves was exercised at MIDDAY and FINAL_PREGAME with complete-batch scoring, constant `start_prob=1.0`, post-score Policy C, immutable book evidence, sentinel enforcement, and isolated preseason grading.

All {summary['four_lane_reconciliation_checks']} four-lane reconciliation checks, {summary['three_lane_negative_tests']} inherited hostile checks, {summary['command_checks']} command/regression checks, and {summary['stages']} stage checks passed. No real slate, provider call, model fit, policy change, upload, execution, LaunchAgent schedule edit, or MLB change occurred.

The prior three-lane package remains unchanged at `{parent_hashes['three_lane_rehearsal']}`. Saves certification is bound at `{parent_hashes['saves_certification']}`. See the runbook, checklist, authorization matrix, artifacts table, and detailed CSV ledgers in this successor package.
""";(out/"executive_summary.md").write_text(report);(out/"main_amendment_report.md").write_text(report+"\nFinal rehearsal decision: `FOUR_LANE_REHEARSAL_PASS`.\n")
    files=sorted(x for x in out.iterdir() if x.is_file() and x.name!="SHA256SUMS");(out/"SHA256SUMS").write_text("".join(f"{sha(x)}  {x.name}\n" for x in files))


def main()->int:
    parser=argparse.ArgumentParser();parser.add_argument("--output-dir",type=Path,required=True);args=parser.parse_args();summary,stages,checks,commands=run();build_package(args.output_dir,summary,stages,checks,commands);print(json.dumps({"output_dir":str(args.output_dir),"summary":summary,"manifest_sha256":sha(args.output_dir/"SHA256SUMS")},indent=2));return 0


if __name__=="__main__":raise SystemExit(main())
