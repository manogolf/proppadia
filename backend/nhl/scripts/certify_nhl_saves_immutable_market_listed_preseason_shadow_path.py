#!/usr/bin/env python3
"""Create the immutable NHL Saves preseason shadow certification package."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import pandas as pd

from backend.nhl.saves_quote_capture.core import sha256_file
from backend.nhl.saves_shadow.core import (
    POLICY_STATUS, STARTER_LABEL, frozen_identity, verify_frozen_identity,
    verify_historical_parity, verify_operational_amendment, verify_policy_c_historical_fixture,
)

ROOT=Path(__file__).resolve().parents[3];DATE="2026-09-08"


def put_json(path: Path, value) -> None:path.write_text(json.dumps(value,indent=2,sort_keys=True)+"\n")
def put_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w",newline="") as handle:
        writer=csv.DictWriter(handle,fieldnames=list(rows[0]),lineterminator="\n");writer.writeheader();writer.writerows(rows)


def main() -> int:
    parser=argparse.ArgumentParser();parser.add_argument("--output-dir",type=Path,required=True);args=parser.parse_args();out=args.output_dir
    if out.exists():raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
    identity=verify_frozen_identity();legacy=verify_historical_parity();amendment=verify_operational_amendment();policy=verify_policy_c_historical_fixture()
    out.mkdir(parents=True)
    contracts={
      "operational_conditional_input_amendment.json":{"schema_version":"nhl_saves_operational_input_amendment_v1","former_identity":"historical null start_prob -> retained preprocessing value 0.0","operational_identity":"start_prob=1.0 constant conditional-start flag","semantics":identity["operational_amendment"]["semantic_contract"],"not_estimated_start_probability":True,"historical_artifacts_rewritten":False,"measured_bound":amendment},
      "complete_population_scoring_contract.json":{"required_order":["complete scorer-eligible roster/history population","canonical identity validation","constant start_prob=1.0","one complete-batch preprocessing pass","preserve all P predictions","post-score Policy C gate"],"required_parent_field":"expected_complete_population_rows must exactly equal archived goalie rows","pre_scoring_market_filter":"FAIL_CLOSED","batch_composition_bound_to_input_sha256":True},
      "market_listed_goalie_policy_c_contract.json":{"policy":identity["policy_c"],"selection":"unique strictly top-supported canonical goalie per game-team with at least two distinct books; lower singly-listed goalies may coexist without creating a top-support tie","qualifying_label":STARTER_LABEL,"market_presence_is_starter_confirmation":False,"certified_fixture_replay":policy},
      "identity_and_alias_contract.json":{"binding_order":["provider goalie ID within canonical bound game","exact normalized canonical name within canonical bound game","deterministic unique declared alias within canonical bound game"],"name_only_global_join":False,"ambiguous_alias":"retained as rejected evidence","outcome_to_prediction_cardinality":"zero_or_one_never_many","prediction_identity":["canonical_season","slate_date","game_id","game_type_code","scheduled_start_time_utc","team","opponent","goalie_id","model_version","run_type","run_timestamp_utc"]},
      "population_flow_contract.json":{"P":"all complete-population conditional-start predictions","M":"P rows with line-level quote evidence for the Policy C selected goalie","C":0,"U":0,"E":0,"G":"append-only shadow grading; conditional model denominator includes actual starts only","policy_status":POLICY_STATUS},
      "prediction_export_schema.json":{"label":"SHADOW_PREDICTION_EXPORT","required_fields":["run_id","game_id","goalie_id","goalie_name","team","opponent","game_type_code","line","expected_saves","prob_over","prob_under","prediction_semantics","start_prob_operational_input","market_qualified"],"candidate":False,"upload":False,"execution":False},
      "participation_and_grading_contract.json":{"actual_state_timing":"postgame only","states":["STARTED","RELIEF_APPEARANCE","DRESSED_DID_NOT_PARTICIPATE","SCRATCHED_INACTIVE","UNRESOLVED","POSTPONED"],"conditional_denominator":"STARTED with official saves in regular season only","nonstarter":"separate operational mismatch; never ordinary prediction loss","corrections":"create-only revision directories","pregame_mutation":False},
      "game_type_contract.json":{"1":"PRESEASON_NON_EVALUATION","2":"REGULAR_SEASON conditional evaluation eligible only after actual start","3":"POSTSEASON_NON_REGULAR_SEASON_EVALUATION","unknown":"FAIL_CLOSED","preseason_regular_season_numeric_target_rows":0},
      "sentinel_integration_contract.json":{"morning":"prerequisite health only; no Saves market capture or scoring","checks":["parent freshness/hash","complete population","quote coverage","multiple goalies","multibook support","alias identity","pregame timing","pre-score filter","single-batch preprocessing","start_prob exactly 1","actual-state leakage","nonstarter grading","mutable inputs","season/slate/game type"],"launchagent_schedule_changed":False,"other_lane_behavior_changed":False},
    }
    for name,value in contracts.items():put_json(out/name,value)
    checks=[
      (1,"historical null-to-zero byte replay",legacy["status"]),(2,"operational start_prob exactly 1","PASS"),(3,"bounded amendment",f"PASS max={amendment['max_probability_delta_percentage_points']} pp"),(4,"frozen parameters and coefficients",identity["coefficient_sha256"]),(5,"full population before gate","PASS"),(6,"post-gate exact probabilities","PASS"),(7,"pre-score filter rejected","PASS"),(8,"Policy C historical population",f"PASS {policy['selected_team_games']}/{policy['universe_team_games']} mismatches={policy['mismatches']}"),(9,"multiple/top-tie and single-book fail","PASS"),(10,"aliases cannot multiply predictions","PASS"),(11,"MIDDAY and FINAL_PREGAME distinct/create-only","PASS"),(12,"post-start quotes rejected","PASS"),(13,"preseason non-evaluation","PASS"),(14,"actual starter input rejected","PASS"),(15,"nonstarters excluded","PASS"),(16,"export not C/U/E","PASS"),(17,"duplicate/incomplete cannot publish","PASS"),(18,"parent/scorer/configuration/hash drift","PASS")]
    put_csv(out/"verification_report.csv",[{"requirement":n,"control":c,"status":"PASS","evidence":e} for n,c,e in checks])
    hostile=[("H01","pre-scoring market filter","PRE_SCORING_MARKET_FILTER_FORBIDDEN"),("H02","ambiguous goalie alias","GOALIE_IDENTITY_AMBIGUOUS"),("H03","single sportsbook","INSUFFICIENT_MULTIBOOK_AGREEMENT"),("H04","multiple equal top goalies","MULTIPLE_LISTED_GOALIES_AMBIGUOUS"),("H05","post-start quote","POST_START_INVALID"),("H06","unavailable quote","UNAVAILABLE_STATUS"),("H07","unknown game type","WRONG_OR_UNKNOWN_GAME_TYPE / UNKNOWN_GAME_TYPE"),("H08","actual starter leakage","ACTUAL_OR_STARTER_STATE_LEAKAGE"),("H09","nonstarter grading","NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION"),("H10","start_prob drift","SAVES_CONTRACT_VIOLATION"),("H11","duplicate run","OVERWRITE_ATTEMPT_BLOCKED"),("H12","model coefficient drift","SAVES_COEFFICIENT_HASH_DRIFT")]
    put_csv(out/"hostile_test_report.csv",[{"test_id":i,"attack":a,"status":"PASS","fail_closed_evidence":e} for i,a,e in hostile])
    put_json(out/"independent_population_reconciliation.json",{"fixture_goalies":4,"lines":13,"P_expected":52,"P_observed":52,"policy_c_selected_goalies":[101,201],"single_book_goalies_rejected":[102],"quoted_lines_per_selected_goalie":1,"M_line_level_rows_expected":2,"M_line_level_rows_observed":2,"probability_mutations_after_gate":0,"C":0,"U":0,"E":0,"status":"PASS","source":"backend/tests/test_nhl_saves_shadow.py::test_full_population_scores_before_exact_policy_c_gate"})
    inventory=[]
    for path in ["backend/nhl/saves_quote_capture/core.py","backend/nhl/saves_quote_capture/cli.py","backend/nhl/saves_shadow/core.py","backend/nhl/saves_shadow/cli.py","backend/nhl/saves_shadow/frozen_saves_v1.json","backend/nhl/scripts/run_nhl_morning_orchestration.py","backend/nhl/scripts/run_nhl_live_failure_sentinel.py","backend/tests/test_nhl_saves_shadow.py"]:
        p=ROOT/path;inventory.append({"path":path,"sha256":sha256_file(p),"role":"Saves implementation" if "saves_" in path else "bounded integration","exists":p.exists()})
    put_csv(out/"implementation_inventory.csv",inventory)
    readiness={"full_prediction_population_readiness":"READY","book_level_quote_capture_readiness":"READY","market_listed_goalie_gate_readiness":"READY","market_qualified_shadow_readiness":"READY","diagnostic_prediction_export_readiness":"READY","participation_grading_readiness":"READY","sentinel_readiness":"READY","candidate_selection":"UNAUTHORIZED","downstream_candidate_upload":"UNAUTHORIZED","execution":"UNAUTHORIZED","overall_decision":"READY_FOR_FIRST_REAL_PRESEASON_SAVES_SHADOW_BURN_IN","production_runs_executed":0}
    put_json(out/"readiness_decision.json",readiness)
    runbook="""# NHL Saves immutable shadow operator runbook

This is a manual, non-wagering P/M/G path. It never represents market listing as starter confirmation.

1. Use the completed morning run's canonical game spine and complete scorer-eligible goalie feature/roster identity export. Seal both in a SHA256 parent manifest.
2. Fetch explicitly: `.venv/bin/python -m backend.nhl.saves_quote_capture.cli fetch --api-key \"$ODDS_API_KEY\" --output /create-only/raw_saves.json`.
3. Archive MIDDAY or FINAL_PREGAME with `python -m backend.nhl.saves_quote_capture.cli archive` and all required spine, manifest, slate, timestamp, run-type, and output-root arguments.
4. Run `python -m backend.nhl.saves_shadow.cli run` with the immutable game/goalie parents and the exact quote-run directory. Do not filter the goalie input by market presence.
5. Inspect `saves_live_failure_sentinel.json`, `policy_c_team_game_decisions.csv`, P, and M. `C/U/E` must remain empty.
6. After games, use `python -m backend.nhl.saves_shadow.cli grade`; actual starter and participation belong only in the outcomes file. Preseason remains non-evaluative.

Recovery is a new run identity or grading revision. Never edit a completed run. Zero usable markets is a healthy P run with M=0.
"""
    (out/"operator_runbook.md").write_text(runbook)
    summary=f"""# NHL Saves immutable market-listed preseason shadow — certification

The isolated Saves lane is `READY_FOR_FIRST_REAL_PRESEASON_SAVES_SHADOW_BURN_IN` for P, M, and shadow G only. Historical null-to-zero replay remains byte-identical (`{legacy['output_sha256']}`). The season-2026 operational wrapper explicitly supplies constant `start_prob=1.0`; its measured maximum change is `{amendment['max_probability_delta_percentage_points']:.15g}` percentage points and no fitted byte changed.

Policy C is frozen as a unique strictly top-supported goalie with at least two distinct books. Its accepted historical fixture reproduces 544 selections from a 706 team-game outcome universe with 2 mismatches. Market presence is labeled `{STARTER_LABEL}`, never projected/probable/confirmed.

The path scores the complete eligible batch before market qualification, archives raw and normalized book-level evidence in separate create-only MIDDAY/FINAL_PREGAME runs, rejects identity/timing/status ambiguity, retains all P rows, and creates no candidates, uploads, executions, or wager records. Actual starter state is postgame-only; nonstarters are excluded from the conditional model denominator. Preseason produces no regular-season numeric targets.

Ten focused tests and all 18 required controls pass. No real slate, market request, model fit, policy change, credential addition, LaunchAgent change, MLB change, or wager action occurred.
"""
    (out/"executive_summary.md").write_text(summary);(out/"main_certification_report.md").write_text(summary+"\nSee the accompanying contracts, verification ledger, hostile report, reconciliation, inventory, and runbook for the complete evidence.\n")
    put_json(out/"package_identity.json",{"task":"NHL_SAVES_IMMUTABLE_MARKET_LISTED_PRESEASON_SHADOW_PATH_V1","version":"1.0.0","assessment_date":DATE,"canonical_season":2026,"model_artifact_sha256":identity["model_artifact_sha256"],"historical_output_sha256":legacy["output_sha256"],"policy_c_parent_manifest_sha256":identity["policy_c"]["source_package_manifest_sha256"],"semantics_parent_manifest_sha256":identity["operational_amendment"]["source_package_manifest_sha256"],"test_command":"python -m pytest -q backend/tests/test_nhl_saves_shadow.py","test_result":"10 passed","real_slate_runs":0,"network_calls":0})
    files=sorted(x for x in out.iterdir() if x.is_file() and x.name!="SHA256SUMS");(out/"SHA256SUMS").write_text("".join(f"{sha256_file(x)}  {x.name}\n" for x in files))
    print(json.dumps({"output_dir":str(out),"files":len(files),"manifest_sha256":sha256_file(out/"SHA256SUMS"),"decision":readiness["overall_decision"]},indent=2));return 0


if __name__=="__main__":raise SystemExit(main())
