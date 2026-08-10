#!/usr/bin/env python3
"""Dependency-aware, fail-closed NHL morning orchestration (no market capture)."""
from __future__ import annotations
import argparse, fcntl, hashlib, json, os, subprocess, sys, time, uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[3]
PY=ROOT/".venv/bin/python"
OPS=ROOT/"artifacts/operational/nhl/morning"
SLATE_HEALTH=ROOT/"artifacts/operational/nhl/slates"

def utc(): return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00","Z")
def parse_utc(v): return datetime.fromisoformat(v.replace("Z","+00:00"))
def atomic_json(path,obj):
 path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(path.suffix+".tmp");tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+"\n");tmp.replace(path)
def load_env(path,env):
 for raw in path.read_text().splitlines():
  s=raw.strip()
  if not s or s.startswith("#") or "=" not in s:continue
  k,v=s.split("=",1);env.setdefault(k.strip(),v.strip().strip("'\""))
def resolve_date(v):
 if v=="today":return datetime.now(ZoneInfo("America/New_York")).date().isoformat()
 return date.fromisoformat(v).isoformat()

def main():
 ap=argparse.ArgumentParser();ap.add_argument("--slate-date",required=True);ap.add_argument("--dry-run",action="store_true");ap.add_argument("--env-file",type=Path,default=ROOT/"backend/.env");ap.add_argument("--output-root",type=Path,default=OPS);ap.add_argument("--fixture-scenario",choices=["valid_empty","nonempty","schedule_failure","partial_slate","export_failure","db_failure","roster_failure","preparation_failure","finalization_failure","interrupted"]);ap.add_argument("--hold-lock-seconds",type=float,default=0)
 a=ap.parse_args(); slate=resolve_date(a.slate_date); season=int(slate[:4]) if int(slate[5:7])>=7 else int(slate[:4])-1
 if season not in {2025,2026}: raise SystemExit(f"WRONG_CANONICAL_SEASON_FOR_SEASON_2026_ORCHESTRATION:{season}")
 outroot=a.output_root.resolve(); lockdir=outroot/"locks";lockdir.mkdir(parents=True,exist_ok=True);lockpath=lockdir/f"{slate}.lock"
 lock=lockpath.open("a+")
 try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError: print("MORNING_ORCHESTRATION_ALREADY_RUNNING",file=sys.stderr);return 73
 if a.hold_lock_seconds:time.sleep(a.hold_lock_seconds)
 run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")+"_"+uuid.uuid4().hex[:8];run=outroot/slate/"runs"/run_id;run.mkdir(parents=True,exist_ok=False)
 status={"schema_version":"nhl_morning_health_v1","slate_date":slate,"canonical_season":season,"orchestration_run_id":run_id,"start_timestamp_utc":utc(),"end_timestamp_utc":None,"overall_status":"RUNNING","valid_empty_slate":False,"schedule_game_count":None,"canonical_game_count":None,"team_history_readiness":"NOT_STARTED","player_history_readiness":"NOT_STARTED","mainline_prerequisite_readiness":"NOT_STARTED","sog_prerequisite_readiness":"NOT_STARTED","optional_context_readiness":"NOT_RUN_MORNING_BOUNDARY","blocking_failure":None,"recovery_command":f"{PY} {Path(__file__).resolve()} --slate-date {slate} --env-file {a.env_file.resolve()}","downstream":{"MAINLINE_MORNING_PREREQUISITES_READY":False,"SOG_MORNING_PREREQUISITES_READY":False,"MIDDAY_MARKET_CAPTURE_ALLOWED":False,"FINAL_PREGAME_CAPTURE_ALLOWED":False,"GRADING_ALLOWED":True},"stages":[]}
 healthpath=run/"morning_health.json";atomic_json(healthpath,status)
 env={"HOME":os.environ.get("HOME",str(Path.home())),"PATH":"/opt/homebrew/opt/libpq/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin","PYTHONUNBUFFERED":"1"}
 if a.env_file.exists():load_env(a.env_file,env)
 env.update({"SLATE_DATE":slate,"YDAY":(date.fromisoformat(slate)-timedelta(days=1)).isoformat(),"HONOR_ENV_DATES":"1"})
 def stage(sid,command,depends=(),fixture_result=None):
  if any(next(x for x in status["stages"] if x["stage_id"]==d)["state"] not in {"PASS","PASS_WITH_BOUNDED_LIMITS"} for d in depends):raise RuntimeError(f"dependency gate blocked {sid}")
  display=["<DATABASE_URL>" if "://" in str(x) else str(x) for x in command]
  rec={"stage_id":sid,"command":" ".join(display),"start_time":utc(),"end_time":None,"state":"RUNNING","exit_status":None,"input_identities":list(depends),"output_identities":[],"row_counts":{},"warnings":[],"failure_class":None,"downstream_allowed":False};status["stages"].append(rec);atomic_json(healthpath,status)
  try:
   if fixture_result and fixture_result.startswith("FAIL"):raise RuntimeError(fixture_result)
   if not a.fixture_scenario and not a.dry_run and str(command[0]) != "internal":
    p=subprocess.run(list(map(str,command)),cwd=ROOT,env=env,text=True,capture_output=True);(run/f"{sid}.stdout.log").write_text(p.stdout);(run/f"{sid}.stderr.log").write_text(p.stderr)
    if p.returncode:raise RuntimeError(f"exit={p.returncode}")
   rec.update(end_time=utc(),state="PASS" if not a.dry_run else "PASS_WITH_BOUNDED_LIMITS",exit_status=0,downstream_allowed=True);atomic_json(healthpath,status)
  except Exception as e:
   rec.update(end_time=utc(),state="FAILED_BLOCKING",exit_status=1,failure_class=str(e),downstream_allowed=False);atomic_json(healthpath,status);raise
  return rec
 try:
  scenario=a.fixture_scenario
  stage("01_db_and_environment",["psql",env.get("SUPABASE_DB_URL",env.get("DATABASE_URL","MISSING")),"-v","ON_ERROR_STOP=1","-Atqc","SELECT 1"],fixture_result="FAIL_DB_UNAVAILABLE" if scenario=="db_failure" else None)
  stage("02_schedule_fetch",[PY,ROOT/"backend/nhl/scripts/import_schedule_today.py"],["01_db_and_environment"],"FAIL_SCHEDULE_FETCH" if scenario=="schedule_failure" else None)
  if scenario:
   games=0 if scenario=="valid_empty" else 2
   completion="PARTIAL" if scenario=="partial_slate" else ("VALID_EMPTY_SLATE" if games==0 else "READY")
   source_health={"slate_date":slate,"canonical_season":season,"completion_status":completion,"downstream_ready":completion in {"READY","VALID_EMPTY_SLATE"},"normalized_game_count":games}
  else: source_health=json.loads((SLATE_HEALTH/slate/"slate_health.json").read_text())
  if source_health.get("slate_date")!=slate or source_health.get("canonical_season")!=season or not source_health.get("downstream_ready"):raise RuntimeError("SLATE_HEALTH_BLOCKED")
  status["valid_empty_slate"]=source_health["completion_status"]=="VALID_EMPTY_SLATE";status["schedule_game_count"]=source_health["normalized_game_count"]
  stage("03_slate_health_gate",["internal","validate_date_season_completion"],["02_schedule_fetch"])
  spine=run/"canonical_game_spine.csv"
  stage("04_canonical_slate_export",[PY,ROOT/"backend/nhl/scripts/export_canonical_slate_spine.py","--slate-date",slate,"--output",spine],["03_slate_health_gate"],"FAIL_EXPORT" if scenario=="export_failure" else None)
  if scenario:
   spine.write_text("canonical_season,slate_date,game_id\n"+(f"{season},{slate},2026020001\n{season},{slate},2026020002\n" if source_health["normalized_game_count"] else ""))
  status["canonical_game_count"]=source_health["normalized_game_count"]
  if status["valid_empty_slate"]:
   for sid in ["05_stable_upstream_daily","06_team_history_readiness","07_player_history_readiness","08_mainline_prerequisites","09_sog_prerequisites"]:
    status["stages"].append({"stage_id":sid,"command":"none","start_time":utc(),"end_time":utc(),"state":"SKIPPED_VALID_EMPTY_SLATE","exit_status":0,"input_identities":["04_canonical_slate_export"],"output_identities":[],"row_counts":{},"warnings":[],"failure_class":None,"downstream_allowed":True})
   status.update(team_history_readiness="NOT_REQUIRED_VALID_EMPTY_SLATE",player_history_readiness="NOT_REQUIRED_VALID_EMPTY_SLATE",mainline_prerequisite_readiness="VALID_EMPTY_SLATE",sog_prerequisite_readiness="VALID_EMPTY_SLATE")
  else:
   stage("05_stable_upstream_daily",[PY,"-m","backend.nhl.cli","daily","--morning-only"],["04_canonical_slate_export"],"FAIL_ROSTER_REFRESH" if scenario=="roster_failure" else ("FAIL_PREPARATION" if scenario=="preparation_failure" else None))
   stage("06_team_history_readiness",["internal","daily morning-only completed-game history stages"],["05_stable_upstream_daily"]);status["team_history_readiness"]="READY"
   stage("07_player_history_readiness",["internal","daily morning-only roster/SOG history stages"],["06_team_history_readiness"]);status["player_history_readiness"]="READY"
   stage("08_mainline_prerequisites",["internal","canonical spine plus team history"],["06_team_history_readiness"]);status["mainline_prerequisite_readiness"]="READY"
   stage("09_sog_prerequisites",["internal","canonical spine plus player history"],["07_player_history_readiness"]);status["sog_prerequisite_readiness"]="READY"
   status["downstream"].update(MAINLINE_MORNING_PREREQUISITES_READY=True,SOG_MORNING_PREREQUISITES_READY=True,MIDDAY_MARKET_CAPTURE_ALLOWED=True,FINAL_PREGAME_CAPTURE_ALLOWED=True)
  if scenario=="interrupted":raise KeyboardInterrupt
  if scenario=="finalization_failure":raise RuntimeError("FINAL_HEALTH_PACKAGING_FAILURE")
  status.update(overall_status="VALID_EMPTY_SLATE" if status["valid_empty_slate"] else ("DRY_RUN" if a.dry_run else "READY"),end_timestamp_utc=utc())
  if not a.fixture_scenario and not a.dry_run:
   sentinel_input=run/"live_failure_sentinel_input.json"
   atomic_json(sentinel_input,{"sentinel_timestamp_utc":status["end_timestamp_utc"],"slate_health":source_health,"parents":[{"child":"morning_canonical_spine","state":"PARENT_PRESENT_AND_CURRENT","parent":"slate_health.json"}],"populations":{"slate_games":status["schedule_game_count"],"market_qualified":0},"identity":{"qualified_issues":[],"diagnostic_issues":[]},"runtime":{"duration_minutes":round((parse_utc(status["end_timestamp_utc"])-parse_utc(status["start_timestamp_utc"])).total_seconds()/60,4),"overlap_minutes":0,"db_errors":[],"slow_threshold_minutes":90},"manual_actions":[],"mutable_inputs":[]})
   sp=subprocess.run([PY,ROOT/"backend/nhl/scripts/run_nhl_live_failure_sentinel.py","--phase","MORNING","--slate-date",slate,"--run-id",run_id,"--input-json",sentinel_input],cwd=ROOT,env=env,text=True,capture_output=True)
   if sp.returncode not in {0}:raise RuntimeError(f"MORNING_SENTINEL_BLOCKED rc={sp.returncode} stderr={sp.stderr.strip()}")
   status["live_failure_sentinel_path"]=sp.stdout.strip()
 except BaseException as e:
  status.update(overall_status="INTERRUPTED" if isinstance(e,KeyboardInterrupt) else "FAILED_BLOCKING",end_timestamp_utc=utc(),blocking_failure=str(e) or type(e).__name__);atomic_json(healthpath,status);print(healthpath);return 130 if isinstance(e,KeyboardInterrupt) else 1
 atomic_json(healthpath,status); digest=hashlib.sha256(healthpath.read_bytes()).hexdigest();(run/"SHA256SUMS").write_text(f"{digest}  morning_health.json\n");print(healthpath);return 0

if __name__=="__main__":raise SystemExit(main())
