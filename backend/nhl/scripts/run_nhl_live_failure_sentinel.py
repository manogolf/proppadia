#!/usr/bin/env python3
"""Run-bound NHL live-failure sentinels. Observability only; no model decisions."""
from __future__ import annotations
import argparse,hashlib,json,math
from datetime import date,datetime,timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
DEFAULT_ROOT=ROOT/"artifacts/operational/nhl/sentinels"
BLOCKING={"STALE_BLOCKING","PARENT_PRESENT_BUT_STALE","PARENT_MISSING","PARENT_HASH_MISMATCH","PARENT_IDENTITY_AMBIGUOUS","ORIENTATION_MISMATCH","IDENTITY_CRITICAL","POST_START_CONTAMINATION","MISGRADING_DETECTED","WRONG_SEASON","WRONG_GAME_TYPE","PARTIAL_SLATE","MUTABLE_INPUT_USED_UNSAFE"}

def parse(v):return datetime.fromisoformat(str(v).replace("Z","+00:00")) if v else None
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def pct(a,b):return round(100*a/b,4) if b else 0.0
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--phase",choices=["MORNING","MIDDAY","FINAL_PREGAME","GRADING"],required=True);ap.add_argument("--slate-date",required=True);ap.add_argument("--run-id",required=True);ap.add_argument("--input-json",type=Path,required=True);ap.add_argument("--prior-run-json",type=Path);ap.add_argument("--output-root",type=Path,default=DEFAULT_ROOT);a=ap.parse_args()
 season=int(a.slate_date[:4]) if int(a.slate_date[5:7])>=7 else int(a.slate_date[:4])-1
 if season!=2026:raise SystemExit("WRONG_CANONICAL_SEASON")
 src=json.loads(a.input_json.read_text());prior=json.loads(a.prior_run_json.read_text()) if a.prior_run_json else {};dest=a.output_root/a.slate_date/a.run_id
 if dest.exists():raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
 dest.mkdir(parents=True);now=parse(src.get("sentinel_timestamp_utc")) or datetime.now(timezone.utc);results=[]
 def add(name,state,severity,evidence,blocking=None):results.append({"sentinel":name,"state":state,"severity":severity,"blocking":state in BLOCKING if blocking is None else blocking,"evidence":evidence})
 # Freshness and slate authority.
 health=src.get("slate_health",{});ft=parse(health.get("fetch_timestamp_utc"));lag=(now-ft).total_seconds()/3600 if ft else None
 fresh="FRESH" if lag is not None and lag<=6 else "STALE_BUT_BOUNDED" if lag is not None and lag<=18 else "STALE_BLOCKING" if lag is not None else "UNKNOWN"
 add("schedule_freshness",fresh,"CRITICAL" if fresh=="STALE_BLOCKING" else "WARNING" if fresh!="FRESH" else "INFO",{"latest":health.get("fetch_timestamp_utc"),"expected":"within 6h","lag_hours":lag})
 completion=health.get("completion_status");add("slate_completion","PARTIAL_SLATE" if completion not in {"READY","VALID_EMPTY_SLATE"} else completion,"CRITICAL",{"completion_status":completion,"downstream_ready":health.get("downstream_ready")})
 for key in ["team_history","feature_source","player_game_logs","sog_history","toi_history","roster_identity"]:
  x=src.get("freshness",{}).get(key,{});latest=x.get("latest_date");expected=x.get("expected_date");state="UNKNOWN" if not latest or not expected else "FRESH" if latest>=expected else "STALE_BUT_BOUNDED" if (date.fromisoformat(expected)-date.fromisoformat(latest)).days<=1 else "STALE_BLOCKING"
  if completion=="VALID_EMPTY_SLATE":state="NOT_APPLICABLE"
  add(f"{key}_freshness",state,"CRITICAL" if state=="STALE_BLOCKING" else "WARNING" if state not in {"FRESH","NOT_APPLICABLE"} else "INFO",{"latest_date":latest,"expected_date":expected,"lag_days":None if not latest or not expected else (date.fromisoformat(expected)-date.fromisoformat(latest)).days},blocking=state=="STALE_BLOCKING")
 # Parent continuity.
 for p in src.get("parents",[]):
  state=p.get("state","PARENT_MISSING");add(f"parent:{p.get('child','unknown')}",state,"CRITICAL" if state!="PARENT_PRESENT_AND_CURRENT" else "INFO",p)
 # Market coverage and late movement (not required in morning).
 market=src.get("market",{});den=int(market.get("eligible",0));covered=int(market.get("covered",0));qual=int(market.get("qualified",0));coverage=pct(covered,den)
 if a.phase=="MORNING":mstate="NOT_APPLICABLE"
 elif den and qual:mstate="MARKET_COVERAGE_PRESENT" if coverage>=50 else "MARKET_COVERAGE_LOW"
 else:mstate="NO_USABLE_MARKETS"
 add("market_coverage",mstate,"WARNING" if mstate in {"MARKET_COVERAGE_LOW","NO_USABLE_MARKETS"} else "INFO",{"eligible":den,"covered":covered,"qualified":qual,"coverage_pct":coverage,"missing_reasons":market.get("missing_reasons",{})},blocking=False)
 add("quote_timing","POST_START_CONTAMINATION" if market.get("post_start_quotes",0) else "PASS","CRITICAL",{"post_start_quotes":market.get("post_start_quotes",0)})
 prevm=prior.get("market",{});prev_ids=set(map(str,prevm.get("identities",[])));ids=set(map(str,market.get("identities",[])));late=[]
 if a.prior_run_json:
  for x in sorted(ids-prev_ids):late.append({"identity":x,"state":"MARKET_POSTED_LATE"})
  for x in sorted(prev_ids-ids):late.append({"identity":x,"state":"MARKET_DISAPPEARED"})
 late.extend(market.get("changes",[]));add("late_market",("NO_CHANGE" if not late else "MARKET_CHANGE_OBSERVED"),"WARNING" if late else "INFO",{"changes":late},blocking=False)
 # Independent orientation.
 orient=[]
 for r in src.get("probabilities",[]):
  if r.get("lane")=="MAINLINE":
   ok=math.isclose(float(r["p_home"])+float(r["p_away"]),1,abs_tol=1e-9) and r.get("selected_side")==('HOME' if float(r["p_home"])>=float(r["p_away"]) else 'AWAY') and r.get("home_team")==r.get("bound_home_team") and r.get("away_team")==r.get("bound_away_team")
  else:ok=math.isclose(float(r["p_over"])+float(r["p_under"]),1,abs_tol=1e-9) and r.get("selected_side")==('OVER' if float(r["p_over"])>=float(r["p_under"]) else 'UNDER') and r.get("line")==r.get("calculation_line")
  if not ok:orient.append(r.get("identity"))
 add("orientation","PASS" if not orient else "ORIENTATION_MISMATCH","CRITICAL",{"mismatch_identities":orient})
 # Identity, population and coverage usefulness.
 identity=src.get("identity",{});qualified_issues=identity.get("qualified_issues",[]);diagnostic=identity.get("diagnostic_issues",[]);add("identity_loss","IDENTITY_CRITICAL" if qualified_issues else "DIAGNOSTIC_IDENTITY_ISSUES" if diagnostic else "PASS","CRITICAL" if qualified_issues else "WARNING" if diagnostic else "INFO",{"qualified":qualified_issues,"diagnostic":diagnostic})
 pops=src.get("populations",{});add("population_denominators","POPULATION_COLLAPSE" if pops.get("unexpected_collapse") else "VISIBLE", "WARNING" if pops.get("unexpected_collapse") else "INFO",pops,blocking=False)
 pipeline_failed=any(x["blocking"] for x in results);useful=int(pops.get("market_qualified",qual))
 usefulness="PIPELINE_FAILED" if pipeline_failed else "PIPELINE_HEALTHY_NO_USABLE_MARKETS" if a.phase!="MORNING" and useful==0 else "PIPELINE_HEALTHY_COVERAGE_LOW" if pops.get("unexpected_collapse") or (den and coverage<50) else "PIPELINE_HEALTHY_COVERAGE_NORMAL"
 add("coverage_usefulness",usefulness,"WARNING" if "LOW" in usefulness or "NO_USABLE" in usefulness else "CRITICAL" if usefulness=="PIPELINE_FAILED" else "INFO",{"usable":useful},blocking=False)
 # Outcomes/nonparticipants.
 outcomes=src.get("outcomes",[]);bad=[];graded=0
 for x in outcomes:
  state=x.get("state");valid=(state=="NONPARTICIPANT" and x.get("settlement")=="UNGRADED") or (state=="POSTPONED" and x.get("settlement")=="UNGRADED") or (state=="MISSING" and x.get("settlement")=="UNGRADED") or state in {"WIN","LOSS","PUSH"}
  if not valid:bad.append(x.get("identity"))
  if state in {"WIN","LOSS","PUSH"}:graded+=1
 add("outcome_nonparticipant","PASS" if not bad else "MISGRADING_DETECTED","CRITICAL",{"invalid":bad,"graded":graded,"total":len(outcomes),"completeness_pct":pct(graded,len(outcomes))})
 # Schedule evolution.
 changes=src.get("schedule_changes",[]);conflicts=[x for x in changes if x.get("state") in {"IDENTITY_CONFLICT","WRONG_GAME_TYPE","WRONG_SEASON"}];add("schedule_change","IDENTITY_CRITICAL" if conflicts else "CHANGE_OBSERVED" if changes else "UNCHANGED","CRITICAL" if conflicts else "WARNING" if changes else "INFO",{"changes":changes})
 # Runtime overlap.
 runtime=src.get("runtime",{});errors=runtime.get("db_errors",[]);overlap=runtime.get("overlap_minutes",0);rstate="RESOURCE_CONTENTION_CONFIRMED" if errors else "RESOURCE_CONTENTION_SUSPECTED" if runtime.get("duration_minutes",0)>runtime.get("slow_threshold_minutes",90) else "BOUNDED_OVERLAP_NO_ERROR" if overlap else "NO_OVERLAP"
 add("runtime_overlap",rstate,"CRITICAL" if errors else "WARNING" if rstate!="NO_OVERLAP" else "INFO",runtime,blocking=bool(errors))
 # Manual and mutable-path lineage.
 manual=src.get("manual_actions",[]);add("manual_intervention","YES" if manual else "NO","WARNING" if manual else "INFO",{"actions":manual,"MANUAL_INTERVENTION_OCCURRED":"YES" if manual else "NO"},blocking=False)
 mutable=src.get("mutable_inputs",[]);unsafe=[x for x in mutable if x.get("used_for_critical_decision") and not x.get("documented_snapshot_binding")];bounded=[x for x in mutable if x.get("used") and x not in unsafe];mstate="MUTABLE_INPUT_USED_UNSAFE" if unsafe else "MUTABLE_INPUT_USED_BOUNDED" if bounded else "MUTABLE_INPUT_PRESENT_BUT_NOT_USED" if mutable else "RUN_BOUND_ONLY";add("mutable_path",mstate,"CRITICAL" if unsafe else "WARNING" if bounded else "INFO",{"unsafe":unsafe,"bounded":bounded})
 # Descriptive historical/live comparison only.
 hist=src.get("historical_expectation",{});sample=int(hist.get("live_sample",0));shift=bool(hist.get("material_shift"));hstate="INSUFFICIENT_LIVE_SAMPLE" if sample<int(hist.get("minimum_sample",20)) else "MATERIAL_DISTRIBUTION_SHIFT" if shift else "WITHIN_HISTORICAL_RANGE";add("historical_live_expectation",hstate,"WARNING" if hstate!="WITHIN_HISTORICAL_RANGE" else "INFO",hist,blocking=False)
 blockers=[x for x in results if x["blocking"]];warnings=[x for x in results if x["severity"]=="WARNING"]
 overall="RED" if blockers else "YELLOW" if warnings else "GREEN";payload={"schema_version":"nhl_live_failure_sentinel_v1","canonical_season":2026,"slate_date":a.slate_date,"run_id":a.run_id,"phase":a.phase,"sentinel_timestamp_utc":now.isoformat().replace("+00:00","Z"),"overall_status":overall,"blocking_reasons":[x["sentinel"]+":"+x["state"] for x in blockers],"bounded_reasons":[x["sentinel"]+":"+x["state"] for x in warnings],"sentinels":results,"market":market,"source_input_sha256":sha(a.input_json)}
 jp=dest/"nhl_live_failure_sentinel.json";jp.write_text(json.dumps(payload,indent=2,sort_keys=True)+"\n");(dest/"nhl_live_failure_sentinel.md").write_text(f"# NHL live-failure sentinel — {a.phase}\n\nOverall: `{overall}`\n\nBlocking: {', '.join(payload['blocking_reasons']) or 'none'}\n\nBounded: {', '.join(payload['bounded_reasons']) or 'none'}\n")
 files=[jp,dest/"nhl_live_failure_sentinel.md"];(dest/"SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files));print(dest);return 2 if overall=="RED" else 0
if __name__=="__main__":raise SystemExit(main())
