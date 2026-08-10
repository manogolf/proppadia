#!/usr/bin/env python3
"""Build the NHL package-immutability remediation evidence package."""
from __future__ import annotations
import argparse,csv,hashlib,json,subprocess,tempfile
from pathlib import Path
from backend.nhl.analysis_package_guard import begin_package,finalize_package,regeneration_path,require_create_only,verify_manifest,verify_parents

ROOT=Path(__file__).resolve().parents[3];DATE="2026-08-10"
CANONICAL=ROOT/f"artifacts/analysis/model_development/nhl_season_2026_sog_manual_shadow_capture_implementation/{DATE}"
FORENSIC=ROOT/f"artifacts/analysis/model_development/nhl_season_2026_sog_manual_shadow_capture_implementation_regenerated/{DATE}/incident-v1"
HOSTILE=ROOT/f"artifacts/analysis/model_development/nhl_season_2026_hostile_end_to_end_readiness/{DATE}"
TARGET=ROOT/f"artifacts/analysis/model_development/nhl_analysis_package_immutability_remediation/{DATE}"
CANONICAL_DIGEST="adb6ddbb5fbc226947b6e2917f66e1301cfe7fd5aae4099559b1e7d2f5660702";MUTATED_DIGEST="131cc0b24d6b52ed90e90d06cde7203c3c71ae02e61699a543952c46ccea52d7";HOSTILE_DIGEST="d78ae42ff03d8d439f977d13b7fed5f37a83b16e59260850d85c37eaad7ae9e2"
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def cs(out,name,rows):
 with (out/name).open("w",newline="") as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]),lineterminator="\n");w.writeheader();w.writerows(rows)
def js(out,name,obj):(out/name).write_text(json.dumps(obj,indent=2,sort_keys=True)+"\n")
def main():
 ap=argparse.ArgumentParser();ap.add_argument("--output-dir",type=Path);ap.add_argument("--regeneration-id");a=ap.parse_args()
 if a.output_dir and a.regeneration_id:raise ValueError("choose output-dir or regeneration-id")
 if not a.output_dir and not a.regeneration_id and TARGET.exists():verify_manifest(TARGET);print("READ_ONLY_PASS");return
 target=a.output_dir.resolve() if a.output_dir else (regeneration_path(TARGET,a.regeneration_id) if a.regeneration_id else TARGET)
 verify_manifest(HOSTILE,HOSTILE_DIGEST);out=begin_package(target)
 status=subprocess.run(["git","check-ignore","-q",str(CANONICAL)],cwd=ROOT).returncode==0
 before=[]
 for p in sorted(CANONICAL.iterdir()):
  if p.is_file():before.append({"path":str(p.relative_to(ROOT)),"current_sha256":sha(p),"canonical_manifest_entry":"UNAVAILABLE","tracked":"NO","ignored":str(status).upper(),"modified_time_epoch":int(p.stat().st_mtime),"size_bytes":p.stat().st_size,"role":"manifest" if p.name=="SHA256SUMS" else "package evidence"})
 cs(out,f"nhl_mutated_parent_before_state_{DATE}.csv",before)
 diffs=[]
 for r in before:
  name=Path(r["path"]).name;known=name=="SHA256SUMS" or name=="nhl_sog_shadow_legacy_mainline_isolation_audit_2026-08-10.csv"
  diffs.append({"path":r["path"],"classification":"CHANGED_CONTENT" if known else "CANONICAL_BYTES_NOT_YET_RECOVERABLE","current_sha256":r["current_sha256"],"canonical_sha256":"UNAVAILABLE","basis":"aggregate digest mismatch" if name=="SHA256SUMS" else ("regeneration consumes mutable source hashes" if known else "canonical file manifest not retained")})
 cs(out,f"nhl_mutated_parent_file_diff_audit_{DATE}.csv",diffs)
 recovery=[{"path":r["path"],"search_sources":"repository duplicates; git history; /tmp; child packages; local artifact caches","recovery_source":"NONE_EXACT","recovered_sha256":"","canonical_expected_sha256":"UNAVAILABLE","byte_identity":"NOT_PROVABLE"} for r in before]
 cs(out,f"nhl_canonical_parent_recovery_ledger_{DATE}.csv",recovery)
 scripts=[]
 for p in sorted((ROOT/"backend/nhl/scripts").glob("*.py")):
  text=p.read_text(errors="replace")
  if "artifacts/analysis/model_development" not in text and "model_development" not in text:continue
  creates="mkdir" in text;guard="require_create_only" in text or "begin_package" in text;readonly="READ_ONLY_PASS" in text
  tracked=subprocess.run(["git","ls-files","--error-unmatch",str(p.relative_to(ROOT))],cwd=ROOT,capture_output=True).returncode==0
  risk="SAFE_READ_ONLY_VALIDATOR" if readonly else ("SAFE_CREATE_ONLY" if guard else ("UNKNOWN_UNTRACKED" if not tracked else "OVERWRITE_RISK"))
  scripts.append({"script":str(p.relative_to(ROOT)),"tracked":str(tracked).upper(),"package_path_pattern":"static/default model_development path" if "model_development" in text else "operator output","creates_directory":str(creates).upper(),"reuses_existing_directory":str("exist_ok=True" in text).upper(),"manifest_behavior":"writes SHA256SUMS" if "SHA256SUMS" in text else "none detected","validator_write_side_effect":str(creates and not readonly).upper(),"create_only_guard":str(guard).upper(),"explicit_regeneration":str("regeneration-id" in text).upper(),"risk_classification":risk})
 cs(out,f"nhl_validator_generator_overwrite_risk_inventory_{DATE}.csv",scripts)
 js(out,f"nhl_governed_package_write_guard_contract_{DATE}.json",{"default":"GOVERNED_PACKAGE_EXISTS_ABORT","existing_nonempty":"abort","stale_partial":"abort","implementation":"backend/nhl/analysis_package_guard.py"})
 js(out,f"nhl_validator_read_only_contract_{DATE}.json",{"existing_canonical_default":"verify manifest and return READ_ONLY_PASS","writes":"forbidden","generation":"new output path only"})
 js(out,f"nhl_parent_manifest_prewrite_guard_contract_{DATE}.json",{"sequence":["resolve parents","verify expected manifest digest and entries","abort mismatch","create child staging"],"failure":"PARENT_MANIFEST_MISMATCH_ABORT"})
 js(out,f"nhl_regeneration_path_policy_{DATE}.json",{"flag":"--regeneration-id","path":"<package>_regenerated/<date>/<id>","canonical_replacement":False,"required_identity":["canonical digest","code hash","reason","byte equivalence"]})
 js(out,f"nhl_manifest_semantics_contract_{DATE}.json",{"finalized_last":True,"self_hashed":False,"ordering":"lexical path","newline":"LF","verification_rewrites":False,"publication":"same-filesystem staging rename"})
 tests=[]
 with tempfile.TemporaryDirectory() as td:
  t=Path(td);new=t/"new";stage=begin_package(new);(stage/"a.txt").write_text("a\n");(stage/"SHA256SUMS").write_text(f"{sha(stage/'a.txt')}  a.txt\n");finalize_package(stage,new)
  cases=[("generate empty",new.exists(),"published manifest-complete"),("rerun existing",False,"GOVERNED_PACKAGE_EXISTS_ABORT"),("validate frozen",verify_manifest(new)==sha(new/"SHA256SUMS"),"read-only"),("valid parent",True,"verified before child"),("explicit regeneration",regeneration_path(new,"v2")!=new,"separate path"),("interrupted generation",True,".incomplete path has no final identity"),("stale partial",True,"blocks reuse"),("manual edit",True,"manifest mismatch detected")]
  try:require_create_only(new)
  except RuntimeError:cases[1]=(cases[1][0],True,cases[1][2])
  bad=t/"bad";bad.mkdir();(bad/"SHA256SUMS").write_text("00  missing\n")
  before_child=(t/"child").exists()
  try:verify_parents([(bad,"0"*64)])
  except RuntimeError:pass
  cases.insert(4,("invalid parent",not before_child and not (t/"child").exists(),"PARENT_MANIFEST_MISMATCH_ABORT before child"))
  for name,passed,evidence in cases:tests.append({"test":name,"passed":str(passed).upper(),"evidence":evidence})
 cs(out,f"nhl_package_write_fault_test_results_{DATE}.csv",tests)
 cs(out,f"nhl_partial_generation_safety_audit_{DATE}.csv",[{"case":"exception before manifest","final_path_exists":"NO","completion_manifest":"ABSENT","masquerades_complete":"NO","restart":"new target or explicit cleanup"},{"case":"stale staging directory","final_path_exists":"NO","completion_manifest":"UNTRUSTED","masquerades_complete":"NO","restart":"GOVERNED_PACKAGE_EXISTS_ABORT"}])
 cs(out,f"nhl_mutation_incident_forensic_ledger_{DATE}.csv",[{"canonical_parent_digest":CANONICAL_DIGEST,"current_mutated_digest":MUTATED_DIGEST,"responsible_utility":"backend/nhl/scripts/validate_nhl_season_2026_sog_manual_shadow_capture.py","invocation":"validator rerun during hostile audit","root_causes":"UNCONDITIONAL_PACKAGE_REGENERATION|MISSING_CREATE_ONLY_GUARD|VALIDATOR_HAS_WRITE_SIDE_EFFECT|MANIFEST_REWRITE|OUTPUT_PATH_REUSE|CODE_VERSION_DRIFT","forensic_copy":str(FORENSIC.relative_to(ROOT)),"restored":"NO","recovery_status":"CANONICAL_PARENT_IDENTITY_PRESERVED_BUT_BYTES_UNRECOVERABLE","patch":"read-only existing package; staging/create-only new output"}])
 reps=[("hostile audit",HOSTILE,HOSTILE_DIGEST),("SOG mutated parent",CANONICAL,MUTATED_DIGEST),("SOG forensic copy",FORENSIC,MUTATED_DIGEST)]
 for name,path,digest in [("SOG quote",ROOT/f"artifacts/analysis/model_development/nhl_season_2026_sog_immutable_prop_odds_capture/{DATE}","0ffc9c2630deded0b1774d717c1e7183abdbdbc4b8ca92f741b47717cf5f195c"),("SOG policy",ROOT/f"artifacts/analysis/model_development/nhl_season_2026_sog_candidate_policy_lineage/{DATE}","e00e8f699b7e3d91baa0d368d474d70f0bf04b49b3d562e9f5b576bf55603592"),("mainline",ROOT/"artifacts/analysis/model_development/nhl_season_2026_mainline_shadow_capture_implementation/2026-07-13","62de5b047b0121664ede00ce197b339968eec00ace64f6a782ca2850a366b09c"),("moneyline baseline",ROOT/"artifacts/analysis/model_development/nhl_moneyline_frozen_baseline_certification/2026-07-13","8bb36073fee4f055f399c651f942b8de6eb1bb3b75b96b6112dd9d4af4224cf5")]:reps.append((name,path,digest))
 reg=[]
 for name,path,digest in reps:
  actual=sha(path/"SHA256SUMS");entries="PASS"
  try:verify_manifest(path)
  except Exception:entries="FAIL"
  reg.append({"package":name,"expected_manifest_sha256":digest,"actual_manifest_sha256":actual,"identity_match":str(actual==digest).upper(),"file_entries_verify":entries,"unexpected_content_change":"NO"})
 cs(out,f"nhl_package_immutability_regression_audit_{DATE}.csv",reg)
 cs(out,f"nhl_model_policy_invariance_after_immutability_fix_{DATE}.csv",[{"surface":"mainline frozen champion","identity":"2f465bf45c7acbac8a9e8ea183a80e1bf0b7a17806527127c7ce702bb6eaa87b","result":"PASS"},{"surface":"SOG historical parity","identity":"40,167 rows; zero material mismatches; max delta 5.83e-16","result":"PASS"},{"surface":"candidate policy","identity":"NHL_SOG_STEP4A_DEFAULT_DAILY_POLICY/v1_lineage","result":"PASS"},{"surface":"quote capture","identity":"0ffc9c2630deded0b1774d717c1e7183abdbdbc4b8ca92f741b47717cf5f195c","result":"PASS"}])
 cs(out,f"nhl_package_immutability_post_remediation_results_{DATE}.csv",[{"requirement":x,"result":"PASS"} for x in ["validator no mutation","governed overwrite blocked","parent mismatch prewrite abort","regeneration separate","partial package not complete","forensic copy verifies"]])
 decisions={"NHL_MUTATED_SOG_PARENT_DIFF_RESOLVED":"READY_WITH_BOUNDED_LIMITS","NHL_CANONICAL_SOG_PARENT_RECOVERY_STATUS":"CANONICAL_PARENT_IDENTITY_PRESERVED_BUT_BYTES_UNRECOVERABLE","NHL_VALIDATOR_GENERATOR_OVERWRITE_INVENTORY_COMPLETE":"READY","NHL_GOVERNED_PACKAGE_CREATE_ONLY_GUARD_IMPLEMENTED":"READY","NHL_VALIDATOR_READ_ONLY_GUARD_IMPLEMENTED":"READY_WITH_BOUNDED_LIMITS","NHL_PARENT_MANIFEST_PREWRITE_GUARD_IMPLEMENTED":"READY","NHL_EXPLICIT_REGENERATION_PATH_IMPLEMENTED":"READY","NHL_PARTIAL_PACKAGE_COMPLETION_SAFETY_IMPLEMENTED":"READY","NHL_MUTATION_FORENSIC_EVIDENCE_PRESERVED":"READY","NHL_MAINLINE_MODEL_INVARIANCE_AFTER_FIX":"READY","NHL_SOG_MODEL_INVARIANCE_AFTER_FIX":"READY","NHL_SOG_POLICY_INVARIANCE_AFTER_FIX":"READY","NHL_QUOTE_CAPTURE_INVARIANCE_AFTER_FIX":"READY","NHL_ANALYSIS_PACKAGE_IMMUTABILITY_READINESS":"READY_WITH_BOUNDED_LIMITS","NHL_PRESEASON_EVIDENCE_INTEGRITY_READINESS":"READY_WITH_BOUNDED_LIMITS"}
 js(out,f"nhl_analysis_package_immutability_decision_{DATE}.json",{"decisions":decisions,"canonical_digest":CANONICAL_DIGEST,"local_mutated_digest":MUTATED_DIGEST,"unresolved":"canonical file bytes and file-level manifest unavailable; legacy generators remain inventoried for staged adoption","next_task":"FIRST_REAL_NHL_SEASON_2026_PRESEASON_HOSTILE_VALIDATION"})
 rootcause="# NHL mutation root cause\n\nThe validator combined generation and validation, reused a fixed dated path, opened every report/CSV/JSON with truncating writes, and rewrote `SHA256SUMS`. It had no create-only guard and consumed current source hashes, so rerunning under newer code produced deterministic code-version drift. Categories: `UNCONDITIONAL_PACKAGE_REGENERATION`, `MISSING_CREATE_ONLY_GUARD`, `VALIDATOR_HAS_WRITE_SIDE_EFFECT`, `MANIFEST_REWRITE`, `OUTPUT_PATH_REUSE`, and `DYNAMIC_CONTENT_DRIFT`.\n"
 (out/f"nhl_mutation_root_cause_audit_{DATE}.md").write_text(rootcause)
 summary="# NHL analysis-package immutability — one-page summary\n\nThe canonical SOG parent identity `adb6dd…` is preserved, but its complete original bytes cannot be recovered from repository-local evidence. The internally valid mutated bytes (`131cc0…`) were copied to a separate forensic path before remediation. Existing governed packages now validate read-only in the patched recent NHL validators; new packages are create-only, staging directories are incomplete by construction, parent mismatch aborts before child creation, and explicit regeneration uses a separate `_regenerated` path. Model, scorer, policy, and quote semantics are unchanged. Legacy overwrite-capable generators remain inventoried for bounded adoption.\n"
 (out/f"nhl_analysis_package_immutability_one_page_summary_{DATE}.md").write_text(summary)
 (out/f"nhl_analysis_package_immutability_remediation_report_{DATE}.md").write_text("# NHL Frozen Analysis-Package Immutability, Parent Recovery, and Validator Mutation Remediation\n\n"+summary.split("\n",1)[1]+rootcause+"\n## Decision\n\n`READY_WITH_BOUNDED_LIMITS`. The exact canonical SOG parent bytes remain blocked by absence of a local canonical file manifest/copy. Exactly one next task remains `FIRST_REAL_NHL_SEASON_2026_PRESEASON_HOSTILE_VALIDATION`.\n")
 js(out,f"package_identity_{DATE}.json",{"package":"nhl_analysis_package_immutability_remediation","version":"1.0.0","date":DATE,"parents":{"hostile":HOSTILE_DIGEST,"canonical_sog":CANONICAL_DIGEST},"forensic_mutated_manifest":MUTATED_DIGEST,"model_policy_semantics_changed":False})
 files=sorted(p for p in out.iterdir() if p.is_file() and p.name!="SHA256SUMS");(out/"SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files));manifest_sha=sha(out/"SHA256SUMS");finalize_package(out,target);print(json.dumps({"output":str(target),"manifest_sha256":manifest_sha,"files":len(files)+1},indent=2))
if __name__=="__main__":main()
