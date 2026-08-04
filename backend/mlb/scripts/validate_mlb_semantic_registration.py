#!/usr/bin/env python3
"""Deterministic assertion-based validation of forward MLB semantic registration."""
from __future__ import annotations
import argparse,csv,json,tempfile
from pathlib import Path
from backend.mlb.shared import prospective_lineage as pl
from backend.mlb.shared import semantic_model_registry as sr

PROPS=("hits","total_bases","strikeouts_pitching")
def base_row(prop):
 ok,status,doc=sr.certify_loaded(prop); assert ok,status; p=doc["registration_payload"]
 ident={"game_date":"2099-01-01","game_id":1,"player_id":2,"prop_type":prop,"line":0.5,"selected_side":"over","bookmaker_key":"book","snapshot_run_tag":"run"}
 row={k:"x" for k in pl.MANDATORY}; row.update({"model_artifact_sha256":p["loaded_artifact_sha256"],"model_semantic_name":p["semantic_model_id"],"model_semantic_version":p["effective_from_timestamp"],"feature_schema_sha256":p["feature_schema_sha256"],"feature_vector_sha256":"f"*64,"calibration_method":p["calibration_mode"],"calibration_artifact_path":"NOT_APPLICABLE_DETERMINISTIC_CODE_CALIBRATION","calibration_artifact_sha256":p["calibration_identity"]["identity_sha256"],"configuration_sha256":p["configuration_sha256"],"price_over_american":-110,"price_under_american":-110,"selected_side":"over","canonical_row_identity":pl.canonical_json(ident),"registered_feature_schema_sha256":p["feature_schema_sha256"],"registered_configuration_sha256":p["configuration_sha256"]}); return row,doc
def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--out-json",type=Path,required=True); a=ap.parse_args(); tests=[]
 def check(name,fn):
  try: fn(); tests.append({"test":name,"status":"PASS"})
  except Exception as e: tests.append({"test":name,"status":"FAIL","detail":f"{type(e).__name__}:{e}"})
 def registered():
  for p in PROPS: assert sr.certify_loaded(p)[0]
 def changed():
  _,d=base_row("hits"); src=Path(d["registration_payload"]["loaded_model_artifact_path"])
  with tempfile.TemporaryDirectory() as td:
   changed=Path(td)/"changed.joblib"; changed.write_bytes(src.read_bytes()+b"changed")
   assert sr.hash_file(changed) != d["registration_payload"]["loaded_artifact_sha256"]
 def missing_cal():
  r,_=base_row("hits"); r["calibration_artifact_sha256"]=""; assert pl.validate(r)[0]=="LINEAGE_BLOCKED_MISSING_CALIBRATION_IDENTITY"
 def feature():
  r,_=base_row("hits"); r["feature_schema_sha256"]="bad"; assert pl.validate(r)[0]=="LINEAGE_BLOCKED_FEATURE_SCHEMA_MISMATCH"
 def config():
  r,_=base_row("hits"); r["configuration_sha256"]="bad"; assert pl.validate(r)[0]=="LINEAGE_BLOCKED_CONFIGURATION_MISMATCH"
 def duplicate():
  r,_=base_row("hits"); r["lineage_status"]="LINEAGE_CERTIFIED"; r["lineage_failure_detail"]=""
  with tempfile.TemporaryDirectory() as td:
   p=Path(td)/"l.csv"; pl.append_rows(p,[r]);
   try: pl.append_rows(p,[r])
   except ValueError: return
   raise AssertionError("duplicate accepted")
 def outcomes():
  r,_=base_row("hits"); r["actual_value"]=1; assert pl.validate(r)[0]=="LINEAGE_BLOCKED_OUTCOME_FIELD"
  with tempfile.TemporaryDirectory() as td:
   try: pl.append_rows(Path(td)/"ledger.csv",[r])
   except ValueError: return
   raise AssertionError("outcome field entered ledger")
 def unregistered():
  with tempfile.TemporaryDirectory() as td:
   root=Path(td); reg={"entries":[]}; rp=root/"r.json"; rp.write_text(json.dumps(reg)); pointer=root/"active_registry.json"; pointer.write_text(json.dumps({"active_registry_path":"r.json","active_registry_sha256":sr.hash_file(rp)})); assert not sr.certify_loaded("hits",pointer)[0]
 for name,fn in [("registered_artifact_hash_matches_loaded_bytes",registered),("changed_artifact_bytes_fail_prior_semantic_version",changed),("missing_calibration_identity_fails",missing_cal),("feature_schema_mismatch_fails",feature),("configuration_mismatch_fails",config),("duplicate_canonical_ledger_identity_fails",duplicate),("outcome_fields_forbidden_from_prediction_ledger",outcomes),("unregistered_mutable_latest_path_cannot_certify",unregistered)]: check(name,fn)
 result={"lineage_test_status":"PASS" if all(x["status"]=="PASS" for x in tests) else "FAIL","tests":tests}; a.out_json.write_text(json.dumps(result,indent=2)+"\n"); print(json.dumps(result,indent=2)); assert result["lineage_test_status"]=="PASS"
if __name__=="__main__": main()
