#!/usr/bin/env python3
"""Create forward-only semantic registrations for the exact currently loaded models."""
from __future__ import annotations
import argparse, json, subprocess
from datetime import datetime,timezone
from pathlib import Path
import joblib
from backend.mlb.shared import semantic_model_registry as sr

PROPS=("hits","total_bases","strikeouts_pitching")
def qname(x): return type(x).__module__+"."+type(x).__qualname__
def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--config-root",type=Path,default=Path("backend/mlb/config/semantic_models")); ap.add_argument("--output-dir",type=Path,required=True); a=ap.parse_args()
 if a.output_dir.exists(): raise FileExistsError(a.output_dir)
 a.output_dir.mkdir(parents=True); a.config_root.mkdir(parents=True,exist_ok=True)
 commit=subprocess.run(["git","rev-parse","HEAD"],check=True,text=True,capture_output=True).stdout.strip(); dirty=bool(subprocess.run(["git","status","--porcelain"],check=True,text=True,capture_output=True).stdout)
 now=datetime.now(timezone.utc).isoformat(); version="registry_"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"); manifests=a.config_root/"manifests"; manifests.mkdir(parents=True,exist_ok=True); config=sr.effective_inference_config(); config_sha=sr.hash_value(config); entries=[]; statuses=[]
 for prop in PROPS:
  path=Path(config["model_dir"])/"latest"/f"{prop}.joblib"; artifact_sha=sr.hash_file(path); obj=joblib.load(path); meta=obj.get("meta",{}); order=list(meta.get("input_columns") or []); lr=obj.get("lr"); rf=obj.get("rf"); lrclf=getattr(lr,"named_steps",{}).get("clf") if lr is not None else None
  sid=f"MLB_{prop.upper()}_SEMANTIC_V1_{artifact_sha[:12]}"; embedded=[]
  if lrclf is not None and qname(lrclf).endswith("CalibratedClassifierCV"): embedded.append({"component":"lr.clf","class":qname(lrclf),"binding":"loaded_model_artifact_sha256"})
  cal_body={"mode":"DETERMINISTIC_CODE_CALIBRATION","function":"backend.mlb.prediction.make_prediction._apply_line_sensitivity","producing_code_git_commit":commit,"configuration_sha256":config_sha,"embedded_calibration_components":embedded,"external_calibration":{"enabled":False,"artifact_path":"","artifact_sha256":""},"identity_definition":"SHA256 of this calibration identity body excluding identity_sha256"}; cal={**cal_body,"identity_sha256":sr.hash_value(cal_body)}
  payload={"semantic_model_id":sid,"identity_scope":"PROSPECTIVE_ONLY_NOT_HISTORICALLY_RECOVERED","effective_from_timestamp":now,"proposition":prop,"loaded_model_artifact_path":str(path.resolve()),"loaded_artifact_sha256":artifact_sha,"estimator_class":{"lr":qname(lr),"rf":qname(rf),"best":qname(obj.get('best'))},"embedded_preprocessing":{"lr":qname(lr.named_steps['pre']),"rf":qname(rf.named_steps['pre'])},"inference_code_path":["backend/app/services/model_registry.py:load_model","backend/mlb/prediction/make_prediction.py:predict","backend/mlb/scripts/build_mlb_predictions_wide.py:_predict_rows"],"producing_code_git_commit":commit,"dirty_working_tree_status":"DIRTY" if dirty else "CLEAN","feature_schema_sha256":sr.hash_value(order),"required_feature_order":order,"configuration":config,"configuration_sha256":config_sha,"probability_orientation_contract":"probability_over=P(actual_value>line); probability_under=1-probability_over; selected_probability=P(selected_side wins)","proposition_contract_version":"mlb_two_sided_player_prop_v1","calibration_mode":"DETERMINISTIC_CODE_CALIBRATION","calibration_identity":cal,"calibration_artifact_path":"","calibration_artifact_sha256":""}
  doc={"registration_payload":payload,"semantic_registration_manifest_sha256":sr.hash_value(payload),"hash_definition":"SHA256 of canonical registration_payload; avoids self-referential whole-document hashing"}; mp=manifests/f"{sid}.json"; mp.write_text(json.dumps(doc,indent=2)+"\n"); rel=Path("manifests")/mp.name; entries.append({"proposition":prop,"semantic_model_id":sid,"semantic_manifest_path":str(rel),"semantic_manifest_sha256":sr.hash_file(mp),"active_artifact_sha256":artifact_sha,"effective_from_timestamp":now}); statuses.append({"proposition":prop,"semantic_model_id":sid,"semantic_status":"REGISTERED","calibration_identity_status":"RESOLVED_DETERMINISTIC_CODE_CALIBRATION","active_artifact_sha256":artifact_sha})
 reg={"registry_id":version,"immutable":True,"created_at":now,"prospective_only":True,"entries":entries}; rp=a.config_root/f"{version}.json"; rp.write_text(json.dumps(reg,indent=2)+"\n"); pointer={"active_registry_path":rp.name,"active_registry_sha256":sr.hash_file(rp),"activated_at":now}; (a.config_root/"active_registry.json").write_text(json.dumps(pointer,indent=2)+"\n")
 (a.output_dir/"semantic_registration_status.json").write_text(json.dumps({"decision":"PROSPECTIVE_SEMANTIC_MODELS_REGISTERED_CAPTURE_READY","statuses":statuses,"registry_path":str(rp),"active_pointer":str(a.config_root/'active_registry.json')},indent=2)+"\n")
 (a.output_dir/"current_model_registry.json").write_bytes(rp.read_bytes()); (a.output_dir/"active_registry_pointer.json").write_bytes((a.config_root/"active_registry.json").read_bytes())
 print(json.dumps(statuses,indent=2))
if __name__=="__main__": main()
