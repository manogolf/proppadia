#!/usr/bin/env python3
"""Emit the mandatory model-identity gate for Hits 1.5 Under shadow setup."""
from __future__ import annotations
import hashlib, json
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/"artifacts/analysis/model_development/mlb_hits_15_under_prospective_shadow_v1/2026-08-14"
STAGE1=ROOT/"artifacts/analysis/model_development/mlb_hits_standalone_prediction_evidence_review_stage1/2026-08-14/frozen_hits_review_population.csv"
MODEL=ROOT/"models_out/latest/hits.joblib"
MANIFEST=ROOT/"backend/mlb/config/semantic_models/manifests/MLB_HITS_SEMANTIC_V1_2e7377b2cdcb.json"

def sha(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()
def main()->int:
 OUT.mkdir(parents=True,exist_ok=True)
 rows=pd.read_csv(STAGE1,low_memory=False); lane=rows[(rows.line==1.5)&(rows.side=="under")]
 manifest=json.loads(MANIFEST.read_text())["registration_payload"]
 identity={
  "task_id":"MLB_HITS_15_UNDER_PROSPECTIVE_SHADOW_V1",
  "gate_status":"HITS15_UNDER_PROSPECTIVE_CAPTURE_BLOCKED_MODEL_IDENTITY",
  "historical_lane_rows":len(lane),"historical_date_start":lane.game_date.min(),"historical_date_end":lane.game_date.max(),
  "historical_reconcile_artifacts":int(lane.prediction_artifact.nunique()),
  "historical_probability_field":"model_probability (derived from retained model_pick_prob)",
  "historical_identity_contract":"game_id + player_id + prop_type=hits + line=1.5 + side=under",
  "historical_model_hash_embedded":False,"historical_semantic_model_id_embedded":False,"historical_prediction_source_file_embedded":False,
  "currently_loadable_model":{"semantic_model_id":manifest["semantic_model_id"],"artifact":str(MODEL.relative_to(ROOT)),"artifact_sha256":sha(MODEL),"manifest_sha256":sha(MANIFEST),"identity_scope":manifest["identity_scope"],"effective_from_timestamp":manifest["effective_from_timestamp"],"producer":manifest["inference_code_path"],"feature_schema_sha256":manifest["feature_schema_sha256"],"probability_orientation_contract":manifest["probability_orientation_contract"]},
  "exact_historical_replay_proven":False,
  "blocker":"The current registered model is prospective-only and effective after the May 8-August 2 historical window; the 717 frozen rows do not bind their probabilities to exact model bytes or a semantic model identity.",
  "prohibited_inference":"Current hits.joblib bytes must not be assumed to have generated all historical rows.",
 }
 (OUT/"hits15_under_model_identity.json").write_text(json.dumps(identity,indent=2)+"\n")
 historical={"rows":717,"brier":0.217089,"log_loss":0.625357,"observed_rate":0.707,"mean_model_probability":0.609,"temporal_behavior":"STABLE","source":"MLB_HITS_LANE_SPECIFIC_PREDICTION_REVIEW_STAGE2","modified":False}
 (OUT/"hits15_under_historical_reference.json").write_text(json.dumps(historical,indent=2)+"\n")
 status={"status":"HITS15_UNDER_PROSPECTIVE_CAPTURE_BLOCKED_MODEL_IDENTITY","predictions_frozen":0,"resolved":0,"unresolved":0,"duplicates":0,"post_start_rejects":0,"market_attachments":0,"new_scheduler_required":False,"authority":"NO_QUALIFIED_MLB_PROP_MODEL","certification_status":"NOT_CERTIFIED_PROSPECTIVE_EVIDENCE_PENDING","stopped_after_part":1,"not_evaluated_due_to_gate":["August 14 BetOnline coverage","current model prediction coverage","prospective ledger contract","market attachment","grading integration"],"files_intentionally_not_created":["hits15_under_prediction_ledger.csv","hits15_under_market_attachment_ledger.csv","hits15_under_outcome_ledger.csv","hits15_under_prospective_contract.json","hits15_under_grading_contract.json"]}
 (OUT/"hits15_under_prospective_status.json").write_text(json.dumps(status,indent=2)+"\n")
 (OUT/"hits15_under_prospective_status.md").write_text("# Hits 1.5 Under prospective shadow status\n\n`HITS15_UNDER_PROSPECTIVE_CAPTURE_BLOCKED_MODEL_IDENTITY`\n\nThe mandatory model-identity gate failed. No prediction, market, outcome, contract, grading, scheduler, or workflow artifacts were initialized. Authority remains `NO_QUALIFIED_MLB_PROP_MODEL`; status remains `NOT_CERTIFIED_PROSPECTIVE_EVIDENCE_PENDING`.\n")
 products=sorted(p for p in OUT.iterdir() if p.name!="reproducibility_hashes.sha256"); (OUT/"reproducibility_hashes.sha256").write_text("".join(f"{sha(p)}  {p.name}\n" for p in products))
 print(json.dumps(status,indent=2)); return 0
if __name__=="__main__":raise SystemExit(main())
