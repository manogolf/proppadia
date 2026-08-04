#!/usr/bin/env python3
"""Initialize the no-backfill prospective MLB false-favorite package."""
from __future__ import annotations
import argparse, csv, hashlib, json
from pathlib import Path
from backend.mlb.shared.prospective_lineage import CONTRACT_VERSION, MANDATORY

def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--output-dir",type=Path,required=True); ap.add_argument("--refresh-empty",action="store_true"); a=ap.parse_args(); o=a.output_dir
 if o.exists() and not a.refresh_empty: raise FileExistsError(o)
 if o.exists():
  for ledger in ("append_only_prediction_ledger.csv","separate_outcome_ledger.csv"):
   p=o/ledger
   if p.exists() and len(p.read_text().splitlines()) > 1: raise RuntimeError(f"refusing to refresh non-empty ledger: {p}")
 o.mkdir(parents=True,exist_ok=True)
 contract={"contract_version":CONTRACT_VERSION,"emission_time":"prediction_time","actual_model_bytes_hashed":True,"exact_feature_vector_canonical_serialization_preserved":True,"two_sided_same_snapshot_required":True,"outcomes_forbidden_in_prediction_ledger":True,"append_only":True,"historical_backfill_forbidden":True,"mandatory_fields":list(MANDATORY),"validator_statuses":["LINEAGE_CERTIFIED","LINEAGE_BLOCKED_MISSING_MODEL_HASH","LINEAGE_BLOCKED_MISSING_FEATURE_HASH","LINEAGE_BLOCKED_MISSING_CALIBRATION_IDENTITY","LINEAGE_BLOCKED_MISSING_CONFIG_HASH","LINEAGE_BLOCKED_ODDS_PAIR","LINEAGE_BLOCKED_TIMESTAMP","LINEAGE_BLOCKED_IDENTITY","LINEAGE_BLOCKED_OTHER_MANDATORY_FIELD"]}
 (o/"semantic_lineage_contract.json").write_text(json.dumps(contract,indent=2)+"\n")
 (o/"prediction_time_metadata_schema.json").write_text(json.dumps({"type":"object","required":list(MANDATORY),"outcome_fields_permitted":False,"canonical_identity_required":["game_date","game_id","player_id","prop_type","line","selected_side","bookmaker_key","snapshot_run_tag"]},indent=2)+"\n")
 (o/"lineage_validator.md").write_text("# Fail-closed lineage validator\n\nImplemented in `backend/mlb/shared/prospective_lineage.py`. Validation is ordered and returns the first exact blocking status. `LINEAGE_CERTIFIED` requires all model, feature, calibration, configuration, odds-pair, timestamp, and canonical-identity fields.\n")
 pred_fields=list(MANDATORY)+["lineage_status","lineage_failure_detail","market_favorite","exact_price_break_even_probability","model_probability_over","direct_fallback_provenance","distribution_coherence_status","contributing_history_observation_status","opportunity_data_availability"]
 with (o/"append_only_prediction_ledger.csv").open("w",newline="") as f: csv.writer(f).writerow(pred_fields)
 with (o/"separate_outcome_ledger.csv").open("w",newline="") as f: csv.writer(f).writerow(["canonical_row_identity","grading_timestamp","outcome_status","actual_value","selected_side_outcome","pnl_1u","outcome_source","outcome_source_sha256"])
 hypotheses=[("model_probability_inflation","READY"),("model_probability_failing_to_improve_market","READY"),("excessive_favorite_price_burden","READY"),("direct_versus_fallback_provenance","READY"),("stale_or_incomplete_contributing_history","READY"),("adjacent_threshold_incoherence","READY"),("unstable_opportunity","PENDING_CERTIFIED_STRICT_PRIOR_DEFINITION"),("hostile_environment","PENDING_CERTIFIED_STRICT_PRIOR_DEFINITION")]
 with (o/"frozen_prospective_hypothesis_registry.csv").open("w",newline="") as f:
  w=csv.writer(f); w.writerow(["mechanism","observation_readiness","status","selector_authorized"]); [w.writerow([x,y,"PROSPECTIVE_CANDIDATE_ONLY",False]) for x,y in hypotheses]
 report={"decision":"PROSPECTIVE_LINEAGE_CONTRACT_IMPLEMENTED_NOT_YET_CAPTURED","implementation_files":["backend/mlb/shared/prospective_lineage.py","backend/mlb/scripts/build_mlb_predictions_wide.py"],"prediction_logic_changed":False,"selection_logic_changed":False,"probability_values_changed":False,"wager_behavior_changed":False,"real_prospective_rows_captured":0,"prediction_ledger_rows":0,"outcome_ledger_rows":0,"no_backfill":True,"validation":"unit tests exercise certification, missing-model fail closure, canonical hashing, and duplicate append rejection"}
 (o/"instrumentation_validation_report.json").write_text(json.dumps(report,indent=2)+"\n")
 files=sorted(x for x in o.iterdir() if x.name!="SHA256SUMS.csv")
 with (o/"SHA256SUMS.csv").open("w",newline="") as f:
  w=csv.writer(f); w.writerow(["file","sha256","bytes"]); [w.writerow([p.name,sha(p),p.stat().st_size]) for p in files]
 print(json.dumps(report,indent=2)); return 0
if __name__=="__main__": raise SystemExit(main())
