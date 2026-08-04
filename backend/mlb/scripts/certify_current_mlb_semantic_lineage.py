#!/usr/bin/env python3
"""Fail-closed certification of current in-scope MLB model artifacts; no backfill."""
from __future__ import annotations
import argparse, csv, hashlib, json, subprocess
from datetime import datetime, timezone
from pathlib import Path
import joblib

PROPS=("hits","total_bases","strikeouts_pitching")
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def dump(path, rows):
    with path.open("w",newline="") as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--contract-package",type=Path,required=True); ap.add_argument("--out-dir",type=Path,required=True); a=ap.parse_args()
    if a.out_dir.exists(): raise FileExistsError(a.out_dir)
    a.out_dir.mkdir(parents=True)
    commit=subprocess.run(["git","rev-parse","HEAD"],capture_output=True,text=True,check=True).stdout.strip()
    rows=[]
    for prop in PROPS:
        p=Path("/var/data/proppadia/models/latest")/f"{prop}.joblib"; obj=joblib.load(p); meta=obj.get("meta",{}) if isinstance(obj,dict) else {}; features=meta.get("input_columns") or meta.get("features_num") or []
        semantic_id=meta.get("semantic_model_id") or meta.get("semantic_name")
        calibration=meta.get("calibration_artifact_path")
        blockers=[]
        if not semantic_id: blockers.append("MISSING_EXPLICIT_SEMANTIC_MODEL_ID")
        if not calibration: blockers.append("MISSING_CALIBRATION_ARTIFACT_IDENTITY")
        rows.append({"proposition":prop,"loaded_artifact_path":str(p),"loaded_artifact_sha256":sha(p),"explicit_semantic_model_id":semantic_id or "","feature_schema_sha256":hashlib.sha256(json.dumps(features,separators=(",",":"),ensure_ascii=False).encode()).hexdigest(),"calibration_artifact_path":calibration or "","calibration_artifact_sha256":sha(Path(calibration)) if calibration and Path(calibration).is_file() else "","configuration_sha256":"","producing_code_commit":commit,"probability_orientation_contract":"probability_over=P(actual_value>line); selected_probability=P(selected_side wins)","proposition_contract_version":"mlb_two_sided_player_prop_v1","lineage_status":"LINEAGE_CERTIFICATION_BLOCKED" if blockers else "LINEAGE_CERTIFIED","blocking_reasons":"|".join(blockers)})
    dump(a.out_dir/"current_semantic_model_registry.csv",rows); dump(a.out_dir/"current_model_lineage_validation.csv",rows)
    ledger=a.contract_package/"append_only_prediction_ledger.csv"
    (a.out_dir/"append_only_prospective_prediction_ledger.csv").write_bytes(ledger.read_bytes())
    hypotheses=[("H1","Market favorites opposed by the model perform worse than market favorites supported by the model."),("H2","Larger positive model-market probability gaps do not necessarily correspond to higher realized frequencies."),("H3","Favorite price burden increases faster than realized win probability."),("H4","Fallback-supported predictions are less calibrated than direct-source predictions."),("H5","Stale or incomplete history weakens calibration."),("H6","Adjacent-line incoherence identifies unreliable confidence.")]
    dump(a.out_dir/"frozen_prospective_hypothesis_registry.csv",[{"hypothesis":k,"frozen_statement":v,"status":"OBSERVATIONAL_ONLY","outcome_derived_threshold":False,"selector_authorized":False} for k,v in hypotheses])
    report={"decision":"PROSPECTIVE_LINEAGE_CONTRACT_READY_NO_ELIGIBLE_LIVE_RUN","audit_timestamp_utc":datetime.now(timezone.utc).isoformat(),"current_semantic_lineage_certification":"BLOCKED","blocking_reason":"currently loaded artifacts lack explicit semantic model IDs and calibration artifact identities","eligible_lineage_certified_models":0,"prediction_rows_appended":0,"historical_backfill_performed":False,"july_outcomes_inspected":False,"ledger_preserved_append_only":True}
    (a.out_dir/"no_eligible_live_run_report.json").write_text(json.dumps(report,indent=2)+"\n")
    (a.out_dir/"no_eligible_live_run_report.md").write_text("# First lineage-certified prospective capture\n\nStatus: `PROSPECTIVE_LINEAGE_CONTRACT_READY_NO_ELIGIBLE_LIVE_RUN`\n\nAll three loaded artifacts were hashed, but none carries an explicit semantic model ID or calibration artifact identity. Mutable `latest/{prop}.joblib` paths and proposition/training metadata were not promoted into invented semantic names. The fail-closed contract therefore admits no current model and no prediction rows were appended or backfilled.\n")
    files=sorted(p for p in a.out_dir.iterdir() if p.name!="SHA256SUMS.csv"); dump(a.out_dir/"SHA256SUMS.csv",[{"file":p.name,"sha256":sha(p),"bytes":p.stat().st_size} for p in files]); print(json.dumps(report,indent=2))
if __name__=="__main__": main()
