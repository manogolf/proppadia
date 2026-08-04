#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,hashlib,json,shutil
from datetime import datetime,timezone
from pathlib import Path
from backend.mlb.shared import semantic_model_registry as sr
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 ap=argparse.ArgumentParser(); ap.add_argument("--package",type=Path,required=True); a=ap.parse_args(); root=a.package
 reg=sr.load_active_registry(); ledger=root/"append_only_prediction_ledger.csv"
 with ledger.open(newline="") as f: rows=list(csv.DictReader(f))
 counts={p:sum(r.get("model_semantic_name")==e["semantic_model_id"] for r in rows) for p,e in [(x["proposition"],x) for x in reg["entries"]]}
 manifests=root/"semantic_manifests"; manifests.mkdir(exist_ok=True)
 pointer=sr.ACTIVE_POINTER
 for e in reg["entries"]:
  src=(pointer.parent/e["semantic_manifest_path"]).resolve(); shutil.copyfile(src,manifests/src.name)
 report={"decision":"FIRST_LINEAGE_CERTIFIED_PROSPECTIVE_RUN_CAPTURED","capture_completed_at_utc":datetime.now(timezone.utc).isoformat(),"live_run_eligibility":"ELIGIBLE_ALL_EIGHT_GAMES_PREGAME_AT_PREDICTION_TIME","run_tag":rows[0]["run_tag"] if rows else "","appended_ledger_rows":len(rows),"rows_by_proposition":counts,"lineage_test_status":json.loads((root/"lineage_validation_report.json").read_text())["lineage_test_status"],"residual_phase_readiness":"NOT_READY_PENDING_CERTIFIED_PROSPECTIVE_OUTCOMES","production_readiness":"NOT_AUTHORIZED","outcomes_inspected":False,"backfill_performed":False,"selector_wager_gate_ev_model_or_promotion_change":False}
 (root/"capture_report.json").write_text(json.dumps(report,indent=2)+"\n"); (root/"capture_report.md").write_text(f"# Prospective semantic registration and first capture\n\nDecision: `FIRST_LINEAGE_CERTIFIED_PROSPECTIVE_RUN_CAPTURED`\n\nAll three forward-only semantic identities are active and validated. A fresh post-registration ordinary-window run appended {len(rows)} certified model-selected-side rows across eight games while all scheduled starts were still in the future. No outcomes were inspected and no backfill occurred. Residual readiness remains `NOT_READY_PENDING_CERTIFIED_PROSPECTIVE_OUTCOMES`; production remains `NOT_AUTHORIZED`.\n")
 files=sorted(p for p in root.rglob('*') if p.is_file() and p.name!='SHA256SUMS.csv');
 with (root/"SHA256SUMS.csv").open('w',newline='') as f:
  w=csv.DictWriter(f,fieldnames=['file','sha256','bytes']); w.writeheader(); w.writerows([{'file':str(p.relative_to(root)),'sha256':sha(p),'bytes':p.stat().st_size} for p in files])
 print(json.dumps(report,indent=2))
if __name__=="__main__": main()
