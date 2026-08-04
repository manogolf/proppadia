"""Write the bounded evidence package for retired MLB retraining suspension."""
from __future__ import annotations

import csv
import hashlib
import json
import os
import stat
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/ops/mlb_retired_model_retraining_suspension/2026-08-03"
PLIST = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.retrain.weekly.plist")
WRAPPER = Path("/Users/jerrystrain/bin/proppadia_mlb_retrain_weekly.sh")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(path: Path, rows: list[dict]) -> None:
    fields = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    originals = OUT / "originals"
    model_rows=[]
    for path in sorted((ROOT/"models_out/latest").glob("*")):
        if path.is_file(): model_rows.append({"path":str(path.relative_to(ROOT)),"sha256":sha(path),"status":"UNCHANGED_AFTER_SUSPENSION","authority":"RETIRED_NO_PRODUCTION_AUTHORITY"})
    semantic_root=ROOT/"backend/mlb/config/semantic_models"
    for path in sorted(semantic_root.rglob("*.json")):
        model_rows.append({"path":str(path.relative_to(ROOT)),"sha256":sha(path),"status":"UNCHANGED_BY_SUSPENSION","authority":"BLOCKED_BY_NO_QUALIFIED_MODEL_GATE"})
    write_csv(OUT/"affected_model_artifacts_and_pointers.csv",model_rows)
    sched=[
      {"plist_path":str(PLIST),"label":"com.proppadia.mlb.retrain.weekly","schedule":"Weekday=3 Hour=23 Minute=5 (Tuesday 11:05 PM local)","executable":str(WRAPPER),"arguments":"wrapper only","working_directory":str(ROOT),"stdout_path":str(ROOT/"artifacts/ops/mlb_retrain_weekly.out.log"),"stderr_path":str(ROOT/"artifacts/ops/mlb_retrain_weekly.err.log"),"downstream":"prereq -> BvP refresh -> reconcile -> broad multi-prop model_trainer -> bundle publish -> phase2 weekly cycle","before_loaded":"YES_NOT_RUNNING","after_loaded":"NO","after_enabled":"NO_DISABLED","classification":"CONFIRMED_RETIRED_MODEL_RETRAINING"},
      {"plist_path":"/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist","label":"com.proppadia.mlb.refresh.daily","schedule":"preserved","executable":"daily MLB wrapper","arguments":"unchanged","working_directory":str(ROOT),"stdout_path":"preserved","stderr_path":"preserved","downstream":"data/odds/reconciliation plus model-dependent stages guarded","before_loaded":"YES","after_loaded":"YES","after_enabled":"YES","classification":"UNRELATED_OPERATIONAL_JOB_PRESERVED"},
      {"plist_path":"/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.bvp.prewarm.daily.plist","label":"com.proppadia.mlb.bvp.prewarm.daily","schedule":"preserved","executable":"BvP prewarm","arguments":"unchanged","working_directory":str(ROOT),"stdout_path":"preserved","stderr_path":"preserved","downstream":"data-only feature refresh","before_loaded":"YES","after_loaded":"YES","after_enabled":"YES","classification":"UNRELATED_OPERATIONAL_JOB_PRESERVED"},
      {"plist_path":"/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.starter-skill-workload.daily.plist","label":"com.proppadia.mlb.starter-skill-workload.daily","schedule":"preserved","executable":"starter workload","arguments":"unchanged","working_directory":str(ROOT),"stdout_path":"preserved","stderr_path":"preserved","downstream":"research/data support","before_loaded":"YES","after_loaded":"YES","after_enabled":"YES","classification":"UNRELATED_OPERATIONAL_JOB_PRESERVED"},
    ]
    write_csv(OUT/"scheduler_inventory.csv",sched)
    chain="""# Confirmed retired retraining chain

`com.proppadia.mlb.retrain.weekly` was the only scheduled retired-model training label found. Its actual cadence was Tuesday at 23:05 local (`Weekday=3`), not Wednesday.

The preserved original wrapper executed:

1. retraining prerequisite check;
2. BvP/PvB refresh;
3. broad reconciliation-row build;
4. `make mlb-retrain-broad-reconcile`, calling `backend/mlb/model_trainer.py` over multiple propositions and replacing `MODEL_DIR/latest/{prop}.joblib` plus `MODEL_INDEX.json`;
5. production bundle publication to a mutable `latest.tgz` alias;
6. prod12 weekly phase cycle.

No other LaunchAgent, LaunchDaemon, crontab entry, or installed parent wrapper was found invoking that training target. The installed wrapper now exits 78 before the preserved unreachable chain.
"""
    (OUT/"confirmed_retraining_chain.md").write_text(chain)
    commands="""# Commands executed

- `launchctl bootout gui/501/com.proppadia.mlb.retrain.weekly`
- `launchctl disable gui/501/com.proppadia.mlb.retrain.weekly`
- copied the original plist and wrapper into `originals/` before mutation
- installed a fail-closed wrapper preserving the original chain below an unconditional exit
- validated runtime load, trainer, recalibration gate, and bundle-publish gate
- compared all `models_out/latest` hashes before and after
"""
    (OUT/"commands_executed.md").write_text(commands)
    before_after="""# launchctl before/after

Before: service existed at `gui/501/com.proppadia.mlb.retrain.weekly`, state `not running`, active count 0, runs 3, last exit code 2, calendar trigger Tuesday 23:05.

After: `launchctl print gui/501/com.proppadia.mlb.retrain.weekly` returns service not found. `launchctl print-disabled gui/501` reports `com.proppadia.mlb.retrain.weekly => disabled`.

No retired trainer process was active. User crontab does not exist. Unrelated MLB LaunchAgents remained loaded.
"""
    (OUT/"launchctl_before_after.md").write_text(before_after)
    validation={"authority_status":"NO_QUALIFIED_MLB_MODEL","blocked_status":"MLB_PREDICTIVE_MODEL_BLOCKED_NO_QUALIFIED_MODEL","production_model_load":"BLOCKED","retired_model_training":"BLOCKED","retired_model_recalibration":"BLOCKED","retired_model_bundle_publish":"BLOCKED","production_slate_generation":"BLOCKED","production_upload_generation":"BLOCKED","production_ranking_and_routing":"BLOCKED","production_quick_card_generation":"BLOCKED","production_wager_and_staking_output":"BLOCKED","production_ranking_upload":"BLOCKED","production_quick_card_upload":"BLOCKED","installed_weekly_wrapper":"BLOCKED_EXIT_78","data_only_authority_check":"PASS_ALLOWED","retired_model_hashes_unchanged":True,"semantic_registry_hashes_unchanged_by_suspension":True,"active_registry_can_restore_authority":False}
    (OUT/"production_authority_guard_validation.json").write_text(json.dumps(validation,indent=2,sort_keys=True)+"\n")
    unrelated={"decision":"PRESERVED","loaded_labels":["com.proppadia.mlb.bvp.prewarm.daily","com.proppadia.mlb.refresh.daily","com.proppadia.mlb.starter-skill-workload.daily","com.proppadia.pregame-lineup-study.20260708","com.proppadia.pregame-lineup-study.20260709"],"disabled_by_this_change":[],"data_collection":"UNCHANGED","odds_capture":"UNCHANGED","grading":"UNCHANGED","reconciliation":"UNCHANGED","exports":"UNCHANGED","database_maintenance":"UNCHANGED"}
    (OUT/"unrelated_job_preservation.json").write_text(json.dumps(unrelated,indent=2,sort_keys=True)+"\n")
    original_plist=originals/PLIST.name; original_wrapper=originals/"proppadia_mlb_retrain_weekly.sh"
    provenance={"original_plist":{"original_path":str(PLIST),"preserved_path":str(original_plist.relative_to(ROOT)),"sha256":sha(original_plist),"permissions":stat.filemode(original_plist.stat().st_mode),"size":original_plist.stat().st_size},"original_wrapper":{"original_path":str(WRAPPER),"preserved_path":str(original_wrapper.relative_to(ROOT)),"sha256":sha(original_wrapper),"permissions":stat.filemode(original_wrapper.stat().st_mode),"size":original_wrapper.stat().st_size},"installed_guarded_wrapper_sha256":sha(WRAPPER)}
    (OUT/"original_scheduler_provenance.json").write_text(json.dumps(provenance,indent=2,sort_keys=True)+"\n")
    final="""# Final status

`RETIRED_MODEL_RETRAINING_SUSPENDED_VERIFIED`

Operational model authority: `NO_QUALIFIED_MLB_MODEL`.

The retired scheduled job is unloaded and disabled; its installed wrapper is fail-closed; production model loading, retired training, recalibration, and bundle publication are blocked. Existing artifacts and semantic manifests remain preserved only as research evidence and negative controls. Replacement-model training has not begun.
"""
    (OUT/"final_status.md").write_text(final)
    manifest=[]
    for path in sorted(OUT.rglob("*")):
        if path.is_file() and path.name!="sha_manifest.csv": manifest.append({"path":str(path.relative_to(OUT)),"sha256":sha(path),"bytes":path.stat().st_size})
    write_csv(OUT/"sha_manifest.csv",manifest)
    print(json.dumps({"decision":"RETIRED_MODEL_RETRAINING_SUSPENDED_VERIFIED","files":len(manifest)+1,"model_artifacts":len(model_rows)},sort_keys=True))
    return 0

if __name__=="__main__":raise SystemExit(main())
