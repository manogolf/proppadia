#!/usr/bin/env python3
"""Manual-only SOG shadow CLI; no scheduler hooks."""
from __future__ import annotations
import argparse,json
from pathlib import Path
from .core import grade_run,run_shadow
from backend.nhl.sog_candidate_lineage.core import effective_config
def main():
 ap=argparse.ArgumentParser();s=ap.add_subparsers(dest="cmd",required=True)
 r=s.add_parser("run");r.add_argument("--game-spine-csv",type=Path,required=True);r.add_argument("--player-inputs-csv",type=Path,required=True);r.add_argument("--quote-run-dir",type=Path,required=True);r.add_argument("--effective-policy-json",type=Path,required=True);r.add_argument("--parity-json",type=Path,required=True);r.add_argument("--output-root",type=Path,default=Path("backend/nhl/exports/sog_shadow_runs"));r.add_argument("--slate-date",required=True);r.add_argument("--run-timestamp-utc",required=True);r.add_argument("--run-type",choices=["MIDDAY","FINAL_PREGAME"],required=True);r.add_argument("--no-upload-shaped-output",action="store_true")
 p=s.add_parser("build-policy-config");p.add_argument("--policy-json",type=Path,required=True);p.add_argument("--output",type=Path,required=True)
 g=s.add_parser("grade");g.add_argument("--run-dir",type=Path,required=True);g.add_argument("--outcomes-csv",type=Path,required=True);g.add_argument("--grade-root",type=Path,default=Path("backend/nhl/exports/sog_shadow_grades"));g.add_argument("--grading-timestamp-utc",required=True)
 a=ap.parse_args()
 if a.cmd=="build-policy-config":
  if a.output.exists():raise SystemExit("OVERWRITE_ATTEMPT_BLOCKED")
  raw=json.loads(a.policy_json.read_text());segments=raw.get("thresholds_for_next_slate",raw)
  if not isinstance(segments,dict) or not segments:raise SystemExit("RUN_BLOCKED_BY_MISSING_EFFECTIVE_POLICY_CONFIG")
  cfg=effective_config(segments);a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(cfg,indent=2,sort_keys=True)+"\n");print(a.output)
 elif a.cmd=="run":print(run_shadow(game_spine_csv=a.game_spine_csv,player_inputs_csv=a.player_inputs_csv,quote_run_dir=a.quote_run_dir,effective_policy_json=a.effective_policy_json,parity_json=a.parity_json,output_root=a.output_root,slate_date=a.slate_date,run_timestamp_utc=a.run_timestamp_utc,run_type=a.run_type,emit_upload=not a.no_upload_shaped_output))
 else:print(grade_run(a.run_dir,a.outcomes_csv,a.grade_root,a.grading_timestamp_utc))
if __name__=="__main__":main()
