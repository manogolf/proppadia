#!/usr/bin/env python3
"""Terminal review utility; refuses a decision before the frozen stopping boundary."""
from __future__ import annotations
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/"artifacts/analysis/model_development/mlb_unified_batter_outcome_v1_coherent_revision/2026-07-23"

def main():
 progress=json.loads((OUT/"stopping_condition_progress.json").read_text())
 if not progress["terminal_boundary_reached"]:
  print(json.dumps({"terminal_decision":"PENDING_UNTIL_FIVE_SLATES_OR_500_GRADED_HITTERS",
                    "qualifying_completed_slates":progress["qualifying_completed_slates"],
                    "graded_eligible_hitters":progress["graded_eligible_hitters"]},indent=2))
  return
 raise SystemExit("Terminal boundary reached: run the frozen grading review implementation; redesign is prohibited.")

if __name__=="__main__":main()
