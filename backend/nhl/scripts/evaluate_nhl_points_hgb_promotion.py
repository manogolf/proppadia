#!/usr/bin/env python3
"""Print the evidence-backed current NHL Points HGB promotion classification."""
from __future__ import annotations
import json
from backend.nhl.points_hgb_promotion import evaluate_promotion

if __name__ == "__main__":
    print(json.dumps(evaluate_promotion(), indent=2, sort_keys=True))
