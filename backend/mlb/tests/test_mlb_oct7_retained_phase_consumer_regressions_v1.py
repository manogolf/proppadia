from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from backend.mlb.hits05_full_board_shadow.phase_gating_v1 import classify_full_board_hits_row
from backend.mlb.scripts import build_mlb_predictions_wide as wide
from backend.mlb.scripts.score_mlb_hits05_full_board_shadow_v1 import retained_schedule_phase_authority
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT
from backend.mlb.identity.provider_event_game_binding_v1 import EvidenceSource
from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    TotalsLiveContextError,
    load_retained_schedule,
    load_retained_schedule_projection,
)


PARENT = REPO_ROOT / "artifacts/analysis/model_development/mlb_hits05_current_nonmarket_parent_producer/2026-10-07/local_daily_20261007T180004Z"
SCHEDULE = PARENT / "governed_lineup_capture/raw/statsapi_schedule_2026-10-07_local_daily_20261007T180004Z.json"
SCHEDULE_SHA256 = "b9e5146bb6960f661f5f3685e4104dbbdfd49793a5835839d3dd801146c34435"


@unittest.skipUnless(SCHEDULE.is_file(), "retained Oct 7 schedule evidence is not present")
class RetainedOct7PhaseConsumerTests(unittest.TestCase):
    def test_totals_projects_outcomes_without_changing_source_binding(self):
        with self.assertRaises(TotalsLiveContextError, msg="strict raw loader must remain fail-closed"):
            load_retained_schedule(SCHEDULE, SCHEDULE_SHA256)
        projected, _, source_hash, projection_hash = load_retained_schedule_projection(SCHEDULE, SCHEDULE_SHA256)
        self.assertEqual(SCHEDULE_SHA256, source_hash)
        self.assertEqual(hashlib.sha256(SCHEDULE.read_bytes()).hexdigest(), source_hash)
        self.assertEqual(64, len(projection_hash))
        self.assertEqual({849833, 849838, 849822, 849827}, {g["gamePk"] for d in projected["dates"] for g in d["games"]})
        self.assertTrue(all("score" not in json.dumps(game).lower() for day in projected["dates"] for game in day["games"]))

    def test_hits_uses_retained_exact_game_authority_not_date_coverage(self):
        authority = retained_schedule_phase_authority(PARENT, "2026-10-07")
        decision = classify_full_board_hits_row(
            {"game_id": 849833, "slate_date": "2026-10-07", "game_type": "D"},
            authority=authority,
        )
        self.assertEqual("POSTSEASON", decision.normalized_phase)
        self.assertEqual("ADMITTED_POSTSEASON_SHADOW", decision.decision_code)
        self.assertEqual(SCHEDULE_SHA256, authority.source_sha256)

    def test_wide_emits_run_bound_per_game_phase_receipt(self):
        payload = json.loads(SCHEDULE.read_text(encoding="utf-8"))
        schedule_source = EvidenceSource(str(SCHEDULE), SCHEDULE_SHA256)
        authority = wide._runtime_phase_authority(schedule_source)
        with tempfile.TemporaryDirectory(dir=REPO_ROOT / "tmp") as temp_dir:
            receipt_path, receipt_sha = wide._write_phase_overlay_receipt(
                Path(temp_dir), "2026-10-07", "local_daily_20261007T180004Z", schedule_source, authority,
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(64, len(receipt_sha))
        self.assertEqual("local_daily_20261007T180004Z", receipt["run_identity"])
        self.assertEqual(SCHEDULE_SHA256, receipt["schedule_source_sha256"])
        decisions = {item["game_pk"]: item for item in receipt["game_decisions"]}
        self.assertEqual({849833, 849838, 849822, 849827}, set(decisions))
        self.assertTrue(all(item["authority_decision"] == "EXACT_GAME_AUTHORITY_ACCEPTED" for item in decisions.values()))


if __name__ == "__main__":
    unittest.main()
