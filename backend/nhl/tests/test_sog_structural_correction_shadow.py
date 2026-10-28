from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import pandas as pd

from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.sog_structural_correction_shadow import (
    MODEL_IDENTITY, build_predictions, capture,
)
import backend.nhl.sog_structural_correction_shadow as shadow


class StructuralCorrectionShadowTest(unittest.TestCase):
    def fixture(self, d10=8.0, d20=8.0, toi=20.0):
        feat = pd.DataFrame([{"game_id": 1, "player_id": 10, "d10_sog_per60": d10,
            "d20_sog_per60": d20, "d5_sog_per60": 7.0, "d10_toi_min_avg": toi,
            "d20_toi_min_avg": toi + 1, "eligible": True}])
        rate = d10 if d10 is not None else d20
        rate = rate if rate is not None else 7.0
        prod = pd.DataFrame([{"game_id": 1, "player_id": 10, "expected_sog": rate*toi/60,
            "selected_sog_rate_per60": rate, "selected_toi_minutes": toi,
            "selected_toi_source": "d10_toi_min_avg"}])
        if d10 is None: feat.loc[0,"d10_sog_per60"] = float("nan")
        if d20 is None: feat.loc[0,"d20_sog_per60"] = float("nan")
        return feat,prod

    def build(self,d10,d20):
        f,p=self.fixture(d10,d20)
        return build_predictions(f,p,slate_date="2026-10-14",run_id="r",feature_cutoff_utc="t",canonical_game_ids=[1])

    def test_activation_states_and_boundaries(self):
        self.assertEqual(self.build(8,8).activation_state.iloc[0],"NEITHER")
        self.assertEqual(self.build(8,7).activation_state.iloc[0],"DIVERGENCE_ONLY")
        self.assertEqual(self.build(10,10).activation_state.iloc[0],"HIGH_RATE_ONLY")
        self.assertEqual(self.build(11,9).activation_state.iloc[0],"BOTH")
        self.assertEqual(self.build(8.5,8).divergence_correction_applied.iloc[0],False)
        self.assertEqual(self.build(9,9).high_rate_compression_applied.iloc[0],False)
        self.assertEqual(self.build(10,8).high_rate_compression_applied.iloc[0],False)

    def test_missing_d20_still_allows_high_rate_compression(self):
        row=self.build(8,None).iloc[0]
        self.assertEqual(row.divergence_reason,"D20_UNAVAILABLE_DIVERGENCE_SKIPPED")
        self.assertFalse(row.high_rate_compression_applied)
        row=self.build(10,None).iloc[0]
        self.assertTrue(row.high_rate_compression_applied)
        self.assertEqual(row.activation_state,"HIGH_RATE_ONLY")

    def test_exposure_and_probabilities(self):
        rows=self.build(11,9)
        self.assertTrue((rows.production_exposure_minutes==20).all())
        self.assertTrue((rows.combined_shadow_lambda==rows.final_shadow_rate_per60*20/60).all())
        p=rows.set_index("line").combined_p_over
        self.assertGreaterEqual(p[1.5],p[2.5]); self.assertGreaterEqual(p[2.5],p[3.5])

    def test_capture_is_immutable_deterministic_and_receipt_api(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); f,p=self.fixture(11,9)
            fp=root/"features.csv"; pp=root/"production.csv"; scorer=Path(__file__).parents[1]/"scripts/score_sog_poisson_baseline.py"
            f.to_csv(fp,index=False); p.to_csv(pp,index=False)
            before=hashlib.sha256(pp.read_bytes()).hexdigest()
            result=capture(feature_path=fp,production_path=pp,output_root=root/"captures",run_id="r",slate_date="2026-10-14",season=2026,
                feature_sha256=hashlib.sha256(fp.read_bytes()).hexdigest(),feature_cutoff_utc="t",canonical_game_ids=[1],started_excluded_game_ids=[],scorer_path=scorer,
                production_identity=("hgb_count","NHL_SOG_COUNT_HGB_V1"))
            self.assertEqual(result["model_identity"],MODEL_IDENTITY); self.assertEqual(result["deterministic_replay"],"PASS")
            manifest=json.loads((Path(result["path"])/"manifest.json").read_text())
            self.assertEqual(manifest["production_model_identity"],"hgb_count/NHL_SOG_COUNT_HGB_V1")
            self.assertEqual(manifest["comparator_model_identity"],"poisson_baseline/baseline_v1")
            self.assertEqual(hashlib.sha256(pp.read_bytes()).hexdigest(),before)
            self.assertTrue((Path(result["path"])/"RUN_COMPLETE.json").is_file())
            rec=DailyRunRecorder(run_id="r",command=["offline"],phase="EARLY")
            rec.lane("sog_d10_structural_correction_shadow")
            rec.start_lane("sog_d10_structural_correction_shadow",inputs=[{"model_identity":MODEL_IDENTITY}])
            rec.finish_lane("sog_d10_structural_correction_shadow",outputs=[result],database_rows_written=False,provider_requests=0,credits_consumed=0)
            lane=rec.lane("sog_d10_structural_correction_shadow")
            self.assertEqual(lane.provider_requests,0); self.assertEqual(lane.credits_consumed,0)
            self.assertEqual(lane.database_write_status,"NONE")

    def test_completion_marker_is_written_only_after_validation(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td); f,p=self.fixture(8,8); fp=root/"f.csv"; pp=root/"p.csv"; scorer=Path(__file__).parents[1]/"scripts/score_sog_poisson_baseline.py"
            f.to_csv(fp,index=False); p.to_csv(pp,index=False); sha=hashlib.sha256(fp.read_bytes()).hexdigest()
            original=shadow.build_predictions; calls={"n":0}
            def altered(*args,**kwargs):
                calls["n"]+=1; result=original(*args,**kwargs)
                if calls["n"]==2: result.loc[0,"combined_shadow_lambda"]+=1
                return result
            with patch.object(shadow,"build_predictions",side_effect=altered):
                with self.assertRaisesRegex(ValueError,"DETERMINISTIC_REPLAY_FAILED"):
                    capture(feature_path=fp,production_path=pp,output_root=root/"out",run_id="r",slate_date="2026-10-14",season=2026,
                        feature_sha256=sha,feature_cutoff_utc="t",canonical_game_ids=[1],started_excluded_game_ids=[],scorer_path=scorer)
            staged=root/"out/season=2026/slate_date=2026-10-14/run_id=r"/f".{MODEL_IDENTITY}.incomplete"
            self.assertTrue(staged.is_dir()); self.assertFalse((staged/"RUN_COMPLETE.json").exists())


if __name__ == "__main__":
    unittest.main()
