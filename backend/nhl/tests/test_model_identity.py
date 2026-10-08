import tempfile
import unittest
from pathlib import Path
import hashlib
from backend.nhl.daily_orchestration import DailyRunRecorder

from backend.nhl.model_identity import fitted_model_identity, validate_fitted_model_evidence
from backend.nhl.scripts import build_nhl_player_performance_pulse as pulse


class FittedModelIdentityTests(unittest.TestCase):
    def _identity(self, components, scoring_configuration=None):
        prediction = Path(__file__).resolve()
        return fitted_model_identity(
            model_family="fixture", model_version="v1", components=components,
            prediction_path=prediction, scoring_run_id="fixture-run",
            scoring_configuration=scoring_configuration,
        )

    def test_deterministic_single_and_multiple_components(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[3]) as temp:
            root = Path(temp)
            a, b = root / "a.bin", root / "b.bin"
            a.write_bytes(b"model-a")
            b.write_bytes(b"calibration-b")
            one = self._identity([("model", a)])
            self.assertEqual(one["fitted_model_identity_sha256"], self._identity([("model", a)])["fitted_model_identity_sha256"])
            multi = self._identity([("calibration", b), ("model", a)])
            reordered = self._identity([("model", a), ("calibration", b)])
            self.assertEqual(multi["fitted_model_identity_sha256"], reordered["fitted_model_identity_sha256"])
            b.write_bytes(b"calibration-changed")
            self.assertNotEqual(multi["fitted_model_identity_sha256"], self._identity([("model", a), ("calibration", b)])["fitted_model_identity_sha256"])
            self.assertEqual(one["prediction_artifact_sha256"], hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
            self.assertEqual(one["prediction_artifact_path"], Path(__file__).resolve().relative_to(Path(__file__).resolve().parents[3]).as_posix())
            self.assertTrue(validate_fitted_model_evidence(one, prediction_path=one["prediction_artifact_path"],
                                                           prediction_sha256=one["prediction_artifact_sha256"]))
            invalid = dict(one, fitted_model_identity_sha256="0" * 64)
            self.assertFalse(validate_fitted_model_evidence(invalid, prediction_path=one["prediction_artifact_path"],
                                                            prediction_sha256=one["prediction_artifact_sha256"]))
            self.assertNotEqual(self._identity([("model", a)], {"no_monotonic": False})["fitted_model_identity_sha256"],
                                self._identity([("model", a)], {"no_monotonic": True})["fitted_model_identity_sha256"])

    def test_daily_lane_receipt_keeps_prediction_sha_bound_to_identity(self):
        prediction = Path(__file__).resolve()
        component = Path(__file__).resolve()
        evidence = self._identity([("fixture_fitted_model", component)], {"line": 1.5})
        recorder = DailyRunRecorder(run_id="fixture-run", command=["daily"], phase="EVENING")
        recorder.lane("points").outputs.append({
            "path": evidence["prediction_artifact_path"],
            "sha256": evidence["prediction_artifact_sha256"],
            "fitted_model_evidence": evidence,
        })
        receipt = recorder.payload()
        self.assertEqual(receipt["fitted_model_identity_contract"], "NHL_FITTED_MODEL_IDENTITY_V1")
        retained = receipt["lanes"]["points"]["outputs"][0]
        self.assertEqual(retained["sha256"], retained["fitted_model_evidence"]["prediction_artifact_sha256"])
        self.assertEqual(retained["fitted_model_evidence"]["fitted_model_identity_sha256"], evidence["fitted_model_identity_sha256"])

    def test_comparability_is_proof_or_explicit_legacy_unavailable(self):
        self.assertEqual(pulse.comparability_status("abc", "abc"), "PROVEN_SAME_FITTED_MODEL")
        self.assertEqual(pulse.comparability_status("abc", "def"), "PROVEN_FITTED_MODEL_CHANGED")
        self.assertEqual(pulse.comparability_status(None, "def"), "HISTORICAL_MODEL_IDENTITY_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
