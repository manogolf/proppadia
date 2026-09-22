from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.mlb import model_trainer
from backend.mlb.season_transition.game_phase_authority_v1 import (
    DEFAULT_PROPOSAL_PATH,
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityMetadata,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.training_phase_eligibility_v1 import (
    EligibilityGateBlocked,
    filter_regular_season_membership,
)
from backend.mlb.training_run_lineage_v1 import (
    INPUT_MANIFEST_CONTRACT,
    RESULT_MANIFEST_CONTRACT,
    TrainingLineageError,
    build_input_manifest,
    build_result_manifest,
    file_sha256,
    require_completed_result_binding,
    write_manifest_immutable,
)


ROOT = Path(__file__).resolve().parents[3]


class StubAuthority(CanonicalGamePhaseAuthority):
    def __init__(
        self,
        metadata: GamePhaseAuthorityMetadata,
        records: dict[int, GamePhaseAuthorityRecord],
    ) -> None:
        self._metadata = metadata
        self._records = records

    @property
    def metadata(self) -> GamePhaseAuthorityMetadata:
        return self._metadata

    def lookup_exact(self, game_pk: object) -> GamePhaseAuthorityRecord:
        if game_pk is None:
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
        exact = int(game_pk)
        try:
            return self._records[exact]
        except KeyError:
            raise GamePhaseAuthorityError("GAME_PHASE_ABSENT", game_pk=exact) from None


class TrainingPhaseActiveCutoverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.authority = HashedProposalAuthority()
        cls.regular = cls.authority.lookup_exact(822744)
        cls.preseason = cls.authority.lookup_exact(831427)
        cls.stub = StubAuthority(
            cls.authority.metadata,
            {cls.regular.game_pk: cls.regular, cls.preseason.game_pk: cls.preseason},
        )

    def gate_report(self):
        result = filter_regular_season_membership(
            [
                {"game_id": self.regular.game_pk, "row": "regular"},
                {"game_id": self.preseason.game_pk, "row": "preseason"},
            ],
            game_pk_field="game_id",
            authority=self.stub,
            consumer_identity="ACTIVE_CUTOVER_TEST",
            input_identity="SYNTHETIC",
            row_identity_fields=("row", "game_id"),
        )
        return dict(result.report)

    def input_manifest(self, run_identity: str = "synthetic-run") -> dict:
        return build_input_manifest(
            run_identity=run_identity,
            code_commit="a" * 40,
            trainer_config={"entrypoint": "backend/mlb/model_trainer.py", "prop": "hits"},
            admitted_game_pks=[self.regular.game_pk],
            admitted_row_count=1,
            gate_report=self.gate_report(),
            source_snapshot={
                "identity": "synthetic",
                "hashes": {"rows_sha256": "b" * 64},
            },
            canonical_row_order_sha256="c" * 64,
            retained_feature_sha256="d" * 64,
            retained_target_sha256="e" * 64,
            feature_contract={"ordered_features": ["feature"]},
            target_contract={"field": "y"},
            split_definition={"method": "SYNTHETIC_80_20", "random_state": 42},
        )

    def test_ordinary_training_path_cannot_bypass_gate_evidence(self) -> None:
        frame = pd.DataFrame(
            [{"game_id": self.regular.game_pk, "y": 1, "feature": 1.0}]
        )
        with patch(
            "backend.mlb.shared.model_authority.assert_predictive_model_qualified"
        ), patch.object(model_trainer, "_supabase_client", return_value=None), patch.object(
            model_trainer, "_load_feature_spec", return_value={"hits": {"features": ["feature"]}}
        ), patch.object(
            model_trainer, "fetch_training_rows", return_value=frame
        ), patch.object(
            model_trainer, "build_pipeline", side_effect=AssertionError("FIT_PATH_REACHED")
        ):
            with self.assertRaisesRegex(
                RuntimeError, "TRAINING_PHASE_ACTIVE_GATE_EVIDENCE_MISSING"
            ):
                model_trainer.train_models_for_prop("hits", quiet=True)

    def test_preseason_never_reaches_feature_aggregation_or_fit(self) -> None:
        seen: list[tuple[str, list[int]]] = []

        def time_features(frame: pd.DataFrame) -> pd.DataFrame:
            seen.append(("time", [int(value) for value in frame["game_id"]]))
            return frame.copy()

        def derived(_sb, frame: pd.DataFrame, _features):
            seen.append(("derived", [int(value) for value in frame["game_id"]]))
            return frame.copy()

        rows = [
            {"game_id": self.regular.game_pk, "outcome": "win", "line": 1, "prop_value": 1},
            {"game_id": self.preseason.game_pk, "outcome": "loss", "line": 1, "prop_value": 1},
        ]
        with patch.object(model_trainer, "_fetch_base_rows_pg", return_value=rows), patch.object(
            model_trainer, "_verified_phase_authority", return_value=self.stub
        ), patch.object(
            model_trainer, "_add_time_features", side_effect=time_features
        ), patch.object(
            model_trainer, "_merge_derived_features", side_effect=derived
        ), patch.object(
            model_trainer, "build_pipeline", side_effect=AssertionError("FIT_PATH_REACHED")
        ):
            admitted = model_trainer._fetch_base_and_merge(
                None, "hits", 365, 100, ["feature"]
            )
        self.assertEqual(list(admitted["game_id"]), [self.regular.game_pk])
        self.assertEqual(
            seen,
            [
                ("time", [self.regular.game_pk]),
                ("derived", [self.regular.game_pk]),
            ],
        )

    def test_missing_authority_stops_before_feature_aggregation(self) -> None:
        rows = [{"game_id": 999999999, "outcome": "win", "line": 1, "prop_value": 1}]
        forbidden = AssertionError("FEATURE_OR_FIT_PATH_REACHED")
        with patch.object(model_trainer, "_fetch_base_rows_pg", return_value=rows), patch.object(
            model_trainer, "_verified_phase_authority", return_value=self.stub
        ), patch.object(
            model_trainer, "_add_time_features", side_effect=forbidden
        ), patch.object(
            model_trainer, "_merge_derived_features", side_effect=forbidden
        ), patch.object(
            model_trainer, "build_pipeline", side_effect=forbidden
        ):
            with self.assertRaises(EligibilityGateBlocked):
                model_trainer._fetch_base_and_merge(None, "hits", 365, 100, ["feature"])

    def test_view_authority_failure_cannot_fall_back(self) -> None:
        class Query:
            def select(self, _fields):
                return self

            def eq(self, _field, _value):
                return self

            def gte(self, _field, _value):
                return self

            def range(self, _start, _end):
                return self

            def execute(self):
                return type(
                    "Response",
                    (),
                    {"data": [{"game_id": 999999999, "prop_type": "hits"}]},
                )()

        class Client:
            def table(self, _name):
                return Query()

        with patch.object(model_trainer, "FEATURE_VIEW", "synthetic_view"), patch.object(
            model_trainer, "_verified_phase_authority", return_value=self.stub
        ):
            with self.assertRaises(EligibilityGateBlocked):
                model_trainer._fetch_from_view(Client(), "hits", 365, 100, [])

    def test_all_trainer_source_modes_have_active_gate_and_fit_ordering(self) -> None:
        source = (ROOT / "backend/mlb/model_trainer.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        functions = {
            node.name: node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for name in ("_fetch_from_view", "_fetch_base_and_merge", "_fetch_reconcile_and_merge"):
            rendered = ast.dump(functions[name], include_attributes=False)
            self.assertIn("_apply_active_training_phase_gate", rendered)
        trainer = functions["train_models_for_prop"]
        calls = {
            node.func.id: node.lineno
            for node in ast.walk(trainer)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        self.assertLess(calls["fetch_training_rows"], calls["_create_training_input_binding"])
        self.assertLess(calls["_create_training_input_binding"], calls["build_pipeline"])

    def test_tampered_authority_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            proposal = Path(directory) / "proposal.jsonl"
            proposal.write_bytes(DEFAULT_PROPOSAL_PATH.read_bytes() + b"\n")
            with self.assertRaisesRegex(
                GamePhaseAuthorityError, "GAME_PHASE_PROPOSAL_HASH_MISMATCH"
            ):
                HashedProposalAuthority(proposal_path=proposal)

    def test_input_manifest_is_deterministic_and_tamper_evident(self) -> None:
        first = self.input_manifest("deterministic-run")
        second = self.input_manifest("deterministic-run")
        self.assertEqual(first, second)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input_manifest.json"
            write_manifest_immutable(
                path, first, contract=INPUT_MANIFEST_CONTRACT
            )
            altered = json.loads(path.read_text())
            altered["input_population"]["admitted_row_count"] = 2
            path.write_text(json.dumps(altered), encoding="utf-8")
            model_fit_called = False
            try:
                from backend.mlb.training_run_lineage_v1 import load_and_verify_manifest

                load_and_verify_manifest(path, INPUT_MANIFEST_CONTRACT)
                model_fit_called = True
            except TrainingLineageError:
                pass
            self.assertFalse(model_fit_called)

    def test_model_trainer_writes_verified_input_binding_before_fit(self) -> None:
        frame = pd.DataFrame(
            [
                {"id": "a", "game_id": self.regular.game_pk, "prop_type": "hits", "player_id": 1, "feature": 1.0, "y": 0},
                {"id": "b", "game_id": self.regular.game_pk, "prop_type": "hits", "player_id": 2, "feature": 2.0, "y": 1},
            ]
        )
        source_snapshot = {
            "identity": "synthetic-frame",
            "hashes": {"rows_sha256": "f" * 64},
        }
        with tempfile.TemporaryDirectory() as directory, patch.object(
            model_trainer, "MODELS_DIR", Path(directory)
        ), patch.object(
            model_trainer, "_new_training_run_identity", return_value="synthetic-bound-run"
        ), patch.object(
            model_trainer, "_repository_commit", return_value="a" * 40
        ):
            run_identity, manifest, path = model_trainer._create_training_input_binding(
                prop_type="hits",
                days_back=365,
                limit=100,
                frame=frame,
                train_frame=frame.iloc[:1],
                validation_frame=frame.iloc[1:],
                feature_columns=["feature"],
                numeric_features=["feature"],
                categorical_features=[],
                split_method="SYNTHETIC_50_50",
                gate_report=self.gate_report(),
                source_snapshot=source_snapshot,
            )
            self.assertEqual(run_identity, "synthetic-bound-run")
            self.assertTrue(path.exists())
            self.assertEqual(manifest["status"], "INPUT_FROZEN_BEFORE_FIT")
            self.assertEqual(json.loads(path.read_text()), manifest)

    def test_interrupted_manifest_creation_leaves_no_valid_run(self) -> None:
        manifest = self.input_manifest("interrupted-run")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run/input_manifest.json"

            def interrupt(_temporary: Path, _final: Path) -> None:
                raise RuntimeError("SIMULATED_INTERRUPTION")

            with self.assertRaisesRegex(RuntimeError, "SIMULATED_INTERRUPTION"):
                write_manifest_immutable(
                    path,
                    manifest,
                    contract=INPUT_MANIFEST_CONTRACT,
                    before_publish_hook=interrupt,
                )
            self.assertFalse(path.exists())
            self.assertEqual(list(path.parent.glob("*.tmp")), [])

    def test_result_publication_requires_completed_binding_and_artifacts(self) -> None:
        manifest = self.input_manifest("result-binding-run")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            input_path = root / "input_manifest.json"
            result_path = root / "result_manifest.json"
            model_path = root / "model.joblib"
            evaluation_path = root / "evaluation.json"
            write_manifest_immutable(
                input_path, manifest, contract=INPUT_MANIFEST_CONTRACT
            )
            with self.assertRaises(TrainingLineageError):
                require_completed_result_binding(input_path, result_path)
            model_path.write_bytes(b"synthetic-model")
            evaluation_path.write_bytes(b"synthetic-evaluation")
            result = build_result_manifest(
                input_manifest=manifest,
                model_artifacts=[
                    {
                        "path": str(model_path),
                        "sha256": file_sha256(model_path),
                        "bytes": model_path.stat().st_size,
                    }
                ],
                result_artifacts=[
                    {
                        "path": str(evaluation_path),
                        "sha256": file_sha256(evaluation_path),
                        "bytes": evaluation_path.stat().st_size,
                    }
                ],
                completed_at_utc="2026-09-22T00:00:00Z",
            )
            write_manifest_immutable(
                result_path, result, contract=RESULT_MANIFEST_CONTRACT
            )
            verified = require_completed_result_binding(input_path, result_path)
            self.assertEqual(verified["status"], "COMPLETED_BOUND")
            model_path.write_bytes(b"tampered")
            with self.assertRaisesRegex(
                TrainingLineageError, "BOUND_ARTIFACT_(SIZE|HASH)_MISMATCH"
            ):
                require_completed_result_binding(input_path, result_path)

    def test_committed_frozen_counts_and_hashes_remain_exact(self) -> None:
        report = json.loads(
            (
                ROOT
                / "docs/contracts/mlb_2026_training_phase_eligibility_dry_run_v1/dry_run_report.json"
            ).read_text()
        )
        gate = report["gate_report"]
        self.assertEqual(
            (gate["input_row_count"], gate["input_distinct_game_pk_count"]),
            (600766, 2812),
        )
        self.assertEqual(
            (gate["admitted_row_count"], gate["admitted_distinct_game_pk_count"]),
            (459604, 2341),
        )
        self.assertEqual(
            (gate["decision_row_counts"]["EXCLUDED_PRESEASON"], len(gate["decision_game_pks"]["EXCLUDED_PRESEASON"])),
            (141162, 471),
        )
        self.assertTrue(report["invariance"]["all_retained_hashes_identical"])

    def test_validation_uses_only_synthetic_temporary_artifacts(self) -> None:
        trainer_source = (ROOT / "backend/mlb/model_trainer.py").read_text()
        test_source = Path(__file__).read_text()
        test_tree = ast.parse(test_source)
        self.assertFalse(
            any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "fit"
                for node in ast.walk(test_tree)
            )
        )
        self.assertIn("tempfile.TemporaryDirectory", test_source)
        self.assertIn("_create_training_input_binding", trainer_source)
        self.assertIn("require_completed_result_binding", trainer_source)


if __name__ == "__main__":
    unittest.main()
