from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from backend.mlb.scripts.validate_mlb_training_phase_eligibility_active_cutover_v1 import (
    ALLOWED_MODEL_EXTENSIONS,
    EXPECTED_HARD_LINKED_PATH_GROUPS,
    ROOT,
    artifact_population_reconciliation,
    mlb_model_binary_paths,
    model_metadata_state,
)


class ActiveCutoverEvidenceCorrectionTests(unittest.TestCase):
    @staticmethod
    def _file(root: Path, relative: str, payload: bytes = b"synthetic") -> Path:
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return path

    def test_nhl_paths_are_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._file(root, "models_out/mlb/hits.joblib")
            self._file(root, "models_out/nhl/sog/lr.joblib")
            paths = {
                path.relative_to(root).as_posix()
                for path in mlb_model_binary_paths(root)
            }
            self.assertEqual(paths, {"models_out/mlb/hits.joblib"})

    def test_all_four_mlb_inventory_sources_are_included(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            expected = {
                "models_out/latest/hits.joblib",
                "artifacts/mlb_models_bundle/latest/hits.joblib",
                "backend/mlb/exports/model_v2/ranking/hits_residual_ranker.joblib",
                "artifacts/analysis/model_development/mlb_research/model.pkl",
            }
            for relative in expected:
                self._file(root, relative)
            actual = {
                path.relative_to(root).as_posix()
                for path in mlb_model_binary_paths(root)
            }
            self.assertEqual(actual, expected)

    def test_all_allowed_extensions_are_handled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for suffix in sorted(ALLOWED_MODEL_EXTENSIONS):
                self._file(
                    root,
                    f"artifacts/analysis/model_development/mlb_extension_test/model{suffix}",
                )
            actual = {path.suffix for path in mlb_model_binary_paths(root)}
            self.assertEqual(actual, set(ALLOWED_MODEL_EXTENSIONS))

    def test_unrelated_files_are_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self._file(root, "models_out/latest/notes.txt")
            self._file(
                root,
                "artifacts/analysis/model_development/baseball_research/model.joblib",
            )
            self._file(root, "backend/other/model.joblib")
            self.assertEqual(mlb_model_binary_paths(root), [])

    def test_hard_linked_paths_remain_separate_path_records(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self._file(root, "models_out/archive/hits/a.joblib")
            second = (
                root
                / "artifacts/analysis/model_development/mlb_replay/a.joblib"
            )
            second.parent.mkdir(parents=True, exist_ok=True)
            os.link(first, second)
            paths = mlb_model_binary_paths(root)
            state = model_metadata_state(root)
            self.assertEqual(len(paths), 2)
            self.assertEqual(state["artifact_path_count"], 2)
            self.assertEqual(len(state["hard_linked_path_groups"]), 1)

    def test_path_and_inode_identity_counts_are_distinct(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self._file(root, "models_out/archive/hits/a.joblib")
            linked = root / "artifacts/mlb_models_bundle/latest/a.joblib"
            linked.parent.mkdir(parents=True, exist_ok=True)
            os.link(first, linked)
            self._file(root, "models_out/archive/hits/b.joblib")
            state = model_metadata_state(root)
            self.assertEqual(state["artifact_path_count"], 3)
            self.assertEqual(state["inode_identity_count"], 2)

    def test_current_reconciliation_and_identity_claims_are_exact(self) -> None:
        state = model_metadata_state(ROOT)
        reconciliation = artifact_population_reconciliation(ROOT)
        self.assertEqual(state["artifact_path_count"], 538)
        self.assertEqual(state["extension_counts"], {".joblib": 534, ".pkl": 4})
        self.assertEqual(state["nhl_path_count"], 0)
        self.assertEqual(state["inode_identity_count"], 536)
        self.assertEqual(
            state["hard_linked_path_groups"], EXPECTED_HARD_LINKED_PATH_GROUPS
        )
        self.assertEqual(reconciliation["original_monitor_mlb_path_count"], 428)
        self.assertEqual(reconciliation["original_monitor_nhl_path_count"], 16)
        self.assertEqual(
            reconciliation["mlb_paths_omitted_by_original_monitor_count"], 110
        )
        self.assertEqual(
            state["identity_semantics"],
            "METADATA_IDENTITY_ONLY_NOT_BYTE_IDENTITY",
        )
        self.assertEqual(state["model_content_hashes_computed"], 0)
        self.assertFalse(state["byte_identity_proven"])


if __name__ == "__main__":
    unittest.main()
