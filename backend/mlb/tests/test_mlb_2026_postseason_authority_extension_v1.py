from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path

from backend.mlb.public_game_predictions.phase_gating_v1 import classify_moneyline_row
from backend.mlb.scripts.activate_mlb_postseason_phase_authority_v2 import (
    ActivationError, EXPECTED_GAME_PKS, EXPECTED_ROUNDS, SOURCE_SHA256,
    _compare_and_swap, validate_child,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    EXPECTED_V1_DESCRIPTOR_SHA256, VersionedFileAuthority, load_v1_authority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    REPO_ROOT, sha256_file,
)
from backend.mlb.season_transition.regular_season_close_inventory_v2 import (
    validate_close_readiness_package,
)


class PostseasonAuthorityExtensionV1Tests(unittest.TestCase):
    def test_governed_child_is_exact_append_only_extension(self) -> None:
        result = validate_child(allow_candidate=False)
        self.assertEqual(result["parent_rows"], 2919)
        self.assertEqual(result["rows"], 2923)
        self.assertEqual(result["new_game_pks"], list(EXPECTED_GAME_PKS))
        self.assertEqual(result["snapshot_status"], "GOVERNED")
        descriptor_path = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v2/descriptor.json"
        authority = VersionedFileAuthority(
            descriptor_path=descriptor_path,
            expected_descriptor_sha256=sha256_file(descriptor_path),
        )
        self.assertEqual(authority.metadata.parent_descriptor_sha256, EXPECTED_V1_DESCRIPTOR_SHA256)
        self.assertEqual(authority.metadata.phase_counts["REGULAR_SEASON"], 2430)
        for game_pk in EXPECTED_GAME_PKS:
            record = authority.lookup_exact(game_pk)
            self.assertEqual(record.source_game_type, "F")
            self.assertEqual(record.season_phase, "POSTSEASON")
            self.assertEqual(record.postseason_round, "WILD_CARD")
            self.assertEqual(record.source_round, EXPECTED_ROUNDS[game_pk])
            self.assertEqual(record.primary_source_sha256, SOURCE_SHA256)

    def test_moneyline_gate_accepts_observed_games_and_keeps_regular_phase(self) -> None:
        descriptor_path = REPO_ROOT / "backend/mlb/season_transition/authority_snapshots/v2/descriptor.json"
        authority = VersionedFileAuthority(
            descriptor_path=descriptor_path,
            expected_descriptor_sha256=sha256_file(descriptor_path),
        )
        authority.require_supported_window("2026-09-29", "2026-09-29")
        decisions = [classify_moneyline_row(
            {"game_id": game_pk, "game_date": "2026-09-29"}, authority=authority
        ) for game_pk in EXPECTED_GAME_PKS]
        self.assertTrue(all(item.evaluation_partition == "POSTSEASON" for item in decisions))
        self.assertEqual({item.postseason_round for item in decisions}, {"WILD_CARD"})
        v1 = load_v1_authority()
        self.assertEqual(v1.metadata.phase_counts["REGULAR_SEASON"], 2430)
        self.assertEqual(v1.metadata.snapshot_descriptor_sha256, EXPECTED_V1_DESCRIPTOR_SHA256)

    def test_regular_season_close_remains_ready_and_v1_pinned(self) -> None:
        report = validate_close_readiness_package()
        self.assertTrue(report["integrity_passed"])
        self.assertTrue(report["close_ready"])
        self.assertEqual(report["population_counts"]["regular_season_game_pks"], 2430)
        self.assertEqual(load_v1_authority().metadata.phase_counts["REGULAR_SEASON"], 2430)

    def test_selector_compare_and_swap_refuses_changed_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "active_selection.json"
            prior = b'{"selection":"v1"}\n'
            child = b'{"selection":"v2"}\n'
            path.write_bytes(prior)
            _compare_and_swap(path, prior, child, "CAS_FAILED")
            self.assertEqual(path.read_bytes(), child)
            _compare_and_swap(path, child, prior, "CAS_FAILED")
            self.assertEqual(path.read_bytes(), prior)
            path.write_bytes(b'{"selection":"other"}\n')
            with self.assertRaisesRegex(ActivationError, "CAS_FAILED"):
                _compare_and_swap(path, prior, child, "CAS_FAILED")


if __name__ == "__main__":
    unittest.main()
