"""Snapshot-only phase authority verification for the public prediction read path.

These tests use committed files only, so they behave the same on a host without
the git-ignored raw schedule sources.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from backend.mlb.season_transition.game_phase_authority_v1 import (
    REPO_ROOT,
    SOURCE_VERIFICATION_SNAPSHOT_ONLY,
    V1_DESCRIPTOR_PATH,
    EXPECTED_V1_DESCRIPTOR_SHA256,
    GamePhaseAuthorityError,
    HashedProposalAuthority,
)

COMMITTED_TREES = (
    "backend/mlb/season_transition/authority_snapshots",
    "docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal",
)
SELECTION = "backend/mlb/season_transition/authority_snapshots/active_selection.json"


class SnapshotOnlyVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        (REPO_ROOT / "tmp").mkdir(exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="snapshot_only_", dir=REPO_ROOT / "tmp"))
        self.addCleanup(shutil.rmtree, self.root, True)
        for tree in COMMITTED_TREES:
            shutil.copytree(REPO_ROOT / tree, self.root / tree)
        selection = json.loads((self.root / SELECTION).read_text())
        self.descriptor = self.root / selection["descriptor_path"]
        self.descriptor_data = json.loads(self.descriptor.read_text())

    def load(self, **kwargs) -> HashedProposalAuthority:
        return HashedProposalAuthority(
            root=self.root, selection_path=self.root / SELECTION, **kwargs
        )

    def test_committed_snapshot_loads_without_raw_sources(self) -> None:
        authority = self.load(verify_source_bytes=False)
        self.assertEqual(authority.source_verification, SOURCE_VERIFICATION_SNAPSHOT_ONLY)
        self.assertEqual(len(authority.records), self.descriptor_data["row_count"])
        self.assertEqual(
            authority.metadata.snapshot_descriptor_sha256,
            json.loads((self.root / SELECTION).read_text())["descriptor_sha256"],
        )

    def test_full_verification_still_requires_raw_sources(self) -> None:
        with self.assertRaises(GamePhaseAuthorityError) as caught:
            self.load()
        self.assertEqual(caught.exception.code, "GAME_PHASE_EVIDENCE_MISSING")

    def test_tampered_snapshot_label_is_rejected(self) -> None:
        proposal = self.descriptor.parent / Path(self.descriptor_data["proposal_path"]).name
        lines = proposal.read_text().splitlines(keepends=True)
        lines[-1] = lines[-1].replace('"season_phase":"', '"season_phase":"X', 1)
        proposal.write_text("".join(lines))
        with self.assertRaises(GamePhaseAuthorityError) as caught:
            self.load(verify_source_bytes=False)
        self.assertEqual(caught.exception.code, "SNAPSHOT_PROPOSAL_HASH_MISMATCH")

    def test_tampered_source_manifest_is_rejected(self) -> None:
        manifest = self.descriptor.parent / Path(
            self.descriptor_data["source_manifest_path"]
        ).name
        manifest.write_bytes(manifest.read_bytes() + b"\n")
        with self.assertRaises(GamePhaseAuthorityError) as caught:
            self.load(verify_source_bytes=False)
        self.assertEqual(caught.exception.code, "SNAPSHOT_SOURCE_MANIFEST_HASH_MISMATCH")

    def test_snapshot_only_is_refused_for_explicit_descriptors(self) -> None:
        with self.assertRaises(GamePhaseAuthorityError) as caught:
            HashedProposalAuthority(
                root=self.root,
                descriptor_path=self.root / V1_DESCRIPTOR_PATH.relative_to(REPO_ROOT),
                expected_descriptor_sha256=EXPECTED_V1_DESCRIPTOR_SHA256,
                verify_source_bytes=False,
            )
        self.assertEqual(
            caught.exception.code, "GAME_PHASE_SNAPSHOT_ONLY_REQUIRES_ACTIVE_SELECTION"
        )

    def test_snapshot_only_loader_is_used_only_by_the_public_projection(self) -> None:
        users = sorted(
            str(path.relative_to(REPO_ROOT))
            for path in (REPO_ROOT / "backend").rglob("*.py")
            if "/tests/" not in str(path)
            and (
                "snapshot_verified_moneyline_phase_authority" in path.read_text()
                or "verify_source_bytes=False" in path.read_text()
            )
        )
        self.assertEqual(
            users,
            [
                "backend/app/services/mlb/public_game_prediction_service.py",
                "backend/mlb/public_game_predictions/phase_gating_v1.py",
            ],
        )


if __name__ == "__main__":
    unittest.main()
