"""V6 postseason authority child: governed, inactive, and servable once selected.

The snapshot-only tests read committed files only. Tests that re-hash the
git-ignored retained schedules skip when that evidence is absent.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from backend.mlb.scripts.prepare_mlb_postseason_authority_child_v1 import (
    CHILDREN,
    ChildValidationError,
    _descends_from,
    activate,
    rollback,
    validate,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    REPO_ROOT,
    GamePhaseAuthorityError,
    HashedProposalAuthority,
)

SPEC = CHILDREN["v6"]
SELECTION = "backend/mlb/season_transition/authority_snapshots/active_selection.json"
COMMITTED_TREES = (
    "backend/mlb/season_transition/authority_snapshots",
    "docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal",
)
RETAINED = all(
    (REPO_ROOT / row["source_path"]).is_file()
    for row in json.loads((SPEC.package_dir / "source_set.json").read_text())["sources"]
)


def _temp_root(test: unittest.TestCase) -> Path:
    (REPO_ROOT / "tmp").mkdir(exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="authority_v6_", dir=REPO_ROOT / "tmp"))
    test.addCleanup(shutil.rmtree, root, True)
    return root


class PostseasonAuthorityV6SnapshotOnlyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = _temp_root(self)
        for tree in COMMITTED_TREES:
            shutil.copytree(REPO_ROOT / tree, self.root / tree)

    def _load(self, selection: bytes) -> HashedProposalAuthority:
        (self.root / SELECTION).write_bytes(selection)
        return HashedProposalAuthority(
            root=self.root, selection_path=self.root / SELECTION, verify_source_bytes=False
        )

    def test_proposed_selection_serves_alcs_game_without_raw_sources(self) -> None:
        proposed = (SPEC.package_dir / "proposed_active_selection.json").read_bytes()
        authority = self._load(proposed)
        self.assertEqual(len(authority.records), 2954)
        record = authority.lookup_exact(849815)
        self.assertEqual(record.season_phase, "POSTSEASON")
        self.assertEqual(record.postseason_round, "LEAGUE_CHAMPIONSHIP_SERIES")
        self.assertEqual(record.source_round, "AL Championship Series")
        for pk, (game_type, round_label, start) in SPEC.additions.items():
            record = authority.lookup_exact(pk)
            self.assertEqual(
                (record.source_game_type, record.source_round, record.scheduled_start_utc),
                (game_type, round_label, start),
            )
        # Parent rows are unchanged.
        self.assertEqual(authority.lookup_exact(849817).source_round, "AL Championship Series")

    def test_parent_selection_still_withholds_alcs_game(self) -> None:
        parent = (self.root / SELECTION).read_bytes()
        if hashlib.sha256(parent).hexdigest() != SPEC.parent_selection_sha256:
            self.skipTest("V6 or a later child is active")
        with self.assertRaises(GamePhaseAuthorityError) as caught:
            self._load(parent).lookup_exact(849815)
        self.assertEqual(caught.exception.code, "GAME_PHASE_ABSENT")

    def test_tampered_child_proposal_fails_snapshot_only_read(self) -> None:
        proposal = self.root / "backend/mlb/season_transition/authority_snapshots/v6/canonical_game_phase_full_snapshot.jsonl"
        proposal.write_bytes(proposal.read_bytes().replace(b"AL Championship Series", b"AL Division Series", 1))
        with self.assertRaises(GamePhaseAuthorityError):
            self._load((SPEC.package_dir / "proposed_active_selection.json").read_bytes())

    def test_lineage_accepts_only_descendants(self) -> None:
        proposed = (SPEC.package_dir / "proposed_active_selection.json").read_bytes()
        v6_sha = json.loads(proposed)["descriptor_sha256"]
        self.assertTrue(_descends_from(proposed, SPEC.parent_descriptor_sha256))
        self.assertTrue(_descends_from(proposed, v6_sha))
        parent = json.loads(proposed)
        parent.update(descriptor_path="backend/mlb/season_transition/authority_snapshots/v5/descriptor.json",
                      descriptor_sha256=SPEC.parent_descriptor_sha256)
        self.assertFalse(_descends_from(json.dumps(parent).encode(), v6_sha))
        forged = dict(json.loads(proposed), descriptor_sha256="0" * 64)
        self.assertFalse(_descends_from(json.dumps(forged).encode(), SPEC.parent_descriptor_sha256))


@unittest.skipUnless(RETAINED, "retained raw schedule evidence is git-ignored and absent")
class PostseasonAuthorityV6RetainedEvidenceTests(unittest.TestCase):
    def test_governed_child_is_source_bound(self) -> None:
        result = validate(SPEC)
        self.assertIn(result["decision"], {
            "GOVERNED_CHILD_VALID_INACTIVE", "GOVERNED_CHILD_VALID_ACTIVE", "GOVERNED_CHILD_VALID_SUPERSEDED",
        })
        self.assertIn(849815, result["new_game_pks"])
        self.assertEqual(len(result["new_game_pks"]), 18)
        self.assertEqual(result["row_count"], 2954)
        self.assertEqual(result["postseason_count"], 35)
        self.assertEqual(result["scheduled_date_through"], "2026-10-20")
        self.assertEqual((result["network_requests"], result["database_requests"]), (0, 0))

    def test_activation_is_compare_and_swap_and_reversible(self) -> None:
        root = _temp_root(self)
        selection, record = root / "active_selection.json", root / "activation_rollback.json"
        prior = (REPO_ROOT / SELECTION).read_bytes()
        if hashlib.sha256(prior).hexdigest() != SPEC.parent_selection_sha256:
            self.skipTest("V6 or a later child is active")
        selection.write_bytes(prior)
        activate(SPEC, selection_path=selection, rollback_path=record)
        self.assertEqual(selection.read_bytes(), (SPEC.package_dir / "proposed_active_selection.json").read_bytes())
        self.assertEqual(validate(SPEC, selection_path=selection)["decision"], "GOVERNED_CHILD_VALID_ACTIVE")
        with self.assertRaises(ChildValidationError):
            activate(SPEC, selection_path=selection, rollback_path=root / "second_rollback.json")
        rollback(SPEC, selection_path=selection, rollback_path=record)
        self.assertEqual(selection.read_bytes(), prior)

    def test_activation_refuses_unreviewed_selector(self) -> None:
        root = _temp_root(self)
        selection = root / "active_selection.json"
        selection.write_bytes(b'{"descriptor_path":"x","descriptor_sha256":"' + b"0" * 64 + b'"}\n')
        with self.assertRaises(ChildValidationError):
            activate(SPEC, selection_path=selection, rollback_path=root / "activation_rollback.json")
        self.assertFalse((root / "activation_rollback.json").exists())


if __name__ == "__main__":
    unittest.main()
