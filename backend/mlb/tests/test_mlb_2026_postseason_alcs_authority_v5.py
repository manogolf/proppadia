from __future__ import annotations

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.mlb.scripts import prepare_mlb_postseason_alcs_authority_v5 as v5
from backend.mlb.scripts.prepare_mlb_postseason_alcs_authority_v5 import validate_package
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT

V6_PROPOSED_SELECTION = REPO_ROOT / "docs/contracts/mlb_2026_postseason_authority_v6/proposed_active_selection.json"


class PostseasonAlcsAuthorityV5Tests(unittest.TestCase):
    def test_governed_child_is_source_bound_before_or_after_reviewed_activation(self) -> None:
        result = validate_package()
        self.assertIn(result["decision"], {
            "GOVERNED_CHILD_VALID_INACTIVE", "GOVERNED_CHILD_VALID_ACTIVE", "GOVERNED_CHILD_VALID_SUPERSEDED",
        })
        self.assertEqual(result["new_game_pks"], [849816, 849817])
        self.assertEqual(result["row_count"], 2936)
        self.assertEqual(result["postseason_count"], 17)
        self.assertEqual(result["scheduled_date_through"], "2026-10-18")
        self.assertEqual(result["selection_changed"], result["decision"] == "GOVERNED_CHILD_VALID_ACTIVE")
        self.assertEqual(result["network_requests"], 0)
        self.assertEqual(result["database_requests"], 0)

    def test_reviewed_descendant_selection_reports_superseded(self) -> None:
        (REPO_ROOT / "tmp").mkdir(exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="authority_v5_", dir=REPO_ROOT / "tmp"))
        self.addCleanup(shutil.rmtree, root, True)
        selection = root / "active_selection.json"
        selection.write_bytes(V6_PROPOSED_SELECTION.read_bytes())
        with mock.patch.object(v5, "ACTIVE_SELECTION_PATH", selection):
            self.assertEqual(validate_package()["decision"], "GOVERNED_CHILD_VALID_SUPERSEDED")
            selection.write_bytes(b'{"descriptor_path":"x","descriptor_sha256":"' + b"0" * 64 + b'"}\n')
            with self.assertRaises(v5.PackageValidationError):
                validate_package()


if __name__ == "__main__":
    unittest.main()
