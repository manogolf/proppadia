from __future__ import annotations

import unittest

from backend.mlb.scripts.prepare_mlb_postseason_alcs_authority_v5 import validate_package


class PostseasonAlcsAuthorityV5Tests(unittest.TestCase):
    def test_governed_child_is_source_bound_before_or_after_reviewed_activation(self) -> None:
        result = validate_package()
        self.assertIn(result["decision"], {"GOVERNED_CHILD_VALID_INACTIVE", "GOVERNED_CHILD_VALID_ACTIVE"})
        self.assertEqual(result["new_game_pks"], [849816, 849817])
        self.assertEqual(result["row_count"], 2936)
        self.assertEqual(result["postseason_count"], 17)
        self.assertEqual(result["scheduled_date_through"], "2026-10-18")
        self.assertEqual(result["selection_changed"], result["decision"] == "GOVERNED_CHILD_VALID_ACTIVE")
        self.assertEqual(result["network_requests"], 0)
        self.assertEqual(result["database_requests"], 0)


if __name__ == "__main__":
    unittest.main()
