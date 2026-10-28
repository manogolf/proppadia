"""P2.37 backfill tool: the write pass runs only against the exact reviewed preview."""
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.nhl.scripts import backfill_nhl_shot_attempts_per_skater as backfill


class CommitGuardTests(unittest.TestCase):
    def test_commit_refuses_a_preview_whose_hash_differs(self):
        with tempfile.TemporaryDirectory(dir=backfill.ROOT / "tmp") as temp:
            preview = Path(temp)
            (preview / "candidates.csv").write_text(",".join(backfill.FIELDS) + "\n")
            with mock.patch.object(backfill.subprocess, "run") as run, \
                    mock.patch.object(backfill.psycopg, "connect") as connect:
                with self.assertRaisesRegex(SystemExit, "PREVIEW_SHA256_MISMATCH"):
                    backfill.commit(preview, "0" * 64)
            run.assert_not_called()
            connect.assert_not_called()

    def test_null_row_query_is_read_only_and_bounded_to_2026_regular_season(self):
        sql = backfill.NULL_ROWS_SQL
        self.assertIn("shot_attempts IS NULL", sql)
        self.assertIn("202602", sql)
        self.assertNotRegex(sql.upper(), r"\b(UPDATE|INSERT|DELETE)\b")


if __name__ == "__main__":
    unittest.main()
