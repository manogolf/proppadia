import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import cleanup_mlb_preseason_rows as cleanup


class TestSharedCleanupMlbPreseasonRows(unittest.TestCase):
    def test_main_fails_on_bad_date(self):
        out = StringIO()
        with redirect_stdout(out):
            rc = cleanup.main(["--from-date", "02/01/2026", "--to-date", "2026-03-01"])
        self.assertEqual(rc, 2)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])

    def test_main_dry_run(self):
        out = StringIO()
        with patch.object(
            cleanup,
            "_count_rows",
            return_value={"model_training_props": 10, "player_props": 2},
        ), redirect_stdout(out):
            rc = cleanup.main(["--from-date", "2026-03-01", "--to-date", "2026-03-31"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["mode"], "dry-run")
        self.assertEqual(payload["preview_counts"]["model_training_props"], 10)

    def test_main_apply(self):
        out = StringIO()
        with patch.object(
            cleanup,
            "_count_rows",
            return_value={"model_training_props": 10, "player_props": 2},
        ), patch.object(
            cleanup,
            "_delete_rows",
            return_value={"model_training_props": 9, "player_props": 2},
        ), redirect_stdout(out):
            rc = cleanup.main(
                [
                    "--from-date",
                    "2026-03-01",
                    "--to-date",
                    "2026-03-31",
                    "--apply",
                    "--include-user-added",
                ]
            )
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertEqual(payload["mode"], "apply")
        self.assertTrue(payload["include_user_added"])
        self.assertEqual(payload["deleted_counts"]["model_training_props"], 9)


if __name__ == "__main__":
    unittest.main()
