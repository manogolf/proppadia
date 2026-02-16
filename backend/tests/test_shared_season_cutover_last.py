import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from backend.scripts import season_cutover_last as scl


class TestSharedSeasonCutoverLast(unittest.TestCase):
    def test_main_json_reports_regressions(self):
        with TemporaryDirectory() as td:
            path = Path(td) / "season_cutover_history.jsonl"
            path.write_text(
                "\n".join(
                    [
                        '{"captured_at":"2026-02-16T00:00:00+00:00","status":"pass","ok":true,"timezone":"America/New_York","lanes":[{"name":"market_cache_refresh","cron":"0 */8 * * *"}]}',
                        '{"captured_at":"2026-02-16T01:00:00+00:00","status":"pass","ok":true,"timezone":"UTC","lanes":[{"name":"market_cache_refresh","cron":"0 */6 * * *"}]}',
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            payload = scl._load_history(path)
            self.assertEqual(len(payload), 2)
            rows = payload
            regressions = scl._regressions(rows[0], rows[1])
            self.assertIn("timezone_changed:America/New_York->UTC", regressions)
            self.assertIn("cron_changed:market_cache_refresh:0 */8 * * *->0 */6 * * *", regressions)


if __name__ == "__main__":
    unittest.main()
