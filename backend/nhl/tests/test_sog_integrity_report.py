from __future__ import annotations

import unittest
from unittest.mock import patch

from backend.nhl.scripts.sog_integrity_report import (
    db_sog_contract_check,
    eligible_game_filter_sql,
    parse_game_ids,
)
from backend.nhl.cli import _sog_integrity_eligible_game_args


class SogIntegrityReportGameScopeTests(unittest.TestCase):
    def test_omitted_scope_preserves_standalone_report_behavior(self):
        self.assertIsNone(parse_game_ids(None))
        self.assertEqual(eligible_game_filter_sql(None), "")

    def test_explicit_daily_scope_includes_only_pregame_games(self):
        eligible = parse_game_ids("2026020134,2026020131,2026020134")
        self.assertEqual(eligible, [2026020131, 2026020134])
        self.assertEqual(
            eligible_game_filter_sql(eligible),
            " AND game_id IN (2026020131, 2026020134)",
        )

    def test_prediction_contract_query_uses_the_daily_eligibility_scope(self):
        with patch(
            "backend.nhl.scripts.sog_integrity_report.run_psql",
            return_value="181|181|579|0|{}",
        ) as run_psql, patch("builtins.print"):
            db_sog_contract_check(
                "restricted-test-dsn", "2026-10-17",
                eligible_game_ids=[2026020131, 2026020134],
            )

        sql = run_psql.call_args.args[1]
        self.assertIn("AND v.game_id IN (2026020131, 2026020134)", sql)
        self.assertNotIn("2026020121", sql)
        self.assertNotIn("2026020130", sql)

    def test_empty_daily_eligibility_scope_makes_contract_population_empty(self):
        with patch(
            "backend.nhl.scripts.sog_integrity_report.run_psql",
            return_value="0|0|579|0|{}",
        ) as run_psql, patch("builtins.print"):
            db_sog_contract_check(
                "restricted-test-dsn", "2026-10-17", eligible_game_ids=[]
            )
        self.assertIn("AND FALSE", run_psql.call_args.args[1])

    def test_explicit_empty_scope_checks_no_game_rows(self):
        self.assertEqual(parse_game_ids(""), [])
        self.assertEqual(eligible_game_filter_sql([]), " AND FALSE")

    def test_malformed_or_nonpositive_game_ids_are_rejected(self):
        for value in ("2026020131,not-a-game", "0", "-1"):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "ELIGIBLE_GAME_IDS_INVALID"
            ):
                eligible_game_filter_sql(parse_game_ids(value))

    def test_daily_cli_forwards_exact_pregame_game_scope(self):
        self.assertEqual(
            _sog_integrity_eligible_game_args({
                "eligible_pregame_game_ids": [2026020131, 2026020134],
            }),
            ["--eligible-game-ids", "2026020131,2026020134"],
        )

    def test_daily_cli_forwards_an_explicit_empty_scope(self):
        self.assertEqual(
            _sog_integrity_eligible_game_args({"eligible_pregame_game_ids": []}),
            ["--eligible-game-ids", ""],
        )

    def test_daily_cli_fails_closed_without_run_eligibility_evidence(self):
        with self.assertRaisesRegex(
            RuntimeError, "SOG_ELIGIBLE_GAME_IDS_MISSING_FOR_INTEGRITY"
        ):
            _sog_integrity_eligible_game_args({})


if __name__ == "__main__":
    unittest.main()
