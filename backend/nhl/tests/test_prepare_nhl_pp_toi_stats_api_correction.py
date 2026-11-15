"""PP TOI correction from the NHL stats API (P2.45): proposal rules and guarded SQL shape."""
import re
import unittest
from decimal import Decimal

from backend.nhl.scripts import prepare_nhl_pp_toi_stats_api_correction as prep
from backend.nhl.source_correction_access import SourceCorrectionError, validate_correction_sql


def raw(game, player, team, pp, toi, day="2025-11-01"):
    return {"game_id": game, "player_id": player, "team_id": team, "game_date": day,
            "pp_toi_minutes": pp, "toi_minutes": toi}


def off(team, pp_s, toi_s):
    return {"team": team, "pp_seconds": pp_s, "toi_seconds": toi_s, "game_date": "2025-11-01", "source_sha256": "a" * 64}


class ProposalTests(unittest.TestCase):
    def test_minutes_round_half_up_to_two_decimals(self):
        self.assertEqual(prep.minutes(29), Decimal("0.48"))
        self.assertEqual(prep.minutes(3), Decimal("0.05"))
        self.assertEqual(prep.minutes(0), Decimal("0.00"))

    def test_rules_change_skip_and_exclude(self):
        rows = [raw(2025020001, 1, 10, Decimal("0.26"), Decimal("16.00")),   # change 0.26 -> 2.00
                raw(2025020001, 2, 10, None, Decimal("15.00")),              # NULL -> verified zero
                raw(2025020001, 3, 10, Decimal("1.00"), Decimal("15.00")),   # already correct
                raw(2025020001, 4, 10, Decimal("0.50"), Decimal("12.00")),   # TOI disagrees
                raw(2025020001, 5, 10, Decimal("0.50"), Decimal("12.00")),   # stored under another team
                raw(2025020001, 6, 10, Decimal("0.50"), Decimal("12.00")),   # no official row
                raw(2025020001, 7, 10, Decimal("0.10"), Decimal("12.00"))]
        official = {(2025020001, 1): off("DAL", 120, 960), (2025020001, 2): off("DAL", 0, 900),
                    (2025020001, 3): off("DAL", 60, 900), (2025020001, 4): off("DAL", 30, 600),
                    (2025020001, 5): off("MIN", 30, 720), (2025020001, 7): off("DAL", 0, 720),
                    (2025020001, 9): off("DAL", 10, 300)}
        proposal, counts, missing = prep.build_proposal(rows, official)
        self.assertEqual([(r["player_id"], r["from_value"], r["to_value"]) for r in proposal],
                         [(1, "0.26", "2.00"), (2, "", "0.00"), (7, "0.10", "0.00")])
        self.assertEqual(counts["already_correct"], 1)
        self.assertEqual(counts["excluded_toi_mismatch"], 1)
        self.assertEqual(counts["excluded_team_mismatch"], 1)
        self.assertEqual(counts["no_official_row"], 1)
        self.assertEqual(counts["change_from_null"], 1)
        self.assertEqual([m["player_id"] for m in missing], [9])


class SqlTests(unittest.TestCase):
    rows = [{"game_id": 2025020001, "player_id": 1, "team_id": 10, "game_date": "2025-11-01",
             "from_value": "0.26", "to_value": "2.00"},
            {"game_id": 2025020001, "player_id": 2, "team_id": 10, "game_date": "2025-11-01",
             "from_value": "", "to_value": "0.00"}]

    def test_repair_and_rollback_pass_the_executor_validator_and_pin_every_row(self):
        for reverse in (False, True):
            sql = prep.update_sql(self.rows, "2025-11", reverse=reverse)
            validate_correction_sql(sql)
            self.assertIn("THEN TRUE", sql)
            self.assertIn("c.matching_rows = 2", sql)
            self.assertIn("IS NOT DISTINCT FROM c.from_value", sql)
            self.assertEqual(len(re.findall(r"^    \(2025020001", sql, re.M)), 2)
        self.assertIn("(2025020001, 2, 10, NULL, 0.00)", prep.update_sql(self.rows, "x"))
        self.assertIn("(2025020001, 2, 10, 0.00, NULL)", prep.update_sql(self.rows, "x", reverse=True))

    def test_editor_block_wraps_the_unchanged_update_in_a_grant_and_revoke(self):
        update = prep.update_sql(self.rows, "2025-11")
        block = prep.editor_block(update, "2025-11", 2)
        self.assertIn(update.strip(), block)
        order = [block.index(s) for s in ("BEGIN;", "GRANT proppadia_nhl_source_correction TO postgres;",
                                          "SET LOCAL ROLE", "UPDATE nhl.skater_game_logs_raw", "RESET ROLE;",
                                          "REVOKE proppadia_nhl_source_correction FROM postgres;", "COMMIT;")]
        self.assertEqual(order, sorted(order))

    def test_validator_rejects_a_second_statement(self):
        with self.assertRaises(SourceCorrectionError):
            validate_correction_sql(prep.update_sql(self.rows, "x") + "DELETE FROM nhl.skater_game_logs_raw;")

    def test_batches_are_monthly_and_split_under_the_size_limit(self):
        many = [{**self.rows[0], "player_id": i} for i in range(30_000)]
        many += [{**self.rows[0], "player_id": 1, "game_date": "2025-12-02"}]
        out = prep.batches(many)
        self.assertEqual(sum(len(v) for v in out.values()), 30_001)
        self.assertIn("2025-12", out)
        self.assertTrue(all(k.startswith("2025-11-part") for k in out if k != "2025-12"))
        for name, rows in out.items():
            self.assertLessEqual(len(prep.update_sql(rows, name).encode()), prep.MAX_SQL_BYTES)


if __name__ == "__main__":
    unittest.main()
