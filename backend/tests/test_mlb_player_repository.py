import unittest
from unittest.mock import patch

from backend.domains.mlb.repository import player_repository as repo


class TestMlbPlayerRepository(unittest.TestCase):
    def test_list_players_mlb_query_includes_latest_prop_team_fallback(self):
        captured = {}

        def _fetchall(sql, params=()):
            captured["sql"] = sql
            captured["params"] = params
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            rows = repo.list_players_mlb(limit=5)

        self.assertEqual(rows, [])
        sql = captured.get("sql", "")
        self.assertIn("latest_prop_team", sql)
        self.assertIn("COALESCE(NULLIF(BTRIM(CAST(p.team AS TEXT)), ''), lt.team, lpt.team)", sql)
        self.assertIn("prop_source IS NULL OR prop_source NOT ILIKE 'nhl_%'", sql)
        self.assertEqual(captured.get("params"), (5,))


if __name__ == "__main__":
    unittest.main()

