import unittest
from unittest.mock import patch

from backend.domains.mlb.repository import player_repository as repo


class TestMlbPlayerRepository(unittest.TestCase):
    def test_lookup_player_prefers_player_ids_source(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "LAD"}]
            if "FROM mlb.model_training_props" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            out = repo.lookup_player(660271)

        self.assertIsNotNone(out)
        self.assertEqual(out["source"], "player_ids")
        self.assertEqual(out["team_abbr"], "LAD")
        self.assertEqual(out["team_id"], 119)

    def test_lookup_player_falls_back_to_training_source(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                return []
            if "FROM mlb.model_training_props" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            out = repo.lookup_player(660271)

        self.assertIsNotNone(out)
        self.assertEqual(out["source"], "model_training_props")
        self.assertEqual(out["team_abbr"], "LAD")
        self.assertEqual(out["team_id"], 119)

    def test_lookup_player_falls_back_when_player_ids_query_errors(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                raise RuntimeError("player_ids unavailable")
            if "FROM mlb.model_training_props" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            out = repo.lookup_player(660271)

        self.assertIsNotNone(out)
        self.assertEqual(out["source"], "model_training_props")
        self.assertEqual(out["team_abbr"], "LAD")
        self.assertEqual(out["team_id"], 119)

    def test_lookup_player_skips_unknown_player_ids_name(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                return [{"player_id": "660271", "player_name": "Unknown", "team": ""}]
            if "FROM mlb.model_training_props" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            out = repo.lookup_player(660271)

        self.assertIsNotNone(out)
        self.assertEqual(out["source"], "model_training_props")
        self.assertEqual(out["player_name"], "Shohei Ohtani")
        self.assertEqual(out["team_abbr"], "LAD")

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
        self.assertIn("latest_training_team", sql)
        self.assertIn("prop_source IS NULL OR prop_source NOT ILIKE 'nhl_%%'", sql)
        self.assertIn("FROM mlb.model_training_props", sql)
        self.assertEqual(captured.get("params"), (5,))

    def test_list_players_mlb_resolves_unknown_name_and_invalid_team(self):
        def _fetchall(sql, params=()):
            return [
                {
                    "player_id": "12345",
                    "player_ids_name": None,
                    "fallback_player_ids_name": "Unknown",
                    "player_ids_team": "NEU",
                    "latest_training_name": "Resolved Player",
                    "latest_training_team": "119",
                    "latest_prop_name": "Fallback Player",
                    "latest_prop_team": "GBR",
                    "last_prop_date": "2026-04-29",
                }
            ]

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            rows = repo.list_players_mlb(limit=5)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["player_name"], "Resolved Player")
        self.assertEqual(rows[0]["team"], "LAD")
        self.assertEqual(rows[0]["player_status"], "recent_mlb")

    def test_resolve_by_name_matches_numeric_team_storage(self):
        captured = {}

        def _fetchall(sql, params=()):
            captured["sql"] = sql
            captured["params"] = params
            if "FROM mlb.player_ids" in sql and "lower(player_name) = lower(%s)" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            out = repo.resolve_by_name(name="Shohei Ohtani", team_abbr="LAD")

        self.assertIsNotNone(out)
        self.assertEqual(out["player_id"], 660271)
        self.assertEqual(out["team_abbr"], "LAD")
        self.assertEqual(out["team_id"], 119)
        params = captured.get("params")
        self.assertEqual(params, ("Shohei Ohtani", "LAD", "LAD", "119"))

    def test_search_players_falls_back_to_training_rows(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                return []
            if "FROM mlb.model_training_props" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            rows = repo.search_players(q="Ohtani", limit=5)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["player_id"], 660271)
        self.assertEqual(rows[0]["team_abbr"], "LAD")
        self.assertEqual(rows[0]["source"], "model_training_props")

    def test_search_players_dedupes_player_ids_against_training_rows(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "LAD"}]
            if "FROM mlb.model_training_props" in sql:
                return [
                    {"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"},
                    {"player_id": "592450", "player_name": "Aaron Judge", "team": "NYY"},
                ]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            rows = repo.search_players(q="a", limit=5)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["player_id"], 660271)
        self.assertEqual(rows[1]["player_id"], 592450)

    def test_fetch_player_profile_rows_tolerates_partial_query_failures(self):
        calls = {"n": 0}

        def _fetchall(sql, params=()):
            calls["n"] += 1
            if "COALESCE(prop_value, line) AS prop_value" in sql:
                return [{"prop_type": "hits"}]
            if "WITH hist AS" in sql:
                raise RuntimeError("streaks unavailable")
            if "SELECT game_date, prop_type, result, outcome" in sql:
                return [{"prop_type": "hits", "outcome": "win"}]
            if "GROUP BY prop_type" in sql:
                raise RuntimeError("summary unavailable")
            if "'model_training_props'::text AS source" in sql:
                return [{"source": "model_training_props", "max_game_date": "2026-04-28"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            out = repo.fetch_player_profile_rows(player_id=660271)

        self.assertEqual(calls["n"], 5)
        self.assertEqual(out["recent_props"], [{"prop_type": "hits"}])
        self.assertEqual(out["streaks"], [])
        self.assertEqual(out["stat_derived"], [{"prop_type": "hits", "outcome": "win"}])
        self.assertEqual(out["training_summary"], [])
        self.assertEqual(out["freshness_metadata"]["source"], "model_training_props")

    def test_resolve_by_name_handles_team_alias_input(self):
        captured = {}

        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql and "lower(player_name) = lower(%s)" in sql:
                captured["params"] = params
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "ARI"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            out = repo.resolve_by_name(name="Shohei Ohtani", team_abbr="az")

        self.assertIsNotNone(out)
        self.assertEqual(out["player_id"], 660271)
        self.assertEqual(out["team_abbr"], "ARI")
        self.assertEqual(out["team_id"], 109)
        self.assertEqual(captured.get("params"), ("Shohei Ohtani", "ARI", "ARI", "109"))

    def test_search_players_uses_training_fallback_when_player_ids_query_fails(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                raise RuntimeError("player_ids unavailable")
            if "FROM mlb.model_training_props" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"}]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            rows = repo.search_players(q="Ohtani", limit=5)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["player_id"], 660271)
        self.assertEqual(rows[0]["team_abbr"], "LAD")
        self.assertEqual(rows[0]["source"], "model_training_props")

    def test_search_players_respects_limit_with_mixed_sources(self):
        def _fetchall(sql, params=()):
            if "FROM mlb.player_ids" in sql:
                return [{"player_id": "660271", "player_name": "Shohei Ohtani", "team": "LAD"}]
            if "FROM mlb.model_training_props" in sql:
                return [
                    {"player_id": "660271", "player_name": "Shohei Ohtani", "team": "119"},
                    {"player_id": "592450", "player_name": "Aaron Judge", "team": "NYY"},
                    {"player_id": "605141", "player_name": "Mookie Betts", "team": "LAD"},
                ]
            return []

        with patch.object(repo, "pg_fetchall", side_effect=_fetchall):
            rows = repo.search_players(q="a", limit=2)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["player_id"], 660271)
        self.assertEqual(rows[1]["player_id"], 592450)


if __name__ == "__main__":
    unittest.main()
