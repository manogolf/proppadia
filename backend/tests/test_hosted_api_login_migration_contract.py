"""Static contract for the prepared hosted API login migration (not applied)."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from backend.shared.db import pg

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "backend/app/sql/migrations/20261019_hosted_api_login.sql"
ROLLBACK = ROOT / "backend/app/sql/migrations/20261019_hosted_api_login_rollback.sql"

# From docs/operations/hosted_api_database_access_inventory_2026-10-19.md.
API_READS = {
    "mlb.player_ids", "mlb.model_training_props", "mlb.player_props",
    "mlb.player_derived_stats", "mlb.prop_features_precomputed",
    "mlb.public_game_moneyline_predictions", "mlb.public_game_moneyline_outcomes",
    "mlb.today_workspace_mlb", "mlb.today_odds_book_rows", "mlb.today_market_snapshot",
    "nhl.games", "nhl.predictions", "nhl.skater_game_logs_raw", "nhl.goalie_game_logs_raw",
    "nhl.players", "nhl.teams", "nhl.roster_status",
    "nhl.training_features_goalie_saves_v2", "nhl.user_props",
}


def _sql(path: Path) -> str:
    return re.sub(r"--[^\n]*", "", path.read_text())


def _statement(sql: str, prefix: str) -> str:
    match = re.search(re.escape(prefix) + r"(.*?);", sql, re.S)
    assert match, prefix
    return match.group(1)


def _relations(text: str) -> set[str]:
    return set(re.findall(r"\b(?:mlb|nhl)\.[a-z0-9_]+", text))


class HostedApiLoginMigrationContract(unittest.TestCase):
    def setUp(self) -> None:
        self.sql = _sql(MIGRATION)
        self.rollback = _sql(ROLLBACK)

    def test_role_names_match_the_code_route(self) -> None:
        self.assertIn(f"CREATE ROLE {pg.API_ROLE} NOLOGIN", self.sql)
        self.assertIn(f"CREATE ROLE {pg.API_LOGIN} LOGIN INHERIT", self.sql)
        self.assertIn(f"GRANT {pg.API_ROLE} TO {pg.API_LOGIN};", self.sql)

    def test_select_grant_is_exactly_the_inventoried_reads(self) -> None:
        grant = _statement(self.sql, "GRANT SELECT ON")
        self.assertEqual(_relations(grant), API_READS)
        self.assertEqual(_relations(_statement(self.rollback, "REVOKE SELECT ON")), API_READS)

    def test_writes_are_limited_to_the_two_user_prop_tables(self) -> None:
        writes = re.findall(r"GRANT\s+(INSERT|UPDATE|DELETE)[^;]*?\bON\s+([a-z]+\.[a-z_]+)", self.sql)
        self.assertEqual(
            sorted(set(writes)),
            [("INSERT", "mlb.player_props"), ("INSERT", "nhl.user_props"),
             ("UPDATE", "nhl.user_props")],
        )
        self.assertIn("GRANT INSERT, DELETE ON nhl.user_props", self.sql)
        self.assertIn("GRANT UPDATE (status, outcome, updated_at) ON nhl.user_props", self.sql)
        self.assertNotRegex(self.sql, r"GRANT UPDATE ON")
        self.assertNotRegex(self.sql, r"\bTRUNCATE\b|\bEXECUTE ON\b|GRANT ALL|ALTER TABLE|CREATE TABLE")

    def test_no_owner_or_service_membership_is_granted(self) -> None:
        for role in ("postgres", "service_role", "supabase_admin", "authenticated", "anon"):
            self.assertNotRegex(self.sql, rf"GRANT\s+{role}\s+TO")
        self.assertNotRegex(self.sql, r"(?i)\bPASSWORD\b")
        self.assertIn("NOBYPASSRLS", self.sql)

    def test_runs_in_one_transaction_with_pre_and_post_checks(self) -> None:
        self.assertTrue(self.sql.lstrip().startswith("BEGIN;"))
        self.assertTrue(self.sql.rstrip().endswith("COMMIT;"))
        self.assertIn("$preflight$", self.sql)
        self.assertIn("$postcheck$", self.sql)

    def test_rollback_removes_policies_grants_and_roles(self) -> None:
        for text in (
            "DROP POLICY", "REVOKE INSERT ON mlb.player_props",
            "REVOKE INSERT, DELETE ON nhl.user_props",
            "REVOKE UPDATE (status, outcome, updated_at) ON nhl.user_props",
            f"DROP ROLE IF EXISTS {pg.API_LOGIN}", f"DROP ROLE IF EXISTS {pg.API_ROLE}",
        ):
            self.assertIn(text, self.rollback)


if __name__ == "__main__":
    unittest.main()
