"""Restricted routes for the PA refresh, recovery, and workspace staging writers."""

from __future__ import annotations

import os
import re
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
MAKEFILE = (ROOT / "Makefile").read_text(encoding="utf-8")
RESEARCH_PREFIX = 'MLB_RESEARCH_DATABASE_URL="$${MLB_RESEARCH_DATABASE_URL:-$${MLB_MONEYLINE_AUDIT_DATABASE_URL:?MLB research read URL is required}}" '


def _target_body(name: str) -> str:
    match = re.search(rf"(?m)^{re.escape(name)}:", MAKEFILE)
    assert match is not None, name
    following = re.search(r"(?m)^[^\s#][^:\n]*:", MAKEFILE[match.end():])
    return MAKEFILE[match.start(): match.end() + following.start() if following else len(MAKEFILE)]


class MakeRecipeRouteTests(unittest.TestCase):
    def test_every_daily_check_reader_command_sets_the_research_url(self) -> None:
        readers = (
            "check_mlb_finalized_training_data", "check_mlb_rolling_integrity",
            "validate_mlb_stat_derived_recent", "check_mlb_pa_foundation_health",
            "hydrate_mlb_pa_foundation_context", "hydrate_expanded_o15_context",
            "run_mlb_hits_o15_review_board", "build_mlb_reconcile_rows", "report_mlb_daily_ops_brief",
        )
        for reader in readers:
            lines = [line for line in MAKEFILE.splitlines() if f"backend/mlb/scripts/{reader}.py" in line]
            with self.subTest(reader=reader):
                self.assertTrue(lines)
                for line in lines:
                    self.assertTrue(line.startswith("\t" + RESEARCH_PREFIX + "$(VENV_PY) "), line[:120])
                    self.assertNotIn("MLB_USE_RESTRICTED_INGEST", line)

    def test_writer_recipes_use_restricted_ingest_and_no_research_url(self) -> None:
        cases = {
            "mlb-refresh-player-pa": "MLB_USE_RESTRICTED_INGEST=1 $(VENV_PY) backend/mlb/scripts/backfill_mlb_player_pa.py",
            "mlb-stat-derived-backfill": "MLB_USE_RESTRICTED_INGEST=1 $(MAKE) mlb-insert-stat-derived",
            "mlb-player-stats-recover-date": "MLB_USE_RESTRICTED_INGEST=1 $(VENV_PY) -u -m backend.mlb.scripts.player_stats_game_completeness --mode recover-date",
        }
        for target, command in cases.items():
            body = _target_body(target)
            with self.subTest(target=target):
                self.assertIn("\t" + command, body)
                self.assertNotIn("MLB_RESEARCH_DATABASE_URL", body)
        # The follow-on check in the backfill stays a reader, not a writer.
        self.assertIn("\t$(MAKE) mlb-check-stat-derived", _target_body("mlb-stat-derived-backfill"))


class RecoveryRepairWriteTests(unittest.TestCase):
    def _row(self) -> dict:
        return {
            "player_id": 7, "game_id": 9, "game_date": "2026-09-20", "team": "NYY", "opponent": "BOS",
            "is_home": True, "position": "CF", "at_bats": 4, "hits": 1, "total_bases": 1, "rbis": 0,
            "runs_scored": 0, "strikeouts_batting": 1, "walks": 0, "singles": 1, "doubles": 0, "triples": 0,
            "home_runs": 0, "stolen_bases": 0, "strikeouts_pitching": 0, "walks_allowed": 0, "hits_allowed": 0,
            "outs_recorded": 0, "earned_runs": 0, "is_starter": 0, "plate_appearances": 4, "hit_by_pitch": 0,
            "sacrifice_flies": 0, "sacrifice_hits": 0, "catcher_interference": 0, "pa_source": "statsapi",
        }

    def test_repair_has_no_raw_player_stats_insert(self) -> None:
        source = (ROOT / "backend/mlb/scripts/player_stats_game_completeness.py").read_text(encoding="utf-8")
        self.assertNotIn("INSERT INTO mlb.player_stats", source)
        self.assertIn("_insert_missing_participant(cur,c,x['player_mlb_id'])", source)

    def test_missing_participant_uses_safe_upsert_then_pa_fill(self) -> None:
        from backend.mlb.scripts import player_stats_game_completeness as psc

        cur = MagicMock()
        cur.fetchone.side_effect = [None, {"result_count": 1}, {"result_count": 1}]
        psc._insert_missing_participant(cur, self._row(), 7)
        statements = [call.args[0] for call in cur.execute.call_args_list]
        self.assertTrue(statements[0].startswith("SELECT 1 FROM mlb.player_stats"))
        self.assertIn("mlb.safe_upsert_player_stats(%s::jsonb)", statements[1])
        self.assertIn("mlb.safe_fill_player_stats_pa(", statements[2])
        payload = cur.execute.call_args_list[1].args[1][0]
        self.assertNotIn("pa_source", payload)
        self.assertNotIn("hit_by_pitch", payload)
        self.assertEqual(cur.execute.call_args_list[2].args[1], (7, 9, 4, 0, 0, 0, 0, "statsapi"))

    def test_existing_or_unwritten_row_fails_closed(self) -> None:
        from backend.mlb.scripts import player_stats_game_completeness as psc

        cur = MagicMock()
        cur.fetchone.side_effect = [(1,)]
        with self.assertRaisesRegex(RuntimeError, "CONCURRENT_OR_CONFLICTING_ROW"):
            psc._insert_missing_participant(cur, self._row(), 7)
        cur = MagicMock()
        cur.fetchone.side_effect = [None, {"result_count": 0}]
        with self.assertRaisesRegex(RuntimeError, "CONCURRENT_OR_CONFLICTING_ROW"):
            psc._insert_missing_participant(cur, self._row(), 7)
        self.assertEqual(cur.execute.call_count, 2)


class WorkspaceStagingRouteTests(unittest.TestCase):
    def _frames(self):
        from backend.mlb.scripts import build_mlb_today_workspace as w

        def frame(columns):
            return pd.DataFrame([{c: ("2026-10-18" if c == "slate_date" else None) for c in columns}])

        return w, frame(w.TODAY_SLATE_ROW_COLUMNS), frame(w.TODAY_WIDE_ROW_COLUMNS), frame(w.TODAY_ODDS_BOOK_ROW_COLUMNS)

    def test_restricted_mode_skips_ddl_and_calls_the_fixed_function(self) -> None:
        w, slate, wide, odds = self._frames()
        conn = MagicMock()
        cur = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
        conn.__enter__.return_value = conn
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = {"slate_rows_written": 1, "wide_rows_written": 1, "odds_rows_written": 1}
        with patch.object(w, "mlb_ingest_restricted_mode_enabled", return_value=True), \
             patch.object(w, "pg_connect", return_value=conn) as connect:
            w._ensure_stage_tables()
            connect.assert_not_called()
            result = w._replace_date_rows(slate_date="2026-10-18", slate_rows=slate, wide_rows=wide, odds_rows=odds)
        self.assertEqual(result, {"today_slate_rows": 1, "today_wide_rows": 1, "today_odds_book_rows": 1})
        self.assertEqual(cur.execute.call_count, 1)
        sql, params = cur.execute.call_args.args
        self.assertIn("mlb.replace_today_workspace_staging(", sql)
        self.assertNotRegex(sql, r"\b(?:DELETE|INSERT)\b")
        self.assertEqual(params[0], "2026-10-18")
        conn.commit.assert_called_once()

    def test_restricted_count_mismatch_fails(self) -> None:
        w, slate, wide, odds = self._frames()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (1, 0, 1)
        with patch.object(w, "mlb_ingest_restricted_mode_enabled", return_value=True), \
             patch.object(w, "pg_connect", return_value=conn):
            with self.assertRaisesRegex(RuntimeError, "row counts mismatch"):
                w._replace_date_rows(slate_date="2026-10-18", slate_rows=slate, wide_rows=wide, odds_rows=odds)

    def test_owner_route_is_unchanged_without_opt_in(self) -> None:
        w, *_ = self._frames()
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(w.mlb_ingest_restricted_mode_enabled())


if __name__ == "__main__":
    unittest.main()
