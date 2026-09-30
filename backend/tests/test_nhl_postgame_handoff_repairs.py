import json
import hashlib
import importlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl import cli
from backend.nhl.scripts import experiment_sog_market_residual_logit as sog_market


class GoalieStageDateUpsertTest(unittest.TestCase):
    def test_conflict_update_refreshes_slate_date(self):
        with patch.dict(os.environ, {
            "SUPABASE_DB_URL": "postgresql://unused",
            "SLATE_DATE": "2026-09-29",
        }):
            seed_goalie_logs_for_date = importlib.import_module(
                "backend.nhl.scripts.seed_goalie_logs_for_date")
        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

            def executemany(self, sql, rows):
                self.sql = sql
                self.rows = rows

        class Connection:
            def __init__(self):
                self.cursor_instance = Cursor()

            def cursor(self):
                return self.cursor_instance

        connection = Connection()
        count = seed_goalie_logs_for_date.upsert_rows(
            connection, [(8474593, 2026020001, "2026-09-29", 20, 22, 60.0)]
        )
        self.assertEqual(count, 1)
        self.assertIn("game_date   = EXCLUDED.game_date", connection.cursor_instance.sql)
        self.assertEqual(connection.cursor_instance.rows[0][2], "2026-09-29")


class SogReconciliationSeasonRoutingTest(unittest.TestCase):
    @staticmethod
    def feature_row(season, game_date, game_id, player_id):
        return {
            "season": season, "game_date": game_date, "game_id": game_id,
            "player_id": player_id, "player_name": "Example Skater",
            "shots_on_goal": 2, "lambda_base": 1.4,
            "d10_sog_per60": 2.0, "attempts_d10_per60": 5.0,
            "d10_toi_min_avg": 15.0, "role_pp_share": 0.2,
            "toi_trend_3v10": 0.1, "d10_toi_cv": 0.2,
            "pace_matchup_index": 1.0, "is_home": True,
        }

    def test_reconciliation_builds_combined_season_scoped_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "reconcile.csv"
            calls = []

            def fake_run(command, **kwargs):
                calls.append([str(value) for value in command])
                if "build_sog_poisson_residual_dataset.py" in str(command[1]):
                    season = int(command[command.index("--season") + 1])
                    out = Path(command[command.index("--out-csv") + 1])
                    row = self.feature_row(
                        season,
                        "2025-10-10" if season == 2025 else "2026-09-29",
                        2025020001 if season == 2025 else 2026020001,
                        8473533 if season == 2025 else 8474013,
                    )
                    pd.DataFrame([row]).to_csv(out, index=False)
                return None

            with patch.object(cli, "SOG_RECONCILE_DATASET_PATH", target), \
                    patch.object(cli, "run", side_effect=fake_run):
                cli.refresh_sog_reconcile_artifacts(to_date="2026-09-30")

            combined = pd.read_csv(target)
            self.assertEqual(set(combined.season.astype(int)), {2025, 2026})
            self.assertIn("2026-09-29", set(combined.game_date.astype(str)))
            builder_calls = [call for call in calls if "build_sog_poisson_residual_dataset.py" in call[1]]
            self.assertEqual([call[call.index("--season") + 1] for call in builder_calls], ["2025", "2026"])

    def test_season_dataset_path_does_not_overwrite_prior_season_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(cli, "SOG_RESIDUAL_DATASET_DEFAULT_CSV", Path(tmp) / "season_2025.csv"), \
                    patch.object(cli, "run") as run:
                cli.refresh_sog_residual_dataset(slate="2026-09-30")
            command = [str(value) for value in run.call_args.args[0]]
            output = Path(command[command.index("--out-csv") + 1])
            self.assertEqual(output.name, "sog_poisson_residual_dataset_season_2026.csv")

    def test_retained_market_quotes_match_feature_rows_without_duplicate_inflation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            day = root / "2026-09-29"
            day.mkdir()
            event = [{
                "bookmakers": [{"key": "betonlineag", "markets": [{
                    "key": "player_shots_on_goal", "outcomes": [{
                        "name": "Over", "description": "Example Skater",
                        "point": 1.5, "price": 110,
                    }],
                }]}],
            }]
            for name in ("odds_latest.json", "odds_latest_compatible.json"):
                (day / name).write_text(json.dumps(event))
            feature_path = root / "features.csv"
            pd.DataFrame([self.feature_row(2026, "2026-09-29", 2026020001, 8474013)]).to_csv(
                feature_path, index=False
            )
            matched = sog_market._prepare_matched(
                feature_path, root, "betonlineag", "2026-09-29", "2026-09-29"
            )
            self.assertEqual(len(matched), 1)
            self.assertEqual(int(matched.iloc[0].player_id), 8474013)
            self.assertEqual(float(matched.iloc[0].price_over), 110.0)

    def test_manifest_verified_governed_observation_quotes_join_feature_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            observation_root = root / "observations"
            day_root = observation_root / "season=2026" / "slate_date=2026-09-29"
            dataset_path = root / "features.csv"
            pd.DataFrame([self.feature_row(
                2026, "2026-09-29", 2026020001, 8474013,
            )]).to_csv(dataset_path, index=False)

            for ordinal, price in enumerate((110, 120), start=1):
                package = day_root / f"observation=fixture{ordinal}"
                package.mkdir(parents=True)
                raw = [{"bookmakers": [{"key": "betonlineag", "markets": [{
                    "key": "player_shots_on_goal", "outcomes": [{
                        "name": "Over", "description": "Example Skater",
                        "point": 1.5, "price": price,
                    }],
                }]}]}]
                (package / "raw_response.json").write_text(json.dumps(raw))
                (package / "RUN_COMPLETE.json").write_text(json.dumps({
                    "classification": "CAPTURED_NONEMPTY",
                }))
                names = ("raw_response.json", "RUN_COMPLETE.json")
                (package / "SHA256SUMS").write_text("".join(
                    f"{hashlib.sha256((package / name).read_bytes()).hexdigest()}  {name}\n"
                    for name in names
                ))

            matched = sog_market._prepare_matched(
                dataset_path, root / "compatibility_odds", "betonlineag",
                "2026-09-29", "2026-09-29", observation_root=observation_root,
            )
            self.assertEqual(len(matched), 1)
            self.assertEqual(float(matched.iloc[0].price_over), 115.0)
            self.assertEqual(int(matched.iloc[0].player_id), 8474013)


if __name__ == "__main__":
    unittest.main()
