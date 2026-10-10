import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from unittest.mock import MagicMock

from backend.nhl.scripts import load_sog_predictions_denali
from backend.nhl import cli
from backend.nhl.scripts.select_nhl_points_saves_8rain_candidates import select_raw_sog
from backend.nhl.scripts.score_sog_poisson_baseline import (
    _poisson_tail,
    main,
    score_predictions,
)


def _features() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "player_id": 101, "game_id": 1, "game_date": "2026-10-01", "season": 2026,
            "d10_sog_per60": 6.0, "d20_sog_per60": 5.0, "d5_sog_per60": 4.0,
            "d10_toi_min_avg": None, "d20_toi_min_avg": None, "d5_toi_min_avg": None,
            "szn_toi_per_game_5on5": None, "szn_toi_per_game_pp": None,
            "season_5on5_icetime_per_game": None, "season_5on4_icetime_per_game": None,
        },
        {
            "player_id": 102, "game_id": 1, "game_date": "2026-10-01", "season": 2026,
            "d10_sog_per60": 6.0, "d20_sog_per60": 5.0, "d5_sog_per60": 4.0,
            "d10_toi_min_avg": 15.0, "d20_toi_min_avg": 14.0, "d5_toi_min_avg": 13.0,
        },
        {
            "player_id": 103, "game_id": 1, "game_date": "2026-10-01", "season": 2026,
            "d10_sog_per60": 0.0, "d20_sog_per60": 0.0, "d5_sog_per60": 0.0,
            "d10_toi_min_avg": 16.78, "d20_toi_min_avg": 15.0, "d5_toi_min_avg": 14.0,
        },
        {
            "player_id": 104, "game_id": 1, "game_date": "2026-10-01", "season": 2026,
            "d10_sog_per60": None, "d20_sog_per60": None, "d5_sog_per60": None,
            "d10_toi_min_avg": 15.0, "d20_toi_min_avg": 14.0, "d5_toi_min_avg": 13.0,
        },
    ])


class ScoreSogPoissonBaselineTests(unittest.TestCase):
    def test_missing_season_toi_does_not_block_scoreable_rows(self):
        frame = pd.DataFrame([
            {
                "player_id": player_id, "game_id": 1, "game_date": "2026-10-01",
                "d10_sog_per60": 6.0, "d10_toi_min_avg": 15.0,
                "szn_toi_per_game_5on5": None,
                "season_5on5_icetime_per_game": None,
            }
            for player_id in range(100, 110)
        ])
        scored, unscored = score_predictions(frame)
        self.assertEqual(len(scored), 10)
        self.assertTrue(unscored.empty)

    def test_poisson_feature_export_skips_ordinal_pairings_veto(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "features.csv"

            def fake_psql(*args, **kwargs):
                kwargs["stdout"].write(
                    "player_id,game_id,game_date,diagnostic\n"
                    "101,1,2026-10-01," + ("x" * 220) + "\n"
                )

            with patch("backend.nhl.cli.sp.run", side_effect=fake_psql) as mocked:
                cli.export_sog_denali_features(
                    "postgresql://unused", "2026-10-01", output,
                    require_pairings_coverage=False,
                )
            self.assertEqual(mocked.call_count, 1)
            self.assertEqual(len(pd.read_csv(output)), 1)

    def test_missing_toi_is_unscored_and_never_emits_probabilities(self):
        scored, unscored = score_predictions(_features(), source_run_id="run-1")
        self.assertNotIn(101, set(scored.player_id))
        missing = unscored[unscored.player_id.eq(101)]
        self.assertEqual(list(missing.line), [1.5, 2.5, 3.5])
        self.assertEqual(set(missing.scoring_status), {"UNSCORED_MISSING_EXPOSURE"})
        self.assertTrue(missing.expected_sog.isna().all())
        self.assertFalse(any(column.startswith("p_over") or column.startswith("p_under") for column in unscored.columns))
        self.assertEqual(set(missing.source_run_id), {"run-1"})
        self.assertTrue(missing.d10_toi_min_avg.isna().all())

    def test_positive_rate_and_valid_toi_keep_baseline_probabilities(self):
        scored, _ = score_predictions(_features())
        row = scored.loc[scored.player_id.eq(102)].iloc[0]
        lam = 6.0 * 15.0 / 60.0
        self.assertEqual(row.expected_sog, lam)
        self.assertEqual(row.p_over_1_5, _poisson_tail(lam, 2))
        self.assertEqual(row.p_over_2_5, _poisson_tail(lam, 3))
        self.assertEqual(row.p_over_3_5, _poisson_tail(lam, 4))

    def test_valid_zero_rate_and_exposure_preserve_zero_lambda(self):
        scored, _ = score_predictions(_features())
        row = scored.loc[scored.player_id.eq(103)].iloc[0]
        self.assertEqual(row.expected_sog, 0.0)
        self.assertEqual(row.p_over_1_5, 0.0)
        self.assertEqual(row.p_over_2_5, 0.0)
        self.assertEqual(row.p_over_3_5, 0.0)

    def test_missing_rate_is_separately_unscored(self):
        scored, unscored = score_predictions(_features())
        self.assertNotIn(104, set(scored.player_id))
        self.assertEqual(
            set(unscored.loc[unscored.player_id.eq(104), "scoring_status"]),
            {"UNSCORED_MISSING_RATE"},
        )

    def test_only_scoreable_population_reaches_normal_wide_output(self):
        scored, unscored = score_predictions(_features())
        self.assertEqual(set(scored.player_id), {102, 103})
        self.assertNotIn(101, set(scored.player_id))
        self.assertNotIn(104, set(scored.player_id))
        self.assertTrue(scored[["p_over_1_5", "p_over_2_5", "p_over_3_5"]].notna().all().all())
        # Loader and raw selector consume the regular prediction CSV; unscored
        # records live only in the separate line-grain audit CSV.
        self.assertEqual(len(unscored), 6)

    def test_scored_probabilities_preserve_existing_range_and_order_contract(self):
        scored, _ = score_predictions(_features())
        pcols = ["p_over_1_5", "p_over_2_5", "p_over_3_5"]
        self.assertTrue(scored[pcols].apply(lambda column: column.between(0, 1)).all().all())
        self.assertTrue((scored.p_over_1_5 >= scored.p_over_2_5).all())
        self.assertTrue((scored.p_over_2_5 >= scored.p_over_3_5).all())
        self.assertTrue((scored[["p_0_1", "p_2", "p_3", "p_4p"]].sum(axis=1).sub(1).abs() < 1e-12).all())

    def test_cli_writes_separate_unscored_ledger(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "features.csv"
            names = root / "names.csv"
            output = root / "run-abc" / "predictions.csv"
            skipped = root / "run-abc" / "unscored.csv"
            source.parent.mkdir(parents=True, exist_ok=True)
            _features().to_csv(source, index=False)
            pd.DataFrame([{"player_id": 101, "game_id": 1, "full_name": "Missing TOI Player", "team_code": "AAA"}]).to_csv(names, index=False)
            with patch("sys.argv", ["score", "--in", str(source), "--out", str(output), "--unscored-out", str(skipped), "--names", str(names)]):
                main()
            scored_file = pd.read_csv(output)
            skipped_file = pd.read_csv(skipped)
            self.assertEqual(set(scored_file.player_id), {102, 103})
            self.assertEqual(len(skipped_file), 6)
            self.assertEqual(set(skipped_file.loc[skipped_file.player_id.eq(101), "scoring_status"]), {"UNSCORED_MISSING_EXPOSURE"})
            self.assertEqual(set(skipped_file.loc[skipped_file.player_id.eq(101), "player_name"]), {"Missing TOI Player"})
            self.assertEqual(set(skipped_file.loc[skipped_file.player_id.eq(101), "player_team_code"]), {"AAA"})
            self.assertEqual(set(skipped_file.source_run_id), {"run-abc"})

    def test_prediction_loader_receives_only_scoreable_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            pred_path = Path(temporary) / "predictions.csv"
            scored, _ = score_predictions(_features())
            scored.to_csv(pred_path, index=False)
            connection = MagicMock()
            cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
            with patch("sys.argv", [
                "load", "--pred-csv", str(pred_path), "--db-url", "postgresql://local/test",
                "--parent-run-id", "daily-run-1",
            ]), patch.object(load_sog_predictions_denali.psycopg, "connect", return_value=connection):
                load_sog_predictions_denali.main()
            rows = cursor.executemany.call_args.args[1]
            self.assertEqual(len(rows), 6)
            self.assertEqual({row[0] for row in rows}, {102, 103})
            self.assertNotIn(101, {row[0] for row in rows})

    def test_prediction_loader_uses_model_aware_run_scoped_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            pred_path = Path(temporary) / "predictions.csv"
            scored, _ = score_predictions(_features())
            scored.to_csv(pred_path, index=False)
            connection = MagicMock()
            cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
            with patch("sys.argv", [
                "load", "--pred-csv", str(pred_path), "--db-url", "postgresql://local/test",
                "--model-family", "poisson_baseline", "--model-version", "baseline_v1",
                "--feature-hash", "poisson_baseline_v1", "--parent-run-id", "daily-run-1",
            ]), patch.object(load_sog_predictions_denali.psycopg, "connect", return_value=connection):
                load_sog_predictions_denali.main()
            sql, rows = cursor.executemany.call_args.args
            self.assertIn("ON CONFLICT (prop, player_id, game_id, line, feature_hash)", sql)
            self.assertNotIn("ON CONFLICT (player_id, game_id, prop, line)", sql)
            self.assertEqual(len(rows), 6)
            self.assertEqual({row[7] for row in rows}, {
                "poisson_baseline_v1:model:poisson_baseline:version:baseline_v1:run:daily-run-1"
            })
            self.assertIn('"parent_daily_run_id": "daily-run-1"', rows[0][8])

    def test_raw_8rain_selector_only_sees_scoreable_population(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pred_path = root / "predictions.csv"
            market_path = root / "market.csv"
            scored, _ = score_predictions(_features())
            scored.to_csv(pred_path, index=False)
            pd.DataFrame(columns=["player_id", "game_id", "line", "p_over"]).to_csv(market_path, index=False)
            names = pd.DataFrame([
                {"player_id": 102, "game_id": 1, "full_name": "Scorable Player", "team_code": "AAA"},
                {"player_id": 103, "game_id": 1, "full_name": "Zero Rate Player", "team_code": "AAA"},
            ])
            schedule = pd.DataFrame([{
                "game_id": 1, "game_date": "2026-10-01", "home_team": "AAA", "away_team": "BBB",
            }])
            decisions, _, summary = select_raw_sog(
                pred_path, market_path, names, schedule, slate_date="2026-10-01",
                source_sha256="test-hash", observation_id="obs", observation_manifest_sha256="manifest",
                capture_timestamp_utc="2026-10-01T00:00:00Z",
                team_map={"AAA": "AAA", "BBB": "BBB"},
                player_map={("scorable player", "AAA"): "scorable-player", ("zero rate player", "AAA"): "zero-rate-player"},
                unique_name_map={}, ambiguous_team_keys=set(), ambiguous_names=set(),
                allowed_bets={"shots_on_goal": {"over", "under"}}, parent_run_id="run-1",
            )
            self.assertNotIn(101, set(decisions.player_id))
            self.assertEqual(summary["valid_model_predictions"], 6)
            self.assertEqual(summary["win_percent_unrepresentable"], 3)
            self.assertEqual(set(decisions.loc[decisions.raw_export_status.eq("WIN_PERCENT_UNREPRESENTABLE"), "player_id"]), {103})


if __name__ == "__main__":
    unittest.main()
