from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor

import backend.nhl.sog_hgb_count_model as shadow
from backend.nhl.daily_orchestration import LANE_NAMES
from backend.nhl.performance_summary import render_markdown, summarize_frames
from backend.nhl.sog_hgb_features import FEATURES, build, normalize_logs, position_groups

SCORER = Path(__file__).parents[1] / "scripts/score_sog_poisson_baseline.py"
SLATE = "2026-10-20"
GAME = 2026020100


def history():
    rows, gid = [], 2025020000
    for season, start in ((2025, "2025-10-10"), (2026, "2026-10-08")):
        for i in range(5):
            gid += 1
            date = (pd.Timestamp(start) + pd.Timedelta(days=2 * i)).strftime("%Y-%m-%d")
            for player, home, sog in ((1, True, i % 3 + 1), (2, False, 1)):
                rows.append({"player_id": player, "game_id": gid, "game_date": date, "season": season,
                             "game_type": "02", "is_home": home, "home_team_id": 10, "away_team_id": 20,
                             "shots_on_goal": sog, "toi_minutes": 18.0, "pp_toi_minutes": 2.0, "shot_attempts": sog * 2})
    return pd.DataFrame(rows)


POS = pd.DataFrame({"player_id": [1, 2, 3], "position": ["C", "D", "L"]})
GAMES = pd.DataFrame({"game_id": [GAME], "home_team_id": [20], "away_team_id": [10]})
PROD = pd.DataFrame({"game_id": [GAME] * 3, "player_id": [1, 2, 3], "is_home": ["f", "t", "f"],
                     "expected_sog": [2.0, 1.0, 0.7]})


class ConstantModel:
    def predict(self, x):
        return np.full(len(x), 1.75)


def build_rows(model=None, **extra):
    kwargs = dict(slate_date=SLATE, season=2026, run_id="r", feature_cutoff_utc="t",
                  canonical_game_ids=[GAME], started_excluded_game_ids=[])
    kwargs.update(extra)
    return shadow.build_predictions(history(), POS, GAMES, PROD, model or ConstantModel(), **kwargs)


class HgbCountShadowTest(unittest.TestCase):
    def test_target_features_match_history_build_for_same_game(self):
        # Score a past game as a target and compare with the features that game gets as history.
        h = normalize_logs(history())
        last = h[h.game_date.eq(h.game_date.max())]
        prior = h[h.game_date.lt(h.game_date.max())]
        targets = last[["player_id", "game_id", "game_date", "season", "is_home", "home_team_id", "away_team_id"]]
        as_target = build(prior, position_groups(POS), targets)
        as_target = as_target[as_target.is_target].set_index("player_id")[FEATURES]
        as_history = build(h, position_groups(POS)).merge(last[["player_id", "game_id"]])
        as_history = as_history.set_index("player_id")[FEATURES]
        pd.testing.assert_frame_equal(as_target.sort_index(), as_history.sort_index(), check_dtype=False)

    def test_rows_lines_fallback_and_home_parsing(self):
        rows = build_rows()
        self.assertEqual(len(rows), 9)
        players = rows.drop_duplicates("player_id").set_index("player_id")
        self.assertEqual(players.loc[1, "shadow_source"], "HGB_COUNT")
        self.assertEqual(players.loc[1, "shadow_lambda"], 1.75)
        self.assertEqual(players.loc[3, "shadow_source"], "NO_CLEAN_HISTORY_PRODUCTION_FALLBACK")
        self.assertEqual(players.loc[3, "shadow_lambda"], 0.7)
        self.assertEqual(players.loc[1, "feature_is_home"], 0)
        self.assertEqual(players.loc[2, "feature_is_home"], 1)
        self.assertEqual(players.loc[1, "feature_games_season"], 5)
        p = rows[rows.player_id.eq(1)].set_index("line").shadow_p_over
        self.assertGreater(p[1.5], p[2.5]); self.assertGreater(p[2.5], p[3.5])

    def test_rejects_late_history_unknown_game_and_bad_home_flag(self):
        late = pd.concat([history(), history().tail(1).assign(game_date=SLATE, game_id=2026029999)])
        with self.assertRaisesRegex(ValueError, "NOT_STRICTLY_PRIOR|TARGET_NOT_AFTER_HISTORY"):
            shadow.build_predictions(late, POS, GAMES, PROD, ConstantModel(), slate_date=SLATE, season=2026, run_id="r",
                                     feature_cutoff_utc="t", canonical_game_ids=[GAME])
        with self.assertRaisesRegex(ValueError, "NONCANONICAL_GAME"):
            build_rows(canonical_game_ids=[1])
        with self.assertRaisesRegex(ValueError, "IS_HOME_UNPARSEABLE"):
            shadow.build_predictions(history(), POS, GAMES, PROD.assign(is_home="maybe"), ConstantModel(),
                                     slate_date=SLATE, season=2026, run_id="r", feature_cutoff_utc="t",
                                     canonical_game_ids=[GAME])

    def _artifact(self, root: Path) -> tuple[Path, str]:
        x = np.random.default_rng(0).normal(size=(50, len(FEATURES)))
        model = HistGradientBoostingRegressor(loss="poisson", max_iter=5).fit(x, np.random.default_rng(1).poisson(1.5, 50))
        joblib.dump(model, root / "m.joblib")
        digest = hashlib.sha256((root / "m.joblib").read_bytes()).hexdigest()
        contract = root / "shadow_contract.json"
        contract.write_text(json.dumps({"fitted_artifact": "m.joblib", "fitted_artifact_sha256": digest,
                                        "feature_columns": FEATURES, "training_frame_sha256": "x"}))
        return contract, digest

    def test_artifact_checksum_guard(self):
        with tempfile.TemporaryDirectory() as td:
            contract, _ = self._artifact(Path(td))
            with self.assertRaisesRegex(ValueError, "ARTIFACT_SHA_MISMATCH"):
                shadow.load_model(contract)

    def test_capture_grade_and_summary(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            contract, digest = self._artifact(root)
            prod_path = root / "production.csv"
            PROD.to_csv(prod_path, index=False)
            with mock.patch.object(shadow, "EXPECTED_ARTIFACT_SHA256", digest):
                manifest = shadow.capture(history=history(), positions=POS, games=GAMES, production_path=prod_path,
                                          output_root=root / "captures", run_id="r", slate_date=SLATE, season=2026,
                                          feature_cutoff_utc="t", canonical_game_ids=[GAME], started_excluded_game_ids=[],
                                          scorer_path=SCORER, contract_path=contract)
            self.assertEqual(manifest["deterministic_replay"], "PASS")
            self.assertEqual(manifest["model_player_game_count"], 2)
            self.assertEqual(manifest["fitted_artifact_sha256"], digest)
            recon = root / "recon"; recon.mkdir()
            pd.DataFrame({"slate_date": [SLATE] * 3, "game_id": [GAME] * 3, "player_id": [1, 2, 3],
                          "shots_on_goal": [3, 0, 1]}).to_csv(recon / "canonical_skater_outcomes.csv", index=False)
            graded = shadow.grade_slate(slate_date=SLATE, reconciliation_package=recon, capture_root=root / "captures")
            self.assertEqual(graded["model_identity"], "NHL_SOG_COUNT_HGB_V1")
            self.assertEqual(graded["latest_pregame"]["settled_player_games"], 3)
            self.assertEqual(graded["latest_pregame"]["production_fallback_player_games"], 1)
            summary = summarize_frames(slate_date=SLATE, games=1, phase="REGULAR_SEASON", reconciliation_status="CREATED",
                                       package_identity="p", grades={}, source_artifacts={},
                                       sog_count_hgb_shadow_context=graded)
            text = render_markdown(summary)
            self.assertIn("## SOG HGB Count-Model Prospective Shadow", text)
            self.assertIn("## SOG Shrunk-Rate (K=180) Prospective Shadow", text)

    def test_daily_lane_is_registered(self):
        self.assertIn("sog_count_hgb_shadow", LANE_NAMES)

    @unittest.skipUnless(shadow.CONTRACT_PATH.is_file(), "frozen artifact is local and git-ignored")
    def test_frozen_artifact_loads(self):
        model, contract, digest = shadow.load_model()
        self.assertEqual(digest, shadow.EXPECTED_ARTIFACT_SHA256)
        self.assertEqual(contract["feature_columns"], FEATURES)


if __name__ == "__main__":
    unittest.main()
