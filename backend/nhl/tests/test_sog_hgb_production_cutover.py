"""HGB SOG production cutover (2026-10-25): production file, formula shadow, identities."""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

import backend.nhl.sog_formula_shadow as formula_shadow
from backend.nhl.daily_orchestration import LANE_NAMES
from backend.nhl.performance_summary import PRODUCTION_SOG_SCORERS, render_markdown, summarize_frames
from backend.nhl.scripts.score_sog_poisson_baseline import score_predictions
from backend.nhl.sog_hgb_count_model import build_production_predictions
from backend.nhl.tests.test_sog_hgb_count_model import GAME, GAMES, POS, SLATE, ConstantModel, history

ROOT = Path(__file__).resolve().parents[3]
SCORER = ROOT / "backend/nhl/scripts/score_sog_poisson_baseline.py"


def formula_frame() -> pd.DataFrame:
    features = pd.DataFrame({
        "player_id": [1, 2, 3], "game_id": [GAME] * 3, "team_id": [10, 20, 10], "opponent_id": [20, 10, 20],
        "is_home": ["f", "t", "f"], "game_date": [SLATE] * 3, "season": [2026] * 3,
        "d10_sog_per60": [6.0, 3.0, 4.0], "d10_toi_min_avg": [18.0, 20.0, 12.0]})
    scored, _ = score_predictions(features)
    return scored.reset_index(drop=True)


class ProductionFileTest(unittest.TestCase):
    def test_layout_matches_formula_and_fallback_keeps_formula(self):
        formula = formula_frame()
        out, feats = build_production_predictions(history(), POS, GAMES, formula, ConstantModel(),
                                                  slate_date=SLATE, season=2026, canonical_game_ids=[GAME])
        self.assertEqual(list(out.columns), list(formula.columns))
        rows = out.set_index("player_id")
        self.assertEqual(rows.loc[1, "expected_sog"], 1.75)
        self.assertEqual(rows.loc[1, "poisson_source"], "hgb_count")
        self.assertEqual(rows.loc[3, "poisson_source"], "formula_fallback")
        f3 = formula.set_index("player_id").loc[3]
        self.assertAlmostEqual(rows.loc[3, "expected_sog"], f3.expected_sog)
        self.assertAlmostEqual(rows.loc[3, "p_over_1_5"], f3.p_over_1_5)
        lam = 1.75
        self.assertAlmostEqual(rows.loc[1, "p_over_1_5"], 1 - np.exp(-lam) * (1 + lam))
        self.assertAlmostEqual(rows.loc[1, "p_0_1"] + rows.loc[1, "p_2"] + rows.loc[1, "p_3"] + rows.loc[1, "p_4p"], 1.0)
        self.assertEqual(feats.poisson_source.tolist(), out.poisson_source.tolist())


class FormulaShadowTest(unittest.TestCase):
    def test_capture_grade_and_summary(self):
        formula = formula_frame()
        production, _ = build_production_predictions(history(), POS, GAMES, formula, ConstantModel(),
                                                     slate_date=SLATE, season=2026, canonical_game_ids=[GAME])
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            fp, pp = root / "formula.csv", root / "production.csv"
            formula.to_csv(fp, index=False); production.to_csv(pp, index=False)
            manifest = formula_shadow.capture(
                formula_path=fp, production_path=pp, output_root=root / "captures", run_id="r", slate_date=SLATE,
                season=2026, feature_cutoff_utc="t", canonical_game_ids=[GAME], scorer_path=SCORER,
                production_identity=("hgb_count", "NHL_SOG_COUNT_HGB_V1"))
            self.assertEqual(manifest["player_game_count"], 3)
            rows = pd.read_csv(Path(manifest["path"]) / "predictions.csv")
            p1 = rows[rows.player_id.eq(1) & rows.line.eq(1.5)].iloc[0]
            self.assertAlmostEqual(p1.shadow_p_over, formula.set_index("player_id").loc[1, "p_over_1_5"])
            self.assertAlmostEqual(p1.production_lambda, 1.75)
            recon = root / "recon"; recon.mkdir()
            pd.DataFrame({"slate_date": [SLATE] * 3, "game_id": [GAME] * 3, "player_id": [1, 2, 3],
                          "shots_on_goal": [2, 0, 1]}).to_csv(recon / "canonical_skater_outcomes.csv", index=False)
            graded = formula_shadow.grade_slate(slate_date=SLATE, reconciliation_package=recon,
                                                capture_root=root / "captures")
            self.assertEqual(graded["model_identity"], "NHL_SOG_POISSON_FORMULA_V1")
            self.assertEqual(graded["latest_pregame"]["settled_player_games"], 3)
            self.assertEqual(graded["latest_pregame"]["production_fallback_player_games"], 0)
            text = render_markdown(summarize_frames(
                slate_date=SLATE, games=1, phase="REGULAR_SEASON", reconciliation_status="CREATED",
                package_identity="p", grades={}, source_artifacts={}, sog_formula_shadow_context=graded))
            self.assertIn("## SOG Poisson Formula Shadow (former production)", text)

    def test_population_mismatch_fails_closed(self):
        formula = formula_frame()
        with self.assertRaisesRegex(ValueError, "POPULATION_MISMATCH"):
            formula_shadow.build_predictions(formula, formula.iloc[:2], slate_date=SLATE, season=2026, run_id="r",
                                             feature_cutoff_utc="t", canonical_game_ids=[GAME],
                                             production_identity=("hgb_count", "NHL_SOG_COUNT_HGB_V1"))


class IdentityTest(unittest.TestCase):
    def test_production_identities_and_lanes(self):
        self.assertEqual(PRODUCTION_SOG_SCORERS[("hgb_count", "NHL_SOG_COUNT_HGB_V1")], "backend/nhl/sog_hgb_count_model.py")
        self.assertIn(("poisson_baseline", "baseline_v1"), PRODUCTION_SOG_SCORERS)
        for path in PRODUCTION_SOG_SCORERS.values():
            self.assertTrue((ROOT / path).is_file(), path)
        self.assertIn("sog_poisson_formula_shadow", LANE_NAMES)

    def test_daily_run_defaults_to_hgb_with_formula_rollback(self):
        source = (ROOT / "backend/nhl/cli.py").read_text()
        self.assertIn('sog_scorer = (os.environ.get("NHL_SOG_SCORER") or "hgb_count").strip().lower()', source)
        self.assertIn('SOG_FORMULA_SCORERS = ("poisson_baseline", "hgb_count")', source)


if __name__ == "__main__":
    unittest.main()
