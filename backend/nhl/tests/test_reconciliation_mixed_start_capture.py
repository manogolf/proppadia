"""Reconciliation keeps captured games when an earlier slate game had started.

The 2026-10-16 slate had two games, at 01:00Z and 02:00Z on 2026-10-17. A
final-pregame capture at 01:00:58Z is valid for the 02:00Z game only; the
started game is excluded and every other check stays per game.
"""
import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.postgame_reconcile.core import (
    _bind_cross_market_game_set,
    resolve_operational_sources,
)


ROOT = Path(__file__).resolve().parents[3]
OPERATIONAL = ROOT / "artifacts/operational/nhl"
SLATE = "2026-10-16"
STARTED_GAME = 2026020119
CAPTURED_GAME = 2026020120
LATE_CAPTURE = "2026-10-17T01:00:58.539000+00:00"
SOG_DAY = OPERATIONAL / "sog_prediction_only/season=2026" / f"slate_date={SLATE}"
CROSS_DAY = OPERATIONAL / "cross_market_shadow/season=2026" / f"slate_date={SLATE}"
SAVES_DAY = OPERATIONAL / "saves_prediction_only/season=2026" / f"slate_date={SLATE}"
SLATE_DIR = OPERATIONAL / "slates" / SLATE


def spine() -> pd.DataFrame:
    return pd.DataFrame({
        "game_id": [STARTED_GAME, CAPTURED_GAME],
        "scheduled_start_time_utc": ["2026-10-17T01:00:00Z", "2026-10-17T02:00:00Z"],
        "home_team_id": [1, 3], "away_team_id": [2, 4],
    })


def reseal(run: Path) -> None:
    manifest = run / "SHA256SUMS"
    names = [line.split("  ", 1)[1] for line in manifest.read_text().splitlines() if line]
    manifest.write_text("".join(
        f"{hashlib.sha256((run / name).read_bytes()).hexdigest()}  {name}\n" for name in names))


def keep_game(path: Path, game_id: int) -> None:
    frame = pd.read_csv(path, float_precision="round_trip")
    frame.loc[frame.game_id.astype(int).eq(game_id)].to_csv(path, index=False)


class CrossMarketGameSetTests(unittest.TestCase):
    def test_partial_card_binds_only_games_still_pregame_at_capture(self):
        card = spine().loc[lambda frame: frame.game_id.eq(CAPTURED_GAME)]
        _bind_cross_market_game_set(
            card, spine(), started_excluded_game_ids=[STARTED_GAME],
            observed=pd.Timestamp(LATE_CAPTURE))

    def test_dropped_game_must_be_recorded_as_started(self):
        card = spine().loc[lambda frame: frame.game_id.eq(CAPTURED_GAME)]
        with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_SOURCE_CANONICAL_GAME_SET_CONFLICT"):
            _bind_cross_market_game_set(
                card, spine(), started_excluded_game_ids=[],
                observed=pd.Timestamp(LATE_CAPTURE))

    def test_game_excluded_before_its_start_is_rejected(self):
        card = spine().loc[lambda frame: frame.game_id.eq(STARTED_GAME)]
        with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_CROSS_MARKET_STARTED_EXCLUSION_INVALID"):
            _bind_cross_market_game_set(
                card, spine(), started_excluded_game_ids=[CAPTURED_GAME],
                observed=pd.Timestamp("2026-10-16T23:45:00Z"))

    def test_card_observed_after_a_carried_game_start_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_SOURCE_OBSERVED_AFTER_GAME_START"):
            _bind_cross_market_game_set(
                spine(), spine(), started_excluded_game_ids=[],
                observed=pd.Timestamp(LATE_CAPTURE))


@unittest.skipUnless(
    all(path.is_dir() for path in (SOG_DAY, CROSS_DAY, SAVES_DAY, SLATE_DIR)),
    "retained 2026-10-16 NHL operational cards are local artifacts")
class October16LateCaptureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="nhl_mixed_start_reconcile_")
        self.root = Path(self.tmp.name)
        for source in (SOG_DAY, CROSS_DAY, SAVES_DAY, SLATE_DIR):
            shutil.copytree(source, self.root / source.relative_to(OPERATIONAL))
        self.sog = self._late_sog_card()
        self._late_cross_card()

    def tearDown(self):
        self.tmp.cleanup()

    def _late_sog_card(self) -> Path:
        day = self.root / SOG_DAY.relative_to(OPERATIONAL)
        (original,) = (day / "phase=FINAL_PREGAME").glob("run_id=*")
        run_id = "nhlsogprediction_s2026_d20261016_t20261017T010058539000Z_FINAL_PREGAME_v1"
        run = original.rename(original.parent / f"run_id={run_id}")
        predictions = pd.read_csv(run / "immutable_predictions.csv")
        predictions = predictions.loc[predictions.game_id.astype(int).eq(CAPTURED_GAME)].copy()
        predictions["prediction_timestamp_utc"] = LATE_CAPTURE
        predictions.to_csv(run / "immutable_predictions.csv", index=False)
        keep_game(run / "excluded_players.csv", CAPTURED_GAME)
        metadata = json.loads((run / "run_metadata.json").read_text())
        metadata.update(
            run_id=run_id, prediction_timestamp_utc=LATE_CAPTURE,
            prediction_rows=len(predictions),
            admitted_players=int(predictions.player_id.nunique()),
            excluded_players=len(pd.read_csv(run / "excluded_players.csv")))
        (run / "run_metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        reseal(run)
        return run

    def _late_cross_card(self) -> None:
        (run,) = (self.root / CROSS_DAY.relative_to(OPERATIONAL)
                  / "run_type=FINAL_PREGAME").glob("state=*")
        for name in ("schedule_event_identity.csv", "v2_immutable_predictions.csv",
                     "moneyline_production_predictions.csv",
                     "moneyline_shot_finishing_challenger_v3_feature_provenance.csv",
                     "puck_line_v1_immutable_predictions.csv"):
            keep_game(run / name, CAPTURED_GAME)
        status = json.loads((run / "daily_execution_status.json").read_text())
        status.update(run_timestamp_utc=LATE_CAPTURE, started_excluded_game_ids=[STARTED_GAME])
        (run / "daily_execution_status.json").write_text(
            json.dumps(status, indent=2, sort_keys=True) + "\n")
        reseal(run)

    def resolve(self):
        return resolve_operational_sources(
            slate_date=SLATE, operational_root=self.root,
            allow_missing_final_for_resilient=True)

    def test_late_capture_after_first_puck_binds_the_captured_game(self):
        binding = self.resolve()
        self.assertEqual(binding["game_ids"], [STARTED_GAME, CAPTURED_GAME])
        self.assertEqual(binding["canonical_games"], 2)
        self.assertEqual(binding["sog"]["prediction_rows"], 723)
        self.assertEqual(binding["cross_market"]["moneyline_rows"], 1)
        self.assertEqual(binding["cross_market"]["puck_line_rows"], 1)
        self.assertEqual(binding["cross_market"]["started_excluded_game_ids"], [STARTED_GAME])

    def test_sog_row_for_the_started_game_still_fails(self):
        (original,) = (SOG_DAY / "phase=FINAL_PREGAME").glob("run_id=*")
        started = pd.read_csv(original / "immutable_predictions.csv")
        started = started.loc[started.game_id.astype(int).eq(STARTED_GAME)].head(3).copy()
        started["prediction_timestamp_utc"] = LATE_CAPTURE
        predictions = pd.concat([pd.read_csv(self.sog / "immutable_predictions.csv"), started])
        predictions.to_csv(self.sog / "immutable_predictions.csv", index=False)
        metadata = json.loads((self.sog / "run_metadata.json").read_text())
        metadata.update(prediction_rows=len(predictions),
                        admitted_players=int(predictions.player_id.nunique()))
        (self.sog / "run_metadata.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n")
        reseal(self.sog)
        with self.assertRaisesRegex(RuntimeError, "POST_START_PREDICTION_PRESENTED_AS_PREGAME"):
            self.resolve()


if __name__ == "__main__":
    unittest.main()
