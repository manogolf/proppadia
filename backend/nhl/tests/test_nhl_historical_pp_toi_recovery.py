from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.scripts.recover_nhl_historical_pp_toi import (
    DEFAULT_CHECKPOINT,
    DEFAULT_REUSE,
    LEGACY_TEAM_ID_TO_ABBR,
    assess_equal_team_allocation,
    boxscore_skaters,
    load_inventory,
    validate_game,
)
from backend.nhl.scripts.analyze_nhl_pp_toi_hgb_feature_exposure import replay


ROOT = Path(__file__).resolve().parents[3]
CURRENT = ROOT / "artifacts/analysis/nhl/raw_foundation_integrity_2026-10-16/current_raw_snapshot.csv"
SAMPLE_GAMES = (2023020024, 2023021169, 2024020211)


class HistoricalPpToiRecoveryTests(unittest.TestCase):
    def test_retained_official_games_reconcile_with_both_existing_methods(self):
        all_players = 0
        verified_zeros = 0
        for game_id in SAMPLE_GAMES:
            pbp = json.loads((DEFAULT_REUSE / f"{game_id}_pbp.json").read_text())
            box = json.loads((DEFAULT_REUSE / f"{game_id}_boxscore.json").read_text())
            shifts = json.loads((DEFAULT_REUSE / f"{game_id}_shiftcharts.json").read_text())
            players, summary = validate_game(game_id, pbp, box, shifts)
            self.assertEqual(summary["status"], "RECONCILED")
            self.assertEqual(len(players), 36)
            all_players += len(players)
            verified_zeros += sum(p["reconstructed_pp_toi_minutes"] == 0 for p in players.values())
            self.assertTrue(all(
                p["reconstructed_pp_toi_minutes"] == p["alternate_pp_toi_minutes"]
                for p in players.values()
            ))
        self.assertEqual(all_players, 108)
        self.assertEqual(verified_zeros, 20)

    def test_missing_shift_source_is_unresolved_not_a_zero_observation(self):
        game_id = SAMPLE_GAMES[0]
        pbp = json.loads((DEFAULT_REUSE / f"{game_id}_pbp.json").read_text())
        box = json.loads((DEFAULT_REUSE / f"{game_id}_boxscore.json").read_text())
        with self.assertRaisesRegex(ValueError, "SHIFTCHARTS_EMPTY"):
            validate_game(game_id, pbp, box, {"data": []})

    def test_team_identity_is_canonical_by_abbreviation_not_numeric_namespace_guess(self):
        raw, _games, teams, _checkpoint = load_inventory(CURRENT, DEFAULT_CHECKPOINT)
        mismatches = 0
        id_namespace_differences = 0
        side_mismatches = 0
        checked = 0
        for game_id in SAMPLE_GAMES:
            box = json.loads((DEFAULT_REUSE / f"{game_id}_boxscore.json").read_text())
            players, _summary = boxscore_skaters(box)
            for player_id, official in players.items():
                row = raw[(game_id, player_id)]
                raw_team_id = int(row["team_id"])
                raw_team = LEGACY_TEAM_ID_TO_ABBR[raw_team_id]
                checked += 1
                mismatches += raw_team != official["team_abbr"]
                id_namespace_differences += raw_team_id != official["team_id"]
                raw_home = str(row["is_home"]).lower() in ("t", "true", "1")
                side_mismatches += raw_home != official["is_home"]
        self.assertEqual(checked, 108)
        self.assertEqual(mismatches, 0)
        self.assertEqual(id_namespace_differences, 54)
        self.assertEqual(side_mismatches, 0)

    def test_equal_team_allocation_reproduces_2023021169_signature(self):
        raw, _games, _teams, _checkpoint = load_inventory(CURRENT, DEFAULT_CHECKPOINT)
        gid = 2023021169
        pbp = json.loads((DEFAULT_REUSE / f"{gid}_pbp.json").read_text())
        box = json.loads((DEFAULT_REUSE / f"{gid}_boxscore.json").read_text())
        shifts = json.loads((DEFAULT_REUSE / f"{gid}_shiftcharts.json").read_text())
        players, summary = validate_game(gid, pbp, box, shifts)
        receipt = {gid: summary}
        reconstruction = {gid: players}
        signature = assess_equal_team_allocation(receipt, reconstruction, raw)
        self.assertEqual(summary["team_pp_wall_seconds"], {"home": 591, "away": 484})
        current_by_side = {True: set(), False: set()}
        for player_id, player in players.items():
            current_by_side[player["is_home"]].add(
                float(raw[(gid, player_id)]["pp_toi_minutes"])
            )
        self.assertEqual(current_by_side[True], {0.55})
        self.assertEqual(current_by_side[False], {0.45})
        self.assertEqual(signature["exactly_uniform_current_pp_toi_groups"], 2)
        self.assertEqual(signature["uniform_groups_matching_team_pp_wall_minutes_divided_by_skater_count"], 2)

    def test_hgb_feature_replay_uses_strict_prior_identity_set_and_verified_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw_path = root / "raw.csv"
            candidate_path = root / "candidate.csv"
            targets_path = root / "targets.csv"
            raw = pd.DataFrame([
                {"game_id": 1, "player_id": 7, "game_date": "2025-08-01", "pp_toi_minutes": 1.0},
                {"game_id": 2, "player_id": 7, "game_date": "2025-09-01", "pp_toi_minutes": 2.0},
                {"game_id": 3, "player_id": 7, "game_date": "2025-09-02", "pp_toi_minutes": 88.0},
                {"game_id": 4, "player_id": 7, "game_date": "2025-09-03", "pp_toi_minutes": 99.0},
            ])
            candidate = pd.DataFrame([
                {"game_id": 1, "player_id": 7, "classification": "VERIFIED_INCORRECT",
                 "reconstructed_pp_toi_minutes": 3.0, "checkpoint_pp_toi_minutes": 1.0},
                {"game_id": 2, "player_id": 7, "classification": "VERIFIED_INCORRECT",
                 "reconstructed_pp_toi_minutes": 4.0, "checkpoint_pp_toi_minutes": 2.0},
            ])
            targets = pd.DataFrame([
                {"game_id": 1, "player_id": 7, "game_date": "2025-08-01", "canonical_season": 2025,
                 "mean_pp_toi_last10": None},
                {"game_id": 2, "player_id": 7, "game_date": "2025-09-01", "canonical_season": 2025,
                 "mean_pp_toi_last10": 1.0},
                {"game_id": 3, "player_id": 7, "game_date": "2025-09-02", "canonical_season": 2025,
                 "mean_pp_toi_last10": 1.5},
            ])
            raw.to_csv(raw_path, index=False)
            candidate.to_csv(candidate_path, index=False)
            targets.to_csv(targets_path, index=False)
            result = replay(raw_path, candidate_path, targets_path, {2025})
            target = result.loc[result.game_id.eq(3)].iloc[0]
            self.assertEqual(target.prior_player_games_in_120d, 2)
            self.assertEqual(target.mean_pp_toi_last10_before, 1.5)
            self.assertEqual(target.mean_pp_toi_last10_after_verified_source_values, 3.5)
            self.assertEqual(target.delta_minutes, 2.0)
            self.assertTrue(target.current_raw_replay_matches_frozen_feature)


if __name__ == "__main__":
    unittest.main()
