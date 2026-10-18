from __future__ import annotations

import unittest

from backend.nhl.postgame_reconcile.resilient_finalization import (
    LANE_PHASES,
    canonical_atomic_identity,
    select_latest_atomic_sources,
    validate_atomic_group,
)


STARTS = {10: "2026-10-18T22:00:00Z", 11: "2026-10-19T00:00:00Z"}


def candidate(*, key="p10", game=10, phase="MIDDAY", published="2026-10-18T19:00:00Z",
              payload="payload-a", **overrides):
    value = {
        "game_id": game, "atomic_prediction_identity": key, "eligible": True,
        "source_phase": phase, "source_path": f"/capture/{phase.lower()}",
        "source_manifest_sha256": "manifest", "observer_receipt_path": "/receipt.json",
        "observer_receipt_sha256": "receipt", "certificate_sha256": "certificate",
        "roster_source_sha256": "roster", "model_identity": "MODEL_V1",
        "feature_contract_identity": "FEATURE_V1", "feature_cutoff_timestamp_utc": published,
        "prediction_timestamp_utc": published,
        "durable_publication_timestamp_utc": published,
        "prediction_payload_sha256": payload,
    }
    value.update(overrides)
    return value


class ResilientPregameSelectionTests(unittest.TestCase):
    def select(self, candidates):
        return select_latest_atomic_sources(
            lane="POINTS", slate_date="2026-10-18", canonical_game_ids=[10, 11],
            scheduled_starts_utc=STARTS, candidates=candidates,
        )

    def test_latest_eligible_capture_selected_independent_of_phase_preference(self):
        index = self.select([
            candidate(phase="FINAL_PREGAME", published="2026-10-18T19:20:00Z", payload="a"),
            candidate(phase="REFRESH", published="2026-10-18T20:00:00Z", payload="b"),
        ])
        self.assertEqual(index["selected"][0]["source_phase"], "REFRESH")
        self.assertEqual(index["coverage_status"], "PARTIAL")

    def test_staggered_games_and_atomic_groups_select_independently(self):
        index = self.select([
            candidate(key="points-player-1-ladder", game=10,
                      published="2026-10-18T19:00:00Z"),
            candidate(key="points-player-2-ladder", game=11,
                      phase="FINAL_PREGAME", published="2026-10-18T22:20:00Z"),
        ])
        self.assertEqual({item["game_id"] for item in index["selected"]}, {10, 11})
        self.assertEqual(index["selected_atomic_prediction_count"], 2)

    def test_post_start_feature_or_publication_is_excluded(self):
        index = self.select([
            candidate(game=11, published="2026-10-19T00:00:00Z"),
        ])
        self.assertEqual(index["selected"], [])
        self.assertIn("FEATURE_CUTOFF_NOT_PREGAME", {item["reason"] for item in index["excluded"]})

    def test_missing_proof_is_explicitly_excluded(self):
        index = self.select([candidate(roster_source_sha256=None)])
        self.assertIn("SOURCE_PROOF_MISSING:roster_source_sha256",
                      {item["reason"] for item in index["excluded"]})

    def test_conflicting_tie_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "RESILIENT_FINALIZATION_CONFLICTING_TIE"):
            self.select([
                candidate(payload="first", source_path="/a"),
                candidate(payload="second", source_path="/b"),
            ])

    def test_identical_tie_uses_fixed_lexical_path(self):
        index = self.select([
            candidate(payload="same", source_path="/b"),
            candidate(payload="same", source_path="/a"),
        ])
        self.assertEqual(index["selected"][0]["source_path"], "/a")
        self.assertEqual(index["selected"][0]["tie_break"],
                         "LEXICOGRAPHIC_SOURCE_PATH_IDENTICAL_PAYLOAD")

    def test_valid_empty_coverage_is_explicit(self):
        index = self.select([])
        self.assertEqual(index["coverage_status"], "EMPTY")
        self.assertEqual(index["selected_atomic_prediction_count"], 0)

    def test_duplicate_canonical_game_ids_fail(self):
        with self.assertRaisesRegex(RuntimeError, "CANONICAL_GAME_DUPLICATE"):
            select_latest_atomic_sources(
                lane="SOG", slate_date="2026-10-18", canonical_game_ids=[10, 10],
                scheduled_starts_utc={10: STARTS[10]}, candidates=[],
            )

    def test_lane_phase_allowlists_keep_market_and_prop_sources_distinct(self):
        self.assertEqual(LANE_PHASES["MONEYLINE"],
                         frozenset({"MIDDAY", "FINAL_PREGAME", "REFRESH"}))
        self.assertEqual(LANE_PHASES["SOG"], frozenset({"MIDDAY", "FINAL_PREGAME"}))
        self.assertNotIn("REFRESH", LANE_PHASES["POINTS"])

    def test_role_model_and_atomic_group_identity(self):
        v2 = {"game_id": 10, "model_role": "SHADOW", "model_identity": "V2",
              "feature_contract_identity": "FEATURE"}
        v3 = {**v2, "model_role": "PRODUCTION", "model_identity": "V3"}
        self.assertNotEqual(canonical_atomic_identity("MONEYLINE", v2),
                            canonical_atomic_identity("MONEYLINE", v3))
        points_rows = [{"run_id": "r1", "phase": "MIDDAY",
                        "prediction_timestamp_utc": "2026-10-18T19:00:00Z",
                        "feature_cutoff_timestamp_utc": "2026-10-18T18:00:00Z",
                        "line": line} for line in (0.5, 1.5, 2.5)]
        validate_atomic_group("POINTS", points_rows)
        points_rows[-1]["run_id"] = "r2"
        with self.assertRaisesRegex(RuntimeError, "ATOMIC_GROUP_MIXED_SOURCE"):
            validate_atomic_group("POINTS", points_rows)


if __name__ == "__main__":
    unittest.main()
