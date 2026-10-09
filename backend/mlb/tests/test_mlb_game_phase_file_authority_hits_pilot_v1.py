from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.mlb.scripts.evaluate_hits_model_candidates import (
    _apply_regular_season_authority,
    _score_all_rows_for_model,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    DEFAULT_PROPOSAL_PATH,
    DEFAULT_SOURCE_MANIFEST_PATH,
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityMetadata,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
    validate_proposal_records,
)


class StubAuthority(CanonicalGamePhaseAuthority):
    def __init__(
        self,
        metadata: GamePhaseAuthorityMetadata,
        records: dict[int, GamePhaseAuthorityRecord] | None = None,
        errors: dict[int, str] | None = None,
    ) -> None:
        self._metadata = metadata
        self._records = records or {}
        self._errors = errors or {}

    @property
    def metadata(self) -> GamePhaseAuthorityMetadata:
        return self._metadata

    def lookup_exact(self, game_pk: object) -> GamePhaseAuthorityRecord:
        if game_pk is None:
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
        exact = int(game_pk)
        if exact in self._errors:
            raise GamePhaseAuthorityError(self._errors[exact], game_pk=exact)
        if exact not in self._records:
            raise GamePhaseAuthorityError("GAME_PHASE_ABSENT", game_pk=exact)
        return self._records[exact]


def proposal_row(game_pk: int, raw_type: str) -> dict[str, object]:
    specs = {
        "R": ("REGULAR_SEASON", None, "MLB_2026_REGULAR_SEASON", "Regular Season"),
        "W": ("POSTSEASON", "WORLD_SERIES", "MLB_2026_POSTSEASON", "World Series"),
        "A": (None, None, None, "All-Star Game"),
    }
    phase, postseason_round, season_name, source_round = specs.get(
        raw_type,
        ("REGULAR_SEASON", None, "MLB_2026_REGULAR_SEASON", "Unknown"),
    )
    return {
        "game_pk": game_pk,
        "source_season": 2026,
        "source_game_type": raw_type,
        "season_phase": phase,
        "postseason_round": postseason_round,
        "season_name": season_name,
        "source_round": source_round,
        "schedule_relationships": {},
        "game_type_source_path": "fixture.json",
        "game_type_source_sha256": "a" * 64,
        "source_paths": ["fixture.json"],
        "source_hashes": ["a" * 64],
        "phase_decision": (
            "CLASSIFIED_FROM_AUTHORITATIVE_SOURCE_TYPE"
            if phase is not None
            else "SPECIAL_GAME_EXCLUDED_FAIL_CLOSED"
        ),
    }


class GamePhaseFileAuthorityHitsPilotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.authority = HashedProposalAuthority()
        cls.regular = cls.authority.lookup_exact(822744)

    def test_valid_regular_season_admission(self) -> None:
        frame = pd.DataFrame(
            [{"id": "regular", "game_id": 822744, "legacy_source_game_type": None}]
        )
        admitted, report = _apply_regular_season_authority(
            frame,
            authority=self.authority,
        )
        self.assertEqual(list(admitted["id"]), ["regular"])
        self.assertEqual(report["decision_row_counts"]["ADMITTED_REGULAR_SEASON"], 1)
        self.assertEqual(report["blocked_row_count"], 0)

    def test_preseason_excluded_from_regular_evaluation(self) -> None:
        frame = pd.DataFrame(
            [{"id": "preseason", "game_id": 831427, "legacy_source_game_type": None}]
        )
        admitted, report = _apply_regular_season_authority(
            frame,
            authority=self.authority,
        )
        self.assertTrue(admitted.empty)
        self.assertEqual(report["decision_row_counts"]["EXCLUDED_PRESEASON"], 1)
        self.assertEqual(report["newly_excluded_or_blocked_game_pks"], [831427])

    def test_postseason_excluded_from_regular_evaluation(self) -> None:
        postseason = replace(
            self.regular,
            game_pk=900001,
            source_game_type="W",
            season_phase="POSTSEASON",
            postseason_round="WORLD_SERIES",
            season_name="MLB_2026_POSTSEASON",
            source_round="World Series",
        )
        authority = StubAuthority(self.authority.metadata, {900001: postseason})
        admitted, report = _apply_regular_season_authority(
            pd.DataFrame(
                [{"id": "post", "game_id": 900001, "legacy_source_game_type": None}]
            ),
            authority=authority,
        )
        self.assertTrue(admitted.empty)
        self.assertEqual(report["decision_row_counts"]["EXCLUDED_POSTSEASON"], 1)

    def test_missing_game_pk_blocks(self) -> None:
        _, report = _apply_regular_season_authority(
            pd.DataFrame(
                [{"id": "missing", "game_id": None, "legacy_source_game_type": None}]
            ),
            authority=self.authority,
        )
        self.assertEqual(report["decision_row_counts"]["BLOCKED_MISSING_GAME_PK"], 1)
        self.assertEqual(report["blocked_row_count"], 1)

    def test_absent_authority_blocks(self) -> None:
        _, report = _apply_regular_season_authority(
            pd.DataFrame(
                [{"id": "absent", "game_id": 999999999, "legacy_source_game_type": None}]
            ),
            authority=self.authority,
        )
        self.assertEqual(report["decision_row_counts"]["BLOCKED_ABSENT_AUTHORITY"], 1)

    def test_proposal_hash_mismatch_blocks_before_parse(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "proposal.jsonl"
            path.write_bytes(DEFAULT_PROPOSAL_PATH.read_bytes() + b"\n")
            with self.assertRaisesRegex(
                GamePhaseAuthorityError,
                "GAME_PHASE_PROPOSAL_HASH_MISMATCH",
            ):
                HashedProposalAuthority(proposal_path=path)

    def test_source_manifest_hash_mismatch_blocks_before_parse(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.jsonl"
            path.write_bytes(DEFAULT_SOURCE_MANIFEST_PATH.read_bytes() + b"\n")
            with self.assertRaisesRegex(
                GamePhaseAuthorityError,
                "GAME_PHASE_SOURCE_MANIFEST_HASH_MISMATCH",
            ):
                HashedProposalAuthority(source_manifest_path=path)

    def test_duplicate_and_conflicting_authority_block(self) -> None:
        regular = proposal_row(1, "R")
        with self.assertRaisesRegex(
            GamePhaseAuthorityError,
            "GAME_PHASE_PROPOSAL_DUPLICATE_IDENTITY",
        ):
            validate_proposal_records(
                [regular, dict(regular)],
                source_hash_by_path={"fixture.json": "a" * 64},
            )
        with self.assertRaisesRegex(
            GamePhaseAuthorityError,
            "GAME_PHASE_PROPOSAL_CONFLICTING_IDENTITY",
        ):
            validate_proposal_records(
                [regular, proposal_row(1, "W")],
                source_hash_by_path={"fixture.json": "a" * 64},
            )

    def test_special_and_unknown_types_fail_closed(self) -> None:
        records, _ = validate_proposal_records(
            [proposal_row(2, "A")],
            source_hash_by_path={"fixture.json": "a" * 64},
        )
        special_authority = StubAuthority(
            self.authority.metadata,
            errors={2: "GAME_PHASE_SPECIAL_EXCLUDED"},
        )
        _, report = _apply_regular_season_authority(
            pd.DataFrame(
                [{"id": "special", "game_id": 2, "legacy_source_game_type": None}]
            ),
            authority=special_authority,
        )
        self.assertEqual(records[2].authority_status, "SPECIAL_EXCLUDED")
        self.assertEqual(report["decision_row_counts"]["BLOCKED_SPECIAL_TYPE"], 1)
        with self.assertRaisesRegex(
            GamePhaseAuthorityError,
            "GAME_PHASE_PROPOSAL_TYPE_UNKNOWN",
        ):
            validate_proposal_records(
                [proposal_row(3, "Z")],
                source_hash_by_path={"fixture.json": "a" * 64},
            )

    def test_conflicting_consumer_type_blocks(self) -> None:
        _, report = _apply_regular_season_authority(
            pd.DataFrame(
                [{"id": "conflict", "game_id": 822744, "legacy_source_game_type": "W"}]
            ),
            authority=self.authority,
        )
        self.assertEqual(report["decision_row_counts"]["BLOCKED_CONFLICTING_TYPE"], 1)

    def test_stale_authority_window_blocks(self) -> None:
        with self.assertRaisesRegex(
            GamePhaseAuthorityError,
            "GAME_PHASE_AUTHORITY_STALE",
        ):
            self.authority.require_supported_window("2026-02-20", "2026-10-06")
        with self.assertRaisesRegex(
            GamePhaseAuthorityError,
            "GAME_PHASE_AUTHORITY_STALE",
        ):
            self.authority.require_supported_window("2025-07-01", "2026-04-22")

    def test_repeated_load_is_deterministic(self) -> None:
        repeated = HashedProposalAuthority()
        self.assertEqual(repeated.metadata, self.authority.metadata)
        self.assertEqual(repeated.lookup_exact(822744), self.regular)

    def test_probability_and_pick_invariance_for_shared_rows(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "id": "regular",
                    "game_id": 822744,
                    "game_date": pd.Timestamp("2026-05-01"),
                    "legacy_source_game_type": None,
                    "fixture_probability": 0.61,
                    "over_under": "over",
                    "outcome": "win",
                },
                {
                    "id": "preseason",
                    "game_id": 831427,
                    "game_date": pd.Timestamp("2026-03-01"),
                    "legacy_source_game_type": None,
                    "fixture_probability": 0.49,
                    "over_under": "under",
                    "outcome": "win",
                },
            ]
        )
        admitted, report = _apply_regular_season_authority(
            frame,
            authority=self.authority,
        )

        def score_probability(*, prop_type: str, features: dict[str, object], allow_heuristic: bool) -> tuple[float, float]:
            self.assertEqual(prop_type, "hits")
            self.assertFalse(allow_heuristic)
            return float(features["fixture_probability"]), 0.5

        with tempfile.TemporaryDirectory() as directory, patch(
            "backend.mlb.scripts.evaluate_hits_model_candidates._build_features",
            side_effect=lambda row: row,
        ), patch(
            "backend.mlb.scripts.evaluate_hits_model_candidates._score_probability",
            side_effect=score_probability,
        ), patch(
            "backend.mlb.scripts.evaluate_hits_model_candidates._actual_side",
            side_effect=lambda over_under, outcome: over_under,
        ):
            before, _ = _score_all_rows_for_model(
                df_rows=frame,
                model_root=Path(directory),
                prop_type="hits",
            )
            after, _ = _score_all_rows_for_model(
                df_rows=admitted,
                model_root=Path(directory),
                prop_type="hits",
            )

        before_shared = before[before["id"] == "regular"].reset_index(drop=True)
        after_shared = after[after["id"] == "regular"].reset_index(drop=True)
        self.assertEqual(report["newly_excluded_or_blocked_game_pks"], [831427])
        self.assertEqual(before_shared.to_dict("records"), after_shared.to_dict("records"))
        self.assertEqual(float(after_shared.iloc[0]["p_over"]), 0.61)
        self.assertEqual(str(after_shared.iloc[0]["predicted_side"]), "over")


if __name__ == "__main__":
    unittest.main()
