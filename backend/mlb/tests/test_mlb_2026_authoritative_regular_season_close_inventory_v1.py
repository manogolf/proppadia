from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.mlb.scripts import prepare_mlb_2026_regular_season_close_v1 as close_command
from backend.mlb.season_transition.game_phase_authority_v1 import (
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    CloseInventoryError,
    INVENTORY_FILENAME,
    MANIFEST_FILENAME,
    SourceObservation,
    build_authoritative_inventory,
    canonical_json_bytes,
    classify_authoritative_game,
    make_inventory_manifest,
    validate_close_inventory_package,
    validate_inventory_rows,
)


def _record(game_pk: int = 1) -> GamePhaseAuthorityRecord:
    return GamePhaseAuthorityRecord(
        game_pk=game_pk,
        source_season=2026,
        source_game_type="R",
        season_phase="REGULAR_SEASON",
        postseason_round=None,
        season_name="MLB_2026_REGULAR_SEASON",
        source_round="Regular Season",
        schedule_relationships={},
        primary_source_path="source.json",
        primary_source_sha256="a" * 64,
        source_paths=("source.json",),
        source_hashes=("a" * 64,),
        phase_decision="CLASSIFIED_FROM_AUTHORITATIVE_SOURCE_TYPE",
        authority_status="AUTHORITATIVE_UNAMBIGUOUS",
    )


def _game(
    game_pk: int = 1,
    detailed: str = "Final",
    **extra: object,
) -> dict[str, object]:
    status_by_detail = {
        "Final": ("Final", "F", "F"),
        "Completed Early": ("Final", "F", "FR"),
        "Cancelled": ("Final", "C", "C"),
        "Scheduled": ("Preview", "S", "S"),
        "Postponed": ("Final", "D", "DI"),
        "Suspended": ("Live", "U", "U"),
    }
    abstract, coded, status_code = status_by_detail.get(
        detailed, ("Mystery", "?", "?")
    )
    game: dict[str, object] = {
        "gamePk": game_pk,
        "gameType": "R",
        "season": "2026",
        "gameDate": "2026-06-01T23:00:00Z",
        "officialDate": "2026-06-01",
        "status": {
            "abstractGameState": abstract,
            "codedGameState": coded,
            "detailedState": detailed,
            "statusCode": status_code,
        },
        "teams": {
            "away": {"team": {"id": 10}, "score": 2},
            "home": {"team": {"id": 20}, "score": 4},
        },
    }
    game.update(extra)
    return game


def _obs(game: dict[str, object], suffix: str = "one") -> SourceObservation:
    return SourceObservation(f"{suffix}.json", "b" * 64, game)


class AuthoritativeRegularSeasonCloseInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.authority = HashedProposalAuthority()
        cls.rows, cls.summary = build_authoritative_inventory(authority=cls.authority)
        cls.by_game = {row["game_pk"]: row for row in cls.rows}

    def test_complete_2430_population_is_bound_to_authority(self) -> None:
        report = validate_inventory_rows(
            self.rows, self.authority.records, expected_count=2430
        )
        self.assertTrue(report["integrity_passed"])
        self.assertEqual(len(self.rows), 2430)
        self.assertEqual(self.summary["population_counts"]["total_classified_game_pks"], 2919)
        self.assertEqual(self.summary["population_counts"]["preseason_game_pks"], 489)

    def test_omitted_and_extra_game_pks_are_rejected(self) -> None:
        omitted = validate_inventory_rows(
            self.rows[:-1], self.authority.records, expected_count=2430
        )
        self.assertFalse(omitted["integrity_passed"])
        self.assertEqual(len(omitted["omitted_game_pks"]), 1)
        extra_rows = list(self.rows) + [dict(self.rows[0], game_pk=999999999)]
        extra = validate_inventory_rows(
            extra_rows, self.authority.records, expected_count=2430
        )
        self.assertFalse(extra["integrity_passed"])
        self.assertEqual(extra["extra_or_nonregular_game_pks"], [999999999])

    def test_preseason_or_postseason_contamination_is_rejected(self) -> None:
        for raw_type, phase in (("S", "PRESEASON"), ("W", "POSTSEASON")):
            rows = list(self.rows)
            rows[0] = dict(
                rows[0],
                authoritative_raw_game_type=raw_type,
                normalized_phase=phase,
            )
            report = validate_inventory_rows(
                rows, self.authority.records, expected_count=2430
            )
            self.assertFalse(report["integrity_passed"])
            self.assertTrue(
                any("PHASE_AUTHORITY_CONFLICT" in item for item in report["invalid_rows"])
            )

    def test_rescheduled_regular_game_preserves_exact_identity(self) -> None:
        row = self.by_game[823598]
        self.assertEqual(
            row["close_disposition"],
            "POSTPONED_RESCHEDULED_IDENTITY_RESOLVED",
        )
        identities = row["relationship_identities"]
        self.assertEqual(identities["game_pk"], 823598)
        self.assertEqual(identities["original"]["game_pk"], 823598)
        self.assertEqual(identities["replacement"]["game_pk"], 823598)
        self.assertEqual(row["normalized_phase"], "REGULAR_SEASON")

    def test_suspended_resumed_game_preserves_exact_identity(self) -> None:
        row = self.by_game[824912]
        self.assertEqual(
            row["close_disposition"],
            "SUSPENDED_RESUMED_IDENTITY_RESOLVED",
        )
        self.assertEqual(row["relationship_identities"]["resumed"]["game_pk"], 824912)

    def test_authoritative_cancelled_game_is_an_accepted_terminal_disposition(self) -> None:
        row = classify_authoritative_game(
            _record(), [_obs(_game(detailed="Cancelled"))]
        )
        self.assertEqual(row["close_disposition"], "AUTHORITATIVELY_CANCELLED")

    def test_missing_unknown_or_conflicting_status_is_unresolved(self) -> None:
        unknown = classify_authoritative_game(_record(), [_obs(_game(detailed="Mystery"))])
        self.assertEqual(
            unknown["close_disposition"], "UNRESOLVED_IDENTITY_OR_STATUS"
        )
        missing_game = _game(detailed="Scheduled")
        missing_game["status"] = {}
        missing = classify_authoritative_game(_record(), [_obs(missing_game)])
        self.assertEqual(
            missing["close_disposition"], "UNRESOLVED_IDENTITY_OR_STATUS"
        )
        cancelled = _obs(_game(detailed="Cancelled"), "cancelled")
        final = _obs(_game(detailed="Final"), "final")
        conflict = classify_authoritative_game(_record(), [cancelled, final])
        self.assertEqual(
            conflict["close_disposition"], "UNRESOLVED_IDENTITY_OR_STATUS"
        )

    def test_manifest_and_inventory_tampering_fail_closed(self) -> None:
        manifest = make_inventory_manifest(self.rows, self.summary)
        with tempfile.TemporaryDirectory() as temporary:
            package = Path(temporary)
            inventory_path = package / INVENTORY_FILENAME
            inventory_path.write_bytes(
                b"".join(canonical_json_bytes(row) + b"\n" for row in self.rows)
            )
            tampered = copy.deepcopy(manifest)
            tampered["inventory_row_count"] = 1
            (package / MANIFEST_FILENAME).write_text(json.dumps(tampered))
            with self.assertRaisesRegex(CloseInventoryError, "MANIFEST_HASH_MISMATCH"):
                validate_close_inventory_package(
                    package_path=package,
                    expected_manifest_sha256=manifest["manifest_sha256"],
                    authority=self.authority,
                )
            (package / MANIFEST_FILENAME).write_text(json.dumps(manifest))
            inventory_path.write_bytes(inventory_path.read_bytes() + b"{}\n")
            with self.assertRaisesRegex(CloseInventoryError, "FILE_HASH_MISMATCH"):
                validate_close_inventory_package(
                    package_path=package,
                    expected_manifest_sha256=manifest["manifest_sha256"],
                    authority=self.authority,
                )

    def test_repeated_build_is_deterministic(self) -> None:
        second_rows, second_summary = build_authoritative_inventory(
            authority=self.authority
        )
        self.assertEqual(self.rows, second_rows)
        self.assertEqual(self.summary, second_summary)
        self.assertEqual(
            make_inventory_manifest(self.rows, self.summary),
            make_inventory_manifest(second_rows, second_summary),
        )

    def test_premature_close_is_blocked_and_command_rejects_arguments(self) -> None:
        report = validate_inventory_rows(
            self.rows, self.authority.records, expected_count=2430
        )
        self.assertFalse(report["close_ready"])
        self.assertEqual(len(report["scheduled_not_final_game_pks"]), 436)
        with patch("sys.argv", ["close-check", "--inventory", "arbitrary.json"]):
            self.assertEqual(close_command.main(), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
