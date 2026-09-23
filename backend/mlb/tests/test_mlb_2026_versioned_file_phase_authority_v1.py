from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.mlb.scripts.build_mlb_versioned_file_phase_authority_v1 import (
    CANDIDATE_READY,
    NO_NEW_AUTHORITY_EVIDENCE,
    SOURCE_SET_CONTRACT_NAME,
    SnapshotBuildError,
    _classification_hash,
    build_candidate_snapshot,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    EXPECTED_PROPOSAL_SHA256,
    EXPECTED_SOURCE_MANIFEST_SHA256,
    EXPECTED_V1_DESCRIPTOR_SHA256,
    GamePhaseAuthorityError,
    HashedProposalAuthority,
    VersionedFileAuthority,
    load_v1_authority,
)
from backend.mlb.season_transition.phase_authority_snapshot_v1 import (
    ACTIVE_SELECTION_CONTRACT_NAME,
    REPO_ROOT,
    V1_DESCRIPTOR_PATH,
    canonical_json_bytes,
)
from backend.mlb.season_transition.regular_season_close_inventory_v1 import (
    validate_close_inventory_package,
)


POSTSEASON_TYPES = {
    "F": ("Wild Card", "WILD_CARD"),
    "D": ("Division Series", "DIVISION_SERIES"),
    "L": ("League Championship Series", "LEAGUE_CHAMPIONSHIP_SERIES"),
    "W": ("World Series", "WORLD_SERIES"),
    "P": ("Playoffs", "PLAYOFFS_UNSPECIFIED"),
    "C": ("Championship", "CHAMPIONSHIP_UNSPECIFIED"),
}


class VersionedFilePhaseAuthorityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.v1 = load_v1_authority()
        cls.v1_descriptor = json.loads(V1_DESCRIPTOR_PATH.read_text(encoding="utf-8"))
        cls.v1_proposal = REPO_ROOT / cls.v1.metadata.proposal_path
        cls.v1_source_manifest = REPO_ROOT / cls.v1.metadata.source_manifest_path

    def setUp(self) -> None:
        self.holder = Path(tempfile.mkdtemp(prefix="versioned_phase_", dir=REPO_ROOT / "tmp"))

    def tearDown(self) -> None:
        shutil.rmtree(self.holder)

    def write_schedule(self, games: list[dict[str, object]], name: str = "schedule.json") -> Path:
        path = self.holder / name
        payload = {"dates": [{"date": "2026-10-01", "games": games}]}
        path.write_bytes(canonical_json_bytes(payload))
        return path

    def write_source_set(self, sources: list[Path], name: str = "source_set.json") -> Path:
        path = self.holder / name
        payload = {
            "contract_name": SOURCE_SET_CONTRACT_NAME,
            "schema_version": 1,
            "source_set_id": "SYNTHETIC_TEST_ONLY",
            "parent_descriptor_path": str(V1_DESCRIPTOR_PATH.relative_to(REPO_ROOT)),
            "parent_descriptor_sha256": EXPECTED_V1_DESCRIPTOR_SHA256,
            "sources": [
                {
                    "source_path": str(source.relative_to(REPO_ROOT)),
                    "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                    "source_bytes": source.stat().st_size,
                }
                for source in sources
            ],
        }
        path.write_bytes(canonical_json_bytes(payload))
        return path

    @staticmethod
    def game(game_pk: int, game_type: str, round_name: str, date_value: str = "2026-10-01T20:00:00Z", **extra: object) -> dict[str, object]:
        return {
            "gamePk": game_pk,
            "gameType": game_type,
            "season": "2026",
            "gameDate": date_value,
            "seriesDescription": round_name,
            **extra,
        }

    def build(self, games: list[dict[str, object]], output_name: str = "candidate") -> tuple[dict[str, object], Path]:
        schedule = self.write_schedule(games)
        source_set = self.write_source_set([schedule])
        output = self.holder / output_name
        report = build_candidate_snapshot(source_set_path=source_set, output_dir=output)
        return report, output

    def load_candidate(self, output: Path) -> VersionedFileAuthority:
        descriptor = output / "descriptor.json"
        return VersionedFileAuthority(
            descriptor_path=descriptor,
            expected_descriptor_sha256=hashlib.sha256(descriptor.read_bytes()).hexdigest(),
            allow_candidate=True,
        )

    def rewrite_descriptor(self, output: Path, mutate) -> str:
        path = output / "descriptor.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        mutate(data)
        path.write_bytes(canonical_json_bytes(data) + b"\n")
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def test_v1_descriptor_and_all_rows_are_byte_identical(self) -> None:
        authority = HashedProposalAuthority()
        self.assertEqual(authority.metadata.snapshot_descriptor_sha256, EXPECTED_V1_DESCRIPTOR_SHA256)
        self.assertEqual(authority.metadata.proposal_count, 2919)
        self.assertEqual(hashlib.sha256(self.v1_proposal.read_bytes()).hexdigest(), EXPECTED_PROPOSAL_SHA256)
        self.assertEqual(hashlib.sha256(self.v1_source_manifest.read_bytes()).hexdigest(), EXPECTED_SOURCE_MANIFEST_SHA256)
        lines = self.v1_proposal.read_bytes().splitlines(keepends=True)
        self.assertEqual(len(lines), 2919)
        self.assertTrue(all(line.endswith(b"\n") for line in lines))
        self.assertEqual(authority.metadata.authority_records_sha256, "5a7cdc460cc42ca2b4ed328c74e978b9da6967f95d7a7c4ba888b3b8d3401a84")

    def test_deterministic_repeated_builds(self) -> None:
        games = [self.game(990100, "F", "Wild Card")]
        schedule = self.write_schedule(games)
        source_set = self.write_source_set([schedule])
        first = self.holder / "candidate_a"
        second = self.holder / "candidate_b"
        one = build_candidate_snapshot(source_set_path=source_set, output_dir=first)
        two = build_candidate_snapshot(source_set_path=source_set, output_dir=second)
        self.assertEqual(one, two)
        for name in (
            "canonical_game_phase_full_snapshot.jsonl",
            "retained_source_manifest.jsonl",
            "descriptor.json",
            "validation_report.json",
            "sha256_manifest.txt",
        ):
            self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())

    def test_no_new_authority_evidence_creates_no_snapshot(self) -> None:
        row = json.loads(self.v1_proposal.read_text(encoding="utf-8").splitlines()[0])
        game = self.game(
            int(row["game_pk"]),
            str(row["source_game_type"]),
            str(row["source_round"]),
            **dict(row["schedule_relationships"]),
        )
        schedule = self.write_schedule([game])
        source_set = self.write_source_set([schedule])
        output = self.holder / "must_not_exist"
        result = build_candidate_snapshot(source_set_path=source_set, output_dir=output)
        self.assertEqual(result["decision"], NO_NEW_AUTHORITY_EVIDENCE)
        self.assertFalse(result["candidate_created"])
        self.assertFalse(output.exists())

    def test_stale_v1_window_fails_closed(self) -> None:
        with self.assertRaisesRegex(
            GamePhaseAuthorityError,
            "GAME_PHASE_AUTHORITY_STALE",
        ):
            self.v1.require_supported_window("2026-09-28", "2026-09-28")

    def test_all_postseason_rounds_and_late_regular_game(self) -> None:
        games = [
            self.game(990200 + index, game_type, raw_round)
            for index, (game_type, (raw_round, _)) in enumerate(POSTSEASON_TYPES.items())
        ]
        games.append(
            self.game(
                990299,
                "R",
                "Regular Season",
                "2026-10-02T20:00:00Z",
                rescheduledFrom="2026-09-27T20:00:00Z",
                resumedFrom="2026-10-01T20:00:00Z",
            )
        )
        report, output = self.build(games)
        self.assertEqual(report["decision"], CANDIDATE_READY)
        authority = self.load_candidate(output)
        for index, (game_type, (_, expected_round)) in enumerate(POSTSEASON_TYPES.items()):
            record = authority.lookup_exact(990200 + index)
            self.assertEqual(record.source_game_type, game_type)
            self.assertEqual(record.season_phase, "POSTSEASON")
            self.assertEqual(record.postseason_round, expected_round)
            self.assertIsNotNone(record.scheduled_start_utc)
        late = authority.lookup_exact(990299)
        self.assertEqual(late.season_phase, "REGULAR_SEASON")
        self.assertEqual(late.schedule_relationships["rescheduledFrom"], "2026-09-27T20:00:00Z")
        self.assertEqual(late.schedule_relationships["resumedFrom"], "2026-10-01T20:00:00Z")
        authority.require_supported_window("2026-10-02", "2026-10-02")

    def test_existing_row_mutation_is_rejected(self) -> None:
        row = json.loads(self.v1_proposal.read_text(encoding="utf-8").splitlines()[0])
        game = self.game(int(row["game_pk"]), "W", "World Series")
        with self.assertRaisesRegex(SnapshotBuildError, "EXISTING_GAME_MUTATION_REJECTED"):
            self.build([game])

    def test_existing_row_deletion_is_rejected_by_parent_chain(self) -> None:
        _, output = self.build([self.game(990400, "F", "Wild Card")])
        proposal = output / "canonical_game_phase_full_snapshot.jsonl"
        rows = [json.loads(line) for line in proposal.read_text().splitlines()][1:]
        proposal.write_bytes(b"".join(canonical_json_bytes(row) + b"\n" for row in rows))
        def mutate(data: dict[str, object]) -> None:
            data["proposal_sha256"] = hashlib.sha256(proposal.read_bytes()).hexdigest()
            data["row_count"] = len(rows)
            game_pks = [int(row["game_pk"]) for row in rows]
            data["distinct_game_pk_count"] = len(set(game_pks))
            data["game_pk_population_sha256"] = hashlib.sha256(canonical_json_bytes(game_pks)).hexdigest()
            data["classification_sha256"] = _classification_hash(rows)
        digest = self.rewrite_descriptor(output, mutate)
        with self.assertRaisesRegex(GamePhaseAuthorityError, "SNAPSHOT_PARENT_PROPOSAL_PREFIX_MISMATCH"):
            VersionedFileAuthority(
                descriptor_path=output / "descriptor.json",
                expected_descriptor_sha256=digest,
                allow_candidate=True,
            )

    def test_unknown_conflicting_and_duplicate_types_reject(self) -> None:
        with self.assertRaisesRegex(SnapshotBuildError, "AUTHORITATIVE_CLASSIFICATION_REJECTED"):
            self.build([self.game(990500, "Z", "Unknown")])
        with self.assertRaisesRegex(SnapshotBuildError, "NEW_GAME_IDENTITY_CONFLICT"):
            self.build([
                self.game(990501, "R", "Regular Season"),
                self.game(990501, "W", "World Series"),
            ])
        schedule = self.write_schedule([self.game(990502, "F", "Wild Card")])
        source_set = self.write_source_set([schedule, schedule], "duplicate_source_set.json")
        with self.assertRaisesRegex(SnapshotBuildError, "SOURCE_SET_DUPLICATE_PATH"):
            build_candidate_snapshot(source_set_path=source_set, output_dir=self.holder / "dup")

    def test_source_and_proposal_hash_mismatch_reject(self) -> None:
        schedule = self.write_schedule([self.game(990600, "F", "Wild Card")])
        source_set = self.write_source_set([schedule])
        source_data = json.loads(source_set.read_text())
        source_data["sources"][0]["source_sha256"] = "0" * 64
        source_set.write_bytes(canonical_json_bytes(source_data))
        with self.assertRaisesRegex(SnapshotBuildError, "RETAINED_EVIDENCE_HASH_MISMATCH"):
            build_candidate_snapshot(source_set_path=source_set, output_dir=self.holder / "bad_source")

        _, output = self.build([self.game(990601, "F", "Wild Card")], "valid")
        proposal = output / "canonical_game_phase_full_snapshot.jsonl"
        proposal.write_bytes(proposal.read_bytes() + b"\n")
        descriptor = output / "descriptor.json"
        with self.assertRaisesRegex(GamePhaseAuthorityError, "SNAPSHOT_PROPOSAL_HASH_MISMATCH"):
            VersionedFileAuthority(
                descriptor_path=descriptor,
                expected_descriptor_sha256=hashlib.sha256(descriptor.read_bytes()).hexdigest(),
                allow_candidate=True,
            )

    def test_broken_parent_chain_and_unsupported_schema_reject(self) -> None:
        _, output = self.build([self.game(990700, "F", "Wild Card")])
        broken_hash = self.rewrite_descriptor(
            output, lambda data: data.__setitem__("parent_descriptor_sha256", "0" * 64)
        )
        with self.assertRaisesRegex(GamePhaseAuthorityError, "SNAPSHOT_DESCRIPTOR_HASH_MISMATCH"):
            VersionedFileAuthority(
                descriptor_path=output / "descriptor.json",
                expected_descriptor_sha256=broken_hash,
                allow_candidate=True,
            )

        _, schema_output = self.build([self.game(990701, "F", "Wild Card")], "schema")
        schema_hash = self.rewrite_descriptor(
            schema_output, lambda data: data.__setitem__("schema_version", 999)
        )
        with self.assertRaisesRegex(GamePhaseAuthorityError, "SNAPSHOT_SCHEMA_UNSUPPORTED"):
            VersionedFileAuthority(
                descriptor_path=schema_output / "descriptor.json",
                expected_descriptor_sha256=schema_hash,
                allow_candidate=True,
            )

    def test_invalid_active_selection_never_falls_back(self) -> None:
        selection = self.holder / "active_selection.json"
        selection.write_bytes(
            canonical_json_bytes(
                {
                    "contract_name": ACTIVE_SELECTION_CONTRACT_NAME,
                    "schema_version": 1,
                    "descriptor_path": "tmp/does_not_exist/descriptor.json",
                    "descriptor_sha256": "0" * 64,
                    "selection_status": "GOVERNED_ACTIVE",
                }
            )
        )
        with self.assertRaisesRegex(GamePhaseAuthorityError, "SNAPSHOT_EVIDENCE_MISSING"):
            HashedProposalAuthority(selection_path=selection)

    def test_non_v1_root_descriptor_is_rejected(self) -> None:
        root_dir = self.holder / "substitute_root"
        root_dir.mkdir()
        descriptor = dict(self.v1_descriptor)
        descriptor["snapshot_id"] = "UNAUTHORIZED_SUBSTITUTE_ROOT"
        descriptor["root_snapshot_id"] = "UNAUTHORIZED_SUBSTITUTE_ROOT"
        descriptor["proposal_path"] = os.path.relpath(self.v1_proposal, root_dir)
        descriptor["source_manifest_path"] = os.path.relpath(
            self.v1_source_manifest, root_dir
        )
        descriptor_path = root_dir / "descriptor.json"
        descriptor_path.write_bytes(canonical_json_bytes(descriptor) + b"\n")
        with self.assertRaisesRegex(
            GamePhaseAuthorityError,
            "GAME_PHASE_ROOT_DESCRIPTOR_NOT_GOVERNED_V1",
        ):
            VersionedFileAuthority(
                descriptor_path=descriptor_path,
                expected_descriptor_sha256=hashlib.sha256(
                    descriptor_path.read_bytes()
                ).hexdigest(),
            )

    def test_close_checker_is_v1_pinned_and_check_only(self) -> None:
        report = validate_close_inventory_package()
        self.assertEqual(report["authority_proposal_sha256"], EXPECTED_PROPOSAL_SHA256)
        self.assertEqual(report["decision"], "REGULAR_SEASON_CLOSE_BLOCKED")
        self.assertTrue(report["check_only"])
        self.assertFalse(report["close_package_created"])

    def test_agreement_v4_and_network_database_boundaries_unchanged(self) -> None:
        agreement = REPO_ROOT / "backend/mlb/scripts/capture_mlb_market_strong_agreement_live_v4.py"
        self.assertEqual(
            hashlib.sha256(agreement.read_bytes()).hexdigest(),
            "95600eb7b3a41aa0f0a4275c833828787c560a928fb0bfa56b0d881d61297120",
        )
        text = (
            REPO_ROOT / "backend/mlb/scripts/build_mlb_versioned_file_phase_authority_v1.py"
        ).read_text(encoding="utf-8")
        for forbidden in (
            "import requests",
            "from requests",
            "urllib.request",
            "psycopg",
            "pg_connect",
            "sqlalchemy",
        ):
            self.assertNotIn(forbidden, text)
        self.assertIn('"network_requests": 0', text)
        self.assertIn('"database_requests": 0', text)


if __name__ == "__main__":
    unittest.main()
