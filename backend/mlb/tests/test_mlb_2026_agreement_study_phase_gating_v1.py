from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.mlb.markets import agreement_phase_gating_v1 as gate
from backend.mlb.scripts import capture_mlb_market_strong_agreement_live_v4 as capture
from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as study
from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityMetadata,
    GamePhaseAuthorityRecord,
)


def metadata(**changes: int | str) -> GamePhaseAuthorityMetadata:
    values = dict(
        authority_interface="SYNTHETIC_TEST_AUTHORITY",
        backend="SYNTHETIC_TEST_BACKEND",
        supported_season=2026,
        supported_from_date="2026-01-01",
        supported_through_date="2026-12-31",
        proposal_path="synthetic/proposal.jsonl",
        proposal_sha256="a" * 64,
        proposal_count=1,
        source_manifest_path="synthetic/sources.jsonl",
        source_manifest_sha256="b" * 64,
        source_file_count=1,
        source_observation_count=1,
        phase_contract_name="SYNTHETIC_TEST_CONTRACT",
        phase_contract_version="contract_v1",
        phase_contract_sha256="c" * 64,
        source_type_counts={"R": 1},
        phase_counts={"REGULAR_SEASON": 1},
        authority_records_sha256="d" * 64,
        missing_count=0,
        unknown_count=0,
        conflicting_count=0,
        duplicate_identity_count=0,
    )
    values.update(changes)
    return GamePhaseAuthorityMetadata(**values)


def record(
    game_pk: int, source_type: str, phase: str | None,
    postseason_round: str | None = None, relationships: dict | None = None,
) -> GamePhaseAuthorityRecord:
    return GamePhaseAuthorityRecord(
        game_pk=game_pk, source_season=2026, source_game_type=source_type,
        season_phase=phase, postseason_round=postseason_round,
        season_name=f"SYNTHETIC_{phase}" if phase else None,
        source_round=postseason_round, schedule_relationships=relationships or {},
        primary_source_path="synthetic_fixture.json", primary_source_sha256="e" * 64,
        source_paths=("synthetic_fixture.json",), source_hashes=("e" * 64,),
        phase_decision="CLASSIFIED_FROM_AUTHORITATIVE_SOURCE_TYPE",
        authority_status="AUTHORITATIVE_UNAMBIGUOUS" if phase else "SPECIAL_EXCLUDED",
    )


class StubAuthority(CanonicalGamePhaseAuthority):
    def __init__(self, records, *, meta=None, errors=None):
        self._records = records
        self._metadata = meta or metadata(proposal_count=len(records))
        self._errors = errors or {}

    @property
    def metadata(self):
        return self._metadata

    def lookup_exact(self, game_pk):
        try:
            exact = int(game_pk)
        except (TypeError, ValueError):
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING") from None
        if exact in self._errors:
            raise GamePhaseAuthorityError(self._errors[exact], game_pk=exact)
        if exact not in self._records:
            raise GamePhaseAuthorityError("GAME_PHASE_ABSENT", game_pk=exact)
        return self._records[exact]


def row(game_pk=1, game_date="2026-09-22", **changes):
    value = {"game_key": f"MLB|{game_date}|{game_pk}", "game_date": game_date,
             "game_id": game_pk}
    value.update(changes)
    return value


def canonical_hash(rows):
    return hashlib.sha256(json.dumps(
        rows, sort_keys=True, separators=(",", ":"), default=str
    ).encode()).hexdigest()


class AgreementStudyPhaseGatingV1Tests(unittest.TestCase):
    def test_all_supported_postseason_rounds_are_shadow_only(self):
        rounds = {
            "F": "WILD_CARD", "D": "DIVISION_SERIES",
            "L": "LEAGUE_CHAMPIONSHIP_SERIES", "W": "WORLD_SERIES",
            "P": "PLAYOFFS_UNSPECIFIED", "C": "CHAMPIONSHIP_UNSPECIFIED",
        }
        records = {
            game_pk: record(game_pk, source_type, "POSTSEASON", round_name)
            for game_pk, (source_type, round_name) in enumerate(rounds.items(), 100)
        }
        decisions = [gate.classify_agreement_row(row(game_pk), authority=StubAuthority(records))
                     for game_pk in records]
        self.assertEqual({item.source_game_type for item in decisions}, set(rounds))
        self.assertEqual({item.postseason_round for item in decisions}, set(rounds.values()))
        self.assertTrue(all(item.decision_code == "ADMITTED_POSTSEASON_SHADOW"
                            for item in decisions))

    def test_regular_after_nominal_close_and_relationships_use_authority(self):
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON", relationships={"rescheduledFrom": 9}),
            2: record(2, "R", "REGULAR_SEASON", relationships={"resumedFrom": 8}),
        })
        partitions = gate.partition_agreement_rows(
            [row(1, "2026-10-10"), row(2, "2026-11-01")], authority=authority)
        self.assertEqual(partitions.counts()["REGULAR_SEASON"], 2)

    def test_preseason_special_missing_stale_conflict_and_duplicate_fail_closed(self):
        authority = StubAuthority(
            {1: record(1, "S", "PRESEASON")}, errors={2: "GAME_PHASE_SPECIAL_EXCLUDED"})
        partitions = gate.partition_agreement_rows([row(1), row(2)], authority=authority)
        self.assertEqual(partitions.counts()["EXCLUDED_PRESEASON"], 1)
        self.assertEqual(partitions.counts()["EXCLUDED_SPECIAL"], 1)
        for code in ("GAME_PHASE_ABSENT", "GAME_PHASE_AUTHORITY_STATUS_UNKNOWN",
                     "GAME_PHASE_CONFLICT_BLOCKED"):
            with self.subTest(code=code), self.assertRaises(gate.AgreementPhaseGateError):
                gate.classify_agreement_row(row(9), authority=StubAuthority({}, errors={9: code}))
        stale = metadata(supported_through_date="2026-09-21")
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "PHASE_AUTHORITY_STALE"):
            gate.classify_agreement_row(
                row(1), authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}, meta=stale))
        duplicate = metadata(duplicate_identity_count=1)
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "POPULATION_INVALID"):
            gate.classify_agreement_row(
                row(1), authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}, meta=duplicate))
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "SOURCE_TYPE_CONFLICT"):
            gate.classify_agreement_row(
                row(1, source_game_type="W"),
                authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}))
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "DUPLICATE_EVALUATION_IDENTITY"):
            gate.partition_agreement_rows(
                [row(1), row(1)], authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}),
                unique_identity_fields=("game_key",))

    def test_exact_gamepk_required_and_capture_rejects_cross_phase_snapshot(self):
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "W", "POSTSEASON", "WORLD_SERIES"),
        })
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "GAME_PK_MISSING"):
            gate.classify_agreement_row({"game_date": "2026-09-22"}, authority=authority)
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "EXACT_GAME_PK_MISMATCH"):
            gate.classify_agreement_row(row(1, gamePk=2), authority=authority)
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "CAPTURE_PHASE_MEMBERSHIP_INVALID"):
            gate.require_snapshot_membership(
                [row(1), row(2)], "REGULAR_SEASON", authority=authority)

    def test_regular_and_postseason_metrics_and_credits_are_disjoint(self):
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "D", "POSTSEASON", "DIVISION_SERIES"),
        })
        rows = [row(1, "2026-09-22"), row(2, "2026-10-02")]
        partitions = gate.partition_agreement_rows(rows, authority=authority)
        self.assertEqual([item["game_id"] for item in partitions.selected("REGULAR_SEASON")], [1])
        self.assertEqual([item["game_id"] for item in partitions.selected("POSTSEASON")], [2])
        predictions = pd.DataFrame(rows)
        claims = pd.DataFrame([
            {"game_date": "2026-09-22", "capture_mode": "LIVE", "x_requests_last": "1"},
            {"game_date": "2026-10-02", "capture_mode": "HISTORICAL_RECOVERY", "x_requests_last": "10"},
        ])
        accounting = study.phase_partitioned_credit_accounting(
            claims, predictions, phase_authority=authority)
        self.assertEqual(accounting["provider_request_claim_counts"]["REGULAR_SEASON"]["LIVE"], 1)
        self.assertEqual(accounting["study_credit_costs"]["POSTSEASON"]["HISTORICAL_RECOVERY"], 10)

    def test_mixed_phase_date_credit_is_unprovable(self):
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "W", "POSTSEASON", "WORLD_SERIES"),
        })
        predictions = pd.DataFrame([row(1), row(2)])
        claims = pd.DataFrame([{"game_date": "2026-09-22", "capture_mode": "LIVE",
                                "x_requests_last": "1"}])
        with self.assertRaisesRegex(gate.AgreementPhaseGateError, "CREDIT_PHASE_UNPROVABLE"):
            study.phase_partitioned_credit_accounting(
                claims, predictions, phase_authority=authority)

    def test_provider_only_event_cannot_enter_immutable_risk_ledger(self):
        authority = StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        snapshot = capture.prediction_snapshot_from_rows("2026-09-22", 1, ({
            "game_date": "2026-09-22", "game_id": 1,
            "scheduled_start_utc": "2026-09-22T23:00:00Z",
            "prediction_timestamp_utc": "2026-09-22T12:29:55Z",
            "prediction_cutoff_utc": "2026-09-22T12:00:00Z",
            "home_team": "Home Club", "away_team": "Away Club",
            "home_win_probability": .65, "away_win_probability": .35,
            "payload_sha256": "a" * 64, "model_version": study.MODEL,
            "prediction_snapshot_class": study.SNAPSHOT, "model_hash": study.MODEL_HASH,
            "admission_status": study.ADMISSION, "created_at": "2026-09-22T12:30:00Z",
        },))
        matched = {"id": "matched", "commence_time": "2026-09-22T23:00:00Z",
                   "home_team": "Home Club", "away_team": "Away Club", "bookmakers": []}
        unmatched = {"id": "unmatched", "commence_time": "2026-09-22T20:00:00Z",
                     "home_team": "Other Home", "away_team": "Other Away", "bookmakers": []}
        with sqlite3.connect(":memory:") as conn:
            conn.row_factory = sqlite3.Row
            capture.schema(conn)
            capture.ingest_snapshot_predictions(conn, snapshot)
            counts = capture.ingest_events(
                conn, "2026-09-22", [matched, unmatched],
                "2026-09-22T12:30:01Z", "2026-09-22T12:30:02Z", "f" * 64,
                "LIVE", "event-1", phase_authority=authority)
            self.assertEqual(counts["risk_rows_inserted"], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM risk_set").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT game_id FROM risk_set").fetchone()[0], 1)

    def test_zero_calendar_reconstruction_and_hash_invariance(self):
        authority = StubAuthority({
            1: record(1, "W", "POSTSEASON", "WORLD_SERIES"),
            2: record(2, "R", "REGULAR_SEASON"),
        })
        rows = [row(1, "2026-04-01", value=.61), row(2, "2026-11-01", value=.62)]
        before = canonical_hash(rows)
        first = gate.partition_agreement_rows(rows, authority=authority)
        second = gate.partition_agreement_rows(rows, authority=authority)
        self.assertEqual(first, second)
        self.assertEqual(first.decisions[0].normalized_phase, "POSTSEASON")
        self.assertEqual(first.decisions[1].normalized_phase, "REGULAR_SEASON")
        self.assertEqual(canonical_hash(rows), before)

    def test_bounded_path_has_no_default_to_r_or_calendar_phase_logic(self):
        sources = "\n".join((
            inspect.getsource(gate), inspect.getsource(study), inspect.getsource(capture),
        ))
        for forbidden in ('or "R"', "or 'R'", 'fillna("R")', "fillna('R')",
                          "POSTSEASON_CALENDAR_WINDOW"):
            self.assertNotIn(forbidden, sources)
        self.assertNotIn("def late_season_regime", sources)
        self.assertEqual(
            inspect.signature(study.report).parameters["evaluation_phase"].default,
            "REGULAR_SEASON")
        self.assertEqual(
            inspect.signature(capture.execute_capture).parameters["evaluation_phase"].default,
            "REGULAR_SEASON")


if __name__ == "__main__":
    unittest.main()
