from __future__ import annotations

import csv
import hashlib
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from backend.mlb.identity.provider_event_game_binding_v1 import (
    ACCEPTED_STATUS,
    BindingRegistry,
    EvidenceSource,
    OfficialGameCandidate,
    ProviderEvent,
    ProviderEventGameIdentityError,
    resolve_binding,
    write_receipts_immutable,
)
from backend.mlb.markets.pinnacle_main_market_capture_v1 import parse_events
from backend.mlb.shared import prospective_lineage


ROOT = Path(__file__).resolve().parents[3]
PRIOR_AUDIT = ROOT / (
    "docs/contracts/mlb_2026_postseason_collection_identity_readiness_audit_v1/"
    "provider_event_to_game_pk_audit.csv"
)


@dataclass(frozen=True)
class _AuthorityRecord:
    source_game_type: str = "R"


class _Metadata:
    def to_dict(self):
        return {"backend": "SYNTHETIC_TEST_AUTHORITY", "authority_records_sha256": "a" * 64}


class _Authority:
    metadata = _Metadata()

    def __init__(self, game_pks=(1, 2), source_game_type="R"):
        self.game_pks = set(game_pks)
        self.source_game_type = source_game_type

    def lookup_exact(self, game_pk):
        if int(game_pk) not in self.game_pks:
            raise KeyError(game_pk)
        return _AuthorityRecord(self.source_game_type)


class ProviderEventGameIdentityHardeningV1Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.schedule_path = self.root / "schedule.json"
        self.provider_path = self.root / "provider.json"
        self.schedule_path.write_bytes(b'{"schedule":true}')
        self.provider_path.write_bytes(b'{"provider":true}')
        self.schedule_source = EvidenceSource(
            str(self.schedule_path), hashlib.sha256(self.schedule_path.read_bytes()).hexdigest()
        )
        self.provider_source = EvidenceSource(
            str(self.provider_path), hashlib.sha256(self.provider_path.read_bytes()).hexdigest()
        )

    def tearDown(self):
        self.temp.cleanup()

    def event(self, *, event_id="evt", commence="2026-07-01T17:00:00Z", game_number=None,
              home="NYY", away="BOS"):
        return ProviderEvent("THE_ODDS_API", event_id, home, away, commence, game_number)

    def game(self, game_pk=1, start="2026-07-01T17:00:00Z", game_number=1,
             home="NYY", away="BOS", game_type="R", doubleheader="N"):
        return OfficialGameCandidate(
            game_pk, home, away, start, game_number, doubleheader, game_type,
            self.schedule_source,
        )

    def resolve(self, event=None, candidates=None, registry=None, authority=None):
        return resolve_binding(
            event=event or self.event(),
            candidates=candidates or [self.game()],
            provider_snapshot=self.provider_source,
            authority=authority or _Authority(),
            observation_timestamp_utc="2026-07-01T15:00:00Z",
            registry=registry,
            root=self.root,
        )

    def assert_code(self, code, **kwargs):
        with self.assertRaises(ProviderEventGameIdentityError) as caught:
            self.resolve(**kwargs)
        self.assertEqual(code, caught.exception.code)

    def test_unique_same_team_game_accepts_without_commence(self):
        receipt = self.resolve(event=self.event(commence=None))
        self.assertEqual(1, receipt.game_pk)
        self.assertEqual(ACCEPTED_STATUS, receipt.binding_status)

    def test_doubleheader_valid_commence_selects_exact_game(self):
        games = [self.game(1, "2026-07-01T17:00:00Z", 1, doubleheader="Y"),
                 self.game(2, "2026-07-01T23:00:00Z", 2, doubleheader="Y")]
        self.assertEqual(2, self.resolve(event=self.event(commence="2026-07-01T23:04:00Z"), candidates=games).game_pk)

    def test_doubleheader_official_game_number_selects_exact_game(self):
        games = [self.game(1, "2026-07-01T17:00:00Z", 1, doubleheader="Y"),
                 self.game(2, "2026-07-01T23:00:00Z", 2, doubleheader="Y")]
        self.assertEqual(2, self.resolve(event=self.event(commence=None, game_number=2), candidates=games).game_pk)

    def test_doubleheader_missing_commence_fails_closed(self):
        games = [self.game(1, game_number=1), self.game(2, "2026-07-01T23:00:00Z", 2)]
        self.assert_code("OFFICIAL_CANDIDATE_AMBIGUOUS", event=self.event(commence=None), candidates=games)

    def test_invalid_commence_cannot_resolve_ambiguous_set(self):
        games = [self.game(1), self.game(2, "2026-07-01T23:00:00Z", 2)]
        self.assert_code("COMMENCE_TIME_INVALID_AMBIGUOUS", event=self.event(commence="not-a-time"), candidates=games)

    def test_zero_candidates_fails_closed(self):
        self.assert_code("OFFICIAL_CANDIDATE_ZERO", candidates=[self.game(home="LAD", away="SD")])

    def test_multiple_remaining_candidates_fail_closed(self):
        games = [self.game(1), self.game(2, game_number=2)]
        self.assert_code("OFFICIAL_CANDIDATE_AMBIGUOUS", candidates=games)

    def test_provider_event_reuse_for_conflicting_game_fails(self):
        registry = BindingRegistry()
        self.resolve(registry=registry)
        self.assert_code(
            "PROVIDER_EVENT_REUSE_CONFLICT",
            candidates=[self.game(2)], registry=registry, authority=_Authority((1, 2)),
        )

    def test_reversed_or_unresolved_team_identity_fails(self):
        self.assert_code("OFFICIAL_CANDIDATE_ZERO", event=self.event(home="BOS", away="NYY"))
        self.assert_code("HOME_TEAM_UNRESOLVED", event=self.event(home=""))

    def test_postponed_rescheduled_identity_is_not_date_inferred(self):
        receipt = self.resolve(
            event=self.event(commence="2026-07-03T17:00:00Z"),
            candidates=[self.game(start="2026-07-03T17:00:00Z")],
        )
        self.assertEqual(1, receipt.game_pk)

    def test_suspended_resumed_identity_is_preserved(self):
        receipt = self.resolve(event=self.event(game_number=1), candidates=[self.game(doubleheader="S")])
        self.assertEqual("S", receipt.official_doubleheader_indicator)

    def test_timezone_date_boundary_uses_instants(self):
        receipt = self.resolve(
            event=self.event(commence="2026-07-02T00:05:00+00:00"),
            candidates=[self.game(start="2026-07-01T20:05:00-04:00")],
        )
        self.assertEqual("2026-07-02T00:05:00Z", receipt.official_scheduled_start_utc)

    def test_schedule_source_hash_mismatch_fails(self):
        bad = EvidenceSource(str(self.schedule_path), "0" * 64)
        candidate = OfficialGameCandidate(1, "NYY", "BOS", "2026-07-01T17:00:00Z", 1, "N", "R", bad)
        self.assert_code("EVIDENCE_HASH_MISMATCH", candidates=[candidate])

    def test_provider_snapshot_hash_mismatch_fails(self):
        bad = EvidenceSource(str(self.provider_path), "0" * 64)
        with self.assertRaises(ProviderEventGameIdentityError) as caught:
            resolve_binding(
                event=self.event(), candidates=[self.game()], provider_snapshot=bad,
                authority=_Authority(), observation_timestamp_utc="2026-07-01T15:00:00Z",
                root=self.root,
            )
        self.assertEqual("EVIDENCE_HASH_MISMATCH", caught.exception.code)

    def test_authority_and_schedule_type_must_agree(self):
        self.assert_code("OFFICIAL_GAME_TYPE_CONFLICT", authority=_Authority(source_game_type="S"))

    def test_duplicate_receipt_ingestion_is_idempotent(self):
        receipt = self.resolve()
        path = self.root / "receipts.jsonl"
        first = write_receipts_immutable(path, [receipt, receipt])
        second = write_receipts_immutable(path, [receipt])
        self.assertEqual(first, second)
        self.assertEqual(1, len(path.read_text().splitlines()))

    def test_pinnacle_636_mapping_population_is_invariant(self):
        with PRIOR_AUDIT.open(newline="", encoding="utf-8") as handle:
            rows = [row for row in csv.DictReader(handle) if row["lane"] == "PINNACLE_MAIN_MARKETS"]
        self.assertEqual(636, len(rows))
        self.assertEqual(636, len({row["provider_event_id"] for row in rows}))
        self.assertTrue(all(row["exact_game_pk"] for row in rows))
        self.assertEqual(636, len({(row["provider_event_id"], row["exact_game_pk"]) for row in rows}))

    def test_shared_resolver_has_no_network_or_phase_logic(self):
        source = (ROOT / "backend/mlb/identity/provider_event_game_binding_v1.py").read_text()
        self.assertNotIn("requests", source)
        self.assertNotIn("urlopen", source)
        self.assertNotIn("season_phase", source)
        self.assertNotIn("game_date", source)

    def test_active_collectors_do_not_add_provider_requests(self):
        pinnacle = (ROOT / "backend/mlb/scripts/capture_mlb_pinnacle_main_markets_v1.py").read_text()
        player = (ROOT / "backend/mlb/scripts/build_mlb_predictions_wide.py").read_text()
        self.assertEqual(1, pinnacle.count("requests.get("))
        self.assertEqual(1, player.count("market_odds_service._fetch_market_snapshot("))
        self.assertIn("require_verified_bindings=True", pinnacle)
        self.assertNotIn("_choose_game_for_event", player)

    def test_pinnacle_verified_mode_rejects_unbound_event_before_rows(self):
        event = {
            "id": "evt", "away_team": "Boston Red Sox", "home_team": "New York Yankees",
            "commence_time": "2026-07-01T17:00:00Z", "bookmakers": [],
        }
        schedule = [{
            "game_pk": 1, "away_team_name": "Boston Red Sox", "home_team_name": "New York Yankees",
            "scheduled_start_utc": "2026-07-01T17:00:00Z", "game_number": 1,
        }]
        rows, audit = parse_events(
            events=[event], schedule=schedule, game_date="2026-07-01",
            fetched_at_utc="2026-07-01T15:00:00Z", run_tag="test",
            raw_source_path=str(self.provider_path), raw_source_sha256=self.provider_source.sha256,
            binding_receipts={}, require_verified_bindings=True,
        )
        self.assertEqual([], rows)
        self.assertEqual("BINDING_NOT_CERTIFIED", audit[0]["certification_status"])

    def test_pinnacle_verified_mode_preserves_price_and_adds_provenance(self):
        receipt = self.resolve()
        event = {
            "id": "evt", "away_team": "Boston Red Sox", "home_team": "New York Yankees",
            "commence_time": "2026-07-01T17:00:00Z",
            "bookmakers": [{"key": "pinnacle", "markets": [{
                "key": "h2h", "last_update": "2026-07-01T14:59:00Z",
                "outcomes": [{"name": "Boston Red Sox", "price": 120},
                             {"name": "New York Yankees", "price": -130}],
            }]}],
        }
        schedule = [{
            "game_pk": 1, "away_team_name": "Boston Red Sox", "home_team_name": "New York Yankees",
            "scheduled_start_utc": "2026-07-01T17:00:00Z", "game_number": 1,
        }]
        rows, _ = parse_events(
            events=[event], schedule=schedule, game_date="2026-07-01",
            fetched_at_utc="2026-07-01T15:00:00Z", run_tag="test",
            raw_source_path=str(self.provider_path), raw_source_sha256=self.provider_source.sha256,
            binding_receipts={"evt": receipt}, require_verified_bindings=True,
        )
        self.assertEqual(1, len(rows))
        self.assertEqual(120, rows[0]["away_american_price"])
        self.assertEqual(receipt.binding_identity_sha256, rows[0]["provider_event_game_binding_identity"])

    def test_feature_lineage_requires_provider_binding_provenance(self):
        identity = {
            "game_date": "2026-07-01", "game_id": 1, "player_id": 2,
            "prop_type": "hits", "line": 0.5, "selected_side": "over",
            "bookmaker_key": "betonlineag", "snapshot_run_tag": "run",
            "provider_event_id": "evt",
        }
        row = {key: "x" for key in prospective_lineage.MANDATORY}
        row.update({
            "canonical_row_identity": prospective_lineage.canonical_json(identity),
            "prediction_timestamp": "2026-07-01T15:00:00Z",
            "odds_snapshot_timestamp": "2026-07-01T14:59:00Z",
            "scheduled_game_start": "2026-07-01T17:00:00Z",
            "selected_side": "over", "price_over_american": -120,
            "price_under_american": 100, "model_artifact_sha256": "a" * 64,
            "feature_vector_sha256": "b" * 64, "feature_schema_sha256": "c" * 64,
            "configuration_sha256": "d" * 64,
        })
        self.assertEqual("LINEAGE_CERTIFIED", prospective_lineage.validate(row)[0])
        row["provider_event_binding_receipt_sha256"] = ""
        self.assertEqual("LINEAGE_BLOCKED_OTHER_MANDATORY_FIELD", prospective_lineage.validate(row)[0])


if __name__ == "__main__":
    unittest.main()
