from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.mlb.identity.provider_event_game_binding_v1 import BindingRegistry, EvidenceSource
from backend.mlb.scripts import build_mlb_predictions_wide as wide
from backend.mlb.shared.mlb_api_v2 import _parse_schedule
from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT


class WideRuntimeSchedulePhaseAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(tempfile.mkdtemp(prefix="wide_runtime_phase_", dir=REPO_ROOT / "tmp"))

    def tearDown(self) -> None:
        shutil.rmtree(self.root)

    def test_retained_division_series_offer_uses_same_exact_game_schedule_authority(self) -> None:
        # Source fields are copied from the retained Oct 5 StatsAPI schedule for
        # gamePk 849834; provider commence time reflects the observed 10-minute
        # provider/official-start difference and remains within resolver policy.
        schedule_payload = {
            "dates": [{"games": [{
                "gamePk": 849834,
                "gameType": "D",
                "season": "2026",
                "officialDate": "2026-10-05",
                "gameDate": "2026-10-05T21:00:00Z",
                "seriesDescription": "AL Division Series",
                "status": {"abstractGameState": "Preview", "codedGameState": "S",
                           "detailedState": "Scheduled", "statusCode": "S"},
                "teams": {"home": {"team": {"id": 114, "name": "Cleveland Guardians"}},
                          "away": {"team": {"id": 145, "name": "Chicago White Sox"}}},
            }]}],
        }
        schedule_path = self.root / "schedule.json"
        schedule_raw = json.dumps(schedule_payload, separators=(",", ":")).encode()
        schedule_path.write_bytes(schedule_raw)
        schedule_source = EvidenceSource(str(schedule_path), hashlib.sha256(schedule_raw).hexdigest())
        game_rows = _parse_schedule("2026-10-05", schedule_payload)
        with patch.object(wide, "fetch_schedule_by_date_with_evidence", return_value=(
            game_rows, {"path": str(schedule_path), "sha256": schedule_source.sha256,
                        "observed_at_utc": "2026-10-05T12:30:00Z"},
        )):
            _, by_pair_games, parsed_schedule_source, schedule_observed_at = (
                wide._build_schedule_maps("2026-10-05", schedule_path))
        self.assertIn(("CLE", "CWS"), by_pair_games)
        game = by_pair_games[("CLE", "CWS")][0]
        provider_path = self.root / "odds.json"
        provider_path.write_text('{"events":[]}', encoding="utf-8")
        provider_source = EvidenceSource(
            str(provider_path), hashlib.sha256(provider_path.read_bytes()).hexdigest())
        offer = wide.Offer(
            event_id="retained-event-849834", commence_time="2026-10-05T21:10:00Z",
            home_team_name="Cleveland Guardians", away_team_name="Chicago White Sox",
            home_team_abbr="CLE", away_team_abbr="CWS", game_number=None,
            prop_type="hits", player_name="Test Batter", line=0.5,
            books_seen=1, books_two_sided=1, bookmaker_key="test",
            price_over_american=-110, price_under_american=-110,
            implied_over=0.52, implied_under=0.52, implied_over_novig=0.5,
            implied_under_novig=0.5, market_hold=0.04,
        )
        player = wide.PlayerRow(123, "Test Batter", "CLE", 114, True)
        common = dict(
            offers=[offer], by_name_team={(wide._norm_name("Test Batter"), "CLE"): [player]},
            by_pair_games=by_pair_games, schedule_source=parsed_schedule_source,
            schedule_observation_timestamp_utc=schedule_observed_at,
            provider_snapshot=provider_source, observation_timestamp_utc="2026-10-05T12:39:01Z",
            registry=BindingRegistry(),
        )

        rejected, rejected_counts = wide._resolve_offers(
            **common, authority=HashedProposalAuthority())
        self.assertEqual([], rejected)
        self.assertEqual(1, rejected_counts["skip_identity_canonical_authority_rejected"])

        authority = wide._runtime_phase_authority(schedule_source)
        resolved, counts = wide._resolve_offers(**common, authority=authority)
        self.assertEqual(1, len(resolved))
        self.assertEqual(849834, resolved[0].game.game_id)
        self.assertEqual("D", resolved[0].game.game_type)
        self.assertEqual(1, counts["resolved"])
        self.assertEqual("POSTSEASON", authority.lookup_exact(849834).season_phase)
        self.assertEqual("AL Division Series", authority.lookup_exact(849834).source_round)
        self.assertEqual(schedule_source.sha256, resolved[0].binding.official_schedule_source_sha256)


if __name__ == "__main__":
    unittest.main()
