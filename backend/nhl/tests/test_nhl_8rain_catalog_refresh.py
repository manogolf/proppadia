from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from backend.nhl.scripts.refresh_nhl_8rain_catalog import (
    ENDPOINTS,
    fetch_catalog,
    get_current_catalog,
    validate_catalog,
)


def _bundle_bytes() -> dict[str, bytes]:
    values = {
        "model_spec.json": {
            "league": {"code": "nhl"}, "markets": {"h2h": {}},
            "stats": [{"code": "shots_on_goal", "bet": ["over", "under"]}],
        },
        "teams.json": {"data": [{"code": "team-a", "name": "Team A", "abbreviation": "AAA"}]},
        "players.json": {"data": [{"code": "player-a", "name": "Player A", "team": "team-a"}]},
    }
    return {name: (json.dumps(value) + "\n").encode() for name, value in values.items()}


def _write_retained(root: Path, *, stamp: str, slate: str | None = None) -> Path:
    path = root / ("retrieval=" + stamp.replace(":", "").replace("-", ""))
    path.mkdir(parents=True)
    bodies = _bundle_bytes()
    hashes = {}
    for name, body in bodies.items():
        (path / name).write_bytes(body)
        hashes[name] = {"sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    (path / "catalog_metadata.json").write_text(json.dumps({
        "schema_version": "NHL_8RAIN_CATALOG_BUNDLE_V1",
        "league_code": "nhl", "slate_date": slate or "2026-10-02",
        "retrieved_at_utc": stamp, "endpoints": {
            name: "https://app.8rainstation.com/public/api/catalog/" + endpoint
            for name, endpoint in ENDPOINTS.items()
        }, "files": hashes,
    }))
    return path


class NHL8RainCatalogRefreshTests(unittest.TestCase):
    def test_same_day_validated_catalog_is_reused_without_fetch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            slate_date = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date().isoformat()
            expected, _ = fetch_catalog(root, slate_date=slate_date, fetch_json=Mock(side_effect=_bundle_bytes().values()))
            fetch = Mock(side_effect=AssertionError("should reuse same-day catalog"))
            selected, info = get_current_catalog(root, slate_date=slate_date, fetch_json=fetch)
            self.assertEqual(selected, expected)
            self.assertEqual(info["catalog_use"], "REUSED")
            fetch.assert_not_called()

    def test_missing_catalog_fetches_and_retains_validated_immutable_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payloads = _bundle_bytes()
            fetch = Mock(side_effect=lambda url: payloads[next(
                name for name, endpoint in ENDPOINTS.items() if url.endswith(endpoint)
            )])
            path, info = get_current_catalog(root, slate_date="2026-10-02", fetch_json=fetch)
            self.assertEqual(fetch.call_count, 3)
            self.assertEqual(info["catalog_use"], "FRESHLY_FETCHED")
            self.assertEqual(info["league_code"], "nhl")
            self.assertEqual(info["player_count"], 1)
            self.assertEqual(info["team_count"], 1)
            self.assertEqual(validate_catalog(path)["catalog_sha256"], info["catalog_sha256"])
            self.assertEqual(len(list(root.glob("retrieval=*"))), 1)

    def test_second_same_day_resolution_reuses_catalog(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            slate_date = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date().isoformat()
            payloads = _bundle_bytes()
            fetch = Mock(side_effect=lambda url: payloads[next(
                name for name, endpoint in ENDPOINTS.items() if url.endswith(endpoint)
            )])
            first, _ = get_current_catalog(root, slate_date=slate_date, fetch_json=fetch)
            second, info = get_current_catalog(root, slate_date=slate_date, fetch_json=fetch)
            self.assertEqual(first, second)
            self.assertEqual(fetch.call_count, 3)
            self.assertEqual(info["catalog_use"], "REUSED")

    def test_prior_day_catalog_is_not_labeled_current_or_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_retained(root, stamp="2026-10-02T02:00:00Z", slate="2026-10-01")
            payloads = _bundle_bytes()
            fetch = Mock(side_effect=lambda url: payloads[next(
                name for name, endpoint in ENDPOINTS.items() if url.endswith(endpoint)
            )])
            path, info = get_current_catalog(root, slate_date="2026-10-02", fetch_json=fetch)
            self.assertEqual(info["catalog_use"], "FRESHLY_FETCHED")
            self.assertNotEqual(path.name, "retrieval=20261002T020000Z")
            self.assertEqual(fetch.call_count, 3)

    def test_malformed_or_hash_invalid_catalog_fails_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = _write_retained(root, stamp="2026-10-02T15:00:00Z")
            (path / "players.json").write_text('{"data": []}')
            with self.assertRaisesRegex(ValueError, "CATALOG_BYTE_COUNT_MISMATCH|CATALOG_HASH_MISMATCH|SCHEMA_INVALID"):
                validate_catalog(path)

    def test_player_catalog_limit_is_bounded_and_exact_limit_requires_pagination(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payloads = _bundle_bytes()
            players = {"data": [
                {"code": f"player-{i}", "name": f"Player {i}", "team": "team-a"}
                for i in range(2000)
            ]}
            payloads["players.json"] = (json.dumps(players) + "\n").encode()
            fetch = Mock(side_effect=lambda url: payloads[next(
                name for name, endpoint in ENDPOINTS.items() if url.endswith(endpoint)
            )])
            with self.assertRaisesRegex(ValueError, "8RAIN_PLAYER_CATALOG_LIMIT_REACHED"):
                fetch_catalog(root, slate_date="2026-10-02", fetch_json=fetch)

    def test_default_limit_catalog_bundle_is_not_reused_as_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = _write_retained(root, stamp="2026-10-02T15:00:00Z")
            metadata_path = path / "catalog_metadata.json"
            metadata = json.loads(metadata_path.read_text())
            metadata["endpoints"]["players.json"] = (
                "https://app.8rainstation.com/public/api/catalog/players?league=nhl"
            )
            metadata_path.write_text(json.dumps(metadata))
            with self.assertRaisesRegex(ValueError, "8RAIN_PLAYER_CATALOG_LIMIT_UNSPECIFIED"):
                validate_catalog(path)

    def test_fetch_failure_leaves_internal_prediction_bytes_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prediction = root / "internal_predictions.csv"
            original = b"player_id,p_over\n123,0.6\n"
            prediction.write_bytes(original)
            def fail(_url):
                raise RuntimeError("network unavailable")
            with self.assertRaisesRegex(RuntimeError, "network unavailable"):
                get_current_catalog(root / "catalog", slate_date="2026-10-02", fetch_json=fail)
            self.assertEqual(prediction.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
