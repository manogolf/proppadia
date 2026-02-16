import json
import unittest

from backend.scripts.probe_mlb_prediction_readiness import run


class _Resp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body

    def json(self):
        return self._body


class _Client:
    def __init__(self, responses):
        self._responses = responses
        self._idx = 0

    def request(self, method, path, **kwargs):
        resp = self._responses[self._idx]
        self._idx += 1
        return resp


class TestSharedMlbPredictionReadiness(unittest.TestCase):
    def test_run_passes_with_minimum_predict_success(self):
        responses = [
            _Resp(200, [{"player_id": 1, "team_id": 10, "player_name": "A"}]),  # /api/players
            _Resp(200, {"ok": True, "features": {"player_id": 1, "team_id": 10}}),  # prepareProp
            _Resp(200, {"commit_token": "tok"}),  # predict
        ]
        client = _Client(responses)
        # capture stdout by temporarily redirecting if needed
        import io
        from contextlib import redirect_stdout

        out = io.StringIO()
        with redirect_stdout(out):
            rc = run(
                client,
                game_date="2025-08-15",
                sample_size=1,
                require_min_success=1,
                prop_types=["hits"],
            )
        self.assertEqual(rc, 0)
        body = json.loads(out.getvalue())
        self.assertTrue(body["ok"])
        self.assertEqual(body["predict_success"], 1)
        self.assertEqual(body["per_prop"]["hits"]["predict_success"], 1)

    def test_run_fails_when_prepare_and_predict_fail(self):
        responses = [
            _Resp(200, [{"player_id": 1, "team_id": 10, "player_name": "A"}]),  # /api/players
            _Resp(500, {"ok": False}),  # prepareProp fail
        ]
        client = _Client(responses)
        import io
        from contextlib import redirect_stdout

        out = io.StringIO()
        with redirect_stdout(out):
            rc = run(
                client,
                game_date="2025-08-15",
                sample_size=1,
                require_min_success=1,
                prop_types=["hits"],
            )
        self.assertEqual(rc, 1)
        body = json.loads(out.getvalue())
        self.assertFalse(body["ok"])
        self.assertEqual(body["prepare_success"], 0)
        self.assertEqual(body["predict_success"], 0)
        self.assertEqual(body["failure_count"], 1)
        self.assertEqual(body["per_prop"]["hits"]["failure_count"], 1)

    def test_run_uses_lookup_fallback_when_players_list_empty(self):
        responses = [
            _Resp(200, []),  # /api/players
            _Resp(200, {"ok": True, "found": True, "player_id": 660271, "team_id": 119, "player_name": "Shohei Ohtani"}),  # /api/players/lookup
            _Resp(200, {"ok": True, "features": {"player_id": 660271, "team_id": 119}}),  # prepareProp
            _Resp(200, {"commit_token": "tok"}),  # predict
        ]
        client = _Client(responses)
        import io
        from contextlib import redirect_stdout

        out = io.StringIO()
        with redirect_stdout(out):
            rc = run(
                client,
                game_date="2025-08-15",
                sample_size=1,
                require_min_success=1,
                prop_types=["hits"],
            )
        self.assertEqual(rc, 0)
        body = json.loads(out.getvalue())
        self.assertTrue(body["ok"])
        self.assertEqual(body["sample_loaded"], 1)
        self.assertEqual(body["predict_success"], 1)


if __name__ == "__main__":
    unittest.main()
