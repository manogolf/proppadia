import unittest

from backend.app.services.mlb.commit_tokens import sign_commit_payload, verify_commit_token


class TestCommitTokens(unittest.TestCase):
    def test_round_trip(self):
        token = sign_commit_payload({"flow": "t", "features": {"player_id": 1}}, ttl_seconds=120)
        payload = verify_commit_token(token)
        self.assertEqual(payload["flow"], "t")
        self.assertEqual(payload["features"]["player_id"], 1)

    def test_invalid_encoding_message(self):
        with self.assertRaises(ValueError) as ctx:
            verify_commit_token("bad.token")
        self.assertIn("invalid token encoding", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

