import unittest
from unittest.mock import patch

from backend.shared.scripts.api_client_utils import HttpClient, first_keys, safe_json


class _Resp:
    def __init__(self, json_body=None, text=""):
        self._json_body = json_body
        self.text = text

    def json(self):
        if isinstance(self._json_body, Exception):
            raise self._json_body
        return self._json_body


class TestSharedApiClientUtils(unittest.TestCase):
    def test_safe_json_prefers_json(self):
        body = {"ok": True, "sport": "mlb"}
        self.assertEqual(safe_json(_Resp(json_body=body, text="fallback")), body)

    def test_safe_json_falls_back_to_text(self):
        out = safe_json(_Resp(json_body=ValueError("bad json"), text="plain text payload"))
        self.assertEqual(out, "plain text payload")

    def test_first_keys_for_dict(self):
        text = first_keys({"a": 1, "b": 2, "c": 3}, max_keys=2)
        self.assertIn('"a": 1', text)
        self.assertIn('"b": 2', text)
        self.assertNotIn('"c": 3', text)

    def test_first_keys_for_list(self):
        self.assertEqual(first_keys([1, 2, 3]), "list(len=3)")

    def test_http_client_request_builds_url(self):
        captured = {}

        def _fake_request(method, url, timeout=0, **kwargs):
            captured["method"] = method
            captured["url"] = url
            captured["timeout"] = timeout
            captured["kwargs"] = kwargs
            return _Resp(json_body={"ok": True})

        with patch("requests.request", side_effect=_fake_request):
            client = HttpClient("https://example.com/", timeout=25)
            client.request("GET", "/api/health", params={"x": 1})

        self.assertEqual(captured["method"], "GET")
        self.assertEqual(captured["url"], "https://example.com/api/health")
        self.assertEqual(captured["timeout"], 25)
        self.assertEqual(captured["kwargs"]["params"], {"x": 1})


if __name__ == "__main__":
    unittest.main()
