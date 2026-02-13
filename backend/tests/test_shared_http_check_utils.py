import unittest

from backend.scripts.http_check_utils import HttpClient, run_check


class _Resp:
    def __init__(self, status_code=200, json_body=None, text=""):
        self.status_code = status_code
        self._json_body = json_body
        self.text = text

    def json(self):
        if isinstance(self._json_body, Exception):
            raise self._json_body
        return self._json_body


class _Client:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def request(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        return self._response


class TestSharedHttpCheckUtils(unittest.TestCase):
    def test_http_client_exposed_for_callers(self):
        # Sanity check that shared HttpClient stays importable and constructible.
        client = HttpClient("https://example.com")
        self.assertTrue(hasattr(client, "request"))

    def test_run_check_success_with_validator(self):
        client = _Client(_Resp(status_code=200, json_body={"ok": True, "sport": "mlb"}))
        result = run_check(
            client,
            name="mlb_ping",
            method="GET",
            path="/api/mlb/ping",
            expected_status=[200],
            validate=lambda body: (body.get("ok") is True, "expects ok=true"),
        )
        self.assertTrue(result.ok)
        self.assertEqual(result.status, 200)
        self.assertIn("expects ok=true", result.detail)
        self.assertEqual(client.calls[0][0], "GET")
        self.assertEqual(client.calls[0][1], "/api/mlb/ping")

    def test_run_check_validator_error_marks_failure(self):
        client = _Client(_Resp(status_code=200, json_body={"ok": True}))
        result = run_check(
            client,
            name="boom",
            method="GET",
            path="/api/x",
            expected_status=[200],
            validate=lambda _body: (_ for _ in ()).throw(ValueError("bad validator")),
        )
        self.assertFalse(result.ok)
        self.assertIn("validator error", result.detail)

    def test_run_check_non_json_payload_uses_text_fallback(self):
        client = _Client(_Resp(status_code=500, json_body=ValueError("no json"), text="server down"))
        result = run_check(
            client,
            name="health",
            method="GET",
            path="/api/health",
            expected_status=[200],
        )
        self.assertFalse(result.ok)
        self.assertIn("server down", result.detail)


if __name__ == "__main__":
    unittest.main()
