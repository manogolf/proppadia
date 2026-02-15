import json
import unittest

from backend.scripts import json_check_runner


class TestSharedJsonCheckRunner(unittest.TestCase):
    def test_parse_json_payload_direct(self):
        payload = json_check_runner.parse_json_payload('{"status":"pass"}')
        self.assertEqual(payload["status"], "pass")

    def test_parse_json_payload_with_prefix_noise(self):
        text = "WARN something happened\n" + json.dumps({"ok": True, "status": "pass"})
        payload = json_check_runner.parse_json_payload(text)
        self.assertTrue(payload["ok"])

    def test_run_json_check_parses_noisy_stdout(self):
        def _fn(_args):
            print("INFO before payload")
            print(json.dumps({"status": "pass"}))
            return 0

        rc, payload = json_check_runner.run_json_check(_fn, ["--json"])
        self.assertEqual(rc, 0)
        self.assertEqual(payload["status"], "pass")


if __name__ == "__main__":
    unittest.main()
