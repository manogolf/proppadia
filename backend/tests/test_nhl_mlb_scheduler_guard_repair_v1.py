"""No-network guard fixtures; no real slate/canary or production artifacts."""
import contextlib
import io
import json
import multiprocessing
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as nhl
from backend.mlb.scripts import dh_forward_automation_common as dh

NOW = datetime(2026, 10, 20, 19, 0, tzinfo=timezone.utc)
SLATE = "2026-10-20"


def export_fixture(dsn, slate, directory):
    directory.mkdir(parents=True)
    schedule, history = directory / "schedule.csv", directory / "history.csv"
    schedule.write_text("fixture\n")
    history.write_text("fixture\n")
    return schedule, history, pd.DataFrame({"scheduled_start_time_utc": ["2026-10-21T02:00:00Z"]})


def simulated_publication(*args, **kwargs):
    return Path(args[3]) / "fixture_publication"


def nhl_worker(root, counter, ready=None, release=None, crash=None, queue=None):
    def fetch(key, path):
        if crash == "before":
            os._exit(91)
        with counter.get_lock():
            counter.value += 1
        if ready:
            ready.set()
            if not release.wait(10):
                raise RuntimeError("FIXTURE_WAIT_TIMEOUT")
        if crash == "after":
            os._exit(92)
        path.write_text(json.dumps({"quota": {"credits_consumed": 1},
                                    "capture_timestamp_utc": NOW.isoformat()}))
    with patch.object(nhl, "utc_now", return_value=NOW), \
         patch.object(nhl, "export_inputs", side_effect=export_fixture), \
         patch.object(nhl, "fetch_markets", side_effect=fetch), \
         patch.object(nhl, "run_capture", side_effect=simulated_publication), \
         patch("urllib.request.urlopen", side_effect=AssertionError("NETWORK_FORBIDDEN")):
        try:
            status = nhl.observe(Path(root), SLATE, "MIDDAY", False, "fixture")
            state = json.loads(status.read_text())["status"]
        except RuntimeError as error:
            state = str(error)
        if queue:
            queue.put(state)


def dh_worker(path, identity, entered, release, active, maximum, queue):
    replace = os.replace
    def observed_replace(source, destination):
        with active.get_lock():
            active.value += 1
            with maximum.get_lock():
                maximum.value = max(maximum.value, active.value)
        entered.set()
        if not release.wait(10):
            raise RuntimeError("FIXTURE_WAIT_TIMEOUT")
        try:
            replace(source, destination)
        finally:
            with active.get_lock():
                active.value -= 1
    try:
        with patch.object(dh.os, "replace", side_effect=observed_replace):
            dh.publish_rolling_status(Path(path), {"writer": identity, "body": "x" * 10000})
        queue.put("PASS")
    except Exception as error:
        queue.put(str(error))


class SchedulerGuardRepairTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="scheduler_guard_unit_")
        self.root = Path(self.tmp.name)
        self.ctx = multiprocessing.get_context("fork")

    def tearDown(self):
        self.tmp.cleanup()

    def join_ok(self, process, code=0):
        process.join(15)
        if process.is_alive():
            process.terminate()
            process.join()
            self.fail("fixture process exceeded bound")
        self.assertEqual(process.exitcode, code)

    def test_two_concurrent_nhl_entries_exactly_one_simulated_request(self):
        counter = self.ctx.Value("i", 0)
        ready, release, queue = self.ctx.Event(), self.ctx.Event(), self.ctx.Queue()
        first = self.ctx.Process(target=nhl_worker, args=(str(self.root), counter, ready, release, None, queue))
        first.start()
        try:
            self.assertTrue(ready.wait(10))
            second = self.ctx.Process(target=nhl_worker, args=(str(self.root), counter, None, None, None, queue))
            second.start()
            self.join_ok(second)
            self.assertEqual(queue.get(timeout=2), "CROSS_MARKET_ACQUISITION_ALREADY_RUNNING")
        finally:
            release.set()
            self.join_ok(first)
        self.assertEqual(queue.get(timeout=2), "CAPTURED")
        nhl_worker(str(self.root), counter, queue=queue)
        self.assertEqual(queue.get(timeout=2), "NOOP_PAID_ATTEMPT_ALREADY_EXISTS")
        self.assertEqual(counter.value, 1)

    def test_crash_before_request_claim_blocks_retry(self):
        self.check_crash("before", 91, 0)

    def test_crash_after_request_claim_blocks_retry(self):
        self.check_crash("after", 92, 1)

    def check_crash(self, mode, code, expected):
        counter, queue = self.ctx.Value("i", 0), self.ctx.Queue()
        process = self.ctx.Process(target=nhl_worker, args=(str(self.root), counter, None, None, mode))
        process.start()
        self.join_ok(process, code)
        nhl_worker(str(self.root), counter, queue=queue)
        self.assertEqual(queue.get(timeout=2), "NOOP_PAID_ATTEMPT_ALREADY_EXISTS")
        self.assertEqual(counter.value, expected)
        self.assertEqual(len(list(self.root.glob("paid_attempt_claims/*/*.claim.json"))), 1)

    def test_unwritable_claim_fails_closed(self):
        original = nhl.durable_json
        def fail_claim(path, payload, **kwargs):
            if kwargs.get("create_only"):
                raise OSError("CLAIM_WRITE_FAILED")
            return original(path, payload, **kwargs)
        with patch.object(nhl, "utc_now", return_value=NOW), \
             patch.object(nhl, "export_inputs", side_effect=export_fixture), \
             patch.object(nhl, "durable_json", side_effect=fail_claim), \
             patch.object(nhl, "fetch_markets") as fetch:
            status = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
        fetch.assert_not_called()
        self.assertEqual(json.loads(status.read_text())["status"], "FAILED_WARN_ONLY")

    def test_request_failure_is_warn_only_and_retry_blocked(self):
        with patch.object(nhl, "utc_now", return_value=NOW), \
             patch.object(nhl, "export_inputs", side_effect=export_fixture), \
             patch.object(nhl, "fetch_markets", side_effect=RuntimeError("SIMULATED_REQUEST_FAILURE")) as fetch:
            first = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
            second = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(json.loads(first.read_text())["status"], "FAILED_WARN_ONLY")
        self.assertEqual(json.loads(second.read_text())["status"], "NOOP_PAID_ATTEMPT_ALREADY_EXISTS")

    def test_publication_failure_preserves_paid_result_and_blocks_retry(self):
        def fetch(key, path):
            path.write_text(json.dumps({"quota": {"credits_consumed": 1},
                                        "capture_timestamp_utc": NOW.isoformat()}))
        with patch.object(nhl, "utc_now", return_value=NOW), \
             patch.object(nhl, "export_inputs", side_effect=export_fixture), \
             patch.object(nhl, "fetch_markets", side_effect=fetch) as mocked, \
             patch.object(nhl, "run_capture", side_effect=RuntimeError("PUBLICATION_FAILURE")):
            first = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
            second = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
        result = json.loads(first.read_text())
        self.assertEqual(mocked.call_count, 1)
        self.assertEqual(result["status"], "FAILED_WARN_ONLY")
        self.assertEqual(result["live_credits_consumed"], 1)
        self.assertEqual(json.loads(second.read_text())["status"], "NOOP_PAID_ATTEMPT_ALREADY_EXISTS")

    def test_pre_request_status_failure_makes_no_request_and_claim_blocks_retry(self):
        original = nhl.durable_json
        def fail_intent(path, payload, **kwargs):
            if payload.get("status") == "REQUEST_ATTEMPT_IN_PROGRESS":
                raise OSError("INTENT_STATUS_FAILURE")
            return original(path, payload, **kwargs)
        with patch.object(nhl, "utc_now", return_value=NOW), \
             patch.object(nhl, "export_inputs", side_effect=export_fixture), \
             patch.object(nhl, "durable_json", side_effect=fail_intent), \
             patch.object(nhl, "fetch_markets") as fetch:
            first = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
            second = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
        fetch.assert_not_called()
        self.assertEqual(json.loads(first.read_text())["status"], "FAILED_WARN_ONLY")
        self.assertEqual(json.loads(second.read_text())["status"], "NOOP_PAID_ATTEMPT_ALREADY_EXISTS")

    def test_partial_claim_blocks_retry_without_reinterpreting_it(self):
        claim = self.root / "paid_attempt_claims" / SLATE / "MIDDAY_partial.claim.json"
        claim.parent.mkdir(parents=True)
        claim.write_bytes(b"")
        with patch.object(nhl, "utc_now", return_value=NOW), \
             patch.object(nhl, "export_inputs", side_effect=export_fixture), \
             patch.object(nhl, "fetch_markets") as fetch:
            status = nhl.observe(self.root, SLATE, "MIDDAY", False, "fixture")
        fetch.assert_not_called()
        self.assertEqual(json.loads(status.read_text())["status"], "NOOP_PAID_ATTEMPT_ALREADY_EXISTS")
        self.assertEqual(claim.read_bytes(), b"")

    def test_phase_separation_and_explicit_force_override(self):
        def fetch(key, path):
            path.write_text(json.dumps({"quota": {"credits_consumed": 1},
                                        "capture_timestamp_utc": NOW.isoformat()}))
        with patch.object(nhl, "utc_now", return_value=NOW), \
             patch.object(nhl, "export_inputs", side_effect=export_fixture), \
             patch.object(nhl, "fetch_markets", side_effect=fetch) as mocked, \
             patch.object(nhl, "run_capture", side_effect=simulated_publication):
            for phase, force in [("MIDDAY", False), ("FINAL_PREGAME", False), ("MIDDAY", True)]:
                nhl.observe(self.root, SLATE, phase, force, "fixture")
        self.assertEqual(mocked.call_count, 3)
        claims = [json.loads(p.read_text()) for p in self.root.glob("paid_attempt_claims/*/*.claim.json")]
        self.assertEqual(sum(c["explicit_operator_override"] for c in claims), 1)

    def test_auto_phase_boundaries_unchanged(self):
        schedule = pd.DataFrame({"scheduled_start_time_utc": ["2026-10-21T02:00:00Z"]})
        self.assertEqual(nhl.phase_for(schedule, NOW, "AUTO", False)[0], "MIDDAY")
        final = datetime(2026, 10, 21, 1, 0, tzinfo=timezone.utc)
        self.assertEqual(nhl.phase_for(schedule, final, "AUTO", False)[0], "FINAL_PREGAME")
        self.assertIsNone(nhl.phase_for(schedule, datetime(2026, 10, 21, 2, 0, tzinfo=timezone.utc), "AUTO", False)[0])

    def test_main_lock_failure_keeps_exit_zero(self):
        with patch("sys.argv", ["runner", "--env-file", str(self.root / "absent")]), \
             patch.object(nhl, "observe", side_effect=RuntimeError("BUSY")), \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(nhl.main(), 0)

    def test_dh_concurrent_publication_serialized_and_atomic(self):
        path = self.root / "status.json"
        path.write_text('{"writer":"original"}')
        active, maximum = self.ctx.Value("i", 0), self.ctx.Value("i", 0)
        entered, release, queue = self.ctx.Event(), self.ctx.Event(), self.ctx.Queue()
        first = self.ctx.Process(target=dh_worker, args=(str(path), "capture", entered, release, active, maximum, queue))
        first.start()
        try:
            self.assertTrue(entered.wait(10))
            second = self.ctx.Process(target=dh_worker, args=(str(path), "grade", entered, release, active, maximum, queue))
            second.start()
            self.assertEqual(json.loads(path.read_text())["writer"], "original")
        finally:
            release.set()
            self.join_ok(first)
        self.join_ok(second)
        self.assertEqual([queue.get(timeout=2), queue.get(timeout=2)], ["PASS", "PASS"])
        self.assertEqual(maximum.value, 1)
        self.assertEqual(json.loads(path.read_text())["writer"], "grade")
        self.assertEqual(list(self.root.glob("*.tmp")), [])

    def test_dh_replace_failure_preserves_prior_status(self):
        path = self.root / "status.json"
        path.write_text('{"prior":true}')
        before = path.read_bytes()
        with patch.object(dh.os, "replace", side_effect=OSError("SIMULATED_REPLACE_FAILURE")):
            with self.assertRaises(OSError):
                dh.publish_rolling_status(path, {"new": True})
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(list(self.root.glob("*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
