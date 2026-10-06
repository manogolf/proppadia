from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from backend.mlb.scripts.write_mlb_daily_wrapper_receipt_v1 import write_receipt
from backend.mlb.scripts.install_mlb_natural_wrapper_run_receipt_hook_v1 import install


class NaturalWrapperReceiptTests(unittest.TestCase):
    def test_installed_wrapper_hook_is_backed_up_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            wrapper = Path(directory) / "wrapper.sh"
            original = (
                b"#!/bin/zsh\ntrap 'wrapper_rc=$?; release_launchagent_locks; "
                b"write_launchagent_summary \"$wrapper_rc\"; exit \"$wrapper_rc\"' EXIT\n"
            )
            wrapper.write_bytes(original)
            wrapper.chmod(0o755)
            result = install(wrapper)
            self.assertEqual(result["status"], "INSTALLED")
            self.assertIn(b"write_mlb_daily_wrapper_receipt_v1", wrapper.read_bytes())
            self.assertEqual(Path(result["backup_path"]).read_bytes(), original)
            self.assertEqual(install(wrapper)["status"], "ALREADY_INSTALLED")

    def test_successful_wrapper_run_retains_exact_zero_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = write_receipt(
                run_identity="local_daily_20261006T123004Z",
                started_at_utc="2026-10-06T12:30:04Z",
                finished_at_utc="2026-10-06T12:54:19Z",
                wrapper_rc=0,
                root=Path(directory),
            )
            payload = json.loads(Path(result["path"]).read_text())
            self.assertEqual(payload["run_identity"], "local_daily_20261006T123004Z")
            self.assertEqual(payload["wrapper_rc"], 0)
            self.assertEqual(payload["status"], "SUCCEEDED")
            self.assertEqual(payload["finished_at_utc"], "2026-10-06T12:54:19Z")

    def test_failed_wrapper_run_retains_nonzero_exit_without_inference(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = write_receipt(
                run_identity="local_daily_20261006T153005Z",
                started_at_utc="2026-10-06T15:30:05Z",
                finished_at_utc="2026-10-06T15:56:22Z",
                wrapper_rc=7,
                root=Path(directory),
            )
            payload = json.loads(Path(result["path"]).read_text())
            self.assertEqual(payload["wrapper_rc"], 7)
            self.assertEqual(payload["status"], "FAILED")

    def test_existing_run_identity_cannot_be_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            kwargs = {
                "run_identity": "local_daily_20261006T153005Z",
                "started_at_utc": "2026-10-06T15:30:05Z",
                "finished_at_utc": "2026-10-06T15:56:22Z",
                "wrapper_rc": 0,
                "root": Path(directory),
            }
            first = write_receipt(**kwargs)
            original = Path(first["path"]).read_bytes()
            with self.assertRaises(FileExistsError):
                write_receipt(**{**kwargs, "wrapper_rc": 12})
            self.assertEqual(Path(first["path"]).read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
