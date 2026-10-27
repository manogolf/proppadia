"""The NHL CLI keeps temporary files inside the repo's tmp/ folder."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

from backend.nhl import cli


def test_use_repo_temp_dir_routes_process_and_children(monkeypatch):
    for key in ("TMPDIR", "TMP", "TEMP"):
        monkeypatch.setenv(key, "/var/folders/system-temp")
    monkeypatch.setattr(tempfile, "tempdir", None)

    target = cli.use_repo_temp_dir()

    assert target == cli.ROOT / "tmp"
    for key in ("TMPDIR", "TMP", "TEMP"):
        assert os.environ[key] == str(target)
    with tempfile.TemporaryDirectory(prefix="nhl_sog_reconcile_") as temp_dir:
        assert Path(temp_dir).parent == target
    child = subprocess.run(
        [sys.executable, "-c", "import tempfile; print(tempfile.gettempdir())"],
        env=os.environ.copy(), capture_output=True, text=True, check=True,
    )
    assert Path(child.stdout.strip()) == target


def test_main_sets_repo_temp_dir_before_dispatch(monkeypatch):
    seen = {}
    for key in ("TMPDIR", "TMP", "TEMP"):
        monkeypatch.setenv(key, "/var/folders/system-temp")
    monkeypatch.setattr(tempfile, "tempdir", None)
    monkeypatch.setattr(cli, "build_sog", lambda slate: seen.setdefault("tmp", tempfile.gettempdir()))
    monkeypatch.setattr(cli, "guard_testing_only_slate", lambda *a, **k: None)

    cli.main(["build-sog", "--slate", "2026-10-27"])

    assert Path(seen["tmp"]) == cli.ROOT / "tmp"
