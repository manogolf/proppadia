from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

from backend.mlb.public_game_predictions.pythagorean_log5_v1 import (
    BETTING_AUTHORITY,
    PROP_AUTHORITY,
    feature_enabled,
)
from backend.mlb.shared.model_authority import (
    BLOCKED_STATUS,
    MLBPredictiveModelBlocked,
    assert_predictive_model_qualified,
)

ROOT = Path(__file__).resolve().parents[3]
GUARD = ROOT / "bin/mlb_slate_output_guarded.sh"


def _run(tmp_path: Path, stderr: str, rc: int, *, artifact: bool = False, parent: bool = False):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    artifact_path = tmp_path / "slate.csv"
    make = fake_bin / "make"
    make.write_text(
        "#!/bin/zsh\n"
        + (f"print created > {artifact_path!s}\n" if artifact else "")
        + f"print -r -- {stderr!r} >&2\nexit {rc}\n",
        encoding="utf-8",
    )
    make.chmod(make.stat().st_mode | stat.S_IXUSR)
    env = {**os.environ, "PATH": f"{fake_bin}:/usr/bin:/bin"}
    command = [str(GUARD), "MLB_DATE=2026-08-05"]
    if parent:
        command = ["/bin/zsh", "-c", f"{GUARD} MLB_DATE=2026-08-05; print NORMAL_REFRESH_COMPLETION"]
    result = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True)
    return result, artifact_path


EXACT_BLOCK = """Traceback (most recent call last):
backend.mlb.shared.model_authority.MLBPredictiveModelBlocked: MLB_PREDICTIVE_MODEL_BLOCKED_NO_QUALIFIED_MODEL: operation=production_slate_generation
make: *** [mlb-slate-output] Error 1"""


def test_exact_authority_block_is_expected_skip_and_parent_continues(tmp_path):
    result, artifact = _run(tmp_path, EXACT_BLOCK, 2, parent=True)
    assert result.returncode == 0
    assert "SKIP MLB slate output: NO_QUALIFIED_MLB_MODEL" in result.stdout
    assert "NORMAL_REFRESH_COMPLETION" in result.stdout
    assert "MLBPredictiveModelBlocked" in result.stderr
    assert not artifact.exists()


def test_unrelated_failure_preserves_original_status(tmp_path):
    result, _ = _run(tmp_path, "OSError: disk full", 17)
    assert result.returncode == 17
    assert "SKIP MLB slate output" not in result.stdout
    assert "disk full" in result.stderr


def test_malformed_authority_error_remains_fatal(tmp_path):
    malformed = "MLBPredictiveModelBlocked: MLB_PREDICTIVE_MODEL_BLOCKED_NO_QUALIFIED_MODEL"
    result, _ = _run(tmp_path, malformed, 18)
    assert result.returncode == 18
    assert "SKIP MLB slate output" not in result.stdout


def test_authority_and_public_surfaces_remain_fail_closed():
    try:
        assert_predictive_model_qualified("production_slate_generation")
    except MLBPredictiveModelBlocked as exc:
        assert BLOCKED_STATUS in str(exc)
    else:
        raise AssertionError("retired-model authority unexpectedly qualified")
    assert feature_enabled({}) is False
    assert BETTING_AUTHORITY == "NO_QUALIFIED_MLB_BETTING_MODEL"
    assert PROP_AUTHORITY == "NO_QUALIFIED_MLB_PROP_MODEL"


def test_moneyline_hook_contract_is_unchanged():
    hook = (ROOT / "bin/mlb_public_game_moneyline_daily_hook.sh").read_text(encoding="utf-8")
    assert "--skip-if-designated-snapshot-exists" in hook
    assert "--write-durable" in hook
    assert "mlb_slate_output_guarded" not in hook
