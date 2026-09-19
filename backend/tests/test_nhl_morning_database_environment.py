from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "backend/nhl/scripts/run_nhl_morning_orchestration.py"
SPEC = importlib.util.spec_from_file_location("nhl_morning_orchestration", PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_resolved_secret_reaches_child_without_argv_or_other_env_changes():
    secret = "resolved-database-value"
    source = {"SUPABASE_DB_URL": secret, "DATABASE_URL": "${SUPABASE_DB_URL}", "KEEP_ME": "yes"}
    child = MODULE.resolved_database_environment(source)
    command = [str(value) for value in MODULE.database_check_command()]
    assert child["SUPABASE_DB_URL"] == secret
    assert child["DATABASE_URL"] == secret
    assert child["KEEP_ME"] == "yes"
    assert secret not in " ".join(command)


@pytest.mark.parametrize("source", [
    {"SUPABASE_DB_URL": "${SUPABASE_DB_URL}", "DATABASE_URL": "$SUPABASE_DB_URL"},
    {},
    {"SUPABASE_DB_URL": "", "DATABASE_URL": ""},
])
def test_invalid_database_environment_fails_before_child(source):
    with pytest.raises(RuntimeError, match="NHL_DATABASE_CREDENTIAL"):
        MODULE.resolved_database_environment(source)


def test_normal_and_recovery_use_same_environment_boundary():
    assert MODULE.SECOND_RECOVERY_AUTHORIZATION
    assert MODULE.database_check_command() == MODULE.database_check_command()


def test_validation_failure_releases_lock_and_secret_stays_out_of_artifacts(
    tmp_path, monkeypatch, capsys
):
    output = tmp_path / "morning"
    invalid = tmp_path / "invalid.env"
    invalid.write_text("SUPABASE_DB_URL=${SUPABASE_DB_URL}\nDATABASE_URL=$SUPABASE_DB_URL\n")
    monkeypatch.setattr(sys, "argv", [
        str(PATH), "--slate-date", "2026-09-20", "--env-file", str(invalid),
        "--output-root", str(output), "--fixture-scenario", "valid_empty",
    ])
    assert MODULE.main() == 1

    secret = "resolved-database-value"
    valid = tmp_path / "valid.env"
    valid.write_text(f"SUPABASE_DB_URL={secret}\nDATABASE_URL=${{SUPABASE_DB_URL}}\nKEEP_ME=yes\n")
    monkeypatch.setattr(sys, "argv", [
        str(PATH), "--slate-date", "2026-09-20", "--env-file", str(valid),
        "--output-root", str(output), "--fixture-scenario", "valid_empty",
        "--recovery-authorization", MODULE.SECOND_RECOVERY_AUTHORIZATION,
    ])
    assert MODULE.main() == 0
    captured = capsys.readouterr()
    assert secret not in captured.out
    assert secret not in captured.err
    for artifact in output.rglob("*"):
        if artifact.is_file():
            assert secret not in artifact.read_text(errors="replace")
    health_files = sorted(output.rglob("morning_health.json"))
    assert len(health_files) == 2
    recovered = next(
        payload for payload in (json.loads(path.read_text()) for path in health_files)
        if payload.get("recovery_authorization")
    )
    assert recovered["recovery_authorization"] == MODULE.SECOND_RECOVERY_AUTHORIZATION
