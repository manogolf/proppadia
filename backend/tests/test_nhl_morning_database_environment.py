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


def test_sog_cold_start_is_lane_local(tmp_path, monkeypatch):
    output = tmp_path / "morning"
    env_file = tmp_path / "valid.env"
    env_file.write_text("SUPABASE_DB_URL=fixture-value\n")
    monkeypatch.setattr(sys, "argv", [
        str(PATH), "--slate-date", "2026-09-19", "--env-file", str(env_file),
        "--output-root", str(output), "--fixture-scenario", "sog_cold_start",
    ])
    assert MODULE.main() == 0
    health_path = next(output.rglob("morning_health.json"))
    health = json.loads(health_path.read_text())
    assert health["overall_status"] == "READY"
    assert health["sog_prerequisite_readiness"] == "BLOCKED_LANE_LOCAL_SEASON_TOI_UNAVAILABLE"
    assert health["downstream"]["SOG_MORNING_PREREQUISITES_READY"] is False
    assert health["downstream"]["MAINLINE_MORNING_PREREQUISITES_READY"] is True
    assert health["downstream"]["POINTS_MORNING_PREREQUISITES_READY"] is True
    assert health["downstream"]["SAVES_MORNING_PREREQUISITES_READY"] is True


OWNER = "postgresql://postgres.ownerref:owner-secret@db.invalid/postgres"
RESEARCH = "postgresql://proppadia_nhl_research_login.ref:r@db.invalid/postgres"
INGEST = "postgresql://proppadia_nhl_daily_login.ref:i@db.invalid/postgres"


def test_restricted_mode_children_never_inherit_the_owner_aliases():
    source = {"NHL_DB_ACCESS_MODE": "restricted", "SUPABASE_DB_URL": OWNER, "DATABASE_URL": OWNER,
              "NHL_RESEARCH_DATABASE_URL": RESEARCH, "NHL_INGEST_DATABASE_URL": INGEST}
    child = MODULE.resolved_database_environment(source)
    assert child["SUPABASE_DB_URL"] == child["DATABASE_URL"] == RESEARCH
    # The daily CLI still routes writers to their own logins.
    assert child["NHL_INGEST_DATABASE_URL"] == INGEST
    assert OWNER not in child.values()


def test_restricted_mode_without_research_route_fails_before_child():
    source = {"NHL_DB_ACCESS_MODE": "restricted", "SUPABASE_DB_URL": OWNER, "DATABASE_URL": OWNER}
    with pytest.raises(RuntimeError, match="NHL_DATABASE_CREDENTIAL_NHL_RESTRICTED_DATABASE_URL_REQUIRED"):
        MODULE.resolved_database_environment(source)


def test_morning_health_records_owner_login_names_only(tmp_path, monkeypatch, capsys):
    env_file = tmp_path / "restricted.env"
    env_file.write_text(f"NHL_DB_ACCESS_MODE=restricted\nSUPABASE_DB_URL={OWNER}\n"
                        f"DATABASE_URL=${{SUPABASE_DB_URL}}\nNHL_RESEARCH_DATABASE_URL={RESEARCH}\n")
    output = tmp_path / "morning"
    monkeypatch.setattr(sys, "argv", [
        str(PATH), "--slate-date", "2026-09-20", "--env-file", str(env_file),
        "--output-root", str(output), "--fixture-scenario", "valid_empty",
    ])
    assert MODULE.main() == 0
    capsys.readouterr()
    (health,) = output.rglob("morning_health.json")
    check = json.loads(health.read_text())["database_credential_check"]
    assert check == {"access_mode": "restricted", "child_environment_owner_free": True,
                     "owner_login_guard": "OFF_UNTIL_STEP_4",
                     "owner_login_keys_in_env_file": ["DATABASE_URL", "SUPABASE_DB_URL"]}
    assert "owner-secret" not in health.read_text()


def test_enforced_owner_login_guard_blocks_the_morning_run(tmp_path, monkeypatch, capsys):
    env_file = tmp_path / "guarded.env"
    env_file.write_text(f"NHL_OWNER_LOGIN_GUARD=enforce\nNHL_DB_ACCESS_MODE=restricted\n"
                        f"SUPABASE_DB_URL={OWNER}\nNHL_RESEARCH_DATABASE_URL={RESEARCH}\n")
    output = tmp_path / "morning"
    monkeypatch.setattr(sys, "argv", [
        str(PATH), "--slate-date", "2026-09-20", "--env-file", str(env_file),
        "--output-root", str(output), "--fixture-scenario", "valid_empty",
    ])
    assert MODULE.main() == 1
    capsys.readouterr()
    (health,) = output.rglob("morning_health.json")
    payload = json.loads(health.read_text())
    assert payload["blocking_failure"] == "NHL_OWNER_LOGIN_IN_APPLICATION_ALIAS:SUPABASE_DB_URL"
    assert payload["database_credential_check"]["owner_login_guard"] == "ENFORCED"
    assert "owner-secret" not in health.read_text()
