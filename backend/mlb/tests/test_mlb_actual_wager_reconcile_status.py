"""Offline status and missing-input behavior for daily actual-wager reconcile."""

import json
import sys
from pathlib import Path

import pytest

from backend.mlb.scripts import compare_mlb_model_v2_upload_vs_actual as reconcile
from backend.mlb.shared.model_authority import predictive_output_skip_reason

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_no_qualified_model_has_an_explicit_upload_skip_reason(tmp_path):
    authority = tmp_path / "model_authority.json"
    authority.write_text(json.dumps({"authority_status": "NO_QUALIFIED_MLB_MODEL"}))

    assert predictive_output_skip_reason(authority) == "NO_QUALIFIED_MLB_MODEL"


def test_non_skip_authority_keeps_missing_uploads_on_failure_path(tmp_path, monkeypatch):
    authority = tmp_path / "model_authority.json"
    authority.write_text(json.dumps({"authority_status": "QUALIFIED"}))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["reconcile", "--date", "2026-10-17"])

    assert predictive_output_skip_reason(authority) is None
    with pytest.raises(SystemExit, match="Missing required input:.*ranking_tool_upload"):
        reconcile.main()

    wrapper = (REPO_ROOT / "bin/proppadia_mlb_refresh_daily.sh").read_text()
    assert 'if [[ "${actual_upload_skip_reason}" == "NO_QUALIFIED_MLB_MODEL" ]]' in wrapper
    assert "backend.mlb.scripts.compare_mlb_model_v2_upload_vs_actual" in wrapper
    assert "actual_reconcile_build_status=failed_or_empty" in wrapper
