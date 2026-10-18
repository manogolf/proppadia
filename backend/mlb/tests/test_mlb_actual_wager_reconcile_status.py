"""Offline status and missing-input behavior for daily actual-wager reconcile."""

import json
import sys
from pathlib import Path

import pytest

from backend.mlb.scripts import compare_mlb_model_v2_upload_vs_actual as reconcile
from backend.mlb.shared.model_authority import (
    actual_wager_reconcile_skip_reason,
    predictive_output_skip_reason,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_no_qualified_model_skips_reconcile_when_uploads_skipped_and_actual_csv_absent(tmp_path):
    authority = tmp_path / "model_authority.json"
    authority.write_text(json.dumps({"authority_status": "NO_QUALIFIED_MLB_MODEL"}))
    actual_csv = tmp_path / "actual_wagers_by_source_2026-10-17.csv"
    assert not actual_csv.exists()
    upload_statuses = {
        "book_upload": "SKIPPED operation=production_upload_generation",
        "ranking_upload": "skipped_no_qualified_model",
        "quick_card_upload": "skipped_no_qualified_model",
    }

    assert predictive_output_skip_reason(authority) == "NO_QUALIFIED_MLB_MODEL"
    skip_reason = actual_wager_reconcile_skip_reason(
        authority, upload_generation_status=upload_statuses["book_upload"]
    )
    assert skip_reason == "NO_QUALIFIED_MLB_MODEL"
    assert upload_statuses["ranking_upload"] == "skipped_no_qualified_model"
    assert upload_statuses["quick_card_upload"] == "skipped_no_qualified_model"

    reconciler_calls = []
    if skip_reason is None:
        reconciler_calls.append("invoked")
    assert reconciler_calls == []

    wrapper = (REPO_ROOT / "bin/proppadia_mlb_refresh_daily.sh").read_text()
    assert "actual_wager_reconcile_skip_reason" in wrapper
    assert '"${book_upload_guard_status}"' in wrapper
    assert 'actual_reconcile_build_status=skipped_no_qualified_model' in wrapper
    assert '.venv/bin/python -m backend.mlb.scripts.compare_mlb_model_v2_upload_vs_actual' in wrapper


def test_non_skip_authority_keeps_missing_uploads_on_failure_path(tmp_path, monkeypatch):
    authority = tmp_path / "model_authority.json"
    authority.write_text(json.dumps({"authority_status": "QUALIFIED"}))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["reconcile", "--date", "2026-10-17"])

    assert predictive_output_skip_reason(authority) is None
    assert actual_wager_reconcile_skip_reason(
        authority,
        upload_generation_status="SKIPPED operation=production_upload_generation",
    ) is None
    assert actual_wager_reconcile_skip_reason(
        authority,
        upload_generation_status="SUCCESS operation=production_upload_generation",
    ) is None
    with pytest.raises(SystemExit, match="Missing required input:.*ranking_tool_upload"):
        reconcile.main()

    wrapper = (REPO_ROOT / "bin/proppadia_mlb_refresh_daily.sh").read_text()
    assert 'if [[ "${actual_upload_skip_reason}" == "NO_QUALIFIED_MLB_MODEL" ]]' in wrapper
    assert "backend.mlb.scripts.compare_mlb_model_v2_upload_vs_actual" in wrapper
    assert "actual_reconcile_build_status=failed_or_empty" in wrapper
