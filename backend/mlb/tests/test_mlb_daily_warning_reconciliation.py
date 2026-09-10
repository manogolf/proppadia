from pathlib import Path

from backend.mlb.scripts.report_mlb_daily_ops_brief import _bvp_prewarm_failure_note


def test_bvp_status_uses_current_structured_run_and_excludes_historical_stderr(tmp_path):
    out_log = tmp_path / "out.log"
    err_log = tmp_path / "err.log"
    out_log.write_text("[old] START local MLB BvP prewarm (MLB_DATE_ET=2026-09-10)\n")
    err_log.write_text(
        "old traceback: failed to resolve statsapi.mlb.com\n"
        "[now] BVP_PREWARM_RUN_START run_tag=local_prewarm_new MLB_DATE_ET=2026-09-10\n"
        "[now] BVP_PREWARM_RUN_END run_tag=local_prewarm_new MLB_DATE_ET=2026-09-10 "
        "wrapper_rc=0 acquisition_status=SUCCESS downstream_status=SKIPPED_NO_QUALIFIED_MODEL "
        "impact_status=SKIPPED_NO_QUALIFIED_MODEL\n"
    )
    note = _bvp_prewarm_failure_note(
        current_slate_date="2026-09-10",
        completed_slate_date="2026-09-09",
        out_log=out_log,
        err_log=err_log,
    )
    assert "acquisition succeeded" in note
    assert "SKIPPED_NO_QUALIFIED_MODEL" in note
    assert "DNS" not in note


def test_bvp_legacy_log_does_not_reuse_historical_dns_error(tmp_path):
    out_log = tmp_path / "out.log"
    err_log = tmp_path / "err.log"
    out_log.write_text("START local MLB BvP prewarm (MLB_DATE_ET=2026-09-10)\n")
    err_log.write_text("2026-09-01 failed to resolve statsapi.mlb.com\n")
    note = _bvp_prewarm_failure_note(
        current_slate_date="2026-09-10",
        completed_slate_date="2026-09-09",
        out_log=out_log,
        err_log=err_log,
    )
    assert "historical stderr was excluded" in note
    assert "DNS" not in note


def test_bvp_structured_failure_does_not_read_past_matching_end_marker(tmp_path):
    out_log = tmp_path / "out.log"
    err_log = tmp_path / "err.log"
    out_log.write_text("")
    err_log.write_text(
        "[now] BVP_PREWARM_RUN_START run_tag=bounded_run MLB_DATE_ET=2026-09-10\n"
        "[now] acquisition failed without network detail\n"
        "[now] BVP_PREWARM_RUN_END run_tag=bounded_run MLB_DATE_ET=2026-09-10 "
        "wrapper_rc=7 acquisition_status=FAILED downstream_status=NOT_STARTED "
        "impact_status=NOT_STARTED\n"
        "[later] failed to resolve statsapi.mlb.com\n"
    )
    note = _bvp_prewarm_failure_note(
        current_slate_date="2026-09-10",
        completed_slate_date="2026-09-09",
        out_log=out_log,
        err_log=err_log,
    )
    assert "current prewarm invocation failed" in note
    assert "DNS" not in note
