from pathlib import Path

from backend.mlb.scripts.write_mlb_rolling_integrity_receipt import build_receipt


def test_receipt_binds_output_run_dates_and_actual_pass_marker(tmp_path: Path):
    output = tmp_path / "rolling.log"
    output.write_text("PASS mlb rolling integrity window=2026-10-01..2026-10-10\n")
    receipt = build_receipt(
        run_id="local_daily_test", slate_date="2026-10-10",
        started_at_utc="2026-10-10T12:00:00Z", ended_at_utc="2026-10-10T12:01:00Z",
        exit_code=0, output_path=output,
    )
    assert receipt["status"] == "PASS"
    assert receipt["result_marker_present"] is True
    assert receipt["output_sha256"]
    assert receipt["run_id"] == "local_daily_test"
    assert receipt["slate_date"] == "2026-10-10"
    assert receipt["receipt_sha256"]


def test_receipt_never_calls_missing_marker_a_pass(tmp_path: Path):
    output = tmp_path / "rolling.log"
    output.write_text("rolling check completed with no summary marker\n")
    receipt = build_receipt(
        run_id="local_daily_test", slate_date="2026-10-10",
        started_at_utc="2026-10-10T12:00:00Z", ended_at_utc="2026-10-10T12:01:00Z",
        exit_code=0, output_path=output,
    )
    assert receipt["status"] == "COMPLETED_NO_PASS_MARKER"
    assert receipt["result_marker_present"] is False


def test_receipt_preserves_checker_failure(tmp_path: Path):
    output = tmp_path / "rolling.log"
    output.write_text("FAIL mlb rolling integrity failures=coverage\n")
    receipt = build_receipt(
        run_id="local_daily_test", slate_date="2026-10-10",
        started_at_utc="2026-10-10T12:00:00Z", ended_at_utc="2026-10-10T12:01:00Z",
        exit_code=1, output_path=output,
    )
    assert receipt["status"] == "FAIL"
    assert receipt["exit_code"] == 1
