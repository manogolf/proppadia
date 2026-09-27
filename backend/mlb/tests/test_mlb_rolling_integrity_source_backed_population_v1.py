from __future__ import annotations

from backend.mlb.scripts import check_mlb_rolling_integrity as rolling


def test_feature_requirement_excludes_only_unavailable_or_ambiguous_sources():
    assert rolling._classify_mtp_feature_requirement(
        "hits", pds_rows=1, player_game_count=1, source_value=0.7,
    ) == "FEATURE_REQUIRED"
    assert rolling._classify_mtp_feature_requirement(
        "hits", pds_rows=1, player_game_count=2, source_value=0.7,
    ) == "AMBIGUOUS_EXACT_GAME_DAY"
    assert rolling._classify_mtp_feature_requirement(
        "hits", pds_rows=1, player_game_count=0, source_value=0.7,
    ) == "NO_EXACT_PLAYER_GAME_DAY"
    assert rolling._classify_mtp_feature_requirement(
        "hits", pds_rows=1, player_game_count=1, source_value=None,
    ) == "PROP_SPECIFIC_D7_VALUE_UNAVAILABLE"
    assert rolling._classify_mtp_feature_requirement(
        "unsupported", pds_rows=1, player_game_count=1, source_value=0.7,
    ) == "UNMAPPED_PROP_TYPE"


def test_coverage_uses_source_backed_required_rows_and_reconciles(monkeypatch):
    seen = {}

    def fake_fetchone(sql, params):
        seen["sql"] = sql
        seen["params"] = params
        return {
            "rows_total": 23098,
            "required_rows": 22400,
            "d7_nonnull": 22390,
            "required_null": 10,
            "not_required_rows": 698,
            "no_player_daily_feature_row": 160,
            "ambiguous_daily_feature_rows": 0,
            "no_exact_player_game_day": 0,
            "ambiguous_exact_game_day": 200,
            "prop_specific_d7_unavailable": 300,
            "unmapped_prop_type": 38,
        }

    monkeypatch.setattr(rolling, "pg_fetchone", fake_fetchone)
    result = rolling._fetch_mtp_coverage("2026-09-17", "2026-09-26")

    assert result["rows_total"] == 23098
    assert result["required_rows"] == 22400
    assert result["d7_nonnull"] == 22390
    assert result["required_null"] == 10
    assert result["not_required_rows"] == 698
    assert result["d7_pct"] == 99.96
    assert result["required_rows"] + result["not_required_rows"] == result["rows_total"]
    assert "COUNT(DISTINCT game_id)" in seen["sql"]
    assert "FROM mlb.player_derived_stats" in seen["sql"]
    assert "FROM mlb.player_stats" in seen["sql"]
    assert "WHEN 'hits' THEN pds.d7_hits" in seen["sql"]
    assert "WHEN 'strikeouts_pitching' THEN pds.d7_strikeouts_pitching" in seen["sql"]
    assert "AS prop_specific_d7_unavailable" in seen["sql"]
    assert seen["params"] == (
        "2026-09-17", "2026-09-26", "2026-09-17", "2026-09-26",
        "2026-09-17", "2026-09-26",
    )


def test_coverage_fails_closed_if_population_does_not_reconcile(monkeypatch):
    monkeypatch.setattr(rolling, "pg_fetchone", lambda *_: {
        "rows_total": 10, "required_rows": 8, "d7_nonnull": 7,
        "required_null": 1, "not_required_rows": 2,
        "no_player_daily_feature_row": 1, "ambiguous_daily_feature_rows": 0,
        "no_exact_player_game_day": 0, "ambiguous_exact_game_day": 0,
        "prop_specific_d7_unavailable": 0, "unmapped_prop_type": 0,
    })
    try:
        rolling._fetch_mtp_coverage("2026-09-17", "2026-09-26")
    except RuntimeError as exc:
        assert str(exc) == "ROLLING_COVERAGE_EXCLUSION_COUNT_MISMATCH"
    else:
        raise AssertionError("nonreconciling rolling coverage counts were accepted")


def test_full_text_summary_formats_every_exclusion_reason_without_mismatch(monkeypatch, capsys):
    def fake_fetchone(sql, _params):
        if sql.lstrip().startswith("SELECT") and "AS d7_nonnull" in sql:
            return {"rows_total": 100, "d7_nonnull": 100, "d15_nonnull": 100, "d30_nonnull": 100}
        if "prev_d7" in sql:
            return {"comparable_rows": 100, "changed_d7": 1, "changed_d15": 1, "changed_d30": 1}
        if "AS not_required_rows" in sql:
            return {
                "rows_total": 103, "required_rows": 100, "d7_nonnull": 100,
                "required_null": 0, "not_required_rows": 3,
                "no_player_daily_feature_row": 1, "ambiguous_daily_feature_rows": 0,
                "no_exact_player_game_day": 0, "ambiguous_exact_game_day": 0,
                "prop_specific_d7_unavailable": 2, "unmapped_prop_type": 0,
            }
        if "AS changed_rows" in sql:
            return {"comparable_rows": 100, "changed_rows": 1}
        raise AssertionError(f"unexpected rolling-integrity query: {sql}")

    monkeypatch.setattr(rolling, "pg_fetchone", fake_fetchone)

    assert rolling.main(["--from-date", "2026-09-17", "--to-date", "2026-09-26"]) == 0
    output = capsys.readouterr().out
    assert "mtp_rows=103 mtp_required=100 mtp_cov_d7=100.00%" in output
    assert "mtp_not_required=3 mtp_excluded(no_pds/ambig_pds/no_game/multi_game/no_prop_d7/unmapped)=1/0/0/0/2/0" in output
    assert "PASS mlb rolling integrity" in output


def test_summary_fails_closed_when_an_exclusion_reason_is_missing():
    exclusions = {key: 0 for key in rolling._MTP_EXCLUSION_REASON_KEYS}
    del exclusions["prop_specific_d7_unavailable"]
    try:
        rolling._format_mtp_exclusion_counts(exclusions)
    except RuntimeError as exc:
        assert str(exc) == "ROLLING_COVERAGE_REASON_MISSING:prop_specific_d7_unavailable"
    else:
        raise AssertionError("missing exclusion reason was accepted")
