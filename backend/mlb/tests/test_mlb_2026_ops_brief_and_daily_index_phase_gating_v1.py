from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from backend.mlb.reporting.phase_reporting_v1 import (
    AGREEMENT_RECONCILIATION,
    AGREEMENT_STALENESS,
    PhaseReportingError,
    build_current_phase_reporting_control,
    build_unavailable_phase_reporting_control,
    partition_exact_game_pk_rows,
    render_phase_reporting_markdown,
    sha256_file,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    GamePhaseAuthorityError,
)


class FakeAuthority:
    def __init__(self, phases: dict[int, str | None], *, errors: dict[int, str] | None = None):
        self.phases = phases
        self.errors = errors or {}

    def lookup_exact(self, game_pk: int):
        if game_pk in self.errors:
            raise GamePhaseAuthorityError(self.errors[game_pk], game_pk=game_pk)
        if game_pk not in self.phases:
            raise GamePhaseAuthorityError("GAME_PHASE_AUTHORITY_MISSING", game_pk=game_pk)
        return SimpleNamespace(game_pk=game_pk, season_phase=self.phases[game_pk])


class OpsBriefAndDailyIndexPhaseGatingV1Tests(unittest.TestCase):
    def test_regular_postseason_and_late_regular_are_separate(self) -> None:
        authority = FakeAuthority({1: "REGULAR_SEASON", 2: "POSTSEASON", 3: "REGULAR_SEASON"})
        rows = [
            {"gamePk": 1, "game_date": "2026-09-30"},
            {"gamePk": 2, "game_date": "2026-09-30"},
            {"gamePk": 3, "game_date": "2026-10-15"},
        ]
        result = partition_exact_game_pk_rows(
            rows, authority=authority, scheduled_date_field="game_date"
        )
        self.assertEqual([1, 3], [row["gamePk"] for row in result.regular_season])
        self.assertEqual([1, 3], [row["gamePk"] for row in result.late_season_regular_season])
        self.assertEqual([2], [row["gamePk"] for row in result.postseason])

    def test_all_supported_postseason_rounds_are_one_shadow_partition(self) -> None:
        rounds = ["Wild Card", "Division Series", "League Championship", "World Series"]
        authority = FakeAuthority({index: "POSTSEASON" for index in range(1, 5)})
        rows = [
            {"gamePk": index, "postseason_round": round_name}
            for index, round_name in enumerate(rounds, start=1)
        ]
        result = partition_exact_game_pk_rows(rows, authority=authority)
        self.assertEqual(rounds, [row["postseason_round"] for row in result.postseason])
        self.assertFalse(result.regular_season)

    def test_preseason_is_excluded_and_special_fails_closed(self) -> None:
        authority = FakeAuthority({10: "PRESEASON", 11: None})
        result = partition_exact_game_pk_rows(
            [{"gamePk": 10}], authority=authority
        )
        self.assertEqual(1, len(result.excluded_preseason))
        self.assertFalse(result.regular_season)
        self.assertFalse(result.postseason)
        with self.assertRaisesRegex(
            PhaseReportingError, "REPORT_AUTHORITY_SPECIAL_OR_UNCLASSIFIED"
        ):
            partition_exact_game_pk_rows([{"gamePk": 11}], authority=authority)

    def test_missing_stale_conflicting_and_duplicate_authority_fail_closed(self) -> None:
        for code in (
            "GAME_PHASE_AUTHORITY_MISSING",
            "GAME_PHASE_AUTHORITY_STALE",
            "GAME_PHASE_AUTHORITY_CONFLICT",
            "GAME_PHASE_AUTHORITY_DUPLICATE_IDENTITY",
        ):
            with self.subTest(code=code), self.assertRaises(PhaseReportingError):
                partition_exact_game_pk_rows(
                    [{"gamePk": 50}],
                    authority=FakeAuthority({}, errors={50: code}),
                )

    def test_missing_and_invalid_game_pk_fail_closed(self) -> None:
        authority = FakeAuthority({1: "REGULAR_SEASON"})
        for row in ({}, {"gamePk": "1.0"}, {"gamePk": 0}, {"gamePk": True}):
            with self.subTest(row=row), self.assertRaises(PhaseReportingError):
                partition_exact_game_pk_rows([row], authority=authority)

    def test_duplicate_reporting_identity_fails_closed(self) -> None:
        with self.assertRaisesRegex(PhaseReportingError, "REPORT_DUPLICATE_IDENTITY"):
            partition_exact_game_pk_rows(
                [{"gamePk": 1}, {"gamePk": 1}],
                authority=FakeAuthority({1: "REGULAR_SEASON"}),
                identity_fields=("gamePk",),
            )

    def test_postponed_rescheduled_and_suspended_resumed_remain_regular(self) -> None:
        rows = [
            {"gamePk": 70, "relationship": "rescheduled", "game_date": "2026-10-02"},
            {"gamePk": 71, "relationship": "resumed", "game_date": "2026-10-03"},
        ]
        result = partition_exact_game_pk_rows(
            rows,
            authority=FakeAuthority({70: "REGULAR_SEASON", 71: "REGULAR_SEASON"}),
            scheduled_date_field="game_date",
        )
        self.assertEqual(2, len(result.regular_season))
        self.assertEqual(2, len(result.late_season_regular_season))

    def test_zero_calendar_phase_reconstruction(self) -> None:
        rows = [
            {"gamePk": 80, "game_date": "2026-03-01"},
            {"gamePk": 81, "game_date": "2026-11-01"},
        ]
        result = partition_exact_game_pk_rows(
            rows,
            authority=FakeAuthority({80: "POSTSEASON", 81: "REGULAR_SEASON"}),
            scheduled_date_field="game_date",
        )
        self.assertEqual([80], [row["gamePk"] for row in result.postseason])
        self.assertEqual([81], [row["gamePk"] for row in result.regular_season])

    def test_current_control_binds_governing_sources_and_close(self) -> None:
        before = {
            str(path): sha256_file(path)
            for path in (AGREEMENT_RECONCILIATION, AGREEMENT_STALENESS)
        }
        control = build_current_phase_reporting_control(report_date="2026-09-22")
        after = {
            str(path): sha256_file(path)
            for path in (AGREEMENT_RECONCILIATION, AGREEMENT_STALENESS)
        }
        self.assertEqual(before, after)
        self.assertEqual(2934, control["canonical_population"]["total"])
        self.assertEqual(2430, control["canonical_population"]["REGULAR_SEASON"])
        self.assertEqual(489, control["canonical_population"]["PRESEASON"])
        self.assertEqual(15, control["canonical_population"]["POSTSEASON"])
        self.assertEqual(2430, control["close"]["canonical_regular_season_game_pks"])
        self.assertEqual(88, control["close"]["outstanding_game_pks"])
        self.assertEqual("REGULAR_SEASON_CLOSE_BLOCKED", control["close"]["decision"])
        self.assertFalse(control["agreement"]["stale_summary"]["may_override_ledger"])

    def test_empty_postseason_is_explicit_not_omission_or_synthetic(self) -> None:
        text = "\n".join(
            render_phase_reporting_markdown(
                build_current_phase_reporting_control(report_date="2026-09-22")
            )
        )
        self.assertIn("no authoritative evidence, not omission or synthetic proof", text)
        self.assertNotIn("combined regular/postseason", text.lower())

    def test_unavailable_control_suppresses_all_phase_totals(self) -> None:
        control = build_unavailable_phase_reporting_control(
            report_date="2026-10-01", reason="synthetic stale authority"
        )
        text = "\n".join(render_phase_reporting_markdown(control))
        self.assertIn("UNAVAILABLE_OR_BLOCKED", text)
        self.assertIn("all regular-season, late-season, postseason", text.lower())
        self.assertNotIn("2430", text)

    def test_stale_report_date_fails_before_governing_counts_are_rendered(self) -> None:
        with self.assertRaisesRegex(PhaseReportingError, "PHASE_AUTHORITY_STALE"):
            build_current_phase_reporting_control(report_date="2026-10-06")

    def test_ops_brief_and_daily_index_use_shared_renderer(self) -> None:
        root = Path(__file__).resolve().parents[3]
        ops_source = (root / "backend/mlb/scripts/report_mlb_daily_ops_brief.py").read_text()
        index_source = (root / "backend/mlb/scripts/build_mlb_artifact_index.py").read_text()
        self.assertIn("render_phase_reporting_markdown(phase_reporting)", ops_source)
        self.assertIn("render_phase_reporting_markdown(phase_reporting)", index_source)
        self.assertIn("LEGACY_UNPARTITIONED_NOT_CERTIFICATION_EVIDENCE", ops_source)
        self.assertIn("UNAVAILABLE_PHASE_UNBOUND", ops_source)
        self.assertNotIn("Model: win rate", ops_source)
        self.assertNotIn("row.get('roi')", ops_source)

    def test_phase_blocked_payload_preserves_only_provenance(self) -> None:
        from backend.mlb.scripts.report_mlb_daily_ops_brief import (
            _phase_blocked_reporting_payload,
        )

        payload = _phase_blocked_reporting_payload(
            source_name="synthetic_metric_summary",
            source_paths=["retained/source.json"],
        )
        self.assertEqual("UNAVAILABLE_PHASE_UNBOUND", payload["reporting_status"])
        self.assertFalse(payload["metrics_exposed"])
        self.assertNotIn("wins", payload)
        self.assertNotIn("roi", payload)

    def test_daily_index_renders_phase_control_without_daily_evidence_side_effects(self) -> None:
        from backend.mlb.scripts.build_mlb_artifact_index import _write_daily_index

        control = build_unavailable_phase_reporting_control(
            report_date="2026-10-01", reason="SYNTHETIC_TEST_AUTHORITY_STALE"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            out_root = Path(temp_dir) / "mlb"
            _write_daily_index(
                "2026-10-01",
                "2026-09-30",
                out_root,
                phase_reporting=control,
            )
            rendered = (
                out_root / "daily/2026-10-01/INDEX.md"
            ).read_text(encoding="utf-8")
        self.assertIn("Canonical Game Phase Control", rendered)
        self.assertIn("SYNTHETIC_TEST_AUTHORITY_STALE", rendered)
        self.assertIn("all regular-season", rendered.lower())
        self.assertTrue(
            rendered.rstrip().endswith("Offseason readiness: `UNAVAILABLE_OR_BLOCKED`")
        )

    def test_ops_brief_renders_phase_control_and_suppresses_unbound_metrics(self) -> None:
        from backend.mlb.scripts.report_mlb_daily_ops_brief import build_markdown

        empty: dict[str, object] = {}
        rendered = build_markdown(
            ops_brief_md_path=Path("/tmp/synthetic_mlb_ops_brief.md"),
            report_date="2026-10-01",
            completed_slate_date="2026-09-30",
            current_slate_date="2026-10-01",
            generated_at_utc="2026-10-01T00:00:00Z",
            overall_status="warn",
            overall_issues=(),
            pipeline=empty,
            ops=empty,
            postgrade=empty,
            model_vs_fade={"model_roi_1u": 0.99, "model_win_rate": 0.99},
            bvp_impact=empty,
            hits_env=empty,
            hits_o15_watch_candidates=empty,
            hits_o15_layered_candidates=empty,
            hits_u15_favorite_audit=empty,
            hits_o15_alternate_discovery=empty,
            hits_15_tier_backtest={
                "o15_top_recent_combined_tiers": [{"roi": 0.99, "wr": 0.99}]
            },
            review_aid_performance={
                "callouts": {"o15_layer_4_latest": {"roi": 0.99, "wins": 99}}
            },
            total_bases_shadow_summary=empty,
            total_bases_shadow_evaluation=empty,
            feature_lineage_health=empty,
            prop_regime=empty,
            model_performance=empty,
            reporting_alignment=empty,
            today_workspace=empty,
            rolling_candidate_obs=empty,
            betonline_capture_integrity=empty,
            hits05_full_spine=empty,
            o15_prospective_status=empty,
            path_forward=(),
            source_states={},
            freshness_audit=(),
            phase_reporting=build_unavailable_phase_reporting_control(
                report_date="2026-10-01", reason="SYNTHETIC_AUTHORITY_STALE"
            ),
        )
        self.assertIn("Canonical Game Phase Control", rendered)
        self.assertIn("SYNTHETIC_AUTHORITY_STALE", rendered)
        self.assertIn("UNAVAILABLE_PHASE_UNBOUND", rendered)
        self.assertNotIn("99.00%", rendered)
        self.assertTrue(
            rendered.rstrip().endswith("Offseason readiness: `UNAVAILABLE_OR_BLOCKED`")
        )

    def test_required_ten_classifications_are_present(self) -> None:
        control = build_current_phase_reporting_control(report_date="2026-09-22")
        self.assertEqual(
            {
                "regular_season_close_readiness",
                "late_season_regular_season_completeness",
                "postseason_collection_status",
                "postseason_evaluation_status",
                "game_type_integrity",
                "outstanding_games",
                "requests_and_credits",
                "agreement_study_status",
                "model_selector_publication_status",
                "offseason_readiness",
            },
            set(control["classifications"]),
        )

    def test_current_control_is_deterministic(self) -> None:
        first = build_current_phase_reporting_control(report_date="2026-09-22")
        second = build_current_phase_reporting_control(report_date="2026-09-22")
        self.assertEqual(
            json.dumps(first, sort_keys=True, separators=(",", ":")),
            json.dumps(second, sort_keys=True, separators=(",", ":")),
        )


if __name__ == "__main__":
    unittest.main()
