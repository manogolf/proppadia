import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.nhl.performance_summary import _summary_lineage_key, select_authoritative_summary


class PerformanceSummaryRevisionAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.expected = {
            "slate_date": "2026-10-04",
            "package_identity": "reconciliation-package",
            "canonical_phase": "REGULAR_SEASON",
            "official_outcomes_status": "FINAL",
            "source_artifacts": {"reconciliation_manifest_sha256": "a" * 64,
                                 "sog_grade_sha256": "b" * 64},
            "models": {"sog": {"grade_count": 10}},
        }

    def candidate(self, revision=None, marker="a", descriptive_valid=True):
        identity = marker * 64
        path = self.root / f"performance_summary={identity[:20]}"
        path.mkdir(exist_ok=True)
        summary = dict(self.expected)
        summary.update({"summary_identity": identity,
                        "summary_revision": revision,
                        "revision_lineage_key": _summary_lineage_key(self.expected)})
        return path, summary, descriptive_valid

    def select(self, candidates):
        with patch("backend.nhl.performance_summary._compatible_summary_versions",
                   return_value=[(path, summary) for path, summary, _ in candidates]), \
             patch("backend.nhl.performance_summary._descriptive_evidence_is_valid",
                   side_effect=[valid for _, _, valid in candidates]):
            return select_authoritative_summary(root=self.root, expected=self.expected)

    def test_one_legacy_summary_is_usable(self):
        path, summary, valid = self.candidate(marker="1")
        selected_path, selected = self.select([(path, summary, valid)])
        self.assertEqual(selected_path, path / "performance_summary.json")
        self.assertEqual(selected["summary_identity"], summary["summary_identity"])

    def test_multiple_legacy_summaries_are_ambiguous(self):
        candidates = [self.candidate(marker="1"), self.candidate(marker="2")]
        with self.assertRaisesRegex(RuntimeError, "LEGACY_SUMMARY_AUTHORITY_AMBIGUOUS"):
            self.select(candidates)

    def test_explicit_revision_overrides_legacy_ambiguity(self):
        candidates = [self.candidate(marker="1"), self.candidate(marker="2"),
                      self.candidate(revision=1, marker="3")]
        _, selected = self.select(candidates)
        self.assertEqual(selected["summary_revision"], 1)
        self.assertEqual(selected["summary_identity"], "3" * 64)

    def test_highest_valid_revision_wins_and_invalid_higher_falls_back(self):
        candidates = [self.candidate(revision=2, marker="2"),
                      self.candidate(revision=3, marker="3", descriptive_valid=False)]
        _, selected = self.select(candidates)
        self.assertEqual(selected["summary_revision"], 2)

        candidates.append(self.candidate(revision=3, marker="4"))
        _, selected = self.select(candidates)
        self.assertEqual(selected["summary_revision"], 3)
        self.assertEqual(selected["summary_identity"], "4" * 64)

    def test_conflicting_identities_at_same_highest_revision_fail(self):
        candidates = [self.candidate(revision=3, marker="3"),
                      self.candidate(revision=3, marker="4")]
        with self.assertRaisesRegex(RuntimeError, "PERFORMANCE_SUMMARY_REVISION_CONFLICT"):
            self.select(candidates)


if __name__ == "__main__":
    unittest.main()
