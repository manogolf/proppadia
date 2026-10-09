# NHL SOG historical feature evidence audit

Window: **2026-10-03 through 2026-10-09**.

## Finding

Top-level classification: **OCT3_9_SOG_RECORDS_FIT_WITH_BOUNDED_CAVEATS**. The preserved Oct. 3–8 selected evaluation package reports 1,582 settled 1.5 decisions, 65.42% accuracy, 56.61% always-Under accuracy, 60.25% Over-call accuracy, 68.31% Under-call accuracy, AUC 0.6913, and AP 0.6237. The audit keeps byte certification separate from analytical fitness. One candidate survives per date, so competing-copy overwrite cannot be tested; its mtime can be later than the selected production scorer on some dates. That does not change the replay result.

Selected prediction artifacts were replayed with the production scorer and surviving date-specific feature CSVs. Exact prediction-hash replays: 2026-10-03, 2026-10-04, 2026-10-05, 2026-10-06, 2026-10-07, 2026-10-08, 2026-10-09. Oct. 9 is included for pregame state only; this audit did not acquire outcomes.

## Authority and candidate evidence

`production_run_authority.csv` records one selected production run per date, its canonical slate, prediction hash verification, scorer SHA, timing, and evaluation package. `feature_file_inventory.csv` inventories discovered retained date-specific feature CSVs with SHA256, size, timestamps, schema, row counts, and identity checks. `feature_replay_results.csv` has per-candidate reproduction results.

## Practical limits

No database reconstruction was performed; no database access or mutation occurred. For Oct. 3–8, the 9 rate/exposure fields compared against retained `prod_input_*` analysis values match every joined identity cell exactly; this is an internal artifact consistency check, not a current-DB reconstruction. `current_reconstruction_comparison.csv` compares the surviving CSV to retained analysis-row feature values as a secondary consistency check and explicitly does not claim a current DB reconstruction. Historical runtime bytes remain `NOT_BYTE_CERTIFIED` where the receipts did not bind them, even when prediction replay matches exactly. Source-table as-of versions remain unavailable.

## 1.5 impact

`sog_1_5_materiality.csv` and `analysis_conclusion_robustness.csv` compare replayed candidate probabilities against the retained settled evaluation population. All seven dates have identical prediction hashes; across 1,582 settled Oct. 3–8 records, max and 99th-percentile probability movement are 0, side mismatches are 0, accuracy/AUC/AP changes are 0, and Over/Under call counts change by 0. The full SOG conclusions (78.6% overall accuracy) and shadow conclusions remain as in the existing packages: selected baseline outputs replay exactly, and the existing shadow comparisons are separately tied to their retained captures. The deep 1.5 conclusions (65.4% accuracy, +7.3 point lift vs always-Under, 60.2% Over accuracy, 68.3% Under accuracy, AUC .691, AP .624) are unchanged. Top-decile Over rate (.730), bottom-decile rate (.196), and losing-Over 0-vs-1 split (86 vs 139) are unchanged. Rate/exposure fields are fit for analysis with a minor provenance caveat because selected feature CSVs replay production exactly; however, the existing study still does not establish causal rate-versus-exposure dominance, and postgame counterfactuals remain descriptive. C/G shadow conclusions remain non-promotional because their paired intervals cross zero.

## Validation / scope

Read-only local artifact audit; no provider calls, paid credits, database mutation, production changes, or output overwrite. Replay outputs were written under temporary directories.
