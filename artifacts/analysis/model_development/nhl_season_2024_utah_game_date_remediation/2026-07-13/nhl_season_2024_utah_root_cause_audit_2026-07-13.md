# Utah missing-date root-cause audit

## Finding

The defect is classified `HISTORICAL_IMPORT_GAP|FRANCHISE_TRANSITION_DEFECT`.

All 1,398 season `2024` game rows were inserted in the same historical batch. The 1,316 non-Utah rows were updated the following day and have dates; all 82 Utah rows retained their original null dates and were never enriched. Team IDs and abbreviations are otherwise correct: Utah is team ID 68 and Arizona remains separately represented as team ID 53. The current daily schedule importer reads and upserts an explicit date, so its normal daily path does not explain the selective historical nulls.

The surviving repository does not identify the exact historical enrichment command. The concentrated franchise boundary and skipped update pattern support a franchise-transition-specific historical import gap, not a present-day schedule parser or canonical game-ID defect. No source code or database row is changed by this remediation.
