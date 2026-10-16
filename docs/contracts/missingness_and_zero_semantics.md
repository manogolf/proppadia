# Missingness and Zero Semantics Contract

Status: **AUTHORITATIVE PROJECT-WIDE DATA CONTRACT**
Applies to: **MLB, NHL, and future sports and pipelines**
Scope: source ingestion, staging, SQL exports, feature construction, rolling aggregates, training, scoring, reporting, and reconciliation.

This contract supplements [Project Doctrine](../PROJECT_DOCTRINE.md). If a local feature contract has weaker or contradictory NULL/zero semantics, this document governs source truth and the local contract must be corrected. A documented model-layer imputation may define a narrower representation for a named feature, but it never changes the source value.

## Core semantics

- **`0` means a verified observed zero.** The source is complete for the row and field, and the observed measurement or event count is zero.
- **`NULL` means unknown, missing, unobserved, or unavailable.** It does not mean zero, no event, no participation, or no opportunity unless a field-specific source contract proves that interpretation.
- Never turn unknown source data into zero to satisfy an importer, SQL export, feature schema, or model.
- Never turn a verified zero into NULL because a downstream calculation does not use it, because zero is inconvenient for a denominator, or to preserve a prior positive value.
- Establish source completeness before interpreting an absent event as zero. Completeness must cover the relevant game, participant population, source endpoint or file, and field.
- Preserve raw source values. A derived representation must not overwrite source truth.
- Retain source provenance and a reason code for missing values when the source or acquisition path provides one. Do not invent a reason when evidence does not distinguish absence from zero.

## Feature and aggregation rules

Model-layer imputation is allowed only when an explicit feature contract states the imputation, its reason, its applicability, and its training and scoring behavior. The imputation must be consistent between training and scoring, expose a missingness indicator or other diagnostic when that distinction matters, and leave the source column unchanged. A generic `fillna(0)`, `COALESCE(value, 0)`, or importer default is not a field contract.

Rolling aggregations must:

1. Include verified zero observations in the numerator and the valid-observation denominator.
2. Exclude unknown observations from both numerator and denominator unless a specific, justified model feature uses a documented imputation.
3. Retain the valid observation count and missing observation count (and expected population count where available).
4. Distinguish “no qualifying history” from “history exists but values are missing.” A neutral model input for no history is a feature-layer choice, not a source zero.

Mass NULL-to-zero or zero-to-NULL updates require field-specific source evidence, before/after counts, affected identities, a reversible plan, and validation that source truth and downstream semantics are preserved. Generic cleanup is prohibited.

## Examples

| Field | Verified zero | NULL / unknown | Required treatment |
|---|---|---|---|
| NHL skater shot attempts | Complete official player-game record reports zero attempts. | Attempts are absent, not collected, or source coverage is incomplete. | Do not use missing attempts as zero in per-60 rates, season totals, or team aggregates. Retain denominator and source coverage. |
| NHL power-play time on ice (PP TOI) | Complete source or a validated shift/penalty reconstruction establishes zero PP seconds. | Provider field is absent, shift/manpower source is incomplete, or source reason is unknown. | Keep zero and NULL distinct. Positive-only backfills cannot establish that every untouched NULL means zero. |
| MLB at-bats, hits, strikeouts, walks, total bases | Complete official player-game source establishes the player participated and recorded zero for that statistic. | Missing player row, unconfirmed participation, incomplete boxscore, or unavailable statistic. | Do not infer nonparticipation or a zero count from NULL without a complete participant/stat source. |
| Player participation | Authoritative game evidence establishes participated, including a valid zero-count performance. | Did not play, status unresolved, or participation source missing. | Participation is a separate categorical state. Do not encode it only as a zero statistic. |
| TOI, innings, pitches, plate appearances, saves faced | Complete source reports a measured zero where that state is possible and meaningful. | Not measured, not applicable, not played, or source unavailable. | Use a field-specific applicable/participation state; do not substitute zero exposure for missing exposure. |

## Historical evidence and issue register

Classification vocabulary:

- **CONFIRMED VIOLATION** — retained source, code, or audit directly demonstrates the contract breach. A confirmed code path does not by itself prove every historical row was affected.
- **INTENTIONAL DOCUMENTED MODEL IMPUTATION** — an explicit model feature contract documents the layer and behavior; source truth remains intact.
- **VALID OBSERVED-ZERO HANDLING** — complete source coverage supports zero and the zero remains a real observation.
- **REQUIRES INVESTIGATION** — retained evidence does not establish source completeness, row-level semantics, or downstream impact.

### Confirmed violations and retained findings

1. **NHL Points shot attempts — missing source values zero-filled. CONFIRMED VIOLATION; exact causal source unresolved.** The retained two-sided audit at [`artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/summary.json`](../../artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/summary.json) classifies `prior_attempts_status` as `CONFIRMED_OPERATIONAL_ZEROING_DEFECT; causal responsibility not established`. The same-slate evidence manifest records `raw_shot_attempts_null_rows=2664` and `raw_source_coverage_assessment=ROW_COUNTS_RETAINED_EXPECTED_ROSTER_DENOMINATOR_NOT_CAPTURED` at [`artifacts/operational/nhl/points_feature_evidence/season=2026/slate_date=2026-10-15/run_id=nhldaily_20261016T005559712749Z_422ddbda/manifest.json`](../../artifacts/operational/nhl/points_feature_evidence/season=2026/slate_date=2026-10-15/run_id=nhldaily_20261016T005559712749Z_422ddbda/manifest.json). The successful run's HGB source manifest at [`artifacts/operational/nhl/points_hgb_shadow/season=2026/slate_date=2026-10-15/run_id=nhldaily_20261016T005559712749Z_422ddbda/phase=EARLY/captured_at=20261016T005835054005Z/source_input_manifest.json`](../../artifacts/operational/nhl/points_hgb_shadow/season=2026/slate_date=2026-10-15/run_id=nhldaily_20261016T005559712749Z_422ddbda/phase=EARLY/captured_at=20261016T005835054005Z/source_input_manifest.json) reports 3,852 missing attempt observations among 3,852 source skater rows. The 2,664-row legacy evidence subset and 3,852-row HGB input are different populations. None of these missing values are verified zeros. `backend/nhl/sql/export_points.sql`, `backend/nhl/points_team_shots_evidence.py`, and `backend/nhl/scripts/export_nhl_points_hgb_features.py` each contain missing-attempt-to-zero transformations; the evidence aggregator’s completeness flag checks missing SOG but not missing attempts. Investigate source coverage and each downstream use before correction or impact claims.

2. **NHL Points team SOG history — missing player SOG zero-filled. CONFIRMED IMPLEMENTATION VIOLATION; affected operational cause unresolved.** The two-sided audit's [`feature_contract.json`](../../artifacts/analysis/nhl/points_team_shots_two_sided_lineage_audit/2026-10-15/feature_contract.json) says the intended contract treats missing SOG as unknown, while both the historical and operational builders zero-fill it. The audit reports historical team identity and operational feature defects, and says the exact upstream stage is not retained. It does not establish that every SOG zero-fill caused the measured feature discrepancies.

3. **NHL PP TOI — zero-to-NULL conversion. CONFIRMED CODE-LEVEL VIOLATION; historical row effects and prior corrective reversals not proven.** `backend/nhl/scripts/seed_skater_logs_for_date.py` applies `NULLIF(EXCLUDED.pp_toi_minutes, 0)` on stage upsert; `backend/nhl/scripts/run_nhl_postgame_reconciliation.py` applies `NULLIF(s.pp_toi_minutes, 0)` when writing raw logs; the historical introduction is visible in commit `848aaa71` (`foray into shifts`). These paths discard an explicit zero. The retained Oct. 15 Points HGB source contains 1,004 NULL PP-TOI values across 107 prior games, but has no per-row reason or completeness denominator that separates verified zero PP time from unavailable source data. Do not mass-fill those NULLs. The repository scan did not find a retained row-level correction/reversal package proving which existing NULLs were previously observed zeros.

4. **NHL missing player SOG/attempts in rolling count SQL. CONFIRMED CODE PATH; downstream validity requires investigation.** `backend/nhl/sql/fill_sog_counts_for_slate.sql` and `backend/nhl/sql/export_sog_denali_pregame.sql` coalesce missing counts to zero before aggregation. Their current inputs do not by themselves prove complete player-game coverage. Preserve as a named investigation item; do not interpret these expressions as evidence that zero was observed.

### Bounded MLB/NHL path scan: other classifications

- `backend/mlb/modeling/train_models.py::_materialize_training_matrix` imputes numeric model features to zero and creates `isna__*` indicators. This is a training-layer transformation, but a general matrix helper is not a named feature contract and the bounded scan did not establish scoring parity. **REQUIRES INVESTIGATION** before treating this as an approved intentional imputation.
- `backend/mlb/modeling/transform_features.py::_coerce_numeric_fill` replaces infinities and all NULLs with zero, without a field-specific contract at this function boundary. **REQUIRES INVESTIGATION** for consumers whose missingness can be meaningful.
- MLB `backend/mlb/model_trainer.py::_fetch_player_stats_tb_history` uses `COALESCE(ps.at_bats, 0)` when the column exists, while using NULL if the column does not exist. **REQUIRES INVESTIGATION**: this code alone does not establish player participation or source completeness.
- MLB `backend/mlb/scripts/insert_mlb_stat_derived.py` sums `hits_runs_rbis` components after coalescing each missing value to zero. **REQUIRES INVESTIGATION** unless the source row and each component are proven complete.
- NHL `backend/nhl/sql/seed_goalie_features_for_slate.sql` coalesces missing shots faced and saves to zero and substitutes 60 minutes when goalie TOI is NULL or zero. **REQUIRES INVESTIGATION**: source completeness, participation semantics, and whether 60 is a documented model fallback must be checked per feature.
- NHL `backend/nhl/scripts/score_nhl_props.py::prepare_X` fills binary, numeric, and absent model columns at scoring time. This is a model-input transformation, not authorization to mutate source values; feature-by-feature training/scoring parity and contract documentation require review.
- **VALID OBSERVED-ZERO HANDLING:** a complete official player-game record with zero hits, shots, attempts, saves, goals, or points is a valid zero observation. A player-game row missing from an incomplete source is not.
- `backend/nhl/sql/export_points.sql` uses `NULLIF(toi_minutes, 0)` to avoid dividing by zero. This is a derived-rate denominator guard, not permission to replace the underlying observed TOI zero with NULL in source storage. The derived rate should be unavailable for zero exposure, with its reason/denominator retained where consumed.

This was a bounded source scan of the cited MLB/NHL ingestion, export, aggregation, and feature/model-input paths, not an exhaustive repository audit. It made no production or database changes.

### Recollections and claims not verified by retained artifacts

The code history confirms the PP-TOI zero-to-NULL transformation and the evidence packages confirm current NULL counts. The scanned repository artifacts do **not** prove the row-level claim that all 1,004 current NULLs were legitimate observed zeros, that all were missing source values, or which specific database rows were changed by earlier imports/backfills. Do not repeat any of those stronger claims as established fact without row-bound source evidence. No reversal of the zero-to-NULL behavior was verified in the scanned history.

## NHL Points issue register: open

- The 3,852 missing shot-attempt observations in the successful Oct. 15 HGB source capture require source-coverage investigation. They are not verified zeros. The separate team-shot evidence capture reports 2,664 NULL attempts in its raw-prior subset; do not conflate the populations.
- The 1,004 NULL PP-TOI observations have unresolved semantics. Do not mass-fill or normalize them.
- Both `NHL_POINTS_COUNT_HGB_V1` and `phoenix_v2` are in scope because both depend on shared NHL skater-game source data.
- The live Phoenix scoring feature path is `backend/nhl/sql/export_points.sql` → `exports/train_nhl_points_v2.csv` → `backend/nhl/scripts/score_nhl_points_with_lineage.py`; it constructs a legacy feature set.
- The live HGB path is the retained official outcomes/skater-log/schedule input → `backend/nhl/scripts/export_nhl_points_hgb_features.py` → the frozen HGB scorer. Its 10-column feature contract differs from Phoenix's legacy export. Both live paths currently turn missing attempts into zero for an attempts-rate feature, but their distinct historical training builders, inputs, and full downstream effects have not been established as identical. Trace and compare those paths before asserting equal exposure or impact.
- Status: **OPEN — broader Points investigation is not closed.** No production data repair, model retraining, calibration, authority change, or feature change is authorized by this register.

## Review and change control

Every new source field, importer, aggregate, feature, or reconciliation path must state its NULL and zero semantics and how source completeness is proven. Tests should include an observed zero, a missing source value, an incomplete population, no prior history, and a missing participant where applicable. A future correction must identify the affected field and rows, preserve before-state evidence, validate expected denominators and downstream training/scoring parity, and update this register with evidence. Do not close an issue solely because a model accepts an imputed matrix.
