# MLB 2026 Game Phase File Authority Hits Pilot V1

Status: **POST-CONTROL CORRECTION VALIDATED; PRIOR CONTROL VIOLATION PRESERVED**

## Audit classification

For only the frozen retained cohort described below, the provisional audit classification is:

`RESULT_SAME_BUT_CONTROL_VIOLATED`

Commit `0ae9d87f4e7cd7a758e87862dcca6d579f1bcd13` is post-control correction evidence. It does not establish that the implementation before that commit complied with the already-established fail-closed requirement. The unchanged result on the frozen cohort establishes outcome parity only: the pre-correction evaluator still contained an impermissible missing-type-to-`R` fallback.

This classification is not evidence about other Hits consumers, prediction construction, Full-board Hits, external modeling splits, Moneyline, Totals, grading, agreement reporting, earlier seasons, or any untested date range.

## Scope

This pilot implements the backend-neutral `CanonicalGamePhaseAuthority` interface with the exact hashed 2026 proposal backend and cuts over only `backend/mlb/scripts/evaluate_hits_model_candidates.py`.

It does not implement the database sidecar, change another production consumer, alter model features or scoring, write predictions, connect to a database, call a provider, or run a pipeline.

## Authority identity

- Proposal: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl`
- Proposal SHA-256: `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`
- Proposal population: 2,919 unique gamePks
- Source manifest: `docs/contracts/mlb_2026_canonical_phase_source_completion_v1/canonical_backfill_proposal/retained_source_manifest.jsonl`
- Source-manifest SHA-256: `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`
- Retained sources: 464 files and 9,092 observations; every retained file is byte/hash verified at load
- Frozen classifier: `backend/mlb/season_transition/contract_v1.py`
- Classifier identity: `MLB_2026_REGULAR_SEASON_CLOSE_AND_POSTSEASON_DATA_PLAN_V1`, version `contract_v1`
- Classifier SHA-256: `eed52e24d123c24e0fbbff21249705d653d77787eab98eb756d5c9752a481c19`
- Authority counts: 489 preseason and 2,430 regular season; zero missing, unknown, conflicting, or duplicate identities
- Supported file-authority window: 2026-02-20 through 2026-09-27

The file backend fails stale outside that window. Dates establish coverage only; they never classify phase.

## Consumer change

The old database predicate admitted a missing type as regular season:

`COALESCE(NULLIF(upper(trim(to_jsonb(m)->>'game_type')), ''), 'R') = 'R'`

The evaluator now fetches its otherwise unchanged rows and applies exact-gamePk authority before scoring. Regular-season admission requires positive `REGULAR_SEASON` membership. It reports:

- admitted regular-season rows;
- excluded preseason rows;
- excluded postseason rows;
- blocked missing gamePk, absent authority, special type, unknown type, conflicting type, or stale authority.

Any blocked row aborts evaluation after writing the gate report. Expected preseason/postseason rows are excluded. The scoring, metric, decile, threshold, and cohort functions retain their pre-pilot AST identities.

## Offline retained-input result

The frozen retained Hits ledger contains 7,564 rows over 651 exact gamePks from 2026-05-08 through 2026-08-02. All 651 gamePks have positive authoritative regular-season membership.

Before and after results are identical:

- rows: 7,564 / 7,564;
- distinct gamePks: 651 / 651;
- newly excluded or blocked gamePks: none;
- prediction/pick hash: `a0337d0450c05db5af8800db903d4c8458858d79197a9bdefa91d99a35434753` / identical;
- outcome hash: `204603647e10cfb937aeda6ed806641e3d833e9c4fb921a3a83019d3b3b67d22` / identical;
- cohort hash: `f5c8663a1abfd744b8da4b5e44d096b9d2bb5a8ca61f0bc0a5aaa7f18c455c40` / identical;
- probability invariance: pass;
- all recorded evaluation metrics: identical.

These equalities do not cure or excuse the historical control violation. Every gamePk in the tested population was authoritatively regular season, so this cohort could demonstrate invariance but could not demonstrate the pre-correction control was safe for preseason, postseason, special, missing, unknown, conflicting, absent, or stale identities.

## Preserved introducing history and control gap

- The exact fallback first entered the repository in commit `d5a5a4f466123c658aa18b1b7cf3b43283c1f5be` on 2026-03-29 in the legacy recompute path. Its inline rationale was an optional Spring Training exclusion that remained compatible with databases where `model_training_props.game_type` did not yet exist. That compatibility choice treated missing type as `R`.
- The fallback entered `evaluate_hits_model_candidates.py` when that file was created by commit `5b24d71143fe120c41f9d1b40bc60bbb5cfbf8de` on 2026-04-24. The introducing commit message, `Commit all pending changes`, records no narrower scientific justification.
- The fail-closed phase contract was added by `28ec66a2334cfa423a4939a1ae0b0cbe4a84ab24` on 2026-09-21, and canonical activation certification followed in `01c610c4ed88c2a7fa814cce3dae6d3e25a5aa46`.
- Certification did not reject the evaluator because its static zero-default/date scan covered only `contract_v1.py`, `canonical_phase_v1.py`, and the offline backfill builder. Producer checks covered selected game writers. Neither control enumerated or executed `evaluate_hits_model_candidates.py`, which had no repository call site or dedicated test.
- The activation blocker inventory named Full-board Hits and external normalized-game fallbacks but omitted this evaluator. Later sidecar-design tracing in `3aec4975adb2aa0753e51bd1714245243956376b` found and classified the evaluator defect.
- Commit `0ae9d87f4e7cd7a758e87862dcca6d579f1bcd13` removed the fallback from this evaluator and added the exact-gamePk authority gate. It is a correction after the control existed, not proof of prior compliance.

## Validation

- Pilot dependency-free `unittest` runner: 13 passed, 0 failed, 0 skipped.
- Existing canonical dependency-free runner: 25 passed, 0 failed, 0 skipped, 0 unexecuted; existing 13-check validator passed.
- Source-completion validator: 14 checks passed, 0 failed.
- Pilot offline validator: 9 checks passed, 0 failed.
- Network requests: 0.
- Database connections and writes: 0.

## Cutover conclusion

This pilot proves only that the corrected evaluator preserves results for the frozen 2026-05-08 through 2026-08-02 cohort. It does not, by itself, justify or validate another consumer cutover. Each additional consumer and date range requires its own historical-control audit, exact-gamePk coverage proof, and bounded before/after validation. Prospective postseason use remains unsupported because this proposal contains no postseason games.
