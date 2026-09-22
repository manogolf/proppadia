# Shared Regular-Season Eligibility Contract

Contract identity: `MLB_REGULAR_SEASON_TRAINING_ELIGIBILITY_V1`

## Scope

This contract governs membership in regular-season training, validation, evaluation, research, and regular-season reporting datasets. It does not mutate or relabel the observation relation. Raw-observation health reports may deliberately inspect all observations, but must not describe that population as regular-season training/evaluation eligible.

## Required interface

One shared function is proposed at the dataset-assembly boundary:

```text
filter_regular_season_membership(
    rows,
    *,
    game_pk_field,
    authority: CanonicalGamePhaseAuthority,
    consumer_identity,
    input_identity,
) -> (admitted_rows, gate_report)
```

It must call only the stable authority methods `lookup_exact(game_pk)` or `require_membership(game_pk, {"REGULAR_SEASON"})`. Consumer code may not map raw types, inspect dates, query phase columns opportunistically, or implement fallback behavior.

## Admission invariant

A row is admitted if and only if:

1. the supplied identity is an exact, positive-integer MLB gamePk;
2. the complete authority instance has passed its hash, count, source-file, contract, conflict, duplicate, and staleness validations;
3. exact lookup returns one unambiguous admitted record;
4. `season_phase == "REGULAR_SEASON"` and the frozen source contract derived it from exact authoritative source type `R`.

The following fail closed and abort certification: missing/malformed gamePk, absent authority, stale authority, special type, unknown type, missing type, source conflict, relationship conflict, duplicate identity, or consumer-retained type conflicting with authority. `PRESEASON` and `POSTSEASON` are valid classifications but are excluded from regular membership and counted separately.

No calendar value, season expectation, market participation, prediction participation, team/player proximity, or default may create phase membership. There is no missing-type-to-`R` path.

## Membership-only guarantee

The interface must preserve the original row object and stable row order for admitted rows. It may append phase evidence only to the gate report, not to the consumer's feature frame unless separately authorized. For the intersection of old and new populations, deterministic hashes over feature columns, target columns, predictions, probabilities, outcomes, and prices must be unchanged.

The gate report must contain:

- consumer and input identities;
- authority backend, proposal/sidecar snapshot identity, source-manifest hash, contract hash, and supported window;
- input/admitted/excluded/blocked row and distinct-gamePk counts;
- counts by authoritative raw type and normalized phase;
- sorted exact gamePk sets for exclusions and each blocked reason;
- input population hash and admitted population hash;
- an invariant hash over retained row content excluding gate annotations.

Any blocked count prevents dataset certification. Expected preseason/postseason exclusions do not block if authority is complete and unambiguous.

## Current hashed-file backend

Current governed use pins the inseparable pair:

- proposal SHA-256 `b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879`;
- retained-source manifest SHA-256 `766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100`.

It requires 2,919 proposal gamePks, 464 retained source files, 9,092 source observations, zero missing/unknown/conflicting/duplicate identities, and byte verification of every referenced source. Its supported window ends 2026-09-27. Requests beyond that coverage fail stale; future retained games require a separately reviewed, versioned authority refresh.

## Future database sidecar backend

The sidecar must implement the same `CanonicalGamePhaseAuthority` behavior and typed errors. Cutover is configuration-only for consumers. Before replacement:

1. pin one verified file authority and one immutable sidecar snapshot;
2. compare the full union of keys and every governed field;
3. require identical regular/preseason/postseason/special membership hashes;
4. require identical failures for missing, excluded, conflicted, and malformed probes;
5. run every cut-over consumer in shadow and require identical admitted gamePk and row hashes;
6. require unchanged feature/target/probability/prediction/outcome/price hashes for retained rows.

Any difference blocks backend cutover. Database recency never silently overrides the pinned source authority. Rollback repins the verified file backend; it does not change observation rows.
