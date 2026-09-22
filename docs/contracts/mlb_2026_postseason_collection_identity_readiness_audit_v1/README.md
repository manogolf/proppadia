# MLB 2026 postseason collection identity readiness audit V1

Audit status: `REVIEW_ONLY_COMPLETE_WITH_BOUNDED_IDENTITY_CORRECTIONS`

This package is a local-evidence-only assessment. It made no network or provider request, opened no operational database connection, ran no pipeline or schedule, changed no acquisition claim, and did not activate postseason collection or evaluation. The extraction began from required ancestry. Concurrent unrelated NHL commits advanced HEAD during the audit: evidence extraction recorded `0b99d2b34162a3f597a18021a5882fad8e713fc5`, and final validation recorded `005099d0b8a33847e7e3a1bd025f33d1507d31bf`. This audit did not touch that work; all three required MLB commits remain ancestors.

## Conclusions

Official MLB paths are identity-ready: retained schedule, game feed, outcome, and inline BvP evidence carry exact `gamePk`. Roster refresh is game-phase-neutral. The current prospective feature-lineage ledger also carries exact `game_id`, player identity, prediction/market times, feature hashes, and odds-snapshot hashes.

The market boundary is not uniformly provenance-complete. The retained player-prop estate contains 2,340 distinct provider event IDs, including 2,211 events with BetOnline, but `build_mlb_predictions_wide.py` does not persist provider event ID in the later exact-game lineage and its doubleheader helper selects the first game when provider commence time is absent or invalid. That is a specific, observed code-path defect; the audit does not allege a known wrong retained mapping. The exact event-level ledger records the missing durable binding for all 2,340 IDs.

Pinnacle is materially stronger: 223 retained identity-audit files contain 3,963 observations for 636 provider events; every event has one retained gamePk and no event maps to conflicting gamePks. Forty-one events had at least one `GAME_NOT_FOUND` observation but were mapped in another retained observation. The binder rejects zero or multiple candidates. Its remaining provenance gap is that the accepted mapping retains the provider response hash but not the official schedule response path/hash used to establish the candidate set.

The verified phase authority currently contains 2,919 gamePks: 2,430 regular season, 489 preseason, and zero postseason, with support through 2026-09-27. Exact-gamePk join capability therefore exists, but operational postseason readiness is not proven or authorized. Synthetic identity tests exercise doubleheader, ambiguous-candidate, and rescheduled-start behavior only; they are not operational evidence.

## Required classifications

| Classification | Result |
|---|---|
| Raw postseason collection readiness by lane | StatsAPI schedule/feed/outcome `READY_EXACT_GAME_PK`; roster `NOT_APPLICABLE`; BvP `READY_EXACT_GAME_PK`; Full-board Hits and BetOnline raw snapshots `READY_REPRODUCIBLE_PROVIDER_MAPPING`; Pinnacle `READY_REPRODUCIBLE_PROVIDER_MAPPING`; prospective lineage `READY_EXACT_GAME_PK` |
| Exact-gamePk identity integrity by lane | StatsAPI and BvP `READY_EXACT_GAME_PK`; roster `NOT_APPLICABLE`; Full-board/BetOnline `CONDITIONAL_IDENTITY_NOT_PROVEN`; Pinnacle `CONDITIONAL_IDENTITY_NOT_PROVEN` only because its official schedule source hash is unbound; feature lineage rows `READY_EXACT_GAME_PK` after the upstream choice |
| Canonical phase-join readiness by lane | Every game-bearing normalized lane is `READY_EXACT_GAME_PK`; roster `NOT_APPLICABLE`. Current postseason population is absent, so this is interface readiness, not operational proof. |
| BvP acquisition health | `READY_EXACT_GAME_PK`; exactly once per successful date is intact and `BVP_INLINE_SUCCESS_ALREADY_EXISTS` is preserved |
| Market identity integrity | `CONDITIONAL_IDENTITY_NOT_PROVEN`; Pinnacle has exact retained mappings, while the shared player-prop path loses provider-event mapping provenance and has a non-fail-closed doubleheader fallback |
| Feature-lineage integrity | `CONDITIONAL_IDENTITY_NOT_PROVEN` end to end; captured rows themselves are exact and hashed, but provider event ID and official schedule-source hash are lost before emission |
| Paid-request impact | `NOT_APPLICABLE`; zero requests/credits in this audit and zero added paid requests proposed |
| Postseason operational readiness | `CONDITIONAL_IDENTITY_NOT_PROVEN`; no real authoritative postseason game exists in the current authority or retained ordinary path |
| Smallest justified source correction | Fail closed in `build_mlb_predictions_wide._choose_game_for_event` unless one exact start/game-number candidate exists, then carry `provider_event_id` and the retained official schedule path/SHA-256 into prospective lineage. Separately bind the already-fetched schedule hash in Pinnacle audit rows. |

## BvP acquisition versus consumption

The inline acquisition contract is healthy independently of later feature use. The successes table has `slate_date` as its primary key, and the same-date ordinary windows return `BVP_INLINE_SUCCESS_ALREADY_EXISTS`. The identity journal directly preserves official gamePk, player/pitcher context, scheduled start, observation time, and feature/source hashes. Phase remains a later exact-gamePk join. Postseason BvP can be retained for possible 2027 strict-prior research without entering 2026 regular-season metrics, but only after prospective authority coverage and feature-specific temporal eligibility are proven.

## Market doubleheaders and reschedules

- The BvP journal on 2026-09-22 retains two same-team games as distinct gamePks with distinct starts.
- The Pinnacle binder uses team orientation plus a 10-minute start window and optional game number; it rejects zero or multiple candidates. Synthetic tests confirm those branches.
- The general player-prop binder chooses the nearest start when parseable, but chooses the first same-team game when commence time is missing or invalid. This is unsafe for doubleheaders and is the priority correction.
- Rescheduled games remain tied to the exact official gamePk selected from the current authoritative schedule. No audited collector needs calendar-derived phase. Suspended/resumed relationship metadata belongs to the phase authority/source record, not an inferred market key.

## Scope boundaries

Moneyline, Hits evaluation, Totals evaluation, and agreement evaluation were not re-audited. Their only appearance is as consumers of the acquisition artifacts identified in `acquisition_identity_manifest.csv`. No prediction, feature, price, outcome, metric, ROI, selector, Quick Card, or publication state was changed.

## Package map

- `lane_readiness.csv`: separate raw, identity, phase-join, evaluation, and operational states.
- `acquisition_identity_manifest.csv`: end-to-end source/writer/key/consumer trace.
- `provider_event_to_game_pk_audit.csv`: all retained Pinnacle and player-prop provider events.
- `ambiguous_missing_identity_ledger.csv`: exact missing/conflicting durable-binding population.
- `bvp_exactly_once_assessment.md`: exactly-once and future-use assessment.
- `paid_acquisition_impact_assessment.md`: zero-request impact decision.
- `future_2027_use_classification.csv`: strict-prior research disposition.
- `prioritized_correction_sequence.csv`: bounded, evidence-driven corrections.
- `source_sha256_manifest.csv`: reviewed source hashes.
- `audit_summary.json`: frozen machine-readable counts.
- `validate_package.py` and `validation_report.json`: dependency-free validation.
- `sha256_manifest.csv`: package artifact hashes, excluding itself and the reproducible validation report.

Smallest next action: implement and test only the fail-closed provider-event resolver/provenance change before the first ordinary postseason player-prop capture. Do not activate postseason or expand paid acquisition as part of that correction.
