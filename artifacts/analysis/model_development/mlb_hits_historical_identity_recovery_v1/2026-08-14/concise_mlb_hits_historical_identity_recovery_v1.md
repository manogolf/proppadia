# MLB Hits historical identity recovery v1

- Frozen inputs: 27,587 four-lane model predictions; 40,866 raw BetOnline side propositions; 7,564 original synchronized rows.
- Investigated 18,334 original unmatched model rows. Recovered 7,618 exact identities, 7,618 valid pregame observations, and 5,054 outcome-complete candidate rows.
- Candidate synchronized population: 12,618 rows; the original population was not overwritten.
- Snapshot selection: latest retained exact observation strictly before the selected model prediction, without price or outcome optimization. All retained observations remain in source artifacts.
- Composition: `RECOVERED_POPULATION_COMPOSITION_SIMILAR`.
- `HISTORICAL_MODEL_IDENTITY = UNRESOLVED`.
- Decision: `HITS_IDENTITY_RECOVERY_EXPOSES_SYSTEMATIC_HISTORICAL_MATCHING_GAP`.
- Human-review next-step gate: `RERUN_PREDICTIVE_PARITY_ON_FROZEN_RECOVERED_POPULATION`. Predictive parity was not rerun.
