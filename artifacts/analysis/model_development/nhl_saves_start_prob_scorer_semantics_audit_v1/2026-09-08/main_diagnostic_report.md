# NHL Saves start-prob scorer semantics audit v1

## Decision

`SCORER_OUTPUT_UNCHANGED_BUT_SEMANTICS_REQUIRE_EXPLICIT_CONTRACT`

Required semantic classification: **conditional Saves distribution given that the named goalie starts**. The fitted distribution is not unconditional: every one of 5,798 fitted rows set `start_prob=1.0`, and no probability-of-start multiplier is applied to expected saves or proposition probabilities. The historical label population joins actual goalie participants without a `start_flag` filter; starter purity is therefore not certifiable (5,397/5,717 matched rows were the unique max-TOI goalie and 320 were lower-TOI participants, with max TOI retained only as a postgame diagnostic proxy). This is an explicit model-quality caveat, not evidence of an unconditional formulation.

## `start_prob` result

`start_prob` is the fourth fitted feature (zero-based position 3), has no scaler, and has coefficient `1.4807531785590077e-05`. The training value was constant 1, so the coefficient does not represent a learned start-likelihood effect. The authoritative exporter emits null for every row; `prepare_X` converts the all-null binary column to 0.0. Null and explicit zero replay byte-identically. Across all 28 representative April 16 rows and all 13 lines, 0 to 1 changed expected saves by at most `0.00028618417805503782` and final probability by at most `2.600557877119325e-05` (0.00260055787712 percentage points). Values 0.35 and 0.55 were observed in the separate heuristic feature table; 0.65 is code-reachable but unobserved. None is consumed by the authoritative exporter.

The null replay SHA256 `c248a82d9247108ec0afda17c1730ef13e06940adac7bf2ccbc2c781546b0375` exactly equals the retained `saves_predictions.csv` SHA256. No fitted artifact, source input, or prior package was modified.

## Starter and participation trace

No projected-starter or confirmed-starter field exists in the authoritative Saves scoring path. Eligibility is roster membership plus any prior goalie-log identity, so all historically appearing roster goalies are scored. Actual `start_flag` originates from boxscore/outcome state and was never supplied to prediction code before puck drop. The current grader joins actual `saves` without requiring `start_flag`; a missing goalie log becomes DNP after the configured delay. For the season-2026 contract, actual starter must instead remain postgame-only and control participation/evaluation eligibility.

Scoring is rowwise after feature preparation, but preparation winsorizes and imputes on the current scoring batch. In the April 16 market-priced six-goalie diagnostic, rescoring only the subset rather than the complete 28-goalie snapshot changed expected saves by as much as `0.040193707015660607` and probability by `0.0036511233626876916` (0.365112336269 percentage points). Thus extra goalies generally create extra conditional predictions, but changing the preprocessing population can also change retained-goalie probabilities. The shadow implementation must transform one complete immutable slate snapshot and gate eligibility afterward.

## Season-2026 shadow contract

- Predictions explicitly conditional on starting.
- Population limited to the latest qualified pregame state.
- Exactly one unique goalie per canonical game-team.
- Multi-book agreement required.
- Status `MARKET_LISTED_STARTER_UNCONFIRMED`; market listing is not starter confirmation.
- No representation of projected or confirmed starter status.
- Actual starter used only for participation and grading.
- Nonstarter excluded from the scored evaluation denominator.
- C/U/E disabled.

## Disposition

The nonzero coefficient makes “unused” technically false, while the measured effect is operationally immaterial and does not justify scorer reconstruction or refitting. The conditional semantics must be explicit, and population-dependent preprocessing must be preserved carefully.

Exact next bounded task: `NHL_SAVES_IMMUTABLE_MARKET_LISTED_PRESEASON_SHADOW_PATH_V1`.
