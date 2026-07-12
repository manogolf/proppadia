# Champion–Challenger Experiment Specification v1.0

Living source of truth: this canonical document governs future Proppadia model-development experiments. The immutable v1.0 copy is preserved at `artifacts/analysis/model_development/champion_challenger_reset_2026-07-10/champion_challenger_experiment_specification_v1_0_2026-07-10.md`.

## Purpose

This framework exists to determine whether a proposed model improvement should replace the current production champion.

Every experiment must produce one or both of:

- a promotion decision,
- durable knowledge gain.

A challenger that is not promoted is still useful when it conclusively explains why promotion should not occur.

## 1. Scope and Governing Principle

This specification applies to model evolution across Proppadia sports, including MLB, NHL, and future sports.

It separates four kinds of promotion:

- feature promotion,
- model promotion,
- operational promotion,
- production promotion.

No one should be inferred from another. A feature can become mature enough to test without changing a model. A model can win an offline comparison without changing reports, uploads, schedulers, or production paths. Operational visibility can exist before production promotion. Production promotion requires a separate approved implementation task after a model-development decision.

## 2. Champion Definition

The champion is the immutable production baseline for an experiment.

The champion freeze record must include:

- exact runtime model artifact path,
- artifact checksum or hash,
- training date and training-data range,
- training dataset or manifest version,
- exact feature list,
- preprocessing pipeline,
- target definition,
- hyperparameters,
- calibration method,
- ranking or selection method, when applicable,
- inference pipeline version or commit,
- evaluation pipeline version or commit,
- baseline metrics,
- baseline prediction outputs used for comparison.

The existing MLB champion has effectively remained conceptually frozen despite automated weekly weight refreshes, but the exact runtime artifact path still must be captured before the first post-reset challenger.

## 3. Challenger Definition

Each challenger must introduce exactly one intentional conceptual change, such as:

- feature bundle,
- target revision,
- architecture,
- preprocessing,
- calibration,
- ranking or selection strategy.

Bundled experiments are allowed only when explicitly declared and justified. A bundled challenger must explain why the components cannot be tested separately first.

## 4. Pre-Training Challenger Proposal

The challenger proposal must be written and frozen before training.

It must include:

- experiment ID,
- sport and prop scope,
- hypothesis,
- baseball or sport mechanism,
- expected beneficiary population,
- expected failure modes,
- possible performance tradeoffs,
- intentional change,
- explicit non-changes,
- primary evaluation objective,
- secondary guardrails,
- predefined promotion criteria,
- predefined failure interpretations.

## 5. Feature and Data Inventory

For every added or materially changed feature, the proposal must document:

- source,
- lineage,
- owner or generating process,
- refresh cadence,
- historical availability,
- prediction-time availability,
- missing rate,
- imputation or fallback policy,
- backfill status,
- temporal leakage assessment,
- known limitations,
- operational dependency,
- overlap or redundancy assessment against champion features.

No undocumented feature may enter training.

## 6. Data Integrity Gate

Training may not begin until each applicable item is explicitly marked `PASS`, `FAIL`, or `NOT_APPLICABLE`:

- lineage verified,
- historical backfill sufficient,
- prediction-time availability verified,
- no future leakage,
- row grain verified,
- join keys verified,
- denominator verified,
- settlement policy verified,
- coverage acceptable,
- missing-value policy documented,
- duplicate policy documented,
- temporal split verified,
- training and evaluation populations defined,
- official outcome source defined,
- price and no-vig policy defined where ROI is evaluated.

A `FAIL` blocks training unless the experiment is explicitly classified as a diagnostic experiment that cannot produce a promotion decision.

## 7. Champion Freeze Package

Before challenger training, preserve:

- champion artifact,
- champion artifact hash,
- champion configuration,
- champion feature manifest,
- champion predictions,
- champion evaluation outputs,
- evaluation population manifest,
- source-data snapshot or reproducible references,
- code commit,
- environment or dependency manifest where practical.

The freeze package must be sufficient to reproduce the comparison later.

## 8. Training Contract

Freeze before execution:

- training population,
- temporal split,
- random seeds,
- feature bundle,
- preprocessing,
- hyperparameters,
- tuning policy,
- early-stopping policy,
- missing-value behavior,
- calibration behavior,
- output paths.

No post-result tuning is allowed within the same experiment. Any material change after results are viewed becomes a new experiment ID.

## 9. Evaluation Contract

Champion and challenger must be evaluated on identical eligible rows wherever technically possible.

Required predictive metrics should include, where applicable:

- log loss,
- Brier score,
- calibration,
- ROC/AUC,
- rank correlation,
- precision or win rate by rank bucket,
- lift over baseline,
- probability distribution diagnostics.

Required operational metrics:

- eligible-row coverage,
- missing rate,
- inference runtime,
- failure rate,
- output stability,
- daily-pipeline compatibility.

Required market and financial metrics where applicable:

- win rate,
- ROI,
- units,
- average price,
- no-vig expected value or model-versus-market gap,
- sample size,
- confidence interval or uncertainty estimate,
- performance by prop, line, side, price band, and meaningful population segment.

Require temporal validation rather than reliance on one slate or one undifferentiated aggregate.

Metric interpretation must distinguish prediction quality from market profitability. A model can improve probability estimation without producing positive ROI, and ROI can move because of pricing, selection, or sample variance.

## 10. Promotion Contract

Promotion criteria must be written before training.

Promotion requires:

- improvement in the predefined primary objective,
- no guardrail degradation beyond predefined tolerances,
- adequate sample and temporal coverage,
- reproducible inference,
- acceptable operational coverage,
- no unresolved leakage or reconciliation defects,
- daily-pipeline compatibility,
- documented segment behavior,
- no dependence on one exceptional slate or narrow unexplained subgroup.

Every experiment must end with an explicit decision:

- promote,
- do not promote,
- diagnostic only / cannot decide.

Promotion may not be based on narrative judgment created after viewing results.

## 11. Mandatory Outcome Classification

Every experiment must conclude with exactly one primary classification:

- `PROMOTED`: the challenger met the predefined promotion criteria and is approved for the next explicit rollout step.
- `VALID_NO_IMPROVEMENT`: the experiment was valid and the challenger did not materially improve the champion.
- `FEATURE_REDUNDANT`: the added feature or bundle added no useful incremental signal beyond champion features.
- `DATA_LIMITED`: data availability, coverage, or missingness prevented a reliable decision.
- `MODEL_LIMITED`: the model architecture could not express or use the intended signal.
- `TARGET_LIMITED`: the target did not match the intended production or market value question.
- `REPORTING_DEFECT`: report construction, denominator, row grain, or display logic made the result unreliable.
- `RECONCILIATION_DEFECT`: outcome grading, settlement, or join-to-result logic made the result unreliable.
- `IMPLEMENTATION_DEFECT`: code, pipeline, artifact, or execution behavior invalidated the experiment.
- `DATA_DEFECT`: source data quality, staleness, correction drift, or lineage defects invalidated the experiment.
- `BASEBALL_INTERPRETATION_UNSUPPORTED`: the sport-specific story was not supported by evidence, even if the data pipeline worked.
- `BASEBALL_HYPOTHESIS_REJECTED`: the sport-specific hypothesis was cleanly tested and failed.
- `INSUFFICIENT_EVIDENCE`: the sample, window, or coverage was too small to decide.
- `INCONCLUSIVE`: the experiment design or execution cannot answer the question.

Do not permit "the champion is unbeatable" or "the challenger lost" as a sufficient technical conclusion.

When `INCONCLUSIVE` or `INSUFFICIENT_EVIDENCE` is used, the report must state the exact missing evidence and the predefined condition that would resolve it.

## 12. Knowledge Capture

Every completed experiment must preserve:

- original proposal,
- freeze package references,
- implementation summary,
- data-integrity results,
- evaluation tables,
- segment diagnostics,
- promotion decision,
- primary outcome classification,
- causal interpretation,
- limitations,
- reusable assets,
- follow-up recommendation,
- whether the experiment should ever be repeated.

## 13. Promotion Boundaries

The following transitions are distinct:

- a feature becoming mature enough to test,
- a challenger being trained,
- a challenger beating the champion,
- a model being approved for shadow operation,
- a model becoming the production champion,
- operational reports or uploads being changed.

None of these transitions should happen implicitly.

Production model paths, uploads, schedulers, reports, and LaunchAgents change only through a separate approved implementation task after model promotion.

## 14. Routine Retraining Policy

Routine retraining without a change to the feature set, target definition, architecture, preprocessing, calibration, or corrected training data is not a Champion-Challenger experiment and must not be represented as model evolution.

A scheduled retrain may refresh model weights, but it does not establish a new conceptual champion unless it is evaluated and accepted under this specification.

Event-driven retraining is the governing philosophy. Appropriate triggers include feature promotion, data-defect repair, material distribution drift, target redesign, architecture change, or an explicitly justified scheduled refresh with measured benefit.

This specification does not alter the current scheduler.

## 15. Experiment Record Template

```markdown
# Champion-Challenger Experiment Record

- Experiment ID:
- Date:
- Owner:
- Sport:
- Prop scope:
- Champion path/hash:
- Challenger change:
- Hypothesis:
- Mechanism:
- Data-integrity gate:
- Training contract:
- Primary metric:
- Guardrails:
- Promotion thresholds:
- Evaluation range:
- Results:
- Outcome classification:
- Decision:
- Limitations:
- Follow-up:
```

## 16. Initial Application

The first intended use of this specification is:

`MLB Hits PA/opportunity-only challenger`

Candidate feature bundle:

- `plate_appearances`,
- `d7_plate_appearances`,
- `d15_plate_appearances`,
- `d30_plate_appearances`,
- `pa_missing_flag`.

This document does not approve or train that challenger.

Its next prerequisites are:

- capture the exact runtime champion artifact path and hash,
- complete champion-feature overlap/redundancy analysis,
- create and approve its experiment record under this specification.
