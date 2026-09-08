# NHL Season 2026 Points and Goalie Saves Operational Readiness Assessment

Assessment date: 2026-09-08 (America/Los_Angeles)  
Scope: repository and read-only database inventory only  
Production changes: none

## Decisions

| Lane | Decision | Basis |
|---|---|---|
| Points | `READY_AFTER_BOUNDED_REMEDIATION` | The surviving three-line logistic scorer is callable and reproduces the retained 2026-04-16 snapshot byte-for-byte. The lane lacks run-bound inputs, immutable predictions/quotes, snapshot semantics, policy lineage, lane-specific health, safe participation handling, and regular-season evaluation isolation. Its probabilities also violate ladder monotonicity for 222 of 310 players in the retained slate. These are bounded shadow-operating defects; they do not require retraining to begin non-actionable preseason capture. |
| Goalie Saves | `NOT_READY_FOR_PRESEASON_BURN_IN` | The lane has no certified projected/confirmed starting-goalie source, cannot qualify participants pregame, and must not use actual starter data as a substitute. The live exporter emits `start_prob` as null and scores every rostered player with prior goalie history. Additional model-package, line-validation, market, persistence, and grading defects remain. |

`Goalie Saves cannot operate safely without a certified projected/confirmed starting-goalie source.` The official NHL API and `goalie_game_logs_raw` provide identity and actual postgame context, not a timestamp-certified pregame state. The August 10 goalie-source assessment found no configured SportsDataIO or Sportradar credential, no source player crosswalk, and no documented source-state timestamp suitable for certification.

## Evidence boundary and current state

- The natural 2026-09-08 morning run completed a fresh official-schedule fetch as `VALID_EMPTY_SLATE`, with zero games and no blockers. Because the slate was empty, it did not exercise Points or Goalie Saves preparation.
- Read-only database inventory found no season-2026 rows in `nhl.games`, `nhl.roster_status`, `nhl.goalie_game_logs_raw`, `nhl.skater_game_logs_raw`, or `nhl.training_features_goalie_saves_v2`. The latest current-season operational rows remain from 2026-04-16, except goalie logs ending 2026-02-01 and skater logs ending 2026-04-15.
- Historical `nhl.predictions` contains 164,754 `player_points` rows and 77,103 `goalie_saves` rows from 2025-10-07 through 2026-04-16. Both lanes are recorded generically as family `phoenix`, feature hash `phoenix_v2`, and model version `phoenix_v2` or null. Those labels do not identify the actual artifact bytes.
- There are no saved Points or Goalie Saves rows in `nhl.user_props` at assessment time, so the repository grading path is callable but has no live lane evidence.
- The Odds API and database credentials are configured. No projected-goalie provider credential is configured. No network acquisition or model fit was performed for this assessment.

## Surviving model and scorer identity

### Points

The callable scorer is `backend/nhl/scripts/score_points_phoenix.py`. It discovers line directories under `backend/nhl/models/latest/points`, loads only `lr.joblib`, and emits `points_phoenix_lr` probabilities for 0.5, 1.5, and 2.5 points. The co-located random-forest files survive but are not callable from the daily scorer.

All three called models are `StandardScaler` followed by L2 `LogisticRegression(C=1.0, class_weight=balanced, solver=liblinear, max_iter=300)`. The full learned scaler and logistic parameters are frozen by these artifact hashes:

| Line | Called LR SHA-256 | Uncalled RF SHA-256 | LR intercept | Recorded rows / LR AUC |
|---|---|---|---:|---|
| 0.5 | `4eb459587b70ef41e709dddc7e1f54b8341c3eba208926022c4803295e370add` | `6dd6ff1b7ae21ac9f3646f4947ed099e3ade97e3ae9d65f7b0e7b72f57881a63` | -0.02289964172174365 | 80,404 / 0.6353383659 |
| 1.5 | `f23a97ade5473cb9557a2f93c0b93c39558cb15607663dd393b707d690994812` | `f8a429135cd0f0bac53b59c7d0fa29884ad27e5a86f4582362cc67d22a56a522` | -0.17222986565262688 | 80,404 / 0.7010889596 |
| 2.5 | `9cea920df96b178e0b554eac0e0c83c201e23f5ec1f2d6daa1c3900a470b8b44` | `e1eb1bdfb7f83b3739490898a1de5c3fd295fa51a47396152e08c92eae0bda96` | -0.3820013939075826 | 80,404 / 0.7439593791 |

The feature metadata names a removed file, `exports/points_training_frame_phoenix_2023_2024.csv`, as training input. The repository does not retain that population, a split manifest, Brier/log-loss/calibration evidence, or a model-package index. The literal metadata version is `v1`; the database loader instead writes `phoenix_v2`.

The 15 inputs are SOG/attempt proxies rather than a SOG scorer reuse: home state; rolling player SOG rates and totals; rolling attempt rates and totals; team SOG/attempt context; player team-share; and a hot-last-five flag. Points has separate scorer, artifacts, SQL, market normalizer, UI surface, and points-specific actual construction. Similar feature names do not confer SOG operational integrity.

### Goalie Saves

The callable scorer is `backend/nhl/scripts/score_nhl_props.py`, using exactly `MODEL_INDEX.json` and `MODEL_ARTIFACT.json` under `backend/nhl/models/latest/goalie_saves`:

- index SHA-256: `150cf3ebbb65ee6769c8641659eda709822ae5473a4016b13373410902f4cee2`
- artifact SHA-256: `28f2d531dfc80ea850cdcbe022b2168612db5ce20931cf647f3ab9d06c306122`
- family: Poisson, alpha metadata 0.1
- intercept: 2.8874870705579982
- ordered coefficients: `[-0.02881233277033034,-0.004396163667899705,0.029273682991793226,0.000014807531785590077,0.006380902714381878,0.007091443617453157,-0.0008788873710245714,0.03410675328208215,-0.000997150604129067]`
- ordered inputs: `is_home,rest_days,b2b_flag,start_prob,d5_saves_per60,d10_saves_per60,d5_shots_faced_per60,season_save_pct,opponent_id`
- evaluated lines: 24.5 and 28.5; 21-day holdout raw Brier 0.2559863/0.2448598, log loss 0.7048131/0.7084822, AUC 0.4047619/0.3333333
- calibration: isotonic only at 24.5, fitted on 32 observations; recorded calibrated Brier 0.2258772 and log loss 0.6345918

The daily path nevertheless emits 13 lines from 18.5 through 30.5. Only 24.5 and 28.5 have recorded evaluation, and only 24.5 has calibration. The API exposes only 18.5 through 23.5, excluding both evaluated lines.

Package identity is ambiguous: `MODEL_INDEX copy.json` has a different 14-day holdout, while `MODEL_ARTIFACT copy.json` and `goalie_saves.json` are identical to each other but contain different coefficients and no calibrator. The active scorer is deterministic only because its filenames are hard-coded; there is no create-only release manifest protecting `models/latest`.

## Reproducibility

The retained 2026-04-16 input snapshots were replayed without repository writes:

| Lane | Input rows | Input SHA-256 | Output rows | Retained/replay SHA-256 | Result |
|---|---:|---|---:|---|---|
| Points | 310 players / 6 games | `daa7515d5760f773037b990627b228d63b0b432299077511122612500659e87f` | 930 | `4ecc984864f5e6fd6ae3e703683b5c148e4e95c58a4c5a38325610c9125ce6bb` | byte-identical |
| Goalie Saves | 28 goalies / 6 games | `6532d1b0ce77e4dfaf62fe899e72d293d50a8b2f13aacc5603adcaf3da282101` | 28 wide rows / 364 player-lines | `c248a82d9247108ec0afda17c1730ef13e06940adac7bf2ccbc2c781546b0375` | byte-identical |

This proves fixed-input scorer reproducibility only. Full historical predictions are not certifiably reproducible: daily inputs and predictions are mutable, database predictions update on conflict, the Points training population is absent, and the 191 date-named odds-history directories retain derived market CSVs without create-only enforcement, parent hashes, or raw Points/Saves prediction files. The Saves scorer also estimates winsor limits and medians from each current slate, making output dependent on the exact contemporaneous row population.

## Feature timing and participation

Points history uses only rows with game date strictly before the slate and a regular-season game-ID pattern. Goalie Saves uses strict earlier dates and `game_type=2`. These predicates prevent same-game leakage in the SQL itself. They are not sufficient for certification because inputs are not snapshotted, source rows can be updated, and no source-as-of timestamp is bound to a run.

Points limits all history to the prior 120 days while naming two aggregates “season to date.” At season opening this can collapse most or all prior features to zero, and it is not the same opening-state contract used by SOG. The base population uses every `roster_status` row for the date without checking `active_flag`, a confirmed lineup, or scratch state.

Goalie Saves creates a base population from every rostered player who has ever appeared in `goalie_game_logs_raw`. Its separate seed SQL assigns a workload heuristic of 0.35/0.55/0.65, but the authoritative exporter does not consume that table and explicitly outputs `NULL AS start_prob`. The scorer then reduces missing values to zero. Neither the heuristic nor roster membership is a projected or confirmed starter source.

Nonparticipants are unsafe in both lanes. Missing actuals become `dnp` after two days with no proof that ingestion completed and no explicit scratched/nonparticipant event. For Saves, a backup goalie can be predicted and later converted to DNP, which does not repair the invalid pregame participant population.

## Market, candidate, upload, and execution path

`backend.nhl.cli daily --with-odds` requests `player_total_saves` and `player_points` from The Odds API across `us,us2`. The raw provider JSON can contain book keys, both sides, prices, market update timestamps, and event state, but it is written to mutable `events_today.json`, `odds_nhl_playerprops_today.json`, and `odds_latest.json`.

Both builders retain only Over and collapse books to a median price. They discard book identity, Under, source/observation timestamp, availability/status, and deterministic provider-event binding. They then join by normalized player name plus line rather than canonical provider player/game identity. The retained April 16 coverage was 187/930 Points rows and 8/377 Saves rows. Saves contained 13 duplicate `(player_id,game_id,line)` rows because its alias expansion multiplies rows before a merge still keyed on the unexpanded normalized name.

There is no frozen Points or Saves candidate policy/effective configuration. The operator-facing `/nhl/props` research mode ranks model-minus-market Over edges dynamically with hard-coded price bounds. Points appears only in “Top Points Model Edges” and has no governed save/upload action. Saves appears on the board with “Save best”; that action selects the largest raw Over probability, which will normally be the lowest ladder line, not the best edge. Saved rows carry no model artifact hash, quote identity, sportsbook, quote timestamp, run type, policy identity, or execution record.

The generic database loader uses `ON CONFLICT ... DO UPDATE`, labels both lanes `phoenix/phoenix_v2`, and stores empty model parameters. Predictions, user-prop grading, and date archives are therefore mutable. Research/candidate/upload/execution states are not represented as separate immutable objects.

## Snapshot, grading, game type, and health

Points and Saves have no `MIDDAY` or `FINAL_PREGAME` run type and no comparison between snapshots. The hardened SOG quote and shadow namespaces demonstrate the required integrity shape, but they are lane-specific and are not inherited by these legacy paths.

Grading can derive Points from `points` or goals+assists and Saves from official goalie logs. It updates `nhl.user_props` in place, has no append-only outcome/correction ledger, and silently suppresses grading failures when history is read. Preseason rows are not labeled non-evaluation. Although official `game_type` is present in `nhl.games` and both feature histories exclude preseason, Points/Saves predictions, market outputs, saved props, and grades do not carry or enforce game type. Preseason can therefore enter ordinary prop history/evaluation surfaces.

The installed 07:30 morning orchestration calls `backend.nhl.cli daily --morning-only`; on a nonempty slate that exports Points and Saves inputs. Its health contract exposes only Mainline and SOG prerequisite readiness. It records no Points/Saves eligible population, feature completeness, starter state, market state, or downstream gate. The generic sentinel can evaluate supplied prop probabilities and outcomes, but the morning invocation supplies none of those lane-specific records. There is no active MIDDAY or FINAL_PREGAME orchestration for these lanes.

## Authoritative operator paths

The only end-to-end callable path for either lane is the legacy full daily command:

`source backend/.env && .venv/bin/python -m backend.nhl.cli daily --with-odds`

The installed scheduler runs only its `--morning-only` prerequisite subset. Independent build subcommands exist but are testing-guarded. The operator UI is ops-restricted `/nhl/props`; `/nhl/predictions` redirects there. `/nhl/props-form` is SOG-only and explicitly stages Points/Saves as inactive. The presence of model files and UI code does not mean either lane has a season-2026 callable immutable burn-in workflow.

## Final conclusions

Points is not SOG-by-another-name. Its model family, model packaging, feature construction, ladder behavior, market reducer, candidate surface, upload behavior, grading, and participation eligibility are separate and materially weaker. It can reach preseason shadow burn-in after a bounded, lane-specific immutable operating wrapper is built without changing or retraining the scorer.

Goalie Saves cannot be made pregame-safe by wrapping the current scorer alone. A timestamp-certifiable projected/confirmed starter source and deterministic goalie crosswalk are prerequisites. Actual starter information is grading-only.

The exact next bounded implementation task is `NHL_POINTS_IMMUTABLE_PRESEASON_SHADOW_PATH_V1`: bind the existing three hashed LR artifacts without refitting; create a run-bound official game/player/type spine, immutable strict-prior input and prediction package, book-level two-sided quote capture for `MIDDAY` and `FINAL_PREGAME`, explicit nonparticipant/game-type gates, an observation-only policy declaration, upload-shaped-but-nonexecuting output, and lane-specific sentinel input. Do not modify the legacy scorer or activate production recommendations.
