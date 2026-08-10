# NHL Season 2026 Opening-State, Preseason, and Context Readiness

## Outcome

The frozen `NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1` is scoreable on opening night without prior-season carryover. With no current-season history, all six raw inputs are null and the unchanged scorer applies its frozen fit-only medians, producing the stored bootstrap probability path and an explicit `SEASON_OPEN_NO_HISTORY` / `MIN_HISTORY_IMPUTED` label. Each strength/rest field becomes observed after the relevant teams have one completed season-2026 game; the prior-10 field is not fully depth-populated until both teams have 10. Sparse history never silently suppresses a score.

Across seasons 2023 and 2024, 39 games had at least one team with zero prior games and 2,463 games had both teams at 10+ prior games. The attached row-level simulation preserves the exact frozen probabilities and outcomes; the depth table reports descriptive accuracy, Brier, log loss, and calibration without tuning or refitting.

## Preseason and identity

The official schedule represents preseason with game type 1, regular season with 2, and postseason with 3, and supplies game IDs, teams, and scheduled starts. The repository importer retains `game_type`, but the current mainline shadow `GAME_COLS` and run archive omit it. Therefore preseason plumbing is not safe for execution until game type is propagated through spine, prediction, metadata, grading, and evaluation filters. Preseason must be labeled `PLUMBING_VALIDATION_ONLY` and probabilities `PRESEASON_NON_EVALUATION`.

## Goalie and lineup sources

The official NHL roster and gamecenter feeds provide strong NHL identity and actual-game context, but not a certified projected/confirmed starter feed. SportsDataIO is the best currently documented single goalie path: its workflow states projected goalies are published the night before and confirmed starters update with announcements. It is commercial and not connected; provider event timestamps and NHL-ID crosswalks require a schema proof capture.

For lineup/injury, SportsDataIO is also the best single path because it documents current line combinations (even-strength and power-play) and distinct injury status. Its line combinations are not historically available, so prospective append-only polling is essential. Official roster state remains useful separately. No public-web scraping integration is recommended.

Primary documentation inspected: https://sportsdata.io/developers/workflow-guide/nhl, https://sportsdata.io/nhl-api, https://developer.sportradar.com/ice-hockey/v5/reference/nhl-injuries, and the repository-connected NHL schedule/roster/gamecenter endpoints.

## Contracts and readiness

Goalie, roster, injury, projected lineup, confirmed lineup, scratch, and actual-goalie events remain separate context types. The archive grain is `canonical_season + game_id + source + source_timestamp_utc + context_type`; transitions append and never overwrite. Pregame qualification requires deterministic binding and source/capture timestamps before scheduled start.

The exactly one next bounded task is `NHL_SEASON_2026_GAME_TYPE_PROPAGATION_AND_PRESEASON_ISOLATION_IMPLEMENTATION`. It unlocks safe preseason isolation and a later human-initiated real preseason validation; it does not unlock live execution, wagering, evaluation claims, or automation.

## Decision summary

- `NHL_SEASON_2026_OPENING_STATE_BEHAVIOR_CERTIFIED` = `READY`
- `NHL_SEASON_2026_EARLY_SEASON_HISTORY_DEPTH_CHARACTERIZED` = `READY`
- `NHL_SEASON_2026_PRIOR_SEASON_CARRYOVER_STATUS` = `NOT_AVAILABLE`
- `NHL_SEASON_2026_PRESEASON_GAME_IDENTITY_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_PRESEASON_SHADOW_VALIDATION_READINESS` = `BLOCKED_BY_GAME_TYPE_PROPAGATION_AND_LIVE_PRESEASON_FIXTURE`
- `NHL_SEASON_2026_REPEATED_RUN_VALIDATION_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_GOALIE_SOURCE_READINESS` = `SOURCE_IDENTIFIED_NOT_CONNECTED`
- `NHL_SEASON_2026_GOALIE_TIMESTAMP_CERTIFICATION_READINESS` = `NOT_READY`
- `NHL_SEASON_2026_LINEUP_INJURY_SOURCE_READINESS` = `SOURCE_IDENTIFIED_NOT_CONNECTED`
- `NHL_SEASON_2026_CONTEXT_ARCHIVE_READINESS` = `READY_WITH_BOUNDED_LIMITS`
- `NHL_SEASON_2026_REGULAR_SEASON_OPENING_READINESS` = `BLOCKED_BY_PRESEASON_LIVE_VALIDATION`
- `NHL_SEASON_2026_MAINLINE_WAGER_RECOMMENDATION_READINESS` = `NOT_READY`
