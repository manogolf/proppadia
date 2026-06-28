# Canonical Identity Doctrine

This doctrine applies across Proppadia. It is sport-portable and starts from a simple rule:

IDs are identity. Names are labels. Aliases are bridges. Fallback joins are diagnostics, not foundations.

## Core Rule

Stored analytical rows should carry canonical IDs whenever the source ecosystem provides them.

For MLB:

- Player canonical identity: MLB `player_id`
- Game canonical identity: MLB `game_id`
- Team canonical identity: canonical team code, with official team ID where available
- Market research identity: `date + game_id + player_id + prop_type + side + line`

For NHL/NBA, the same doctrine applies using each league's official player, game, and team IDs.

## Labels Are Not Identity

Player names are display labels. They are useful for humans and necessary for some provider bridges, but they are not identity.

Name joins are forbidden as canonical joins when official IDs are available upstream.

Examples that must not be treated as canonical identity:

- `player_name`
- normalized player name
- player name + team
- player name + opponent
- sportsbook display name
- public catalog label

These can be used only as bridge or fallback fields with visible provenance.

## Aliases Are Bridges

External providers often do not supply official league IDs. OddsAPI, sportsbook catalogs, and public-market sources may provide:

- provider event IDs
- player names
- bookmaker labels
- teams
- market keys
- lines and prices

Those rows should be ingested as provider identity first, then graduated to canonical identity as soon as local game/player context is available.

Required bridge fields:

- provider/source name
- provider event ID when available
- source artifact path
- normalized display name if used
- canonical team/opponent after normalization
- match method
- identity provenance
- identity confidence
- unresolved/ambiguous reason when not matched

## When Fallbacks Are Acceptable

Fallback joins are acceptable only when:

1. The source does not provide canonical IDs.
2. A provider ID, event ID, team context, or source path is retained.
3. The fallback method is written to the row or diagnostic artifact.
4. Ambiguous/unresolved rows are surfaced in health checks or reports.
5. The row can be rehydrated to canonical IDs later when official context arrives.

Fallbacks are not acceptable when they silently replace available IDs.

## When Name Joins Are Forbidden

Name joins are forbidden as the primary join when both sides have:

- player ID
- game ID
- team ID or canonical team code
- market key with line/side/prop

Name joins are also forbidden when the result affects:

- grading
- reconcile accounting
- model training labels
- upload eligibility
- selector thresholds
- performance claims

Name joins may remain display-only.

## External Provider Graduation

Provider rows should graduate through these stages:

1. Raw provider row: provider event/name/team/market only.
2. Provider-context row: provider event mapped to date/game/team where possible.
3. Canonical-context row: MLB/NHL/NBA game ID and player ID attached.
4. Analytical row: canonical IDs retained through boards, reconcile, performance, research, and exports.

Rows that cannot graduate must keep a visible status such as:

- `provider_only`
- `fallback_name_team`
- `unresolved_player_identity`
- `ambiguous_player_name`
- `provider_event_unmapped`
- `probable_starter_context_only`

## Identity, Role, Market, Forecast, Outcome

Identity is not role. Role is not market availability. Market availability is not forecast trust. Forecast trust is not actual usage.

Every player-prop lifecycle should preserve these layers separately:

1. Identity Layer: stable canonical player, team, and game identity.
2. Role Layer: transient pregame/current role, such as probable starter, replaced probable, actual starter, reliever, did not appear, or unknown.
3. Market Layer: transient sportsbook availability, line, book count, selected price, disappeared market, or provider-only market.
4. Forecast Layer: model/research trust status, such as trusted forecast, insufficient history, context-only forecast, no market, or unavailable source stats.
5. Outcome Layer: actual game usage and result, such as starter innings, relief appearance, no appearance, resolved outcome, or grading status.

Canonical identity must remain stable even when role, market, forecast, or outcome changes. For example, a probable starter can have a valid pitcher market in the morning, be replaced after a weather delay, later enter as a reliever, and still retain the same canonical player/game identity with a lifecycle warning rather than being treated as an identity failure.

## Health Standard

Durable artifacts should report:

- player ID coverage
- game ID coverage
- canonical team coverage
- provider event ID coverage where relevant
- fallback identity row count
- ambiguous identity row count
- unresolved identity row count

Derived artifacts should not drop canonical IDs that existed upstream.

## Review Questions

Every feature that adds columns, artifacts, reports, derived fields, or joins must answer:

- What is the canonical identity for this row?
- Which fields prove identity?
- Which provider/alias fields are only labels?
- What fallback was used, if any?
- Is the fallback visible in the artifact?
- How does this row graduate to canonical IDs later?
- What health check catches identity regression tomorrow?

If those answers are missing, the feature is incomplete.
