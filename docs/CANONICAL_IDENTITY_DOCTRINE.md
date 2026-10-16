# Canonical Identity Doctrine

This doctrine applies across Proppadia. It is sport-portable and starts from a simple rule:

IDs are identity. Names are labels. Aliases are bridges. Fallback joins are diagnostics, not foundations.

## Core Rule

Stored analytical rows must carry official numeric player, team, and game IDs
whenever the sport's official source provides them. Team codes may remain useful
descriptive attributes, but they are not the canonical team key.

For MLB:

- Player canonical identity: official MLB numeric player ID
- Game canonical identity: official MLB numeric game ID
- Team canonical identity: official MLB numeric team ID
- Team codes and abbreviations remain labels/attributes, not canonical keys.
- Market research identity: `date + game_id + player_id + prop_type + side + line`

For NHL, canonical player, game, and team identities are the official NHL
numeric IDs. NBA and future sports use the corresponding official numeric IDs
provided by their governing data source.

An internal or legacy numeric ID belongs to its own namespace. It is not an
official ID unless a crosswalk explicitly establishes that relationship.
Every cross-namespace mapping must be unique, validated against authoritative
context, and traceable to retained source evidence. For example, the PP-TOI
historical investigation decoded legacy NHL team IDs through the original
internal abbreviation map; comparing those raw numbers directly with official
NHL team IDs produced false mismatches.

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
They must never be authoritative database join keys, including when their
normalized forms happen to be unique in one observed sample.

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
- original provider text, preserved without normalization

Normalization for candidate discovery must account for diacritics and
transliterations, punctuation, initials, compound surnames, and suffixes
(`Jr.`, `Sr.`, `II`, `III`). Retain both the original text and any normalized
candidate form. Normalization proposes candidates; it does not prove identity.
A match must bind to verified official player, team, and game identities using
the available provider event, team, schedule, and source context. If more than
one candidate remains plausible, leave the row unresolved.

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
- official team ID
- market key with line/side/prop

Name joins are also forbidden when the result affects:

- grading
- reconcile accounting
- model training labels
- upload eligibility
- selector thresholds
- performance claims

Name joins may remain display-only.

An alias or normalized-name join must not be promoted to an authoritative
relationship solely because it returns one row. The candidate and supporting
evidence must be retained, and the final binding must be validated against
official numeric identities and game context.

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

When a downstream file has a fixed external schema, canonical identity should be preserved in a parallel diagnostics artifact rather than silently omitted. The diagnostics artifact must be generated by the same daily workflow, linked from health reports or indexes, and gated with the same coverage standards as embedded identity columns. This keeps external upload behavior stable while still preserving identity lineage for audits.

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

## Source Integrity and Governing Contracts

Identity mappings and joins must also preserve source truth and source
completeness. Follow the project-wide [Missingness and Zero Semantics
Contract](contracts/missingness_and_zero_semantics.md) and the source-integrity
requirements linked from [Project Doctrine](PROJECT_DOCTRINE.md). Research,
training, and scoring code must not alter canonical source identifiers as a side
effect. Changes to ingestion, identity mapping, feature joins, or reconciliation
must review these governing contracts before implementation.
