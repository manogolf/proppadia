# NHL 2026 all-candidate evaluation export (bounded)

This read-only sample is bounded to the October 16, 2026 slate and was built
from existing retained reconciliation and Points grading artifacts.
`candidate_predictions.csv` holds every parsed
candidate row before filters, including unresolved outcomes. The stable key is
the natural tuple of season, slate, game, player/goalie, market, line, side,
model family/version/role, and prediction identity. Identical repeated rows are
collapsed; conflicting duplicate keys stop export generation.

The sample also joins the immutable SOG production and two fixed-blend captures
and the final pregame Puck Line challenger capture. Quote rows join normalized
retained Odds API observations using canonical event binding, player identity,
market, line, and side. Timestamp checks require the source market update and
observation to precede captured prediction time and scheduled start. Repeated
snapshots collapse to the latest eligible quote per candidate and sportsbook;
standard market records take precedence over an alternate-market alias for the
same line. Moneyline uses its separately audited ledger, including retained
no-vig probability. `candidate_quote_coverage.csv` keeps a diagnostic for
candidates without a qualified quote.

## Sample counts

| Market | Candidate rows | Settled outcome rows | Unresolved outcome rows |
|---|---:|---:|---:|
| Moneyline | 4 | 4 | 0 |
| Puck Line | 4 | 4 | 0 |
| Shots on goal | 1,635 | 1,308 | 327 |
| Points | 288 | 210 | 78 |
| Goalie saves | 104 | 104 | 0 |

Model rows, quote counts by arm, missingness, and output SHA-256 values are in
`manifest.json`. Candidate rows total 2,035. The added arms contain 132 rows
each for SOG production and each fixed blend, plus two Puck Line challenger
rows. Quote rows retain separate sportsbook identities and deduplicate to one
quote per candidate and book; see the manifest for market and model coverage.

The prediction CSV includes postgame outcome columns for evaluation. Treat
those columns as append-bound evaluation data; never feed them into a pregame
filter or decision. No candidate is represented as an executed wager.

## Current schema

`candidate_predictions.csv` includes canonical season/date/phase; run and
parent-run identity where retained; scheduled start, prediction and feature
cutoff times; prediction identity; model role/family/version and available
feature/model/artifact hashes; market/line/side/probabilities/expected count;
candidate policy status; stable game/player/goalie/team IDs; display name;
outcome status/value/source/timestamp; and source path/hash.

`candidate_quotes.csv` includes the candidate key, market identity, sportsbook,
provider event ID and market/outcome labels, American/decimal price, source quote and observation times,
qualification status, implied probability, EV, model-market edge, and source
lineage. EV uses one unit risk: `p * decimal_price - 1`. Edge is model side
probability minus market probability. Normalized Odds API rows use raw implied
probabilities, including bookmaker margin. The audited Moneyline ledger supplies
no-vig probabilities. For normalized player markets, `market_last_update` is
the retained market-level update time for the outcomes.

The regenerated bounded sample has 5,429 quote rows: 100 Moneyline, 4,374 SOG,
882 Points, and 73 Saves. All 5,429 have supported one-unit EV and market-edge
values under the probability semantics above. Puck Line has no retained
spread quotes, so its price-based calculations remain unavailable. The
coverage CSV identifies 1,218 candidate rows without a qualified quote.

Filters must write a separate derived CSV with filter expression/version/hash
and the parent raw-package manifest hash. They must not overwrite these files.
