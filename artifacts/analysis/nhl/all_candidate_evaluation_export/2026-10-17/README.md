# NHL 2026 all-candidate evaluation export (bounded)

This read-only sample is bounded to the October 16, 2026 slate and was built
from existing retained reconciliation and Points grading artifacts.
`candidate_predictions.csv` holds every parsed
candidate row before filters, including unresolved outcomes. The stable key is
the natural tuple of season, slate, game, player/goalie, market, line, side,
model family/version/role, and prediction identity. Identical repeated rows are
collapsed; conflicting duplicate keys stop export generation.

`candidate_quotes.csv` has the documented quote columns but currently has no
data rows. The retained reconciliation attachments have prices and market
summaries without both a source quote timestamp and an observation timestamp.
The exporter therefore cannot establish that a quote was known by prediction
time and before scheduled start. It does not calculate EV or model-market edge
without a qualified quote. `candidate_quote_coverage.csv` contains one
diagnostic per candidate explaining the missing timestamp-qualified quote.

## Sample counts

| Market | Candidate rows | Settled outcome rows | Unresolved outcome rows |
|---|---:|---:|---:|
| Moneyline | 4 | 4 | 0 |
| Puck Line | 2 | 2 | 0 |
| Shots on goal | 1,239 | 993 | 246 |
| Points | 288 | 210 | 78 |
| Goalie saves | 104 | 104 | 0 |

Model rows and output SHA-256 values are in `manifest.json`. On this slate,
the model counts are 2 Moneyline V3 + 2 V2, 2 Puck Line baseline, 237 each
for SOG arms A/B/C, 264 each for arms D/G, 144 each for Points HGB/Phoenix,
and 104 Saves. Models are
separate rows: Moneyline V3/V2, Points HGB/Phoenix, and the qualified cold-start
SOG arms retained in reconciliation. The Puck Line challenger and the SOG
production/fixed-blend arms are not present in these reconciliation row
sources, so this package does not claim complete model-arm coverage.

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

`candidate_quotes.csv` includes the candidate key, market identity, sportsbook
and provider IDs, American/decimal price, source quote and observation times,
qualification status, implied probability, EV, model-market edge, and source
lineage. EV uses one unit risk: `p * decimal_price - 1`. Edge is model side
probability minus a valid market probability.

Filters must write a separate derived CSV with filter expression/version/hash
and the parent raw-package manifest hash. They must not overwrite these files.
