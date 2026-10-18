# NHL 2026 full-window prospective candidate export

This read-only package covers retained canonical 2026 regular-season evidence
from September 29 through October 17, 2026. September 19–26 source rows are
game type 1 preseason; rows without a regular-season identity are excluded.
The October 16 bounded package remains unchanged as an engineering fixture.

`candidate_predictions.csv` contains all deduplicated candidates from the
latest retained reconciliation package per slate, augmented with retained SOG
fixed-blend captures, Puck Line challenger captures, and the October 17 parent
run predictions. It keeps production, challenger, and shadow arms separate.
The October 17 parent receipt's eligible game IDs constrain its unsettled
predictions to four games; ten games already in progress are excluded by that
receipt. Those 3,054 candidates remain unresolved and conditional.

`candidate_quotes.csv` retains the latest chronology-qualified quote per
candidate and sportsbook. Source market update and observation timestamps must
precede the prediction timestamp, which must precede scheduled start. The
audited Moneyline ledger is used when retained. EV is one-unit expected return;
edge is model-side probability minus the retained market probability. These
values are unavailable when the necessary price or probability evidence is
missing.

`candidate_quote_coverage.csv` records candidates without any chronology-
qualified quote and retained normalized quotes that could not be bound to a
candidate. Both retain reason and source lineage. No quote is inferred for
those candidates. Unresolved outcomes remain explicitly unresolved. The
manifest records per-date, per-game, market/model-arm, outcome, quote, and
exclusion counts plus output hashes and source-path/hash references.

## Package counts

- Candidate predictions: 111,676; unique candidate keys: 111,676.
- Qualified candidate/book quote rows: 207,582.
- Retained normalized quote rows that did not bind to a candidate: 303,702;
  these are retained with price, book, event, timestamps, and source lineage in
  `candidate_quote_coverage.csv`.
- Settled outcomes: 88,429; unresolved outcomes: 23,247.
- Quote rows with EV and edge: 205,446 each.
- Dates: 19; first September 29; last October 17.
- Quote rows: goalie saves 1,144; Moneyline 5,196; Points 29,713; SOG 171,529; Puck Line 0.

The detailed model-arm counts are in `manifest.json`. The population is not an
inventory of every superseded daily capture. SOG cold-start shadow arms have
larger populations than production and should be evaluated separately. The
Oct 16 two-game slate and October 17 late capture are small or operationally
bounded samples, not representative performance samples. This export is for
exploratory filtering and prospective evaluation; it does not represent
executed wagers.
