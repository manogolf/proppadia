# Leakage-safe baseline contracts

These contracts were fixed before inspecting comparative performance.

## Baseline A — prior league scoring mean

The first forecast starts from the frozen pre-August-6 dynamic league state: 1720 completed 2026 games and 8.973837209302 runs/game. Each date uses that strict-prior mean; only after the date is scored are all retained official final MLB games from that date appended. Excluded prediction identities therefore still update the next day's baseball baseline.

## Baseline B — simple team-shrunk scoring baseline

For each game: `0.5 * (home_offense + away_offense + home_prevention + away_prevention)`. These four values are the frozen model context's strict-prior, governed team offense/prevention states, already shrunk by their source procedure. No coefficient or shrinkage parameter was fit here.

For comparative CRPS only, both baselines use the already frozen model dispersion contract (`alpha=0.129444799770130`). No baseline parameter was tuned.
