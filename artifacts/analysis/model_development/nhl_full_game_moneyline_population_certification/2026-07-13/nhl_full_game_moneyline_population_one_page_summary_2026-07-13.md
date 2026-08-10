# NHL full-game moneyline population — one-page summary

Certified 2,798 outcome-only game rows: 1,400 in season `2023` and 1,398 in season `2024`, with no exclusions. Game identity comes from `nhl.games`; scores come from raw shot-stage score state plus goal events. Team summaries validate two-team grain but their all-zero goal field is rejected as score evidence. Eighty-two Utah rows have a visible missing-date metadata gap.

Every row has a decisive final score and HOME_WIN/AWAY_WIN target. Extra time is classified as regulation, overtime, or shootout. One erroneous winner flag for game `2024020002` remains visible and is resolved by the decisive NJD 3–1 BUF event score.

Exactly one next task is unlocked: bounded strict-prior team/goalie feature-spine construction on the frozen population. Historical prices, a baseline, training, ROI, production changes, and restart remain blocked.
