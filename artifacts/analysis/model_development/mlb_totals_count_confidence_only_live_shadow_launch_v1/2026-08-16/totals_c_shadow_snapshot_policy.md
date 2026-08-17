# C shadow snapshot policy

- 05:30 PT is `PRIMARY_SCORE`.
- 08:30, 11:00, 13:00, and 16:30 PT are `SCORE_MISSING` only.
- One canonical C identity per game. A valid primary row is immutable.
- If RAW/context is unavailable at 05:30, the first later valid strict-pregame shared RAW identity may be admitted.
- Post-start and pre-August-17 construction fail closed. Snapshot selection never uses outcomes or forecast quality.
