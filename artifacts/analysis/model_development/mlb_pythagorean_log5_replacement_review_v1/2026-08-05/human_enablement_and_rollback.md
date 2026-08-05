# Human enablement and rollback plan — not executed

Enablement is not recommended or executed by this review. A later authorized action would set `MLB_PUBLIC_GAME_PREDICTIONS_ENABLED=1`, require winner version `MLB_GAME_PYTHAGOREAN_LOG5_V1` and hash `804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6`, require score companion **absent**, then verify `/api/mlb/game-predictions/status` and `/api/mlb/game-predictions?game_date=<CURRENT_DATE>` on the first live pregame run. Confirm betting and prop authorities remain disabled.

Rollback: set `MLB_PUBLIC_GAME_PREDICTIONS_ENABLED=0` (or remove it), restart the API process if its environment is process-bound, and verify the status endpoint reports disabled with zero public rows. Do not bind the archived baseline as fallback.
