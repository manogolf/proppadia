# Root Cause Report

The immutable complete population contained 170 exact identities, but the
standard closeout assembled a separate 131-row morning-board surface and the
broad and consensus closeouts each assembled their own selected populations.
The reconciliation candidate union therefore was not derived from the complete
population manifest. Completed rows absent from `reconcile_rows.csv` had no
shared exact-ID fallback in the normal lifecycle; the read-only contract audit
alone joined those 15 official outcomes. This also explains why Ronald Acuña
Jr. could be resolved in the broad closeout while absent from the standard
union.

The repair centralizes settlement in
`backend/mlb/shared/ubo5_tb15_outcome_resolver.py`. Every closeout now preserves
its immutable population membership and resolves outcomes in this order:
market-backed reconciliation, exact `game_pk + batter_mlb_id` player stats,
official final lineup/participation no-action, official pending game, then a
visible technical failure. The complete 170-row manifest now has its own
revisioned audit ledger. No name join or outcome-derived membership is used.
