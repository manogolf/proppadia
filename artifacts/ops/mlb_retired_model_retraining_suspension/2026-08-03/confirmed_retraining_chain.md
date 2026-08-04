# Confirmed retired retraining chain

`com.proppadia.mlb.retrain.weekly` was the only scheduled retired-model training label found. Its actual cadence was Tuesday at 23:05 local (`Weekday=3`), not Wednesday.

The preserved original wrapper executed:

1. retraining prerequisite check;
2. BvP/PvB refresh;
3. broad reconciliation-row build;
4. `make mlb-retrain-broad-reconcile`, calling `backend/mlb/model_trainer.py` over multiple propositions and replacing `MODEL_DIR/latest/{prop}.joblib` plus `MODEL_INDEX.json`;
5. production bundle publication to a mutable `latest.tgz` alias;
6. prod12 weekly phase cycle.

No other LaunchAgent, LaunchDaemon, crontab entry, or installed parent wrapper was found invoking that training target. The installed wrapper now exits 78 before the preserved unreachable chain.
