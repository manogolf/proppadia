# Hits 0.5 probability semantics audit

`SELECTED_SIDE_PROBABILITY_CONTRACT_CONFIRMED`

`backend/mlb/scripts/build_mlb_slate_output.py::build_slate_output` reads `prob_over`, optionally applies the configured calibrator to that Over probability, assigns `p_under = 1 - p_over`, selects Over at `p_over >= 0.5` and Under otherwise, and stores `model_pick_prob = p_over` for Over or `p_under` for Under. Fair odds are computed from the same complementary probabilities before side selection. No threshold other than 0.5 and no post-selection probability transformation is present. Historical semantic model version/hash remains unresolved.
