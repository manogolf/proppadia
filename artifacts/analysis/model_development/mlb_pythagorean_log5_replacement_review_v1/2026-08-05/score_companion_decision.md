# Score companion decision

SCORE_COMPANION_DECISION = **NO_QUALIFIED_SCORE_COMPANION**

Only the accepted benchmark was inspected. On frozen validation, OFFENSE_DEFENSE_POISSON had total MAE 3.641629 versus 3.626983 for LEAGUE_SCORE_CONTROL: it did not improve the static control. The accepted package also contains no run-margin MAE, run-line calibration, bounded-overdispersion validation, or deterministic prospective score-distribution export. The league-score control is not promoted merely because an older dormant integration used it.

Expected scores, totals, margins, and standard ±1.5 probabilities therefore remain explicit nulls with `score_prediction_status = UNAVAILABLE_NO_QUALIFIED_SCORE_MODEL`.
