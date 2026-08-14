# Hits 0.5 market-input audit

`MARKET_INPUTS_IN_MODEL = NO`

The frozen artifact exposes 73 ordered inputs. None match market, odds, price, implied probability, consensus, Pinnacle, BetOnline, sportsbook, or movement concepts. Inputs are baseball histories, batter-versus-pitcher state, pitcher results, and missingness indicators. `make_prediction` produces its LR/RF blend and deterministic line transform before market comparison; no market-derived calibration layer is evidenced.

`MODEL_IS_METHODologically_INDEPENDENT_OF_MARKET`

Code independence does not by itself prove statistical independence; the synchronized diagnostics address that separately.
