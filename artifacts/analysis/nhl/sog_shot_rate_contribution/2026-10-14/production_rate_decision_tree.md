# Production SOG rate decision tree

For each row the scorer converts each input to numeric, treats non-finite values as missing, then selects the first nonmissing value:

1. `d10_sog_per60`
2. else `d20_sog_per60`
3. else `d5_sog_per60`
4. else unscored for missing rate

The selected rate is accepted when >= 0. Exposure independently coalesces D10 TOI, D20 TOI, D5 TOI, season 5v5+PP minutes, then season 5v5+5v4 seconds converted to minutes. Lambda = selected rate × selected exposure minutes / 60. Negative rate/exposure or non-finite lambda is unscored. There is no rate cap, floor, smoothing, rate transform, season SOG rate, attempts multiplier, team/opponent adjustment, or PP-role multiplier in the scorer. Upstream rolling rates are total prior-window SOG divided by total prior-window TOI × 60; window frames include up to 5/10/20 prior player-game rows; zero/nonpositive denominator yields NULL. No minimum count gate is applied by the scorer.
