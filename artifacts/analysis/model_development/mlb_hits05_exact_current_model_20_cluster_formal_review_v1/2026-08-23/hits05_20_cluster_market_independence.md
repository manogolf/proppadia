# Market-independence review

`MARKET_INPUTS_IN_MODEL = NO`

`HITS05_MEANINGFULLY_INDEPENDENT_PREDICTION_OPINION`

The registered model schema and earlier artifact audit identify 73 baseball-only inputs and no odds, price, implied probability, consensus, sportsbook, or market-derived calibration feature. On 862 BetOnline rows synchronized within 30 minutes, Pearson/Spearman probability correlation is 0.3855/0.3836; median absolute separation is 5.56%, with 475 at >=5 percentage points. Proppadia-only/BetOnline-only binary correctness counts are 93/111. This is descriptive independence evidence, not edge.
