# C point/distribution product contract

```
EXPECTED_TOTAL_RUNS=NEGATIVE_BINOMIAL_MEAN
CENTRAL_TYPICAL_TOTAL=NEGATIVE_BINOMIAL_MEDIAN
MAE_OPTIMAL_POINT=NEGATIVE_BINOMIAL_MEDIAN
PROBABILITY_FOUNDATION=FULL_NEGATIVE_BINOMIAL_DISTRIBUTION
```

The mean is the deterministic `exp(intercept + standardized_features @ coefficients)` location. The median is the first integer whose frozen-support CDF reaches 0.5. Probabilities use the full frozen negative-binomial mass with the 30-plus tail folded into 30. Repeated scoring is deterministic; point summaries do not change the probability distribution.
