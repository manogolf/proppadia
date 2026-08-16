# Negative-binomial parameterization

Both subjects use `REGULARIZED_POISSON_LOCATION_WITH_NEGATIVE_BINOMIAL_DISTRIBUTION` and differ through frozen fitted predictor structure and their frozen dispersion estimates; neither was refit here.

- CONTROL `fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac`: alpha `0.12944479977012996`, `22` location inputs.
- C `21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd`: alpha `0.12988346817423194`, `19` location inputs.

For each row, `mu = exp(beta_0 + z beta)`. With `alpha > 0`, code sets `size = 1/alpha` and `p = size/(size+mu)`. Under SciPy's failures-before-success parameterization:

`P(Y=k) = Gamma(k+size)/(Gamma(size) Gamma(k+1)) * p^size * (1-p)^k`, for nonnegative integer `k`.

The theoretical mean/location is `E[Y]=mu`; variance is `mu + alpha*mu^2`. PMF support is evaluated at 0..30 and remaining mass above 30 is folded into 30. Median is exact first CDF index at or above 0.5; mode is exact PMF argmax on the governed support. Thus the models share the exact family/PMF contract and differ only through fitted predictor feature structure, coefficients/scaling, and the frozen alpha resulting from their original development fit.
