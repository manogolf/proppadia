# MLB totals park-depth repair feature contract

| Contract item | Control | Repaired challenger |
|---|---|---|
| Direct location fields | 22 | 21 |
| `park_history_depth` in location | Yes | No |
| `strict_prior_total_run_factor` in location | Yes | Yes |
| Depth used upstream for park confidence | `w=n/(n+50)` | Unchanged `w=n/(n+50)` |
| All other location fields/order | Frozen control order | Identical order with only depth removed |
| Retained-field preprocessing | Frozen development scaler | Exact same means/scales |

The repaired scorer consumes only the artifact's 21-field `feature_order`; raw depth is not an input and cannot be silently reintroduced. The unchanged feature builder/live bridge continue using depth solely to compute the governed park shrinkage and fallback state.
