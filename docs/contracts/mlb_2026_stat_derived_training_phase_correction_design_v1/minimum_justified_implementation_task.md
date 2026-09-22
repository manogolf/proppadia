# Minimum Justified Implementation Task

Implement and test only the backend-neutral `MLB_REGULAR_SEASON_TRAINING_ELIGIBILITY_V1` helper, then integrate it into a dry-run adapter for `backend/mlb/model_trainer.py` without training or writing artifacts.

The bounded task should:

1. reuse `CanonicalGamePhaseAuthority` rather than create another raw-type mapping;
2. cover all trainer input modes (`reconcile_csv`, `base_merge`, and configured view);
3. emit a deterministic gate report and an admitted-row manifest;
4. compare against the frozen 600,766-row population and require exactly 459,604 admitted regular rows plus 141,162 excluded preseason rows;
5. prove stable row order and unchanged feature/target hashes for the admitted intersection;
6. fail closed on every unresolved authority condition;
7. make no database writes, training calls, model writes, result restatements, or consumer cutovers beyond this one dry-run adapter.

Only after that evidence is reviewed should a selector cutover be authorized.
