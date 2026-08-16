# Shadow snapshot policy — defined, not launched

- Use the governed totals lifecycle: `05:30 PRIMARY_SCORE`; later `SCORE_MISSING` only for identities legitimately missing required state at 05:30.
- Keep exactly one canonical shadow prediction per game. Never replace a valid earlier prediction because a later score differs.
- Optional later observations are separate immutable observations and cannot change primary evaluation identity.
- Reject post-start scoring and retrospective construction.
