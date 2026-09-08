# NHL season-2026 preseason readiness integrity checkpoint

Assessment date: 2026-09-08  
Task: `NHL_SEASON_2026_PRESEASON_READINESS_INTEGRITY_CHECKPOINT_V1`

## Decision

All bounded NHL readiness changes are identifiable and selectively committable. Historical Moneyline and SOG scoring parity remains exact within the certified `1e-12` tolerance; Points fixed-input output remains byte-identical. All targeted isolation, hostile, reconciliation, drift-rejection, immutability, syntax/import, and command-surface checks pass. Local commit is authorized; remote push and live burn-in are not.

## Diff findings

- Moneyline now fails closed when game type is unknown and suppresses numeric outcome targets for preseason/non-regular-season rows.
- SOG grading isolates preseason/non-regular-season results and leaves nonparticipants ungraded.
- Points adds immutable quote/input/run lineage, a material ladder-coherence gate, and P/M-only shadow behavior. Effective candidate policy remains unresolved, so C/U/E are structurally disabled.
- Morning orchestration gains Points prerequisites and frozen game-spine hashing; the 07:30 LaunchAgent is unchanged.
- Frozen probabilities, fitted parameters, model/scorer files, strict-prior timing, Goalie Saves status, MLB code, and MLB schedules are unchanged.

## Runtime identity disposition

The edited integration cores have new bytes. Neither core was protected by a current self-hash pin: Moneyline enforces the separate frozen-parameter hash, and SOG enforces the separate frozen-scorer and parity-summary hashes. Those enforced hashes remain exact. The dated SOG implementation package records an older integration-core hash and remains valid immutable historical evidence; it was not rewritten. The companion operational-code-identity amendment preserves both former identities and explicitly binds the current repaired cores.

## Verification

- Moneyline parity: 2,798 rows, maximum delta `1.16573417585641e-15`, zero mismatches.
- SOG parity: 40,167 rows / 13,389 player-games, maximum delta `5.828670879282072e-16`, zero material or side mismatches.
- SOG shadow/isolation: 53 checks passed.
- Points: 24/24 tests passed, including fixed-input byte parity and ladder-gate parity.
- Three-lane: 24/24 hostile tests, 14/14 reconciliation checks, and 10/10 command/regression checks passed.
- Runtime drift rejection: 2/2 passed. Package/parent immutability: 19/19 passed.
- Syntax/import validation: 7 modules passed. `git diff --check` passed.
- Eight manifest-complete same-day readiness packages verified; the incomplete attempt remains visibly incomplete and excluded.

## Lane state

- Moneyline: authorized preseason shadow scope.
- SOG: authorized preseason scope.
- Points: P/M shadow-only with ladder gate; C/U/E disabled.
- Goalie Saves: disabled / not ready.

## Boundary

No live slate was contacted, no market was captured, no model was retrained, no policy changed, no production schedule changed, and no remote push was performed. The next task remains `FIRST_REAL_NHL_SEASON_2026_PRESEASON_MULTI_RUN_BURN_IN`, and it must wait for the first real nonempty preseason slate.
