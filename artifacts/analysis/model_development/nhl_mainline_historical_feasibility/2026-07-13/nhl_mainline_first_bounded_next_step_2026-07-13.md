# First bounded NHL mainline follow-up

## Selection

Certify a full-game moneyline population and neutral outcome spine for canonical seasons `2023` and `2024`.

- Date ranges: `2023-10-10` through `2024-06-24`, and `2024-10-04` through `2025-06-17`.
- Authorities: `nhl.games` for season/game/team/date identity; `nhl.team_game_2023_summary` and `nhl.team_game_2024_summary` for two-team goal outcomes; shot-event period fields only as reconciliation evidence.
- Grain: one game-team side row for evaluation; one game row for neutral outcome truth.
- Expected output: frozen game ledger, home/away score, full-game winner, goal differential, total goals, status/exclusion decisions, duplicate and source-agreement audits.
- Pass: all 2,798 games reconcile to one canonical game, two distinct teams, one score per team, one neutral settlement outcome, canonical season/date agreement, and explicit overtime/shootout scope.
- Fail: any unresolved identity collision, missing team score, ambiguous winner, or silent population narrowing.
- Unlock: one later historical mainline price/timestamp certification against the frozen moneyline spine.
- Unauthorized: odds acquisition, backfill, model training, feature selection, ROI, recommendations, promotion, or restart.
