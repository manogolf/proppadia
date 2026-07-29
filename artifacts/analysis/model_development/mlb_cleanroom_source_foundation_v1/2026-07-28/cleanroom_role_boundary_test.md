# Clean-room Role Boundary Test

Status: **PASS**

Role: `mlb_cleanroom_research` (`NOLOGIN`, `NOINHERIT`)

- `SET LOCAL ROLE mlb_cleanroom_research`
- `SELECT count(*) FROM mlb_cleanroom_v1.current_games`: PASS, 16 rows
- `SELECT count(*) FROM mlb.model_training_props`: correctly failed with
  `permission denied for schema mlb`

The role can read the clean-room schema and cannot read the tested inherited derived
object. Active production was not repointed to this role.
