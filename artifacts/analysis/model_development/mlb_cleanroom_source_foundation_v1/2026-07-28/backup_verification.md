# Supabase Backup Verification

Status: **BLOCKED — NOT VERIFIED**

The local environment has PostgreSQL credentials and a Supabase service secret, but
no Supabase Management API personal access token, authenticated Supabase CLI, or
dashboard session. The Supabase CLI is not installed. PostgreSQL catalog access does
not establish managed-backup state.

Consequently the following remain unknown:

- backup mechanism;
- latest successful backup timestamp;
- project-specific retention period;
- PITR availability and recovery window;
- tested restore procedure.

## Operator action required

Use an account with access to the project:

1. Open Supabase Dashboard → Database → Backups.
2. Record the newest backup whose status is `COMPLETED`, its timestamp and type.
3. Open Point in Time settings and record whether PITR is enabled and its earliest
   and latest recovery points.
4. Record the displayed retention period and restore workflow.

Alternatively, create a read-only Management API personal access token with
`database:read` / `backups_read`, provide `SUPABASE_ACCESS_TOKEN` and `PROJECT_REF`,
then call:

```bash
curl -H "Authorization: Bearer $SUPABASE_ACCESS_TOKEN" \
  "https://api.supabase.com/v1/projects/$PROJECT_REF/database/backups"
```

No restore request should be sent during verification.
