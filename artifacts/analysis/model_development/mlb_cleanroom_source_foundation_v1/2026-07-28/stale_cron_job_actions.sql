-- PLAN ONLY. NOT EXECUTED: managed backup state is unverified.
-- Jobs 1-9 may be disabled only after backup verification and a fresh dependency check.
SELECT cron.alter_job(1, active := false);
SELECT cron.alter_job(2, active := false);
SELECT cron.alter_job(3, active := false);
SELECT cron.alter_job(4, active := false);
SELECT cron.alter_job(5, active := false);
SELECT cron.alter_job(6, active := false);
SELECT cron.alter_job(7, active := false);
SELECT cron.alter_job(8, active := false);
SELECT cron.alter_job(9, active := false);

-- Exact restoration commands:
-- SELECT cron.alter_job(1, active := true);
-- SELECT cron.alter_job(2, active := true);
-- SELECT cron.alter_job(3, active := true);
-- SELECT cron.alter_job(4, active := true);
-- SELECT cron.alter_job(5, active := true);
-- SELECT cron.alter_job(6, active := true);
-- SELECT cron.alter_job(7, active := true);
-- SELECT cron.alter_job(8, active := true);
-- SELECT cron.alter_job(9, active := true);
-- Job 10 (ANALYZE) is not stale and must remain active.
