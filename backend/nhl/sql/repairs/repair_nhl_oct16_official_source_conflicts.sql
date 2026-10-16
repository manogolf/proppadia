-- PREPARED ONLY. Separate authorization is required before execution.
-- Candidate CSV SHA-256: cf663c4ec6fa1a5759b9e450cc00341984d34ff2bd27425c598a677a8a49a048
-- Official-source manifest SHA-256: 628c0d7df5a8a0097b4f463690f8d85469dd60df4863058a1fbd17cd003672be
-- Attempt diagnostic SHA-256: 0a5531dd94f151c6ece2395cc9c16820434a7125a27c8f737c76c6863e8068bd
-- PP fill diagnostic SHA-256: bf3fa0cdb30aa600b2af1f8247e727660665b73b79f48c48176d61cc7cb35647
-- Only 8 shot_attempts and 2 pp_toi_minutes fields are in scope.
-- Official team IDs/abbreviations derive from final NHL boxscores; expected DB team IDs are separately pinned.
-- The five unresolved PP rows are guard-only and never updated.
BEGIN;
SET LOCAL lock_timeout='10s';
CREATE TEMP TABLE oct16_source_candidates(
 statistic text NOT NULL CHECK(statistic IN ('shot_attempts','pp_toi_minutes')),
 game_id bigint NOT NULL, player_id bigint NOT NULL, expected_db_team_id bigint NOT NULL,
 official_team_id bigint NOT NULL, official_team_abbr text NOT NULL, expected_shots_on_goal integer NOT NULL,
 original_value numeric NOT NULL, replacement_value numeric NOT NULL, validation_status text NOT NULL,
 pbp_sha256 text NOT NULL, boxscore_sha256 text NOT NULL, shiftcharts_sha256 text,
 PRIMARY KEY(game_id,player_id), CHECK(player_id>0), CHECK(expected_shots_on_goal>=0), CHECK(original_value<>replacement_value),
 CHECK(pbp_sha256 ~ '^[0-9a-f]{64}$'), CHECK(boxscore_sha256 ~ '^[0-9a-f]{64}$'),
 CHECK(shiftcharts_sha256 IS NULL OR shiftcharts_sha256 ~ '^[0-9a-f]{64}$')
) ON COMMIT DROP;
INSERT INTO oct16_source_candidates VALUES
  ('shot_attempts', 2026020108, 8482803, 7, 7, 'BUF', 6, 9, 8, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', '9d4188300d36e007a09f45c10c6406b4ef705ad20aa99bd44a6e25f821b07c43', 'b6925173bdf23be020831881dc57be98d084bb5e5de13b53ade3e7744685759b', NULL),
  ('shot_attempts', 2026020112, 8484136, 23, 23, 'VAN', 2, 3, 2, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', 'f91c527814246322b1ef42c5d84ceb6229147908cf29f4dee349f0858f370492', '188b8359aba5ba1a3a03b5b60f7b41aac3608bb7307043778312157e3502677c', NULL),
  ('shot_attempts', 2026020112, 8483476, 23, 23, 'VAN', 2, 3, 2, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', 'f91c527814246322b1ef42c5d84ceb6229147908cf29f4dee349f0858f370492', '188b8359aba5ba1a3a03b5b60f7b41aac3608bb7307043778312157e3502677c', NULL),
  ('shot_attempts', 2026020113, 8476468, 3, 3, 'NYR', 0, 2, 1, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', 'eb046ccd208bcae77e326a2592bf904d25fb7ff2f6197b938eda263a520984f5', '5bc12cae60cb441e658ae9d428d9c02703cb2694a582628f14858f59cb0a6f55', NULL),
  ('shot_attempts', 2026020110, 8477429, 17, 17, 'DET', 2, 4, 3, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', '2e23202fa986e3ba29bfa8462414701eee9dcf337edc705370761b4222fdd19e', '17c8d22b6c5eeb73f1b6f15a924b64733390210c7ad09111395c8544f2ca0861', NULL),
  ('shot_attempts', 2026020116, 8478864, 30, 30, 'MIN', 6, 15, 14, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', 'b29c00cc0b684f5d5fc431fca1fceb88c1ef8d9bbc626459f92898921abfc399', 'c63131d2d78a5d08b75a48e1204818bafcd15461e345144e8a9aa69b0fcebb9c', NULL),
  ('shot_attempts', 2026020117, 8477951, 68, 68, 'UTA', 2, 1, 2, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', 'bcf4f4493fb42941eef834bb5629f2f899d8cee9df594ab2e4adc94be1514890', '0de4c08d7bb9df1214d1ae9172b0ac0785e6d6f178166ed7dce6c53e362defe4', NULL),
  ('shot_attempts', 2026020117, 8479977, 68, 68, 'UTA', 4, 6, 5, 'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED', 'bcf4f4493fb42941eef834bb5629f2f899d8cee9df594ab2e4adc94be1514890', '0de4c08d7bb9df1214d1ae9172b0ac0785e6d6f178166ed7dce6c53e362defe4', NULL),
  ('pp_toi_minutes', 2026020111, 8480995, 14, 14, 'TBL', 1, 2.18, 1.6, 'VERIFIED_FINAL_PBP_DEDUPLICATED_SHIFTS_PLAYER_TOI_RECONCILED', 'cb76e4acdae472481516a356d8ebb57ea41536724b3bbde9a0dd1aa8879be0a9', '48a8d04ed76c2d4b77140fd32d73feda4b8db7bd394c70e2db8875a68e5f79e2', 'cd21e5dd4d71edbb74043cc763b39635aaaf9cbea8c5b82032d05c2f1116fea2'),
  ('pp_toi_minutes', 2026020116, 8474567, 30, 30, 'MIN', 0, 1.62, 1.47, 'VERIFIED_FINAL_PBP_DEDUPLICATED_SHIFTS_PLAYER_TOI_RECONCILED', 'b29c00cc0b684f5d5fc431fca1fceb88c1ef8d9bbc626459f92898921abfc399', 'c63131d2d78a5d08b75a48e1204818bafcd15461e345144e8a9aa69b0fcebb9c', 'a5ce463ba0c75d6f263c7adc058c0a7ef445a9a051fdb14f01cdd0a17f11c66f');

CREATE TEMP TABLE oct16_unresolved_pp_guard(
 game_id bigint NOT NULL, player_id bigint NOT NULL, expected_db_team_id bigint NOT NULL,
 expected_pp_toi numeric NOT NULL, PRIMARY KEY(game_id,player_id)
) ON COMMIT DROP;
INSERT INTO oct16_unresolved_pp_guard VALUES
  (2026020114,8476460,52,3.12),
  (2026020114,8486025,52,0.32),
  (2026020116,8481013,19,0.88),
  (2026020116,8481580,19,0.88),
  (2026020118,8475343,54,0.02);
DO $$
DECLARE n integer;
BEGIN
 SELECT count(*) INTO n FROM oct16_source_candidates;
 IF n<>10 OR (SELECT count(*) FROM oct16_source_candidates WHERE statistic='shot_attempts')<>8
   OR (SELECT count(*) FROM oct16_source_candidates WHERE statistic='pp_toi_minutes')<>2
 THEN RAISE EXCEPTION 'CANDIDATE_COUNT_MISMATCH'; END IF;
 IF EXISTS (SELECT 1 FROM oct16_source_candidates c WHERE
   (c.statistic='shot_attempts' AND c.validation_status<>'VERIFIED_FINAL_PBP_AND_PLAYER_SOG_RECONCILED') OR
   (c.statistic='pp_toi_minutes' AND c.validation_status<>'VERIFIED_FINAL_PBP_DEDUPLICATED_SHIFTS_PLAYER_TOI_RECONCILED'))
 THEN RAISE EXCEPTION 'SOURCE_VALIDATION_STATUS_MISMATCH'; END IF;
 IF EXISTS (
  SELECT 1 FROM oct16_source_candidates c LEFT JOIN nhl.skater_game_logs_raw r USING(game_id,player_id)
  WHERE r.player_id IS NULL OR r.team_id IS DISTINCT FROM c.expected_db_team_id
   OR r.shots_on_goal IS DISTINCT FROM c.expected_shots_on_goal
   OR (c.statistic='shot_attempts' AND NOT (r.shot_attempts IS NOT DISTINCT FROM c.original_value::integer OR r.shot_attempts IS NOT DISTINCT FROM c.replacement_value::integer))
   OR (c.statistic='pp_toi_minutes' AND NOT (r.pp_toi_minutes IS NOT DISTINCT FROM c.original_value OR r.pp_toi_minutes IS NOT DISTINCT FROM c.replacement_value))
 ) THEN RAISE EXCEPTION 'CURRENT_IDENTITY_OR_VALUE_MISMATCH'; END IF;
 IF EXISTS (
  SELECT 1 FROM oct16_source_candidates c LEFT JOIN nhl.teams t
    ON t.team_id=c.expected_db_team_id AND upper(t.team)=c.official_team_abbr
  WHERE t.team_id IS NULL OR NOT EXISTS (
    SELECT 1 FROM nhl.team_external_ids e WHERE e.team_id=c.expected_db_team_id
      AND lower(e.provider)='nhl' AND e.provider_team_id::bigint=c.official_team_id)
   OR (SELECT count(DISTINCT e.provider_team_id) FROM nhl.team_external_ids e
       WHERE e.team_id=c.expected_db_team_id AND lower(e.provider)='nhl')<>1
 ) THEN RAISE EXCEPTION 'OFFICIAL_TO_DATABASE_TEAM_CROSSWALK_NOT_UNIQUE'; END IF;
 IF EXISTS (
  SELECT 1 FROM oct16_unresolved_pp_guard u LEFT JOIN nhl.skater_game_logs_raw r USING(game_id,player_id)
  WHERE r.player_id IS NULL OR r.team_id IS DISTINCT FROM u.expected_db_team_id OR r.pp_toi_minutes IS DISTINCT FROM u.expected_pp_toi
 ) THEN RAISE EXCEPTION 'UNRESOLVED_PP_PROTECTED_VALUE_CHANGED'; END IF;
END $$;
DO $$
DECLARE n_attempts integer; n_pp integer;
BEGIN
 UPDATE nhl.skater_game_logs_raw r SET shot_attempts=c.replacement_value::integer
 FROM oct16_source_candidates c WHERE c.statistic='shot_attempts' AND r.game_id=c.game_id AND r.player_id=c.player_id
  AND r.team_id=c.expected_db_team_id AND r.shot_attempts=c.original_value::integer;
 GET DIAGNOSTICS n_attempts=ROW_COUNT;
 UPDATE nhl.skater_game_logs_raw r SET pp_toi_minutes=c.replacement_value
 FROM oct16_source_candidates c WHERE c.statistic='pp_toi_minutes' AND r.game_id=c.game_id AND r.player_id=c.player_id
  AND r.team_id=c.expected_db_team_id AND r.pp_toi_minutes=c.original_value;
 GET DIAGNOSTICS n_pp=ROW_COUNT;
 IF n_attempts>8 OR n_pp>2 THEN RAISE EXCEPTION 'UPDATE_ROW_COUNT_EXCEEDED_SCOPE'; END IF;
 RAISE NOTICE 'OCT16_SOURCE_CORRECTIONS: shot_attempts=%, pp_toi_minutes=%',n_attempts,n_pp;
 IF EXISTS (
  SELECT 1 FROM oct16_source_candidates c JOIN nhl.skater_game_logs_raw r USING(game_id,player_id)
  WHERE r.team_id IS DISTINCT FROM c.expected_db_team_id OR r.shots_on_goal IS DISTINCT FROM c.expected_shots_on_goal OR
   (c.statistic='shot_attempts' AND r.shot_attempts IS DISTINCT FROM c.replacement_value::integer) OR
   (c.statistic='pp_toi_minutes' AND r.pp_toi_minutes IS DISTINCT FROM c.replacement_value)
 ) THEN RAISE EXCEPTION 'POST_UPDATE_VERIFICATION_FAILED'; END IF;
 IF EXISTS (
  SELECT 1 FROM oct16_unresolved_pp_guard u JOIN nhl.skater_game_logs_raw r USING(game_id,player_id)
  WHERE r.team_id IS DISTINCT FROM u.expected_db_team_id OR r.pp_toi_minutes IS DISTINCT FROM u.expected_pp_toi
 ) THEN RAISE EXCEPTION 'UNRESOLVED_PP_CHANGED'; END IF;
END $$;
COMMIT;
