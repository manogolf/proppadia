// scripts/syncUserAddedPropsNow.js
// script syncs user_added from player_props with model_training_props

import { upsertUserPropsToTraining } from "../backend/scripts/shared/modelTrainingUtils.js";

const DEFAULT_BATCH_SIZE = 1000;
const DEFAULT_DAYS_BACK = 3; // ⏱️ Only sync user-added props from last 2 days

await upsertUserPropsToTraining({
  batchSize: DEFAULT_BATCH_SIZE,
  daysBack: DEFAULT_DAYS_BACK,
});
