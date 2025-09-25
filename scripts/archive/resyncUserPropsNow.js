// scripts/resyncUserPropsNow.js
import { upsertUserPropsToTraining } from "../backend/scripts/shared/modelTrainingUtils.js";
await upsertUserPropsToTraining();
