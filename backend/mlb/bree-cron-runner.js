// File: backend/scripts/cron-runner.js
import Bree from "bree";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const bree = new Bree({
  root: path.join(__dirname, "jobs"),
  defaultExtension: "js",
  jobs: [
    { name: "insertStatDerivedProps", interval: "at 4:00am" },
    { name: "generateDerivedStats", interval: "at 4:15am" },
    { name: "generateBvpStats", interval: "at 4:30am" },
    { name: "backfillPredictions", interval: "at 4:45am" },
    { name: "uploadModelMetadata", interval: "at 5:00am" },
    { name: "expireOldPredictions", interval: "at 5:15am" },
    { name: "syncUserAddedPropsNow", interval: "at 5:30am" },
    { name: "refreshPlayerIDs", interval: "at 5:45am" },
    { name: "updatePlayerPositions", interval: "at 6:00am" },
    { name: "repairBadData", interval: "at 6:15am" },
  ],
  errorHandler: (err, job) => {
    console.error(`❌ Job "${job.name}" failed:`, err);
  },
  worker: {
    exitHandler: (code, signal) => {
      if (code !== 0) {
        console.warn(`⚠️ Job exited with code ${code}, signal ${signal}`);
      }
    },
  },
});

bree.start();
