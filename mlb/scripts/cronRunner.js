// 📄 File: scripts/cronRunner.js

import "dotenv/config";
import cron from "node-cron";
import { supabase } from "../backend/scripts/shared/supabaseBackend.js";
import { yesterdayET } from "../src/shared/timeUtils.js";
import { updatePropStatusesForRows } from "../backend/scripts/resolution/updatePropResults.js";
import { syncStatsForDate } from "../backend/scripts/resolution/syncPlayerStats.js";
import runBackfillPlayerPositions from "./backfillPlayerPositions.js";

console.log("⏳ Cron runner starting...");

const month = new Date().getUTCMonth();
const inSeason = month >= 2 && month <= 9;
const cronExpression = inSeason ? "*/90 * * * *" : "0 10 * * *";
const isGitHubAction = process.env.GITHUB_ACTIONS === "true";

console.log(
  `📅 Scheduling cron job: ${
    inSeason
      ? "every 90 minutes (in-season)"
      : "daily at 10:00 UTC (off-season)"
  }`
);

// 🧠 Run one full cycle of tasks
const safelyRun = async (label) => {
  try {
    console.log(`🔁 ${label}: Starting scheduled tasks...`);

    // Step 1: Sync stats
    console.log("📊 Syncing stats for yesterday...");
    await syncStatsForDate(yesterdayET());
    console.log("✅ Stats sync complete.");

    // Step 2: Update pending props
    const { data: pendingProps, error } = await supabase
      .from("player_props")
      .select("*")
      .eq("status", "pending")
      .limit(500);

    if (error) {
      console.error(`❌ Failed to fetch pending props: ${error.message}`);
    } else if (pendingProps.length) {
      console.log(`🔧 Resolving ${pendingProps.length} pending props...`);
      await updatePropStatusesForRows(pendingProps);
      console.log("✅ Prop resolution complete.");
    } else {
      console.log("✅ No pending props to resolve.");
    }

    // Step 3: Backfill positions
    await runBackfillPlayerPositions();
  } catch (err) {
    console.error(`❌ Error during cron run: ${err.message}`, err);
  }
};

// Run once immediately
await safelyRun(isGitHubAction ? "GitHub Action" : "Local run");

// Schedule repeated execution if not in GitHub Actions
if (!isGitHubAction) {
  cron.schedule(cronExpression, async () => {
    const now = new Date().toISOString();
    console.log(`🕒 Cron triggered at ${now}`);
    await safelyRun("Scheduled Cron Job");
  });
}
