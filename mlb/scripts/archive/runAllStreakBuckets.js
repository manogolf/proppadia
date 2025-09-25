// ==========================================
// 📄 File: scripts/runAllStreakBuckets.js
// 📌 Purpose: Run all buckets of generatePlayerStreakProfiles.js sequentially
// ==========================================

import { execSync } from "child_process";

const totalBuckets = 16;

for (let i = 1; i <= totalBuckets; i++) {
  console.log(`\n🟢 Running bucket ${i}/${totalBuckets}...`);
  try {
    execSync(
      `node backend/scripts/generatePlayerStreakProfiles.js --bucket=${i}/${totalBuckets}`,
      {
        stdio: "inherit", // stream output live
      }
    );
  } catch (err) {
    console.error(`❌ Bucket ${i} failed:`, err.message || err);
  }
}
