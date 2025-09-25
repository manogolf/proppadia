// 📄 scripts/runBackfillAllBuckets.js

import { exec } from "child_process";

const totalBuckets = 16;

async function runBucketsSequentially() {
  for (let i = 1; i <= totalBuckets; i++) {
    console.log(`\n⏳ Starting bucket ${i}/${totalBuckets}...\n`);

    await new Promise((resolve, reject) => {
      const cmd = `node scripts/backfillTrainingFieldsExtended.js --bucket=${i}/${totalBuckets}`;
      const proc = exec(cmd, { maxBuffer: 1024 * 1024 * 10 });

      proc.stdout.on("data", (data) => process.stdout.write(data));
      proc.stderr.on("data", (data) => process.stderr.write(data));
      proc.on("exit", (code) => {
        console.log(`✅ Bucket ${i} complete with exit code ${code}`);
        resolve();
      });
      proc.on("error", reject);
    });
  }

  console.log("\n🎉 All buckets completed!\n");
}

runBucketsSequentially().catch((err) => {
  console.error("❌ Error during sequential bucket run:", err);
});
