import {
  fetchResolvedProps,
  getPendingProps,
  syncTrainingData,
  expireOldPendingProps,
} from "../../src/utils/supabaseUtils.js";

async function runTests() {
  console.log("📥 Testing fetchResolvedProps...");
  const resolved = await fetchResolvedProps();
  console.log(`Found ${resolved.length} resolved props.`);

  console.log("📥 Testing getPendingProps...");
  const pending = await getPendingProps();
  console.log(`Found ${pending.length} pending props.`);

  console.log("🧹 Testing expireOldPendingProps...");
  await expireOldPendingProps();

  console.log("🔄 Testing syncTrainingData...");
  await syncTrainingData();

  console.log("✅ All Supabase utility tests complete.");
}

runTests();
