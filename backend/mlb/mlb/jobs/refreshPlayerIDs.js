export default async function () {
  console.log("🆔 Running refreshPlayerIDs...");
  try {
    await import("../refreshPlayerIDs.js");
    console.log("✅ refreshPlayerIDs complete.");
  } catch (err) {
    console.error("❌ refreshPlayerIDs failed:", err);
    throw err;
  }
}
