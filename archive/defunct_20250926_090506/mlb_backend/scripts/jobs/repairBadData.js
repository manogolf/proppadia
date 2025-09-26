export default async function () {
  console.log("🛠️ Running repairBadData...");
  try {
    await import("../repairBadData.js");
    console.log("✅ repairBadData complete.");
  } catch (err) {
    console.error("❌ repairBadData failed:", err);
    throw err;
  }
}
