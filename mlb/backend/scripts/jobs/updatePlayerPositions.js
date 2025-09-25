export default async function () {
  console.log("🧭 Running updatePlayerPositions...");
  try {
    await import("../updatePlayerPositions.js");
    console.log("✅ updatePlayerPositions complete.");
  } catch (err) {
    console.error("❌ updatePlayerPositions failed:", err);
    throw err;
  }
}
