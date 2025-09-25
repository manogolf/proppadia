export default async function () {
  console.log("⚾️ Running generateBvpStats...");
  try {
    await import("../generateBvpStats.js");
    console.log("✅ generateBvpStats complete.");
  } catch (err) {
    console.error("❌ generateBvpStats failed:", err);
    throw err;
  }
}
