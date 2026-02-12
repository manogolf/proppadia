export default async function () {
  console.log("📊 Running generateDerivedStats...");
  try {
    await import("../generateDerivedStats.js");
    console.log("✅ generateDerivedStats complete.");
  } catch (err) {
    console.error("❌ generateDerivedStats failed:", err);
    throw err;
  }
}
