//export default async function () {
  console.log("📁 Running fetchHotStreaks...");
  try {
    await import("../../../scripts/fetchHotStreaks.js");
    console.log("✅ fetchHotStreaks complete.");
  } catch (err) {
    console.error("❌ fetchHotStreaks failed:", err);
    throw err;
  }
}
