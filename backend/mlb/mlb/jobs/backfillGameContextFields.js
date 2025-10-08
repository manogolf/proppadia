export default async function () {
  console.log("🔮 Running backfillGameContextFields...");
  try {
    await import("../../../scripts/backfillGameContextFields.js");
    console.log("✅ backfillGameContextFields complete.");
  } catch (err) {
    console.error("❌ backfillGameContextFields:", err);
    throw err;
  }
}
