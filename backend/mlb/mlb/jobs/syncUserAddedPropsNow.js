export default async function () {
  console.log("🔄 Running syncUserAddedPropsNow...");
  try {
    await import("../../../scripts/syncUserAddedPropsNow.js");
    console.log("✅ syncUserAddedPropsNow complete.");
  } catch (err) {
    console.error("❌ syncUserAddedPropsNow failed:", err);
    throw err;
  }
}
