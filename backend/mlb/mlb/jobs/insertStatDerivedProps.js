export default async function () {
  console.log("📥 Running insertStatDerivedProps...");
  try {
    await import("../insertStatDerivedProps.js");
    console.log("✅ insertStatDerivedProps complete.");
  } catch (err) {
    console.error("❌ insertStatDerivedProps failed:", err);
    throw err;
  }
}
