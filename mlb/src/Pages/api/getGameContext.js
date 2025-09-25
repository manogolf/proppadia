// 📁 File: src/pages/api/getGameContext.js
import { getGameContextFields } from "../../../backend/scripts/shared/mlbApiUtils.js";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { game_id, team } = req.body;

  if (!game_id || !team) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    const context = await getGameContextFields(game_id, team);
    return res.status(200).json({ context });
  } catch (err) {
    console.error("❌ Error fetching game context:", err);
    return res.status(500).json({ error: "Failed to fetch game context" });
  }
}
