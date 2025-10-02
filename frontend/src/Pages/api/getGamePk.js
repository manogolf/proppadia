// 📁 File: src/pages/api/getGamePk.js
import { getGamePkForTeamOnDate } from "../../../backend/scripts/shared/fetchGameID.jsx";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { team, game_date } = req.body;

  if (!team || !game_date) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    const gamePk = await getGamePkForTeamOnDate(team, game_date);
    return res.status(200).json({ gamePk });
  } catch (err) {
    console.error("❌ Error resolving gamePk:", err);
    return res.status(500).json({ error: "Failed to resolve gamePk" });
  }
}
