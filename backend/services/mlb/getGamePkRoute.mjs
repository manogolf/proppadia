// backend/services/getGamePkRoute.mjs
import express from "express";
import { getGamePkForTeamOnDate } from "../scripts/shared/fetchGameID.js";

const router = express.Router();

router.post("/getGamePk", async (req, res) => {
  const { team, game_date } = req.body;

  if (!team || !game_date) {
    return res.status(400).json({ error: "Missing team or game_date" });
  }

  try {
    const gameId = await getGamePkForTeamOnDate(team, game_date);
    res.json({ gamePk: gameId });
  } catch (err) {
    console.error("❌ Error resolving gamePk:", err);
    res.status(500).json({ error: "Failed to resolve gamePk" });
  }
});

export default router;
