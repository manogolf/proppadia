// scripts/archive/updatePlayerIds.js
// ⚠️ DEPRECATED: DO NOT RUN WITHOUT UNDERSTANDING SIDE EFFECTS
// This script refreshes all player IDs from MLB active rosters and upserts into Supabase.
// It is safe **only** if you have verified player_id is your unique identifier.
// Fixes prior bug: previously used (player_name, team) as conflict keys — this caused duplicates.

import { supabase } from "../../backend/scripts/shared/supabaseBackend.js";
import fetch from "node-fetch";

const MLB_TEAMS_API = "https://statsapi.mlb.com/api/v1/teams?sportId=1";

async function fetchActivePlayers() {
  const teamsRes = await fetch(MLB_TEAMS_API);
  const teamsData = await teamsRes.json();

  const teams = teamsData.teams || [];
  const allPlayers = [];

  for (const team of teams) {
    const rosterUrl = `https://statsapi.mlb.com/api/v1/teams/${team.id}/roster/Active`;
    const rosterRes = await fetch(rosterUrl);
    const rosterData = await rosterRes.json();

    (rosterData.roster || []).forEach((player) => {
      allPlayers.push({
        player_name: player.person.fullName,
        team: team.abbreviation,
        player_id: player.person.id,
      });
    });
  }

  return allPlayers;
}

async function updatePlayerIDs() {
  const players = await fetchActivePlayers();

  for (const player of players) {
    const { error } = await supabase
      .from("player_ids")
      .upsert(player, { onConflict: ["player_id"] }); // ✅ FIXED KEY

    if (error) {
      console.error(
        `❌ Failed to upsert ${player.player_name}:`,
        error.message
      );
    } else {
      console.log(`✅ Upserted: ${player.player_name} (${player.team})`);
    }
  }

  console.log("🎉 Player ID sync complete.");
}

updatePlayerIDs();
