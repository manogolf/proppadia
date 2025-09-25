export async function getPlayerID(playerName, gameId) {
  if (!playerName || !gameId) {
    console.error("❌ getPlayerID requires both playerName and gameId.");
    return null;
  }

  const boxscoreUrl = `https://statsapi.mlb.com/api/v1/game/${gameId}/boxscore`;
  console.log(`📡 Fetching boxscore from: ${boxscoreUrl}`);

  try {
    const res = await fetch(boxscoreUrl);
    if (!res.ok) {
      console.error(`❌ Failed to fetch boxscore: HTTP ${res.status}`);
      return null;
    }

    const data = await res.json();
    const normalize = (name) =>
      name
        .toLowerCase()
        .replace(/[\.\,]/g, "")
        .trim();

    const normalizedTarget = normalize(playerName);
    const homePlayers = data.teams?.home?.players || {};
    const awayPlayers = data.teams?.away?.players || {};
    const allPlayers = { ...homePlayers, ...awayPlayers };

    // 🎯 Exact Match First
    for (const [key, player] of Object.entries(allPlayers)) {
      const fullName = player?.person?.fullName || "";
      if (normalize(fullName) === normalizedTarget) {
        console.log(
          `✅ Found exact match for ${playerName}: ${player.person.id}`
        );
        return player.person.id;
      }
    }

    // 🤔 Fuzzy Match Attempt (if exact match fails)
    for (const [key, player] of Object.entries(allPlayers)) {
      const fullName = player?.person?.fullName || "";
      if (
        normalize(fullName).includes(normalizedTarget) ||
        normalizedTarget.includes(normalize(fullName))
      ) {
        console.warn(
          `⚠️ Fuzzy match found for ${playerName}: ${fullName} (ID: ${player.person.id})`
        );
        return player.person.id;
      }
    }

    console.warn(`❌ Player ID not found for ${playerName} in game ${gameId}.`);
    return null;
  } catch (err) {
    console.error(
      `🔥 Error fetching player ID for ${playerName}:`,
      err.message
    );
    return null;
  }
}
