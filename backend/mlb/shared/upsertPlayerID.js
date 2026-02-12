// backend/scripts/shared/upsertPlayerID.js

export async function upsertPlayerID(supabase, { player_name, team }) {
  // Check if player already exists
  const { data: existing, error: fetchError } = await supabase
    .from("player_ids")
    .select("player_id")
    .eq("player_name", player_name)
    .eq("team", team)
    .limit(1);

  if (existing?.length) return existing[0].player_id;

  // Fallback: check model_training_props
  const { data: mtp, error: mtpError } = await supabase
    .from("model_training_props")
    .select("player_id")
    .eq("player_name", player_name)
    .eq("team", team)
    .limit(1);

  if (!mtp?.length) {
    console.warn(
      `❌ upsertPlayerID: Could not resolve player_id for ${player_name} (${team})`
    );
    return null;
  }

  const player_id = mtp[0].player_id;

  // Upsert into player_ids
  const { error: upsertError } = await supabase.from("player_ids").upsert(
    {
      player_name,
      player_id,
      team,
    },
    { onConflict: "player_id" }
  );

  if (upsertError) {
    console.warn(
      `⚠️ upsertPlayerID failed for ${player_name}:`,
      upsertError.message
    );
  } else {
    console.log(`✅ upserted player_id for ${player_name}: ${player_id}`);
  }

  return player_id;
}
