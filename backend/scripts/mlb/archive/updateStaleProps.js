import "dotenv/config";
import { supabase } from "../utils/supabaseBackend.js";
import { updatePropStatus } from "../../scripts/updatePropResults.js";
import { nowET } from "../utils/timeUtilsBackend";

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

// Re-attempt props still marked as "pending"
async function updateStaleProps() {
  const { data: props, error } = await supabase
    .from("player_props")
    .select("*")
    .eq("status", "pending");

  if (error) {
    console.error("❌ Failed to fetch pending props:", error.message);
    return;
  }

  console.log(`🔄 Reprocessing ${props.length} older pending props...`);

  let updated = 0;
  for (const prop of props) {
    const ok = await updatePropStatus(prop);
    if (ok) updated++;
  }

  console.log(`✅ Reprocessed ${updated} props`);
}

// Mark stale props older than 24 hours as unresolved
async function expireOldProps() {
  const yesterday = nowET().minus({ hours: 24 }).toISODate();

  const { data, error } = await supabase
    .from("player_props")
    .update({ status: "unresolved", outcome: "none" })
    .eq("status", "pending")
    .lt("game_date", yesterday)
    .select();

  if (error) {
    console.error("❌ Failed to expire old props:", error.message);
  } else {
    console.log(`🕓 Expired ${data.length} unresolved props older than 24h`);
  }
}

// Run both steps
await updateStaleProps();
await expireOldProps();
