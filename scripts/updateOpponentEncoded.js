// scripts/updateOpponentEncoded.js
import { supabase } from "../backend/scripts/shared/supabaseBackend.js";

(async () => {
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 108 })
    .eq("opponent", "LAA")
    .neq("opponent_encoded", 108);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 109 })
    .eq("opponent", "ARI")
    .neq("opponent_encoded", 109);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 110 })
    .eq("opponent", "BAL")
    .neq("opponent_encoded", 110);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 111 })
    .eq("opponent", "BOS")
    .neq("opponent_encoded", 111);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 112 })
    .eq("opponent", "CHC")
    .neq("opponent_encoded", 112);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 113 })
    .eq("opponent", "CIN")
    .neq("opponent_encoded", 113);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 114 })
    .eq("opponent", "CLE")
    .neq("opponent_encoded", 114);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 115 })
    .eq("opponent", "COL")
    .neq("opponent_encoded", 115);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 116 })
    .eq("opponent", "DET")
    .neq("opponent_encoded", 116);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 117 })
    .eq("opponent", "HOU")
    .neq("opponent_encoded", 117);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 118 })
    .eq("opponent", "KC")
    .neq("opponent_encoded", 118);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 119 })
    .eq("opponent", "LAD")
    .neq("opponent_encoded", 119);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 120 })
    .eq("opponent", "WSH")
    .neq("opponent_encoded", 120);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 121 })
    .eq("opponent", "NYM")
    .neq("opponent_encoded", 121);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 133 })
    .eq("opponent", "OAK")
    .neq("opponent_encoded", 133);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 133 })
    .eq("opponent", "ATH")
    .neq("opponent_encoded", 133);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 134 })
    .eq("opponent", "PIT")
    .neq("opponent_encoded", 134);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 135 })
    .eq("opponent", "SD")
    .neq("opponent_encoded", 135);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 136 })
    .eq("opponent", "SEA")
    .neq("opponent_encoded", 136);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 137 })
    .eq("opponent", "SF")
    .neq("opponent_encoded", 137);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 138 })
    .eq("opponent", "STL")
    .neq("opponent_encoded", 138);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 139 })
    .eq("opponent", "TB")
    .neq("opponent_encoded", 139);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 140 })
    .eq("opponent", "TEX")
    .neq("opponent_encoded", 140);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 141 })
    .eq("opponent", "TOR")
    .neq("opponent_encoded", 141);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 142 })
    .eq("opponent", "MIN")
    .neq("opponent_encoded", 142);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 143 })
    .eq("opponent", "PHI")
    .neq("opponent_encoded", 143);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 144 })
    .eq("opponent", "ATL")
    .neq("opponent_encoded", 144);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 145 })
    .eq("opponent", "CWS")
    .neq("opponent_encoded", 145);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 146 })
    .eq("opponent", "MIA")
    .neq("opponent_encoded", 146);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 147 })
    .eq("opponent", "NYY")
    .neq("opponent_encoded", 147);
  await supabase
    .from("model_training_props")
    .update({ opponent_encoded: 158 })
    .eq("opponent", "MIL")
    .neq("opponent_encoded", 158);

  console.log("✅ Opponent encoded values updated");
  process.exit(0);
})();
