import { supabase } from "../shared/supabaseBackend.js";

async function syncTrainingData() {
  try {
    console.log("🚀 Starting training data sync...");

    const { data: resolvedProps, error } = await supabase
      .from("player_props")
      .select("*")
      .in("status", ["win", "loss", "push"]) // ✅ safer than just "resolved"
      .not("predicted_outcome", "is", null);

    if (error) throw new Error(`Error fetching props: ${error.message}`);

    for (const prop of resolvedProps) {
      const insertPayload = {
        id: prop.id,
        player_name: prop.player_name,
        team: prop.team,
        position: prop.position,
        prop_type: prop.prop_type,
        prop_value: prop.prop_value,
        result: prop.result,
        outcome: prop.outcome,
        is_pitcher: prop.is_pitcher,
        game_date: prop.game_date,
        game_id: prop.game_id,
        over_under: prop.over_under,
        prop_source: "user_added", // ✅ Matches training/metrics logic
        player_id: prop.player_id ?? null,
        rolling_result_avg_7: prop.rolling_result_avg_7 ?? null,
        line_diff:
          prop.rolling_result_avg_7 !== null && prop.prop_value !== null
            ? parseFloat(
                (prop.rolling_result_avg_7 - prop.prop_value).toFixed(2)
              )
            : null,
        hit_streak: prop.hit_streak ?? null,
        win_streak: prop.win_streak ?? null,

        // ✅ Prediction fields
        predicted_outcome: prop.predicted_outcome ?? null,
        confidence_score: prop.confidence_score ?? null,
        prediction_timestamp: prop.prediction_timestamp ?? null,
        was_correct:
          prop.predicted_outcome && prop.outcome
            ? prop.predicted_outcome === prop.outcome
            : null,
      };

      const { error: insertError } = await supabase
        .from("model_training_props")
        .upsert(insertPayload, {
          onConflict: ["player_id", "game_id", "prop_type"],
        });

      if (insertError) {
        console.warn(
          `⚠️ Failed to upsert prop ID ${prop.id}: ${insertError.message}`
        );
      } else {
        console.log(`✅ Synced prop ID ${prop.id}`);
      }
    }

    console.log("🎉 Training data sync complete!");
  } catch (err) {
    console.error(`❌ Sync failed: ${err.message}`);
  }
}

syncTrainingData();
