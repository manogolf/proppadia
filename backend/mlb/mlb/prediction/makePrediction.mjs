import path from "path";
import { fileURLToPath } from "url";
import { PythonShell } from "python-shell";

function extractFeaturesOnly(preparedData) {
  const {
    is_home,
    opponent_encoded,
    game_day_of_week,
    time_of_day_bucket,
    starting_pitcher_id,
    rolling_result_avg_7,
    hit_streak,
    win_streak,
    line_diff,
  } = preparedData;

  return {
    is_home,
    opponent_encoded,
    game_day_of_week,
    time_of_day_bucket,
    starting_pitcher_id,
    rolling_result_avg_7,
    hit_streak,
    win_streak,
    line_diff,
  };
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export default async function makePrediction(preparedData) {
  const { prop_type } = preparedData;
  const modelDir = path.resolve(__dirname, `../../models/${prop_type}`);
  const rfModelPath = path.join(modelDir, `${prop_type}_random_forest.pkl`);
  const lrModelPath = path.join(
    modelDir,
    `${prop_type}_logistic_regression.pkl`
  );
  const scriptPath = path.resolve(
    __dirname,
    "../../scripts/prediction/predict_single_prop.py"
  );

  const options = {
    mode: "json",
    pythonOptions: ["-u"],
    scriptPath: null, // optional when using absolute `scriptPath`
    args: [
      JSON.stringify({
        prop_type: preparedData.prop_type,
        features: extractFeaturesOnly(preparedData),
      }),
      rfModelPath,
      lrModelPath,
    ],
  };

  return new Promise((resolve, reject) => {
    PythonShell.run(scriptPath, options)
      .then((results) => {
        if (!results || results.length === 0) {
          return reject(
            new Error("No results returned from prediction script.")
          );
        }

        const result = results[0];
        if (result.error) {
          return reject(new Error(result.error));
        }

        console.log("📈 Prediction result:", result);
        resolve(result);
      })
      .catch((err) => {
        console.error("🐍 PythonShell error:", err);
        reject(new Error("Prediction script failed."));
      });
  });
}
