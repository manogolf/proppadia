// scripts/build-tailwind.js
import { exec } from "child_process";

exec(
  "npx tailwindcss -i ./src/tailwind.css -o ./src/index.css --watch",
  (err, stdout, stderr) => {
    if (err) {
      console.error("❌ Build failed:", err);
      return;
    }
    console.log(stdout);
    console.error(stderr);
  }
);
