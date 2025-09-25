import Bree from "bree";
import path from "path";

const bree = new Bree({
  root: path.resolve("./jobs"),
  jobs: [
    {
      name: "predict",
      interval: "at 8:00am", // cron-style: every day at 8 AM local time
      timeout: 0, // run immediately once scheduled time is hit
    },
  ],
});

export default bree;
