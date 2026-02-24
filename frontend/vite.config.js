import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

function vendorChunkName(id) {
  const normalized = String(id || "").replace(/\\/g, "/");
  if (!normalized.includes("/node_modules/")) return null;

  if (
    normalized.includes("/node_modules/react/") ||
    normalized.includes("/node_modules/react-dom/") ||
    normalized.includes("/node_modules/scheduler/")
  ) {
    return "vendor-react";
  }

  if (
    normalized.includes("/node_modules/react-router/") ||
    normalized.includes("/node_modules/react-router-dom/") ||
    normalized.includes("/node_modules/@remix-run/router/")
  ) {
    return "vendor-router";
  }

  if (
    normalized.includes("/node_modules/recharts/") ||
    normalized.includes("/node_modules/d3-") ||
    normalized.includes("/node_modules/internmap/") ||
    normalized.includes("/node_modules/decimal.js-light/")
  ) {
    return "vendor-charts";
  }

  if (
    normalized.includes("/node_modules/@supabase/") ||
    normalized.includes("/node_modules/@babel/runtime/")
  ) {
    return "vendor-supabase";
  }

  if (
    normalized.includes("/node_modules/react-day-picker/") ||
    normalized.includes("/node_modules/date-fns/") ||
    normalized.includes("/node_modules/luxon/")
  ) {
    return "vendor-dates";
  }

  return "vendor-misc";
}

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8001",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    rollupOptions: {
      output: {
        // Prevent Rollup from auto-hoisting dependencies of a manually chunked
        // module into that chunk. We want explicit vendor grouping so React
        // stays in the React chunk and doesn't get pulled into vendor-charts.
        onlyExplicitManualChunks: true,
        manualChunks(id) {
          return vendorChunkName(id);
        },
      },
    },
  },
});
