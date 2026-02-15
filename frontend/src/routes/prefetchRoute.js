import { loadWithRetry } from "./lazyRetry.js";

export const loadHomeGateway = () =>
  loadWithRetry(() => import("../Pages/HomeGateway.jsx"), "home-gateway");
export const loadMLBHome = () =>
  loadWithRetry(() => import("../Pages/mlb/MLBHome.jsx"), "mlb-home");
export const loadNHLHome = () =>
  loadWithRetry(() => import("../Pages/nhl/NHLHome.jsx"), "nhl-home");
export const loadPlayerPropsPage = () =>
  loadWithRetry(() => import("../Pages/PlayerPropsPage.jsx"), "player-props-page");
export const loadNHLPredictions = () =>
  loadWithRetry(() => import("../Pages/nhl/NHLPredictions.jsx"), "nhl-predictions");
export const loadLoginPage = () =>
  loadWithRetry(() => import("../Pages/Login.jsx"), "login-page");
export const loadOpsPage = () =>
  loadWithRetry(() => import("../Pages/OpsPage.jsx"), "ops-page");
export const loadPlayerTeamBrowser = () =>
  loadWithRetry(() => import("../Pages/PlayerTeamBrowser.jsx"), "player-team-browser");
export const loadPlayerTeamChooser = () =>
  loadWithRetry(() => import("../Pages/PlayerTeamChooser.jsx"), "player-team-chooser");
export const loadModelMetricsDashboard = () =>
  loadWithRetry(() => import("../Pages/ModelMetricsDashboard.jsx"), "model-metrics-dashboard");
export const loadPlayerProfileDashboard = () =>
  loadWithRetry(() => import("../Pages/PlayerProfileDashboard.jsx"), "player-profile-dashboard");
export const loadAccessRequiredPage = () =>
  loadWithRetry(() => import("../Pages/AccessRequiredPage.jsx"), "access-required-page");
export const loadWatchlistPage = () =>
  loadWithRetry(() => import("../Pages/WatchlistPage.jsx"), "watchlist-page");

const routeLoaders = {
  "/": loadHomeGateway,
  "/mlb": loadMLBHome,
  "/nhl": loadNHLHome,
  "/props": loadPlayerPropsPage,
  "/props/v2": loadPlayerPropsPage,
  "/nhl/predictions": loadNHLPredictions,
  "/login": loadLoginPage,
  "/ops": loadOpsPage,
  "/owner": loadOpsPage,
  "/players": loadPlayerTeamChooser,
  "/players/mlb": loadPlayerTeamBrowser,
  "/players/nhl": loadPlayerTeamBrowser,
  "/metrics": loadModelMetricsDashboard,
  "/watchlist": loadWatchlistPage,
};

const prefetched = new Set();

export function prefetchRoute(pathname) {
  const key = String(pathname || "").trim();
  if (!key || prefetched.has(key)) return;
  const loader = routeLoaders[key];
  if (!loader) return;
  prefetched.add(key);
  loader().catch(() => {
    // Best-effort prefetch only; ignore transient failures.
    prefetched.delete(key);
  });
}
