export const loadHomeGateway = () => import("../Pages/HomeGateway.jsx");
export const loadMLBHome = () => import("../Pages/mlb/MLBHome.jsx");
export const loadNHLHome = () => import("../Pages/nhl/NHLHome.jsx");
export const loadPropsDashboard = () => import("../Pages/PropsDashboard.jsx");
export const loadPlayerPropsPage = () => import("../Pages/PlayerPropsPage.jsx");
export const loadNHLPredictions = () => import("../Pages/nhl/NHLPredictions.jsx");
export const loadLoginPage = () => import("../Pages/Login.jsx");
export const loadOpsPage = () => import("../Pages/OpsPage.jsx");
export const loadPlayerTeamBrowser = () =>
  import("../Pages/PlayerTeamBrowser.jsx");
export const loadModelMetricsDashboard = () =>
  import("../Pages/ModelMetricsDashboard.jsx");
export const loadPlayerProfileDashboard = () =>
  import("../Pages/PlayerProfileDashboard.jsx");
export const loadAccessRequiredPage = () =>
  import("../Pages/AccessRequiredPage.jsx");
export const loadWatchlistPage = () => import("../Pages/WatchlistPage.jsx");

const routeLoaders = {
  "/": loadHomeGateway,
  "/mlb": loadMLBHome,
  "/nhl": loadNHLHome,
  "/props": loadPropsDashboard,
  "/props/v2": loadPlayerPropsPage,
  "/nhl/predictions": loadNHLPredictions,
  "/login": loadLoginPage,
  "/ops": loadOpsPage,
  "/owner": loadOpsPage,
  "/players": loadPlayerTeamBrowser,
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
