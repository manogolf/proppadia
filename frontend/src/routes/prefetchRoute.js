export const loadHomeGateway = () =>
  import("../Pages/HomeGateway.jsx");
export const loadMLBHome = () =>
  import("../Pages/mlb/MLBHome.jsx");
export const loadNHLHome = () =>
  import("../Pages/nhl/NHLHome.jsx");
export const loadPlayerPropsPage = () =>
  import("../Pages/PlayerPropsPage.jsx");
export const loadNHLPredictions = () =>
  import("../Pages/nhl/NHLPredictions.jsx");
export const loadLoginPage = () =>
  import("../Pages/Login.jsx");
export const loadOpsPage = () =>
  import("../Pages/OpsPage.jsx");
export const loadPlayerTeamBrowser = () =>
  import("../Pages/PlayerTeamBrowser.jsx");
export const loadPlayerTeamChooser = () =>
  import("../Pages/PlayerTeamChooser.jsx");
export const loadModelMetricsDashboard = () =>
  import("../Pages/ModelMetricsDashboard.jsx");
export const loadPlayerProfileDashboard = () =>
  import("../Pages/PlayerProfileDashboard.jsx");
export const loadAccessRequiredPage = () =>
  import("../Pages/AccessRequiredPage.jsx");
export const loadWatchlistPage = () =>
  import("../Pages/WatchlistPage.jsx");

const routeLoaders = {
  "/": loadHomeGateway,
  "/mlb/slate": loadMLBHome,
  "/mlb/predictions": loadPlayerPropsPage,
  "/nhl/slate": loadNHLHome,
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

const dynamicRouteLoaders = [
  {
    pattern: "/mlb/players/:playerId",
    test: (path) => path.startsWith("/mlb/players/"),
    loader: loadPlayerProfileDashboard,
  },
  {
    pattern: "/nhl/players/:playerId",
    test: (path) => path.startsWith("/nhl/players/"),
    loader: loadPlayerProfileDashboard,
  },
  {
    pattern: "/player/:playerId",
    test: (path) => path.startsWith("/player/"),
    loader: loadPlayerProfileDashboard,
  },
];

const prefetched = new Set();

export function prefetchRoute(pathname) {
  const key = String(pathname || "").trim();
  if (!key || prefetched.has(key)) return;
  const staticLoader = routeLoaders[key];
  const dynamicMatch = !staticLoader
    ? dynamicRouteLoaders.find((entry) => entry.test(key))
    : null;
  const loader = staticLoader || dynamicMatch?.loader;
  if (!loader) return;
  const prefetchKey = dynamicMatch ? dynamicMatch.pattern : key;
  if (prefetched.has(prefetchKey)) return;
  prefetched.add(prefetchKey);
  loader().catch(() => {
    // Best-effort prefetch only; ignore transient failures.
    prefetched.delete(prefetchKey);
  });
}
