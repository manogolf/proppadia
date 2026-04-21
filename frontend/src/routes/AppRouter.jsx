import { lazy, Suspense, useCallback, useEffect, useState } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  useLocation,
  NavLink,
} from "react-router-dom";
import Header from "../components/Header.jsx";
import RouteErrorBoundary from "../components/RouteErrorBoundary.jsx";
import { useAuth } from "../context/AuthContext.jsx";
import { isOpsUser, isUserPredictionRoute } from "../shared/opsAccess.js";
import {
  getWatchlistTotal,
  WATCHLIST_UPDATED_EVENT,
} from "../shared/watchlistStorage.js";
import AccessRequiredPage from "../Pages/AccessRequiredPage.jsx";
import Home from "../Pages/Home.jsx";
import LoginPage from "../Pages/Login.jsx";
import ModelMetricsDashboard from "../Pages/ModelMetricsDashboard.jsx";
import PlayerProfileDashboard from "../Pages/PlayerProfileDashboard.jsx";
import PlayerPropsPage from "../Pages/PlayerPropsPage.jsx";
import PlayerTeamBrowser from "../Pages/PlayerTeamBrowser.jsx";
import PlayerTeamChooser from "../Pages/PlayerTeamChooser.jsx";
import WatchlistPage from "../Pages/WatchlistPage.jsx";
import MLBHome from "../Pages/mlb/MLBHome.jsx";
import MLBTodayWorkspacePage from "../Pages/mlb/MLBTodayWorkspacePage.jsx";
import NHLHome from "../Pages/nhl/NHLHome.jsx";
import NHLPredictions from "../Pages/nhl/NHLPredictions.jsx";
import NHLPlayerPropsPage from "../Pages/nhl/NHLPlayerPropsPage.jsx";

const OpsPage = lazy(() => import("../Pages/OpsPage.jsx"));

function RequireSignedIn({ children, requiredPath, requiredLabel }) {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <div className="min-h-screen pp-page flex items-center justify-center text-slate-600">
        Checking authentication...
      </div>
    );
  }

  if (!user) {
    return (
      <AccessRequiredPage
        requiredPath={requiredPath || location.pathname}
        requiredLabel={requiredLabel || "member-only features"}
      />
    );
  }
  return children;
}

function RedirectWithSearch({ pathname }) {
  const location = useLocation();
  return (
    <Navigate
      to={{
        pathname,
        search: location.search || "",
      }}
      replace
    />
  );
}

function OpsAccessRestrictedCard() {
  return (
    <div className="min-h-screen pp-page px-4 py-10">
      <div className="max-w-2xl mx-auto pp-card p-6">
        <h2 className="text-2xl font-semibold text-slate-900">Ops Access Restricted</h2>
        <p className="text-slate-700 mt-2">
          This page is restricted to authorized operations users.
        </p>
      </div>
    </div>
  );
}

function RequireOpsUser({ children }) {
  const { user } = useAuth();
  if (isOpsUser(user)) return children;
  return <OpsAccessRestrictedCard />;
}

function AppShell() {
  const { user } = useAuth();
  const location = useLocation();
  const hasOpsAccess = isOpsUser(user);
  const hideOpsNav = isUserPredictionRoute(location.pathname);
  const [watchlistTotal, setWatchlistTotal] = useState(0);

  const navLinkClass = useCallback(
    ({ isActive }) =>
      `text-xs sm:text-sm font-medium transition ${
        isActive ? "text-slate-900 underline underline-offset-4 decoration-slate-400" : "text-slate-700 hover:text-slate-900"
      }`,
    []
  );

  const refreshWatchlistTotal = useCallback(() => {
    setWatchlistTotal(getWatchlistTotal(user?.id));
  }, [user?.id]);

  useEffect(() => {
    refreshWatchlistTotal();
  }, [refreshWatchlistTotal]);

  useEffect(() => {
    function onStorage(e) {
      if (e?.key && String(e.key).startsWith("proppadia_watchlist_v1:")) {
        refreshWatchlistTotal();
      }
    }
    function onWatchlistUpdated() {
      refreshWatchlistTotal();
    }
    window.addEventListener("storage", onStorage);
    window.addEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener(WATCHLIST_UPDATED_EVENT, onWatchlistUpdated);
    };
  }, [refreshWatchlistTotal]);

  return (
    <>
      <div>
        <Header />

        {/* ✅ This nav bar is global, shown on every page */}
        <nav className="px-4 py-2 mb-0">
          <div className="max-w-6xl mx-auto pp-chip pp-reveal-soft px-3 sm:px-4 py-2 flex flex-wrap justify-center sm:justify-end gap-x-4 sm:gap-x-6 gap-y-1">
            <NavLink to="/" end className={navLinkClass}>
              Home
            </NavLink>
            <NavLink to="/mlb/predictions" className={navLinkClass}>
              MLB Picks
            </NavLink>
            <NavLink to="/mlb/today" className={navLinkClass}>
              MLB Today
            </NavLink>
            <NavLink to="/nhl/predictions" className={navLinkClass}>
              NHL Picks
            </NavLink>
            {user ? (
              <NavLink to="/watchlist" className={navLinkClass}>
                {watchlistTotal > 0 ? `Watchlist (${watchlistTotal})` : "Watchlist"}
              </NavLink>
            ) : null}
            <NavLink to="/players/mlb" className={navLinkClass}>
              MLB Players
            </NavLink>
            <NavLink to="/players/nhl" className={navLinkClass}>
              NHL Players
            </NavLink>
            {!user ? (
              <NavLink to="/login" className={navLinkClass}>
                Login
              </NavLink>
            ) : null}
            {user && hasOpsAccess && !hideOpsNav ? (
              <NavLink to="/ops" className={navLinkClass}>
                Ops
              </NavLink>
            ) : null}
          </div>
        </nav>
      </div>

      {/* Render route-based pages */}
      <RouteErrorBoundary>
        <Routes>
          {/* New multi-sport gateway at "/" */}
          <Route path="/" element={<Home />} />
          <Route path="/mlb/slate" element={<MLBHome />} />
          <Route
            path="/mlb/today"
            element={
              <RequireSignedIn
                requiredPath="/mlb/today"
                requiredLabel="MLB today workspace"
              >
                <MLBTodayWorkspacePage />
              </RequireSignedIn>
            }
          />
          <Route
            path="/mlb/predictions"
            element={
              <RequireSignedIn
                requiredPath="/mlb/predictions"
                requiredLabel="MLB predictions"
              >
                <PlayerPropsPage />
              </RequireSignedIn>
            }
          />
          <Route path="/mlb/players/:playerId" element={<PlayerProfileDashboard />} />
          <Route path="/nhl/slate" element={<NHLHome />} />
          <Route
            path="/nhl/props"
            element={
              <RequireSignedIn
                requiredPath="/nhl/props"
                requiredLabel="NHL predictions"
              >
                <NHLPredictions />
              </RequireSignedIn>
            }
          />
          <Route
            path="/nhl/props-form"
            element={
              <RequireSignedIn
                requiredPath="/nhl/props-form"
                requiredLabel="NHL player props form"
              >
                <NHLPlayerPropsPage />
              </RequireSignedIn>
            }
          />
          <Route
            path="/nhl/predictions"
            element={
              <RequireSignedIn
                requiredPath="/nhl/predictions"
                requiredLabel="NHL predictions"
              >
                <RedirectWithSearch pathname="/nhl/props" />
              </RequireSignedIn>
            }
          />
          <Route path="/nhl/players/:playerId" element={<PlayerProfileDashboard />} />
          {/* Legacy route aliases */}
          <Route path="/mlb" element={<RedirectWithSearch pathname="/mlb/slate" />} />
          <Route path="/nhl" element={<RedirectWithSearch pathname="/nhl/slate" />} />
          <Route
            path="/props"
            element={
              <RequireSignedIn requiredPath="/mlb/predictions" requiredLabel="MLB predictions">
                <RedirectWithSearch pathname="/mlb/predictions" />
              </RequireSignedIn>
            }
          />
          <Route path="/login" element={<LoginPage />} />
          <Route path="/player/:playerId" element={<PlayerProfileDashboard />} />
          <Route path="/metrics" element={<ModelMetricsDashboard />} />
          <Route path="/players" element={<PlayerTeamChooser />} />
          <Route path="/players/mlb" element={<PlayerTeamBrowser forcedSport="mlb" />} />
          <Route path="/players/nhl" element={<PlayerTeamBrowser forcedSport="nhl" />} />
          <Route
            path="/watchlist"
            element={
              <RequireSignedIn requiredPath="/watchlist" requiredLabel="watchlist">
                <WatchlistPage />
              </RequireSignedIn>
            }
          />
          <Route
            path="/props/v2"
            element={
              <RequireSignedIn
                requiredPath="/mlb/predictions"
                requiredLabel="MLB predictions"
              >
                <RedirectWithSearch pathname="/mlb/predictions" />
              </RequireSignedIn>
            }
          />
          <Route
            path="/ops"
            element={
              <RequireSignedIn requiredPath="/ops" requiredLabel="operations dashboard">
                <RequireOpsUser>
                  <Suspense
                    fallback={
                      <div className="min-h-screen pp-page flex items-center justify-center text-slate-600">
                        Loading operations dashboard...
                      </div>
                    }
                  >
                    <OpsPage />
                  </Suspense>
                </RequireOpsUser>
              </RequireSignedIn>
            }
          />
          <Route path="/owner" element={<Navigate to="/ops" replace />} />
        </Routes>
      </RouteErrorBoundary>
    </>
  );
}

export default function AppRouter() {
  return (
    <BrowserRouter>
      <AppShell />
    </BrowserRouter>
  );
}
