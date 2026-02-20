import { useCallback, useEffect, useState } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  useLocation,
} from "react-router-dom";
import Header from "../components/Header.jsx";
import RouteErrorBoundary from "../components/RouteErrorBoundary.jsx";
import { useAuth } from "../context/AuthContext.jsx";
import { isOpsUser } from "../shared/opsAccess.js";
import {
  getWatchlistTotal,
  WATCHLIST_UPDATED_EVENT,
} from "../shared/watchlistStorage.js";
import AccessRequiredPage from "../Pages/AccessRequiredPage.jsx";
import HomeGateway from "../Pages/HomeGateway.jsx";
import LoginPage from "../Pages/Login.jsx";
import ModelMetricsDashboard from "../Pages/ModelMetricsDashboard.jsx";
import OpsPage from "../Pages/OpsPage.jsx";
import PlayerProfileDashboard from "../Pages/PlayerProfileDashboard.jsx";
import PlayerPropsPage from "../Pages/PlayerPropsPage.jsx";
import PlayerTeamBrowser from "../Pages/PlayerTeamBrowser.jsx";
import PlayerTeamChooser from "../Pages/PlayerTeamChooser.jsx";
import WatchlistPage from "../Pages/WatchlistPage.jsx";
import MLBHome from "../Pages/mlb/MLBHome.jsx";
import NHLHome from "../Pages/nhl/NHLHome.jsx";
import NHLPredictions from "../Pages/nhl/NHLPredictions.jsx";

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

export default function AppRouter() {
  const { user } = useAuth();
  const hasOpsAccess = isOpsUser(user);
  const [watchlistTotal, setWatchlistTotal] = useState(0);

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
    <BrowserRouter>
      <div>
        <Header />

        {/* ✅ This nav bar is global, shown on every page */}
        <nav className="px-4 py-2 mb-0">
          <div className="max-w-6xl mx-auto pp-chip pp-reveal-soft px-3 sm:px-4 py-2 flex flex-wrap justify-center sm:justify-end gap-x-4 sm:gap-x-6 gap-y-1">
            <a href="/" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
              Home
            </a>
            {user ? (
              <a href="/mlb/predictions" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
                Props
              </a>
            ) : (
              <a href="/login" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
                Predictions
              </a>
            )}
            {user ? (
              <a href="/watchlist" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
                {watchlistTotal > 0 ? `Watchlist (${watchlistTotal})` : "Watchlist"}
              </a>
            ) : null}
            <a href="/players/mlb" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
              MLB Players
            </a>
            <a href="/players/nhl" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
              NHL Players
            </a>
            {!user ? (
              <a href="/login" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
                Login
              </a>
            ) : null}
            {user && hasOpsAccess ? (
              <a href="/ops" className="text-xs sm:text-sm font-medium transition text-slate-700 hover:text-slate-900">
                Ops
              </a>
            ) : null}
          </div>
        </nav>
      </div>

      {/* Render route-based pages */}
      <RouteErrorBoundary>
        <Routes>
          {/* New multi-sport gateway at "/" */}
          <Route path="/" element={<HomeGateway />} />
          <Route path="/mlb/slate" element={<MLBHome />} />
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
            path="/nhl/predictions"
            element={
              <RequireSignedIn
                requiredPath="/nhl/predictions"
                requiredLabel="NHL predictions"
              >
                <NHLPredictions />
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
                {hasOpsAccess ? (
                  <OpsPage />
                ) : (
                  <div className="min-h-screen pp-page px-4 py-10">
                    <div className="max-w-2xl mx-auto pp-card p-6">
                      <h2 className="text-2xl font-semibold text-slate-900">Ops Access Restricted</h2>
                      <p className="text-slate-700 mt-2">
                        This page is restricted to authorized operations users.
                      </p>
                    </div>
                  </div>
                )}
              </RequireSignedIn>
            }
          />
          <Route path="/owner" element={<Navigate to="/ops" replace />} />
        </Routes>
      </RouteErrorBoundary>
    </BrowserRouter>
  );
}
