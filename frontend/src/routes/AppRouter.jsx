import { Suspense, lazy } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Navigate,
  useLocation,
} from "react-router-dom";
import Header from "../components/Header.jsx";
import { PrefetchNavLink } from "../components/navigation/PrefetchLink.jsx";
import RouteErrorBoundary from "../components/RouteErrorBoundary.jsx";
import { useAuth } from "../context/AuthContext.jsx";
import { isOpsUser } from "../shared/opsAccess.js";
import {
  loadAccessRequiredPage,
  loadHomeGateway,
  loadLoginPage,
  loadMLBHome,
  loadModelMetricsDashboard,
  loadNHLHome,
  loadNHLPredictions,
  loadOpsPage,
  loadPlayerProfileDashboard,
  loadPlayerPropsPage,
  loadPlayerTeamBrowser,
  loadPropsDashboard,
  loadWatchlistPage,
} from "./prefetchRoute.js";

const HomeGateway = lazy(loadHomeGateway);
const MLBHome = lazy(loadMLBHome);
const NHLHome = lazy(loadNHLHome);
const NHLPredictions = lazy(loadNHLPredictions);
const PropsDashboard = lazy(loadPropsDashboard);
const LoginPage = lazy(loadLoginPage);
const PlayerProfileDashboard = lazy(loadPlayerProfileDashboard);
const ModelMetricsDashboard = lazy(loadModelMetricsDashboard);
const PlayerTeamBrowser = lazy(loadPlayerTeamBrowser);
const PlayerPropsPage = lazy(loadPlayerPropsPage);
const OpsPage = lazy(loadOpsPage);
const AccessRequiredPage = lazy(loadAccessRequiredPage);
const WatchlistPage = lazy(loadWatchlistPage);

function RouteFallback() {
  return (
    <div className="min-h-screen pp-page flex items-center justify-center text-slate-600">
      Loading page...
    </div>
  );
}

function navClassName({ isActive }) {
  return [
    "text-xs sm:text-sm font-medium transition",
    isActive ? "text-slate-900" : "text-slate-700 hover:text-slate-900",
  ].join(" ");
}

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

export default function AppRouter() {
  const { user } = useAuth();
  const hasOpsAccess = isOpsUser(user);

  return (
    <BrowserRouter>
      <Header />

      {/* ✅ This nav bar is global, shown on every page */}
      <nav className="px-4 py-2 mb-0">
        <div className="max-w-6xl mx-auto pp-chip pp-reveal-soft px-3 sm:px-4 py-2 flex flex-wrap justify-center sm:justify-end gap-x-4 sm:gap-x-6 gap-y-1">
          <PrefetchNavLink
            to="/"
            className={navClassName}
            end
          >
            Home
          </PrefetchNavLink>
          {user ? (
            <PrefetchNavLink
              to="/props"
              className={navClassName}
            >
              Props
            </PrefetchNavLink>
          ) : (
            <PrefetchNavLink
              to="/login"
              state={{ from: { pathname: "/props" } }}
              prefetchTo="/login"
              className={navClassName}
            >
              Predictions
            </PrefetchNavLink>
          )}
          {user ? (
            <PrefetchNavLink
              to="/watchlist"
              className={navClassName}
            >
              Watchlist
            </PrefetchNavLink>
          ) : null}
          <PrefetchNavLink
            to="/players"
            className={navClassName}
          >
            Players By Team
          </PrefetchNavLink>
          {!user ? (
            <PrefetchNavLink
              to="/login"
              className={navClassName}
            >
              Login
            </PrefetchNavLink>
          ) : null}
          {user && hasOpsAccess ? (
            <PrefetchNavLink
              to="/ops"
              className={navClassName}
            >
              Ops
            </PrefetchNavLink>
          ) : null}
        </div>
      </nav>

      {/* Render route-based pages */}
      <RouteErrorBoundary>
        <Suspense fallback={<RouteFallback />}>
          <Routes>
            {/* New multi-sport gateway at "/" */}
            <Route path="/" element={<HomeGateway />} />
            <Route path="/mlb" element={<MLBHome />} />
            <Route path="/nhl" element={<NHLHome />} />
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
            {/* Existing MLB dashboard moved to "/mlb" */}
            <Route
              path="/props"
              element={
                <RequireSignedIn requiredPath="/props" requiredLabel="MLB predictions">
                  <PropsDashboard />
                </RequireSignedIn>
              }
            />
            <Route path="/login" element={<LoginPage />} />
            <Route path="/player/:playerId" element={<PlayerProfileDashboard />} />
            <Route path="/metrics" element={<ModelMetricsDashboard />} />
            <Route path="/players" element={<PlayerTeamBrowser />} />
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
                  requiredPath="/props/v2"
                  requiredLabel="MLB predictions"
                >
                  <PlayerPropsPage />
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
        </Suspense>
      </RouteErrorBoundary>
    </BrowserRouter>
  );
}
