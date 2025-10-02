import {
  BrowserRouter,
  Routes,
  Route,
  Link,
  useLocation,
} from "react-router-dom";
import PropsDashboard from "../Pages/PropsDashboard.jsx";
import LoginPage from "../Pages/Login.jsx";
import PlayerProfileDashboard from "../Pages/PlayerProfileDashboard.jsx"; // adjust path if needed
import ModelMetricsDashboard from "../Pages/ModelMetricsDashboard.jsx";
import PlayerTeamBrowser from "../Pages/PlayerTeamBrowser.jsx";
import PlayerPropsPage from "../Pages/PlayerPropsPage.jsx";
import Header from "../components/Header.jsx";
import Home from "../Pages/Home.jsx"; // ← existing MLB dashboard (kept as-is, now lives at /mlb)
import HomeGateway from "../Pages/HomeGateway.jsx"; // ← new multi-sport Home for "/"

export default function AppRouter() {
  return (
    <BrowserRouter>
      <Header />

      {/* ✅ This nav bar is global, shown on every page */}
      <nav className="bg-gray-100 border-b border-gray-300 py-2 mb-0">
        <div className="max-w-4xl mx-auto flex justify-end gap-6 px-4">
          <Link
            to="/"
            className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
          >
            Home
          </Link>
          <Link
            to="/props"
            className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
          >
            Props
          </Link>
          <Link
            to="/players"
            className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
          >
            Players By Team
          </Link>
          {/* <Link
            to="/player/665019" // Example player profile route
            className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
          >
            Player Profile
          </Link>
          <Link
            to="/metrics"
            className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
          >
            Metrics
          </Link>*/}
          <Link
            to="/login"
            className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
          >
            Login
          </Link>
        </div>
      </nav>

      {/* Render route-based pages */}
      <Routes>
        {/* New multi-sport gateway at "/" */}
        <Route path="/" element={<HomeGateway />} />
        {/* Existing MLB dashboard moved to "/mlb" */}
        <Route path="/mlb" element={<Home />} />
        <Route path="/props" element={<PropsDashboard />} />
        <Route path="/login" element={<LoginPage />} />
        <Route path="/player/:playerId" element={<PlayerProfileDashboard />} />
        <Route path="/metrics" element={<ModelMetricsDashboard />} />
        <Route path="/players" element={<PlayerTeamBrowser />} />
        <Route path="/props/v2" element={<PlayerPropsPage />} />
      </Routes>
    </BrowserRouter>
  );
}
