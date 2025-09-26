import {
  BrowserRouter,
  Routes,
  Route,
  Link,
  useLocation,
} from "react-router-dom";
import Header from "../components/Header.js";
import Home from "../Pages/Home.js";
import PropsDashboard from "../Pages/PropsDashboard.js";
import LoginPage from "../Pages/Login.js";
import PlayerProfileDashboard from "../Pages/PlayerProfileDashboard.js"; // adjust path if needed
import ModelMetricsDashboard from "../Pages/ModelMetricsDashboard.js";
import PlayerTeamBrowser from "../Pages/PlayerTeamBrowser.js";
import PlayerPropsPage from "../Pages/PlayerPropsPage.js";

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
        <Route path="/" element={<Home />} />
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
