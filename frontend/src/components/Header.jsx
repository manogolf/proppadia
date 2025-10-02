import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext.js";

export default function Header() {
  const { user, signOut } = useAuth();
  const navigate = useNavigate();

  const handleLogout = async () => {
    await signOut();
    navigate("/");
  };

  return (
    <header className="bg-blue-100 border-b border-blue-50 shadow-sm py-6 px-4 mb-4">
      <div className="max-w-4xl mx-auto py-6 flex items-center justify-between">
        {/* LEFT: Logo */}
        <Link to="/" className="flex items-baseline">
          <h1 className="text-6xl font-bold text-indigo-900 flex items-start">
            <span>P</span>
            <span className="text-[25px] align-super">3</span>
          </h1>
          <span className="text-4xl font-bold text-indigo-900 mt-1 -ml-[10px]">
            roppadia
          </span>
        </Link>

        {/* CENTER: Optional Spacer */}
        <div className="flex-1 flex justify-center">
          <div className="w-4"></div>{" "}
          {/* Try w-2, w-8, or px-4 to test spacing */}
        </div>

        <div className="flex items-center gap-6">
          <Link
            to="/"
            className="text-lg font-semibold text-blue-900 hover:text-blue-700"
          >
            Proppadia
          </Link>
          {/* sport quick links */}
          <nav className="flex items-center gap-4">
            <Link
              to="/mlb"
              className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
            >
              MLB
            </Link>
            <Link
              to="/nhl"
              className="text-sm text-gray-700 hover:text-indigo-700 font-medium"
            >
              NHL
            </Link>
          </nav>
        </div>

        {/* RIGHT: Stacked text + auth */}
        <div className="flex flex-col items-end text-right space-y-1">
          <div className="text-med text-gray-600 font-medium">
            Player Prop Predictions
          </div>
          <div className="text-xs text-gray-400">Powered by Momentum</div>
          {user ? (
            <button
              onClick={handleLogout}
              className="text-xs text-red-600 hover:underline"
            >
              Logout
            </button>
          ) : (
            <Link
              to="/login"
              className="text-xs text-indigo-00 hover:underline"
            >
              Login
            </Link>
          )}
        </div>
      </div>
    </header>
  );
}
