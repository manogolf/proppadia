// frontend/src/components/Header.jsx
import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext.jsx";
import { PrefetchNavLink } from "./navigation/PrefetchLink.jsx";

function sportNavClass({ isActive }) {
  return [
    "text-sm font-medium transition",
    isActive ? "text-slate-900" : "text-slate-700 hover:text-slate-900",
  ].join(" ");
}

export default function Header() {
  const { user, signOut } = useAuth();
  const navigate = useNavigate();

  const handleLogout = async () => {
    await signOut();
    navigate("/");
  };

  return (
    <header className="px-4 pt-4 mb-3">
      <div className="max-w-4xl mx-auto pp-card pp-reveal px-4 sm:px-5 py-4 sm:py-5 flex flex-wrap items-center justify-between gap-4">
        {/* LEFT: Logo */}
        <Link to="/" className="flex items-baseline">
          <h1 className="text-5xl sm:text-6xl font-bold text-slate-900 flex items-start leading-none">
            <span>P</span>
            <span className="text-[20px] sm:text-[25px] align-super">3</span>
          </h1>
          <span className="text-3xl sm:text-4xl font-bold text-slate-900 mt-1 -ml-[8px] sm:-ml-[10px]">
            roppadia
          </span>
        </Link>

        {/* CENTER: Brand + sport quick links */}
        <div className="flex items-center gap-4 sm:gap-6">
          <nav className="flex items-center gap-3 sm:gap-4">
            <PrefetchNavLink
              to="/mlb"
              className={sportNavClass}
            >
              MLB
            </PrefetchNavLink>
            <PrefetchNavLink
              to="/nhl"
              className={sportNavClass}
            >
              NHL
            </PrefetchNavLink>
          </nav>
        </div>

        {/* RIGHT: Tagline + auth */}
        <div className="flex flex-col items-end text-right space-y-1 min-w-0">
          <div className="text-med text-slate-700 font-medium">
            Player Prop Predictions
          </div>
          <div className="text-xs text-slate-500">Powered by Momentum</div>

          {/* Signed-in indicator */}
          {user?.email && (
            <span
              className="text-xs text-slate-600 border border-slate-300 rounded-full px-2 py-0.5 whitespace-nowrap max-w-[210px] truncate"
              title={user.email}
            >
              Signed in as {user.email}
            </span>
          )}

          {user ? (
            <button
              onClick={handleLogout}
              className="text-xs text-rose-600 hover:underline"
            >
              Logout
            </button>
          ) : (
            <PrefetchNavLink
              to="/login"
              className="text-xs text-slate-700 hover:underline"
            >
              Login
            </PrefetchNavLink>
          )}
        </div>
      </div>
    </header>
  );
}
