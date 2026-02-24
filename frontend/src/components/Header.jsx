// frontend/src/components/Header.jsx
import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../context/AuthContext.jsx";

export default function Header() {
  const { user, signOut } = useAuth();
  const navigate = useNavigate();

  const handleLogout = async () => {
    await signOut();
    navigate("/");
  };

  return (
    <header className="px-4 pt-4 mb-3">
      <div className="max-w-6xl mx-auto pp-card pp-reveal px-4 sm:px-5 py-4 sm:py-5 flex flex-wrap items-center justify-between gap-4">
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
          <nav className="flex flex-wrap items-center justify-center gap-2 sm:gap-3">
            <a
              href="/mlb"
              className="pp-chip px-3 py-1.5 sm:px-4 sm:py-2 text-xs sm:text-sm font-semibold tracking-tight text-slate-900 transition hover:bg-white"
            >
              Today&apos;s MLB Games
            </a>
            <a
              href="/nhl"
              className="pp-chip px-3 py-1.5 sm:px-4 sm:py-2 text-xs sm:text-sm font-semibold tracking-tight text-slate-900 transition hover:bg-white"
            >
              Today&apos;s NHL Games
            </a>
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
            <a href="/login" className="text-xs text-slate-700 hover:underline">
              Login
            </a>
          )}
        </div>
      </div>
    </header>
  );
}
