import React from "react";
import { Navigate, useLocation } from "react-router-dom";
import MemberLogin from "../components/MemberLogin.jsx";
import { Link } from "react-router-dom";
import { useAuth } from "../context/AuthContext.jsx";

export default function LoginPage() {
  const { user, loading } = useAuth();
  const location = useLocation();
  const from = location.state?.from?.pathname || "/props";
  const accessLabel =
    from === "/nhl/predictions"
      ? "NHL Predictions"
      : from === "/props" || from === "/props/v2"
      ? "MLB Predictions"
      : from === "/owner" || from === "/ops"
      ? "Operations Dashboard"
      : "member-only features";

  if (!loading && user) {
    return <Navigate to={from} replace />;
  }

  return (
    <div className="min-h-screen pp-page flex items-center justify-center px-4">
      <div className="pp-card pp-reveal p-8 w-full max-w-sm space-y-4">
        <h2 className="text-xl font-semibold text-center text-slate-900">
          Member Login
        </h2>
        <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
          Sign in to access <span className="font-medium">{accessLabel}</span>.
        </div>
        <MemberLogin />
        <div className="text-center text-sm text-slate-500">
          <Link to="/" className="text-slate-700 hover:underline">
            ← Back to Home
          </Link>
        </div>
      </div>
    </div>
  );
}
