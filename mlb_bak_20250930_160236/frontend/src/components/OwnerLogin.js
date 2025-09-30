// src/pages/OwnerLogin.js (Supabase JS v2)
import React, { useState } from "react";
import { supabase } from "../utils/supabaseFrontend.js";

export default function OwnerLogin() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const handlePasswordLogin = async (e) => {
    e.preventDefault();
    setErr("");
    setLoading(true);
    const { data, error } = await supabase.auth.signInWithPassword({
      email,
      password,
    });
    setLoading(false);
    if (error) return setErr(error.message);

    // onAuthStateChange in AuthContext will update the app;
    // optionally navigate now:
    window.location.assign("/");
  };

  const handleMagicLink = async (e) => {
    e.preventDefault();
    setErr("");
    setLoading(true);
    const { error } = await supabase.auth.signInWithOtp({
      email,
      options: {
        emailRedirectTo: `${window.location.origin}/`,
      },
    });
    setLoading(false);
    if (error) return setErr(error.message);
    alert("Check your email for the magic link.");
  };

  const handleOAuth = async (provider) => {
    const { error } = await supabase.auth.signInWithOAuth({
      provider,
      options: { redirectTo: `${window.location.origin}/` },
    });
    if (error) setErr(error.message);
  };

  return (
    <div className="max-w-sm mx-auto p-6 bg-white rounded-xl shadow">
      <h1 className="text-xl font-semibold mb-4">Owner Login</h1>

      {err && (
        <div className="mb-3 rounded border border-red-200 bg-red-50 p-2 text-sm text-red-700">
          {err}
        </div>
      )}

      <form className="space-y-3" onSubmit={handlePasswordLogin}>
        <input
          type="email"
          placeholder="Email"
          className="w-full border rounded p-2"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          autoComplete="email"
          required
        />
        <input
          type="password"
          placeholder="Password"
          className="w-full border rounded p-2"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="current-password"
          required
        />
        <button
          type="submit"
          disabled={loading}
          className="w-full px-4 py-2 rounded bg-blue-600 text-white disabled:opacity-50"
        >
          {loading ? "Signing in…" : "Sign in"}
        </button>
      </form>

      <div className="mt-4 flex gap-2">
        <button
          onClick={handleMagicLink}
          disabled={loading || !email}
          className="flex-1 px-3 py-2 rounded border"
        >
          Email magic link
        </button>
        <button
          onClick={() => handleOAuth("github")}
          className="flex-1 px-3 py-2 rounded border"
        >
          GitHub
        </button>
      </div>
    </div>
  );
}
