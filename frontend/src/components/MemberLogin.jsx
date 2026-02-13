// src/components/MemberLogin.jsx (Supabase JS v2)
import React, { useState } from "react";
import { supabase } from "../utils/supabaseFrontend.js";

export default function MemberLogin() {
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
    // Let AuthContext + LoginPage route-state redirect handle post-login navigation.
    if (!data?.session) return setErr("Sign in did not return a session.");
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
    <>
      {err && (
        <div className="mb-3 rounded border border-rose-200 bg-rose-50 p-2 text-sm text-rose-700">
          {err}
        </div>
      )}

      <form className="space-y-3" onSubmit={handlePasswordLogin}>
        <input
          type="email"
          placeholder="Email"
          className="w-full pp-chip rounded p-2"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          autoComplete="email"
          required
        />
        <input
          type="password"
          placeholder="Password"
          className="w-full pp-chip rounded p-2"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          autoComplete="current-password"
          required
        />
        <button
          type="submit"
          disabled={loading}
          className="pp-btn pp-btn-primary pp-btn-md pp-btn-block"
        >
          {loading ? "Signing in…" : "Sign in"}
        </button>
      </form>

      <div className="mt-4 flex gap-2">
        <button
          onClick={handleMagicLink}
          disabled={loading || !email}
          className="pp-btn pp-btn-secondary pp-btn-md flex-1"
        >
          Email magic link
        </button>
        <button
          onClick={() => handleOAuth("github")}
          className="pp-btn pp-btn-secondary pp-btn-md flex-1"
        >
          GitHub
        </button>
      </div>
    </>
  );
}
