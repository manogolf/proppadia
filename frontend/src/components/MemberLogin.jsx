// src/components/MemberLogin.jsx (Supabase JS v2)
import React, { useState } from "react";
import { AUTH_CONFIG, supabase } from "../utils/supabaseFrontend.js";

const OAUTH_LABELS = {
  apple: "Apple",
  github: "GitHub",
  google: "Google",
};

function authErrorMessage(err) {
  const msg = String(err?.message || "");
  const lower = msg.toLowerCase();
  if (lower.includes("provider") && lower.includes("not enabled")) {
    return "That login provider is not enabled yet. Update Supabase Auth providers and retry.";
  }
  if (lower.includes("signup is disabled")) {
    return "Signups are disabled. Ask for an invite or use an existing member account.";
  }
  return msg || "Authentication failed.";
}

export default function MemberLogin() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const oauthProviders = AUTH_CONFIG.oauthProviders.filter((p) => OAUTH_LABELS[p]);

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
        emailRedirectTo: AUTH_CONFIG.redirectTo,
        shouldCreateUser: AUTH_CONFIG.magicLinkCreateUser,
      },
    });
    setLoading(false);
    if (error) return setErr(authErrorMessage(error));
    alert("Check your email for the magic link.");
  };

  const handleOAuth = async (provider) => {
    const { error } = await supabase.auth.signInWithOAuth({
      provider,
      options: { redirectTo: AUTH_CONFIG.redirectTo },
    });
    if (error) setErr(authErrorMessage(error));
  };

  return (
    <>
      {err && (
        <div className="mb-3 rounded border border-rose-200 bg-rose-50 p-2 text-sm text-rose-700">
          {err}
        </div>
      )}

      {(AUTH_CONFIG.enablePasswordLogin || AUTH_CONFIG.enableMagicLink) && (
        <div className="space-y-3">
          <input
            type="email"
            placeholder="Email"
            className="w-full pp-chip rounded p-2"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            autoComplete="email"
            required={AUTH_CONFIG.enablePasswordLogin}
          />

          {AUTH_CONFIG.enablePasswordLogin ? (
            <form className="space-y-3" onSubmit={handlePasswordLogin}>
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
          ) : null}

          {AUTH_CONFIG.enableMagicLink ? (
            <button
              onClick={handleMagicLink}
              disabled={loading || !email}
              className="pp-btn pp-btn-secondary pp-btn-md pp-btn-block"
            >
              Email magic link
            </button>
          ) : null}
        </div>
      )}

      {oauthProviders.length > 0 ? (
        <div className="mt-4 grid gap-2">
          {oauthProviders.map((provider) => (
            <button
              key={provider}
              onClick={() => handleOAuth(provider)}
              className="pp-btn pp-btn-secondary pp-btn-md"
            >
              Continue with {OAUTH_LABELS[provider]}
            </button>
          ))}
        </div>
      ) : null}

      {!AUTH_CONFIG.enablePasswordLogin &&
      !AUTH_CONFIG.enableMagicLink &&
      oauthProviders.length === 0 ? (
        <div className="mt-3 rounded border border-amber-200 bg-amber-50 p-2 text-sm text-amber-800">
          No login methods are enabled. Configure auth providers in env/Supabase.
        </div>
      ) : null}
    </>
  );
}
