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
  if (lower.includes("user already registered")) {
    return "That email already has an account. Use Sign in or Magic link.";
  }
  if (lower.includes("password") && lower.includes("6")) {
    return "Password must be at least 6 characters.";
  }
  return msg || "Authentication failed.";
}

export default function MemberLogin() {
  const [mode, setMode] = useState("signin");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");
  const [notice, setNotice] = useState("");
  const oauthProviders = AUTH_CONFIG.oauthProviders.filter((p) => OAUTH_LABELS[p]);

  const isSignupMode = mode === "signup";

  const handlePasswordAuth = async (e) => {
    e.preventDefault();
    setErr("");
    setNotice("");
    if (isSignupMode && password !== confirmPassword) {
      setErr("Password confirmation does not match.");
      return;
    }
    setLoading(true);
    const trimmedEmail = String(email || "").trim();
    const trimmedPassword = String(password || "");
    const { data, error } = isSignupMode
      ? await supabase.auth.signUp({
          email: trimmedEmail,
          password: trimmedPassword,
          options: {
            emailRedirectTo: AUTH_CONFIG.redirectTo,
          },
        })
      : await supabase.auth.signInWithPassword({
          email: trimmedEmail,
          password: trimmedPassword,
        });
    setLoading(false);
    if (error) return setErr(authErrorMessage(error));

    if (isSignupMode) {
      if (data?.session) {
        // Let AuthContext + LoginPage route-state redirect handle post-login navigation.
        setNotice("Account created. You are now signed in.");
      } else {
        setNotice(
          "Account created. Check your email to confirm your account, then return to sign in."
        );
      }
      return;
    }

    // Let AuthContext + LoginPage route-state redirect handle post-login navigation.
    if (!data?.session) return setErr("Sign in did not return a session.");
  };

  const handleMagicLink = async (e) => {
    e.preventDefault();
    setErr("");
    setNotice("");
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
    setNotice("Check your email for the magic link.");
  };

  const handleOAuth = async (provider) => {
    setErr("");
    setNotice("");
    const { error } = await supabase.auth.signInWithOAuth({
      provider,
      options: { redirectTo: AUTH_CONFIG.redirectTo },
    });
    if (error) setErr(authErrorMessage(error));
  };

  return (
    <>
      {AUTH_CONFIG.enablePasswordLogin ? (
        <div className="mb-3 grid grid-cols-2 overflow-hidden rounded border border-slate-200 bg-slate-50 text-sm">
          <button
            type="button"
            onClick={() => {
              setMode("signin");
              setErr("");
              setNotice("");
            }}
            className={`px-3 py-2 text-center font-medium ${
              !isSignupMode ? "bg-white text-slate-900" : "text-slate-600 hover:bg-white"
            }`}
          >
            Sign in
          </button>
          <button
            type="button"
            onClick={() => {
              setMode("signup");
              setErr("");
              setNotice("");
            }}
            className={`border-l border-slate-200 px-3 py-2 text-center font-medium ${
              isSignupMode ? "bg-white text-slate-900" : "text-slate-600 hover:bg-white"
            }`}
          >
            Create account
          </button>
        </div>
      ) : null}

      {notice && (
        <div className="mb-3 rounded border border-emerald-200 bg-emerald-50 p-2 text-sm text-emerald-800">
          {notice}
        </div>
      )}

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
            <form className="space-y-3" onSubmit={handlePasswordAuth}>
              <input
                type="password"
                placeholder="Password"
                className="w-full pp-chip rounded p-2"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                autoComplete={isSignupMode ? "new-password" : "current-password"}
                required
              />
              {isSignupMode ? (
                <input
                  type="password"
                  placeholder="Confirm password"
                  className="w-full pp-chip rounded p-2"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  autoComplete="new-password"
                  required
                />
              ) : null}
              <button
                type="submit"
                disabled={loading}
                className="pp-btn pp-btn-primary pp-btn-md pp-btn-block"
              >
                {loading
                  ? isSignupMode
                    ? "Creating account…"
                    : "Signing in…"
                  : isSignupMode
                  ? "Create account"
                  : "Sign in"}
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
