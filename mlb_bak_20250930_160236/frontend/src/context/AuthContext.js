// src/context/AuthContext.js (v2-ready)
import React, { createContext, useContext, useEffect, useState } from "react";
import { supabase } from "../utils/supabaseFrontend.js";

const AuthContext = createContext({ user: null, session: null, loading: true });

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [session, setSession] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let mounted = true;

    // 1) Get current session once
    (async () => {
      const { data, error } = await supabase.auth.getSession();
      if (!mounted) return;
      const sess = data?.session ?? null;
      setSession(sess);
      setUser(sess?.user ?? null);
      setLoading(false);
    })();

    // 2) Listen for auth changes
    const { data: sub } = supabase.auth.onAuthStateChange((_event, sess) => {
      setSession(sess ?? null);
      setUser(sess?.user ?? null);
    });

    return () => {
      mounted = false;
      sub?.subscription?.unsubscribe?.(); // v2 unsubscribe
    };
  }, []);

  return (
    <AuthContext.Provider value={{ user, session, loading }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);
