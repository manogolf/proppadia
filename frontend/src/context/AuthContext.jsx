// frontend/src/context/AuthContext.jsx
import React, { createContext, useContext, useEffect, useState } from "react";
import { supabase } from "../utils/supabaseFrontend.js";

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [session, setSession] = useState(null);
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  // initialize from current session
  useEffect(() => {
    let mounted = true;

    (async () => {
      const { data, error } = await supabase.auth.getSession();
      if (mounted) {
        if (!error) {
          setSession(data?.session ?? null);
          setUser(data?.session?.user ?? null);
        }
        setLoading(false);
      }
    })();

    // subscribe to auth state changes
    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, newSession) => {
      setSession(newSession);
      setUser(newSession?.user ?? null);
    });

    return () => {
      mounted = false;
      subscription?.unsubscribe?.();
    };
  }, []);

  const signOut = async () => {
    try {
      await supabase.auth.signOut();
    } finally {
      // ensure local state clears even if network flakes
      setSession(null);
      setUser(null);
    }
  };

  return (
    <AuthContext.Provider value={{ user, session, loading, signOut }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
