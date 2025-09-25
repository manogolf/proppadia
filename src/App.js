import React from "react";
import AppRouter from "./routes/AppRouter.js";
import { AuthProvider } from "./context/AuthContext.js"; // make sure this path is correct

export default function App() {
  return (
    <AuthProvider>
      <AppRouter />
    </AuthProvider>
  );
}
