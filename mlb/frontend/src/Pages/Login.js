import React from "react";
import OwnerLogin from "../components/OwnerLogin.js";
import { Link } from "react-router-dom";

export default function LoginPage() {
  return (
    <div className="min-h-screen bg-gray-100 flex items-center justify-center px-4">
      <div className="bg-white shadow-xl rounded-2xl p-8 w-full max-w-sm space-y-4">
        <h2 className="text-xl font-semibold text-center text-gray-800">
          Owner Login
        </h2>
        <OwnerLogin />
        <div className="text-center text-sm text-gray-500">
          <Link to="/" className="text-blue-600 hover:underline">
            ← Back to Home
          </Link>
        </div>
      </div>
    </div>
  );
}
