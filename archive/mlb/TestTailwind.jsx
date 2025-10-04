import React from "react";

export default function TestTailwind() {
  return (
    <div className="p-6 max-w-md mx-auto bg-white rounded-xl shadow-md space-y-4">
      <h1 className="text-2xl font-bold text-gray-800">
        Tailwind is Working ✅
      </h1>
      <p className="text-gray-600">
        If you're seeing padding, rounded corners, and a shadow, you're good to
        go!
      </p>
      <button className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
        Click Me
      </button>
    </div>
  );
}
