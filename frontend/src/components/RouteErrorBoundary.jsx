import React from "react";

export default class RouteErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, message: "" };
  }

  static getDerivedStateFromError(error) {
    return {
      hasError: true,
      message: error?.message || "Failed to load this page.",
    };
  }

  componentDidCatch(error) {
    console.error("RouteErrorBoundary caught:", error);
  }

  render() {
    if (!this.state.hasError) return this.props.children;

    return (
      <div className="min-h-screen pp-page flex items-center justify-center px-4">
        <div className="pp-card p-6 max-w-md w-full text-center space-y-3">
          <h1 className="text-lg font-semibold text-slate-900">
            Page failed to load
          </h1>
          <p className="text-sm text-slate-600">{this.state.message}</p>
          <button
            type="button"
            onClick={() => window.location.reload()}
            className="pp-btn pp-btn-primary pp-btn-md"
          >
            Reload
          </button>
        </div>
      </div>
    );
  }
}
