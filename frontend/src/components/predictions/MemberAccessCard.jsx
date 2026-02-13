import React from "react";
import { useAuth } from "../../context/AuthContext.jsx";
import { PrefetchLink } from "../navigation/PrefetchLink.jsx";

export default function MemberAccessCard({
  openTo,
  loginFrom,
  ctas = [],
  singleLabel = "Predictions",
}) {
  const { user } = useAuth();
  const links =
    ctas.length > 0
      ? ctas
      : [{ label: singleLabel, openTo, loginFrom: loginFrom || openTo }];

  return (
    <div className="pp-card pp-reveal-soft p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-medium text-slate-900">
            Prediction Workspace
          </h2>
          <p className="text-sm text-slate-600 mt-1">
            Member access is required for MLB/NHL prediction pages.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {links.map((item) =>
            user ? (
              <PrefetchLink
                key={item.openTo}
                to={item.openTo}
                className="pp-btn pp-btn-primary pp-btn-md"
              >
                Open {item.label}
              </PrefetchLink>
            ) : (
              <PrefetchLink
                key={item.openTo}
                to="/login"
                state={{ from: { pathname: item.loginFrom || item.openTo } }}
                prefetchTo="/login"
                className="pp-btn pp-btn-primary pp-btn-md"
              >
                Login for {item.label}
              </PrefetchLink>
            )
          )}
        </div>
      </div>
    </div>
  );
}
