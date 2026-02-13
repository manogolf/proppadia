import React from "react";
import { Link, NavLink } from "react-router-dom";
import { prefetchRoute } from "../../routes/prefetchRoute.js";

function callMaybe(fn, event) {
  if (typeof fn === "function") fn(event);
}

function buildPrefetchHandlers(prefetchTo, handlers = {}) {
  return {
    onMouseEnter: (e) => {
      callMaybe(handlers.onMouseEnter, e);
      if (prefetchTo) prefetchRoute(prefetchTo);
    },
    onFocus: (e) => {
      callMaybe(handlers.onFocus, e);
      if (prefetchTo) prefetchRoute(prefetchTo);
    },
    onTouchStart: (e) => {
      callMaybe(handlers.onTouchStart, e);
      if (prefetchTo) prefetchRoute(prefetchTo);
    },
  };
}

export function PrefetchLink({
  to,
  prefetchTo,
  onMouseEnter,
  onFocus,
  onTouchStart,
  ...rest
}) {
  const target = prefetchTo || (typeof to === "string" ? to : "");
  const handlers = buildPrefetchHandlers(target, {
    onMouseEnter,
    onFocus,
    onTouchStart,
  });
  return <Link to={to} {...handlers} {...rest} />;
}

export function PrefetchNavLink({
  to,
  prefetchTo,
  onMouseEnter,
  onFocus,
  onTouchStart,
  ...rest
}) {
  const target = prefetchTo || (typeof to === "string" ? to : "");
  const handlers = buildPrefetchHandlers(target, {
    onMouseEnter,
    onFocus,
    onTouchStart,
  });
  return <NavLink to={to} {...handlers} {...rest} />;
}
