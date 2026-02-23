export const loadOpsPage = () => import("../Pages/OpsPage.jsx");

const routeLoaders = {
  // Keep prefetch only for routes that are actually lazy-loaded in AppRouter.
  // Revisit bundle-size optimization later: if we reintroduce route-level lazy
  // loading for non-core pages, add matching prefetch loaders here in lockstep
  // with AppRouter to avoid dynamic/static import mismatch warnings and nav
  // instability.
  "/ops": loadOpsPage,
};

const prefetched = new Set();

export function prefetchRoute(pathname) {
  const key = String(pathname || "").trim();
  if (!key || prefetched.has(key)) return;
  const staticLoader = routeLoaders[key];
  const loader = staticLoader;
  if (!loader) return;
  prefetched.add(key);
  loader().catch(() => {
    // Best-effort prefetch only; ignore transient failures.
    prefetched.delete(key);
  });
}
