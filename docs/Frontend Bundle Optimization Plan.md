# Frontend Bundle Optimization Plan

Purpose: improve frontend load performance without reintroducing prior route/nav instability caused by mixed lazy/static loading behavior.

## Current Baseline (as of commit `5c42d5b9`)

- Frontend build passes: `npm --prefix frontend run build`
- Dynamic/static prefetch mismatch warnings: removed
  - `frontend/src/routes/prefetchRoute.js` now only prefetches `/ops` (the route that is actually lazy-loaded)
- Remaining warning:
  - large main bundle (`dist/assets/index-*.js` about `~977 kB` minified / `~289 kB` gzip)

## Constraints (Do Not Break)

- Keep core nav flows stable (prior "haunted" nav behavior was tied to mixed lazy/static route loading)
- Do not reintroduce dynamic/static import mismatch between:
  - `frontend/src/routes/AppRouter.jsx`
  - `frontend/src/routes/prefetchRoute.js`
- Optimize incrementally and smoke test after each step

Core paths that must remain stable during optimization:

- `/mlb` (redirect -> `/mlb/slate`)
- `/mlb/predictions`
- `/nhl` (redirect -> `/nhl/slate`)
- `/nhl/predictions` (auth/redirect path)

## Phased Plan

### Phase 1 (Safe First Move): Vite `manualChunks` only

Goal: reduce main bundle size without changing route behavior.

Approach:

- Add `manualChunks` in Vite config for heavy vendor libraries only (no route import changes)
- Candidate libraries:
  - `recharts`
  - `@supabase/supabase-js`
  - `react-day-picker`
  - other large third-party groups identified by build output

Validation:

- `npm --prefix frontend run build`
- Compare main chunk size before/after
- Quick smoke on core paths listed above

### Phase 2: Selective Lazy-Load Low-Risk Pages

Goal: reduce initial app bundle further without touching core nav pages.

Candidate routes/pages (lower frequency / safer to isolate):

- `ModelMetricsDashboard`
- `PlayerProfileDashboard`
- `PlayerTeamBrowser`
- `PlayerTeamChooser`
- `WatchlistPage`
- `OpsPage` (already lazy; keep)

Rules:

- If a route becomes lazy-loaded in `AppRouter.jsx`, add/keep matching prefetch support in `prefetchRoute.js`
- If a route remains static in `AppRouter.jsx`, do not dynamically prefetch it

Validation:

- Build
- Core path smoke
- New lazy-route click-through smoke

### Phase 3 (Optional): Core Prediction Route Lazy-Loading

Only consider if Phases 1-2 do not bring bundle size down enough.

Targets (high caution):

- `PlayerPropsPage` (MLB picks)
- `NHLPlayerPropsPage` (NHL picks/props)

Requirements:

- Explicit `Suspense` fallback UX
- Signed-in and signed-out smoke tests
- No prefetch changes unless route import strategy is intentionally updated in both router + prefetch files

### Phase 4: In-Page Deferral (Component-Level)

Goal: improve first render while keeping route structure stable.

Examples:

- Heavy chart/dashboard sections
- Below-the-fold analytics panels
- Secondary widgets not needed for initial interaction

## Validation Checklist (Every Phase)

- `npm --prefix frontend run build`
- Manual smoke:
  - `/mlb`
  - `/mlb/predictions`
  - `/nhl`
  - `/nhl/predictions`
  - back/forward navigation
  - direct refresh on each route
- Check browser console for route/loading errors
- Confirm no return of Vite dynamic/static import mismatch warnings (if route prefetch changes were touched)

## Unfinished Nav / Page Follow-Up (Return After Phase 1)

This work remains intentionally open and should be revisited after Phase 1 bundle changes:

- Confirm final nav wording/placement after live usage (`MLB Picks`, `NHL Picks`)
- Review MLB/NHL Today pages for wrapper/header treatment consistency (currently close, not identical)
- Decide whether additional CTA polish is needed on sport hub / Today’s Games pages
- Reconfirm no nav interaction regressions after any performance changes

## Notes

- The low-risk prefetch cleanup that removed mismatch warnings is in commit `5c42d5b9` (`Align route prefetching with lazy-loaded pages`).
- MLB/NHL home/nav UX refactor (including `MLBHome.jsx` and `Home.jsx` repurpose) is in commit `19ae60cb` (`Refine MLB and NHL home navigation flow`).
