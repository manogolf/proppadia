import { useCallback, useEffect, useMemo, useState } from "react";
import { getBaseURL } from "../shared/getBaseURL.js";

const OPS_PREFS_KEY = "proppadia_ops_prefs_v1";
const OPS_LAST_SUCCESS_KEY = "proppadia_ops_last_success_v1";
const OPS_TOKEN_KEY = "proppadia_ops_token_v1";
const SLOW_CHECK_MS = 1000;
const STALE_SUCCESS_HOURS = 24;

function statusTone(ok) {
  if (ok === true) return "text-emerald-700 bg-emerald-50 border-emerald-200";
  if (ok === false) return "text-rose-700 bg-rose-50 border-rose-200";
  return "text-slate-700 bg-slate-50 border-slate-200";
}

function statusLabel(ok) {
  if (ok === true) return "PASS";
  if (ok === false) return "FAIL";
  return "UNKNOWN";
}

function latencyTone(ms) {
  if (typeof ms !== "number") return "text-slate-500";
  if (ms >= SLOW_CHECK_MS) return "text-rose-700";
  if (ms >= 500) return "text-amber-700";
  return "text-emerald-700";
}

function timeAgoLabel(isoTs) {
  if (!isoTs) return "never";
  const ts = new Date(isoTs).getTime();
  if (!Number.isFinite(ts)) return "unknown";
  const diffMs = Date.now() - ts;
  if (diffMs < 60_000) return "just now";
  const diffMin = Math.floor(diffMs / 60_000);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 48) return `${diffHr}h ago`;
  const diffDay = Math.floor(diffHr / 24);
  return `${diffDay}d ago`;
}

function successAgeTone(isoTs) {
  if (!isoTs) return "text-slate-500";
  const ts = new Date(isoTs).getTime();
  if (!Number.isFinite(ts)) return "text-slate-500";
  const ageHr = (Date.now() - ts) / 3_600_000;
  if (ageHr >= STALE_SUCCESS_HOURS) return "text-rose-700";
  if (ageHr >= 8) return "text-amber-700";
  return "text-emerald-700";
}

async function fetchJson(path, options = {}) {
  const base = getBaseURL();
  const url = `${base}${path.startsWith("/api/") ? path : `/api${path}`}`;
  const res = await fetch(url, { credentials: "include", ...options });
  let body = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  return { ok: res.ok, status: res.status, body };
}

async function fetchJsonTimed(path, options = {}) {
  const started = performance.now();
  const result = await fetchJson(path, options);
  const durationMs = Math.round(performance.now() - started);
  return { ...result, durationMs };
}

function isDeployInProgress(status) {
  const s = String(status || "").toLowerCase();
  if (!s) return false;
  return !["live", "failed", "canceled", "cancelled", "deactivated"].includes(s);
}

function formatMetricValue(value, unit) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  if (String(unit || "").toLowerCase() === "bytes") {
    return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  }
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
  return n.toFixed(3).replace(/\.?0+$/, "");
}

function buildMetricAlerts(metricsData) {
  const alerts = [];
  const cpuLatest = Number(metricsData?.cpu?.latest_value);
  if (Number.isFinite(cpuLatest)) {
    if (cpuLatest >= 95) {
      alerts.push({ level: "critical", text: `CPU critical at ${cpuLatest.toFixed(1)}%.` });
    } else if (cpuLatest >= 80) {
      alerts.push({ level: "warn", text: `CPU elevated at ${cpuLatest.toFixed(1)}%.` });
    }
  }

  const memLatest = Number(metricsData?.memory?.latest_value);
  const memUnit = String(metricsData?.memory?.unit || "").toLowerCase();
  if (Number.isFinite(memLatest)) {
    if (memUnit === "bytes") {
      const memMb = memLatest / (1024 * 1024);
      if (memMb >= 1900) {
        alerts.push({ level: "critical", text: `Memory critical at ${memMb.toFixed(0)} MB.` });
      } else if (memMb >= 1600) {
        alerts.push({ level: "warn", text: `Memory elevated at ${memMb.toFixed(0)} MB.` });
      }
    } else if (memLatest >= 95) {
      alerts.push({ level: "critical", text: `Memory critical at ${memLatest.toFixed(1)}%.` });
    } else if (memLatest >= 80) {
      alerts.push({ level: "warn", text: `Memory elevated at ${memLatest.toFixed(1)}%.` });
    }
  }
  return alerts;
}

export default function OpsPage() {
  const baseUrl = getBaseURL();
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [refreshSeconds, setRefreshSeconds] = useState(0);
  const [copiedKey, setCopiedKey] = useState("");
  const [copiedSnapshot, setCopiedSnapshot] = useState(false);
  const [failuresOnly, setFailuresOnly] = useState(false);
  const [expandedKeys, setExpandedKeys] = useState({});
  const [lastSuccessByKey, setLastSuccessByKey] = useState({});
  const [checks, setChecks] = useState([]);
  const [marketCoverage, setMarketCoverage] = useState({ count: 0, rows: [] });
  const [mlbStandingsMeta, setMlbStandingsMeta] = useState(null);
  const [nhlSlateMeta, setNhlSlateMeta] = useState(null);
  const [error, setError] = useState("");
  const [opsToken, setOpsToken] = useState("");
  const [deployStatus, setDeployStatus] = useState(null);
  const [deployLoading, setDeployLoading] = useState(false);
  const [deployError, setDeployError] = useState("");
  const [redeployRunning, setRedeployRunning] = useState(false);
  const [clearCache, setClearCache] = useState(false);
  const [metricsData, setMetricsData] = useState(null);
  const [metricsLoading, setMetricsLoading] = useState(false);
  const [resolveFromDate, setResolveFromDate] = useState("");
  const [resolveToDate, setResolveToDate] = useState("");
  const [resolveOnlyPast, setResolveOnlyPast] = useState(true);
  const [resolveDryRun, setResolveDryRun] = useState(true);
  const [resolveOutcome, setResolveOutcome] = useState("dnp");
  const [resolveLoading, setResolveLoading] = useState(false);
  const [resolveResult, setResolveResult] = useState(null);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(OPS_PREFS_KEY);
      if (!raw) return;
      const prefs = JSON.parse(raw);
      const refresh = Number(prefs?.refreshSeconds || 0);
      setRefreshSeconds([0, 30, 60].includes(refresh) ? refresh : 0);
      setFailuresOnly(Boolean(prefs?.failuresOnly));
    } catch {
      // ignore malformed local preferences
    }
  }, []);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(OPS_TOKEN_KEY);
      if (raw) setOpsToken(raw);
    } catch {
      // ignore local storage read errors
    }
  }, []);

  useEffect(() => {
    try {
      if (opsToken) window.localStorage.setItem(OPS_TOKEN_KEY, opsToken);
      else window.localStorage.removeItem(OPS_TOKEN_KEY);
    } catch {
      // ignore local storage write errors
    }
  }, [opsToken]);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        OPS_PREFS_KEY,
        JSON.stringify({ refreshSeconds, failuresOnly })
      );
    } catch {
      // ignore local storage write errors
    }
  }, [refreshSeconds, failuresOnly]);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(OPS_LAST_SUCCESS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") setLastSuccessByKey(parsed);
    } catch {
      // ignore malformed success timestamps
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        OPS_LAST_SUCCESS_KEY,
        JSON.stringify(lastSuccessByKey)
      );
    } catch {
      // ignore local storage write errors
    }
  }, [lastSuccessByKey]);

  const runChecks = useCallback(async () => {
    setRunning(true);
    setError("");
    try {
      const [
        health,
        mlbPing,
        mlbDb,
        nhlPing,
        nhlDb,
        marketCache,
        marketSupported,
        mlbStandings,
        nhlSlate,
      ] = await Promise.all([
        fetchJsonTimed("/api/health"),
        fetchJsonTimed("/api/mlb/ping"),
        fetchJsonTimed("/api/mlb/ping-db"),
        fetchJsonTimed("/api/nhl/ping"),
        fetchJsonTimed("/api/nhl/ping-db"),
        fetchJsonTimed("/api/mlb/market-cache-status"),
        fetchJsonTimed("/api/mlb/market-supported-props"),
        fetchJsonTimed("/api/mlb/standings"),
        fetchJsonTimed("/api/nhl/slate/meta"),
      ]);

      const nextChecks = [
        {
          key: "health",
          label: "API Health",
          path: "/api/health",
          ok: health.ok && health.body?.ok === true,
          durationMs: health.durationMs,
          detail: health.body || { status: health.status },
        },
        {
          key: "mlb_ping",
          label: "MLB Ping",
          path: "/api/mlb/ping",
          ok: mlbPing.ok && mlbPing.body?.ok === true,
          durationMs: mlbPing.durationMs,
          detail: mlbPing.body || { status: mlbPing.status },
        },
        {
          key: "mlb_db",
          label: "MLB DB Ping",
          path: "/api/mlb/ping-db",
          ok: mlbDb.ok && mlbDb.body?.ok === true,
          durationMs: mlbDb.durationMs,
          detail: mlbDb.body || { status: mlbDb.status },
        },
        {
          key: "nhl_ping",
          label: "NHL Ping",
          path: "/api/nhl/ping",
          ok: nhlPing.ok && nhlPing.body?.ok === true,
          durationMs: nhlPing.durationMs,
          detail: nhlPing.body || { status: nhlPing.status },
        },
        {
          key: "nhl_db",
          label: "NHL DB Ping",
          path: "/api/nhl/ping-db",
          ok: nhlDb.ok && nhlDb.body?.ok === true,
          durationMs: nhlDb.durationMs,
          detail: nhlDb.body || { status: nhlDb.status },
        },
        {
          key: "mlb_market_cache",
          label: "MLB Market Cache",
          path: "/api/mlb/market-cache-status",
          ok: marketCache.ok && marketCache.body?.ok === true,
          durationMs: marketCache.durationMs,
          detail: marketCache.body || { status: marketCache.status },
        },
        {
          key: "mlb_standings",
          label: "MLB Standings Cache",
          path: "/api/mlb/standings",
          ok:
            mlbStandings.ok &&
            mlbStandings.body?.ok === true &&
            Array.isArray(mlbStandings.body?.records),
          durationMs: mlbStandings.durationMs,
          detail: mlbStandings.body || { status: mlbStandings.status },
        },
        {
          key: "nhl_slate_meta",
          label: "NHL Slate Meta",
          path: "/api/nhl/slate/meta",
          ok:
            nhlSlate.ok &&
            nhlSlate.body?.ok === true &&
            typeof nhlSlate.body?.components === "object",
          durationMs: nhlSlate.durationMs,
          detail: nhlSlate.body || { status: nhlSlate.status },
        },
      ];

      const snapshotTs = new Date().toISOString();
      setLastSuccessByKey((prev) => {
        const next = { ...prev };
        for (const check of nextChecks) {
          if (check.ok) next[check.key] = snapshotTs;
        }
        return next;
      });

      setChecks(nextChecks);
      setMlbStandingsMeta(
        mlbStandings.ok && mlbStandings.body?.ok ? mlbStandings.body : null
      );
      setNhlSlateMeta(
        nhlSlate.ok && nhlSlate.body?.ok ? nhlSlate.body : null
      );
      if (marketSupported.ok && marketSupported.body?.ok === true) {
        setMarketCoverage({
          count: Number(marketSupported.body?.count || 0),
          rows: Array.isArray(marketSupported.body?.rows)
            ? marketSupported.body.rows
            : [],
        });
      } else {
        setMarketCoverage({ count: 0, rows: [] });
      }
      setLastUpdated(new Date().toISOString());
    } catch (e) {
      setError(e?.message || "Failed to run operations checks.");
    } finally {
      setLoading(false);
      setRunning(false);
    }
  }, []);

  const deployHeaders = useMemo(() => {
    const headers = {};
    if (opsToken) headers["X-Ops-Token"] = opsToken;
    return headers;
  }, [opsToken]);

  const loadDeployStatus = useCallback(async () => {
    setDeployLoading(true);
    setDeployError("");
    try {
      const res = await fetchJsonTimed("/api/ops/render/deploy-status", {
        headers: deployHeaders,
      });
      if (!res.ok || !res.body?.ok) {
        throw new Error(res.body?.detail || `deploy-status failed (${res.status})`);
      }
      setDeployStatus((prev) => {
        if (res.body?.deploy) return res.body;
        if (prev?.deploy) return { ...res.body, deploy: prev.deploy };
        return res.body;
      });
    } catch (e) {
      setDeployError(e?.message || "Failed to load deploy status.");
    } finally {
      setDeployLoading(false);
    }
  }, [deployHeaders]);

  const runRedeploy = useCallback(async () => {
    setRedeployRunning(true);
    setDeployError("");
    try {
      const res = await fetchJson("/api/ops/render/redeploy", {
        method: "POST",
        headers: { "Content-Type": "application/json", ...deployHeaders },
        body: JSON.stringify({ clear_cache: clearCache }),
      });
      if (!res.ok || !res.body?.ok) {
        throw new Error(res.body?.detail || `redeploy failed (${res.status})`);
      }
      setDeployStatus((prev) => {
        if (res.body?.deploy) return res.body;
        if (prev?.deploy) return { ...res.body, deploy: prev.deploy };
        return res.body;
      });
      await loadDeployStatus();
    } catch (e) {
      setDeployError(e?.message || "Failed to trigger redeploy.");
    } finally {
      setRedeployRunning(false);
    }
  }, [clearCache, deployHeaders, loadDeployStatus]);

  const loadMetrics = useCallback(async () => {
    setMetricsLoading(true);
    setDeployError("");
    try {
      const res = await fetchJsonTimed("/api/ops/render/metrics?window_minutes=360&resolution_seconds=60", {
        headers: deployHeaders,
      });
      if (!res.ok || !res.body?.ok) {
        throw new Error(res.body?.detail || `metrics failed (${res.status})`);
      }
      setMetricsData(res.body);
    } catch (e) {
      setDeployError(e?.message || "Failed to load Render metrics.");
    } finally {
      setMetricsLoading(false);
    }
  }, [deployHeaders]);

  const runResolve = useCallback(async () => {
    setResolveLoading(true);
    setDeployError("");
    setResolveResult(null);
    try {
      const payload = {
        from_date: resolveFromDate || null,
        to_date: resolveToDate || null,
        dry_run: resolveDryRun,
        only_past_games: resolveOnlyPast,
        outcome: resolveOutcome,
      };
      const res = await fetchJson("/api/ops/nhl/resolve-props", {
        method: "POST",
        headers: { "Content-Type": "application/json", ...deployHeaders },
        body: JSON.stringify(payload),
      });
      if (!res.ok || !res.body?.ok) {
        throw new Error(res.body?.detail || `resolve failed (${res.status})`);
      }
      setResolveResult(res.body);
    } catch (e) {
      setDeployError(e?.message || "Failed to resolve NHL props.");
    } finally {
      setResolveLoading(false);
    }
  }, [
    deployHeaders,
    resolveDryRun,
    resolveFromDate,
    resolveOnlyPast,
    resolveOutcome,
    resolveToDate,
  ]);

  useEffect(() => {
    runChecks();
  }, [runChecks]);

  useEffect(() => {
    if (!opsToken) return;
    loadDeployStatus();
    loadMetrics();
  }, [opsToken, loadDeployStatus, loadMetrics]);

  useEffect(() => {
    if (!refreshSeconds) return;
    const timer = window.setInterval(() => {
      runChecks();
    }, refreshSeconds * 1000);
    return () => window.clearInterval(timer);
  }, [refreshSeconds, runChecks]);

  useEffect(() => {
    if (!opsToken) return;
    if (!isDeployInProgress(deployStatus?.deploy?.status)) return;
    const timer = window.setInterval(() => {
      loadDeployStatus();
    }, 10_000);
    return () => window.clearInterval(timer);
  }, [deployStatus?.deploy?.status, loadDeployStatus, opsToken]);

  const summary = useMemo(() => {
    const total = checks.length;
    const passed = checks.filter((c) => c.ok).length;
    const failed = checks.filter((c) => c.ok === false).length;
    const timings = checks
      .map((c) => c.durationMs)
      .filter((v) => typeof v === "number");
    const avgMs = timings.length
      ? Math.round(timings.reduce((a, b) => a + b, 0) / timings.length)
      : 0;
    const maxMs = timings.length ? Math.max(...timings) : 0;
    const slow = timings.filter((ms) => ms >= SLOW_CHECK_MS).length;
    return { passed, failed, total, avgMs, maxMs, slow };
  }, [checks]);

  const metricAlerts = useMemo(() => buildMetricAlerts(metricsData), [metricsData]);

  const visibleChecks = useMemo(() => {
    const sorted = [...checks].sort((a, b) => {
      const aFail = a.ok === false ? 0 : 1;
      const bFail = b.ok === false ? 0 : 1;
      return aFail - bFail;
    });
    if (!failuresOnly) return sorted;
    return sorted.filter((c) => c.ok === false);
  }, [checks, failuresOnly]);

  const runbook = useMemo(
    () => [
      {
        key: "mlb_post_deploy",
        label: "MLB Post-Deploy",
        cmd: `make mlb-post-deploy BASE_URL=${baseUrl}`,
      },
      {
        key: "mlb_post_deploy_strict",
        label: "MLB Post-Deploy (Strict)",
        cmd: `make mlb-post-deploy-strict BASE_URL=${baseUrl}`,
      },
      {
        key: "nhl_post_deploy",
        label: "NHL Post-Deploy",
        cmd: `make nhl-post-deploy BASE_URL=${baseUrl}`,
      },
    ],
    [baseUrl]
  );

  const handleCopy = useCallback(async (key, cmd) => {
    try {
      await navigator.clipboard.writeText(cmd);
      setCopiedKey(key);
      window.setTimeout(() => setCopiedKey(""), 1200);
    } catch {
      setCopiedKey("");
    }
  }, []);

  const toggleExpanded = useCallback((key) => {
    setExpandedKeys((prev) => ({ ...prev, [key]: !prev[key] }));
  }, []);

  const expandAll = useCallback(() => {
    setExpandedKeys(
      checks.reduce((acc, check) => {
        acc[check.key] = true;
        return acc;
      }, {})
    );
  }, [checks]);

  const collapseAll = useCallback(() => {
    setExpandedKeys({});
  }, []);

  const copySnapshot = useCallback(async () => {
    const payload = {
      captured_at: new Date().toISOString(),
      base_url: baseUrl,
      last_updated: lastUpdated,
      summary,
      checks,
      market_coverage_count: marketCoverage.count,
    };
    try {
      await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
      setCopiedSnapshot(true);
      window.setTimeout(() => setCopiedSnapshot(false), 1200);
    } catch {
      setCopiedSnapshot(false);
    }
  }, [baseUrl, checks, lastUpdated, marketCoverage.count, summary]);

  return (
    <div className="min-h-screen pp-page">
      <div className="max-w-6xl mx-auto px-4 py-6">
        <div className="pp-card">
          <div className="px-5 py-4 border-b border-slate-200 flex flex-wrap items-start justify-between gap-3">
            <div>
              <div className="text-xs tracking-wide uppercase text-slate-500 mb-1">
                Admin
              </div>
              <h1 className="text-2xl font-semibold text-slate-900">Operations Dashboard</h1>
              <p className="text-sm text-slate-600 mt-1">
                Live checks for backend health, sport services, and MLB market coverage.
              </p>
            </div>
            <div className="text-right">
              <div className="flex items-center justify-end gap-2">
                <select
                  value={refreshSeconds}
                  onChange={(e) => setRefreshSeconds(Number(e.target.value))}
                  className="pp-btn pp-btn-secondary pp-btn-md"
                >
                  <option value={0}>Auto Refresh: Off</option>
                  <option value={30}>Auto Refresh: 30s</option>
                  <option value={60}>Auto Refresh: 60s</option>
                </select>
                <button
                  type="button"
                  onClick={runChecks}
                  disabled={running}
                  className="pp-btn pp-btn-secondary pp-btn-md"
                >
                  {running ? "Refreshing..." : "Refresh Checks"}
                </button>
              </div>
              <div className="text-xs text-slate-500 mt-2">
                {lastUpdated ? `Last updated: ${new Date(lastUpdated).toLocaleString()}` : "Not run yet"}
              </div>
              <label className="mt-2 inline-flex items-center gap-2 text-xs text-slate-600">
                <input
                  type="checkbox"
                  checked={failuresOnly}
                  onChange={(e) => setFailuresOnly(e.target.checked)}
                />
                Failures only
              </label>
            </div>
          </div>

          <div className="px-5 py-4 border-b border-slate-200">
            <div className="text-sm text-slate-700">
              Summary:{" "}
              <span className="font-semibold">
                {loading
                  ? "..."
                  : `${summary.passed}/${summary.total} passing (${summary.failed} failing)`}
              </span>
            </div>
            {!loading ? (
              <div className="text-xs text-slate-600 mt-1">
                Latency: avg {summary.avgMs}ms, max {summary.maxMs}ms, slow{" "}
                ({SLOW_CHECK_MS}ms+) {summary.slow}
              </div>
            ) : null}
            <div className="mt-2 flex items-center gap-2">
              <button
                type="button"
                onClick={expandAll}
                className="pp-btn pp-btn-secondary pp-btn-sm text-xs"
              >
                Expand all
              </button>
              <button
                type="button"
                onClick={collapseAll}
                className="pp-btn pp-btn-secondary pp-btn-sm text-xs"
              >
                Collapse all
              </button>
              <button
                type="button"
                onClick={copySnapshot}
                className="pp-btn pp-btn-secondary pp-btn-sm text-xs"
              >
                {copiedSnapshot ? "Snapshot copied" : "Copy Snapshot JSON"}
              </button>
            </div>
            {error ? <div className="text-sm text-rose-700 mt-2">{error}</div> : null}
          </div>

          <div className="px-5 py-4 border-b border-slate-200">
            <div className="text-sm font-semibold text-slate-900">Render Controls</div>
            <p className="text-xs text-slate-600 mt-1">
              Ops-only redeploy trigger and latest deploy status. No raw logs shown.
            </p>
            <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
              <div className="md:col-span-2">
                <div className="text-xs text-slate-500 mb-1">Ops token</div>
                <input
                  type="password"
                  value={opsToken}
                  onChange={(e) => setOpsToken(e.target.value)}
                  placeholder="X-Ops-Token value"
                  className="w-full pp-chip px-3 py-2 text-sm text-slate-800"
                />
              </div>
              <div className="flex gap-2 justify-start md:justify-end">
                <button
                  type="button"
                  onClick={loadDeployStatus}
                  disabled={deployLoading || !opsToken}
                  className="pp-btn pp-btn-secondary pp-btn-md"
                >
                  {deployLoading ? "Refreshing..." : "Refresh Deploy"}
                </button>
                <button
                  type="button"
                  onClick={loadMetrics}
                  disabled={metricsLoading || !opsToken}
                  className="pp-btn pp-btn-secondary pp-btn-md"
                >
                  {metricsLoading ? "Refreshing..." : "Refresh Metrics"}
                </button>
                <button
                  type="button"
                  onClick={runRedeploy}
                  disabled={redeployRunning || !opsToken}
                  className="pp-btn pp-btn-secondary pp-btn-md"
                >
                  {redeployRunning ? "Triggering..." : "Redeploy"}
                </button>
              </div>
            </div>
            <label className="mt-2 inline-flex items-center gap-2 text-xs text-slate-600">
              <input
                type="checkbox"
                checked={clearCache}
                onChange={(e) => setClearCache(e.target.checked)}
              />
              Clear build cache on redeploy
            </label>
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm">
              <div className="font-medium text-slate-800">Latest deploy</div>
              <div className="text-xs text-slate-600 mt-1">
                Status:{" "}
                <span className={isDeployInProgress(deployStatus?.deploy?.status) ? "text-amber-700 font-semibold" : "text-slate-700 font-semibold"}>
                  {deployStatus?.deploy?.status || "unknown"}
                </span>
              </div>
              <div className="text-xs text-slate-600">Deploy ID: {deployStatus?.deploy?.id || "-"}</div>
              <div className="text-xs text-slate-600">Commit: {deployStatus?.deploy?.commit_id || "-"}</div>
              <div className="text-xs text-slate-600">
                Created: {deployStatus?.deploy?.created_at ? new Date(deployStatus.deploy.created_at).toLocaleString() : "-"}
              </div>
              <div className="text-xs text-slate-600">
                Finished: {deployStatus?.deploy?.finished_at ? new Date(deployStatus.deploy.finished_at).toLocaleString() : "-"}
              </div>
            </div>
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm">
              <div className="font-medium text-slate-800">Alert strip</div>
              {metricAlerts.length === 0 ? (
                <div className="text-xs text-emerald-700 mt-1">No active CPU/Memory alerts.</div>
              ) : (
                <div className="mt-2 space-y-1">
                  {metricAlerts.map((alert, idx) => (
                    <div
                      key={`${alert.level}-${idx}`}
                      className={
                        alert.level === "critical"
                          ? "rounded border border-rose-200 bg-rose-50 text-rose-700 px-2 py-1 text-xs"
                          : "rounded border border-amber-200 bg-amber-50 text-amber-700 px-2 py-1 text-xs"
                      }
                    >
                      {alert.text}
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm">
              <div className="font-medium text-slate-800">Application metrics (Render)</div>
              <div className="text-xs text-slate-600 mt-1">
                Window: {metricsData?.window?.minutes || 0}m @ {metricsData?.window?.resolution_seconds || 0}s
              </div>
              <div className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-2 text-xs text-slate-700">
                <div className="rounded border border-slate-200 bg-white px-2 py-2">
                  <div className="font-semibold text-slate-800">CPU</div>
                  <div>Latest: {formatMetricValue(metricsData?.cpu?.latest_value, metricsData?.cpu?.unit)}</div>
                  <div>Avg: {formatMetricValue(metricsData?.cpu?.avg, metricsData?.cpu?.unit)}</div>
                  <div>Max: {formatMetricValue(metricsData?.cpu?.max, metricsData?.cpu?.unit)}</div>
                  <div>Points: {metricsData?.cpu?.points ?? "-"}</div>
                </div>
                <div className="rounded border border-slate-200 bg-white px-2 py-2">
                  <div className="font-semibold text-slate-800">Memory</div>
                  <div>Latest: {formatMetricValue(metricsData?.memory?.latest_value, metricsData?.memory?.unit)}</div>
                  <div>Avg: {formatMetricValue(metricsData?.memory?.avg, metricsData?.memory?.unit)}</div>
                  <div>Max: {formatMetricValue(metricsData?.memory?.max, metricsData?.memory?.unit)}</div>
                  <div>Points: {metricsData?.memory?.points ?? "-"}</div>
                </div>
              </div>
            </div>
            <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm">
              <div className="font-medium text-slate-800">NHL Lifecycle Resolver (Ops)</div>
              <div className="text-xs text-slate-600 mt-1">
                Resolve pending NHL props in `player_props` for a date window. Use dry-run first.
              </div>
              <div className="mt-2 grid grid-cols-1 md:grid-cols-2 gap-2">
                <label className="text-xs text-slate-700">
                  <span className="block mb-1">From date</span>
                  <input
                    type="date"
                    className="w-full pp-chip px-2 py-1 text-sm text-slate-800"
                    value={resolveFromDate}
                    onChange={(e) => setResolveFromDate(e.target.value)}
                  />
                </label>
                <label className="text-xs text-slate-700">
                  <span className="block mb-1">To date</span>
                  <input
                    type="date"
                    className="w-full pp-chip px-2 py-1 text-sm text-slate-800"
                    value={resolveToDate}
                    onChange={(e) => setResolveToDate(e.target.value)}
                  />
                </label>
              </div>
              <div className="mt-2 flex flex-wrap gap-3 items-center">
                <label className="inline-flex items-center gap-2 text-xs text-slate-700">
                  <input
                    type="checkbox"
                    checked={resolveOnlyPast}
                    onChange={(e) => setResolveOnlyPast(e.target.checked)}
                  />
                  only past games
                </label>
                <label className="inline-flex items-center gap-2 text-xs text-slate-700">
                  <input
                    type="checkbox"
                    checked={resolveDryRun}
                    onChange={(e) => setResolveDryRun(e.target.checked)}
                  />
                  dry run
                </label>
                <label className="inline-flex items-center gap-2 text-xs text-slate-700">
                  outcome
                  <select
                    className="pp-chip px-2 py-1 text-sm text-slate-800"
                    value={resolveOutcome}
                    onChange={(e) => setResolveOutcome(e.target.value)}
                  >
                    <option value="dnp">dnp</option>
                    <option value="push">push</option>
                    <option value="win">win</option>
                    <option value="loss">loss</option>
                  </select>
                </label>
                <button
                  type="button"
                  className="pp-btn pp-btn-secondary pp-btn-sm"
                  onClick={runResolve}
                  disabled={resolveLoading || !opsToken}
                >
                  {resolveLoading ? "Running..." : resolveDryRun ? "Preview Resolve" : "Apply Resolve"}
                </button>
              </div>
              {resolveResult ? (
                <div className="mt-2 text-xs text-slate-700 rounded border border-slate-200 bg-white px-2 py-2">
                  matched={resolveResult.matched ?? "-"} updated={resolveResult.updated ?? "-"} dry_run=
                  {String(resolveResult.dry_run)}
                  {resolveResult?.range?.min_game_date || resolveResult?.range?.max_game_date ? (
                    <span>
                      {" "}range={resolveResult?.range?.min_game_date || "-"}..{resolveResult?.range?.max_game_date || "-"}
                    </span>
                  ) : null}
                </div>
              ) : null}
            </div>
            {deployError ? <div className="text-sm text-rose-700 mt-2">{deployError}</div> : null}
          </div>

          <div className="px-5 py-4 border-b border-slate-200">
            <div className="text-sm font-semibold text-slate-900">Runbook</div>
            <div className="text-xs text-slate-600 mt-1 break-all">
              Active BASE_URL: {baseUrl}
            </div>
            <div className="mt-3 space-y-2">
              {runbook.map((item) => (
                <div
                  key={item.key}
                  className="rounded-lg border border-slate-200 bg-slate-50 p-2 flex items-center justify-between gap-2"
                >
                  <div className="min-w-0">
                    <div className="text-xs font-medium text-slate-700">{item.label}</div>
                    <div className="text-xs text-slate-600 break-all">{item.cmd}</div>
                  </div>
                  <button
                    type="button"
                    onClick={() => handleCopy(item.key, item.cmd)}
                    className="pp-btn pp-btn-secondary pp-btn-sm text-xs shrink-0"
                  >
                    {copiedKey === item.key ? "Copied" : "Copy"}
                  </button>
                </div>
              ))}
            </div>
          </div>

          <div className="px-5 py-5 grid grid-cols-1 md:grid-cols-2 gap-3">
            {visibleChecks.map((check) => (
              <div
                key={check.key}
                className={`rounded-xl border px-3 py-3 ${statusTone(check.ok)}`}
              >
                <div className="flex items-center justify-between gap-2">
                  <div className="font-medium">{check.label}</div>
                  <div className="flex items-center gap-2">
                    <div className="text-xs opacity-80">
                      <span className={latencyTone(check.durationMs)}>
                        {typeof check.durationMs === "number"
                          ? `${check.durationMs}ms`
                          : ""}
                      </span>
                    </div>
                    <div className="text-xs font-semibold tracking-wide">
                      {statusLabel(check.ok)}
                    </div>
                  </div>
                </div>
                <div className="text-xs opacity-80 mt-1">
                  Last success:{" "}
                  <span className={successAgeTone(lastSuccessByKey[check.key])}>
                    {lastSuccessByKey[check.key]
                      ? `${new Date(lastSuccessByKey[check.key]).toLocaleString()} (${timeAgoLabel(lastSuccessByKey[check.key])})`
                      : "never"}
                  </span>
                </div>
                {check.path ? (
                  <div className="mt-1">
                    <a
                      href={`${baseUrl}${check.path}`}
                      target="_blank"
                      rel="noreferrer"
                      className="text-xs underline"
                    >
                      Open endpoint
                    </a>
                  </div>
                ) : null}
                <button
                  type="button"
                  onClick={() => toggleExpanded(check.key)}
                  className="mt-2 pp-btn pp-btn-ghost pp-btn-sm text-xs"
                >
                  {expandedKeys[check.key] ? "Hide details" : "Show details"}
                </button>
                {expandedKeys[check.key] ? (
                  <pre className="mt-2 text-xs whitespace-pre-wrap break-words">
                    {JSON.stringify(check.detail, null, 2)}
                  </pre>
                ) : null}
              </div>
            ))}
            {visibleChecks.length === 0 ? (
              <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
                No failing checks in current snapshot.
              </div>
            ) : null}
          </div>

          <div className="px-5 pb-5">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 mb-4">
              <h2 className="text-sm font-semibold text-slate-900">Data Freshness</h2>
              <p className="text-xs text-slate-600 mt-1">
                Cache/source status for backend-owned MLB and NHL slate context feeds.
              </p>
              <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-2 text-xs text-slate-700">
                <div className="rounded border border-slate-200 bg-white px-3 py-2">
                  <div className="font-semibold text-slate-800">MLB Standings</div>
                  <div>source: {mlbStandingsMeta?.source || "-"}</div>
                  <div>stale: {String(Boolean(mlbStandingsMeta?.stale))}</div>
                  <div>
                    cached_at:{" "}
                    {mlbStandingsMeta?.cached_at
                      ? new Date(mlbStandingsMeta.cached_at).toLocaleString()
                      : "-"}
                  </div>
                  <div>records: {Array.isArray(mlbStandingsMeta?.records) ? mlbStandingsMeta.records.length : "-"}</div>
                </div>
                <div className="rounded border border-slate-200 bg-white px-3 py-2">
                  <div className="font-semibold text-slate-800">NHL Slate Meta</div>
                  <div>source: {nhlSlateMeta?.source || "-"}</div>
                  <div>stale: {String(Boolean(nhlSlateMeta?.stale))}</div>
                  <div>
                    cached_at:{" "}
                    {nhlSlateMeta?.cached_at
                      ? new Date(nhlSlateMeta.cached_at).toLocaleString()
                      : "-"}
                  </div>
                  <div>
                    components ok:{" "}
                    {nhlSlateMeta?.components
                      ? Object.values(nhlSlateMeta.components).filter((c) => c?.ok === true).length
                      : "-"}
                  </div>
                </div>
              </div>
            </div>

            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4">
              <h2 className="text-sm font-semibold text-slate-900">MLB Market Coverage</h2>
              <p className="text-xs text-slate-600 mt-1">
                Supported prop types from backend mapping: {marketCoverage.count}
              </p>
              <div className="mt-3 flex flex-wrap gap-2">
                {marketCoverage.rows.map((row) => (
                  <span
                    key={row.prop_type}
                    className="inline-flex items-center gap-1 rounded-full bg-white border border-slate-300 px-2 py-1 text-xs text-slate-700"
                  >
                    <span className="font-medium">{row.prop_type}</span>
                    <span className="text-slate-400">→</span>
                    <span>{row.market_key}</span>
                  </span>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
