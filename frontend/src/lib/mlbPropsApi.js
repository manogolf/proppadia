import { api } from "./api.js";

const DEFAULT_PAGE_SIZE = 200;
const DEFAULT_MAX_ROWS = 5000;

function buildHistoryQuery({
  fromDate,
  toDate,
  limit,
  offset,
  userId,
  propSource,
  status,
}) {
  const params = new URLSearchParams();
  if (fromDate) params.set("from_date", String(fromDate));
  if (toDate) params.set("to_date", String(toDate));
  if (limit != null) params.set("limit", String(limit));
  if (offset != null) params.set("offset", String(offset));
  if (userId) params.set("user_id", String(userId));
  if (propSource) params.set("prop_source", String(propSource));
  if (status) params.set("status", String(status));
  return `/api/props/history?${params.toString()}`;
}

function buildCurrentHistoryQuery({
  fromDate,
  toDate,
  limit,
  offset,
  propSource,
  status,
}) {
  const params = new URLSearchParams();
  if (fromDate) params.set("from_date", String(fromDate));
  if (toDate) params.set("to_date", String(toDate));
  if (limit != null) params.set("limit", String(limit));
  if (offset != null) params.set("offset", String(offset));
  if (propSource) params.set("prop_source", String(propSource));
  if (status) params.set("status", String(status));
  return `/api/mlb/streak-history?${params.toString()}`;
}

export async function fetchMlbPropHistoryPage(params) {
  const payload = await api(buildHistoryQuery(params || {}));
  return {
    total: Number(payload?.total ?? 0),
    rows: Array.isArray(payload?.rows) ? payload.rows : [],
    raw: payload,
  };
}

export async function fetchMlbPropHistoryAll({
  fromDate,
  toDate,
  userId,
  propSource,
  status,
  pageSize = DEFAULT_PAGE_SIZE,
  maxRows = DEFAULT_MAX_ROWS,
} = {}) {
  const allRows = [];
  let offset = 0;
  let total = Infinity;

  while (offset < total && offset < maxRows) {
    const page = await fetchMlbPropHistoryPage({
      fromDate,
      toDate,
      userId,
      propSource,
      status,
      limit: pageSize,
      offset,
    });
    total = Number.isFinite(page.total) ? page.total : allRows.length + page.rows.length;
    allRows.push(...page.rows);
    if (page.rows.length < pageSize) break;
    offset += pageSize;
  }

  return allRows;
}

export async function fetchMlbCurrentPropHistoryPage(params) {
  const payload = await api(buildCurrentHistoryQuery(params || {}));
  return {
    total: Number(payload?.total ?? 0),
    rows: Array.isArray(payload?.rows) ? payload.rows : [],
    raw: payload,
  };
}

export async function fetchMlbCurrentPropHistoryAll({
  fromDate,
  toDate,
  propSource = "mlb_api",
  status,
  pageSize = DEFAULT_PAGE_SIZE,
  maxRows = DEFAULT_MAX_ROWS,
} = {}) {
  const allRows = [];
  let offset = 0;
  let total = Infinity;

  while (offset < total && offset < maxRows) {
    const page = await fetchMlbCurrentPropHistoryPage({
      fromDate,
      toDate,
      propSource,
      status,
      limit: pageSize,
      offset,
    });
    total = Number.isFinite(page.total) ? page.total : allRows.length + page.rows.length;
    allRows.push(...page.rows);
    if (page.rows.length < pageSize) break;
    offset += pageSize;
  }

  return allRows;
}

export async function fetchMlbPropsForDate(gameDate, opts = {}) {
  return fetchMlbPropHistoryAll({
    fromDate: gameDate,
    toDate: gameDate,
    ...opts,
  });
}
