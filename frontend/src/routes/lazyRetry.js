function isChunkLoadError(err) {
  const msg = String(err?.message || err || "");
  return (
    msg.includes("Failed to fetch dynamically imported module") ||
    msg.includes("Importing a module script failed") ||
    msg.includes("ChunkLoadError")
  );
}

/**
 * Load a lazy chunk and auto-recover once on stale-chunk deploy races.
 * This avoids "dead nav" behavior when a route chunk hash changed between deploys.
 */
export async function loadWithRetry(importer, key) {
  try {
    return await importer();
  } catch (err) {
    if (typeof window === "undefined" || !isChunkLoadError(err)) {
      throw err;
    }

    const marker = `proppadia:lazy-retry:${String(key || "route")}`;
    const alreadyRetried = window.sessionStorage.getItem(marker) === "1";
    if (!alreadyRetried) {
      window.sessionStorage.setItem(marker, "1");
      window.location.reload();
      return new Promise(() => {});
    }

    throw err;
  }
}

