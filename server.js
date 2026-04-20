const express = require("express");
const fetch = require("node-fetch");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

// ─── HubSpot config ─────────────────────────────────────────────
const HUBSPOT_TOKEN = process.env.HUBSPOT_API_KEY;

const PROPERTIES = [
  "hs_v2_date_entered_1223620771", "hs_v2_date_exited_1223620771",
  "hs_v2_date_entered_1223620775", "hs_v2_date_entered_1223620777",
  "hs_v2_date_entered_1223620773", "hs_v2_date_exited_1223620773",
  "hs_v2_date_entered_1223620772",
  "hs_v2_date_entered_1223620778",
  "hs_v2_date_exited_1223620774",
  "hs_priority", "urgent_request_reason",
  "drafting_owner", "proof_reading__owner",
  "pipeline", "dealstage", "dealname",
  "original_date_entered_drafting_instructions",
  "date_draft_will_sent", "original_date_draft_will_sent",
  "drafting_query_reason",
  "original_date_entered_drafts_with_customer",
  "original_date_entered_ready_for_printing",
  "original_date_entered_sent_to_customer",
  "hs_v2_date_entered_1338772492", "hs_v2_date_exited_1338772492",
  "amendment_source",
  "date_marked_urgent",
  "first_date_exited_drafting_instructions",
  "hs_v2_date_entered_1223751329"
];

// ─── In-memory cache ─────────────────────────────────────────────
let cache = { data: null, timestamp: 0 };
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

// ─── Helper: sleep ──────────────────────────────────────────────
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─── Fetch all deals from HubSpot CRM Search API ───────────────
// HubSpot limits to 5 filterGroups per request, so we batch and deduplicate
async function fetchDealsWithFilters(filterGroups) {
  const url = "https://api.hubapi.com/crm/v3/objects/deals/search";
  const headers = {
    "Authorization": "Bearer " + HUBSPOT_TOKEN,
    "Content-Type": "application/json"
  };

  let allDeals = [];
  let after = null;
  const maxPages = 100;

  for (let page = 0; page < maxPages; page++) {
    const body = {
      filterGroups,
      properties: PROPERTIES,
      limit: 100
    };
    if (after) body.after = after;

    // Retry logic for 429 rate limits
    let response = null;
    for (let attempt = 0; attempt < 5; attempt++) {
      response = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(body)
      });

      if (response.status === 429) {
        await sleep(Math.pow(2, attempt) * 1000);
        continue;
      }
      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HubSpot API error ${response.status}: ${text}`);
      }
      break;
    }
    if (response.status === 429) {
      throw new Error("HubSpot API rate limit exceeded after 5 retries");
    }

    const data = await response.json();
    allDeals = allDeals.concat(data.results || []);

    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      break;
    }

    // Small delay between pages
    if (after) await sleep(200);
  }

  return allDeals;
}

async function fetchAllDeals() {
  // Batch 1: pipeline stage filters (max 5)
  const batch1 = [
    { filters: [{ propertyName: "hs_v2_date_entered_1223620771", operator: "HAS_PROPERTY" }] },
    { filters: [{ propertyName: "hs_v2_date_entered_1223620773", operator: "HAS_PROPERTY" }] },
    { filters: [{ propertyName: "hs_v2_date_exited_1223620773", operator: "HAS_PROPERTY" }] },
    { filters: [{ propertyName: "hs_v2_date_entered_1223620775", operator: "HAS_PROPERTY" }] },
    { filters: [{ propertyName: "hs_v2_date_entered_1223620777", operator: "HAS_PROPERTY" }] }
  ];

  // Batch 2: additional stage filters
  const batch2 = [
    { filters: [{ propertyName: "hs_v2_date_entered_1223751329", operator: "HAS_PROPERTY" }] }
  ];

  const [deals1, deals2] = await Promise.all([
    fetchDealsWithFilters(batch1),
    fetchDealsWithFilters(batch2)
  ]);

  // Deduplicate by deal ID
  const seen = new Set();
  const merged = [];
  [...deals1, ...deals2].forEach(d => {
    if (!seen.has(d.id)) {
      seen.add(d.id);
      merged.push(d);
    }
  });

  return merged;
}

// ─── Fetch HubSpot owners ──────────────────────────────────────
async function fetchOwners() {
  const url = "https://api.hubapi.com/crm/v3/owners?limit=500";
  const response = await fetch(url, {
    headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
  });

  if (!response.ok) return {};

  const data = await response.json();
  const map = {};
  (data.results || []).forEach(o => {
    const name = ((o.firstName || "") + " " + (o.lastName || "")).trim();
    map[String(o.id)] = name || o.email || String(o.id);
  });
  return map;
}

// ─── Fetch property options (enum labels) ──────────────────────
async function fetchPropertyOptions(propName) {
  const url = `https://api.hubapi.com/crm/v3/properties/deals/${propName}`;
  const response = await fetch(url, {
    headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
  });

  if (!response.ok) return {};

  const data = await response.json();
  const map = {};
  (data.options || []).forEach(o => {
    map[o.value] = o.label;
  });
  return map;
}

// ─── Fetch all fresh data ──────────────────────────────────────
async function fetchFreshData() {
  const [deals, ownerMap, draftingOwnerOptions, proofOwnerOptions, queryReasonOptions, urgentReasonOptions, amendmentSourceOptions] =
    await Promise.all([
      fetchAllDeals(),
      fetchOwners(),
      fetchPropertyOptions("drafting_owner").catch(() => ({})),
      fetchPropertyOptions("proof_reading__owner").catch(() => ({})),
      fetchPropertyOptions("drafting_query_reason").catch(() => ({})),
      fetchPropertyOptions("urgent_request_reason").catch(() => ({})),
      fetchPropertyOptions("amendment_source").catch(() => ({}))
    ]);

  return {
    deals,
    ownerMap,
    draftingOwnerOptions,
    proofOwnerOptions,
    queryReasonOptions,
    urgentReasonOptions,
    amendmentSourceOptions
  };
}

// ─── Middleware ──────────────────────────────────────────────────
app.use(express.json({ limit: "50mb" }));

// Serve static files from public directory
app.use(express.static(path.join(__dirname, "public")));

// ─── API Routes ─────────────────────────────────────────────────

// GET /api/deals — serves cached data or fetches fresh
app.get("/api/deals", async (req, res) => {
  try {
    if (!HUBSPOT_TOKEN) {
      return res.status(500).json({ error: "HUBSPOT_API_KEY environment variable is not set" });
    }

    const now = Date.now();
    if (cache.data && (now - cache.timestamp) < CACHE_TTL_MS) {
      return res.json({ ...cache.data, _cachedAt: String(cache.timestamp) });
    }

    console.log("[" + new Date().toISOString() + "] Cache miss — fetching fresh data from HubSpot...");
    const data = await fetchFreshData();
    cache = { data, timestamp: now };
    console.log("[" + new Date().toISOString() + "] Fetched " + data.deals.length + " deals, cached.");

    res.json({ ...data, _cachedAt: String(now) });
  } catch (err) {
    console.error("Error fetching deals:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/deals/refresh — force refresh, ignoring cache
app.post("/api/deals/refresh", async (req, res) => {
  try {
    if (!HUBSPOT_TOKEN) {
      return res.status(500).json({ error: "HUBSPOT_API_KEY environment variable is not set" });
    }

    console.log("[" + new Date().toISOString() + "] Force refresh requested...");
    const data = await fetchFreshData();
    const now = Date.now();
    cache = { data, timestamp: now };
    console.log("[" + new Date().toISOString() + "] Fetched " + data.deals.length + " deals, cached.");

    res.json({ ...data, _cachedAt: String(now) });
  } catch (err) {
    console.error("Error refreshing deals:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Health check
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    cached: !!cache.data,
    cacheAge: cache.timestamp ? Math.round((Date.now() - cache.timestamp) / 1000) + "s" : null,
    dealCount: cache.data ? cache.data.deals.length : 0
  });
});

// Serve the dashboard for all other routes (SPA fallback)
app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ─── Background cache refresh (every hour) ──────────────────────
async function backgroundRefresh() {
  if (!HUBSPOT_TOKEN) return;
  try {
    console.log("[" + new Date().toISOString() + "] Background cache refresh...");
    const data = await fetchFreshData();
    cache = { data, timestamp: Date.now() };
    console.log("[" + new Date().toISOString() + "] Background refresh done — " + data.deals.length + " deals.");
  } catch (err) {
    console.error("Background refresh failed:", err.message);
  }
}

// ─── Start server ───────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Dashboard server running on port ${PORT}`);

  // Initial data fetch on startup
  backgroundRefresh();

  // Refresh every hour
  setInterval(backgroundRefresh, CACHE_TTL_MS);
});
