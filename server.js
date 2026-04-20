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

// ─── Fetch engagements (activity) for heatmap ─────────────────
// Uses the HubSpot v1 Engagements API which is more reliable for
// fetching all engagement types (notes, calls, emails, tasks, meetings)
async function fetchAllEngagements(daysBack) {
  const since = Date.now() - (daysBack * 24 * 60 * 60 * 1000);
  const headers = { "Authorization": "Bearer " + HUBSPOT_TOKEN };

  let allResults = [];
  let offset = 0;
  const maxPages = 50;

  for (let page = 0; page < maxPages; page++) {
    const url = `https://api.hubapi.com/engagements/v1/engagements/paged?offset=${offset}&limit=250`;

    let response = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      response = await fetch(url, { headers });
      if (response.status === 429) { await sleep(Math.pow(2, attempt) * 1000); continue; }
      break;
    }

    if (!response || !response.ok) {
      const text = response ? await response.text() : "no response";
      console.error("Engagements API error:", response ? response.status : "?", text);
      break;
    }

    const data = await response.json();
    const results = data.results || [];

    // Filter to recent engagements and extract what we need
    results.forEach(eng => {
      const ts = eng.engagement && eng.engagement.timestamp;
      const ownerId = eng.engagement && eng.engagement.ownerId;
      const type = eng.engagement && eng.engagement.type;
      if (ts && ts >= since && ownerId) {
        allResults.push({
          ownerId: String(ownerId),
          timestamp: ts,
          type: type || "unknown"
        });
      }
    });

    // Check if there are more pages
    if (data.hasMore && data.offset) {
      offset = data.offset;
    } else {
      break;
    }

    // Stop early if we've gone past our date range (engagements are sorted newest first)
    const oldestInBatch = results.reduce((min, eng) => {
      const ts = eng.engagement && eng.engagement.timestamp;
      return ts && ts < min ? ts : min;
    }, Infinity);
    if (oldestInBatch < since) break;

    await sleep(150);
  }

  return allResults;
}

// In-memory activity cache
let activityCache = { data: null, timestamp: 0 };
const ACTIVITY_CACHE_TTL = 30 * 60 * 1000; // 30 min

// GET /api/activity — drafter activity heatmap data
app.get("/api/activity", async (req, res) => {
  try {
    if (!HUBSPOT_TOKEN) {
      return res.status(500).json({ error: "HUBSPOT_API_KEY not set" });
    }

    const now = Date.now();
    if (activityCache.data && (now - activityCache.timestamp) < ACTIVITY_CACHE_TTL) {
      return res.json({ ...activityCache.data, _cachedAt: String(activityCache.timestamp) });
    }

    console.log("[" + new Date().toISOString() + "] Fetching activity data via Engagements v1...");
    const daysBack = 14; // fetch 2 weeks for navigation flexibility

    const [activities, ownerMap] = await Promise.all([
      fetchAllEngagements(daysBack),
      fetchOwners()
    ]);

    console.log("[" + new Date().toISOString() + "] Fetched " + activities.length + " engagements from v1 API.");

    const data = { activities, ownerMap };
    activityCache = { data, timestamp: now };

    res.json({ ...data, _cachedAt: String(now) });
  } catch (err) {
    console.error("Error fetching activity:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Diagnostic endpoint — tries multiple HubSpot engagement APIs to find what works
app.get("/api/activity-debug", async (req, res) => {
  if (!HUBSPOT_TOKEN) return res.status(500).json({ error: "No token" });

  const headers = { "Authorization": "Bearer " + HUBSPOT_TOKEN, "Content-Type": "application/json" };
  const results = {};

  // Test 1: Engagements v1 paged
  try {
    const r = await fetch("https://api.hubapi.com/engagements/v1/engagements/paged?limit=10", { headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN } });
    const text = await r.text();
    results["v1_engagements"] = { status: r.status, body: text.substring(0, 500) };
  } catch (e) { results["v1_engagements"] = { error: e.message }; }

  // Test 2: CRM v3 notes search
  try {
    const r = await fetch("https://api.hubapi.com/crm/v3/objects/notes/search", {
      method: "POST", headers,
      body: JSON.stringify({ filterGroups: [], properties: ["hs_timestamp", "hs_note_body", "hubspot_owner_id"], limit: 5 })
    });
    const text = await r.text();
    results["v3_notes_search"] = { status: r.status, body: text.substring(0, 500) };
  } catch (e) { results["v3_notes_search"] = { error: e.message }; }

  // Test 3: CRM v3 notes list (no search, just list)
  try {
    const r = await fetch("https://api.hubapi.com/crm/v3/objects/notes?limit=5&properties=hs_timestamp,hubspot_owner_id,hs_note_body", {
      headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
    });
    const text = await r.text();
    results["v3_notes_list"] = { status: r.status, body: text.substring(0, 500) };
  } catch (e) { results["v3_notes_list"] = { error: e.message }; }

  // Test 4: CRM v3 calls list
  try {
    const r = await fetch("https://api.hubapi.com/crm/v3/objects/calls?limit=5&properties=hs_timestamp,hubspot_owner_id", {
      headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
    });
    const text = await r.text();
    results["v3_calls_list"] = { status: r.status, body: text.substring(0, 500) };
  } catch (e) { results["v3_calls_list"] = { error: e.message }; }

  // Test 5: CRM v3 emails list
  try {
    const r = await fetch("https://api.hubapi.com/crm/v3/objects/emails?limit=5&properties=hs_timestamp,hubspot_owner_id", {
      headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
    });
    const text = await r.text();
    results["v3_emails_list"] = { status: r.status, body: text.substring(0, 500) };
  } catch (e) { results["v3_emails_list"] = { error: e.message }; }

  // Test 6: CRM v3 tasks list
  try {
    const r = await fetch("https://api.hubapi.com/crm/v3/objects/tasks?limit=5&properties=hs_timestamp,hubspot_owner_id", {
      headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
    });
    const text = await r.text();
    results["v3_tasks_list"] = { status: r.status, body: text.substring(0, 500) };
  } catch (e) { results["v3_tasks_list"] = { error: e.message }; }

  // Test 7: Engagements v2 (recent)
  try {
    const r = await fetch("https://api.hubapi.com/engagements/v1/engagements/recent/modified?count=10", {
      headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
    });
    const text = await r.text();
    results["v1_recent_modified"] = { status: r.status, body: text.substring(0, 500) };
  } catch (e) { results["v1_recent_modified"] = { error: e.message }; }

  res.json(results);
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
