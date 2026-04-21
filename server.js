const express = require("express");
const fetch = require("node-fetch");
const path = require("path");
const session = require("express-session");
const passport = require("passport");
const GoogleStrategy = require("passport-google-oauth20").Strategy;

const app = express();
const PORT = process.env.PORT || 3000;

// ─── HubSpot config ─────────────────────────────────────────────
const HUBSPOT_TOKEN = process.env.HUBSPOT_API_KEY;

// ─── Google OAuth config ────────────────────────────────────────
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const SESSION_SECRET = process.env.SESSION_SECRET || "fallback-dev-secret-change-me";
const ALLOWED_DOMAIN = process.env.ALLOWED_EMAIL_DOMAIN || "octopuslegacy.com";
const CALLBACK_URL = process.env.CALLBACK_URL || "/auth/google/callback";
const AUTH_ENABLED = !!(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET);

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
  "hs_v2_date_entered_1223751329",
  "hs_v2_date_entered_1223751330",
  "ep_lead_source", "date_of_appointment"
];

// ─── Komal Singh exclusion list ─────────────────────────────────
// Deals that had drafting queries under Komal Singh's ownership.
// After reassignment, these queries should NOT count against the new owner's metrics.
const fs = require("fs");
let KOMAL_EXCLUSION_IDS = new Set();
try {
  const exPath = path.join(__dirname, "komal-exclusions.json");
  const exData = JSON.parse(fs.readFileSync(exPath, "utf8"));
  KOMAL_EXCLUSION_IDS = new Set(exData.deals.map(d => d.recordId));
  console.log(`[Komal exclusion] Loaded ${KOMAL_EXCLUSION_IDS.size} deal IDs to exclude from query metrics`);
} catch (e) {
  console.log("[Komal exclusion] No exclusion file found or error reading it — no exclusions active");
}

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

// ─── Fetch deal pipeline stage labels ──────────────────────────
async function fetchDealStageLabels() {
  const url = "https://api.hubapi.com/crm/v3/pipelines/deals";
  const response = await fetch(url, {
    headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
  });
  if (!response.ok) return {};
  const data = await response.json();
  const map = {};
  (data.results || []).forEach(pipeline => {
    (pipeline.stages || []).forEach(stage => {
      map[stage.id] = stage.label;
    });
  });
  return map;
}

// ─── Fetch all fresh data ──────────────────────────────────────
async function fetchFreshData() {
  const [deals, ownerMap, draftingOwnerOptions, proofOwnerOptions, queryReasonOptions, urgentReasonOptions, amendmentSourceOptions, leadSourceOptions, stageLabels] =
    await Promise.all([
      fetchAllDeals(),
      fetchOwners(),
      fetchPropertyOptions("drafting_owner").catch(() => ({})),
      fetchPropertyOptions("proof_reading__owner").catch(() => ({})),
      fetchPropertyOptions("drafting_query_reason").catch(() => ({})),
      fetchPropertyOptions("urgent_request_reason").catch(() => ({})),
      fetchPropertyOptions("amendment_source").catch(() => ({})),
      fetchPropertyOptions("ep_lead_source").catch(() => ({})),
      fetchDealStageLabels().catch(() => ({}))
    ]);

  return {
    deals,
    ownerMap,
    draftingOwnerOptions,
    proofOwnerOptions,
    queryReasonOptions,
    urgentReasonOptions,
    amendmentSourceOptions,
    leadSourceOptions,
    stageLabels,
    komalExclusionIds: Array.from(KOMAL_EXCLUSION_IDS)
  };
}

// ─── Middleware ──────────────────────────────────────────────────
app.use(express.json({ limit: "50mb" }));

// ─── Session & Passport ─────────────────────────────────────────
app.set("trust proxy", 1); // trust Render's proxy for secure cookies
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === "production" || !!process.env.RENDER,
    httpOnly: true,
    maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
    sameSite: "lax"
  }
}));

app.use(passport.initialize());
app.use(passport.session());

// Passport serialize/deserialize — store minimal user info in session
passport.serializeUser((user, done) => done(null, user));
passport.deserializeUser((user, done) => done(null, user));

// Google OAuth strategy
if (AUTH_ENABLED) {
  passport.use(new GoogleStrategy({
    clientID: GOOGLE_CLIENT_ID,
    clientSecret: GOOGLE_CLIENT_SECRET,
    callbackURL: CALLBACK_URL
  }, (accessToken, refreshToken, profile, done) => {
    const email = (profile.emails && profile.emails[0] && profile.emails[0].value) || "";
    const domain = email.split("@")[1] || "";
    if (domain.toLowerCase() !== ALLOWED_DOMAIN.toLowerCase()) {
      return done(null, false, { message: "Unauthorized domain: " + domain });
    }
    return done(null, {
      id: profile.id,
      email: email,
      name: profile.displayName || email,
      photo: (profile.photos && profile.photos[0] && profile.photos[0].value) || null
    });
  }));
  console.log("Google OAuth enabled — restricting to @" + ALLOWED_DOMAIN);
} else {
  console.log("Google OAuth NOT configured — dashboard is open (set GOOGLE_CLIENT_ID & GOOGLE_CLIENT_SECRET to enable)");
}

// ─── Auth Routes ────────────────────────────────────────────────
app.get("/auth/google", (req, res, next) => {
  if (!AUTH_ENABLED) return res.redirect("/");
  passport.authenticate("google", { scope: ["profile", "email"] })(req, res, next);
});

app.get("/auth/google/callback",
  (req, res, next) => {
    if (!AUTH_ENABLED) return res.redirect("/");
    passport.authenticate("google", { failureRedirect: "/auth/denied" })(req, res, next);
  },
  (req, res) => { res.redirect("/"); }
);

app.get("/auth/denied", (req, res) => {
  res.status(403).send(`
    <!DOCTYPE html><html><head><title>Access Denied</title>
    <style>body{font-family:-apple-system,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#1a1a2e;color:#e0e0e0;}
    .box{text-align:center;padding:40px;border:1px solid #333;border-radius:12px;background:#16213e;max-width:400px;}
    h1{color:#e74c3c;font-size:20px;margin-bottom:16px;}
    a{color:#3498db;text-decoration:none;}</style></head>
    <body><div class="box"><h1>Access Denied</h1>
    <p>Only <strong>@${ALLOWED_DOMAIN}</strong> accounts are allowed.</p>
    <p style="margin-top:20px;"><a href="/auth/google">Try another account</a></p>
    </div></body></html>
  `);
});

app.get("/auth/logout", (req, res) => {
  req.logout(() => {
    req.session.destroy(() => {
      res.redirect("/");
    });
  });
});

app.get("/api/user", (req, res) => {
  if (req.isAuthenticated && req.isAuthenticated()) {
    return res.json({ authenticated: true, user: req.user });
  }
  res.json({ authenticated: false });
});

// ─── Auth middleware — protect everything below ─────────────────
function requireAuth(req, res, next) {
  if (!AUTH_ENABLED) return next(); // no OAuth configured = open access
  if (req.isAuthenticated && req.isAuthenticated()) return next();
  // API routes return 401, page routes redirect to login
  if (req.path.startsWith("/api/")) {
    return res.status(401).json({ error: "Unauthorized — please sign in" });
  }
  return res.redirect("/auth/google");
}

// Serve static files — protected
app.use(requireAuth, express.static(path.join(__dirname, "public")));

// ─── API Routes ─────────────────────────────────────────────────

// GET /api/deals — serves cached data or fetches fresh
app.get("/api/deals", requireAuth, async (req, res) => {
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
app.post("/api/deals/refresh", requireAuth, async (req, res) => {
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

// Method A: v3 CRM Search for a single object type (notes, calls, tasks)
async function fetchV3Engagements(objectType, sinceMs) {
  const url = `https://api.hubapi.com/crm/v3/objects/${objectType}/search`;
  const headers = {
    "Authorization": "Bearer " + HUBSPOT_TOKEN,
    "Content-Type": "application/json"
  };

  let allResults = [];
  let after = null;

  for (let page = 0; page < 30; page++) {
    const body = {
      filterGroups: [
        { filters: [{ propertyName: "hs_createdate", operator: "GTE", value: String(sinceMs) }] }
      ],
      properties: ["hs_createdate", "hs_timestamp", "hubspot_owner_id"],
      sorts: [{ propertyName: "hs_createdate", direction: "DESCENDING" }],
      limit: 100
    };
    if (after) body.after = after;

    let response = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      response = await fetch(url, { method: "POST", headers, body: JSON.stringify(body) });
      if (response.status === 429) { await sleep(Math.pow(2, attempt) * 1000); continue; }
      break;
    }

    if (!response || !response.ok) {
      const text = response ? await response.text().catch(() => "") : "";
      console.error(`v3 search ${objectType} error: ${response ? response.status : "?"} ${text.substring(0, 200)}`);
      break;
    }

    const data = await response.json();
    (data.results || []).forEach(r => {
      const p = r.properties || {};
      const ts = p.hs_timestamp || p.hs_createdate;
      const ownerId = p.hubspot_owner_id;
      if (ts && ownerId) {
        allResults.push({
          id: r.id,
          ownerId: String(ownerId),
          timestamp: new Date(ts).getTime(),
          type: objectType,
          url: r.url || null
        });
      }
    });

    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else {
      break;
    }
    await sleep(150);
  }

  console.log(`  -> ${objectType}: ${allResults.length} results`);
  return allResults;
}

// Method B: v1 recent/modified (catches emails, meetings, and anything v3 misses)
async function fetchV1RecentEngagements(sinceMs) {
  const headers = { "Authorization": "Bearer " + HUBSPOT_TOKEN };
  let allResults = [];
  let offset = 0;

  for (let page = 0; page < 30; page++) {
    const url = `https://api.hubapi.com/engagements/v1/engagements/recent/modified?count=100&offset=${offset}&since=${sinceMs}`;

    let response = null;
    for (let attempt = 0; attempt < 3; attempt++) {
      response = await fetch(url, { headers });
      if (response.status === 429) { await sleep(Math.pow(2, attempt) * 1000); continue; }
      break;
    }

    if (!response || !response.ok) {
      const text = response ? await response.text().catch(() => "") : "";
      console.error(`v1 recent error: ${response ? response.status : "?"} ${text.substring(0, 200)}`);
      break;
    }

    const data = await response.json();
    (data.results || []).forEach(eng => {
      const e = eng.engagement || {};
      const assoc = eng.associations || {};
      if (e.ownerId && e.timestamp && e.timestamp >= sinceMs) {
        allResults.push({
          id: String(e.id),
          ownerId: String(e.ownerId),
          timestamp: e.timestamp,
          type: (e.type || "unknown").toLowerCase(),
          dealIds: (assoc.dealIds || []).map(String)
        });
      }
    });

    if (data.hasMore && data.offset) {
      offset = data.offset;
    } else {
      break;
    }
    await sleep(150);
  }

  console.log(`  -> v1 recent: ${allResults.length} results`);
  return allResults;
}

// In-memory activity cache
let activityCache = { data: null, timestamp: 0 };
const ACTIVITY_CACHE_TTL = 30 * 60 * 1000; // 30 min

// GET /api/activity — drafter activity heatmap data
app.get("/api/activity", requireAuth, async (req, res) => {
  try {
    if (!HUBSPOT_TOKEN) {
      return res.status(500).json({ error: "HUBSPOT_API_KEY not set" });
    }

    const now = Date.now();
    if (activityCache.data && (now - activityCache.timestamp) < ACTIVITY_CACHE_TTL) {
      return res.json({ ...activityCache.data, _cachedAt: String(activityCache.timestamp) });
    }

    console.log("[" + new Date().toISOString() + "] Fetching activity data (v3 search + v1 recent)...");
    const daysBack = 14;
    const sinceMs = now - (daysBack * 24 * 60 * 60 * 1000);

    // Fetch from multiple sources in parallel
    const [notes, calls, tasks, v1Recent, ownerMap] = await Promise.all([
      fetchV3Engagements("notes", sinceMs).catch(e => { console.error("notes error:", e.message); return []; }),
      fetchV3Engagements("calls", sinceMs).catch(e => { console.error("calls error:", e.message); return []; }),
      fetchV3Engagements("tasks", sinceMs).catch(e => { console.error("tasks error:", e.message); return []; }),
      fetchV1RecentEngagements(sinceMs).catch(e => { console.error("v1 recent error:", e.message); return []; }),
      fetchOwners()
    ]);

    // Normalize type names (v3 uses plural "notes", v1 uses singular "note")
    const normalizeType = t => {
      const map = { notes: "Note", note: "Note", calls: "Call", call: "Call",
        tasks: "Task", task: "Task", email: "Email", incoming_email: "Email",
        forwarded_email: "Email", meeting: "Meeting" };
      return map[(t || "").toLowerCase()] || (t || "Unknown").charAt(0).toUpperCase() + (t || "").slice(1).toLowerCase();
    };

    // Merge and deduplicate (v1 may overlap with v3)
    const seen = new Set();
    const activities = [];
    [...notes, ...calls, ...tasks, ...v1Recent].forEach(a => {
      a.type = normalizeType(a.type);
      // Round timestamp to nearest minute for better dedup (v1 and v3 may differ by ms)
      const roundedTs = Math.round(a.timestamp / 60000) * 60000;
      const key = a.ownerId + "_" + roundedTs + "_" + a.type;
      if (!seen.has(key)) {
        seen.add(key);
        activities.push(a);
      }
    });

    console.log("[" + new Date().toISOString() + "] Total activities: " + activities.length +
      " (notes:" + notes.length + " calls:" + calls.length + " tasks:" + tasks.length + " v1:" + v1Recent.length + ")");

    const data = { activities, ownerMap };
    activityCache = { data, timestamp: now };

    res.json({ ...data, _cachedAt: String(now) });
  } catch (err) {
    console.error("Error fetching activity:", err.message);
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
app.get("*", requireAuth, (req, res) => {
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
