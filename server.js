const express = require("express");
const fetch = require("node-fetch");
const path = require("path");
const session = require("express-session");
const passport = require("passport");
const GoogleStrategy = require("passport-google-oauth20").Strategy;
const { google } = require("googleapis");

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

// Admin allowlist (comma-separated emails in ADMIN_EMAILS env var).
// If unset, admin endpoints are blocked for everyone (safer default).
const ADMIN_EMAILS = new Set(
  (process.env.ADMIN_EMAILS || "")
    .split(",").map(e => e.trim().toLowerCase()).filter(Boolean)
);

const PROPERTIES = [
  "hs_v2_date_entered_1223620771", "hs_v2_date_exited_1223620771",
  "hs_v2_date_entered_1223620775", "hs_v2_date_entered_1223620777",
  "hs_v2_date_entered_1223620773", "hs_v2_date_exited_1223620773",
  "hs_v2_date_entered_1223620772",
  "hs_v2_date_entered_1223620778",
  "hs_v2_date_entered_1223620774", "hs_v2_date_exited_1223620774",
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
  "hs_v2_date_entered_1223751329", "hs_v2_date_exited_1223751329",
  "hs_v2_date_entered_1223751330",
  "ep_lead_source", "date_of_appointment",
  // Coral SLA tab: lead source tier 3 ("Coral Insurance" / similar) plus
  // paid_line_items_summary (a HubSpot multi-line text populated via the
  // printing workflow). Free-text values look like:
  //   "Severance of tenancy x1 150.00\nCouple Will with Trust x1 750.00"
  // so Will / LPA detection just substring-matches the raw string.
  "lead_source_tier_3", "paid_line_items_summary",
  // SLA breach reason (surfaced in Macmillan + Coral tables).
  "sla_breach_reason",
  "date_drafting_query",
  "have_they_signed_the_14_day_waiver_",
  // Date the 14-day waiver expires. When the customer hasn't signed the
  // waiver, this date drives the SLA extension dynamically (rather than
  // the previous flat +14 days). Fallback when missing: DI entry + 14d.
  "waiver_expiry_date",
  "consultant_query_reason",
  // Estate Planning Consultant (the person who runs the appointment).
  // Used by Chart 18 ("Consultant Query Rate per EPC") to attribute
  // queries back to the consultant whose ambiguous instructions caused
  // them. Owner-type field — resolved via ownerMap.
  "will_consultant",
  "legacy_advisor__owner",
  // Legacy Advisor → Macmillan follow-up queue: deals 6 months past
  // appointment, region IS NOT Scotland, non-urgent.
  //
  // The legacy single-field tracker (confirmed_will_has_been_signed__macmillan__v2)
  // is being replaced by three dedicated call-attempt properties so we can
  // track the 3-attempt SLA explicitly. During the transition we read both —
  // a deal drops out of the queue if EITHER the legacy field is set OR any
  // of the three new calls is marked "Yes". Once HubSpot data is fully
  // migrated, the v2 property can be removed from this list.
  "region",
  "confirmed_will_has_been_signed__macmillan__v2",
  "macmillan_call_1", "macmillan_call_2", "macmillan_call_3",
  // Macmillan Service KPIs (Reporting → SLA Audit → Macmillan tab).
  // Cohort is filtered by createdate. Service-time anchor is the cumulative
  // time the deal spent in the "Pending - Macmillan" stage (112034598).
  // first_connected_call_date is the HubSpot first-connected-call timestamp.
  "createdate",
  "hs_v2_cumulative_time_in_112034598",
  "first_connected_call_date",
  "hs_v2_date_entered_1223620776", "hs_v2_date_exited_1223620776",
  // Cumulative time in completion stages (used by audit heuristic when
  // original_date_entered_<stage> isn't populated — see processDeals).
  "hs_v2_cumulative_time_in_1223620775",
  "hs_v2_cumulative_time_in_1223620777",
  "hs_v2_cumulative_time_in_1223620778"
];

// ─── Deactivated drafter exclusion list ─────────────────────────
// Map of { dealId → originalDrafterName } for deals that had drafting queries
// under a now-deactivated drafter's ownership. If the deal has since been
// reassigned, those queries should NOT count against the new owner.
// Reads from two sources (merged):
//   - drafter-exclusions.json  (current format, supports multiple drafters)
//   - komal-exclusions.json    (legacy format, Komal only)
const fs = require("fs");
const DRAFTER_EXCLUSIONS = {}; // dealId → originalDrafterName

try {
  const exPath = path.join(__dirname, "drafter-exclusions.json");
  const exData = JSON.parse(fs.readFileSync(exPath, "utf8"));
  (exData.drafters || []).forEach(drafter => {
    (drafter.deals || []).forEach(d => {
      if (d.recordId) DRAFTER_EXCLUSIONS[String(d.recordId)] = drafter.name;
    });
  });
  const names = (exData.drafters || []).map(d => `${d.name} (${(d.deals || []).length})`).join(", ");
  console.log(`[Drafter exclusions] Loaded from drafter-exclusions.json: ${names || "none"}`);
} catch (e) {
  console.log("[Drafter exclusions] No drafter-exclusions.json found — skipping");
}

try {
  const exPath = path.join(__dirname, "komal-exclusions.json");
  const exData = JSON.parse(fs.readFileSync(exPath, "utf8"));
  let added = 0;
  (exData.deals || []).forEach(d => {
    if (d.recordId && !DRAFTER_EXCLUSIONS[String(d.recordId)]) {
      DRAFTER_EXCLUSIONS[String(d.recordId)] = "Komal Singh";
      added++;
    }
  });
  console.log(`[Drafter exclusions] Loaded ${added} legacy Komal Singh deal IDs from komal-exclusions.json`);
} catch (e) {
  // legacy file is optional — fine if absent
}

console.log(`[Drafter exclusions] Total deals excluded when reassigned: ${Object.keys(DRAFTER_EXCLUSIONS).length}`);

// ─── Google Sheets (target overrides persistence) ───────────────
const SHEETS_ID = process.env.GOOGLE_SHEETS_ID;
const SHEETS_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON;
const SHEETS_ENABLED = !!(SHEETS_ID && SHEETS_SERVICE_ACCOUNT_JSON);
const DEFAULT_TARGETS = {
  drafting: {
    default: 80,
    overrides: { "Komal Singh": 60, "Liam Tuapola": 60, "Gurleen Sagoo": 30, "Erin James": 48 }
  },
  proofreading: {
    default: 425,
    overrides: { "Neelam Ranchhod": 425 }
  }
};

let sheetsClient = null;
function getSheetsClient() {
  if (sheetsClient) return sheetsClient;
  if (!SHEETS_ENABLED) return null;
  try {
    const creds = JSON.parse(SHEETS_SERVICE_ACCOUNT_JSON);
    const auth = new google.auth.JWT({
      email: creds.client_email,
      key: creds.private_key,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"]
    });
    sheetsClient = google.sheets({ version: "v4", auth });
    return sheetsClient;
  } catch (e) {
    console.error("[Sheets] Failed to init client:", e.message);
    return null;
  }
}

async function ensureSheetTabsExist() {
  const sheets = getSheetsClient();
  if (!sheets) return;
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SHEETS_ID });
    const existing = new Set((meta.data.sheets || []).map(s => s.properties.title));
    const toCreate = ["Drafting", "Proofreading", "Logins"].filter(n => !existing.has(n));
    if (toCreate.length) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SHEETS_ID,
        requestBody: {
          requests: toCreate.map(title => ({ addSheet: { properties: { title } } }))
        }
      });
      // Seed new tabs with headers + defaults
      for (const title of toCreate) {
        let rows;
        if (title === "Logins") {
          rows = [["Timestamp", "Email", "Name", "IP"]];
        } else {
          const key = title.toLowerCase();
          const def = DEFAULT_TARGETS[key];
          rows = [
            ["Name", "Target"],
            ["_default", def.default],
            ...Object.entries(def.overrides).map(([n, v]) => [n, v])
          ];
        }
        await sheets.spreadsheets.values.update({
          spreadsheetId: SHEETS_ID,
          range: `${title}!A1`,
          valueInputOption: "RAW",
          requestBody: { values: rows }
        });
      }
      console.log(`[Sheets] Created and seeded tabs: ${toCreate.join(", ")}`);
    }
  } catch (e) {
    console.error("[Sheets] ensureSheetTabsExist error:", e.message);
  }
}

async function appendLoginToSheet({ timestamp, email, name, ip }) {
  const sheets = getSheetsClient();
  if (!sheets) return;
  try {
    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEETS_ID,
      range: "Logins!A:D",
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: [[timestamp, email, name || "", ip || ""]] }
    });
  } catch (e) {
    console.error("[Sheets] Login log error:", e.message);
  }
}

async function readTargetsFromSheet() {
  const sheets = getSheetsClient();
  if (!sheets) return null;
  try {
    const res = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: SHEETS_ID,
      ranges: ["Drafting!A2:B", "Proofreading!A2:B"]
    });
    const parseTab = (rows, fallback) => {
      let defaultVal = fallback.default;
      const overrides = {};
      (rows || []).forEach(row => {
        const name = (row[0] || "").trim();
        const val = parseInt(row[1], 10);
        if (!name || isNaN(val)) return;
        if (name === "_default") defaultVal = val;
        else overrides[name] = val;
      });
      return { default: defaultVal, overrides };
    };
    const [dr, pr] = res.data.valueRanges || [];
    return {
      drafting: parseTab(dr && dr.values, DEFAULT_TARGETS.drafting),
      proofreading: parseTab(pr && pr.values, DEFAULT_TARGETS.proofreading)
    };
  } catch (e) {
    console.error("[Sheets] read error:", e.message);
    return null;
  }
}

async function writeTargetsToSheet(payload) {
  const sheets = getSheetsClient();
  if (!sheets) throw new Error("Google Sheets not configured on server");
  const toRows = tab => [
    ["_default", Number.isFinite(tab.default) ? tab.default : ""],
    ...Object.entries(tab.overrides || {}).map(([k, v]) => [k, Number.isFinite(v) ? v : ""])
  ];
  // Clear data rows then write fresh
  await sheets.spreadsheets.values.batchClear({
    spreadsheetId: SHEETS_ID,
    requestBody: { ranges: ["Drafting!A2:B", "Proofreading!A2:B"] }
  });
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEETS_ID,
    requestBody: {
      valueInputOption: "RAW",
      data: [
        { range: "Drafting!A2", values: toRows(payload.drafting || DEFAULT_TARGETS.drafting) },
        { range: "Proofreading!A2", values: toRows(payload.proofreading || DEFAULT_TARGETS.proofreading) }
      ]
    }
  });
}

// ─── In-memory cache ─────────────────────────────────────────────
// Cache shape:
//   buffer:    Buffer holding the JSON.stringify()'d response body, ready
//              to ship to the client byte-for-byte. Pre-serialised once per
//              fetch (not per request) to avoid the per-request 20MB
//              JSON.stringify spike that was causing OOMs under concurrent
//              /api/deals load on the 512MB Render starter tier.
//   dealCount: deals.length, captured so we can log it without keeping
//              the giant V8 object tree resident alongside the Buffer.
//   timestamp: ms epoch when the Buffer was produced (== _cachedAt baked
//              into the Buffer; surfaced separately so cache-validity
//              checks don't need to parse the JSON back out).
let cache = { buffer: null, dealCount: 0, timestamp: 0 };
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour

// Serialise `data` once and store it as the bytes we'll ship on /api/deals.
// _cachedAt is baked into the buffer so each request just streams the
// pre-built bytes — no per-request stringify, no per-request object spread.
function setCachedData(data) {
  const now = Date.now();
  const wrapped = Object.assign({}, data, { _cachedAt: String(now) });
  const json = JSON.stringify(wrapped);
  cache = {
    buffer: Buffer.from(json, "utf8"),
    dealCount: (data && data.deals && data.deals.length) || 0,
    timestamp: now
  };
}

// Stream the cached bytes directly to the client. Multiple concurrent
// /api/deals callers share the same Buffer reference (no per-request copy
// of the 20MB payload), which is the whole reason we did this.
function serveCachedData(res) {
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Content-Length", cache.buffer.length);
  res.end(cache.buffer);
}

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

  // Extend the property list with hs_v2_date_entered_<stageId> for every
  // DO NOT USE stage we discovered at startup. If any of these are populated
  // on a deal, the deal has been in an archive pipeline and should be
  // excluded from the audit (see AuditDashboard).
  const properties = DO_NOT_USE_STAGE_IDS.length
    ? PROPERTIES.concat(DO_NOT_USE_STAGE_IDS.map(id => `hs_v2_date_entered_${id}`))
    : PROPERTIES;

  for (let page = 0; page < maxPages; page++) {
    const body = {
      filterGroups,
      properties,
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
  // Batch 1: pipeline stage filters (max 5) — catches deals that have ever
  // entered specific stages (workflow-stamped via hs_v2_date_entered_*).
  // Note: these timestamps survive cross-pipeline moves, so batch1/2 catch
  // deals that no longer live in the EP pipeline but historically passed
  // through these stages. Without them, audits of moved deals would miss.
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

  // (Former batch3 — dealstage IN [EP stage IDs] — was removed because it
  // is a strict subset of batch4: every deal with one of those dealstage
  // IDs is by definition in the EP pipeline (56009273), which batch4 already
  // catches. Eliminating it cut one full paginated HubSpot fetch per refresh
  // and reduced peak concurrent search-API calls.)

  // Batch 4: any deal in the Estate Planning pipeline modified within the
  // last RECENCY_MONTHS, regardless of stage. Catches post-completion stages
  // (Will Verified, Beacon Contacted, Closed Won, LPA with OPG, etc.) that
  // the Macmillan follow-up queue and audit need. Pipeline ID 56009273 =
  // Estate Planning.
  //
  // Why hs_lastmodifieddate (not createdate): Macmillan deals can have a
  // long lag between createdate (e.g. at referral) and the actual appointment
  // — sometimes 12+ months. Filtering on createdate dropped ~18 valid deals
  // with November 2025 appointments because their referral-era createdates
  // were older than the cutoff. hs_lastmodifieddate reflects "deal still
  // active in the workflow", which is a much better proxy for "we care
  // about this deal."
  //
  // Recency rationale: longest active dashboard window is the Macmillan
  // SLA at 6 months from appointment; drafting SLAs are days-to-weeks;
  // audit history typically <= 12 months. 18 months gives generous headroom
  // while excluding years of truly dormant Closed Won/Lost deals.
  //
  // Deals older than the cutoff that genuinely matter still come through
  // batch1/2 via their hs_v2_date_entered_<stage> timestamps.
  const RECENCY_MONTHS = 18;
  const recencyCutoff = new Date();
  recencyCutoff.setMonth(recencyCutoff.getMonth() - RECENCY_MONTHS);
  recencyCutoff.setHours(0, 0, 0, 0);
  const recencyCutoffMs = String(recencyCutoff.getTime());
  console.log(`[Fetch] batch4 recency filter: hs_lastmodifieddate >= ${recencyCutoff.toISOString().slice(0, 10)} (${RECENCY_MONTHS}mo)`);

  const batch4 = [
    { filters: [
      { propertyName: "pipeline", operator: "EQ", value: "56009273" },
      { propertyName: "hs_lastmodifieddate", operator: "GTE", value: recencyCutoffMs }
    ]}
  ];

  // Sequential, not parallel. HubSpot's CRM search API is rate-limited to
  // 4 req/sec. Running 3 batches in parallel — each paginating up to 100
  // pages — could fire ~12+ concurrent requests, triggering 429s. When two
  // cache-miss fetches overlapped (e.g. background refresh + a user click)
  // this doubled to ~24 concurrent requests and reliably 429'd, leaving the
  // dashboard unusable until cache eventually populated. Serialising the
  // batches gives a single in-flight paginated stream at any time, well
  // under the limit. Slower per refresh (~30-45s vs ~10s) but the in-flight
  // lock on fetchFreshData means callers share the same fetch anyway.
  const deals1 = await fetchDealsWithFilters(batch1);
  const deals2 = await fetchDealsWithFilters(batch2);
  const deals4 = await fetchDealsWithFilters(batch4);

  // Stream-merge into the seen Set — no spread, no temporary concat array
  // (previously briefly held 2x the data while merging).
  const seen = new Set();
  const merged = [];
  const addUnique = (arr) => arr.forEach(d => {
    if (!seen.has(d.id)) {
      seen.add(d.id);
      merged.push(d);
    }
  });
  addUnique(deals1);
  addUnique(deals2);
  addUnique(deals4);

  return merged;
}

// ─── Fetch HubSpot owners ──────────────────────────────────────
// Fetches both active AND archived (deactivated) owners so deals
// owned by deactivated users still resolve to a human-readable name.
// Without this, deactivated owners' deals fall through to the raw
// numeric owner ID and get filtered out / bucketed as "Unassigned"
// in the dashboard's drafter / proofreader charts.
async function fetchOwners() {
  const headers = { "Authorization": "Bearer " + HUBSPOT_TOKEN };
  const map = {};

  const fetchPage = async (archived) => {
    let after = null;
    for (let page = 0; page < 20; page++) {
      const params = new URLSearchParams({ limit: "500" });
      if (archived) params.set("archived", "true");
      if (after) params.set("after", after);
      const res = await fetch(`https://api.hubapi.com/crm/v3/owners?${params}`, { headers });
      if (!res.ok) return;
      const data = await res.json();
      (data.results || []).forEach(o => {
        const name = ((o.firstName || "") + " " + (o.lastName || "")).trim();
        // Mark archived owners so the UI / charts can optionally surface them.
        // Existing call sites just read the name string so this is transparent.
        map[String(o.id)] = (name || o.email || String(o.id)) + (archived ? " (Deactivated)" : "");
      });
      after = data.paging && data.paging.next && data.paging.next.after;
      if (!after) return;
    }
  };

  await fetchPage(false);
  await fetchPage(true);
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
// Returns BOTH the label map and the list of stage IDs we want to use as a
// "stop the audit from including this deal" signal. We only flag a narrow
// set of legacy completion stages from the archived "DO NOT USE" pipelines —
// these are the ones whose presence in a deal's history means the *real*
// drafting cycle happened in the old pipeline and the deal was later
// reallocated to Estate Planning (its Date of Appointment refers to the
// original cycle, not the EP cycle, so working-day calcs would explode).
// Earlier DO NOT USE stages like "Appointment scheduled" / "On Hold" are
// NOT flagged — a deal could pass through those without ever completing
// the old workflow.
const DO_NOT_USE_STAGE_LABELS = new Set([
  "instructions delivered",
  "documents ready",
  "documents with customer for review",
  "preparing documents",
  "appointment sat"
]);

async function fetchDealStageLabels() {
  const url = "https://api.hubapi.com/crm/v3/pipelines/deals";
  const response = await fetch(url, {
    headers: { "Authorization": "Bearer " + HUBSPOT_TOKEN }
  });
  if (!response.ok) return { labels: {}, doNotUseStageIds: [] };
  const data = await response.json();
  const labels = {};
  const doNotUseStageIds = [];
  (data.results || []).forEach(pipeline => {
    const pipelineLabel = pipeline.label || "";
    const pipelineIsDoNotUse = /DO\s*NOT\s*USE/i.test(pipelineLabel);
    (pipeline.stages || []).forEach(stage => {
      labels[stage.id] = stage.label;
      if (!pipelineIsDoNotUse) return;
      const normalized = (stage.label || "").toLowerCase().trim();
      if (DO_NOT_USE_STAGE_LABELS.has(normalized)) {
        doNotUseStageIds.push(stage.id);
      }
    });
  });
  return { labels, doNotUseStageIds };
}

// Cached at startup so we know which extra hs_v2_date_entered_<stageId>
// properties to request per-deal.
let DO_NOT_USE_STAGE_IDS = [];

// Pipeline / stage label discovery is expensive (~1-2 HubSpot calls plus
// label resolution) and the data rarely changes — stages get added /
// renamed at most a few times a year. Cache for 24h so we don't repeat
// it on every cold fetch. Previously this ran on every fetchFreshData
// invocation, which was visible as the repeated `[Pipelines] Discovered
// 5 DO NOT USE stage(s)` log line on every cache miss.
let _stageLabelsCache = null;
let _stageLabelsCachedAt = 0;
const STAGE_LABELS_TTL_MS = 24 * 60 * 60 * 1000;

async function getCachedStageLabels() {
  const now = Date.now();
  if (_stageLabelsCache && (now - _stageLabelsCachedAt) < STAGE_LABELS_TTL_MS) {
    return _stageLabelsCache;
  }
  const data = await fetchDealStageLabels().catch(() => ({ labels: {}, doNotUseStageIds: [] }));
  _stageLabelsCache = data;
  _stageLabelsCachedAt = now;
  return data;
}

// Resolve owner IDs that don't appear in the list endpoints (active or
// archived). Fully-removed HubSpot users sometimes drop out of both lists
// but can still be fetched individually with archived=true. Updates the
// given map in place. Capped concurrency so this stays cheap.
async function resolveMissingOwners(deals, ownerMap) {
  const idsToTry = new Set();
  const ownerFields = ["drafting_owner", "proof_reading__owner", "hubspot_owner_id", "legacy_advisor__owner"];
  deals.forEach(d => {
    const p = d.properties || {};
    ownerFields.forEach(field => {
      const v = p[field];
      if (v && /^\d+$/.test(String(v)) && !ownerMap[String(v)]) {
        idsToTry.add(String(v));
      }
    });
  });
  if (idsToTry.size === 0) return;

  const headers = { "Authorization": "Bearer " + HUBSPOT_TOKEN };
  const ids = Array.from(idsToTry);
  const CONCURRENCY = 5;
  let cursor = 0;
  const workers = Array.from({ length: CONCURRENCY }, async () => {
    while (cursor < ids.length) {
      const id = ids[cursor++];
      try {
        // Try archived=true first — it returns both archived and (in
        // practice) some removed users that the list endpoints omit.
        let res = await fetch(`https://api.hubapi.com/crm/v3/owners/${id}?archived=true`, { headers });
        if (!res.ok) res = await fetch(`https://api.hubapi.com/crm/v3/owners/${id}`, { headers });
        if (!res.ok) continue;
        const o = await res.json();
        const name = ((o.firstName || "") + " " + (o.lastName || "")).trim();
        ownerMap[id] = (name || o.email || id) + " (Deactivated)";
      } catch (e) { /* skip */ }
    }
  });
  await Promise.all(workers);
  const resolvedCount = ids.filter(id => ownerMap[id]).length;
  console.log(`[Owners] Resolved ${resolvedCount}/${ids.size} previously-unknown owner IDs via per-ID lookup`);
}

// ─── Fetch all fresh data ──────────────────────────────────────
// In-flight lock: if a fetch is already running, every subsequent caller
// joins the same Promise instead of starting their own. Previously, three
// concurrent /api/deals requests during a cache-miss window (background
// refresh + user load + retry) could spawn three full parallel fetches,
// each firing 12+ HubSpot search-API calls simultaneously and tripping
// the 4 req/sec rate limit. The 429 retry backoff then kept failing
// fetches resident in memory for 40+ seconds, compounding pressure and
// (we believe) triggering the Render OOM.
//
// With the lock, even a thundering herd of cache-miss callers produces
// exactly one in-flight fetch. They all await the same result.
let _fetchInFlight = null;

async function fetchFreshData() {
  if (_fetchInFlight) {
    console.log(`[${new Date().toISOString()}] Fetch already in flight — joining existing promise`);
    return _fetchInFlight;
  }
  _fetchInFlight = (async () => {
    try {
      const data = await _fetchFreshDataImpl();
      // Serialise the response into the cache Buffer *once* per fetch,
      // before any concurrent caller gets a chance to do its own stringify.
      // The handler then just streams cache.buffer; the in-memory V8 object
      // tree (`data`) is held only briefly by this IIFE scope and gets GC'd
      // once the Promise's awaiters complete.
      setCachedData(data);
      return data;
    } finally {
      _fetchInFlight = null;
    }
  })();
  return _fetchInFlight;
}

async function _fetchFreshDataImpl() {
  // Pipelines first — we need the DO NOT USE stage IDs to know which extra
  // per-deal properties to request. This is also what powers the audit's
  // "skip deals that have ever been in a DO NOT USE stage" filter.
  // Cached for 24h (see STAGE_LABELS_TTL_MS); pipeline structure rarely
  // changes and there's no value in re-fetching it on every cold deal pull.
  const stageData = await getCachedStageLabels();
  DO_NOT_USE_STAGE_IDS = stageData.doNotUseStageIds || [];
  if (DO_NOT_USE_STAGE_IDS.length) {
    console.log(`[Pipelines] ${DO_NOT_USE_STAGE_IDS.length} DO NOT USE stage(s) loaded — will check history per deal`);
  }

  const [deals, ownerMap, draftingOwnerOptions, proofOwnerOptions, queryReasonOptions, urgentReasonOptions, amendmentSourceOptions, leadSourceOptions, consultantQueryReasonOptions, legacyAdvisorOptions, waiverOptions, leadSourceTier3Options, slaBreachReasonOptions, regionOptions, macmillanFollowUpOptions] =
    await Promise.all([
      fetchAllDeals(),
      fetchOwners(),
      fetchPropertyOptions("drafting_owner").catch(() => ({})),
      fetchPropertyOptions("proof_reading__owner").catch(() => ({})),
      fetchPropertyOptions("drafting_query_reason").catch(() => ({})),
      fetchPropertyOptions("urgent_request_reason").catch(() => ({})),
      fetchPropertyOptions("amendment_source").catch(() => ({})),
      fetchPropertyOptions("ep_lead_source").catch(() => ({})),
      fetchPropertyOptions("consultant_query_reason").catch(() => ({})),
      fetchPropertyOptions("legacy_advisor__owner").catch(() => ({})),
      fetchPropertyOptions("have_they_signed_the_14_day_waiver_").catch(() => ({})),
      fetchPropertyOptions("lead_source_tier_3").catch(() => ({})),
      fetchPropertyOptions("sla_breach_reason").catch(() => ({})),
      fetchPropertyOptions("region").catch(() => ({})),
      fetchPropertyOptions("confirmed_will_has_been_signed__macmillan__v2").catch(() => ({}))
    ]);
  // paid_line_items_summary is multi-line text, not an enum, so there are
  // no options to resolve. Send an empty map for backwards compatibility
  // with processDeals's `productOptions` parameter.
  const productOptions = {};

  // Per-ID fallback for removed/deactivated owners that don't appear in
  // either list endpoint. Without this, deals owned by fully-removed users
  // fall through to the raw numeric ID and get bucketed as "Unassigned".
  await resolveMissingOwners(deals, ownerMap);

  return {
    deals,
    ownerMap,
    draftingOwnerOptions,
    proofOwnerOptions,
    queryReasonOptions,
    urgentReasonOptions,
    amendmentSourceOptions,
    leadSourceOptions,
    consultantQueryReasonOptions,
    legacyAdvisorOptions,
    waiverOptions,
    leadSourceTier3Options,
    productOptions,
    slaBreachReasonOptions,
    regionOptions,
    macmillanFollowUpOptions,
    stageLabels: stageData.labels || {},
    doNotUseStageIds: DO_NOT_USE_STAGE_IDS,
    dealExclusions: DRAFTER_EXCLUSIONS
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
  (req, res) => {
    if (req.user && req.user.email) {
      appendLoginToSheet({
        timestamp: new Date().toISOString(),
        email: req.user.email,
        name: req.user.name,
        ip: req.ip || (req.connection && req.connection.remoteAddress) || ""
      }).catch(e => console.error("[Sheets] Login append failed:", e.message));
    }
    res.redirect("/");
  }
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

function requireAdmin(req, res, next) {
  const email = (req.user && req.user.email || "").toLowerCase();
  if (!email || !ADMIN_EMAILS.has(email)) {
    if (req.path.startsWith("/api/")) {
      return res.status(403).json({ error: "Admin access required" });
    }
    return res.status(403).send(`
      <!DOCTYPE html><html><head><title>Admin only</title>
      <style>body{font-family:-apple-system,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#1a1a1a;color:#e0e0e0;}
      .box{text-align:center;padding:40px;border:1px solid #333;border-radius:12px;background:#242424;max-width:400px;}
      h1{color:#E74C3C;font-size:20px;margin-bottom:16px;}
      a{color:#3498db;text-decoration:none;}</style></head>
      <body><div class="box"><h1>Admin only</h1>
      <p>This page is restricted. Your account (${email || "(not signed in)"}) is not on the admin list.</p>
      <p style="margin-top:20px;"><a href="/">Back to dashboard</a></p>
      </div></body></html>
    `);
  }
  next();
}

// ─── TV display mode (unauth) ───────────────────────────────────
// Public-by-design read-only view at /tv showing a fixed set of charts
// for an office TV. No auth — anyone with the URL can view.
// Data exposed is the same as the authenticated /api/deals payload.
app.get("/tv", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/api/tv-deals", async (req, res) => {
  try {
    if (!HUBSPOT_TOKEN) return res.status(500).json({ error: "HUBSPOT_API_KEY not set" });
    const now = Date.now();
    if (cache.buffer && (now - cache.timestamp) < CACHE_TTL_MS) {
      return serveCachedData(res);
    }
    await fetchFreshData();
    serveCachedData(res);
  } catch (err) {
    console.error("/api/tv-deals error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// Serve static files — protected
app.use(requireAuth, express.static(path.join(__dirname, "public")));

// ─── API Routes ─────────────────────────────────────────────────

// GET /api/deals — serves cached Buffer or fetches fresh
app.get("/api/deals", requireAuth, async (req, res) => {
  try {
    if (!HUBSPOT_TOKEN) {
      return res.status(500).json({ error: "HUBSPOT_API_KEY environment variable is not set" });
    }

    const now = Date.now();
    if (cache.buffer && (now - cache.timestamp) < CACHE_TTL_MS) {
      return serveCachedData(res);
    }

    console.log("[" + new Date().toISOString() + "] Cache miss — fetching fresh data from HubSpot...");
    // fetchFreshData populates cache.buffer via setCachedData inside its
    // in-flight wrapper — see comment on _fetchInFlight. We don't need the
    // returned data object here; cache.buffer / cache.dealCount are the
    // source of truth from this point onwards.
    await fetchFreshData();
    console.log("[" + new Date().toISOString() + "] Fetched " + cache.dealCount + " deals, cached.");

    serveCachedData(res);
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
    // Bypass the cache-age check and force a new fetch. The in-flight lock
    // still de-dupes if multiple force-refresh requests overlap.
    await fetchFreshData();
    console.log("[" + new Date().toISOString() + "] Fetched " + cache.dealCount + " deals, cached.");

    serveCachedData(res);
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

// ─── Targets (persisted overrides via Google Sheets) ──────────
app.get("/api/targets", requireAuth, async (req, res) => {
  try {
    const data = await readTargetsFromSheet();
    if (!data) {
      return res.json({ ...DEFAULT_TARGETS, _source: SHEETS_ENABLED ? "error_fallback" : "code_defaults" });
    }
    res.json({ ...data, _source: "sheet" });
  } catch (e) {
    console.error("/api/targets GET error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post("/api/targets", requireAuth, async (req, res) => {
  if (!SHEETS_ENABLED) {
    return res.status(503).json({ error: "Google Sheets not configured on server (GOOGLE_SHEETS_ID + GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON required)" });
  }
  try {
    const body = req.body || {};
    if (!body.drafting || !body.proofreading) {
      return res.status(400).json({ error: "Request body must include drafting and proofreading objects" });
    }
    await writeTargetsToSheet(body);
    res.json({ ok: true });
  } catch (e) {
    console.error("/api/targets POST error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─── Admin: Login Activity ─────────────────────────────────────
app.get("/api/logins", requireAuth, requireAdmin, async (req, res) => {
  try {
    const sheets = getSheetsClient();
    if (!sheets) return res.status(503).json({ error: "Google Sheets not configured" });
    const data = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEETS_ID,
      range: "Logins!A2:D"
    });
    const logins = (data.data.values || []).map(r => ({
      timestamp: r[0] || "",
      email: r[1] || "",
      name: r[2] || "",
      ip: r[3] || ""
    }));
    res.json({ logins });
  } catch (e) {
    console.error("/api/logins error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get("/admin/logins", requireAuth, requireAdmin, (req, res) => {
  res.sendFile(path.join(__dirname, "admin", "logins.html"));
});

// ─── Admin: Inspect a single deal ──────────────────────────────
// Returns the deal's raw HubSpot properties plus selected computed values
// (matches what processDeals would produce client-side). Useful for
// diagnosing "why isn't this deal in the chart?" without hand-tracing
// every transform.
// GET /api/debug/deal/:id
app.get("/api/debug/deal/:id", requireAuth, requireAdmin, async (req, res) => {
  if (!HUBSPOT_TOKEN) return res.status(500).json({ error: "HUBSPOT_API_KEY not set" });
  const dealId = String(req.params.id || "").replace(/[^0-9]/g, "");
  if (!dealId) return res.status(400).json({ error: "Provide a numeric deal id" });
  const headers = { "Authorization": "Bearer " + HUBSPOT_TOKEN };

  // We need to list properties explicitly — HubSpot doesn't accept `*` as a
  // wildcard and without a `properties` parameter it returns only 3 default
  // fields. Build the request list from:
  //   1) The PROPERTIES we already fetch for the dashboard
  //   2) Any "drafts with customer" / DWC / RDWC related internal name
  //      discovered via the deal-properties metadata endpoint (so we catch
  //      mis-spelled or alternately-named variants of the same field).
  const baseProps = [...PROPERTIES,
    // Extra fields useful for diagnosing why a row is missing
    "createdate", "hs_lastmodifieddate", "hs_pipeline",
    "first_date_entered_drafting_instructions", "first_date_entered_drafts_with_customer",
    "hs_v2_date_entered_1223751329"
  ];

  // 1) Get the full list of deal properties so we can find any field whose
  //    label or internal name looks "DWC"-related.
  let dwcLikeNames = [];
  try {
    const metaRes = await fetch("https://api.hubapi.com/crm/v3/properties/deals", { headers });
    if (metaRes.ok) {
      const meta = await metaRes.json();
      dwcLikeNames = (meta.results || [])
        .filter(p => {
          const blob = `${p.name || ""} ${p.label || ""}`.toLowerCase();
          return blob.includes("drafts with customer") || blob.includes("drafts_with_customer") ||
                 blob.includes("dwc") || blob.includes("rdwc");
        })
        .map(p => p.name);
    }
  } catch (e) { /* best-effort */ }

  const propsToFetch = Array.from(new Set([...baseProps, ...dwcLikeNames]));

  try {
    const url = `https://api.hubapi.com/crm/v3/objects/deals/${dealId}?properties=${encodeURIComponent(propsToFetch.join(","))}&archived=false`;
    const r = await fetch(url, { headers });
    if (!r.ok) return res.status(r.status).json({ error: `HubSpot ${r.status}: ${await r.text()}` });
    const data = await r.json();
    const props = data.properties || {};

    const relevant = {};
    [
      "dealname", "dealstage", "pipeline", "hs_pipeline", "drafting_owner", "proof_reading__owner",
      "hs_v2_date_entered_1223620771", "hs_v2_date_exited_1223620771",
      "hs_v2_date_entered_1223620775", "hs_v2_date_entered_1223620777",
      "original_date_entered_drafting_instructions",
      "original_date_entered_drafts_with_customer",
      "original_date_entered_ready_for_printing",
      "original_date_entered_sent_to_customer",
      "first_date_exited_drafting_instructions",
      "date_of_appointment", "lead_source_tier_3", "ep_lead_source",
      "paid_line_items_summary", "ep_product", "have_they_signed_the_14_day_waiver_",
      "hs_priority", "amendment_source", "sla_breach_reason"
    ].forEach(k => { relevant[k] = props[k] !== undefined ? props[k] : null; });

    // All DWC-named properties (whether or not we already had them)
    const dwcLike = {};
    dwcLikeNames.forEach(n => { dwcLike[n] = props[n] !== undefined ? props[n] : null; });

    // Whether the deal is in the search-filter result set the dashboard
    // ingests. If false, the deal is invisible to the dashboard regardless
    // of its properties.
    const inDashboardFetchFilter = !!(
      props.hs_v2_date_entered_1223620771 || props.hs_v2_date_entered_1223620773 ||
      props.hs_v2_date_exited_1223620773 || props.hs_v2_date_entered_1223620775 ||
      props.hs_v2_date_entered_1223620777 || props.hs_v2_date_entered_1223751329
    );

    res.json({
      id: data.id,
      inDashboardFetchFilter,
      relevant,
      dwcLikePropertyValues: dwcLike,
      _propsRequestedCount: propsToFetch.length,
      _propsReturnedCount: Object.keys(props).length
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/debug/missing-entry-stamps
// Lists deals that have an exit timestamp on "Appointment Outcome (Estate
// Planning)" (hs_v2_date_exited_1223751329) but no entry timestamp
// (hs_v2_date_entered_1223751329 is null). This is the 329-deal class of
// bulk-imported / migrated deals that show up in Chart 13's count but get
// silently dropped from Chart 17's avg-turnaround calculation, because we
// can't compute "entry → exit" without an entry.
//
// Query params:
//   week  — filter to a specific Saturday-anchored week (YYYY-MM-DD)
//   limit — max deals in `deals` array (default 500). The `count` /
//           `byExitWeek` totals are NOT limited.
//
// Reads from the live cache (which is the same data the dashboard sees).
// Admin only — no point exposing per-deal owner names to all signed-in users.
app.get("/api/debug/missing-entry-stamps", requireAuth, requireAdmin, (req, res) => {
  if (!cache.buffer) {
    return res.status(503).json({ error: "Deals cache not populated yet — try again in a few seconds" });
  }
  const AO_STAGE = "1223751329";
  const ENTRY_PROP = `hs_v2_date_entered_${AO_STAGE}`;
  const EXIT_PROP = `hs_v2_date_exited_${AO_STAGE}`;

  try {
    // Parse the cached buffer back into a usable object. Done on demand
    // because this endpoint is admin-only / low-frequency.
    const cached = JSON.parse(cache.buffer.toString("utf8"));
    const deals = cached.deals || [];
    const ownerMap = cached.ownerMap || {};
    const stageLabels = cached.stageLabels || {};

    const limit = Math.max(1, Math.min(parseInt(req.query.limit || "500", 10) || 500, 5000));
    const weekFilter = req.query.week || null;

    // Saturday-anchored week key matching the dashboard's WoW bucketing.
    // Saturday = day 6; offset = (dow + 1) % 7 days back to nearest Sat.
    const satWeekKey = (msStr) => {
      const ms = parseInt(msStr, 10);
      if (isNaN(ms)) return null;
      const d = new Date(ms);
      // Use UTC day-of-week for consistency on the server (no TZ drift).
      const dow = d.getUTCDay();
      const offset = (dow + 1) % 7;
      const sat = new Date(d);
      sat.setUTCDate(sat.getUTCDate() - offset);
      const y = sat.getUTCFullYear();
      const m = String(sat.getUTCMonth() + 1).padStart(2, "0");
      const day = String(sat.getUTCDate()).padStart(2, "0");
      return `${y}-${m}-${day}`;
    };

    const byExitWeek = {};
    const matches = [];
    const todayMs = Date.now();

    for (const d of deals) {
      const p = d.properties || {};
      const exitedRaw = p[EXIT_PROP];
      const enteredRaw = p[ENTRY_PROP];
      if (!exitedRaw) continue;     // didn't exit AO → not in scope
      if (enteredRaw) continue;     // has entry stamp → already counted in Chart 17

      const wk = satWeekKey(exitedRaw);
      if (!wk) continue;
      byExitWeek[wk] = (byExitWeek[wk] || 0) + 1;
      if (weekFilter && wk !== weekFilter) continue;

      const exitMs = parseInt(exitedRaw, 10);
      const daysAgo = Math.round((todayMs - exitMs) / 86400000);
      const ownerId = p.drafting_owner;
      matches.push({
        id: d.id,
        dealname: p.dealname,
        dealstage: p.dealstage,
        stageLabel: stageLabels[p.dealstage] || p.dealstage || null,
        drafter: ownerId ? (ownerMap[ownerId] || ownerId) : null,
        exitedAt: new Date(exitMs).toISOString().slice(0, 10),
        exitedWeek: wk,
        daysAgo,
        hubspotLink: `https://app.hubspot.com/contacts/4385478/record/0-3/${d.id}`
      });
    }

    // Total count is across ALL weeks even when filtering, so the user can
    // see "this week is X of total Y" without re-querying.
    const totalCount = Object.values(byExitWeek).reduce((s, v) => s + v, 0);

    // Newest exits first — usually what you want for backfilling.
    matches.sort((a, b) => b.exitedAt.localeCompare(a.exitedAt));

    res.json({
      count: totalCount,
      cacheAge: cache.timestamp ? Math.round((Date.now() - cache.timestamp) / 1000) + "s" : null,
      cachedDealsScanned: deals.length,
      byExitWeek,
      weekFilter,
      limitApplied: limit,
      returnedDealsCount: Math.min(matches.length, limit),
      deals: matches.slice(0, limit)
    });
  } catch (e) {
    console.error("[/api/debug/missing-entry-stamps] error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// Health check
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    cached: !!cache.buffer,
    cacheAge: cache.timestamp ? Math.round((Date.now() - cache.timestamp) / 1000) + "s" : null,
    dealCount: cache.dealCount,
    cachedBytes: cache.buffer ? cache.buffer.length : 0
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
    // fetchFreshData internally calls setCachedData — the buffer is
    // populated before this resolves. No need to reassign cache here.
    await fetchFreshData();
    console.log("[" + new Date().toISOString() + "] Background refresh done — " + cache.dealCount + " deals (" + cache.buffer.length + " bytes cached).");
  } catch (err) {
    console.error("Background refresh failed:", err.message);
  }
}

// ─── Start server ───────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Dashboard server running on port ${PORT}`);
  if (SHEETS_ENABLED) {
    console.log("[Sheets] Targets persistence enabled (spreadsheet: " + SHEETS_ID + ")");
    ensureSheetTabsExist();
  } else {
    console.log("[Sheets] Targets persistence DISABLED — set GOOGLE_SHEETS_ID and GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON to enable");
  }

  // Initial data fetch on startup
  backgroundRefresh();

  // Refresh every hour
  setInterval(backgroundRefresh, CACHE_TTL_MS);
});
