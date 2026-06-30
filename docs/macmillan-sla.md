# Macmillan SLA Reporting — Engineering Reference

Technical reference for the Macmillan SLA logic implemented in the Drafting Dashboard.
Covers cohort definitions, per-row classification, compliance % calculation, and the
exact HubSpot properties / stage IDs each tab reads.

All references are to `public/index.html` unless prefixed `server.js`. Line numbers
are accurate as of `1ba9503` and may drift — search by symbol if they're stale.

---

## 1. Architecture overview

### Data flow

```
HubSpot CRM Search API
        │
        ▼  fetchAllDeals (server.js ~line 540)
        │  3 batches → dedup by id → ~22.5K deals
        ▼
   cache.buffer (in-memory, 1h TTL)
        │
        ▼  GET /api/deals
        │
        ▼  processDeals (public/index.html line 316)
        │  raw HubSpot props → enriched deal objects
        │  (parses dates, resolves owners, computes derived fields)
        ▼
   React state `deals` (AuditDashboard line 5109)
        │
        ▼  Three useMemos compute per-tab cohorts:
        │  - allRows  → Drafting tab (line 5187)
        │  - macmillanKPIs / epaRows → EPA tab (lines 5552, 5639)
        │  - macmillanCallStats / csRows → CS tab (lines 5476, 5681)
        ▼
   Rendered tables + tiles
```

### Pipeline + key stage IDs (Estate Planning pipeline `56009273`)

| Stage ID | Label | Used by |
|---|---|---|
| `1223620771` | Drafting Instructions (DI) | All tabs (SLA anchor) |
| `1223620772` | Drafting Query | Workload chart |
| `1223620773` | Required — Proofreading (RP) | Workload chart |
| `1223620774` | Approved — Proofreading (AP) | Workload chart |
| `1223620775` | Drafts With Customer (DWC) | Drafting SLA end date |
| `1223620776` | Replied — DWC (RDWC) | Legacy Advisor metrics |
| `1223620777` | Ready For Printing (RFP) | Drafting SLA end date |
| `1223620778` | Sent To Customer (STC) | Drafting SLA end date |
| `1338772492` | Amendment Requested | Workload chart |
| `1223751329` | Appointment Outcome | Chart 17 |
| `112034598` | Pending — Macmillan | EPA KPI 4, 5, 6 |
| `1230854698` | Attempting to contact | EPA KPI 6 |

### Macmillan cohort (shared by all tabs)

A deal is "a Macmillan deal" when:

```js
d.leadSources.includes("Macmillan")
```

`leadSources` is derived in `processDeals` from `ep_lead_source` (multi-select enum,
resolved via the property's options map).

The CS tab also references `lead_source_tier_3` for Coral, but Macmillan is always
on `ep_lead_source`.

---

## 2. EPA tab — Macmillan Service KPIs

Renders four tiles (KPI 4, 5, 6, 7) plus a per-deal table. Source: `epaRows`
(line 5639) and `macmillanKPIs` (line 5552).

### Shared cohort filter

```js
const start = monthStart(fromMonth);
const end   = monthEnd(toMonth);
const inCohort = d =>
  d.leadSources.includes("Macmillan") &&
  d.createDate &&
  d.createDate >= start &&
  d.createDate <= end;
```

Cohort anchor = `createdate` (HubSpot deal creation timestamp).

### KPI 4 — Initial contact (2d)

**Rule**: Provider to contact Beneficiary by phone within 2 days of Macmillan referral.

```js
// processDeals (line 339)
const pendingMacmillanMs   = parseFloat(p.hs_v2_cumulative_time_in_112034598);
const pendingMacmillanDays = isNaN(pendingMacmillanMs) ? null : (pendingMacmillanMs / 86400000);

// macmillanKPIs (line 5552)
k4Total++;  // every cohort deal counted
if (d.pendingMacmillanDays != null && d.pendingMacmillanDays > 3) k4Fail++;
```

**Fail condition**: `pendingMacmillanDays > 3`. The threshold is **3 days, not 2**
— mirrors the source spreadsheet's `>3d` cell rule (1 day grace beyond the 2-day
SLA before flagging).

**Per-row status** (`epaRows` line 5650):
```js
k4Status = pendingMacmillanDays == null ? "N/A"
         : pendingMacmillanDays > 3    ? "Failed"
         :                                "Met";
```

### KPI 5 — Urgent contact (24h)

**Rule**: Urgent wills phone call within 24 hours.

**Cohort subset**: urgent only. A deal is urgent when:
```js
const isUrgent = d.isUrgent ||
                 (Array.isArray(d.urgentReason) && d.urgentReason.length > 0);
```

`d.isUrgent = (p.hs_priority === "high")`. `urgentReason` from `urgent_request_reason`
(multi-select enum).

**Fail condition** (line 5577): `pendingMacmillanDays > 2`. Again the threshold is
1 day beyond the SLA wording (24h SLA → flag at >2d).

**Per-row status**:
```js
k5Status = !isUrgentForKPI                  ? "N/A"
         : d.pendingMacmillanDays == null   ? "N/A"
         : d.pendingMacmillanDays > 2       ? "Failed"
         :                                    "Met";
```

### KPI 6 — Two further call attempts (5d)

**Rule**: If no answer on initial call, Provider makes two further attempts within 5
working days.

```js
// processDeals (~line 419)
const pendingExitedDate    = parseHSDate(p.hs_v2_date_exited_112034598);
const attemptingExitedDate = parseHSDate(p.hs_v2_date_exited_1230854698);

// macmillanKPIs (~line 5590)
k6Total++;
if (!d.pendingExitedDate || !d.attemptingExitedDate) k6Fail++;
```

**Fail condition**: either `pendingExitedDate` OR `attemptingExitedDate` is missing.

**Note**: this is a **structural proxy**, not a literal "5 working days" check. The
workflow only advances a deal past both contact stages once it has run the full
callback journey — so the existence of both stage exits implies ≥2 call attempts
were logged. A deal that took 30 days to clear both stages still passes structurally.
If Ops needs the strict `≤5 biz day gap between the two exits`, change the
`if(!pendingExitedDate || !attemptingExitedDate)` to a `calcBusinessDaysUK` check.

### KPI 7 — Follow-up email

**Rule**: If still no contact, Provider sends a follow-up email after the Pending
stage exit.

```js
// processDeals (~line 423)
const customerEmailMacmillan = (p.customer_email__macmillan_ || "").trim();

// macmillanKPIs (~line 5602)
k7Total++;
if (!d.pendingExitedDate || !d.customerEmailMacmillan) k7Fail++;
```

**Fail condition**: missing Pending exit OR missing Macmillan customer email.

Replaced an earlier `first_connected_call_date`-based rule (commit `f87dc8d`)
because that property wasn't populated on Macmillan deals — every row was failing.

### Compliance % math (tiles)

```js
pctFail = total > 0 ? Math.round((fail / total) * 100) : null;
```

Tile colour scale (all four KPIs):
- green: `pct ≤ 5`
- amber: `pct ≤ 10`
- red:   `pct > 10`

---

## 3. Drafting tab — Standard / Urgent SLA

The Drafting tab's per-deal table and Standard / Urgent Compliance tiles. Source:
`allRows` (line 5187) → `rows` (filtered by leadFilter) → `stats` (the tile math).

### Cohort filter

```js
// allRows (line 5187)
const start = monthStart(fromMonth);
const end   = monthEnd(toMonth);

deals.forEach(d => {
  if (!d.dateOfAppointment) return;
  if (d.wasInDoNotUse) return;                              // archived pipeline history
  if (!d.origEnteredDrafting && !d.enteredDrafting) return; // never entered DI

  // End = earliest of firstDWC, firstRFP, firstSentToCustomer
  const candidates = [];
  if (d.firstDWC)            candidates.push(d.firstDWC.getTime());
  if (d.firstRFP)            candidates.push(d.firstRFP.getTime());
  if (d.firstSentToCustomer) candidates.push(d.firstSentToCustomer.getTime());
  if (!candidates.length) return;
  const endDate = new Date(Math.min(...candidates));

  if (start && endDate < start) return;
  if (end   && endDate > end)   return;
  // ... classification continues
});
```

**Cohort anchor**: `endDate` (NOT `dateOfAppointment`). A deal appears in the
selected month's tile/audit if it **completed** within that month. Compare this
with the EPA tab which uses `createdate`.

### `firstDWC` / `firstRFP` / `firstSentToCustomer` — `estimateFirstEntry`

```js
// processDeals (line 342)
const estimateFirstEntry = (orig, entered, cumulative) => {
  if (orig) return orig;                                   // workflow stamp wins
  if (entered && cumulative > 0)
    return new Date(entered.getTime() - cumulative);       // heuristic fallback
  return null;
};

const firstDWC            = estimateFirstEntry(origDraftsWithCustomer, enteredDWC_v2, cumDWC);
const firstRFP            = estimateFirstEntry(origReadyForPrinting, enteredRFP_v2, cumRFP);
const firstSentToCustomer = estimateFirstEntry(origSentToCustomer, enteredSTC_v2, cumSTC);
```

HubSpot props read (per deal):

| Variable | Property |
|---|---|
| `origDraftsWithCustomer` | `original_date_entered_drafts_with_customer` |
| `origReadyForPrinting` | `original_date_entered_ready_for_printing` |
| `origSentToCustomer` | `original_date_entered_sent_to_customer` |
| `enteredDWC_v2` | `hs_v2_date_entered_1223620775` |
| `enteredRFP_v2` | `hs_v2_date_entered_1223620777` |
| `enteredSTC_v2` | `hs_v2_date_entered_1223620778` |
| `cumDWC` | `hs_v2_cumulative_time_in_1223620775` |
| `cumRFP` | `hs_v2_cumulative_time_in_1223620777` |
| `cumSTC` | `hs_v2_cumulative_time_in_1223620778` |

The heuristic is wrong for re-entry deals (e.g. DWC → RDWC → DWC after an amendment
cycle): cumulative time grows while `entered` jumps to the re-entry stamp, so the
heuristic understates the first-entry. Trust the workflow stamp when present — see
the comment block at processDeals line 343-360.

### SLA anchor + target

```js
const startDay = new Date(d.dateOfAppointment); startDay.setHours(0,0,0,0);
const endDay   = new Date(endDate);             endDay.setHours(0,0,0,0);

// MSO-only urgent → Standard
const isMSOReason = r => /management\s*sign[-\s]*off/i.test(String(r || "").trim());
const isMSOOnly   = d.isUrgent
                 && Array.isArray(d.urgentReason)
                 && d.urgentReason.length > 0
                 && d.urgentReason.every(isMSOReason);
const slaIsUrgent = d.isUrgent && !isMSOOnly;

// 14-day waiver anchor shift (Standard only — urgent unaffected)
let effectiveStartDay = startDay;
if (!slaIsUrgent && d.waiverSigned14d === false) {
  let waiverAnchor = null;
  if (d.waiverExpiryDate) {
    waiverAnchor = new Date(d.waiverExpiryDate); waiverAnchor.setHours(0,0,0,0);
  } else if (d.origEnteredDrafting) {
    waiverAnchor = new Date(new Date(d.origEnteredDrafting).getTime() + 14 * 86400000);
    waiverAnchor.setHours(0,0,0,0);
  }
  if (waiverAnchor && waiverAnchor > startDay) effectiveStartDay = waiverAnchor;
}

const workingDays = endDay >= effectiveStartDay
  ? (calcBusinessDaysUK(effectiveStartDay, endDay) || 0)
  : null;

const baseTarget  = slaIsUrgent ? 3 : 20;  // working days
const met         = workingDays <= baseTarget;
```

**Working-days helper** `calcBusinessDaysUK` excludes weekends + UK England & Wales
bank holidays.

### MSO-only exception

Deals flagged urgent **only** because `urgent_request_reason` is "Management Sign
Off" are reclassified as **Standard** (20wd) rather than Urgent (3wd). Rationale:
MSO is admin paperwork urgency, not customer-driven time pressure. Such rows show
a "MSO → Standard" badge in the table.

### 14-day waiver — anchor shift, not target extension

When the customer **hasn't signed** the waiver:
- SLA clock **starts at the waiver expiry date** (or `origEnteredDrafting + 14
  calendar days` as fallback) — not at DoA
- Target stays 20 working days
- Anchor only shifts **forward** (never earlier than DoA)
- Affects Standard SLA only — Urgent is unaffected (rush jobs override the
  grace-period concept)

Source: `waiver_expiry_date` for the explicit expiry, `have_they_signed_the_14_day_waiver_`
for the signed flag (`waiverSigned14d === false` when "No").

### Excluded deals (SLA Breach Reason = N/A)

A **breached** deal whose `sla_breach_reason` is "N/A" (case-insensitive) is
flagged as **Excluded**:
- Kept in the audit table for audit trail
- **Removed from the compliance % denominator**

`Met` deals are always Met regardless of breach reason.

### Compliance % math (`stats` useMemo, ~line 5413)

```js
const standard      = rows.filter(r => !r.isUrgent);
const urgent        = rows.filter(r =>  r.isUrgent);
const stdMet        = standard.filter(r => r.displayMet).length;
const urgMet        = urgent.filter(r => r.displayMet).length;
const stdEligible   = standard.filter(r => !r.displayExcluded).length;
const urgEligible   = urgent.filter(r => !r.displayExcluded).length;
const stdPct        = stdEligible > 0 ? Math.round((stdMet / stdEligible) * 100) : null;
const urgPct        = urgEligible > 0 ? Math.round((urgMet / urgEligible) * 100) : null;
```

Worked example (May 2026): Standard 82/90 met = **91%**, Urgent 26/28 met = **93%**.
The 7 standard + 1 urgent excluded deals are in the cohort but not the denominator.

---

## 4. CS tab — Phone-Call SLA (6-month signed will)

Source: `macmillanCallStats` (line 5476) and `csRows` (line 5681). One tile
("Phone-Call SLA %") plus a per-deal table.

### Cohort filter

```js
// csRows (line 5681)
const today = new Date(); today.setHours(0,0,0,0);
const POST_DWC_STAGES = new Set(["1223620775","1223620776","1223620777","1223620778"]);

deals.forEach(d => {
  if (!d.leadSources.includes("Macmillan"))   return;
  if (!d.dateOfAppointment)                   return;
  if ((d.region || "").trim() === "Scotland") return;     // jurisdiction-specific

  const hasDraft = d.firstDWC || d.firstRFP || POST_DWC_STAGES.has(d.dealstage);
  if (!hasDraft) return;

  const doa      = new Date(d.dateOfAppointment);
  const deadline = new Date(doa.getFullYear(), doa.getMonth() + 6, doa.getDate());
  deadline.setHours(0,0,0,0);

  if (deadline < start || deadline > end) return;          // deadline in selected month
  if (deadline > today)                   return;          // only past-deadline deals
  // ... classification continues
});
```

**Cohort anchor**: `deadline = dateOfAppointment + 6 months` falls within the
selected month range, AND the deadline has already passed (`<= today`).

Exclusions:
- `region === "Scotland"` — different jurisdiction, different SLA
- Deals that have no evidence of a draft having been sent

### Compliance rule

```js
const c1 = d.macmillanCall1 || "";
const c2 = d.macmillanCall2 || "";
const c3 = d.macmillanCall3 || "";

// Legacy field — see "Legacy handling" below
const legacyYes = (d.macmillanFollowUpOutcome || "").trim().toLowerCase() === "yes";

const hasYes   = legacyYes || c1 === "yes" || c2 === "yes" || c3 === "yes";
const allNos   = c1 === "no" && c2 === "no" && c3 === "no";

const status = hasYes ? "Met (Yes)"
             : allNos ? "Met (3 No)"
             :          "Breached";
```

**Compliant when**:
- **any** of `macmillan_call_1/2/3` is "Yes" (signing confirmed), OR
- the legacy `confirmed_will_has_been_signed__macmillan__v2 = "Yes"` (see below), OR
- all three Call N = "No" (full 3-attempt effort logged)

**Breached when**: deadline has passed, no Yes from any source, and the Call N
fields are blank or mixed.

This is intentionally looser than the Macmillan Follow-Up Queue's "all 3 filled"
rule on Drafting → Legacy Advisor. The queue tracks **paper-trail completion**;
the audit measures **SLA outcomes**.

### Legacy field handling

Some deals were resolved before the `macmillan_call_1/2/3` schema rolled out.
Those deals only have the legacy field `confirmed_will_has_been_signed__macmillan__v2`.
A "Yes" there is treated as a compliant outcome (added in commit `2c39075`).

HubSpot property: `confirmed_will_has_been_signed__macmillan__v2` (resolved via
its options map → label string, then trimmed/lowercased for the compare).

### Compliance % math (`macmillanCallStats` useMemo, ~line 5476)

```js
total      = filteredDeals.length;
compliant  = filteredDeals.filter(isCompliant).length;
pct        = total > 0 ? Math.round((compliant / total) * 100) : null;
```

The tile colour scale mirrors the others (green ≤ 5% non-compliant, etc.).

---

## 5. Field reference

### HubSpot properties read for Macmillan SLA

Fetched in `server.js` `PROPERTIES` array.

| Property | Used by |
|---|---|
| `ep_lead_source` | Macmillan cohort filter (all tabs) |
| `lead_source_tier_3` | Coral cohort (not Macmillan, but shares filter UI) |
| `createdate` | EPA cohort anchor |
| `date_of_appointment` | Drafting + CS cohort anchor |
| `dealstage` | Cohort + classification |
| `hs_priority` | Urgent classification |
| `urgent_request_reason` | Urgent classification + MSO exception |
| `hs_v2_date_entered_1223620771` | DI entry (Drafting tab anchor fallback) |
| `hs_v2_date_entered_1223620775/7/8` | Heuristic firstDWC/RFP/STC |
| `hs_v2_cumulative_time_in_1223620775/7/8` | Heuristic firstDWC/RFP/STC |
| `original_date_entered_drafting_instructions` | Drafting tab anchor |
| `original_date_entered_drafts_with_customer` | firstDWC (preferred) |
| `original_date_entered_ready_for_printing` | firstRFP (preferred) |
| `original_date_entered_sent_to_customer` | firstSentToCustomer (preferred) |
| `hs_v2_cumulative_time_in_112034598` | EPA KPI 4 + 5 (Pending-Macmillan days) |
| `hs_v2_date_exited_112034598` | EPA KPI 6 + 7 (Pending-Macmillan exit) |
| `hs_v2_date_exited_1230854698` | EPA KPI 6 (Attempting-to-contact exit) |
| `customer_email__macmillan_` | EPA KPI 7 |
| `macmillan_call_1` / `_2` / `_3` | CS tab compliance |
| `confirmed_will_has_been_signed__macmillan__v2` | CS legacy compliance |
| `region` | CS tab (Scotland exclusion) |
| `waiver_expiry_date` | Drafting tab anchor shift |
| `have_they_signed_the_14_day_waiver_` | Drafting tab anchor shift |
| `sla_breach_reason` | Drafting tab Excluded classification |
| `drafting_owner` | Drafter name resolution |

### Stage IDs

See section 1 above. All stage IDs are in the Estate Planning pipeline (`56009273`).

### Server-side filter batches (`fetchAllDeals`, server.js ~line 540)

- **batch1**: deals with `HAS_PROPERTY` on `hs_v2_date_entered_1223620771/3 / exited 1223620773 / entered 1223620775/7`
- **batch2**: deals with `HAS_PROPERTY` on `hs_v2_date_entered_1223751329` (Appointment Outcome)
- **batch4**: `pipeline = 56009273 AND hs_lastmodifieddate >= 18 months ago`, sorted by `hs_lastmodifieddate DESC`

Dedup on `deal.id`. Total result set in cache ~22.5K deals.

---

## 6. Code call sites — quick index

| Symbol | Line | Role |
|---|---|---|
| `processDeals` | 316 | Raw HubSpot → enriched deal objects |
| `estimateFirstEntry` | 342 | firstDWC/RFP/STC fallback logic |
| `parseHSDate` | 177 | HubSpot date parsing (UTC-midnight re-anchoring) |
| `calcBusinessDaysUK` | 269 | Working-day math (excludes weekends + bank holidays) |
| `addBusinessDays` | 296 | Working-day forward arithmetic |
| `AuditDashboard` | 5109 | The SLA Audit page component |
| `allRows` (Drafting) | 5187 | Drafting cohort + classification |
| `stats` (compliance tiles) | ~5413 | Standard / Urgent Compliance % |
| `macmillanCallStats` | 5476 | CS tab compliance % |
| `macmillanKPIs` | 5552 | EPA tab KPI aggregates |
| `epaRows` | 5639 | EPA per-deal table |
| `csRows` | 5681 | CS per-deal table |
| `MacmillanSLATable` | 2995 | Drafting → Partnerships SLA page (separate from audit) |
| `PROPERTIES` | server.js ~50 | HubSpot property fetch list |
| `fetchAllDeals` | server.js ~540 | Three-batch fetch + dedup |

---

## 7. Compliance reporting export

Generated by `exportMacmillanXlsx` (~line 5828). Produces an .xlsx in the
"Archive: Octopus Legacy" template format with a Summary sheet + 7 drilldown tabs
(one per KPI / row). Aggregates the same cohorts described above per calendar
month across the From / To range.

See commit `f9f3b73` (drilldown tabs) and `3d000bf` (Summary sheet) for the
ExcelJS layout details. The export is **not** Macmillan-facing by default —
drilldown tabs contain deal names + (KPI 7) customer email addresses, which are
personal data under UK GDPR.

---

## 8. Known limitations

1. **`estimateFirstEntry` heuristic is wrong for re-entry deals**. When the
   workflow stamp `original_date_entered_*` is missing AND the deal entered the
   stage multiple times (e.g. DWC → RDWC → DWC), the heuristic understates
   first-entry. Surfaced via deal 57542012984 (John & Karen Wilkinson) — see
   comment at processDeals line 343-360.

2. **HubSpot CRM Search API caps at 10,000 results per query**. Batches now
   sort by `hs_lastmodifieddate DESC` so we always retain the most recent 10K
   per batch. Once a batch genuinely exceeds 10K-active-in-18-months, we'd
   start silently dropping the oldest tail — flag for a fix (split batch4 by
   month) if you ever see deals from >12 months ago disappearing.

3. **KPI 6 is structural, not temporal**. The literal "5 working days" SLA
   isn't directly measured — see section 2 KPI 6 for the rationale.

4. **Tile colour thresholds are hardcoded** (5% / 10%). If Ops wants
   per-cohort thresholds (e.g. tighter for Initial contact, looser for
   Two-further-calls), they'd need to become inputs.
