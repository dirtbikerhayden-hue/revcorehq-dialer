require('dotenv').config();

const express = require('express');
const bodyParser = require('body-parser');
const twilioLib = require('twilio');
const twilio = twilioLib;
const { jwt: { AccessToken } } = twilioLib;
const VoiceGrant = AccessToken.VoiceGrant;
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const session = require('express-session');

const DATA_DIR = process.env.DATA_DIR || __dirname;
try {
  fs.mkdirSync(DATA_DIR, { recursive: true });
} catch (err) {
  console.error('Error ensuring DATA_DIR exists:', err);
}

const app = express();

// =========================
//   CORE MIDDLEWARE
// =========================

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.use(
  session({
    secret: 'rehash-engine-secret-key', // TODO: change for production
    resave: false,
    saveUninitialized: true,
    cookie: { maxAge: 1000 * 60 * 60 * 12 } // 12 hours
  })
);

// Twilio webhooks: log inbound requests to help debug application errors
app.use('/twilio', bodyParser.urlencoded({ extended: false }), (req, res, next) => {
  try {
    console.log('[Twilio] inbound', req.method, req.path, 'query:', req.query, 'body:', req.body);
  } catch (err) {
    console.error('[Twilio] log error:', err.message);
  }
  next();
});

// Fallback when primary inbound agent(s) did not answer
app.all('/twilio/inbound/fallback', (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  const fromNumber = req.body.From || req.query.From || null;
  const toNumber = req.body.To || req.query.To || null;
  const attempted = (req.body.attempted || req.query.attempted || '')
    .split(',')
    .map(normalizeUsername)
    .filter(Boolean);
  const dialStatus = req.body.DialCallStatus || req.query.DialCallStatus || '';

  console.log('[Inbound fallback] status:', dialStatus, 'attempted:', attempted);

  // If the primary leg succeeded, stop.
  if (dialStatus === 'completed') {
    return res.type('text/xml').send(twiml.toString());
  }

  // Otherwise, dial the next two available agents not yet tried
  const allTargets = selectInboundTargets(fromNumber, toNumber)
    .filter(id => !attempted.includes(id))
    .slice(0, 2);

  if (!allTargets.length) {
    twiml.say('No agents available. Please try again later.');
    return res.type('text/xml').send(twiml.toString());
  }

  const normalizedTo = normalizePhone(toNumber);
  const dial = twiml.dial({ timeout: 20, callerId: normalizedTo || defaultCallerId });
  allTargets.forEach(id => dial.client(id));

  res.type('text/xml').send(twiml.toString());
});

// ===============================
//   USER MANAGEMENT (users.json)
// ===============================

const USERS_FILE = path.join(DATA_DIR, 'users.json');
const AGENT_SLOTS_FILE = path.join(DATA_DIR, 'agent-slots.json');
const LOCAL_PRESENCE_FILE = path.join(DATA_DIR, 'local-presence.json');
const REPORT_METRICS_FILE = path.join(DATA_DIR, 'report-metrics.json');
const RECENT_DIAL_FILE = path.join(DATA_DIR, 'recent-dials.json');
const AGENT_METRICS_FILE = path.join(DATA_DIR, 'agent-metrics.json');
const SCRIPTS_FILE = path.join(DATA_DIR, 'scripts.json');
// High-priority inbound agents can be set later via config; default empty.
const PRIORITY_INBOUND_AGENTS = [];

function normalizeUsername(name) {
  return (name || '').toString().trim().toLowerCase();
}

function normalizeUsersMap(map = {}) {
  const normalized = {};
  Object.entries(map || {}).forEach(([key, value]) => {
    const normalizedKey = normalizeUsername(key);
    if (!normalizedKey) return;
    normalized[normalizedKey] = value || {};
  });
  return normalized;
}

function loadUsers() {
  try {
    if (!fs.existsSync(USERS_FILE)) {
      const defaultUsers = {
        outbound1: { password: 'Rehashengine11!', role: 'agent' }
      };
      fs.writeFileSync(USERS_FILE, JSON.stringify(defaultUsers, null, 2));
      return defaultUsers;
    }

    const raw = fs.readFileSync(USERS_FILE, 'utf8');
    return normalizeUsersMap(JSON.parse(raw) || {});
  } catch (err) {
    console.error('Error loading users.json:', err);
    return {};
  }
}

function saveUsers(users) {
  try {
    fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
  } catch (err) {
    console.error('Error saving users.json:', err);
  }
}

function hasAfterHoursOverride(agentId) {
  const id = normalizeUsername(agentId);
  if (!id || !users || !users[id]) {
    return false;
  }
  return users[id].allowAfterHours === true;
}

let users = loadUsers();

function loadScriptsStore() {
  try {
    if (!fs.existsSync(SCRIPTS_FILE)) {
      const initial = { default: { title: 'Call Script', sections: [] } };
      fs.writeFileSync(SCRIPTS_FILE, JSON.stringify(initial, null, 2));
      return initial;
    }
    const raw = fs.readFileSync(SCRIPTS_FILE, 'utf8');
    const parsed = JSON.parse(raw) || {};
    return parsed;
  } catch (err) {
    console.error('Error loading scripts.json:', err);
    return { default: { title: 'Call Script', sections: [] } };
  }
}

function saveScriptsStore() {
  try {
    fs.writeFileSync(SCRIPTS_FILE, JSON.stringify(scriptsStore, null, 2));
  } catch (err) {
    console.error('Error saving scripts.json:', err);
  }
}

let scriptsStore = loadScriptsStore();

function normalizePhone(num) {
  if (!num) return '';
  const digits = num.replace(/[^\d]/g, '');
  if (!digits) return '';
  if (digits.startsWith('1') && digits.length === 11) return `+${digits}`;
  if (digits.length === 10) return `+1${digits}`;
  return `+${digits}`;
}

// ===============================
//         CAMPAIGN STORE
// ===============================

const CAMPAIGNS_FILE = path.join(DATA_DIR, 'campaigns.json');
const CAMPAIGN_STATS_FILE = path.join(DATA_DIR, 'campaign-stats.json');
const LOCAL_LEADS_FILE = path.join(DATA_DIR, 'local-leads.json');

const DEFAULT_CAMPAIGNS = {
  'old-bids':       { id: 'old-bids',       name: 'Old Bids – 30–180 Days', totalLeads: 200, ghlPipelineId: '', ghlStageId: '', ghlTag: '' },
  'second-visit':   { id: 'second-visit',   name: 'Second-Visit Quotes',    totalLeads: 150, ghlPipelineId: '', ghlStageId: '', ghlTag: '' },
  'lost-estimates': { id: 'lost-estimates', name: 'Lost Estimates – Rehash', totalLeads: 120, ghlPipelineId: '', ghlStageId: '', ghlTag: '' }
};

function loadCampaignMap() {
  try {
    if (!fs.existsSync(CAMPAIGNS_FILE)) {
      fs.writeFileSync(CAMPAIGNS_FILE, JSON.stringify(DEFAULT_CAMPAIGNS, null, 2));
      return { ...DEFAULT_CAMPAIGNS };
    }
    const raw = fs.readFileSync(CAMPAIGNS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    if (!parsed || !Object.keys(parsed).length) {
      return { ...DEFAULT_CAMPAIGNS };
    }
    Object.keys(parsed).forEach(key => {
      parsed[key] = normalizeCampaignRecord(parsed[key], key);
    });
    return parsed;
  } catch (err) {
    console.error('Error loading campaigns.json:', err);
    return { ...DEFAULT_CAMPAIGNS };
  }
}

function saveCampaignMap(map) {
  try {
    fs.writeFileSync(CAMPAIGNS_FILE, JSON.stringify(map, null, 2));
  } catch (err) {
    console.error('Error saving campaigns.json:', err);
  }
}

function normalizeCampaignRecord(record = {}, id = null) {
  return {
    id: record.id || id,
    name: record.name || record.ghlTag || id || 'Campaign',
    totalLeads: typeof record.totalLeads === 'number' ? record.totalLeads : Number(record.totalLeads || 0),
    ghlPipelineId: record.ghlPipelineId || '',
    ghlStageId: record.ghlStageId || '',
    ghlTag: record.ghlTag || ''
  };
}

function listCampaignArray() {
  return Object.values(campaigns);
}

const DEFAULT_CAMPAIGN_STATS = {};
const DEFAULT_LOCAL_LEADS = { queue: {}, completed: {} };
const RECENT_DIAL_TTL_MS = 15 * 60 * 1000; // 15 minutes

function loadAgentMetricsStore() {
  try {
    if (!fs.existsSync(AGENT_METRICS_FILE)) {
      return {};
    }
    const raw = fs.readFileSync(AGENT_METRICS_FILE, 'utf8');
    const parsed = JSON.parse(raw) || {};
    if (!parsed || typeof parsed !== 'object') return {};
    return parsed;
  } catch (err) {
    console.error('Error loading agent-metrics.json:', err);
    return {};
  }
}

function saveAgentMetricsStore() {
  try {
    fs.writeFileSync(AGENT_METRICS_FILE, JSON.stringify(metricsByAgent, null, 2));
  } catch (err) {
    console.error('Error saving agent-metrics.json:', err);
  }
}

function loadRecentDialMap() {
  try {
    if (!fs.existsSync(RECENT_DIAL_FILE)) {
      return {};
    }
    const raw = fs.readFileSync(RECENT_DIAL_FILE, 'utf8');
    const parsed = JSON.parse(raw) || {};
    const now = Date.now();
    const cleaned = {};
    Object.entries(parsed).forEach(([campaignId, entry]) => {
      if (!entry || typeof entry !== 'object') return;
      const ts = Number(entry.ts || 0);
      if (!ts || now - ts > RECENT_DIAL_TTL_MS) return;
      cleaned[campaignId] = {
        phone: entry.phone || null,
        contactId: entry.contactId || null,
        ts
      };
    });
    return cleaned;
  } catch (err) {
    console.error('Error loading recent-dials.json:', err);
    return {};
  }
}

function saveRecentDialMap() {
  try {
    fs.writeFileSync(RECENT_DIAL_FILE, JSON.stringify(recentDialMap, null, 2));
  } catch (err) {
    console.error('Error saving recent-dials.json:', err);
  }
}

function loadCampaignStats() {
  try {
    if (!fs.existsSync(CAMPAIGN_STATS_FILE)) {
      fs.writeFileSync(CAMPAIGN_STATS_FILE, JSON.stringify(DEFAULT_CAMPAIGN_STATS, null, 2));
      return {};
    }
    const raw = fs.readFileSync(CAMPAIGN_STATS_FILE, 'utf8');
    return JSON.parse(raw) || {};
  } catch (err) {
    console.error('Error loading campaign-stats.json:', err);
    return {};
  }
}

function saveCampaignStats() {
  try {
    fs.writeFileSync(CAMPAIGN_STATS_FILE, JSON.stringify(campaignStats, null, 2));
  } catch (err) {
    console.error('Error saving campaign-stats.json:', err);
  }
}

function ensureCampaignStats(campaignId) {
  if (!campaignStats[campaignId]) {
    campaignStats[campaignId] = {
      totals: {},
      byAgent: {},
      byContact: {},
      lastUpdated: null
    };
  } else if (!campaignStats[campaignId].byContact) {
    campaignStats[campaignId].byContact = {};
  }
  return campaignStats[campaignId];
}

function recordCampaignDisposition(campaignId, agentId, outcome, contactId) {
  if (!campaignId) return;
  const stats = ensureCampaignStats(campaignId);
  stats.totals[outcome] = (stats.totals[outcome] || 0) + 1;
  stats.totals.total = (stats.totals.total || 0) + 1;

  if (!stats.byAgent[agentId]) stats.byAgent[agentId] = {};
  const agentStats = stats.byAgent[agentId];
  agentStats[outcome] = (agentStats[outcome] || 0) + 1;
  agentStats.total = (agentStats.total || 0) + 1;

  // track per-contact attempts and last outcome
  if (contactId) {
    const key = String(contactId);
    if (!stats.byContact[key]) {
      stats.byContact[key] = {
        attempts: 0,
        lastOutcome: null,
        lastAttemptMs: null
      };
    }
    const contactStats = stats.byContact[key];
    contactStats.lastOutcome = outcome;
    contactStats.lastAttemptMs = Date.now();

    if (NON_PICKUP_OUTCOMES.includes(outcome)) {
      contactStats.attempts = (contactStats.attempts || 0) + 1;

      if (contactStats.attempts >= MAX_NON_PICKUP_ATTEMPTS) {
        // mark in GHL so future fetches also skip by tag
        ghlAddTags(contactId, [NON_PICKUP_REMOVED_TAG]).catch(() => {});
      }
    }
  }

  stats.lastUpdated = new Date().toISOString();
  saveCampaignStats();
}

let campaigns = loadCampaignMap();
let campaignStats = loadCampaignStats();
let localLeadStore = loadLocalLeadStore();
let dailyAgentReport = {};
let dailyCampaignReport = {};
const reportMetricState = loadReportMetrics();
dailyAgentReport = reportMetricState.agents || {};
dailyCampaignReport = reportMetricState.campaigns || {};
const recentDialMap = loadRecentDialMap();
const contactLocks = {}; // in-memory lock so a contact isn't dialed twice concurrently
const NON_PICKUP_OUTCOMES = [
  'no_answer',
  'busy',
  'failed',
  'machine',
  'machine_voicemail',
  'left_voicemail',
  'callback_requested'
];
const MAX_NON_PICKUP_ATTEMPTS = 3;
const NON_PICKUP_REMOVED_TAG = 'removed 3 attempts made';
const NON_PICKUP_COOLDOWN_MS = 24 * 60 * 60 * 1000; // 24 hours for all non-pickup outcomes
const GLOBAL_RETRY_COOLDOWN_MS = 24 * 60 * 60 * 1000; // 24-hour global cooldown for outbound attempts
const DISPOSITION_SKIP_TAGS = [
  'bad_number',
  'not_interested',
  'wrong_contact',
  'booked',
  'send_info_email',
  'send info via email',
  'general_email_info',
  'general email info',
  'manual_email_info',
  'manual email info',
  NON_PICKUP_REMOVED_TAG
];
const lastAgentByNumber = {}; // track last agent who called a number
const OUTCOME_LABELS = {
  not_interested: 'Not Interested',
  callback_requested: 'Callback Requested',
  wrong_contact: 'Wrong Contact',
  machine: 'Machine',
  left_voicemail: 'Left Voicemail',
  machine_voicemail: 'Machine / Voicemail',
  bad_number: 'Bad Number',
  send_info_email: 'Send Info via Email',
  general_email_info: 'General Email Info',
  manual_email_info: 'Manual Email Info',
  booked: 'Booked Demo',
  connected: 'Connected',
  no_answer: 'No Answer',
  busy: 'Busy',
  failed: 'Failed'
};

// ===============================
//   LEADERBOARD WEEKLY WINDOW
// ===============================

let leaderboardWeekId = null;

function getEasternNow() {
  const now = new Date();
  const easternString = now.toLocaleString('en-US', { timeZone: 'America/New_York' });
  return new Date(easternString);
}

const BUSINESS_OPEN_HOUR = 9;  // 9:00 AM
const BUSINESS_CLOSE_HOUR = 20; // 8:00 PM

function isWithinBusinessHours(easternDate) {
  const day = easternDate.getDay(); // 0 = Sun, 1 = Mon, ...
  const hour = easternDate.getHours();
  const minute = easternDate.getMinutes();

  // Business days: Monday (1) through Saturday (6)
  if (day === 0) return false;

  if (hour < BUSINESS_OPEN_HOUR) return false;
  if (hour > BUSINESS_CLOSE_HOUR) return false;
  if (hour === BUSINESS_CLOSE_HOUR && minute > 0) return false;

  return true;
}

function getLeaderboardWeekId(easternDate) {
  const d = new Date(easternDate.getTime());
  const day = d.getDay(); // 0 = Sun, 1 = Mon, ...
  const hour = d.getHours();

  // Start with Monday of the current calendar week
  const monday = new Date(d.getTime());
  const diffToMonday = day === 0 ? -6 : 1 - day;
  monday.setDate(monday.getDate() + diffToMonday);
  monday.setHours(0, 0, 0, 0);

  // If it's Monday before 5am, treat it as part of the previous week
  if (day === 1 && hour < 5) {
    monday.setDate(monday.getDate() - 7);
  }

  const y = monday.getFullYear();
  const m = String(monday.getMonth() + 1).padStart(2, '0');
  const dd = String(monday.getDate()).padStart(2, '0');
  return `${y}-${m}-${dd}`;
}

function isWithinWeeklyWindow(easternDate) {
  const day = easternDate.getDay();
  const hour = easternDate.getHours();
  const minute = easternDate.getMinutes();

  // Monday: from 5:00 onward
  if (day === 1) {
    return hour > 5 || (hour === 5 && minute >= 0);
  }
  // Tuesday–Friday: all day
  if (day >= 2 && day <= 5) return true;
  // Saturday: until 20:00 (8pm)
  if (day === 6) {
    if (hour < 20) return true;
    if (hour === 20 && minute === 0) return true;
    return false;
  }
  // Sunday and the rest of Saturday night: outside window
  return false;
}

function ensureLeaderboardWeek() {
  const easternNow = getEasternNow();
  const weekId = getLeaderboardWeekId(easternNow);
  if (leaderboardWeekId && leaderboardWeekId !== weekId) {
    Object.keys(metricsByAgent).forEach(key => { delete metricsByAgent[key]; });
    saveAgentMetricsStore();
  }
  leaderboardWeekId = weekId;
  return easternNow;
}

// ===============================
//     DAILY / WEEKLY REPORTING
// ===============================

const DAILY_REPORT_START_HOUR = 9;
const DAILY_REPORT_END_HOUR = 20; // 8pm

function getDateId(easternDate) {
  const y = easternDate.getFullYear();
  const m = String(easternDate.getMonth() + 1).padStart(2, '0');
  const d = String(easternDate.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function isWithinDailyReportWindow(easternDate) {
  const hour = easternDate.getHours();
  const minute = easternDate.getMinutes();
  if (hour < DAILY_REPORT_START_HOUR) return false;
  if (hour > DAILY_REPORT_END_HOUR) return false;
  if (hour === DAILY_REPORT_END_HOUR && minute > 0) return false;
  return true;
}

function isWithinWeeklyReportWindow(easternDate) {
  const day = easternDate.getDay(); // 0=Sun
  if (day === 0) return false; // Sunday always out
  if (day === 1 && !isWithinDailyReportWindow(easternDate)) return false;
  if (day >= 2 && day <= 5) return isWithinDailyReportWindow(easternDate);
  if (day === 6) return isWithinDailyReportWindow(easternDate); // Saturday until 8pm
  return false;
}

// For "Today's Numbers" on the dialer UI, we treat a "day"
// as running from 5:00am–4:59am Eastern.
function getDialerDayId(easternDate) {
  const shifted = new Date(easternDate.getTime() - 5 * 60 * 60 * 1000);
  return getDateId(shifted);
}

function loadReportMetrics() {
  try {
    if (!fs.existsSync(REPORT_METRICS_FILE)) {
      return { agents: {}, campaigns: {} };
    }
    const raw = fs.readFileSync(REPORT_METRICS_FILE, 'utf8');
    const parsed = JSON.parse(raw) || {};
    return {
      agents: parsed.agents || {},
      campaigns: parsed.campaigns || {}
    };
  } catch (err) {
    console.error('Error loading report-metrics.json:', err);
    return { agents: {}, campaigns: {} };
  }
}

function saveReportMetrics() {
  try {
    fs.writeFileSync(REPORT_METRICS_FILE, JSON.stringify({
      agents: dailyAgentReport,
      campaigns: dailyCampaignReport
    }, null, 2));
  } catch (err) {
    console.error('Error saving report-metrics.json:', err);
  }
}

function ensureDailyAgentMetric(dateId, agentId) {
  if (!dailyAgentReport[dateId]) dailyAgentReport[dateId] = {};
  if (!dailyAgentReport[dateId][agentId]) {
    dailyAgentReport[dateId][agentId] = {
      totalCalls: 0,
      liveConnects: 0,
      dispositions: {}
    };
  }
  return dailyAgentReport[dateId][agentId];
}

function ensureDailyCampaignMetric(dateId, campaignId) {
  if (!dailyCampaignReport[dateId]) dailyCampaignReport[dateId] = {};
  if (!dailyCampaignReport[dateId][campaignId]) {
    dailyCampaignReport[dateId][campaignId] = {
      dispositions: {}
    };
  }
  return dailyCampaignReport[dateId][campaignId];
}

async function buildDailyReport(dateId) {
  const hoursInWindow = DAILY_REPORT_END_HOUR - DAILY_REPORT_START_HOUR;
  const agentMetrics = dailyAgentReport[dateId] || {};
  const agents = Object.entries(agentMetrics).map(([agentId, data]) => {
    const totalCalls = data.totalCalls || 0;
    const liveConnects = data.liveConnects || 0;
    const avgCallsPerHour = hoursInWindow > 0 ? totalCalls / hoursInWindow : 0;
    return {
      agentId,
      totalCalls,
      avgCallsPerHour,
      liveConnects,
      dispositions: data.dispositions || {}
    };
  });

  const totals = {
    totalCalls: 0,
    liveConnects: 0,
    dispositions: {}
  };
  agents.forEach(a => {
    totals.totalCalls += a.totalCalls || 0;
    totals.liveConnects += a.liveConnects || 0;
    Object.entries(a.dispositions || {}).forEach(([key, val]) => {
      totals.dispositions[key] = (totals.dispositions[key] || 0) + (val || 0);
    });
  });

  const campaignSnapshot = await buildCampaignResponse();
  const campaignMetrics = dailyCampaignReport[dateId] || {};
  const campaignsReport = Object.entries(campaignMetrics).map(([campaignId, data]) => {
    const snapshot = campaignSnapshot[campaignId] || {};
    const totalLeads =
      typeof snapshot.computedTotalLeads === 'number'
        ? snapshot.computedTotalLeads
        : typeof snapshot.totalLeads === 'number'
          ? snapshot.totalLeads
          : 0;
    const remainingLeads =
      typeof snapshot.computedRemainingLeads === 'number'
        ? snapshot.computedRemainingLeads
        : 0;
    return {
      campaignId,
      name: snapshot.name || campaignId,
      totalLeads,
      remainingLeads,
      dispositions: data.dispositions || {}
    };
  });

  return {
    date: dateId,
    hoursInWindow,
    agents,
    totals,
    campaigns: campaignsReport
  };
}

async function buildWeeklyReport() {
  const easternNow = getEasternNow();
  const base = new Date(easternNow.getTime());
  const day = base.getDay(); // 0 Sun
  const diffToMonday = day === 0 ? -6 : 1 - day;
  const monday = new Date(base.getTime());
  monday.setDate(base.getDate() + diffToMonday);
  monday.setHours(0, 0, 0, 0);

  const dateIds = [];
  for (let i = 0; i < 6; i += 1) { // Monday–Saturday
    const d = new Date(monday.getTime());
    d.setDate(monday.getDate() + i);
    dateIds.push(getDateId(d));
  }

  const weeklyAgents = {};
  dateIds.forEach(dateId => {
    const dayMetrics = dailyAgentReport[dateId] || {};
    Object.entries(dayMetrics).forEach(([agentId, data]) => {
      if (!weeklyAgents[agentId]) {
        weeklyAgents[agentId] = {
          totalCalls: 0,
          liveConnects: 0,
          dispositions: {}
        };
      }
      const agg = weeklyAgents[agentId];
      agg.totalCalls += data.totalCalls || 0;
      agg.liveConnects += data.liveConnects || 0;
      Object.entries(data.dispositions || {}).forEach(([key, val]) => {
        agg.dispositions[key] = (agg.dispositions[key] || 0) + (val || 0);
      });
    });
  });

  const hoursInWindow = DAILY_REPORT_END_HOUR - DAILY_REPORT_START_HOUR;
  const totalHours = hoursInWindow * dateIds.length;
  const agents = Object.entries(weeklyAgents).map(([agentId, data]) => {
    const totalCalls = data.totalCalls || 0;
    const liveConnects = data.liveConnects || 0;
    const avgCallsPerHour = totalHours > 0 ? totalCalls / totalHours : 0;
    return {
      agentId,
      totalCalls,
      avgCallsPerHour,
      liveConnects,
      dispositions: data.dispositions || {}
    };
  });

  const totals = {
    totalCalls: 0,
    liveConnects: 0,
    dispositions: {}
  };
  agents.forEach(a => {
    totals.totalCalls += a.totalCalls || 0;
    totals.liveConnects += a.liveConnects || 0;
    Object.entries(a.dispositions || {}).forEach(([key, val]) => {
      totals.dispositions[key] = (totals.dispositions[key] || 0) + (val || 0);
    });
  });

  const campaignSnapshot = await buildCampaignResponse();
  const weeklyCampaigns = {};
  dateIds.forEach(dateId => {
    const dayCampaigns = dailyCampaignReport[dateId] || {};
    Object.entries(dayCampaigns).forEach(([campaignId, data]) => {
      if (!weeklyCampaigns[campaignId]) {
        weeklyCampaigns[campaignId] = { dispositions: {} };
      }
      const agg = weeklyCampaigns[campaignId];
      Object.entries(data.dispositions || {}).forEach(([key, val]) => {
        agg.dispositions[key] = (agg.dispositions[key] || 0) + (val || 0);
      });
    });
  });

  const campaignsReport = Object.entries(weeklyCampaigns).map(([campaignId, data]) => {
    const snapshot = campaignSnapshot[campaignId] || {};
    const totalLeads =
      typeof snapshot.computedTotalLeads === 'number'
        ? snapshot.computedTotalLeads
        : typeof snapshot.totalLeads === 'number'
          ? snapshot.totalLeads
          : 0;
    const remainingLeads =
      typeof snapshot.computedRemainingLeads === 'number'
        ? snapshot.computedRemainingLeads
        : 0;
    return {
      campaignId,
      name: snapshot.name || campaignId,
      totalLeads,
      remainingLeads,
      dispositions: data.dispositions || {}
    };
  });

  return {
    weekStartDate: getDateId(monday),
    hoursPerDay: hoursInWindow,
    days: dateIds,
    agents,
    totals,
    campaigns: campaignsReport
  };
}

// ===============================
//         APP SETTINGS
// ===============================

const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const DEFAULT_SETTINGS = {
  machineDetectionEnabled: false,
  callRecordingEnabled: false
};

function loadSettings() {
  try {
    if (!fs.existsSync(SETTINGS_FILE)) {
      fs.writeFileSync(SETTINGS_FILE, JSON.stringify(DEFAULT_SETTINGS, null, 2));
      return { ...DEFAULT_SETTINGS };
    }
    const raw = fs.readFileSync(SETTINGS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return { ...DEFAULT_SETTINGS, ...(parsed || {}) };
  } catch (err) {
    console.error('Error loading settings.json:', err);
    return { ...DEFAULT_SETTINGS };
  }
}

function saveSettings(settings) {
  try {
    fs.writeFileSync(SETTINGS_FILE, JSON.stringify(settings, null, 2));
  } catch (err) {
    console.error('Error saving settings.json:', err);
  }
}

let appSettings = loadSettings();

// ===============================
//       LOCAL LEAD DATA STORE
// ===============================

function loadLocalLeadStore() {
  try {
    if (!fs.existsSync(LOCAL_LEADS_FILE)) {
      fs.writeFileSync(LOCAL_LEADS_FILE, JSON.stringify(DEFAULT_LOCAL_LEADS, null, 2));
      return JSON.parse(JSON.stringify(DEFAULT_LOCAL_LEADS));
    }
    const raw = fs.readFileSync(LOCAL_LEADS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    return {
      queue: parsed.queue || {},
      completed: parsed.completed || {}
    };
  } catch (err) {
    console.error('Error loading local-leads.json:', err);
    return JSON.parse(JSON.stringify(DEFAULT_LOCAL_LEADS));
  }
}

function saveLocalLeadStore() {
  try {
    fs.writeFileSync(LOCAL_LEADS_FILE, JSON.stringify(localLeadStore, null, 2));
  } catch (err) {
    console.error('Error saving local-leads.json:', err);
  }
}

function normalizeLocalPhone(raw) {
  if (!raw) return null;
  const digits = String(raw).replace(/[^\d]/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  if (String(raw).startsWith('+') && raw.length > 4) return raw;
  return null;
}

function enqueueLocalLeads(campaignId, leads = [], mode = 'append') {
  if (!campaignId) return { added: 0, total: 0 };
  if (!Array.isArray(leads) || !leads.length) {
    return { added: 0, total: getLocalQueueCount(campaignId) };
  }
  if (mode === 'replace') {
    localLeadStore.queue[campaignId] = [];
  }
  if (!Array.isArray(localLeadStore.queue[campaignId])) {
    localLeadStore.queue[campaignId] = [];
  }
  const now = Date.now();
  let added = 0;
  leads.forEach((lead, idx) => {
    const phone = normalizeLocalPhone(lead.phone || lead.phoneNumber);
    if (!phone) return;
    const payload = {
      localLeadId: `${campaignId}-${now}-${idx}-${Math.floor(Math.random() * 10000)}`,
      name: (lead.name || `${lead.firstName || ''} ${lead.lastName || ''}` || '').trim() || `Lead ${idx + 1}`,
      phone,
      company: lead.company || lead.companyName || '',
      email: lead.email || lead.emailAddress || '',
      city: lead.city || '',
      state: lead.state || lead.region || '',
      meta: lead.meta || {}
    };
    localLeadStore.queue[campaignId].push(payload);
    added += 1;
  });
  saveLocalLeadStore();
  return { added, total: getLocalQueueCount(campaignId) };
}

function popLocalLead(campaignId) {
  if (!campaignId) return null;
  const queue = localLeadStore.queue[campaignId];
  if (!queue || !queue.length) return null;
  const lead = queue.shift();
  saveLocalLeadStore();
  return lead;
}

function recordLocalLeadOutcome(campaignId, meta, outcome, notes, leadDetails) {
  if (!campaignId || !meta || !meta.localLeadId) return;
  if (!Array.isArray(localLeadStore.completed[campaignId])) {
    localLeadStore.completed[campaignId] = [];
  }
  localLeadStore.completed[campaignId].push({
    id: meta.localLeadId,
    outcome,
    notes: notes || '',
    leadName: leadDetails?.leadName || meta.localLeadName || '',
    leadPhone: leadDetails?.leadPhone || meta.localLeadPhone || '',
    timestamp: new Date().toISOString()
  });
  saveLocalLeadStore();
}

function getLocalQueueCount(campaignId) {
  if (!campaignId) return 0;
  const queue = localLeadStore.queue[campaignId];
  return Array.isArray(queue) ? queue.length : 0;
}

function getLocalCompletedCount(campaignId) {
  if (!campaignId) return 0;
  const list = localLeadStore.completed[campaignId];
  return Array.isArray(list) ? list.length : 0;
}

function getLocalLeadSummary() {
  const queue = {};
  const completed = {};
  Object.keys(localLeadStore.queue).forEach(id => {
    queue[id] = getLocalQueueCount(id);
  });
  Object.keys(localLeadStore.completed).forEach(id => {
    completed[id] = getLocalCompletedCount(id);
  });
  return { queue, completed };
}

function isRecentlyDialed(campaignId, phone, contactId) {
  if (!campaignId) return false;
  const entry = recentDialMap[campaignId];
  if (!entry) return false;
  if (contactId && entry.contactId && contactId === entry.contactId) return true;
  if (phone && entry.phone && phone === entry.phone) return true;
  return false;
}

function markRecentlyDialed(campaignId, phone, contactId) {
  if (!campaignId) return;
  recentDialMap[campaignId] = {
    phone: phone || null,
    contactId: contactId || null,
    ts: Date.now()
  };
  saveRecentDialMap();
}

function isContactLocked(contactId) {
  if (!contactId) return false;
  const lock = contactLocks[contactId];
  if (!lock) return false;
  if (lock.expiresAt && lock.expiresAt < Date.now()) {
    delete contactLocks[contactId];
    return false;
  }
  return true;
}

function lockContact(contactId, campaignId, agentId, ttlMs = 20 * 60 * 1000) {
  if (!contactId) return;
  contactLocks[contactId] = {
    campaignId,
    agentId,
    expiresAt: Date.now() + ttlMs
  };
}

function unlockContact(contactId) {
  if (!contactId) return;
  delete contactLocks[contactId];
}

// ===============================
//         ADMIN BACKEND
// ===============================

// Admin password from env with fallback
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '5683';

function isValidAdmin(req) {
  const key = req.header('x-admin-key');
  return key && key === ADMIN_PASSWORD;
}

// POST /api/admin/login
app.post('/api/admin/login', express.json(), (req, res) => {
  const { password } = req.body || {};

  if (!password || password !== ADMIN_PASSWORD) {
    return res.status(401).json({ ok: false, error: 'Invalid admin password' });
  }

  return res.json({ ok: true });
});

// Daily / weekly reports (JSON)
app.get('/api/admin/report/daily', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const dateParam = (req.query.date || '').toString().trim();
  let dateId;
  if (dateParam) {
    dateId = dateParam;
  } else {
    const easternNow = getEasternNow();
    dateId = getDateId(easternNow);
  }
  const report = await buildDailyReport(dateId);
  res.json({ ok: true, report });
});

app.get('/api/admin/report/weekly', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const report = await buildWeeklyReport();
  res.json({ ok: true, report });
});

// Send reports to Zapier webhook
app.post('/api/admin/report/daily/send', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  if (!ZAPIER_HOOK_URL) {
    return res.status(400).json({ ok: false, error: 'ZAPIER_HOOK_URL not configured' });
  }
  const dateParam = (req.query.date || '').toString().trim();
  let dateId;
  if (dateParam) {
    dateId = dateParam;
  } else {
    const easternNow = getEasternNow();
    dateId = getDateId(easternNow);
  }
  const report = await buildDailyReport(dateId);
  try {
    await axios.post(ZAPIER_HOOK_URL, {
      type: 'daily_report',
      date: report.date,
      report
    });
    res.json({ ok: true, sent: true });
  } catch (err) {
    console.error('Error sending daily report to Zapier:', err.message);
    res.status(500).json({ ok: false, error: 'Failed to send daily report to Zapier' });
  }
});

app.post('/api/admin/report/weekly/send', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  if (!ZAPIER_HOOK_URL) {
    return res.status(400).json({ ok: false, error: 'ZAPIER_HOOK_URL not configured' });
  }
  const report = await buildWeeklyReport();
  try {
    await axios.post(ZAPIER_HOOK_URL, {
      type: 'weekly_report',
      weekStartDate: report.weekStartDate,
      report
    });
    res.json({ ok: true, sent: true });
  } catch (err) {
    console.error('Error sending weekly report to Zapier:', err.message);
    res.status(500).json({ ok: false, error: 'Failed to send weekly report to Zapier' });
  }
});

// GET /api/admin/users
app.get('/api/admin/users', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  res.json({ ok: true, users });
});

// POST /api/admin/users
app.post('/api/admin/users', express.json(), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const {
    username,
    password,
    role,
    campaignId,
    backupCampaignId,
    outboundNumber,
    inboundNumber,
    allowAfterHours
  } = req.body || {};
  const normalizedUsername = normalizeUsername(username);
  const normalizedCampaignId = campaignId ? String(campaignId) : null;
  const normalizedBackupCampaignId = backupCampaignId ? String(backupCampaignId) : null;
  const normalizedOutbound = outboundNumber ? normalizePhone(outboundNumber) : '';
  const normalizedInbound = inboundNumber ? normalizePhone(inboundNumber) : '';

  if (!normalizedUsername || !password) {
    return res
      .status(400)
      .json({ ok: false, error: 'username and password are required' });
  }

  if (normalizedCampaignId && !campaigns[normalizedCampaignId]) {
    return res.status(400).json({ ok: false, error: 'Invalid campaignId' });
  }
  if (normalizedBackupCampaignId && !campaigns[normalizedBackupCampaignId]) {
    return res.status(400).json({ ok: false, error: 'Invalid backupCampaignId' });
  }

  if (users[normalizedUsername]) {
    return res
      .status(400)
      .json({ ok: false, error: 'User already exists' });
  }

  users[normalizedUsername] = {
    password,
    role: role || 'agent',
    campaignId: normalizedCampaignId,
    backupCampaignId: normalizedBackupCampaignId,
    outboundNumber: normalizedOutbound,
    inboundNumber: normalizedInbound,
    allowAfterHours: !!allowAfterHours
  };

  saveUsers(users);

  res.json({ ok: true, users });
});

// PUT /api/admin/users/:username
app.put('/api/admin/users/:username', express.json(), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const oldUsername = normalizeUsername(req.params.username);
  const {
    newUsername,
    password,
    role,
    campaignId,
    backupCampaignId,
    outboundNumber,
    inboundNumber,
    allowAfterHours
  } = req.body || {};
  const normalizedCampaignId = typeof campaignId === 'undefined' || campaignId === ''
    ? undefined
    : String(campaignId);
  const normalizedBackupCampaignId = typeof backupCampaignId === 'undefined' || backupCampaignId === ''
    ? undefined
    : String(backupCampaignId);
  const normalizedOutbound = typeof outboundNumber === 'undefined'
    ? undefined
    : outboundNumber === ''
      ? ''
      : normalizePhone(outboundNumber);
  const normalizedInbound = typeof inboundNumber === 'undefined'
    ? undefined
    : inboundNumber === ''
      ? ''
      : normalizePhone(inboundNumber);

  if (!users[oldUsername]) {
    return res.status(404).json({ ok: false, error: 'User not found' });
  }

  const nextUsername = newUsername ? normalizeUsername(newUsername) : oldUsername;

  if (normalizedCampaignId && !campaigns[normalizedCampaignId]) {
    return res.status(400).json({ ok: false, error: 'Invalid campaignId' });
  }
  if (normalizedBackupCampaignId && !campaigns[normalizedBackupCampaignId]) {
    return res.status(400).json({ ok: false, error: 'Invalid backupCampaignId' });
  }

  const current = users[oldUsername];

  const updatedUser = {
    password: password || current.password,
    role: role || current.role || 'agent',
    campaignId:
      typeof normalizedCampaignId !== 'undefined'
        ? normalizedCampaignId || null
        : current.campaignId || null,
    backupCampaignId:
      typeof normalizedBackupCampaignId !== 'undefined'
        ? normalizedBackupCampaignId || null
        : current.backupCampaignId || null,
    outboundNumber:
      typeof normalizedOutbound !== 'undefined'
        ? normalizedOutbound
        : current.outboundNumber || '',
    inboundNumber:
      typeof normalizedInbound !== 'undefined'
        ? normalizedInbound
        : current.inboundNumber || '',
    allowAfterHours:
      typeof allowAfterHours !== 'undefined'
        ? !!allowAfterHours
        : !!current.allowAfterHours
  };

  if (nextUsername && nextUsername !== oldUsername) {
    if (users[nextUsername]) {
      return res
        .status(400)
        .json({ ok: false, error: 'New username already exists' });
    }
    delete users[oldUsername];
    users[nextUsername] = updatedUser;
  } else {
    users[oldUsername] = updatedUser;
  }

  saveUsers(users);

  res.json({ ok: true, users });
});

// DELETE /api/admin/users/:username
app.delete('/api/admin/users/:username', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const username = normalizeUsername(req.params.username);

  if (!users[username]) {
    return res.status(404).json({ ok: false, error: 'User not found' });
  }

  delete users[username];
  saveUsers(users);

  res.json({ ok: true, users });
});

app.get('/api/admin/slots', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  res.json({ ok: true, slots: agentSlots });
});

app.post('/api/admin/slots', express.json(), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const slot = normalizeSlot(req.body || {});
  if (!slot.id || !slot.dialTarget) {
    return res.status(400).json({ ok: false, error: 'slot id and dialTarget are required' });
  }
  if (agentSlots[slot.id]) {
    return res.status(400).json({ ok: false, error: 'Slot already exists' });
  }
  agentSlots[slot.id] = slot;
  saveAgentSlots(agentSlots);
  res.json({ ok: true, slots: agentSlots });
});

app.put('/api/admin/slots/:id', express.json(), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const slotId = normalizeUsername(req.params.id);
  if (!agentSlots[slotId]) {
    return res.status(404).json({ ok: false, error: 'Slot not found' });
  }
  const incoming = normalizeSlot({ ...agentSlots[slotId], ...req.body, id: slotId });
  if (!incoming.dialTarget) {
    return res.status(400).json({ ok: false, error: 'dialTarget is required' });
  }
  agentSlots[slotId] = incoming;
  saveAgentSlots(agentSlots);
  res.json({ ok: true, slots: agentSlots });
});

app.delete('/api/admin/slots/:id', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const slotId = normalizeUsername(req.params.id);
  if (!agentSlots[slotId]) {
    return res.status(404).json({ ok: false, error: 'Slot not found' });
  }
  delete agentSlots[slotId];
  saveAgentSlots(agentSlots);
  res.json({ ok: true, slots: agentSlots });
});

app.get('/api/admin/local-presence', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  res.json({ ok: true, map: localPresenceMap });
});

app.put('/api/admin/local-presence', express.json(), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const incoming = req.body?.map || {};
  const cleaned = {};
  Object.entries(incoming).forEach(([area, num]) => {
    const normArea = (area || '').replace(/[^\d]/g, '').slice(0, 3);
    const normNum = normalizePhone(num);
    if (normArea && normNum) cleaned[normArea] = normNum;
  });
  localPresenceMap = cleaned;
  saveLocalPresence(localPresenceMap);
  res.json({ ok: true, map: localPresenceMap });
});

// Campaign admin endpoints
async function buildCampaignResponse() {
  const output = {};
  for (const [id, value] of Object.entries(campaigns)) {
    const normalized = normalizeCampaignRecord(value, id);
    const localQueue = getLocalQueueCount(id);
    const localDone = getLocalCompletedCount(id);
    if (localQueue || localDone) {
      normalized.computedTotalLeads = (normalized.computedTotalLeads || 0) + localQueue + localDone;
      normalized.computedRemainingLeads = (normalized.computedRemainingLeads || 0) + localQueue;
    }
    if (isGhlCampaign(normalized)) {
      const stats = await getCampaignLeadStats(normalized);
      if (stats) {
        normalized.computedTotalLeads = stats.total;
        normalized.computedRemainingLeads = stats.remaining;
      }
    }
    output[id] = normalized;
  }
  return output;
}

async function respondWithCampaigns(res) {
  const map = await buildCampaignResponse();
  res.json({ ok: true, campaigns: map });
}

app.get('/api/admin/campaigns', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  await respondWithCampaigns(res);
});

app.post('/api/admin/campaigns', express.json(), async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const { id, name, totalLeads, ghlPipelineId, ghlStageId, ghlTag } = req.body || {};
  if (!id || !name) {
    return res.status(400).json({ ok: false, error: 'id and name are required' });
  }
  if (campaigns[id]) {
    return res.status(400).json({ ok: false, error: 'Campaign ID already exists' });
  }
  campaigns[id] = normalizeCampaignRecord(
    {
      id,
      name,
      totalLeads: Number(totalLeads) || 0,
      ghlPipelineId: ghlPipelineId || '',
      ghlStageId: ghlStageId || '',
      ghlTag: ghlTag || ''
    },
    id
  );
  saveCampaignMap(campaigns);
  await respondWithCampaigns(res);
});

app.put('/api/admin/campaigns/:id', express.json(), async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const campaignId = req.params.id;
  if (!campaigns[campaignId]) {
    return res.status(404).json({ ok: false, error: 'Campaign not found' });
  }
  const { name, totalLeads, ghlPipelineId, ghlStageId, ghlTag } = req.body || {};
  if (name) campaigns[campaignId].name = name;
  if (typeof totalLeads !== 'undefined') {
    campaigns[campaignId].totalLeads = Math.max(0, Number(totalLeads) || 0);
  }
  if (typeof ghlPipelineId !== 'undefined') {
    campaigns[campaignId].ghlPipelineId = ghlPipelineId || '';
  }
  if (typeof ghlStageId !== 'undefined') {
    campaigns[campaignId].ghlStageId = ghlStageId || '';
  }
  if (typeof ghlTag !== 'undefined') {
    campaigns[campaignId].ghlTag = ghlTag || '';
  }
  saveCampaignMap(campaigns);
  await respondWithCampaigns(res);
});

app.delete('/api/admin/campaigns/:id', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const campaignId = req.params.id;
  if (!campaigns[campaignId]) {
    return res.status(404).json({ ok: false, error: 'Campaign not found' });
  }
  delete campaigns[campaignId];
  saveCampaignMap(campaigns);
  if (campaignStats[campaignId]) {
    delete campaignStats[campaignId];
    saveCampaignStats();
  }
  await respondWithCampaigns(res);
});

app.get('/api/admin/campaign-metrics', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const { campaignId, agents, dispositions } = req.query || {};
  if (!campaignId) {
    return res.status(400).json({ ok: false, error: 'campaignId is required' });
  }
  const stats = ensureCampaignStats(campaignId);
  const campaign = campaigns[campaignId] || { id: campaignId, name: campaignId, totalLeads: 0 };
  const agentFilter = agents ? agents.split(',').map(a => a.trim()).filter(Boolean) : null;
  const dispoFilter = dispositions ? dispositions.split(',').map(d => d.trim()).filter(Boolean) : null;

  const totals = { ...(stats.totals || {}) };
  const filteredTotals = {};
  const dispositionsList = Object.keys(totals).filter(key => key !== 'total');
  const totalDispos = dispositionsList.reduce((sum, key) => sum + (totals[key] || 0), 0);
  const remainingLeads = Math.max(0, (campaign.totalLeads || 0) - totalDispos);

  (dispoFilter || dispositionsList).forEach(key => {
    if (typeof totals[key] !== 'undefined') {
      filteredTotals[key] = totals[key];
    }
  });

  const agentBreakdown = Object.entries(stats.byAgent || {})
    .filter(([agentId]) => !agentFilter || agentFilter.includes(agentId))
    .map(([agentId, counts]) => {
      const filtered = {};
      (dispoFilter || Object.keys(counts)).forEach(key => {
        if (key === 'total') return;
        if (typeof counts[key] !== 'undefined') {
          filtered[key] = counts[key];
        }
      });
      return {
        agentId,
        total: counts.total || 0,
        counts: filtered
      };
    });

  res.json({
    ok: true,
    campaign,
    totals,
    filteredTotals,
    remainingLeads,
    agentBreakdown,
    dispositions: dispositionsList
  });
});
// GET /api/admin/settings
app.get('/api/admin/settings', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  res.json({ ok: true, settings: appSettings });
});

// Local lead upload (CSV/JSON) for fallback mode while GHL is down
app.post('/api/admin/local-leads', express.json({ limit: '5mb' }), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }

  const { campaignId, leads, mode } = req.body || {};
  if (!campaignId) {
    return res.status(400).json({ ok: false, error: 'campaignId is required' });
  }
  if (!Array.isArray(leads) || !leads.length) {
    return res.status(400).json({ ok: false, error: 'No leads provided' });
  }

  const { added, total } = enqueueLocalLeads(campaignId, leads, mode === 'replace' ? 'replace' : 'append');
  const summary = getLocalLeadSummary();

  res.json({ ok: true, added, total, summary });
});

// Hang up active call for the current agent
app.post('/api/agent/hangup', async (req, res) => {
  const agentId = requireAgent(req, res);
  if (!agentId) return;
  const callSid = activeCallByAgent[agentId];
  if (!callSid) {
    return res.json({ ok: false, error: 'No active call for agent.' });
  }
  try {
    await client.calls(callSid).update({ status: 'completed' });
    delete activeCallByAgent[agentId];
    return res.json({ ok: true });
  } catch (err) {
    console.error('Error hanging up call:', err.message);
    return res.status(500).json({ ok: false, error: 'Failed to hang up call.' });
  }
});

// Manual dial endpoint
app.post('/api/manual-dial', express.json(), async (req, res) => {
  const agentId = requireAgent(req, res);
  if (!agentId) return;
  const easternNow = getEasternNow();
  const reportDateId = getDateId(easternNow);

  const bypassHours = hasAfterHoursOverride(agentId);
  if (!bypassHours && !isWithinBusinessHours(easternNow)) {
    return res.status(403).json({
      ok: false,
      error: 'Manual dialing is only allowed between 9:00AM and 8:00PM EST, Monday–Saturday.'
    });
  }

  const { toNumber, callerId, campaignId, reason } = req.body || {};
  if (!toNumber) {
    return res.status(400).json({ ok: false, error: 'Destination number required' });
  }
  if (activeCallByAgent[agentId]) {
    return res.status(400).json({ ok: false, error: 'Hang up the current call before manual dial.' });
  }

  const fromNumber =
    callerId ||
    getDialNumberForAgent(agentId) ||
    chooseLocalNumberForLead(toNumber) ||
    defaultCallerId;
  console.log('[Manual outbound]', { agentId, fromNumber, toNumber });
  if (!fromNumber) {
    return res.status(400).json({ ok: false, error: 'No caller ID available' });
  }

  try {
    const call = await client.calls.create({
      to: toNumber,
      from: fromNumber,
      url: `${BASE_URL}/twilio/voice?agentId=${agentId}`,
      statusCallback: `${BASE_URL}/twilio/status`,
      statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
      machineDetection: appSettings.machineDetectionEnabled ? 'Enable' : undefined
    });

    callMap[call.sid] = {
      agentId,
      lead: {
        id: null,
        name: toNumber,
        phone: toNumber,
        campaignId: campaignId || null,
        campaignTag: campaignId && campaigns[campaignId]?.ghlTag ? campaigns[campaignId].ghlTag : null
      },
      campaignId: campaignId || null,
      manual: { toNumber, callerId: fromNumber, reason, contactCreated: false }
    };
    activeCallByAgent[agentId] = call.sid;
    lastAgentByNumber[toNumber] = agentId;

    if (isWithinDailyReportWindow(easternNow) && isWithinWeeklyReportWindow(easternNow)) {
      const daily = ensureDailyAgentMetric(reportDateId, agentId);
      daily.totalCalls += 1;
      saveReportMetrics();
    }

    return res.json({ ok: true, callSid: call.sid });
  } catch (err) {
    console.error('Manual dial failed:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// Inbound routing webhook (set this as your Twilio number voice webhook)
app.all('/twilio/inbound', (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  const fromNumber = req.body.From || req.query.From || null;
  const toNumber = req.body.To || req.query.To || null;
  console.log('[Twilio inbound] From:', fromNumber);
  if (!fromNumber) {
    twiml.say('No from number provided.');
    return res.type('text/xml').send(twiml.toString());
  }

  const success = buildSequentialInbound(twiml, fromNumber, toNumber);
  if (!success) {
    twiml.say('No agents available. Please try again later.');
  }

  res.type('text/xml').send(twiml.toString());
});


// POST /api/admin/settings
app.post('/api/admin/settings', express.json(), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const { machineDetectionEnabled, callRecordingEnabled } = req.body || {};
  appSettings = {
    ...appSettings,
    machineDetectionEnabled: Boolean(machineDetectionEnabled),
    callRecordingEnabled: Boolean(callRecordingEnabled)
  };
  saveSettings(appSettings);
  res.json({ ok: true, settings: appSettings });
});

app.get('/api/admin/ghl/pipelines', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  if (!ghlClient || !GHL_LOCATION_ID) {
    return res.status(400).json({ ok: false, error: 'GHL API not configured' });
  }
  try {
    const response = await ghlClient.get('/opportunities/pipelines', {
      params: { locationId: GHL_LOCATION_ID }
    });
    res.json({ ok: true, pipelines: response.data?.pipelines || [] });
  } catch (err) {
    console.error('Error fetching GHL pipelines:', err.response?.data || err.message);
    const status = err.response?.status || 500;
    const friendly = formatGhlError(err, 'Failed to load pipelines from GHL');
    res.status(status === 401 || status === 403 ? 400 : 500).json({ ok: false, error: friendly });
  }
});

app.get('/api/admin/ghl/tags', async (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  if (!ghlClient || !GHL_LOCATION_ID) {
    return res.status(400).json({ ok: false, error: 'GHL API not configured' });
  }
  try {
    const response = await ghlClient.get(`/locations/${GHL_LOCATION_ID}/tags`, {
      params: { locationId: GHL_LOCATION_ID }
    });
    res.json({ ok: true, tags: response.data?.tags || response.data?.data || [] });
  } catch (err) {
    console.error('Error fetching GHL tags:', err.response?.data || err.message);
    const status = err.response?.status || 500;
    const friendly = formatGhlError(err, 'Failed to load tags from GHL');
    res.status(status === 401 || status === 403 ? 400 : 500).json({ ok: false, error: friendly });
  }
});

// ===============================
//             LOGIN
// ===============================

app.post('/login', express.json(), (req, res) => {
  const { username, password } = req.body;

  if (!username || !password) {
    return res.status(400).json({ error: 'Missing username or password' });
  }

  const normalizedUsername = normalizeUsername(username);
  const user = users[normalizedUsername];

  if (!user || user.password !== password) {
    return res.status(401).json({ error: 'Invalid username or password' });
  }

  req.session.agentId = normalizedUsername;
  req.session.agentName = normalizedUsername;
  const assignedCampaignId = user.campaignId || null;
  req.session.assignedCampaignId = assignedCampaignId;
  req.session.allowAfterHours = !!user.allowAfterHours;

  console.log('Agent logged in:', normalizedUsername);

  const callbackNumber = getDialNumberForAgent(normalizedUsername) || null;

  return res.json({
    success: true,
    agentId: normalizedUsername,
    agentName: normalizedUsername,
    allowAfterHours: !!user.allowAfterHours,
    callbackNumber,
    campaignId: assignedCampaignId,
    campaignName: assignedCampaignId ? (campaigns[assignedCampaignId]?.name || assignedCampaignId) : null
  });
});

// GET /me
app.get('/me', (req, res) => {
  if (!req.session.agentId) {
    return res.json({ loggedIn: false });
  }

  const user = users[req.session.agentId] || {};
  const callbackNumber = getDialNumberForAgent(req.session.agentId) || null;

  return res.json({
    loggedIn: true,
    agentId: req.session.agentId,
    agentName: req.session.agentName,
    allowAfterHours: !!user.allowAfterHours,
    callbackNumber,
    campaignId: req.session.assignedCampaignId || null,
    campaignName: req.session.assignedCampaignId
      ? (campaigns[req.session.assignedCampaignId]?.name || req.session.assignedCampaignId)
      : null
  });
});

// ---------- END AUTH / ADMIN ----------

// serve static files
app.use('/public', express.static(path.join(__dirname, 'public')));

// =========================
//   ENV / TWILIO CONFIG
// =========================

const {
  TWILIO_ACCOUNT_SID,
  TWILIO_AUTH_TOKEN,
  BASE_URL,
  ZAPIER_HOOK_URL,
  SLACK_WEBHOOK_URL,
  PORT,
  GHL_API_KEY,
  GHL_LOCATION_ID,
  GHL_BASE_URL,
  GHL_LOCK_TAG
} = process.env;

const GHL_API_VERSION = '2021-07-28';
const GHL_BASE_ENDPOINT = GHL_BASE_URL || 'https://services.leadconnectorhq.com';
const DIALER_LOCK_TAG = GHL_LOCK_TAG || 'RehashDialer:Locked';

console.log('Loaded Twilio SID:', TWILIO_ACCOUNT_SID);
console.log(
  'Auth token length:',
  TWILIO_AUTH_TOKEN ? TWILIO_AUTH_TOKEN.length : 'missing'
);

const client = twilio(TWILIO_ACCOUNT_SID || '', TWILIO_AUTH_TOKEN || '');

// =========================
//   AGENT SLOTS (dial targets)
// =========================

const DEFAULT_AGENT_SLOTS = {
  outbound1: {
    id: 'outbound1',
    name: 'Slot 1',
    dialTarget: process.env.AGENT1_PHONE || '+10000000001'
  },
  outbound2: {
    id: 'outbound2',
    name: 'Slot 2',
    dialTarget: process.env.AGENT2_PHONE || '+10000000002'
  },
  outbound3: {
    id: 'outbound3',
    name: 'Slot 3',
    dialTarget: process.env.AGENT3_PHONE || '+10000000003'
  },
  outbound4: {
    id: 'outbound4',
    name: 'Slot 4',
    dialTarget: process.env.AGENT4_PHONE || '+10000000004'
  },
  outbound5: {
    id: 'outbound5',
    name: 'Slot 5',
    dialTarget: process.env.AGENT5_PHONE || '+10000000005'
  },
  outbound6: {
    id: 'outbound6',
    name: 'Slot 6',
    dialTarget: process.env.AGENT6_PHONE || '+10000000006'
  },
  outbound7: {
    id: 'outbound7',
    name: 'Slot 7',
    dialTarget: process.env.AGENT7_PHONE || '+10000000007'
  },
  outbound8: {
    id: 'outbound8',
    name: 'Slot 8',
    dialTarget: process.env.AGENT8_PHONE || '+10000000008'
  },
  outbound9: {
    id: 'outbound9',
    name: 'Slot 9',
    dialTarget: process.env.AGENT9_PHONE || '+10000000009'
  },
  outbound10: {
    id: 'outbound10',
    name: 'Slot 10',
    dialTarget: process.env.AGENT10_PHONE || '+10000000010'
  }
};

function loadAgentSlotsFromFile() {
  try {
    if (!fs.existsSync(AGENT_SLOTS_FILE)) {
      fs.writeFileSync(AGENT_SLOTS_FILE, JSON.stringify(DEFAULT_AGENT_SLOTS, null, 2));
      return { ...DEFAULT_AGENT_SLOTS };
    }
    const raw = fs.readFileSync(AGENT_SLOTS_FILE, 'utf8');
    const parsed = JSON.parse(raw);
    const normalized = {};
    Object.values(parsed || {}).forEach(slot => {
      const norm = normalizeSlot(slot);
      if (norm.id) normalized[norm.id] = norm;
    });
    return Object.keys(normalized).length ? normalized : { ...DEFAULT_AGENT_SLOTS };
  } catch (err) {
    console.error('Error loading agent slots:', err);
    return { ...DEFAULT_AGENT_SLOTS };
  }
}

function saveAgentSlots(map) {
  try {
    fs.writeFileSync(AGENT_SLOTS_FILE, JSON.stringify(map, null, 2));
  } catch (err) {
    console.error('Error saving agent slots:', err);
  }
}

function normalizeSlot(slot = {}) {
  const id = normalizeUsername(slot.id);
  if (!id) return { id: null, name: '', dialTarget: '', inboundNumber: '' };
  return {
    id,
    name: slot.name || id,
    dialTarget: normalizePhone(slot.dialTarget),
    inboundNumber: normalizePhone(slot.inboundNumber)
  };
}

let agentSlots = loadAgentSlotsFromFile();

// test queues – replace with real lead source later
const leadsQueue = {
  'old-bids': [
    { id: 1, name: 'Old Bid Lead', phone: '+14804476460' }
  ],
  'second-visit': [
    { id: 2, name: 'Second Visit', phone: '+14805551234' }
  ],
  'lost-estimates': [
    { id: 3, name: 'Lost Estimate', phone: '+14805555678' }
  ],
  default: [
    { id: 4, name: 'Test Lead', phone: '+14804476460' }
  ]
};

function getNextLocalLead(campaignId) {
  const queue = leadsQueue[campaignId] || leadsQueue.default;
  if (!queue || !queue.length) return null;
  return queue.shift();
}

function getAvailableAgents(agentIds = []) {
  return (agentIds || [])
    .map(normalizeUsername)
    .filter(Boolean)
    .filter(id => !activeCallByAgent[id] && users[id]);
}

function getDialNumberForAgent(agentId) {
  const normalizedId = normalizeUsername(agentId);
  if (!normalizedId) return null;
  const user = users[normalizedId];
  if (!user || !user.outboundNumber) return null;
  const normalized = normalizePhone(user.outboundNumber);
  return normalized || null;
}

function selectInboundTargets(fromNumber, toNumber) {
  const preferredAgent = normalizeUsername(lastAgentByNumber[fromNumber]);
  const allAgents = Object.keys(users || {}).map(normalizeUsername).filter(Boolean);
  const normalizedTo = normalizePhone(toNumber);

  // If an inboundNumber is configured for a user matching the dialed number,
  // route to that user first (if available and not on a call).
  if (normalizedTo) {
    const matchedAgents = Object.entries(users || {})
      .filter(([id, info]) => normalizePhone(info.inboundNumber) === normalizedTo)
      .map(([id]) => id);
    const matchAvailable = getAvailableAgents(matchedAgents).filter(id => {
      const status = getAgentStatus(id);
      return status === 'online' || status === 'away';
    });
    if (matchAvailable.length) {
      return matchAvailable;
    }
  }

  // Otherwise, get all agents who are free and at least away/online.
  const available = getAvailableAgents(allAgents).filter(id => {
    const status = getAgentStatus(id);
    return status === 'online' || status === 'away';
  });

  if (!available.length) return [];

  const statusOrder = { online: 0, away: 1 };
  available.sort((a, b) => {
    const sa = getAgentStatus(a);
    const sb = getAgentStatus(b);
    return (statusOrder[sa] ?? 2) - (statusOrder[sb] ?? 2);
  });

  // If the last agent who spoke with this caller is available, prefer them.
  if (preferredAgent && available.includes(preferredAgent)) {
    return [preferredAgent];
  }

  // Otherwise, return the top 1–2 best candidates.
  return available.slice(0, 2);
}

function buildInboundDial(twiml, targets, toNumber) {
  const normalizedTo = normalizePhone(toNumber);
  const effectiveCallerId = normalizedTo || defaultCallerId;
  const dial = twiml.dial({ timeout: 20, callerId: effectiveCallerId });
  targets.forEach(agentId => {
    dial.client(agentId);
  });
}

function buildSequentialInbound(twiml, fromNumber, toNumber) {
  const targets = selectInboundTargets(fromNumber, toNumber);
  if (!targets.length) return false;
  const primary = targets[0];
  const fallback = targets.slice(1, 3); // up to 2 more

  const attempted = encodeURIComponent(primary);
  const actionUrl = `${BASE_URL}/twilio/inbound/fallback?from=${encodeURIComponent(fromNumber || '')}&to=${encodeURIComponent(toNumber || '')}&attempted=${attempted}`;
  const dial = twiml.dial({ timeout: 10, callerId: normalizePhone(toNumber) || defaultCallerId, action: actionUrl });
  dial.client(primary);
  return true;
}

const metricsByAgent = loadAgentMetricsStore();
const callMap = {};
const activeLeadMetaByAgent = {};
const activeCallByAgent = {};

const ghlClient = GHL_API_KEY
  ? axios.create({
      baseURL: GHL_BASE_ENDPOINT,
      headers: {
        Authorization: `Bearer ${GHL_API_KEY}`,
        Version: GHL_API_VERSION,
        Accept: 'application/json',
        'Content-Type': 'application/json'
      }
    })
  : null;

function formatGhlError(err, fallback) {
  const prefix = fallback || 'GHL request failed';
  if (!err) return prefix;
  const data = err.response?.data;
  const parts = [];
  if (typeof data === 'string') {
    parts.push(data);
  } else if (data && typeof data === 'object') {
    if (data.message) parts.push(data.message);
    if (data.error && typeof data.error === 'object') {
      if (data.error.message) parts.push(data.error.message);
      if (data.error.error) parts.push(data.error.error);
    }
    if (!parts.length) {
      parts.push(JSON.stringify(data));
    }
  } else if (err.message) {
    parts.push(err.message);
  }
  return `${prefix}${parts.length ? `: ${parts.join(' • ')}` : ''}`;
}

function isGhlCampaign(campaign) {
  return Boolean(
    ghlClient &&
    GHL_LOCATION_ID &&
    campaign &&
    campaign.ghlPipelineId &&
    campaign.ghlTag
  );
}

function extractContactPhone(contact) {
  if (!contact) return null;
  if (typeof contact.phone === 'string' && contact.phone.trim()) {
    return contact.phone.trim();
  }
  const phones = contact.contactPhones || contact.phoneNumbers || [];
  const firstPhone = phones.find(p => p && (p.phone || p.number));
  if (firstPhone && (firstPhone.phone || firstPhone.number)) {
    return (firstPhone.phone || firstPhone.number).trim();
  }
  if (Array.isArray(contact.phone)) {
    const raw = contact.phone.find(Boolean);
    if (raw) return raw.trim();
  }
  return null;
}

function extractContactEmail(contact) {
  if (!contact) return null;
  if (typeof contact.email === 'string' && contact.email.trim()) {
    return contact.email.trim();
  }
  const emails = contact.contactEmails || contact.emails || [];
  const firstEmail = emails.find(e => e && (e.email || e.address));
  if (firstEmail && (firstEmail.email || firstEmail.address)) {
    return (firstEmail.email || firstEmail.address).trim();
  }
  if (Array.isArray(contact.email)) {
    const raw = contact.email.find(Boolean);
    if (raw) return raw.trim();
  }
  return null;
}

function buildContactName(contact = {}) {
  const { firstName, lastName, name, fullName, companyName } = contact;
  const composite = `${firstName || ''} ${lastName || ''}`.trim();
  return composite || name || fullName || companyName || 'Lead';
}

function normalizeTagList(tags) {
  if (!tags) return [];
  if (Array.isArray(tags)) {
    return tags.map(tag => {
      if (!tag) return null;
      if (typeof tag === 'string') return tag;
      return tag.name || tag.tag || null;
    }).filter(Boolean);
  }
  return [];
}

async function ghlAddTags(contactId, tags = []) {
  if (!ghlClient || !contactId || !tags.length) return;
  try {
    await ghlClient.post(
      `/contacts/${contactId}/tags/`,
      { tags },
      { params: { locationId: GHL_LOCATION_ID } }
    );
  } catch (err) {
    console.error('Error adding GHL tags:', err.response?.data || err.message);
  }
}

async function ghlRemoveTags(contactId, tags = []) {
  if (!ghlClient || !contactId || !tags.length) return;
  try {
    await ghlClient.post(
      `/contacts/${contactId}/tags/remove`,
      { tags },
      { params: { locationId: GHL_LOCATION_ID } }
    );
  } catch (err) {
    console.error('Error removing GHL tags:', err.response?.data || err.message);
  }
}

async function ghlAddNote(contactId, text) {
  if (!ghlClient || !contactId || !text) return;
  try {
    await ghlClient.post(
      `/contacts/${contactId}/notes/`,
      { body: text },
      { params: { locationId: GHL_LOCATION_ID } }
    );
  } catch (err) {
    console.error('Error adding GHL note:', err.response?.data || err.message);
  }
}

async function ghlUpdateOpportunity(opportunityId, payload = {}) {
  // Temporarily disabled due to 422 from IAM service; re-enable when allowed
  return;
}

async function ghlFetchContact(contactId) {
  if (!ghlClient || !contactId) return null;
  try {
    const res = await ghlClient.get(`/contacts/${contactId}`, {
      params: { locationId: GHL_LOCATION_ID }
    });
    return res.data?.contact || res.data?.data || res.data;
  } catch (err) {
    console.error('Error fetching GHL contact:', err.response?.data || err.message);
    return null;
  }
}

async function ghlSearchContactByPhone(phone) {
  if (!ghlClient || !phone) return null;
  try {
    const res = await ghlClient.get('/contacts/search', {
      params: {
        locationId: GHL_LOCATION_ID,
        query: phone
      }
    });
    const contacts = res.data?.contacts || res.data?.data || [];
    if (!contacts.length) return null;
    // Best-effort: pick exact phone match if present
    const normalized = normalizeLocalPhone(phone);
    const exact = contacts.find(c => normalizeLocalPhone(extractContactPhone(c)) === normalized);
    return exact || contacts[0];
  } catch (err) {
    console.error('Error searching GHL contact by phone:', err.response?.data || err.message);
    return null;
  }
}

async function ghlCreateContact(payload = {}) {
  if (!ghlClient) return null;
  try {
    const res = await ghlClient.post('/contacts/', payload, {
      params: { locationId: GHL_LOCATION_ID }
    });
    return res.data?.contact || res.data?.data || res.data || null;
  } catch (err) {
    console.error('Error creating GHL contact:', err.response?.data || err.message);
    return null;
  }
}

async function ghlUpdateContactEmail(contactId, email) {
  if (!ghlClient || !contactId || !email) return;
  try {
    await ghlClient.put(
      `/contacts/${contactId}`,
      { email },
      { params: { locationId: GHL_LOCATION_ID } }
    );
  } catch (err) {
    console.error('Error updating GHL contact email:', err.response?.data || err.message);
    throw err;
  }
}

async function fetchGhlLeadForCampaign(campaign) {
  if (!isGhlCampaign(campaign)) return null;
  try {
    const campaignId = campaign.id || campaign.campaignId || null;
    const stats = ensureCampaignStats(campaignId);
    const byContact = stats.byContact || {};
    let page = 1;
    const limit = 50;

    while (true) {
      const params = {
        location_id: GHL_LOCATION_ID,
        pipeline_id: campaign.ghlPipelineId,
        pipeline_stage_id: campaign.ghlStageId || undefined,
        limit,
        page
      };
      const res = await ghlClient.get('/opportunities/search', { params });
      const collection = res.data?.opportunities || res.data?.data || [];
      if (!collection.length) break;

      for (const opp of collection) {
        const contactId = opp.contactId || opp.contact_id || null;
        const contact =
          opp.contact ||
          (contactId ? await ghlFetchContact(contactId) : null);
        if (!contact) continue;
        if (isContactLocked(contactId)) continue;

        const contactKey = String(contactId || contact.id);
        const contactStats = byContact[contactKey];

        // Skip if this contact has already been marked as a bad number
        if (contactStats && contactStats.lastOutcome === 'bad_number') {
          continue;
        }

        // Global cooldown: skip any contact with a recent attempt, regardless of outcome
        if (contactStats && contactStats.lastAttemptMs) {
          const now = Date.now();
          if (now - contactStats.lastAttemptMs < GLOBAL_RETRY_COOLDOWN_MS) {
            continue;
          }
        }
        if (contactStats && NON_PICKUP_OUTCOMES.includes(contactStats.lastOutcome || '')) {
          const now = Date.now();
          if (contactStats.lastAttemptMs) {
            const ageMs = now - contactStats.lastAttemptMs;
            // For all non-pickup outcomes (no answer, busy, failed, machine, voicemail, callback requested),
            // enforce at least 24 hours between attempts.
            if (ageMs < NON_PICKUP_COOLDOWN_MS) {
              continue;
            }
          }
          if ((contactStats.attempts || 0) >= MAX_NON_PICKUP_ATTEMPTS) {
            // extra guard: if attempts >= 3, skip entirely
            continue;
          }
        }

        const tags = normalizeTagList(contact.tags);
        if (!tags.includes(campaign.ghlTag)) continue;
        // Skip if contact already has a final disposition we don't want to redial
        const hasFinalDispo = tags.some(t => {
          if (!t) return false;
          const lower = t.toLowerCase();
          return DISPOSITION_SKIP_TAGS.includes(lower);
        });
        if (hasFinalDispo) continue;
        const phone = extractContactPhone(contact);
        if (!phone) continue;
        if (isRecentlyDialed(campaignId, phone, contactId || contact.id)) continue;

        if (campaign.ghlStageId) {
          await ghlUpdateOpportunity(opp.id, {
            stageId: campaign.ghlStageId,
            status: opp.status || 'open'
          });
        }

        lockContact(contact.id, campaignId, null);
        return {
          id: contact.id,
          name: buildContactName(contact),
          phone,
          email: contact.email || null,
          ghlOpportunityId: opp.id,
          ghlContactId: contact.id || contactId || null,
          ghlPipelineId: opp.pipelineId || opp.pipeline_id,
          ghlStageId: opp.stageId || opp.stage_id,
          campaignTag: campaign.ghlTag,
          // surface attempt metadata to the dialer UI so agents can see history
          attempts: contactStats ? (contactStats.attempts || 0) : 0,
          lastOutcome: contactStats ? (contactStats.lastOutcome || null) : null,
          lastAttemptMs: contactStats ? (contactStats.lastAttemptMs || null) : null
        };
      }

      if (collection.length < limit) break;
      page += 1;
      // Safety guard: avoid unbounded paging in pathological cases
      if (page > 40) break; // up to ~2000 opportunities
    }
  } catch (err) {
    console.error('GHL fetch error:', err.response?.data || err.message);
  }
  return null;
}

function getContactAttemptSummary(contactId) {
  if (!contactId) return null;
  const key = String(contactId);
  let attempts = 0;
  let lastOutcome = null;
  let lastAttemptMs = null;
  Object.values(campaignStats || {}).forEach(stats => {
    if (!stats || !stats.byContact) return;
    const cStats = stats.byContact[key];
    if (!cStats) return;
    attempts += cStats.attempts || 0;
    if (typeof cStats.lastAttemptMs === 'number') {
      if (!lastAttemptMs || cStats.lastAttemptMs > lastAttemptMs) {
        lastAttemptMs = cStats.lastAttemptMs;
        lastOutcome = cStats.lastOutcome || null;
      }
    }
  });
  if (!attempts && !lastAttemptMs) return null;
  return { attempts, lastOutcome, lastAttemptMs };
}

async function handleGhlDisposition(agentId, campaignId, meta, outcome, notes, leadDetails) {
  if (!meta) return;
  const campaign = campaigns[campaignId] || {};
  const safeOutcome = outcome || 'unknown';
  const friendlyOutcome = OUTCOME_LABELS[safeOutcome] || safeOutcome;
  const outcomeTag = (!safeOutcome || safeOutcome === 'connected') ? null : safeOutcome; // only tag meaningful dispositions
  let contactId = meta.ghlContactId || null;

  if (!contactId && leadDetails?.leadPhone) {
    const contact = await ghlSearchContactByPhone(leadDetails.leadPhone);
    if (contact && contact.id) {
      contactId = contact.id;
    }
  }

  if (!contactId) {
    console.warn('handleGhlDisposition: no contactId available to tag/note.');
    return;
  }
  console.log('handleGhlDisposition: using contactId', contactId, 'outcome', safeOutcome);

  const lines = [
    `Disposition: ${friendlyOutcome}`,
    agentId ? `Agent: ${agentId}` : null,
    notes ? `Notes: ${notes}` : null
  ].filter(Boolean);

  if (outcomeTag) {
    try {
      await ghlAddTags(contactId, [outcomeTag]);
    } catch (err) {
      console.error('handleGhlDisposition: add outcome tag failed', err.response?.data || err.message);
    }
  }

  if (lines.length && safeOutcome !== 'connected') {
    try {
      await ghlAddNote(contactId, lines.join('\n'));
    } catch (err) {
      console.error('handleGhlDisposition: add note failed', err.response?.data || err.message);
    }
  }
  if (meta.ghlOpportunityId) {
    const stagePayload = {};
    if (campaign.ghlStageId) {
      stagePayload.pipeline_stage_id = campaign.ghlStageId;
    }
    if (Object.keys(stagePayload).length) {
      await ghlUpdateOpportunity(meta.ghlOpportunityId, stagePayload);
    }
  }
}

async function getCampaignLeadStats(campaign) {
  if (!isGhlCampaign(campaign)) return null;
  if (!ghlClient) return null;
  try {
    let page = 1;
    const limit = 100;
    let total = 0;
    let remaining = 0;
    const outcomePrefix = `${campaign.ghlTag}:`;
    while (true) {
      const params = {
        location_id: GHL_LOCATION_ID,
        pipeline_id: campaign.ghlPipelineId,
        pipeline_stage_id: campaign.ghlStageId || undefined,
        page,
        limit
      };
      const res = await ghlClient.get('/opportunities/search', { params });
      const opportunities = res.data?.opportunities || res.data?.data || [];
      if (!opportunities.length) break;
      for (const opp of opportunities) {
        const contact = opp.contact || (opp.contactId ? await ghlFetchContact(opp.contactId) : null);
        if (!contact) continue;
        const tags = normalizeTagList(contact.tags);
        if (!tags.includes(campaign.ghlTag)) continue;
        total += 1;
        const hasOutcome = tags.some(tag => tag && tag.startsWith(outcomePrefix));
        if (!hasOutcome) {
          remaining += 1;
        }
      }
      if (opportunities.length < limit) break;
      page += 1;
    }
    return { total, remaining };
  } catch (err) {
    console.error('Error counting GHL leads:', err.response?.data || err.message);
    return null;
  }
}

// =========================
//   SESSION HELPER
// =========================

function requireAgent(req, res) {
  const agentId = req.session.agentId;
  if (!agentId) {
    res.status(401).json({ error: 'Not logged in as agent' });
    return null;
  }
  return String(agentId);
}

// =========================
//   VOICE TOKEN FOR BROWSER
// =========================

app.get('/api/voice-token', (req, res) => {
  const agentId = requireAgent(req, res);
  if (!agentId) return; // requireAgent already sent 401

  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const apiKey = process.env.TWILIO_API_KEY;
  const apiSecret = process.env.TWILIO_API_SECRET;

  if (!accountSid || !apiKey || !apiSecret) {
    console.error('Missing TWILIO_ACCOUNT_SID or TWILIO_API_KEY / SECRET');
    return res
      .status(500)
      .json({ error: 'Twilio API key/secret not configured' });
  }

  // Identity should match the <Client> identity in /twilio/voice
  const identity = String(agentId); // e.g. "outbound1"

  const token = new AccessToken(accountSid, apiKey, apiSecret, {
    identity
  });

  const voiceGrant = new VoiceGrant({
    incomingAllow: true
    // We don't need outgoingApplicationSid yet – browser isn't dialing out directly
  });

  token.addGrant(voiceGrant);

  const jwt = token.toJwt();
  res.json({ token: jwt, identity });
});

// =========================
//   LOCAL PRESENCE / HELPERS
// =========================

function loadLocalPresence() {
  try {
    if (!fs.existsSync(LOCAL_PRESENCE_FILE)) {
      fs.writeFileSync(LOCAL_PRESENCE_FILE, JSON.stringify({}, null, 2));
      return {};
    }
    const raw = fs.readFileSync(LOCAL_PRESENCE_FILE, 'utf8');
    return JSON.parse(raw) || {};
  } catch (err) {
    console.error('Error loading local presence map:', err);
    return {};
  }
}

function saveLocalPresence(map) {
  try {
    fs.writeFileSync(LOCAL_PRESENCE_FILE, JSON.stringify(map, null, 2));
  } catch (err) {
    console.error('Error saving local presence map:', err);
  }
}

let localPresenceMap = loadLocalPresence();
const defaultCallerId = '+14158304289';

function getAreaCode(phone) {
  if (!phone || !phone.startsWith('+1') || phone.length < 5) return null;
  return phone.slice(2, 5);
}

function chooseLocalNumberForLead(leadPhone) {
  const area = getAreaCode(leadPhone);
  if (area && localPresenceMap[area]) return normalizePhone(localPresenceMap[area]);
  return defaultCallerId;
}

// =========================
//   METRICS HELPERS
// =========================

function ensureAgentMetrics(agentId) {
  if (!metricsByAgent[agentId]) {
    metricsByAgent[agentId] = {
      totalCalls: 0,
      answeredHuman: 0,
      answeredMachine: 0,
      noAnswer: 0,
      busy: 0,
      failed: 0,
      conversions: 0,
      lastOutcome: null,
      lastLeadName: null,
      lastTimestamp: null,
      lastActivityMs: null,
      totalTalkTimeSec: 0,
      lastCallDurationSec: 0,
      avgCallDurationSec: 0,
      completedCallCount: 0,
      currentCallStartMs: null
    };
  }
  return metricsByAgent[agentId];
}

function markAgentActivity(agentId) {
  const m = ensureAgentMetrics(agentId);
  m.lastActivityMs = Date.now();
}

function getAgentStatus(agentId) {
  if (activeCallByAgent[agentId]) return 'on_call';
  const m = ensureAgentMetrics(agentId);
  if (!m.lastActivityMs) return 'offline';

  const diffMinutes = (Date.now() - m.lastActivityMs) / 60000;

  if (diffMinutes <= 2) return 'online';
  if (diffMinutes <= 10) return 'away';
  return 'offline';
}

// =========================
//      FRONTEND ROUTES
// =========================

// Dialer as home page
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dialer.html'));
});

// Keep /dialer for backwards compatibility
app.get('/dialer', (req, res) => {
  res.redirect('/');
});

// Admin console
app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// campaigns list (agent-safe)
app.get('/api/campaigns', (req, res) => {
  res.json(listCampaignArray());
});

app.get('/api/metrics/:agentId', (req, res) => {
  const { agentId } = req.params;
  const easternNow = ensureLeaderboardWeek();
  const metrics = ensureAgentMetrics(agentId);
  const status = getAgentStatus(agentId);
  let statusDurationSec = null;
  if (status === 'on_call' && metrics.currentCallStartMs) {
    statusDurationSec = Math.floor((Date.now() - metrics.currentCallStartMs) / 1000);
  } else if (metrics.lastActivityMs) {
    statusDurationSec = Math.floor((Date.now() - metrics.lastActivityMs) / 1000);
  }
  const dialerDayId = getDialerDayId(easternNow);
  const dailyAgent = (dailyAgentReport[dialerDayId] && dailyAgentReport[dialerDayId][agentId])
    ? dailyAgentReport[dialerDayId][agentId]
    : null;

  let todayAnsweredHuman = 0;
  let todayConversions = 0;
  if (dailyAgent) {
    todayAnsweredHuman = dailyAgent.liveConnects || 0;
    const dispositions = dailyAgent.dispositions || {};
    todayConversions = dispositions.booked || 0;
  }

  res.json({
    agentId,
    metrics: {
      ...metrics,
      status,
      statusDurationSec,
      todayAnsweredHuman,
      todayConversions
    }
  });
});

// ============ SCRIPT MANAGEMENT ============

app.get('/api/admin/scripts', (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  res.json({ ok: true, scripts: scriptsStore });
});

app.post('/api/admin/scripts', express.json({ limit: '1mb' }), (req, res) => {
  if (!isValidAdmin(req)) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  const { id, title, sections } = req.body || {};
  const key = (id || 'default').toString().trim() || 'default';
  const safeTitle = (title || 'Call Script').toString().trim() || 'Call Script';
  const safeSections = Array.isArray(sections) ? sections.map(s => ({
    heading: (s.heading || '').toString(),
    body: (s.body || '').toString()
  })) : [];

  scriptsStore[key] = { title: safeTitle, sections: safeSections };
  saveScriptsStore();

  res.json({ ok: true, script: scriptsStore[key] });
});

app.get('/api/scripts/current', (req, res) => {
  const { campaignId } = req.query || {};
  let scriptKey = 'default';
  if (campaignId && scriptsStore[campaignId]) {
    scriptKey = campaignId;
  }
  const script = scriptsStore[scriptKey] || scriptsStore.default || { title: 'Call Script', sections: [] };
  res.json({ ok: true, scriptKey, script });
});

app.get('/api/incoming-lookup', async (req, res) => {
  const { phone } = req.query;
  if (!phone) {
    return res.status(400).json({ success: false, error: 'phone is required' });
  }
  ensureLeaderboardWeek();
  const normalized = normalizeLocalPhone(phone) || normalizePhone(phone);
  let contact = null;
  let contactId = null;
  let contactPhone = normalized;
  let email = null;
  let name = null;

  try {
    contact = await ghlSearchContactByPhone(normalized || phone);
  } catch (err) {
    console.error('incoming lookup GHL error:', err.response?.data || err.message);
  }

  if (contact) {
    contactId = contact.id || contact.contactId || null;
    contactPhone = extractContactPhone(contact) || normalized;
    email = extractContactEmail(contact) || null;
    name = buildContactName(contact);
  }

  let attempts = 0;
  let lastOutcome = null;
  let lastAttemptMs = null;
  if (contactId) {
    const summary = getContactAttemptSummary(contactId);
    if (summary) {
      attempts = summary.attempts || 0;
      lastOutcome = summary.lastOutcome || null;
      lastAttemptMs = summary.lastAttemptMs || null;
    }
  }

  if (!contact && !attempts && !lastAttemptMs) {
    return res.json({ success: false, contact: null });
  }

  res.json({
    success: true,
    contact: {
      name: name || null,
      phone: contactPhone || normalized || phone,
      email: email || null,
      attempts,
      lastOutcome,
      lastAttemptMs
    }
  });
});

// leaderboard used by admin dashboard
app.get('/api/leaderboard', (req, res) => {
  ensureLeaderboardWeek();
  const rows = Object.entries(metricsByAgent).map(([agentId, m]) => {
    const status = getAgentStatus(agentId);
    let statusDurationSec = null;
    if (status === 'on_call' && m.currentCallStartMs) {
      statusDurationSec = Math.floor((Date.now() - m.currentCallStartMs) / 1000);
    } else if (m.lastActivityMs) {
      statusDurationSec = Math.floor((Date.now() - m.lastActivityMs) / 1000);
    }
    return {
      agentId,
      totalCalls: m.totalCalls || 0,
      live: m.answeredHuman || 0,
      conversions: m.conversions || 0,
      status,
      statusDurationSec,
      avgCallDurationSec: m.avgCallDurationSec || 0,
      lastCallDurationSec: m.lastCallDurationSec || 0,
      totalTalkTimeSec: m.totalTalkTimeSec || 0,
      completedCallCount: m.completedCallCount || 0
    };
  });

  rows.sort((a, b) => {
    if (b.conversions !== a.conversions) return b.conversions - a.conversions;
    return b.live - a.live;
  });

  rows.forEach((row, index) => {
    row.rank = index + 1;
  });

  let onlineCount = 0;
  let awayCount = 0;
  let offlineCount = 0;
  let totalTalkTimeSec = 0;
  let totalCompletedCalls = 0;
  let lastCallDurationSec = 0;

  rows.forEach(row => {
    if (row.status === 'online') onlineCount += 1;
    else if (row.status === 'away') awayCount += 1;
    else offlineCount += 1;

    totalTalkTimeSec += row.totalTalkTimeSec || 0;
    totalCompletedCalls += row.completedCallCount || 0;
    if ((row.lastCallDurationSec || 0) > 0) {
      lastCallDurationSec = row.lastCallDurationSec;
    }
  });

  const avgCallDurationSec = totalCompletedCalls
    ? Math.round(totalTalkTimeSec / totalCompletedCalls)
    : 0;

  const sessionAgentId = req.session.agentId
    ? String(req.session.agentId)
    : null;
  const queryAgentId = req.query.agentId;
  const meId = sessionAgentId || queryAgentId || null;

  let me = null;
  if (meId) {
    me = rows.find(r => r.agentId === String(meId)) || null;
  }

  res.json({
    totalAgents: rows.length,
    onlineAgents: onlineCount,
    awayAgents: awayCount,
    offlineAgents: offlineCount,
    avgCallDurationSec,
    lastCallDurationSec,
    me,
    top: rows
  });
});

// =========================
//        DIALER API
// =========================

app.post('/api/dialer/next', async (req, res) => {
  const agentId = requireAgent(req, res);
  if (!agentId) return;
  const easternNow = ensureLeaderboardWeek();
  const reportDateId = getDateId(easternNow);

  const bypassHours = hasAfterHoursOverride(agentId);
  if (!bypassHours && !isWithinBusinessHours(easternNow)) {
    return res.status(403).json({
      success: false,
      error: 'Outbound dialing is only allowed between 9:00AM and 8:00PM EST, Monday–Saturday.'
    });
  }

  const { campaignId } = req.body || {};
  const sessionCampaignId = req.session.assignedCampaignId || null;
  const resolvedCampaignId = campaignId || sessionCampaignId || null;
  if (sessionCampaignId && campaignId && sessionCampaignId !== campaignId) {
    return res.status(400).json({ success: false, error: 'Assigned campaign mismatch' });
  }
  if (!resolvedCampaignId) {
    return res.status(400).json({ success: false, error: 'No campaign assigned to agent' });
  }
  const user = users[agentId] || {};
  const backupCampaignId = user.backupCampaignId || null;

  const candidateIds = [];
  if (resolvedCampaignId) candidateIds.push(resolvedCampaignId);
  if (backupCampaignId && backupCampaignId !== resolvedCampaignId) {
    candidateIds.push(backupCampaignId);
  }

  let chosenCampaignId = null;
  let campaign = null;
  let lead = null;

  for (const cid of candidateIds) {
    const c = campaigns[cid];
    if (!c) continue;
    let candidateLead = null;
    if (isGhlCampaign(c)) {
      candidateLead = await fetchGhlLeadForCampaign(c);
    }
    if (!candidateLead) {
      candidateLead = popLocalLead(cid);
    }
    if (candidateLead) {
      chosenCampaignId = cid;
      campaign = c;
      lead = candidateLead;
      break;
    }
  }

  if (!lead || !campaign) {
    return res.json({
      success: false,
      error: 'No leads in queue for assigned or backup campaigns'
    });
  }
  const fromNumber =
    getDialNumberForAgent(agentId) ||
    chooseLocalNumberForLead(lead.phone) ||
    defaultCallerId;
  if (!fromNumber) {
    return res.status(400).json({ success: false, error: 'No caller ID available' });
  }
  console.log('[Dialer outbound]', { agentId, fromNumber, leadPhone: lead.phone });

  try {
    const call = await client.calls.create({
      to: lead.phone,
      from: fromNumber,
      url: `${BASE_URL}/twilio/voice?agentId=${agentId}`,
      statusCallback: `${BASE_URL}/twilio/status`,
      statusCallbackEvent: ['initiated', 'ringing', 'answered', 'completed'],
      ...(appSettings.machineDetectionEnabled
        ? { machineDetection: 'Enable', machineDetectionTimeout: 30 }
        : {})
    });

    const leadPayload = {
      ...lead,
      campaignId: chosenCampaignId,
      campaignName: campaign.name
    };

    callMap[call.sid] = { agentId, lead: leadPayload, campaignId: chosenCampaignId };
    activeCallByAgent[agentId] = call.sid;
    lastAgentByNumber[lead.phone] = agentId;

    const m = ensureAgentMetrics(agentId);
    if (isWithinWeeklyWindow(easternNow)) {
      m.totalCalls += 1;
      markAgentActivity(agentId);
    }

    if (isWithinDailyReportWindow(easternNow) && isWithinWeeklyReportWindow(easternNow)) {
      const daily = ensureDailyAgentMetric(reportDateId, agentId);
      daily.totalCalls += 1;
      saveReportMetrics();
    }

    activeLeadMetaByAgent[agentId] = {
      ghlOpportunityId: leadPayload.ghlOpportunityId || null,
      ghlContactId: leadPayload.ghlContactId || null,
      campaignId: chosenCampaignId,
      campaignTag: leadPayload.campaignTag || campaign.ghlTag || null,
      localLeadId: leadPayload.localLeadId || null,
      localLeadName: leadPayload.name || null,
      localLeadPhone: leadPayload.phone || null
    };
    markRecentlyDialed(resolvedCampaignId, leadPayload.phone, leadPayload.ghlContactId || leadPayload.id || leadPayload.localLeadId || null);

    const responseLead = { ...leadPayload };
    delete responseLead.ghlOpportunityId;
    delete responseLead.ghlContactId;
    delete responseLead.campaignTag;
    // keep localLeadId on response to show richer UI when offline leads are used

    res.json({
      success: true,
      twilioCallSid: call.sid,
      fromNumber,
      lead: responseLead
    });
  } catch (err) {
    console.error('Error creating outbound call:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Twilio calls this when connecting the agent leg. Keep this resilient to avoid “application error”.
app.all('/twilio/voice', (req, res) => {
  const VoiceResponse = twilio.twiml.VoiceResponse;
  const twiml = new VoiceResponse();

  try {
    const agentId = req.query.agentId;         // e.g. "outbound1"
    const answeredBy = req.body.AnsweredBy;    // "human" | "machine" | undefined
    const toNumber = req.body.To || null;

    console.log('Voice webhook – AnsweredBy:', answeredBy, 'Agent:', agentId, 'To:', toNumber);

    const machineDetectionOn = appSettings.machineDetectionEnabled;
    const callRecordingOn = appSettings.callRecordingEnabled;

    if (machineDetectionOn && answeredBy === 'machine') {
      twiml.hangup();
    } else {
      if (!agentId) {
        const fromNumber = req.body.From || req.query.From || null;
        const success = buildSequentialInbound(twiml, fromNumber, toNumber);
        if (!success) {
          twiml.say('No agents available. Please try again later.');
          twiml.hangup();
        }
      } else {
        const dialOptions = {};
        if (toNumber) dialOptions.callerId = toNumber; // optional callerId

        if (callRecordingOn) {
          dialOptions.record = 'record-from-answer-dual';
          if (BASE_URL) {
            dialOptions.recordingStatusCallback = `${BASE_URL}/twilio/recording`;
            dialOptions.recordingStatusCallbackEvent = 'in-progress completed';
          }
        }

        const dial = twiml.dial(dialOptions);
        dial.client(agentId);
      }
    }
  } catch (err) {
    console.error('Voice webhook error:', err);
    twiml.say('An application error occurred.');
    twiml.hangup();
  }

  res.type('text/xml');
  res.send(twiml.toString());
});

app.all('/twilio/status', (req, res) => {
  const easternNow = ensureLeaderboardWeek();
  const payload = req.body && Object.keys(req.body).length ? req.body : req.query || {};
  const { CallSid, CallStatus, AnsweredBy, CallDuration } = payload;

  console.log('[Twilio status]', req.method, 'CallSid:', CallSid, 'Status:', CallStatus, 'AnsweredBy:', AnsweredBy);

  const callInfo = callMap[CallSid];
  if (!callInfo) {
    return res.json({ received: true });
  }

  // If manual call answered, create contact/tag once
  if (callInfo.manual && CallStatus === 'answered' && !callInfo.manual.contactCreated) {
    (async () => {
      const phone = callInfo.manual.toNumber;
      const campaignId = callInfo.manual.campaignId || null;
      const campaign = campaignId ? campaigns[campaignId] : null;
      const tagList = [];
      if (campaign?.ghlTag) tagList.push(campaign.ghlTag);
      const contact = await ghlCreateContact({
        firstName: callInfo.manual.displayName || 'Manual Dial',
        phone,
        tags: tagList,
        locationId: GHL_LOCATION_ID
      });
      if (contact && contact.id) {
        callInfo.lead = {
          id: contact.id,
          name: contact.displayName || contact.firstName || contact.lastName || phone,
          phone,
          ghlContactId: contact.id,
          campaignId,
          campaignTag: campaign?.ghlTag || null
        };
        callInfo.manual.contactCreated = true;
      }
    })().catch(err => console.error('manual contact create failed', err.message));
  }

  const { agentId } = callInfo;
  const m = ensureAgentMetrics(agentId);

  const durationSec = parseInt(CallDuration, 10);
  const hasDuration = !Number.isNaN(durationSec) && durationSec >= 0;
  const LIVE_MIN_DURATION_SEC = 15;
  const isHumanLive =
    CallStatus === 'completed' &&
    AnsweredBy === 'human' &&
    hasDuration &&
    durationSec >= LIVE_MIN_DURATION_SEC;

  if (isWithinWeeklyWindow(easternNow)) {
    if (CallStatus === 'in-progress' || CallStatus === 'answered') {
      if (!m.currentCallStartMs) {
        m.currentCallStartMs = Date.now();
      }
    }

    if (CallStatus === 'completed') {
      if (isHumanLive) m.answeredHuman += 1;
      if (AnsweredBy === 'machine') m.answeredMachine += 1;
      if (hasDuration) {
        m.totalTalkTimeSec += durationSec;
        m.lastCallDurationSec = durationSec;
        m.completedCallCount += 1;
        if (m.completedCallCount > 0) {
          m.avgCallDurationSec = Math.round(m.totalTalkTimeSec / m.completedCallCount);
        }
      }
      m.currentCallStartMs = null;
    } else if (CallStatus === 'no-answer') {
      m.noAnswer += 1;
    } else if (CallStatus === 'busy') {
      m.busy += 1;
    } else if (CallStatus === 'failed') {
      m.failed += 1;
    }

    markAgentActivity(agentId);
    saveAgentMetricsStore();
  }

  // For outbound and manual calls, count a "live conversation" once per
  // completed human call that meets the duration threshold.
  if (
    isHumanLive &&
    isWithinDailyReportWindow(easternNow) &&
    isWithinWeeklyReportWindow(easternNow)
  ) {
    const reportDateId = getDateId(easternNow);
    const dailyAgent = ensureDailyAgentMetric(reportDateId, agentId);
    dailyAgent.liveConnects += 1;
    saveReportMetrics();
  }

  // Auto-mark hard failures with 0s duration as bad numbers so they are skipped in future
  if (CallStatus === 'failed') {
    const isZeroDuration = !CallDuration || Number.isNaN(durationSec) || durationSec === 0;
    if (isZeroDuration) {
      const lead = callInfo.lead || {};
      const campaignId = callInfo.campaignId || lead.campaignId || null;
      const safeCampaignId = campaignId ? String(campaignId) : null;
      const contactIdForStats = lead.ghlContactId || lead.id || null;
      const leadPhone = lead.phone || null;
      const leadName = lead.name || null;

      if (safeCampaignId || contactIdForStats || leadPhone) {
        recordCampaignDisposition(safeCampaignId, agentId, 'bad_number', contactIdForStats);
        // Fire-and-forget sync to GHL / local leads, similar to /api/disposition
        if (lead.localLeadId) {
          recordLocalLeadOutcome(
            safeCampaignId || lead.campaignId || null,
            { localLeadId: lead.localLeadId },
            'bad_number',
            '',
            { leadPhone, leadName }
          );
        }
        if (contactIdForStats) {
          (async () => {
            try {
              await handleGhlDisposition(
                agentId,
                safeCampaignId || lead.campaignId || null,
                {
                  ghlContactId: contactIdForStats,
                  ghlOpportunityId: lead.ghlOpportunityId || null,
                  campaignId: safeCampaignId || lead.campaignId || null,
                  campaignTag: lead.campaignTag || null
                },
                'bad_number',
                '',
                { leadPhone, leadName }
              );
            } catch (err) {
              console.error('Error auto-tagging bad_number on failed call:', err.response?.data || err.message);
            }
          })().catch(() => {});
        }
      }
    }
  }

  if (CallStatus === 'completed' || CallStatus === 'failed' || CallStatus === 'busy' || CallStatus === 'no-answer') {
    if (activeCallByAgent[agentId] === CallSid) {
      delete activeCallByAgent[agentId];
    }
    // unlock contact if we have it
    const lead = callInfo.lead || {};
    const contactId = lead.ghlContactId || lead.id || null;
    if (contactId) {
      unlockContact(contactId);
    }
  }

  res.json({ received: true });
});

app.post('/twilio/recording', (req, res) => {
  console.log('Recording callback:', req.body.CallSid, req.body.RecordingUrl, req.body.RecordingStatus);
  res.json({ received: true });
});

app.post('/api/contact/email', express.json(), async (req, res) => {
  const agentId = requireAgent(req, res);
  if (!agentId) return;
  const { email, phone } = req.body || {};
  if (!email) {
    return res.status(400).json({ ok: false, error: 'Email is required' });
  }

  let contactId = null;
  const meta = activeLeadMetaByAgent[agentId];
  if (meta && meta.ghlContactId) {
    contactId = meta.ghlContactId;
  } else if (phone) {
    const normalized = normalizeLocalPhone(phone) || normalizePhone(phone);
    try {
      const contact = await ghlSearchContactByPhone(normalized || phone);
      if (contact) {
        contactId = contact.id || contact.contactId || null;
      }
    } catch (err) {
      console.error('lookup contact by phone for email update failed:', err.response?.data || err.message);
    }
  }

  if (!contactId) {
    return res.status(404).json({ ok: false, error: 'No contact found to update' });
  }

  try {
    await ghlUpdateContactEmail(contactId, email);
    return res.json({ ok: true });
  } catch (err) {
    return res.status(500).json({ ok: false, error: 'Unable to update contact email' });
  }
});

app.post('/api/disposition', async (req, res) => {
  const { agentId, campaignId, outcome, notes, leadPhone, leadName } = req.body;
  const easternNow = ensureLeaderboardWeek();
  const normalizedAgentId = normalizeUsername(agentId);
  const dialInfo = users && normalizedAgentId ? users[normalizedAgentId] : null;

  const safeOutcome = outcome || 'other';
  console.log('Disposition:', {
    agentId,
    campaignId,
    outcome: safeOutcome,
    leadPhone,
    leadName
  });

  const m = ensureAgentMetrics(agentId);
  const metricOutcomeMap = {
    connected: null,
    booked: 'answeredHuman',
    not_interested: 'answeredHuman',
    callback_requested: 'answeredHuman',
    wrong_contact: 'answeredHuman',
    send_info_email: 'answeredHuman',
    general_email_info: 'answeredHuman',
    manual_email_info: 'answeredHuman',
    machine: 'answeredMachine',
    left_voicemail: 'answeredMachine',
    machine_voicemail: 'answeredMachine',
    no_answer: 'noAnswer',
    busy: 'busy',
    failed: 'failed',
    bad_number: 'failed'
  };

  const metricKey = metricOutcomeMap[safeOutcome];
  if (isWithinWeeklyWindow(easternNow)) {
    if (metricKey && typeof m[metricKey] === 'number') {
      m[metricKey] += 1;
    }

    if (safeOutcome === 'booked') m.conversions += 1;

    m.lastOutcome = safeOutcome;
    m.lastLeadName = leadName || null;
    m.lastTimestamp = new Date().toISOString();
    saveAgentMetricsStore();
  }

  const reportDateId = getDateId(easternNow);
  if (isWithinDailyReportWindow(easternNow) && isWithinWeeklyReportWindow(easternNow)) {
    const dailyAgent = ensureDailyAgentMetric(reportDateId, agentId);
    dailyAgent.dispositions[safeOutcome] = (dailyAgent.dispositions[safeOutcome] || 0) + 1;
    // Use the actual campaign that owns this lead (from meta) if request campaignId is missing/mismatched.
    const effectiveCampaignId = campaignId || (meta && meta.campaignId) || null;
    if (effectiveCampaignId) {
      const dailyCampaign = ensureDailyCampaignMetric(reportDateId, String(effectiveCampaignId));
      dailyCampaign.dispositions[safeOutcome] = (dailyCampaign.dispositions[safeOutcome] || 0) + 1;
    }
    saveReportMetrics();
  }
  let meta = activeLeadMetaByAgent[agentId];
  let contactIdForStats = meta?.ghlContactId || null;

  // If this was a manual call (no active meta) but we have a campaign + phone,
  // try to resolve the GHL contact so cooldowns and tags still apply.
  if (!contactIdForStats && campaignId && leadPhone && ghlClient && GHL_LOCATION_ID) {
    try {
      const normalizedPhone = normalizeLocalPhone(leadPhone) || normalizePhone(leadPhone);
      const contact = await ghlSearchContactByPhone(normalizedPhone || leadPhone);
      if (contact && contact.id) {
        contactIdForStats = contact.id;
        if (!meta) {
          const safeCampaignId = String(campaignId);
          const campaign = campaigns[safeCampaignId] || {};
          meta = {
            ghlContactId: contact.id,
            ghlOpportunityId: null,
            campaignId: safeCampaignId,
            campaignTag: campaign.ghlTag || null,
            localLeadId: null,
            localLeadName: leadName || null,
            localLeadPhone: normalizedPhone || leadPhone
          };
        } else if (!meta.ghlContactId) {
          meta.ghlContactId = contact.id;
        }
      }
    } catch (err) {
      console.error('lookup contact by phone for disposition failed:', err.response?.data || err.message);
    }
  }

  const effectiveCampaignIdForStats = campaignId || (meta && meta.campaignId) || null;
  recordCampaignDisposition(effectiveCampaignIdForStats, agentId, safeOutcome, contactIdForStats);

  if (meta) {
    const activeCallSid = activeCallByAgent[agentId] || null;
    const isAutosyncConnect = safeOutcome === 'connected';
    if (!isAutosyncConnect) {
      try {
        await handleGhlDisposition(
          agentId,
          campaignId || meta.campaignId,
          meta,
          safeOutcome,
          notes,
          { leadPhone, leadName }
        );
      } catch (err) {
        console.error('Error syncing disposition to GHL:', err.response?.data || err.message);
      }
      if (meta.localLeadId) {
        recordLocalLeadOutcome(
          campaignId || meta.campaignId,
          meta,
          safeOutcome,
          notes,
          { leadPhone, leadName }
        );
      }
      delete activeLeadMetaByAgent[agentId];
    }
  }

  // End the active call if it exists
  const activeCallSid = activeCallByAgent[agentId] || null;
  const isAutosyncConnect = safeOutcome === 'connected';
  if (activeCallSid && !isAutosyncConnect) {
    try {
      await client.calls(activeCallSid).update({ status: 'completed' });
    } catch (err) {
      console.error('Error hanging up call for disposition:', err.message);
    } finally {
      if (activeCallByAgent[agentId] === activeCallSid) {
        delete activeCallByAgent[agentId];
      }
    }
  }

  if (ZAPIER_HOOK_URL) {
    try {
      await axios.post(ZAPIER_HOOK_URL, {
        type: 'agent_disposition',
        outcome: safeOutcome,
        notes,
        leadPhone,
        leadName,
        agentId,
        agentName: dialInfo?.name || agentId,
        campaignId,
        timestamp: m.lastTimestamp
      });
    } catch (err) {
      console.error('Error sending disposition to Zapier:', err.message);
    }
  }

  if (SLACK_WEBHOOK_URL && safeOutcome === 'booked') {
    try {
      const text = [
        `🎯 *BOOKED DEMO*`,
        `• Agent: *${dialInfo?.name || agentId}* (ID: ${agentId})`,
        `• Lead: *${leadName || 'Unknown'}*`,
        `• Phone: \`${leadPhone || 'N/A'}\``,
        campaignId ? `• Campaign: \`${campaignId}\`` : null,
        notes ? `• Notes: ${notes}` : null,
        '',
        `_Rehash Dialer • RevCoreHQ_`
      ]
        .filter(Boolean)
        .join('\n');

      await axios.post(SLACK_WEBHOOK_URL, { text });
    } catch (err) {
      console.error('Error sending Slack notification:', err.message);
    }
  }

  res.json({ success: true });
});

// health check
app.get('/health', (req, res) => {
  res.send('Rehash Dialer backend is running.');
});

// =========================
//   SIMPLE ADMIN METRICS PAGE
// =========================

app.get('/metrics-admin', (req, res) => {
  let rows = '';

  Object.keys(users || {}).forEach(agentId => {
    const stats = ensureAgentMetrics(agentId);

    rows += `
      <tr>
        <td>${agentId}</td>
        <td>${agentId}</td>
        <td>${stats.totalCalls || 0}</td>
        <td>${stats.answeredHuman || 0}</td>
        <td>${stats.conversions || 0}</td>
        <td>${stats.lastOutcome || '-'}</td>
        <td>${stats.lastLeadName || '-'}</td>
        <td>${stats.lastTimestamp || '-'}</td>
      </tr>
    `;
  });

  const html = `
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <title>Rehash Dialer Admin</title>
        <style>
          body {
            margin: 0;
            padding: 24px;
            font-family: -apple-system, system-ui, BlinkMacSystemFont, "SF Pro Text", sans-serif;
            background: radial-gradient(circle at top, #020617, #020617 40%, #020617 100%);
            color: #e5f1ff;
          }
          h1 {
            font-size: 22px;
            margin: 8px 0 4px;
          }
          h2 {
            font-size: 13px;
            margin: 0 0 20px;
            opacity: 0.7;
          }
          .pill {
            display: inline-flex;
            align-items: center;
            padding: 2px 9px;
            border-radius: 999px;
            font-size: 11px;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            border: 1px solid rgba(74, 222, 128, 0.5);
            color: #4ade80;
          }
          table {
            border-collapse: collapse;
            width: 100%;
            max-width: 960px;
            background: rgba(15,23,42,0.98);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 22px 60px rgba(0,0,0,0.7);
          }
          th, td {
            padding: 10px 14px;
            font-size: 13px;
            border-bottom: 1px solid rgba(148,163,184,0.26);
          }
          th {
            text-align: left;
            background: rgba(15,23,42,1);
            font-weight: 500;
          }
          tr:last-child td {
            border-bottom: none;
          }
        </style>
      </head>
      <body>
        <div style="margin-bottom: 18px;">
          <div class="pill">Rehash Dialer · Admin</div>
          <h1>Agent Overview</h1>
          <h2>Live metrics for all outbound agents using the dialer.</h2>
        </div>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Agent</th>
              <th>Total Calls</th>
              <th>Live Connects</th>
              <th>Conversions</th>
              <th>Last Outcome</th>
              <th>Last Lead</th>
              <th>Last Timestamp</th>
            </tr>
          </thead>
          <tbody>
            ${rows}
          </tbody>
        </table>
      </body>
    </html>
  `;

  res.send(html);
});

// =====================
// START SERVER
// =====================

const listenPort = process.env.PORT || 3000;

app.listen(listenPort, () => {
  console.log(`Rehash Dialer server listening on port ${listenPort}`);
});
