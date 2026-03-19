const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const FEED_REFRESH_INTERVAL_MS = 15 * 60 * 1000;
const DEFAULT_TOPIC = 'all';

const STARTER_FEEDS = [
  { id: 'reuters-politics', name: 'Reuters Politics', url: 'https://feeds.reuters.com/Reuters/PoliticsNews', topic: 'politics' },
  { id: 'reuters-world', name: 'Reuters World', url: 'https://feeds.reuters.com/Reuters/worldNews', topic: 'world' },
  { id: 'reuters-business', name: 'Reuters Business', url: 'https://feeds.reuters.com/reuters/businessNews', topic: 'econ' },
  { id: 'reuters-tech', name: 'Reuters Technology', url: 'https://feeds.reuters.com/reuters/technologyNews', topic: 'tech' },
  { id: 'ap-politics', name: 'AP Politics', url: 'https://apnews.com/politics?output=rss', topic: 'politics' },
  { id: 'ft-world', name: 'Financial Times World', url: 'https://www.ft.com/world?format=rss', topic: 'world' },
  { id: 'ft-companies', name: 'Financial Times Companies', url: 'https://www.ft.com/companies?format=rss', topic: 'econ' },
  { id: 'verge', name: 'The Verge', url: 'https://www.theverge.com/rss/index.xml', topic: 'tech' },
  { id: 'ars', name: 'Ars Technica', url: 'https://feeds.arstechnica.com/arstechnica/index', topic: 'tech' },
  { id: 'ap-top', name: 'AP Top News', url: 'https://apnews.com/hub/ap-top-news/rss.xml', topic: 'general' },
];

function nowTs() {
  return Date.now();
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function makeId(prefix = 'evt') {
  return `${prefix}_${nowTs()}_${Math.random().toString(36).slice(2, 10)}`;
}

function hashText(value = '') {
  return crypto.createHash('sha1').update(String(value || '')).digest('hex');
}

function normalizeTopic(value = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return DEFAULT_TOPIC;
  const allowed = new Set([DEFAULT_TOPIC, 'general', 'tech', 'econ', 'world', 'politics']);
  return allowed.has(raw) ? raw : DEFAULT_TOPIC;
}

function escapeRegex(value = '') {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function decodeXmlEntities(value = '') {
  return String(value || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, '\'')
    .replace(/&#39;/g, '\'')
    .replace(/&#x27;/gi, '\'')
    .replace(/&#x2F;/gi, '/')
    .replace(/&#(\d+);/g, (_match, code) => {
      const num = Number(code || 0);
      return Number.isFinite(num) ? String.fromCharCode(num) : '';
    })
    .replace(/&#x([0-9a-f]+);/gi, (_match, code) => {
      const num = parseInt(code || '0', 16);
      return Number.isFinite(num) ? String.fromCharCode(num) : '';
    });
}

function stripXmlTags(value = '') {
  return decodeXmlEntities(String(value || '').replace(/<[^>]+>/g, ' '))
    .replace(/\s+/g, ' ')
    .trim();
}

function extractTagText(block = '', tagName = '') {
  const pattern = new RegExp(`<${escapeRegex(tagName)}\\b[^>]*>([\\s\\S]*?)<\\/${escapeRegex(tagName)}>`, 'i');
  const match = String(block || '').match(pattern);
  return match ? String(match[1] || '') : '';
}

function extractLink(block = '') {
  const raw = String(block || '');
  const atomHref = raw.match(/<link\b[^>]*href=(?:"([^"]+)"|'([^']+)')[^>]*\/?>/i);
  if (atomHref) return decodeXmlEntities(String(atomHref[1] || atomHref[2] || '').trim());
  const simple = extractTagText(raw, 'link');
  return decodeXmlEntities(String(simple || '').trim());
}

function normalizeUrl(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    return new URL(raw).toString();
  } catch (_) {
    return raw;
  }
}

function getDomain(value = '') {
  try {
    return new URL(String(value || '')).hostname.replace(/^www\./i, '').toLowerCase();
  } catch (_) {
    return '';
  }
}

function parseFeedDate(value = '') {
  const parsed = Date.parse(String(value || '').trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function parseFeedXml(xmlText = '', source = {}) {
  const xml = String(xmlText || '');
  const itemMatches = [...xml.matchAll(/<item\b[\s\S]*?<\/item>/gi)].map((match) => String(match[0] || ''));
  const entryMatches = [...xml.matchAll(/<entry\b[\s\S]*?<\/entry>/gi)].map((match) => String(match[0] || ''));
  const blocks = itemMatches.length > 0 ? itemMatches : entryMatches;
  const fetchedAt = nowTs();
  return blocks.map((block) => {
    const title = stripXmlTags(extractTagText(block, 'title'));
    const url = normalizeUrl(extractLink(block));
    const publishedAt = parseFeedDate(
      extractTagText(block, 'pubDate')
      || extractTagText(block, 'updated')
      || extractTagText(block, 'published')
      || extractTagText(block, 'dc:date')
    );
    const summary = stripXmlTags(
      extractTagText(block, 'description')
      || extractTagText(block, 'summary')
      || extractTagText(block, 'content')
      || extractTagText(block, 'content:encoded')
    );
    if (!title && !url) return null;
    return {
      id: hashText(`${source.id || 'feed'}|${url}|${title}|${publishedAt}`),
      title: title || url || 'Untitled',
      url,
      summary,
      source_id: String(source.id || '').trim(),
      source_name: String(source.name || '').trim(),
      source_domain: getDomain(url || source.url || ''),
      topic: normalizeTopic(source.topic),
      published_at: publishedAt || fetchedAt,
      fetched_at: fetchedAt,
    };
  }).filter(Boolean);
}

async function fetchTextWithTimeout(url, timeoutMs = 12000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        'user-agent': 'Subgrapher/1.0 (+https://subgrapher.local)',
        accept: 'application/rss+xml, application/atom+xml, application/xml, text/xml, text/plain;q=0.9, */*;q=0.1',
      },
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    return await res.text();
  } finally {
    clearTimeout(timer);
  }
}

function normalizeEvent(input = {}, existing = null) {
  const base = (existing && typeof existing === 'object') ? existing : {};
  const src = (input && typeof input === 'object') ? input : {};
  const title = String(Object.prototype.hasOwnProperty.call(src, 'title') ? src.title : (base.title || '')).trim().slice(0, 160);
  const date = String(Object.prototype.hasOwnProperty.call(src, 'date') ? src.date : (base.date || '')).trim();
  const time = String(Object.prototype.hasOwnProperty.call(src, 'time') ? src.time : (base.time || '')).trim();
  if (!title) return { ok: false, message: 'title is required.' };
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return { ok: false, message: 'date must be YYYY-MM-DD.' };
  if (time && !/^\d{2}:\d{2}$/.test(time)) return { ok: false, message: 'time must be HH:MM.' };
  return {
    ok: true,
    event: {
      id: String(base.id || src.id || makeId('evt')).trim(),
      title,
      date,
      time,
      created_at: Number(base.created_at || nowTs()),
      updated_at: nowTs(),
    },
  };
}

function toEventSortTs(event = {}) {
  const raw = `${String(event.date || '').trim()}T${String(event.time || '23:59').trim() || '23:59'}:00`;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

function createDashboardStore(options = {}) {
  const userDataPath = String(options.userDataPath || '').trim();
  const filePath = path.join(userDataPath || process.cwd(), 'dashboard_state.json');

  function readState() {
    try {
      if (!fs.existsSync(filePath)) {
        return {
          version: 1,
          events: [],
          filters: { selected_topic: DEFAULT_TOPIC },
          rss: { items: [], last_refreshed_at: 0 },
        };
      }
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return {
        version: 1,
        events: Array.isArray(parsed && parsed.events) ? parsed.events : [],
        filters: {
          selected_topic: normalizeTopic(parsed && parsed.filters ? parsed.filters.selected_topic : DEFAULT_TOPIC),
        },
        rss: {
          items: Array.isArray(parsed && parsed.rss && parsed.rss.items) ? parsed.rss.items : [],
          last_refreshed_at: Number((parsed && parsed.rss && parsed.rss.last_refreshed_at) || 0),
        },
      };
    } catch (_) {
      return {
        version: 1,
        events: [],
        filters: { selected_topic: DEFAULT_TOPIC },
        rss: { items: [], last_refreshed_at: 0 },
      };
    }
  }

  function writeState(state) {
    const next = {
      version: 1,
      events: Array.isArray(state && state.events) ? state.events : [],
      filters: {
        selected_topic: normalizeTopic(state && state.filters ? state.filters.selected_topic : DEFAULT_TOPIC),
      },
      rss: {
        items: Array.isArray(state && state.rss && state.rss.items) ? state.rss.items : [],
        last_refreshed_at: Number((state && state.rss && state.rss.last_refreshed_at) || 0),
      },
    };
    fs.writeFileSync(filePath, JSON.stringify(next, null, 2), 'utf8');
  }

  function getTopics() {
    const available = new Set(STARTER_FEEDS.map((feed) => normalizeTopic(feed.topic)).filter((topic) => topic !== DEFAULT_TOPIC));
    return [DEFAULT_TOPIC, 'politics', 'world', 'econ', 'tech', 'general'].filter((topic) => topic === DEFAULT_TOPIC || available.has(topic));
  }

  function getState() {
    const state = readState();
    const events = state.events
      .map((item) => normalizeEvent(item, item))
      .filter((item) => item && item.ok && item.event)
      .map((item) => item.event)
      .sort((a, b) => toEventSortTs(a) - toEventSortTs(b));
    return {
      ok: true,
      state: {
        events,
        filters: { selected_topic: normalizeTopic(state.filters.selected_topic) },
        rss: {
          last_refreshed_at: Number((state.rss && state.rss.last_refreshed_at) || 0),
          sources: clone(STARTER_FEEDS).map((feed) => ({
            ...feed,
            domain: getDomain(feed.url),
          })),
          topics: getTopics(),
        },
      },
    };
  }

  function shouldRefreshFeeds() {
    const state = readState();
    const last = Number((state.rss && state.rss.last_refreshed_at) || 0);
    const items = Array.isArray(state.rss && state.rss.items) ? state.rss.items : [];
    return !last || !items.length || (nowTs() - last) >= FEED_REFRESH_INTERVAL_MS;
  }

  function listFeedItems(options = {}) {
    const topic = normalizeTopic(options.topic || DEFAULT_TOPIC);
    const limit = Math.max(1, Math.min(200, Number(options.limit || 80)));
    const state = readState();
    const items = (Array.isArray(state.rss && state.rss.items) ? state.rss.items : [])
      .filter((item) => topic === DEFAULT_TOPIC || normalizeTopic(item && item.topic) === topic)
      .sort((a, b) => Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0))
      .slice(0, limit);
    return {
      ok: true,
      topic,
      items: clone(items),
      last_refreshed_at: Number((state.rss && state.rss.last_refreshed_at) || 0),
      topics: getTopics(),
    };
  }

  async function refreshFeeds(options = {}) {
    const force = !!options.force;
    if (!force && !shouldRefreshFeeds()) {
      return listFeedItems({ topic: options.topic, limit: options.limit });
    }
    const state = readState();
    const itemMap = new Map();
    const errors = [];
    const results = await Promise.allSettled(STARTER_FEEDS.map(async (source) => {
      const xml = await fetchTextWithTimeout(source.url);
      return parseFeedXml(xml, source).slice(0, 24);
    }));
    results.forEach((result, index) => {
      const source = STARTER_FEEDS[index];
      if (result.status !== 'fulfilled') {
        errors.push(`${source.name}: ${String((result.reason && result.reason.message) || result.reason || 'Unable to fetch.')}`);
        return;
      }
      result.value.forEach((item) => {
        const key = String((item && item.url) || '').trim().toLowerCase() || String((item && item.id) || '').trim();
        if (!key) return;
        const existing = itemMap.get(key);
        if (!existing || Number((item && item.published_at) || 0) > Number((existing && existing.published_at) || 0)) {
          itemMap.set(key, item);
        }
      });
    });
    const nextItems = Array.from(itemMap.values())
      .sort((a, b) => Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0))
      .slice(0, 240);
    state.rss = {
      items: nextItems.length
        ? nextItems
        : (Array.isArray(state.rss && state.rss.items) ? state.rss.items : []),
      last_refreshed_at: nowTs(),
    };
    writeState(state);
    const listing = listFeedItems({ topic: options.topic, limit: options.limit });
    return {
      ...listing,
      message: errors.length ? errors.join(' | ') : '',
    };
  }

  function setSelectedTopic(topic = DEFAULT_TOPIC) {
    const state = readState();
    state.filters = { selected_topic: normalizeTopic(topic) };
    writeState(state);
    return {
      ok: true,
      selected_topic: state.filters.selected_topic,
    };
  }

  function saveEvent(payload = {}) {
    const state = readState();
    state.events = Array.isArray(state.events) ? state.events : [];
    const eventId = String((payload && payload.id) || '').trim();
    const existingIndex = eventId
      ? state.events.findIndex((item) => String((item && item.id) || '') === eventId)
      : -1;
    const normalized = normalizeEvent(payload, existingIndex >= 0 ? state.events[existingIndex] : null);
    if (!normalized.ok) return normalized;
    if (existingIndex >= 0) state.events[existingIndex] = normalized.event;
    else state.events.push(normalized.event);
    state.events.sort((a, b) => toEventSortTs(a) - toEventSortTs(b));
    writeState(state);
    return { ok: true, event: clone(normalized.event), events: clone(state.events) };
  }

  function deleteEvent(eventId = '') {
    const id = String(eventId || '').trim();
    if (!id) return { ok: false, message: 'event_id is required.' };
    const state = readState();
    const next = (Array.isArray(state.events) ? state.events : []).filter((item) => String((item && item.id) || '') !== id);
    state.events = next;
    writeState(state);
    return { ok: true, events: clone(next) };
  }

  return {
    getState,
    shouldRefreshFeeds,
    listFeedItems,
    refreshFeeds,
    setSelectedTopic,
    saveEvent,
    deleteEvent,
  };
}

module.exports = {
  createDashboardStore,
};
