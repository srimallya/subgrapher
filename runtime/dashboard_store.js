const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const { embedTexts } = require('./embedding_runtime');
const { fetchWebPagePreview } = require('./lumino_crawler');

const FEED_REFRESH_INTERVAL_MS = 15 * 60 * 1000;
const FEED_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const DEFAULT_TOPIC = 'all';
const OTHER_TOPIC = 'other';
const TOPIC_KEYS = ['politics', 'world', 'econ', 'tech', OTHER_TOPIC];
const ENRICH_CONCURRENCY = 6;
const MAX_PER_SOURCE = 24;
const MAX_REFRESH_ITEMS = 240;
const MAX_CONTENT_CHARS = 12_000;
const MAX_EMBED_INPUT_CHARS = 2_500;
const KEYWORD_WEIGHT = 3;
const SEMANTIC_MIN_SCORE = 0.18;

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
  { id: 'ap-top', name: 'AP Top News', url: 'https://apnews.com/hub/ap-top-news/rss.xml', topic: OTHER_TOPIC },
];

const TOPIC_PROTOTYPE_TEXT = {
  politics: 'government election congress parliament senate law policy diplomacy campaign administration white house ministry political party leadership constitution sanctions public office legislation voting',
  world: 'international global conflict war border united nations diplomacy foreign affairs migration humanitarian earthquake crisis country region global summit international relations worldwide',
  econ: 'economy markets finance business companies banking inflation stocks bonds trade tariffs startups earnings jobs industry commerce central bank recession investment',
  tech: 'technology software hardware ai cybersecurity internet mobile gadgets chips semiconductors cloud startups research digital platform apps computing devices engineering open source',
  other: 'general breaking news culture science health sports weather entertainment daily life society education environment community features analysis',
};

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
  if (raw === 'general') return OTHER_TOPIC;
  const allowed = new Set([DEFAULT_TOPIC, ...TOPIC_KEYS]);
  return allowed.has(raw) ? raw : DEFAULT_TOPIC;
}

function normalizeClassifiedTopic(value = '') {
  const topic = normalizeTopic(value);
  return topic === DEFAULT_TOPIC ? OTHER_TOPIC : topic;
}

function normalizeRepeat(value = '') {
  const raw = String(value || '').trim().toLowerCase();
  const allowed = new Set(['off', 'weekly', 'monthly', 'yearly']);
  return allowed.has(raw) ? raw : 'off';
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

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
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
    const parsed = new URL(raw);
    parsed.hash = '';
    return parsed.toString();
  } catch (_) {
    return raw;
  }
}

function canonicalizeUrl(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    parsed.hash = '';
    parsed.hostname = parsed.hostname.toLowerCase();
    return parsed.toString();
  } catch (_) {
    return raw.toLowerCase();
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

function buildContentExcerpt(value = '', maxChars = 220) {
  const text = normalizeWhitespace(value);
  if (!text) return '';
  return text.length > maxChars ? `${text.slice(0, maxChars - 1).trim()}…` : text;
}

function toNumberArray(input) {
  if (!Array.isArray(input) && !ArrayBuffer.isView(input)) return [];
  const out = [];
  for (let i = 0; i < input.length; i += 1) {
    const value = Number(input[i]);
    if (!Number.isFinite(value)) return [];
    out.push(value);
  }
  return out;
}

function cosineSimilarity(vecA = [], vecB = []) {
  if (!Array.isArray(vecA) || !Array.isArray(vecB) || !vecA.length || !vecB.length) return 0;
  const len = Math.min(vecA.length, vecB.length);
  let dot = 0;
  let magA = 0;
  let magB = 0;
  for (let i = 0; i < len; i += 1) {
    const a = Number(vecA[i] || 0);
    const b = Number(vecB[i] || 0);
    dot += a * b;
    magA += a * a;
    magB += b * b;
  }
  if (!magA || !magB) return 0;
  return dot / (Math.sqrt(magA) * Math.sqrt(magB));
}

function computeKeywordScore(item = {}, query = '') {
  const terms = normalizeWhitespace(query).toLowerCase().split(' ').filter(Boolean);
  if (!terms.length) return 0;
  const title = String(item.display_title || '').toLowerCase();
  const source = `${String(item.source_name || '').toLowerCase()} ${String(item.source_domain || '').toLowerCase()}`;
  const url = String(item.url || '').toLowerCase();
  const excerpt = String(item.content_excerpt || '').toLowerCase();
  const content = String(item.content_text || '').toLowerCase();
  let score = 0;
  terms.forEach((term) => {
    if (!term) return;
    if (title.includes(term)) score += 4;
    if (source.includes(term)) score += 2;
    if (url.includes(term)) score += 1;
    if (excerpt.includes(term)) score += 3;
    if (content.includes(term)) score += 2;
  });
  return score;
}

function buildEmbeddingText(item = {}) {
  const text = [
    String(item.display_title || item.crawler_title || item.title || ''),
    String(item.clean_summary || item.raw_content_text || item.content_text || item.content_excerpt || item.summary || ''),
    String(item.source_name || ''),
  ].filter(Boolean).join('\n');
  return text.slice(0, MAX_EMBED_INPUT_CHARS);
}

function buildSearchText(item = {}) {
  return normalizeWhitespace([
    String(item.display_title || item.crawler_title || item.title || ''),
    String(item.clean_excerpt || item.content_excerpt || ''),
    String(item.clean_summary || ''),
    String(item.raw_content_text || item.content_text || ''),
    String(item.summary || ''),
    String(item.source_name || ''),
    String(item.source_domain || ''),
    String(item.url || ''),
  ].filter(Boolean).join(' '));
}

function getItemLookupKeys(item = {}) {
  const keys = [];
  const canonical = canonicalizeUrl(item.url || '');
  const id = String(item.id || '').trim();
  if (canonical) keys.push(`url:${canonical}`);
  if (id) keys.push(`id:${id}`);
  return keys;
}

function pruneFeedItems(items = []) {
  const cutoff = nowTs() - FEED_RETENTION_MS;
  return (Array.isArray(items) ? items : [])
    .map((item) => normalizeStoredFeedItem(item))
    .filter((item) => item && (Number(item.published_at || 0) >= cutoff || Number(item.fetched_at || 0) >= cutoff));
}

function normalizeStoredFeedItem(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const canonicalUrl = normalizeUrl(src.url || '');
  const displayTitle = String(
    src.display_title
    || src.crawler_title
    || src.title
    || canonicalUrl
    || 'Untitled'
  ).trim() || 'Untitled';
  const rawContentText = String(src.raw_content_text || src.content_text || src.summary || '').replace(/\u0000/g, '').trim().slice(0, MAX_CONTENT_CHARS);
  const cleanSummary = String(src.clean_summary || src.summary || '').trim().slice(0, MAX_CONTENT_CHARS);
  const cleanExcerpt = String(src.clean_excerpt || buildContentExcerpt(cleanSummary || rawContentText || src.summary || '')).trim();
  const contentMarkdown = String(src.content_markdown || '').trim().slice(0, MAX_CONTENT_CHARS);
  return {
    id: String(src.id || hashText(`${canonicalUrl}|${displayTitle}|${src.published_at || 0}`)).trim(),
    url: canonicalUrl,
    title: String(src.title || displayTitle).trim() || displayTitle,
    crawler_title: String(src.crawler_title || '').trim(),
    display_title: displayTitle,
    summary: String(src.summary || '').trim(),
    raw_content_text: rawContentText,
    content_text: rawContentText,
    clean_summary: cleanSummary,
    clean_excerpt: cleanExcerpt,
    content_excerpt: String(src.content_excerpt || cleanExcerpt || buildContentExcerpt(rawContentText || src.summary || '')).trim(),
    content_markdown: contentMarkdown,
    source_id: String(src.source_id || '').trim(),
    source_name: String(src.source_name || '').trim(),
    source_domain: String(src.source_domain || getDomain(canonicalUrl)).trim(),
    topic: normalizeClassifiedTopic(src.topic || src.source_topic || OTHER_TOPIC),
    topic_source: String(src.topic_source || 'legacy').trim() || 'legacy',
    source_topic: normalizeClassifiedTopic(src.source_topic || src.topic || OTHER_TOPIC),
    published_at: Number(src.published_at || 0) || 0,
    fetched_at: Number(src.fetched_at || 0) || 0,
    content_fetched_at: Number(src.content_fetched_at || 0) || 0,
    summary_generated_at: Number(src.summary_generated_at || 0) || 0,
    summary_model_id: String(src.summary_model_id || '').trim(),
    summary_status: String(src.summary_status || (cleanSummary ? 'generated' : 'pending')).trim() || 'pending',
    content_quality: String(src.content_quality || '').trim(),
    entities: Array.isArray(src.entities) ? src.entities.slice(0, 12).map((item) => String(item || '').trim()).filter(Boolean) : [],
    summary_content_hash: String(src.summary_content_hash || '').trim(),
    search_text: String(buildSearchText({
      ...src,
      display_title: displayTitle,
      raw_content_text: rawContentText,
      clean_summary: cleanSummary,
      clean_excerpt: cleanExcerpt,
    })).trim(),
    embedding: toNumberArray(src.embedding),
    fetch_status: String(src.fetch_status || (rawContentText ? 'cached' : 'rss_only')).trim() || 'rss_only',
    has_full_content: !!(src.has_full_content || (Number(src.content_fetched_at || 0) > 0 && rawContentText)),
  };
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
      source_topic: normalizeClassifiedTopic(source.topic),
      topic: normalizeClassifiedTopic(source.topic),
      topic_source: 'source',
      published_at: publishedAt || fetchedAt,
      fetched_at: fetchedAt,
      content_fetched_at: 0,
      fetch_status: 'rss_only',
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

async function mapWithConcurrency(items, concurrency, task) {
  const list = Array.isArray(items) ? items : [];
  const limit = Math.max(1, Number(concurrency) || 1);
  const out = new Array(list.length);
  let nextIndex = 0;
  async function worker() {
    while (nextIndex < list.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      out[currentIndex] = await task(list[currentIndex], currentIndex);
    }
  }
  const workers = [];
  for (let i = 0; i < Math.min(limit, list.length); i += 1) {
    workers.push(worker());
  }
  await Promise.all(workers);
  return out;
}

function normalizeEvent(input = {}, existing = null) {
  const base = (existing && typeof existing === 'object') ? existing : {};
  const src = (input && typeof input === 'object') ? input : {};
  const title = String(Object.prototype.hasOwnProperty.call(src, 'title') ? src.title : (base.title || '')).trim().slice(0, 160);
  const legacyDate = String(Object.prototype.hasOwnProperty.call(src, 'date') ? src.date : (base.date || '')).trim();
  const startDate = String(Object.prototype.hasOwnProperty.call(src, 'start_date') ? src.start_date : (base.start_date || legacyDate)).trim();
  const endDate = String(Object.prototype.hasOwnProperty.call(src, 'end_date') ? src.end_date : (base.end_date || startDate || legacyDate)).trim();
  const repeat = normalizeRepeat(Object.prototype.hasOwnProperty.call(src, 'repeat') ? src.repeat : (base.repeat || 'off'));
  const note = String(Object.prototype.hasOwnProperty.call(src, 'note') ? src.note : (base.note || '')).trim().slice(0, 2000);
  const time = String(Object.prototype.hasOwnProperty.call(src, 'time') ? src.time : (base.time || '')).trim();
  if (!title) return { ok: false, message: 'title is required.' };
  if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate)) return { ok: false, message: 'start_date must be YYYY-MM-DD.' };
  if (!/^\d{4}-\d{2}-\d{2}$/.test(endDate)) return { ok: false, message: 'end_date must be YYYY-MM-DD.' };
  if (endDate < startDate) return { ok: false, message: 'end_date must be on or after start_date.' };
  if (time && !/^\d{2}:\d{2}$/.test(time)) return { ok: false, message: 'time must be HH:MM.' };
  return {
    ok: true,
    event: {
      id: String(base.id || src.id || makeId('evt')).trim(),
      title,
      start_date: startDate,
      end_date: endDate,
      repeat,
      note,
      time,
      created_at: Number(base.created_at || nowTs()),
      updated_at: nowTs(),
    },
  };
}

function toEventSortTs(event = {}) {
  const raw = `${String(event.start_date || event.date || '').trim()}T${String(event.time || '23:59').trim() || '23:59'}:00`;
  const parsed = Date.parse(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeTask(input = {}, existing = null) {
  const base = (existing && typeof existing === 'object') ? existing : {};
  const src = (input && typeof input === 'object') ? input : {};
  const title = String(Object.prototype.hasOwnProperty.call(src, 'title') ? src.title : (base.title || '')).trim().slice(0, 140);
  if (!title) return { ok: false, message: 'title is required.' };
  return {
    ok: true,
    task: {
      id: String(base.id || src.id || makeId('tsk')).trim(),
      title,
      created_at: Number(base.created_at || nowTs()),
      updated_at: nowTs(),
    },
  };
}

function createDashboardStore(options = {}) {
  const userDataPath = String(options.userDataPath || '').trim();
  const filePath = path.join(userDataPath || process.cwd(), 'dashboard_state.json');
  const getEmbeddingConfig = typeof options.getEmbeddingConfig === 'function' ? options.getEmbeddingConfig : (() => ({}));
  const summarizeArticle = typeof options.summarizeArticle === 'function' ? options.summarizeArticle : null;

  function readState() {
    try {
      if (!fs.existsSync(filePath)) {
        return {
          version: 3,
          events: [],
          tasks: [],
          filters: { selected_topic: DEFAULT_TOPIC },
          rss: { items: [], last_refreshed_at: 0 },
        };
      }
      const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      return {
        version: 3,
        events: Array.isArray(parsed && parsed.events) ? parsed.events : [],
        tasks: Array.isArray(parsed && parsed.tasks) ? parsed.tasks : [],
        filters: {
          selected_topic: normalizeTopic(parsed && parsed.filters ? parsed.filters.selected_topic : DEFAULT_TOPIC),
        },
        rss: {
          items: pruneFeedItems(parsed && parsed.rss && parsed.rss.items),
          last_refreshed_at: Number((parsed && parsed.rss && parsed.rss.last_refreshed_at) || 0),
        },
      };
    } catch (_) {
      return {
        version: 3,
        events: [],
        tasks: [],
        filters: { selected_topic: DEFAULT_TOPIC },
        rss: { items: [], last_refreshed_at: 0 },
      };
    }
  }

  function writeState(state) {
    const next = {
      version: 3,
      events: Array.isArray(state && state.events) ? state.events : [],
      tasks: Array.isArray(state && state.tasks) ? state.tasks : [],
      filters: {
        selected_topic: normalizeTopic(state && state.filters ? state.filters.selected_topic : DEFAULT_TOPIC),
      },
      rss: {
        items: pruneFeedItems(state && state.rss && state.rss.items),
        last_refreshed_at: Number((state && state.rss && state.rss.last_refreshed_at) || 0),
      },
    };
    fs.writeFileSync(filePath, JSON.stringify(next, null, 2), 'utf8');
  }

  function getTopics() {
    return [DEFAULT_TOPIC, ...TOPIC_KEYS];
  }

  function getState() {
    const state = readState();
    const events = state.events
      .map((item) => normalizeEvent(item, item))
      .filter((item) => item && item.ok && item.event)
      .map((item) => item.event)
      .sort((a, b) => toEventSortTs(a) - toEventSortTs(b));
    const tasks = (Array.isArray(state.tasks) ? state.tasks : [])
      .map((item) => normalizeTask(item, item))
      .filter((item) => item && item.ok && item.task)
      .map((item) => item.task)
      .sort((a, b) => Number((a && a.created_at) || 0) - Number((b && b.created_at) || 0));
    return {
      ok: true,
      state: {
        events,
        tasks,
        filters: { selected_topic: normalizeTopic(state.filters.selected_topic) },
        rss: {
          last_refreshed_at: Number((state.rss && state.rss.last_refreshed_at) || 0),
          sources: clone(STARTER_FEEDS).map((feed) => ({
            ...feed,
            topic: normalizeClassifiedTopic(feed.topic),
            domain: getDomain(feed.url),
          })),
          topics: getTopics(),
        },
      },
    };
  }

  function getSummaryBacklogStatus() {
    const state = readState();
    const items = pruneFeedItems(state.rss && state.rss.items);
    const totalCandidates = items.filter((item) => String((item && (item.raw_content_text || item.content_text)) || '').trim()).length;
    const pendingItems = items.filter((item) => {
      const rawText = String((item && (item.raw_content_text || item.content_text)) || '').trim();
      if (!rawText) return false;
      const expectedHash = hashText(rawText);
      return !String((item && item.clean_summary) || '').trim()
        || String((item && item.summary_content_hash) || '').trim() !== expectedHash;
    }).length;
    return {
      ok: true,
      total_candidates: totalCandidates,
      pending_items: pendingItems,
      completed_items: Math.max(0, totalCandidates - pendingItems),
    };
  }

  function shouldRefreshFeeds() {
    const state = readState();
    const last = Number((state.rss && state.rss.last_refreshed_at) || 0);
    const items = Array.isArray(state.rss && state.rss.items) ? state.rss.items : [];
    return !last || !items.length || (nowTs() - last) >= FEED_REFRESH_INTERVAL_MS;
  }

  function getLookupMap(items = []) {
    const map = new Map();
    (Array.isArray(items) ? items : []).forEach((item) => {
      getItemLookupKeys(item).forEach((key) => {
        map.set(key, item);
      });
    });
    return map;
  }

  async function classifyItems(items = []) {
    const list = (Array.isArray(items) ? items : []).map((item) => normalizeStoredFeedItem(item));
    if (!list.length) return list;
    const articleTexts = list.map((item) => buildEmbeddingText(item));
    const prototypeTexts = TOPIC_KEYS.map((topic) => TOPIC_PROTOTYPE_TEXT[topic]);
    const config = getEmbeddingConfig() || {};
    const embeddingRes = await embedTexts(prototypeTexts.concat(articleTexts), config);
    const vectors = Array.isArray(embeddingRes && embeddingRes.embeddings) ? embeddingRes.embeddings : [];
    const prototypeVectors = TOPIC_KEYS.map((topic, idx) => ({
      topic,
      vector: toNumberArray(vectors[idx]),
    }));
    return list.map((item, idx) => {
      const vector = toNumberArray(vectors[prototypeTexts.length + idx]);
      let bestTopic = normalizeClassifiedTopic(item.source_topic || item.topic || OTHER_TOPIC);
      let bestScore = -1;
      prototypeVectors.forEach((entry) => {
        const score = cosineSimilarity(vector, entry.vector);
        if (score > bestScore) {
          bestScore = score;
          bestTopic = entry.topic;
        }
      });
      const classifiedTopic = bestScore >= SEMANTIC_MIN_SCORE ? bestTopic : OTHER_TOPIC;
      return normalizeStoredFeedItem({
        ...item,
        embedding: vector,
        topic: classifiedTopic,
        topic_source: vector.length ? 'embedding' : 'source_fallback',
      });
    });
  }

  async function buildCleanSummary(item = {}, existing = null) {
    const normalized = normalizeStoredFeedItem({ ...existing, ...item });
    const rawContentText = String(normalized.raw_content_text || normalized.content_text || '').trim();
    const contentHash = hashText(rawContentText);
    if (!rawContentText) {
      return {
        clean_summary: String(normalized.clean_summary || normalized.summary || '').trim(),
        clean_excerpt: String(normalized.clean_excerpt || normalized.content_excerpt || '').trim(),
        summary_generated_at: Number(normalized.summary_generated_at || 0) || 0,
        summary_model_id: String(normalized.summary_model_id || '').trim(),
        summary_status: String(normalized.summary_status || 'empty').trim() || 'empty',
        content_quality: String(normalized.content_quality || '').trim(),
        entities: Array.isArray(normalized.entities) ? normalized.entities.slice(0, 12) : [],
        summary_content_hash: String(normalized.summary_content_hash || '').trim(),
      };
    }
    if (existing && String(existing.summary_content_hash || '').trim() === contentHash && String(existing.clean_summary || '').trim()) {
      return {
        clean_summary: String(existing.clean_summary || '').trim(),
        clean_excerpt: String(existing.clean_excerpt || buildContentExcerpt(existing.clean_summary || '')).trim(),
        summary_generated_at: Number(existing.summary_generated_at || 0) || 0,
        summary_model_id: String(existing.summary_model_id || '').trim(),
        summary_status: String(existing.summary_status || 'generated').trim() || 'generated',
        content_quality: String(existing.content_quality || '').trim(),
        entities: Array.isArray(existing.entities) ? existing.entities.slice(0, 12) : [],
        summary_content_hash: contentHash,
      };
    }
    if (!summarizeArticle) {
      return {
        clean_summary: String(normalized.summary || buildContentExcerpt(rawContentText, 1600)).trim(),
        clean_excerpt: buildContentExcerpt(rawContentText || normalized.summary || '', 220),
        summary_generated_at: nowTs(),
        summary_model_id: '',
        summary_status: 'fallback',
        content_quality: String(normalized.content_quality || '').trim(),
        entities: Array.isArray(normalized.entities) ? normalized.entities.slice(0, 12) : [],
        summary_content_hash: contentHash,
      };
    }
    const res = await summarizeArticle({
      id: normalized.id,
      title: normalized.display_title || normalized.title,
      raw_content_text: rawContentText,
      source_name: normalized.source_name,
      source_domain: normalized.source_domain,
      url: normalized.url,
    }).catch(() => null);
    if (!res || res.ok === false) {
      return {
        clean_summary: String(normalized.summary || buildContentExcerpt(rawContentText, 1600)).trim(),
        clean_excerpt: buildContentExcerpt(rawContentText || normalized.summary || '', 220),
        summary_generated_at: nowTs(),
        summary_model_id: '',
        summary_status: 'fallback',
        content_quality: String(normalized.content_quality || '').trim(),
        entities: Array.isArray(normalized.entities) ? normalized.entities.slice(0, 12) : [],
        summary_content_hash: contentHash,
      };
    }
    const cleanSummary = String(res.summary || normalized.summary || buildContentExcerpt(rawContentText, 1600)).trim();
    return {
      clean_summary: cleanSummary,
      clean_excerpt: String(res.excerpt || buildContentExcerpt(cleanSummary || rawContentText, 220)).trim(),
      summary_generated_at: Number(res.generated_at || nowTs()) || nowTs(),
      summary_model_id: String(res.model_id || '').trim(),
      summary_status: String((res.analysis_source === 'fallback') ? 'fallback' : 'generated').trim(),
      content_quality: String(res.content_quality || '').trim(),
      entities: Array.isArray(res.entities) ? res.entities.slice(0, 12).map((entity) => String(entity || '').trim()).filter(Boolean) : [],
      summary_content_hash: contentHash,
    };
  }

  async function enrichFeedCandidate(candidate = {}, existing = null) {
    const base = normalizeStoredFeedItem({
      ...existing,
      ...candidate,
      display_title: candidate.title || (existing && existing.display_title) || candidate.url,
      raw_content_text: (existing && (existing.raw_content_text || existing.content_text)) || candidate.summary || '',
      clean_summary: (existing && existing.clean_summary) || candidate.summary || '',
      clean_excerpt: (existing && existing.clean_excerpt) || buildContentExcerpt(candidate.summary || ''),
      content_excerpt: (existing && existing.content_excerpt) || buildContentExcerpt(candidate.summary || ''),
      source_topic: candidate.source_topic || (existing && existing.source_topic) || candidate.topic || OTHER_TOPIC,
      topic: candidate.topic || (existing && existing.topic) || candidate.source_topic || OTHER_TOPIC,
      topic_source: (existing && existing.topic_source) || 'source',
      fetch_status: (existing && existing.fetch_status) || candidate.fetch_status || 'rss_only',
    });
    const targetUrl = String(base.url || '').trim();
    if (!targetUrl) return base;
    const preview = await fetchWebPagePreview(targetUrl, { markdownFirst: true, maxChars: MAX_CONTENT_CHARS, timeoutMs: 12_000 });
    if (!preview || !preview.ok) {
      if (existing) {
        return normalizeStoredFeedItem({
          ...existing,
          ...base,
          display_title: existing.display_title || base.display_title,
          crawler_title: existing.crawler_title || base.crawler_title,
          raw_content_text: existing.raw_content_text || existing.content_text || base.raw_content_text,
          clean_summary: existing.clean_summary || base.clean_summary,
          clean_excerpt: existing.clean_excerpt || base.clean_excerpt,
          content_text: existing.raw_content_text || existing.content_text || base.raw_content_text,
          content_excerpt: existing.content_excerpt || base.content_excerpt,
          content_markdown: existing.content_markdown || base.content_markdown,
          content_fetched_at: existing.content_fetched_at || base.content_fetched_at,
          fetch_status: existing.fetch_status || String((preview && preview.fetch_status) || 'cached').trim() || 'cached',
          has_full_content: !!(existing.has_full_content || base.has_full_content),
        });
      }
      return normalizeStoredFeedItem({
        ...base,
        fetch_status: String((preview && preview.fetch_status) || 'rss_only').trim() || 'rss_only',
      });
    }
    const fallbackTitle = String(
      (existing && (existing.display_title || existing.crawler_title || existing.title))
      || candidate.title
      || base.display_title
      || base.title
      || targetUrl
    ).trim() || targetUrl;
    const crawlerTitle = normalizeWhitespace(preview.title || '');
    const contentText = String(preview.text || '').trim().slice(0, MAX_CONTENT_CHARS);
    const contentMarkdown = String(preview.markdown || '').trim().slice(0, MAX_CONTENT_CHARS);
    return normalizeStoredFeedItem({
      ...base,
      crawler_title: crawlerTitle,
      display_title: crawlerTitle || fallbackTitle,
      raw_content_text: contentText || base.raw_content_text,
      content_text: contentText || base.raw_content_text,
      content_excerpt: buildContentExcerpt(contentText || base.raw_content_text || base.summary || ''),
      content_markdown: contentMarkdown,
      content_fetched_at: nowTs(),
      fetch_status: 'fetched',
      has_full_content: !!contentText,
    });
  }

  async function listFeedItems(options = {}) {
    const topic = normalizeTopic(options.topic || DEFAULT_TOPIC);
    const limit = Math.max(1, Math.min(200, Number(options.limit || 80)));
    const query = String(options.query || '').trim();
    const state = readState();
    const sourceItems = pruneFeedItems(state.rss && state.rss.items);
    let items = sourceItems
      .filter((item) => topic === DEFAULT_TOPIC || normalizeClassifiedTopic(item && item.topic) === topic);
    if (query) {
      const config = getEmbeddingConfig() || {};
      const queryEmbeddingRes = await embedTexts([query], config);
      const queryVector = toNumberArray(queryEmbeddingRes && queryEmbeddingRes.embeddings && queryEmbeddingRes.embeddings[0]);
      items = items
        .map((item) => {
          const keywordScore = computeKeywordScore(item, query);
          const semanticScore = cosineSimilarity(queryVector, toNumberArray(item.embedding));
          const score = (keywordScore * KEYWORD_WEIGHT) + semanticScore;
          return {
            item,
            score,
            keywordScore,
            semanticScore,
          };
        })
        .filter((entry) => entry.keywordScore > 0 || entry.semanticScore >= SEMANTIC_MIN_SCORE)
        .sort((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          return Number((b.item && b.item.published_at) || 0) - Number((a.item && a.item.published_at) || 0);
        })
        .slice(0, limit)
        .map((entry) => entry.item);
    } else {
      items = items
        .sort((a, b) => Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0))
        .slice(0, limit);
    }
    return {
      ok: true,
      topic,
      query,
      items: clone(items),
      last_refreshed_at: Number((state.rss && state.rss.last_refreshed_at) || 0),
      topics: getTopics(),
    };
  }

  async function refreshFeeds(options = {}) {
    const force = !!options.force;
    if (!force && !shouldRefreshFeeds()) {
      return listFeedItems({ topic: options.topic, limit: options.limit, query: options.query });
    }
    const state = readState();
    const existingItems = pruneFeedItems(state.rss && state.rss.items);
    const existingMap = getLookupMap(existingItems);
    const discoveredMap = new Map();
    const errors = [];
    const results = await Promise.allSettled(STARTER_FEEDS.map(async (source) => {
      const xml = await fetchTextWithTimeout(source.url);
      return parseFeedXml(xml, source).slice(0, MAX_PER_SOURCE);
    }));
    results.forEach((result, index) => {
      const source = STARTER_FEEDS[index];
      if (result.status !== 'fulfilled') {
        errors.push(`${source.name}: ${String((result.reason && result.reason.message) || result.reason || 'Unable to fetch.')}`);
        return;
      }
      result.value.forEach((item) => {
        const canonical = canonicalizeUrl(item.url || '');
        const key = canonical ? `url:${canonical}` : `id:${String(item.id || '').trim()}`;
        const existing = discoveredMap.get(key);
        if (!existing || Number(item.published_at || 0) > Number(existing.published_at || 0)) {
          discoveredMap.set(key, item);
        }
      });
    });
    const discovered = Array.from(discoveredMap.values())
      .sort((a, b) => Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0))
      .slice(0, MAX_REFRESH_ITEMS);
    const enriched = await mapWithConcurrency(discovered, ENRICH_CONCURRENCY, async (candidate) => {
      const keys = getItemLookupKeys(candidate);
      let existing = null;
      for (let i = 0; i < keys.length; i += 1) {
        if (existingMap.has(keys[i])) {
          existing = existingMap.get(keys[i]);
          break;
        }
      }
      return enrichFeedCandidate(candidate, existing);
    });
    const summarized = await mapWithConcurrency(enriched, ENRICH_CONCURRENCY, async (item) => {
      const existing = existingMap.get(`url:${canonicalizeUrl(item.url || '')}`) || null;
      const summaryPatch = await buildCleanSummary(item, existing);
      return normalizeStoredFeedItem({
        ...item,
        ...summaryPatch,
      });
    });
    const classified = await classifyItems(summarized);
    const mergedMap = new Map();
    existingItems.forEach((item) => {
      getItemLookupKeys(item).forEach((key) => {
        if (!mergedMap.has(key)) mergedMap.set(key, item);
      });
    });
    classified.forEach((item) => {
      getItemLookupKeys(item).forEach((key) => {
        mergedMap.set(key, item);
      });
    });
    const unique = new Map();
    mergedMap.forEach((item) => {
      const normalized = normalizeStoredFeedItem(item);
      const key = canonicalizeUrl(normalized.url || '') || `id:${normalized.id}`;
      const existing = unique.get(key);
      if (!existing || Number(normalized.published_at || 0) > Number(existing.published_at || 0)) {
        unique.set(key, normalized);
      }
    });
    state.rss = {
      items: pruneFeedItems(Array.from(unique.values()))
        .sort((a, b) => Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0)),
      last_refreshed_at: nowTs(),
    };
    writeState(state);
    const listing = await listFeedItems({ topic: options.topic, limit: options.limit, query: options.query });
    return {
      ...listing,
      message: errors.length ? errors.join(' | ') : '',
    };
  }

  async function rerunSummaries(options = {}) {
    const state = readState();
    const progress = typeof options.onProgress === 'function' ? options.onProgress : null;
    const items = pruneFeedItems(state.rss && state.rss.items);
    let completed = 0;
    const nextItems = await mapWithConcurrency(items, Math.max(1, Math.min(ENRICH_CONCURRENCY, 4)), async (item) => {
      const summaryPatch = await buildCleanSummary({
        ...item,
        raw_content_text: String(item.raw_content_text || item.content_text || '').trim(),
      }, options.force ? null : item);
      completed += 1;
      if (progress) {
        progress({
          total: items.length,
          completed,
          label: String((item && (item.display_title || item.title || item.url)) || 'article').trim(),
        });
      }
      return normalizeStoredFeedItem({
        ...item,
        ...summaryPatch,
      });
    });
    state.rss = {
      items: nextItems,
      last_refreshed_at: Number((state.rss && state.rss.last_refreshed_at) || 0),
    };
    writeState(state);
    return {
      ok: true,
      total: items.length,
      completed,
    };
  }

  function getFeedItem(itemId = '') {
    const id = String(itemId || '').trim();
    if (!id) return { ok: false, message: 'item_id is required.' };
    const state = readState();
    const item = pruneFeedItems(state.rss && state.rss.items)
      .find((entry) => String((entry && entry.id) || '').trim() === id);
    if (!item) return { ok: false, message: 'Feed item not found.' };
    return { ok: true, item: clone(item) };
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

  function saveTask(payload = {}) {
    const state = readState();
    state.tasks = Array.isArray(state.tasks) ? state.tasks : [];
    const taskId = String((payload && payload.id) || '').trim();
    const existingIndex = taskId
      ? state.tasks.findIndex((item) => String((item && item.id) || '') === taskId)
      : -1;
    const normalized = normalizeTask(payload, existingIndex >= 0 ? state.tasks[existingIndex] : null);
    if (!normalized.ok) return normalized;
    if (existingIndex >= 0) state.tasks[existingIndex] = normalized.task;
    else state.tasks.push(normalized.task);
    state.tasks.sort((a, b) => Number((a && a.created_at) || 0) - Number((b && b.created_at) || 0));
    writeState(state);
    return { ok: true, task: clone(normalized.task), tasks: clone(state.tasks) };
  }

  function deleteTask(taskId = '') {
    const id = String(taskId || '').trim();
    if (!id) return { ok: false, message: 'task_id is required.' };
    const state = readState();
    const next = (Array.isArray(state.tasks) ? state.tasks : []).filter((item) => String((item && item.id) || '') !== id);
    state.tasks = next;
    writeState(state);
    return { ok: true, tasks: clone(next) };
  }

  return {
    getState,
    getSummaryBacklogStatus,
    shouldRefreshFeeds,
    listFeedItems,
    refreshFeeds,
    rerunSummaries,
    getFeedItem,
    setSelectedTopic,
    saveEvent,
    deleteEvent,
    saveTask,
    deleteTask,
  };
}

module.exports = {
  createDashboardStore,
};
