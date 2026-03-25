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
const SUMMARY_CONCURRENCY = 1;
const MAX_PER_SOURCE = 24;
const MAX_REFRESH_ITEMS = 240;
const MAX_CONTENT_CHARS = 12_000;
const MAX_EMBED_INPUT_CHARS = 2_500;
const KEYWORD_WEIGHT = 3;
const SEMANTIC_MIN_SCORE = 0.18;
const SOURCE_BALANCE_TARGET = 2;

const STARTER_FEEDS = [
  { id: 'reuters-politics', name: 'The Guardian Politics', url: 'https://www.theguardian.com/politics/rss', topic: 'politics', topic_label: 'Politics', source_kind: 'publisher' },
  { id: 'reuters-world', name: 'BBC World', url: 'https://feeds.bbci.co.uk/news/world/rss.xml', topic: 'world', topic_label: 'World', source_kind: 'publisher' },
  { id: 'reuters-business', name: 'BBC Business', url: 'https://feeds.bbci.co.uk/news/business/rss.xml', topic: 'econ', topic_label: 'Business', source_kind: 'publisher' },
  { id: 'reuters-tech', name: 'TechCrunch', url: 'https://techcrunch.com/feed/', topic: 'tech', topic_label: 'Technology', source_kind: 'publisher' },
  { id: 'ap-politics', name: 'NPR Politics', url: 'https://feeds.npr.org/1014/rss.xml', topic: 'politics', topic_label: 'Politics', source_kind: 'publisher' },
  { id: 'verge', name: 'The Verge', url: 'https://www.theverge.com/rss/index.xml', topic: 'tech', topic_label: 'Technology', source_kind: 'publisher' },
  { id: 'ars', name: 'Ars Technica', url: 'https://feeds.arstechnica.com/arstechnica/index', topic: 'tech', topic_label: 'Technology', source_kind: 'publisher' },
  { id: 'ap-top', name: 'NPR News', url: 'https://feeds.npr.org/1001/rss.xml', topic: OTHER_TOPIC, topic_label: 'Top News', source_kind: 'publisher' },
];

function normalizeFeedSettings(raw = {}) {
  const src = (raw && typeof raw === 'object') ? raw : {};
  const knownSourceIds = new Set(STARTER_FEEDS.map((feed) => String(feed.id || '').trim()).filter(Boolean));
  const disabledSourceIds = Array.isArray(src.rss_disabled_source_ids)
    ? src.rss_disabled_source_ids
      .map((item) => String(item || '').trim())
      .filter((item) => item && knownSourceIds.has(item))
    : [];
  return {
    rss_strict_source_topics: Object.prototype.hasOwnProperty.call(src, 'rss_strict_source_topics')
      ? !!src.rss_strict_source_topics
      : true,
    rss_disabled_source_ids: Array.from(new Set(disabledSourceIds)),
  };
}

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
  let decoded = String(value || '').replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1');
  // Some feeds double-encode entities, e.g. "&amp;#x27;".
  for (let i = 0; i < 3; i += 1) {
    const next = decoded
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
    if (next === decoded) break;
    decoded = next;
  }
  return decoded;
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

function extractTagTexts(block = '', tagName = '') {
  const pattern = new RegExp(`<${escapeRegex(tagName)}\\b[^>]*>([\\s\\S]*?)<\\/${escapeRegex(tagName)}>`, 'gi');
  return [...String(block || '').matchAll(pattern)]
    .map((match) => String(match[1] || ''))
    .filter(Boolean);
}

function extractTagAttributeValues(block = '', tagName = '', attrName = '') {
  const pattern = new RegExp(`<${escapeRegex(tagName)}\\b[^>]*\\b${escapeRegex(attrName)}=(?:"([^"]+)"|'([^']+)')[^>]*\\/?>`, 'gi');
  return [...String(block || '').matchAll(pattern)]
    .map((match) => decodeXmlEntities(String(match[1] || match[2] || '').trim()))
    .filter(Boolean);
}

function extractLink(block = '') {
  const raw = String(block || '');
  const atomHref = raw.match(/<link\b[^>]*href=(?:"([^"]+)"|'([^']+)')[^>]*\/?>/i);
  if (atomHref) return decodeXmlEntities(String(atomHref[1] || atomHref[2] || '').trim());
  const simple = extractTagText(raw, 'link');
  return decodeXmlEntities(String(simple || '').trim());
}

function extractSourceMeta(block = '') {
  const raw = String(block || '');
  const match = raw.match(/<source\b[^>]*url=(?:"([^"]+)"|'([^']+)')[^>]*>([\s\S]*?)<\/source>/i);
  if (!match) return { name: '', url: '' };
  return {
    name: stripXmlTags(match[3] || ''),
    url: decodeXmlEntities(String(match[1] || match[2] || '').trim()),
  };
}

function splitFeedTagText(value = '') {
  return String(value || '')
    .split(/[|,/;>]+/)
    .map((item) => normalizeWhitespace(stripXmlTags(item)))
    .filter(Boolean);
}

const RSS_TOPIC_KEYWORDS = {
  politics: ['politics', 'political', 'government', 'policy', 'policies', 'election', 'elections', 'congress', 'senate', 'house', 'white house', 'campaign', 'diplomacy', 'trump', 'biden'],
  world: ['world', 'international', 'global', 'foreign', 'middle east', 'europe', 'asia', 'africa', 'americas', 'ukraine', 'iran', 'china', 'india'],
  econ: ['business', 'economy', 'economics', 'finance', 'financial', 'markets', 'market', 'companies', 'company', 'banking', 'trade', 'stocks', 'money', 'earnings'],
  tech: ['technology', 'tech', 'ai', 'artificial intelligence', 'software', 'hardware', 'cybersecurity', 'internet', 'gadgets', 'mobile', 'apps', 'computing', 'chip', 'chips', 'semiconductor', 'science'],
  other: ['science', 'health', 'sports', 'culture', 'entertainment', 'weather', 'education', 'environment'],
};

function deriveRssTopicsFromTags(tags = []) {
  const normalizedTags = (Array.isArray(tags) ? tags : [])
    .map((tag) => normalizeWhitespace(String(tag || '').toLowerCase()))
    .filter(Boolean);
  if (!normalizedTags.length) return [];
  const scores = new Map(TOPIC_KEYS.map((topic) => [topic, 0]));
  normalizedTags.forEach((tag) => {
    Object.entries(RSS_TOPIC_KEYWORDS).forEach(([topic, keywords]) => {
      keywords.forEach((keyword) => {
        const needle = String(keyword || '').trim().toLowerCase();
        if (!needle) return;
        if (tag === needle) scores.set(topic, Number(scores.get(topic) || 0) + 3);
        else if (tag.includes(needle) || needle.includes(tag)) scores.set(topic, Number(scores.get(topic) || 0) + 1);
      });
    });
  });
  const ranked = Array.from(scores.entries())
    .filter((entry) => Number(entry[1] || 0) > 0)
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    })
    .map((entry) => entry[0]);
  return ranked.slice(0, 3);
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

function normalizeSourceKind(value = '') {
  return String(value || '').trim().toLowerCase() === 'aggregator' ? 'aggregator' : 'publisher';
}

function isGoogleNewsArticleUrl(value = '') {
  try {
    const parsed = new URL(String(value || '').trim());
    const host = String(parsed.hostname || '').toLowerCase();
    const parts = String(parsed.pathname || '').split('/').filter(Boolean);
    return host === 'news.google.com' && (parts.includes('articles') || parts.includes('read'));
  } catch (_) {
    return false;
  }
}

function extractGoogleNewsArticleId(value = '') {
  try {
    const parsed = new URL(String(value || '').trim());
    const parts = String(parsed.pathname || '').split('/').filter(Boolean);
    if (!parts.length) return '';
    const idx = Math.max(parts.lastIndexOf('articles'), parts.lastIndexOf('read'));
    if (idx < 0 || idx >= parts.length - 1) return '';
    return String(parts[idx + 1] || '').trim();
  } catch (_) {
    return '';
  }
}

function decodeGoogleNewsArticleIdLocally(articleId = '') {
  const id = String(articleId || '').trim();
  if (!id) return { ok: false, needsBatch: false, url: '', id: '' };
  try {
    const decodedBytes = Buffer.from(id, 'base64url');
    let decoded = decodedBytes.toString('latin1');
    const prefix = Buffer.from([0x08, 0x13, 0x22]).toString('latin1');
    if (decoded.startsWith(prefix)) decoded = decoded.slice(prefix.length);
    const suffix = Buffer.from([0xd2, 0x01, 0x00]).toString('latin1');
    if (decoded.endsWith(suffix)) decoded = decoded.slice(0, -suffix.length);
    const bytes = Buffer.from(decoded, 'latin1');
    if (!bytes.length) return { ok: false, needsBatch: false, url: '', id };
    const first = bytes[0];
    const start = first >= 0x80 ? 2 : 1;
    const end = (first >= 0x80 ? bytes[1] : first) + 1;
    const candidate = bytes.subarray(start, end).toString('latin1');
    if (candidate.startsWith('AU_yqL')) return { ok: false, needsBatch: true, url: '', id };
    const normalized = normalizeUrl(candidate);
    return normalized ? { ok: true, needsBatch: false, url: normalized, id } : { ok: false, needsBatch: false, url: '', id };
  } catch (_) {
    return { ok: false, needsBatch: false, url: '', id };
  }
}

async function decodeGoogleNewsArticleIdsViaBatch(articleIds = [], fetchImpl = fetch) {
  const ids = (Array.isArray(articleIds) ? articleIds : []).map((item) => String(item || '').trim()).filter(Boolean);
  if (!ids.length) return new Map();
  const envelopes = ids.map((id, index) => (
    `["Fbv4je","[\\"garturlreq\\",[[\\"en-US\\",\\"US\\",[\\"FINANCE_TOP_INDICES\\",\\"WEB_TEST_1_0_0\\"],null,null,1,1,\\"US:en\\",null,180,null,null,null,null,null,0,null,null,[1608992183,723341000]],\\"en-US\\",\\"US\\",1,[2,3,4,8],1,0,\\"655000234\\",0,0,null,0],\\"${id}\\"]",null,"${index + 1}"]`
  ));
  const body = `[[${envelopes.join(',')}]]`;
  const response = await fetchImpl('https://news.google.com/_/DotsSplashUi/data/batchexecute?rpcids=Fbv4je', {
    method: 'POST',
    headers: {
      'content-type': 'application/x-www-form-urlencoded;charset=utf-8',
      referer: 'https://news.google.com/',
    },
    body: new URLSearchParams({ 'f.req': body }),
  });
  if (!response || !response.ok) return new Map();
  const text = await response.text();
  const urls = [];
  let rest = text;
  const header = '[\\"garturlres\\",\\"';
  const footer = '\\",';
  while (rest.includes(header)) {
    const start = rest.split(header, 2)[1] || '';
    if (!start.includes(footer)) break;
    const url = start.split(footer, 1)[0] || '';
    urls.push(normalizeUrl(url.replace(/\\u003d/g, '=').replace(/\\u0026/g, '&').replace(/\\"/g, '"')));
    rest = start.split(footer, 2)[1] || '';
  }
  const out = new Map();
  ids.forEach((id, index) => {
    const resolved = String(urls[index] || '').trim();
    if (resolved) out.set(id, resolved);
  });
  return out;
}

async function resolveGoogleNewsUrls(items = [], fetchImpl = fetch) {
  const list = Array.isArray(items) ? items : [];
  const pendingIds = [];
  const pendingByIndex = new Map();
  const resolvedItems = list.map((item, index) => {
    const normalized = { ...(item || {}) };
    if (normalizeSourceKind(normalized.source_kind) !== 'aggregator' || !isGoogleNewsArticleUrl(normalized.url)) {
      return normalized;
    }
    const articleId = extractGoogleNewsArticleId(normalized.url);
    const local = decodeGoogleNewsArticleIdLocally(articleId);
    if (local.ok && local.url) {
      normalized.canonical_article_url = local.url;
      normalized.aggregator_resolved = true;
      normalized.url = local.url;
      return normalized;
    }
    if (local.needsBatch && articleId) {
      pendingIds.push(articleId);
      pendingByIndex.set(index, articleId);
    }
    normalized.canonical_article_url = '';
    normalized.aggregator_resolved = false;
    return normalized;
  });
  if (!pendingIds.length) return resolvedItems;
  const batchResolved = await decodeGoogleNewsArticleIdsViaBatch(pendingIds, fetchImpl).catch(() => new Map());
  pendingByIndex.forEach((articleId, index) => {
    const resolved = String(batchResolved.get(articleId) || '').trim();
    if (!resolved) return;
    resolvedItems[index] = {
      ...resolvedItems[index],
      canonical_article_url: resolved,
      aggregator_resolved: true,
      url: resolved,
    };
  });
  return resolvedItems;
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
    Array.isArray(item.rss_tags) ? item.rss_tags.join(' ') : '',
    String(item.source_name || ''),
    String(item.source_domain || ''),
    String(item.url || ''),
  ].filter(Boolean).join(' '));
}

function getItemLookupKeys(item = {}) {
  const keys = [];
  const canonical = canonicalizeUrl(item.canonical_article_url || item.url || '');
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

function deriveReadabilityState(src = {}) {
  const summaryStatus = String(src.summary_status || '').trim().toLowerCase();
  const fetchStatus = String(src.fetch_status || '').trim().toLowerCase();
  const failureReason = String(src.failure_reason || '').trim();
  const hasFullContent = !!(src.has_full_content || (Number(src.content_fetched_at || 0) > 0 && String(src.raw_content_text || src.content_text || '').trim()));
  if (normalizeSourceKind(src.source_kind) === 'aggregator' && !src.aggregator_resolved) return 'unavailable';
  if (summaryStatus === 'unavailable') return 'unavailable';
  if (hasFullContent && fetchStatus === 'fetched') return 'readable';
  if (failureReason && !hasFullContent) return 'headline_only';
  return 'headline_only';
}

function getReadabilityRank(item = {}) {
  const state = String(item.readability_state || '').trim().toLowerCase();
  if (state === 'readable') return 0;
  if (state === 'headline_only') return 1;
  return 2;
}

function sortFeedItemsByQualityAndRecency(items = []) {
  return [...(Array.isArray(items) ? items : [])].sort((a, b) => {
    const rankDiff = getReadabilityRank(a) - getReadabilityRank(b);
    if (rankDiff !== 0) return rankDiff;
    return Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0);
  });
}

function applySourceBalance(items = [], limit = 80, options = {}) {
  const list = Array.isArray(items) ? items.slice() : [];
  if (!list.length) return [];
  const visibleLimit = Math.max(1, Number(limit || list.length) || list.length);
  const perSourceLimit = Math.max(1, Number(options.perSourceLimit || SOURCE_BALANCE_TARGET) || SOURCE_BALANCE_TARGET);
  const strictUntil = Math.min(visibleLimit, Math.max(6, perSourceLimit * 5));
  const picked = [];
  const pickedIds = new Set();
  const sourceCounts = new Map();
  const sourceOf = (item = {}) => String(item.source_id || item.source_domain || item.source_name || 'unknown').trim().toLowerCase() || 'unknown';
  const sorted = list.slice();
  let cursor = 0;
  while (picked.length < Math.min(visibleLimit, sorted.length)) {
    let chosenIndex = -1;
    for (let i = 0; i < sorted.length; i += 1) {
      const candidate = sorted[i];
      if (!candidate) continue;
      const id = String(candidate.id || '').trim();
      if (id && pickedIds.has(id)) continue;
      const sourceKey = sourceOf(candidate);
      const used = Number(sourceCounts.get(sourceKey) || 0);
      const inStrictWindow = picked.length < strictUntil;
      if (!inStrictWindow || used < perSourceLimit) {
        chosenIndex = i;
        break;
      }
    }
    if (chosenIndex < 0) {
      for (let i = cursor; i < sorted.length; i += 1) {
        const candidate = sorted[i];
        if (!candidate) continue;
        const id = String(candidate.id || '').trim();
        if (!id || !pickedIds.has(id)) {
          chosenIndex = i;
          break;
        }
      }
    }
    if (chosenIndex < 0) break;
    const chosen = sorted[chosenIndex];
    const id = String(chosen.id || '').trim();
    const sourceKey = sourceOf(chosen);
    picked.push(chosen);
    if (id) pickedIds.add(id);
    sourceCounts.set(sourceKey, Number(sourceCounts.get(sourceKey) || 0) + 1);
    cursor = chosenIndex + 1;
  }
  return picked;
}

function finalizeFeedListing(items = [], options = {}) {
  const limit = Math.max(1, Number(options.limit || 80) || 80);
  const hideUnavailable = options.hideUnavailable !== false;
  const readableFirst = sortFeedItemsByQualityAndRecency(
    (Array.isArray(items) ? items : []).filter((item) => {
      if (!item) return false;
      if (normalizeSourceKind(item.source_kind) === 'aggregator' && !item.aggregator_resolved) return false;
      return !hideUnavailable || String((item && item.readability_state) || '').trim() !== 'unavailable';
    })
  );
  const balanced = applySourceBalance(readableFirst, limit, {
    perSourceLimit: options.perSourceLimit,
  });
  return balanced.slice(0, limit);
}

function normalizeStoredFeedItem(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const canonicalArticleUrl = normalizeUrl(src.canonical_article_url || src.url || '');
  const canonicalUrl = normalizeUrl(src.url || canonicalArticleUrl || '');
  const decodedDisplayTitle = normalizeWhitespace(decodeXmlEntities(String(src.display_title || src.crawler_title || src.title || '').trim()));
  const decodedTitle = normalizeWhitespace(decodeXmlEntities(String(src.title || src.display_title || '').trim()));
  const decodedCrawlerTitle = normalizeWhitespace(decodeXmlEntities(String(src.crawler_title || '').trim()));
  const decodedSummary = normalizeWhitespace(decodeXmlEntities(String(src.summary || '').trim()));
  const displayTitle = String(
    decodedDisplayTitle
    || decodedCrawlerTitle
    || decodedTitle
    || canonicalUrl
    || 'Untitled'
  ).trim() || 'Untitled';
  const rawContentText = decodeXmlEntities(String(src.raw_content_text || src.content_text || src.summary || ''))
    .replace(/\u0000/g, '')
    .trim()
    .slice(0, MAX_CONTENT_CHARS);
  const cleanSummary = decodeXmlEntities(String(src.clean_summary || decodedSummary || ''))
    .trim()
    .slice(0, MAX_CONTENT_CHARS);
  const cleanExcerpt = decodeXmlEntities(String(src.clean_excerpt || buildContentExcerpt(cleanSummary || rawContentText || decodedSummary || '')).trim());
  const contentMarkdown = String(src.content_markdown || '').trim().slice(0, MAX_CONTENT_CHARS);
  const readabilityState = deriveReadabilityState({
    ...src,
    raw_content_text: rawContentText,
    content_text: rawContentText,
  });
  return {
    id: String(src.id || hashText(`${canonicalUrl}|${displayTitle}|${src.published_at || 0}`)).trim(),
    url: canonicalUrl,
    canonical_article_url: canonicalArticleUrl,
    title: decodedTitle || displayTitle,
    crawler_title: decodedCrawlerTitle,
    display_title: displayTitle,
    summary: decodedSummary,
    raw_content_text: rawContentText,
    content_text: rawContentText,
    clean_summary: cleanSummary,
    clean_excerpt: cleanExcerpt,
    content_excerpt: decodeXmlEntities(String(src.content_excerpt || cleanExcerpt || buildContentExcerpt(rawContentText || decodedSummary || '')).trim()),
    content_markdown: contentMarkdown,
    source_id: String(src.source_id || '').trim(),
    source_name: String(src.source_name || '').trim(),
    source_domain: String(src.source_domain || getDomain(canonicalArticleUrl || canonicalUrl)).trim(),
    source_kind: normalizeSourceKind(src.source_kind || 'publisher'),
    discovery_source_id: String(src.discovery_source_id || '').trim(),
    discovery_source_name: String(src.discovery_source_name || '').trim(),
    discovery_source_domain: String(src.discovery_source_domain || '').trim(),
    aggregator_resolved: !!src.aggregator_resolved,
    rss_tags: Array.isArray(src.rss_tags) ? src.rss_tags.map((item) => normalizeWhitespace(String(item || ''))).filter(Boolean).slice(0, 16) : [],
    rss_topics: Array.isArray(src.rss_topics) ? src.rss_topics.map((item) => normalizeClassifiedTopic(item)).filter(Boolean).slice(0, 4) : [],
    topic: normalizeClassifiedTopic(src.topic || src.source_topic || OTHER_TOPIC),
    topic_source: String(src.topic_source || 'legacy').trim() || 'legacy',
    source_topic: normalizeClassifiedTopic(src.source_topic || src.topic || OTHER_TOPIC),
    published_at: Number(src.published_at || 0) || 0,
    fetched_at: Number(src.fetched_at || 0) || 0,
    content_fetched_at: Number(src.content_fetched_at || 0) || 0,
    summary_generated_at: Number(src.summary_generated_at || 0) || 0,
    summary_model_id: String(src.summary_model_id || '').trim(),
    summary_status: String(src.summary_status || (cleanSummary ? 'generated' : 'empty')).trim() || 'empty',
    content_quality: String(src.content_quality || '').trim(),
    entities: Array.isArray(src.entities) ? src.entities.slice(0, 12).map((item) => String(item || '').trim()).filter(Boolean) : [],
    summary_content_hash: String(src.summary_content_hash || '').trim(),
    hidden_from_feed: !!src.hidden_from_feed,
    manual_retry_count: Math.max(0, Number(src.manual_retry_count || 0) || 0),
    failure_reason: String(src.failure_reason || '').trim(),
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
    readability_state: readabilityState,
  };
}

function parseFeedXml(xmlText = '', source = {}) {
  const xml = String(xmlText || '');
  const itemMatches = [...xml.matchAll(/<item\b[\s\S]*?<\/item>/gi)].map((match) => String(match[0] || ''));
  const entryMatches = [...xml.matchAll(/<entry\b[\s\S]*?<\/entry>/gi)].map((match) => String(match[0] || ''));
  const blocks = itemMatches.length > 0 ? itemMatches : entryMatches;
  const fetchedAt = nowTs();
  return blocks.map((block) => {
    const sourceMeta = extractSourceMeta(block);
    const title = stripXmlTags(extractTagText(block, 'title'));
    const url = normalizeUrl(extractLink(block));
    const rawTags = [
      ...extractTagTexts(block, 'category').flatMap((item) => splitFeedTagText(item)),
      ...extractTagTexts(block, 'dc:subject').flatMap((item) => splitFeedTagText(item)),
      ...extractTagTexts(block, 'media:keywords').flatMap((item) => splitFeedTagText(item)),
      ...extractTagTexts(block, 'news:keywords').flatMap((item) => splitFeedTagText(item)),
      ...extractTagAttributeValues(block, 'category', 'term').flatMap((item) => splitFeedTagText(item)),
    ];
    const rssTags = Array.from(new Set(rawTags.map((item) => normalizeWhitespace(item)).filter(Boolean))).slice(0, 16);
    const rssTopics = deriveRssTopicsFromTags(rssTags);
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
    const sourceKind = normalizeSourceKind(source.source_kind || 'publisher');
    const publisherName = sourceKind === 'aggregator' ? String(sourceMeta.name || '').trim() : '';
    return {
      id: hashText(`${source.id || 'feed'}|${url}|${title}|${publishedAt}`),
      title: title || url || 'Untitled',
      url,
      canonical_article_url: sourceKind === 'aggregator' ? '' : url,
      summary,
      source_id: String(source.id || '').trim(),
      source_name: publisherName || String(source.name || '').trim(),
      source_domain: getDomain(sourceMeta.url || url || source.url || ''),
      source_kind: sourceKind,
      discovery_source_id: sourceKind === 'aggregator' ? String(source.id || '').trim() : '',
      discovery_source_name: sourceKind === 'aggregator' ? String(source.name || '').trim() : '',
      discovery_source_domain: sourceKind === 'aggregator' ? getDomain(source.url || '') : '',
      aggregator_resolved: sourceKind === 'aggregator' ? false : true,
      rss_tags: rssTags,
      rss_topics: rssTopics,
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

function buildUnavailableSummaryPatch(item = {}, reason = '') {
  const normalized = normalizeStoredFeedItem(item);
  const priorSummary = String(normalized.clean_summary || '').trim();
  const priorExcerpt = String(normalized.clean_excerpt || '').trim();
  const fallbackSummary = priorSummary || '';
  const fallbackExcerpt = priorExcerpt || '';
  return {
    clean_summary: fallbackSummary,
    clean_excerpt: fallbackExcerpt,
    summary_generated_at: nowTs(),
    summary_model_id: String(normalized.summary_model_id || '').trim(),
    summary_status: 'unavailable',
    content_quality: String(normalized.content_quality || 'fragmented').trim() || 'fragmented',
    entities: Array.isArray(normalized.entities) ? normalized.entities.slice(0, 12) : [],
    summary_content_hash: hashText(String(normalized.raw_content_text || normalized.content_text || '').trim()),
    failure_reason: String(reason || '').trim(),
  };
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
  const getFeedSettings = typeof options.getFeedSettings === 'function' ? options.getFeedSettings : (() => ({}));
  const summarizeArticle = typeof options.summarizeArticle === 'function' ? options.summarizeArticle : null;
  const fetchArticlePreview = typeof options.fetchArticlePreview === 'function' ? options.fetchArticlePreview : fetchWebPagePreview;
  const resolveAggregatorItems = typeof options.resolveAggregatorItems === 'function'
    ? options.resolveAggregatorItems
    : ((items = []) => resolveGoogleNewsUrls(items, fetch));
  let refreshPromise = null;

  function getConfiguredFeedSettings() {
    return normalizeFeedSettings(getFeedSettings() || {});
  }

  function isSourceEnabled(sourceId = '', settings = getConfiguredFeedSettings()) {
    const id = String(sourceId || '').trim();
    if (!id) return true;
    const disabled = new Set(Array.isArray(settings.rss_disabled_source_ids) ? settings.rss_disabled_source_ids : []);
    return !disabled.has(id);
  }

  function getFeedSourcesWithSettings(settings = getConfiguredFeedSettings()) {
    return clone(STARTER_FEEDS).map((feed) => ({
      ...feed,
      topic: normalizeClassifiedTopic(feed.topic),
      topic_label: String(feed.topic_label || feed.topic || '').trim(),
      domain: getDomain(feed.url),
      enabled: isSourceEnabled(feed.id, settings),
    }));
  }

  function isKnownFeedSourceId(sourceId = '') {
    const id = String(sourceId || '').trim();
    if (!id) return true;
    return STARTER_FEEDS.some((feed) => String(feed.id || '').trim() === id);
  }

  function getActiveFeedSources(settings = getConfiguredFeedSettings()) {
    return getFeedSourcesWithSettings(settings).filter((feed) => !!feed.enabled);
  }

  function shouldIncludeFeedItem(item = {}, settings = getConfiguredFeedSettings()) {
    const sourceId = String((item && item.source_id) || '').trim();
    return isKnownFeedSourceId(sourceId) && isSourceEnabled(sourceId, settings);
  }

  function getFeedTopicForListing(item = {}, settings = getConfiguredFeedSettings()) {
    if (settings.rss_strict_source_topics) return normalizeClassifiedTopic(item && item.source_topic);
    return normalizeClassifiedTopic(item && item.topic);
  }

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

  function getTopicLabels(settings = getConfiguredFeedSettings()) {
    const labels = { [DEFAULT_TOPIC]: 'All' };
    getFeedSourcesWithSettings(settings)
      .filter((source) => !!source.enabled)
      .forEach((source) => {
        const topic = normalizeClassifiedTopic(source.topic);
        if (!topic || labels[topic]) return;
        labels[topic] = String(source.topic_label || source.topic || '').trim() || topic;
      });
    return labels;
  }

  function getAvailableTopics(items = [], settings = getConfiguredFeedSettings()) {
    const labels = getTopicLabels(settings);
    const topics = (Array.isArray(items) ? items : [])
      .filter((item) => shouldIncludeFeedItem(item, settings))
      .filter((item) => String((item && item.readability_state) || '').trim() !== 'unavailable')
      .map((item) => getFeedTopicForListing(item, settings))
      .filter(Boolean);
    return {
      topics: [DEFAULT_TOPIC, ...Array.from(new Set(topics))],
      topic_labels: Object.fromEntries(
        Object.entries(labels).filter(([topic]) => topic === DEFAULT_TOPIC || topics.includes(topic))
      ),
    };
  }

  function getState() {
    const state = readState();
    const feedSettings = getConfiguredFeedSettings();
    const sourceItems = mergeFeedItems([], pruneFeedItems(state.rss && state.rss.items));
    const availableTopics = getAvailableTopics(sourceItems, feedSettings);
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
          sources: getFeedSourcesWithSettings(feedSettings),
          topics: availableTopics.topics,
          topic_labels: availableTopics.topic_labels,
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

  function resetFeedCache() {
    const state = readState();
    const clearedItems = pruneFeedItems(state.rss && state.rss.items).length;
    state.rss = {
      items: [],
      last_refreshed_at: 0,
    };
    writeState(state);
    return {
      ok: true,
      cleared_items: clearedItems,
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

function mergeFeedItems(existingItems = [], incomingItems = []) {
  const mergedMap = new Map();
  (Array.isArray(existingItems) ? existingItems : []).forEach((item) => {
    getItemLookupKeys(item).forEach((key) => {
      if (!mergedMap.has(key)) mergedMap.set(key, item);
    });
  });
  (Array.isArray(incomingItems) ? incomingItems : []).forEach((item) => {
    getItemLookupKeys(item).forEach((key) => {
      mergedMap.set(key, item);
    });
  });
  const unique = new Map();
  mergedMap.forEach((item) => {
    const normalized = normalizeStoredFeedItem(item);
    const key = canonicalizeUrl(normalized.canonical_article_url || normalized.url || '') || `id:${normalized.id}`;
    const existing = unique.get(key);
    const existingKind = normalizeSourceKind(existing && existing.source_kind);
    const nextKind = normalizeSourceKind(normalized.source_kind);
    if (
      !existing
      || (existingKind === 'aggregator' && nextKind === 'publisher')
      || (existingKind === nextKind && Number(normalized.published_at || 0) > Number(existing.published_at || 0))
    ) {
      unique.set(key, normalized);
    }
  });
  return pruneFeedItems(Array.from(unique.values()))
    .sort((a, b) => Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0));
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
      const rssTopics = Array.isArray(item.rss_topics) ? item.rss_topics.map((topic) => normalizeClassifiedTopic(topic)).filter(Boolean) : [];
      const rssTopic = rssTopics[0] || '';
      let bestTopic = rssTopic || normalizeClassifiedTopic(item.source_topic || item.topic || OTHER_TOPIC);
      let bestScore = -1;
      prototypeVectors.forEach((entry) => {
        const score = cosineSimilarity(vector, entry.vector);
        if (score > bestScore) {
          bestScore = score;
          if (!rssTopic) bestTopic = entry.topic;
        }
      });
      const classifiedTopic = rssTopic || (bestScore >= SEMANTIC_MIN_SCORE ? bestTopic : OTHER_TOPIC);
      return normalizeStoredFeedItem({
        ...item,
        embedding: vector,
        topic: classifiedTopic,
        topic_source: rssTopic ? 'rss_tag' : (vector.length ? 'embedding' : 'source_fallback'),
      });
    });
  }

  async function buildCleanSummary(item = {}, existing = null, options = {}) {
    const normalized = normalizeStoredFeedItem({ ...existing, ...item });
    const rawContentText = String(normalized.raw_content_text || normalized.content_text || '').trim();
    const contentHash = hashText(rawContentText);
    const force = !!options.force;
    if (!rawContentText) {
      return buildUnavailableSummaryPatch(normalized, 'empty_article_body');
    }
    if (!force && existing && String(existing.summary_content_hash || '').trim() === contentHash && String(existing.clean_summary || '').trim()) {
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
        failure_reason: '',
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
        failure_reason: '',
      };
    }
    if (String(res.status || '').trim() === 'unavailable') {
      return {
        ...buildUnavailableSummaryPatch(normalized, String(res.reason || '').trim() || 'llm_declared_unavailable'),
        summary_model_id: String(res.model_id || '').trim(),
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
      failure_reason: '',
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
    const preview = await fetchArticlePreview(targetUrl, { markdownFirst: true, maxChars: MAX_CONTENT_CHARS, timeoutMs: 12_000 });
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
    const feedSettings = getConfiguredFeedSettings();
    const state = readState();
    const sourceItems = mergeFeedItems([], pruneFeedItems(state.rss && state.rss.items));
    const availableTopics = getAvailableTopics(sourceItems, feedSettings);
    let items = sourceItems
      .filter((item) => shouldIncludeFeedItem(item, feedSettings))
      .filter((item) => topic === DEFAULT_TOPIC || getFeedTopicForListing(item, feedSettings) === topic);
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
          const rankDiff = getReadabilityRank(a.item) - getReadabilityRank(b.item);
          if (rankDiff !== 0) return rankDiff;
          return Number((b.item && b.item.published_at) || 0) - Number((a.item && a.item.published_at) || 0);
        })
        .map((entry) => entry.item);
      items = finalizeFeedListing(items, {
        limit,
        perSourceLimit: Math.max(2, Math.floor(limit / 8) || 2),
      });
    } else {
      items = finalizeFeedListing(items, {
        limit,
        perSourceLimit: Math.max(2, Math.floor(limit / 10) || 2),
      });
    }
    return {
      ok: true,
      topic,
      query,
      items: clone(items),
      last_refreshed_at: Number((state.rss && state.rss.last_refreshed_at) || 0),
      topics: availableTopics.topics,
      topic_labels: availableTopics.topic_labels,
    };
  }

  async function runFeedRefresh(options = {}) {
    const force = !!options.force;
    const progress = typeof options.onProgress === 'function' ? options.onProgress : null;
    const discardExisting = !!options.discardExisting;
    const emitProgress = (meta = {}) => {
      if (!progress) return;
      try {
        progress(meta);
      } catch (_) {
        // ignore observer failures
      }
    };
    if (!force && !shouldRefreshFeeds()) {
      return listFeedItems({ topic: options.topic, limit: options.limit, query: options.query });
    }
    const state = readState();
    const existingItems = discardExisting ? [] : pruneFeedItems(state.rss && state.rss.items);
    const existingMap = getLookupMap(existingItems);
    const discoveredMap = new Map();
    const errors = [];
    const feedSettings = getConfiguredFeedSettings();
    const activeSources = getActiveFeedSources(feedSettings);
    emitProgress({
      stage: 'fetching_feeds',
      total: activeSources.length,
      completed: 0,
      label: 'Fetching RSS sources',
    });
    const results = await Promise.allSettled(activeSources.map(async (source) => {
      const xml = await fetchTextWithTimeout(source.url);
      return parseFeedXml(xml, source).slice(0, MAX_PER_SOURCE);
    }));
    results.forEach((result, index) => {
      const source = activeSources[index];
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
    const discovered = await resolveAggregatorItems(Array.from(discoveredMap.values()))
      .then((items) => Array.isArray(items) ? items : Array.from(discoveredMap.values()))
      .catch(() => Array.from(discoveredMap.values()));
    const orderedDiscovered = discovered
      .sort((a, b) => Number((b && b.published_at) || 0) - Number((a && a.published_at) || 0))
      .slice(0, MAX_REFRESH_ITEMS);
    emitProgress({
      stage: 'enriching_articles',
      total: orderedDiscovered.length,
      completed: 0,
      label: orderedDiscovered.length ? 'Fetching article text' : 'No feed items discovered',
    });
    let enrichedCompleted = 0;
    const enriched = await mapWithConcurrency(orderedDiscovered, ENRICH_CONCURRENCY, async (candidate) => {
      const lookupKey = canonicalizeUrl(candidate.canonical_article_url || candidate.url || '');
      const result = await enrichFeedCandidate(candidate, discardExisting ? null : existingMap.get(`url:${lookupKey}`) || null);
      enrichedCompleted += 1;
      emitProgress({
        stage: 'enriching_articles',
        total: orderedDiscovered.length,
        completed: enrichedCompleted,
        label: String((candidate && (candidate.title || candidate.url)) || 'article').trim(),
      });
      return result;
    });
    state.rss = {
      items: mergeFeedItems(existingItems, enriched),
      last_refreshed_at: nowTs(),
    };
    writeState(state);
    emitProgress({
      stage: 'summarizing_articles',
      total: enriched.length,
      completed: 0,
      label: enriched.length ? 'Summarizing feeds' : 'No articles to summarize',
    });
    let summarizedCompleted = 0;
    const summarized = await mapWithConcurrency(enriched, SUMMARY_CONCURRENCY, async (item) => {
      const existing = discardExisting ? null : existingMap.get(`url:${canonicalizeUrl(item.canonical_article_url || item.url || '')}`) || null;
      const summaryPatch = await buildCleanSummary(item, existing);
      summarizedCompleted += 1;
      emitProgress({
        stage: 'summarizing_articles',
        total: enriched.length,
        completed: summarizedCompleted,
        label: String((item && (item.display_title || item.title || item.url)) || 'article').trim(),
      });
      return normalizeStoredFeedItem({
        ...item,
        ...summaryPatch,
      });
    });
    const classified = await classifyItems(summarized);
    state.rss = {
      items: mergeFeedItems(existingItems, classified),
      last_refreshed_at: nowTs(),
    };
    writeState(state);
    const listing = await listFeedItems({ topic: options.topic, limit: options.limit, query: options.query });
    return {
      ...listing,
      message: errors.length ? errors.join(' | ') : '',
    };
  }

  function isFeedRefreshInFlight() {
    return !!refreshPromise;
  }

  function refreshFeedsInBackground(options = {}) {
    const force = !!options.force;
    if (!force && !shouldRefreshFeeds()) {
      return {
        ok: true,
        refresh_started: false,
        refresh_in_flight: isFeedRefreshInFlight(),
      };
    }
    if (!refreshPromise) {
      refreshPromise = runFeedRefresh(options)
        .catch(() => null)
        .finally(() => {
          refreshPromise = null;
        });
      return {
        ok: true,
        refresh_started: true,
        refresh_in_flight: true,
      };
    }
    return {
      ok: true,
      refresh_started: false,
      refresh_in_flight: true,
    };
  }

  async function refreshFeeds(options = {}) {
    const force = !!options.force;
    if (!force && !shouldRefreshFeeds()) {
      return listFeedItems({ topic: options.topic, limit: options.limit, query: options.query });
    }
    if (refreshPromise) {
      await refreshPromise.catch(() => null);
      return listFeedItems({ topic: options.topic, limit: options.limit, query: options.query });
    }
    refreshPromise = runFeedRefresh(options)
      .finally(() => {
        refreshPromise = null;
      });
    const result = await refreshPromise.catch(() => null);
    if (result && result.ok) return result;
    return listFeedItems({ topic: options.topic, limit: options.limit, query: options.query });
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

  async function rerunFeedItemSummary(itemId = '') {
    const id = String(itemId || '').trim();
    if (!id) return { ok: false, message: 'item_id is required.' };
    const state = readState();
    const items = pruneFeedItems(state.rss && state.rss.items);
    const index = items.findIndex((entry) => String((entry && entry.id) || '').trim() === id);
    if (index < 0) return { ok: false, message: 'Feed item not found.' };
    const item = normalizeStoredFeedItem(items[index]);
    const manualRetryCount = Math.max(0, Number(item.manual_retry_count || 0) || 0) + 1;
    const refetched = await enrichFeedCandidate({
      ...item,
      raw_content_text: '',
      content_text: '',
      content_markdown: '',
      content_excerpt: item.content_excerpt,
      clean_summary: item.clean_summary,
      clean_excerpt: item.clean_excerpt,
      fetch_status: item.fetch_status,
    }, null);
    const hasUsableText = !!String((refetched && (refetched.raw_content_text || refetched.content_text)) || '').trim();
    const summaryPatch = hasUsableText
      ? await buildCleanSummary({
        ...refetched,
        raw_content_text: String(refetched.raw_content_text || refetched.content_text || '').trim(),
      }, null, { force: true })
      : buildUnavailableSummaryPatch(refetched || item, 'refetch_empty_article_body');
    const shouldDelete = String(summaryPatch.summary_status || '').trim() === 'unavailable' && manualRetryCount >= 2;
    const updatedItem = normalizeStoredFeedItem({
      ...refetched,
      ...summaryPatch,
      manual_retry_count: manualRetryCount,
      hidden_from_feed: false,
    });
    if (shouldDelete) {
      items.splice(index, 1);
    } else {
      items[index] = updatedItem;
    }
    state.rss = {
      items,
      last_refreshed_at: Number((state.rss && state.rss.last_refreshed_at) || 0),
    };
    writeState(state);
    if (shouldDelete) {
      return {
        ok: true,
        deleted: true,
        item_id: id,
        item: clone(updatedItem),
      };
    }
    return {
      ok: true,
      item: clone(updatedItem),
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
    resetFeedCache,
    isFeedRefreshInFlight,
    shouldRefreshFeeds,
    refreshFeedsInBackground,
    listFeedItems,
    refreshFeeds,
    rerunSummaries,
    rerunFeedItemSummary,
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
