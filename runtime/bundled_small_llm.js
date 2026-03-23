const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const MANIFEST_FILENAME = 'policy_manifest.json';
const NOTE_POLICY_SCHEMA_VERSION = 1;
const FEED_SUMMARY_SCHEMA_VERSION = 1;

function nowTs() {
  return Date.now();
}

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function readJsonSafe(filePath = '') {
  try {
    if (!filePath || !fs.existsSync(filePath)) return null;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (_) {
    return null;
  }
}

function hashText(value = '') {
  return crypto.createHash('sha1').update(String(value || ''), 'utf8').digest('hex');
}

function clampNumber(value, min, max, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  if (n < min) return min;
  if (n > max) return max;
  return n;
}

function detectNamedEntities(text = '', limit = 8) {
  const matches = String(text || '').match(/\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}|[A-Z]{2,})\b/g);
  const out = [];
  const seen = new Set();
  for (const item of Array.isArray(matches) ? matches : []) {
    const normalized = normalizeWhitespace(item);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
    if (out.length >= limit) break;
  }
  return out;
}

function firstSentences(text = '', count = 3, maxChars = 420) {
  const sentences = String(text || '')
    .split(/(?<=[.!?])\s+/)
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
  const out = [];
  for (const sentence of sentences) {
    const next = out.concat(sentence).join(' ');
    if (next.length > maxChars && out.length > 0) break;
    out.push(sentence);
    if (out.length >= count) break;
  }
  return normalizeWhitespace(out.join(' ')).slice(0, maxChars);
}

function stripFeedNoise(text = '') {
  return normalizeWhitespace(String(text || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/\b(cookie policy|privacy policy|subscribe to continue reading|read more|sign up for|all rights reserved|javascript is required|enable javascript|share this article|newsletter)\b/gi, ' ')
    .replace(/\s*\|\s*/g, ' ')
    .replace(/\s{2,}/g, ' '));
}

function coerceTopicList(value = [], fallback = []) {
  const items = Array.isArray(value) ? value : [];
  const out = [];
  const seen = new Set();
  items.forEach((item) => {
    const normalized = normalizeWhitespace(String(item || '').toLowerCase()).replace(/[^a-z0-9_-]/g, '');
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
  });
  return out.length ? out.slice(0, 8) : fallback.slice(0, 8);
}

function buildFallbackNotePolicy(note = {}) {
  const title = normalizeWhitespace(String((note && note.title) || ''));
  const body = normalizeWhitespace(String((note && note.body_markdown) || ''));
  const text = `${title}\n${body}`.trim();
  const lower = text.toLowerCase();
  const hasCurrentCue = /\b(today|current|currently|latest|ongoing|right now|developing|breaking)\b/.test(lower);
  const hasAsOfDate = /\bas of\s+[a-z]+\s+\d{1,2},\s+(19|20)\d{2}\b/i.test(text);
  const hasHistoricalCue = /\b(history|historical|timeline|how it started|background|recap|origin|previously|in \d{4})\b/.test(lower);
  const hasOpinionCue = /\b(i think|in my view|should|must|appears to|likely|probably|may|might)\b/.test(lower);
  const hasConflictCue = /\b(war|conflict|missile|strike|attack|invasion|troops|military|ceasefire)\b/.test(lower);

  let noteMode = 'background_brief';
  if ((hasCurrentCue || hasAsOfDate) && hasConflictCue) noteMode = 'live_update';
  else if (hasHistoricalCue && !hasCurrentCue) noteMode = 'historical_summary';
  else if (hasOpinionCue && !hasCurrentCue) noteMode = 'analysis_opinion';
  else if (hasCurrentCue || hasAsOfDate) noteMode = 'live_update';
  else if (hasHistoricalCue && hasOpinionCue) noteMode = 'mixed';

  const defaultsByMode = {
    live_update: {
      freshness_bias: 'high',
      source_mix: 'latest_news',
      contradiction_scan: true,
      result_budget: 8,
      staleness_ttl_minutes: 90,
      prefer_recent_window_days: 3,
    },
    background_brief: {
      freshness_bias: 'medium',
      source_mix: 'mixed',
      contradiction_scan: true,
      result_budget: 5,
      staleness_ttl_minutes: 1440,
      prefer_recent_window_days: 14,
    },
    historical_summary: {
      freshness_bias: 'low',
      source_mix: 'reference_background',
      contradiction_scan: true,
      result_budget: 4,
      staleness_ttl_minutes: 10080,
      prefer_recent_window_days: 365,
    },
    analysis_opinion: {
      freshness_bias: hasCurrentCue ? 'medium' : 'low',
      source_mix: hasCurrentCue ? 'mixed' : 'reference_background',
      contradiction_scan: true,
      result_budget: 4,
      staleness_ttl_minutes: hasCurrentCue ? 720 : 10080,
      prefer_recent_window_days: hasCurrentCue ? 14 : 365,
    },
    mixed: {
      freshness_bias: 'medium',
      source_mix: 'mixed',
      contradiction_scan: true,
      result_budget: 6,
      staleness_ttl_minutes: 720,
      prefer_recent_window_days: 14,
    },
  };
  const defaults = defaultsByMode[noteMode] || defaultsByMode.background_brief;
  return {
    note_mode: noteMode,
    freshness_bias: defaults.freshness_bias,
    source_mix: defaults.source_mix,
    contradiction_scan: !!defaults.contradiction_scan,
    result_budget: defaults.result_budget,
    staleness_ttl_minutes: defaults.staleness_ttl_minutes,
    prefer_recent_window_days: defaults.prefer_recent_window_days,
    analysis_source: 'fallback',
    schema_version: NOTE_POLICY_SCHEMA_VERSION,
  };
}

function buildFallbackFeedSummary(article = {}) {
  const rawText = stripFeedNoise(String((article && article.raw_content_text) || (article && article.content_text) || ''));
  const title = normalizeWhitespace(String((article && (article.display_title || article.title || article.crawler_title)) || ''));
  const summary = firstSentences(rawText || title, 3, 420);
  const excerpt = firstSentences(summary || rawText || title, 2, 220);
  const topics = [];
  const lower = `${title} ${rawText}`.toLowerCase();
  if (/\b(election|congress|parliament|white house|senate|policy|politic)\b/.test(lower)) topics.push('politics');
  if (/\b(war|conflict|global|international|country|diplomacy|world)\b/.test(lower)) topics.push('world');
  if (/\b(market|economy|business|finance|bank|stocks|trade|company)\b/.test(lower)) topics.push('econ');
  if (/\b(ai|software|chip|technology|tech|internet|cyber|device)\b/.test(lower)) topics.push('tech');
  return {
    summary,
    excerpt,
    entities: detectNamedEntities(`${title} ${rawText}`, 6),
    topics: coerceTopicList(topics, ['other']),
    content_quality: rawText.length < 80 ? 'fragmented' : (/\b(cookie|subscribe|sign up|javascript)\b/i.test(rawText) ? 'noisy' : 'clean'),
    analysis_source: 'fallback',
    schema_version: FEED_SUMMARY_SCHEMA_VERSION,
  };
}

class BundledSmallLlmRuntime {
  constructor(options = {}) {
    this.projectRoot = String(options.projectRoot || process.cwd());
    this.manifest = null;
    this.lastPolicyError = '';
    this.lastFeedError = '';
    this.activeTasks = new Map();
  }

  _manifestPath() {
    return path.join(this.projectRoot, 'runtime', 'models', MANIFEST_FILENAME);
  }

  _loadManifest() {
    if (!this.manifest) {
      this.manifest = readJsonSafe(this._manifestPath()) || {
        bundled: true,
        backend: 'heuristic',
        model_id: 'subgrapher-policy-fallback-v1',
        tasks: ['note_policy_classification', 'rss_article_cleanup_summary'],
        schema_version: 1,
      };
    }
    return this.manifest;
  }

  diagnostics() {
    const manifest = this._loadManifest();
    const activeTasks = Array.from(this.activeTasks.values());
    return {
      ok: true,
      bundled: !!manifest.bundled,
      available: true,
      backend: String(manifest.backend || 'heuristic'),
      model_id: String(manifest.model_id || 'subgrapher-policy-fallback-v1'),
      tasks: Array.isArray(manifest.tasks) ? manifest.tasks.slice() : [],
      manifest_path: this._manifestPath(),
      active_count: activeTasks.length,
      active_tasks: activeTasks,
      last_policy_error: String(this.lastPolicyError || ''),
      last_feed_error: String(this.lastFeedError || ''),
    };
  }

  _beginTask(taskType = '', meta = {}) {
    const taskId = hashText(`${taskType}:${nowTs()}:${Math.random()}`);
    this.activeTasks.set(taskId, {
      task_id: taskId,
      task_type: String(taskType || '').trim(),
      started_at: nowTs(),
      label: String(meta.label || '').trim(),
    });
    return taskId;
  }

  _finishTask(taskId = '') {
    this.activeTasks.delete(String(taskId || '').trim());
  }

  async classifyNotePolicy(note = {}) {
    const taskId = this._beginTask('note_policy_classification', {
      label: String((note && note.title) || 'note').trim(),
    });
    try {
      const manifest = this._loadManifest();
      const policy = buildFallbackNotePolicy(note);
      return {
        ok: true,
        ...policy,
        model_id: String(manifest.model_id || 'subgrapher-policy-fallback-v1'),
        classified_at: nowTs(),
      };
    } catch (err) {
      this.lastPolicyError = String((err && err.message) || 'note_policy_failed');
      const policy = buildFallbackNotePolicy(note);
      return {
        ok: true,
        ...policy,
        classified_at: nowTs(),
        error: this.lastPolicyError,
      };
    } finally {
      this._finishTask(taskId);
    }
  }

  async summarizeFeedArticle(article = {}) {
    const taskId = this._beginTask('rss_article_cleanup_summary', {
      label: String((article && article.title) || (article && article.url) || 'article').trim(),
    });
    try {
      const manifest = this._loadManifest();
      const payload = buildFallbackFeedSummary(article);
      return {
        ok: true,
        ...payload,
        model_id: String(manifest.model_id || 'subgrapher-policy-fallback-v1'),
        generated_at: nowTs(),
        content_hash: hashText(String((article && article.raw_content_text) || (article && article.content_text) || '')),
      };
    } catch (err) {
      this.lastFeedError = String((err && err.message) || 'feed_summary_failed');
      const payload = buildFallbackFeedSummary(article);
      return {
        ok: true,
        ...payload,
        generated_at: nowTs(),
        error: this.lastFeedError,
      };
    } finally {
      this._finishTask(taskId);
    }
  }
}

function createBundledSmallLlmRuntime(options = {}) {
  return new BundledSmallLlmRuntime(options);
}

module.exports = {
  createBundledSmallLlmRuntime,
  buildFallbackNotePolicy,
  buildFallbackFeedSummary,
};
