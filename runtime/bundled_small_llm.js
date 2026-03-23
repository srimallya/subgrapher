const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn } = require('child_process');

const MANIFEST_FILENAME = 'policy_manifest.json';
const ASSET_MANIFEST_FILENAME = 'runtime-manifest.json';
const NOTE_POLICY_SCHEMA_VERSION = 1;
const FEED_SUMMARY_SCHEMA_VERSION = 1;
const DEFAULT_TIMEOUT_MS = 60_000;
const DEFAULT_CTX_SIZE = 2048;
const DEFAULT_SEED = 7;

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

function listUnique(items = []) {
  const out = [];
  const seen = new Set();
  for (const item of Array.isArray(items) ? items : []) {
    const normalized = normalizeWhitespace(item);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

function pickPlatformValue(value, platform = process.platform) {
  if (typeof value === 'string') return normalizeWhitespace(value);
  if (!value || typeof value !== 'object') return '';
  return normalizeWhitespace(value[platform] || value.default || '');
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

function splitSentences(text = '') {
  return String(text || '')
    .split(/(?<=[.!?])\s+/)
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
}

function getMeaningfulSentences(text = '', limit = 8) {
  return splitSentences(text)
    .filter((sentence) => /[a-z]{4,}/i.test(sentence) && sentence.replace(/[^a-z]/gi, '').length >= 12)
    .slice(0, limit);
}

function stripFeedNoise(text = '') {
  return normalizeWhitespace(String(text || '')
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1')
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/\b(cookie policy|privacy policy|subscribe to continue reading|read more|sign up for|all rights reserved|javascript is required|enable javascript|share this article|newsletter)\b/gi, ' ')
    .replace(/(^|[\s])([.][\s]*){2,}/g, ' ')
    .replace(/\s+\./g, '.')
    .replace(/^\.+/, ' ')
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

function truncateText(value = '', maxChars = 8000) {
  return String(value || '').slice(0, Math.max(0, Number(maxChars) || 0));
}

function extractFirstJsonObject(text = '') {
  const src = String(text || '');
  const start = src.indexOf('{');
  if (start < 0) throw new Error('bundled llm output did not contain JSON');
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < src.length; i += 1) {
    const ch = src[i];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === '{') depth += 1;
    if (ch === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(start, i + 1);
    }
  }
  throw new Error('bundled llm returned incomplete JSON');
}

function buildTaskPrompt(taskType = '', payload = {}) {
  if (taskType === 'note_policy_classification') {
    const title = truncateText(normalizeWhitespace(payload.title || ''), 240);
    const body = truncateText(String(payload.body_markdown || ''), 6000);
    return [
      'You are Subgrapher local policy router.',
      'Read the note and output one JSON object only.',
      'No prose. No markdown. No code fences.',
      'Choose exactly one note_mode from: live_update, background_brief, historical_summary, analysis_opinion, mixed.',
      'Choose exactly one freshness_bias from: low, medium, high.',
      'Choose exactly one source_mix from: latest_news, official_sources, reference_background, mixed.',
      'Choose contradiction_scan as true or false.',
      'Set result_budget between 3 and 10.',
      'Set staleness_ttl_minutes between 30 and 20160.',
      'Set prefer_recent_window_days between 1 and 3650.',
      'Favor live_update with high freshness when the note says today, latest, current, ongoing, or as of a recent date.',
      'Output schema:',
      '{"note_mode":"","freshness_bias":"","source_mix":"","contradiction_scan":true,"result_budget":5,"staleness_ttl_minutes":1440,"prefer_recent_window_days":14}',
      '',
      `Title: ${title || '[untitled]'}`,
      'Body:',
      body || '[empty]',
      '',
      'JSON:',
    ].join('\n');
  }
  const title = truncateText(normalizeWhitespace(payload.title || ''), 240);
  const cleaned = stripFeedNoise(String(payload.raw_content_text || payload.content_text || ''));
  const raw = truncateText(cleaned || String(payload.raw_content_text || payload.content_text || ''), 9000);
  const sourceName = truncateText(normalizeWhitespace(payload.source_name || ''), 160);
  const url = truncateText(normalizeWhitespace(payload.url || ''), 400);
  return [
    'You are Subgrapher local feed cleanup summarizer.',
    'Read the fetched article text and output one JSON object only.',
    'No prose. No markdown. No code fences.',
    'Remove scraper noise, nav text, subscribe prompts, repeated headlines, and legal boilerplate.',
    'Write the gist of the story in 5 to 10 factual sentences.',
    'Do not include subscribe prompts, cookie banners, nav labels, social prompts, or website chrome.',
    'Write a short excerpt under 220 characters.',
    'Return entities as a short list of names.',
    'Return topics as a short list of lowercase tags.',
    'Choose content_quality from: clean, noisy, fragmented.',
    'Output schema:',
    '{"summary":"","excerpt":"","entities":[],"topics":[],"content_quality":"clean"}',
    '',
    `Title: ${title || '[untitled]'}`,
    `Source: ${sourceName || '[unknown]'}`,
    `URL: ${url || '[unknown]'}`,
    'Raw article text:',
    raw || '[empty]',
    '',
    'JSON:',
  ].join('\n');
}

function getTaskJsonSchema(taskType = '') {
  if (taskType === 'note_policy_classification') {
    return JSON.stringify({
      type: 'object',
      additionalProperties: false,
      required: ['note_mode', 'freshness_bias', 'source_mix', 'contradiction_scan', 'result_budget', 'staleness_ttl_minutes', 'prefer_recent_window_days'],
      properties: {
        note_mode: { type: 'string', enum: ['live_update', 'background_brief', 'historical_summary', 'analysis_opinion', 'mixed'] },
        freshness_bias: { type: 'string', enum: ['low', 'medium', 'high'] },
        source_mix: { type: 'string', enum: ['latest_news', 'official_sources', 'reference_background', 'mixed'] },
        contradiction_scan: { type: 'boolean' },
        result_budget: { type: 'integer', minimum: 3, maximum: 10 },
        staleness_ttl_minutes: { type: 'integer', minimum: 30, maximum: 20160 },
        prefer_recent_window_days: { type: 'integer', minimum: 1, maximum: 3650 },
      },
    });
  }
  return JSON.stringify({
    type: 'object',
    additionalProperties: false,
    required: ['summary', 'excerpt', 'entities', 'topics', 'content_quality'],
    properties: {
      summary: { type: 'string', minLength: 120, maxLength: 2200 },
      excerpt: { type: 'string', minLength: 30, maxLength: 220 },
      entities: {
        type: 'array',
        maxItems: 12,
        items: { type: 'string', minLength: 1, maxLength: 120 },
      },
      topics: {
        type: 'array',
        maxItems: 8,
        items: { type: 'string', minLength: 1, maxLength: 40 },
      },
      content_quality: { type: 'string', enum: ['clean', 'noisy', 'fragmented'] },
    },
  });
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
    analysis_detail: 'fallback_heuristic',
    fallback_reason: '',
    schema_version: NOTE_POLICY_SCHEMA_VERSION,
  };
}

function buildFallbackFeedSummary(article = {}) {
  const rawText = stripFeedNoise(String((article && article.raw_content_text) || (article && article.content_text) || ''));
  const title = normalizeWhitespace(String((article && (article.display_title || article.title || article.crawler_title)) || ''));
  const summary = normalizeWhitespace(getMeaningfulSentences(rawText || title, 8).join(' ') || firstSentences(rawText || title, 8, 1600));
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
    analysis_detail: 'fallback_heuristic',
    fallback_reason: '',
    schema_version: FEED_SUMMARY_SCHEMA_VERSION,
  };
}

class BundledSmallLlmRuntime {
  constructor(options = {}) {
    this.projectRoot = path.resolve(String(options.projectRoot || process.cwd()));
    this.app = options.app || null;
    this.bundledRootDir = options.bundledRootDir ? path.resolve(String(options.bundledRootDir)) : '';
    this.manifest = null;
    this.lastPolicyError = '';
    this.lastFeedError = '';
    this.activeTasks = new Map();
  }

  _isPackaged() {
    return !!(this.app && this.app.isPackaged);
  }

  _bundledRootDir() {
    if (this.bundledRootDir) return this.bundledRootDir;
    if (process.env.SUBGRAPHER_BUNDLED_LLM_ROOT) return String(process.env.SUBGRAPHER_BUNDLED_LLM_ROOT);
    if (this._isPackaged()) return path.join(process.resourcesPath, 'llm');
    return path.join(this.projectRoot, 'build', 'bundled-llm', 'current');
  }

  _manifestPath() {
    return path.join(this.projectRoot, 'runtime', 'models', MANIFEST_FILENAME);
  }

  _assetManifestPath() {
    return path.join(this._bundledRootDir(), ASSET_MANIFEST_FILENAME);
  }

  _defaultManifest() {
    return {
      bundled: true,
      backend: 'llama.cpp-cli',
      model_id: 'qwen3.5-0.8b-q8_0',
      model_name: 'Qwen3.5 0.8B Q8_0',
      tasks: ['note_policy_classification', 'rss_article_cleanup_summary'],
      schema_version: 1,
      prompt_versions: {
        note_policy_classification: NOTE_POLICY_SCHEMA_VERSION,
        rss_article_cleanup_summary: FEED_SUMMARY_SCHEMA_VERSION,
      },
      executable_rel_path: process.platform === 'win32'
        ? 'engine/llama/llama-cli.exe'
        : 'engine/llama/llama-cli',
      model_rel_path: 'models/Qwen3.5-0.8B-Q8_0.gguf',
      timeout_ms: DEFAULT_TIMEOUT_MS,
      ctx_size: DEFAULT_CTX_SIZE,
      seed: DEFAULT_SEED,
    };
  }

  _loadManifest() {
    if (!this.manifest) {
      const projectManifest = readJsonSafe(this._manifestPath()) || {};
      const assetManifest = readJsonSafe(this._assetManifestPath()) || {};
      this.manifest = {
        ...this._defaultManifest(),
        ...projectManifest,
        ...assetManifest,
        prompt_versions: {
          ...(this._defaultManifest().prompt_versions || {}),
          ...((projectManifest && projectManifest.prompt_versions) || {}),
          ...((assetManifest && assetManifest.prompt_versions) || {}),
        },
      };
    }
    return this.manifest;
  }

  _resolveExecutablePath(manifest = {}) {
    const envPath = normalizeWhitespace(process.env.SUBGRAPHER_BUNDLED_LLM_BIN || '');
    if (envPath) return envPath;
    const relPath = pickPlatformValue(manifest.executable_rel_path, process.platform);
    if (!relPath) return '';
    return path.join(this._bundledRootDir(), relPath);
  }

  _resolveModelPath(manifest = {}) {
    const envPath = normalizeWhitespace(process.env.SUBGRAPHER_BUNDLED_LLM_MODEL || '');
    if (envPath) return envPath;
    const relPath = pickPlatformValue(manifest.model_rel_path, process.platform);
    if (!relPath) return '';
    return path.join(this._bundledRootDir(), relPath);
  }

  _resolveBundledAvailability(manifest = {}) {
    const bundledRoot = this._bundledRootDir();
    const assetManifestPath = this._assetManifestPath();
    const executablePath = this._resolveExecutablePath(manifest);
    const modelPath = this._resolveModelPath(manifest);
    const errors = [];
    if (!fs.existsSync(bundledRoot)) {
      errors.push(`bundled llm resources missing at ${bundledRoot}`);
    }
    if (!fs.existsSync(assetManifestPath)) {
      errors.push(`runtime manifest missing at ${assetManifestPath}`);
    }
    if (!executablePath || !fs.existsSync(executablePath)) {
      errors.push(`runtime binary missing at ${executablePath || '[unset]'}`);
    }
    if (!modelPath || !fs.existsSync(modelPath)) {
      errors.push(`model file missing at ${modelPath || '[unset]'}`);
    }
    return {
      available: errors.length === 0,
      reason: errors.join(' | '),
      bundled_root: bundledRoot,
      asset_manifest_path: assetManifestPath,
      executable_path: executablePath,
      model_path: modelPath,
    };
  }

  diagnostics() {
    const manifest = this._loadManifest();
    const availability = this._resolveBundledAvailability(manifest);
    const activeTasks = Array.from(this.activeTasks.values());
    return {
      ok: true,
      bundled: !!manifest.bundled,
      available: availability.available,
      backend: String(manifest.backend || 'bundled-cli'),
      model_id: String(manifest.model_id || ''),
      model_name: String(manifest.model_name || manifest.model_id || ''),
      tasks: Array.isArray(manifest.tasks) ? manifest.tasks.slice() : [],
      manifest_path: this._manifestPath(),
      asset_manifest_path: availability.asset_manifest_path,
      bundled_root: availability.bundled_root,
      executable_path: availability.executable_path,
      model_path: availability.model_path,
      unavailable_reason: availability.available ? '' : availability.reason,
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

  _validateNotePolicyPayload(payload = {}) {
    const src = (payload && typeof payload === 'object') ? payload : {};
    const noteMode = String(src.note_mode || '').trim();
    const freshnessBias = String(src.freshness_bias || '').trim();
    const sourceMix = String(src.source_mix || '').trim();
    if (!['live_update', 'background_brief', 'historical_summary', 'analysis_opinion', 'mixed'].includes(noteMode)) {
      throw new Error(`invalid note_mode: ${noteMode || '[empty]'}`);
    }
    if (!['low', 'medium', 'high'].includes(freshnessBias)) {
      throw new Error(`invalid freshness_bias: ${freshnessBias || '[empty]'}`);
    }
    if (!['latest_news', 'official_sources', 'reference_background', 'mixed'].includes(sourceMix)) {
      throw new Error(`invalid source_mix: ${sourceMix || '[empty]'}`);
    }
    return {
      note_mode: noteMode,
      freshness_bias: freshnessBias,
      source_mix: sourceMix,
      contradiction_scan: src.contradiction_scan !== false,
      result_budget: clampNumber(src.result_budget, 3, 10, 5),
      staleness_ttl_minutes: clampNumber(src.staleness_ttl_minutes, 30, 60 * 24 * 14, 1440),
      prefer_recent_window_days: clampNumber(src.prefer_recent_window_days, 1, 3650, 14),
      schema_version: NOTE_POLICY_SCHEMA_VERSION,
    };
  }

  _validateFeedSummaryPayload(payload = {}) {
    const src = (payload && typeof payload === 'object') ? payload : {};
    const summary = normalizeWhitespace(src.summary || '');
    const excerpt = normalizeWhitespace(src.excerpt || '');
    if (!summary) throw new Error('summary is required');
    const sentenceCount = getMeaningfulSentences(summary, 12).length;
    if (sentenceCount < 5 || sentenceCount > 10) {
      throw new Error(`summary must contain 5 to 10 sentences, got ${sentenceCount}`);
    }
    return {
      summary: summary.slice(0, 2200),
      excerpt: (excerpt || firstSentences(summary, 2, 220)).slice(0, 320),
      entities: Array.isArray(src.entities)
        ? src.entities.map((item) => normalizeWhitespace(item)).filter(Boolean).slice(0, 12)
        : [],
      topics: coerceTopicList(src.topics, ['other']),
      content_quality: ['clean', 'noisy', 'fragmented'].includes(String(src.content_quality || '').trim())
        ? String(src.content_quality || '').trim()
        : 'clean',
      schema_version: FEED_SUMMARY_SCHEMA_VERSION,
    };
  }

  _runBundledTask(taskType = '', payload = {}) {
    const manifest = this._loadManifest();
    const availability = this._resolveBundledAvailability(manifest);
    if (!availability.available) {
      const error = new Error(availability.reason || 'bundled llm unavailable');
      error.code = 'BUNDLED_LLM_UNAVAILABLE';
      throw error;
    }
    const timeoutMs = clampNumber(manifest.timeout_ms, 1_000, 180_000, DEFAULT_TIMEOUT_MS);
    const schemaVersion = taskType === 'note_policy_classification'
      ? NOTE_POLICY_SCHEMA_VERSION
      : FEED_SUMMARY_SCHEMA_VERSION;
    const promptVersion = Number((manifest.prompt_versions && manifest.prompt_versions[taskType]) || schemaVersion) || schemaVersion;
    const backend = String(manifest.backend || 'llama.cpp-cli').trim() || 'llama.cpp-cli';
    return new Promise((resolve, reject) => {
      const args = backend === 'llama.cpp-cli'
        ? [
          '-m',
          availability.model_path,
          '-no-cnv',
          '-st',
          '--reasoning',
          'off',
          '--log-disable',
          '--no-perf',
          '--simple-io',
          '--no-display-prompt',
          '--json-schema',
          getTaskJsonSchema(taskType),
          '--ctx-size',
          String(clampNumber(manifest.ctx_size, 1024, 32768, DEFAULT_CTX_SIZE)),
          '--seed',
          String(clampNumber(manifest.seed, 0, 2147483647, DEFAULT_SEED)),
          '--temp',
          '0',
          '--top-p',
          '0.1',
          '-n',
          String(taskType === 'note_policy_classification' ? 96 : 220),
          '-p',
          buildTaskPrompt(taskType, payload),
        ]
        : [
          '--task',
          String(taskType || '').trim(),
          '--schema-version',
          String(schemaVersion),
          '--prompt-version',
          String(promptVersion),
          '--model',
          availability.model_path,
        ];
      const child = spawn(availability.executable_path, args, {
        cwd: path.dirname(availability.executable_path) || availability.bundled_root,
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      let stdout = '';
      let stderr = '';
      let settled = false;
      const timeout = setTimeout(() => {
        if (settled) return;
        settled = true;
        child.kill('SIGKILL');
        reject(new Error(`bundled llm timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      child.stdout.on('data', (chunk) => {
        stdout += String(chunk || '');
      });
      child.stderr.on('data', (chunk) => {
        stderr += String(chunk || '');
      });
      child.on('error', (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        reject(err);
      });
      child.on('close', (code) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        if (code !== 0) {
          reject(new Error(`bundled llm exited with code ${code}: ${normalizeWhitespace(stderr).slice(0, 400)}`));
          return;
        }
        try {
          const parsed = JSON.parse(backend === 'llama.cpp-cli' ? extractFirstJsonObject(stdout) : stdout);
          resolve({
            payload: parsed,
            stderr: normalizeWhitespace(stderr),
            prompt_version: promptVersion,
          });
        } catch (err) {
          reject(new Error(`bundled llm returned invalid JSON: ${err.message}`));
        }
      });
      if (backend === 'llama.cpp-cli') {
        child.stdin.end();
      } else {
        child.stdin.write(JSON.stringify(payload));
        child.stdin.end();
      }
    });
  }

  async classifyNotePolicy(note = {}) {
    const taskId = this._beginTask('note_policy_classification', {
      label: String((note && note.title) || 'note').trim(),
    });
    try {
      const manifest = this._loadManifest();
      const result = await this._runBundledTask('note_policy_classification', {
        id: String((note && note.id) || '').trim(),
        title: String((note && note.title) || ''),
        body_markdown: String((note && note.body_markdown) || ''),
      });
      const policy = this._validateNotePolicyPayload(result.payload);
      this.lastPolicyError = '';
      return {
        ok: true,
        ...policy,
        analysis_source: 'llm',
        analysis_detail: 'bundled_llm',
        fallback_reason: '',
        model_id: String(manifest.model_id || ''),
        model_name: String(manifest.model_name || manifest.model_id || ''),
        prompt_version: Number(result.prompt_version || NOTE_POLICY_SCHEMA_VERSION) || NOTE_POLICY_SCHEMA_VERSION,
        classified_at: nowTs(),
      };
    } catch (err) {
      const reason = String((err && err.message) || 'note_policy_failed');
      this.lastPolicyError = reason;
      const unavailable = err && err.code === 'BUNDLED_LLM_UNAVAILABLE';
      const policy = buildFallbackNotePolicy(note);
      return {
        ok: true,
        ...policy,
        model_id: String(this._loadManifest().model_id || ''),
        model_name: String(this._loadManifest().model_name || this._loadManifest().model_id || ''),
        classified_at: nowTs(),
        error: reason,
        analysis_detail: unavailable ? 'bundled_llm_unavailable' : 'bundled_llm_error',
        fallback_reason: reason,
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
      const result = await this._runBundledTask('rss_article_cleanup_summary', {
        id: String((article && article.id) || '').trim(),
        title: String((article && article.title) || ''),
        raw_content_text: String((article && article.raw_content_text) || (article && article.content_text) || ''),
        source_name: String((article && article.source_name) || ''),
        source_domain: String((article && article.source_domain) || ''),
        url: String((article && article.url) || ''),
      });
      const payload = this._validateFeedSummaryPayload(result.payload);
      this.lastFeedError = '';
      return {
        ok: true,
        ...payload,
        analysis_source: 'llm',
        analysis_detail: 'bundled_llm',
        fallback_reason: '',
        model_id: String(manifest.model_id || ''),
        model_name: String(manifest.model_name || manifest.model_id || ''),
        prompt_version: Number(result.prompt_version || FEED_SUMMARY_SCHEMA_VERSION) || FEED_SUMMARY_SCHEMA_VERSION,
        generated_at: nowTs(),
        content_hash: hashText(String((article && article.raw_content_text) || (article && article.content_text) || '')),
      };
    } catch (err) {
      const reason = String((err && err.message) || 'feed_summary_failed');
      this.lastFeedError = reason;
      const unavailable = err && err.code === 'BUNDLED_LLM_UNAVAILABLE';
      const payload = buildFallbackFeedSummary(article);
      return {
        ok: true,
        ...payload,
        generated_at: nowTs(),
        error: reason,
        analysis_detail: unavailable ? 'bundled_llm_unavailable' : 'bundled_llm_error',
        fallback_reason: reason,
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
