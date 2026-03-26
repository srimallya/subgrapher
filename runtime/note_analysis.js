const crypto = require('crypto');
const { hashEmbedText } = require('./embedding_runtime');

const ANALYZER_VERSION = 'note-analyzer-v8';
const NOTE_EVIDENCE_MODE = 'web_only';
const DAY_MS = 24 * 60 * 60 * 1000;
const HIGHLIGHT_THRESHOLD = 0.72;
const PASSAGE_CHUNK_SIZE = 480;
const PASSAGE_OVERLAP = 80;
const MAX_PASSAGES_PER_SOURCE = 8;
const MAX_WEB_RESULTS_PER_CLAIM = 3;
const MAX_FETCHES_PER_NOTE = 18;
const MAX_CONCURRENCY = 2;
const MAX_RESOLUTION_CANDIDATES = 8;
const VERB_PATTERN = /\b(is|are|was|were|has|have|had|began|begin|beginning|started|start|launched|released|acquired|bought|won|lost|grew|fell|sued|announced|built|uses|use|caused|causes|reported|reports|showed|shows|said|says|fight|fights|fighting|attack|attacks|attacked|attacking|invade|invades|invaded|invading|bombed|bombing|strike|strikes|struck|broke|break|broken|wounded|wound|redirected|redirect|deployed|deploy|sent|send|targeted|target|carried|carry|carrying|kill|kills|killed)\b/i;
const VERB_PATTERN_GLOBAL = new RegExp(VERB_PATTERN.source, 'ig');
const WEB_NOTE_SOURCE_KINDS = new Set([
  'explicit_url',
  'web_search',
  'official_search',
  'challenge_search',
  'rss_search',
  'rss_official_search',
  'rss_challenge_search',
]);
const QUERY_STOPWORDS = new Set(['the', 'and', 'for', 'with', 'this', 'that', 'from', 'into', 'their', 'there', 'about', 'according', 'over', 'than']);
const GENERIC_SUBJECT_PATTERN = /^(it|its|they|their|them|this|that|these|those|the company|company|the business|business|the startup|startup|the firm|firm|the platform|platform|the app|app|the product|product|the tool|tool)\b/i;
const GENERIC_QUERY_PATTERN = /\b(the company|company|the business|business|the startup|startup|the firm|firm|the platform|platform|the app|app|the product|product|the tool|tool)\b/i;
const CONTRADICTION_PATTERNS = [
  /\b(no official record|no evidence|not true|false|falsely|fabricated|debunked|denied|denies|deny|did not|didn't|not describe|never happened|unconfirmed|rumor)\b/i,
  /\b(but|however|while)\b.{0,60}\b(not|never|denied|did not|didn't|no)\b/i,
];
const HIGH_AUTHORITY_PATTERNS = [
  /(^|\/\/)(www\.)?(reuters|apnews|ap|axios|bbc|nytimes|washingtonpost|wsj|ft)\./i,
];
const BOILERPLATE_PASSAGE_PATTERNS = [
  /\bplease click here if the page does not redirect automatically\b/i,
  /\bredirect automatically\b/i,
  /\benable javascript\b/i,
  /\bsign in to continue\b/i,
  /\bsubscribe to continue reading\b/i,
  /\bcookie policy\b/i,
];
const LANGUAGE_LEARNING_PATTERNS = [
  /用法/,
  /意思/,
  /\bmeaning\b/i,
  /\bdefinition\b/i,
  /\btranslate(?:d|s|ing|ion)?\b/i,
  /\bgrammar\b/i,
  /\bexample sentence\b/i,
  /\bhow to use\b/i,
  /\bconjugat(?:e|ed|ion)\b/i,
];

const searchCache = new Map();
const rssSearchCache = new Map();
const fetchCache = new Map();
const DEFAULT_NOTE_POLICY = {
  note_mode: 'background_brief',
  freshness_bias: 'medium',
  source_mix: 'mixed',
  contradiction_scan: true,
  result_budget: 5,
  staleness_ttl_minutes: 1440,
  prefer_recent_window_days: 14,
  analysis_source: 'fallback',
  schema_version: 1,
};

function nowTs() {
  return Date.now();
}

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function isBoilerplatePassage(text = '') {
  const normalized = normalizeWhitespace(text);
  if (!normalized) return true;
  return BOILERPLATE_PASSAGE_PATTERNS.some((pattern) => pattern.test(normalized));
}

function escapeRegExp(value = '') {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeUrl(raw = '') {
  const input = String(raw || '').trim();
  if (!input) return '';
  try {
    return new URL(input).toString();
  } catch (_) {
    return '';
  }
}

function normalizeClaimText(text = '') {
  return normalizeWhitespace(String(text || '').toLowerCase().replace(/https?:\/\/[^\s]+/g, ' ').replace(/[^a-z0-9\s]/g, ' '));
}

function countMatches(text = '', pattern) {
  const matches = String(text || '').match(pattern);
  return Array.isArray(matches) ? matches.length : 0;
}

function latinLetterRatio(text = '') {
  const source = String(text || '');
  const latinCount = countMatches(source, /[A-Za-z]/g);
  const letterCount = countMatches(source, /\p{L}/gu);
  if (letterCount <= 0) return 0;
  return latinCount / letterCount;
}

function cjkCharacterRatio(text = '') {
  const source = String(text || '');
  const cjkCount = countMatches(source, /[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]/g);
  const visibleCount = countMatches(source, /[^\s]/g);
  if (visibleCount <= 0) return 0;
  return cjkCount / visibleCount;
}

function getEffectiveClaimText(claim = {}) {
  return normalizeWhitespace(String((claim && claim.resolved_claim_text) || (claim && claim.claim_text) || ''));
}

function getEffectiveClaimSubject(claim = {}) {
  return normalizeWhitespace(String((claim && claim.resolved_subject_text) || (claim && claim.subject_text) || ''));
}

function clampUnit(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(1, numeric));
}

function tokenize(text = '') {
  return normalizeClaimText(text).split(' ').filter(Boolean);
}

function tokenCoverageScore(terms = [], text = '') {
  const required = Array.from(new Set((Array.isArray(terms) ? terms : []).map((item) => normalizeWhitespace(item).toLowerCase()).filter((item) => item && item.length >= 2)));
  if (required.length === 0) return 0;
  const haystack = new Set(tokenize(text));
  let matched = 0;
  required.forEach((term) => {
    if (haystack.has(term)) matched += 1;
  });
  return matched / required.length;
}

function cosineSimilarity(vecA = [], vecB = []) {
  if (!vecA || !vecB || vecA.length !== vecB.length) return 0;
  let dot = 0;
  for (let i = 0; i < vecA.length; i += 1) {
    dot += Number(vecA[i] || 0) * Number(vecB[i] || 0);
  }
  return Math.max(0, Math.min(1, dot));
}

function yearTokens(text = '') {
  const matches = String(text || '').match(/\b(19|20)\d{2}\b/g);
  return Array.isArray(matches) ? matches.map((item) => String(item)) : [];
}

function extractNamedEntityTerms(text = '') {
  const raw = String(text || '');
  const out = [];
  const matches = raw.match(/\b(?:[A-Z][a-z]*[A-Z][A-Za-z0-9-]*|[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}|[A-Z]{2,}|[A-Z][a-z]+-[A-Z0-9]+)\b/g);
  (Array.isArray(matches) ? matches : []).forEach((item) => {
    const normalized = normalizeWhitespace(item);
    if (normalized && normalized.length >= 2) out.push(normalized);
  });
  return Array.from(new Set(out));
}

function namedEntityCoverageScore(claimText = '', passageText = '') {
  const terms = extractNamedEntityTerms(claimText);
  if (terms.length === 0) return 0.5;
  const lowerPassage = String(passageText || '').toLowerCase();
  const hits = terms.filter((term) => lowerPassage.includes(term.toLowerCase())).length;
  return hits / terms.length;
}

function contradictionCueScore(claimText = '', passageText = '') {
  const text = normalizeWhitespace(`${claimText} ${passageText}`);
  if (!text) return 0;
  let score = 0;
  CONTRADICTION_PATTERNS.forEach((pattern) => {
    if (pattern.test(text)) score += 0.34;
  });
  if (/\b(not|no|never|denied|false|rumor)\b/i.test(String(passageText || ''))) score += 0.22;
  if (/\b(every|all|always|never|only)\b/i.test(String(claimText || '')) && /\b(in stages|staged|rather than|not every|not all|partial|phased|rollout happened in stages)\b/i.test(String(passageText || ''))) {
    score += 0.48;
  }
  return clampUnit(score);
}

function exactnessScore(claimText = '', passageText = '') {
  const normalizedClaim = normalizeClaimText(claimText);
  const normalizedPassage = normalizeClaimText(passageText);
  if (!normalizedClaim || !normalizedPassage) return 0;
  if (normalizedPassage.includes(normalizedClaim)) return 1;
  const claimTerms = tokenize(claimText).filter((item) => item.length >= 4);
  const phrase = claimTerms.slice(0, 6).join(' ');
  if (phrase && normalizedPassage.includes(phrase)) return 0.72;
  return 0;
}

function lexicalOverlapScore(claimText = '', passageText = '') {
  const claimTokens = new Set(tokenize(claimText).filter((item) => item.length >= 3));
  if (claimTokens.size === 0) return 0;
  const passageTokens = new Set(tokenize(passageText).filter((item) => item.length >= 3));
  let matchCount = 0;
  claimTokens.forEach((token) => {
    if (passageTokens.has(token)) matchCount += 1;
  });
  return matchCount / claimTokens.size;
}

function timeMatchScore(claimText = '', passageText = '') {
  const claimYears = yearTokens(claimText);
  if (claimYears.length === 0) return 0.5;
  const passageYears = new Set(yearTokens(passageText));
  const matches = claimYears.filter((item) => passageYears.has(item)).length;
  return matches > 0 ? 1 : 0;
}

function splitPassageSentences(text = '') {
  return String(text || '')
    .split(/(?<=[.!?])\s+/)
    .map((item) => normalizeWhitespace(item))
    .filter(Boolean);
}

function buildCitationExcerpt(claimText = '', passageText = '', maxChars = 320) {
  const passage = normalizeWhitespace(passageText);
  if (!passage) return '';
  const sentences = splitPassageSentences(passage);
  if (!sentences.length) return passage.slice(0, maxChars);
  const ranked = sentences
    .map((sentence, index) => ({
      sentence,
      index,
      score: (
        (0.52 * lexicalOverlapScore(claimText, sentence))
        + (0.22 * exactnessScore(claimText, sentence))
        + (0.12 * timeMatchScore(claimText, sentence))
        + (0.14 * namedEntityCoverageScore(claimText, sentence))
      ),
    }))
    .sort((a, b) => (b.score - a.score) || (a.index - b.index));
  const best = ranked[0] || null;
  if (!best) return passage.slice(0, maxChars);
  const window = [best.sentence];
  const nextSentence = sentences[best.index + 1];
  if (nextSentence && (window.join(' ') + ' ' + nextSentence).length <= maxChars) {
    window.push(nextSentence);
  }
  return normalizeWhitespace(window.join(' ')).slice(0, maxChars);
}

function chunkText(text = '', size = PASSAGE_CHUNK_SIZE, overlap = PASSAGE_OVERLAP) {
  const raw = String(text || '').trim();
  if (!raw) return [];
  const out = [];
  let start = 0;
  while (start < raw.length && out.length < MAX_PASSAGES_PER_SOURCE) {
    const end = Math.min(raw.length, start + size);
    const chunk = raw.slice(start, end).trim();
    if (chunk && !isBoilerplatePassage(chunk)) out.push({ text: chunk, start, end });
    if (end >= raw.length) break;
    start = Math.max(start + 1, end - overlap);
  }
  return out;
}

function sourceAuthorityScore(source = {}, claim = {}) {
  const url = String((source && source.url) || '').trim();
  const title = String((source && source.title) || '').trim();
  const sourceKind = String((source && source.source_kind) || '').trim();
  if (sourceKind === 'official_search' || sourceKind === 'explicit_url') return 0.95;
  if (HIGH_AUTHORITY_PATTERNS.some((pattern) => pattern.test(url))) return 0.92;
  if (/\b(reuters|associated press|ap news|axios|bbc|new york times|washington post|wall street journal|financial times)\b/i.test(title)) return 0.9;
  if (isConflictClaim(getEffectiveClaimText(claim))) return 0.4;
  return 0.3;
}

function sourceFreshnessScore(source = {}, claim = {}, policy = DEFAULT_NOTE_POLICY) {
  const ts = Number((source && source.published_at) || (source && source.fetched_at) || 0);
  if (!ts) return 0.2;
  const ageDays = Math.max(0, (nowTs() - ts) / DAY_MS);
  const normalizedPolicy = normalizeNotePolicy(policy);
  const recentWindow = Math.max(1, Number(normalizedPolicy.prefer_recent_window_days || 14) || 14);
  const effectiveClaimText = getEffectiveClaimText(claim);
  const currentEvent = isConflictClaim(effectiveClaimText)
    || /\b(now|today|currently|latest|ongoing)\b/i.test(effectiveClaimText);
  const policySensitive = isFreshnessSensitivePolicy(normalizedPolicy);
  if (!currentEvent && !policySensitive) return ageDays <= 365 ? 0.45 : 0.25;
  if (ageDays <= Math.max(1, recentWindow / 3)) return 1;
  if (ageDays <= recentWindow) return 0.88;
  if (ageDays <= Math.max(30, recentWindow * 2)) return 0.62;
  return 0.24;
}

function buildHash(value = '') {
  return crypto.createHash('sha1').update(String(value || ''), 'utf8').digest('hex').slice(0, 16);
}

function normalizeNotePolicy(input = {}) {
  const src = (input && typeof input === 'object') ? input : {};
  const noteMode = String(src.note_mode || DEFAULT_NOTE_POLICY.note_mode).trim() || DEFAULT_NOTE_POLICY.note_mode;
  const freshnessBias = String(src.freshness_bias || DEFAULT_NOTE_POLICY.freshness_bias).trim() || DEFAULT_NOTE_POLICY.freshness_bias;
  const sourceMix = String(src.source_mix || DEFAULT_NOTE_POLICY.source_mix).trim() || DEFAULT_NOTE_POLICY.source_mix;
  return {
    note_mode: ['live_update', 'background_brief', 'historical_summary', 'analysis_opinion', 'mixed'].includes(noteMode) ? noteMode : DEFAULT_NOTE_POLICY.note_mode,
    freshness_bias: ['low', 'medium', 'high'].includes(freshnessBias) ? freshnessBias : DEFAULT_NOTE_POLICY.freshness_bias,
    source_mix: ['latest_news', 'official_sources', 'reference_background', 'mixed'].includes(sourceMix) ? sourceMix : DEFAULT_NOTE_POLICY.source_mix,
    contradiction_scan: src.contradiction_scan !== false,
    result_budget: Math.max(3, Math.min(10, Number(src.result_budget || DEFAULT_NOTE_POLICY.result_budget) || DEFAULT_NOTE_POLICY.result_budget)),
    staleness_ttl_minutes: Math.max(30, Math.min(60 * 24 * 14, Number(src.staleness_ttl_minutes || DEFAULT_NOTE_POLICY.staleness_ttl_minutes) || DEFAULT_NOTE_POLICY.staleness_ttl_minutes)),
    prefer_recent_window_days: Math.max(1, Math.min(3650, Number(src.prefer_recent_window_days || DEFAULT_NOTE_POLICY.prefer_recent_window_days) || DEFAULT_NOTE_POLICY.prefer_recent_window_days)),
    analysis_source: String(src.analysis_source || DEFAULT_NOTE_POLICY.analysis_source).trim() || DEFAULT_NOTE_POLICY.analysis_source,
    analysis_detail: String(src.analysis_detail || '').trim(),
    fallback_reason: String(src.fallback_reason || '').trim(),
    schema_version: Number(src.schema_version || DEFAULT_NOTE_POLICY.schema_version) || DEFAULT_NOTE_POLICY.schema_version,
    classified_at: Number(src.classified_at || 0) || 0,
    model_id: String(src.model_id || '').trim(),
    model_name: String(src.model_name || '').trim(),
    prompt_version: Number(src.prompt_version || 0) || 0,
  };
}

function computeNextRefreshAt(completedAt = 0, policy = {}) {
  const finished = Number(completedAt || 0) || 0;
  if (!finished) return 0;
  const ttlMinutes = Math.max(1, Number((policy && policy.staleness_ttl_minutes) || DEFAULT_NOTE_POLICY.staleness_ttl_minutes) || DEFAULT_NOTE_POLICY.staleness_ttl_minutes);
  return finished + (ttlMinutes * 60 * 1000);
}

function isFreshnessSensitivePolicy(policy = {}) {
  const normalized = normalizeNotePolicy(policy);
  return normalized.note_mode === 'live_update' || normalized.freshness_bias === 'high';
}

function scorePolicyFreshness(policy = {}) {
  const normalized = normalizeNotePolicy(policy);
  if (normalized.freshness_bias === 'high') return 1;
  if (normalized.freshness_bias === 'low') return 0.3;
  return 0.65;
}

function dedupeBy(list = [], keyFn = (item) => item) {
  const out = [];
  const seen = new Set();
  list.forEach((item) => {
    const key = String(keyFn(item) || '');
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push(item);
  });
  return out;
}

function extractExplicitUrls(markdown = '') {
  const text = String(markdown || '');
  const re = /https?:\/\/[^\s<>)\]]+/g;
  const out = [];
  let match = re.exec(text);
  while (match) {
    const raw = String(match[0] || '').trim().replace(/[),.;:!?]+$/, '');
    const normalized = normalizeUrl(raw);
    if (normalized) {
      out.push({
        url: raw,
        canonical_url: normalized,
        start_offset: match.index,
        end_offset: match.index + raw.length,
      });
    }
    match = re.exec(text);
  }
  return dedupeBy(out, (item) => item.canonical_url);
}

function isMarkdownHeadingLine(line = '') {
  const text = String(line || '').trim();
  return !!text && (
    /^\s{0,3}#{1,6}\s+/.test(text)
    || /^\*\*[^*]+\*\*$/.test(text)
    || /^__[^_]+__$/.test(text)
  );
}

function isMarkdownListLine(line = '') {
  return /^(\s*)([-+*]|\d+\.)\s+/.test(String(line || ''));
}

function isMarkdownSeparatorLine(line = '') {
  return /^\s{0,3}([-*_])(?:\s*\1){2,}\s*$/.test(String(line || ''));
}

function stripMarkdownListMarker(line = '') {
  const raw = String(line || '');
  const match = raw.match(/^(\s*)([-+*]|\d+\.)\s+(.*)$/);
  if (!match) {
    return {
      text: raw,
      contentOffset: 0,
    };
  }
  const content = String(match[3] || '');
  const contentOffset = raw.indexOf(content);
  return {
    text: content,
    contentOffset: Math.max(0, contentOffset),
  };
}

function splitClaims(markdown = '') {
  const text = String(markdown || '');
  const out = [];
  let offset = 0;
  text.split('\n').forEach((lineRaw, lineIndex) => {
    const line = String(lineRaw || '');
    const listLine = isMarkdownListLine(line);
    const normalizedLine = listLine ? stripMarkdownListMarker(line) : { text: line, contentOffset: 0 };
    const contentLine = String(normalizedLine.text || '');
    const trimmed = contentLine.trim();
    if (!trimmed || isMarkdownHeadingLine(line) || isMarkdownSeparatorLine(line)) {
      offset += line.length + 1;
      return;
    }
    const re = /[^.!?]+[.!?]?/g;
    let match = re.exec(contentLine);
    while (match) {
      const chunk = String(match[0] || '');
      const sentence = chunk.trim();
      if (sentence) {
        const startTrim = chunk.indexOf(sentence);
        const start = offset + Number(normalizedLine.contentOffset || 0) + match.index + Math.max(0, startTrim);
        splitCompoundClaimSegments(sentence, start).forEach((segment) => out.push({
          ...segment,
          line_index: lineIndex,
          line_text: trimmed,
          is_list_line: listLine,
        }));
      }
      match = re.exec(contentLine);
    }
    offset += line.length + 1;
  });
  return out;
}

function classifyClaimType(claim = {}) {
  const claimText = normalizeWhitespace(String((claim && claim.claim_text) || ''));
  const lower = claimText.toLowerCase();
  if (!claimText) return 'normative_opinion';
  if (/\b(i think|in my view|should|must|ought|better|worse|best|worst|need to)\b/i.test(lower)) return 'normative_opinion';
  if (/\b(thesis|paradigm|fundamental departure|represents|points the way|demonstrates the viability|significant cognitive benefits)\b/i.test(lower)) return 'broad_interpretation';
  if (/\baccording to\b/i.test(lower)) return 'attribution';
  if (/\b(19|20)\d{2}\b/.test(claimText)) return 'date_time';
  if (/\b\d+([.,]\d+)?(%| percent| people| troops| marines| million| billion| thousand)?\b/i.test(claimText)) return 'numeric_stat';
  if (/\b(is down|is up|grew|fell|increase|decrease|dominance|trend|already in the region)\b/i.test(lower)) return 'state_trend';
  if (/\b(started|began|launched|released|announced|killed|attacked|redirected|deployed|wounded|retaliated)\b/i.test(lower)) return 'event';
  return 'state_trend';
}

function getClaimTypeWeight(claimType = '') {
  const type = String(claimType || '').trim();
  if (['numeric_stat', 'date_time', 'event'].includes(type)) return 1.3;
  if (['state_trend', 'attribution'].includes(type)) return 1.0;
  if (type === 'broad_interpretation') return 0.5;
  return 0;
}

function isHardScoredClaimType(claimType = '') {
  return getClaimTypeWeight(claimType) > 0;
}

function buildNoteAggregate(claims = []) {
  const items = (Array.isArray(claims) ? claims : []).filter(Boolean);
  const weighted = items.filter((claim) => getClaimTypeWeight(claim.claim_type) > 0);
  if (weighted.length === 0) {
    return {
      note_score: 0,
      coverage_score: 0,
      risk_level: 'needs_review',
    };
  }
  const totalWeight = weighted.reduce((sum, claim) => sum + getClaimTypeWeight(claim.claim_type), 0) || 1;
  const weightedMean = weighted.reduce((sum, claim) => sum + ((Number(claim.claim_reliability || 0) || 0) * getClaimTypeWeight(claim.claim_type)), 0) / totalWeight;
  const coverageScore = weighted.reduce((sum, claim) => {
    const weight = getClaimTypeWeight(claim.claim_type);
    const coverage = Math.max(Number(claim.support_confidence || 0) || 0, Number(claim.contradict_confidence || 0) || 0);
    return sum + (coverage * weight);
  }, 0) / totalWeight;
  const corroborationScore = weighted.reduce((sum, claim) => sum + ((Number(claim.corroboration || 0) || 0) * getClaimTypeWeight(claim.claim_type)), 0) / totalWeight;
  const contradictionPenalty = weighted.reduce((sum, claim) => {
    const weight = getClaimTypeWeight(claim.claim_type);
    const severity = String(claim.status || '') === 'contradicted' ? 1 : 0.45;
    return sum + ((Number(claim.contradict_confidence || 0) || 0) * severity * weight);
  }, 0) / totalWeight;
  const supportedCount = weighted.filter((claim) => ['supported', 'mostly_supported'].includes(String(claim.status || ''))).length;
  const densityBonus = Math.tanh(supportedCount / 12) * 0.1;
  const stability = Math.min(1, Math.log2(1 + weighted.length) / 4);
  const rawScore = ((0.72 * weightedMean)
    + (0.12 * coverageScore)
    + (0.06 * corroborationScore)
    + densityBonus
    - (0.28 * contradictionPenalty)) * (0.78 + (0.22 * stability));
  const noteScore = Math.round(clampUnit(rawScore) * 100);
  const highRisk = weighted.some((claim) => {
    const weight = getClaimTypeWeight(claim.claim_type);
    const support = Number(claim.support_confidence || 0) || 0;
    const contradict = Number(claim.contradict_confidence || 0) || 0;
    return weight >= 1
      && contradict >= 0.72
      && contradict >= support + 0.08;
  });
  const weakOrContradicted = weighted.filter((claim) => ['contradicted', 'weak_evidence', 'mixed'].includes(String(claim.status || ''))).length / Math.max(1, weighted.length);
  return {
    note_score: noteScore,
    coverage_score: Number(coverageScore.toFixed(4)),
    risk_level: highRisk ? 'high_contradiction_risk' : (weakOrContradicted >= 0.28 || noteScore < 72 ? 'needs_review' : 'clean'),
  };
}

function splitCompoundClaimSegments(sentenceText = '', startOffset = 0) {
  const text = String(sentenceText || '').trim();
  if (!text) return [];
  const segments = [];

  function pushSegment(segmentText = '', segmentStart = 0) {
    const trimmed = String(segmentText || '').trim();
    if (!trimmed) return;
    const localIndex = String(segmentText || '').indexOf(trimmed);
    const start = segmentStart + Math.max(0, localIndex);
    segments.push({
      start_offset: start,
      end_offset: start + trimmed.length,
      claim_text: trimmed,
    });
  }

  function walk(segmentText = '', segmentStart = 0) {
    const raw = String(segmentText || '');
    const matches = Array.from(raw.matchAll(/\s+\band\b\s+/ig));
    for (let i = 0; i < matches.length; i += 1) {
      const match = matches[i];
      const boundary = Number(match.index || 0);
      const leftRaw = raw.slice(0, boundary);
      const rightRaw = raw.slice(boundary + String(match[0] || '').length);
      const left = leftRaw.trim();
      const right = rightRaw.trim();
      if (!left || !right) continue;
      const leftLooksClaimLike = VERB_PATTERN.test(left);
      const rightLooksClaimLike = VERB_PATTERN.test(right);
      if (!leftLooksClaimLike || !rightLooksClaimLike) continue;
      const leftStart = segmentStart + leftRaw.indexOf(left);
      const rightStart = segmentStart + boundary + String(match[0] || '').length + rightRaw.indexOf(right);
      walk(left, leftStart);
      walk(right, rightStart);
      return;
    }
    pushSegment(raw, segmentStart);
  }

  walk(text, Number(startOffset) || 0);
  const derived = [];
  const whenMatch = text.match(/\bwhen\s+(.+)$/i);
  if (whenMatch && VERB_PATTERN.test(String(whenMatch[1] || ''))) {
    const local = text.toLowerCase().indexOf(String(whenMatch[1] || '').toLowerCase());
    if (local >= 0) {
      derived.push({
        start_offset: Number(startOffset) + local,
        end_offset: Number(startOffset) + local + String(whenMatch[1] || '').trim().length,
        claim_text: normalizeWhitespace(String(whenMatch[1] || '')),
      });
    }
  }
  const butMatch = text.match(/\bbut\s+(.+)$/i);
  if (butMatch && VERB_PATTERN.test(String(butMatch[1] || ''))) {
    const local = text.toLowerCase().indexOf(String(butMatch[1] || '').toLowerCase());
    if (local >= 0) {
      derived.push({
        start_offset: Number(startOffset) + local,
        end_offset: Number(startOffset) + local + String(butMatch[1] || '').trim().length,
        claim_text: normalizeWhitespace(String(butMatch[1] || '')),
      });
    }
  }
  return dedupeBy(segments.concat(derived), (item) => `${item.start_offset}:${item.end_offset}:${normalizeClaimText(item.claim_text)}`);
}

function parseClaimStructure(claimText = '') {
  const normalized = normalizeWhitespace(claimText);
  VERB_PATTERN_GLOBAL.lastIndex = 0;
  const matches = Array.from(normalized.matchAll(VERB_PATTERN_GLOBAL));
  const verbMatch = matches
    .map((match) => {
      const predicateText = String(match[0] || '').trim().toLowerCase();
      let weight = 0.5;
      if (/\b(kill|killed|kills|attack|attacked|attacking|strike|strikes|struck|bombed|acquired|released|launched|announced|sued)\b/i.test(predicateText)) weight = 1;
      else if (/\b(began|begin|started|start|reported|reports|said|says|showed|shows|is|are|was|were)\b/i.test(predicateText)) weight = 0.2;
      return {
        value: match,
        weight,
        index: Number(match.index || 0),
      };
    })
    .sort((a, b) => (b.weight - a.weight) || (b.index - a.index))[0];
  if (!verbMatch) {
    return {
      subject_text: normalized,
      predicate_text: '',
      object_text: '',
    };
  }
  const predicateText = String((verbMatch.value && verbMatch.value[0]) || '').trim();
  const idx = Number((verbMatch.value && verbMatch.value.index) || 0);
  return {
    subject_text: normalizeWhitespace(normalized.slice(0, idx)),
    predicate_text: predicateText,
    object_text: normalizeWhitespace(normalized.slice(idx + predicateText.length)),
  };
}

function deriveClaimAnchor(claim = {}) {
  const subject = getEffectiveClaimSubject(claim);
  const predicate = normalizeWhitespace(String((claim && claim.predicate_text) || '')).toLowerCase();
  const object = normalizeWhitespace(String((claim && claim.object_text) || ''));
  if (!subject) return '';
  if ((predicate === 'released' || predicate === 'launched' || predicate === 'announced') && object) {
    return normalizeWhitespace(`${subject} ${object}`);
  }
  return subject;
}

function primaryNamedEntity(text = '') {
  const terms = extractNamedEntityTerms(text);
  return String(terms[0] || '').trim();
}

function isGenericSubject(subject = '') {
  return GENERIC_SUBJECT_PATTERN.test(normalizeWhitespace(subject));
}

function genericSubjectSuffix(subject = '') {
  return normalizeWhitespace(String(subject || '').replace(GENERIC_SUBJECT_PATTERN, ''));
}

function getClaimAnchorText(claim = {}) {
  return primaryNamedEntity(String((claim && claim.subject_text) || ''))
    || primaryNamedEntity(String((claim && claim.claim_text) || ''))
    || normalizeWhitespace(String((claim && claim.subject_text) || ''));
}

function claimNeedsEntityResolution(claim = {}) {
  const baseSubject = normalizeWhitespace(String((claim && claim.base_subject_text) || ''));
  const subject = getEffectiveClaimSubject(claim);
  const claimText = getEffectiveClaimText(claim);
  if (baseSubject && isGenericSubject(baseSubject)) return true;
  if (!subject) return true;
  if (isGenericSubject(subject)) return true;
  if (!extractNamedEntityTerms(subject).length && GENERIC_QUERY_PATTERN.test(claimText)) return true;
  return false;
}

function resolveClaimContext(claims = []) {
  let anchor = '';
  return (Array.isArray(claims) ? claims : []).map((claim) => {
    const next = { ...claim };
    const subject = normalizeWhitespace(String(next.subject_text || ''));
    if (isGenericSubject(subject) && anchor) {
      const suffix = genericSubjectSuffix(subject);
      next.subject_text = normalizeWhitespace(`${anchor} ${suffix}`);
    }
    const derivedAnchor = getClaimAnchorText(next) || deriveClaimAnchor(next);
    if (/\b([A-Z][a-z]{1,}|[A-Z]{2,}|GPT-\d+)\b/.test(derivedAnchor)) {
      anchor = derivedAnchor;
    }
    return next;
  });
}

function detectFactualClaim(claimText = '') {
  const text = normalizeWhitespace(claimText);
  if (!text) return { factuality: 'non_factual', modality: 'statement' };
  if (/[?]$/.test(text)) return { factuality: 'non_factual', modality: 'question' };
  const lower = text.toLowerCase();
  if (/\b(i think|maybe|perhaps|could|should|might|probably)\b/.test(lower)) {
    return { factuality: 'speculative', modality: 'speculative' };
  }
  const hasYear = /\b(19|20)\d{2}\b/.test(text);
  const hasNumber = /\b\d+([.,]\d+)?\b/.test(text);
  const hasVerb = VERB_PATTERN.test(text);
  const hasEntity = /\b(?:[A-Z][a-z]{2,}|[A-Z]{2,}|GPT-\d+)\b/.test(text);
  const hasCurrentCue = /\b(now|today|currently|ongoing|current|latest)\b/.test(lower);
  const hasConflictCue = /\b(war|conflict|fight|fighting|attack|attacking|invasion|invade|invading|missile|airstrike|ceasefire|troops|military)\b/.test(lower);
  const concreteTerms = tokenize(text).filter((item) => item.length >= 3 && !QUERY_STOPWORDS.has(item));
  const hasConcreteTopic = concreteTerms.length >= 2;
  const claimType = classifyClaimType({ claim_text: text });
  if (!isHardScoredClaimType(claimType)) {
    return { factuality: 'non_factual', modality: 'statement' };
  }
  if (hasVerb && (hasYear || hasNumber || hasEntity || hasConflictCue || (hasCurrentCue && hasConcreteTopic))) {
    return { factuality: 'factual', modality: 'statement' };
  }
  return { factuality: 'non_factual', modality: 'statement' };
}

async function runPool(items = [], worker, concurrency = MAX_CONCURRENCY) {
  const out = [];
  let cursor = 0;
  async function runOne() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      out[index] = await worker(items[index], index);
    }
  }
  const jobs = Array.from({ length: Math.max(1, Math.min(concurrency, items.length || 1)) }, () => runOne());
  await Promise.all(jobs);
  return out;
}

function buildClaimQuery(claim = {}) {
  const parts = [
    getEffectiveClaimSubject(claim),
    String((claim && claim.predicate_text) || ''),
    String((claim && claim.object_text) || ''),
    String((claim && claim.time_text) || ''),
  ].map((item) => normalizeWhitespace(item)).filter(Boolean);
  return parts.join(' ');
}

function isWebNoteSourceKind(sourceKind = '') {
  return WEB_NOTE_SOURCE_KINDS.has(String(sourceKind || '').trim());
}

function isRssNoteSourceKind(sourceKind = '') {
  return ['rss_search', 'rss_official_search', 'rss_challenge_search'].includes(String(sourceKind || '').trim());
}

function getSearchIntentForSourceKind(sourceKind = '') {
  const kind = String(sourceKind || '').trim();
  if (kind === 'official_search' || kind === 'rss_official_search') return 'official';
  if (kind === 'challenge_search' || kind === 'rss_challenge_search') return 'challenge';
  return 'support';
}

function getRssSourceKindForIntent(intent = '') {
  const normalized = String(intent || '').trim();
  if (normalized === 'official') return 'rss_official_search';
  if (normalized === 'challenge') return 'rss_challenge_search';
  return 'rss_search';
}

function isReleasePredicate(predicate = '') {
  return ['released', 'launched', 'announced'].includes(String(predicate || '').trim().toLowerCase());
}

function isComparisonClaim(claimText = '') {
  return /\b(faster|slower|better|worse|more|less|grew|fell|compare|comparison)\b/i.test(String(claimText || ''));
}

function isConflictClaim(claimText = '') {
  return /\b(war|conflict|fight|fighting|attack|attacking|invasion|invade|invading|missile|airstrike|ceasefire|troops|military)\b/i.test(String(claimText || ''));
}

function buildCoverageTerms(text = '') {
  return tokenize(text).filter((item) => item.length >= 3 && !QUERY_STOPWORDS.has(item));
}

function buildPredicateTerms(claim = {}) {
  const predicate = String((claim && claim.predicate_text) || '').trim().toLowerCase();
  const claimText = getEffectiveClaimText(claim);
  if (isReleasePredicate(predicate)) return ['release', 'released', 'launch', 'launched', 'announcement'];
  if (isComparisonClaim(claimText)) return ['adoption', 'growth', 'metric', 'compare', 'comparison', 'usage'];
  if (isConflictClaim(claimText)) return ['war', 'conflict', 'fighting', 'attack', 'military'];
  return buildCoverageTerms(predicate);
}

function buildHeadlineStyleQuery(claim = {}) {
  const effectiveText = getEffectiveClaimText(claim);
  const entities = extractNamedEntityTerms(effectiveText).slice(0, 4);
  const tokens = buildCoverageTerms(effectiveText).slice(0, 10);
  return normalizeWhitespace(entities.concat(tokens).join(' '));
}

function scoreSearchCandidate(claim = {}, plan = {}, result = {}) {
  const title = String((result && result.title) || '');
  const snippet = String((result && result.snippet) || '');
  const combined = normalizeWhitespace(`${title} ${snippet}`);
  const effectiveClaimText = getEffectiveClaimText(claim);
  if (!combined) {
    return {
      query_confidence: 0,
      semantic_score: 0,
      lexical_score: 0,
      subject_coverage: 0,
      object_coverage: 0,
      predicate_coverage: 0,
      time_score: 0,
    };
  }
  const semanticScore = cosineSimilarity(hashEmbedText(effectiveClaimText), hashEmbedText(combined));
  const lexicalScore = lexicalOverlapScore(effectiveClaimText, combined);
  const subjectCoverage = tokenCoverageScore(buildCoverageTerms(getEffectiveClaimSubject(claim)), combined);
  const objectCoverage = tokenCoverageScore(buildCoverageTerms(String((claim && claim.object_text) || '')), combined);
  const predicateCoverage = tokenCoverageScore(buildPredicateTerms(claim), combined);
  const timeScore = timeMatchScore(effectiveClaimText, combined);
  const entityCoverage = namedEntityCoverageScore(effectiveClaimText, combined);
  const queryConfidence = Math.max(0, Math.min(1,
    (0.22 * semanticScore)
    + (0.16 * lexicalScore)
    + (0.2 * subjectCoverage)
    + (0.12 * objectCoverage)
    + (0.1 * predicateCoverage)
    + (0.1 * timeScore)
    + (0.1 * entityCoverage)
  ));
  return {
    query_confidence: queryConfidence,
    semantic_score: semanticScore,
    lexical_score: lexicalScore,
    subject_coverage: subjectCoverage,
    object_coverage: objectCoverage,
    predicate_coverage: predicateCoverage,
    time_score: timeScore,
    entity_coverage: entityCoverage,
    source_kind: String((plan && plan.source_kind) || 'web_search'),
    search_intent: String((plan && plan.intent) || 'support'),
  };
}

function shouldAcceptSearchCandidate(scores = {}, plan = {}) {
  const minConfidence = Number((plan && plan.min_query_confidence) || 0.34);
  return Number(scores.query_confidence || 0) >= minConfidence
    || (
      Number(scores.subject_coverage || 0) >= 0.8
      && Number(scores.object_coverage || 0) >= 0.45
      && Number(scores.semantic_score || 0) >= 0.18
    );
}

function shouldRejectSearchCandidate(claim = {}, result = {}, scores = {}) {
  const claimText = getEffectiveClaimText(claim);
  const title = String((result && result.title) || '');
  const snippet = String((result && result.snippet) || '');
  const combined = normalizeWhitespace(`${title} ${snippet}`);
  if (!combined) return false;
  const claimIsLatin = latinLetterRatio(claimText) >= 0.7;
  const nonLatinHeavy = cjkCharacterRatio(combined) >= 0.08 || latinLetterRatio(combined) < 0.45;
  const weakEntityCoverage = Number(scores.entity_coverage || 0) < 0.45;
  const weakSubjectCoverage = Number(scores.subject_coverage || 0) < 0.45;
  const weakObjectCoverage = Number(scores.object_coverage || 0) < 0.35;
  if (claimIsLatin && nonLatinHeavy && weakEntityCoverage && weakSubjectCoverage && weakObjectCoverage) {
    return true;
  }
  const languageLearningNoise = LANGUAGE_LEARNING_PATTERNS.some((pattern) => pattern.test(combined));
  if (languageLearningNoise && weakEntityCoverage && weakSubjectCoverage) {
    return true;
  }
  return false;
}

function hasSatisfiedSearchIntent(bucket = {}, plan = {}) {
  const items = Array.isArray(bucket.items) ? bucket.items : [];
  const top = items[0] || null;
  const intent = String((plan && plan.intent) || 'support');
  if (intent === 'official') {
    return !!(top && Number(top.query_confidence || 0) >= 0.58);
  }
  if (intent === 'challenge') {
    return !!(top && Number(top.query_confidence || 0) >= 0.52);
  }
  return !!(
    (top && Number(top.query_confidence || 0) >= 0.68)
    || items.filter((item) => Number(item.query_confidence || 0) >= 0.52).length >= 2
  );
}

function buildClaimSearchPlans(claim = {}) {
  const baseQuery = buildClaimQuery(claim);
  const subject = getEffectiveClaimSubject(claim);
  const predicate = String((claim && claim.predicate_text) || '').trim().toLowerCase();
  const object = String((claim && claim.object_text) || '').trim();
  const time = String((claim && claim.time_text) || '').trim();
  const claimText = getEffectiveClaimText(claim);
  const anchor = deriveClaimAnchor(claim);
  const plans = [];
  if (claimNeedsEntityResolution(claim) && !String((claim && claim.resolved_subject_text) || '').trim()) {
    return [];
  }
  function pushPlan(query, sourceKind, intent, minQueryConfidence = 0.34) {
    const normalized = normalizeWhitespace(query);
    if (!normalized) return;
    plans.push({
      query: normalized,
      source_kind: sourceKind,
      intent,
      min_query_confidence: minQueryConfidence,
    });
  }
  if (subject || object) {
    pushPlan(`${subject} ${object} official announcement ${time}`, 'official_search', 'official', 0.36);
    pushPlan(`${subject} ${object} official blog ${time}`, 'official_search', 'official', 0.34);
    pushPlan(`${subject} ${object} denied ${time}`, 'challenge_search', 'challenge', 0.3);
    pushPlan(`${subject} ${predicate} ${object} false`, 'challenge_search', 'challenge', 0.28);
    pushPlan(`${subject} ${object} rumor ${time}`, 'challenge_search', 'challenge', 0.28);
  }
  if (baseQuery) {
    pushPlan(baseQuery, 'web_search', 'support', 0.34);
    pushPlan(claimText, 'web_search', 'support', 0.32);
    pushPlan(buildHeadlineStyleQuery(claim), 'web_search', 'support', 0.3);
    pushPlan(`${subject} ${object} ${time}`, 'web_search', 'support', 0.32);
  }
  if (anchor && time) {
    pushPlan(`${anchor} ${time} timeline`, 'web_search', 'support', 0.32);
  }
  if (isReleasePredicate(predicate)) {
    pushPlan(`${subject} ${object} release date official`, 'official_search', 'official', 0.36);
    pushPlan(`${subject} ${object} release date ${time}`, 'web_search', 'support', 0.34);
    pushPlan(`${subject} ${object} launch announcement`, 'web_search', 'support', 0.32);
    pushPlan(`${subject} ${object} not released ${time}`, 'challenge_search', 'challenge', 0.3);
    pushPlan(`${subject} ${object} rumor ${time}`, 'challenge_search', 'challenge', 0.28);
  }
  if (isComparisonClaim(claimText)) {
    pushPlan(`${baseQuery} metric source`, 'web_search', 'support', 0.3);
    pushPlan(`${subject} ${object} adoption data`, 'web_search', 'support', 0.3);
    pushPlan(`${subject} ${object} compared with data`, 'challenge_search', 'challenge', 0.3);
    pushPlan(`${subject} ${object} criticism benchmark`, 'challenge_search', 'challenge', 0.28);
  }
  if (isConflictClaim(claimText)) {
    pushPlan(`${subject} ${object} latest`, 'web_search', 'support', 0.3);
    pushPlan(`${subject} conflict latest`, 'web_search', 'support', 0.3);
    pushPlan(`${subject} official statement conflict`, 'official_search', 'official', 0.28);
    pushPlan(`${subject} denied conflict`, 'challenge_search', 'challenge', 0.28);
  }
  return dedupeBy(plans.filter((plan) => String(plan.query || '').trim()), (plan) => `${plan.source_kind}:${normalizeClaimText(plan.query)}`);
}

function claimSemanticSimilarity(left = {}, right = {}) {
  return cosineSimilarity(
    hashEmbedText(getEffectiveClaimText(left)),
    hashEmbedText(getEffectiveClaimText(right))
  );
}

function buildClaimClusters(claims = []) {
  const ordered = (Array.isArray(claims) ? claims : []).map((claim) => ({ ...claim })).sort((a, b) => Number(a.claim_index || 0) - Number(b.claim_index || 0));
  let currentClusterId = -1;
  let currentAnchors = new Set();
  let previous = null;
  return ordered.map((claim) => {
    const entities = new Set(extractNamedEntityTerms(String(claim.subject_text || '')).concat(extractNamedEntityTerms(String(claim.claim_text || ''))));
    let nextCluster = currentClusterId;
    if (!previous) {
      nextCluster = 0;
      currentAnchors = new Set(entities);
    } else {
      const lineGap = Math.abs(Number(claim.line_index || 0) - Number(previous.line_index || 0));
      const similarity = claimSemanticSimilarity(previous, claim);
      const explicitEntityShift = entities.size > 0
        && currentAnchors.size > 0
        && !Array.from(entities).some((item) => currentAnchors.has(item));
      if (lineGap > 2 || explicitEntityShift || (entities.size > 0 && similarity < 0.2)) {
        nextCluster += 1;
        currentAnchors = new Set(entities);
      } else {
        entities.forEach((item) => currentAnchors.add(item));
      }
    }
    previous = claim;
    currentClusterId = nextCluster;
    return {
      ...claim,
      cluster_id: nextCluster,
    };
  });
}

function rankAnchorCandidates(target = {}, claims = []) {
  const targetClaim = target || {};
  const sourceClaims = Array.isArray(claims) ? claims : [];
  const candidates = [];
  sourceClaims.forEach((candidate) => {
    if (!candidate || String(candidate.id || '') === String(targetClaim.id || '')) return;
    const anchor = getClaimAnchorText(candidate);
    if (!anchor) return;
    const claimIndexGap = Math.abs(Number(candidate.claim_index || 0) - Number(targetClaim.claim_index || 0));
    if (claimIndexGap > 6) return;
    const sameCluster = Number(candidate.cluster_id || -1) === Number(targetClaim.cluster_id || -2);
    const semanticScore = claimSemanticSimilarity(targetClaim, candidate);
    const priorBoost = Number(candidate.claim_index || 0) < Number(targetClaim.claim_index || 0) ? 0.16 : 0.04;
    const entityBoost = extractNamedEntityTerms(String(candidate.claim_text || '')).length > 0 ? 0.2 : 0;
    const clusterBoost = sameCluster ? 0.28 : 0;
    const distancePenalty = Math.min(0.22, claimIndexGap * 0.04);
    const score = clusterBoost + priorBoost + entityBoost + (0.42 * semanticScore) - distancePenalty;
    candidates.push({
      anchor,
      score,
      claim_id: String(candidate.id || '').trim(),
      line_index: Number(candidate.line_index || 0),
      semantic_score: semanticScore,
    });
  });
  return dedupeBy(
    candidates.sort((a, b) => Number(b.score || 0) - Number(a.score || 0)),
    (item) => String(item.anchor || '').toLowerCase()
  ).slice(0, MAX_RESOLUTION_CANDIDATES);
}

function applyHeuristicClaimResolution(claims = [], contextClaims = []) {
  const rankingPool = Array.isArray(contextClaims) && contextClaims.length ? contextClaims : claims;
  return (Array.isArray(claims) ? claims : []).map((claim) => {
    const next = { ...claim };
    const subject = normalizeWhitespace(String(next.subject_text || ''));
    const candidates = rankAnchorCandidates(next, rankingPool);
    next.anchor_candidates = candidates;
    next.source_line_indexes = [Number(next.line_index || 0)];
    next.parser_provenance = 'deterministic';
    next.resolution_confidence = 0;
    next.resolution_ambiguous = false;
    next.resolved_subject_text = '';
    next.resolved_claim_text = '';
    if (!claimNeedsEntityResolution(next)) {
      next.resolved_subject_text = subject;
      next.resolved_claim_text = normalizeWhitespace(`${subject} ${String(next.predicate_text || '')} ${String(next.object_text || '')}`);
      next.parser_provenance = 'deterministic';
      next.resolution_confidence = 1;
      return next;
    }
    const best = candidates[0] || null;
    if (best && Number(best.score || 0) >= 0.3) {
      const suffix = genericSubjectSuffix(subject);
      next.resolved_subject_text = normalizeWhitespace(`${best.anchor} ${suffix}`) || best.anchor;
      next.resolved_claim_text = normalizeWhitespace(`${next.resolved_subject_text} ${String(next.predicate_text || '')} ${String(next.object_text || '')}`);
      next.parser_provenance = 'reconciled';
      next.resolution_confidence = clampUnit(0.35 + (Number(best.score || 0) * 0.5));
      next.source_line_indexes = dedupeBy([Number(next.line_index || 0), Number(best.line_index || 0)], (item) => String(item));
      return next;
    }
    next.resolution_ambiguous = true;
    return next;
  });
}

function buildResolutionRequests(claims = []) {
  return (Array.isArray(claims) ? claims : [])
    .filter((claim) => claimNeedsEntityResolution(claim) && !String(claim.resolved_subject_text || '').trim())
    .map((claim) => ({
      claim_id: String(claim.id || '').trim(),
      claim_index: Number(claim.claim_index || 0),
      line_index: Number(claim.line_index || 0),
      claim_text: String(claim.claim_text || ''),
      subject_text: String(claim.subject_text || ''),
      predicate_text: String(claim.predicate_text || ''),
      object_text: String(claim.object_text || ''),
      cluster_id: Number(claim.cluster_id || 0),
      anchor_candidates: Array.isArray(claim.anchor_candidates)
        ? claim.anchor_candidates.map((item) => ({
          anchor: String((item && item.anchor) || '').trim(),
          score: Number((item && item.score) || 0) || 0,
          line_index: Number((item && item.line_index) || 0) || 0,
        }))
        : [],
    }));
}

function summarizeAnalysis(claims = []) {
  return {
    claim_count: claims.length,
    supported_count: claims.filter((item) => ['supported', 'mostly_supported'].includes(item.status)).length,
    contested_count: claims.filter((item) => item.status === 'mixed').length,
    uncertain_count: claims.filter((item) => item.status === 'contradicted').length,
    no_evidence_count: claims.filter((item) => item.status === 'weak_evidence').length,
  };
}

function firstSentence(text = '') {
  const normalized = normalizeWhitespace(text);
  if (!normalized) return '';
  const match = normalized.match(/[^.!?]+[.!?]?/);
  return normalizeWhitespace(String((match && match[0]) || normalized));
}

function buildClaimExplanation(claim = {}, topSupport = null, topContradict = null) {
  const supportTitle = String((topSupport && topSupport.source && topSupport.source.title) || '').trim();
  const contradictTitle = String((topContradict && topContradict.source && topContradict.source.title) || '').trim();
  if (claim.status === 'supported') {
    return supportTitle
      ? `Multiple web sources support this wording, led by ${supportTitle}.`
      : 'Multiple web sources support this wording.';
  }
  if (claim.status === 'contradicted') {
    return contradictTitle
      ? `Current reporting contradicts this wording, led by ${contradictTitle}.`
      : 'Current reporting contradicts this wording.';
  }
  if (claim.status === 'mixed') {
    return 'Supporting and contradicting sources both match parts of this claim.';
  }
  return 'The available web evidence is too weak or incomplete to confirm this wording yet.';
}

function buildRewriteSuggestions(claim = {}, topSupport = null, topContradict = null) {
  const suggestions = [];
  const claimText = String((claim && claim.claim_text) || '').trim();
  if (!claimText) return suggestions;
  if (claim.status === 'contradicted' && topContradict) {
    const replacement = firstSentence(String((topContradict && topContradict.passage && topContradict.passage.passage_text) || (topContradict && topContradict.excerpt) || ''));
    if (replacement) {
      suggestions.push({
        key: 'correct',
        label: 'Use corrected wording',
        description: 'Replace the claim with wording grounded in the strongest contradicting source.',
        replacement,
      });
    }
  }
  if (claim.status === 'supported' && topSupport) {
    const sourceTitle = String((topSupport && topSupport.source && topSupport.source.title) || 'the cited source').trim();
    const replacement = /\baccording to\b/i.test(claimText)
      ? ''
      : `${claimText.replace(/\s+$/g, '').replace(/[.]+$/g, '')}, according to ${sourceTitle}.`;
    if (replacement && replacement.length > claimText.length + 8) {
      suggestions.push({
        key: 'attribute',
        label: 'Add attribution',
        description: 'Tighten the sentence by tying it to the strongest supporting source.',
        replacement,
      });
    }
  }
  if ((claim.status === 'mixed' || claim.status === 'weak_evidence') && topSupport) {
    const replacement = firstSentence(String((topSupport && topSupport.passage && topSupport.passage.passage_text) || (topSupport && topSupport.excerpt) || ''));
    if (replacement) {
      suggestions.push({
        key: 'narrow',
        label: 'Use narrower wording',
        description: 'Replace the claim with the strongest evidence-backed wording currently available.',
        replacement,
      });
    }
  }
  return suggestions.slice(0, 3);
}

function createNoteAnalysisEngine(options = {}) {
  const webSearch = typeof options.webSearch === 'function' ? options.webSearch : null;
  const rssSearch = typeof options.rssSearch === 'function' ? options.rssSearch : null;
  const fetchUrl = typeof options.fetchUrl === 'function' ? options.fetchUrl : null;
  const temporalGraphScorer = typeof options.temporalGraphScorer === 'function' ? options.temporalGraphScorer : null;
  const classifyNotePolicy = typeof options.classifyNotePolicy === 'function' ? options.classifyNotePolicy : null;
  const resolveClaimEntities = typeof options.resolveClaimEntities === 'function' ? options.resolveClaimEntities : null;
  const makeId = typeof options.makeId === 'function' ? options.makeId : ((prefix) => `${prefix}_${nowTs()}_${Math.random().toString(36).slice(2, 10)}`);

  async function searchWithCache(query = '', maxResults = MAX_WEB_RESULTS_PER_CLAIM) {
    const normalized = normalizeWhitespace(query);
    if (!normalized || !webSearch) return [];
    const safeMaxResults = Math.max(1, Math.min(6, Number(maxResults) || MAX_WEB_RESULTS_PER_CLAIM));
    const cacheKey = `${normalizeClaimText(normalized)}:${safeMaxResults}`;
    const cached = searchCache.get(cacheKey);
    if (cached && (nowTs() - Number(cached.ts || 0)) < DAY_MS) {
      return cached.results;
    }
    const res = await webSearch({ query: normalized, max_results: safeMaxResults });
    const results = Array.isArray(res && res.results) ? res.results : [];
    searchCache.set(cacheKey, { ts: nowTs(), results });
    return results;
  }

  async function fetchWithCache(url = '') {
    const canonical = normalizeUrl(url);
    if (!canonical || !fetchUrl) return null;
    const cached = fetchCache.get(canonical);
    if (cached && (nowTs() - Number(cached.ts || 0)) < DAY_MS) {
      return cached.preview;
    }
    const preview = await fetchUrl(canonical);
    if (preview && preview.ok) {
      fetchCache.set(canonical, { ts: nowTs(), preview });
    }
    return preview;
  }

  async function rssSearchWithCache(query = '', maxResults = MAX_WEB_RESULTS_PER_CLAIM) {
    const normalized = normalizeWhitespace(query);
    if (!normalized || !rssSearch) return [];
    const safeMaxResults = Math.max(1, Math.min(12, Number(maxResults) || MAX_WEB_RESULTS_PER_CLAIM));
    const cacheKey = `${normalizeClaimText(normalized)}:${safeMaxResults}`;
    const cached = rssSearchCache.get(cacheKey);
    if (cached && (nowTs() - Number(cached.ts || 0)) < DAY_MS) {
      return cached.results;
    }
    const res = await rssSearch({ query: normalized, max_results: safeMaxResults });
    const results = Array.isArray(res && res.results) ? res.results : [];
    rssSearchCache.set(cacheKey, { ts: nowTs(), results });
    return results;
  }

  async function analyze(note = {}, context = {}) {
    const startedAt = nowTs();
    const noteId = String((note && note.id) || '').trim();
    const noteRevision = Number((note && note.analysis_revision) || 0);
    const body = String((note && note.body_markdown) || '');
    const analysisRunId = makeId('analysis');
    const reportProgress = typeof context.onProgress === 'function' ? context.onProgress : null;
    const emitProgress = (stage = '', meta = {}) => {
      if (!reportProgress) return;
      try {
        reportProgress({
          stage: String(stage || '').trim(),
          note_id: noteId,
          ...meta,
        });
      } catch (_) {
        // ignore observer failures
      }
    };
    const policyRes = classifyNotePolicy
      ? await classifyNotePolicy({
        id: noteId,
        title: String((note && note.title) || ''),
        body_markdown: body,
      })
      : null;
    const notePolicy = normalizeNotePolicy(policyRes && policyRes.ok ? policyRes : DEFAULT_NOTE_POLICY);
    const analysisSource = String(notePolicy.analysis_source || 'fallback').trim() || 'fallback';
    const explicitUrls = extractExplicitUrls(body);
    const parsedSegments = resolveClaimContext(splitClaims(body)
      .map((item, idx) => {
        const structure = parseClaimStructure(item.claim_text);
        const factuality = detectFactualClaim(item.claim_text);
        const normalizedClaim = normalizeClaimText(item.claim_text);
        const claim = {
          id: makeId('claim'),
          note_id: noteId,
          analysis_run_id: analysisRunId,
          claim_index: idx,
          start_offset: item.start_offset,
          end_offset: item.end_offset,
          claim_text: item.claim_text,
          normalized_claim_text: normalizedClaim,
          base_subject_text: String(structure.subject_text || ''),
          line_index: Number(item.line_index || 0),
          line_text: String(item.line_text || ''),
          is_list_line: !!item.is_list_line,
          time_text: yearTokens(item.claim_text).join(' '),
          ...structure,
          claim_type: classifyClaimType({ claim_text: item.claim_text, ...structure }),
          claim_weight: 0,
          modality: factuality.modality,
          factuality: factuality.factuality,
          status: 'weak_evidence',
          top_score: 0,
          highlight_score: 0,
          truth_confidence: 0,
          support_confidence: 0,
          contradict_confidence: 0,
          corroboration: 0,
          authority: 0,
          freshness: 0,
          claim_reliability: 0,
          explanation: '',
          rewrite_suggestions: [],
        };
        claim.claim_weight = getClaimTypeWeight(claim.claim_type);
        return claim;
      }));
    const clusteredSegments = buildClaimClusters(parsedSegments);
    let claims = clusteredSegments.filter((item) => item.factuality === 'factual');
    claims = applyHeuristicClaimResolution(claims, clusteredSegments);
    const resolutionRequests = buildResolutionRequests(claims);
    if (resolutionRequests.length > 0 && resolveClaimEntities) {
      emitProgress('claim_resolution_start', {
        candidate_count: resolutionRequests.length,
        cluster_count: new Set(claims.map((item) => Number(item.cluster_id || 0))).size,
      });
      try {
        const resolutionRes = await resolveClaimEntities({
          note: {
            id: noteId,
            title: String((note && note.title) || ''),
            body_markdown: body,
          },
          claims,
          resolution_requests: resolutionRequests,
        });
        const resolvedById = new Map(
          (Array.isArray(resolutionRes && resolutionRes.claims) ? resolutionRes.claims : [])
            .map((item) => [String((item && item.claim_id) || '').trim(), item])
            .filter((entry) => entry[0])
        );
        claims = claims.map((claim) => {
          const resolved = resolvedById.get(String(claim.id || '').trim());
          if (!resolved) return claim;
          const resolvedSubject = normalizeWhitespace(String((resolved && resolved.resolved_subject_text) || ''));
          const resolvedClaimText = normalizeWhitespace(String((resolved && resolved.resolved_claim_text) || ''));
          if (!resolvedSubject || !resolvedClaimText || String((resolved && resolved.classification) || '') !== 'factual') {
            return {
              ...claim,
              resolution_ambiguous: true,
            };
          }
          return {
            ...claim,
            resolved_subject_text: resolvedSubject,
            resolved_claim_text: resolvedClaimText,
            parser_provenance: claim.parser_provenance === 'deterministic' ? 'llm_resolved' : 'reconciled',
            resolution_confidence: Math.max(Number(claim.resolution_confidence || 0) || 0, Number((resolved && resolved.confidence) || 0) || 0),
            resolution_ambiguous: !!(resolved && resolved.ambiguous),
            source_line_indexes: Array.isArray(resolved && resolved.source_line_indexes)
              ? dedupeBy((claim.source_line_indexes || []).concat(resolved.source_line_indexes.map((item) => Number(item || 0))), (item) => String(item))
              : claim.source_line_indexes,
          };
        });
        emitProgress('claim_resolution_done', {
          candidate_count: resolutionRequests.length,
          resolved_count: claims.filter((claim) => String(claim.resolved_subject_text || '').trim() && String(claim.resolved_subject_text || '').trim() !== String(claim.subject_text || '').trim()).length,
          backend: String((resolutionRes && resolutionRes.backend) || '').trim(),
          provider: String((resolutionRes && resolutionRes.provider) || '').trim(),
          model: String((resolutionRes && resolutionRes.model) || '').trim(),
        });
      } catch (err) {
        emitProgress('claim_resolution_done', {
          candidate_count: resolutionRequests.length,
          resolved_count: 0,
          skipped: true,
          error: String((err && err.message) || err || 'claim_resolution_failed').trim(),
        });
      }
    }
    emitProgress('claims_detected', {
      claim_count: claims.length,
      explicit_url_count: explicitUrls.length,
      note_mode: String(notePolicy.note_mode || '').trim(),
    });
    if (claims.length === 0) {
      const summary = summarizeAnalysis(claims);
      return {
        ok: true,
        analysis_run_id: analysisRunId,
        note_revision: noteRevision,
        extractor_version: ANALYZER_VERSION,
        evidence_mode: NOTE_EVIDENCE_MODE,
        started_at: startedAt,
        completed_at: nowTs(),
        status: 'completed',
        message: 'No factual claims detected.',
        note_policy: notePolicy,
        analysis_source: analysisSource,
        latest_evidence_at: 0,
        next_refresh_at: computeNextRefreshAt(nowTs(), notePolicy),
        claims,
        sources: [],
        passages: [],
        citations: [],
        summary,
        explicit_urls: explicitUrls,
      };
    }

    const sources = [];
    const passages = [];
    const citations = [];
    const sourceByCanonicalUrl = new Map();
    const maxResultsPerClaim = Math.max(MAX_WEB_RESULTS_PER_CLAIM, Math.min(6, Number(notePolicy.result_budget || MAX_WEB_RESULTS_PER_CLAIM) || MAX_WEB_RESULTS_PER_CLAIM));
    const maxFetchesForNote = Math.max(MAX_FETCHES_PER_NOTE, Number(notePolicy.result_budget || 0) * 4);

    function ensureSource(sourceInput = {}) {
      const sourceKind = String(sourceInput.source_kind || '').trim() || 'web_search';
      if (!isWebNoteSourceKind(sourceKind)) return null;
      const canonical = normalizeUrl(sourceInput.canonical_url || sourceInput.url || '');
      if (!canonical) return null;
      let source = sourceByCanonicalUrl.get(canonical);
      if (!source) {
        source = {
          id: String(sourceInput.id || makeId('src')).trim() || makeId('src'),
          note_id: noteId,
          analysis_run_id: analysisRunId,
          source_kind: sourceKind,
          source_query: String(sourceInput.source_query || ''),
          url: String(sourceInput.url || canonical),
          canonical_url: canonical,
          title: String(sourceInput.title || canonical),
          published_at: Number(sourceInput.published_at || 0) || 0,
          fetched_at: Number(sourceInput.fetched_at || startedAt) || startedAt,
          content_hash: String(sourceInput.content_hash || ''),
          query_confidence: Number(sourceInput.query_confidence || 0) || 0,
          search_intent: String(sourceInput.search_intent || getSearchIntentForSourceKind(sourceKind)),
        };
        sourceByCanonicalUrl.set(canonical, source);
        sources.push(source);
      } else {
        if (!source.title && sourceInput.title) source.title = String(sourceInput.title || '');
        if (!source.source_query && sourceInput.source_query) source.source_query = String(sourceInput.source_query || '');
        source.fetched_at = Math.max(Number(source.fetched_at || 0), Number(sourceInput.fetched_at || 0), startedAt);
        if (!source.content_hash && sourceInput.content_hash) source.content_hash = String(sourceInput.content_hash || '');
        source.query_confidence = Math.max(Number(source.query_confidence || 0), Number(sourceInput.query_confidence || 0));
        if (!source.search_intent && sourceInput.search_intent) source.search_intent = String(sourceInput.search_intent || '');
      }
      return source;
    }

    function addSnippetFallback(item = {}) {
      const canonical = normalizeUrl(item.url);
      const snippetText = normalizeWhitespace(`${String(item.title || '')}. ${String(item.snippet || '')}`);
      if (!canonical || !snippetText || isBoilerplatePassage(snippetText)) return;
      const source = ensureSource({
        source_kind: String(item.source_kind || 'web_search'),
        source_query: String(item.source_query || ''),
        url: canonical,
        canonical_url: canonical,
        title: String(item.title || canonical),
        fetched_at: startedAt,
        content_hash: buildHash(snippetText),
        query_confidence: Number(item.query_confidence || 0) || 0,
        search_intent: String(item.search_intent || getSearchIntentForSourceKind(item.source_kind)),
      });
      if (!source) return;
      const alreadyHasPassage = passages.some((passage) => String((passage && passage.source_id) || '') === String(source.id || ''));
      if (alreadyHasPassage) return;
      passages.push({
        id: makeId('passage'),
        note_id: noteId,
        analysis_run_id: analysisRunId,
        source_id: source.id,
        passage_index: 0,
        passage_text: snippetText,
        passage_start: 0,
        passage_end: snippetText.length,
        fetched_at: Number(source.fetched_at || startedAt),
      });
    }

    function addFetchedSource(entry = {}) {
      const item = entry && entry.item ? entry.item : {};
      const preview = entry && entry.preview;
      const canonical = normalizeUrl(item.url);
      if (!canonical || !preview || !preview.ok) {
        addSnippetFallback(item);
        return;
      }
      const text = normalizeWhitespace(String(preview.markdown || preview.text || ''));
      if (!text) {
        addSnippetFallback(item);
        return;
      }
      const source = ensureSource({
        source_kind: String(item.source_kind || 'web_search'),
        source_query: String(item.source_query || ''),
        url: canonical,
        canonical_url: canonical,
        title: String(preview.title || item.title || canonical),
        fetched_at: nowTs(),
        content_hash: buildHash(text),
        query_confidence: Number(item.query_confidence || 0) || 0,
        search_intent: String(item.search_intent || getSearchIntentForSourceKind(item.source_kind)),
      });
      if (!source) return;
      chunkText(text).forEach((chunk, idx) => {
        passages.push({
          id: makeId('passage'),
          note_id: noteId,
          analysis_run_id: analysisRunId,
          source_id: source.id,
          passage_index: idx,
          passage_text: chunk.text,
          passage_start: chunk.start,
          passage_end: chunk.end,
          fetched_at: Number(source.fetched_at || startedAt),
        });
      });
      if (!passages.some((passage) => String((passage && passage.source_id) || '') === String(source.id || ''))) {
        addSnippetFallback(item);
      }
    }

    function addRssSource(item = {}) {
      const canonical = normalizeUrl(item.canonical_article_url || item.url);
      if (!canonical) return;
      const source = ensureSource({
        source_kind: String(item.source_kind || 'rss_search'),
        source_query: String(item.source_query || ''),
        url: canonical,
        canonical_url: canonical,
        title: String(item.title || item.display_title || canonical),
        published_at: Number(item.published_at || 0) || 0,
        fetched_at: Number(item.content_fetched_at || item.fetched_at || startedAt) || startedAt,
        content_hash: String(item.content_hash || buildHash(String(item.raw_content_text || item.content_text || item.clean_summary || item.content_excerpt || ''))),
        query_confidence: Number(item.query_confidence || 0) || 0,
        search_intent: String(item.search_intent || getSearchIntentForSourceKind(item.source_kind)),
      });
      if (!source) return;
      const text = normalizeWhitespace(String(
        item.raw_content_text
        || item.content_text
        || item.clean_summary
        || item.content_excerpt
        || item.snippet
        || ''
      ));
      if (!text) {
        addSnippetFallback(item);
        return;
      }
      chunkText(text).forEach((chunk, idx) => {
        passages.push({
          id: makeId('passage'),
          note_id: noteId,
          analysis_run_id: analysisRunId,
          source_id: source.id,
          passage_index: idx,
          passage_text: chunk.text,
          passage_start: chunk.start,
          passage_end: chunk.end,
          fetched_at: Number(source.fetched_at || startedAt),
        });
      });
      if (!passages.some((passage) => String((passage && passage.source_id) || '') === String(source.id || ''))) {
        addSnippetFallback(item);
      }
    }

    const explicitFetchItems = explicitUrls.slice(0, maxFetchesForNote).map((item) => ({
      source_kind: 'explicit_url',
      source_query: '',
      url: item.canonical_url,
      title: '',
    }));
    emitProgress('explicit_fetch_queue', {
      item_count: explicitFetchItems.length,
    });
    explicitFetchItems.forEach((item) => {
      ensureSource({
        source_kind: item.source_kind,
        source_query: item.source_query,
        url: item.url,
        canonical_url: item.url,
        title: item.url,
        fetched_at: startedAt,
      });
    });
    const explicitFetchResults = await runPool(explicitFetchItems, async (item) => {
      const preview = await fetchWithCache(item.url);
      return { item, preview };
    }, MAX_CONCURRENCY);
    explicitFetchResults.forEach(addFetchedSource);
    emitProgress('explicit_fetch_done', {
      fetched_count: explicitFetchResults.length,
      source_count: sources.length,
      passage_count: passages.length,
    });

    const claimSearchResults = await Promise.all(claims.map(async (claim) => {
      const searchPlans = buildClaimSearchPlans(claim);
      emitProgress('claim_search_start', {
        claim_id: String(claim.id || '').trim(),
        claim_text: String(claim.claim_text || '').slice(0, 180),
        plan_count: searchPlans.length,
      });
      const buckets = {
        support: { items: [] },
        official: { items: [] },
        challenge: { items: [] },
      };
      for (const plan of searchPlans) {
        if (hasSatisfiedSearchIntent(buckets[plan.intent], plan)) continue;
        if (rssSearch) {
          const rssResults = await rssSearchWithCache(plan.query, Math.max(4, maxResultsPerClaim + 1));
          const acceptedRss = rssResults
            .map((result) => ({
              ...result,
              score_meta: scoreSearchCandidate(claim, plan, result),
            }))
            .filter((result) => shouldAcceptSearchCandidate(result.score_meta, plan))
            .filter((result) => !shouldRejectSearchCandidate(claim, result, result.score_meta))
            .sort((a, b) => Number((b.score_meta && b.score_meta.query_confidence) || 0) - Number((a.score_meta && a.score_meta.query_confidence) || 0))
            .slice(0, 3)
            .map((result) => ({
              title: String((result && (result.title || result.display_title)) || ''),
              url: normalizeUrl(result && (result.canonical_article_url || result.url)),
              canonical_article_url: normalizeUrl(result && (result.canonical_article_url || result.url)),
              snippet: String((result && (result.clean_summary || result.content_excerpt || result.summary || result.snippet)) || ''),
              raw_content_text: String((result && (result.raw_content_text || result.content_text || '')) || ''),
              content_text: String((result && (result.content_text || result.raw_content_text || '')) || ''),
              published_at: Number((result && result.published_at) || 0) || 0,
              fetched_at: Number((result && (result.content_fetched_at || result.fetched_at)) || 0) || startedAt,
              source_kind: getRssSourceKindForIntent(plan.intent),
              source_query: String(plan.query || ''),
              search_intent: String(plan.intent || 'support'),
              query_confidence: Number((result.score_meta && result.score_meta.query_confidence) || 0) || 0,
            }))
            .filter((result) => result.url);
          if (acceptedRss.length > 0) {
            buckets[plan.intent].items = dedupeBy(
              buckets[plan.intent].items.concat(acceptedRss),
              (item) => String(item.url || '')
            ).sort((a, b) => Number(b.query_confidence || 0) - Number(a.query_confidence || 0)).slice(0, 4);
          }
        }
        if (hasSatisfiedSearchIntent(buckets[plan.intent], plan)) continue;
        const results = await searchWithCache(plan.query, maxResultsPerClaim);
        const accepted = results
          .map((result) => ({
            ...result,
            score_meta: scoreSearchCandidate(claim, plan, result),
          }))
          .filter((result) => shouldAcceptSearchCandidate(result.score_meta, plan))
          .filter((result) => !shouldRejectSearchCandidate(claim, result, result.score_meta))
          .sort((a, b) => Number((b.score_meta && b.score_meta.query_confidence) || 0) - Number((a.score_meta && a.score_meta.query_confidence) || 0))
          .slice(0, 3)
          .map((result) => ({
            title: String((result && result.title) || ''),
            url: normalizeUrl(result && result.url),
            snippet: String((result && result.snippet) || ''),
            source_kind: String(plan.source_kind || 'web_search'),
            source_query: String(plan.query || ''),
            search_intent: String(plan.intent || 'support'),
            query_confidence: Number((result.score_meta && result.score_meta.query_confidence) || 0) || 0,
          }))
          .filter((result) => result.url);
        if (accepted.length > 0) {
          buckets[plan.intent].items = dedupeBy(
            buckets[plan.intent].items.concat(accepted),
            (item) => String(item.url || '')
          ).sort((a, b) => Number(b.query_confidence || 0) - Number(a.query_confidence || 0)).slice(0, 4);
        }
      }
      emitProgress('claim_search_done', {
        claim_id: String(claim.id || '').trim(),
        claim_text: String(claim.claim_text || '').slice(0, 180),
        support_results: buckets.support.items.length,
        official_results: buckets.official.items.length,
        challenge_results: buckets.challenge.items.length,
      });
      return {
        claim,
        webRes: ['official', 'support', 'challenge'].map((intent) => ({
          intent,
          results: (buckets[intent] && Array.isArray(buckets[intent].items)) ? buckets[intent].items : [],
        })),
      };
    }));

    const searchSources = [];
    claimSearchResults.forEach((entry) => {
      const resultGroups = Array.isArray(entry.webRes) ? entry.webRes : [];
      resultGroups.forEach((group) => {
        const results = Array.isArray(group && group.results) ? group.results : [];
        results.slice(0, maxResultsPerClaim).forEach((item) => {
          const canonical = normalizeUrl(item && item.url);
          if (!canonical) return;
          searchSources.push({
            source_kind: String((item && item.source_kind) || 'web_search'),
            source_query: String((item && item.source_query) || ''),
            search_intent: String((item && item.search_intent) || (group && group.intent) || 'support'),
            query_confidence: Number((item && item.query_confidence) || 0) || 0,
            url: canonical,
            title: String((item && item.title) || ''),
            snippet: String((item && item.snippet) || ''),
          });
        });
      });
    });

    const rssSources = dedupeBy(
      searchSources.filter((item) => isRssNoteSourceKind(item && item.source_kind)),
      (item) => `${item.source_kind}:${normalizeUrl(item.url)}`
    );
    rssSources.forEach(addRssSource);
    emitProgress('rss_source_done', {
      rss_source_count: rssSources.length,
      source_count: sources.length,
      passage_count: passages.length,
    });

    const fetchQueue = dedupeBy(
      searchSources.filter((item) => !isRssNoteSourceKind(item && item.source_kind)),
      (item) => `${item.source_kind}:${normalizeUrl(item.url)}`
    )
      .slice(0, maxFetchesForNote);
    emitProgress('web_fetch_queue', {
      item_count: fetchQueue.length,
      source_candidate_count: searchSources.length,
    });

    const fetchResults = await runPool(fetchQueue, async (item) => {
      const preview = await fetchWithCache(item.url);
      return { item, preview };
    }, MAX_CONCURRENCY);
    fetchResults.forEach(addFetchedSource);
    emitProgress('web_fetch_done', {
      fetched_count: fetchResults.length,
      source_count: sources.length,
      passage_count: passages.length,
    });

    const passageScoresByClaim = new Map();
    const sourceSupportCount = new Map();
    claims.forEach((claim) => passageScoresByClaim.set(claim.id, []));

    const passageScored = passages.map((passage) => {
      const source = sources.find((item) => String(item.id || '') === String(passage.source_id || '')) || null;
      const passageVector = hashEmbedText(String(passage.passage_text || ''));
      return { passage, source, passageVector };
    });

    claims.forEach((claim) => {
      const effectiveClaimText = getEffectiveClaimText(claim);
      const claimVector = hashEmbedText(effectiveClaimText);
      const ranked = passageScored
        .map((entry) => {
          const semanticScore = cosineSimilarity(claimVector, entry.passageVector);
          const lexicalScore = lexicalOverlapScore(effectiveClaimText, entry.passage.passage_text);
          const timeScore = timeMatchScore(effectiveClaimText, entry.passage.passage_text);
          const entityScore = namedEntityCoverageScore(effectiveClaimText, entry.passage.passage_text);
          const exactScore = exactnessScore(effectiveClaimText, entry.passage.passage_text);
          const contradictionScore = contradictionCueScore(effectiveClaimText, entry.passage.passage_text);
          const searchIntent = getSearchIntentForSourceKind(entry.source && entry.source.source_kind);
          const queryConfidence = Number((entry.source && entry.source.query_confidence) || 0) || 0;
          const authorityScore = sourceAuthorityScore(entry.source, claim);
          const freshnessScore = sourceFreshnessScore(entry.source, claim, notePolicy);
          return {
            claim,
            source: entry.source,
            passage: entry.passage,
            search_intent: searchIntent,
            query_confidence: queryConfidence,
            semantic_score: semanticScore,
            lexical_score: lexicalScore,
            time_score: timeScore,
            entity_score: entityScore,
            exact_score: exactScore,
            contradiction_score: contradictionScore,
            authority_score: authorityScore,
            freshness_score: freshnessScore,
          };
        })
        .filter((entry) => entry.source && (entry.semantic_score > 0.16 || entry.lexical_score > 0.1 || entry.query_confidence >= 0.52 || entry.contradiction_score >= 0.3))
        .sort((a, b) => ((b.query_confidence + b.semantic_score + b.lexical_score + b.entity_score + b.exact_score) - (a.query_confidence + a.semantic_score + a.lexical_score + a.entity_score + a.exact_score)) || a.passage.id.localeCompare(b.passage.id))
        .slice(0, 10);
      const supportRanked = ranked.filter((item) => item.search_intent !== 'challenge');
      const challengeRanked = ranked.filter((item) => item.search_intent === 'challenge' || item.contradiction_score >= 0.34);
      const uniqueSupportCount = new Set(supportRanked.map((item) => String((item.source && item.source.canonical_url) || ''))).size;
      const uniqueChallengeCount = new Set(challengeRanked.map((item) => String((item.source && item.source.canonical_url) || ''))).size;
      sourceSupportCount.set(claim.id, {
        support: uniqueSupportCount,
        challenge: uniqueChallengeCount,
      });
      passageScoresByClaim.set(claim.id, {
        support: supportRanked,
        challenge: challengeRanked,
      });
    });
    let temporalScores = new Map();
    if (temporalGraphScorer && sources.length > 0) {
      const rows = [];
      const sourceTerms = new Map();
      sources.forEach((source) => {
        const relatedPassages = passages.filter((item) => String(item.source_id || '') === String(source.id || ''));
        const combined = normalizeWhitespace(relatedPassages.map((item) => item.passage_text).join(' '));
        const terms = Array.from(new Set(tokenize(combined).filter((item) => item.length >= 4))).slice(0, 18);
        sourceTerms.set(source.id, new Set(terms));
        rows.push({
          src: source.id,
          dst: source.id,
          ts: Number(source.fetched_at || startedAt),
          weight: 0,
          source_key: source.id,
        });
      });
      for (let i = 0; i < sources.length; i += 1) {
        for (let j = i + 1; j < sources.length; j += 1) {
          const left = sources[i];
          const right = sources[j];
          const leftTerms = sourceTerms.get(left.id) || new Set();
          const shared = Array.from(leftTerms).filter((term) => (sourceTerms.get(right.id) || new Set()).has(term));
          if (shared.length === 0) continue;
          rows.push({ src: left.id, dst: right.id, ts: Math.max(Number(left.fetched_at || startedAt), Number(right.fetched_at || startedAt)), weight: shared.length, source_key: left.id });
          rows.push({ src: right.id, dst: left.id, ts: Math.max(Number(left.fetched_at || startedAt), Number(right.fetched_at || startedAt)), weight: shared.length, source_key: right.id });
        }
      }
      const res = await temporalGraphScorer({ rows, now_ts: nowTs() });
      const scores = Array.isArray(res && res.scores) ? res.scores : [];
      temporalScores = new Map(scores.map((item) => [String(item.source_key || ''), Number(item.recent_7d_score || item.global_score || 0) || 0]));
    }

    claims.forEach((claim) => {
      const rankedGroups = passageScoresByClaim.get(claim.id) || {};
      const supportRanked = Array.isArray(rankedGroups.support) ? rankedGroups.support : [];
      const challengeRanked = Array.isArray(rankedGroups.challenge) ? rankedGroups.challenge : [];
      const supportCounts = sourceSupportCount.get(claim.id) || {};
      const supportCorroboration = Math.min(1, (Number(supportCounts.support || 0) / 3));
      const challengeCorroboration = Math.min(1, (Number(supportCounts.challenge || 0) / 2));
      const freshnessWeight = scorePolicyFreshness(notePolicy);
      const supportFreshnessWeight = 0.04 + (0.08 * freshnessWeight);
      const contradictionFreshnessWeight = 0.03 + (0.06 * freshnessWeight);
      const scoreEntries = (entries = [], corroborationBase = 0, stance = 'support') => entries
        .map((entry) => {
          const temporalScore = Number(temporalScores.get(String((entry.source && entry.source.id) || '')) || 0) || 0;
          const score = stance === 'contradict'
            ? ((0.16 * entry.semantic_score)
              + (0.12 * entry.lexical_score)
              + (0.1 * entry.time_score)
              + (0.08 * corroborationBase)
              + (0.08 * temporalScore)
              + (0.12 * entry.query_confidence)
              + (0.12 * entry.entity_score)
              + (0.08 * entry.exact_score)
              + (0.08 * entry.authority_score)
              + (contradictionFreshnessWeight * entry.freshness_score)
              + (0.24 * entry.contradiction_score)
              + (entry.search_intent === 'challenge' ? 0.08 : 0))
            : ((0.24 * entry.semantic_score)
              + (0.16 * entry.lexical_score)
              + (0.12 * entry.time_score)
              + (0.1 * corroborationBase)
              + (0.08 * temporalScore)
              + (0.14 * entry.query_confidence)
              + (0.08 * entry.entity_score)
              + (0.12 * entry.exact_score)
              + (0.08 * entry.authority_score)
              + (supportFreshnessWeight * entry.freshness_score)
              - (0.12 * entry.contradiction_score));
          return {
            source: entry.source,
            passage: entry.passage,
            search_intent: entry.search_intent,
            support_label: stance,
            stance,
            score: clampUnit(score),
            query_confidence: entry.query_confidence,
            semantic_score: entry.semantic_score,
            lexical_score: entry.lexical_score,
            time_score: entry.time_score,
            corroboration_score: corroborationBase,
            temporal_score: temporalScore,
            contradiction_score: entry.contradiction_score,
            entity_score: entry.entity_score,
            exact_score: entry.exact_score,
            authority_score: entry.authority_score,
            freshness_score: entry.freshness_score,
            excerpt: buildCitationExcerpt(getEffectiveClaimText(entry.claim), entry.passage && entry.passage.passage_text, 340),
          };
        })
        .sort((a, b) => (b.score - a.score) || String((a.passage && a.passage.id) || '').localeCompare(String((b.passage && b.passage.id) || '')));
      const scoredSupport = scoreEntries(supportRanked, supportCorroboration);
      const scoredChallenge = scoreEntries(challengeRanked, challengeCorroboration, 'contradict');
      const topSupport = scoredSupport[0] || null;
      const topChallenge = scoredChallenge[0] || null;
      const supportConfidence = Number((topSupport && topSupport.score) || 0) || 0;
      const contradictConfidence = Number((topChallenge && topChallenge.score) || 0) || 0;
      const absolutistClaim = /\b(every|all|always|never|entire|completely|only)\b/i.test(String(claim.claim_text || ''));
      let status = 'weak_evidence';
      if (
        supportConfidence >= 0.62
        && contradictConfidence >= 0.48
        && Math.abs(supportConfidence - contradictConfidence) <= 0.22
      ) {
        status = 'mixed';
      } else if (topChallenge && contradictConfidence >= 0.58 && contradictConfidence >= supportConfidence + 0.08) {
        status = 'contradicted';
      } else if (topSupport && supportConfidence >= 0.82 && supportConfidence >= contradictConfidence + 0.08) {
        status = 'supported';
      } else if (topSupport && supportConfidence >= HIGHLIGHT_THRESHOLD && supportConfidence >= contradictConfidence + 0.06) {
        status = 'mostly_supported';
      } else if ((topSupport && supportConfidence >= 0.56) || (topChallenge && contradictConfidence >= 0.56)) {
        status = 'mixed';
      }
      if ((status === 'supported' || status === 'mostly_supported') && absolutistClaim && topSupport && Number(topSupport.contradiction_score || 0) >= 0.28) {
        status = 'mixed';
      }
      claim.status = status;
      claim.verdict = status;
      claim.support_confidence = supportConfidence;
      claim.contradict_confidence = contradictConfidence;
      claim.corroboration = Math.max(supportCorroboration, challengeCorroboration);
      claim.authority = Math.max(Number((topSupport && topSupport.authority_score) || 0) || 0, Number((topChallenge && topChallenge.authority_score) || 0) || 0);
      claim.freshness = Math.max(Number((topSupport && topSupport.freshness_score) || 0) || 0, Number((topChallenge && topChallenge.freshness_score) || 0) || 0);
      claim.truth_confidence = clampUnit(
        status === 'supported'
          ? supportConfidence * (1 - (contradictConfidence * 0.35))
          : status === 'mostly_supported'
            ? supportConfidence * (1 - (contradictConfidence * 0.42))
          : status === 'contradicted'
            ? contradictConfidence * (1 - (supportConfidence * 0.35))
            : status === 'mixed'
              ? Math.max(supportConfidence, contradictConfidence) * 0.84
              : Math.max(supportConfidence, contradictConfidence) * 0.55
      );
      claim.top_score = claim.truth_confidence;
      claim.highlight_score = claim.truth_confidence;
      claim.claim_reliability = clampUnit(
        0.5
        + (0.55 * supportConfidence)
        - (0.75 * contradictConfidence)
        + (0.1 * claim.corroboration)
      );
      claim.explanation = buildClaimExplanation(claim, topSupport, topChallenge);
      claim.rewrite_suggestions = buildRewriteSuggestions(claim, topSupport, topChallenge);
      const storedCitations = dedupeBy(
        scoredSupport.slice(0, 3).concat(scoredChallenge.slice(0, 2)),
        (item) => `${String((item.source && item.source.id) || '')}:${String((item.passage && item.passage.id) || '')}`
      );
      storedCitations.forEach((item, idx) => {
        citations.push({
          id: makeId('citation'),
          note_id: noteId,
          analysis_run_id: analysisRunId,
          claim_id: claim.id,
          source_id: String((item.source && item.source.id) || ''),
          passage_id: String((item.passage && item.passage.id) || ''),
          citation_index: idx,
          support_label: item.stance,
          score: item.score,
          semantic_score: item.semantic_score,
          lexical_score: item.lexical_score,
          time_score: item.time_score,
          corroboration_score: item.corroboration_score,
          temporal_score: item.temporal_score,
          excerpt: item.excerpt,
        });
      });
    });

    emitProgress('scoring_done', {
      claim_count: claims.length,
      source_count: sources.length,
      passage_count: passages.length,
      citation_count: citations.length,
    });

    const summary = summarizeAnalysis(claims);
    const aggregate = buildNoteAggregate(claims);
    const latestEvidenceAt = sources.reduce((maxTs, source) => Math.max(maxTs, Number((source && source.published_at) || (source && source.fetched_at) || 0) || 0), 0);
    const completedAt = nowTs();
    return {
      ok: true,
      analysis_run_id: analysisRunId,
      note_revision: noteRevision,
      extractor_version: ANALYZER_VERSION,
      evidence_mode: NOTE_EVIDENCE_MODE,
      started_at: startedAt,
      completed_at: completedAt,
      status: 'completed',
      message: summary.claim_count > 0 ? 'Evidence scan completed.' : 'No factual claims detected.',
      note_policy: notePolicy,
      analysis_source: analysisSource,
      latest_evidence_at: latestEvidenceAt,
      next_refresh_at: computeNextRefreshAt(completedAt, notePolicy),
      claims,
      sources,
      passages,
      citations,
      summary,
      note_score: aggregate.note_score,
      coverage_score: aggregate.coverage_score,
      risk_level: aggregate.risk_level,
      explicit_urls: explicitUrls,
    };
  }

  return {
    analyze,
    HIGHLIGHT_THRESHOLD,
    ANALYZER_VERSION,
    NOTE_EVIDENCE_MODE,
  };
}

module.exports = {
  createNoteAnalysisEngine,
  HIGHLIGHT_THRESHOLD,
  NOTE_EVIDENCE_MODE,
};
