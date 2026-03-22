const crypto = require('crypto');
const { hashEmbedText } = require('./embedding_runtime');

const ANALYZER_VERSION = 'note-analyzer-v5';
const NOTE_EVIDENCE_MODE = 'web_only';
const DAY_MS = 24 * 60 * 60 * 1000;
const HIGHLIGHT_THRESHOLD = 0.72;
const PASSAGE_CHUNK_SIZE = 480;
const PASSAGE_OVERLAP = 80;
const MAX_PASSAGES_PER_SOURCE = 8;
const MAX_WEB_RESULTS_PER_CLAIM = 3;
const MAX_FETCHES_PER_NOTE = 18;
const MAX_CONCURRENCY = 2;
const VERB_PATTERN = /\b(is|are|was|were|has|have|had|launched|released|acquired|bought|won|lost|grew|fell|sued|announced|built|uses|use|caused|causes|reported|reports|showed|shows|said|says|fight|fights|fighting|attack|attacks|attacked|attacking|invade|invades|invaded|invading|bombed|bombing|strike|strikes|struck|kill|kills|killed)\b/i;
const WEB_NOTE_SOURCE_KINDS = new Set(['explicit_url', 'web_search', 'official_search', 'challenge_search']);
const QUERY_STOPWORDS = new Set(['the', 'and', 'for', 'with', 'this', 'that', 'from', 'into', 'their', 'there', 'about', 'according', 'over', 'than']);

const searchCache = new Map();
const fetchCache = new Map();

function nowTs() {
  return Date.now();
}

function normalizeWhitespace(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
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

function chunkText(text = '', size = PASSAGE_CHUNK_SIZE, overlap = PASSAGE_OVERLAP) {
  const raw = String(text || '').trim();
  if (!raw) return [];
  const out = [];
  let start = 0;
  while (start < raw.length && out.length < MAX_PASSAGES_PER_SOURCE) {
    const end = Math.min(raw.length, start + size);
    const chunk = raw.slice(start, end).trim();
    if (chunk) out.push({ text: chunk, start, end });
    if (end >= raw.length) break;
    start = Math.max(start + 1, end - overlap);
  }
  return out;
}

function buildHash(value = '') {
  return crypto.createHash('sha1').update(String(value || ''), 'utf8').digest('hex').slice(0, 16);
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

function splitClaims(markdown = '') {
  const text = String(markdown || '');
  const out = [];
  const re = /[^.!?\n]+[.!?]?/g;
  let match = re.exec(text);
  while (match) {
    const chunk = String(match[0] || '');
    const trimmed = chunk.trim();
    if (!trimmed) {
      match = re.exec(text);
      continue;
    }
    const startTrim = chunk.indexOf(trimmed);
    const start = match.index + Math.max(0, startTrim);
    const end = start + trimmed.length;
    splitCompoundClaimSegments(trimmed, start).forEach((segment) => out.push(segment));
    match = re.exec(text);
  }
  return out;
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
  return segments;
}

function parseClaimStructure(claimText = '') {
  const normalized = normalizeWhitespace(claimText);
  const verbMatch = normalized.match(VERB_PATTERN);
  if (!verbMatch) {
    return {
      subject_text: normalized,
      predicate_text: '',
      object_text: '',
    };
  }
  const predicateText = String(verbMatch[0] || '').trim();
  const idx = verbMatch.index || 0;
  return {
    subject_text: normalizeWhitespace(normalized.slice(0, idx)),
    predicate_text: predicateText,
    object_text: normalizeWhitespace(normalized.slice(idx + predicateText.length)),
  };
}

function deriveClaimAnchor(claim = {}) {
  const subject = normalizeWhitespace(String((claim && claim.subject_text) || ''));
  const predicate = normalizeWhitespace(String((claim && claim.predicate_text) || '')).toLowerCase();
  const object = normalizeWhitespace(String((claim && claim.object_text) || ''));
  if (!subject) return '';
  if ((predicate === 'released' || predicate === 'launched' || predicate === 'announced') && object) {
    return normalizeWhitespace(`${subject} ${object}`);
  }
  return subject;
}

function resolveClaimContext(claims = []) {
  let anchor = '';
  return (Array.isArray(claims) ? claims : []).map((claim) => {
    const next = { ...claim };
    const subject = normalizeWhitespace(String(next.subject_text || ''));
    if (/^(it|its|they|their|this|that)\b/i.test(subject) && anchor) {
      const suffix = normalizeWhitespace(subject.replace(/^(it|its|they|their|this|that)\b/i, ''));
      next.subject_text = normalizeWhitespace(`${anchor} ${suffix}`);
    }
    const derivedAnchor = deriveClaimAnchor(next);
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
    String((claim && claim.subject_text) || ''),
    String((claim && claim.predicate_text) || ''),
    String((claim && claim.object_text) || ''),
    String((claim && claim.time_text) || ''),
  ].map((item) => normalizeWhitespace(item)).filter(Boolean);
  return parts.join(' ');
}

function isWebNoteSourceKind(sourceKind = '') {
  return WEB_NOTE_SOURCE_KINDS.has(String(sourceKind || '').trim());
}

function getSearchIntentForSourceKind(sourceKind = '') {
  const kind = String(sourceKind || '').trim();
  if (kind === 'official_search') return 'official';
  if (kind === 'challenge_search') return 'challenge';
  return 'support';
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
  const claimText = String((claim && claim.claim_text) || '');
  if (isReleasePredicate(predicate)) return ['release', 'released', 'launch', 'launched', 'announcement'];
  if (isComparisonClaim(claimText)) return ['adoption', 'growth', 'metric', 'compare', 'comparison', 'usage'];
  if (isConflictClaim(claimText)) return ['war', 'conflict', 'fighting', 'attack', 'military'];
  return buildCoverageTerms(predicate);
}

function scoreSearchCandidate(claim = {}, plan = {}, result = {}) {
  const title = String((result && result.title) || '');
  const snippet = String((result && result.snippet) || '');
  const combined = normalizeWhitespace(`${title} ${snippet}`);
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
  const semanticScore = cosineSimilarity(hashEmbedText(String((claim && claim.claim_text) || '')), hashEmbedText(combined));
  const lexicalScore = lexicalOverlapScore(String((claim && claim.claim_text) || ''), combined);
  const subjectCoverage = tokenCoverageScore(buildCoverageTerms(String((claim && claim.subject_text) || '')), combined);
  const objectCoverage = tokenCoverageScore(buildCoverageTerms(String((claim && claim.object_text) || '')), combined);
  const predicateCoverage = tokenCoverageScore(buildPredicateTerms(claim), combined);
  const timeScore = timeMatchScore(String((claim && claim.claim_text) || ''), combined);
  const queryConfidence = Math.max(0, Math.min(1,
    (0.24 * semanticScore)
    + (0.18 * lexicalScore)
    + (0.22 * subjectCoverage)
    + (0.14 * objectCoverage)
    + (0.12 * predicateCoverage)
    + (0.10 * timeScore)
  ));
  return {
    query_confidence: queryConfidence,
    semantic_score: semanticScore,
    lexical_score: lexicalScore,
    subject_coverage: subjectCoverage,
    object_coverage: objectCoverage,
    predicate_coverage: predicateCoverage,
    time_score: timeScore,
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
  const subject = String((claim && claim.subject_text) || '').trim();
  const predicate = String((claim && claim.predicate_text) || '').trim().toLowerCase();
  const object = String((claim && claim.object_text) || '').trim();
  const time = String((claim && claim.time_text) || '').trim();
  const claimText = String((claim && claim.claim_text) || '').trim();
  const anchor = deriveClaimAnchor(claim);
  const plans = [];
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
  }
  if (baseQuery) {
    pushPlan(baseQuery, 'web_search', 'support', 0.34);
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

function summarizeAnalysis(claims = []) {
  return {
    claim_count: claims.length,
    supported_count: claims.filter((item) => item.status === 'supported').length,
    contested_count: claims.filter((item) => item.status === 'contested').length,
    uncertain_count: claims.filter((item) => item.status === 'uncertain').length,
    no_evidence_count: claims.filter((item) => item.status === 'no_evidence').length,
  };
}

function createNoteAnalysisEngine(options = {}) {
  const webSearch = typeof options.webSearch === 'function' ? options.webSearch : null;
  const fetchUrl = typeof options.fetchUrl === 'function' ? options.fetchUrl : null;
  const temporalGraphScorer = typeof options.temporalGraphScorer === 'function' ? options.temporalGraphScorer : null;
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

  async function analyze(note = {}, context = {}) {
    const startedAt = nowTs();
    const noteId = String((note && note.id) || '').trim();
    const noteRevision = Number((note && note.analysis_revision) || 0);
    const body = String((note && note.body_markdown) || '');
    const analysisRunId = makeId('analysis');
    const explicitUrls = extractExplicitUrls(body);
    const rawClaims = resolveClaimContext(splitClaims(body)
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
          time_text: yearTokens(item.claim_text).join(' '),
          ...structure,
          modality: factuality.modality,
          factuality: factuality.factuality,
          status: 'no_evidence',
          top_score: 0,
          highlight_score: 0,
        };
        return claim;
      }));
    const claims = rawClaims.filter((item) => item.factuality === 'factual');
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
      if (!canonical || !snippetText) return;
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
    }

    const explicitFetchItems = explicitUrls.slice(0, MAX_FETCHES_PER_NOTE).map((item) => ({
      source_kind: 'explicit_url',
      source_query: '',
      url: item.canonical_url,
      title: '',
    }));
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

    const claimSearchResults = await Promise.all(claims.map(async (claim) => {
      const searchPlans = buildClaimSearchPlans(claim);
      const buckets = {
        support: { items: [] },
        official: { items: [] },
        challenge: { items: [] },
      };
      for (const plan of searchPlans) {
        if (hasSatisfiedSearchIntent(buckets[plan.intent], plan)) continue;
        const results = await searchWithCache(plan.query, 4);
        const accepted = results
          .map((result) => ({
            ...result,
            score_meta: scoreSearchCandidate(claim, plan, result),
          }))
          .filter((result) => shouldAcceptSearchCandidate(result.score_meta, plan))
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
        results.slice(0, MAX_WEB_RESULTS_PER_CLAIM).forEach((item) => {
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

    const fetchQueue = dedupeBy(searchSources, (item) => `${item.source_kind}:${normalizeUrl(item.url)}`)
      .slice(0, MAX_FETCHES_PER_NOTE);

    const fetchResults = await runPool(fetchQueue, async (item) => {
      const preview = await fetchWithCache(item.url);
      return { item, preview };
    }, MAX_CONCURRENCY);
    fetchResults.forEach(addFetchedSource);

    const passageScoresByClaim = new Map();
    const sourceSupportCount = new Map();
    claims.forEach((claim) => passageScoresByClaim.set(claim.id, []));

    const passageScored = passages.map((passage) => {
      const source = sources.find((item) => String(item.id || '') === String(passage.source_id || '')) || null;
      const passageVector = hashEmbedText(String(passage.passage_text || ''));
      return { passage, source, passageVector };
    });

    claims.forEach((claim) => {
      const claimVector = hashEmbedText(String(claim.claim_text || ''));
      const ranked = passageScored
        .map((entry) => {
          const semanticScore = cosineSimilarity(claimVector, entry.passageVector);
          const lexicalScore = lexicalOverlapScore(claim.claim_text, entry.passage.passage_text);
          const timeScore = timeMatchScore(claim.claim_text, entry.passage.passage_text);
          const searchIntent = getSearchIntentForSourceKind(entry.source && entry.source.source_kind);
          const queryConfidence = Number((entry.source && entry.source.query_confidence) || 0) || 0;
          return {
            claim,
            source: entry.source,
            passage: entry.passage,
            search_intent: searchIntent,
            query_confidence: queryConfidence,
            semantic_score: semanticScore,
            lexical_score: lexicalScore,
            time_score: timeScore,
          };
        })
        .filter((entry) => entry.source && (entry.semantic_score > 0.16 || entry.lexical_score > 0.1 || entry.query_confidence >= 0.58))
        .sort((a, b) => ((b.query_confidence + b.semantic_score + b.lexical_score) - (a.query_confidence + a.semantic_score + a.lexical_score)) || a.passage.id.localeCompare(b.passage.id))
        .slice(0, 8);
      const supportRanked = ranked.filter((item) => item.search_intent !== 'challenge');
      const challengeRanked = ranked.filter((item) => item.search_intent === 'challenge');
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
      const scoreEntries = (entries = [], corroborationBase = 0, supportLabel = 'no_evidence') => entries
        .map((entry) => {
          const temporalScore = Number(temporalScores.get(String((entry.source && entry.source.id) || '')) || 0) || 0;
          const score = (0.32 * entry.semantic_score)
            + (0.18 * entry.lexical_score)
            + (0.12 * entry.time_score)
            + (0.12 * corroborationBase)
            + (0.10 * temporalScore)
            + (0.16 * entry.query_confidence);
          return {
            source: entry.source,
            passage: entry.passage,
            search_intent: entry.search_intent,
            support_label: supportLabel,
            score,
            query_confidence: entry.query_confidence,
            semantic_score: entry.semantic_score,
            lexical_score: entry.lexical_score,
            time_score: entry.time_score,
            corroboration_score: corroborationBase,
            temporal_score: temporalScore,
            excerpt: entry.passage.passage_text.slice(0, 340),
          };
        })
        .sort((a, b) => (b.score - a.score) || String((a.passage && a.passage.id) || '').localeCompare(String((b.passage && b.passage.id) || '')));
      const scoredSupport = scoreEntries(supportRanked, supportCorroboration);
      const scoredChallenge = scoreEntries(challengeRanked, challengeCorroboration, 'challenge');
      const topSupport = scoredSupport[0] || null;
      const topChallenge = scoredChallenge[0] || null;
      let status = 'no_evidence';
      if (topSupport && topSupport.score >= HIGHLIGHT_THRESHOLD) {
        status = 'supported';
      } else if (topSupport && topSupport.score >= 0.56) {
        status = 'uncertain';
      }
      if (
        topSupport
        && topChallenge
        && topSupport.score >= 0.64
        && topChallenge.score >= 0.56
        && Math.abs(topSupport.score - topChallenge.score) <= 0.08
      ) {
        status = 'contested';
      }
      claim.status = status;
      claim.top_score = Math.max(Number((topSupport && topSupport.score) || 0), Number((topChallenge && topChallenge.score) || 0));
      claim.highlight_score = claim.top_score;
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
          support_label: item.search_intent === 'challenge' ? 'challenge' : status,
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
      message: summary.claim_count > 0 ? 'Evidence scan completed.' : 'No factual claims detected.',
      claims,
      sources,
      passages,
      citations,
      summary,
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
