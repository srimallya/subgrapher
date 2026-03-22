const crypto = require('crypto');
const { hashEmbedText } = require('./embedding_runtime');

const ANALYZER_VERSION = 'note-analyzer-v2';
const DAY_MS = 24 * 60 * 60 * 1000;
const HIGHLIGHT_THRESHOLD = 0.72;
const PASSAGE_CHUNK_SIZE = 480;
const PASSAGE_OVERLAP = 80;
const MAX_PASSAGES_PER_SOURCE = 8;
const MAX_WEB_RESULTS_PER_CLAIM = 3;
const MAX_FETCHES_PER_NOTE = 18;
const MAX_CONCURRENCY = 2;
const VERB_PATTERN = /\b(is|are|was|were|has|have|had|launched|released|acquired|bought|won|lost|grew|fell|sued|announced|built|uses|use|caused|causes|reported|reports|showed|shows|said|says)\b/i;

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
  const hasEntity = /\b[A-Z][a-z]{2,}\b/.test(text);
  if (hasVerb && (hasYear || hasNumber || hasEntity)) {
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

function buildClaimSearchPlans(claim = {}) {
  const baseQuery = buildClaimQuery(claim);
  const subject = String((claim && claim.subject_text) || '').trim();
  const predicate = String((claim && claim.predicate_text) || '').trim().toLowerCase();
  const object = String((claim && claim.object_text) || '').trim();
  const time = String((claim && claim.time_text) || '').trim();
  const claimText = String((claim && claim.claim_text) || '').trim();
  const plans = [];
  if (baseQuery) {
    plans.push({ query: baseQuery, source_kind: 'web_search', intent: 'support' });
  }
  if (subject || object) {
    plans.push({
      query: normalizeWhitespace(`${subject} ${object} official announcement ${time}`),
      source_kind: 'official_search',
      intent: 'official',
    });
  }
  if (predicate === 'released' || predicate === 'launched' || predicate === 'announced') {
    plans.push({
      query: normalizeWhitespace(`${subject} ${object} release date official`),
      source_kind: 'challenge_search',
      intent: 'challenge',
    });
  }
  if (/\b(faster|slower|better|worse|more|less|grew|fell)\b/i.test(claimText)) {
    plans.push({
      query: normalizeWhitespace(`${baseQuery} metric source`),
      source_kind: 'challenge_search',
      intent: 'challenge',
    });
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
  const localEvidenceSearch = typeof options.localEvidenceSearch === 'function' ? options.localEvidenceSearch : null;
  const temporalGraphScorer = typeof options.temporalGraphScorer === 'function' ? options.temporalGraphScorer : null;
  const makeId = typeof options.makeId === 'function' ? options.makeId : ((prefix) => `${prefix}_${nowTs()}_${Math.random().toString(36).slice(2, 10)}`);

  async function searchWithCache(query = '') {
    const normalized = normalizeWhitespace(query);
    if (!normalized || !webSearch) return [];
    const cacheKey = normalizeClaimText(normalized);
    const cached = searchCache.get(cacheKey);
    if (cached && (nowTs() - Number(cached.ts || 0)) < DAY_MS) {
      return cached.results;
    }
    const res = await webSearch({ query: normalized, max_results: MAX_WEB_RESULTS_PER_CLAIM });
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
    const rawClaims = splitClaims(body)
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
      });
    const claims = rawClaims.filter((item) => item.factuality === 'factual');
    const localRefs = Array.isArray(context.scopedRefs) ? context.scopedRefs : [];

    const claimInputs = claims.map((claim) => {
      const searchPlans = buildClaimSearchPlans(claim);
      return {
        claim,
        searchPlans,
        localPromise: localEvidenceSearch ? localEvidenceSearch(buildClaimQuery(claim), localRefs, context.localEvidenceOptions || {}) : Promise.resolve(null),
        webPromise: Promise.all(searchPlans.map(async (plan) => ({
          ...plan,
          results: await searchWithCache(plan.query),
        }))),
      };
    });

    const explicitFetchItems = explicitUrls.slice(0, MAX_FETCHES_PER_NOTE).map((item) => ({
      source_kind: 'explicit_url',
      source_query: '',
      url: item.canonical_url,
      title: '',
    }));

    const claimSearchResults = await Promise.all(claimInputs.map(async (item) => ({
      claim: item.claim,
      localRes: await item.localPromise,
      webRes: await item.webPromise,
    })));

    const searchSources = [];
    claimSearchResults.forEach((entry) => {
      const resultGroups = Array.isArray(entry.webRes) ? entry.webRes : [];
      resultGroups.forEach((group) => {
        const query = String((group && group.query) || '').trim();
        const sourceKind = String((group && group.source_kind) || 'web_search');
        const results = Array.isArray(group && group.results) ? group.results : [];
        results.slice(0, MAX_WEB_RESULTS_PER_CLAIM).forEach((item) => {
          const canonical = normalizeUrl(item && item.url);
          if (!canonical) return;
          searchSources.push({
            source_kind: sourceKind,
            source_query: query,
            url: canonical,
            title: String((item && item.title) || ''),
          });
        });
      });
    });

    const fetchQueue = dedupeBy(explicitFetchItems.concat(searchSources), (item) => `${item.source_kind}:${normalizeUrl(item.url)}`)
      .slice(0, MAX_FETCHES_PER_NOTE);

    const fetchResults = await runPool(fetchQueue, async (item) => {
      const preview = await fetchWithCache(item.url);
      return { item, preview };
    }, MAX_CONCURRENCY);

    const sources = [];
    const passages = [];
    const citations = [];
    const sourceByCanonicalUrl = new Map();

    explicitUrls.forEach((urlItem) => {
      const sourceId = makeId('src');
      const canonical = urlItem.canonical_url;
      if (!sourceByCanonicalUrl.has(canonical)) {
        const source = {
          id: sourceId,
          note_id: noteId,
          analysis_run_id: analysisRunId,
          source_kind: 'explicit_url',
          source_query: '',
          url: urlItem.url,
          canonical_url: canonical,
          title: canonical,
          published_at: 0,
          fetched_at: startedAt,
          content_hash: '',
        };
        sourceByCanonicalUrl.set(canonical, source);
        sources.push(source);
      }
    });

    fetchResults.forEach((entry) => {
      const canonical = normalizeUrl(entry && entry.item && entry.item.url);
      const preview = entry && entry.preview;
      if (!canonical || !preview || !preview.ok) return;
      let source = sourceByCanonicalUrl.get(canonical);
      if (!source) {
        source = {
          id: makeId('src'),
          note_id: noteId,
          analysis_run_id: analysisRunId,
          source_kind: String((entry.item && entry.item.source_kind) || 'web_search'),
          source_query: String((entry.item && entry.item.source_query) || ''),
          url: canonical,
          canonical_url: canonical,
          title: String(preview.title || canonical),
          published_at: 0,
          fetched_at: nowTs(),
          content_hash: buildHash(String(preview.text || preview.markdown || '')),
        };
        sourceByCanonicalUrl.set(canonical, source);
        sources.push(source);
      } else {
        source.title = source.title || String(preview.title || canonical);
        source.fetched_at = Math.max(Number(source.fetched_at || 0), nowTs());
      }
      const text = normalizeWhitespace(String(preview.markdown || preview.text || ''));
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
    });

    claimSearchResults.forEach((entry) => {
      const localRes = entry.localRes;
      const results = Array.isArray(localRes && localRes.results) ? localRes.results : [];
      results.slice(0, 2).forEach((item, idx) => {
        const canonical = normalizeUrl((item && item.url) || '');
        const sourceKey = canonical || `local:${buildHash(String((item && item.source_locator) || (item && item.snippet) || ''))}`;
        let source = sourceByCanonicalUrl.get(sourceKey);
        if (!source) {
          source = {
            id: makeId('src'),
            note_id: noteId,
            analysis_run_id: analysisRunId,
            source_kind: 'local_evidence',
            source_query: buildClaimQuery(entry.claim),
            url: canonical,
            canonical_url: sourceKey,
            title: String((item && item.reference_title) || (item && item.url) || 'Local evidence'),
            published_at: 0,
            fetched_at: startedAt,
            content_hash: buildHash(String((item && item.snippet) || '')),
          };
          sourceByCanonicalUrl.set(sourceKey, source);
          sources.push(source);
        }
        passages.push({
          id: makeId('passage'),
          note_id: noteId,
          analysis_run_id: analysisRunId,
          source_id: source.id,
          passage_index: idx,
          passage_text: normalizeWhitespace(String((item && item.snippet) || '')),
          passage_start: 0,
          passage_end: String((item && item.snippet) || '').length,
          fetched_at: startedAt,
        });
      });
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
      const claimVector = hashEmbedText(String(claim.claim_text || ''));
      const ranked = passageScored
        .map((entry) => {
          const semanticScore = cosineSimilarity(claimVector, entry.passageVector);
          const lexicalScore = lexicalOverlapScore(claim.claim_text, entry.passage.passage_text);
          const timeScore = timeMatchScore(claim.claim_text, entry.passage.passage_text);
          return {
            claim,
            source: entry.source,
            passage: entry.passage,
            semantic_score: semanticScore,
            lexical_score: lexicalScore,
            time_score: timeScore,
          };
        })
        .filter((entry) => entry.source && (entry.semantic_score > 0.18 || entry.lexical_score > 0.12))
        .sort((a, b) => ((b.semantic_score + b.lexical_score) - (a.semantic_score + a.lexical_score)) || a.passage.id.localeCompare(b.passage.id))
        .slice(0, 6);
      const uniqueSourceCount = new Set(ranked.map((item) => String((item.source && item.source.canonical_url) || ''))).size;
      sourceSupportCount.set(claim.id, uniqueSourceCount);
      passageScoresByClaim.set(claim.id, ranked);
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
      const ranked = Array.isArray(passageScoresByClaim.get(claim.id)) ? passageScoresByClaim.get(claim.id) : [];
      const corroborationBase = Math.min(1, (Number(sourceSupportCount.get(claim.id) || 0) / 3));
      const topCitations = ranked
        .map((entry, idx) => {
          const temporalScore = Number(temporalScores.get(String((entry.source && entry.source.id) || '')) || 0) || 0;
          const score = (0.40 * entry.semantic_score)
            + (0.20 * entry.lexical_score)
            + (0.15 * entry.time_score)
            + (0.15 * corroborationBase)
            + (0.10 * temporalScore);
          return {
            id: makeId('citation'),
            note_id: noteId,
            analysis_run_id: analysisRunId,
            claim_id: claim.id,
            source_id: entry.source.id,
            passage_id: entry.passage.id,
            citation_index: idx,
            support_label: 'no_evidence',
            score,
            semantic_score: entry.semantic_score,
            lexical_score: entry.lexical_score,
            time_score: entry.time_score,
            corroboration_score: corroborationBase,
            temporal_score: temporalScore,
            excerpt: entry.passage.passage_text.slice(0, 340),
          };
        })
        .sort((a, b) => (b.score - a.score) || a.id.localeCompare(b.id))
        .slice(0, 3);

      const top = topCitations[0] || null;
      let status = 'no_evidence';
      if (top && top.score >= HIGHLIGHT_THRESHOLD) {
        status = 'supported';
      } else if (top && top.score >= 0.56) {
        status = 'uncertain';
      }
      if (topCitations.length >= 2 && top && top.score >= 0.64 && Math.abs(top.score - topCitations[1].score) <= 0.06) {
        status = 'contested';
      }
      claim.status = status;
      claim.top_score = top ? Number(top.score || 0) : 0;
      claim.highlight_score = claim.top_score;
      topCitations.forEach((item) => {
        item.support_label = status;
        citations.push(item);
      });
    });

    const summary = summarizeAnalysis(claims);
    return {
      ok: true,
      analysis_run_id: analysisRunId,
      note_revision: noteRevision,
      extractor_version: ANALYZER_VERSION,
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
  };
}

module.exports = {
  createNoteAnalysisEngine,
  HIGHLIGHT_THRESHOLD,
};
