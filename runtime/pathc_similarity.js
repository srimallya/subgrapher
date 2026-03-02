const crypto = require('crypto');

const VECTOR_DIM = 256;

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n <= 0) return 0;
  if (n >= 1) return 1;
  return n;
}

function keywordScore(query, targetText) {
  const q = normalizeText(query);
  const t = normalizeText(targetText);
  if (!q || !t) return 0;
  const queryTokens = q.split(' ').filter(Boolean);
  const targetTokens = new Set(t.split(' ').filter(Boolean));
  if (queryTokens.length === 0 || targetTokens.size === 0) return 0;
  let tokenHits = 0;
  queryTokens.forEach((token) => {
    if (targetTokens.has(token)) tokenHits += 1;
  });
  const tokenScore = tokenHits / queryTokens.length;
  const phraseScore = t.includes(q) ? 1 : 0;
  return clamp01((0.7 * tokenScore) + (0.3 * phraseScore));
}

function buildReferenceSearchText(ref) {
  if (!ref || typeof ref !== 'object') return '';
  const parts = [];
  parts.push(String(ref.title || ''));
  parts.push(String(ref.intent || ''));
  if (Array.isArray(ref.tags)) parts.push(ref.tags.join(' '));
  if (Array.isArray(ref.tabs)) {
    ref.tabs.slice(0, 40).forEach((tab) => {
      parts.push(String((tab && tab.title) || ''));
      parts.push(String((tab && tab.url) || ''));
      parts.push(String((tab && tab.excerpt) || ''));
    });
  }
  if (Array.isArray(ref.artifacts)) {
    ref.artifacts.slice(0, 60).forEach((artifact) => {
      parts.push(String((artifact && artifact.title) || ''));
      parts.push(String((artifact && artifact.content) || '').slice(0, 1400));
    });
  }
  if (Array.isArray(ref.context_files)) {
    ref.context_files.slice(0, 80).forEach((file) => {
      parts.push(String((file && file.original_name) || (file && file.relative_path) || ''));
      parts.push(String((file && file.summary) || '').slice(0, 700));
    });
  }
  if (Array.isArray(ref.highlights)) {
    ref.highlights.slice(-120).forEach((item) => {
      parts.push(String((item && item.text) || '').slice(0, 320));
      parts.push(String((item && item.context_before) || '').slice(0, 120));
      parts.push(String((item && item.context_after) || '').slice(0, 120));
    });
  }
  return parts.join(' ').replace(/\s+/g, ' ').trim();
}

function hashTokenToIndex(token) {
  const h = crypto.createHash('sha256').update(token).digest();
  return h.readUInt16BE(0) % VECTOR_DIM;
}

function embedText(text) {
  const vec = new Float32Array(VECTOR_DIM);
  const normalized = normalizeText(text);
  if (!normalized) return vec;
  const tokens = normalized.split(' ').filter(Boolean);
  tokens.forEach((token) => {
    const idx = hashTokenToIndex(token);
    vec[idx] += 1;
  });
  let norm = 0;
  for (let i = 0; i < vec.length; i += 1) {
    norm += vec[i] * vec[i];
  }
  norm = Math.sqrt(norm);
  if (norm > 0) {
    for (let i = 0; i < vec.length; i += 1) {
      vec[i] /= norm;
    }
  }
  return vec;
}

function cosineSimilarity(vecA, vecB) {
  if (!vecA || !vecB || vecA.length !== vecB.length) return 0;
  let dot = 0;
  for (let i = 0; i < vecA.length; i += 1) {
    dot += vecA[i] * vecB[i];
  }
  return clamp01(dot);
}

function scoreReferencesHybrid(query, refs, options = {}) {
  const list = Array.isArray(refs) ? refs : [];
  const topK = Math.max(1, Math.min(200, Number(options.topK) || 60));
  const q = String(query || '').trim();
  if (!q) return { ok: true, method: 'none', results: [] };

  const queryVec = embedText(q);
  const rows = list.map((ref) => {
    const srId = String((ref && ref.id) || '').trim();
    const text = buildReferenceSearchText(ref);
    const keyword = keywordScore(q, text);
    const semantic = cosineSimilarity(queryVec, embedText(text));
    const score = clamp01((0.7 * semantic) + (0.3 * keyword));
    return {
      sr_id: srId,
      score,
      keyword_score: keyword,
      semantic_score: semantic,
    };
  }).filter((row) => !!row.sr_id);

  const minScore = 0.08;
  const results = rows
    .filter((row) => row.score >= minScore || row.keyword_score >= 0.2 || row.semantic_score >= 0.2)
    .sort((a, b) => (b.score - a.score) || (b.semantic_score - a.semantic_score) || (b.keyword_score - a.keyword_score))
    .slice(0, topK)
    .map((row) => ({
      sr_id: row.sr_id,
      score: Number(row.score.toFixed(4)),
      keyword_score: Number(row.keyword_score.toFixed(4)),
      semantic_score: Number(row.semantic_score.toFixed(4)),
    }));

  return {
    ok: true,
    method: 'hybrid:local-hash-embedding-v1',
    results,
  };
}

module.exports = {
  buildReferenceSearchText,
  scoreReferencesHybrid,
};
