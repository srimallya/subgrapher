const fs = require('fs');
const crypto = require('crypto');
const { embedTexts, hashEmbedText, HASH_FALLBACK_MODEL, DEFAULT_EMBEDDING_MODEL } = require('./embedding_runtime');
const { ensureReferenceRagIndex, readReferenceRagStatus } = require('./rag_index');
const { extractContextTextFromFile } = require('./context_file_support');

const VECTOR_DIM = 256;
const BM25_K1 = 1.2;
const BM25_B = 0.75;
const DEFAULT_TOP_K = 8;
const MAX_TOP_K = 24;
const MAX_CACHE_REFS = 64;

const ARTIFACT_CHUNK_SIZE = 900;
const ARTIFACT_CHUNK_OVERLAP = 180;
const ARTIFACT_MAX_CHUNKS_PER_DOC = 8;
const CONTEXT_CHUNK_SIZE = 900;
const CONTEXT_CHUNK_OVERLAP = 180;
const CONTEXT_MAX_CHUNKS_PER_DOC = 8;

const MAX_ARTIFACTS_PER_REF = 80;
const MAX_HIGHLIGHTS_PER_REF = 240;
const MAX_CONTEXT_FILES_PER_REF = 80;
const MAX_CONTEXT_FILE_READ_CHARS = 80_000;

const SUPPORTED_KINDS = new Set(['artifact', 'highlight', 'context_file']);
const METHOD_HASH = 'hybrid:bm25+local-hash-embedding-v1';
const METHOD_LM = 'hybrid:bm25+lmstudio-embedding-v1';

const referenceIndexCache = new Map();

function clamp(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  if (n < min) return min;
  if (n > max) return max;
  return n;
}

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/https?:\/\/[^\s]+/g, ' ')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenize(value) {
  const normalized = normalizeText(value);
  if (!normalized) return [];
  return normalized.split(' ').filter(Boolean);
}

function hashTokenToIndex(token) {
  const digest = crypto.createHash('sha256').update(String(token || '')).digest();
  return digest.readUInt16BE(0) % VECTOR_DIM;
}

function embedTokens(tokens = []) {
  const vec = new Float32Array(VECTOR_DIM);
  const list = Array.isArray(tokens) ? tokens : [];
  list.forEach((token) => {
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
    dot += Number(vecA[i] || 0) * Number(vecB[i] || 0);
  }
  if (!Number.isFinite(dot)) return 0;
  if (dot < 0) return 0;
  if (dot > 1) return 1;
  return dot;
}

function truncate(value, maxLen = 240) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  if (text.length <= maxLen) return text;
  return `${text.slice(0, Math.max(1, maxLen - 3))}...`;
}

function chunkText(value, size, overlap, maxChunks) {
  const text = String(value || '').trim();
  if (!text) return [];
  const chunkSize = Math.max(200, Number(size) || 900);
  const chunkOverlap = Math.max(0, Math.min(chunkSize - 1, Number(overlap) || 0));
  const limit = Math.max(1, Number(maxChunks) || 1);
  const out = [];
  let start = 0;
  while (start < text.length && out.length < limit) {
    const end = Math.min(text.length, start + chunkSize);
    const chunk = text.slice(start, end).trim();
    if (chunk) out.push({ chunk, start, end });
    if (end >= text.length) break;
    start = Math.max(start + 1, end - chunkOverlap);
  }
  return out;
}

function buildRollingHash(parts = [], maxItems = 400) {
  const hash = crypto.createHash('sha256');
  const list = Array.isArray(parts) ? parts : [];
  const limit = Math.max(1, Number(maxItems) || 1);
  list.slice(0, limit).forEach((part) => {
    hash.update(String(part || ''));
    hash.update('|');
  });
  return hash.digest('hex').slice(0, 16);
}

function buildReferenceVersionKey(ref = {}) {
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts : [];
  const highlights = Array.isArray(ref.highlights) ? ref.highlights : [];
  const contextFiles = Array.isArray(ref.context_files) ? ref.context_files : [];

  const artifactHash = buildRollingHash(
    artifacts.map((item) => [
      String((item && item.id) || ''),
      String((item && item.updated_at) || ''),
      String((item && item.title) || ''),
      String((item && item.content_hash) || ''),
      String((item && item.content && String(item.content).length) || 0),
    ].join(':')),
    500,
  );

  const highlightHash = buildRollingHash(
    highlights.map((item) => [
      String((item && item.id) || ''),
      String((item && item.updated_at) || ''),
      String((item && item.source) || ''),
      String((item && item.url) || ''),
      String((item && item.artifact_id) || ''),
      truncate((item && item.text) || '', 120),
    ].join(':')),
    800,
  );

  const contextHash = buildRollingHash(
    contextFiles.map((item) => [
      String((item && item.id) || ''),
      String((item && item.updated_at) || ''),
      String((item && item.content_hash) || ''),
      String((item && item.size_bytes) || ''),
      String((item && item.relative_path) || ''),
    ].join(':')),
    500,
  );

  return [
    String((ref && ref.id) || ''),
    String((ref && ref.updated_at) || ''),
    `a:${artifacts.length}:${artifactHash}`,
    `h:${highlights.length}:${highlightHash}`,
    `c:${contextFiles.length}:${contextHash}`,
  ].join('|');
}

function makeSnippet(parts = []) {
  const text = parts
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .join(' - ');
  return truncate(text, 280);
}

function buildArtifactDocs(ref = {}) {
  const out = [];
  const refId = String((ref && ref.id) || '').trim();
  const refTitle = String((ref && ref.title) || 'Untitled').trim();
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts.slice(0, MAX_ARTIFACTS_PER_REF) : [];
  artifacts.forEach((artifact) => {
    const artifactId = String((artifact && artifact.id) || '').trim();
    if (!artifactId) return;
    const artifactTitle = String((artifact && artifact.title) || 'Artifact').trim();
    const content = String((artifact && artifact.content) || '');
    const chunks = chunkText(content, ARTIFACT_CHUNK_SIZE, ARTIFACT_CHUNK_OVERLAP, ARTIFACT_MAX_CHUNKS_PER_DOC);
    if (chunks.length === 0) {
      const summaryOnly = truncate(artifactTitle, 260);
      if (!summaryOnly) return;
      out.push({
        doc_id: `artifact:${refId}:${artifactId}:0`,
        kind: 'artifact',
        reference_id: refId,
        reference_title: refTitle,
        source_key: `artifact:${refId}:${artifactId}`,
        source_locator: `artifact:${artifactId}`,
        artifact_id: artifactId,
        marker_backed: false,
        text: summaryOnly,
        snippet: makeSnippet([artifactTitle]),
      });
      return;
    }
    chunks.forEach((piece, idx) => {
      const body = String(piece.chunk || '').trim();
      if (!body) return;
      out.push({
        doc_id: `artifact:${refId}:${artifactId}:${idx}`,
        kind: 'artifact',
        reference_id: refId,
        reference_title: refTitle,
        source_key: `artifact:${refId}:${artifactId}`,
        source_locator: `artifact:${artifactId}#char:${piece.start}-${piece.end}`,
        artifact_id: artifactId,
        marker_backed: false,
        text: `${artifactTitle}\n${body}`,
        snippet: makeSnippet([artifactTitle, body]),
      });
    });
  });
  return out;
}

function buildHighlightDocs(ref = {}) {
  const out = [];
  const refId = String((ref && ref.id) || '').trim();
  const refTitle = String((ref && ref.title) || 'Untitled').trim();
  const highlights = Array.isArray(ref.highlights) ? ref.highlights.slice(-MAX_HIGHLIGHTS_PER_REF) : [];
  highlights.forEach((highlight, idx) => {
    const sourceType = String((highlight && highlight.source) || 'web').trim().toLowerCase() === 'artifact'
      ? 'artifact'
      : 'web';
    const text = String((highlight && highlight.text) || '').trim();
    if (!text) return;

    if (sourceType === 'artifact') {
      const artifactId = String((highlight && highlight.artifact_id) || '').trim();
      if (!artifactId) return;
      const start = Number((highlight && highlight.artifact_start) || 0);
      const end = Number((highlight && highlight.artifact_end) || 0);
      out.push({
        doc_id: `highlight:${refId}:artifact:${artifactId}:${idx}`,
        kind: 'highlight',
        reference_id: refId,
        reference_title: refTitle,
        source_key: `artifact:${refId}:${artifactId}`,
        source_locator: `artifact:${artifactId}#char:${Math.max(0, Math.round(start))}-${Math.max(0, Math.round(end))}`,
        artifact_id: artifactId,
        marker_backed: true,
        text,
        snippet: makeSnippet([text, `artifact:${artifactId}`]),
      });
      return;
    }

    const url = String((highlight && (highlight.url_norm || highlight.url)) || '').trim();
    if (!url) return;
    const before = String((highlight && highlight.context_before) || '').trim();
    const after = String((highlight && highlight.context_after) || '').trim();
    out.push({
      doc_id: `highlight:${refId}:web:${idx}`,
      kind: 'highlight',
      reference_id: refId,
      reference_title: refTitle,
      source_key: `url:${url}`,
      source_locator: url,
      url,
      marker_backed: true,
      text: `${before} ${text} ${after}`.trim(),
      snippet: makeSnippet([text, url]),
    });
  });
  return out;
}

function readContextFileContent(file = {}) {
  const storedPath = String((file && file.stored_path) || '').trim();
  if (!storedPath) return '';
  try {
    if (!fs.existsSync(storedPath)) return '';
    const extracted = extractContextTextFromFile(storedPath, {
      filePath: storedPath,
      filename: String((file && file.original_name) || (file && file.relative_path) || '').trim(),
      mimeType: String((file && file.mime_type) || '').trim(),
      maxChars: MAX_CONTEXT_FILE_READ_CHARS,
    });
    const raw = String((extracted && extracted.text) || '').trim();
    if (!raw) return '';
    return raw.slice(0, MAX_CONTEXT_FILE_READ_CHARS);
  } catch (_) {
    return '';
  }
}

function buildContextFileDocs(ref = {}) {
  const out = [];
  const refId = String((ref && ref.id) || '').trim();
  const refTitle = String((ref && ref.title) || 'Untitled').trim();
  const contextFiles = Array.isArray(ref.context_files) ? ref.context_files.slice(0, MAX_CONTEXT_FILES_PER_REF) : [];
  contextFiles.forEach((file) => {
    const fileId = String((file && file.id) || '').trim();
    if (!fileId) return;
    const name = String((file && file.original_name) || (file && file.relative_path) || 'context.txt').trim();
    const summary = String((file && file.summary) || '').trim();
    const content = readContextFileContent(file);
    const baseText = [name, summary, content].filter(Boolean).join('\n');
    const chunks = chunkText(baseText, CONTEXT_CHUNK_SIZE, CONTEXT_CHUNK_OVERLAP, CONTEXT_MAX_CHUNKS_PER_DOC);
    if (chunks.length === 0) {
      const text = [name, summary].filter(Boolean).join(' ');
      if (!text.trim()) return;
      out.push({
        doc_id: `context_file:${refId}:${fileId}:0`,
        kind: 'context_file',
        reference_id: refId,
        reference_title: refTitle,
        source_key: `context_file:${refId}:${fileId}`,
        source_locator: `context_file:${fileId}:${name}`,
        context_file_id: fileId,
        marker_backed: false,
        text,
        snippet: makeSnippet([name, summary]),
      });
      return;
    }

    chunks.forEach((piece, idx) => {
      const body = String(piece.chunk || '').trim();
      if (!body) return;
      out.push({
        doc_id: `context_file:${refId}:${fileId}:${idx}`,
        kind: 'context_file',
        reference_id: refId,
        reference_title: refTitle,
        source_key: `context_file:${refId}:${fileId}`,
        source_locator: `context_file:${fileId}:${name}`,
        context_file_id: fileId,
        marker_backed: false,
        text: `${name}\n${body}`,
        snippet: makeSnippet([name, body]),
      });
    });
  });
  return out;
}

function buildReferenceDocuments(ref = {}) {
  return [
    ...buildArtifactDocs(ref),
    ...buildHighlightDocs(ref),
    ...buildContextFileDocs(ref),
  ];
}

function buildReferenceIndex(ref = {}) {
  const docs = buildReferenceDocuments(ref);
  const rows = [];
  const docFreq = new Map();

  docs.forEach((doc) => {
    const tokens = tokenize(doc.text);
    if (tokens.length === 0) return;
    const tf = new Map();
    tokens.forEach((token) => tf.set(token, (tf.get(token) || 0) + 1));
    tf.forEach((_count, token) => {
      docFreq.set(token, (docFreq.get(token) || 0) + 1);
    });
    rows.push({
      doc,
      tf,
      token_count: tokens.length,
      embedding: embedTokens(tokens),
    });
  });

  const avgDocLen = rows.length > 0
    ? rows.reduce((sum, row) => sum + Number(row.token_count || 0), 0) / rows.length
    : 0;

  return {
    docs_count: rows.length,
    rows,
    doc_freq: docFreq,
    avg_doc_len: avgDocLen,
  };
}

function touchCache(refId, entry) {
  const key = String(refId || '').trim();
  if (!key) return;
  if (referenceIndexCache.has(key)) referenceIndexCache.delete(key);
  referenceIndexCache.set(key, entry);
  while (referenceIndexCache.size > MAX_CACHE_REFS) {
    const first = referenceIndexCache.keys().next();
    if (first && !first.done) {
      referenceIndexCache.delete(first.value);
    } else {
      break;
    }
  }
}

function getReferenceIndex(ref = {}) {
  const refId = String((ref && ref.id) || '').trim();
  if (!refId) return { version: '', index: { docs_count: 0, rows: [], doc_freq: new Map(), avg_doc_len: 0 } };
  const nextVersion = buildReferenceVersionKey(ref);
  const existing = referenceIndexCache.get(refId);
  if (existing && existing.version === nextVersion) {
    touchCache(refId, existing);
    return existing;
  }
  const built = {
    version: nextVersion,
    index: buildReferenceIndex(ref),
  };
  touchCache(refId, built);
  return built;
}

function normalizeKinds(includeKinds) {
  const list = Array.isArray(includeKinds) ? includeKinds : [];
  const normalized = new Set();
  list.forEach((item) => {
    const kind = String(item || '').trim().toLowerCase();
    if (SUPPORTED_KINDS.has(kind)) normalized.add(kind);
  });
  if (normalized.size === 0) {
    return new Set(SUPPORTED_KINDS);
  }
  return normalized;
}

function bm25Score(queryTokens, row, docFreq, totalDocs, avgDocLen) {
  if (!Array.isArray(queryTokens) || queryTokens.length === 0) return 0;
  if (!row || !row.tf || !Number.isFinite(totalDocs) || totalDocs <= 0) return 0;
  const tfMap = row.tf;
  const len = Math.max(1, Number(row.token_count) || 1);
  const avgLen = Math.max(1, Number(avgDocLen) || 1);
  let score = 0;
  const dedup = new Set(queryTokens);
  dedup.forEach((token) => {
    const tf = Number(tfMap.get(token) || 0);
    if (tf <= 0) return;
    const df = Number(docFreq.get(token) || 0);
    const idf = Math.log(1 + ((totalDocs - df + 0.5) / (df + 0.5)));
    const num = tf * (BM25_K1 + 1);
    const den = tf + BM25_K1 * (1 - BM25_B + BM25_B * (len / avgLen));
    if (den <= 0) return;
    score += idf * (num / den);
  });
  return score;
}

function round4(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  return Number(n.toFixed(4));
}

function normalizeVector(input = []) {
  if (input instanceof Float32Array) return input;
  if (!Array.isArray(input)) return new Float32Array(0);
  return Float32Array.from(input.map((item) => Number(item) || 0));
}

async function searchLocalEvidence(query, scopedRefs = [], options = {}) {
  const q = String(query || '').trim();
  const refs = Array.isArray(scopedRefs) ? scopedRefs : [];
  const topK = clamp(Number(options.topK || options.top_k || DEFAULT_TOP_K), 1, MAX_TOP_K);
  if (!q) {
    return {
      ok: true,
      method: METHOD_HASH,
      results: [],
      citations: [],
      embedding_runtime: 'local-hash',
      embedding_model: HASH_FALLBACK_MODEL,
      fallback_used: true,
      index_state: 'idle',
    };
  }

  const includeKinds = normalizeKinds(options.includeKinds || options.include_kinds);
  const queryTokens = tokenize(q);
  const queryHashEmbedding = hashEmbedText(q);

  const ragEnabled = Object.prototype.hasOwnProperty.call(options, 'ragEnabled') ? !!options.ragEnabled : true;
  const userDataPath = String(options.userDataPath || '').trim();
  const embeddingConfig = (options.embeddingConfig && typeof options.embeddingConfig === 'object')
    ? options.embeddingConfig
    : {};
  const requestedModel = String(options.embeddingModel || embeddingConfig.model || DEFAULT_EMBEDDING_MODEL).trim() || DEFAULT_EMBEDDING_MODEL;

  const queryEmbeddingRes = await embedTexts([q], {
    ...embeddingConfig,
    model: requestedModel,
  });

  const queryEmbedding = normalizeVector((queryEmbeddingRes && queryEmbeddingRes.embeddings && queryEmbeddingRes.embeddings[0]) || []);
  const embeddingRuntime = String((queryEmbeddingRes && queryEmbeddingRes.runtime) || 'local-hash').trim() || 'local-hash';
  const embeddingModel = String((queryEmbeddingRes && queryEmbeddingRes.model) || HASH_FALLBACK_MODEL).trim() || HASH_FALLBACK_MODEL;
  const fallbackUsed = !!(queryEmbeddingRes && queryEmbeddingRes.fallback_used);

  const scored = [];
  const ragIndexStates = [];

  for (let r = 0; r < refs.length; r += 1) {
    const ref = refs[r];
    const cached = getReferenceIndex(ref);
    const index = cached && cached.index ? cached.index : null;
    if (!index || !Array.isArray(index.rows) || index.rows.length === 0) continue;

    const totalDocs = Number(index.docs_count || 0);
    if (totalDocs <= 0) continue;

    let vectorByDoc = new Map();
    if (ragEnabled && userDataPath) {
      const ragRes = await ensureReferenceRagIndex({
        userDataPath,
        referenceId: String((ref && ref.id) || '').trim(),
        referenceVersion: cached.version,
        docs: index.rows.map((row) => row.doc),
        modelId: embeddingModel,
        embeddingRuntime,
        embeddingConfig: {
          ...embeddingConfig,
          model: embeddingModel,
        },
      });
      ragIndexStates.push(String((ragRes && ragRes.index_state) || 'error').trim() || 'error');
      const sameModel = String((ragRes && ragRes.model_id) || '').trim() === embeddingModel;
      if (ragRes && ragRes.ok && sameModel && ragRes.vector_by_doc instanceof Map) {
        vectorByDoc = ragRes.vector_by_doc;
      }
    }

    for (let i = 0; i < index.rows.length; i += 1) {
      const row = index.rows[i];
      if (!row || !row.doc) continue;
      const doc = row.doc;
      if (!includeKinds.has(String(doc.kind || '').trim().toLowerCase())) continue;
      const bm25 = bm25Score(queryTokens, row, index.doc_freq, totalDocs, index.avg_doc_len);
      const semanticVector = vectorByDoc.get(String(doc.doc_id || '').trim()) || row.embedding;
      const semanticQuery = semanticVector === row.embedding ? queryHashEmbedding : queryEmbedding;
      const semantic = cosineSimilarity(semanticQuery, semanticVector);
      scored.push({
        doc,
        bm25,
        semantic,
      });
    }
  }

  if (scored.length === 0) {
    return {
      ok: true,
      method: embeddingRuntime === 'lmstudio' && embeddingModel !== HASH_FALLBACK_MODEL ? METHOD_LM : METHOD_HASH,
      results: [],
      citations: [],
      cache_size: referenceIndexCache.size,
      embedding_runtime: embeddingRuntime,
      embedding_model: embeddingModel,
      fallback_used: fallbackUsed,
      index_state: ragEnabled ? (ragIndexStates.includes('error') ? 'error' : 'empty') : 'disabled',
    };
  }

  let maxBm25 = 0;
  scored.forEach((row) => {
    if (row.bm25 > maxBm25) maxBm25 = row.bm25;
  });

  const ranked = scored
    .map((row) => {
      const bm25Norm = maxBm25 > 0 ? Math.min(1, row.bm25 / maxBm25) : 0;
      const semanticNorm = Math.max(0, Math.min(1, row.semantic));
      const finalScore = (0.55 * bm25Norm) + (0.45 * semanticNorm);
      return {
        ...row,
        bm25_norm: bm25Norm,
        final_score: finalScore,
      };
    })
    .sort((a, b) => (
      (b.final_score - a.final_score)
      || (b.bm25_norm - a.bm25_norm)
      || (b.semantic - a.semantic)
      || String((a.doc && a.doc.doc_id) || '').localeCompare(String((b.doc && b.doc.doc_id) || ''))
    ));

  const topRows = ranked.slice(0, topK);
  const citationMap = new Map();
  topRows.forEach((row) => {
    const doc = row.doc || {};
    const sourceKey = String(doc.source_key || '').trim();
    if (!sourceKey || citationMap.has(sourceKey)) return;
    citationMap.set(sourceKey, {
      source_key: sourceKey,
      source_locator: String(doc.source_locator || '').trim(),
      kind: String(doc.kind || '').trim(),
      reference_id: String(doc.reference_id || '').trim(),
      reference_title: String(doc.reference_title || '').trim(),
      marker_backed: !!doc.marker_backed,
      url: String(doc.url || '').trim(),
      artifact_id: String(doc.artifact_id || '').trim(),
      context_file_id: String(doc.context_file_id || '').trim(),
    });
  });

  const citationIds = new Map();
  const citations = Array.from(citationMap.values()).map((item, idx) => {
    const sourceId = `S${idx + 1}`;
    citationIds.set(item.source_key, sourceId);
    return {
      source_id: sourceId,
      ...item,
    };
  });

  const results = topRows.map((row, idx) => {
    const doc = row.doc || {};
    const sourceKey = String(doc.source_key || '').trim();
    return {
      rank: idx + 1,
      source_id: citationIds.get(sourceKey) || '',
      kind: String(doc.kind || '').trim(),
      reference_id: String(doc.reference_id || '').trim(),
      reference_title: String(doc.reference_title || '').trim(),
      snippet: String(doc.snippet || '').trim(),
      source_locator: String(doc.source_locator || '').trim(),
      source_key: sourceKey,
      marker_backed: !!doc.marker_backed,
      url: String(doc.url || '').trim(),
      artifact_id: String(doc.artifact_id || '').trim(),
      context_file_id: String(doc.context_file_id || '').trim(),
      score: round4(row.final_score),
      bm25_score: round4(row.bm25_norm),
      semantic_score: round4(row.semantic),
    };
  });

  return {
    ok: true,
    method: embeddingRuntime === 'lmstudio' && embeddingModel !== HASH_FALLBACK_MODEL ? METHOD_LM : METHOD_HASH,
    results,
    citations,
    cache_size: referenceIndexCache.size,
    embedding_runtime: embeddingRuntime,
    embedding_model: embeddingModel,
    fallback_used: fallbackUsed,
    index_state: ragEnabled
      ? (ragIndexStates.includes('error') ? 'error' : (ragIndexStates.includes('ready') ? 'ready' : 'empty'))
      : 'disabled',
  };
}

async function getLocalEvidenceRagStatus(referenceId = '', options = {}) {
  const cfg = (options && typeof options === 'object') ? options : {};
  const refId = String(referenceId || '').trim();
  if (!refId) {
    return { ok: false, state: 'error', message: 'referenceId is required.' };
  }
  const userDataPath = String(cfg.userDataPath || '').trim();
  return readReferenceRagStatus({ userDataPath, referenceId: refId });
}

async function reindexLocalEvidenceReference(referenceId = '', scopedRefs = [], options = {}) {
  const refId = String(referenceId || '').trim();
  if (!refId) return { ok: false, message: 'referenceId is required.' };
  const refs = Array.isArray(scopedRefs) ? scopedRefs : [];
  const targetRef = refs.find((ref) => String((ref && ref.id) || '').trim() === refId);
  if (!targetRef) return { ok: false, message: 'Reference not found in scope.' };

  const cfg = (options && typeof options === 'object') ? options : {};
  const userDataPath = String(cfg.userDataPath || '').trim();
  const ragEnabled = Object.prototype.hasOwnProperty.call(cfg, 'ragEnabled') ? !!cfg.ragEnabled : true;
  if (!ragEnabled) {
    return { ok: false, message: 'RAG is disabled in settings.' };
  }

  const embeddingConfig = (cfg.embeddingConfig && typeof cfg.embeddingConfig === 'object')
    ? cfg.embeddingConfig
    : {};
  const requestedModel = String(cfg.embeddingModel || embeddingConfig.model || DEFAULT_EMBEDDING_MODEL).trim() || DEFAULT_EMBEDDING_MODEL;
  const probeRes = await embedTexts(['subgrapher rag health check'], {
    ...embeddingConfig,
    model: requestedModel,
  });
  const modelId = String((probeRes && probeRes.model) || HASH_FALLBACK_MODEL).trim() || HASH_FALLBACK_MODEL;
  const embeddingRuntime = String((probeRes && probeRes.runtime) || 'local-hash').trim() || 'local-hash';

  const cached = getReferenceIndex(targetRef);
  const index = cached && cached.index ? cached.index : null;
  if (!index || !Array.isArray(index.rows) || index.rows.length === 0) {
    return {
      ok: true,
      message: 'No local evidence documents to index.',
      index_state: 'empty',
      model_id: modelId,
      embedding_runtime: embeddingRuntime,
      doc_count: 0,
    };
  }

  const ragRes = await ensureReferenceRagIndex({
    userDataPath,
    referenceId: refId,
    referenceVersion: cached.version,
    docs: index.rows.map((row) => row.doc),
    modelId,
    embeddingRuntime,
    embeddingConfig: {
      ...embeddingConfig,
      model: modelId,
    },
    forceRebuild: true,
  });

  return {
    ok: !!(ragRes && ragRes.ok),
    message: ragRes && ragRes.ok
      ? `RAG index rebuilt (${Number(ragRes.doc_count || 0)} docs).`
      : String((ragRes && ragRes.message) || 'RAG reindex failed.'),
    ...(ragRes || {}),
  };
}

module.exports = {
  searchLocalEvidence,
  getLocalEvidenceRagStatus,
  reindexLocalEvidenceReference,
};
