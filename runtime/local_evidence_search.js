const fs = require('fs');
const crypto = require('crypto');
const { embedTexts, hashEmbedText, HASH_FALLBACK_MODEL, DEFAULT_EMBEDDING_MODEL } = require('./embedding_runtime');
const {
  ensureReferenceRagIndex,
  readReferenceRagStatus,
  writeReferenceGraphSidecar,
  readReferenceGraphSidecar,
} = require('./rag_index');
const { extractContextTextFromFile } = require('./context_file_support');

const VECTOR_DIM = 256;
const BM25_K1 = 1.2;
const BM25_B = 0.75;
const DEFAULT_TOP_K = 8;
const MAX_TOP_K = 24;
const DEFAULT_GRAPH_TOP_K = 6;
const MAX_GRAPH_TOP_K = 12;
const GRAPH_SEED_LIMIT = 3;
const GRAPH_TERM_LIMIT = 12;
const GRAPH_EDGE_TERM_LIMIT = 8;
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

const GRAPH_STOPWORDS = new Set([
  'about', 'after', 'again', 'against', 'agent', 'all', 'also', 'and', 'any', 'are', 'around',
  'artifact', 'artifacts', 'because', 'been', 'before', 'being', 'between', 'both', 'but', 'can',
  'could', 'context', 'contexts', 'data', 'details', 'document', 'documents', 'during', 'each',
  'evidence', 'file', 'files', 'from', 'further', 'have', 'here', 'highlights', 'into', 'just',
  'local', 'many', 'more', 'most', 'much', 'notes', 'only', 'other', 'over', 'same', 'should',
  'since', 'some', 'such', 'than', 'that', 'their', 'them', 'then', 'there', 'these', 'they',
  'this', 'those', 'through', 'under', 'using', 'very', 'what', 'when', 'where', 'which', 'while',
  'with', 'within', 'work', 'workspace', 'would', 'your',
]);

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

function buildScopedGraphVersionKey(anchorReferenceId = '', scopedRefs = []) {
  const refs = Array.isArray(scopedRefs) ? scopedRefs : [];
  const parts = refs
    .map((ref) => {
      const refId = String((ref && ref.id) || '').trim();
      if (!refId) return '';
      return `${refId}:${buildReferenceVersionKey(ref)}`;
    })
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b));
  return [
    'graph',
    String(anchorReferenceId || '').trim(),
    `refs:${parts.length}`,
    buildRollingHash(parts, 4000),
  ].join('|');
}

function makeSnippet(parts = []) {
  const text = parts
    .map((item) => String(item || '').trim())
    .filter(Boolean)
    .join(' - ');
  return truncate(text, 280);
}

function toTimestamp(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return Math.max(0, Number(fallback) || 0);
  return Math.round(n);
}

function canonicalArtifactLocator(artifactId = '') {
  const id = String(artifactId || '').trim();
  if (!id) return '';
  return `artifact:${id}`;
}

function buildArtifactDocs(ref = {}) {
  const out = [];
  const refId = String((ref && ref.id) || '').trim();
  const refTitle = String((ref && ref.title) || 'Untitled').trim();
  const refUpdatedAt = toTimestamp(ref && ref.updated_at, 0);
  const artifacts = Array.isArray(ref.artifacts) ? ref.artifacts.slice(0, MAX_ARTIFACTS_PER_REF) : [];
  artifacts.forEach((artifact) => {
    const artifactId = String((artifact && artifact.id) || '').trim();
    if (!artifactId) return;
    const artifactTitle = String((artifact && artifact.title) || 'Artifact').trim();
    const artifactUpdatedAt = toTimestamp(artifact && artifact.updated_at, refUpdatedAt);
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
        source_locator: canonicalArtifactLocator(artifactId),
        artifact_id: artifactId,
        marker_backed: false,
        text: summaryOnly,
        snippet: makeSnippet([artifactTitle]),
        source_title: artifactTitle,
        source_updated_at: artifactUpdatedAt,
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
        source_title: artifactTitle,
        source_updated_at: artifactUpdatedAt,
      });
    });
  });
  return out;
}

function buildHighlightDocs(ref = {}) {
  const out = [];
  const refId = String((ref && ref.id) || '').trim();
  const refTitle = String((ref && ref.title) || 'Untitled').trim();
  const refUpdatedAt = toTimestamp(ref && ref.updated_at, 0);
  const highlights = Array.isArray(ref.highlights) ? ref.highlights.slice(-MAX_HIGHLIGHTS_PER_REF) : [];
  highlights.forEach((highlight, idx) => {
    const sourceType = String((highlight && highlight.source) || 'web').trim().toLowerCase() === 'artifact'
      ? 'artifact'
      : 'web';
    const text = String((highlight && highlight.text) || '').trim();
    if (!text) return;
    const highlightUpdatedAt = toTimestamp(highlight && highlight.updated_at, refUpdatedAt);

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
        source_title: text,
        source_updated_at: highlightUpdatedAt,
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
      source_title: text,
      source_updated_at: highlightUpdatedAt,
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
  const refUpdatedAt = toTimestamp(ref && ref.updated_at, 0);
  const contextFiles = Array.isArray(ref.context_files) ? ref.context_files.slice(0, MAX_CONTEXT_FILES_PER_REF) : [];
  contextFiles.forEach((file) => {
    const fileId = String((file && file.id) || '').trim();
    if (!fileId) return;
    const fileUpdatedAt = toTimestamp(file && file.updated_at, refUpdatedAt);
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
        source_title: name,
        source_updated_at: fileUpdatedAt,
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
        source_title: name,
        source_updated_at: fileUpdatedAt,
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

function normalizeEmbeddingRuntime(value) {
  return String(value || '').trim().toLowerCase() === 'lmstudio' ? 'lmstudio' : 'local-hash';
}

function inferSourceKindFromDoc(doc = {}) {
  const sourceKey = String(doc.source_key || '').trim();
  if (sourceKey.startsWith('artifact:')) return 'artifact';
  if (sourceKey.startsWith('context_file:')) return 'context_file';
  if (sourceKey.startsWith('url:')) return 'highlight';
  const rawKind = String(doc.kind || '').trim().toLowerCase();
  return SUPPORTED_KINDS.has(rawKind) ? rawKind : 'highlight';
}

function sourceKindPriority(kind = '') {
  const normalized = String(kind || '').trim().toLowerCase();
  if (normalized === 'artifact') return 3;
  if (normalized === 'context_file') return 2;
  if (normalized === 'highlight') return 1;
  return 0;
}

function canonicalSourceLocatorFromDoc(doc = {}) {
  if (doc.artifact_id) return canonicalArtifactLocator(doc.artifact_id);
  if (doc.url) return String(doc.url || '').trim();
  return String(doc.source_locator || '').trim();
}

function shouldReplaceCanonicalSource(current = null, candidate = {}) {
  if (!current) return true;
  const nextTs = toTimestamp(candidate.source_updated_at, 0);
  if (nextTs !== current.source_updated_at) return nextTs > current.source_updated_at;
  const nextKind = inferSourceKindFromDoc(candidate);
  if (sourceKindPriority(nextKind) !== sourceKindPriority(current.kind)) {
    return sourceKindPriority(nextKind) > sourceKindPriority(current.kind);
  }
  return String(candidate.doc_id || '').localeCompare(String(current.canonical_doc_id || '')) < 0;
}

function collectGraphTerms(text = '') {
  return tokenize(text).filter((token) => token.length >= 3 && !/^\d+$/.test(token) && !GRAPH_STOPWORDS.has(token));
}

function buildScopedSourceGraph(scopedRefs = []) {
  const refs = Array.isArray(scopedRefs) ? scopedRefs : [];
  const sourceMap = new Map();

  refs.forEach((ref) => {
    const refDocs = buildReferenceDocuments(ref);
    const refId = String((ref && ref.id) || '').trim();
    refDocs.forEach((doc) => {
      const sourceKey = String(doc.source_key || '').trim();
      if (!sourceKey) return;
      const sourceUpdatedAt = toTimestamp(doc.source_updated_at, ref && ref.updated_at);
      const nextKind = inferSourceKindFromDoc(doc);
      let entry = sourceMap.get(sourceKey);
      if (!entry) {
        entry = {
          source_key: sourceKey,
          kind: nextKind,
          source_locator: canonicalSourceLocatorFromDoc(doc),
          artifact_id: String(doc.artifact_id || '').trim(),
          context_file_id: String(doc.context_file_id || '').trim(),
          url: String(doc.url || '').trim(),
          reference_id: String(doc.reference_id || '').trim(),
          reference_title: String(doc.reference_title || '').trim(),
          snippet: String(doc.snippet || '').trim(),
          marker_backed: !!doc.marker_backed,
          source_updated_at: sourceUpdatedAt,
          scope_reference_ids: new Set(refId ? [refId] : []),
          term_text_parts: [],
          canonical_doc_id: String(doc.doc_id || '').trim(),
        };
        sourceMap.set(sourceKey, entry);
      }

      if (refId) entry.scope_reference_ids.add(refId);
      entry.marker_backed = entry.marker_backed || !!doc.marker_backed;
      entry.term_text_parts.push(
        String(doc.source_title || '').trim(),
        String(doc.snippet || '').trim(),
        String(doc.text || '').trim(),
      );

      if (shouldReplaceCanonicalSource(entry, doc)) {
        entry.kind = nextKind;
        entry.source_locator = canonicalSourceLocatorFromDoc(doc);
        entry.artifact_id = String(doc.artifact_id || '').trim();
        entry.context_file_id = String(doc.context_file_id || '').trim();
        entry.url = String(doc.url || '').trim();
        entry.reference_id = String(doc.reference_id || '').trim();
        entry.reference_title = String(doc.reference_title || '').trim();
        entry.snippet = String(doc.snippet || '').trim();
        entry.source_updated_at = sourceUpdatedAt;
        entry.canonical_doc_id = String(doc.doc_id || '').trim();
      } else if (sourceUpdatedAt > entry.source_updated_at) {
        entry.source_updated_at = sourceUpdatedAt;
      }
    });
  });

  const nodes = Array.from(sourceMap.values())
    .sort((a, b) => a.source_key.localeCompare(b.source_key));

  const termFreqByNode = new Map();
  const docFreq = new Map();
  nodes.forEach((node) => {
    const combined = node.term_text_parts.join('\n');
    const tf = new Map();
    collectGraphTerms(combined).forEach((token) => {
      tf.set(token, (tf.get(token) || 0) + 1);
    });
    const uniqueTerms = Array.from(tf.keys());
    uniqueTerms.forEach((term) => {
      docFreq.set(term, (docFreq.get(term) || 0) + 1);
    });
    termFreqByNode.set(node.source_key, tf);
  });

  const totalNodes = Math.max(1, nodes.length);
  const idfByTerm = new Map();
  docFreq.forEach((df, term) => {
    idfByTerm.set(term, Math.log(1 + (totalNodes / (1 + Number(df || 0)))));
  });

  const normalizedNodes = nodes.map((node) => {
    const tf = termFreqByNode.get(node.source_key) || new Map();
    const rankedTerms = Array.from(tf.entries())
      .map(([term, count]) => ({
        term,
        weight: (Number(count || 0) || 0) * (Number(idfByTerm.get(term) || 0) || 0),
      }))
      .filter((item) => item.weight > 0)
      .sort((a, b) => (b.weight - a.weight) || a.term.localeCompare(b.term))
      .slice(0, GRAPH_TERM_LIMIT)
      .map((item) => item.term);
    return {
      source_key: node.source_key,
      kind: node.kind,
      source_locator: node.source_locator,
      artifact_id: node.artifact_id,
      context_file_id: node.context_file_id,
      url: node.url,
      reference_id: node.reference_id,
      reference_title: node.reference_title,
      snippet: node.snippet,
      marker_backed: node.marker_backed,
      source_updated_at: node.source_updated_at,
      scope_reference_ids: Array.from(node.scope_reference_ids).sort((a, b) => a.localeCompare(b)),
      terms: rankedTerms,
    };
  });

  const edges = [];
  for (let i = 0; i < normalizedNodes.length; i += 1) {
    const left = normalizedNodes[i];
    const leftTerms = new Set(left.terms);
    for (let j = i + 1; j < normalizedNodes.length; j += 1) {
      const right = normalizedNodes[j];
      const sharedTerms = right.terms
        .filter((term) => leftTerms.has(term))
        .map((term) => ({ term, idf: Number(idfByTerm.get(term) || 0) }))
        .filter((item) => item.idf > 0)
        .sort((a, b) => (b.idf - a.idf) || a.term.localeCompare(b.term))
        .slice(0, GRAPH_EDGE_TERM_LIMIT);
      if (sharedTerms.length === 0) continue;
      edges.push({
        src_key: left.source_key,
        dst_key: right.source_key,
        ts: Math.max(Number(left.source_updated_at || 0), Number(right.source_updated_at || 0)),
        weight: sharedTerms.reduce((sum, item) => sum + Number(item.idf || 0), 0),
        shared_terms: sharedTerms.map((item) => item.term),
      });
    }
  }

  return {
    nodes: normalizedNodes,
    edges,
    idf_by_term: idfByTerm,
  };
}

function buildTemporalGraphRows(graph = {}) {
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph.edges) ? graph.edges : [];
  const rows = [];
  nodes.forEach((node) => {
    rows.push({
      src: String(node.source_key || '').trim(),
      dst: String(node.source_key || '').trim(),
      ts: toTimestamp(node.source_updated_at || 0),
      weight: 0,
      source_key: String(node.source_key || '').trim(),
    });
  });
  edges.forEach((edge) => {
    const src = String(edge.src_key || '').trim();
    const dst = String(edge.dst_key || '').trim();
    if (!src || !dst || src === dst) return;
    const ts = toTimestamp(edge.ts || 0);
    const weight = Number(edge.weight) || 0;
    rows.push({ src, dst, ts, weight, source_key: src });
    rows.push({ src: dst, dst: src, ts, weight, source_key: dst });
  });
  rows.sort((a, b) => (
    a.src.localeCompare(b.src)
    || a.dst.localeCompare(b.dst)
    || (a.ts - b.ts)
    || (a.weight - b.weight)
  ));
  return rows;
}

function graphScoreMapFromResult(scoreResult = {}, defaultComputedAt = 0) {
  const list = Array.isArray(scoreResult && scoreResult.scores) ? scoreResult.scores : [];
  const map = new Map();
  list.forEach((item) => {
    const sourceKey = String(item && item.source_key || '').trim();
    if (!sourceKey) return;
    map.set(sourceKey, {
      source_key: sourceKey,
      global_score: Number(item && item.global_score) || 0,
      recent_30d_score: Number(item && item.recent_30d_score) || 0,
      recent_7d_score: Number(item && item.recent_7d_score) || 0,
      top_neighbors: Array.isArray(item && item.top_neighbors) ? item.top_neighbors : [],
      computed_at: toTimestamp(item && item.computed_at || defaultComputedAt),
    });
  });
  return map;
}

async function ensureScopedGraphSidecar(anchorReferenceId = '', scopedRefs = [], options = {}) {
  const anchorId = String(anchorReferenceId || '').trim();
  const refs = Array.isArray(scopedRefs) ? scopedRefs : [];
  const cfg = (options && typeof options === 'object') ? options : {};
  const userDataPath = String(cfg.userDataPath || '').trim();
  if (!anchorId || !userDataPath) {
    return {
      ok: false,
      graph_state: 'disabled',
      message: 'Graph sidecar path is unavailable.',
    };
  }

  const graphVersion = buildScopedGraphVersionKey(anchorId, refs);
  const forceRebuild = !!cfg.forceGraphRebuild;
  const status = await readReferenceRagStatus({ userDataPath, referenceId: anchorId });
  if (
    !forceRebuild
    && status
    && status.ok
    && String(status.graph_version || '') === graphVersion
    && (String(status.graph_state || '') === 'ready' || String(status.graph_state || '') === 'empty')
  ) {
    return {
      ok: true,
      skipped: true,
      graph_state: String(status.graph_state || ''),
      graph_version: graphVersion,
      graph_node_count: Number(status.graph_node_count || 0),
      graph_edge_count: Number(status.graph_edge_count || 0),
      graph_scored_at: Number(status.graph_scored_at || 0),
      message: String(status.message || ''),
    };
  }

  const graph = buildScopedSourceGraph(refs);
  const now = toTimestamp(cfg.nowTs || Date.now());
  let graphState = graph.nodes.length > 0 ? 'ready' : 'empty';
  let graphMessage = '';
  let graphScoredAt = 0;
  let scores = [];

  if (graph.nodes.length > 0) {
    if (typeof cfg.temporalGraphScorer !== 'function') {
      graphState = 'error';
      graphMessage = 'Temporal graph scorer is unavailable.';
    } else {
      const scoreRes = await cfg.temporalGraphScorer({
        rows: buildTemporalGraphRows(graph),
        now_ts: now,
      });
      if (!scoreRes || scoreRes.ok === false) {
        graphState = 'error';
        graphMessage = String((scoreRes && scoreRes.message) || 'Temporal graph scoring failed.');
      } else {
        const scoreMap = graphScoreMapFromResult(scoreRes, now);
        scores = graph.nodes.map((node) => scoreMap.get(node.source_key) || {
          source_key: node.source_key,
          global_score: 0,
          recent_30d_score: 0,
          recent_7d_score: 0,
          top_neighbors: [],
          computed_at: now,
        });
        graphScoredAt = Math.max(
          now,
          ...scores.map((item) => Number((item && item.computed_at) || 0)),
        );
      }
    }
  }

  const writeRes = await writeReferenceGraphSidecar({
    userDataPath,
    referenceId: anchorId,
    graphVersion,
    graphState,
    graphMessage,
    graphScoredAt,
    nodes: graph.nodes,
    edges: graph.edges,
    scores,
  });

  return {
    ok: !!(writeRes && writeRes.ok) && graphState !== 'error',
    ...(writeRes || {}),
    graph_state: graphState,
    graph_version: graphVersion,
    graph_scored_at: graphScoredAt,
    message: graphMessage || String((writeRes && writeRes.message) || ''),
  };
}

function buildResultFromDoc(doc = {}, row = {}, citationIds = new Map(), rank = 1) {
  const sourceKey = String(doc.source_key || '').trim();
  return {
    rank,
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
}

function buildResultFromGraphNode(node = {}, options = {}) {
  const opts = (options && typeof options === 'object') ? options : {};
  return {
    rank: Number(opts.rank || 0),
    source_id: String(opts.source_id || ''),
    kind: String(node.kind || '').trim(),
    reference_id: String(node.reference_id || '').trim(),
    reference_title: String(node.reference_title || '').trim(),
    snippet: String(node.snippet || '').trim(),
    source_locator: String(node.source_locator || '').trim(),
    source_key: String(node.source_key || '').trim(),
    marker_backed: !!node.marker_backed,
    url: String(node.url || '').trim(),
    artifact_id: String(node.artifact_id || '').trim(),
    context_file_id: String(node.context_file_id || '').trim(),
    score: round4(opts.score || 0),
    bm25_score: round4(opts.bm25_score || 0),
    semantic_score: round4(opts.semantic_score || 0),
  };
}

function buildAdjacency(edges = []) {
  const adjacency = new Map();
  const list = Array.isArray(edges) ? edges : [];
  list.forEach((edge) => {
    const src = String(edge.src_key || '').trim();
    const dst = String(edge.dst_key || '').trim();
    if (!src || !dst || src === dst) return;
    const record = {
      neighbor: dst,
      ts: Number(edge.ts || 0),
      weight: Number(edge.weight || 0),
      shared_terms: Array.isArray(edge.shared_terms) ? edge.shared_terms.map((item) => String(item || '').trim()).filter(Boolean) : [],
    };
    const reverse = { ...record, neighbor: src };
    if (!adjacency.has(src)) adjacency.set(src, []);
    if (!adjacency.has(dst)) adjacency.set(dst, []);
    adjacency.get(src).push(record);
    adjacency.get(dst).push(reverse);
  });
  adjacency.forEach((items) => {
    items.sort((a, b) => (b.weight - a.weight) || a.neighbor.localeCompare(b.neighbor));
  });
  return adjacency;
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
  const requestedRuntime = normalizeEmbeddingRuntime(options.embeddingRuntime || embeddingConfig.runtime);
  const requestedModel = requestedRuntime === 'lmstudio'
    ? (String(options.embeddingModel || embeddingConfig.model || DEFAULT_EMBEDDING_MODEL).trim() || DEFAULT_EMBEDDING_MODEL)
    : HASH_FALLBACK_MODEL;

  const queryEmbeddingRes = requestedRuntime === 'lmstudio'
    ? await embedTexts([q], {
      ...embeddingConfig,
      model: requestedModel,
    })
    : {
      ok: true,
      embeddings: [hashEmbedText(q)],
      runtime: 'local-hash',
      model: HASH_FALLBACK_MODEL,
      fallback_used: true,
    };

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

  let graphSync = null;
  if (ragEnabled && userDataPath && String(options.anchorReferenceId || '').trim()) {
    graphSync = await ensureScopedGraphSidecar(String(options.anchorReferenceId || '').trim(), refs, options);
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
      graph_state: String((graphSync && graphSync.graph_state) || ''),
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

  const results = topRows.map((row, idx) => buildResultFromDoc(row.doc || {}, row, citationIds, idx + 1));

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
    graph_state: String((graphSync && graphSync.graph_state) || ''),
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
  const requestedRuntime = normalizeEmbeddingRuntime(cfg.embeddingRuntime || embeddingConfig.runtime);
  const requestedModel = requestedRuntime === 'lmstudio'
    ? (String(cfg.embeddingModel || embeddingConfig.model || DEFAULT_EMBEDDING_MODEL).trim() || DEFAULT_EMBEDDING_MODEL)
    : HASH_FALLBACK_MODEL;
  const probeRes = requestedRuntime === 'lmstudio'
    ? await embedTexts(['subgrapher rag health check'], {
      ...embeddingConfig,
      model: requestedModel,
    })
    : {
      ok: true,
      embeddings: [hashEmbedText('subgrapher rag health check')],
      runtime: 'local-hash',
      model: HASH_FALLBACK_MODEL,
      fallback_used: true,
    };
  const modelId = String((probeRes && probeRes.model) || HASH_FALLBACK_MODEL).trim() || HASH_FALLBACK_MODEL;
  const embeddingRuntime = String((probeRes && probeRes.runtime) || 'local-hash').trim() || 'local-hash';

  const cached = getReferenceIndex(targetRef);
  const index = cached && cached.index ? cached.index : null;
  let ragRes = {
    ok: true,
    index_state: 'empty',
    model_id: modelId,
    embedding_runtime: embeddingRuntime,
    doc_count: 0,
  };

  if (index && Array.isArray(index.rows) && index.rows.length > 0) {
    ragRes = await ensureReferenceRagIndex({
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
  }

  const graphRes = await ensureScopedGraphSidecar(refId, refs, {
    ...cfg,
    userDataPath,
    forceGraphRebuild: true,
  });

  const parts = [];
  if (ragRes && ragRes.ok) {
    parts.push(`RAG index rebuilt (${Number(ragRes.doc_count || 0)} docs).`);
  } else {
    parts.push(String((ragRes && ragRes.message) || 'RAG reindex failed.'));
  }
  if (graphRes && graphRes.graph_state === 'ready') {
    parts.push(`Graph sidecar rebuilt (${Number(graphRes.graph_node_count || 0)} nodes, ${Number(graphRes.graph_edge_count || 0)} edges).`);
  } else if (graphRes && graphRes.graph_state === 'empty') {
    parts.push('Graph sidecar rebuilt (empty scope graph).');
  } else if (graphRes && graphRes.message) {
    parts.push(`Graph sidecar error: ${graphRes.message}`);
  }

  return {
    ok: !!(ragRes && ragRes.ok) && !!(graphRes && graphRes.ok !== false),
    message: parts.join(' '),
    ...(ragRes || {}),
    graph_state: String((graphRes && graphRes.graph_state) || ''),
    graph_version: String((graphRes && graphRes.graph_version) || ''),
    graph_node_count: Number((graphRes && graphRes.graph_node_count) || 0),
    graph_edge_count: Number((graphRes && graphRes.graph_edge_count) || 0),
    graph_scored_at: Number((graphRes && graphRes.graph_scored_at) || 0),
  };
}

async function expandLocalEvidenceGraph(query, scopedRefs = [], options = {}) {
  const q = String(query || '').trim();
  if (!q) return { ok: false, message: 'query is required.' };

  const refs = Array.isArray(scopedRefs) ? scopedRefs : [];
  const cfg = (options && typeof options === 'object') ? options : {};
  const ragEnabled = Object.prototype.hasOwnProperty.call(cfg, 'ragEnabled') ? !!cfg.ragEnabled : true;
  if (!ragEnabled) return { ok: false, message: 'RAG is disabled in settings.' };

  const anchorReferenceId = String(cfg.anchorReferenceId || cfg.referenceId || '').trim();
  const userDataPath = String(cfg.userDataPath || '').trim();
  if (!anchorReferenceId || !userDataPath) {
    return { ok: false, message: 'Graph sidecar is unavailable for this reference.' };
  }

  const includeKinds = normalizeKinds(cfg.includeKinds || cfg.include_kinds);
  const topK = clamp(Number(cfg.topK || cfg.top_k || DEFAULT_GRAPH_TOP_K), 1, MAX_GRAPH_TOP_K);

  const graphRes = await ensureScopedGraphSidecar(anchorReferenceId, refs, cfg);
  if (String((graphRes && graphRes.graph_state) || '') === 'error') {
    return {
      ok: false,
      message: String((graphRes && graphRes.message) || 'Temporal graph sidecar is unavailable.'),
      graph_state: 'error',
      seed_results: [],
      expanded_results: [],
      graph_signals: {},
    };
  }

  const graphSidecar = await readReferenceGraphSidecar({ userDataPath, referenceId: anchorReferenceId });
  if (!graphSidecar || graphSidecar.ok === false) {
    return {
      ok: false,
      message: String((graphSidecar && graphSidecar.message) || 'Unable to read graph sidecar.'),
      graph_state: 'error',
      seed_results: [],
      expanded_results: [],
      graph_signals: {},
    };
  }

  const nodeMap = new Map((Array.isArray(graphSidecar.nodes) ? graphSidecar.nodes : []).map((node) => [String(node.source_key || '').trim(), node]));
  const scoreMap = new Map((Array.isArray(graphSidecar.scores) ? graphSidecar.scores : []).map((item) => [String(item.source_key || '').trim(), item]));
  const adjacency = buildAdjacency(graphSidecar.edges);
  const graphState = String(graphSidecar.graph_state || '').trim() || 'missing';

  if (nodeMap.size === 0) {
    return {
      ok: true,
      query: q,
      graph_state: graphState,
      seed_results: [],
      expanded_results: [],
      graph_signals: {},
    };
  }

  const explicitSeeds = Array.isArray(cfg.seed_source_keys)
    ? cfg.seed_source_keys.map((item) => String(item || '').trim()).filter(Boolean)
    : [];

  let seedResults = [];
  const seedScoreMap = new Map();
  let candidateSeedKeys = [];

  if (explicitSeeds.length > 0) {
    const seen = new Set();
    candidateSeedKeys = explicitSeeds.filter((item) => {
      if (seen.has(item)) return false;
      seen.add(item);
      return true;
    });
    seedResults = candidateSeedKeys
      .map((sourceKey, idx) => {
        const node = nodeMap.get(sourceKey);
        if (!node) return null;
        return buildResultFromGraphNode(node, { rank: idx + 1, score: 0 });
      })
      .filter(Boolean);
  } else {
    const searchRes = await searchLocalEvidence(q, refs, {
      ...cfg,
      topK: Math.max(DEFAULT_TOP_K, topK),
      includeKinds: Array.from(includeKinds),
      anchorReferenceId,
      userDataPath,
    });
    const seen = new Set();
    const seeds = [];
    const rankedResults = Array.isArray(searchRes && searchRes.results) ? searchRes.results : [];
    rankedResults.forEach((item) => {
      const sourceKey = String((item && item.source_key) || '').trim();
      if (!sourceKey || seen.has(sourceKey)) return;
      seen.add(sourceKey);
      seeds.push(item);
    });
    seedResults = seeds.slice(0, GRAPH_SEED_LIMIT);
    seedResults.forEach((item) => {
      seedScoreMap.set(String(item.source_key || '').trim(), Number(item.score || 0) || 0);
    });
    candidateSeedKeys = seedResults.map((item) => String(item.source_key || '').trim());
  }

  const validSeeds = candidateSeedKeys
    .filter((sourceKey) => nodeMap.has(sourceKey))
    .filter((sourceKey) => {
      const node = nodeMap.get(sourceKey);
      return includeKinds.has(String((node && node.kind) || '').trim().toLowerCase());
    });
  const seedSet = new Set(validSeeds);

  if (validSeeds.length === 0) {
    return {
      ok: true,
      query: q,
      graph_state: graphState,
      seed_results: seedResults,
      expanded_results: [],
      graph_signals: {},
    };
  }

  const candidateMap = new Map();
  validSeeds.forEach((seedKey) => {
    const neighbors = adjacency.get(seedKey) || [];
    neighbors.forEach((edge) => {
      const neighborKey = String(edge.neighbor || '').trim();
      if (!neighborKey || seedSet.has(neighborKey)) return;
      const node = nodeMap.get(neighborKey);
      if (!node) return;
      if (!includeKinds.has(String(node.kind || '').trim().toLowerCase())) return;
      let entry = candidateMap.get(neighborKey);
      if (!entry) {
        entry = {
          node,
          neighbor_of: new Set(),
          shared_terms: new Set(),
          edge_weight: 0,
          seed_score: 0,
        };
        candidateMap.set(neighborKey, entry);
      }
      entry.neighbor_of.add(seedKey);
      edge.shared_terms.forEach((term) => entry.shared_terms.add(term));
      entry.edge_weight += Number(edge.weight || 0);
      entry.seed_score = Math.max(entry.seed_score, Number(seedScoreMap.get(seedKey) || 0));
    });
  });

  if (candidateMap.size === 0) {
    return {
      ok: true,
      query: q,
      graph_state: graphState,
      seed_results: seedResults,
      expanded_results: [],
      graph_signals: {},
    };
  }

  let maxEdgeWeight = 0;
  candidateMap.forEach((entry) => {
    if (entry.edge_weight > maxEdgeWeight) maxEdgeWeight = entry.edge_weight;
  });

  const ranked = Array.from(candidateMap.entries())
    .map(([sourceKey, entry]) => {
      const scoreRow = scoreMap.get(sourceKey) || {};
      const globalScore = Number(scoreRow.global_score || 0);
      const recent30 = Number(scoreRow.recent_30d_score || 0);
      const recent7 = Number(scoreRow.recent_7d_score || 0);
      const recentScore = (0.7 * recent7) + (0.3 * recent30);
      const edgeScore = maxEdgeWeight > 0 ? Math.min(1, entry.edge_weight / maxEdgeWeight) : 0;
      const finalScore = (0.45 * edgeScore) + (0.25 * recentScore) + (0.20 * globalScore) + (0.10 * entry.seed_score);
      return {
        source_key: sourceKey,
        node: entry.node,
        neighbor_of: Array.from(entry.neighbor_of).sort((a, b) => a.localeCompare(b)),
        shared_terms: Array.from(entry.shared_terms).sort((a, b) => a.localeCompare(b)),
        edge_weight: entry.edge_weight,
        global_score: globalScore,
        recent_30d_score: recent30,
        recent_7d_score: recent7,
        source_updated_at: Number((entry.node && entry.node.source_updated_at) || 0),
        final_score: finalScore,
      };
    })
    .sort((a, b) => (
      (b.final_score - a.final_score)
      || (Number(b.source_updated_at || 0) - Number(a.source_updated_at || 0))
      || a.source_key.localeCompare(b.source_key)
    ))
    .slice(0, topK);

  const expandedResults = ranked.map((item, idx) => buildResultFromGraphNode(item.node, {
    rank: idx + 1,
    score: item.final_score,
  }));
  const graphSignals = {};
  ranked.forEach((item) => {
    graphSignals[item.source_key] = {
      neighbor_of: item.neighbor_of,
      shared_terms: item.shared_terms,
      edge_weight: round4(item.edge_weight),
      global_score: round4(item.global_score),
      recent_30d_score: round4(item.recent_30d_score),
      recent_7d_score: round4(item.recent_7d_score),
      source_updated_at: Number(item.source_updated_at || 0),
    };
  });

  return {
    ok: true,
    query: q,
    graph_state: graphState,
    seed_results: seedResults,
    expanded_results: expandedResults,
    graph_signals: graphSignals,
  };
}

module.exports = {
  searchLocalEvidence,
  expandLocalEvidenceGraph,
  getLocalEvidenceRagStatus,
  reindexLocalEvidenceReference,
  __private: {
    buildReferenceDocuments,
    buildReferenceVersionKey,
    buildScopedGraphVersionKey,
    buildScopedSourceGraph,
    buildTemporalGraphRows,
  },
};
