const crypto = require('crypto');

const DEFAULT_LMSTUDIO_BASE_URL = 'http://127.0.0.1:1234';
const DEFAULT_EMBEDDING_MODEL = 'text-embedding-nomic-embed-text-v1.5';
const HASH_FALLBACK_MODEL = 'hybrid:local-hash-embedding-v1';
const HASH_VECTOR_DIM = 256;

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

function hashTokenToIndex(token, dim = HASH_VECTOR_DIM) {
  const digest = crypto.createHash('sha256').update(String(token || '')).digest();
  return digest.readUInt16BE(0) % Math.max(8, Number(dim) || HASH_VECTOR_DIM);
}

function embedTokensHash(tokens = [], dim = HASH_VECTOR_DIM) {
  const safeDim = Math.max(8, Number(dim) || HASH_VECTOR_DIM);
  const vec = new Float32Array(safeDim);
  const list = Array.isArray(tokens) ? tokens : [];
  list.forEach((token) => {
    const idx = hashTokenToIndex(token, safeDim);
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

function hashEmbedText(text, dim = HASH_VECTOR_DIM) {
  return embedTokensHash(tokenize(text), dim);
}

function toNumberVector(input) {
  if (!Array.isArray(input)) return [];
  const out = [];
  for (let i = 0; i < input.length; i += 1) {
    const n = Number(input[i]);
    if (!Number.isFinite(n)) return [];
    out.push(n);
  }
  return out;
}

function normalizeBaseUrl(value, fallback = DEFAULT_LMSTUDIO_BASE_URL) {
  const raw = String(value || '').trim() || String(fallback || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    if (!/^https?:$/i.test(parsed.protocol)) return '';
    parsed.hash = '';
    parsed.search = '';
    return parsed.toString().replace(/\/+$/, '');
  } catch (_) {
    return '';
  }
}

function fallbackEmbeddings(texts = []) {
  const safeTexts = Array.isArray(texts) ? texts : [];
  const vectors = safeTexts.map((text) => hashEmbedText(text, HASH_VECTOR_DIM));
  return {
    ok: true,
    embeddings: vectors,
    dim: HASH_VECTOR_DIM,
    runtime: 'local-hash',
    model: HASH_FALLBACK_MODEL,
    fallback_used: true,
  };
}

async function embedTexts(texts = [], options = {}) {
  const list = Array.isArray(texts) ? texts.map((item) => String(item || '')) : [];
  if (list.length === 0) {
    return {
      ok: true,
      embeddings: [],
      dim: 0,
      runtime: 'none',
      model: '',
      fallback_used: false,
    };
  }

  const cfg = (options && typeof options === 'object') ? options : {};
  const model = String(cfg.model || DEFAULT_EMBEDDING_MODEL).trim() || DEFAULT_EMBEDDING_MODEL;
  const baseUrl = normalizeBaseUrl(cfg.baseUrl, DEFAULT_LMSTUDIO_BASE_URL);
  const apiKey = String(cfg.apiKey || '').trim();
  const timeoutMsRaw = Number(cfg.timeoutMs);
  const timeoutMs = Number.isFinite(timeoutMsRaw) ? Math.max(2_000, Math.min(60_000, Math.round(timeoutMsRaw))) : 20_000;

  if (!baseUrl) {
    const fallback = fallbackEmbeddings(list);
    return {
      ...fallback,
      message: 'LM Studio embeddings URL is not configured.',
    };
  }

  try {
    const controller = typeof AbortController === 'function' ? new AbortController() : null;
    let timer = null;
    if (controller) {
      timer = setTimeout(() => {
        try {
          controller.abort();
        } catch (_) {
          // noop
        }
      }, timeoutMs);
    }
    const headers = {
      'Content-Type': 'application/json',
    };
    if (apiKey) headers.Authorization = `Bearer ${apiKey}`;
    const response = await fetch(`${baseUrl}/v1/embeddings`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        model,
        input: list,
      }),
      signal: controller ? controller.signal : undefined,
    });
    if (timer) clearTimeout(timer);
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      const detail = String((body && (body.error && (body.error.message || body.error) || body.message)) || '').trim();
      throw new Error(detail || `LM Studio embeddings request failed (${response.status}).`);
    }
    const data = Array.isArray(body && body.data) ? body.data : [];
    if (data.length !== list.length) {
      throw new Error(`Embeddings response count mismatch (${data.length}/${list.length}).`);
    }
    const parsed = data.map((item) => toNumberVector(item && item.embedding));
    const dim = parsed.length > 0 ? Number(parsed[0].length || 0) : 0;
    if (!dim) {
      throw new Error('Embeddings endpoint returned empty vectors.');
    }
    for (let i = 0; i < parsed.length; i += 1) {
      if (parsed[i].length !== dim) {
        throw new Error('Embeddings endpoint returned inconsistent vector dimensions.');
      }
    }
    return {
      ok: true,
      embeddings: parsed,
      dim,
      runtime: 'lmstudio',
      model,
      fallback_used: false,
    };
  } catch (err) {
    const fallback = fallbackEmbeddings(list);
    return {
      ...fallback,
      message: String((err && err.message) || 'LM Studio embeddings failed.'),
    };
  }
}

module.exports = {
  DEFAULT_EMBEDDING_MODEL,
  HASH_FALLBACK_MODEL,
  HASH_VECTOR_DIM,
  hashEmbedText,
  embedTexts,
};
