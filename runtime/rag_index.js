const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { embedTexts, hashEmbedText, HASH_FALLBACK_MODEL } = require('./embedding_runtime');

const SCHEMA_VERSION = 1;
const DEFAULT_BATCH_SIZE = 24;
const dbTaskByPath = new Map();

let sqlModulePromise = null;

function nowTs() {
  return Date.now();
}

function safeRefSegment(referenceId = '') {
  return String(referenceId || '').trim().replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 160);
}

function getRagIndexPath(userDataPath = '', referenceId = '') {
  const safeRef = safeRefSegment(referenceId);
  if (!safeRef || !userDataPath) return '';
  return path.join(userDataPath, 'semantic_references', safeRef, 'rag', 'index.sqlite');
}

function ensureDirForFile(filePath = '') {
  if (!filePath) return;
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}

function hashDocument(doc = {}) {
  const hash = crypto.createHash('sha256');
  [
    String(doc.doc_id || ''),
    String(doc.kind || ''),
    String(doc.source_key || ''),
    String(doc.source_locator || ''),
    String(doc.artifact_id || ''),
    String(doc.context_file_id || ''),
    String(doc.url || ''),
    doc.marker_backed ? '1' : '0',
    String(doc.snippet || ''),
    String(doc.text || ''),
  ].forEach((part) => {
    hash.update(part);
    hash.update('|');
  });
  return hash.digest('hex');
}

function encodeVector(vector = []) {
  const arr = Array.isArray(vector)
    ? Float32Array.from(vector.map((item) => Number(item) || 0))
    : (vector instanceof Float32Array ? vector : new Float32Array(0));
  return Buffer.from(arr.buffer, arr.byteOffset, arr.byteLength);
}

function decodeVector(blob) {
  if (!blob) return new Float32Array(0);
  const view = Buffer.isBuffer(blob)
    ? blob
    : Buffer.from(blob);
  if (!view.byteLength || view.byteLength % 4 !== 0) return new Float32Array(0);
  return new Float32Array(view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength));
}

function toDocRows(referenceId = '', docs = []) {
  const refId = String(referenceId || '').trim();
  const list = Array.isArray(docs) ? docs : [];
  return list
    .map((doc) => {
      const row = (doc && typeof doc === 'object') ? doc : {};
      const docId = String(row.doc_id || '').trim();
      if (!docId) return null;
      const normalized = {
        doc_id: docId,
        reference_id: refId,
        kind: String(row.kind || '').trim(),
        source_key: String(row.source_key || '').trim(),
        source_locator: String(row.source_locator || '').trim(),
        artifact_id: String(row.artifact_id || '').trim(),
        context_file_id: String(row.context_file_id || '').trim(),
        url: String(row.url || '').trim(),
        marker_backed: !!row.marker_backed,
        snippet: String(row.snippet || '').trim(),
        text: String(row.text || ''),
      };
      normalized.content_hash = hashDocument(normalized);
      return normalized;
    })
    .filter(Boolean);
}

function readMeta(db) {
  const out = {};
  const result = db.exec('SELECT key, value FROM meta');
  const rows = result && result[0] && Array.isArray(result[0].values) ? result[0].values : [];
  rows.forEach((pair) => {
    const key = String((pair && pair[0]) || '').trim();
    if (!key) return;
    out[key] = String((pair && pair[1]) || '');
  });
  return out;
}

function upsertMeta(db, key, value) {
  const stmt = db.prepare('INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value');
  stmt.bind([String(key || ''), String(value || '')]);
  stmt.step();
  stmt.free();
}

async function getSqlModule() {
  if (!sqlModulePromise) {
    const initSqlJs = require('sql.js');
    const wasmPath = require.resolve('sql.js/dist/sql-wasm.wasm');
    const wasmDir = path.dirname(wasmPath);
    sqlModulePromise = initSqlJs({
      locateFile: (fileName) => path.join(wasmDir, fileName),
    });
  }
  return sqlModulePromise;
}

function ensureSchema(db) {
  db.run('PRAGMA foreign_keys = ON');
  db.run('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)');
  const meta = readMeta(db);
  const existingVersion = Number(meta.schema_version || 0);
  if (existingVersion && existingVersion !== SCHEMA_VERSION) {
    db.run('DROP TABLE IF EXISTS embeddings');
    db.run('DROP TABLE IF EXISTS documents');
    db.run('DELETE FROM meta');
  }
  db.run(`
    CREATE TABLE IF NOT EXISTS documents (
      doc_id TEXT PRIMARY KEY,
      reference_id TEXT NOT NULL,
      kind TEXT,
      source_key TEXT,
      source_locator TEXT,
      artifact_id TEXT,
      context_file_id TEXT,
      url TEXT,
      marker_backed INTEGER NOT NULL DEFAULT 0,
      snippet TEXT,
      text TEXT,
      content_hash TEXT,
      updated_at INTEGER NOT NULL
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS embeddings (
      doc_id TEXT NOT NULL,
      model_id TEXT NOT NULL,
      dim INTEGER NOT NULL,
      vector_blob BLOB NOT NULL,
      updated_at INTEGER NOT NULL,
      PRIMARY KEY (doc_id, model_id),
      FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
    );
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_documents_reference ON documents(reference_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_embeddings_model ON embeddings(model_id)');
  upsertMeta(db, 'schema_version', String(SCHEMA_VERSION));
}

function withDbLock(dbPath, taskFn) {
  const key = String(dbPath || '').trim();
  const prior = dbTaskByPath.get(key) || Promise.resolve();
  const next = prior
    .catch(() => {})
    .then(() => taskFn())
    .finally(() => {
      if (dbTaskByPath.get(key) === next) {
        dbTaskByPath.delete(key);
      }
    });
  dbTaskByPath.set(key, next);
  return next;
}

async function openDatabase(dbPath) {
  const SQL = await getSqlModule();
  if (fs.existsSync(dbPath)) {
    const bytes = fs.readFileSync(dbPath);
    return new SQL.Database(bytes);
  }
  return new SQL.Database();
}

function saveDatabase(db, dbPath) {
  ensureDirForFile(dbPath);
  const bytes = db.export();
  fs.writeFileSync(dbPath, Buffer.from(bytes));
}

function queryExistingDocs(db, referenceId = '') {
  const stmt = db.prepare('SELECT doc_id, content_hash FROM documents WHERE reference_id = ?');
  stmt.bind([String(referenceId || '')]);
  const map = new Map();
  while (stmt.step()) {
    const row = stmt.getAsObject();
    const docId = String(row.doc_id || '').trim();
    if (!docId) continue;
    map.set(docId, String(row.content_hash || ''));
  }
  stmt.free();
  return map;
}

function queryEmbeddedDocIds(db, modelId = '', docIds = []) {
  const set = new Set();
  if (!docIds.length) return set;
  const stmt = db.prepare('SELECT doc_id FROM embeddings WHERE model_id = ? AND doc_id = ?');
  for (let i = 0; i < docIds.length; i += 1) {
    stmt.bind([String(modelId || ''), String(docIds[i] || '')]);
    if (stmt.step()) {
      const row = stmt.getAsObject();
      const docId = String(row.doc_id || '').trim();
      if (docId) set.add(docId);
    }
    stmt.reset();
  }
  stmt.free();
  return set;
}

function insertOrUpdateDocs(db, docs = []) {
  const ts = nowTs();
  const stmt = db.prepare(`
    INSERT INTO documents (
      doc_id, reference_id, kind, source_key, source_locator, artifact_id,
      context_file_id, url, marker_backed, snippet, text, content_hash, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(doc_id) DO UPDATE SET
      reference_id = excluded.reference_id,
      kind = excluded.kind,
      source_key = excluded.source_key,
      source_locator = excluded.source_locator,
      artifact_id = excluded.artifact_id,
      context_file_id = excluded.context_file_id,
      url = excluded.url,
      marker_backed = excluded.marker_backed,
      snippet = excluded.snippet,
      text = excluded.text,
      content_hash = excluded.content_hash,
      updated_at = excluded.updated_at
  `);
  for (let i = 0; i < docs.length; i += 1) {
    const doc = docs[i];
    stmt.bind([
      doc.doc_id,
      doc.reference_id,
      doc.kind,
      doc.source_key,
      doc.source_locator,
      doc.artifact_id,
      doc.context_file_id,
      doc.url,
      doc.marker_backed ? 1 : 0,
      doc.snippet,
      doc.text,
      doc.content_hash,
      ts,
    ]);
    stmt.step();
    stmt.reset();
  }
  stmt.free();
}

function removeMissingDocs(db, referenceId = '', docIds = []) {
  const existingStmt = db.prepare('SELECT doc_id FROM documents WHERE reference_id = ?');
  existingStmt.bind([String(referenceId || '')]);
  const keep = new Set(docIds.map((item) => String(item || '').trim()).filter(Boolean));
  const remove = [];
  while (existingStmt.step()) {
    const row = existingStmt.getAsObject();
    const docId = String(row.doc_id || '').trim();
    if (!docId) continue;
    if (!keep.has(docId)) remove.push(docId);
  }
  existingStmt.free();
  if (!remove.length) return 0;
  const delEmb = db.prepare('DELETE FROM embeddings WHERE doc_id = ?');
  const delDoc = db.prepare('DELETE FROM documents WHERE doc_id = ?');
  remove.forEach((docId) => {
    delEmb.bind([docId]);
    delEmb.step();
    delEmb.reset();
    delDoc.bind([docId]);
    delDoc.step();
    delDoc.reset();
  });
  delEmb.free();
  delDoc.free();
  return remove.length;
}

function upsertEmbeddings(db, modelId = '', vectorsByDoc = new Map()) {
  const ts = nowTs();
  const stmt = db.prepare(`
    INSERT INTO embeddings (doc_id, model_id, dim, vector_blob, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(doc_id, model_id) DO UPDATE SET
      dim = excluded.dim,
      vector_blob = excluded.vector_blob,
      updated_at = excluded.updated_at
  `);
  vectorsByDoc.forEach((vector, docId) => {
    const vec = Array.isArray(vector) ? vector : Array.from(vector || []);
    stmt.bind([
      String(docId || ''),
      String(modelId || ''),
      Math.max(0, Number(vec.length || 0)),
      encodeVector(vec),
      ts,
    ]);
    stmt.step();
    stmt.reset();
  });
  stmt.free();
}

function loadVectors(db, modelId = '', docIds = []) {
  const out = new Map();
  if (!docIds.length) return out;
  const stmt = db.prepare('SELECT doc_id, vector_blob FROM embeddings WHERE model_id = ? AND doc_id = ?');
  docIds.forEach((docId) => {
    stmt.bind([String(modelId || ''), String(docId || '')]);
    if (stmt.step()) {
      const row = stmt.getAsObject();
      const foundDocId = String(row.doc_id || '').trim();
      if (foundDocId) {
        out.set(foundDocId, decodeVector(row.vector_blob));
      }
    }
    stmt.reset();
  });
  stmt.free();
  return out;
}

async function buildVectorsForDocs(docs = [], options = {}) {
  const list = Array.isArray(docs) ? docs : [];
  const targetModel = String(options.modelId || '').trim() || HASH_FALLBACK_MODEL;
  const runtime = String(options.embeddingRuntime || '').trim().toLowerCase();
  const batchSize = Math.max(1, Math.min(64, Math.round(Number(options.batchSize) || DEFAULT_BATCH_SIZE)));

  if (runtime !== 'lmstudio' || targetModel === HASH_FALLBACK_MODEL) {
    const vectors = new Map();
    list.forEach((doc) => {
      vectors.set(doc.doc_id, hashEmbedText(doc.text));
    });
    return {
      ok: true,
      model_id: HASH_FALLBACK_MODEL,
      embedding_runtime: 'local-hash',
      fallback_used: true,
      vectors,
    };
  }

  const vectors = new Map();
  for (let i = 0; i < list.length; i += batchSize) {
    const batch = list.slice(i, i + batchSize);
    const texts = batch.map((doc) => String(doc.text || ''));
    const embRes = await embedTexts(texts, {
      ...(options.embeddingConfig || {}),
      model: targetModel,
    });
    const embList = Array.isArray(embRes && embRes.embeddings) ? embRes.embeddings : [];
    if (!embRes || !embRes.ok || embList.length !== batch.length || embRes.runtime !== 'lmstudio') {
      const fallbackVectors = new Map();
      list.forEach((doc) => {
        fallbackVectors.set(doc.doc_id, hashEmbedText(doc.text));
      });
      return {
        ok: true,
        model_id: HASH_FALLBACK_MODEL,
        embedding_runtime: 'local-hash',
        fallback_used: true,
        message: String((embRes && embRes.message) || 'LM Studio embedding failed during indexing.'),
        vectors: fallbackVectors,
      };
    }
    for (let j = 0; j < batch.length; j += 1) {
      vectors.set(batch[j].doc_id, embList[j]);
    }
  }

  return {
    ok: true,
    model_id: targetModel,
    embedding_runtime: 'lmstudio',
    fallback_used: false,
    vectors,
  };
}

async function ensureReferenceRagIndex(options = {}) {
  const cfg = (options && typeof options === 'object') ? options : {};
  const userDataPath = String(cfg.userDataPath || '').trim();
  const referenceId = String(cfg.referenceId || '').trim();
  const dbPath = getRagIndexPath(userDataPath, referenceId);
  if (!dbPath || !referenceId) {
    return {
      ok: false,
      index_state: 'disabled',
      message: 'RAG index path is unavailable.',
      vector_by_doc: new Map(),
      model_id: '',
      embedding_runtime: 'none',
      fallback_used: true,
      db_path: dbPath,
    };
  }

  return withDbLock(dbPath, async () => {
    const docs = toDocRows(referenceId, cfg.docs);
    const referenceVersion = String(cfg.referenceVersion || '').trim();
    const targetModel = String(cfg.modelId || '').trim() || HASH_FALLBACK_MODEL;
    const forceRebuild = !!cfg.forceRebuild;

    let db = null;
    try {
      db = await openDatabase(dbPath);
      ensureSchema(db);

      if (forceRebuild) {
        const delEmb = db.prepare('DELETE FROM embeddings WHERE doc_id IN (SELECT doc_id FROM documents WHERE reference_id = ?)');
        delEmb.bind([referenceId]);
        delEmb.step();
        delEmb.free();
        const delDocs = db.prepare('DELETE FROM documents WHERE reference_id = ?');
        delDocs.bind([referenceId]);
        delDocs.step();
        delDocs.free();
      }

      const existing = queryExistingDocs(db, referenceId);
      const docIds = docs.map((doc) => doc.doc_id);
      const removedCount = removeMissingDocs(db, referenceId, docIds);

      const changedDocs = [];
      docs.forEach((doc) => {
        const prevHash = String(existing.get(doc.doc_id) || '');
        if (!prevHash || prevHash !== doc.content_hash || forceRebuild) {
          changedDocs.push(doc);
        }
      });

      if (changedDocs.length > 0) {
        insertOrUpdateDocs(db, changedDocs);
      }

      const embedded = queryEmbeddedDocIds(db, targetModel, docIds);
      const missingDocs = docs.filter((doc) => !embedded.has(doc.doc_id) || changedDocs.some((changed) => changed.doc_id === doc.doc_id));

      let modelUsed = targetModel;
      let runtimeUsed = String(cfg.embeddingRuntime || '').trim() || 'none';
      let fallbackUsed = runtimeUsed !== 'lmstudio';
      let indexMessage = '';

      if (missingDocs.length > 0) {
        const vectorBuild = await buildVectorsForDocs(missingDocs, {
          embeddingRuntime: cfg.embeddingRuntime,
          modelId: targetModel,
          embeddingConfig: cfg.embeddingConfig,
          batchSize: cfg.batchSize,
        });
        modelUsed = String(vectorBuild.model_id || targetModel).trim() || targetModel;
        runtimeUsed = String(vectorBuild.embedding_runtime || runtimeUsed).trim() || runtimeUsed;
        fallbackUsed = !!vectorBuild.fallback_used;
        indexMessage = String(vectorBuild.message || '').trim();

        if (modelUsed !== targetModel) {
          const staleStmt = db.prepare('DELETE FROM embeddings WHERE doc_id = ? AND model_id = ?');
          missingDocs.forEach((doc) => {
            staleStmt.bind([doc.doc_id, targetModel]);
            staleStmt.step();
            staleStmt.reset();
          });
          staleStmt.free();
        }
        upsertEmbeddings(db, modelUsed, vectorBuild.vectors);
      }

      upsertMeta(db, 'reference_id', referenceId);
      upsertMeta(db, 'reference_version', referenceVersion);
      upsertMeta(db, 'active_model_id', modelUsed);
      upsertMeta(db, 'embedding_runtime', runtimeUsed);
      upsertMeta(db, 'updated_at', String(nowTs()));
      upsertMeta(db, 'doc_count', String(docs.length));

      const vectorByDoc = loadVectors(db, modelUsed, docIds);
      saveDatabase(db, dbPath);

      return {
        ok: true,
        index_state: 'ready',
        db_path: dbPath,
        reference_id: referenceId,
        reference_version: referenceVersion,
        model_id: modelUsed,
        embedding_runtime: runtimeUsed,
        fallback_used: fallbackUsed,
        vector_by_doc: vectorByDoc,
        doc_count: docs.length,
        changed_docs: changedDocs.length,
        removed_docs: removedCount,
        message: indexMessage,
      };
    } catch (err) {
      return {
        ok: false,
        index_state: 'error',
        db_path: dbPath,
        reference_id: referenceId,
        vector_by_doc: new Map(),
        model_id: targetModel,
        embedding_runtime: String(cfg.embeddingRuntime || '').trim() || 'none',
        fallback_used: true,
        message: String((err && err.message) || 'RAG index update failed.'),
      };
    } finally {
      if (db) {
        try {
          db.close();
        } catch (_) {
          // noop
        }
      }
    }
  });
}

async function readReferenceRagStatus(options = {}) {
  const cfg = (options && typeof options === 'object') ? options : {};
  const userDataPath = String(cfg.userDataPath || '').trim();
  const referenceId = String(cfg.referenceId || '').trim();
  const dbPath = getRagIndexPath(userDataPath, referenceId);
  if (!dbPath || !referenceId) {
    return { ok: false, state: 'disabled', message: 'RAG index path is unavailable.', db_path: dbPath };
  }
  if (!fs.existsSync(dbPath)) {
    return {
      ok: true,
      state: 'missing',
      reference_id: referenceId,
      db_path: dbPath,
      doc_count: 0,
      embedding_count: 0,
      model_id: '',
      embedding_runtime: 'none',
      updated_at: 0,
      reference_version: '',
    };
  }

  let db = null;
  try {
    db = await openDatabase(dbPath);
    ensureSchema(db);
    const meta = readMeta(db);
    const modelId = String(meta.active_model_id || '').trim();
    const docCountRes = db.exec('SELECT COUNT(*) AS count FROM documents');
    const docCount = Number((docCountRes && docCountRes[0] && docCountRes[0].values && docCountRes[0].values[0] && docCountRes[0].values[0][0]) || 0);
    let embeddingCount = 0;
    if (modelId) {
      const embStmt = db.prepare('SELECT COUNT(*) AS count FROM embeddings WHERE model_id = ?');
      embStmt.bind([modelId]);
      if (embStmt.step()) {
        const row = embStmt.getAsObject();
        embeddingCount = Number(row.count || 0) || 0;
      }
      embStmt.free();
    }
    return {
      ok: true,
      state: docCount > 0 ? 'ready' : 'empty',
      reference_id: referenceId,
      db_path: dbPath,
      doc_count: docCount,
      embedding_count: embeddingCount,
      model_id: modelId,
      embedding_runtime: String(meta.embedding_runtime || 'none').trim() || 'none',
      updated_at: Number(meta.updated_at || 0),
      reference_version: String(meta.reference_version || ''),
    };
  } catch (err) {
    return {
      ok: false,
      state: 'error',
      reference_id: referenceId,
      db_path: dbPath,
      message: String((err && err.message) || 'Unable to read RAG index status.'),
    };
  } finally {
    if (db) {
      try {
        db.close();
      } catch (_) {
        // noop
      }
    }
  }
}

async function deleteReferenceRagIndex(options = {}) {
  const cfg = (options && typeof options === 'object') ? options : {};
  const dbPath = getRagIndexPath(cfg.userDataPath, cfg.referenceId);
  if (!dbPath) return { ok: false, message: 'RAG index path is unavailable.' };
  try {
    if (fs.existsSync(dbPath)) {
      fs.unlinkSync(dbPath);
    }
    return { ok: true, db_path: dbPath };
  } catch (err) {
    return { ok: false, db_path: dbPath, message: String((err && err.message) || 'Unable to delete RAG index.') };
  }
}

module.exports = {
  HASH_FALLBACK_MODEL,
  getRagIndexPath,
  ensureReferenceRagIndex,
  readReferenceRagStatus,
  deleteReferenceRagIndex,
};
