const fs = require('fs');
const path = require('path');

const SCHEMA_VERSION = 1;
const dbTaskByPath = new Map();

let sqlModulePromise = null;

function nowTs() {
  return Date.now();
}

function clampNumber(value, min, max, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  if (n < min) return min;
  if (n > max) return max;
  return n;
}

function escapeSegment(value = '') {
  return String(value || '').trim().replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 160);
}

function getNotesDbPath(userDataPath = '') {
  const base = String(userDataPath || '').trim();
  if (!base) return '';
  return path.join(base, 'notes', `${escapeSegment('notes')}.sqlite`);
}

function ensureDirForFile(filePath = '') {
  if (!filePath) return;
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
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
  fs.writeFileSync(dbPath, Buffer.from(db.export()));
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

function ensureSchema(db) {
  db.run('PRAGMA foreign_keys = ON');
  db.run('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)');
  const meta = readMeta(db);
  const existingVersion = Number(meta.schema_version || 0);
  if (existingVersion && existingVersion !== SCHEMA_VERSION) {
    db.run('DROP TABLE IF EXISTS note_citations');
    db.run('DROP TABLE IF EXISTS note_passages');
    db.run('DROP TABLE IF EXISTS note_sources');
    db.run('DROP TABLE IF EXISTS note_claims');
    db.run('DROP TABLE IF EXISTS analysis_runs');
    db.run('DROP TABLE IF EXISTS notes');
    db.run('DELETE FROM meta');
  }
  db.run(`
    CREATE TABLE IF NOT EXISTS notes (
      id TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      body_markdown TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      last_saved_at INTEGER NOT NULL,
      last_analyzed_at INTEGER NOT NULL DEFAULT 0,
      active_mode TEXT NOT NULL DEFAULT 'edit',
      promoted_reference_id TEXT NOT NULL DEFAULT '',
      analysis_revision INTEGER NOT NULL DEFAULT 0
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS analysis_runs (
      id TEXT PRIMARY KEY,
      note_id TEXT NOT NULL,
      note_revision INTEGER NOT NULL,
      started_at INTEGER NOT NULL,
      completed_at INTEGER NOT NULL DEFAULT 0,
      extractor_version TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'pending',
      claim_count INTEGER NOT NULL DEFAULT 0,
      supported_count INTEGER NOT NULL DEFAULT 0,
      contested_count INTEGER NOT NULL DEFAULT 0,
      uncertain_count INTEGER NOT NULL DEFAULT 0,
      no_evidence_count INTEGER NOT NULL DEFAULT 0,
      message TEXT NOT NULL DEFAULT '',
      FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS note_claims (
      id TEXT PRIMARY KEY,
      note_id TEXT NOT NULL,
      analysis_run_id TEXT NOT NULL,
      claim_index INTEGER NOT NULL DEFAULT 0,
      start_offset INTEGER NOT NULL DEFAULT 0,
      end_offset INTEGER NOT NULL DEFAULT 0,
      claim_text TEXT NOT NULL DEFAULT '',
      normalized_claim_text TEXT NOT NULL DEFAULT '',
      subject_text TEXT NOT NULL DEFAULT '',
      predicate_text TEXT NOT NULL DEFAULT '',
      object_text TEXT NOT NULL DEFAULT '',
      time_text TEXT NOT NULL DEFAULT '',
      modality TEXT NOT NULL DEFAULT '',
      factuality TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'no_evidence',
      top_score REAL NOT NULL DEFAULT 0,
      highlight_score REAL NOT NULL DEFAULT 0,
      FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE,
      FOREIGN KEY (analysis_run_id) REFERENCES analysis_runs(id) ON DELETE CASCADE
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS note_sources (
      id TEXT PRIMARY KEY,
      note_id TEXT NOT NULL,
      analysis_run_id TEXT NOT NULL,
      source_kind TEXT NOT NULL DEFAULT '',
      source_query TEXT NOT NULL DEFAULT '',
      url TEXT NOT NULL DEFAULT '',
      canonical_url TEXT NOT NULL DEFAULT '',
      title TEXT NOT NULL DEFAULT '',
      published_at INTEGER NOT NULL DEFAULT 0,
      fetched_at INTEGER NOT NULL DEFAULT 0,
      content_hash TEXT NOT NULL DEFAULT '',
      FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE,
      FOREIGN KEY (analysis_run_id) REFERENCES analysis_runs(id) ON DELETE CASCADE
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS note_passages (
      id TEXT PRIMARY KEY,
      note_id TEXT NOT NULL,
      analysis_run_id TEXT NOT NULL,
      source_id TEXT NOT NULL,
      passage_index INTEGER NOT NULL DEFAULT 0,
      passage_text TEXT NOT NULL DEFAULT '',
      passage_start INTEGER NOT NULL DEFAULT 0,
      passage_end INTEGER NOT NULL DEFAULT 0,
      fetched_at INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE,
      FOREIGN KEY (analysis_run_id) REFERENCES analysis_runs(id) ON DELETE CASCADE,
      FOREIGN KEY (source_id) REFERENCES note_sources(id) ON DELETE CASCADE
    );
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS note_citations (
      id TEXT PRIMARY KEY,
      note_id TEXT NOT NULL,
      analysis_run_id TEXT NOT NULL,
      claim_id TEXT NOT NULL,
      source_id TEXT NOT NULL,
      passage_id TEXT NOT NULL,
      citation_index INTEGER NOT NULL DEFAULT 0,
      support_label TEXT NOT NULL DEFAULT 'no_evidence',
      score REAL NOT NULL DEFAULT 0,
      semantic_score REAL NOT NULL DEFAULT 0,
      lexical_score REAL NOT NULL DEFAULT 0,
      time_score REAL NOT NULL DEFAULT 0,
      corroboration_score REAL NOT NULL DEFAULT 0,
      temporal_score REAL NOT NULL DEFAULT 0,
      excerpt TEXT NOT NULL DEFAULT '',
      FOREIGN KEY (note_id) REFERENCES notes(id) ON DELETE CASCADE,
      FOREIGN KEY (analysis_run_id) REFERENCES analysis_runs(id) ON DELETE CASCADE,
      FOREIGN KEY (claim_id) REFERENCES note_claims(id) ON DELETE CASCADE,
      FOREIGN KEY (source_id) REFERENCES note_sources(id) ON DELETE CASCADE,
      FOREIGN KEY (passage_id) REFERENCES note_passages(id) ON DELETE CASCADE
    );
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_notes_updated ON notes(updated_at DESC)');
  db.run('CREATE INDEX IF NOT EXISTS idx_analysis_runs_note ON analysis_runs(note_id, started_at DESC)');
  db.run('CREATE INDEX IF NOT EXISTS idx_note_claims_note ON note_claims(note_id, analysis_run_id, claim_index)');
  db.run('CREATE INDEX IF NOT EXISTS idx_note_sources_note ON note_sources(note_id, analysis_run_id, canonical_url)');
  db.run('CREATE INDEX IF NOT EXISTS idx_note_passages_source ON note_passages(source_id, passage_index)');
  db.run('CREATE INDEX IF NOT EXISTS idx_note_citations_claim ON note_citations(claim_id, citation_index)');
  upsertMeta(db, 'schema_version', String(SCHEMA_VERSION));
}

function makeId(prefix = 'note') {
  return `${prefix}_${nowTs()}_${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeNote(row = {}) {
  return {
    id: String(row.id || '').trim(),
    title: String(row.title || 'Untitled Note').trim() || 'Untitled Note',
    body_markdown: String(row.body_markdown || ''),
    created_at: clampNumber(row.created_at, 0, Number.MAX_SAFE_INTEGER, nowTs()),
    updated_at: clampNumber(row.updated_at, 0, Number.MAX_SAFE_INTEGER, nowTs()),
    last_saved_at: clampNumber(row.last_saved_at, 0, Number.MAX_SAFE_INTEGER, nowTs()),
    last_analyzed_at: clampNumber(row.last_analyzed_at, 0, Number.MAX_SAFE_INTEGER, 0),
    active_mode: String(row.active_mode || 'edit').trim().toLowerCase() === 'view' ? 'view' : 'edit',
    promoted_reference_id: String(row.promoted_reference_id || '').trim(),
    analysis_revision: clampNumber(row.analysis_revision, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

function noteSummaryFromRow(row = {}) {
  return {
    id: String(row.id || '').trim(),
    title: String(row.title || 'Untitled Note').trim() || 'Untitled Note',
    updated_at: clampNumber(row.updated_at, 0, Number.MAX_SAFE_INTEGER, 0),
    last_saved_at: clampNumber(row.last_saved_at, 0, Number.MAX_SAFE_INTEGER, 0),
    last_analyzed_at: clampNumber(row.last_analyzed_at, 0, Number.MAX_SAFE_INTEGER, 0),
    active_mode: String(row.active_mode || 'edit').trim().toLowerCase() === 'view' ? 'view' : 'edit',
    promoted_reference_id: String(row.promoted_reference_id || '').trim(),
    analysis_revision: clampNumber(row.analysis_revision, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

function readAllRows(stmt) {
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function upsertNote(db, note = {}) {
  const normalized = normalizeNote(note);
  const stmt = db.prepare(`
    INSERT INTO notes (
      id, title, body_markdown, created_at, updated_at, last_saved_at, last_analyzed_at, active_mode, promoted_reference_id, analysis_revision
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      title = excluded.title,
      body_markdown = excluded.body_markdown,
      created_at = excluded.created_at,
      updated_at = excluded.updated_at,
      last_saved_at = excluded.last_saved_at,
      last_analyzed_at = excluded.last_analyzed_at,
      active_mode = excluded.active_mode,
      promoted_reference_id = excluded.promoted_reference_id,
      analysis_revision = excluded.analysis_revision
  `);
  stmt.bind([
    normalized.id,
    normalized.title,
    normalized.body_markdown,
    normalized.created_at,
    normalized.updated_at,
    normalized.last_saved_at,
    normalized.last_analyzed_at,
    normalized.active_mode,
    normalized.promoted_reference_id,
    normalized.analysis_revision,
  ]);
  stmt.step();
  stmt.free();
  return normalized;
}

function queryNoteById(db, noteId = '') {
  const stmt = db.prepare('SELECT * FROM notes WHERE id = ? LIMIT 1');
  stmt.bind([String(noteId || '').trim()]);
  if (!stmt.step()) {
    stmt.free();
    return null;
  }
  const row = stmt.getAsObject();
  stmt.free();
  return normalizeNote(row);
}

function queryLatestAnalysisSummary(db, noteId = '') {
  const stmt = db.prepare(`
    SELECT id, note_revision, started_at, completed_at, extractor_version, status, claim_count, supported_count, contested_count, uncertain_count, no_evidence_count, message
    FROM analysis_runs
    WHERE note_id = ?
    ORDER BY started_at DESC
    LIMIT 1
  `);
  stmt.bind([String(noteId || '').trim()]);
  if (!stmt.step()) {
    stmt.free();
    return null;
  }
  const row = stmt.getAsObject();
  stmt.free();
  return {
    analysis_run_id: String(row.id || '').trim(),
    note_revision: clampNumber(row.note_revision, 0, Number.MAX_SAFE_INTEGER, 0),
    started_at: clampNumber(row.started_at, 0, Number.MAX_SAFE_INTEGER, 0),
    completed_at: clampNumber(row.completed_at, 0, Number.MAX_SAFE_INTEGER, 0),
    extractor_version: String(row.extractor_version || '').trim(),
    status: String(row.status || '').trim(),
    claim_count: clampNumber(row.claim_count, 0, Number.MAX_SAFE_INTEGER, 0),
    supported_count: clampNumber(row.supported_count, 0, Number.MAX_SAFE_INTEGER, 0),
    contested_count: clampNumber(row.contested_count, 0, Number.MAX_SAFE_INTEGER, 0),
    uncertain_count: clampNumber(row.uncertain_count, 0, Number.MAX_SAFE_INTEGER, 0),
    no_evidence_count: clampNumber(row.no_evidence_count, 0, Number.MAX_SAFE_INTEGER, 0),
    message: String(row.message || '').trim(),
  };
}

function queryClaimsForAnalysis(db, analysisRunId = '') {
  const stmt = db.prepare(`
    SELECT * FROM note_claims
    WHERE analysis_run_id = ?
    ORDER BY claim_index ASC, start_offset ASC, id ASC
  `);
  stmt.bind([String(analysisRunId || '').trim()]);
  const rows = readAllRows(stmt);
  return rows.map((row) => ({
    id: String(row.id || '').trim(),
    note_id: String(row.note_id || '').trim(),
    analysis_run_id: String(row.analysis_run_id || '').trim(),
    claim_index: clampNumber(row.claim_index, 0, Number.MAX_SAFE_INTEGER, 0),
    start_offset: clampNumber(row.start_offset, 0, Number.MAX_SAFE_INTEGER, 0),
    end_offset: clampNumber(row.end_offset, 0, Number.MAX_SAFE_INTEGER, 0),
    claim_text: String(row.claim_text || ''),
    normalized_claim_text: String(row.normalized_claim_text || ''),
    subject_text: String(row.subject_text || ''),
    predicate_text: String(row.predicate_text || ''),
    object_text: String(row.object_text || ''),
    time_text: String(row.time_text || ''),
    modality: String(row.modality || ''),
    factuality: String(row.factuality || ''),
    status: String(row.status || 'no_evidence'),
    top_score: Number(row.top_score || 0) || 0,
    highlight_score: Number(row.highlight_score || 0) || 0,
  }));
}

function queryCitationsForClaim(db, claimId = '') {
  const stmt = db.prepare(`
    SELECT
      c.*,
      s.url AS source_url,
      s.canonical_url AS source_canonical_url,
      s.title AS source_title,
      s.published_at AS source_published_at,
      s.source_kind AS source_kind,
      p.passage_text AS passage_text
    FROM note_citations c
    LEFT JOIN note_sources s ON s.id = c.source_id
    LEFT JOIN note_passages p ON p.id = c.passage_id
    WHERE c.claim_id = ?
    ORDER BY c.citation_index ASC, c.score DESC, c.id ASC
  `);
  stmt.bind([String(claimId || '').trim()]);
  const rows = readAllRows(stmt);
  return rows.map((row) => ({
    id: String(row.id || '').trim(),
    note_id: String(row.note_id || '').trim(),
    analysis_run_id: String(row.analysis_run_id || '').trim(),
    claim_id: String(row.claim_id || '').trim(),
    source_id: String(row.source_id || '').trim(),
    passage_id: String(row.passage_id || '').trim(),
    citation_index: clampNumber(row.citation_index, 0, Number.MAX_SAFE_INTEGER, 0),
    support_label: String(row.support_label || 'no_evidence'),
    score: Number(row.score || 0) || 0,
    semantic_score: Number(row.semantic_score || 0) || 0,
    lexical_score: Number(row.lexical_score || 0) || 0,
    time_score: Number(row.time_score || 0) || 0,
    corroboration_score: Number(row.corroboration_score || 0) || 0,
    temporal_score: Number(row.temporal_score || 0) || 0,
    excerpt: String(row.excerpt || ''),
    source: {
      id: String(row.source_id || '').trim(),
      url: String(row.source_url || ''),
      canonical_url: String(row.source_canonical_url || ''),
      title: String(row.source_title || ''),
      published_at: clampNumber(row.source_published_at, 0, Number.MAX_SAFE_INTEGER, 0),
      source_kind: String(row.source_kind || ''),
    },
    passage_text: String(row.passage_text || ''),
  }));
}

function deleteAnalysisRows(db, noteId = '') {
  const target = String(noteId || '').trim();
  db.run('DELETE FROM note_citations WHERE note_id = ?', [target]);
  db.run('DELETE FROM note_passages WHERE note_id = ?', [target]);
  db.run('DELETE FROM note_sources WHERE note_id = ?', [target]);
  db.run('DELETE FROM note_claims WHERE note_id = ?', [target]);
  db.run('DELETE FROM analysis_runs WHERE note_id = ?', [target]);
}

function createNotesStore(options = {}) {
  const userDataPath = String(options.userDataPath || '').trim();
  const dbPath = getNotesDbPath(userDataPath);

  async function runWithDb(taskFn) {
    if (!dbPath) return { ok: false, message: 'Notes database path is unavailable.' };
    return withDbLock(dbPath, async () => {
      let db = null;
      try {
        db = await openDatabase(dbPath);
        ensureSchema(db);
        const result = await taskFn(db);
        saveDatabase(db, dbPath);
        return result;
      } catch (err) {
        return { ok: false, message: String((err && err.message) || 'Notes database operation failed.') };
      } finally {
        if (db) {
          try { db.close(); } catch (_) {}
        }
      }
    });
  }

  return {
    async listNotes() {
      return runWithDb(async (db) => {
        const stmt = db.prepare('SELECT * FROM notes ORDER BY updated_at DESC, created_at DESC');
        const rows = readAllRows(stmt);
        return { ok: true, notes: rows.map((row) => noteSummaryFromRow(row)) };
      });
    },

    async createNote(input = {}) {
      return runWithDb(async (db) => {
        const createdAt = nowTs();
        const body = String((input && input.body_markdown) || '').replace(/\r\n?/g, '\n');
        const note = upsertNote(db, {
          id: String((input && input.id) || makeId('note')).trim() || makeId('note'),
          title: String((input && input.title) || 'Untitled Note').trim() || 'Untitled Note',
          body_markdown: body,
          created_at: createdAt,
          updated_at: createdAt,
          last_saved_at: createdAt,
          last_analyzed_at: 0,
          active_mode: String((input && input.active_mode) || 'edit').trim(),
          promoted_reference_id: '',
          analysis_revision: 1,
        });
        return { ok: true, note };
      });
    },

    async getNote(noteId = '') {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        return {
          ok: true,
          note,
          analysis_summary: queryLatestAnalysisSummary(db, noteId),
        };
      });
    },

    async updateNote(noteId = '', patch = {}) {
      return runWithDb(async (db) => {
        const current = queryNoteById(db, noteId);
        if (!current) return { ok: false, message: 'Note not found.' };
        const hasBodyUpdate = Object.prototype.hasOwnProperty.call(patch || {}, 'body_markdown');
        const now = nowTs();
        const next = upsertNote(db, {
          ...current,
          title: Object.prototype.hasOwnProperty.call(patch || {}, 'title') ? String(patch.title || '').trim() || current.title : current.title,
          body_markdown: hasBodyUpdate ? String(patch.body_markdown || '').replace(/\r\n?/g, '\n') : current.body_markdown,
          updated_at: now,
          last_saved_at: now,
          active_mode: Object.prototype.hasOwnProperty.call(patch || {}, 'active_mode') ? String(patch.active_mode || '').trim() : current.active_mode,
          promoted_reference_id: Object.prototype.hasOwnProperty.call(patch || {}, 'promoted_reference_id') ? String(patch.promoted_reference_id || '').trim() : current.promoted_reference_id,
          analysis_revision: current.analysis_revision + (hasBodyUpdate ? 1 : 0),
        });
        return { ok: true, note: next, save_state: 'saved' };
      });
    },

    async deleteNote(noteId = '') {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        db.run('DELETE FROM notes WHERE id = ?', [String(noteId || '').trim()]);
        return { ok: true, deleted_note_id: String(noteId || '').trim() };
      });
    },

    async saveAnalysis(noteId = '', payload = {}) {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        const noteRevision = clampNumber(payload.note_revision, 0, Number.MAX_SAFE_INTEGER, note.analysis_revision);
        if (note.analysis_revision !== noteRevision) {
          return { ok: false, stale: true, message: 'Note analysis is stale.' };
        }
        deleteAnalysisRows(db, noteId);
        const analysisRunId = String(payload.analysis_run_id || makeId('analysis')).trim() || makeId('analysis');
        const startedAt = clampNumber(payload.started_at, 0, Number.MAX_SAFE_INTEGER, nowTs());
        const completedAt = clampNumber(payload.completed_at, 0, Number.MAX_SAFE_INTEGER, nowTs());
        const claims = Array.isArray(payload.claims) ? payload.claims : [];
        const sources = Array.isArray(payload.sources) ? payload.sources : [];
        const passages = Array.isArray(payload.passages) ? payload.passages : [];
        const citations = Array.isArray(payload.citations) ? payload.citations : [];
        const counts = {
          supported: claims.filter((item) => String((item && item.status) || '') === 'supported').length,
          contested: claims.filter((item) => String((item && item.status) || '') === 'contested').length,
          uncertain: claims.filter((item) => String((item && item.status) || '') === 'uncertain').length,
          noEvidence: claims.filter((item) => String((item && item.status) || '') === 'no_evidence').length,
        };

        const runStmt = db.prepare(`
          INSERT INTO analysis_runs (
            id, note_id, note_revision, started_at, completed_at, extractor_version, status, claim_count, supported_count, contested_count, uncertain_count, no_evidence_count, message
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        runStmt.bind([
          analysisRunId,
          note.id,
          noteRevision,
          startedAt,
          completedAt,
          String(payload.extractor_version || ''),
          String(payload.status || 'completed'),
          claims.length,
          counts.supported,
          counts.contested,
          counts.uncertain,
          counts.noEvidence,
          String(payload.message || ''),
        ]);
        runStmt.step();
        runStmt.free();

        const claimStmt = db.prepare(`
          INSERT INTO note_claims (
            id, note_id, analysis_run_id, claim_index, start_offset, end_offset, claim_text, normalized_claim_text, subject_text, predicate_text, object_text, time_text, modality, factuality, status, top_score, highlight_score
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        claims.forEach((item, idx) => {
          claimStmt.bind([
            String((item && item.id) || makeId('claim')),
            note.id,
            analysisRunId,
            clampNumber((item && item.claim_index), 0, Number.MAX_SAFE_INTEGER, idx),
            clampNumber((item && item.start_offset), 0, Number.MAX_SAFE_INTEGER, 0),
            clampNumber((item && item.end_offset), 0, Number.MAX_SAFE_INTEGER, 0),
            String((item && item.claim_text) || ''),
            String((item && item.normalized_claim_text) || ''),
            String((item && item.subject_text) || ''),
            String((item && item.predicate_text) || ''),
            String((item && item.object_text) || ''),
            String((item && item.time_text) || ''),
            String((item && item.modality) || ''),
            String((item && item.factuality) || ''),
            String((item && item.status) || 'no_evidence'),
            Number((item && item.top_score) || 0) || 0,
            Number((item && item.highlight_score) || 0) || 0,
          ]);
          claimStmt.step();
          claimStmt.reset();
        });
        claimStmt.free();

        const sourceStmt = db.prepare(`
          INSERT INTO note_sources (
            id, note_id, analysis_run_id, source_kind, source_query, url, canonical_url, title, published_at, fetched_at, content_hash
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        sources.forEach((item) => {
          sourceStmt.bind([
            String((item && item.id) || makeId('src')),
            note.id,
            analysisRunId,
            String((item && item.source_kind) || ''),
            String((item && item.source_query) || ''),
            String((item && item.url) || ''),
            String((item && item.canonical_url) || ''),
            String((item && item.title) || ''),
            clampNumber((item && item.published_at), 0, Number.MAX_SAFE_INTEGER, 0),
            clampNumber((item && item.fetched_at), 0, Number.MAX_SAFE_INTEGER, 0),
            String((item && item.content_hash) || ''),
          ]);
          sourceStmt.step();
          sourceStmt.reset();
        });
        sourceStmt.free();

        const passageStmt = db.prepare(`
          INSERT INTO note_passages (
            id, note_id, analysis_run_id, source_id, passage_index, passage_text, passage_start, passage_end, fetched_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        passages.forEach((item, idx) => {
          passageStmt.bind([
            String((item && item.id) || makeId('passage')),
            note.id,
            analysisRunId,
            String((item && item.source_id) || ''),
            clampNumber((item && item.passage_index), 0, Number.MAX_SAFE_INTEGER, idx),
            String((item && item.passage_text) || ''),
            clampNumber((item && item.passage_start), 0, Number.MAX_SAFE_INTEGER, 0),
            clampNumber((item && item.passage_end), 0, Number.MAX_SAFE_INTEGER, 0),
            clampNumber((item && item.fetched_at), 0, Number.MAX_SAFE_INTEGER, 0),
          ]);
          passageStmt.step();
          passageStmt.reset();
        });
        passageStmt.free();

        const citationStmt = db.prepare(`
          INSERT INTO note_citations (
            id, note_id, analysis_run_id, claim_id, source_id, passage_id, citation_index, support_label, score, semantic_score, lexical_score, time_score, corroboration_score, temporal_score, excerpt
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        citations.forEach((item, idx) => {
          citationStmt.bind([
            String((item && item.id) || makeId('citation')),
            note.id,
            analysisRunId,
            String((item && item.claim_id) || ''),
            String((item && item.source_id) || ''),
            String((item && item.passage_id) || ''),
            clampNumber((item && item.citation_index), 0, Number.MAX_SAFE_INTEGER, idx),
            String((item && item.support_label) || 'no_evidence'),
            Number((item && item.score) || 0) || 0,
            Number((item && item.semantic_score) || 0) || 0,
            Number((item && item.lexical_score) || 0) || 0,
            Number((item && item.time_score) || 0) || 0,
            Number((item && item.corroboration_score) || 0) || 0,
            Number((item && item.temporal_score) || 0) || 0,
            String((item && item.excerpt) || ''),
          ]);
          citationStmt.step();
          citationStmt.reset();
        });
        citationStmt.free();

        const nextNote = upsertNote(db, {
          ...note,
          last_analyzed_at: completedAt,
        });
        return {
          ok: true,
          note: nextNote,
          analysis_summary: queryLatestAnalysisSummary(db, note.id),
        };
      });
    },

    async getAnalysis(noteId = '') {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        const summary = queryLatestAnalysisSummary(db, note.id);
        const claims = summary ? queryClaimsForAnalysis(db, summary.analysis_run_id) : [];
        return {
          ok: true,
          note,
          analysis_summary: summary,
          claims,
        };
      });
    },

    async getCitations(noteId = '', claimId = '') {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        const summary = queryLatestAnalysisSummary(db, note.id);
        if (!summary) return { ok: true, note, claim: null, citations: [] };
        const claims = queryClaimsForAnalysis(db, summary.analysis_run_id);
        const claim = claims.find((item) => String(item.id || '').trim() === String(claimId || '').trim()) || null;
        if (!claim) return { ok: false, message: 'Claim not found.' };
        return {
          ok: true,
          note,
          claim,
          citations: queryCitationsForClaim(db, claim.id).slice(0, 3),
        };
      });
    },
  };
}

module.exports = {
  createNotesStore,
  getNotesDbPath,
};
