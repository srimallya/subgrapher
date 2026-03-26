const fs = require('fs');
const path = require('path');

const SCHEMA_VERSION = 1;
const dbTaskByPath = new Map();
const NOTE_EVIDENCE_MODE = 'web_only';
const WEB_ONLY_SOURCE_KINDS = new Set([
  'explicit_url',
  'web_search',
  'official_search',
  'challenge_search',
  'rss_search',
  'rss_official_search',
  'rss_challenge_search',
]);

let sqlModulePromise = null;

function nowTs() {
  return Date.now();
}

function clampUnit(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(1, numeric));
}

function clampNumber(value, min, max, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  if (n < min) return min;
  if (n > max) return max;
  return n;
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

function hasTableColumn(db, tableName = '', columnName = '') {
  const table = String(tableName || '').trim();
  const column = String(columnName || '').trim();
  if (!table || !column) return false;
  const result = db.exec(`PRAGMA table_info(${table})`);
  const rows = result && result[0] && Array.isArray(result[0].values) ? result[0].values : [];
  return rows.some((row) => String((row && row[1]) || '').trim() === column);
}

function ensureTableColumn(db, tableName = '', columnName = '', columnSpec = '') {
  if (!tableName || !columnName || !columnSpec) return;
  if (hasTableColumn(db, tableName, columnName)) return;
  db.run(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnSpec}`);
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
      note_score REAL NOT NULL DEFAULT 0,
      coverage_score REAL NOT NULL DEFAULT 0,
      risk_level TEXT NOT NULL DEFAULT 'needs_review',
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
  ensureTableColumn(db, 'note_claims', 'support_confidence', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'contradict_confidence', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'truth_confidence', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'claim_type', "TEXT NOT NULL DEFAULT 'state_trend'");
  ensureTableColumn(db, 'note_claims', 'claim_weight', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'claim_reliability', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'corroboration', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'authority', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'freshness', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'note_claims', 'explanation', "TEXT NOT NULL DEFAULT ''");
  ensureTableColumn(db, 'note_claims', 'rewrite_suggestions', "TEXT NOT NULL DEFAULT '[]'");
  ensureTableColumn(db, 'analysis_runs', 'note_score', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'analysis_runs', 'coverage_score', 'REAL NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'analysis_runs', 'risk_level', "TEXT NOT NULL DEFAULT 'needs_review'");
  ensureTableColumn(db, 'analysis_runs', 'note_policy_json', "TEXT NOT NULL DEFAULT '{}'");
  ensureTableColumn(db, 'analysis_runs', 'analysis_source', "TEXT NOT NULL DEFAULT 'fallback'");
  ensureTableColumn(db, 'analysis_runs', 'freshness_state', "TEXT NOT NULL DEFAULT 'stable'");
  ensureTableColumn(db, 'analysis_runs', 'latest_evidence_at', 'INTEGER NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'analysis_runs', 'next_refresh_at', 'INTEGER NOT NULL DEFAULT 0');
  ensureTableColumn(db, 'analysis_runs', 'policy_classified_at', 'INTEGER NOT NULL DEFAULT 0');
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

function isWebOnlySourceKind(sourceKind = '') {
  return WEB_ONLY_SOURCE_KINDS.has(String(sourceKind || '').trim());
}

function getSearchIntentForSourceKind(sourceKind = '') {
  const kind = String(sourceKind || '').trim();
  if (kind === 'official_search' || kind === 'rss_official_search') return 'official';
  if (kind === 'challenge_search' || kind === 'rss_challenge_search') return 'challenge';
  return 'support';
}

function getProvenanceLabelForSourceKind(sourceKind = '') {
  const kind = String(sourceKind || '').trim();
  if (kind === 'explicit_url') return 'from pasted URL';
  if (kind === 'official_search') return 'from official source search';
  if (kind === 'challenge_search') return 'from challenge search';
  if (kind === 'rss_official_search') return 'from RSS cache (official search)';
  if (kind === 'rss_challenge_search') return 'from RSS cache (challenge search)';
  if (kind === 'rss_search') return 'from RSS cache';
  return 'from web search';
}

function getRelevanceReasonForCitation(citation = {}) {
  const sourceKind = String((citation && citation.source && citation.source.source_kind) || '').trim();
  const intent = getSearchIntentForSourceKind(sourceKind);
  const stance = String((citation && citation.stance) || (citation && citation.support_label) || '').trim();
  if (sourceKind === 'explicit_url') return 'This source came from a URL already present in the note.';
  if (sourceKind === 'rss_search' || sourceKind === 'rss_official_search' || sourceKind === 'rss_challenge_search') {
    if (intent === 'official') return 'This result came from the cached RSS article bank while looking for official confirmation.';
    if (intent === 'challenge' || stance === 'contradict') return 'This cached RSS article contradicts or narrows part of the claim.';
    return 'This cached RSS article supports the claim wording.';
  }
  if (intent === 'official') return 'This result came from an official-source search for the claim.';
  if (intent === 'challenge' || stance === 'contradict') return 'This source contradicts or narrows part of the claim.';
  return 'This source supports the claim wording.';
}

function decorateAnalysisSummary(summary = null) {
  if (!summary) return null;
  const notePolicy = (summary.note_policy && typeof summary.note_policy === 'object')
    ? summary.note_policy
    : {};
  return {
    ...summary,
    evidence_mode: NOTE_EVIDENCE_MODE,
    note_score: clampNumber(summary.note_score, 0, 100, 0),
    coverage_score: clampUnit(summary.coverage_score),
    risk_level: String(summary.risk_level || 'needs_review').trim() || 'needs_review',
    contradicted_count: clampNumber(summary.uncertain_count, 0, Number.MAX_SAFE_INTEGER, 0),
    mixed_count: clampNumber(summary.contested_count, 0, Number.MAX_SAFE_INTEGER, 0),
    insufficient_evidence_count: clampNumber(summary.no_evidence_count, 0, Number.MAX_SAFE_INTEGER, 0),
    note_policy: notePolicy,
    freshness_state: String(summary.freshness_state || 'stable').trim() || 'stable',
    latest_evidence_at: clampNumber(summary.latest_evidence_at, 0, Number.MAX_SAFE_INTEGER, 0),
    next_refresh_at: clampNumber(summary.next_refresh_at, 0, Number.MAX_SAFE_INTEGER, 0),
    analysis_source: String(summary.analysis_source || 'fallback').trim() || 'fallback',
    policy_classified_at: clampNumber(summary.policy_classified_at, 0, Number.MAX_SAFE_INTEGER, 0),
  };
}

function normalizeClaimStatus(value = '') {
  const status = String(value || '').trim();
  if (status === 'insufficient_evidence') return 'weak_evidence';
  if (['supported', 'mostly_supported', 'mixed', 'weak_evidence', 'contradicted'].includes(status)) return status;
  return 'weak_evidence';
}

function getClaimTypeWeight(claimType = '') {
  const type = String(claimType || '').trim();
  if (['numeric_stat', 'date_time', 'event'].includes(type)) return 1.3;
  if (['state_trend', 'attribution'].includes(type)) return 1.0;
  if (type === 'broad_interpretation') return 0.5;
  return 0;
}

function computeClaimReliability(claim = {}) {
  const explicit = Number((claim && claim.claim_reliability) || 0);
  if (explicit > 0) return clampUnit(explicit);
  const support = clampUnit(Number((claim && claim.support_confidence) || 0));
  const contradict = clampUnit(Number((claim && claim.contradict_confidence) || 0));
  const corroboration = clampUnit(Number((claim && claim.corroboration) || 0));
  return clampUnit(0.5 + (0.55 * support) - (0.75 * contradict) + (0.1 * corroboration));
}

function computeAggregateMetrics(claims = []) {
  const weighted = (Array.isArray(claims) ? claims : []).filter((claim) => getClaimTypeWeight(claim && claim.claim_type) > 0);
  if (weighted.length === 0) {
    return { note_score: 0, coverage_score: 0, risk_level: 'needs_review' };
  }
  const totalWeight = weighted.reduce((sum, claim) => sum + getClaimTypeWeight(claim.claim_type), 0) || 1;
  const weightedMean = weighted.reduce((sum, claim) => sum + (computeClaimReliability(claim) * getClaimTypeWeight(claim.claim_type)), 0) / totalWeight;
  const coverageScore = weighted.reduce((sum, claim) => {
    const coverage = Math.max(Number((claim && claim.support_confidence) || 0) || 0, Number((claim && claim.contradict_confidence) || 0) || 0);
    return sum + (coverage * getClaimTypeWeight(claim.claim_type));
  }, 0) / totalWeight;
  const corroborationScore = weighted.reduce((sum, claim) => sum + ((Number((claim && claim.corroboration) || 0) || 0) * getClaimTypeWeight(claim.claim_type)), 0) / totalWeight;
  const contradictionPenalty = weighted.reduce((sum, claim) => {
    const severity = normalizeClaimStatus(claim && claim.status) === 'contradicted' ? 1 : 0.45;
    return sum + ((Number((claim && claim.contradict_confidence) || 0) || 0) * severity * getClaimTypeWeight(claim.claim_type));
  }, 0) / totalWeight;
  const supportedCount = weighted.filter((claim) => ['supported', 'mostly_supported'].includes(normalizeClaimStatus(claim && claim.status))).length;
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
    const support = Number((claim && claim.support_confidence) || 0) || 0;
    const contradict = Number((claim && claim.contradict_confidence) || 0) || 0;
    return weight >= 1 && contradict >= 0.72 && contradict >= support + 0.08;
  });
  const weakMass = weighted.filter((claim) => ['contradicted', 'weak_evidence', 'mixed'].includes(normalizeClaimStatus(claim && claim.status))).length / Math.max(1, weighted.length);
  return {
    note_score: noteScore,
    coverage_score: Number(coverageScore.toFixed(4)),
    risk_level: highRisk ? 'high_contradiction_risk' : (weakMass >= 0.28 || noteScore < 72 ? 'needs_review' : 'clean'),
  };
}

function buildSentenceRegions(bodyMarkdown = '', claims = []) {
  const text = String(bodyMarkdown || '');
  const scored = [];
  let offset = 0;
  let paragraphIndex = 0;
  text.split('\n').forEach((lineRaw) => {
    const line = String(lineRaw || '');
    const trimmed = line.trim();
    const isHeading = !!trimmed && (/^\s{0,3}#{1,6}\s+/.test(trimmed) || /^\*\*[^*]+\*\*$/.test(trimmed) || /^__[^_]+__$/.test(trimmed));
    const isSeparator = /^\s{0,3}([-*_])(?:\s*\1){2,}\s*$/.test(trimmed);
    const normalizedLine = stripMarkdownListMarker(line);
    const contentLine = String(normalizedLine.text || '');
    const contentTrimmed = contentLine.trim();
    if (!trimmed || isHeading || isSeparator || !contentTrimmed) {
      offset += line.length + 1;
      if (!trimmed) paragraphIndex += 1;
      return;
    }
    const re = /[^.!?]+[.!?]?/g;
    let match = re.exec(contentLine);
    while (match) {
      const chunk = String(match[0] || '');
      const sentence = chunk.trim();
      if (sentence) {
        const localStart = Number(normalizedLine.contentOffset || 0) + match.index + chunk.indexOf(sentence);
        const start = offset + localStart;
        const end = start + sentence.length;
        const linkedClaims = (Array.isArray(claims) ? claims : []).filter((claim) => {
          const claimStart = Number((claim && claim.start_offset) ?? -1);
          const claimEnd = Number((claim && claim.end_offset) ?? -1);
          return claimStart >= 0 && claimEnd > claimStart && claimEnd > start && claimStart < end;
        });
        if (linkedClaims.length > 0) {
          const totalWeight = linkedClaims.reduce((sum, claim) => sum + Math.max(0.2, getClaimTypeWeight(claim.claim_type)), 0) || 1;
          const average = linkedClaims.reduce((sum, claim) => sum + (computeClaimReliability(claim) * Math.max(0.2, getClaimTypeWeight(claim.claim_type))), 0) / totalWeight;
          const hardContradiction = linkedClaims.some((claim) => normalizeClaimStatus(claim.status) === 'contradicted' && Number((claim && claim.contradict_confidence) || 0) >= 0.72);
          const hasMixed = linkedClaims.some((claim) => normalizeClaimStatus(claim.status) === 'mixed');
          const hasWeak = linkedClaims.some((claim) => normalizeClaimStatus(claim.status) === 'weak_evidence');
          scored.push({
            region_id: `region_${scored.length}_${start}_${end}`,
            start_offset: start,
            end_offset: end,
            paragraph_index: paragraphIndex,
            region_text: sentence,
            region_score: Number(average.toFixed(4)),
            region_status: hardContradiction ? 'contradicted' : (average >= 0.82 ? 'supported' : (average >= 0.68 ? 'mostly_supported' : (hasMixed ? 'mixed' : (hasWeak ? 'weak_evidence' : 'weak_evidence')))),
            claim_ids: linkedClaims.map((claim) => String((claim && claim.id) || '').trim()).filter(Boolean),
            support_count: linkedClaims.filter((claim) => ['supported', 'mostly_supported'].includes(normalizeClaimStatus(claim.status))).length,
            contradict_count: linkedClaims.filter((claim) => normalizeClaimStatus(claim.status) === 'contradicted').length,
            weak_count: linkedClaims.filter((claim) => ['mixed', 'weak_evidence'].includes(normalizeClaimStatus(claim.status))).length,
          });
        }
      }
      match = re.exec(contentLine);
    }
    offset += line.length + 1;
    paragraphIndex += 1;
  });
  const merged = [];
  scored.forEach((region) => {
    const prev = merged[merged.length - 1];
    if (prev && prev.paragraph_index === region.paragraph_index && prev.region_status === region.region_status && Math.abs(prev.end_offset - region.start_offset) <= 2) {
      prev.end_offset = region.end_offset;
      prev.region_text = `${prev.region_text} ${region.region_text}`.trim();
      prev.region_score = Number((((prev.region_score + region.region_score) / 2)).toFixed(4));
      prev.claim_ids = Array.from(new Set(prev.claim_ids.concat(region.claim_ids)));
      prev.support_count += region.support_count;
      prev.contradict_count += region.contradict_count;
      prev.weak_count += region.weak_count;
      return;
    }
    merged.push({ ...region });
  });
  return merged;
}

function buildEvidenceFeed(regions = [], claims = [], citationsByClaimId = new Map()) {
  function hasReadableEvidenceText(item = {}) {
    const source = (item && item.source && typeof item.source === 'object') ? item.source : {};
    const text = String((item && item.excerpt) || (item && item.passage_text) || source.title || source.url || '').trim();
    if (!text) return false;
    return text.length >= 24 || /^https?:\/\//i.test(String(source.url || ''));
  }

  function isHelpfulEvidenceItem(item = {}) {
    const score = Number((item && item.score) || 0) || 0;
    return score >= 0.34 && hasReadableEvidenceText(item);
  }

  return (Array.isArray(regions) ? regions : []).map((region) => {
    const linkedClaims = (Array.isArray(claims) ? claims : []).filter((claim) => region.claim_ids.includes(String((claim && claim.id) || '').trim()));
    const allCitations = linkedClaims
      .flatMap((claim) => citationsByClaimId.get(String((claim && claim.id) || '').trim()) || [])
      .filter(isHelpfulEvidenceItem)
      .sort((a, b) => Number((b && b.score) || 0) - Number((a && a.score) || 0));
    const supportItems = allCitations.filter((item) => String((item && item.stance) || '') !== 'contradict').slice(0, 2);
    const contradictionItems = allCitations.filter((item) => String((item && item.stance) || '') === 'contradict').slice(0, 2);
    const contextItems = allCitations
      .filter((item) => String((item && item.stance) || '') !== 'contradict')
      .slice(0, 2);
    const leadClaim = linkedClaims
      .slice()
      .sort((a, b) => Math.max(Number((b && b.contradict_confidence) || 0), Number((b && b.support_confidence) || 0)) - Math.max(Number((a && a.contradict_confidence) || 0), Number((a && a.support_confidence) || 0)))[0] || null;
    const rewrite = Array.isArray(leadClaim && leadClaim.rewrite_suggestions) ? leadClaim.rewrite_suggestions[0] || null : null;
    return {
      region_id: region.region_id,
      start_offset: region.start_offset,
      end_offset: region.end_offset,
      region_status: region.region_status,
      region_score: region.region_score,
      region_text: region.region_text,
      claim_ids: region.claim_ids.slice(),
      support_count: region.support_count,
      contradict_count: region.contradict_count,
      weak_count: region.weak_count,
      support_items: supportItems,
      contradiction_items: contradictionItems,
      context_items: contextItems,
      suggested_rewrite: rewrite,
    };
  });
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
    SELECT id, note_revision, started_at, completed_at, extractor_version, status, claim_count, supported_count, contested_count, uncertain_count, no_evidence_count, note_score, coverage_score, risk_level, message, note_policy_json, analysis_source, freshness_state, latest_evidence_at, next_refresh_at, policy_classified_at
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
    note_score: clampNumber(row.note_score, 0, 100, 0),
    coverage_score: clampUnit(row.coverage_score),
    risk_level: String(row.risk_level || 'needs_review').trim() || 'needs_review',
    message: String(row.message || '').trim(),
    note_policy: (() => {
      try {
        const parsed = JSON.parse(String(row.note_policy_json || '{}'));
        return parsed && typeof parsed === 'object' ? parsed : {};
      } catch (_) {
        return {};
      }
    })(),
    analysis_source: String(row.analysis_source || 'fallback').trim() || 'fallback',
    freshness_state: String(row.freshness_state || 'stable').trim() || 'stable',
    latest_evidence_at: clampNumber(row.latest_evidence_at, 0, Number.MAX_SAFE_INTEGER, 0),
    next_refresh_at: clampNumber(row.next_refresh_at, 0, Number.MAX_SAFE_INTEGER, 0),
    policy_classified_at: clampNumber(row.policy_classified_at, 0, Number.MAX_SAFE_INTEGER, 0),
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
    claim_type: String(row.claim_type || 'state_trend').trim() || 'state_trend',
    claim_weight: Number(row.claim_weight || 0) || getClaimTypeWeight(row.claim_type),
    status: normalizeClaimStatus(row.status),
    verdict: normalizeClaimStatus(row.status),
    top_score: Number(row.top_score || 0) || 0,
    highlight_score: Number(row.highlight_score || 0) || 0,
    truth_confidence: Number(row.truth_confidence || row.highlight_score || row.top_score || 0) || 0,
    support_confidence: Number(row.support_confidence || 0) || 0,
    contradict_confidence: Number(row.contradict_confidence || 0) || 0,
    claim_reliability: Number(row.claim_reliability || 0) || 0,
    corroboration: Number(row.corroboration || 0) || 0,
    authority: Number(row.authority || 0) || 0,
    freshness: Number(row.freshness || 0) || 0,
    explanation: String(row.explanation || ''),
    rewrite_suggestions: (() => {
      try {
        const parsed = JSON.parse(String(row.rewrite_suggestions || '[]'));
        return Array.isArray(parsed) ? parsed : [];
      } catch (_) {
        return [];
      }
    })(),
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
    support_label: String(row.support_label || 'support'),
    stance: String(row.support_label || 'support') === 'contradict' ? 'contradict' : 'support',
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
  }))
    .filter((citation) => isWebOnlySourceKind(citation && citation.source && citation.source.source_kind))
    .map((citation) => ({
      ...citation,
      search_intent: getSearchIntentForSourceKind(citation.source && citation.source.source_kind),
      provenance_label: getProvenanceLabelForSourceKind(citation.source && citation.source.source_kind),
      relevance_reason: getRelevanceReasonForCitation(citation),
    }));
}

function resolveClaimForLookup(claims = [], claimId = '', claimInput = {}) {
  const items = Array.isArray(claims) ? claims : [];
  const targetId = String(claimId || '').trim();
  if (targetId) {
    const byId = items.find((item) => String((item && item.id) || '').trim() === targetId);
    if (byId) return byId;
  }
  const start = Number((claimInput && claimInput.start_offset) ?? -1);
  const end = Number((claimInput && claimInput.end_offset) ?? -1);
  if (Number.isFinite(start) && Number.isFinite(end) && start >= 0 && end > start) {
    const byRange = items.find((item) => Number((item && item.start_offset) ?? -1) === start && Number((item && item.end_offset) ?? -1) === end);
    if (byRange) return byRange;
  }
  const targetText = String((claimInput && claimInput.claim_text) || '').trim().toLowerCase();
  if (targetText) {
    return items.find((item) => String((item && item.claim_text) || '').trim().toLowerCase() === targetText) || null;
  }
  return null;
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
          analysis_summary: decorateAnalysisSummary(queryLatestAnalysisSummary(db, noteId)),
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

    async resetAnalyses(noteIds = []) {
      return runWithDb(async (db) => {
        const targets = Array.isArray(noteIds)
          ? noteIds.map((item) => String(item || '').trim()).filter(Boolean)
          : [];
        if (targets.length === 0) {
          db.run('DELETE FROM note_citations');
          db.run('DELETE FROM note_passages');
          db.run('DELETE FROM note_sources');
          db.run('DELETE FROM note_claims');
          db.run('DELETE FROM analysis_runs');
          db.run('UPDATE notes SET last_analyzed_at = 0');
          return { ok: true, cleared_notes: null, reset_scope: 'all' };
        }
        targets.forEach((target) => {
          deleteAnalysisRows(db, target);
          db.run('UPDATE notes SET last_analyzed_at = 0 WHERE id = ?', [target]);
        });
        return { ok: true, cleared_notes: targets.length, reset_scope: 'partial' };
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
          supported: claims.filter((item) => ['supported', 'mostly_supported'].includes(normalizeClaimStatus(item && item.status))).length,
          mixed: claims.filter((item) => String((item && item.status) || '') === 'mixed').length,
          contradicted: claims.filter((item) => String((item && item.status) || '') === 'contradicted').length,
          insufficient: claims.filter((item) => normalizeClaimStatus(item && item.status) === 'weak_evidence').length,
        };
        const aggregate = computeAggregateMetrics(claims);

        const runStmt = db.prepare(`
          INSERT INTO analysis_runs (
            id, note_id, note_revision, started_at, completed_at, extractor_version, status, claim_count, supported_count, contested_count, uncertain_count, no_evidence_count, note_score, coverage_score, risk_level, message, note_policy_json, analysis_source, freshness_state, latest_evidence_at, next_refresh_at, policy_classified_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        const notePolicy = (payload.note_policy && typeof payload.note_policy === 'object') ? payload.note_policy : {};
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
          counts.mixed,
          counts.contradicted,
          counts.insufficient,
          clampNumber(payload.note_score, 0, 100, aggregate.note_score),
          clampUnit(Object.prototype.hasOwnProperty.call(payload, 'coverage_score') ? payload.coverage_score : aggregate.coverage_score),
          String((payload && payload.risk_level) || aggregate.risk_level || 'needs_review'),
          String(payload.message || ''),
          JSON.stringify(notePolicy),
          String(payload.analysis_source || 'fallback'),
          String(payload.freshness_state || 'stable'),
          clampNumber(payload.latest_evidence_at, 0, Number.MAX_SAFE_INTEGER, 0),
          clampNumber(payload.next_refresh_at, 0, Number.MAX_SAFE_INTEGER, 0),
          clampNumber((payload && payload.policy_classified_at) || (notePolicy && notePolicy.classified_at), 0, Number.MAX_SAFE_INTEGER, 0),
        ]);
        runStmt.step();
        runStmt.free();

        const claimStmt = db.prepare(`
          INSERT INTO note_claims (
            id, note_id, analysis_run_id, claim_index, start_offset, end_offset, claim_text, normalized_claim_text, subject_text, predicate_text, object_text, time_text, modality, factuality, status, top_score, highlight_score, support_confidence, contradict_confidence, truth_confidence, claim_type, claim_weight, claim_reliability, corroboration, authority, freshness, explanation, rewrite_suggestions
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            normalizeClaimStatus(item && item.status),
            Number((item && item.top_score) || (item && item.truth_confidence) || 0) || 0,
            Number((item && item.highlight_score) || (item && item.truth_confidence) || 0) || 0,
            Number((item && item.support_confidence) || 0) || 0,
            Number((item && item.contradict_confidence) || 0) || 0,
            Number((item && item.truth_confidence) || (item && item.highlight_score) || (item && item.top_score) || 0) || 0,
            String((item && item.claim_type) || 'state_trend'),
            Number((item && item.claim_weight) || getClaimTypeWeight(item && item.claim_type) || 0) || 0,
            Number((item && item.claim_reliability) || 0) || 0,
            Number((item && item.corroboration) || 0) || 0,
            Number((item && item.authority) || 0) || 0,
            Number((item && item.freshness) || 0) || 0,
            String((item && item.explanation) || ''),
            JSON.stringify(Array.isArray(item && item.rewrite_suggestions) ? item.rewrite_suggestions : []),
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
            String((item && item.support_label) || (item && item.stance) || 'support'),
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
          analysis_summary: decorateAnalysisSummary(queryLatestAnalysisSummary(db, note.id)),
        };
      });
    },

    async getAnalysis(noteId = '') {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        const summary = queryLatestAnalysisSummary(db, note.id);
        const claims = summary ? queryClaimsForAnalysis(db, summary.analysis_run_id) : [];
        const metrics = summary ? {
          note_score: Number(summary.note_score || 0) || 0,
          coverage_score: Number(summary.coverage_score || 0) || 0,
          risk_level: String(summary.risk_level || 'needs_review').trim() || 'needs_review',
        } : computeAggregateMetrics(claims);
        const citationsByClaimId = new Map(claims.map((claim) => [String((claim && claim.id) || '').trim(), queryCitationsForClaim(db, String((claim && claim.id) || '').trim())]));
        const regions = buildSentenceRegions(note.body_markdown, claims);
        return {
          ok: true,
          note,
          analysis_summary: decorateAnalysisSummary(summary),
          claims,
          note_score: metrics.note_score,
          coverage_score: metrics.coverage_score,
          risk_level: metrics.risk_level,
          regions,
          evidence_feed: buildEvidenceFeed(regions, claims, citationsByClaimId),
        };
      });
    },

    async getEvidenceFeed(noteId = '') {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        const summary = queryLatestAnalysisSummary(db, note.id);
        const claims = summary ? queryClaimsForAnalysis(db, summary.analysis_run_id) : [];
        const citationsByClaimId = new Map(claims.map((claim) => [String((claim && claim.id) || '').trim(), queryCitationsForClaim(db, String((claim && claim.id) || '').trim())]));
        const regions = buildSentenceRegions(note.body_markdown, claims);
        return {
          ok: true,
          note,
          analysis_summary: decorateAnalysisSummary(summary),
          evidence_feed: buildEvidenceFeed(regions, claims, citationsByClaimId),
        };
      });
    },

    async getCitations(noteId = '', claimId = '', options = {}) {
      return runWithDb(async (db) => {
        const note = queryNoteById(db, noteId);
        if (!note) return { ok: false, message: 'Note not found.' };
        const summary = queryLatestAnalysisSummary(db, note.id);
        if (!summary) return { ok: true, note, claim: null, citations: [] };
        const claims = queryClaimsForAnalysis(db, summary.analysis_run_id);
        const claimInput = (options && typeof options === 'object')
          ? ((options.claim && typeof options.claim === 'object') ? options.claim : options)
          : {};
        const claim = resolveClaimForLookup(claims, claimId, claimInput);
        if (!claim) return { ok: false, message: 'Claim not found.' };
        return {
          ok: true,
          note,
          evidence_mode: NOTE_EVIDENCE_MODE,
          claim,
          citations: queryCitationsForClaim(db, claim.id).slice(0, 6),
        };
      });
    },
  };
}

module.exports = {
  createNotesStore,
  getNotesDbPath,
};
