const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { createNotesStore } = require('../runtime/notes_store');

function makeTempDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'subgrapher-notes-store-test-'));
}

test('notes store persists analysis policy and freshness metadata', async () => {
  const tempDir = makeTempDir();
  const store = createNotesStore({ userDataPath: tempDir });
  const createRes = await store.createNote({
    title: 'Live note',
    body_markdown: 'As of today, the current conflict continues.',
    active_mode: 'view',
  });
  assert.equal(createRes.ok, true);

  const noteId = createRes.note.id;
  const saveRes = await store.saveAnalysis(noteId, {
    analysis_run_id: 'analysis_1',
    note_revision: createRes.note.analysis_revision,
    extractor_version: 'note-analyzer-v8',
    status: 'completed',
    message: 'Evidence scan completed.',
    note_policy: {
      note_mode: 'live_update',
      freshness_bias: 'high',
      staleness_ttl_minutes: 90,
    },
    analysis_source: 'llm',
    freshness_state: 'stale',
    latest_evidence_at: 1_710_000_000_000,
    next_refresh_at: 1_710_000_360_000,
    policy_classified_at: 1_710_000_000_000,
    claims: [],
    sources: [],
    passages: [],
    citations: [],
  });
  assert.equal(saveRes.ok, true);

  const analysisRes = await store.getAnalysis(noteId);
  assert.equal(analysisRes.ok, true);
  assert.equal(analysisRes.analysis_summary.note_policy.note_mode, 'live_update');
  assert.equal(analysisRes.analysis_summary.analysis_source, 'llm');
  assert.equal(analysisRes.analysis_summary.freshness_state, 'stale');
  assert.equal(analysisRes.analysis_summary.latest_evidence_at, 1_710_000_000_000);
  assert.equal(analysisRes.analysis_summary.next_refresh_at, 1_710_000_360_000);
});

test('notes store resetAnalyses clears saved evidence state without deleting notes', async () => {
  const tempDir = makeTempDir();
  const store = createNotesStore({ userDataPath: tempDir });
  const createRes = await store.createNote({
    title: 'Persistent note',
    body_markdown: 'This note body should survive reset.',
    active_mode: 'view',
  });
  assert.equal(createRes.ok, true);

  const noteId = createRes.note.id;
  const saveRes = await store.saveAnalysis(noteId, {
    analysis_run_id: 'analysis_reset_1',
    note_revision: createRes.note.analysis_revision,
    extractor_version: 'note-analyzer-v8',
    status: 'completed',
    message: 'Saved before reset.',
    note_policy: { note_mode: 'live_update', freshness_bias: 'high' },
    analysis_source: 'llm',
    freshness_state: 'stable',
    latest_evidence_at: 1_710_000_000_000,
    next_refresh_at: 1_710_000_360_000,
    claims: [],
    sources: [],
    passages: [],
    citations: [],
  });
  assert.equal(saveRes.ok, true);

  const resetRes = await store.resetAnalyses([noteId]);
  assert.equal(resetRes.ok, true);
  assert.equal(resetRes.cleared_notes, 1);

  const noteRes = await store.getNote(noteId);
  assert.equal(noteRes.ok, true);
  assert.equal(noteRes.note.title, 'Persistent note');
  assert.equal(noteRes.note.body_markdown, 'This note body should survive reset.');
  assert.equal(noteRes.note.last_analyzed_at, 0);
  assert.equal(noteRes.analysis_summary, null);
});
