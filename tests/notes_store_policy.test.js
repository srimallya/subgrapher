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
